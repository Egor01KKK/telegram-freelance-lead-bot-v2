from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
from decimal import Decimal
import unittest
from unittest.mock import AsyncMock, patch
from uuid import UUID, uuid4

import sqlalchemy as sa

from freelancer_bot.billing import BillingPlan
from freelancer_bot.payment_provider import (
    PaymentCheckoutRequest,
    PaymentProvider,
    PaymentProviderUnavailable,
    PaymentStatus,
    PaymentVerificationError,
    PaymentWebhook,
    VerifiedPaymentEvent,
    YooKassaPaymentProvider,
    YooKassaPaymentSnapshot,
)
from freelancer_bot.persistence.database import Database
from freelancer_bot.persistence.payments import (
    PaymentPersistenceConflict,
    PaymentRepository,
)
from freelancer_bot.persistence.schema import payment_provider_events, subscription_periods
from freelancer_bot.persistence.search_profiles import UserRepository
from postgres_support import TEST_DATABASE_URL, migrate_to_head, temporary_database


NOW = datetime(2026, 8, 15, 12, 0, tzinfo=timezone.utc)


class FakePaymentProvider:
    name = "fake"

    def __init__(self, events: dict[str, VerifiedPaymentEvent]) -> None:
        self.events = events
        self.verification_calls = 0

    async def create_checkout(self, request: PaymentCheckoutRequest):
        return type(
            "FakeCheckout",
            (),
            {
                "provider": self.name,
                "provider_payment_id": f"payment:{request.idempotency_key}",
                "checkout_url": "https://payments.example/checkout",
                "idempotency_key": request.idempotency_key,
            },
        )()

    async def verify_webhook(self, webhook: PaymentWebhook) -> VerifiedPaymentEvent:
        self.verification_calls += 1
        try:
            return self.events[webhook.provider_event_id]
        except KeyError as exc:
            raise PaymentVerificationError("fake provider rejected webhook") from exc


class FakeYooKassaGateway:
    def __init__(self, snapshot: YooKassaPaymentSnapshot) -> None:
        self.snapshot = snapshot
        self.fetch_calls = 0

    async def create_payment(self, request: PaymentCheckoutRequest):
        return self.snapshot

    async def payment_id_from_webhook(self, webhook: PaymentWebhook) -> str:
        return "yk-payment-1"

    async def fetch_payment(self, provider_payment_id: str):
        self.fetch_calls += 1
        return self.snapshot


class PaymentProviderContractTest(unittest.IsolatedAsyncioTestCase):
    def _event(
        self,
        *,
        event_id: str = "event-1",
        payment_id: str = "payment-1",
        user_id: UUID | None = None,
        status: PaymentStatus = PaymentStatus.SUCCEEDED,
        payload: dict[str, object] | None = None,
    ) -> VerifiedPaymentEvent:
        return VerifiedPaymentEvent(
            provider="fake",
            provider_event_id=event_id,
            event_type="payment.succeeded",
            provider_payment_id=payment_id,
            user_id=user_id or uuid4(),
            status=status,
            amount=Decimal("990"),
            currency="RUB",
            period_start_at=NOW,
            period_end_at=NOW + timedelta(days=31),
            occurred_at=NOW,
            received_at=NOW + timedelta(minutes=1),
            payload=payload or {"authoritative": True},
            verification_version="fake-authoritative.v1",
        )

    async def test_fake_provider_contract_returns_verified_event_and_is_replaceable(self):
        event = self._event()
        provider: PaymentProvider = FakePaymentProvider({event.provider_event_id: event})
        webhook = PaymentWebhook(
            provider_event_id=event.provider_event_id,
            payload={"client_claim": "succeeded"},
            signature=None,
            received_at=event.received_at,
        )

        verified = await provider.verify_webhook(webhook)

        self.assertEqual(verified, event)
        self.assertEqual(provider.name, "fake")

    async def test_yookassa_adapter_uses_authoritative_fetch_not_callback_status(self):
        user_id = uuid4()
        snapshot = YooKassaPaymentSnapshot(
            provider_payment_id="yk-payment-1",
            user_id=user_id,
            status=PaymentStatus.SUCCEEDED,
            amount=Decimal("990"),
            currency="RUB",
            period_start_at=NOW,
            period_end_at=NOW + timedelta(days=31),
            occurred_at=NOW,
            checkout_url="https://yookassa.example/checkout/1",
            event_type="payment.succeeded",
            payload={"status": "succeeded", "source": "provider-fetch"},
        )
        gateway = FakeYooKassaGateway(snapshot)
        provider = YooKassaPaymentProvider(gateway)
        request = PaymentCheckoutRequest(
            user_id=user_id,
            plan=BillingPlan(),
            idempotency_key="checkout-1",
        )

        checkout = await provider.create_checkout(request)
        verified = await provider.verify_webhook(
            PaymentWebhook(
                provider_event_id="webhook-1",
                payload={"status": "succeeded", "user_id": str(uuid4())},
                signature="untrusted-routing-input",
                received_at=NOW + timedelta(minutes=1),
            )
        )

        self.assertEqual(checkout.provider, "yookassa")
        self.assertEqual(checkout.provider_payment_id, "yk-payment-1")
        self.assertEqual(verified.user_id, user_id)
        self.assertEqual(verified.payload, snapshot.payload)
        self.assertEqual(gateway.fetch_calls, 1)

    async def test_yookassa_outage_does_not_become_a_verified_event(self):
        class FailingGateway(FakeYooKassaGateway):
            async def fetch_payment(self, provider_payment_id: str):
                raise RuntimeError("provider unavailable")

        gateway = FailingGateway(
            YooKassaPaymentSnapshot(
                provider_payment_id="yk-payment-1",
                user_id=uuid4(),
                status=PaymentStatus.SUCCEEDED,
                amount=Decimal("990"),
                currency="RUB",
                period_start_at=NOW,
                period_end_at=NOW + timedelta(days=31),
                occurred_at=NOW,
                checkout_url=None,
                event_type="payment.succeeded",
                payload={},
            )
        )
        provider = YooKassaPaymentProvider(gateway)

        with self.assertRaises(PaymentProviderUnavailable):
            await provider.verify_webhook(
                PaymentWebhook(
                    provider_event_id="webhook-1",
                    payload={},
                    signature=None,
                    received_at=NOW,
                )
            )


@unittest.skipUnless(TEST_DATABASE_URL, "TEST_DATABASE_URL is not configured")
class PaymentPersistenceIntegrationTest(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.database_context = temporary_database()
        self.database_url = self.database_context.__enter__()
        migrate_to_head(self.database_url)
        self.database = Database(self.database_url, pool_size=4, max_overflow=8)
        self.repository = PaymentRepository()
        self.user_id: UUID | None = None

    async def asyncTearDown(self):
        await self.database.close()
        self.database_context.__exit__(None, None, None)

    async def _user(self) -> UUID:
        if self.user_id is None:
            async with self.database.transaction() as connection:
                outcome = await UserRepository().ensure(
                    connection,
                    platform="telegram",
                    external_user_id="payment-user",
                )
                self.user_id = outcome.user.id
        assert self.user_id is not None
        return self.user_id

    async def _event(
        self,
        *,
        event_id: str = "event-1",
        payment_id: str = "payment-1",
        status: PaymentStatus = PaymentStatus.SUCCEEDED,
        user_id: UUID | None = None,
        payload: dict[str, object] | None = None,
        period_start_at: datetime = NOW,
    ) -> VerifiedPaymentEvent:
        return VerifiedPaymentEvent(
            provider="fake",
            provider_event_id=event_id,
            event_type=(
                "payment.succeeded"
                if status is PaymentStatus.SUCCEEDED
                else "payment.updated"
            ),
            provider_payment_id=payment_id,
            user_id=user_id or await self._user(),
            status=status,
            amount=Decimal("990"),
            currency="RUB",
            period_start_at=period_start_at,
            period_end_at=period_start_at + timedelta(days=31),
            occurred_at=NOW,
            received_at=NOW + timedelta(minutes=1),
            payload=payload or {"provider": "fake", "payment_id": payment_id},
            verification_version="fake-authoritative.v1",
        )

    async def test_verified_success_is_auditable_and_duplicate_callback_creates_one_period(self):
        event = await self._event()
        provider = FakePaymentProvider({event.provider_event_id: event})
        webhook = PaymentWebhook(
            provider_event_id=event.provider_event_id,
            payload={"status": "succeeded"},
            signature="verified-by-fake",
            received_at=event.received_at,
        )

        async with self.database.transaction() as connection:
            first = await self.repository.process_webhook(
                connection,
                provider=provider,
                webhook=webhook,
            )
        async with self.database.transaction() as connection:
            second = await self.repository.process_webhook(
                connection,
                provider=provider,
                webhook=webhook,
            )

        self.assertTrue(first.event_created)
        self.assertTrue(first.period_created)
        self.assertFalse(second.event_created)
        self.assertFalse(second.period_created)
        self.assertIsNotNone(first.period)
        self.assertEqual(first.period.id, second.period.id)
        async with self.database.connect() as connection:
            self.assertEqual(
                await connection.scalar(
                    sa.select(sa.func.count()).select_from(payment_provider_events)
                ),
                1,
            )
            self.assertEqual(
                await connection.scalar(
                    sa.select(sa.func.count()).select_from(subscription_periods)
                ),
                1,
            )

    async def test_concurrent_duplicate_callbacks_converge_without_duplicate_periods(self):
        event = await self._event()
        provider = FakePaymentProvider({event.provider_event_id: event})
        webhook = PaymentWebhook(
            provider_event_id=event.provider_event_id,
            payload={"status": "succeeded"},
            signature="verified-by-fake",
            received_at=event.received_at,
        )

        async def apply_once():
            async with self.database.transaction() as connection:
                return await self.repository.process_webhook(
                    connection,
                    provider=provider,
                    webhook=webhook,
                )

        first, second = await asyncio.gather(apply_once(), apply_once())

        self.assertEqual(
            sum(outcome.period_created for outcome in (first, second)),
            1,
        )
        self.assertEqual(
            sum(outcome.event_created for outcome in (first, second)),
            1,
        )
        async with self.database.connect() as connection:
            self.assertEqual(
                await connection.scalar(
                    sa.select(sa.func.count()).select_from(subscription_periods)
                ),
                1,
            )

    async def test_renewal_payment_creates_one_period_and_replay_does_not_duplicate_it(self):
        user_id = await self._user()
        first_event = await self._event(user_id=user_id)
        renewal_event = await self._event(
            event_id="renewal-event-1",
            payment_id="renewal-payment-1",
            user_id=user_id,
            period_start_at=NOW + timedelta(days=31),
        )

        async with self.database.transaction() as connection:
            first = await self.repository.record_verified_event(connection, first_event)
        async with self.database.transaction() as connection:
            renewal = await self.repository.record_verified_event(
                connection,
                renewal_event,
            )
        async with self.database.transaction() as connection:
            replay = await self.repository.record_verified_event(
                connection,
                renewal_event,
            )

        self.assertTrue(first.period_created)
        self.assertTrue(renewal.period_created)
        self.assertFalse(replay.period_created)
        async with self.database.connect() as connection:
            periods = await self.repository.list_periods_for_user(
                connection,
                user_id=user_id,
            )
        self.assertEqual(len(periods), 2)
        self.assertEqual(
            {period.provider_payment_id for period in periods},
            {"payment-1", "renewal-payment-1"},
        )

    async def test_conflicting_event_replay_does_not_overwrite_audit_history(self):
        event = await self._event()
        async with self.database.transaction() as connection:
            first = await self.repository.record_verified_event(connection, event)

        conflicting = await self._event(
            payload={"provider": "fake", "payment_id": event.provider_payment_id, "tampered": True}
        )
        with self.assertRaises(PaymentPersistenceConflict):
            async with self.database.transaction() as connection:
                await self.repository.record_verified_event(connection, conflicting)

        async with self.database.connect() as connection:
            stored_event = await self.repository.get_event(
                connection,
                provider=event.provider,
                provider_event_id=event.provider_event_id,
            )
            stored_period = await self.repository.get_period(
                connection,
                provider=event.provider,
                provider_payment_id=event.provider_payment_id,
            )

        self.assertIsNotNone(stored_event)
        self.assertIsNotNone(stored_period)
        self.assertEqual(stored_event.id, first.event.id)
        self.assertEqual(stored_event.payload, dict(event.payload))
        self.assertEqual(stored_period.id, first.period.id)

    async def test_conflicting_payment_identity_rolls_back_new_event_and_period(self):
        event = await self._event()
        async with self.database.transaction() as connection:
            first = await self.repository.record_verified_event(connection, event)

        conflicting = await self._event(
            event_id="event-2",
            period_start_at=NOW + timedelta(days=1),
        )
        with self.assertRaises(PaymentPersistenceConflict):
            async with self.database.transaction() as connection:
                await self.repository.record_verified_event(connection, conflicting)

        async with self.database.connect() as connection:
            self.assertIsNone(
                await self.repository.get_event(
                    connection,
                    provider=conflicting.provider,
                    provider_event_id=conflicting.provider_event_id,
                )
            )
            stored_period = await self.repository.get_period(
                connection,
                provider=event.provider,
                provider_payment_id=event.provider_payment_id,
            )
            self.assertEqual(
                await connection.scalar(
                    sa.select(sa.func.count()).select_from(payment_provider_events)
                ),
                1,
            )
            self.assertEqual(
                await connection.scalar(
                    sa.select(sa.func.count()).select_from(subscription_periods)
                ),
                1,
            )

        self.assertIsNotNone(stored_period)
        self.assertEqual(stored_period.id, first.period.id)
        self.assertEqual(stored_period.period_start_at, event.period_start_at)

    async def test_reconcile_failure_rolls_back_event_and_period_together(self):
        event = await self._event()
        failing_reconcile = AsyncMock(side_effect=RuntimeError("state projection unavailable"))

        with patch(
            "freelancer_bot.persistence.subscriptions.SubscriptionRepository.reconcile",
            new=failing_reconcile,
        ):
            with self.assertRaisesRegex(RuntimeError, "state projection unavailable"):
                async with self.database.transaction() as connection:
                    await self.repository.record_verified_event(connection, event)

        async with self.database.connect() as connection:
            self.assertEqual(
                await connection.scalar(
                    sa.select(sa.func.count()).select_from(payment_provider_events)
                ),
                0,
            )
            self.assertEqual(
                await connection.scalar(
                    sa.select(sa.func.count()).select_from(subscription_periods)
                ),
                0,
            )
            self.assertEqual(
                await connection.scalar(
                    sa.select(sa.func.count()).select_from(
                        sa.table("subscription_states")
                    )
                ),
                0,
            )

    async def test_non_success_event_is_preserved_without_granting_a_paid_period(self):
        event = await self._event(status=PaymentStatus.PENDING)

        async with self.database.transaction() as connection:
            outcome = await self.repository.record_verified_event(connection, event)

        self.assertTrue(outcome.event_created)
        self.assertIsNone(outcome.period)
        async with self.database.connect() as connection:
            self.assertEqual(
                await connection.scalar(
                    sa.select(sa.func.count()).select_from(subscription_periods)
                ),
                0,
            )

    async def test_unverified_webhook_is_rejected_before_postgres_mutation(self):
        provider = FakePaymentProvider({})
        webhook = PaymentWebhook(
            provider_event_id="unverified-event",
            payload={"status": "succeeded", "amount": "990"},
            signature=None,
            received_at=NOW,
        )

        with self.assertRaises(PaymentVerificationError):
            async with self.database.transaction() as connection:
                await self.repository.process_webhook(
                    connection,
                    provider=provider,
                    webhook=webhook,
                )
        async with self.database.connect() as connection:
            self.assertEqual(
                await connection.scalar(
                    sa.select(sa.func.count()).select_from(payment_provider_events)
                ),
                0,
            )

    async def test_provider_outage_preserves_existing_payment_and_entitlement_state(self):
        event = await self._event()
        async with self.database.transaction() as connection:
            first = await self.repository.record_verified_event(connection, event)

        class OutageProvider:
            name = "fake"

            async def verify_webhook(self, webhook):
                raise PaymentProviderUnavailable("fake provider unavailable")

        with self.assertRaises(PaymentProviderUnavailable):
            async with self.database.transaction() as connection:
                await self.repository.process_webhook(
                    connection,
                    provider=OutageProvider(),
                    webhook=PaymentWebhook(
                        provider_event_id="outage-event",
                        payload={},
                        signature=None,
                        received_at=NOW,
                    ),
                )

        async with self.database.connect() as connection:
            stored_event = await self.repository.get_event(
                connection,
                provider=event.provider,
                provider_event_id=event.provider_event_id,
            )
            stored_period = await self.repository.get_period(
                connection,
                provider=event.provider,
                provider_payment_id=event.provider_payment_id,
            )
            self.assertEqual(
                await connection.scalar(
                    sa.select(sa.func.count()).select_from(payment_provider_events)
                ),
                1,
            )
            self.assertEqual(
                await connection.scalar(
                    sa.select(sa.func.count()).select_from(subscription_periods)
                ),
                1,
            )

        self.assertEqual(stored_event.id, first.event.id)
        self.assertEqual(stored_period.id, first.period.id)

    async def test_payment_history_database_guard_rejects_update_and_delete(self):
        event = await self._event()
        async with self.database.transaction() as connection:
            outcome = await self.repository.record_verified_event(connection, event)

        with self.assertRaises(sa.exc.DBAPIError):
            async with self.database.transaction() as connection:
                await connection.execute(
                    sa.update(payment_provider_events)
                    .where(
                        payment_provider_events.c.provider_event_id
                        == event.provider_event_id
                    )
                    .values(status="failed")
                )

        with self.assertRaises(sa.exc.DBAPIError):
            async with self.database.transaction() as connection:
                await connection.execute(
                    sa.delete(payment_provider_events).where(
                        payment_provider_events.c.provider_event_id
                        == event.provider_event_id
                    )
                )

        with self.assertRaises(sa.exc.DBAPIError):
            async with self.database.transaction() as connection:
                await connection.execute(
                    sa.delete(subscription_periods).where(
                        subscription_periods.c.id == outcome.period.id
                    )
                )

        async with self.database.connect() as connection:
            self.assertEqual(
                await connection.scalar(
                    sa.select(sa.func.count()).select_from(payment_provider_events)
                ),
                1,
            )
            self.assertEqual(
                await connection.scalar(
                    sa.select(sa.func.count()).select_from(subscription_periods)
                ),
                1,
            )


if __name__ == "__main__":
    unittest.main()
