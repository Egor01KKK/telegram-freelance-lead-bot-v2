from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
from decimal import Decimal
import unittest
from uuid import UUID, uuid4

import sqlalchemy as sa

from freelancer_bot.billing import EntitlementState
from freelancer_bot.payment_provider import PaymentStatus, VerifiedPaymentEvent
from freelancer_bot.persistence.database import Database
from freelancer_bot.persistence.entitlements import TrialEntitlementChecker
from freelancer_bot.persistence.payments import PaymentRepository
from freelancer_bot.persistence.schema import (
    subscription_periods,
    subscription_state_events,
    subscription_states,
    users,
)
from freelancer_bot.persistence.subscriptions import (
    SubscriptionRepository,
    SubscriptionTransitionError,
)
from postgres_support import TEST_DATABASE_URL, migrate_to_head, temporary_database


NOW = datetime.now(timezone.utc).replace(microsecond=0)


@unittest.skipUnless(TEST_DATABASE_URL, "TEST_DATABASE_URL is not configured")
class SubscriptionLifecycleIntegrationTest(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.database_context = temporary_database()
        self.database_url = self.database_context.__enter__()
        migrate_to_head(self.database_url)
        self.database = Database(self.database_url, pool_size=6, max_overflow=8)
        self.payments = PaymentRepository()
        self.subscriptions = SubscriptionRepository()

    async def asyncTearDown(self):
        await self.database.close()
        self.database_context.__exit__(None, None, None)

    async def _create_user(
        self,
        external_user_id: str,
        *,
        trial_started_at: datetime | None = None,
        trial_expires_at: datetime | None = None,
    ) -> UUID:
        user_id = uuid4()
        values = {
            "id": user_id,
            "platform": "telegram",
            "external_user_id": external_user_id,
            "created_at": NOW - timedelta(days=10),
            "trial_started_at": trial_started_at,
            "trial_expires_at": trial_expires_at,
            "trial_policy_version": (
                None
                if trial_started_at is None
                else "trial-entitlement.v1"
            ),
        }
        async with self.database.transaction() as connection:
            await connection.execute(users.insert().values(**values))
        return user_id

    async def _record_period(
        self,
        user_id: UUID,
        *,
        payment_id: str = "payment-1",
        event_id: str = "event-1",
        period_start_at: datetime = NOW - timedelta(hours=1),
        period_end_at: datetime = NOW + timedelta(days=31),
    ):
        event = VerifiedPaymentEvent(
            provider="robokassa",
            provider_event_id=event_id,
            event_type="payment.succeeded",
            provider_payment_id=payment_id,
            user_id=user_id,
            status=PaymentStatus.SUCCEEDED,
            amount=Decimal("1250"),
            currency="RUB",
            period_start_at=period_start_at,
            period_end_at=period_end_at,
            occurred_at=NOW,
            received_at=NOW,
            payload={"fixture": "authoritative"},
            verification_version="fixture-authoritative.v1",
        )
        async with self.database.transaction() as connection:
            return await self.payments.record_verified_event(connection, event)

    async def _decision(self, user_id: UUID, *, evaluated_at: datetime = NOW):
        checker = TrialEntitlementChecker(clock=lambda: evaluated_at)
        async with self.database.transaction() as connection:
            return await checker.check(connection, user_id)

    async def test_paid_period_restores_access_after_trial_without_resetting_trial(self):
        user_id = await self._create_user(
            "8100001",
            trial_started_at=NOW - timedelta(days=5),
            trial_expires_at=NOW - timedelta(days=2),
        )
        before = await self._decision(user_id)
        self.assertEqual(before.state, EntitlementState.TRIAL_EXPIRED)
        self.assertFalse(before.can_receive_deliveries)

        outcome = await self._record_period(user_id)
        after = await self._decision(user_id)

        self.assertTrue(outcome.period_created)
        self.assertEqual(after.state, EntitlementState.PAID_ACTIVE)
        self.assertTrue(after.can_receive_deliveries)
        self.assertEqual(after.trial_started_at, NOW - timedelta(days=5))
        self.assertEqual(after.trial_expires_at, NOW - timedelta(days=2))
        self.assertEqual(after.paid_provider, "robokassa")

    async def test_renewal_switches_current_period_without_rewriting_period_history(self):
        user_id = await self._create_user("8100007")
        await self._record_period(user_id)
        await self._record_period(
            user_id,
            payment_id="payment-2",
            event_id="event-2",
            period_start_at=NOW + timedelta(days=31),
            period_end_at=NOW + timedelta(days=62),
        )

        async with self.database.transaction() as connection:
            current_before_renewal = await self.subscriptions.get(
                connection,
                user_id=user_id,
            )
            renewed = await self.subscriptions.reconcile(
                connection,
                user_id=user_id,
                evaluated_at=NOW + timedelta(days=32),
            )
            periods = await self.payments.list_periods_for_user(
                connection,
                user_id=user_id,
            )
            events = await self.subscriptions.list_events(
                connection,
                user_id=user_id,
            )

        self.assertIsNotNone(current_before_renewal)
        self.assertEqual(current_before_renewal.current_period_id, periods[0].id)
        self.assertEqual(renewed.state.value, "paid_active")
        self.assertEqual(renewed.current_period_id, periods[1].id)
        self.assertEqual(len(periods), 2)
        self.assertEqual([event.reason for event in events], [
            "payment.confirmed",
            "subscription.renewed",
        ])


    async def test_expired_paid_period_blocks_without_rewriting_payment_history(self):
        user_id = await self._create_user("8100002")
        outcome = await self._record_period(
            user_id,
            period_start_at=NOW - timedelta(days=32),
            period_end_at=NOW - timedelta(days=1),
        )

        decision = await self._decision(user_id)

        self.assertTrue(outcome.period_created)
        self.assertEqual(decision.state, EntitlementState.EXPIRED)
        self.assertEqual(decision.failure_code, "SubscriptionExpired")
        self.assertFalse(decision.can_receive_deliveries)
        async with self.database.connect() as connection:
            self.assertEqual(
                await connection.scalar(
                    sa.select(sa.func.count()).select_from(subscription_periods)
                ),
                1,
            )

    async def test_pause_cancel_and_resume_are_explicit_and_safe(self):
        user_id = await self._create_user("8100003")
        await self._record_period(user_id)

        async with self.database.transaction() as connection:
            paused = await self.subscriptions.pause(
                connection,
                user_id=user_id,
                effective_at=NOW,
            )
        self.assertEqual(paused.state.value, "paused")
        self.assertEqual((await self._decision(user_id)).state, EntitlementState.PAUSED)

        async with self.database.transaction() as connection:
            repeated_pause = await self.subscriptions.pause(
                connection,
                user_id=user_id,
                effective_at=NOW,
            )
            resumed = await self.subscriptions.resume(
                connection,
                user_id=user_id,
                effective_at=NOW,
            )
        self.assertEqual(repeated_pause.state.value, "paused")
        self.assertEqual(resumed.state.value, "paid_active")
        self.assertEqual((await self._decision(user_id)).state, EntitlementState.PAID_ACTIVE)

        async with self.database.transaction() as connection:
            cancelled = await self.subscriptions.cancel(
                connection,
                user_id=user_id,
                effective_at=NOW,
            )
        self.assertEqual(cancelled.state.value, "cancelled")
        self.assertEqual(
            (await self._decision(user_id)).state,
            EntitlementState.CANCELLED,
        )

        async with self.database.transaction() as connection:
            resumed_after_cancel = await self.subscriptions.resume(
                connection,
                user_id=user_id,
                effective_at=NOW,
            )
        self.assertEqual(resumed_after_cancel.state.value, "paid_active")

        async with self.database.connect() as connection:
            events = await self.subscriptions.list_events(
                connection,
                user_id=user_id,
            )
        self.assertEqual(
            [event.to_state.value for event in events],
            ["paid_active", "paused", "paid_active", "cancelled", "paid_active"],
        )
        self.assertEqual([event.state_version for event in events], list(range(1, 6)))

    async def test_resume_after_period_expiry_does_not_grant_access(self):
        user_id = await self._create_user("8100004")
        await self._record_period(
            user_id,
            period_start_at=NOW - timedelta(hours=1),
            period_end_at=NOW + timedelta(days=1),
        )
        async with self.database.transaction() as connection:
            paused = await self.subscriptions.pause(
                connection,
                user_id=user_id,
                effective_at=NOW,
            )
        self.assertEqual(paused.state.value, "paused")
        async with self.database.transaction() as connection:
            resumed = await self.subscriptions.resume(
                connection,
                user_id=user_id,
                effective_at=NOW + timedelta(days=2),
            )
        self.assertEqual(resumed.state.value, "expired")
        decision = await self._decision(user_id, evaluated_at=NOW + timedelta(days=2))
        self.assertEqual(decision.state, EntitlementState.EXPIRED)
        self.assertFalse(decision.can_receive_deliveries)

    async def test_concurrent_reconciliation_records_one_initial_state_event(self):
        user_id = await self._create_user(
            "8100005",
            trial_started_at=NOW - timedelta(hours=1),
            trial_expires_at=NOW + timedelta(days=3) - timedelta(hours=1),
        )

        async def check_once():
            return await self._decision(user_id)

        first, second = await asyncio.gather(check_once(), check_once())

        self.assertEqual(first.state, EntitlementState.TRIAL_ACTIVE)
        self.assertEqual(second.state, EntitlementState.TRIAL_ACTIVE)
        async with self.database.connect() as connection:
            state_count = await connection.scalar(
                sa.select(sa.func.count()).select_from(subscription_states)
            )
            event_count = await connection.scalar(
                sa.select(sa.func.count()).select_from(subscription_state_events)
            )
        self.assertEqual(state_count, 1)
        self.assertEqual(event_count, 1)

    async def test_invalid_pause_does_not_grant_or_mutate_an_active_trial(self):
        user_id = await self._create_user(
            "8100006",
            trial_started_at=NOW - timedelta(hours=1),
            trial_expires_at=NOW + timedelta(days=3) - timedelta(hours=1),
        )

        with self.assertRaises(SubscriptionTransitionError):
            async with self.database.transaction() as connection:
                await self.subscriptions.pause(
                    connection,
                    user_id=user_id,
                    effective_at=NOW,
                )

        self.assertEqual((await self._decision(user_id)).state, EntitlementState.TRIAL_ACTIVE)

    async def test_subscription_state_history_is_append_only(self):
        user_id = await self._create_user(
            "8100008",
            trial_started_at=NOW - timedelta(hours=1),
            trial_expires_at=NOW + timedelta(days=3) - timedelta(hours=1),
        )
        await self._decision(user_id)

        with self.assertRaises(sa.exc.DBAPIError):
            async with self.database.transaction() as connection:
                await connection.execute(
                    sa.update(subscription_state_events)
                    .where(subscription_state_events.c.user_id == user_id)
                    .values(reason="tampered")
                )


if __name__ == "__main__":
    unittest.main()
