from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Any, Mapping
from uuid import UUID, uuid4

import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncConnection

from ..payment_provider import (
    PAYMENT_PROVIDER_EVENT_SCHEMA_VERSION,
    SUBSCRIPTION_PERIOD_SCHEMA_VERSION,
    PaymentProvider,
    PaymentStatus,
    PaymentWebhook,
    VerifiedPaymentEvent,
)
from .schema import payment_provider_events, subscription_periods


class PaymentPersistenceConflict(RuntimeError):
    """The same provider identity was reused with different evidence."""


@dataclass(frozen=True)
class PaymentProviderEventRecord:
    id: UUID
    schema_version: str
    provider: str
    provider_event_id: str
    event_type: str
    provider_payment_id: str
    user_id: UUID
    status: PaymentStatus
    amount: Decimal
    currency: str
    period_start_at: datetime | None
    period_end_at: datetime | None
    occurred_at: datetime
    received_at: datetime
    verification_version: str
    payload: Mapping[str, Any]


@dataclass(frozen=True)
class SubscriptionPeriodRecord:
    id: UUID
    schema_version: str
    provider: str
    provider_payment_id: str
    payment_provider_event_id: UUID
    user_id: UUID
    amount: Decimal
    currency: str
    period_start_at: datetime
    period_end_at: datetime
    created_at: datetime


@dataclass(frozen=True)
class PaymentWriteOutcome:
    event: PaymentProviderEventRecord
    period: SubscriptionPeriodRecord | None
    event_created: bool
    period_created: bool


class PaymentRepository:
    """Atomically persist provider evidence and a confirmed paid period.

    Provider verification happens before this repository is called. The local
    transaction then uses unique provider identities as its idempotency ledger;
    duplicate callbacks converge without updating historical rows.
    """

    async def process_webhook(
        self,
        connection: AsyncConnection,
        *,
        provider: PaymentProvider,
        webhook: PaymentWebhook,
    ) -> PaymentWriteOutcome:
        verified = await provider.verify_webhook(webhook)
        return await self.record_verified_event(connection, verified)

    async def record_verified_event(
        self,
        connection: AsyncConnection,
        event: VerifiedPaymentEvent,
    ) -> PaymentWriteOutcome:
        values = _event_values(event)
        inserted_event_id = await connection.scalar(
            pg_insert(payment_provider_events)
            .values(id=uuid4(), **values)
            .on_conflict_do_nothing(
                constraint="uq_payment_provider_events_provider_event"
            )
            .returning(payment_provider_events.c.id)
        )
        stored_event = await self.get_event(
            connection,
            provider=event.provider,
            provider_event_id=event.provider_event_id,
        )
        if stored_event is None:
            raise PaymentPersistenceConflict(
                "verified payment event insert returned no record"
            )
        _validate_existing_event(stored_event, event)

        period: SubscriptionPeriodRecord | None = None
        period_created = False
        if event.status is PaymentStatus.SUCCEEDED:
            period_values = _period_values(event, stored_event.id)
            inserted_period_id = await connection.scalar(
                pg_insert(subscription_periods)
                .values(id=uuid4(), **period_values)
                .on_conflict_do_nothing(
                    constraint="uq_subscription_periods_provider_payment"
                )
                .returning(subscription_periods.c.id)
            )
            period = await self.get_period(
                connection,
                provider=event.provider,
                provider_payment_id=event.provider_payment_id,
            )
            if period is None:
                raise PaymentPersistenceConflict(
                    "confirmed subscription period insert returned no record"
                )
            _validate_existing_period(period, event)
            period_created = inserted_period_id is not None
            # Keep the current provider-neutral projection in the same local
            # transaction as the confirmed period. The provider boundary has
            # already completed before this method is entered.
            from .subscriptions import SubscriptionRepository

            await SubscriptionRepository().reconcile(
                connection,
                user_id=event.user_id,
                reason="payment.confirmed",
            )

        return PaymentWriteOutcome(
            event=stored_event,
            period=period,
            event_created=inserted_event_id is not None,
            period_created=period_created,
        )

    async def get_event(
        self,
        connection: AsyncConnection,
        *,
        provider: str,
        provider_event_id: str,
    ) -> PaymentProviderEventRecord | None:
        row = (
            await connection.execute(
                sa.select(payment_provider_events).where(
                    payment_provider_events.c.provider == provider,
                    payment_provider_events.c.provider_event_id
                    == provider_event_id,
                )
            )
        ).mappings().one_or_none()
        return None if row is None else _event_record(row)

    async def get_period(
        self,
        connection: AsyncConnection,
        *,
        provider: str,
        provider_payment_id: str,
    ) -> SubscriptionPeriodRecord | None:
        row = (
            await connection.execute(
                sa.select(subscription_periods).where(
                    subscription_periods.c.provider == provider,
                    subscription_periods.c.provider_payment_id
                    == provider_payment_id,
                )
            )
        ).mappings().one_or_none()
        return None if row is None else _period_record(row)

    async def list_periods_for_user(
        self,
        connection: AsyncConnection,
        *,
        user_id: UUID,
    ) -> tuple[SubscriptionPeriodRecord, ...]:
        rows = (
            await connection.execute(
                sa.select(subscription_periods)
                .where(subscription_periods.c.user_id == user_id)
                .order_by(
                    subscription_periods.c.period_start_at,
                    subscription_periods.c.id,
                )
            )
        ).mappings().all()
        return tuple(_period_record(row) for row in rows)


def _event_values(event: VerifiedPaymentEvent) -> dict[str, object]:
    return {
        "schema_version": PAYMENT_PROVIDER_EVENT_SCHEMA_VERSION,
        "provider": event.provider,
        "provider_event_id": event.provider_event_id,
        "event_type": event.event_type,
        "provider_payment_id": event.provider_payment_id,
        "user_id": event.user_id,
        "status": event.status.value,
        "amount": event.amount,
        "currency": event.currency,
        "period_start_at": event.period_start_at,
        "period_end_at": event.period_end_at,
        "occurred_at": event.occurred_at,
        "received_at": event.received_at,
        "verification_version": event.verification_version,
        "payload": dict(event.payload),
    }


def _period_values(
    event: VerifiedPaymentEvent,
    payment_provider_event_id: UUID,
) -> dict[str, object]:
    if event.period_start_at is None or event.period_end_at is None:
        raise PaymentPersistenceConflict(
            "successful payment is missing a subscription period"
        )
    return {
        "schema_version": SUBSCRIPTION_PERIOD_SCHEMA_VERSION,
        "provider": event.provider,
        "provider_payment_id": event.provider_payment_id,
        "payment_provider_event_id": payment_provider_event_id,
        "user_id": event.user_id,
        "amount": event.amount,
        "currency": event.currency,
        "period_start_at": event.period_start_at,
        "period_end_at": event.period_end_at,
    }


def _validate_existing_event(
    stored: PaymentProviderEventRecord,
    event: VerifiedPaymentEvent,
) -> None:
    actual = (
        stored.schema_version,
        stored.provider,
        stored.provider_event_id,
        stored.event_type,
        stored.provider_payment_id,
        stored.user_id,
        stored.status,
        stored.amount,
        stored.currency,
        stored.period_start_at,
        stored.period_end_at,
        stored.occurred_at,
        stored.verification_version,
        dict(stored.payload),
    )
    expected = (
        PAYMENT_PROVIDER_EVENT_SCHEMA_VERSION,
        event.provider,
        event.provider_event_id,
        event.event_type,
        event.provider_payment_id,
        event.user_id,
        event.status,
        event.amount,
        event.currency,
        event.period_start_at,
        event.period_end_at,
        event.occurred_at,
        event.verification_version,
        dict(event.payload),
    )
    if actual != expected:
        raise PaymentPersistenceConflict(
            "provider event identity exists with different evidence"
        )


def _validate_existing_period(
    stored: SubscriptionPeriodRecord,
    event: VerifiedPaymentEvent,
) -> None:
    if event.period_start_at is None or event.period_end_at is None:
        raise PaymentPersistenceConflict(
            "successful payment is missing a subscription period"
        )
    actual = (
        stored.schema_version,
        stored.provider,
        stored.provider_payment_id,
        stored.user_id,
        stored.amount,
        stored.currency,
        stored.period_start_at,
        stored.period_end_at,
    )
    expected = (
        SUBSCRIPTION_PERIOD_SCHEMA_VERSION,
        event.provider,
        event.provider_payment_id,
        event.user_id,
        event.amount,
        event.currency,
        event.period_start_at,
        event.period_end_at,
    )
    if actual != expected:
        raise PaymentPersistenceConflict(
            "provider payment identity exists with a different subscription period"
        )


def _event_record(row: Mapping[str, Any]) -> PaymentProviderEventRecord:
    return PaymentProviderEventRecord(
        id=row["id"],
        schema_version=str(row["schema_version"]),
        provider=str(row["provider"]),
        provider_event_id=str(row["provider_event_id"]),
        event_type=str(row["event_type"]),
        provider_payment_id=str(row["provider_payment_id"]),
        user_id=row["user_id"],
        status=PaymentStatus(row["status"]),
        amount=Decimal(row["amount"]),
        currency=str(row["currency"]),
        period_start_at=row["period_start_at"],
        period_end_at=row["period_end_at"],
        occurred_at=row["occurred_at"],
        received_at=row["received_at"],
        verification_version=str(row["verification_version"]),
        payload=dict(row["payload"]),
    )


def _period_record(row: Mapping[str, Any]) -> SubscriptionPeriodRecord:
    return SubscriptionPeriodRecord(
        id=row["id"],
        schema_version=str(row["schema_version"]),
        provider=str(row["provider"]),
        provider_payment_id=str(row["provider_payment_id"]),
        payment_provider_event_id=row["payment_provider_event_id"],
        user_id=row["user_id"],
        amount=Decimal(row["amount"]),
        currency=str(row["currency"]),
        period_start_at=row["period_start_at"],
        period_end_at=row["period_end_at"],
        created_at=row["created_at"],
    )
