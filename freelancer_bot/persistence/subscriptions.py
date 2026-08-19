from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from hashlib import sha256
import json
from typing import Any, Mapping
from uuid import UUID, uuid4

import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncConnection

from ..billing import SubscriptionState
from .payments import SubscriptionPeriodRecord
from .schema import subscription_state_events, subscription_states, subscription_periods, users
from .search_profiles import UserNotFound


SUBSCRIPTION_STATE_SCHEMA_VERSION = "subscription-state.v1"
SUBSCRIPTION_STATE_EVENT_SCHEMA_VERSION = "subscription-state-event.v1"


class SubscriptionTransitionError(RuntimeError):
    """A requested lifecycle transition is not valid for the current state."""


@dataclass(frozen=True)
class SubscriptionStateRecord:
    user_id: UUID
    state: SubscriptionState
    state_version: int
    provider: str | None
    current_period_id: UUID | None
    current_period_start_at: datetime | None
    current_period_end_at: datetime | None
    reason: str
    changed_at: datetime
    created_at: datetime
    updated_at: datetime


@dataclass(frozen=True)
class SubscriptionStateEventRecord:
    id: UUID
    idempotency_key: str
    schema_version: str
    user_id: UUID
    state_version: int
    from_state: SubscriptionState | None
    to_state: SubscriptionState
    provider: str | None
    subscription_period_id: UUID | None
    reason: str
    effective_at: datetime
    created_at: datetime


class SubscriptionRepository:
    """Maintain a provider-neutral current state and immutable transitions."""

    async def reconcile(
        self,
        connection: AsyncConnection,
        *,
        user_id: UUID,
        evaluated_at: datetime | None = None,
        reason: str = "entitlement.reconcile",
    ) -> SubscriptionStateRecord:
        user = await self._lock_user(connection, user_id)
        effective_at = (
            evaluated_at
            if evaluated_at is not None
            else await connection.scalar(sa.select(sa.func.now()))
        )
        if effective_at is None:
            raise SubscriptionTransitionError("database clock returned no timestamp")
        effective_at = _aware_utc(effective_at, "evaluated_at")
        periods = await self._periods(connection, user_id)
        current = await self._get_for_update(connection, user_id)
        if current is None:
            desired_state, desired_period, desired_reason = _natural_projection(
                user,
                periods,
                evaluated_at=effective_at,
            )
            return await self._create_state(
                connection,
                user_id=user_id,
                state=desired_state,
                period=desired_period,
                reason=_requested_or_natural_reason(reason, desired_reason),
                effective_at=effective_at,
            )

        if current.state in {
            SubscriptionState.PAUSED,
            SubscriptionState.CANCELLED,
        }:
            return current

        desired_state, desired_period, desired_reason = _natural_projection(
            user,
            periods,
            evaluated_at=effective_at,
        )
        if _same_projection(current, desired_state, desired_period):
            return current
        transition_reason = _requested_or_natural_reason(
            reason,
            _transition_reason(
                current,
                desired_state=desired_state,
                desired_reason=desired_reason,
            ),
        )
        return await self._transition(
            connection,
            current=current,
            state=desired_state,
            period=desired_period,
            reason=transition_reason,
            effective_at=effective_at,
        )

    async def pause(
        self,
        connection: AsyncConnection,
        *,
        user_id: UUID,
        effective_at: datetime,
        reason: str = "subscription.paused",
    ) -> SubscriptionStateRecord:
        current = await self._state_for_transition(
            connection,
            user_id=user_id,
            effective_at=effective_at,
        )
        if current.state is SubscriptionState.PAUSED:
            return current
        if current.state is not SubscriptionState.PAID_ACTIVE:
            raise SubscriptionTransitionError(
                "only a paid-active subscription can be paused"
            )
        return await self._transition(
            connection,
            current=current,
            state=SubscriptionState.PAUSED,
            period=None,
            reason=reason,
            effective_at=effective_at,
        )

    async def cancel(
        self,
        connection: AsyncConnection,
        *,
        user_id: UUID,
        effective_at: datetime,
        reason: str = "subscription.cancelled",
    ) -> SubscriptionStateRecord:
        current = await self._state_for_transition(
            connection,
            user_id=user_id,
            effective_at=effective_at,
        )
        if current.state is SubscriptionState.CANCELLED:
            return current
        if current.state not in {
            SubscriptionState.TRIAL_ACTIVE,
            SubscriptionState.PAID_ACTIVE,
            SubscriptionState.PAUSED,
        }:
            raise SubscriptionTransitionError(
                "only an active or paused subscription can be cancelled"
            )
        return await self._transition(
            connection,
            current=current,
            state=SubscriptionState.CANCELLED,
            period=None,
            reason=reason,
            effective_at=effective_at,
        )

    async def resume(
        self,
        connection: AsyncConnection,
        *,
        user_id: UUID,
        effective_at: datetime,
        reason: str = "subscription.resumed",
    ) -> SubscriptionStateRecord:
        user = await self._lock_user(connection, user_id)
        effective_at = _aware_utc(effective_at, "effective_at")
        periods = await self._periods(connection, user_id)
        current = await self._get_for_update(connection, user_id)
        if current is None:
            return await self.reconcile(
                connection,
                user_id=user_id,
                evaluated_at=effective_at,
                reason=reason,
            )
        if current.state not in {
            SubscriptionState.PAUSED,
            SubscriptionState.CANCELLED,
        }:
            return current
        desired_state, desired_period, desired_reason = _natural_projection(
            user,
            periods,
            evaluated_at=effective_at,
        )
        return await self._transition(
            connection,
            current=current,
            state=desired_state,
            period=desired_period,
            reason=reason if desired_state is not SubscriptionState.EXPIRED else desired_reason,
            effective_at=effective_at,
        )

    async def get(
        self,
        connection: AsyncConnection,
        *,
        user_id: UUID,
    ) -> SubscriptionStateRecord | None:
        row = (
            await connection.execute(
                sa.select(subscription_states).where(
                    subscription_states.c.user_id == user_id
                )
            )
        ).mappings().one_or_none()
        return None if row is None else _state_record(row)

    async def list_events(
        self,
        connection: AsyncConnection,
        *,
        user_id: UUID,
    ) -> tuple[SubscriptionStateEventRecord, ...]:
        rows = (
            await connection.execute(
                sa.select(subscription_state_events)
                .where(subscription_state_events.c.user_id == user_id)
                .order_by(
                    subscription_state_events.c.state_version,
                    subscription_state_events.c.id,
                )
            )
        ).mappings().all()
        return tuple(_event_record(row) for row in rows)

    async def _state_for_transition(
        self,
        connection: AsyncConnection,
        *,
        user_id: UUID,
        effective_at: datetime,
    ) -> SubscriptionStateRecord:
        await self._lock_user(connection, user_id)
        current = await self._get_for_update(connection, user_id)
        if current is not None:
            return current
        return await self.reconcile(
            connection,
            user_id=user_id,
            evaluated_at=effective_at,
            reason="entitlement.reconcile",
        )

    async def _lock_user(
        self,
        connection: AsyncConnection,
        user_id: UUID,
    ) -> Mapping[str, Any]:
        row = (
            await connection.execute(
                sa.select(
                    users.c.id,
                    users.c.trial_started_at,
                    users.c.trial_expires_at,
                    users.c.trial_policy_version,
                )
                .where(users.c.id == user_id)
                .with_for_update()
            )
        ).mappings().one_or_none()
        if row is None:
            raise UserNotFound(f"User {user_id} does not exist")
        return row

    async def _get_for_update(
        self,
        connection: AsyncConnection,
        user_id: UUID,
    ) -> SubscriptionStateRecord | None:
        row = (
            await connection.execute(
                sa.select(subscription_states)
                .where(subscription_states.c.user_id == user_id)
                .with_for_update()
            )
        ).mappings().one_or_none()
        return None if row is None else _state_record(row)

    async def _periods(
        self,
        connection: AsyncConnection,
        user_id: UUID,
    ) -> tuple[SubscriptionPeriodRecord, ...]:
        rows = (
            await connection.execute(
                sa.select(subscription_periods)
                .where(subscription_periods.c.user_id == user_id)
                .order_by(
                    subscription_periods.c.period_start_at,
                    subscription_periods.c.period_end_at,
                    subscription_periods.c.id,
                )
            )
        ).mappings().all()
        return tuple(
            SubscriptionPeriodRecord(
                id=row["id"],
                schema_version=str(row["schema_version"]),
                provider=str(row["provider"]),
                provider_payment_id=str(row["provider_payment_id"]),
                payment_provider_event_id=row["payment_provider_event_id"],
                user_id=row["user_id"],
                amount=row["amount"],
                currency=str(row["currency"]),
                period_start_at=row["period_start_at"],
                period_end_at=row["period_end_at"],
                created_at=row["created_at"],
            )
            for row in rows
        )

    async def _create_state(
        self,
        connection: AsyncConnection,
        *,
        user_id: UUID,
        state: SubscriptionState,
        period: SubscriptionPeriodRecord | None,
        reason: str,
        effective_at: datetime,
    ) -> SubscriptionStateRecord:
        reason = _safe_reason(reason)
        values = _state_values(
            user_id=user_id,
            state=state,
            state_version=1,
            period=period,
            reason=reason,
            effective_at=effective_at,
        )
        inserted_id = await connection.scalar(
            pg_insert(subscription_states)
            .values(**values)
            .on_conflict_do_nothing(index_elements=[subscription_states.c.user_id])
            .returning(subscription_states.c.user_id)
        )
        current = await self._get_for_update(connection, user_id)
        if current is None:
            raise SubscriptionTransitionError(
                "subscription state insert returned no record"
            )
        if inserted_id is not None:
            await self._record_event(
                connection,
                current=current,
                from_state=None,
                period=period,
                reason=reason,
                effective_at=effective_at,
            )
        return current

    async def _transition(
        self,
        connection: AsyncConnection,
        *,
        current: SubscriptionStateRecord,
        state: SubscriptionState,
        period: SubscriptionPeriodRecord | None,
        reason: str,
        effective_at: datetime,
    ) -> SubscriptionStateRecord:
        reason = _safe_reason(reason)
        values = _state_values(
            user_id=current.user_id,
            state=state,
            state_version=current.state_version + 1,
            period=period,
            reason=reason,
            effective_at=effective_at,
        )
        await connection.execute(
            sa.update(subscription_states)
            .where(subscription_states.c.user_id == current.user_id)
            .values(**values)
        )
        updated = await self._get_for_update(connection, current.user_id)
        if updated is None:
            raise SubscriptionTransitionError(
                "subscription state transition returned no record"
            )
        await self._record_event(
            connection,
            current=updated,
            from_state=current.state,
            period=period,
            reason=reason,
            effective_at=effective_at,
        )
        return updated

    async def _record_event(
        self,
        connection: AsyncConnection,
        *,
        current: SubscriptionStateRecord,
        from_state: SubscriptionState | None,
        period: SubscriptionPeriodRecord | None,
        reason: str,
        effective_at: datetime,
    ) -> None:
        idempotency_key = _event_idempotency_key(
            user_id=current.user_id,
            state_version=current.state_version,
            state=current.state,
            period_id=None if period is None else period.id,
        )
        await connection.execute(
            pg_insert(subscription_state_events)
            .values(
                id=uuid4(),
                idempotency_key=idempotency_key,
                schema_version=SUBSCRIPTION_STATE_EVENT_SCHEMA_VERSION,
                user_id=current.user_id,
                state_version=current.state_version,
                from_state=None if from_state is None else from_state.value,
                to_state=current.state.value,
                provider=current.provider,
                subscription_period_id=current.current_period_id,
                reason=reason,
                effective_at=effective_at,
            )
            .on_conflict_do_nothing(
                constraint="uq_subscription_state_events_user_version"
            )
        )


def _natural_projection(
    user: Mapping[str, Any],
    periods: tuple[SubscriptionPeriodRecord, ...],
    *,
    evaluated_at: datetime,
) -> tuple[SubscriptionState, SubscriptionPeriodRecord | None, str]:
    evaluated_at = _aware_utc(evaluated_at, "evaluated_at")
    active = [
        period
        for period in periods
        if _aware_utc(period.period_start_at, "period_start_at")
        <= evaluated_at
        < _aware_utc(period.period_end_at, "period_end_at")
    ]
    if active:
        period = max(
            active,
            key=lambda value: (
                _aware_utc(value.period_end_at, "period_end_at"),
                _aware_utc(value.period_start_at, "period_start_at"),
                str(value.id),
            ),
        )
        return SubscriptionState.PAID_ACTIVE, period, "paid_period_active"

    started = user["trial_started_at"]
    if started is None:
        if periods:
            return SubscriptionState.EXPIRED, None, "subscription_expired"
        return SubscriptionState.TRIAL_NOT_STARTED, None, "trial_not_started"
    started = _aware_utc(started, "trial_started_at")
    expires = user["trial_expires_at"]
    if expires is None:
        raise SubscriptionTransitionError("trial expiry is missing for started trial")
    expires = _aware_utc(expires, "trial_expires_at")
    if evaluated_at < expires:
        return SubscriptionState.TRIAL_ACTIVE, None, "trial_active"
    if periods:
        return SubscriptionState.EXPIRED, None, "subscription_expired"
    return SubscriptionState.EXPIRED, None, "trial_expired"


def _same_projection(
    current: SubscriptionStateRecord,
    state: SubscriptionState,
    period: SubscriptionPeriodRecord | None,
) -> bool:
    return (
        current.state is state
        and current.current_period_id == (None if period is None else period.id)
    )


def _transition_reason(
    current: SubscriptionStateRecord,
    *,
    desired_state: SubscriptionState,
    desired_reason: str,
) -> str:
    if (
        current.state is SubscriptionState.PAID_ACTIVE
        and desired_state is SubscriptionState.PAID_ACTIVE
    ):
        return "subscription.renewed"
    return desired_reason


def _requested_or_natural_reason(requested: str, natural: str) -> str:
    return natural if requested == "entitlement.reconcile" else requested


def _state_values(
    *,
    user_id: UUID,
    state: SubscriptionState,
    state_version: int,
    period: SubscriptionPeriodRecord | None,
    reason: str,
    effective_at: datetime,
) -> dict[str, object]:
    return {
        "user_id": user_id,
        "state": state.value,
        "state_version": state_version,
        "provider": None if period is None else period.provider,
        "current_period_id": None if period is None else period.id,
        "current_period_start_at": (
            None if period is None else period.period_start_at
        ),
        "current_period_end_at": None if period is None else period.period_end_at,
        "reason": reason,
        "changed_at": effective_at,
        "updated_at": sa.func.now(),
    }


def _event_idempotency_key(
    *,
    user_id: UUID,
    state_version: int,
    state: SubscriptionState,
    period_id: UUID | None,
) -> str:
    payload = {
        "schema_version": SUBSCRIPTION_STATE_EVENT_SCHEMA_VERSION,
        "user_id": str(user_id),
        "state_version": state_version,
        "state": state.value,
        "period_id": None if period_id is None else str(period_id),
    }
    return sha256(
        json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()


def _safe_reason(value: str) -> str:
    normalized = value.strip().lower()
    if not normalized or len(normalized) > 64:
        raise ValueError("subscription transition reason is invalid")
    if not normalized[0].isalpha() or not all(
        character.isascii()
        and (character.isalnum() or character in "._-")
        for character in normalized
    ):
        raise ValueError("subscription transition reason is invalid")
    return normalized


def _aware_utc(value: datetime, field: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field} must include a timezone")
    return value.astimezone(timezone.utc)


def _state_record(row: Mapping[str, Any]) -> SubscriptionStateRecord:
    return SubscriptionStateRecord(
        user_id=row["user_id"],
        state=SubscriptionState(row["state"]),
        state_version=int(row["state_version"]),
        provider=None if row["provider"] is None else str(row["provider"]),
        current_period_id=row["current_period_id"],
        current_period_start_at=row["current_period_start_at"],
        current_period_end_at=row["current_period_end_at"],
        reason=str(row["reason"]),
        changed_at=row["changed_at"],
        created_at=row["created_at"],
        updated_at=row["updated_at"],
    )


def _event_record(row: Mapping[str, Any]) -> SubscriptionStateEventRecord:
    return SubscriptionStateEventRecord(
        id=row["id"],
        idempotency_key=str(row["idempotency_key"]),
        schema_version=str(row["schema_version"]),
        user_id=row["user_id"],
        state_version=int(row["state_version"]),
        from_state=(
            None if row["from_state"] is None else SubscriptionState(row["from_state"])
        ),
        to_state=SubscriptionState(row["to_state"]),
        provider=None if row["provider"] is None else str(row["provider"]),
        subscription_period_id=row["subscription_period_id"],
        reason=str(row["reason"]),
        effective_at=row["effective_at"],
        created_at=row["created_at"],
    )
