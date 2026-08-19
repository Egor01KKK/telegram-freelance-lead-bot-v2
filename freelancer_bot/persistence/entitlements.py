from __future__ import annotations

from collections.abc import Callable
from datetime import datetime, timezone
from uuid import UUID

import sqlalchemy as sa
from sqlalchemy.ext.asyncio import AsyncConnection

from ..billing import (
    DEFAULT_TRIAL_POLICY,
    EntitlementDecision,
    PaidEntitlementPeriod,
    TrialPolicy,
    evaluate_subscription_entitlement,
)
from .subscriptions import SubscriptionRepository
from .schema import users
from .search_profiles import UserNotFound


class EntitlementUserNotFound(LookupError):
    pass


class TrialEntitlementChecker:
    """Resolve provider-neutral delivery entitlement from PostgreSQL state.

    The compatibility name is retained for callers from G9-T02. Trial columns
    remain immutable; paid access comes only from verified periods already
    persisted by the payment boundary. Paused/cancelled state is local domain
    state and never depends on a provider name.
    """

    def __init__(
        self,
        *,
        policy: TrialPolicy = DEFAULT_TRIAL_POLICY,
        clock: Callable[[], datetime] | None = None,
        subscriptions: SubscriptionRepository | None = None,
    ) -> None:
        self._policy = policy
        self._clock = clock
        self._subscriptions = subscriptions or SubscriptionRepository()

    async def check(
        self,
        connection: AsyncConnection,
        user_id: UUID,
    ) -> EntitlementDecision:
        evaluated_at = (
            self._clock()
            if self._clock is not None
            else await connection.scalar(sa.select(sa.func.now()))
        )
        if evaluated_at is None:
            raise EntitlementUserNotFound("database clock returned no timestamp")
        if evaluated_at.tzinfo is None or evaluated_at.utcoffset() is None:
            evaluated_at = evaluated_at.replace(tzinfo=timezone.utc)
        try:
            state = await self._subscriptions.reconcile(
                connection,
                user_id=user_id,
                evaluated_at=evaluated_at,
            )
        except UserNotFound as exc:
            raise EntitlementUserNotFound(f"User {user_id} does not exist") from exc
        row = (
            await connection.execute(
                sa.select(
                    users.c.trial_started_at,
                    users.c.trial_expires_at,
                    users.c.trial_policy_version,
                ).where(users.c.id == user_id)
            )
        ).mappings().one_or_none()
        if row is None:
            raise EntitlementUserNotFound(f"User {user_id} does not exist")
        paid_periods = ()
        if (
            state.current_period_id is not None
            and state.provider is not None
            and state.current_period_start_at is not None
            and state.current_period_end_at is not None
        ):
            paid_periods = (
                PaidEntitlementPeriod(
                    id=state.current_period_id,
                    provider=state.provider,
                    period_start_at=state.current_period_start_at,
                    period_end_at=state.current_period_end_at,
                ),
            )
        return evaluate_subscription_entitlement(
            row["trial_started_at"],
            trial_expires_at=row["trial_expires_at"],
            trial_policy_version=row["trial_policy_version"],
            evaluated_at=evaluated_at,
            paid_periods=paid_periods,
            lifecycle_state=state.state,
            lifecycle_reason=state.reason,
            policy=self._policy,
        )
