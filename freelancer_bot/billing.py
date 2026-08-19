from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from enum import Enum
from typing import Any, Iterable, Protocol
from uuid import UUID


TRIAL_POLICY_VERSION = "trial-entitlement.v1"
BILLING_PLAN_SCHEMA_VERSION = "subscription-plan.v1"
DEFAULT_TRIAL_DURATION = timedelta(days=3)
DEFAULT_SUBSCRIPTION_PLAN_AMOUNT = Decimal("990")
DEFAULT_SUBSCRIPTION_PLAN_CURRENCY = "RUB"
DEFAULT_SUBSCRIPTION_PLAN_INTERVAL = "month"


class EntitlementState(str, Enum):
    TRIAL_NOT_STARTED = "trial_not_started"
    TRIAL_ACTIVE = "trial_active"
    TRIAL_EXPIRED = "trial_expired"
    PAID_ACTIVE = "paid_active"
    EXPIRED = "expired"
    CANCELLED = "cancelled"
    PAUSED = "paused"


class SubscriptionState(str, Enum):
    """Persisted lifecycle projection, independent of any payment provider."""

    TRIAL_NOT_STARTED = "trial_not_started"
    TRIAL_ACTIVE = "trial_active"
    PAID_ACTIVE = "paid_active"
    EXPIRED = "expired"
    CANCELLED = "cancelled"
    PAUSED = "paused"


@dataclass(frozen=True)
class TrialPolicy:
    duration: timedelta = DEFAULT_TRIAL_DURATION
    version: str = TRIAL_POLICY_VERSION

    def __post_init__(self) -> None:
        if self.duration <= timedelta(0):
            raise ValueError("trial duration must be positive")
        if not self.version or not self.version[0].isalnum():
            raise ValueError("trial policy version must be non-empty")


DEFAULT_TRIAL_POLICY = TrialPolicy()


@dataclass(frozen=True)
class BillingPlan:
    amount: Decimal = DEFAULT_SUBSCRIPTION_PLAN_AMOUNT
    currency: str = DEFAULT_SUBSCRIPTION_PLAN_CURRENCY
    interval: str = DEFAULT_SUBSCRIPTION_PLAN_INTERVAL
    schema_version: str = BILLING_PLAN_SCHEMA_VERSION

    def __post_init__(self) -> None:
        amount = Decimal(self.amount)
        currency = self.currency.strip().upper()
        interval = self.interval.strip().lower()
        if not amount.is_finite() or amount <= Decimal("0"):
            raise ValueError("billing plan amount must be positive and finite")
        if len(currency) != 3 or not currency.isascii() or not currency.isalpha():
            raise ValueError("billing plan currency must be a three-letter code")
        if interval != DEFAULT_SUBSCRIPTION_PLAN_INTERVAL:
            raise ValueError("billing plan interval must be month")
        object.__setattr__(self, "amount", amount)
        object.__setattr__(self, "currency", currency)
        object.__setattr__(self, "interval", interval)

    @classmethod
    def from_config(cls, config: Any) -> BillingPlan:
        return cls(
            amount=config.subscription_plan_amount,
            currency=config.subscription_plan_currency,
            interval=config.subscription_plan_interval,
        )

    @property
    def price_label(self) -> str:
        amount = format(self.amount, "f")
        if "." in amount:
            amount = amount.rstrip("0").rstrip(".")
        return f"{amount} {self.currency}/{self.interval}"


@dataclass(frozen=True)
class EntitlementDecision:
    state: EntitlementState
    can_receive_deliveries: bool
    evaluated_at: datetime
    trial_started_at: datetime | None
    trial_expires_at: datetime | None
    trial_policy_version: str | None
    schema_version: str = "entitlement-decision.v1"
    subscription_state: SubscriptionState | None = None
    paid_period_id: UUID | None = None
    paid_provider: str | None = None
    paid_period_start_at: datetime | None = None
    paid_period_end_at: datetime | None = None

    @property
    def is_entitled(self) -> bool:
        return self.can_receive_deliveries

    @property
    def failure_code(self) -> str | None:
        if self.can_receive_deliveries:
            return None
        if self.state is EntitlementState.TRIAL_EXPIRED:
            return "TrialExpired"
        if self.state is EntitlementState.TRIAL_NOT_STARTED:
            return "TrialNotStarted"
        if self.state is EntitlementState.EXPIRED:
            return "SubscriptionExpired"
        if self.state is EntitlementState.CANCELLED:
            return "SubscriptionCancelled"
        if self.state is EntitlementState.PAUSED:
            return "SubscriptionPaused"
        return "EntitlementRequired"


class EntitlementChecker(Protocol):
    async def check(self, connection: Any, user_id: UUID) -> EntitlementDecision: ...


def evaluate_trial_entitlement(
    trial_started_at: datetime | None,
    *,
    evaluated_at: datetime,
    trial_expires_at: datetime | None = None,
    trial_policy_version: str | None = None,
    policy: TrialPolicy = DEFAULT_TRIAL_POLICY,
) -> EntitlementDecision:
    evaluated_at = _utc(evaluated_at)
    if trial_started_at is None:
        if trial_expires_at is not None:
            raise ValueError("trial expiry cannot exist without a trial start")
        return EntitlementDecision(
            state=EntitlementState.TRIAL_NOT_STARTED,
            can_receive_deliveries=False,
            evaluated_at=evaluated_at,
            trial_started_at=None,
            trial_expires_at=None,
            trial_policy_version=None,
        )

    started = _utc(trial_started_at)
    expires = (
        _utc(trial_expires_at)
        if trial_expires_at is not None
        else started + policy.duration
    )
    if expires <= started:
        raise ValueError("trial expiry must be after trial start")
    state = (
        EntitlementState.TRIAL_ACTIVE
        if evaluated_at < expires
        else EntitlementState.TRIAL_EXPIRED
    )
    return EntitlementDecision(
        state=state,
        can_receive_deliveries=state is EntitlementState.TRIAL_ACTIVE,
        evaluated_at=evaluated_at,
        trial_started_at=started,
        trial_expires_at=expires,
        trial_policy_version=trial_policy_version or policy.version,
    )


@dataclass(frozen=True)
class PaidEntitlementPeriod:
    """Provider-neutral paid-period snapshot used by entitlement evaluation."""

    id: UUID
    provider: str
    period_start_at: datetime
    period_end_at: datetime

    def __post_init__(self) -> None:
        provider = self.provider.strip().lower()
        if not provider or provider != self.provider:
            raise ValueError("paid-period provider must be lowercase and non-empty")
        if self.period_start_at.tzinfo is None or self.period_start_at.utcoffset() is None:
            raise ValueError("paid period start must include a timezone")
        if self.period_end_at.tzinfo is None or self.period_end_at.utcoffset() is None:
            raise ValueError("paid period end must include a timezone")
        if self.period_end_at <= self.period_start_at:
            raise ValueError("paid period must end after it starts")


def evaluate_subscription_entitlement(
    trial_started_at: datetime | None,
    *,
    evaluated_at: datetime,
    trial_expires_at: datetime | None = None,
    trial_policy_version: str | None = None,
    paid_periods: Iterable[PaidEntitlementPeriod] = (),
    lifecycle_state: SubscriptionState | str | None = None,
    lifecycle_reason: str | None = None,
    policy: TrialPolicy = DEFAULT_TRIAL_POLICY,
) -> EntitlementDecision:
    """Resolve trial and paid access without trusting provider identity.

    Only periods already persisted from authoritative provider events are
    accepted here. A paid period is active only in its half-open interval
    ``[period_start_at, period_end_at)``. Paused/cancelled are explicit local
    lifecycle decisions and therefore override an otherwise active period.
    """

    evaluated_at = _utc(evaluated_at)
    normalized_state = _subscription_state(lifecycle_state)
    if normalized_state in {
        SubscriptionState.PAUSED,
        SubscriptionState.CANCELLED,
    }:
        state = EntitlementState(normalized_state.value)
        return EntitlementDecision(
            state=state,
            can_receive_deliveries=False,
            evaluated_at=evaluated_at,
            trial_started_at=_optional_utc(trial_started_at),
            trial_expires_at=_trial_expiry(
                trial_started_at,
                trial_expires_at=trial_expires_at,
                policy=policy,
            ),
            trial_policy_version=trial_policy_version,
            subscription_state=normalized_state,
        )

    active_periods = [
        period
        for period in paid_periods
        if _utc(period.period_start_at) <= evaluated_at < _utc(period.period_end_at)
    ]
    active_period = max(
        active_periods,
        key=lambda period: (
            _utc(period.period_end_at),
            _utc(period.period_start_at),
            str(period.id),
        ),
        default=None,
    )
    started = _optional_utc(trial_started_at)
    expires = _trial_expiry(
        trial_started_at,
        trial_expires_at=trial_expires_at,
        policy=policy,
    )
    if active_period is not None:
        return EntitlementDecision(
            state=EntitlementState.PAID_ACTIVE,
            can_receive_deliveries=True,
            evaluated_at=evaluated_at,
            trial_started_at=started,
            trial_expires_at=expires,
            trial_policy_version=trial_policy_version,
            subscription_state=SubscriptionState.PAID_ACTIVE,
            paid_period_id=active_period.id,
            paid_provider=active_period.provider,
            paid_period_start_at=_utc(active_period.period_start_at),
            paid_period_end_at=_utc(active_period.period_end_at),
        )
    if started is None:
        expired_without_trial = normalized_state is SubscriptionState.EXPIRED
        return EntitlementDecision(
            state=(
                EntitlementState.EXPIRED
                if expired_without_trial
                else EntitlementState.TRIAL_NOT_STARTED
            ),
            can_receive_deliveries=False,
            evaluated_at=evaluated_at,
            trial_started_at=None,
            trial_expires_at=None,
            trial_policy_version=None,
            subscription_state=(
                normalized_state
                if expired_without_trial
                else SubscriptionState.TRIAL_NOT_STARTED
            ),
        )
    assert expires is not None
    if evaluated_at < expires:
        return EntitlementDecision(
            state=EntitlementState.TRIAL_ACTIVE,
            can_receive_deliveries=True,
            evaluated_at=evaluated_at,
            trial_started_at=started,
            trial_expires_at=expires,
            trial_policy_version=trial_policy_version or policy.version,
            subscription_state=SubscriptionState.TRIAL_ACTIVE,
        )
    expired_state = (
        EntitlementState.TRIAL_EXPIRED
        if lifecycle_reason in {None, "trial_expired"}
        else EntitlementState.EXPIRED
    )
    return EntitlementDecision(
        state=expired_state,
        can_receive_deliveries=False,
        evaluated_at=evaluated_at,
        trial_started_at=started,
        trial_expires_at=expires,
        trial_policy_version=trial_policy_version or policy.version,
        subscription_state=SubscriptionState.EXPIRED,
    )


def _subscription_state(
    value: SubscriptionState | str | None,
) -> SubscriptionState | None:
    if value is None:
        return None
    return value if isinstance(value, SubscriptionState) else SubscriptionState(value)


def _optional_utc(value: datetime | None) -> datetime | None:
    return None if value is None else _utc(value)


def _trial_expiry(
    trial_started_at: datetime | None,
    *,
    trial_expires_at: datetime | None,
    policy: TrialPolicy,
) -> datetime | None:
    if trial_started_at is None:
        if trial_expires_at is not None:
            raise ValueError("trial expiry cannot exist without a trial start")
        return None
    started = _utc(trial_started_at)
    expires = (
        _utc(trial_expires_at)
        if trial_expires_at is not None
        else started + policy.duration
    )
    if expires <= started:
        raise ValueError("trial expiry must be after trial start")
    return expires


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)
