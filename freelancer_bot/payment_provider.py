from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from enum import Enum
from types import MappingProxyType
from typing import Any, Mapping, Protocol
from uuid import UUID

from .billing import BillingPlan


PAYMENT_PROVIDER_EVENT_SCHEMA_VERSION = "payment-provider-event.v1"
SUBSCRIPTION_PERIOD_SCHEMA_VERSION = "subscription-period.v1"


class PaymentProviderError(RuntimeError):
    """Base error for an unavailable or invalid payment-provider operation."""


class PaymentProviderUnavailable(PaymentProviderError):
    """The provider could not be reached or returned an unusable response."""


class PaymentVerificationError(PaymentProviderError):
    """The provider event could not be authenticated and authoritative."""


class PaymentProviderContractError(ValueError):
    """A provider adapter returned data outside the payment contract."""


class PaymentStatus(str, Enum):
    PENDING = "pending"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    CANCELLED = "cancelled"


@dataclass(frozen=True)
class PaymentCheckoutRequest:
    """Server-created checkout intent; clients never choose its price or user."""

    user_id: UUID
    plan: BillingPlan
    idempotency_key: str

    def __post_init__(self) -> None:
        _bounded_text(self.idempotency_key, "idempotency_key", limit=255)


@dataclass(frozen=True)
class PaymentCheckout:
    provider: str
    provider_payment_id: str
    checkout_url: str
    idempotency_key: str

    def __post_init__(self) -> None:
        _provider_name(self.provider)
        _bounded_text(self.provider_payment_id, "provider_payment_id", limit=255)
        _bounded_text(self.checkout_url, "checkout_url", limit=2048)
        _bounded_text(self.idempotency_key, "idempotency_key", limit=255)


@dataclass(frozen=True)
class PaymentWebhook:
    """Untrusted provider callback envelope.

    The payload is only an input to the provider adapter. It is never persisted as
    a successful payment or used to grant access until the adapter has verified
    provider state.
    """

    provider_event_id: str
    payload: Mapping[str, Any]
    signature: str | None
    received_at: datetime

    def __post_init__(self) -> None:
        _bounded_text(self.provider_event_id, "provider_event_id", limit=255)
        _aware_datetime(self.received_at, "received_at")
        object.__setattr__(
            self,
            "payload",
            MappingProxyType(dict(self.payload)),
        )


@dataclass(frozen=True)
class VerifiedPaymentEvent:
    """Provider-authenticated, normalized payment evidence.

    Only a PaymentProvider implementation should construct this value. The
    persistence boundary accepts this normalized result, never a client success
    flag or a raw webhook payload.
    """

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
    payload: Mapping[str, Any]
    verification_version: str

    def __post_init__(self) -> None:
        _provider_name(self.provider)
        _bounded_text(self.provider_event_id, "provider_event_id", limit=255)
        _bounded_identifier(self.event_type, "event_type", limit=64)
        _bounded_text(self.provider_payment_id, "provider_payment_id", limit=255)
        status = PaymentStatus(self.status)
        amount = Decimal(self.amount)
        currency = self.currency.strip().upper()
        _aware_datetime(self.occurred_at, "occurred_at")
        _aware_datetime(self.received_at, "received_at")
        _bounded_identifier(
            self.verification_version,
            "verification_version",
            limit=64,
        )
        if not amount.is_finite() or amount < Decimal("0"):
            raise PaymentProviderContractError("payment amount must be finite and non-negative")
        if len(currency) != 3 or not currency.isascii() or not currency.isalpha():
            raise PaymentProviderContractError("payment currency must be a three-letter code")
        if (self.period_start_at is None) != (self.period_end_at is None):
            raise PaymentProviderContractError(
                "payment period must contain both start and end timestamps"
            )
        if self.period_start_at is not None and self.period_end_at is not None:
            _aware_datetime(self.period_start_at, "period_start_at")
            _aware_datetime(self.period_end_at, "period_end_at")
            if self.period_end_at <= self.period_start_at:
                raise PaymentProviderContractError(
                    "payment period must end after it starts"
                )
        if status is PaymentStatus.SUCCEEDED:
            if amount <= Decimal("0"):
                raise PaymentProviderContractError(
                    "successful payment amount must be positive"
                )
            if self.period_start_at is None or self.period_end_at is None:
                raise PaymentProviderContractError(
                    "successful payment must contain a subscription period"
                )
        object.__setattr__(self, "status", status)
        object.__setattr__(self, "amount", amount)
        object.__setattr__(self, "currency", currency)
        object.__setattr__(self, "payload", MappingProxyType(dict(self.payload)))


class PaymentProvider(Protocol):
    """Replaceable boundary for checkout creation and provider verification."""

    @property
    def name(self) -> str: ...

    async def create_checkout(
        self,
        request: PaymentCheckoutRequest,
    ) -> PaymentCheckout: ...

    async def verify_webhook(
        self,
        webhook: PaymentWebhook,
    ) -> VerifiedPaymentEvent: ...


@dataclass(frozen=True)
class YooKassaPaymentSnapshot:
    """Authoritative result returned by the isolated YooKassa gateway adapter."""

    provider_payment_id: str
    user_id: UUID
    status: PaymentStatus
    amount: Decimal
    currency: str
    period_start_at: datetime | None
    period_end_at: datetime | None
    occurred_at: datetime
    checkout_url: str | None
    event_type: str
    payload: Mapping[str, Any]


class YooKassaGateway(Protocol):
    """Small provider-specific transport seam.

    A real gateway owns credentials, HTTP, signature/authentication details and
    provider JSON mapping. The domain and PostgreSQL repositories only receive
    the authoritative snapshot returned by these methods.
    """

    async def create_payment(
        self,
        request: PaymentCheckoutRequest,
    ) -> YooKassaPaymentSnapshot: ...

    async def payment_id_from_webhook(
        self,
        webhook: PaymentWebhook,
    ) -> str: ...

    async def fetch_payment(
        self,
        provider_payment_id: str,
    ) -> YooKassaPaymentSnapshot: ...


class YooKassaPaymentProvider:
    """Concrete provider adapter kept outside billing and delivery decisions.

    Webhook payloads are only routing hints. Payment status, user, amount and
    subscription period come from the provider-authoritative fetch performed by
    the injected gateway. This makes provider outages fail before any local
    payment or entitlement state is changed.
    """

    name = "yookassa"
    verification_version = "yookassa-authoritative-fetch.v1"

    def __init__(self, gateway: YooKassaGateway) -> None:
        self._gateway = gateway

    async def create_checkout(
        self,
        request: PaymentCheckoutRequest,
    ) -> PaymentCheckout:
        try:
            snapshot = await self._gateway.create_payment(request)
        except PaymentProviderError:
            raise
        except Exception as exc:
            raise PaymentProviderUnavailable(
                "YooKassa checkout creation is unavailable"
            ) from exc
        _validate_snapshot(snapshot, require_checkout_url=True)
        assert snapshot.checkout_url is not None
        return PaymentCheckout(
            provider=self.name,
            provider_payment_id=snapshot.provider_payment_id,
            checkout_url=snapshot.checkout_url,
            idempotency_key=request.idempotency_key,
        )

    async def verify_webhook(
        self,
        webhook: PaymentWebhook,
    ) -> VerifiedPaymentEvent:
        try:
            provider_payment_id = await self._gateway.payment_id_from_webhook(webhook)
            _bounded_text(provider_payment_id, "provider_payment_id", limit=255)
            snapshot = await self._gateway.fetch_payment(provider_payment_id)
        except PaymentVerificationError:
            raise
        except PaymentProviderError:
            raise
        except Exception as exc:
            raise PaymentProviderUnavailable(
                "YooKassa payment verification is unavailable"
            ) from exc
        _validate_snapshot(snapshot, require_checkout_url=False)
        if snapshot.provider_payment_id != provider_payment_id:
            raise PaymentVerificationError(
                "provider payment identifier changed during verification"
            )
        return VerifiedPaymentEvent(
            provider=self.name,
            provider_event_id=webhook.provider_event_id,
            event_type=snapshot.event_type,
            provider_payment_id=snapshot.provider_payment_id,
            user_id=snapshot.user_id,
            status=snapshot.status,
            amount=snapshot.amount,
            currency=snapshot.currency,
            period_start_at=snapshot.period_start_at,
            period_end_at=snapshot.period_end_at,
            occurred_at=snapshot.occurred_at,
            received_at=webhook.received_at,
            payload=snapshot.payload,
            verification_version=self.verification_version,
        )


def _validate_snapshot(
    snapshot: YooKassaPaymentSnapshot,
    *,
    require_checkout_url: bool,
) -> None:
    if not isinstance(snapshot, YooKassaPaymentSnapshot):
        raise PaymentProviderContractError("YooKassa gateway returned an invalid snapshot")
    _bounded_text(snapshot.provider_payment_id, "provider_payment_id", limit=255)
    if require_checkout_url and not snapshot.checkout_url:
        raise PaymentProviderContractError("YooKassa checkout URL is missing")
    VerifiedPaymentEvent(
        provider="yookassa",
        provider_event_id="gateway-validation",
        event_type=snapshot.event_type,
        provider_payment_id=snapshot.provider_payment_id,
        user_id=snapshot.user_id,
        status=snapshot.status,
        amount=snapshot.amount,
        currency=snapshot.currency,
        period_start_at=snapshot.period_start_at,
        period_end_at=snapshot.period_end_at,
        occurred_at=snapshot.occurred_at,
        received_at=snapshot.occurred_at,
        payload=snapshot.payload,
        verification_version="gateway-validation.v1",
    )


def _provider_name(value: str) -> str:
    normalized = _bounded_identifier(value, "provider", limit=64).lower()
    if normalized != value:
        raise PaymentProviderContractError("provider must be lowercase")
    return normalized


def _bounded_identifier(value: str, field: str, *, limit: int) -> str:
    normalized = _bounded_text(value, field, limit=limit)
    if not normalized[0].isalnum() or not all(
        character.isascii()
        and (character.isalnum() or character in "._-")
        for character in normalized
    ):
        raise PaymentProviderContractError(f"{field} must be a safe identifier")
    return normalized


def _bounded_text(value: str, field: str, *, limit: int) -> str:
    if not isinstance(value, str):
        raise PaymentProviderContractError(f"{field} must be text")
    normalized = value.strip()
    if not normalized or len(normalized) > limit:
        raise PaymentProviderContractError(
            f"{field} must be non-empty and at most {limit} characters"
        )
    return normalized


def _aware_datetime(value: datetime, field: str) -> None:
    if value.tzinfo is None or value.utcoffset() is None:
        raise PaymentProviderContractError(f"{field} must include a timezone")
