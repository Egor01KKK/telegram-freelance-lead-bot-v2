from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Protocol
from uuid import UUID


@dataclass(frozen=True)
class AIModelPrice:
    pricing_version: str
    input_usd_per_million: Decimal
    output_usd_per_million: Decimal


@dataclass(frozen=True)
class AISpendGuardPolicy:
    """Conservative, provider-neutral spend reservations for AI calls."""

    daily_limit_usd: Decimal | None = None
    monthly_limit_usd: Decimal | None = None
    reserve_input_tokens: int = 1_000
    reserve_output_tokens: int = 300

    def __post_init__(self) -> None:
        for value, name in (
            (self.daily_limit_usd, "daily_limit_usd"),
            (self.monthly_limit_usd, "monthly_limit_usd"),
        ):
            if value is not None:
                normalized = Decimal(str(value))
                if not normalized.is_finite() or normalized < 0:
                    raise ValueError(f"{name} must be a finite nonnegative decimal")
        for value, name in (
            (self.reserve_input_tokens, "reserve_input_tokens"),
            (self.reserve_output_tokens, "reserve_output_tokens"),
        ):
            if not isinstance(value, int) or value < 0:
                raise ValueError(f"{name} must be a nonnegative integer")
        if self.daily_limit_usd is None and self.monthly_limit_usd is None:
            raise ValueError("at least one AI spend limit must be configured")

    def reserve_cost(self, price: AIModelPrice) -> Decimal:
        return (
            Decimal(self.reserve_input_tokens) * price.input_usd_per_million
            + Decimal(self.reserve_output_tokens) * price.output_usd_per_million
        ) / Decimal(1_000_000)


@dataclass(frozen=True)
class AICallStart:
    raw_message_id: UUID
    stage: str
    provider: str
    requested_model: str
    analyzer_version: str
    prompt_version: str
    schema_version: str
    routing_version: str
    route_reason: str
    provider_attempt: int
    price: AIModelPrice


@dataclass(frozen=True)
class AICallFinish:
    status: str
    latency_ms: int
    response_model: str | None = None
    input_tokens: int | None = None
    output_tokens: int | None = None
    total_tokens: int | None = None
    error_code: str | None = None


class AIBudgetExceeded(RuntimeError):
    """A configured daily/monthly guard rejected a new provider attempt."""

    def __init__(
        self,
        *,
        window: str,
        limit_usd: Decimal,
        used_usd: Decimal,
        requested_usd: Decimal,
    ) -> None:
        self.window = window
        self.limit_usd = limit_usd
        self.used_usd = used_usd
        self.requested_usd = requested_usd
        super().__init__(
            f"AI {window} spend guard exceeded: "
            f"used={used_usd} requested={requested_usd} limit={limit_usd}"
        )


class AICallRecorder(Protocol):
    async def begin(self, call: AICallStart) -> UUID: ...

    async def finish(self, call_id: UUID, result: AICallFinish) -> None: ...
