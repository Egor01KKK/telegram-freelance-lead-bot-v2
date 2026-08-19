from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from uuid import UUID, uuid4

import sqlalchemy as sa

from ..ai_telemetry import (
    AIBudgetExceeded,
    AICallFinish,
    AICallStart,
    AISpendGuardPolicy,
)
from .database import Database
from .schema import ai_call_telemetry


@dataclass(frozen=True)
class AIDailyCost:
    day: date
    stage: str
    call_count: int
    succeeded_count: int
    input_tokens: int
    output_tokens: int
    total_tokens: int
    estimated_cost_usd: Decimal


@dataclass(frozen=True)
class AIStageCost:
    stage: str
    call_count: int
    succeeded_count: int
    fallback_count: int
    input_tokens: int
    output_tokens: int
    total_tokens: int
    estimated_cost_usd: Decimal


@dataclass(frozen=True)
class AICostReport:
    started_at: datetime
    ended_at: datetime
    call_count: int
    succeeded_count: int
    failed_count: int
    fallback_count: int
    fallback_rate: Decimal | None
    input_tokens: int
    output_tokens: int
    total_tokens: int
    estimated_cost_usd: Decimal
    daily: tuple[AIDailyCost, ...]
    stages: tuple[AIStageCost, ...]


class PostgreSQLAICallRecorder:
    def __init__(
        self,
        database: Database,
        *,
        spend_guard: AISpendGuardPolicy | None = None,
    ) -> None:
        self._database = database
        self._spend_guard = spend_guard

    async def begin(self, call: AICallStart) -> UUID:
        call_id = uuid4()
        async with self._database.transaction() as connection:
            reserved_cost = None
            if self._spend_guard is not None:
                await self._lock_spend_guard(connection)
                await self._check_spend_guard(connection, call)
                reserved_cost = self._spend_guard.reserve_cost(call.price)
            await connection.execute(
                ai_call_telemetry.insert().values(
                    id=call_id,
                    raw_message_id=call.raw_message_id,
                    stage=call.stage,
                    provider=call.provider,
                    requested_model=call.requested_model,
                    analyzer_version=call.analyzer_version,
                    prompt_version=call.prompt_version,
                    schema_version=call.schema_version,
                    routing_version=call.routing_version,
                    route_reason=call.route_reason,
                    provider_attempt=call.provider_attempt,
                    status="started",
                    pricing_version=call.price.pricing_version,
                    input_usd_per_million=call.price.input_usd_per_million,
                    output_usd_per_million=call.price.output_usd_per_million,
                    estimated_cost_usd=reserved_cost,
                )
            )
        return call_id

    async def finish(self, call_id: UUID, result: AICallFinish) -> None:
        estimated_cost = None
        if result.input_tokens is not None and result.output_tokens is not None:
            estimated_cost = (
                result.input_tokens * ai_call_telemetry.c.input_usd_per_million
                + result.output_tokens * ai_call_telemetry.c.output_usd_per_million
            ) / 1_000_000
        estimated_cost_value = (
            estimated_cost
            if estimated_cost is not None
            else ai_call_telemetry.c.estimated_cost_usd
        )
        async with self._database.transaction() as connection:
            updated = await connection.execute(
                ai_call_telemetry.update()
                .where(
                    ai_call_telemetry.c.id == call_id,
                    ai_call_telemetry.c.status == "started",
                )
                .values(
                    status=result.status,
                    response_model=result.response_model,
                    input_tokens=result.input_tokens,
                    output_tokens=result.output_tokens,
                    total_tokens=result.total_tokens,
                    latency_ms=result.latency_ms,
                    estimated_cost_usd=estimated_cost_value,
                    error_code=result.error_code,
                    finished_at=sa.func.now(),
                )
            )
        if updated.rowcount != 1:
            raise RuntimeError("AI telemetry call is missing or already finalized")

    async def daily_costs(
        self,
        *,
        started_at: datetime,
        ended_at: datetime,
    ) -> list[AIDailyCost]:
        day = sa.cast(ai_call_telemetry.c.started_at, sa.Date())
        succeeded = sa.func.count().filter(ai_call_telemetry.c.status == "succeeded")
        async with self._database.connect() as connection:
            rows = await connection.execute(
                sa.select(
                    day.label("day"),
                    ai_call_telemetry.c.stage,
                    sa.func.count().label("call_count"),
                    succeeded.label("succeeded_count"),
                    sa.func.coalesce(
                        sa.func.sum(ai_call_telemetry.c.input_tokens), 0
                    ).label("input_tokens"),
                    sa.func.coalesce(
                        sa.func.sum(ai_call_telemetry.c.output_tokens), 0
                    ).label("output_tokens"),
                    sa.func.coalesce(
                        sa.func.sum(ai_call_telemetry.c.total_tokens), 0
                    ).label("total_tokens"),
                    sa.func.coalesce(
                        sa.func.sum(ai_call_telemetry.c.estimated_cost_usd), 0
                    ).label("estimated_cost_usd"),
                )
                .where(
                    ai_call_telemetry.c.started_at >= started_at,
                    ai_call_telemetry.c.started_at < ended_at,
                )
                .group_by(day, ai_call_telemetry.c.stage)
                .order_by(day, ai_call_telemetry.c.stage)
            )
        return [AIDailyCost(**dict(row)) for row in rows.mappings()]

    async def cost_report(
        self,
        *,
        started_at: datetime,
        ended_at: datetime,
    ) -> AICostReport:
        _validate_window(started_at, ended_at)
        fallback_predicate = ai_call_telemetry.c.stage.like("%.fallback")
        succeeded_predicate = ai_call_telemetry.c.status == "succeeded"
        async with self._database.connect() as connection:
            row = (
                await connection.execute(
                    sa.select(
                        sa.func.count().label("call_count"),
                        sa.func.count().filter(succeeded_predicate).label(
                            "succeeded_count"
                        ),
                        sa.func.count().filter(fallback_predicate).label(
                            "fallback_count"
                        ),
                        sa.func.coalesce(
                            sa.func.sum(ai_call_telemetry.c.input_tokens), 0
                        ).label("input_tokens"),
                        sa.func.coalesce(
                            sa.func.sum(ai_call_telemetry.c.output_tokens), 0
                        ).label("output_tokens"),
                        sa.func.coalesce(
                            sa.func.sum(ai_call_telemetry.c.total_tokens), 0
                        ).label("total_tokens"),
                        sa.func.coalesce(
                            sa.func.sum(ai_call_telemetry.c.estimated_cost_usd), 0
                        ).label("estimated_cost_usd"),
                    ).where(
                        ai_call_telemetry.c.started_at >= started_at,
                        ai_call_telemetry.c.started_at < ended_at,
                    )
                )
            ).mappings().one()
            stage_rows = (
                await connection.execute(
                    sa.select(
                        ai_call_telemetry.c.stage,
                        sa.func.count().label("call_count"),
                        sa.func.count().filter(succeeded_predicate).label(
                            "succeeded_count"
                        ),
                        sa.func.count().filter(fallback_predicate).label(
                            "fallback_count"
                        ),
                        sa.func.coalesce(
                            sa.func.sum(ai_call_telemetry.c.input_tokens), 0
                        ).label("input_tokens"),
                        sa.func.coalesce(
                            sa.func.sum(ai_call_telemetry.c.output_tokens), 0
                        ).label("output_tokens"),
                        sa.func.coalesce(
                            sa.func.sum(ai_call_telemetry.c.total_tokens), 0
                        ).label("total_tokens"),
                        sa.func.coalesce(
                            sa.func.sum(ai_call_telemetry.c.estimated_cost_usd), 0
                        ).label("estimated_cost_usd"),
                    )
                    .where(
                        ai_call_telemetry.c.started_at >= started_at,
                        ai_call_telemetry.c.started_at < ended_at,
                    )
                    .group_by(ai_call_telemetry.c.stage)
                    .order_by(ai_call_telemetry.c.stage)
                )
            ).mappings().all()
        call_count = int(row["call_count"])
        succeeded_count = int(row["succeeded_count"])
        fallback_count = int(row["fallback_count"])
        return AICostReport(
            started_at=started_at,
            ended_at=ended_at,
            call_count=call_count,
            succeeded_count=succeeded_count,
            failed_count=call_count - succeeded_count,
            fallback_count=fallback_count,
            fallback_rate=(
                None
                if call_count == 0
                else (Decimal(fallback_count) / Decimal(call_count)).quantize(
                    Decimal("0.0001")
                )
            ),
            input_tokens=int(row["input_tokens"]),
            output_tokens=int(row["output_tokens"]),
            total_tokens=int(row["total_tokens"]),
            estimated_cost_usd=Decimal(row["estimated_cost_usd"]),
            daily=tuple(
                await self.daily_costs(
                    started_at=started_at,
                    ended_at=ended_at,
                )
            ),
            stages=tuple(AIStageCost(**dict(item)) for item in stage_rows),
        )

    async def _check_spend_guard(self, connection, call: AICallStart) -> None:
        policy = self._spend_guard
        if policy is None:
            return
        requested = policy.reserve_cost(call.price)
        windows = (
            ("daily", policy.daily_limit_usd, sa.func.date_trunc("day", sa.func.now())),
            (
                "monthly",
                policy.monthly_limit_usd,
                sa.func.date_trunc("month", sa.func.now()),
            ),
        )
        for name, limit, start in windows:
            if limit is None:
                continue
            used = await connection.scalar(
                sa.select(
                    sa.func.coalesce(
                        sa.func.sum(ai_call_telemetry.c.estimated_cost_usd), 0
                    )
                ).where(ai_call_telemetry.c.started_at >= start)
            )
            used_decimal = Decimal(str(used or 0))
            if used_decimal + requested > Decimal(str(limit)):
                raise AIBudgetExceeded(
                    window=name,
                    limit_usd=Decimal(str(limit)),
                    used_usd=used_decimal,
                    requested_usd=requested,
                )

    async def _lock_spend_guard(self, connection) -> None:
        await connection.execute(
            sa.select(
                sa.func.pg_advisory_xact_lock(
                    sa.func.hashtext("freelancer_bot.ai_spend_guard.v1")
                )
            )
        )


def _validate_window(started_at: datetime, ended_at: datetime) -> None:
    for value, name in ((started_at, "started_at"), (ended_at, "ended_at")):
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError(f"{name} must include a timezone")
    if ended_at <= started_at:
        raise ValueError("ended_at must be after started_at")
