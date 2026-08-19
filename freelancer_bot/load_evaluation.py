from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from decimal import Decimal, ROUND_HALF_UP
import json
from math import ceil
from pathlib import Path
from time import perf_counter
from typing import Any, Literal
from hashlib import sha256
import re


LOAD_EVALUATION_SCHEMA_VERSION = "load-evaluation.v1"
LOAD_EVALUATION_RUNNER_VERSION = "load-evaluation-runner.v1"
LOAD_STAGE_NAMES = ("ingestion", "matching", "delivery")
_SAFE_VERSION = re.compile(r"^[a-z0-9][a-z0-9._-]{0,127}$")
_MONEY_QUANTUM = Decimal("0.000000001")


class LoadEvaluationError(ValueError):
    """The load/cost harness cannot produce trustworthy operational evidence."""


@dataclass(frozen=True)
class LoadWorkload:
    """Versioned load inputs, separate from any production quality dataset."""

    dataset_version: str
    message_count: int
    profile_count: int
    delivery_count: int
    daily_message_projection: int
    monthly_message_projection: int
    stage_concurrency: Mapping[str, int] = field(
        default_factory=lambda: {
            "ingestion": 1,
            "matching": 1,
            "delivery": 1,
        }
    )
    evidence_kind: Literal["test_fixture", "captured"] = "test_fixture"
    evidence_ref: str = "synthetic-load-fixture"

    def __post_init__(self) -> None:
        _safe_identifier(self.dataset_version, "dataset_version")
        for name, value in (
            ("message_count", self.message_count),
            ("profile_count", self.profile_count),
            ("delivery_count", self.delivery_count),
            ("daily_message_projection", self.daily_message_projection),
            ("monthly_message_projection", self.monthly_message_projection),
        ):
            if not isinstance(value, int) or value <= 0:
                raise LoadEvaluationError(f"{name} must be a positive integer")
        if self.message_count > 1_000_000:
            raise LoadEvaluationError("message_count exceeds the bounded load limit")
        if self.profile_count > 100_000:
            raise LoadEvaluationError("profile_count exceeds the bounded load limit")
        if self.delivery_count > 1_000_000:
            raise LoadEvaluationError("delivery_count exceeds the bounded load limit")
        if self.evidence_kind not in {"test_fixture", "captured"}:
            raise LoadEvaluationError("unsupported evidence_kind")
        if not isinstance(self.evidence_ref, str) or not self.evidence_ref.strip():
            raise LoadEvaluationError("evidence_ref must not be blank")
        normalized = dict(self.stage_concurrency)
        if set(normalized) != set(LOAD_STAGE_NAMES):
            raise LoadEvaluationError(
                "stage_concurrency must contain ingestion, matching and delivery"
            )
        for stage, concurrency in normalized.items():
            if not isinstance(concurrency, int) or concurrency <= 0:
                raise LoadEvaluationError(
                    f"stage concurrency for {stage} must be positive"
                )
            if concurrency > 1_000:
                raise LoadEvaluationError(
                    f"stage concurrency for {stage} exceeds the bounded limit"
                )

    @property
    def stage_counts(self) -> dict[str, int]:
        return {
            "ingestion": self.message_count,
            "matching": self.message_count * self.profile_count,
            "delivery": self.delivery_count,
        }

    @property
    def fingerprint(self) -> str:
        return _fingerprint(self.as_dict())

    def as_dict(self) -> dict[str, object]:
        return {
            "dataset_version": self.dataset_version,
            "message_count": self.message_count,
            "profile_count": self.profile_count,
            "delivery_count": self.delivery_count,
            "daily_message_projection": self.daily_message_projection,
            "monthly_message_projection": self.monthly_message_projection,
            "stage_concurrency": {
                stage: self.stage_concurrency[stage] for stage in LOAD_STAGE_NAMES
            },
            "evidence_kind": self.evidence_kind,
            "evidence_ref": self.evidence_ref,
        }


@dataclass(frozen=True)
class AICallObservation:
    """One measured AI attempt, including failed attempts and fallback calls."""

    stage: str
    route: Literal["primary", "fallback"]
    provider: str
    requested_model: str
    status: Literal["succeeded", "request_failed", "invalid_output"]
    latency_ms: int
    input_tokens: int
    output_tokens: int
    estimated_cost_usd: Decimal
    quality_pass: bool | None = None

    def __post_init__(self) -> None:
        for name, value in (
            ("stage", self.stage),
            ("provider", self.provider),
            ("requested_model", self.requested_model),
        ):
            if not isinstance(value, str) or not value.strip():
                raise LoadEvaluationError(f"{name} must not be blank")
        if self.route not in {"primary", "fallback"}:
            raise LoadEvaluationError("route must be primary or fallback")
        if self.status not in {"succeeded", "request_failed", "invalid_output"}:
            raise LoadEvaluationError("unsupported AI call status")
        for name, value in (
            ("latency_ms", self.latency_ms),
            ("input_tokens", self.input_tokens),
            ("output_tokens", self.output_tokens),
        ):
            if not isinstance(value, int) or value < 0:
                raise LoadEvaluationError(f"{name} must be a nonnegative integer")
        cost = _money(self.estimated_cost_usd, "estimated_cost_usd")
        if cost < 0:
            raise LoadEvaluationError("estimated_cost_usd must be nonnegative")
        if self.quality_pass is not None and not isinstance(self.quality_pass, bool):
            raise LoadEvaluationError("quality_pass must be bool or None")

    @property
    def total_tokens(self) -> int:
        return self.input_tokens + self.output_tokens

    def as_dict(self) -> dict[str, object]:
        return {
            "stage": self.stage,
            "route": self.route,
            "provider": self.provider,
            "requested_model": self.requested_model,
            "status": self.status,
            "latency_ms": self.latency_ms,
            "input_tokens": self.input_tokens,
            "output_tokens": self.output_tokens,
            "total_tokens": self.total_tokens,
            "estimated_cost_usd": str(_money(self.estimated_cost_usd, "cost")),
            "quality_pass": self.quality_pass,
        }


@dataclass(frozen=True)
class AIFallbackCostImpact:
    primary_call_count: int
    fallback_call_count: int
    primary_success_count: int
    fallback_success_count: int
    fallback_rate: Decimal | None
    primary_quality_rate: Decimal | None
    fallback_quality_rate: Decimal | None
    quality_delta: Decimal | None
    primary_cost_usd: Decimal
    fallback_cost_usd: Decimal
    fallback_cost_share: Decimal | None

    def as_dict(self) -> dict[str, object]:
        return {
            "primary_call_count": self.primary_call_count,
            "fallback_call_count": self.fallback_call_count,
            "primary_success_count": self.primary_success_count,
            "fallback_success_count": self.fallback_success_count,
            "fallback_rate": _optional_money_ratio(self.fallback_rate),
            "primary_quality_rate": _optional_money_ratio(self.primary_quality_rate),
            "fallback_quality_rate": _optional_money_ratio(self.fallback_quality_rate),
            "quality_delta": _optional_money_ratio(self.quality_delta),
            "primary_cost_usd": str(self.primary_cost_usd),
            "fallback_cost_usd": str(self.fallback_cost_usd),
            "fallback_cost_share": _optional_money_ratio(self.fallback_cost_share),
        }


@dataclass(frozen=True)
class AICostProjection:
    observed_call_count: int
    succeeded_call_count: int
    failed_call_count: int
    input_tokens: int
    output_tokens: int
    total_tokens: int
    observed_cost_usd: Decimal
    projected_daily_cost_usd: Decimal | None
    projected_monthly_cost_usd: Decimal | None
    cost_per_message_usd: Decimal | None
    fallback: AIFallbackCostImpact
    daily_spend_limit_usd: Decimal | None = None
    monthly_spend_limit_usd: Decimal | None = None
    daily_guard_status: Literal["not_configured", "within_limit", "exceeded"] = (
        "not_configured"
    )
    monthly_guard_status: Literal["not_configured", "within_limit", "exceeded"] = (
        "not_configured"
    )

    def as_dict(self) -> dict[str, object]:
        return {
            "observed_call_count": self.observed_call_count,
            "succeeded_call_count": self.succeeded_call_count,
            "failed_call_count": self.failed_call_count,
            "input_tokens": self.input_tokens,
            "output_tokens": self.output_tokens,
            "total_tokens": self.total_tokens,
            "observed_cost_usd": str(self.observed_cost_usd),
            "projected_daily_cost_usd": _optional_money(self.projected_daily_cost_usd),
            "projected_monthly_cost_usd": _optional_money(
                self.projected_monthly_cost_usd
            ),
            "cost_per_message_usd": _optional_money(self.cost_per_message_usd),
            "fallback": self.fallback.as_dict(),
            "daily_spend_limit_usd": _optional_money(self.daily_spend_limit_usd),
            "monthly_spend_limit_usd": _optional_money(self.monthly_spend_limit_usd),
            "daily_guard_status": self.daily_guard_status,
            "monthly_guard_status": self.monthly_guard_status,
        }


@dataclass(frozen=True)
class LoadStageResult:
    stage: str
    requested_count: int
    completed_count: int
    failed_count: int
    initial_backlog: int
    max_backlog: int
    final_backlog: int
    concurrency: int
    elapsed_ms: int
    p50_latency_ms: int | None
    p95_latency_ms: int | None
    throughput_per_second: Decimal
    failure_types: tuple[str, ...]

    def as_dict(self) -> dict[str, object]:
        return {
            "stage": self.stage,
            "requested_count": self.requested_count,
            "completed_count": self.completed_count,
            "failed_count": self.failed_count,
            "initial_backlog": self.initial_backlog,
            "max_backlog": self.max_backlog,
            "final_backlog": self.final_backlog,
            "concurrency": self.concurrency,
            "elapsed_ms": self.elapsed_ms,
            "p50_latency_ms": self.p50_latency_ms,
            "p95_latency_ms": self.p95_latency_ms,
            "throughput_per_second": str(self.throughput_per_second),
            "failure_types": list(self.failure_types),
        }


@dataclass(frozen=True)
class LoadEvaluationReport:
    schema_version: str
    runner_version: str
    dataset_version: str
    dataset_kind: Literal["test_fixture", "captured"]
    evidence_ref: str
    quality_claim_allowed: bool
    workload_fingerprint: str
    workload: LoadWorkload
    stages: tuple[LoadStageResult, ...]
    ai_cost: AICostProjection
    model_call_count: int
    user_specific_llm_calls: int
    notes: tuple[str, ...]

    def __post_init__(self) -> None:
        if self.schema_version != LOAD_EVALUATION_SCHEMA_VERSION:
            raise LoadEvaluationError("unsupported load evaluation schema")
        if self.runner_version != LOAD_EVALUATION_RUNNER_VERSION:
            raise LoadEvaluationError("unsupported load evaluation runner")
        if self.dataset_kind != self.workload.evidence_kind:
            raise LoadEvaluationError("dataset kind must match workload provenance")
        if self.workload_fingerprint != self.workload.fingerprint:
            raise LoadEvaluationError("workload fingerprint does not match inputs")
        if self.quality_claim_allowed:
            raise LoadEvaluationError(
                "load evidence cannot authorize a production quality claim"
            )
        if tuple(stage.stage for stage in self.stages) != LOAD_STAGE_NAMES:
            raise LoadEvaluationError("load report must contain all ordered pipeline stages")
        if self.model_call_count != self.ai_cost.observed_call_count:
            raise LoadEvaluationError("model_call_count must match AI observations")
        if self.user_specific_llm_calls < 0:
            raise LoadEvaluationError("user_specific_llm_calls cannot be negative")

    @property
    def fingerprint(self) -> str:
        return _fingerprint(self.as_dict())

    def as_dict(self) -> dict[str, object]:
        return {
            "schema_version": self.schema_version,
            "runner_version": self.runner_version,
            "dataset_version": self.dataset_version,
            "dataset_kind": self.dataset_kind,
            "evidence_ref": self.evidence_ref,
            "quality_claim_allowed": self.quality_claim_allowed,
            "workload_fingerprint": self.workload_fingerprint,
            "workload": self.workload.as_dict(),
            "stages": [stage.as_dict() for stage in self.stages],
            "ai_cost": self.ai_cost.as_dict(),
            "model_call_count": self.model_call_count,
            "user_specific_llm_calls": self.user_specific_llm_calls,
            "notes": list(self.notes),
        }

    def write_json(self, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(
            json.dumps(self.as_dict(), ensure_ascii=False, indent=2, sort_keys=True)
            + "\n",
            encoding="utf-8",
        )


LoadStageHandler = Callable[
    [int], Awaitable[AICallObservation | Iterable[AICallObservation] | None]
]


async def run_load_evaluation(
    workload: LoadWorkload,
    handlers: Mapping[str, LoadStageHandler],
    *,
    ai_calls: Sequence[AICallObservation] = (),
    daily_spend_limit_usd: Decimal | None = None,
    monthly_spend_limit_usd: Decimal | None = None,
    user_specific_llm_calls: int = 0,
) -> LoadEvaluationReport:
    """Exercise ingestion, matching and delivery with bounded concurrency.

    This runner measures the supplied stage handlers and never writes product,
    evaluation or AI telemetry. Callers may use real test doubles for the V2
    repositories, but the resulting report remains operational evidence rather
    than production-quality evidence.
    """

    if set(handlers) != set(LOAD_STAGE_NAMES):
        raise LoadEvaluationError(
            "handlers must contain ingestion, matching and delivery only"
        )
    if user_specific_llm_calls < 0:
        raise LoadEvaluationError("user_specific_llm_calls cannot be negative")

    observations = list(ai_calls)
    stages: list[LoadStageResult] = []
    for stage in LOAD_STAGE_NAMES:
        result, stage_observations = await _run_stage(
            stage,
            workload.stage_counts[stage],
            workload.stage_concurrency[stage],
            handlers[stage],
        )
        stages.append(result)
        observations.extend(stage_observations)

    projection = _build_cost_projection(
        tuple(observations),
        workload,
        daily_spend_limit_usd=daily_spend_limit_usd,
        monthly_spend_limit_usd=monthly_spend_limit_usd,
    )
    return LoadEvaluationReport(
        schema_version=LOAD_EVALUATION_SCHEMA_VERSION,
        runner_version=LOAD_EVALUATION_RUNNER_VERSION,
        dataset_version=workload.dataset_version,
        dataset_kind=workload.evidence_kind,
        evidence_ref=workload.evidence_ref,
        quality_claim_allowed=False,
        workload_fingerprint=workload.fingerprint,
        workload=workload,
        stages=tuple(stages),
        ai_cost=projection,
        model_call_count=len(observations),
        user_specific_llm_calls=user_specific_llm_calls,
        notes=(
            "Operational load/cost evidence only; it is not a production quality claim.",
            "Synthetic/test-fixture provenance must not be promoted to release evidence.",
        ),
    )


def load_load_evaluation_report(path: Path) -> LoadEvaluationReport:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if payload.get("schema_version") != LOAD_EVALUATION_SCHEMA_VERSION:
        raise LoadEvaluationError("unsupported load evaluation schema")
    workload_payload = payload["workload"]
    workload = LoadWorkload(
        dataset_version=workload_payload["dataset_version"],
        message_count=workload_payload["message_count"],
        profile_count=workload_payload["profile_count"],
        delivery_count=workload_payload["delivery_count"],
        daily_message_projection=workload_payload["daily_message_projection"],
        monthly_message_projection=workload_payload["monthly_message_projection"],
        stage_concurrency=workload_payload["stage_concurrency"],
        evidence_kind=workload_payload["evidence_kind"],
        evidence_ref=workload_payload["evidence_ref"],
    )
    stages = tuple(
        LoadStageResult(
            stage=item["stage"],
            requested_count=item["requested_count"],
            completed_count=item["completed_count"],
            failed_count=item["failed_count"],
            initial_backlog=item["initial_backlog"],
            max_backlog=item["max_backlog"],
            final_backlog=item["final_backlog"],
            concurrency=item["concurrency"],
            elapsed_ms=item["elapsed_ms"],
            p50_latency_ms=item["p50_latency_ms"],
            p95_latency_ms=item["p95_latency_ms"],
            throughput_per_second=Decimal(item["throughput_per_second"]),
            failure_types=tuple(item["failure_types"]),
        )
        for item in payload["stages"]
    )
    fallback_payload = payload["ai_cost"]["fallback"]
    fallback = AIFallbackCostImpact(
        primary_call_count=fallback_payload["primary_call_count"],
        fallback_call_count=fallback_payload["fallback_call_count"],
        primary_success_count=fallback_payload["primary_success_count"],
        fallback_success_count=fallback_payload["fallback_success_count"],
        fallback_rate=_optional_decimal(fallback_payload["fallback_rate"]),
        primary_quality_rate=_optional_decimal(
            fallback_payload["primary_quality_rate"]
        ),
        fallback_quality_rate=_optional_decimal(
            fallback_payload["fallback_quality_rate"]
        ),
        quality_delta=_optional_decimal(fallback_payload["quality_delta"]),
        primary_cost_usd=Decimal(fallback_payload["primary_cost_usd"]),
        fallback_cost_usd=Decimal(fallback_payload["fallback_cost_usd"]),
        fallback_cost_share=_optional_decimal(fallback_payload["fallback_cost_share"]),
    )
    cost_payload = payload["ai_cost"]
    ai_cost = AICostProjection(
        observed_call_count=cost_payload["observed_call_count"],
        succeeded_call_count=cost_payload["succeeded_call_count"],
        failed_call_count=cost_payload["failed_call_count"],
        input_tokens=cost_payload["input_tokens"],
        output_tokens=cost_payload["output_tokens"],
        total_tokens=cost_payload["total_tokens"],
        observed_cost_usd=Decimal(cost_payload["observed_cost_usd"]),
        projected_daily_cost_usd=_optional_decimal(
            cost_payload["projected_daily_cost_usd"]
        ),
        projected_monthly_cost_usd=_optional_decimal(
            cost_payload["projected_monthly_cost_usd"]
        ),
        cost_per_message_usd=_optional_decimal(cost_payload["cost_per_message_usd"]),
        fallback=fallback,
        daily_spend_limit_usd=_optional_decimal(cost_payload["daily_spend_limit_usd"]),
        monthly_spend_limit_usd=_optional_decimal(
            cost_payload["monthly_spend_limit_usd"]
        ),
        daily_guard_status=cost_payload["daily_guard_status"],
        monthly_guard_status=cost_payload["monthly_guard_status"],
    )
    return LoadEvaluationReport(
        schema_version=payload["schema_version"],
        runner_version=payload["runner_version"],
        dataset_version=payload["dataset_version"],
        dataset_kind=payload["dataset_kind"],
        evidence_ref=payload["evidence_ref"],
        quality_claim_allowed=payload["quality_claim_allowed"],
        workload_fingerprint=payload["workload_fingerprint"],
        workload=workload,
        stages=stages,
        ai_cost=ai_cost,
        model_call_count=payload["model_call_count"],
        user_specific_llm_calls=payload["user_specific_llm_calls"],
        notes=tuple(payload["notes"]),
    )


async def _run_stage(
    stage: str,
    requested_count: int,
    concurrency: int,
    handler: LoadStageHandler,
) -> tuple[LoadStageResult, tuple[AICallObservation, ...]]:
    queue: asyncio.Queue[int] = asyncio.Queue()
    for index in range(requested_count):
        queue.put_nowait(index)
    initial_backlog = queue.qsize()
    max_backlog = initial_backlog
    completed = 0
    failed = 0
    failure_types: set[str] = set()
    latencies: list[int] = []
    observations: list[AICallObservation] = []
    started = perf_counter()

    async def worker() -> None:
        nonlocal completed, failed, max_backlog
        while True:
            try:
                index = queue.get_nowait()
            except asyncio.QueueEmpty:
                return
            max_backlog = max(max_backlog, queue.qsize() + 1)
            item_started = perf_counter()
            try:
                output = await handler(index)
                observations.extend(_observations(output))
                completed += 1
            except Exception as error:
                failed += 1
                failure_types.add(type(error).__name__)
            finally:
                latencies.append(max(0, round((perf_counter() - item_started) * 1000)))
                queue.task_done()

    await asyncio.gather(*(worker() for _ in range(min(concurrency, requested_count))))
    elapsed_ms = max(0, round((perf_counter() - started) * 1000))
    elapsed_seconds = max(elapsed_ms / 1000, 0.000001)
    return (
        LoadStageResult(
            stage=stage,
            requested_count=requested_count,
            completed_count=completed,
            failed_count=failed,
            initial_backlog=initial_backlog,
            max_backlog=max_backlog,
            final_backlog=queue.qsize(),
            concurrency=concurrency,
            elapsed_ms=elapsed_ms,
            p50_latency_ms=_percentile(latencies, 0.50),
            p95_latency_ms=_percentile(latencies, 0.95),
            throughput_per_second=_decimal_rate(completed, elapsed_seconds),
            failure_types=tuple(sorted(failure_types)),
        ),
        tuple(observations),
    )


def _build_cost_projection(
    calls: Sequence[AICallObservation],
    workload: LoadWorkload,
    *,
    daily_spend_limit_usd: Decimal | None,
    monthly_spend_limit_usd: Decimal | None,
) -> AICostProjection:
    total_cost = _money(sum((call.estimated_cost_usd for call in calls), Decimal("0")), "cost")
    primary = tuple(call for call in calls if call.route == "primary")
    fallback = tuple(call for call in calls if call.route == "fallback")
    succeeded = tuple(call for call in calls if call.status == "succeeded")
    fallback_success = tuple(call for call in fallback if call.status == "succeeded")
    primary_success = tuple(call for call in primary if call.status == "succeeded")
    primary_quality = _quality_rate(primary_success)
    fallback_quality = _quality_rate(fallback_success)
    fallback_cost = _money(
        sum((call.estimated_cost_usd for call in fallback), Decimal("0")),
        "fallback cost",
    )
    primary_cost = _money(
        sum((call.estimated_cost_usd for call in primary), Decimal("0")),
        "primary cost",
    )
    fallback_rate = _ratio(len(fallback), len(calls))
    fallback_cost_share = _ratio(fallback_cost, total_cost)
    cost_per_message = _money(total_cost / Decimal(workload.message_count), "cost per message")
    daily_projection = _money(
        cost_per_message * Decimal(workload.daily_message_projection),
        "daily projection",
    )
    monthly_projection = _money(
        cost_per_message * Decimal(workload.monthly_message_projection),
        "monthly projection",
    )
    return AICostProjection(
        observed_call_count=len(calls),
        succeeded_call_count=len(succeeded),
        failed_call_count=len(calls) - len(succeeded),
        input_tokens=sum(call.input_tokens for call in calls),
        output_tokens=sum(call.output_tokens for call in calls),
        total_tokens=sum(call.total_tokens for call in calls),
        observed_cost_usd=total_cost,
        projected_daily_cost_usd=daily_projection,
        projected_monthly_cost_usd=monthly_projection,
        cost_per_message_usd=cost_per_message,
        fallback=AIFallbackCostImpact(
            primary_call_count=len(primary),
            fallback_call_count=len(fallback),
            primary_success_count=len(primary_success),
            fallback_success_count=len(fallback_success),
            fallback_rate=fallback_rate,
            primary_quality_rate=primary_quality,
            fallback_quality_rate=fallback_quality,
            quality_delta=(
                None
                if primary_quality is None or fallback_quality is None
                else _money(fallback_quality - primary_quality, "quality delta")
            ),
            primary_cost_usd=primary_cost,
            fallback_cost_usd=fallback_cost,
            fallback_cost_share=fallback_cost_share,
        ),
        daily_spend_limit_usd=(
            None
            if daily_spend_limit_usd is None
            else _money(daily_spend_limit_usd, "daily spend limit")
        ),
        monthly_spend_limit_usd=(
            None
            if monthly_spend_limit_usd is None
            else _money(monthly_spend_limit_usd, "monthly spend limit")
        ),
        daily_guard_status=_guard_status(daily_projection, daily_spend_limit_usd),
        monthly_guard_status=_guard_status(monthly_projection, monthly_spend_limit_usd),
    )


def _observations(
    value: AICallObservation | Iterable[AICallObservation] | None,
) -> tuple[AICallObservation, ...]:
    if value is None:
        return ()
    if isinstance(value, AICallObservation):
        return (value,)
    values = tuple(value)
    if any(not isinstance(item, AICallObservation) for item in values):
        raise LoadEvaluationError("stage handlers may return only AI observations")
    return values


def _quality_rate(calls: Sequence[AICallObservation]) -> Decimal | None:
    labelled = tuple(call for call in calls if call.quality_pass is not None)
    if not labelled:
        return None
    return _ratio(
        sum(call.quality_pass is True for call in labelled),
        len(labelled),
    )


def _guard_status(
    projection: Decimal | None,
    limit: Decimal | None,
) -> Literal["not_configured", "within_limit", "exceeded"]:
    if limit is None:
        return "not_configured"
    if projection is None or projection <= limit:
        return "within_limit"
    return "exceeded"


def _percentile(values: Sequence[int], percentile: float) -> int | None:
    if not values:
        return None
    ordered = sorted(values)
    index = min(len(ordered) - 1, max(0, ceil(percentile * len(ordered)) - 1))
    return ordered[index]


def _decimal_rate(numerator: int, denominator: float) -> Decimal:
    return Decimal(numerator / denominator).quantize(Decimal("0.0001"), rounding=ROUND_HALF_UP)


def _ratio(numerator: int | Decimal, denominator: int | Decimal) -> Decimal | None:
    if denominator == 0:
        return None
    return (Decimal(numerator) / Decimal(denominator)).quantize(
        Decimal("0.0001"), rounding=ROUND_HALF_UP
    )


def _money(value: Decimal | int | float | str, name: str) -> Decimal:
    try:
        normalized = Decimal(str(value))
    except (ArithmeticError, ValueError):
        raise LoadEvaluationError(f"{name} must be a finite decimal") from None
    if not normalized.is_finite():
        raise LoadEvaluationError(f"{name} must be a finite decimal")
    return normalized.quantize(_MONEY_QUANTUM, rounding=ROUND_HALF_UP)


def _optional_money(value: Decimal | None) -> str | None:
    return None if value is None else str(_money(value, "money"))


def _optional_money_ratio(value: Decimal | None) -> str | None:
    return None if value is None else str(value)


def _optional_decimal(value: object) -> Decimal | None:
    return None if value is None else Decimal(str(value))


def _safe_identifier(value: object, name: str) -> None:
    if not isinstance(value, str) or not _SAFE_VERSION.fullmatch(value):
        raise LoadEvaluationError(f"{name} must be a safe version identifier")


def _fingerprint(value: Mapping[str, object]) -> str:
    canonical = json.dumps(value, ensure_ascii=True, sort_keys=True, separators=(",", ":"))
    return sha256(canonical.encode("utf-8")).hexdigest()
