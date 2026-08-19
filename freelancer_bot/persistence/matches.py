from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from uuid import UUID, uuid4

import sqlalchemy as sa
from sqlalchemy.ext.asyncio import AsyncConnection

from ..match_decisions import (
    MATCH_DECISION_ALGORITHM_VERSION,
    MATCH_DECISION_SCHEMA_VERSION,
    MatchDecisionBatch,
    MatchDecisionCode,
    MatchTraceDraft,
)
from .schema import match_evaluation_runs, match_traces


class MatchPersistenceConflict(RuntimeError):
    pass


@dataclass(frozen=True)
class MatchEvaluationRunRecord:
    id: UUID
    idempotency_key: str
    schema_version: str
    algorithm_version: str
    policy_version: str
    policy_config: dict[str, object]
    evaluated_at: datetime
    trace_count: int
    created_at: datetime


@dataclass(frozen=True)
class MatchTraceRecord:
    id: UUID
    run_id: UUID
    trace: MatchTraceDraft
    created_at: datetime


@dataclass(frozen=True)
class MatchPersistenceOutcome:
    run: MatchEvaluationRunRecord
    traces: tuple[MatchTraceRecord, ...]
    created: bool


class MatchTraceRepository:
    async def list_runs(
        self,
        connection: AsyncConnection,
        *,
        limit: int = 100,
    ) -> tuple[MatchEvaluationRunRecord, ...]:
        if not 1 <= limit <= 1000:
            raise ValueError("limit must be between 1 and 1000")
        rows = await connection.execute(
            sa.select(match_evaluation_runs)
            .order_by(
                match_evaluation_runs.c.evaluated_at.desc(),
                match_evaluation_runs.c.id,
            )
            .limit(limit)
        )
        return tuple(_run_record(row) for row in rows.mappings())

    async def list_traces(
        self,
        connection: AsyncConnection,
        *,
        run_id: UUID | None = None,
        opportunity_id: UUID | None = None,
        limit: int = 100,
    ) -> tuple[MatchTraceRecord, ...]:
        if not 1 <= limit <= 1000:
            raise ValueError("limit must be between 1 and 1000")
        statement = sa.select(match_traces)
        if run_id is not None:
            statement = statement.where(match_traces.c.run_id == run_id)
        if opportunity_id is not None:
            statement = statement.where(match_traces.c.opportunity_id == opportunity_id)
        rows = await connection.execute(
            statement.order_by(
                match_traces.c.evaluated_at.desc(),
                match_traces.c.search_profile_id,
                match_traces.c.opportunity_id,
            ).limit(limit)
        )
        return tuple(_trace_record(row) for row in rows.mappings())

    async def get(
        self,
        connection: AsyncConnection,
        trace_id: UUID,
    ) -> MatchTraceRecord | None:
        row = (
            await connection.execute(
                sa.select(match_traces).where(match_traces.c.id == trace_id)
            )
        ).mappings().one_or_none()
        return None if row is None else _trace_record(row)

    async def persist_batch(
        self,
        connection: AsyncConnection,
        batch: MatchDecisionBatch,
    ) -> MatchPersistenceOutcome:
        await connection.execute(
            sa.text("SELECT pg_advisory_xact_lock(hashtextextended(:key, 0))"),
            {"key": batch.idempotency_key},
        )
        existing = await self.get_by_idempotency_key(
            connection,
            batch.idempotency_key,
        )
        if existing is not None:
            _validate_existing(existing, batch)
            return MatchPersistenceOutcome(
                run=existing.run,
                traces=existing.traces,
                created=False,
            )

        run_id = uuid4()
        await connection.execute(
            match_evaluation_runs.insert().values(
                id=run_id,
                idempotency_key=batch.idempotency_key,
                schema_version=MATCH_DECISION_SCHEMA_VERSION,
                algorithm_version=MATCH_DECISION_ALGORITHM_VERSION,
                policy_version=batch.policy.version,
                policy_config=batch.policy_config,
                evaluated_at=batch.evaluated_at,
                trace_count=len(batch.traces),
            )
        )
        if batch.traces:
            await connection.execute(
                match_traces.insert(),
                tuple(
                    _trace_values(run_id, trace)
                    for trace in batch.traces
                ),
            )
        outcome = await self.get_by_idempotency_key(
            connection,
            batch.idempotency_key,
        )
        if outcome is None:
            raise MatchPersistenceConflict("persisted matching batch disappeared")
        return MatchPersistenceOutcome(
            run=outcome.run,
            traces=outcome.traces,
            created=True,
        )

    async def get_by_idempotency_key(
        self,
        connection: AsyncConnection,
        idempotency_key: str,
    ) -> MatchPersistenceOutcome | None:
        run_row = (
            await connection.execute(
                sa.select(match_evaluation_runs).where(
                    match_evaluation_runs.c.idempotency_key == idempotency_key
                )
            )
        ).mappings().one_or_none()
        if run_row is None:
            return None
        trace_rows = (
            await connection.execute(
                sa.select(match_traces)
                .where(match_traces.c.run_id == run_row["id"])
                .order_by(
                    match_traces.c.search_profile_id,
                    match_traces.c.rank.asc().nulls_last(),
                    match_traces.c.opportunity_id,
                )
            )
        ).mappings().all()
        return MatchPersistenceOutcome(
            run=_run_record(run_row),
            traces=tuple(_trace_record(row) for row in trace_rows),
            created=False,
        )

    async def list_eligible_for_profile(
        self,
        connection: AsyncConnection,
        *,
        run_id: UUID,
        search_profile_id: UUID,
    ) -> tuple[MatchTraceRecord, ...]:
        rows = (
            await connection.execute(
                sa.select(match_traces)
                .where(
                    match_traces.c.run_id == run_id,
                    match_traces.c.search_profile_id == search_profile_id,
                    match_traces.c.eligible.is_(True),
                )
                .order_by(match_traces.c.rank, match_traces.c.opportunity_id)
            )
        ).mappings().all()
        return tuple(_trace_record(row) for row in rows)

    async def list_eligible_for_run(
        self,
        connection: AsyncConnection,
        run_id: UUID,
    ) -> tuple[MatchTraceRecord, ...]:
        rows = (
            await connection.execute(
                sa.select(match_traces)
                .where(
                    match_traces.c.run_id == run_id,
                    match_traces.c.eligible.is_(True),
                )
                .order_by(
                    match_traces.c.search_profile_id,
                    match_traces.c.rank,
                    match_traces.c.opportunity_id,
                )
            )
        ).mappings().all()
        return tuple(_trace_record(row) for row in rows)


def _trace_values(run_id: UUID, trace: MatchTraceDraft) -> dict[str, object]:
    return {
        "id": uuid4(),
        "run_id": run_id,
        "opportunity_id": trace.opportunity_id,
        "search_profile_id": trace.search_profile_id,
        "profile_revision": trace.profile_revision,
        "profile_schema_version": trace.profile_schema_version,
        "preferences_schema_version": trace.preferences_schema_version,
        "input_sha256": trace.input_sha256,
        "opportunity_lifecycle_status": trace.opportunity_lifecycle_status,
        "opportunity_last_seen_at": trace.opportunity_last_seen_at,
        "filter_version": trace.filter_version,
        "hard_filter_eligible": trace.hard_filter_eligible,
        "hard_filter_reasons": list(trace.hard_filter_reasons),
        "nonblocking_unknowns": list(trace.nonblocking_unknowns),
        "structured_scoring_version": trace.structured_scoring_version,
        "structured_policy_version": trace.structured_policy_version,
        "structured_components": list(trace.structured_components),
        "user_relevance_score": trace.user_relevance_score,
        "structured_score": trace.structured_score,
        "semantic_matching_version": trace.semantic_matching_version,
        "semantic_policy_version": trace.semantic_policy_version,
        "semantic_status": trace.semantic_status,
        "semantic_degraded_reason": trace.semantic_degraded_reason,
        "semantic_similarity": trace.semantic_similarity,
        "semantic_provider": trace.semantic_provider,
        "semantic_model": trace.semantic_model,
        "semantic_model_version": trace.semantic_model_version,
        "opportunity_representation_sha256": (
            trace.opportunity_representation_sha256
        ),
        "profile_representation_sha256": trace.profile_representation_sha256,
        "combined_relevance_score": trace.combined_relevance_score,
        "opportunity_quality_score": trace.opportunity_quality_score,
        "source_quality_score": trace.source_quality_score,
        "source_quality_snapshot_id": trace.source_quality_snapshot_id,
        "red_flag_penalty": trace.red_flag_penalty,
        "base_combined_score": trace.base_combined_score,
        "freshness_age_seconds": trace.freshness_age_seconds,
        "freshness_score": trace.freshness_score,
        "final_rank_score": trace.final_rank_score,
        "minimum_relevance_threshold": trace.minimum_relevance_threshold,
        "minimum_rank_score_threshold": trace.minimum_rank_score_threshold,
        "decision_code": trace.decision_code.value,
        "eligible": trace.eligible,
        "rank": trace.rank,
        "decision_schema_version": trace.decision_schema_version,
        "decision_algorithm_version": trace.decision_algorithm_version,
        "decision_policy_version": trace.decision_policy_version,
        "evaluated_at": trace.evaluated_at,
    }


def _run_record(row) -> MatchEvaluationRunRecord:
    return MatchEvaluationRunRecord(
        id=row["id"],
        idempotency_key=row["idempotency_key"],
        schema_version=row["schema_version"],
        algorithm_version=row["algorithm_version"],
        policy_version=row["policy_version"],
        policy_config=dict(row["policy_config"]),
        evaluated_at=row["evaluated_at"],
        trace_count=row["trace_count"],
        created_at=row["created_at"],
    )


def _trace_record(row) -> MatchTraceRecord:
    return MatchTraceRecord(
        id=row["id"],
        run_id=row["run_id"],
        trace=MatchTraceDraft(
            opportunity_id=row["opportunity_id"],
            search_profile_id=row["search_profile_id"],
            profile_revision=row["profile_revision"],
            profile_schema_version=row["profile_schema_version"],
            preferences_schema_version=row["preferences_schema_version"],
            opportunity_lifecycle_status=row["opportunity_lifecycle_status"],
            opportunity_last_seen_at=row["opportunity_last_seen_at"],
            filter_version=row["filter_version"],
            hard_filter_eligible=row["hard_filter_eligible"],
            hard_filter_reasons=tuple(row["hard_filter_reasons"]),
            nonblocking_unknowns=tuple(row["nonblocking_unknowns"]),
            structured_scoring_version=row["structured_scoring_version"],
            structured_policy_version=row["structured_policy_version"],
            structured_components=tuple(row["structured_components"]),
            user_relevance_score=_decimal(row["user_relevance_score"]),
            structured_score=_decimal(row["structured_score"]),
            semantic_matching_version=row["semantic_matching_version"],
            semantic_policy_version=row["semantic_policy_version"],
            semantic_status=row["semantic_status"],
            semantic_degraded_reason=row["semantic_degraded_reason"],
            semantic_similarity=_decimal(row["semantic_similarity"]),
            semantic_provider=row["semantic_provider"],
            semantic_model=row["semantic_model"],
            semantic_model_version=row["semantic_model_version"],
            opportunity_representation_sha256=(
                row["opportunity_representation_sha256"]
            ),
            profile_representation_sha256=row["profile_representation_sha256"],
            combined_relevance_score=_decimal(row["combined_relevance_score"]),
            opportunity_quality_score=_decimal(row["opportunity_quality_score"]),
            source_quality_score=_decimal(row["source_quality_score"]),
            source_quality_snapshot_id=row["source_quality_snapshot_id"],
            red_flag_penalty=_decimal(row["red_flag_penalty"]),
            base_combined_score=_decimal(row["base_combined_score"]),
            freshness_age_seconds=row["freshness_age_seconds"],
            freshness_score=row["freshness_score"],
            final_rank_score=_decimal(row["final_rank_score"]),
            minimum_relevance_threshold=row["minimum_relevance_threshold"],
            minimum_rank_score_threshold=row["minimum_rank_score_threshold"],
            decision_code=MatchDecisionCode(row["decision_code"]),
            eligible=row["eligible"],
            rank=row["rank"],
            decision_schema_version=row["decision_schema_version"],
            decision_algorithm_version=row["decision_algorithm_version"],
            decision_policy_version=row["decision_policy_version"],
            evaluated_at=row["evaluated_at"],
            input_sha256=row["input_sha256"],
        ),
        created_at=row["created_at"],
    )


def _validate_existing(
    existing: MatchPersistenceOutcome,
    batch: MatchDecisionBatch,
) -> None:
    if existing.run.trace_count != len(batch.traces):
        raise MatchPersistenceConflict(
            "matching idempotency key has another trace count"
        )
    if existing.run.policy_config != batch.policy_config:
        raise MatchPersistenceConflict(
            "matching idempotency key has another policy configuration"
        )
    existing_hashes = sorted(record.trace.input_sha256 for record in existing.traces)
    requested_hashes = sorted(trace.input_sha256 for trace in batch.traces)
    if existing_hashes != requested_hashes:
        raise MatchPersistenceConflict("matching idempotency key has different inputs")


def _decimal(value) -> Decimal | None:
    return None if value is None else Decimal(value)
