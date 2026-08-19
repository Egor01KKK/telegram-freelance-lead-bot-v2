from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any
from uuid import UUID, uuid4

import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncConnection

from .schema import message_prefilter_results, opportunity_analysis_cache


class PrefilterResultConflict(RuntimeError):
    pass


class AnalysisCacheConflict(RuntimeError):
    pass


@dataclass(frozen=True)
class PrefilterResultRecord:
    id: UUID
    raw_message_id: UUID
    parent_raw_message_id: UUID | None
    analysis_job_id: UUID | None
    canonical_prefilter_result_id: UUID | None
    schema_version: str
    decision: str
    reason_codes: tuple[str, ...]
    normalized_content: str | None
    normalized_content_sha256: str | None
    analysis_input_sha256: str | None
    analyzer_version: str | None
    analysis_schema_version: str | None
    dedup_relation: str | None
    dedup_window_seconds: int | None
    created_at: datetime


@dataclass(frozen=True)
class PrefilterWriteOutcome:
    result: PrefilterResultRecord
    created: bool


@dataclass(frozen=True)
class AnalysisCacheRecord:
    id: UUID
    normalized_content: str
    normalized_content_sha256: str
    analysis_input_sha256: str
    analyzer_version: str
    analysis_schema_version: str
    result: Mapping[str, Any]
    created_at: datetime


@dataclass(frozen=True)
class AnalysisCacheWriteOutcome:
    entry: AnalysisCacheRecord
    created: bool


class MessagePrefilterRepository:
    async def get_for_raw(
        self,
        connection: AsyncConnection,
        *,
        raw_message_id: UUID,
        schema_version: str,
    ) -> PrefilterResultRecord | None:
        row = (
            await connection.execute(
                sa.select(message_prefilter_results).where(
                    message_prefilter_results.c.raw_message_id == raw_message_id,
                    message_prefilter_results.c.schema_version == schema_version,
                )
            )
        ).mappings().one_or_none()
        return None if row is None else _record(row)

    async def get_canonical_for_analysis_job(
        self,
        connection: AsyncConnection,
        analysis_job_id: UUID,
    ) -> PrefilterResultRecord | None:
        row = (
            await connection.execute(
                sa.select(message_prefilter_results).where(
                    message_prefilter_results.c.analysis_job_id == analysis_job_id,
                    message_prefilter_results.c.dedup_relation == "canonical",
                )
            )
        ).mappings().one_or_none()
        return None if row is None else _record(row)

    async def list_for_analysis_job(
        self,
        connection: AsyncConnection,
        analysis_job_id: UUID,
    ) -> tuple[PrefilterResultRecord, ...]:
        rows = (
            await connection.execute(
                sa.select(message_prefilter_results)
                .where(message_prefilter_results.c.analysis_job_id == analysis_job_id)
                .order_by(message_prefilter_results.c.created_at, message_prefilter_results.c.id)
            )
        ).mappings()
        return tuple(_record(row) for row in rows)

    async def lock_exact_key(
        self,
        connection: AsyncConnection,
        compatibility_key: str,
    ) -> None:
        await connection.execute(
            sa.select(
                sa.func.pg_advisory_xact_lock(
                    sa.func.hashtextextended(compatibility_key, 0)
                )
            )
        )

    async def find_recent_canonical(
        self,
        connection: AsyncConnection,
        *,
        normalized_content: str,
        normalized_content_sha256: str,
        analysis_input_sha256: str,
        analyzer_version: str,
        analysis_schema_version: str,
        window: timedelta,
    ) -> PrefilterResultRecord | None:
        row = (
            await connection.execute(
                sa.select(message_prefilter_results)
                .where(
                    message_prefilter_results.c.decision == "passed",
                    message_prefilter_results.c.dedup_relation == "canonical",
                    message_prefilter_results.c.normalized_content == normalized_content,
                    message_prefilter_results.c.normalized_content_sha256
                    == normalized_content_sha256,
                    message_prefilter_results.c.analysis_input_sha256
                    == analysis_input_sha256,
                    message_prefilter_results.c.analyzer_version == analyzer_version,
                    message_prefilter_results.c.analysis_schema_version
                    == analysis_schema_version,
                    message_prefilter_results.c.created_at
                    >= sa.func.now() - window,
                )
                .order_by(message_prefilter_results.c.created_at.desc())
                .limit(1)
            )
        ).mappings().one_or_none()
        return None if row is None else _record(row)

    async def record(
        self,
        connection: AsyncConnection,
        *,
        raw_message_id: UUID,
        schema_version: str,
        decision: str,
        reason_codes: Sequence[str],
        parent_raw_message_id: UUID | None,
        analysis_job_id: UUID | None,
        canonical_prefilter_result_id: UUID | None = None,
        normalized_content: str | None = None,
        normalized_content_sha256: str | None = None,
        analysis_input_sha256: str | None = None,
        analyzer_version: str | None = None,
        analysis_schema_version: str | None = None,
        dedup_relation: str | None = None,
        dedup_window_seconds: int | None = None,
    ) -> PrefilterWriteOutcome:
        values = {
            "raw_message_id": raw_message_id,
            "parent_raw_message_id": parent_raw_message_id,
            "analysis_job_id": analysis_job_id,
            "canonical_prefilter_result_id": canonical_prefilter_result_id,
            "schema_version": schema_version,
            "decision": decision,
            "reason_codes": list(reason_codes),
            "normalized_content": normalized_content,
            "normalized_content_sha256": normalized_content_sha256,
            "analysis_input_sha256": analysis_input_sha256,
            "analyzer_version": analyzer_version,
            "analysis_schema_version": analysis_schema_version,
            "dedup_relation": dedup_relation,
            "dedup_window_seconds": dedup_window_seconds,
        }
        result_id = uuid4()
        inserted_id = await connection.scalar(
            pg_insert(message_prefilter_results)
            .values(id=result_id, **values)
            .on_conflict_do_nothing(
                constraint="uq_message_prefilter_results_raw_schema"
            )
            .returning(message_prefilter_results.c.id)
        )
        row = (
            await connection.execute(
                sa.select(message_prefilter_results).where(
                    message_prefilter_results.c.raw_message_id == raw_message_id,
                    message_prefilter_results.c.schema_version == schema_version,
                )
            )
        ).mappings().one()
        if inserted_id is None and not _compatible(row, values):
            raise PrefilterResultConflict(
                "Prefilter result already exists with a different decision"
            )
        return PrefilterWriteOutcome(
            result=_record(row),
            created=inserted_id is not None,
        )


class OpportunityAnalysisCacheRepository:
    async def get_for_prefilter_result(
        self,
        connection: AsyncConnection,
        prefilter_result: PrefilterResultRecord,
    ) -> AnalysisCacheRecord | None:
        _require_passed_dedup_result(prefilter_result)
        row = (
            await connection.execute(
                sa.select(opportunity_analysis_cache).where(
                    opportunity_analysis_cache.c.normalized_content_sha256
                    == prefilter_result.normalized_content_sha256,
                    opportunity_analysis_cache.c.analysis_input_sha256
                    == prefilter_result.analysis_input_sha256,
                    opportunity_analysis_cache.c.analyzer_version
                    == prefilter_result.analyzer_version,
                    opportunity_analysis_cache.c.analysis_schema_version
                    == prefilter_result.analysis_schema_version,
                    opportunity_analysis_cache.c.normalized_content
                    == prefilter_result.normalized_content,
                )
            )
        ).mappings().one_or_none()
        return None if row is None else _cache_record(row)

    async def store_for_prefilter_result(
        self,
        connection: AsyncConnection,
        *,
        prefilter_result: PrefilterResultRecord,
        result: Mapping[str, Any],
    ) -> AnalysisCacheWriteOutcome:
        _require_passed_dedup_result(prefilter_result)
        if not isinstance(result, Mapping) or not result:
            raise ValueError("Cached analysis result must be a nonempty object")
        values = {
            "normalized_content": prefilter_result.normalized_content,
            "normalized_content_sha256": prefilter_result.normalized_content_sha256,
            "analysis_input_sha256": prefilter_result.analysis_input_sha256,
            "analyzer_version": prefilter_result.analyzer_version,
            "analysis_schema_version": prefilter_result.analysis_schema_version,
            "result": dict(result),
        }
        inserted_id = await connection.scalar(
            pg_insert(opportunity_analysis_cache)
            .values(id=uuid4(), **values)
            .on_conflict_do_nothing(
                constraint="uq_opportunity_analysis_cache_compatible_input"
            )
            .returning(opportunity_analysis_cache.c.id)
        )
        row = (
            await connection.execute(
                sa.select(opportunity_analysis_cache).where(
                    opportunity_analysis_cache.c.normalized_content_sha256
                    == prefilter_result.normalized_content_sha256,
                    opportunity_analysis_cache.c.analysis_input_sha256
                    == prefilter_result.analysis_input_sha256,
                    opportunity_analysis_cache.c.analyzer_version
                    == prefilter_result.analyzer_version,
                    opportunity_analysis_cache.c.analysis_schema_version
                    == prefilter_result.analysis_schema_version,
                )
            )
        ).mappings().one()
        if inserted_id is None and not _cache_compatible(row, values):
            raise AnalysisCacheConflict(
                "Compatible analysis cache key already has a different result"
            )
        return AnalysisCacheWriteOutcome(
            entry=_cache_record(row),
            created=inserted_id is not None,
        )


def _compatible(row: Mapping[str, Any], values: Mapping[str, Any]) -> bool:
    return all(row[field] == values[field] for field in values)


def _cache_compatible(row: Mapping[str, Any], values: Mapping[str, Any]) -> bool:
    return all(row[field] == values[field] for field in values)


def _require_passed_dedup_result(result: PrefilterResultRecord) -> None:
    required = (
        result.normalized_content,
        result.normalized_content_sha256,
        result.analysis_input_sha256,
        result.analyzer_version,
        result.analysis_schema_version,
    )
    if result.decision != "passed" or any(value is None for value in required):
        raise ValueError("Analysis cache requires a passed deduplicated result")


def _record(row: Mapping[str, Any]) -> PrefilterResultRecord:
    return PrefilterResultRecord(
        id=row["id"],
        raw_message_id=row["raw_message_id"],
        parent_raw_message_id=row["parent_raw_message_id"],
        analysis_job_id=row["analysis_job_id"],
        canonical_prefilter_result_id=row["canonical_prefilter_result_id"],
        schema_version=str(row["schema_version"]),
        decision=str(row["decision"]),
        reason_codes=tuple(str(code) for code in row["reason_codes"]),
        normalized_content=row["normalized_content"],
        normalized_content_sha256=row["normalized_content_sha256"],
        analysis_input_sha256=row["analysis_input_sha256"],
        analyzer_version=row["analyzer_version"],
        analysis_schema_version=row["analysis_schema_version"],
        dedup_relation=row["dedup_relation"],
        dedup_window_seconds=row["dedup_window_seconds"],
        created_at=row["created_at"],
    )


def _cache_record(row: Mapping[str, Any]) -> AnalysisCacheRecord:
    return AnalysisCacheRecord(
        id=row["id"],
        normalized_content=row["normalized_content"],
        normalized_content_sha256=row["normalized_content_sha256"],
        analysis_input_sha256=row["analysis_input_sha256"],
        analyzer_version=row["analyzer_version"],
        analysis_schema_version=row["analysis_schema_version"],
        result=dict(row["result"]),
        created_at=row["created_at"],
    )
