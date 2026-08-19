from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Any
from uuid import UUID, uuid4

import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncConnection

from ..discovery import (
    DiscoveredSourceCandidate,
    normalize_provider_kind,
    normalize_provider_name,
)
from .schema import discovery_results, discovery_runs


class DiscoveryRunStatus(str, Enum):
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"


class DiscoveryResultOutcome(str, Enum):
    CREATED = "created"
    EXISTING = "existing"


class DiscoveryRunNotFound(LookupError):
    pass


class DiscoveryRunConflict(RuntimeError):
    pass


class DiscoveryRunStateError(RuntimeError):
    pass


@dataclass(frozen=True)
class DiscoveryRunRecord:
    id: UUID
    provider: str
    provider_kind: str
    run_key: str
    request: Mapping[str, Any]
    status: DiscoveryRunStatus
    result_count: int
    materialized_count: int
    failure_code: str | None
    started_at: datetime
    finished_at: datetime | None
    created_at: datetime


@dataclass(frozen=True)
class DiscoveryResultRecord:
    id: int
    run_id: UUID
    provider_result_key: str
    source_id: int
    outcome: DiscoveryResultOutcome
    platform: str
    external_id: str
    access_type: str
    display_name: str
    handle: str | None
    canonical_url: str | None
    discovered_at: datetime
    seed_source_id: int | None
    seed_reference: str | None
    context: Mapping[str, Any]
    created_at: datetime


@dataclass(frozen=True)
class StartedDiscoveryRun:
    run: DiscoveryRunRecord
    created: bool


class DiscoveryRunRepository:
    async def list_runs(
        self,
        connection: AsyncConnection,
        *,
        provider: str | None = None,
        status: DiscoveryRunStatus | str | None = None,
        limit: int = 100,
    ) -> tuple[DiscoveryRunRecord, ...]:
        if not 1 <= limit <= 1000:
            raise ValueError("limit must be between 1 and 1000")
        statement = sa.select(discovery_runs)
        if provider is not None:
            statement = statement.where(
                discovery_runs.c.provider == normalize_provider_name(provider)
            )
        if status is not None:
            try:
                normalized_status = DiscoveryRunStatus(status)
            except ValueError:
                raise ValueError(f"unknown discovery run status: {status}") from None
            statement = statement.where(discovery_runs.c.status == normalized_status.value)
        rows = await connection.execute(
            statement.order_by(
                discovery_runs.c.started_at.desc(), discovery_runs.c.id
            ).limit(limit)
        )
        return tuple(_run_record(row) for row in rows.mappings())

    async def start(
        self,
        connection: AsyncConnection,
        *,
        provider: str,
        provider_kind: str,
        run_key: str,
        request: Mapping[str, Any],
        started_at: datetime,
    ) -> StartedDiscoveryRun:
        provider = normalize_provider_name(provider)
        provider_kind = normalize_provider_kind(provider_kind)
        run_key = _bounded_text(run_key, "run_key", 255)
        started_at = _aware(started_at, "started_at")
        request_payload = dict(request)
        run_id = uuid4()
        inserted_id = await connection.scalar(
            pg_insert(discovery_runs)
            .values(
                id=run_id,
                provider=provider,
                provider_kind=provider_kind,
                run_key=run_key,
                request=request_payload,
                status=DiscoveryRunStatus.RUNNING.value,
                started_at=started_at,
            )
            .on_conflict_do_nothing(
                constraint="uq_discovery_runs_provider_run_key"
            )
            .returning(discovery_runs.c.id)
        )
        if inserted_id is not None:
            return StartedDiscoveryRun(
                run=await self.get(connection, run_id),
                created=True,
            )

        existing = await self.get_by_key(
            connection,
            provider=provider,
            run_key=run_key,
        )
        if (
            existing.provider_kind != provider_kind
            or _request_without_observability(existing.request)
            != _request_without_observability(request_payload)
        ):
            raise DiscoveryRunConflict(
                "Discovery run key already exists with a different request"
            )
        return StartedDiscoveryRun(run=existing, created=False)

    async def update_request(
        self,
        connection: AsyncConnection,
        *,
        run_id: UUID,
        request: Mapping[str, Any],
    ) -> DiscoveryRunRecord:
        """Attach safe runtime observability while a run is still running."""

        request_payload = dict(request)
        result = await connection.execute(
            sa.update(discovery_runs)
            .where(
                discovery_runs.c.id == run_id,
                discovery_runs.c.status == DiscoveryRunStatus.RUNNING.value,
            )
            .values(request=request_payload)
        )
        if result.rowcount != 1:
            raise DiscoveryRunStateError(
                f"Discovery run {run_id} is not in running state"
            )
        return await self.get(connection, run_id)

    async def get(
        self,
        connection: AsyncConnection,
        run_id: UUID,
    ) -> DiscoveryRunRecord:
        row = (
            await connection.execute(
                sa.select(discovery_runs).where(discovery_runs.c.id == run_id)
            )
        ).mappings().one_or_none()
        if row is None:
            raise DiscoveryRunNotFound(f"Discovery run {run_id} does not exist")
        return _run_record(row)

    async def get_by_key(
        self,
        connection: AsyncConnection,
        *,
        provider: str,
        run_key: str,
    ) -> DiscoveryRunRecord:
        row = (
            await connection.execute(
                sa.select(discovery_runs).where(
                    discovery_runs.c.provider == normalize_provider_name(provider),
                    discovery_runs.c.run_key
                    == _bounded_text(run_key, "run_key", 255),
                )
            )
        ).mappings().one_or_none()
        if row is None:
            raise DiscoveryRunNotFound(
                "Discovery run does not exist for this provider and run key"
            )
        return _run_record(row)

    async def record_result(
        self,
        connection: AsyncConnection,
        *,
        run_id: UUID,
        candidate: DiscoveredSourceCandidate,
        source_id: int,
        outcome: DiscoveryResultOutcome | str,
    ) -> DiscoveryResultRecord:
        result_outcome = _result_outcome(outcome)
        status = await self._locked_status(connection, run_id)
        if status is not DiscoveryRunStatus.RUNNING:
            raise DiscoveryRunStateError(
                f"Discovery run {run_id} is not in running state"
            )
        result_id = await connection.scalar(
            sa.insert(discovery_results)
            .values(
                run_id=run_id,
                provider_result_key=candidate.result_key,
                source_id=source_id,
                outcome=result_outcome.value,
                platform=candidate.platform,
                external_id=candidate.external_id,
                access_type=candidate.access_type,
                display_name=candidate.display_name,
                handle=candidate.handle,
                canonical_url=candidate.canonical_url,
                discovered_at=candidate.discovered_at,
                seed_source_id=candidate.seed_source_id,
                seed_reference=candidate.seed_reference,
                context=dict(candidate.context or {}),
            )
            .returning(discovery_results.c.id)
        )
        if result_id is None:
            raise RuntimeError("Discovery result insert returned no identifier")
        return await self.get_result(connection, int(result_id))

    async def get_result(
        self,
        connection: AsyncConnection,
        result_id: int,
    ) -> DiscoveryResultRecord:
        row = (
            await connection.execute(
                sa.select(discovery_results).where(discovery_results.c.id == result_id)
            )
        ).mappings().one()
        return _result_record(row)

    async def list_results(
        self,
        connection: AsyncConnection,
        run_id: UUID,
    ) -> list[DiscoveryResultRecord]:
        rows = await connection.execute(
            sa.select(discovery_results)
            .where(discovery_results.c.run_id == run_id)
            .order_by(discovery_results.c.id)
        )
        return [_result_record(row) for row in rows.mappings()]

    async def complete(
        self,
        connection: AsyncConnection,
        *,
        run_id: UUID,
        result_count: int,
        finished_at: datetime,
    ) -> DiscoveryRunRecord:
        if result_count < 0:
            raise ValueError("result_count must be nonnegative")
        finished_at = _aware(finished_at, "finished_at")
        status = await self._locked_status(connection, run_id)
        if status is not DiscoveryRunStatus.RUNNING:
            raise DiscoveryRunStateError(
                f"Discovery run {run_id} is not in running state"
            )
        persisted_count = await connection.scalar(
            sa.select(sa.func.count())
            .select_from(discovery_results)
            .where(discovery_results.c.run_id == run_id)
        )
        if persisted_count != result_count:
            raise DiscoveryRunStateError(
                "Discovery run result count does not match persisted results"
            )
        result = await connection.execute(
            sa.update(discovery_runs)
            .where(
                discovery_runs.c.id == run_id,
                discovery_runs.c.status == DiscoveryRunStatus.RUNNING.value,
            )
            .values(
                status=DiscoveryRunStatus.COMPLETED.value,
                result_count=result_count,
                materialized_count=result_count,
                finished_at=finished_at,
            )
        )
        if result.rowcount != 1:
            raise DiscoveryRunStateError(
                f"Discovery run {run_id} is not in running state"
            )
        return await self.get(connection, run_id)

    async def _locked_status(
        self,
        connection: AsyncConnection,
        run_id: UUID,
    ) -> DiscoveryRunStatus:
        status = await connection.scalar(
            sa.select(discovery_runs.c.status)
            .where(discovery_runs.c.id == run_id)
            .with_for_update()
        )
        if status is None:
            raise DiscoveryRunNotFound(f"Discovery run {run_id} does not exist")
        return DiscoveryRunStatus(status)

    async def fail(
        self,
        connection: AsyncConnection,
        *,
        run_id: UUID,
        failure_code: str,
        finished_at: datetime,
    ) -> DiscoveryRunRecord:
        failure_code = _failure_code(failure_code)
        finished_at = _aware(finished_at, "finished_at")
        result = await connection.execute(
            sa.update(discovery_runs)
            .where(
                discovery_runs.c.id == run_id,
                discovery_runs.c.status == DiscoveryRunStatus.RUNNING.value,
            )
            .values(
                status=DiscoveryRunStatus.FAILED.value,
                failure_code=failure_code,
                finished_at=finished_at,
            )
        )
        if result.rowcount != 1:
            raise DiscoveryRunStateError(
                f"Discovery run {run_id} is not in running state"
            )
        return await self.get(connection, run_id)


def _run_record(row: Mapping[str, Any]) -> DiscoveryRunRecord:
    return DiscoveryRunRecord(
        id=row["id"],
        provider=str(row["provider"]),
        provider_kind=str(row["provider_kind"]),
        run_key=str(row["run_key"]),
        request=dict(row["request"]),
        status=DiscoveryRunStatus(row["status"]),
        result_count=int(row["result_count"]),
        materialized_count=int(row["materialized_count"]),
        failure_code=row["failure_code"],
        started_at=row["started_at"],
        finished_at=row["finished_at"],
        created_at=row["created_at"],
    )


def _request_without_observability(request: Mapping[str, Any]) -> dict[str, Any]:
    payload = dict(request)
    payload.pop("observability", None)
    return payload


def _result_record(row: Mapping[str, Any]) -> DiscoveryResultRecord:
    return DiscoveryResultRecord(
        id=int(row["id"]),
        run_id=row["run_id"],
        provider_result_key=str(row["provider_result_key"]),
        source_id=int(row["source_id"]),
        outcome=DiscoveryResultOutcome(row["outcome"]),
        platform=str(row["platform"]),
        external_id=str(row["external_id"]),
        access_type=str(row["access_type"]),
        display_name=str(row["display_name"]),
        handle=row["handle"],
        canonical_url=row["canonical_url"],
        discovered_at=row["discovered_at"],
        seed_source_id=row["seed_source_id"],
        seed_reference=row["seed_reference"],
        context=dict(row["context"]),
        created_at=row["created_at"],
    )


def _result_outcome(value: DiscoveryResultOutcome | str) -> DiscoveryResultOutcome:
    try:
        return DiscoveryResultOutcome(value)
    except ValueError:
        raise ValueError(f"Unknown discovery result outcome: {value}") from None


def _aware(value: datetime, field: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field} must include a timezone")
    return value


def _bounded_text(value: str, field: str, max_length: int) -> str:
    normalized = value.strip()
    if not normalized:
        raise ValueError(f"{field} must not be blank")
    if len(normalized) > max_length:
        raise ValueError(f"{field} must not exceed {max_length} characters")
    return normalized


def _failure_code(value: str) -> str:
    normalized = value.strip().lower()
    if not normalized or len(normalized) > 64:
        raise ValueError("failure_code must contain 1 to 64 characters")
    if not all(character.isascii() for character in normalized):
        raise ValueError("failure_code must be ASCII")
    allowed = set("abcdefghijklmnopqrstuvwxyz0123456789_.-")
    if normalized[0] not in set("abcdefghijklmnopqrstuvwxyz") or any(
        character not in allowed for character in normalized
    ):
        raise ValueError("failure_code must be a safe identifier")
    return normalized
