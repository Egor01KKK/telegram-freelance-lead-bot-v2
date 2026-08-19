from __future__ import annotations

import re
from collections.abc import Collection
from dataclasses import dataclass
from datetime import timedelta
from uuid import UUID, uuid4

import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncConnection

from freelancer_bot.metrics import MetricNames, MetricsSink, NoOpMetrics

from .schema import durable_jobs


SAFE_FAILURE_CODE = re.compile(r"^[A-Za-z][A-Za-z0-9_.-]{0,63}$")


@dataclass(frozen=True)
class JobClaim:
    id: UUID
    job_type: str
    idempotency_key: str
    correlation_id: UUID
    attempt_count: int
    max_attempts: int
    worker_id: str
    reclaimed: bool


class DurableJobRepository:
    def __init__(self, metrics: MetricsSink | None = None) -> None:
        self._metrics = metrics or NoOpMetrics()

    async def enqueue(
        self,
        connection: AsyncConnection,
        *,
        job_type: str,
        idempotency_key: str,
        max_attempts: int = 3,
        correlation_id: UUID | None = None,
    ) -> UUID:
        job_id = uuid4()
        trace_id = correlation_id or uuid4()
        statement = (
            pg_insert(durable_jobs)
            .values(
                id=job_id,
                job_type=job_type,
                idempotency_key=idempotency_key,
                max_attempts=max_attempts,
                correlation_id=trace_id,
            )
            .on_conflict_do_nothing(
                constraint="uq_durable_jobs_type_idempotency_key"
            )
            .returning(durable_jobs.c.id)
        )
        inserted_id = await connection.scalar(statement)
        if inserted_id is not None:
            self._metrics.increment(
                MetricNames.JOBS_CREATED,
                tags={"job_type": job_type},
            )
            return inserted_id

        existing_id = await connection.scalar(
            sa.select(durable_jobs.c.id).where(
                durable_jobs.c.job_type == job_type,
                durable_jobs.c.idempotency_key == idempotency_key,
            )
        )
        if existing_id is None:
            raise RuntimeError("Idempotent job enqueue returned no identifier")
        return existing_id

    async def claim_next(
        self,
        connection: AsyncConnection,
        *,
        worker_id: str,
        lease_duration: timedelta,
        job_types: Collection[str] | None = None,
        job_ids: Collection[UUID] | None = None,
    ) -> JobClaim | None:
        accepted_types = _job_types(job_types)
        if accepted_types == ():
            return None
        accepted_ids = None if job_ids is None else tuple(job_ids)
        if accepted_ids == ():
            return None
        await self._terminalize_exhausted_leases(connection, accepted_types)
        type_filter = (
            sa.true()
            if accepted_types is None
            else durable_jobs.c.job_type.in_(accepted_types)
        )
        id_filter = (
            sa.true()
            if accepted_ids is None
            else durable_jobs.c.id.in_(accepted_ids)
        )
        candidate = (
            await connection.execute(
                sa.select(durable_jobs)
                .where(
                    type_filter,
                    id_filter,
                    durable_jobs.c.attempt_count < durable_jobs.c.max_attempts,
                    sa.or_(
                        sa.and_(
                            durable_jobs.c.state == "queued",
                            durable_jobs.c.available_at <= sa.func.now(),
                        ),
                        sa.and_(
                            durable_jobs.c.state == "running",
                            durable_jobs.c.lease_expires_at <= sa.func.now(),
                        ),
                    ),
                )
                .order_by(
                    sa.case((durable_jobs.c.state == "running", 0), else_=1),
                    durable_jobs.c.available_at,
                    durable_jobs.c.created_at,
                    durable_jobs.c.id,
                )
                .with_for_update(skip_locked=True)
                .limit(1)
            )
        ).mappings().first()
        if candidate is None:
            return None

        reclaimed = candidate["state"] == "running"
        attempt_count = int(candidate["attempt_count"]) + 1
        updated = (
            await connection.execute(
                sa.update(durable_jobs)
                .where(durable_jobs.c.id == candidate["id"])
                .values(
                    state="running",
                    attempt_count=attempt_count,
                    claimed_at=sa.func.now(),
                    lease_owner=worker_id,
                    lease_expires_at=_after(lease_duration),
                    failure_code=None,
                    completed_at=None,
                    failed_at=None,
                    updated_at=sa.func.now(),
                )
                .returning(durable_jobs)
            )
        ).mappings().one()
        self._metrics.increment(
            MetricNames.JOBS_CLAIMED,
            tags={"job_type": updated["job_type"]},
        )
        if reclaimed:
            self._metrics.increment(
                MetricNames.JOBS_LEASE_RECLAIMED,
                tags={"job_type": updated["job_type"]},
            )
        return JobClaim(
            id=updated["id"],
            job_type=updated["job_type"],
            idempotency_key=updated["idempotency_key"],
            correlation_id=updated["correlation_id"],
            attempt_count=int(updated["attempt_count"]),
            max_attempts=int(updated["max_attempts"]),
            worker_id=worker_id,
            reclaimed=reclaimed,
        )

    async def complete(self, connection: AsyncConnection, claim: JobClaim) -> bool:
        result = await connection.execute(
            sa.update(durable_jobs)
            .where(*_valid_claim(claim))
            .values(
                state="completed",
                claimed_at=None,
                lease_owner=None,
                lease_expires_at=None,
                failure_code=None,
                completed_at=sa.func.now(),
                failed_at=None,
                updated_at=sa.func.now(),
            )
        )
        if result.rowcount == 1:
            self._metrics.increment(
                MetricNames.JOBS_COMPLETED,
                tags={"job_type": claim.job_type},
            )
        return result.rowcount == 1

    async def fail(
        self,
        connection: AsyncConnection,
        claim: JobClaim,
        *,
        failure_code: str,
        retry_delay: timedelta,
        retryable: bool = True,
    ) -> str | None:
        code = sanitize_failure_code(failure_code)
        terminal = not retryable or claim.attempt_count >= claim.max_attempts
        values: dict[str, object] = {
            "state": "failed" if terminal else "queued",
            "claimed_at": None,
            "lease_owner": None,
            "lease_expires_at": None,
            "failure_code": code,
            "updated_at": sa.func.now(),
            "completed_at": None,
            "failed_at": sa.func.now() if terminal else None,
        }
        if not terminal:
            values["available_at"] = _after(retry_delay)
        state = await connection.scalar(
            sa.update(durable_jobs)
            .where(*_valid_claim(claim))
            .values(**values)
            .returning(durable_jobs.c.state)
        )
        if state == "failed":
            self._metrics.increment(MetricNames.JOBS_FAILED, tags={"job_type": claim.job_type})
        elif state == "queued":
            self._metrics.increment(MetricNames.JOBS_RETRIED, tags={"job_type": claim.job_type})
        return state

    async def release_for_shutdown(
        self,
        connection: AsyncConnection,
        claim: JobClaim,
    ) -> str | None:
        terminal = claim.attempt_count >= claim.max_attempts
        state = await connection.scalar(
            sa.update(durable_jobs)
            .where(
                durable_jobs.c.id == claim.id,
                durable_jobs.c.state == "running",
                durable_jobs.c.lease_owner == claim.worker_id,
                durable_jobs.c.attempt_count == claim.attempt_count,
            )
            .values(
                state="failed" if terminal else "queued",
                available_at=sa.func.now(),
                claimed_at=None,
                lease_owner=None,
                lease_expires_at=None,
                failure_code="ShutdownTimeout",
                completed_at=None,
                failed_at=sa.func.now() if terminal else None,
                updated_at=sa.func.now(),
            )
            .returning(durable_jobs.c.state)
        )
        if state == "failed":
            self._metrics.increment(MetricNames.JOBS_FAILED, tags={"job_type": claim.job_type})
        elif state == "queued":
            self._metrics.increment(MetricNames.JOBS_RETRIED, tags={"job_type": claim.job_type})
        return state

    async def renew_lease(
        self,
        connection: AsyncConnection,
        claim: JobClaim,
        *,
        lease_duration: timedelta,
    ) -> bool:
        result = await connection.execute(
            sa.update(durable_jobs)
            .where(*_valid_claim(claim))
            .values(
                lease_expires_at=_after(lease_duration),
                updated_at=sa.func.now(),
            )
        )
        return result.rowcount == 1

    async def get(self, connection: AsyncConnection, job_id: UUID):
        return (
            await connection.execute(
                sa.select(durable_jobs).where(durable_jobs.c.id == job_id)
            )
        ).mappings().first()

    async def count_by_state(self, connection: AsyncConnection) -> dict[str, int]:
        rows = await connection.execute(
            sa.select(durable_jobs.c.state, sa.func.count().label("count"))
            .group_by(durable_jobs.c.state)
            .order_by(durable_jobs.c.state)
        )
        return {row.state: int(row.count) for row in rows}

    async def _terminalize_exhausted_leases(
        self,
        connection: AsyncConnection,
        job_types: tuple[str, ...] | None,
    ) -> None:
        type_filter = (
            sa.true()
            if job_types is None
            else durable_jobs.c.job_type.in_(job_types)
        )
        result = await connection.execute(
            sa.update(durable_jobs)
            .where(
                type_filter,
                durable_jobs.c.state == "running",
                durable_jobs.c.lease_expires_at <= sa.func.now(),
                durable_jobs.c.attempt_count >= durable_jobs.c.max_attempts,
            )
            .values(
                state="failed",
                claimed_at=None,
                lease_owner=None,
                lease_expires_at=None,
                failure_code="LeaseExpired",
                completed_at=None,
                failed_at=sa.func.now(),
                updated_at=sa.func.now(),
            )
        )
        if result.rowcount:
            self._metrics.increment(MetricNames.JOBS_FAILED, result.rowcount)


def sanitize_failure_code(value: str) -> str:
    return value if SAFE_FAILURE_CODE.fullmatch(value) else "ProcessingError"


def _job_types(values: Collection[str] | None) -> tuple[str, ...] | None:
    if values is None:
        return None
    return tuple(sorted({value.strip() for value in values if value.strip()}))


def _valid_claim(claim: JobClaim) -> tuple[sa.ColumnElement[bool], ...]:
    return (
        durable_jobs.c.id == claim.id,
        durable_jobs.c.state == "running",
        durable_jobs.c.lease_owner == claim.worker_id,
        durable_jobs.c.attempt_count == claim.attempt_count,
        durable_jobs.c.lease_expires_at > sa.func.now(),
    )


def _after(duration: timedelta):
    if duration.total_seconds() < 0:
        raise ValueError("Duration must not be negative")
    return sa.func.now() + sa.cast(duration, sa.Interval())
