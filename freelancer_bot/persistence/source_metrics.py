from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta
from decimal import Decimal, InvalidOperation
from enum import Enum
from typing import Any

import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncConnection

from .feedback import SourceFeedbackSignal, SourceFeedbackSignalRepository
from .schema import source_health, source_quality_snapshots, sources
from .source_repository import SourceNotFound, SourceStatus


_RATIO_QUANTUM = Decimal("0.0000001")
_RATE_QUANTUM = Decimal("0.0001")
_MAX_RATE = Decimal("9999999999.9999")


class SourceHealthStatus(str, Enum):
    UNKNOWN = "unknown"
    HEALTHY = "healthy"
    DEGRADED = "degraded"


class SourceHealthNotFound(LookupError):
    pass


class SourceMetricConflict(RuntimeError):
    pass


@dataclass(frozen=True)
class SourceQualitySnapshot:
    id: int
    source_id: int
    audit_key: str
    audited_at: datetime
    window_started_at: datetime
    window_ended_at: datetime
    sampled_message_count: int
    opportunity_yield: Decimal
    buyer_intent_ratio: Decimal
    seller_ratio: Decimal
    spam_ratio: Decimal
    duplicate_ratio: Decimal
    created_at: datetime


@dataclass(frozen=True)
class SourceHealthRecord:
    source_id: int
    health_status: SourceHealthStatus
    last_message_at: datetime | None
    last_audited_at: datetime | None
    messages_per_day: Decimal | None
    opportunities_per_day: Decimal | None
    activity_observed_at: datetime | None
    status_changed_at: datetime | None
    degraded_at: datetime | None
    degradation_reason: str | None
    updated_at: datetime


@dataclass(frozen=True)
class SourceReauditCandidate:
    source_id: int
    lifecycle_status: SourceStatus
    health_status: SourceHealthStatus
    last_message_at: datetime | None
    last_audited_at: datetime | None
    messages_per_day: Decimal | None
    opportunities_per_day: Decimal | None
    cadence_days: int | None = None
    due_reason: str | None = None


class SourceMetricsRepository:
    async def get_feedback_signal(
        self,
        connection: AsyncConnection,
        source_id: int,
    ) -> SourceFeedbackSignal | None:
        return await SourceFeedbackSignalRepository().get(connection, source_id)

    async def get_source_feedback_signal(
        self,
        connection: AsyncConnection,
        source_id: int,
    ) -> SourceFeedbackSignal | None:
        return await self.get_feedback_signal(connection, source_id)

    async def list_feedback_signals(
        self,
        connection: AsyncConnection,
    ) -> tuple[SourceFeedbackSignal, ...]:
        return await SourceFeedbackSignalRepository().list(connection)

    async def list_source_feedback_signals(
        self,
        connection: AsyncConnection,
    ) -> tuple[SourceFeedbackSignal, ...]:
        return await self.list_feedback_signals(connection)

    async def record_quality_snapshot(
        self,
        connection: AsyncConnection,
        *,
        source_id: int,
        audit_key: str,
        audited_at: datetime,
        window_started_at: datetime,
        window_ended_at: datetime,
        sampled_message_count: int,
        opportunity_yield: Decimal | float | int,
        buyer_intent_ratio: Decimal | float | int,
        seller_ratio: Decimal | float | int,
        spam_ratio: Decimal | float | int,
        duplicate_ratio: Decimal | float | int,
    ) -> SourceQualitySnapshot:
        await self._ensure_source(connection, source_id)
        audited_at = _aware(audited_at, "audited_at")
        window_started_at = _aware(window_started_at, "window_started_at")
        window_ended_at = _aware(window_ended_at, "window_ended_at")
        if window_ended_at <= window_started_at:
            raise ValueError("audit window must end after it starts")
        if audited_at < window_ended_at:
            raise ValueError("audited_at must not precede the audit window end")
        if sampled_message_count <= 0:
            raise ValueError("sampled_message_count must be positive")

        values = {
            "source_id": source_id,
            "audit_key": _required_text(audit_key, "audit_key"),
            "audited_at": audited_at,
            "window_started_at": window_started_at,
            "window_ended_at": window_ended_at,
            "sampled_message_count": sampled_message_count,
            "opportunity_yield": _ratio(opportunity_yield, "opportunity_yield"),
            "buyer_intent_ratio": _ratio(
                buyer_intent_ratio,
                "buyer_intent_ratio",
            ),
            "seller_ratio": _ratio(seller_ratio, "seller_ratio"),
            "spam_ratio": _ratio(spam_ratio, "spam_ratio"),
            "duplicate_ratio": _ratio(duplicate_ratio, "duplicate_ratio"),
        }
        snapshot_id = await connection.scalar(
            pg_insert(source_quality_snapshots)
            .values(**values)
            .on_conflict_do_nothing(
                constraint="uq_source_quality_snapshots_source_audit_key"
            )
            .returning(source_quality_snapshots.c.id)
        )
        row = (
            await connection.execute(
                sa.select(source_quality_snapshots).where(
                    source_quality_snapshots.c.source_id == source_id,
                    source_quality_snapshots.c.audit_key == values["audit_key"],
                )
            )
        ).mappings().one()
        if snapshot_id is None and not _snapshot_matches(row, values):
            raise SourceMetricConflict(
                "Audit key already exists with different source-quality metrics"
            )

        await self._record_last_audit(connection, source_id, audited_at)
        return _snapshot_record(row)

    async def list_quality_snapshots(
        self,
        connection: AsyncConnection,
        source_id: int,
    ) -> list[SourceQualitySnapshot]:
        rows = await connection.execute(
            sa.select(source_quality_snapshots)
            .where(source_quality_snapshots.c.source_id == source_id)
            .order_by(
                source_quality_snapshots.c.audited_at,
                source_quality_snapshots.c.id,
            )
        )
        return [_snapshot_record(row) for row in rows.mappings()]

    async def get_latest_quality_snapshot(
        self,
        connection: AsyncConnection,
        source_id: int,
    ) -> SourceQualitySnapshot | None:
        row = (
            await connection.execute(
                sa.select(source_quality_snapshots)
                .where(source_quality_snapshots.c.source_id == source_id)
                .order_by(
                    source_quality_snapshots.c.audited_at.desc(),
                    source_quality_snapshots.c.id.desc(),
                )
                .limit(1)
            )
        ).mappings().one_or_none()
        return None if row is None else _snapshot_record(row)

    async def record_activity(
        self,
        connection: AsyncConnection,
        *,
        source_id: int,
        observed_at: datetime,
        last_message_at: datetime | None,
        messages_per_day: Decimal | float | int,
        opportunities_per_day: Decimal | float | int,
    ) -> SourceHealthRecord:
        await self._ensure_source(connection, source_id)
        observed_at = _aware(observed_at, "observed_at")
        if last_message_at is not None:
            last_message_at = _aware(last_message_at, "last_message_at")
            if last_message_at > observed_at:
                raise ValueError("last_message_at must not be later than observed_at")

        statement = pg_insert(source_health).values(
            source_id=source_id,
            last_message_at=last_message_at,
            messages_per_day=_nonnegative(messages_per_day, "messages_per_day"),
            opportunities_per_day=_nonnegative(
                opportunities_per_day,
                "opportunities_per_day",
            ),
            activity_observed_at=observed_at,
        )
        newer_observation = sa.or_(
            source_health.c.activity_observed_at.is_(None),
            statement.excluded.activity_observed_at
            >= source_health.c.activity_observed_at,
        )
        statement = statement.on_conflict_do_update(
            index_elements=[source_health.c.source_id],
            set_={
                "last_message_at": _latest_timestamp(
                    source_health.c.last_message_at,
                    statement.excluded.last_message_at,
                ),
                "messages_per_day": statement.excluded.messages_per_day,
                "opportunities_per_day": statement.excluded.opportunities_per_day,
                "activity_observed_at": statement.excluded.activity_observed_at,
                "updated_at": sa.func.now(),
            },
            where=newer_observation,
        )
        await connection.execute(statement)
        return await self.get_health(connection, source_id)

    async def set_health_status(
        self,
        connection: AsyncConnection,
        *,
        source_id: int,
        health_status: SourceHealthStatus | str,
        changed_at: datetime,
        reason: str | None = None,
    ) -> SourceHealthRecord:
        await self._ensure_source(connection, source_id)
        status = _health_status(health_status)
        changed_at = _aware(changed_at, "changed_at")
        if status is SourceHealthStatus.UNKNOWN:
            raise ValueError("unknown health is implicit and cannot be set explicitly")
        normalized_reason = None if reason is None else _required_text(reason, "reason")
        if status is SourceHealthStatus.DEGRADED and normalized_reason is None:
            raise ValueError("degraded health requires a reason")
        if status is not SourceHealthStatus.DEGRADED and normalized_reason is not None:
            raise ValueError("only degraded health may have a reason")

        degraded_at = changed_at if status is SourceHealthStatus.DEGRADED else None
        statement = pg_insert(source_health).values(
            source_id=source_id,
            health_status=status.value,
            status_changed_at=changed_at,
            degraded_at=degraded_at,
            degradation_reason=normalized_reason,
        )
        statement = statement.on_conflict_do_update(
            index_elements=[source_health.c.source_id],
            set_={
                "health_status": statement.excluded.health_status,
                "status_changed_at": statement.excluded.status_changed_at,
                "degraded_at": statement.excluded.degraded_at,
                "degradation_reason": statement.excluded.degradation_reason,
                "updated_at": sa.func.now(),
            },
            where=sa.or_(
                source_health.c.status_changed_at.is_(None),
                statement.excluded.status_changed_at
                >= source_health.c.status_changed_at,
            ),
        )
        await connection.execute(statement)
        return await self.get_health(connection, source_id)

    async def get_health(
        self,
        connection: AsyncConnection,
        source_id: int,
    ) -> SourceHealthRecord:
        row = (
            await connection.execute(
                sa.select(source_health).where(source_health.c.source_id == source_id)
            )
        ).mappings().one_or_none()
        if row is None:
            raise SourceHealthNotFound(f"Source {source_id} has no health record")
        return _health_record(row)

    async def record_audit_completed(
        self,
        connection: AsyncConnection,
        *,
        source_id: int,
        audited_at: datetime,
    ) -> SourceHealthRecord:
        await self._ensure_source(connection, source_id)
        await self._record_last_audit(
            connection,
            source_id,
            _aware(audited_at, "audited_at"),
        )
        return await self.get_health(connection, source_id)

    async def list_due_for_reaudit(
        self,
        connection: AsyncConnection,
        *,
        as_of: datetime,
        stale_after: timedelta,
        limit: int = 100,
    ) -> list[SourceReauditCandidate]:
        as_of = _aware(as_of, "as_of")
        if stale_after <= timedelta(0):
            raise ValueError("stale_after must be positive")
        if limit <= 0:
            raise ValueError("limit must be positive")
        cutoff = as_of - stale_after
        health_status = sa.func.coalesce(
            source_health.c.health_status,
            SourceHealthStatus.UNKNOWN.value,
        )
        statement = (
            sa.select(
                sources.c.id.label("source_id"),
                sources.c.lifecycle_status,
                health_status.label("health_status"),
                source_health.c.last_message_at,
                source_health.c.last_audited_at,
                source_health.c.messages_per_day,
                source_health.c.opportunities_per_day,
            )
            .select_from(
                sources.outerjoin(
                    source_health,
                    source_health.c.source_id == sources.c.id,
                )
            )
            .where(
                sources.c.lifecycle_status.in_(
                    [SourceStatus.APPROVED.value, SourceStatus.PAUSED.value]
                ),
                sa.or_(
                    health_status == SourceHealthStatus.DEGRADED.value,
                    source_health.c.last_audited_at.is_(None),
                    source_health.c.last_audited_at <= cutoff,
                ),
            )
            .order_by(
                sa.case(
                    (health_status == SourceHealthStatus.DEGRADED.value, 0),
                    else_=1,
                ),
                sa.case((source_health.c.last_audited_at.is_(None), 0), else_=1),
                source_health.c.last_audited_at,
                sources.c.id,
            )
            .limit(limit)
        )
        rows = await connection.execute(statement)
        return [_reaudit_candidate(row) for row in rows.mappings()]

    async def list_due_for_periodic_reaudit(
        self,
        connection: AsyncConnection,
        *,
        eligible_source_ids: Sequence[int],
        as_of: datetime,
        degraded_cadence: timedelta,
        high_activity_cadence: timedelta,
        normal_cadence: timedelta,
        quiet_cadence: timedelta,
        high_activity_messages_per_day: Decimal | float | int,
        quiet_activity_messages_per_day: Decimal | float | int,
        limit: int = 100,
    ) -> list[SourceReauditCandidate]:
        as_of = _aware(as_of, "as_of")
        source_ids = tuple(dict.fromkeys(eligible_source_ids))
        if any(source_id <= 0 for source_id in source_ids):
            raise ValueError("eligible_source_ids must contain positive identifiers")
        if not source_ids:
            return []
        cadences = {
            "degraded": degraded_cadence,
            "high_activity": high_activity_cadence,
            "normal": normal_cadence,
            "quiet": quiet_cadence,
        }
        for name, cadence in cadences.items():
            if not timedelta(days=7) <= cadence <= timedelta(days=30):
                raise ValueError(f"{name} cadence must be between 7 and 30 days")
        if limit <= 0:
            raise ValueError("limit must be positive")
        high_threshold = _nonnegative(
            high_activity_messages_per_day,
            "high_activity_messages_per_day",
        )
        quiet_threshold = _nonnegative(
            quiet_activity_messages_per_day,
            "quiet_activity_messages_per_day",
        )
        if quiet_threshold >= high_threshold:
            raise ValueError(
                "quiet activity threshold must be below high activity threshold"
            )

        health_status = sa.func.coalesce(
            source_health.c.health_status,
            SourceHealthStatus.UNKNOWN.value,
        )
        last_audited_at = source_health.c.last_audited_at
        is_degraded = health_status == SourceHealthStatus.DEGRADED.value
        is_high_activity = sa.and_(
            ~is_degraded,
            source_health.c.messages_per_day >= high_threshold,
        )
        is_quiet = sa.and_(
            ~is_degraded,
            source_health.c.messages_per_day.is_not(None),
            source_health.c.messages_per_day <= quiet_threshold,
        )
        is_normal = sa.and_(
            ~is_degraded,
            sa.or_(
                source_health.c.messages_per_day.is_(None),
                sa.and_(
                    source_health.c.messages_per_day > quiet_threshold,
                    source_health.c.messages_per_day < high_threshold,
                ),
            ),
        )

        def stale(cadence: timedelta):
            return sa.or_(
                last_audited_at.is_(None),
                last_audited_at <= as_of - cadence,
            )

        due = sa.or_(
            sa.and_(is_degraded, stale(degraded_cadence)),
            sa.and_(is_high_activity, stale(high_activity_cadence)),
            sa.and_(is_quiet, stale(quiet_cadence)),
            sa.and_(is_normal, stale(normal_cadence)),
        )
        cadence_days = sa.case(
            (is_degraded, degraded_cadence.days),
            (is_high_activity, high_activity_cadence.days),
            (is_quiet, quiet_cadence.days),
            else_=normal_cadence.days,
        )
        due_reason = sa.case(
            (last_audited_at.is_(None), "never_audited"),
            (is_degraded, "degraded"),
            (is_high_activity, "high_activity"),
            (is_quiet, "quiet_activity"),
            else_="normal_activity",
        )
        priority = sa.case(
            (is_degraded, 0),
            (last_audited_at.is_(None), 1),
            (is_high_activity, 2),
            (is_normal, 3),
            else_=4,
        )
        statement = (
            sa.select(
                sources.c.id.label("source_id"),
                sources.c.lifecycle_status,
                health_status.label("health_status"),
                source_health.c.last_message_at,
                last_audited_at,
                source_health.c.messages_per_day,
                source_health.c.opportunities_per_day,
                cadence_days.label("cadence_days"),
                due_reason.label("due_reason"),
            )
            .select_from(
                sources.outerjoin(
                    source_health,
                    source_health.c.source_id == sources.c.id,
                )
            )
            .where(
                sources.c.id.in_(source_ids),
                sources.c.lifecycle_status == SourceStatus.APPROVED.value,
                due,
            )
            .order_by(
                priority,
                last_audited_at,
                sources.c.id,
            )
            .limit(limit)
        )
        rows = await connection.execute(statement)
        return [_reaudit_candidate(row) for row in rows.mappings()]

    async def _record_last_audit(
        self,
        connection: AsyncConnection,
        source_id: int,
        audited_at: datetime,
    ) -> None:
        statement = pg_insert(source_health).values(
            source_id=source_id,
            last_audited_at=audited_at,
        )
        newer_audit = sa.or_(
            source_health.c.last_audited_at.is_(None),
            statement.excluded.last_audited_at > source_health.c.last_audited_at,
        )
        statement = statement.on_conflict_do_update(
            index_elements=[source_health.c.source_id],
            set_={
                "last_audited_at": statement.excluded.last_audited_at,
                "updated_at": sa.func.now(),
            },
            where=newer_audit,
        )
        await connection.execute(statement)

    async def _ensure_source(
        self,
        connection: AsyncConnection,
        source_id: int,
    ) -> None:
        exists = await connection.scalar(
            sa.select(sources.c.id).where(sources.c.id == source_id)
        )
        if exists is None:
            raise SourceNotFound(f"Source {source_id} does not exist")


def _latest_timestamp(current, incoming):
    return sa.case(
        (incoming.is_(None), current),
        (current.is_(None), incoming),
        (incoming > current, incoming),
        else_=current,
    )


def _aware(value: datetime, field: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field} must include a timezone")
    return value


def _required_text(value: str, field: str) -> str:
    normalized = value.strip()
    if not normalized:
        raise ValueError(f"{field} must not be blank")
    return normalized


def _decimal(value: Decimal | float | int, field: str) -> Decimal:
    try:
        number = Decimal(str(value))
    except (InvalidOperation, ValueError):
        raise ValueError(f"{field} must be a finite number") from None
    if not number.is_finite():
        raise ValueError(f"{field} must be a finite number")
    return number


def _ratio(value: Decimal | float | int, field: str) -> Decimal:
    number = _decimal(value, field)
    if number < 0 or number > 1:
        raise ValueError(f"{field} must be between 0 and 1")
    return number.quantize(_RATIO_QUANTUM)


def _nonnegative(value: Decimal | float | int, field: str) -> Decimal:
    number = _decimal(value, field)
    if number < 0:
        raise ValueError(f"{field} must be nonnegative")
    number = number.quantize(_RATE_QUANTUM)
    if number > _MAX_RATE:
        raise ValueError(f"{field} exceeds the supported daily rate")
    return number


def _health_status(value: SourceHealthStatus | str) -> SourceHealthStatus:
    try:
        return SourceHealthStatus(value)
    except ValueError:
        raise ValueError(f"Unknown source health status: {value}") from None


def _snapshot_matches(row: Mapping[str, Any], values: Mapping[str, Any]) -> bool:
    return all(row[field] == value for field, value in values.items())


def _snapshot_record(row: Mapping[str, Any]) -> SourceQualitySnapshot:
    return SourceQualitySnapshot(
        id=int(row["id"]),
        source_id=int(row["source_id"]),
        audit_key=str(row["audit_key"]),
        audited_at=row["audited_at"],
        window_started_at=row["window_started_at"],
        window_ended_at=row["window_ended_at"],
        sampled_message_count=int(row["sampled_message_count"]),
        opportunity_yield=row["opportunity_yield"],
        buyer_intent_ratio=row["buyer_intent_ratio"],
        seller_ratio=row["seller_ratio"],
        spam_ratio=row["spam_ratio"],
        duplicate_ratio=row["duplicate_ratio"],
        created_at=row["created_at"],
    )


def _health_record(row: Mapping[str, Any]) -> SourceHealthRecord:
    return SourceHealthRecord(
        source_id=int(row["source_id"]),
        health_status=SourceHealthStatus(row["health_status"]),
        last_message_at=row["last_message_at"],
        last_audited_at=row["last_audited_at"],
        messages_per_day=row["messages_per_day"],
        opportunities_per_day=row["opportunities_per_day"],
        activity_observed_at=row["activity_observed_at"],
        status_changed_at=row["status_changed_at"],
        degraded_at=row["degraded_at"],
        degradation_reason=row["degradation_reason"],
        updated_at=row["updated_at"],
    )


def _reaudit_candidate(row: Mapping[str, Any]) -> SourceReauditCandidate:
    return SourceReauditCandidate(
        source_id=int(row["source_id"]),
        lifecycle_status=SourceStatus(row["lifecycle_status"]),
        health_status=SourceHealthStatus(row["health_status"]),
        last_message_at=row["last_message_at"],
        last_audited_at=row["last_audited_at"],
        messages_per_day=row["messages_per_day"],
        opportunities_per_day=row["opportunities_per_day"],
        cadence_days=row.get("cadence_days"),
        due_reason=row.get("due_reason"),
    )
