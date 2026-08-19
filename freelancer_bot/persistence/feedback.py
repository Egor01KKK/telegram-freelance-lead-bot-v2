from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from enum import Enum
from typing import Any
from uuid import UUID, uuid4

import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncConnection

from .schema import (
    delivery_action_events,
    feedback_events,
    match_evaluation_runs,
    match_traces,
    opportunities,
    source_feedback_signals,
)


FEEDBACK_SCHEMA_VERSION = "feedback.v1"
SOURCE_FEEDBACK_SIGNAL_VERSION = "source-feedback-signal.v1"


class FeedbackType(str, Enum):
    NOT_SUITABLE = "not_suitable"
    GOT_JOB = "got_job"


class FeedbackPersistenceConflict(RuntimeError):
    pass


@dataclass(frozen=True)
class FeedbackRecord:
    id: UUID
    delivery_action_event_id: UUID
    schema_version: str
    feedback_type: FeedbackType
    signal_scope: str
    delivery_id: UUID
    match_trace_id: UUID
    match_run_id: UUID
    opportunity_id: UUID
    opportunity_type: str
    search_profile_id: UUID
    profile_revision: int
    user_id: UUID
    source_id: int
    source_raw_message_id: UUID
    source_url: str
    match_score: Decimal
    match_score_version: str
    match_policy_version: str
    feedback_at: datetime
    created_at: datetime

    @property
    def match_algorithm_version(self) -> str:
        """Compatibility name for consumers that call the score algorithm out."""
        return self.match_score_version

    @property
    def is_personal_match_signal(self) -> bool:
        return self.signal_scope == "personal_match"

    @property
    def is_conversion(self) -> bool:
        return self.feedback_type is FeedbackType.GOT_JOB


@dataclass(frozen=True)
class FeedbackWriteOutcome:
    feedback: FeedbackRecord
    created: bool


@dataclass(frozen=True)
class SourceFeedbackSignal:
    source_id: int
    signal_version: str
    feedback_count: int
    not_suitable_count: int
    got_job_count: int
    last_feedback_at: datetime | None
    created_at: datetime
    updated_at: datetime

    @property
    def conversion_count(self) -> int:
        return self.got_job_count

    @property
    def won_job_count(self) -> int:
        return self.got_job_count


class SourceFeedbackSignalRepository:
    async def record(
        self,
        connection: AsyncConnection,
        *,
        source_id: int,
        feedback_type: FeedbackType,
        feedback_at: datetime,
    ) -> SourceFeedbackSignal:
        if source_id <= 0:
            raise ValueError("source_id must be positive")
        if feedback_at.tzinfo is None or feedback_at.utcoffset() is None:
            raise ValueError("feedback_at must include a timezone")
        feedback_type = _feedback_type(feedback_type)
        statement = pg_insert(source_feedback_signals).values(
            source_id=source_id,
            signal_version=SOURCE_FEEDBACK_SIGNAL_VERSION,
            feedback_count=1,
            not_suitable_count=(1 if feedback_type is FeedbackType.NOT_SUITABLE else 0),
            got_job_count=(1 if feedback_type is FeedbackType.GOT_JOB else 0),
            last_feedback_at=feedback_at,
        )
        statement = statement.on_conflict_do_update(
            index_elements=[source_feedback_signals.c.source_id],
            set_={
                "feedback_count": (
                    source_feedback_signals.c.feedback_count
                    + statement.excluded.feedback_count
                ),
                "not_suitable_count": (
                    source_feedback_signals.c.not_suitable_count
                    + statement.excluded.not_suitable_count
                ),
                "got_job_count": (
                    source_feedback_signals.c.got_job_count
                    + statement.excluded.got_job_count
                ),
                "last_feedback_at": _latest_timestamp(
                    source_feedback_signals.c.last_feedback_at,
                    statement.excluded.last_feedback_at,
                ),
                "updated_at": sa.func.now(),
            },
        )
        await connection.execute(statement)
        record = await self.get(connection, source_id)
        if record is None:
            raise FeedbackPersistenceConflict(
                "source feedback signal upsert returned no record"
            )
        return record

    async def get(
        self,
        connection: AsyncConnection,
        source_id: int,
    ) -> SourceFeedbackSignal | None:
        row = (
            await connection.execute(
                sa.select(source_feedback_signals).where(
                    source_feedback_signals.c.source_id == source_id
                )
            )
        ).mappings().one_or_none()
        return None if row is None else _source_signal(row)

    async def list(
        self,
        connection: AsyncConnection,
    ) -> tuple[SourceFeedbackSignal, ...]:
        rows = (
            await connection.execute(
                sa.select(source_feedback_signals).order_by(
                    source_feedback_signals.c.source_id
                )
            )
        ).mappings().all()
        return tuple(_source_signal(row) for row in rows)


class FeedbackRepository:
    async def record_for_action_event(
        self,
        connection: AsyncConnection,
        *,
        delivery_action_event_id: UUID,
    ) -> FeedbackWriteOutcome | None:
        row = (
            await connection.execute(
                sa.select(
                    delivery_action_events,
                    match_traces.c.run_id.label("trace_run_id"),
                    match_traces.c.opportunity_id.label("trace_opportunity_id"),
                    match_traces.c.search_profile_id.label(
                        "trace_search_profile_id"
                    ),
                    match_traces.c.profile_revision.label("trace_profile_revision"),
                    match_traces.c.final_rank_score.label("match_score"),
                    match_traces.c.decision_algorithm_version.label(
                        "match_score_version"
                    ),
                    match_evaluation_runs.c.policy_version.label(
                        "match_policy_version"
                    ),
                    opportunities.c.opportunity_type.label("opportunity_type"),
                )
                .select_from(
                    delivery_action_events
                    .join(
                        match_traces,
                        delivery_action_events.c.match_trace_id == match_traces.c.id,
                    )
                    .join(
                        match_evaluation_runs,
                        delivery_action_events.c.match_run_id
                        == match_evaluation_runs.c.id,
                    )
                    .join(
                        opportunities,
                        delivery_action_events.c.opportunity_id
                        == opportunities.c.id,
                    )
                )
                .where(delivery_action_events.c.id == delivery_action_event_id)
            )
        ).mappings().one_or_none()
        if row is None:
            raise FeedbackPersistenceConflict(
                "delivery action event does not exist"
            )
        if row["action_type"] == "open":
            return None

        feedback_type = _feedback_type(row["action_type"])
        _validate_trace_context(row)
        values = {
            "id": uuid4(),
            "schema_version": FEEDBACK_SCHEMA_VERSION,
            "delivery_action_event_id": delivery_action_event_id,
            "feedback_type": feedback_type.value,
            "signal_scope": (
                "personal_match"
                if feedback_type is FeedbackType.NOT_SUITABLE
                else "conversion"
            ),
            "delivery_id": row["delivery_id"],
            "match_trace_id": row["match_trace_id"],
            "match_run_id": row["match_run_id"],
            "opportunity_id": row["opportunity_id"],
            "opportunity_type": row["opportunity_type"],
            "search_profile_id": row["search_profile_id"],
            "profile_revision": row["profile_revision"],
            "user_id": row["user_id"],
            "source_id": row["source_id"],
            "source_raw_message_id": row["source_raw_message_id"],
            "source_url": row["source_url"],
            "match_score": row["match_score"],
            "match_score_version": row["match_score_version"],
            "match_policy_version": row["match_policy_version"],
            "feedback_at": row["created_at"],
        }
        inserted_id = await connection.scalar(
            pg_insert(feedback_events)
            .values(**values)
            .on_conflict_do_nothing(
                constraint="uq_feedback_events_delivery_action_event_id"
            )
            .returning(feedback_events.c.id)
        )
        record = await self.get_by_action_event(
            connection,
            delivery_action_event_id,
        )
        if record is None:
            raise FeedbackPersistenceConflict(
                "feedback insert returned no record"
            )
        _validate_existing(record, values)
        if inserted_id is not None:
            await SourceFeedbackSignalRepository().record(
                connection,
                source_id=record.source_id,
                feedback_type=record.feedback_type,
                feedback_at=record.feedback_at,
            )
        return FeedbackWriteOutcome(record, created=inserted_id is not None)

    async def record(
        self,
        connection: AsyncConnection,
        *,
        delivery_action_event_id: UUID,
    ) -> FeedbackWriteOutcome | None:
        return await self.record_for_action_event(
            connection,
            delivery_action_event_id=delivery_action_event_id,
        )

    async def get(
        self,
        connection: AsyncConnection,
        feedback_id: UUID,
    ) -> FeedbackRecord | None:
        row = (
            await connection.execute(
                sa.select(feedback_events).where(feedback_events.c.id == feedback_id)
            )
        ).mappings().one_or_none()
        return None if row is None else _feedback_record(row)

    async def get_by_action_event(
        self,
        connection: AsyncConnection,
        delivery_action_event_id: UUID,
    ) -> FeedbackRecord | None:
        row = (
            await connection.execute(
                sa.select(feedback_events).where(
                    feedback_events.c.delivery_action_event_id
                    == delivery_action_event_id
                )
            )
        ).mappings().one_or_none()
        return None if row is None else _feedback_record(row)

    async def list_for_delivery(
        self,
        connection: AsyncConnection,
        delivery_id: UUID,
    ) -> tuple[FeedbackRecord, ...]:
        rows = (
            await connection.execute(
                sa.select(feedback_events)
                .where(feedback_events.c.delivery_id == delivery_id)
                .order_by(feedback_events.c.feedback_at, feedback_events.c.id)
            )
        ).mappings().all()
        return tuple(_feedback_record(row) for row in rows)

    async def list_for_profile(
        self,
        connection: AsyncConnection,
        search_profile_id: UUID,
    ) -> tuple[FeedbackRecord, ...]:
        rows = (
            await connection.execute(
                sa.select(feedback_events)
                .where(feedback_events.c.search_profile_id == search_profile_id)
                .order_by(feedback_events.c.feedback_at, feedback_events.c.id)
            )
        ).mappings().all()
        return tuple(_feedback_record(row) for row in rows)

    async def list_for_source(
        self,
        connection: AsyncConnection,
        source_id: int,
    ) -> tuple[FeedbackRecord, ...]:
        rows = (
            await connection.execute(
                sa.select(feedback_events)
                .where(feedback_events.c.source_id == source_id)
                .order_by(feedback_events.c.feedback_at, feedback_events.c.id)
            )
        ).mappings().all()
        return tuple(_feedback_record(row) for row in rows)


def _feedback_type(value: FeedbackType | str) -> FeedbackType:
    try:
        return FeedbackType(value)
    except ValueError:
        raise ValueError(f"Unknown feedback type: {value}") from None


def _validate_trace_context(row: Any) -> None:
    expected = (
        row["match_run_id"],
        row["match_trace_id"],
        row["opportunity_id"],
        row["search_profile_id"],
        row["profile_revision"],
    )
    actual = (
        row["trace_run_id"],
        row["match_trace_id"],
        row["trace_opportunity_id"],
        row["trace_search_profile_id"],
        row["trace_profile_revision"],
    )
    if expected != actual:
        raise FeedbackPersistenceConflict(
            "delivery action event is inconsistent with its match trace"
        )
    score = row["match_score"]
    if score is None or not Decimal("0") <= score <= Decimal("1"):
        raise FeedbackPersistenceConflict(
            "delivered match has no valid immutable match score"
        )
    for field in ("match_score_version", "match_policy_version"):
        value = row[field]
        if not value or not isinstance(value, str):
            raise FeedbackPersistenceConflict(
                f"delivered match has no valid {field}"
            )


def _validate_existing(record: FeedbackRecord, values: dict[str, object]) -> None:
    actual = {
        "delivery_action_event_id": record.delivery_action_event_id,
        "schema_version": record.schema_version,
        "feedback_type": record.feedback_type.value,
        "signal_scope": record.signal_scope,
        "delivery_id": record.delivery_id,
        "match_trace_id": record.match_trace_id,
        "match_run_id": record.match_run_id,
        "opportunity_id": record.opportunity_id,
        "opportunity_type": record.opportunity_type,
        "search_profile_id": record.search_profile_id,
        "profile_revision": record.profile_revision,
        "user_id": record.user_id,
        "source_id": record.source_id,
        "source_raw_message_id": record.source_raw_message_id,
        "source_url": record.source_url,
        "match_score": record.match_score,
        "match_score_version": record.match_score_version,
        "match_policy_version": record.match_policy_version,
        "feedback_at": record.feedback_at,
    }
    expected = {key: values[key] for key in actual}
    if actual != expected:
        raise FeedbackPersistenceConflict(
            "feedback idempotency key exists with different content"
        )


def _feedback_record(row: Any) -> FeedbackRecord:
    return FeedbackRecord(
        id=row["id"],
        delivery_action_event_id=row["delivery_action_event_id"],
        schema_version=row["schema_version"],
        feedback_type=FeedbackType(row["feedback_type"]),
        signal_scope=row["signal_scope"],
        delivery_id=row["delivery_id"],
        match_trace_id=row["match_trace_id"],
        match_run_id=row["match_run_id"],
        opportunity_id=row["opportunity_id"],
        opportunity_type=row["opportunity_type"],
        search_profile_id=row["search_profile_id"],
        profile_revision=row["profile_revision"],
        user_id=row["user_id"],
        source_id=int(row["source_id"]),
        source_raw_message_id=row["source_raw_message_id"],
        source_url=row["source_url"],
        match_score=row["match_score"],
        match_score_version=row["match_score_version"],
        match_policy_version=row["match_policy_version"],
        feedback_at=row["feedback_at"],
        created_at=row["created_at"],
    )


def _source_signal(row: Any) -> SourceFeedbackSignal:
    return SourceFeedbackSignal(
        source_id=int(row["source_id"]),
        signal_version=row["signal_version"],
        feedback_count=int(row["feedback_count"]),
        not_suitable_count=int(row["not_suitable_count"]),
        got_job_count=int(row["got_job_count"]),
        last_feedback_at=row["last_feedback_at"],
        created_at=row["created_at"],
        updated_at=row["updated_at"],
    )


def _latest_timestamp(current, incoming):
    return sa.case(
        (current.is_(None), incoming),
        (incoming > current, incoming),
        else_=current,
    )
