from __future__ import annotations

from collections import defaultdict
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal, ROUND_HALF_UP
from uuid import UUID

import sqlalchemy as sa
from sqlalchemy.ext.asyncio import AsyncConnection

from .schema import (
    feedback_events,
    match_traces,
    message_prefilter_results,
    opportunities,
    opportunity_analysis_links,
    opportunity_source_messages,
    personalized_deliveries,
    raw_messages,
    source_quality_snapshots,
    sources,
)


PRODUCT_METRICS_SCHEMA_VERSION = "product-metrics.v1"
PROFILE_SEGMENT_STRATEGY = "stable-search-profile-id.v1"
RANKING_DIMENSIONS = (
    "quality_opportunity_yield",
    "pipeline_opportunity_yield",
    "feedback_events",
    "got_job_count",
)
_RATE_QUANTUM = Decimal("0.0001")


@dataclass(frozen=True)
class ProductMetricsWindow:
    """Half-open UTC-aware event window used by one reproducible report."""

    started_at: datetime
    ended_at: datetime

    def __post_init__(self) -> None:
        _aware(self.started_at, "started_at")
        _aware(self.ended_at, "ended_at")
        if self.ended_at <= self.started_at:
            raise ValueError("metrics window must end after it starts")


@dataclass(frozen=True)
class FunnelCounters:
    """Counters read from durable PostgreSQL pipeline events."""

    messages: int
    candidates: int
    analyses: int
    opportunities: int
    matches: int
    deliveries: int
    sent_deliveries: int
    feedback: int
    not_suitable: int
    got_job: int


@dataclass(frozen=True)
class WonLeadRateMetric:
    source_id: int
    search_profile_id: UUID
    profile_segment: str
    opportunity_type: str
    delivered_count: int
    feedback_count: int
    not_suitable_count: int
    got_job_count: int
    won_lead_rate: Decimal | None

    @property
    def won_rate(self) -> Decimal | None:
        return self.won_lead_rate


@dataclass(frozen=True)
class SourcePerformanceMetric:
    source_id: int
    source_display_name: str
    messages: int
    candidates: int
    analyses: int
    opportunities: int
    scheduled_deliveries: int
    delivered_deliveries: int
    feedback_events: int
    not_suitable_count: int
    got_job_count: int
    pipeline_opportunity_yield: Decimal | None
    quality_opportunity_yield: Decimal | None
    buyer_intent_ratio: Decimal | None
    seller_ratio: Decimal | None
    spam_ratio: Decimal | None
    duplicate_ratio: Decimal | None
    source_quality_audited_at: datetime | None
    source_quality_window_started_at: datetime | None
    source_quality_window_ended_at: datetime | None
    source_quality_snapshot_id: int | None
    source_quality_audit_key: str | None
    won_lead_rate: Decimal | None
    rank: int

    @property
    def feedback_rate(self) -> Decimal | None:
        return _rate(self.feedback_events, self.delivered_deliveries)

    @property
    def source_yield(self) -> Decimal | None:
        return self.quality_opportunity_yield


@dataclass(frozen=True)
class ProductMetricsReport:
    schema_version: str
    profile_segment_strategy: str
    window: ProductMetricsWindow
    funnel: FunnelCounters
    won_lead_rate: tuple[WonLeadRateMetric, ...]
    source_performance: tuple[SourcePerformanceMetric, ...]
    unattributed_scheduled_deliveries: int
    unattributed_sent_deliveries: int
    ranking_dimensions: tuple[str, ...] = RANKING_DIMENSIONS
    evidence_tables: tuple[str, ...] = (
        "raw_messages",
        "message_prefilter_results",
        "opportunity_analysis_links",
        "opportunities",
        "match_traces",
        "personalized_deliveries",
        "delivery_action_events",
        "feedback_events",
        "source_quality_snapshots",
    )


@dataclass
class _SourceAggregate:
    message_ids: set[UUID] = field(default_factory=set)
    candidate_ids: set[UUID] = field(default_factory=set)
    analysis_ids: set[UUID] = field(default_factory=set)
    opportunity_ids: set[UUID] = field(default_factory=set)
    match_ids: set[UUID] = field(default_factory=set)
    scheduled_delivery_ids: set[UUID] = field(default_factory=set)
    delivered_delivery_ids: set[UUID] = field(default_factory=set)
    feedback_ids: set[UUID] = field(default_factory=set)
    not_suitable_count: int = 0
    got_job_count: int = 0


class ProductMetricsRepository:
    """Builds product/source/conversion reports from immutable V2 evidence.

    The report is deliberately read-only. It does not update source projections,
    delivery state, feedback rows, or historical audit data. Source attribution
    follows every persisted ``opportunity_source_messages`` observation, so a
    canonical opportunity observed in two sources is visible in both source
    rows while the global funnel counters remain distinct-event counts.
    """

    async def build_report(
        self,
        connection: AsyncConnection,
        *,
        window_started_at: datetime,
        window_ended_at: datetime,
        profile_segment_labels: Mapping[UUID, str] | None = None,
    ) -> ProductMetricsReport:
        window = ProductMetricsWindow(window_started_at, window_ended_at)
        labels = _validated_profile_labels(profile_segment_labels or {})

        funnel = FunnelCounters(
            messages=await self._count(
                connection,
                raw_messages,
                raw_messages.c.observed_at,
                window,
            ),
            candidates=await self._count(
                connection,
                message_prefilter_results,
                message_prefilter_results.c.created_at,
                window,
                message_prefilter_results.c.decision == "passed",
            ),
            analyses=await self._count(
                connection,
                opportunity_analysis_links,
                opportunity_analysis_links.c.linked_at,
                window,
            ),
            opportunities=await self._count(
                connection,
                opportunities,
                opportunities.c.created_at,
                window,
            ),
            matches=await self._count(
                connection,
                match_traces,
                match_traces.c.evaluated_at,
                window,
                match_traces.c.eligible.is_(True),
            ),
            deliveries=await self._count(
                connection,
                personalized_deliveries,
                personalized_deliveries.c.created_at,
                window,
            ),
            sent_deliveries=await self._count(
                connection,
                personalized_deliveries,
                personalized_deliveries.c.sent_at,
                window,
                personalized_deliveries.c.status == "sent",
            ),
            feedback=await self._count(
                connection,
                feedback_events,
                feedback_events.c.feedback_at,
                window,
            ),
            not_suitable=await self._count(
                connection,
                feedback_events,
                feedback_events.c.feedback_at,
                window,
                feedback_events.c.feedback_type == "not_suitable",
            ),
            got_job=await self._count(
                connection,
                feedback_events,
                feedback_events.c.feedback_at,
                window,
                feedback_events.c.feedback_type == "got_job",
            ),
        )

        raw_rows = await self._raw_rows(connection, window)
        candidate_rows = await self._candidate_rows(connection, window)
        analysis_rows = await self._analysis_rows(connection, window)
        opportunity_rows = await self._opportunity_rows(connection, window)
        match_rows = await self._match_rows(connection, window)
        delivery_rows = await self._delivery_rows(connection, window)
        feedback_rows = await self._feedback_rows(connection, window)

        opportunity_ids = {row["id"] for row in opportunity_rows}
        opportunity_ids.update(row["opportunity_id"] for row in analysis_rows)
        opportunity_ids.update(row["opportunity_id"] for row in match_rows)
        opportunity_ids.update(row["opportunity_id"] for row in delivery_rows)
        opportunity_ids.update(row["opportunity_id"] for row in feedback_rows)
        source_links = await self._source_links(connection, opportunity_ids)

        aggregates: defaultdict[int, _SourceAggregate] = defaultdict(_SourceAggregate)
        for row in raw_rows:
            aggregates[row["source_id"]].message_ids.add(row["id"])
        for row in candidate_rows:
            aggregates[row["source_id"]].candidate_ids.add(row["id"])
        for row in analysis_rows:
            for source_id in source_links.get(row["opportunity_id"], ()):
                aggregates[source_id].analysis_ids.add(row["id"])
        for row in opportunity_rows:
            for source_id in source_links.get(row["id"], ()):
                aggregates[source_id].opportunity_ids.add(row["id"])
        for row in match_rows:
            for source_id in source_links.get(row["opportunity_id"], ()):
                aggregates[source_id].match_ids.add(row["id"])

        delivered_by_dimension: defaultdict[
            tuple[int, UUID, str, str], int
        ] = defaultdict(int)
        feedback_by_dimension: defaultdict[
            tuple[int, UUID, str, str], list[int]
        ] = defaultdict(lambda: [0, 0, 0])
        source_ids: set[int] = set(aggregates)
        scheduled_unattributed = 0
        sent_unattributed = 0
        for row in delivery_rows:
            row_sources = source_links.get(row["opportunity_id"], ())
            is_scheduled = _in_window(row["created_at"], window)
            is_delivered = (
                row["status"] == "sent"
                and _in_window(row["sent_at"], window)
            )
            if is_scheduled and not row_sources:
                scheduled_unattributed += 1
            if is_delivered and not row_sources:
                sent_unattributed += 1
            profile_segment = _profile_segment(row["search_profile_id"], labels)
            for source_id in row_sources:
                source_ids.add(source_id)
                aggregate = aggregates[source_id]
                if is_scheduled:
                    aggregate.scheduled_delivery_ids.add(row["id"])
                if is_delivered:
                    aggregate.delivered_delivery_ids.add(row["id"])
                    delivered_by_dimension[
                        (
                            source_id,
                            row["search_profile_id"],
                            profile_segment,
                            row["opportunity_type"],
                        )
                    ] += 1

        for row in feedback_rows:
            source_ids.add(row["source_id"])
            aggregate = aggregates[row["source_id"]]
            if row["feedback_type"] == "not_suitable":
                aggregate.not_suitable_count += 1
            else:
                aggregate.got_job_count += 1
            aggregate.feedback_ids.add(row["id"])
            key = (
                row["source_id"],
                row["search_profile_id"],
                _profile_segment(row["search_profile_id"], labels),
                row["opportunity_type"],
            )
            feedback_by_dimension[key][0] += 1
            feedback_by_dimension[key][1] += int(
                row["feedback_type"] == "not_suitable"
            )
            feedback_by_dimension[key][2] += int(row["feedback_type"] == "got_job")

        won_metrics = self._won_lead_rate_metrics(
            delivered_by_dimension,
            feedback_by_dimension,
        )
        snapshots_by_source = await self._latest_quality_snapshots(
            connection,
            window,
        )
        source_ids.update(snapshots_by_source)
        for source_id in snapshots_by_source:
            aggregates.setdefault(source_id, _SourceAggregate())
        sources_by_id = await self._sources(connection, source_ids)
        performance = self._source_performance(
            aggregates,
            sources_by_id,
            snapshots_by_source,
            window,
        )

        return ProductMetricsReport(
            schema_version=PRODUCT_METRICS_SCHEMA_VERSION,
            profile_segment_strategy=PROFILE_SEGMENT_STRATEGY,
            window=window,
            funnel=funnel,
            won_lead_rate=won_metrics,
            source_performance=performance,
            unattributed_scheduled_deliveries=scheduled_unattributed,
            unattributed_sent_deliveries=sent_unattributed,
        )

    async def report(
        self,
        connection: AsyncConnection,
        *,
        window_started_at: datetime,
        window_ended_at: datetime,
        profile_segment_labels: Mapping[UUID, str] | None = None,
    ) -> ProductMetricsReport:
        """Compatibility alias with a concise read-only report verb."""
        return await self.build_report(
            connection,
            window_started_at=window_started_at,
            window_ended_at=window_ended_at,
            profile_segment_labels=profile_segment_labels,
        )

    async def _count(
        self,
        connection: AsyncConnection,
        table: sa.Table,
        timestamp_column: sa.Column,
        window: ProductMetricsWindow,
        *conditions: sa.ColumnElement[bool],
    ) -> int:
        statement = sa.select(sa.func.count()).select_from(table).where(
            _window_clause(timestamp_column, window),
            *conditions,
        )
        return int(await connection.scalar(statement) or 0)

    async def _raw_rows(self, connection, window):
        return (
            await connection.execute(
                sa.select(raw_messages.c.id, raw_messages.c.source_id).where(
                    _window_clause(raw_messages.c.observed_at, window)
                )
            )
        ).mappings().all()

    async def _candidate_rows(self, connection, window):
        return (
            await connection.execute(
                sa.select(
                    message_prefilter_results.c.id,
                    raw_messages.c.source_id,
                )
                .select_from(
                    message_prefilter_results.join(
                        raw_messages,
                        message_prefilter_results.c.raw_message_id == raw_messages.c.id,
                    )
                )
                .where(
                    _window_clause(message_prefilter_results.c.created_at, window),
                    message_prefilter_results.c.decision == "passed",
                )
            )
        ).mappings().all()

    async def _analysis_rows(self, connection, window):
        return (
            await connection.execute(
                sa.select(
                    opportunity_analysis_links.c.analysis_cache_id.label("id"),
                    opportunity_analysis_links.c.opportunity_id,
                ).where(
                    _window_clause(opportunity_analysis_links.c.linked_at, window)
                )
            )
        ).mappings().all()

    async def _opportunity_rows(self, connection, window):
        return (
            await connection.execute(
                sa.select(
                    opportunities.c.id,
                    opportunities.c.opportunity_type,
                ).where(_window_clause(opportunities.c.created_at, window))
            )
        ).mappings().all()

    async def _match_rows(self, connection, window):
        return (
            await connection.execute(
                sa.select(match_traces.c.id, match_traces.c.opportunity_id).where(
                    _window_clause(match_traces.c.evaluated_at, window),
                    match_traces.c.eligible.is_(True),
                )
            )
        ).mappings().all()

    async def _delivery_rows(self, connection, window):
        return (
            await connection.execute(
                sa.select(
                    personalized_deliveries.c.id,
                    personalized_deliveries.c.opportunity_id,
                    personalized_deliveries.c.search_profile_id,
                    personalized_deliveries.c.status,
                    personalized_deliveries.c.created_at,
                    personalized_deliveries.c.sent_at,
                    opportunities.c.opportunity_type,
                )
                .select_from(
                    personalized_deliveries.join(
                        opportunities,
                        personalized_deliveries.c.opportunity_id == opportunities.c.id,
                    )
                )
                .where(
                    sa.or_(
                        _window_clause(personalized_deliveries.c.created_at, window),
                        _window_clause(personalized_deliveries.c.sent_at, window),
                    )
                )
            )
        ).mappings().all()

    async def _feedback_rows(self, connection, window):
        return (
            await connection.execute(
                sa.select(
                    feedback_events.c.id,
                    feedback_events.c.feedback_type,
                    feedback_events.c.opportunity_type,
                    feedback_events.c.search_profile_id,
                    feedback_events.c.source_id,
                    feedback_events.c.opportunity_id,
                ).where(_window_clause(feedback_events.c.feedback_at, window))
            )
        ).mappings().all()

    async def _source_links(
        self,
        connection: AsyncConnection,
        opportunity_ids: set[UUID],
    ) -> dict[UUID, tuple[int, ...]]:
        if not opportunity_ids:
            return {}
        rows = (
            await connection.execute(
                sa.select(
                    opportunity_source_messages.c.opportunity_id,
                    raw_messages.c.source_id,
                )
                .select_from(
                    opportunity_source_messages.join(
                        raw_messages,
                        opportunity_source_messages.c.raw_message_id == raw_messages.c.id,
                    )
                )
                .where(opportunity_source_messages.c.opportunity_id.in_(opportunity_ids))
            )
        ).mappings().all()
        grouped: defaultdict[UUID, set[int]] = defaultdict(set)
        for row in rows:
            grouped[row["opportunity_id"]].add(row["source_id"])
        return {
            opportunity_id: tuple(sorted(source_ids))
            for opportunity_id, source_ids in grouped.items()
        }

    async def _sources(
        self,
        connection: AsyncConnection,
        source_ids: set[int],
    ) -> dict[int, str]:
        if not source_ids:
            return {}
        rows = (
            await connection.execute(
                sa.select(sources.c.id, sources.c.display_name).where(
                    sources.c.id.in_(source_ids)
                )
            )
        ).mappings().all()
        return {int(row["id"]): str(row["display_name"]) for row in rows}

    async def _latest_quality_snapshots(
        self,
        connection: AsyncConnection,
        window: ProductMetricsWindow,
    ) -> dict[int, Mapping[str, object]]:
        rows = (
            await connection.execute(
                sa.select(source_quality_snapshots)
                .where(
                    source_quality_snapshots.c.audited_at <= window.ended_at,
                )
                .order_by(
                    source_quality_snapshots.c.source_id,
                    source_quality_snapshots.c.audited_at.desc(),
                    source_quality_snapshots.c.id.desc(),
                )
            )
        ).mappings().all()
        latest: dict[int, Mapping[str, object]] = {}
        for row in rows:
            latest.setdefault(int(row["source_id"]), row)
        return latest

    @staticmethod
    def _won_lead_rate_metrics(
        delivered_by_dimension: Mapping[tuple[int, UUID, str, str], int],
        feedback_by_dimension: Mapping[tuple[int, UUID, str, str], list[int]],
    ) -> tuple[WonLeadRateMetric, ...]:
        keys = set(delivered_by_dimension) | set(feedback_by_dimension)
        metrics: list[WonLeadRateMetric] = []
        for source_id, profile_id, segment, opportunity_type in sorted(
            keys,
            key=lambda value: (value[0], str(value[1]), value[2], value[3]),
        ):
            feedback_count, not_suitable_count, got_job_count = (
                feedback_by_dimension.get((source_id, profile_id, segment, opportunity_type), [0, 0, 0])
            )
            delivered_count = delivered_by_dimension.get(
                (source_id, profile_id, segment, opportunity_type),
                0,
            )
            metrics.append(
                WonLeadRateMetric(
                    source_id=source_id,
                    search_profile_id=profile_id,
                    profile_segment=segment,
                    opportunity_type=opportunity_type,
                    delivered_count=delivered_count,
                    feedback_count=feedback_count,
                    not_suitable_count=not_suitable_count,
                    got_job_count=got_job_count,
                    won_lead_rate=_rate(got_job_count, delivered_count),
                )
            )
        return tuple(metrics)

    @staticmethod
    def _source_performance(
        aggregates: Mapping[int, _SourceAggregate],
        source_names: Mapping[int, str],
        snapshots: Mapping[int, Mapping[str, object]],
        window: ProductMetricsWindow,
    ) -> tuple[SourcePerformanceMetric, ...]:
        rows: list[SourcePerformanceMetric] = []
        for source_id in sorted(aggregates):
            aggregate = aggregates[source_id]
            snapshot = snapshots.get(source_id)
            opportunities_count = len(aggregate.opportunity_ids)
            messages_count = len(aggregate.message_ids)
            rows.append(
                SourcePerformanceMetric(
                    source_id=source_id,
                    source_display_name=source_names.get(source_id, f"source:{source_id}"),
                    messages=messages_count,
                    candidates=len(aggregate.candidate_ids),
                    analyses=len(aggregate.analysis_ids),
                    opportunities=opportunities_count,
                    scheduled_deliveries=len(aggregate.scheduled_delivery_ids),
                    delivered_deliveries=len(aggregate.delivered_delivery_ids),
                    feedback_events=len(aggregate.feedback_ids),
                    not_suitable_count=aggregate.not_suitable_count,
                    got_job_count=aggregate.got_job_count,
                    pipeline_opportunity_yield=_rate(
                        opportunities_count,
                        messages_count,
                    ),
                    quality_opportunity_yield=(
                        None
                        if snapshot is None
                        else Decimal(snapshot["opportunity_yield"])
                    ),
                    buyer_intent_ratio=(
                        None
                        if snapshot is None
                        else Decimal(snapshot["buyer_intent_ratio"])
                    ),
                    seller_ratio=(
                        None if snapshot is None else Decimal(snapshot["seller_ratio"])
                    ),
                    spam_ratio=(
                        None if snapshot is None else Decimal(snapshot["spam_ratio"])
                    ),
                    duplicate_ratio=(
                        None
                        if snapshot is None
                        else Decimal(snapshot["duplicate_ratio"])
                    ),
                    source_quality_audited_at=(
                        None if snapshot is None else snapshot["audited_at"]
                    ),
                    source_quality_window_started_at=(
                        None
                        if snapshot is None
                        else snapshot["window_started_at"]
                    ),
                    source_quality_window_ended_at=(
                        None
                        if snapshot is None
                        else snapshot["window_ended_at"]
                    ),
                    source_quality_snapshot_id=(
                        None if snapshot is None else int(snapshot["id"])
                    ),
                    source_quality_audit_key=(
                        None if snapshot is None else str(snapshot["audit_key"])
                    ),
                    won_lead_rate=_rate(
                        aggregate.got_job_count,
                        len(aggregate.delivered_delivery_ids),
                    ),
                    rank=0,
                )
            )

        ranked = sorted(rows, key=_source_rank_key)
        return tuple(
            _with_rank(row, rank)
            for rank, row in enumerate(ranked, start=1)
        )


def _source_rank_key(row: SourcePerformanceMetric):
    return (
        -_rank_value(row.quality_opportunity_yield, row.pipeline_opportunity_yield),
        -_rank_value(row.pipeline_opportunity_yield, None),
        -row.feedback_events,
        -row.got_job_count,
        row.source_id,
    )


def _rank_value(primary: Decimal | None, fallback: Decimal | None) -> Decimal:
    return primary if primary is not None else (fallback or Decimal("-1"))


def _with_rank(row: SourcePerformanceMetric, rank: int) -> SourcePerformanceMetric:
    return SourcePerformanceMetric(
        source_id=row.source_id,
        source_display_name=row.source_display_name,
        messages=row.messages,
        candidates=row.candidates,
        analyses=row.analyses,
        opportunities=row.opportunities,
        scheduled_deliveries=row.scheduled_deliveries,
        delivered_deliveries=row.delivered_deliveries,
        feedback_events=row.feedback_events,
        not_suitable_count=row.not_suitable_count,
        got_job_count=row.got_job_count,
        pipeline_opportunity_yield=row.pipeline_opportunity_yield,
        quality_opportunity_yield=row.quality_opportunity_yield,
        buyer_intent_ratio=row.buyer_intent_ratio,
        seller_ratio=row.seller_ratio,
        spam_ratio=row.spam_ratio,
        duplicate_ratio=row.duplicate_ratio,
        source_quality_audited_at=row.source_quality_audited_at,
        source_quality_window_started_at=row.source_quality_window_started_at,
        source_quality_window_ended_at=row.source_quality_window_ended_at,
        source_quality_snapshot_id=row.source_quality_snapshot_id,
        source_quality_audit_key=row.source_quality_audit_key,
        won_lead_rate=row.won_lead_rate,
        rank=rank,
    )


def _profile_segment(profile_id: UUID, labels: Mapping[UUID, str]) -> str:
    return labels.get(profile_id, f"profile:{profile_id}")


def _validated_profile_labels(labels: Mapping[UUID, str]) -> dict[UUID, str]:
    result: dict[UUID, str] = {}
    for profile_id, label in labels.items():
        if not isinstance(profile_id, UUID):
            raise TypeError("profile segment keys must be UUID values")
        if not isinstance(label, str) or not label.strip():
            raise ValueError("profile segment labels must be non-empty strings")
        normalized = " ".join(label.split())
        if len(normalized) > 128:
            raise ValueError("profile segment labels must be at most 128 characters")
        result[profile_id] = normalized
    return result


def _window_clause(column: sa.Column, window: ProductMetricsWindow):
    return sa.and_(column >= window.started_at, column < window.ended_at)


def _in_window(value: datetime | None, window: ProductMetricsWindow) -> bool:
    return value is not None and window.started_at <= value < window.ended_at


def _aware(value: datetime, field: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field} must include a timezone")
    return value


def _rate(numerator: int, denominator: int) -> Decimal | None:
    if denominator <= 0:
        return None
    return (Decimal(numerator) / Decimal(denominator)).quantize(
        _RATE_QUANTUM,
        rounding=ROUND_HALF_UP,
    )


__all__ = [
    "FunnelCounters",
    "PRODUCT_METRICS_SCHEMA_VERSION",
    "PROFILE_SEGMENT_STRATEGY",
    "ProductMetricsReport",
    "ProductMetricsRepository",
    "ProductMetricsWindow",
    "RANKING_DIMENSIONS",
    "SourcePerformanceMetric",
    "WonLeadRateMetric",
]
