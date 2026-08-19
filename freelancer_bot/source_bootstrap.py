"""Operator-facing durable Global Source Library bootstrap orchestration."""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal
import hashlib
from typing import Any

import sqlalchemy as sa

from .config import RuntimeConfig
from .discovery import DiscoveryRequest
from .discovery_runner import DiscoveryExecution, DiscoveryRunner
from .global_source_library import (
    DiscoveryCampaignSpec,
    bootstrap_campaign_specs,
    generate_campaign_queries,
    validate_bootstrap_targets,
)
from .global_web_discovery import GlobalWebDiscoveryProvider
from .persistence.database import Database
from .persistence.discovery_campaigns import (
    DiscoveryCampaignRecord,
    DiscoveryCampaignRepository,
    LibraryStats,
    _campaign_record,
)
from .persistence.schema import (
    discovery_campaign_queries,
    discovery_campaigns,
    discovery_cost_events,
    source_discovery_evidence,
    source_monitoring_assignments,
    source_quality_snapshots,
    sources,
    source_taxonomy_assignments,
    source_taxonomy_terms,
    telegram_source_validations,
    web_provider_health,
)
from .web_discovery import WebDiscoveryGovernor
from .web_discovery import WebSearchBackend, WebSearchBackendError
from .web_page_extraction import SafeWebPageFetcher
from .web_provider_chain import build_web_search_backends, web_discovery_readiness


@dataclass(frozen=True)
class BootstrapStartResult:
    campaigns: tuple[DiscoveryCampaignRecord, ...]
    queries_created: int
    jobs_created: int
    web_readiness: dict[str, object]


class GlobalSourceLibraryService:
    def __init__(self, database: Database, config: RuntimeConfig | None = None) -> None:
        self.database = database
        self.config = config
        self.repository = DiscoveryCampaignRepository()

    async def start_bootstrap(
        self,
        *,
        target_unique_candidates: int = 1000,
        target_validated_sources: int = 500,
        target_approved_sources: int = 100,
        priority: int = 50,
    ) -> BootstrapStartResult:
        validate_bootstrap_targets(
            target_unique_candidates=target_unique_candidates,
            target_validated_sources=target_validated_sources,
            target_approved_sources=target_approved_sources,
        )
        budget = {
            "target_unique_candidates": target_unique_candidates,
            "target_validated_sources": target_validated_sources,
            "target_approved_sources": target_approved_sources,
            "web_search_calls_per_day": getattr(self.config, "web_search_calls_per_day", 500),
            "web_page_fetches_per_day": getattr(self.config, "web_page_fetches_per_day", 1000),
            "brave_search_requests_per_day": getattr(self.config, "brave_search_requests_per_day", 100),
            "brave_search_requests_per_campaign": getattr(self.config, "brave_search_requests_per_campaign", 15),
            "brave_search_cost_usd_per_request": str(getattr(self.config, "brave_search_cost_usd_per_request", Decimal("0"))),
            "brave_search_pricing_version": getattr(self.config, "brave_search_pricing_version", "brave-pricing.v1"),
        }
        campaigns: list[DiscoveryCampaignRecord] = []
        queries_created = 0
        jobs_created = 0
        async with self.database.transaction() as connection:
            for base in bootstrap_campaign_specs(priority=priority):
                spec = DiscoveryCampaignSpec(
                    **{
                        **base.__dict__,
                        "priority": priority,
                    }
                )
                campaign = await self.repository.ensure_campaign(
                    connection,
                    spec,
                    budget=budget,
                )
                campaigns.append(campaign)
                queries_created += await self.repository.ensure_queries(
                    connection,
                    campaign.id,
                    generate_campaign_queries(spec),
                )
                await self.repository.enqueue_campaign_plan(connection, campaign=campaign)
                jobs_created += 1
        return BootstrapStartResult(
            campaigns=tuple(campaigns),
            queries_created=queries_created,
            jobs_created=jobs_created,
            web_readiness=web_discovery_readiness(self.config) if self.config is not None else {},
        )

    async def run_campaign(
        self,
        campaign_key: str,
        *,
        max_queries: int = 20,
        results_per_query: int = 10,
        max_candidates: int = 100,
        max_page_fetches: int = 100,
    ) -> DiscoveryExecution:
        if self.config is None:
            raise ValueError("RuntimeConfig is required to execute a campaign")
        if min(max_queries, results_per_query, max_candidates, max_page_fetches) <= 0:
            raise ValueError("campaign execution bounds must be positive")
        backends = build_web_search_backends(self.config)
        if not backends:
            raise RuntimeError(
                "WEB_DISCOVERY_UNAVAILABLE: configure BRAVE_SEARCH_API_KEY, "
                "WEB_PRIMARY_SEARCH_URL or SEARXNG_URL"
            )
        query_reclaim_before = datetime.now(timezone.utc) - timedelta(minutes=30)
        async with self.database.transaction() as connection:
            campaign_row = (
                await connection.execute(
                    sa.select(discovery_campaigns)
                    .where(discovery_campaigns.c.campaign_key == campaign_key)
                    .with_for_update()
                )
            ).mappings().one_or_none()
            if campaign_row is None:
                raise LookupError("discovery campaign not found")
            campaign = _campaign_record(campaign_row)
            if campaign.status == "paused":
                raise RuntimeError("discovery campaign is paused")
            rows = await connection.execute(
                sa.select(discovery_campaign_queries)
                .where(
                    discovery_campaign_queries.c.campaign_id == campaign.id,
                    sa.or_(
                        discovery_campaign_queries.c.status.in_(("queued", "failed")),
                        sa.and_(
                            discovery_campaign_queries.c.status == "running",
                            discovery_campaign_queries.c.updated_at <= query_reclaim_before,
                        ),
                    ),
                )
                .order_by(discovery_campaign_queries.c.id)
                .with_for_update(skip_locked=True)
                .limit(max_queries)
            )
            query_rows = rows.mappings().all()
            if not query_rows:
                raise RuntimeError("discovery campaign has no claimable query batch")
            batch_key = hashlib.sha256(
                "|".join(str(row["normalized_query_key"]) for row in query_rows).encode()
            ).hexdigest()[:24]
            batch_attempt = int(campaign.progress.get("batch_attempt", 0) or 0) + 1
            run_progress = {
                **dict(campaign.progress),
                "batch_attempt": batch_attempt,
                "active_batch_key": batch_key,
            }
            for row in query_rows:
                await connection.execute(
                    sa.update(discovery_campaign_queries)
                    .where(discovery_campaign_queries.c.id == row["id"])
                    .values(status="running", updated_at=sa.func.now())
                )
            await self.repository.set_status(
                connection,
                campaign_id=campaign.id,
                status="running",
                progress=run_progress,
            )
        from .global_source_library import GlobalDiscoveryQuery, QueryFamily

        queries = tuple(
            GlobalDiscoveryQuery(
                text=row["query_text"],
                family=QueryFamily(row["query_family"]),
                language=row["language"],
                normalized_query_key=row["normalized_query_key"],
                strategy_version=row["strategy_version"],
                campaign_key=campaign.campaign_key,
                topic=row["query_text"],
            )
            for row in query_rows
        )
        now = datetime.now(timezone.utc)

        async def reserve_web_request(
            backend: WebSearchBackend,
            query: Any,
            _limit: int,
        ) -> None:
            if getattr(backend, "health_identity", "") != "brave":
                return
            idempotency_key = hashlib.sha256(
                "|".join(
                    (
                        "brave",
                        str(campaign.id),
                        str(query.normalized_query_key),
                    )
                ).encode("utf-8")
            ).hexdigest()
            cost = getattr(self.config, "brave_search_cost_usd_per_request", Decimal("0"))
            async with self.database.transaction() as connection:
                allowed = await self.repository.reserve_cost(
                    connection,
                    campaign_id=campaign.id,
                    stage="web_search",
                    provider="brave",
                    idempotency_key=f"brave:{idempotency_key}",
                    units=1,
                    estimated_cost_usd=cost,
                    daily_units_limit=int(getattr(self.config, "brave_search_requests_per_day", 100)),
                    campaign_units_limit=int(getattr(self.config, "brave_search_requests_per_campaign", 15)),
                )
            if not allowed:
                raise WebSearchBackendError(
                    "Brave Search budget exhausted",
                    failure_class="budget_exceeded",
                )

        provider = GlobalWebDiscoveryProvider(
            backends,
            governor=WebDiscoveryGovernor.from_config(self.config, database=self.database),
            queries=queries,
            results_per_query=results_per_query,
            max_candidates=max_candidates,
            max_page_fetches=max_page_fetches,
            page_fetcher=SafeWebPageFetcher(),
            campaign_id=campaign.id,
            before_backend_request=reserve_web_request,
            provider_costs_usd={
                "brave": float(getattr(self.config, "brave_search_cost_usd_per_request", Decimal("0")))
            },
        )
        try:
            execution = await DiscoveryRunner(self.database).run(
                provider,
                run_key=(
                    f"global-source-library:{campaign.campaign_key}:"
                    f"{batch_key}:attempt:{run_progress['batch_attempt']}"
                ),
                request=DiscoveryRequest(
                    parameters={
                        "trigger": "global_source_library",
                        "campaign_key": campaign.campaign_key,
                        "campaign_id": str(campaign.id),
                        "query_count": len(queries),
                        "web_readiness": web_discovery_readiness(self.config),
                    },
                    requested_at=now,
                ),
            )
        except Exception:
            async with self.database.transaction() as connection:
                await self.repository.set_status(
                    connection,
                    campaign_id=campaign.id,
                    status="failed",
                    progress=run_progress,
                )
                for row in query_rows:
                    await connection.execute(
                        sa.update(discovery_campaign_queries)
                        .where(discovery_campaign_queries.c.id == row["id"])
                        .values(status="failed", updated_at=sa.func.now())
                    )
            raise
        observability = execution.run.request.get("observability", {})
        progress = {
            **dict(run_progress),
            "query_count": len(queries),
            "result_count": execution.run.result_count,
            "materialized_count": execution.run.materialized_count,
            "observability": dict(observability) if isinstance(observability, dict) else {},
        }
        async with self.database.transaction() as connection:
            for row in query_rows:
                await connection.execute(
                    sa.update(discovery_campaign_queries)
                    .where(discovery_campaign_queries.c.id == row["id"])
                    .values(status="completed", last_run_at=now, updated_at=sa.func.now())
                )
            completed_query_count = int(run_progress.get("completed_query_count", 0) or 0) + len(query_rows)
            pending_query_count = await self.repository.pending_query_count(
                connection,
                campaign_id=campaign.id,
            )
            claimable_query_count = await self.repository.pending_query_count(
                connection,
                campaign_id=campaign.id,
                include_running=False,
            )
            progress.update(
                {
                    "completed_query_count": completed_query_count,
                    "last_batch_query_count": len(query_rows),
                    "pending_query_count": pending_query_count,
                }
            )
            updated = await self.repository.set_status(
                connection,
                campaign_id=campaign.id,
                status="completed" if pending_query_count == 0 else "planned",
                progress=progress,
                last_run_at=now,
            )
            if isinstance(observability, dict):
                queries_executed = int(observability.get("queries_executed", 0) or 0)
                page_fetches = int(observability.get("page_fetches", 0) or 0)
                if queries_executed > 0:
                    await self.repository.record_cost(
                        connection,
                        campaign_id=campaign.id,
                        stage="web_search",
                        provider="web_search",
                        idempotency_key=f"{execution.run.id}:web_search",
                        units=queries_executed,
                    )
                if page_fetches > 0:
                    await self.repository.record_cost(
                        connection,
                        campaign_id=campaign.id,
                        stage="page_fetch",
                        provider="web_page_fetcher",
                        idempotency_key=f"{execution.run.id}:page_fetch",
                        units=page_fetches,
                    )
            if claimable_query_count > 0:
                await self.repository.enqueue_campaign_plan(
                    connection,
                    campaign=updated,
                    batch_key=str(completed_query_count),
                )
        return execution

    async def stats(self) -> LibraryStats:
        async with self.database.connect() as connection:
            campaign_rows = await connection.execute(
                sa.select(discovery_campaigns.c.status, sa.func.count()).group_by(discovery_campaigns.c.status)
            )
            query_rows = await connection.execute(
                sa.select(discovery_campaign_queries.c.status, sa.func.count()).group_by(discovery_campaign_queries.c.status)
            )
            validation_rows = await connection.execute(
                sa.select(telegram_source_validations.c.state, sa.func.count()).group_by(telegram_source_validations.c.state)
            )
            source_rows = await connection.execute(
                sa.select(sources.c.lifecycle_status, sa.func.count()).where(sources.c.platform == "telegram").group_by(sources.c.lifecycle_status)
            )
            source_dimensions = await connection.execute(
                sa.select(
                    sources.c.id,
                    sources.c.platform,
                    sources.c.access_type,
                ).where(sources.c.platform == "telegram")
            )
            evidence_rows = await connection.execute(
                sa.select(
                    source_discovery_evidence.c.source_id,
                    discovery_campaigns.c.languages,
                    discovery_campaigns.c.buyer_habitats,
                    discovery_campaigns.c.industry_contexts,
                )
                .select_from(
                    source_discovery_evidence.join(
                        discovery_campaigns,
                        source_discovery_evidence.c.campaign_id
                        == discovery_campaigns.c.id,
                    )
                )
                .where(source_discovery_evidence.c.source_id.in_(
                    sa.select(sources.c.id).where(sources.c.platform == "telegram")
                ))
            )
            taxonomy_rows = await connection.execute(
                sa.select(
                    source_taxonomy_assignments.c.source_id,
                    source_taxonomy_terms.c.dimension,
                    source_taxonomy_terms.c.key,
                )
                .select_from(
                    source_taxonomy_assignments.join(
                        source_taxonomy_terms,
                        source_taxonomy_assignments.c.term_id
                        == source_taxonomy_terms.c.id,
                    )
                )
                .where(source_taxonomy_assignments.c.source_id.in_(
                    sa.select(sources.c.id).where(sources.c.platform == "telegram")
                ))
            )
            quality_rows = await connection.execute(
                sa.select(
                    source_quality_snapshots.c.source_id,
                    source_quality_snapshots.c.audited_at,
                    source_quality_snapshots.c.opportunity_yield,
                    source_quality_snapshots.c.buyer_intent_ratio,
                    source_quality_snapshots.c.seller_ratio,
                    source_quality_snapshots.c.spam_ratio,
                    source_quality_snapshots.c.duplicate_ratio,
                )
                .where(source_quality_snapshots.c.source_id.in_(
                    sa.select(sources.c.id).where(sources.c.platform == "telegram")
                ))
                .order_by(
                    source_quality_snapshots.c.source_id,
                    source_quality_snapshots.c.audited_at.desc(),
                    source_quality_snapshots.c.id.desc(),
                )
            )
            monitoring_rows = await connection.execute(
                sa.select(
                    source_monitoring_assignments.c.state,
                    source_monitoring_assignments.c.tier,
                    source_monitoring_assignments.c.next_due_at,
                )
            )
            provider_rows = await connection.execute(
                sa.select(web_provider_health)
            )
            cost_rows = await connection.execute(
                sa.select(
                    discovery_cost_events.c.stage,
                    discovery_cost_events.c.provider,
                    sa.func.sum(discovery_cost_events.c.units).label("units"),
                    sa.func.sum(discovery_cost_events.c.estimated_cost_usd).label("estimated_cost_usd"),
                )
                .group_by(
                    discovery_cost_events.c.stage,
                    discovery_cost_events.c.provider,
                )
            )
            source_dimension_rows = source_dimensions.mappings().all()
            evidence_dimension_rows = evidence_rows.mappings().all()
            taxonomy_dimension_rows = taxonomy_rows.mappings().all()
            quality_snapshot_rows = quality_rows.mappings().all()
            monitoring_state_rows = monitoring_rows.mappings().all()
            provider_health_rows = provider_rows.mappings().all()
            cost_summary_rows = cost_rows.mappings().all()
        coverage = _coverage_dimensions(
            source_dimension_rows,
            evidence_dimension_rows,
            taxonomy_dimension_rows,
            quality_snapshot_rows,
        )
        now = datetime.now(timezone.utc)
        monitoring = _monitoring_stats(monitoring_state_rows, now=now)
        provider_health = {
            f"{row['provider']}/{row['backend']}": {
                key: row[key]
                for key in (
                    "state",
                    "successful_searches",
                    "http_403",
                    "http_429",
                    "captcha_or_suspension",
                    "consecutive_failures",
                    "last_failure_category",
                    "last_failure_at",
                    "backoff_until",
                    "last_success_at",
                )
            }
            for row in provider_health_rows
        }
        cost_summary = {
            f"{row['stage']}/{row['provider']}": {
                "units": int(row["units"] or 0),
                "estimated_cost_usd": row["estimated_cost_usd"] or Decimal("0"),
            }
            for row in cost_summary_rows
        }
        return LibraryStats(
            campaigns={str(row[0]): int(row[1]) for row in campaign_rows},
            queries={str(row[0]): int(row[1]) for row in query_rows},
            validation_states={str(row[0]): int(row[1]) for row in validation_rows},
            source_lifecycle={str(row[0]): int(row[1]) for row in source_rows},
            coverage=coverage,
            monitoring=monitoring,
            provider_health=provider_health,
            cost_summary=cost_summary,
        )


def _coverage_dimensions(
    source_rows: list[dict[str, Any]],
    evidence_rows: list[dict[str, Any]],
    taxonomy_rows: list[dict[str, Any]],
    quality_rows: list[dict[str, Any]],
) -> dict[str, dict[str, int]]:
    """Return read-only distinct-source coverage, never a quality decision."""

    dimensions: dict[str, defaultdict[str, set[int]]] = {
        name: defaultdict(set)
        for name in (
            "buyer_habitat",
            "industry",
            "language",
            "source_type",
            "quality_tier",
        )
    }
    source_ids = {int(row["id"]) for row in source_rows}
    for row in source_rows:
        source_id = int(row["id"])
        dimensions["source_type"][
            f"{row['platform']}:{row['access_type']}"
        ].add(source_id)
    for row in evidence_rows:
        source_id = int(row["source_id"])
        if source_id not in source_ids:
            continue
        for dimension, column in (
            ("language", "languages"),
            ("buyer_habitat", "buyer_habitats"),
            ("industry", "industry_contexts"),
        ):
            values = row[column] or ()
            for value in values:
                if str(value).strip():
                    dimensions[dimension][str(value).strip().casefold()].add(source_id)
    for row in taxonomy_rows:
        source_id = int(row["source_id"])
        if source_id not in source_ids:
            continue
        dimension = "language" if row["dimension"] == "language" else "industry"
        dimensions[dimension][str(row["key"]).strip().casefold()].add(source_id)

    latest_quality: dict[int, dict[str, Any]] = {}
    for row in quality_rows:
        latest_quality.setdefault(int(row["source_id"]), row)
    for source_id in source_ids:
        tier = _quality_tier(latest_quality.get(source_id))
        dimensions["quality_tier"][tier].add(source_id)
    return {
        dimension: {
            label: len(source_ids_for_label)
            for label, source_ids_for_label in sorted(values.items())
        }
        for dimension, values in dimensions.items()
    }


def _quality_tier(row: dict[str, Any] | None) -> str:
    """Bucket the existing source-quality score for observability only."""

    if row is None:
        return "unmeasured"
    positive = (
        Decimal(str(row["opportunity_yield"])) * Decimal("0.60")
        + Decimal(str(row["buyer_intent_ratio"])) * Decimal("0.40")
    )
    negative = (
        Decimal(str(row["seller_ratio"])) * Decimal("0.40")
        + Decimal(str(row["spam_ratio"])) * Decimal("0.40")
        + Decimal(str(row["duplicate_ratio"])) * Decimal("0.20")
    )
    score = max(Decimal("0"), min(Decimal("1"), positive * (Decimal("1") - negative)))
    if score >= Decimal("0.70"):
        return "high"
    if score >= Decimal("0.40"):
        return "medium"
    return "low"


def _monitoring_stats(rows: list[dict[str, Any]], *, now: datetime) -> dict[str, int]:
    result: defaultdict[str, int] = defaultdict(int)
    for row in rows:
        result[f"state:{row['state']}"] += 1
        result[f"tier:{row['tier']}"] += 1
        if row["next_due_at"] <= now:
            result["due"] += 1
    return dict(sorted(result.items()))
