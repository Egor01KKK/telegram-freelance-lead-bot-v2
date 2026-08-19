"""PostgreSQL repositories for the durable Global Source Library state."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from collections import Counter
from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal
import hashlib
import re
from urllib.parse import urlsplit
from typing import Any
from uuid import UUID, uuid4

import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncConnection

from ..global_source_library import DiscoveryCampaignSpec, GlobalDiscoveryQuery
from .jobs import DurableJobRepository
from .schema import (
    discovery_campaign_profiles,
    discovery_campaign_queries,
    discovery_campaigns,
    discovery_cost_events,
    source_discovery_lineage,
    source_discovery_evidence,
    source_monitoring_assignments,
    source_reference_aliases,
    sources,
    telegram_source_validations,
)


@dataclass(frozen=True)
class DiscoveryCampaignRecord:
    id: UUID
    campaign_key: str
    campaign_type: str
    status: str
    languages: tuple[str, ...]
    geo_constraints: tuple[str, ...]
    specialist_concepts: tuple[str, ...]
    buyer_concepts: tuple[str, ...]
    buyer_habitats: tuple[str, ...]
    industry_contexts: tuple[str, ...]
    query_strategy_version: str
    priority: int
    created_from: str
    budget: Mapping[str, Any]
    progress: Mapping[str, Any]
    created_at: datetime
    updated_at: datetime
    last_run_at: datetime | None
    next_run_at: datetime | None


@dataclass(frozen=True)
class LibraryStats:
    campaigns: Mapping[str, int]
    queries: Mapping[str, int]
    validation_states: Mapping[str, int]
    source_lifecycle: Mapping[str, int]
    coverage: Mapping[str, Mapping[str, int]] = field(default_factory=dict)
    monitoring: Mapping[str, int] = field(default_factory=dict)
    provider_health: Mapping[str, Mapping[str, Any]] = field(default_factory=dict)
    cost_summary: Mapping[str, Mapping[str, Any]] = field(default_factory=dict)


@dataclass(frozen=True)
class LegacyEvidenceBackfillResult:
    candidate_sources: int
    recoverable_sources: int
    unrecoverable_sources: int
    evidence_created: int
    evidence_existing: int
    source_categories: Mapping[str, int]


class DiscoveryCampaignRepository:
    async def ensure_campaign(
        self,
        connection: AsyncConnection,
        spec: DiscoveryCampaignSpec,
        *,
        budget: Mapping[str, Any] | None = None,
        next_run_at: datetime | None = None,
    ) -> DiscoveryCampaignRecord:
        values = _campaign_values(spec, budget=budget, next_run_at=next_run_at)
        await connection.execute(
            pg_insert(discovery_campaigns)
            .values(**values)
            .on_conflict_do_nothing(constraint="uq_discovery_campaigns_campaign_key")
        )
        row = (
            await connection.execute(
                sa.select(discovery_campaigns).where(
                    discovery_campaigns.c.campaign_key == spec.campaign_key
                )
            )
        ).mappings().one_or_none()
        if row is None:
            raise RuntimeError("discovery campaign was not persisted")
        return _campaign_record(row)

    async def list_campaigns(
        self,
        connection: AsyncConnection,
        *,
        status: str | None = None,
        limit: int = 100,
    ) -> tuple[DiscoveryCampaignRecord, ...]:
        if not 1 <= limit <= 1000:
            raise ValueError("limit must be between 1 and 1000")
        statement = sa.select(discovery_campaigns)
        if status:
            statement = statement.where(discovery_campaigns.c.status == status)
        rows = await connection.execute(
            statement.order_by(
                discovery_campaigns.c.priority.desc(),
                discovery_campaigns.c.updated_at.desc(),
            ).limit(limit)
        )
        return tuple(_campaign_record(row) for row in rows.mappings())

    async def get_by_key(
        self, connection: AsyncConnection, campaign_key: str
    ) -> DiscoveryCampaignRecord | None:
        row = (
            await connection.execute(
                sa.select(discovery_campaigns).where(
                    discovery_campaigns.c.campaign_key == campaign_key
                )
            )
        ).mappings().one_or_none()
        return None if row is None else _campaign_record(row)

    async def ensure_queries(
        self,
        connection: AsyncConnection,
        campaign_id: UUID,
        queries: Sequence[GlobalDiscoveryQuery],
    ) -> int:
        created = 0
        for query in queries:
            query_hash = hashlib.sha256(query.text.encode("utf-8")).hexdigest()
            statement = (
                pg_insert(discovery_campaign_queries)
                .values(
                    id=uuid4(),
                    campaign_id=campaign_id,
                    normalized_query_key=query.normalized_query_key,
                    query_sha256=query_hash,
                    query_text=query.text,
                    query_family=query.family.value,
                    language=query.language,
                    strategy_version=query.strategy_version,
                )
                .on_conflict_do_nothing(
                    constraint="uq_discovery_campaign_queries_campaign_key"
                )
                .returning(discovery_campaign_queries.c.id)
            )
            result = await connection.execute(statement)
            # asyncpg/SQLAlchemy can expose a negative or driver-specific
            # rowcount for INSERT .. ON CONFLICT. RETURNING is the stable
            # signal for whether this idempotent insert created a row.
            if result.scalar_one_or_none() is not None:
                created += 1
        return created

    async def list_query_rows(
        self,
        connection: AsyncConnection,
        *,
        limit: int = 10_000,
    ) -> tuple[Mapping[str, Any], ...]:
        if not 1 <= limit <= 100_000:
            raise ValueError("limit must be between 1 and 100000")
        rows = await connection.execute(
            sa.select(
                discovery_campaign_queries.c.campaign_id,
                discovery_campaigns.c.campaign_key,
                discovery_campaign_queries.c.normalized_query_key,
                discovery_campaign_queries.c.query_sha256,
                discovery_campaign_queries.c.query_text,
                discovery_campaign_queries.c.query_family,
                discovery_campaign_queries.c.language,
                discovery_campaign_queries.c.status,
            )
            .select_from(
                discovery_campaign_queries.join(
                    discovery_campaigns,
                    discovery_campaign_queries.c.campaign_id == discovery_campaigns.c.id,
                )
            )
            .order_by(discovery_campaigns.c.campaign_key, discovery_campaign_queries.c.id)
            .limit(limit)
        )
        return tuple(rows.mappings())

    async def link_profile(
        self,
        connection: AsyncConnection,
        *,
        campaign_id: UUID,
        search_profile_id: UUID,
        gap_key: str,
    ) -> None:
        await connection.execute(
            pg_insert(discovery_campaign_profiles)
            .values(
                campaign_id=campaign_id,
                search_profile_id=search_profile_id,
                gap_key=gap_key,
            )
            .on_conflict_do_nothing()
        )

    async def enqueue_campaign_plan(
        self,
        connection: AsyncConnection,
        *,
        campaign: DiscoveryCampaignRecord,
        batch_key: str | None = None,
    ) -> UUID:
        suffix = "" if batch_key is None else f":batch:{batch_key}"
        return await DurableJobRepository().enqueue(
            connection,
            job_type="discovery.campaign.plan",
            idempotency_key=f"campaign:{campaign.campaign_key}{suffix}",
        )

    async def pending_query_count(
        self,
        connection: AsyncConnection,
        *,
        campaign_id: UUID,
        include_running: bool = True,
    ) -> int:
        statuses = ("queued", "running", "failed") if include_running else ("queued", "failed")
        return int(
            await connection.scalar(
                sa.select(sa.func.count())
                .select_from(discovery_campaign_queries)
                .where(
                    discovery_campaign_queries.c.campaign_id == campaign_id,
                    discovery_campaign_queries.c.status.in_(statuses),
                )
            )
            or 0
        )

    async def set_status(
        self,
        connection: AsyncConnection,
        *,
        campaign_id: UUID,
        status: str,
        reason: str | None = None,
        progress: Mapping[str, Any] | None = None,
        last_run_at: datetime | None = None,
    ) -> DiscoveryCampaignRecord:
        if status not in {"planned", "running", "paused", "completed", "failed"}:
            raise ValueError("invalid discovery campaign status")
        values: dict[str, Any] = {"status": status, "updated_at": sa.func.now()}
        if progress is not None:
            values["progress"] = dict(progress)
        if last_run_at is not None:
            values["last_run_at"] = last_run_at
        if status == "paused":
            if not reason or not reason.strip():
                raise ValueError("paused campaigns require a reason")
            values.update({"paused_at": sa.func.now(), "pause_reason": reason.strip()})
        else:
            values.update({"paused_at": None, "pause_reason": None})
        await connection.execute(
            sa.update(discovery_campaigns)
            .where(discovery_campaigns.c.id == campaign_id)
            .values(**values)
        )
        row = (
            await connection.execute(
                sa.select(discovery_campaigns).where(discovery_campaigns.c.id == campaign_id)
            )
        ).mappings().one()
        return _campaign_record(row)

    async def source_for_canonical_peer(
        self,
        connection: AsyncConnection,
        *,
        platform: str,
        canonical_peer_identity: str,
    ) -> int | None:
        """Return the existing global source for a resolved peer identity."""

        identity = canonical_peer_identity.strip()
        if not identity:
            raise ValueError("canonical_peer_identity must not be blank")
        alias_source = await connection.scalar(
            sa.select(source_reference_aliases.c.source_id)
            .where(
                source_reference_aliases.c.platform == platform,
                source_reference_aliases.c.canonical_peer_identity == identity,
            )
            .order_by(source_reference_aliases.c.source_id)
            .limit(1)
        )
        if alias_source is not None:
            return int(alias_source)
        return await connection.scalar(
            sa.select(telegram_source_validations.c.source_id)
            .where(
                telegram_source_validations.c.canonical_peer_identity == identity,
            )
            .order_by(telegram_source_validations.c.source_id)
            .limit(1)
        )

    async def record_alias(
        self,
        connection: AsyncConnection,
        *,
        source_id: int,
        platform: str,
        normalized_reference: str,
        reference_kind: str,
        canonical_peer_identity: str | None = None,
        seen_at: datetime | None = None,
    ) -> None:
        existing = (
            await connection.execute(
                sa.select(source_reference_aliases.c.source_id).where(
                    source_reference_aliases.c.platform == platform,
                    source_reference_aliases.c.normalized_reference == normalized_reference,
                )
            )
        ).scalar_one_or_none()
        if existing is not None and int(existing) != source_id:
            raise ValueError("Telegram reference alias points to another canonical source")
        values = {
            "source_id": source_id,
            "platform": platform,
            "normalized_reference": normalized_reference,
            "reference_kind": reference_kind,
            "canonical_peer_identity": canonical_peer_identity,
        }
        statement = pg_insert(source_reference_aliases).values(**values)
        await connection.execute(
            statement.on_conflict_do_update(
                constraint="uq_source_reference_aliases_platform_reference",
                set_={
                    "source_id": statement.excluded.source_id,
                    "canonical_peer_identity": sa.func.coalesce(
                        statement.excluded.canonical_peer_identity,
                        source_reference_aliases.c.canonical_peer_identity,
                    ),
                    "last_seen_at": seen_at or sa.func.now(),
                },
            )
        )

    async def source_for_alias(
        self,
        connection: AsyncConnection,
        *,
        platform: str,
        normalized_reference: str,
    ) -> int | None:
        return await connection.scalar(
            sa.select(source_reference_aliases.c.source_id).where(
                source_reference_aliases.c.platform == platform,
                source_reference_aliases.c.normalized_reference == normalized_reference,
            )
        )

    async def record_evidence(
        self,
        connection: AsyncConnection,
        *,
        source_id: int,
        provider: str,
        provider_kind: str,
        independent_evidence_key: str,
        extraction_kind: str,
        campaign_id: UUID | None = None,
        discovery_run_id: UUID | None = None,
        query_family: str | None = None,
        query_key: str | None = None,
        query_sha256: str | None = None,
        result_domain: str | None = None,
        profile_gap_keys: Sequence[str] = (),
        source_graph_provenance: Mapping[str, Any] | None = None,
    ) -> None:
        values = {
            "id": uuid4(),
            "source_id": source_id,
            "campaign_id": campaign_id,
            "discovery_run_id": discovery_run_id,
            "provider": provider,
            "provider_kind": provider_kind,
            "query_family": query_family,
            "query_key": query_key,
            "query_sha256": query_sha256,
            "result_domain": result_domain,
            "extraction_kind": extraction_kind,
            "independent_evidence_key": independent_evidence_key,
            "profile_gap_keys": list(dict.fromkeys(profile_gap_keys)),
            "source_graph_provenance": dict(source_graph_provenance or {}),
        }
        statement = pg_insert(source_discovery_evidence).values(**values)
        await connection.execute(
            statement.on_conflict_do_update(
                constraint="uq_source_discovery_evidence_independent_key",
                set_={"last_seen_at": sa.func.now()},
            )
        )

    async def record_evidence_if_missing(
        self,
        connection: AsyncConnection,
        *,
        source_id: int,
        provider: str,
        provider_kind: str,
        independent_evidence_key: str,
        extraction_kind: str,
        campaign_id: UUID | None = None,
        discovery_run_id: UUID | None = None,
        query_family: str | None = None,
        query_key: str | None = None,
        query_sha256: str | None = None,
        result_domain: str | None = None,
        profile_gap_keys: Sequence[str] = (),
        source_graph_provenance: Mapping[str, Any] | None = None,
    ) -> bool:
        """Insert historical evidence without rewriting an existing record."""

        values = {
            "id": uuid4(),
            "source_id": source_id,
            "campaign_id": campaign_id,
            "discovery_run_id": discovery_run_id,
            "provider": provider,
            "provider_kind": provider_kind,
            "query_family": query_family,
            "query_key": query_key,
            "query_sha256": query_sha256,
            "result_domain": result_domain,
            "extraction_kind": extraction_kind,
            "independent_evidence_key": independent_evidence_key,
            "profile_gap_keys": list(dict.fromkeys(profile_gap_keys)),
            "source_graph_provenance": dict(source_graph_provenance or {}),
        }
        result = await connection.execute(
            pg_insert(source_discovery_evidence)
            .values(**values)
            .on_conflict_do_nothing(
                constraint="uq_source_discovery_evidence_independent_key"
            )
            .returning(source_discovery_evidence.c.id)
        )
        return result.scalar_one_or_none() is not None

    async def backfill_legacy_evidence(
        self,
        connection: AsyncConnection,
        *,
        limit: int = 1000,
    ) -> LegacyEvidenceBackfillResult:
        """Reconcile pre-v1 lineage into v1 evidence without inventing signals.

        Only fields present in the immutable legacy lineage are normalized.  A
        missing query, domain, profile gap or graph observation remains NULL or
        empty in the new evidence row.
        """

        if not 1 <= limit <= 10000:
            raise ValueError("legacy evidence backfill limit must be between 1 and 10000")
        source_rows = (
            await connection.execute(
                sa.select(sources.c.id)
                .where(
                    sources.c.platform == "telegram",
                    sources.c.lifecycle_status == "candidate",
                )
                .order_by(sources.c.id)
                .limit(limit)
            )
        ).mappings().all()
        source_ids = {int(row["id"]) for row in source_rows}
        if not source_ids:
            return LegacyEvidenceBackfillResult(
                candidate_sources=0,
                recoverable_sources=0,
                unrecoverable_sources=0,
                evidence_created=0,
                evidence_existing=0,
                source_categories={},
            )

        lineage_rows = (
            await connection.execute(
                sa.select(source_discovery_lineage)
                .where(source_discovery_lineage.c.source_id.in_(source_ids))
                .order_by(
                    source_discovery_lineage.c.source_id,
                    source_discovery_lineage.c.id,
                )
            )
        ).mappings().all()
        grouped: dict[int, list[Mapping[str, Any]]] = {
            source_id: [] for source_id in source_ids
        }
        for row in lineage_rows:
            grouped[int(row["source_id"])].append(row)

        existing_source_ids = set(
            int(value)
            for value in (
                await connection.execute(
                    sa.select(sa.distinct(source_discovery_evidence.c.source_id)).where(
                        source_discovery_evidence.c.source_id.in_(source_ids)
                    )
                )
            ).scalars().all()
        )
        categories: Counter[str] = Counter()
        recoverable = 0
        unrecoverable = 0
        created = 0
        existing = 0
        for source_id in sorted(source_ids):
            items: list[dict[str, Any]] = []
            for lineage in grouped[source_id]:
                items.extend(_legacy_evidence_items(lineage))
            if not items:
                categories["NO_DISCOVERY_EVIDENCE" if not grouped[source_id] else "LEGACY_EVIDENCE_UNRECOVERABLE"] += 1
                unrecoverable += 1
                continue
            recoverable += 1
            categories[
                "CURRENT_EVIDENCE_COMPLETE"
                if source_id in existing_source_ids
                else "LEGACY_EVIDENCE_RECOVERABLE"
            ] += 1
            for item in items:
                inserted = await self.record_evidence_if_missing(
                    connection,
                    source_id=source_id,
                    provider=item["provider"],
                    provider_kind=item["provider_kind"],
                    independent_evidence_key=item["independent_evidence_key"],
                    extraction_kind=item["extraction_kind"],
                    discovery_run_id=item["discovery_run_id"],
                    query_family=item["query_family"],
                    query_key=item["query_key"],
                    query_sha256=item["query_sha256"],
                    result_domain=item["result_domain"],
                    profile_gap_keys=item["profile_gap_keys"],
                    source_graph_provenance=item["source_graph_provenance"],
                )
                if inserted:
                    created += 1
                else:
                    existing += 1
        return LegacyEvidenceBackfillResult(
            candidate_sources=len(source_ids),
            recoverable_sources=recoverable,
            unrecoverable_sources=unrecoverable,
            evidence_created=created,
            evidence_existing=existing,
            source_categories=dict(sorted(categories.items())),
        )

    async def upsert_validation(
        self,
        connection: AsyncConnection,
        *,
        source_id: int,
        collector_account_id: int,
        state: str,
        access_mode: str | None = None,
        canonical_peer_identity: str | None = None,
        failure_code: str | None = None,
        checked_at: datetime | None = None,
        checked_by: str | None = None,
    ) -> None:
        values = {
            "source_id": source_id,
            "collector_account_id": collector_account_id,
            "state": state,
            "access_mode": access_mode,
            "canonical_peer_identity": canonical_peer_identity,
            "failure_code": failure_code,
            "checked_at": checked_at,
            "checked_by": checked_by,
        }
        statement = pg_insert(telegram_source_validations).values(**values)
        await connection.execute(
            statement.on_conflict_do_update(
                index_elements=(
                    telegram_source_validations.c.source_id,
                    telegram_source_validations.c.collector_account_id,
                ),
                set_={
                    key: getattr(statement.excluded, key)
                    for key in values
                    if key not in {"source_id", "collector_account_id"}
                }
                | {
                    "canonical_peer_identity": sa.func.coalesce(
                        statement.excluded.canonical_peer_identity,
                        telegram_source_validations.c.canonical_peer_identity,
                    ),
                    "updated_at": sa.func.now(),
                },
            )
        )

    async def assign_monitoring(
        self,
        connection: AsyncConnection,
        *,
        source_id: int,
        collector_account_id: int,
        tier: str,
        next_due_at: datetime,
        cursor: Mapping[str, Any] | None = None,
    ) -> None:
        if tier not in {"A", "B", "C", "D"}:
            raise ValueError("invalid monitoring tier")
        statement = pg_insert(source_monitoring_assignments).values(
            source_id=source_id,
            collector_account_id=collector_account_id,
            tier=tier,
            next_due_at=next_due_at,
            cursor=dict(cursor or {}),
        )
        await connection.execute(
            statement.on_conflict_do_update(
                index_elements=(source_monitoring_assignments.c.source_id,),
                set_={
                    "collector_account_id": statement.excluded.collector_account_id,
                    "tier": statement.excluded.tier,
                    "next_due_at": statement.excluded.next_due_at,
                    "updated_at": sa.func.now(),
                },
            )
        )

    async def record_cost(
        self,
        connection: AsyncConnection,
        *,
        campaign_id: UUID,
        stage: str,
        provider: str,
        idempotency_key: str,
        units: int = 1,
        estimated_cost_usd: float = 0.0,
    ) -> None:
        await connection.execute(
            pg_insert(discovery_cost_events)
            .values(
                id=uuid4(),
                campaign_id=campaign_id,
                stage=stage,
                provider=provider,
                idempotency_key=idempotency_key,
                units=units,
                estimated_cost_usd=estimated_cost_usd,
            )
            .on_conflict_do_nothing(constraint="uq_discovery_cost_events_idempotency_key")
        )

    async def reserve_cost(
        self,
        connection: AsyncConnection,
        *,
        campaign_id: UUID,
        stage: str,
        provider: str,
        idempotency_key: str,
        units: int = 1,
        estimated_cost_usd: float | Decimal = 0.0,
        daily_units_limit: int | None = None,
        campaign_units_limit: int | None = None,
    ) -> bool:
        """Reserve a bounded provider attempt exactly once.

        The reservation is written before the external request.  A fixed
        PostgreSQL advisory lock serializes daily budget checks across
        campaigns; the unique idempotency key makes retries/restarts converge
        without charging the same logical query twice.
        """

        if units <= 0:
            raise ValueError("units must be positive")
        if daily_units_limit is not None and daily_units_limit <= 0:
            raise ValueError("daily_units_limit must be positive")
        if campaign_units_limit is not None and campaign_units_limit <= 0:
            raise ValueError("campaign_units_limit must be positive")
        existing = await connection.scalar(
            sa.select(discovery_cost_events.c.id).where(
                discovery_cost_events.c.idempotency_key == idempotency_key
            )
        )
        if existing is not None:
            return True

        await connection.execute(
            sa.text(
                "SELECT pg_advisory_xact_lock(hashtext(:lock_key))"
            ).bindparams(lock_key=f"discovery-cost:{stage}:{provider}")
        )
        campaign_exists = await connection.scalar(
            sa.select(discovery_campaigns.c.id)
            .where(discovery_campaigns.c.id == campaign_id)
            .with_for_update()
        )
        if campaign_exists is None:
            raise LookupError("discovery campaign not found")
        existing = await connection.scalar(
            sa.select(discovery_cost_events.c.id).where(
                discovery_cost_events.c.idempotency_key == idempotency_key
            )
        )
        if existing is not None:
            return True

        total_expression = sa.func.coalesce(sa.func.sum(discovery_cost_events.c.units), 0)
        if daily_units_limit is not None:
            daily_units = int(
                await connection.scalar(
                    sa.select(total_expression).where(
                        discovery_cost_events.c.stage == stage,
                        discovery_cost_events.c.provider == provider,
                        discovery_cost_events.c.recorded_at
                        >= sa.func.now() - sa.text("interval '1 day'"),
                    )
                )
                or 0
            )
            if daily_units + units > daily_units_limit:
                return False
        if campaign_units_limit is not None:
            campaign_units = int(
                await connection.scalar(
                    sa.select(total_expression).where(
                        discovery_cost_events.c.campaign_id == campaign_id,
                        discovery_cost_events.c.stage == stage,
                        discovery_cost_events.c.provider == provider,
                    )
                )
                or 0
            )
            if campaign_units + units > campaign_units_limit:
                return False
        await connection.execute(
            pg_insert(discovery_cost_events)
            .values(
                id=uuid4(),
                campaign_id=campaign_id,
                stage=stage,
                provider=provider,
                idempotency_key=idempotency_key,
                units=units,
                estimated_cost_usd=estimated_cost_usd,
            )
            .on_conflict_do_nothing(constraint="uq_discovery_cost_events_idempotency_key")
        )
        return True


def _legacy_evidence_items(lineage: Mapping[str, Any]) -> list[dict[str, Any]]:
    provider = str(lineage["provider"]).casefold()
    context = lineage.get("context")
    context = context if isinstance(context, Mapping) else {}
    if provider in {"web_search", "telegram_global_profile"}:
        raw_items = context.get("matches")
    elif provider == "telegram_source_graph":
        raw_items = context.get("observations")
    elif provider == "repository_seed":
        raw_items = (context,)
    else:
        raw_items = ()
    if not isinstance(raw_items, Sequence) or isinstance(raw_items, (str, bytes)):
        return []

    provider_kind = {
        "web_search": "web",
        "telegram_global_profile": "telegram_global_search",
        "telegram_source_graph": "source_graph",
        "repository_seed": "repository",
    }.get(provider, "legacy")
    extraction_kind = {
        "web_search": "global_search",
        "telegram_global_profile": "global_search",
        "telegram_source_graph": "source_graph",
        "repository_seed": "operator",
    }.get(provider, "operator")
    items: list[dict[str, Any]] = []
    for index, raw_item in enumerate(raw_items):
        item = raw_item if isinstance(raw_item, Mapping) else {}
        query = item.get("query")
        query_text = query.strip() if isinstance(query, str) and query.strip() else None
        query_key = _legacy_query_key(query_text)
        query_sha256 = (
            hashlib.sha256(query_text.encode("utf-8")).hexdigest()
            if query_text is not None
            else None
        )
        result_domain = _legacy_result_domain(item)
        legacy_kind = str(item.get("query_kind") or "").casefold()
        query_family = {
            "community": "COMMUNITY_DIRECTORY",
            "buyer_intent": "BUYER_HABITAT",
            "buyer_need": "BUYER_HABITAT",
        }.get(legacy_kind)
        profile_gap_keys = tuple(
            str(value)
            for value in item.get("profile_gap_keys", ())
            if isinstance(value, str) and value.strip()
        )
        provenance = dict(item) if provider == "telegram_source_graph" else {}
        items.append(
            {
                "provider": provider,
                "provider_kind": provider_kind,
                "extraction_kind": extraction_kind,
                "independent_evidence_key": f"legacy:{lineage['id']}:{index}",
                "discovery_run_id": lineage.get("discovery_run_id"),
                "query_family": query_family,
                "query_key": query_key,
                "query_sha256": query_sha256,
                "result_domain": result_domain,
                "profile_gap_keys": profile_gap_keys,
                "source_graph_provenance": provenance,
            }
        )
    return items


def _legacy_query_key(query: str | None) -> str | None:
    if query is None:
        return None
    value = re.sub(r"\s+", " ", query.casefold()).strip()
    return value[:255] or None


def _legacy_result_domain(item: Mapping[str, Any]) -> str | None:
    direct = item.get("result_domain")
    if isinstance(direct, str) and direct.strip():
        return direct.strip().casefold()[:255]
    result_url = item.get("result_url")
    if not isinstance(result_url, str) or not result_url.strip():
        return None
    try:
        hostname = urlsplit(result_url).hostname
    except ValueError:
        return None
    return None if not hostname else hostname.casefold()[:255]


def _campaign_values(
    spec: DiscoveryCampaignSpec,
    *,
    budget: Mapping[str, Any] | None,
    next_run_at: datetime | None,
) -> dict[str, Any]:
    return {
        "id": uuid4(),
        "campaign_key": spec.campaign_key,
        "campaign_type": spec.campaign_type.value,
        "languages": list(spec.languages),
        "geo_constraints": list(spec.geo_constraints),
        "specialist_concepts": list(spec.specialist_concepts),
        "buyer_concepts": list(spec.buyer_concepts),
        "buyer_habitats": list(spec.buyer_habitats),
        "industry_contexts": list(spec.industry_contexts),
        "query_strategy_version": spec.query_strategy_version,
        "priority": spec.priority,
        "created_from": spec.created_from,
        "budget": dict(budget or {}),
        "progress": {},
        "next_run_at": next_run_at,
    }


def _campaign_record(row: Mapping[str, Any]) -> DiscoveryCampaignRecord:
    return DiscoveryCampaignRecord(
        id=row["id"],
        campaign_key=str(row["campaign_key"]),
        campaign_type=str(row["campaign_type"]),
        status=str(row["status"]),
        languages=tuple(row["languages"]),
        geo_constraints=tuple(row["geo_constraints"]),
        specialist_concepts=tuple(row["specialist_concepts"]),
        buyer_concepts=tuple(row["buyer_concepts"]),
        buyer_habitats=tuple(row["buyer_habitats"]),
        industry_contexts=tuple(row["industry_contexts"]),
        query_strategy_version=str(row["query_strategy_version"]),
        priority=int(row["priority"]),
        created_from=str(row["created_from"]),
        budget=dict(row["budget"]),
        progress=dict(row["progress"]),
        created_at=row["created_at"],
        updated_at=row["updated_at"],
        last_run_at=row["last_run_at"],
        next_run_at=row["next_run_at"],
    )
