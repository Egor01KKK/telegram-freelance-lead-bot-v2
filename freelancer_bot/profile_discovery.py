"""Profile-driven source discovery and explainable source relevance.

This module deliberately keeps discovery independent from Telegram transport.  A
profile produces a versioned intent, the intent produces bounded Web Discovery
queries, and the existing DiscoveryRunner materializes/deduplicates global
source candidates.  Telegram validation and source lifecycle decisions remain
owned by their existing governed services.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal
import re
from typing import Any
from uuid import NAMESPACE_URL, UUID, uuid5

import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncConnection

from .discovery import DiscoveryRequest
from .discovery_runner import DiscoveryExecution, DiscoveryRunner
from .persistence.database import Database
from .persistence.schema import (
    profile_discovery_intents,
    source_profile_relevance,
)
from .persistence.search_profiles import SearchProfileRecord
from .persistence.source_repository import SourceRepository, SourceStatus
from .persistence.source_repository import PostgresSourceCatalog
from .global_source_library import profile_gap_campaign_spec, generate_campaign_queries
from .persistence.discovery_campaigns import DiscoveryCampaignRepository
from .search_profiles import WorkMode
from .telegram_profile_discovery import (
    DEFAULT_MAX_QUERIES,
    DEFAULT_RESULTS_PER_QUERY,
    TELEGRAM_PROFILE_DISCOVERY_STRATEGY_VERSION,
    TelegramGlobalSearchProvider,
    TelegramGlobalSearchPageCache,
    TelegramProfileSearchQuery,
    build_telegram_profile_search_queries,
)
from .web_discovery import (
    BuyerIntentSeed,
    CommunityCategory,
    SearxngSearchBackend,
    WebDiscoveryProvider,
    WebDiscoveryStrategy,
    WebDiscoveryTopic,
    WebDiscoveryGovernor,
    WebSearchBackend,
)


PROFILE_DISCOVERY_INTENT_VERSION = "profile-discovery-intent.v1"
SOURCE_PROFILE_RELEVANCE_VERSION = "source-profile-relevance.v2"
PROFILE_DISCOVERY_JOB_VERSION = "profile-web-discovery.v1"


@dataclass(frozen=True)
class DiscoveryProfileInput:
    """Structured profile input used by production and non-user evaluation runs."""

    identity_key: str
    search_profile_id: UUID | None
    profile_revision: int
    roles: tuple[str, ...]
    services: tuple[str, ...]
    skills: tuple[str, ...]
    industries: tuple[str, ...]
    languages: tuple[str, ...]
    geographies: tuple[str, ...]
    work_modes: tuple[str, ...]

    def __post_init__(self) -> None:
        if not self.identity_key.strip():
            raise ValueError("identity_key must not be blank")
        if self.profile_revision < 1:
            raise ValueError("profile_revision must be positive")
        for field in (
            "roles",
            "services",
            "skills",
            "industries",
            "languages",
            "geographies",
            "work_modes",
        ):
            values = getattr(self, field)
            if any(not isinstance(value, str) or not value.strip() for value in values):
                raise ValueError(f"{field} must contain non-empty strings")


@dataclass(frozen=True)
class ProfileDiscoveryIntent:
    id: UUID
    search_profile_id: UUID | None
    profile_revision: int
    roles: tuple[str, ...]
    services: tuple[str, ...]
    skills: tuple[str, ...]
    industries: tuple[str, ...]
    languages: tuple[str, ...]
    geo_remote: Mapping[str, Any]
    likely_buyer_roles: tuple[str, ...]
    buyer_contexts: tuple[str, ...]
    buyer_habitats: tuple[str, ...]
    literal_concepts: tuple[str, ...]
    adjacent_concepts: tuple[str, ...]
    generated_web_queries: tuple[str, ...]
    version: str = PROFILE_DISCOVERY_INTENT_VERSION

    def __post_init__(self) -> None:
        if self.profile_revision < 1:
            raise ValueError("profile_revision must be positive")
        if self.search_profile_id is None and not str(self.id):
            raise ValueError("evaluation intents require a deterministic id")
        if not self.version or not re.fullmatch(r"[a-z0-9][a-z0-9._-]{0,63}", self.version):
            raise ValueError("intent version is not safe")

    @property
    def identity(self) -> str:
        return str(self.search_profile_id or self.id)

    def to_values(self) -> dict[str, Any]:
        if self.search_profile_id is None:
            raise ValueError("evaluation intents are not persisted as user intents")
        return {
            "id": self.id,
            "search_profile_id": self.search_profile_id,
            "profile_revision": self.profile_revision,
            "roles": list(self.roles),
            "services": list(self.services),
            "skills": list(self.skills),
            "industries": list(self.industries),
            "languages": list(self.languages),
            "geo_remote": dict(self.geo_remote),
            "likely_buyer_roles": list(self.likely_buyer_roles),
            "buyer_contexts": list(self.buyer_contexts),
            "buyer_habitats": list(self.buyer_habitats),
            "literal_concepts": list(self.literal_concepts),
            "adjacent_concepts": list(self.adjacent_concepts),
            "generated_web_queries": list(self.generated_web_queries),
            "version": self.version,
        }


@dataclass(frozen=True)
class ProfileDiscoveryIntentWriteOutcome:
    intent: ProfileDiscoveryIntent
    created: bool


@dataclass(frozen=True)
class RelevanceExplanation:
    semantic_category: str
    direct_profession_hits: int
    direct_service_hits: int
    supporting_hits: int
    specific_buyer_hits: int
    generic_buyer_hits: int
    adjacent_hits: int
    query_signal_count: int
    score_components: tuple[tuple[str, Decimal], ...]
    diagnostic_label: str
    why: str
    result_title_snippet_hits: int = 0
    independent_evidence_families: int = 0
    primary_evidence_family: str = "none"
    priority_class: str = "INSUFFICIENT"


@dataclass(frozen=True)
class SourceRelevanceEvaluation:
    source_id: int
    search_profile_id: UUID | None
    discovery_intent_id: UUID
    profile_revision: int
    relevance_score: Decimal
    relevance_class: str
    evidence_categories: tuple[str, ...]
    explanation: RelevanceExplanation | None = None


@dataclass(frozen=True)
class ProfileSourceCoverage:
    approved_total: int
    relevant: int
    direct: int
    buyer_habitat: int
    weak: int
    adequate: int
    strong: int
    discovery_priority: str


@dataclass(frozen=True)
class ProfileDiscoveryExecution:
    profile_key: str
    intent: ProfileDiscoveryIntent
    execution: DiscoveryExecution
    generated_query_count: int
    direct_query_count: int
    buyer_habitat_query_count: int
    adjacent_query_count: int
    search_results_considered: int
    telegram_like_candidates: int
    unique_candidates: int
    known_candidates: int
    new_candidates: int
    overlap_with_previous_profiles: int = 0
    coverage: ProfileSourceCoverage | None = None
    candidate_priority_counts: Mapping[str, int] = field(default_factory=dict)
    provider_observability: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class EvaluationProfileSpec:
    key: str
    label: str
    roles: tuple[str, ...]
    services: tuple[str, ...]
    skills: tuple[str, ...]
    industries: tuple[str, ...]
    languages: tuple[str, ...] = ("en", "ru")
    geographies: tuple[str, ...] = ()
    work_modes: tuple[str, ...] = ("remote",)


class ProfileDiscoveryIntentRepository:
    async def ensure(
        self,
        connection: AsyncConnection,
        intent: ProfileDiscoveryIntent,
    ) -> ProfileDiscoveryIntentWriteOutcome:
        values = intent.to_values()
        inserted = await connection.scalar(
            pg_insert(profile_discovery_intents)
            .values(**values)
            .on_conflict_do_nothing(
                constraint="uq_profile_discovery_intents_profile_revision_version"
            )
            .returning(profile_discovery_intents.c.id)
        )
        row = (
            await connection.execute(
                sa.select(profile_discovery_intents).where(
                    profile_discovery_intents.c.id == intent.id
                )
            )
        ).mappings().one_or_none()
        if row is None:
            raise RuntimeError("profile discovery intent was not persisted")
        stored = _intent_record(row)
        if stored.to_values() != values:
            raise RuntimeError("profile discovery intent identity has conflicting content")
        return ProfileDiscoveryIntentWriteOutcome(stored, inserted is not None)

    async def list_for_profile(
        self,
        connection: AsyncConnection,
        *,
        search_profile_id: UUID,
        limit: int = 100,
    ) -> tuple[ProfileDiscoveryIntent, ...]:
        if not 1 <= limit <= 1000:
            raise ValueError("limit must be between 1 and 1000")
        rows = await connection.execute(
            sa.select(profile_discovery_intents)
            .where(profile_discovery_intents.c.search_profile_id == search_profile_id)
            .order_by(
                profile_discovery_intents.c.profile_revision.desc(),
                profile_discovery_intents.c.created_at.desc(),
            )
            .limit(limit)
        )
        return tuple(_intent_record(row) for row in rows.mappings())

    async def list_all(
        self,
        connection: AsyncConnection,
        *,
        limit: int = 100,
    ) -> tuple[ProfileDiscoveryIntent, ...]:
        if not 1 <= limit <= 1000:
            raise ValueError("limit must be between 1 and 1000")
        rows = await connection.execute(
            sa.select(profile_discovery_intents)
            .order_by(
                profile_discovery_intents.c.created_at.desc(),
                profile_discovery_intents.c.search_profile_id,
            )
            .limit(limit)
        )
        return tuple(_intent_record(row) for row in rows.mappings())


class SourceProfileRelevanceRepository:
    async def upsert(
        self,
        connection: AsyncConnection,
        evaluation: SourceRelevanceEvaluation,
        *,
        evaluated_at: datetime,
    ) -> None:
        if evaluation.search_profile_id is None:
            raise ValueError("source relevance requires a persisted search profile")
        _aware(evaluated_at, "evaluated_at")
        values = {
            "source_id": evaluation.source_id,
            "search_profile_id": evaluation.search_profile_id,
            "discovery_intent_id": evaluation.discovery_intent_id,
            "profile_revision": evaluation.profile_revision,
            "relevance_score": evaluation.relevance_score,
            "relevance_class": evaluation.relevance_class,
            "evidence_categories": list(evaluation.evidence_categories),
            "last_evaluated_at": evaluated_at,
            "version": SOURCE_PROFILE_RELEVANCE_VERSION,
        }
        statement = pg_insert(source_profile_relevance).values(**values)
        statement = statement.on_conflict_do_update(
            constraint="uq_source_profile_relevance_source_intent",
            set_={
                "relevance_score": statement.excluded.relevance_score,
                "relevance_class": statement.excluded.relevance_class,
                "evidence_categories": statement.excluded.evidence_categories,
                "last_evaluated_at": statement.excluded.last_evaluated_at,
                "version": statement.excluded.version,
                "updated_at": sa.func.now(),
            },
        )
        await connection.execute(statement)

    async def list_for_profile(
        self,
        connection: AsyncConnection,
        *,
        search_profile_id: UUID,
        intent_id: UUID | None = None,
        limit: int = 1000,
    ) -> tuple[SourceRelevanceEvaluation, ...]:
        if not 1 <= limit <= 5000:
            raise ValueError("limit must be between 1 and 5000")
        statement = sa.select(source_profile_relevance).where(
            source_profile_relevance.c.search_profile_id == search_profile_id
        )
        if intent_id is not None:
            statement = statement.where(
                source_profile_relevance.c.discovery_intent_id == intent_id
            )
        rows = await connection.execute(
            statement.order_by(
                source_profile_relevance.c.relevance_score.desc(),
                source_profile_relevance.c.source_id,
            ).limit(limit)
        )
        return tuple(_relevance_record(row) for row in rows.mappings())


class ProfileDiscoveryService:
    """Persist intents, run profile Web Discovery, and project relevance."""

    def __init__(
        self,
        database: Database,
        *,
        runner: DiscoveryRunner | None = None,
        intents: ProfileDiscoveryIntentRepository | None = None,
        relevance: SourceProfileRelevanceRepository | None = None,
        web_governor: WebDiscoveryGovernor | None = None,
    ) -> None:
        self._database = database
        self._runner = runner or DiscoveryRunner(database)
        self._intents = intents or ProfileDiscoveryIntentRepository()
        self._relevance = relevance or SourceProfileRelevanceRepository()
        self._sources = SourceRepository()
        self._web_governor = web_governor

    async def list_intents(
        self,
        connection: AsyncConnection,
        *,
        search_profile_id: UUID | None = None,
        limit: int = 100,
    ) -> tuple[ProfileDiscoveryIntent, ...]:
        if search_profile_id is None:
            return await self._intents.list_all(connection, limit=limit)
        return await self._intents.list_for_profile(
            connection,
            search_profile_id=search_profile_id,
            limit=limit,
        )

    async def ensure_intent(
        self,
        profile: SearchProfileRecord,
    ) -> ProfileDiscoveryIntentWriteOutcome:
        intent = build_profile_discovery_intent(profile)
        async with self._database.transaction() as connection:
            return await self._intents.ensure(connection, intent)

    async def ensure_intent_in_connection(
        self,
        connection: AsyncConnection,
        profile: SearchProfileRecord,
    ) -> ProfileDiscoveryIntentWriteOutcome:
        return await self._intents.ensure(
            connection,
            build_profile_discovery_intent(profile),
        )

    async def discover_profile(
        self,
        profile: SearchProfileRecord,
        *,
        requested_at: datetime,
        run_key: str,
        backend: WebSearchBackend | None = None,
        searxng_url: str | None = None,
        results_per_query: int = 10,
        max_candidates: int = 100,
    ) -> ProfileDiscoveryExecution:
        _aware(requested_at, "requested_at")
        outcome = await self.ensure_intent(profile)
        return await self._discover(
            profile_key=str(profile.id),
            intent=outcome.intent,
            requested_at=requested_at,
            run_key=run_key,
            backend=backend,
            searxng_url=searxng_url,
            results_per_query=results_per_query,
            max_candidates=max_candidates,
            persist_relevance=True,
        )

    async def discover_telegram_profile(
        self,
        profile: SearchProfileRecord,
        *,
        requested_at: datetime,
        run_key: str,
        client: Any,
        governor: Any,
        max_queries: int = DEFAULT_MAX_QUERIES,
        results_per_query: int = DEFAULT_RESULTS_PER_QUERY,
        max_candidates: int = 10,
        fresh_run_id: str | None = None,
        fresh_run_started_at: datetime | None = None,
        page_cache: TelegramGlobalSearchPageCache | None = None,
    ) -> ProfileDiscoveryExecution:
        """Run profile-driven Telegram global discovery through the normal runner."""

        _aware(requested_at, "requested_at")
        if fresh_run_started_at is not None:
            _aware(fresh_run_started_at, "fresh_run_started_at")
        outcome = await self.ensure_intent(profile)
        queries = build_telegram_profile_search_queries(
            outcome.intent,
            max_queries=max_queries,
        )
        catalog = PostgresSourceCatalog(self._database)
        known_identities = await catalog.list_known_source_identities(
            platform="telegram",
        )
        provider = TelegramGlobalSearchProvider(
            client,
            governor=governor,
            intent=outcome.intent,
            queries=queries,
            known_source_identities=known_identities,
            max_results_per_query=results_per_query,
            max_candidates=max_candidates,
            page_cache=page_cache,
        )
        parameters: dict[str, Any] = {
            "trigger": "profile_telegram_discovery",
            "profile_discovery": {
                "intent_id": str(outcome.intent.id),
                "profile_revision": outcome.intent.profile_revision,
                "intent_version": outcome.intent.version,
                "strategy_version": TELEGRAM_PROFILE_DISCOVERY_STRATEGY_VERSION,
                "queries": [query.text for query in queries],
                "query_matrix": [
                    {
                        "text": query.text,
                        "family": query.family,
                        "language": query.language,
                        "angle": query.angle,
                        "query_kind": query.query_kind,
                    }
                    for query in queries
                ],
                "query_count": len(queries),
                "results_per_query": results_per_query,
            },
        }
        if fresh_run_id is not None or fresh_run_started_at is not None:
            parameters["fresh_run"] = {
                "run_id": fresh_run_id,
                "started_at": (
                    None
                    if fresh_run_started_at is None
                    else fresh_run_started_at.isoformat()
                ),
            }
        execution = await self._runner.run(
            provider,
            run_key=run_key,
            request=DiscoveryRequest(
                parameters=parameters,
                requested_at=requested_at,
            ),
        )
        async with self._database.transaction() as connection:
            evaluations = await self._evaluate_sources(
                connection,
                intent=outcome.intent,
                source_ids=tuple(result.source_id for result in execution.results),
                evaluated_at=requested_at,
                persist_relevance=True,
            )
            coverage = await self._coverage(
                connection,
                intent=outcome.intent,
                evaluated_at=requested_at,
                persist_relevance=True,
            )
        result_source_ids = {result.source_id for result in execution.results}
        observability = provider.observability
        return ProfileDiscoveryExecution(
            profile_key=str(profile.id),
            intent=outcome.intent,
            execution=execution,
            generated_query_count=len(queries),
            direct_query_count=sum(query.angle == "direct" for query in queries),
            buyer_habitat_query_count=sum(
                query.angle == "buyer_habitat" for query in queries
            ),
            adjacent_query_count=sum(query.angle == "adjacent" for query in queries),
            search_results_considered=int(observability.get("search_results_considered", 0)),
            telegram_like_candidates=int(observability.get("telegram_like_results", 0)),
            unique_candidates=len(result_source_ids),
            known_candidates=sum(
                result.outcome.value == "existing" for result in execution.results
            ),
            new_candidates=sum(
                result.outcome.value == "created" for result in execution.results
            ),
            coverage=coverage,
            candidate_priority_counts=_priority_counts(evaluations),
            provider_observability=dict(observability),
        )

    async def discover_evaluation_profile(
        self,
        spec: EvaluationProfileSpec,
        *,
        requested_at: datetime,
        run_key: str,
        backend: WebSearchBackend | None = None,
        searxng_url: str | None = None,
        results_per_query: int = 10,
        max_candidates: int = 100,
        previous_source_ids: set[int] | None = None,
    ) -> ProfileDiscoveryExecution:
        intent = build_evaluation_intent(spec)
        return await self._discover(
            profile_key=spec.key,
            intent=intent,
            requested_at=requested_at,
            run_key=run_key,
            backend=backend,
            searxng_url=searxng_url,
            results_per_query=results_per_query,
            max_candidates=max_candidates,
            persist_relevance=False,
            previous_source_ids=previous_source_ids,
        )

    async def coverage_for_profile(
        self,
        profile: SearchProfileRecord,
        *,
        evaluated_at: datetime | None = None,
    ) -> ProfileSourceCoverage:
        outcome = await self.ensure_intent(profile)
        timestamp = evaluated_at or datetime.now(timezone.utc)
        _aware(timestamp, "evaluated_at")
        async with self._database.transaction() as connection:
            coverage = await self._coverage(
                connection,
                intent=outcome.intent,
                evaluated_at=timestamp,
                persist_relevance=True,
            )
            if coverage.discovery_priority in {"high", "medium"}:
                await self._ensure_profile_gap_campaign(
                    connection,
                    intent=outcome.intent,
                )
            return coverage

    async def _ensure_profile_gap_campaign(
        self,
        connection: AsyncConnection,
        *,
        intent: ProfileDiscoveryIntent,
    ) -> None:
        if intent.search_profile_id is None:
            return
        spec = profile_gap_campaign_spec(
            profile_id=str(intent.search_profile_id),
            buyer_habitats=intent.buyer_habitats,
            industries=intent.industries,
            specialist_concepts=(*intent.roles, *intent.services, *intent.skills),
            languages=intent.languages or ("en", "ru"),
            geographies=tuple(
                str(value)
                for value in intent.geo_remote.get("geographies", ())
                if isinstance(value, str)
            ),
        )
        repository = DiscoveryCampaignRepository()
        campaign = await repository.ensure_campaign(connection, spec)
        await repository.ensure_queries(
            connection,
            campaign.id,
            generate_campaign_queries(spec),
        )
        await repository.link_profile(
            connection,
            campaign_id=campaign.id,
            search_profile_id=intent.search_profile_id,
            gap_key=spec.gap_key or "profile-gap",
        )
        await repository.enqueue_campaign_plan(connection, campaign=campaign)

    async def _discover(
        self,
        *,
        profile_key: str,
        intent: ProfileDiscoveryIntent,
        requested_at: datetime,
        run_key: str,
        backend: WebSearchBackend | None,
        searxng_url: str | None,
        results_per_query: int,
        max_candidates: int,
        persist_relevance: bool,
        previous_source_ids: set[int] | None = None,
    ) -> ProfileDiscoveryExecution:
        strategy = web_strategy_for_intent(
            intent,
            results_per_query=results_per_query,
            max_candidates=max_candidates,
        )
        if backend is None:
            if not searxng_url:
                raise ValueError("SEARXNG_URL is required for profile Web Discovery")
            backend = SearxngSearchBackend(searxng_url)
        provider = WebDiscoveryProvider(
            backend,
            strategy=strategy,
            governor=self._web_governor,
        )
        execution = await self._runner.run(
            provider,
            run_key=run_key,
            request=DiscoveryRequest(
                parameters={
                    "trigger": "profile_discovery",
                    "profile_discovery": {
                        "intent_id": str(intent.id),
                        "profile_revision": intent.profile_revision,
                        "intent_version": intent.version,
                    },
                },
                requested_at=requested_at,
            ),
        )
        async with self._database.transaction() as connection:
            evaluations = await self._evaluate_sources(
                connection,
                intent=intent,
                source_ids=tuple(result.source_id for result in execution.results),
                evaluated_at=requested_at,
                persist_relevance=persist_relevance,
            )
            coverage = await self._coverage(
                connection,
                intent=intent,
                evaluated_at=requested_at,
                persist_relevance=persist_relevance,
            )
        observability = provider.observability
        result_source_ids = {result.source_id for result in execution.results}
        known = sum(
            1
            for result in execution.results
            if result.outcome.value == "existing"
        )
        return ProfileDiscoveryExecution(
            profile_key=profile_key,
            intent=intent,
            execution=execution,
            generated_query_count=observability["queries_generated"]
            or len(strategy.build_queries(_request_for_stats())),
            direct_query_count=_query_count(strategy, "direct"),
            buyer_habitat_query_count=_query_count(strategy, "buyer_habitat"),
            adjacent_query_count=_query_count(strategy, "adjacent"),
            search_results_considered=observability["search_results_considered"],
            telegram_like_candidates=observability["telegram_like_results"],
            unique_candidates=len(result_source_ids),
            known_candidates=known,
            new_candidates=sum(
                1
                for result in execution.results
                if result.outcome.value == "created"
            ),
            overlap_with_previous_profiles=(
                0
                if previous_source_ids is None
                else len(result_source_ids & previous_source_ids)
            ),
            coverage=coverage,
            candidate_priority_counts=_priority_counts(evaluations),
            provider_observability=dict(observability),
        )

    async def _evaluate_sources(
        self,
        connection: AsyncConnection,
        *,
        intent: ProfileDiscoveryIntent,
        source_ids: Sequence[int],
        evaluated_at: datetime,
        persist_relevance: bool,
    ) -> tuple[SourceRelevanceEvaluation, ...]:
        values: list[SourceRelevanceEvaluation] = []
        for source_id in dict.fromkeys(source_ids):
            source = await self._sources.get(connection, source_id)
            lineages = _lineages_for_intent(
                intent,
                await self._sources.list_lineage(connection, source_id),
            )
            evaluation = evaluate_source_relevance(intent, source, lineages)
            values.append(evaluation)
            if persist_relevance:
                await self._relevance.upsert(
                    connection,
                    evaluation,
                    evaluated_at=evaluated_at,
                )
        return tuple(values)

    async def _coverage(
        self,
        connection: AsyncConnection,
        *,
        intent: ProfileDiscoveryIntent,
        evaluated_at: datetime,
        persist_relevance: bool,
    ) -> ProfileSourceCoverage:
        approved_candidates = await self._sources.list_sources(
            connection,
            platform="telegram",
            limit=1000,
        )
        approved = tuple(
            source
            for source in approved_candidates
            if source.lifecycle_status
            in {SourceStatus.APPROVED, SourceStatus.ACTIVE, SourceStatus.DEGRADED}
        )
        evaluations: list[SourceRelevanceEvaluation] = []
        for source in approved:
            lineages = _lineages_for_intent(
                intent,
                await self._sources.list_lineage(connection, source.id),
            )
            evaluation = evaluate_source_relevance(intent, source, lineages)
            evaluations.append(evaluation)
            if persist_relevance:
                await self._relevance.upsert(
                    connection,
                    evaluation,
                    evaluated_at=evaluated_at,
                )
        return coverage_from_evaluations(len(approved), intent, evaluations)


def _lineages_for_intent(
    intent: ProfileDiscoveryIntent,
    lineages: Sequence[Any],
) -> tuple[Any, ...]:
    """Keep source evidence attributable to this profile intent.

    Repository seed metadata is global source metadata.  Web query/result
    provenance is profile-scoped only when the discovery provider persisted the
    intent marker.  Historical unscoped Web lineages are intentionally excluded
    instead of being allowed to leak another profile's evidence into coverage.
    Test doubles without a provider attribute remain usable as direct semantic
    fixtures.
    """

    selected: list[Any] = []
    intent_id = str(intent.id)
    for lineage in lineages:
        provider = getattr(lineage, "provider", None)
        context = getattr(lineage, "context", {}) or {}
        if provider is None:
            selected.append(lineage)
            continue
        if provider == "repository_seed":
            selected.append(lineage)
            continue
        if provider == "web_search":
            if isinstance(context, Mapping) and context.get(
                "profile_discovery_intent_id"
            ) == intent_id:
                selected.append(lineage)
            continue
        selected.append(lineage)
    return tuple(selected)


def build_profile_discovery_intent(
    profile: SearchProfileRecord,
) -> ProfileDiscoveryIntent:
    preferences = profile.preferences
    languages = tuple(
        term.value for term in (preferences.languages or ())
    ) or infer_languages(profile.semantic_text_normalized)
    geographies = tuple(
        term.value for term in (preferences.geographies or ())
    )
    work_modes = tuple(mode.value for mode in (preferences.work_modes or ()))
    return build_discovery_intent(
        DiscoveryProfileInput(
            identity_key=str(profile.id),
            search_profile_id=profile.id,
            profile_revision=profile.revision,
            roles=tuple(term.value for term in profile.roles),
            services=tuple(term.value for term in profile.categories),
            skills=tuple(term.value for term in profile.skills),
            industries=tuple(term.value for term in profile.categories),
            languages=languages,
            geographies=geographies,
            work_modes=work_modes,
        )
    )


def build_evaluation_intent(spec: EvaluationProfileSpec) -> ProfileDiscoveryIntent:
    return build_discovery_intent(
        DiscoveryProfileInput(
            identity_key=f"evaluation:{spec.key}",
            search_profile_id=None,
            profile_revision=1,
            roles=spec.roles,
            services=spec.services,
            skills=spec.skills,
            industries=spec.industries,
            languages=spec.languages,
            geographies=spec.geographies,
            work_modes=spec.work_modes,
        )
    )


def build_discovery_intent(
    profile: DiscoveryProfileInput,
) -> ProfileDiscoveryIntent:
    roles = _unique(profile.roles)
    services = _unique(profile.services or profile.roles)
    skills = _unique(profile.skills)
    industries = _unique(profile.industries or profile.services)
    literal = _unique((*roles, *services, *skills, *industries))
    adjacent = _unique(
        item
        for term in literal[:8]
        for item in (
            f"{term} hiring",
            f"{term} agency",
            f"{term} implementation",
        )
    )[:24]
    buyer_roles = _unique(
        (
            *(f"teams hiring {role}" for role in roles[:3]),
            *(f"buyers of {service}" for service in services[:3]),
            "founders and operators",
            "product and delivery teams",
            "marketing and growth teams",
            "agencies and studios",
            "hiring and procurement teams",
        )
    )[:16]
    buyer_contexts = (
        "hiring",
        "vendor selection",
        "project delivery",
        "implementation help",
        "recommendations",
    )
    habitats = _unique(
        (
            *(f"{role} hiring communities" for role in roles[:3]),
            *(f"{service} operator communities" for service in services[:3]),
            *(f"{industry} buyer discussions" for industry in industries[:3]),
            "founder and operator communities",
            "agency overflow communities",
            "tool user and implementation communities",
            "vendor recommendation discussions",
        )
    )[:16]
    geo_remote = {
        "languages": list(_unique(profile.languages)),
        "geographies": list(_unique(profile.geographies)),
        "work_modes": list(_unique(profile.work_modes)),
        "remote": not profile.work_modes or WorkMode.REMOTE.value in profile.work_modes,
    }
    intent_id = uuid5(
        NAMESPACE_URL,
        f"{PROFILE_DISCOVERY_INTENT_VERSION}:{profile.identity_key}:"
        f"{profile.profile_revision}",
    )
    provisional = ProfileDiscoveryIntent(
        id=intent_id,
        search_profile_id=profile.search_profile_id,
        profile_revision=profile.profile_revision,
        roles=roles,
        services=services,
        skills=skills,
        industries=industries,
        languages=_unique(profile.languages),
        geo_remote=geo_remote,
        likely_buyer_roles=buyer_roles,
        buyer_contexts=buyer_contexts,
        buyer_habitats=habitats,
        literal_concepts=literal[:32],
        adjacent_concepts=adjacent,
        generated_web_queries=(),
    )
    queries = tuple(
        query.text
        for query in web_strategy_for_intent(provisional).build_queries(
            _request_for_stats()
        )
    )
    return ProfileDiscoveryIntent(
        **{
            **provisional.__dict__,
            "generated_web_queries": queries[:128],
        }
    )


def web_strategy_for_intent(
    intent: ProfileDiscoveryIntent,
    *,
    results_per_query: int = 10,
    max_candidates: int = 100,
) -> WebDiscoveryStrategy:
    topics: list[WebDiscoveryTopic] = []
    for concept in intent.literal_concepts[:4]:
        for language in intent.languages[:2] or ("en",):
            topics.append(
                WebDiscoveryTopic(
                    _topic_category(concept),
                    concept,
                    language,
                    "direct",
                )
            )
    for habitat in intent.buyer_habitats[:3]:
        for language in intent.languages[:2] or ("en",):
            topics.append(
                WebDiscoveryTopic(
                    _topic_category(habitat),
                    habitat,
                    language,
                    "buyer_habitat",
                )
            )
    for concept in intent.adjacent_concepts[:2]:
        for language in intent.languages[:2] or ("en",):
            topics.append(
                WebDiscoveryTopic(
                    _topic_category(concept),
                    concept,
                    language,
                    "adjacent",
                )
            )
    topics = _unique_topics(topics)
    seeds: list[BuyerIntentSeed] = []
    for language in intent.languages[:2] or ("en",):
        if language.casefold().startswith("ru"):
            phrases = ("ищу специалиста", "нужен подрядчик", "посоветуйте исполнителя")
        else:
            phrases = ("looking for a specialist", "need a contractor", "recommend a provider")
        seeds.extend(BuyerIntentSeed(phrase, language) for phrase in phrases)
    return WebDiscoveryStrategy(
        topics=tuple(topics),
        buyer_intent_seeds=tuple(seeds),
        results_per_query=results_per_query,
        max_candidates=max_candidates,
    )


def evaluate_source_relevance(
    intent: ProfileDiscoveryIntent,
    source: Any,
    lineages: Sequence[Any] = (),
) -> SourceRelevanceEvaluation:
    source_texts, result_texts, buyer_texts, query_signal_count = _relevance_corpora(
        source,
        lineages,
    )
    source_corpus = " ".join(source_texts).casefold()
    result_corpus = " ".join(result_texts).casefold()
    buyer_corpus = " ".join(buyer_texts).casefold()
    direct_profession_hits = _matching_terms_strict(intent.roles, source_corpus)
    direct_service_hits = _matching_terms_strict(intent.services, source_corpus)
    supporting_hits = _matching_terms(
        (*intent.skills, *intent.industries),
        source_corpus,
    )
    adjacent_hits = _matching_terms(intent.adjacent_concepts, source_corpus)
    specific_buyer_terms = tuple(
        term
        for term in (*intent.buyer_habitats, *intent.likely_buyer_roles)
        if _buyer_term_is_specific(term, intent)
    )
    generic_buyer_terms = tuple(
        term
        for term in (*intent.buyer_habitats, *intent.likely_buyer_roles)
        if not _buyer_term_is_specific(term, intent)
    )
    specific_buyer_hits = _matching_terms_strict(specific_buyer_terms, buyer_corpus)
    generic_buyer_hits = _matching_terms_strict(generic_buyer_terms, buyer_corpus)
    result_title_snippet_hits = _result_semantic_hits(
        intent,
        result_corpus,
    )

    evidence: list[str] = []
    if direct_profession_hits or direct_service_hits:
        evidence.extend(("direct_concept",))
    if direct_profession_hits:
        evidence.append("direct_profession")
    if direct_service_hits:
        evidence.append("direct_service")
    if supporting_hits:
        evidence.append("supporting_context")
    if specific_buyer_hits:
        evidence.append("buyer_habitat")
        evidence.append("specific_buyer_habitat")
    elif generic_buyer_hits:
        evidence.append("generic_buyer_habitat")
    if adjacent_hits:
        evidence.append("adjacent_concept")
    if query_signal_count >= 2:
        evidence.append("query_diversity")
    if getattr(source, "platform", None) == "telegram":
        evidence.append("telegram_source_likeness")

    components: list[tuple[str, Decimal]] = [("base", Decimal("0.03"))]
    if direct_profession_hits:
        components.append(
            (
                "direct_profession",
                Decimal("0.76")
                + min(Decimal("0.12"), Decimal(direct_profession_hits - 1) * Decimal("0.04")),
            )
        )
    if direct_service_hits:
        components.append(
            (
                "direct_service",
                Decimal("0.74")
                + min(Decimal("0.12"), Decimal(direct_service_hits - 1) * Decimal("0.04")),
            )
        )
    if supporting_hits:
        components.append(
            (
                "supporting_context",
                min(Decimal("0.12"), Decimal(supporting_hits) * Decimal("0.04")),
            )
        )
    if specific_buyer_hits:
        components.append(
            (
                "specific_buyer_habitat",
                Decimal("0.28")
                + min(Decimal("0.12"), Decimal(specific_buyer_hits - 1) * Decimal("0.04")),
            )
        )
    if generic_buyer_hits:
        components.append(("generic_buyer_habitat", Decimal("0.04")))
    if adjacent_hits:
        components.append(
            (
                "adjacent_context",
                min(Decimal("0.12"), Decimal(adjacent_hits) * Decimal("0.04")),
            )
        )
    if (direct_profession_hits or direct_service_hits) and supporting_hits:
        components.append(("evidence_diversity", Decimal("0.06")))
    if (direct_profession_hits or direct_service_hits) and specific_buyer_hits:
        components.append(("buyer_confirmation", Decimal("0.08")))
    if specific_buyer_hits and supporting_hits:
        components.append(("buyer_support", Decimal("0.08")))
    if query_signal_count >= 2 and (
        direct_profession_hits
        or direct_service_hits
        or supporting_hits
        or specific_buyer_hits
    ):
        components.append(("query_diversity", Decimal("0.03")))

    score = _score_components(components)
    relevance_class = _relevance_class(score)
    semantic_category = _semantic_category(
        direct_profession_hits=direct_profession_hits,
        direct_service_hits=direct_service_hits,
        supporting_hits=supporting_hits,
        specific_buyer_hits=specific_buyer_hits,
        generic_buyer_hits=generic_buyer_hits,
        adjacent_hits=adjacent_hits,
    )
    diagnostic_label = _diagnostic_label(
        relevance_class,
        direct_profession_hits=direct_profession_hits,
        direct_service_hits=direct_service_hits,
        supporting_hits=supporting_hits,
        specific_buyer_hits=specific_buyer_hits,
        generic_buyer_hits=generic_buyer_hits,
        adjacent_hits=adjacent_hits,
    )
    independent_evidence_families = sum(
        bool(value)
        for value in (
            direct_profession_hits,
            direct_service_hits,
            supporting_hits,
            specific_buyer_hits,
            generic_buyer_hits,
            adjacent_hits,
        )
    )
    primary_evidence_family = _primary_evidence_family(
        direct_profession_hits=direct_profession_hits,
        direct_service_hits=direct_service_hits,
        supporting_hits=supporting_hits,
        specific_buyer_hits=specific_buyer_hits,
        generic_buyer_hits=generic_buyer_hits,
        adjacent_hits=adjacent_hits,
    )
    priority_class = _priority_class(
        relevance_class,
        independent_evidence_families=independent_evidence_families,
    )
    why = _relevance_why(
        semantic_category,
        direct_profession_hits=direct_profession_hits,
        direct_service_hits=direct_service_hits,
        supporting_hits=supporting_hits,
        specific_buyer_hits=specific_buyer_hits,
        generic_buyer_hits=generic_buyer_hits,
        adjacent_hits=adjacent_hits,
        query_signal_count=query_signal_count,
        result_title_snippet_hits=result_title_snippet_hits,
    )
    explanation = RelevanceExplanation(
        semantic_category=semantic_category,
        direct_profession_hits=direct_profession_hits,
        direct_service_hits=direct_service_hits,
        supporting_hits=supporting_hits,
        specific_buyer_hits=specific_buyer_hits,
        generic_buyer_hits=generic_buyer_hits,
        adjacent_hits=adjacent_hits,
        query_signal_count=query_signal_count,
        score_components=tuple(components),
        diagnostic_label=diagnostic_label,
        why=why,
        result_title_snippet_hits=result_title_snippet_hits,
        independent_evidence_families=independent_evidence_families,
        primary_evidence_family=primary_evidence_family,
        priority_class=priority_class,
    )
    return SourceRelevanceEvaluation(
        source_id=int(source.id),
        search_profile_id=intent.search_profile_id,
        discovery_intent_id=intent.id,
        profile_revision=intent.profile_revision,
        relevance_score=score,
        relevance_class=relevance_class,
        evidence_categories=tuple(dict.fromkeys(evidence)),
        explanation=explanation,
    )


def evaluate_source_relevance_legacy(
    intent: ProfileDiscoveryIntent,
    source: Any,
    lineages: Sequence[Any] = (),
) -> SourceRelevanceEvaluation:
    """Reproduce the v1 calculation for offline before/after diagnostics only."""

    texts = [
        str(getattr(source, "display_name", "")),
        str(getattr(source, "external_id", "")),
    ]
    query_angles: set[str] = set()
    query_count = 0
    for lineage in lineages:
        context = getattr(lineage, "context", {}) or {}
        _append_context_text(context, texts, query_angles)
        matches = context.get("matches") if isinstance(context, Mapping) else None
        if isinstance(matches, list):
            query_count += len(matches)
            for match in matches:
                _append_context_text(match, texts, query_angles)
    corpus = " ".join(texts).casefold()
    direct_hits = _matching_terms(intent.literal_concepts, corpus)
    adjacent_hits = _matching_terms(intent.adjacent_concepts, corpus)
    buyer_hits = _matching_terms(
        (*intent.buyer_habitats, *intent.likely_buyer_roles),
        corpus,
    )
    if "buyer_habitat" in query_angles or "buyer_intent" in query_angles:
        buyer_hits = max(buyer_hits, 1)
    evidence: list[str] = []
    if direct_hits:
        evidence.append("direct_concept")
    if buyer_hits:
        evidence.append("buyer_habitat")
    if adjacent_hits:
        evidence.append("adjacent_concept")
    if query_count >= 2:
        evidence.append("query_diversity")
    if getattr(source, "platform", None) == "telegram":
        evidence.append("telegram_source_likeness")
    score = Decimal("0.05")
    if direct_hits:
        score = max(
            score,
            Decimal("0.55")
            + min(Decimal("0.20"), Decimal(direct_hits - 1) * Decimal("0.08")),
        )
    if buyer_hits:
        score = max(
            score,
            Decimal("0.45")
            + min(Decimal("0.15"), Decimal(buyer_hits - 1) * Decimal("0.05")),
        )
    if adjacent_hits:
        score = max(
            score,
            Decimal("0.30")
            + min(Decimal("0.10"), Decimal(adjacent_hits - 1) * Decimal("0.04")),
        )
    if direct_hits and buyer_hits:
        score += Decimal("0.15")
    if query_count >= 2:
        score += Decimal("0.05")
    score = min(Decimal("1"), score)
    return SourceRelevanceEvaluation(
        source_id=int(source.id),
        search_profile_id=intent.search_profile_id,
        discovery_intent_id=intent.id,
        profile_revision=intent.profile_revision,
        relevance_score=score,
        relevance_class=_relevance_class(score),
        evidence_categories=tuple(dict.fromkeys(evidence)),
    )


def _relevance_corpora(
    source: Any,
    lineages: Sequence[Any],
) -> tuple[list[str], list[str], list[str], int]:
    """Split semantic evidence from query/provenance metadata.

    Search text, URLs, handles and query angles are provenance, not source
    meaning.  Result titles/snippets and curated seed tags/reasons are the only
    lineage values admitted into the semantic corpus.
    """

    source_texts = [str(getattr(source, "display_name", ""))]
    result_texts: list[str] = []
    buyer_texts: list[str] = []
    query_signals: set[tuple[str, str, str]] = set()
    for lineage in lineages:
        context = getattr(lineage, "context", {}) or {}
        provider = getattr(lineage, "provider", None)
        if not isinstance(context, Mapping):
            continue
        if provider == "repository_seed":
            for key in ("tags", "reason"):
                _append_scalar_text(context.get(key), source_texts)
            continue
        matches = context.get("matches")
        if isinstance(matches, list):
            for match in matches:
                if not isinstance(match, Mapping):
                    continue
                for key in ("result_title", "result_snippet"):
                    _append_scalar_text(match.get(key), source_texts)
                    _append_scalar_text(match.get(key), result_texts)
                topic = match.get("topic")
                if isinstance(topic, str):
                    buyer_texts.append(topic)
                query_signals.add(
                    (
                        str(match.get("query_angle") or "unknown"),
                        str(match.get("query_kind") or "unknown"),
                        str(match.get("topic") or "unknown"),
                    )
                )
            continue
        for key in ("title", "snippet", "summary", "description"):
            _append_scalar_text(context.get(key), source_texts)
            if key in {"title", "snippet"}:
                _append_scalar_text(context.get(key), result_texts)
    return source_texts, result_texts, buyer_texts, len(query_signals)


def _append_scalar_text(value: Any, target: list[str]) -> None:
    if isinstance(value, str) and value.strip():
        target.append(value)
    elif isinstance(value, (int, float)):
        target.append(str(value))


def _buyer_term_is_specific(
    term: str,
    intent: ProfileDiscoveryIntent,
) -> bool:
    term_corpus = term.casefold()
    return any(
        _phrase_match(candidate, term_corpus)
        for candidate in (
            *intent.roles,
            *intent.services,
            *intent.skills,
            *intent.industries,
        )
    )


def _score_components(components: Sequence[tuple[str, Decimal]]) -> Decimal:
    base = next(
        (value for name, value in components if name == "base"),
        Decimal("0"),
    )
    primary_names = {
        "direct_profession",
        "direct_service",
        "supporting_context",
        "specific_buyer_habitat",
        "generic_buyer_habitat",
        "adjacent_context",
    }
    primary = max(
        (value for name, value in components if name in primary_names),
        default=Decimal("0"),
    )
    additive = sum(
        (
            value
            for name, value in components
            if name not in primary_names and name != "base"
        ),
        Decimal("0"),
    )
    return min(Decimal("1"), base + primary + additive)


def _relevance_class(score: Decimal) -> str:
    return (
        "strong"
        if score >= Decimal("0.75")
        else "adequate"
        if score >= Decimal("0.45")
        else "weak"
    )


def _semantic_category(
    *,
    direct_profession_hits: int,
    direct_service_hits: int,
    supporting_hits: int,
    specific_buyer_hits: int,
    generic_buyer_hits: int,
    adjacent_hits: int,
) -> str:
    if direct_profession_hits or direct_service_hits:
        return "direct_specialist"
    if specific_buyer_hits:
        return "buyer_habitat"
    if supporting_hits or adjacent_hits:
        return "supporting_or_adjacent"
    if generic_buyer_hits:
        return "generic_business"
    return "unrelated_or_unknown"


def _result_semantic_hits(
    intent: ProfileDiscoveryIntent,
    result_corpus: str,
) -> int:
    if not result_corpus:
        return 0
    terms = (
        *intent.roles,
        *intent.services,
        *intent.skills,
        *intent.industries,
        *intent.buyer_habitats,
        *intent.likely_buyer_roles,
        *intent.adjacent_concepts,
    )
    return len({term.casefold() for term in terms if _strict_phrase_match(term, result_corpus)})


def _primary_evidence_family(
    *,
    direct_profession_hits: int,
    direct_service_hits: int,
    supporting_hits: int,
    specific_buyer_hits: int,
    generic_buyer_hits: int,
    adjacent_hits: int,
) -> str:
    if direct_profession_hits:
        return "direct_role"
    if direct_service_hits:
        return "direct_service"
    if specific_buyer_hits:
        return "specific_buyer_habitat"
    if supporting_hits:
        return "supporting_context"
    if adjacent_hits:
        return "adjacent_context"
    if generic_buyer_hits:
        return "generic_buyer_habitat"
    return "none"


def _priority_class(
    relevance_class: str,
    *,
    independent_evidence_families: int,
) -> str:
    if relevance_class == "strong":
        return "HIGH"
    if relevance_class == "adequate":
        return "MEDIUM"
    if independent_evidence_families == 0:
        return "INSUFFICIENT"
    return "LOW"


def _diagnostic_label(
    relevance_class: str,
    *,
    direct_profession_hits: int,
    direct_service_hits: int,
    supporting_hits: int,
    specific_buyer_hits: int,
    generic_buyer_hits: int,
    adjacent_hits: int,
) -> str:
    if (direct_profession_hits or direct_service_hits) and relevance_class == "strong":
        return "CLEARLY_RELEVANT"
    if (
        specific_buyer_hits
        and (supporting_hits or adjacent_hits)
        and relevance_class != "weak"
    ):
        return "PLAUSIBLY_RELEVANT"
    if generic_buyer_hits or supporting_hits or adjacent_hits:
        return "WEAK"
    return "CLEARLY_IRRELEVANT"


def _relevance_why(
    semantic_category: str,
    *,
    direct_profession_hits: int,
    direct_service_hits: int,
    supporting_hits: int,
    specific_buyer_hits: int,
    generic_buyer_hits: int,
    adjacent_hits: int,
    query_signal_count: int,
    result_title_snippet_hits: int = 0,
) -> str:
    details = [
        f"category={semantic_category}",
        f"direct_profession={direct_profession_hits}",
        f"direct_service={direct_service_hits}",
        f"supporting={supporting_hits}",
        f"specific_buyer={specific_buyer_hits}",
        f"generic_buyer={generic_buyer_hits}",
        f"adjacent={adjacent_hits}",
        f"query_signals={query_signal_count}",
        f"result_title_snippet_hits={result_title_snippet_hits}",
    ]
    return "; ".join(details)


def coverage_from_evaluations(
    approved_total: int,
    intent: ProfileDiscoveryIntent,
    evaluations: Sequence[SourceRelevanceEvaluation],
) -> ProfileSourceCoverage:
    direct = sum("direct_concept" in item.evidence_categories for item in evaluations)
    buyer = sum("buyer_habitat" in item.evidence_categories for item in evaluations)
    relevant = sum(item.relevance_class != "weak" for item in evaluations)
    weak = sum(item.relevance_class == "weak" for item in evaluations)
    adequate = sum(item.relevance_class == "adequate" for item in evaluations)
    strong = sum(item.relevance_class == "strong" for item in evaluations)
    if not relevant or not direct:
        priority = "high"
    elif relevant < 3 or weak > relevant:
        priority = "medium"
    else:
        priority = "low"
    return ProfileSourceCoverage(
        approved_total=approved_total,
        relevant=relevant,
        direct=direct,
        buyer_habitat=buyer,
        weak=weak,
        adequate=adequate,
        strong=strong,
        discovery_priority=priority,
    )


def _priority_counts(
    evaluations: Sequence[SourceRelevanceEvaluation],
) -> Mapping[str, int]:
    counts = {"high": 0, "medium": 0, "low": 0}
    for evaluation in evaluations:
        if evaluation.relevance_class == "strong":
            counts["high"] += 1
        elif evaluation.relevance_class == "adequate":
            counts["medium"] += 1
        else:
            counts["low"] += 1
    return counts


def evaluation_profile_specs() -> tuple[EvaluationProfileSpec, ...]:
    return (
        EvaluationProfileSpec(
            "python-telegram-developer",
            "Python / Telegram Developer",
            ("Python developer", "backend developer", "Telegram bot developer"),
            (
                "Telegram bots",
                "automation",
                "parsers",
                "integrations",
                "backend development",
            ),
            ("Python", "FastAPI", "Telethon", "PostgreSQL"),
            ("software", "automation", "messaging", "backend"),
        ),
        EvaluationProfileSpec(
            "product-ux-designer",
            "Product / UX Designer",
            ("Product Designer", "UX/UI Designer"),
            (
                "Figma",
                "UX",
                "UI",
                "Design Systems",
                "SaaS",
                "mobile/web products",
            ),
            ("Figma", "prototyping", "user research", "design systems"),
            ("SaaS", "mobile apps", "web products"),
        ),
        EvaluationProfileSpec(
            "graphic-brand-designer",
            "Graphic / Brand Designer",
            ("Graphic Designer", "Brand Designer"),
            (
                "branding",
                "visual identity",
                "social media creatives",
                "presentations",
                "graphic design",
            ),
            ("Adobe Illustrator", "Photoshop", "typography", "identity systems"),
            ("consumer brands", "retail", "marketing"),
        ),
        EvaluationProfileSpec(
            "smm-performance-marketer",
            "SMM / Performance Marketer",
            ("SMM Manager", "Social Media Manager", "Performance Marketer"),
            (
                "social media management",
                "paid advertising",
                "content strategy",
                "analytics",
                "acquisition",
            ),
            ("Meta Ads", "Google Ads", "analytics", "content strategy"),
            ("ecommerce", "SaaS", "local business", "media"),
        ),
        EvaluationProfileSpec(
            "video-motion-editor",
            "Video Editor / Motion Designer",
            ("Video Editor", "Motion Designer"),
            (
                "video editing",
                "motion graphics",
                "Reels",
                "Shorts",
                "short-form content",
            ),
            ("After Effects", "Premiere Pro", "DaVinci Resolve", "animation"),
            ("media", "advertising", "creators", "education"),
        ),
        EvaluationProfileSpec(
            "photographer-content-creator",
            "Photographer / Content Creator",
            ("Photographer", "Content Creator", "UGC Creator"),
            ("photography", "UGC", "branded content", "content production"),
            ("lighting", "retouching", "storytelling", "Instagram"),
            ("retail", "hospitality", "fashion", "food"),
        ),
        EvaluationProfileSpec(
            "copywriter-content-manager",
            "Copywriter / Content Manager",
            ("Copywriter", "Content Writer", "Content Manager"),
            ("copywriting", "editorial", "SEO content", "Telegram/content production"),
            ("SEO", "English", "Russian", "editorial planning"),
            ("media", "SaaS", "education", "ecommerce"),
        ),
        EvaluationProfileSpec(
            "recruiter-hr",
            "Recruiter / HR",
            ("Recruiter", "Talent Acquisition", "HR"),
            ("sourcing", "hiring", "interviewing", "recruiting"),
            ("Boolean search", "ATS", "interviewing", "talent acquisition"),
            ("technology", "startups", "outsourcing", "professional services"),
        ),
        EvaluationProfileSpec(
            "sales-business-development",
            "Sales / Business Development",
            ("Sales Manager", "Business Development Manager"),
            ("B2B sales", "outreach", "lead generation", "partnerships"),
            ("CRM", "prospecting", "negotiation", "pipeline management"),
            ("B2B", "SaaS", "services", "technology"),
        ),
        EvaluationProfileSpec(
            "ecommerce-marketplace",
            "E-commerce / Marketplace",
            ("Marketplace Manager", "E-commerce Specialist"),
            (
                "Ozon",
                "Wildberries",
                "Amazon",
                "marketplace operations",
                "product cards",
                "marketplace growth",
            ),
            ("product listings", "store management", "catalog optimization", "unit economics"),
            ("ecommerce", "retail", "consumer goods", "marketplaces"),
        ),
    )


def infer_languages(text: str) -> tuple[str, ...]:
    has_cyrillic = bool(re.search(r"[а-яё]", text.casefold()))
    has_latin = bool(re.search(r"[a-z]", text.casefold()))
    if has_cyrillic and has_latin:
        return ("ru", "en")
    return ("ru",) if has_cyrillic else ("en",)


def _intent_record(row: Mapping[str, Any]) -> ProfileDiscoveryIntent:
    return ProfileDiscoveryIntent(
        id=row["id"],
        search_profile_id=row["search_profile_id"],
        profile_revision=int(row["profile_revision"]),
        roles=tuple(row["roles"]),
        services=tuple(row["services"]),
        skills=tuple(row["skills"]),
        industries=tuple(row["industries"]),
        languages=tuple(row["languages"]),
        geo_remote=dict(row["geo_remote"]),
        likely_buyer_roles=tuple(row["likely_buyer_roles"]),
        buyer_contexts=tuple(row["buyer_contexts"]),
        buyer_habitats=tuple(row["buyer_habitats"]),
        literal_concepts=tuple(row["literal_concepts"]),
        adjacent_concepts=tuple(row["adjacent_concepts"]),
        generated_web_queries=tuple(row["generated_web_queries"]),
        version=str(row["version"]),
    )


def _relevance_record(row: Mapping[str, Any]) -> SourceRelevanceEvaluation:
    return SourceRelevanceEvaluation(
        source_id=int(row["source_id"]),
        search_profile_id=row["search_profile_id"],
        discovery_intent_id=row["discovery_intent_id"],
        profile_revision=int(row["profile_revision"]),
        relevance_score=Decimal(str(row["relevance_score"])),
        relevance_class=str(row["relevance_class"]),
        evidence_categories=tuple(row["evidence_categories"]),
    )


def _matching_terms(terms: Iterable[str], corpus: str) -> int:
    return sum(1 for term in _unique(terms) if _phrase_match(term, corpus))


def _matching_terms_strict(terms: Iterable[str], corpus: str) -> int:
    return sum(1 for term in _unique(terms) if _strict_phrase_match(term, corpus))


def _strict_phrase_match(term: str, corpus: str) -> bool:
    normalized = re.sub(r"\s+", " ", term.casefold()).strip()
    if not normalized:
        return False
    if normalized in corpus:
        return True
    tokens = [token for token in re.findall(r"[\w-]+", normalized) if len(token) >= 4]
    return bool(tokens) and all(token in corpus for token in tokens)


def _phrase_match(term: str, corpus: str) -> bool:
    normalized = re.sub(r"\s+", " ", term.casefold()).strip()
    if not normalized:
        return False
    if normalized in corpus:
        return True
    tokens = [token for token in re.findall(r"[\w-]+", normalized) if len(token) >= 4]
    return bool(tokens) and sum(token in corpus for token in tokens) >= max(1, len(tokens) // 2 + len(tokens) % 2)


def _append_context_text(
    value: Any,
    texts: list[str],
    query_angles: set[str],
) -> None:
    if isinstance(value, Mapping):
        angle = value.get("query_angle")
        if isinstance(angle, str):
            query_angles.add(angle)
        for nested in value.values():
            if isinstance(nested, (str, int, float)):
                texts.append(str(nested))
            elif isinstance(nested, (Mapping, list, tuple)):
                _append_context_text(nested, texts, query_angles)
    elif isinstance(value, (list, tuple)):
        for nested in value:
            _append_context_text(nested, texts, query_angles)


def _unique(values: Iterable[str]) -> tuple[str, ...]:
    result: list[str] = []
    seen: set[str] = set()
    for value in values:
        normalized = value.strip()
        identity = normalized.casefold()
        if not normalized or identity in seen:
            continue
        seen.add(identity)
        result.append(normalized)
    return tuple(result)


def _unique_topics(values: Iterable[WebDiscoveryTopic]) -> list[WebDiscoveryTopic]:
    result: list[WebDiscoveryTopic] = []
    seen: set[tuple[str, str, str]] = set()
    for value in values:
        key = (value.phrase.casefold(), value.language, value.angle)
        if key in seen:
            continue
        seen.add(key)
        result.append(value)
    return result


def _topic_category(value: str) -> CommunityCategory:
    lowered = value.casefold()
    if any(token in lowered for token in ("figma", "adobe", "python", "fastapi", "telethon", "postgresql", "crm", "ats", "amazon", "ozon", "wildberries")):
        return CommunityCategory.TOOL
    if any(token in lowered for token in ("founder", "operator", "buyer", "seller", "sales", "recruit", "hr", "marketplace", "ecommerce")):
        return CommunityCategory.BUSINESS
    if any(token in lowered for token in ("industry", "retail", "saas", "media", "hospitality", "fashion", "education")):
        return CommunityCategory.INDUSTRY
    return CommunityCategory.PROFESSION


def _query_count(strategy: WebDiscoveryStrategy, angle: str) -> int:
    return sum(2 for topic in strategy.topics if topic.angle == angle)


def _request_for_stats() -> DiscoveryRequest:
    return DiscoveryRequest(
        parameters={},
        requested_at=datetime(2026, 1, 1, tzinfo=timezone.utc),
    )


def _aware(value: datetime, field: str) -> None:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field} must include a timezone")
