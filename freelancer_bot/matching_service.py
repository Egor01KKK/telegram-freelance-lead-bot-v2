from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
import logging
from time import monotonic
from uuid import UUID

from .match_decisions import (
    MatchDecisionPolicy,
    MatchScoringInput,
    decide_and_rank_matches,
)
from .matching import (
    CandidateNarrowingResult,
    StructuredScoringPolicy,
    StructuredScoringResult,
    narrow_and_filter_candidates,
    score_narrowed_candidates,
)
from .metrics import MetricNames, MetricsSink, NoOpMetrics
from .observability import log_event
from .persistence.database import Database
from .persistence.opportunities import (
    CanonicalOpportunityRepository,
    OpportunityNotFound,
)
from .persistence.search_profiles import SearchProfileRepository
from .persistence.matches import MatchPersistenceOutcome, MatchTraceRepository
from .persistence.source_metrics import SourceMetricsRepository
from .semantic_matching import (
    DeterministicHashEmbeddingProvider,
    SemanticEmbeddingProvider,
    SemanticMatchingPolicy,
    SemanticScoringResult,
    score_candidates_semantic,
)


@dataclass(frozen=True)
class MatchGenerationReport:
    opportunity_count: int
    active_profile_count: int
    candidate_pair_count: int
    hard_rejected_count: int
    eligible_match_count: int
    semantic_available_count: int
    semantic_degraded_count: int
    user_specific_llm_calls: int
    opportunity_analyzer_calls: int
    elapsed_seconds: float


@dataclass(frozen=True)
class MatchGenerationOutcome:
    persistence: MatchPersistenceOutcome
    report: MatchGenerationReport


class CandidateMatchingService:
    def __init__(
        self,
        database: Database,
        *,
        opportunities: CanonicalOpportunityRepository | None = None,
        profiles: SearchProfileRepository | None = None,
        source_metrics: SourceMetricsRepository | None = None,
        match_traces: MatchTraceRepository | None = None,
        semantic_provider: SemanticEmbeddingProvider | None = None,
        metrics: MetricsSink | None = None,
        logger: logging.Logger | None = None,
    ) -> None:
        self._database = database
        self._opportunities = opportunities or CanonicalOpportunityRepository()
        self._profiles = profiles or SearchProfileRepository()
        self._source_metrics = source_metrics or SourceMetricsRepository()
        self._match_traces = match_traces or MatchTraceRepository()
        self._semantic_provider = (
            semantic_provider or DeterministicHashEmbeddingProvider()
        )
        self._metrics = metrics or NoOpMetrics()
        self._logger = logger or logging.getLogger(__name__)

    async def candidates_for_opportunity(
        self,
        opportunity_id: UUID,
    ) -> CandidateNarrowingResult:
        async with self._database.connect() as connection:
            opportunity = await self._opportunities.get(connection, opportunity_id)
            if opportunity is None:
                raise OpportunityNotFound(
                    f"Opportunity {opportunity_id} does not exist"
                )
            profiles = await self._profiles.list_active(connection)
        return narrow_and_filter_candidates(opportunity, profiles)

    async def structured_scores_for_opportunity(
        self,
        opportunity_id: UUID,
        *,
        policy: StructuredScoringPolicy | None = None,
    ) -> StructuredScoringResult:
        async with self._database.connect() as connection:
            opportunity = await self._opportunities.get(connection, opportunity_id)
            if opportunity is None:
                raise OpportunityNotFound(
                    f"Opportunity {opportunity_id} does not exist"
                )
            profiles = await self._profiles.list_active(connection)
            source_quality = None
            if opportunity.preferred_source is not None:
                source_quality = (
                    await self._source_metrics.get_latest_quality_snapshot(
                        connection,
                        opportunity.preferred_source.source_id,
                    )
                )
        return score_narrowed_candidates(
            opportunity,
            profiles,
            source_quality=source_quality,
            policy=policy,
        )

    async def semantic_scores_for_opportunity(
        self,
        opportunity_id: UUID,
        *,
        structured_policy: StructuredScoringPolicy | None = None,
        semantic_policy: SemanticMatchingPolicy | None = None,
    ) -> SemanticScoringResult:
        async with self._database.connect() as connection:
            opportunity = await self._opportunities.get(connection, opportunity_id)
            if opportunity is None:
                raise OpportunityNotFound(
                    f"Opportunity {opportunity_id} does not exist"
                )
            profiles = await self._profiles.list_active(connection)
            source_quality = None
            if opportunity.preferred_source is not None:
                source_quality = (
                    await self._source_metrics.get_latest_quality_snapshot(
                        connection,
                        opportunity.preferred_source.source_id,
                    )
                )
        return score_candidates_semantic(
            opportunity,
            profiles,
            provider=self._semantic_provider,
            source_quality=source_quality,
            structured_policy=structured_policy,
            semantic_policy=semantic_policy,
        )

    async def evaluate_and_persist(
        self,
        opportunity_ids: tuple[UUID, ...],
        *,
        evaluated_at: datetime,
        structured_policy: StructuredScoringPolicy | None = None,
        semantic_policy: SemanticMatchingPolicy | None = None,
        decision_policy: MatchDecisionPolicy | None = None,
    ) -> MatchPersistenceOutcome:
        generated = await self.generate_matches(
            opportunity_ids,
            evaluated_at=evaluated_at,
            structured_policy=structured_policy,
            semantic_policy=semantic_policy,
            decision_policy=decision_policy,
        )
        return generated.persistence

    async def generate_matches(
        self,
        opportunity_ids: tuple[UUID, ...],
        *,
        evaluated_at: datetime,
        structured_policy: StructuredScoringPolicy | None = None,
        semantic_policy: SemanticMatchingPolicy | None = None,
        decision_policy: MatchDecisionPolicy | None = None,
    ) -> MatchGenerationOutcome:
        if len(set(opportunity_ids)) != len(opportunity_ids):
            raise ValueError("opportunity_ids must be unique")
        started_at = monotonic()
        selected_structured_policy = structured_policy or StructuredScoringPolicy()
        selected_semantic_policy = semantic_policy or SemanticMatchingPolicy()
        try:
            async with self._database.transaction() as connection:
                profiles = await self._profiles.list_active(connection)
                scoring_inputs: list[MatchScoringInput] = []
                for opportunity_id in opportunity_ids:
                    opportunity = await self._opportunities.get(
                        connection,
                        opportunity_id,
                    )
                    if opportunity is None:
                        raise OpportunityNotFound(
                            f"Opportunity {opportunity_id} does not exist"
                        )
                    source_quality = None
                    if opportunity.preferred_source is not None:
                        source_quality = (
                            await self._source_metrics.get_latest_quality_snapshot(
                                connection,
                                opportunity.preferred_source.source_id,
                            )
                        )
                    scoring_inputs.append(
                        MatchScoringInput(
                            opportunity=opportunity,
                            profiles=profiles,
                            semantic=score_candidates_semantic(
                                opportunity,
                                profiles,
                                provider=self._semantic_provider,
                                source_quality=source_quality,
                                structured_policy=selected_structured_policy,
                                semantic_policy=selected_semantic_policy,
                            ),
                            structured_policy=selected_structured_policy,
                            semantic_policy=selected_semantic_policy,
                        )
                    )
                batch = decide_and_rank_matches(
                    tuple(scoring_inputs),
                    evaluated_at=evaluated_at,
                    policy=decision_policy,
                )
                persistence = await self._match_traces.persist_batch(
                    connection,
                    batch,
                )
        except Exception as error:
            self._metrics.increment(
                MetricNames.MATCHING_BATCH_FAILURES,
                tags={"error_type": type(error).__name__},
            )
            log_event(
                self._logger,
                logging.ERROR,
                "matching.batch_failed",
                opportunity_count=len(opportunity_ids),
                error_type=type(error).__name__,
            )
            raise

        report = _generation_report(
            persistence,
            opportunity_count=len(opportunity_ids),
            active_profile_count=len(profiles),
            elapsed_seconds=monotonic() - started_at,
        )
        self._record_generation(persistence, report)
        return MatchGenerationOutcome(persistence=persistence, report=report)

    def _record_generation(
        self,
        persistence: MatchPersistenceOutcome,
        report: MatchGenerationReport,
    ) -> None:
        tags = {
            "created": persistence.created,
            "algorithm_version": persistence.run.algorithm_version,
        }
        self._metrics.increment(MetricNames.MATCHING_BATCHES, tags=tags)
        self._metrics.observe(
            MetricNames.MATCHING_BATCH_SECONDS,
            report.elapsed_seconds,
            tags=tags,
        )
        self._metrics.gauge(
            MetricNames.MATCHING_OPPORTUNITIES,
            report.opportunity_count,
        )
        self._metrics.gauge(
            MetricNames.MATCHING_ACTIVE_PROFILES,
            report.active_profile_count,
        )
        self._metrics.increment(
            MetricNames.MATCHING_PAIRS_EVALUATED,
            report.candidate_pair_count,
            tags=tags,
        )
        trace_metric = (
            MetricNames.MATCHING_TRACES_CREATED
            if persistence.created
            else MetricNames.MATCHING_TRACES_REUSED
        )
        self._metrics.increment(trace_metric, report.candidate_pair_count)
        if persistence.created:
            self._metrics.increment(
                MetricNames.MATCHES,
                report.eligible_match_count,
            )
        self._metrics.gauge(
            MetricNames.MATCHING_USER_SPECIFIC_LLM_CALLS,
            report.user_specific_llm_calls,
        )
        self._metrics.gauge(
            MetricNames.MATCHING_OPPORTUNITY_ANALYZER_CALLS,
            report.opportunity_analyzer_calls,
        )
        log_event(
            self._logger,
            logging.INFO,
            "matching.batch_completed",
            run_id=persistence.run.id,
            idempotency_key=persistence.run.idempotency_key,
            created=persistence.created,
            opportunity_count=report.opportunity_count,
            active_profile_count=report.active_profile_count,
            candidate_pair_count=report.candidate_pair_count,
            hard_rejected_count=report.hard_rejected_count,
            eligible_match_count=report.eligible_match_count,
            semantic_available_count=report.semantic_available_count,
            semantic_degraded_count=report.semantic_degraded_count,
            user_specific_llm_calls=report.user_specific_llm_calls,
            opportunity_analyzer_calls=report.opportunity_analyzer_calls,
            elapsed_seconds=round(report.elapsed_seconds, 6),
        )


def _generation_report(
    persistence: MatchPersistenceOutcome,
    *,
    opportunity_count: int,
    active_profile_count: int,
    elapsed_seconds: float,
) -> MatchGenerationReport:
    traces = tuple(record.trace for record in persistence.traces)
    return MatchGenerationReport(
        opportunity_count=opportunity_count,
        active_profile_count=active_profile_count,
        candidate_pair_count=len(traces),
        hard_rejected_count=sum(not trace.hard_filter_eligible for trace in traces),
        eligible_match_count=sum(trace.eligible for trace in traces),
        semantic_available_count=sum(
            trace.semantic_status == "available" for trace in traces
        ),
        semantic_degraded_count=sum(
            trace.semantic_status == "degraded" for trace in traces
        ),
        user_specific_llm_calls=0,
        opportunity_analyzer_calls=0,
        elapsed_seconds=elapsed_seconds,
    )
