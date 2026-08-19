from __future__ import annotations

from dataclasses import dataclass
import json
import logging
from uuid import UUID

from sqlalchemy.ext.asyncio import AsyncConnection

from .message_prefilter import OPPORTUNITY_ANALYSIS_JOB_TYPE, AnalyzerInputLoader
from .matching_delivery import enqueue_matching_delivery
from .observability import log_event
from .opportunity_analysis import (
    DEFAULT_OPPORTUNITY_ANALYSIS_MODEL,
    OPPORTUNITY_ANALYSIS_PROMPT_VERSION,
    OPPORTUNITY_ANALYSIS_SCHEMA_VERSION,
    OPPORTUNITY_ANALYZER_VERSION,
    OpportunityAnalysis,
    OpportunityAnalysisCacheEnvelope,
    OpportunityAnalysisCall,
    OpportunityAnalyzer,
    opportunity_analysis_cache_envelope,
    opportunity_analysis_cache_version,
    opportunity_analysis_call_is_compatible,
    validate_opportunity_analysis_grounding,
)
from .persistence.database import Database
from .persistence.jobs import JobClaim
from .persistence.jobs import DurableJobRepository
from .persistence.message_prefilter import (
    AnalysisCacheRecord,
    MessagePrefilterRepository,
    OpportunityAnalysisCacheRepository,
    PrefilterResultRecord,
)
from .persistence.opportunities import (
    CanonicalOpportunityRecord,
    CanonicalOpportunityRepository,
)


LOGGER = logging.getLogger("freelancer_bot")
LEGACY_ANALYSIS_SCHEMA_VERSION = "opportunity-analysis.v1"


class IncompatibleOpportunityAnalysisJob(RuntimeError):
    pass


@dataclass(frozen=True)
class OpportunityClassificationResult:
    analysis: OpportunityAnalysis
    cache: AnalysisCacheRecord
    opportunity: CanonicalOpportunityRecord | None
    analyzed: bool


class OpportunityAnalysisJobProcessor:
    def __init__(
        self,
        database: Database,
        analyzer: OpportunityAnalyzer,
        *,
        inputs: AnalyzerInputLoader | None = None,
        prefilters: MessagePrefilterRepository | None = None,
        cache: OpportunityAnalysisCacheRepository | None = None,
        opportunities: CanonicalOpportunityRepository | None = None,
        jobs: DurableJobRepository | None = None,
        logger: logging.Logger | None = None,
    ) -> None:
        if not isinstance(analyzer, OpportunityAnalyzer):
            raise TypeError("analyzer must implement OpportunityAnalyzer")
        self._database = database
        self._analyzer = analyzer
        self._inputs = inputs or AnalyzerInputLoader(database)
        self._prefilters = prefilters or MessagePrefilterRepository()
        self._cache = cache or OpportunityAnalysisCacheRepository()
        self._opportunities = opportunities or CanonicalOpportunityRepository()
        self._jobs = jobs or DurableJobRepository()
        self._logger = logger or LOGGER
        self._cache_analyzer_version = opportunity_analysis_cache_version(analyzer)

    async def __call__(self, claim: JobClaim) -> None:
        await self.process(claim)

    async def process(self, claim: JobClaim) -> OpportunityClassificationResult:
        if claim.job_type != OPPORTUNITY_ANALYSIS_JOB_TYPE:
            raise ValueError("Analyzer processor received an unsupported job type")

        prefilter, cached = await self._load_state(claim)
        if cached is not None:
            return await self._result_from_cache(claim, cached)
        self._require_compatible_job(prefilter)

        candidate = await self._inputs.load(claim.id)
        call = await self._analyzer.analyze(candidate)
        self._require_compatible_call(call)
        validate_opportunity_analysis_grounding(call.analysis, candidate)
        envelope = opportunity_analysis_cache_envelope(
            call,
            cache_analyzer_version=prefilter.analyzer_version,
        )

        async with self._database.transaction() as connection:
            current = await self._prefilters.get_canonical_for_analysis_job(
                connection,
                claim.id,
            )
            if current is None:
                raise LookupError("Analysis job has no canonical prefilter result")
            self._require_compatible_job(current)
            cached = await self._cache.get_for_prefilter_result(connection, current)
            if cached is None:
                outcome = await self._cache.store_for_prefilter_result(
                    connection,
                    prefilter_result=current,
                    result=envelope.model_dump(mode="json"),
                )
                cached = outcome.entry
            opportunity = await self._persist_opportunity(
                connection,
                analysis_job_id=claim.id,
                cache=cached,
                analysis=envelope.analysis,
            )

        result = OpportunityClassificationResult(
            analysis=envelope.analysis,
            cache=cached,
            opportunity=opportunity,
            analyzed=True,
        )
        self._log_result(claim, result)
        return result

    async def _load_state(
        self,
        claim: JobClaim,
    ) -> tuple[PrefilterResultRecord, AnalysisCacheRecord | None]:
        async with self._database.connect() as connection:
            prefilter = await self._prefilters.get_canonical_for_analysis_job(
                connection,
                claim.id,
            )
            if prefilter is None:
                raise LookupError("Analysis job has no canonical prefilter result")
            self._require_compatible_job(prefilter)
            cached = await self._cache.get_for_prefilter_result(
                connection,
                prefilter,
            )
        return prefilter, cached

    async def _result_from_cache(
        self,
        claim: JobClaim,
        cached: AnalysisCacheRecord,
    ) -> OpportunityClassificationResult:
        envelope = _parse_cache_envelope(cached)
        self._require_compatible_envelope(envelope)
        if envelope.invocation.cache_analyzer_version != cached.analyzer_version:
            raise IncompatibleOpportunityAnalysisJob(
                "Cached analyzer identity does not match its compatibility key"
            )
        async with self._database.transaction() as connection:
            opportunity = await self._persist_opportunity(
                connection,
                analysis_job_id=claim.id,
                cache=cached,
                analysis=envelope.analysis,
            )
        result = OpportunityClassificationResult(
            analysis=envelope.analysis,
            cache=cached,
            opportunity=opportunity,
            analyzed=False,
        )
        self._log_result(claim, result)
        return result

    async def _persist_opportunity(
        self,
        connection: AsyncConnection,
        *,
        analysis_job_id: UUID,
        cache: AnalysisCacheRecord,
        analysis: OpportunityAnalysis,
    ) -> CanonicalOpportunityRecord | None:
        if not analysis.is_opportunity:
            return None
        prefilters = await self._prefilters.list_for_analysis_job(
            connection,
            analysis_job_id,
        )
        outcome = await self._opportunities.ensure_from_analysis(
            connection,
            analysis_cache_id=cache.id,
            raw_message_ids=tuple(item.raw_message_id for item in prefilters),
            analysis=analysis,
        )
        await enqueue_matching_delivery(
            connection,
            jobs=self._jobs,
            opportunity_id=outcome.opportunity.id,
        )
        return outcome.opportunity

    def _require_compatible_job(self, prefilter: PrefilterResultRecord) -> None:
        supported_analyzers = {
            self._cache_analyzer_version,
        }
        supported_analyzers.update(
            getattr(self._analyzer, "compatible_cache_versions", ())
        )
        if _supports_legacy_default_route(self._analyzer):
            supported_analyzers.add(self._analyzer.analyzer_version)
        if prefilter.analyzer_version not in supported_analyzers:
            raise IncompatibleOpportunityAnalysisJob(
                "Analysis job targets a different analyzer compatibility version"
            )
        if prefilter.analysis_schema_version not in {
            self._analyzer.schema_version,
            LEGACY_ANALYSIS_SCHEMA_VERSION,
        }:
            raise IncompatibleOpportunityAnalysisJob(
                "Analysis job targets an unsupported output schema version"
            )

    def _require_compatible_call(self, call: OpportunityAnalysisCall) -> None:
        if (
            not opportunity_analysis_call_is_compatible(self._analyzer, call)
            or call.analysis.schema_version != OPPORTUNITY_ANALYSIS_SCHEMA_VERSION
        ):
            raise IncompatibleOpportunityAnalysisJob(
                "Analyzer response metadata does not match the configured route"
            )

    def _require_compatible_envelope(
        self,
        envelope: OpportunityAnalysisCacheEnvelope,
    ) -> None:
        invocation = envelope.invocation
        checker = getattr(self._analyzer, "accepts_metadata", None)
        if checker is not None:
            compatible = checker(
                provider=invocation.provider,
                model=invocation.requested_model,
                analyzer_version=invocation.analyzer_version,
                prompt_version=invocation.prompt_version,
                schema_version=invocation.schema_version,
                routing_version=invocation.routing_version,
            )
            if compatible:
                return
        expected = (
            self._analyzer.provider,
            self._analyzer.model,
            self._analyzer.analyzer_version,
            self._analyzer.prompt_version,
            self._analyzer.schema_version,
        )
        actual = (
            invocation.provider,
            invocation.requested_model,
            invocation.analyzer_version,
            invocation.prompt_version,
            invocation.schema_version,
        )
        if actual != expected:
            raise IncompatibleOpportunityAnalysisJob(
                "Cached invocation metadata does not match the configured route"
            )

    def _log_result(
        self,
        claim: JobClaim,
        result: OpportunityClassificationResult,
    ) -> None:
        analysis = result.analysis
        log_event(
            self._logger,
            logging.INFO,
            "opportunity.analysis.classified",
            analysis_job_id=claim.id,
            cache_id=result.cache.id,
            opportunity_id=(
                None if result.opportunity is None else result.opportunity.id
            ),
            cache_hit=not result.analyzed,
            is_opportunity=analysis.is_opportunity,
            market_direction=analysis.market_direction.value,
            intent_stage=analysis.intent_stage.value,
            opportunity_type=analysis.opportunity_type.value,
            analyzer_version=result.cache.analyzer_version,
            analysis_schema_version=result.cache.analysis_schema_version,
        )


def _parse_cache_envelope(
    cached: AnalysisCacheRecord,
) -> OpportunityAnalysisCacheEnvelope:
    try:
        return OpportunityAnalysisCacheEnvelope.model_validate_json(
            json.dumps(cached.result),
            strict=True,
        )
    except (TypeError, ValueError):
        raise IncompatibleOpportunityAnalysisJob(
            "Cached opportunity analysis does not match the strict envelope"
        ) from None


def _supports_legacy_default_route(analyzer: OpportunityAnalyzer) -> bool:
    return (
        analyzer.provider == "openai"
        and analyzer.model == DEFAULT_OPPORTUNITY_ANALYSIS_MODEL
        and analyzer.analyzer_version == OPPORTUNITY_ANALYZER_VERSION
        and analyzer.prompt_version == OPPORTUNITY_ANALYSIS_PROMPT_VERSION
        and analyzer.schema_version == OPPORTUNITY_ANALYSIS_SCHEMA_VERSION
    )
