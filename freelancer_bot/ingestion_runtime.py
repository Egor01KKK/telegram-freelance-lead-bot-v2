from __future__ import annotations

import asyncio
import logging
import os
import socket
from collections.abc import Awaitable
from uuid import uuid4

from .ai_telemetry import AIModelPrice, AISpendGuardPolicy
from .config import RuntimeConfig
from .delivery import PersonalizedDeliveryJobProcessor, TelegramDeliverySender
from .message_prefilter import (
    OPPORTUNITY_ANALYSIS_JOB_TYPE,
    RawMessagePrefilterProcessor,
)
from .matching_delivery import MATCHING_DELIVERY_JOB_TYPE, MatchingDeliveryJobProcessor
from .opportunity_analysis import (
    OPPORTUNITY_ANALYSIS_SCHEMA_VERSION,
    OPPORTUNITY_ANALYZER_VERSION,
    OpenAIOpportunityAnalyzer,
    OpportunityAnalysisError,
    OpportunityAnalyzer,
    RoutedOpportunityAnalyzer,
    opportunity_analysis_cache_version,
)
from .opportunity_classifier import OpportunityAnalysisJobProcessor
from .persistence.database import Database
from .persistence.jobs import DurableJobRepository
from .persistence.ai_telemetry import PostgreSQLAICallRecorder
from .persistence.raw_messages import RAW_MESSAGE_JOB_TYPE
from .profile_rematch import PROFILE_REMATCH_JOB_TYPE, ProfileRematchJobProcessor
from .worker import DurableWorker, WorkerOptions
from .global_source_library_runtime import (
    DiscoveryCampaignPlanProcessor,
    ProfileCoverageRecheckProcessor,
)


class TelegramIngestionRuntime:
    """Own the one durable Telegram-to-analysis worker used by the collector."""

    def __init__(
        self,
        database: Database,
        config: RuntimeConfig,
        *,
        logger: logging.Logger,
        worker: DurableWorker | None = None,
        worker_id: str | None = None,
        analyzer: OpportunityAnalyzer | None = None,
        delivery_sender: TelegramDeliverySender | None = None,
    ) -> None:
        self._worker = worker or _build_worker(
            database,
            config,
            logger=logger,
            worker_id=worker_id or _worker_id(),
            analyzer=(
                analyzer
                if analyzer is not None
                else _configured_analyzer(database, config)
            ),
            delivery_sender=delivery_sender,
        )
        self._task: asyncio.Task[None] | None = None
        self._started = False

    @property
    def running(self) -> bool:
        return self._task is not None and not self._task.done()

    async def start(self) -> None:
        if self._started:
            raise RuntimeError("Telegram ingestion runtime cannot be started twice")
        self._started = True
        self._task = asyncio.create_task(
            self._worker.run(install_signal_handlers=False),
            name="telegram-pipeline-worker",
        )
        await asyncio.sleep(0)
        if self._task.done():
            await self._task

    async def stop(self) -> None:
        task = self._task
        if task is None:
            return
        self._worker.request_stop()
        try:
            await task
        finally:
            self._task = None

    async def wait_until_collector_stops(
        self,
        collector_stop: Awaitable[None],
    ) -> None:
        worker_task = self._task
        if worker_task is None:
            raise RuntimeError("Telegram ingestion runtime is not started")
        collector_task = asyncio.create_task(collector_stop)
        try:
            done, _ = await asyncio.wait(
                {worker_task, collector_task},
                return_when=asyncio.FIRST_COMPLETED,
            )
        except BaseException:
            collector_task.cancel()
            await _consume_cancelled(collector_task)
            raise
        if worker_task in done:
            collector_task.cancel()
            await _consume_cancelled(collector_task)
            await worker_task
            raise RuntimeError("Telegram ingestion worker stopped unexpectedly")
        await collector_task


def _build_worker(
    database: Database,
    config: RuntimeConfig,
    *,
    logger: logging.Logger,
    worker_id: str,
    analyzer: OpportunityAnalyzer | None,
    delivery_sender: TelegramDeliverySender | None,
) -> DurableWorker:
    jobs = DurableJobRepository()
    analyzer_version = (
        OPPORTUNITY_ANALYZER_VERSION
        if analyzer is None
        else opportunity_analysis_cache_version(analyzer)
    )
    processor = RawMessagePrefilterProcessor(
        database,
        jobs=jobs,
        analyzer_version=analyzer_version,
        analysis_schema_version=(
            OPPORTUNITY_ANALYSIS_SCHEMA_VERSION
            if analyzer is None
            else analyzer.schema_version
        ),
        logger=logger,
    )
    handlers = {RAW_MESSAGE_JOB_TYPE: processor}
    handlers["profile.coverage.recheck"] = ProfileCoverageRecheckProcessor(database)
    handlers["discovery.campaign.plan"] = DiscoveryCampaignPlanProcessor(database, config)
    if analyzer is not None:
        handlers[OPPORTUNITY_ANALYSIS_JOB_TYPE] = OpportunityAnalysisJobProcessor(
            database,
            analyzer,
            logger=logger,
        )
    if delivery_sender is not None:
        from .persistence.deliveries import PERSONALIZED_DELIVERY_JOB_TYPE

        handlers[PERSONALIZED_DELIVERY_JOB_TYPE] = PersonalizedDeliveryJobProcessor(
            database,
            delivery_sender,
            logger=logger,
        )
        handlers[MATCHING_DELIVERY_JOB_TYPE] = MatchingDeliveryJobProcessor(
            database,
            config,
            logger=logger,
        )
        handlers[PROFILE_REMATCH_JOB_TYPE] = ProfileRematchJobProcessor(
            database,
            config,
            logger=logger,
        )
    return DurableWorker(
        database,
        repository=jobs,
        worker_id=worker_id,
        handlers=handlers,
        logger=logger,
        options=WorkerOptions.from_config(config),
        close_database_on_exit=False,
    )


def _worker_id() -> str:
    return (
        f"telegram-pipeline-{socket.gethostname()}-{os.getpid()}-"
        f"{uuid4().hex[:8]}"
    )


def _configured_analyzer(
    database: Database,
    config: RuntimeConfig,
) -> OpportunityAnalyzer | None:
    if (
        config.openai_api_key is None
        or not config.openai_api_key.get_secret_value().strip()
    ):
        return None
    if config.opportunity_analysis_provider != "openai" or (
        config.opportunity_analysis_fallback_enabled
        and config.opportunity_analysis_fallback_provider != "openai"
    ):
        raise OpportunityAnalysisError(
            "A non-OpenAI opportunity provider must be injected into the runtime"
        )
    key = config.openai_api_key.get_secret_value()
    spend_guard = None
    if (
        config.opportunity_analysis_daily_spend_limit_usd is not None
        or config.opportunity_analysis_monthly_spend_limit_usd is not None
    ):
        spend_guard = AISpendGuardPolicy(
            daily_limit_usd=config.opportunity_analysis_daily_spend_limit_usd,
            monthly_limit_usd=config.opportunity_analysis_monthly_spend_limit_usd,
            reserve_input_tokens=config.opportunity_analysis_budget_reserve_input_tokens,
            reserve_output_tokens=config.opportunity_analysis_budget_reserve_output_tokens,
        )
    recorder = PostgreSQLAICallRecorder(database, spend_guard=spend_guard)
    primary = OpenAIOpportunityAnalyzer(
        api_key=key,
        model=config.opportunity_analysis_model,
        temperature=config.opportunity_analysis_temperature,
        timeout_seconds=config.opportunity_analysis_timeout_seconds,
        max_output_attempts=config.opportunity_analysis_max_output_attempts,
        recorder=recorder,
        stage="opportunity_analysis.primary",
        routing_version=config.opportunity_analysis_routing_version,
        route_reason="primary",
        price=AIModelPrice(
            pricing_version=config.opportunity_analysis_pricing_version,
            input_usd_per_million=config.opportunity_analysis_input_usd_per_million,
            output_usd_per_million=config.opportunity_analysis_output_usd_per_million,
        ),
    )
    fallback = None
    if config.opportunity_analysis_fallback_enabled:
        fallback = OpenAIOpportunityAnalyzer(
            api_key=key,
            model=config.opportunity_analysis_fallback_model,
            temperature=config.opportunity_analysis_temperature,
            timeout_seconds=config.opportunity_analysis_timeout_seconds,
            max_output_attempts=config.opportunity_analysis_max_output_attempts,
            recorder=recorder,
            stage="opportunity_analysis.fallback",
            routing_version=config.opportunity_analysis_routing_version,
            route_reason="low_confidence",
            price=AIModelPrice(
                pricing_version=config.opportunity_analysis_pricing_version,
                input_usd_per_million=(
                    config.opportunity_analysis_fallback_input_usd_per_million
                ),
                output_usd_per_million=(
                    config.opportunity_analysis_fallback_output_usd_per_million
                ),
            ),
        )
    return RoutedOpportunityAnalyzer(
        primary,
        fallback,
        confidence_threshold=config.opportunity_analysis_confidence_threshold,
        routing_version=config.opportunity_analysis_routing_version,
    )


async def _consume_cancelled(task: asyncio.Task[None]) -> None:
    try:
        await task
    except asyncio.CancelledError:
        pass
