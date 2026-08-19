from __future__ import annotations

import asyncio
from datetime import datetime, timezone
import io
import json
from decimal import Decimal
from time import monotonic
import unittest
from uuid import UUID

import sqlalchemy as sa

from freelancer_bot.config import RuntimeConfig
from freelancer_bot.delivery import TelegramSendReceipt
from freelancer_bot.ingestion_runtime import TelegramIngestionRuntime
from freelancer_bot.matching_delivery import MATCHING_DELIVERY_JOB_TYPE
from freelancer_bot.message_prefilter import OPPORTUNITY_ANALYSIS_JOB_TYPE
from freelancer_bot.observability import Redactor, configure_structured_logger
from freelancer_bot.opportunity_analysis import (
    OPPORTUNITY_ANALYSIS_PROMPT_VERSION,
    OPPORTUNITY_ANALYSIS_SCHEMA_VERSION,
    OPPORTUNITY_ANALYZER_VERSION,
    IntentStage,
    MarketDirection,
    OpportunityAnalysis,
    OpportunityAnalysisCacheEnvelope,
    OpportunityAnalysisCall,
    OpportunityAnalysisError,
    OpportunityAnalysisUsage,
    OpportunityType,
    RoutedOpportunityAnalyzer,
)
from freelancer_bot.opportunity_classifier import OpportunityAnalysisJobProcessor
from freelancer_bot.persistence.collector_accounts import CollectorAccountRepository
from freelancer_bot.persistence.database import Database
from freelancer_bot.persistence.jobs import JobClaim
from freelancer_bot.persistence.message_prefilter import (
    MessagePrefilterRepository,
    OpportunityAnalysisCacheRepository,
)
from freelancer_bot.persistence.raw_messages import (
    RawMessageIngestor,
    RawMessageInput,
    RawMessageOrigin,
)
from freelancer_bot.persistence.schema import (
    durable_jobs,
    match_evaluation_runs,
    match_traces,
    opportunities,
    opportunity_analysis_cache,
    opportunity_source_messages,
    personalized_deliveries,
)
from freelancer_bot.persistence.search_profiles import (
    SearchProfileRepository,
    UserRepository,
)
from freelancer_bot.search_profiles import (
    parse_search_profile,
    parse_search_profile_preferences,
)
from freelancer_bot.persistence.source_repository import SourceRepository, SourceStatus
from postgres_support import TEST_DATABASE_URL, migrate_to_head, temporary_database


NOW = datetime.now(timezone.utc)
TRACE_ID = UUID("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")
RAW_CANARY = "G4_CLASSIFIER_RAW_CONTENT_481209"


class RecordingDeliverySender:
    def __init__(self) -> None:
        self.calls: list[dict[str, object]] = []

    async def send(self, **kwargs):
        self.calls.append(kwargs)
        return TelegramSendReceipt(message_id=91001 + len(self.calls))


class FixtureAnalyzer:
    provider = "fixture_ai"
    model = "fixture-low-cost-model"
    analyzer_version = OPPORTUNITY_ANALYZER_VERSION
    prompt_version = OPPORTUNITY_ANALYSIS_PROMPT_VERSION
    schema_version = OPPORTUNITY_ANALYSIS_SCHEMA_VERSION

    def __init__(
        self,
        analysis: OpportunityAnalysis,
        *,
        failures: int = 0,
    ) -> None:
        self.analysis = analysis
        self.failures = failures
        self.calls = []

    async def analyze(self, candidate):
        self.calls.append(candidate)
        if len(self.calls) <= self.failures:
            raise OpportunityAnalysisError("fixture provider request failed")
        return OpportunityAnalysisCall(
            analysis=self.analysis,
            provider=self.provider,
            requested_model=self.model,
            response_model="fixture-low-cost-model-2026-08-09",
            analyzer_version=self.analyzer_version,
            prompt_version=self.prompt_version,
            schema_version=self.schema_version,
            attempt_count=1,
            usage=OpportunityAnalysisUsage(
                input_tokens=41,
                output_tokens=23,
                total_tokens=64,
            ),
        )


@unittest.skipUnless(TEST_DATABASE_URL, "TEST_DATABASE_URL is not configured")
class OpportunityClassifierPipelineTest(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.database_context = temporary_database()
        self.database_url = self.database_context.__enter__()
        migrate_to_head(self.database_url)
        self.database = Database(self.database_url, pool_size=8, max_overflow=16)
        self.sources = SourceRepository()
        self.accounts = CollectorAccountRepository()
        self.prefilters = MessagePrefilterRepository()
        self.cache = OpportunityAnalysisCacheRepository()
        self.log_output = io.StringIO()
        self.logger = configure_structured_logger(
            "freelancer_bot.g4_classifier_test",
            redactor=Redactor(),
            stream=self.log_output,
        )
        self.config = RuntimeConfig(
            app_environment="test",
            database_url=self.database_url,
            worker_poll_interval_seconds=0.01,
            worker_lease_seconds=0.5,
            worker_heartbeat_seconds=0.1,
            worker_retry_delay_seconds=0,
            worker_shutdown_timeout_seconds=0.1,
            _env_file=None,
        )

    async def asyncSetUp(self):
        async with self.database.transaction() as connection:
            self.account = await self.accounts.ensure(
                connection,
                platform="telegram",
                external_account_id="91001",
                display_name="G4 classifier collector",
            )
            candidate = await self.sources.create_candidate(
                connection,
                platform="telegram",
                external_id="username:g4_classifier",
                access_type="public",
                display_name="G4 classifier source",
                handle="@g4_classifier",
                canonical_url="https://t.me/g4_classifier",
                provider="g4_classifier_fixture",
                lineage_key="g4-classifier:source",
            )
            self.source = await self.sources.transition(
                connection,
                candidate.id,
                SourceStatus.APPROVED,
                reason="G4 classifier fixture approved",
            )

    async def asyncTearDown(self):
        await self.database.close()
        self.database_context.__exit__(None, None, None)

    async def test_worker_classifies_persisted_context_and_reuses_auditable_cache(self):
        analyzer = FixtureAnalyzer(_rich_analysis())
        parent = await self._ingest(
            100,
            "Ищем архитектора автоматизаций для n8n и Python.",
            metadata={"service_action_type": "fixture_parent_context"},
        )
        current = await self._ingest(
            101,
            "Бюджет 80 000–120 000 ₽ за проект. Пишите @client_ru, "
            f"ТЗ https://example.test/brief {RAW_CANARY}",
            metadata={"reply_to_msg_id": 100},
        )
        runtime = TelegramIngestionRuntime(
            self.database,
            self.config,
            logger=self.logger,
            analyzer=analyzer,
        )

        await runtime.start()
        try:
            await self._wait_for_analysis_state("completed")
        finally:
            await runtime.stop()

        self.assertEqual(len(analyzer.calls), 1)
        model_input = analyzer.calls[0]
        self.assertEqual(model_input.current.raw_message_id, current.message.id)
        self.assertEqual(model_input.parent.raw_message_id, parent.message.id)
        self.assertEqual(
            model_input.parent.content,
            "Ищем архитектора автоматизаций для n8n и Python.",
        )
        self.assertFalse(hasattr(model_input, "history"))

        async with self.database.connect() as connection:
            prefilter = await self.prefilters.get_canonical_for_analysis_job(
                connection,
                await self._analysis_job_id(connection),
            )
            cached = await self.cache.get_for_prefilter_result(connection, prefilter)
            analysis_job = await connection.execute(
                sa.select(durable_jobs).where(
                    durable_jobs.c.id == prefilter.analysis_job_id
                )
            )
            analysis_job = analysis_job.mappings().one()
            opportunity_row = (
                (await connection.execute(sa.select(opportunities))).mappings().one()
            )
            opportunity_raw_ids = (
                (
                    await connection.execute(
                        sa.select(opportunity_source_messages.c.raw_message_id)
                    )
                )
                .scalars()
                .all()
            )

        self.assertEqual(analysis_job["state"], "completed")
        self.assertIsNotNone(cached)
        envelope = OpportunityAnalysisCacheEnvelope.model_validate_json(
            json.dumps(cached.result),
            strict=True,
        )
        self.assertTrue(envelope.analysis.is_opportunity)
        self.assertEqual(
            envelope.analysis.market_direction,
            MarketDirection.BUYER_TO_SPECIALIST,
        )
        self.assertEqual(envelope.analysis.intent_stage, IntentStage.ACTIVE)
        self.assertEqual(envelope.analysis.opportunity_type, OpportunityType.PROJECT)
        self.assertEqual(
            envelope.analysis.category,
            "business_process_automation",
        )
        self.assertEqual(
            envelope.analysis.role_title,
            "n8n automation architect",
        )
        self.assertEqual(envelope.analysis.skills, ("n8n", "Python"))
        self.assertEqual(
            (
                envelope.analysis.budget.min,
                envelope.analysis.budget.max,
                envelope.analysis.budget.currency,
                envelope.analysis.budget.period,
            ),
            (80_000, 120_000, "RUB", "project"),
        )
        self.assertEqual(envelope.analysis.contact.telegram, "@client_ru")
        self.assertIsNone(envelope.analysis.contact.email)
        self.assertEqual(
            envelope.analysis.contact.url,
            "https://example.test/brief",
        )
        self.assertEqual(envelope.analysis.quality.credibility, 0.55)
        self.assertEqual(envelope.analysis.red_flags, ("external_brief_link",))
        self.assertEqual(envelope.invocation.provider, analyzer.provider)
        self.assertEqual(envelope.invocation.requested_model, analyzer.model)
        self.assertEqual(
            envelope.invocation.response_model,
            "fixture-low-cost-model-2026-08-09",
        )
        self.assertEqual(envelope.invocation.usage.total_tokens, 64)
        self.assertEqual(
            envelope.invocation.cache_analyzer_version,
            prefilter.analyzer_version,
        )
        self.assertEqual(opportunity_row["canonical_title"], "n8n automation architect")
        self.assertEqual(
            opportunity_row["task_summary"],
            "Build an n8n and Python automation project",
        )
        self.assertEqual(opportunity_row["budget_min"], 80_000)
        self.assertEqual(opportunity_row["quality_credibility"], Decimal("0.5500"))
        self.assertEqual(opportunity_raw_ids, [current.message.id])

        cache_only_analyzer = FixtureAnalyzer(_rich_analysis())
        cache_result = await OpportunityAnalysisJobProcessor(
            self.database,
            cache_only_analyzer,
            logger=self.logger,
        ).process(_claim(prefilter.analysis_job_id, attempt=2))
        self.assertFalse(cache_result.analyzed)
        self.assertEqual(cache_result.opportunity.id, opportunity_row["id"])
        self.assertEqual(cache_only_analyzer.calls, [])
        async with self.database.connect() as connection:
            opportunity_count = await connection.scalar(
                sa.select(sa.func.count()).select_from(opportunities)
            )
        self.assertEqual(opportunity_count, 1)
        self.assertNotIn(RAW_CANARY, self.log_output.getvalue())

    async def test_transient_provider_failure_uses_durable_retry_and_persists_once(self):
        analyzer = FixtureAnalyzer(_analysis(), failures=1)
        await self._ingest(200, f"Нужен backend-исполнитель {RAW_CANARY}")
        runtime = TelegramIngestionRuntime(
            self.database,
            self.config,
            logger=self.logger,
            analyzer=analyzer,
        )

        await runtime.start()
        try:
            await self._wait_for_analysis_state("completed")
        finally:
            await runtime.stop()

        async with self.database.connect() as connection:
            job_id = await self._analysis_job_id(connection)
            job = (
                await connection.execute(
                    sa.select(durable_jobs).where(durable_jobs.c.id == job_id)
                )
            ).mappings().one()
            cache_count = await connection.scalar(
                sa.select(sa.func.count()).select_from(opportunity_analysis_cache)
            )
            opportunity_count = await connection.scalar(
                sa.select(sa.func.count()).select_from(opportunities)
            )

        self.assertEqual(len(analyzer.calls), 2)
        self.assertEqual(job["attempt_count"], 2)
        self.assertEqual(job["state"], "completed")
        self.assertEqual(cache_count, 1)
        self.assertEqual(opportunity_count, 1)
        self.assertNotIn(RAW_CANARY, self.log_output.getvalue())

    async def test_live_opportunity_orchestrates_matching_and_delivery_once(self):
        async with self.database.transaction() as connection:
            user = await UserRepository().ensure(
                connection,
                platform="telegram",
                external_user_id="7004001",
            )
            parsed = parse_search_profile(
                roles=("n8n automation architect",),
                skills=("n8n", "Python"),
                categories=(),
                semantic_text="n8n Python automation projects",
            )
            profile = await SearchProfileRepository().create(
                connection,
                user_id=user.user.id,
                parsed_profile=parsed,
            )
            profile_record = await SearchProfileRepository().update_preferences(
                connection,
                profile_id=profile.profile.id,
                user_id=user.user.id,
                preferences=parse_search_profile_preferences(
                    work_types=("project",),
                    languages=("ru",),
                ),
                expected_revision=profile.profile.revision,
            )
            profile_record = await SearchProfileRepository().confirm(
                connection,
                profile_id=profile_record.id,
                user_id=user.user.id,
                expected_revision=profile_record.revision,
            )
            activation = await SearchProfileRepository().activate_primary(
                connection,
                profile_id=profile_record.id,
                user_id=user.user.id,
                expected_revision=profile_record.revision,
            )
        self.assertTrue(activation.trial_started)

        sender = RecordingDeliverySender()
        analyzer = FixtureAnalyzer(_rich_analysis())
        await self._ingest(
            450,
            "Бюджет 80 000–120 000 ₽ за проект. Пишите @client_ru, "
            "ТЗ https://example.test/brief",
        )
        runtime = TelegramIngestionRuntime(
            self.database,
            self.config,
            logger=self.logger,
            analyzer=analyzer,
            delivery_sender=sender,
        )

        await runtime.start()
        try:
            await self._wait_for_analysis_state("completed")
            await self._wait_for_live_fanout(sender)
        finally:
            await runtime.stop()

        async with self.database.connect() as connection:
            counts = (
                await connection.scalar(
                    sa.select(sa.func.count()).select_from(match_evaluation_runs)
                ),
                await connection.scalar(
                    sa.select(sa.func.count()).select_from(match_traces)
                ),
                await connection.scalar(
                    sa.select(sa.func.count()).select_from(personalized_deliveries)
                ),
                await connection.scalar(
                    sa.select(sa.func.count())
                    .select_from(durable_jobs)
                    .where(durable_jobs.c.job_type == MATCHING_DELIVERY_JOB_TYPE)
                ),
            )
            matching_job = (
                await connection.execute(
                    sa.select(durable_jobs).where(
                        durable_jobs.c.job_type == MATCHING_DELIVERY_JOB_TYPE
                    )
                )
            ).mappings().one()

        self.assertEqual(counts, (1, 1, 1, 1))
        self.assertEqual(matching_job["state"], "completed")
        self.assertEqual(matching_job["attempt_count"], 1)
        self.assertEqual(len(sender.calls), 1)

        # Replaying the same canonical analysis job reuses the same matching
        # job and the same delivery; it cannot create a second logical fan-out.
        async with self.database.connect() as connection:
            analysis_job_id = await self._analysis_job_id(connection)
        await OpportunityAnalysisJobProcessor(
            self.database,
            FixtureAnalyzer(_rich_analysis()),
            logger=self.logger,
        ).process(_claim(analysis_job_id, attempt=2))

        async with self.database.connect() as connection:
            replay_counts = (
                await connection.scalar(
                    sa.select(sa.func.count()).select_from(match_evaluation_runs)
                ),
                await connection.scalar(
                    sa.select(sa.func.count()).select_from(personalized_deliveries)
                ),
            )
        self.assertEqual(replay_counts, (1, 1))

    async def _wait_for_live_fanout(self, sender: RecordingDeliverySender) -> None:
        deadline = monotonic() + 3
        while monotonic() < deadline:
            async with self.database.connect() as connection:
                matching_completed = await connection.scalar(
                    sa.select(sa.func.count())
                    .select_from(durable_jobs)
                    .where(
                        durable_jobs.c.job_type == MATCHING_DELIVERY_JOB_TYPE,
                        durable_jobs.c.state == "completed",
                    )
                )
                delivery_count = await connection.scalar(
                    sa.select(sa.func.count()).select_from(personalized_deliveries)
                )
                delivery_status = await connection.scalar(
                    sa.select(personalized_deliveries.c.status).limit(1)
                )
            if (
                matching_completed == 1
                and delivery_count == 1
                and delivery_status == "sent"
                and len(sender.calls) == 1
            ):
                return
            await asyncio.sleep(0.01)
        self.fail("Timed out waiting for matching and delivery fan-out")

    async def test_routed_cache_hit_skips_both_primary_and_fallback_models(self):
        primary = FixtureAnalyzer(_analysis_with_confidence(0.40))
        fallback = FixtureAnalyzer(_analysis_with_confidence(0.93))
        fallback.model = "fixture-stronger-model"
        analyzer = RoutedOpportunityAnalyzer(
            primary,
            fallback,
            confidence_threshold=0.65,
        )
        await self._ingest(250, "Нужен разработчик бота")
        runtime = TelegramIngestionRuntime(
            self.database,
            self.config,
            logger=self.logger,
            analyzer=analyzer,
        )

        await runtime.start()
        try:
            await self._wait_for_analysis_state("completed")
        finally:
            await runtime.stop()

        self.assertEqual(len(primary.calls), 1)
        self.assertEqual(len(fallback.calls), 1)
        cache_primary = FixtureAnalyzer(_analysis_with_confidence(0.40))
        cache_fallback = FixtureAnalyzer(_analysis_with_confidence(0.93))
        cache_fallback.model = "fixture-stronger-model"
        cached_router = RoutedOpportunityAnalyzer(
            cache_primary,
            cache_fallback,
            confidence_threshold=0.65,
        )
        async with self.database.connect() as connection:
            job_id = await self._analysis_job_id(connection)

        result = await OpportunityAnalysisJobProcessor(
            self.database,
            cached_router,
            logger=self.logger,
        ).process(_claim(job_id, attempt=2))

        self.assertFalse(result.analyzed)
        self.assertEqual(cache_primary.calls, [])
        self.assertEqual(cache_fallback.calls, [])

    async def test_non_opportunity_is_cached_without_canonical_record(self):
        payload = json.loads(_analysis().model_dump_json())
        payload.update(
            is_opportunity=False,
            market_direction="specialist_to_buyer",
            intent_stage="none",
            opportunity_type="unknown",
        )
        analysis = OpportunityAnalysis.model_validate_json(
            json.dumps(payload),
            strict=True,
        )
        analyzer = FixtureAnalyzer(analysis)
        await self._ingest(
            275,
            "Я разработчик Telegram-ботов, ищу клиентов и предлагаю свои услуги.",
        )
        runtime = TelegramIngestionRuntime(
            self.database,
            self.config,
            logger=self.logger,
            analyzer=analyzer,
        )

        await runtime.start()
        try:
            await self._wait_for_analysis_state("completed")
        finally:
            await runtime.stop()

        async with self.database.connect() as connection:
            counts = (
                await connection.scalar(
                    sa.select(sa.func.count()).select_from(opportunity_analysis_cache)
                ),
                await connection.scalar(
                    sa.select(sa.func.count()).select_from(opportunities)
                ),
                await connection.scalar(
                    sa.select(sa.func.count())
                    .select_from(durable_jobs)
                    .where(durable_jobs.c.job_type == MATCHING_DELIVERY_JOB_TYPE)
                ),
                await connection.scalar(
                    sa.select(sa.func.count()).select_from(personalized_deliveries)
                ),
            )
        self.assertEqual(counts, (1, 0, 0, 0))

    async def test_persistent_provider_failure_is_terminal_without_cache(self):
        analyzer = FixtureAnalyzer(_analysis(), failures=10)
        await self._ingest(300, f"Нужен консультант {RAW_CANARY}")
        runtime = TelegramIngestionRuntime(
            self.database,
            self.config,
            logger=self.logger,
            analyzer=analyzer,
        )

        await runtime.start()
        try:
            await self._wait_for_analysis_state("failed")
        finally:
            await runtime.stop()

        async with self.database.connect() as connection:
            job_id = await self._analysis_job_id(connection)
            job = (
                await connection.execute(
                    sa.select(durable_jobs).where(durable_jobs.c.id == job_id)
                )
            ).mappings().one()
            cache_count = await connection.scalar(
                sa.select(sa.func.count()).select_from(opportunity_analysis_cache)
            )
            opportunity_count = await connection.scalar(
                sa.select(sa.func.count()).select_from(opportunities)
            )

        self.assertEqual(len(analyzer.calls), 3)
        self.assertEqual(job["attempt_count"], 3)
        self.assertEqual(job["failure_code"], "OpportunityAnalysisError")
        self.assertEqual(cache_count, 0)
        self.assertEqual(opportunity_count, 0)
        self.assertNotIn(RAW_CANARY, self.log_output.getvalue())

    async def test_injected_provider_cannot_persist_ungrounded_contact(self):
        analyzer = FixtureAnalyzer(_analysis_with_contact("@invented_contact"))
        await self._ingest(400, f"Нужен Python-разработчик {RAW_CANARY}")
        runtime = TelegramIngestionRuntime(
            self.database,
            self.config,
            logger=self.logger,
            analyzer=analyzer,
        )

        await runtime.start()
        try:
            await self._wait_for_analysis_state("failed")
        finally:
            await runtime.stop()

        async with self.database.connect() as connection:
            job_id = await self._analysis_job_id(connection)
            job = (
                await connection.execute(
                    sa.select(durable_jobs).where(durable_jobs.c.id == job_id)
                )
            ).mappings().one()
            cache_count = await connection.scalar(
                sa.select(sa.func.count()).select_from(opportunity_analysis_cache)
            )
            opportunity_count = await connection.scalar(
                sa.select(sa.func.count()).select_from(opportunities)
            )

        self.assertEqual(len(analyzer.calls), 3)
        self.assertEqual(job["attempt_count"], 3)
        self.assertEqual(job["failure_code"], "OpportunityAnalysisOutputError")
        self.assertEqual(cache_count, 0)
        self.assertEqual(opportunity_count, 0)
        logs = self.log_output.getvalue()
        self.assertNotIn("@invented_contact", logs)
        self.assertNotIn(RAW_CANARY, logs)

    async def _ingest(self, message_id, content, *, metadata=None):
        return await RawMessageIngestor(self.database).ingest(
            RawMessageInput(
                source_id=self.source.id,
                collector_account_id=self.account.id,
                external_message_id=message_id,
                message_date=NOW,
                observed_at=NOW,
                message_url=f"https://t.me/g4_classifier/{message_id}",
                content=content,
                transport_metadata={} if metadata is None else metadata,
                ingestion_origin=RawMessageOrigin.LIVE,
                correlation_id=TRACE_ID,
            )
        )

    async def _wait_for_analysis_state(self, state: str) -> None:
        deadline = monotonic() + 3
        while monotonic() < deadline:
            async with self.database.connect() as connection:
                count = await connection.scalar(
                    sa.select(sa.func.count())
                    .select_from(durable_jobs)
                    .where(
                        durable_jobs.c.job_type == OPPORTUNITY_ANALYSIS_JOB_TYPE,
                        durable_jobs.c.state == state,
                    )
                )
                if count == 1:
                    return
            await asyncio.sleep(0.01)
        self.fail(f"Timed out waiting for analysis job state={state}")

    @staticmethod
    async def _analysis_job_id(connection):
        return await connection.scalar(
            sa.select(durable_jobs.c.id).where(
                durable_jobs.c.job_type == OPPORTUNITY_ANALYSIS_JOB_TYPE
            )
        )


def _claim(job_id, *, attempt):
    return JobClaim(
        id=job_id,
        job_type=OPPORTUNITY_ANALYSIS_JOB_TYPE,
        idempotency_key="g4-classifier-fixture",
        correlation_id=TRACE_ID,
        attempt_count=attempt,
        max_attempts=3,
        worker_id="g4-cache-reuse",
        reclaimed=False,
    )


def _analysis() -> OpportunityAnalysis:
    return OpportunityAnalysis.model_validate_json(
        json.dumps(
            {
                "schema_version": OPPORTUNITY_ANALYSIS_SCHEMA_VERSION,
                "is_opportunity": True,
                "confidence": 0.91,
                "market_direction": "buyer_to_specialist",
                "intent_stage": "active",
                "opportunity_type": "project",
                "category": None,
                "role_title": None,
                "skills": [],
                "task_summary": None,
                "budget": {
                    "known": False,
                    "min": None,
                    "max": None,
                    "currency": None,
                    "period": None,
                    "explicit": False,
                },
                "work": {
                    "remote": None,
                    "location": None,
                    "full_time": None,
                    "part_time": None,
                },
                "language": None,
                "contact": {
                    "telegram": None,
                    "email": None,
                    "url": None,
                },
                "quality": {
                    "actionability": 0.8,
                    "commercial_plausibility": 0.8,
                    "specificity": 0.7,
                    "credibility": 0.7,
                },
                "red_flags": [],
            }
        ),
        strict=True,
    )


def _rich_analysis() -> OpportunityAnalysis:
    payload = json.loads(_analysis().model_dump_json())
    payload.update(
        category="business_process_automation",
        role_title="n8n automation architect",
        skills=["n8n", "Python"],
        task_summary="Build an n8n and Python automation project",
        budget={
            "known": True,
            "min": 80_000,
            "max": 120_000,
            "currency": "RUB",
            "period": "project",
            "explicit": True,
        },
        work={
            "remote": None,
            "location": None,
            "full_time": None,
            "part_time": None,
        },
        language="ru",
        contact={
            "telegram": "@client_ru",
            "email": None,
            "url": "https://example.test/brief",
        },
        quality={
            "actionability": 0.9,
            "commercial_plausibility": 0.85,
            "specificity": 0.9,
            "credibility": 0.55,
        },
        red_flags=["external_brief_link"],
    )
    return OpportunityAnalysis.model_validate_json(json.dumps(payload), strict=True)


def _analysis_with_confidence(confidence: float) -> OpportunityAnalysis:
    payload = json.loads(_analysis().model_dump_json())
    payload["confidence"] = confidence
    return OpportunityAnalysis.model_validate_json(json.dumps(payload), strict=True)


def _analysis_with_contact(telegram: str) -> OpportunityAnalysis:
    payload = json.loads(_analysis().model_dump_json())
    payload["contact"] = {"telegram": telegram, "email": None, "url": None}
    return OpportunityAnalysis.model_validate_json(json.dumps(payload), strict=True)


if __name__ == "__main__":
    unittest.main()
