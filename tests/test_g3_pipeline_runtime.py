from __future__ import annotations

import asyncio
import io
import json
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from time import monotonic
from unittest.mock import patch
from uuid import UUID

import sqlalchemy as sa
from postgres_support import TEST_DATABASE_URL, migrate_to_head, temporary_database
from pydantic import SecretStr

from freelancer_bot.app import LeadBot
from freelancer_bot.config import RuntimeConfig
from freelancer_bot.filters import FilterConfig
from freelancer_bot.ingestion_runtime import (
    TelegramIngestionRuntime,
    _configured_analyzer,
)
from freelancer_bot.legacy_pipeline import LegacyLeadProcessor
from freelancer_bot.message_prefilter import (
    OPPORTUNITY_ANALYSIS_JOB_TYPE,
    RawMessagePrefilterProcessor,
)
from freelancer_bot.observability import Redactor, configure_structured_logger
from freelancer_bot.opportunity_analysis import OpportunityAnalysisError
from freelancer_bot.persistence.collector_accounts import CollectorAccountRepository
from freelancer_bot.persistence.database import Database
from freelancer_bot.persistence.jobs import DurableJobRepository
from freelancer_bot.persistence.raw_messages import (
    RAW_MESSAGE_JOB_TYPE,
    RawMessageIngestor,
    RawMessageInput,
    RawMessageOrigin,
)
from freelancer_bot.persistence.schema import (
    durable_jobs,
    message_prefilter_results,
    opportunities,
    opportunity_analysis_cache,
    raw_messages,
)
from freelancer_bot.persistence.source_repository import SourceRepository, SourceStatus
from freelancer_bot.storage import Storage
from freelancer_bot.telegram_collector import TelegramCollectorSource
from freelancer_bot.worker import DurableWorker, WorkerOptions

NOW = datetime(2026, 8, 9, 20, 0, tzinfo=timezone.utc)
TRACE_ID = UUID("99999999-9999-9999-9999-999999999999")
CANARY_SECRET = "G3_PIPELINE_SECRET_739104"
RAW_CANARY = "G3_RAW_MESSAGE_CONTENT_518207"


class FakeMessage:
    def __init__(self, message_id: int, text: str, date: datetime) -> None:
        self.id = message_id
        self.message = text
        self.date = date
        self.chat_id = -10090005
        self.post = True


class FakeHistoryClient:
    def __init__(self, messages: list[FakeMessage]) -> None:
        self.messages = messages
        self.iter_calls: list[tuple[object, int]] = []

    def iter_messages(self, entity: object, *, limit: int):
        self.iter_calls.append((entity, limit))

        async def iterate():
            for message in self.messages[:limit]:
                yield message

        return iterate()


class RecordingDelivery:
    def __init__(self) -> None:
        self.calls: list[tuple[int, str, int]] = []

    async def deliver_lead(self, chat_id: int, body: str, lead_id: int) -> int:
        self.calls.append((chat_id, body, lead_id))
        return 7000 + len(self.calls)


@unittest.skipUnless(TEST_DATABASE_URL, "TEST_DATABASE_URL is not configured")
class G3PipelineRuntimeTest(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.database_context = temporary_database()
        self.database_url = self.database_context.__enter__()
        migrate_to_head(self.database_url)
        self.tempdir = tempfile.TemporaryDirectory()
        self.database = Database(self.database_url, pool_size=8, max_overflow=16)
        self.config = _runtime_config(
            self.database_url,
            Path(self.tempdir.name) / "legacy.sqlite3",
        )
        self.log_output = io.StringIO()
        self.logger = configure_structured_logger(
            "freelancer_bot",
            redactor=Redactor.from_config(self.config),
            stream=self.log_output,
        )
        self.sources = SourceRepository()
        self.accounts = CollectorAccountRepository()
        self.jobs = DurableJobRepository()

    async def asyncSetUp(self):
        async with self.database.transaction() as connection:
            self.account = await self.accounts.ensure(
                connection,
                platform="telegram",
                external_account_id="90005",
                display_name="G3-T05 collector",
            )
            candidate = await self.sources.create_candidate(
                connection,
                platform="telegram",
                external_id="username:g3_t05",
                access_type="public",
                display_name="G3-T05 source",
                handle="@g3_t05",
                canonical_url="https://t.me/g3_t05",
                provider="g3_t05_fixture",
                lineage_key="g3-t05:source",
            )
            source = await self.sources.transition(
                connection,
                candidate.id,
                SourceStatus.APPROVED,
                reason="G3-T05 fixture approved",
            )
        self.source = TelegramCollectorSource(source, "@g3_t05")

    async def test_configured_analyzer_uses_selected_provider_and_explicit_fallback(self):
        deepseek_config = self.config.model_copy(
            update={
                "opportunity_analysis_provider": "deepseek",
                "deepseek_api_key": SecretStr("deepseek-test-secret"),
                "opportunity_analysis_model": "deepseek-v4-flash",
            }
        )
        analyzer = _configured_analyzer(self.database, deepseek_config)

        self.assertIsNotNone(analyzer)
        self.assertEqual(analyzer.provider, "deepseek")
        self.assertEqual(analyzer.cache_identity["primary"]["provider"], "deepseek")

        missing_selected_key = self.config.model_copy(
            update={
                "openai_api_key": SecretStr("openai-test-secret"),
                "opportunity_analysis_provider": "deepseek",
                "deepseek_api_key": None,
            }
        )
        self.assertIsNone(_configured_analyzer(self.database, missing_selected_key))

        cross_provider = self.config.model_copy(
            update={
                "openai_api_key": SecretStr("openai-test-secret"),
                "deepseek_api_key": SecretStr("deepseek-test-secret"),
                "opportunity_analysis_provider": "deepseek",
                "opportunity_analysis_fallback_enabled": True,
                "opportunity_analysis_fallback_provider": "openai",
            }
        )
        routed = _configured_analyzer(self.database, cross_provider)

        self.assertIsNotNone(routed)
        self.assertEqual(routed.cache_identity["primary"]["provider"], "deepseek")
        self.assertEqual(routed.cache_identity["fallback"]["provider"], "openai")

        missing_fallback_key = self.config.model_copy(
            update={
                "deepseek_api_key": SecretStr("deepseek-test-secret"),
                "opportunity_analysis_provider": "deepseek",
                "opportunity_analysis_fallback_enabled": True,
                "opportunity_analysis_fallback_provider": "openai",
                "openai_api_key": None,
            }
        )
        with self.assertRaisesRegex(
            OpportunityAnalysisError,
            "fallback provider is unavailable",
        ):
            _configured_analyzer(self.database, missing_fallback_key)

    async def asyncTearDown(self):
        await self.database.close()
        self.tempdir.cleanup()
        self.database_context.__exit__(None, None, None)

    async def test_live_then_bounded_restart_catch_up_is_exactly_once(self):
        storage = Storage(self.config.database_path)
        storage.add_subscriber(501)
        first_delivery = RecordingDelivery()
        first_runtime = TelegramIngestionRuntime(
            self.database,
            self.config,
            logger=self.logger,
            worker_id="g3-live-worker",
        )
        first_bot = self._bot(
            storage,
            first_delivery,
            user_client=FakeHistoryClient([]),
        )
        old = FakeMessage(100, f"Нужен телеграм бот {RAW_CANARY}", NOW)

        await first_runtime.start()
        try:
            await first_bot._dispatch_message(self.source, old, origin="live")
            await self._wait_for_raw_jobs(completed=1)
        finally:
            await first_runtime.stop()
            storage.close()

        await self.database.close()
        self.database = Database(self.database_url, pool_size=8, max_overflow=16)
        restarted_storage = Storage(self.config.database_path)
        restarted_delivery = RecordingDelivery()
        new = FakeMessage(
            101,
            f"Нужен телеграм бот {RAW_CANARY}",
            NOW + timedelta(minutes=1),
        )
        outside_bound = FakeMessage(
            99,
            "Нужен телеграм бот outside configured bound",
            NOW - timedelta(minutes=1),
        )
        history = FakeHistoryClient([new, old, outside_bound])
        restarted_bot = self._bot(
            restarted_storage,
            restarted_delivery,
            user_client=history,
        )
        restarted_runtime = TelegramIngestionRuntime(
            self.database,
            self.config,
            logger=self.logger,
            worker_id="g3-restarted-worker",
        )

        await restarted_runtime.start()
        try:
            await restarted_bot._catch_up(((self.source, "entity:g3_t05"),))
            await self._wait_for_raw_jobs(completed=2)
        finally:
            await restarted_runtime.stop()

        async with self.database.connect() as connection:
            raw_rows = (
                await connection.execute(
                    sa.select(raw_messages).order_by(
                        raw_messages.c.external_message_id
                    )
                )
            ).mappings().all()
            prefilter_rows = (
                await connection.execute(
                    sa.select(message_prefilter_results).order_by(
                        message_prefilter_results.c.created_at
                    )
                )
            ).mappings().all()
            analysis_count = await connection.scalar(
                sa.select(sa.func.count())
                .select_from(durable_jobs)
                .where(durable_jobs.c.job_type == OPPORTUNITY_ANALYSIS_JOB_TYPE)
            )

        self.assertEqual(history.iter_calls, [("entity:g3_t05", 2)])
        self.assertEqual(
            [row["external_message_id"] for row in raw_rows],
            [100, 101],
        )
        self.assertEqual(
            [row["ingestion_origin"] for row in raw_rows],
            [RawMessageOrigin.LIVE.value, RawMessageOrigin.CATCH_UP.value],
        )
        self.assertEqual(len(prefilter_rows), 2)
        self.assertEqual({row["decision"] for row in prefilter_rows}, {"passed"})
        self.assertEqual(
            {row["analysis_job_id"] for row in prefilter_rows},
            {prefilter_rows[0]["analysis_job_id"]},
        )
        self.assertEqual(
            {row["dedup_relation"] for row in prefilter_rows},
            {"canonical", "exact_duplicate"},
        )
        self.assertEqual(analysis_count, 1)
        self.assertEqual(len(first_delivery.calls), 1)
        self.assertEqual(len(restarted_delivery.calls), 1)
        self.assertIn("/101", restarted_delivery.calls[0][1])
        self.assertEqual(
            restarted_storage.stats(),
            {"leads": 2, "pending": 0, "subscribers": 1},
        )
        restarted_storage.close()

        serialized = self.log_output.getvalue()
        self.assertNotIn(RAW_CANARY, serialized)
        first_trace = str(raw_rows[0]["correlation_id"])
        traced_events = {
            payload["event"]
            for payload in _log_payloads(serialized)
            if payload.get("correlation_id") == first_trace
        }
        self.assertTrue(
            {
                "telegram.collector.message_dispatched",
                "telegram.collector.raw_message_persisted",
                "job.claimed",
                "telegram.prefilter.completed",
                "job.completed",
            }.issubset(traced_events)
        )

    async def test_forced_retry_redacts_secret_and_creates_one_logical_result(self):
        ingested = await RawMessageIngestor(self.database).ingest(
            self._raw_input(200, f"keywordless candidate {RAW_CANARY}")
        )
        processor = RawMessagePrefilterProcessor(
            self.database,
            jobs=self.jobs,
            logger=self.logger,
        )

        class FailOnce:
            def __init__(self):
                self.calls = 0

            async def __call__(self, claim):
                self.calls += 1
                if self.calls == 1:
                    try:
                        raise ValueError(f"database cause {CANARY_SECRET}")
                    except ValueError as cause:
                        raise RuntimeError(
                            f"forced retry {CANARY_SECRET}"
                        ) from cause
                await processor(claim)

        fail_once = FailOnce()
        worker = DurableWorker(
            self.database,
            repository=self.jobs,
            worker_id="g3-retry-worker",
            handlers={RAW_MESSAGE_JOB_TYPE: fail_once},
            logger=self.logger,
            options=WorkerOptions(
                poll_interval=0.01,
                lease_duration=0.5,
                heartbeat_interval=0.1,
                retry_delay=0,
                shutdown_timeout=0.1,
            ),
            close_database_on_exit=False,
        )
        runtime = TelegramIngestionRuntime(
            self.database,
            self.config,
            logger=self.logger,
            worker=worker,
        )

        await runtime.start()
        try:
            await self._wait_for_job_state(
                ingested.message.processing_job_id,
                "completed",
            )
        finally:
            await runtime.stop()

        async with self.database.connect() as connection:
            raw_job = await self.jobs.get(
                connection, ingested.message.processing_job_id
            )
            result_count = await connection.scalar(
                sa.select(sa.func.count())
                .select_from(message_prefilter_results)
                .where(
                    message_prefilter_results.c.raw_message_id
                    == ingested.message.id
                )
            )
            analysis_count = await connection.scalar(
                sa.select(sa.func.count())
                .select_from(durable_jobs)
                .where(durable_jobs.c.job_type == OPPORTUNITY_ANALYSIS_JOB_TYPE)
            )

        self.assertEqual(fail_once.calls, 2)
        self.assertEqual(raw_job["attempt_count"], 2)
        self.assertEqual((result_count, analysis_count), (1, 1))
        serialized = self.log_output.getvalue()
        self.assertNotIn(CANARY_SECRET, serialized)
        self.assertNotIn(RAW_CANARY, serialized)
        retry = next(
            payload
            for payload in _log_payloads(serialized)
            if payload["event"] == "job.retry_scheduled"
        )
        self.assertEqual(retry["correlation_id"], str(TRACE_ID))
        self.assertNotIn(CANARY_SECRET, json.dumps(retry))
        self.assertIn("[REDACTED]", retry["error"]["message"])
        self.assertIn("[REDACTED]", retry["error"]["stack"])
        self.assertIn("[REDACTED]", retry["error"]["cause"]["message"])

    async def test_concurrent_live_and_catch_up_dispatch_delivers_once(self):
        storage = Storage(self.config.database_path)
        storage.add_subscriber(502)
        delivery = RecordingDelivery()
        bot = self._bot(storage, delivery, user_client=FakeHistoryClient([]))
        message = FakeMessage(250, "Нужен телеграм бот concurrent", NOW)
        runtime = TelegramIngestionRuntime(
            self.database,
            self.config,
            logger=self.logger,
            worker_id="g3-concurrent-worker",
        )

        await runtime.start()
        try:
            await asyncio.gather(
                bot._dispatch_message(self.source, message, origin="live"),
                bot._dispatch_message(self.source, message, origin="catch_up"),
            )
            await self._wait_for_raw_jobs(completed=1)
        finally:
            await runtime.stop()

        async with self.database.connect() as connection:
            counts = (
                await connection.scalar(
                    sa.select(sa.func.count()).select_from(raw_messages)
                ),
                await connection.scalar(
                    sa.select(sa.func.count()).select_from(
                        message_prefilter_results
                    )
                ),
                await connection.scalar(
                    sa.select(sa.func.count())
                    .select_from(durable_jobs)
                    .where(
                        durable_jobs.c.job_type
                        == OPPORTUNITY_ANALYSIS_JOB_TYPE
                    )
                ),
            )
        self.assertEqual(counts, (1, 1, 1))
        self.assertEqual(len(delivery.calls), 1)
        self.assertEqual(
            storage.stats(),
            {"leads": 1, "pending": 0, "subscribers": 1},
        )
        storage.close()

    async def test_collector_keywordless_message_still_reaches_v2_analysis(self):
        storage = Storage(self.config.database_path)
        storage.add_subscriber(503)
        delivery = RecordingDelivery()
        bot = self._bot(storage, delivery, user_client=FakeHistoryClient([]))
        message = FakeMessage(
            275,
            "Подскажите исполнителя для автоматизации",
            NOW,
        )
        runtime = TelegramIngestionRuntime(
            self.database,
            self.config,
            logger=self.logger,
            worker_id="g3-keywordless-worker",
        )

        await runtime.start()
        try:
            await bot._dispatch_message(self.source, message, origin="live")
            await self._wait_for_raw_jobs(completed=1)
        finally:
            await runtime.stop()

        async with self.database.connect() as connection:
            result = (
                await connection.execute(
                    sa.select(message_prefilter_results)
                )
            ).mappings().one()
            analysis_count = await connection.scalar(
                sa.select(sa.func.count())
                .select_from(durable_jobs)
                .where(durable_jobs.c.job_type == OPPORTUNITY_ANALYSIS_JOB_TYPE)
            )
        self.assertEqual(result["decision"], "passed")
        self.assertEqual(result["dedup_relation"], "canonical")
        self.assertEqual(analysis_count, 1)
        self.assertEqual(delivery.calls, [])
        self.assertEqual(
            storage.stats(),
            {"leads": 0, "pending": 0, "subscribers": 1},
        )
        storage.close()

    async def test_configured_deepseek_pipeline_persists_opportunity_and_matching_boundary(self):
        config = self.config.model_copy(
            update={
                "opportunity_analysis_provider": "deepseek",
                "deepseek_api_key": SecretStr("deepseek-test-secret"),
                "opportunity_analysis_model": "deepseek-v4-flash",
                "opportunity_analysis_max_output_attempts": 1,
            }
        )
        storage = Storage(config.database_path)
        delivery = RecordingDelivery()
        bot = self._bot(storage, delivery, user_client=FakeHistoryClient([]))
        runtime = TelegramIngestionRuntime(
            self.database,
            config,
            logger=self.logger,
            worker_id="g3-deepseek-worker",
        )
        response = _opportunity_provider_response()
        requests = []

        def fake_urlopen(request, timeout):
            requests.append(request.full_url)
            return _Response(response)

        await runtime.start()
        try:
            with patch(
                "freelancer_bot.opportunity_analysis.urllib.request.urlopen",
                fake_urlopen,
            ):
                await bot._dispatch_message(
                    self.source,
                    FakeMessage(325, "Нужен Telegram-бот на Python", NOW),
                    origin="live",
                )
                await self._wait_for(
                    lambda connection: self._opportunity_boundary_ready(connection)
                )
        finally:
            await runtime.stop()

        async with self.database.connect() as connection:
            opportunity_count = await connection.scalar(
                sa.select(sa.func.count()).select_from(opportunities)
            )
            matching_count = await connection.scalar(
                sa.select(sa.func.count())
                .select_from(durable_jobs)
                .where(durable_jobs.c.job_type == "opportunity.matching_delivery.v1")
            )
            cache = (
                await connection.execute(sa.select(opportunity_analysis_cache))
            ).mappings().one()

        self.assertEqual(requests, ["https://api.deepseek.com/chat/completions"])
        self.assertEqual(opportunity_count, 1)
        self.assertEqual(matching_count, 1)
        self.assertEqual(cache["result"]["invocation"]["provider"], "deepseek")
        self.assertEqual(delivery.calls, [])
        storage.close()

    async def test_shutdown_timeout_requeues_and_restart_completes(self):
        ingested = await RawMessageIngestor(self.database).ingest(
            self._raw_input(300, "Persisted before bounded shutdown")
        )
        started = asyncio.Event()

        async def blocking_handler(claim):
            started.set()
            await asyncio.Event().wait()

        blocking_worker = DurableWorker(
            self.database,
            repository=self.jobs,
            worker_id="g3-blocked-worker",
            handlers={RAW_MESSAGE_JOB_TYPE: blocking_handler},
            logger=self.logger,
            options=WorkerOptions(
                poll_interval=0.01,
                lease_duration=0.5,
                heartbeat_interval=0.1,
                retry_delay=0,
                shutdown_timeout=0.02,
            ),
            close_database_on_exit=False,
        )
        interrupted = TelegramIngestionRuntime(
            self.database,
            self.config,
            logger=self.logger,
            worker=blocking_worker,
        )

        await interrupted.start()
        await asyncio.wait_for(started.wait(), timeout=1)
        stopping_started = monotonic()
        await interrupted.stop()
        self.assertLess(monotonic() - stopping_started, 1)
        async with self.database.connect() as connection:
            released = await self.jobs.get(
                connection, ingested.message.processing_job_id
            )
        self.assertEqual(released["state"], "queued")
        self.assertIsNone(released["lease_owner"])

        recovered = TelegramIngestionRuntime(
            self.database,
            self.config,
            logger=self.logger,
            worker_id="g3-recovery-worker",
        )
        await recovered.start()
        try:
            await self._wait_for_job_state(
                ingested.message.processing_job_id,
                "completed",
            )
        finally:
            await recovered.stop()

        async with self.database.connect() as connection:
            completed = await self.jobs.get(
                connection, ingested.message.processing_job_id
            )
            result_count = await connection.scalar(
                sa.select(sa.func.count())
                .select_from(message_prefilter_results)
                .where(
                    message_prefilter_results.c.raw_message_id
                    == ingested.message.id
                )
            )
            analysis_count = await connection.scalar(
                sa.select(sa.func.count())
                .select_from(durable_jobs)
                .where(durable_jobs.c.job_type == OPPORTUNITY_ANALYSIS_JOB_TYPE)
            )
        self.assertEqual(completed["state"], "completed")
        self.assertEqual(completed["attempt_count"], 2)
        self.assertEqual((result_count, analysis_count), (1, 1))

    async def test_unexpected_worker_exit_is_propagated_to_collector(self):
        release = asyncio.Event()

        class FailingWorker:
            async def run(self, *, install_signal_handlers):
                self.install_signal_handlers = install_signal_handlers
                await release.wait()
                raise RuntimeError("raw worker stopped")

            def request_stop(self):
                pass

        worker = FailingWorker()
        runtime = TelegramIngestionRuntime(
            self.database,
            self.config,
            logger=self.logger,
            worker=worker,
        )
        collector_stop = asyncio.Event()
        await runtime.start()
        waiting = asyncio.create_task(
            runtime.wait_until_collector_stops(collector_stop.wait())
        )
        release.set()

        with self.assertRaisesRegex(RuntimeError, "raw worker stopped"):
            await waiting
        self.assertFalse(collector_stop.is_set())
        self.assertFalse(worker.install_signal_handlers)
        with self.assertRaisesRegex(RuntimeError, "raw worker stopped"):
            await runtime.stop()

    def _bot(self, storage, delivery, *, user_client):
        bot = LeadBot.__new__(LeadBot)
        bot.config = self.config
        bot.user_client = user_client
        bot.collector_account_id = self.account.id
        bot.raw_ingestor = RawMessageIngestor(self.database)
        bot.legacy_processor = LegacyLeadProcessor(
            FilterConfig(
                min_score=5,
                keywords={"телеграм бот": 5},
                stop_words=(),
            ),
            storage,
            storage,
            delivery,
        )
        return bot

    def _raw_input(self, message_id: int, content: str) -> RawMessageInput:
        return RawMessageInput(
            source_id=self.source.record.id,
            collector_account_id=self.account.id,
            external_message_id=message_id,
            message_date=NOW,
            observed_at=NOW,
            message_url=self.source.message_url(message_id),
            content=content,
            transport_metadata={},
            ingestion_origin=RawMessageOrigin.LIVE,
            correlation_id=TRACE_ID,
        )

    async def _wait_for_raw_jobs(self, *, completed: int) -> None:
        async def predicate(connection):
            count = await connection.scalar(
                sa.select(sa.func.count())
                .select_from(durable_jobs)
                .where(
                    durable_jobs.c.job_type == RAW_MESSAGE_JOB_TYPE,
                    durable_jobs.c.state == "completed",
                )
            )
            return count == completed

        await self._wait_for(predicate)

    async def _wait_for_job_state(self, job_id, state: str) -> None:
        async def predicate(connection):
            row = await self.jobs.get(connection, job_id)
            return row is not None and row["state"] == state

        await self._wait_for(predicate)

    async def _opportunity_boundary_ready(self, connection) -> bool:
        analysis_completed = await connection.scalar(
            sa.select(sa.func.count())
            .select_from(durable_jobs)
            .where(
                durable_jobs.c.job_type == OPPORTUNITY_ANALYSIS_JOB_TYPE,
                durable_jobs.c.state == "completed",
            )
        )
        opportunity_count = await connection.scalar(
            sa.select(sa.func.count()).select_from(opportunities)
        )
        matching_queued = await connection.scalar(
            sa.select(sa.func.count())
            .select_from(durable_jobs)
            .where(
                durable_jobs.c.job_type == "opportunity.matching_delivery.v1",
            )
        )
        return (
            analysis_completed == 1
            and opportunity_count == 1
            and matching_queued == 1
        )

    async def _wait_for(self, predicate) -> None:
        deadline = monotonic() + 2
        while monotonic() < deadline:
            async with self.database.connect() as connection:
                if await predicate(connection):
                    return
            await asyncio.sleep(0.01)
        self.fail("Timed out waiting for durable pipeline state")


class _Response:
    def __init__(self, payload: str) -> None:
        self.payload = payload

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return None

    def read(self) -> bytes:
        return self.payload.encode("utf-8")


def _opportunity_provider_response() -> str:
    analysis = {
        "schema_version": "opportunity_analysis.v1",
        "is_opportunity": True,
        "confidence": 0.94,
        "market_direction": "buyer_to_specialist",
        "intent_stage": "active",
        "opportunity_type": "project",
        "category": "telegram_development",
        "role_title": "Telegram bot developer",
        "skills": ["Python", "Telegram Bot API"],
        "task_summary": "Build a Telegram bot",
        "budget": {
            "known": False,
            "min": None,
            "max": None,
            "currency": None,
            "period": None,
            "explicit": False,
        },
        "work": {
            "remote": True,
            "location": None,
            "full_time": None,
            "part_time": None,
        },
        "language": "ru",
        "contact": {"telegram": None, "email": None, "url": None},
        "quality": {
            "actionability": 0.9,
            "commercial_plausibility": 0.8,
            "specificity": 0.8,
            "credibility": 0.8,
        },
        "red_flags": [],
    }
    return json.dumps(
        {
            "model": "deepseek-v4-flash-fixture",
            "usage": {
                "prompt_tokens": 17,
                "completion_tokens": 31,
                "total_tokens": 48,
            },
            "choices": [{"message": {"content": json.dumps(analysis)}}],
        }
    )


def _runtime_config(database_url: str, database_path: Path) -> RuntimeConfig:
    return RuntimeConfig(
        app_environment="test",
        api_id=12345,
        api_hash=CANARY_SECRET,
        bot_token=f"123456:{CANARY_SECRET}abcdefghijklmnop",
        # Keep this durable-runtime fixture fully offline even when the
        # developer shell has a real OpenAI key exported for live testing.
        openai_api_key=None,
        database_url=database_url,
        database_path=database_path,
        catch_up_limit=2,
        send_catch_up=True,
        # These tests exercise the legacy compatibility path explicitly.  The
        # production default remains V2-only, so opt in here rather than
        # coupling the fixture to the runtime default.
        legacy_delivery_enabled=True,
        worker_poll_interval_seconds=0.01,
        worker_lease_seconds=0.5,
        worker_heartbeat_seconds=0.1,
        worker_retry_delay_seconds=0,
        worker_shutdown_timeout_seconds=0.1,
        _env_file=None,
    )


def _log_payloads(serialized: str) -> list[dict[str, object]]:
    return [json.loads(line) for line in serialized.splitlines() if line.strip()]


if __name__ == "__main__":
    unittest.main()
