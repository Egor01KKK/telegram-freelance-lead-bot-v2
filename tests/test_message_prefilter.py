from __future__ import annotations

import io
import unittest
from datetime import datetime, timezone
from uuid import UUID

import sqlalchemy as sa

from freelancer_bot.filters import FilterConfig, match_text
from freelancer_bot.message_prefilter import (
    AnalyzerInputLoader,
    OPPORTUNITY_ANALYSIS_JOB_TYPE,
    PREFILTER_SCHEMA_VERSION,
    PrefilterDecision,
    PrefilterReason,
    RawMessagePrefilterProcessor,
)
from freelancer_bot.observability import Redactor, configure_structured_logger
from freelancer_bot.persistence.collector_accounts import CollectorAccountRepository
from freelancer_bot.persistence.database import Database
from freelancer_bot.persistence.jobs import DurableJobRepository, JobClaim
from freelancer_bot.persistence.message_prefilter import MessagePrefilterRepository
from freelancer_bot.persistence.raw_messages import (
    RAW_MESSAGE_JOB_TYPE,
    RawMessageIngestor,
    RawMessageInput,
    RawMessageOrigin,
)
from freelancer_bot.persistence.schema import (
    durable_jobs,
    message_prefilter_results,
)
from freelancer_bot.persistence.source_repository import SourceRepository, SourceStatus
from freelancer_bot.worker import DurableWorker, WorkerOptions
from postgres_support import TEST_DATABASE_URL, migrate_to_head, temporary_database


NOW = datetime(2026, 8, 9, 17, 0, tzinfo=timezone.utc)
TRACE_ID = UUID("77777777-7777-7777-7777-777777777777")


@unittest.skipUnless(TEST_DATABASE_URL, "TEST_DATABASE_URL is not configured")
class MessagePrefilterPipelineTest(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.database_context = temporary_database()
        self.database_url = self.database_context.__enter__()
        migrate_to_head(self.database_url)
        self.database = Database(self.database_url, pool_size=8, max_overflow=16)
        self.sources = SourceRepository()
        self.accounts = CollectorAccountRepository()
        self.jobs = DurableJobRepository()
        self.results = MessagePrefilterRepository()
        self.source = None
        self.account = None

    async def asyncSetUp(self):
        async with self.database.transaction() as connection:
            self.account = await self.accounts.ensure(
                connection,
                platform="telegram",
                external_account_id="90003",
                display_name="G3-T03 collector",
            )
            candidate = await self.sources.create_candidate(
                connection,
                platform="telegram",
                external_id="username:g3_t03",
                access_type="public",
                display_name="G3-T03 source",
                handle="@g3_t03",
                canonical_url="https://t.me/g3_t03",
                provider="g3_t03_fixture",
                lineage_key="g3-t03:source",
            )
            self.source = await self.sources.transition(
                connection,
                candidate.id,
                SourceStatus.APPROVED,
                reason="G3-T03 fixture approved",
            )

    async def asyncTearDown(self):
        await self.database.close()
        self.database_context.__exit__(None, None, None)

    async def test_keywordless_candidate_reaches_analysis_queue_through_worker(self):
        content = "Подскажите исполнителя для автоматизации внутреннего процесса."
        legacy = FilterConfig(
            min_score=10,
            keywords={"телеграм бот": 5, "python": 2},
            stop_words=(),
        )
        self.assertFalse(match_text(content, legacy).accepted)
        ingested = await self._ingest(701, content=content)
        stream = io.StringIO()
        logger = configure_structured_logger(
            f"test.prefilter.{id(self)}",
            redactor=Redactor(),
            stream=stream,
        )
        processor = RawMessagePrefilterProcessor(self.database, logger=logger)
        worker = None

        async def handle(claim):
            await processor(claim)
            worker.request_stop()

        worker = DurableWorker(
            self.database,
            repository=self.jobs,
            worker_id="prefilter-worker",
            handlers={RAW_MESSAGE_JOB_TYPE: handle},
            logger=logger,
            options=WorkerOptions(
                poll_interval=0.01,
                lease_duration=1,
                heartbeat_interval=0.1,
                retry_delay=0,
                shutdown_timeout=0.5,
            ),
            close_database_on_exit=False,
        )
        await worker.run(install_signal_handlers=False)

        async with self.database.connect() as connection:
            raw_job = await self.jobs.get(
                connection,
                ingested.message.processing_job_id,
            )
            result = await self.results.get_for_raw(
                connection,
                raw_message_id=ingested.message.id,
                schema_version=PREFILTER_SCHEMA_VERSION,
            )
            analysis_job = await self.jobs.get(connection, result.analysis_job_id)
        self.assertEqual(raw_job["state"], "completed")
        self.assertEqual(result.decision, PrefilterDecision.PASSED.value)
        self.assertEqual(result.reason_codes, ())
        self.assertEqual(analysis_job["job_type"], OPPORTUNITY_ANALYSIS_JOB_TYPE)
        self.assertEqual(analysis_job["state"], "queued")
        self.assertEqual(analysis_job["attempt_count"], 0)
        self.assertNotIn(content, stream.getvalue())

    async def test_empty_and_service_events_stop_before_analysis_queue(self):
        empty = await self._ingest(702, content="  ")
        service = await self._ingest(
            703,
            content="A member joined",
            metadata={"service_action_type": "MessageActionChatAddUser"},
        )
        processor = RawMessagePrefilterProcessor(self.database)

        empty_result = await processor.process(self._claim(empty))
        service_result = await processor.process(self._claim(service))

        self.assertEqual(
            empty_result.reason_codes,
            (PrefilterReason.EMPTY_CONTENT.value,),
        )
        self.assertEqual(
            service_result.reason_codes,
            (PrefilterReason.SERVICE_EVENT.value,),
        )
        self.assertEqual(empty_result.decision, PrefilterDecision.REJECTED.value)
        self.assertEqual(service_result.decision, PrefilterDecision.REJECTED.value)
        async with self.database.connect() as connection:
            analysis_count = await connection.scalar(
                sa.select(sa.func.count())
                .select_from(durable_jobs)
                .where(durable_jobs.c.job_type == OPPORTUNITY_ANALYSIS_JOB_TYPE)
            )
        self.assertEqual(analysis_count, 0)

    async def test_retry_and_restart_reconstruct_only_current_and_direct_parent(self):
        await self._ingest(710, content="Unrelated history")
        parent = await self._ingest(711, content="Parent request context")
        reply = await self._ingest(
            712,
            content="Да, могу это сделать.",
            metadata={"reply_to_msg_id": 711},
        )
        processor = RawMessagePrefilterProcessor(self.database)

        first = await processor.process(self._claim(reply))
        second = await processor.process(self._claim(reply))

        self.assertEqual(first.id, second.id)
        self.assertEqual(first.analysis_job_id, second.analysis_job_id)
        self.assertEqual(first.parent_raw_message_id, parent.message.id)
        async with self.database.connect() as connection:
            result_count = await connection.scalar(
                sa.select(sa.func.count())
                .select_from(message_prefilter_results)
                .where(
                    message_prefilter_results.c.raw_message_id
                    == reply.message.id
                )
            )
            analysis_count = await connection.scalar(
                sa.select(sa.func.count())
                .select_from(durable_jobs)
                .where(durable_jobs.c.job_type == OPPORTUNITY_ANALYSIS_JOB_TYPE)
            )
        self.assertEqual((result_count, analysis_count), (1, 1))

        await self.database.close()
        self.database = Database(self.database_url)
        restored = await AnalyzerInputLoader(self.database).load(first.analysis_job_id)

        self.assertEqual(restored.current.external_message_id, 712)
        self.assertEqual(restored.current.content, "Да, могу это сделать.")
        self.assertEqual(restored.parent.external_message_id, 711)
        self.assertEqual(restored.parent.content, "Parent request context")
        self.assertFalse(hasattr(restored, "history"))

    async def test_prefilter_transaction_rolls_back_analysis_job_and_result(self):
        ingested = await self._ingest(720, content="Keep the raw input durable")

        class FailingResultRepository(MessagePrefilterRepository):
            async def record(self, connection, **kwargs):
                await super().record(connection, **kwargs)
                raise RuntimeError("forced prefilter rollback")

        processor = RawMessagePrefilterProcessor(
            self.database,
            results=FailingResultRepository(),
        )
        with self.assertRaisesRegex(RuntimeError, "forced prefilter rollback"):
            await processor.process(self._claim(ingested))

        async with self.database.connect() as connection:
            result_count = await connection.scalar(
                sa.select(sa.func.count()).select_from(message_prefilter_results)
            )
            analysis_count = await connection.scalar(
                sa.select(sa.func.count())
                .select_from(durable_jobs)
                .where(durable_jobs.c.job_type == OPPORTUNITY_ANALYSIS_JOB_TYPE)
            )
            raw_job = await self.jobs.get(
                connection,
                ingested.message.processing_job_id,
            )
        self.assertEqual((result_count, analysis_count), (0, 0))
        self.assertEqual(raw_job["state"], "queued")

    async def _ingest(self, message_id, *, content, metadata=None):
        return await RawMessageIngestor(self.database).ingest(
            RawMessageInput(
                source_id=self.source.id,
                collector_account_id=self.account.id,
                external_message_id=message_id,
                message_date=NOW,
                observed_at=NOW,
                message_url=f"https://t.me/g3_t03/{message_id}",
                content=content,
                transport_metadata={} if metadata is None else metadata,
                ingestion_origin=RawMessageOrigin.LIVE,
                correlation_id=TRACE_ID,
            )
        )

    @staticmethod
    def _claim(ingested):
        return JobClaim(
            id=ingested.message.processing_job_id,
            job_type=RAW_MESSAGE_JOB_TYPE,
            idempotency_key="g3-t03-test",
            correlation_id=TRACE_ID,
            attempt_count=1,
            max_attempts=3,
            worker_id="prefilter-test",
            reclaimed=False,
        )


if __name__ == "__main__":
    unittest.main()
