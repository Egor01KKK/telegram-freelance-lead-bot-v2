from __future__ import annotations

import asyncio
from dataclasses import replace
from datetime import datetime, timezone
import unittest
from uuid import UUID, uuid4

import sqlalchemy as sa
from alembic import command

from freelancer_bot.message_prefilter import (
    OPPORTUNITY_ANALYSIS_JOB_TYPE,
    PREFILTER_SCHEMA_VERSION,
    AnalyzerInputLoader,
    RawMessagePrefilterProcessor,
)
from freelancer_bot.persistence.collector_accounts import CollectorAccountRepository
from freelancer_bot.persistence.database import Database
from freelancer_bot.persistence.jobs import DurableJobRepository, JobClaim
from freelancer_bot.persistence.message_prefilter import (
    AnalysisCacheConflict,
    MessagePrefilterRepository,
    OpportunityAnalysisCacheRepository,
)
from freelancer_bot.persistence.raw_messages import (
    RAW_MESSAGE_JOB_TYPE,
    RawMessageIngestor,
    RawMessageInput,
    RawMessageOrigin,
)
from freelancer_bot.persistence.schema import (
    durable_jobs,
    message_prefilter_results,
    raw_messages,
)
from freelancer_bot.persistence.source_repository import SourceRepository, SourceStatus
from postgres_support import (
    TEST_DATABASE_URL,
    alembic_config,
    migrate_to_head,
    temporary_database,
)


NOW = datetime(2026, 8, 9, 18, 0, tzinfo=timezone.utc)
TRACE_ID = UUID("88888888-8888-8888-8888-888888888888")


@unittest.skipUnless(TEST_DATABASE_URL, "TEST_DATABASE_URL is not configured")
class ExactDedupCacheTest(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.database_context = temporary_database()
        self.database_url = self.database_context.__enter__()
        migrate_to_head(self.database_url)
        self.database = Database(self.database_url, pool_size=8, max_overflow=16)
        self.sources = SourceRepository()
        self.accounts = CollectorAccountRepository()
        self.results = MessagePrefilterRepository()
        self.cache = OpportunityAnalysisCacheRepository()

    async def asyncSetUp(self):
        async with self.database.transaction() as connection:
            self.account = await self.accounts.ensure(
                connection,
                platform="telegram",
                external_account_id="90004",
                display_name="G3-T04 collector",
            )
            self.source = await self._create_source(connection, "g3_t04_a")
            self.other_source = await self._create_source(connection, "g3_t04_b")

    async def asyncTearDown(self):
        await self.database.close()
        self.database_context.__exit__(None, None, None)

    async def test_cross_source_normalized_reposts_share_one_analysis_job(self):
        first = await self._ingest(
            self.source.id,
            801,
            "  НУЖЕН\u00a0БОТ для TELEGRAM  ",
        )
        duplicate = await self._ingest(
            self.other_source.id,
            901,
            "нужен бот ДЛЯ telegram",
        )
        processor = RawMessagePrefilterProcessor(self.database)

        first_result = await processor.process(self._claim(first))
        duplicate_result = await processor.process(self._claim(duplicate))

        self.assertEqual(first_result.dedup_relation, "canonical")
        self.assertEqual(duplicate_result.dedup_relation, "exact_duplicate")
        self.assertEqual(duplicate_result.canonical_prefilter_result_id, first_result.id)
        self.assertEqual(first_result.analysis_job_id, duplicate_result.analysis_job_id)
        self.assertEqual(first_result.normalized_content, "нужен бот для telegram")
        async with self.database.connect() as connection:
            linked = await self.results.list_for_analysis_job(
                connection, first_result.analysis_job_id
            )
            source_ids = set(
                (
                    await connection.execute(
                        sa.select(raw_messages.c.source_id).where(
                            raw_messages.c.id.in_([item.raw_message_id for item in linked])
                        )
                    )
                ).scalars()
            )
            analysis_count = await self._analysis_count(connection)
        self.assertEqual(len(linked), 2)
        self.assertEqual(source_ids, {self.source.id, self.other_source.id})
        self.assertEqual(analysis_count, 1)

    async def test_concurrent_reposts_converge_on_one_canonical_job(self):
        first = await self._ingest(self.source.id, 802, "Нужен Python разработчик")
        duplicate = await self._ingest(
            self.other_source.id, 902, "нужен python разработчик"
        )
        processor = RawMessagePrefilterProcessor(self.database)

        routed = await asyncio.gather(
            processor.process(self._claim(first)),
            processor.process(self._claim(duplicate)),
        )

        self.assertEqual({item.analysis_job_id for item in routed}, {routed[0].analysis_job_id})
        self.assertEqual(
            {item.dedup_relation for item in routed},
            {"canonical", "exact_duplicate"},
        )
        async with self.database.connect() as connection:
            self.assertEqual(await self._analysis_count(connection), 1)

    async def test_concurrent_reclaim_of_same_raw_job_is_idempotent(self):
        ingested = await self._ingest(self.source.id, 803, "Нужен backend бот")
        processor = RawMessagePrefilterProcessor(self.database)

        routed = await asyncio.gather(
            processor.process(self._claim(ingested)),
            processor.process(self._claim(ingested)),
        )

        self.assertEqual(routed[0].id, routed[1].id)
        self.assertEqual(routed[0].analysis_job_id, routed[1].analysis_job_id)
        self.assertEqual(routed[0].dedup_relation, "canonical")
        async with self.database.connect() as connection:
            self.assertEqual(await self._analysis_count(connection), 1)

    async def test_parent_context_and_window_bound_exact_reuse(self):
        await self._ingest(self.source.id, 810, "Первый родитель")
        await self._ingest(self.source.id, 811, "Второй родитель")
        first = await self._ingest(
            self.source.id,
            812,
            "Сделаю",
            metadata={"reply_to_msg_id": 810},
        )
        different_parent = await self._ingest(
            self.source.id,
            813,
            "сделаю",
            metadata={"reply_to_msg_id": 811},
        )
        processor = RawMessagePrefilterProcessor(self.database)
        first_result = await processor.process(self._claim(first))
        other_result = await processor.process(self._claim(different_parent))
        self.assertNotEqual(first_result.analysis_job_id, other_result.analysis_job_id)

        async with self.database.transaction() as connection:
            await connection.execute(
                sa.update(message_prefilter_results)
                .where(message_prefilter_results.c.id == first_result.id)
                .values(created_at=sa.func.now() - sa.text("INTERVAL '8 days'"))
            )
        after_window = await self._ingest(
            self.source.id,
            814,
            "СДЕЛАЮ",
            metadata={"reply_to_msg_id": 810},
        )
        after_result = await processor.process(self._claim(after_window))
        self.assertEqual(after_result.dedup_relation, "canonical")
        self.assertNotEqual(first_result.analysis_job_id, after_result.analysis_job_id)

    async def test_cache_is_idempotent_and_strictly_versioned(self):
        ingested = await self._ingest(self.source.id, 820, "Нужен Telegram бот")
        duplicate = await self._ingest(
            self.other_source.id, 920, "нужен telegram бот"
        )
        prefilter = await RawMessagePrefilterProcessor(self.database).process(
            self._claim(ingested)
        )
        duplicate_prefilter = await RawMessagePrefilterProcessor(
            self.database
        ).process(self._claim(duplicate))
        payload = {"schema_version": "opportunity_analysis.v1", "buyer_intent": True}

        async with self.database.transaction() as connection:
            first = await self.cache.store_for_prefilter_result(
                connection, prefilter_result=prefilter, result=payload
            )
            second = await self.cache.store_for_prefilter_result(
                connection, prefilter_result=prefilter, result=payload
            )
            loaded = await self.cache.get_for_prefilter_result(
                connection, duplicate_prefilter
            )
            changed_analyzer = await self.cache.get_for_prefilter_result(
                connection,
                replace(prefilter, analyzer_version="opportunity-analyzer.v2"),
            )

        self.assertTrue(first.created)
        self.assertFalse(second.created)
        self.assertEqual(first.entry.id, second.entry.id)
        self.assertEqual(loaded.result, payload)
        self.assertEqual(
            duplicate_prefilter.canonical_prefilter_result_id,
            prefilter.id,
        )
        self.assertNotEqual(
            duplicate_prefilter.raw_message_id,
            prefilter.raw_message_id,
        )
        self.assertIsNone(changed_analyzer)

        async with self.database.transaction() as connection:
            with self.assertRaises(AnalysisCacheConflict):
                await self.cache.store_for_prefilter_result(
                    connection,
                    prefilter_result=prefilter,
                    result={"schema_version": "opportunity_analysis.v1", "buyer_intent": False},
                )

    async def test_restart_loader_uses_canonical_postgres_input(self):
        first = await self._ingest(self.source.id, 830, "Ищу разработчика бота")
        duplicate = await self._ingest(
            self.other_source.id, 930, "ищу разработчика бота"
        )
        processor = RawMessagePrefilterProcessor(self.database)
        canonical = await processor.process(self._claim(first))
        repeated = await processor.process(self._claim(duplicate))
        self.assertEqual(canonical.analysis_job_id, repeated.analysis_job_id)

        await self.database.close()
        self.database = Database(self.database_url)
        restored = await AnalyzerInputLoader(self.database).load(
            repeated.analysis_job_id
        )
        self.assertEqual(restored.current.raw_message_id, canonical.raw_message_id)
        self.assertEqual(restored.current.content, "Ищу разработчика бота")
        self.assertIsNone(restored.parent)

    async def _create_source(self, connection, handle):
        candidate = await self.sources.create_candidate(
            connection,
            platform="telegram",
            external_id=f"username:{handle}",
            access_type="public",
            display_name=handle,
            handle=f"@{handle}",
            canonical_url=f"https://t.me/{handle}",
            provider="g3_t04_fixture",
            lineage_key=f"g3-t04:{handle}",
        )
        return await self.sources.transition(
            connection,
            candidate.id,
            SourceStatus.APPROVED,
            reason="G3-T04 fixture approved",
        )

    async def _ingest(self, source_id, message_id, content, metadata=None):
        return await RawMessageIngestor(self.database).ingest(
            RawMessageInput(
                source_id=source_id,
                collector_account_id=self.account.id,
                external_message_id=message_id,
                message_date=NOW,
                observed_at=NOW,
                message_url=f"https://t.me/c/{source_id}/{message_id}",
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
            idempotency_key="g3-t04-test",
            correlation_id=TRACE_ID,
            attempt_count=1,
            max_attempts=3,
            worker_id="exact-dedup-test",
            reclaimed=False,
        )

    @staticmethod
    async def _analysis_count(connection):
        return await connection.scalar(
            sa.select(sa.func.count())
            .select_from(durable_jobs)
            .where(durable_jobs.c.job_type == OPPORTUNITY_ANALYSIS_JOB_TYPE)
        )


@unittest.skipUnless(TEST_DATABASE_URL, "TEST_DATABASE_URL is not configured")
class ExistingPrefilterMigrationTest(unittest.TestCase):
    def test_existing_passed_result_is_backfilled_without_identity_change(self):
        with temporary_database() as database_url:
            command.upgrade(alembic_config(database_url), "20260809_0010")
            raw_id, result_id, analysis_job_id = asyncio.run(
                self._seed_existing_result(database_url)
            )

            command.upgrade(alembic_config(database_url), "head")
            result = asyncio.run(self._load_result(database_url, raw_id))

        self.assertEqual(result.id, result_id)
        self.assertEqual(result.analysis_job_id, analysis_job_id)
        self.assertEqual(result.dedup_relation, "canonical")
        self.assertEqual(result.normalized_content, "нужен telegram бот")
        self.assertEqual(result.dedup_window_seconds, 7 * 24 * 60 * 60)

    @staticmethod
    async def _seed_existing_result(database_url):
        database = Database(database_url)
        sources = SourceRepository()
        accounts = CollectorAccountRepository()
        jobs = DurableJobRepository()
        try:
            async with database.transaction() as connection:
                account = await accounts.ensure(
                    connection,
                    platform="telegram",
                    external_account_id="90005",
                    display_name="G3-T03 existing collector",
                )
                candidate = await sources.create_candidate(
                    connection,
                    platform="telegram",
                    external_id="username:g3_t03_existing",
                    access_type="public",
                    display_name="G3-T03 existing source",
                    handle="@g3_t03_existing",
                    canonical_url="https://t.me/g3_t03_existing",
                    provider="g3_t04_migration_fixture",
                    lineage_key="g3-t04:migration-source",
                )
                source = await sources.transition(
                    connection,
                    candidate.id,
                    SourceStatus.APPROVED,
                    reason="Existing G3-T03 fixture approved",
                )
            ingested = await RawMessageIngestor(database).ingest(
                RawMessageInput(
                    source_id=source.id,
                    collector_account_id=account.id,
                    external_message_id=950,
                    message_date=NOW,
                    observed_at=NOW,
                    message_url="https://t.me/g3_t03_existing/950",
                    content="  Нужен TELEGRAM\u00a0бот ",
                    transport_metadata={},
                    ingestion_origin=RawMessageOrigin.LIVE,
                    correlation_id=TRACE_ID,
                )
            )
            result_id = uuid4()
            async with database.transaction() as connection:
                analysis_job_id = await jobs.enqueue(
                    connection,
                    job_type=OPPORTUNITY_ANALYSIS_JOB_TYPE,
                    idempotency_key=f"{PREFILTER_SCHEMA_VERSION}:{ingested.message.id}",
                    correlation_id=TRACE_ID,
                )
                await connection.execute(
                    sa.insert(message_prefilter_results).values(
                        id=result_id,
                        raw_message_id=ingested.message.id,
                        parent_raw_message_id=None,
                        analysis_job_id=analysis_job_id,
                        schema_version=PREFILTER_SCHEMA_VERSION,
                        decision="passed",
                        reason_codes=[],
                    )
                )
            return ingested.message.id, result_id, analysis_job_id
        finally:
            await database.close()

    @staticmethod
    async def _load_result(database_url, raw_message_id):
        database = Database(database_url)
        try:
            async with database.connect() as connection:
                return await MessagePrefilterRepository().get_for_raw(
                    connection,
                    raw_message_id=raw_message_id,
                    schema_version=PREFILTER_SCHEMA_VERSION,
                )
        finally:
            await database.close()


if __name__ == "__main__":
    unittest.main()
