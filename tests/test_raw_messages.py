from __future__ import annotations

import asyncio
import unittest
from datetime import datetime, timedelta, timezone
from uuid import UUID

import sqlalchemy as sa

from freelancer_bot.persistence.collector_accounts import (
    CollectorAccessStatus,
    CollectorAccountRepository,
)
from freelancer_bot.persistence.database import Database
from freelancer_bot.persistence.jobs import DurableJobRepository
from freelancer_bot.persistence.raw_messages import (
    IneligibleRawMessageSource,
    RAW_MESSAGE_JOB_TYPE,
    RAW_MESSAGE_SCHEMA_VERSION,
    RawMessageIngestor,
    RawMessageInput,
    RawMessageOrigin,
    RawMessageRepository,
)
from freelancer_bot.persistence.schema import durable_jobs, raw_messages
from freelancer_bot.persistence.source_repository import SourceRepository, SourceStatus
from postgres_support import TEST_DATABASE_URL, migrate_to_head, temporary_database


MESSAGE_DATE = datetime(2026, 8, 9, 10, 30, tzinfo=timezone.utc)
OBSERVED_AT = datetime(2026, 8, 9, 10, 31, tzinfo=timezone.utc)
TRACE_ID = UUID("44444444-4444-4444-4444-444444444444")


@unittest.skipUnless(TEST_DATABASE_URL, "TEST_DATABASE_URL is not configured")
class RawMessageIngestorTest(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.database_context = temporary_database()
        self.database_url = self.database_context.__enter__()
        migrate_to_head(self.database_url)
        self.database = Database(self.database_url, pool_size=8, max_overflow=16)
        self.sources = SourceRepository()
        self.accounts = CollectorAccountRepository()
        self.messages = RawMessageRepository()
        self.jobs = DurableJobRepository()

    async def asyncTearDown(self):
        await self.database.close()
        self.database_context.__exit__(None, None, None)

    async def test_persists_complete_raw_context_and_queues_one_durable_job(self):
        source, account = await self._eligible_source("complete")

        result = await RawMessageIngestor(self.database).ingest(
            self._message(source.id, account.id)
        )

        self.assertTrue(result.created)
        self.assertEqual(result.message.schema_version, RAW_MESSAGE_SCHEMA_VERSION)
        self.assertEqual(result.message.platform, "telegram")
        self.assertEqual(result.message.external_source_id, "username:g3_complete")
        self.assertEqual(result.message.external_message_id, 501)
        self.assertEqual(result.message.message_date, MESSAGE_DATE)
        self.assertEqual(result.message.observed_at, OBSERVED_AT)
        self.assertEqual(result.message.message_url, "https://t.me/g3_complete/501")
        self.assertEqual(result.message.content, "raw payload for durable recovery")
        self.assertEqual(
            result.message.transport_metadata,
            {"chat_id": -100123, "post": True, "media_type": "MessageMediaPhoto"},
        )
        self.assertEqual(result.message.ingestion_origin, RawMessageOrigin.LIVE)
        self.assertEqual(result.message.correlation_id, TRACE_ID)

        async with self.database.connect() as connection:
            job = await self.jobs.get(connection, result.message.processing_job_id)
            raw_count = await connection.scalar(
                sa.select(sa.func.count()).select_from(raw_messages)
            )
        self.assertEqual(raw_count, 1)
        self.assertEqual(job["job_type"], RAW_MESSAGE_JOB_TYPE)
        self.assertEqual(job["state"], "queued")
        self.assertEqual(job["correlation_id"], TRACE_ID)

    async def test_live_and_catch_up_duplicates_converge_on_one_raw_row_and_job(self):
        source, account = await self._eligible_source("duplicate")
        ingestor = RawMessageIngestor(self.database)
        live = self._message(source.id, account.id, suffix="duplicate")
        catch_up = RawMessageInput(
            **{
                **live.__dict__,
                "content": "later duplicate must not overwrite raw payload",
                "ingestion_origin": RawMessageOrigin.CATCH_UP,
            }
        )

        first, second = await asyncio.gather(
            ingestor.ingest(live),
            ingestor.ingest(catch_up),
        )

        self.assertEqual(first.message.id, second.message.id)
        self.assertEqual(first.message.processing_job_id, second.message.processing_job_id)
        self.assertEqual(sum((first.created, second.created)), 1)
        self.assertIn(
            first.message.content,
            {live.content, catch_up.content},
        )
        self.assertEqual(second.message.content, first.message.content)
        async with self.database.connect() as connection:
            raw_count = await connection.scalar(
                sa.select(sa.func.count()).select_from(raw_messages)
            )
            job_count = await connection.scalar(
                sa.select(sa.func.count())
                .select_from(durable_jobs)
                .where(durable_jobs.c.job_type == RAW_MESSAGE_JOB_TYPE)
            )
        self.assertEqual((raw_count, job_count), (1, 1))

    async def test_restart_can_claim_job_and_reconstruct_payload_without_telegram(self):
        source, account = await self._eligible_source("restart")
        created = await RawMessageIngestor(self.database).ingest(
            self._message(source.id, account.id, suffix="restart")
        )
        await self.database.close()

        restarted = Database(self.database_url)
        try:
            async with restarted.transaction() as connection:
                claim = await self.jobs.claim_next(
                    connection,
                    worker_id="restart-worker",
                    lease_duration=timedelta(seconds=30),
                )
                restored = await self.messages.get_for_job(connection, claim.id)
            self.assertEqual(claim.id, created.message.processing_job_id)
            self.assertEqual(restored.id, created.message.id)
            self.assertEqual(restored.content, "raw payload for durable recovery")
            self.assertEqual(restored.message_url, "https://t.me/g3_restart/501")
            self.assertEqual(restored.external_source_id, "username:g3_restart")
        finally:
            await restarted.close()

    async def test_job_failure_rolls_back_both_job_and_raw_message(self):
        source, account = await self._eligible_source("rollback")

        class FailingAfterEnqueue(DurableJobRepository):
            async def enqueue(self, connection, **kwargs):
                await super().enqueue(connection, **kwargs)
                raise RuntimeError("forced atomic rollback")

        ingestor = RawMessageIngestor(self.database, jobs=FailingAfterEnqueue())
        with self.assertRaisesRegex(RuntimeError, "forced atomic rollback"):
            await ingestor.ingest(
                self._message(source.id, account.id, suffix="rollback")
            )

        async with self.database.connect() as connection:
            raw_count = await connection.scalar(
                sa.select(sa.func.count()).select_from(raw_messages)
            )
            job_count = await connection.scalar(
                sa.select(sa.func.count()).select_from(durable_jobs)
            )
        self.assertEqual((raw_count, job_count), (0, 0))

    async def test_lifecycle_and_private_access_are_rechecked_at_ingest_time(self):
        account = await self._account()
        candidate = await self._source("candidate", status=SourceStatus.CANDIDATE)
        paused = await self._source("paused", status=SourceStatus.PAUSED)
        rejected = await self._source("rejected", status=SourceStatus.REJECTED)
        private_denied = await self._source("private-denied", access_type="private")
        private_permitted = await self._source("private-permitted", access_type="private")
        async with self.database.transaction() as connection:
            await self.accounts.record_source_access(
                connection,
                source_id=private_denied.id,
                collector_account_id=account.id,
                access_status=CollectorAccessStatus.INACCESSIBLE,
                checked_at=OBSERVED_AT,
                checked_by="operator:g3-t02",
            )
            await self.accounts.record_source_access(
                connection,
                source_id=private_permitted.id,
                collector_account_id=account.id,
                access_status=CollectorAccessStatus.PERMITTED,
                checked_at=OBSERVED_AT,
                checked_by="operator:g3-t02",
            )

        ingestor = RawMessageIngestor(self.database)
        for source in (candidate, paused, rejected, private_denied):
            with self.subTest(source=source.external_id):
                with self.assertRaises(IneligibleRawMessageSource):
                    await ingestor.ingest(
                        self._message(
                            source.id,
                            account.id,
                            suffix=source.external_id.removeprefix("username:g3_"),
                        )
                    )

        accepted = await ingestor.ingest(
            self._message(
                private_permitted.id,
                account.id,
                suffix="private-permitted",
            )
        )
        self.assertTrue(accepted.created)
        async with self.database.connect() as connection:
            counts = (
                await connection.scalar(sa.select(sa.func.count()).select_from(raw_messages)),
                await connection.scalar(sa.select(sa.func.count()).select_from(durable_jobs)),
            )
        self.assertEqual(counts, (1, 1))

    async def _eligible_source(self, suffix):
        account = await self._account()
        source = await self._source(suffix)
        return source, account

    async def _account(self):
        async with self.database.transaction() as connection:
            return await self.accounts.ensure(
                connection,
                platform="telegram",
                external_account_id="80001",
                display_name="G3-T02 collector",
            )

    async def _source(
        self,
        suffix,
        *,
        status=SourceStatus.APPROVED,
        access_type="public",
    ):
        async with self.database.transaction() as connection:
            source = await self.sources.create_candidate(
                connection,
                platform="telegram",
                external_id=f"username:g3_{suffix}",
                access_type=access_type,
                display_name=f"G3-T02 {suffix}",
                handle=f"@g3_{suffix}",
                canonical_url=f"https://t.me/g3_{suffix}",
                provider="g3_t02_fixture",
                lineage_key=f"g3-t02:{suffix}",
            )
            if status in {SourceStatus.APPROVED, SourceStatus.PAUSED}:
                source = await self.sources.transition(
                    connection,
                    source.id,
                    SourceStatus.APPROVED,
                    reason="G3-T02 fixture approved",
                )
            if status is SourceStatus.PAUSED:
                source = await self.sources.transition(
                    connection,
                    source.id,
                    SourceStatus.PAUSED,
                    reason="G3-T02 fixture paused",
                )
            if status is SourceStatus.REJECTED:
                source = await self.sources.transition(
                    connection,
                    source.id,
                    SourceStatus.REJECTED,
                    reason="G3-T02 fixture rejected",
                )
            return source

    @staticmethod
    def _message(source_id, account_id, *, suffix="complete"):
        return RawMessageInput(
            source_id=source_id,
            collector_account_id=account_id,
            external_message_id=501,
            message_date=MESSAGE_DATE,
            observed_at=OBSERVED_AT,
            message_url="https://t.me/g3_" + suffix + "/501",
            content="raw payload for durable recovery",
            transport_metadata={
                "chat_id": -100123,
                "post": True,
                "media_type": "MessageMediaPhoto",
            },
            ingestion_origin=RawMessageOrigin.LIVE,
            correlation_id=TRACE_ID,
        )


if __name__ == "__main__":
    unittest.main()
