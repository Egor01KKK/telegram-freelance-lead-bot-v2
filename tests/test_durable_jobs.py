import asyncio
import unittest
from datetime import timedelta

import sqlalchemy as sa

from freelancer_bot.metrics import InMemoryMetrics, MetricNames
from freelancer_bot.persistence.database import Database
from freelancer_bot.persistence.jobs import DurableJobRepository
from freelancer_bot.persistence.schema import durable_jobs
from postgres_support import TEST_DATABASE_URL, migrate_to_head, temporary_database


@unittest.skipUnless(TEST_DATABASE_URL, "TEST_DATABASE_URL is not configured")
class DurableJobRepositoryTest(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.database_context = temporary_database()
        self.database_url = self.database_context.__enter__()
        migrate_to_head(self.database_url)
        self.database = Database(self.database_url, pool_size=8, max_overflow=16)
        self.metrics = InMemoryMetrics()
        self.repository = DurableJobRepository(self.metrics)

    async def asyncTearDown(self):
        await self.database.close()
        self.database_context.__exit__(None, None, None)

    async def test_idempotent_enqueue_and_transaction_rollback(self):
        async with self.database.transaction() as connection:
            first = await self.repository.enqueue(
                connection,
                job_type="fixture",
                idempotency_key="same-input",
            )
            second = await self.repository.enqueue(
                connection,
                job_type="fixture",
                idempotency_key="same-input",
            )
        self.assertEqual(first, second)

        with self.assertRaisesRegex(RuntimeError, "rollback"):
            async with self.database.transaction() as connection:
                await self.repository.enqueue(
                    connection,
                    job_type="fixture",
                    idempotency_key="rolled-back",
                )
                raise RuntimeError("force rollback")

        async with self.database.connect() as connection:
            count = await connection.scalar(sa.select(sa.func.count()).select_from(durable_jobs))
        self.assertEqual(count, 1)

    async def test_concurrent_workers_claim_different_jobs(self):
        async with self.database.transaction() as connection:
            identifiers = [
                await self.repository.enqueue(
                    connection,
                    job_type="fixture",
                    idempotency_key=f"job-{index}",
                )
                for index in range(6)
            ]

        async def claim(index: int):
            async with self.database.transaction() as connection:
                return await self.repository.claim_next(
                    connection,
                    worker_id=f"worker-{index}",
                    lease_duration=timedelta(seconds=30),
                )

        claims = await asyncio.gather(*(claim(index) for index in range(6)))
        claimed_ids = {item.id for item in claims if item is not None}
        self.assertEqual(claimed_ids, set(identifiers))

    async def test_same_job_cannot_be_claimed_twice_concurrently(self):
        async with self.database.transaction() as connection:
            await self.repository.enqueue(
                connection,
                job_type="fixture",
                idempotency_key="one-job",
            )

        async def claim(index: int):
            async with self.database.transaction() as connection:
                return await self.repository.claim_next(
                    connection,
                    worker_id=f"worker-{index}",
                    lease_duration=timedelta(seconds=30),
                )

        claims = await asyncio.gather(*(claim(index) for index in range(12)))
        self.assertEqual(sum(item is not None for item in claims), 1)

    async def test_expired_lease_is_reclaimed_and_only_new_owner_completes(self):
        async with self.database.transaction() as connection:
            job_id = await self.repository.enqueue(
                connection,
                job_type="fixture",
                idempotency_key="crash-recovery",
            )
            first = await self.repository.claim_next(
                connection,
                worker_id="worker-a",
                lease_duration=timedelta(seconds=30),
            )
        self.assertIsNotNone(first)

        async with self.database.transaction() as connection:
            await connection.execute(
                sa.update(durable_jobs)
                .where(durable_jobs.c.id == job_id)
                .values(lease_expires_at=sa.func.now() - sa.text("INTERVAL '1 second'"))
            )
            second = await self.repository.claim_next(
                connection,
                worker_id="worker-b",
                lease_duration=timedelta(seconds=30),
            )
        self.assertIsNotNone(second)
        self.assertTrue(second.reclaimed)
        self.assertEqual(second.attempt_count, 2)

        async with self.database.transaction() as connection:
            self.assertFalse(await self.repository.complete(connection, first))
            self.assertTrue(await self.repository.complete(connection, second))
        async with self.database.connect() as connection:
            record = await self.repository.get(connection, job_id)
        self.assertEqual(record["state"], "completed")
        self.assertEqual(record["attempt_count"], 2)
        self.assertEqual(
            self.metrics.counter(
                MetricNames.JOBS_LEASE_RECLAIMED,
                tags={"job_type": "fixture"},
            ),
            1,
        )

    async def test_retry_then_terminal_failure_is_bounded_and_sanitized(self):
        async with self.database.transaction() as connection:
            job_id = await self.repository.enqueue(
                connection,
                job_type="fixture",
                idempotency_key="bounded-retry",
                max_attempts=2,
            )
            first = await self.repository.claim_next(
                connection,
                worker_id="worker-a",
                lease_duration=timedelta(seconds=30),
            )
            state = await self.repository.fail(
                connection,
                first,
                failure_code="credential leaked in raw error",
                retry_delay=timedelta(0),
            )
        self.assertEqual(state, "queued")
        async with self.database.connect() as connection:
            retry_record = await self.repository.get(connection, job_id)
        self.assertEqual(retry_record["failure_code"], "ProcessingError")

        async with self.database.transaction() as connection:
            second = await self.repository.claim_next(
                connection,
                worker_id="worker-b",
                lease_duration=timedelta(seconds=30),
            )
            state = await self.repository.fail(
                connection,
                second,
                failure_code="TransientError",
                retry_delay=timedelta(0),
            )
        self.assertEqual(state, "failed")

        async with self.database.connect() as connection:
            record = await self.repository.get(connection, job_id)
        self.assertEqual(record["attempt_count"], 2)
        self.assertEqual(record["failure_code"], "TransientError")
        self.assertIsNotNone(record["failed_at"])
        self.assertEqual(
            self.metrics.counter(MetricNames.JOBS_RETRIED, tags={"job_type": "fixture"}),
            1,
        )
        self.assertEqual(
            self.metrics.counter(MetricNames.JOBS_FAILED, tags={"job_type": "fixture"}),
            1,
        )

    async def test_last_attempt_expired_lease_becomes_terminal(self):
        async with self.database.transaction() as connection:
            job_id = await self.repository.enqueue(
                connection,
                job_type="fixture",
                idempotency_key="last-lease",
                max_attempts=1,
            )
            await self.repository.claim_next(
                connection,
                worker_id="worker-a",
                lease_duration=timedelta(seconds=30),
            )
            await connection.execute(
                sa.update(durable_jobs)
                .where(durable_jobs.c.id == job_id)
                .values(lease_expires_at=sa.func.now() - sa.text("INTERVAL '1 second'"))
            )

        async with self.database.transaction() as connection:
            claim = await self.repository.claim_next(
                connection,
                worker_id="worker-b",
                lease_duration=timedelta(seconds=30),
            )
        self.assertIsNone(claim)
        async with self.database.connect() as connection:
            record = await self.repository.get(connection, job_id)
        self.assertEqual(record["state"], "failed")
        self.assertEqual(record["failure_code"], "LeaseExpired")


if __name__ == "__main__":
    unittest.main()
