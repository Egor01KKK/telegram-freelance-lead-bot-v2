import asyncio
import io
import unittest
from datetime import timedelta
from unittest.mock import AsyncMock

from freelancer_bot.config import RuntimeConfig
from freelancer_bot.metrics import InMemoryMetrics, MetricNames
from freelancer_bot.observability import (
    Redactor,
    configure_structured_logger,
    current_trace_id,
)
from freelancer_bot.persistence.database import Database
from freelancer_bot.persistence.jobs import DurableJobRepository
from freelancer_bot.worker import DurableWorker, WorkerOptions, WorkerState
from postgres_support import TEST_DATABASE_URL, migrate_to_head, temporary_database


class WorkerOptionsTest(unittest.TestCase):
    def test_worker_options_use_typed_runtime_configuration(self):
        config = RuntimeConfig(
            worker_poll_interval_seconds=0.2,
            worker_lease_seconds=12,
            worker_heartbeat_seconds=3,
            worker_retry_delay_seconds=7,
            worker_shutdown_timeout_seconds=9,
        )

        options = WorkerOptions.from_config(config)

        self.assertEqual(options.poll_interval, 0.2)
        self.assertEqual(options.lease_duration, 12)
        self.assertEqual(options.heartbeat_interval, 3)
        self.assertEqual(options.retry_delay, 7)
        self.assertEqual(options.shutdown_timeout, 9)

    def test_heartbeat_must_be_shorter_than_lease(self):
        with self.assertRaisesRegex(ValueError, "Heartbeat interval"):
            WorkerOptions(lease_duration=1, heartbeat_interval=1)


@unittest.skipUnless(TEST_DATABASE_URL, "TEST_DATABASE_URL is not configured")
class DurableWorkerTest(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.database_context = temporary_database()
        self.database_url = self.database_context.__enter__()
        migrate_to_head(self.database_url)
        self.database = Database(self.database_url, pool_size=6, max_overflow=8)
        self.metrics = InMemoryMetrics()
        self.repository = DurableJobRepository(self.metrics)
        self.log_output = io.StringIO()
        self.logger = configure_structured_logger(
            f"test.worker.{id(self)}",
            redactor=Redactor(),
            stream=self.log_output,
        )
        self.options = WorkerOptions(
            poll_interval=0.01,
            lease_duration=0.5,
            heartbeat_interval=0.05,
            retry_delay=0,
            shutdown_timeout=0.1,
        )

    async def asyncTearDown(self):
        await self.database.close()
        self.database_context.__exit__(None, None, None)

    async def _enqueue(self, key: str, *, max_attempts: int = 3):
        async with self.database.transaction() as connection:
            return await self.repository.enqueue(
                connection,
                job_type="fixture",
                idempotency_key=key,
                max_attempts=max_attempts,
            )

    def _worker(self, handler, **kwargs):
        return DurableWorker(
            self.database,
            repository=self.repository,
            worker_id=kwargs.pop("worker_id", "worker-test"),
            handlers={"fixture": handler},
            logger=self.logger,
            metrics=self.metrics,
            options=kwargs.pop("options", self.options),
            close_database_on_exit=kwargs.pop("close_database_on_exit", False),
            **kwargs,
        )

    async def test_successful_job_completes_and_propagates_correlation_id(self):
        job_id = await self._enqueue("success")
        observed_trace_ids = []
        worker = None

        async def handler(claim):
            observed_trace_ids.append(current_trace_id())
            worker.request_stop()

        worker = self._worker(handler)
        await asyncio.wait_for(worker.run(install_signal_handlers=False), timeout=1)

        async with self.database.connect() as connection:
            record = await self.repository.get(connection, job_id)
        self.assertEqual(record["state"], "completed")
        self.assertEqual(observed_trace_ids, [str(record["correlation_id"])])
        self.assertEqual(worker.health.state, WorkerState.STOPPED)
        self.assertIn(str(record["correlation_id"]), self.log_output.getvalue())
        self.assertEqual(
            self.metrics.counter(MetricNames.JOBS_COMPLETED, tags={"job_type": "fixture"}),
            1,
        )

    async def test_worker_claims_only_job_types_with_registered_handlers(self):
        async with self.database.transaction() as connection:
            future_job_id = await self.repository.enqueue(
                connection,
                job_type="future-stage",
                idempotency_key="future-first",
            )
        fixture_job_id = await self._enqueue("supported-second")
        worker = None

        async def handler(claim):
            worker.request_stop()

        worker = self._worker(handler)
        await asyncio.wait_for(worker.run(install_signal_handlers=False), timeout=1)

        async with self.database.connect() as connection:
            future = await self.repository.get(connection, future_job_id)
            fixture = await self.repository.get(connection, fixture_job_id)
        self.assertEqual(future["state"], "queued")
        self.assertEqual(future["attempt_count"], 0)
        self.assertEqual(fixture["state"], "completed")
        self.assertEqual(
            self.metrics.counter(MetricNames.JOBS_CREATED, tags={"job_type": "fixture"}),
            1,
        )
        self.assertEqual(
            self.metrics.counter(MetricNames.JOBS_CLAIMED, tags={"job_type": "fixture"}),
            1,
        )
        self.assertEqual(
            len(
                self.metrics.observations(
                    MetricNames.JOB_PROCESSING_SECONDS,
                    tags={"job_type": "fixture"},
                )
            ),
            1,
        )

    async def test_idle_worker_stops_without_hanging(self):
        async def handler(claim):
            raise AssertionError("No job should have been claimed")

        worker = self._worker(handler)
        task = asyncio.create_task(worker.run())
        await asyncio.sleep(0.03)
        worker.request_stop()
        await asyncio.wait_for(task, timeout=0.5)
        self.assertEqual(worker.health.state, WorkerState.STOPPED)

    async def test_shutdown_during_active_job_drains_within_timeout(self):
        job_id = await self._enqueue("graceful-active")
        started = asyncio.Event()
        finish = asyncio.Event()

        async def handler(claim):
            started.set()
            await finish.wait()

        worker = self._worker(handler)
        task = asyncio.create_task(worker.run(install_signal_handlers=False))
        await asyncio.wait_for(started.wait(), timeout=0.5)
        worker.request_stop()
        await asyncio.sleep(0)
        finish.set()
        await asyncio.wait_for(task, timeout=0.5)

        async with self.database.connect() as connection:
            record = await self.repository.get(connection, job_id)
        self.assertEqual(record["state"], "completed")

    async def test_heartbeat_keeps_long_running_claim_owned(self):
        job_id = await self._enqueue("heartbeat")
        started = asyncio.Event()
        finish = asyncio.Event()

        async def handler(claim):
            started.set()
            await finish.wait()

        options = WorkerOptions(
            poll_interval=0.01,
            lease_duration=0.15,
            heartbeat_interval=0.03,
            retry_delay=0,
            shutdown_timeout=0.2,
        )
        worker = self._worker(handler, options=options)
        task = asyncio.create_task(worker.run(install_signal_handlers=False))
        await asyncio.wait_for(started.wait(), timeout=0.5)
        await asyncio.sleep(0.25)

        async with self.database.transaction() as connection:
            stolen = await self.repository.claim_next(
                connection,
                worker_id="worker-b",
                lease_duration=timedelta(seconds=1),
            )
        self.assertIsNone(stolen)

        worker.request_stop()
        finish.set()
        await asyncio.wait_for(task, timeout=0.5)
        async with self.database.connect() as connection:
            record = await self.repository.get(connection, job_id)
        self.assertEqual(record["state"], "completed")

    async def test_shutdown_timeout_requeues_retryable_job(self):
        job_id = await self._enqueue("shutdown-timeout", max_attempts=2)
        started = asyncio.Event()

        async def handler(claim):
            started.set()
            await asyncio.Event().wait()

        worker = self._worker(handler)
        task = asyncio.create_task(worker.run(install_signal_handlers=False))
        await asyncio.wait_for(started.wait(), timeout=0.5)
        worker.request_stop()
        await asyncio.wait_for(task, timeout=0.5)

        async with self.database.connect() as connection:
            record = await self.repository.get(connection, job_id)
        self.assertEqual(record["state"], "queued")
        self.assertEqual(record["failure_code"], "ShutdownTimeout")
        self.assertIsNone(record["lease_owner"])
        self.assertEqual(
            self.metrics.counter(MetricNames.JOBS_RETRIED, tags={"job_type": "fixture"}),
            1,
        )
        pending = [
            task
            for task in asyncio.all_tasks()
            if task is not asyncio.current_task() and not task.done()
        ]
        self.assertEqual(pending, [])

    async def test_retryable_handler_failure_then_success_completes_once(self):
        job_id = await self._enqueue("retry-success", max_attempts=2)
        attempts = 0
        worker = None

        async def handler(claim):
            nonlocal attempts
            attempts += 1
            if attempts == 1:
                raise ConnectionError("transient fixture")
            worker.request_stop()

        worker = self._worker(handler)
        await asyncio.wait_for(worker.run(install_signal_handlers=False), timeout=1)

        async with self.database.connect() as connection:
            record = await self.repository.get(connection, job_id)
        self.assertEqual(attempts, 2)
        self.assertEqual(record["state"], "completed")
        self.assertEqual(record["attempt_count"], 2)
        self.assertEqual(
            self.metrics.counter(MetricNames.JOBS_RETRIED, tags={"job_type": "fixture"}),
            1,
        )
        self.assertEqual(
            self.metrics.counter(MetricNames.JOBS_COMPLETED, tags={"job_type": "fixture"}),
            1,
        )

    async def test_non_retryable_handler_failure_is_terminal_on_first_attempt(self):
        job_id = await self._enqueue("non-retryable", max_attempts=3)
        worker = None

        async def handler(claim):
            worker.request_stop()
            error = RuntimeError("invalid provider configuration")
            error.retryable = False
            raise error

        worker = self._worker(handler)
        await asyncio.wait_for(worker.run(install_signal_handlers=False), timeout=1)

        async with self.database.connect() as connection:
            record = await self.repository.get(connection, job_id)
        self.assertEqual(record["state"], "failed")
        self.assertEqual(record["attempt_count"], 1)
        self.assertEqual(record["failure_code"], "RuntimeError")
        self.assertEqual(
            self.metrics.counter(MetricNames.JOBS_RETRIED, tags={"job_type": "fixture"}),
            0,
        )

    async def test_external_cancellation_releases_active_claim(self):
        job_id = await self._enqueue("cancel-active", max_attempts=2)
        started = asyncio.Event()

        async def handler(claim):
            started.set()
            await asyncio.Event().wait()

        worker = self._worker(handler)
        task = asyncio.create_task(worker.run(install_signal_handlers=False))
        await asyncio.wait_for(started.wait(), timeout=0.5)
        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task

        async with self.database.connect() as connection:
            record = await self.repository.get(connection, job_id)
        self.assertEqual(record["state"], "queued")
        self.assertIsNone(record["lease_owner"])

    async def test_worker_closes_database_resources_on_exit_by_default(self):
        async def handler(claim):
            raise AssertionError("No job should have been claimed")

        original_close = self.database.close
        self.database.close = AsyncMock(wraps=original_close)
        worker = DurableWorker(
            self.database,
            repository=self.repository,
            worker_id="worker-close",
            handlers={"fixture": handler},
            logger=self.logger,
            metrics=self.metrics,
            options=self.options,
        )
        worker.request_stop()
        await worker.run(install_signal_handlers=False)
        self.database.close.assert_awaited_once()


if __name__ == "__main__":
    unittest.main()
