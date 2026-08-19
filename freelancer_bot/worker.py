from __future__ import annotations

import asyncio
import logging
import signal
from collections.abc import Awaitable, Callable, Collection
from dataclasses import dataclass, replace
from datetime import datetime, timedelta, timezone
from enum import Enum
from time import monotonic
from uuid import UUID

from .config import RuntimeConfig
from .metrics import MetricNames, MetricsSink, NoOpMetrics
from .observability import log_event, trace_context
from .persistence import Database, DurableJobRepository, JobClaim


JobHandler = Callable[[JobClaim], Awaitable[None]]


class WorkerState(str, Enum):
    STARTING = "starting"
    IDLE = "idle"
    PROCESSING = "processing"
    STOPPING = "stopping"
    STOPPED = "stopped"


@dataclass(frozen=True)
class WorkerOptions:
    poll_interval: float = 1.0
    lease_duration: float = 30.0
    heartbeat_interval: float = 10.0
    retry_delay: float = 5.0
    shutdown_timeout: float = 20.0

    @classmethod
    def from_config(cls, config: RuntimeConfig) -> WorkerOptions:
        return cls(
            poll_interval=config.worker_poll_interval_seconds,
            lease_duration=config.worker_lease_seconds,
            heartbeat_interval=config.worker_heartbeat_seconds,
            retry_delay=config.worker_retry_delay_seconds,
            shutdown_timeout=config.worker_shutdown_timeout_seconds,
        )

    def __post_init__(self) -> None:
        positive = (
            self.poll_interval,
            self.lease_duration,
            self.heartbeat_interval,
            self.shutdown_timeout,
        )
        if any(value <= 0 for value in positive) or self.retry_delay < 0:
            raise ValueError("Worker timing values must be positive; retry delay may be zero")
        if self.heartbeat_interval >= self.lease_duration:
            raise ValueError("Heartbeat interval must be shorter than the lease duration")


@dataclass(frozen=True)
class WorkerHealth:
    state: WorkerState
    worker_id: str
    current_job_id: str | None = None
    last_poll_at: datetime | None = None


class DurableWorker:
    def __init__(
        self,
        database: Database,
        *,
        repository: DurableJobRepository,
        worker_id: str,
        handlers: dict[str, JobHandler],
        logger: logging.Logger,
        metrics: MetricsSink | None = None,
        options: WorkerOptions | None = None,
        close_database_on_exit: bool = True,
        job_ids: Collection[UUID] | None = None,
    ) -> None:
        if not worker_id.strip():
            raise ValueError("worker_id must not be blank")
        self._database = database
        self._repository = repository
        self._worker_id = worker_id
        self._handlers = dict(handlers)
        self._logger = logger
        self._metrics = metrics or NoOpMetrics()
        self._options = options or WorkerOptions()
        self._close_database_on_exit = close_database_on_exit
        self._job_ids = None if job_ids is None else tuple(job_ids)
        self._stop_event = asyncio.Event()
        self._health = WorkerHealth(WorkerState.STARTING, worker_id)
        self._active_claim: JobClaim | None = None
        self._active_handler: asyncio.Task[None] | None = None

    @property
    def health(self) -> WorkerHealth:
        return self._health

    def request_stop(self) -> None:
        self._stop_event.set()

    async def run(self, *, install_signal_handlers: bool = True) -> None:
        installed_signals = self._install_signal_handlers() if install_signal_handlers else []
        log_event(self._logger, logging.INFO, "worker.started", worker_id=self._worker_id)
        try:
            while not self._stop_event.is_set():
                self._health = replace(
                    self._health,
                    state=WorkerState.IDLE,
                    current_job_id=None,
                    last_poll_at=datetime.now(timezone.utc),
                )
                claim = await self._claim_next()
                if claim is None:
                    await self._wait_for_poll_or_stop()
                    continue
                await self._execute_claim(claim)
        except asyncio.CancelledError:
            self._stop_event.set()
            await self._cancel_active_claim()
            raise
        finally:
            self._health = replace(self._health, state=WorkerState.STOPPING)
            for sig in installed_signals:
                asyncio.get_running_loop().remove_signal_handler(sig)
            if self._close_database_on_exit:
                await self._database.close()
            self._health = replace(
                self._health,
                state=WorkerState.STOPPED,
                current_job_id=None,
            )
            log_event(self._logger, logging.INFO, "worker.stopped", worker_id=self._worker_id)

    async def _claim_next(self) -> JobClaim | None:
        async with self._database.transaction() as connection:
            return await self._repository.claim_next(
                connection,
                worker_id=self._worker_id,
                lease_duration=timedelta(seconds=self._options.lease_duration),
                job_types=self._handlers.keys(),
                job_ids=self._job_ids,
            )

    async def _execute_claim(self, claim: JobClaim) -> None:
        self._active_claim = claim
        self._health = replace(
            self._health,
            state=WorkerState.PROCESSING,
            current_job_id=str(claim.id),
        )
        started = monotonic()
        with trace_context(claim.correlation_id):
            log_event(
                self._logger,
                logging.INFO,
                "job.claimed",
                job_id=claim.id,
                job_type=claim.job_type,
                attempt=claim.attempt_count,
                worker_id=self._worker_id,
                state_transition="queued_or_expired->running",
                reclaimed=claim.reclaimed,
            )
            handler = self._handlers.get(claim.job_type)
            if handler is None:
                await self._record_failure(claim, "UnknownJobType")
                self._active_claim = None
                return

            self._active_handler = asyncio.create_task(handler(claim))
            heartbeat = asyncio.create_task(self._heartbeat(claim))
            stop_waiter = asyncio.create_task(self._stop_event.wait())
            try:
                done, _ = await asyncio.wait(
                    {self._active_handler, heartbeat, stop_waiter},
                    return_when=asyncio.FIRST_COMPLETED,
                )
                if self._active_handler in done:
                    await self._finish_handler(claim, self._active_handler)
                elif heartbeat in done:
                    self._active_handler.cancel()
                    await _consume_cancelled(self._active_handler)
                    log_event(
                        self._logger,
                        logging.WARNING,
                        "job.claim_lost",
                        job_id=claim.id,
                        job_type=claim.job_type,
                        attempt=claim.attempt_count,
                        worker_id=self._worker_id,
                    )
                else:
                    self._health = replace(self._health, state=WorkerState.STOPPING)
                    await self._drain_or_release(claim, self._active_handler)
            except asyncio.CancelledError:
                self._active_handler.cancel()
                await _consume_cancelled(self._active_handler)
                await self._release_claim(claim)
                raise
            finally:
                heartbeat.cancel()
                stop_waiter.cancel()
                await _consume_cancelled(heartbeat)
                await _consume_cancelled(stop_waiter)
                self._metrics.observe(
                    MetricNames.JOB_PROCESSING_SECONDS,
                    monotonic() - started,
                    tags={"job_type": claim.job_type},
                )
                self._active_handler = None
                self._active_claim = None

    async def _finish_handler(self, claim: JobClaim, task: asyncio.Task[None]) -> None:
        try:
            await task
        except asyncio.CancelledError:
            await self._release_claim(claim)
            raise
        except Exception as exc:
            await self._record_failure(
                claim,
                type(exc).__name__,
                error=exc,
                retryable=bool(getattr(exc, "retryable", True)),
            )
            return

        async with self._database.transaction() as connection:
            completed = await self._repository.complete(connection, claim)
        event = "job.completed" if completed else "job.claim_lost"
        log_event(
            self._logger,
            logging.INFO if completed else logging.WARNING,
            event,
            job_id=claim.id,
            job_type=claim.job_type,
            attempt=claim.attempt_count,
            worker_id=self._worker_id,
            state_transition="running->completed" if completed else "running->unchanged",
        )

    async def _record_failure(
        self,
        claim: JobClaim,
        failure_code: str,
        *,
        error: Exception | None = None,
        retryable: bool = True,
    ) -> None:
        async with self._database.transaction() as connection:
            state = await self._repository.fail(
                connection,
                claim,
                failure_code=failure_code,
                retry_delay=timedelta(seconds=self._options.retry_delay),
                retryable=retryable,
            )
        log_event(
            self._logger,
            logging.WARNING,
            "job.failed" if state == "failed" else "job.retry_scheduled",
            job_id=claim.id,
            job_type=claim.job_type,
            attempt=claim.attempt_count,
            worker_id=self._worker_id,
            state_transition=f"running->{state or 'unchanged'}",
            failure_code=failure_code,
            retryable=retryable,
            error=error,
        )

    async def _drain_or_release(self, claim: JobClaim, task: asyncio.Task[None]) -> None:
        try:
            await asyncio.wait_for(
                asyncio.shield(task),
                timeout=self._options.shutdown_timeout,
            )
        except TimeoutError:
            task.cancel()
            await _consume_cancelled(task)
            await self._release_claim(claim)
            log_event(
                self._logger,
                logging.WARNING,
                "job.shutdown_timeout",
                job_id=claim.id,
                job_type=claim.job_type,
                attempt=claim.attempt_count,
                worker_id=self._worker_id,
                state_transition="running->queued_or_failed",
            )
        else:
            await self._finish_handler(claim, task)

    async def _release_claim(self, claim: JobClaim) -> None:
        async with self._database.transaction() as connection:
            await self._repository.release_for_shutdown(connection, claim)

    async def _cancel_active_claim(self) -> None:
        if self._active_handler is not None:
            self._active_handler.cancel()
            await _consume_cancelled(self._active_handler)
        if self._active_claim is not None:
            await self._release_claim(self._active_claim)

    async def _heartbeat(self, claim: JobClaim) -> bool:
        while True:
            await asyncio.sleep(self._options.heartbeat_interval)
            async with self._database.transaction() as connection:
                renewed = await self._repository.renew_lease(
                    connection,
                    claim,
                    lease_duration=timedelta(seconds=self._options.lease_duration),
                )
            if not renewed:
                return False

    async def _wait_for_poll_or_stop(self) -> None:
        try:
            await asyncio.wait_for(
                self._stop_event.wait(),
                timeout=self._options.poll_interval,
            )
        except TimeoutError:
            pass

    def _install_signal_handlers(self) -> list[signal.Signals]:
        loop = asyncio.get_running_loop()
        installed: list[signal.Signals] = []
        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(sig, self.request_stop)
            except (NotImplementedError, RuntimeError):
                continue
            installed.append(sig)
        return installed


async def _consume_cancelled(task: asyncio.Task[object]) -> None:
    try:
        await task
    except (asyncio.CancelledError, Exception):
        pass
