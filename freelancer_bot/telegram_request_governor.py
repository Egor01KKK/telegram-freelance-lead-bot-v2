from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from datetime import datetime, timedelta, timezone
import random
from typing import Any, TypeVar
from uuid import uuid4

from telethon.errors import FloodWaitError

from .config import RuntimeConfig
from .persistence.database import Database
from .persistence.telegram_operation_state import (
    TelegramCollectorFloodWaitActive,
    TelegramCollectorOperationRepository,
    TelegramCollectorOperationState,
    TelegramOperationOutcome,
)


T = TypeVar("T")


class TelegramRequestCategory:
    ENTITY_ACCESS = "entity_access"
    HISTORY = "history"
    GRAPH_HISTORY = "graph_history"
    AUDIT_HISTORY = "audit_history"
    GLOBAL_SEARCH = "global_search"


class TelegramRequestGovernor:
    """One durable, per-collector gate for active Telegram crawl operations.

    The in-process lock avoids needless local contention.  The PostgreSQL lease
    is the authoritative cross-process guard, so two discovery/audit commands
    using the same collector account cannot issue active requests concurrently.
    Passive update handlers do not call this class.
    """

    def __init__(
        self,
        database: Database,
        collector_account_id: int,
        config: RuntimeConfig,
        *,
        clock: Callable[[], datetime] | None = None,
        sleep: Callable[[float], Awaitable[None]] | None = None,
        random_uniform: Callable[[float, float], float] | None = None,
        repository: TelegramCollectorOperationRepository | None = None,
    ) -> None:
        if isinstance(collector_account_id, bool) or collector_account_id <= 0:
            raise ValueError("collector_account_id must be positive")
        self._database = database
        self.collector_account_id = collector_account_id
        self._min_delay = config.telegram_crawl_min_delay_seconds
        self._max_delay = config.telegram_crawl_max_delay_seconds
        self._source_cooldown_min = config.telegram_source_cooldown_min_seconds
        self._source_cooldown_max = config.telegram_source_cooldown_max_seconds
        self._lease_seconds = config.telegram_governor_lease_seconds
        self._clock = clock or (lambda: datetime.now(timezone.utc))
        self._sleep = sleep or asyncio.sleep
        self._random_uniform = random_uniform or random.uniform
        self._repository = repository or TelegramCollectorOperationRepository()
        self._local_lock = asyncio.Lock()

    async def run(
        self,
        category: str,
        operation: Callable[[], Awaitable[T]],
        *,
        before_reserve: Callable[[], Awaitable[tuple[bool, T | None]]] | None = None,
    ) -> T:
        if not callable(operation):
            raise TypeError("operation must be callable")
        if before_reserve is not None and not callable(before_reserve):
            raise TypeError("before_reserve must be callable")
        async with self._local_lock:
            request_token = uuid4()
            while True:
                if before_reserve is not None:
                    ready, value = await before_reserve()
                    if ready:
                        return value  # type: ignore[return-value]
                reservation = await self._reserve(request_token, category)
                if reservation.acquired:
                    break
                wait_until = reservation.wait_until
                if wait_until is None:
                    wait_seconds = 0.25
                else:
                    wait_seconds = max(
                        0.05,
                        (wait_until - self._now()).total_seconds(),
                    )
                await self._sleep(wait_seconds)

            try:
                result = await operation()
            except FloodWaitError as exc:
                seconds = _floodwait_seconds(exc)
                await self._finish(
                    request_token,
                    category=category,
                    outcome=TelegramOperationOutcome.FLOODWAIT,
                    floodwait_seconds=seconds,
                )
                raise
            except BaseException:
                await self._finish(
                    request_token,
                    category=category,
                    outcome=TelegramOperationOutcome.ERROR,
                )
                raise
            else:
                await self._finish(
                    request_token,
                    category=category,
                    outcome=TelegramOperationOutcome.COMPLETED,
                )
                return result

    async def current_state(self) -> TelegramCollectorOperationState:
        async with self._database.connect() as connection:
            return await self._repository.get(connection, self.collector_account_id)

    async def _reserve(self, request_token, category: str):
        now = self._now()
        async with self._database.transaction() as connection:
            return await self._repository.reserve(
                connection,
                collector_account_id=self.collector_account_id,
                request_token=request_token,
                request_category=category,
                now=now,
                lease_seconds=self._lease_seconds,
            )

    async def _finish(
        self,
        request_token,
        *,
        category: str,
        outcome: TelegramOperationOutcome,
        floodwait_seconds: int | None = None,
    ) -> None:
        now = self._now()
        if outcome is TelegramOperationOutcome.FLOODWAIT:
            next_allowed = None
        else:
            next_allowed = now + timedelta(seconds=self._next_delay(category))
        async with self._database.transaction() as connection:
            state = await self._repository.finish(
                connection,
                collector_account_id=self.collector_account_id,
                request_token=request_token,
                finished_at=now,
                outcome=outcome,
                next_allowed_request_at=next_allowed,
                floodwait_seconds=floodwait_seconds,
            )
        if outcome is TelegramOperationOutcome.FLOODWAIT:
            # The repository has already persisted the authoritative wait.  The
            # local exception is deliberately the original Telethon error.
            _ = state

    def _next_delay(self, category: str) -> float:
        if category in {
            TelegramRequestCategory.HISTORY,
            TelegramRequestCategory.GRAPH_HISTORY,
            TelegramRequestCategory.AUDIT_HISTORY,
        }:
            return self._random_uniform(
                self._source_cooldown_min,
                self._source_cooldown_max,
            )
        return self._random_uniform(self._min_delay, self._max_delay)

    def _now(self) -> datetime:
        value = self._clock()
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("Telegram governor clock must return an aware datetime")
        return value


def _floodwait_seconds(error: FloodWaitError) -> int:
    value = getattr(error, "seconds", None)
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise RuntimeError("Telegram FloodWaitError did not contain a positive wait")
    return value
