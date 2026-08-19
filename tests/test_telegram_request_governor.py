from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
import unittest
from uuid import uuid4

from telethon.errors import FloodWaitError

from freelancer_bot.config import RuntimeConfig
from freelancer_bot.persistence.collector_accounts import CollectorAccountRepository
from freelancer_bot.persistence.database import Database
from freelancer_bot.persistence.telegram_operation_state import (
    TelegramCollectorFloodWaitActive,
    TelegramCollectorOperationRepository,
    TelegramCollectorStatus,
)
from freelancer_bot.telegram_request_governor import (
    TelegramRequestCategory,
    TelegramRequestGovernor,
)
from postgres_support import TEST_DATABASE_URL, temporary_database, migrate_to_head


NOW = datetime(2026, 8, 16, 12, 0, tzinfo=timezone.utc)


@unittest.skipUnless(TEST_DATABASE_URL, "TEST_DATABASE_URL is not configured")
class TelegramRequestGovernorTest(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.database_context = temporary_database()
        self.database_url = self.database_context.__enter__()
        migrate_to_head(self.database_url)
        self.database = Database(self.database_url, pool_size=4, max_overflow=8)
        self.clock = _Clock(NOW)
        self.sleeps: list[float] = []

    async def asyncTearDown(self):
        await self.database.close()
        self.database_context.__exit__(None, None, None)

    async def test_pacing_and_source_cooldown_are_persisted(self):
        account_id = await self._account("pacing")
        governor = self._governor(account_id)

        await governor.run(
            TelegramRequestCategory.ENTITY_ACCESS,
            _value_operation("entity"),
        )
        state = await self._state(account_id)
        self.assertEqual(state.status, TelegramCollectorStatus.READY)
        self.assertEqual(state.last_request_category, "entity_access")
        self.assertEqual(state.next_allowed_request_at, NOW + timedelta(seconds=5))

        await governor.run(
            TelegramRequestCategory.GRAPH_HISTORY,
            _value_operation("history"),
        )
        self.assertEqual(self.sleeps, [5.0])
        state = await self._state(account_id)
        self.assertEqual(state.last_request_category, "graph_history")
        self.assertEqual(
            state.next_allowed_request_at,
            NOW + timedelta(seconds=5 + 15),
        )

    async def test_local_concurrency_is_one_per_collector(self):
        account_id = await self._account("concurrency")
        governor = self._governor(account_id)
        active = 0
        maximum = 0

        async def operation():
            nonlocal active, maximum
            active += 1
            maximum = max(maximum, active)
            await asyncio.sleep(0)
            active -= 1

        await asyncio.gather(
            governor.run(TelegramRequestCategory.ENTITY_ACCESS, operation),
            governor.run(TelegramRequestCategory.ENTITY_ACCESS, operation),
        )
        self.assertEqual(maximum, 1)

    async def test_persistent_lease_blocks_a_second_worker_for_same_account(self):
        account_id = await self._account("persistent-lease")
        repository = TelegramCollectorOperationRepository()
        first_token = uuid4()
        second_token = uuid4()
        async with self.database.transaction() as connection:
            first = await repository.reserve(
                connection,
                collector_account_id=account_id,
                request_token=first_token,
                request_category=TelegramRequestCategory.HISTORY,
                now=NOW,
                lease_seconds=900,
            )
        async with self.database.transaction() as connection:
            second = await repository.reserve(
                connection,
                collector_account_id=account_id,
                request_token=second_token,
                request_category=TelegramRequestCategory.HISTORY,
                now=NOW,
                lease_seconds=900,
            )
        self.assertTrue(first.acquired)
        self.assertFalse(second.acquired)
        self.assertEqual(second.wait_until, NOW + timedelta(seconds=900))

    async def test_floodwait_is_persisted_and_new_runtime_makes_zero_calls(self):
        account_id = await self._account("floodwait")
        governor = self._governor(account_id)
        calls = 0

        async def flood_operation():
            nonlocal calls
            calls += 1
            raise FloodWaitError(request=None, capture=42)

        with self.assertRaises(FloodWaitError):
            await governor.run(TelegramRequestCategory.ENTITY_ACCESS, flood_operation)
        self.assertEqual(calls, 1)
        state = await self._state(account_id)
        self.assertEqual(state.status, TelegramCollectorStatus.FLOODWAIT)
        self.assertEqual(state.last_floodwait_seconds, 42)
        self.assertEqual(state.cooldown_until, NOW + timedelta(seconds=42))

        restarted = self._governor(account_id)

        async def should_not_run():
            raise AssertionError("Telegram operation ran during persisted FloodWait")

        with self.assertRaises(TelegramCollectorFloodWaitActive):
            await restarted.run(TelegramRequestCategory.HISTORY, should_not_run)
        self.assertEqual(calls, 1)

    async def test_independent_collector_accounts_do_not_share_gate(self):
        first = await self._account("first")
        second = await self._account("second")
        first_governor = self._governor(first)
        second_governor = self._governor(second)
        entered: list[int] = []

        async def first_operation():
            entered.append(first)
            await asyncio.sleep(0)

        async def second_operation():
            entered.append(second)
            await asyncio.sleep(0)

        await asyncio.gather(
            first_governor.run(TelegramRequestCategory.HISTORY, first_operation),
            second_governor.run(TelegramRequestCategory.HISTORY, second_operation),
        )
        self.assertEqual(set(entered), {first, second})

    async def _account(self, suffix: str) -> int:
        async with self.database.transaction() as connection:
            account = await CollectorAccountRepository().ensure(
                connection,
                platform="telegram",
                external_account_id=f"governor-{suffix}-{uuid4().hex}",
                display_name=f"Governor {suffix}",
            )
            await TelegramCollectorOperationRepository().ensure(
                connection,
                collector_account_id=account.id,
            )
        return account.id

    async def _state(self, account_id: int):
        async with self.database.connect() as connection:
            return await TelegramCollectorOperationRepository().get(connection, account_id)

    def _governor(self, account_id: int) -> TelegramRequestGovernor:
        config = RuntimeConfig(
            _env_file=None,
            telegram_crawl_min_delay_seconds=5,
            telegram_crawl_max_delay_seconds=10,
            telegram_source_cooldown_min_seconds=15,
            telegram_source_cooldown_max_seconds=30,
            telegram_governor_lease_seconds=900,
        )

        async def sleep(seconds: float) -> None:
            self.sleeps.append(seconds)
            self.clock.value += timedelta(seconds=seconds)

        return TelegramRequestGovernor(
            self.database,
            account_id,
            config,
            clock=self.clock,
            sleep=sleep,
            random_uniform=lambda lower, upper: lower,
        )


class _Clock:
    def __init__(self, value: datetime):
        self.value = value

    def __call__(self) -> datetime:
        return self.value


def _value_operation(value):
    async def operation():
        return value

    return operation


if __name__ == "__main__":
    unittest.main()
