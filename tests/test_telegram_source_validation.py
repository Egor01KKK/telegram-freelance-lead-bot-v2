from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace
import unittest

import sqlalchemy as sa

from freelancer_bot.config import RuntimeConfig
from freelancer_bot.persistence.collector_accounts import CollectorAccountRepository
from freelancer_bot.persistence.database import Database
from freelancer_bot.persistence.discovery_campaigns import DiscoveryCampaignRepository
from freelancer_bot.persistence.schema import (
    telegram_collector_operation_events,
    telegram_source_validations,
)
from freelancer_bot.persistence.source_repository import SourceRepository, SourceStatus
from freelancer_bot.persistence.telegram_operation_state import (
    TelegramCollectorOperationRepository,
)
from freelancer_bot.telegram_request_governor import TelegramRequestGovernor
from freelancer_bot.telegram_source_validation import TelegramSourceValidationService
from postgres_support import TEST_DATABASE_URL, migrate_to_head, temporary_database


NOW = datetime(2026, 8, 17, 12, 0, tzinfo=timezone.utc)


@unittest.skipUnless(TEST_DATABASE_URL, "TEST_DATABASE_URL is not configured")
class TelegramSourceValidationTest(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.database_context = temporary_database()
        self.database_url = self.database_context.__enter__()
        migrate_to_head(self.database_url)
        self.database = Database(self.database_url, pool_size=4, max_overflow=8)
        self.sources = SourceRepository()

    async def asyncTearDown(self) -> None:
        await self.database.close()
        self.database_context.__exit__(None, None, None)

    async def test_terminal_state_and_canonical_alias_reuse_without_new_api_calls(self):
        async with self.database.transaction() as connection:
            account = await CollectorAccountRepository().ensure(
                connection,
                platform="telegram",
                external_account_id="validation-cache-account",
                display_name="Validation cache account",
            )
            await TelegramCollectorOperationRepository().ensure(
                connection,
                collector_account_id=account.id,
            )
            first = await self.sources.create_candidate(
                connection,
                platform="telegram",
                external_id="username:validation_cache_target",
                access_type="public",
                display_name="Validation cache target",
                provider="test",
                lineage_key="validation-cache:first",
                handle="@validation_cache_target",
                canonical_url="https://t.me/validation_cache_target",
            )
            second = await self.sources.create_candidate(
                connection,
                platform="telegram",
                external_id="username:validation_cache_alias",
                access_type="public",
                display_name="Validation cache alias",
                provider="test",
                lineage_key="validation-cache:second",
                canonical_url="https://t.me/validation_cache_target",
            )

        calls = 0

        class Client:
            async def get_entity(self, _lookup):
                nonlocal calls
                calls += 1
                return SimpleNamespace(id=912345)

        client = Client()
        config = RuntimeConfig(
            _env_file=None,
            telegram_crawl_min_delay_seconds=0,
            telegram_crawl_max_delay_seconds=0,
            telegram_source_cooldown_min_seconds=0,
            telegram_source_cooldown_max_seconds=0,
            telegram_governor_lease_seconds=900,
        )
        service = TelegramSourceValidationService(
            self.database,
            library_repository=DiscoveryCampaignRepository(),
        )

        first_result = await service.validate(
            source_id=first.id,
            collector_account_id=account.id,
            client=client,
            governor=TelegramRequestGovernor(
                self.database,
                account.id,
                config,
                clock=lambda: NOW,
                random_uniform=lambda lower, _upper: lower,
            ),
            checked_by="test",
        )
        repeated_result = await service.validate(
            source_id=first.id,
            collector_account_id=account.id,
            client=client,
            governor=TelegramRequestGovernor(
                self.database,
                account.id,
                config,
                clock=lambda: NOW,
                random_uniform=lambda lower, _upper: lower,
            ),
            checked_by="test",
        )
        alias_result = await service.validate(
            source_id=second.id,
            collector_account_id=account.id,
            client=client,
            governor=TelegramRequestGovernor(
                self.database,
                account.id,
                config,
                clock=lambda: NOW,
                random_uniform=lambda lower, _upper: lower,
            ),
            checked_by="test",
        )

        self.assertEqual(first_result.state, "accessible")
        self.assertEqual(repeated_result, first_result)
        self.assertEqual(alias_result.state, "rejected")
        self.assertEqual(alias_result.failure_code, "canonical_alias_existing")
        self.assertEqual(alias_result.duplicate_of_source_id, first.id)
        self.assertEqual(calls, 1)

        async with self.database.connect() as connection:
            event_count = await connection.scalar(
                sa.select(sa.func.count()).select_from(telegram_collector_operation_events)
            )
            validation_rows = (
                await connection.execute(
                    sa.select(
                        telegram_source_validations.c.source_id,
                        telegram_source_validations.c.state,
                    ).order_by(telegram_source_validations.c.source_id)
                )
            ).mappings().all()
            second_source = await self.sources.get(connection, second.id)

        self.assertEqual(event_count, 1)
        self.assertEqual(
            {(int(row["source_id"]), row["state"]) for row in validation_rows},
            {(first.id, "accessible"), (second.id, "rejected")},
        )
        self.assertEqual(second_source.lifecycle_status, SourceStatus.REJECTED)


if __name__ == "__main__":
    unittest.main()
