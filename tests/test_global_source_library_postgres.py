from __future__ import annotations

import asyncio
from datetime import datetime, timezone
import unittest
from uuid import uuid4

import sqlalchemy as sa

from freelancer_bot.global_source_library import (
    bootstrap_campaign_specs,
    generate_campaign_queries,
)
from freelancer_bot.persistence.database import Database
from freelancer_bot.persistence.discovery_campaigns import DiscoveryCampaignRepository
from freelancer_bot.persistence.schema import (
    collector_accounts,
    discovery_cost_events,
    source_discovery_evidence,
    sources,
)
from freelancer_bot.persistence.source_repository import SourceRepository
from postgres_support import TEST_DATABASE_URL, migrate_to_head, temporary_database


@unittest.skipUnless(TEST_DATABASE_URL, "TEST_DATABASE_URL is not configured")
class GlobalSourceLibraryPostgresTest(unittest.TestCase):
    def setUp(self) -> None:
        self.database_url = next(iter(self._database_context()))

    def _database_context(self):
        context = temporary_database()
        database_url = context.__enter__()
        self.addCleanup(context.__exit__, None, None, None)
        migrate_to_head(database_url)
        yield database_url

    def test_campaign_query_alias_and_plan_job_are_idempotent(self):
        async def scenario() -> None:
            database = Database(self.database_url)
            try:
                repository = DiscoveryCampaignRepository()
                spec = bootstrap_campaign_specs()[0]
                async with database.transaction() as connection:
                    campaign = await repository.ensure_campaign(connection, spec)
                    first_count = await repository.ensure_queries(
                        connection,
                        campaign.id,
                        generate_campaign_queries(spec),
                    )
                    second_count = await repository.ensure_queries(
                        connection,
                        campaign.id,
                        generate_campaign_queries(spec),
                    )
                    first_job = await repository.enqueue_campaign_plan(
                        connection,
                        campaign=campaign,
                    )
                    second_job = await repository.enqueue_campaign_plan(
                        connection,
                        campaign=campaign,
                    )
                    continuation_job = await repository.enqueue_campaign_plan(
                        connection,
                        campaign=campaign,
                        batch_key="20",
                    )
                    source_id = await connection.scalar(
                        sources.insert()
                        .values(
                            platform="telegram",
                            external_id="username:library_fixture",
                            access_type="public",
                            lifecycle_status="candidate",
                            display_name="Library fixture",
                            handle="@library_fixture",
                            canonical_url="https://t.me/library_fixture",
                        )
                        .returning(sources.c.id)
                    )
                    collector_id = await connection.scalar(
                        collector_accounts.insert()
                        .values(
                            platform="telegram",
                            external_account_id="library-fixture-account",
                            display_name="Library fixture account",
                        )
                        .returning(collector_accounts.c.id)
                    )
                    await repository.record_alias(
                        connection,
                        source_id=int(source_id),
                        platform="telegram",
                        normalized_reference="username:library_fixture",
                        reference_kind="username",
                    )
                    await repository.upsert_validation(
                        connection,
                        source_id=int(source_id),
                        collector_account_id=int(collector_id),
                        state="accessible",
                        access_mode="public_readable",
                        checked_at=datetime.now(timezone.utc),
                        checked_by="test",
                    )
                    alias_source = await repository.source_for_alias(
                        connection,
                        platform="telegram",
                        normalized_reference="username:library_fixture",
                    )
                    pending_count = await repository.pending_query_count(
                        connection,
                        campaign_id=campaign.id,
                    )
                self.assertEqual(second_count, 0)
                self.assertEqual(first_job, second_job)
                self.assertNotEqual(first_job, continuation_job)
                self.assertEqual(pending_count, first_count)
                self.assertEqual(alias_source, source_id)
                self.assertGreater(first_count, 0)
            finally:
                await database.close()

        asyncio.run(scenario())

    def test_brave_cost_reservation_is_idempotent_and_campaign_bounded(self):
        async def scenario() -> None:
            database = Database(self.database_url)
            try:
                repository = DiscoveryCampaignRepository()
                campaign_spec = bootstrap_campaign_specs()[0]
                async with database.transaction() as connection:
                    campaign = await repository.ensure_campaign(connection, campaign_spec)
                    first = await repository.reserve_cost(
                        connection,
                        campaign_id=campaign.id,
                        stage="web_search",
                        provider="brave",
                        idempotency_key="brave:fixture:one",
                        daily_units_limit=10,
                        campaign_units_limit=1,
                    )
                    repeated = await repository.reserve_cost(
                        connection,
                        campaign_id=campaign.id,
                        stage="web_search",
                        provider="brave",
                        idempotency_key="brave:fixture:one",
                        daily_units_limit=10,
                        campaign_units_limit=1,
                    )
                    rejected = await repository.reserve_cost(
                        connection,
                        campaign_id=campaign.id,
                        stage="web_search",
                        provider="brave",
                        idempotency_key="brave:fixture:two",
                        daily_units_limit=10,
                        campaign_units_limit=1,
                    )
                    count = await connection.scalar(
                        sa.select(sa.func.count()).select_from(discovery_cost_events)
                    )
                self.assertTrue(first)
                self.assertTrue(repeated)
                self.assertFalse(rejected)
                self.assertEqual(count, 1)
            finally:
                await database.close()

        asyncio.run(scenario())

    def test_legacy_evidence_backfill_is_truthful_and_idempotent(self):
        async def scenario() -> None:
            database = Database(self.database_url)
            try:
                repository = DiscoveryCampaignRepository()
                async with database.transaction() as connection:
                    source = await SourceRepository().create_candidate(
                        connection,
                        platform="telegram",
                        external_id="username:legacy_evidence_fixture",
                        access_type="public",
                        display_name="Legacy evidence fixture",
                        handle="@legacy_evidence_fixture",
                        canonical_url="https://t.me/legacy_evidence_fixture",
                        provider="web_search",
                        lineage_key="legacy-evidence-fixture",
                        provider_run_id="legacy-run",
                        context={
                            "discovery_method": "web_search",
                            "matches": [
                                {
                                    "query": "Telegram community for founders",
                                    "query_kind": "community",
                                    "result_url": "https://example.org/list",
                                }
                            ],
                        },
                    )
                    first = await repository.backfill_legacy_evidence(connection)
                    second = await repository.backfill_legacy_evidence(connection)
                    evidence = (
                        await connection.execute(
                            source_discovery_evidence.select().where(
                                source_discovery_evidence.c.source_id == source.id
                            )
                        )
                    ).mappings().all()
                self.assertEqual(first.candidate_sources, 1)
                self.assertEqual(first.recoverable_sources, 1)
                self.assertEqual(first.unrecoverable_sources, 0)
                self.assertEqual(first.evidence_created, 1)
                self.assertEqual(second.evidence_created, 0)
                self.assertEqual(second.evidence_existing, 1)
                self.assertEqual(len(evidence), 1)
                self.assertEqual(evidence[0]["provider"], "web_search")
                self.assertEqual(evidence[0]["extraction_kind"], "global_search")
                self.assertEqual(evidence[0]["query_family"], "COMMUNITY_DIRECTORY")
                self.assertEqual(evidence[0]["result_domain"], "example.org")
            finally:
                await database.close()

        asyncio.run(scenario())
