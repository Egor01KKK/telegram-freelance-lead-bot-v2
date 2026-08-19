import unittest

import sqlalchemy as sa
from sqlalchemy.exc import IntegrityError

from freelancer_bot.persistence.collector_accounts import CollectorAccountRepository
from freelancer_bot.persistence.database import Database
from freelancer_bot.persistence.schema import (
    source_discovery_lineage,
    source_lifecycle_events,
    source_taxonomy_assignments,
    source_taxonomy_terms,
    sources,
)
from freelancer_bot.persistence.source_repository import (
    InvalidSourceTransition,
    PostgresSourceCatalog,
    SourceIdentityConflict,
    SourceRepository,
    SourceStatus,
)
from postgres_support import TEST_DATABASE_URL, migrate_to_head, temporary_database


@unittest.skipUnless(TEST_DATABASE_URL, "TEST_DATABASE_URL is not configured")
class SourceRepositoryTest(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.database_context = temporary_database()
        self.database_url = self.database_context.__enter__()
        migrate_to_head(self.database_url)
        self.database = Database(self.database_url, pool_size=4, max_overflow=8)
        self.repository = SourceRepository()
        self.collector_accounts = CollectorAccountRepository()

    async def asyncTearDown(self):
        await self.database.close()
        self.database_context.__exit__(None, None, None)

    async def test_valid_lifecycle_and_collector_catalog_expose_only_approved(self):
        approved = await self._create_candidate("approved", "@approved_source")
        candidate = await self._create_candidate("candidate", "@candidate_source")
        rejected = await self._create_candidate("rejected", "@rejected_source")

        async with self.database.transaction() as connection:
            approved = await self.repository.transition(
                connection,
                approved.id,
                SourceStatus.APPROVED,
                reason="operator review passed",
            )
            approved = await self.repository.transition(
                connection,
                approved.id,
                SourceStatus.PAUSED,
                reason="temporary collector pause",
            )
            approved = await self.repository.transition(
                connection,
                approved.id,
                SourceStatus.APPROVED,
                reason="collector access restored",
            )
            approved = await self.repository.update_metadata(
                connection,
                approved.id,
                display_name="Approved source updated",
                access_type="PUBLIC",
                handle="@APPROVED_SOURCE",
                canonical_url="https://t.me/approved_source",
            )
            rejected = await self.repository.transition(
                connection,
                rejected.id,
                SourceStatus.REJECTED,
                reason="not relevant",
            )

        async with self.database.transaction() as connection:
            with self.assertRaisesRegex(
                InvalidSourceTransition,
                "candidate -> paused",
            ):
                await self.repository.transition(
                    connection,
                    candidate.id,
                    SourceStatus.PAUSED,
                    reason="invalid normal transition",
                )

        async with self.database.transaction() as connection:
            collector_account = await self.collector_accounts.ensure(
                connection,
                platform="telegram",
                external_account_id="repository-test-account",
                display_name="Repository test account",
            )

        catalog = PostgresSourceCatalog(self.database, self.repository)
        collector_sources = await catalog.list_approved(
            collector_account_id=collector_account.id,
            platform="telegram",
        )

        self.assertEqual([source.id for source in collector_sources], [approved.id])
        self.assertEqual(collector_sources[0].lifecycle_status, SourceStatus.APPROVED)
        self.assertEqual(collector_sources[0].display_name, "Approved source updated")
        self.assertEqual(collector_sources[0].handle, "@approved_source")
        async with self.database.connect() as connection:
            events = await self.repository.list_lifecycle_events(connection, approved.id)
        self.assertEqual(
            [(event.from_status, event.to_status) for event in events],
            [
                (None, SourceStatus.CANDIDATE),
                (SourceStatus.CANDIDATE, SourceStatus.APPROVED),
                (SourceStatus.APPROVED, SourceStatus.PAUSED),
                (SourceStatus.PAUSED, SourceStatus.APPROVED),
            ],
        )

    async def test_manual_overrides_preserve_source_lineage_taxonomy_and_history(self):
        source = await self._create_candidate("manual", "@manual_source")
        async with self.database.transaction() as connection:
            term_id = await connection.scalar(
                sa.insert(source_taxonomy_terms)
                .values(
                    dimension="category",
                    key="telegram_development",
                    display_name="Telegram development",
                )
                .returning(source_taxonomy_terms.c.id)
            )
            await connection.execute(
                sa.insert(source_taxonomy_assignments).values(
                    source_id=source.id,
                    term_id=term_id,
                )
            )
            source = await self.repository.override(
                connection,
                source.id,
                SourceStatus.PAUSED,
                operator_id="operator-7",
                reason="manual pause",
            )
            source = await self.repository.override(
                connection,
                source.id,
                SourceStatus.REJECTED,
                operator_id="operator-7",
                reason="manual rejection",
            )
            source = await self.repository.override(
                connection,
                source.id,
                SourceStatus.APPROVED,
                operator_id="operator-8",
                reason="manual approval after review",
            )

        async with self.database.connect() as connection:
            events = await self.repository.list_lifecycle_events(connection, source.id)
            lineage = await self.repository.list_lineage(connection, source.id)
            assignments = await connection.scalar(
                sa.select(sa.func.count())
                .select_from(source_taxonomy_assignments)
                .where(source_taxonomy_assignments.c.source_id == source.id)
            )

        self.assertEqual(source.lifecycle_status, SourceStatus.APPROVED)
        self.assertEqual(assignments, 1)
        self.assertEqual(len(lineage), 1)
        self.assertEqual(lineage[0].provider, "repository_seed")
        self.assertEqual(
            [event.to_status for event in events],
            [
                SourceStatus.CANDIDATE,
                SourceStatus.PAUSED,
                SourceStatus.REJECTED,
                SourceStatus.APPROVED,
            ],
        )
        self.assertEqual([event.is_override for event in events], [False, True, True, True])
        self.assertEqual(
            [event.actor_id for event in events[1:]],
            ["operator-7", "operator-7", "operator-8"],
        )

    async def test_platform_external_identity_and_lineage_are_idempotent(self):
        source = await self._create_candidate("identity", "@identity_source")
        with self.assertRaises(SourceIdentityConflict):
            async with self.database.transaction() as connection:
                await self.repository.create_candidate(
                    connection,
                    platform="TELEGRAM",
                    external_id="identity",
                    access_type="public",
                    display_name="Duplicate identity",
                    handle="@identity_duplicate",
                    provider="web_search",
                    lineage_key="result-duplicate",
                )

        async with self.database.transaction() as connection:
            duplicate_created = await self.repository.record_lineage(
                connection,
                source_id=source.id,
                provider="repository_seed",
                lineage_key="fixture:identity",
            )
            second_created = await self.repository.record_lineage(
                connection,
                source_id=source.id,
                provider="telegram_graph",
                lineage_key="graph:identity:2",
                provider_run_id="discovery-run-2",
                seed_reference="@parent_source",
                context={"distance": 1},
            )

        self.assertFalse(duplicate_created)
        self.assertTrue(second_created)
        async with self.database.connect() as connection:
            lineage = await self.repository.list_lineage(connection, source.id)
        self.assertEqual(len(lineage), 2)
        self.assertEqual(lineage[1].provider, "telegram_graph")
        self.assertEqual(lineage[1].provider_run_id, "discovery-run-2")
        self.assertEqual(lineage[1].context, {"distance": 1})

    async def test_source_with_history_cannot_be_deleted(self):
        source = await self._create_candidate("protected", "@protected_source")

        with self.assertRaises(IntegrityError):
            async with self.database.transaction() as connection:
                await connection.execute(sa.delete(sources).where(sources.c.id == source.id))

        async with self.database.connect() as connection:
            self.assertEqual(
                await connection.scalar(
                    sa.select(sa.func.count())
                    .select_from(source_lifecycle_events)
                    .where(source_lifecycle_events.c.source_id == source.id)
                ),
                1,
            )
            self.assertEqual(
                await connection.scalar(
                    sa.select(sa.func.count())
                    .select_from(source_discovery_lineage)
                    .where(source_discovery_lineage.c.source_id == source.id)
                ),
                1,
            )

    async def _create_candidate(self, external_id: str, handle: str):
        async with self.database.transaction() as connection:
            return await self.repository.create_candidate(
                connection,
                platform="telegram",
                external_id=external_id,
                access_type="public",
                display_name=f"Source {external_id}",
                handle=handle,
                canonical_url=f"https://t.me/{handle.removeprefix('@')}",
                provider="repository_seed",
                lineage_key=f"fixture:{external_id}",
                provider_run_id="fixture-run-1",
                seed_reference=handle,
                context={"fixture": True},
            )


if __name__ == "__main__":
    unittest.main()
