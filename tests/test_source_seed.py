import asyncio
import contextlib
import io
import json
import os
import unittest
from pathlib import Path
from unittest.mock import patch

import sqlalchemy as sa

from freelancer_bot.persistence.database import Database
from freelancer_bot.persistence.schema import (
    source_discovery_lineage,
    source_lifecycle_events,
    sources,
)
from freelancer_bot.persistence.source_repository import SourceRepository, SourceStatus
from freelancer_bot.persistence.source_seed import SourceSeedImporter, main
from freelancer_bot.sources import load_sources
from postgres_support import ROOT, TEST_DATABASE_URL, migrate_to_head, temporary_database


SOURCES_PATH = ROOT / "config" / "sources.json"


@unittest.skipUnless(TEST_DATABASE_URL, "TEST_DATABASE_URL is not configured")
class SourceSeedImporterTest(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.database_context = temporary_database()
        self.database_url = self.database_context.__enter__()
        migrate_to_head(self.database_url)
        self.database = Database(self.database_url, pool_size=4, max_overflow=8)
        self.repository = SourceRepository()
        self.importer = SourceSeedImporter(self.database, self.repository)

    async def asyncTearDown(self):
        await self.database.close()
        self.database_context.__exit__(None, None, None)

    async def test_current_repository_seed_is_complete_and_idempotent(self):
        configured = load_sources(SOURCES_PATH)

        first = await self.importer.import_file(SOURCES_PATH)
        second = await self.importer.import_file(SOURCES_PATH)

        self.assertEqual(
            (first.total, first.created, first.updated, first.unchanged),
            (15, 15, 0, 0),
        )
        self.assertEqual(first.lineage_created, 15)
        self.assertEqual((first.approved_entries, first.candidate_entries), (13, 2))
        self.assertEqual(
            (second.total, second.created, second.updated, second.unchanged),
            (15, 0, 0, 15),
        )
        self.assertEqual(second.lineage_created, 0)
        self.assertEqual(second.snapshot_sha256, first.snapshot_sha256)

        async with self.database.connect() as connection:
            records = (
                await connection.execute(sa.select(sources).order_by(sources.c.id))
            ).mappings().all()
            lineage = (
                await connection.execute(
                    sa.select(source_discovery_lineage).order_by(
                        source_discovery_lineage.c.source_id
                    )
                )
            ).mappings().all()
            events = (
                await connection.execute(sa.select(source_lifecycle_events))
            ).mappings().all()

        expected_handles = {source.handle.lower() for source in configured}
        self.assertEqual({record["handle"] for record in records}, expected_handles)
        self.assertEqual(
            {record["external_id"] for record in records},
            {
                f"username:{source.handle.removeprefix('@').lower()}"
                for source in configured
            },
        )
        self.assertEqual(
            sum(record["lifecycle_status"] == "approved" for record in records),
            13,
        )
        self.assertEqual(
            sum(record["lifecycle_status"] == "candidate" for record in records),
            2,
        )
        self.assertEqual(len(lineage), 15)
        self.assertEqual({row["provider"] for row in lineage}, {"repository_seed"})
        self.assertTrue(all(row["provider_run_id"] == first.snapshot_sha256 for row in lineage))
        self.assertEqual(len(events), 15)
        self.assertEqual({event["actor_kind"] for event in events}, {"seed"})

        apibot = next(row for row in lineage if row["seed_reference"] == "@apibot_tg")
        self.assertFalse(apibot["context"]["enabled"])
        self.assertIn("unverified", apibot["context"]["tags"])
        self.assertTrue(apibot["context"]["reason"])

    async def test_seed_rerun_preserves_manual_lifecycle_override(self):
        await self.importer.import_file(SOURCES_PATH)
        async with self.database.transaction() as connection:
            source = await self.repository.get_by_identity(
                connection,
                platform="telegram",
                external_id="username:freelansim_ru",
            )
            self.assertIsNotNone(source)
            source = await self.repository.override(
                connection,
                source.id,
                SourceStatus.PAUSED,
                operator_id="operator-seed-test",
                reason="manual quality pause",
            )

        result = await self.importer.import_file(SOURCES_PATH)

        self.assertEqual((result.created, result.updated, result.unchanged), (0, 0, 15))
        async with self.database.connect() as connection:
            persisted = await self.repository.get(connection, source.id)
            events = await self.repository.list_lifecycle_events(connection, source.id)
        self.assertEqual(persisted.lifecycle_status, SourceStatus.PAUSED)
        self.assertEqual(
            [event.to_status for event in events],
            [SourceStatus.APPROVED, SourceStatus.PAUSED],
        )
        self.assertTrue(events[-1].is_override)

    async def test_concurrent_seed_imports_create_one_copy(self):
        results = await asyncio.gather(
            self.importer.import_file(SOURCES_PATH),
            self.importer.import_file(SOURCES_PATH),
        )

        self.assertEqual(sum(result.created for result in results), 15)
        self.assertEqual(sum(result.lineage_created for result in results), 15)
        async with self.database.connect() as connection:
            source_count = await connection.scalar(
                sa.select(sa.func.count()).select_from(sources)
            )
            lineage_count = await connection.scalar(
                sa.select(sa.func.count()).select_from(source_discovery_lineage)
            )
        self.assertEqual(source_count, 15)
        self.assertEqual(lineage_count, 15)


@unittest.skipUnless(TEST_DATABASE_URL, "TEST_DATABASE_URL is not configured")
class SourceSeedCommandTest(unittest.TestCase):
    def test_module_command_imports_repository_seed(self):
        with temporary_database() as database_url:
            migrate_to_head(database_url)
            output = io.StringIO()
            with (
                patch.dict(os.environ, {"DATABASE_URL": database_url}, clear=False),
                contextlib.redirect_stdout(output),
            ):
                exit_code = main(["--sources-json", str(SOURCES_PATH)])

            result = json.loads(output.getvalue())
            self.assertEqual(exit_code, 0)
            self.assertEqual(result["created"], 15)
            self.assertEqual(result["approved_entries"], 13)

            engine = sa.create_engine(database_url)
            try:
                with engine.connect() as connection:
                    self.assertEqual(
                        connection.scalar(sa.select(sa.func.count()).select_from(sources)),
                        15,
                    )
            finally:
                engine.dispose()


if __name__ == "__main__":
    unittest.main()
