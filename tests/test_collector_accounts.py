import asyncio
import unittest
from datetime import datetime, timezone

import sqlalchemy as sa
from alembic import command
from sqlalchemy.exc import IntegrityError

from freelancer_bot.persistence.collector_accounts import (
    CollectorAccessStatus,
    CollectorAccountRepository,
    InvalidCollectorAccess,
)
from freelancer_bot.persistence.database import Database
from freelancer_bot.persistence.schema import (
    collector_accounts,
    source_collector_access,
    sources,
)
from freelancer_bot.persistence.source_repository import (
    PostgresSourceCatalog,
    SourceRepository,
    SourceStatus,
)
from freelancer_bot.persistence.source_seed import SourceSeedImporter
from postgres_support import (
    ROOT,
    TEST_DATABASE_URL,
    alembic_config,
    migrate_to_head,
    temporary_database,
)


SOURCES_PATH = ROOT / "config" / "sources.json"


@unittest.skipUnless(TEST_DATABASE_URL, "TEST_DATABASE_URL is not configured")
class CollectorAccountRepositoryTest(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.database_context = temporary_database()
        self.database_url = self.database_context.__enter__()
        migrate_to_head(self.database_url)
        self.database = Database(self.database_url, pool_size=4, max_overflow=8)
        self.accounts = CollectorAccountRepository()
        self.sources = SourceRepository()

    async def asyncTearDown(self):
        await self.database.close()
        self.database_context.__exit__(None, None, None)

    async def test_multiple_accounts_have_stable_identity_and_active_state(self):
        async with self.database.transaction() as connection:
            first = await self.accounts.ensure(
                connection,
                platform="TELEGRAM",
                external_account_id="10001",
                display_name="Primary collector",
            )
            second = await self.accounts.ensure(
                connection,
                platform="telegram",
                external_account_id="10002",
                display_name="Secondary collector",
            )
            first = await self.accounts.set_active(connection, first.id, active=False)
            repeated = await self.accounts.ensure(
                connection,
                platform="telegram",
                external_account_id="10001",
                display_name="Primary collector renamed",
                active_on_create=True,
            )

        self.assertNotEqual(first.id, second.id)
        self.assertEqual(repeated.id, first.id)
        self.assertEqual(repeated.display_name, "Primary collector renamed")
        self.assertFalse(repeated.is_active)
        async with self.database.connect() as connection:
            count = await connection.scalar(
                sa.select(sa.func.count()).select_from(collector_accounts)
            )
        self.assertEqual(count, 2)

    async def test_access_resolution_enforces_private_grants_and_source_lifecycle(self):
        public = await self._create_source("public", "public", approved=True)
        private = await self._create_source("private", "private", approved=True)
        candidate = await self._create_source("candidate", "private", approved=False)
        async with self.database.transaction() as connection:
            primary = await self.accounts.ensure(
                connection,
                platform="telegram",
                external_account_id="20001",
                display_name="Primary collector",
            )
            secondary = await self.accounts.ensure(
                connection,
                platform="telegram",
                external_account_id="20002",
                display_name="Secondary collector",
            )
            await self.accounts.record_source_access(
                connection,
                source_id=private.id,
                collector_account_id=primary.id,
                access_status=CollectorAccessStatus.PERMITTED,
                checked_at=datetime.now(timezone.utc),
                checked_by="telethon-check:g1-fixture",
            )
            await self.accounts.record_source_access(
                connection,
                source_id=private.id,
                collector_account_id=secondary.id,
                access_status=CollectorAccessStatus.INACCESSIBLE,
                checked_at=datetime.now(timezone.utc),
                checked_by="telethon-check:g1-fixture",
            )
            await self.accounts.record_source_access(
                connection,
                source_id=candidate.id,
                collector_account_id=primary.id,
                access_status=CollectorAccessStatus.PERMITTED,
                checked_at=datetime.now(timezone.utc),
                checked_by="telethon-check:g1-fixture",
            )

        async with self.database.connect() as connection:
            public_resolution = await self.accounts.resolve_source_access(
                connection,
                public.id,
            )
            private_resolution = await self.accounts.resolve_source_access(
                connection,
                private.id,
            )
            candidate_resolution = await self.accounts.resolve_source_access(
                connection,
                candidate.id,
            )

        self.assertEqual(
            [account.id for account in public_resolution.collector_accounts],
            [primary.id, secondary.id],
        )
        self.assertTrue(public_resolution.collection_allowed)
        self.assertEqual(
            [account.id for account in private_resolution.collector_accounts],
            [primary.id],
        )
        self.assertTrue(private_resolution.collection_allowed)
        self.assertEqual(candidate_resolution.collector_accounts, ())
        self.assertFalse(candidate_resolution.collection_allowed)

        catalog = PostgresSourceCatalog(self.database, self.sources)
        primary_sources = await catalog.list_approved(
            collector_account_id=primary.id,
            platform="telegram",
        )
        secondary_sources = await catalog.list_approved(
            collector_account_id=secondary.id,
            platform="telegram",
        )
        self.assertEqual([source.id for source in primary_sources], [public.id, private.id])
        self.assertEqual([source.id for source in secondary_sources], [public.id])

        async with self.database.transaction() as connection:
            await self.accounts.set_active(connection, primary.id, active=False)
        async with self.database.connect() as connection:
            private_resolution = await self.accounts.resolve_source_access(
                connection,
                private.id,
            )
        self.assertFalse(private_resolution.collection_allowed)
        self.assertEqual(private_resolution.collector_accounts, ())
        self.assertEqual(
            await catalog.list_approved(
                collector_account_id=primary.id,
                platform="telegram",
            ),
            [],
        )

        async with self.database.transaction() as connection:
            await self.accounts.set_active(connection, primary.id, active=True)
            revoked = await self.accounts.record_source_access(
                connection,
                source_id=private.id,
                collector_account_id=primary.id,
                access_status=CollectorAccessStatus.REVOKED,
                checked_at=datetime.now(timezone.utc),
                checked_by="operator:g1-fixture",
            )
        self.assertEqual(revoked.access_status, CollectorAccessStatus.REVOKED)
        async with self.database.connect() as connection:
            private_resolution = await self.accounts.resolve_source_access(
                connection,
                private.id,
            )
        self.assertFalse(private_resolution.collection_allowed)
        self.assertEqual(private_resolution.collector_accounts, ())

    async def test_invalid_or_unverified_access_cannot_authorize_collection(self):
        public = await self._create_source("public-constraint", "public", approved=True)
        private = await self._create_source("private-constraint", "private", approved=True)
        async with self.database.transaction() as connection:
            telegram = await self.accounts.ensure(
                connection,
                platform="telegram",
                external_account_id="30001",
                display_name="Telegram collector",
            )
            web = await self.accounts.ensure(
                connection,
                platform="web",
                external_account_id="web-collector",
                display_name="Web collector",
            )

        with self.assertRaisesRegex(InvalidCollectorAccess, "timezone"):
            async with self.database.transaction() as connection:
                await self.accounts.record_source_access(
                    connection,
                    source_id=private.id,
                    collector_account_id=telegram.id,
                    access_status=CollectorAccessStatus.PERMITTED,
                    checked_at=datetime.now(),
                    checked_by="operator:g1-fixture",
                )
        with self.assertRaisesRegex(InvalidCollectorAccess, "private sources"):
            async with self.database.transaction() as connection:
                await self.accounts.record_source_access(
                    connection,
                    source_id=public.id,
                    collector_account_id=telegram.id,
                    access_status=CollectorAccessStatus.PERMITTED,
                    checked_at=datetime.now(timezone.utc),
                    checked_by="operator:g1-fixture",
                )
        with self.assertRaisesRegex(InvalidCollectorAccess, "platform"):
            async with self.database.transaction() as connection:
                await self.accounts.record_source_access(
                    connection,
                    source_id=private.id,
                    collector_account_id=web.id,
                    access_status=CollectorAccessStatus.PERMITTED,
                    checked_at=datetime.now(timezone.utc),
                    checked_by="operator:g1-fixture",
                )

        async with self.database.transaction() as connection:
            await self.accounts.record_source_access(
                connection,
                source_id=private.id,
                collector_account_id=telegram.id,
                access_status=CollectorAccessStatus.PERMITTED,
                checked_at=datetime.now(timezone.utc),
                checked_by="operator:g1-fixture",
            )
        with self.assertRaises(IntegrityError):
            async with self.database.transaction() as connection:
                await connection.execute(
                    sa.delete(collector_accounts).where(
                        collector_accounts.c.id == telegram.id
                    )
                )
        with self.assertRaises(IntegrityError):
            async with self.database.transaction() as connection:
                await connection.execute(
                    sa.insert(source_collector_access).values(
                        source_id=private.id,
                        collector_account_id=web.id,
                        access_status="invented",
                        checked_at=datetime.now(timezone.utc),
                        checked_by="operator:g1-fixture",
                    )
                )

    async def _create_source(
        self,
        external_id: str,
        access_type: str,
        *,
        approved: bool,
    ):
        async with self.database.transaction() as connection:
            source = await self.sources.create_candidate(
                connection,
                platform="telegram",
                external_id=external_id,
                access_type=access_type,
                display_name=f"Source {external_id}",
                provider="repository_seed",
                lineage_key=f"collector-fixture:{external_id}",
            )
            if approved:
                source = await self.sources.transition(
                    connection,
                    source.id,
                    SourceStatus.APPROVED,
                    reason="collector access fixture approved",
                )
            return source


@unittest.skipUnless(TEST_DATABASE_URL, "TEST_DATABASE_URL is not configured")
class CollectorAccessMigrationCompatibilityTest(unittest.TestCase):
    def test_existing_seed_sources_are_unchanged_across_access_migration(self):
        with temporary_database() as database_url:
            config = alembic_config(database_url)
            command.upgrade(config, "20260809_0004")

            first = asyncio.run(_import_seed(database_url))
            before = _source_snapshot(database_url)
            command.upgrade(config, "head")
            after = _source_snapshot(database_url)
            repeated = asyncio.run(_import_seed(database_url))

            self.assertEqual((first.created, first.total), (15, 15))
            self.assertEqual(before, after)
            self.assertEqual(len(after), 15)
            self.assertEqual(
                (repeated.created, repeated.updated, repeated.unchanged),
                (0, 0, 15),
            )


async def _import_seed(database_url):
    database = Database(database_url)
    try:
        return await SourceSeedImporter(database).import_file(SOURCES_PATH)
    finally:
        await database.close()


def _source_snapshot(database_url):
    engine = sa.create_engine(database_url)
    try:
        with engine.connect() as connection:
            return connection.execute(
                sa.select(
                    sources.c.id,
                    sources.c.platform,
                    sources.c.external_id,
                    sources.c.access_type,
                    sources.c.lifecycle_status,
                    sources.c.display_name,
                    sources.c.handle,
                    sources.c.canonical_url,
                ).order_by(sources.c.id)
            ).all()
    finally:
        engine.dispose()


if __name__ == "__main__":
    unittest.main()
