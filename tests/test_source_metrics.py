import asyncio
import unittest
from datetime import datetime, timedelta, timezone
from decimal import Decimal

import sqlalchemy as sa
from alembic import command
from sqlalchemy.exc import IntegrityError

from freelancer_bot.persistence.database import Database
from freelancer_bot.persistence.schema import (
    source_health,
    source_quality_snapshots,
    sources,
)
from freelancer_bot.persistence.source_metrics import (
    SourceHealthStatus,
    SourceMetricConflict,
    SourceMetricsRepository,
)
from freelancer_bot.persistence.source_repository import SourceRepository, SourceStatus
from freelancer_bot.persistence.source_seed import SourceSeedImporter
from postgres_support import (
    ROOT,
    TEST_DATABASE_URL,
    alembic_config,
    migrate_to_head,
    temporary_database,
)


SOURCES_PATH = ROOT / "config" / "sources.json"
NOW = datetime(2026, 8, 9, 12, 0, tzinfo=timezone.utc)


@unittest.skipUnless(TEST_DATABASE_URL, "TEST_DATABASE_URL is not configured")
class SourceMetricsRepositoryTest(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.database_context = temporary_database()
        self.database_url = self.database_context.__enter__()
        migrate_to_head(self.database_url)
        self.database = Database(self.database_url, pool_size=4, max_overflow=8)
        self.metrics = SourceMetricsRepository()
        self.sources = SourceRepository()

    async def asyncTearDown(self):
        await self.database.close()
        self.database_context.__exit__(None, None, None)

    async def test_quality_snapshots_are_source_level_idempotent_and_immutable(self):
        source = await self._create_source("quality", SourceStatus.APPROVED)
        values = self._snapshot_values(source.id, "audit:quality:1", NOW)

        async with self.database.transaction() as connection:
            first = await self.metrics.record_quality_snapshot(connection, **values)
            repeated = await self.metrics.record_quality_snapshot(connection, **values)
            health = await self.metrics.get_health(connection, source.id)
            snapshots = await self.metrics.list_quality_snapshots(connection, source.id)

        self.assertEqual(first.id, repeated.id)
        self.assertEqual(len(snapshots), 1)
        self.assertEqual(first.opportunity_yield, Decimal("0.2500000"))
        self.assertEqual(first.seller_ratio, Decimal("0.1500000"))
        self.assertEqual(health.last_audited_at, NOW)
        self.assertNotIn("opportunity_quality", source_quality_snapshots.c)
        self.assertNotIn("user_relevance", source_quality_snapshots.c)

        conflicting = dict(values, spam_ratio=Decimal("0.9"))
        with self.assertRaises(SourceMetricConflict):
            async with self.database.transaction() as connection:
                await self.metrics.record_quality_snapshot(connection, **conflicting)

        with self.assertRaises(IntegrityError):
            async with self.database.transaction() as connection:
                await connection.execute(
                    sa.delete(sources).where(sources.c.id == source.id)
                )

    async def test_activity_and_health_status_do_not_regress_from_stale_updates(self):
        source = await self._create_source("activity", SourceStatus.APPROVED)
        first_observed = NOW - timedelta(days=2)
        first_message = first_observed - timedelta(minutes=10)

        async with self.database.transaction() as connection:
            await self.metrics.record_activity(
                connection,
                source_id=source.id,
                observed_at=first_observed,
                last_message_at=first_message,
                messages_per_day=20,
                opportunities_per_day=2,
            )
            stale = await self.metrics.record_activity(
                connection,
                source_id=source.id,
                observed_at=first_observed - timedelta(days=1),
                last_message_at=first_message - timedelta(days=1),
                messages_per_day=1,
                opportunities_per_day=0,
            )
        self.assertEqual(stale.messages_per_day, Decimal("20.0000"))
        self.assertEqual(stale.last_message_at, first_message)

        newer_observed = NOW - timedelta(days=1)
        async with self.database.transaction() as connection:
            activity = await self.metrics.record_activity(
                connection,
                source_id=source.id,
                observed_at=newer_observed,
                last_message_at=first_message - timedelta(hours=1),
                messages_per_day=12.5,
                opportunities_per_day=1.25,
            )
            degraded = await self.metrics.set_health_status(
                connection,
                source_id=source.id,
                health_status=SourceHealthStatus.DEGRADED,
                changed_at=newer_observed,
                reason="opportunity yield dropped",
            )
        self.assertEqual(activity.last_message_at, first_message)
        self.assertEqual(activity.messages_per_day, Decimal("12.5000"))
        self.assertEqual(degraded.health_status, SourceHealthStatus.DEGRADED)
        self.assertEqual(degraded.degradation_reason, "opportunity yield dropped")

        async with self.database.transaction() as connection:
            stale_status = await self.metrics.set_health_status(
                connection,
                source_id=source.id,
                health_status=SourceHealthStatus.HEALTHY,
                changed_at=newer_observed - timedelta(hours=1),
            )
            healthy = await self.metrics.set_health_status(
                connection,
                source_id=source.id,
                health_status=SourceHealthStatus.HEALTHY,
                changed_at=NOW,
            )
        self.assertEqual(stale_status.health_status, SourceHealthStatus.DEGRADED)
        self.assertEqual(healthy.health_status, SourceHealthStatus.HEALTHY)
        self.assertIsNone(healthy.degraded_at)
        self.assertIsNone(healthy.degradation_reason)

    async def test_reaudit_query_selects_stale_and_degraded_operational_sources(self):
        missing = await self._create_source("missing", SourceStatus.APPROVED)
        recent = await self._create_source("recent", SourceStatus.APPROVED)
        degraded = await self._create_source("degraded", SourceStatus.APPROVED)
        stale = await self._create_source("stale", SourceStatus.APPROVED)
        paused = await self._create_source("paused", SourceStatus.PAUSED)
        candidate = await self._create_source("candidate", SourceStatus.CANDIDATE)

        async with self.database.transaction() as connection:
            await self.metrics.record_quality_snapshot(
                connection,
                **self._snapshot_values(
                    recent.id,
                    "audit:recent",
                    NOW - timedelta(days=2),
                ),
            )
            await self.metrics.record_quality_snapshot(
                connection,
                **self._snapshot_values(
                    degraded.id,
                    "audit:degraded",
                    NOW - timedelta(days=1),
                ),
            )
            await self.metrics.set_health_status(
                connection,
                source_id=degraded.id,
                health_status=SourceHealthStatus.DEGRADED,
                changed_at=NOW - timedelta(hours=12),
                reason="spam ratio increased",
            )
            await self.metrics.record_quality_snapshot(
                connection,
                **self._snapshot_values(
                    stale.id,
                    "audit:stale",
                    NOW - timedelta(days=45),
                ),
            )
            await self.metrics.set_health_status(
                connection,
                source_id=paused.id,
                health_status=SourceHealthStatus.DEGRADED,
                changed_at=NOW - timedelta(days=3),
                reason="source activity stopped",
            )

        async with self.database.connect() as connection:
            due = await self.metrics.list_due_for_reaudit(
                connection,
                as_of=NOW,
                stale_after=timedelta(days=30),
            )
            limited = await self.metrics.list_due_for_reaudit(
                connection,
                as_of=NOW,
                stale_after=timedelta(days=30),
                limit=2,
            )

        self.assertEqual(
            [item.source_id for item in due],
            [paused.id, degraded.id, missing.id, stale.id],
        )
        self.assertEqual([item.source_id for item in limited], [paused.id, degraded.id])
        self.assertNotIn(recent.id, [item.source_id for item in due])
        self.assertNotIn(candidate.id, [item.source_id for item in due])

    async def test_metric_validation_and_database_constraints_reject_invalid_state(self):
        source = await self._create_source("constraints", SourceStatus.APPROVED)
        naive = dict(self._snapshot_values(source.id, "audit:naive", NOW))
        naive["audited_at"] = NOW.replace(tzinfo=None)
        with self.assertRaisesRegex(ValueError, "timezone"):
            async with self.database.transaction() as connection:
                await self.metrics.record_quality_snapshot(connection, **naive)

        invalid_ratio = dict(self._snapshot_values(source.id, "audit:ratio", NOW))
        invalid_ratio["duplicate_ratio"] = Decimal("1.01")
        with self.assertRaisesRegex(ValueError, "between 0 and 1"):
            async with self.database.transaction() as connection:
                await self.metrics.record_quality_snapshot(connection, **invalid_ratio)

        with self.assertRaisesRegex(ValueError, "requires a reason"):
            async with self.database.transaction() as connection:
                await self.metrics.set_health_status(
                    connection,
                    source_id=source.id,
                    health_status=SourceHealthStatus.DEGRADED,
                    changed_at=NOW,
                )
        with self.assertRaisesRegex(ValueError, "implicit"):
            async with self.database.transaction() as connection:
                await self.metrics.set_health_status(
                    connection,
                    source_id=source.id,
                    health_status=SourceHealthStatus.UNKNOWN,
                    changed_at=NOW,
                )

        with self.assertRaises(IntegrityError):
            async with self.database.transaction() as connection:
                await connection.execute(
                    sa.insert(source_health).values(
                        source_id=source.id,
                        health_status="degraded",
                        status_changed_at=NOW,
                    )
                )
        with self.assertRaises(IntegrityError):
            async with self.database.transaction() as connection:
                await connection.execute(
                    sa.insert(source_health).values(
                        source_id=source.id,
                        health_status="healthy",
                    )
                )

    async def _create_source(self, external_id: str, status: SourceStatus):
        async with self.database.transaction() as connection:
            source = await self.sources.create_candidate(
                connection,
                platform="telegram",
                external_id=f"metrics:{external_id}",
                access_type="public",
                display_name=f"Metrics source {external_id}",
                provider="repository_seed",
                lineage_key=f"metrics-fixture:{external_id}",
            )
            if status is SourceStatus.APPROVED:
                return await self.sources.transition(
                    connection,
                    source.id,
                    SourceStatus.APPROVED,
                    reason="metrics fixture approved",
                )
            if status is SourceStatus.PAUSED:
                await self.sources.transition(
                    connection,
                    source.id,
                    SourceStatus.APPROVED,
                    reason="metrics fixture approved",
                )
                return await self.sources.transition(
                    connection,
                    source.id,
                    SourceStatus.PAUSED,
                    reason="metrics fixture paused",
                )
            return source

    def _snapshot_values(self, source_id: int, audit_key: str, audited_at: datetime):
        return {
            "source_id": source_id,
            "audit_key": audit_key,
            "audited_at": audited_at,
            "window_started_at": audited_at - timedelta(days=3),
            "window_ended_at": audited_at - timedelta(minutes=5),
            "sampled_message_count": 100,
            "opportunity_yield": Decimal("0.25000004"),
            "buyer_intent_ratio": Decimal("0.30"),
            "seller_ratio": Decimal("0.15"),
            "spam_ratio": Decimal("0.10"),
            "duplicate_ratio": Decimal("0.05"),
        }


@unittest.skipUnless(TEST_DATABASE_URL, "TEST_DATABASE_URL is not configured")
class SourceMetricsMigrationCompatibilityTest(unittest.TestCase):
    def test_existing_seed_sources_are_unchanged_across_metrics_migration(self):
        with temporary_database() as database_url:
            config = alembic_config(database_url)
            command.upgrade(config, "20260809_0005")

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
