from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal
import unittest
from unittest.mock import patch

import sqlalchemy as sa

from freelancer_bot.config import RuntimeConfig, RuntimeMode
from freelancer_bot.persistence.collector_accounts import (
    CollectorAccessStatus,
    CollectorAccountRepository,
)
from freelancer_bot.persistence.database import Database
from freelancer_bot.persistence.schema import (
    source_audits,
    source_collector_access,
    source_discovery_lineage,
    source_lifecycle_events,
    source_quality_snapshots,
)
from freelancer_bot.persistence.source_metrics import (
    SourceHealthStatus,
    SourceMetricsRepository,
)
from freelancer_bot.persistence.source_repository import SourceRepository, SourceStatus
from freelancer_bot.source_audit import (
    SOURCE_AUDIT_SCHEMA_VERSION,
    SourceAuditClassification,
    SourceAuditPipeline,
)
from freelancer_bot.source_audit_sampler import (
    SourceAuditMessage,
    SourceAuditSampler,
)
from freelancer_bot.source_reaudit import (
    DefaultSourceReauditTargetResolver,
    SourceReauditPolicy,
    SourceReauditScheduler,
)
from postgres_support import TEST_DATABASE_URL, migrate_to_head, temporary_database


NOW = datetime(2026, 8, 10, 12, 0, tzinfo=timezone.utc)


class SourceMappedHistoryReader:
    def __init__(self, messages_by_source):
        self.messages_by_source = messages_by_source
        self.calls = []

    async def fetch_window(
        self,
        target,
        *,
        window_started_at,
        window_ended_at,
        limit,
    ):
        self.calls.append(
            (target.source_id, window_started_at, window_ended_at, limit)
        )
        return tuple(
            sorted(
                (
                    message
                    for message in self.messages_by_source.get(target.source_id, ())
                    if window_started_at <= message.occurred_at <= window_ended_at
                ),
                key=lambda message: (message.occurred_at, message.message_id),
                reverse=True,
            )[:limit]
        )


class SourceMappedAuditProvider:
    name = "reaudit_fixture"
    model = "replaceable-reaudit-model"
    analyzer_version = "reaudit-v1"

    def __init__(self, outcomes):
        self.outcomes = outcomes
        self.calls = []

    async def classify(self, sample):
        self.calls.append(sample.source_id)
        outcome = self.outcomes[sample.source_id]
        if isinstance(outcome, Exception):
            raise outcome
        return outcome


class SourceReauditPolicyTest(unittest.TestCase):
    def test_policy_enforces_configurable_seven_to_thirty_day_range(self):
        policy = SourceReauditPolicy()
        self.assertEqual(policy.degraded_cadence_days, 7)
        self.assertEqual(policy.normal_cadence_days, 14)
        self.assertEqual(policy.quiet_cadence_days, 30)

        with self.assertRaisesRegex(ValueError, "between 7 and 30"):
            SourceReauditPolicy(normal_cadence_days=6)
        with self.assertRaisesRegex(ValueError, "between 7 and 30"):
            SourceReauditPolicy(quiet_cadence_days=31)
        with self.assertRaisesRegex(ValueError, "below high"):
            SourceReauditPolicy(
                high_activity_messages_per_day=5,
                quiet_activity_messages_per_day=5,
            )

    def test_runtime_configuration_builds_replaceable_reaudit_policy(self):
        with patch.dict(
            "os.environ",
            {
                "SOURCE_REAUDIT_DEGRADED_CADENCE_DAYS": "8",
                "SOURCE_REAUDIT_HIGH_ACTIVITY_CADENCE_DAYS": "9",
                "SOURCE_REAUDIT_NORMAL_CADENCE_DAYS": "18",
                "SOURCE_REAUDIT_QUIET_CADENCE_DAYS": "29",
                "SOURCE_REAUDIT_HIGH_ACTIVITY_MESSAGES_PER_DAY": "75",
                "SOURCE_REAUDIT_QUIET_ACTIVITY_MESSAGES_PER_DAY": "3",
            },
            clear=True,
        ):
            config = RuntimeConfig.from_env(
                mode=RuntimeMode.CHECK_CONFIG,
                env_file=None,
            )
        policy = SourceReauditPolicy.from_config(config)
        self.assertEqual(
            (
                policy.degraded_cadence_days,
                policy.high_activity_cadence_days,
                policy.normal_cadence_days,
                policy.quiet_cadence_days,
            ),
            (8, 9, 18, 29),
        )
        self.assertEqual(policy.high_activity_messages_per_day, 75.0)
        self.assertEqual(policy.quiet_activity_messages_per_day, 3.0)


@unittest.skipUnless(TEST_DATABASE_URL, "TEST_DATABASE_URL is not configured")
class SourceReauditSchedulerTest(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.database_context = temporary_database()
        self.database_url = self.database_context.__enter__()
        migrate_to_head(self.database_url)
        self.database = Database(self.database_url, pool_size=4, max_overflow=8)
        self.sources = SourceRepository()
        self.metrics = SourceMetricsRepository()
        self.collectors = CollectorAccountRepository()

    async def asyncTearDown(self):
        await self.database.close()
        self.database_context.__exit__(None, None, None)

    async def test_activity_health_cadence_reaudits_due_and_pauses_degraded(self):
        account = await self._account()
        degraded = await self._approved("degraded")
        never = await self._approved("never")
        high = await self._approved("high")
        quiet = await self._approved("quiet")
        normal_recent = await self._approved("normal-recent")
        private_without_access = await self._approved(
            "private-without-access",
            access_type="private",
        )
        manually_paused = await self._approved("manually-paused")
        async with self.database.transaction() as connection:
            await self._health(
                connection,
                degraded.id,
                last_audited_at=NOW - timedelta(days=8),
                messages_per_day=20,
                health_status=SourceHealthStatus.DEGRADED,
            )
            await self._health(
                connection,
                high.id,
                last_audited_at=NOW - timedelta(days=8),
                messages_per_day=100,
            )
            await self._health(
                connection,
                quiet.id,
                last_audited_at=NOW - timedelta(days=31),
                messages_per_day=1,
            )
            await self._health(
                connection,
                normal_recent.id,
                last_audited_at=NOW - timedelta(days=8),
                messages_per_day=20,
            )
            manually_paused = await self.sources.override(
                connection,
                manually_paused.id,
                SourceStatus.PAUSED,
                operator_id="operator-reaudit",
                reason="manual maintenance",
            )

        due_ids = {degraded.id, never.id, high.id, quiet.id}
        reader = SourceMappedHistoryReader(
            {source_id: _messages(source_id, 40) for source_id in due_ids}
        )
        provider = SourceMappedAuditProvider(
            {
                degraded.id: _classification(40, opportunities=5),
                never.id: _classification(40, opportunities=4),
                high.id: _classification(40, opportunities=0),
                quiet.id: _classification(40, opportunities=3),
            }
        )
        pipeline = SourceAuditPipeline(
            self.database,
            SourceAuditSampler(reader),
            provider,
        )
        scheduler = SourceReauditScheduler(
            self.database,
            pipeline,
            collector_account_id=account.id,
        )

        batch = await scheduler.run_once(as_of=NOW)
        repeated = await scheduler.run_once(as_of=NOW)

        self.assertEqual(
            [(item.source_id, item.cadence_days, item.due_reason) for item in batch.due],
            [
                (degraded.id, 7, "degraded"),
                (never.id, 14, "never_audited"),
                (high.id, 7, "high_activity"),
                (quiet.id, 30, "quiet_activity"),
            ],
        )
        self.assertEqual(len(batch.completed), 4)
        self.assertEqual(batch.failures, ())
        self.assertEqual(repeated.due, ())
        self.assertTrue(all(call[3] <= 151 for call in reader.calls))
        self.assertEqual({call[0] for call in reader.calls}, due_ids)
        self.assertNotIn(private_without_access.id, provider.calls)
        self.assertNotIn(normal_recent.id, provider.calls)
        self.assertNotIn(manually_paused.id, provider.calls)

        async with self.database.connect() as connection:
            degraded_health = await self.metrics.get_health(connection, degraded.id)
            high_health = await self.metrics.get_health(connection, high.id)
            high_source = await self.sources.get(connection, high.id)
            paused_events = (
                await connection.execute(
                    sa.select(source_lifecycle_events).where(
                        source_lifecycle_events.c.source_id == high.id,
                        source_lifecycle_events.c.to_status == "paused",
                    )
                )
            ).mappings().all()
            audit_count = await connection.scalar(
                sa.select(sa.func.count()).select_from(source_audits)
            )
            metric_count = await connection.scalar(
                sa.select(sa.func.count()).select_from(source_quality_snapshots)
            )
            private_access_count = await connection.scalar(
                sa.select(sa.func.count())
                .select_from(source_collector_access)
                .where(source_collector_access.c.source_id == private_without_access.id)
            )
            paused_lineage = await connection.scalar(
                sa.select(sa.func.count())
                .select_from(source_discovery_lineage)
                .where(source_discovery_lineage.c.source_id == manually_paused.id)
            )

        self.assertEqual(degraded_health.health_status, SourceHealthStatus.HEALTHY)
        self.assertEqual(high_health.health_status, SourceHealthStatus.DEGRADED)
        self.assertIn("review.low_opportunity_yield", high_health.degradation_reason)
        self.assertEqual(high_source.lifecycle_status, SourceStatus.PAUSED)
        self.assertEqual(len(paused_events), 1)
        self.assertIsNotNone(paused_events[0]["source_audit_id"])
        self.assertEqual(audit_count, 4)
        self.assertEqual(metric_count, 4)
        self.assertEqual(private_access_count, 0)
        self.assertEqual(manually_paused.lifecycle_status, SourceStatus.PAUSED)
        self.assertEqual(paused_lineage, 1)

    async def test_private_permission_lookup_and_provider_failures_are_isolated(self):
        account = await self._account()
        private = await self._approved(
            "private-permitted",
            access_type="private",
            external_id="invite_sha256:opaque",
            handle=None,
        )
        failing = await self._approved("provider-failure")
        succeeding = await self._approved("provider-success")
        async with self.database.transaction() as connection:
            await self.collectors.record_source_access(
                connection,
                source_id=private.id,
                collector_account_id=account.id,
                access_status=CollectorAccessStatus.PERMITTED,
                checked_at=NOW - timedelta(days=1),
                checked_by="access-checker",
            )

        reader = SourceMappedHistoryReader(
            {
                failing.id: _messages(failing.id, 40),
                succeeding.id: _messages(succeeding.id, 40),
            }
        )
        provider = SourceMappedAuditProvider(
            {
                failing.id: RuntimeError("protected provider failure detail"),
                succeeding.id: _classification(40, opportunities=5),
            }
        )
        scheduler = SourceReauditScheduler(
            self.database,
            SourceAuditPipeline(
                self.database,
                SourceAuditSampler(reader),
                provider,
            ),
            collector_account_id=account.id,
        )

        batch = await scheduler.run_once(as_of=NOW)

        self.assertEqual({item.source_id for item in batch.due}, {private.id, failing.id, succeeding.id})
        self.assertEqual([item.source.id for item in batch.completed], [succeeding.id])
        self.assertEqual(
            {(failure.source_id, failure.code) for failure in batch.failures},
            {
                (private.id, "lookup_unavailable"),
                (failing.id, "runtime_error"),
            },
        )
        self.assertNotIn("protected", repr(batch.failures))
        async with self.database.connect() as connection:
            private_access = await connection.scalar(
                sa.select(source_collector_access.c.access_status).where(
                    source_collector_access.c.source_id == private.id,
                    source_collector_access.c.collector_account_id == account.id,
                )
            )
            failing_audits = await connection.scalar(
                sa.select(sa.func.count())
                .select_from(source_audits)
                .where(source_audits.c.source_id == failing.id)
            )
        self.assertEqual(private_access, CollectorAccessStatus.PERMITTED.value)
        self.assertEqual(failing_audits, 0)

    async def _account(self):
        async with self.database.transaction() as connection:
            return await self.collectors.ensure(
                connection,
                platform="telegram",
                external_account_id="reaudit-account",
                display_name="Re-audit collector",
            )

    async def _approved(
        self,
        suffix,
        *,
        access_type="public",
        external_id=None,
        handle="default",
    ):
        normalized_handle = None if handle is None else f"@reaudit_{suffix}"
        async with self.database.transaction() as connection:
            source = await self.sources.create_candidate(
                connection,
                platform="telegram",
                external_id=external_id or f"reaudit:{suffix}",
                access_type=access_type,
                display_name=f"Re-audit source {suffix}",
                handle=normalized_handle,
                provider="web_search",
                lineage_key=f"reaudit-lineage:{suffix}",
            )
            return await self.sources.transition(
                connection,
                source.id,
                SourceStatus.APPROVED,
                reason="reaudit fixture approved",
            )

    async def _health(
        self,
        connection,
        source_id,
        *,
        last_audited_at,
        messages_per_day,
        health_status=SourceHealthStatus.HEALTHY,
    ):
        await self.metrics.record_activity(
            connection,
            source_id=source_id,
            observed_at=NOW - timedelta(days=1),
            last_message_at=NOW - timedelta(days=1, hours=1),
            messages_per_day=messages_per_day,
            opportunities_per_day=Decimal("1.0"),
        )
        await self.metrics.record_audit_completed(
            connection,
            source_id=source_id,
            audited_at=last_audited_at,
        )
        await self.metrics.set_health_status(
            connection,
            source_id=source_id,
            health_status=health_status,
            changed_at=NOW - timedelta(days=2),
            reason=(
                "health fixture degraded"
                if health_status is SourceHealthStatus.DEGRADED
                else None
            ),
        )


def _messages(source_id, count):
    return tuple(
        SourceAuditMessage(
            source_id * 1000 + index,
            NOW - timedelta(minutes=index * 10),
            f"Source {source_id} message {index}",
        )
        for index in range(1, count + 1)
    )


def _classification(count, *, opportunities):
    return SourceAuditClassification.model_validate(
        {
            "schema_version": SOURCE_AUDIT_SCHEMA_VERSION,
            "analyzed_message_count": count,
            "commercial_opportunity_count": opportunities,
            "buyer_intent_count": min(opportunities + 2, count),
            "seller_promotion_count": 2,
            "ads_spam_count": 1,
            "duplicate_count": 1,
            "content_mix": {"requests": 0.6, "discussion": 0.4},
            "primary_language": "ru",
            "languages": [{"key": "ru", "display_name": "Russian"}],
            "categories": [{"key": "software", "display_name": "Software"}],
        }
    )


if __name__ == "__main__":
    unittest.main()
