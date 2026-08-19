from hashlib import sha256
from datetime import datetime, timedelta, timezone
import unittest
from uuid import uuid4

import sqlalchemy as sa
from alembic import command

from freelancer_bot.persistence.schema import (
    collector_accounts,
    durable_jobs,
    opportunities,
    opportunity_lifecycle_events,
    opportunity_source_messages,
    raw_messages,
    sources,
)
from freelancer_bot.billing import TRIAL_POLICY_VERSION
from postgres_support import TEST_DATABASE_URL, alembic_config, temporary_database


@unittest.skipUnless(TEST_DATABASE_URL, "TEST_DATABASE_URL is not configured")
class PostgresMigrationTest(unittest.TestCase):
    def test_clean_upgrade_head_check_downgrade_and_reupgrade(self):
        with temporary_database() as database_url:
            config = alembic_config(database_url)

            command.upgrade(config, "head")
            command.current(config, check_heads=True)
            command.check(config)
            self.assertEqual(_domain_tables(database_url), EXPECTED_TABLES)

            command.downgrade(config, "base")
            self.assertEqual(_domain_tables(database_url), set())

            command.upgrade(config, "head")
            command.current(config, check_heads=True)
            command.check(config)
            self.assertEqual(_domain_tables(database_url), EXPECTED_TABLES)

    def test_trial_entitlement_migration_backfills_existing_trial_start(self):
        with temporary_database() as database_url:
            config = alembic_config(database_url)
            command.upgrade(config, "20260814_0021")
            started_at = datetime(2026, 8, 14, 20, 0, tzinfo=timezone.utc)
            engine = sa.create_engine(database_url)
            try:
                with engine.begin() as connection:
                    connection.execute(
                        sa.text(
                            "INSERT INTO users "
                            "(id, platform, external_user_id, created_at, trial_started_at) "
                            "VALUES (:id, 'telegram', 'migration-trial', "
                            ":created_at, :trial_started_at)"
                        ),
                        {
                            "id": uuid4(),
                            "created_at": started_at,
                            "trial_started_at": started_at,
                        },
                    )
                command.upgrade(config, "head")
                with engine.connect() as connection:
                    row = connection.execute(
                        sa.text(
                            "SELECT trial_started_at, trial_expires_at, "
                            "trial_policy_version FROM users "
                            "WHERE external_user_id = 'migration-trial'"
                        )
                    ).mappings().one()
                self.assertEqual(row["trial_started_at"], started_at)
                self.assertEqual(
                    row["trial_expires_at"],
                    started_at + timedelta(days=3),
                )
                self.assertEqual(row["trial_policy_version"], TRIAL_POLICY_VERSION)
            finally:
                engine.dispose()

    def test_opportunity_dedup_migration_backfills_existing_link_in_place(self):
        with temporary_database() as database_url:
            config = alembic_config(database_url)
            command.upgrade(config, "20260809_0013")
            opportunity_id = uuid4()
            cache_id = uuid4()
            engine = sa.create_engine(database_url)
            try:
                with engine.begin() as connection:
                    connection.execute(
                        sa.text(
                            "INSERT INTO opportunity_analysis_cache "
                            "(id, normalized_content, normalized_content_sha256, "
                            "analysis_input_sha256, analyzer_version, "
                            "analysis_schema_version, result) VALUES "
                            "(:id, :content, :content_hash, :input_hash, "
                            "'fixture-analyzer.v1', 'opportunity_analysis.v1', "
                            "'{}'::jsonb)"
                        ),
                        {
                            "id": cache_id,
                            "content": "Need Telegram BOT!!!",
                            "content_hash": "a" * 64,
                            "input_hash": "b" * 64,
                        },
                    )
                    connection.execute(
                        sa.text(
                            "INSERT INTO opportunities "
                            "(id, schema_version, market_direction, intent_stage, "
                            "opportunity_type, skills, budget_known, budget_explicit, "
                            "analysis_confidence, quality_actionability, "
                            "quality_commercial_plausibility, quality_specificity, "
                            "quality_credibility, red_flags, first_seen_at, last_seen_at) "
                            "VALUES (:id, 'canonical_opportunity.v1', "
                            "'buyer_to_specialist', 'active', 'project', '[]'::jsonb, "
                            "false, false, 0.9, 0.8, 0.8, 0.8, 0.8, '[]'::jsonb, "
                            "'2026-08-09T12:00:00+00:00', "
                            "'2026-08-09T12:00:00+00:00')"
                        ),
                        {"id": opportunity_id},
                    )
                    connection.execute(
                        sa.text(
                            "INSERT INTO opportunity_analysis_links "
                            "(analysis_cache_id, opportunity_id) VALUES "
                            "(:cache_id, :opportunity_id)"
                        ),
                        {
                            "cache_id": cache_id,
                            "opportunity_id": opportunity_id,
                        },
                    )

                command.upgrade(config, "head")
                with engine.connect() as connection:
                    row = connection.execute(
                        sa.text(
                            "SELECT * FROM opportunity_analysis_links "
                            "WHERE analysis_cache_id = :cache_id"
                        ),
                        {"cache_id": cache_id},
                    ).mappings().one()
                self.assertEqual(row["opportunity_id"], opportunity_id)
                self.assertEqual(row["dedup_relation"], "canonical")
                self.assertEqual(
                    row["dedup_algorithm_version"],
                    "canonical-opportunity-dedup.v1",
                )
                self.assertEqual(
                    row["normalized_text_sha256"],
                    sha256(b"need telegram bot").hexdigest(),
                )
                self.assertEqual(row["dedup_window_seconds"], 7 * 24 * 60 * 60)

                command.downgrade(config, "20260809_0013")
                with engine.connect() as connection:
                    preserved = connection.execute(
                        sa.text(
                            "SELECT opportunity_id FROM opportunity_analysis_links "
                            "WHERE analysis_cache_id = :cache_id"
                        ),
                        {"cache_id": cache_id},
                    ).scalar_one()
                self.assertEqual(preserved, opportunity_id)
            finally:
                engine.dispose()

    def test_preferred_source_migration_backfills_existing_observations(self):
        with temporary_database() as database_url:
            config = alembic_config(database_url)
            command.upgrade(config, "20260809_0014")
            opportunity_id = uuid4()
            older_raw_id = uuid4()
            newer_raw_id = uuid4()
            older_job_id = uuid4()
            newer_job_id = uuid4()
            correlation_id = uuid4()
            engine = sa.create_engine(database_url)
            try:
                with engine.begin() as connection:
                    collector_id = connection.scalar(
                        collector_accounts.insert()
                        .values(
                            platform="telegram",
                            external_account_id="migration-fixture",
                            display_name="Migration fixture",
                        )
                        .returning(collector_accounts.c.id)
                    )
                    source_ids = []
                    for external_id, handle in (
                        ("migration-source-new", "migration_new"),
                        ("migration-source-old", "migration_old"),
                    ):
                        source_ids.append(
                            connection.scalar(
                                sources.insert()
                                .values(
                                    platform="telegram",
                                    external_id=external_id,
                                    access_type="public",
                                    lifecycle_status="approved",
                                    display_name=external_id,
                                    handle=handle,
                                    canonical_url=f"https://t.me/{handle}",
                                )
                                .returning(sources.c.id)
                            )
                        )
                    for job_id, key in (
                        (older_job_id, "migration-old"),
                        (newer_job_id, "migration-new"),
                    ):
                        connection.execute(
                            durable_jobs.insert().values(
                                id=job_id,
                                job_type="telegram.raw_message.v1",
                                idempotency_key=key,
                                correlation_id=correlation_id,
                            )
                        )
                    for raw_id, source_id, job_id, message_id, message_date in (
                        (
                            newer_raw_id,
                            source_ids[0],
                            newer_job_id,
                            902,
                            "2026-08-09T14:00:00+00:00",
                        ),
                        (
                            older_raw_id,
                            source_ids[1],
                            older_job_id,
                            901,
                            "2026-08-09T12:00:00+00:00",
                        ),
                    ):
                        connection.execute(
                            raw_messages.insert().values(
                                id=raw_id,
                                source_id=source_id,
                                collector_account_id=collector_id,
                                processing_job_id=job_id,
                                schema_version="telegram.raw_message.v1",
                                platform="telegram",
                                external_source_id=str(source_id),
                                external_message_id=message_id,
                                message_date=message_date,
                                observed_at="2026-08-09T15:00:00+00:00",
                                message_url=f"https://t.me/c/{source_id}/{message_id}",
                                content="migration fixture",
                                transport_metadata={},
                                ingestion_origin="live",
                                correlation_id=correlation_id,
                            )
                        )
                    connection.execute(
                        opportunities.insert().values(
                            id=opportunity_id,
                            schema_version="canonical_opportunity.v1",
                            market_direction="buyer_to_specialist",
                            intent_stage="active",
                            opportunity_type="project",
                            skills=[],
                            budget_known=False,
                            budget_explicit=False,
                            analysis_confidence=0.9,
                            quality_actionability=0.8,
                            quality_commercial_plausibility=0.8,
                            quality_specificity=0.8,
                            quality_credibility=0.8,
                            red_flags=[],
                            first_seen_at="2026-08-09T12:00:00+00:00",
                            last_seen_at="2026-08-09T15:00:00+00:00",
                        )
                    )
                    connection.execute(
                        opportunity_source_messages.insert(),
                        (
                            {
                                "raw_message_id": newer_raw_id,
                                "opportunity_id": opportunity_id,
                            },
                            {
                                "raw_message_id": older_raw_id,
                                "opportunity_id": opportunity_id,
                            },
                        ),
                    )

                command.upgrade(config, "head")
                with engine.connect() as connection:
                    preferred = connection.execute(
                        sa.text(
                            "SELECT preferred_raw_message_id, lifecycle_status, "
                            "preferred_source_policy_version FROM opportunities "
                            "WHERE id = :opportunity_id"
                        ),
                        {"opportunity_id": opportunity_id},
                    ).mappings().one()
                    observation_count = connection.scalar(
                        sa.select(sa.func.count())
                        .select_from(opportunity_source_messages)
                        .where(
                            opportunity_source_messages.c.opportunity_id
                            == opportunity_id
                        )
                    )
                    lifecycle = connection.execute(
                        sa.select(opportunity_lifecycle_events).where(
                            opportunity_lifecycle_events.c.opportunity_id
                            == opportunity_id
                        )
                    ).mappings().one()
                self.assertEqual(preferred["preferred_raw_message_id"], older_raw_id)
                self.assertEqual(
                    preferred["preferred_source_policy_version"],
                    "canonical-source-earliest-message.v1",
                )
                self.assertEqual(preferred["lifecycle_status"], "active")
                self.assertEqual(observation_count, 2)
                self.assertIsNone(lifecycle["from_status"])
                self.assertEqual(lifecycle["to_status"], "active")
                self.assertEqual(lifecycle["actor_kind"], "migration")
                self.assertIsNone(lifecycle["evidence_raw_message_id"])
            finally:
                engine.dispose()


EXPECTED_TABLES = {
    "ai_call_telemetry",
    "collector_accounts",
    "telegram_collector_operation_state",
    "telegram_collector_operation_events",
    "discovery_results",
    "discovery_runs",
    "web_provider_health",
    "discovery_campaigns",
    "discovery_campaign_queries",
    "discovery_campaign_profiles",
    "source_reference_aliases",
    "source_discovery_evidence",
    "telegram_source_validations",
    "source_monitoring_assignments",
    "discovery_cost_events",
    "delivery_action_events",
    "feedback_events",
    "durable_jobs",
    "source_discovery_lineage",
    "source_lifecycle_events",
    "source_collector_access",
    "source_health",
    "source_audits",
    "source_quality_snapshots",
    "source_feedback_signals",
    "source_profile_relevance",
    "sources",
    "telegram_chat_discovery_topics",
    "telegram_chat_discovery_search_runs",
    "telegram_chat_discovery_peers",
    "telegram_chat_discovery_peer_aliases",
    "telegram_chat_discovery_observations",
    "telegram_chat_discovery_screen_attempts",
    "source_taxonomy_assignments",
    "source_taxonomy_terms",
    "subscribers",
    "legacy_import_runs",
    "legacy_processed_messages",
    "legacy_recipient_deliveries",
    "match_evaluation_runs",
    "match_traces",
    "message_prefilter_results",
    "opportunity_analysis_cache",
    "opportunities",
    "opportunity_analysis_links",
    "opportunity_lifecycle_events",
    "opportunity_source_messages",
    "personalized_deliveries",
    "payment_provider_events",
    "profile_discovery_intents",
    "raw_messages",
    "search_profiles",
    "search_profile_analysis_cache",
    "search_profile_onboarding_attempts",
    "subscription_state_events",
    "subscription_states",
    "subscription_periods",
    "users",
}


def _domain_tables(database_url: str) -> set[str]:
    engine = sa.create_engine(database_url)
    try:
        return set(sa.inspect(engine).get_table_names()) - {"alembic_version"}
    finally:
        engine.dispose()


if __name__ == "__main__":
    unittest.main()
