from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal
import logging
import unittest
from uuid import UUID, uuid4

import sqlalchemy as sa

from freelancer_bot.config import RuntimeConfig
from freelancer_bot.ingestion_runtime import _build_worker
from freelancer_bot.persistence.database import Database
from freelancer_bot.persistence.jobs import JobClaim
from freelancer_bot.persistence.matches import MatchTraceRepository
from freelancer_bot.persistence.schema import (
    ai_call_telemetry,
    durable_jobs,
    match_evaluation_runs,
    opportunities,
    opportunity_analysis_cache,
    opportunity_analysis_links,
    personalized_deliveries,
)
from freelancer_bot.profile_confirmation import ProfileConfirmationService
from freelancer_bot.profile_rematch import (
    PROFILE_REMATCH_JOB_TYPE,
    ProfileRematchJobProcessor,
    parse_profile_rematch_job_key,
    profile_rematch_job_key,
)
from postgres_support import TEST_DATABASE_URL, migrate_to_head, temporary_database


@unittest.skipUnless(TEST_DATABASE_URL, "TEST_DATABASE_URL is not configured")
class ProfileRematchPostgresTest(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.database_context = temporary_database()
        self.database_url = self.database_context.__enter__()
        migrate_to_head(self.database_url)
        self.database = Database(self.database_url, pool_size=4, max_overflow=8)
        self.service = ProfileConfirmationService(self.database)
        self.config = RuntimeConfig(
            _env_file=None,
            database_url=self.database_url,
            app_environment="test",
            worker_poll_interval_seconds=0.005,
            worker_lease_seconds=1.0,
            worker_heartbeat_seconds=0.05,
            worker_retry_delay_seconds=0,
            worker_shutdown_timeout_seconds=0.2,
        )

    async def asyncTearDown(self):
        await self.database.close()
        self.database_context.__exit__(None, None, None)

    async def test_stale_profile_revision_is_skipped_without_match_run(self):
        draft = await self.service.create_manual_draft(
            platform="telegram",
            external_user_id="2001",
            semantic_text="Python developer | Python | Telegram",
            roles=("Python developer",),
            skills=("Python",),
            categories=("Telegram",),
        )
        confirmed = await self.service.confirm(
            platform="telegram",
            external_user_id="2001",
            profile_id=draft.profile.id,
            expected_revision=draft.profile.revision,
        )
        activated = await self.service.activate(
            platform="telegram",
            external_user_id="2001",
            profile_id=confirmed.profile.id,
            expected_revision=confirmed.profile.revision,
        )
        async with self.database.transaction() as connection:
            job = (
                await connection.execute(
                    sa.select(durable_jobs).where(
                        durable_jobs.c.job_type == PROFILE_REMATCH_JOB_TYPE,
                        durable_jobs.c.idempotency_key
                        == profile_rematch_job_key(
                            activated.profile.profile.id,
                            activated.profile.profile.revision,
                        ),
                    )
                )
            ).mappings().one()
        await self.service.deactivate(
            platform="telegram",
            external_user_id="2001",
            profile_id=activated.profile.profile.id,
            expected_revision=activated.profile.profile.revision,
        )

        result = await ProfileRematchJobProcessor(
            self.database,
            self.config,
        ).process(_claim(job))

        self.assertTrue(result.skipped)
        async with self.database.connect() as connection:
            self.assertEqual(
                await connection.scalar(sa.select(sa.func.count()).select_from(match_evaluation_runs)),
                0,
            )

    async def test_recent_opportunity_is_evaluated_only_for_activated_profile(self):
        draft = await self.service.create_manual_draft(
            platform="telegram",
            external_user_id="2002",
            semantic_text="Python developer | Python | Telegram",
            roles=("Python developer",),
            skills=("Python",),
            categories=("Telegram",),
        )
        confirmed = await self.service.confirm(
            platform="telegram",
            external_user_id="2002",
            profile_id=draft.profile.id,
            expected_revision=draft.profile.revision,
        )
        opportunity_id = await self._insert_opportunity(
            last_seen_at=datetime.now(timezone.utc) - timedelta(hours=1),
            lifecycle_status="active",
        )
        old_opportunity_id = await self._insert_opportunity(
            last_seen_at=datetime.now(timezone.utc) - timedelta(days=8),
            lifecycle_status="active",
        )
        closed_opportunity_id = await self._insert_opportunity(
            last_seen_at=datetime.now(timezone.utc) - timedelta(hours=1),
            lifecycle_status="closed",
        )
        activated = await self.service.activate(
            platform="telegram",
            external_user_id="2002",
            profile_id=confirmed.profile.id,
            expected_revision=confirmed.profile.revision,
        )
        async with self.database.connect() as connection:
            job = (
                await connection.execute(
                    sa.select(durable_jobs).where(
                        durable_jobs.c.job_type == PROFILE_REMATCH_JOB_TYPE,
                        durable_jobs.c.idempotency_key
                        == profile_rematch_job_key(
                            activated.profile.profile.id,
                            activated.profile.profile.revision,
                        ),
                    )
                )
            ).mappings().one()

        result = await ProfileRematchJobProcessor(
            self.database,
            self.config,
        ).process(_claim(job))

        self.assertFalse(result.skipped)
        self.assertEqual(result.opportunity_count, 1)
        async with self.database.connect() as connection:
            traces = await MatchTraceRepository().list_traces(
                connection,
                run_id=result.match_run_id,
            )
            delivery_count = await connection.scalar(
                sa.select(sa.func.count())
                .select_from(personalized_deliveries)
                .where(
                    personalized_deliveries.c.search_profile_id
                    == activated.profile.profile.id
                )
            )
            ai_call_count = await connection.scalar(
                sa.select(sa.func.count()).select_from(ai_call_telemetry)
            )
        self.assertEqual(
            {trace.trace.opportunity_id for trace in traces},
            {opportunity_id},
        )
        self.assertNotIn(old_opportunity_id, {trace.trace.opportunity_id for trace in traces})
        self.assertNotIn(closed_opportunity_id, {trace.trace.opportunity_id for trace in traces})
        self.assertEqual(delivery_count, 1)
        self.assertEqual(ai_call_count, 0)

    async def test_rematch_does_not_duplicate_delivery_for_existing_user(self):
        opportunity_id = await self._insert_opportunity(
            last_seen_at=datetime.now(timezone.utc) - timedelta(hours=1),
            lifecycle_status="active",
        )
        first = await self._activate_profile("3001")
        first_result = await ProfileRematchJobProcessor(
            self.database,
            self.config,
        ).process(await self._rematch_claim(first))
        self.assertEqual(first_result.opportunity_count, 1)

        second = await self._activate_profile("3002")
        second_result = await ProfileRematchJobProcessor(
            self.database,
            self.config,
        ).process(await self._rematch_claim(second))
        self.assertEqual(second_result.opportunity_count, 1)

        async with self.database.connect() as connection:
            first_count = await connection.scalar(
                sa.select(sa.func.count())
                .select_from(personalized_deliveries)
                .where(
                    personalized_deliveries.c.search_profile_id
                    == first.profile.profile.id
                )
            )
            second_count = await connection.scalar(
                sa.select(sa.func.count())
                .select_from(personalized_deliveries)
                .where(
                    personalized_deliveries.c.search_profile_id
                    == second.profile.profile.id
                )
            )
        self.assertEqual(first_count, 1)
        self.assertEqual(second_count, 1)
        self.assertEqual(
            {trace.trace.opportunity_id for trace in await self._traces(second_result)},
            {opportunity_id},
        )

    async def _activate_profile(self, external_user_id: str):
        draft = await self.service.create_manual_draft(
            platform="telegram",
            external_user_id=external_user_id,
            semantic_text="Python developer | Python | Telegram",
            roles=("Python developer",),
            skills=("Python",),
            categories=("Telegram",),
        )
        confirmed = await self.service.confirm(
            platform="telegram",
            external_user_id=external_user_id,
            profile_id=draft.profile.id,
            expected_revision=draft.profile.revision,
        )
        return await self.service.activate(
            platform="telegram",
            external_user_id=external_user_id,
            profile_id=confirmed.profile.id,
            expected_revision=confirmed.profile.revision,
        )

    async def _rematch_claim(self, activation):
        async with self.database.connect() as connection:
            row = (
                await connection.execute(
                    sa.select(durable_jobs).where(
                        durable_jobs.c.job_type == PROFILE_REMATCH_JOB_TYPE,
                        durable_jobs.c.idempotency_key
                        == profile_rematch_job_key(
                            activation.profile.profile.id,
                            activation.profile.profile.revision,
                        ),
                    )
                )
            ).mappings().one()
        return _claim(row)

    async def _traces(self, result):
        async with self.database.connect() as connection:
            return await MatchTraceRepository().list_traces(
                connection,
                run_id=result.match_run_id,
            )

    async def _insert_opportunity(
        self,
        *,
        last_seen_at: datetime,
        lifecycle_status: str,
    ) -> UUID:
        opportunity_id = uuid4()
        cache_id = uuid4()
        content = f"Need a Python developer for Telegram automation {opportunity_id}"
        content_hash = _sha256(content)
        async with self.database.transaction() as connection:
            await connection.execute(
                opportunity_analysis_cache.insert().values(
                    id=cache_id,
                    normalized_content=content,
                    normalized_content_sha256=content_hash,
                    analysis_input_sha256=_sha256(f"analysis:{content}"),
                    analyzer_version="profile-rematch-fixture.v1",
                    analysis_schema_version="opportunity_analysis.v1",
                    result={"fixture": "profile-rematch"},
                )
            )
            await connection.execute(
                opportunities.insert().values(
                    id=opportunity_id,
                    schema_version="canonical_opportunity.v1",
                    canonical_title="Python developer",
                    task_summary="Build Telegram automation",
                    market_direction="buyer_to_specialist",
                    intent_stage="active",
                    opportunity_type="project",
                    category="Telegram",
                    role_title="Python developer",
                    skills=["Python", "Telegram"],
                    budget_known=False,
                    budget_explicit=False,
                    work_remote=True,
                    analysis_confidence=Decimal("0.9000"),
                    quality_actionability=Decimal("0.9000"),
                    quality_commercial_plausibility=Decimal("0.9000"),
                    quality_specificity=Decimal("0.9000"),
                    quality_credibility=Decimal("0.9000"),
                    red_flags=[],
                    first_seen_at=last_seen_at,
                    last_seen_at=last_seen_at,
                    lifecycle_status=lifecycle_status,
                    lifecycle_changed_at=last_seen_at,
                )
            )
            await connection.execute(
                opportunity_analysis_links.insert().values(
                    analysis_cache_id=cache_id,
                    opportunity_id=opportunity_id,
                    dedup_relation="canonical",
                    dedup_algorithm_version="profile-rematch-fixture.v1",
                    normalized_text_sha256=content_hash,
                    dedup_window_seconds=604800,
                    dedup_evidence={"fixture": "profile-rematch"},
                )
            )
        return opportunity_id


class ProfileRematchUnitTest(unittest.TestCase):
    def test_pipeline_worker_registers_profile_rematch_handler(self):
        worker = _build_worker(
            object(),
            RuntimeConfig(_env_file=None),
            logger=logging.getLogger("profile-rematch-test"),
            worker_id="profile-rematch-worker",
            analyzer=None,
            delivery_sender=object(),
        )
        self.assertIn(PROFILE_REMATCH_JOB_TYPE, worker._handlers)

    def test_job_key_round_trips_profile_and_revision(self):
        profile_id = UUID("11111111-1111-1111-1111-111111111111")
        key = profile_rematch_job_key(profile_id, 7)
        self.assertEqual(parse_profile_rematch_job_key(key), (profile_id, 7))

    def test_rematch_bound_is_hard_capped(self):
        with self.assertRaises(ValueError):
            ProfileRematchJobProcessor(
                object(),
                RuntimeConfig(_env_file=None),
                max_opportunities=501,
            )


def _claim(row) -> JobClaim:
    return JobClaim(
        id=row["id"],
        job_type=row["job_type"],
        idempotency_key=row["idempotency_key"],
        correlation_id=row["correlation_id"],
        attempt_count=int(row["attempt_count"]),
        max_attempts=int(row["max_attempts"]),
        worker_id="profile-rematch-test",
        reclaimed=False,
    )


def _sha256(value: str) -> str:
    from hashlib import sha256

    return sha256(value.encode("utf-8")).hexdigest()


if __name__ == "__main__":
    unittest.main()
