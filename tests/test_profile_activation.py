from __future__ import annotations

import asyncio
from datetime import timedelta
from types import SimpleNamespace
import unittest
from uuid import UUID

import sqlalchemy as sa
from sqlalchemy.exc import IntegrityError

from freelancer_bot.billing import TRIAL_POLICY_VERSION
from freelancer_bot.persistence.database import Database
from freelancer_bot.persistence.schema import (
    durable_jobs,
    search_profile_analysis_cache,
    telegram_chat_discovery_topics,
)
from freelancer_bot.persistence.search_profiles import (
    SearchProfileActivationConflict,
    SearchProfileActivationError,
    SearchProfileOwnershipError,
    UserRepository,
)
from freelancer_bot.persistence.telegram_chat_discovery import SEARCH_JOB_TYPE
from freelancer_bot.profile_confirmation import ProfileConfirmationService
from freelancer_bot.profile_rematch import (
    PROFILE_REMATCH_JOB_TYPE,
    profile_rematch_job_key,
)
from freelancer_bot.telegram_chat_discovery import TelegramChatDiscoveryService
from freelancer_bot.telegram_onboarding import TelegramProfileOnboarding
from freelancer_bot.telegram_profile_discovery import (
    TELEGRAM_PROFILE_DISCOVERY_JOB_TYPE,
    profile_discovery_job_key,
)
from postgres_support import TEST_DATABASE_URL, migrate_to_head, temporary_database


@unittest.skipUnless(TEST_DATABASE_URL, "TEST_DATABASE_URL is not configured")
class SearchProfileActivationIntegrationTest(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.database_context = temporary_database()
        self.database_url = self.database_context.__enter__()
        migrate_to_head(self.database_url)
        self.database = Database(self.database_url, pool_size=4, max_overflow=8)
        self.service = ProfileConfirmationService(self.database)

    async def asyncTearDown(self):
        await self.database.close()
        self.database_context.__exit__(None, None, None)

    async def test_trial_starts_only_on_first_confirmed_profile_activation(self):
        draft = await self._draft("first-activation")
        user_before = await self._user("first-activation")
        self.assertIsNone(user_before.trial_started_at)

        with self.assertRaises(SearchProfileActivationError):
            await self.service.activate(
                platform="telegram",
                external_user_id="first-activation",
                profile_id=draft.profile.id,
                expected_revision=draft.profile.revision,
            )
        self.assertIsNone((await self._user("first-activation")).trial_started_at)

        confirmed = await self._confirm("first-activation", draft)
        before_activation = await self._user("first-activation")
        self.assertIsNone(before_activation.trial_started_at)
        self.assertIsNone(before_activation.trial_expires_at)
        self.assertIsNone(before_activation.trial_policy_version)
        activated = await self.service.activate(
            platform="telegram",
            external_user_id="first-activation",
            profile_id=confirmed.profile.id,
            expected_revision=confirmed.profile.revision,
        )
        trial_started_at = (await self._user("first-activation")).trial_started_at
        activated_user = await self._user("first-activation")

        self.assertTrue(activated.trial_started)
        self.assertTrue(activated.profile.profile.is_active)
        self.assertTrue(activated.profile.profile.is_primary)
        self.assertIsNotNone(activated.profile.profile.activated_at)
        self.assertIsNotNone(trial_started_at)
        self.assertEqual(
            activated_user.trial_expires_at,
            trial_started_at + timedelta(days=3),
        )
        self.assertEqual(activated_user.trial_policy_version, TRIAL_POLICY_VERSION)
        async with self.database.connect() as connection:
            discovery_jobs = (
                await connection.execute(
                    sa.select(durable_jobs).where(
                        durable_jobs.c.job_type == TELEGRAM_PROFILE_DISCOVERY_JOB_TYPE,
                        durable_jobs.c.idempotency_key
                        == profile_discovery_job_key(
                            activated.profile.profile.id,
                            activated.profile.profile.revision,
                        ),
                    )
                )
            ).mappings().all()
        self.assertEqual(len(discovery_jobs), 1)
        async with self.database.connect() as connection:
            rematch_jobs = (
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
            ).mappings().all()
        self.assertEqual(len(rematch_jobs), 1)

        repeated = await self.service.activate(
            platform="telegram",
            external_user_id="first-activation",
            profile_id=confirmed.profile.id,
            expected_revision=confirmed.profile.revision,
        )
        self.assertFalse(repeated.trial_started)
        self.assertEqual(
            (await self._user("first-activation")).trial_started_at,
            trial_started_at,
        )
        self.assertEqual(
            (await self._user("first-activation")).trial_expires_at,
            activated_user.trial_expires_at,
        )
        async with self.database.connect() as connection:
            discovery_job_count = await connection.scalar(
                sa.select(sa.func.count())
                .select_from(durable_jobs)
                .where(
                    durable_jobs.c.job_type == TELEGRAM_PROFILE_DISCOVERY_JOB_TYPE,
                    durable_jobs.c.idempotency_key
                    == profile_discovery_job_key(
                        activated.profile.profile.id,
                        activated.profile.profile.revision,
                    ),
                )
            )
        self.assertEqual(discovery_job_count, 1)
        async with self.database.connect() as connection:
            rematch_job_count = await connection.scalar(
                sa.select(sa.func.count())
                .select_from(durable_jobs)
                .where(
                    durable_jobs.c.job_type == PROFILE_REMATCH_JOB_TYPE,
                    durable_jobs.c.idempotency_key
                    == profile_rematch_job_key(
                        activated.profile.profile.id,
                        activated.profile.profile.revision,
                    ),
                )
            )
        self.assertEqual(rematch_job_count, 1)

    async def test_activating_profile_deactivates_previous_active_profile(self):
        first = await self._confirm(
            "multi-profile",
            await self._draft("multi-profile", suffix="first"),
        )
        second = await self._confirm(
            "multi-profile",
            await self._draft("multi-profile", suffix="second"),
        )
        first_active = await self.service.activate(
            platform="telegram",
            external_user_id="multi-profile",
            profile_id=first.profile.id,
            expected_revision=first.profile.revision,
        )
        second_active = await self.service.activate(
            platform="telegram",
            external_user_id="multi-profile",
            profile_id=second.profile.id,
            expected_revision=second.profile.revision,
        )
        profiles = await self.service.list_profiles(
            platform="telegram",
            external_user_id="multi-profile",
        )
        by_id = {view.profile.id: view.profile for view in profiles}

        self.assertEqual(len(profiles), 2)
        self.assertFalse(by_id[first.profile.id].is_active)
        self.assertFalse(by_id[first.profile.id].is_primary)
        self.assertIsNotNone(by_id[first.profile.id].deactivated_at)
        self.assertTrue(by_id[second.profile.id].is_active)
        self.assertTrue(by_id[second.profile.id].is_primary)
        self.assertTrue(first_active.trial_started)
        self.assertFalse(second_active.trial_started)
        self.assertEqual(sum(profile.is_active for profile in by_id.values()), 1)
        self.assertEqual(sum(profile.is_primary for profile in by_id.values()), 1)

    async def test_activating_profile_does_not_deactivate_another_users_profile(self):
        first = await self._confirm(
            "isolated-user-one",
            await self._draft("isolated-user-one", suffix="first"),
        )
        second = await self._confirm(
            "isolated-user-two",
            await self._draft("isolated-user-two", suffix="second"),
        )
        await self.service.activate(
            platform="telegram",
            external_user_id="isolated-user-one",
            profile_id=first.profile.id,
            expected_revision=first.profile.revision,
        )
        await self.service.activate(
            platform="telegram",
            external_user_id="isolated-user-two",
            profile_id=second.profile.id,
            expected_revision=second.profile.revision,
        )

        first_profiles = await self.service.list_profiles(
            platform="telegram",
            external_user_id="isolated-user-one",
        )
        second_profiles = await self.service.list_profiles(
            platform="telegram",
            external_user_id="isolated-user-two",
        )
        self.assertTrue(first_profiles[0].profile.is_active)
        self.assertTrue(first_profiles[0].profile.is_primary)
        self.assertTrue(second_profiles[0].profile.is_active)
        self.assertTrue(second_profiles[0].profile.is_primary)

    async def test_deactivation_and_reactivation_preserve_trial_and_first_activation(self):
        confirmed = await self._confirm(
            "reactivate",
            await self._draft("reactivate"),
        )
        active = await self.service.activate(
            platform="telegram",
            external_user_id="reactivate",
            profile_id=confirmed.profile.id,
            expected_revision=confirmed.profile.revision,
        )
        first_activated_at = active.profile.profile.activated_at
        activated_user = await self._user("reactivate")
        trial_started_at = activated_user.trial_started_at
        trial_expires_at = activated_user.trial_expires_at

        stopped = await self.service.deactivate(
            platform="telegram",
            external_user_id="reactivate",
            profile_id=confirmed.profile.id,
            expected_revision=active.profile.profile.revision,
        )
        repeated_stop = await self.service.deactivate(
            platform="telegram",
            external_user_id="reactivate",
            profile_id=confirmed.profile.id,
            expected_revision=active.profile.profile.revision,
        )
        self.assertEqual(stopped.profile, repeated_stop.profile)
        self.assertFalse(stopped.profile.is_active)
        self.assertIsNotNone(stopped.profile.deactivated_at)

        reactivated = await self.service.activate(
            platform="telegram",
            external_user_id="reactivate",
            profile_id=confirmed.profile.id,
            expected_revision=stopped.profile.revision,
        )
        self.assertFalse(reactivated.trial_started)
        self.assertEqual(reactivated.profile.profile.activated_at, first_activated_at)
        self.assertIsNone(reactivated.profile.profile.deactivated_at)
        self.assertEqual(
            (await self._user("reactivate")).trial_started_at,
            trial_started_at,
        )
        self.assertEqual(
            (await self._user("reactivate")).trial_expires_at,
            trial_expires_at,
        )

    async def test_concurrent_first_activations_start_one_trial_and_one_primary(self):
        first = await self._confirm(
            "concurrent-activation",
            await self._draft("concurrent-activation", suffix="first"),
        )
        second = await self._confirm(
            "concurrent-activation",
            await self._draft("concurrent-activation", suffix="second"),
        )

        async def activate(view):
            return await self.service.activate(
                platform="telegram",
                external_user_id="concurrent-activation",
                profile_id=view.profile.id,
                expected_revision=view.profile.revision,
            )

        outcomes = await asyncio.gather(activate(first), activate(second))
        profiles = await self.service.list_profiles(
            platform="telegram",
            external_user_id="concurrent-activation",
        )

        self.assertEqual(sum(outcome.trial_started for outcome in outcomes), 1)
        self.assertEqual(sum(view.profile.is_active for view in profiles), 1)
        self.assertEqual(sum(view.profile.is_primary for view in profiles), 1)
        self.assertEqual(sum(view.profile.deactivated_at is not None for view in profiles), 1)
        self.assertIsNotNone(
            (await self._user("concurrent-activation")).trial_started_at
        )

    async def test_activation_preserves_ownership_and_revision_guards(self):
        confirmed = await self._confirm(
            "activation-owner",
            await self._draft("activation-owner"),
        )
        await self._draft("another-user")
        with self.assertRaises(SearchProfileOwnershipError):
            await self.service.activate(
                platform="telegram",
                external_user_id="another-user",
                profile_id=confirmed.profile.id,
                expected_revision=confirmed.profile.revision,
            )
        with self.assertRaises(SearchProfileActivationConflict):
            await self.service.activate(
                platform="telegram",
                external_user_id="activation-owner",
                profile_id=confirmed.profile.id,
                expected_revision=confirmed.profile.revision - 1,
            )
        self.assertIsNone((await self._user("activation-owner")).trial_started_at)

    async def test_manual_telegram_activation_works_without_ai_or_cache(self):
        telegram = TelegramProfileOnboarding(self.service, None)
        draft_response = await telegram.create_manual(
            external_user_id="manual-no-ai",
            payload="Разработчик | Python | Telegram",
        )
        profile_id = _profile_id_from_callback(draft_response.buttons[0][0].data)
        draft = await self.service.show(
            platform="telegram",
            external_user_id="manual-no-ai",
            profile_id=profile_id,
        )
        confirmed_response = await telegram.confirm(
            external_user_id="manual-no-ai",
            profile_id=profile_id,
            expected_revision=draft.profile.revision,
        )
        confirmed = await self.service.show(
            platform="telegram",
            external_user_id="manual-no-ai",
            profile_id=profile_id,
        )
        activated_response = await telegram.activate(
            external_user_id="manual-no-ai",
            profile_id=profile_id,
            expected_revision=confirmed.profile.revision,
        )

        self.assertIn("Активировать поиск", confirmed_response.buttons[0][0].label)
        self.assertIn("поиск активен", activated_response.text)
        self.assertIn("Пробный период начался", activated_response.text)
        self.assertIn("Остановить поиск", activated_response.buttons[0][0].label)
        self.assertLessEqual(len(activated_response.buttons[0][0].data), 64)
        async with self.database.connect() as connection:
            cache_count = await connection.scalar(
                sa.select(sa.func.count()).select_from(
                    search_profile_analysis_cache
                )
            )
        self.assertEqual(cache_count, 0)

    async def test_chat_discovery_activation_uses_buyer_queries_and_skips_legacy_job(self):
        service = ProfileConfirmationService(
            self.database,
            telegram_chat_discovery_enabled=True,
            telegram_chat_discovery_max_topics_per_cycle=2,
        )
        draft = await service.create_manual_draft(
            platform="telegram",
            external_user_id="chat-discovery-profile",
            semantic_text="Video Editor | Premiere Pro | YouTube editing",
            roles=("Video Editor",),
            skills=("Premiere Pro",),
            categories=("YouTube editing", "short-form video"),
        )
        confirmed = await service.confirm(
            platform="telegram",
            external_user_id="chat-discovery-profile",
            profile_id=draft.profile.id,
            expected_revision=draft.profile.revision,
        )

        await service.activate(
            platform="telegram",
            external_user_id="chat-discovery-profile",
            profile_id=confirmed.profile.id,
            expected_revision=confirmed.profile.revision,
        )

        async with self.database.connect() as connection:
            topics = (
                await connection.execute(
                    sa.select(telegram_chat_discovery_topics).where(
                        telegram_chat_discovery_topics.c.topic_kind == "profile"
                    )
                )
            ).mappings().all()
            chat_jobs = (
                await connection.execute(
                    sa.select(durable_jobs).where(
                        durable_jobs.c.job_type == SEARCH_JOB_TYPE
                    )
                )
            ).mappings().all()
            legacy_jobs = (
                await connection.execute(
                    sa.select(durable_jobs).where(
                        durable_jobs.c.job_type == TELEGRAM_PROFILE_DISCOVERY_JOB_TYPE
                    )
                )
            ).mappings().all()

        self.assertGreater(len(topics), 0)
        self.assertLessEqual(len(topics), 20)
        self.assertEqual(len(chat_jobs), 2)
        self.assertEqual(legacy_jobs, [])
        self.assertIn("Video Editor", {topic["topic_text"] for topic in topics})
        self.assertIn("YouTube editing", {topic["topic_text"] for topic in topics})
        self.assertTrue(any(topic["priority"] == 90 for topic in topics))
        self.assertTrue(any(topic["priority"] == 80 for topic in topics))
        self.assertTrue(any(topic["priority"] == 70 for topic in topics))
        buyer_intent_markers = (
            "looking for",
            "need",
            "hiring",
            "needed",
            "project:",
            "vacancy:",
            "who can handle",
        )
        self.assertTrue(
            any(
                any(
                    marker in topic["topic_text"].casefold()
                    for marker in buyer_intent_markers
                )
                for topic in topics
            )
        )

    async def test_bounded_chat_scheduler_mixes_broad_and_short_profile_topics(self):
        service = ProfileConfirmationService(
            self.database,
            telegram_chat_discovery_enabled=True,
            telegram_chat_discovery_max_topics_per_cycle=5,
        )
        draft = await service.create_manual_draft(
            platform="telegram",
            external_user_id="chat-discovery-scheduler-profile",
            semantic_text=(
                "Я Python-разработчик и специалист по автоматизации. "
                "Делаю Telegram-ботов и backend на Python."
            ),
            roles=("Python Developer", "Telegram Bot Developer"),
            skills=("Python", "Telegram Bots", "API integrations", "Backend"),
            categories=("Telegram bot development", "Python backend", "automation"),
        )
        confirmed = await service.confirm(
            platform="telegram",
            external_user_id="chat-discovery-scheduler-profile",
            profile_id=draft.profile.id,
            expected_revision=draft.profile.revision,
        )
        await service.activate(
            platform="telegram",
            external_user_id="chat-discovery-scheduler-profile",
            profile_id=confirmed.profile.id,
            expected_revision=confirmed.profile.revision,
        )

        class ReadyGovernor:
            async def current_state(self):
                return SimpleNamespace(
                    status=SimpleNamespace(value="ready"),
                    cooldown_until=None,
                )

        config = SimpleNamespace(
            telegram_chat_discovery_refresh_interval_seconds=21_600,
            telegram_chat_discovery_max_pending_screens=50,
            source_audit_calls_per_day=100,
            opportunity_analysis_backlog_threshold=500,
            telegram_chat_discovery_screen_policy_version="test-policy",
            telegram_chat_discovery_screen_min_sample=10,
            telegram_chat_discovery_screen_min_useful_messages=3,
            telegram_chat_discovery_screen_min_useful_ratio=0.12,
            telegram_chat_discovery_screen_min_confidence=0.65,
            telegram_chat_discovery_screen_max_seller_ratio=0.70,
            telegram_chat_discovery_screen_max_spam_ratio=0.70,
        )
        discovery_service = TelegramChatDiscoveryService(
            self.database,
            object(),
            config=config,
            collector_account_id=1,
            governor=ReadyGovernor(),
            screen_provider=object(),
        )
        job_ids = await discovery_service.schedule_due_searches(max_topics=6)

        async with self.database.connect() as connection:
            job_rows = (
                await connection.execute(
                    sa.select(durable_jobs.c.idempotency_key).where(
                        durable_jobs.c.id.in_(job_ids)
                    )
                )
            ).mappings().all()
            topic_ids = tuple(
                UUID(row["idempotency_key"].split(":")[1]) for row in job_rows
            )
            selected_topics = (
                await connection.execute(
                    sa.select(telegram_chat_discovery_topics).where(
                        telegram_chat_discovery_topics.c.id.in_(topic_ids)
                    )
                )
            ).mappings().all()

        self.assertEqual(len(job_ids), 6)
        priorities = {topic["priority"] for topic in selected_topics}
        self.assertIn(90, priorities)
        self.assertIn(80, priorities)
        self.assertTrue(all(priority >= 80 for priority in priorities))

    async def test_chat_topics_are_global_and_recent_topic_is_not_requeued(self):
        service = ProfileConfirmationService(
            self.database,
            telegram_chat_discovery_enabled=True,
            telegram_chat_discovery_max_topics_per_cycle=2,
        )
        first = await service.create_manual_draft(
            platform="telegram",
            external_user_id="chat-dedup-first",
            semantic_text="Copywriter | SEO writing | website copy",
            roles=("Copywriter",),
            skills=("SEO writing",),
            categories=("website copy",),
        )
        first_confirmed = await service.confirm(
            platform="telegram",
            external_user_id="chat-dedup-first",
            profile_id=first.profile.id,
            expected_revision=first.profile.revision,
        )
        await service.activate(
            platform="telegram",
            external_user_id="chat-dedup-first",
            profile_id=first_confirmed.profile.id,
            expected_revision=first_confirmed.profile.revision,
        )

        async with self.database.transaction() as connection:
            first_topic = (
                await connection.execute(
                    sa.select(telegram_chat_discovery_topics)
                    .where(
                        telegram_chat_discovery_topics.c.topic_kind == "profile"
                    )
                    .order_by(telegram_chat_discovery_topics.c.topic_key)
                    .limit(1)
                )
            ).mappings().one()
            await connection.execute(
                sa.update(telegram_chat_discovery_topics)
                .where(telegram_chat_discovery_topics.c.id == first_topic["id"])
                .values(next_eligible_at=sa.func.now() + sa.text("interval '1 day'"))
            )
            jobs_before = await connection.scalar(
                sa.select(sa.func.count())
                .select_from(durable_jobs)
                .where(
                    durable_jobs.c.job_type == SEARCH_JOB_TYPE,
                    durable_jobs.c.idempotency_key.like(
                        f"topic:{first_topic['id']}:refresh:%"
                    ),
                )
            )

        second = await service.create_manual_draft(
            platform="telegram",
            external_user_id="chat-dedup-second",
            semantic_text="Copywriter | SEO writing | website copy",
            roles=("Copywriter",),
            skills=("SEO writing",),
            categories=("website copy",),
        )
        second_confirmed = await service.confirm(
            platform="telegram",
            external_user_id="chat-dedup-second",
            profile_id=second.profile.id,
            expected_revision=second.profile.revision,
        )
        await service.activate(
            platform="telegram",
            external_user_id="chat-dedup-second",
            profile_id=second_confirmed.profile.id,
            expected_revision=second_confirmed.profile.revision,
        )

        async with self.database.connect() as connection:
            topic_count = await connection.scalar(
                sa.select(sa.func.count())
                .select_from(telegram_chat_discovery_topics)
                .where(telegram_chat_discovery_topics.c.topic_kind == "profile")
            )
            jobs_after = await connection.scalar(
                sa.select(sa.func.count())
                .select_from(durable_jobs)
                .where(
                    durable_jobs.c.job_type == SEARCH_JOB_TYPE,
                    durable_jobs.c.idempotency_key.like(
                        f"topic:{first_topic['id']}:refresh:%"
                    ),
                )
            )

        self.assertEqual(topic_count, 20)
        self.assertEqual(jobs_before, jobs_after)

    async def test_database_rejects_active_draft_and_multiple_primary_profiles(self):
        first = await self._confirm(
            "constraint-user",
            await self._draft("constraint-user", suffix="first"),
        )
        second = await self._confirm(
            "constraint-user",
            await self._draft("constraint-user", suffix="second"),
        )
        await self.service.activate(
            platform="telegram",
            external_user_id="constraint-user",
            profile_id=first.profile.id,
            expected_revision=first.profile.revision,
        )
        with self.assertRaises(IntegrityError):
            async with self.database.transaction() as connection:
                await connection.execute(
                    sa.text(
                        "UPDATE search_profiles SET is_active = true, "
                        "is_primary = true, activated_at = now() WHERE id = :id"
                    ),
                    {"id": second.profile.id},
                )

        draft = await self._draft("draft-constraint")
        with self.assertRaises(IntegrityError):
            async with self.database.transaction() as connection:
                await connection.execute(
                    sa.text(
                        "UPDATE search_profiles SET is_active = true, "
                        "activated_at = now() WHERE id = :id"
                    ),
                    {"id": draft.profile.id},
                )

    async def _draft(self, external_user_id: str, *, suffix: str = "profile"):
        return await self.service.create_manual_draft(
            platform="telegram",
            external_user_id=external_user_id,
            semantic_text=f"Developer | Python | Telegram {suffix}",
            roles=("Developer",),
            skills=("Python",),
            categories=(f"Telegram {suffix}",),
        )

    async def _confirm(self, external_user_id: str, draft):
        return await self.service.confirm(
            platform="telegram",
            external_user_id=external_user_id,
            profile_id=draft.profile.id,
            expected_revision=draft.profile.revision,
        )

    async def _user(self, external_user_id: str):
        async with self.database.connect() as connection:
            return await UserRepository().get_by_identity(
                connection,
                platform="telegram",
                external_user_id=external_user_id,
            )


def _profile_id_from_callback(data: bytes) -> UUID:
    return UUID(data.decode("ascii").split(":")[2])


if __name__ == "__main__":
    unittest.main()
