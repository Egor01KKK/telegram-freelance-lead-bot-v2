from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
from hashlib import sha256
import json
import unittest
from uuid import UUID, uuid4

import sqlalchemy as sa

from freelancer_bot.opportunity_analysis import (
    OPPORTUNITY_ANALYSIS_SCHEMA_VERSION,
    OpportunityAnalysis,
)
from freelancer_bot.opportunity_dedup import (
    PREFERRED_SOURCE_POLICY_VERSION,
    STRUCTURED_DEDUP_ALGORITHM_VERSION,
    STRUCTURED_DEDUP_RELATION,
)
from freelancer_bot.persistence.collector_accounts import CollectorAccountRepository
from freelancer_bot.persistence.database import Database
from freelancer_bot.persistence.opportunities import (
    CANONICAL_OPPORTUNITY_SCHEMA_VERSION,
    EXACT_DEDUP_RELATION,
    NEAR_DEDUP_RELATION,
    OPPORTUNITY_DEDUP_ALGORITHM_VERSION,
    CanonicalOpportunityRepository,
    InvalidOpportunityTransition,
    OpportunityLifecycleStatus,
    OpportunityLinkConflict,
)
from freelancer_bot.persistence.raw_messages import (
    RawMessageIngestor,
    RawMessageInput,
    RawMessageOrigin,
)
from freelancer_bot.persistence.schema import (
    opportunities,
    opportunity_analysis_cache,
    opportunity_analysis_links,
    opportunity_source_messages,
)
from freelancer_bot.persistence.source_repository import SourceRepository, SourceStatus
from postgres_support import TEST_DATABASE_URL, migrate_to_head, temporary_database


NOW = datetime(2026, 8, 9, 22, 0, tzinfo=timezone.utc)
TRACE_ID = UUID("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb")


@unittest.skipUnless(TEST_DATABASE_URL, "TEST_DATABASE_URL is not configured")
class CanonicalOpportunityRepositoryTest(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.database_context = temporary_database()
        self.database_url = self.database_context.__enter__()
        migrate_to_head(self.database_url)
        self.database = Database(self.database_url, pool_size=8, max_overflow=16)
        self.repository = CanonicalOpportunityRepository()

    async def asyncSetUp(self):
        sources = SourceRepository()
        accounts = CollectorAccountRepository()
        async with self.database.transaction() as connection:
            self.account = await accounts.ensure(
                connection,
                platform="telegram",
                external_account_id="92001",
                display_name="G5 opportunity collector",
            )
            self.sources = []
            for index in range(2):
                candidate = await sources.create_candidate(
                    connection,
                    platform="telegram",
                    external_id=f"username:g5_opportunity_{index}",
                    access_type="public",
                    display_name=f"G5 opportunity source {index}",
                    handle=f"@g5_opportunity_{index}",
                    canonical_url=f"https://t.me/g5_opportunity_{index}",
                    provider="g5_opportunity_fixture",
                    lineage_key=f"g5-opportunity:{index}",
                )
                self.sources.append(
                    await sources.transition(
                        connection,
                        candidate.id,
                        SourceStatus.APPROVED,
                        reason="G5 opportunity fixture approved",
                    )
                )
        self.messages = (
            await self._ingest(0, 501, NOW),
            await self._ingest(1, 601, NOW + timedelta(minutes=7)),
        )

    async def asyncTearDown(self):
        await self.database.close()
        self.database_context.__exit__(None, None, None)

    async def test_one_canonical_row_represents_multiple_source_messages(self):
        cache_id = await self._cache_entry("a")
        analysis = _analysis()

        async with self.database.transaction() as connection:
            first = await self.repository.ensure_from_analysis(
                connection,
                analysis_cache_id=cache_id,
                raw_message_ids=tuple(message.message.id for message in self.messages),
                analysis=analysis,
            )
        async with self.database.transaction() as connection:
            repeated = await self.repository.ensure_from_analysis(
                connection,
                analysis_cache_id=cache_id,
                raw_message_ids=tuple(
                    message.message.id for message in reversed(self.messages)
                ),
                analysis=analysis,
            )

        self.assertTrue(first.created)
        self.assertEqual(first.linked_message_count, 2)
        self.assertFalse(repeated.created)
        self.assertEqual(repeated.linked_message_count, 0)
        self.assertEqual(first.opportunity.id, repeated.opportunity.id)
        self.assertEqual(first.opportunity.updated_at, repeated.opportunity.updated_at)
        self.assertEqual(
            first.opportunity.schema_version, CANONICAL_OPPORTUNITY_SCHEMA_VERSION
        )
        self.assertEqual(
            set(first.opportunity.raw_message_ids),
            {
                self.messages[0].message.id,
                self.messages[1].message.id,
            },
        )
        self.assertEqual(first.opportunity.analysis_cache_ids, (cache_id,))
        self.assertEqual(first.opportunity.first_seen_at, NOW)
        self.assertEqual(
            first.opportunity.last_seen_at,
            NOW + timedelta(minutes=7),
        )
        self.assertEqual(
            first.opportunity.source_message_urls,
            (
                "https://t.me/g5_opportunity_0/501",
                "https://t.me/g5_opportunity_1/601",
            ),
        )
        self.assertEqual(
            {item.source_id for item in first.opportunity.source_observations},
            {self.sources[0].id, self.sources[1].id},
        )
        self.assertTrue(
            all(item.linked_at is not None for item in first.opportunity.source_observations)
        )

        async with self.database.connect() as connection:
            counts = (
                await connection.scalar(
                    sa.select(sa.func.count()).select_from(opportunities)
                ),
                await connection.scalar(
                    sa.select(sa.func.count()).select_from(opportunity_source_messages)
                ),
                await connection.scalar(
                    sa.select(sa.func.count()).select_from(opportunity_analysis_links)
                ),
            )
            by_raw = await self.repository.get_for_raw_message(
                connection,
                self.messages[1].message.id,
            )
        self.assertEqual(counts, (1, 2, 1))
        self.assertEqual(by_raw.id, first.opportunity.id)

    async def test_unknown_budget_stays_valid_and_quality_is_opportunity_only(self):
        cache_id = await self._cache_entry("b")
        analysis = _analysis(known_budget=False)
        async with self.database.transaction() as connection:
            outcome = await self.repository.ensure_from_analysis(
                connection,
                analysis_cache_id=cache_id,
                raw_message_ids=(self.messages[0].message.id,),
                analysis=analysis,
            )
        stored = outcome.opportunity

        self.assertFalse(stored.analysis.budget.known)
        self.assertIsNone(stored.analysis.budget.min)
        self.assertIsNone(stored.analysis.budget.max)
        self.assertEqual(stored.analysis.quality, analysis.quality)
        self.assertEqual(stored.canonical_title, "Telegram bot developer")
        self.assertEqual(stored.task_summary, "Build a Telegram ordering bot")
        self.assertNotIn("source_quality", opportunities.c)
        self.assertNotIn("user_relevance", opportunities.c)
        self.assertIn("quality_actionability", opportunities.c)

        async with self.database.connect() as connection:
            fresh = await self.repository.list_observed_since(
                connection,
                NOW - timedelta(seconds=1),
            )
            stale = await self.repository.list_observed_since(
                connection,
                NOW + timedelta(seconds=1),
            )
        self.assertEqual([item.id for item in fresh], [stored.id])
        self.assertEqual(stale, ())

    async def test_same_cache_is_concurrency_safe_and_idempotent(self):
        cache_id = await self._cache_entry("c")
        analysis = _analysis()

        async def ensure():
            async with self.database.transaction() as connection:
                return await self.repository.ensure_from_analysis(
                    connection,
                    analysis_cache_id=cache_id,
                    raw_message_ids=(self.messages[0].message.id,),
                    analysis=analysis,
                )

        outcomes = await asyncio.gather(ensure(), ensure())

        self.assertEqual(
            {item.opportunity.id for item in outcomes}, {outcomes[0].opportunity.id}
        )
        self.assertEqual(sum(item.created for item in outcomes), 1)
        async with self.database.connect() as connection:
            count = await connection.scalar(
                sa.select(sa.func.count()).select_from(opportunities)
            )
        self.assertEqual(count, 1)

    async def test_exact_text_across_sources_collapses_distinct_analysis_inputs(self):
        text = "need a python developer to build a telegram ordering bot this week"
        first_message = await self._ingest(0, 701, NOW, content=text)
        second_message = await self._ingest(
            1,
            702,
            NOW + timedelta(minutes=5),
            content=text,
        )
        first_cache = await self._cache_entry("exact-a", content=text)
        second_cache = await self._cache_entry("exact-b", content=text)

        async with self.database.transaction() as connection:
            first = await self.repository.ensure_from_analysis(
                connection,
                analysis_cache_id=first_cache,
                raw_message_ids=(first_message.message.id,),
                analysis=_analysis(),
            )
        async with self.database.transaction() as connection:
            duplicate = await self.repository.ensure_from_analysis(
                connection,
                analysis_cache_id=second_cache,
                raw_message_ids=(second_message.message.id,),
                analysis=_analysis(),
            )

        self.assertTrue(first.created)
        self.assertFalse(duplicate.created)
        self.assertEqual(duplicate.dedup_relation, EXACT_DEDUP_RELATION)
        self.assertEqual(duplicate.dedup_similarity, 1.0)
        self.assertEqual(first.opportunity.id, duplicate.opportunity.id)
        self.assertEqual(
            set(duplicate.opportunity.raw_message_ids),
            {first_message.message.id, second_message.message.id},
        )
        self.assertEqual(
            set(duplicate.opportunity.analysis_cache_ids),
            {first_cache, second_cache},
        )
        exact_link = next(
            link
            for link in duplicate.opportunity.analysis_links
            if link.analysis_cache_id == second_cache
        )
        self.assertEqual(exact_link.dedup_relation, EXACT_DEDUP_RELATION)
        self.assertEqual(exact_link.matched_analysis_cache_id, first_cache)
        self.assertEqual(exact_link.dedup_window_seconds, 7 * 24 * 60 * 60)
        self.assertEqual(
            exact_link.dedup_algorithm_version,
            OPPORTUNITY_DEDUP_ALGORITHM_VERSION,
        )

    async def test_near_text_repost_collapses_and_preserves_both_observations(self):
        original_text = (
            "need a python developer to build a telegram ordering bot with stripe "
            "payments and an admin dashboard this week"
        )
        repost_text = (
            "repost need a python developer to build a telegram ordering bot with "
            "stripe payments and an admin dashboard this week"
        )
        first_message = await self._ingest(0, 711, NOW, content=original_text)
        second_message = await self._ingest(
            1,
            712,
            NOW + timedelta(hours=3),
            content=repost_text,
        )
        first_cache = await self._cache_entry("near-a", content=original_text)
        second_cache = await self._cache_entry("near-b", content=repost_text)

        async with self.database.transaction() as connection:
            first = await self.repository.ensure_from_analysis(
                connection,
                analysis_cache_id=first_cache,
                raw_message_ids=(first_message.message.id,),
                analysis=_analysis(),
            )
        async with self.database.transaction() as connection:
            duplicate = await self.repository.ensure_from_analysis(
                connection,
                analysis_cache_id=second_cache,
                raw_message_ids=(second_message.message.id,),
                analysis=_analysis(),
            )

        self.assertEqual(duplicate.dedup_relation, NEAR_DEDUP_RELATION)
        self.assertGreaterEqual(duplicate.dedup_similarity, 0.9)
        self.assertEqual(first.opportunity.id, duplicate.opportunity.id)
        self.assertEqual(len(duplicate.opportunity.raw_message_ids), 2)
        self.assertEqual(len(duplicate.opportunity.analysis_cache_ids), 2)

    async def test_exact_text_outside_window_remains_distinct(self):
        text = "need a python developer for a detailed telegram commerce project"
        first_message = await self._ingest(0, 721, NOW, content=text)
        second_message = await self._ingest(
            1,
            722,
            NOW + timedelta(days=8),
            content=text,
        )
        first_cache = await self._cache_entry("window-a", content=text)
        second_cache = await self._cache_entry("window-b", content=text)

        async with self.database.transaction() as connection:
            first = await self.repository.ensure_from_analysis(
                connection,
                analysis_cache_id=first_cache,
                raw_message_ids=(first_message.message.id,),
                analysis=_analysis(),
            )
        async with self.database.transaction() as connection:
            second = await self.repository.ensure_from_analysis(
                connection,
                analysis_cache_id=second_cache,
                raw_message_ids=(second_message.message.id,),
                analysis=_analysis(),
            )

        self.assertTrue(first.created)
        self.assertTrue(second.created)
        self.assertNotEqual(first.opportunity.id, second.opportunity.id)

    async def test_different_numeric_evidence_prevents_false_near_merge(self):
        first_text = (
            "need a python developer to build a telegram ordering bot budget 1000 usd "
            "with stripe payments and an admin dashboard"
        )
        second_text = (
            "need a python developer to build a telegram ordering bot budget 2000 usd "
            "with stripe payments and an admin dashboard"
        )
        first_message = await self._ingest(0, 731, NOW, content=first_text)
        second_message = await self._ingest(
            1,
            732,
            NOW + timedelta(minutes=2),
            content=second_text,
        )
        first_cache = await self._cache_entry("numeric-a", content=first_text)
        second_cache = await self._cache_entry("numeric-b", content=second_text)

        async with self.database.transaction() as connection:
            first = await self.repository.ensure_from_analysis(
                connection,
                analysis_cache_id=first_cache,
                raw_message_ids=(first_message.message.id,),
                analysis=_analysis(),
            )
        async with self.database.transaction() as connection:
            second = await self.repository.ensure_from_analysis(
                connection,
                analysis_cache_id=second_cache,
                raw_message_ids=(second_message.message.id,),
                analysis=_analysis(),
            )

        self.assertTrue(first.created)
        self.assertTrue(second.created)
        self.assertNotEqual(first.opportunity.id, second.opportunity.id)

    async def test_concurrent_near_reposts_converge_to_one_opportunity(self):
        first_text = (
            "looking for a python developer to create a telegram mini app for booking "
            "appointments with payments and notifications"
        )
        second_text = (
            "repost looking for a python developer to create a telegram mini app for "
            "booking appointments with payments and notifications"
        )
        first_message = await self._ingest(0, 741, NOW, content=first_text)
        second_message = await self._ingest(1, 742, NOW, content=second_text)
        first_cache = await self._cache_entry("concurrent-a", content=first_text)
        second_cache = await self._cache_entry("concurrent-b", content=second_text)

        async def ensure(cache_id, raw_message_id):
            async with self.database.transaction() as connection:
                return await self.repository.ensure_from_analysis(
                    connection,
                    analysis_cache_id=cache_id,
                    raw_message_ids=(raw_message_id,),
                    analysis=_analysis(),
                )

        outcomes = await asyncio.gather(
            ensure(first_cache, first_message.message.id),
            ensure(second_cache, second_message.message.id),
        )

        self.assertEqual(
            {outcome.opportunity.id for outcome in outcomes},
            {outcomes[0].opportunity.id},
        )
        self.assertEqual(sum(outcome.created for outcome in outcomes), 1)
        self.assertEqual(
            {outcome.dedup_relation for outcome in outcomes},
            {"canonical", NEAR_DEDUP_RELATION},
        )

    async def test_structured_duplicate_selects_earliest_source_and_keeps_alternates(
        self,
    ):
        task = (
            "Implement a Telegram booking mini app with payments reminders and an "
            "operator dashboard"
        )
        later_text = (
            "Partner digest has a Python automation project. Contact @buyer for the "
            "complete commercial brief."
        )
        original_text = (
            "@buyer needs a booking product delivered: Mini App, payment flow, "
            "notifications and admin operations."
        )
        later_message = await self._ingest(
            0,
            751,
            NOW + timedelta(hours=2),
            content=later_text,
        )
        original_message = await self._ingest(
            1,
            752,
            NOW,
            content=original_text,
        )
        later_cache = await self._cache_entry("signals-a", content=later_text)
        original_cache = await self._cache_entry("signals-b", content=original_text)
        analysis = _analysis(
            known_budget=False,
            task_summary=task,
            telegram="@buyer",
        )

        async with self.database.transaction() as connection:
            first = await self.repository.ensure_from_analysis(
                connection,
                analysis_cache_id=later_cache,
                raw_message_ids=(later_message.message.id,),
                analysis=analysis,
            )
        async with self.database.transaction() as connection:
            duplicate = await self.repository.ensure_from_analysis(
                connection,
                analysis_cache_id=original_cache,
                raw_message_ids=(original_message.message.id,),
                analysis=analysis,
            )

        self.assertFalse(duplicate.created)
        self.assertEqual(duplicate.dedup_relation, STRUCTURED_DEDUP_RELATION)
        self.assertEqual(first.opportunity.id, duplicate.opportunity.id)
        self.assertEqual(
            duplicate.opportunity.preferred_source_policy_version,
            PREFERRED_SOURCE_POLICY_VERSION,
        )
        self.assertEqual(
            duplicate.opportunity.preferred_source.raw_message_id,
            original_message.message.id,
        )
        self.assertEqual(
            duplicate.opportunity.preferred_source.source_id,
            self.sources[1].id,
        )
        self.assertEqual(
            {item.raw_message_id for item in duplicate.opportunity.alternate_sources},
            {later_message.message.id},
        )
        self.assertEqual(len(duplicate.opportunity.source_observations), 2)
        self.assertEqual(
            set(duplicate.opportunity.analysis_cache_ids),
            {later_cache, original_cache},
        )
        structured_link = next(
            link
            for link in duplicate.opportunity.analysis_links
            if link.analysis_cache_id == original_cache
        )
        self.assertEqual(structured_link.dedup_relation, STRUCTURED_DEDUP_RELATION)
        self.assertEqual(
            structured_link.dedup_algorithm_version,
            STRUCTURED_DEDUP_ALGORITHM_VERSION,
        )
        self.assertEqual(
            structured_link.matched_analysis_cache_id,
            later_cache,
        )
        self.assertEqual(
            structured_link.dedup_evidence["decision_rule"],
            "shared_contact_and_task",
        )
        self.assertNotIn("buyer", json.dumps(structured_link.dedup_evidence))
        self.assertFalse(duplicate.opportunity.analysis.budget.known)

    async def test_shared_contact_and_category_do_not_merge_different_tasks(self):
        first_text = "@agency wants a support bot for retail customer questions"
        second_text = "@agency wants an executive banking analytics dashboard"
        first_message = await self._ingest(0, 761, NOW, content=first_text)
        second_message = await self._ingest(1, 762, NOW, content=second_text)
        first_cache = await self._cache_entry("distinct-a", content=first_text)
        second_cache = await self._cache_entry("distinct-b", content=second_text)

        async with self.database.transaction() as connection:
            first = await self.repository.ensure_from_analysis(
                connection,
                analysis_cache_id=first_cache,
                raw_message_ids=(first_message.message.id,),
                analysis=_analysis(
                    task_summary=(
                        "Build Telegram support bot for online retail customer service"
                    ),
                    telegram="@agency",
                ),
            )
        async with self.database.transaction() as connection:
            second = await self.repository.ensure_from_analysis(
                connection,
                analysis_cache_id=second_cache,
                raw_message_ids=(second_message.message.id,),
                analysis=_analysis(
                    task_summary=(
                        "Create banking analytics dashboard for executive finance team"
                    ),
                    telegram="@agency",
                ),
            )

        self.assertTrue(first.created)
        self.assertTrue(second.created)
        self.assertNotEqual(first.opportunity.id, second.opportunity.id)

    async def test_structured_duplicate_respects_time_window(self):
        task = (
            "Implement Telegram mini app booking flow with payments reminders and "
            "operator dashboard"
        )
        first_text = "old @buyer request about a booking product implementation"
        second_text = "new @buyer brief with unrelated wording for delivery"
        first_message = await self._ingest(0, 771, NOW, content=first_text)
        second_message = await self._ingest(
            1,
            772,
            NOW + timedelta(days=8),
            content=second_text,
        )
        first_cache = await self._cache_entry("signals-window-a", content=first_text)
        second_cache = await self._cache_entry("signals-window-b", content=second_text)
        analysis = _analysis(task_summary=task, telegram="@buyer")

        async with self.database.transaction() as connection:
            first = await self.repository.ensure_from_analysis(
                connection,
                analysis_cache_id=first_cache,
                raw_message_ids=(first_message.message.id,),
                analysis=analysis,
            )
        async with self.database.transaction() as connection:
            second = await self.repository.ensure_from_analysis(
                connection,
                analysis_cache_id=second_cache,
                raw_message_ids=(second_message.message.id,),
                analysis=analysis,
            )

        self.assertTrue(first.created)
        self.assertTrue(second.created)
        self.assertNotEqual(first.opportunity.id, second.opportunity.id)

    async def test_concurrent_structured_duplicates_keep_one_canonical_identity(self):
        task = (
            "Implement Telegram mini app booking flow with payments reminders and "
            "operator dashboard"
        )
        first_text = "@buyer posted a full product request in the founders group"
        second_text = "project notice from another community with contact @buyer"
        first_message = await self._ingest(0, 781, NOW, content=first_text)
        second_message = await self._ingest(1, 782, NOW, content=second_text)
        first_cache = await self._cache_entry("signals-concurrent-a", content=first_text)
        second_cache = await self._cache_entry(
            "signals-concurrent-b",
            content=second_text,
        )
        analysis = _analysis(
            known_budget=False,
            task_summary=task,
            telegram="@buyer",
        )

        async def ensure(cache_id, raw_message_id):
            async with self.database.transaction() as connection:
                return await self.repository.ensure_from_analysis(
                    connection,
                    analysis_cache_id=cache_id,
                    raw_message_ids=(raw_message_id,),
                    analysis=analysis,
                )

        outcomes = await asyncio.gather(
            ensure(first_cache, first_message.message.id),
            ensure(second_cache, second_message.message.id),
        )

        self.assertEqual(
            {outcome.opportunity.id for outcome in outcomes},
            {outcomes[0].opportunity.id},
        )
        self.assertEqual(sum(outcome.created for outcome in outcomes), 1)
        self.assertEqual(
            {outcome.dedup_relation for outcome in outcomes},
            {"canonical", STRUCTURED_DEDUP_RELATION},
        )
        self.assertEqual(len(outcomes[-1].opportunity.source_observations), 2)

    async def test_later_observation_reactivates_stale_but_not_retracted(self):
        cache_id = await self._cache_entry("lifecycle-a")
        async with self.database.transaction() as connection:
            created = await self.repository.ensure_from_analysis(
                connection,
                analysis_cache_id=cache_id,
                raw_message_ids=(self.messages[0].message.id,),
                analysis=_analysis(),
            )
        self.assertEqual(
            created.opportunity.lifecycle_status,
            OpportunityLifecycleStatus.ACTIVE,
        )
        self.assertEqual(len(created.opportunity.lifecycle_events), 1)
        self.assertIsNone(created.opportunity.lifecycle_events[0].from_status)
        self.assertEqual(
            created.opportunity.lifecycle_events[0].evidence_raw_message_id,
            self.messages[0].message.id,
        )

        async with self.database.transaction() as connection:
            stale = await self.repository.transition_lifecycle(
                connection,
                created.opportunity.id,
                OpportunityLifecycleStatus.STALE,
                reason="no recent buyer activity",
                evidence_raw_message_id=self.messages[0].message.id,
            )
        self.assertTrue(stale.changed)
        later_message = await self._ingest(
            1,
            811,
            NOW + timedelta(days=2),
        )
        async with self.database.transaction() as connection:
            reactivated = await self.repository.ensure_from_analysis(
                connection,
                analysis_cache_id=cache_id,
                raw_message_ids=(later_message.message.id,),
                analysis=_analysis(),
            )
        self.assertEqual(
            reactivated.opportunity.lifecycle_status,
            OpportunityLifecycleStatus.ACTIVE,
        )
        self.assertEqual(
            [event.to_status for event in reactivated.opportunity.lifecycle_events],
            [
                OpportunityLifecycleStatus.ACTIVE,
                OpportunityLifecycleStatus.STALE,
                OpportunityLifecycleStatus.ACTIVE,
            ],
        )
        self.assertEqual(
            reactivated.opportunity.lifecycle_events[-1].evidence_raw_message_id,
            later_message.message.id,
        )

        async with self.database.transaction() as connection:
            retracted = await self.repository.transition_lifecycle(
                connection,
                created.opportunity.id,
                OpportunityLifecycleStatus.RETRACTED,
                reason="buyer retracted the request",
                evidence_raw_message_id=later_message.message.id,
            )
        self.assertTrue(retracted.changed)
        final_message = await self._ingest(
            0,
            812,
            NOW + timedelta(days=3),
        )
        async with self.database.transaction() as connection:
            repeated = await self.repository.ensure_from_analysis(
                connection,
                analysis_cache_id=cache_id,
                raw_message_ids=(final_message.message.id,),
                analysis=_analysis(),
            )
        self.assertEqual(
            repeated.opportunity.lifecycle_status,
            OpportunityLifecycleStatus.RETRACTED,
        )
        self.assertEqual(len(repeated.opportunity.lifecycle_events), 4)
        self.assertEqual(len(repeated.opportunity.source_observations), 3)
        self.assertEqual(
            set(repeated.opportunity.source_message_urls),
            {
                "https://t.me/g5_opportunity_0/501",
                "https://t.me/g5_opportunity_1/811",
                "https://t.me/g5_opportunity_0/812",
            },
        )

    async def test_closed_suppressed_and_operator_override_preserve_history(self):
        cache_id = await self._cache_entry("lifecycle-b")
        async with self.database.transaction() as connection:
            created = await self.repository.ensure_from_analysis(
                connection,
                analysis_cache_id=cache_id,
                raw_message_ids=(self.messages[0].message.id,),
                analysis=_analysis(),
            )
            closed = await self.repository.transition_lifecycle(
                connection,
                created.opportunity.id,
                OpportunityLifecycleStatus.CLOSED,
                reason="buyer confirmed the role was filled",
                evidence_raw_message_id=self.messages[0].message.id,
            )
        self.assertEqual(
            closed.opportunity.lifecycle_status,
            OpportunityLifecycleStatus.CLOSED,
        )

        async with self.database.transaction() as connection:
            reopened = await self.repository.override_lifecycle(
                connection,
                created.opportunity.id,
                OpportunityLifecycleStatus.ACTIVE,
                operator_id="operator:g5",
                reason="buyer supplied a verified reopening",
            )
            suppressed = await self.repository.transition_lifecycle(
                connection,
                created.opportunity.id,
                OpportunityLifecycleStatus.SUPPRESSED,
                reason="moderation policy",
            )
        self.assertTrue(reopened.changed)
        self.assertEqual(reopened.event.actor_kind, "operator")
        self.assertEqual(reopened.event.actor_id, "operator:g5")
        self.assertEqual(
            suppressed.opportunity.lifecycle_status,
            OpportunityLifecycleStatus.SUPPRESSED,
        )
        self.assertEqual(
            [event.to_status for event in suppressed.opportunity.lifecycle_events],
            [
                OpportunityLifecycleStatus.ACTIVE,
                OpportunityLifecycleStatus.CLOSED,
                OpportunityLifecycleStatus.ACTIVE,
                OpportunityLifecycleStatus.SUPPRESSED,
            ],
        )
        self.assertEqual(len(suppressed.opportunity.raw_message_ids), 1)

        with self.assertRaises(InvalidOpportunityTransition):
            async with self.database.transaction() as connection:
                await self.repository.transition_lifecycle(
                    connection,
                    created.opportunity.id,
                    OpportunityLifecycleStatus.CLOSED,
                    reason="invalid terminal transition",
                )

    async def test_lifecycle_evidence_must_belong_to_canonical_opportunity(self):
        cache_id = await self._cache_entry("lifecycle-c")
        async with self.database.transaction() as connection:
            created = await self.repository.ensure_from_analysis(
                connection,
                analysis_cache_id=cache_id,
                raw_message_ids=(self.messages[0].message.id,),
                analysis=_analysis(),
            )

        with self.assertRaises(OpportunityLinkConflict):
            async with self.database.transaction() as connection:
                await self.repository.transition_lifecycle(
                    connection,
                    created.opportunity.id,
                    OpportunityLifecycleStatus.RETRACTED,
                    reason="invalid evidence",
                    evidence_raw_message_id=self.messages[1].message.id,
                )
        async with self.database.connect() as connection:
            unchanged = await self.repository.get(connection, created.opportunity.id)
        self.assertEqual(
            unchanged.lifecycle_status,
            OpportunityLifecycleStatus.ACTIVE,
        )
        self.assertEqual(len(unchanged.lifecycle_events), 1)

    async def test_concurrent_lifecycle_transition_records_one_change(self):
        cache_id = await self._cache_entry("lifecycle-d")
        async with self.database.transaction() as connection:
            created = await self.repository.ensure_from_analysis(
                connection,
                analysis_cache_id=cache_id,
                raw_message_ids=(self.messages[0].message.id,),
                analysis=_analysis(),
            )

        async def mark_stale():
            async with self.database.transaction() as connection:
                return await self.repository.transition_lifecycle(
                    connection,
                    created.opportunity.id,
                    OpportunityLifecycleStatus.STALE,
                    reason="stale policy",
                )

        outcomes = await asyncio.gather(mark_stale(), mark_stale())

        self.assertEqual(sum(outcome.changed for outcome in outcomes), 1)
        async with self.database.connect() as connection:
            current = await self.repository.get(connection, created.opportunity.id)
        self.assertEqual(current.lifecycle_status, OpportunityLifecycleStatus.STALE)
        self.assertEqual(len(current.lifecycle_events), 2)

    async def test_negative_analysis_and_conflicting_message_link_do_not_mutate(self):
        first_cache = await self._cache_entry("d")
        second_cache = await self._cache_entry("e")
        async with self.database.transaction() as connection:
            original = await self.repository.ensure_from_analysis(
                connection,
                analysis_cache_id=first_cache,
                raw_message_ids=(self.messages[0].message.id,),
                analysis=_analysis(),
            )

        with self.assertRaises(OpportunityLinkConflict):
            async with self.database.transaction() as connection:
                await self.repository.ensure_from_analysis(
                    connection,
                    analysis_cache_id=second_cache,
                    raw_message_ids=(self.messages[0].message.id,),
                    analysis=_analysis(),
                )
        with self.assertRaises(ValueError):
            async with self.database.transaction() as connection:
                await self.repository.ensure_from_analysis(
                    connection,
                    analysis_cache_id=second_cache,
                    raw_message_ids=(self.messages[1].message.id,),
                    analysis=_negative_analysis(),
                )

        async with self.database.connect() as connection:
            records = (
                (await connection.execute(sa.select(opportunities.c.id)))
                .scalars()
                .all()
            )
        self.assertEqual(records, [original.opportunity.id])

    async def _ingest(
        self,
        source_index,
        external_message_id,
        observed_at,
        *,
        content="Need a Telegram bot developer",
    ):
        return await RawMessageIngestor(self.database).ingest(
            RawMessageInput(
                source_id=self.sources[source_index].id,
                collector_account_id=self.account.id,
                external_message_id=external_message_id,
                message_date=observed_at,
                observed_at=observed_at,
                message_url=(
                    f"https://t.me/g5_opportunity_{source_index}/"
                    f"{external_message_id}"
                ),
                content=content,
                transport_metadata={},
                ingestion_origin=RawMessageOrigin.LIVE,
                correlation_id=TRACE_ID,
            )
        )

    async def _cache_entry(self, marker: str, *, content: str | None = None):
        cache_id = uuid4()
        normalized_content = f"fixture-{marker}" if content is None else content
        content_hash = sha256(normalized_content.encode("utf-8")).hexdigest()
        input_hash = sha256(f"input:{marker}".encode("utf-8")).hexdigest()
        async with self.database.transaction() as connection:
            await connection.execute(
                opportunity_analysis_cache.insert().values(
                    id=cache_id,
                    normalized_content=normalized_content,
                    normalized_content_sha256=content_hash,
                    analysis_input_sha256=input_hash,
                    analyzer_version="fixture-analyzer.v1",
                    analysis_schema_version=OPPORTUNITY_ANALYSIS_SCHEMA_VERSION,
                    result={"fixture": marker},
                )
            )
        return cache_id


def _analysis(
    *,
    known_budget=True,
    task_summary="Build a Telegram ordering bot",
    role_title="Telegram bot developer",
    telegram=None,
    budget_min=1000,
    budget_max=1500,
) -> OpportunityAnalysis:
    budget = (
        {
            "known": True,
            "min": budget_min,
            "max": budget_max,
            "currency": "USD",
            "period": "project",
            "explicit": True,
        }
        if known_budget
        else {
            "known": False,
            "min": None,
            "max": None,
            "currency": None,
            "period": None,
            "explicit": False,
        }
    )
    return OpportunityAnalysis.model_validate_json(
        json.dumps(
            {
                "schema_version": OPPORTUNITY_ANALYSIS_SCHEMA_VERSION,
                "is_opportunity": True,
                "confidence": 0.92,
                "market_direction": "buyer_to_specialist",
                "intent_stage": "active",
                "opportunity_type": "project",
                "category": "telegram_automation",
                "role_title": role_title,
                "skills": ["Python", "Telegram Bot API"],
                "task_summary": task_summary,
                "budget": budget,
                "work": {
                    "remote": True,
                    "location": None,
                    "full_time": None,
                    "part_time": None,
                },
                "language": "en",
                "contact": {
                    "telegram": telegram,
                    "email": None,
                    "url": None,
                },
                "quality": {
                    "actionability": 0.9,
                    "commercial_plausibility": 0.85,
                    "specificity": 0.8,
                    "credibility": 0.75,
                },
                "red_flags": [],
            }
        ),
        strict=True,
    )


def _negative_analysis() -> OpportunityAnalysis:
    payload = json.loads(_analysis().model_dump_json())
    payload.update(
        is_opportunity=False,
        market_direction="specialist_to_buyer",
        intent_stage="none",
        opportunity_type="unknown",
    )
    return OpportunityAnalysis.model_validate_json(json.dumps(payload), strict=True)


if __name__ == "__main__":
    unittest.main()
