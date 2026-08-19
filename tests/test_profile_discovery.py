from __future__ import annotations

import unittest
from datetime import datetime, timezone
from types import SimpleNamespace

import sqlalchemy as sa

from freelancer_bot.discovery import DiscoveryRequest
from freelancer_bot.persistence.database import Database
from freelancer_bot.persistence.schema import (
    discovery_runs,
    profile_discovery_intents,
    source_profile_relevance,
    sources,
)
from freelancer_bot.profile_discovery import (
    ProfileDiscoveryService,
    build_evaluation_intent,
    build_profile_discovery_intent,
    evaluation_profile_specs,
    evaluate_source_relevance,
    web_strategy_for_intent,
)
from freelancer_bot.profile_confirmation import ProfileConfirmationService
from freelancer_bot.web_discovery import (
    WebSearchResult,
    collapse_near_duplicate_queries,
)
from postgres_support import TEST_DATABASE_URL, migrate_to_head, temporary_database


NOW = datetime(2026, 8, 16, 12, 0, tzinfo=timezone.utc)


class ProfileDiscoveryIntentTest(unittest.TestCase):
    def test_ten_evaluation_intents_are_deterministic_and_materially_distinct(self):
        specs = evaluation_profile_specs()
        intents = [build_evaluation_intent(spec) for spec in specs]

        self.assertEqual(len(specs), 10)
        self.assertEqual(len({intent.id for intent in intents}), 10)
        self.assertEqual(
            len({intent.literal_concepts for intent in intents}),
            10,
        )
        expected_role_terms = (
            ("Python developer", "backend developer", "Telegram bot developer"),
            ("Product Designer", "UX/UI Designer"),
            ("Graphic Designer", "Brand Designer"),
            ("SMM Manager", "Social Media Manager", "Performance Marketer"),
            ("Video Editor", "Motion Designer"),
            ("Photographer", "Content Creator", "UGC Creator"),
            ("Copywriter", "Content Writer", "Content Manager"),
            ("Recruiter", "Talent Acquisition", "HR"),
            ("Sales Manager", "Business Development Manager"),
            ("Marketplace Manager", "E-commerce Specialist"),
        )
        for spec, expected in zip(specs, expected_role_terms):
            self.assertEqual(spec.roles, expected)
        for intent in intents:
            strategy = web_strategy_for_intent(intent)
            queries = strategy.build_queries(
                DiscoveryRequest(parameters={}, requested_at=NOW)
            )
            self.assertGreaterEqual(len(queries), 10)
            self.assertTrue(any(query.angle == "direct" for query in queries))
            self.assertTrue(any(query.angle == "buyer_habitat" for query in queries))
            self.assertTrue(any(query.angle == "adjacent" for query in queries))
            self.assertTrue(intent.generated_web_queries)
            self.assertTrue(intent.likely_buyer_roles)
            self.assertTrue(intent.buyer_habitats)

    def test_relevance_is_explainable_and_does_not_make_irrelevant_sources_strong(self):
        intent = build_evaluation_intent(evaluation_profile_specs()[0])
        relevant_source = SimpleNamespace(
            id=11,
            platform="telegram",
            external_id="username:python_buyers",
            display_name="Python Telegram Bots Automation Buyers",
        )
        relevant_lineage = SimpleNamespace(
            context={
                "matches": [
                    {
                        "topic": "Python developer hiring communities",
                        "query_angle": "buyer_habitat",
                        "query": "site:t.me python developer",
                    },
                    {
                        "topic": "Telethon implementation",
                        "query_angle": "direct",
                    },
                ]
            }
        )
        relevant = evaluate_source_relevance(intent, relevant_source, (relevant_lineage,))
        irrelevant = evaluate_source_relevance(
            intent,
            SimpleNamespace(
                id=12,
                platform="telegram",
                external_id="username:cooking",
                display_name="Home Cooking Recipes",
            ),
            (),
        )

        self.assertEqual(relevant.relevance_class, "strong")
        self.assertIn("direct_concept", relevant.evidence_categories)
        self.assertIn("buyer_habitat", relevant.evidence_categories)
        self.assertEqual(irrelevant.relevance_class, "weak")

    def test_generic_buyer_habitat_never_becomes_high_without_support(self):
        intent = build_evaluation_intent(evaluation_profile_specs()[0])
        generic = evaluate_source_relevance(
            intent,
            SimpleNamespace(
                id=13,
                platform="telegram",
                external_id="username:generic_business",
                display_name="Founders and Operators Community",
            ),
            (
                SimpleNamespace(
                    context={
                        "matches": [
                            {
                                "topic": "founder and operator communities",
                                "query_angle": "buyer_habitat",
                            }
                        ]
                    }
                ),
            ),
        )

        self.assertNotEqual(generic.relevance_class, "strong")
        self.assertEqual(generic.explanation.semantic_category, "generic_business")
        self.assertEqual(generic.explanation.diagnostic_label, "WEAK")
        self.assertNotIn("direct_concept", generic.evidence_categories)


@unittest.skipUnless(TEST_DATABASE_URL, "TEST_DATABASE_URL is not configured")
class ProfileDiscoveryPostgresIntegrationTest(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.database_context = temporary_database()
        self.database_url = self.database_context.__enter__()
        migrate_to_head(self.database_url)
        self.database = Database(self.database_url, pool_size=4, max_overflow=8)

    async def asyncTearDown(self):
        await self.database.close()
        self.database_context.__exit__(None, None, None)

    async def test_activation_persists_one_intent_and_web_discovery_is_idempotent(self):
        confirmation = ProfileConfirmationService(self.database)
        draft = await confirmation.create_manual_draft(
            platform="telegram",
            external_user_id="profile-discovery-owner",
            semantic_text="Python Telegram automation",
            roles=("Python developer",),
            skills=("Telethon", "PostgreSQL"),
            categories=("Telegram bots",),
        )
        confirmed = await confirmation.confirm(
            platform="telegram",
            external_user_id="profile-discovery-owner",
            profile_id=draft.profile.id,
            expected_revision=draft.profile.revision,
        )
        activated = await confirmation.activate(
            platform="telegram",
            external_user_id="profile-discovery-owner",
            profile_id=draft.profile.id,
            expected_revision=confirmed.profile.revision,
        )

        class Backend:
            def __init__(self):
                self.calls = 0

            async def search(self, query, *, language, limit):
                self.calls += 1
                return (
                    WebSearchResult(
                        "https://t.me/python_automation_buyers/1",
                        "Python Telegram Automation Buyers",
                        "Need a Telethon implementation partner",
                    ),
                )

        backend = Backend()
        service = ProfileDiscoveryService(self.database)
        first = await service.discover_profile(
            activated.profile.profile,
            requested_at=NOW,
            run_key="profile-discovery-integration-v1",
            backend=backend,
        )
        second = await service.discover_profile(
            activated.profile.profile,
            requested_at=NOW,
            run_key="profile-discovery-integration-v1",
            backend=backend,
        )

        async with self.database.connect() as connection:
            intent_count = await connection.scalar(
                sa.select(sa.func.count()).select_from(profile_discovery_intents)
            )
            relevance_count = await connection.scalar(
                sa.select(sa.func.count()).select_from(source_profile_relevance)
            )
            source_count = await connection.scalar(
                sa.select(sa.func.count()).select_from(sources)
            )
            run_count = await connection.scalar(
                sa.select(sa.func.count())
                .select_from(discovery_runs)
                .where(discovery_runs.c.run_key == "profile-discovery-integration-v1")
            )

        self.assertTrue(activated.profile.profile.is_active)
        self.assertEqual(intent_count, 1)
        self.assertEqual(relevance_count, 1)
        self.assertEqual(source_count, 1)
        self.assertEqual(run_count, 1)
        self.assertEqual(first.new_candidates, 1)
        self.assertEqual(second.unique_candidates, 1)
        queries = web_strategy_for_intent(first.intent).build_queries(
            DiscoveryRequest(parameters={}, requested_at=NOW)
        )
        self.assertEqual(
            backend.calls,
            len(collapse_near_duplicate_queries(queries).queries),
        )


if __name__ == "__main__":
    unittest.main()
