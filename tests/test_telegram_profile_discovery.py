from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from types import SimpleNamespace
import unittest

from telethon.tl.types import Channel, User

from freelancer_bot.discovery import DiscoveryRequest
from freelancer_bot.telegram_profile_discovery import (
    TelegramGlobalSearchProvider,
    TelegramGlobalSearchPageCache,
    build_telegram_profile_search_queries,
)


NOW = datetime(2026, 8, 16, 12, 0, tzinfo=timezone.utc)


class _Governor:
    collector_account_id = 23

    def __init__(self):
        self.categories: list[str] = []

    async def run(self, category, operation):
        self.categories.append(category)
        return await operation()


class _Client:
    def __init__(self):
        self.calls: list[tuple[object, str, int, int]] = []

    async def get_messages(self, entity, *, search, limit, offset_id=0):
        self.calls.append((entity, search, limit, offset_id))
        channel = Channel(
            id=123,
            title="Python Telegram buyers",
            photo=None,
            date=None,
            username="python_buyers",
            megagroup=True,
        )
        user = User(id=99, bot=False, first_name="ignored")
        return (
            SimpleNamespace(
                id=101,
                chat=channel,
                date=NOW,
                message="private body must not persist",
            ),
            SimpleNamespace(
                id=102,
                chat=channel,
                date=NOW,
                message="another private body",
            ),
            SimpleNamespace(id=103, chat=user, date=NOW, message="user result"),
        )


class TelegramProfileDiscoveryTest(unittest.IsolatedAsyncioTestCase):
    def _intent(self):
        return SimpleNamespace(
            id="00000000-0000-0000-0000-000000000001",
            profile_revision=3,
            languages=("ru", "en"),
            roles=("Python-разработчик",),
            skills=("Python", "Telethon"),
            services=("Telegram-боты",),
            industries=("Telegram-боты",),
        )

    def test_queries_are_bounded_and_cover_both_buyer_languages(self):
        queries = build_telegram_profile_search_queries(self._intent())

        self.assertEqual(len(queries), 16)
        self.assertEqual({query.language for query in queries}, {"ru", "en"})
        self.assertTrue(any("looking for" in query.text for query in queries))
        self.assertTrue(any("ищу" in query.text for query in queries))
        self.assertTrue(all("community" not in query.text for query in queries))

    def test_query_count_is_hard_bounded_and_queries_are_unique(self):
        queries = build_telegram_profile_search_queries(
            self._intent(),
            max_queries=20,
        )

        self.assertEqual(len(queries), 20)
        self.assertEqual(
            len({query.text.casefold() for query in queries}),
            len(queries),
        )
        self.assertEqual({query.language for query in queries}, {"ru", "en"})

    def test_empty_profile_does_not_invent_an_unrelated_search_term(self):
        intent = SimpleNamespace(
            languages=("ru", "en"),
            roles=(),
            services=(),
            skills=(),
            industries=(),
        )

        self.assertEqual(build_telegram_profile_search_queries(intent), ())

    def test_query_cap_rejects_values_above_twenty(self):
        with self.assertRaises(ValueError):
            build_telegram_profile_search_queries(self._intent(), max_queries=21)

    def test_meaningful_format_terms_are_used_without_generic_work_type_noise(self):
        intent = SimpleNamespace(
            languages=("en",),
            roles=(),
            services=(),
            skills=(),
            industries=(),
            work_types=("project", "short-form video"),
            formats=("Reels",),
        )

        texts = {
            query.text.casefold()
            for query in build_telegram_profile_search_queries(intent)
        }

        self.assertTrue(any("short-form video" in text for text in texts))
        self.assertTrue(any("reels" in text for text in texts))
        self.assertFalse(any("specialist in project" in text for text in texts))

    def test_profile_matrix_uses_buyer_intent_and_stays_profile_specific(self):
        fixtures = {
            "python_telegram": {
                "roles": ("Python-разработчик",),
                "services": ("Telegram-боты",),
                "skills": ("Python", "Telethon"),
                "languages": ("ru", "en"),
                "required": ("python", "telegram"),
            },
            "video_editor": {
                "roles": ("Video Editor",),
                "services": ("YouTube editing", "short-form video"),
                "skills": ("Premiere", "After Effects", "Reels"),
                "languages": ("en", "ru"),
                "required": ("video editor", "youtube"),
            },
            "product_designer": {
                "roles": ("Product Designer", "UX/UI Designer"),
                "services": ("product design", "user research"),
                "skills": ("Figma",),
                "languages": ("en", "ru"),
                "required": ("product designer", "product design"),
            },
            "copywriter": {
                "roles": ("Copywriter",),
                "services": ("website copy", "email sequences"),
                "skills": ("SEO writing",),
                "languages": ("en", "ru"),
                "required": ("copywriter", "website copy"),
            },
            "performance_marketer": {
                "roles": ("Performance Marketer",),
                "services": ("paid ads", "Google Ads"),
                "skills": ("analytics",),
                "languages": ("en", "ru"),
                "required": ("performance marketer", "paid ads"),
            },
            "three_d_cgi": {
                "roles": ("3D Artist", "CGI Artist"),
                "services": ("3D modeling", "CGI rendering"),
                "skills": ("Blender",),
                "languages": ("en", "ru"),
                "required": ("3d artist", "cgi"),
            },
        }

        for name, fixture in fixtures.items():
            intent = SimpleNamespace(**fixture)
            first = build_telegram_profile_search_queries(intent, max_queries=20)
            second = build_telegram_profile_search_queries(intent, max_queries=20)
            texts = tuple(query.text.casefold() for query in first)

            self.assertGreaterEqual(len(first), 10, name)
            self.assertLessEqual(len(first), 20, name)
            self.assertEqual(first, second, name)
            self.assertEqual(len(texts), len(set(texts)), name)
            self.assertTrue(
                all(
                    any(keyword in text for text in texts)
                    for keyword in fixture["required"]
                ),
                name,
            )
            self.assertTrue(
                all(
                    any(
                        marker in text
                        for marker in (
                            "нужен",
                            "ищу",
                            "ищем",
                            "кто может",
                            "требуется",
                            "вакансия",
                            "проект",
                            "посоветуйте",
                            "looking for",
                            "need",
                            "hiring",
                            "needed",
                            "recommend",
                            "contract",
                            "freelance",
                        )
                    )
                    for text in texts
                ),
                name,
            )
            if name != "python_telegram":
                self.assertFalse(any("python" in text for text in texts), name)
                self.assertFalse(any("telegram" in text for text in texts), name)
                self.assertFalse(any("automation" in text for text in texts), name)
                self.assertFalse(any("developer" in text for text in texts), name)
            if name == "video_editor":
                self.assertFalse(any("video editor developer" in text for text in texts))

    def test_profile_matrix_queries_are_printable_for_manual_quality_review(self):
        fixtures = (
            ("python_telegram", ("Python-разработчик",), ("Telegram-боты",)),
            ("video_editor", ("Video Editor",), ("YouTube editing",)),
            ("product_designer", ("Product Designer",), ("product design",)),
            ("copywriter", ("Copywriter",), ("website copy",)),
            ("performance_marketer", ("Performance Marketer",), ("paid ads",)),
            ("three_d_cgi", ("3D Artist",), ("CGI rendering",)),
        )
        for name, roles, services in fixtures:
            intent = SimpleNamespace(
                roles=roles,
                services=services,
                skills=(),
                industries=(),
                languages=("en", "ru"),
            )
            print(name, [query.text for query in build_telegram_profile_search_queries(intent)])

    async def test_search_uses_own_chat_only_deduplicates_and_preserves_safe_lineage(self):
        client = _Client()
        governor = _Governor()
        provider = TelegramGlobalSearchProvider(
            client,
            governor=governor,
            intent=self._intent(),
            queries=build_telegram_profile_search_queries(
                self._intent(),
                max_queries=8,
            ),
            known_source_identities=("known_source",),
        )

        candidates = await provider.discover(
            DiscoveryRequest(parameters={}, requested_at=NOW)
        )

        self.assertEqual(len(candidates), 1)
        self.assertEqual(candidates[0].handle, "@python_buyers")
        self.assertEqual(candidates[0].context["message_hit_count"], 16)
        self.assertNotIn("private body", str(candidates[0].context))
        self.assertEqual(len(client.calls), 8)
        self.assertTrue(
            all(entity is None and limit == 20 for entity, _, limit, _ in client.calls)
        )
        self.assertEqual(len(governor.categories), 8)
        self.assertEqual(
            set(governor.categories),
            {"global_search"},
        )
        self.assertEqual(provider.observability["unique_chat_count"], 1)
        self.assertEqual(provider.observability["unique_message_count"], 3)
        self.assertEqual(len(provider.search_hits), 3)
        self.assertIn("private body", provider.search_hits[0].text)
        self.assertIn("user", {hit.source_kind for hit in provider.search_hits})
        self.assertEqual(
            {
                match.family
                for match in provider.search_hits[0].query_matches
            },
            {
                "ROLE_DIRECT",
                "SERVICE_DIRECT",
                "SKILL_SERVICE",
            },
        )

    async def test_known_source_is_removed_without_resolving_entities(self):
        client = _Client()
        provider = TelegramGlobalSearchProvider(
            client,
            governor=_Governor(),
            intent=self._intent(),
            queries=build_telegram_profile_search_queries(
                self._intent(),
                max_queries=8,
            ),
            known_source_identities=("@python_buyers",),
        )

        candidates = await provider.discover(
            DiscoveryRequest(parameters={}, requested_at=NOW)
        )

        self.assertEqual(candidates, ())
        self.assertEqual(provider.observability["known_sources_removed"], 1)
        self.assertEqual(provider.observability["known_message_count"], 2)

    async def test_raw_global_search_pages_are_governed_and_hits_are_deduplicated(self):
        channel = Channel(
            id=456,
            title="Paged buyers",
            photo=None,
            date=None,
            username="paged_buyers",
            megagroup=True,
        )

        class RawClient:
            def __init__(self):
                self.calls = []

            async def __call__(self, request):
                self.calls.append(request)
                if request.offset_id == 0:
                    messages = (
                        SimpleNamespace(id=20, chat=channel, date=NOW, message="one"),
                        SimpleNamespace(id=19, chat=channel, date=NOW, message="two"),
                    )
                else:
                    messages = (
                        SimpleNamespace(id=19, chat=channel, date=NOW, message="two"),
                        SimpleNamespace(id=17, chat=channel, date=NOW, message="three"),
                    )
                return SimpleNamespace(
                    messages=messages,
                    users=(),
                    chats=(),
                    next_rate=0,
                )

        client = RawClient()
        provider = TelegramGlobalSearchProvider(
            client,
            governor=_Governor(),
            intent=self._intent(),
            queries=build_telegram_profile_search_queries(
                self._intent(), max_queries=1
            ),
            max_results_per_query=4,
            page_size=2,
        )

        await provider.discover(DiscoveryRequest(parameters={}, requested_at=NOW))

        self.assertEqual(len(client.calls), 2)
        self.assertEqual([request.offset_id for request in client.calls], [0, 19])
        self.assertEqual(provider.observability["request_count"], 2)
        self.assertEqual(provider.observability["raw_search_hits"], 4)
        self.assertEqual(provider.observability["unique_message_count"], 3)
        self.assertEqual(
            sorted(hit.message_id for hit in provider.search_hits),
            [17, 19, 20],
        )

    async def test_shared_page_cache_deduplicates_identical_profile_queries(self):
        client = _Client()
        queries = build_telegram_profile_search_queries(
            self._intent(),
            max_queries=1,
        )
        page_cache = TelegramGlobalSearchPageCache()
        first = TelegramGlobalSearchProvider(
            client,
            governor=_Governor(),
            intent=self._intent(),
            queries=queries,
            page_cache=page_cache,
        )
        second = TelegramGlobalSearchProvider(
            client,
            governor=_Governor(),
            intent=self._intent(),
            queries=queries,
            page_cache=page_cache,
        )

        await first.discover(DiscoveryRequest(parameters={}, requested_at=NOW))
        await second.discover(DiscoveryRequest(parameters={}, requested_at=NOW))

        self.assertEqual(len(client.calls), 1)
        self.assertEqual(first.observability["request_count"], 1)
        self.assertEqual(second.observability["request_count"], 0)
        self.assertEqual(second.observability["cache_hit_count"], 1)
        self.assertEqual(len(second.search_hits), 3)


if __name__ == "__main__":
    unittest.main()
