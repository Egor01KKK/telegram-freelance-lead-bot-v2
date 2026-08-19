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

        self.assertEqual(len(queries), 8)
        self.assertEqual({query.language for query in queries}, {"ru", "en"})
        self.assertTrue(any("looking for" in query.text for query in queries))
        self.assertTrue(any("ищу" in query.text for query in queries))
        self.assertTrue(all("community" not in query.text for query in queries))

    def test_full_quality_matrix_has_eight_diverse_families(self):
        queries = build_telegram_profile_search_queries(
            self._intent(),
            max_queries=24,
        )

        self.assertEqual(len(queries), 24)
        self.assertEqual(
            {query.family for query in queries},
            {
                "DIRECT_ROLE",
                "DIRECT_SERVICE",
                "PROBLEM_TO_SOLVE",
                "INTEGRATION",
                "RECOMMENDATION",
                "PROJECT_OUTSOURCE",
                "VACANCY_PART_TIME",
                "MINI_APP_SPECIFIC_SERVICE",
            },
        )
        self.assertEqual({query.language for query in queries}, {"ru", "en"})

    async def test_search_uses_own_chat_only_deduplicates_and_preserves_safe_lineage(self):
        client = _Client()
        governor = _Governor()
        provider = TelegramGlobalSearchProvider(
            client,
            governor=governor,
            intent=self._intent(),
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
                "DIRECT_ROLE",
                "DIRECT_SERVICE",
                "PROBLEM_TO_SOLVE",
            },
        )

    async def test_known_source_is_removed_without_resolving_entities(self):
        client = _Client()
        provider = TelegramGlobalSearchProvider(
            client,
            governor=_Governor(),
            intent=self._intent(),
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
