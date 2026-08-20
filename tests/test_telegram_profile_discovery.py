from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from types import SimpleNamespace
import unittest
from uuid import uuid4

from telethon.tl.types import Channel, User

from freelancer_bot.discovery import DiscoveryRequest
from freelancer_bot.persistence.search_profiles import (
    SearchProfileConfirmationStatus,
    SearchProfileRecord,
)
from freelancer_bot.profile_discovery import build_profile_discovery_intent
from freelancer_bot.search_profiles import (
    parse_search_profile,
    parse_search_profile_preferences,
)
from freelancer_bot.telegram_profile_discovery import (
    TelegramGlobalSearchProvider,
    TelegramGlobalSearchPageCache,
    build_telegram_profile_search_queries,
)


NOW = datetime(2026, 8, 16, 12, 0, tzinfo=timezone.utc)


def _persisted_profile(
    *,
    roles: tuple[str, ...],
    skills: tuple[str, ...],
    categories: tuple[str, ...],
    languages: tuple[str, ...] | None,
    semantic_text: str,
) -> SearchProfileRecord:
    parsed = parse_search_profile(
        roles=roles,
        skills=skills,
        categories=categories,
        semantic_text=semantic_text,
    )
    preferences = parse_search_profile_preferences(
        **({} if languages is None else {"languages": languages})
    )
    return SearchProfileRecord(
        id=uuid4(),
        user_id=uuid4(),
        schema_version=parsed.schema_version,
        parser_version=parsed.parser_version,
        analysis_cache_id=None,
        roles=parsed.roles,
        skills=parsed.skills,
        categories=parsed.categories,
        semantic_text_original=parsed.semantic_text_original,
        semantic_text_normalized=parsed.semantic_text_normalized,
        preferences=preferences,
        confirmation_status=SearchProfileConfirmationStatus.CONFIRMED,
        revision=1,
        confirmed_at=NOW,
        is_active=True,
        is_primary=True,
        activated_at=NOW,
        deactivated_at=None,
        created_at=NOW,
        updated_at=NOW,
    )


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

    def test_production_profile_portfolio_keeps_broad_and_buyer_recall(self):
        profile = _persisted_profile(
            roles=("Python Developer", "Telegram Bot Developer"),
            skills=(
                "Python",
                "Telegram Bots",
                "API integrations",
                "Backend",
                "Automation",
            ),
            categories=(
                "Telegram bot development",
                "Python backend",
                "automation",
            ),
            languages=None,
            semantic_text=(
                "Я Python-разработчик и специалист по автоматизации. "
                "Делаю Telegram-ботов и backend на Python."
            ),
        )
        intent = build_profile_discovery_intent(profile)
        first = build_telegram_profile_search_queries(intent, max_queries=20)
        second = build_telegram_profile_search_queries(intent, max_queries=20)
        texts = tuple(query.text.casefold() for query in first)

        self.assertEqual(first, second)
        self.assertLessEqual(len(first), 20)
        self.assertEqual(len(texts), len(set(texts)))
        self.assertTrue(any(query.tier == "broad" for query in first))
        self.assertTrue(any(query.tier == "short_buyer" for query in first))
        self.assertTrue(any(query.tier == "long_buyer" for query in first))
        self.assertTrue(
            any(query.tier == "broad" and query.text.casefold() == "python" for query in first)
        )
        self.assertTrue(
            any(
                query.tier == "short_buyer"
                and "ищу" in query.text.casefold()
                and "telegram" in query.text.casefold()
                for query in first
            )
        )
        self.assertTrue(
            any(
                query.tier == "short_buyer"
                and any(
                    marker in query.text.casefold()
                    for marker in ("looking for", "need ", "hiring ")
                )
                for query in first
            )
        )
        self.assertTrue(all(query.priority in {70, 80, 90} for query in first))
        self.assertFalse(any("video editor" in text for text in texts))
        self.assertFalse(any("copywriter" in text for text in texts))

    def test_english_only_profile_keeps_english_wrappers(self):
        intent = SimpleNamespace(
            languages=("en",),
            roles=("Video Editor",),
            services=(),
            skills=("Premiere Pro",),
            industries=(),
        )

        queries = build_telegram_profile_search_queries(intent, max_queries=20)
        texts = tuple(query.text.casefold() for query in queries)
        self.assertEqual({query.language for query in queries}, {"en"})
        self.assertIn("video editor", texts)
        self.assertTrue(any("looking for video editor" in text for text in texts))
        self.assertFalse(any(marker in text for text in texts for marker in ("ищу", "нужен", "требуется")))

    def test_cyrillic_term_uses_russian_wrappers_only(self):
        intent = SimpleNamespace(
            languages=("ru", "en"),
            roles=("Телеграм-боты",),
            services=(),
            skills=(),
            industries=(),
        )

        queries = build_telegram_profile_search_queries(intent, max_queries=20)
        matching = [
            query for query in queries if "телеграм-боты" in query.text.casefold()
        ]
        self.assertTrue(matching)
        self.assertTrue(all(query.language == "ru" for query in matching))
        self.assertFalse(
            any("looking for телеграм-боты" in query.text.casefold() for query in matching)
        )

    def test_real_search_profile_path_drives_profile_specific_queries(self):
        fixtures = {
            "video_editor": {
                "roles": ("Video Editor",),
                "skills": ("Premiere Pro", "After Effects"),
                "categories": ("YouTube editing", "short-form video"),
                "required": ("video editor", "youtube editing"),
                "semantic_text": (
                    "I am a Video Editor working on YouTube editing and short-form video."
                ),
            },
            "product_designer": {
                "roles": ("Product Designer", "UX/UI Designer"),
                "skills": ("Figma", "user research"),
                "categories": ("product design", "UX/UI design"),
                "required": ("product designer", "product design"),
                "semantic_text": (
                    "I am a Product Designer focused on product design and UX/UI design."
                ),
            },
            "copywriter": {
                "roles": ("Copywriter",),
                "skills": ("SEO writing", "editing"),
                "categories": ("website copy", "email sequences"),
                "required": ("copywriter", "website copy"),
                "semantic_text": (
                    "I am a Copywriter creating website copy and email sequences."
                ),
            },
            "performance_marketer": {
                "roles": ("Performance Marketer",),
                "skills": ("analytics", "conversion tracking"),
                "categories": ("paid ads", "performance campaigns"),
                "required": ("performance marketer", "paid ads"),
                "semantic_text": (
                    "I am a Performance Marketer focused on paid ads and performance campaigns."
                ),
            },
            "three_d_cgi": {
                "roles": ("3D Artist", "CGI Artist"),
                "skills": ("Blender", "rendering"),
                "categories": ("3D modeling", "CGI rendering"),
                "required": ("3d artist", "cgi rendering"),
                "semantic_text": (
                    "I am a 3D Artist creating 3D modeling and CGI rendering."
                ),
            },
            "python_telegram": {
                "roles": ("Python-разработчик",),
                "skills": ("Python", "Telethon", "PostgreSQL"),
                "categories": ("Telegram-боты", "парсеры"),
                "required": ("python-разработчик", "telegram-боты"),
                "semantic_text": (
                    "Я Python-разработчик, делаю Telegram-боты и парсеры."
                ),
            },
        }

        for name, fixture in fixtures.items():
            profile = _persisted_profile(
                roles=fixture["roles"],
                skills=fixture["skills"],
                categories=fixture["categories"],
                # This is the shape persisted by AI onboarding: optional
                # language preferences are empty, so discovery infers the
                # usable query languages from the normalized profile text.
                languages=None,
                semantic_text=fixture["semantic_text"],
            )
            intent = build_profile_discovery_intent(profile)
            queries = build_telegram_profile_search_queries(intent, max_queries=20)
            texts = tuple(query.text.casefold() for query in queries)

            self.assertEqual(
                intent.services,
                fixture["categories"],
                name,
            )
            self.assertLessEqual(len(queries), 20, name)
            self.assertEqual(
                len(texts),
                len(set(texts)),
                name,
            )
            for required in fixture["required"]:
                self.assertTrue(
                    any(required in text for text in texts),
                    (name, required, texts),
                )
            if name != "python_telegram":
                self.assertFalse(any("python" in text for text in texts), name)
                self.assertFalse(any("telegram" in text for text in texts), name)
                self.assertFalse(any("developer" in text for text in texts), name)
            else:
                self.assertTrue(any("python" in text for text in texts))
                self.assertTrue(any("telegram" in text for text in texts))
            if name == "video_editor":
                self.assertFalse(any("video editor developer" in text for text in texts))

            for term in fixture["required"]:
                matching = [query for query in queries if term in query.text.casefold()]
                expected_language = (
                    "ru"
                    if any("а" <= char <= "я" for char in term)
                    else "en"
                )
                self.assertTrue(matching, (name, term))
                self.assertTrue(
                    all(query.language == expected_language for query in matching),
                    (name, term, matching),
                )

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
            buyer_markers = (
                "нужен",
                "нужна",
                "ищу",
                "ищем",
                "кто может",
                "требуется",
                "vacancy",
                "вакансия",
                "проект",
                "посоветуйте",
                "looking for",
                "who can",
                "need",
                "hiring",
                "needed",
                "recommend",
                "contract",
                "freelance",
            )
            self.assertTrue(
                any(any(marker in text for marker in buyer_markers) for text in texts),
                name,
            )
            self.assertTrue(
                all(
                    query.tier == "broad"
                    or any(marker in query.text.casefold() for marker in buyer_markers)
                    for query in first
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
        families = {
            match.family for match in provider.search_hits[0].query_matches
        }
        self.assertIn("TERM_BROAD", families)
        self.assertTrue(
            families.intersection(
                {"ROLE_SHORT", "SERVICE_SHORT", "SKILL_SERVICE_SHORT"}
            )
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
