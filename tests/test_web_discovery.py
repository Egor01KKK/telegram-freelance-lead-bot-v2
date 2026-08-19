import json
import unittest
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch
import urllib.error
import urllib.parse
from types import SimpleNamespace

import sqlalchemy as sa

from freelancer_bot.discovery import DiscoveryProvider, DiscoveryRequest
from freelancer_bot.discovery_runner import DiscoveryRunner
from freelancer_bot.persistence.database import Database
from freelancer_bot.persistence.schema import sources
from freelancer_bot.persistence.source_repository import SourceRepository, SourceStatus
from freelancer_bot.persistence.source_seed import SourceSeedImporter
from freelancer_bot.web_discovery import (
    BuyerIntentSeed,
    CommunityCategory,
    SearxngSearchBackend,
    WebDiscoveryGovernor,
    WebDiscoveryProvider,
    WebDiscoveryQuery,
    WebDiscoveryQueryKind,
    WebDiscoveryStrategy,
    WebDiscoveryTopic,
    collapse_near_duplicate_queries,
    WebSearchBackend,
    WebSearchBackendError,
    WebSearchResult,
    WebProviderState,
)
from postgres_support import ROOT, TEST_DATABASE_URL, migrate_to_head, temporary_database
from freelancer_bot.web_provider_chain import (
    BraveSearchBackend,
    build_web_search_backends,
    web_discovery_readiness,
)
from pydantic import SecretStr


NOW = datetime(2026, 8, 9, 15, 0, tzinfo=timezone.utc)
SOURCES_PATH = ROOT / "config" / "sources.json"


class RecordingSearchBackend:
    def __init__(self, responses=None):
        self.responses = responses or {}
        self.calls = []

    async def search(self, query, *, language, limit):
        self.calls.append((query, language, limit))
        return tuple(
            result
            for marker, results in self.responses.items()
            if marker in query
            for result in results
        )[:limit]


class WebDiscoveryStrategyTest(unittest.IsolatedAsyncioTestCase):
    def test_default_strategy_covers_every_non_job_community_category(self):
        strategy = WebDiscoveryStrategy.default()
        categories = {topic.category for topic in strategy.topics}
        self.assertEqual(categories, set(CommunityCategory))

        queries = strategy.build_queries(
            DiscoveryRequest(
                parameters={"location": "Europe"},
                requested_at=NOW,
            )
        )
        self.assertTrue(
            any(query.kind is WebDiscoveryQueryKind.COMMUNITY for query in queries)
        )
        buyer_queries = [
            query for query in queries
            if query.kind is WebDiscoveryQueryKind.BUYER_INTENT
        ]
        self.assertTrue(buyer_queries)
        self.assertTrue(all(query.buyer_intent_seeds for query in buyer_queries))
        self.assertTrue(all("site:t.me" in query.text for query in queries))
        self.assertTrue(all('"Europe"' in query.text for query in queries))

    async def test_provider_normalizes_deduplicates_and_keeps_query_provenance(self):
        backend = RecordingSearchBackend(
            {
                "Product builders": (
                    WebSearchResult(
                        "https://t.me/s/ProductBuildersHub/117",
                        "Telegram: Product Builders Hub",
                        "A workflow is blocking our launch; who has solved this?",
                    ),
                    WebSearchResult("https://example.com/not-telegram", "Ignore"),
                    WebSearchResult("https://t.me/+privateInviteHash", "Private"),
                ),
            }
        )
        strategy = WebDiscoveryStrategy(
            topics=(
                WebDiscoveryTopic("founder", "Product builders", "en"),
            ),
            buyer_intent_seeds=(
                BuyerIntentSeed("need a contractor", "en"),
            ),
        )
        provider = WebDiscoveryProvider(backend, strategy=strategy)

        self.assertIsInstance(backend, WebSearchBackend)
        self.assertIsInstance(provider, DiscoveryProvider)
        candidates = await provider.discover(
            DiscoveryRequest(parameters={}, requested_at=NOW)
        )

        self.assertEqual(len(candidates), 1)
        candidate = candidates[0]
        self.assertEqual(candidate.external_id, "username:productbuildershub")
        self.assertEqual(candidate.handle, "@productbuildershub")
        self.assertEqual(candidate.canonical_url, "https://t.me/productbuildershub")
        self.assertEqual(candidate.display_name, "Product Builders Hub")
        self.assertEqual(candidate.access_type, "public")
        self.assertEqual(candidate.context["discovery_method"], "web_search")
        self.assertEqual(len(candidate.context["matches"]), 2)
        self.assertEqual(
            {match["query_kind"] for match in candidate.context["matches"]},
            {"community", "buyer_intent"},
        )
        self.assertIn(
            "A workflow is blocking our launch",
            candidate.context["matches"][0]["result_snippet"],
        )
        self.assertNotIn(
            "need a contractor",
            candidate.context["matches"][0]["result_snippet"],
        )

    def test_query_collapse_preserves_discovery_families_and_buyer_context(self):
        queries = (
            WebDiscoveryQuery(
                WebDiscoveryQueryKind.COMMUNITY,
                CommunityCategory.PROFESSION,
                "en",
                "Python developer",
                'site:t.me "Python developer" community',
                angle="direct",
            ),
            WebDiscoveryQuery(
                WebDiscoveryQueryKind.COMMUNITY,
                CommunityCategory.PROFESSION,
                "en",
                "Python developers",
                'site:t.me "Python developers" community',
                angle="direct",
            ),
            WebDiscoveryQuery(
                WebDiscoveryQueryKind.BUYER_INTENT,
                CommunityCategory.FOUNDER,
                "en",
                "startup founders",
                'site:t.me "startup founders" "need a contractor"',
                buyer_intent_seeds=("need a contractor",),
                angle="buyer_habitat",
            ),
            WebDiscoveryQuery(
                WebDiscoveryQueryKind.BUYER_INTENT,
                CommunityCategory.FOUNDER,
                "en",
                "startup marketers",
                'site:t.me "startup marketers" "need a contractor"',
                buyer_intent_seeds=("need a contractor",),
                angle="buyer_habitat",
            ),
        )

        collapsed = collapse_near_duplicate_queries(queries)

        self.assertEqual(collapsed.generated_count, 4)
        self.assertEqual(collapsed.near_duplicates, 1)
        self.assertEqual(len(collapsed.queries), 3)
        self.assertEqual(
            {query.angle for query in collapsed.queries},
            {"direct", "buyer_habitat"},
        )
        self.assertIn("startup marketers", {query.topic for query in collapsed.queries})


class SearxngSearchBackendTest(unittest.IsolatedAsyncioTestCase):
    async def test_queries_json_api_and_returns_bounded_results(self):
        response = MagicMock()
        response.read.return_value = json.dumps(
            {
                "results": [
                    {
                        "url": "https://t.me/founders_europe",
                        "title": "Founders Europe",
                        "content": "Operator community",
                    },
                    {"url": "https://example.com/ignored-by-limit"},
                ]
            }
        ).encode("utf-8")
        response_context = MagicMock()
        response_context.__enter__.return_value = response
        backend = SearxngSearchBackend(
            "https://search.example.test/",
            timeout_seconds=7,
        )

        with patch("urllib.request.urlopen", return_value=response_context) as urlopen:
            results = await backend.search(
                'site:t.me "startup founders"',
                language="en",
                limit=1,
            )

        self.assertEqual(
            results,
            (
                WebSearchResult(
                    "https://t.me/founders_europe",
                    "Founders Europe",
                    "Operator community",
                ),
            ),
        )
        request = urlopen.call_args.args[0]
        query = request.full_url.split("?", 1)[1]
        parameters = urllib.parse.parse_qs(query)
        self.assertEqual(parameters["format"], ["json"])
        self.assertEqual(parameters["language"], ["en"])
        self.assertEqual(parameters["q"], ['site:t.me "startup founders"'])
        self.assertEqual(urlopen.call_args.kwargs["timeout"], 7)

    async def test_rejects_oversized_and_malformed_responses(self):
        response = MagicMock()
        response.read.return_value = b"x" * 1025
        response_context = MagicMock()
        response_context.__enter__.return_value = response
        backend = SearxngSearchBackend(
            "http://localhost:8080",
            max_response_bytes=1024,
        )
        with patch("urllib.request.urlopen", return_value=response_context):
            with self.assertRaises(WebSearchBackendError):
                await backend.search("site:t.me test", language="en", limit=5)

        response.read.return_value = b"not-json"
        backend = SearxngSearchBackend("http://localhost:8080")
        with patch("urllib.request.urlopen", return_value=response_context):
            with self.assertRaises(WebSearchBackendError):
                await backend.search("site:t.me test", language="en", limit=5)


class WebDiscoveryGovernorTest(unittest.IsolatedAsyncioTestCase):
    async def test_deduplicates_queries_reuses_successes_and_paces_serially(self):
        backend = RecordingSearchBackend()
        governor = WebDiscoveryGovernor(
            min_delay_seconds=0,
            max_delay_seconds=0,
        )
        strategy = WebDiscoveryStrategy(
            topics=(
                WebDiscoveryTopic("profession", "Python developers", "en", "direct"),
                WebDiscoveryTopic(
                    "profession",
                    "Python developers",
                    "en",
                    "buyer_habitat",
                ),
            ),
            buyer_intent_seeds=(BuyerIntentSeed("need a contractor", "en"),),
        )
        first = WebDiscoveryProvider(backend, strategy=strategy, governor=governor)
        await first.discover(DiscoveryRequest(parameters={}, requested_at=NOW))
        second = WebDiscoveryProvider(backend, strategy=strategy, governor=governor)
        await second.discover(DiscoveryRequest(parameters={}, requested_at=NOW))

        self.assertEqual(len(backend.calls), 2)
        self.assertEqual(first.observability["queries_deduplicated"], 0)
        self.assertEqual(first.observability["queries_executable"], 4)
        self.assertEqual(first.observability["queries_reused"], 2)
        self.assertEqual(second.observability["queries_reused"], 4)
        self.assertEqual(governor.health.state, WebProviderState.READY)

    async def test_backend_failure_stops_batch_and_persists_health_class(self):
        class FailingBackend:
            async def search(self, query, *, language, limit):
                raise WebSearchBackendError(
                    "rate limited",
                    failure_class="http_429",
                )

        provider = WebDiscoveryProvider(
            FailingBackend(),
            strategy=WebDiscoveryStrategy(
                topics=(WebDiscoveryTopic("profession", "Python developers", "en"),),
                buyer_intent_seeds=(BuyerIntentSeed("need a contractor", "en"),),
            ),
            governor=WebDiscoveryGovernor(
                min_delay_seconds=0,
                max_delay_seconds=0,
                base_backoff_seconds=60,
                max_backoff_seconds=60,
            ),
        )

        candidates = await provider.discover(
            DiscoveryRequest(parameters={}, requested_at=NOW)
        )

        self.assertEqual(candidates, ())
        self.assertEqual(provider.observability["outcome"], "SEARCH_BACKEND_DEGRADED")
        self.assertEqual(provider.observability["provider_state"], "BACKOFF")
        health = provider.observability["provider_health"]
        self.assertEqual(health["http_429"], 1)
        self.assertEqual(health["consecutive_failures"], 1)

    async def test_healthy_fallback_backend_continues_after_first_backend_backoff(self):
        class FailingBackend:
            health_identity = "fragile"

            async def search(self, query, *, language, limit):
                raise WebSearchBackendError("blocked", failure_class="captcha")

        class HealthyBackend(RecordingSearchBackend):
            health_identity = "healthy"

        backend = HealthyBackend(
            {
                "Python": (
                    WebSearchResult(
                        "https://t.me/python_builders",
                        "Python builders",
                        "Technical community",
                    ),
                )
            }
        )
        provider = WebDiscoveryProvider(
            (FailingBackend(), backend),
            strategy=WebDiscoveryStrategy(
                topics=(WebDiscoveryTopic("profession", "Python", "en"),),
                buyer_intent_seeds=(BuyerIntentSeed("need a contractor", "en"),),
            ),
            governor=WebDiscoveryGovernor(min_delay_seconds=0, max_delay_seconds=0),
        )

        candidates = await provider.discover(
            DiscoveryRequest(parameters={}, requested_at=NOW)
        )

        self.assertEqual(len(candidates), 1)
        self.assertEqual(len(backend.calls), 2)
        self.assertEqual(provider.observability["backend_failures"], 2)
        self.assertEqual(
            provider.observability["query_attempts"][0]["provider_backend"],
            "fragile",
        )
        self.assertEqual(
            provider.observability["query_attempts"][1]["provider_backend"],
            "healthy",
        )


@unittest.skipUnless(TEST_DATABASE_URL, "TEST_DATABASE_URL is not configured")
class WebDiscoveryPostgresIntegrationTest(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.database_context = temporary_database()
        self.database_url = self.database_context.__enter__()
        migrate_to_head(self.database_url)
        self.database = Database(self.database_url, pool_size=4, max_overflow=8)

    async def asyncTearDown(self):
        await self.database.close()
        self.database_context.__exit__(None, None, None)

    async def test_profession_business_and_non_job_results_persist_with_provenance(self):
        seeded = await SourceSeedImporter(self.database).import_file(SOURCES_PATH)
        self.assertEqual((seeded.created, seeded.total), (15, 15))
        before = await self._seed_snapshot()

        backend = RecordingSearchBackend(
            {
                "Telegram engineers": (
                    WebSearchResult(
                        "https://t.me/telegram_engineering_club/42",
                        "Telegram Engineering Club - Telegram",
                        "Our customer workflow needs a technical owner.",
                    ),
                ),
                "SaaS founders": (
                    WebSearchResult(
                        "https://telegram.me/saas_founders_moscow",
                        "SaaS Founders Moscow",
                        "Founders compare go-to-market experiments.",
                    ),
                ),
                "Figma makers": (
                    WebSearchResult(
                        "https://telegram.dog/s/figma_makers_ru/5",
                        "Figma Makers RU",
                        "Design systems and product critique.",
                    ),
                ),
            }
        )
        strategy = WebDiscoveryStrategy(
            topics=(
                WebDiscoveryTopic("profession", "Telegram engineers", "en"),
                WebDiscoveryTopic("founder", "SaaS founders", "en"),
                WebDiscoveryTopic("business", "Business operators", "en"),
                WebDiscoveryTopic("creator", "Video creators", "en"),
                WebDiscoveryTopic("tool", "Figma makers", "en"),
                WebDiscoveryTopic("industry", "Ecommerce operators", "en"),
            ),
            buyer_intent_seeds=(
                BuyerIntentSeed("looking for a specialist", "en"),
                BuyerIntentSeed("need a contractor", "en"),
            ),
        )
        execution = await DiscoveryRunner(
            self.database,
            clock=lambda: NOW,
        ).run(
            WebDiscoveryProvider(backend, strategy=strategy),
            run_key="representative-profession-business-queries-v1",
            request=DiscoveryRequest(
                parameters={"location": "Moscow"},
                requested_at=NOW,
            ),
        )

        self.assertEqual(execution.run.provider, "web_search")
        self.assertEqual(execution.run.provider_kind, "web")
        self.assertEqual(execution.run.result_count, 3)
        self.assertEqual(len(execution.results), 3)
        self.assertTrue(any("SaaS Founders" in row.display_name for row in execution.results))
        self.assertTrue(any("Figma Makers" in row.display_name for row in execution.results))
        self.assertTrue(
            all("freelance" not in row.display_name.lower() for row in execution.results)
        )

        repository = SourceRepository()
        async with self.database.connect() as connection:
            discovered = [
                await repository.get(connection, result.source_id)
                for result in execution.results
            ]
            lineages = [
                (await repository.list_lineage(connection, source.id))[0]
                for source in discovered
            ]
        self.assertTrue(
            all(source.lifecycle_status is SourceStatus.CANDIDATE for source in discovered)
        )
        self.assertTrue(
            all(lineage.discovery_run_id == execution.run.id for lineage in lineages)
        )
        self.assertTrue(all(lineage.provider == "web_search" for lineage in lineages))
        self.assertTrue(
            all(lineage.context["discovery_method"] == "web_search" for lineage in lineages)
        )
        categories = {
            match["community_category"]
            for lineage in lineages
            for match in lineage.context["matches"]
        }
        self.assertTrue({"profession", "founder", "tool"}.issubset(categories))

        self.assertEqual(await self._seed_snapshot(), before)
        repeated = await SourceSeedImporter(self.database).import_file(SOURCES_PATH)
        self.assertEqual(
            (repeated.created, repeated.updated, repeated.unchanged),
            (0, 0, 15),
        )
        dependency_text = (
            (ROOT / "pyproject.toml").read_text()
            + (ROOT / "uv.lock").read_text()
        ).lower()
        self.assertNotIn("tgstat", dependency_text)
        self.assertNotIn("telemetr", dependency_text)

    async def test_web_backoff_survives_governor_restart_without_probe(self):
        class FailingBackend:
            health_identity = "durable_test_backend"

            async def search(self, query, *, language, limit):
                raise WebSearchBackendError("blocked", failure_class="captcha")

        class ProbeBackend:
            health_identity = "durable_test_backend"

            def __init__(self):
                self.calls = 0

            async def search(self, query, *, language, limit):
                self.calls += 1
                return ()

        strategy = WebDiscoveryStrategy(
            topics=(WebDiscoveryTopic("profession", "Python", "en"),),
            buyer_intent_seeds=(BuyerIntentSeed("need a contractor", "en"),),
        )
        governor = WebDiscoveryGovernor(
            min_delay_seconds=0,
            max_delay_seconds=0,
            base_backoff_seconds=300,
            max_backoff_seconds=300,
            clock=lambda: NOW,
            database=self.database,
        )
        first = WebDiscoveryProvider(
            FailingBackend(),
            strategy=strategy,
            governor=governor,
        )
        await first.discover(DiscoveryRequest(parameters={}, requested_at=NOW))

        probe = ProbeBackend()
        restarted = WebDiscoveryGovernor(
            min_delay_seconds=0,
            max_delay_seconds=0,
            base_backoff_seconds=300,
            max_backoff_seconds=300,
            clock=lambda: NOW,
            database=self.database,
        )
        second = WebDiscoveryProvider(
            probe,
            strategy=strategy,
            governor=restarted,
        )
        await second.discover(DiscoveryRequest(parameters={}, requested_at=NOW))

        self.assertEqual(probe.calls, 0)
        self.assertEqual(
            second.observability["query_attempts"][0]["failure_class"],
            "provider_backoff",
        )
        self.assertEqual(restarted.health_for("durable_test_backend").state, WebProviderState.BACKOFF)

    async def _seed_snapshot(self):
        async with self.database.connect() as connection:
            result = await connection.execute(
                sa.select(
                    sources.c.id,
                    sources.c.platform,
                    sources.c.external_id,
                    sources.c.lifecycle_status,
                    sources.c.display_name,
                    sources.c.handle,
                    sources.c.canonical_url,
                )
                .where(sources.c.id <= 15)
                .order_by(sources.c.id)
            )
            rows = result.all()
        return tuple(
            (
                row.id,
                row.platform,
                row.external_id,
                row.lifecycle_status,
                row.display_name,
                row.handle,
                row.canonical_url,
            )
            for row in rows
            if row.id <= 15
        )


class BraveSearchBackendTest(unittest.IsolatedAsyncioTestCase):
    @staticmethod
    def _response(payload):
        response = MagicMock()
        response.read.return_value = json.dumps(payload).encode("utf-8")
        context = MagicMock()
        context.__enter__.return_value = response
        return context

    async def test_uses_official_json_shape_and_bounds_results(self):
        backend = BraveSearchBackend("brave-secret", timeout_seconds=7)
        payload = {
            "web": {
                "results": [
                    {
                        "url": "https://t.me/founders",
                        "title": "Founders",
                        "description": "A bounded result",
                    },
                    {"url": "https://example.test/ignored"},
                ]
            }
        }
        with patch("urllib.request.urlopen", return_value=self._response(payload)) as urlopen:
            results = await backend.search("startup founders", language="ru", limit=1)
        self.assertEqual(results[0].snippet, "A bounded result")
        request = urlopen.call_args.args[0]
        parameters = urllib.parse.parse_qs(request.full_url.split("?", 1)[1])
        self.assertEqual(parameters["count"], ["1"])
        self.assertEqual(parameters["search_lang"], ["ru"])
        self.assertEqual(request.get_header("X-subscription-token"), "brave-secret")
        self.assertEqual(urlopen.call_args.kwargs["timeout"], 7)

    async def test_classifies_provider_http_failures_and_retry_after(self):
        cases = ((401, "http_401", None), (403, "http_403", None), (422, "http_422", None), (429, "http_429", "9"), (503, "http_5xx", None))
        for status, failure_class, retry_after in cases:
            with self.subTest(status=status):
                headers = {} if retry_after is None else {"Retry-After": retry_after}
                error = urllib.error.HTTPError(
                    "https://api.search.brave.com/res/v1/web/search",
                    status,
                    "failure",
                    headers,
                    None,
                )
                backend = BraveSearchBackend("brave-secret")
                with patch("urllib.request.urlopen", side_effect=error):
                    with self.assertRaises(WebSearchBackendError) as raised:
                        await backend.search("query", language="en", limit=20)
                self.assertEqual(raised.exception.failure_class, failure_class)
                self.assertEqual(raised.exception.retry_after_seconds, None if retry_after is None else 9)

    async def test_classifies_malformed_and_empty_results_without_private_payloads(self):
        backend = BraveSearchBackend("brave-secret")
        malformed = MagicMock()
        malformed.read.return_value = b"not-json"
        malformed_context = MagicMock()
        malformed_context.__enter__.return_value = malformed
        with patch("urllib.request.urlopen", return_value=malformed_context):
            with self.assertRaises(WebSearchBackendError) as raised:
                await backend.search("query", language="en", limit=5)
        self.assertEqual(raised.exception.failure_class, "malformed_json")

        with patch(
            "urllib.request.urlopen",
            return_value=self._response({"web": {"results": []}}),
        ):
            self.assertEqual(await backend.search("query", language="en", limit=5), ())
        self.assertEqual(backend.health_observability["empty_results"], 1)

    def test_brave_is_primary_and_searxng_is_independent_fallback(self):
        config = SimpleNamespace(
            brave_search_api_key=SecretStr("brave-secret"),
            brave_search_timeout_seconds=12,
            primary_web_search_url=None,
            primary_web_search_api_key=None,
            searxng_url="https://search.example.test",
        )
        backends = build_web_search_backends(config)
        self.assertEqual(
            [backend.health_identity for backend in backends],
            ["brave", "searxng"],
        )
        self.assertTrue(web_discovery_readiness(config)["brave_configured"])
        missing = web_discovery_readiness(
            SimpleNamespace(
                brave_search_api_key=None,
                primary_web_search_url=None,
                searxng_url="https://search.example.test",
            )
        )
        self.assertEqual(missing["missing_primary_environment_variable"], "BRAVE_SEARCH_API_KEY")


if __name__ == "__main__":
    unittest.main()
