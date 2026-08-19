from __future__ import annotations

import unittest
import asyncio
from datetime import datetime, timezone
from unittest.mock import patch
import urllib.error

from freelancer_bot.discovery import DiscoveryRequest
from freelancer_bot.global_web_discovery import GlobalWebDiscoveryProvider
from freelancer_bot.global_source_library import (
    BOOTSTRAP_TAXONOMY,
    CampaignType,
    CandidatePriority,
    MonitoringCandidate,
    MonitoringTier,
    bootstrap_campaign_specs,
    campaign_key,
    collapse_campaign_queries,
    decide_backpressure,
    generate_campaign_queries,
    GlobalDiscoveryQuery,
    QueryFamily,
    profile_gap_campaign_spec,
    prioritize_candidate,
    run_offline_scale_test,
    validate_bootstrap_targets,
    WeightedMonitoringScheduler,
)
from freelancer_bot.telegram_references import InvalidTelegramReference, normalize_telegram_reference
from freelancer_bot.web_discovery import WebDiscoveryGovernor, WebSearchResult
from freelancer_bot.web_page_extraction import (
    ExtractedTelegramLink,
    UnsafePageURL,
    validate_public_page_url,
)
from freelancer_bot.source_bootstrap import _coverage_dimensions


class GlobalSourceLibraryTest(unittest.TestCase):
    def test_bootstrap_targets_are_monotonic(self):
        validate_bootstrap_targets(
            target_unique_candidates=1000,
            target_validated_sources=500,
            target_approved_sources=100,
        )
        with self.assertRaisesRegex(ValueError, "target_approved_sources"):
            validate_bootstrap_targets(
                target_unique_candidates=1000,
                target_validated_sources=500,
                target_approved_sources=1000,
            )

    def test_bootstrap_taxonomy_has_fifteen_global_buyer_ecosystems(self):
        self.assertGreaterEqual(len(BOOTSTRAP_TAXONOMY), 15)
        self.assertEqual({"en", "ru"}, set(BOOTSTRAP_TAXONOMY[0].languages))
        self.assertEqual(len(bootstrap_campaign_specs()), 15)

    def test_campaign_and_profile_gap_keys_are_deterministic_and_reusable(self):
        left = profile_gap_campaign_spec(
            profile_id="profile-a",
            buyer_habitats=("SaaS founders", "product teams"),
            industries=("software",),
        )
        right = profile_gap_campaign_spec(
            profile_id="profile-b",
            buyer_habitats=("product teams", "SaaS founders"),
            industries=("software",),
        )
        self.assertEqual(campaign_key(left), campaign_key(right))
        self.assertIs(CampaignType.PROFILE_GAP, left.campaign_type)

    def test_query_families_are_diverse_and_collapse_is_conservative(self):
        queries = generate_campaign_queries(bootstrap_campaign_specs()[0])
        families = {query.family.value for query in queries}
        self.assertTrue({"DIRECT_TELEGRAM_SOURCE", "SITE_TELEGRAM", "BUYER_HABITAT"} <= families)
        self.assertEqual(len(queries), len(collapse_campaign_queries(queries)))

    def test_telegram_normalization_separates_source_and_message(self):
        source = normalize_telegram_reference("telegram.me/Example_Channel")
        message = normalize_telegram_reference("https://t.me/Example_Channel/123")
        self.assertEqual(source.source_key, message.source_key)
        self.assertIsNone(source.message_id)
        self.assertEqual(message.message_id, 123)
        with self.assertRaises(InvalidTelegramReference):
            normalize_telegram_reference("https://t.me/contact")

    def test_telegram_normalization_supports_invites_and_internal_message_links(self):
        invite = normalize_telegram_reference("https://t.me/+SecretInviteHash")
        internal = normalize_telegram_reference("https://t.me/c/123456/77")
        self.assertTrue(invite.is_invite)
        self.assertEqual(invite.source_key, "invite:secretinvitehash")
        self.assertEqual(internal.source_key, "peer:-100123456")
        self.assertEqual(internal.message_id, 77)

    def test_global_web_provider_keeps_numeric_references_and_candidate_bound(self):
        class Backend:
            health_identity = "fixture_web"

            async def search(self, query, *, language, limit):
                return (WebSearchResult("https://example.test/hub", "Hub"),)

        class PageFetcher:
            def extract_telegram_links(self, *, result_url, page_url, max_links):
                return tuple(
                    ExtractedTelegramLink(
                        reference=normalize_telegram_reference(
                            f"https://t.me/c/{123456 + index}/{index + 1}"
                        ),
                        page_url=page_url,
                        result_url=result_url,
                        domain="example.test",
                    )
                    for index in range(min(max_links, 5))
                )

        query = GlobalDiscoveryQuery(
            text="Telegram founders",
            family=QueryFamily.DIRECT_TELEGRAM_SOURCE,
            language="en",
            normalized_query_key="telegram founders",
            strategy_version="test.v1",
            campaign_key="bootstrap:test",
            topic="founders",
        )

        async def scenario():
            provider = GlobalWebDiscoveryProvider(
                (Backend(),),
                governor=WebDiscoveryGovernor(min_delay_seconds=0, max_delay_seconds=0),
                queries=(query,),
                max_candidates=2,
                page_fetcher=PageFetcher(),
            )
            candidates = await provider.discover(
                DiscoveryRequest(
                    parameters={},
                    requested_at=datetime.now(timezone.utc),
                )
            )
            return candidates, provider.observability

        candidates, observability = asyncio.run(scenario())
        self.assertEqual(len(candidates), 2)
        self.assertTrue(all(item.external_id.startswith("peer:-100") for item in candidates))
        self.assertTrue(all(item.context["telegram_reference"].startswith("https://t.me/c/") for item in candidates))
        funnel = observability["candidate_funnel"]
        self.assertEqual(funnel["RAW_TELEGRAM_REFERENCES"], 2)
        self.assertEqual(funnel["LOCAL_STRUCTURALLY_VALID"], 2)
        self.assertEqual(funnel["NORMALIZED_UNIQUE"], 2)
        self.assertTrue(all("reference_sha256" in item for item in funnel["reference_observations"]))

    def test_global_web_provider_skips_unsafe_page_results(self):
        class Backend:
            health_identity = "fixture_web"

            async def search(self, query, *, language, limit):
                return (WebSearchResult("https://example.test/hub", "Hub"),)

        class PageFetcher:
            def extract_telegram_links(self, *, result_url, page_url, max_links):
                raise UnsafePageURL("fixture blocked")

        query = GlobalDiscoveryQuery(
            text="Telegram founders",
            family=QueryFamily.DIRECT_TELEGRAM_SOURCE,
            language="en",
            normalized_query_key="telegram founders",
            strategy_version="test.v1",
            campaign_key="bootstrap:test",
            topic="founders",
        )

        async def scenario():
            provider = GlobalWebDiscoveryProvider(
                (Backend(),),
                governor=WebDiscoveryGovernor(min_delay_seconds=0, max_delay_seconds=0),
                queries=(query,),
                page_fetcher=PageFetcher(),
            )
            candidates = await provider.discover(
                DiscoveryRequest(
                    parameters={},
                    requested_at=datetime.now(timezone.utc),
                )
            )
            return candidates, provider.observability

        candidates, observability = asyncio.run(scenario())
        self.assertEqual(candidates, ())
        self.assertEqual(observability["failure_classes"], {"unsafe_page_url": 1})

    def test_ssrf_blocks_local_and_non_http_targets(self):
        with self.assertRaises(UnsafePageURL):
            validate_public_page_url("http://127.0.0.1/private")
        with self.assertRaises(UnsafePageURL):
            validate_public_page_url("file:///etc/passwd")

    def test_ssrf_blocks_private_redirect_before_following_it(self):
        from freelancer_bot.web_page_extraction import SafeWebPageFetcher

        fetcher = SafeWebPageFetcher(min_domain_delay_seconds=0)
        redirect = urllib.error.HTTPError(
            "https://public.example/hub",
            302,
            "redirect",
            {"Location": "http://127.0.0.1/internal"},
            None,
        )
        with patch(
            "freelancer_bot.web_page_extraction.socket.getaddrinfo",
            return_value=[(None, None, None, None, ("93.184.216.34", 0))],
        ), patch.object(fetcher._opener, "open", side_effect=redirect) as opener:
            with self.assertRaises(UnsafePageURL):
                fetcher.fetch("https://public.example/hub")
        self.assertEqual(opener.call_count, 1)

    def test_priority_and_backpressure_do_not_confuse_validation_with_approval(self):
        self.assertEqual(
            prioritize_candidate(
                {
                    "normalized_reference": True,
                    "direct_telegram_result": True,
                    "independent_domains": 2,
                }
            ),
            CandidatePriority.HIGH,
        )
        self.assertEqual(
            prioritize_candidate({"direct_telegram_result": True}, previously_rejected=True),
            CandidatePriority.INSUFFICIENT,
        )
        self.assertEqual(decide_backpressure(queued_analysis_jobs=1000, threshold=500).state, "PAUSE_COLD_CATCH_UP")

    def test_scheduler_reserves_exploration_share(self):
        scheduler = WeightedMonitoringScheduler(exploration_share=0.25)
        selected = scheduler.choose(
            tuple(
                MonitoringCandidate(index, MonitoringTier.D if index == 0 else MonitoringTier.A, 0)
                for index in range(8)
            ),
            now=1,
            limit=4,
        )
        self.assertIn(0, {item.source_id for item in selected})

    def test_offline_scale_harness_is_explicitly_synthetic(self):
        result = run_offline_scale_test()
        self.assertTrue(result["synthetic_only"])
        self.assertEqual(result["references"], 10_000)
        self.assertEqual(result["unique_normalized_candidates"], 5_000)

    def test_library_stats_coverage_is_distinct_source_observability(self):
        coverage = _coverage_dimensions(
            [
                {"id": 1, "platform": "telegram", "access_type": "public"},
                {"id": 2, "platform": "telegram", "access_type": "public"},
            ],
            [
                {
                    "source_id": 1,
                    "languages": ["en", "ru"],
                    "buyer_habitats": ["founders"],
                    "industry_contexts": ["software"],
                },
                {
                    "source_id": 2,
                    "languages": ["en"],
                    "buyer_habitats": ["founders"],
                    "industry_contexts": ["software"],
                },
            ],
            [{"source_id": 1, "dimension": "language", "key": "en"}],
            [],
        )
        self.assertEqual(coverage["buyer_habitat"]["founders"], 2)
        self.assertEqual(coverage["language"]["en"], 2)
        self.assertEqual(coverage["source_type"]["telegram:public"], 2)
        self.assertEqual(coverage["quality_tier"]["unmeasured"], 2)
