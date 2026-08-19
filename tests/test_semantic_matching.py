from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
import json
import unittest
from uuid import uuid4

from freelancer_bot.opportunity_analysis import OpportunityAnalysis
from freelancer_bot.persistence.opportunities import (
    CANONICAL_OPPORTUNITY_SCHEMA_VERSION,
    CanonicalOpportunityRecord,
    OpportunityLifecycleStatus,
)
from freelancer_bot.persistence.search_profiles import (
    SearchProfileConfirmationStatus,
    SearchProfileRecord,
)
from freelancer_bot.search_profiles import (
    BudgetPolicy,
    OpportunityType,
    WorkMode,
    parse_search_profile,
    parse_search_profile_preferences,
)
from freelancer_bot.semantic_matching import (
    LOCAL_EMBEDDING_MODEL,
    LOCAL_EMBEDDING_MODEL_VERSION,
    LOCAL_EMBEDDING_PROVIDER,
    SEMANTIC_MATCHING_POLICY_VERSION,
    SEMANTIC_MATCHING_VERSION,
    DeterministicHashEmbeddingProvider,
    SemanticProviderUnavailable,
    SemanticStatus,
    score_candidates_semantic,
)


NOW = datetime(2026, 8, 14, 16, 0, tzinfo=timezone.utc)


class SemanticMatchingTest(unittest.TestCase):
    def test_compound_terms_and_technical_aliases_keep_local_overlap(self):
        provider = DeterministicHashEmbeddingProvider()

        compound = provider.embed("python-разработчик telegram-боты")
        english = provider.embed("Backend Developer (Python) Telegram Bot")
        postgres = provider.embed("PostgreSQL")
        postgres_alias = provider.embed("Postgres")

        self.assertGreater(
            _cosine(compound.vector, english.vector),
            Decimal("0.3000"),
        )
        self.assertGreater(
            _cosine(postgres.vector, postgres_alias.vector),
            Decimal("0.0000"),
        )

    def test_local_embeddings_are_deterministic_versioned_and_cached(self):
        provider = DeterministicHashEmbeddingProvider(cache_size=2)

        first = provider.embed("Python Telegram bot automation")
        second = provider.embed("Python Telegram bot automation")
        cache = provider.cache_info()

        self.assertIs(first, second)
        self.assertEqual(first.provider, LOCAL_EMBEDDING_PROVIDER)
        self.assertEqual(first.model, LOCAL_EMBEDDING_MODEL)
        self.assertEqual(first.model_version, LOCAL_EMBEDDING_MODEL_VERSION)
        self.assertEqual(first.dimensions, 384)
        self.assertEqual(len(first.input_sha256), 64)
        self.assertEqual(cache.entries, 1)
        self.assertEqual(cache.misses, 1)
        self.assertEqual(cache.hits, 1)

    def test_semantic_similarity_combines_after_structured_score(self):
        provider = DeterministicHashEmbeddingProvider()
        opportunity = _opportunity()
        semantically_aligned = _profile(
            roles=("Automation engineer",),
            skills=("Bots",),
            categories=("Telegram",),
            semantic_text="Python Telegram bot automation integrations",
        )
        category_only = _profile(
            roles=("Designer",),
            skills=("Figma",),
            categories=("Telegram",),
            semantic_text="Visual identity and mobile interface design",
        )

        result = score_candidates_semantic(
            opportunity,
            (semantically_aligned, category_only),
            provider=provider,
        )
        by_profile = {score.structured.profile_id: score for score in result.scores}
        aligned = by_profile[semantically_aligned.id]
        weak = by_profile[category_only.id]

        self.assertEqual(result.status, SemanticStatus.AVAILABLE)
        self.assertGreater(aligned.semantic_similarity, weak.semantic_similarity)
        self.assertGreater(
            aligned.combined_relevance_score,
            aligned.structured.user_relevance_score,
        )
        self.assertGreater(aligned.combined_score, weak.combined_score)
        self.assertLess(weak.semantic_similarity, Decimal("0.5000"))
        self.assertEqual(aligned.semantic_matching_version, SEMANTIC_MATCHING_VERSION)
        self.assertEqual(
            aligned.semantic_policy_version,
            SEMANTIC_MATCHING_POLICY_VERSION,
        )

    def test_hard_rejection_prevents_any_embedding_work(self):
        provider = _CountingProvider()
        excluded = _profile(excluded_categories=("Telegram",))

        result = score_candidates_semantic(
            _opportunity(),
            (excluded,),
            provider=provider,
        )

        self.assertEqual(provider.calls, 0)
        self.assertEqual(result.scores, ())
        self.assertEqual(result.status, SemanticStatus.UNAVAILABLE_INPUT)
        self.assertEqual(
            result.degraded_reason,
            "no_hard_filter_eligible_candidates",
        )

    def test_provider_outage_returns_complete_structured_fallback(self):
        profiles = (_profile(), _profile(semantic_text="Python API integrations"))

        result = score_candidates_semantic(
            _opportunity(),
            profiles,
            provider=_UnavailableProvider(),
        )

        self.assertEqual(result.status, SemanticStatus.DEGRADED)
        self.assertEqual(result.degraded_reason, "semantic_provider_unavailable")
        self.assertEqual(len(result.scores), 2)
        for score in result.scores:
            self.assertIsNone(score.semantic_similarity)
            self.assertEqual(
                score.combined_relevance_score,
                score.structured.user_relevance_score,
            )
            self.assertEqual(score.combined_score, score.structured.structured_score)
            self.assertIsNone(score.provider)

    def test_missing_opportunity_semantic_input_does_not_invent_evidence(self):
        provider = _CountingProvider()
        opportunity = _opportunity(
            role_title=None,
            skills=(),
            category=None,
            task_summary=None,
        )

        result = score_candidates_semantic(
            opportunity,
            (_profile(),),
            provider=provider,
        )

        self.assertEqual(provider.calls, 0)
        self.assertEqual(result.status, SemanticStatus.UNAVAILABLE_INPUT)
        self.assertEqual(
            result.degraded_reason,
            "opportunity_semantic_input_missing",
        )
        self.assertIsNone(result.scores[0].semantic_similarity)

    def test_representation_identity_and_hashes_are_auditable(self):
        result = score_candidates_semantic(
            _opportunity(),
            (_profile(),),
            provider=DeterministicHashEmbeddingProvider(),
        )
        score = result.scores[0]

        self.assertEqual(score.provider, LOCAL_EMBEDDING_PROVIDER)
        self.assertEqual(score.model, LOCAL_EMBEDDING_MODEL)
        self.assertEqual(score.model_version, LOCAL_EMBEDDING_MODEL_VERSION)
        self.assertRegex(score.opportunity_representation_sha256 or "", r"^[0-9a-f]{64}$")
        self.assertRegex(score.profile_representation_sha256 or "", r"^[0-9a-f]{64}$")

    def test_repeated_batch_reuses_opportunity_and_profile_representations(self):
        provider = DeterministicHashEmbeddingProvider()
        opportunity = _opportunity()
        profiles = (
            _profile(semantic_text="Python Telegram bots"),
            _profile(semantic_text="Telegram API automation"),
        )

        first = score_candidates_semantic(
            opportunity,
            profiles,
            provider=provider,
        )
        first_cache = provider.cache_info()
        second = score_candidates_semantic(
            opportunity,
            profiles,
            provider=provider,
        )
        second_cache = provider.cache_info()

        self.assertEqual(first, second)
        self.assertEqual(first_cache.misses, 3)
        self.assertEqual(second_cache.misses, 3)
        self.assertEqual(second_cache.hits - first_cache.hits, 3)

    def test_no_provider_is_explicit_structured_only_degraded_mode(self):
        result = score_candidates_semantic(
            _opportunity(),
            (_profile(),),
            provider=None,
        )

        self.assertEqual(result.status, SemanticStatus.DEGRADED)
        self.assertIsNone(result.scores[0].semantic_similarity)
        self.assertEqual(
            result.scores[0].combined_score,
            result.scores[0].structured.structured_score,
        )


class _CountingProvider:
    provider = LOCAL_EMBEDDING_PROVIDER
    model = LOCAL_EMBEDDING_MODEL
    model_version = LOCAL_EMBEDDING_MODEL_VERSION

    def __init__(self) -> None:
        self.calls = 0
        self._delegate = DeterministicHashEmbeddingProvider()

    def embed(self, text):
        self.calls += 1
        return self._delegate.embed(text)


class _UnavailableProvider(_CountingProvider):
    def embed(self, text):
        self.calls += 1
        raise SemanticProviderUnavailable("fixture outage")


def _cosine(left, right):
    from freelancer_bot.semantic_matching import _cosine_similarity

    return _cosine_similarity(left, right)


def _profile(
    *,
    roles=("Python developer",),
    skills=("Python",),
    categories=("Telegram",),
    semantic_text="Python developer building Telegram bots",
    excluded_categories=(),
) -> SearchProfileRecord:
    parsed = parse_search_profile(
        roles=roles,
        skills=skills,
        categories=categories,
        semantic_text=semantic_text,
    )
    preferences = parse_search_profile_preferences(
        work_types=(OpportunityType.PROJECT,),
        minimum_budget="100000",
        currency="RUB",
        budget_policy=BudgetPolicy.ALLOW_UNKNOWN,
        languages=("English",),
        geographies=("Berlin",),
        work_modes=(WorkMode.REMOTE,),
        excluded_categories=excluded_categories,
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
        is_primary=False,
        activated_at=NOW,
        deactivated_at=None,
        created_at=NOW,
        updated_at=NOW,
    )


def _opportunity(
    *,
    role_title="Python developer",
    skills=("Python", "Telegram API"),
    category="Telegram",
    task_summary="Build and integrate a Telegram automation bot",
) -> CanonicalOpportunityRecord:
    analysis = OpportunityAnalysis.model_validate_json(
        json.dumps(
            {
                "schema_version": "opportunity_analysis.v1",
                "is_opportunity": True,
                "confidence": 0.9,
                "market_direction": "buyer_to_specialist",
                "intent_stage": "active",
                "opportunity_type": "project",
                "category": category,
                "role_title": role_title,
                "skills": skills,
                "task_summary": task_summary,
                "budget": {
                    "known": True,
                    "min": 120000,
                    "max": 150000,
                    "currency": "RUB",
                    "period": None,
                    "explicit": True,
                },
                "work": {
                    "remote": True,
                    "location": "Berlin",
                    "full_time": None,
                    "part_time": None,
                },
                "language": "English",
                "contact": {"telegram": None, "email": None, "url": None},
                "quality": {
                    "actionability": 0.8,
                    "commercial_plausibility": 0.8,
                    "specificity": 0.8,
                    "credibility": 0.8,
                },
                "red_flags": (),
            }
        )
    )
    return CanonicalOpportunityRecord(
        id=uuid4(),
        schema_version=CANONICAL_OPPORTUNITY_SCHEMA_VERSION,
        canonical_title=analysis.role_title,
        task_summary=analysis.task_summary,
        analysis=analysis,
        first_seen_at=NOW,
        last_seen_at=NOW,
        lifecycle_status=OpportunityLifecycleStatus.ACTIVE,
        lifecycle_changed_at=NOW,
        raw_message_ids=(),
        analysis_cache_ids=(),
        analysis_links=(),
        preferred_source_policy_version=None,
        preferred_source=None,
        source_observations=(),
        lifecycle_events=(),
        created_at=NOW,
        updated_at=NOW,
    )


if __name__ == "__main__":
    unittest.main()
