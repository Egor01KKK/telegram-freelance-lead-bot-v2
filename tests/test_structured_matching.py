from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timedelta, timezone
from decimal import Decimal
import json
import unittest
from uuid import uuid4

from freelancer_bot.matching import (
    STRUCTURED_SCORING_POLICY_VERSION,
    STRUCTURED_SCORING_VERSION,
    HardFilteredCandidateError,
    score_candidate_structured,
    score_narrowed_candidates,
)
from freelancer_bot.lexical_matching import labels_have_overlap
from freelancer_bot.opportunity_analysis import OpportunityAnalysis
from freelancer_bot.persistence.database import Database
from freelancer_bot.persistence.opportunities import (
    CANONICAL_OPPORTUNITY_SCHEMA_VERSION,
    CanonicalOpportunityRecord,
    OpportunityLifecycleStatus,
)
from freelancer_bot.persistence.search_profiles import (
    SearchProfileConfirmationStatus,
    SearchProfileRecord,
)
from freelancer_bot.persistence.source_metrics import (
    SourceMetricsRepository,
    SourceQualitySnapshot,
)
from freelancer_bot.persistence.source_repository import SourceRepository, SourceStatus
from freelancer_bot.search_profiles import (
    BudgetPolicy,
    OpportunityType,
    WorkMode,
    parse_search_profile,
    parse_search_profile_preferences,
)
from postgres_support import TEST_DATABASE_URL, migrate_to_head, temporary_database


NOW = datetime(2026, 8, 14, 14, 0, tzinfo=timezone.utc)


class StructuredMatchingScoreTest(unittest.TestCase):
    def test_generalized_lexical_equivalents_are_structured_and_explainable(self):
        profile = _profile(
            roles=("python-разработчик",),
            skills=("PostgreSQL",),
            categories=("telegram-боты",),
        )
        opportunity = _opportunity(
            role_title="Backend Developer (Python)",
            skills=("Postgres",),
            category="Telegram Bot Developer",
        )

        score = score_candidate_structured(opportunity, profile)

        self.assertGreater(score.component("role").score, Decimal("0"))
        self.assertGreater(score.component("skills").score, Decimal("0"))
        self.assertGreater(score.component("category").score, Decimal("0"))
        self.assertTrue(score.component("role").evidence)
        self.assertTrue(score.component("skills").evidence)
        self.assertTrue(score.component("category").evidence)
        self.assertTrue(labels_have_overlap(("SMM",), ("social media manager",)))

    def test_unrelated_professions_remain_outside_structured_candidate_set(self):
        profile = _profile(
            roles=("Python developer",),
            skills=("Python",),
            categories=("Telegram bots",),
            work_types=None,
            minimum_budget=None,
            currency=None,
            languages=None,
            geographies=None,
            work_modes=None,
        )
        for role_title, category, skills in (
            ("Graphic Designer", "Graphic Design", ("Figma",)),
            ("Video Editor", "Video Editing", ("Premiere Pro",)),
            ("PR Manager", "Public Relations", ("Media relations",)),
            ("Beta Reader", "Editing", ("Proofreading",)),
            ("TG Ads Specialist", "Telegram advertising", ("TG Ads",)),
        ):
            with self.subTest(role_title=role_title):
                opportunity = _opportunity(
                    role_title=role_title,
                    category=category,
                    skills=skills,
                )
                result = score_narrowed_candidates(opportunity, (profile,))
                self.assertFalse(result.candidates.eligible_profiles)
                self.assertEqual(
                    result.candidates.trace.exclusions[0].code.value,
                    "no_structured_target_overlap",
                )

    def test_aligned_score_is_versioned_inspectable_and_reproducible(self):
        opportunity = _opportunity()
        profile = _profile()

        first = score_candidate_structured(opportunity, profile)
        second = score_candidate_structured(opportunity, profile)

        self.assertEqual(first, second)
        self.assertEqual(first.scoring_version, STRUCTURED_SCORING_VERSION)
        self.assertEqual(first.policy_version, STRUCTURED_SCORING_POLICY_VERSION)
        self.assertEqual(first.user_relevance_score, Decimal("1.0000"))
        self.assertEqual(first.component("skills").score, Decimal("1.0000"))
        self.assertEqual(first.component("budget").score, Decimal("1.0000"))
        self.assertEqual(first.opportunity_quality_score, Decimal("0.8000"))
        self.assertEqual(first.structured_score, Decimal("0.8700"))
        self.assertIsNone(first.source_quality_score)
        self.assertIsNone(first.source_quality_snapshot_id)

    def test_hard_rejection_cannot_be_overridden_by_soft_score(self):
        profile = _profile(excluded_categories=("Telegram",))

        with self.assertRaises(HardFilteredCandidateError) as raised:
            score_candidate_structured(
                _opportunity(quality=1.0),
                profile,
                source_quality=_source_quality(high=True),
            )

        self.assertFalse(raised.exception.decision.eligible)
        self.assertEqual(
            tuple(failure.code.value for failure in raised.exception.decision.failures),
            ("excluded_category",),
        )

    def test_high_intrinsic_quality_can_still_have_low_user_relevance(self):
        opportunity = _opportunity(quality=1.0)
        weak_profile = _profile(
            roles=("Designer",),
            skills=("Figma",),
            categories=("Telegram",),
            work_types=None,
            minimum_budget=None,
            currency=None,
            languages=None,
            geographies=None,
            work_modes=None,
        )
        aligned_profile = _profile()

        weak = score_candidate_structured(
            opportunity,
            weak_profile,
            source_quality=_source_quality(high=True),
        )
        aligned = score_candidate_structured(
            opportunity,
            aligned_profile,
            source_quality=_source_quality(high=False),
        )

        self.assertEqual(weak.opportunity_quality_score, Decimal("1.0000"))
        self.assertEqual(weak.source_quality_score, Decimal("1.0000"))
        self.assertEqual(weak.user_relevance_score, Decimal("0.1500"))
        self.assertGreater(aligned.user_relevance_score, weak.user_relevance_score)
        self.assertGreater(aligned.structured_score, weak.structured_score)

    def test_source_quality_changes_only_its_separate_soft_signal(self):
        opportunity = _opportunity()
        profile = _profile()

        low = score_candidate_structured(
            opportunity,
            profile,
            source_quality=_source_quality(high=False),
        )
        high = score_candidate_structured(
            opportunity,
            profile,
            source_quality=_source_quality(high=True),
        )

        self.assertEqual(low.user_relevance_score, high.user_relevance_score)
        self.assertEqual(
            low.opportunity_quality_score,
            high.opportunity_quality_score,
        )
        self.assertLess(low.source_quality_score, high.source_quality_score)
        self.assertLess(low.structured_score, high.structured_score)
        self.assertEqual(high.source_quality_snapshot_id, 2)

    def test_opportunity_red_flags_apply_independent_bounded_penalty(self):
        clean = score_candidate_structured(
            _opportunity(red_flags=()),
            _profile(),
            source_quality=_source_quality(high=True),
        )
        flagged = score_candidate_structured(
            _opportunity(red_flags=("payment risk", "identity mismatch")),
            _profile(),
            source_quality=_source_quality(high=True),
        )

        self.assertEqual(clean.user_relevance_score, flagged.user_relevance_score)
        self.assertEqual(clean.source_quality_score, flagged.source_quality_score)
        self.assertEqual(flagged.red_flag_penalty, Decimal("0.1600"))
        self.assertEqual(
            clean.structured_score - flagged.structured_score,
            Decimal("0.1600"),
        )

    def test_unknown_structured_fields_are_absent_not_fabricated(self):
        opportunity = _opportunity(
            role_title=None,
            skills=(),
            category=None,
            opportunity_type="unknown",
            budget=_budget(known=False, explicit=False),
            language=None,
            location=None,
            remote=None,
        )
        profile = _profile(
            work_types=None,
            minimum_budget=None,
            currency=None,
            budget_policy=BudgetPolicy.ALLOW_UNKNOWN,
            languages=None,
            geographies=None,
            work_modes=None,
        )

        score = score_candidate_structured(opportunity, profile)

        self.assertEqual(score.user_relevance_score, Decimal("0.0000"))
        for name in (
            "role",
            "skills",
            "category",
            "work_type",
            "budget",
            "preferences",
        ):
            self.assertIsNone(score.component(name).score)
            self.assertEqual(score.component(name).evidence, ())

    def test_multiple_active_profiles_remain_independent_scoring_inputs(self):
        aligned = _profile()
        category_only = _profile(
            roles=("Designer",),
            skills=("Figma",),
            categories=("Telegram",),
        )

        result = score_narrowed_candidates(
            _opportunity(),
            (aligned, category_only),
        )

        self.assertEqual(len(result.scores), 2)
        self.assertEqual(
            {score.profile_id for score in result.scores},
            {aligned.id, category_only.id},
        )
        self.assertNotEqual(
            result.scores[0].user_relevance_score,
            result.scores[1].user_relevance_score,
        )


@unittest.skipUnless(TEST_DATABASE_URL, "TEST_DATABASE_URL is not configured")
class SourceQualitySelectionPostgresTest(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.database_context = temporary_database()
        self.database_url = self.database_context.__enter__()
        migrate_to_head(self.database_url)
        self.database = Database(self.database_url)

    async def asyncTearDown(self):
        await self.database.close()
        self.database_context.__exit__(None, None, None)

    async def test_latest_source_quality_snapshot_is_selected_deterministically(self):
        sources = SourceRepository()
        metrics = SourceMetricsRepository()
        async with self.database.transaction() as connection:
            candidate = await sources.create_candidate(
                connection,
                platform="telegram",
                external_id="username:g7_score_source",
                access_type="public",
                display_name="G7 score source",
                handle="@g7_score_source",
                canonical_url="https://t.me/g7_score_source",
                provider="g7_score_fixture",
                lineage_key="g7-score-source",
            )
            source = await sources.transition(
                connection,
                candidate.id,
                SourceStatus.APPROVED,
                reason="G7 score fixture",
            )
            first = await metrics.record_quality_snapshot(
                connection,
                source_id=source.id,
                audit_key="g7-score:first",
                audited_at=NOW,
                window_started_at=NOW - timedelta(days=3),
                window_ended_at=NOW - timedelta(minutes=1),
                sampled_message_count=10,
                opportunity_yield=Decimal("0.3"),
                buyer_intent_ratio=Decimal("0.4"),
                seller_ratio=Decimal("0.2"),
                spam_ratio=Decimal("0.1"),
                duplicate_ratio=Decimal("0.1"),
            )
            second = await metrics.record_quality_snapshot(
                connection,
                source_id=source.id,
                audit_key="g7-score:second",
                audited_at=NOW + timedelta(hours=1),
                window_started_at=NOW - timedelta(days=2),
                window_ended_at=NOW + timedelta(minutes=30),
                sampled_message_count=12,
                opportunity_yield=Decimal("0.8"),
                buyer_intent_ratio=Decimal("0.9"),
                seller_ratio=Decimal("0.0"),
                spam_ratio=Decimal("0.0"),
                duplicate_ratio=Decimal("0.1"),
            )
            latest = await metrics.get_latest_quality_snapshot(connection, source.id)

        self.assertNotEqual(first.id, second.id)
        self.assertEqual(latest, second)


def _profile(
    *,
    roles=("Python developer",),
    skills=("Python",),
    categories=("Telegram",),
    work_types=(OpportunityType.PROJECT,),
    minimum_budget="100000",
    currency="RUB",
    budget_policy=BudgetPolicy.ALLOW_UNKNOWN,
    languages=("English",),
    geographies=("Berlin",),
    work_modes=(WorkMode.REMOTE,),
    excluded_categories=(),
) -> SearchProfileRecord:
    parsed = parse_search_profile(
        roles=roles,
        skills=skills,
        categories=categories,
        semantic_text="Python developer for Telegram projects",
    )
    preferences = parse_search_profile_preferences(
        work_types=work_types,
        minimum_budget=minimum_budget,
        currency=currency,
        budget_policy=budget_policy,
        languages=languages,
        geographies=geographies,
        work_modes=work_modes,
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
    skills=("Python",),
    category="Telegram",
    opportunity_type="project",
    budget=None,
    language="English",
    location="Berlin",
    remote=True,
    quality=0.8,
    red_flags=(),
) -> CanonicalOpportunityRecord:
    analysis = OpportunityAnalysis.model_validate_json(
        json.dumps(
            {
                "schema_version": "opportunity_analysis.v1",
                "is_opportunity": True,
                "confidence": 0.9,
                "market_direction": "buyer_to_specialist",
                "intent_stage": "active",
                "opportunity_type": opportunity_type,
                "category": category,
                "role_title": role_title,
                "skills": skills,
                "task_summary": "Build a Telegram bot",
                "budget": budget
                or _budget(
                    known=True,
                    explicit=True,
                    minimum=120000,
                    maximum=150000,
                    currency="RUB",
                ),
                "work": {
                    "remote": remote,
                    "location": location,
                    "full_time": None,
                    "part_time": None,
                },
                "language": language,
                "contact": {"telegram": None, "email": None, "url": None},
                "quality": {
                    "actionability": quality,
                    "commercial_plausibility": quality,
                    "specificity": quality,
                    "credibility": quality,
                },
                "red_flags": red_flags,
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


def _budget(
    *,
    known,
    explicit,
    minimum=None,
    maximum=None,
    currency=None,
):
    return {
        "known": known,
        "min": minimum,
        "max": maximum,
        "currency": currency,
        "period": None,
        "explicit": explicit,
    }


def _source_quality(*, high: bool) -> SourceQualitySnapshot:
    return SourceQualitySnapshot(
        id=2 if high else 1,
        source_id=1,
        audit_key="high" if high else "low",
        audited_at=NOW,
        window_started_at=NOW - timedelta(days=3),
        window_ended_at=NOW - timedelta(minutes=1),
        sampled_message_count=20,
        opportunity_yield=Decimal("1") if high else Decimal("0.1"),
        buyer_intent_ratio=Decimal("1") if high else Decimal("0.1"),
        seller_ratio=Decimal("0") if high else Decimal("0.8"),
        spam_ratio=Decimal("0") if high else Decimal("0.8"),
        duplicate_ratio=Decimal("0") if high else Decimal("0.8"),
        created_at=NOW,
    )


if __name__ == "__main__":
    unittest.main()
