from __future__ import annotations

from datetime import datetime, timezone
import json
import unittest
from uuid import uuid4

from freelancer_bot.config import RuntimeConfig
from freelancer_bot.match_decisions import (
    MatchScoringInput,
    decide_and_rank_matches,
    match_decision_policy_from_config,
)
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
    DeterministicHashEmbeddingProvider,
    score_candidates_semantic,
)


NOW = datetime(2026, 8, 16, 12, 0, tzinfo=timezone.utc)


class MatchingControlsTest(unittest.TestCase):
    def test_profile_control_positives_pass_and_negatives_are_hard_rejected(self):
        profile = _profile()
        policy = match_decision_policy_from_config(RuntimeConfig(_env_file=None))
        positives = (
            ("Python developer", ("Python", "Telegram bot"), "Telegram"),
            ("Telegram bot developer", ("Telegram bot", "CRM"), "Telegram"),
            ("Backend developer", ("Python", "automation", "FastAPI"), "backend"),
            ("Python developer", ("Python", "parsers", "automation"), "automation"),
            ("Telegram mini app developer", ("Telegram", "backend"), "Telegram-боты"),
        )
        negatives = (
            ("Graphic designer", ("Figma", "branding"), "design"),
            ("Video editor", ("Premiere", "video"), "video"),
            ("SMM manager", ("social media",), "marketing"),
            ("Copywriter", ("SEO", "articles"), "writing"),
            ("Recruiter", ("HR", "hiring"), "recruiting"),
        )

        for role, skills, category in positives:
            trace = _trace(profile, role, skills, category, policy)
            self.assertTrue(trace.hard_filter_eligible, role)
            self.assertTrue(trace.eligible, role)

        for role, skills, category in negatives:
            trace = _trace(profile, role, skills, category, policy)
            self.assertFalse(trace.hard_filter_eligible, role)
            self.assertFalse(trace.eligible, role)


def _profile() -> SearchProfileRecord:
    parsed = parse_search_profile(
        roles=("Python-разработчик", "Telegram-боты"),
        skills=("Python", "Telethon", "PostgreSQL"),
        categories=("Telegram-боты", "парсеры"),
        semantic_text="Python Telegram bot Telethon PostgreSQL",
    )
    preferences = parse_search_profile_preferences(
        work_types=(OpportunityType.PROJECT,),
        budget_policy=BudgetPolicy.ALLOW_UNKNOWN,
        work_modes=(WorkMode.REMOTE,),
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


def _trace(profile, role, skills, category, policy):
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
                "role_title": role,
                "skills": list(skills),
                "task_summary": f"Need {role}",
                "budget": {"known": False, "min": None, "max": None, "currency": None, "period": None, "explicit": False},
                "work": {"remote": None, "location": None, "full_time": None, "part_time": None},
                "language": None,
                "contact": {"telegram": None, "email": None, "url": None},
                "quality": {"actionability": 0.8, "commercial_plausibility": 0.8, "specificity": 0.8, "credibility": 0.8},
                "red_flags": [],
            },
            ensure_ascii=False,
        )
    )
    opportunity = CanonicalOpportunityRecord(
        id=uuid4(),
        schema_version=CANONICAL_OPPORTUNITY_SCHEMA_VERSION,
        canonical_title=role,
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
    semantic = score_candidates_semantic(
        opportunity,
        (profile,),
        provider=DeterministicHashEmbeddingProvider(),
    )
    return decide_and_rank_matches(
        (MatchScoringInput(opportunity, (profile,), semantic),),
        evaluated_at=NOW,
        policy=policy,
    ).traces[0]


if __name__ == "__main__":
    unittest.main()
