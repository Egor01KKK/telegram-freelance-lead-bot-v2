from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timedelta, timezone
from decimal import Decimal
import json
import unittest
from uuid import uuid4

from freelancer_bot.lead_renderer import (
    TELEGRAM_LEAD_CARD_SCHEMA_VERSION,
    LeadCardRenderError,
    render_telegram_lead_card,
)
from freelancer_bot.match_decisions import (
    MatchDecisionCode,
    MatchDecisionPolicy,
    MatchScoringInput,
    decide_and_rank_matches,
)
from freelancer_bot.opportunity_analysis import OpportunityAnalysis
from freelancer_bot.persistence.matches import MatchTraceRecord
from freelancer_bot.persistence.opportunities import (
    CANONICAL_OPPORTUNITY_SCHEMA_VERSION,
    CanonicalOpportunityRecord,
    OpportunityLifecycleStatus,
    OpportunitySourceObservationRecord,
)
from freelancer_bot.semantic_matching import (
    DeterministicHashEmbeddingProvider,
    score_candidates_semantic,
)
from freelancer_bot.search_profiles import (
    OpportunityType as ProfileOpportunityType,
    WorkMode,
)
from tests.test_semantic_matching import _profile


NOW = datetime(2026, 8, 14, 18, 0, tzinfo=timezone.utc)


class TelegramLeadRendererTest(unittest.TestCase):
    def test_representative_card_matches_minimal_product_hierarchy(self):
        opportunity = _opportunity(
            title="Нужен разработчик Telegram Mini App",
            summary=(
                "Нужно собрать Mini App для сервиса бронирования. "
                "Опыт с платежами будет плюсом."
            ),
            skills=("React", "Telegram Mini Apps API"),
            budget=(100_000, 150_000, "RUB", "project"),
            remote=True,
            last_seen_at=NOW - timedelta(minutes=6),
            with_source=True,
        )
        match = _eligible_match(opportunity, relevance=Decimal("0.9600"))

        card = render_telegram_lead_card(
            opportunity,
            match,
            rendered_at=NOW,
        )

        self.assertEqual(card.schema_version, TELEGRAM_LEAD_CARD_SCHEMA_VERSION)
        self.assertEqual(card.opportunity_id, opportunity.id)
        self.assertEqual(card.match_trace_id, match.id)
        self.assertEqual(card.search_profile_id, match.trace.search_profile_id)
        self.assertEqual(card.profile_revision, match.trace.profile_revision)
        self.assertEqual(card.parse_mode, "html")
        self.assertFalse(card.link_preview)
        self.assertEqual(
            card.body_html,
            "<b>Нужен разработчик Telegram Mini App</b>\n\n"
            "100–150 тыс. ₽ · проект · удалённо\n\n"
            "Нужно собрать Mini App для сервиса бронирования. "
            "Опыт с платежами будет плюсом.\n"
            "React, Telegram Mini Apps API\n\n"
            '<a href="https://t.me/freelance_fixture/42">Источник</a> · '
            "6 минут назад",
        )
        self.assertNotIn("%", card.body_html)
        self.assertNotIn("MATCH", card.body_html)
        self.assertNotIn("0.96", card.body_html)
        self.assertNotIn("confidence", card.body_html)
        self.assertNotIn("schema", card.body_html)

    def test_unknown_fields_are_omitted_without_placeholders(self):
        opportunity = _opportunity(
            title="Нужен специалист по автоматизации",
            summary="Настроить внутренний процесс обработки заявок.",
            skills=(),
            opportunity_type="unknown",
            budget=None,
            remote=None,
            location=None,
            language=None,
            with_source=False,
        )

        card = render_telegram_lead_card(
            opportunity,
            _eligible_match(opportunity),
            rendered_at=NOW,
        )

        self.assertEqual(
            card.body_html,
            "<b>Нужен специалист по автоматизации</b>\n\n"
            "Настроить внутренний процесс обработки заявок.\n\n"
            "только что",
        )
        for placeholder in (
            "бюджет не указан",
            "неизвестно",
            "remote",
            "Источник",
            "unknown",
        ):
            self.assertNotIn(placeholder, card.body_html)
        self.assertIsNone(card.source_url)

    def test_dynamic_content_and_source_url_are_html_escaped(self):
        opportunity = _opportunity(
            title="Backend <Python> & Telegram",
            summary="Интеграция A&B без <script>.",
            skills=("Python > PHP",),
            with_source=True,
            source_url="https://t.me/source/42?ref=a&mode=b",
        )

        card = render_telegram_lead_card(
            opportunity,
            _eligible_match(opportunity),
            rendered_at=NOW,
        )

        self.assertIn("Backend &lt;Python&gt; &amp; Telegram", card.body_html)
        self.assertIn("Интеграция A&amp;B без &lt;script&gt;.", card.body_html)
        self.assertIn("Python &gt; PHP", card.body_html)
        self.assertIn("ref=a&amp;mode=b", card.body_html)
        self.assertNotIn("<script>", card.body_html)

    def test_renderer_rejects_ineligible_or_stale_match_state(self):
        opportunity = _opportunity()
        eligible = _eligible_match(opportunity)
        trace = eligible.trace
        stale_opportunity = replace(
            opportunity,
            lifecycle_status=OpportunityLifecycleStatus.STALE,
        )
        changed_opportunity = replace(
            opportunity,
            last_seen_at=opportunity.last_seen_at + timedelta(minutes=1),
        )

        for decision, hard_filter_eligible in (
            (MatchDecisionCode.HARD_REJECTED, False),
            (MatchDecisionCode.FRESHNESS_EXPIRED, True),
            (MatchDecisionCode.BELOW_RELEVANCE_THRESHOLD, True),
            (MatchDecisionCode.BELOW_RANK_SCORE_THRESHOLD, True),
        ):
            with self.subTest(decision=decision.value):
                rejected = replace(
                    eligible,
                    trace=replace(
                        trace,
                        hard_filter_eligible=hard_filter_eligible,
                        eligible=False,
                        rank=None,
                        decision_code=decision,
                    ),
                )
                with self.assertRaisesRegex(
                    LeadCardRenderError,
                    "eligible persisted match",
                ):
                    render_telegram_lead_card(
                        opportunity,
                        rejected,
                        rendered_at=NOW,
                    )
        with self.assertRaisesRegex(LeadCardRenderError, "inactive Opportunity"):
            render_telegram_lead_card(stale_opportunity, eligible, rendered_at=NOW)
        with self.assertRaisesRegex(LeadCardRenderError, "current Opportunity"):
            render_telegram_lead_card(changed_opportunity, eligible, rendered_at=NOW)

    def test_renderer_rejects_another_opportunity_and_naive_time(self):
        opportunity = _opportunity()
        match = _eligible_match(opportunity)

        with self.assertRaisesRegex(LeadCardRenderError, "another Opportunity"):
            render_telegram_lead_card(
                replace(opportunity, id=uuid4()),
                match,
                rendered_at=NOW,
            )
        with self.assertRaisesRegex(LeadCardRenderError, "include a timezone"):
            render_telegram_lead_card(
                opportunity,
                match,
                rendered_at=NOW.replace(tzinfo=None),
            )

    def test_exact_budget_and_work_metadata_stay_compact(self):
        opportunity = _opportunity(
            title=None,
            role_title=None,
            summary=None,
            skills=(),
            opportunity_type="vacancy",
            budget=(2_500, 2_500, "USD", "month"),
            remote=False,
            location="Berlin",
            language="English",
        )

        card = render_telegram_lead_card(
            opportunity,
            _eligible_match(opportunity),
            rendered_at=NOW + timedelta(hours=2),
        )

        self.assertEqual(
            card.body_html,
            "<b>Новая вакансия</b>\n\n"
            "2 500 $/мес. · вакансия · очно · Berlin · English\n\n"
            "2 часа назад",
        )
        self.assertLess(len(card.body_html), 300)


def _eligible_match(
    opportunity: CanonicalOpportunityRecord,
    *,
    relevance: Decimal | None = None,
) -> MatchTraceRecord:
    profile = _profile(
        skills=("React", "Telegram Mini Apps API"),
        categories=("Telegram",),
        semantic_text="React Telegram Mini App development",
    )
    profile_type = (
        ProfileOpportunityType(opportunity.analysis.opportunity_type.value)
        if opportunity.analysis.opportunity_type.value
        in {item.value for item in ProfileOpportunityType}
        else ProfileOpportunityType.PROJECT
    )
    profile_mode = (
        WorkMode.REMOTE
        if opportunity.analysis.work.remote is not False
        else WorkMode.ON_SITE
    )
    profile = replace(
        profile,
        preferences=replace(
            profile.preferences,
            work_types=(profile_type,),
            work_modes=(profile_mode,),
        ),
    )
    semantic = score_candidates_semantic(
        opportunity,
        (profile,),
        provider=DeterministicHashEmbeddingProvider(),
    )
    batch = decide_and_rank_matches(
        (
            MatchScoringInput(
                opportunity=opportunity,
                profiles=(profile,),
                semantic=semantic,
            ),
        ),
        evaluated_at=NOW,
        policy=MatchDecisionPolicy(
            minimum_relevance_score=Decimal("0.0000"),
            minimum_rank_score=Decimal("0.0000"),
        ),
    )
    trace = batch.traces[0]
    if relevance is not None:
        trace = replace(
            trace,
            combined_relevance_score=relevance,
            user_relevance_score=relevance,
        )
    return MatchTraceRecord(
        id=uuid4(),
        run_id=uuid4(),
        trace=trace,
        created_at=NOW,
    )


def _opportunity(
    *,
    title="Нужен разработчик Telegram Mini App",
    role_title="Telegram Mini App developer",
    summary="Собрать Telegram Mini App.",
    skills=("React", "Telegram Mini Apps API"),
    opportunity_type="project",
    budget=(100_000, 150_000, "RUB", "project"),
    remote=True,
    location=None,
    language=None,
    last_seen_at=NOW,
    with_source=False,
    source_url="https://t.me/freelance_fixture/42",
) -> CanonicalOpportunityRecord:
    budget_payload = (
        {
            "known": False,
            "min": None,
            "max": None,
            "currency": None,
            "period": None,
            "explicit": False,
        }
        if budget is None
        else {
            "known": True,
            "min": budget[0],
            "max": budget[1],
            "currency": budget[2],
            "period": budget[3],
            "explicit": True,
        }
    )
    analysis = OpportunityAnalysis.model_validate_json(
        json.dumps(
            {
                "schema_version": "opportunity_analysis.v1",
                "is_opportunity": True,
                "confidence": 0.96,
                "market_direction": "buyer_to_specialist",
                "intent_stage": "active",
                "opportunity_type": opportunity_type,
                "category": "Telegram",
                "role_title": role_title,
                "skills": skills,
                "task_summary": summary,
                "budget": budget_payload,
                "work": {
                    "remote": remote,
                    "location": location,
                    "full_time": None,
                    "part_time": None,
                },
                "language": language,
                "contact": {"telegram": None, "email": None, "url": None},
                "quality": {
                    "actionability": 0.9,
                    "commercial_plausibility": 0.9,
                    "specificity": 0.9,
                    "credibility": 0.9,
                },
                "red_flags": [],
            }
        ),
        strict=True,
    )
    source = None
    observations = ()
    if with_source:
        source = OpportunitySourceObservationRecord(
            raw_message_id=uuid4(),
            source_id=42,
            platform="telegram",
            external_source_id="username:freelance_fixture",
            source_display_name="Freelance Fixture",
            source_handle="@freelance_fixture",
            source_canonical_url="https://t.me/freelance_fixture",
            message_url=source_url,
            message_date=last_seen_at,
            observed_at=last_seen_at,
            linked_at=last_seen_at,
            is_preferred=True,
        )
        observations = (source,)
    return CanonicalOpportunityRecord(
        id=uuid4(),
        schema_version=CANONICAL_OPPORTUNITY_SCHEMA_VERSION,
        canonical_title=title,
        task_summary=summary,
        analysis=analysis,
        first_seen_at=last_seen_at,
        last_seen_at=last_seen_at,
        lifecycle_status=OpportunityLifecycleStatus.ACTIVE,
        lifecycle_changed_at=last_seen_at,
        raw_message_ids=tuple(
            observation.raw_message_id for observation in observations
        ),
        analysis_cache_ids=(),
        analysis_links=(),
        preferred_source_policy_version=(
            "fixture-preferred-source.v1" if source is not None else None
        ),
        preferred_source=source,
        source_observations=observations,
        lifecycle_events=(),
        created_at=last_seen_at,
        updated_at=last_seen_at,
    )


if __name__ == "__main__":
    unittest.main()
