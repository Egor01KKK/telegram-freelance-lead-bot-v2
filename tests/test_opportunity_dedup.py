from __future__ import annotations

import json
import unittest

from freelancer_bot.opportunity_analysis import OpportunityAnalysis
from freelancer_bot.opportunity_dedup import (
    StructuredDedupPolicy,
    evaluate_structured_duplicate,
)


class StructuredOpportunityDedupTest(unittest.TestCase):
    def setUp(self):
        self.policy = StructuredDedupPolicy()

    def test_shared_contact_requires_compatible_task(self):
        first = _analysis(
            task_summary=(
                "Build a Telegram booking bot with payments reminders and admin tools"
            ),
            telegram="@buyer",
        )
        duplicate = _analysis(
            task_summary=(
                "Build a Telegram booking bot with payments reminders and admin panel"
            ),
            telegram="https://t.me/buyer",
        )

        decision = evaluate_structured_duplicate(
            first,
            duplicate,
            policy=self.policy,
        )

        self.assertIsNotNone(decision)
        self.assertEqual(
            decision.evidence["decision_rule"],
            "shared_contact_and_task",
        )
        self.assertEqual(decision.evidence["shared_contact_fields"], ["telegram"])
        self.assertNotIn("buyer", json.dumps(decision.evidence))

    def test_same_contact_does_not_merge_different_real_jobs(self):
        first = _analysis(
            task_summary="Build a Telegram booking bot for a medical clinic",
            telegram="@agency",
        )
        second = _analysis(
            task_summary="Design a mobile banking application and research user flows",
            telegram="@agency",
        )

        decision = evaluate_structured_duplicate(first, second, policy=self.policy)

        self.assertIsNone(decision)

    def test_conflicting_contact_or_budget_blocks_merge(self):
        task = "Build a Telegram booking bot with payments reminders and admin tools"
        contact_conflict = evaluate_structured_duplicate(
            _analysis(task_summary=task, telegram="@first"),
            _analysis(task_summary=task, telegram="@second"),
            policy=self.policy,
        )
        budget_conflict = evaluate_structured_duplicate(
            _analysis(task_summary=task, telegram="@buyer", budget=(1000, 1500)),
            _analysis(task_summary=task, telegram="@buyer", budget=(3000, 4000)),
            policy=self.policy,
        )

        self.assertIsNone(contact_conflict)
        self.assertIsNone(budget_conflict)

    def test_rich_analysis_task_can_merge_with_unknown_budget(self):
        task = (
            "Implement Telegram mini app booking flow with payments reminders and "
            "operator dashboard"
        )
        decision = evaluate_structured_duplicate(
            _analysis(task_summary=task, budget=None),
            _analysis(task_summary=task, budget=None),
            policy=self.policy,
        )

        self.assertIsNotNone(decision)
        self.assertEqual(decision.evidence["decision_rule"], "analysis_semantic_task")
        self.assertEqual(decision.evidence["budget_relation"], "both_unknown")

    def test_compatible_budget_supports_a_similar_task(self):
        decision = evaluate_structured_duplicate(
            _analysis(
                task_summary=(
                    "Build Telegram booking bot with payments reminders and admin "
                    "dashboard"
                ),
                budget=(1000, 1800),
            ),
            _analysis(
                task_summary=(
                    "Build Telegram booking bot with payments reminders and operator "
                    "dashboard"
                ),
                budget=(1500, 2200),
            ),
            policy=self.policy,
        )

        self.assertIsNotNone(decision)
        self.assertEqual(
            decision.evidence["decision_rule"],
            "compatible_budget_and_task",
        )
        self.assertEqual(decision.evidence["budget_relation"], "overlap")

    def test_category_and_skills_alone_never_merge(self):
        first = _analysis(
            task_summary="Build Telegram support bot for online retail customer service",
        )
        second = _analysis(
            task_summary="Create banking analytics dashboard for executive finance team",
        )

        decision = evaluate_structured_duplicate(first, second, policy=self.policy)

        self.assertIsNone(decision)


def _analysis(
    *,
    task_summary: str,
    telegram: str | None = None,
    budget: tuple[float, float] | None = (1000, 1500),
) -> OpportunityAnalysis:
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
            "currency": "USD",
            "period": "project",
            "explicit": True,
        }
    )
    return OpportunityAnalysis.model_validate_json(
        json.dumps(
            {
                "schema_version": "opportunity_analysis.v1",
                "is_opportunity": True,
                "confidence": 0.91,
                "market_direction": "buyer_to_specialist",
                "intent_stage": "active",
                "opportunity_type": "project",
                "category": "telegram_automation",
                "role_title": "Telegram developer",
                "skills": ["Python", "Telegram Bot API"],
                "task_summary": task_summary,
                "budget": budget_payload,
                "work": {
                    "remote": True,
                    "location": None,
                    "full_time": None,
                    "part_time": None,
                },
                "language": "en",
                "contact": {
                    "telegram": telegram,
                    "email": None,
                    "url": None,
                },
                "quality": {
                    "actionability": 0.9,
                    "commercial_plausibility": 0.9,
                    "specificity": 0.8,
                    "credibility": 0.8,
                },
                "red_flags": [],
            }
        ),
        strict=True,
    )


if __name__ == "__main__":
    unittest.main()
