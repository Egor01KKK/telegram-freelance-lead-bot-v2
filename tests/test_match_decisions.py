from __future__ import annotations

import asyncio
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from hashlib import sha256
import unittest
from unittest import mock
from uuid import uuid4

import sqlalchemy as sa

from freelancer_bot.config import RuntimeConfig
from freelancer_bot.match_decisions import (
    MATCH_DECISION_ALGORITHM_VERSION,
    MATCH_DECISION_SCHEMA_VERSION,
    MatchDecisionCode,
    MatchDecisionPolicy,
    MatchScoringInput,
    decide_and_rank_matches,
    match_decision_policy_from_config,
)
from freelancer_bot.matching_service import CandidateMatchingService
from freelancer_bot.metrics import InMemoryMetrics, MetricNames
from freelancer_bot.opportunity_analysis import (
    OPPORTUNITY_ANALYSIS_SCHEMA_VERSION,
    OpenAIOpportunityAnalyzer,
    RoutedOpportunityAnalyzer,
)
from freelancer_bot.persistence.database import Database
from freelancer_bot.persistence.matches import MatchTraceRepository
from freelancer_bot.persistence.opportunities import (
    CANONICAL_OPPORTUNITY_SCHEMA_VERSION,
)
from freelancer_bot.persistence.schema import (
    ai_call_telemetry,
    match_evaluation_runs,
    match_traces,
    opportunities,
    opportunity_analysis_cache,
    opportunity_analysis_links,
    search_profiles,
    users,
)
from freelancer_bot.profile_confirmation import ProfileConfirmationService
from freelancer_bot.search_profiles import (
    SEARCH_PROFILE_PARSER_VERSION,
    SEARCH_PROFILE_PREFERENCES_SCHEMA_VERSION,
    SEARCH_PROFILE_SCHEMA_VERSION,
)
from freelancer_bot.semantic_matching import (
    DeterministicHashEmbeddingProvider,
    SemanticStatus,
    score_candidates_semantic,
)
from postgres_support import TEST_DATABASE_URL, migrate_to_head, temporary_database
from tests.test_semantic_matching import NOW, _opportunity, _profile


EVALUATED_AT = datetime(2026, 8, 14, 18, 37, tzinfo=timezone.utc)


class MatchDecisionTest(unittest.TestCase):
    def test_newer_otherwise_equal_opportunity_ranks_first_per_profile(self):
        profile = _profile()
        old = _seen_at(_opportunity(), EVALUATED_AT - timedelta(days=2))
        new = _seen_at(_opportunity(), EVALUATED_AT - timedelta(hours=1))

        batch = decide_and_rank_matches(
            (_scoring(old, (profile,)), _scoring(new, (profile,))),
            evaluated_at=EVALUATED_AT,
            policy=_permissive_policy(),
        )
        by_opportunity = {trace.opportunity_id: trace for trace in batch.traces}

        self.assertEqual(by_opportunity[new.id].rank, 1)
        self.assertEqual(by_opportunity[old.id].rank, 2)
        self.assertGreater(
            by_opportunity[new.id].freshness_score,
            by_opportunity[old.id].freshness_score,
        )
        self.assertGreater(
            by_opportunity[new.id].final_rank_score,
            by_opportunity[old.id].final_rank_score,
        )
        self.assertEqual(
            batch.evaluated_at,
            datetime(2026, 8, 14, 18, 0, tzinfo=timezone.utc),
        )

    def test_expired_active_opportunity_is_suppressed_by_policy(self):
        profile = _profile()
        old = _seen_at(_opportunity(), EVALUATED_AT - timedelta(days=8))

        trace = decide_and_rank_matches(
            (_scoring(old, (profile,)),),
            evaluated_at=EVALUATED_AT,
            policy=_permissive_policy(),
        ).traces[0]

        self.assertTrue(trace.hard_filter_eligible)
        self.assertEqual(trace.freshness_score, Decimal("0.0000"))
        self.assertEqual(trace.decision_code, MatchDecisionCode.FRESHNESS_EXPIRED)
        self.assertFalse(trace.eligible)
        self.assertIsNone(trace.rank)

    def test_quality_cannot_compensate_for_low_user_relevance(self):
        profile = _profile(
            roles=("Designer",),
            skills=("Figma",),
            categories=("Telegram",),
            semantic_text="Visual identity mobile interface design",
        )
        policy = replace(
            _permissive_policy(),
            minimum_relevance_score=Decimal("0.9000"),
        )

        trace = decide_and_rank_matches(
            (_scoring(_opportunity(), (profile,)),),
            evaluated_at=EVALUATED_AT,
            policy=policy,
        ).traces[0]

        self.assertGreater(trace.opportunity_quality_score, Decimal("0.7000"))
        self.assertLess(trace.combined_relevance_score, Decimal("0.9000"))
        self.assertEqual(
            trace.decision_code,
            MatchDecisionCode.BELOW_RELEVANCE_THRESHOLD,
        )
        self.assertFalse(trace.eligible)

    def test_hard_rejection_is_persistable_and_never_ranked(self):
        profile = _profile(excluded_categories=("Telegram",))
        scoring = _scoring(_opportunity(), (profile,))

        trace = decide_and_rank_matches(
            (scoring,),
            evaluated_at=EVALUATED_AT,
            policy=_permissive_policy(),
        ).traces[0]

        self.assertEqual(scoring.semantic.scores, ())
        self.assertFalse(trace.hard_filter_eligible)
        self.assertEqual(trace.decision_code, MatchDecisionCode.HARD_REJECTED)
        self.assertIn(
            "excluded_category",
            {reason["code"] for reason in trace.hard_filter_reasons},
        )
        self.assertIsNone(trace.semantic_similarity)
        self.assertIsNone(trace.final_rank_score)

    def test_no_provider_uses_structured_score_and_still_decides(self):
        profile = _profile()
        opportunity = _opportunity()
        semantic = score_candidates_semantic(
            opportunity,
            (profile,),
            provider=None,
        )
        batch = decide_and_rank_matches(
            (MatchScoringInput(opportunity, (profile,), semantic),),
            evaluated_at=EVALUATED_AT,
            policy=_permissive_policy(),
        )
        trace = batch.traces[0]

        self.assertEqual(semantic.status, SemanticStatus.DEGRADED)
        self.assertEqual(trace.semantic_status, "degraded")
        self.assertIsNone(trace.semantic_provider)
        self.assertEqual(trace.decision_code, MatchDecisionCode.ELIGIBLE)

    def test_policy_is_runtime_configurable_and_changes_identity(self):
        default = match_decision_policy_from_config(RuntimeConfig(_env_file=None))
        configured = match_decision_policy_from_config(
            RuntimeConfig(
                _env_file=None,
                MATCHING_DECISION_POLICY_VERSION="matching-experiment.v2",
                MATCHING_MINIMUM_RELEVANCE_SCORE="0.55",
                MATCHING_MINIMUM_RANK_SCORE="0.60",
                MATCHING_FRESHNESS_WEIGHT="0.20",
                MATCHING_MAXIMUM_AGE_SECONDS=172800,
                MATCHING_EVALUATION_BUCKET_SECONDS=3600,
            )
        )
        scoring = (_scoring(_opportunity(), (_profile(),)),)
        default_batch = decide_and_rank_matches(
            scoring,
            evaluated_at=EVALUATED_AT,
            policy=default,
        )
        configured_batch = decide_and_rank_matches(
            scoring,
            evaluated_at=EVALUATED_AT,
            policy=configured,
        )

        self.assertEqual(configured.version, "matching-experiment.v2")
        self.assertEqual(configured.minimum_relevance_score, Decimal("0.55"))
        self.assertEqual(configured.freshness_weight, Decimal("0.20"))
        self.assertNotEqual(
            default_batch.idempotency_key,
            configured_batch.idempotency_key,
        )

    def test_identical_inputs_and_bucket_are_reproducible(self):
        scoring = (_scoring(_opportunity(), (_profile(),)),)

        first = decide_and_rank_matches(
            scoring,
            evaluated_at=EVALUATED_AT,
            policy=_permissive_policy(),
        )
        same_bucket = decide_and_rank_matches(
            scoring,
            evaluated_at=EVALUATED_AT + timedelta(minutes=20),
            policy=_permissive_policy(),
        )

        self.assertEqual(first, same_bucket)
        self.assertEqual(len(first.idempotency_key), 64)
        self.assertEqual(len(first.traces[0].input_sha256), 64)

    def test_exact_ties_use_opportunity_identity_for_each_profile_independently(self):
        first_profile = _profile()
        second_profile = _profile()
        first_opportunity = _seen_at(_opportunity(), NOW)
        second_opportunity = _seen_at(_opportunity(), NOW)
        profiles = (first_profile, second_profile)

        batch = decide_and_rank_matches(
            (
                _scoring(first_opportunity, profiles),
                _scoring(second_opportunity, profiles),
            ),
            evaluated_at=EVALUATED_AT,
            policy=_permissive_policy(),
        )
        expected_first = min(
            (first_opportunity.id, second_opportunity.id),
            key=str,
        )
        by_profile = {
            profile_id: sorted(
                (
                    trace
                    for trace in batch.traces
                    if trace.search_profile_id == profile_id
                ),
                key=lambda trace: trace.rank or 999,
            )
            for profile_id in (first_profile.id, second_profile.id)
        }

        self.assertEqual(by_profile[first_profile.id][0].opportunity_id, expected_first)
        self.assertEqual(by_profile[second_profile.id][0].opportunity_id, expected_first)
        self.assertEqual(
            [trace.rank for trace in by_profile[first_profile.id]],
            [1, 2],
        )
        self.assertEqual(
            [trace.rank for trace in by_profile[second_profile.id]],
            [1, 2],
        )


@unittest.skipUnless(TEST_DATABASE_URL, "TEST_DATABASE_URL is not configured")
class MatchTracePostgresTest(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.database_context = temporary_database()
        self.database_url = self.database_context.__enter__()
        migrate_to_head(self.database_url)
        self.database = Database(self.database_url, pool_size=4, max_overflow=8)

    async def asyncTearDown(self):
        await self.database.close()
        self.database_context.__exit__(None, None, None)

    async def test_concurrent_evaluation_persists_one_complete_auditable_batch(self):
        profile = await self._active_profile()
        rejected_profile = await self._active_profile(
            external_user_id="match-trace-rejected",
            role="Designer",
            skill="Figma",
            category="Design",
        )
        newer = await self._opportunity(EVALUATED_AT - timedelta(hours=1))
        older = await self._opportunity(EVALUATED_AT - timedelta(days=2))
        service = CandidateMatchingService(self.database)
        policy = _permissive_policy()

        first, second = await asyncio.gather(
            service.evaluate_and_persist(
                (older, newer),
                evaluated_at=EVALUATED_AT,
                decision_policy=policy,
            ),
            service.evaluate_and_persist(
                (newer, older),
                evaluated_at=EVALUATED_AT + timedelta(minutes=20),
                decision_policy=policy,
            ),
        )

        self.assertEqual({first.created, second.created}, {True, False})
        self.assertEqual(first.run.id, second.run.id)
        self.assertEqual(first.run.trace_count, 4)
        self.assertEqual(first.run.schema_version, MATCH_DECISION_SCHEMA_VERSION)
        self.assertEqual(first.run.algorithm_version, MATCH_DECISION_ALGORITHM_VERSION)
        self.assertEqual(
            set(first.run.policy_config),
            {"decision", "structured", "semantic"},
        )
        traces = {
            record.trace.opportunity_id: record.trace
            for record in first.traces
            if record.trace.search_profile_id == profile.id
        }
        self.assertEqual(traces[newer].rank, 1)
        self.assertEqual(traces[older].rank, 2)
        for trace in traces.values():
            self.assertEqual(trace.search_profile_id, profile.id)
            self.assertEqual(trace.profile_revision, profile.revision)
            self.assertTrue(trace.structured_components)
            self.assertIsNotNone(trace.combined_relevance_score)
            self.assertIsNotNone(trace.semantic_similarity)
            self.assertEqual(trace.semantic_status, "available")
            self.assertIsNotNone(trace.opportunity_quality_score)
            self.assertIsNotNone(trace.red_flag_penalty)
            self.assertEqual(trace.decision_policy_version, policy.version)
            self.assertEqual(len(trace.input_sha256), 64)

        rejected = tuple(
            record.trace
            for record in first.traces
            if record.trace.search_profile_id == rejected_profile.id
        )
        self.assertEqual(len(rejected), 2)
        for trace in rejected:
            self.assertFalse(trace.hard_filter_eligible)
            self.assertEqual(trace.decision_code, MatchDecisionCode.HARD_REJECTED)
            self.assertIn(
                "narrowing.no_structured_target_overlap",
                {reason["code"] for reason in trace.hard_filter_reasons},
            )
            self.assertIsNone(trace.rank)

        async with self.database.connect() as connection:
            eligible = await MatchTraceRepository().list_eligible_for_profile(
                connection,
                run_id=first.run.id,
                search_profile_id=profile.id,
            )
        self.assertEqual([record.trace.rank for record in eligible], [1, 2])

    async def test_one_analysis_generates_many_idempotent_matches_without_ai_calls(self):
        profile_ids = await self._active_profile_batch(64)
        opportunity_id, analysis_cache_id = await self._analyzed_opportunity(
            EVALUATED_AT - timedelta(hours=1)
        )
        metrics = InMemoryMetrics()
        service = CandidateMatchingService(self.database, metrics=metrics)

        with (
            mock.patch.object(
                OpenAIOpportunityAnalyzer,
                "analyze",
                side_effect=AssertionError("matching invoked the opportunity analyzer"),
            ) as primary_analyzer,
            mock.patch.object(
                RoutedOpportunityAnalyzer,
                "analyze",
                side_effect=AssertionError("matching invoked routed AI analysis"),
            ) as routed_analyzer,
        ):
            first, repeated = await asyncio.gather(
                service.generate_matches(
                    (opportunity_id,),
                    evaluated_at=EVALUATED_AT,
                    decision_policy=_permissive_policy(),
                ),
                service.generate_matches(
                    (opportunity_id,),
                    evaluated_at=EVALUATED_AT,
                    decision_policy=_permissive_policy(),
                ),
            )

        self.assertFalse(primary_analyzer.called)
        self.assertFalse(routed_analyzer.called)
        self.assertEqual(
            {first.persistence.created, repeated.persistence.created},
            {True, False},
        )
        self.assertEqual(first.persistence.run.id, repeated.persistence.run.id)
        for result in (first, repeated):
            self.assertEqual(result.report.opportunity_count, 1)
            self.assertEqual(result.report.active_profile_count, 64)
            self.assertEqual(result.report.candidate_pair_count, 64)
            self.assertEqual(result.report.eligible_match_count, 64)
            self.assertEqual(result.report.hard_rejected_count, 0)
            self.assertEqual(result.report.semantic_available_count, 64)
            self.assertEqual(result.report.user_specific_llm_calls, 0)
            self.assertEqual(result.report.opportunity_analyzer_calls, 0)
            self.assertLess(result.report.elapsed_seconds, 5.0)

        async with self.database.connect() as connection:
            counts = (
                await connection.scalar(
                    sa.select(sa.func.count()).select_from(match_evaluation_runs)
                ),
                await connection.scalar(
                    sa.select(sa.func.count()).select_from(match_traces)
                ),
                await connection.scalar(
                    sa.select(sa.func.count())
                    .select_from(opportunity_analysis_links)
                    .where(
                        opportunity_analysis_links.c.analysis_cache_id
                        == analysis_cache_id,
                        opportunity_analysis_links.c.opportunity_id
                        == opportunity_id,
                    )
                ),
                await connection.scalar(
                    sa.select(sa.func.count()).select_from(ai_call_telemetry)
                ),
            )
            persisted_profiles = set(
                (
                    await connection.execute(
                        sa.select(match_traces.c.search_profile_id)
                    )
                ).scalars()
            )

        self.assertEqual(counts, (1, 64, 1, 0))
        self.assertEqual(persisted_profiles, set(profile_ids))
        self.assertEqual(metrics.counter(MetricNames.MATCHES), 64)
        self.assertEqual(metrics.counter(MetricNames.MATCHING_TRACES_CREATED), 64)
        self.assertEqual(metrics.counter(MetricNames.MATCHING_TRACES_REUSED), 64)
        metric_snapshot = metrics.snapshot()
        self.assertEqual(
            metric_snapshot.gauges[
                (MetricNames.MATCHING_USER_SPECIFIC_LLM_CALLS, ())
            ],
            0,
        )
        self.assertEqual(
            metric_snapshot.gauges[
                (MetricNames.MATCHING_OPPORTUNITY_ANALYZER_CALLS, ())
            ],
            0,
        )

    async def _active_profile(
        self,
        *,
        external_user_id="match-trace-user",
        role="Python developer",
        skill="Python",
        category="Telegram",
    ):
        profiles = ProfileConfirmationService(self.database)
        draft = await profiles.create_manual_draft(
            platform="telegram",
            external_user_id=external_user_id,
            semantic_text=f"{role} | {skill} | {category}",
            roles=(role,),
            skills=(skill,),
            categories=(category,),
        )
        confirmed = await profiles.confirm(
            platform="telegram",
            external_user_id=external_user_id,
            profile_id=draft.profile.id,
            expected_revision=draft.profile.revision,
        )
        activated = await profiles.activate(
            platform="telegram",
            external_user_id=external_user_id,
            profile_id=confirmed.profile.id,
            expected_revision=confirmed.profile.revision,
        )
        return activated.profile.profile

    async def _opportunity(self, last_seen_at: datetime):
        opportunity_id = uuid4()
        async with self.database.transaction() as connection:
            await connection.execute(
                opportunities.insert().values(
                    id=opportunity_id,
                    schema_version=CANONICAL_OPPORTUNITY_SCHEMA_VERSION,
                    canonical_title="Python developer",
                    task_summary="Build and integrate a Telegram automation bot",
                    market_direction="buyer_to_specialist",
                    intent_stage="active",
                    opportunity_type="project",
                    category="Telegram",
                    role_title="Python developer",
                    skills=["Python", "Telegram API"],
                    budget_known=False,
                    budget_explicit=False,
                    work_remote=True,
                    analysis_confidence=Decimal("0.9000"),
                    quality_actionability=Decimal("0.8000"),
                    quality_commercial_plausibility=Decimal("0.8000"),
                    quality_specificity=Decimal("0.8000"),
                    quality_credibility=Decimal("0.8000"),
                    red_flags=[],
                    first_seen_at=last_seen_at,
                    last_seen_at=last_seen_at,
                    lifecycle_status="active",
                    lifecycle_changed_at=last_seen_at,
                )
            )
        return opportunity_id

    async def _active_profile_batch(self, count: int):
        user_ids = tuple(uuid4() for _ in range(count))
        profile_ids = tuple(uuid4() for _ in range(count))
        preferences = {
            "schema_version": SEARCH_PROFILE_PREFERENCES_SCHEMA_VERSION,
            "work_types": ["project"],
            "minimum_budget": None,
            "currency": None,
            "budget_policy": "allow_unknown",
            "languages": None,
            "geographies": None,
            "work_modes": ["remote"],
            "excluded_categories": None,
        }
        async with self.database.transaction() as connection:
            await connection.execute(
                users.insert(),
                tuple(
                    {
                        "id": user_id,
                        "platform": "telegram",
                        "external_user_id": f"batch-match-user-{index}",
                    }
                    for index, user_id in enumerate(user_ids)
                ),
            )
            await connection.execute(
                search_profiles.insert(),
                tuple(
                    {
                        "id": profile_id,
                        "user_id": user_id,
                        "schema_version": SEARCH_PROFILE_SCHEMA_VERSION,
                        "parser_version": SEARCH_PROFILE_PARSER_VERSION,
                        "roles": [_explicit_term("Python developer")],
                        "skills": [
                            _explicit_term("Python"),
                            _explicit_term("Telegram API"),
                        ],
                        "categories": [_explicit_term("Telegram")],
                        "semantic_text_original": (
                            "Python developer for Telegram automation"
                        ),
                        "semantic_text_normalized": (
                            "Python developer for Telegram automation"
                        ),
                        "preferences": preferences,
                        "confirmation_status": "confirmed",
                        "revision": 1,
                        "confirmed_at": EVALUATED_AT,
                        "is_active": True,
                        "is_primary": True,
                        "activated_at": EVALUATED_AT,
                    }
                    for user_id, profile_id in zip(user_ids, profile_ids, strict=True)
                ),
            )
        return profile_ids

    async def _analyzed_opportunity(self, last_seen_at: datetime):
        opportunity_id = uuid4()
        cache_id = uuid4()
        content = "Need a Python developer for Telegram automation"
        content_hash = sha256(content.encode("utf-8")).hexdigest()
        input_hash = sha256(f"analysis:{content}".encode("utf-8")).hexdigest()
        async with self.database.transaction() as connection:
            await connection.execute(
                opportunity_analysis_cache.insert().values(
                    id=cache_id,
                    normalized_content=content,
                    normalized_content_sha256=content_hash,
                    analysis_input_sha256=input_hash,
                    analyzer_version="batch-fixture-analyzer.v1",
                    analysis_schema_version=OPPORTUNITY_ANALYSIS_SCHEMA_VERSION,
                    result={"fixture": "one-global-analysis"},
                )
            )
            await connection.execute(
                opportunities.insert().values(
                    id=opportunity_id,
                    schema_version=CANONICAL_OPPORTUNITY_SCHEMA_VERSION,
                    canonical_title="Python developer",
                    task_summary="Build and integrate Telegram automation",
                    market_direction="buyer_to_specialist",
                    intent_stage="active",
                    opportunity_type="project",
                    category="Telegram",
                    role_title="Python developer",
                    skills=["Python", "Telegram API"],
                    budget_known=False,
                    budget_explicit=False,
                    work_remote=True,
                    analysis_confidence=Decimal("0.9000"),
                    quality_actionability=Decimal("0.8000"),
                    quality_commercial_plausibility=Decimal("0.8000"),
                    quality_specificity=Decimal("0.8000"),
                    quality_credibility=Decimal("0.8000"),
                    red_flags=[],
                    first_seen_at=last_seen_at,
                    last_seen_at=last_seen_at,
                    lifecycle_status="active",
                    lifecycle_changed_at=last_seen_at,
                )
            )
            await connection.execute(
                opportunity_analysis_links.insert().values(
                    analysis_cache_id=cache_id,
                    opportunity_id=opportunity_id,
                    dedup_relation="canonical",
                    dedup_algorithm_version="batch-fixture-dedup.v1",
                    normalized_text_sha256=content_hash,
                    dedup_window_seconds=604800,
                    dedup_evidence={"fixture": "one-global-analysis"},
                )
            )
        return opportunity_id, cache_id


def _scoring(opportunity, profiles, *, provider=...):
    selected_provider = (
        DeterministicHashEmbeddingProvider()
        if provider is ...
        else provider
    )
    return MatchScoringInput(
        opportunity=opportunity,
        profiles=profiles,
        semantic=score_candidates_semantic(
            opportunity,
            profiles,
            provider=selected_provider,
        ),
    )


def _explicit_term(value: str) -> dict[str, str]:
    return {
        "value": value,
        "normalized_value": value.casefold(),
        "origin": "explicit",
        "evidence": value,
    }


def _seen_at(opportunity, value):
    return replace(
        opportunity,
        first_seen_at=value,
        last_seen_at=value,
        lifecycle_changed_at=value,
        created_at=value,
        updated_at=value,
    )


def _permissive_policy():
    return MatchDecisionPolicy(
        minimum_relevance_score=Decimal("0.0000"),
        minimum_rank_score=Decimal("0.0000"),
    )


if __name__ == "__main__":
    unittest.main()
