from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
import json
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest

from freelancer_bot.ai_telemetry import AIModelPrice
from freelancer_bot.evaluation_runner import (
    EvaluationCostLatency,
    EvaluationGateStatus,
    MetricObservation,
    EvaluationThresholds,
    EvaluationVersionIdentity,
    DuplicateDeliveryCase,
    FeedbackEvaluationCase,
    MatchEvaluationCase,
    PrefilterCase,
    RelevanceCase,
    build_automated_evaluation_report,
    compare_evaluation_versions,
    evaluation_gate_summary,
    load_automated_evaluation_report,
    main,
    measure_duplicate_delivery,
    measure_prefilter,
    measure_relevance,
    measure_feedback_cases,
    measure_match_cases,
    run_opportunity_evaluation,
)
from freelancer_bot.golden_evaluation import create_golden_dataset_template
from freelancer_bot.opportunity_analysis import (
    OPPORTUNITY_ANALYSIS_PROMPT_VERSION,
    OPPORTUNITY_ANALYSIS_SCHEMA_VERSION,
    OPPORTUNITY_ANALYZER_VERSION,
    OpportunityAnalysis,
    OpportunityAnalysisCall,
    OpportunityAnalysisUsage,
)
from freelancer_bot.opportunity_evaluation import load_opportunity_eval_dataset


FIXTURE = (
    Path(__file__).parent
    / "fixtures"
    / "g10_synthetic_opportunity_eval.v1.json"
)


class ReplayAnalyzer:
    provider = "fixture_ai"
    model = "fixture-mass-model"
    analyzer_version = OPPORTUNITY_ANALYZER_VERSION
    prompt_version = OPPORTUNITY_ANALYSIS_PROMPT_VERSION
    schema_version = OPPORTUNITY_ANALYSIS_SCHEMA_VERSION

    def __init__(
        self,
        dataset,
        *,
        false_positive_id: int | None = None,
        false_positive_ids: set[int] | None = None,
    ):
        self._results = {
            case.current.external_message_id: case.expected
            for case in dataset.cases
        }
        self._false_positive_ids = set(false_positive_ids or ())
        if false_positive_id is not None:
            self._false_positive_ids.add(false_positive_id)

    async def analyze(self, candidate):
        analysis = self._results[candidate.current.external_message_id]
        if candidate.current.external_message_id in self._false_positive_ids:
            payload = analysis.model_dump(mode="json")
            payload.update(
                is_opportunity=True,
                market_direction="buyer_to_specialist",
                intent_stage="active",
                opportunity_type="project",
            )
            analysis = OpportunityAnalysis.model_validate_json(
                json.dumps(payload),
                strict=True,
            )
        return OpportunityAnalysisCall(
            analysis=analysis,
            provider=self.provider,
            requested_model=self.model,
            response_model="fixture-mass-model-2026-08-15",
            analyzer_version=self.analyzer_version,
            prompt_version=self.prompt_version,
            schema_version=self.schema_version,
            attempt_count=1,
            usage=OpportunityAnalysisUsage(
                input_tokens=10,
                output_tokens=5,
                total_tokens=15,
            ),
        )


def _version_identity(*, prompt_version=OPPORTUNITY_ANALYSIS_PROMPT_VERSION):
    from freelancer_bot.opportunity_evaluation import OpportunityEvalRoute

    return EvaluationVersionIdentity.from_routes(
        (
            OpportunityEvalRoute(
                provider="fixture_ai",
                requested_model="fixture-mass-model",
                response_model="fixture-mass-model-2026-08-15",
                analyzer_version=OPPORTUNITY_ANALYZER_VERSION,
                prompt_version=prompt_version,
                schema_version=OPPORTUNITY_ANALYSIS_SCHEMA_VERSION,
                routing_version="opportunity-routing.v1",
            ),
        ),
        matching_algorithm_version="matching-decision.v1",
        matching_policy_version="matching-policy.fixture.v1",
        semantic_matching_version="semantic-matching-score.v1",
        semantic_policy_version="semantic-matching-policy.v1",
        pricing_version="fixture-pricing.v1",
    )


class EvaluationRunnerTest(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.dataset = load_opportunity_eval_dataset(FIXTURE)
        self.evaluated_at = datetime(2026, 8, 15, 12, 0, tzinfo=timezone.utc)

    async def test_synthetic_runner_reports_metrics_but_blocks_release_claim(self):
        duplicate_cases = (
            DuplicateDeliveryCase(
                case_id="duplicate.repost",
                profile_id="profile.1",
                scenario="duplicate",
                delivered_opportunity_ids=("canonical.1",),
                predicted_same_opportunity=True,
            ),
            DuplicateDeliveryCase(
                case_id="distinct.jobs",
                profile_id="profile.1",
                scenario="distinct",
                delivered_opportunity_ids=("canonical.2", "canonical.3"),
                predicted_same_opportunity=False,
            ),
        )
        relevance_cases = (
            RelevanceCase("pair.1", "relevant", True),
            RelevanceCase("pair.2", "not_relevant", False),
            RelevanceCase("pair.3", "uncertain", True),
        )
        prefilter_cases = (
            PrefilterCase("candidate.1", True, True),
            PrefilterCase("noise.1", False, False, ("empty_content",)),
        )

        report = await run_opportunity_evaluation(
            ReplayAnalyzer(self.dataset),
            self.dataset,
            run_id="g10-t03.synthetic.v1",
            duplicate_cases=duplicate_cases,
            relevance_cases=relevance_cases,
            prefilter_cases=prefilter_cases,
            version_identity=_version_identity(),
            price=AIModelPrice(
                pricing_version="fixture-pricing.v1",
                input_usd_per_million=Decimal("1"),
                output_usd_per_million=Decimal("2"),
            ),
            evaluated_at=self.evaluated_at,
        )

        self.assertEqual(report.release_status, EvaluationGateStatus.BLOCKED)
        self.assertFalse(report.quality_claim_allowed)
        self.assertEqual(report.dataset_kind, "test_fixture")
        self.assertEqual(report.metric("opportunity_precision").value, 1.0)
        self.assertEqual(report.metric("opportunity_recall").value, 1.0)
        self.assertEqual(report.metric("opportunity_precision").status, "blocked")
        self.assertEqual(report.metric("duplicate_user_delivery_rate").value, 0.0)
        self.assertEqual(report.metric("personal_positive_relevance").value, 1.0)
        self.assertEqual(report.metric("prefilter_candidate_recall").value, 1.0)
        self.assertEqual(report.metric("seller_self_promotion_block_rate").value, 1.0)
        self.assertEqual(report.cost_latency.input_tokens, 720)
        self.assertEqual(report.cost_latency.output_tokens, 360)
        self.assertEqual(report.cost_latency.total_tokens, 1080)
        self.assertEqual(report.cost_latency.estimated_cost_usd, Decimal("0.001440000"))
        self.assertTrue(any("test_fixture" in reason for reason in report.blocked_reasons))
        self.assertIn("Synthetic/test-fixture metrics", report.notes[0])

        with TemporaryDirectory() as directory:
            path = Path(directory) / "report.json"
            report.write_json(path)
            loaded = load_automated_evaluation_report(path)
            self.assertEqual(loaded, report)
            self.assertEqual(evaluation_gate_summary(loaded)["release_status"], "blocked")
            self.assertEqual(main(["--report", str(path)]), 2)

    async def test_threshold_failure_is_not_hidden_by_synthetic_provenance(self):
        false_positive_ids = {
            case.current.external_message_id
            for case in self.dataset.cases
            if not case.expected.is_opportunity
        }
        false_positive_ids = set(sorted(false_positive_ids)[:5])
        report = await run_opportunity_evaluation(
            ReplayAnalyzer(self.dataset, false_positive_ids=false_positive_ids),
            self.dataset,
            run_id="g10-t03.synthetic.regression.v1",
            duplicate_cases=(
                DuplicateDeliveryCase(
                    case_id="duplicate.repost",
                    profile_id="profile.1",
                    scenario="duplicate",
                    delivered_opportunity_ids=("canonical.1",),
                    predicted_same_opportunity=True,
                ),
            ),
            relevance_cases=(RelevanceCase("pair.1", "relevant", True),),
            version_identity=_version_identity(),
            evaluated_at=self.evaluated_at,
        )

        precision = report.metric("opportunity_precision")
        self.assertLess(precision.value, 0.90)
        self.assertEqual(precision.threshold_met, False)
        self.assertEqual(precision.status, "failed")
        self.assertEqual(report.release_status, "failed")

    def test_measurements_keep_denominators_and_rejection_reasons_separate(self):
        duplicate = measure_duplicate_delivery(
            (
                DuplicateDeliveryCase(
                    "duplicate.1",
                    "profile.1",
                    "duplicate",
                    ("a", "b"),
                    True,
                ),
                DuplicateDeliveryCase(
                    "distinct.1",
                    "profile.1",
                    "distinct",
                    ("c",),
                    True,
                ),
            )
        )
        self.assertEqual(duplicate.duplicate_delivery_count, 1)
        self.assertEqual(duplicate.duplicate_delivery_denominator, 2)
        self.assertEqual(duplicate.duplicate_delivery_rate, Decimal("0.5000000"))
        self.assertEqual(duplicate.false_merge_count, 1)
        self.assertEqual(duplicate.false_merge_denominator, 1)

        relevance = measure_relevance(
            (
                RelevanceCase("pair.1", "relevant", True),
                RelevanceCase("pair.2", "not_relevant", True),
                RelevanceCase("pair.3", "relevant", False),
                RelevanceCase("pair.4", "uncertain", True),
            )
        )
        self.assertEqual(relevance.positive_relevance, Decimal("0.5000000"))
        self.assertEqual(relevance.recall, Decimal("0.5000000"))
        self.assertEqual(relevance.uncertain_count, 1)

        prefilter = measure_prefilter(
            (
                PrefilterCase("candidate.1", True, True),
                PrefilterCase("candidate.2", True, False, ("service_event",)),
                PrefilterCase("noise.1", False, False, ("empty_content",)),
            )
        )
        self.assertEqual(prefilter.candidate_recall, Decimal("0.5000000"))
        self.assertEqual(prefilter.rejection_reason_coverage, Decimal("1.0000000"))
        self.assertEqual(prefilter.documented_rejected_count, 2)

        with self.assertRaises(ValueError):
            PrefilterCase("invalid", False, False)

    def test_matching_and_feedback_observations_remain_separate(self):
        match_observations = measure_match_cases(
            (
                MatchEvaluationCase(
                    pair_id="match.1",
                    label="relevant",
                    predicted_relevant=True,
                    hard_filter_eligible=True,
                    hard_filter_respected=True,
                    structured_predicted_relevant=False,
                    semantic_predicted_relevant=True,
                    trace_explainable=True,
                    source_quality_hard_filter_respected=True,
                    opportunity_quality_penalty_present=True,
                    version_metadata_complete=True,
                    feedback_signal_present=True,
                ),
                MatchEvaluationCase(
                    pair_id="match.2",
                    label="not_relevant",
                    predicted_relevant=False,
                    hard_filter_eligible=False,
                    hard_filter_respected=True,
                    structured_predicted_relevant=False,
                    semantic_predicted_relevant=False,
                    trace_explainable=True,
                    source_quality_hard_filter_respected=True,
                    opportunity_quality_penalty_present=False,
                    version_metadata_complete=True,
                ),
            )
        )
        matching = {item.name: item for item in match_observations}
        self.assertEqual(matching["matching_positive_relevance"].value, Decimal("1.0000000"))
        self.assertEqual(matching["matching_structured_recall"].value, Decimal("0.0000000"))
        self.assertEqual(matching["matching_semantic_recall"].value, Decimal("1.0000000"))
        self.assertEqual(matching["matching_semantic_recall_delta_count"].value, Decimal("1.0000000"))
        self.assertEqual(matching["match_trace_explainability_coverage"].value, Decimal("1.0000000"))
        self.assertEqual(matching["feedback_signal_context_coverage"].value, Decimal("0.5000000"))

        feedback_observations = measure_feedback_cases(
            (
                FeedbackEvaluationCase(
                    "feedback.1",
                    "not_suitable",
                    "source-feedback-signal.v1",
                    Decimal("0.8000"),
                    Decimal("0.6000"),
                ),
                FeedbackEvaluationCase(
                    "feedback.2",
                    "got_job",
                    "source-feedback-signal.v1",
                    Decimal("0.5000"),
                    Decimal("0.7000"),
                ),
            )
        )
        feedback = {item.name: item for item in feedback_observations}
        self.assertEqual(
            feedback["feedback_adjustment_direction_accuracy"].value,
            Decimal("1.0000000"),
        )
        self.assertEqual(
            feedback["feedback_signal_version_coverage"].value,
            Decimal("1.0000000"),
        )

    def test_prefilter_threshold_is_explicit_when_provided(self):
        identity = _version_identity()
        report = build_automated_evaluation_report(
            run_id="g10-t03.prefilter.v1",
            dataset_version="synthetic-g10-t01.2026-08-15.v1",
            dataset_fingerprint="a" * 64,
            dataset_kind="test_fixture",
            collection_status="ready",
            target_reached=False,
            version_identity=identity,
            thresholds=EvaluationThresholds(prefilter_recall_min=Decimal("0.90")),
            observations=(
                MetricObservation("opportunity_precision", 1, 1, 1, 0.90, "gte", "precision"),
                MetricObservation("opportunity_recall", 1, 1, 1, 0.85, "gte", "recall"),
                MetricObservation("duplicate_user_delivery_rate", 0, 0, 1, 0.02, "lt", "duplicates"),
                MetricObservation("personal_positive_relevance", 1, 1, 1, 0.75, "gte", "relevance"),
                MetricObservation("prefilter_candidate_recall", 0.5, 1, 2, 0.90, "gte", "prefilter"),
            ),
        )
        self.assertEqual(report.metric("prefilter_candidate_recall").status, "failed")
        self.assertEqual(report.release_status, "failed")

    async def test_version_comparison_requires_same_slice_and_records_rationale(self):
        first = await run_opportunity_evaluation(
            ReplayAnalyzer(self.dataset),
            self.dataset,
            run_id="g10-t03.version.a",
            duplicate_cases=(
                DuplicateDeliveryCase("duplicate.1", "profile.1", "duplicate", ("a",), True),
            ),
            relevance_cases=(RelevanceCase("pair.1", "relevant", True),),
            version_identity=_version_identity(),
            evaluated_at=self.evaluated_at,
        )
        second_identity = _version_identity(prompt_version="opportunity-prompt.v2")
        second = await run_opportunity_evaluation(
            ReplayAnalyzer(self.dataset),
            self.dataset,
            run_id="g10-t03.version.b",
            duplicate_cases=(
                DuplicateDeliveryCase("duplicate.1", "profile.1", "duplicate", ("a",), True),
            ),
            relevance_cases=(RelevanceCase("pair.1", "relevant", True),),
            version_identity=second_identity,
            evaluated_at=self.evaluated_at,
        )

        unresolved = compare_evaluation_versions((first, second))
        self.assertEqual(unresolved.status, "blocked")
        selected = compare_evaluation_versions(
            (first, second),
            selected_run_id=second.run_id,
            selection_rationale="Prompt v2 is the selected candidate; quality and cost are retained for review.",
        )
        self.assertEqual(selected.status, "ready")
        self.assertEqual(selected.selected_run_id, second.run_id)
        self.assertEqual(len(selected.to_dict()["runs"]), 2)

    def test_real_world_in_progress_report_remains_blocked(self):
        template = create_golden_dataset_template(
            dataset_version="golden-evaluation.2026-08-15.v1"
        )
        report = build_automated_evaluation_report(
            run_id="g10-t03.real-world.pending",
            dataset_version=template.dataset_version,
            dataset_fingerprint=template.fingerprint,
            dataset_kind="real_world",
            collection_status=template.collection_status,
            target_reached=template.target_reached,
            version_identity=_version_identity(),
            observations=(
                # The values are gate mechanics only; no real quality claim is
                # made because the collection envelope is explicitly pending.
                # The runner cannot turn this template into a release pass.
                # noqa: E501
                MetricObservation(
                    "opportunity_precision", 1, 1, 1, 0.90, "gte", "precision"
                ),
                MetricObservation(
                    "opportunity_recall", 1, 1, 1, 0.85, "gte", "recall"
                ),
                MetricObservation(
                    "duplicate_user_delivery_rate", 0, 0, 1, 0.02, "lt", "duplicates"
                ),
                MetricObservation(
                    "personal_positive_relevance", 1, 1, 1, 0.75, "gte", "relevance"
                ),
            ),
        )
        self.assertFalse(report.quality_claim_allowed)
        self.assertEqual(report.release_status, "blocked")
        self.assertIn("collection_status", " ".join(report.blocked_reasons))


if __name__ == "__main__":
    unittest.main()
