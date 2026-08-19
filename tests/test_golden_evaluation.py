from __future__ import annotations

import json
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest

from pydantic import ValidationError

from freelancer_bot.golden_evaluation import (
    GOLDEN_ANNOTATION_POLICY_VERSION,
    GoldenDataset,
    GoldenDatasetError,
    create_golden_dataset_template,
    evaluate_golden_dataset,
    load_golden_dataset,
    write_golden_dataset,
)
from freelancer_bot.opportunity_analysis import (
    OPPORTUNITY_ANALYSIS_PROMPT_VERSION,
    OPPORTUNITY_ANALYSIS_SCHEMA_VERSION,
    OPPORTUNITY_ANALYZER_VERSION,
    OpportunityAnalysis,
    OpportunityAnalysisCall,
    OpportunityAnalysisUsage,
    OpportunityBudget,
    OpportunityContact,
    OpportunityQuality,
    OpportunityWork,
)


FIXTURE = Path(__file__).parent / "fixtures" / "golden_evaluation.test.v1.json"


def _analysis_for(label) -> OpportunityAnalysis:
    return OpportunityAnalysis(
        schema_version=OPPORTUNITY_ANALYSIS_SCHEMA_VERSION,
        is_opportunity=label.is_opportunity,
        confidence=0.8,
        market_direction=label.market_direction,
        intent_stage=label.intent_stage,
        opportunity_type=label.opportunity_type,
        category=label.category,
        role_title=label.role_title,
        skills=label.skills,
        task_summary=None,
        budget=OpportunityBudget(
            known=False,
            min=None,
            max=None,
            currency=None,
            period=None,
            explicit=False,
        ),
        work=OpportunityWork(
            remote=None,
            location=None,
            full_time=None,
            part_time=None,
        ),
        language=None,
        contact=OpportunityContact(telegram=None, email=None, url=None),
        quality=OpportunityQuality(
            actionability=0.5,
            commercial_plausibility=0.5,
            specificity=0.5,
            credibility=0.5,
        ),
        red_flags=(),
    )


class ReplayGoldenAnalyzer:
    provider = "fixture_ai"
    model = "fixture-golden-model"
    analyzer_version = OPPORTUNITY_ANALYZER_VERSION
    prompt_version = OPPORTUNITY_ANALYSIS_PROMPT_VERSION
    schema_version = OPPORTUNITY_ANALYSIS_SCHEMA_VERSION

    def __init__(self, dataset: GoldenDataset) -> None:
        self._results = {
            record.message.external_message_id: record.label
            for record in dataset.messages
        }
        self.calls = []

    async def analyze(self, candidate):
        self.calls.append(candidate)
        label = self._results[candidate.current.external_message_id]
        return OpportunityAnalysisCall(
            analysis=_analysis_for(label),
            provider=self.provider,
            requested_model=self.model,
            response_model="fixture-golden-model-2026-08-15",
            analyzer_version=self.analyzer_version,
            prompt_version=self.prompt_version,
            schema_version=self.schema_version,
            attempt_count=1,
            usage=OpportunityAnalysisUsage(
                input_tokens=20,
                output_tokens=30,
                total_tokens=50,
            ),
        )


class GoldenEvaluationTest(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.dataset = load_golden_dataset(FIXTURE, allow_test_fixture=True)

    def test_test_fixture_is_explicit_and_real_loader_rejects_it(self):
        self.assertEqual(self.dataset.dataset_kind, "test_fixture")
        self.assertEqual(self.dataset.collection_status, "ready")
        self.assertEqual(len(self.dataset.messages), 4)
        self.assertEqual(len(self.dataset.relevance_pairs), 2)
        self.assertEqual(self.dataset.duplicate_group_count, 1)
        self.assertFalse(self.dataset.target_reached)
        self.assertEqual(len(self.dataset.fingerprint), 64)

        with self.assertRaises(GoldenDatasetError):
            load_golden_dataset(FIXTURE)

    def test_empty_real_world_template_preserves_collection_status_without_labels(self):
        dataset = create_golden_dataset_template(
            dataset_version="golden-evaluation.2026-08-15.v1"
        )

        self.assertEqual(dataset.dataset_kind, "real_world")
        self.assertEqual(dataset.collection_status, "in_progress")
        self.assertEqual(dataset.annotation_policy_version, GOLDEN_ANNOTATION_POLICY_VERSION)
        self.assertEqual(len(dataset.messages), 0)
        self.assertFalse(dataset.target_reached)

        with TemporaryDirectory() as directory:
            path = Path(directory) / "golden.json"
            write_golden_dataset(path, dataset)
            loaded = load_golden_dataset(path)

        self.assertEqual(loaded, dataset)
        self.assertEqual(loaded.fingerprint, dataset.fingerprint)

    async def test_runner_consumes_labels_and_repeats_reproducibly(self):
        analyzer = ReplayGoldenAnalyzer(self.dataset)

        first = await evaluate_golden_dataset(
            analyzer,
            self.dataset,
            allow_test_fixture=True,
        )
        second = await evaluate_golden_dataset(
            ReplayGoldenAnalyzer(self.dataset),
            self.dataset,
            allow_test_fixture=True,
        )

        self.assertEqual(first, second)
        self.assertEqual(first.dataset_version, self.dataset.dataset_version)
        self.assertEqual(first.dataset_fingerprint, self.dataset.fingerprint)
        self.assertEqual(first.dataset_kind, "test_fixture")
        self.assertEqual(
            (
                first.case_count,
                first.relevance_pair_count,
                first.duplicate_group_count,
                first.true_positive,
                first.false_positive,
                first.true_negative,
                first.false_negative,
            ),
            (4, 2, 1, 2, 0, 2, 0),
        )
        self.assertEqual(first.precision, 1.0)
        self.assertEqual(first.recall, 1.0)
        self.assertEqual(first.direction_accuracy, 1.0)
        self.assertEqual(first.intent_accuracy, 1.0)
        self.assertEqual(first.type_accuracy, 1.0)
        self.assertEqual(first.label_accuracy, 1.0)
        self.assertEqual(first.mismatches, ())
        self.assertEqual(len(first.routes), 1)
        self.assertEqual(first.routes[0].provider, analyzer.provider)
        self.assertEqual(len(analyzer.calls), 4)

    async def test_runner_requires_explicit_test_fixture_opt_in(self):
        with self.assertRaises(GoldenDatasetError):
            await evaluate_golden_dataset(ReplayGoldenAnalyzer(self.dataset), self.dataset)

    def test_real_world_dataset_cannot_contain_test_provenance(self):
        payload = json.loads(FIXTURE.read_text(encoding="utf-8"))
        payload["dataset_kind"] = "real_world"
        payload["collection_status"] = "in_progress"

        with self.assertRaises(ValidationError):
            GoldenDataset.model_validate_json(json.dumps(payload), strict=True)

    def test_real_world_dataset_cannot_claim_ready_before_target_floor(self):
        payload = json.loads(FIXTURE.read_text(encoding="utf-8"))
        payload["dataset_kind"] = "real_world"
        payload["collection_status"] = "ready"
        for message in payload["messages"]:
            message["provenance"] = "captured"

        with self.assertRaises(ValidationError):
            GoldenDataset.model_validate_json(json.dumps(payload), strict=True)


if __name__ == "__main__":
    unittest.main()
