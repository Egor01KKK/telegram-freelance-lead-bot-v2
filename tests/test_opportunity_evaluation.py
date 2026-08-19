from __future__ import annotations

import json
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest

from pydantic import ValidationError

from freelancer_bot.opportunity_analysis import (
    OPPORTUNITY_ANALYSIS_PROMPT_VERSION,
    OPPORTUNITY_ANALYSIS_SCHEMA_VERSION,
    OPPORTUNITY_ANALYZER_VERSION,
    MarketDirection,
    OpportunityAnalysis,
    OpportunityAnalysisCall,
    OpportunityAnalysisUsage,
    OpportunityType,
)
from freelancer_bot.opportunity_evaluation import (
    OPPORTUNITY_EVALUATION_SCHEMA_VERSION,
    OpportunityEvalDataset,
    OpportunityEvaluationError,
    evaluate_opportunity_analyzer,
    load_opportunity_eval_dataset,
)


FIXTURE = (
    Path(__file__).parent
    / "fixtures"
    / "opportunity_evaluation.v1.json"
)
G10_FIXTURE = (
    Path(__file__).parent
    / "fixtures"
    / "g10_synthetic_opportunity_eval.v1.json"
)

G10_FAMILIES = {
    "software_development": {
        "software_frontend",
        "software_backend",
        "mobile_development",
        "telegram_automation",
        "devops",
        "ai_engineering",
        "business_process_automation",
    },
    "design": {
        "product_design",
        "ux_ui_design",
        "graphic_design",
        "branding_design",
        "3d_design",
        "motion_design",
        "service_design",
    },
    "marketing": {
        "smm",
        "performance_marketing",
        "seo",
        "content_marketing",
        "crm_marketing",
        "growth_marketing",
        "marketing_analytics",
    },
    "creator_content": {
        "video_editing",
        "creator_motion",
        "ugc_creation",
        "scriptwriting",
        "thumbnail_design",
        "content_production",
        "podcast_production",
    },
    "operations_business": {
        "sales",
        "project_management",
        "operations",
        "executive_assistance",
        "recruiting",
        "business_analytics",
        "procurement",
    },
    "extensible_roles": {
        "data_science",
        "user_research",
        "education",
        "translation",
        "legal_services",
        "finance",
        "architecture",
    },
}
G10_SCENARIOS = {
    "active_order",
    "vacancy",
    "project",
    "recommendation",
    "research",
    "seller_promotion",
    "ad",
    "course",
    "discussion",
    "spam_scam",
    "ambiguous",
    "duplicate",
}
G10_FINGERPRINT = "b4a8085993b0b6564196f3fb84c9500edc62a3a157b8da4fc4df5817e647ca2a"


class ReplayOpportunityAnalyzer:
    provider = "fixture_ai"
    model = "fixture-mass-model"
    analyzer_version = OPPORTUNITY_ANALYZER_VERSION
    prompt_version = OPPORTUNITY_ANALYSIS_PROMPT_VERSION
    schema_version = OPPORTUNITY_ANALYSIS_SCHEMA_VERSION

    def __init__(
        self,
        dataset: OpportunityEvalDataset,
        *,
        overrides: dict[int, OpportunityAnalysis] | None = None,
    ) -> None:
        self._results = {
            case.current.external_message_id: case.expected for case in dataset.cases
        }
        self._results.update(overrides or {})
        self.calls = []

    async def analyze(self, candidate):
        self.calls.append(candidate)
        analysis = self._results[candidate.current.external_message_id]
        return OpportunityAnalysisCall(
            analysis=analysis,
            provider=self.provider,
            requested_model=self.model,
            response_model="fixture-mass-model-2026-08-09",
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


class OpportunityEvaluationTest(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.dataset = load_opportunity_eval_dataset(FIXTURE)

    def test_versioned_synthetic_dataset_covers_initial_g4_matrix(self):
        self.assertEqual(
            self.dataset.schema_version,
            OPPORTUNITY_EVALUATION_SCHEMA_VERSION,
        )
        self.assertEqual(len(self.dataset.cases), 8)
        expected = [case.expected for case in self.dataset.cases]
        self.assertEqual(
            {item.market_direction for item in expected},
            set(MarketDirection),
        )
        self.assertTrue(
            {
                OpportunityType.ONE_OFF_ORDER,
                OpportunityType.PROJECT,
                OpportunityType.VACANCY,
                OpportunityType.PART_TIME_CONTRACTOR,
                OpportunityType.CONSULTATION,
                OpportunityType.UNKNOWN,
            }.issubset({item.opportunity_type for item in expected})
        )
        self.assertEqual(
            sum(case.parent is not None for case in self.dataset.cases),
            1,
        )
        self.assertEqual(len(self.dataset.fingerprint), 64)

    def test_g10_t01_fixture_covers_six_families_and_all_required_scenarios(self):
        dataset = load_opportunity_eval_dataset(G10_FIXTURE)

        self.assertEqual(dataset.dataset_version, "synthetic-g10-t01.2026-08-15.v1")
        self.assertEqual(len(dataset.cases), 72)
        self.assertEqual(dataset.fingerprint, G10_FINGERPRINT)
        self.assertEqual(
            {case.case_id.split(".")[1] for case in dataset.cases},
            set(G10_FAMILIES),
        )
        self.assertEqual(
            {case.case_id.split(".")[2] for case in dataset.cases},
            G10_SCENARIOS,
        )
        self.assertEqual(
            sum(case.expected.is_opportunity for case in dataset.cases),
            36,
        )
        self.assertEqual(
            {case.expected.language for case in dataset.cases},
            {"en", "ru"},
        )

        for family, categories in G10_FAMILIES.items():
            family_cases = [
                case
                for case in dataset.cases
                if case.case_id.split(".")[1] == family
            ]
            self.assertEqual(len(family_cases), len(G10_SCENARIOS))
            self.assertEqual(
                {case.case_id.split(".")[2] for case in family_cases},
                G10_SCENARIOS,
            )
            self.assertTrue(
                categories.issubset(
                    {case.expected.category for case in family_cases}
                )
            )

            project = next(
                case
                for case in family_cases
                if case.case_id.split(".")[2] == "project"
            )
            duplicate = next(
                case
                for case in family_cases
                if case.case_id.split(".")[2] == "duplicate"
            )
            self.assertEqual(duplicate.current.content, project.current.content)
            self.assertEqual(duplicate.expected, project.expected)
            self.assertNotEqual(
                duplicate.current.raw_message_id,
                project.current.raw_message_id,
            )

        self.assertEqual(
            sum(case.parent is not None for case in dataset.cases),
            len(G10_FAMILIES),
        )

    async def test_provider_neutral_runner_reports_perfect_replay_reproducibly(self):
        analyzer = ReplayOpportunityAnalyzer(self.dataset)

        report = await evaluate_opportunity_analyzer(analyzer, self.dataset)

        self.assertEqual(report.dataset_version, self.dataset.dataset_version)
        self.assertEqual(report.dataset_fingerprint, self.dataset.fingerprint)
        self.assertEqual(
            (
                report.case_count,
                report.true_positive,
                report.false_positive,
                report.true_negative,
                report.false_negative,
            ),
            (8, 6, 0, 2, 0),
        )
        self.assertEqual(report.precision, 1.0)
        self.assertEqual(report.recall, 1.0)
        self.assertEqual(report.direction_accuracy, 1.0)
        self.assertEqual(report.intent_accuracy, 1.0)
        self.assertEqual(report.type_accuracy, 1.0)
        self.assertEqual(report.structured_field_accuracy, 1.0)
        self.assertEqual(report.exact_case_accuracy, 1.0)
        self.assertEqual(report.mismatches, ())
        self.assertEqual(len(report.routes), 1)
        self.assertEqual(report.routes[0].requested_model, analyzer.model)
        self.assertEqual(len(analyzer.calls), 8)
        self.assertTrue(all(not hasattr(call, "history") for call in analyzer.calls))
        self.assertEqual(sum(call.parent is not None for call in analyzer.calls), 1)

    async def test_runner_exposes_false_positive_and_case_level_regression(self):
        seller = next(
            case for case in self.dataset.cases if case.case_id == "ru.seller.promotion"
        )
        regressed_payload = seller.expected.model_dump(mode="json")
        regressed_payload.update(
            is_opportunity=True,
            confidence=0.91,
            market_direction="buyer_to_specialist",
            intent_stage="active",
            opportunity_type="project",
        )
        regression = OpportunityAnalysis.model_validate_json(
            json.dumps(regressed_payload),
            strict=True,
        )
        analyzer = ReplayOpportunityAnalyzer(
            self.dataset,
            overrides={seller.current.external_message_id: regression},
        )

        report = await evaluate_opportunity_analyzer(analyzer, self.dataset)

        self.assertEqual(report.false_positive, 1)
        self.assertEqual(report.precision, 6 / 7)
        self.assertEqual(report.recall, 1.0)
        self.assertEqual(report.exact_case_accuracy, 7 / 8)
        self.assertEqual(len(report.mismatches), 1)
        mismatch = report.mismatches[0]
        self.assertEqual(mismatch.case_id, seller.case_id)
        self.assertTrue(
            {
                "is_opportunity",
                "market_direction",
                "intent_stage",
                "opportunity_type",
            }.issubset(mismatch.fields)
        )

    async def test_runner_rejects_incompatible_provider_metadata(self):
        analyzer = ReplayOpportunityAnalyzer(self.dataset)
        original_analyze = analyzer.analyze

        async def incompatible(candidate):
            call = await original_analyze(candidate)
            return OpportunityAnalysisCall(
                analysis=call.analysis,
                provider=call.provider,
                requested_model="unconfigured-model",
                response_model=call.response_model,
                analyzer_version=call.analyzer_version,
                prompt_version=call.prompt_version,
                schema_version=call.schema_version,
                attempt_count=call.attempt_count,
                usage=call.usage,
            )

        analyzer.analyze = incompatible
        with self.assertRaises(OpportunityEvaluationError):
            await evaluate_opportunity_analyzer(analyzer, self.dataset)

    def test_loader_rejects_duplicate_identity_and_ungrounded_contacts(self):
        original = json.loads(FIXTURE.read_text(encoding="utf-8"))
        invalid_payloads = []

        duplicate = json.loads(json.dumps(original))
        duplicate["cases"][1]["case_id"] = duplicate["cases"][0]["case_id"]
        invalid_payloads.append(duplicate)

        ungrounded = json.loads(json.dumps(original))
        ungrounded["cases"][0]["expected"]["contact"]["telegram"] = "@invented"
        invalid_payloads.append(ungrounded)

        with TemporaryDirectory() as directory:
            path = Path(directory) / "invalid.json"
            for payload in invalid_payloads:
                with self.subTest(case=payload["cases"][0]["case_id"]):
                    path.write_text(json.dumps(payload), encoding="utf-8")
                    with self.assertRaises(ValidationError):
                        load_opportunity_eval_dataset(path)


if __name__ == "__main__":
    unittest.main()
