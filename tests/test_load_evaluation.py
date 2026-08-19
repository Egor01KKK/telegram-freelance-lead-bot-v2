from __future__ import annotations

import asyncio
from decimal import Decimal
from pathlib import Path
import tempfile
import unittest

from freelancer_bot.load_evaluation import (
    AICallObservation,
    LoadEvaluationError,
    LoadWorkload,
    load_load_evaluation_report,
    run_load_evaluation,
)


class LoadEvaluationTest(unittest.IsolatedAsyncioTestCase):
    async def test_report_measures_all_stages_cost_fallback_and_provenance(self):
        workload = LoadWorkload(
            dataset_version="synthetic-g10-t05.v1",
            message_count=12,
            profile_count=2,
            delivery_count=6,
            daily_message_projection=120,
            monthly_message_projection=3_600,
            stage_concurrency={
                "ingestion": 3,
                "matching": 4,
                "delivery": 2,
            },
            evidence_ref="tests/test_load_evaluation.py",
        )

        async def ingestion(index: int):
            await asyncio.sleep(0)
            return AICallObservation(
                stage="opportunity_analysis.primary",
                route="primary",
                provider="fixture-ai",
                requested_model="fixture-cheap",
                status="succeeded",
                latency_ms=2,
                input_tokens=100,
                output_tokens=20,
                estimated_cost_usd=Decimal("0.000100000"),
                quality_pass=index % 3 != 0,
            ) if index != 0 else (
                AICallObservation(
                    stage="opportunity_analysis.primary",
                    route="primary",
                    provider="fixture-ai",
                    requested_model="fixture-cheap",
                    status="succeeded",
                    latency_ms=2,
                    input_tokens=100,
                    output_tokens=20,
                    estimated_cost_usd=Decimal("0.000100000"),
                    quality_pass=True,
                ),
                AICallObservation(
                    stage="opportunity_analysis.fallback",
                    route="fallback",
                    provider="fixture-ai",
                    requested_model="fixture-strong",
                    status="succeeded",
                    latency_ms=4,
                    input_tokens=120,
                    output_tokens=40,
                    estimated_cost_usd=Decimal("0.000300000"),
                    quality_pass=True,
                ),
            )

        async def matching(_index: int):
            await asyncio.sleep(0)

        async def delivery(_index: int):
            await asyncio.sleep(0)

        report = await run_load_evaluation(
            workload,
            {
                "ingestion": ingestion,
                "matching": matching,
                "delivery": delivery,
            },
            daily_spend_limit_usd=Decimal("0.001"),
            monthly_spend_limit_usd=Decimal("1"),
        )

        self.assertFalse(report.quality_claim_allowed)
        self.assertEqual(report.dataset_kind, "test_fixture")
        self.assertEqual(report.workload_fingerprint, workload.fingerprint)
        self.assertEqual(
            [stage.requested_count for stage in report.stages],
            [12, 24, 6],
        )
        self.assertEqual(
            [(stage.completed_count, stage.failed_count, stage.final_backlog) for stage in report.stages],
            [(12, 0, 0), (24, 0, 0), (6, 0, 0)],
        )
        self.assertEqual(report.model_call_count, 13)
        self.assertEqual(report.ai_cost.succeeded_call_count, 13)
        self.assertEqual(report.ai_cost.fallback.fallback_call_count, 1)
        self.assertEqual(report.ai_cost.fallback.fallback_rate, Decimal("0.0769"))
        self.assertEqual(report.ai_cost.fallback.primary_cost_usd, Decimal("0.001200000"))
        self.assertEqual(report.ai_cost.fallback.fallback_cost_usd, Decimal("0.000300000"))
        self.assertEqual(report.ai_cost.observed_cost_usd, Decimal("0.001500000"))
        self.assertEqual(report.ai_cost.daily_guard_status, "exceeded")
        self.assertEqual(report.ai_cost.monthly_guard_status, "within_limit")
        self.assertEqual(report.user_specific_llm_calls, 0)

        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "load-report.json"
            report.write_json(path)
            self.assertEqual(load_load_evaluation_report(path), report)

    async def test_failure_is_reported_and_queue_is_drained_without_hiding_it(self):
        workload = LoadWorkload(
            dataset_version="synthetic-g10-t05.recovery.v1",
            message_count=4,
            profile_count=1,
            delivery_count=2,
            daily_message_projection=4,
            monthly_message_projection=120,
            evidence_ref="tests/test_load_evaluation.py",
        )

        async def failing_matching(index: int):
            if index == 1:
                raise TimeoutError("fixture provider timeout")
            await asyncio.sleep(0)

        async def no_op(_index: int):
            await asyncio.sleep(0)

        report = await run_load_evaluation(
            workload,
            {
                "ingestion": no_op,
                "matching": failing_matching,
                "delivery": no_op,
            },
        )

        matching = report.stages[1]
        self.assertEqual(matching.requested_count, 4)
        self.assertEqual(matching.completed_count, 3)
        self.assertEqual(matching.failed_count, 1)
        self.assertEqual(matching.final_backlog, 0)
        self.assertEqual(matching.failure_types, ("TimeoutError",))

    def test_workload_rejects_missing_stage_or_quality_claim(self):
        with self.assertRaises(LoadEvaluationError):
            LoadWorkload(
                dataset_version="synthetic",
                message_count=1,
                profile_count=1,
                delivery_count=1,
                daily_message_projection=1,
                monthly_message_projection=1,
                stage_concurrency={"ingestion": 1},
            )
