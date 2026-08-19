from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
from decimal import Decimal
import json
import unittest
from unittest.mock import patch
from uuid import UUID

import sqlalchemy as sa

from freelancer_bot.ai_telemetry import (
    AIBudgetExceeded,
    AICallFinish,
    AICallStart,
    AIModelPrice,
    AISpendGuardPolicy,
)
from freelancer_bot.message_prefilter import AnalyzerMessage, MinimalAnalyzerInput
from freelancer_bot.opportunity_analysis import (
    OpenAIOpportunityAnalyzer,
    RoutedOpportunityAnalyzer,
)
from freelancer_bot.persistence.ai_telemetry import PostgreSQLAICallRecorder
from freelancer_bot.persistence.collector_accounts import CollectorAccountRepository
from freelancer_bot.persistence.database import Database
from freelancer_bot.persistence.raw_messages import (
    RawMessageIngestor,
    RawMessageInput,
    RawMessageOrigin,
)
from freelancer_bot.persistence.schema import ai_call_telemetry
from freelancer_bot.persistence.source_repository import SourceRepository, SourceStatus
from postgres_support import TEST_DATABASE_URL, migrate_to_head, temporary_database


NOW = datetime(2026, 8, 9, 22, 0, tzinfo=timezone.utc)


@unittest.skipUnless(TEST_DATABASE_URL, "TEST_DATABASE_URL is not configured")
class AITelemetryIntegrationTest(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.database_context = temporary_database()
        self.database_url = self.database_context.__enter__()
        migrate_to_head(self.database_url)
        self.database = Database(self.database_url)

    async def asyncSetUp(self):
        sources = SourceRepository()
        accounts = CollectorAccountRepository()
        async with self.database.transaction() as connection:
            account = await accounts.ensure(
                connection,
                platform="telegram",
                external_account_id="92001",
                display_name="G4 telemetry collector",
            )
            source = await sources.create_candidate(
                connection,
                platform="telegram",
                external_id="username:g4_telemetry",
                access_type="public",
                display_name="G4 telemetry source",
                handle="@g4_telemetry",
                canonical_url="https://t.me/g4_telemetry",
                provider="g4_telemetry_fixture",
                lineage_key="g4-telemetry:source",
            )
            source = await sources.transition(
                connection,
                source.id,
                SourceStatus.APPROVED,
                reason="G4 telemetry fixture approved",
            )
        ingested = await RawMessageIngestor(self.database).ingest(
            RawMessageInput(
                source_id=source.id,
                collector_account_id=account.id,
                external_message_id=101,
                message_date=NOW,
                observed_at=NOW,
                message_url="https://t.me/g4_telemetry/101",
                content="Нужен разработчик Telegram-бота",
                transport_metadata={},
                ingestion_origin=RawMessageOrigin.LIVE,
                correlation_id=UUID("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"),
            )
        )
        self.candidate = MinimalAnalyzerInput(
            current=AnalyzerMessage(
                raw_message_id=ingested.message.id,
                source_id=source.id,
                external_source_id=source.external_id,
                external_message_id=101,
                message_date=NOW,
                message_url="https://t.me/g4_telemetry/101",
                content="Нужен разработчик Telegram-бота",
            ),
            parent=None,
        )

    async def asyncTearDown(self):
        await self.database.close()
        self.database_context.__exit__(None, None, None)

    async def test_low_confidence_records_primary_fallback_cost_and_daily_report(self):
        recorder = PostgreSQLAICallRecorder(self.database)
        primary = _analyzer(
            recorder,
            model="gpt-5-nano",
            stage="opportunity_analysis.primary",
            reason="primary",
            input_price="0.05",
            output_price="0.40",
        )
        fallback = _analyzer(
            recorder,
            model="gpt-5-mini",
            stage="opportunity_analysis.fallback",
            reason="low_confidence",
            input_price="0.25",
            output_price="2.00",
        )
        responses = iter(
            (
                _response(0.40, "gpt-5-nano-2026"),
                _response(0.92, "gpt-5-mini-2026"),
            )
        )

        with patch(
            "freelancer_bot.opportunity_analysis.urllib.request.urlopen",
            side_effect=lambda *_args, **_kwargs: _Response(next(responses)),
        ):
            result = await RoutedOpportunityAnalyzer(
                primary,
                fallback,
                confidence_threshold=0.65,
            ).analyze(self.candidate)

        self.assertEqual(result.requested_model, "gpt-5-mini")
        self.assertEqual(result.route_reason, "low_confidence_fallback")
        async with self.database.connect() as connection:
            rows = (
                await connection.execute(
                    sa.select(ai_call_telemetry).order_by(
                        ai_call_telemetry.c.stage.desc()
                    )
                )
            ).mappings().all()
        self.assertEqual(len(rows), 2)
        self.assertEqual({row["status"] for row in rows}, {"succeeded"})
        self.assertEqual(
            {row["routing_version"] for row in rows},
            {"opportunity-routing.v1"},
        )
        self.assertEqual({row["total_tokens"] for row in rows}, {48})
        costs = {row["stage"]: row["estimated_cost_usd"] for row in rows}
        self.assertEqual(costs["opportunity_analysis.primary"], Decimal("0.000013250"))
        self.assertEqual(costs["opportunity_analysis.fallback"], Decimal("0.000066250"))

        now = datetime.now(timezone.utc)
        report = await recorder.daily_costs(
            started_at=now - timedelta(days=1),
            ended_at=now + timedelta(days=1),
        )
        self.assertEqual(len(report), 2)
        self.assertEqual(sum(item.call_count for item in report), 2)
        self.assertEqual(sum(item.succeeded_count for item in report), 2)
        self.assertEqual(
            sum((item.estimated_cost_usd for item in report), Decimal("0")),
            Decimal("0.000079500"),
        )

        cost_report = await recorder.cost_report(
            started_at=now - timedelta(days=1),
            ended_at=now + timedelta(days=1),
        )
        self.assertEqual(cost_report.call_count, 2)
        self.assertEqual(cost_report.succeeded_count, 2)
        self.assertEqual(cost_report.failed_count, 0)
        self.assertEqual(cost_report.fallback_count, 1)
        self.assertEqual(cost_report.fallback_rate, Decimal("0.5000"))
        self.assertEqual(cost_report.estimated_cost_usd, Decimal("0.000079500"))
        self.assertEqual(
            [(row.stage, row.call_count, row.fallback_count) for row in cost_report.stages],
            [
                ("opportunity_analysis.fallback", 1, 1),
                ("opportunity_analysis.primary", 1, 0),
            ],
        )

    async def test_daily_and_monthly_spend_guards_are_atomic_and_conservative(self):
        price = AIModelPrice(
            pricing_version="fixture-pricing.v1",
            input_usd_per_million=Decimal("1"),
            output_usd_per_million=Decimal("1"),
        )
        recorder = PostgreSQLAICallRecorder(
            self.database,
            spend_guard=AISpendGuardPolicy(
                daily_limit_usd=Decimal("0.001"),
                monthly_limit_usd=Decimal("0.001"),
                reserve_input_tokens=500,
                reserve_output_tokens=500,
            ),
        )

        def start(attempt: int) -> AICallStart:
            return AICallStart(
                raw_message_id=self.candidate.current.raw_message_id,
                stage="opportunity_analysis.primary",
                provider="fixture-ai",
                requested_model="fixture-model",
                analyzer_version="opportunity-analyzer.v1",
                prompt_version="opportunity-analysis-prompt.v2",
                schema_version="opportunity_analysis.v1",
                routing_version="opportunity-routing.v1",
                route_reason="primary",
                provider_attempt=attempt,
                price=price,
            )

        first, second = await asyncio.gather(
            recorder.begin(start(1)),
            recorder.begin(start(2)),
            return_exceptions=True,
        )
        successful = [value for value in (first, second) if not isinstance(value, Exception)]
        failures = [value for value in (first, second) if isinstance(value, Exception)]
        self.assertEqual(len(successful), 1)
        self.assertEqual(len(failures), 1)
        self.assertIsInstance(failures[0], AIBudgetExceeded)

        await recorder.finish(
            successful[0],
            AICallFinish(
                status="request_failed",
                latency_ms=10,
                error_code="provider_request_failed",
            ),
        )
        report_now = datetime.now(timezone.utc)
        report = await recorder.cost_report(
            started_at=report_now - timedelta(minutes=1),
            ended_at=report_now + timedelta(days=1),
        )
        self.assertEqual(report.call_count, 1)
        self.assertEqual(report.estimated_cost_usd, Decimal("0.001000000"))


def _analyzer(recorder, *, model, stage, reason, input_price, output_price):
    return OpenAIOpportunityAnalyzer(
        api_key="test-secret",
        model=model,
        max_output_attempts=1,
        recorder=recorder,
        stage=stage,
        route_reason=reason,
        price=AIModelPrice(
            pricing_version="openai-gpt5-2025-08-07",
            input_usd_per_million=Decimal(input_price),
            output_usd_per_million=Decimal(output_price),
        ),
    )


class _Response:
    def __init__(self, payload: str) -> None:
        self.payload = payload

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return None

    def read(self) -> bytes:
        return self.payload.encode("utf-8")


def _response(confidence: float, model: str) -> str:
    analysis = {
        "schema_version": "opportunity_analysis.v1",
        "is_opportunity": True,
        "confidence": confidence,
        "market_direction": "buyer_to_specialist",
        "intent_stage": "active",
        "opportunity_type": "project",
        "category": "telegram_automation",
        "role_title": "Telegram bot developer",
        "skills": ["Telegram Bot API"],
        "task_summary": "Разработать Telegram-бота",
        "budget": {
            "known": False,
            "min": None,
            "max": None,
            "currency": None,
            "period": None,
            "explicit": False,
        },
        "work": {
            "remote": None,
            "location": None,
            "full_time": None,
            "part_time": None,
        },
        "language": "ru",
        "contact": {"telegram": None, "email": None, "url": None},
        "quality": {
            "actionability": 0.8,
            "commercial_plausibility": 0.8,
            "specificity": 0.7,
            "credibility": 0.7,
        },
        "red_flags": [],
    }
    return json.dumps(
        {
            "model": model,
            "usage": {
                "prompt_tokens": 17,
                "completion_tokens": 31,
                "total_tokens": 48,
            },
            "choices": [{"message": {"content": json.dumps(analysis)}}],
        }
    )
