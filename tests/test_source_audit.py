from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from pydantic import ValidationError
import sqlalchemy as sa

from freelancer_bot.config import RuntimeConfig, RuntimeMode
from freelancer_bot.persistence.database import Database
from freelancer_bot.persistence.schema import (
    source_audits,
    source_collector_access,
    source_lifecycle_events,
    source_quality_snapshots,
    source_taxonomy_assignments,
    source_taxonomy_terms,
)
from freelancer_bot.persistence.source_audits import SourceAuditRepository
from freelancer_bot.persistence.source_repository import SourceRepository, SourceStatus
from freelancer_bot.source_audit import (
    SOURCE_AUDIT_SCHEMA_VERSION,
    OpenAISourceAuditProvider,
    SourceAuditClassification,
    SourceAuditDecision,
    SourceAuditError,
    SourceAuditPipeline,
    SourceAuditProvider,
    source_audit_response_schema,
)
from freelancer_bot.source_audit_sampler import (
    SourceAuditMessage,
    SourceAuditSampler,
    SourceAuditTarget,
)
from postgres_support import TEST_DATABASE_URL, migrate_to_head, temporary_database


NOW = datetime(2026, 8, 9, 19, 0, tzinfo=timezone.utc)


class WindowReader:
    def __init__(self, messages):
        self.messages = tuple(messages)
        self.calls = []

    async def fetch_window(
        self,
        target,
        *,
        window_started_at,
        window_ended_at,
        limit,
    ):
        self.calls.append((window_started_at, window_ended_at, limit))
        return tuple(
            sorted(
                (
                    message
                    for message in self.messages
                    if window_started_at <= message.occurred_at <= window_ended_at
                ),
                key=lambda message: (message.occurred_at, message.message_id),
                reverse=True,
            )[:limit]
        )


class FixedAuditProvider:
    name = "fixture_ai"
    model = "replaceable-test-model"
    analyzer_version = "fixture-v1"

    def __init__(self, classification):
        self.classification = classification
        self.calls = []

    async def classify(self, sample):
        self.calls.append(sample)
        return self.classification


class SourceAuditSchemaAndProviderTest(unittest.IsolatedAsyncioTestCase):
    def test_openai_schema_declares_closed_content_mix_object(self):
        schema = source_audit_response_schema()
        content_mix = schema["properties"]["content_mix"]

        self.assertEqual(len(content_mix["anyOf"]), 2)
        non_empty_mix = content_mix["anyOf"][0]
        empty_mix = content_mix["anyOf"][1]
        self.assertFalse(non_empty_mix["additionalProperties"])
        self.assertFalse(empty_mix["additionalProperties"])
        self.assertEqual(
            set(non_empty_mix["properties"]),
            {
                "buyer_demand",
                "seller_promotion",
                "ads_spam",
                "duplicate",
                "other",
            },
        )
        self.assertEqual(
            set(non_empty_mix["required"]),
            set(non_empty_mix["properties"]),
        )
        self.assertEqual(empty_mix["properties"], {})
        self.assertEqual(empty_mix["required"], [])

    def test_strict_schema_rejects_extra_fields_invalid_counts_and_invalid_mix(self):
        valid = _classification(100)
        self.assertEqual(valid.schema_version, SOURCE_AUDIT_SCHEMA_VERSION)

        with self.assertRaises(ValidationError):
            SourceAuditClassification.model_validate(
                {**valid.model_dump(), "unexpected": True}
            )
        with self.assertRaises(ValidationError):
            SourceAuditClassification.model_validate(
                {**valid.model_dump(), "ads_spam_count": 101}
            )
        with self.assertRaises(ValidationError):
            SourceAuditClassification.model_validate(
                {**valid.model_dump(), "content_mix": {"requests": 0.4}}
            )

    async def test_openai_adapter_uses_configurable_model_and_strict_json_schema(self):
        classification = _classification(1, opportunities=1)
        response = {
            "choices": [
                {"message": {"content": classification.model_dump_json()}}
            ]
        }
        captured = {}

        class Response:
            def __enter__(self):
                return self

            def __exit__(self, *args):
                return None

            def read(self):
                return json.dumps(response).encode()

        def fake_urlopen(request, timeout):
            captured["payload"] = json.loads(request.data)
            captured["timeout"] = timeout
            return Response()

        provider = OpenAISourceAuditProvider(
            api_key="test-secret",
            model="configurable-small-model",
            temperature=0,
            timeout_seconds=12,
        )
        sample = SimpleNamespace(
            source_id=7,
            window_started_at=NOW - timedelta(days=3),
            window_ended_at=NOW,
            messages=(
                SourceAuditMessage(1, NOW - timedelta(hours=1), "Нужен бот"),
            ),
        )
        with patch("freelancer_bot.source_audit.urllib.request.urlopen", fake_urlopen):
            result = await provider.classify(sample)

        self.assertIsInstance(provider, SourceAuditProvider)
        self.assertEqual(result, classification)
        self.assertEqual(captured["payload"]["model"], "configurable-small-model")
        self.assertTrue(
            captured["payload"]["response_format"]["json_schema"]["strict"]
        )
        self.assertEqual(captured["timeout"], 12)

    async def test_gpt5_source_audit_omits_unsupported_temperature(self):
        classification = _classification(1, opportunities=1)
        response = {
            "choices": [
                {"message": {"content": classification.model_dump_json()}}
            ]
        }

        class Response:
            def __enter__(self):
                return self

            def __exit__(self, *args):
                return None

            def read(self):
                return json.dumps(response).encode()

        captured = {}

        def fake_urlopen(request, timeout):
            captured["payload"] = json.loads(request.data)
            return Response()

        provider = OpenAISourceAuditProvider(
            api_key="test-secret",
            model="gpt-5-nano",
            temperature=0,
        )
        sample = SimpleNamespace(
            source_id=7,
            window_started_at=NOW - timedelta(days=3),
            window_ended_at=NOW,
            messages=(SourceAuditMessage(1, NOW - timedelta(hours=1), "Нужен бот"),),
        )
        with patch("freelancer_bot.source_audit.urllib.request.urlopen", fake_urlopen):
            await provider.classify(sample)

        self.assertNotIn("temperature", captured["payload"])

    async def test_openai_adapter_retries_internally_inconsistent_output(self):
        valid = _classification(1, opportunities=1)
        invalid = valid.model_dump(mode="json")
        invalid["primary_language"] = "en"
        responses = iter(
            [
                {"choices": [{"message": {"content": json.dumps(invalid)}}]},
                {"choices": [{"message": {"content": valid.model_dump_json()}}]},
            ]
        )
        calls = []

        class Response:
            def __init__(self, response):
                self.response = response

            def __enter__(self):
                return self

            def __exit__(self, *args):
                return None

            def read(self):
                return json.dumps(self.response).encode()

        def fake_urlopen(request, timeout):
            calls.append(json.loads(request.data))
            return Response(next(responses))

        provider = OpenAISourceAuditProvider(
            api_key="test-secret",
            model="configurable-small-model",
            max_output_attempts=2,
        )
        sample = SimpleNamespace(
            source_id=7,
            window_started_at=NOW - timedelta(days=3),
            window_ended_at=NOW,
            messages=(SourceAuditMessage(1, NOW - timedelta(hours=1), "Нужен бот"),),
        )
        with patch("freelancer_bot.source_audit.urllib.request.urlopen", fake_urlopen):
            result = await provider.classify(sample)

        self.assertEqual(result, valid)
        self.assertEqual(len(calls), 2)
        self.assertEqual(calls[0]["messages"][-1]["role"], "user")
        self.assertEqual(calls[1]["messages"][-1]["role"], "system")

    def test_runtime_configuration_keeps_audit_provider_and_model_replaceable(self):
        with patch.dict(
            "os.environ",
            {
                "SOURCE_AUDIT_PROVIDER": "fixture-ai",
                "SOURCE_AUDIT_MODEL": "cheap-audit-model",
                "SOURCE_AUDIT_TEMPERATURE": "0.2",
                "SOURCE_AUDIT_TIMEOUT_SECONDS": "17",
            },
            clear=True,
        ):
            config = RuntimeConfig.from_env(
                mode=RuntimeMode.CHECK_CONFIG,
                env_file=None,
            )
        self.assertEqual(config.source_audit_provider, "fixture-ai")
        self.assertEqual(config.source_audit_model, "cheap-audit-model")
        self.assertEqual(config.source_audit_temperature, 0.2)
        self.assertEqual(config.source_audit_timeout_seconds, 17)


@unittest.skipUnless(TEST_DATABASE_URL, "TEST_DATABASE_URL is not configured")
class SourceAuditPipelineTest(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.database_context = temporary_database()
        self.database_url = self.database_context.__enter__()
        migrate_to_head(self.database_url)
        self.database = Database(self.database_url, pool_size=4, max_overflow=8)
        self.sources = SourceRepository()
        self.audits = SourceAuditRepository()

    async def asyncTearDown(self):
        await self.database.close()
        self.database_context.__exit__(None, None, None)

    def test_lifecycle_actor_kind_rejects_values_that_would_hit_database_constraint(self):
        with self.assertRaisesRegex(ValueError, "lifecycle_actor_kind must be one of"):
            SourceAuditPipeline(
                self.database,
                SourceAuditSampler(WindowReader(())),
                FixedAuditProvider(_classification(0)),
                lifecycle_actor_kind="autonomous_source_discovery",
            )

    async def test_autonomous_discovery_identity_uses_allowed_system_actor_kind(self):
        source = await self._candidate("runtime-actor")
        pipeline = SourceAuditPipeline(
            self.database,
            SourceAuditSampler(WindowReader(_messages(40))),
            FixedAuditProvider(_classification(40, opportunities=3)),
            lifecycle_actor_kind="system",
            lifecycle_actor_id="autonomous_source_discovery",
        )

        await pipeline.run(
            SourceAuditTarget(source.id, "telegram", "@runtime-actor"),
            audited_at=NOW,
        )

        async with self.database.connect() as connection:
            event = (
                await connection.execute(
                    sa.select(source_lifecycle_events)
                    .where(source_lifecycle_events.c.source_id == source.id)
                    .order_by(source_lifecycle_events.c.id.desc())
                    .limit(1)
                )
            ).mappings().one()
        self.assertEqual(event["actor_kind"], "system")
        self.assertEqual(event["actor_id"], "autonomous_source_discovery")

    async def test_approved_audit_persists_signals_taxonomy_and_linked_lifecycle_once(self):
        source = await self._candidate("approved")
        reader = WindowReader(_messages(100))
        provider = FixedAuditProvider(
            _classification(
                100,
                opportunities=12,
                buyer_intent=15,
                seller=8,
                spam=5,
                duplicates=4,
                categories=(
                    {"key": "telegram_development", "display_name": "Telegram development"},
                    {"key": "new_unknown_vertical", "display_name": "New unknown vertical"},
                ),
            )
        )
        pipeline = SourceAuditPipeline(
            self.database,
            SourceAuditSampler(reader),
            provider,
        )
        target = SourceAuditTarget(source.id, "telegram", "@approved")

        first = await pipeline.run(target, audited_at=NOW)
        repeated = await pipeline.run(target, audited_at=NOW)

        self.assertEqual(first.audit.decision, SourceAuditDecision.APPROVED.value)
        self.assertEqual(
            first.audit.decision_policy["version"],
            "source-audit-thresholds.v1",
        )
        self.assertEqual(
            first.audit.decision_policy["approval_minimum_yield"],
            0.03,
        )
        self.assertEqual(first.source.lifecycle_status, SourceStatus.APPROVED)
        self.assertTrue(first.lifecycle_changed)
        self.assertFalse(repeated.created)
        self.assertEqual(len(provider.calls), 1)
        self.assertTrue(all(call[2] <= 151 for call in reader.calls))

        async with self.database.connect() as connection:
            audit_count = await connection.scalar(
                sa.select(sa.func.count()).select_from(source_audits)
            )
            metrics = (
                await connection.execute(
                    sa.select(source_quality_snapshots).where(
                        source_quality_snapshots.c.source_id == source.id
                    )
                )
            ).mappings().one()
            lifecycle = (
                await connection.execute(
                    sa.select(source_lifecycle_events).where(
                        source_lifecycle_events.c.source_audit_id == first.audit.id
                    )
                )
            ).mappings().one()
            terms = (
                await connection.execute(
                    sa.select(
                        source_taxonomy_terms.c.dimension,
                        source_taxonomy_terms.c.key,
                    )
                    .select_from(
                        source_taxonomy_assignments.join(source_taxonomy_terms)
                    )
                    .where(source_taxonomy_assignments.c.source_id == source.id)
                )
            ).all()

        self.assertEqual(audit_count, 1)
        self.assertEqual(metrics["opportunity_yield"], Decimal("0.1200000"))
        self.assertEqual(metrics["seller_ratio"], Decimal("0.0800000"))
        self.assertEqual(metrics["spam_ratio"], Decimal("0.0500000"))
        self.assertEqual(metrics["duplicate_ratio"], Decimal("0.0400000"))
        self.assertEqual(lifecycle["to_status"], "approved")
        self.assertEqual(
            set(terms),
            {
                ("language", "ru"),
                ("category", "telegram_development"),
                ("category", "new_unknown_vertical"),
            },
        )

    async def test_thresholds_reject_noise_and_route_insufficient_evidence_to_review(self):
        rejected = await self._candidate("rejected")
        rejected_pipeline = SourceAuditPipeline(
            self.database,
            SourceAuditSampler(WindowReader(_messages(100))),
            FixedAuditProvider(
                _classification(100, opportunities=0, seller=80, spam=75)
            ),
        )
        rejected_result = await rejected_pipeline.run(
            SourceAuditTarget(rejected.id, "telegram", "@rejected"),
            audited_at=NOW,
        )

        review = await self._candidate("review")
        review_reader = WindowReader(_messages(10))
        review_pipeline = SourceAuditPipeline(
            self.database,
            SourceAuditSampler(review_reader),
            FixedAuditProvider(_classification(10, opportunities=1)),
        )
        review_result = await review_pipeline.run(
            SourceAuditTarget(review.id, "telegram", "@review"),
            audited_at=NOW,
        )

        self.assertEqual(rejected_result.source.lifecycle_status, SourceStatus.REJECTED)
        self.assertIn("rejected.spam_ratio", rejected_result.audit.reason_codes)
        self.assertIn("rejected.seller_ratio", rejected_result.audit.reason_codes)
        self.assertIn(
            "rejected.no_commercial_opportunities",
            rejected_result.audit.reason_codes,
        )
        self.assertEqual(review_result.source.lifecycle_status, SourceStatus.NEEDS_REVIEW)
        self.assertEqual(
            review_result.audit.reason_codes,
            ("review.insufficient_evidence",),
        )
        self.assertTrue(review_result.audit.expanded)
        self.assertEqual(len(review_reader.calls), 2)

    async def test_private_audit_never_creates_collector_access_and_override_keeps_history(self):
        source = await self._candidate("private", access_type="private")
        pipeline = SourceAuditPipeline(
            self.database,
            SourceAuditSampler(WindowReader(_messages(80))),
            FixedAuditProvider(_classification(80, opportunities=8)),
        )
        result = await pipeline.run(
            SourceAuditTarget(source.id, "telegram", "private-invite-reference"),
            audited_at=NOW,
        )
        async with self.database.transaction() as connection:
            overridden = await self.sources.override(
                connection,
                source.id,
                SourceStatus.PAUSED,
                operator_id="operator-9",
                reason="manual quality review",
            )
        async with self.database.connect() as connection:
            access_count = await connection.scalar(
                sa.select(sa.func.count()).select_from(source_collector_access)
            )
            audits = await self.audits.list_for_source(connection, source.id)
            metrics_count = await connection.scalar(
                sa.select(sa.func.count())
                .select_from(source_quality_snapshots)
                .where(source_quality_snapshots.c.source_id == source.id)
            )
            events = await self.sources.list_lifecycle_events(connection, source.id)

        self.assertEqual(result.source.lifecycle_status, SourceStatus.APPROVED)
        self.assertEqual(overridden.lifecycle_status, SourceStatus.PAUSED)
        self.assertEqual(access_count, 0)
        self.assertEqual(len(audits), 1)
        self.assertEqual(metrics_count, 1)
        self.assertEqual(events[-1].actor_id, "operator-9")
        self.assertTrue(events[-1].is_override)
        self.assertIsNone(events[-1].source_audit_id)

    async def test_provider_count_mismatch_cannot_persist_or_change_lifecycle(self):
        source = await self._candidate("mismatch")
        pipeline = SourceAuditPipeline(
            self.database,
            SourceAuditSampler(WindowReader(_messages(40))),
            FixedAuditProvider(_classification(39, opportunities=3)),
        )
        with self.assertRaisesRegex(SourceAuditError, "does not match"):
            await pipeline.run(
                SourceAuditTarget(source.id, "telegram", "@mismatch"),
                audited_at=NOW,
            )
        async with self.database.connect() as connection:
            current = await self.sources.get(connection, source.id)
            audit_count = await connection.scalar(
                sa.select(sa.func.count()).select_from(source_audits)
            )
        self.assertEqual(current.lifecycle_status, SourceStatus.CANDIDATE)
        self.assertEqual(audit_count, 0)

    async def _candidate(self, suffix, *, access_type="public"):
        async with self.database.transaction() as connection:
            return await self.sources.create_candidate(
                connection,
                platform="telegram",
                external_id=f"audit:{suffix}",
                access_type=access_type,
                display_name=f"Audit source {suffix}",
                provider="web_search",
                lineage_key=f"audit-lineage:{suffix}",
            )


def _messages(count):
    return tuple(
        SourceAuditMessage(
            index,
            NOW - timedelta(minutes=index * 10),
            f"Message {index}",
        )
        for index in range(1, count + 1)
    )


def _classification(
    count,
    *,
    opportunities=5,
    buyer_intent=7,
    seller=2,
    spam=1,
    duplicates=1,
    categories=None,
):
    if count == 0:
        opportunities = buyer_intent = seller = spam = duplicates = 0
    return SourceAuditClassification.model_validate(
        {
            "schema_version": SOURCE_AUDIT_SCHEMA_VERSION,
            "analyzed_message_count": count,
            "commercial_opportunity_count": min(opportunities, count),
            "buyer_intent_count": min(buyer_intent, count),
            "seller_promotion_count": min(seller, count),
            "ads_spam_count": min(spam, count),
            "duplicate_count": min(duplicates, count),
            "content_mix": {} if count == 0 else {"requests": 0.6, "discussion": 0.4},
            "primary_language": None if count == 0 else "ru",
            "languages": []
            if count == 0
            else [{"key": "ru", "display_name": "Russian"}],
            "categories": categories
            if categories is not None
            else [{"key": "software", "display_name": "Software"}],
        }
    )


if __name__ == "__main__":
    unittest.main()
