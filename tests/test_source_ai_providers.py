from __future__ import annotations

import json
import unittest
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from unittest.mock import patch

from postgres_support import TEST_DATABASE_URL, migrate_to_head, temporary_database

from freelancer_bot.config import RuntimeConfig
from freelancer_bot.persistence.collector_accounts import CollectorAccountRepository
from freelancer_bot.persistence.database import Database
from freelancer_bot.persistence.source_repository import SourceRepository, SourceStatus
from freelancer_bot.persistence.telegram_chat_discovery import (
    TelegramChatDiscoveryRepository,
)
from freelancer_bot.source_ai_config import (
    SourceAIProviderUnavailable,
    UnsupportedSourceAIProvider,
    normalize_chat_completions_url,
)
from freelancer_bot.source_audit import (
    SourceAuditClassification,
    SourceAuditDecisionPolicy,
    SourceAuditError,
    source_audit_provider_from_config,
)
from freelancer_bot.source_audit_sampler import SourceAuditMessage
from freelancer_bot.source_discovery_runtime import AutonomousSourceDiscoveryRuntime
from freelancer_bot.telegram_chat_discovery import (
    ScreenMessage,
    TelegramChatDiscoveryService,
    TelegramChatScreenError,
)

NOW = datetime(2026, 8, 20, 12, 0, tzinfo=UTC)


class _Response:
    def __init__(self, payload):
        self.payload = payload

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return None

    def read(self):
        return json.dumps(self.payload).encode()


def _source_classification(count: int) -> SourceAuditClassification:
    return SourceAuditClassification.model_validate(
        {
            "schema_version": "source-audit.v1",
            "analyzed_message_count": count,
            "commercial_opportunity_count": min(5, count),
            "buyer_intent_count": min(6, count),
            "seller_promotion_count": min(2, count),
            "ads_spam_count": min(1, count),
            "duplicate_count": min(1, count),
            "content_mix": {
                "buyer_demand": 0.6,
                "seller_promotion": 0.1,
                "ads_spam": 0.1,
                "duplicate": 0.1,
                "other": 0.1,
            },
            "primary_language": "en",
            "languages": [{"key": "en", "display_name": "English"}],
            "categories": [{"key": "software", "display_name": "Software"}],
        }
    )


def _source_response(classification: SourceAuditClassification) -> dict:
    return {"choices": [{"message": {"content": classification.model_dump_json()}}]}


def _screen_response() -> dict:
    return {
        "choices": [
            {
                "message": {
                    "content": json.dumps(
                        {
                            "decision": "WATCH",
                            "confidence": 0.95,
                            "labels": [
                                {
                                    "message_index": 1,
                                    "category": "BUYER_TO_SPECIALIST",
                                    "confidence": 0.95,
                                }
                            ],
                            "reason_codes": ["fixture"],
                        }
                    )
                }
            }
        ]
    }


def _config(
    provider: str,
    *,
    key: str | None = "selected-key",
    tokenrouter_url: str = "https://router.example/v1",
    source_audit_enabled: bool = False,
):
    values = {
        "_env_file": None,
        "source_audit_provider": provider,
        "source_audit_model": "source-stage-test-model",
        "source_audit_temperature": 0,
        "source_audit_timeout_seconds": 12,
        "openai_api_key": None,
        "deepseek_api_key": None,
        "tokenrouter_api_key": None,
        "tokenrouter_base_url": tokenrouter_url,
        "source_audit_enabled": source_audit_enabled,
    }
    if provider == "openai":
        values["openai_api_key"] = key
    elif provider == "deepseek":
        values["deepseek_api_key"] = key
    elif provider == "tokenrouter":
        values["tokenrouter_api_key"] = key
    return RuntimeConfig(**values)


class SourceAIProviderPayloadTest(unittest.IsolatedAsyncioTestCase):
    async def test_source_audit_payloads_select_provider_key_and_output_mode(self):
        expected = {
            "openai": (
                "https://api.openai.com/v1/chat/completions",
                "openai-secret",
                "json_schema",
            ),
            "deepseek": (
                "https://api.deepseek.com/chat/completions",
                "deepseek-secret",
                "json_object",
            ),
            "tokenrouter": (
                "https://router.example/v1/chat/completions",
                "tokenrouter-secret",
                "json_object",
            ),
        }
        sample = SimpleNamespace(
            source_id=7,
            window_started_at=NOW - timedelta(days=3),
            window_ended_at=NOW,
            messages=(SourceAuditMessage(1, NOW - timedelta(hours=1), "buyer demand"),),
        )
        for provider_name, (url, key, output_mode) in expected.items():
            with self.subTest(provider=provider_name):
                provider = source_audit_provider_from_config(
                    _config(provider_name, key=key)
                )
                calls = []

                def fake_urlopen(request, timeout, calls=calls):
                    calls.append(
                        {
                            "url": request.full_url,
                            "authorization": request.headers.get("Authorization"),
                            "payload": json.loads(request.data),
                            "timeout": timeout,
                        }
                    )
                    return _Response(_source_response(_source_classification(1)))

                with patch(
                    "freelancer_bot.source_audit.urllib.request.urlopen",
                    fake_urlopen,
                ):
                    result = await provider.classify(sample)

                self.assertEqual(provider.name, provider_name)
                self.assertEqual(
                    provider.analyzer_version,
                    f"{provider_name}-source-audit-v1",
                )
                self.assertEqual(calls[0]["url"], url)
                self.assertEqual(calls[0]["authorization"], f"Bearer {key}")
                self.assertEqual(calls[0]["payload"]["response_format"]["type"], output_mode)
                if provider_name == "openai":
                    self.assertTrue(
                        calls[0]["payload"]["response_format"]["json_schema"]["strict"]
                    )
                else:
                    prompt = calls[0]["payload"]["messages"][0]["content"]
                    self.assertIn("analyzed_message_count", prompt)
                    self.assertIn("source-audit.v1", prompt)
                self.assertEqual(result.analyzed_message_count, 1)

    async def test_chat_screen_payloads_and_policy_are_provider_neutral(self):
        expected = {
            "openai": ("https://api.openai.com/v1/chat/completions", "json_schema"),
            "deepseek": ("https://api.deepseek.com/chat/completions", "json_object"),
            "tokenrouter": ("https://router.example/v1/chat/completions", "json_object"),
        }
        decisions = []
        messages = (ScreenMessage(1, NOW - timedelta(minutes=1), "buyer demand"),)
        peer = SimpleNamespace(peer_type="supergroup", display_name="Fixture")
        for provider_name, (url, output_mode) in expected.items():
            with self.subTest(provider=provider_name):
                config = _config(provider_name)
                service = TelegramChatDiscoveryService(
                    None,
                    None,
                    config=config,
                    collector_account_id=1,
                    governor=SimpleNamespace(),
                )
                calls = []

                def fake_urlopen(request, timeout, calls=calls):
                    calls.append(
                        {
                            "url": request.full_url,
                            "payload": json.loads(request.data),
                            "timeout": timeout,
                        }
                    )
                    return _Response(_screen_response())

                with patch(
                    "freelancer_bot.telegram_chat_discovery.urllib.request.urlopen",
                    fake_urlopen,
                ):
                    classification = await service.screen_provider.classify(peer, messages)

                self.assertEqual(service.screen_provider.name, provider_name)
                self.assertEqual(calls[0]["url"], url)
                self.assertEqual(calls[0]["payload"]["response_format"]["type"], output_mode)
                if provider_name == "openai":
                    self.assertTrue(
                        calls[0]["payload"]["response_format"]["json_schema"]["strict"]
                    )
                else:
                    self.assertIn(
                        "message_index",
                        calls[0]["payload"]["messages"][0]["content"],
                    )
                decisions.append(
                    service.policy.evaluate(
                        sample_count=1,
                        classification=classification,
                    )[0]
                )
        self.assertEqual(decisions, ["UNCLEAR", "UNCLEAR", "UNCLEAR"])

    async def test_selected_provider_key_is_required_and_unknown_provider_fails_closed(self):
        with self.assertRaises(SourceAIProviderUnavailable):
            source_audit_provider_from_config(
                _config("deepseek", key=None)
            )
        with self.assertRaises(UnsupportedSourceAIProvider):
            source_audit_provider_from_config(_config("unknown", key="ignored"))

        service = TelegramChatDiscoveryService(
            None,
            None,
            config=_config("deepseek", key=None),
            collector_account_id=1,
            governor=SimpleNamespace(),
        )
        self.assertIsNone(service.screen_provider)
        with self.assertRaises(TelegramChatScreenError):
            TelegramChatDiscoveryService(
                None,
                None,
                config=_config("unknown", key="ignored"),
                collector_account_id=1,
                governor=SimpleNamespace(),
            )

    def test_tokenrouter_url_normalization_is_idempotent(self):
        self.assertEqual(
            normalize_chat_completions_url("https://router.example/v1"),
            "https://router.example/v1/chat/completions",
        )
        self.assertEqual(
            normalize_chat_completions_url(
                "https://router.example/v1/chat/completions/"
            ),
            "https://router.example/v1/chat/completions",
        )

    async def test_invalid_output_error_names_selected_provider(self):
        provider = source_audit_provider_from_config(_config("deepseek"))
        provider._max_output_attempts = 1
        sample = SimpleNamespace(
            source_id=7,
            window_started_at=NOW - timedelta(days=3),
            window_ended_at=NOW,
            messages=(SourceAuditMessage(1, NOW - timedelta(hours=1), "buyer demand"),),
        )

        with (
            patch(
                "freelancer_bot.source_audit.urllib.request.urlopen",
                lambda request, timeout: _Response(
                    {"choices": [{"message": {"content": "not-json"}}]}
                ),
            ),
            self.assertRaisesRegex(SourceAuditError, "deepseek returned"),
        ):
            await provider.classify(sample)

    async def test_equivalent_source_classifications_have_identical_policy_decisions(self):
        sample = SimpleNamespace(
            source_id=7,
            window_started_at=NOW - timedelta(days=3),
            window_ended_at=NOW,
            messages=(SourceAuditMessage(1, NOW - timedelta(hours=1), "buyer demand"),),
        )
        decisions = []
        for provider_name in ("openai", "deepseek", "tokenrouter"):
            provider = source_audit_provider_from_config(_config(provider_name))
            with patch(
                "freelancer_bot.source_audit.urllib.request.urlopen",
                lambda request, timeout: _Response(
                    _source_response(_source_classification(1))
                ),
            ):
                classification = await provider.classify(sample)
            decisions.append(SourceAuditDecisionPolicy().decide(classification))
        self.assertEqual(decisions[0], decisions[1])
        self.assertEqual(decisions[1], decisions[2])


@unittest.skipUnless(TEST_DATABASE_URL, "TEST_DATABASE_URL is not configured")
class SourceAIProviderRuntimePostgresTest(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.database_context = temporary_database()
        self.database_url = self.database_context.__enter__()
        migrate_to_head(self.database_url)
        self.database = Database(self.database_url, pool_size=4, max_overflow=8)

    async def asyncTearDown(self) -> None:
        await self.database.close()
        self.database_context.__exit__(None, None, None)

    async def test_source_discovery_runtime_uses_deepseek_factory_end_to_end(self):
        async with self.database.transaction() as connection:
            account = await CollectorAccountRepository().ensure(
                connection,
                platform="telegram",
                external_account_id="provider-runtime-fixture",
                display_name="Provider runtime fixture",
            )
            source = await SourceRepository().create_candidate(
                connection,
                platform="telegram",
                external_id="provider-runtime-source",
                access_type="public",
                display_name="Provider runtime source",
                provider="telegram_chat_search",
                lineage_key="provider-runtime-lineage",
                handle="@provider_runtime",
            )

        class Client:
            async def get_entity(self, _lookup):
                return object()

            async def iter_messages(self, _entity, *, offset_date, limit):
                for index in range(1, 61):
                    yield SimpleNamespace(
                        id=index,
                        date=NOW - timedelta(minutes=index),
                        message="buyer demand",
                        media=None,
                    )

        config = _config("deepseek", source_audit_enabled=True)
        runtime = AutonomousSourceDiscoveryRuntime(self.database, Client(), config)
        pipeline = runtime._build_audit_pipeline()
        self.assertIsNotNone(pipeline)
        self.assertEqual(pipeline._provider.name, "deepseek")
        self.assertIsNone(config.openai_api_key)

        calls = []

        def fake_urlopen(request, timeout):
            calls.append(
                {
                    "url": request.full_url,
                    "payload": json.loads(request.data),
                }
            )
            return _Response(_source_response(_source_classification(60)))

        with patch(
            "freelancer_bot.source_audit.urllib.request.urlopen",
            fake_urlopen,
        ):
            result = await runtime._audit_candidate(
                source.id,
                collector_account_id=account.id,
                pipeline=pipeline,
                audited_at=NOW,
            )

        self.assertIsNotNone(result)
        self.assertEqual(result.audit.provider, "deepseek")
        self.assertEqual(result.source.lifecycle_status, SourceStatus.APPROVED)
        self.assertEqual(calls[0]["url"], "https://api.deepseek.com/chat/completions")
        self.assertEqual(calls[0]["payload"]["response_format"]["type"], "json_object")

    async def test_private_chat_boundary_precedes_selected_provider(self):
        async with self.database.transaction() as connection:
            account = await CollectorAccountRepository().ensure(
                connection,
                platform="telegram",
                external_account_id="private-provider-fixture-2",
                display_name="Private provider fixture 2",
            )
            peer, _created = await TelegramChatDiscoveryRepository().upsert_peer(
                connection,
                canonical_peer_identity="private-provider-peer",
                peer_type="supergroup",
                telegram_peer_id=901,
                telegram_access_hash=902,
                display_name="Private provider peer",
                username=None,
                canonical_url=None,
                access_type="private",
                source_id=None,
                dedup_bucket="GENUINELY_NEW",
                collector_account_id=account.id,
            )
            await TelegramChatDiscoveryRepository().enqueue_screen_job(
                connection,
                peer_id=peer.id,
                attempt_number=1,
            )

        class Client:
            def __init__(self):
                self.history_calls = 0

            async def get_messages(self, _entity, *, limit):
                self.history_calls += 1
                raise AssertionError("private provider control read history")

        client = Client()
        service = TelegramChatDiscoveryService(
            self.database,
            client,
            config=_config("deepseek"),
            collector_account_id=account.id,
            governor=SimpleNamespace(),
        )
        result = await service.screen_peer(peer.id)
        self.assertEqual(result.status, "SKIP")
        self.assertEqual(result.reason_codes, ("private_source_not_global",))
        self.assertEqual(client.history_calls, 0)
        self.assertEqual(service.screen_provider.name, "deepseek")


if __name__ == "__main__":
    unittest.main()
