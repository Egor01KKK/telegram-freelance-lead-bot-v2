from __future__ import annotations

import json
import time
import unittest
from unittest.mock import patch
import urllib.error
from uuid import uuid4

from pydantic import ValidationError
import sqlalchemy as sa

from freelancer_bot.config import RuntimeConfig, RuntimeMode
from freelancer_bot.persistence.database import Database
from freelancer_bot.persistence.search_profiles import (
    SearchProfileAnalysisCacheRepository,
)
from freelancer_bot.persistence.schema import (
    search_profile_analysis_cache,
    search_profile_onboarding_attempts,
    search_profiles,
    users,
)
from freelancer_bot.profile_onboarding import (
    ONBOARDING_PROFILE_ANALYSIS_SCHEMA_VERSION,
    ONBOARDING_PROFILE_ANALYZER_VERSION,
    ONBOARDING_PROFILE_PROMPT_VERSION,
    OnboardingProfileAnalysis,
    OnboardingProfileAnalysisCall,
    OnboardingProfileAnalyzer,
    OnboardingProfileError,
    OnboardingProfileOutputError,
    OnboardingProfileUsage,
    OpenAICompatibleOnboardingProfileAnalyzer,
    OpenAIOnboardingProfileAnalyzer,
    onboarding_profile_cache_version,
    parsed_profile_from_analysis,
    validate_onboarding_profile_grounding,
)
from freelancer_bot.profile_onboarding_service import ProfileOnboardingService
from freelancer_bot.search_profiles import SearchProfileTermOrigin
from postgres_support import TEST_DATABASE_URL, migrate_to_head, temporary_database


DESCRIPTION = (
    "Я продуктовый дизайнер. Работаю в Figma и делаю SaaS-сервисы и лендинги."
)


class FakeOnboardingAnalyzer:
    provider = "fixture"
    model = "fixture-profile-model"
    analyzer_version = ONBOARDING_PROFILE_ANALYZER_VERSION
    prompt_version = ONBOARDING_PROFILE_PROMPT_VERSION
    schema_version = ONBOARDING_PROFILE_ANALYSIS_SCHEMA_VERSION

    def __init__(self, analysis: OnboardingProfileAnalysis | None = None) -> None:
        self.analysis = analysis or _analysis()
        self.calls: list[str] = []

    async def analyze(self, description: str) -> OnboardingProfileAnalysisCall:
        self.calls.append(description)
        return OnboardingProfileAnalysisCall(
            analysis=self.analysis,
            provider=self.provider,
            requested_model=self.model,
            response_model=f"{self.model}-resolved",
            analyzer_version=self.analyzer_version,
            prompt_version=self.prompt_version,
            schema_version=self.schema_version,
            attempt_count=1,
            usage=OnboardingProfileUsage(
                input_tokens=40,
                output_tokens=20,
                total_tokens=60,
            ),
        )


class FailingOnboardingAnalyzer(FakeOnboardingAnalyzer):
    async def analyze(self, description: str) -> OnboardingProfileAnalysisCall:
        self.calls.append(description)
        raise OnboardingProfileOutputError("terminal fixture failure")


class OnboardingProfileContractTest(unittest.IsolatedAsyncioTestCase):
    def test_strict_contract_rejects_extra_missing_and_unusable_output(self):
        valid = _analysis_payload()
        parsed = OnboardingProfileAnalysis.model_validate_json(
            json.dumps(valid),
            strict=True,
        )
        self.assertEqual(
            parsed.schema_version,
            ONBOARDING_PROFILE_ANALYSIS_SCHEMA_VERSION,
        )

        invalid = (
            {**valid, "unexpected": True},
            {**valid, "missing_fields": ["roles"]},
            {
                **valid,
                "roles": [],
                "skills": [],
                "categories": [],
                "missing_fields": ["roles", "skills", "categories"],
            },
        )
        for payload in invalid:
            with self.subTest(payload=payload):
                with self.assertRaises(ValidationError):
                    OnboardingProfileAnalysis.model_validate_json(
                        json.dumps(payload),
                        strict=True,
                    )

    def test_grounding_preserves_evidence_and_marks_normalized_inference(self):
        analysis = _analysis()

        parsed = parsed_profile_from_analysis(DESCRIPTION, analysis)

        self.assertEqual(parsed.semantic_text_original, DESCRIPTION)
        self.assertEqual(parsed.roles[0].evidence, "продуктовый дизайнер")
        self.assertEqual(
            parsed.roles[0].origin,
            SearchProfileTermOrigin.EXPLICIT,
        )
        self.assertEqual(
            parsed.categories[0].origin,
            SearchProfileTermOrigin.NORMALIZED_INFERENCE,
        )
        self.assertEqual(parsed.categories[0].evidence, "SaaS-сервисы")

        unsupported = OnboardingProfileAnalysis.model_validate_json(
            json.dumps(
                {
                    **_analysis_payload(),
                    "skills": [
                        {
                            "value": "Python",
                            "evidence": "Python",
                            "origin": "explicit",
                        }
                    ],
                }
            ),
            strict=True,
        )
        with self.assertRaises(OnboardingProfileOutputError):
            validate_onboarding_profile_grounding(unsupported, DESCRIPTION)

    def test_provider_protocol_and_cache_identity_are_replaceable_and_versioned(self):
        first = FakeOnboardingAnalyzer()
        second = FakeOnboardingAnalyzer()
        second.model = "fixture-other-profile-model"

        self.assertIsInstance(first, OnboardingProfileAnalyzer)
        self.assertNotEqual(
            onboarding_profile_cache_version(first),
            onboarding_profile_cache_version(second),
        )
        configured = OpenAIOnboardingProfileAnalyzer(
            api_key="profile-secret",
            temperature=0.1,
        )
        other_temperature = OpenAIOnboardingProfileAnalyzer(
            api_key="profile-secret",
            temperature=0.2,
        )
        self.assertNotEqual(
            onboarding_profile_cache_version(configured),
            onboarding_profile_cache_version(other_temperature),
        )

    async def test_openai_adapter_uses_strict_schema_and_bounded_output_retry(self):
        captured: list[dict] = []
        responses = [
            _openai_response("{}"),
            _openai_response(json.dumps(_analysis_payload(), ensure_ascii=False)),
        ]

        class Response:
            def __init__(self, payload: str) -> None:
                self.payload = payload

            def __enter__(self):
                return self

            def __exit__(self, *args):
                return None

            def read(self):
                return self.payload.encode("utf-8")

        def fake_urlopen(request, timeout):
            captured.append(
                {
                    "payload": json.loads(request.data),
                    "timeout": timeout,
                    "authorization": request.headers["Authorization"],
                }
            )
            return Response(responses.pop(0))

        analyzer = OpenAIOnboardingProfileAnalyzer(
            api_key="profile-secret",
            model="replaceable-profile-model",
            timeout_seconds=17,
            max_output_attempts=2,
        )
        with patch(
            "freelancer_bot.profile_onboarding.urllib.request.urlopen",
            fake_urlopen,
        ):
            result = await analyzer.analyze(DESCRIPTION)

        self.assertEqual(result.attempt_count, 2)
        self.assertEqual(len(captured), 2)
        self.assertEqual(captured[0]["payload"]["model"], "replaceable-profile-model")
        self.assertTrue(
            captured[0]["payload"]["response_format"]["json_schema"]["strict"]
        )
        self.assertEqual(captured[0]["timeout"], 17)
        self.assertNotIn("profile-secret", json.dumps(captured[0]["payload"]))

    async def test_openai_adapter_stops_after_configured_invalid_outputs(self):
        calls = 0

        class Response:
            def __enter__(self):
                return self

            def __exit__(self, *args):
                return None

            def read(self):
                return _openai_response("{}").encode("utf-8")

        def fake_urlopen(request, timeout):
            nonlocal calls
            calls += 1
            return Response()

        analyzer = OpenAIOnboardingProfileAnalyzer(
            api_key="profile-secret",
            max_output_attempts=2,
        )
        with patch(
            "freelancer_bot.profile_onboarding.urllib.request.urlopen",
            fake_urlopen,
        ):
            with self.assertRaises(OnboardingProfileOutputError):
                await analyzer.analyze(DESCRIPTION)
        self.assertEqual(calls, 2)

    async def test_openai_compatible_deepseek_adapter_uses_json_object_and_shared_contract(self):
        captured: list[dict] = []

        class Response:
            def __enter__(self):
                return self

            def __exit__(self, *args):
                return None

            def read(self):
                return _openai_response(
                    json.dumps(_analysis_payload(), ensure_ascii=False)
                ).encode("utf-8")

        def fake_urlopen(request, timeout):
            captured.append(
                {
                    "url": request.full_url,
                    "payload": json.loads(request.data),
                    "authorization": request.headers["Authorization"],
                    "timeout": timeout,
                }
            )
            return Response()

        analyzer = OpenAICompatibleOnboardingProfileAnalyzer(
            provider="deepseek",
            api_key="deepseek-secret",
            model="deepseek-v4-flash",
            base_url="https://api.deepseek.com/chat/completions",
            timeout_seconds=17,
            max_output_attempts=1,
            max_transport_attempts=1,
        )
        with patch(
            "freelancer_bot.profile_onboarding.urllib.request.urlopen",
            fake_urlopen,
        ):
            result = await analyzer.analyze(DESCRIPTION)

        self.assertEqual(result.provider, "deepseek")
        self.assertEqual(result.requested_model, "deepseek-v4-flash")
        self.assertEqual(result.provider_metrics.completed_calls, 1)
        self.assertEqual(result.usage.total_tokens, 60)
        self.assertEqual(captured[0]["url"], "https://api.deepseek.com/chat/completions")
        self.assertEqual(
            captured[0]["payload"]["response_format"],
            {"type": "json_object"},
        )
        self.assertEqual(captured[0]["payload"]["model"], "deepseek-v4-flash")
        self.assertEqual(captured[0]["authorization"], "Bearer deepseek-secret")

    async def test_openai_compatible_tokenrouter_adapter_uses_gateway_model_and_json_object(self):
        captured = []

        class Response:
            def __enter__(self):
                return self

            def __exit__(self, *args):
                return None

            def read(self):
                return _openai_response(
                    json.dumps(_analysis_payload(), ensure_ascii=False)
                ).encode("utf-8")

        def fake_urlopen(request, timeout):
            captured.append(
                {
                    "url": request.full_url,
                    "payload": json.loads(request.data),
                    "authorization": request.headers["Authorization"],
                    "timeout": timeout,
                }
            )
            return Response()

        analyzer = OpenAICompatibleOnboardingProfileAnalyzer(
            provider="tokenrouter",
            api_key="tokenrouter-secret",
            model="deepseek/deepseek-v4-pro-0813-free",
            base_url="https://api.tokenrouter.com/v1/chat/completions",
            timeout_seconds=17,
            max_output_attempts=1,
            max_transport_attempts=1,
        )
        with patch(
            "freelancer_bot.profile_onboarding.urllib.request.urlopen",
            fake_urlopen,
        ):
            result = await analyzer.analyze(DESCRIPTION)

        self.assertEqual(result.provider, "tokenrouter")
        self.assertEqual(
            result.requested_model,
            "deepseek/deepseek-v4-pro-0813-free",
        )
        self.assertEqual(
            captured[0]["url"],
            "https://api.tokenrouter.com/v1/chat/completions",
        )
        self.assertEqual(
            captured[0]["payload"]["response_format"],
            {"type": "json_object"},
        )
        self.assertEqual(
            captured[0]["payload"]["model"],
            "deepseek/deepseek-v4-pro-0813-free",
        )
        self.assertEqual(captured[0]["authorization"], "Bearer tokenrouter-secret")

    async def test_provider_call_budget_is_hard_and_reported(self):
        calls = 0

        class Response:
            def __enter__(self):
                return self

            def __exit__(self, *args):
                return None

            def read(self):
                return _openai_response("{}").encode("utf-8")

        def fake_urlopen(request, timeout):
            nonlocal calls
            calls += 1
            return Response()

        analyzer = OpenAICompatibleOnboardingProfileAnalyzer(
            provider="deepseek",
            api_key="deepseek-secret",
            model="deepseek-v4-flash",
            max_output_attempts=3,
            max_transport_attempts=1,
            max_ai_calls_per_run=2,
        )
        with patch(
            "freelancer_bot.profile_onboarding.urllib.request.urlopen",
            fake_urlopen,
        ):
            with self.assertRaises(OnboardingProfileError) as raised:
                await analyzer.analyze(DESCRIPTION)

        self.assertEqual(calls, 2)
        self.assertEqual(raised.exception.failure_class, "budget_exceeded")
        self.assertFalse(raised.exception.retryable)
        self.assertEqual(raised.exception.provider_metrics.provider_attempts, 2)

    def test_runtime_configuration_selects_deepseek_provider_key_and_model(self):
        with patch.dict(
            "os.environ",
            {
                "DEEPSEEK_API_KEY": "deepseek-secret",
                "ONBOARDING_PROFILE_PROVIDER": "deepseek",
                "ONBOARDING_PROFILE_MODEL": "deepseek-v4-flash",
            },
            clear=True,
        ):
            config = RuntimeConfig.from_env(
                mode=RuntimeMode.CHECK_CONFIG,
                env_file=None,
            )

        analyzer = OpenAICompatibleOnboardingProfileAnalyzer.from_config(config)

        self.assertEqual(analyzer.provider, "deepseek")
        self.assertEqual(analyzer.model, "deepseek-v4-flash")
        self.assertEqual(config.deepseek_api_key.get_secret_value(), "deepseek-secret")

    def test_runtime_configuration_selects_tokenrouter_key_model_and_base_url(self):
        with patch.dict(
            "os.environ",
            {
                "TOKENROUTER_API_KEY": "tokenrouter-secret",
                "TOKENROUTER_BASE_URL": "https://gateway.example/v1",
                "ONBOARDING_PROFILE_PROVIDER": "tokenrouter",
                "ONBOARDING_PROFILE_MODEL": "deepseek/deepseek-v4-pro-0813-free",
            },
            clear=True,
        ):
            config = RuntimeConfig.from_env(
                mode=RuntimeMode.CHECK_CONFIG,
                env_file=None,
            )

        analyzer = OpenAICompatibleOnboardingProfileAnalyzer.from_config(config)

        self.assertEqual(analyzer.provider, "tokenrouter")
        self.assertEqual(
            analyzer.model,
            "deepseek/deepseek-v4-pro-0813-free",
        )
        self.assertEqual(
            analyzer.base_url,
            "https://gateway.example/v1/chat/completions",
        )
        self.assertEqual(
            config.tokenrouter_api_key.get_secret_value(),
            "tokenrouter-secret",
        )

    def test_openai_configuration_remains_the_default_compatible_provider(self):
        with patch.dict(
            "os.environ",
            {
                "OPENAI_API_KEY": "openai-secret",
                "ONBOARDING_PROFILE_PROVIDER": "openai",
            },
            clear=True,
        ):
            config = RuntimeConfig.from_env(
                mode=RuntimeMode.CHECK_CONFIG,
                env_file=None,
            )

        analyzer = OpenAICompatibleOnboardingProfileAnalyzer.from_config(config)

        self.assertEqual(analyzer.provider, "openai")
        self.assertEqual(analyzer.model, "gpt-5-nano")

    async def test_openai_transport_timeout_has_a_hard_async_deadline(self):
        class HangingResponse:
            def __enter__(self):
                return self

            def __exit__(self, *args):
                return None

            def read(self):
                time.sleep(0.20)
                return b"{}"

        def fake_urlopen(request, timeout):
            time.sleep(0.20)
            return HangingResponse()

        analyzer = OpenAIOnboardingProfileAnalyzer(
            api_key="profile-secret",
            timeout_seconds=0.03,
            max_transport_attempts=1,
            transport_retry_backoff_seconds=0,
        )
        started = time.monotonic()
        with patch(
            "freelancer_bot.profile_onboarding.urllib.request.urlopen",
            fake_urlopen,
        ):
            with self.assertRaises(OnboardingProfileError) as raised:
                await analyzer.analyze(DESCRIPTION)
        elapsed = time.monotonic() - started

        self.assertLess(elapsed, 0.12)
        self.assertEqual(raised.exception.failure_class, "timeout")
        self.assertEqual(raised.exception.provider_metrics.provider_attempts, 1)
        self.assertEqual(raised.exception.provider_metrics.timeouts, 1)

    async def test_openai_transport_timeout_retries_once_then_succeeds(self):
        calls = 0

        class Response:
            def __enter__(self):
                return self

            def __exit__(self, *args):
                return None

            def read(self):
                return _openai_response(json.dumps(_analysis_payload(), ensure_ascii=False)).encode("utf-8")

        def fake_urlopen(request, timeout):
            nonlocal calls
            calls += 1
            if calls == 1:
                time.sleep(0.10)
            return Response()

        analyzer = OpenAIOnboardingProfileAnalyzer(
            api_key="profile-secret",
            timeout_seconds=0.03,
            max_transport_attempts=2,
            max_output_attempts=1,
            transport_retry_backoff_seconds=0,
        )
        with patch(
            "freelancer_bot.profile_onboarding.urllib.request.urlopen",
            fake_urlopen,
        ):
            result = await analyzer.analyze(DESCRIPTION)

        self.assertEqual(calls, 2)
        self.assertEqual(result.provider_metrics.provider_attempts, 2)
        self.assertEqual(result.provider_metrics.timeouts, 1)
        self.assertEqual(result.provider_metrics.completed_calls, 1)
        self.assertEqual(result.provider_metrics.invalid_output_retries, 0)

    async def test_openai_transport_timeout_twice_is_bounded_and_retryable(self):
        calls = 0

        def fake_urlopen(request, timeout):
            nonlocal calls
            calls += 1
            time.sleep(0.10)

        analyzer = OpenAIOnboardingProfileAnalyzer(
            api_key="profile-secret",
            timeout_seconds=0.03,
            max_transport_attempts=2,
            max_output_attempts=1,
            transport_retry_backoff_seconds=0,
        )
        with patch(
            "freelancer_bot.profile_onboarding.urllib.request.urlopen",
            fake_urlopen,
        ):
            with self.assertRaises(OnboardingProfileError) as raised:
                await analyzer.analyze(DESCRIPTION)

        self.assertEqual(calls, 2)
        self.assertTrue(raised.exception.retryable)
        self.assertEqual(raised.exception.failure_class, "timeout")
        self.assertEqual(raised.exception.provider_metrics.provider_attempts, 2)
        self.assertEqual(raised.exception.provider_metrics.timeouts, 2)
        self.assertEqual(raised.exception.provider_metrics.completed_calls, 0)

    async def test_openai_non_retryable_http_failure_is_not_retried(self):
        calls = 0

        def fake_urlopen(request, timeout):
            nonlocal calls
            calls += 1
            raise urllib.error.HTTPError(
                request.full_url,
                401,
                "unauthorized",
                hdrs=None,
                fp=None,
            )

        analyzer = OpenAIOnboardingProfileAnalyzer(
            api_key="profile-secret",
            max_transport_attempts=2,
            transport_retry_backoff_seconds=0,
        )
        with patch(
            "freelancer_bot.profile_onboarding.urllib.request.urlopen",
            fake_urlopen,
        ):
            with self.assertRaises(OnboardingProfileError) as raised:
                await analyzer.analyze(DESCRIPTION)

        self.assertEqual(calls, 1)
        self.assertFalse(raised.exception.retryable)
        self.assertEqual(raised.exception.failure_class, "http_4xx")
        self.assertEqual(raised.exception.provider_metrics.non_retryable_failures, 1)
        self.assertEqual(raised.exception.provider_metrics.transient_failures, 0)

    def test_runtime_configuration_keeps_provider_and_model_replaceable(self):
        with patch.dict(
            "os.environ",
            {
                "OPENAI_API_KEY": "profile-secret",
                "ONBOARDING_PROFILE_PROVIDER": "openai",
                "ONBOARDING_PROFILE_MODEL": "cheap-profile-model",
                "ONBOARDING_PROFILE_TEMPERATURE": "0.2",
                "ONBOARDING_PROFILE_TIMEOUT_SECONDS": "19",
                "ONBOARDING_PROFILE_MAX_OUTPUT_ATTEMPTS": "3",
                "ONBOARDING_PROFILE_MAX_TRANSPORT_ATTEMPTS": "2",
                "ONBOARDING_PROFILE_TRANSPORT_RETRY_BACKOFF_SECONDS": "0.5",
            },
            clear=True,
        ):
            config = RuntimeConfig.from_env(
                mode=RuntimeMode.CHECK_CONFIG,
                env_file=None,
            )
        analyzer = OpenAIOnboardingProfileAnalyzer.from_config(config)

        self.assertEqual(analyzer.model, "cheap-profile-model")
        self.assertEqual(config.onboarding_profile_temperature, 0.2)
        self.assertEqual(config.onboarding_profile_timeout_seconds, 19)
        self.assertEqual(config.onboarding_profile_max_output_attempts, 3)
        self.assertEqual(config.onboarding_profile_max_transport_attempts, 2)
        self.assertEqual(config.onboarding_profile_transport_retry_backoff_seconds, 0.5)


@unittest.skipUnless(TEST_DATABASE_URL, "TEST_DATABASE_URL is not configured")
class ProfileOnboardingServiceTest(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.database_context = temporary_database()
        self.database_url = self.database_context.__enter__()
        migrate_to_head(self.database_url)
        self.database = Database(self.database_url, pool_size=4, max_overflow=8)

    async def asyncTearDown(self):
        await self.database.close()
        self.database_context.__exit__(None, None, None)

    async def test_free_text_creates_usable_profiles_and_reuses_global_cache(self):
        analyzer = FakeOnboardingAnalyzer()
        service = ProfileOnboardingService(self.database, analyzer)

        first = await service.create_from_description(
            platform="telegram",
            external_user_id="onboarding-user-1",
            description=DESCRIPTION,
            profile_id=uuid4(),
        )
        second = await service.create_from_description(
            platform="telegram",
            external_user_id="onboarding-user-2",
            description=DESCRIPTION,
            profile_id=uuid4(),
        )

        self.assertEqual(analyzer.calls, [DESCRIPTION])
        self.assertTrue(first.model_invoked)
        self.assertTrue(first.cache_created)
        self.assertFalse(second.model_invoked)
        self.assertFalse(second.cache_created)
        self.assertEqual(first.analysis_cache.id, second.analysis_cache.id)
        self.assertEqual(first.profile.analysis_cache_id, first.analysis_cache.id)
        self.assertEqual(first.profile.semantic_text_original, DESCRIPTION)
        self.assertEqual(
            [term.value for term in first.profile.roles],
            ["продуктовый дизайнер"],
        )
        self.assertEqual(
            first.profile.categories[0].origin,
            SearchProfileTermOrigin.NORMALIZED_INFERENCE,
        )
        self.assertEqual(first.analysis_cache.requested_model, analyzer.model)
        self.assertEqual(first.analysis_cache.total_tokens, 60)
        self.assertEqual(first.analysis_cache.provider_attempts, 1)
        self.assertEqual(first.analysis_cache.completed_calls, 1)
        self.assertEqual(first.analysis_cache.invalid_output_retry_count, 0)
        self.assertNotEqual(first.user.id, second.user.id)
        async with self.database.connect() as connection:
            audited = await SearchProfileAnalysisCacheRepository().get(
                connection,
                first.profile.analysis_cache_id,
            )
        self.assertEqual(audited.analysis, first.analysis)

    async def test_terminal_analysis_failure_leaves_no_partial_v2_state(self):
        analyzer = FailingOnboardingAnalyzer()
        service = ProfileOnboardingService(self.database, analyzer)

        with self.assertRaises(OnboardingProfileOutputError):
            await service.create_from_description(
                platform="telegram",
                external_user_id="failed-onboarding-user",
                description=DESCRIPTION,
            )

        async with self.database.connect() as connection:
            counts = [
                await connection.scalar(sa.select(sa.func.count()).select_from(table))
                for table in (users, search_profiles, search_profile_analysis_cache)
            ]
            attempt = (
                await connection.execute(
                    sa.select(search_profile_onboarding_attempts)
                )
            ).mappings().one()
        self.assertEqual(counts, [0, 0, 0])
        self.assertEqual(attempt["status"], "failed")
        self.assertTrue(attempt["retryable"])
        self.assertEqual(attempt["error_code"], "invalid_output")

    async def test_cache_repository_rejects_inconsistent_input_identity(self):
        analyzer = FakeOnboardingAnalyzer()
        call = await analyzer.analyze(DESCRIPTION)

        with self.assertRaises(ValueError):
            async with self.database.transaction() as connection:
                await SearchProfileAnalysisCacheRepository().record(
                    connection,
                    input_sha256="0" * 64,
                    original_input_text=DESCRIPTION,
                    normalized_input_text=DESCRIPTION,
                    cache_version=onboarding_profile_cache_version(analyzer),
                    call=call,
                )


def _analysis() -> OnboardingProfileAnalysis:
    return OnboardingProfileAnalysis.model_validate_json(
        json.dumps(_analysis_payload(), ensure_ascii=False),
        strict=True,
    )


def _analysis_payload() -> dict:
    return {
        "schema_version": ONBOARDING_PROFILE_ANALYSIS_SCHEMA_VERSION,
        "roles": [
            {
                "value": "продуктовый дизайнер",
                "evidence": "продуктовый дизайнер",
                "origin": "explicit",
            }
        ],
        "skills": [
            {
                "value": "Figma",
                "evidence": "Figma",
                "origin": "explicit",
            }
        ],
        "categories": [
            {
                "value": "SaaS",
                "evidence": "SaaS-сервисы",
                "origin": "normalized_inference",
            },
            {
                "value": "лендинги",
                "evidence": "лендинги",
                "origin": "explicit",
            },
        ],
        "uncertain_terms": [],
        "missing_fields": [],
    }


def _openai_response(content: str) -> str:
    return json.dumps(
        {
            "model": "resolved-profile-model",
            "choices": [{"message": {"content": content}}],
            "usage": {
                "prompt_tokens": 40,
                "completion_tokens": 20,
                "total_tokens": 60,
            },
        }
    )


if __name__ == "__main__":
    unittest.main()
