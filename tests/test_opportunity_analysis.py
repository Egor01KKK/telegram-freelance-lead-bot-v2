from __future__ import annotations

import json
import unittest
import urllib.error
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from unittest.mock import patch
from uuid import UUID

from pydantic import ValidationError

from freelancer_bot.ai_telemetry import AIBudgetExceeded
from freelancer_bot.config import RuntimeConfig, RuntimeMode
from freelancer_bot.message_prefilter import AnalyzerMessage, MinimalAnalyzerInput
from freelancer_bot.opportunity_analysis import (
    DEEPSEEK_CHAT_COMPLETIONS_URL,
    OPPORTUNITY_ANALYSIS_PROMPT_VERSION,
    OPPORTUNITY_ANALYSIS_SCHEMA_VERSION,
    OPPORTUNITY_ANALYZER_VERSION,
    OpenAICompatibleOpportunityAnalyzer,
    OpenAIOpportunityAnalyzer,
    OpportunityAnalysis,
    OpportunityAnalysisCall,
    OpportunityAnalysisError,
    OpportunityAnalysisOutputError,
    OpportunityAnalysisUsage,
    OpportunityAnalyzer,
    RoutedOpportunityAnalyzer,
    opportunity_analysis_cache_version,
    opportunity_analysis_provider_available,
)

NOW = datetime(2026, 8, 9, 20, 0, tzinfo=timezone.utc)
CONTRACT_PATH = (
    Path(__file__).parents[1]
    / "docs"
    / "contracts"
    / "opportunity-analysis.schema.json"
)


class FakeOpportunityAnalyzer:
    provider = "fixture"
    model = "fixture-low-cost-model"
    analyzer_version = OPPORTUNITY_ANALYZER_VERSION
    prompt_version = OPPORTUNITY_ANALYSIS_PROMPT_VERSION
    schema_version = OPPORTUNITY_ANALYSIS_SCHEMA_VERSION

    def __init__(self, analysis: OpportunityAnalysis) -> None:
        self.analysis = analysis
        self.calls: list[MinimalAnalyzerInput] = []

    async def analyze(self, candidate: MinimalAnalyzerInput) -> OpportunityAnalysisCall:
        self.calls.append(candidate)
        return OpportunityAnalysisCall(
            analysis=self.analysis,
            provider=self.provider,
            requested_model=self.model,
            response_model=self.model,
            analyzer_version=self.analyzer_version,
            prompt_version=self.prompt_version,
            schema_version=self.analysis.schema_version,
            attempt_count=1,
            usage=OpportunityAnalysisUsage(
                input_tokens=0,
                output_tokens=0,
                total_tokens=0,
            ),
        )


class OpportunityAnalysisContractTest(unittest.IsolatedAsyncioTestCase):
    async def test_domain_can_use_fake_provider_neutral_analyzer(self):
        analysis = _analysis()
        analyzer = FakeOpportunityAnalyzer(analysis)
        candidate = _candidate()

        result = await _run_domain_analysis(analyzer, candidate)

        self.assertIsInstance(analyzer, OpportunityAnalyzer)
        self.assertEqual(result.analysis, analysis)
        self.assertEqual(analyzer.calls, [candidate])

    def test_typed_contract_matches_canonical_versioned_schema(self):
        canonical = json.loads(CONTRACT_PATH.read_text(encoding="utf-8"))
        generated = OpportunityAnalysis.model_json_schema()

        self.assertEqual(
            OpportunityAnalysis.model_validate_json(
                json.dumps(_analysis_payload()),
                strict=True,
            ).schema_version,
            OPPORTUNITY_ANALYSIS_SCHEMA_VERSION,
        )
        self.assertFalse(generated["additionalProperties"])
        self.assertEqual(set(generated["required"]), set(canonical["required"]))
        self.assertEqual(set(generated["properties"]), set(canonical["properties"]))
        self.assertEqual(
            generated["properties"]["schema_version"]["const"],
            canonical["properties"]["schema_version"]["const"],
        )

    def test_typed_contract_rejects_extra_coerced_and_out_of_range_output(self):
        invalid_cases = (
            {**_analysis_payload(), "unexpected": True},
            {**_analysis_payload(), "is_opportunity": "true"},
            {**_analysis_payload(), "confidence": 1.1},
            {**_analysis_payload(), "schema_version": "opportunity_analysis.v2"},
        )

        for payload in invalid_cases:
            with self.subTest(payload=payload):
                with self.assertRaises(ValidationError):
                    OpportunityAnalysis.model_validate_json(
                        json.dumps(payload),
                        strict=True,
                    )

    def test_classification_consistency_blocks_seller_false_positive(self):
        seller = {
            **_analysis_payload(),
            "is_opportunity": False,
            "market_direction": "specialist_to_buyer",
            "intent_stage": "none",
            "opportunity_type": "unknown",
        }
        parsed = OpportunityAnalysis.model_validate_json(
            json.dumps(seller),
            strict=True,
        )
        self.assertFalse(parsed.is_opportunity)

        invalid = (
            {**seller, "is_opportunity": True},
            {**seller, "intent_stage": "active"},
            {
                **_analysis_payload(),
                "is_opportunity": True,
                "market_direction": "unknown",
            },
        )
        for payload in invalid:
            with self.subTest(payload=payload):
                with self.assertRaises(ValidationError):
                    OpportunityAnalysis.model_validate_json(
                        json.dumps(payload),
                        strict=True,
                    )

    def test_cache_identity_separates_model_and_prompt_versions(self):
        analysis = _analysis()
        default = FakeOpportunityAnalyzer(analysis)
        different_model = FakeOpportunityAnalyzer(analysis)
        different_model.model = "fixture-other-model"
        different_prompt = FakeOpportunityAnalyzer(analysis)
        different_prompt.prompt_version = "opportunity-analysis-prompt.v3"

        versions = {
            opportunity_analysis_cache_version(default),
            opportunity_analysis_cache_version(different_model),
            opportunity_analysis_cache_version(different_prompt),
        }

        self.assertEqual(len(versions), 3)
        self.assertTrue(
            all(
                version.startswith(f"{OPPORTUNITY_ANALYZER_VERSION}.")
                for version in versions
            )
        )

    async def test_confidence_router_uses_primary_for_high_and_fallback_for_low(self):
        high_primary = FakeOpportunityAnalyzer(_analysis_with_confidence(0.81))
        unused_fallback = FakeOpportunityAnalyzer(_analysis_with_confidence(0.99))
        high_router = RoutedOpportunityAnalyzer(
            high_primary,
            unused_fallback,
            confidence_threshold=0.65,
        )

        high = await high_router.analyze(_candidate())

        self.assertEqual(high.route_reason, "primary_confident")
        self.assertEqual(len(high_primary.calls), 1)
        self.assertEqual(unused_fallback.calls, [])

        low_primary = FakeOpportunityAnalyzer(_analysis_with_confidence(0.40))
        selected_fallback = FakeOpportunityAnalyzer(_analysis_with_confidence(0.93))
        selected_fallback.model = "fixture-stronger-model"
        low_router = RoutedOpportunityAnalyzer(
            low_primary,
            selected_fallback,
            confidence_threshold=0.65,
        )

        low = await low_router.analyze(_candidate())

        self.assertEqual(low.requested_model, "fixture-stronger-model")
        self.assertEqual(low.route_reason, "low_confidence_fallback")
        self.assertEqual(len(low_primary.calls), 1)
        self.assertEqual(len(selected_fallback.calls), 1)

    async def test_budget_exhaustion_suspends_optional_fallback_without_losing_primary(self):
        primary = FakeOpportunityAnalyzer(_analysis_with_confidence(0.40))

        class BudgetLimitedFallback(FakeOpportunityAnalyzer):
            async def analyze(self, candidate):
                raise AIBudgetExceeded(
                    window="daily",
                    limit_usd=Decimal("0.01"),
                    used_usd=Decimal("0.01"),
                    requested_usd=Decimal("0.001"),
                )

        fallback = BudgetLimitedFallback(_analysis_with_confidence(0.93))
        result = await RoutedOpportunityAnalyzer(
            primary,
            fallback,
            confidence_threshold=0.65,
        ).analyze(_candidate())

        self.assertEqual(result.route_reason, "fallback_budget_exhausted")
        self.assertEqual(result.requested_model, primary.model)
        self.assertEqual(len(primary.calls), 1)
        self.assertEqual(len(fallback.calls), 0)

    def test_routing_configuration_is_part_of_cache_identity(self):
        primary = FakeOpportunityAnalyzer(_analysis())
        fallback = FakeOpportunityAnalyzer(_analysis())
        fallback.model = "fixture-stronger-model"
        versions = {
            opportunity_analysis_cache_version(
                RoutedOpportunityAnalyzer(primary, fallback, confidence_threshold=value)
            )
            for value in (0.60, 0.70)
        }
        fallback.model = "fixture-other-stronger-model"
        versions.add(
            opportunity_analysis_cache_version(
                RoutedOpportunityAnalyzer(primary, fallback, confidence_threshold=0.60)
            )
        )

        self.assertEqual(len(versions), 3)

    def test_budget_distinguishes_unknown_zero_and_negotiable(self):
        unknown = OpportunityAnalysis.model_validate_json(
            json.dumps(_analysis_payload()),
            strict=True,
        )
        zero_payload = _analysis_payload()
        zero_payload["budget"] = {
            "known": True,
            "min": 0,
            "max": 0,
            "currency": "USD",
            "period": "project",
            "explicit": True,
        }
        zero = OpportunityAnalysis.model_validate_json(
            json.dumps(zero_payload),
            strict=True,
        )
        negotiable_payload = _analysis_payload()
        negotiable_payload["budget"] = {
            "known": False,
            "min": None,
            "max": None,
            "currency": None,
            "period": None,
            "explicit": True,
        }
        negotiable = OpportunityAnalysis.model_validate_json(
            json.dumps(negotiable_payload),
            strict=True,
        )

        self.assertFalse(unknown.budget.known)
        self.assertFalse(unknown.budget.explicit)
        self.assertTrue(zero.budget.known)
        self.assertEqual((zero.budget.min, zero.budget.max), (0, 0))
        self.assertFalse(negotiable.budget.known)
        self.assertTrue(negotiable.budget.explicit)

    def test_budget_rejects_inconsistent_or_invented_metadata(self):
        invalid_budgets = (
            {
                "known": True,
                "min": None,
                "max": None,
                "currency": "RUB",
                "period": "month",
                "explicit": True,
            },
            {
                "known": True,
                "min": 120_000,
                "max": 80_000,
                "currency": "RUB",
                "period": "project",
                "explicit": True,
            },
            {
                "known": False,
                "min": 0,
                "max": 0,
                "currency": "USD",
                "period": "project",
                "explicit": False,
            },
            {
                "known": True,
                "min": -1,
                "max": None,
                "currency": "EUR",
                "period": "hour",
                "explicit": True,
            },
        )
        for budget in invalid_budgets:
            with self.subTest(budget=budget):
                payload = _analysis_payload()
                payload["budget"] = budget
                with self.assertRaises(ValidationError):
                    OpportunityAnalysis.model_validate_json(
                        json.dumps(payload),
                        strict=True,
                    )

    def test_quality_and_red_flags_are_profile_independent_fields(self):
        payload = _analysis_payload()
        payload["quality"] = {
            "actionability": 0.8,
            "commercial_plausibility": 0.3,
            "specificity": 0.7,
            "credibility": 0.2,
        }
        payload["red_flags"] = ["advance_payment_request", "unverified_identity"]

        analysis = OpportunityAnalysis.model_validate_json(
            json.dumps(payload),
            strict=True,
        )

        self.assertEqual(analysis.quality.credibility, 0.2)
        self.assertEqual(
            analysis.red_flags,
            ("advance_payment_request", "unverified_identity"),
        )
        self.assertNotIn("relevance", OpportunityAnalysis.model_fields)


class OpenAIOpportunityAnalyzerTest(unittest.IsolatedAsyncioTestCase):
    def test_model_capability_omits_temperature_only_for_gpt5_family(self):
        gpt5 = OpenAIOpportunityAnalyzer(
            api_key="test-secret",
            model="gpt-5-nano",
            temperature=0.0,
        )
        compatible = OpenAIOpportunityAnalyzer(
            api_key="test-secret",
            model="configured-mass-model",
            temperature=0.2,
        )

        self.assertNotIn("temperature", gpt5._payload(_candidate()))
        self.assertEqual(compatible._payload(_candidate())["temperature"], 0.2)

    async def test_openai_transport_uses_openai_endpoint_key_and_strict_schema(self):
        captured = []
        analyzer = OpenAIOpportunityAnalyzer(
            api_key="openai-test-secret",
            model="gpt-5-nano",
            max_output_attempts=1,
        )

        def fake_urlopen(request, timeout):
            captured.append(request)
            return _Response(
                _openai_response(_analysis_payload(), response_model="gpt-5-nano")
            )

        with patch(
            "freelancer_bot.opportunity_analysis.urllib.request.urlopen",
            fake_urlopen,
        ):
            await analyzer.analyze(_candidate())

        request = captured[0]
        payload = json.loads(request.data)
        self.assertEqual(request.full_url, "https://api.openai.com/v1/chat/completions")
        self.assertEqual(request.headers["Authorization"], "Bearer openai-test-secret")
        self.assertEqual(payload["response_format"]["type"], "json_schema")
        self.assertTrue(payload["response_format"]["json_schema"]["strict"])

    def test_missing_credentials_are_not_replaced_with_fake_provider_success(self):
        with patch.dict("os.environ", {}, clear=True):
            config = RuntimeConfig.from_env(
                mode=RuntimeMode.CHECK_CONFIG,
                env_file=None,
            )
        with self.assertRaisesRegex(OpportunityAnalysisError, "API_KEY"):
            OpenAIOpportunityAnalyzer.from_config(config)

    async def test_provider_outage_and_rate_limit_fail_without_unbounded_requests(self):
        for provider_error in (
            urllib.error.URLError("provider unavailable"),
            urllib.error.HTTPError(
                "https://api.example.test",
                429,
                "rate limited",
                {},
                None,
            ),
        ):
            with self.subTest(error=type(provider_error).__name__):
                analyzer = OpenAIOpportunityAnalyzer(
                    api_key="test-secret",
                    model="fixture-model",
                    max_output_attempts=1,
                )
                with patch(
                    "freelancer_bot.opportunity_analysis.urllib.request.urlopen",
                    side_effect=provider_error,
                ):
                    with self.assertRaisesRegex(
                        OpportunityAnalysisError,
                        "request failed",
                    ):
                        await analyzer.analyze(_candidate())

    async def test_http_400_is_non_retryable_and_429_remains_retryable(self):
        for status, retryable in ((400, False), (429, True), (503, True)):
            with self.subTest(status=status):
                analyzer = OpenAIOpportunityAnalyzer(
                    api_key="test-secret",
                    model="gpt-5-nano",
                    max_output_attempts=1,
                )
                error = urllib.error.HTTPError(
                    "https://api.example.test",
                    status,
                    "provider error",
                    {},
                    None,
                )
                with patch(
                    "freelancer_bot.opportunity_analysis.urllib.request.urlopen",
                    side_effect=error,
                ):
                    with self.assertRaises(OpportunityAnalysisError) as raised:
                        await analyzer.analyze(_candidate())
                self.assertEqual(raised.exception.retryable, retryable)
                self.assertEqual(
                    raised.exception.error_code,
                    "provider_request_failed"
                    if retryable
                    else "provider_invalid_request",
                )

    async def test_configured_model_strict_schema_retry_and_telemetry(self):
        candidate = _candidate(with_parent=True)
        invalid = {**_analysis_payload(), "unexpected": "invalid"}
        responses = iter(
            (
                _openai_response(invalid, response_model="actual-model-revision"),
                _openai_response(
                    _analysis_payload(),
                    response_model="actual-model-revision",
                ),
            )
        )
        captured: list[tuple[dict, int]] = []

        def fake_urlopen(request, timeout):
            captured.append((json.loads(request.data), timeout))
            return _Response(next(responses))

        with patch.dict(
            "os.environ",
            {
                "OPENAI_API_KEY": "test-secret",
                "OPPORTUNITY_ANALYSIS_PROVIDER": "openai",
                "OPPORTUNITY_ANALYSIS_MODEL": "configured-mass-model",
                "OPPORTUNITY_ANALYSIS_TEMPERATURE": "0",
                "OPPORTUNITY_ANALYSIS_TIMEOUT_SECONDS": "12",
                "OPPORTUNITY_ANALYSIS_MAX_OUTPUT_ATTEMPTS": "2",
            },
            clear=True,
        ):
            config = RuntimeConfig.from_env(
                mode=RuntimeMode.CHECK_CONFIG,
                env_file=None,
            )
        analyzer = OpenAIOpportunityAnalyzer.from_config(config)
        with patch(
            "freelancer_bot.opportunity_analysis.urllib.request.urlopen",
            fake_urlopen,
        ):
            call = await analyzer.analyze(candidate)

        self.assertIsInstance(analyzer, OpportunityAnalyzer)
        self.assertEqual(len(captured), 2)
        self.assertEqual(call.attempt_count, 2)
        self.assertEqual(call.requested_model, "configured-mass-model")
        self.assertEqual(call.response_model, "actual-model-revision")
        self.assertEqual(call.schema_version, OPPORTUNITY_ANALYSIS_SCHEMA_VERSION)
        self.assertEqual(call.usage.input_tokens, 17)
        self.assertEqual(call.usage.output_tokens, 31)
        self.assertEqual(call.usage.total_tokens, 48)
        self.assertEqual(
            call.analysis.contact.model_dump(),
            {"telegram": None, "email": None, "url": None},
        )

        payload, timeout = captured[0]
        self.assertEqual(payload["model"], "configured-mass-model")
        self.assertEqual(timeout, 12)
        response_format = payload["response_format"]["json_schema"]
        self.assertTrue(response_format["strict"])
        self.assertEqual(
            response_format["schema"]["properties"]["schema_version"]["const"],
            OPPORTUNITY_ANALYSIS_SCHEMA_VERSION,
        )
        user_input = json.loads(payload["messages"][1]["content"])
        self.assertEqual(set(user_input), {"current", "parent"})
        self.assertEqual(user_input["current"]["content"], "Нужен Telegram-бот")
        self.assertEqual(user_input["parent"]["content"], "Какая задача?")
        rubric = payload["messages"][0]["content"]
        for label in (
            "buyer_to_specialist",
            "specialist_to_buyer",
            "active",
            "recommendation",
            "research",
            "weak",
            "none",
            "one_off_order",
            "project",
            "vacancy",
            "part_time_contractor",
            "consultation",
        ):
            with self.subTest(label=label):
                self.assertIn(label, rubric)

        for extraction_rule in (
            "free-text role",
            "руб/₽",
            "known=false",
            "Copy Telegram handles",
            "personal relevance",
            "direct parent",
        ):
            with self.subTest(extraction_rule=extraction_rule):
                self.assertIn(extraction_rule, rubric)

        self.assertEqual(analyzer.provider, "openai")

    async def test_deepseek_uses_json_object_and_records_real_provider(self):
        payload = _analysis_payload()
        payload["contact"] = {"telegram": "@foo", "email": None, "url": None}
        recorder = _RecordingRecorder()
        captured = []
        analyzer = OpenAICompatibleOpportunityAnalyzer(
            api_key="deepseek-test-secret",
            model="deepseek-v4-flash",
            provider="deepseek",
            base_url=DEEPSEEK_CHAT_COMPLETIONS_URL,
            max_output_attempts=1,
            recorder=recorder,
        )

        def fake_urlopen(request, timeout):
            captured.append((request, json.loads(request.data), timeout))
            return _Response(_openai_response(payload, response_model="deepseek-v4"))

        with patch(
            "freelancer_bot.opportunity_analysis.urllib.request.urlopen",
            fake_urlopen,
        ):
            call = await analyzer.analyze(
                _candidate(current_content="Пишите https://t.me/Foo")
            )

        self.assertEqual(call.provider, "deepseek")
        self.assertEqual(recorder.starts[0].provider, "deepseek")
        self.assertEqual(recorder.finishes[0][1].status, "succeeded")
        request, request_payload, timeout = captured[0]
        self.assertEqual(request.full_url, DEEPSEEK_CHAT_COMPLETIONS_URL)
        self.assertEqual(request.headers["Authorization"], "Bearer deepseek-test-secret")
        self.assertEqual(timeout, 45)
        self.assertEqual(request_payload["response_format"], {"type": "json_object"})
        system_contract = request_payload["messages"][0]["content"]
        self.assertIn('"schema_version"', system_contract)
        self.assertIn('"additionalProperties": false', system_contract)

    async def test_compatible_provider_errors_are_provider_aware(self):
        analyzer = OpenAICompatibleOpportunityAnalyzer(
            api_key="deepseek-test-secret",
            model="deepseek-v4-flash",
            provider="deepseek",
            max_output_attempts=1,
        )
        with patch(
            "freelancer_bot.opportunity_analysis.urllib.request.urlopen",
            return_value=_Response(
                _openai_response({"invalid": True}, response_model="deepseek-v4")
            ),
        ), self.assertRaises(OpportunityAnalysisOutputError) as raised:
            await analyzer.analyze(_candidate())

        self.assertIn("deepseek returned invalid opportunity-analysis output", str(raised.exception))
        self.assertNotIn("OpenAI returned", str(raised.exception))

    async def test_tokenrouter_config_normalizes_both_base_url_shapes(self):
        for base_url in (
            "https://router.example/v1",
            "https://router.example/v1/chat/completions",
        ):
            with self.subTest(base_url=base_url), patch.dict(
                "os.environ",
                {
                    "TOKENROUTER_API_KEY": "tokenrouter-test-secret",
                    "TOKENROUTER_BASE_URL": base_url,
                    "OPPORTUNITY_ANALYSIS_PROVIDER": "tokenrouter",
                    "OPPORTUNITY_ANALYSIS_MODEL": "deepseek/deepseek-v4-pro-0813-free",
                },
                clear=True,
            ):
                config = RuntimeConfig.from_env(
                    mode=RuntimeMode.CHECK_CONFIG,
                    env_file=None,
                )
                analyzer = OpenAICompatibleOpportunityAnalyzer.from_config(config)

                def fake_urlopen(request, timeout):
                    self.assertEqual(
                        request.full_url,
                        "https://router.example/v1/chat/completions",
                    )
                    self.assertEqual(
                        request.headers["Authorization"],
                        "Bearer tokenrouter-test-secret",
                    )
                    return _Response(
                        _openai_response(
                            _analysis_payload(),
                            response_model="tokenrouter-v4",
                        )
                    )

                with patch(
                    "freelancer_bot.opportunity_analysis.urllib.request.urlopen",
                    fake_urlopen,
                ):
                    call = await analyzer.analyze(_candidate())

            self.assertEqual(call.provider, "tokenrouter")

    async def test_contact_grounding_accepts_safe_representational_forms_once(self):
        payload = _analysis_payload()
        payload["contact"] = {
            "telegram": "@foo",
            "email": "name@example.com",
            "url": "https://EXAMPLE.com/",
        }
        requests = 0

        def fake_urlopen(request, timeout):
            nonlocal requests
            requests += 1
            return _Response(_openai_response(payload, response_model="fixture-model"))

        analyzer = OpenAIOpportunityAnalyzer(
            api_key="test-secret",
            model="configured-mass-model",
            max_output_attempts=2,
        )
        with patch(
            "freelancer_bot.opportunity_analysis.urllib.request.urlopen",
            fake_urlopen,
        ):
            call = await analyzer.analyze(
                _candidate(
                    current_content=(
                        "Пишите https://t.me/Foo. Contact: Name@Example.COM "
                        "https://example.com"
                    )
                )
            )

        self.assertEqual(call.attempt_count, 1)
        self.assertEqual(requests, 1)

    async def test_cross_provider_fallback_preserves_provider_identity(self):
        primary_payload = _analysis_payload()
        primary_payload["confidence"] = 0.40
        fallback_payload = _analysis_payload()
        fallback_payload["confidence"] = 0.92
        responses = iter(
            (
                _openai_response(primary_payload, response_model="deepseek-result"),
                _openai_response(fallback_payload, response_model="openai-result"),
            )
        )
        requests = []

        def fake_urlopen(request, timeout):
            requests.append(request.full_url)
            return _Response(next(responses))

        primary = OpenAICompatibleOpportunityAnalyzer(
            api_key="deepseek-test-secret",
            model="deepseek-v4-flash",
            provider="deepseek",
            base_url=DEEPSEEK_CHAT_COMPLETIONS_URL,
            max_output_attempts=1,
        )
        fallback = OpenAICompatibleOpportunityAnalyzer(
            api_key="openai-test-secret",
            model="gpt-5-mini",
            provider="openai",
            base_url="https://api.openai.example/v1/chat/completions",
            max_output_attempts=1,
        )
        with patch(
            "freelancer_bot.opportunity_analysis.urllib.request.urlopen",
            fake_urlopen,
        ):
            result = await RoutedOpportunityAnalyzer(
                primary,
                fallback,
                confidence_threshold=0.65,
            ).analyze(_candidate())

        self.assertEqual(result.provider, "openai")
        self.assertEqual(result.route_reason, "low_confidence_fallback")
        self.assertEqual(
            requests,
            [
                DEEPSEEK_CHAT_COMPLETIONS_URL,
                "https://api.openai.example/v1/chat/completions",
            ],
        )

    async def test_extracts_common_ru_and_en_budgets_and_free_text_roles(self):
        cases = (
            (
                "Ищем архитектора n8n. Бюджет 80–120 тыс ₽ за проект. "
                "Пишите @client_ru, ТЗ https://example.test/brief",
                _extracted_payload(
                    category="business_process_automation",
                    role_title="n8n automation architect",
                    skills=["n8n", "Python"],
                    budget={
                        "known": True,
                        "min": 80_000,
                        "max": 120_000,
                        "currency": "RUB",
                        "period": "project",
                        "explicit": True,
                    },
                    contact={
                        "telegram": "@client_ru",
                        "email": None,
                        "url": "https://example.test/brief",
                    },
                ),
                (80_000, 120_000, "RUB", "project"),
            ),
            (
                "Need a computational linguist for evaluation, $75/hour. "
                "Email jobs@example.test or see https://example.test/job",
                _extracted_payload(
                    category="language_technology",
                    role_title="computational linguist",
                    skills=["linguistic evaluation"],
                    budget={
                        "known": True,
                        "min": 75,
                        "max": 75,
                        "currency": "USD",
                        "period": "hour",
                        "explicit": True,
                    },
                    contact={
                        "telegram": None,
                        "email": "jobs@example.test",
                        "url": "https://example.test/job",
                    },
                ),
                (75, 75, "USD", "hour"),
            ),
        )

        for content, output, expected_budget in cases:
            with self.subTest(content=content):
                response = _openai_response(
                    output,
                    response_model="fixture-extraction-model",
                )
                analyzer = OpenAIOpportunityAnalyzer(
                    api_key="test-secret",
                    model="configured-mass-model",
                    max_output_attempts=1,
                )
                with patch(
                    "freelancer_bot.opportunity_analysis.urllib.request.urlopen",
                    return_value=_Response(response),
                ):
                    call = await analyzer.analyze(
                        _candidate(current_content=content)
                    )

                budget = call.analysis.budget
                self.assertEqual(
                    (budget.min, budget.max, budget.currency, budget.period),
                    expected_budget,
                )
                self.assertEqual(call.analysis.category, output["category"])
                self.assertEqual(call.analysis.role_title, output["role_title"])
                self.assertEqual(call.analysis.skills, tuple(output["skills"]))
                self.assertEqual(
                    call.analysis.contact.model_dump(),
                    output["contact"],
                )

    async def test_contacts_must_be_exactly_grounded_or_null(self):
        output = _analysis_payload()
        output["contact"] = {
            "telegram": "@invented_contact",
            "email": None,
            "url": None,
        }
        response = _openai_response(output, response_model="fixture-model")
        analyzer = OpenAIOpportunityAnalyzer(
            api_key="test-secret",
            model="configured-mass-model",
            max_output_attempts=1,
        )

        with patch(
            "freelancer_bot.opportunity_analysis.urllib.request.urlopen",
            return_value=_Response(response),
        ):
            with self.assertRaises(OpportunityAnalysisOutputError) as raised:
                await analyzer.analyze(_candidate())

        self.assertIn("after 1 attempts", str(raised.exception))
        self.assertNotIn("@invented_contact", str(raised.exception))

    async def test_permanently_invalid_output_is_bounded_and_not_disclosed(self):
        canary = "raw-model-output-must-not-leak"
        response = _openai_response(
            {"invalid": canary},
            response_model="actual-model-revision",
        )
        requests = 0

        def fake_urlopen(request, timeout):
            nonlocal requests
            requests += 1
            return _Response(response)

        analyzer = OpenAIOpportunityAnalyzer(
            api_key="test-secret",
            model="configured-mass-model",
            max_output_attempts=2,
        )
        with patch(
            "freelancer_bot.opportunity_analysis.urllib.request.urlopen",
            fake_urlopen,
        ):
            with self.assertRaises(OpportunityAnalysisOutputError) as raised:
                await analyzer.analyze(_candidate())

        self.assertEqual(requests, 2)
        self.assertIn("after 2 attempts", str(raised.exception))
        self.assertNotIn(canary, str(raised.exception))

    def test_runtime_configuration_keeps_provider_and_model_replaceable(self):
        with patch.dict("os.environ", {}, clear=True):
            defaults = RuntimeConfig.from_env(
                mode=RuntimeMode.CHECK_CONFIG,
                env_file=None,
            )
        self.assertEqual(defaults.opportunity_analysis_provider, "openai")
        self.assertEqual(defaults.opportunity_analysis_model, "gpt-5-nano")
        self.assertFalse(defaults.opportunity_analysis_fallback_enabled)
        self.assertEqual(
            defaults.opportunity_analysis_fallback_model,
            "gpt-5-mini",
        )
        self.assertEqual(defaults.opportunity_analysis_confidence_threshold, 0.65)

        with patch.dict(
            "os.environ",
            {
                "OPPORTUNITY_ANALYSIS_PROVIDER": "fixture-ai",
                "OPPORTUNITY_ANALYSIS_MODEL": "replaceable-cheap-model",
                "OPPORTUNITY_ANALYSIS_TEMPERATURE": "0.2",
                "OPPORTUNITY_ANALYSIS_TIMEOUT_SECONDS": "19",
                "OPPORTUNITY_ANALYSIS_MAX_OUTPUT_ATTEMPTS": "3",
                "OPPORTUNITY_ANALYSIS_FALLBACK_PROVIDER": "fixture-ai",
                "OPPORTUNITY_ANALYSIS_FALLBACK_MODEL": "replaceable-strong-model",
                "OPPORTUNITY_ANALYSIS_CONFIDENCE_THRESHOLD": "0.72",
                "OPPORTUNITY_ANALYSIS_ROUTING_VERSION": "fixture-routing.v2",
            },
            clear=True,
        ):
            configured = RuntimeConfig.from_env(
                mode=RuntimeMode.CHECK_CONFIG,
                env_file=None,
            )
        self.assertEqual(configured.opportunity_analysis_provider, "fixture-ai")
        self.assertEqual(
            configured.opportunity_analysis_model,
            "replaceable-cheap-model",
        )
        self.assertEqual(configured.opportunity_analysis_temperature, 0.2)
        self.assertEqual(configured.opportunity_analysis_timeout_seconds, 19)
        self.assertEqual(configured.opportunity_analysis_max_output_attempts, 3)
        self.assertEqual(
            configured.opportunity_analysis_fallback_provider,
            "fixture-ai",
        )
        self.assertEqual(
            configured.opportunity_analysis_fallback_model,
            "replaceable-strong-model",
        )
        self.assertEqual(configured.opportunity_analysis_confidence_threshold, 0.72)
        self.assertEqual(
            configured.opportunity_analysis_routing_version,
            "fixture-routing.v2",
        )

    def test_provider_availability_follows_selected_primary_and_fallback_keys(self):
        with patch.dict(
            "os.environ",
            {
                "OPENAI_API_KEY": "openai-test-secret",
                "OPPORTUNITY_ANALYSIS_PROVIDER": "deepseek",
                "DEEPSEEK_API_KEY": "deepseek-test-secret",
                "OPPORTUNITY_ANALYSIS_FALLBACK_ENABLED": "true",
                "OPPORTUNITY_ANALYSIS_FALLBACK_PROVIDER": "tokenrouter",
                "TOKENROUTER_API_KEY": "tokenrouter-test-secret",
            },
            clear=True,
        ):
            config = RuntimeConfig.from_env(
                mode=RuntimeMode.CHECK_CONFIG,
                env_file=None,
            )
        self.assertTrue(opportunity_analysis_provider_available(config))
        self.assertTrue(
            opportunity_analysis_provider_available(config, fallback=True)
        )

        with patch.dict(
            "os.environ",
            {
                "OPENAI_API_KEY": "openai-test-secret",
                "OPPORTUNITY_ANALYSIS_PROVIDER": "deepseek",
            },
            clear=True,
        ):
            unavailable = RuntimeConfig.from_env(
                mode=RuntimeMode.CHECK_CONFIG,
                env_file=None,
            )
        self.assertFalse(opportunity_analysis_provider_available(unavailable))


async def _run_domain_analysis(
    analyzer: OpportunityAnalyzer,
    candidate: MinimalAnalyzerInput,
) -> OpportunityAnalysisCall:
    return await analyzer.analyze(candidate)


class _Response:
    def __init__(self, payload: str) -> None:
        self.payload = payload

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return None

    def read(self) -> bytes:
        return self.payload.encode("utf-8")


class _RecordingRecorder:
    def __init__(self) -> None:
        self.starts = []
        self.finishes = []

    async def begin(self, call):
        self.starts.append(call)
        return UUID("99999999-9999-9999-9999-999999999999")

    async def finish(self, call_id, result):
        self.finishes.append((call_id, result))


def _openai_response(payload: dict, *, response_model: str) -> str:
    return json.dumps(
        {
            "model": response_model,
            "usage": {
                "prompt_tokens": 17,
                "completion_tokens": 31,
                "total_tokens": 48,
            },
            "choices": [
                {
                    "message": {
                        "content": json.dumps(payload, ensure_ascii=False),
                    }
                }
            ],
        },
        ensure_ascii=False,
    )


def _candidate(
    *,
    with_parent: bool = False,
    current_content: str = "Нужен Telegram-бот",
) -> MinimalAnalyzerInput:
    return MinimalAnalyzerInput(
        current=_message(101, current_content),
        parent=_message(100, "Какая задача?") if with_parent else None,
    )


def _message(message_id: int, content: str) -> AnalyzerMessage:
    return AnalyzerMessage(
        raw_message_id=UUID(f"00000000-0000-0000-0000-{message_id:012d}"),
        source_id=7,
        external_source_id="username:test_source",
        external_message_id=message_id,
        message_date=NOW,
        message_url=f"https://t.me/test_source/{message_id}",
        content=content,
    )


def _analysis() -> OpportunityAnalysis:
    return OpportunityAnalysis.model_validate_json(
        json.dumps(_analysis_payload()),
        strict=True,
    )


def _analysis_with_confidence(confidence: float) -> OpportunityAnalysis:
    payload = _analysis_payload()
    payload["confidence"] = confidence
    return OpportunityAnalysis.model_validate_json(json.dumps(payload), strict=True)


def _analysis_payload() -> dict:
    return {
        "schema_version": OPPORTUNITY_ANALYSIS_SCHEMA_VERSION,
        "is_opportunity": True,
        "confidence": 0.94,
        "market_direction": "buyer_to_specialist",
        "intent_stage": "active",
        "opportunity_type": "project",
        "category": "telegram_development",
        "role_title": "Telegram bot developer",
        "skills": ["Python", "Telegram Bot API"],
        "task_summary": "Build a Telegram bot",
        "budget": {
            "known": False,
            "min": None,
            "max": None,
            "currency": None,
            "period": None,
            "explicit": False,
        },
        "work": {
            "remote": True,
            "location": None,
            "full_time": None,
            "part_time": None,
        },
        "language": "ru",
        "contact": {
            "telegram": None,
            "email": None,
            "url": None,
        },
        "quality": {
            "actionability": 0.9,
            "commercial_plausibility": 0.8,
            "specificity": 0.7,
            "credibility": 0.8,
        },
        "red_flags": [],
    }


def _extracted_payload(
    *,
    category: str,
    role_title: str,
    skills: list[str],
    budget: dict,
    contact: dict,
) -> dict:
    payload = _analysis_payload()
    payload.update(
        category=category,
        role_title=role_title,
        skills=skills,
        budget=budget,
        contact=contact,
    )
    return payload


if __name__ == "__main__":
    unittest.main()
