from __future__ import annotations

import asyncio
from collections.abc import Mapping
from dataclasses import dataclass
from hashlib import sha256
import json
import re
import socket
from typing import Any, Literal, Protocol, runtime_checkable
import urllib.error
import urllib.request

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    ValidationError,
    field_validator,
    model_validator,
)

from .config import RuntimeConfig
from .openai_compat import add_sampling_parameter
from .search_profiles import (
    ParsedSearchProfile,
    SearchProfileTermInput,
    SearchProfileTermOrigin,
    normalize_semantic_text,
    parse_search_profile,
)


ONBOARDING_PROFILE_ANALYSIS_SCHEMA_VERSION = "onboarding_profile_analysis.v1"
ONBOARDING_PROFILE_ANALYZER_VERSION = "onboarding-profile-analyzer.v1"
ONBOARDING_PROFILE_PROMPT_VERSION = "onboarding-profile-prompt.v1"
DEFAULT_ONBOARDING_PROFILE_MODEL = "gpt-5-nano"
OPENAI_CHAT_COMPLETIONS_URL = "https://api.openai.com/v1/chat/completions"
DEEPSEEK_API_BASE_URL = "https://api.deepseek.com"
DEEPSEEK_CHAT_COMPLETIONS_URL = f"{DEEPSEEK_API_BASE_URL}/chat/completions"
_SAFE_VERSION = re.compile(r"^[a-z0-9][a-z0-9._-]{0,99}$")
_PROFILE_FIELDS = frozenset({"roles", "skills", "categories"})


def _chat_completions_url(base_url: str) -> str:
    normalized = base_url.strip().rstrip("/")
    if normalized.endswith("/chat/completions"):
        return normalized
    return f"{normalized}/chat/completions"


@dataclass(frozen=True)
class OnboardingProfileProviderMetrics:
    """Bounded, stage-specific metrics for one onboarding request."""

    provider_attempts: int = 0
    completed_calls: int = 0
    timeouts: int = 0
    transient_failures: int = 0
    non_retryable_failures: int = 0
    invalid_output_retries: int = 0

    def __post_init__(self) -> None:
        for name in (
            "provider_attempts",
            "completed_calls",
            "timeouts",
            "transient_failures",
            "non_retryable_failures",
            "invalid_output_retries",
        ):
            value = getattr(self, name)
            if not isinstance(value, int) or value < 0:
                raise ValueError(f"{name} must be a nonnegative integer")


def _metrics_add(
    metrics: OnboardingProfileProviderMetrics,
    **increments: int,
) -> OnboardingProfileProviderMetrics:
    values = {
        name: getattr(metrics, name) + int(increments.get(name, 0))
        for name in (
            "provider_attempts",
            "completed_calls",
            "timeouts",
            "transient_failures",
            "non_retryable_failures",
            "invalid_output_retries",
        )
    }
    return OnboardingProfileProviderMetrics(**values)


class OnboardingProfileError(RuntimeError):
    def __init__(
        self,
        message: str,
        *,
        retryable: bool = False,
        failure_class: str = "provider_error",
        provider_metrics: OnboardingProfileProviderMetrics | None = None,
    ) -> None:
        super().__init__(message)
        self.retryable = retryable
        self.failure_class = failure_class
        self.provider_metrics = provider_metrics or OnboardingProfileProviderMetrics()


class OnboardingProfileOutputError(OnboardingProfileError):
    def __init__(
        self,
        message: str,
        *,
        provider_metrics: OnboardingProfileProviderMetrics | None = None,
    ) -> None:
        super().__init__(
            message,
            retryable=True,
            failure_class="invalid_output",
            provider_metrics=provider_metrics,
        )


class _StrictContract(BaseModel):
    model_config = ConfigDict(
        extra="forbid",
        frozen=True,
        strict=True,
        str_strip_whitespace=True,
    )


class OnboardingProfileTerm(_StrictContract):
    value: str = Field(min_length=1, max_length=200)
    evidence: str = Field(min_length=1, max_length=2000)
    origin: Literal["explicit", "normalized_inference"]


class OnboardingProfileAnalysis(_StrictContract):
    schema_version: Literal["onboarding_profile_analysis.v1"]
    roles: tuple[OnboardingProfileTerm, ...]
    skills: tuple[OnboardingProfileTerm, ...]
    categories: tuple[OnboardingProfileTerm, ...]
    uncertain_terms: tuple[str, ...]
    missing_fields: tuple[Literal["roles", "skills", "categories"], ...]

    @field_validator("roles", "skills", "categories")
    @classmethod
    def validate_terms(
        cls,
        values: tuple[OnboardingProfileTerm, ...],
    ) -> tuple[OnboardingProfileTerm, ...]:
        if len(values) > 64:
            raise ValueError("profile term collections are bounded to 64 values")
        normalized = [term.value.casefold() for term in values]
        if len(normalized) != len(set(normalized)):
            raise ValueError("profile terms must be unique")
        return values

    @field_validator("uncertain_terms")
    @classmethod
    def validate_uncertain_terms(cls, values: tuple[str, ...]) -> tuple[str, ...]:
        if len(values) > 32:
            raise ValueError("uncertain terms are bounded to 32 values")
        normalized: set[str] = set()
        for value in values:
            if not value or len(value) > 2000:
                raise ValueError("uncertain terms must be non-empty and bounded")
            identity = value.casefold()
            if identity in normalized:
                raise ValueError("uncertain terms must be unique")
            normalized.add(identity)
        return values

    @model_validator(mode="after")
    def validate_completeness(self) -> OnboardingProfileAnalysis:
        missing = tuple(self.missing_fields)
        if len(missing) != len(set(missing)):
            raise ValueError("missing_fields must be unique")
        actual_missing = {
            field
            for field in _PROFILE_FIELDS
            if not getattr(self, field)
        }
        if set(missing) != actual_missing:
            raise ValueError("missing_fields must exactly describe empty profile fields")
        if actual_missing == _PROFILE_FIELDS:
            raise ValueError("description contains no usable profile structure")
        return self


class OnboardingProfileUsage(_StrictContract):
    input_tokens: int = Field(ge=0)
    output_tokens: int = Field(ge=0)
    total_tokens: int = Field(ge=0)

    @model_validator(mode="after")
    def validate_total(self) -> OnboardingProfileUsage:
        if self.total_tokens != self.input_tokens + self.output_tokens:
            raise ValueError("total_tokens must equal input_tokens plus output_tokens")
        return self


@dataclass(frozen=True)
class OnboardingProfileAnalysisCall:
    analysis: OnboardingProfileAnalysis
    provider: str
    requested_model: str
    response_model: str
    analyzer_version: str
    prompt_version: str
    schema_version: str
    attempt_count: int
    usage: OnboardingProfileUsage
    provider_metrics: OnboardingProfileProviderMetrics = OnboardingProfileProviderMetrics()


@runtime_checkable
class OnboardingProfileAnalyzer(Protocol):
    @property
    def provider(self) -> str: ...

    @property
    def model(self) -> str: ...

    @property
    def analyzer_version(self) -> str: ...

    @property
    def prompt_version(self) -> str: ...

    @property
    def schema_version(self) -> str: ...

    async def analyze(self, description: str) -> OnboardingProfileAnalysisCall: ...


class OpenAIOnboardingProfileAnalyzer:
    def __init__(
        self,
        *,
        api_key: str,
        provider: str = "openai",
        api_key_name: str = "OPENAI_API_KEY",
        model: str = DEFAULT_ONBOARDING_PROFILE_MODEL,
        temperature: float = 0.0,
        timeout_seconds: int = 45,
        max_output_attempts: int = 2,
        max_transport_attempts: int = 2,
        transport_retry_backoff_seconds: float = 1.0,
        base_url: str = OPENAI_CHAT_COMPLETIONS_URL,
        max_ai_calls_per_run: int | None = None,
        analyzer_version: str = ONBOARDING_PROFILE_ANALYZER_VERSION,
        prompt_version: str = ONBOARDING_PROFILE_PROMPT_VERSION,
    ) -> None:
        self._api_key = api_key.strip()
        self._provider = _safe_version(provider, "provider")
        self._api_key_name = _bounded_text(api_key_name, "api_key_name", 128)
        self._model = _bounded_text(model, "model", 128)
        if not 0 <= temperature <= 2:
            raise ValueError("temperature must be between 0 and 2")
        if timeout_seconds <= 0:
            raise ValueError("timeout_seconds must be positive")
        if not 1 <= max_output_attempts <= 5:
            raise ValueError("max_output_attempts must be between 1 and 5")
        if not 1 <= max_transport_attempts <= 5:
            raise ValueError("max_transport_attempts must be between 1 and 5")
        if transport_retry_backoff_seconds < 0:
            raise ValueError("transport_retry_backoff_seconds must be nonnegative")
        if max_ai_calls_per_run is not None and max_ai_calls_per_run < 1:
            raise ValueError("max_ai_calls_per_run must be positive when configured")
        self._temperature = temperature
        self._timeout_seconds = timeout_seconds
        self._max_output_attempts = max_output_attempts
        self._max_transport_attempts = max_transport_attempts
        self._transport_retry_backoff_seconds = transport_retry_backoff_seconds
        self._base_url = _bounded_text(base_url, "base_url", 2048)
        self._max_ai_calls_per_run = max_ai_calls_per_run
        self._ai_calls_used = 0
        self._analyzer_version = _safe_version(
            analyzer_version,
            "analyzer_version",
        )
        self._prompt_version = _safe_version(prompt_version, "prompt_version")
        self._metrics = OnboardingProfileProviderMetrics()

    @classmethod
    def from_config(cls, config: RuntimeConfig) -> OpenAIOnboardingProfileAnalyzer:
        if config.onboarding_profile_provider != "openai":
            raise OnboardingProfileError(
                "OpenAI adapter requires ONBOARDING_PROFILE_PROVIDER=openai"
            )
        api_key = (
            ""
            if config.openai_api_key is None
            else config.openai_api_key.get_secret_value()
        )
        return cls(
            api_key=api_key,
            provider="openai",
            api_key_name="OPENAI_API_KEY",
            model=config.onboarding_profile_model,
            temperature=config.onboarding_profile_temperature,
            timeout_seconds=config.onboarding_profile_timeout_seconds,
            max_output_attempts=config.onboarding_profile_max_output_attempts,
            max_transport_attempts=config.onboarding_profile_max_transport_attempts,
            transport_retry_backoff_seconds=(
                config.onboarding_profile_transport_retry_backoff_seconds
            ),
            max_ai_calls_per_run=config.max_ai_calls_per_run,
        )

    @property
    def provider(self) -> str:
        return self._provider

    @property
    def model(self) -> str:
        return self._model

    @property
    def analyzer_version(self) -> str:
        return self._analyzer_version

    @property
    def prompt_version(self) -> str:
        return self._prompt_version

    @property
    def schema_version(self) -> str:
        return ONBOARDING_PROFILE_ANALYSIS_SCHEMA_VERSION

    @property
    def metrics(self) -> OnboardingProfileProviderMetrics:
        return self._metrics

    @property
    def base_url(self) -> str:
        return self._base_url

    @property
    def cache_identity(self) -> Mapping[str, Any]:
        return {
            "provider": self.provider,
            "model": self.model,
            "temperature": self._temperature,
            "base_url": self._base_url,
            "analyzer_version": self.analyzer_version,
            "prompt_version": self.prompt_version,
            "schema_version": self.schema_version,
        }

    async def analyze(self, description: str) -> OnboardingProfileAnalysisCall:
        normalize_semantic_text(description)
        payload = self._payload(description)
        metrics = OnboardingProfileProviderMetrics()
        for attempt in range(1, self._max_output_attempts + 1):
            for transport_attempt in range(1, self._max_transport_attempts + 1):
                if (
                    self._max_ai_calls_per_run is not None
                    and self._ai_calls_used >= self._max_ai_calls_per_run
                ):
                    self._record_metrics(metrics)
                    raise OnboardingProfileError(
                        f"{self.provider} onboarding-profile call budget exhausted",
                        failure_class="budget_exceeded",
                        provider_metrics=metrics,
                    ) from None
                self._ai_calls_used += 1
                metrics = _metrics_add(metrics, provider_attempts=1)
                try:
                    raw = await self._bounded_request(payload)
                except asyncio.TimeoutError:
                    metrics = _metrics_add(metrics, timeouts=1)
                    if transport_attempt < self._max_transport_attempts:
                        await self._transport_backoff()
                        continue
                    self._record_metrics(metrics)
                    raise OnboardingProfileError(
                        f"{self.provider} onboarding-profile request timed out",
                        retryable=True,
                        failure_class="timeout",
                        provider_metrics=metrics,
                    ) from None
                except OnboardingProfileError as exc:
                    metrics = _metrics_add(
                        metrics,
                        transient_failures=1 if exc.retryable else 0,
                        non_retryable_failures=0 if exc.retryable else 1,
                    )
                    if exc.retryable and transport_attempt < self._max_transport_attempts:
                        await self._transport_backoff()
                        continue
                    self._record_metrics(metrics)
                    raise OnboardingProfileError(
                        str(exc),
                        retryable=exc.retryable,
                        failure_class=exc.failure_class,
                        provider_metrics=metrics,
                    ) from None
                break
            else:
                raise AssertionError("unreachable")
            try:
                result = self._parse_response(
                    raw,
                    description=description,
                    attempt_count=attempt,
                )
            except OnboardingProfileOutputError:
                metrics = _metrics_add(metrics, invalid_output_retries=1)
                if attempt == self._max_output_attempts:
                    self._record_metrics(metrics)
                    raise OnboardingProfileOutputError(
                        f"{self.provider} returned invalid onboarding-profile output "
                        f"after {attempt} attempts",
                        provider_metrics=metrics,
                    ) from None
                continue
            metrics = _metrics_add(metrics, completed_calls=1)
            self._record_metrics(metrics)
            return OnboardingProfileAnalysisCall(
                analysis=result.analysis,
                provider=result.provider,
                requested_model=result.requested_model,
                response_model=result.response_model,
                analyzer_version=result.analyzer_version,
                prompt_version=result.prompt_version,
                schema_version=result.schema_version,
                attempt_count=result.attempt_count,
                usage=result.usage,
                provider_metrics=metrics,
            )
        raise AssertionError("unreachable")

    async def _bounded_request(self, payload: Mapping[str, Any]) -> str:
        try:
            return await asyncio.wait_for(
                asyncio.to_thread(self._request, payload),
                timeout=self._timeout_seconds,
            )
        except asyncio.TimeoutError:
            raise
        except (TimeoutError, socket.timeout) as exc:
            raise asyncio.TimeoutError() from exc

    async def _transport_backoff(self) -> None:
        if self._transport_retry_backoff_seconds:
            await asyncio.sleep(self._transport_retry_backoff_seconds)

    def _record_metrics(self, metrics: OnboardingProfileProviderMetrics) -> None:
        self._metrics = _metrics_add(
            self._metrics,
            provider_attempts=metrics.provider_attempts,
            completed_calls=metrics.completed_calls,
            timeouts=metrics.timeouts,
            transient_failures=metrics.transient_failures,
            non_retryable_failures=metrics.non_retryable_failures,
            invalid_output_retries=metrics.invalid_output_retries,
        )

    def _payload(self, description: str) -> dict[str, Any]:
        system_content = (
            "Extract a freelancer search profile only from the supplied user "
            "description. roles are professions or job titles the user says "
            "they perform or seek. skills are tools, technologies or methods "
            "the user explicitly claims. categories are product, project or "
            "business domains the user explicitly does or wants. Every term "
            "must include an exact contiguous evidence excerpt from the user "
            "text. Use origin=explicit only when value matches that evidence "
            "apart from Unicode, whitespace or case normalization. Use "
            "origin=normalized_inference for a directly supported translation, "
            "alias or canonical label, retaining the exact evidence. Never add "
            "adjacent skills, seniority, work formats, budgets, languages, "
            "locations, exclusions or preferences that the user did not state. "
            "Put ambiguous exact excerpts in uncertain_terms instead of guessing. "
            "missing_fields must exactly list roles, skills or categories whose "
            "arrays are empty. At least one structured field must be usable."
        )
        if self.provider in {"deepseek", "tokenrouter"}:
            response_format: dict[str, Any] = {"type": "json_object"}
            system_content += (
                " Return only one JSON object matching this schema exactly: "
                + json.dumps(
                    OnboardingProfileAnalysis.model_json_schema(),
                    ensure_ascii=False,
                    separators=(",", ":"),
                )
            )
        else:
            response_format = {
                "type": "json_schema",
                "json_schema": {
                    "name": "onboarding_profile_analysis",
                    "strict": True,
                    "schema": OnboardingProfileAnalysis.model_json_schema(),
                },
            }
        payload: dict[str, Any] = {
            "model": self._model,
            "response_format": response_format,
            "messages": [
                {
                    "role": "system",
                    "content": system_content,
                },
                {
                    "role": "user",
                    "content": json.dumps(
                        {"description": description},
                        ensure_ascii=False,
                    ),
                },
            ],
        }
        add_sampling_parameter(
            payload,
            model=self._model,
            temperature=self._temperature,
        )
        return payload

    def _parse_response(
        self,
        raw: str,
        *,
        description: str,
        attempt_count: int,
    ) -> OnboardingProfileAnalysisCall:
        try:
            response = json.loads(raw)
            content = response["choices"][0]["message"]["content"]
            response_model = _bounded_text(
                response["model"],
                "response_model",
                128,
            )
            usage_payload = response["usage"]
            usage = OnboardingProfileUsage.model_validate(
                {
                    "input_tokens": _usage_value(
                        usage_payload,
                        "prompt_tokens",
                        "input_tokens",
                    ),
                    "output_tokens": _usage_value(
                        usage_payload,
                        "completion_tokens",
                        "output_tokens",
                    ),
                    "total_tokens": _usage_value(
                        usage_payload,
                        "total_tokens",
                        "total",
                    ),
                },
                strict=True,
            )
            analysis = OnboardingProfileAnalysis.model_validate_json(
                content,
                strict=True,
            )
            validate_onboarding_profile_grounding(analysis, description)
        except (
            IndexError,
            KeyError,
            TypeError,
            ValueError,
            ValidationError,
        ):
            raise OnboardingProfileOutputError(
                f"{self.provider} returned invalid onboarding-profile output"
            ) from None
        return OnboardingProfileAnalysisCall(
            analysis=analysis,
            provider=self.provider,
            requested_model=self._model,
            response_model=response_model,
            analyzer_version=self.analyzer_version,
            prompt_version=self.prompt_version,
            schema_version=self.schema_version,
            attempt_count=attempt_count,
            usage=usage,
        )

    def _request(self, payload: Mapping[str, Any]) -> str:
        if not self._api_key:
            raise OnboardingProfileError(f"{self._api_key_name} is not configured")
        request = urllib.request.Request(
            self._base_url,
            data=json.dumps(payload, ensure_ascii=False).encode("utf-8"),
            headers={
                "Authorization": f"Bearer {self._api_key}",
                "Content-Type": "application/json",
            },
            method="POST",
        )
        try:
            with urllib.request.urlopen(
                request,
                timeout=self._timeout_seconds,
            ) as response:
                return response.read().decode("utf-8")
        except urllib.error.HTTPError as exc:
            retryable = exc.code == 429 or exc.code >= 500
            failure_class = "http_429" if exc.code == 429 else (
                "http_5xx" if exc.code >= 500 else "http_4xx"
            )
            raise OnboardingProfileError(
                f"{self.provider} onboarding-profile request failed",
                retryable=retryable,
                failure_class=failure_class,
            ) from None
        except urllib.error.URLError as exc:
            raise OnboardingProfileError(
                f"{self.provider} onboarding-profile request failed",
                retryable=True,
                failure_class=_transport_failure_class(exc.reason),
            ) from None
        except (ConnectionError, BrokenPipeError) as exc:
            raise OnboardingProfileError(
                f"{self.provider} onboarding-profile request failed",
                retryable=True,
                failure_class="connection_reset",
            ) from exc


class OpenAICompatibleOnboardingProfileAnalyzer(OpenAIOnboardingProfileAnalyzer):
    """Provider-neutral adapter for OpenAI-compatible chat-completions APIs."""

    @classmethod
    def from_config(
        cls,
        config: RuntimeConfig,
    ) -> OpenAICompatibleOnboardingProfileAnalyzer:
        provider = config.onboarding_profile_provider
        if provider == "openai":
            api_key = (
                ""
                if config.openai_api_key is None
                else config.openai_api_key.get_secret_value()
            )
            api_key_name = "OPENAI_API_KEY"
            base_url = OPENAI_CHAT_COMPLETIONS_URL
        elif provider == "deepseek":
            api_key = (
                ""
                if config.deepseek_api_key is None
                else config.deepseek_api_key.get_secret_value()
            )
            api_key_name = "DEEPSEEK_API_KEY"
            base_url = DEEPSEEK_CHAT_COMPLETIONS_URL
        elif provider == "tokenrouter":
            api_key = (
                ""
                if config.tokenrouter_api_key is None
                else config.tokenrouter_api_key.get_secret_value()
            )
            api_key_name = "TOKENROUTER_API_KEY"
            base_url = _chat_completions_url(config.tokenrouter_base_url)
        else:
            raise OnboardingProfileError(
                f"Unsupported onboarding profile provider: {provider}"
            )
        return cls(
            api_key=api_key,
            provider=provider,
            api_key_name=api_key_name,
            model=config.onboarding_profile_model,
            temperature=config.onboarding_profile_temperature,
            timeout_seconds=config.onboarding_profile_timeout_seconds,
            max_output_attempts=config.onboarding_profile_max_output_attempts,
            max_transport_attempts=config.onboarding_profile_max_transport_attempts,
            transport_retry_backoff_seconds=(
                config.onboarding_profile_transport_retry_backoff_seconds
            ),
            base_url=base_url,
            max_ai_calls_per_run=config.max_ai_calls_per_run,
        )


def validate_onboarding_profile_grounding(
    analysis: OnboardingProfileAnalysis,
    description: str,
) -> None:
    normalize_semantic_text(description)
    for collection in (analysis.roles, analysis.skills, analysis.categories):
        for term in collection:
            normalized_evidence = normalize_semantic_text(term.evidence).casefold()
            if term.evidence not in description:
                raise OnboardingProfileOutputError(
                    "profile term evidence is not present in the user description"
                )
            if (
                term.origin == "explicit"
                and normalize_semantic_text(term.value).casefold()
                != normalized_evidence
            ):
                raise OnboardingProfileOutputError(
                    "explicit profile term does not match its evidence"
                )
    for uncertain in analysis.uncertain_terms:
        if uncertain not in description:
            raise OnboardingProfileOutputError(
                "uncertain term is not present in the user description"
            )


def parsed_profile_from_analysis(
    description: str,
    analysis: OnboardingProfileAnalysis,
) -> ParsedSearchProfile:
    validate_onboarding_profile_grounding(analysis, description)
    return parse_search_profile(
        roles=_term_inputs(analysis.roles),
        skills=_term_inputs(analysis.skills),
        categories=_term_inputs(analysis.categories),
        semantic_text=description,
    )


def onboarding_profile_cache_version(analyzer: OnboardingProfileAnalyzer) -> str:
    configured_identity = getattr(analyzer, "cache_identity", None)
    payload = json.dumps(
        configured_identity
        if configured_identity is not None
        else {
            "provider": analyzer.provider,
            "model": analyzer.model,
            "analyzer_version": analyzer.analyzer_version,
            "prompt_version": analyzer.prompt_version,
            "schema_version": analyzer.schema_version,
        },
        sort_keys=True,
        separators=(",", ":"),
    )
    digest = sha256(payload.encode("utf-8")).hexdigest()[:16]
    return f"{analyzer.analyzer_version}.{digest}"


def _term_inputs(
    terms: tuple[OnboardingProfileTerm, ...],
) -> tuple[SearchProfileTermInput, ...]:
    return tuple(
        SearchProfileTermInput(
            value=term.value,
            evidence=term.evidence,
            origin=SearchProfileTermOrigin(term.origin),
        )
        for term in terms
    )


def _bounded_text(value: Any, field: str, limit: int) -> str:
    if not isinstance(value, str):
        raise ValueError(f"{field} must be a string")
    normalized = value.strip()
    if not normalized or len(normalized) > limit:
        raise ValueError(f"{field} must be non-empty and at most {limit} characters")
    return normalized


def _usage_value(payload: Mapping[str, Any], *names: str) -> Any:
    for name in names:
        if name in payload:
            return payload[name]
    raise KeyError(names[0])


def _safe_version(value: str, field: str) -> str:
    normalized = _bounded_text(value, field, 100)
    if not _SAFE_VERSION.fullmatch(normalized):
        raise ValueError(f"{field} must be a safe version identifier")
    return normalized


def _transport_failure_class(reason: object) -> str:
    if isinstance(reason, (TimeoutError, socket.timeout)):
        return "timeout"
    if isinstance(reason, (ConnectionResetError, ConnectionAbortedError, BrokenPipeError)):
        return "connection_reset"
    return "network_error"
