from __future__ import annotations

import asyncio
from collections.abc import Mapping
from dataclasses import dataclass, replace
from decimal import Decimal
from enum import Enum
from hashlib import sha256
import json
import math
import re
from time import perf_counter
from typing import TYPE_CHECKING, Any, Literal, Protocol, runtime_checkable
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

from .ai_telemetry import (
    AIBudgetExceeded,
    AICallFinish,
    AICallRecorder,
    AICallStart,
    AIModelPrice,
)
from .openai_compat import add_sampling_parameter

if TYPE_CHECKING:
    from .config import RuntimeConfig
    from .message_prefilter import AnalyzerMessage, MinimalAnalyzerInput


OPPORTUNITY_ANALYSIS_SCHEMA_VERSION = "opportunity_analysis.v1"
OPPORTUNITY_ANALYZER_VERSION = "opportunity-analyzer.v1"
OPPORTUNITY_ANALYSIS_PROMPT_VERSION = "opportunity-analysis-prompt.v2"
DEFAULT_OPPORTUNITY_ANALYSIS_MODEL = "gpt-5-nano"
OPPORTUNITY_ROUTING_VERSION = "opportunity-routing.v1"
OPENAI_CHAT_COMPLETIONS_URL = "https://api.openai.com/v1/chat/completions"
_SAFE_VERSION = re.compile(r"^[a-z0-9][a-z0-9._-]{0,99}$")


class OpportunityAnalysisError(RuntimeError):
    def __init__(
        self,
        message: str,
        *,
        retryable: bool = True,
        error_code: str = "provider_request_failed",
        http_status: int | None = None,
    ) -> None:
        super().__init__(message)
        self.retryable = retryable
        self.error_code = error_code
        self.http_status = http_status


class OpportunityAnalysisOutputError(OpportunityAnalysisError):
    pass


class MarketDirection(str, Enum):
    BUYER_TO_SPECIALIST = "buyer_to_specialist"
    SPECIALIST_TO_BUYER = "specialist_to_buyer"
    UNKNOWN = "unknown"


class IntentStage(str, Enum):
    ACTIVE = "active"
    RECOMMENDATION = "recommendation"
    RESEARCH = "research"
    WEAK = "weak"
    NONE = "none"


class OpportunityType(str, Enum):
    ONE_OFF_ORDER = "one_off_order"
    PROJECT = "project"
    VACANCY = "vacancy"
    PART_TIME_CONTRACTOR = "part_time_contractor"
    CONSULTATION = "consultation"
    UNKNOWN = "unknown"


class _StrictContract(BaseModel):
    model_config = ConfigDict(
        extra="forbid",
        frozen=True,
        strict=True,
        str_strip_whitespace=True,
    )


class OpportunityBudget(_StrictContract):
    known: bool
    min: float | None
    max: float | None
    currency: str | None
    period: str | None
    explicit: bool

    @field_validator("currency", "period")
    @classmethod
    def validate_optional_label(cls, value: str | None) -> str | None:
        return _validated_optional_text(value, limit=32)

    @model_validator(mode="after")
    def validate_budget_consistency(self) -> OpportunityBudget:
        amounts = (self.min, self.max)
        if any(
            value is not None and (not math.isfinite(value) or value < 0)
            for value in amounts
        ):
            raise ValueError("budget amounts must be finite and non-negative")
        if self.min is not None and self.max is not None and self.min > self.max:
            raise ValueError("budget min cannot exceed max")
        if self.known and self.min is None and self.max is None:
            raise ValueError("known budget requires at least one numeric amount")
        if self.known and not self.explicit:
            raise ValueError("known budget must be explicit in the supplied context")
        if not self.known and any(
            value is not None
            for value in (self.min, self.max, self.currency, self.period)
        ):
            raise ValueError("unknown budget cannot contain normalized amount metadata")
        return self


class OpportunityWork(_StrictContract):
    remote: bool | None
    location: str | None
    full_time: bool | None
    part_time: bool | None


class OpportunityContact(_StrictContract):
    telegram: str | None
    email: str | None
    url: str | None

    @field_validator("telegram", "email", "url")
    @classmethod
    def validate_optional_contact(cls, value: str | None) -> str | None:
        return _validated_optional_text(value, limit=2048)


class OpportunityQuality(_StrictContract):
    actionability: float = Field(ge=0, le=1)
    commercial_plausibility: float = Field(ge=0, le=1)
    specificity: float = Field(ge=0, le=1)
    credibility: float = Field(ge=0, le=1)


class OpportunityAnalysis(_StrictContract):
    schema_version: Literal["opportunity_analysis.v1"]
    is_opportunity: bool
    confidence: float = Field(ge=0, le=1)
    market_direction: MarketDirection
    intent_stage: IntentStage
    opportunity_type: OpportunityType
    category: str | None
    role_title: str | None
    skills: tuple[str, ...]
    task_summary: str | None
    budget: OpportunityBudget
    work: OpportunityWork
    language: str | None
    contact: OpportunityContact
    quality: OpportunityQuality
    red_flags: tuple[str, ...]

    @field_validator("category", "role_title", "task_summary", "language")
    @classmethod
    def validate_optional_extracted_text(cls, value: str | None) -> str | None:
        return _validated_optional_text(value, limit=1000)

    @field_validator("skills", "red_flags")
    @classmethod
    def validate_extracted_labels(cls, values: tuple[str, ...]) -> tuple[str, ...]:
        if len(values) > 64:
            raise ValueError("extracted label collections are bounded to 64 values")
        normalized: list[str] = []
        seen: set[str] = set()
        for value in values:
            label = _validated_optional_text(value, limit=200)
            if label is None:
                raise ValueError("extracted labels cannot be empty")
            identity = label.casefold()
            if identity in seen:
                raise ValueError("extracted labels must be unique")
            seen.add(identity)
            normalized.append(label)
        return tuple(normalized)

    @model_validator(mode="after")
    def validate_classification_consistency(self) -> OpportunityAnalysis:
        if (
            self.is_opportunity
            and self.market_direction is not MarketDirection.BUYER_TO_SPECIALIST
        ):
            raise ValueError("an opportunity must express buyer-to-specialist demand")
        if self.is_opportunity and self.intent_stage is IntentStage.NONE:
            raise ValueError("an opportunity cannot have intent_stage=none")
        if (
            self.market_direction is MarketDirection.SPECIALIST_TO_BUYER
            and self.intent_stage is not IntentStage.NONE
        ):
            raise ValueError("specialist self-promotion must have intent_stage=none")
        return self


class OpportunityAnalysisUsage(_StrictContract):
    input_tokens: int = Field(ge=0)
    output_tokens: int = Field(ge=0)
    total_tokens: int = Field(ge=0)


class OpportunityAnalysisInvocation(_StrictContract):
    provider: str = Field(min_length=1, max_length=64)
    requested_model: str = Field(min_length=1, max_length=128)
    response_model: str = Field(min_length=1, max_length=128)
    analyzer_version: str = Field(min_length=1, max_length=64)
    cache_analyzer_version: str = Field(min_length=1, max_length=64)
    prompt_version: str = Field(min_length=1, max_length=100)
    schema_version: str = Field(min_length=1, max_length=32)
    attempt_count: int = Field(ge=1, le=5)
    usage: OpportunityAnalysisUsage
    stage: str = "opportunity_analysis.primary"
    routing_version: str = OPPORTUNITY_ROUTING_VERSION
    route_reason: str = "primary"


class OpportunityAnalysisCacheEnvelope(_StrictContract):
    envelope_version: Literal["opportunity_analysis_cache.v1"]
    analysis: OpportunityAnalysis
    invocation: OpportunityAnalysisInvocation


@dataclass(frozen=True)
class OpportunityAnalysisCall:
    analysis: OpportunityAnalysis
    provider: str
    requested_model: str
    response_model: str
    analyzer_version: str
    prompt_version: str
    schema_version: str
    attempt_count: int
    usage: OpportunityAnalysisUsage
    stage: str = "opportunity_analysis.primary"
    routing_version: str = OPPORTUNITY_ROUTING_VERSION
    route_reason: str = "primary"


@runtime_checkable
class OpportunityAnalyzer(Protocol):
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

    async def analyze(
        self,
        candidate: MinimalAnalyzerInput,
    ) -> OpportunityAnalysisCall: ...


class OpenAIOpportunityAnalyzer:
    def __init__(
        self,
        *,
        api_key: str,
        model: str,
        temperature: float = 0.0,
        timeout_seconds: int = 45,
        max_output_attempts: int = 2,
        base_url: str = OPENAI_CHAT_COMPLETIONS_URL,
        analyzer_version: str = OPPORTUNITY_ANALYZER_VERSION,
        prompt_version: str = OPPORTUNITY_ANALYSIS_PROMPT_VERSION,
        recorder: AICallRecorder | None = None,
        stage: str = "opportunity_analysis.primary",
        routing_version: str = OPPORTUNITY_ROUTING_VERSION,
        route_reason: str = "primary",
        price: AIModelPrice | None = None,
    ) -> None:
        if not api_key.strip():
            raise OpportunityAnalysisError("OPENAI_API_KEY is not configured")
        self._api_key = api_key
        self._model = _bounded_text(model, "model", 128)
        self._temperature = _bounded_ratio(temperature, "temperature", upper=2)
        if timeout_seconds <= 0:
            raise ValueError("timeout_seconds must be positive")
        if not 1 <= max_output_attempts <= 5:
            raise ValueError("max_output_attempts must be between 1 and 5")
        self._timeout_seconds = timeout_seconds
        self._max_output_attempts = max_output_attempts
        self._base_url = _bounded_text(base_url, "base_url", 2048)
        self._analyzer_version = _safe_version(analyzer_version, "analyzer_version")
        self._prompt_version = _safe_version(prompt_version, "prompt_version")
        self._recorder = recorder
        self._stage = _safe_version(stage, "stage")
        self._routing_version = _safe_version(routing_version, "routing_version")
        self._route_reason = _safe_version(route_reason, "route_reason")
        self._price = price or AIModelPrice(
            pricing_version="unconfigured.v1",
            input_usd_per_million=Decimal("0"),
            output_usd_per_million=Decimal("0"),
        )

    @classmethod
    def from_config(cls, config: RuntimeConfig) -> OpenAIOpportunityAnalyzer:
        if config.opportunity_analysis_provider != "openai":
            raise OpportunityAnalysisError(
                "OpenAI adapter requires OPPORTUNITY_ANALYSIS_PROVIDER=openai"
            )
        api_key = (
            ""
            if config.openai_api_key is None
            else config.openai_api_key.get_secret_value()
        )
        return cls(
            api_key=api_key,
            model=config.opportunity_analysis_model,
            temperature=config.opportunity_analysis_temperature,
            timeout_seconds=config.opportunity_analysis_timeout_seconds,
            max_output_attempts=config.opportunity_analysis_max_output_attempts,
        )

    @property
    def provider(self) -> str:
        return "openai"

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
        return OPPORTUNITY_ANALYSIS_SCHEMA_VERSION

    async def analyze(
        self,
        candidate: MinimalAnalyzerInput,
    ) -> OpportunityAnalysisCall:
        payload = self._payload(candidate)
        for attempt in range(1, self._max_output_attempts + 1):
            telemetry_id = None
            started = perf_counter()
            if self._recorder is not None:
                telemetry_id = await self._recorder.begin(
                    AICallStart(
                        raw_message_id=candidate.current.raw_message_id,
                        stage=self._stage,
                        provider=self.provider,
                        requested_model=self._model,
                        analyzer_version=self._analyzer_version,
                        prompt_version=self._prompt_version,
                        schema_version=self.schema_version,
                        routing_version=self._routing_version,
                        route_reason=self._route_reason,
                        provider_attempt=attempt,
                        price=self._price,
                    )
                )
            try:
                raw = await asyncio.to_thread(self._request, payload)
            except OpportunityAnalysisError as exc:
                if telemetry_id is not None:
                    await self._recorder.finish(
                        telemetry_id,
                        AICallFinish(
                            status="request_failed",
                            latency_ms=_latency_ms(started),
                            error_code=exc.error_code,
                        ),
                    )
                raise
            try:
                call = self._parse_response(
                    raw,
                    attempt_count=attempt,
                    candidate=candidate,
                )
            except OpportunityAnalysisOutputError:
                if telemetry_id is not None:
                    response_model, usage = _safe_response_metadata(raw)
                    await self._recorder.finish(
                        telemetry_id,
                        AICallFinish(
                            status="invalid_output",
                            latency_ms=_latency_ms(started),
                            response_model=response_model,
                            input_tokens=None if usage is None else usage.input_tokens,
                            output_tokens=None if usage is None else usage.output_tokens,
                            total_tokens=None if usage is None else usage.total_tokens,
                            error_code="invalid_structured_output",
                        ),
                    )
                if attempt == self._max_output_attempts:
                    raise OpportunityAnalysisOutputError(
                        "OpenAI returned invalid opportunity-analysis output "
                        f"after {attempt} attempts"
                    ) from None
            else:
                if telemetry_id is not None:
                    await self._recorder.finish(
                        telemetry_id,
                        AICallFinish(
                            status="succeeded",
                            latency_ms=_latency_ms(started),
                            response_model=call.response_model,
                            input_tokens=call.usage.input_tokens,
                            output_tokens=call.usage.output_tokens,
                            total_tokens=call.usage.total_tokens,
                        ),
                    )
                return call
        raise AssertionError("unreachable")

    def _payload(self, candidate: MinimalAnalyzerInput) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "model": self._model,
            "response_format": {
                "type": "json_schema",
                "json_schema": {
                    "name": "opportunity_analysis",
                    "strict": True,
                    "schema": OpportunityAnalysis.model_json_schema(),
                },
            },
            "messages": [
                {
                    "role": "system",
                    "content": (
                        "Classify whether the current message is potentially paid demand. "
                        "market_direction=buyer_to_specialist only when a person or company "
                        "seeks a specialist, contractor, executor or employee; use "
                        "specialist_to_buyer for self-promotion or offered services and "
                        "unknown when direction is genuinely ambiguous. Set is_opportunity "
                        "true only for buyer_to_specialist demand. intent_stage=active for "
                        "a current explicit request, recommendation for a referral request, "
                        "research for exploratory cost/feasibility/vendor research, weak for "
                        "vague possible demand, and none when there is no buyer intent. "
                        "Use opportunity_type one_off_order for a bounded task, project for "
                        "multi-step delivery, vacancy for employment, part_time_contractor "
                        "for ongoing fractional/contract work, consultation for paid expert "
                        "advice, and unknown only when the type is unsupported or unclear. "
                        "Extract category as a concise normalized free-text domain and "
                        "role_title as a normalized free-text role; never force either into "
                        "a closed profession list. Extract only supported skills and keep "
                        "unknown category, role, summary, work or language fields null. "
                        "For budget, normalize common RU/EN forms such as руб/₽ to RUB, "
                        "$ to USD and € to EUR, including k/к/тыс multipliers and ranges. "
                        "Normalize period to project, hour, day, week, month or year when "
                        "stated. known=true requires a numeric min or max; zero is a known "
                        "zero, while no amount uses known=false and null amount metadata. "
                        "explicit marks any direct budget statement, including negotiable. "
                        "Copy Telegram handles, email addresses and URLs exactly from current "
                        "or parent content; use null when absent and never derive a contact. "
                        "The supplied message_url is metadata, not message content, and must "
                        "never be copied into contact fields. "
                        "Score quality as intrinsic actionability, commercial plausibility, "
                        "specificity and credibility only, independent of personal relevance. "
                        "Put concise evidence-based scam, spam or credibility concerns in "
                        "red_flags without changing them for any user's preferences. "
                        "Use current as primary and only the supplied direct parent for "
                        "disambiguation; never assume other chat history exists. Never invent "
                        "unknown details; return null, empty lists or unknown as allowed."
                    ),
                },
                {
                    "role": "user",
                    "content": json.dumps(
                        {
                            "current": _message_payload(candidate.current),
                            "parent": (
                                None
                                if candidate.parent is None
                                else _message_payload(candidate.parent)
                            ),
                        },
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
        attempt_count: int,
        candidate: MinimalAnalyzerInput,
    ) -> OpportunityAnalysisCall:
        try:
            response = json.loads(raw)
            content = response["choices"][0]["message"]["content"]
            response_model = _bounded_text(response["model"], "response_model", 128)
            usage = _usage(response["usage"])
            analysis = OpportunityAnalysis.model_validate_json(content, strict=True)
            validate_opportunity_analysis_grounding(analysis, candidate)
        except (
            IndexError,
            KeyError,
            TypeError,
            ValueError,
            ValidationError,
        ):
            raise OpportunityAnalysisOutputError(
                "OpenAI returned invalid opportunity-analysis output"
            ) from None
        return OpportunityAnalysisCall(
            analysis=analysis,
            provider=self.provider,
            requested_model=self._model,
            response_model=response_model,
            analyzer_version=self._analyzer_version,
            prompt_version=self._prompt_version,
            schema_version=analysis.schema_version,
            attempt_count=attempt_count,
            usage=usage,
            stage=self._stage,
            routing_version=self._routing_version,
            route_reason=self._route_reason,
        )

    def _request(self, payload: Mapping[str, Any]) -> str:
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
            status = int(exc.code)
            retryable = status == 429 or status >= 500
            exc.close()
            raise OpportunityAnalysisError(
                "OpenAI opportunity-analysis request failed",
                retryable=retryable,
                error_code=(
                    "provider_invalid_request"
                    if not retryable
                    else "provider_request_failed"
                ),
                http_status=status,
            ) from None
        except urllib.error.URLError:
            raise OpportunityAnalysisError(
                "OpenAI opportunity-analysis request failed",
                retryable=True,
                error_code="provider_request_failed",
            ) from None


class RoutedOpportunityAnalyzer:
    def __init__(
        self,
        primary: OpportunityAnalyzer,
        fallback: OpportunityAnalyzer | None,
        *,
        confidence_threshold: float = 0.65,
        routing_version: str = OPPORTUNITY_ROUTING_VERSION,
    ) -> None:
        if primary.schema_version != OPPORTUNITY_ANALYSIS_SCHEMA_VERSION:
            raise ValueError("primary analyzer uses an unsupported schema")
        if fallback is not None and (
            fallback.analyzer_version != primary.analyzer_version
            or fallback.prompt_version != primary.prompt_version
            or fallback.schema_version != primary.schema_version
        ):
            raise ValueError("fallback analyzer must use the same contract versions")
        self._primary = primary
        self._fallback = fallback
        self._confidence_threshold = _bounded_ratio(
            confidence_threshold,
            "confidence_threshold",
        )
        self._routing_version = _safe_version(routing_version, "routing_version")

    @property
    def provider(self) -> str:
        return self._primary.provider

    @property
    def model(self) -> str:
        return self._primary.model

    @property
    def analyzer_version(self) -> str:
        return self._primary.analyzer_version

    @property
    def prompt_version(self) -> str:
        return self._primary.prompt_version

    @property
    def schema_version(self) -> str:
        return self._primary.schema_version

    @property
    def cache_identity(self) -> Mapping[str, Any]:
        return {
            "primary": _route_identity(self._primary),
            "fallback": (
                None if self._fallback is None else _route_identity(self._fallback)
            ),
            "confidence_threshold": self._confidence_threshold,
            "routing_version": self._routing_version,
        }

    @property
    def compatible_cache_versions(self) -> frozenset[str]:
        return frozenset({opportunity_analysis_cache_version(self._primary)})

    async def analyze(self, candidate: MinimalAnalyzerInput) -> OpportunityAnalysisCall:
        primary = await self._primary.analyze(candidate)
        if (
            self._fallback is None
            or primary.analysis.confidence >= self._confidence_threshold
        ):
            return replace(
                primary,
                routing_version=self._routing_version,
                route_reason="primary_confident",
            )
        try:
            fallback = await self._fallback.analyze(candidate)
        except AIBudgetExceeded:
            # The stronger route is optional work. Preserve the successful
            # primary result when a configured spend guard suspends it, so a
            # budget boundary never blocks critical opportunity processing.
            return replace(
                primary,
                routing_version=self._routing_version,
                route_reason="fallback_budget_exhausted",
            )
        return replace(
            fallback,
            routing_version=self._routing_version,
            route_reason="low_confidence_fallback",
        )

    def accepts_call(self, call: OpportunityAnalysisCall) -> bool:
        return self.accepts_metadata(
            provider=call.provider,
            model=call.requested_model,
            analyzer_version=call.analyzer_version,
            prompt_version=call.prompt_version,
            schema_version=call.schema_version,
            routing_version=call.routing_version,
        )

    def accepts_metadata(
        self,
        *,
        provider: str,
        model: str,
        analyzer_version: str,
        prompt_version: str,
        schema_version: str,
        routing_version: str,
    ) -> bool:
        return any(
            provider == route.provider
            and model == route.model
            and analyzer_version == route.analyzer_version
            and prompt_version == route.prompt_version
            and schema_version == route.schema_version
            for route in (self._primary, self._fallback)
            if route is not None
        ) and routing_version == self._routing_version


def _message_payload(message: AnalyzerMessage) -> dict[str, Any]:
    return {
        "source_id": message.source_id,
        "external_source_id": message.external_source_id,
        "external_message_id": message.external_message_id,
        "message_date": message.message_date.isoformat(),
        "message_url": message.message_url,
        "content": message.content,
    }


def _usage(value: Any) -> OpportunityAnalysisUsage:
    if not isinstance(value, Mapping):
        raise ValueError("usage is missing")
    return OpportunityAnalysisUsage.model_validate(
        {
            "input_tokens": value.get("prompt_tokens"),
            "output_tokens": value.get("completion_tokens"),
            "total_tokens": value.get("total_tokens"),
        },
        strict=True,
    )


def _safe_response_metadata(
    raw: str,
) -> tuple[str | None, OpportunityAnalysisUsage | None]:
    try:
        response = json.loads(raw)
        return (
            _bounded_text(response["model"], "response_model", 128),
            _usage(response["usage"]),
        )
    except (KeyError, TypeError, ValueError, ValidationError):
        return None, None


def _latency_ms(started: float) -> int:
    return max(0, round((perf_counter() - started) * 1000))


def validate_opportunity_analysis_grounding(
    analysis: OpportunityAnalysis,
    candidate: MinimalAnalyzerInput,
) -> None:
    supplied_content = [candidate.current.content]
    if candidate.parent is not None:
        supplied_content.append(candidate.parent.content)
    for field_name in ("telegram", "email", "url"):
        value = getattr(analysis.contact, field_name)
        if value is not None and not any(
            value in text for text in supplied_content
        ):
            raise OpportunityAnalysisOutputError(
                f"Extracted {field_name} contact is not grounded in supplied context"
            )


def _bounded_text(value: Any, name: str, limit: int) -> str:
    if not isinstance(value, str):
        raise ValueError(f"{name} must be text")
    normalized = value.strip()
    if not normalized or len(normalized) > limit:
        raise ValueError(f"{name} must contain between 1 and {limit} characters")
    return normalized


def _validated_optional_text(value: str | None, *, limit: int) -> str | None:
    if value is None:
        return None
    if not value or len(value) > limit:
        raise ValueError(f"optional text must contain between 1 and {limit} characters")
    return value


def _bounded_ratio(value: float, name: str, *, upper: float = 1) -> float:
    normalized = float(value)
    if not math.isfinite(normalized) or not 0 <= normalized <= upper:
        raise ValueError(f"{name} must be a finite value from 0 to {upper}")
    return normalized


def _safe_version(value: str, name: str) -> str:
    normalized = _bounded_text(value, name, 100).lower()
    if not _SAFE_VERSION.fullmatch(normalized):
        raise ValueError(f"{name} must be a safe lowercase version identifier")
    return normalized


def opportunity_analysis_cache_version(analyzer: OpportunityAnalyzer) -> str:
    configured_identity = getattr(analyzer, "cache_identity", None)
    identity = json.dumps(
        configured_identity
        if configured_identity is not None
        else _route_identity(analyzer),
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    )
    digest = sha256(identity.encode("utf-8")).hexdigest()[:12]
    base = _safe_version(analyzer.analyzer_version, "analyzer_version")
    return f"{base[:50]}.{digest}"


def opportunity_analysis_call_is_compatible(
    analyzer: OpportunityAnalyzer,
    call: OpportunityAnalysisCall,
) -> bool:
    checker = getattr(analyzer, "accepts_call", None)
    if checker is not None:
        return bool(checker(call))
    return (
        call.provider == analyzer.provider
        and call.requested_model == analyzer.model
        and call.analyzer_version == analyzer.analyzer_version
        and call.prompt_version == analyzer.prompt_version
        and call.schema_version == analyzer.schema_version
    )


def _route_identity(analyzer: OpportunityAnalyzer) -> dict[str, str]:
    return {
        "provider": analyzer.provider,
        "model": analyzer.model,
        "analyzer_version": analyzer.analyzer_version,
        "prompt_version": analyzer.prompt_version,
        "schema_version": analyzer.schema_version,
    }


def opportunity_analysis_cache_envelope(
    call: OpportunityAnalysisCall,
    *,
    cache_analyzer_version: str,
) -> OpportunityAnalysisCacheEnvelope:
    return OpportunityAnalysisCacheEnvelope(
        envelope_version="opportunity_analysis_cache.v1",
        analysis=call.analysis,
        invocation=OpportunityAnalysisInvocation(
            provider=call.provider,
            requested_model=call.requested_model,
            response_model=call.response_model,
            analyzer_version=call.analyzer_version,
            cache_analyzer_version=cache_analyzer_version,
            prompt_version=call.prompt_version,
            schema_version=call.schema_version,
            attempt_count=call.attempt_count,
            usage=call.usage,
            stage=call.stage,
            routing_version=call.routing_version,
            route_reason=call.route_reason,
        ),
    )
