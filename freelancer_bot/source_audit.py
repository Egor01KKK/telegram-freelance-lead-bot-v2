from __future__ import annotations

import asyncio
from collections.abc import Mapping
from copy import deepcopy
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
import hashlib
import json
import math
import re
from typing import Any, Literal, Protocol, runtime_checkable
import urllib.error
import urllib.request

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from .persistence.database import Database
from .persistence.source_audits import (
    SourceAuditRecord,
    SourceAuditRepository,
    SourceAuditWrite,
)
from .persistence.source_metrics import SourceHealthStatus, SourceMetricsRepository
from .persistence.source_repository import SourceRecord, SourceRepository, SourceStatus
from .openai_compat import add_sampling_parameter
from .source_audit_sampler import SourceAuditSample, SourceAuditSampler, SourceAuditTarget
from .source_ai_config import (
    OPENAI_CHAT_COMPLETIONS_URL,
    SourceAIProviderSettings,
    normalize_chat_completions_url,
    resolve_source_ai_provider,
)


SOURCE_AUDIT_SCHEMA_VERSION = "source-audit.v1"
_SAFE_IDENTIFIER = re.compile(r"^[a-z0-9][a-z0-9._:-]{0,99}$")
_LIFECYCLE_ACTOR_KINDS = frozenset({"seed", "system", "operator"})


class SourceAuditDecision(str, Enum):
    APPROVED = "approved"
    REJECTED = "rejected"
    NEEDS_REVIEW = "needs_review"


class SourceAuditError(RuntimeError):
    pass


class SourceAuditTaxonomyTerm(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, str_strip_whitespace=True)

    key: str = Field(min_length=1, max_length=100)
    display_name: str = Field(min_length=1, max_length=200)

    @field_validator("key")
    @classmethod
    def normalize_key(cls, value: str) -> str:
        normalized = value.lower()
        if not _SAFE_IDENTIFIER.fullmatch(normalized):
            raise ValueError("taxonomy key must be a safe lowercase identifier")
        return normalized


class SourceAuditClassification(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, str_strip_whitespace=True)

    schema_version: Literal["source-audit.v1"]
    analyzed_message_count: int = Field(ge=0, le=200)
    commercial_opportunity_count: int = Field(ge=0)
    buyer_intent_count: int = Field(ge=0)
    seller_promotion_count: int = Field(ge=0)
    ads_spam_count: int = Field(ge=0)
    duplicate_count: int = Field(ge=0)
    content_mix: dict[str, float]
    primary_language: str | None = Field(max_length=100)
    languages: tuple[SourceAuditTaxonomyTerm, ...]
    categories: tuple[SourceAuditTaxonomyTerm, ...]

    @field_validator("primary_language")
    @classmethod
    def normalize_primary_language(cls, value: str | None) -> str | None:
        if value is None:
            return None
        normalized = value.lower()
        if not _SAFE_IDENTIFIER.fullmatch(normalized):
            raise ValueError("primary_language must be a safe lowercase identifier")
        return normalized

    @field_validator("content_mix")
    @classmethod
    def validate_content_mix(cls, value: dict[str, float]) -> dict[str, float]:
        normalized: dict[str, float] = {}
        for raw_key, raw_ratio in value.items():
            key = raw_key.strip().lower()
            if not _SAFE_IDENTIFIER.fullmatch(key):
                raise ValueError("content_mix keys must be safe lowercase identifiers")
            ratio = float(raw_ratio)
            if not math.isfinite(ratio) or not 0 <= ratio <= 1:
                raise ValueError("content_mix ratios must be finite values from 0 to 1")
            normalized[key] = ratio
        return normalized

    @model_validator(mode="after")
    def validate_strict_result(self) -> "SourceAuditClassification":
        counts = (
            self.commercial_opportunity_count,
            self.buyer_intent_count,
            self.seller_promotion_count,
            self.ads_spam_count,
            self.duplicate_count,
        )
        if any(count > self.analyzed_message_count for count in counts):
            raise ValueError("classification counts cannot exceed analyzed_message_count")
        if len({term.key for term in self.languages}) != len(self.languages):
            raise ValueError("language taxonomy keys must be unique")
        if len({term.key for term in self.categories}) != len(self.categories):
            raise ValueError("category taxonomy keys must be unique")
        language_keys = {term.key for term in self.languages}
        if self.primary_language is not None and self.primary_language not in language_keys:
            raise ValueError("primary_language must be present in languages")
        mix_total = sum(self.content_mix.values())
        if self.analyzed_message_count == 0:
            if self.content_mix:
                raise ValueError("an empty sample must have an empty content_mix")
        elif not self.content_mix or not math.isclose(mix_total, 1.0, abs_tol=0.001):
            raise ValueError("content_mix ratios must sum to 1 for a non-empty sample")
        return self


def source_audit_response_schema() -> dict[str, Any]:
    """Return the OpenAI strict-compatible schema for source-audit output.

    ``content_mix`` remains an extensible mapping in the persisted model, but
    OpenAI Structured Outputs requires strict object properties to be declared
    explicitly. The provider contract therefore uses a closed operational mix
    while preserving the historical storage shape.
    """

    schema = deepcopy(SourceAuditClassification.model_json_schema())
    mix_properties = {
        "buyer_demand": {"type": "number"},
        "seller_promotion": {"type": "number"},
        "ads_spam": {"type": "number"},
        "duplicate": {"type": "number"},
        "other": {"type": "number"},
    }
    schema["properties"]["content_mix"] = {
        "anyOf": [
            {
                "type": "object",
                "additionalProperties": False,
                "properties": mix_properties,
                "required": list(mix_properties),
            },
            {
                "type": "object",
                "additionalProperties": False,
                "properties": {},
                "required": [],
            },
        ]
    }
    return schema


@runtime_checkable
class SourceAuditProvider(Protocol):
    @property
    def name(self) -> str: ...

    @property
    def model(self) -> str: ...

    @property
    def analyzer_version(self) -> str: ...

    async def classify(self, sample: SourceAuditSample) -> SourceAuditClassification: ...


class OpenAICompatibleSourceAuditProvider:
    def __init__(
        self,
        *,
        api_key: str,
        model: str,
        temperature: float = 0.0,
        timeout_seconds: int = 45,
        base_url: str = OPENAI_CHAT_COMPLETIONS_URL,
        provider: str = "openai",
        analyzer_version: str | None = None,
        max_output_attempts: int = 2,
    ) -> None:
        if not api_key.strip():
            raise SourceAuditError(f"{provider.upper()}_API_KEY is not configured")
        normalized_provider = provider.strip().lower()
        if normalized_provider not in {"openai", "deepseek", "tokenrouter"}:
            raise SourceAuditError(f"Unsupported source-audit provider: {provider}")
        self._provider = normalized_provider
        self._api_key = api_key
        self._model = _bounded_text(model, "model", 128)
        self._temperature = _bounded_ratio(temperature, "temperature", upper=2)
        if timeout_seconds <= 0:
            raise ValueError("timeout_seconds must be positive")
        self._timeout_seconds = timeout_seconds
        self._base_url = _required_text(
            (
                base_url
                if normalized_provider == "openai"
                else normalize_chat_completions_url(base_url)
            ),
            "base_url",
        )
        self._analyzer_version = _safe_name(
            analyzer_version or f"{normalized_provider}-source-audit-v1",
            "analyzer_version",
            64,
        )
        if not 1 <= max_output_attempts <= 3:
            raise ValueError("max_output_attempts must be between 1 and 3")
        self._max_output_attempts = max_output_attempts

    @property
    def name(self) -> str:
        return self._provider

    @property
    def model(self) -> str:
        return self._model

    @property
    def analyzer_version(self) -> str:
        return self._analyzer_version

    async def classify(self, sample: SourceAuditSample) -> SourceAuditClassification:
        payload: dict[str, Any] = {
            "model": self._model,
            "response_format": _source_audit_response_format(self._provider),
            "messages": [
                {
                    "role": "system",
                    "content": _source_audit_system_prompt(self._provider),
                },
                {
                    "role": "user",
                    "content": json.dumps(
                        {
                            "source_id": sample.source_id,
                            "window_started_at": sample.window_started_at.isoformat(),
                            "window_ended_at": sample.window_ended_at.isoformat(),
                            "messages": [
                                {
                                    "message_id": message.message_id,
                                    "occurred_at": message.occurred_at.isoformat(),
                                    "text": message.text,
                                }
                                for message in sample.messages
                            ],
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
        for attempt in range(1, self._max_output_attempts + 1):
            raw = await asyncio.to_thread(self._request, payload)
            try:
                response = json.loads(raw)
                content = response["choices"][0]["message"]["content"]
                return SourceAuditClassification.model_validate_json(content)
            except (KeyError, IndexError, TypeError, ValueError) as exc:
                if attempt == self._max_output_attempts:
                    raise SourceAuditError(
                        f"{self._provider} returned an invalid source-audit result"
                    ) from exc
                payload["messages"].append(
                    {
                        "role": "system",
                        "content": (
                            "Validation correction: return a complete replacement object. "
                            "primary_language must be null or exactly one key present in "
                            "languages. For an empty sample, content_mix must be {}. "
                            "For a non-empty sample, use all five declared content_mix "
                            "keys and make their ratios sum to 1."
                        ),
                    }
                )
        raise AssertionError("unreachable")

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
        except (urllib.error.HTTPError, urllib.error.URLError) as exc:
            if isinstance(exc, urllib.error.HTTPError):
                exc.close()
            raise SourceAuditError(
                f"{self._provider} source-audit request failed"
            ) from exc


class OpenAISourceAuditProvider(OpenAICompatibleSourceAuditProvider):
    """Backward-compatible OpenAI source-audit provider name."""

    def __init__(
        self,
        *,
        api_key: str,
        model: str,
        temperature: float = 0.0,
        timeout_seconds: int = 45,
        base_url: str = OPENAI_CHAT_COMPLETIONS_URL,
        analyzer_version: str = "openai-source-audit-v1",
        max_output_attempts: int = 2,
    ) -> None:
        super().__init__(
            api_key=api_key,
            model=model,
            temperature=temperature,
            timeout_seconds=timeout_seconds,
            base_url=base_url,
            provider="openai",
            analyzer_version=analyzer_version,
            max_output_attempts=max_output_attempts,
        )


def source_audit_provider_from_config(config: Any) -> SourceAuditProvider:
    settings: SourceAIProviderSettings = resolve_source_ai_provider(config)
    return OpenAICompatibleSourceAuditProvider(
        api_key=settings.api_key,
        model=config.source_audit_model,
        temperature=config.source_audit_temperature,
        timeout_seconds=config.source_audit_timeout_seconds,
        base_url=settings.base_url,
        provider=settings.name,
        analyzer_version=f"{settings.name}-source-audit-v1",
    )


def _source_audit_response_format(provider: str) -> dict[str, Any]:
    if provider == "openai":
        return {
            "type": "json_schema",
            "json_schema": {
                "name": "source_audit",
                "strict": True,
                "schema": source_audit_response_schema(),
            },
        }
    return {"type": "json_object"}


def _source_audit_system_prompt(provider: str) -> str:
    prompt = (
        "Classify a bounded recent sample from one community. "
        "Count commercial buyer opportunities separately from seller "
        "self-promotion, ads/spam and duplicate/reposted messages. "
        "Return extensible language/category taxonomy labels and a content "
        "mix whose ratios sum to 1. The content_mix object must contain "
        "exactly buyer_demand, seller_promotion, ads_spam, duplicate and "
        "other ratios; classify each sampled message into one primary "
        "bucket. Never invent messages or infer access."
    )
    if provider != "openai":
        prompt += (
            " Return one JSON object only, with no markdown or prose, matching "
            "this complete source-audit contract: "
            + json.dumps(
                source_audit_response_schema(),
                ensure_ascii=False,
                sort_keys=True,
            )
        )
    return prompt


@dataclass(frozen=True)
class SourceAuditDecisionPolicy:
    minimum_evidence_messages: int = 30
    approval_minimum_yield: float = 0.03
    approval_maximum_seller_ratio: float = 0.50
    approval_maximum_spam_ratio: float = 0.40
    approval_maximum_duplicate_ratio: float = 0.60
    rejection_minimum_evidence_messages: int = 60
    rejection_spam_ratio: float = 0.70
    rejection_seller_ratio: float = 0.75

    def to_payload(self) -> dict[str, Any]:
        return {
            "version": "source-audit-thresholds.v1",
            "minimum_evidence_messages": self.minimum_evidence_messages,
            "approval_minimum_yield": self.approval_minimum_yield,
            "approval_maximum_seller_ratio": self.approval_maximum_seller_ratio,
            "approval_maximum_spam_ratio": self.approval_maximum_spam_ratio,
            "approval_maximum_duplicate_ratio": self.approval_maximum_duplicate_ratio,
            "rejection_minimum_evidence_messages": (
                self.rejection_minimum_evidence_messages
            ),
            "rejection_spam_ratio": self.rejection_spam_ratio,
            "rejection_seller_ratio": self.rejection_seller_ratio,
        }

    def __post_init__(self) -> None:
        if self.minimum_evidence_messages <= 0:
            raise ValueError("minimum_evidence_messages must be positive")
        if self.rejection_minimum_evidence_messages < self.minimum_evidence_messages:
            raise ValueError("rejection evidence cannot be lower than approval evidence")
        for name in (
            "approval_minimum_yield",
            "approval_maximum_seller_ratio",
            "approval_maximum_spam_ratio",
            "approval_maximum_duplicate_ratio",
            "rejection_spam_ratio",
            "rejection_seller_ratio",
        ):
            _bounded_ratio(getattr(self, name), name)

    def decide(
        self,
        classification: SourceAuditClassification,
    ) -> tuple[SourceAuditDecision, tuple[Mapping[str, Any], ...]]:
        count = classification.analyzed_message_count
        opportunity_yield = _ratio(classification.commercial_opportunity_count, count)
        seller_ratio = _ratio(classification.seller_promotion_count, count)
        spam_ratio = _ratio(classification.ads_spam_count, count)
        duplicate_ratio = _ratio(classification.duplicate_count, count)

        if count < self.minimum_evidence_messages:
            return SourceAuditDecision.NEEDS_REVIEW, (
                _reason("review.insufficient_evidence", observed=count),
            )

        rejection_reasons: list[Mapping[str, Any]] = []
        if spam_ratio >= self.rejection_spam_ratio:
            rejection_reasons.append(
                _reason("rejected.spam_ratio", observed=spam_ratio)
            )
        if seller_ratio >= self.rejection_seller_ratio:
            rejection_reasons.append(
                _reason("rejected.seller_ratio", observed=seller_ratio)
            )
        if (
            count >= self.rejection_minimum_evidence_messages
            and classification.commercial_opportunity_count == 0
        ):
            rejection_reasons.append(
                _reason("rejected.no_commercial_opportunities", observed=0)
            )
        if rejection_reasons:
            return SourceAuditDecision.REJECTED, tuple(rejection_reasons)

        review_reasons: list[Mapping[str, Any]] = []
        if not classification.languages or classification.primary_language is None:
            review_reasons.append(_reason("review.language_unresolved"))
        if not classification.categories:
            review_reasons.append(_reason("review.category_unresolved"))
        if opportunity_yield < self.approval_minimum_yield:
            review_reasons.append(
                _reason("review.low_opportunity_yield", observed=opportunity_yield)
            )
        if seller_ratio > self.approval_maximum_seller_ratio:
            review_reasons.append(
                _reason("review.elevated_seller_ratio", observed=seller_ratio)
            )
        if spam_ratio > self.approval_maximum_spam_ratio:
            review_reasons.append(
                _reason("review.elevated_spam_ratio", observed=spam_ratio)
            )
        if duplicate_ratio > self.approval_maximum_duplicate_ratio:
            review_reasons.append(
                _reason("review.elevated_duplicate_ratio", observed=duplicate_ratio)
            )
        if review_reasons:
            return SourceAuditDecision.NEEDS_REVIEW, tuple(review_reasons)
        return SourceAuditDecision.APPROVED, (
            _reason("approved.thresholds_met", observed=opportunity_yield),
        )


@dataclass(frozen=True)
class SourceAuditRunResult:
    audit: SourceAuditRecord
    source: SourceRecord
    created: bool
    lifecycle_changed: bool


class SourceAuditPipeline:
    def __init__(
        self,
        database: Database,
        sampler: SourceAuditSampler,
        provider: SourceAuditProvider,
        *,
        policy: SourceAuditDecisionPolicy | None = None,
        audits: SourceAuditRepository | None = None,
        metrics: SourceMetricsRepository | None = None,
        sources: SourceRepository | None = None,
        lifecycle_actor_kind: str = "system",
        lifecycle_actor_id: str | None = None,
    ) -> None:
        if not isinstance(sampler, SourceAuditSampler):
            raise TypeError("sampler must be SourceAuditSampler")
        if not isinstance(provider, SourceAuditProvider):
            raise TypeError("provider must implement SourceAuditProvider")
        self._database = database
        self._sampler = sampler
        self._provider = provider
        self._policy = policy or SourceAuditDecisionPolicy()
        self._audits = audits or SourceAuditRepository()
        self._metrics = metrics or SourceMetricsRepository()
        self._sources = sources or SourceRepository()
        self._lifecycle_actor_kind = lifecycle_actor_kind.strip()
        self._lifecycle_actor_id = (
            None if lifecycle_actor_id is None else lifecycle_actor_id.strip()
        )
        if not self._lifecycle_actor_kind:
            raise ValueError("lifecycle_actor_kind must not be blank")
        if self._lifecycle_actor_kind not in _LIFECYCLE_ACTOR_KINDS:
            raise ValueError(
                "lifecycle_actor_kind must be one of: "
                + ", ".join(sorted(_LIFECYCLE_ACTOR_KINDS))
            )
        if self._lifecycle_actor_id == "":
            raise ValueError("lifecycle_actor_id must not be blank")

    async def run(
        self,
        target: SourceAuditTarget,
        *,
        audited_at: datetime,
    ) -> SourceAuditRunResult:
        return await self._execute(
            target,
            audited_at=audited_at,
            operational_reaudit=False,
        )

    async def re_audit(
        self,
        target: SourceAuditTarget,
        *,
        audited_at: datetime,
    ) -> SourceAuditRunResult:
        return await self._execute(
            target,
            audited_at=audited_at,
            operational_reaudit=True,
        )

    async def _execute(
        self,
        target: SourceAuditTarget,
        *,
        audited_at: datetime,
        operational_reaudit: bool,
    ) -> SourceAuditRunResult:
        async with self._database.connect() as connection:
            source = await self._sources.get(connection, target.source_id)

        sample = await self._sampler.sample(target, audited_at=audited_at)
        fingerprint = _sample_fingerprint(sample)
        audit_key = _audit_key(
            sample,
            fingerprint=fingerprint,
            provider=self._provider.name,
            model=self._provider.model,
            analyzer_version=self._provider.analyzer_version,
        )
        async with self._database.connect() as connection:
            existing = await self._audits.get_by_key(
                connection,
                source_id=target.source_id,
                audit_key=audit_key,
            )
            if existing is not None:
                return SourceAuditRunResult(
                    audit=existing,
                    source=await self._sources.get(connection, target.source_id),
                    created=False,
                    lifecycle_changed=False,
                )

        _require_source_state(source, operational_reaudit=operational_reaudit)
        classification = await self._provider.classify(sample)
        if classification.analyzed_message_count != sample.sampled_message_count:
            raise SourceAuditError(
                "AI analyzed_message_count does not match the bounded sampler output"
            )
        decision, reasons = self._policy.decide(classification)
        write = _audit_write(
            sample,
            classification,
            audit_key=audit_key,
            fingerprint=fingerprint,
            provider=self._provider,
            decision=decision,
            reasons=reasons,
            decision_policy=self._policy,
        )

        async with self._database.transaction() as connection:
            current = await self._sources.get(connection, target.source_id)
            _require_source_state(
                current,
                operational_reaudit=operational_reaudit,
            )
            outcome = await self._audits.record(connection, write)
            if not outcome.created:
                return SourceAuditRunResult(
                    audit=outcome.audit,
                    source=await self._sources.get(connection, target.source_id),
                    created=False,
                    lifecycle_changed=False,
                )

            if sample.sampled_message_count > 0:
                count = sample.sampled_message_count
                await self._metrics.record_quality_snapshot(
                    connection,
                    source_id=target.source_id,
                    audit_key=audit_key,
                    audited_at=sample.audited_at,
                    window_started_at=sample.window_started_at,
                    window_ended_at=sample.window_ended_at,
                    sampled_message_count=count,
                    opportunity_yield=_ratio(
                        classification.commercial_opportunity_count,
                        count,
                    ),
                    buyer_intent_ratio=_ratio(
                        classification.buyer_intent_count,
                        count,
                    ),
                    seller_ratio=_ratio(classification.seller_promotion_count, count),
                    spam_ratio=_ratio(classification.ads_spam_count, count),
                    duplicate_ratio=_ratio(classification.duplicate_count, count),
                )
            await self._metrics.record_audit_completed(
                connection,
                source_id=target.source_id,
                audited_at=sample.audited_at,
            )
            await self._audits.assign_taxonomy(
                connection,
                source_id=target.source_id,
                dimension="language",
                terms=[term.model_dump() for term in classification.languages],
            )
            await self._audits.assign_taxonomy(
                connection,
                source_id=target.source_id,
                dimension="category",
                terms=[term.model_dump() for term in classification.categories],
            )

            if operational_reaudit:
                if decision is SourceAuditDecision.APPROVED:
                    target_status = SourceStatus.APPROVED
                    await self._metrics.set_health_status(
                        connection,
                        source_id=target.source_id,
                        health_status=SourceHealthStatus.HEALTHY,
                        changed_at=sample.audited_at,
                    )
                else:
                    target_status = SourceStatus.PAUSED
                    await self._metrics.set_health_status(
                        connection,
                        source_id=target.source_id,
                        health_status=SourceHealthStatus.DEGRADED,
                        changed_at=sample.audited_at,
                        reason=(
                            "source re-audit: "
                            + ",".join(outcome.audit.reason_codes)
                        ),
                    )
            else:
                target_status = SourceStatus(decision.value)
            lifecycle_changed = current.lifecycle_status is not target_status
            if lifecycle_changed:
                current = await self._sources.transition(
                    connection,
                    target.source_id,
                    target_status,
                    reason=(
                        "source re-audit: "
                        if operational_reaudit
                        else "source audit: "
                    )
                    + ",".join(outcome.audit.reason_codes),
                    source_audit_id=outcome.audit.id,
                    actor_kind=self._lifecycle_actor_kind,
                    actor_id=self._lifecycle_actor_id,
                )
            return SourceAuditRunResult(
                audit=outcome.audit,
                source=current,
                created=True,
                lifecycle_changed=lifecycle_changed,
            )


def _require_source_state(
    source: SourceRecord,
    *,
    operational_reaudit: bool,
) -> None:
    allowed = (
        {SourceStatus.APPROVED}
        if operational_reaudit
        else {SourceStatus.CANDIDATE, SourceStatus.NEEDS_REVIEW}
    )
    if source.lifecycle_status not in allowed:
        mode = "operational re-audit" if operational_reaudit else "initial source audit"
        raise SourceAuditError(
            f"G2 {mode} does not accept {source.lifecycle_status.value} sources"
        )


def _audit_write(
    sample: SourceAuditSample,
    classification: SourceAuditClassification,
    *,
    audit_key: str,
    fingerprint: str,
    provider: SourceAuditProvider,
    decision: SourceAuditDecision,
    reasons: tuple[Mapping[str, Any], ...],
    decision_policy: SourceAuditDecisionPolicy,
) -> SourceAuditWrite:
    return SourceAuditWrite(
        source_id=sample.source_id,
        audit_key=audit_key,
        schema_version=classification.schema_version,
        provider=_provider_name(provider.name),
        model=_bounded_text(provider.model, "model", 128),
        analyzer_version=_safe_name(
            provider.analyzer_version,
            "analyzer_version",
            64,
        ),
        audited_at=sample.audited_at,
        window_started_at=sample.window_started_at,
        window_ended_at=sample.window_ended_at,
        sampled_from=sample.sampled_from,
        sampled_to=sample.sampled_to,
        sampled_message_count=sample.sampled_message_count,
        probe_message_count=sample.probe_message_count,
        expanded=sample.expanded,
        high_volume=sample.high_volume,
        sample_fingerprint=fingerprint,
        commercial_opportunity_count=classification.commercial_opportunity_count,
        buyer_intent_count=classification.buyer_intent_count,
        seller_promotion_count=classification.seller_promotion_count,
        ads_spam_count=classification.ads_spam_count,
        duplicate_count=classification.duplicate_count,
        content_mix=classification.content_mix,
        primary_language=classification.primary_language,
        languages=[term.model_dump() for term in classification.languages],
        categories=[term.model_dump() for term in classification.categories],
        decision_policy=decision_policy.to_payload(),
        decision=decision.value,
        reasons=reasons,
    )


def _sample_fingerprint(sample: SourceAuditSample) -> str:
    payload = {
        "source_id": sample.source_id,
        "window_started_at": sample.window_started_at.isoformat(),
        "window_ended_at": sample.window_ended_at.isoformat(),
        "messages": [
            {
                "message_id": message.message_id,
                "occurred_at": message.occurred_at.isoformat(),
                "text_sha256": hashlib.sha256(message.text.encode("utf-8")).hexdigest(),
            }
            for message in sample.messages
        ],
    }
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
    return hashlib.sha256(encoded).hexdigest()


def _audit_key(
    sample: SourceAuditSample,
    *,
    fingerprint: str,
    provider: str,
    model: str,
    analyzer_version: str,
) -> str:
    identity = "|".join(
        (
            str(sample.source_id),
            fingerprint,
            _provider_name(provider),
            _bounded_text(model, "model", 128),
            _safe_name(analyzer_version, "analyzer_version", 64),
            SOURCE_AUDIT_SCHEMA_VERSION,
        )
    )
    return "source-audit:" + hashlib.sha256(identity.encode()).hexdigest()


def _ratio(numerator: int, denominator: int) -> float:
    return 0.0 if denominator == 0 else numerator / denominator


def _reason(code: str, **details: Any) -> Mapping[str, Any]:
    return {"code": code, "details": details}


def _safe_name(value: str, field: str, max_length: int) -> str:
    normalized = _required_text(value, field).lower()
    if len(normalized) > max_length or not re.fullmatch(
        r"[a-z0-9][a-z0-9_.-]*",
        normalized,
    ):
        raise ValueError(f"{field} must be a safe identifier")
    return normalized


def _provider_name(value: str) -> str:
    normalized = _required_text(value, "provider").lower()
    if len(normalized) > 64 or not re.fullmatch(
        r"[a-z][a-z0-9_-]*",
        normalized,
    ):
        raise ValueError("provider must be a safe lowercase identifier")
    return normalized


def _required_text(value: str, field: str) -> str:
    normalized = value.strip()
    if not normalized:
        raise ValueError(f"{field} must not be blank")
    return normalized


def _bounded_text(value: str, field: str, max_length: int) -> str:
    normalized = _required_text(value, field)
    if len(normalized) > max_length:
        raise ValueError(f"{field} must not exceed {max_length} characters")
    return normalized


def _bounded_ratio(value: float, field: str, *, upper: float = 1) -> float:
    number = float(value)
    if not math.isfinite(number) or not 0 <= number <= upper:
        raise ValueError(f"{field} must be between 0 and {upper}")
    return number
