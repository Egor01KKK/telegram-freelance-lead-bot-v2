from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from hashlib import sha256
import json
from pathlib import Path
from typing import Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from .message_prefilter import AnalyzerMessage, MinimalAnalyzerInput
from .opportunity_analysis import (
    OpportunityAnalysis,
    OpportunityAnalysisOutputError,
    OpportunityAnalyzer,
    opportunity_analysis_call_is_compatible,
    validate_opportunity_analysis_grounding,
)


OPPORTUNITY_EVALUATION_SCHEMA_VERSION = "opportunity_evaluation.v1"
EVALUATED_FIELDS = (
    "is_opportunity",
    "market_direction",
    "intent_stage",
    "opportunity_type",
    "category",
    "role_title",
    "skills",
    "task_summary",
    "budget",
    "work",
    "language",
    "contact",
    "quality",
    "red_flags",
)


class OpportunityEvaluationError(RuntimeError):
    pass


class _StrictEvaluationModel(BaseModel):
    model_config = ConfigDict(
        extra="forbid",
        frozen=True,
        strict=True,
        str_strip_whitespace=True,
    )


class OpportunityEvalMessage(_StrictEvaluationModel):
    raw_message_id: UUID
    source_id: int = Field(gt=0)
    external_source_id: str = Field(min_length=1, max_length=255)
    external_message_id: int = Field(gt=0)
    message_date: datetime
    message_url: str = Field(min_length=1, max_length=2048)
    content: str = Field(min_length=1)

    @field_validator("message_date")
    @classmethod
    def validate_aware_message_date(cls, value: datetime) -> datetime:
        if value.utcoffset() is None:
            raise ValueError("evaluation message date must include a timezone")
        return value

    def analyzer_message(self) -> AnalyzerMessage:
        return AnalyzerMessage(
            raw_message_id=self.raw_message_id,
            source_id=self.source_id,
            external_source_id=self.external_source_id,
            external_message_id=self.external_message_id,
            message_date=self.message_date,
            message_url=self.message_url,
            content=self.content,
        )


class OpportunityEvalCase(_StrictEvaluationModel):
    case_id: str = Field(pattern=r"^[a-z0-9][a-z0-9._-]{0,99}$")
    current: OpportunityEvalMessage
    parent: OpportunityEvalMessage | None
    expected: OpportunityAnalysis

    @model_validator(mode="after")
    def validate_context_and_grounding(self) -> OpportunityEvalCase:
        candidate = self.candidate()
        if candidate.parent is not None and (
            candidate.parent.source_id != candidate.current.source_id
        ):
            raise ValueError("direct parent must belong to the current source")
        try:
            validate_opportunity_analysis_grounding(self.expected, candidate)
        except OpportunityAnalysisOutputError:
            raise ValueError(
                "expected contacts must be grounded in the bounded case context"
            ) from None
        return self

    def candidate(self) -> MinimalAnalyzerInput:
        return MinimalAnalyzerInput(
            current=self.current.analyzer_message(),
            parent=None if self.parent is None else self.parent.analyzer_message(),
        )


class OpportunityEvalDataset(_StrictEvaluationModel):
    schema_version: Literal["opportunity_evaluation.v1"]
    dataset_version: str = Field(pattern=r"^[a-z0-9][a-z0-9._-]{0,99}$")
    cases: tuple[OpportunityEvalCase, ...] = Field(min_length=1)

    @model_validator(mode="after")
    def validate_unique_cases(self) -> OpportunityEvalDataset:
        case_ids = [case.case_id for case in self.cases]
        if len(case_ids) != len(set(case_ids)):
            raise ValueError("evaluation case IDs must be unique")
        raw_ids = [case.current.raw_message_id for case in self.cases]
        if len(raw_ids) != len(set(raw_ids)):
            raise ValueError("evaluation current raw-message IDs must be unique")
        return self

    @property
    def fingerprint(self) -> str:
        canonical = self.model_dump_json(exclude_none=False)
        return sha256(canonical.encode("utf-8")).hexdigest()


@dataclass(frozen=True)
class OpportunityEvalMismatch:
    case_id: str
    fields: tuple[str, ...]


@dataclass(frozen=True)
class OpportunityEvalRoute:
    provider: str
    requested_model: str
    response_model: str
    analyzer_version: str
    prompt_version: str
    schema_version: str
    routing_version: str


@dataclass(frozen=True)
class OpportunityEvalReport:
    dataset_version: str
    dataset_fingerprint: str
    case_count: int
    true_positive: int
    false_positive: int
    true_negative: int
    false_negative: int
    precision: float
    recall: float
    direction_accuracy: float
    intent_accuracy: float
    type_accuracy: float
    structured_field_accuracy: float
    exact_case_accuracy: float
    mean_analysis_confidence: float
    minimum_analysis_confidence: float
    routes: tuple[OpportunityEvalRoute, ...]
    mismatches: tuple[OpportunityEvalMismatch, ...]


def load_opportunity_eval_dataset(path: Path) -> OpportunityEvalDataset:
    return OpportunityEvalDataset.model_validate_json(
        path.read_text(encoding="utf-8"),
        strict=True,
    )


async def evaluate_opportunity_analyzer(
    analyzer: OpportunityAnalyzer,
    dataset: OpportunityEvalDataset,
) -> OpportunityEvalReport:
    true_positive = false_positive = true_negative = false_negative = 0
    direction_matches = intent_matches = type_matches = 0
    field_matches = exact_matches = 0
    confidence_total = 0.0
    minimum_confidence = 1.0
    routes: set[OpportunityEvalRoute] = set()
    mismatches: list[OpportunityEvalMismatch] = []

    for case in dataset.cases:
        candidate = case.candidate()
        call = await analyzer.analyze(candidate)
        if not opportunity_analysis_call_is_compatible(analyzer, call):
            raise OpportunityEvaluationError(
                "evaluation call metadata does not match the configured analyzer"
            )
        actual = call.analysis
        validate_opportunity_analysis_grounding(actual, candidate)
        expected = case.expected

        if actual.is_opportunity and expected.is_opportunity:
            true_positive += 1
        elif actual.is_opportunity:
            false_positive += 1
        elif expected.is_opportunity:
            false_negative += 1
        else:
            true_negative += 1

        direction_matches += actual.market_direction == expected.market_direction
        intent_matches += actual.intent_stage == expected.intent_stage
        type_matches += actual.opportunity_type == expected.opportunity_type
        confidence_total += actual.confidence
        minimum_confidence = min(minimum_confidence, actual.confidence)

        mismatched_fields = tuple(
            field
            for field in EVALUATED_FIELDS
            if getattr(actual, field) != getattr(expected, field)
        )
        field_matches += len(EVALUATED_FIELDS) - len(mismatched_fields)
        if not mismatched_fields:
            exact_matches += 1
        else:
            mismatches.append(
                OpportunityEvalMismatch(
                    case_id=case.case_id,
                    fields=mismatched_fields,
                )
            )
        routes.add(
            OpportunityEvalRoute(
                provider=call.provider,
                requested_model=call.requested_model,
                response_model=call.response_model,
                analyzer_version=call.analyzer_version,
                prompt_version=call.prompt_version,
                schema_version=call.schema_version,
                routing_version=call.routing_version,
            )
        )

    count = len(dataset.cases)
    return OpportunityEvalReport(
        dataset_version=dataset.dataset_version,
        dataset_fingerprint=dataset.fingerprint,
        case_count=count,
        true_positive=true_positive,
        false_positive=false_positive,
        true_negative=true_negative,
        false_negative=false_negative,
        precision=_ratio(true_positive, true_positive + false_positive),
        recall=_ratio(true_positive, true_positive + false_negative),
        direction_accuracy=direction_matches / count,
        intent_accuracy=intent_matches / count,
        type_accuracy=type_matches / count,
        structured_field_accuracy=field_matches / (count * len(EVALUATED_FIELDS)),
        exact_case_accuracy=exact_matches / count,
        mean_analysis_confidence=confidence_total / count,
        minimum_analysis_confidence=minimum_confidence,
        routes=tuple(sorted(routes, key=_route_sort_key)),
        mismatches=tuple(mismatches),
    )


def _ratio(numerator: int, denominator: int) -> float:
    return 0.0 if denominator == 0 else numerator / denominator


def _route_sort_key(route: OpportunityEvalRoute) -> tuple[str, ...]:
    return (
        route.provider,
        route.requested_model,
        route.response_model,
        route.analyzer_version,
        route.prompt_version,
        route.schema_version,
        route.routing_version,
    )
