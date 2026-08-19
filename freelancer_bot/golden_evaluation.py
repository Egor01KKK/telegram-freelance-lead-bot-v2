from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from hashlib import sha256
import json
from pathlib import Path
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from .message_prefilter import MinimalAnalyzerInput
from .opportunity_analysis import (
    IntentStage,
    MarketDirection,
    OpportunityAnalysis,
    OpportunityAnalyzer,
    OpportunityType,
    opportunity_analysis_call_is_compatible,
)
from .opportunity_evaluation import (
    OpportunityEvalMessage,
    OpportunityEvalRoute,
)


GOLDEN_EVALUATION_SCHEMA_VERSION = "golden_evaluation.v1"
GOLDEN_ANNOTATION_POLICY_VERSION = "golden-annotation-policy.v1"
GOLDEN_TARGET_MIN_RECORDS = 500
GOLDEN_TARGET_MAX_RECORDS = 1000
GOLDEN_EVALUATED_FIELDS = (
    "is_opportunity",
    "market_direction",
    "intent_stage",
    "opportunity_type",
    "category",
    "role_title",
    "skills",
)


class GoldenDatasetError(RuntimeError):
    """The golden-evaluation workflow cannot safely consume this dataset."""


class _StrictGoldenModel(BaseModel):
    model_config = ConfigDict(
        extra="forbid",
        frozen=True,
        strict=True,
        str_strip_whitespace=True,
    )


class GoldenOpportunityLabel(_StrictGoldenModel):
    """Manually assigned opportunity and duplicate ground truth."""

    is_opportunity: bool
    market_direction: MarketDirection
    intent_stage: IntentStage
    opportunity_type: OpportunityType
    category: str | None
    role_title: str | None
    skills: tuple[str, ...]
    duplicate_label: Literal["unique", "duplicate"]
    duplicate_group_id: str | None = None

    @field_validator("category", "role_title")
    @classmethod
    def validate_optional_text(cls, value: str | None) -> str | None:
        if value is not None and not value:
            raise ValueError("optional annotation text cannot be empty")
        return value

    @field_validator("skills")
    @classmethod
    def validate_skills(cls, values: tuple[str, ...]) -> tuple[str, ...]:
        if len(values) > 64:
            raise ValueError("skills are bounded to 64 labels")
        normalized = tuple(value.strip() for value in values)
        if any(not value for value in normalized):
            raise ValueError("skills cannot contain empty labels")
        if len({value.casefold() for value in normalized}) != len(normalized):
            raise ValueError("skills must be unique")
        return normalized

    @field_validator("duplicate_group_id")
    @classmethod
    def validate_duplicate_group_id(cls, value: str | None) -> str | None:
        if value is not None and not value:
            raise ValueError("duplicate_group_id cannot be empty")
        return value

    @model_validator(mode="after")
    def validate_label_consistency(self) -> GoldenOpportunityLabel:
        if self.duplicate_label == "duplicate" and self.duplicate_group_id is None:
            raise ValueError("duplicate labels require duplicate_group_id")
        if self.duplicate_label == "unique" and self.duplicate_group_id is not None:
            raise ValueError("unique labels cannot have duplicate_group_id")
        if self.is_opportunity:
            if self.market_direction is not MarketDirection.BUYER_TO_SPECIALIST:
                raise ValueError("opportunity labels must be buyer_to_specialist")
            if self.intent_stage is IntentStage.NONE:
                raise ValueError("opportunity labels cannot have intent_stage=none")
        if (
            self.market_direction is MarketDirection.SPECIALIST_TO_BUYER
            and self.intent_stage is not IntentStage.NONE
        ):
            raise ValueError(
                "specialist_to_buyer labels must have intent_stage=none"
            )
        return self


class GoldenMessageAnnotation(_StrictGoldenModel):
    """One manually annotated captured message with auditable provenance."""

    record_id: str = Field(pattern=r"^[a-z0-9][a-z0-9._-]{0,127}$")
    message: OpportunityEvalMessage
    provenance: Literal["captured", "test_fixture"]
    capture_ref: str = Field(min_length=1, max_length=255)
    annotator_id: str = Field(pattern=r"^[a-z0-9][a-z0-9._-]{0,127}$")
    annotated_at: datetime
    label: GoldenOpportunityLabel

    @field_validator("annotated_at")
    @classmethod
    def validate_aware_annotation_time(cls, value: datetime) -> datetime:
        if value.utcoffset() is None:
            raise ValueError("annotation timestamp must include a timezone")
        return value

    def candidate(self) -> MinimalAnalyzerInput:
        return MinimalAnalyzerInput(current=self.message.analyzer_message(), parent=None)


class GoldenRelevanceAnnotation(_StrictGoldenModel):
    """A manually labelled profile-to-opportunity relevance pair."""

    pair_id: str = Field(pattern=r"^[a-z0-9][a-z0-9._-]{0,127}$")
    profile_id: str = Field(pattern=r"^[a-z0-9][a-z0-9._-]{0,127}$")
    profile_version: str = Field(pattern=r"^[a-z0-9][a-z0-9._-]{0,127}$")
    profile_text: str = Field(min_length=1, max_length=10000)
    opportunity_record_id: str = Field(pattern=r"^[a-z0-9][a-z0-9._-]{0,127}$")
    label: Literal["relevant", "not_relevant", "uncertain"]
    annotator_id: str = Field(pattern=r"^[a-z0-9][a-z0-9._-]{0,127}$")
    annotated_at: datetime

    @field_validator("annotated_at")
    @classmethod
    def validate_aware_annotation_time(cls, value: datetime) -> datetime:
        if value.utcoffset() is None:
            raise ValueError("annotation timestamp must include a timezone")
        return value


class GoldenDataset(_StrictGoldenModel):
    """Versioned real-world annotation store plus reproducibility metadata.

    Empty real-world datasets are allowed only while collection_status is
    ``in_progress``. This makes it possible to commit the workflow before
    captured data is available without misrepresenting synthetic fixtures as
    production evidence.
    """

    schema_version: Literal["golden_evaluation.v1"]
    dataset_version: str = Field(pattern=r"^[a-z0-9][a-z0-9._-]{0,127}$")
    dataset_kind: Literal["real_world", "test_fixture"]
    collection_status: Literal["in_progress", "ready"]
    annotation_policy_version: str = Field(
        pattern=r"^[a-z0-9][a-z0-9._-]{0,127}$"
    )
    target_min_records: int = Field(
        default=GOLDEN_TARGET_MIN_RECORDS,
        ge=1,
    )
    target_max_records: int = Field(
        default=GOLDEN_TARGET_MAX_RECORDS,
        ge=1,
    )
    messages: tuple[GoldenMessageAnnotation, ...] = Field(default_factory=tuple)
    relevance_pairs: tuple[GoldenRelevanceAnnotation, ...] = Field(
        default_factory=tuple
    )

    @model_validator(mode="after")
    def validate_dataset(self) -> GoldenDataset:
        if self.target_min_records > self.target_max_records:
            raise ValueError("target_min_records cannot exceed target_max_records")
        if self.dataset_kind == "real_world":
            expected_provenance = "captured"
        else:
            expected_provenance = "test_fixture"
        if any(
            item.provenance != expected_provenance for item in self.messages
        ):
            raise ValueError(
                "dataset_kind and message provenance must agree; synthetic/test "
                "fixtures cannot enter the real-world dataset"
            )
        if self.collection_status == "ready":
            if not self.messages:
                raise ValueError("ready datasets must contain annotated messages")
            if (
                self.dataset_kind == "real_world"
                and len(self.messages) < self.target_min_records
            ):
                raise ValueError(
                    "ready real-world datasets must meet target_min_records"
                )

        message_ids = [item.record_id for item in self.messages]
        if len(message_ids) != len(set(message_ids)):
            raise ValueError("golden record IDs must be unique")

        duplicate_groups: dict[str, list[GoldenMessageAnnotation]] = {}
        for item in self.messages:
            group_id = item.label.duplicate_group_id
            if group_id is not None:
                duplicate_groups.setdefault(group_id, []).append(item)
        if any(len(items) < 2 for items in duplicate_groups.values()):
            raise ValueError("duplicate groups must contain at least two records")

        pair_ids = [pair.pair_id for pair in self.relevance_pairs]
        if len(pair_ids) != len(set(pair_ids)):
            raise ValueError("relevance pair IDs must be unique")
        message_id_set = set(message_ids)
        if any(
            pair.opportunity_record_id not in message_id_set
            for pair in self.relevance_pairs
        ):
            raise ValueError("relevance pairs must reference known golden records")
        return self

    @property
    def fingerprint(self) -> str:
        canonical = json.dumps(
            self.model_dump(mode="json", exclude_none=False),
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        )
        return sha256(canonical.encode("utf-8")).hexdigest()

    @property
    def target_reached(self) -> bool:
        return self.target_min_records <= len(self.messages) <= self.target_max_records

    @property
    def duplicate_group_count(self) -> int:
        return len(
            {
                item.label.duplicate_group_id
                for item in self.messages
                if item.label.duplicate_group_id is not None
            }
        )


@dataclass(frozen=True)
class GoldenEvaluationMismatch:
    record_id: str
    fields: tuple[str, ...]


@dataclass(frozen=True)
class GoldenEvaluationReport:
    dataset_version: str
    dataset_fingerprint: str
    dataset_kind: str
    case_count: int
    relevance_pair_count: int
    duplicate_group_count: int
    true_positive: int
    false_positive: int
    true_negative: int
    false_negative: int
    precision: float
    recall: float
    direction_accuracy: float
    intent_accuracy: float
    type_accuracy: float
    label_accuracy: float
    routes: tuple[OpportunityEvalRoute, ...]
    mismatches: tuple[GoldenEvaluationMismatch, ...]


def load_golden_dataset(
    path: Path,
    *,
    allow_test_fixture: bool = False,
) -> GoldenDataset:
    dataset = GoldenDataset.model_validate_json(
        path.read_text(encoding="utf-8"),
        strict=True,
    )
    if dataset.dataset_kind == "test_fixture" and not allow_test_fixture:
        raise GoldenDatasetError(
            "test fixtures are not real-world evidence; pass "
            "allow_test_fixture=True only from tests"
        )
    return dataset


def create_golden_dataset_template(
    *,
    dataset_version: str,
    annotation_policy_version: str = GOLDEN_ANNOTATION_POLICY_VERSION,
) -> GoldenDataset:
    """Create an empty real-world collection envelope without fake labels."""

    return GoldenDataset(
        schema_version=GOLDEN_EVALUATION_SCHEMA_VERSION,
        dataset_version=dataset_version,
        dataset_kind="real_world",
        collection_status="in_progress",
        annotation_policy_version=annotation_policy_version,
        target_min_records=GOLDEN_TARGET_MIN_RECORDS,
        target_max_records=GOLDEN_TARGET_MAX_RECORDS,
        messages=(),
        relevance_pairs=(),
    )


def write_golden_dataset(path: Path, dataset: GoldenDataset) -> None:
    """Persist a validated annotation envelope with deterministic formatting."""

    dataset = GoldenDataset.model_validate(dataset.model_dump(mode="python"))
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        dataset.model_dump_json(indent=2, exclude_none=False) + "\n",
        encoding="utf-8",
    )


async def evaluate_golden_dataset(
    analyzer: OpportunityAnalyzer,
    dataset: GoldenDataset,
    *,
    allow_test_fixture: bool = False,
) -> GoldenEvaluationReport:
    """Run the provider-neutral opportunity slice against stored labels.

    This runner evaluates message labels only. Duplicate groups and relevance
    pairs are retained and counted for later evaluation tasks; no synthetic
    or unlabelled prediction is invented for them here.
    """

    if dataset.dataset_kind == "test_fixture" and not allow_test_fixture:
        raise GoldenDatasetError(
            "test fixtures cannot be used as real-world quality evidence"
        )
    if not dataset.messages:
        raise GoldenDatasetError("cannot evaluate an empty golden dataset")

    true_positive = false_positive = true_negative = false_negative = 0
    direction_matches = intent_matches = type_matches = 0
    field_matches = exact_matches = 0
    routes: set[OpportunityEvalRoute] = set()
    mismatches: list[GoldenEvaluationMismatch] = []

    for record in dataset.messages:
        call = await analyzer.analyze(record.candidate())
        if not opportunity_analysis_call_is_compatible(analyzer, call):
            raise GoldenDatasetError(
                "evaluation call metadata does not match the configured analyzer"
            )
        actual = call.analysis
        expected = record.label

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
        mismatched_fields = tuple(
            field
            for field in GOLDEN_EVALUATED_FIELDS
            if getattr(actual, field) != getattr(expected, field)
        )
        field_matches += len(GOLDEN_EVALUATED_FIELDS) - len(mismatched_fields)
        if not mismatched_fields:
            exact_matches += 1
        else:
            mismatches.append(
                GoldenEvaluationMismatch(
                    record_id=record.record_id,
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

    count = len(dataset.messages)
    return GoldenEvaluationReport(
        dataset_version=dataset.dataset_version,
        dataset_fingerprint=dataset.fingerprint,
        dataset_kind=dataset.dataset_kind,
        case_count=count,
        relevance_pair_count=len(dataset.relevance_pairs),
        duplicate_group_count=dataset.duplicate_group_count,
        true_positive=true_positive,
        false_positive=false_positive,
        true_negative=true_negative,
        false_negative=false_negative,
        precision=_ratio(true_positive, true_positive + false_positive),
        recall=_ratio(true_positive, true_positive + false_negative),
        direction_accuracy=direction_matches / count,
        intent_accuracy=intent_matches / count,
        type_accuracy=type_matches / count,
        label_accuracy=exact_matches / count,
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
