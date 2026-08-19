from __future__ import annotations

import argparse
from dataclasses import dataclass, replace
from datetime import datetime, timezone
from decimal import Decimal
import json
import math
from pathlib import Path
from time import perf_counter
from typing import Any, Literal, Mapping, Sequence

from .ai_telemetry import AIModelPrice
from .golden_evaluation import (
    GoldenDataset,
    GoldenEvaluationReport,
    evaluate_golden_dataset,
)
from .opportunity_analysis import (
    MarketDirection,
    OpportunityAnalysisCall,
    OpportunityAnalyzer,
)
from .opportunity_evaluation import (
    OpportunityEvalDataset,
    OpportunityEvalReport,
    OpportunityEvalRoute,
    evaluate_opportunity_analyzer,
)
from .match_decisions import MatchTraceDraft


AUTOMATED_EVALUATION_SCHEMA_VERSION = "automated_eval_report.v1"
AUTOMATED_EVALUATION_RUNNER_VERSION = "automated-eval-runner.v1"

OPPORTUNITY_PRECISION_TARGET = Decimal("0.90")
OPPORTUNITY_RECALL_TARGET = Decimal("0.85")
DUPLICATE_DELIVERY_RATE_MAX_EXCLUSIVE = Decimal("0.02")
PERSONAL_POSITIVE_RELEVANCE_TARGET = Decimal("0.75")

_RUN_ID_PATTERN = r"^[a-z0-9][a-z0-9._-]{0,127}$"
_VERSION_PATTERN = r"^[a-z0-9][a-z0-9._-]{0,127}$"
_REQUIRED_GATE_METRICS = frozenset(
    {
        "opportunity_precision",
        "opportunity_recall",
        "duplicate_user_delivery_rate",
        "personal_positive_relevance",
    }
)


class EvaluationRunnerError(RuntimeError):
    """The automated evaluation cannot produce safe release evidence."""


class EvaluationEvidenceKind(str):
    REAL_WORLD = "real_world"
    TEST_FIXTURE = "test_fixture"


class EvaluationGateStatus(str):
    PASSED = "passed"
    FAILED = "failed"
    BLOCKED = "blocked"
    NOT_MEASURED = "not_measured"
    OBSERVED = "observed"


Comparator = Literal["gte", "gt", "lt", "lte", "none"]


@dataclass(frozen=True)
class EvaluationThresholds:
    """Pack-defined release thresholds with no hidden evaluator tuning."""

    opportunity_precision_min: Decimal = OPPORTUNITY_PRECISION_TARGET
    opportunity_recall_min: Decimal = OPPORTUNITY_RECALL_TARGET
    duplicate_delivery_rate_max_exclusive: Decimal = (
        DUPLICATE_DELIVERY_RATE_MAX_EXCLUSIVE
    )
    personal_positive_relevance_min: Decimal = (
        PERSONAL_POSITIVE_RELEVANCE_TARGET
    )
    prefilter_recall_min: Decimal | None = None

    def __post_init__(self) -> None:
        for value, name in (
            (self.opportunity_precision_min, "opportunity_precision_min"),
            (self.opportunity_recall_min, "opportunity_recall_min"),
            (
                self.duplicate_delivery_rate_max_exclusive,
                "duplicate_delivery_rate_max_exclusive",
            ),
            (
                self.personal_positive_relevance_min,
                "personal_positive_relevance_min",
            ),
        ):
            value = _decimal(value, name)
            if not Decimal("0") <= value <= Decimal("1"):
                raise ValueError(f"{name} must be between 0 and 1")
        if self.prefilter_recall_min is not None:
            value = _decimal(self.prefilter_recall_min, "prefilter_recall_min")
            if not Decimal("0") <= value <= Decimal("1"):
                raise ValueError("prefilter_recall_min must be between 0 and 1")

    def as_dict(self) -> dict[str, float | None]:
        return {
            "opportunity_precision_min": float(self.opportunity_precision_min),
            "opportunity_recall_min": float(self.opportunity_recall_min),
            "duplicate_delivery_rate_max_exclusive": float(
                self.duplicate_delivery_rate_max_exclusive
            ),
            "personal_positive_relevance_min": float(
                self.personal_positive_relevance_min
            ),
            "prefilter_recall_min": (
                None
                if self.prefilter_recall_min is None
                else float(self.prefilter_recall_min)
            ),
        }


@dataclass(frozen=True)
class EvaluationVersionIdentity:
    """All route/policy identities needed to compare two evaluation runs."""

    routes: tuple[OpportunityEvalRoute, ...]
    matching_algorithm_version: str | None = None
    matching_policy_version: str | None = None
    semantic_matching_version: str | None = None
    semantic_policy_version: str | None = None
    pricing_version: str | None = None

    def __post_init__(self) -> None:
        if not self.routes:
            raise ValueError("an evaluation run must record at least one route")
        for name, value in (
            ("matching_algorithm_version", self.matching_algorithm_version),
            ("matching_policy_version", self.matching_policy_version),
            ("semantic_matching_version", self.semantic_matching_version),
            ("semantic_policy_version", self.semantic_policy_version),
            ("pricing_version", self.pricing_version),
        ):
            if value is not None:
                _validate_version(value, name)

    @classmethod
    def from_routes(
        cls,
        routes: Sequence[OpportunityEvalRoute],
        *,
        matching_algorithm_version: str | None = None,
        matching_policy_version: str | None = None,
        semantic_matching_version: str | None = None,
        semantic_policy_version: str | None = None,
        pricing_version: str | None = None,
    ) -> EvaluationVersionIdentity:
        return cls(
            routes=tuple(routes),
            matching_algorithm_version=matching_algorithm_version,
            matching_policy_version=matching_policy_version,
            semantic_matching_version=semantic_matching_version,
            semantic_policy_version=semantic_policy_version,
            pricing_version=pricing_version,
        )

    def as_dict(self) -> dict[str, object]:
        return {
            "routes": [
                {
                    "provider": route.provider,
                    "requested_model": route.requested_model,
                    "response_model": route.response_model,
                    "analyzer_version": route.analyzer_version,
                    "prompt_version": route.prompt_version,
                    "schema_version": route.schema_version,
                    "routing_version": route.routing_version,
                }
                for route in self.routes
            ],
            "matching_algorithm_version": self.matching_algorithm_version,
            "matching_policy_version": self.matching_policy_version,
            "semantic_matching_version": self.semantic_matching_version,
            "semantic_policy_version": self.semantic_policy_version,
            "pricing_version": self.pricing_version,
        }

    @property
    def fingerprint(self) -> str:
        return _sha256(self.as_dict())


@dataclass(frozen=True)
class EvaluationCostLatency:
    latency_ms: int | None = None
    input_tokens: int | None = None
    output_tokens: int | None = None
    total_tokens: int | None = None
    estimated_cost_usd: Decimal | None = None
    pricing_version: str | None = None

    def __post_init__(self) -> None:
        for value, name in (
            (self.latency_ms, "latency_ms"),
            (self.input_tokens, "input_tokens"),
            (self.output_tokens, "output_tokens"),
            (self.total_tokens, "total_tokens"),
        ):
            if value is not None and (not isinstance(value, int) or value < 0):
                raise ValueError(f"{name} must be a nonnegative integer")
        if (
            self.input_tokens is not None
            and self.output_tokens is not None
            and self.total_tokens is not None
            and self.total_tokens != self.input_tokens + self.output_tokens
        ):
            raise ValueError("total_tokens must equal input_tokens + output_tokens")
        if self.estimated_cost_usd is not None:
            cost = _decimal(self.estimated_cost_usd, "estimated_cost_usd")
            if cost < 0:
                raise ValueError("estimated_cost_usd must be nonnegative")
            if self.pricing_version is None:
                raise ValueError("pricing_version is required with estimated cost")

    @classmethod
    def from_calls(
        cls,
        calls: Sequence[OpportunityAnalysisCall],
        latencies_ms: Sequence[int],
        *,
        price: AIModelPrice | None = None,
    ) -> EvaluationCostLatency:
        if len(calls) != len(latencies_ms):
            raise ValueError("call and latency observations must have equal length")
        input_tokens = sum(call.usage.input_tokens for call in calls)
        output_tokens = sum(call.usage.output_tokens for call in calls)
        total_tokens = sum(call.usage.total_tokens for call in calls)
        cost = None
        pricing_version = None
        if price is not None:
            pricing_version = price.pricing_version
            cost = (
                Decimal(input_tokens) * price.input_usd_per_million
                + Decimal(output_tokens) * price.output_usd_per_million
            ) / Decimal(1_000_000)
            cost = cost.quantize(Decimal("0.000000001"))
        return cls(
            latency_ms=sum(latencies_ms),
            input_tokens=input_tokens,
            output_tokens=output_tokens,
            total_tokens=total_tokens,
            estimated_cost_usd=cost,
            pricing_version=pricing_version,
        )

    def as_dict(self) -> dict[str, object]:
        return {
            "latency_ms": self.latency_ms,
            "input_tokens": self.input_tokens,
            "output_tokens": self.output_tokens,
            "total_tokens": self.total_tokens,
            "estimated_cost_usd": (
                None
                if self.estimated_cost_usd is None
                else str(self.estimated_cost_usd)
            ),
            "pricing_version": self.pricing_version,
        }


@dataclass(frozen=True)
class MetricObservation:
    """A metric before provenance-aware release-gate status is applied."""

    name: str
    value: Decimal | float | None
    numerator: int | None
    denominator: int | None
    target: Decimal | float | None
    comparator: Comparator
    description: str
    gate: bool = True

    def __post_init__(self) -> None:
        _validate_metric_name(self.name)
        if self.numerator is not None and self.numerator < 0:
            raise ValueError("metric numerator cannot be negative")
        if self.denominator is not None and self.denominator < 0:
            raise ValueError("metric denominator cannot be negative")
        if (
            self.numerator is not None
            and self.denominator is not None
            and self.numerator > self.denominator
        ):
            raise ValueError("metric numerator cannot exceed denominator")
        if self.comparator == "none" and self.target is not None:
            raise ValueError("informational metrics cannot have a target")
        if self.comparator != "none" and self.target is None:
            raise ValueError("gated metrics require a target")


@dataclass(frozen=True)
class EvaluationMetric:
    name: str
    value: float | None
    numerator: int | None
    denominator: int | None
    target: float | None
    comparator: Comparator
    description: str
    gate: bool
    threshold_met: bool | None
    status: str

    def as_dict(self) -> dict[str, object]:
        return {
            "name": self.name,
            "value": self.value,
            "numerator": self.numerator,
            "denominator": self.denominator,
            "target": self.target,
            "comparator": self.comparator,
            "description": self.description,
            "gate": self.gate,
            "threshold_met": self.threshold_met,
            "status": self.status,
        }

    def as_observation(self) -> MetricObservation:
        return MetricObservation(
            name=self.name,
            value=self.value,
            numerator=self.numerator,
            denominator=self.denominator,
            target=self.target,
            comparator=self.comparator,
            description=self.description,
            gate=self.gate,
        )


@dataclass(frozen=True)
class DuplicateDeliveryCase:
    case_id: str
    profile_id: str
    scenario: Literal["duplicate", "distinct"]
    delivered_opportunity_ids: tuple[str, ...]
    predicted_same_opportunity: bool

    def __post_init__(self) -> None:
        _validate_identifier(self.case_id, "case_id")
        _validate_identifier(self.profile_id, "profile_id")
        if not self.delivered_opportunity_ids:
            raise ValueError("duplicate evaluation cases need delivery observations")
        if any(not value.strip() for value in self.delivered_opportunity_ids):
            raise ValueError("delivered opportunity IDs cannot be empty")
        if len(set(self.delivered_opportunity_ids)) != len(
            self.delivered_opportunity_ids
        ):
            raise ValueError("delivered opportunity IDs must be unique per case")


@dataclass(frozen=True)
class DuplicateDeliveryMeasurement:
    duplicate_delivery_count: int
    duplicate_delivery_denominator: int
    duplicate_delivery_rate: Decimal | None
    false_merge_count: int
    false_merge_denominator: int

    @property
    def false_merge_rate(self) -> Decimal | None:
        if self.false_merge_denominator == 0:
            return None
        return _ratio(self.false_merge_count, self.false_merge_denominator)

    def observations(
        self,
        *,
        thresholds: EvaluationThresholds,
    ) -> tuple[MetricObservation, ...]:
        return (
            MetricObservation(
                name="duplicate_user_delivery_rate",
                value=self.duplicate_delivery_rate,
                numerator=self.duplicate_delivery_count,
                denominator=self.duplicate_delivery_denominator,
                target=thresholds.duplicate_delivery_rate_max_exclusive,
                comparator="lt",
                description=(
                    "Extra deliveries beyond the first per labelled duplicate "
                    "group divided by deliveries in duplicate scenarios."
                ),
            ),
            MetricObservation(
                name="dedup_false_merge_rate",
                value=self.false_merge_rate,
                numerator=self.false_merge_count,
                denominator=self.false_merge_denominator,
                target=None,
                comparator="none",
                description=(
                    "Distinct labelled pairs incorrectly treated as one "
                    "canonical opportunity; reported separately from delivery rate."
                ),
                gate=False,
            ),
        )


@dataclass(frozen=True)
class RelevanceCase:
    pair_id: str
    label: Literal["relevant", "not_relevant", "uncertain"]
    predicted_relevant: bool

    def __post_init__(self) -> None:
        _validate_identifier(self.pair_id, "pair_id")


@dataclass(frozen=True)
class RelevanceMeasurement:
    true_positive: int
    false_positive: int
    true_negative: int
    false_negative: int
    uncertain_count: int

    @property
    def labelled_count(self) -> int:
        return (
            self.true_positive
            + self.false_positive
            + self.true_negative
            + self.false_negative
        )

    @property
    def positive_relevance(self) -> Decimal | None:
        return _optional_ratio(
            self.true_positive,
            self.true_positive + self.false_positive,
        )

    @property
    def recall(self) -> Decimal | None:
        return _optional_ratio(
            self.true_positive,
            self.true_positive + self.false_negative,
        )

    @property
    def accuracy(self) -> Decimal | None:
        return _optional_ratio(
            self.true_positive + self.true_negative,
            self.labelled_count,
        )

    def observations(
        self,
        *,
        thresholds: EvaluationThresholds,
    ) -> tuple[MetricObservation, ...]:
        return (
            MetricObservation(
                name="personal_positive_relevance",
                value=self.positive_relevance,
                numerator=self.true_positive,
                denominator=self.true_positive + self.false_positive,
                target=thresholds.personal_positive_relevance_min,
                comparator="gte",
                description=(
                    "Labelled relevant pairs among predicted positive pairs; "
                    "uncertain labels are excluded from the denominator."
                ),
            ),
            MetricObservation(
                name="personal_relevance_recall",
                value=self.recall,
                numerator=self.true_positive,
                denominator=self.true_positive + self.false_negative,
                target=None,
                comparator="none",
                description=(
                    "Labelled relevant pairs retrieved by the matching configuration."
                ),
                gate=False,
            ),
            MetricObservation(
                name="personal_relevance_label_accuracy",
                value=self.accuracy,
                numerator=self.true_positive + self.true_negative,
                denominator=self.labelled_count,
                target=None,
                comparator="none",
                description="Accuracy over labelled non-uncertain relevance pairs.",
                gate=False,
            ),
        )


@dataclass(frozen=True)
class PrefilterCase:
    case_id: str
    expected_candidate: bool
    actual_passed: bool
    rejection_reasons: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        _validate_identifier(self.case_id, "case_id")
        if self.actual_passed and self.rejection_reasons:
            raise ValueError("passed prefilter cases cannot have rejection reasons")
        if not self.actual_passed and not self.rejection_reasons:
            raise ValueError("rejected prefilter cases need documented reasons")


@dataclass(frozen=True)
class PrefilterMeasurement:
    candidate_recall: Decimal | None
    candidate_count: int
    candidate_retrieved_count: int
    rejection_reason_coverage: Decimal | None
    rejected_count: int
    documented_rejected_count: int

    def observations(
        self,
        *,
        thresholds: EvaluationThresholds,
    ) -> tuple[MetricObservation, ...]:
        target = thresholds.prefilter_recall_min
        return (
            MetricObservation(
                name="prefilter_candidate_recall",
                value=self.candidate_recall,
                numerator=self.candidate_retrieved_count,
                denominator=self.candidate_count,
                target=target,
                comparator="gte" if target is not None else "none",
                description=(
                    "Expected candidate messages that reached analysis; no "
                    "default numeric target is invented when the Pack leaves it "
                    "dataset-specific."
                ),
                gate=target is not None,
            ),
            MetricObservation(
                name="prefilter_rejection_reason_coverage",
                value=self.rejection_reason_coverage,
                numerator=(
                    self.documented_rejected_count
                    if self.rejection_reason_coverage is not None
                    else None
                ),
                denominator=self.rejected_count,
                target=None,
                comparator="none",
                description=(
                    "Every rejected prefilter case must retain at least one "
                    "stable rejection reason."
                ),
                gate=False,
            ),
        )


@dataclass(frozen=True)
class MatchEvaluationCase:
    """One labelled profile/opportunity observation derived from a match trace."""

    pair_id: str
    label: Literal["relevant", "not_relevant", "uncertain"]
    predicted_relevant: bool
    hard_filter_eligible: bool
    hard_filter_respected: bool
    structured_predicted_relevant: bool | None
    semantic_predicted_relevant: bool | None
    trace_explainable: bool
    source_quality_hard_filter_respected: bool
    opportunity_quality_penalty_present: bool
    version_metadata_complete: bool
    feedback_signal_present: bool = False

    def __post_init__(self) -> None:
        _validate_identifier(self.pair_id, "pair_id")

    @classmethod
    def from_trace(
        cls,
        trace: MatchTraceDraft,
        *,
        label: Literal["relevant", "not_relevant", "uncertain"],
        feedback_signal_present: bool = False,
    ) -> MatchEvaluationCase:
        structured_predicted = (
            trace.user_relevance_score is not None
            and trace.user_relevance_score >= trace.minimum_relevance_threshold
        )
        semantic_predicted = (
            trace.combined_relevance_score is not None
            and trace.combined_relevance_score >= trace.minimum_relevance_threshold
        )
        hard_filter_respected = (
            not trace.eligible or trace.hard_filter_eligible
        )
        trace_explainable = bool(
            trace.decision_schema_version
            and trace.decision_algorithm_version
            and trace.decision_policy_version
            and trace.filter_version
            and (
                trace.hard_filter_eligible
                or trace.hard_filter_reasons
                or trace.decision_code.value
            )
        )
        version_metadata_complete = all(
            value
            for value in (
                trace.decision_schema_version,
                trace.decision_algorithm_version,
                trace.decision_policy_version,
                trace.filter_version,
            )
        )
        return cls(
            pair_id=f"{trace.search_profile_id}.{trace.opportunity_id}",
            label=label,
            predicted_relevant=trace.eligible,
            hard_filter_eligible=trace.hard_filter_eligible,
            hard_filter_respected=hard_filter_respected,
            structured_predicted_relevant=structured_predicted,
            semantic_predicted_relevant=semantic_predicted,
            trace_explainable=trace_explainable,
            source_quality_hard_filter_respected=hard_filter_respected,
            opportunity_quality_penalty_present=(
                trace.red_flag_penalty is not None
                and trace.red_flag_penalty > Decimal("0")
            ),
            version_metadata_complete=version_metadata_complete,
            feedback_signal_present=feedback_signal_present,
        )


@dataclass(frozen=True)
class FeedbackEvaluationCase:
    """Persisted feedback context used to test deterministic score adjustment."""

    case_id: str
    feedback_type: Literal["not_suitable", "got_job"]
    signal_version: str
    baseline_score: Decimal
    adjusted_score: Decimal

    def __post_init__(self) -> None:
        _validate_identifier(self.case_id, "case_id")
        _validate_version(self.signal_version, "signal_version")
        for value, name in (
            (self.baseline_score, "baseline_score"),
            (self.adjusted_score, "adjusted_score"),
        ):
            score = _decimal(value, name)
            if not Decimal("0") <= score <= Decimal("1"):
                raise ValueError(f"{name} must be between 0 and 1")


def measure_match_cases(
    cases: Sequence[MatchEvaluationCase],
) -> tuple[MetricObservation, ...]:
    _validate_case_ids(cases, attribute="pair_id")
    labelled = [case for case in cases if case.label != "uncertain"]
    relevant = [case for case in labelled if case.label == "relevant"]
    positive = [case for case in labelled if case.predicted_relevant]
    true_positive = sum(
        case.predicted_relevant and case.label == "relevant" for case in labelled
    )
    hard_filter_violations = sum(
        not case.hard_filter_respected for case in cases
    )
    semantic_relevant = [
        case
        for case in relevant
        if case.semantic_predicted_relevant is True
    ]
    structured_relevant = [
        case
        for case in relevant
        if case.structured_predicted_relevant is True
    ]
    semantic_improvements = sum(
        case.semantic_predicted_relevant is True
        and case.structured_predicted_relevant is not True
        for case in relevant
    )
    source_quality_violations = sum(
        not case.source_quality_hard_filter_respected for case in cases
    )
    component_complete = sum(case.trace_explainable for case in cases)
    version_complete = sum(case.version_metadata_complete for case in cases)
    penalty_observed = sum(case.opportunity_quality_penalty_present for case in cases)
    feedback_context = sum(case.feedback_signal_present for case in cases)

    observations = [
        _informational_ratio(
            "matching_positive_relevance",
            true_positive,
            len(positive),
            "Labelled relevant pairs among positive match decisions.",
        ),
        _informational_ratio(
            "matching_hard_filter_violation_rate",
            hard_filter_violations,
            len(cases),
            "Eligible decisions that contradict a hard-filter rejection.",
        ),
        _informational_ratio(
            "matching_structured_recall",
            len(structured_relevant),
            len(relevant),
            "Relevant labelled pairs reached by structured matching alone.",
        ),
        _informational_ratio(
            "matching_semantic_recall",
            len(semantic_relevant),
            len(relevant),
            "Relevant labelled pairs reached after semantic scoring.",
        ),
        _informational_ratio(
            "matching_semantic_recall_delta_count",
            semantic_improvements,
            len(relevant),
            "Relevant pairs newly reached by semantic scoring beyond structured matching.",
        ),
        _informational_ratio(
            "match_trace_explainability_coverage",
            component_complete,
            len(cases),
            "Traces contain decision, filter, component and version evidence.",
        ),
        _informational_ratio(
            "source_quality_hard_filter_violation_rate",
            source_quality_violations,
            len(cases),
            "Source quality never bypasses a hard relevance constraint.",
        ),
        _informational_ratio(
            "opportunity_quality_penalty_coverage",
            penalty_observed,
            len(cases),
            "Low-quality/red-flag traces retain a separate opportunity-quality penalty.",
        ),
        _informational_ratio(
            "match_version_metadata_coverage",
            version_complete,
            len(cases),
            "Match traces retain algorithm, policy, schema and filter versions.",
        ),
        _informational_ratio(
            "feedback_signal_context_coverage",
            feedback_context,
            len(cases),
            "Match observations carry persisted feedback-signal context where supplied.",
        ),
    ]
    return tuple(observations)


def measure_feedback_cases(
    cases: Sequence[FeedbackEvaluationCase],
) -> tuple[MetricObservation, ...]:
    _validate_case_ids(cases)
    valid_direction = sum(
        (
            case.adjusted_score <= case.baseline_score
            if case.feedback_type == "not_suitable"
            else case.adjusted_score >= case.baseline_score
        )
        for case in cases
    )
    return (
        _informational_ratio(
            "feedback_adjustment_direction_accuracy",
            valid_direction,
            len(cases),
            "Not-suitable feedback does not increase a score; got-job feedback "
            "does not decrease it.",
        ),
        _informational_ratio(
            "feedback_signal_version_coverage",
            sum(bool(case.signal_version) for case in cases),
            len(cases),
            "Feedback-aware evaluation retains the derived signal version.",
        ),
    )


@dataclass(frozen=True)
class AutomatedEvaluationReport:
    schema_version: str
    runner_version: str
    run_id: str
    evaluated_at: datetime
    dataset_version: str
    dataset_fingerprint: str
    dataset_kind: Literal["real_world", "test_fixture"]
    collection_status: Literal["in_progress", "ready"]
    target_reached: bool
    quality_claim_allowed: bool
    version_identity: EvaluationVersionIdentity
    thresholds: EvaluationThresholds
    cost_latency: EvaluationCostLatency
    metrics: tuple[EvaluationMetric, ...]
    release_status: str
    blocked_reasons: tuple[str, ...]
    notes: tuple[str, ...]

    def __post_init__(self) -> None:
        if self.schema_version != AUTOMATED_EVALUATION_SCHEMA_VERSION:
            raise ValueError("unsupported automated evaluation report schema")
        if self.runner_version != AUTOMATED_EVALUATION_RUNNER_VERSION:
            raise ValueError("unsupported automated evaluation runner version")
        _validate_identifier(self.run_id, "run_id")
        if self.evaluated_at.tzinfo is None or self.evaluated_at.utcoffset() is None:
            raise ValueError("evaluated_at must include a timezone")
        if not self.dataset_version.strip():
            raise ValueError("dataset_version cannot be empty")
        if len(self.dataset_fingerprint) != 64 or any(
            character not in "0123456789abcdef"
            for character in self.dataset_fingerprint
        ):
            raise ValueError("dataset_fingerprint must be a lowercase SHA-256 hex digest")
        if self.dataset_kind not in {
            EvaluationEvidenceKind.REAL_WORLD,
            EvaluationEvidenceKind.TEST_FIXTURE,
        }:
            raise ValueError("unsupported evaluation dataset kind")
        if self.collection_status not in {"in_progress", "ready"}:
            raise ValueError("unsupported collection status")
        names = [metric.name for metric in self.metrics]
        if len(names) != len(set(names)):
            raise ValueError("evaluation metric names must be unique")
        if self.release_status not in {
            EvaluationGateStatus.PASSED,
            EvaluationGateStatus.FAILED,
            EvaluationGateStatus.BLOCKED,
            EvaluationGateStatus.NOT_MEASURED,
        }:
            raise ValueError("unsupported release status")

    @property
    def metric_map(self) -> dict[str, EvaluationMetric]:
        return {metric.name: metric for metric in self.metrics}

    def metric(self, name: str) -> EvaluationMetric:
        try:
            return self.metric_map[name]
        except KeyError:
            raise KeyError(f"evaluation metric is not present: {name}") from None

    @property
    def report_fingerprint(self) -> str:
        payload = self.to_dict()
        payload.pop("evaluated_at", None)
        return _sha256(payload)

    def recompute_gate(self) -> AutomatedEvaluationReport:
        quality_claim_allowed, blocked_reasons = _quality_claim_policy(
            self.dataset_kind,
            self.collection_status,
            self.target_reached,
        )
        metrics = tuple(
            _finalize_metric(
                metric.as_observation(),
                quality_claim_allowed=quality_claim_allowed,
            )
            for metric in self.metrics
        )
        return replace(
            self,
            quality_claim_allowed=quality_claim_allowed,
            metrics=metrics,
            release_status=_release_status(metrics),
            blocked_reasons=blocked_reasons,
        )

    def to_dict(self) -> dict[str, object]:
        return {
            "schema_version": self.schema_version,
            "runner_version": self.runner_version,
            "run_id": self.run_id,
            "evaluated_at": self.evaluated_at.isoformat(),
            "dataset_version": self.dataset_version,
            "dataset_fingerprint": self.dataset_fingerprint,
            "dataset_kind": self.dataset_kind,
            "collection_status": self.collection_status,
            "target_reached": self.target_reached,
            "quality_claim_allowed": self.quality_claim_allowed,
            "version_identity": self.version_identity.as_dict(),
            "thresholds": self.thresholds.as_dict(),
            "cost_latency": self.cost_latency.as_dict(),
            "metrics": [metric.as_dict() for metric in self.metrics],
            "release_status": self.release_status,
            "blocked_reasons": list(self.blocked_reasons),
            "notes": list(self.notes),
        }

    def write_json(self, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(
            json.dumps(self.to_dict(), ensure_ascii=False, indent=2, sort_keys=True)
            + "\n",
            encoding="utf-8",
        )

    @classmethod
    def from_dict(cls, payload: Mapping[str, object]) -> AutomatedEvaluationReport:
        if payload.get("schema_version") != AUTOMATED_EVALUATION_SCHEMA_VERSION:
            raise EvaluationRunnerError("unsupported automated report schema")
        evaluated_at = _parse_timestamp(payload.get("evaluated_at"))
        dataset_kind = payload.get("dataset_kind")
        collection_status = payload.get("collection_status")
        if dataset_kind not in {
            EvaluationEvidenceKind.REAL_WORLD,
            EvaluationEvidenceKind.TEST_FIXTURE,
        }:
            raise EvaluationRunnerError("invalid report dataset_kind")
        if collection_status not in {"in_progress", "ready"}:
            raise EvaluationRunnerError("invalid report collection_status")
        version_identity = _version_identity_from_dict(payload.get("version_identity"))
        thresholds = _thresholds_from_dict(payload.get("thresholds"))
        cost_latency = _cost_latency_from_dict(payload.get("cost_latency"))
        raw_metrics = payload.get("metrics")
        if not isinstance(raw_metrics, list):
            raise EvaluationRunnerError("report metrics must be a JSON array")
        metrics = tuple(_metric_from_dict(item) for item in raw_metrics)
        report = cls(
            schema_version=str(payload["schema_version"]),
            runner_version=str(payload.get("runner_version")),
            run_id=str(payload.get("run_id")),
            evaluated_at=evaluated_at,
            dataset_version=str(payload.get("dataset_version")),
            dataset_fingerprint=str(payload.get("dataset_fingerprint")),
            dataset_kind=dataset_kind,
            collection_status=collection_status,
            target_reached=bool(payload.get("target_reached")),
            quality_claim_allowed=bool(payload.get("quality_claim_allowed")),
            version_identity=version_identity,
            thresholds=thresholds,
            cost_latency=cost_latency,
            metrics=metrics,
            release_status=str(payload.get("release_status")),
            blocked_reasons=tuple(_string_list(payload.get("blocked_reasons"))),
            notes=tuple(_string_list(payload.get("notes"))),
        )
        return report.recompute_gate()


@dataclass(frozen=True)
class EvaluationVersionComparison:
    dataset_version: str
    dataset_fingerprint: str
    runs: tuple[AutomatedEvaluationReport, ...]
    selected_run_id: str | None
    selection_rationale: str | None
    status: Literal["ready", "blocked"]

    def __post_init__(self) -> None:
        if not self.runs:
            raise ValueError("version comparison needs at least one run")
        if len({run.run_id for run in self.runs}) != len(self.runs):
            raise ValueError("version comparison run IDs must be unique")
        if any(
            run.dataset_version != self.dataset_version
            or run.dataset_fingerprint != self.dataset_fingerprint
            for run in self.runs
        ):
            raise ValueError("version comparisons must use one dataset fingerprint")
        if self.selected_run_id is not None:
            if self.selected_run_id not in {run.run_id for run in self.runs}:
                raise ValueError("selected run is not part of the comparison")
            if not self.selection_rationale or not self.selection_rationale.strip():
                raise ValueError("selected version requires a non-empty rationale")
        expected_status = "ready" if self.selected_run_id is not None else "blocked"
        if self.status != expected_status:
            raise ValueError("version comparison status does not match selection")

    def to_dict(self) -> dict[str, object]:
        return {
            "schema_version": "evaluation_version_comparison.v1",
            "dataset_version": self.dataset_version,
            "dataset_fingerprint": self.dataset_fingerprint,
            "status": self.status,
            "selected_run_id": self.selected_run_id,
            "selection_rationale": self.selection_rationale,
            "runs": [
                {
                    "run_id": run.run_id,
                    "release_status": run.release_status,
                    "quality_claim_allowed": run.quality_claim_allowed,
                    "version_identity": run.version_identity.as_dict(),
                    "cost_latency": run.cost_latency.as_dict(),
                    "metrics": [metric.as_dict() for metric in run.metrics],
                }
                for run in self.runs
            ],
        }

    def write_json(self, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(
            json.dumps(self.to_dict(), ensure_ascii=False, indent=2, sort_keys=True)
            + "\n",
            encoding="utf-8",
        )


class _RecordingAnalyzer:
    def __init__(self, delegate: OpportunityAnalyzer) -> None:
        self._delegate = delegate
        self.calls: list[OpportunityAnalysisCall] = []
        self.latencies_ms: list[int] = []

    @property
    def provider(self) -> str:
        return self._delegate.provider

    @property
    def model(self) -> str:
        return self._delegate.model

    @property
    def analyzer_version(self) -> str:
        return self._delegate.analyzer_version

    @property
    def prompt_version(self) -> str:
        return self._delegate.prompt_version

    @property
    def schema_version(self) -> str:
        return self._delegate.schema_version

    def accepts_call(self, call: OpportunityAnalysisCall) -> bool:
        checker = getattr(self._delegate, "accepts_call", None)
        return (
            bool(checker(call))
            if checker is not None
            else (
                call.provider == self.provider
                and call.requested_model == self.model
                and call.analyzer_version == self.analyzer_version
                and call.prompt_version == self.prompt_version
                and call.schema_version == self.schema_version
            )
        )

    async def analyze(self, candidate):
        started = perf_counter()
        call = await self._delegate.analyze(candidate)
        self.calls.append(call)
        self.latencies_ms.append(max(0, round((perf_counter() - started) * 1000)))
        return call


async def run_opportunity_evaluation(
    analyzer: OpportunityAnalyzer,
    dataset: OpportunityEvalDataset | GoldenDataset,
    *,
    run_id: str,
    thresholds: EvaluationThresholds | None = None,
    allow_test_fixture: bool = False,
    duplicate_cases: Sequence[DuplicateDeliveryCase] = (),
    relevance_cases: Sequence[RelevanceCase] = (),
    prefilter_cases: Sequence[PrefilterCase] = (),
    match_cases: Sequence[MatchEvaluationCase] = (),
    feedback_cases: Sequence[FeedbackEvaluationCase] = (),
    version_identity: EvaluationVersionIdentity | None = None,
    price: AIModelPrice | None = None,
    evaluated_at: datetime | None = None,
    additional_metrics: Sequence[MetricObservation] = (),
) -> AutomatedEvaluationReport:
    """Run one versioned analyzer slice and apply release gates.

    ``OpportunityEvalDataset`` is always treated as synthetic/test evidence.
    A real-world claim must use ``GoldenDataset`` with captured provenance and
    its ready/target checks. This function never turns missing captured data
    into a passing release result.
    """

    _validate_identifier(run_id, "run_id")
    selected_thresholds = thresholds or EvaluationThresholds()
    recorder = _RecordingAnalyzer(analyzer)
    started = perf_counter()
    if isinstance(dataset, OpportunityEvalDataset):
        opportunity_report = await evaluate_opportunity_analyzer(recorder, dataset)
        dataset_kind = EvaluationEvidenceKind.TEST_FIXTURE
        collection_status = "ready"
        target_reached = False
    elif isinstance(dataset, GoldenDataset):
        opportunity_report = await evaluate_golden_dataset(
            recorder,
            dataset,
            allow_test_fixture=allow_test_fixture,
        )
        dataset_kind = dataset.dataset_kind
        collection_status = dataset.collection_status
        target_reached = dataset.target_reached
    else:
        raise TypeError("unsupported evaluation dataset type")

    if not recorder.calls:
        raise EvaluationRunnerError("opportunity evaluation produced no analyzer calls")
    elapsed_ms = max(0, round((perf_counter() - started) * 1000))
    cost_latency = EvaluationCostLatency.from_calls(
        recorder.calls,
        recorder.latencies_ms,
        price=price,
    )
    if cost_latency.latency_ms == 0 and elapsed_ms > 0:
        cost_latency = replace(cost_latency, latency_ms=elapsed_ms)
    if version_identity is None:
        version_identity = EvaluationVersionIdentity.from_routes(
            opportunity_report.routes,
            pricing_version=(None if price is None else price.pricing_version),
        )

    observations = list(
        _opportunity_observations(opportunity_report, selected_thresholds)
    )
    observations.extend(_prefilter_observations(prefilter_cases, selected_thresholds))
    observations.extend(_duplicate_observations(duplicate_cases, selected_thresholds))
    observations.extend(_relevance_observations(relevance_cases, selected_thresholds))
    if match_cases:
        observations.extend(measure_match_cases(match_cases))
    if feedback_cases:
        observations.extend(measure_feedback_cases(feedback_cases))
    if not duplicate_cases:
        observations.append(
            MetricObservation(
                name="duplicate_user_delivery_rate",
                value=None,
                numerator=0,
                denominator=0,
                target=selected_thresholds.duplicate_delivery_rate_max_exclusive,
                comparator="lt",
                description="No duplicate/repost delivery observations were supplied.",
            )
        )
    if not relevance_cases:
        observations.append(
            MetricObservation(
                name="personal_positive_relevance",
                value=None,
                numerator=0,
                denominator=0,
                target=selected_thresholds.personal_positive_relevance_min,
                comparator="gte",
                description="No labelled profile-to-opportunity relevance observations were supplied.",
            )
        )
    observations.extend(additional_metrics)
    observations.extend(_quality_observations(dataset, recorder.calls))
    return build_automated_evaluation_report(
        run_id=run_id,
        dataset_version=opportunity_report.dataset_version,
        dataset_fingerprint=opportunity_report.dataset_fingerprint,
        dataset_kind=dataset_kind,
        collection_status=collection_status,
        target_reached=target_reached,
        version_identity=version_identity,
        thresholds=selected_thresholds,
        cost_latency=cost_latency,
        observations=observations,
        evaluated_at=evaluated_at,
    )


def build_automated_evaluation_report(
    *,
    run_id: str,
    dataset_version: str,
    dataset_fingerprint: str,
    dataset_kind: Literal["real_world", "test_fixture"],
    collection_status: Literal["in_progress", "ready"],
    target_reached: bool,
    version_identity: EvaluationVersionIdentity,
    thresholds: EvaluationThresholds | None = None,
    cost_latency: EvaluationCostLatency | None = None,
    observations: Sequence[MetricObservation] = (),
    evaluated_at: datetime | None = None,
    notes: Sequence[str] = (),
) -> AutomatedEvaluationReport:
    _validate_identifier(run_id, "run_id")
    if not dataset_version.strip():
        raise ValueError("dataset_version cannot be empty")
    if not dataset_fingerprint.strip():
        raise ValueError("dataset_fingerprint cannot be empty")
    if len(dataset_fingerprint) != 64 or any(
        character not in "0123456789abcdef" for character in dataset_fingerprint
    ):
        raise ValueError("dataset_fingerprint must be a lowercase SHA-256 hex digest")
    selected_thresholds = thresholds or EvaluationThresholds()
    selected_cost_latency = cost_latency or EvaluationCostLatency()
    unique_names = [observation.name for observation in observations]
    if len(unique_names) != len(set(unique_names)):
        raise ValueError("evaluation observations must have unique names")
    missing_metrics = _REQUIRED_GATE_METRICS - set(unique_names)
    if missing_metrics:
        raise ValueError(
            "automated evaluation report is missing required gate metrics: "
            + ", ".join(sorted(missing_metrics))
        )
    quality_claim_allowed, blocked_reasons = _quality_claim_policy(
        dataset_kind,
        collection_status,
        target_reached,
    )
    metrics = tuple(
        _finalize_metric(
            observation,
            quality_claim_allowed=quality_claim_allowed,
        )
        for observation in observations
    )
    return AutomatedEvaluationReport(
        schema_version=AUTOMATED_EVALUATION_SCHEMA_VERSION,
        runner_version=AUTOMATED_EVALUATION_RUNNER_VERSION,
        run_id=run_id,
        evaluated_at=evaluated_at or datetime.now(timezone.utc),
        dataset_version=dataset_version,
        dataset_fingerprint=dataset_fingerprint,
        dataset_kind=dataset_kind,
        collection_status=collection_status,
        target_reached=target_reached,
        quality_claim_allowed=quality_claim_allowed,
        version_identity=version_identity,
        thresholds=selected_thresholds,
        cost_latency=selected_cost_latency,
        metrics=metrics,
        release_status=_release_status(metrics),
        blocked_reasons=blocked_reasons,
        notes=tuple(notes)
        + (
            (
                "Synthetic/test-fixture metrics are coverage evidence only; "
                "they are not production quality claims."
            ,)
            if dataset_kind == EvaluationEvidenceKind.TEST_FIXTURE
            else ()
        ),
    )


def measure_duplicate_delivery(
    cases: Sequence[DuplicateDeliveryCase],
) -> DuplicateDeliveryMeasurement:
    _validate_case_ids(cases)
    duplicate_cases = [case for case in cases if case.scenario == "duplicate"]
    distinct_cases = [case for case in cases if case.scenario == "distinct"]
    denominator = sum(len(case.delivered_opportunity_ids) for case in duplicate_cases)
    duplicate_count = sum(
        max(0, len(case.delivered_opportunity_ids) - 1)
        for case in duplicate_cases
    )
    return DuplicateDeliveryMeasurement(
        duplicate_delivery_count=duplicate_count,
        duplicate_delivery_denominator=denominator,
        duplicate_delivery_rate=(
            None if denominator == 0 else _ratio(duplicate_count, denominator)
        ),
        false_merge_count=sum(
            case.predicted_same_opportunity for case in distinct_cases
        ),
        false_merge_denominator=len(distinct_cases),
    )


def measure_relevance(cases: Sequence[RelevanceCase]) -> RelevanceMeasurement:
    _validate_case_ids(cases, attribute="pair_id")
    true_positive = false_positive = true_negative = false_negative = 0
    uncertain_count = 0
    for case in cases:
        if case.label == "uncertain":
            uncertain_count += 1
        elif case.label == "relevant":
            if case.predicted_relevant:
                true_positive += 1
            else:
                false_negative += 1
        elif case.predicted_relevant:
            false_positive += 1
        else:
            true_negative += 1
    return RelevanceMeasurement(
        true_positive=true_positive,
        false_positive=false_positive,
        true_negative=true_negative,
        false_negative=false_negative,
        uncertain_count=uncertain_count,
    )


def measure_prefilter(cases: Sequence[PrefilterCase]) -> PrefilterMeasurement:
    _validate_case_ids(cases)
    expected_candidates = [case for case in cases if case.expected_candidate]
    retrieved_candidates = [
        case for case in expected_candidates if case.actual_passed
    ]
    rejected = [case for case in cases if not case.actual_passed]
    documented_rejections = [case for case in rejected if case.rejection_reasons]
    return PrefilterMeasurement(
        candidate_recall=(
            None
            if not expected_candidates
            else _ratio(len(retrieved_candidates), len(expected_candidates))
        ),
        candidate_count=len(expected_candidates),
        candidate_retrieved_count=len(retrieved_candidates),
        rejection_reason_coverage=(
            None
            if not rejected
            else _ratio(len(documented_rejections), len(rejected))
        ),
        rejected_count=len(rejected),
        documented_rejected_count=len(documented_rejections),
    )


def compare_evaluation_versions(
    reports: Sequence[AutomatedEvaluationReport],
    *,
    selected_run_id: str | None = None,
    selection_rationale: str | None = None,
) -> EvaluationVersionComparison:
    selected_reports = tuple(report.recompute_gate() for report in reports)
    if not selected_reports:
        raise ValueError("at least one evaluation report is required")
    first = selected_reports[0]
    return EvaluationVersionComparison(
        dataset_version=first.dataset_version,
        dataset_fingerprint=first.dataset_fingerprint,
        runs=selected_reports,
        selected_run_id=selected_run_id,
        selection_rationale=selection_rationale,
        status="ready" if selected_run_id is not None else "blocked",
    )


def load_automated_evaluation_report(path: Path) -> AutomatedEvaluationReport:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise EvaluationRunnerError(f"cannot load evaluation report: {exc}") from exc
    if not isinstance(payload, dict):
        raise EvaluationRunnerError("evaluation report root must be an object")
    return AutomatedEvaluationReport.from_dict(payload)


def evaluation_gate_summary(report: AutomatedEvaluationReport) -> dict[str, object]:
    checked = report.recompute_gate()
    return {
        "schema_version": checked.schema_version,
        "runner_version": checked.runner_version,
        "run_id": checked.run_id,
        "dataset_version": checked.dataset_version,
        "dataset_fingerprint": checked.dataset_fingerprint,
        "dataset_kind": checked.dataset_kind,
        "release_status": checked.release_status,
        "quality_claim_allowed": checked.quality_claim_allowed,
        "blocked_reasons": list(checked.blocked_reasons),
        "metrics": {
            metric.name: {
                "value": metric.value,
                "target": metric.target,
                "comparator": metric.comparator,
                "numerator": metric.numerator,
                "denominator": metric.denominator,
                "threshold_met": metric.threshold_met,
                "status": metric.status,
            }
            for metric in checked.metrics
        },
    }


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Validate a versioned automated evaluation report and emit JSON."
    )
    parser.add_argument("--report", type=Path, required=True)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args(argv)
    try:
        summary = evaluation_gate_summary(load_automated_evaluation_report(args.report))
    except (EvaluationRunnerError, ValueError, TypeError, KeyError) as exc:
        print(json.dumps({"release_status": "invalid", "error": str(exc)}))
        return 3
    serialized = json.dumps(summary, ensure_ascii=False, sort_keys=True)
    print(serialized)
    if args.output is not None:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(serialized + "\n", encoding="utf-8")
    status = summary["release_status"]
    return 0 if status == EvaluationGateStatus.PASSED else (1 if status == EvaluationGateStatus.FAILED else 2)


def _opportunity_observations(
    report: OpportunityEvalReport | GoldenEvaluationReport,
    thresholds: EvaluationThresholds,
) -> tuple[MetricObservation, ...]:
    return (
        MetricObservation(
            name="opportunity_precision",
            value=report.precision,
            numerator=report.true_positive,
            denominator=report.true_positive + report.false_positive,
            target=thresholds.opportunity_precision_min,
            comparator="gte",
            description="Predicted opportunity messages that are labelled opportunities.",
        ),
        MetricObservation(
            name="opportunity_recall",
            value=report.recall,
            numerator=report.true_positive,
            denominator=report.true_positive + report.false_negative,
            target=thresholds.opportunity_recall_min,
            comparator="gte",
            description="Labelled opportunities that reached opportunity classification.",
        ),
        MetricObservation(
            name="market_direction_accuracy",
            value=report.direction_accuracy,
            numerator=None,
            denominator=report.case_count,
            target=None,
            comparator="none",
            description="Buyer/specialist/unknown market-direction accuracy.",
            gate=False,
        ),
        MetricObservation(
            name="intent_stage_accuracy",
            value=report.intent_accuracy,
            numerator=None,
            denominator=report.case_count,
            target=None,
            comparator="none",
            description="Active/recommendation/research/weak/none accuracy.",
            gate=False,
        ),
        MetricObservation(
            name="opportunity_type_accuracy",
            value=report.type_accuracy,
            numerator=None,
            denominator=report.case_count,
            target=None,
            comparator="none",
            description="Cross-profession opportunity-type accuracy.",
            gate=False,
        ),
        MetricObservation(
            name="opportunity_structured_field_accuracy",
            value=getattr(
                report,
                "structured_field_accuracy",
                getattr(report, "label_accuracy", None),
            ),
            numerator=None,
            denominator=report.case_count,
            target=None,
            comparator="none",
            description="Structured extraction/label accuracy from the selected dataset.",
            gate=False,
        ),
    )


def _quality_observations(
    dataset: OpportunityEvalDataset | GoldenDataset,
    calls: Sequence[OpportunityAnalysisCall],
) -> tuple[MetricObservation, ...]:
    if not isinstance(dataset, OpportunityEvalDataset):
        return ()
    if len(dataset.cases) != len(calls):
        raise EvaluationRunnerError("quality metric calls do not match evaluation cases")
    # Replay analyzers used by the tests preserve result order. Production
    # analyzers are evaluated in dataset order by the underlying runner.
    pairs = tuple(zip(dataset.cases, calls, strict=True))
    self_promotion = [
        (case, call)
        for case, call in pairs
        if case.expected.market_direction is MarketDirection.SPECIALIST_TO_BUYER
    ]
    red_flagged = [
        (case, call)
        for case, call in pairs
        if case.expected.red_flags
    ]
    role_cases = [
        (case, call)
        for case, call in pairs
        if case.expected.is_opportunity and case.expected.role_title
    ]
    category_cases = [
        (case, call)
        for case, call in pairs
        if case.expected.is_opportunity and case.expected.category
    ]
    skills_cases = [
        (case, call)
        for case, call in pairs
        if case.expected.is_opportunity and case.expected.skills
    ]
    observations: list[MetricObservation] = []
    if self_promotion:
        blocked = sum(not call.analysis.is_opportunity for _, call in self_promotion)
        observations.append(
            _informational_ratio(
                "seller_self_promotion_block_rate",
                blocked,
                len(self_promotion),
                "Seller self-promotion cases rejected as opportunities.",
            )
        )
    if red_flagged:
        flagged = sum(bool(call.analysis.red_flags) for _, call in red_flagged)
        observations.append(
            _informational_ratio(
                "quality_red_flag_detection_rate",
                flagged,
                len(red_flagged),
                "Cases with expected scam/spam/suspicion signals retaining a red flag.",
            )
        )
    for name, cases in (
        ("role_title_coverage", role_cases),
        ("category_coverage", category_cases),
        ("skills_coverage", skills_cases),
    ):
        if cases:
            covered = sum(
                bool(
                    getattr(call.analysis, name.removesuffix("_coverage"))
                    if name != "skills_coverage"
                    else call.analysis.skills
                )
                for _, call in cases
            )
            observations.append(
                _informational_ratio(
                    name,
                    covered,
                    len(cases),
                    "Non-empty extensible structured taxonomy output.",
                )
            )
    return tuple(observations)


def _prefilter_observations(
    cases: Sequence[PrefilterCase],
    thresholds: EvaluationThresholds,
) -> tuple[MetricObservation, ...]:
    if not cases:
        return ()
    return measure_prefilter(cases).observations(thresholds=thresholds)


def _duplicate_observations(
    cases: Sequence[DuplicateDeliveryCase],
    thresholds: EvaluationThresholds,
) -> tuple[MetricObservation, ...]:
    if not cases:
        return ()
    return measure_duplicate_delivery(cases).observations(thresholds=thresholds)


def _relevance_observations(
    cases: Sequence[RelevanceCase],
    thresholds: EvaluationThresholds,
) -> tuple[MetricObservation, ...]:
    if not cases:
        return ()
    return measure_relevance(cases).observations(thresholds=thresholds)


def _informational_ratio(
    name: str,
    numerator: int,
    denominator: int,
    description: str,
) -> MetricObservation:
    return MetricObservation(
        name=name,
        value=_optional_ratio(numerator, denominator),
        numerator=numerator,
        denominator=denominator,
        target=None,
        comparator="none",
        description=description,
        gate=False,
    )


def _finalize_metric(
    observation: MetricObservation,
    *,
    quality_claim_allowed: bool,
) -> EvaluationMetric:
    value = None if observation.value is None else float(observation.value)
    threshold_met: bool | None
    if value is None or observation.denominator in (None, 0):
        threshold_met = None
    elif observation.comparator == "none":
        threshold_met = None
    else:
        threshold = _decimal(observation.target, "metric target")
        actual = _decimal(observation.value, "metric value")
        threshold_met = {
            "gte": actual >= threshold,
            "gt": actual > threshold,
            "lt": actual < threshold,
            "lte": actual <= threshold,
        }[observation.comparator]
    if not observation.gate:
        status = EvaluationGateStatus.OBSERVED
    elif threshold_met is None:
        status = EvaluationGateStatus.NOT_MEASURED
    elif not threshold_met:
        status = EvaluationGateStatus.FAILED
    elif not quality_claim_allowed:
        status = EvaluationGateStatus.BLOCKED
    else:
        status = EvaluationGateStatus.PASSED
    return EvaluationMetric(
        name=observation.name,
        value=value,
        numerator=observation.numerator,
        denominator=observation.denominator,
        target=(None if observation.target is None else float(observation.target)),
        comparator=observation.comparator,
        description=observation.description,
        gate=observation.gate,
        threshold_met=threshold_met,
        status=status,
    )


def _quality_claim_policy(
    dataset_kind: Literal["real_world", "test_fixture"],
    collection_status: Literal["in_progress", "ready"],
    target_reached: bool,
) -> tuple[bool, tuple[str, ...]]:
    reasons: list[str] = []
    if dataset_kind == EvaluationEvidenceKind.TEST_FIXTURE:
        reasons.append(
            "test_fixture evidence cannot satisfy a production release gate"
        )
    if collection_status != "ready":
        reasons.append("dataset collection_status is not ready")
    if not target_reached:
        reasons.append("dataset target range has not been reached")
    return not reasons, tuple(reasons)


def _release_status(metrics: Sequence[EvaluationMetric]) -> str:
    gated = [metric for metric in metrics if metric.gate]
    if any(metric.status == EvaluationGateStatus.FAILED for metric in gated):
        return EvaluationGateStatus.FAILED
    if any(
        metric.status in {
            EvaluationGateStatus.BLOCKED,
            EvaluationGateStatus.NOT_MEASURED,
        }
        for metric in gated
    ):
        return EvaluationGateStatus.BLOCKED
    return EvaluationGateStatus.PASSED


def _metric_from_dict(payload: object) -> EvaluationMetric:
    if not isinstance(payload, dict):
        raise EvaluationRunnerError("each report metric must be an object")
    comparator = payload.get("comparator")
    if comparator not in {"gte", "gt", "lt", "lte", "none"}:
        raise EvaluationRunnerError("invalid metric comparator")
    return EvaluationMetric(
        name=str(payload.get("name")),
        value=_optional_float(payload.get("value")),
        numerator=_optional_int(payload.get("numerator")),
        denominator=_optional_int(payload.get("denominator")),
        target=_optional_float(payload.get("target")),
        comparator=comparator,
        description=str(payload.get("description")),
        gate=bool(payload.get("gate")),
        threshold_met=(
            None
            if payload.get("threshold_met") is None
            else bool(payload.get("threshold_met"))
        ),
        status=str(payload.get("status")),
    )


def _version_identity_from_dict(payload: object) -> EvaluationVersionIdentity:
    if not isinstance(payload, dict):
        raise EvaluationRunnerError("version_identity must be an object")
    raw_routes = payload.get("routes")
    if not isinstance(raw_routes, list) or not raw_routes:
        raise EvaluationRunnerError("version_identity.routes must be non-empty")
    routes = []
    for raw in raw_routes:
        if not isinstance(raw, dict):
            raise EvaluationRunnerError("version identity route must be an object")
        try:
            routes.append(
                OpportunityEvalRoute(
                    provider=str(raw["provider"]),
                    requested_model=str(raw["requested_model"]),
                    response_model=str(raw["response_model"]),
                    analyzer_version=str(raw["analyzer_version"]),
                    prompt_version=str(raw["prompt_version"]),
                    schema_version=str(raw["schema_version"]),
                    routing_version=str(raw["routing_version"]),
                )
            )
        except (KeyError, TypeError, ValueError) as exc:
            raise EvaluationRunnerError("invalid evaluation route identity") from exc
    return EvaluationVersionIdentity.from_routes(
        routes,
        matching_algorithm_version=_optional_string(
            payload.get("matching_algorithm_version")
        ),
        matching_policy_version=_optional_string(
            payload.get("matching_policy_version")
        ),
        semantic_matching_version=_optional_string(
            payload.get("semantic_matching_version")
        ),
        semantic_policy_version=_optional_string(
            payload.get("semantic_policy_version")
        ),
        pricing_version=_optional_string(payload.get("pricing_version")),
    )


def _thresholds_from_dict(payload: object) -> EvaluationThresholds:
    if not isinstance(payload, dict):
        raise EvaluationRunnerError("thresholds must be an object")
    return EvaluationThresholds(
        opportunity_precision_min=_decimal(
            payload.get("opportunity_precision_min"),
            "opportunity_precision_min",
        ),
        opportunity_recall_min=_decimal(
            payload.get("opportunity_recall_min"),
            "opportunity_recall_min",
        ),
        duplicate_delivery_rate_max_exclusive=_decimal(
            payload.get("duplicate_delivery_rate_max_exclusive"),
            "duplicate_delivery_rate_max_exclusive",
        ),
        personal_positive_relevance_min=_decimal(
            payload.get("personal_positive_relevance_min"),
            "personal_positive_relevance_min",
        ),
        prefilter_recall_min=(
            None
            if payload.get("prefilter_recall_min") is None
            else _decimal(payload.get("prefilter_recall_min"), "prefilter_recall_min")
        ),
    )


def _cost_latency_from_dict(payload: object) -> EvaluationCostLatency:
    if not isinstance(payload, dict):
        raise EvaluationRunnerError("cost_latency must be an object")
    return EvaluationCostLatency(
        latency_ms=_optional_int(payload.get("latency_ms")),
        input_tokens=_optional_int(payload.get("input_tokens")),
        output_tokens=_optional_int(payload.get("output_tokens")),
        total_tokens=_optional_int(payload.get("total_tokens")),
        estimated_cost_usd=(
            None
            if payload.get("estimated_cost_usd") is None
            else _decimal(payload.get("estimated_cost_usd"), "estimated_cost_usd")
        ),
        pricing_version=_optional_string(payload.get("pricing_version")),
    )


def _parse_timestamp(value: object) -> datetime:
    if not isinstance(value, str):
        raise EvaluationRunnerError("evaluated_at must be an ISO timestamp")
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError as exc:
        raise EvaluationRunnerError("evaluated_at is not a valid ISO timestamp") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise EvaluationRunnerError("evaluated_at must include a timezone")
    return parsed


def _validate_case_ids(cases: Sequence[Any], *, attribute: str = "case_id") -> None:
    values = [getattr(case, attribute) for case in cases]
    if len(values) != len(set(values)):
        raise ValueError(f"evaluation {attribute}s must be unique")


def _validate_identifier(value: str, name: str) -> None:
    import re

    if not isinstance(value, str) or re.fullmatch(_RUN_ID_PATTERN, value) is None:
        raise ValueError(f"{name} is invalid")


def _validate_version(value: str, name: str) -> None:
    import re

    if re.fullmatch(_VERSION_PATTERN, value) is None:
        raise ValueError(f"{name} is invalid")


def _validate_metric_name(value: str) -> None:
    _validate_identifier(value, "metric name")


def _decimal(value: Any, name: str) -> Decimal:
    try:
        result = Decimal(str(value))
    except Exception as exc:
        raise ValueError(f"{name} must be numeric") from exc
    if not result.is_finite():
        raise ValueError(f"{name} must be finite")
    return result


def _ratio(numerator: int, denominator: int) -> Decimal:
    return (Decimal(numerator) / Decimal(denominator)).quantize(
        Decimal("0.0000001")
    )


def _optional_ratio(numerator: int, denominator: int) -> Decimal | None:
    return None if denominator == 0 else _ratio(numerator, denominator)


def _sha256(value: object) -> str:
    canonical = json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    )
    import hashlib

    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def _optional_float(value: object) -> float | None:
    if value is None:
        return None
    result = float(value)
    if not math.isfinite(result):
        raise EvaluationRunnerError("metric value must be finite")
    return result


def _optional_int(value: object) -> int | None:
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, int):
        raise EvaluationRunnerError("metric counts must be integers")
    return value


def _optional_string(value: object) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str) or not value:
        raise EvaluationRunnerError("version fields must be non-empty strings")
    return value


def _string_list(value: object) -> list[str]:
    if value is None:
        return []
    if not isinstance(value, list) or any(not isinstance(item, str) for item in value):
        raise EvaluationRunnerError("report string lists are invalid")
    return value


if __name__ == "__main__":
    raise SystemExit(main())
