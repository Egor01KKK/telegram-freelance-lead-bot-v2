from __future__ import annotations

from dataclasses import dataclass, field, replace
from datetime import datetime, timezone
from decimal import Decimal
from enum import Enum
from hashlib import sha256
import json
import re
from uuid import UUID

from .config import RuntimeConfig
from .matching import (
    MATCHING_FILTER_VERSION,
    CandidateExclusionCode,
    HardFilterDecision,
    StructuredScoreComponent,
    StructuredScoringPolicy,
    evaluate_hard_filters,
)
from .persistence.opportunities import CanonicalOpportunityRecord
from .persistence.search_profiles import (
    SearchProfileConfirmationStatus,
    SearchProfileRecord,
)
from .semantic_matching import (
    SemanticCandidateScore,
    SemanticMatchingPolicy,
    SemanticScoringResult,
)


MATCH_DECISION_SCHEMA_VERSION = "match-decision-trace.v1"
MATCH_DECISION_ALGORITHM_VERSION = "matching-decision.v4"
MATCH_DECISION_POLICY_VERSION = "matching-decision-policy.v1"
_VERSION_PATTERN = re.compile(r"^[a-z0-9][a-z0-9._-]{0,99}$")
_SCORE_QUANTUM = Decimal("0.0001")


class MatchDecisionCode(str, Enum):
    ELIGIBLE = "eligible"
    HARD_REJECTED = "hard_rejected"
    FRESHNESS_EXPIRED = "freshness_expired"
    BELOW_RELEVANCE_THRESHOLD = "below_relevance_threshold"
    BELOW_RANK_SCORE_THRESHOLD = "below_rank_score_threshold"


@dataclass(frozen=True)
class MatchDecisionPolicy:
    version: str = MATCH_DECISION_POLICY_VERSION
    minimum_relevance_score: Decimal = Decimal("0.3000")
    minimum_rank_score: Decimal = Decimal("0.4000")
    freshness_weight: Decimal = Decimal("0.1000")
    maximum_age_seconds: int = 7 * 24 * 60 * 60
    suppress_expired: bool = True
    evaluation_bucket_seconds: int = 60 * 60

    def __post_init__(self) -> None:
        if not _VERSION_PATTERN.fullmatch(self.version):
            raise ValueError("match decision policy version is invalid")
        for value, name in (
            (self.minimum_relevance_score, "minimum_relevance_score"),
            (self.minimum_rank_score, "minimum_rank_score"),
            (self.freshness_weight, "freshness_weight"),
        ):
            if not Decimal("0") <= value <= Decimal("1"):
                raise ValueError(f"{name} must be between 0 and 1")
        if self.maximum_age_seconds < 60:
            raise ValueError("maximum_age_seconds must be at least 60")
        if not 60 <= self.evaluation_bucket_seconds <= 24 * 60 * 60:
            raise ValueError(
                "evaluation_bucket_seconds must be between 60 and 86400"
            )
        if self.maximum_age_seconds % self.evaluation_bucket_seconds:
            raise ValueError(
                "maximum_age_seconds must be divisible by evaluation_bucket_seconds"
            )

    def as_dict(self) -> dict[str, str | int | bool]:
        return {
            "version": self.version,
            "minimum_relevance_score": str(self.minimum_relevance_score),
            "minimum_rank_score": str(self.minimum_rank_score),
            "freshness_weight": str(self.freshness_weight),
            "maximum_age_seconds": self.maximum_age_seconds,
            "suppress_expired": self.suppress_expired,
            "evaluation_bucket_seconds": self.evaluation_bucket_seconds,
        }


@dataclass(frozen=True)
class MatchScoringInput:
    opportunity: CanonicalOpportunityRecord
    profiles: tuple[SearchProfileRecord, ...]
    semantic: SemanticScoringResult
    structured_policy: StructuredScoringPolicy = field(
        default_factory=StructuredScoringPolicy
    )
    semantic_policy: SemanticMatchingPolicy = field(
        default_factory=SemanticMatchingPolicy
    )


@dataclass(frozen=True)
class MatchTraceDraft:
    opportunity_id: UUID
    search_profile_id: UUID
    profile_revision: int
    profile_schema_version: str
    preferences_schema_version: str
    opportunity_lifecycle_status: str
    opportunity_last_seen_at: datetime
    filter_version: str
    hard_filter_eligible: bool
    hard_filter_reasons: tuple[dict[str, object], ...]
    nonblocking_unknowns: tuple[str, ...]
    structured_scoring_version: str | None
    structured_policy_version: str | None
    structured_components: tuple[dict[str, object], ...]
    user_relevance_score: Decimal | None
    structured_score: Decimal | None
    semantic_matching_version: str | None
    semantic_policy_version: str | None
    semantic_status: str
    semantic_degraded_reason: str | None
    semantic_similarity: Decimal | None
    semantic_provider: str | None
    semantic_model: str | None
    semantic_model_version: str | None
    opportunity_representation_sha256: str | None
    profile_representation_sha256: str | None
    combined_relevance_score: Decimal | None
    opportunity_quality_score: Decimal | None
    source_quality_score: Decimal | None
    source_quality_snapshot_id: int | None
    red_flag_penalty: Decimal | None
    base_combined_score: Decimal | None
    freshness_age_seconds: int
    freshness_score: Decimal
    final_rank_score: Decimal | None
    minimum_relevance_threshold: Decimal
    minimum_rank_score_threshold: Decimal
    decision_code: MatchDecisionCode
    eligible: bool
    rank: int | None
    decision_schema_version: str
    decision_algorithm_version: str
    decision_policy_version: str
    evaluated_at: datetime
    input_sha256: str = ""


@dataclass(frozen=True)
class MatchDecisionBatch:
    idempotency_key: str
    evaluated_at: datetime
    policy: MatchDecisionPolicy
    policy_config: dict[str, object]
    traces: tuple[MatchTraceDraft, ...]


def match_decision_policy_from_config(config: RuntimeConfig) -> MatchDecisionPolicy:
    return MatchDecisionPolicy(
        version=config.matching_decision_policy_version,
        minimum_relevance_score=config.matching_minimum_relevance_score,
        minimum_rank_score=config.matching_minimum_rank_score,
        freshness_weight=config.matching_freshness_weight,
        maximum_age_seconds=config.matching_maximum_age_seconds,
        suppress_expired=config.matching_suppress_expired,
        evaluation_bucket_seconds=config.matching_evaluation_bucket_seconds,
    )


def decide_and_rank_matches(
    scoring_inputs: tuple[MatchScoringInput, ...],
    *,
    evaluated_at: datetime,
    policy: MatchDecisionPolicy | None = None,
) -> MatchDecisionBatch:
    selected_policy = policy or MatchDecisionPolicy()
    reference_time = _bucketed_time(
        _aware_utc(evaluated_at),
        selected_policy.evaluation_bucket_seconds,
    )
    traces: list[MatchTraceDraft] = []
    seen_opportunities: set[UUID] = set()
    scoring_policy_configs: dict[str, dict[str, object]] = {}
    for scoring_input in scoring_inputs:
        opportunity = scoring_input.opportunity
        if opportunity.id in seen_opportunities:
            raise ValueError("matching input contains a duplicate Opportunity")
        seen_opportunities.add(opportunity.id)
        if (
            scoring_input.semantic.structured.candidates.opportunity_id
            != opportunity.id
        ):
            raise ValueError("semantic result belongs to another Opportunity")
        scoring_config = _scoring_policy_config(
            scoring_input.structured_policy,
            scoring_input.semantic_policy,
        )
        scoring_policy_configs[_canonical_sha256(scoring_config)] = scoring_config
        traces.extend(
            _opportunity_traces(
                scoring_input,
                evaluated_at=reference_time,
                policy=selected_policy,
            )
        )

    ranked = _rank_eligible(tuple(traces))
    identified = tuple(
        replace(trace, input_sha256=_trace_sha256(trace)) for trace in ranked
    )
    if len(scoring_policy_configs) > 1:
        raise ValueError("one matching batch must use one scoring policy configuration")
    scoring_config = next(
        iter(scoring_policy_configs.values()),
        _scoring_policy_config(
            StructuredScoringPolicy(),
            SemanticMatchingPolicy(),
        ),
    )
    policy_config = {
        "decision": selected_policy.as_dict(),
        **scoring_config,
    }
    batch_payload = {
        "schema_version": MATCH_DECISION_SCHEMA_VERSION,
        "algorithm_version": MATCH_DECISION_ALGORITHM_VERSION,
        "policy": policy_config,
        "evaluated_at": reference_time.isoformat(),
        "trace_inputs": sorted(trace.input_sha256 for trace in identified),
    }
    return MatchDecisionBatch(
        idempotency_key=_canonical_sha256(batch_payload),
        evaluated_at=reference_time,
        policy=selected_policy,
        policy_config=policy_config,
        traces=identified,
    )


def _opportunity_traces(
    scoring_input: MatchScoringInput,
    *,
    evaluated_at: datetime,
    policy: MatchDecisionPolicy,
) -> tuple[MatchTraceDraft, ...]:
    opportunity = scoring_input.opportunity
    semantic = scoring_input.semantic
    scores = {score.structured.profile_id: score for score in semantic.scores}
    hard_decisions = {
        decision.profile.id: decision
        for decision in semantic.structured.candidates.decisions
    }
    exclusions = {
        exclusion.profile_id: exclusion.code
        for exclusion in semantic.structured.candidates.trace.exclusions
    }
    profiles = tuple(
        profile
        for profile in scoring_input.profiles
        if profile.is_active
        and profile.confirmation_status is SearchProfileConfirmationStatus.CONFIRMED
    )
    if len({profile.id for profile in profiles}) != len(profiles):
        raise ValueError("matching input contains a duplicate SearchProfile")
    age_seconds, freshness_score = _freshness(
        opportunity.last_seen_at,
        evaluated_at,
        policy.maximum_age_seconds,
    )
    return tuple(
        _profile_trace(
            opportunity,
            profile,
            semantic_score=scores.get(profile.id),
            hard_decision=hard_decisions.get(profile.id),
            narrowing_exclusion=exclusions.get(profile.id),
            semantic=semantic,
            age_seconds=age_seconds,
            freshness_score=freshness_score,
            evaluated_at=evaluated_at,
            policy=policy,
        )
        for profile in profiles
    )


def _profile_trace(
    opportunity: CanonicalOpportunityRecord,
    profile: SearchProfileRecord,
    *,
    semantic_score: SemanticCandidateScore | None,
    hard_decision: HardFilterDecision | None,
    narrowing_exclusion: CandidateExclusionCode | None,
    semantic: SemanticScoringResult,
    age_seconds: int,
    freshness_score: Decimal,
    evaluated_at: datetime,
    policy: MatchDecisionPolicy,
) -> MatchTraceDraft:
    if hard_decision is None:
        hard_decision = evaluate_hard_filters(opportunity, profile)
    reasons = tuple(_failure_payload(failure) for failure in hard_decision.failures)
    if narrowing_exclusion is not None:
        reasons += (
            {
                "code": f"narrowing.{narrowing_exclusion.value}",
                "opportunity_value": None,
                "profile_values": (),
            },
        )
    hard_eligible = hard_decision.eligible and narrowing_exclusion is None
    structured = None if semantic_score is None else semantic_score.structured
    components = (
        ()
        if structured is None
        else tuple(_component_payload(component) for component in structured.components)
    )
    final_rank_score = None
    decision = MatchDecisionCode.HARD_REJECTED
    eligible = False
    if hard_eligible and semantic_score is not None:
        final_rank_score = _quantize(
            semantic_score.combined_score * (Decimal("1") - policy.freshness_weight)
            + freshness_score * policy.freshness_weight
        )
        if policy.suppress_expired and age_seconds >= policy.maximum_age_seconds:
            decision = MatchDecisionCode.FRESHNESS_EXPIRED
        elif (
            semantic_score.combined_relevance_score
            < policy.minimum_relevance_score
        ):
            decision = MatchDecisionCode.BELOW_RELEVANCE_THRESHOLD
        elif final_rank_score < policy.minimum_rank_score:
            decision = MatchDecisionCode.BELOW_RANK_SCORE_THRESHOLD
        else:
            decision = MatchDecisionCode.ELIGIBLE
            eligible = True

    return MatchTraceDraft(
        opportunity_id=opportunity.id,
        search_profile_id=profile.id,
        profile_revision=profile.revision,
        profile_schema_version=profile.schema_version,
        preferences_schema_version=profile.preferences.schema_version,
        opportunity_lifecycle_status=opportunity.lifecycle_status.value,
        opportunity_last_seen_at=opportunity.last_seen_at,
        filter_version=MATCHING_FILTER_VERSION,
        hard_filter_eligible=hard_eligible,
        hard_filter_reasons=reasons,
        nonblocking_unknowns=tuple(
            unknown.value for unknown in hard_decision.nonblocking_unknowns
        ),
        structured_scoring_version=(
            None if structured is None else structured.scoring_version
        ),
        structured_policy_version=(
            None if structured is None else structured.policy_version
        ),
        structured_components=components,
        user_relevance_score=(
            None if structured is None else structured.user_relevance_score
        ),
        structured_score=None if structured is None else structured.structured_score,
        semantic_matching_version=(
            None if semantic_score is None else semantic_score.semantic_matching_version
        ),
        semantic_policy_version=(
            None if semantic_score is None else semantic_score.semantic_policy_version
        ),
        semantic_status=semantic.status.value,
        semantic_degraded_reason=semantic.degraded_reason,
        semantic_similarity=(
            None if semantic_score is None else semantic_score.semantic_similarity
        ),
        semantic_provider=None if semantic_score is None else semantic_score.provider,
        semantic_model=None if semantic_score is None else semantic_score.model,
        semantic_model_version=(
            None if semantic_score is None else semantic_score.model_version
        ),
        opportunity_representation_sha256=(
            None
            if semantic_score is None
            else semantic_score.opportunity_representation_sha256
        ),
        profile_representation_sha256=(
            None
            if semantic_score is None
            else semantic_score.profile_representation_sha256
        ),
        combined_relevance_score=(
            None
            if semantic_score is None
            else semantic_score.combined_relevance_score
        ),
        opportunity_quality_score=(
            None if structured is None else structured.opportunity_quality_score
        ),
        source_quality_score=(
            None if structured is None else structured.source_quality_score
        ),
        source_quality_snapshot_id=(
            None if structured is None else structured.source_quality_snapshot_id
        ),
        red_flag_penalty=None if structured is None else structured.red_flag_penalty,
        base_combined_score=(
            None if semantic_score is None else semantic_score.combined_score
        ),
        freshness_age_seconds=age_seconds,
        freshness_score=freshness_score,
        final_rank_score=final_rank_score,
        minimum_relevance_threshold=policy.minimum_relevance_score,
        minimum_rank_score_threshold=policy.minimum_rank_score,
        decision_code=decision,
        eligible=eligible,
        rank=None,
        decision_schema_version=MATCH_DECISION_SCHEMA_VERSION,
        decision_algorithm_version=MATCH_DECISION_ALGORITHM_VERSION,
        decision_policy_version=policy.version,
        evaluated_at=evaluated_at,
    )


def _rank_eligible(traces: tuple[MatchTraceDraft, ...]) -> tuple[MatchTraceDraft, ...]:
    ranks: dict[tuple[UUID, UUID], int] = {}
    profile_ids = sorted({trace.search_profile_id for trace in traces}, key=str)
    for profile_id in profile_ids:
        eligible = sorted(
            (
                trace
                for trace in traces
                if trace.search_profile_id == profile_id and trace.eligible
            ),
            key=lambda trace: (
                -(trace.final_rank_score or Decimal("0")),
                -(trace.combined_relevance_score or Decimal("0")),
                -trace.freshness_score,
                -trace.opportunity_last_seen_at.timestamp(),
                str(trace.opportunity_id),
            ),
        )
        for rank, trace in enumerate(eligible, start=1):
            ranks[(trace.opportunity_id, trace.search_profile_id)] = rank
    return tuple(
        replace(
            trace,
            rank=ranks.get((trace.opportunity_id, trace.search_profile_id)),
        )
        for trace in traces
    )


def _freshness(
    last_seen_at: datetime,
    evaluated_at: datetime,
    maximum_age_seconds: int,
) -> tuple[int, Decimal]:
    last_seen = _aware_utc(last_seen_at)
    age_seconds = max(0, int((evaluated_at - last_seen).total_seconds()))
    bounded_age = min(age_seconds, maximum_age_seconds)
    score = Decimal("1") - Decimal(bounded_age) / Decimal(maximum_age_seconds)
    return age_seconds, _quantize(score)


def _failure_payload(failure) -> dict[str, object]:
    return {
        "code": failure.code.value,
        "opportunity_value": failure.opportunity_value,
        "profile_values": failure.profile_values,
    }


def _component_payload(component: StructuredScoreComponent) -> dict[str, object]:
    return {
        "name": component.name,
        "score": None if component.score is None else str(component.score),
        "weight": str(component.weight),
        "contribution": str(component.contribution),
        "evidence": component.evidence,
    }


def _scoring_policy_config(
    structured: StructuredScoringPolicy,
    semantic: SemanticMatchingPolicy,
) -> dict[str, object]:
    return {
        "structured": {
            "version": structured.version,
            "role_weight": str(structured.role_weight),
            "skills_weight": str(structured.skills_weight),
            "category_weight": str(structured.category_weight),
            "work_type_weight": str(structured.work_type_weight),
            "budget_weight": str(structured.budget_weight),
            "preferences_weight": str(structured.preferences_weight),
            "relevance_weight": str(structured.relevance_weight),
            "opportunity_quality_weight": str(
                structured.opportunity_quality_weight
            ),
            "source_quality_weight": str(structured.source_quality_weight),
            "red_flag_penalty": str(structured.red_flag_penalty),
            "maximum_red_flag_penalty": str(
                structured.maximum_red_flag_penalty
            ),
        },
        "semantic": {
            "version": semantic.version,
            "structured_relevance_weight": str(
                semantic.structured_relevance_weight
            ),
            "semantic_relevance_weight": str(semantic.semantic_relevance_weight),
        },
    }


def _trace_sha256(trace: MatchTraceDraft) -> str:
    payload = {
        field: _json_value(value)
        for field, value in trace.__dict__.items()
        if field != "input_sha256"
    }
    return _canonical_sha256(payload)


def _canonical_sha256(payload: object) -> str:
    encoded = json.dumps(
        payload,
        ensure_ascii=True,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return sha256(encoded).hexdigest()


def _json_value(value: object) -> object:
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, datetime):
        return _aware_utc(value).isoformat()
    if isinstance(value, UUID):
        return str(value)
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, tuple):
        return [_json_value(item) for item in value]
    if isinstance(value, dict):
        return {key: _json_value(item) for key, item in value.items()}
    return value


def _bucketed_time(value: datetime, bucket_seconds: int) -> datetime:
    timestamp = int(value.timestamp())
    return datetime.fromtimestamp(
        timestamp - timestamp % bucket_seconds,
        tz=timezone.utc,
    )


def _aware_utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("matching timestamps must be timezone-aware")
    return value.astimezone(timezone.utc)


def _quantize(value: Decimal) -> Decimal:
    return min(Decimal("1"), max(Decimal("0"), value)).quantize(_SCORE_QUANTUM)
