from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from enum import Enum
import re
import unicodedata
from uuid import UUID

from .opportunity_analysis import OpportunityAnalysis, OpportunityType as AnalysisType
from .persistence.opportunities import (
    CanonicalOpportunityRecord,
    OpportunityLifecycleStatus,
)
from .persistence.search_profiles import (
    SearchProfileConfirmationStatus,
    SearchProfileRecord,
)
from .persistence.source_metrics import SourceQualitySnapshot
from .lexical_matching import (
    label_similarity as _lexical_label_similarity,
    label_matches,
    labels_f1,
    labels_have_overlap,
    lexical_concepts,
)
from .search_profiles import (
    BudgetPolicy,
    OpportunityType,
    SearchProfileTerm,
    WorkMode,
)


MATCHING_FILTER_VERSION = "matching-hard-filters.v4"
STRUCTURED_SCORING_VERSION = "structured-matching-score.v4"
STRUCTURED_SCORING_POLICY_VERSION = "structured-matching-policy.v4"
_VERSION_PATTERN = re.compile(r"^[a-z0-9][a-z0-9._-]{0,99}$")
_SCORE_QUANTUM = Decimal("0.0001")
_ROLE_FAMILY_CONCEPTS = frozenset(
    {
        "develop",
        "developer",
        "engine",
        "engineer",
        "program",
        "programmer",
        "cod",
        "coder",
        "разработчик",
        "разрабочик",
        "программист",
        "инженер",
        "специалист",
        "specialist",
    }
)
_GENERIC_SKILL_CONCEPTS = _ROLE_FAMILY_CONCEPTS | frozenset(
    {
        "work",
        "project",
        "service",
        "skill",
        "experience",
        "backend",
        "full",
        "stack",
    }
)


class HardFilterCode(str, Enum):
    OPPORTUNITY_NOT_ACTIVE = "opportunity_not_active"
    PROFILE_NOT_ACTIVE = "profile_not_active"
    PROFILE_NOT_CONFIRMED = "profile_not_confirmed"
    WORK_TYPE_MISMATCH = "work_type_mismatch"
    EXCLUDED_CATEGORY = "excluded_category"
    LANGUAGE_MISMATCH = "language_mismatch"
    GEOGRAPHY_MISMATCH = "geography_mismatch"
    WORK_MODE_MISMATCH = "work_mode_mismatch"
    BUDGET_NOT_EXPLICIT = "budget_not_explicit"
    BUDGET_BELOW_MINIMUM = "budget_below_minimum"


class UnknownMatchField(str, Enum):
    OPPORTUNITY_TYPE = "opportunity_type"
    CATEGORY = "category"
    LANGUAGE = "language"
    GEOGRAPHY = "geography"
    WORK_MODE = "work_mode"
    BUDGET = "budget"
    BUDGET_CURRENCY = "budget_currency"


class CandidateExclusionCode(str, Enum):
    NO_STRUCTURED_TARGET_OVERLAP = "no_structured_target_overlap"
    WORK_TYPE_MISMATCH = HardFilterCode.WORK_TYPE_MISMATCH.value
    LANGUAGE_MISMATCH = HardFilterCode.LANGUAGE_MISMATCH.value
    GEOGRAPHY_MISMATCH = HardFilterCode.GEOGRAPHY_MISMATCH.value


@dataclass(frozen=True)
class HardFilterFailure:
    code: HardFilterCode
    opportunity_value: str | None
    profile_values: tuple[str, ...]


@dataclass(frozen=True)
class HardFilterDecision:
    profile: SearchProfileRecord
    eligible: bool
    failures: tuple[HardFilterFailure, ...]
    nonblocking_unknowns: tuple[UnknownMatchField, ...]
    filter_version: str = MATCHING_FILTER_VERSION


@dataclass(frozen=True)
class CandidateExclusion:
    profile_id: UUID
    code: CandidateExclusionCode


@dataclass(frozen=True)
class CandidateNarrowingTrace:
    active_profile_count: int
    narrowed_candidate_count: int
    hard_filter_eligible_count: int
    semantic_score_candidate_count: int
    narrowing_dimensions: tuple[str, ...]
    exclusions: tuple[CandidateExclusion, ...]
    filter_version: str = MATCHING_FILTER_VERSION


@dataclass(frozen=True)
class CandidateNarrowingResult:
    opportunity_id: UUID
    opportunity_eligible: bool
    decisions: tuple[HardFilterDecision, ...]
    eligible_profiles: tuple[SearchProfileRecord, ...]
    trace: CandidateNarrowingTrace


class HardFilteredCandidateError(ValueError):
    def __init__(self, decision: HardFilterDecision) -> None:
        self.decision = decision
        reasons = ", ".join(failure.code.value for failure in decision.failures)
        super().__init__(f"Candidate failed hard filters: {reasons}")


@dataclass(frozen=True)
class StructuredScoringPolicy:
    version: str = STRUCTURED_SCORING_POLICY_VERSION
    role_weight: Decimal = Decimal("0.30")
    skills_weight: Decimal = Decimal("0.35")
    category_weight: Decimal = Decimal("0.15")
    work_type_weight: Decimal = Decimal("0.08")
    budget_weight: Decimal = Decimal("0.07")
    preferences_weight: Decimal = Decimal("0.05")
    relevance_weight: Decimal = Decimal("0.75")
    opportunity_quality_weight: Decimal = Decimal("0.15")
    source_quality_weight: Decimal = Decimal("0.10")
    red_flag_penalty: Decimal = Decimal("0.08")
    maximum_red_flag_penalty: Decimal = Decimal("0.32")

    def __post_init__(self) -> None:
        if not _VERSION_PATTERN.fullmatch(self.version):
            raise ValueError("structured scoring policy version is invalid")
        relevance_weights = (
            self.role_weight,
            self.skills_weight,
            self.category_weight,
            self.work_type_weight,
            self.budget_weight,
            self.preferences_weight,
        )
        aggregate_weights = (
            self.relevance_weight,
            self.opportunity_quality_weight,
            self.source_quality_weight,
        )
        if sum(relevance_weights, Decimal("0")) != Decimal("1"):
            raise ValueError("structured relevance weights must sum to 1")
        if sum(aggregate_weights, Decimal("0")) != Decimal("1"):
            raise ValueError("aggregate structured-score weights must sum to 1")
        if any(weight < 0 or weight > 1 for weight in relevance_weights):
            raise ValueError("structured relevance weights must be between 0 and 1")
        if any(weight < 0 or weight > 1 for weight in aggregate_weights):
            raise ValueError("aggregate score weights must be between 0 and 1")
        if self.red_flag_penalty < 0:
            raise ValueError("red-flag penalty must not be negative")
        if not Decimal("0") <= self.maximum_red_flag_penalty <= Decimal("1"):
            raise ValueError("maximum red-flag penalty must be between 0 and 1")


@dataclass(frozen=True)
class StructuredScoreComponent:
    name: str
    score: Decimal | None
    weight: Decimal
    contribution: Decimal
    evidence: tuple[str, ...]


@dataclass(frozen=True)
class StructuredCandidateScore:
    opportunity_id: UUID
    profile_id: UUID
    components: tuple[StructuredScoreComponent, ...]
    user_relevance_score: Decimal
    opportunity_quality_score: Decimal
    source_quality_score: Decimal | None
    red_flag_penalty: Decimal
    structured_score: Decimal
    source_quality_snapshot_id: int | None
    scoring_version: str = STRUCTURED_SCORING_VERSION
    policy_version: str = STRUCTURED_SCORING_POLICY_VERSION

    def component(self, name: str) -> StructuredScoreComponent:
        for component in self.components:
            if component.name == name:
                return component
        raise KeyError(name)


@dataclass(frozen=True)
class StructuredScoringResult:
    candidates: CandidateNarrowingResult
    scores: tuple[StructuredCandidateScore, ...]


def evaluate_hard_filters(
    opportunity: CanonicalOpportunityRecord,
    profile: SearchProfileRecord,
) -> HardFilterDecision:
    failures: list[HardFilterFailure] = []
    unknowns: list[UnknownMatchField] = []
    analysis = opportunity.analysis
    preferences = profile.preferences

    if opportunity.lifecycle_status is not OpportunityLifecycleStatus.ACTIVE:
        failures.append(
            _failure(
                HardFilterCode.OPPORTUNITY_NOT_ACTIVE,
                opportunity.lifecycle_status.value,
            )
        )
    if not profile.is_active:
        failures.append(_failure(HardFilterCode.PROFILE_NOT_ACTIVE, None))
    if (
        profile.confirmation_status
        is not SearchProfileConfirmationStatus.CONFIRMED
    ):
        failures.append(_failure(HardFilterCode.PROFILE_NOT_CONFIRMED, None))

    profile_type = _profile_opportunity_type(analysis.opportunity_type)
    if analysis.opportunity_type is AnalysisType.UNKNOWN:
        unknowns.append(UnknownMatchField.OPPORTUNITY_TYPE)
    elif (
        preferences.work_types is not None
        and (
            profile_type is None
            or profile_type not in preferences.work_types
        )
    ):
        failures.append(
            _failure(
                HardFilterCode.WORK_TYPE_MISMATCH,
                analysis.opportunity_type.value,
                tuple(item.value for item in preferences.work_types),
            )
        )

    category = _optional_identity(analysis.category)
    if category is None:
        unknowns.append(UnknownMatchField.CATEGORY)
    elif preferences.excluded_categories is not None:
        excluded = tuple(
            term.normalized_value for term in preferences.excluded_categories
        )
        if category in excluded:
            failures.append(
                _failure(
                    HardFilterCode.EXCLUDED_CATEGORY,
                    category,
                    excluded,
                )
            )

    _evaluate_term_constraint(
        failures,
        unknowns,
        opportunity_value=analysis.language,
        profile_values=preferences.languages,
        failure_code=HardFilterCode.LANGUAGE_MISMATCH,
        unknown_field=UnknownMatchField.LANGUAGE,
    )
    _evaluate_term_constraint(
        failures,
        unknowns,
        opportunity_value=analysis.work.location,
        profile_values=preferences.geographies,
        failure_code=HardFilterCode.GEOGRAPHY_MISMATCH,
        unknown_field=UnknownMatchField.GEOGRAPHY,
    )

    work_mode = _work_mode(analysis)
    if work_mode is None:
        unknowns.append(UnknownMatchField.WORK_MODE)
    elif (
        preferences.work_modes is not None
        and work_mode not in preferences.work_modes
    ):
        failures.append(
            _failure(
                HardFilterCode.WORK_MODE_MISMATCH,
                work_mode.value,
                tuple(item.value for item in preferences.work_modes),
            )
        )

    budget = analysis.budget
    if not budget.known:
        unknowns.append(UnknownMatchField.BUDGET)
        if (
            preferences.budget_policy is BudgetPolicy.REQUIRE_EXPLICIT
            and not budget.explicit
        ):
            failures.append(
                _failure(HardFilterCode.BUDGET_NOT_EXPLICIT, "unknown")
            )
    elif preferences.minimum_budget is not None:
        if (
            budget.currency is None
            or budget.currency.upper() != preferences.currency
        ):
            unknowns.append(UnknownMatchField.BUDGET_CURRENCY)
        else:
            comparable_amount = _maximum_budget(budget.min, budget.max)
            if comparable_amount < preferences.minimum_budget:
                failures.append(
                    _failure(
                        HardFilterCode.BUDGET_BELOW_MINIMUM,
                        str(comparable_amount),
                        (
                            str(preferences.minimum_budget),
                            preferences.currency or "",
                        ),
                    )
                )

    return HardFilterDecision(
        profile=profile,
        eligible=not failures,
        failures=tuple(failures),
        nonblocking_unknowns=tuple(dict.fromkeys(unknowns)),
    )


def narrow_and_filter_candidates(
    opportunity: CanonicalOpportunityRecord,
    profiles: tuple[SearchProfileRecord, ...],
) -> CandidateNarrowingResult:
    active_profiles = tuple(
        profile
        for profile in profiles
        if profile.is_active
        and profile.confirmation_status is SearchProfileConfirmationStatus.CONFIRMED
    )
    dimensions = _available_narrowing_dimensions(opportunity.analysis)
    candidate_profiles: list[SearchProfileRecord] = []
    exclusions: list[CandidateExclusion] = []

    if opportunity.lifecycle_status is OpportunityLifecycleStatus.ACTIVE:
        for profile in active_profiles:
            exclusion = _candidate_exclusion(opportunity.analysis, profile)
            if exclusion is None:
                candidate_profiles.append(profile)
            else:
                exclusions.append(CandidateExclusion(profile.id, exclusion))

    decisions = tuple(
        evaluate_hard_filters(opportunity, profile) for profile in candidate_profiles
    )
    eligible_profiles = tuple(
        decision.profile for decision in decisions if decision.eligible
    )
    trace = CandidateNarrowingTrace(
        active_profile_count=len(active_profiles),
        narrowed_candidate_count=len(candidate_profiles),
        hard_filter_eligible_count=len(eligible_profiles),
        semantic_score_candidate_count=len(eligible_profiles),
        narrowing_dimensions=dimensions,
        exclusions=tuple(exclusions),
    )
    return CandidateNarrowingResult(
        opportunity_id=opportunity.id,
        opportunity_eligible=(
            opportunity.lifecycle_status is OpportunityLifecycleStatus.ACTIVE
        ),
        decisions=decisions,
        eligible_profiles=eligible_profiles,
        trace=trace,
    )


def score_candidate_structured(
    opportunity: CanonicalOpportunityRecord,
    profile: SearchProfileRecord,
    *,
    source_quality: SourceQualitySnapshot | None = None,
    policy: StructuredScoringPolicy | None = None,
) -> StructuredCandidateScore:
    selected_policy = policy or StructuredScoringPolicy()
    decision = evaluate_hard_filters(opportunity, profile)
    if not decision.eligible:
        raise HardFilteredCandidateError(decision)

    analysis = opportunity.analysis
    components = (
        _role_component(analysis, profile, selected_policy.role_weight),
        _skills_component(analysis, profile, selected_policy.skills_weight),
        _category_component(analysis, profile, selected_policy.category_weight),
        _work_type_component(
            analysis,
            profile,
            selected_policy.work_type_weight,
        ),
        _budget_component(analysis, profile, selected_policy.budget_weight),
        _preferences_component(
            analysis,
            profile,
            selected_policy.preferences_weight,
        ),
    )
    relevance_score = _quantize(
        sum((component.contribution for component in components), Decimal("0"))
    )
    opportunity_quality_score = _opportunity_quality_score(analysis)
    source_quality_score = (
        None if source_quality is None else _source_quality_score(source_quality)
    )
    red_flag_penalty = _quantize(
        min(
            selected_policy.maximum_red_flag_penalty,
            selected_policy.red_flag_penalty * len(analysis.red_flags),
        )
    )
    aggregate = (
        relevance_score * selected_policy.relevance_weight
        + opportunity_quality_score * selected_policy.opportunity_quality_weight
    )
    if source_quality_score is not None:
        aggregate += source_quality_score * selected_policy.source_quality_weight
    structured_score = _quantize(max(Decimal("0"), aggregate - red_flag_penalty))
    return StructuredCandidateScore(
        opportunity_id=opportunity.id,
        profile_id=profile.id,
        components=components,
        user_relevance_score=relevance_score,
        opportunity_quality_score=opportunity_quality_score,
        source_quality_score=source_quality_score,
        red_flag_penalty=red_flag_penalty,
        structured_score=structured_score,
        source_quality_snapshot_id=(
            None if source_quality is None else source_quality.id
        ),
        policy_version=selected_policy.version,
    )


def score_narrowed_candidates(
    opportunity: CanonicalOpportunityRecord,
    profiles: tuple[SearchProfileRecord, ...],
    *,
    source_quality: SourceQualitySnapshot | None = None,
    policy: StructuredScoringPolicy | None = None,
) -> StructuredScoringResult:
    candidates = narrow_and_filter_candidates(opportunity, profiles)
    scores = tuple(
        score_candidate_structured(
            opportunity,
            profile,
            source_quality=source_quality,
            policy=policy,
        )
        for profile in candidates.eligible_profiles
    )
    return StructuredScoringResult(candidates=candidates, scores=scores)


def _role_component(
    analysis: OpportunityAnalysis,
    profile: SearchProfileRecord,
    weight: Decimal,
) -> StructuredScoreComponent:
    opportunity_role = _optional_identity(analysis.role_title)
    profile_roles = frozenset(term.normalized_value for term in profile.roles)
    if opportunity_role is None or not profile_roles:
        return _score_component("role", None, weight)
    score = max(
        (_label_similarity(opportunity_role, role) for role in profile_roles),
        default=Decimal("0"),
    )
    if score == Decimal("0") and _role_family_compatible(
        opportunity_role,
        profile_roles,
    ) and _has_specific_skill_overlap(analysis, profile):
        # A generic role label such as "Backend developer" carries useful
        # role evidence when it is paired with a profile-specific technical
        # skill.  This is a calibration of evidence interpretation, not a
        # threshold change or a generic role-only match.
        score = Decimal("0.6000")
    matched = tuple(
        sorted(
            role
            for role in profile_roles
            if _label_similarity(opportunity_role, role)
        )
    )
    if score == Decimal("0.6000"):
        matched = tuple(sorted((*matched, "role_family_with_specific_skill")))
    return _score_component("role", score, weight, matched)


def _skills_component(
    analysis: OpportunityAnalysis,
    profile: SearchProfileRecord,
    weight: Decimal,
) -> StructuredScoreComponent:
    opportunity_skills = frozenset(
        identity
        for skill in analysis.skills
        if (identity := _optional_identity(skill)) is not None
    )
    profile_skills = frozenset(term.normalized_value for term in profile.skills)
    if not opportunity_skills or not profile_skills:
        return _score_component("skills", None, weight)
    score, matches = labels_f1(opportunity_skills, profile_skills)
    if (
        score < Decimal("0.5000")
        and _has_specific_skill_overlap(analysis, profile)
        and _role_supports_skill_overlap(analysis, profile)
    ):
        # One exact, profile-specific skill in a coherent role context is
        # stronger evidence than the raw set F1 when the opportunity names
        # additional implementation skills absent from the profile.
        score = Decimal("0.5000")
    evidence = tuple(
        sorted(
            profile_skill
            for _, profile_skill, similarity in matches
            if similarity
        )
    )
    return _score_component("skills", score, weight, evidence)


def _role_family_compatible(
    opportunity_role: str,
    profile_roles: frozenset[str],
) -> bool:
    opportunity_concepts = lexical_concepts(opportunity_role)
    if not opportunity_concepts & _ROLE_FAMILY_CONCEPTS:
        return False
    return any(
        lexical_concepts(role) & _ROLE_FAMILY_CONCEPTS
        for role in profile_roles
    )


def _has_specific_skill_overlap(
    analysis: OpportunityAnalysis,
    profile: SearchProfileRecord,
) -> bool:
    opportunity_skills = tuple(
        identity
        for skill in analysis.skills
        if (identity := _optional_identity(skill)) is not None
    )
    profile_skills = tuple(term.normalized_value for term in profile.skills)
    if not opportunity_skills or not profile_skills:
        return False
    return any(
        similarity >= Decimal("0.7500")
        and any(
            concept not in _GENERIC_SKILL_CONCEPTS
            for concept in lexical_concepts(opportunity_skill)
        )
        and any(
            concept not in _GENERIC_SKILL_CONCEPTS
            for concept in lexical_concepts(profile_skill)
        )
        for opportunity_skill, profile_skill, similarity in label_matches(
            opportunity_skills,
            profile_skills,
        )
    )


def _role_supports_skill_overlap(
    analysis: OpportunityAnalysis,
    profile: SearchProfileRecord,
) -> bool:
    opportunity_role = _optional_identity(analysis.role_title)
    profile_roles = frozenset(term.normalized_value for term in profile.roles)
    if opportunity_role is None or not profile_roles:
        return False
    return (
        any(_label_similarity(opportunity_role, role) > Decimal("0") for role in profile_roles)
        or _role_family_compatible(opportunity_role, profile_roles)
    )


def _category_component(
    analysis: OpportunityAnalysis,
    profile: SearchProfileRecord,
    weight: Decimal,
) -> StructuredScoreComponent:
    opportunity_category = _optional_identity(analysis.category)
    profile_categories = frozenset(
        term.normalized_value for term in profile.categories
    )
    if opportunity_category is None or not profile_categories:
        return _score_component("category", None, weight)
    score = max(
        (
            _label_similarity(opportunity_category, category)
            for category in profile_categories
        ),
        default=Decimal("0"),
    )
    matched = tuple(
        sorted(
            category
            for category in profile_categories
            if _label_similarity(opportunity_category, category)
        )
    )
    return _score_component("category", score, weight, matched)


def _work_type_component(
    analysis: OpportunityAnalysis,
    profile: SearchProfileRecord,
    weight: Decimal,
) -> StructuredScoreComponent:
    opportunity_type = _profile_opportunity_type(analysis.opportunity_type)
    if (
        analysis.opportunity_type is AnalysisType.UNKNOWN
        or profile.preferences.work_types is None
    ):
        return _score_component("work_type", None, weight)
    if opportunity_type is None:
        return _score_component("work_type", Decimal("0"), weight)
    score = Decimal(int(opportunity_type in profile.preferences.work_types))
    evidence = (opportunity_type.value,) if score else ()
    return _score_component("work_type", score, weight, evidence)


def _budget_component(
    analysis: OpportunityAnalysis,
    profile: SearchProfileRecord,
    weight: Decimal,
) -> StructuredScoreComponent:
    preferences = profile.preferences
    budget = analysis.budget
    if preferences.minimum_budget is None:
        return _score_component("budget", None, weight)
    if not budget.known:
        score = Decimal("0.75") if budget.explicit else Decimal("0.50")
        evidence = ("negotiable",) if budget.explicit else ("unknown",)
        return _score_component("budget", score, weight, evidence)
    if budget.currency is None or budget.currency.upper() != preferences.currency:
        return _score_component("budget", None, weight)
    amount = _maximum_budget(budget.min, budget.max)
    score = Decimal(int(amount >= preferences.minimum_budget))
    return _score_component(
        "budget",
        score,
        weight,
        (str(amount), preferences.currency or ""),
    )


def _preferences_component(
    analysis: OpportunityAnalysis,
    profile: SearchProfileRecord,
    weight: Decimal,
) -> StructuredScoreComponent:
    scores: list[Decimal] = []
    evidence: list[str] = []
    preferences = profile.preferences
    constraints = (
        (
            "language",
            _optional_identity(analysis.language),
            preferences.languages,
        ),
        (
            "geography",
            _optional_identity(analysis.work.location),
            preferences.geographies,
        ),
    )
    for name, opportunity_value, profile_values in constraints:
        if opportunity_value is None or profile_values is None:
            continue
        accepted = {term.normalized_value for term in profile_values}
        scores.append(Decimal(int(opportunity_value in accepted)))
        if opportunity_value in accepted:
            evidence.append(f"{name}:{opportunity_value}")

    work_mode = _work_mode(analysis)
    if work_mode is not None and preferences.work_modes is not None:
        scores.append(Decimal(int(work_mode in preferences.work_modes)))
        if work_mode in preferences.work_modes:
            evidence.append(f"work_mode:{work_mode.value}")
    if not scores:
        return _score_component("preferences", None, weight)
    return _score_component(
        "preferences",
        sum(scores, Decimal("0")) / len(scores),
        weight,
        tuple(evidence),
    )


def _opportunity_quality_score(analysis: OpportunityAnalysis) -> Decimal:
    quality = analysis.quality
    values = (
        quality.actionability,
        quality.commercial_plausibility,
        quality.specificity,
        quality.credibility,
    )
    return _quantize(
        sum((Decimal(str(value)) for value in values), Decimal("0")) / len(values)
    )


def _source_quality_score(snapshot: SourceQualitySnapshot) -> Decimal:
    positive = (
        snapshot.opportunity_yield * Decimal("0.60")
        + snapshot.buyer_intent_ratio * Decimal("0.40")
    )
    negative = (
        snapshot.seller_ratio * Decimal("0.40")
        + snapshot.spam_ratio * Decimal("0.40")
        + snapshot.duplicate_ratio * Decimal("0.20")
    )
    return _quantize(_clamp(positive * (Decimal("1") - negative)))


def _label_similarity(left: str, right: str) -> Decimal:
    return _lexical_label_similarity(left, right)


def _score_component(
    name: str,
    score: Decimal | None,
    weight: Decimal,
    evidence: tuple[str, ...] = (),
) -> StructuredScoreComponent:
    normalized_score = None if score is None else _quantize(_clamp(score))
    contribution = (
        Decimal("0")
        if normalized_score is None
        else _quantize(normalized_score * weight)
    )
    return StructuredScoreComponent(
        name=name,
        score=normalized_score,
        weight=weight,
        contribution=contribution,
        evidence=evidence,
    )


def _clamp(value: Decimal) -> Decimal:
    return min(Decimal("1"), max(Decimal("0"), value))


def _quantize(value: Decimal) -> Decimal:
    return value.quantize(_SCORE_QUANTUM)


def _candidate_exclusion(
    analysis: OpportunityAnalysis,
    profile: SearchProfileRecord,
) -> CandidateExclusionCode | None:
    profile_type = _profile_opportunity_type(analysis.opportunity_type)
    if (
        analysis.opportunity_type is not AnalysisType.UNKNOWN
        and profile.preferences.work_types is not None
        and (
            profile_type is None
            or profile_type not in profile.preferences.work_types
        )
    ):
        return CandidateExclusionCode.WORK_TYPE_MISMATCH

    if _known_term_mismatch(analysis.language, profile.preferences.languages):
        return CandidateExclusionCode.LANGUAGE_MISMATCH
    if _known_term_mismatch(
        analysis.work.location,
        profile.preferences.geographies,
    ):
        return CandidateExclusionCode.GEOGRAPHY_MISMATCH

    opportunity_targets = _opportunity_targets(analysis)
    profile_targets = _profile_targets(profile)
    if (
        opportunity_targets
        and profile_targets
        and not _target_overlap(opportunity_targets, profile_targets)
    ):
        return CandidateExclusionCode.NO_STRUCTURED_TARGET_OVERLAP
    return None


def _available_narrowing_dimensions(
    analysis: OpportunityAnalysis,
) -> tuple[str, ...]:
    dimensions: list[str] = []
    if _opportunity_targets(analysis):
        dimensions.append("category_role_skills")
    if analysis.opportunity_type is not AnalysisType.UNKNOWN:
        dimensions.append("work_type")
    if _optional_identity(analysis.language) is not None:
        dimensions.append("language")
    if _optional_identity(analysis.work.location) is not None:
        dimensions.append("geography")
    return tuple(dimensions)


def _profile_opportunity_type(value: AnalysisType) -> OpportunityType | None:
    try:
        return OpportunityType(value.value)
    except ValueError:
        return None


def _work_mode(analysis: OpportunityAnalysis) -> WorkMode | None:
    if analysis.work.remote is True:
        return WorkMode.REMOTE
    if analysis.work.remote is False:
        return WorkMode.ON_SITE
    return None


def _evaluate_term_constraint(
    failures: list[HardFilterFailure],
    unknowns: list[UnknownMatchField],
    *,
    opportunity_value: str | None,
    profile_values: tuple[SearchProfileTerm, ...] | None,
    failure_code: HardFilterCode,
    unknown_field: UnknownMatchField,
) -> None:
    identity = _optional_identity(opportunity_value)
    if identity is None:
        unknowns.append(unknown_field)
        return
    if profile_values is None:
        return
    accepted = tuple(term.normalized_value for term in profile_values)
    if identity not in accepted:
        failures.append(_failure(failure_code, identity, accepted))


def _known_term_mismatch(
    opportunity_value: str | None,
    profile_values: tuple[SearchProfileTerm, ...] | None,
) -> bool:
    identity = _optional_identity(opportunity_value)
    if identity is None or profile_values is None:
        return False
    return identity not in {term.normalized_value for term in profile_values}


def _opportunity_targets(analysis: OpportunityAnalysis) -> frozenset[str]:
    return frozenset(
        identity
        for value in (analysis.category, analysis.role_title, *analysis.skills)
        if (identity := _optional_identity(value)) is not None
    )


def _profile_targets(profile: SearchProfileRecord) -> frozenset[str]:
    return frozenset(
        term.normalized_value
        for terms in (profile.categories, profile.roles, profile.skills)
        for term in terms
    )


def _target_overlap(
    opportunity_targets: frozenset[str],
    profile_targets: frozenset[str],
) -> bool:
    return labels_have_overlap(opportunity_targets, profile_targets)


def _tokens(value: str) -> frozenset[str]:
    return lexical_concepts(value)


def _optional_identity(value: str | None) -> str | None:
    if value is None:
        return None
    normalized = re.sub(r"\s+", " ", unicodedata.normalize("NFKC", value)).strip()
    return normalized.casefold() or None


def _maximum_budget(minimum: float | None, maximum: float | None) -> Decimal:
    value = maximum if maximum is not None else minimum
    if value is None:
        raise ValueError("known budget requires a comparable amount")
    return Decimal(str(value))


def _failure(
    code: HardFilterCode,
    opportunity_value: str | None,
    profile_values: tuple[str, ...] = (),
) -> HardFilterFailure:
    return HardFilterFailure(
        code=code,
        opportunity_value=opportunity_value,
        profile_values=profile_values,
    )
