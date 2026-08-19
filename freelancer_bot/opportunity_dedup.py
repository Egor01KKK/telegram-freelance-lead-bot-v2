from __future__ import annotations

from collections import Counter
from dataclasses import dataclass
from difflib import SequenceMatcher
from urllib.parse import urlsplit, urlunsplit
import unicodedata

from .opportunity_analysis import OpportunityAnalysis


STRUCTURED_DEDUP_ALGORITHM_VERSION = "canonical-opportunity-signals.v1"
STRUCTURED_DEDUP_RELATION = "semantic_duplicate"
PREFERRED_SOURCE_POLICY_VERSION = "canonical-source-earliest-message.v1"


@dataclass(frozen=True)
class StructuredDedupPolicy:
    contact_task_threshold: float = 0.7
    budget_task_threshold: float = 0.84
    semantic_task_threshold: float = 0.96
    minimum_task_tokens: int = 6
    minimum_semantic_only_tokens: int = 8

    def __post_init__(self) -> None:
        thresholds = (
            self.contact_task_threshold,
            self.budget_task_threshold,
            self.semantic_task_threshold,
        )
        if any(not 0 < threshold <= 1 for threshold in thresholds):
            raise ValueError("Structured dedup thresholds must be in (0, 1]")
        if not (
            self.contact_task_threshold
            <= self.budget_task_threshold
            <= self.semantic_task_threshold
        ):
            raise ValueError("Structured dedup thresholds must be nondecreasing")
        if self.minimum_task_tokens < 3:
            raise ValueError("Structured dedup requires at least three task tokens")
        if self.minimum_semantic_only_tokens < self.minimum_task_tokens:
            raise ValueError("Semantic-only token minimum cannot be lower")


@dataclass(frozen=True)
class StructuredDedupDecision:
    similarity: float
    evidence: dict[str, object]


def evaluate_structured_duplicate(
    incoming: OpportunityAnalysis,
    candidate: OpportunityAnalysis,
    *,
    policy: StructuredDedupPolicy,
) -> StructuredDedupDecision | None:
    if not incoming.is_opportunity or not candidate.is_opportunity:
        return None
    if not _opportunity_types_compatible(incoming, candidate):
        return None

    incoming_task = _tokens(incoming.task_summary)
    candidate_task = _tokens(candidate.task_summary)
    if min(len(incoming_task), len(candidate_task)) < policy.minimum_task_tokens:
        return None
    task_similarity = _token_similarity(incoming_task, candidate_task)
    if not _roles_compatible(incoming.role_title, candidate.role_title):
        return None

    incoming_contacts = _contacts(incoming)
    candidate_contacts = _contacts(candidate)
    shared_contact_fields = tuple(
        sorted(
            field
            for field in incoming_contacts.keys() & candidate_contacts.keys()
            if incoming_contacts[field] == candidate_contacts[field]
        )
    )
    conflicting_contact_fields = tuple(
        sorted(
            field
            for field in incoming_contacts.keys() & candidate_contacts.keys()
            if incoming_contacts[field] != candidate_contacts[field]
        )
    )
    if conflicting_contact_fields:
        return None

    budget_relation = _budget_relation(incoming, candidate)
    if budget_relation == "conflict":
        return None

    decision_rule: str | None = None
    if shared_contact_fields and task_similarity >= policy.contact_task_threshold:
        decision_rule = "shared_contact_and_task"
    elif (
        budget_relation in {"exact", "overlap"}
        and task_similarity >= policy.budget_task_threshold
    ):
        decision_rule = "compatible_budget_and_task"
    elif (
        min(len(incoming_task), len(candidate_task))
        >= policy.minimum_semantic_only_tokens
        and task_similarity >= policy.semantic_task_threshold
    ):
        decision_rule = "analysis_semantic_task"
    if decision_rule is None:
        return None

    return StructuredDedupDecision(
        similarity=task_similarity,
        evidence={
            "decision_rule": decision_rule,
            "task_similarity": round(task_similarity, 6),
            "task_token_floor": min(len(incoming_task), len(candidate_task)),
            "shared_contact_fields": list(shared_contact_fields),
            "budget_relation": budget_relation,
            "opportunity_type": incoming.opportunity_type.value,
        },
    )


def _tokens(value: str | None) -> tuple[str, ...]:
    if value is None:
        return ()
    normalized = unicodedata.normalize("NFKC", value).casefold()
    return tuple(
        "".join(character for character in token if character.isalnum())
        for token in normalized.split()
        if any(character.isalnum() for character in token)
    )


def _token_similarity(left: tuple[str, ...], right: tuple[str, ...]) -> float:
    left_counts = Counter(left)
    right_counts = Counter(right)
    overlap = sum((left_counts & right_counts).values())
    multiset_similarity = overlap / max(len(left), len(right))
    sequence_similarity = SequenceMatcher(None, left, right, autojunk=False).ratio()
    return min(multiset_similarity, sequence_similarity)


def _opportunity_types_compatible(
    incoming: OpportunityAnalysis,
    candidate: OpportunityAnalysis,
) -> bool:
    incoming_type = incoming.opportunity_type.value
    candidate_type = candidate.opportunity_type.value
    return (
        incoming_type == candidate_type
        or incoming_type == "unknown"
        or candidate_type == "unknown"
    )


def _roles_compatible(left: str | None, right: str | None) -> bool:
    left_tokens = _tokens(left)
    right_tokens = _tokens(right)
    if not left_tokens or not right_tokens:
        return True
    return _token_similarity(left_tokens, right_tokens) >= 0.5


def _contacts(analysis: OpportunityAnalysis) -> dict[str, str]:
    contact = analysis.contact
    values = {
        "telegram": _normalize_telegram(contact.telegram),
        "email": _normalize_email(contact.email),
        "url": _normalize_url(contact.url),
    }
    return {field: value for field, value in values.items() if value is not None}


def _normalize_telegram(value: str | None) -> str | None:
    if value is None:
        return None
    return value.strip().casefold().removeprefix("https://t.me/").removeprefix("@")


def _normalize_email(value: str | None) -> str | None:
    return None if value is None else value.strip().casefold()


def _normalize_url(value: str | None) -> str | None:
    if value is None:
        return None
    parsed = urlsplit(value.strip())
    if not parsed.scheme or not parsed.netloc:
        return value.strip().casefold().rstrip("/")
    return urlunsplit(
        (
            parsed.scheme.casefold(),
            parsed.netloc.casefold(),
            parsed.path.rstrip("/"),
            parsed.query,
            "",
        )
    )


def _budget_relation(
    incoming: OpportunityAnalysis,
    candidate: OpportunityAnalysis,
) -> str:
    left = incoming.budget
    right = candidate.budget
    if not left.known and not right.known:
        return "both_unknown"
    if not left.known or not right.known:
        return "one_unknown"
    if (
        left.currency is not None
        and right.currency is not None
        and left.currency.casefold() != right.currency.casefold()
    ):
        return "conflict"
    if (
        left.period is not None
        and right.period is not None
        and left.period.casefold() != right.period.casefold()
    ):
        return "conflict"
    left_min, left_max = _budget_bounds(left.min, left.max)
    right_min, right_max = _budget_bounds(right.min, right.max)
    if left_max < right_min or right_max < left_min:
        return "conflict"
    if left_min == right_min and left_max == right_max:
        return "exact"
    return "overlap"


def _budget_bounds(
    minimum: float | None,
    maximum: float | None,
) -> tuple[float, float]:
    if minimum is None:
        if maximum is None:
            raise ValueError("Known budget requires at least one bound")
        return maximum, maximum
    if maximum is None:
        return minimum, minimum
    return minimum, maximum
