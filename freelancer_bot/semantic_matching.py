from __future__ import annotations

from collections import OrderedDict
from dataclasses import dataclass
from decimal import Decimal
from enum import Enum
from hashlib import sha256
import math
import re
from typing import Mapping, Protocol, runtime_checkable
import unicodedata
from uuid import UUID

from .matching import (
    STRUCTURED_SCORING_POLICY_VERSION,
    StructuredCandidateScore,
    StructuredScoringPolicy,
    StructuredScoringResult,
    score_narrowed_candidates,
)
from .lexical_matching import (
    lexical_acronym,
    lexical_concepts,
    lexical_token_sequence,
)
from .persistence.opportunities import CanonicalOpportunityRecord
from .persistence.search_profiles import SearchProfileRecord
from .persistence.source_metrics import SourceQualitySnapshot


SEMANTIC_REPRESENTATION_SCHEMA_VERSION = "semantic-representation.v1"
SEMANTIC_MATCHING_VERSION = "semantic-matching-score.v3"
SEMANTIC_MATCHING_POLICY_VERSION = "semantic-matching-policy.v1"
LOCAL_EMBEDDING_PROVIDER = "local-feature-hash"
LOCAL_EMBEDDING_MODEL = "word-bigram-char-translit-hash-384"
LOCAL_EMBEDDING_MODEL_VERSION = "word-bigram-char-translit-hash.v3"

_VERSION_PATTERN = re.compile(r"^[a-z0-9][a-z0-9._-]{0,99}$")
_SCORE_QUANTUM = Decimal("0.0001")
_GENERIC_TOKENS = frozenset(
    {
        "developer",
        "job",
        "project",
        "specialist",
        "work",
        "заказ",
        "проект",
        "работа",
        "специалист",
        "develop",
        "разработчик",
    }
)


class SemanticProviderUnavailable(RuntimeError):
    pass


class SemanticStatus(str, Enum):
    AVAILABLE = "available"
    DEGRADED = "degraded"
    UNAVAILABLE_INPUT = "unavailable_input"


@dataclass(frozen=True)
class SemanticRepresentation:
    schema_version: str
    provider: str
    model: str
    model_version: str
    input_sha256: str
    dimensions: int
    vector: tuple[float, ...]

    def __post_init__(self) -> None:
        if self.schema_version != SEMANTIC_REPRESENTATION_SCHEMA_VERSION:
            raise ValueError("unsupported semantic representation schema")
        for value in (self.provider, self.model, self.model_version):
            if not _VERSION_PATTERN.fullmatch(value):
                raise ValueError("semantic representation identity is invalid")
        if not re.fullmatch(r"[0-9a-f]{64}", self.input_sha256):
            raise ValueError("semantic representation input hash is invalid")
        if self.dimensions <= 0 or len(self.vector) != self.dimensions:
            raise ValueError("semantic representation dimensions are inconsistent")
        if any(not math.isfinite(value) for value in self.vector):
            raise ValueError("semantic representation must contain finite values")


@dataclass(frozen=True)
class SemanticCacheInfo:
    entries: int
    capacity: int
    hits: int
    misses: int


@runtime_checkable
class SemanticEmbeddingProvider(Protocol):
    @property
    def provider(self) -> str: ...

    @property
    def model(self) -> str: ...

    @property
    def model_version(self) -> str: ...

    def embed(self, text: str) -> SemanticRepresentation: ...


class DeterministicHashEmbeddingProvider:
    provider = LOCAL_EMBEDDING_PROVIDER
    model_version = LOCAL_EMBEDDING_MODEL_VERSION

    def __init__(self, *, dimensions: int = 384, cache_size: int = 10_000) -> None:
        if dimensions < 64 or dimensions > 4096:
            raise ValueError("embedding dimensions must be between 64 and 4096")
        if cache_size < 1 or cache_size > 100_000:
            raise ValueError("embedding cache_size must be between 1 and 100000")
        self._dimensions = dimensions
        self.model = f"word-bigram-char-translit-hash-{dimensions}"
        self._cache_size = cache_size
        self._cache: OrderedDict[str, SemanticRepresentation] = OrderedDict()
        self._hits = 0
        self._misses = 0

    def embed(self, text: str) -> SemanticRepresentation:
        normalized = _normalize_semantic_text(text)
        input_sha256 = sha256(normalized.encode("utf-8")).hexdigest()
        cached = self._cache.get(input_sha256)
        if cached is not None:
            self._hits += 1
            self._cache.move_to_end(input_sha256)
            return cached

        self._misses += 1
        vector = _feature_hash_vector(normalized, self._dimensions)
        representation = SemanticRepresentation(
            schema_version=SEMANTIC_REPRESENTATION_SCHEMA_VERSION,
            provider=self.provider,
            model=self.model,
            model_version=self.model_version,
            input_sha256=input_sha256,
            dimensions=self._dimensions,
            vector=vector,
        )
        self._cache[input_sha256] = representation
        self._cache.move_to_end(input_sha256)
        if len(self._cache) > self._cache_size:
            self._cache.popitem(last=False)
        return representation

    def cache_info(self) -> SemanticCacheInfo:
        return SemanticCacheInfo(
            entries=len(self._cache),
            capacity=self._cache_size,
            hits=self._hits,
            misses=self._misses,
        )


@dataclass(frozen=True)
class SemanticMatchingPolicy:
    version: str = SEMANTIC_MATCHING_POLICY_VERSION
    structured_relevance_weight: Decimal = Decimal("0.70")
    semantic_relevance_weight: Decimal = Decimal("0.30")

    def __post_init__(self) -> None:
        if not _VERSION_PATTERN.fullmatch(self.version):
            raise ValueError("semantic matching policy version is invalid")
        if (
            self.structured_relevance_weight + self.semantic_relevance_weight
            != Decimal("1")
        ):
            raise ValueError("semantic relevance weights must sum to 1")
        if not Decimal("0") <= self.structured_relevance_weight <= Decimal("1"):
            raise ValueError("structured relevance weight must be between 0 and 1")
        if not Decimal("0") <= self.semantic_relevance_weight <= Decimal("1"):
            raise ValueError("semantic relevance weight must be between 0 and 1")


@dataclass(frozen=True)
class SemanticCandidateScore:
    structured: StructuredCandidateScore
    semantic_similarity: Decimal | None
    combined_relevance_score: Decimal
    combined_score: Decimal
    opportunity_representation_sha256: str | None
    profile_representation_sha256: str | None
    provider: str | None
    model: str | None
    model_version: str | None
    semantic_matching_version: str = SEMANTIC_MATCHING_VERSION
    semantic_policy_version: str = SEMANTIC_MATCHING_POLICY_VERSION


@dataclass(frozen=True)
class SemanticScoringResult:
    structured: StructuredScoringResult
    scores: tuple[SemanticCandidateScore, ...]
    status: SemanticStatus
    degraded_reason: str | None


def score_candidates_semantic(
    opportunity: CanonicalOpportunityRecord,
    profiles: tuple[SearchProfileRecord, ...],
    *,
    provider: SemanticEmbeddingProvider | None,
    source_quality: SourceQualitySnapshot | None = None,
    structured_policy: StructuredScoringPolicy | None = None,
    semantic_policy: SemanticMatchingPolicy | None = None,
) -> SemanticScoringResult:
    selected_structured_policy = structured_policy or StructuredScoringPolicy()
    selected_semantic_policy = semantic_policy or SemanticMatchingPolicy()
    structured = score_narrowed_candidates(
        opportunity,
        profiles,
        source_quality=source_quality,
        policy=selected_structured_policy,
    )
    if not structured.scores:
        return SemanticScoringResult(
            structured=structured,
            scores=(),
            status=SemanticStatus.UNAVAILABLE_INPUT,
            degraded_reason="no_hard_filter_eligible_candidates",
        )
    if provider is None:
        return _structured_fallback(
            structured,
            status=SemanticStatus.DEGRADED,
            reason="semantic_provider_unavailable",
            semantic_policy=selected_semantic_policy,
        )

    opportunity_text = _opportunity_semantic_text(opportunity)
    if opportunity_text is None:
        return _structured_fallback(
            structured,
            status=SemanticStatus.UNAVAILABLE_INPUT,
            reason="opportunity_semantic_input_missing",
            semantic_policy=selected_semantic_policy,
        )

    profiles_by_id = {
        profile.id: profile for profile in structured.candidates.eligible_profiles
    }
    try:
        opportunity_representation = provider.embed(opportunity_text)
        representations = {
            score.profile_id: provider.embed(
                _profile_semantic_text(profiles_by_id[score.profile_id])
            )
            for score in structured.scores
        }
    except SemanticProviderUnavailable:
        return _structured_fallback(
            structured,
            status=SemanticStatus.DEGRADED,
            reason="semantic_provider_unavailable",
            semantic_policy=selected_semantic_policy,
        )

    _validate_provider_batch(opportunity_representation, representations)
    scores = tuple(
        _combine_score(
            structured_score,
            opportunity_representation,
            representations[structured_score.profile_id],
            structured_policy=selected_structured_policy,
            semantic_policy=selected_semantic_policy,
        )
        for structured_score in structured.scores
    )
    return SemanticScoringResult(
        structured=structured,
        scores=scores,
        status=SemanticStatus.AVAILABLE,
        degraded_reason=None,
    )


def _combine_score(
    structured: StructuredCandidateScore,
    opportunity_representation: SemanticRepresentation,
    profile_representation: SemanticRepresentation,
    *,
    structured_policy: StructuredScoringPolicy,
    semantic_policy: SemanticMatchingPolicy,
) -> SemanticCandidateScore:
    similarity = _cosine_similarity(
        opportunity_representation.vector,
        profile_representation.vector,
    )
    combined_relevance = _quantize(
        structured.user_relevance_score
        * semantic_policy.structured_relevance_weight
        + similarity * semantic_policy.semantic_relevance_weight
    )
    aggregate = (
        combined_relevance * structured_policy.relevance_weight
        + structured.opportunity_quality_score
        * structured_policy.opportunity_quality_weight
    )
    if structured.source_quality_score is not None:
        aggregate += (
            structured.source_quality_score * structured_policy.source_quality_weight
        )
    combined_score = _quantize(
        _clamp(aggregate - structured.red_flag_penalty)
    )
    return SemanticCandidateScore(
        structured=structured,
        semantic_similarity=similarity,
        combined_relevance_score=combined_relevance,
        combined_score=combined_score,
        opportunity_representation_sha256=(
            opportunity_representation.input_sha256
        ),
        profile_representation_sha256=profile_representation.input_sha256,
        provider=opportunity_representation.provider,
        model=opportunity_representation.model,
        model_version=opportunity_representation.model_version,
        semantic_policy_version=semantic_policy.version,
    )


def _structured_fallback(
    structured: StructuredScoringResult,
    *,
    status: SemanticStatus,
    reason: str,
    semantic_policy: SemanticMatchingPolicy,
) -> SemanticScoringResult:
    scores = tuple(
        SemanticCandidateScore(
            structured=score,
            semantic_similarity=None,
            combined_relevance_score=score.user_relevance_score,
            combined_score=score.structured_score,
            opportunity_representation_sha256=None,
            profile_representation_sha256=None,
            provider=None,
            model=None,
            model_version=None,
            semantic_policy_version=semantic_policy.version,
        )
        for score in structured.scores
    )
    return SemanticScoringResult(
        structured=structured,
        scores=scores,
        status=status,
        degraded_reason=reason,
    )


def _opportunity_semantic_text(
    opportunity: CanonicalOpportunityRecord,
) -> str | None:
    analysis = opportunity.analysis
    values = (
        opportunity.canonical_title,
        opportunity.task_summary,
        analysis.category,
        analysis.role_title,
        *analysis.skills,
    )
    parts = tuple(
        dict.fromkeys(
            normalized
            for value in values
            if value is not None
            if (normalized := _normalize_semantic_text(value))
        )
    )
    return " | ".join(parts) or None


def _profile_semantic_text(profile: SearchProfileRecord) -> str:
    structured_terms = (
        term.value
        for terms in (profile.roles, profile.skills, profile.categories)
        for term in terms
    )
    return " | ".join(
        dict.fromkeys((profile.semantic_text_normalized, *structured_terms))
    )


def _feature_hash_vector(text: str, dimensions: int) -> tuple[float, ...]:
    tokens = tuple(
        token
        for token in lexical_token_sequence(text)
        if token not in _GENERIC_TOKENS
    )
    if not tokens:
        return tuple(0.0 for _ in range(dimensions))
    concepts = tuple(
        concept
        for concept in sorted(lexical_concepts(text))
        if concept not in _GENERIC_TOKENS
    )
    acronym = lexical_acronym(text)
    if acronym is not None and acronym not in concepts:
        concepts = (*concepts, acronym)
    features = [(f"word::{token}", 2.0) for token in tokens]
    features.extend((f"concept::{concept}", 1.0) for concept in concepts)
    features.extend(
        (f"bigram::{left}::{right}", 1.0)
        for left, right in zip(tokens, tokens[1:], strict=False)
    )
    features.extend(
        (f"char{size}::{token[index:index + size]}", 0.25)
        for token in tokens
        for size in (3, 4)
        if len(token) >= size
        for index in range(len(token) - size + 1)
    )
    vector = [0.0] * dimensions
    for feature, weight in features:
        digest = sha256(feature.encode("utf-8")).digest()
        index = int.from_bytes(digest[:4], "big") % dimensions
        sign = 1.0 if digest[4] & 1 else -1.0
        vector[index] += sign * weight
    norm = math.sqrt(sum(value * value for value in vector))
    if norm == 0:
        return tuple(0.0 for _ in range(dimensions))
    return tuple(value / norm for value in vector)


def _cosine_similarity(
    left: tuple[float, ...],
    right: tuple[float, ...],
) -> Decimal:
    if len(left) != len(right):
        raise ValueError("semantic representation dimensions do not match")
    left_norm = math.sqrt(sum(value * value for value in left))
    right_norm = math.sqrt(sum(value * value for value in right))
    if left_norm == 0 or right_norm == 0:
        return Decimal("0.0000")
    cosine = sum(a * b for a, b in zip(left, right, strict=True)) / (
        left_norm * right_norm
    )
    return _quantize(_clamp(Decimal(str(cosine))))


def _validate_provider_batch(
    opportunity: SemanticRepresentation,
    profiles: Mapping[UUID, SemanticRepresentation],
) -> None:
    expected = (
        opportunity.provider,
        opportunity.model,
        opportunity.model_version,
        opportunity.dimensions,
    )
    for representation in profiles.values():
        actual = (
            representation.provider,
            representation.model,
            representation.model_version,
            representation.dimensions,
        )
        if actual != expected:
            raise ValueError("semantic provider returned an inconsistent batch")


def _normalize_semantic_text(value: str) -> str:
    if not isinstance(value, str):
        raise TypeError("semantic embedding input must be a string")
    normalized = re.sub(
        r"\s+",
        " ",
        unicodedata.normalize("NFKC", value),
    ).strip()
    if not normalized:
        raise ValueError("semantic embedding input must not be blank")
    return normalized.casefold()


def _clamp(value: Decimal) -> Decimal:
    return min(Decimal("1"), max(Decimal("0"), value))


def _quantize(value: Decimal) -> Decimal:
    return value.quantize(_SCORE_QUANTUM)
