from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from enum import Enum
import re
from typing import TypeVar
import unicodedata


SEARCH_PROFILE_SCHEMA_VERSION = "search_profile.v1"
SEARCH_PROFILE_PARSER_VERSION = "search-profile-parser.v1"
SEARCH_PROFILE_PREFERENCES_SCHEMA_VERSION = "search_profile_preferences.v1"
PROFILE_TERM_LIMIT = 64
PROFILE_TERM_LENGTH_LIMIT = 200
SEMANTIC_TEXT_LENGTH_LIMIT = 10_000
EnumT = TypeVar("EnumT", bound=Enum)


class SearchProfileTermOrigin(str, Enum):
    EXPLICIT = "explicit"
    NORMALIZED_INFERENCE = "normalized_inference"


class OpportunityType(str, Enum):
    ONE_OFF_ORDER = "one_off_order"
    PROJECT = "project"
    VACANCY = "vacancy"
    PART_TIME_CONTRACTOR = "part_time_contractor"


class BudgetPolicy(str, Enum):
    ALLOW_UNKNOWN = "allow_unknown"
    REQUIRE_EXPLICIT = "require_explicit"


class WorkMode(str, Enum):
    REMOTE = "remote"
    HYBRID = "hybrid"
    ON_SITE = "on_site"


@dataclass(frozen=True)
class SearchProfileTermInput:
    value: str
    evidence: str
    origin: SearchProfileTermOrigin


@dataclass(frozen=True)
class SearchProfileTerm:
    value: str
    normalized_value: str
    origin: SearchProfileTermOrigin = SearchProfileTermOrigin.EXPLICIT
    evidence: str | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.value, str) or not isinstance(
            self.normalized_value,
            str,
        ):
            raise TypeError("search profile term values must be strings")
        if not self.value or self.value != _normalize_whitespace(self.value):
            raise ValueError("search profile term value must be normalized")
        if len(self.value) > PROFILE_TERM_LENGTH_LIMIT:
            raise ValueError("search profile term value exceeds the supported limit")
        if self.normalized_value != self.value.casefold():
            raise ValueError("search profile normalized term value is inconsistent")
        if not isinstance(self.origin, SearchProfileTermOrigin):
            raise TypeError("search profile term origin is invalid")
        if self.evidence is not None:
            if not isinstance(self.evidence, str) or not self.evidence.strip():
                raise ValueError("search profile term evidence must not be blank")
            if len(self.evidence) > SEMANTIC_TEXT_LENGTH_LIMIT:
                raise ValueError("search profile term evidence exceeds the supported limit")
        if (
            self.origin is SearchProfileTermOrigin.NORMALIZED_INFERENCE
            and self.evidence is None
        ):
            raise ValueError("normalized inferred terms require explicit source evidence")


@dataclass(frozen=True)
class SearchProfilePreferences:
    schema_version: str
    work_types: tuple[OpportunityType, ...] | None
    minimum_budget: Decimal | None
    currency: str | None
    budget_policy: BudgetPolicy | None
    languages: tuple[SearchProfileTerm, ...] | None
    geographies: tuple[SearchProfileTerm, ...] | None
    work_modes: tuple[WorkMode, ...] | None
    excluded_categories: tuple[SearchProfileTerm, ...] | None

    def __post_init__(self) -> None:
        if self.schema_version != SEARCH_PROFILE_PREFERENCES_SCHEMA_VERSION:
            raise ValueError("unsupported search profile preferences schema version")
        _validate_enum_tuple(self.work_types, OpportunityType, "work_types")
        _validate_enum_tuple(self.work_modes, WorkMode, "work_modes")
        if (self.minimum_budget is None) != (self.currency is None):
            raise ValueError("minimum budget and currency must be set together")
        if self.minimum_budget is not None:
            _validate_budget(self.minimum_budget)
            if self.currency is None or not _valid_currency(self.currency):
                raise ValueError("currency must be a three-letter uppercase code")
        if self.budget_policy is not None and not isinstance(
            self.budget_policy,
            BudgetPolicy,
        ):
            raise TypeError("budget_policy must be a BudgetPolicy")
        for field, terms in (
            ("languages", self.languages),
            ("geographies", self.geographies),
            ("excluded_categories", self.excluded_categories),
        ):
            if terms is None:
                continue
            if not isinstance(terms, tuple):
                raise TypeError(f"{field} must be a tuple or None")
            if len(terms) > PROFILE_TERM_LIMIT:
                raise ValueError(f"{field} exceeds the supported term limit")
            if not all(isinstance(term, SearchProfileTerm) for term in terms):
                raise TypeError(f"{field} must contain structured terms")
            if any(
                term.origin is not SearchProfileTermOrigin.EXPLICIT
                for term in terms
            ):
                raise ValueError(f"{field} preferences must be explicit")
            identities = [term.normalized_value for term in terms]
            if len(identities) != len(set(identities)):
                raise ValueError(f"{field} must contain unique terms")

    def accepts_work_type(self, opportunity_type: OpportunityType) -> bool:
        if not isinstance(opportunity_type, OpportunityType):
            raise TypeError("opportunity_type must be an OpportunityType")
        return (
            self.work_types is not None
            and opportunity_type in self.work_types
        )


@dataclass(frozen=True)
class ParsedSearchProfile:
    schema_version: str
    parser_version: str
    roles: tuple[SearchProfileTerm, ...]
    skills: tuple[SearchProfileTerm, ...]
    categories: tuple[SearchProfileTerm, ...]
    semantic_text_original: str
    semantic_text_normalized: str

    def __post_init__(self) -> None:
        if self.schema_version != SEARCH_PROFILE_SCHEMA_VERSION:
            raise ValueError("unsupported search profile schema version")
        if self.parser_version != SEARCH_PROFILE_PARSER_VERSION:
            raise ValueError("unsupported search profile parser version")
        for field, terms in (
            ("roles", self.roles),
            ("skills", self.skills),
            ("categories", self.categories),
        ):
            if not isinstance(terms, tuple):
                raise TypeError(f"{field} must be a tuple of structured terms")
            if len(terms) > PROFILE_TERM_LIMIT:
                raise ValueError(f"{field} exceeds the supported term limit")
            if not all(isinstance(term, SearchProfileTerm) for term in terms):
                raise TypeError(f"{field} must contain structured search profile terms")
            normalized_values = [term.normalized_value for term in terms]
            if len(normalized_values) != len(set(normalized_values)):
                raise ValueError(f"{field} must contain unique normalized terms")
        if not isinstance(self.semantic_text_original, str):
            raise TypeError("semantic_text_original must be a string")
        if not self.semantic_text_original.strip():
            raise ValueError("semantic_text_original must not be blank")
        if len(self.semantic_text_original) > SEMANTIC_TEXT_LENGTH_LIMIT:
            raise ValueError("semantic_text_original exceeds the supported limit")
        expected_normalized = _normalize_whitespace(self.semantic_text_original)
        if self.semantic_text_normalized != expected_normalized:
            raise ValueError("semantic_text_normalized is inconsistent with original text")


def parse_search_profile(
    *,
    roles: Iterable[str | SearchProfileTermInput],
    skills: Iterable[str | SearchProfileTermInput],
    categories: Iterable[str | SearchProfileTermInput],
    semantic_text: str,
) -> ParsedSearchProfile:
    if not isinstance(semantic_text, str):
        raise TypeError("semantic_text must be a string")
    if not semantic_text.strip():
        raise ValueError("semantic_text must not be blank")
    if len(semantic_text) > SEMANTIC_TEXT_LENGTH_LIMIT:
        raise ValueError(
            f"semantic_text must be at most {SEMANTIC_TEXT_LENGTH_LIMIT} characters"
        )
    normalized_semantic_text = _normalize_whitespace(semantic_text)
    if len(normalized_semantic_text) > SEMANTIC_TEXT_LENGTH_LIMIT:
        raise ValueError(
            "normalized semantic_text exceeds the supported character limit"
        )
    return ParsedSearchProfile(
        schema_version=SEARCH_PROFILE_SCHEMA_VERSION,
        parser_version=SEARCH_PROFILE_PARSER_VERSION,
        roles=_parse_terms(roles, "roles"),
        skills=_parse_terms(skills, "skills"),
        categories=_parse_terms(categories, "categories"),
        semantic_text_original=semantic_text,
        semantic_text_normalized=normalized_semantic_text,
    )


def empty_search_profile_preferences() -> SearchProfilePreferences:
    return SearchProfilePreferences(
        schema_version=SEARCH_PROFILE_PREFERENCES_SCHEMA_VERSION,
        work_types=None,
        minimum_budget=None,
        currency=None,
        budget_policy=None,
        languages=None,
        geographies=None,
        work_modes=None,
        excluded_categories=None,
    )


def parse_search_profile_preferences(
    *,
    work_types: Iterable[str | OpportunityType] | None = None,
    minimum_budget: Decimal | str | int | None = None,
    currency: str | None = None,
    budget_policy: str | BudgetPolicy | None = None,
    languages: Iterable[str] | None = None,
    geographies: Iterable[str] | None = None,
    work_modes: Iterable[str | WorkMode] | None = None,
    excluded_categories: Iterable[str] | None = None,
) -> SearchProfilePreferences:
    parsed_budget = _parse_budget(minimum_budget)
    parsed_currency = _parse_currency(currency)
    if (parsed_budget is None) != (parsed_currency is None):
        raise ValueError("minimum_budget and currency must be set together")
    return SearchProfilePreferences(
        schema_version=SEARCH_PROFILE_PREFERENCES_SCHEMA_VERSION,
        work_types=_parse_enum_values(
            work_types,
            OpportunityType,
            "work_types",
        ),
        minimum_budget=parsed_budget,
        currency=parsed_currency,
        budget_policy=(
            None
            if budget_policy is None
            else _parse_enum_value(budget_policy, BudgetPolicy, "budget_policy")
        ),
        languages=_parse_optional_explicit_terms(languages, "languages"),
        geographies=_parse_optional_explicit_terms(
            geographies,
            "geographies",
        ),
        work_modes=_parse_enum_values(work_modes, WorkMode, "work_modes"),
        excluded_categories=_parse_optional_explicit_terms(
            excluded_categories,
            "excluded_categories",
        ),
    )


def _parse_terms(
    values: Iterable[str | SearchProfileTermInput],
    field: str,
) -> tuple[SearchProfileTerm, ...]:
    if isinstance(values, (str, bytes)):
        raise TypeError(f"{field} must be a collection of strings")
    materialized = tuple(values)
    if len(materialized) > PROFILE_TERM_LIMIT:
        raise ValueError(f"{field} supports at most {PROFILE_TERM_LIMIT} values")

    parsed: list[SearchProfileTerm] = []
    seen: set[str] = set()
    for raw_term in materialized:
        if isinstance(raw_term, str):
            raw_value = raw_term
            evidence = raw_term
            origin = SearchProfileTermOrigin.EXPLICIT
        elif isinstance(raw_term, SearchProfileTermInput):
            raw_value = raw_term.value
            evidence = raw_term.evidence
            origin = raw_term.origin
        else:
            raise TypeError(f"{field} values must be strings or structured terms")
        value = _normalize_whitespace(raw_value)
        if not value:
            raise ValueError(f"{field} values must not be blank")
        if len(value) > PROFILE_TERM_LENGTH_LIMIT:
            raise ValueError(
                f"{field} values must be at most {PROFILE_TERM_LENGTH_LIMIT} characters"
            )
        normalized_value = value.casefold()
        normalized_evidence = _normalize_whitespace(evidence)
        if not normalized_evidence:
            raise ValueError(f"{field} term evidence must not be blank")
        if (
            origin is SearchProfileTermOrigin.EXPLICIT
            and normalized_evidence.casefold() != normalized_value
        ):
            raise ValueError(f"{field} explicit values must match their evidence")
        if normalized_value in seen:
            continue
        seen.add(normalized_value)
        parsed.append(
            SearchProfileTerm(
                value=value,
                normalized_value=normalized_value,
                origin=origin,
                evidence=evidence,
            )
        )
    return tuple(parsed)


def _normalize_whitespace(value: str) -> str:
    normalized = unicodedata.normalize("NFKC", value)
    return re.sub(r"\s+", " ", normalized).strip()


def normalize_semantic_text(value: str) -> str:
    if not isinstance(value, str):
        raise TypeError("semantic text must be a string")
    normalized = _normalize_whitespace(value)
    if not normalized:
        raise ValueError("semantic text must not be blank")
    if len(value) > SEMANTIC_TEXT_LENGTH_LIMIT:
        raise ValueError("semantic text exceeds the supported limit")
    if len(normalized) > SEMANTIC_TEXT_LENGTH_LIMIT:
        raise ValueError("normalized semantic text exceeds the supported limit")
    return normalized


def _parse_optional_explicit_terms(
    values: Iterable[str] | None,
    field: str,
) -> tuple[SearchProfileTerm, ...] | None:
    return None if values is None else _parse_terms(values, field)


def _parse_enum_values(
    values: Iterable[str | EnumT] | None,
    enum_type: type[EnumT],
    field: str,
) -> tuple[EnumT, ...] | None:
    if values is None:
        return None
    if isinstance(values, (str, bytes)):
        raise TypeError(f"{field} must be a collection")
    parsed: list[EnumT] = []
    seen: set[EnumT] = set()
    for value in values:
        item = _parse_enum_value(value, enum_type, field)
        if item not in seen:
            parsed.append(item)
            seen.add(item)
    return tuple(parsed)


def _parse_enum_value(
    value: str | EnumT,
    enum_type: type[EnumT],
    field: str,
) -> EnumT:
    if isinstance(value, enum_type):
        return value
    if not isinstance(value, str):
        raise TypeError(f"{field} values must be strings")
    try:
        return enum_type(value.strip().lower())
    except ValueError:
        raise ValueError(f"unsupported {field} value: {value}") from None


def _validate_enum_tuple(
    values: tuple[EnumT, ...] | None,
    enum_type: type[EnumT],
    field: str,
) -> None:
    if values is None:
        return
    if not isinstance(values, tuple):
        raise TypeError(f"{field} must be a tuple or None")
    if not all(isinstance(value, enum_type) for value in values):
        raise TypeError(f"{field} contains an invalid value")
    if len(values) != len(set(values)):
        raise ValueError(f"{field} values must be unique")


def _parse_budget(value: Decimal | str | int | None) -> Decimal | None:
    if value is None:
        return None
    if isinstance(value, bool):
        raise TypeError("minimum_budget must be numeric")
    try:
        parsed = Decimal(str(value))
    except (InvalidOperation, ValueError):
        raise ValueError("minimum_budget must be a decimal number") from None
    _validate_budget(parsed)
    return parsed


def _validate_budget(value: Decimal) -> None:
    if not isinstance(value, Decimal):
        raise TypeError("minimum_budget must be a Decimal")
    if not value.is_finite() or value < 0:
        raise ValueError("minimum_budget must be finite and non-negative")
    if value.as_tuple().exponent < -2:
        raise ValueError("minimum_budget supports at most two decimal places")
    if value >= Decimal("10000000000000000"):
        raise ValueError("minimum_budget exceeds the supported limit")


def _parse_currency(value: str | None) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str):
        raise TypeError("currency must be a string")
    parsed = value.strip().upper()
    if not _valid_currency(parsed):
        raise ValueError("currency must be a three-letter ASCII code")
    return parsed


def _valid_currency(value: str) -> bool:
    return (
        len(value) == 3
        and value.isascii()
        and value.isalpha()
        and value == value.upper()
    )
