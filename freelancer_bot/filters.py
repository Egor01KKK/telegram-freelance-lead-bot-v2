from __future__ import annotations

import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable


DEFAULT_FILTERS_PATH = Path("config/filters.json")


@dataclass(frozen=True)
class FilterConfig:
    min_score: int
    keywords: dict[str, int]
    stop_words: tuple[str, ...]


def _positive_int(value: Any, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ValueError(f"Filter config: '{field}' must be a positive integer")
    return value


def load_filter_config(path: Path = DEFAULT_FILTERS_PATH) -> FilterConfig:
    """Load and validate keyword scoring rules from a JSON file."""
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError as exc:
        raise ValueError(f"Filters config was not found: {path}") from exc
    except json.JSONDecodeError as exc:
        raise ValueError(f"Filters config is not valid JSON: {path}: {exc.msg}") from exc
    except OSError as exc:
        raise ValueError(f"Could not read filters config {path}: {exc}") from exc

    if not isinstance(payload, dict):
        raise ValueError(f"Filters config must contain a JSON object: {path}")

    min_score = _positive_int(payload.get("min_score"), "min_score")
    raw_keywords = payload.get("keywords")
    if not isinstance(raw_keywords, dict) or not raw_keywords:
        raise ValueError("Filter config: 'keywords' must be a non-empty object")

    keywords: dict[str, int] = {}
    normalized_keywords: set[str] = set()
    for keyword, weight in raw_keywords.items():
        if not isinstance(keyword, str) or not keyword.strip():
            raise ValueError("Filter config: keyword names must be non-empty strings")
        normalized_keyword = normalize(keyword)
        if normalized_keyword in normalized_keywords:
            raise ValueError(f"Filter config: duplicate normalized keyword '{keyword}'")
        normalized_keywords.add(normalized_keyword)
        keywords[keyword.strip()] = _positive_int(weight, f"keyword '{keyword}'")

    raw_stop_words = payload.get("stop_words")
    if not isinstance(raw_stop_words, list):
        raise ValueError("Filter config: 'stop_words' must be an array")

    stop_words: list[str] = []
    normalized_stop_words: set[str] = set()
    for word in raw_stop_words:
        if not isinstance(word, str) or not word.strip():
            raise ValueError("Filter config: stop words must be non-empty strings")
        normalized_word = normalize(word)
        if normalized_word in normalized_stop_words:
            raise ValueError(f"Filter config: duplicate normalized stop word '{word}'")
        normalized_stop_words.add(normalized_word)
        stop_words.append(word.strip())

    return FilterConfig(min_score=min_score, keywords=keywords, stop_words=tuple(stop_words))


@dataclass(frozen=True)
class MatchResult:
    accepted: bool
    score: int
    matched_keywords: tuple[str, ...]
    rejected_by: tuple[str, ...]


def normalize(text: str) -> str:
    lowered = text.lower().replace("ё", "е")
    return re.sub(r"\s+", " ", lowered).strip()


def find_terms(text: str, terms: Iterable[str]) -> tuple[str, ...]:
    normalized = normalize(text)
    matches: list[str] = []
    for term in terms:
        term_normalized = normalize(term)
        if term_normalized and term_normalized in normalized:
            matches.append(term)
    return tuple(matches)


def match_text(text: str, config: FilterConfig | None = None) -> MatchResult:
    active_config = config or load_filter_config()
    rejected_by = find_terms(text, active_config.stop_words)
    if rejected_by:
        return MatchResult(False, 0, (), rejected_by)

    normalized = normalize(text)
    matched: list[str] = []
    score = 0
    for keyword, weight in active_config.keywords.items():
        if normalize(keyword) in normalized:
            matched.append(keyword)
            score += weight

    accepted = score >= active_config.min_score
    return MatchResult(accepted, score, tuple(matched), ())
