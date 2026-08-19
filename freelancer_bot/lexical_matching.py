from __future__ import annotations

from collections.abc import Iterable, Sequence
from decimal import Decimal
import re
import unicodedata


LEXICAL_NORMALIZATION_VERSION = "lexical-concepts.v3"
_SCORE_QUANTUM = Decimal("0.0001")
_MINIMUM_TARGET_LABEL_SIMILARITY = Decimal("0.6000")
_GENERIC_TARGET_CONCEPTS = frozenset(
    {
        "develop",
        "developer",
        "job",
        "project",
        "работа",
        "разработчик",
        "specialist",
        "специалист",
        "work",
    }
)

# Separators such as hyphens, slashes, underscores and parentheses divide
# concepts.  The optional suffix keeps common technical labels such as C++
# and C# as one token without making a compound label indivisible.
_TOKEN_PATTERN = re.compile(r"[^\W_]+(?:[+#]+)?", re.UNICODE)
_CYRILLIC_TO_LATIN = {
    "а": "a",
    "б": "b",
    "в": "v",
    "г": "g",
    "д": "d",
    "е": "e",
    "ё": "yo",
    "ж": "zh",
    "з": "z",
    "и": "i",
    "й": "y",
    "к": "k",
    "л": "l",
    "м": "m",
    "н": "n",
    "о": "o",
    "п": "p",
    "р": "r",
    "с": "s",
    "т": "t",
    "у": "u",
    "ф": "f",
    "х": "kh",
    "ц": "ts",
    "ч": "ch",
    "ш": "sh",
    "щ": "shch",
    "ъ": "",
    "ы": "y",
    "ь": "",
    "э": "e",
    "ю": "yu",
    "я": "ya",
}
_SUFFIX_RULES: tuple[tuple[str, str], ...] = (
    ("ization", "ize"),
    ("ational", "ate"),
    ("tional", "tion"),
    ("fulness", "ful"),
    ("ousness", "ous"),
    ("iveness", "ive"),
    ("ment", ""),
    ("ness", ""),
    ("ation", "ate"),
    ("tion", ""),
    ("ing", ""),
    ("ed", ""),
    ("er", ""),
    ("or", ""),
    ("ity", ""),
    ("al", ""),
)


def normalize_lexical_text(value: str) -> str:
    if not isinstance(value, str):
        raise TypeError("lexical matching input must be a string")
    normalized = unicodedata.normalize("NFKC", value).casefold()
    return re.sub(r"\s+", " ", normalized).strip()


def lexical_token_sequence(value: str) -> tuple[str, ...]:
    """Return deterministic concept-bearing tokens in source order."""

    normalized = normalize_lexical_text(value)
    return tuple(
        canonical
        for token in _TOKEN_PATTERN.findall(normalized)
        if _usable_token(token)
        if (canonical := _canonical_token(token)) is not None
    )


def lexical_concepts(value: str) -> frozenset[str]:
    """Return the normalized lexical concepts in a label."""

    concepts = set(lexical_token_sequence(value))
    for token in tuple(concepts):
        concepts.update(_token_variants(token))
    return frozenset(concepts)


def lexical_acronym(value: str) -> str | None:
    tokens = lexical_token_sequence(value)
    if len(tokens) < 2:
        return None
    acronym = "".join(token[0] for token in tokens if token)
    return acronym if len(acronym) >= 2 else None


def label_similarity(left: str, right: str) -> Decimal:
    if normalize_lexical_text(left) == normalize_lexical_text(right):
        return Decimal("1.0000")
    left_tokens = lexical_token_sequence(left)
    right_tokens = lexical_token_sequence(right)
    left_acronym = lexical_acronym(left)
    right_acronym = lexical_acronym(right)
    if (
        len(left_tokens) == 1
        and right_acronym == left_tokens[0]
    ) or (
        len(right_tokens) == 1
        and left_acronym == right_tokens[0]
    ):
        return Decimal("1.0000")
    matches = _greedy_concept_matches(
        left_tokens,
        right_tokens,
    )
    return _f1_from_match_count(
        len(matches),
        len(left_tokens),
        len(right_tokens),
    )


def label_matches(
    left: Iterable[str],
    right: Iterable[str],
) -> tuple[tuple[str, str, Decimal], ...]:
    """Greedily pair conceptually equivalent labels once, deterministically."""

    left_labels = tuple(dict.fromkeys(left))
    right_labels = tuple(dict.fromkeys(right))
    candidates: list[tuple[Decimal, str, str]] = []
    for left_label in left_labels:
        for right_label in right_labels:
            similarity = label_similarity(left_label, right_label)
            if similarity:
                candidates.append((similarity, left_label, right_label))
    candidates.sort(key=lambda item: (-item[0], item[1], item[2]))
    used_left: set[str] = set()
    used_right: set[str] = set()
    matches: list[tuple[str, str, Decimal]] = []
    for similarity, left_label, right_label in candidates:
        if left_label in used_left or right_label in used_right:
            continue
        used_left.add(left_label)
        used_right.add(right_label)
        matches.append((left_label, right_label, similarity))
    return tuple(matches)


def labels_have_overlap(left: Iterable[str], right: Iterable[str]) -> bool:
    left_labels = tuple(dict.fromkeys(left))
    right_labels = tuple(dict.fromkeys(right))
    matches = label_matches(left_labels, right_labels)
    if not matches:
        return False
    if any(
        similarity >= _MINIMUM_TARGET_LABEL_SIMILARITY
        and (
            left_concept not in _GENERIC_TARGET_CONCEPTS
            or right_concept not in _GENERIC_TARGET_CONCEPTS
        )
        for left_label, right_label, similarity in matches
        for left_concept in lexical_concepts(left_label)
        for right_concept in lexical_concepts(right_label)
        if _concept_equivalent(left_concept, right_concept)
    ):
        return True
    if any(
        similarity == Decimal("1.0000")
        and (
            any(concept not in _GENERIC_TARGET_CONCEPTS for concept in lexical_concepts(left_label))
            or any(concept not in _GENERIC_TARGET_CONCEPTS for concept in lexical_concepts(right_label))
        )
        for left_label, right_label, similarity in matches
    ):
        return True
    left_specific = {
        concept
        for label in left_labels
        for concept in lexical_concepts(label)
        if concept not in _GENERIC_TARGET_CONCEPTS
    }
    right_specific = {
        concept
        for label in right_labels
        for concept in lexical_concepts(label)
        if concept not in _GENERIC_TARGET_CONCEPTS
    }
    return not left_specific or not right_specific


def labels_f1(
    left: Sequence[str] | set[str] | frozenset[str],
    right: Sequence[str] | set[str] | frozenset[str],
) -> tuple[Decimal, tuple[tuple[str, str, Decimal], ...]]:
    matches = label_matches(left, right)
    score = _f1_from_match_count(len(matches), len(set(left)), len(set(right)))
    return score, matches


def _greedy_concept_matches(
    left: Sequence[str],
    right: Sequence[str],
) -> tuple[tuple[str, str], ...]:
    candidates: list[tuple[str, str]] = []
    for left_concept in left:
        for right_concept in right:
            if _concept_equivalent(left_concept, right_concept):
                candidates.append((left_concept, right_concept))
    candidates.sort()
    used_left: set[str] = set()
    used_right: set[str] = set()
    matches: list[tuple[str, str]] = []
    for left_concept, right_concept in candidates:
        if left_concept in used_left or right_concept in used_right:
            continue
        used_left.add(left_concept)
        used_right.add(right_concept)
        matches.append((left_concept, right_concept))
    return tuple(matches)


def _f1_from_match_count(
    match_count: int,
    left_count: int,
    right_count: int,
) -> Decimal:
    if not match_count or not left_count or not right_count:
        return Decimal("0.0000")
    precision = Decimal(match_count) / left_count
    recall = Decimal(match_count) / right_count
    return (
        Decimal("2") * precision * recall / (precision + recall)
    ).quantize(_SCORE_QUANTUM)


def _concept_equivalent(left: str, right: str) -> bool:
    if any(
        _surface_concept_equivalent(left_variant, right_variant)
        for left_variant in _token_variants(left)
        for right_variant in _token_variants(right)
    ):
        return True
    return False


def _surface_concept_equivalent(left: str, right: str) -> bool:
    if left == right:
        return True
    if _plural_variant(left, right):
        return True
    shorter, longer = sorted((left, right), key=len)
    if len(shorter) < 5 or len(longer) - len(shorter) > 3:
        return False
    return longer.startswith(shorter) and len(shorter) / len(longer) >= 0.75


def _plural_variant(left: str, right: str) -> bool:
    shorter, longer = sorted((left, right), key=len)
    if len(shorter) < 3:
        return False
    return longer in {f"{shorter}s", f"{shorter}es", f"{shorter}ies"}


def _canonical_token(token: str) -> str | None:
    if not _usable_token(token):
        return None
    if not token.isalpha():
        return token
    for suffix, replacement in _SUFFIX_RULES:
        if not token.endswith(suffix):
            continue
        stem_length = len(token) - len(suffix)
        if stem_length < 3:
            continue
        stem = token[:stem_length] + replacement
        if stem.endswith(("nn", "mm", "tt", "pp")):
            stem = stem[:-1]
        return stem
    return token


def _token_variants(token: str) -> frozenset[str]:
    variants = {token}
    transliterated = "".join(
        _CYRILLIC_TO_LATIN.get(character, character)
        for character in token
    )
    if transliterated != token:
        variants.add(_transliterated_root(transliterated))
    return frozenset(variants)


def _transliterated_root(value: str) -> str:
    if len(value) >= 4 and value.endswith("ies"):
        return f"{value[:-3]}y"
    if len(value) >= 4 and value.endswith("y"):
        return value[:-1]
    return value


def _usable_token(token: str) -> bool:
    alphanumeric_length = sum(character.isalnum() for character in token)
    return alphanumeric_length >= 2 or any(
        character in token for character in "+#"
    )
