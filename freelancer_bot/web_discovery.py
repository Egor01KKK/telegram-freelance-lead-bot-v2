from __future__ import annotations

import asyncio
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from enum import Enum
from difflib import SequenceMatcher
import json
import random
import re
import time
from types import MappingProxyType
from typing import TYPE_CHECKING, Any, Awaitable, Callable, Protocol, runtime_checkable
import urllib.error
import urllib.parse
import urllib.request

from .discovery import DiscoveredSourceCandidate, DiscoveryRequest

if TYPE_CHECKING:
    from .persistence.database import Database


class CommunityCategory(str, Enum):
    PROFESSION = "profession"
    FOUNDER = "founder"
    BUSINESS = "business"
    CREATOR = "creator"
    TOOL = "tool"
    INDUSTRY = "industry"


class WebDiscoveryQueryKind(str, Enum):
    COMMUNITY = "community"
    BUYER_INTENT = "buyer_intent"


@dataclass(frozen=True)
class BuyerIntentSeed:
    phrase: str
    language: str

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "phrase",
            _bounded_text(self.phrase, "phrase", 120),
        )
        object.__setattr__(
            self,
            "language",
            _language(self.language),
        )


@dataclass(frozen=True)
class WebDiscoveryTopic:
    category: CommunityCategory | str
    phrase: str
    language: str
    angle: str = "direct"

    def __post_init__(self) -> None:
        object.__setattr__(self, "category", CommunityCategory(self.category))
        object.__setattr__(self, "phrase", _bounded_text(self.phrase, "phrase", 120))
        object.__setattr__(self, "language", _language(self.language))
        angle = _bounded_text(self.angle, "angle", 32).lower()
        if not re.fullmatch(r"[a-z][a-z0-9_-]*", angle):
            raise ValueError("angle must be a safe lowercase identifier")
        object.__setattr__(self, "angle", angle)


@dataclass(frozen=True)
class WebDiscoveryQuery:
    kind: WebDiscoveryQueryKind
    category: CommunityCategory
    language: str
    topic: str
    text: str
    buyer_intent_seeds: tuple[str, ...] = ()
    angle: str = "direct"


@dataclass(frozen=True)
class WebDiscoveryStrategy:
    topics: tuple[WebDiscoveryTopic, ...]
    buyer_intent_seeds: tuple[BuyerIntentSeed, ...]
    results_per_query: int = 10
    max_candidates: int = 100

    def __post_init__(self) -> None:
        topics = tuple(self.topics)
        seeds = tuple(self.buyer_intent_seeds)
        if not topics:
            raise ValueError("Web discovery strategy must contain at least one topic")
        if not seeds:
            raise ValueError("Web discovery strategy must contain buyer-intent seeds")
        if any(not isinstance(topic, WebDiscoveryTopic) for topic in topics):
            raise TypeError("topics must contain WebDiscoveryTopic values")
        if any(not isinstance(seed, BuyerIntentSeed) for seed in seeds):
            raise TypeError("buyer_intent_seeds must contain BuyerIntentSeed values")
        if not 1 <= self.results_per_query <= 50:
            raise ValueError("results_per_query must be between 1 and 50")
        if not 1 <= self.max_candidates <= 1000:
            raise ValueError("max_candidates must be between 1 and 1000")
        object.__setattr__(self, "topics", topics)
        object.__setattr__(self, "buyer_intent_seeds", seeds)

    @classmethod
    def default(cls) -> WebDiscoveryStrategy:
        return cls(
            topics=(
                WebDiscoveryTopic("profession", "Telegram developers", "en"),
                WebDiscoveryTopic("profession", "product designers", "en"),
                WebDiscoveryTopic("founder", "startup founders", "en"),
                WebDiscoveryTopic("business", "small business owners", "en"),
                WebDiscoveryTopic("creator", "content creators bloggers", "en"),
                WebDiscoveryTopic("tool", "Figma design", "en"),
                WebDiscoveryTopic("tool", "n8n Make automation", "en"),
                WebDiscoveryTopic("industry", "ecommerce store owners", "en"),
                WebDiscoveryTopic("profession", "разработчики Telegram", "ru"),
                WebDiscoveryTopic("founder", "основатели стартапов", "ru"),
                WebDiscoveryTopic("business", "владельцы бизнеса", "ru"),
                WebDiscoveryTopic("creator", "блогеры создатели контента", "ru"),
                WebDiscoveryTopic("industry", "интернет магазины ecommerce", "ru"),
            ),
            buyer_intent_seeds=(
                BuyerIntentSeed("looking for a specialist", "en"),
                BuyerIntentSeed("need a contractor", "en"),
                BuyerIntentSeed("can anyone recommend", "en"),
                BuyerIntentSeed("ищу специалиста", "ru"),
                BuyerIntentSeed("нужен подрядчик", "ru"),
                BuyerIntentSeed("посоветуйте", "ru"),
            ),
        )

    def build_queries(self, request: DiscoveryRequest) -> tuple[WebDiscoveryQuery, ...]:
        location = _optional_request_text(request.parameters, "location", 120)
        queries: list[WebDiscoveryQuery] = []
        seeds_by_language: dict[str, list[str]] = {}
        for seed in self.buyer_intent_seeds:
            seeds_by_language.setdefault(seed.language, []).append(seed.phrase)

        for topic in self.topics:
            suffix = f' "{location}"' if location else ""
            community_text = (
                f'site:t.me "{topic.phrase}" '
                f'(community OR chat OR group OR сообщество OR чат){suffix}'
            )
            queries.append(
                WebDiscoveryQuery(
                    kind=WebDiscoveryQueryKind.COMMUNITY,
                    category=topic.category,
                    language=topic.language,
                    topic=topic.phrase,
                    text=community_text,
                    angle=topic.angle,
                )
            )

            seeds = tuple(seeds_by_language.get(topic.language, ()))
            if not seeds:
                continue
            seed_expression = " OR ".join(f'"{phrase}"' for phrase in seeds)
            queries.append(
                WebDiscoveryQuery(
                    kind=WebDiscoveryQueryKind.BUYER_INTENT,
                    category=topic.category,
                    language=topic.language,
                    topic=topic.phrase,
                    text=(
                        f'site:t.me "{topic.phrase}" ({seed_expression}){suffix}'
                    ),
                    buyer_intent_seeds=seeds,
                    angle=topic.angle,
                )
            )
        return tuple(queries)


@dataclass(frozen=True)
class WebQueryCollapseResult:
    queries: tuple[WebDiscoveryQuery, ...]
    generated_count: int
    exact_duplicates: int
    near_duplicates: int
    generated_by_angle: Mapping[str, int]
    executable_by_angle: Mapping[str, int]


def collapse_near_duplicate_queries(
    queries: Sequence[WebDiscoveryQuery],
) -> WebQueryCollapseResult:
    """Collapse only materially redundant queries within one discovery family.

    Family, language, category and buyer seed context are part of the identity.
    In particular, buyer-habitat queries with different topics or buyer seeds are
    not collapsed merely because they share one generic token.
    """

    generated_by_angle: dict[str, int] = {}
    exact_keys: set[tuple[str, ...]] = set()
    representatives: dict[tuple[str, ...], list[WebDiscoveryQuery]] = {}
    executable: list[WebDiscoveryQuery] = []
    exact_duplicates = 0
    near_duplicates = 0
    for query in queries:
        generated_by_angle[query.angle] = generated_by_angle.get(query.angle, 0) + 1
        family = _query_family_key(query)
        exact_key = (*family, _normalized_query(query.text))
        if exact_key in exact_keys:
            exact_duplicates += 1
            continue
        exact_keys.add(exact_key)
        bucket = representatives.setdefault(family, [])
        if any(_near_duplicate_query(query, existing) for existing in bucket):
            near_duplicates += 1
            continue
        bucket.append(query)
        executable.append(query)
    executable_by_angle: dict[str, int] = {}
    for query in executable:
        executable_by_angle[query.angle] = executable_by_angle.get(query.angle, 0) + 1
    return WebQueryCollapseResult(
        queries=tuple(executable),
        generated_count=len(queries),
        exact_duplicates=exact_duplicates,
        near_duplicates=near_duplicates,
        generated_by_angle=generated_by_angle,
        executable_by_angle=executable_by_angle,
    )


def _query_family_key(query: WebDiscoveryQuery) -> tuple[str, ...]:
    return (
        query.angle,
        query.kind.value,
        query.category.value,
        query.language.casefold(),
        "|".join(sorted(seed.casefold() for seed in query.buyer_intent_seeds)),
    )


def _near_duplicate_query(
    left: WebDiscoveryQuery,
    right: WebDiscoveryQuery,
) -> bool:
    if _query_family_key(left) != _query_family_key(right):
        return False
    left_tokens = _query_tokens(left.topic)
    right_tokens = _query_tokens(right.topic)
    if not left_tokens or not right_tokens:
        return False
    union = left_tokens | right_tokens
    similarity = len(left_tokens & right_tokens) / len(union)
    if left.kind is WebDiscoveryQueryKind.BUYER_INTENT:
        # Buyer habitat context is deliberately stricter than direct queries.
        # One shared word is never sufficient to collapse two buyer contexts.
        if similarity < 0.88:
            return False
        if len(left_tokens ^ right_tokens) > 1:
            return False
    elif len(left_tokens & right_tokens) < 2:
        return False
    return SequenceMatcher(
        None,
        _normalized_query(left.text),
        _normalized_query(right.text),
    ).ratio() >= (0.94 if left.kind is WebDiscoveryQueryKind.BUYER_INTENT else 0.92)


def _query_tokens(value: str) -> set[str]:
    tokens: set[str] = set()
    for token in re.findall(r"[\w'-]+", value.casefold(), flags=re.UNICODE):
        if len(token) <= 1:
            continue
        if token.endswith("ies") and len(token) > 5:
            token = token[:-3] + "y"
        elif token.endswith("s") and len(token) > 4:
            token = token[:-1]
        tokens.add(token)
    return tokens


@dataclass(frozen=True)
class WebSearchResult:
    url: str
    title: str = ""
    snippet: str = ""
    engines: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        object.__setattr__(self, "url", _bounded_text(self.url, "url", 4096))
        object.__setattr__(self, "title", self.title.strip()[:500])
        object.__setattr__(self, "snippet", self.snippet.strip()[:2000])
        object.__setattr__(
            self,
            "engines",
            tuple(
                _safe_failure_class(value)
                for value in self.engines
                if isinstance(value, str) and value.strip()
            ),
        )


@runtime_checkable
class WebSearchBackend(Protocol):
    async def search(
        self,
        query: str,
        *,
        language: str,
        limit: int,
    ) -> Sequence[WebSearchResult]: ...


class WebSearchBackendError(RuntimeError):
    """A bounded, classifiable failure from a Web Search backend."""

    def __init__(
        self,
        message: str,
        *,
        failure_class: str = "backend_error",
        status_code: int | None = None,
        retry_after_seconds: float | None = None,
    ) -> None:
        super().__init__(message)
        self.failure_class = _safe_failure_class(failure_class)
        self.status_code = status_code
        self.retry_after_seconds = retry_after_seconds


class WebProviderState(str, Enum):
    READY = "READY"
    DEGRADED = "DEGRADED"
    BACKOFF = "BACKOFF"
    UNAVAILABLE = "UNAVAILABLE"


@dataclass(frozen=True)
class WebProviderHealth:
    state: WebProviderState = WebProviderState.READY
    successful_searches: int = 0
    http_403: int = 0
    http_429: int = 0
    captcha_or_suspension: int = 0
    consecutive_failures: int = 0
    last_failure_category: str | None = None
    last_failure_at: datetime | None = None
    backoff_until: datetime | None = None
    last_success_at: datetime | None = None

    def to_payload(self) -> dict[str, object]:
        return {
            "state": self.state.value,
            "successful_searches": self.successful_searches,
            "http_403": self.http_403,
            "http_429": self.http_429,
            "captcha_or_suspension": self.captcha_or_suspension,
            "consecutive_failures": self.consecutive_failures,
            "last_failure_category": self.last_failure_category or "",
            "last_failure_at": (
                "" if self.last_failure_at is None else self.last_failure_at.isoformat()
            ),
            "backoff_until": (
                "" if self.backoff_until is None else self.backoff_until.isoformat()
            ),
            "last_success_at": (
                "" if self.last_success_at is None else self.last_success_at.isoformat()
            ),
        }


class WebDiscoveryGovernor:
    """Serialize, pace and durably govern Web requests per backend."""

    def __init__(
        self,
        *,
        min_delay_seconds: float = 5.0,
        max_delay_seconds: float = 10.0,
        max_concurrency: int = 1,
        base_backoff_seconds: float = 60.0,
        max_backoff_seconds: float = 3600.0,
        clock: Callable[[], datetime] | None = None,
        sleeper: Callable[[float], Awaitable[None]] | None = None,
        random_source: random.Random | None = None,
        database: Database | None = None,
        provider_name: str = "web_search",
    ) -> None:
        if min_delay_seconds < 0 or max_delay_seconds < min_delay_seconds:
            raise ValueError("Web governor delay range is invalid")
        if max_concurrency < 1:
            raise ValueError("Web governor max_concurrency must be positive")
        if base_backoff_seconds <= 0 or max_backoff_seconds < base_backoff_seconds:
            raise ValueError("Web governor backoff range is invalid")
        self._min_delay_seconds = min_delay_seconds
        self._max_delay_seconds = max_delay_seconds
        self._base_backoff_seconds = base_backoff_seconds
        self._max_backoff_seconds = max_backoff_seconds
        self._clock = clock or (lambda: datetime.now(timezone.utc))
        self._sleeper = sleeper or asyncio.sleep
        self._random = random_source or random.Random()
        self._database = database
        self._provider_name = _safe_failure_class(provider_name)
        self._semaphore = asyncio.Semaphore(max_concurrency)
        self._lock = asyncio.Lock()
        self._restore_lock = asyncio.Lock()
        self._restored = False
        self._last_request_at: datetime | None = None
        self._request_times: list[datetime] = []
        self._cache: dict[tuple[str, str, str, int], tuple[WebSearchResult, ...]] = {}
        self._reused_queries = 0
        self._health_by_backend: dict[str, WebProviderHealth] = {}

    @classmethod
    def from_config(
        cls,
        config: Any,
        *,
        database: Database | None = None,
        provider_name: str = "web_search",
    ) -> "WebDiscoveryGovernor":
        return cls(
            min_delay_seconds=float(
                getattr(config, "web_discovery_min_delay_seconds", 5.0)
            ),
            max_delay_seconds=float(
                getattr(config, "web_discovery_max_delay_seconds", 10.0)
            ),
            max_concurrency=int(
                getattr(config, "web_discovery_max_concurrency", 1)
            ),
            base_backoff_seconds=float(
                getattr(config, "web_discovery_base_backoff_seconds", 60.0)
            ),
            max_backoff_seconds=float(
                getattr(config, "web_discovery_max_backoff_seconds", 3600.0)
            ),
            database=database,
            provider_name=provider_name,
        )

    @property
    def health(self) -> WebProviderHealth:
        if len(self._health_by_backend) == 1:
            return next(iter(self._health_by_backend.values()))
        return self._aggregate_health()

    def health_for(self, backend: WebSearchBackend | str) -> WebProviderHealth:
        backend_id = (
            backend if isinstance(backend, str) else _backend_identity(backend)
        )
        return self._health_by_backend.get(backend_id, WebProviderHealth())

    @property
    def reused_queries(self) -> int:
        return self._reused_queries

    @property
    def request_times(self) -> tuple[datetime, ...]:
        return tuple(self._request_times)

    async def restore(self) -> None:
        """Load persisted cooldowns before the first provider request."""

        if self._restored:
            return
        async with self._restore_lock:
            if self._restored:
                return
            if self._database is not None:
                from .persistence.web_provider_health import WebProviderHealthRepository

                now = _aware_now(self._clock())
                async with self._database.connect() as connection:
                    records = await WebProviderHealthRepository().list_for_provider(
                        connection,
                        provider=self._provider_name,
                    )
                for record in records:
                    try:
                        state = WebProviderState(record.state)
                    except ValueError:
                        state = WebProviderState.DEGRADED
                    if (
                        state is WebProviderState.BACKOFF
                        and record.backoff_until is not None
                        and record.backoff_until <= now
                    ):
                        state = WebProviderState.DEGRADED
                    self._health_by_backend[record.backend] = WebProviderHealth(
                        state=state,
                        successful_searches=record.successful_searches,
                        http_403=record.http_403,
                        http_429=record.http_429,
                        captcha_or_suspension=record.captcha_or_suspension,
                        consecutive_failures=record.consecutive_failures,
                        last_failure_category=record.last_failure_category,
                        last_failure_at=record.last_failure_at,
                        backoff_until=record.backoff_until
                        if state is WebProviderState.BACKOFF
                        else None,
                        last_success_at=record.last_success_at,
                    )
            self._restored = True

    async def search(
        self,
        backend: WebSearchBackend,
        query: str,
        *,
        language: str,
        limit: int,
    ) -> Sequence[WebSearchResult]:
        await self.restore()
        backend_id = _backend_identity(backend)
        cache_key = (
            backend_id,
            _normalized_query(query),
            language.casefold(),
            limit,
        )
        cached = self._cache.get(cache_key)
        if cached is not None:
            self._reused_queries += 1
            return cached

        async with self._semaphore:
            async with self._lock:
                now = _aware_now(self._clock())
                health = self._health_by_backend.get(backend_id, WebProviderHealth())
                if health.state is WebProviderState.UNAVAILABLE:
                    raise WebSearchBackendError(
                        f"Web search backend {backend_id} is unavailable",
                        failure_class="provider_unavailable",
                    )
                if (
                    health.backoff_until is not None
                    and now < health.backoff_until
                ):
                    raise WebSearchBackendError(
                        f"Web search backend {backend_id} is in backoff",
                        failure_class="provider_backoff",
                    )
                if health.backoff_until is not None and now >= health.backoff_until:
                    health = _replace_health(
                        health,
                        state=WebProviderState.DEGRADED,
                        backoff_until=None,
                    )
                    self._health_by_backend[backend_id] = health
                if self._last_request_at is not None:
                    target_gap = self._random.uniform(
                        self._min_delay_seconds,
                        self._max_delay_seconds,
                    )
                    elapsed = (now - self._last_request_at).total_seconds()
                    if elapsed < target_gap:
                        await self._sleeper(target_gap - elapsed)
                        now = _aware_now(self._clock())
                self._last_request_at = now
                self._request_times.append(now)

            try:
                results = tuple(
                    await backend.search(query, language=language, limit=limit)
                )
            except WebSearchBackendError as exc:
                health = self._record_failure(backend_id, exc)
                await self._persist_health(backend_id, health)
                raise
            except Exception as exc:
                error = WebSearchBackendError(
                    "Web search backend failed",
                    failure_class="backend_error",
                )
                health = self._record_failure(backend_id, error)
                await self._persist_health(backend_id, health)
                raise error from exc

            health = self._record_success(backend_id)
            await self._persist_health(backend_id, health)
            self._cache[cache_key] = results
            return results

    def observability(self) -> dict[str, object]:
        gaps = [
            (right - left).total_seconds()
            for left, right in zip(self._request_times, self._request_times[1:])
        ]
        backend_health = {
            backend: health.to_payload()
            for backend, health in sorted(self._health_by_backend.items())
        }
        payload: dict[str, object] = {
            **self.health.to_payload(),
            "backend_health": backend_health,
            "queries_reused": self._reused_queries,
            "request_gap_min_ms": int(min(gaps) * 1000) if gaps else 0,
            "request_gap_max_ms": int(max(gaps) * 1000) if gaps else 0,
            "request_gap_avg_ms": int(sum(gaps) / len(gaps) * 1000) if gaps else 0,
        }
        return payload

    def _record_success(self, backend_id: str) -> WebProviderHealth:
        health = self._health_by_backend.get(backend_id, WebProviderHealth())
        updated = _replace_health(
            health,
            state=WebProviderState.READY,
            successful_searches=health.successful_searches + 1,
            consecutive_failures=0,
            backoff_until=None,
            last_success_at=_aware_now(self._clock()),
        )
        self._health_by_backend[backend_id] = updated
        return updated

    def _record_failure(
        self,
        backend_id: str,
        error: WebSearchBackendError,
    ) -> WebProviderHealth:
        health = self._health_by_backend.get(backend_id, WebProviderHealth())
        consecutive = health.consecutive_failures + 1
        if error.failure_class == "http_403":
            http_403 = health.http_403 + 1
        else:
            http_403 = health.http_403
        if error.failure_class == "http_429":
            http_429 = health.http_429 + 1
        else:
            http_429 = health.http_429
        captcha = health.captcha_or_suspension
        if error.failure_class in {"captcha", "suspension"}:
            captcha += 1
        backoff_seconds = min(
            self._max_backoff_seconds,
            error.retry_after_seconds
            if error.retry_after_seconds is not None
            else self._base_backoff_seconds * (2 ** (consecutive - 1)),
        )
        now = _aware_now(self._clock())
        state = (
            WebProviderState.UNAVAILABLE
            if consecutive >= 3
            else WebProviderState.BACKOFF
        )
        updated = _replace_health(
            health,
            state=state,
            http_403=http_403,
            http_429=http_429,
            captcha_or_suspension=captcha,
            consecutive_failures=consecutive,
            last_failure_category=error.failure_class,
            last_failure_at=now,
            backoff_until=now.replace(microsecond=0) + timedelta(seconds=backoff_seconds),
        )
        self._health_by_backend[backend_id] = updated
        return updated

    async def _persist_health(
        self,
        backend_id: str,
        health: WebProviderHealth,
    ) -> None:
        if self._database is None:
            return
        from .persistence.web_provider_health import WebProviderHealthRepository

        async with self._database.transaction() as connection:
            await WebProviderHealthRepository().upsert(
                connection,
                provider=self._provider_name,
                backend=backend_id,
                state=health.state.value,
                successful_searches=health.successful_searches,
                http_403=health.http_403,
                http_429=health.http_429,
                captcha_or_suspension=health.captcha_or_suspension,
                consecutive_failures=health.consecutive_failures,
                last_failure_category=health.last_failure_category,
                last_failure_at=health.last_failure_at,
                backoff_until=health.backoff_until,
                last_success_at=health.last_success_at,
            )

    def _aggregate_health(self) -> WebProviderHealth:
        if not self._health_by_backend:
            return WebProviderHealth()
        values = tuple(self._health_by_backend.values())
        if all(item.state is WebProviderState.UNAVAILABLE for item in values):
            state = WebProviderState.UNAVAILABLE
        elif any(
            item.state in {WebProviderState.BACKOFF, WebProviderState.UNAVAILABLE}
            for item in values
        ):
            state = WebProviderState.DEGRADED
        elif any(item.state is WebProviderState.DEGRADED for item in values):
            state = WebProviderState.DEGRADED
        else:
            state = WebProviderState.READY
        return WebProviderHealth(
            state=state,
            successful_searches=sum(item.successful_searches for item in values),
            http_403=sum(item.http_403 for item in values),
            http_429=sum(item.http_429 for item in values),
            captcha_or_suspension=sum(item.captcha_or_suspension for item in values),
            consecutive_failures=max(item.consecutive_failures for item in values),
            last_failure_category=next(
                (
                    item.last_failure_category
                    for item in reversed(values)
                    if item.last_failure_category
                ),
                None,
            ),
            last_failure_at=max(
                (item.last_failure_at for item in values if item.last_failure_at),
                default=None,
            ),
            backoff_until=max(
                (item.backoff_until for item in values if item.backoff_until),
                default=None,
            ),
            last_success_at=max(
                (item.last_success_at for item in values if item.last_success_at),
                default=None,
            ),
        )


def _replace_health(health: WebProviderHealth, **changes: Any) -> WebProviderHealth:
    return WebProviderHealth(
        state=changes.get("state", health.state),
        successful_searches=changes.get(
            "successful_searches", health.successful_searches
        ),
        http_403=changes.get("http_403", health.http_403),
        http_429=changes.get("http_429", health.http_429),
        captcha_or_suspension=changes.get(
            "captcha_or_suspension", health.captcha_or_suspension
        ),
        consecutive_failures=changes.get(
            "consecutive_failures", health.consecutive_failures
        ),
        last_failure_category=changes.get(
            "last_failure_category", health.last_failure_category
        ),
        last_failure_at=changes.get("last_failure_at", health.last_failure_at),
        backoff_until=changes.get("backoff_until", health.backoff_until),
        last_success_at=changes.get("last_success_at", health.last_success_at),
    )


def _safe_failure_class(value: str) -> str:
    normalized = re.sub(r"[^a-z0-9_-]", "_", value.casefold()).strip("_")
    return normalized[:64] or "backend_error"


def _normalized_query(value: str) -> str:
    return re.sub(r"\s+", " ", value.casefold()).strip()


def _backend_identity(backend: WebSearchBackend) -> str:
    value = getattr(backend, "health_identity", None)
    if not isinstance(value, str) or not value.strip():
        value = backend.__class__.__name__
    return _safe_failure_class(value)


def _backend_health_payload(backend: WebSearchBackend) -> dict[str, object]:
    value = getattr(backend, "health_observability", None)
    if not isinstance(value, Mapping):
        return {}
    payload: dict[str, object] = {}
    for key, item in value.items():
        if not isinstance(key, str) or not re.fullmatch(r"[a-z][a-z0-9_]{0,63}", key):
            continue
        if isinstance(item, (str, int, float, bool)):
            payload[key] = item
        elif isinstance(item, (list, tuple)):
            safe_items = [
                _safe_failure_class(entry)
                for entry in item
                if isinstance(entry, str) and entry.strip()
            ]
            payload[key] = safe_items
    return payload


def _elapsed_ms(started: float) -> int:
    return max(0, int((time.perf_counter() - started) * 1000))


def _aware_now(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("Web governor clock must return a timezone-aware time")
    return value


class SearxngSearchBackend:
    health_identity = "searxng"

    def __init__(
        self,
        base_url: str,
        *,
        timeout_seconds: float = 15.0,
        max_response_bytes: int = 1_000_000,
        user_agent: str = "telegram-freelance-lead-bot/0.1",
    ) -> None:
        parsed = urllib.parse.urlsplit(base_url.strip())
        if parsed.scheme not in {"http", "https"} or not parsed.netloc:
            raise ValueError("SearXNG base_url must be an absolute HTTP(S) URL")
        if timeout_seconds <= 0:
            raise ValueError("timeout_seconds must be positive")
        if max_response_bytes < 1024:
            raise ValueError("max_response_bytes must be at least 1024")
        self._base_url = base_url.rstrip("/")
        self._timeout_seconds = timeout_seconds
        self._max_response_bytes = max_response_bytes
        self._user_agent = _bounded_text(user_agent, "user_agent", 255)
        self._last_unresponsive_engines: tuple[str, ...] = ()
        self._last_result_engines: tuple[str, ...] = ()

    @property
    def health_observability(self) -> dict[str, object]:
        return {
            "unresponsive_engines": self._last_unresponsive_engines,
            "result_engines": self._last_result_engines,
            "engine_level_control": False,
        }

    async def search(
        self,
        query: str,
        *,
        language: str,
        limit: int,
    ) -> Sequence[WebSearchResult]:
        query = _bounded_text(query, "query", 2000)
        language = _language(language)
        if not 1 <= limit <= 50:
            raise ValueError("limit must be between 1 and 50")
        return await asyncio.to_thread(
            self._search_sync,
            query,
            language,
            limit,
        )

    def _search_sync(
        self,
        query: str,
        language: str,
        limit: int,
    ) -> tuple[WebSearchResult, ...]:
        self._last_unresponsive_engines = ()
        self._last_result_engines = ()
        params = urllib.parse.urlencode(
            {
                "q": query,
                "format": "json",
                "language": language,
                "categories": "general",
            }
        )
        request = urllib.request.Request(
            f"{self._base_url}/search?{params}",
            headers={
                "Accept": "application/json",
                "User-Agent": self._user_agent,
            },
        )
        try:
            with urllib.request.urlopen(
                request,
                timeout=self._timeout_seconds,
            ) as response:
                body = response.read(self._max_response_bytes + 1)
        except urllib.error.HTTPError as exc:
            failure_class = {
                403: "http_403",
                429: "http_429",
            }.get(exc.code, "http_error")
            raise WebSearchBackendError(
                "SearXNG search request failed",
                failure_class=failure_class,
                status_code=exc.code,
                retry_after_seconds=_retry_after_seconds(exc.headers),
            ) from exc
        except urllib.error.URLError as exc:
            raise WebSearchBackendError(
                "SearXNG search request failed",
                failure_class="network_error",
            ) from exc
        except TimeoutError as exc:
            raise WebSearchBackendError(
                "SearXNG search request timed out",
                failure_class="timeout",
            ) from exc
        if len(body) > self._max_response_bytes:
            raise WebSearchBackendError(
                "SearXNG response exceeds the configured limit",
                failure_class="invalid_response",
            )
        try:
            payload = json.loads(body)
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise WebSearchBackendError(
                "SearXNG returned invalid JSON",
                failure_class="invalid_response",
            ) from exc
        if not isinstance(payload, Mapping) or not isinstance(payload.get("results"), list):
            raise WebSearchBackendError(
                "SearXNG response has no results list",
                failure_class="invalid_response",
            )

        unresponsive = payload.get("unresponsive_engines")
        self._last_unresponsive_engines = _engine_names(unresponsive)
        failure_class = _unresponsive_failure_class(unresponsive)
        if not payload["results"] and failure_class is not None:
            raise WebSearchBackendError(
                "SearXNG search engines are degraded",
                failure_class=failure_class,
            )

        results: list[WebSearchResult] = []
        result_engines: set[str] = set()
        for item in payload["results"]:
            if not isinstance(item, Mapping) or not isinstance(item.get("url"), str):
                continue
            engines = _engine_names(item.get("engines"))
            if isinstance(item.get("engine"), str):
                engines = tuple(dict.fromkeys((*engines, item["engine"])))
            result_engines.update(engines)
            results.append(
                WebSearchResult(
                    url=item["url"],
                    title=item.get("title") if isinstance(item.get("title"), str) else "",
                    snippet=(
                        item.get("content")
                        if isinstance(item.get("content"), str)
                        else ""
                    ),
                    engines=engines,
                )
            )
            if len(results) >= limit:
                break
        self._last_result_engines = tuple(sorted(result_engines))
        return tuple(results)


class WebDiscoveryProvider:
    name = "web_search"
    kind = "web"

    def __init__(
        self,
        backend: WebSearchBackend | Sequence[WebSearchBackend],
        *,
        strategy: WebDiscoveryStrategy | None = None,
        governor: WebDiscoveryGovernor | None = None,
        queries: Sequence[WebDiscoveryQuery] | None = None,
    ) -> None:
        if isinstance(backend, Sequence) and not isinstance(backend, (str, bytes)):
            backends = tuple(backend)
        else:
            backends = (backend,)
        if not backends or any(not isinstance(item, WebSearchBackend) for item in backends):
            raise TypeError("backend must implement WebSearchBackend")
        self._backends = backends
        self._strategy = strategy or WebDiscoveryStrategy.default()
        self._governor = governor
        self._queries = None if queries is None else tuple(queries)
        self._observability: dict[str, object] = {
            "queries_generated": 0,
            "queries_deduplicated": 0,
            "queries_near_deduplicated": 0,
            "queries_executable": 0,
            "query_angle_counts": {},
            "queries_executed": 0,
            "backend_attempts": 0,
            "queries_reused": 0,
            "search_results_considered": 0,
            "telegram_like_results": 0,
            "unique_candidates": 0,
            "backend_failures": 0,
            "backend_failure_classes": {},
            "query_attempts": [],
            "outcome": "NO_RESULTS",
            "provider_state": WebProviderState.READY.value,
        }

    @property
    def observability(self) -> Mapping[str, object]:
        return dict(self._observability)

    async def discover(
        self,
        request: DiscoveryRequest,
    ) -> Sequence[DiscoveredSourceCandidate]:
        accumulators: dict[str, _CandidateAccumulator] = {}
        generated_queries = (
            self._queries
            if self._queries is not None
            else self._strategy.build_queries(request)
        )
        collapsed = collapse_near_duplicate_queries(generated_queries)
        queries = collapsed.queries
        self._observability = {
            "queries_generated": collapsed.generated_count,
            "queries_deduplicated": (
                collapsed.exact_duplicates + collapsed.near_duplicates
            ),
            "queries_near_deduplicated": collapsed.near_duplicates,
            "queries_executable": len(queries),
            "query_angle_counts": {
                "generated": dict(collapsed.generated_by_angle),
                "executable": dict(collapsed.executable_by_angle),
            },
            "queries_executed": 0,
            "backend_attempts": 0,
            "queries_reused": 0,
            "search_results_considered": 0,
            "telegram_like_results": 0,
            "unique_candidates": 0,
            "backend_failures": 0,
            "backend_failure_classes": {},
            "query_attempts": [],
            "outcome": "NO_RESULTS",
            "provider_state": WebProviderState.READY.value,
        }
        reused_before = 0 if self._governor is None else self._governor.reused_queries
        for query in queries:
            self._observability["queries_executed"] += 1
            query_succeeded = False
            query_failure = False
            for backend in self._backends:
                backend_id = _backend_identity(backend)
                self._observability["backend_attempts"] += 1
                started = time.perf_counter()
                try:
                    if self._governor is None:
                        results = tuple(
                            await backend.search(
                                query.text,
                                language=query.language,
                                limit=self._strategy.results_per_query,
                            )
                        )
                    else:
                        results = tuple(
                            await self._governor.search(
                                backend,
                                query.text,
                                language=query.language,
                                limit=self._strategy.results_per_query,
                            )
                        )
                except WebSearchBackendError as exc:
                    query_failure = True
                    self._observability["backend_failures"] += 1
                    classes = self._observability["backend_failure_classes"]
                    if isinstance(classes, dict):
                        classes[exc.failure_class] = classes.get(exc.failure_class, 0) + 1
                    self._append_query_attempt(
                        query,
                        backend_id=backend_id,
                        success=False,
                        failure_class=exc.failure_class,
                        result_count=0,
                        elapsed_ms=_elapsed_ms(started),
                    )
                    continue

                query_succeeded = True
                self._append_query_attempt(
                    query,
                    backend_id=backend_id,
                    success=True,
                    failure_class=None,
                    result_count=len(results),
                    elapsed_ms=_elapsed_ms(started),
                )
                for rank, result in enumerate(results, start=1):
                    self._observability["search_results_considered"] += 1
                    if not isinstance(result, WebSearchResult):
                        raise TypeError(
                            "Web search backends must return WebSearchResult values"
                        )
                    identity = _telegram_identity(result.url)
                    if identity is None:
                        continue
                    self._observability["telegram_like_results"] += 1
                    handle, canonical_url = identity
                    match = {
                        "query": query.text,
                        "query_kind": query.kind.value,
                        "community_category": query.category.value,
                        "topic": query.topic,
                        "language": query.language,
                        "query_angle": query.angle,
                        "buyer_intent_seeds": list(query.buyer_intent_seeds),
                        "result_url": result.url,
                        "result_title": result.title,
                        "result_snippet": result.snippet,
                        "rank": rank,
                    }
                    existing = accumulators.get(handle)
                    if existing is None:
                        accumulators[handle] = _CandidateAccumulator(
                            handle=handle,
                            canonical_url=canonical_url,
                            display_name=_display_name(result.title, handle),
                            first_query=query.text,
                            matches=[match],
                        )
                    else:
                        existing.matches.append(match)
                    if len(accumulators) >= self._strategy.max_candidates:
                        break
                break

            if not query_succeeded and query_failure:
                # A failure in one backend may be recovered by a healthy
                # fallback. Only stop this run when every backend failed.
                break
            if len(accumulators) >= self._strategy.max_candidates:
                break

        self._observability["unique_candidates"] = len(accumulators)
        if self._governor is not None:
            governor_payload = self._governor.observability()
            self._observability["queries_reused"] = (
                self._governor.reused_queries - reused_before
            )
            self._observability["provider_state"] = governor_payload["state"]
            self._observability["provider_health"] = governor_payload
        self._observability["backend_health"] = {
            _backend_identity(backend): _backend_health_payload(backend)
            for backend in self._backends
        }
        if self._observability["backend_failures"]:
            self._observability["outcome"] = "SEARCH_BACKEND_DEGRADED"
        elif not accumulators:
            self._observability["outcome"] = "NO_RESULTS"
        else:
            self._observability["outcome"] = "RESULTS"
        return tuple(
            accumulator.to_candidate(request)
            for accumulator in accumulators.values()
        )

    def _append_query_attempt(
        self,
        query: WebDiscoveryQuery,
        *,
        backend_id: str,
        success: bool,
        failure_class: str | None,
        result_count: int,
        elapsed_ms: int,
    ) -> None:
        attempts = self._observability["query_attempts"]
        if not isinstance(attempts, list):
            return
        state = (
            self._governor.health_for(backend_id).state.value
            if self._governor is not None
            else "READY" if success else "DEGRADED"
        )
        attempts.append(
            {
                "angle": query.angle,
                "provider_backend": backend_id,
                "success": success,
                "failure_class": failure_class or "",
                "result_count": result_count,
                "elapsed_ms": elapsed_ms,
                "provider_state_after": state,
            }
        )


@dataclass
class _CandidateAccumulator:
    handle: str
    canonical_url: str
    display_name: str
    first_query: str
    matches: list[dict[str, Any]]

    def to_candidate(self, request: DiscoveryRequest) -> DiscoveredSourceCandidate:
        profile_discovery = request.parameters.get("profile_discovery")
        profile_intent_id = (
            profile_discovery.get("intent_id")
            if isinstance(profile_discovery, Mapping)
            and isinstance(profile_discovery.get("intent_id"), str)
            else None
        )
        context: dict[str, Any] = {
            "discovery_method": "web_search",
            "matches": list(self.matches),
        }
        if profile_intent_id is not None:
            context["profile_discovery_intent_id"] = profile_intent_id
        context = MappingProxyType(
            context
        )
        return DiscoveredSourceCandidate(
            result_key=f"web:telegram:{self.handle}",
            platform="telegram",
            external_id=f"username:{self.handle}",
            access_type="public",
            display_name=self.display_name,
            handle=f"@{self.handle}",
            canonical_url=self.canonical_url,
            discovered_at=request.requested_at,
            seed_reference=self.first_query,
            context=context,
        )


_TELEGRAM_HOSTS = {"t.me", "telegram.me", "telegram.dog"}
_RESERVED_PATHS = {
    "addlist",
    "addemoji",
    "addstickers",
    "addtheme",
    "boost",
    "confirmphone",
    "contact",
    "giftcode",
    "invoice",
    "iv",
    "joinchat",
    "login",
    "nft",
    "proxy",
    "resolve",
    "setlanguage",
    "share",
    "socks",
    "stars_topup",
}
_HANDLE_PATTERN = re.compile(r"^[A-Za-z][A-Za-z0-9_]{4,31}$")


def _retry_after_seconds(headers: Any) -> float | None:
    if headers is None:
        return None
    value = headers.get("Retry-After") if hasattr(headers, "get") else None
    if value is None:
        return None
    try:
        parsed = float(str(value).strip())
    except (TypeError, ValueError):
        return None
    return parsed if parsed >= 0 else None


def _engine_names(value: Any) -> tuple[str, ...]:
    values: list[str] = []
    if isinstance(value, Mapping):
        value = list(value)
    if not isinstance(value, (list, tuple, set)):
        return ()
    for item in value:
        if isinstance(item, str) and item.strip():
            values.append(_safe_failure_class(item))
        elif isinstance(item, (list, tuple)) and item:
            first = item[0]
            if isinstance(first, str) and first.strip():
                values.append(_safe_failure_class(first))
    return tuple(dict.fromkeys(values))


def _unresponsive_failure_class(value: Any) -> str | None:
    if not isinstance(value, (list, tuple, Mapping)):
        return None
    text = json.dumps(value, ensure_ascii=False).casefold()
    if any(marker in text for marker in ("captcha", "unusual traffic", "suspended")):
        return "captcha" if "captcha" in text or "unusual traffic" in text else "suspension"
    if "429" in text or "rate limit" in text or "too many requests" in text:
        return "http_429"
    if "403" in text or "forbidden" in text:
        return "http_403"
    return None


def _telegram_identity(url: str) -> tuple[str, str] | None:
    try:
        parsed = urllib.parse.urlsplit(url)
    except ValueError:
        return None
    host = (parsed.hostname or "").lower()
    if host.startswith("www."):
        host = host[4:]
    if parsed.scheme not in {"http", "https"} or host not in _TELEGRAM_HOSTS:
        return None
    parts = [urllib.parse.unquote(part) for part in parsed.path.split("/") if part]
    if parts and parts[0].lower() == "s":
        parts = parts[1:]
    if not parts:
        return None
    handle = parts[0]
    if (
        handle.startswith("+")
        or handle.lower() in _RESERVED_PATHS
        or not _HANDLE_PATTERN.fullmatch(handle)
    ):
        return None
    normalized = handle.lower()
    return normalized, f"https://t.me/{normalized}"


def _display_name(title: str, handle: str) -> str:
    normalized = re.sub(r"^Telegram:\s*", "", title, flags=re.IGNORECASE)
    normalized = re.sub(r"\s*[-|]\s*Telegram\s*$", "", normalized, flags=re.IGNORECASE)
    normalized = normalized.strip()
    return normalized or f"@{handle}"


def _optional_request_text(
    parameters: Mapping[str, Any],
    key: str,
    max_length: int,
) -> str | None:
    value = parameters.get(key)
    if value is None:
        return None
    if not isinstance(value, str):
        raise ValueError(f"Discovery request parameter {key} must be a string")
    return _bounded_text(value, key, max_length)


def _language(value: str) -> str:
    normalized = _bounded_text(value, "language", 16).lower()
    if not re.fullmatch(r"[a-z]{2,3}(?:-[a-z]{2})?", normalized):
        raise ValueError("language must be a language code")
    return normalized


def _bounded_text(value: str, field: str, max_length: int) -> str:
    normalized = value.strip()
    if not normalized:
        raise ValueError(f"{field} must not be blank")
    if len(normalized) > max_length:
        raise ValueError(f"{field} must not exceed {max_length} characters")
    return normalized
