"""Profile-driven Telegram global source discovery.

The provider searches Telegram's global message index with a small, versioned
query plan derived from one SearchProfile.  It only uses the message's own
resolved chat as the source candidate; forwarded entities, mentions and raw
message bodies never become candidate evidence.
"""

from __future__ import annotations

import asyncio
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timezone
import logging
from time import monotonic
from typing import Any
from uuid import UUID

from telethon import functions
from telethon import utils as telethon_utils
from telethon.tl import types as telethon_types

from .discovery import DiscoveredSourceCandidate, DiscoveryProvider, DiscoveryRequest
from .config import RuntimeConfig
from .observability import log_event
from .persistence.database import Database
from .persistence.jobs import DurableJobRepository, JobClaim
from .persistence.search_profiles import SearchProfileRepository
from .telegram_request_governor import TelegramRequestCategory, TelegramRequestGovernor
from .worker import DurableWorker, WorkerOptions


TELEGRAM_PROFILE_DISCOVERY_STRATEGY_VERSION = "telegram-profile-discovery.v2"
TELEGRAM_PROFILE_DISCOVERY_PROVIDER = "telegram_global_profile"
TELEGRAM_PROFILE_DISCOVERY_KIND = "telegram_global_search"
TELEGRAM_PROFILE_DISCOVERY_JOB_TYPE = "profile.telegram_discovery.v1"
TELEGRAM_PROFILE_DISCOVERY_JOB_KEY_PREFIX = "profile-telegram-discovery"
DEFAULT_MAX_QUERIES = 8
MAX_QUERY_COUNT = 24
DEFAULT_RESULTS_PER_QUERY = 20
MAX_RESULTS_PER_QUERY = 50
DEFAULT_MAX_TOTAL_HITS = 600
MAX_HIT_TEXT_LENGTH = 12000


class TelegramGlobalSearchPageCache:
    """Deduplicate identical global-search pages within one runtime cycle.

    The cache is deliberately caller-owned and short-lived.  It never crosses
    collector sessions or discovery cycles, and failed requests are not cached.
    Concurrent callers for the same page share one in-flight request.
    """

    def __init__(self, *, max_entries: int = 256) -> None:
        if max_entries < 1:
            raise ValueError("max_entries must be positive")
        self._max_entries = max_entries
        self._values: dict[tuple[Any, ...], Any] = {}
        self._in_flight: dict[tuple[Any, ...], asyncio.Task[Any]] = {}
        self._lock = asyncio.Lock()

    async def get_or_fetch(
        self,
        key: tuple[Any, ...],
        operation: Any,
    ) -> tuple[Any, bool]:
        async with self._lock:
            if key in self._values:
                return self._values[key], True
            task = self._in_flight.get(key)
            if task is None:
                task = asyncio.create_task(operation())
                self._in_flight[key] = task
                owner = True
            else:
                owner = False
        try:
            value = await asyncio.shield(task)
        except BaseException:
            if owner:
                async with self._lock:
                    if self._in_flight.get(key) is task:
                        self._in_flight.pop(key, None)
            raise
        async with self._lock:
            if owner:
                self._in_flight.pop(key, None)
                self._values[key] = value
                if len(self._values) > self._max_entries:
                    self._values.pop(next(iter(self._values)))
        return value, not owner


@dataclass(frozen=True)
class TelegramProfileSearchQueryMatch:
    """The safe query lineage attached to one global message hit."""

    text: str
    family: str
    language: str
    angle: str
    query_kind: str
    result_rank: int


@dataclass(frozen=True)
class TelegramGlobalSearchHit:
    """An in-memory global-search message preserved for evaluation/classification."""

    message_id: int
    source_identity: str
    source_kind: str
    source_access_type: str
    known_source: bool
    message_date: datetime
    observed_at: datetime
    text: str
    query_matches: tuple[TelegramProfileSearchQueryMatch, ...]


@dataclass(frozen=True)
class TelegramProfileSearchQuery:
    text: str
    language: str
    angle: str
    query_kind: str
    family: str = ""

    def __post_init__(self) -> None:
        if not self.text.strip():
            raise ValueError("Telegram profile search query must not be blank")
        if self.language not in {"ru", "en"}:
            raise ValueError("Telegram profile search query language must be ru or en")
        if not self.angle.strip() or not self.query_kind.strip():
            raise ValueError("Telegram profile search query metadata must not be blank")
        if not self.family.strip():
            object.__setattr__(self, "family", self.angle.upper())
        if not self.family.replace("_", "").isalnum():
            raise ValueError("Telegram profile search query family must be safe")


def build_telegram_profile_search_queries(
    intent: Any,
    *,
    max_queries: int = DEFAULT_MAX_QUERIES,
) -> tuple[TelegramProfileSearchQuery, ...]:
    """Build bounded buyer-language queries from a persisted profile intent."""

    if not 1 <= max_queries <= MAX_QUERY_COUNT:
        raise ValueError("max_queries must be between 1 and 24")
    languages = _language_order(getattr(intent, "languages", ()))
    technical_term = _technical_term(intent)
    templates = (
        (
            "DIRECT_ROLE",
            "direct",
            "buyer_need",
            (
                ("ru", f"ищу {technical_term} разработчика"),
                ("en", f"looking for {technical_term} developer"),
                ("ru", f"нужен {technical_term} разработчик"),
            ),
        ),
        (
            "DIRECT_SERVICE",
            "direct",
            "service_need",
            (
                ("ru", "нужен разработчик Telegram бота"),
                ("en", "Telegram bot developer needed"),
                ("en", "who can build a Telegram bot"),
            ),
        ),
        (
            "PROBLEM_TO_SOLVE",
            "buyer_habitat",
            "problem_need",
            (
                ("ru", "нужно автоматизировать заявки"),
                ("en", "need automation for business requests"),
                ("ru", "нужен парсер данных"),
            ),
        ),
        (
            "INTEGRATION",
            "buyer_habitat",
            "integration_need",
            (
                ("ru", "интеграция Telegram с CRM"),
                ("en", "Telegram CRM integration"),
                ("en", "connect Telegram to CRM"),
            ),
        ),
        (
            "RECOMMENDATION",
            "buyer_habitat",
            "recommendation_request",
            (
                ("ru", "посоветуйте разработчика Telegram"),
                ("en", "recommend a Telegram developer"),
                ("ru", "кто может сделать бота"),
            ),
        ),
        (
            "PROJECT_OUTSOURCE",
            "buyer_habitat",
            "project_need",
            (
                ("ru", "разработчик на проект Python"),
                ("en", "Telegram developer freelance"),
                ("en", "backend contractor Python"),
            ),
        ),
        (
            "VACANCY_PART_TIME",
            "buyer_habitat",
            "vacancy_need",
            (
                ("ru", "ищем Python разработчика удаленно"),
                ("en", "Telegram bot developer vacancy"),
                ("ru", "Python разработчик на part time"),
            ),
        ),
        (
            "MINI_APP_SPECIFIC_SERVICE",
            "direct",
            "specific_service_need",
            (
                ("ru", "разработчик Telegram mini app"),
                ("en", "Telegram web app developer"),
                ("ru", "разработчик Telegram web app"),
            ),
        ),
    )
    values: list[TelegramProfileSearchQuery] = []
    for family, angle, query_kind, variants in templates:
        for language, text in variants:
            if language not in languages:
                continue
            values.append(
                TelegramProfileSearchQuery(
                    text=text,
                    language=language,
                    angle=angle,
                    query_kind=query_kind,
                    family=family,
                )
            )
            if len(values) >= max_queries:
                return tuple(values)
    return tuple(values)


class TelegramGlobalSearchProvider(DiscoveryProvider):
    """Search Telegram globally while preserving the collector governor."""

    name = TELEGRAM_PROFILE_DISCOVERY_PROVIDER
    kind = TELEGRAM_PROFILE_DISCOVERY_KIND

    def __init__(
        self,
        client: Any,
        *,
        governor: TelegramRequestGovernor,
        intent: Any,
        queries: Sequence[TelegramProfileSearchQuery] | None = None,
        known_source_identities: Sequence[str] = (),
        max_results_per_query: int = DEFAULT_RESULTS_PER_QUERY,
        max_candidates: int = 10,
        max_total_hits: int = DEFAULT_MAX_TOTAL_HITS,
        page_size: int | None = None,
        page_cache: TelegramGlobalSearchPageCache | None = None,
    ) -> None:
        if not hasattr(client, "get_messages") and not callable(client):
            raise TypeError(
                "Telegram global discovery client must expose Telethon request access"
            )
        if not hasattr(governor, "run"):
            raise TypeError("Telegram global discovery requires a request governor")
        if not 1 <= max_results_per_query <= MAX_RESULTS_PER_QUERY:
            raise ValueError("max_results_per_query must be between 1 and 50")
        if not 1 <= max_candidates <= 100:
            raise ValueError("max_candidates must be between 1 and 100")
        if not 1 <= max_total_hits <= DEFAULT_MAX_TOTAL_HITS:
            raise ValueError("max_total_hits must be between 1 and 600")
        effective_page_size = max_results_per_query if page_size is None else page_size
        if not 1 <= effective_page_size <= max_results_per_query:
            raise ValueError("page_size must be between 1 and max_results_per_query")
        self._client = client
        self._governor = governor
        self._intent = intent
        self._queries = tuple(queries or build_telegram_profile_search_queries(intent))
        self._known = {_normalize_identity(value) for value in known_source_identities}
        self._max_results_per_query = max_results_per_query
        self._max_candidates = max_candidates
        self._max_total_hits = max_total_hits
        self._page_size = effective_page_size
        self._page_cache = page_cache
        self._search_hit_records: dict[tuple[str, int], dict[str, Any]] = {}
        self._observability: dict[str, Any] = {}

    @property
    def observability(self) -> Mapping[str, Any]:
        return self._observability

    @property
    def search_hits(self) -> tuple[TelegramGlobalSearchHit, ...]:
        """Return deduplicated message hits without exposing them in logs."""

        return tuple(
            TelegramGlobalSearchHit(
                message_id=record["message_id"],
                source_identity=record["source_identity"],
                source_kind=record["source_kind"],
                source_access_type=record["source_access_type"],
                known_source=record["known_source"],
                message_date=record["message_date"],
                observed_at=record["observed_at"],
                text=record["text"],
                query_matches=tuple(record["query_matches"]),
            )
            for record in self._search_hit_records.values()
        )

    async def discover(
        self,
        request: DiscoveryRequest,
    ) -> Sequence[DiscoveredSourceCandidate]:
        self._reset_observability()
        self._search_hit_records = {}
        matches: dict[str, dict[str, Any]] = {}
        known_removed: set[str] = set()
        total_raw_hits = 0
        stop_after_cap = False
        for query_index, query in enumerate(self._queries):
            if total_raw_hits >= self._max_total_hits:
                stop_after_cap = True
                break
            started = monotonic()
            query_raw_hits = 0
            query_pages = 0
            offset_id = 0
            offset_peer: Any = None
            offset_rate = 0
            seen_cursors: set[tuple[int, str, int]] = set()
            try:
                while query_raw_hits < self._max_results_per_query:
                    remaining = min(
                        self._max_results_per_query - query_raw_hits,
                        self._max_total_hits - total_raw_hits,
                    )
                    if remaining <= 0:
                        stop_after_cap = total_raw_hits >= self._max_total_hits
                        break
                    page_limit = min(self._page_size, remaining)
                    (
                        messages,
                        next_offset_id,
                        next_offset_peer,
                        next_offset_rate,
                        cache_hit,
                    ) = (
                        await self._governed_global_search_page(
                            query,
                            limit=page_limit,
                            offset_id=offset_id,
                            offset_peer=offset_peer,
                            offset_rate=offset_rate,
                        )
                    )
                    # Telegram normally honors ``limit``; truncate defensively
                    # so the experiment's global raw-hit cap remains hard even
                    # if a test double or transport adapter over-returns.
                    values = tuple(messages or ())[:remaining]
                    if not cache_hit:
                        self._observability["request_count"] += 1
                    else:
                        self._observability["cache_hit_count"] += 1
                    query_pages += 1
                    query_raw_hits += len(values)
                    total_raw_hits += len(values)
                    self._observability["search_results_considered"] += len(values)
                    for rank, message in enumerate(
                        values,
                        start=query_raw_hits - len(values) + 1,
                    ):
                        self._record_search_hit(
                            message,
                            query=query,
                            result_rank=rank,
                            observed_at=datetime.now(timezone.utc),
                            matches=matches,
                            known_removed=known_removed,
                        )
                    if (
                        not values
                        or len(values) < page_limit
                        or next_offset_id <= 0
                    ):
                        break
                    cursor = (
                        next_offset_id,
                        repr(next_offset_peer),
                        next_offset_rate,
                    )
                    if cursor in seen_cursors:
                        break
                    seen_cursors.add(cursor)
                    offset_id = next_offset_id
                    offset_peer = next_offset_peer
                    offset_rate = next_offset_rate
                self._observability["queries_executed"] += 1
                self._observability["query_results"].append(
                    {
                        "query_index": query_index,
                        "family": query.family,
                        "angle": query.angle,
                        "language": query.language,
                        "success": True,
                        "result_count": query_raw_hits,
                        "page_count": query_pages,
                        "elapsed_ms": int((monotonic() - started) * 1000),
                    }
                )
            except Exception as exc:
                self._observability["query_results"].append(
                    {
                        "query_index": query_index,
                        "family": query.family,
                        "angle": query.angle,
                        "language": query.language,
                        "success": False,
                        "result_count": query_raw_hits,
                        "page_count": query_pages,
                        "elapsed_ms": int((monotonic() - started) * 1000),
                        "error_class": type(exc).__name__,
                    }
                )
                raise
            if stop_after_cap:
                break

        ordered = sorted(
            matches.values(),
            key=lambda item: (
                -len(item["query_hits"]),
                -item["message_hits"],
                item["first_rank"],
                item["identity"],
            ),
        )[: self._max_candidates]
        candidates = tuple(
            _candidate_from_match(item, request.requested_at, self._intent)
            for item in ordered
        )
        self._observability.update(
            {
                "known_sources_removed": len(known_removed),
                "unique_chat_count": len(matches),
                "telegram_like_results": len(matches),
                "new_source_candidate_count": len(candidates),
                "raw_search_hits": total_raw_hits,
                "unique_message_count": len(self._search_hit_records),
                "known_message_count": sum(
                    record["known_source"] for record in self._search_hit_records.values()
                ),
                "new_message_count": sum(
                    not record["known_source"]
                    for record in self._search_hit_records.values()
                ),
                "global_hit_cap": self._max_total_hits,
                "global_hit_cap_reached": total_raw_hits >= self._max_total_hits,
            }
        )
        return candidates

    async def _governed_global_search_page(
        self,
        query: TelegramProfileSearchQuery,
        *,
        limit: int,
        offset_id: int,
        offset_peer: Any,
        offset_rate: int,
    ) -> tuple[tuple[Any, ...], int, Any, int, bool]:
        async def operation() -> tuple[tuple[Any, ...], int, Any, int]:
            return await self._governor.run(
                TelegramRequestCategory.GLOBAL_SEARCH,
                lambda: self._global_search_page(
                    query,
                    limit=limit,
                    offset_id=offset_id,
                    offset_peer=offset_peer,
                    offset_rate=offset_rate,
                ),
            )

        if self._page_cache is None:
            return (*await operation(), False)
        value, cache_hit = await self._page_cache.get_or_fetch(
            (
                TELEGRAM_PROFILE_DISCOVERY_STRATEGY_VERSION,
                query.text,
                limit,
                offset_id,
                repr(offset_peer),
                offset_rate,
            ),
            operation,
        )
        return (*value, cache_hit)

    async def _global_search_page(
        self,
        query: TelegramProfileSearchQuery,
        *,
        limit: int,
        offset_id: int,
        offset_peer: Any,
        offset_rate: int,
    ) -> tuple[tuple[Any, ...], int, Any, int]:
        if callable(self._client):
            response = await self._client(
                functions.messages.SearchGlobalRequest(
                    q=query.text,
                    filter=telethon_types.InputMessagesFilterEmpty(),
                    min_date=None,
                    max_date=None,
                    offset_rate=offset_rate,
                    offset_peer=(
                        offset_peer
                        if offset_peer is not None
                        else telethon_types.InputPeerEmpty()
                    ),
                    offset_id=offset_id,
                    limit=limit,
                )
            )
            entities = {
                telethon_utils.get_peer_id(entity): entity
                for entity in (
                    tuple(getattr(response, "users", ()))
                    + tuple(getattr(response, "chats", ()))
                )
            }
            messages = tuple(getattr(response, "messages", ()))
            for message in messages:
                finish_init = getattr(message, "_finish_init", None)
                if callable(finish_init):
                    finish_init(self._client, entities, None)
            last = next(
                (
                    message
                    for message in reversed(messages)
                    if isinstance(getattr(message, "id", None), int)
                ),
                None,
            )
            if last is None:
                return messages, 0, telethon_types.InputPeerEmpty(), 0
            return (
                messages,
                int(last.id),
                getattr(last, "input_chat", None)
                or telethon_types.InputPeerEmpty(),
                int(getattr(response, "next_rate", 0) or 0),
            )

        kwargs: dict[str, Any] = {
            "search": query.text,
            "limit": limit,
        }
        if offset_id:
            kwargs["offset_id"] = offset_id
        try:
            result = await self._client.get_messages(None, **kwargs)
        except TypeError:
            if offset_id:
                return (), 0, telethon_types.InputPeerEmpty(), 0
            result = await self._client.get_messages(
                None,
                search=query.text,
                limit=limit,
            )
        values = tuple(result or ())
        last = next(
            (
                message
                for message in reversed(values)
                if isinstance(getattr(message, "id", None), int)
            ),
            None,
        )
        return (
            values,
            0 if last is None else int(last.id),
            telethon_types.InputPeerEmpty(),
            0,
        )

    def _record_search_hit(
        self,
        message: Any,
        *,
        query: TelegramProfileSearchQuery,
        result_rank: int,
        observed_at: datetime,
        matches: dict[str, dict[str, Any]],
        known_removed: set[str],
    ) -> None:
        chat = _message_chat(message)
        if chat is None:
            return
        source_chat = _source_chat(message)
        identity = _chat_identity(chat)
        message_id = getattr(message, "id", None)
        if identity is None:
            return
        known = _chat_is_known(chat, identity, self._known)
        if known and source_chat is not None:
            known_removed.add(identity)
        self._observability["global_message_hits"] += 1
        query_match = TelegramProfileSearchQueryMatch(
            text=query.text,
            family=query.family,
            language=query.language,
            angle=query.angle,
            query_kind=query.query_kind,
            result_rank=result_rank,
        )
        if isinstance(message_id, int) and message_id > 0:
            hit_key = (identity, message_id)
            raw_text = getattr(message, "message", None)
            if raw_text is None:
                raw_text = getattr(message, "text", "")
            text = str(raw_text or "")[:MAX_HIT_TEXT_LENGTH]
            existing = self._search_hit_records.get(hit_key)
            if existing is None:
                message_date = getattr(message, "date", None)
                if not isinstance(message_date, datetime):
                    message_date = observed_at
                self._search_hit_records[hit_key] = {
                    "message_id": message_id,
                    "source_identity": identity,
                    "source_kind": _source_kind(chat),
                    "source_access_type": (
                        "public"
                        if getattr(chat, "username", None)
                        else "private"
                    ),
                    "known_source": known,
                    "message_date": message_date,
                    "observed_at": observed_at,
                    "text": text,
                    "query_matches": [query_match],
                }
            else:
                existing["known_source"] = existing["known_source"] or known
                existing["query_matches"].append(query_match)
        if known or source_chat is None:
            return
        item = matches.setdefault(
            identity,
            {
                "chat": source_chat,
                "identity": identity,
                "first_rank": result_rank,
                "message_hits": 0,
                "query_hits": {},
                "matches": [],
            },
        )
        item["message_hits"] += 1
        item["first_rank"] = min(item["first_rank"], result_rank)
        item["query_hits"][query.text] = item["query_hits"].get(query.text, 0) + 1
        item["matches"].append(
            {
                "query": query.text,
                "query_family": query.family,
                "query_angle": query.angle,
                "query_kind": query.query_kind,
                "topic": query.text,
                "result_title": _chat_display_name(chat),
                "result_snippet": query.text,
                "result_rank": result_rank,
            }
        )

    def _reset_observability(self) -> None:
        self._observability = {
            "strategy_version": TELEGRAM_PROFILE_DISCOVERY_STRATEGY_VERSION,
            "queries_generated": len(self._queries),
            "queries_executed": 0,
            "search_results_considered": 0,
            "request_count": 0,
            "cache_hit_count": 0,
            "global_message_hits": 0,
            "unique_chat_count": 0,
            "known_sources_removed": 0,
            "telegram_like_results": 0,
            "new_source_candidate_count": 0,
            "raw_search_hits": 0,
            "unique_message_count": 0,
            "known_message_count": 0,
            "new_message_count": 0,
            "global_hit_cap": self._max_total_hits,
            "global_hit_cap_reached": False,
            "query_results": [],
        }


def profile_discovery_job_key(profile_id: Any, profile_revision: int) -> str:
    if profile_revision < 1:
        raise ValueError("profile_revision must be positive")
    return f"{TELEGRAM_PROFILE_DISCOVERY_JOB_KEY_PREFIX}:{profile_id}:r{profile_revision}"


def _language_order(values: Sequence[Any]) -> tuple[str, ...]:
    result: list[str] = []
    for value in values:
        text = str(value).casefold()
        language = "ru" if text.startswith("ru") or any("а" <= char <= "я" for char in text) else "en"
        if language not in result:
            result.append(language)
    return tuple(result) or ("en", "ru")


def _technical_term(intent: Any) -> str:
    values = tuple(getattr(intent, "skills", ())) + tuple(getattr(intent, "roles", ()))
    for value in values:
        text = str(value).strip()
        if text and any(char.isascii() for char in text):
            return text
    return "Python"


def _source_chat(message: Any) -> Any | None:
    chat = _message_chat(message)
    if isinstance(chat, (telethon_types.Channel, telethon_types.Chat)):
        return chat
    return None


def _message_chat(message: Any) -> Any | None:
    chat = getattr(message, "chat", None)
    if isinstance(
        chat,
        (telethon_types.Channel, telethon_types.Chat, telethon_types.User),
    ):
        return chat
    return None


def _source_kind(chat: Any) -> str:
    if isinstance(chat, telethon_types.Channel):
        return "channel"
    if isinstance(chat, telethon_types.User):
        return "user"
    return "group"


def _chat_identity(chat: Any) -> str | None:
    try:
        value = telethon_utils.get_peer_id(chat)
    except (TypeError, ValueError):
        value = getattr(chat, "id", None)
    if isinstance(value, bool) or not isinstance(value, int):
        return None
    return str(value)


def _chat_is_known(chat: Any, identity: str, known: set[str]) -> bool:
    values = {identity}
    username = getattr(chat, "username", None)
    if isinstance(username, str) and username.strip():
        values.update({username.casefold(), f"@{username.casefold()}", f"https://t.me/{username.casefold()}"})
    return any(_normalize_identity(value) in known for value in values)


def _candidate_from_match(item: Mapping[str, Any], discovered_at: datetime, intent: Any) -> DiscoveredSourceCandidate:
    chat = item["chat"]
    identity = item["identity"]
    username = getattr(chat, "username", None)
    username = username.strip().casefold() if isinstance(username, str) and username.strip() else None
    display_name = _chat_display_name(chat)
    context = {
        "profile_discovery_intent_id": str(getattr(intent, "id")),
        "profile_revision": int(getattr(intent, "profile_revision")),
        "strategy_version": TELEGRAM_PROFILE_DISCOVERY_STRATEGY_VERSION,
        "message_hit_count": int(item["message_hits"]),
        "query_hit_count": len(item["query_hits"]),
        "first_result_rank": int(item["first_rank"]),
        "matches": list(item["matches"]),
    }
    return DiscoveredSourceCandidate(
        result_key=f"telegram-global:{identity}",
        platform="telegram",
        external_id=identity,
        access_type="public" if username is not None else "private",
        display_name=display_name,
        discovered_at=discovered_at,
        handle=None if username is None else f"@{username}",
        canonical_url=None if username is None else f"https://t.me/{username}",
        context=context,
    )


def _chat_display_name(chat: Any) -> str:
    for attribute in ("title", "username"):
        value = getattr(chat, attribute, None)
        if isinstance(value, str) and value.strip():
            return value.strip()
    return "Telegram source"


def _normalize_identity(value: Any) -> str:
    text = str(value).strip().casefold()
    if text.startswith("https://t.me/"):
        text = text.removeprefix("https://t.me/").rstrip("/")
    return text.removeprefix("@")


class TelegramProfileDiscoveryJobBlocked(RuntimeError):
    retryable = False


class TelegramProfileDiscoveryJobProcessor:
    """Process one activation-triggered discovery job without delivery handlers."""

    def __init__(
        self,
        database: Database,
        config: RuntimeConfig,
        *,
        client: Any,
        governor: TelegramRequestGovernor,
        logger: Any,
        collector_account_id: int,
    ) -> None:
        self._database = database
        self._config = config
        self._client = client
        self._governor = governor
        self._logger = logger
        self._collector_account_id = collector_account_id

    async def __call__(self, claim: JobClaim) -> None:
        profile_id, revision = _parse_profile_discovery_job_key(claim.idempotency_key)
        async with self._database.connect() as connection:
            profile = await SearchProfileRepository().get(connection, profile_id)
        if (
            not profile.is_active
            or not profile.is_primary
            or profile.revision != revision
        ):
            log_event(
                self._logger,
                logging.INFO,
                "profile.discovery.telegram_job_skipped",
                profile_id=profile_id,
                profile_revision=revision,
                reason="profile_revision_not_currently_active",
            )
            return
        from .profile_discovery import ProfileDiscoveryService

        try:
            await ProfileDiscoveryService(self._database).discover_telegram_profile(
                profile,
                requested_at=datetime.now(timezone.utc),
                run_key=f"{TELEGRAM_PROFILE_DISCOVERY_JOB_KEY_PREFIX}:{profile_id}:r{revision}:activation",
                client=self._client,
                governor=self._governor,
                max_candidates=min(100, self._config.source_discovery_max_candidates),
            )
        except Exception as exc:
            if "flood" in type(exc).__name__.casefold() or "flood" in str(exc).casefold():
                raise TelegramProfileDiscoveryJobBlocked(
                    "Telegram global discovery stopped by collector rate limit"
                ) from exc
            raise


class TelegramProfileDiscoveryRuntime:
    """Discovery-only durable worker; it never owns bot or delivery handlers."""

    def __init__(
        self,
        database: Database,
        config: RuntimeConfig,
        *,
        client: Any,
        collector_account_id: int,
        governor: TelegramRequestGovernor,
        logger: Any,
        worker_id: str,
    ) -> None:
        self._worker = DurableWorker(
            database,
            repository=DurableJobRepository(),
            worker_id=worker_id,
            handlers={
                TELEGRAM_PROFILE_DISCOVERY_JOB_TYPE: TelegramProfileDiscoveryJobProcessor(
                    database,
                    config,
                    client=client,
                    governor=governor,
                    logger=logger,
                    collector_account_id=collector_account_id,
                )
            },
            logger=logger,
            options=WorkerOptions.from_config(config),
            close_database_on_exit=False,
        )
        self._task: Any | None = None

    async def start(self) -> None:
        if self._task is not None:
            raise RuntimeError("Telegram profile discovery runtime already started")
        self._task = asyncio.create_task(
            self._worker.run(install_signal_handlers=False),
            name="telegram-profile-discovery-worker",
        )
        await asyncio.sleep(0)
        if self._task.done():
            await self._task

    async def stop(self) -> None:
        if self._task is None:
            return
        self._worker.request_stop()
        try:
            await self._task
        finally:
            self._task = None


def _parse_profile_discovery_job_key(value: str) -> tuple[UUID, int]:
    prefix = f"{TELEGRAM_PROFILE_DISCOVERY_JOB_KEY_PREFIX}:"
    if not value.startswith(prefix):
        raise ValueError("invalid Telegram profile discovery job key")
    payload = value.removeprefix(prefix)
    profile_text, revision_text = payload.rsplit(":r", 1)
    revision = int(revision_text)
    return UUID(profile_text), revision
