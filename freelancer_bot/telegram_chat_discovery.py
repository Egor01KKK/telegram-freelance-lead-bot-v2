"""Bounded, durable Telegram chat discovery.

The service searches Telegram's global index for reusable community topics,
deduplicates the entities returned in ``response.chats`` and screens only new
peers with one bounded recent-history read.  It never joins chats, paginates
history or creates an approval outside the normal source lifecycle.
"""

from __future__ import annotations

import asyncio
from collections import Counter, defaultdict
from collections.abc import Awaitable, Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import json
import logging
import re
import socket
import urllib.error
import urllib.request
from typing import Any, Literal, Protocol
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, field_validator
from telethon import functions
from telethon.errors import FloodWaitError
from telethon import types as telethon_types
from telethon import utils as telethon_utils

from .config import RuntimeConfig
from .observability import log_event
from .persistence.database import Database
from .persistence.jobs import DurableJobRepository, JobClaim
from .persistence.source_repository import (
    SourceIdentityConflict,
    SourceRepository,
    SourceStatus,
)
from .persistence.telegram_chat_discovery import (
    DEDUP_BUCKETS,
    SCREEN_JOB_TYPE,
    SEARCH_JOB_TYPE,
    ChatDiscoveryBackpressure,
    ChatDiscoveryPeerRecord,
    ChatDiscoverySearchRunRecord,
    ChatDiscoveryTopicRecord,
    ScreenClaim,
    TelegramChatDiscoveryRepository,
    normalize_topic,
)
from .persistence.telegram_operation_state import TelegramCollectorFloodWaitActive
from .source_ai_config import (
    OPENAI_CHAT_COMPLETIONS_URL,
    SourceAIProviderSettings,
    SourceAIProviderUnavailable,
    UnsupportedSourceAIProvider,
    normalize_chat_completions_url,
    resolve_source_ai_provider,
    source_ai_provider_available,
)
from .telegram_request_governor import TelegramRequestCategory, TelegramRequestGovernor
from .telegram_profile_discovery import build_telegram_profile_search_queries
from .worker import DurableWorker, WorkerOptions
from .openai_compat import add_sampling_parameter


TELEGRAM_CHAT_DISCOVERY_PROVIDER = "telegram_chat_search"
TELEGRAM_CHAT_DISCOVERY_SCHEMA_VERSION = "telegram-chat-screen.v1"
TELEGRAM_CHAT_DISCOVERY_PROMPT_VERSION = "telegram-chat-screen-prompt.v1"
TELEGRAM_CHAT_DISCOVERY_ANALYZER_VERSION = "telegram-chat-screen-openai.v1"

SCREEN_CATEGORIES = (
    "BUYER_TO_SPECIALIST",
    "PROJECT_CONTRACTOR_DEMAND",
    "VACANCY",
    "RECOMMENDATION_REQUEST",
    "SELLER_SELF_PROMO",
    "ADS_SPAM",
    "IRRELEVANT",
    "DUPLICATE_REPOST",
    "OTHER",
)
USEFUL_CATEGORIES = frozenset(
    {
        "BUYER_TO_SPECIALIST",
        "PROJECT_CONTRACTOR_DEMAND",
        "VACANCY",
        "RECOMMENDATION_REQUEST",
    }
)

LOGGER = logging.getLogger("freelancer_bot")


class TelegramChatDiscoveryError(RuntimeError):
    retryable = True


class TelegramChatDiscoveryFloodWait(TelegramChatDiscoveryError):
    """Stop this collector's durable work until its governor cooldown ends."""

    retryable = False


class TelegramChatScreenError(TelegramChatDiscoveryError):
    pass


class TelegramChatScreenTimeout(TelegramChatScreenError):
    """The provider request exceeded its bounded network timeout."""

    pass


@dataclass(frozen=True)
class ScreenMessage:
    message_id: int
    occurred_at: datetime
    text: str


@dataclass(frozen=True)
class ScreenClassification:
    decision: str
    confidence: float
    labels: tuple[str, ...]
    reason_codes: tuple[str, ...] = ()


class TelegramChatScreenProvider(Protocol):
    name: str
    model: str

    async def classify(
        self,
        peer: ChatDiscoveryPeerRecord,
        messages: Sequence[ScreenMessage],
    ) -> ScreenClassification: ...


class ScreenLabel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    message_index: int = Field(ge=1, le=25)
    category: Literal[
        "BUYER_TO_SPECIALIST",
        "PROJECT_CONTRACTOR_DEMAND",
        "VACANCY",
        "RECOMMENDATION_REQUEST",
        "SELLER_SELF_PROMO",
        "ADS_SPAM",
        "IRRELEVANT",
        "DUPLICATE_REPOST",
        "OTHER",
    ]
    confidence: float = Field(ge=0, le=1)


class ScreenResponse(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    decision: Literal["WATCH", "SKIP", "UNCLEAR"]
    confidence: float = Field(ge=0, le=1)
    labels: tuple[ScreenLabel, ...] = Field(min_length=0, max_length=25)
    reason_codes: tuple[str, ...] = Field(max_length=8)

    @field_validator("reason_codes")
    @classmethod
    def safe_reasons(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        return tuple(
            re.sub(r"[^a-z0-9_.-]", "_", item.strip().casefold())[:64]
            for item in value
            if item.strip()
        )


def telegram_chat_screen_response_schema() -> dict[str, Any]:
    return {
        "type": "object",
        "additionalProperties": False,
        "required": ["decision", "confidence", "labels", "reason_codes"],
        "properties": {
            "decision": {"type": "string", "enum": ["WATCH", "SKIP", "UNCLEAR"]},
            "confidence": {"type": "number", "minimum": 0, "maximum": 1},
            "labels": {
                "type": "array",
                "minItems": 0,
                "maxItems": 25,
                "items": {
                    "type": "object",
                    "additionalProperties": False,
                    "required": ["message_index", "category", "confidence"],
                    "properties": {
                        "message_index": {"type": "integer", "minimum": 1, "maximum": 25},
                        "category": {"type": "string", "enum": list(SCREEN_CATEGORIES)},
                        "confidence": {"type": "number", "minimum": 0, "maximum": 1},
                    },
                },
            },
            "reason_codes": {
                "type": "array",
                "maxItems": 8,
                "items": {"type": "string", "pattern": "^[a-z0-9_.-]{1,64}$"},
            },
        },
    }


class OpenAICompatibleTelegramChatScreenProvider:
    def __init__(
        self,
        *,
        api_key: str,
        model: str,
        temperature: float = 0.0,
        timeout_seconds: int = 45,
        max_output_attempts: int = 2,
        base_url: str = OPENAI_CHAT_COMPLETIONS_URL,
        provider: str = "openai",
    ) -> None:
        if not api_key.strip():
            raise TelegramChatScreenError(f"{provider.upper()}_API_KEY is not configured")
        if timeout_seconds <= 0:
            raise ValueError("timeout_seconds must be positive")
        if not 1 <= max_output_attempts <= 3:
            raise ValueError("max_output_attempts must be between 1 and 3")
        normalized_provider = provider.strip().lower()
        if normalized_provider not in {"openai", "deepseek", "tokenrouter"}:
            raise TelegramChatScreenError(
                f"Unsupported Telegram chat-screen provider: {provider}"
            )
        self._api_key = api_key
        self.name = normalized_provider
        self.model = model.strip()
        self._temperature = temperature
        self._timeout_seconds = timeout_seconds
        self._max_output_attempts = max_output_attempts
        self._base_url = (
            base_url
            if normalized_provider == "openai"
            else normalize_chat_completions_url(base_url)
        )

    async def classify(
        self,
        peer: ChatDiscoveryPeerRecord,
        messages: Sequence[ScreenMessage],
    ) -> ScreenClassification:
        payload: dict[str, Any] = {
            "model": self.model,
            "response_format": _telegram_chat_screen_response_format(self.name),
            "messages": [
                {
                    "role": "system",
                    "content": _telegram_chat_screen_system_prompt(self.name),
                },
                {
                    "role": "user",
                    "content": json.dumps(
                        {
                            "peer_type": peer.peer_type,
                            "display_name": peer.display_name,
                            "sample": [
                                {
                                    "message_index": index,
                                    "message_id": item.message_id,
                                    "occurred_at": item.occurred_at.isoformat(),
                                    "text": item.text,
                                }
                                for index, item in enumerate(messages, start=1)
                            ],
                        },
                        ensure_ascii=False,
                    ),
                },
            ],
        }
        add_sampling_parameter(payload, model=self.model, temperature=self._temperature)
        last_error: Exception | None = None
        for attempt in range(1, self._max_output_attempts + 1):
            try:
                raw = await asyncio.to_thread(self._request, payload)
                response = json.loads(raw)
                content = response["choices"][0]["message"]["content"]
                parsed = ScreenResponse.model_validate_json(content)
                indices = [label.message_index for label in parsed.labels]
                expected_indices = list(range(1, len(messages) + 1))
                if sorted(indices) != expected_indices:
                    raise ValueError("OpenAI did not return one label per message index")
                return ScreenClassification(
                    decision=parsed.decision,
                    confidence=float(parsed.confidence),
                    labels=tuple(label.category for label in sorted(parsed.labels, key=lambda item: item.message_index)),
                    reason_codes=parsed.reason_codes,
                )
            except (KeyError, IndexError, TypeError, ValueError) as exc:
                last_error = exc
                if attempt == self._max_output_attempts:
                    break
                payload["messages"].append(
                    {
                        "role": "system",
                        "content": (
                            f"{self.name} must return a complete replacement JSON object "
                            "with one label for every message_index."
                        ),
                    }
                )
            except TelegramChatScreenError:
                raise
        raise TelegramChatScreenError(
            f"{self.name} returned an invalid chat-screen result"
        ) from last_error

    def _request(self, payload: Mapping[str, Any]) -> str:
        request = urllib.request.Request(
            self._base_url,
            data=json.dumps(payload, ensure_ascii=False).encode("utf-8"),
            headers={
                "Authorization": f"Bearer {self._api_key}",
                "Content-Type": "application/json",
            },
            method="POST",
        )
        try:
            with urllib.request.urlopen(request, timeout=self._timeout_seconds) as response:
                return response.read().decode("utf-8")
        except socket.timeout as exc:
            raise TelegramChatScreenTimeout(
                f"{self.name} chat-screen request timed out"
            ) from exc
        except TimeoutError as exc:
            raise TelegramChatScreenTimeout(
                f"{self.name} chat-screen request timed out"
            ) from exc
        except urllib.error.HTTPError as exc:
            exc.close()
            raise TelegramChatScreenError(
                f"{self.name} chat-screen request failed"
            ) from exc
        except urllib.error.URLError as exc:
            reason = getattr(exc, "reason", None)
            if (
                isinstance(reason, (socket.timeout, TimeoutError))
                or "timed out" in str(reason).casefold()
                or "timeout" in str(reason).casefold()
            ):
                raise TelegramChatScreenTimeout(
                    f"{self.name} chat-screen request timed out"
                ) from exc
            raise TelegramChatScreenError(
                f"{self.name} chat-screen request failed"
            ) from exc


class OpenAITelegramChatScreenProvider(OpenAICompatibleTelegramChatScreenProvider):
    """Backward-compatible OpenAI chat-screen provider name."""

    def __init__(
        self,
        *,
        api_key: str,
        model: str,
        temperature: float = 0.0,
        timeout_seconds: int = 45,
        max_output_attempts: int = 2,
        base_url: str = OPENAI_CHAT_COMPLETIONS_URL,
    ) -> None:
        super().__init__(
            api_key=api_key,
            model=model,
            temperature=temperature,
            timeout_seconds=timeout_seconds,
            max_output_attempts=max_output_attempts,
            base_url=base_url,
            provider="openai",
        )


def telegram_chat_screen_provider_from_config(
    config: RuntimeConfig,
) -> OpenAICompatibleTelegramChatScreenProvider:
    provider = telegram_chat_screen_provider_name(config)
    model = telegram_chat_screen_model(config)
    settings: SourceAIProviderSettings = resolve_source_ai_provider(
        config,
        provider=provider,
    )
    return OpenAICompatibleTelegramChatScreenProvider(
        api_key=settings.api_key,
        model=model,
        temperature=config.source_audit_temperature,
        timeout_seconds=config.telegram_chat_discovery_screen_timeout_seconds,
        base_url=settings.base_url,
        provider=settings.name,
    )


def telegram_chat_screen_provider_name(config: RuntimeConfig) -> str:
    configured = getattr(config, "telegram_chat_discovery_screen_provider", None)
    return str(
        configured or getattr(config, "source_audit_provider", "openai")
    ).strip().lower()


def telegram_chat_screen_model(config: RuntimeConfig) -> str:
    configured = getattr(config, "telegram_chat_discovery_screen_model", None)
    return str(
        configured or getattr(config, "source_audit_model", "gpt-5-nano")
    ).strip()


def telegram_chat_screen_provider_available(config: RuntimeConfig) -> bool:
    return source_ai_provider_available(
        config,
        provider=telegram_chat_screen_provider_name(config),
    )


def _telegram_chat_screen_response_format(provider: str) -> dict[str, Any]:
    if provider == "openai":
        return {
            "type": "json_schema",
            "json_schema": {
                "name": "telegram_chat_screen",
                "strict": True,
                "schema": telegram_chat_screen_response_schema(),
            },
        }
    return {"type": "json_object"}


def _telegram_chat_screen_system_prompt(provider: str) -> str:
    prompt = (
        "Decide whether this Telegram community should enter a later "
        "source-candidate audit. Classify every sampled message into one "
        "primary category. Buyer demand and contractor/project demand are "
        "useful; seller self-promotion is not useful. Never infer missing "
        "messages, access, identity or approval. Return exactly one label "
        "per supplied message index."
    )
    if provider != "openai":
        prompt += (
            " Return one JSON object only, with no markdown or prose, matching "
            "this complete Telegram chat-screen contract: "
            + json.dumps(
                telegram_chat_screen_response_schema(),
                ensure_ascii=False,
                sort_keys=True,
            )
        )
    return prompt


@dataclass(frozen=True)
class TelegramChatScreenPolicy:
    version: str
    minimum_sample: int
    minimum_useful_messages: int
    minimum_useful_ratio: float
    minimum_confidence: float
    maximum_seller_ratio: float
    maximum_spam_ratio: float

    @classmethod
    def from_config(cls, config: RuntimeConfig) -> "TelegramChatScreenPolicy":
        return cls(
            version=config.telegram_chat_discovery_screen_policy_version,
            minimum_sample=config.telegram_chat_discovery_screen_min_sample,
            minimum_useful_messages=config.telegram_chat_discovery_screen_min_useful_messages,
            minimum_useful_ratio=config.telegram_chat_discovery_screen_min_useful_ratio,
            minimum_confidence=config.telegram_chat_discovery_screen_min_confidence,
            maximum_seller_ratio=config.telegram_chat_discovery_screen_max_seller_ratio,
            maximum_spam_ratio=config.telegram_chat_discovery_screen_max_spam_ratio,
        )

    def evaluate(
        self,
        *,
        sample_count: int,
        classification: ScreenClassification,
    ) -> tuple[str, int, Mapping[str, int], tuple[str, ...]]:
        labels = tuple(classification.labels)
        counts = Counter(labels)
        category_counts = {category: int(counts.get(category, 0)) for category in SCREEN_CATEGORIES}
        useful = sum(category_counts.get(category, 0) for category in USEFUL_CATEGORIES)
        if sample_count <= 0 or sample_count < self.minimum_sample:
            return "UNCLEAR", useful, category_counts, ("insufficient_sample",)
        if len(labels) != sample_count:
            return "UNCLEAR", useful, category_counts, ("incomplete_message_labels",)
        if classification.confidence < self.minimum_confidence:
            return "UNCLEAR", useful, category_counts, ("low_confidence",)
        useful_ratio = useful / sample_count
        seller_ratio = category_counts["SELLER_SELF_PROMO"] / sample_count
        spam_ratio = category_counts["ADS_SPAM"] / sample_count
        if (
            useful >= self.minimum_useful_messages
            and useful_ratio >= self.minimum_useful_ratio
            and seller_ratio <= self.maximum_seller_ratio
            and spam_ratio <= self.maximum_spam_ratio
        ):
            return "WATCH", useful, category_counts, ("useful_demand_thresholds_met",)
        if useful == 0 or seller_ratio > self.maximum_seller_ratio or spam_ratio > self.maximum_spam_ratio:
            return "SKIP", useful, category_counts, ("non_useful_content_dominates",)
        return "UNCLEAR", useful, category_counts, ("ambiguous_content_mix",)


@dataclass(frozen=True)
class ChatDiscoverySearchResult:
    topic: ChatDiscoveryTopicRecord
    run: ChatDiscoverySearchRunRecord
    unique_peers: int
    known_peers: int
    new_peers: int
    screen_jobs_created: int


@dataclass(frozen=True)
class ChatDiscoveryScreenResult:
    peer: ChatDiscoveryPeerRecord
    status: str
    sample_count: int
    useful_count: int
    category_counts: Mapping[str, int]
    reason_codes: tuple[str, ...]


class TelegramChatDiscoveryService:
    def __init__(
        self,
        database: Database,
        client: Any,
        *,
        config: RuntimeConfig,
        collector_account_id: int,
        governor: TelegramRequestGovernor,
        screen_provider: TelegramChatScreenProvider | None = None,
        repository: TelegramChatDiscoveryRepository | None = None,
        source_repository: SourceRepository | None = None,
        watch_candidate_callback: Callable[[int], Awaitable[None]] | None = None,
        logger: logging.Logger | None = None,
    ) -> None:
        if collector_account_id <= 0:
            raise ValueError("collector_account_id must be positive")
        self.database = database
        self.client = client
        self.config = config
        self.collector_account_id = collector_account_id
        self.governor = governor
        self.repository = repository or TelegramChatDiscoveryRepository()
        self.sources = source_repository or SourceRepository()
        self.watch_candidate_callback = watch_candidate_callback
        self.logger = logger or LOGGER
        self.policy = TelegramChatScreenPolicy.from_config(config)
        self.screen_provider = screen_provider or self._provider_from_config(config)

    async def run_search(
        self,
        topic: ChatDiscoveryTopicRecord,
        *,
        search_budget: int = 20,
        refresh_key: str | None = None,
    ) -> ChatDiscoverySearchResult:
        if not 1 <= search_budget <= 50:
            raise ValueError("search_budget must be between 1 and 50")
        refresh = refresh_key or _refresh_bucket(topic)
        idempotency_key = f"topic:{topic.id}:refresh:{refresh}"
        async with self.database.transaction() as connection:
            started = await self.repository.start_search(
                connection,
                topic_id=topic.id,
                collector_account_id=self.collector_account_id,
                idempotency_key=idempotency_key,
            )
        if started is None:
            raise RuntimeError("chat-discovery search was not started")
        if started.status == "completed":
            return ChatDiscoverySearchResult(
                topic=topic,
                run=started,
                unique_peers=started.unique_peer_count,
                known_peers=started.known_peer_count,
                new_peers=started.new_peer_count,
                screen_jobs_created=0,
            )

        try:
            response = await self.governor.run(
                TelegramRequestCategory.GLOBAL_SEARCH,
                lambda: self.client(
                    functions.messages.SearchGlobalRequest(
                        q=topic.topic_text,
                        filter=telethon_types.InputMessagesFilterEmpty(),
                        min_date=None,
                        max_date=None,
                        offset_rate=0,
                        offset_peer=telethon_types.InputPeerEmpty(),
                        offset_id=0,
                        limit=search_budget,
                    )
                ),
            )
            messages = tuple(getattr(response, "messages", ()) or ())[:search_budget]
            entities = tuple(getattr(response, "chats", ()) or ())
            message_hits = _message_hits_by_peer(messages)
            unique_entities: dict[str, Any] = {}
            for entity in entities:
                descriptor = _peer_descriptor(entity)
                if descriptor is not None:
                    unique_entities.setdefault(descriptor.canonical_peer_identity, entity)
            screen_jobs_created = 0
            known_count = 0
            new_count = 0
            groups = 0
            broadcasts = 0
            async with self.database.transaction() as connection:
                for canonical_identity, entity in unique_entities.items():
                    descriptor = _peer_descriptor(entity)
                    if descriptor is None:
                        continue
                    references = _peer_references(descriptor)
                    existing_peer = None
                    for reference in references:
                        existing_peer = await self.repository.get_peer_by_alias(connection, reference)
                        if existing_peer is not None:
                            break
                    source = await self.repository.find_source_for_references(connection, references)
                    bucket = _dedup_bucket(source)
                    source_id = None if source is None else int(source["id"])
                    if existing_peer is not None and source_id is None:
                        source_id = existing_peer.source_id
                        bucket = existing_peer.dedup_bucket
                    peer, created = await self.repository.upsert_peer(
                        connection,
                        canonical_peer_identity=(
                            existing_peer.canonical_peer_identity
                            if existing_peer is not None
                            else canonical_identity
                        ),
                        peer_type=descriptor.peer_type,
                        telegram_peer_id=descriptor.telegram_peer_id,
                        telegram_access_hash=descriptor.telegram_access_hash,
                        display_name=descriptor.display_name,
                        username=descriptor.username,
                        canonical_url=descriptor.canonical_url,
                        access_type=descriptor.access_type,
                        source_id=source_id,
                        dedup_bucket=bucket,
                        collector_account_id=self.collector_account_id,
                    )
                    await self.repository.add_aliases(
                        connection,
                        peer_id=peer.id,
                        aliases=tuple((value, kind) for value, kind in references_with_kinds(descriptor)),
                    )
                    await self.repository.add_observation(
                        connection,
                        peer_id=peer.id,
                        topic_id=topic.id,
                        search_run_id=started.id,
                        collector_account_id=self.collector_account_id,
                        language=topic.language,
                        search_mode="global",
                        message_hit_count=message_hits.get(canonical_identity, 0),
                    )
                    if source_id is not None:
                        await self.sources.record_lineage(
                            connection,
                            source_id=source_id,
                            provider=TELEGRAM_CHAT_DISCOVERY_PROVIDER,
                            lineage_key=f"{topic.topic_key}:{peer.canonical_peer_identity}",
                            provider_run_id=str(started.id),
                            discovered_at=datetime.now(timezone.utc),
                            context={
                                "topic": topic.topic_text,
                                "language": topic.language,
                                "search_mode": "global",
                                "canonical_peer_identity": peer.canonical_peer_identity,
                            },
                        )
                    if descriptor.peer_type in {"group", "supergroup"}:
                        groups += 1
                    else:
                        broadcasts += 1
                    if bucket == "GENUINELY_NEW" and peer.screen_status in {"SCREEN_PENDING", "UNCLEAR", "SCREEN_FAILED"}:
                        await self.repository.enqueue_screen_job(
                            connection,
                            peer_id=peer.id,
                            attempt_number=max(1, peer.screen_attempt_count + 1),
                        )
                        screen_jobs_created += int(created)
                    known_count += int(bucket != "GENUINELY_NEW")
                    new_count += int(bucket == "GENUINELY_NEW")
                finished = await self.repository.finish_search(
                    connection,
                    run_id=started.id,
                    topic_id=topic.id,
                    collector_account_id=self.collector_account_id,
                    request_count=1,
                    message_hit_count=len(messages),
                    chat_entity_occurrence_count=len(entities),
                    unique_peer_count=len(unique_entities),
                    known_peer_count=known_count,
                    new_peer_count=new_count,
                    group_peer_count=groups,
                    broadcast_peer_count=broadcasts,
                )
            log_event(
                self.logger,
                logging.INFO,
                "telegram.chat_discovery.search_completed",
                topic=topic.normalized_topic,
                language=topic.language,
                collector_account_id=self.collector_account_id,
                message_hits=len(messages),
                chat_entity_occurrences=len(entities),
                unique_peers=len(unique_entities),
                known_peers=known_count,
                new_peers=new_count,
            )
            return ChatDiscoverySearchResult(
                topic=topic,
                run=finished,
                unique_peers=len(unique_entities),
                known_peers=known_count,
                new_peers=new_count,
                screen_jobs_created=screen_jobs_created,
            )
        except (FloodWaitError, TelegramCollectorFloodWaitActive) as exc:
            async with self.database.transaction() as connection:
                await self.repository.finish_search(
                    connection,
                    run_id=started.id,
                    topic_id=topic.id,
                    collector_account_id=self.collector_account_id,
                    request_count=1,
                    message_hit_count=0,
                    chat_entity_occurrence_count=0,
                    unique_peer_count=0,
                    known_peer_count=0,
                    new_peer_count=0,
                    group_peer_count=0,
                    broadcast_peer_count=0,
                    error_code=type(exc).__name__,
                )
            raise TelegramChatDiscoveryFloodWait("collector is paused by Telegram FloodWait") from exc
        except Exception as exc:
            async with self.database.transaction() as connection:
                finished = await self.repository.finish_search(
                    connection,
                    run_id=started.id,
                    topic_id=topic.id,
                    collector_account_id=self.collector_account_id,
                    request_count=1,
                    message_hit_count=0,
                    chat_entity_occurrence_count=0,
                    unique_peer_count=0,
                    known_peer_count=0,
                    new_peer_count=0,
                    group_peer_count=0,
                    broadcast_peer_count=0,
                    error_code=type(exc).__name__,
                )
            log_event(
                self.logger,
                logging.WARNING,
                "telegram.chat_discovery.search_failed",
                topic=topic.normalized_topic,
                collector_account_id=self.collector_account_id,
                error=exc,
            )
            raise

    async def screen_peer(self, peer_id: UUID) -> ChatDiscoveryScreenResult | None:
        now = datetime.now(timezone.utc)
        async with self.database.transaction() as connection:
            claim = await self.repository.claim_screen(connection, peer_id=peer_id, now=now)
        if claim is None:
            return None
        # Automatic global Chat Discovery is intentionally public-source only.
        # A peer without a public username/canonical URL may still be useful
        # to manually managed collector sources, but it must not enter the
        # automatic history/AI/source-candidate path.  Finish it as a durable
        # terminal screen result before checking provider configuration so the
        # boundary is deterministic even when AI is unavailable.
        if (
            claim.peer.dedup_bucket == "GENUINELY_NEW"
            and claim.peer.source_id is None
            and claim.peer.access_type == "private"
        ):
            async with self.database.transaction() as connection:
                peer = await self.repository.finish_screen(
                    connection,
                    claim=claim,
                    collector_account_id=self.collector_account_id,
                    status="SKIP",
                    decision="SKIP",
                    policy_version=self.policy.version,
                    provider="not_applicable",
                    model=None,
                    sample_count=0,
                    useful_count=0,
                    history_request_count=0,
                    ai_call_count=0,
                    confidence=None,
                    category_counts={},
                    reason_codes=("private_source_not_global",),
                    retry_at=None,
                )
            log_event(
                self.logger,
                logging.INFO,
                "telegram.chat_discovery.screen_skipped",
                peer_type=claim.peer.peer_type,
                collector_account_id=self.collector_account_id,
                reason="private_source_not_global",
            )
            return ChatDiscoveryScreenResult(
                peer,
                "SKIP",
                0,
                0,
                {},
                ("private_source_not_global",),
            )
        if self.screen_provider is None:
            async with self.database.transaction() as connection:
                peer = await self.repository.finish_screen(
                    connection,
                    claim=claim,
                    collector_account_id=self.collector_account_id,
                    status="SCREEN_FAILED",
                    decision="UNCLEAR",
                    policy_version=self.policy.version,
                    provider="unconfigured",
                    model=None,
                    sample_count=0,
                    useful_count=0,
                    confidence=None,
                    category_counts={},
                    reason_codes=("openai_not_configured",),
                    error_code="OpenAIKeyMissing",
                    retry_at=now + timedelta(seconds=self.config.telegram_chat_discovery_screen_retry_interval_seconds),
                )
            return ChatDiscoveryScreenResult(peer, "SCREEN_FAILED", 0, 0, {}, ("openai_not_configured",))

        history_request_count = 0
        ai_call_count = 0
        watch_source_id: int | None = None
        try:
            entity = _input_entity_for_peer(claim.peer)
            messages = await self.governor.run(
                TelegramRequestCategory.HISTORY,
                lambda: self.client.get_messages(
                    entity,
                    limit=min(25, self.config.telegram_chat_discovery_history_limit),
                ),
            )
            history_request_count = 1
            sample = _screen_messages(
                messages,
                history_limit=self.config.telegram_chat_discovery_history_limit,
                max_message_chars=self.config.telegram_chat_discovery_screen_max_message_chars,
                max_total_chars=self.config.telegram_chat_discovery_screen_max_total_chars,
                min_sample=self.config.telegram_chat_discovery_screen_min_sample,
            )
            ai_call_count = 1
            classification = await self.screen_provider.classify(claim.peer, sample)
            status, useful, category_counts, reasons = self.policy.evaluate(
                sample_count=len(sample),
                classification=classification,
            )
            async with self.database.transaction() as connection:
                peer = await self.repository.finish_screen(
                    connection,
                    claim=claim,
                    collector_account_id=self.collector_account_id,
                    status=status,
                    decision=status,
                    policy_version=self.policy.version,
                    provider=self.screen_provider.name,
                    model=self.screen_provider.model,
                    sample_count=len(sample),
                    useful_count=useful,
                    history_request_count=history_request_count,
                    ai_call_count=ai_call_count,
                    confidence=classification.confidence,
                    category_counts=category_counts,
                    reason_codes=reasons or classification.reason_codes,
                    retry_at=(
                        now + timedelta(seconds=self.config.telegram_chat_discovery_screen_retry_interval_seconds)
                        if status == "UNCLEAR"
                        else None
                    ),
                )
                if status == "WATCH":
                    watch_source_id = await self._persist_candidate(
                        connection,
                        peer=peer,
                        provider=TELEGRAM_CHAT_DISCOVERY_PROVIDER,
                        policy_version=self.policy.version,
                    )
                    peer = await self.repository.attach_source(
                        connection,
                        peer_id=peer.id,
                        source_id=watch_source_id,
                    )
            if watch_source_id is not None and self.watch_candidate_callback is not None:
                try:
                    await self.watch_candidate_callback(watch_source_id)
                except Exception as exc:
                    # The candidate is already durable. A wake signal is only an
                    # optimization; periodic discovery remains the correctness
                    # fallback if the runtime is stopping or unavailable.
                    log_event(
                        self.logger,
                        logging.WARNING,
                        "telegram.chat_discovery.watch_wake_failed",
                        source_id=watch_source_id,
                        error=exc,
                    )
            log_event(
                self.logger,
                logging.INFO,
                "telegram.chat_discovery.screen_completed",
                peer_type=claim.peer.peer_type,
                status=status,
                sample_count=len(sample),
                useful_count=useful,
                collector_account_id=self.collector_account_id,
            )
            return ChatDiscoveryScreenResult(peer, status, len(sample), useful, category_counts, reasons)
        except (FloodWaitError, TelegramCollectorFloodWaitActive) as exc:
            async with self.database.transaction() as connection:
                await self.repository.finish_screen(
                    connection,
                    claim=claim,
                    collector_account_id=self.collector_account_id,
                    status="SCREEN_FAILED",
                    decision="UNCLEAR",
                    policy_version=self.policy.version,
                    provider=getattr(self.screen_provider, "name", "unknown"),
                    model=getattr(self.screen_provider, "model", None),
                    sample_count=0,
                    useful_count=0,
                    history_request_count=history_request_count,
                    ai_call_count=ai_call_count,
                    confidence=None,
                    category_counts={},
                    reason_codes=("collector_floodwait",),
                    error_code=type(exc).__name__,
                    retry_at=now + timedelta(seconds=self.config.telegram_chat_discovery_screen_retry_interval_seconds),
                )
            raise TelegramChatDiscoveryFloodWait("collector is paused by Telegram FloodWait") from exc
        except Exception as exc:
            async with self.database.transaction() as connection:
                peer = await self.repository.finish_screen(
                    connection,
                    claim=claim,
                    collector_account_id=self.collector_account_id,
                    status="SCREEN_FAILED",
                    decision="UNCLEAR",
                    policy_version=self.policy.version,
                    provider=getattr(self.screen_provider, "name", "unknown"),
                    model=getattr(self.screen_provider, "model", None),
                    sample_count=0,
                    useful_count=0,
                    history_request_count=history_request_count,
                    ai_call_count=ai_call_count,
                    confidence=None,
                    category_counts={},
                    reason_codes=("screen_failed", type(exc).__name__.casefold()),
                    error_code=type(exc).__name__,
                    retry_at=now + timedelta(seconds=self.config.telegram_chat_discovery_screen_retry_interval_seconds),
                )
            log_event(
                self.logger,
                logging.WARNING,
                "telegram.chat_discovery.screen_failed",
                peer_type=claim.peer.peer_type,
                collector_account_id=self.collector_account_id,
                error=exc,
            )
            raise

    async def schedule_due_searches(self, *, max_topics: int) -> tuple[UUID, ...]:
        if not 1 <= max_topics <= 100:
            raise ValueError("max_topics must be between 1 and 100")
        now = datetime.now(timezone.utc)
        if await self._collector_is_paused(now):
            return ()
        async with self.database.transaction() as connection:
            await self.repository.ensure_base_topics(
                connection,
                refresh_interval_seconds=self.config.telegram_chat_discovery_refresh_interval_seconds,
            )
            pressure = await self.repository.backpressure(
                connection,
                pending_screen_limit=self.config.telegram_chat_discovery_max_pending_screens,
                source_audit_limit=self.config.source_audit_calls_per_day,
                ai_limit=self.config.opportunity_analysis_backlog_threshold,
            )
            if pressure.paused:
                return ()
            topics = await self.repository.due_topics(connection, now=now, limit=max_topics)
            job_ids: list[UUID] = []
            for topic in topics:
                key = _refresh_bucket(topic, now=now)
                job_ids.append(await self.repository.enqueue_search_job(
                    connection,
                    topic_id=topic.id,
                    refresh_key=key,
                ))
            return tuple(job_ids)

    async def _collector_is_paused(self, now: datetime) -> bool:
        try:
            state = await self.governor.current_state()
        except LookupError:
            return False
        if state.status.value == "paused":
            return True
        return (
            state.status.value == "floodwait"
            and state.cooldown_until is not None
            and state.cooldown_until > now
        )

    async def enqueue_pending_screens(self, *, limit: int) -> tuple[UUID, ...]:
        now = datetime.now(timezone.utc)
        async with self.database.transaction() as connection:
            await self.repository.reclaim_orphaned_screens(connection)
            peers = await self.repository.list_screen_pending(connection, now=now, limit=limit)
            job_ids: list[UUID] = []
            for peer in peers:
                job_ids.append(await self.repository.enqueue_screen_job(
                    connection,
                    peer_id=peer.id,
                    attempt_number=max(1, peer.screen_attempt_count + 1),
                ))
        return tuple(job_ids)

    def worker(
        self,
        *,
        worker_id: str,
        job_types: Sequence[str] = (SEARCH_JOB_TYPE, SCREEN_JOB_TYPE),
        job_ids: Sequence[UUID] | None = None,
    ) -> DurableWorker:
        handlers = {
            SEARCH_JOB_TYPE: self._handle_search_job,
            SCREEN_JOB_TYPE: self._handle_screen_job,
        }
        return DurableWorker(
            self.database,
            repository=DurableJobRepository(),
            worker_id=worker_id,
            handlers={job_type: handlers[job_type] for job_type in job_types},
            logger=self.logger,
            options=WorkerOptions.from_config(self.config),
            close_database_on_exit=False,
            job_ids=job_ids,
        )

    async def drain(
        self,
        *,
        worker_id: str,
        job_type: str,
        job_ids: Sequence[UUID] = (),
        timeout_seconds: float = 300.0,
    ) -> None:
        scoped_job_ids = tuple(job_ids)
        if not scoped_job_ids:
            return
        worker = self.worker(
            worker_id=worker_id,
            job_types=(job_type,),
            job_ids=scoped_job_ids,
        )
        task = asyncio.create_task(worker.run(install_signal_handlers=False))
        deadline = asyncio.get_running_loop().time() + timeout_seconds
        try:
            while asyncio.get_running_loop().time() < deadline:
                async with self.database.connect() as connection:
                    active = await self.repository.active_job_count(
                        connection,
                        job_ids=scoped_job_ids,
                    )
                if active == 0:
                    return
                await asyncio.sleep(min(1.0, self.config.worker_poll_interval_seconds))
            raise TimeoutError(f"timed out draining {job_type}")
        finally:
            worker.request_stop()
            await task

    async def _handle_search_job(self, claim: JobClaim) -> None:
        topic_id, refresh_key = _parse_search_key(claim.idempotency_key)
        async with self.database.connect() as connection:
            topic = await self.repository.get_topic(connection, topic_id)
        if topic is None:
            raise TelegramChatDiscoveryError("search topic disappeared")
        await self.run_search(topic, refresh_key=refresh_key)

    async def _handle_screen_job(self, claim: JobClaim) -> None:
        peer_id = _parse_screen_key(claim.idempotency_key)
        try:
            await self.screen_peer(peer_id)
        except asyncio.CancelledError:
            async with self.database.transaction() as connection:
                await self.repository.release_screen_claim(
                    connection,
                    peer_id=peer_id,
                )
            raise

    async def _persist_candidate(
        self,
        connection: Any,
        *,
        peer: ChatDiscoveryPeerRecord,
        provider: str,
        policy_version: str,
    ) -> int:
        if provider == TELEGRAM_CHAT_DISCOVERY_PROVIDER and peer.access_type == "private":
            raise TelegramChatDiscoveryError("private_source_not_global")
        external_id = (
            f"username:{peer.username.removeprefix('@').casefold()}"
            if peer.username
            else peer.canonical_peer_identity
        )
        source = await self.sources.get_by_identity(
            connection,
            platform="telegram",
            external_id=external_id,
        )
        if source is None:
            try:
                source = await self.sources.create_candidate(
                    connection,
                    platform="telegram",
                    external_id=external_id,
                    access_type=peer.access_type,
                    display_name=peer.display_name,
                    provider=provider,
                    lineage_key=f"screen:{peer.canonical_peer_identity}",
                    handle=(None if peer.username is None else f"@{peer.username.removeprefix('@')}"),
                    canonical_url=peer.canonical_url,
                    provider_run_id=policy_version,
                    context={
                        "canonical_peer_identity": peer.canonical_peer_identity,
                        "screen_policy_version": policy_version,
                        "screen_status": "WATCH",
                    },
                )
            except SourceIdentityConflict:
                source = await self.sources.get_by_identity(
                    connection,
                    platform="telegram",
                    external_id=external_id,
                )
                if source is None:
                    raise
        return source.id

    def _provider_from_config(self, config: RuntimeConfig) -> TelegramChatScreenProvider | None:
        try:
            return telegram_chat_screen_provider_from_config(config)
        except SourceAIProviderUnavailable:
            return None
        except UnsupportedSourceAIProvider as exc:
            raise TelegramChatScreenError(str(exc)) from exc


class TelegramChatDiscoveryRuntime:
    """Long-running collector-only scheduler for chat discovery jobs."""

    def __init__(self, service: TelegramChatDiscoveryService, *, logger: logging.Logger | None = None) -> None:
        self.service = service
        self.logger = logger or LOGGER
        self._stop = asyncio.Event()

    def request_stop(self) -> None:
        self._stop.set()

    async def run(self) -> None:
        worker = self.service.worker(
            worker_id=f"telegram-chat-discovery-{self.service.collector_account_id}"
        )
        worker_task = asyncio.create_task(worker.run(install_signal_handlers=False))
        try:
            while not self._stop.is_set():
                await self.service.schedule_due_searches(
                    max_topics=self.service.config.telegram_chat_discovery_max_topics_per_cycle
                )
                try:
                    await asyncio.wait_for(
                        self._stop.wait(),
                        timeout=float(self.service.config.telegram_chat_discovery_refresh_interval_seconds),
                    )
                except TimeoutError:
                    pass
        finally:
            worker.request_stop()
            await worker_task


async def ensure_profile_derived_topics(
    connection: Any,
    intent: Any,
    *,
    refresh_interval_seconds: int = 21_600,
    use_buyer_intent_queries: bool = False,
) -> tuple[ChatDiscoveryTopicRecord, ...]:
    """Project profile concepts into the global topic pool.

    Chat Discovery mode opts into the existing deterministic buyer-intent query
    builder. Legacy profile discovery keeps its historical raw projection.
    Neither path makes a per-profile AI call or creates a user-specific source
    list.
    """
    if use_buyer_intent_queries:
        topic_specs = tuple(
            (query.text, query.language, query.priority)
            for query in build_telegram_profile_search_queries(intent, max_queries=20)
        )
    else:
        concepts: list[str] = []
        for field in ("roles", "services", "skills", "industries"):
            values = getattr(intent, field, ()) or ()
            concepts.extend(
                str(value).strip()
                for value in values
                if str(value).strip()
            )
        deduplicated: list[str] = []
        seen: set[str] = set()
        for value in concepts:
            normalized, _ = normalize_topic(value, "en")
            if normalized in seen:
                continue
            seen.add(normalized)
            deduplicated.append(value)
        languages = tuple(getattr(intent, "languages", ()) or ()) or ("en", "ru")
        topic_specs = tuple(
            (value, language, 60)
            for value in deduplicated[:24]
            for language in languages[:2]
        )

    repository = TelegramChatDiscoveryRepository()
    records: list[ChatDiscoveryTopicRecord] = []
    for topic_text, language, priority in topic_specs:
        records.append(
            await repository.ensure_topic(
                connection,
                topic_text=topic_text,
                language=language,
                topic_kind="profile",
                origin_key=f"profile-intent:{getattr(intent, 'id', 'unknown')}",
                priority=priority,
                refresh_interval_seconds=refresh_interval_seconds,
            )
        )
    return tuple(records)


@dataclass(frozen=True)
class _PeerDescriptor:
    canonical_peer_identity: str
    peer_type: str
    telegram_peer_id: int
    telegram_access_hash: int | None
    display_name: str
    username: str | None
    canonical_url: str | None
    access_type: str


def _peer_descriptor(entity: Any) -> _PeerDescriptor | None:
    if isinstance(entity, telethon_types.Channel):
        peer_type = "broadcast" if bool(getattr(entity, "broadcast", False)) else (
            "supergroup" if bool(getattr(entity, "megagroup", False)) else "channel"
        )
    elif isinstance(entity, telethon_types.Chat):
        peer_type = "group"
    else:
        return None
    try:
        peer_id = int(telethon_utils.get_peer_id(entity))
    except (TypeError, ValueError):
        raw_id = getattr(entity, "id", None)
        if not isinstance(raw_id, int):
            return None
        peer_id = raw_id
    username = getattr(entity, "username", None)
    username_value = None if not isinstance(username, str) or not username.strip() else username.strip().casefold()
    display_name = getattr(entity, "title", None)
    if not isinstance(display_name, str) or not display_name.strip():
        display_name = f"Telegram {peer_type}"
    return _PeerDescriptor(
        canonical_peer_identity=f"peer:{peer_id}",
        peer_type=peer_type,
        telegram_peer_id=int(getattr(entity, "id", abs(peer_id)) or abs(peer_id)),
        telegram_access_hash=(
            None
            if getattr(entity, "access_hash", None) is None
            else int(entity.access_hash)
        ),
        display_name=display_name.strip()[:255],
        username=username_value,
        canonical_url=(None if username_value is None else f"https://t.me/{username_value}"),
        access_type="public" if username_value else "private",
    )


def references_with_kinds(descriptor: _PeerDescriptor) -> tuple[tuple[str, str], ...]:
    values: list[tuple[str, str]] = [(descriptor.canonical_peer_identity, "peer")]
    if descriptor.username:
        values.extend(
            (
                (f"username:{descriptor.username}", "username"),
                (f"@{descriptor.username}", "username"),
            )
        )
    if descriptor.canonical_url:
        values.append((descriptor.canonical_url, "canonical_url"))
    return tuple(values)


def _peer_references(descriptor: _PeerDescriptor) -> tuple[str, ...]:
    return tuple(value for value, _kind in references_with_kinds(descriptor))


def _message_hits_by_peer(messages: Sequence[Any]) -> Mapping[str, int]:
    counts: defaultdict[str, int] = defaultdict(int)
    for message in messages:
        entity = getattr(message, "chat", None)
        if entity is None:
            entity = getattr(message, "peer_id", None)
        try:
            identity = f"peer:{int(telethon_utils.get_peer_id(entity))}"
        except (TypeError, ValueError):
            continue
        counts[identity] += 1
    return counts


_SCREEN_MESSAGE_USEFUL_FLOOR = 200


def _screen_messages(
    messages: Any,
    *,
    history_limit: int = 25,
    max_message_chars: int = 1000,
    max_total_chars: int = 20_000,
    min_sample: int = 10,
) -> tuple[ScreenMessage, ...]:
    if history_limit < 1:
        return ()
    if max_message_chars < _SCREEN_MESSAGE_USEFUL_FLOOR:
        raise ValueError("max_message_chars is below the useful screening floor")
    if max_total_chars < 1:
        raise ValueError("max_total_chars must be positive")

    values: list[ScreenMessage] = []
    for message in tuple(messages or ())[:history_limit]:
        message_id = getattr(message, "id", None)
        if not isinstance(message_id, int) or message_id <= 0:
            continue
        occurred_at = getattr(message, "date", None)
        if not isinstance(occurred_at, datetime):
            occurred_at = datetime.now(timezone.utc)
        if occurred_at.tzinfo is None or occurred_at.utcoffset() is None:
            occurred_at = occurred_at.replace(tzinfo=timezone.utc)
        text = getattr(message, "message", None)
        if text is None:
            text = getattr(message, "text", "")
        values.append(ScreenMessage(message_id, occurred_at, str(text or "")))

    if not values:
        return ()

    minimum_retained = min(len(values), max(1, min_sample))
    if max_total_chars < minimum_retained * _SCREEN_MESSAGE_USEFUL_FLOOR:
        raise ValueError("max_total_chars cannot preserve the configured minimum sample")

    retained_count = len(values)
    while (
        retained_count > minimum_retained
        and max_total_chars // retained_count < _SCREEN_MESSAGE_USEFUL_FLOOR
    ):
        retained_count -= 1

    effective_per_message = min(
        max_message_chars,
        max_total_chars // retained_count,
    )
    if effective_per_message < _SCREEN_MESSAGE_USEFUL_FLOOR:
        raise ValueError("screening message budget is below the useful floor")

    return tuple(
        ScreenMessage(item.message_id, item.occurred_at, item.text[:effective_per_message])
        for item in values[:retained_count]
    )


def _input_entity_for_peer(peer: ChatDiscoveryPeerRecord) -> Any:
    raw_id = peer.telegram_peer_id
    if raw_id is None:
        raise TelegramChatDiscoveryError("Telegram peer metadata is incomplete")
    if peer.peer_type in {"channel", "broadcast", "supergroup"}:
        if peer.telegram_access_hash is None:
            if peer.username:
                return peer.username
            if peer.canonical_url:
                return peer.canonical_url
            raise TelegramChatDiscoveryError("Telegram channel access metadata is incomplete")
        return telethon_types.InputPeerChannel(
            channel_id=raw_id,
            access_hash=peer.telegram_access_hash,
        )
    return telethon_types.InputPeerChat(chat_id=raw_id)


def input_entity_for_peer(peer: ChatDiscoveryPeerRecord) -> Any:
    """Build the access-hash-aware Telethon input for persisted peer metadata."""

    return _input_entity_for_peer(peer)


def _dedup_bucket(source: Mapping[str, Any] | None) -> str:
    if source is None:
        return "GENUINELY_NEW"
    status = str(source["lifecycle_status"])
    if status in {SourceStatus.APPROVED.value, SourceStatus.ACTIVE.value, SourceStatus.DEGRADED.value}:
        return "ALREADY_APPROVED"
    if status == SourceStatus.CANDIDATE.value:
        return "ALREADY_CANDIDATE"
    if status == SourceStatus.REJECTED.value:
        return "ALREADY_REJECTED"
    if status in {SourceStatus.NEEDS_REVIEW.value, SourceStatus.REVIEW_REQUIRED.value}:
        return "ALREADY_NEEDS_REVIEW"
    return "ALREADY_NEEDS_REVIEW"


def _refresh_bucket(topic: ChatDiscoveryTopicRecord, *, now: datetime | None = None) -> str:
    current = now or datetime.now(timezone.utc)
    interval = max(300, topic.refresh_interval_seconds)
    return str(int(current.timestamp()) // interval)


def _parse_search_key(value: str) -> tuple[UUID, str]:
    match = re.fullmatch(r"topic:([0-9a-f-]{36}):refresh:(.+)", value)
    if match is None:
        raise TelegramChatDiscoveryError("invalid chat-discovery search idempotency key")
    return UUID(match.group(1)), match.group(2)


def _parse_screen_key(value: str) -> UUID:
    match = re.fullmatch(r"peer:([0-9a-f-]{36}):attempt:[0-9]+", value)
    if match is None:
        raise TelegramChatDiscoveryError("invalid chat-discovery screen idempotency key")
    return UUID(match.group(1))
