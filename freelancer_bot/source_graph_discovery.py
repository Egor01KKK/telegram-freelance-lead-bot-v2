from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
import hashlib
import re
from types import MappingProxyType
from typing import Any, Protocol, runtime_checkable
import urllib.parse

from telethon import utils as telethon_utils
from telethon.errors import FloodWaitError
from telethon.tl import types as telethon_types

from .discovery import DiscoveredSourceCandidate, DiscoveryRequest
from .persistence.database import Database
from .persistence.source_repository import PostgresSourceCatalog, SourceRecord
from .telegram_request_governor import (
    TelegramRequestCategory,
    TelegramRequestGovernor,
)


class GraphReferenceKind(str, Enum):
    LINK = "link"
    MENTION = "mention"
    FORWARD = "forward"
    INVITE = "invite"


@dataclass
class SourceGraphObservability:
    """Safe, body-free counters persisted with a graph discovery run."""

    messages_sampled: int = 0
    raw_references_extracted: int = 0
    references_after_local_validation: int = 0
    references_after_dedup: int = 0
    known_sources_removed: int = 0
    entity_resolve_attempts: int = 0
    entity_resolve_successes: int = 0
    entity_resolve_errors: int = 0
    candidate_sources_created: int = 0
    entity_resolve_error_categories: dict[str, int] | None = None
    reference_kinds_after_local_validation: dict[str, int] | None = None

    def __post_init__(self) -> None:
        if self.entity_resolve_error_categories is None:
            self.entity_resolve_error_categories = {}
        if self.reference_kinds_after_local_validation is None:
            self.reference_kinds_after_local_validation = {}

    def add(self, other: "SourceGraphObservability") -> None:
        for field in (
            "messages_sampled",
            "raw_references_extracted",
            "references_after_local_validation",
            "references_after_dedup",
            "known_sources_removed",
            "entity_resolve_attempts",
            "entity_resolve_successes",
            "entity_resolve_errors",
        ):
            setattr(self, field, getattr(self, field) + getattr(other, field))
        for name, count in (other.entity_resolve_error_categories or {}).items():
            self.entity_resolve_error_categories[name] = (
                self.entity_resolve_error_categories.get(name, 0) + count
            )
        for name, count in (other.reference_kinds_after_local_validation or {}).items():
            self.reference_kinds_after_local_validation[name] = (
                self.reference_kinds_after_local_validation.get(name, 0) + count
            )

    def to_payload(self) -> dict[str, Any]:
        return {
            "messages_sampled": self.messages_sampled,
            "raw_references_extracted": self.raw_references_extracted,
            "references_after_local_validation": self.references_after_local_validation,
            "references_after_dedup": self.references_after_dedup,
            "known_sources_removed": self.known_sources_removed,
            "entity_resolve_attempts": self.entity_resolve_attempts,
            "entity_resolve_successes": self.entity_resolve_successes,
            "entity_resolve_errors": self.entity_resolve_errors,
            "candidate_sources_created": self.candidate_sources_created,
            "entity_resolve_error_categories": dict(
                sorted(self.entity_resolve_error_categories.items())
            ),
            "reference_kinds_after_local_validation": dict(
                sorted(self.reference_kinds_after_local_validation.items())
            ),
        }


@dataclass(frozen=True)
class SourceGraphSeed:
    id: int
    platform: str
    external_id: str
    access_type: str
    display_name: str
    handle: str | None
    canonical_url: str | None

    def __post_init__(self) -> None:
        if self.id <= 0:
            raise ValueError("Source graph seed id must be positive")
        object.__setattr__(self, "platform", _identifier(self.platform, "platform"))
        object.__setattr__(
            self,
            "external_id",
            _bounded_text(self.external_id, "external_id", 255),
        )
        object.__setattr__(
            self,
            "access_type",
            _access_type(self.access_type),
        )
        object.__setattr__(
            self,
            "display_name",
            _bounded_text(self.display_name, "display_name", 500),
        )
        object.__setattr__(
            self,
            "handle",
            None if self.handle is None else _handle(self.handle),
        )
        object.__setattr__(
            self,
            "canonical_url",
            None
            if self.canonical_url is None
            else _bounded_text(self.canonical_url, "canonical_url", 4096),
        )


@dataclass(frozen=True)
class SourceGraphTarget:
    external_id: str
    access_type: str
    display_name: str
    handle: str | None = None
    canonical_url: str | None = None

    def __post_init__(self) -> None:
        access_type = _access_type(self.access_type)
        handle = None if self.handle is None else _handle(self.handle)
        external_id = _bounded_text(self.external_id, "external_id", 255)
        if access_type == "public" and handle is None:
            raise ValueError("Public graph targets require a Telegram handle")
        if access_type == "public" and external_id != f"username:{handle[1:]}":
            raise ValueError("Public graph target identity must match its handle")
        if access_type == "private" and not re.fullmatch(r"peer:-?\d+", external_id):
            raise ValueError("Private graph target identity must be a Telegram peer id")
        object.__setattr__(self, "external_id", external_id)
        object.__setattr__(self, "access_type", access_type)
        object.__setattr__(
            self,
            "display_name",
            _bounded_text(self.display_name, "display_name", 500),
        )
        object.__setattr__(self, "handle", handle)
        object.__setattr__(
            self,
            "canonical_url",
            None
            if self.canonical_url is None
            else _bounded_text(self.canonical_url, "canonical_url", 4096),
        )


@dataclass(frozen=True)
class SourceGraphObservation:
    target: SourceGraphTarget
    kind: GraphReferenceKind | str
    reference: str
    observed_at: datetime
    message_id: int | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.target, SourceGraphTarget):
            raise TypeError("target must be a SourceGraphTarget")
        object.__setattr__(self, "kind", GraphReferenceKind(self.kind))
        object.__setattr__(
            self,
            "reference",
            _bounded_text(self.reference, "reference", 4096),
        )
        _aware(self.observed_at, "observed_at")
        if self.message_id is not None and self.message_id <= 0:
            raise ValueError("message_id must be positive")


@runtime_checkable
class SourceGraphSeedResolver(Protocol):
    async def resolve(
        self,
        seed_source_ids: Sequence[int],
    ) -> Sequence[SourceGraphSeed]: ...


@runtime_checkable
class SourceGraphBackend(Protocol):
    async def scan(
        self,
        seed: SourceGraphSeed,
        *,
        message_limit: int,
    ) -> Sequence[SourceGraphObservation]: ...


class GraphSeedSelectionError(ValueError):
    pass


class PostgresSourceGraphSeedResolver:
    def __init__(
        self,
        database: Database,
        *,
        collector_account_id: int,
        catalog: PostgresSourceCatalog | None = None,
    ) -> None:
        if collector_account_id <= 0:
            raise ValueError("collector_account_id must be positive")
        self._catalog = catalog or PostgresSourceCatalog(database)
        self._collector_account_id = collector_account_id

    async def resolve(
        self,
        seed_source_ids: Sequence[int],
    ) -> Sequence[SourceGraphSeed]:
        requested_ids = tuple(dict.fromkeys(seed_source_ids))
        if not requested_ids:
            raise GraphSeedSelectionError(
                "Source graph discovery requires explicit seed source identifiers"
            )
        if any(source_id <= 0 for source_id in requested_ids):
            raise GraphSeedSelectionError("Seed source identifiers must be positive")

        accessible = await self._catalog.list_approved(
            collector_account_id=self._collector_account_id,
            platform="telegram",
        )
        by_id = {source.id: source for source in accessible}
        return tuple(
            _graph_seed(by_id[source_id])
            for source_id in requested_ids
            if source_id in by_id
        )

    async def list_known_source_identities(self) -> tuple[str, ...]:
        """Load persisted identities for local, pre-resolution filtering."""

        return await self._catalog.list_known_source_identities(platform="telegram")


class SourceGraphDiscoveryProvider:
    name = "telegram_source_graph"
    kind = "source_graph"

    def __init__(
        self,
        seed_resolver: SourceGraphSeedResolver,
        backend: SourceGraphBackend,
        *,
        message_limit_per_seed: int = 100,
        max_candidates: int = 100,
        max_observations: int = 1000,
    ) -> None:
        if not isinstance(seed_resolver, SourceGraphSeedResolver):
            raise TypeError("seed_resolver must implement SourceGraphSeedResolver")
        if not isinstance(backend, SourceGraphBackend):
            raise TypeError("backend must implement SourceGraphBackend")
        if not 1 <= message_limit_per_seed <= 1000:
            raise ValueError("message_limit_per_seed must be between 1 and 1000")
        if not 1 <= max_candidates <= 1000:
            raise ValueError("max_candidates must be between 1 and 1000")
        if not 1 <= max_observations <= 10_000:
            raise ValueError("max_observations must be between 1 and 10000")
        self._seed_resolver = seed_resolver
        self._backend = backend
        self._message_limit_per_seed = message_limit_per_seed
        self._max_candidates = max_candidates
        self._max_observations = max_observations
        self._observability = SourceGraphObservability()

    @property
    def observability(self) -> Mapping[str, Any]:
        return MappingProxyType(self._observability.to_payload())

    async def discover(
        self,
        request: DiscoveryRequest,
    ) -> Sequence[DiscoveredSourceCandidate]:
        self._observability = SourceGraphObservability()
        begin_run = getattr(self._backend, "begin_run", None)
        if callable(begin_run):
            begin_run()
        seeds = tuple(await self._seed_resolver.resolve(request.seed_source_ids))
        if not seeds:
            raise GraphSeedSelectionError(
                "No approved accessible source graph seeds were selected"
            )
        if any(not isinstance(seed, SourceGraphSeed) for seed in seeds):
            raise TypeError("Seed resolvers must return SourceGraphSeed values")
        if any(seed.platform != "telegram" for seed in seeds):
            raise GraphSeedSelectionError("Source graph seeds must be Telegram sources")

        accumulators: dict[str, _GraphCandidateAccumulator] = {}
        observation_count = 0
        for seed in seeds:
            observations = await self._backend.scan(
                seed,
                message_limit=self._message_limit_per_seed,
            )
            scan_observability = getattr(self._backend, "last_observability", None)
            if isinstance(scan_observability, SourceGraphObservability):
                self._observability.add(scan_observability)
            for observation in observations:
                if not isinstance(observation, SourceGraphObservation):
                    raise TypeError(
                        "Source graph backends must return SourceGraphObservation values"
                    )
                if _is_seed_target(seed, observation.target):
                    continue
                match = {
                    "seed_source_id": seed.id,
                    "seed_external_id": seed.external_id,
                    "seed_handle": seed.handle,
                    "reference_kind": observation.kind.value,
                    "reference": observation.reference,
                    "message_id": observation.message_id,
                    "observed_at": observation.observed_at.isoformat(),
                }
                target = observation.target
                existing = accumulators.get(target.external_id)
                if existing is None:
                    accumulators[target.external_id] = _GraphCandidateAccumulator(
                        target=target,
                        first_seed_id=seed.id,
                        first_reference=observation.reference,
                        observations=[match],
                    )
                else:
                    existing.observations.append(match)
                observation_count += 1
                if (
                    len(accumulators) >= self._max_candidates
                    or observation_count >= self._max_observations
                ):
                    break
            if (
                len(accumulators) >= self._max_candidates
                or observation_count >= self._max_observations
            ):
                break

        self._observability.candidate_sources_created = len(accumulators)
        return tuple(
            accumulator.to_candidate(request)
            for accumulator in accumulators.values()
        )


@dataclass
class _GraphCandidateAccumulator:
    target: SourceGraphTarget
    first_seed_id: int
    first_reference: str
    observations: list[dict[str, Any]]

    def to_candidate(self, request: DiscoveryRequest) -> DiscoveredSourceCandidate:
        digest = hashlib.sha256(self.target.external_id.encode("utf-8")).hexdigest()[:32]
        return DiscoveredSourceCandidate(
            result_key=f"graph:telegram:{digest}",
            platform="telegram",
            external_id=self.target.external_id,
            access_type=self.target.access_type,
            display_name=self.target.display_name,
            handle=self.target.handle,
            canonical_url=self.target.canonical_url,
            discovered_at=request.requested_at,
            seed_source_id=self.first_seed_id,
            seed_reference=self.first_reference,
            context=MappingProxyType(
                {
                    "discovery_method": "telegram_source_graph",
                    "observations": list(self.observations),
                    "source_graph_provenance": {
                        "seed_source_id": self.first_seed_id,
                        "observation_count": len(self.observations),
                    },
                }
            ),
        )


class TelethonSourceGraphBackend:
    def __init__(
        self,
        client: Any,
        *,
        governor: TelegramRequestGovernor | None = None,
        max_message_limit: int | None = None,
        known_source_identities: Sequence[str] = (),
        entity_resolution_budget: int = 5,
    ) -> None:
        if not hasattr(client, "get_entity") or not hasattr(client, "iter_messages"):
            raise TypeError("Telethon graph client must expose get_entity and iter_messages")
        if max_message_limit is not None and max_message_limit <= 0:
            raise ValueError("max_message_limit must be positive")
        if not 1 <= entity_resolution_budget <= 1000:
            raise ValueError("entity_resolution_budget must be between 1 and 1000")
        self._client = client
        self._governor = governor
        self._max_message_limit = max_message_limit
        self._known_source_keys = {
            key
            for value in known_source_identities
            if (key := _known_source_key(value)) is not None
        }
        self._entity_resolution_budget = entity_resolution_budget
        self._resolution_cache: dict[str, Any | None] = {}
        self._resolves_used = 0
        self._run_observability = SourceGraphObservability()
        self._aggregate_observability = SourceGraphObservability()

    @property
    def last_observability(self) -> SourceGraphObservability:
        return self._run_observability

    @property
    def run_observability(self) -> SourceGraphObservability:
        return self._aggregate_observability

    def begin_run(self) -> None:
        """Reset per-discovery-run caches and counters.

        The backend instance is normally scoped to one run.  Resetting here
        also makes repeated calls in tests and long-lived runtimes explicit.
        Successful resolutions are reused across seeds in the same run;
        persisted source identities are reused across runs by the known-source
        filter.
        """

        self._resolution_cache = {}
        self._resolves_used = 0
        self._run_observability = SourceGraphObservability()
        self._aggregate_observability = SourceGraphObservability()

    async def scan(
        self,
        seed: SourceGraphSeed,
        *,
        message_limit: int,
    ) -> Sequence[SourceGraphObservation]:
        if not 1 <= message_limit <= 1000:
            raise ValueError("message_limit must be between 1 and 1000")
        if self._max_message_limit is not None:
            message_limit = min(message_limit, self._max_message_limit)
        self._run_observability = SourceGraphObservability()
        lookup = seed.handle or seed.canonical_url or seed.external_id
        entity = await self._request(
            TelegramRequestCategory.ENTITY_ACCESS,
            lambda: self._client.get_entity(lookup),
        )
        pending_references: list[tuple[int | None, datetime, _TelegramReference]] = []
        messages_sampled = 0

        async def read_history() -> None:
            nonlocal messages_sampled
            async for message in self._client.iter_messages(
                entity,
                limit=message_limit,
            ):
                messages_sampled += 1
                message_id = _positive_message_id(getattr(message, "id", None))
                observed_at = _message_date(getattr(message, "date", None))
                for raw_reference in _message_references(message):
                    pending_references.append((message_id, observed_at, raw_reference))

        await self._request(TelegramRequestCategory.GRAPH_HISTORY, read_history)
        local_references: dict[str, _NormalizedTelegramReference] = {}
        for message_id, observed_at, raw_reference in pending_references:
            normalized = _normalize_reference(raw_reference)
            if normalized is None:
                continue
            self._run_observability.references_after_local_validation += 1
            kind = raw_reference.kind.value
            self._run_observability.reference_kinds_after_local_validation[kind] = (
                self._run_observability.reference_kinds_after_local_validation.get(kind, 0)
                + 1
            )
            existing = local_references.get(normalized.key)
            if existing is None:
                normalized.occurrences.append((message_id, observed_at))
                local_references[normalized.key] = normalized
            else:
                existing.occurrences.append((message_id, observed_at))

        self._run_observability.messages_sampled += messages_sampled
        self._run_observability.raw_references_extracted += len(pending_references)
        self._run_observability.references_after_dedup += len(local_references)

        known_removed = set(local_references).intersection(self._known_source_keys)
        self._run_observability.known_sources_removed += len(known_removed)
        for key in known_removed:
            local_references.pop(key, None)

        observations: list[SourceGraphObservation] = []
        seen: set[tuple[int | None, str, str, str]] = set()
        prioritized = sorted(
            local_references.values(),
            key=lambda reference: (
                _reference_priority(reference.raw),
                reference.key,
            ),
        )
        for normalized in prioritized:
            raw_reference = normalized.raw
            target_entity = raw_reference.entity
            if target_entity is None:
                if normalized.key in self._resolution_cache:
                    target_entity = self._resolution_cache[normalized.key]
                elif self._resolves_used >= self._entity_resolution_budget:
                    continue
                else:
                    self._resolves_used += 1
                    self._run_observability.entity_resolve_attempts += 1
                    try:
                        target_entity = await self._request(
                            TelegramRequestCategory.ENTITY_ACCESS,
                            lambda raw_reference=raw_reference: self._client.get_entity(
                                raw_reference.lookup
                            ),
                        )
                    except FloodWaitError:
                        raise
                    except Exception as exc:
                        self._resolution_cache[normalized.key] = None
                        self._run_observability.entity_resolve_errors += 1
                        category = _resolution_error_category(exc)
                        self._run_observability.entity_resolve_error_categories[category] = (
                            self._run_observability.entity_resolve_error_categories.get(
                                category,
                                0,
                            )
                            + 1
                        )
                        continue
                    else:
                        self._resolution_cache[normalized.key] = target_entity
                        self._run_observability.entity_resolve_successes += 1
            else:
                self._resolution_cache.setdefault(normalized.key, target_entity)

            target = _target_from_entity(target_entity)
            if target is None:
                continue
            safe_reference = _safe_reference(
                raw_reference.kind,
                raw_reference.reference,
                target.external_id,
            )
            for message_id, observed_at in normalized.occurrences:
                key = (
                    message_id,
                    raw_reference.kind.value,
                    safe_reference,
                    target.external_id,
                )
                if key in seen:
                    continue
                seen.add(key)
                observations.append(
                    SourceGraphObservation(
                        target=target,
                        kind=raw_reference.kind,
                        reference=safe_reference,
                        observed_at=observed_at,
                        message_id=message_id,
                    )
                )
        self._aggregate_observability.add(self._run_observability)
        return tuple(observations)

    async def _request(self, category: str, operation):
        if self._governor is None:
            return await operation()
        return await self._governor.run(category, operation)


_URL_PATTERN = re.compile(
    r"(?:(?:https?://)?(?:www\.)?(?:t\.me|telegram\.me|telegram\.dog)/"
    r"(?:s/)?(?:\+[A-Za-z0-9_-]+|joinchat/[A-Za-z0-9_-]+|"
    r"[A-Za-z][A-Za-z0-9_]{4,31})(?:/\d+)?)",
    re.IGNORECASE,
)
_MENTION_PATTERN = re.compile(r"(?<![A-Za-z0-9_])@[A-Za-z][A-Za-z0-9_]{4,31}")
_HANDLE_PATTERN = re.compile(r"^[A-Za-z][A-Za-z0-9_]{4,31}$")


@dataclass(frozen=True)
class _TelegramReference:
    kind: GraphReferenceKind
    reference: str
    lookup: Any
    entity: Any | None = None


@dataclass
class _NormalizedTelegramReference:
    raw: _TelegramReference
    key: str
    occurrences: list[tuple[int | None, datetime]]


def _message_references(
    message: Any,
) -> tuple[_TelegramReference, ...]:
    values: list[_TelegramReference] = []
    text = getattr(message, "message", None) or getattr(message, "text", None) or ""
    if isinstance(text, str):
        for match in _URL_PATTERN.finditer(text):
            reference = match.group(0)
            values.append(
                _TelegramReference(
                    _reference_kind(reference),
                    reference,
                    _telegram_lookup(reference),
                )
            )
        for match in _MENTION_PATTERN.finditer(text):
            reference = match.group(0)
            values.append(
                _TelegramReference(
                    GraphReferenceKind.MENTION,
                    reference,
                    reference,
                )
            )

    entities = getattr(message, "entities", None) or ()
    for entity in entities:
        reference = getattr(entity, "url", None)
        if isinstance(reference, str) and reference.strip():
            reference = reference.strip()
            values.append(
                _TelegramReference(
                    _reference_kind(reference),
                    reference,
                    _telegram_lookup(reference),
                )
            )

    forward = getattr(message, "forward", None)
    if forward is not None:
        forward_entity = getattr(forward, "chat", None)
        if forward_entity is not None:
            values.append(
                _TelegramReference(
                    GraphReferenceKind.FORWARD,
                    "forward",
                    forward_entity,
                    forward_entity,
                )
            )
        else:
            peer = getattr(forward, "from_id", None)
            if peer is not None:
                values.append(
                    _TelegramReference(
                        GraphReferenceKind.FORWARD,
                        repr(peer),
                        peer,
                    )
                )
    elif getattr(message, "fwd_from", None) is not None:
        peer = getattr(message.fwd_from, "from_id", None)
        if peer is not None:
            values.append(
                _TelegramReference(
                    GraphReferenceKind.FORWARD,
                    repr(peer),
                    peer,
                )
            )
    return tuple(values)


def _target_from_entity(entity: Any) -> SourceGraphTarget | None:
    if not _is_community_entity(entity):
        return None
    username = _entity_username(entity)
    title = getattr(entity, "title", None)
    if not isinstance(title, str) or not title.strip():
        return None
    if username is not None:
        normalized = username.lower()
        return SourceGraphTarget(
            external_id=f"username:{normalized}",
            access_type="public",
            display_name=title,
            handle=f"@{normalized}",
            canonical_url=f"https://t.me/{normalized}",
        )
    try:
        peer_id = int(telethon_utils.get_peer_id(entity))
    except (TypeError, ValueError):
        raw_id = getattr(entity, "id", None)
        if not isinstance(raw_id, int):
            return None
        peer_id = raw_id
    return SourceGraphTarget(
        external_id=f"peer:{peer_id}",
        access_type="private",
        display_name=title,
    )


def _is_community_entity(entity: Any) -> bool:
    if bool(getattr(entity, "bot", False)):
        return False
    return isinstance(
        entity,
        (
            telethon_types.Channel,
            telethon_types.Chat,
        ),
    ) or bool(
        getattr(entity, "broadcast", False)
        or getattr(entity, "megagroup", False)
        or getattr(entity, "_graph_community", False)
    )


def _entity_username(entity: Any) -> str | None:
    username = getattr(entity, "username", None)
    if isinstance(username, str) and _HANDLE_PATTERN.fullmatch(username):
        return username
    for item in getattr(entity, "usernames", None) or ():
        value = getattr(item, "username", None)
        if (
            getattr(item, "active", True)
            and isinstance(value, str)
            and _HANDLE_PATTERN.fullmatch(value)
        ):
            return value
    return None


def _reference_kind(reference: str) -> GraphReferenceKind:
    parsed = urllib.parse.urlsplit(
        reference if "://" in reference else f"https://{reference}"
    )
    parts = [part for part in parsed.path.split("/") if part]
    if parts and (parts[0].startswith("+") or parts[0].lower() == "joinchat"):
        return GraphReferenceKind.INVITE
    return GraphReferenceKind.LINK


def _telegram_lookup(reference: str) -> str:
    parsed = urllib.parse.urlsplit(
        reference if "://" in reference else f"https://{reference}"
    )
    parts = [part for part in parsed.path.split("/") if part]
    if parts and parts[0].lower() == "s":
        parts = parts[1:]
    if parts and _HANDLE_PATTERN.fullmatch(parts[0]):
        return f"@{parts[0]}"
    return reference


def _normalize_reference(
    reference: _TelegramReference,
) -> _NormalizedTelegramReference | None:
    """Normalize and structurally validate a reference without Telegram I/O."""

    if reference.kind is GraphReferenceKind.MENTION:
        # A bare @name is usually an author/contact/bot mention.  Its entity
        # type cannot be known structurally, so it is never sent to Telegram
        # as a graph candidate.  Channel/group links and forward metadata are
        # the safe source signals.
        return None

    if reference.kind is GraphReferenceKind.FORWARD:
        if reference.entity is not None:
            target = _target_from_entity(reference.entity)
            if target is None:
                return None
            key = target.external_id
        else:
            key = _peer_reference_key(reference.lookup)
            if key is None:
                return None
        return _NormalizedTelegramReference(reference, key, [])

    parsed = _telegram_reference_parts(reference.reference)
    if parsed is None:
        return None
    kind, value = parsed
    if kind is GraphReferenceKind.INVITE:
        key = f"invite:sha256:{hashlib.sha256(value.encode('utf-8')).hexdigest()[:16]}"
    else:
        key = f"username:{value.lower()}"
    return _NormalizedTelegramReference(reference, key, [])


def _telegram_reference_parts(
    reference: str,
) -> tuple[GraphReferenceKind, str] | None:
    parsed = urllib.parse.urlsplit(
        reference if "://" in reference else f"https://{reference}"
    )
    host = (parsed.hostname or "").lower().rstrip(".")
    if host not in {"t.me", "telegram.me", "telegram.dog"}:
        return None
    parts = [part for part in parsed.path.split("/") if part]
    if parts and parts[0].lower() == "s":
        parts = parts[1:]
    if not parts:
        return None
    first = parts[0]
    if first.startswith("+") or first.lower() == "joinchat":
        if first.startswith("+") and len(first) > 1:
            return GraphReferenceKind.INVITE, first
        if first.lower() == "joinchat" and len(parts) > 1 and parts[1]:
            return GraphReferenceKind.INVITE, f"joinchat/{parts[1]}"
        return None
    if not _HANDLE_PATTERN.fullmatch(first):
        return None
    if len(parts) > 1 and not all(part.isdigit() for part in parts[1:]):
        return None
    return GraphReferenceKind.LINK, first.lower()


def _reference_priority(reference: _TelegramReference) -> int:
    if reference.kind is GraphReferenceKind.FORWARD and reference.entity is not None:
        return 0
    if reference.kind is GraphReferenceKind.LINK:
        return 1
    if reference.kind is GraphReferenceKind.INVITE:
        return 2
    return 3


def _known_source_key(value: str) -> str | None:
    normalized = value.strip()
    if not normalized:
        return None
    if normalized.lower().startswith("username:"):
        username = normalized.split(":", 1)[1].removeprefix("@").lower()
        return f"username:{username}" if _HANDLE_PATTERN.fullmatch(username) else None
    if re.fullmatch(r"peer:-?\d+", normalized.lower()):
        return normalized.lower()
    if normalized.startswith("@") and _HANDLE_PATTERN.fullmatch(normalized[1:]):
        return f"username:{normalized[1:].lower()}"
    parsed = _telegram_reference_parts(normalized)
    if parsed is None:
        return None
    kind, reference = parsed
    if kind is GraphReferenceKind.LINK:
        return f"username:{reference.lower()}"
    return None


def _peer_reference_key(peer: Any) -> str | None:
    if isinstance(peer, telethon_types.PeerChannel):
        return f"peer:{telethon_utils.get_peer_id(peer)}"
    if isinstance(peer, telethon_types.PeerChat):
        return f"peer:{telethon_utils.get_peer_id(peer)}"
    peer_type = type(peer).__name__.lower()
    if peer_type not in {"peerchannel", "peerchat"}:
        return None
    try:
        return f"peer:{telethon_utils.get_peer_id(peer)}"
    except (TypeError, ValueError):
        return None


def _resolution_error_category(error: Exception) -> str:
    name = error.__class__.__name__.lower()
    if "username" in name and ("invalid" in name or "occupied" in name or "not" in name):
        return "invalid_username"
    if any(token in name for token in ("notfound", "not_found", "peeridinvalid", "channelinvalid")):
        return "entity_not_found"
    if any(token in name for token in ("private", "inaccessible", "access", "forbidden")):
        return "private_inaccessible"
    if "timeout" in name:
        return "timeout"
    if "unsupported" in name or "typeerror" in name:
        return "unsupported_reference"
    return "other"


def _safe_reference(
    kind: GraphReferenceKind,
    reference: str,
    external_id: str,
) -> str:
    if kind is GraphReferenceKind.INVITE:
        digest = hashlib.sha256(reference.encode("utf-8")).hexdigest()[:16]
        return f"invite:sha256:{digest}"
    if kind is GraphReferenceKind.FORWARD:
        return f"forward:{external_id}"
    return reference


def _graph_seed(source: SourceRecord) -> SourceGraphSeed:
    return SourceGraphSeed(
        id=source.id,
        platform=source.platform,
        external_id=source.external_id,
        access_type=source.access_type,
        display_name=source.display_name,
        handle=source.handle,
        canonical_url=source.canonical_url,
    )


def _is_seed_target(seed: SourceGraphSeed, target: SourceGraphTarget) -> bool:
    return seed.external_id == target.external_id or (
        seed.handle is not None
        and target.handle is not None
        and seed.handle.lower() == target.handle.lower()
    )


def _message_date(value: Any) -> datetime:
    if not isinstance(value, datetime):
        raise ValueError("Telegram graph messages require a datetime")
    if value.tzinfo is None or value.utcoffset() is None:
        return value.replace(tzinfo=timezone.utc)
    return value


def _positive_message_id(value: Any) -> int | None:
    if value is None:
        return None
    identifier = int(value)
    if identifier <= 0:
        raise ValueError("Telegram graph message id must be positive")
    return identifier


def _handle(value: str) -> str:
    normalized = _bounded_text(value, "handle", 33).lower()
    username = normalized.removeprefix("@")
    if not _HANDLE_PATTERN.fullmatch(username):
        raise ValueError("handle must be a valid public Telegram username")
    return f"@{username}"


def _access_type(value: str) -> str:
    normalized = value.strip().lower()
    if normalized not in {"public", "private"}:
        raise ValueError("access_type must be public or private")
    return normalized


def _identifier(value: str, field: str) -> str:
    normalized = value.strip().lower()
    if not re.fullmatch(r"[a-z][a-z0-9_-]{0,31}", normalized):
        raise ValueError(f"{field} must be a normalized identifier")
    return normalized


def _bounded_text(value: str, field: str, max_length: int) -> str:
    normalized = value.strip()
    if not normalized:
        raise ValueError(f"{field} must not be blank")
    if len(normalized) > max_length:
        raise ValueError(f"{field} must not exceed {max_length} characters")
    return normalized


def _aware(value: datetime, field: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field} must include a timezone")
    return value
