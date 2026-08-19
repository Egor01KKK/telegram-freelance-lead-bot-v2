from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import Enum
from types import MappingProxyType
from typing import Any, Protocol, runtime_checkable

from .telegram_request_governor import (
    TelegramRequestCategory,
    TelegramRequestGovernor,
)


@dataclass(frozen=True)
class SourceAuditTarget:
    source_id: int
    platform: str
    lookup: Any

    def __post_init__(self) -> None:
        if self.source_id <= 0:
            raise ValueError("source_id must be positive")
        platform = self.platform.strip().lower()
        if not platform:
            raise ValueError("platform must not be blank")
        if self.lookup is None or (isinstance(self.lookup, str) and not self.lookup.strip()):
            raise ValueError("lookup must identify a source")
        object.__setattr__(self, "platform", platform)


@dataclass(frozen=True)
class SourceAuditMessage:
    message_id: int
    occurred_at: datetime
    text: str
    metadata: Mapping[str, Any] | None = None

    def __post_init__(self) -> None:
        if self.message_id <= 0:
            raise ValueError("message_id must be positive")
        _aware(self.occurred_at, "occurred_at")
        if not isinstance(self.text, str):
            raise TypeError("text must be a string")
        metadata = dict(self.metadata or {})
        object.__setattr__(self, "metadata", MappingProxyType(metadata))


@runtime_checkable
class SourceAuditHistoryReader(Protocol):
    async def fetch_window(
        self,
        target: SourceAuditTarget,
        *,
        window_started_at: datetime,
        window_ended_at: datetime,
        limit: int,
    ) -> Sequence[SourceAuditMessage]: ...


class AuditFetchPurpose(str, Enum):
    INITIAL_PROBE = "initial_probe"
    EXPANDED_PROBE = "expanded_probe"
    DISTRIBUTED_BUCKET = "distributed_bucket"


@dataclass(frozen=True)
class AuditFetchRecord:
    purpose: AuditFetchPurpose
    window_started_at: datetime
    window_ended_at: datetime
    limit: int
    returned_count: int


@dataclass(frozen=True)
class SourceAuditPolicy:
    initial_window_days: int = 3
    expanded_window_days: int = 14
    minimum_evidence_messages: int = 30
    sample_size: int = 150
    distribution_buckets: int = 10

    def __post_init__(self) -> None:
        if not 2 <= self.initial_window_days <= 4:
            raise ValueError("initial_window_days must be between 2 and 4")
        if not 7 <= self.expanded_window_days <= 14:
            raise ValueError("expanded_window_days must be between 7 and 14")
        if self.expanded_window_days <= self.initial_window_days:
            raise ValueError("expanded window must be longer than the initial window")
        if not 20 <= self.sample_size <= 200:
            raise ValueError("sample_size must be between 20 and 200")
        if not 1 <= self.minimum_evidence_messages <= self.sample_size:
            raise ValueError(
                "minimum_evidence_messages must be positive and no greater than sample_size"
            )
        if not 2 <= self.distribution_buckets <= self.sample_size:
            raise ValueError(
                "distribution_buckets must be between 2 and sample_size"
            )


@dataclass(frozen=True)
class SourceAuditSample:
    source_id: int
    audited_at: datetime
    initial_window_started_at: datetime
    window_started_at: datetime
    window_ended_at: datetime
    sampled_from: datetime | None
    sampled_to: datetime | None
    expanded: bool
    high_volume: bool
    probe_message_count: int
    messages: tuple[SourceAuditMessage, ...]
    fetches: tuple[AuditFetchRecord, ...]

    @property
    def sampled_message_count(self) -> int:
        return len(self.messages)


class SourceAuditSampler:
    def __init__(
        self,
        reader: SourceAuditHistoryReader,
        *,
        policy: SourceAuditPolicy | None = None,
    ) -> None:
        if not isinstance(reader, SourceAuditHistoryReader):
            raise TypeError("reader must implement SourceAuditHistoryReader")
        self._reader = reader
        self._policy = policy or SourceAuditPolicy()

    async def sample(
        self,
        target: SourceAuditTarget,
        *,
        audited_at: datetime,
    ) -> SourceAuditSample:
        audited_at = _aware(audited_at, "audited_at")
        initial_start = audited_at - timedelta(
            days=self._policy.initial_window_days
        )
        probe_limit = self._policy.sample_size + 1
        fetches: list[AuditFetchRecord] = []
        initial_probe = await self._fetch(
            target,
            window_started_at=initial_start,
            window_ended_at=audited_at,
            limit=probe_limit,
            purpose=AuditFetchPurpose.INITIAL_PROBE,
            fetches=fetches,
        )
        initial_probe = _unique_sorted(initial_probe)

        expanded = len(initial_probe) < self._policy.minimum_evidence_messages
        selected_start = initial_start
        selected_probe = initial_probe
        if expanded:
            selected_start = audited_at - timedelta(
                days=self._policy.expanded_window_days
            )
            selected_probe = await self._fetch(
                target,
                window_started_at=selected_start,
                window_ended_at=audited_at,
                limit=probe_limit,
                purpose=AuditFetchPurpose.EXPANDED_PROBE,
                fetches=fetches,
            )
            selected_probe = _unique_sorted(selected_probe)

        high_volume = len(selected_probe) > self._policy.sample_size
        if high_volume:
            messages = await self._distributed_sample(
                target,
                window_started_at=selected_start,
                window_ended_at=audited_at,
                probe=selected_probe,
                fetches=fetches,
            )
        else:
            messages = _unique_sorted(selected_probe)

        if len(messages) > self._policy.sample_size:
            raise RuntimeError("Source audit sample exceeded the configured bound")
        sampled_from = messages[0].occurred_at if messages else None
        sampled_to = messages[-1].occurred_at if messages else None
        return SourceAuditSample(
            source_id=target.source_id,
            audited_at=audited_at,
            initial_window_started_at=initial_start,
            window_started_at=selected_start,
            window_ended_at=audited_at,
            sampled_from=sampled_from,
            sampled_to=sampled_to,
            expanded=expanded,
            high_volume=high_volume,
            probe_message_count=len(selected_probe),
            messages=messages,
            fetches=tuple(fetches),
        )

    async def _distributed_sample(
        self,
        target: SourceAuditTarget,
        *,
        window_started_at: datetime,
        window_ended_at: datetime,
        probe: Sequence[SourceAuditMessage],
        fetches: list[AuditFetchRecord],
    ) -> tuple[SourceAuditMessage, ...]:
        duration = window_ended_at - window_started_at
        base_quota, remainder = divmod(
            self._policy.sample_size,
            self._policy.distribution_buckets,
        )
        selected: list[SourceAuditMessage] = []
        for index in range(self._policy.distribution_buckets):
            bucket_start = window_started_at + (
                duration * index / self._policy.distribution_buckets
            )
            bucket_end = window_started_at + (
                duration * (index + 1) / self._policy.distribution_buckets
            )
            quota = base_quota + int(index < remainder)
            selected.extend(
                await self._fetch(
                    target,
                    window_started_at=bucket_start,
                    window_ended_at=bucket_end,
                    limit=quota,
                    purpose=AuditFetchPurpose.DISTRIBUTED_BUCKET,
                    fetches=fetches,
                )
            )

        unique = list(_unique_sorted(selected))
        if len(unique) < self._policy.sample_size:
            known_ids = {message.message_id for message in unique}
            for message in _unique_sorted(probe):
                if message.message_id in known_ids:
                    continue
                unique.append(message)
                known_ids.add(message.message_id)
                if len(unique) >= self._policy.sample_size:
                    break
        return tuple(
            sorted(
                unique[: self._policy.sample_size],
                key=lambda message: (message.occurred_at, message.message_id),
            )
        )

    async def _fetch(
        self,
        target: SourceAuditTarget,
        *,
        window_started_at: datetime,
        window_ended_at: datetime,
        limit: int,
        purpose: AuditFetchPurpose,
        fetches: list[AuditFetchRecord],
    ) -> tuple[SourceAuditMessage, ...]:
        if limit <= 0:
            raise RuntimeError("Source audit fetch limits must be positive")
        values = tuple(
            await self._reader.fetch_window(
                target,
                window_started_at=window_started_at,
                window_ended_at=window_ended_at,
                limit=limit,
            )
        )
        if len(values) > limit:
            raise RuntimeError("Source audit reader exceeded the requested limit")
        if any(not isinstance(message, SourceAuditMessage) for message in values):
            raise TypeError("Source audit readers must return SourceAuditMessage values")
        if any(
            not window_started_at <= message.occurred_at <= window_ended_at
            for message in values
        ):
            raise RuntimeError("Source audit reader returned a message outside its window")
        fetches.append(
            AuditFetchRecord(
                purpose=purpose,
                window_started_at=window_started_at,
                window_ended_at=window_ended_at,
                limit=limit,
                returned_count=len(values),
            )
        )
        return values


class TelethonSourceAuditHistoryReader:
    def __init__(
        self,
        client: Any,
        *,
        governor: TelegramRequestGovernor | None = None,
        max_messages_per_pass: int | None = None,
    ) -> None:
        if not hasattr(client, "get_entity") or not hasattr(client, "iter_messages"):
            raise TypeError("Telethon audit client must expose get_entity and iter_messages")
        if max_messages_per_pass is not None and max_messages_per_pass <= 0:
            raise ValueError("max_messages_per_pass must be positive")
        self._client = client
        self._governor = governor
        self._max_messages_per_pass = max_messages_per_pass
        self._entities: dict[int, Any] = {}

    async def fetch_window(
        self,
        target: SourceAuditTarget,
        *,
        window_started_at: datetime,
        window_ended_at: datetime,
        limit: int,
    ) -> Sequence[SourceAuditMessage]:
        window_started_at = _aware(window_started_at, "window_started_at")
        window_ended_at = _aware(window_ended_at, "window_ended_at")
        if window_ended_at <= window_started_at:
            raise ValueError("Audit fetch window must end after it starts")
        if limit <= 0:
            raise ValueError("Audit fetch limit must be positive")
        if self._max_messages_per_pass is not None:
            limit = min(limit, self._max_messages_per_pass)
        entity = self._entities.get(target.source_id)
        if entity is None:
            entity = await self._request(
                TelegramRequestCategory.ENTITY_ACCESS,
                lambda: self._client.get_entity(target.lookup),
            )
            self._entities[target.source_id] = entity

        messages: list[SourceAuditMessage] = []
        async def read_history() -> None:
            async for message in self._client.iter_messages(
                entity,
                offset_date=window_ended_at,
                limit=limit,
            ):
                occurred_at = _message_date(getattr(message, "date", None))
                if not window_started_at <= occurred_at <= window_ended_at:
                    continue
                message_id = int(getattr(message, "id", 0))
                if message_id <= 0:
                    continue
                text = getattr(message, "message", None)
                if not isinstance(text, str):
                    text = getattr(message, "raw_text", None)
                messages.append(
                    SourceAuditMessage(
                        message_id=message_id,
                        occurred_at=occurred_at,
                        text=text if isinstance(text, str) else "",
                        metadata={
                            "has_media": getattr(message, "media", None) is not None,
                        },
                    )
                )

        await self._request(TelegramRequestCategory.AUDIT_HISTORY, read_history)
        return tuple(messages)

    async def _request(self, category: str, operation):
        if self._governor is None:
            return await operation()
        return await self._governor.run(category, operation)


def _unique_sorted(
    messages: Sequence[SourceAuditMessage],
) -> tuple[SourceAuditMessage, ...]:
    by_id: dict[int, SourceAuditMessage] = {}
    for message in messages:
        existing = by_id.get(message.message_id)
        if existing is not None and existing != message:
            raise RuntimeError(
                f"Message {message.message_id} changed within one source audit"
            )
        by_id[message.message_id] = message
    return tuple(
        sorted(
            by_id.values(),
            key=lambda message: (message.occurred_at, message.message_id),
        )
    )


def _message_date(value: Any) -> datetime:
    if not isinstance(value, datetime):
        raise ValueError("Telegram audit messages require a datetime")
    return _aware(value, "message date")


def _aware(value: datetime, field: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field} must include a timezone")
    return value
