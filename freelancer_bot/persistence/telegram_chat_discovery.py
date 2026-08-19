"""PostgreSQL state for the bounded Telegram chat-discovery loop.

This repository deliberately keeps discovery observations and screen evidence
separate from the normal ``sources`` table.  A WATCH result is materialized
through ``SourceRepository`` by the service; SKIP and UNCLEAR remain durable
screen evidence without becoming silently approved sources.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import re
import unicodedata
from typing import Any
from uuid import UUID, uuid4

import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncConnection

from .jobs import DurableJobRepository
from .schema import (
    durable_jobs,
    source_reference_aliases,
    sources,
    telegram_chat_discovery_observations,
    telegram_chat_discovery_peer_aliases,
    telegram_chat_discovery_peers,
    telegram_chat_discovery_screen_attempts,
    telegram_chat_discovery_search_runs,
    telegram_chat_discovery_topics,
)


PROVIDER = "telegram_chat_search"
SEARCH_JOB_TYPE = "telegram.chat_discovery.search"
SCREEN_JOB_TYPE = "telegram.chat_discovery.screen"
BASE_TOPIC_KIND = "base"
PROFILE_TOPIC_KIND = "profile"

DEDUP_BUCKETS = (
    "ALREADY_APPROVED",
    "ALREADY_CANDIDATE",
    "ALREADY_REJECTED",
    "ALREADY_NEEDS_REVIEW",
    "GENUINELY_NEW",
)
SCREEN_STATUSES = (
    "SCREEN_PENDING",
    "SCREEN_RUNNING",
    "WATCH",
    "SKIP",
    "UNCLEAR",
    "SCREEN_FAILED",
)

_SAFE_TOPIC_KEY = re.compile(r"^[a-z0-9][a-z0-9._:-]{0,254}$")


@dataclass(frozen=True)
class ChatDiscoveryTopicRecord:
    id: UUID
    topic_key: str
    topic_text: str
    normalized_topic: str
    language: str
    topic_kind: str
    origin_key: str | None
    is_active: bool
    priority: int
    refresh_interval_seconds: int
    last_searched_at: datetime | None
    next_eligible_at: datetime | None
    last_collector_account_id: int | None
    search_status: str
    search_count: int
    message_hit_count: int
    chat_entity_occurrence_count: int
    unique_peer_count: int
    known_peer_count: int
    new_peer_count: int
    last_error_code: str | None
    created_at: datetime
    updated_at: datetime


@dataclass(frozen=True)
class ChatDiscoverySearchRunRecord:
    id: UUID
    topic_id: UUID
    collector_account_id: int
    search_mode: str
    idempotency_key: str
    status: str
    started_at: datetime
    finished_at: datetime | None
    request_count: int
    message_hit_count: int
    chat_entity_occurrence_count: int
    unique_peer_count: int
    known_peer_count: int
    new_peer_count: int
    group_peer_count: int
    broadcast_peer_count: int
    error_code: str | None
    created_at: datetime


@dataclass(frozen=True)
class ChatDiscoveryPeerRecord:
    id: UUID
    canonical_peer_identity: str
    peer_type: str
    telegram_peer_id: int | None
    telegram_access_hash: int | None
    display_name: str
    username: str | None
    canonical_url: str | None
    access_type: str
    source_id: int | None
    dedup_bucket: str
    screen_status: str
    screen_attempt_count: int
    next_screen_at: datetime | None
    last_screened_at: datetime | None
    screen_decision: str | None
    screen_policy_version: str | None
    screen_model: str | None
    screen_sample_count: int
    screen_useful_count: int
    screen_confidence: float | None
    screen_error_code: str | None
    first_seen_at: datetime
    last_seen_at: datetime
    last_collector_account_id: int | None
    created_at: datetime
    updated_at: datetime


@dataclass(frozen=True)
class ChatDiscoveryBackpressure:
    pending_screens: int
    source_audit_backlog: int
    ai_backlog: int
    paused: bool
    reasons: tuple[str, ...]


@dataclass(frozen=True)
class ScreenClaim:
    peer: ChatDiscoveryPeerRecord
    attempt_number: int


def normalize_topic(value: str, language: str) -> tuple[str, str]:
    """Normalize only exact topic variants; do not merge related concepts."""

    topic = " ".join(
        unicodedata.normalize("NFKC", str(value)).strip().casefold().split()
    )
    lang = _language(language)
    if not topic:
        raise ValueError("topic must not be blank")
    if len(topic) > 255:
        raise ValueError("topic must not exceed 255 characters")
    return topic, f"{lang}:{topic}"


class TelegramChatDiscoveryRepository:
    async def ensure_topic(
        self,
        connection: AsyncConnection,
        *,
        topic_text: str,
        language: str,
        topic_kind: str,
        origin_key: str | None = None,
        priority: int = 50,
        refresh_interval_seconds: int = 21_600,
    ) -> ChatDiscoveryTopicRecord:
        normalized, topic_key = normalize_topic(topic_text, language)
        if topic_kind not in {BASE_TOPIC_KIND, PROFILE_TOPIC_KIND}:
            raise ValueError("topic_kind must be base or profile")
        if not 0 <= priority <= 100:
            raise ValueError("priority must be between 0 and 100")
        if refresh_interval_seconds < 300:
            raise ValueError("refresh_interval_seconds must be at least 300")
        await connection.execute(
            pg_insert(telegram_chat_discovery_topics)
            .values(
                id=uuid4(),
                topic_key=topic_key,
                topic_text=str(topic_text).strip(),
                normalized_topic=normalized,
                language=_language(language),
                topic_kind=topic_kind,
                origin_key=None if origin_key is None else _bounded(origin_key, 255),
                priority=priority,
                refresh_interval_seconds=refresh_interval_seconds,
                next_eligible_at=sa.func.now(),
            )
            .on_conflict_do_nothing(
                constraint="uq_telegram_chat_discovery_topics_normalized_language"
            )
        )
        row = (
            await connection.execute(
                sa.select(telegram_chat_discovery_topics).where(
                    telegram_chat_discovery_topics.c.normalized_topic == normalized,
                    telegram_chat_discovery_topics.c.language == _language(language),
                )
            )
        ).mappings().one_or_none()
        if row is None:
            raise RuntimeError("Telegram chat-discovery topic was not persisted")
        return _topic_record(row)

    async def ensure_base_topics(
        self,
        connection: AsyncConnection,
        *,
        refresh_interval_seconds: int = 21_600,
    ) -> tuple[ChatDiscoveryTopicRecord, ...]:
        records = []
        for topic, language in BASE_DISCOVERY_TOPICS:
            records.append(
                await self.ensure_topic(
                    connection,
                    topic_text=topic,
                    language=language,
                    topic_kind=BASE_TOPIC_KIND,
                    priority=50,
                    refresh_interval_seconds=refresh_interval_seconds,
                )
            )
        return tuple(records)

    async def list_topics(
        self,
        connection: AsyncConnection,
        *,
        limit: int = 100,
        active_only: bool = False,
    ) -> tuple[ChatDiscoveryTopicRecord, ...]:
        if not 1 <= limit <= 10_000:
            raise ValueError("limit must be between 1 and 10000")
        statement = sa.select(telegram_chat_discovery_topics)
        if active_only:
            statement = statement.where(telegram_chat_discovery_topics.c.is_active.is_(True))
        rows = await connection.execute(
            statement.order_by(
                telegram_chat_discovery_topics.c.priority.desc(),
                telegram_chat_discovery_topics.c.topic_key,
            ).limit(limit)
        )
        return tuple(_topic_record(row) for row in rows.mappings())

    async def due_topics(
        self,
        connection: AsyncConnection,
        *,
        now: datetime,
        limit: int,
    ) -> tuple[ChatDiscoveryTopicRecord, ...]:
        if now.tzinfo is None or now.utcoffset() is None:
            raise ValueError("now must be timezone-aware")
        if not 1 <= limit <= 100:
            raise ValueError("limit must be between 1 and 100")
        rows = await connection.execute(
            sa.select(telegram_chat_discovery_topics)
            .where(
                telegram_chat_discovery_topics.c.is_active.is_(True),
                sa.or_(
                    telegram_chat_discovery_topics.c.next_eligible_at.is_(None),
                    telegram_chat_discovery_topics.c.next_eligible_at <= now,
                ),
            )
            .order_by(
                telegram_chat_discovery_topics.c.priority.desc(),
                telegram_chat_discovery_topics.c.next_eligible_at,
                telegram_chat_discovery_topics.c.topic_key,
            )
            .limit(limit)
        )
        return tuple(_topic_record(row) for row in rows.mappings())

    async def get_topic(
        self,
        connection: AsyncConnection,
        topic_id: UUID,
        *,
        lock: bool = False,
    ) -> ChatDiscoveryTopicRecord | None:
        statement = sa.select(telegram_chat_discovery_topics).where(
            telegram_chat_discovery_topics.c.id == topic_id
        )
        if lock:
            statement = statement.with_for_update()
        row = (await connection.execute(statement)).mappings().one_or_none()
        return None if row is None else _topic_record(row)

    async def start_search(
        self,
        connection: AsyncConnection,
        *,
        topic_id: UUID,
        collector_account_id: int,
        idempotency_key: str,
    ) -> ChatDiscoverySearchRunRecord | None:
        topic = await self.get_topic(connection, topic_id, lock=True)
        if topic is None:
            raise LookupError("Telegram chat-discovery topic does not exist")
        existing = (
            await connection.execute(
                sa.select(telegram_chat_discovery_search_runs).where(
                    telegram_chat_discovery_search_runs.c.idempotency_key == idempotency_key
                )
            )
        ).mappings().one_or_none()
        if existing is not None:
            if existing["status"] != "completed":
                await connection.execute(
                    sa.update(telegram_chat_discovery_search_runs)
                    .where(telegram_chat_discovery_search_runs.c.id == existing["id"])
                    .values(
                        status="running",
                        collector_account_id=collector_account_id,
                        started_at=sa.func.now(),
                        finished_at=None,
                        error_code=None,
                    )
                )
                existing = (
                    await connection.execute(
                        sa.select(telegram_chat_discovery_search_runs).where(
                            telegram_chat_discovery_search_runs.c.id == existing["id"]
                        )
                    )
                ).mappings().one()
            return _search_run_record(existing)
        run_id = uuid4()
        await connection.execute(
            telegram_chat_discovery_search_runs.insert().values(
                id=run_id,
                topic_id=topic_id,
                collector_account_id=collector_account_id,
                search_mode="global",
                idempotency_key=idempotency_key,
            )
        )
        await connection.execute(
            sa.update(telegram_chat_discovery_topics)
            .where(telegram_chat_discovery_topics.c.id == topic_id)
            .values(
                search_status="running",
                last_collector_account_id=collector_account_id,
                updated_at=sa.func.now(),
            )
        )
        row = (
            await connection.execute(
                sa.select(telegram_chat_discovery_search_runs).where(
                    telegram_chat_discovery_search_runs.c.id == run_id
                )
            )
        ).mappings().one()
        return _search_run_record(row)

    async def finish_search(
        self,
        connection: AsyncConnection,
        *,
        run_id: UUID,
        topic_id: UUID,
        collector_account_id: int,
        request_count: int,
        message_hit_count: int,
        chat_entity_occurrence_count: int,
        unique_peer_count: int,
        known_peer_count: int,
        new_peer_count: int,
        group_peer_count: int,
        broadcast_peer_count: int,
        error_code: str | None = None,
    ) -> ChatDiscoverySearchRunRecord:
        values = {
            "status": "failed" if error_code else "completed",
            "finished_at": sa.func.now(),
            "request_count": request_count,
            "message_hit_count": message_hit_count,
            "chat_entity_occurrence_count": chat_entity_occurrence_count,
            "unique_peer_count": unique_peer_count,
            "known_peer_count": known_peer_count,
            "new_peer_count": new_peer_count,
            "group_peer_count": group_peer_count,
            "broadcast_peer_count": broadcast_peer_count,
            "error_code": _error_code(error_code),
        }
        await connection.execute(
            sa.update(telegram_chat_discovery_search_runs)
            .where(
                telegram_chat_discovery_search_runs.c.id == run_id,
                telegram_chat_discovery_search_runs.c.status == "running",
            )
            .values(**values)
        )
        await connection.execute(
            sa.update(telegram_chat_discovery_topics)
            .where(telegram_chat_discovery_topics.c.id == topic_id)
            .values(
                search_status="failed" if error_code else "completed",
                search_count=telegram_chat_discovery_topics.c.search_count + 1,
                message_hit_count=telegram_chat_discovery_topics.c.message_hit_count + message_hit_count,
                chat_entity_occurrence_count=telegram_chat_discovery_topics.c.chat_entity_occurrence_count + chat_entity_occurrence_count,
                unique_peer_count=telegram_chat_discovery_topics.c.unique_peer_count + unique_peer_count,
                known_peer_count=telegram_chat_discovery_topics.c.known_peer_count + known_peer_count,
                new_peer_count=telegram_chat_discovery_topics.c.new_peer_count + new_peer_count,
                last_searched_at=sa.func.now(),
                next_eligible_at=(
                    None
                    if error_code
                    else sa.func.now()
                    + sa.text(
                        "make_interval(secs => refresh_interval_seconds)"
                    )
                ),
                last_error_code=_error_code(error_code),
                updated_at=sa.func.now(),
            )
        )
        row = (
            await connection.execute(
                sa.select(telegram_chat_discovery_search_runs).where(
                    telegram_chat_discovery_search_runs.c.id == run_id
                )
            )
        ).mappings().one()
        return _search_run_record(row)

    async def fail_search(
        self,
        connection: AsyncConnection,
        *,
        run_id: UUID,
        topic_id: UUID,
        error_code: str,
    ) -> ChatDiscoverySearchRunRecord:
        return await self.finish_search(
            connection,
            run_id=run_id,
            topic_id=topic_id,
            collector_account_id=0,
            request_count=1,
            message_hit_count=0,
            chat_entity_occurrence_count=0,
            unique_peer_count=0,
            known_peer_count=0,
            new_peer_count=0,
            group_peer_count=0,
            broadcast_peer_count=0,
            error_code=error_code,
        )

    async def find_source_for_references(
        self,
        connection: AsyncConnection,
        references: Sequence[str],
    ) -> Mapping[str, Any] | None:
        values = tuple(sorted({value.strip().casefold() for value in references if value.strip()}))
        if not values:
            return None
        alias_match = sa.exists(
            sa.select(1).where(
                source_reference_aliases.c.source_id == sources.c.id,
                source_reference_aliases.c.platform == "telegram",
                source_reference_aliases.c.normalized_reference.in_(values),
            )
        )
        row = (
            await connection.execute(
                sa.select(sources)
                .where(
                    sources.c.platform == "telegram",
                    sa.or_(
                        sources.c.external_id.in_(values),
                        sa.func.lower(sa.func.coalesce(sources.c.handle, "")).in_(values),
                        sa.func.lower(sa.func.coalesce(sources.c.canonical_url, "")).in_(values),
                        alias_match,
                    ),
                )
                .order_by(sources.c.id)
                .limit(1)
            )
        ).mappings().first()
        return row

    async def get_peer_by_alias(
        self,
        connection: AsyncConnection,
        normalized_reference: str,
    ) -> ChatDiscoveryPeerRecord | None:
        row = (
            await connection.execute(
                sa.select(telegram_chat_discovery_peers)
                .join(
                    telegram_chat_discovery_peer_aliases,
                    telegram_chat_discovery_peer_aliases.c.peer_id
                    == telegram_chat_discovery_peers.c.id,
                )
                .where(
                    telegram_chat_discovery_peer_aliases.c.normalized_reference
                    == normalized_reference.casefold()
                )
                .limit(1)
            )
        ).mappings().first()
        return None if row is None else _peer_record(row)

    async def upsert_peer(
        self,
        connection: AsyncConnection,
        *,
        canonical_peer_identity: str,
        peer_type: str,
        telegram_peer_id: int | None = None,
        telegram_access_hash: int | None = None,
        display_name: str,
        username: str | None,
        canonical_url: str | None,
        access_type: str,
        source_id: int | None,
        dedup_bucket: str,
        collector_account_id: int,
    ) -> tuple[ChatDiscoveryPeerRecord, bool]:
        if dedup_bucket not in DEDUP_BUCKETS:
            raise ValueError("invalid dedup bucket")
        canonical = _bounded(canonical_peer_identity, 255)
        existing = (
            await connection.execute(
                sa.select(telegram_chat_discovery_peers)
                .where(
                    telegram_chat_discovery_peers.c.canonical_peer_identity == canonical
                )
                .with_for_update()
            )
        ).mappings().one_or_none()
        if existing is None:
            peer_id = uuid4()
            await connection.execute(
                telegram_chat_discovery_peers.insert().values(
                    id=peer_id,
                    canonical_peer_identity=canonical,
                    peer_type=peer_type,
                    telegram_peer_id=telegram_peer_id,
                    telegram_access_hash=telegram_access_hash,
                    display_name=_bounded(display_name or canonical, 255),
                    username=_optional(username, 255),
                    canonical_url=_optional(canonical_url, 2048),
                    access_type=access_type,
                    source_id=source_id,
                    dedup_bucket=dedup_bucket,
                    last_collector_account_id=collector_account_id,
                )
            )
            row = (
                await connection.execute(
                    sa.select(telegram_chat_discovery_peers).where(
                        telegram_chat_discovery_peers.c.id == peer_id
                    )
                )
            ).mappings().one()
            return _peer_record(row), True

        values: dict[str, Any] = {
            "last_seen_at": sa.func.now(),
            "updated_at": sa.func.now(),
            "last_collector_account_id": collector_account_id,
        }
        for field, value in (
            ("username", _optional(username, 255)),
            ("canonical_url", _optional(canonical_url, 2048)),
        ):
            if value is not None and existing[field] is None:
                values[field] = value
        if source_id is not None and existing["source_id"] is None:
            values["source_id"] = source_id
            values["dedup_bucket"] = dedup_bucket
        if telegram_peer_id is not None and existing["telegram_peer_id"] is None:
            values["telegram_peer_id"] = telegram_peer_id
        if telegram_access_hash is not None and existing["telegram_access_hash"] is None:
            values["telegram_access_hash"] = telegram_access_hash
        await connection.execute(
            sa.update(telegram_chat_discovery_peers)
            .where(telegram_chat_discovery_peers.c.id == existing["id"])
            .values(**values)
        )
        row = (
            await connection.execute(
                sa.select(telegram_chat_discovery_peers).where(
                    telegram_chat_discovery_peers.c.id == existing["id"]
                )
            )
        ).mappings().one()
        return _peer_record(row), False

    async def add_aliases(
        self,
        connection: AsyncConnection,
        *,
        peer_id: UUID,
        aliases: Sequence[tuple[str, str]],
    ) -> None:
        for value, kind in aliases:
            normalized = _bounded(value, 255).casefold()
            await connection.execute(
                pg_insert(telegram_chat_discovery_peer_aliases)
                .values(
                    peer_id=peer_id,
                    normalized_reference=normalized,
                    reference_kind=kind,
                )
                .on_conflict_do_update(
                    index_elements=[telegram_chat_discovery_peer_aliases.c.normalized_reference],
                    set_={"last_seen_at": sa.func.now()},
                )
            )

    async def add_observation(
        self,
        connection: AsyncConnection,
        *,
        peer_id: UUID,
        topic_id: UUID,
        search_run_id: UUID,
        collector_account_id: int,
        language: str,
        search_mode: str,
        message_hit_count: int,
        chat_entity_occurrence_count: int = 1,
    ) -> bool:
        inserted = await connection.scalar(
            pg_insert(telegram_chat_discovery_observations)
            .values(
                id=uuid4(),
                peer_id=peer_id,
                topic_id=topic_id,
                search_run_id=search_run_id,
                collector_account_id=collector_account_id,
                provider=PROVIDER,
                language=_language(language),
                search_mode=search_mode,
                message_hit_count=max(0, message_hit_count),
                chat_entity_occurrence_count=max(1, chat_entity_occurrence_count),
            )
            .on_conflict_do_nothing(
                constraint="uq_telegram_chat_discovery_observations_peer_run"
            )
            .returning(telegram_chat_discovery_observations.c.id)
        )
        if inserted is not None:
            return True
        await connection.execute(
            sa.update(telegram_chat_discovery_observations)
            .where(
                telegram_chat_discovery_observations.c.peer_id == peer_id,
                telegram_chat_discovery_observations.c.search_run_id == search_run_id,
            )
            .values(
                message_hit_count=telegram_chat_discovery_observations.c.message_hit_count
                + max(0, message_hit_count),
                chat_entity_occurrence_count=telegram_chat_discovery_observations.c.chat_entity_occurrence_count
                + max(1, chat_entity_occurrence_count),
                last_seen_at=sa.func.now(),
            )
        )
        return False

    async def get_peer(
        self,
        connection: AsyncConnection,
        peer_id: UUID,
        *,
        lock: bool = False,
    ) -> ChatDiscoveryPeerRecord | None:
        statement = sa.select(telegram_chat_discovery_peers).where(
            telegram_chat_discovery_peers.c.id == peer_id
        )
        if lock:
            statement = statement.with_for_update()
        row = (await connection.execute(statement)).mappings().one_or_none()
        return None if row is None else _peer_record(row)

    async def get_peer_for_source(
        self,
        connection: AsyncConnection,
        *,
        source_id: int,
    ) -> ChatDiscoveryPeerRecord | None:
        """Return the latest persisted peer metadata for a linked source."""

        if source_id <= 0:
            raise ValueError("source_id must be positive")
        row = (
            await connection.execute(
                sa.select(telegram_chat_discovery_peers)
                .where(telegram_chat_discovery_peers.c.source_id == source_id)
                .order_by(
                    telegram_chat_discovery_peers.c.updated_at.desc(),
                    telegram_chat_discovery_peers.c.id,
                )
                .limit(1)
            )
        ).mappings().one_or_none()
        return None if row is None else _peer_record(row)

    async def list_screen_pending(
        self,
        connection: AsyncConnection,
        *,
        now: datetime,
        limit: int,
    ) -> tuple[ChatDiscoveryPeerRecord, ...]:
        if not 1 <= limit <= 10_000:
            raise ValueError("limit must be between 1 and 10000")
        rows = await connection.execute(
            sa.select(telegram_chat_discovery_peers)
            .where(
                telegram_chat_discovery_peers.c.dedup_bucket == "GENUINELY_NEW",
                sa.or_(
                    telegram_chat_discovery_peers.c.screen_status == "SCREEN_PENDING",
                    sa.and_(
                        telegram_chat_discovery_peers.c.screen_status.in_(("UNCLEAR", "SCREEN_FAILED")),
                        sa.or_(
                            telegram_chat_discovery_peers.c.next_screen_at.is_(None),
                            telegram_chat_discovery_peers.c.next_screen_at <= now,
                        ),
                    ),
                ),
            )
            .order_by(telegram_chat_discovery_peers.c.created_at, telegram_chat_discovery_peers.c.id)
            .limit(limit)
        )
        return tuple(_peer_record(row) for row in rows.mappings())

    async def reclaim_orphaned_screens(
        self,
        connection: AsyncConnection,
    ) -> int:
        """Return screen claims left behind when a worker died or was stopped."""

        rows = await connection.execute(
            sa.select(
                telegram_chat_discovery_peers.c.id,
                telegram_chat_discovery_peers.c.screen_attempt_count,
            ).where(telegram_chat_discovery_peers.c.screen_status == "SCREEN_RUNNING")
        )
        reclaimed = 0
        for row in rows:
            job_key = f"peer:{row.id}:attempt:{int(row.screen_attempt_count)}"
            active = await connection.scalar(
                sa.select(sa.func.count())
                .select_from(durable_jobs)
                .where(
                    durable_jobs.c.job_type == SCREEN_JOB_TYPE,
                    durable_jobs.c.idempotency_key == job_key,
                    durable_jobs.c.state == "running",
                )
            )
            if int(active or 0) != 0:
                continue
            result = await connection.execute(
                sa.update(telegram_chat_discovery_peers)
                .where(
                    telegram_chat_discovery_peers.c.id == row.id,
                    telegram_chat_discovery_peers.c.screen_status == "SCREEN_RUNNING",
                )
                .values(
                    screen_status="SCREEN_PENDING",
                    next_screen_at=None,
                    screen_error_code="WorkerRestarted",
                    updated_at=sa.func.now(),
                )
            )
            reclaimed += int(result.rowcount or 0)
        return reclaimed

    async def release_screen_claim(
        self,
        connection: AsyncConnection,
        *,
        peer_id: UUID,
    ) -> bool:
        result = await connection.execute(
            sa.update(telegram_chat_discovery_peers)
            .where(
                telegram_chat_discovery_peers.c.id == peer_id,
                telegram_chat_discovery_peers.c.screen_status == "SCREEN_RUNNING",
            )
            .values(
                screen_status="SCREEN_PENDING",
                next_screen_at=None,
                screen_error_code="WorkerCancelled",
                updated_at=sa.func.now(),
            )
        )
        return result.rowcount == 1

    async def claim_screen(
        self,
        connection: AsyncConnection,
        *,
        peer_id: UUID,
        now: datetime,
    ) -> ScreenClaim | None:
        peer = await self.get_peer(connection, peer_id, lock=True)
        if peer is None or peer.dedup_bucket != "GENUINELY_NEW":
            return None
        if peer.screen_status in {"WATCH", "SKIP"}:
            return None
        if peer.screen_status in {"UNCLEAR", "SCREEN_FAILED"} and peer.next_screen_at is not None and peer.next_screen_at > now:
            return None
        attempt = peer.screen_attempt_count + 1
        await connection.execute(
            sa.update(telegram_chat_discovery_peers)
            .where(telegram_chat_discovery_peers.c.id == peer_id)
            .values(
                screen_status="SCREEN_RUNNING",
                screen_attempt_count=attempt,
                screen_error_code=None,
                updated_at=sa.func.now(),
            )
        )
        updated = await self.get_peer(connection, peer_id)
        if updated is None:
            raise RuntimeError("claimed chat peer disappeared")
        return ScreenClaim(updated, attempt)

    async def finish_screen(
        self,
        connection: AsyncConnection,
        *,
        claim: ScreenClaim,
        collector_account_id: int,
        status: str,
        decision: str | None,
        policy_version: str,
        provider: str,
        model: str | None,
        sample_count: int,
        useful_count: int,
        confidence: float | None,
        category_counts: Mapping[str, int],
        reason_codes: Sequence[str],
        history_request_count: int = 0,
        ai_call_count: int = 0,
        error_code: str | None = None,
        retry_at: datetime | None = None,
    ) -> ChatDiscoveryPeerRecord:
        if status not in {"WATCH", "SKIP", "UNCLEAR", "SCREEN_FAILED"}:
            raise ValueError("invalid final screen status")
        await connection.execute(
            pg_insert(telegram_chat_discovery_screen_attempts)
            .values(
                id=uuid4(),
                peer_id=claim.peer.id,
                collector_account_id=collector_account_id,
                attempt_number=claim.attempt_number,
                status=status,
                decision=decision,
                policy_version=_bounded(policy_version, 64),
                provider=_bounded(provider, 64).casefold(),
                model=None if model is None else _bounded(model, 128),
                sample_count=max(0, sample_count),
                useful_count=max(0, useful_count),
                history_request_count=max(0, history_request_count),
                ai_call_count=max(0, ai_call_count),
                confidence=confidence,
                category_counts=dict(category_counts),
                reason_codes=list(reason_codes),
                finished_at=sa.func.now(),
                error_code=_error_code(error_code),
            )
            .on_conflict_do_nothing(
                constraint="uq_telegram_chat_discovery_screen_attempts_peer_attempt"
            )
        )
        await connection.execute(
            sa.update(telegram_chat_discovery_peers)
            .where(
                telegram_chat_discovery_peers.c.id == claim.peer.id,
                telegram_chat_discovery_peers.c.screen_status == "SCREEN_RUNNING",
            )
            .values(
                screen_status=status,
                next_screen_at=retry_at,
                last_screened_at=sa.func.now(),
                screen_decision=decision,
                screen_policy_version=_bounded(policy_version, 64),
                screen_model=None if model is None else _bounded(model, 128),
                screen_sample_count=max(0, sample_count),
                screen_useful_count=max(0, useful_count),
                screen_confidence=confidence,
                screen_error_code=_error_code(error_code),
                updated_at=sa.func.now(),
            )
        )
        updated = await self.get_peer(connection, claim.peer.id)
        if updated is None:
            raise RuntimeError("screened chat peer disappeared")
        return updated

    async def attach_source(
        self,
        connection: AsyncConnection,
        *,
        peer_id: UUID,
        source_id: int,
        dedup_bucket: str = "ALREADY_CANDIDATE",
    ) -> ChatDiscoveryPeerRecord:
        if dedup_bucket not in DEDUP_BUCKETS:
            raise ValueError("invalid dedup bucket")
        await connection.execute(
            sa.update(telegram_chat_discovery_peers)
            .where(telegram_chat_discovery_peers.c.id == peer_id)
            .values(
                source_id=source_id,
                dedup_bucket=dedup_bucket,
                updated_at=sa.func.now(),
            )
        )
        updated = await self.get_peer(connection, peer_id)
        if updated is None:
            raise RuntimeError("chat peer disappeared while attaching source")
        return updated

    async def screen_status_counts(
        self,
        connection: AsyncConnection,
    ) -> dict[str, int]:
        rows = await connection.execute(
            sa.select(
                telegram_chat_discovery_peers.c.screen_status,
                sa.func.count().label("count"),
            )
            .group_by(telegram_chat_discovery_peers.c.screen_status)
            .order_by(telegram_chat_discovery_peers.c.screen_status)
        )
        return {str(row[0]): int(row[1]) for row in rows}

    async def backpressure(
        self,
        connection: AsyncConnection,
        *,
        pending_screen_limit: int,
        source_audit_limit: int,
        ai_limit: int,
    ) -> ChatDiscoveryBackpressure:
        pending = int(
            await connection.scalar(
                sa.select(sa.func.count())
                .select_from(telegram_chat_discovery_peers)
                .where(
                    telegram_chat_discovery_peers.c.screen_status.in_(("SCREEN_PENDING", "SCREEN_RUNNING"))
                )
            )
            or 0
        )
        source_audit = int(
            await connection.scalar(
                sa.select(sa.func.count())
                .select_from(durable_jobs)
                .where(
                    durable_jobs.c.job_type.like("source.audit%"),
                    durable_jobs.c.state.in_(("queued", "running")),
                )
            )
            or 0
        )
        ai_backlog = int(
            await connection.scalar(
                sa.select(sa.func.count())
                .select_from(durable_jobs)
                .where(
                    durable_jobs.c.job_type.like("opportunity.analysis%"),
                    durable_jobs.c.state.in_(("queued", "running")),
                )
            )
            or 0
        )
        reasons = tuple(
            reason
            for reason, value, limit in (
                ("screen_backlog", pending, pending_screen_limit),
                ("source_audit_backlog", source_audit, source_audit_limit),
                ("ai_backlog", ai_backlog, ai_limit),
            )
            if value >= limit
        )
        return ChatDiscoveryBackpressure(
            pending_screens=pending,
            source_audit_backlog=source_audit,
            ai_backlog=ai_backlog,
            paused=bool(reasons),
            reasons=reasons,
        )

    async def enqueue_screen_job(
        self,
        connection: AsyncConnection,
        *,
        peer_id: UUID,
        attempt_number: int = 1,
    ) -> UUID:
        return await DurableJobRepository().enqueue(
            connection,
            job_type=SCREEN_JOB_TYPE,
            idempotency_key=f"peer:{peer_id}:attempt:{attempt_number}",
        )

    async def enqueue_search_job(
        self,
        connection: AsyncConnection,
        *,
        topic_id: UUID,
        refresh_key: str,
    ) -> UUID:
        return await DurableJobRepository().enqueue(
            connection,
            job_type=SEARCH_JOB_TYPE,
            idempotency_key=f"topic:{topic_id}:refresh:{_bounded(refresh_key, 128)}",
        )

    async def job_counts(self, connection: AsyncConnection) -> Mapping[str, int]:
        rows = await connection.execute(
            sa.select(durable_jobs.c.job_type, durable_jobs.c.state, sa.func.count().label("count"))
            .where(durable_jobs.c.job_type.in_((SEARCH_JOB_TYPE, SCREEN_JOB_TYPE)))
            .group_by(durable_jobs.c.job_type, durable_jobs.c.state)
            .order_by(durable_jobs.c.job_type, durable_jobs.c.state)
        )
        return {
            f"{row.job_type}:{row.state}": int(row.count)
            for row in rows
        }

    async def active_job_count(
        self,
        connection: AsyncConnection,
        *,
        job_ids: Sequence[UUID],
    ) -> int:
        if not job_ids:
            return 0
        return int(
            await connection.scalar(
                sa.select(sa.func.count())
                .select_from(durable_jobs)
                .where(
                    durable_jobs.c.id.in_(tuple(job_ids)),
                    durable_jobs.c.state.in_(("queued", "running")),
                )
            )
            or 0
        )

    async def status_snapshot(
        self,
        connection: AsyncConnection,
        *,
        now: datetime,
    ) -> Mapping[str, Any]:
        topic_rows = (
            await connection.execute(
                sa.select(
                    sa.func.count().label("active_topics"),
                    sa.func.count().filter(
                        sa.or_(
                            telegram_chat_discovery_topics.c.next_eligible_at.is_(None),
                            telegram_chat_discovery_topics.c.next_eligible_at <= now,
                        )
                    ).label("due_topics"),
                    sa.func.coalesce(sa.func.sum(telegram_chat_discovery_topics.c.search_count), 0).label("searches_completed"),
                    sa.func.coalesce(sa.func.sum(telegram_chat_discovery_topics.c.message_hit_count), 0).label("message_hits"),
                    sa.func.coalesce(sa.func.sum(telegram_chat_discovery_topics.c.chat_entity_occurrence_count), 0).label("chat_entity_occurrences"),
                    sa.func.coalesce(sa.func.sum(telegram_chat_discovery_topics.c.unique_peer_count), 0).label("unique_peers"),
                    sa.func.coalesce(sa.func.sum(telegram_chat_discovery_topics.c.known_peer_count), 0).label("known_peers"),
                    sa.func.coalesce(sa.func.sum(telegram_chat_discovery_topics.c.new_peer_count), 0).label("new_peers"),
                )
                .where(telegram_chat_discovery_topics.c.is_active.is_(True))
            )
        ).mappings().one()
        search_metrics = (
            await connection.execute(
                sa.select(
                    sa.func.count().filter(
                        telegram_chat_discovery_search_runs.c.status == "completed"
                    ).label("completed_searches"),
                    sa.func.coalesce(
                        sa.func.sum(telegram_chat_discovery_search_runs.c.request_count), 0
                    ).label("search_requests"),
                )
            )
        ).mappings().one()
        bucket_rows = await connection.execute(
            sa.select(
                telegram_chat_discovery_peers.c.dedup_bucket,
                sa.func.count().label("count"),
            )
            .group_by(telegram_chat_discovery_peers.c.dedup_bucket)
            .order_by(telegram_chat_discovery_peers.c.dedup_bucket)
        )
        screen_rows = await self.screen_status_counts(connection)
        screen_metrics = (
            await connection.execute(
                sa.select(
                    sa.func.count().label("attempts"),
                    sa.func.count().filter(
                        telegram_chat_discovery_screen_attempts.c.status == "WATCH"
                    ).label("watch_attempts"),
                    sa.func.coalesce(
                        sa.func.sum(telegram_chat_discovery_screen_attempts.c.history_request_count), 0
                    ).label("history_requests"),
                    sa.func.coalesce(
                        sa.func.sum(telegram_chat_discovery_screen_attempts.c.ai_call_count), 0
                    ).label("ai_calls"),
                )
                .select_from(telegram_chat_discovery_screen_attempts)
            )
        ).mappings().one()
        search_requests = int(search_metrics["search_requests"])
        new_peers = int(topic_rows["new_peers"])
        watch_attempts = int(screen_metrics["watch_attempts"])
        return {
            "topics": {
                "active": int(topic_rows["active_topics"]),
                "due": int(topic_rows["due_topics"]),
                "searches_completed": int(search_metrics["completed_searches"]),
            },
            "search": {
                "message_hits": int(topic_rows["message_hits"]),
                "chat_entity_occurrences": int(topic_rows["chat_entity_occurrences"]),
                "unique_peers": int(topic_rows["unique_peers"]),
                "known_peers": int(topic_rows["known_peers"]),
                "new_peers": int(topic_rows["new_peers"]),
            },
            "dedup": {str(row[0]): int(row[1]) for row in bucket_rows},
            "screen": {
                **screen_rows,
                "attempts": int(screen_metrics["attempts"]),
                "history_requests": int(screen_metrics["history_requests"]),
                "ai_calls": int(screen_metrics["ai_calls"]),
            },
            "efficiency": {
                "new_peers_per_search_request": _ratio(new_peers, search_requests),
                "watch_per_100_new_peers": _ratio(watch_attempts * 100, new_peers),
                "history_requests_per_watch": _ratio(
                    int(screen_metrics["history_requests"]), watch_attempts
                ),
                "ai_calls_per_watch": _ratio(int(screen_metrics["ai_calls"]), watch_attempts),
            },
            "jobs": dict(await self.job_counts(connection)),
        }


BASE_DISCOVERY_TOPICS: tuple[tuple[str, str], ...] = (
    ("фриланс", "ru"),
    ("вакансии", "ru"),
    ("удаленная работа", "ru"),
    ("заказы фриланс", "ru"),
    ("разработчики", "ru"),
    ("дизайнеры", "ru"),
    ("маркетологи", "ru"),
    ("предприниматели", "ru"),
    ("стартапы", "ru"),
    ("видеомонтаж", "ru"),
    ("freelance", "en"),
    ("remote jobs", "en"),
    ("developers", "en"),
    ("designers", "en"),
    ("marketing", "en"),
    ("startup", "en"),
    ("founders", "en"),
    ("ecommerce", "en"),
    ("video editing", "en"),
    ("motion design", "en"),
)


def _topic_record(row: Mapping[str, Any]) -> ChatDiscoveryTopicRecord:
    return ChatDiscoveryTopicRecord(
        id=row["id"],
        topic_key=str(row["topic_key"]),
        topic_text=str(row["topic_text"]),
        normalized_topic=str(row["normalized_topic"]),
        language=str(row["language"]),
        topic_kind=str(row["topic_kind"]),
        origin_key=row["origin_key"],
        is_active=bool(row["is_active"]),
        priority=int(row["priority"]),
        refresh_interval_seconds=int(row["refresh_interval_seconds"]),
        last_searched_at=row["last_searched_at"],
        next_eligible_at=row["next_eligible_at"],
        last_collector_account_id=(
            None
            if row["last_collector_account_id"] is None
            else int(row["last_collector_account_id"])
        ),
        search_status=str(row["search_status"]),
        search_count=int(row["search_count"]),
        message_hit_count=int(row["message_hit_count"]),
        chat_entity_occurrence_count=int(row["chat_entity_occurrence_count"]),
        unique_peer_count=int(row["unique_peer_count"]),
        known_peer_count=int(row["known_peer_count"]),
        new_peer_count=int(row["new_peer_count"]),
        last_error_code=row["last_error_code"],
        created_at=row["created_at"],
        updated_at=row["updated_at"],
    )


def _search_run_record(row: Mapping[str, Any]) -> ChatDiscoverySearchRunRecord:
    return ChatDiscoverySearchRunRecord(
        id=row["id"],
        topic_id=row["topic_id"],
        collector_account_id=int(row["collector_account_id"]),
        search_mode=str(row["search_mode"]),
        idempotency_key=str(row["idempotency_key"]),
        status=str(row["status"]),
        started_at=row["started_at"],
        finished_at=row["finished_at"],
        request_count=int(row["request_count"]),
        message_hit_count=int(row["message_hit_count"]),
        chat_entity_occurrence_count=int(row["chat_entity_occurrence_count"]),
        unique_peer_count=int(row["unique_peer_count"]),
        known_peer_count=int(row["known_peer_count"]),
        new_peer_count=int(row["new_peer_count"]),
        group_peer_count=int(row["group_peer_count"]),
        broadcast_peer_count=int(row["broadcast_peer_count"]),
        error_code=row["error_code"],
        created_at=row["created_at"],
    )


def _peer_record(row: Mapping[str, Any]) -> ChatDiscoveryPeerRecord:
    return ChatDiscoveryPeerRecord(
        id=row["id"],
        canonical_peer_identity=str(row["canonical_peer_identity"]),
        peer_type=str(row["peer_type"]),
        telegram_peer_id=(
            None if row["telegram_peer_id"] is None else int(row["telegram_peer_id"])
        ),
        telegram_access_hash=(
            None
            if row["telegram_access_hash"] is None
            else int(row["telegram_access_hash"])
        ),
        display_name=str(row["display_name"]),
        username=row["username"],
        canonical_url=row["canonical_url"],
        access_type=str(row["access_type"]),
        source_id=None if row["source_id"] is None else int(row["source_id"]),
        dedup_bucket=str(row["dedup_bucket"]),
        screen_status=str(row["screen_status"]),
        screen_attempt_count=int(row["screen_attempt_count"]),
        next_screen_at=row["next_screen_at"],
        last_screened_at=row["last_screened_at"],
        screen_decision=row["screen_decision"],
        screen_policy_version=row["screen_policy_version"],
        screen_model=row["screen_model"],
        screen_sample_count=int(row["screen_sample_count"]),
        screen_useful_count=int(row["screen_useful_count"]),
        screen_confidence=(
            None if row["screen_confidence"] is None else float(row["screen_confidence"])
        ),
        screen_error_code=row["screen_error_code"],
        first_seen_at=row["first_seen_at"],
        last_seen_at=row["last_seen_at"],
        last_collector_account_id=(
            None
            if row["last_collector_account_id"] is None
            else int(row["last_collector_account_id"])
        ),
        created_at=row["created_at"],
        updated_at=row["updated_at"],
    )


def _language(value: str) -> str:
    normalized = str(value).strip().casefold()
    if not re.fullmatch(r"[a-z]{2,3}(?:-[a-z]{2})?", normalized):
        raise ValueError("language must be a two or three letter code")
    return normalized


def _bounded(value: object, limit: int) -> str:
    normalized = str(value).strip()
    if not normalized or len(normalized) > limit:
        raise ValueError("text value is blank or exceeds its limit")
    return normalized


def _optional(value: object | None, limit: int) -> str | None:
    return None if value is None else _bounded(value, limit)


def _error_code(value: str | None) -> str | None:
    if value is None:
        return None
    normalized = re.sub(r"[^A-Za-z0-9_.-]", "_", str(value).strip())[:64]
    return normalized if normalized and normalized[0].isalpha() else "ProcessingError"


def _ratio(numerator: int, denominator: int) -> float | None:
    if denominator <= 0:
        return None
    return round(numerator / denominator, 4)
