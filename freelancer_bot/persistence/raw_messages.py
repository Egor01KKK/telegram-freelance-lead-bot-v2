from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from hashlib import sha256
from typing import Any
from uuid import UUID, uuid4

import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncConnection

from .database import Database
from .jobs import DurableJobRepository
from .schema import raw_messages
from .source_repository import SourceRecord, SourceRepository


RAW_MESSAGE_SCHEMA_VERSION = "telegram-raw.v1"
RAW_MESSAGE_JOB_TYPE = "telegram.raw_message.v1"


class RawMessageOrigin(str, Enum):
    LIVE = "live"
    CATCH_UP = "catch_up"


class IneligibleRawMessageSource(PermissionError):
    pass


@dataclass(frozen=True)
class RawMessageInput:
    source_id: int
    collector_account_id: int
    external_message_id: int
    message_date: datetime
    observed_at: datetime
    message_url: str
    content: str
    transport_metadata: Mapping[str, Any]
    ingestion_origin: RawMessageOrigin
    correlation_id: UUID

    def __post_init__(self) -> None:
        if self.source_id <= 0 or self.collector_account_id <= 0:
            raise ValueError("source and collector account identifiers must be positive")
        if self.external_message_id <= 0:
            raise ValueError("external_message_id must be positive")
        _aware(self.message_date, "message_date")
        _aware(self.observed_at, "observed_at")
        if not self.message_url.strip():
            raise ValueError("message_url must not be blank")
        if not isinstance(self.content, str):
            raise TypeError("content must be text")
        if not isinstance(self.transport_metadata, Mapping):
            raise TypeError("transport_metadata must be a mapping")
        if not isinstance(self.ingestion_origin, RawMessageOrigin):
            raise TypeError("ingestion_origin must be RawMessageOrigin")
        if not isinstance(self.correlation_id, UUID):
            raise TypeError("correlation_id must be UUID")


@dataclass(frozen=True)
class RawMessageRecord:
    id: UUID
    source_id: int
    collector_account_id: int
    processing_job_id: UUID
    schema_version: str
    platform: str
    external_source_id: str
    external_message_id: int
    message_date: datetime
    observed_at: datetime
    message_url: str
    content: str
    transport_metadata: Mapping[str, Any]
    ingestion_origin: RawMessageOrigin
    correlation_id: UUID
    created_at: datetime


@dataclass(frozen=True)
class RawMessageIngestResult:
    message: RawMessageRecord
    created: bool


class RawMessageRepository:
    async def list_recent(
        self,
        connection: AsyncConnection,
        *,
        limit: int = 100,
    ) -> tuple[RawMessageRecord, ...]:
        if not 1 <= limit <= 500:
            raise ValueError("limit must be between 1 and 500")
        rows = await connection.execute(
            sa.select(raw_messages)
            .order_by(raw_messages.c.observed_at.desc(), raw_messages.c.id)
            .limit(limit)
        )
        return tuple(_record(row) for row in rows.mappings())

    async def record(
        self,
        connection: AsyncConnection,
        *,
        message: RawMessageInput,
        source: SourceRecord,
        processing_job_id: UUID,
    ) -> RawMessageIngestResult:
        message_id = uuid4()
        inserted = (
            await connection.execute(
                pg_insert(raw_messages)
                .values(
                    id=message_id,
                    source_id=source.id,
                    collector_account_id=message.collector_account_id,
                    processing_job_id=processing_job_id,
                    schema_version=RAW_MESSAGE_SCHEMA_VERSION,
                    platform=source.platform,
                    external_source_id=source.external_id,
                    external_message_id=message.external_message_id,
                    message_date=message.message_date,
                    observed_at=message.observed_at,
                    message_url=message.message_url.strip(),
                    content=message.content,
                    transport_metadata=dict(message.transport_metadata),
                    ingestion_origin=message.ingestion_origin.value,
                    correlation_id=message.correlation_id,
                )
                .on_conflict_do_nothing(
                    constraint="uq_raw_messages_source_message"
                )
                .returning(raw_messages)
            )
        ).mappings().one_or_none()
        if inserted is not None:
            return RawMessageIngestResult(_record(inserted), created=True)

        existing = (
            await connection.execute(
                sa.select(raw_messages).where(
                    raw_messages.c.source_id == source.id,
                    raw_messages.c.external_message_id
                    == message.external_message_id,
                )
            )
        ).mappings().one()
        if existing["processing_job_id"] != processing_job_id:
            raise RuntimeError("Raw message and durable job identity diverged")
        return RawMessageIngestResult(_record(existing), created=False)

    async def get_for_job(
        self,
        connection: AsyncConnection,
        processing_job_id: UUID,
    ) -> RawMessageRecord | None:
        row = (
            await connection.execute(
                sa.select(raw_messages).where(
                    raw_messages.c.processing_job_id == processing_job_id
                )
            )
        ).mappings().one_or_none()
        return None if row is None else _record(row)

    async def get_by_id(
        self,
        connection: AsyncConnection,
        raw_message_id: UUID,
    ) -> RawMessageRecord | None:
        row = (
            await connection.execute(
                sa.select(raw_messages).where(raw_messages.c.id == raw_message_id)
            )
        ).mappings().one_or_none()
        return None if row is None else _record(row)

    async def get_by_source_message(
        self,
        connection: AsyncConnection,
        *,
        source_id: int,
        external_message_id: int,
    ) -> RawMessageRecord | None:
        row = (
            await connection.execute(
                sa.select(raw_messages).where(
                    raw_messages.c.source_id == source_id,
                    raw_messages.c.external_message_id == external_message_id,
                )
            )
        ).mappings().one_or_none()
        return None if row is None else _record(row)


class RawMessageIngestor:
    def __init__(
        self,
        database: Database,
        *,
        messages: RawMessageRepository | None = None,
        jobs: DurableJobRepository | None = None,
        sources: SourceRepository | None = None,
    ) -> None:
        self._database = database
        self._messages = messages or RawMessageRepository()
        self._jobs = jobs or DurableJobRepository()
        self._sources = sources or SourceRepository()

    async def ingest(self, message: RawMessageInput) -> RawMessageIngestResult:
        async with self._database.transaction() as connection:
            source = await self._sources.get_for_collector(
                connection,
                source_id=message.source_id,
                collector_account_id=message.collector_account_id,
                platform="telegram",
                lock=True,
            )
            if source is None:
                raise IneligibleRawMessageSource(
                    "Source is not approved and accessible for this collector account"
                )
            job_id = await self._jobs.enqueue(
                connection,
                job_type=RAW_MESSAGE_JOB_TYPE,
                idempotency_key=_job_key(
                    source.platform,
                    source.external_id,
                    message.external_message_id,
                ),
                correlation_id=message.correlation_id,
            )
            return await self._messages.record(
                connection,
                message=message,
                source=source,
                processing_job_id=job_id,
            )


def _job_key(platform: str, external_source_id: str, message_id: int) -> str:
    identity = f"{platform}\0{external_source_id}\0{message_id}".encode("utf-8")
    return "telegram-raw-v1:" + sha256(identity).hexdigest()


def _record(row: Mapping[str, Any]) -> RawMessageRecord:
    return RawMessageRecord(
        id=row["id"],
        source_id=int(row["source_id"]),
        collector_account_id=int(row["collector_account_id"]),
        processing_job_id=row["processing_job_id"],
        schema_version=row["schema_version"],
        platform=row["platform"],
        external_source_id=row["external_source_id"],
        external_message_id=int(row["external_message_id"]),
        message_date=row["message_date"],
        observed_at=row["observed_at"],
        message_url=row["message_url"],
        content=row["content"],
        transport_metadata=dict(row["transport_metadata"]),
        ingestion_origin=RawMessageOrigin(row["ingestion_origin"]),
        correlation_id=row["correlation_id"],
        created_at=row["created_at"],
    )


def _aware(value: datetime, field: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field} must include a timezone")
    return value
