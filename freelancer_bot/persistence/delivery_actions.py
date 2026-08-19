from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from hashlib import sha256
import json
from urllib.parse import urlsplit
from uuid import UUID, uuid4

import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncConnection

from .schema import (
    delivery_action_events,
    opportunity_source_messages,
    personalized_deliveries,
    raw_messages,
)


DELIVERY_ACTION_SCHEMA_VERSION = "delivery-action.v1"


class DeliveryActionType(str, Enum):
    OPEN = "open"
    NOT_SUITABLE = "not_suitable"
    GOT_JOB = "got_job"


class DeliveryActionError(RuntimeError):
    pass


class DeliveryActionUnavailable(DeliveryActionError):
    pass


class DeliveryActionOwnershipError(DeliveryActionError):
    pass


@dataclass(frozen=True)
class DeliveryActionRecord:
    id: UUID
    idempotency_key: str
    schema_version: str
    action_type: DeliveryActionType
    delivery_id: UUID
    match_trace_id: UUID
    match_run_id: UUID
    opportunity_id: UUID
    search_profile_id: UUID
    profile_revision: int
    user_id: UUID
    source_id: int
    source_raw_message_id: UUID
    source_url: str
    actor_platform: str
    actor_external_user_id: str
    created_at: datetime


@dataclass(frozen=True)
class DeliveryActionWriteOutcome:
    event: DeliveryActionRecord
    created: bool


class DeliveryActionRepository:
    async def record(
        self,
        connection: AsyncConnection,
        *,
        delivery_id: UUID,
        action_type: DeliveryActionType,
        actor_external_user_id: str,
    ) -> DeliveryActionWriteOutcome:
        actor_id = _telegram_actor_id(actor_external_user_id)
        delivery = (
            await connection.execute(
                sa.select(personalized_deliveries).where(
                    personalized_deliveries.c.id == delivery_id
                )
            )
        ).mappings().one_or_none()
        if delivery is None or delivery["status"] != "sent":
            raise DeliveryActionUnavailable("delivery action is unavailable")
        if (
            delivery["recipient_platform"] != "telegram"
            or delivery["recipient_external_user_id"] != actor_id
        ):
            raise DeliveryActionOwnershipError("delivery does not belong to actor")

        source_url = _safe_source_url(delivery["source_url"])
        source = (
            await connection.execute(
                sa.select(
                    raw_messages.c.id.label("raw_message_id"),
                    raw_messages.c.source_id,
                )
                .join(
                    opportunity_source_messages,
                    opportunity_source_messages.c.raw_message_id
                    == raw_messages.c.id,
                )
                .where(
                    opportunity_source_messages.c.opportunity_id
                    == delivery["opportunity_id"],
                    raw_messages.c.message_url == source_url,
                )
                .order_by(
                    opportunity_source_messages.c.linked_at,
                    raw_messages.c.id,
                )
                .limit(1)
            )
        ).mappings().one_or_none()
        if source is None:
            raise DeliveryActionUnavailable("delivery source is unavailable")

        idempotency_key = delivery_action_idempotency_key(
            delivery_id,
            action_type,
        )
        event_id = uuid4()
        inserted_id = await connection.scalar(
            pg_insert(delivery_action_events)
            .values(
                id=event_id,
                idempotency_key=idempotency_key,
                schema_version=DELIVERY_ACTION_SCHEMA_VERSION,
                action_type=action_type.value,
                delivery_id=delivery_id,
                match_trace_id=delivery["match_trace_id"],
                match_run_id=delivery["match_run_id"],
                opportunity_id=delivery["opportunity_id"],
                search_profile_id=delivery["search_profile_id"],
                profile_revision=delivery["profile_revision"],
                user_id=delivery["user_id"],
                source_id=source["source_id"],
                source_raw_message_id=source["raw_message_id"],
                source_url=source_url,
                actor_platform="telegram",
                actor_external_user_id=actor_id,
            )
            # Both unique constraints describe the same replay boundary: the
            # deterministic key is derived from delivery_id + action_type.
            # Arbitrate any unique conflict so a concurrent replay cannot
            # surface the secondary idempotency constraint as a 500.
            .on_conflict_do_nothing()
            .returning(delivery_action_events.c.id)
        )
        record = await self.get_by_idempotency_key(connection, idempotency_key)
        if record is None:
            raise DeliveryActionError("delivery action insert returned no record")
        _validate_existing(record, delivery, source, action_type, actor_id)
        return DeliveryActionWriteOutcome(
            event=record,
            created=inserted_id is not None,
        )

    async def get_by_idempotency_key(
        self,
        connection: AsyncConnection,
        idempotency_key: str,
    ) -> DeliveryActionRecord | None:
        row = (
            await connection.execute(
                sa.select(delivery_action_events).where(
                    delivery_action_events.c.idempotency_key == idempotency_key
                )
            )
        ).mappings().one_or_none()
        return None if row is None else _record(row)

    async def list_for_delivery(
        self,
        connection: AsyncConnection,
        delivery_id: UUID,
    ) -> tuple[DeliveryActionRecord, ...]:
        rows = (
            await connection.execute(
                sa.select(delivery_action_events)
                .where(delivery_action_events.c.delivery_id == delivery_id)
                .order_by(
                    delivery_action_events.c.created_at,
                    delivery_action_events.c.action_type,
                    delivery_action_events.c.id,
                )
            )
        ).mappings().all()
        return tuple(_record(row) for row in rows)


def delivery_action_idempotency_key(
    delivery_id: UUID,
    action_type: DeliveryActionType,
) -> str:
    payload = {
        "schema_version": DELIVERY_ACTION_SCHEMA_VERSION,
        "delivery_id": str(delivery_id),
        "action_type": action_type.value,
    }
    encoded = json.dumps(
        payload,
        ensure_ascii=True,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return sha256(encoded).hexdigest()


def _telegram_actor_id(value: str) -> str:
    if not value.isascii() or not value.isdecimal():
        raise DeliveryActionOwnershipError("invalid Telegram actor")
    actor_id = int(value)
    if actor_id <= 0 or len(value) > 20 or str(actor_id) != value:
        raise DeliveryActionOwnershipError("invalid Telegram actor")
    return value


def _safe_source_url(value: str | None) -> str:
    if value is None or len(value) > 2048:
        raise DeliveryActionUnavailable("delivery source is unavailable")
    parsed = urlsplit(value)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        raise DeliveryActionUnavailable("delivery source is unavailable")
    return value


def _validate_existing(
    record: DeliveryActionRecord,
    delivery,
    source,
    action_type: DeliveryActionType,
    actor_id: str,
) -> None:
    expected = (
        delivery["id"],
        delivery["match_trace_id"],
        delivery["match_run_id"],
        delivery["opportunity_id"],
        delivery["search_profile_id"],
        delivery["profile_revision"],
        delivery["user_id"],
        source["source_id"],
        source["raw_message_id"],
        action_type,
        actor_id,
    )
    actual = (
        record.delivery_id,
        record.match_trace_id,
        record.match_run_id,
        record.opportunity_id,
        record.search_profile_id,
        record.profile_revision,
        record.user_id,
        record.source_id,
        record.source_raw_message_id,
        record.action_type,
        record.actor_external_user_id,
    )
    if actual != expected:
        raise DeliveryActionError(
            "delivery action idempotency key exists with different content"
        )


def _record(row) -> DeliveryActionRecord:
    return DeliveryActionRecord(
        id=row["id"],
        idempotency_key=row["idempotency_key"],
        schema_version=row["schema_version"],
        action_type=DeliveryActionType(row["action_type"]),
        delivery_id=row["delivery_id"],
        match_trace_id=row["match_trace_id"],
        match_run_id=row["match_run_id"],
        opportunity_id=row["opportunity_id"],
        search_profile_id=row["search_profile_id"],
        profile_revision=row["profile_revision"],
        user_id=row["user_id"],
        source_id=row["source_id"],
        source_raw_message_id=row["source_raw_message_id"],
        source_url=row["source_url"],
        actor_platform=row["actor_platform"],
        actor_external_user_id=row["actor_external_user_id"],
        created_at=row["created_at"],
    )
