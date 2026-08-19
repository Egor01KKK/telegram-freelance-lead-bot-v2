from __future__ import annotations

from datetime import datetime
from uuid import UUID

import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncConnection

from .schema import (
    legacy_processed_messages,
    legacy_recipient_deliveries,
    subscribers,
)


class DeliveryEvidenceConflict(RuntimeError):
    pass


class SubscriberRepository:
    async def ensure(
        self,
        connection: AsyncConnection,
        telegram_chat_id: int,
        *,
        active: bool,
        created_at: datetime | None = None,
    ) -> int:
        values: dict[str, object] = {
            "telegram_chat_id": telegram_chat_id,
            "is_active": active,
        }
        if created_at is not None:
            values["created_at"] = created_at

        statement = pg_insert(subscribers).values(**values)
        statement = statement.on_conflict_do_update(
            index_elements=[subscribers.c.telegram_chat_id],
            set_={
                "is_active": sa.or_(
                    subscribers.c.is_active,
                    statement.excluded.is_active,
                ),
                "updated_at": sa.func.now(),
            },
        ).returning(subscribers.c.id)
        subscriber_id = await connection.scalar(statement)
        if subscriber_id is None:
            raise RuntimeError("Subscriber upsert returned no identifier")
        return int(subscriber_id)

    async def deactivate(self, connection: AsyncConnection, telegram_chat_id: int) -> bool:
        result = await connection.execute(
            sa.update(subscribers)
            .where(subscribers.c.telegram_chat_id == telegram_chat_id)
            .values(is_active=False, updated_at=sa.func.now())
        )
        return result.rowcount == 1

    async def list_active(self, connection: AsyncConnection) -> list[int]:
        rows = await connection.execute(
            sa.select(subscribers.c.telegram_chat_id)
            .where(subscribers.c.is_active.is_(True))
            .order_by(subscribers.c.created_at, subscribers.c.id)
        )
        return [int(row.telegram_chat_id) for row in rows]

    async def count(self, connection: AsyncConnection) -> int:
        return int(await connection.scalar(sa.select(sa.func.count()).select_from(subscribers)) or 0)


class LegacyCompatibilityRepository:
    async def upsert_message(
        self,
        connection: AsyncConnection,
        *,
        import_run_id: UUID,
        legacy_lead_id: int,
        source_key: str,
        telegram_message_id: int,
        state: str,
        processed_at: datetime | None,
        legacy_created_at: datetime,
    ) -> int:
        statement = pg_insert(legacy_processed_messages).values(
            first_import_run_id=import_run_id,
            legacy_lead_id=legacy_lead_id,
            source_key=source_key,
            telegram_message_id=telegram_message_id,
            state=state,
            processed_at=processed_at,
            legacy_created_at=legacy_created_at,
        )
        statement = statement.on_conflict_do_update(
            constraint="uq_legacy_processed_messages_source_message",
            set_={
                "state": sa.case(
                    (statement.excluded.state == "processed", "processed"),
                    else_=legacy_processed_messages.c.state,
                ),
                "processed_at": sa.case(
                    (
                        statement.excluded.state == "processed",
                        sa.func.coalesce(
                            legacy_processed_messages.c.processed_at,
                            statement.excluded.processed_at,
                        ),
                    ),
                    else_=legacy_processed_messages.c.processed_at,
                ),
            },
        ).returning(legacy_processed_messages.c.id)
        message_id = await connection.scalar(statement)
        if message_id is None:
            raise RuntimeError("Legacy message upsert returned no identifier")
        return int(message_id)

    async def is_processed(
        self,
        connection: AsyncConnection,
        source_key: str,
        telegram_message_id: int,
    ) -> bool:
        state = await connection.scalar(
            sa.select(legacy_processed_messages.c.state).where(
                legacy_processed_messages.c.source_key == source_key,
                legacy_processed_messages.c.telegram_message_id == telegram_message_id,
            )
        )
        return state == "processed"

    async def count_messages(self, connection: AsyncConnection) -> int:
        return int(
            await connection.scalar(
                sa.select(sa.func.count()).select_from(legacy_processed_messages)
            )
            or 0
        )

    async def count_delivery_eligible_messages(self, connection: AsyncConnection) -> int:
        return int(
            await connection.scalar(
                sa.select(sa.func.count())
                .select_from(legacy_processed_messages)
                .where(legacy_processed_messages.c.state == "pending")
            )
            or 0
        )

    async def record_sent_delivery(
        self,
        connection: AsyncConnection,
        *,
        legacy_processed_message_id: int,
        subscriber_id: int,
        telegram_message_id: int,
        sent_at: datetime,
    ) -> int:
        statement = pg_insert(legacy_recipient_deliveries).values(
            legacy_processed_message_id=legacy_processed_message_id,
            subscriber_id=subscriber_id,
            state="sent",
            telegram_message_id=telegram_message_id,
            attempt_count=1,
            sent_at=sent_at,
        )
        statement = statement.on_conflict_do_update(
            constraint="uq_legacy_recipient_deliveries_message_subscriber",
            set_={
                "state": "sent",
                "telegram_message_id": statement.excluded.telegram_message_id,
                "attempt_count": sa.func.greatest(
                    legacy_recipient_deliveries.c.attempt_count,
                    1,
                ),
                "sent_at": statement.excluded.sent_at,
                "error_code": None,
                "updated_at": sa.func.now(),
            },
            where=sa.or_(
                legacy_recipient_deliveries.c.telegram_message_id.is_(None),
                legacy_recipient_deliveries.c.telegram_message_id
                == statement.excluded.telegram_message_id,
            ),
        ).returning(legacy_recipient_deliveries.c.id)
        delivery_id = await connection.scalar(statement)
        if delivery_id is None:
            raise DeliveryEvidenceConflict(
                "Conflicting Telegram message identifiers for one legacy recipient delivery"
            )
        return int(delivery_id)

    async def count_deliveries(self, connection: AsyncConnection) -> int:
        return int(
            await connection.scalar(
                sa.select(sa.func.count()).select_from(legacy_recipient_deliveries)
            )
            or 0
        )
