from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import sqlite3
import sys
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import quote
from uuid import UUID, uuid4

import sqlalchemy as sa
from sqlalchemy.ext.asyncio import AsyncConnection

from freelancer_bot.config import RuntimeConfig, RuntimeMode

from .database import Database
from .repositories import LegacyCompatibilityRepository, SubscriberRepository
from .schema import legacy_import_runs


IMPORT_LOCK_NAME = "freelancer_bot:legacy_sqlite_import:v1"


class LegacySnapshotError(ValueError):
    pass


class LegacyImportFailed(RuntimeError):
    def __init__(self, error_code: str):
        super().__init__(f"Legacy SQLite import failed ({error_code})")
        self.error_code = error_code


@dataclass(frozen=True)
class LegacySubscriber:
    telegram_chat_id: int
    created_at: datetime


@dataclass(frozen=True)
class LegacyMessageMarker:
    legacy_lead_id: int
    source_key: str
    telegram_message_id: int
    state: str
    processed_at: datetime | None
    legacy_created_at: datetime


@dataclass(frozen=True)
class LegacyDeliveryEvidence:
    legacy_lead_id: int
    telegram_chat_id: int
    telegram_message_id: int
    sent_at: datetime


@dataclass(frozen=True)
class LegacySnapshot:
    source_sha256: str
    source_size_bytes: int
    subscribers: tuple[LegacySubscriber, ...]
    messages: tuple[LegacyMessageMarker, ...]
    deliveries: tuple[LegacyDeliveryEvidence, ...]


@dataclass(frozen=True)
class LegacyImportResult:
    run_id: UUID
    status: str
    source_sha256: str
    subscribers_seen: int
    messages_seen: int
    deliveries_seen: int


def read_legacy_snapshot(protected_copy: Path) -> LegacySnapshot:
    path = protected_copy.resolve(strict=True)
    if protected_copy.is_symlink() or not path.is_file():
        raise LegacySnapshotError("Legacy SQLite input must be a regular protected copy")

    digest_before = _sha256(path)
    source_size = path.stat().st_size
    uri = f"file:{quote(str(path), safe='/')}?mode=ro&immutable=1"
    connection = sqlite3.connect(uri, uri=True)
    connection.row_factory = sqlite3.Row
    try:
        _validate_legacy_schema(connection)
        subscriber_rows = connection.execute(
            "SELECT chat_id, created_at FROM subscribers ORDER BY created_at, chat_id"
        ).fetchall()
        lead_rows = connection.execute(
            """
            SELECT id, source, message_id, notified_at, created_at,
                   notification_chat_id, notification_message_id
            FROM leads
            ORDER BY id
            """
        ).fetchall()
    finally:
        connection.close()

    digest_after = _sha256(path)
    if digest_before != digest_after:
        raise LegacySnapshotError("Legacy SQLite copy changed while it was being read")

    subscribers = tuple(
        LegacySubscriber(
            telegram_chat_id=int(row["chat_id"]),
            created_at=_timestamp(row["created_at"], field="subscribers.created_at"),
        )
        for row in subscriber_rows
    )
    messages: list[LegacyMessageMarker] = []
    deliveries: list[LegacyDeliveryEvidence] = []
    for row in lead_rows:
        notified_at = _optional_timestamp(row["notified_at"], field="leads.notified_at")
        marker = LegacyMessageMarker(
            legacy_lead_id=int(row["id"]),
            source_key=str(row["source"]),
            telegram_message_id=int(row["message_id"]),
            state="processed" if notified_at is not None else "pending",
            processed_at=notified_at,
            legacy_created_at=_timestamp(row["created_at"], field="leads.created_at"),
        )
        messages.append(marker)

        notification_chat_id = row["notification_chat_id"]
        notification_message_id = row["notification_message_id"]
        if (
            notified_at is not None
            and notification_chat_id is not None
            and notification_message_id is not None
        ):
            deliveries.append(
                LegacyDeliveryEvidence(
                    legacy_lead_id=marker.legacy_lead_id,
                    telegram_chat_id=int(notification_chat_id),
                    telegram_message_id=int(notification_message_id),
                    sent_at=notified_at,
                )
            )

    return LegacySnapshot(
        source_sha256=digest_before,
        source_size_bytes=source_size,
        subscribers=subscribers,
        messages=tuple(messages),
        deliveries=tuple(deliveries),
    )


class LegacySQLiteImporter:
    def __init__(self, database: Database) -> None:
        self._database = database
        self._subscribers = SubscriberRepository()
        self._compatibility = LegacyCompatibilityRepository()

    async def import_copy(self, protected_copy: Path) -> LegacyImportResult:
        return await self.import_snapshot(read_legacy_snapshot(protected_copy))

    async def import_snapshot(self, snapshot: LegacySnapshot) -> LegacyImportResult:
        run_id: UUID | None = None
        async with self._database.connect() as connection:
            await _acquire_import_lock(connection)
            try:
                existing = await self._start_or_find_completed(connection, snapshot)
                if existing is not None:
                    return existing

                run_id = await _latest_running_run_id(connection, snapshot.source_sha256)
                if run_id is None:
                    raise RuntimeError("Committed legacy import run was not found")

                async with connection.begin():
                    await self._import_data(connection, run_id, snapshot)

                async with connection.begin():
                    await connection.execute(
                        sa.update(legacy_import_runs)
                        .where(
                            legacy_import_runs.c.id == run_id,
                            legacy_import_runs.c.status == "running",
                        )
                        .values(
                            status="completed",
                            subscribers_seen=len(snapshot.subscribers),
                            messages_seen=len(snapshot.messages),
                            deliveries_seen=len(snapshot.deliveries),
                            error_code=None,
                            finished_at=sa.func.now(),
                        )
                    )

                return LegacyImportResult(
                    run_id=run_id,
                    status="completed",
                    source_sha256=snapshot.source_sha256,
                    subscribers_seen=len(snapshot.subscribers),
                    messages_seen=len(snapshot.messages),
                    deliveries_seen=len(snapshot.deliveries),
                )
            except Exception as exc:
                if connection.in_transaction():
                    await connection.rollback()
                error_code = _sanitized_error_code(exc)
                if run_id is not None:
                    async with connection.begin():
                        await connection.execute(
                            sa.update(legacy_import_runs)
                            .where(
                                legacy_import_runs.c.id == run_id,
                                legacy_import_runs.c.status == "running",
                            )
                            .values(
                                status="failed",
                                error_code=error_code,
                                finished_at=sa.func.now(),
                            )
                        )
                raise LegacyImportFailed(error_code) from exc
            finally:
                if connection.in_transaction():
                    await connection.rollback()
                await _release_import_lock(connection)

    async def _start_or_find_completed(
        self,
        connection: AsyncConnection,
        snapshot: LegacySnapshot,
    ) -> LegacyImportResult | None:
        async with connection.begin():
            existing = (
                await connection.execute(
                    sa.select(legacy_import_runs)
                    .where(
                        legacy_import_runs.c.source_sha256 == snapshot.source_sha256,
                        legacy_import_runs.c.status == "completed",
                    )
                    .limit(1)
                )
            ).mappings().first()
            if existing is not None:
                return LegacyImportResult(
                    run_id=existing["id"],
                    status="already_completed",
                    source_sha256=existing["source_sha256"],
                    subscribers_seen=int(existing["subscribers_seen"]),
                    messages_seen=int(existing["messages_seen"]),
                    deliveries_seen=int(existing["deliveries_seen"]),
                )

            attempt_number = int(
                await connection.scalar(
                    sa.select(
                        sa.func.coalesce(sa.func.max(legacy_import_runs.c.attempt_number), 0) + 1
                    ).where(legacy_import_runs.c.source_sha256 == snapshot.source_sha256)
                )
                or 1
            )
            run_id = uuid4()
            await connection.execute(
                sa.insert(legacy_import_runs).values(
                    id=run_id,
                    source_sha256=snapshot.source_sha256,
                    source_size_bytes=snapshot.source_size_bytes,
                    attempt_number=attempt_number,
                    status="running",
                )
            )
        return None

    async def _import_data(
        self,
        connection: AsyncConnection,
        run_id: UUID,
        snapshot: LegacySnapshot,
    ) -> None:
        subscriber_ids: dict[int, int] = {}
        for subscriber in snapshot.subscribers:
            subscriber_ids[subscriber.telegram_chat_id] = await self._subscribers.ensure(
                connection,
                subscriber.telegram_chat_id,
                active=True,
                created_at=subscriber.created_at,
            )

        message_ids: dict[int, int] = {}
        for marker in snapshot.messages:
            message_ids[marker.legacy_lead_id] = await self._compatibility.upsert_message(
                connection,
                import_run_id=run_id,
                legacy_lead_id=marker.legacy_lead_id,
                source_key=marker.source_key,
                telegram_message_id=marker.telegram_message_id,
                state=marker.state,
                processed_at=marker.processed_at,
                legacy_created_at=marker.legacy_created_at,
            )

        for delivery in snapshot.deliveries:
            subscriber_id = subscriber_ids.get(delivery.telegram_chat_id)
            if subscriber_id is None:
                subscriber_id = await self._subscribers.ensure(
                    connection,
                    delivery.telegram_chat_id,
                    active=False,
                )
                subscriber_ids[delivery.telegram_chat_id] = subscriber_id
            message_id = message_ids.get(delivery.legacy_lead_id)
            if message_id is None:
                raise LegacySnapshotError("Delivery evidence references an unknown legacy lead")
            await self._compatibility.record_sent_delivery(
                connection,
                legacy_processed_message_id=message_id,
                subscriber_id=subscriber_id,
                telegram_message_id=delivery.telegram_message_id,
                sent_at=delivery.sent_at,
            )


async def _acquire_import_lock(connection: AsyncConnection) -> None:
    await connection.execute(
        sa.text("SELECT pg_advisory_lock(hashtext(:lock_name))"),
        {"lock_name": IMPORT_LOCK_NAME},
    )
    await connection.commit()


async def _release_import_lock(connection: AsyncConnection) -> None:
    await connection.execute(
        sa.text("SELECT pg_advisory_unlock(hashtext(:lock_name))"),
        {"lock_name": IMPORT_LOCK_NAME},
    )
    await connection.commit()


async def _latest_running_run_id(
    connection: AsyncConnection,
    source_sha256: str,
) -> UUID | None:
    async with connection.begin():
        value = await connection.scalar(
            sa.select(legacy_import_runs.c.id)
            .where(
                legacy_import_runs.c.source_sha256 == source_sha256,
                legacy_import_runs.c.status == "running",
            )
            .order_by(legacy_import_runs.c.attempt_number.desc())
            .limit(1)
        )
    return value


def _validate_legacy_schema(connection: sqlite3.Connection) -> None:
    required = {
        "subscribers": {"chat_id", "created_at"},
        "leads": {
            "id",
            "source",
            "message_id",
            "notified_at",
            "created_at",
            "notification_chat_id",
            "notification_message_id",
        },
    }
    tables = {
        row[0]
        for row in connection.execute(
            "SELECT name FROM sqlite_master WHERE type = 'table'"
        ).fetchall()
    }
    for table, required_columns in required.items():
        if table not in tables:
            raise LegacySnapshotError(f"Legacy SQLite copy is missing required table: {table}")
        columns = {
            str(row[1]) for row in connection.execute(f"PRAGMA table_info({table})").fetchall()
        }
        missing = sorted(required_columns - columns)
        if missing:
            raise LegacySnapshotError(
                f"Legacy SQLite table {table} is missing required columns: {', '.join(missing)}"
            )


def _timestamp(value: object, *, field: str) -> datetime:
    if not isinstance(value, str) or not value.strip():
        raise LegacySnapshotError(f"Legacy SQLite field {field} must contain a timestamp")
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        raise LegacySnapshotError(f"Legacy SQLite field {field} contains an invalid timestamp") from None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _optional_timestamp(value: object, *, field: str) -> datetime | None:
    if value is None:
        return None
    return _timestamp(value, field=field)


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _sanitized_error_code(error: Exception) -> str:
    return type(error).__name__[:64] or "ImportError"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Import a protected V1 SQLite copy into PostgreSQL")
    parser.add_argument("--sqlite-copy", required=True, type=Path)
    arguments = parser.parse_args(argv)

    config = RuntimeConfig.from_env(mode=RuntimeMode.DATABASE)

    async def run() -> LegacyImportResult:
        database = Database(config.postgresql_url())
        try:
            return await LegacySQLiteImporter(database).import_copy(arguments.sqlite_copy)
        finally:
            await database.close()

    try:
        result = asyncio.run(run())
    except (LegacySnapshotError, LegacyImportFailed) as exc:
        print(str(exc), file=sys.stderr)
        return 1
    print(json.dumps(asdict(result), default=str, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
