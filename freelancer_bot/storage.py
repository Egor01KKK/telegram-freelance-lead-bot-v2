from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


@dataclass(frozen=True)
class LeadRecord:
    source: str
    message_id: int
    link: str
    text: str
    score: int
    keywords: tuple[str, ...]
    message_date: str


@dataclass(frozen=True)
class StoredLead:
    id: int
    source: str
    message_id: int
    link: str
    text: str
    score: int
    keywords: tuple[str, ...]
    message_date: str
    status: str
    ai_draft_json: str | None
    notification_chat_id: int | None
    notification_message_id: int | None


class Storage:
    def __init__(self, path: Path):
        path.parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(path)
        self._conn.row_factory = sqlite3.Row
        self._init_schema()

    def _init_schema(self) -> None:
        self._conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS subscribers (
                chat_id INTEGER PRIMARY KEY,
                created_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS leads (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                source TEXT NOT NULL,
                message_id INTEGER NOT NULL,
                link TEXT NOT NULL,
                text TEXT NOT NULL,
                score INTEGER NOT NULL,
                keywords_json TEXT NOT NULL,
                message_date TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'new',
                ai_draft_json TEXT,
                draft_requested_at TEXT,
                draft_ready_at TEXT,
                notification_chat_id INTEGER,
                notification_message_id INTEGER,
                notified_at TEXT,
                created_at TEXT NOT NULL,
                UNIQUE(source, message_id)
            );
            """
        )
        self._ensure_column("leads", "status", "TEXT NOT NULL DEFAULT 'new'")
        self._ensure_column("leads", "ai_draft_json", "TEXT")
        self._ensure_column("leads", "draft_requested_at", "TEXT")
        self._ensure_column("leads", "draft_ready_at", "TEXT")
        self._ensure_column("leads", "notification_chat_id", "INTEGER")
        self._ensure_column("leads", "notification_message_id", "INTEGER")
        self._conn.commit()

    def _ensure_column(self, table: str, column: str, definition: str) -> None:
        rows = self._conn.execute(f"PRAGMA table_info({table})").fetchall()
        if column in {row["name"] for row in rows}:
            return
        self._conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {definition}")

    def close(self) -> None:
        self._conn.close()

    def add_subscriber(self, chat_id: int) -> None:
        self._conn.execute(
            """
            INSERT INTO subscribers(chat_id, created_at)
            VALUES(?, ?)
            ON CONFLICT(chat_id) DO NOTHING
            """,
            (chat_id, utc_now()),
        )
        self._conn.commit()

    def remove_subscriber(self, chat_id: int) -> None:
        self._conn.execute("DELETE FROM subscribers WHERE chat_id = ?", (chat_id,))
        self._conn.commit()

    def subscribers(self) -> list[int]:
        rows = self._conn.execute("SELECT chat_id FROM subscribers ORDER BY created_at").fetchall()
        return [int(row["chat_id"]) for row in rows]

    def record_or_should_retry(self, lead: LeadRecord) -> int | None:
        existing = self._conn.execute(
            "SELECT id, notified_at FROM leads WHERE source = ? AND message_id = ?",
            (lead.source, lead.message_id),
        ).fetchone()
        if existing:
            return int(existing["id"]) if existing["notified_at"] is None else None

        cursor = self._conn.execute(
            """
            INSERT INTO leads(
                source, message_id, link, text, score, keywords_json,
                message_date, status, notified_at, created_at
            )
            VALUES(?, ?, ?, ?, ?, ?, ?, 'new', NULL, ?)
            """,
            (
                lead.source,
                lead.message_id,
                lead.link,
                lead.text,
                lead.score,
                json.dumps(list(lead.keywords), ensure_ascii=False),
                lead.message_date,
                utc_now(),
            ),
        )
        self._conn.commit()
        return int(cursor.lastrowid)

    def get_lead(self, lead_id: int) -> StoredLead | None:
        row = self._conn.execute("SELECT * FROM leads WHERE id = ?", (lead_id,)).fetchone()
        if row is None:
            return None
        return self._row_to_stored_lead(row)

    def mark_draft_requested(self, lead_id: int) -> None:
        self._conn.execute(
            "UPDATE leads SET status = ?, draft_requested_at = ? WHERE id = ?",
            ("draft_requested", utc_now(), lead_id),
        )
        self._conn.commit()

    def save_ai_draft(self, lead_id: int, draft: dict) -> None:
        self._conn.execute(
            "UPDATE leads SET status = ?, ai_draft_json = ?, draft_ready_at = ? WHERE id = ?",
            ("draft_ready", json.dumps(draft, ensure_ascii=False), utc_now(), lead_id),
        )
        self._conn.commit()

    def mark_ignored(self, lead_id: int) -> None:
        self._conn.execute("UPDATE leads SET status = ? WHERE id = ?", ("ignored", lead_id))
        self._conn.commit()

    def get_ai_draft(self, lead_id: int) -> dict | None:
        row = self._conn.execute("SELECT ai_draft_json FROM leads WHERE id = ?", (lead_id,)).fetchone()
        if row is None or not row["ai_draft_json"]:
            return None
        return json.loads(row["ai_draft_json"])

    def mark_notified(self, source: str, message_id: int) -> None:
        self._conn.execute(
            "UPDATE leads SET notified_at = ? WHERE source = ? AND message_id = ?",
            (utc_now(), source, message_id),
        )
        self._conn.commit()

    def mark_notification_message(
        self, lead_id: int, chat_id: int, telegram_message_id: int
    ) -> None:
        self._conn.execute(
            """
            UPDATE leads
            SET notification_chat_id = ?, notification_message_id = ?, notified_at = ?
            WHERE id = ?
            """,
            (chat_id, telegram_message_id, utc_now(), lead_id),
        )
        self._conn.commit()

    def stats(self) -> dict[str, int]:
        lead_count = self._conn.execute("SELECT COUNT(*) AS count FROM leads").fetchone()["count"]
        pending_count = self._conn.execute(
            "SELECT COUNT(*) AS count FROM leads WHERE notified_at IS NULL"
        ).fetchone()["count"]
        subscriber_count = self._conn.execute("SELECT COUNT(*) AS count FROM subscribers").fetchone()[
            "count"
        ]
        return {
            "leads": int(lead_count),
            "pending": int(pending_count),
            "subscribers": int(subscriber_count),
        }

    def add_initial_subscribers(self, chat_ids: Iterable[int]) -> None:
        for chat_id in chat_ids:
            self.add_subscriber(chat_id)

    def _row_to_stored_lead(self, row: sqlite3.Row) -> StoredLead:
        return StoredLead(
            id=int(row["id"]),
            source=str(row["source"]),
            message_id=int(row["message_id"]),
            link=str(row["link"]),
            text=str(row["text"]),
            score=int(row["score"]),
            keywords=tuple(json.loads(row["keywords_json"])),
            message_date=str(row["message_date"]),
            status=str(row["status"]),
            ai_draft_json=row["ai_draft_json"],
            notification_chat_id=(
                int(row["notification_chat_id"]) if row["notification_chat_id"] is not None else None
            ),
            notification_message_id=(
                int(row["notification_message_id"]) if row["notification_message_id"] is not None else None
            ),
        )
