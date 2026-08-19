import asyncio
import hashlib
import shutil
import sqlite3
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

import sqlalchemy as sa

from freelancer_bot.persistence.database import Database
from freelancer_bot.persistence.legacy_import import (
    LegacyImportFailed,
    LegacyMessageMarker,
    LegacySQLiteImporter,
    LegacySnapshot,
    LegacySubscriber,
    read_legacy_snapshot,
)
from freelancer_bot.persistence.repositories import (
    LegacyCompatibilityRepository,
    SubscriberRepository,
)
from freelancer_bot.persistence.schema import legacy_import_runs
from postgres_support import TEST_DATABASE_URL, migrate_to_head, temporary_database


ROOT = Path(__file__).resolve().parents[1]
REAL_LEGACY_DATABASE = ROOT / "job_parser.db"


class LegacySnapshotReaderTest(unittest.TestCase):
    def test_reads_only_explicit_delivery_evidence(self):
        with tempfile.TemporaryDirectory() as temporary_directory:
            path = Path(temporary_directory) / "legacy-copy.db"
            _create_legacy_fixture(path)

            snapshot = read_legacy_snapshot(path)

        self.assertEqual(len(snapshot.subscribers), 1)
        self.assertEqual(len(snapshot.messages), 3)
        self.assertEqual(len(snapshot.deliveries), 1)
        self.assertEqual(
            [message.state for message in snapshot.messages],
            ["processed", "processed", "pending"],
        )


@unittest.skipUnless(TEST_DATABASE_URL, "TEST_DATABASE_URL is not configured")
class LegacySQLiteImportTest(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.database_context = temporary_database()
        self.database_url = self.database_context.__enter__()
        migrate_to_head(self.database_url)
        self.database = Database(self.database_url, pool_size=4, max_overflow=4)
        self.importer = LegacySQLiteImporter(self.database)
        self.subscribers = SubscriberRepository()
        self.compatibility = LegacyCompatibilityRepository()

    async def asyncTearDown(self):
        await self.database.close()
        self.database_context.__exit__(None, None, None)

    async def test_import_success_and_identical_snapshot_rerun(self):
        with tempfile.TemporaryDirectory() as temporary_directory:
            path = Path(temporary_directory) / "legacy-copy.db"
            _create_legacy_fixture(path)

            first = await self.importer.import_copy(path)
            second = await self.importer.import_copy(path)

        self.assertEqual(first.status, "completed")
        self.assertEqual(second.status, "already_completed")
        self.assertEqual(first.run_id, second.run_id)
        async with self.database.connect() as connection:
            self.assertEqual(await self.subscribers.count(connection), 1)
            self.assertEqual(await self.compatibility.count_messages(connection), 3)
            self.assertEqual(await self.compatibility.count_deliveries(connection), 1)
            self.assertEqual(
                await self.compatibility.count_delivery_eligible_messages(connection),
                1,
            )
            run_count = await connection.scalar(
                sa.select(sa.func.count()).select_from(legacy_import_runs)
            )
            self.assertEqual(run_count, 1)

    async def test_import_failure_rolls_back_data_and_persists_sanitized_failure(self):
        now = datetime.now(timezone.utc)
        snapshot = LegacySnapshot(
            source_sha256="f" * 64,
            source_size_bytes=1,
            subscribers=(LegacySubscriber(telegram_chat_id=404, created_at=now),),
            messages=(
                LegacyMessageMarker(
                    legacy_lead_id=1,
                    source_key="",
                    telegram_message_id=1,
                    state="pending",
                    processed_at=None,
                    legacy_created_at=now,
                ),
            ),
            deliveries=(),
        )

        with self.assertRaises(LegacyImportFailed) as raised:
            await self.importer.import_snapshot(snapshot)

        self.assertEqual(raised.exception.error_code, "IntegrityError")
        async with self.database.connect() as connection:
            self.assertEqual(await self.subscribers.count(connection), 0)
            self.assertEqual(await self.compatibility.count_messages(connection), 0)
            run = (
                await connection.execute(sa.select(legacy_import_runs))
            ).mappings().one()
            self.assertEqual(run["status"], "failed")
            self.assertEqual(run["error_code"], "IntegrityError")
            self.assertIsNotNone(run["finished_at"])

    async def test_concurrent_imports_are_serialized_and_import_once(self):
        with tempfile.TemporaryDirectory() as temporary_directory:
            path = Path(temporary_directory) / "legacy-copy.db"
            _create_legacy_fixture(path)

            results = await asyncio.gather(
                self.importer.import_copy(path),
                self.importer.import_copy(path),
            )

        self.assertEqual(
            sorted(result.status for result in results),
            ["already_completed", "completed"],
        )
        async with self.database.connect() as connection:
            self.assertEqual(await self.compatibility.count_messages(connection), 3)
            run_count = await connection.scalar(
                sa.select(sa.func.count()).select_from(legacy_import_runs)
            )
            self.assertEqual(run_count, 1)

    @unittest.skipUnless(REAL_LEGACY_DATABASE.exists(), "Real legacy SQLite database is absent")
    async def test_real_protected_copy_imports_exact_compatibility_state(self):
        original_hash = _sha256(REAL_LEGACY_DATABASE)
        with tempfile.TemporaryDirectory() as temporary_directory:
            protected_copy = Path(temporary_directory) / "job_parser-protected.db"
            shutil.copy2(REAL_LEGACY_DATABASE, protected_copy)
            protected_copy.chmod(0o600)
            source_snapshot = read_legacy_snapshot(protected_copy)

            result = await self.importer.import_copy(protected_copy)

        self.assertGreater(len(source_snapshot.messages), 0)
        self.assertEqual(result.messages_seen, len(source_snapshot.messages))
        self.assertEqual(result.subscribers_seen, len(source_snapshot.subscribers))
        self.assertEqual(result.deliveries_seen, len(source_snapshot.deliveries))
        self.assertEqual(_sha256(REAL_LEGACY_DATABASE), original_hash)
        async with self.database.connect() as connection:
            self.assertEqual(
                await self.compatibility.count_messages(connection),
                len(source_snapshot.messages),
            )
            self.assertEqual(
                await self.subscribers.count(connection),
                len(source_snapshot.subscribers),
            )
            self.assertEqual(
                await self.compatibility.count_deliveries(connection),
                len(source_snapshot.deliveries),
            )
            self.assertEqual(
                await self.compatibility.count_delivery_eligible_messages(connection),
                sum(message.state == "pending" for message in source_snapshot.messages),
            )


def _create_legacy_fixture(path: Path) -> None:
    connection = sqlite3.connect(path)
    try:
        connection.executescript(
            """
            CREATE TABLE subscribers (
                chat_id INTEGER PRIMARY KEY,
                created_at TEXT NOT NULL
            );
            CREATE TABLE leads (
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
        now = "2026-08-08T12:00:00+00:00"
        connection.execute(
            "INSERT INTO subscribers(chat_id, created_at) VALUES(?, ?)",
            (101, now),
        )
        rows = [
            ("@one", 1, now, None, None),
            ("@two", 2, now, 101, 900),
            ("@three", 3, None, None, None),
        ]
        for source, message_id, notified_at, chat_id, notification_message_id in rows:
            connection.execute(
                """
                INSERT INTO leads(
                    source, message_id, link, text, score, keywords_json,
                    message_date, status, notification_chat_id,
                    notification_message_id, notified_at, created_at
                )
                VALUES(?, ?, ?, ?, 7, '[]', ?, 'new', ?, ?, ?, ?)
                """,
                (
                    source,
                    message_id,
                    f"https://t.me/{source[1:]}/{message_id}",
                    "fixture",
                    now,
                    chat_id,
                    notification_message_id,
                    notified_at,
                    now,
                ),
            )
        connection.commit()
    finally:
        connection.close()


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


if __name__ == "__main__":
    unittest.main()
