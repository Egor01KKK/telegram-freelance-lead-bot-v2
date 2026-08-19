import asyncio
import unittest
from datetime import datetime, timezone
from uuid import uuid4

import sqlalchemy as sa
from sqlalchemy.exc import IntegrityError

from freelancer_bot.persistence.database import Database
from freelancer_bot.persistence.repositories import (
    DeliveryEvidenceConflict,
    LegacyCompatibilityRepository,
    SubscriberRepository,
)
from freelancer_bot.persistence.schema import legacy_import_runs, subscribers
from postgres_support import TEST_DATABASE_URL, migrate_to_head, temporary_database


@unittest.skipUnless(TEST_DATABASE_URL, "TEST_DATABASE_URL is not configured")
class PostgresRepositoryTest(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.database_context = temporary_database()
        self.database_url = self.database_context.__enter__()
        migrate_to_head(self.database_url)
        self.database = Database(self.database_url, pool_size=4, max_overflow=8)
        self.subscribers = SubscriberRepository()
        self.compatibility = LegacyCompatibilityRepository()

    async def asyncTearDown(self):
        await self.database.close()
        self.database_context.__exit__(None, None, None)

    async def test_async_query_commit_and_rollback(self):
        async with self.database.connect() as connection:
            value = await connection.scalar(sa.text("SELECT CAST(:value AS integer)"), {"value": 42})
        self.assertEqual(value, 42)

        async with self.database.transaction() as connection:
            await self.subscribers.ensure(connection, 101, active=True)

        with self.assertRaisesRegex(RuntimeError, "rollback"):
            async with self.database.transaction() as connection:
                await self.subscribers.ensure(connection, 202, active=True)
                raise RuntimeError("force rollback")

        async with self.database.connect() as connection:
            self.assertEqual(await self.subscribers.list_active(connection), [101])

    async def test_database_constraints_reject_duplicate_subscriber(self):
        async with self.database.transaction() as connection:
            await connection.execute(
                sa.insert(subscribers).values(telegram_chat_id=101, is_active=True)
            )

        with self.assertRaises(IntegrityError):
            async with self.database.transaction() as connection:
                await connection.execute(
                    sa.insert(subscribers).values(telegram_chat_id=101, is_active=True)
                )

    async def test_concurrent_subscriber_upserts_create_one_row(self):
        async def upsert_once() -> int:
            async with self.database.transaction() as connection:
                return await self.subscribers.ensure(connection, 303, active=True)

        identifiers = await asyncio.gather(*(upsert_once() for _ in range(12)))

        self.assertEqual(len(set(identifiers)), 1)
        async with self.database.connect() as connection:
            self.assertEqual(await self.subscribers.count(connection), 1)

    async def test_concurrent_message_upserts_preserve_processed_idempotency(self):
        run_id = uuid4()
        now = datetime.now(timezone.utc)
        async with self.database.transaction() as connection:
            await connection.execute(
                sa.insert(legacy_import_runs).values(
                    id=run_id,
                    source_sha256="a" * 64,
                    source_size_bytes=1,
                    attempt_number=1,
                    status="running",
                )
            )

        async def upsert_once() -> int:
            async with self.database.transaction() as connection:
                return await self.compatibility.upsert_message(
                    connection,
                    import_run_id=run_id,
                    legacy_lead_id=1,
                    source_key="@source",
                    telegram_message_id=77,
                    state="processed",
                    processed_at=now,
                    legacy_created_at=now,
                )

        identifiers = await asyncio.gather(*(upsert_once() for _ in range(12)))

        self.assertEqual(len(set(identifiers)), 1)
        async with self.database.connect() as connection:
            self.assertEqual(await self.compatibility.count_messages(connection), 1)
            self.assertTrue(await self.compatibility.is_processed(connection, "@source", 77))
            self.assertEqual(
                await self.compatibility.count_delivery_eligible_messages(connection),
                0,
            )

    async def test_recipient_delivery_pair_is_idempotent_and_rejects_conflicting_evidence(self):
        run_id = uuid4()
        now = datetime.now(timezone.utc)
        async with self.database.transaction() as connection:
            await connection.execute(
                sa.insert(legacy_import_runs).values(
                    id=run_id,
                    source_sha256="b" * 64,
                    source_size_bytes=1,
                    attempt_number=1,
                    status="running",
                )
            )
            subscriber_id = await self.subscribers.ensure(connection, 505, active=True)
            message_id = await self.compatibility.upsert_message(
                connection,
                import_run_id=run_id,
                legacy_lead_id=1,
                source_key="@delivery_source",
                telegram_message_id=88,
                state="processed",
                processed_at=now,
                legacy_created_at=now,
            )
            first = await self.compatibility.record_sent_delivery(
                connection,
                legacy_processed_message_id=message_id,
                subscriber_id=subscriber_id,
                telegram_message_id=901,
                sent_at=now,
            )
            repeated = await self.compatibility.record_sent_delivery(
                connection,
                legacy_processed_message_id=message_id,
                subscriber_id=subscriber_id,
                telegram_message_id=901,
                sent_at=now,
            )

        self.assertEqual(first, repeated)
        with self.assertRaises(DeliveryEvidenceConflict):
            async with self.database.transaction() as connection:
                await self.compatibility.record_sent_delivery(
                    connection,
                    legacy_processed_message_id=message_id,
                    subscriber_id=subscriber_id,
                    telegram_message_id=902,
                    sent_at=now,
                )

        async with self.database.connect() as connection:
            self.assertEqual(await self.compatibility.count_deliveries(connection), 1)


if __name__ == "__main__":
    unittest.main()
