from __future__ import annotations

from datetime import datetime, timezone
import io
import json
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock, patch
from uuid import UUID

from freelancer_bot.app import LeadBot, _transport_metadata
from freelancer_bot.observability import (
    Redactor,
    configure_structured_logger,
    current_trace_id,
)
from freelancer_bot.persistence.collector_accounts import (
    CollectorAccessStatus,
    CollectorAccountRepository,
)
from freelancer_bot.persistence.database import Database
from freelancer_bot.persistence.raw_messages import IneligibleRawMessageSource
from freelancer_bot.persistence.source_repository import SourceRepository, SourceStatus
from freelancer_bot.telegram_collector import (
    ApprovedTelegramSourceAdapter,
    TelegramCollectorSource,
)
from postgres_support import TEST_DATABASE_URL, migrate_to_head, temporary_database


NOW = datetime(2026, 8, 10, 12, 0, tzinfo=timezone.utc)
TRACE_ID = UUID("33333333-3333-3333-3333-333333333333")


class FakeTelegramSession:
    def __init__(self, account_id=70001):
        self.account_id = account_id
        self.get_me_calls = 0
        self.handlers = []

    async def get_me(self):
        self.get_me_calls += 1
        return SimpleNamespace(id=self.account_id)

    async def get_entity(self, lookup):
        return f"entity:{lookup}"

    def on(self, event_builder):
        def register(handler):
            self.handlers.append((event_builder, handler))
            return handler

        return register


@unittest.skipUnless(TEST_DATABASE_URL, "TEST_DATABASE_URL is not configured")
class ApprovedTelegramSourceAdapterTest(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.database_context = temporary_database()
        self.database_url = self.database_context.__enter__()
        migrate_to_head(self.database_url)
        self.database = Database(self.database_url, pool_size=4, max_overflow=8)
        self.sources = SourceRepository()
        self.accounts = CollectorAccountRepository()

    async def asyncTearDown(self):
        await self.database.close()
        self.database_context.__exit__(None, None, None)

    async def test_session_catalog_excludes_every_unapproved_or_inaccessible_source(self):
        public = await self._source("public", status=SourceStatus.APPROVED)
        await self._source("candidate", status=SourceStatus.CANDIDATE)
        await self._source("paused", status=SourceStatus.PAUSED)
        await self._source("rejected", status=SourceStatus.REJECTED)
        permitted = await self._source(
            "private-permitted",
            status=SourceStatus.APPROVED,
            access_type="private",
        )
        inaccessible = await self._source(
            "private-inaccessible",
            status=SourceStatus.APPROVED,
            access_type="private",
        )
        await self._source(
            "private-unverified",
            status=SourceStatus.APPROVED,
            access_type="private",
        )
        no_lookup = await self._source(
            "no-lookup",
            status=SourceStatus.APPROVED,
            handle=None,
            canonical_url=None,
        )
        canonical_only = await self._source(
            "canonical-only",
            status=SourceStatus.APPROVED,
            handle=None,
            canonical_url="https://t.me/g3_canonical_only",
        )
        invite_only = await self._source(
            "invite-only",
            status=SourceStatus.APPROVED,
            access_type="private",
            handle=None,
            canonical_url="https://t.me/+PRIVATE_INVITE_CANARY",
        )
        async with self.database.transaction() as connection:
            account = await self.accounts.ensure(
                connection,
                platform="telegram",
                external_account_id="70001",
                display_name="Existing Telethon account",
            )
            await self.accounts.record_source_access(
                connection,
                source_id=permitted.id,
                collector_account_id=account.id,
                access_status=CollectorAccessStatus.PERMITTED,
                checked_at=NOW,
                checked_by="operator:g3-test",
            )
            await self.accounts.record_source_access(
                connection,
                source_id=invite_only.id,
                collector_account_id=account.id,
                access_status=CollectorAccessStatus.PERMITTED,
                checked_at=NOW,
                checked_by="operator:g3-test",
            )
            await self.accounts.record_source_access(
                connection,
                source_id=inaccessible.id,
                collector_account_id=account.id,
                access_status=CollectorAccessStatus.INACCESSIBLE,
                checked_at=NOW,
                checked_by="operator:g3-test",
            )

        client = FakeTelegramSession()
        output = io.StringIO()
        configure_structured_logger(
            "freelancer_bot",
            redactor=Redactor(),
            stream=output,
        )
        snapshot = await ApprovedTelegramSourceAdapter(self.database).list_for_session(
            client
        )

        self.assertEqual(client.get_me_calls, 1)
        self.assertEqual(snapshot.collector_account.id, account.id)
        self.assertEqual(
            [target.record.id for target in snapshot.sources],
            [public.id, permitted.id, canonical_only.id],
        )
        self.assertNotIn(no_lookup.id, [target.record.id for target in snapshot.sources])
        self.assertNotIn(invite_only.id, [target.record.id for target in snapshot.sources])
        self.assertEqual(
            [target.lookup for target in snapshot.sources],
            [
                "@g3_public",
                "@g3_private-permitted",
                "https://t.me/g3_canonical_only",
            ],
        )
        self.assertEqual(snapshot.sources[-1].legacy_source().handle, "@g3_canonical_only")
        self.assertNotIn("PRIVATE_INVITE_CANARY", output.getvalue())

    async def test_inactive_session_account_fails_closed_without_json_fallback(self):
        await self._source("public", status=SourceStatus.APPROVED)
        async with self.database.transaction() as connection:
            account = await self.accounts.ensure(
                connection,
                platform="telegram",
                external_account_id="70001",
                display_name="Disabled account",
            )
            await self.accounts.set_active(connection, account.id, active=False)

        snapshot = await ApprovedTelegramSourceAdapter(self.database).list_for_session(
            FakeTelegramSession()
        )

        self.assertFalse(snapshot.collector_account.is_active)
        self.assertEqual(snapshot.sources, ())

    async def _source(
        self,
        suffix,
        *,
        status,
        access_type="public",
        handle="default",
        canonical_url="default",
    ):
        normalized_handle = None if handle is None else f"@g3_{suffix}"
        normalized_url = (
            None
            if canonical_url is None
            else (
                canonical_url
                if isinstance(canonical_url, str) and canonical_url != "default"
                else f"https://t.me/g3_{suffix}"
            )
        )
        async with self.database.transaction() as connection:
            source = await self.sources.create_candidate(
                connection,
                platform="telegram",
                external_id=f"g3:{suffix}",
                access_type=access_type,
                display_name=f"G3 source {suffix}",
                handle=normalized_handle,
                canonical_url=normalized_url,
                provider="g3_fixture",
                lineage_key=f"g3:{suffix}",
            )
            if status is SourceStatus.APPROVED or status is SourceStatus.PAUSED:
                source = await self.sources.transition(
                    connection,
                    source.id,
                    SourceStatus.APPROVED,
                    reason="G3 fixture approved",
                )
            if status is SourceStatus.PAUSED:
                source = await self.sources.transition(
                    connection,
                    source.id,
                    SourceStatus.PAUSED,
                    reason="G3 fixture paused",
                )
            if status is SourceStatus.REJECTED:
                source = await self.sources.transition(
                    connection,
                    source.id,
                    SourceStatus.REJECTED,
                    reason="G3 fixture rejected",
                )
            return source


class TelegramCollectorDispatchBoundaryTest(unittest.IsolatedAsyncioTestCase):
    def test_service_action_is_recorded_as_transport_metadata(self):
        class MessageActionChatAddUser:
            pass

        metadata = _transport_metadata(
            SimpleNamespace(action=MessageActionChatAddUser(), media=None)
        )

        self.assertEqual(
            metadata,
            {"service_action_type": "MessageActionChatAddUser"},
        )

    async def test_live_handler_dispatches_with_correlation_and_no_domain_logic(self):
        stream = io.StringIO()
        configure_structured_logger(
            "freelancer_bot",
            redactor=Redactor(),
            stream=stream,
        )
        client = FakeTelegramSession()
        source = _collector_source(91, "@g3_dispatch")
        bot = LeadBot.__new__(LeadBot)
        bot.sources = [source]
        bot.user_client = client
        bot.collector_account_id = 71
        bot.raw_ingestor = AsyncMock()
        bot.raw_ingestor.ingest.return_value = SimpleNamespace(
            message=SimpleNamespace(
                id=UUID("55555555-5555-5555-5555-555555555555"),
                processing_job_id=UUID("66666666-6666-6666-6666-666666666666"),
            ),
            created=True,
        )
        observed = []

        async def process(selected, message):
            observed.append((selected.record.id, message.id, current_trace_id()))

        bot._process_message = process
        active = await bot._register_source_handlers()
        message = SimpleNamespace(id=17, message="raw fixture", date=NOW)
        with patch("freelancer_bot.app.new_correlation_id", return_value=TRACE_ID):
            await client.handlers[0][1](SimpleNamespace(message=message))

        self.assertEqual(active, [(source, "entity:@g3_dispatch")])
        self.assertEqual(observed, [(91, 17, str(TRACE_ID))])
        raw_input = bot.raw_ingestor.ingest.await_args.args[0]
        self.assertEqual(raw_input.source_id, 91)
        self.assertEqual(raw_input.collector_account_id, 71)
        self.assertEqual(raw_input.external_message_id, 17)
        self.assertEqual(raw_input.message_url, "https://t.me/g3_dispatch/17")
        self.assertEqual(raw_input.content, "raw fixture")
        self.assertEqual(raw_input.correlation_id, TRACE_ID)
        events = [json.loads(line) for line in stream.getvalue().splitlines()]
        event = next(item for item in events if item["event"] == "telegram.collector.message_dispatched")
        self.assertEqual(event["correlation_id"], str(TRACE_ID))
        self.assertEqual(event["source_id"], 91)
        self.assertEqual(event["telegram_message_id"], 17)
        self.assertNotIn("raw fixture", stream.getvalue())

    async def test_ineligible_source_is_refused_before_legacy_processing(self):
        source = _collector_source(92, "@g3_refused")
        bot = LeadBot.__new__(LeadBot)
        bot.collector_account_id = 72
        bot.raw_ingestor = AsyncMock()
        bot.raw_ingestor.ingest.side_effect = IneligibleRawMessageSource("paused")
        bot._process_message = AsyncMock()

        await bot._dispatch_message(
            source,
            SimpleNamespace(id=18, message="must not reach V1", date=NOW),
            origin="catch_up",
        )

        bot._process_message.assert_not_awaited()

    async def test_postgres_failure_propagates_before_legacy_processing(self):
        source = _collector_source(93, "@g3_db_failure")
        bot = LeadBot.__new__(LeadBot)
        bot.collector_account_id = 73
        bot.raw_ingestor = AsyncMock()
        bot.raw_ingestor.ingest.side_effect = RuntimeError("postgres unavailable")
        bot._process_message = AsyncMock()

        with self.assertRaisesRegex(RuntimeError, "postgres unavailable"):
            await bot._dispatch_message(
                source,
                SimpleNamespace(id=19, message="must not reach V1", date=NOW),
                origin="live",
            )

        bot._process_message.assert_not_awaited()


def _collector_source(source_id, handle):
    from freelancer_bot.persistence.source_repository import SourceRecord

    record = SourceRecord(
        id=source_id,
        platform="telegram",
        external_id=f"username:{handle.removeprefix('@')}",
        access_type="public",
        lifecycle_status=SourceStatus.APPROVED,
        display_name="G3 dispatch source",
        handle=handle,
        canonical_url=f"https://t.me/{handle.removeprefix('@')}",
        created_at=NOW,
        updated_at=NOW,
    )
    return TelegramCollectorSource(record=record, lookup=handle)


if __name__ == "__main__":
    unittest.main()
