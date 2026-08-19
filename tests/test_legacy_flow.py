import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from telethon.errors import RPCError

from freelancer_bot.app import LeadBot, TelethonLegacyLeadDelivery
from freelancer_bot.config import RuntimeConfig
from freelancer_bot.filters import FilterConfig
from freelancer_bot.legacy_pipeline import LegacyLeadProcessor
from freelancer_bot.ports import CollectedMessage
from freelancer_bot.sources import Source
from freelancer_bot.storage import Storage


class LegacyLeadFlowCharacterizationTest(unittest.IsolatedAsyncioTestCase):
    async def test_v2_default_does_not_route_messages_to_legacy_delivery(self):
        storage = FakeStorage(subscribers=[101])
        bot_client = FakeBotClient()

        with self._build_bot(storage, bot_client) as lead_bot:
            lead_bot.config = lead_bot.config.model_copy(update={
                "legacy_delivery_enabled": False,
            })
            lead_bot.legacy_processor.handle = AsyncMock()
            await lead_bot._process_message(
                SOURCE,
                FakeMessage(41, "Я разработчик и ищу клиентов", MESSAGE_DATE),
            )

        lead_bot.legacy_processor.handle.assert_not_awaited()
        self.assertEqual(storage.recorded, [])
        self.assertEqual(bot_client.calls, [])

    async def test_matching_message_is_stored_and_delivered_to_every_subscriber(self):
        storage = FakeStorage(subscribers=[101, 202])
        bot_client = FakeBotClient()

        with self._build_bot(storage, bot_client) as lead_bot:
            await lead_bot._process_message(
                SOURCE,
                FakeMessage(42, "Нужен телеграм бот на Python", MESSAGE_DATE),
            )

        self.assertEqual(len(storage.recorded), 1)
        lead = storage.recorded[0]
        self.assertEqual(lead.source, "@test_source")
        self.assertEqual(lead.message_id, 42)
        self.assertEqual(lead.link, "https://t.me/test_source/42")
        self.assertEqual(lead.score, 7)
        self.assertEqual(lead.keywords, ("телеграм бот", "python"))
        self.assertEqual(lead.message_date, MESSAGE_DATE.isoformat())

        self.assertEqual([call[0] for call in bot_client.calls], [101, 202])
        for _, body, kwargs in bot_client.calls:
            self.assertIn("<b>📌 Лид #17</b> · score 7", body)
            self.assertIn("<b>Источник:</b> Тестовый канал", body)
            self.assertIn('href="https://t.me/test_source/42"', body)
            self.assertEqual(kwargs["parse_mode"], "html")
            self.assertFalse(kwargs["link_preview"])
            self.assertEqual(len(kwargs["buttons"]), 1)
            self.assertEqual(len(kwargs["buttons"][0]), 2)

        self.assertEqual(storage.notifications, [(17, 101, 900), (17, 202, 901)])

    async def test_stop_word_message_is_not_stored_or_delivered(self):
        storage = FakeStorage(subscribers=[101])
        bot_client = FakeBotClient()

        with self._build_bot(storage, bot_client) as lead_bot:
            await lead_bot._process_message(
                SOURCE,
                FakeMessage(43, "Нужен телеграм бот для SMM", MESSAGE_DATE),
            )

        self.assertEqual(storage.recorded, [])
        self.assertEqual(storage.notifications, [])
        self.assertEqual(bot_client.calls, [])

    async def test_below_threshold_message_is_not_stored_or_delivered(self):
        storage = FakeStorage(subscribers=[101])
        bot_client = FakeBotClient()

        with self._build_bot(storage, bot_client) as lead_bot:
            await lead_bot._process_message(
                SOURCE,
                FakeMessage(44, "Python скрипт", MESSAGE_DATE),
            )

        self.assertEqual(storage.recorded, [])
        self.assertEqual(storage.notifications, [])
        self.assertEqual(bot_client.calls, [])

    async def test_empty_whitespace_and_non_text_messages_are_ignored(self):
        storage = FakeStorage(subscribers=[101])
        bot_client = FakeBotClient()

        with self._build_bot(storage, bot_client) as lead_bot:
            for message_id, text in ((45, ""), (46, "   \n"), (47, None)):
                await lead_bot._process_message(
                    SOURCE,
                    FakeMessage(message_id, text, None),
                )

        self.assertEqual(storage.recorded, [])
        self.assertEqual(storage.notifications, [])
        self.assertEqual(bot_client.calls, [])

    async def test_matching_message_without_subscribers_is_stored_pending(self):
        with tempfile.TemporaryDirectory() as tmp:
            storage = Storage(Path(tmp) / "leads.sqlite3")
            delivery = SequenceDelivery()
            processor = _processor(storage, delivery)

            with patch("freelancer_bot.legacy_pipeline.LOGGER.warning") as warning:
                await processor.handle(_collected(48))

            self.assertEqual(storage.stats(), {"leads": 1, "pending": 1, "subscribers": 0})
            self.assertEqual(delivery.calls, [])
            warning.assert_called_once()
            self.assertIn("no subscribers", warning.call_args.args[0])
            storage.close()

    async def test_successful_duplicate_source_message_is_not_delivered_twice(self):
        with tempfile.TemporaryDirectory() as tmp:
            storage = Storage(Path(tmp) / "leads.sqlite3")
            storage.add_subscriber(101)
            delivery = SequenceDelivery(900)
            processor = _processor(storage, delivery)

            await processor.handle(_collected(49))
            await processor.handle(_collected(49))

            self.assertEqual(storage.stats(), {"leads": 1, "pending": 0, "subscribers": 1})
            self.assertEqual([call[0] for call in delivery.calls], [101])
            storage.close()

    async def test_failed_delivery_remains_pending_and_is_retried_on_duplicate(self):
        with tempfile.TemporaryDirectory() as tmp:
            storage = Storage(Path(tmp) / "leads.sqlite3")
            storage.add_subscriber(101)
            delivery = SequenceDelivery(None, 901)
            processor = _processor(storage, delivery)

            await processor.handle(_collected(50))
            self.assertEqual(storage.stats()["pending"], 1)

            await processor.handle(_collected(50))
            self.assertEqual(storage.stats()["pending"], 0)
            self.assertEqual([call[0] for call in delivery.calls], [101, 101])
            storage.close()

    async def test_multiple_successes_store_only_the_last_recipient_metadata(self):
        with tempfile.TemporaryDirectory() as tmp:
            storage = Storage(Path(tmp) / "leads.sqlite3")
            storage.add_subscriber(101)
            storage.add_subscriber(202)
            delivery = SequenceDelivery(901, 902)
            processor = _processor(storage, delivery)

            await processor.handle(_collected(51))

            stored = storage.get_lead(1)
            self.assertEqual([call[0] for call in delivery.calls], [101, 202])
            self.assertEqual(stored.notification_chat_id, 202)
            self.assertEqual(stored.notification_message_id, 902)
            storage.close()

    async def test_partial_success_is_logically_complete_and_failed_recipient_is_not_retried(self):
        with tempfile.TemporaryDirectory() as tmp:
            storage = Storage(Path(tmp) / "leads.sqlite3")
            storage.add_subscriber(101)
            storage.add_subscriber(202)
            delivery = SequenceDelivery(901, None)
            processor = _processor(storage, delivery)

            await processor.handle(_collected(52))
            await processor.handle(_collected(52))

            stored = storage.get_lead(1)
            self.assertEqual([call[0] for call in delivery.calls], [101, 202])
            self.assertEqual(storage.stats()["pending"], 0)
            self.assertEqual(stored.notification_chat_id, 101)
            self.assertEqual(stored.notification_message_id, 901)
            storage.close()

    def _build_bot(self, storage, bot_client):
        temporary_directory = tempfile.TemporaryDirectory()
        root = Path(temporary_directory.name)
        config = RuntimeConfig(
            api_id=123,
            api_hash="api-hash",
            bot_token="bot-token",
            target_chat_id=None,
            database_path=root / "leads.sqlite3",
            database_url="postgresql+psycopg://test:test@localhost/test",
            sources_path=root / "sources.json",
            filters_path=root / "filters.json",
            user_session_path=root / "sessions" / "user",
            bot_session_path=root / "sessions" / "bot",
            catch_up_limit=25,
            send_catch_up=True,
            legacy_delivery_enabled=True,
            ai_reply_enabled=False,
            openai_api_key="",
            openai_model="gpt-4.1-mini",
            freelancer_profile_path=root / "profile.json",
            log_level="INFO",
        )
        filter_config = FilterConfig(
            min_score=5,
            keywords={"телеграм бот": 5, "python": 2},
            stop_words=("smm",),
        )
        patches = [
            patch("freelancer_bot.app.Storage", return_value=storage),
            patch("freelancer_bot.app.load_filter_config", return_value=filter_config),
            patch("freelancer_bot.app.TelegramClient", side_effect=[FakeUserClient(), bot_client]),
        ]
        return PatchedLeadBot(temporary_directory, patches, config)


class PatchedLeadBot:
    def __init__(self, temporary_directory, patches, config):
        self.temporary_directory = temporary_directory
        self.patches = patches
        self.config = config
        self.lead_bot = None

    def __enter__(self):
        for active_patch in self.patches:
            active_patch.start()
        self.lead_bot = LeadBot(self.config)
        return self.lead_bot

    def __exit__(self, exc_type, exc_value, traceback):
        for active_patch in reversed(self.patches):
            active_patch.stop()
        self.temporary_directory.cleanup()


class FakeStorage:
    def __init__(self, subscribers):
        self._subscribers = subscribers
        self.recorded = []
        self.notifications = []

    def record_or_should_retry(self, lead):
        self.recorded.append(lead)
        return 17

    def subscribers(self):
        return list(self._subscribers)

    def mark_notification_message(self, lead_id, chat_id, telegram_message_id):
        self.notifications.append((lead_id, chat_id, telegram_message_id))


class FakeUserClient:
    pass


class FakeBotClient:
    def __init__(self):
        self.calls = []

    async def send_message(self, chat_id, body, **kwargs):
        self.calls.append((chat_id, body, kwargs))
        return SimpleNamespace(id=899 + len(self.calls))


class FakeMessage:
    def __init__(self, message_id, text, date):
        self.id = message_id
        self.message = text
        self.date = date


class SequenceDelivery:
    def __init__(self, *outcomes):
        self.outcomes = list(outcomes)
        self.calls = []

    async def deliver_lead(self, chat_id, body, lead_id):
        self.calls.append((chat_id, body, lead_id))
        if self.outcomes:
            return self.outcomes.pop(0)
        return None


class StubRPCError(RPCError):
    def __init__(self, message="fixture RPC failure"):
        Exception.__init__(self, message)


class FailingBotClient:
    async def send_message(self, *args, **kwargs):
        raise StubRPCError()


class TelethonDeliveryFailureTest(unittest.IsolatedAsyncioTestCase):
    async def test_rpc_error_leaves_the_sqlite_lead_pending(self):
        with tempfile.TemporaryDirectory() as tmp:
            storage = Storage(Path(tmp) / "leads.sqlite3")
            storage.add_subscriber(101)
            processor = _processor(
                storage,
                TelethonLegacyLeadDelivery(FailingBotClient()),
            )

            with patch("freelancer_bot.app.LOGGER.warning") as warning:
                await processor.handle(_collected(53))

            self.assertEqual(storage.stats(), {"leads": 1, "pending": 1, "subscribers": 1})
            warning.assert_called_once()
            self.assertIn("Could not deliver lead", warning.call_args.args[0])
            storage.close()


def _processor(storage, delivery):
    return LegacyLeadProcessor(
        FilterConfig(
            min_score=5,
            keywords={"телеграм бот": 5, "python": 2},
            stop_words=("smm",),
        ),
        storage,
        storage,
        delivery,
    )


def _collected(message_id, text="Нужен телеграм бот на Python"):
    return CollectedMessage(
        source=SOURCE,
        message_id=message_id,
        text=text,
        message_date=MESSAGE_DATE,
    )


SOURCE = Source(
    handle="@test_source",
    title="Тестовый канал",
    reason="Characterization fixture",
)
MESSAGE_DATE = datetime(2026, 8, 8, 12, 30, tzinfo=timezone.utc)


if __name__ == "__main__":
    unittest.main()
