import ast
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, patch

from pydantic import SecretStr
from telethon.errors import RPCError

from freelancer_bot.app import LeadBot
from freelancer_bot.filters import FilterConfig
from freelancer_bot.legacy_pipeline import LegacyLeadProcessor
from freelancer_bot.sources import Source
from freelancer_bot.storage import Storage


ROOT = Path(__file__).resolve().parents[1]
SOURCE_A = Source("@source_a", "Source A", "fixture")
SOURCE_B = Source("@source_b", "Source B", "fixture")
BASE_DATE = datetime(2026, 8, 8, 12, 0, tzinfo=timezone.utc)


class StubRPCError(RPCError):
    def __init__(self, message="fixture RPC failure"):
        Exception.__init__(self, message)


class FakeMessage:
    def __init__(self, message_id, text, date):
        self.id = message_id
        self.message = text
        self.date = date


class FakeTelegramClient:
    def __init__(self, *, entities=None, histories=None, failures=None, events_log=None, role="client"):
        self.entities = entities or {}
        self.histories = histories or {}
        self.failures = failures or {}
        self.events_log = events_log if events_log is not None else []
        self.role = role
        self.handlers = []
        self.iter_calls = []
        self.start_calls = []
        self.disconnect_calls = 0

    def on(self, event_builder):
        def register(handler):
            self.handlers.append((event_builder, handler))
            return handler

        return register

    async def start(self, **kwargs):
        self.start_calls.append(kwargs)
        self.events_log.append(f"{self.role}.start")

    async def disconnect(self):
        self.disconnect_calls += 1
        self.events_log.append(f"{self.role}.disconnect")

    async def get_entity(self, handle):
        failure = self.failures.get(("entity", handle))
        if failure:
            raise failure
        return self.entities.get(handle, f"entity:{handle}")

    def iter_messages(self, entity, *, limit):
        self.iter_calls.append((entity, limit))

        async def iterate():
            failure = self.failures.get(("history", entity))
            if failure:
                raise failure
            for message in self.histories.get(entity, ())[:limit]:
                yield message

        return iterate()


class LifecycleStorage:
    def __init__(self, events_log):
        self.events_log = events_log
        self.added = []
        self.closed = 0

    def add_subscriber(self, chat_id):
        self.added.append(chat_id)

    def close(self):
        self.closed += 1
        self.events_log.append("storage.close")


class RecordingDelivery:
    def __init__(self):
        self.calls = []

    async def deliver_lead(self, chat_id, body, lead_id):
        self.calls.append((chat_id, body, lead_id))
        return 1000 + len(self.calls)


class CollectorCatchUpTest(unittest.IsolatedAsyncioTestCase):
    async def test_catch_up_uses_per_source_bound_and_global_chronological_order(self):
        client = FakeTelegramClient(
            histories={
                "entity-a": [
                    FakeMessage(3, "a3", datetime(2026, 8, 8, 12, 3, tzinfo=timezone.utc)),
                    FakeMessage(1, "a1", datetime(2026, 8, 8, 12, 1, tzinfo=timezone.utc)),
                    FakeMessage(0, "outside bound", datetime(2026, 8, 8, 12, 0, tzinfo=timezone.utc)),
                ],
                "entity-b": [
                    FakeMessage(4, "b4", datetime(2026, 8, 8, 12, 4, tzinfo=timezone.utc)),
                    FakeMessage(2, "b2", datetime(2026, 8, 8, 12, 2, tzinfo=timezone.utc)),
                    FakeMessage(9, "outside bound", datetime(2026, 8, 8, 12, 9, tzinfo=timezone.utc)),
                ],
            }
        )
        bot = _bare_bot(catch_up_limit=2, user_client=client)
        observed = []

        async def process(source, message):
            observed.append((source.handle, message.id))

        bot._process_message = process
        await bot._catch_up(((SOURCE_A, "entity-a"), (SOURCE_B, "entity-b")))

        self.assertEqual(client.iter_calls, [("entity-a", 2), ("entity-b", 2)])
        self.assertEqual(
            observed,
            [
                ("@source_a", 1),
                ("@source_b", 2),
                ("@source_a", 3),
                ("@source_b", 4),
            ],
        )

    async def test_catch_up_rpc_failure_skips_one_source_and_continues(self):
        client = FakeTelegramClient(
            histories={
                "entity-b": [FakeMessage(2, "b2", BASE_DATE)],
            },
            failures={("history", "entity-a"): StubRPCError()},
        )
        bot = _bare_bot(catch_up_limit=5, user_client=client)
        bot._process_message = AsyncMock()

        with patch("freelancer_bot.app.log_event") as logged:
            await bot._catch_up(((SOURCE_A, "entity-a"), (SOURCE_B, "entity-b")))

        bot._process_message.assert_awaited_once_with(
            SOURCE_B,
            client.histories["entity-b"][0],
        )
        self.assertEqual(
            [call.args[2] for call in logged.call_args_list],
            [
                "telegram.collector.catch_up_failed",
                "telegram.collector.message_dispatched",
            ],
        )

    async def test_restart_catch_up_delivers_only_unseen_messages(self):
        with tempfile.TemporaryDirectory() as tmp:
            database_path = Path(tmp) / "restart.sqlite3"
            first_storage = Storage(database_path)
            first_storage.add_subscriber(101)
            first_delivery = RecordingDelivery()
            first_processor = _processor(first_storage, first_delivery)
            first_bot = _bare_bot(catch_up_limit=2)
            first_bot.legacy_processor = first_processor

            old_message = FakeMessage(10, "Нужен телеграм бот", BASE_DATE)
            await first_bot._process_message(SOURCE_A, old_message)
            first_storage.close()

            restarted_storage = Storage(database_path)
            restarted_delivery = RecordingDelivery()
            restarted_processor = _processor(restarted_storage, restarted_delivery)
            new_message = FakeMessage(
                11,
                "Нужен телеграм бот на Python",
                datetime(2026, 8, 8, 12, 5, tzinfo=timezone.utc),
            )
            history_client = FakeTelegramClient(histories={"entity-a": [new_message, old_message]})
            restarted_bot = _bare_bot(catch_up_limit=2, user_client=history_client)
            restarted_bot.legacy_processor = restarted_processor

            await restarted_bot._catch_up(((SOURCE_A, "entity-a"),))

            self.assertEqual(len(first_delivery.calls), 1)
            self.assertEqual(len(restarted_delivery.calls), 1)
            self.assertIn("https://t.me/source_a/11", restarted_delivery.calls[0][1])
            self.assertEqual(restarted_storage.stats(), {"leads": 2, "pending": 0, "subscribers": 1})
            restarted_storage.close()


class CollectorHandlerAndLifecycleTest(unittest.IsolatedAsyncioTestCase):
    async def test_bot_commands_callbacks_and_source_handlers_are_registered_on_fake_clients(self):
        bot_client = FakeTelegramClient()
        user_client = FakeTelegramClient(entities={"@source_a": "entity-a"})
        bot = _bare_bot(user_client=user_client)
        bot.bot_client = bot_client
        bot.storage = LifecycleStorage([])
        bot.sources = [SOURCE_A]
        bot.filter_config = FilterConfig(
            min_score=5,
            keywords={"телеграм бот": 5},
            stop_words=("smm",),
        )
        bot.config = SimpleNamespace(ai_reply_enabled=False)

        bot._register_bot_commands()
        bot._register_callback_handlers()
        active = await bot._register_source_handlers()

        self.assertEqual(len(bot_client.handlers), 36)
        self.assertEqual(len(user_client.handlers), 1)
        self.assertEqual(active, [(SOURCE_A, "entity-a")])

    async def test_source_resolution_failure_is_skipped_and_resolved_handler_keeps_identity(self):
        client = FakeTelegramClient(
            entities={"@source_b": "entity-b"},
            failures={("entity", "@source_a"): ValueError("not found")},
        )
        bot = _bare_bot(user_client=client)
        bot.sources = [SOURCE_A, SOURCE_B]
        bot._process_message = AsyncMock()

        with patch("freelancer_bot.app.log_event") as logged:
            active = await bot._register_source_handlers()

        self.assertEqual(active, [(SOURCE_B, "entity-b")])
        self.assertEqual(len(client.handlers), 1)
        message = FakeMessage(20, "fixture", BASE_DATE)
        await client.handlers[0][1](SimpleNamespace(message=message))
        bot._process_message.assert_awaited_once_with(SOURCE_B, message)
        logged.assert_called_once()
        self.assertEqual(
            logged.call_args.args[2],
            "telegram.collector.source_resolution_failed",
        )

    async def test_run_starts_clients_then_catches_up_before_waiting(self):
        bot, events_log = _orchestrated_bot(send_catch_up=True, catch_up_limit=3)

        await bot.run()

        self.assertEqual(
            events_log,
            [
                "register.commands",
                "register.callbacks",
                "user.start",
                "bot.start",
                "load.sources",
                "pipeline.start",
                "register.sources",
                "catch_up",
                "wait",
                "pipeline.stop",
            ],
        )
        self.assertEqual(bot.bot_client.start_calls, [{"bot_token": "bot-token"}])

    async def test_disabled_catch_up_does_nothing(self):
        bot, events_log = _orchestrated_bot(send_catch_up=False, catch_up_limit=3)

        await bot.run()

        self.assertNotIn("catch_up", events_log)
        self.assertEqual(events_log[-2:], ["wait", "pipeline.stop"])

    async def test_zero_limit_catch_up_does_nothing(self):
        bot, events_log = _orchestrated_bot(send_catch_up=True, catch_up_limit=0)

        await bot.run()

        self.assertNotIn("catch_up", events_log)
        self.assertEqual(events_log[-2:], ["wait", "pipeline.stop"])

    async def test_postgresql_source_failure_aborts_without_registering_json_sources(self):
        bot, events_log = _orchestrated_bot(send_catch_up=True, catch_up_limit=3)
        bot.source_adapter.list_for_session = AsyncMock(
            side_effect=RuntimeError("postgres unavailable")
        )

        with self.assertRaisesRegex(RuntimeError, "postgres unavailable"):
            await bot.run()

        self.assertEqual(
            events_log,
            [
                "register.commands",
                "register.callbacks",
                "user.start",
                "bot.start",
            ],
        )
        self.assertNotIn("register.sources", events_log)
        self.assertNotIn("catch_up", events_log)

    async def test_shutdown_disconnects_both_clients_and_closes_sqlite(self):
        events_log = []
        bot = _bare_bot(
            user_client=FakeTelegramClient(events_log=events_log, role="user"),
        )
        bot.bot_client = FakeTelegramClient(events_log=events_log, role="bot")
        bot.storage = LifecycleStorage(events_log)
        bot.database = SimpleNamespace(close=AsyncMock())

        await bot.shutdown()

        self.assertEqual(events_log, ["user.disconnect", "bot.disconnect", "storage.close"])
        self.assertEqual(bot.storage.closed, 1)
        bot.database.close.assert_awaited_once()


class CollectorGateBoundaryTest(unittest.TestCase):
    def test_g3_runtime_uses_postgres_ingestion_and_bounded_worker_wrapper(self):
        modules = set()
        for relative in (
            Path("freelancer_bot/app.py"),
            Path("freelancer_bot/legacy_pipeline.py"),
        ):
            tree = ast.parse((ROOT / relative).read_text(encoding="utf-8"))
            for node in ast.walk(tree):
                if isinstance(node, ast.Import):
                    modules.update(alias.name for alias in node.names)
                elif isinstance(node, ast.ImportFrom) and node.module:
                    modules.add(node.module)

        forbidden_imports = {
            module
            for module in modules
            if module == "worker"
            or module.startswith("freelancer_bot.worker")
            or module.endswith("persistence.jobs")
        }
        app_text = (ROOT / "freelancer_bot/app.py").read_text(encoding="utf-8")

        self.assertEqual(forbidden_imports, set())
        self.assertNotIn("DurableWorker", app_text)
        self.assertNotIn("durable_jobs", app_text)
        self.assertIn("persistence.raw_messages", app_text)
        self.assertIn("RawMessageIngestor", app_text)
        self.assertIn("TelegramIngestionRuntime", app_text)
        self.assertIn("ApprovedTelegramSourceAdapter", app_text)
        self.assertIn("LegacyLeadProcessor", app_text)


def _bare_bot(*, catch_up_limit=25, user_client=None):
    bot = LeadBot.__new__(LeadBot)
    bot.config = SimpleNamespace(
        catch_up_limit=catch_up_limit,
        legacy_delivery_enabled=True,
    )
    bot.user_client = user_client or FakeTelegramClient()
    return bot


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


def _orchestrated_bot(*, send_catch_up, catch_up_limit):
    events_log = []
    bot = LeadBot.__new__(LeadBot)
    bot.config = SimpleNamespace(
        bot_token=SecretStr("bot-token"),
        target_chat_id=None,
        send_catch_up=send_catch_up,
        catch_up_limit=catch_up_limit,
    )
    bot.user_client = FakeTelegramClient(events_log=events_log, role="user")
    bot.bot_client = FakeTelegramClient(events_log=events_log, role="bot")
    bot.storage = LifecycleStorage(events_log)
    bot.sources = []

    class SourceAdapter:
        async def list_for_session(self, client):
            events_log.append("load.sources")
            return SimpleNamespace(
                collector_account=SimpleNamespace(id=81),
                sources=(SOURCE_A,),
            )

    bot.source_adapter = SourceAdapter()

    class PipelineRuntime:
        async def start(self):
            events_log.append("pipeline.start")

        async def wait_until_collector_stops(self, collector_stop):
            await collector_stop

        async def stop(self):
            events_log.append("pipeline.stop")

    bot.ingestion_runtime = PipelineRuntime()

    bot._register_bot_commands = Mock(side_effect=lambda: events_log.append("register.commands"))
    bot._register_callback_handlers = Mock(side_effect=lambda: events_log.append("register.callbacks"))

    async def register_sources():
        events_log.append("register.sources")
        return [(SOURCE_A, "entity-a")]

    async def catch_up(active_sources):
        events_log.append("catch_up")

    async def wait():
        events_log.append("wait")

    bot._register_source_handlers = register_sources
    bot._catch_up = catch_up
    bot._wait_until_stopped = wait
    return bot, events_log


if __name__ == "__main__":
    unittest.main()
