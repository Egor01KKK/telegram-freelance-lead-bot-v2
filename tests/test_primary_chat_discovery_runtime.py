from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from types import SimpleNamespace
import unittest
from unittest.mock import ANY, AsyncMock, MagicMock, patch

from freelancer_bot.app import LeadBot
from freelancer_bot.source_discovery_runtime import AutonomousSourceDiscoveryRuntime
from freelancer_bot.telegram_chat_discovery import ensure_profile_derived_topics
from freelancer_bot.telegram_collector import TelegramCollectorSource


class PrimaryChatDiscoveryRuntimeTest(unittest.IsolatedAsyncioTestCase):
    def _bot(self, *, chat_enabled: bool, source_enabled: bool = True) -> LeadBot:
        bot = LeadBot.__new__(LeadBot)
        bot.config = SimpleNamespace(
            telegram_chat_discovery_enabled=chat_enabled,
            source_discovery_enabled=source_enabled,
        )
        bot.database = MagicMock(name="database")
        bot.user_client = MagicMock(name="user_client")
        bot.collector_account_id = 23
        bot._telegram_governor = MagicMock(name="governor")
        bot._profile_discovery_runtime = None
        bot._chat_discovery_runtime = None
        bot._chat_discovery_task = None
        return bot

    def test_chat_mode_builds_primary_runtime_from_existing_client_and_governor(self):
        bot = self._bot(chat_enabled=True)

        with (
            patch("freelancer_bot.app.TelegramChatDiscoveryService") as service,
            patch("freelancer_bot.app.TelegramChatDiscoveryRuntime") as runtime,
            patch("freelancer_bot.app.TelegramProfileDiscoveryRuntime") as legacy,
        ):
            bot._configure_telegram_discovery_runtimes()

        service.assert_called_once_with(
            bot.database,
            bot.user_client,
            config=bot.config,
            collector_account_id=bot.collector_account_id,
            governor=bot._telegram_governor,
            watch_candidate_callback=ANY,
        )
        runtime.assert_called_once_with(service.return_value, logger=ANY)
        legacy.assert_not_called()
        self.assertIsNotNone(bot._chat_discovery_runtime)
        self.assertIsNone(bot._profile_discovery_runtime)

    async def test_chat_watch_wakes_source_discovery_without_waiting_for_interval(self):
        bot = self._bot(chat_enabled=True)
        bot.config.source_discovery_interval_seconds = 3600
        bot._source_discovery_first_cycle = None
        bot._source_discovery_wake = asyncio.Event()

        class Runtime:
            def __init__(self):
                self.calls = 0
                self.first_call = asyncio.Event()
                self.second_call = asyncio.Event()

            async def run_once(self):
                self.calls += 1
                if self.calls == 1:
                    self.first_call.set()
                if self.calls == 2:
                    self.second_call.set()
                return SimpleNamespace(reload_required=False)

        runtime = Runtime()
        bot.source_discovery_runtime = runtime
        task = asyncio.create_task(bot._run_source_discovery_loop())
        try:
            await asyncio.wait_for(runtime.first_call.wait(), timeout=1)
            await bot._signal_source_discovery_wake(901)
            await asyncio.wait_for(runtime.second_call.wait(), timeout=1)
            self.assertEqual(runtime.calls, 2)
        finally:
            bot.source_discovery_runtime = None
            bot._source_discovery_wake.set()
            await asyncio.wait_for(task, timeout=1)

    async def test_source_discovery_keeps_periodic_timeout_fallback(self):
        bot = self._bot(chat_enabled=True)
        bot.config.source_discovery_interval_seconds = 0.01
        bot._source_discovery_first_cycle = None
        bot._source_discovery_wake = asyncio.Event()

        class Runtime:
            def __init__(self):
                self.calls = 0

            async def run_once(self):
                self.calls += 1
                if self.calls == 2:
                    bot.source_discovery_runtime = None
                return SimpleNamespace(reload_required=False)

        runtime = Runtime()
        bot.source_discovery_runtime = runtime

        await asyncio.wait_for(bot._run_source_discovery_loop(), timeout=1)

        self.assertEqual(runtime.calls, 2)

    async def test_reload_catches_up_only_newly_active_sources(self):
        bot = self._bot(chat_enabled=True)
        bot.config.send_catch_up = True
        bot.config.catch_up_limit = 10
        bot._active_sources = []
        bot._source_discovery_caught_up_keys = set()
        source = TelegramCollectorSource(
            record=SimpleNamespace(id=77, updated_at=None),
            lookup="@new_source",
        )
        bot.source_adapter = MagicMock()
        bot.source_adapter.list_for_session = AsyncMock(
            return_value=SimpleNamespace(
                collector_account=SimpleNamespace(id=23),
                sources=(source,),
            )
        )
        bot._register_source_handlers = AsyncMock(return_value=[(source, object())])
        bot._catch_up = AsyncMock()

        newly_active = await bot._reload_approved_sources()

        self.assertEqual(newly_active, [(source, bot._active_sources[0][1])])
        bot._catch_up.assert_awaited_once_with(bot._active_sources)
        self.assertIn(("id", 77), bot._source_discovery_caught_up_keys)

        second_reload = await bot._reload_approved_sources()

        self.assertEqual(second_reload, [])
        bot._catch_up.assert_awaited_once_with(bot._active_sources)

    async def test_reload_registers_without_history_when_catch_up_is_disabled(self):
        bot = self._bot(chat_enabled=True)
        bot.config.send_catch_up = False
        bot.config.catch_up_limit = 10
        bot._active_sources = []
        bot._source_discovery_caught_up_keys = set()
        source = TelegramCollectorSource(
            record=SimpleNamespace(id=78, updated_at=None),
            lookup="@no_history_source",
        )
        bot.source_adapter = MagicMock()
        bot.source_adapter.list_for_session = AsyncMock(
            return_value=SimpleNamespace(
                collector_account=SimpleNamespace(id=23),
                sources=(source,),
            )
        )
        bot._register_source_handlers = AsyncMock(return_value=[(source, object())])
        bot._catch_up = AsyncMock()

        await bot._reload_approved_sources()

        bot._register_source_handlers.assert_awaited_once_with()
        bot._catch_up.assert_not_awaited()

    def test_chat_mode_disabled_preserves_legacy_profile_runtime(self):
        bot = self._bot(chat_enabled=False, source_enabled=True)

        with (
            patch("freelancer_bot.app.TelegramChatDiscoveryService") as service,
            patch("freelancer_bot.app.TelegramChatDiscoveryRuntime") as runtime,
            patch("freelancer_bot.app.TelegramProfileDiscoveryRuntime") as legacy,
        ):
            bot._configure_telegram_discovery_runtimes()

        service.assert_not_called()
        runtime.assert_not_called()
        legacy.assert_called_once()
        self.assertIsNone(bot._chat_discovery_runtime)
        self.assertIs(legacy.return_value, bot._profile_discovery_runtime)

    async def test_chat_runtime_stops_cleanly(self):
        bot = self._bot(chat_enabled=True)
        bot._chat_discovery_runtime = MagicMock()
        bot._chat_discovery_task = None

        await bot._stop_telegram_chat_discovery_runtime()

        bot._chat_discovery_runtime.request_stop.assert_called_once_with()

    async def test_source_discovery_skips_old_profile_telegram_when_chat_mode_enabled(self):
        runtime = AutonomousSourceDiscoveryRuntime.__new__(AutonomousSourceDiscoveryRuntime)
        runtime._config = SimpleNamespace(
            telegram_chat_discovery_enabled=True,
            telegram_global_discovery_enabled=True,
        )
        runtime._client = SimpleNamespace(get_messages=AsyncMock())
        runtime._database = MagicMock()
        runtime._profile_discovery = MagicMock()
        runtime._logger = MagicMock()

        with patch(
            "freelancer_bot.source_discovery_runtime.SearchProfileRepository.list_active",
            new_callable=AsyncMock,
        ) as list_active:
            result = await runtime._run_profile_telegram(
                requested_at=datetime.now(timezone.utc),
                bucket=1,
                governor=MagicMock(),
                page_cache=MagicMock(),
            )

        self.assertEqual(result, ())
        list_active.assert_not_awaited()

    async def test_profile_topics_use_buyer_intent_queries_not_raw_profile_terms(self):
        intent = SimpleNamespace(
            id="profile-intent",
            languages=("en",),
            roles=("Video Editor",),
            services=("YouTube editing",),
            skills=("Premiere Pro",),
            industries=("video",),
        )
        repository = MagicMock()

        async def ensure_topic(_connection, **values):
            return SimpleNamespace(**values)

        repository.ensure_topic = ensure_topic
        with patch(
            "freelancer_bot.telegram_chat_discovery.TelegramChatDiscoveryRepository",
            return_value=repository,
        ):
            topics = await ensure_profile_derived_topics(
                MagicMock(),
                intent,
                use_buyer_intent_queries=True,
            )

        topic_texts = tuple(topic.topic_text for topic in topics)
        self.assertLessEqual(len(topic_texts), 20)
        self.assertIn("looking for Video Editor", topic_texts)
        self.assertIn("looking for a specialist in YouTube editing", topic_texts)
        self.assertNotIn("Video Editor", topic_texts)
        self.assertNotIn("Premiere Pro", topic_texts)

    async def test_legacy_profile_topics_keep_raw_projection(self):
        intent = SimpleNamespace(
            id="legacy-profile-intent",
            languages=("en",),
            roles=("Video Editor",),
            services=("YouTube editing",),
            skills=("Premiere Pro",),
            industries=("video",),
        )
        repository = MagicMock()

        async def ensure_topic(_connection, **values):
            return SimpleNamespace(**values)

        repository.ensure_topic = ensure_topic
        with patch(
            "freelancer_bot.telegram_chat_discovery.TelegramChatDiscoveryRepository",
            return_value=repository,
        ):
            topics = await ensure_profile_derived_topics(MagicMock(), intent)

        topic_texts = tuple(topic.topic_text for topic in topics)
        self.assertIn("Video Editor", topic_texts)
        self.assertIn("Premiere Pro", topic_texts)


if __name__ == "__main__":
    unittest.main()
