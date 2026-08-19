from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace
import unittest
from unittest.mock import ANY, AsyncMock, MagicMock, patch

from freelancer_bot.app import LeadBot
from freelancer_bot.source_discovery_runtime import AutonomousSourceDiscoveryRuntime
from freelancer_bot.telegram_chat_discovery import ensure_profile_derived_topics


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
        )
        runtime.assert_called_once_with(service.return_value, logger=ANY)
        legacy.assert_not_called()
        self.assertIsNotNone(bot._chat_discovery_runtime)
        self.assertIsNone(bot._profile_discovery_runtime)

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
