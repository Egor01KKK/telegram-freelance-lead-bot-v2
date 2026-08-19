import tempfile
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

from freelancer_bot.app import LeadBot
from freelancer_bot.config import RuntimeConfig
from freelancer_bot.filters import FilterConfig


class BotOnlyRuntimeTest(unittest.IsolatedAsyncioTestCase):
    def _config(self, root: Path) -> RuntimeConfig:
        return RuntimeConfig(
            api_id=123,
            api_hash="api-hash",
            bot_token="bot-token",
            database_url="postgresql+psycopg://test:test@localhost/test",
            database_path=root / "legacy.sqlite3",
            sources_path=root / "sources.json",
            filters_path=root / "filters.json",
            user_session_path=root / "sessions" / "user",
            bot_session_path=root / "sessions" / "bot",
            _env_file=None,
        )

    async def test_bot_only_does_not_construct_collector_or_legacy_storage(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            database = MagicMock()
            bot_client = MagicMock()
            with (
                patch("freelancer_bot.app.Database", return_value=database),
                patch("freelancer_bot.app.Storage") as storage,
                patch("freelancer_bot.app.TelegramClient", return_value=bot_client) as client,
                patch(
                    "freelancer_bot.app.load_filter_config",
                    return_value=FilterConfig(1, {"python": 1}, ()),
                ),
            ):
                bot = LeadBot(self._config(root), background_enabled=False)

            self.assertIsNone(bot.user_client)
            self.assertIsNone(bot.ingestion_runtime)
            self.assertIsNone(bot.raw_ingestor)
            self.assertIsNone(bot.legacy_processor)
            storage.assert_not_called()
            client.assert_called_once()

    async def test_bot_only_starts_ui_without_user_session_or_delivery_worker(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            database = MagicMock()
            database.close = AsyncMock()
            bot_client = MagicMock()
            bot_client.start = AsyncMock()
            bot_client.disconnect = AsyncMock()
            with (
                patch("freelancer_bot.app.Database", return_value=database),
                patch("freelancer_bot.app.Storage"),
                patch("freelancer_bot.app.TelegramClient", return_value=bot_client),
                patch(
                    "freelancer_bot.app.load_filter_config",
                    return_value=FilterConfig(1, {"python": 1}, ()),
                ),
            ):
                bot = LeadBot(self._config(root), background_enabled=False)
                bot._register_bot_commands = MagicMock()
                bot._register_callback_handlers = MagicMock()
                bot._wait_until_stopped = AsyncMock()
                await bot.run()
                await bot.shutdown()

            bot_client.start.assert_awaited_once()
            bot_client.disconnect.assert_awaited_once()
            database.close.assert_awaited_once()
            self.assertIsNone(bot.user_client)
            self.assertIsNone(bot.ingestion_runtime)


if __name__ == "__main__":
    unittest.main()
