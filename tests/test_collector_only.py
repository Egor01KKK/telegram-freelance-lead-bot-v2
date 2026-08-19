from __future__ import annotations

import tempfile
from pathlib import Path
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock, MagicMock, patch

from freelancer_bot.collector_only import CollectorOnlyRuntime
from freelancer_bot.config import RuntimeConfig, RuntimeMode
from freelancer_bot.telegram_session import (
    TelegramSessionFileLock,
    TelegramSessionInUseError,
)


class CollectorOnlyRuntimeTest(unittest.IsolatedAsyncioTestCase):
    def _config(self, session_path: Path, *, chat_discovery_enabled: bool = False) -> RuntimeConfig:
        return RuntimeConfig(
            api_id=12345,
            api_hash="api-hash",
            bot_token=None,
            database_url="postgresql+psycopg://test:test@localhost/test",
            user_session_path=session_path,
            source_discovery_enabled=False,
            source_audit_enabled=False,
            telegram_chat_discovery_enabled=chat_discovery_enabled,
            _env_file=None,
        )

    async def test_collector_only_authenticates_catalog_without_bot_or_delivery(self):
        with tempfile.TemporaryDirectory() as directory:
            session_path = Path(directory) / "collector_2_user"
            config = self._config(session_path)
            database = MagicMock()
            database.close = AsyncMock()
            client = MagicMock()
            client.start = AsyncMock()
            client.disconnect = AsyncMock()
            adapter = MagicMock()
            adapter.list_for_session = AsyncMock(
                return_value=SimpleNamespace(
                    collector_account=SimpleNamespace(id=42),
                    sources=(),
                )
            )

            with patch(
                "freelancer_bot.collector_only.TelegramClient",
                return_value=client,
            ) as telegram_client:
                runtime = CollectorOnlyRuntime(
                    config,
                    database=database,
                    source_adapter=adapter,
                )
                snapshot = await runtime.start()
                await runtime.stop()

            telegram_client.assert_called_once_with(
                str(session_path),
                12345,
                "api-hash",
                flood_sleep_threshold=0,
            )
            client.start.assert_awaited_once_with()
            adapter.list_for_session.assert_awaited_once_with(client)
            client.disconnect.assert_awaited_once_with()
            database.close.assert_awaited_once_with()
            self.assertEqual(snapshot.collector_account.id, 42)
            self.assertFalse(hasattr(runtime, "bot_client"))
            self.assertFalse(hasattr(runtime, "ingestion_runtime"))

    def test_collector_only_config_requires_postgres_and_user_credentials_only(self):
        env = {
            "TELEGRAM_API_ID": "12345",
            "TELEGRAM_API_HASH": "api-hash",
            "DATABASE_URL": "postgresql+psycopg://test:test@localhost/test",
        }
        with patch.dict("os.environ", env, clear=True):
            config = RuntimeConfig.from_env(
                mode=RuntimeMode.COLLECTOR_ONLY,
                env_file=None,
            )

        self.assertIsNone(config.bot_token)
        self.assertEqual(config.api_id, 12345)
        self.assertEqual(config.postgresql_url(), env["DATABASE_URL"])

    async def test_collector_only_startup_log_contains_only_safe_runtime_fields(self):
        with tempfile.TemporaryDirectory() as directory:
            config = self._config(Path(directory) / "collector_2_user")
            database = MagicMock()
            database.close = AsyncMock()
            client = MagicMock()
            client.start = AsyncMock()
            client.disconnect = AsyncMock()
            adapter = MagicMock()
            adapter.list_for_session = AsyncMock(
                return_value=SimpleNamespace(
                    collector_account=SimpleNamespace(id=7),
                    sources=(),
                )
            )

            with patch("freelancer_bot.collector_only.log_event") as log_event:
                runtime = CollectorOnlyRuntime(
                    config,
                    database=database,
                    client=client,
                    source_adapter=adapter,
                )
                await runtime.start()
                await runtime.stop()

        log_event.assert_called_once()
        fields = log_event.call_args.kwargs
        self.assertEqual(fields["runtime_mode"], "collector_only")
        self.assertEqual(fields["collector_account_id"], 7)
        self.assertFalse(fields["source_discovery_enabled"])
        self.assertFalse(fields["source_audit_enabled"])
        self.assertNotIn("api-hash", str(fields))

    async def test_chat_discovery_mode_does_not_advertise_or_start_broad_discovery(self):
        with tempfile.TemporaryDirectory() as directory:
            config = self._config(
                Path(directory) / "collector_chat_discovery",
                chat_discovery_enabled=True,
            )
            database = MagicMock()
            database.close = AsyncMock()
            client = MagicMock()
            client.start = AsyncMock()
            client.disconnect = AsyncMock()
            adapter = MagicMock()
            adapter.list_for_session = AsyncMock(
                return_value=SimpleNamespace(
                    collector_account=SimpleNamespace(id=8),
                    sources=(),
                )
            )
            with patch("freelancer_bot.collector_only.log_event") as log_event:
                runtime = CollectorOnlyRuntime(
                    config,
                    database=database,
                    client=client,
                    source_adapter=adapter,
                )
                await runtime.start()
                chat_runtime = runtime.chat_discovery_runtime
                await runtime.stop()

        self.assertIsNotNone(chat_runtime)
        fields = log_event.call_args.kwargs
        self.assertFalse(fields["source_discovery_enabled"])
        self.assertFalse(fields["source_audit_enabled"])


class TelegramSessionFileLockTest(unittest.TestCase):
    def test_same_session_path_cannot_be_owned_twice(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "collector_2_user"
            first = TelegramSessionFileLock(path)
            second = TelegramSessionFileLock(path)
            first.acquire()
            try:
                with self.assertRaises(TelegramSessionInUseError):
                    second.acquire()
            finally:
                first.release()

            second.acquire()
            second.release()


if __name__ == "__main__":
    unittest.main()
