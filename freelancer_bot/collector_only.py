"""Collector-only runtime for controlled Telegram source operations.

This runtime deliberately owns one authenticated Telethon user client and the
PostgreSQL source catalog only.  It does not construct the user-facing bot,
ingestion workers, matching/delivery services or legacy delivery pipeline.
"""

from __future__ import annotations

import asyncio
import logging
import signal
from typing import Any

from telethon import TelegramClient

from .config import RuntimeConfig
from .observability import log_event
from .persistence.database import Database
from .source_discovery_runtime import AutonomousSourceDiscoveryRuntime
from .telegram_profile_discovery import TelegramProfileDiscoveryRuntime
from .telegram_request_governor import TelegramRequestGovernor
from .telegram_chat_discovery import (
    TelegramChatDiscoveryRuntime,
    TelegramChatDiscoveryService,
)
from .telegram_collector import (
    ApprovedTelegramSourceAdapter,
    TelegramCollectorSourceSnapshot,
)
from .telegram_session import TelegramSessionFileLock


LOGGER = logging.getLogger("freelancer_bot")


class CollectorOnlyRuntime:
    """Authenticate one collector account and optionally run discovery cycles."""

    def __init__(
        self,
        config: RuntimeConfig,
        *,
        database: Database | None = None,
        client: Any | None = None,
        source_adapter: ApprovedTelegramSourceAdapter | None = None,
    ) -> None:
        if config.api_id is None:
            raise ValueError("TELEGRAM_API_ID/API_ID is required for collector-only mode")
        self.config = config
        self.database = database or Database(config.postgresql_url())
        self.client = client or TelegramClient(
            str(config.user_session_path),
            config.api_id,
            _required_secret(config.api_hash, "TELEGRAM_API_HASH/API_HASH"),
            flood_sleep_threshold=0,
        )
        self.source_adapter = source_adapter or ApprovedTelegramSourceAdapter(self.database)
        self.session_lock = TelegramSessionFileLock(
            config.user_session_path,
            role="collector_only",
        )
        self.discovery_runtime: AutonomousSourceDiscoveryRuntime | None = None
        self.profile_discovery_runtime: TelegramProfileDiscoveryRuntime | None = None
        self.chat_discovery_runtime: TelegramChatDiscoveryRuntime | None = None
        self._chat_discovery_task: asyncio.Task[None] | None = None
        self.governor: TelegramRequestGovernor | None = None
        self.snapshot: TelegramCollectorSourceSnapshot | None = None
        self.collector_account_id: int | None = None
        self._started = False

    async def start(self) -> TelegramCollectorSourceSnapshot:
        """Authenticate the user session and register it in the normal catalog."""
        if self._started:
            if self.snapshot is None:
                raise RuntimeError("collector-only runtime started without a catalog snapshot")
            return self.snapshot

        self.config.user_session_path.parent.mkdir(parents=True, exist_ok=True)
        self.session_lock.acquire()
        try:
            await self.client.start()
            self.snapshot = await self.source_adapter.list_for_session(self.client)
            self.collector_account_id = self.snapshot.collector_account.id
            self.governor = TelegramRequestGovernor(
                self.database,
                self.collector_account_id,
                self.config,
            )
            if self.config.telegram_chat_discovery_enabled:
                self.chat_discovery_runtime = TelegramChatDiscoveryRuntime(
                    TelegramChatDiscoveryService(
                        self.database,
                        self.client,
                        config=self.config,
                        collector_account_id=self.collector_account_id,
                        governor=self.governor,
                    ),
                    logger=LOGGER,
                )
            self._started = True
            log_event(
                LOGGER,
                logging.INFO,
                "runtime.started",
                runtime_mode="collector_only",
                collector_account_id=self.collector_account_id,
                source_discovery_enabled=(
                    self.config.source_discovery_enabled
                    and not self.config.telegram_chat_discovery_enabled
                ),
                source_audit_enabled=(
                    self.config.source_audit_enabled
                    and not self.config.telegram_chat_discovery_enabled
                ),
            )
            return self.snapshot
        except BaseException:
            try:
                await self.client.disconnect()
            finally:
                self.session_lock.release()
            raise

    async def run(self) -> None:
        try:
            await self.start()
        except BaseException:
            await self.database.close()
            raise
        try:
            if self.chat_discovery_runtime is not None:
                self._chat_discovery_task = asyncio.create_task(
                    self.chat_discovery_runtime.run()
                )
            if self.config.source_discovery_enabled and not self.config.telegram_chat_discovery_enabled:
                self.profile_discovery_runtime = TelegramProfileDiscoveryRuntime(
                    self.database,
                    self.config,
                    client=self.client,
                    collector_account_id=self.collector_account_id,
                    governor=self.governor,
                    logger=LOGGER,
                    worker_id=f"telegram-profile-discovery-{id(self)}",
                )
                await self.profile_discovery_runtime.start()
                self.discovery_runtime = AutonomousSourceDiscoveryRuntime(
                    self.database,
                    self.client,
                    self.config,
                    source_adapter=self.source_adapter,
                    logger=LOGGER,
                    governor=self.governor,
                )
                await self._run_discovery_loop()
            else:
                await self._wait_until_stopped()
        finally:
            if self.chat_discovery_runtime is not None:
                self.chat_discovery_runtime.request_stop()
            if self._chat_discovery_task is not None:
                await self._chat_discovery_task
                self._chat_discovery_task = None
            await self.stop()

    async def stop(self) -> None:
        try:
            if self.profile_discovery_runtime is not None:
                await self.profile_discovery_runtime.stop()
                self.profile_discovery_runtime = None
            if self.chat_discovery_runtime is not None:
                self.chat_discovery_runtime.request_stop()
                self.chat_discovery_runtime = None
            if self._started:
                await self.client.disconnect()
        finally:
            self._started = False
            self.snapshot = None
            self.collector_account_id = None
            self.governor = None
            self.session_lock.release()
            await self.database.close()

    async def _run_discovery_loop(self) -> None:
        interval = self.config.source_discovery_interval_seconds
        while True:
            try:
                if self.discovery_runtime is None:
                    return
                await self.discovery_runtime.run_once()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                log_event(
                    LOGGER,
                    logging.WARNING,
                    "source.discovery.runtime_failed",
                    error=exc,
                )
            await asyncio.sleep(interval)

    async def _wait_until_stopped(self) -> None:
        stop_event = asyncio.Event()
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(sig, stop_event.set)
            except NotImplementedError:
                pass
        await stop_event.wait()


async def run_collector_only() -> None:
    """Load collector-only configuration and run until SIGINT/SIGTERM."""
    from .config import RuntimeMode
    from .observability import Redactor, configure_structured_logger

    config = RuntimeConfig.from_env(mode=RuntimeMode.COLLECTOR_ONLY)
    configure_structured_logger(
        "freelancer_bot",
        redactor=Redactor.from_config(config),
        level=getattr(logging, config.log_level, logging.INFO),
    )
    runtime = CollectorOnlyRuntime(config)
    await runtime.run()


def _required_secret(value: Any, name: str) -> str:
    if value is None:
        raise ValueError(f"{name} is required for collector-only mode")
    secret = value.get_secret_value()
    if not secret.strip():
        raise ValueError(f"{name} is required for collector-only mode")
    return secret
