from __future__ import annotations

import argparse
import asyncio
import logging
import signal
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Iterable
from uuid import UUID

from pydantic import SecretStr
from telethon import Button, TelegramClient, events
from telethon.errors import RPCError
from telethon.tl.custom.message import Message

from .billing import BillingPlan
from .collector_only import run_collector_only
from .config import AIProvider, ConfigurationError, RuntimeConfig, RuntimeMode
from .delivery import TelegramSendReceipt
from .delivery_actions import (
    DELIVERY_ACTION_CALLBACK_PATTERN,
    DeliveryActionService,
    TelegramDeliveryActionButton,
    decode_delivery_action_callback,
    delivery_action_buttons,
)
from .filters import FilterConfig, load_filter_config, match_text
from .formatting import format_reply_draft
from .ingestion_runtime import TelegramIngestionRuntime
from .legacy_pipeline import LegacyLeadProcessor
from .observability import (
    Redactor,
    configure_structured_logger,
    log_event,
    new_correlation_id,
    trace_context,
)
from .opportunity_analysis import opportunity_analysis_provider_available
from .persistence.database import Database
from .persistence.delivery_actions import (
    DeliveryActionError,
    DeliveryActionOwnershipError,
    DeliveryActionType,
)
from .persistence.raw_messages import (
    IneligibleRawMessageSource,
    RawMessageIngestor,
    RawMessageInput,
    RawMessageOrigin,
)
from .persistence.search_profiles import (
    SearchProfileActivationError,
    SearchProfileEditConflict,
    SearchProfileNotFound,
    SearchProfileOwnershipError,
    UserNotFound,
)
from .ports import CollectedMessage, ReplyDraftProvider
from .profile import load_freelancer_profile
from .profile_confirmation import ProfileConfirmationService
from .profile_onboarding import (
    OnboardingProfileError,
    OpenAICompatibleOnboardingProfileAnalyzer,
)
from .profile_onboarding_service import ProfileOnboardingService
from .replies import OpenAIReplyDraftGenerator, ReplyDraft, ReplyDraftError
from .source_ai_config import source_ai_provider_available
from .source_discovery_runtime import AutonomousSourceDiscoveryRuntime
from .sources import Source, load_sources
from .storage import Storage, StoredLead
from .telegram_chat_discovery import (
    TelegramChatDiscoveryRuntime,
    TelegramChatDiscoveryService,
)
from .telegram_collector import (
    ApprovedTelegramSourceAdapter,
    TelegramCollectorSource,
)
from .telegram_navigation import (
    CANCEL_LABEL,
    HOME_LABEL,
    MAIN_MENU_LABELS,
    TelegramNavigationService,
    new_profile_prompt,
    setting_field_from_code,
    setting_prompt,
)
from .telegram_onboarding import (
    WORK_TYPE_CALLBACK_CODES,
    TelegramOnboardingResponse,
    TelegramProfileOnboarding,
    settings_help,
)
from .telegram_profile_discovery import TelegramProfileDiscoveryRuntime
from .telegram_request_governor import TelegramRequestCategory, TelegramRequestGovernor
from .telegram_session import TelegramSessionFileLock

LOGGER = logging.getLogger("freelancer_bot")


@dataclass(frozen=True)
class _PendingNavigationInput:
    kind: str
    profile_id: UUID | None = None
    expected_revision: int | None = None
    setting_code: str | None = None


class TelethonLegacyLeadDelivery:
    def __init__(self, bot_client: TelegramClient):
        self.bot_client = bot_client

    async def deliver_lead(self, chat_id: int, body: str, lead_id: int) -> int | None:
        buttons = [
            [
                Button.inline("Сделать отклик", data=f"draft:{lead_id}".encode("utf-8")),
                Button.inline("Игнор", data=f"ignore:{lead_id}".encode("utf-8")),
            ]
        ]
        try:
            sent_message = await self.bot_client.send_message(
                chat_id,
                body,
                parse_mode="html",
                link_preview=False,
                buttons=buttons,
            )
        except RPCError as exc:
            LOGGER.warning("Could not deliver lead to %s: %s", chat_id, exc)
            return None
        return int(sent_message.id)


class TelethonPersonalizedDeliverySender:
    def __init__(self, bot_client: TelegramClient):
        self.bot_client = bot_client

    async def send(
        self,
        *,
        recipient_chat_id: int,
        body_html: str,
        parse_mode: str,
        link_preview: bool,
        idempotency_key: str,
        buttons: tuple[tuple[TelegramDeliveryActionButton, ...], ...],
    ) -> TelegramSendReceipt:
        # Telethon's convenience API does not expose an external idempotency key.
        # PostgreSQL suppresses every retry after a confirmed send; the narrow
        # send-success/DB-commit crash window remains best effort.
        del idempotency_key
        sent_message = await self.bot_client.send_message(
            recipient_chat_id,
            body_html,
            parse_mode=parse_mode,
            link_preview=link_preview,
            buttons=_telethon_action_buttons(buttons),
        )
        return TelegramSendReceipt(message_id=int(sent_message.id))


class LeadBot:
    def __init__(
        self,
        config: RuntimeConfig,
        reply_draft_provider: ReplyDraftProvider | None = None,
        *,
        database: Database | None = None,
        source_adapter: ApprovedTelegramSourceAdapter | None = None,
        raw_ingestor: RawMessageIngestor | None = None,
        ingestion_runtime: TelegramIngestionRuntime | None = None,
        background_enabled: bool = True,
    ):
        self.config = config
        self._background_enabled = background_enabled
        if background_enabled:
            config.user_session_path.parent.mkdir(parents=True, exist_ok=True)
        config.bot_session_path.parent.mkdir(parents=True, exist_ok=True)
        self.database = database or Database(config.postgresql_url())
        self.storage = Storage(config.database_path) if background_enabled else None
        self.filter_config = load_filter_config(config.filters_path)
        self.sources: list[TelegramCollectorSource] = []
        if config.api_id is None:
            raise ConfigurationError("TELEGRAM_API_ID/API_ID is required for the Telegram runtime")
        self.user_client = (
            TelegramClient(
                str(config.user_session_path),
                config.api_id,
                _required_secret(config.api_hash, "TELEGRAM_API_HASH/API_HASH"),
                flood_sleep_threshold=0,
            )
            if background_enabled
            else None
        )
        self.bot_client = TelegramClient(
            str(config.bot_session_path),
            config.api_id,
            _required_secret(config.api_hash, "TELEGRAM_API_HASH/API_HASH"),
        )
        self.source_adapter = source_adapter or ApprovedTelegramSourceAdapter(
            self.database
        )
        self.source_discovery_runtime: AutonomousSourceDiscoveryRuntime | None = None
        self.raw_ingestor = (
            raw_ingestor or RawMessageIngestor(self.database)
            if background_enabled
            else None
        )
        self.ingestion_runtime = (
            ingestion_runtime or TelegramIngestionRuntime(
                self.database,
                config,
                logger=LOGGER,
                delivery_sender=TelethonPersonalizedDeliverySender(self.bot_client),
            )
            if background_enabled
            else None
        )
        self.profile_confirmation = ProfileConfirmationService(
            self.database,
            telegram_chat_discovery_enabled=getattr(
                config,
                "telegram_chat_discovery_enabled",
                False,
            ),
            telegram_chat_discovery_max_topics_per_cycle=getattr(
                config,
                "telegram_chat_discovery_max_topics_per_cycle",
                5,
            ),
        )
        self.profile_onboarding = _build_profile_onboarding(
            config,
            self.database,
            confirmation=self.profile_confirmation,
        )
        self.navigation = TelegramNavigationService(
            self.profile_confirmation,
            billing_plan=BillingPlan.from_config(config),
        )
        self._pending_navigation_inputs: dict[str, _PendingNavigationInput] = {}
        self.delivery_actions = DeliveryActionService(
            self.database,
            logger=LOGGER,
        )
        self.collector_account_id: int | None = None
        self._session_lock: TelegramSessionFileLock | None = None
        self._registered_source_keys: set[object] = set()
        self._source_discovery_task: asyncio.Task[None] | None = None
        self._source_discovery_first_cycle: asyncio.Event | None = None
        self._source_discovery_error: BaseException | None = None
        self._source_discovery_wake = asyncio.Event()
        self._source_discovery_caught_up_keys: set[object] = set()
        self._profile_discovery_runtime: TelegramProfileDiscoveryRuntime | None = None
        self._chat_discovery_runtime: TelegramChatDiscoveryRuntime | None = None
        self._chat_discovery_task: asyncio.Task[None] | None = None
        self._telegram_governor: TelegramRequestGovernor | None = None
        self.reply_draft_provider = reply_draft_provider
        if self.reply_draft_provider is None and config.ai_reply_enabled:
            self.reply_draft_provider = _build_reply_draft_provider(config)
        self.legacy_delivery = TelethonLegacyLeadDelivery(self.bot_client)
        self.legacy_processor = (
            LegacyLeadProcessor(
                self.filter_config,
                self.storage,
                self.storage,
                self.legacy_delivery,
            )
            if self.storage is not None
            else None
        )

    async def run(self) -> None:
        background_enabled = getattr(self, "_background_enabled", True)
        session_path = getattr(self.config, "user_session_path", None)
        if background_enabled and session_path is not None:
            self._session_lock = TelegramSessionFileLock(
                session_path,
                role="user_runtime_collector",
            )
            self._session_lock.acquire()
        self._log_runtime_mode()
        self._register_bot_commands()
        self._register_callback_handlers()

        if background_enabled:
            await self.user_client.start()
        await self.bot_client.start(
            bot_token=_required_secret(self.config.bot_token, "TELEGRAM_BOT_TOKEN/BOT_TOKEN")
        )

        if background_enabled:
            snapshot = await self.source_adapter.list_for_session(self.user_client)
            self.collector_account_id = snapshot.collector_account.id
            self.sources = list(snapshot.sources)
            database = getattr(self, "database", None)
            if database is not None:
                self._telegram_governor = TelegramRequestGovernor(
                    database,
                    self.collector_account_id,
                    self.config,
                )
            self._configure_telegram_discovery_runtimes()

            if self.config.target_chat_id is not None and self.storage is not None:
                self.storage.add_subscriber(self.config.target_chat_id)

            await self.ingestion_runtime.start()
        try:
            if background_enabled:
                if getattr(self, "_profile_discovery_runtime", None) is not None:
                    await self._profile_discovery_runtime.start()
                if getattr(self, "_chat_discovery_runtime", None) is not None:
                    self._chat_discovery_task = asyncio.create_task(
                        self._chat_discovery_runtime.run(),
                        name="telegram-chat-discovery-runtime",
                    )
                active_sources = await self._register_source_handlers()
                self._active_sources = active_sources
                LOGGER.info("Monitoring %s Telegram sources", len(active_sources))

                if (
                    getattr(self.config, "source_discovery_enabled", False)
                    and database is not None
                    and self._telegram_governor is not None
                ):
                    self._source_discovery_first_cycle = asyncio.Event()
                    self._source_discovery_error = None
                    self.source_discovery_runtime = AutonomousSourceDiscoveryRuntime(
                        database,
                        self.user_client,
                        self.config,
                        source_adapter=self.source_adapter,
                        logger=LOGGER,
                        governor=self._telegram_governor,
                    )
                    self._source_discovery_task = asyncio.create_task(
                        self._run_source_discovery_loop(),
                        name="autonomous-source-discovery",
                    )

                if getattr(self.config, "catch_up_after_source_discovery", False):
                    first_cycle = getattr(self, "_source_discovery_first_cycle", None)
                    if first_cycle is not None:
                        await first_cycle.wait()
                    discovery_error = getattr(self, "_source_discovery_error", None)
                    if discovery_error is not None:
                        raise discovery_error
                    active_sources = getattr(
                        self,
                        "_active_sources",
                        active_sources,
                    )

                if self.config.send_catch_up and self.config.catch_up_limit > 0:
                    caught_up_keys = getattr(
                        self,
                        "_source_discovery_caught_up_keys",
                        set(),
                    )
                    await self._catch_up(
                        tuple(
                            item
                            for item in active_sources
                            if _source_identity_key(item[0]) not in caught_up_keys
                        )
                    )

                await self.ingestion_runtime.wait_until_collector_stops(
                    self._wait_until_stopped()
                )
            else:
                await self._wait_until_stopped()
        finally:
            if background_enabled:
                if getattr(self, "_profile_discovery_runtime", None) is not None:
                    await self._profile_discovery_runtime.stop()
                await self._stop_telegram_chat_discovery_runtime()
                await self._stop_source_discovery_loop()
                await self.ingestion_runtime.stop()

    async def shutdown(self) -> None:
        if getattr(self, "_profile_discovery_runtime", None) is not None:
            await self._profile_discovery_runtime.stop()
            self._profile_discovery_runtime = None
        await self._stop_telegram_chat_discovery_runtime()
        await self._stop_source_discovery_loop()
        try:
            try:
                if getattr(self, "user_client", None) is not None:
                    await self.user_client.disconnect()
            finally:
                try:
                    await self.bot_client.disconnect()
                finally:
                    try:
                        if self.storage is not None:
                            self.storage.close()
                    finally:
                        await self.database.close()
        finally:
            session_lock = getattr(self, "_session_lock", None)
            if session_lock is not None:
                session_lock.release()
                self._session_lock = None

    def _register_bot_commands(self) -> None:
        @self.bot_client.on(
            events.NewMessage(
                pattern=r"^(Мой поиск|Настройки|Подписка|Главное меню|Отмена)$"
            )
        )
        async def navigation_control(event: events.NewMessage.Event) -> None:
            label = event.pattern_match.group(1)
            if label == CANCEL_LABEL:
                self._pending_navigation_inputs.pop(
                    _telegram_user_id(event),
                    None,
                )
                response = self.navigation.home()
            elif label == "Мой поиск":
                response = await self.navigation.my_search(
                    external_user_id=_telegram_user_id(event),
                )
            elif label == "Настройки":
                response = await self.navigation.settings(
                    external_user_id=_telegram_user_id(event),
                )
            elif label == "Подписка":
                response = await self.navigation.subscription(
                    external_user_id=_telegram_user_id(event),
                )
            else:
                response = self.navigation.home()
            self._pending_navigation_inputs.pop(_telegram_user_id(event), None)
            await _respond_navigation(event, response)

        @self.bot_client.on(events.NewMessage(pattern=r"^/start"))
        async def start(event: events.NewMessage.Event) -> None:
            chat_id = int(event.chat_id)
            if (
                getattr(self, "_background_enabled", True)
                and getattr(self.config, "legacy_delivery_enabled", False)
            ):
                self.storage.add_subscriber(chat_id)
                await event.respond(
                    "Готово. Этот чат подписан на лиды.\n\n"
                    f"Chat ID: <code>{chat_id}</code>\n"
                    "Настроить профиль поиска: /profile\n"
                    "Команды: /status, /sources, /keywords, /test текст, /stop",
                    parse_mode="html",
                    buttons=_main_navigation_keyboard(),
                )
                return
            await _respond_navigation(event, self.navigation.home())

        @self.bot_client.on(events.NewMessage(pattern=r"^/stop"))
        async def stop(event: events.NewMessage.Event) -> None:
            if self.storage is not None:
                self.storage.remove_subscriber(int(event.chat_id))
            await event.respond("Ок, этот чат отписан от уведомлений.")

        @self.bot_client.on(events.NewMessage(pattern=r"^/status"))
        async def status(event: events.NewMessage.Event) -> None:
            if self.storage is None:
                await event.respond(
                    "UI-only режим активен. Коллектор, фоновые workers и legacy-доставка выключены."
                )
                return
            stats = self.storage.stats()
            await event.respond(
                "Статус:\n"
                f"- источников: {len(self.sources)}\n"
                f"- подписчиков: {stats['subscribers']}\n"
                f"- лидов в базе: {stats['leads']}\n"
                f"- ожидают повторной отправки: {stats['pending']}"
            )

        @self.bot_client.on(events.NewMessage(pattern=r"^/sources"))
        async def sources(event: events.NewMessage.Event) -> None:
            lines = [f"{index}. {source.handle} — {source.title}" for index, source in enumerate(self.sources, 1)]
            await event.respond("Активные источники:\n" + "\n".join(lines))

        @self.bot_client.on(events.NewMessage(pattern=r"^/keywords"))
        async def keywords(event: events.NewMessage.Event) -> None:
            keyword_preview = ", ".join(list(self.filter_config.keywords)[:35])
            stop_preview = ", ".join(self.filter_config.stop_words[:35])
            await event.respond(
                "Ключевые слова:\n"
                f"{keyword_preview}\n\n"
                "Стоп-слова:\n"
                f"{stop_preview}\n\n"
                f"Минимальный score: {self.filter_config.min_score}"
            )

        @self.bot_client.on(events.NewMessage(pattern=r"^/test(?:\s+(.+))?"))
        async def test_filter(event: events.NewMessage.Event) -> None:
            text = event.pattern_match.group(1)
            if not text:
                await event.respond("Пришли так: /test нужен телеграм бот на Python")
                return

            result = match_text(text, self.filter_config)
            if result.accepted:
                await event.respond(
                    f"Пройдет фильтр. Score: {result.score}. Совпало: {', '.join(result.matched_keywords)}"
                )
            else:
                reason = (
                    f"стоп-слова: {', '.join(result.rejected_by)}"
                    if result.rejected_by
                    else f"score ниже порога: {result.score}"
                )
                await event.respond(f"Не пройдет фильтр: {reason}")

        @self.bot_client.on(events.NewMessage(pattern=r"^/profile(?:\s+([\s\S]+))?$"))
        async def profile(event: events.NewMessage.Event) -> None:
            response = await self.profile_onboarding.begin(
                external_user_id=_telegram_user_id(event),
                description=event.pattern_match.group(1),
            )
            await _respond_onboarding(event, response)

        @self.bot_client.on(
            events.NewMessage(pattern=r"^/profile_manual(?:\s+([\s\S]+))?$")
        )
        async def profile_manual(event: events.NewMessage.Event) -> None:
            self._pending_navigation_inputs.pop(_telegram_user_id(event), None)
            payload = event.pattern_match.group(1)
            if payload is None:
                await event.respond(
                    "Формат: /profile_manual роли | навыки | категории"
                )
                return
            try:
                response = await self.profile_onboarding.create_manual(
                    external_user_id=_telegram_user_id(event),
                    payload=payload,
                )
            except ValueError as exc:
                await event.respond(f"Не удалось сохранить профиль: {exc}")
                return
            await _respond_onboarding(event, response)

        @self.bot_client.on(
            events.NewMessage(
                pattern=(
                    r"^/profile_edit\s+([0-9a-f-]{36})\s+(\d+)\s+"
                    r"(roles|skills|categories)\s+([\s\S]+)$"
                )
            )
        )
        async def profile_edit(event: events.NewMessage.Event) -> None:
            try:
                response = await self.profile_onboarding.edit(
                    external_user_id=_telegram_user_id(event),
                    profile_id=UUID(event.pattern_match.group(1)),
                    expected_revision=int(event.pattern_match.group(2)),
                    field=event.pattern_match.group(3),
                    values=event.pattern_match.group(4),
                )
            except _PROFILE_INPUT_ERRORS as exc:
                await event.respond(f"Не удалось изменить профиль: {exc}")
                return
            await _respond_onboarding(event, response)

        @self.bot_client.on(
            events.NewMessage(pattern=r"^/profile_show\s+([0-9a-f-]{36})$")
        )
        async def profile_show(event: events.NewMessage.Event) -> None:
            try:
                response = await self.profile_onboarding.show(
                    external_user_id=_telegram_user_id(event),
                    profile_id=UUID(event.pattern_match.group(1)),
                )
            except _PROFILE_INPUT_ERRORS as exc:
                await event.respond(f"Не удалось открыть профиль: {exc}")
                return
            await _respond_onboarding(event, response)

        @self.bot_client.on(
            events.NewMessage(
                pattern=(
                    r"^/profile_setting\s+([0-9a-f-]{36})\s+(\d+)\s+"
                    r"(work_types|budget|languages|geography|work_modes|"
                    r"exclusions)\s+([\s\S]+)$"
                )
            )
        )
        async def profile_setting(event: events.NewMessage.Event) -> None:
            try:
                response = await self.profile_onboarding.edit_setting(
                    external_user_id=_telegram_user_id(event),
                    profile_id=UUID(event.pattern_match.group(1)),
                    expected_revision=int(event.pattern_match.group(2)),
                    field=event.pattern_match.group(3),
                    value=event.pattern_match.group(4),
                )
            except _PROFILE_INPUT_ERRORS as exc:
                await event.respond(f"Не удалось изменить настройки: {exc}")
                return
            await _respond_onboarding(event, response)

        @self.bot_client.on(
            events.NewMessage(
                pattern=r"^/profile_activate\s+([0-9a-f-]{36})\s+(\d+)$"
            )
        )
        async def profile_activate(event: events.NewMessage.Event) -> None:
            try:
                response = await self.profile_onboarding.activate(
                    external_user_id=_telegram_user_id(event),
                    profile_id=UUID(event.pattern_match.group(1)),
                    expected_revision=int(event.pattern_match.group(2)),
                )
            except _PROFILE_INPUT_ERRORS as exc:
                await event.respond(f"Не удалось активировать поиск: {exc}")
                return
            await _respond_onboarding(event, response)

        @self.bot_client.on(
            events.NewMessage(
                pattern=r"^/profile_deactivate\s+([0-9a-f-]{36})\s+(\d+)$"
            )
        )
        async def profile_deactivate(event: events.NewMessage.Event) -> None:
            try:
                response = await self.profile_onboarding.deactivate(
                    external_user_id=_telegram_user_id(event),
                    profile_id=UUID(event.pattern_match.group(1)),
                    expected_revision=int(event.pattern_match.group(2)),
                )
            except _PROFILE_INPUT_ERRORS as exc:
                await event.respond(f"Не удалось остановить поиск: {exc}")
                return
            await _respond_onboarding(event, response)

        @self.bot_client.on(events.NewMessage())
        async def navigation_text_input(event: events.NewMessage.Event) -> None:
            text = _event_text(event)
            if (
                not text
                or text.startswith("/")
                or text in (*MAIN_MENU_LABELS, HOME_LABEL, CANCEL_LABEL)
            ):
                return
            user_id = _telegram_user_id(event)
            pending = self._pending_navigation_inputs.get(user_id)
            if pending is None:
                return

            if pending.kind == "profile_ai":
                try:
                    response = await self.profile_onboarding.begin(
                        external_user_id=user_id,
                        description=text,
                    )
                except _PROFILE_INPUT_ERRORS as exc:
                    await event.respond(
                        f"Не удалось обработать описание: {exc}\n\n"
                        "Попробуйте отправить описание ещё раз.",
                        parse_mode="html",
                        buttons=[[Button.inline(CANCEL_LABEL, data=b"nav:cancel")]],
                    )
                    return
                if not response.retryable:
                    self._pending_navigation_inputs.pop(user_id, None)
                await _respond_onboarding(event, response)
                return

            try:
                if pending.kind == "profile_manual":
                    response = await self.profile_onboarding.create_manual(
                        external_user_id=user_id,
                        payload=text,
                    )
                    self._pending_navigation_inputs.pop(user_id, None)
                    await _respond_onboarding(event, response)
                    return

                if pending.kind != "setting" or pending.profile_id is None:
                    self._pending_navigation_inputs.pop(user_id, None)
                    return
                field = setting_field_from_code(pending.setting_code or "")
                if field in {"roles", "skills", "categories"}:
                    response = await self.profile_onboarding.edit(
                        external_user_id=user_id,
                        profile_id=pending.profile_id,
                        expected_revision=pending.expected_revision or 0,
                        field=field,
                        values=text,
                    )
                else:
                    onboarding_field = {
                        "languages": "languages",
                        "geographies": "geography",
                        "work_modes": "work_modes",
                        "excluded_categories": "exclusions",
                        "budget": "budget",
                    }[field]
                    response = await self.profile_onboarding.edit_setting(
                        external_user_id=user_id,
                        profile_id=pending.profile_id,
                        expected_revision=pending.expected_revision or 0,
                        field=onboarding_field,
                        value=text,
                    )
                self._pending_navigation_inputs.pop(user_id, None)
            except SearchProfileEditConflict:
                self._pending_navigation_inputs.pop(user_id, None)
                await event.respond(
                    "Настройки уже изменились. Откройте раздел «Настройки» "
                    "снова и выберите актуальный профиль."
                )
                return
            except _PROFILE_INPUT_ERRORS as exc:
                prompt = (
                    new_profile_prompt()
                    if pending.kind == "profile_manual"
                    else setting_prompt(pending.setting_code or "")
                )
                await event.respond(
                    f"Не удалось сохранить значение: {exc}\n\n"
                    f"{prompt}",
                    parse_mode="html",
                    buttons=[[Button.inline(CANCEL_LABEL, data=b"nav:cancel")]],
                )
                return

            await _respond_navigation(
                event,
                await self.navigation.settings_for_profile(
                    external_user_id=user_id,
                    profile_id=pending.profile_id,
                ),
            )

    def _register_callback_handlers(self) -> None:
        @self.bot_client.on(
            events.CallbackQuery(pattern=DELIVERY_ACTION_CALLBACK_PATTERN)
        )
        async def record_delivery_action(event: events.CallbackQuery.Event) -> None:
            await self._handle_delivery_action_callback(event)

        @self.bot_client.on(events.CallbackQuery(pattern=rb"^nav:home$"))
        async def navigation_home(event: events.CallbackQuery.Event) -> None:
            self._pending_navigation_inputs.pop(_telegram_user_id(event), None)
            await event.answer()
            await _respond_navigation(event, self.navigation.home())

        @self.bot_client.on(events.CallbackQuery(pattern=rb"^nav:search$"))
        async def navigation_search(event: events.CallbackQuery.Event) -> None:
            self._pending_navigation_inputs.pop(_telegram_user_id(event), None)
            await event.answer()
            await _respond_navigation(
                event,
                await self.navigation.my_search(
                    external_user_id=_telegram_user_id(event),
                ),
            )

        @self.bot_client.on(events.CallbackQuery(pattern=rb"^nav:settings$"))
        async def navigation_settings(event: events.CallbackQuery.Event) -> None:
            self._pending_navigation_inputs.pop(_telegram_user_id(event), None)
            await event.answer()
            await _respond_navigation(
                event,
                await self.navigation.settings(
                    external_user_id=_telegram_user_id(event),
                ),
            )

        @self.bot_client.on(
            events.CallbackQuery(pattern=rb"^nav:subscription$")
        )
        async def navigation_subscription(
            event: events.CallbackQuery.Event,
        ) -> None:
            self._pending_navigation_inputs.pop(_telegram_user_id(event), None)
            await event.answer()
            await _respond_navigation(
                event,
                await self.navigation.subscription(
                    external_user_id=_telegram_user_id(event),
                ),
            )

        @self.bot_client.on(
            events.CallbackQuery(pattern=rb"^nav:profile:new$")
        )
        async def navigation_new_profile(
            event: events.CallbackQuery.Event,
        ) -> None:
            self._pending_navigation_inputs[_telegram_user_id(event)] = (
                _PendingNavigationInput(kind="profile_ai")
            )
            await event.answer()
            await event.respond(
                new_profile_prompt(),
                parse_mode="html",
                buttons=[[Button.inline(CANCEL_LABEL, data=b"nav:cancel")]],
            )

        @self.bot_client.on(events.CallbackQuery(pattern=rb"^nav:cancel$"))
        async def navigation_cancel(event: events.CallbackQuery.Event) -> None:
            self._pending_navigation_inputs.pop(
                _telegram_user_id(event),
                None,
            )
            await event.answer("Изменение отменено")
            await _respond_navigation(event, self.navigation.home())

        @self.bot_client.on(
            events.CallbackQuery(pattern=rb"^nav:profile:([0-9a-f-]{36})$")
        )
        async def navigation_profile(event: events.CallbackQuery.Event) -> None:
            self._pending_navigation_inputs.pop(_telegram_user_id(event), None)
            try:
                response = await self.navigation.profile(
                    external_user_id=_telegram_user_id(event),
                    profile_id=UUID(event.pattern_match.group(1).decode("ascii")),
                )
            except _PROFILE_INPUT_ERRORS as exc:
                await event.answer(str(exc), alert=True)
                return
            await event.answer()
            await _respond_navigation(event, response)

        @self.bot_client.on(
            events.CallbackQuery(
                pattern=rb"^nav:settings:([0-9a-f-]{36}):(\d+)$"
            )
        )
        async def navigation_profile_settings(
            event: events.CallbackQuery.Event,
        ) -> None:
            self._pending_navigation_inputs.pop(_telegram_user_id(event), None)
            try:
                response = await self.navigation.settings_for_profile(
                    external_user_id=_telegram_user_id(event),
                    profile_id=UUID(event.pattern_match.group(1).decode("ascii")),
                )
            except _PROFILE_INPUT_ERRORS as exc:
                await event.answer(str(exc), alert=True)
                return
            await event.answer()
            await _respond_navigation(event, response)

        @self.bot_client.on(
            events.CallbackQuery(
                pattern=(
                    rb"^nav:edit:([0-9a-f-]{36}):(\d+):"
                    rb"(roles|skills|categories|budget|lang|geo|mode|exclude)$"
                )
            )
        )
        async def navigation_edit_setting(
            event: events.CallbackQuery.Event,
        ) -> None:
            user_id = _telegram_user_id(event)
            setting_code = event.pattern_match.group(3).decode("ascii")
            self._pending_navigation_inputs[user_id] = _PendingNavigationInput(
                kind="setting",
                profile_id=UUID(event.pattern_match.group(1).decode("ascii")),
                expected_revision=int(event.pattern_match.group(2)),
                setting_code=setting_code,
            )
            await event.answer()
            await event.respond(
                setting_prompt(setting_code),
                parse_mode="html",
                buttons=[[Button.inline(CANCEL_LABEL, data=b"nav:cancel")]],
            )

        @self.bot_client.on(
            events.CallbackQuery(
                pattern=rb"^nav:toggle:([0-9a-f-]{36}):([opvc]):(\d+)$"
            )
        )
        async def navigation_toggle_work_type(
            event: events.CallbackQuery.Event,
        ) -> None:
            self._pending_navigation_inputs.pop(_telegram_user_id(event), None)
            try:
                await self.profile_onboarding.toggle_work_type(
                    external_user_id=_telegram_user_id(event),
                    profile_id=UUID(event.pattern_match.group(1).decode("ascii")),
                    work_type=WORK_TYPE_CALLBACK_CODES[
                        event.pattern_match.group(2).decode("ascii")
                    ],
                    expected_revision=int(event.pattern_match.group(3)),
                )
                navigation_response = await self.navigation.settings_for_profile(
                    external_user_id=_telegram_user_id(event),
                    profile_id=UUID(event.pattern_match.group(1).decode("ascii")),
                )
            except _PROFILE_INPUT_ERRORS as exc:
                await event.answer(str(exc), alert=True)
                return
            await event.answer("Настройка сохранена")
            await _respond_navigation(event, navigation_response)

        @self.bot_client.on(
            events.CallbackQuery(
                pattern=rb"^nav:confirm:([0-9a-f-]{36}):(\d+)$"
            )
        )
        async def navigation_confirm_profile(
            event: events.CallbackQuery.Event,
        ) -> None:
            self._pending_navigation_inputs.pop(_telegram_user_id(event), None)
            try:
                await self.profile_onboarding.confirm(
                    external_user_id=_telegram_user_id(event),
                    profile_id=UUID(event.pattern_match.group(1).decode("ascii")),
                    expected_revision=int(event.pattern_match.group(2)),
                )
                response = await self.navigation.profile(
                    external_user_id=_telegram_user_id(event),
                    profile_id=UUID(event.pattern_match.group(1).decode("ascii")),
                )
            except _PROFILE_INPUT_ERRORS as exc:
                await event.answer(str(exc), alert=True)
                return
            await event.answer("Профиль подтверждён")
            await _respond_navigation(event, response)

        @self.bot_client.on(
            events.CallbackQuery(
                pattern=rb"^nav:(activate|deactivate):([0-9a-f-]{36}):(\d+)$"
            )
        )
        async def navigation_toggle_profile(
            event: events.CallbackQuery.Event,
        ) -> None:
            self._pending_navigation_inputs.pop(_telegram_user_id(event), None)
            action = event.pattern_match.group(1).decode("ascii")
            profile_id = UUID(event.pattern_match.group(2).decode("ascii"))
            revision = int(event.pattern_match.group(3))
            try:
                if action == "activate":
                    outcome = await self.profile_onboarding.activate(
                        external_user_id=_telegram_user_id(event),
                        profile_id=profile_id,
                        expected_revision=revision,
                    )
                    answer = (
                        "Поиск активирован"
                        + (". Пробный период начался" if "Пробный период начался" in outcome.text else "")
                    )
                else:
                    await self.profile_onboarding.deactivate(
                        external_user_id=_telegram_user_id(event),
                        profile_id=profile_id,
                        expected_revision=revision,
                    )
                    answer = "Поиск остановлен"
                response = await self.navigation.profile(
                    external_user_id=_telegram_user_id(event),
                    profile_id=profile_id,
                )
            except _PROFILE_INPUT_ERRORS as exc:
                await event.answer(str(exc), alert=True)
                return
            await event.answer(answer)
            await _respond_navigation(event, response)

        @self.bot_client.on(
            events.CallbackQuery(
                pattern=rb"^profile:activate:([0-9a-f-]{36}):(\d+)$"
            )
        )
        async def activate_profile(event: events.CallbackQuery.Event) -> None:
            try:
                response = await self.profile_onboarding.activate(
                    external_user_id=_telegram_user_id(event),
                    profile_id=UUID(event.pattern_match.group(1).decode("ascii")),
                    expected_revision=int(event.pattern_match.group(2)),
                )
            except _PROFILE_INPUT_ERRORS as exc:
                await event.answer(str(exc), alert=True)
                return
            await event.answer("Поиск активирован")
            await _respond_onboarding(event, response)

        @self.bot_client.on(
            events.CallbackQuery(
                pattern=rb"^profile:deactivate:([0-9a-f-]{36}):(\d+)$"
            )
        )
        async def deactivate_profile(event: events.CallbackQuery.Event) -> None:
            try:
                response = await self.profile_onboarding.deactivate(
                    external_user_id=_telegram_user_id(event),
                    profile_id=UUID(event.pattern_match.group(1).decode("ascii")),
                    expected_revision=int(event.pattern_match.group(2)),
                )
            except _PROFILE_INPUT_ERRORS as exc:
                await event.answer(str(exc), alert=True)
                return
            await event.answer("Поиск остановлен")
            await _respond_onboarding(event, response)

        @self.bot_client.on(
            events.CallbackQuery(
                pattern=rb"^pwt:([0-9a-f-]{36}):([opvc]):(\d+)$"
            )
        )
        async def toggle_profile_work_type(
            event: events.CallbackQuery.Event,
        ) -> None:
            try:
                response = await self.profile_onboarding.toggle_work_type(
                    external_user_id=_telegram_user_id(event),
                    profile_id=UUID(event.pattern_match.group(1).decode("ascii")),
                    work_type=WORK_TYPE_CALLBACK_CODES[
                        event.pattern_match.group(2).decode("ascii")
                    ],
                    expected_revision=int(event.pattern_match.group(3)),
                )
            except _PROFILE_INPUT_ERRORS as exc:
                await event.answer(str(exc), alert=True)
                return
            await event.answer("Настройка сохранена")
            await _respond_onboarding(event, response)

        @self.bot_client.on(
            events.CallbackQuery(pattern=rb"^pset:([0-9a-f-]{36}):(\d+)$")
        )
        async def explain_profile_settings(
            event: events.CallbackQuery.Event,
        ) -> None:
            profile_id = UUID(event.pattern_match.group(1).decode("ascii"))
            revision = int(event.pattern_match.group(2))
            await event.answer()
            await event.respond(settings_help(profile_id, revision))

        @self.bot_client.on(
            events.CallbackQuery(
                pattern=rb"^profile:confirm:([0-9a-f-]{36}):(\d+)$"
            )
        )
        async def confirm_profile(event: events.CallbackQuery.Event) -> None:
            try:
                response = await self.profile_onboarding.confirm(
                    external_user_id=_telegram_user_id(event),
                    profile_id=UUID(event.pattern_match.group(1).decode("ascii")),
                    expected_revision=int(event.pattern_match.group(2)),
                )
            except _PROFILE_INPUT_ERRORS as exc:
                await event.answer(str(exc), alert=True)
                return
            await event.answer("Профиль подтверждён")
            await _respond_onboarding(event, response)

        @self.bot_client.on(
            events.CallbackQuery(
                pattern=(
                    rb"^profile:edit:([0-9a-f-]{36}):"
                    rb"(roles|skills|categories):(\d+)$"
                )
            )
        )
        async def explain_profile_edit(event: events.CallbackQuery.Event) -> None:
            profile_id = event.pattern_match.group(1).decode("ascii")
            field = event.pattern_match.group(2).decode("ascii")
            revision = event.pattern_match.group(3).decode("ascii")
            await event.answer()
            await event.respond(
                "Отправьте новые значения через запятую или '-' "
                "для пустого списка:\n"
                f"/profile_edit {profile_id} {revision} {field} "
                "значение 1, значение 2"
            )

        @self.bot_client.on(events.CallbackQuery(pattern=rb"^draft:(\d+)$"))
        async def draft_reply(event: events.CallbackQuery.Event) -> None:
            if self.storage is None:
                await event.answer("Legacy-отклики выключены", alert=True)
                return
            lead_id = int(event.pattern_match.group(1))
            await event.answer("Готовлю отклик..." if self.config.ai_reply_enabled else "AI-отклики выключены")

            lead = self.storage.get_lead(lead_id)
            if lead is None:
                await event.respond(f"Лид id {lead_id} не найден.")
                return

            cached = self.storage.get_ai_draft(lead_id)
            if cached is not None:
                await self._send_draft_response(
                    event,
                    lead,
                    format_reply_draft(lead, ReplyDraft.from_dict(cached)),
                )
                return

            if not self.config.ai_reply_enabled:
                await event.respond(
                    "AI-отклики выключены. Включи `AI_REPLY_ENABLED=true` и добавь `OPENAI_API_KEY` в `.env`."
                )
                return

            self.storage.mark_draft_requested(lead_id)
            try:
                profile = load_freelancer_profile(self.config.freelancer_profile_path)
                if self.reply_draft_provider is None:
                    raise ReplyDraftError("AI reply provider is not configured")
                draft = await asyncio.to_thread(self.reply_draft_provider.generate, lead, profile)
            except (OSError, ValueError, ReplyDraftError) as exc:
                LOGGER.warning("Could not generate draft for lead %s: %s", lead_id, exc)
                await event.respond(f"Не получилось подготовить отклик: {exc}")
                return

            self.storage.save_ai_draft(lead_id, draft.as_dict())
            await self._send_draft_response(event, lead, format_reply_draft(lead, draft))

        @self.bot_client.on(events.CallbackQuery(pattern=rb"^ignore:(\d+)$"))
        async def ignore_lead(event: events.CallbackQuery.Event) -> None:
            if self.storage is None:
                await event.answer("Legacy-доставка выключена", alert=True)
                return
            lead_id = int(event.pattern_match.group(1))
            self.storage.mark_ignored(lead_id)
            await event.answer("Лид помечен как ignored")

    async def _handle_delivery_action_callback(
        self,
        event: events.CallbackQuery.Event,
    ) -> None:
        try:
            action_type, delivery_id = decode_delivery_action_callback(event.data)
            outcome = await self.delivery_actions.record(
                delivery_id=delivery_id,
                action_type=action_type,
                actor_external_user_id=_telegram_user_id(event),
            )
        except DeliveryActionOwnershipError:
            await event.answer("Это действие вам недоступно", alert=True)
            return
        except (DeliveryActionError, ValueError):
            await event.answer("Действие больше недоступно", alert=True)
            return

        if action_type is DeliveryActionType.OPEN:
            await event.answer("Ссылка готова. Нажмите «Открыть» ещё раз")
            await event.edit(
                buttons=_opened_delivery_buttons(
                    delivery_id,
                    outcome.event.source_url,
                )
            )
        elif action_type is DeliveryActionType.NOT_SUITABLE:
            await event.answer("Понял, учту")
        else:
            await event.answer("Отлично, записал")

    async def _register_source_handlers(
        self,
    ) -> list[tuple[TelegramCollectorSource | Source, object]]:
        active: list[tuple[TelegramCollectorSource | Source, object]] = []
        registered_keys = getattr(self, "_registered_source_keys", set())
        governor = getattr(self, "_telegram_governor", None)
        for source in self.sources:
            try:
                async def resolve_source() -> object:
                    return await self.user_client.get_entity(_source_lookup(source))

                if governor is None:
                    entity = await resolve_source()
                else:
                    entity = await governor.run(
                        TelegramRequestCategory.ENTITY_ACCESS,
                        resolve_source,
                    )
            except (ValueError, RPCError) as exc:
                log_event(
                    LOGGER,
                    logging.WARNING,
                    "telegram.collector.source_resolution_failed",
                    source_id=_collector_source_id(source),
                    error=exc,
                )
                continue

            active.append((source, entity))

            source_key: object = _collector_source_id(source) or _source_lookup(source)
            if source_key not in registered_keys:
                @self.user_client.on(events.NewMessage(chats=entity))
                async def on_message(
                    event: events.NewMessage.Event,
                    source: TelegramCollectorSource | Source = source,
                ) -> None:
                    await self._dispatch_message(source, event.message, origin="live")

                registered_keys.add(source_key)

        self._registered_source_keys = registered_keys
        self._active_sources = active

        return active

    async def _send_draft_response(
        self,
        event: events.CallbackQuery.Event,
        lead,
        body: str,
    ) -> None:
        if lead.notification_message_id is not None:
            await self.bot_client.send_message(
                event.chat_id,
                body,
                parse_mode="html",
                link_preview=False,
                reply_to=lead.notification_message_id,
            )
            return

        await event.respond(body, parse_mode="html", link_preview=False)

    async def _catch_up(
        self,
        active_sources: Iterable[
            tuple[TelegramCollectorSource | Source, object]
        ],
    ) -> None:
        buffered: list[
            tuple[datetime, TelegramCollectorSource | Source, Message]
        ] = []
        fresh_started_at = getattr(self.config, "fresh_run_started_at", None)
        source_limit = getattr(self.config, "catch_up_source_limit", 1000)
        selected_sources = tuple(active_sources)
        if (
            getattr(self.config, "catch_up_newly_approved_sources_only", False)
            and fresh_started_at is not None
        ):
            selected_sources = tuple(
                (source, entity)
                for source, entity in selected_sources
                if _source_updated_at(source) is not None
                and _source_updated_at(source) >= fresh_started_at
            )
        selected_sources = selected_sources[:source_limit]
        governor = getattr(self, "_telegram_governor", None)
        for source, entity in selected_sources:
            try:
                async def read_history() -> list[Message]:
                    values: list[Message] = []
                    async for message in self.user_client.iter_messages(
                        entity,
                        limit=self.config.catch_up_limit,
                    ):
                        values.append(message)
                    return values

                if governor is None:
                    # Legacy unit fixtures construct LeadBot via __new__ and
                    # exercise the historical SQLite catch-up helper directly.
                    # Real runtime instances always initialize the governor
                    # before calling this method.
                    messages = await read_history()
                else:
                    messages = await governor.run(
                        TelegramRequestCategory.HISTORY,
                        read_history,
                    )
                for message in messages:
                    message_date = message.date or datetime.now(timezone.utc)
                    buffered.append((message_date, source, message))
            except RPCError as exc:
                log_event(
                    LOGGER,
                    logging.WARNING,
                    "telegram.collector.catch_up_failed",
                    source_id=_collector_source_id(source),
                    error=exc,
                )

        for _, source, message in sorted(buffered, key=lambda item: item[0]):
            await self._dispatch_message(source, message, origin="catch_up")

    async def _dispatch_message(
        self,
        source: TelegramCollectorSource | Source,
        message: Message,
        *,
        origin: str,
    ) -> None:
        correlation_id = new_correlation_id()
        with trace_context(correlation_id):
            log_event(
                LOGGER,
                logging.INFO,
                "telegram.collector.message_dispatched",
                correlation_id=correlation_id,
                source_id=_collector_source_id(source),
                telegram_message_id=int(message.id),
                origin=origin,
            )
            if isinstance(source, TelegramCollectorSource):
                if self.collector_account_id is None or self.raw_ingestor is None:
                    raise RuntimeError("Collector account is not initialized")
                try:
                    outcome = await self.raw_ingestor.ingest(
                        RawMessageInput(
                            source_id=source.record.id,
                            collector_account_id=self.collector_account_id,
                            external_message_id=int(message.id),
                            message_date=(
                                message.date or datetime.now(timezone.utc)
                            ),
                            observed_at=datetime.now(timezone.utc),
                            message_url=source.message_url(int(message.id)),
                            content=message.message or "",
                            transport_metadata=_transport_metadata(message),
                            ingestion_origin=RawMessageOrigin(origin),
                            correlation_id=correlation_id,
                        )
                    )
                except IneligibleRawMessageSource:
                    log_event(
                        LOGGER,
                        logging.WARNING,
                        "telegram.collector.message_refused",
                        correlation_id=correlation_id,
                        source_id=source.record.id,
                        telegram_message_id=int(message.id),
                        reason_code="source_ineligible",
                    )
                    return
                log_event(
                    LOGGER,
                    logging.INFO,
                    "telegram.collector.raw_message_persisted",
                    correlation_id=correlation_id,
                    source_id=source.record.id,
                    telegram_message_id=int(message.id),
                    raw_message_id=outcome.message.id,
                    processing_job_id=outcome.message.processing_job_id,
                    created=outcome.created,
                )
            await self._process_message(source, message)

    async def _process_message(
        self,
        source: TelegramCollectorSource | Source,
        message: Message,
    ) -> None:
        if not getattr(self.config, "legacy_delivery_enabled", False):
            return
        legacy_source = (
            source.legacy_source()
            if isinstance(source, TelegramCollectorSource)
            else source
        )
        await self.legacy_processor.handle(
            CollectedMessage(
                source=legacy_source,
                message_id=int(message.id),
                text=message.message or "",
                message_date=message.date or datetime.now(timezone.utc),
            )
        )

    def _configure_telegram_discovery_runtimes(self) -> None:
        database = getattr(self, "database", None)
        governor = getattr(self, "_telegram_governor", None)
        collector_account_id = getattr(self, "collector_account_id", None)
        if database is None or governor is None or collector_account_id is None:
            return

        if getattr(self.config, "telegram_chat_discovery_enabled", False):
            service = TelegramChatDiscoveryService(
                database,
                self.user_client,
                config=self.config,
                collector_account_id=collector_account_id,
                governor=governor,
                watch_candidate_callback=(
                    self._signal_source_discovery_wake
                    if getattr(self.config, "source_discovery_enabled", False)
                    else None
                ),
            )
            self._chat_discovery_runtime = TelegramChatDiscoveryRuntime(
                service,
                logger=LOGGER,
            )
            self._profile_discovery_runtime = None
            return

        if getattr(self.config, "source_discovery_enabled", False):
            self._profile_discovery_runtime = TelegramProfileDiscoveryRuntime(
                database,
                self.config,
                client=self.user_client,
                collector_account_id=collector_account_id,
                governor=governor,
                logger=LOGGER,
                worker_id=f"telegram-profile-discovery-{id(self)}",
            )

    async def _stop_telegram_chat_discovery_runtime(self) -> None:
        runtime = getattr(self, "_chat_discovery_runtime", None)
        if runtime is None:
            return
        runtime.request_stop()
        task = getattr(self, "_chat_discovery_task", None)
        if task is None:
            return
        try:
            await task
        finally:
            self._chat_discovery_task = None

    def _log_runtime_mode(self) -> None:
        opportunity_provider = getattr(
            self.config,
            "opportunity_analysis_provider",
            "openai",
        )
        opportunity_ai_enabled = opportunity_analysis_provider_available(self.config)
        opportunity_fallback_enabled = (
            getattr(self.config, "opportunity_analysis_fallback_enabled", False)
            and opportunity_analysis_provider_available(self.config, fallback=True)
        )
        source_provider_available = source_ai_provider_available(self.config)
        source_audit_enabled = (
            getattr(self.config, "source_audit_enabled", False)
            and source_provider_available
        )
        log_event(
            LOGGER,
            logging.INFO,
            "runtime.mode_summary",
            runtime_mode=("run" if getattr(self, "_background_enabled", True) else "bot_only"),
            background_enabled=getattr(self, "_background_enabled", True),
            v2_ingestion_enabled=True,
            opportunity_ai_enabled=opportunity_ai_enabled,
            opportunity_analysis_provider=opportunity_provider,
            opportunity_analysis_fallback_enabled=opportunity_fallback_enabled,
            opportunity_analysis_fallback_provider=getattr(
                self.config,
                "opportunity_analysis_fallback_provider",
                "openai",
            ),
            v2_matching_enabled=True,
            v2_personalized_delivery_enabled=True,
            legacy_delivery_enabled=getattr(
                self.config,
                "legacy_delivery_enabled",
                False,
            ),
            source_discovery_enabled=getattr(
                self.config,
                "source_discovery_enabled",
                False,
            ),
            telegram_chat_discovery_enabled=getattr(
                self.config,
                "telegram_chat_discovery_enabled",
                False,
            ),
            source_audit_enabled=source_audit_enabled,
            source_audit_provider=getattr(self.config, "source_audit_provider", "unknown"),
            chat_screen_provider_available=source_provider_available,
        )

    async def _run_source_discovery_loop(self) -> None:
        interval = getattr(self.config, "source_discovery_interval_seconds", 6 * 60 * 60)
        first_cycle = getattr(self, "_source_discovery_first_cycle", None)
        wake = getattr(self, "_source_discovery_wake", None)
        while True:
            try:
                if self.source_discovery_runtime is None:
                    if first_cycle is not None:
                        first_cycle.set()
                    return
                if wake is not None:
                    # Consume a signal before the cycle. Signals arriving while
                    # the cycle is running remain set for the next cycle.
                    wake.clear()
                cycle = await self.source_discovery_runtime.run_once()
                if cycle.reload_required:
                    await self._reload_approved_sources()
                if first_cycle is not None:
                    first_cycle.set()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                if getattr(self.config, "catch_up_after_source_discovery", False):
                    self._source_discovery_error = exc
                    if first_cycle is not None:
                        first_cycle.set()
                    return
                log_event(
                    LOGGER,
                    logging.WARNING,
                    "source.discovery.runtime_failed",
                    error=exc,
                )
            if wake is None:
                await asyncio.sleep(interval)
            else:
                try:
                    await asyncio.wait_for(wake.wait(), timeout=interval)
                except asyncio.TimeoutError:
                    pass
                else:
                    wake.clear()

    async def _signal_source_discovery_wake(self, source_id: int) -> None:
        wake = getattr(self, "_source_discovery_wake", None)
        if wake is None:
            return
        wake.set()
        log_event(
            LOGGER,
            logging.INFO,
            "source.discovery.wake_requested",
            source_id=source_id,
            reason="telegram_chat_watch",
        )

    async def _reload_approved_sources(
        self,
    ) -> list[tuple[TelegramCollectorSource | Source, object]]:
        previous_active = getattr(self, "_active_sources", ())
        previous_keys = {
            _source_identity_key(source)
            for source, _entity in previous_active
        }
        snapshot = await self.source_adapter.list_for_session(self.user_client)
        self.collector_account_id = snapshot.collector_account.id
        self.sources = list(snapshot.sources)
        active = await self._register_source_handlers()
        self._active_sources = active
        newly_active = [
            item
            for item in active
            if _source_identity_key(item[0]) not in previous_keys
        ]
        if (
            newly_active
            and getattr(self.config, "send_catch_up", False)
            and getattr(self.config, "catch_up_limit", 0) > 0
        ):
            await self._catch_up(newly_active)
            caught_up_keys = getattr(self, "_source_discovery_caught_up_keys", None)
            if caught_up_keys is None:
                caught_up_keys = set()
                self._source_discovery_caught_up_keys = caught_up_keys
            caught_up_keys.update(
                _source_identity_key(source)
                for source, _entity in newly_active
            )
        log_event(
            LOGGER,
            logging.INFO,
            "telegram.collector.source_catalog_reloaded",
            collector_account_id=self.collector_account_id,
            source_count=len(self.sources),
            active_source_count=len(active),
            newly_active_source_count=len(newly_active),
        )
        return newly_active

    async def _stop_source_discovery_loop(self) -> None:
        task = getattr(self, "_source_discovery_task", None)
        if task is None:
            return
        self._source_discovery_task = None
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

    async def _wait_until_stopped(self) -> None:
        stop_event = asyncio.Event()
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(sig, stop_event.set)
            except NotImplementedError:
                pass
        await stop_event.wait()


async def run_app(*, mode: RuntimeMode = RuntimeMode.RUN) -> None:
    config = RuntimeConfig.from_env(mode=mode)
    configure_structured_logger(
        "freelancer_bot",
        redactor=Redactor.from_config(config),
        level=getattr(logging, config.log_level, logging.INFO),
    )
    app = LeadBot(config, background_enabled=mode is RuntimeMode.RUN)
    try:
        await app.run()
    finally:
        await app.shutdown()

async def check_sources(config: RuntimeConfig) -> None:
    """Resolve configured public usernames with the user Telegram session."""
    sources = load_sources(config.sources_path)
    config.user_session_path.parent.mkdir(parents=True, exist_ok=True)
    if config.api_id is None:
        raise ConfigurationError("TELEGRAM_API_ID/API_ID is required to check sources")
    with TelegramSessionFileLock(config.user_session_path, role="source_check"):
        client = TelegramClient(
            str(config.user_session_path),
            config.api_id,
            _required_secret(config.api_hash, "TELEGRAM_API_HASH/API_HASH"),
        )
        try:
            await client.start()
            enabled_count = sum(source.enabled for source in sources)
            print(f"Проверяю источники: {enabled_count} активных, {len(sources) - enabled_count} отключенных")
            for source in sources:
                if not source.enabled:
                    print(f"SKIP  {source.handle:<32} {source.title} (отключен в конфиге)")
                    continue
                try:
                    await client.get_entity(source.handle)
                except (ValueError, RPCError) as exc:
                    print(f"FAIL  {source.handle:<32} {source.title}: {exc}")
                else:
                    print(f"OK    {source.handle:<32} {source.title}")
        finally:
            await client.disconnect()


def cli() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Telegram freelance lead bot. No runtime starts without an explicit "
            "mode flag."
        )
    )
    parser.add_argument("--check-filter", help="Check a text against the current keyword filter.")
    parser.add_argument("--draft-text", help="Generate an AI reply draft for a sample lead text.")
    parser.add_argument("--check-config", action="store_true", help="Validate JSON sources and filter configs.")
    parser.add_argument("--check-sources", action="store_true", help="Resolve enabled sources through Telegram.")
    parser.add_argument(
        "--run",
        action="store_true",
        help="Explicitly start the full collector, durable workers, matching and delivery runtime.",
    )
    parser.add_argument(
        "--bot-only",
        action="store_true",
        help="Explicitly start only the user-facing bot UI; no user collector or background workers.",
    )
    parser.add_argument(
        "--collector-only",
        action="store_true",
        help="Run the authenticated PostgreSQL-backed collector without the user-facing bot.",
    )
    args = parser.parse_args()

    if args.check_config:
        config = RuntimeConfig.from_env(mode=RuntimeMode.CHECK_CONFIG)
        sources = load_sources(config.sources_path)
        filters = load_filter_config(config.filters_path)
        enabled_count = sum(source.enabled for source in sources)
        print(f"OK: {config.sources_path} ({enabled_count}/{len(sources)} источников включено)")
        print(
            f"OK: {config.filters_path} ({len(filters.keywords)} ключевых слов, "
            f"{len(filters.stop_words)} стоп-слов, min_score={filters.min_score})"
        )
        return

    if args.check_filter:
        config = RuntimeConfig.from_env(mode=RuntimeMode.CHECK_FILTER)
        result = match_text(args.check_filter, load_filter_config(config.filters_path))
        print(result)
        return

    if args.check_sources:
        config = RuntimeConfig.from_env(mode=RuntimeMode.CHECK_SOURCES)
        asyncio.run(check_sources(config))
        return

    if args.collector_only:
        asyncio.run(run_collector_only())
        return

    if args.bot_only:
        asyncio.run(run_app(mode=RuntimeMode.BOT_ONLY))
        return

    if args.run:
        asyncio.run(run_app(mode=RuntimeMode.RUN))
        return

    if args.draft_text:
        config = RuntimeConfig.from_env(mode=RuntimeMode.DRAFT_TEXT)
        profile = load_freelancer_profile(config.freelancer_profile_path)
        filter_config: FilterConfig = load_filter_config(config.filters_path)
        lead = StoredLead(
            id=0,
            source="manual",
            message_id=0,
            link="",
            text=args.draft_text,
            score=0,
            keywords=match_text(args.draft_text, filter_config).matched_keywords,
            message_date=datetime.now(timezone.utc).isoformat(),
            status="new",
            ai_draft_json=None,
            notification_chat_id=None,
            notification_message_id=None,
        )
        generator = _build_reply_draft_provider(config)
        draft = generator.generate(lead, profile)
        print("Fit:", f"{draft.fit_score}/100")
        print("Summary:", draft.fit_summary)
        print("\nRisks:")
        for risk in draft.risks:
            print("-", risk)
        print("\nQuestions:")
        for question in draft.questions_to_client:
            print("-", question)
        print("\nShort reply:\n", draft.short_reply, sep="")
        print("\nFull reply:\n", draft.proposal_draft, sep="")
        return

    parser.print_help()
    print(
        "\nNo runtime started. Choose --bot-only, --collector-only or --run explicitly."
    )


def _build_reply_draft_provider(config: RuntimeConfig) -> ReplyDraftProvider:
    if config.ai_provider is not AIProvider.OPENAI:
        raise ConfigurationError(f"Unsupported AI reply provider: {config.ai_provider.value}")
    return OpenAIReplyDraftGenerator(
        _required_secret(config.openai_api_key, "OPENAI_API_KEY"),
        config.openai_model,
        temperature=config.ai_reply_temperature,
        timeout_seconds=config.ai_request_timeout_seconds,
    )


def _build_profile_onboarding(
    config: RuntimeConfig,
    database: Database,
    *,
    confirmation: ProfileConfirmationService | None = None,
) -> TelegramProfileOnboarding:
    ai_onboarding: ProfileOnboardingService | None = None
    try:
        analyzer = OpenAICompatibleOnboardingProfileAnalyzer.from_config(config)
    except OnboardingProfileError:
        pass
    else:
        ai_onboarding = ProfileOnboardingService(database, analyzer)
    return TelegramProfileOnboarding(
        confirmation or ProfileConfirmationService(database),
        ai_onboarding,
    )


async def _respond_onboarding(event, response: TelegramOnboardingResponse) -> None:
    await _respond_navigation(event, response)


async def _respond_navigation(event, response: TelegramOnboardingResponse) -> None:
    buttons = [
        [Button.inline(button.label, data=button.data) for button in row]
        for row in response.buttons
    ]
    await event.respond(
        response.text,
        parse_mode="html",
        buttons=buttons or None,
    )


def _main_navigation_keyboard() -> list[list[object]]:
    return [
        [Button.text("Мой поиск", resize=True, single_use=False),
         Button.text("Настройки", resize=True, single_use=False)],
        [Button.text("Подписка", resize=True, single_use=False)],
    ]


def _event_text(event) -> str:
    value = getattr(event, "raw_text", None)
    if value is None:
        value = getattr(event, "text", None)
    return value.strip() if isinstance(value, str) else ""


def _telethon_action_buttons(
    buttons: tuple[tuple[TelegramDeliveryActionButton, ...], ...],
) -> list[list[object]]:
    return [
        [Button.inline(button.label, data=button.data) for button in row]
        for row in buttons
    ]


def _opened_delivery_buttons(
    delivery_id: UUID,
    source_url: str,
) -> list[list[object]]:
    feedback_rows = delivery_action_buttons(
        delivery_id,
        source_available=False,
    )
    return [
        [Button.url("Открыть", source_url)],
        *_telethon_action_buttons(feedback_rows),
    ]


def _telegram_user_id(event) -> str:
    identifier = event.sender_id if event.sender_id is not None else event.chat_id
    if identifier is None:
        raise ValueError("Telegram user identity is unavailable")
    return str(identifier)


_PROFILE_INPUT_ERRORS = (
    ValueError,
    UserNotFound,
    SearchProfileNotFound,
    SearchProfileOwnershipError,
    SearchProfileEditConflict,
    SearchProfileActivationError,
)


def _required_secret(value: SecretStr | None, name: str) -> str:
    if value is None or not value.get_secret_value().strip():
        raise ConfigurationError(f"{name} is required")
    return value.get_secret_value()


def _source_lookup(source: TelegramCollectorSource | Source) -> str:
    return source.lookup if isinstance(source, TelegramCollectorSource) else source.handle


def _collector_source_id(source: TelegramCollectorSource | Source) -> int | None:
    return source.record.id if isinstance(source, TelegramCollectorSource) else None


def _source_identity_key(source: TelegramCollectorSource | Source) -> tuple[str, object]:
    source_id = _collector_source_id(source)
    if source_id is not None:
        return ("id", source_id)
    return ("lookup", _source_lookup(source))


def _source_updated_at(source: TelegramCollectorSource | Source) -> datetime | None:
    if isinstance(source, TelegramCollectorSource):
        return source.record.updated_at
    return None


def _transport_metadata(message: Message) -> dict[str, object]:
    metadata: dict[str, object] = {}
    scalar_fields = (
        "chat_id",
        "sender_id",
        "reply_to_msg_id",
        "grouped_id",
        "via_bot_id",
        "post",
        "mentioned",
        "out",
        "silent",
    )
    for field in scalar_fields:
        value = getattr(message, field, None)
        if isinstance(value, (bool, int, str)):
            metadata[field] = value
    edit_date = getattr(message, "edit_date", None)
    if isinstance(edit_date, datetime):
        metadata["edit_date"] = edit_date.astimezone(timezone.utc).isoformat()
    media = getattr(message, "media", None)
    if media is not None:
        metadata["media_type"] = type(media).__name__
    action = getattr(message, "action", None)
    if action is not None:
        metadata["service_action_type"] = type(action).__name__
    return metadata
