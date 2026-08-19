from __future__ import annotations

from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock

from freelancer_bot.app import LeadBot
from freelancer_bot.persistence.database import Database
from freelancer_bot.profile_confirmation import ProfileConfirmationService
from freelancer_bot.profile_onboarding import OnboardingProfileError
from freelancer_bot.telegram_navigation import TelegramNavigationService
from freelancer_bot.telegram_onboarding import TelegramProfileOnboarding
from postgres_support import TEST_DATABASE_URL, migrate_to_head, temporary_database


class _HandlerClient:
    def __init__(self):
        self.handlers = []

    def on(self, event_builder):
        def register(handler):
            self.handlers.append((event_builder, handler))
            return handler

        return register


class _RecordingProfileOnboarding:
    def __init__(self, response):
        self.response = response
        self.begin_calls = []
        self.create_manual = AsyncMock()

    async def begin(self, *, external_user_id: str, description: str):
        self.begin_calls.append((external_user_id, description))
        return self.response


class _TelegramEvent(SimpleNamespace):
    def __init__(self, *, text: str | None = None):
        super().__init__(
            sender_id=4242,
            chat_id=4242,
            raw_text=text,
            answer=AsyncMock(),
            respond=AsyncMock(),
        )


class _UnavailableAI:
    async def create_from_description(self, **kwargs):
        raise OnboardingProfileError("provider unavailable")


class TelegramNavigationHandlerTest(unittest.IsolatedAsyncioTestCase):
    async def test_new_search_routes_natural_text_to_ai_onboarding_handler(self):
        bot = LeadBot.__new__(LeadBot)
        bot.bot_client = _HandlerClient()
        bot._pending_navigation_inputs = {}
        bot.profile_onboarding = _RecordingProfileOnboarding(
            SimpleNamespace(text="Профиль поиска", buttons=(), retryable=False)
        )
        bot._register_bot_commands()
        bot._register_callback_handlers()

        new_search = next(
            handler
            for _, handler in bot.bot_client.handlers
            if handler.__name__ == "navigation_new_profile"
        )
        text_input = next(
            handler
            for _, handler in bot.bot_client.handlers
            if handler.__name__ == "navigation_text_input"
        )

        await new_search(_TelegramEvent())
        event = _TelegramEvent(
            text=(
                "Я Python-разработчик и специалист по автоматизации. "
                "Делаю Telegram-ботов, Mini Apps и API-интеграции."
            )
        )
        await text_input(event)

        self.assertEqual(
            bot.profile_onboarding.begin_calls,
            [
                (
                    "4242",
                    "Я Python-разработчик и специалист по автоматизации. "
                    "Делаю Telegram-ботов, Mini Apps и API-интеграции.",
                )
            ],
        )
        bot.profile_onboarding.create_manual.assert_not_awaited()
        self.assertEqual(event.respond.await_args.args[0], "Профиль поиска")
        self.assertNotIn("4242", bot._pending_navigation_inputs)

    async def test_ai_unavailable_is_retryable_and_never_invokes_manual_parser(self):
        bot = LeadBot.__new__(LeadBot)
        bot.bot_client = _HandlerClient()
        bot._pending_navigation_inputs = {}
        bot.profile_onboarding = _RecordingProfileOnboarding(
            SimpleNamespace(
                text="Не удалось обработать описание через AI. Попробуйте ещё раз.",
                buttons=(),
                retryable=True,
            )
        )
        bot._register_bot_commands()
        bot._register_callback_handlers()

        new_search = next(
            handler
            for _, handler in bot.bot_client.handlers
            if handler.__name__ == "navigation_new_profile"
        )
        text_input = next(
            handler
            for _, handler in bot.bot_client.handlers
            if handler.__name__ == "navigation_text_input"
        )

        await new_search(_TelegramEvent())
        event = _TelegramEvent(text="Нужен Python backend")
        await text_input(event)

        bot.profile_onboarding.create_manual.assert_not_awaited()
        self.assertIn("4242", bot._pending_navigation_inputs)
        self.assertIn("Попробуйте", event.respond.await_args.args[0])

    async def test_real_onboarding_adapter_marks_provider_error_retryable(self):
        confirmation = SimpleNamespace(show=AsyncMock())
        onboarding = TelegramProfileOnboarding(confirmation, _UnavailableAI())

        response = await onboarding.begin(
            external_user_id="4242",
            description="Нужен Python backend",
        )

        self.assertTrue(response.retryable)
        self.assertIn("Попробуйте отправить описание ещё раз", response.text)
        confirmation.show.assert_not_awaited()


@unittest.skipUnless(TEST_DATABASE_URL, "TEST_DATABASE_URL is not configured")
class TelegramNavigationIntegrationTest(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.database_context = temporary_database()
        self.database_url = self.database_context.__enter__()
        migrate_to_head(self.database_url)
        self.database = Database(self.database_url, pool_size=4, max_overflow=8)
        self.confirmation = ProfileConfirmationService(self.database)
        self.onboarding = TelegramProfileOnboarding(self.confirmation, None)
        self.navigation = TelegramNavigationService(self.confirmation)

    async def asyncTearDown(self):
        await self.database.close()
        self.database_context.__exit__(None, None, None)

    async def test_navigation_keeps_profiles_isolated_and_exposes_controls(self):
        first = await self._draft("navigation-owner", "first")
        second = await self._draft("navigation-owner", "second")

        search = await self.navigation.my_search(
            external_user_id="navigation-owner",
        )
        self.assertIn("Мой поиск", search.text)
        profile_callbacks = [
            button.data
            for row in search.buttons
            for button in row
            if button.data.startswith(b"nav:profile:")
            and button.data != b"nav:profile:new"
        ]
        self.assertEqual(len(profile_callbacks), 2)
        self.assertTrue(any(str(first.profile.id).encode() in data for data in profile_callbacks))
        self.assertTrue(any(str(second.profile.id).encode() in data for data in profile_callbacks))

        first_settings = await self.navigation.settings_for_profile(
            external_user_id="navigation-owner",
            profile_id=first.profile.id,
        )
        second_settings = await self.navigation.settings_for_profile(
            external_user_id="navigation-owner",
            profile_id=second.profile.id,
        )
        first_id = str(first.profile.id).encode("ascii")
        second_id = str(second.profile.id).encode("ascii")
        first_controls = [
            button.data
            for row in first_settings.buttons
            for button in row
            if button.data.startswith(b"nav:")
        ]
        second_controls = [
            button.data
            for row in second_settings.buttons
            for button in row
            if button.data.startswith(b"nav:")
        ]
        self.assertTrue(any(first_id in data for data in first_controls))
        self.assertFalse(any(second_id in data for data in first_controls))
        self.assertTrue(any(second_id in data for data in second_controls))
        self.assertTrue(all(len(data) <= 64 for data in first_controls + second_controls))

        subscription = await self.navigation.subscription(
            external_user_id="navigation-owner",
        )
        self.assertIn("Пробный период ещё не начался", subscription.text)
        self.assertIn("Тариф: 990 RUB/month", subscription.text)

    async def test_confirmed_profile_settings_can_be_changed_from_navigation_state(self):
        draft = await self._draft("settings-owner", "settings")
        confirmed = await self.confirmation.confirm(
            platform="telegram",
            external_user_id="settings-owner",
            profile_id=draft.profile.id,
            expected_revision=draft.profile.revision,
        )
        activated = await self.confirmation.activate(
            platform="telegram",
            external_user_id="settings-owner",
            profile_id=confirmed.profile.id,
            expected_revision=confirmed.profile.revision,
        )

        profile = await self.navigation.profile(
            external_user_id="settings-owner",
            profile_id=draft.profile.id,
        )
        self.assertIn("Остановить поиск", " ".join(
            button.label for row in profile.buttons for button in row
        ))
        settings = await self.navigation.settings_for_profile(
            external_user_id="settings-owner",
            profile_id=draft.profile.id,
        )
        self.assertIn("Бюджет", " ".join(
            button.label for row in settings.buttons for button in row
        ))
        self.assertFalse(any(
            button.data.endswith(b":roles")
            for row in settings.buttons
            for button in row
        ))

        updated = await self.onboarding.edit_setting(
            external_user_id="settings-owner",
            profile_id=draft.profile.id,
            field="budget",
            value="80000,RUB,allow_unknown",
            expected_revision=activated.profile.profile.revision,
        )
        self.assertIn("от 80000 RUB", updated.text)
        subscription = await self.navigation.subscription(
            external_user_id="settings-owner",
        )
        self.assertIn("Пробный период активирован", subscription.text)
        self.assertIn("действует до", subscription.text)

    async def _draft(self, external_user_id: str, suffix: str):
        return await self.confirmation.create_manual_draft(
            platform="telegram",
            external_user_id=external_user_id,
            semantic_text=f"Разработчик {suffix} | Python | Telegram",
            roles=(f"Разработчик {suffix}",),
            skills=("Python",),
            categories=("Telegram",),
        )


if __name__ == "__main__":
    unittest.main()
