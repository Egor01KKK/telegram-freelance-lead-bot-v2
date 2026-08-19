from __future__ import annotations

import asyncio
from decimal import Decimal
from itertools import combinations
import unittest
from uuid import UUID

import sqlalchemy as sa
from sqlalchemy.exc import IntegrityError

from freelancer_bot.persistence.database import Database
from freelancer_bot.persistence.schema import search_profiles
from freelancer_bot.persistence.search_profiles import SearchProfileEditConflict
from freelancer_bot.profile_confirmation import (
    ProfileConfirmationService,
    format_profile_summary,
)
from freelancer_bot.search_profiles import (
    BudgetPolicy,
    OpportunityType,
    SearchProfileTermOrigin,
    WorkMode,
    empty_search_profile_preferences,
    parse_search_profile_preferences,
)
from freelancer_bot.telegram_onboarding import (
    TelegramProfileOnboarding,
    parse_budget_setting,
)
from postgres_support import TEST_DATABASE_URL, migrate_to_head, temporary_database


class SearchProfilePreferencesContractTest(unittest.TestCase):
    def test_unknown_preferences_are_distinct_from_explicit_empty_values(self):
        unknown = empty_search_profile_preferences()
        explicit_empty = parse_search_profile_preferences(
            work_types=(),
            languages=(),
            geographies=(),
            work_modes=(),
            excluded_categories=(),
        )

        self.assertIsNone(unknown.work_types)
        self.assertEqual(explicit_empty.work_types, ())
        self.assertIsNone(unknown.languages)
        self.assertEqual(explicit_empty.languages, ())
        for opportunity_type in OpportunityType:
            self.assertFalse(unknown.accepts_work_type(opportunity_type))
            self.assertFalse(explicit_empty.accepts_work_type(opportunity_type))

    def test_all_supported_work_type_combinations_have_exact_policy_behavior(self):
        supported = tuple(OpportunityType)
        observed = 0
        for size in range(len(supported) + 1):
            for selected in combinations(supported, size):
                preferences = parse_search_profile_preferences(
                    work_types=selected,
                )
                self.assertEqual(preferences.work_types, selected)
                for opportunity_type in supported:
                    self.assertEqual(
                        preferences.accepts_work_type(opportunity_type),
                        opportunity_type in selected,
                    )
                observed += 1
        self.assertEqual(observed, 16)

    def test_structured_budget_location_language_mode_and_exclusions_are_explicit(self):
        preferences = parse_search_profile_preferences(
            minimum_budget="80000.50",
            currency="rub",
            budget_policy="require_explicit",
            languages=("Русский", "русский", "English"),
            geographies=("Россия", "Москва"),
            work_modes=("remote", "hybrid"),
            excluded_categories=("Gambling", "Adult"),
        )

        self.assertEqual(preferences.minimum_budget, Decimal("80000.50"))
        self.assertEqual(preferences.currency, "RUB")
        self.assertEqual(preferences.budget_policy, BudgetPolicy.REQUIRE_EXPLICIT)
        self.assertEqual(
            [term.value for term in preferences.languages or ()],
            ["Русский", "English"],
        )
        self.assertTrue(
            all(
                term.origin is SearchProfileTermOrigin.EXPLICIT
                for term in preferences.excluded_categories or ()
            )
        )
        self.assertEqual(
            preferences.work_modes,
            (WorkMode.REMOTE, WorkMode.HYBRID),
        )

    def test_invalid_settings_are_rejected_instead_of_guessed(self):
        invalid = (
            {"minimum_budget": "10", "currency": None},
            {"minimum_budget": "10.999", "currency": "RUB"},
            {"minimum_budget": "-1", "currency": "RUB"},
            {"minimum_budget": "10", "currency": "RUBLE"},
            {"work_types": ("freelance",)},
            {"work_modes": ("sometimes",)},
            {"budget_policy": "guess"},
        )
        for settings in invalid:
            with self.subTest(settings=settings), self.assertRaises(
                (TypeError, ValueError)
            ):
                parse_search_profile_preferences(**settings)

        self.assertEqual(
            parse_budget_setting("-,-,allow_unknown"),
            (None, None, BudgetPolicy.ALLOW_UNKNOWN),
        )
        with self.assertRaises(ValueError):
            parse_budget_setting("80000,RUB")


@unittest.skipUnless(TEST_DATABASE_URL, "TEST_DATABASE_URL is not configured")
class SearchProfilePreferencesIntegrationTest(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.database_context = temporary_database()
        self.database_url = self.database_context.__enter__()
        migrate_to_head(self.database_url)
        self.database = Database(self.database_url, pool_size=4, max_overflow=8)
        self.service = ProfileConfirmationService(self.database)

    async def asyncTearDown(self):
        await self.database.close()
        self.database_context.__exit__(None, None, None)

    async def test_all_work_type_combinations_round_trip_without_affecting_semantics(self):
        draft = await self._draft("combination-user")
        original_text = draft.profile.semantic_text_original
        revision = draft.profile.revision
        supported = tuple(OpportunityType)
        observed = 0

        for size in range(len(supported) + 1):
            for selected in combinations(supported, size):
                updated = await self.service.set_work_types(
                    platform="telegram",
                    external_user_id="combination-user",
                    profile_id=draft.profile.id,
                    work_types=selected,
                    expected_revision=revision,
                )
                revision = updated.profile.revision
                self.assertEqual(updated.profile.preferences.work_types, selected)
                for opportunity_type in supported:
                    self.assertEqual(
                        updated.profile.preferences.accepts_work_type(
                            opportunity_type
                        ),
                        opportunity_type in selected,
                    )
                observed += 1

        self.assertEqual(observed, 16)
        self.assertEqual(updated.profile.semantic_text_original, original_text)
        self.assertEqual(updated.profile.revision, draft.profile.revision + 16)

    async def test_all_preferences_persist_as_one_versioned_explicit_contract(self):
        draft = await self._draft("settings-user")
        profile_id = draft.profile.id
        revision = draft.profile.revision

        work = await self.service.set_work_types(
            platform="telegram",
            external_user_id="settings-user",
            profile_id=profile_id,
            work_types=(
                OpportunityType.ONE_OFF_ORDER,
                OpportunityType.PROJECT,
                OpportunityType.VACANCY,
                OpportunityType.PART_TIME_CONTRACTOR,
            ),
            expected_revision=revision,
        )
        budget = await self.service.set_budget(
            platform="telegram",
            external_user_id="settings-user",
            profile_id=profile_id,
            minimum_budget="80000",
            currency="rub",
            budget_policy=BudgetPolicy.ALLOW_UNKNOWN,
            expected_revision=work.profile.revision,
        )
        languages = await self.service.set_term_preferences(
            platform="telegram",
            external_user_id="settings-user",
            profile_id=profile_id,
            field="languages",
            values=("Русский", "English"),
            expected_revision=budget.profile.revision,
        )
        geography = await self.service.set_term_preferences(
            platform="telegram",
            external_user_id="settings-user",
            profile_id=profile_id,
            field="geographies",
            values=("Россия", "Москва"),
            expected_revision=languages.profile.revision,
        )
        modes = await self.service.set_work_modes(
            platform="telegram",
            external_user_id="settings-user",
            profile_id=profile_id,
            work_modes=(WorkMode.REMOTE, WorkMode.HYBRID),
            expected_revision=geography.profile.revision,
        )
        final = await self.service.set_term_preferences(
            platform="telegram",
            external_user_id="settings-user",
            profile_id=profile_id,
            field="excluded_categories",
            values=("Gambling", "Adult"),
            expected_revision=modes.profile.revision,
        )

        preferences = final.profile.preferences
        self.assertEqual(preferences.minimum_budget, Decimal("80000"))
        self.assertEqual(preferences.currency, "RUB")
        self.assertEqual(preferences.budget_policy, BudgetPolicy.ALLOW_UNKNOWN)
        self.assertEqual(
            [term.normalized_value for term in preferences.excluded_categories or ()],
            ["gambling", "adult"],
        )
        summary = format_profile_summary(final)
        self.assertIn("разовые заказы", summary)
        self.assertIn("от 80000 RUB", summary)
        self.assertIn("Русский, English", summary)
        self.assertIn("Россия, Москва", summary)
        self.assertIn("удалённо, гибрид", summary)
        self.assertIn("Gambling, Adult", summary)
        self.assertEqual(
            final.profile.semantic_text_original,
            draft.profile.semantic_text_original,
        )

    async def test_concurrent_settings_and_post_confirmation_edits_are_protected(self):
        draft = await self._draft("protected-user")

        async def set_types(value: OpportunityType):
            return await self.service.set_work_types(
                platform="telegram",
                external_user_id="protected-user",
                profile_id=draft.profile.id,
                work_types=(value,),
                expected_revision=draft.profile.revision,
            )

        outcomes = await asyncio.gather(
            set_types(OpportunityType.PROJECT),
            set_types(OpportunityType.VACANCY),
            return_exceptions=True,
        )
        self.assertEqual(sum(not isinstance(item, Exception) for item in outcomes), 1)
        self.assertEqual(
            sum(isinstance(item, SearchProfileEditConflict) for item in outcomes),
            1,
        )
        current = await self.service.show(
            platform="telegram",
            external_user_id="protected-user",
            profile_id=draft.profile.id,
        )
        confirmed = await self.service.confirm(
            platform="telegram",
            external_user_id="protected-user",
            profile_id=draft.profile.id,
            expected_revision=current.profile.revision,
        )
        updated = await self.service.set_budget(
            platform="telegram",
            external_user_id="protected-user",
            profile_id=draft.profile.id,
            minimum_budget=None,
            currency=None,
            budget_policy=BudgetPolicy.REQUIRE_EXPLICIT,
            expected_revision=confirmed.profile.revision,
        )
        self.assertEqual(updated.profile.revision, confirmed.profile.revision + 1)
        with self.assertRaises(SearchProfileEditConflict):
            await self.service.set_budget(
                platform="telegram",
                external_user_id="protected-user",
                profile_id=draft.profile.id,
                minimum_budget=None,
                currency=None,
                budget_policy=BudgetPolicy.REQUIRE_EXPLICIT,
                expected_revision=confirmed.profile.revision,
            )

    async def test_telegram_manual_flow_edits_settings_without_ai(self):
        telegram = TelegramProfileOnboarding(self.service, None)
        draft = await telegram.create_manual(
            external_user_id="manual-settings-user",
            payload="Разработчик | Python | Telegram",
        )
        profile_id = _profile_id_from_button(draft.buttons[0][0].data)
        shown = await self.service.show(
            platform="telegram",
            external_user_id="manual-settings-user",
            profile_id=profile_id,
        )

        work = await telegram.edit_setting(
            external_user_id="manual-settings-user",
            profile_id=profile_id,
            field="work_types",
            value="one_off_order,project",
            expected_revision=shown.profile.revision,
        )
        current = await self.service.show(
            platform="telegram",
            external_user_id="manual-settings-user",
            profile_id=profile_id,
        )
        budget = await telegram.edit_setting(
            external_user_id="manual-settings-user",
            profile_id=profile_id,
            field="budget",
            value="-,-,require_explicit",
            expected_revision=current.profile.revision,
        )
        toggled = await telegram.toggle_work_type(
            external_user_id="manual-settings-user",
            profile_id=profile_id,
            work_type=OpportunityType.VACANCY,
            expected_revision=current.profile.revision + 1,
        )

        self.assertIn("разовые заказы, проекты", work.text)
        self.assertIn("только с указанным бюджетом", budget.text)
        self.assertIn("вакансии", toggled.text)
        self.assertTrue(
            all(
                len(button.data) <= 64
                for row in toggled.buttons
                for button in row
            )
        )

    async def test_database_rejects_malformed_preference_contract(self):
        draft = await self._draft("constraint-user")
        with self.assertRaises(IntegrityError):
            async with self.database.transaction() as connection:
                await connection.execute(
                    sa.update(search_profiles)
                    .where(search_profiles.c.id == draft.profile.id)
                    .values(preferences={"schema_version": "wrong"})
                )

    async def _draft(self, external_user_id: str):
        return await self.service.create_manual_draft(
            platform="telegram",
            external_user_id=external_user_id,
            semantic_text="Разработчик | Python | Telegram",
            roles=("Разработчик",),
            skills=("Python",),
            categories=("Telegram",),
        )


def _profile_id_from_button(data: bytes) -> UUID:
    return UUID(data.decode("ascii").split(":")[2])


if __name__ == "__main__":
    unittest.main()
