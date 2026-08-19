from __future__ import annotations

import asyncio
from hashlib import sha256
import json
import unittest
from uuid import UUID

import sqlalchemy as sa

from freelancer_bot.persistence.database import Database
from freelancer_bot.persistence.schema import (
    search_profile_analysis_cache,
    search_profiles,
)
from freelancer_bot.persistence.search_profiles import (
    SearchProfileAnalysisCacheRepository,
    SearchProfileConfirmationStatus,
    SearchProfileEditConflict,
    SearchProfileOwnershipError,
)
from freelancer_bot.profile_confirmation import (
    ProfileConfirmationService,
    format_profile_summary,
)
from freelancer_bot.profile_onboarding import (
    ONBOARDING_PROFILE_ANALYSIS_SCHEMA_VERSION,
    ONBOARDING_PROFILE_ANALYZER_VERSION,
    ONBOARDING_PROFILE_PROMPT_VERSION,
    OnboardingProfileAnalysis,
    OnboardingProfileAnalysisCall,
    OnboardingProfileError,
    OnboardingProfileUsage,
    OpenAIOnboardingProfileAnalyzer,
    onboarding_profile_cache_version,
)
from freelancer_bot.profile_onboarding_service import ProfileOnboardingService
from freelancer_bot.search_profiles import SearchProfileTermOrigin
from freelancer_bot.telegram_onboarding import (
    TelegramProfileOnboarding,
    parse_manual_profile_payload,
    parse_term_list,
)
from postgres_support import TEST_DATABASE_URL, migrate_to_head, temporary_database


DESCRIPTION = (
    "Я продуктовый дизайнер, работаю в Figma. "
    "Возможно, хочу SaaS."
)


class FixtureAnalyzer:
    provider = "fixture"
    model = "fixture-profile-model"
    analyzer_version = ONBOARDING_PROFILE_ANALYZER_VERSION
    prompt_version = ONBOARDING_PROFILE_PROMPT_VERSION
    schema_version = ONBOARDING_PROFILE_ANALYSIS_SCHEMA_VERSION

    def __init__(self) -> None:
        self.calls: list[str] = []

    async def analyze(self, description: str) -> OnboardingProfileAnalysisCall:
        self.calls.append(description)
        analysis = OnboardingProfileAnalysis.model_validate_json(
            json.dumps(
                {
                    "schema_version": ONBOARDING_PROFILE_ANALYSIS_SCHEMA_VERSION,
                    "roles": [
                        {
                            "value": "продуктовый дизайнер",
                            "evidence": "продуктовый дизайнер",
                            "origin": "explicit",
                        }
                    ],
                    "skills": [
                        {
                            "value": "Figma",
                            "evidence": "Figma",
                            "origin": "explicit",
                        }
                    ],
                    "categories": [],
                    "uncertain_terms": ["SaaS"],
                    "missing_fields": ["categories"],
                },
                ensure_ascii=False,
            ),
            strict=True,
        )
        return OnboardingProfileAnalysisCall(
            analysis=analysis,
            provider=self.provider,
            requested_model=self.model,
            response_model=self.model,
            analyzer_version=self.analyzer_version,
            prompt_version=self.prompt_version,
            schema_version=self.schema_version,
            attempt_count=1,
            usage=OnboardingProfileUsage(
                input_tokens=20,
                output_tokens=10,
                total_tokens=30,
            ),
        )


class UnavailableAIOnboarding:
    async def create_from_description(self, **kwargs):
        raise OnboardingProfileError("provider unavailable")


class TelegramProfileInputTest(unittest.TestCase):
    def test_manual_input_is_deterministic_and_supports_explicit_empty_fields(self):
        self.assertEqual(
            parse_manual_profile_payload(
                "Telegram-разработчик | Python, Telethon | -"
            ),
            (
                ("Telegram-разработчик",),
                ("Python", "Telethon"),
                (),
            ),
        )
        self.assertEqual(parse_term_list("-"), ())
        with self.assertRaises(ValueError):
            parse_manual_profile_payload("проекты на Python")
        with self.assertRaises(ValueError):
            parse_manual_profile_payload("- | - | -")


@unittest.skipUnless(TEST_DATABASE_URL, "TEST_DATABASE_URL is not configured")
class ProfileConfirmationIntegrationTest(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.database_context = temporary_database()
        self.database_url = self.database_context.__enter__()
        migrate_to_head(self.database_url)
        self.database = Database(self.database_url, pool_size=4, max_overflow=8)
        self.confirmation = ProfileConfirmationService(self.database)

    async def asyncTearDown(self):
        await self.database.close()
        self.database_context.__exit__(None, None, None)

    async def test_ai_draft_can_be_inspected_corrected_and_confirmed_before_activation(self):
        analyzer = FixtureAnalyzer()
        ai = ProfileOnboardingService(self.database, analyzer)
        outcome = await ai.create_from_description(
            platform="telegram",
            external_user_id="profile-owner",
            description=DESCRIPTION,
        )

        initial = await self.confirmation.show(
            platform="telegram",
            external_user_id="profile-owner",
            profile_id=outcome.profile.id,
        )
        self.assertEqual(initial.profile.confirmation_status, SearchProfileConfirmationStatus.DRAFT)
        self.assertEqual(initial.profile.revision, 1)
        self.assertEqual(initial.missing_fields, ("categories",))
        self.assertEqual(initial.uncertain_terms, ("SaaS",))
        self.assertIn("Не указано: категории", format_profile_summary(initial))
        self.assertIn("Нужно проверить: SaaS", format_profile_summary(initial))

        corrected = await self.confirmation.edit_terms(
            platform="telegram",
            external_user_id="profile-owner",
            profile_id=outcome.profile.id,
            field="categories",
            values=("SaaS", "Mobile Apps"),
            expected_revision=1,
        )
        self.assertEqual(corrected.profile.revision, 2)
        self.assertEqual(corrected.missing_fields, ())
        self.assertEqual(corrected.uncertain_terms, ())
        self.assertEqual(
            [term.origin for term in corrected.profile.categories],
            [SearchProfileTermOrigin.EXPLICIT, SearchProfileTermOrigin.EXPLICIT],
        )
        self.assertEqual(corrected.profile.semantic_text_original, DESCRIPTION)
        self.assertEqual(
            corrected.profile.analysis_cache_id,
            outcome.profile.analysis_cache_id,
        )

        confirmed = await self.confirmation.confirm(
            platform="telegram",
            external_user_id="profile-owner",
            profile_id=outcome.profile.id,
            expected_revision=2,
        )
        repeated = await self.confirmation.confirm(
            platform="telegram",
            external_user_id="profile-owner",
            profile_id=outcome.profile.id,
            expected_revision=2,
        )
        self.assertEqual(confirmed.profile, repeated.profile)
        self.assertEqual(
            confirmed.profile.confirmation_status,
            SearchProfileConfirmationStatus.CONFIRMED,
        )
        self.assertIsNotNone(confirmed.profile.confirmed_at)
        self.assertIn(
            "поиск ещё не активирован",
            format_profile_summary(confirmed),
        )
        with self.assertRaises(SearchProfileEditConflict):
            await self.confirmation.edit_terms(
                platform="telegram",
                external_user_id="profile-owner",
                profile_id=outcome.profile.id,
                field="roles",
                values=("UX Designer",),
                expected_revision=confirmed.profile.revision,
            )

    async def test_concurrent_confirmation_is_idempotent(self):
        draft = await self.confirmation.create_manual_draft(
            platform="telegram",
            external_user_id="confirm-owner",
            semantic_text="Developer | Python | Telegram",
            roles=("Developer",),
            skills=("Python",),
            categories=("Telegram",),
        )

        async def confirm():
            return await self.confirmation.confirm(
                platform="telegram",
                external_user_id="confirm-owner",
                profile_id=draft.profile.id,
                expected_revision=1,
            )

        first, second = await asyncio.gather(confirm(), confirm())
        self.assertEqual(first.profile, second.profile)
        self.assertEqual(first.profile.revision, 2)

    async def test_stale_concurrent_edit_is_rejected_without_losing_ai_lineage(self):
        analyzer = FixtureAnalyzer()
        ai = ProfileOnboardingService(self.database, analyzer)
        outcome = await ai.create_from_description(
            platform="telegram",
            external_user_id="concurrent-owner",
            description=DESCRIPTION,
        )

        async def edit(value: str):
            return await self.confirmation.edit_terms(
                platform="telegram",
                external_user_id="concurrent-owner",
                profile_id=outcome.profile.id,
                field="roles",
                values=(value,),
                expected_revision=1,
            )

        results = await asyncio.gather(
            edit("UX Designer"),
            edit("Product Designer"),
            return_exceptions=True,
        )
        self.assertEqual(sum(not isinstance(result, Exception) for result in results), 1)
        self.assertEqual(
            sum(
                isinstance(result, SearchProfileEditConflict)
                for result in results
            ),
            1,
        )
        stored = await self.confirmation.show(
            platform="telegram",
            external_user_id="concurrent-owner",
            profile_id=outcome.profile.id,
        )
        self.assertEqual(stored.profile.analysis_cache_id, outcome.analysis_cache.id)
        self.assertEqual(stored.profile.revision, 2)

    async def test_profile_ownership_is_enforced(self):
        owner = await self.confirmation.create_manual_draft(
            platform="telegram",
            external_user_id="owner",
            semantic_text="Developer | Python | Telegram",
            roles=("Developer",),
            skills=("Python",),
            categories=("Telegram",),
        )
        await self.confirmation.create_manual_draft(
            platform="telegram",
            external_user_id="other",
            semantic_text="Designer | Figma | SaaS",
            roles=("Designer",),
            skills=("Figma",),
            categories=("SaaS",),
        )
        with self.assertRaises(SearchProfileOwnershipError):
            await self.confirmation.show(
                platform="telegram",
                external_user_id="other",
                profile_id=owner.profile.id,
            )

    async def test_ai_outage_is_retryable_and_manual_profile_remains_explicit(self):
        telegram = TelegramProfileOnboarding(
            self.confirmation,
            UnavailableAIOnboarding(),
        )
        degraded = await telegram.begin(
            external_user_id="fallback-user",
            description="Нужны проекты на Python",
        )
        self.assertTrue(degraded.retryable)
        self.assertIn("Попробуйте отправить описание ещё раз", degraded.text)
        self.assertIn("/profile_manual", degraded.text)

        draft = await telegram.create_manual(
            external_user_id="fallback-user",
            payload="Python-разработчик | Python, Telethon | Telegram-боты",
        )
        self.assertIn("Профиль поиска", draft.text)
        self.assertEqual(len(draft.buttons), 5)
        self.assertTrue(
            all(
                len(button.data) <= 64
                for row in draft.buttons
                for button in row
            )
        )
        profile_id = _uuid_from_callback(draft.buttons[0][0].data)
        shown = await self.confirmation.show(
            platform="telegram",
            external_user_id="fallback-user",
            profile_id=profile_id,
        )
        confirmed = await telegram.confirm(
            external_user_id="fallback-user",
            profile_id=profile_id,
            expected_revision=shown.profile.revision,
        )
        self.assertIn("подтверждено", confirmed.text)
        async with self.database.connect() as connection:
            cache_count = await connection.scalar(
                sa.select(sa.func.count()).select_from(search_profile_analysis_cache)
            )
            profile_count = await connection.scalar(
                sa.select(sa.func.count()).select_from(search_profiles)
            )
        self.assertEqual(cache_count, 0)
        self.assertEqual(profile_count, 1)

    async def test_compatible_cache_remains_usable_without_provider_key(self):
        analyzer = OpenAIOnboardingProfileAnalyzer(api_key="")
        analysis = await FixtureAnalyzer().analyze(DESCRIPTION)
        compatible_call = OnboardingProfileAnalysisCall(
            analysis=analysis.analysis,
            provider=analyzer.provider,
            requested_model=analyzer.model,
            response_model=analyzer.model,
            analyzer_version=analyzer.analyzer_version,
            prompt_version=analyzer.prompt_version,
            schema_version=analyzer.schema_version,
            attempt_count=1,
            usage=analysis.usage,
        )
        async with self.database.transaction() as connection:
            await SearchProfileAnalysisCacheRepository().record(
                connection,
                input_sha256=sha256(DESCRIPTION.encode("utf-8")).hexdigest(),
                original_input_text=DESCRIPTION,
                normalized_input_text=DESCRIPTION,
                cache_version=onboarding_profile_cache_version(analyzer),
                call=compatible_call,
            )

        service = ProfileOnboardingService(self.database, analyzer)
        outcome = await service.create_from_description(
            platform="telegram",
            external_user_id="cached-without-key",
            description=DESCRIPTION,
        )
        self.assertFalse(outcome.model_invoked)
        self.assertEqual(outcome.profile.semantic_text_original, DESCRIPTION)

        missing = TelegramProfileOnboarding(self.confirmation, service)
        response = await missing.begin(
            external_user_id="no-cache-without-key",
            description="Я backend-разработчик на Python",
        )
        self.assertTrue(response.retryable)
        self.assertIn("Попробуйте отправить описание ещё раз", response.text)


def _uuid_from_callback(data: bytes) -> UUID:
    return UUID(data.decode("ascii").split(":")[2])


if __name__ == "__main__":
    unittest.main()
