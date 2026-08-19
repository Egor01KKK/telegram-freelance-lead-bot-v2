from __future__ import annotations

import asyncio
from dataclasses import replace
import unittest
from uuid import uuid4

from sqlalchemy.exc import IntegrityError

from freelancer_bot.persistence.database import Database
from freelancer_bot.persistence.search_profiles import (
    SearchProfileConflict,
    SearchProfileRepository,
    UserNotFound,
    UserRepository,
)
from freelancer_bot.search_profiles import (
    PROFILE_TERM_LIMIT,
    SEARCH_PROFILE_PARSER_VERSION,
    SEARCH_PROFILE_SCHEMA_VERSION,
    parse_search_profile,
)
from postgres_support import TEST_DATABASE_URL, migrate_to_head, temporary_database


class SearchProfileParserTest(unittest.TestCase):
    def test_produces_explicit_structured_fields_and_both_semantic_text_forms(self):
        original = "  Я Product   Designer.\nРаботаю в Ｆｉｇｍａ.  "

        parsed = parse_search_profile(
            roles=(" Product   Designer ", "product designer"),
            skills=("Ｆｉｇｍａ", "Telegram Mini Apps"),
            categories=(" SaaS ", "Mobile Apps"),
            semantic_text=original,
        )

        self.assertEqual(parsed.schema_version, SEARCH_PROFILE_SCHEMA_VERSION)
        self.assertEqual(parsed.parser_version, SEARCH_PROFILE_PARSER_VERSION)
        self.assertEqual(parsed.semantic_text_original, original)
        self.assertEqual(
            parsed.semantic_text_normalized,
            "Я Product Designer. Работаю в Figma.",
        )
        self.assertEqual(
            [(term.value, term.normalized_value) for term in parsed.roles],
            [("Product Designer", "product designer")],
        )
        self.assertEqual(
            [term.value for term in parsed.skills],
            ["Figma", "Telegram Mini Apps"],
        )
        self.assertEqual(
            [term.normalized_value for term in parsed.categories],
            ["saas", "mobile apps"],
        )
        self.assertNotIn(
            "backend",
            {term.normalized_value for term in parsed.skills},
        )

    def test_rejects_invalid_or_unbounded_explicit_input(self):
        with self.assertRaises(ValueError):
            parse_search_profile(
                roles=(),
                skills=(),
                categories=(),
                semantic_text="   ",
            )
        with self.assertRaises(ValueError):
            parse_search_profile(
                roles=("",),
                skills=(),
                categories=(),
                semantic_text="Designer",
            )
        with self.assertRaises(TypeError):
            parse_search_profile(
                roles="designer",
                skills=(),
                categories=(),
                semantic_text="Designer",
            )
        with self.assertRaises(ValueError):
            parse_search_profile(
                roles=tuple(f"role-{index}" for index in range(PROFILE_TERM_LIMIT + 1)),
                skills=(),
                categories=(),
                semantic_text="Designer",
            )
        parsed = _parsed_profile()
        with self.assertRaises(ValueError):
            replace(parsed, semantic_text_normalized="invented preference")


@unittest.skipUnless(TEST_DATABASE_URL, "TEST_DATABASE_URL is not configured")
class SearchProfileRepositoryTest(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.database_context = temporary_database()
        self.database_url = self.database_context.__enter__()
        migrate_to_head(self.database_url)
        self.database = Database(self.database_url, pool_size=4, max_overflow=8)
        self.users = UserRepository()
        self.profiles = SearchProfileRepository()

    async def asyncTearDown(self):
        await self.database.close()
        self.database_context.__exit__(None, None, None)

    async def test_user_identity_and_profile_creation_are_idempotent(self):
        async with self.database.transaction() as connection:
            first_user = await self.users.ensure(
                connection,
                platform=" Telegram ",
                external_user_id=" 123456 ",
            )
            repeated_user = await self.users.ensure(
                connection,
                platform="telegram",
                external_user_id="123456",
            )
        self.assertTrue(first_user.created)
        self.assertFalse(repeated_user.created)
        self.assertEqual(first_user.user.id, repeated_user.user.id)

        profile_id = uuid4()
        parsed = _parsed_profile()
        async with self.database.transaction() as connection:
            first = await self.profiles.create(
                connection,
                user_id=first_user.user.id,
                parsed_profile=parsed,
                profile_id=profile_id,
            )
            repeated = await self.profiles.create(
                connection,
                user_id=first_user.user.id,
                parsed_profile=parsed,
                profile_id=profile_id,
            )

        self.assertTrue(first.created)
        self.assertFalse(repeated.created)
        self.assertEqual(first.profile, repeated.profile)
        self.assertEqual(first.profile.schema_version, SEARCH_PROFILE_SCHEMA_VERSION)
        self.assertEqual(first.profile.parser_version, SEARCH_PROFILE_PARSER_VERSION)
        self.assertEqual(first.profile.roles, parsed.roles)
        self.assertEqual(first.profile.skills, parsed.skills)
        self.assertEqual(first.profile.categories, parsed.categories)
        self.assertEqual(
            first.profile.semantic_text_original,
            parsed.semantic_text_original,
        )
        self.assertEqual(
            first.profile.semantic_text_normalized,
            parsed.semantic_text_normalized,
        )

    async def test_concurrent_profile_creation_converges_without_overwrite(self):
        async with self.database.transaction() as connection:
            user = await self.users.ensure(
                connection,
                platform="telegram",
                external_user_id="concurrent-user",
            )
        profile_id = uuid4()
        parsed = _parsed_profile()

        async def create_profile():
            async with self.database.transaction() as connection:
                return await self.profiles.create(
                    connection,
                    user_id=user.user.id,
                    parsed_profile=parsed,
                    profile_id=profile_id,
                )

        outcomes = await asyncio.gather(create_profile(), create_profile())

        self.assertEqual(sum(outcome.created for outcome in outcomes), 1)
        self.assertEqual(outcomes[0].profile, outcomes[1].profile)

    async def test_conflicting_identifier_and_unknown_user_are_rejected(self):
        async with self.database.transaction() as connection:
            user = await self.users.ensure(
                connection,
                platform="telegram",
                external_user_id="profile-owner",
            )
            created = await self.profiles.create(
                connection,
                user_id=user.user.id,
                parsed_profile=_parsed_profile(),
            )

        with self.assertRaises(SearchProfileConflict):
            async with self.database.transaction() as connection:
                await self.profiles.create(
                    connection,
                    user_id=user.user.id,
                    parsed_profile=parse_search_profile(
                        roles=("Backend Developer",),
                        skills=("Python",),
                        categories=("Backend",),
                        semantic_text="Backend developer",
                    ),
                    profile_id=created.profile.id,
                )
        with self.assertRaises(UserNotFound):
            async with self.database.transaction() as connection:
                await self.profiles.create(
                    connection,
                    user_id=uuid4(),
                    parsed_profile=_parsed_profile(),
                )

    async def test_database_rejects_non_array_structured_profile_data(self):
        async with self.database.transaction() as connection:
            user = await self.users.ensure(
                connection,
                platform="telegram",
                external_user_id="constraint-owner",
            )

        with self.assertRaises(IntegrityError):
            async with self.database.transaction() as connection:
                await connection.exec_driver_sql(
                    "INSERT INTO search_profiles "
                    "(id, user_id, schema_version, parser_version, roles, skills, "
                    "categories, semantic_text_original, semantic_text_normalized) "
                    "VALUES (%s, %s, %s, %s, %s::jsonb, %s::jsonb, %s::jsonb, %s, %s)",
                    (
                        uuid4(),
                        user.user.id,
                        SEARCH_PROFILE_SCHEMA_VERSION,
                        SEARCH_PROFILE_PARSER_VERSION,
                        "{}",
                        "[]",
                        "[]",
                        "Designer",
                        "Designer",
                    ),
                )


def _parsed_profile():
    return parse_search_profile(
        roles=("Product Designer",),
        skills=("Figma", "Telegram Mini Apps"),
        categories=("SaaS", "Mobile Apps"),
        semantic_text="Я продуктовый дизайнер. Ищу SaaS и mobile проекты.",
    )


if __name__ == "__main__":
    unittest.main()
