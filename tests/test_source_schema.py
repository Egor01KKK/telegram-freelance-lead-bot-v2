import unittest

import sqlalchemy as sa
from sqlalchemy.exc import IntegrityError

from freelancer_bot.persistence.schema import (
    source_taxonomy_assignments,
    source_taxonomy_terms,
    sources,
)
from postgres_support import TEST_DATABASE_URL, migrate_to_head, temporary_database


@unittest.skipUnless(TEST_DATABASE_URL, "TEST_DATABASE_URL is not configured")
class SourceDomainSchemaTest(unittest.TestCase):
    def setUp(self):
        self.database_context = temporary_database()
        self.database_url = self.database_context.__enter__()
        migrate_to_head(self.database_url)
        self.engine = sa.create_engine(self.database_url)

    def tearDown(self):
        self.engine.dispose()
        self.database_context.__exit__(None, None, None)

    def test_approved_source_can_be_created_updated_and_queried(self):
        source_id = self._insert_source(
            external_id="-1001234567890",
            access_type="public",
            lifecycle_status="approved",
            display_name="Telegram Jobs",
            handle="@telegram_jobs",
        )

        with self.engine.begin() as connection:
            connection.execute(
                sa.update(sources)
                .where(sources.c.id == source_id)
                .values(
                    display_name="Telegram Jobs RU",
                    canonical_url="https://t.me/telegram_jobs",
                    updated_at=sa.func.now(),
                )
            )

        with self.engine.connect() as connection:
            record = connection.execute(
                sa.select(sources).where(
                    sources.c.lifecycle_status == "approved",
                    sources.c.platform == "telegram",
                )
            ).mappings().one()

        self.assertEqual(record["id"], source_id)
        self.assertEqual(record["display_name"], "Telegram Jobs RU")
        self.assertEqual(record["external_id"], "-1001234567890")
        self.assertEqual(record["canonical_url"], "https://t.me/telegram_jobs")

    def test_access_type_and_platform_external_identity_are_enforced(self):
        public_id = self._insert_source(
            external_id="-100111",
            access_type="public",
            lifecycle_status="approved",
            display_name="Public channel",
            handle="@public_channel",
        )
        private_id = self._insert_source(
            external_id="-100222",
            access_type="private",
            lifecycle_status="candidate",
            display_name="Private community",
        )

        with self.engine.connect() as connection:
            access_by_id = dict(
                connection.execute(
                    sa.select(sources.c.id, sources.c.access_type).where(
                        sources.c.id.in_((public_id, private_id))
                    )
                ).all()
            )
        self.assertEqual(access_by_id, {public_id: "public", private_id: "private"})

        with self.assertRaises(IntegrityError):
            self._insert_source(
                external_id="-100111",
                access_type="private",
                lifecycle_status="candidate",
                display_name="Duplicate Telegram entity",
            )

        other_platform_id = self._insert_source(
            platform="web",
            external_id="-100111",
            access_type="public",
            lifecycle_status="candidate",
            display_name="Same external ID on another platform",
        )
        self.assertIsInstance(other_platform_id, int)

        with self.assertRaises(IntegrityError):
            self._insert_source(
                external_id="-100333",
                access_type="invite_only",
                lifecycle_status="candidate",
                display_name="Invalid access type",
            )

        paused_id = self._insert_source(
            external_id="-100555",
            access_type="public",
            lifecycle_status="paused",
            display_name="Paused source",
        )
        self.assertIsInstance(paused_id, int)

    def test_taxonomy_is_extensible_and_supports_multiple_assignments(self):
        source_id = self._insert_source(
            external_id="-100444",
            access_type="public",
            lifecycle_status="approved",
            display_name="Multilingual founders",
        )
        terms = (
            ("source_type", "telegram_group", "Telegram group"),
            ("language", "ru", "Russian"),
            ("language", "en", "English"),
            ("category", "telegram_development", "Telegram development"),
            ("category", "quantum_widgets", "Previously unknown category"),
            ("vertical", "ecommerce", "E-commerce"),
        )

        with self.engine.begin() as connection:
            term_ids = [
                connection.execute(
                    sa.insert(source_taxonomy_terms)
                    .values(dimension=dimension, key=key, display_name=display_name)
                    .returning(source_taxonomy_terms.c.id)
                ).scalar_one()
                for dimension, key, display_name in terms
            ]
            connection.execute(
                sa.insert(source_taxonomy_assignments),
                [{"source_id": source_id, "term_id": term_id} for term_id in term_ids],
            )

        with self.engine.connect() as connection:
            assigned = connection.execute(
                sa.select(source_taxonomy_terms.c.dimension, source_taxonomy_terms.c.key)
                .join(
                    source_taxonomy_assignments,
                    source_taxonomy_assignments.c.term_id == source_taxonomy_terms.c.id,
                )
                .where(source_taxonomy_assignments.c.source_id == source_id)
                .order_by(source_taxonomy_terms.c.dimension, source_taxonomy_terms.c.key)
            ).all()

        self.assertEqual(
            assigned,
            [
                ("category", "quantum_widgets"),
                ("category", "telegram_development"),
                ("language", "en"),
                ("language", "ru"),
                ("source_type", "telegram_group"),
                ("vertical", "ecommerce"),
            ],
        )

    def _insert_source(
        self,
        *,
        external_id,
        access_type,
        lifecycle_status,
        display_name,
        platform="telegram",
        handle=None,
    ):
        with self.engine.begin() as connection:
            return connection.execute(
                sa.insert(sources)
                .values(
                    platform=platform,
                    external_id=external_id,
                    access_type=access_type,
                    lifecycle_status=lifecycle_status,
                    display_name=display_name,
                    handle=handle,
                )
                .returning(sources.c.id)
            ).scalar_one()


if __name__ == "__main__":
    unittest.main()
