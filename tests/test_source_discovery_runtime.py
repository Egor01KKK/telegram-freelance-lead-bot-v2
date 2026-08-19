from __future__ import annotations

from datetime import datetime, timezone
import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

from telethon.tl.types import InputPeerChannel

from freelancer_bot.persistence.collector_accounts import CollectorAccountRepository
from freelancer_bot.persistence.database import Database
from freelancer_bot.persistence.source_repository import SourceRepository, SourceStatus
from freelancer_bot.source_discovery_runtime import _source_audit_lookup
from postgres_support import TEST_DATABASE_URL, migrate_to_head, temporary_database


class SourceDiscoveryAuditLookupTest(unittest.TestCase):
    def test_private_chat_discovery_source_uses_persisted_peer_access_hash(self):
        source = SimpleNamespace(
            platform="telegram",
            access_type="private",
            handle=None,
            canonical_url=None,
            external_id="peer:private",
        )
        peer = SimpleNamespace(
            peer_type="supergroup",
            telegram_peer_id=123,
            telegram_access_hash=456,
            username=None,
            canonical_url=None,
        )

        lookup = _source_audit_lookup(source, peer)

        self.assertIsInstance(lookup, InputPeerChannel)
        self.assertEqual(lookup.channel_id, 123)
        self.assertEqual(lookup.access_hash, 456)

    def test_public_source_keeps_provider_visible_lookup(self):
        source = SimpleNamespace(
            platform="telegram",
            access_type="public",
            handle="@public_source",
            canonical_url="https://t.me/public_source",
            external_id="username:public_source",
        )

        self.assertEqual(_source_audit_lookup(source, None), "@public_source")


@unittest.skipUnless(TEST_DATABASE_URL, "TEST_DATABASE_URL is not configured")
class SourceDiscoveryCandidateSelectionPostgresTest(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.database_context = temporary_database()
        self.database_url = self.database_context.__enter__()
        migrate_to_head(self.database_url)
        self.database = Database(self.database_url, pool_size=4, max_overflow=8)

    async def asyncTearDown(self) -> None:
        await self.database.close()
        self.database_context.__exit__(None, None, None)

    async def test_chat_watch_candidate_is_pending_when_new_only_is_enabled(self):
        async with self.database.transaction() as connection:
            await CollectorAccountRepository().ensure(
                connection,
                platform="telegram",
                external_account_id="candidate-selection-fixture",
                display_name="Candidate selection fixture",
            )
            candidate = await SourceRepository().create_candidate(
                connection,
                platform="telegram",
                external_id="username:watch_candidate",
                access_type="public",
                display_name="Watch candidate",
                provider="telegram_chat_search",
                lineage_key="watch-candidate-fixture",
                handle="@watch_candidate",
                canonical_url="https://t.me/watch_candidate",
            )
            needs_review = await SourceRepository().create_candidate(
                connection,
                platform="telegram",
                external_id="username:needs_review",
                access_type="public",
                display_name="Needs review",
                provider="fixture",
                lineage_key="needs-review-fixture",
                handle="@needs_review",
                canonical_url="https://t.me/needs_review",
            )
            await SourceRepository().transition(
                connection,
                needs_review.id,
                SourceStatus.NEEDS_REVIEW,
                reason="fixture review",
            )

        from freelancer_bot.source_discovery_runtime import AutonomousSourceDiscoveryRuntime

        runtime = AutonomousSourceDiscoveryRuntime.__new__(AutonomousSourceDiscoveryRuntime)
        runtime._database = self.database
        runtime._config = SimpleNamespace(source_discovery_max_candidates=100)

        candidates_only = await runtime._pending_candidate_ids(
            statuses=(SourceStatus.CANDIDATE,)
        )
        all_pending = await runtime._pending_candidate_ids()
        discovered_ids = await runtime._filter_source_ids_by_status(
            (candidate.id, needs_review.id),
            statuses=(SourceStatus.CANDIDATE,),
        )

        self.assertIn(candidate.id, candidates_only)
        self.assertNotIn(needs_review.id, candidates_only)
        self.assertIn(candidate.id, all_pending)
        self.assertIn(needs_review.id, all_pending)
        self.assertEqual(discovered_ids, (candidate.id,))

    async def test_run_once_audits_pending_chat_candidates_with_normal_bound(self):
        async with self.database.transaction() as connection:
            account = await CollectorAccountRepository().ensure(
                connection,
                platform="telegram",
                external_account_id="run-once-candidate-fixture",
                display_name="Run once candidate fixture",
            )
            candidates = [
                await SourceRepository().create_candidate(
                    connection,
                    platform="telegram",
                    external_id=f"username:run_once_{index}",
                    access_type="public",
                    display_name=f"Run once {index}",
                    provider="telegram_chat_search",
                    lineage_key=f"run-once-{index}",
                    handle=f"@run_once_{index}",
                    canonical_url=f"https://t.me/run_once_{index}",
                )
                for index in range(3)
            ]

        from freelancer_bot.source_discovery_runtime import AutonomousSourceDiscoveryRuntime

        runtime = AutonomousSourceDiscoveryRuntime.__new__(AutonomousSourceDiscoveryRuntime)
        runtime._database = self.database
        runtime._client = SimpleNamespace()
        runtime._config = SimpleNamespace(
            source_discovery_enabled=True,
            source_discovery_seed_limit=0,
            telegram_graph_seeds_per_pass=0,
            source_discovery_interval_seconds=60,
            source_discovery_audit_new_candidates_only=True,
            source_discovery_audit_limit=1,
            telegram_max_audits_per_batch=1,
            source_discovery_max_candidates=100,
            source_audit_enabled=True,
            telegram_chat_discovery_enabled=True,
        )
        runtime._source_adapter = MagicMock()
        runtime._source_adapter.list_for_session = AsyncMock(
            return_value=SimpleNamespace(
                collector_account=account,
                sources=(),
            )
        )
        runtime._governor = SimpleNamespace(collector_account_id=account.id)
        runtime._clock = lambda: datetime(2026, 8, 20, tzinfo=timezone.utc)
        runtime._logger = MagicMock()
        runtime._run_profile_telegram = AsyncMock(return_value=())
        runtime._run_graph = AsyncMock(return_value=None)
        runtime._run_web = AsyncMock(return_value=None)
        runtime._run_profile_web = AsyncMock(return_value=())
        runtime._build_audit_pipeline = MagicMock(return_value=object())
        runtime._audit_candidate = AsyncMock(return_value=None)

        await runtime.run_once()

        runtime._audit_candidate.assert_awaited_once()
        self.assertIn(
            runtime._audit_candidate.await_args.args[0],
            {candidate.id for candidate in candidates},
        )
