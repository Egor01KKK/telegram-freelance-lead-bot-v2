from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock

import sqlalchemy as sa
from telethon.tl.types import Channel

from freelancer_bot.config import RuntimeConfig
from freelancer_bot.app import LeadBot
from freelancer_bot.persistence.collector_accounts import CollectorAccountRepository
from freelancer_bot.persistence.database import Database
from freelancer_bot.persistence.source_repository import SourceRepository, SourceStatus
from freelancer_bot.persistence.schema import (
    sources,
    telegram_chat_discovery_screen_attempts,
)
from freelancer_bot.persistence.telegram_chat_discovery import (
    SCREEN_JOB_TYPE,
    TelegramChatDiscoveryRepository,
    normalize_topic,
)
from freelancer_bot.persistence.telegram_operation_state import (
    TelegramCollectorOperationRepository,
)
from freelancer_bot.source_discovery_runtime import SourceDiscoveryCycle
from freelancer_bot.telegram_chat_discovery import (
    ScreenClassification,
    ScreenResponse,
    TelegramChatDiscoveryError,
    TelegramChatDiscoveryService,
    TelegramChatScreenPolicy,
    input_entity_for_peer,
    telegram_chat_screen_response_schema,
)
from freelancer_bot.telegram_request_governor import TelegramRequestGovernor
from freelancer_bot.telegram_collector import TelegramCollectorSource
from postgres_support import TEST_DATABASE_URL, migrate_to_head, temporary_database


NOW = datetime(2026, 8, 17, 12, 0, tzinfo=timezone.utc)


class _ScreenProvider:
    name = "fake"
    model = "fake-screen-v1"

    async def classify(self, _peer, messages):
        return ScreenClassification(
            decision="WATCH",
            confidence=0.95,
            labels=tuple("BUYER_TO_SPECIALIST" for _ in messages),
            reason_codes=("fixture",),
        )


class TelegramChatDiscoverySchemaTest(unittest.TestCase):
    def test_screen_schema_is_strict_and_covers_all_message_indices(self):
        schema = telegram_chat_screen_response_schema()
        self.assertFalse(schema["additionalProperties"])
        self.assertEqual(schema["properties"]["decision"]["enum"], ["WATCH", "SKIP", "UNCLEAR"])
        valid = ScreenResponse.model_validate(
            {
                "decision": "WATCH",
                "confidence": 0.9,
                "labels": [
                    {"message_index": 1, "category": "BUYER_TO_SPECIALIST", "confidence": 0.9},
                    {"message_index": 2, "category": "OTHER", "confidence": 0.8},
                ],
                "reason_codes": ["useful_demand_thresholds_met"],
            }
        )
        self.assertEqual([item.message_index for item in valid.labels], [1, 2])
        with self.assertRaises(ValueError):
            ScreenResponse.model_validate(
                {
                    "decision": "WATCH",
                    "confidence": 0.9,
                    "labels": [
                        {"message_index": 1, "category": "OTHER", "confidence": 0.8},
                    ],
                    "reason_codes": [],
                    "unexpected": True,
                }
            )

    def test_private_peer_input_preserves_access_hash_for_reuse(self):
        peer = SimpleNamespace(
            peer_type="supergroup",
            telegram_peer_id=123,
            telegram_access_hash=456,
            username=None,
            canonical_url=None,
        )

        input_peer = input_entity_for_peer(peer)

        self.assertEqual(type(input_peer).__name__, "InputPeerChannel")
        self.assertEqual(input_peer.channel_id, 123)
        self.assertEqual(input_peer.access_hash, 456)


@unittest.skipUnless(TEST_DATABASE_URL, "TEST_DATABASE_URL is not configured")
class TelegramChatDiscoveryPostgresTest(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.database_context = temporary_database()
        self.database_url = self.database_context.__enter__()
        migrate_to_head(self.database_url)
        self.database = Database(self.database_url, pool_size=4, max_overflow=8)
        self.repository = TelegramChatDiscoveryRepository()

    async def asyncTearDown(self) -> None:
        await self.database.close()
        self.database_context.__exit__(None, None, None)

    async def _account(self, external_account_id: str = "chat-discovery-fixture") -> int:
        async with self.database.transaction() as connection:
            account = await CollectorAccountRepository().ensure(
                connection,
                platform="telegram",
                external_account_id=external_account_id,
                display_name="Chat discovery fixture",
            )
            await TelegramCollectorOperationRepository().ensure(
                connection,
                collector_account_id=account.id,
            )
        return account.id

    def _config(self) -> RuntimeConfig:
        return RuntimeConfig(
            _env_file=None,
            telegram_crawl_min_delay_seconds=0,
            telegram_crawl_max_delay_seconds=0,
            telegram_source_cooldown_min_seconds=0,
            telegram_source_cooldown_max_seconds=0,
            telegram_governor_lease_seconds=900,
            telegram_chat_discovery_history_limit=25,
        )

    async def test_topic_normalization_is_conservative_and_idempotent(self):
        first = normalize_topic("  Startup   ", "EN")
        second = normalize_topic("startup", "en")
        russian = normalize_topic("startup", "ru")
        self.assertEqual(first, second)
        self.assertNotEqual(first, russian)
        self.assertNotEqual(normalize_topic("founders", "en"), first)

        async with self.database.transaction() as connection:
            one = await self.repository.ensure_topic(
                connection,
                topic_text="  Startup ",
                language="en",
                topic_kind="base",
            )
            two = await self.repository.ensure_topic(
                connection,
                topic_text="startup",
                language="en",
                topic_kind="profile",
            )
        self.assertEqual(one.id, two.id)

    async def test_response_chats_are_deduplicated_and_known_source_is_not_screened(self):
        account_id = await self._account()
        known = Channel(
            id=100,
            access_hash=1000,
            title="Known group",
            photo=None,
            date=None,
            username="known_group",
            megagroup=True,
        )
        broadcast = Channel(
            id=200,
            access_hash=2000,
            title="Broadcast channel",
            photo=None,
            date=None,
            username="broadcast_channel",
            megagroup=False,
            broadcast=True,
        )
        async with self.database.transaction() as connection:
            known_source = await SourceRepository().create_candidate(
                connection,
                platform="telegram",
                external_id="username:known_group",
                access_type="public",
                display_name="Known group",
                provider="fixture",
                lineage_key="fixture-known",
                handle="@known_group",
                canonical_url="https://t.me/known_group",
            )
            topic = await self.repository.ensure_topic(
                connection,
                topic_text="developers",
                language="en",
                topic_kind="base",
                refresh_interval_seconds=300,
            )

        class Client:
            def __init__(self):
                self.search_calls = []
                self.history_calls = []

            async def __call__(self, request):
                self.search_calls.append(request)
                return SimpleNamespace(
                    messages=(
                        SimpleNamespace(id=1, chat=known),
                        SimpleNamespace(id=2, chat=broadcast),
                        SimpleNamespace(id=3, chat=broadcast),
                    ),
                    chats=(known, broadcast, broadcast),
                )

            async def get_messages(self, entity, *, limit):
                self.history_calls.append((entity, limit))
                return tuple(SimpleNamespace(id=index, message="buyer demand") for index in range(1, 11))

        client = Client()
        wake_calls = []

        async def on_watch(source_id):
            wake_calls.append(source_id)

        service = TelegramChatDiscoveryService(
            self.database,
            client,
            config=self._config(),
            collector_account_id=account_id,
            governor=TelegramRequestGovernor(
                self.database,
                account_id,
                self._config(),
                clock=lambda: NOW,
                random_uniform=lambda lower, _upper: lower,
            ),
            screen_provider=_ScreenProvider(),
            watch_candidate_callback=on_watch,
        )
        first = await service.run_search(topic, search_budget=20, refresh_key="fixture")
        repeated = await service.run_search(topic, search_budget=20, refresh_key="fixture")

        self.assertEqual(len(client.search_calls), 1)
        self.assertEqual(first.unique_peers, 2)
        self.assertEqual(first.known_peers, 1)
        self.assertEqual(first.new_peers, 1)
        self.assertEqual(first.run.group_peer_count, 1)
        self.assertEqual(first.run.broadcast_peer_count, 1)
        self.assertEqual(first.run.chat_entity_occurrence_count, 3)
        self.assertEqual(repeated.run.id, first.run.id)
        self.assertEqual(first.screen_jobs_created, 1)
        self.assertEqual(known_source.lifecycle_status, SourceStatus.CANDIDATE)

        async with self.database.connect() as connection:
            pending = await self.repository.list_screen_pending(
                connection,
                now=NOW,
                limit=10,
            )
            jobs = await self.repository.job_counts(connection)
        self.assertEqual(len(pending), 1)
        self.assertEqual(jobs[f"{SCREEN_JOB_TYPE}:queued"], 1)
        screened = await service.screen_peer(pending[0].id)
        self.assertIsNotNone(screened)
        self.assertEqual(screened.status, "WATCH")
        self.assertEqual(screened.sample_count, 10)
        self.assertEqual(len(client.history_calls), 1)
        self.assertEqual(client.history_calls[0][1], 25)

        async with self.database.connect() as connection:
            peer = await self.repository.get_peer(connection, pending[0].id)
            source = await SourceRepository().get(connection, peer.source_id)
            lineage = await SourceRepository().list_lineage(connection, source.id)
        self.assertEqual(peer.dedup_bucket, "ALREADY_CANDIDATE")
        self.assertEqual(source.lifecycle_status, SourceStatus.CANDIDATE)
        self.assertTrue(any(item.provider == "telegram_chat_search" for item in lineage))
        self.assertEqual(wake_calls, [source.id])

        async with self.database.transaction() as connection:
            second_topic = await self.repository.ensure_topic(
                connection,
                topic_text="startup",
                language="en",
                topic_kind="base",
                refresh_interval_seconds=300,
            )
        second = await service.run_search(second_topic, search_budget=20, refresh_key="second-topic")
        self.assertEqual(second.known_peers, 2)
        self.assertEqual(second.new_peers, 0)
        self.assertEqual(second.screen_jobs_created, 0)
        self.assertEqual(len(client.history_calls), 1)

    async def test_screen_pending_limit_drains_only_requested_batch(self):
        account_id = await self._account("chat-discovery-bounded-screen")
        async with self.database.transaction() as connection:
            for index in range(20):
                await self.repository.upsert_peer(
                    connection,
                    canonical_peer_identity=f"peer:bounded:{index}",
                    peer_type="group",
                    telegram_peer_id=10_000 + index,
                    telegram_access_hash=20_000 + index,
                    display_name=f"Bounded peer {index}",
                    username=None,
                    canonical_url=None,
                    access_type="public",
                    source_id=None,
                    dedup_bucket="GENUINELY_NEW",
                    collector_account_id=account_id,
                )

        class Client:
            def __init__(self):
                self.history_calls = 0

            async def get_messages(self, _entity, *, limit):
                self.history_calls += 1
                if limit != 25:
                    raise AssertionError(f"expected history limit 25, got {limit}")
                return tuple(
                    SimpleNamespace(id=index, message="buyer demand")
                    for index in range(1, 26)
                )

        class CountingScreenProvider(_ScreenProvider):
            def __init__(self):
                self.calls = 0

            async def classify(self, peer, messages):
                self.calls += 1
                return await super().classify(peer, messages)

        client = Client()
        screen_provider = CountingScreenProvider()
        service = TelegramChatDiscoveryService(
            self.database,
            client,
            config=self._config(),
            collector_account_id=account_id,
            governor=TelegramRequestGovernor(
                self.database,
                account_id,
                self._config(),
                clock=lambda: NOW,
                random_uniform=lambda lower, _upper: lower,
            ),
            screen_provider=screen_provider,
        )

        job_ids = await service.enqueue_pending_screens(limit=5)
        self.assertEqual(len(job_ids), 5)
        await service.drain(
            worker_id="bounded-screen-test",
            job_type=SCREEN_JOB_TYPE,
            job_ids=job_ids,
            timeout_seconds=10,
        )

        async with self.database.connect() as connection:
            screen_statuses = await self.repository.screen_status_counts(connection)
            jobs = await self.repository.job_counts(connection)

        self.assertEqual(screen_provider.calls, 5)
        self.assertEqual(client.history_calls, 5)
        self.assertEqual(screen_statuses["WATCH"], 5)
        self.assertEqual(screen_statuses["SCREEN_PENDING"], 15)
        self.assertEqual(jobs.get(f"{SCREEN_JOB_TYPE}:completed", 0), 5)
        self.assertEqual(jobs.get(f"{SCREEN_JOB_TYPE}:queued", 0), 0)
        self.assertEqual(jobs.get(f"{SCREEN_JOB_TYPE}:running", 0), 0)

    async def test_new_private_peer_is_terminal_skip_without_history_ai_or_source(self):
        account_id = await self._account("chat-discovery-private-screen")

        class Client:
            def __init__(self):
                self.history_calls = 0

            async def get_messages(self, _entity, *, limit):
                self.history_calls += 1
                raise AssertionError("private peer must not read history")

        class FailingProvider(_ScreenProvider):
            def __init__(self):
                self.calls = 0

            async def classify(self, peer, messages):
                self.calls += 1
                raise AssertionError("private peer must not invoke screening AI")

        async with self.database.transaction() as connection:
            peer, _created = await self.repository.upsert_peer(
                connection,
                canonical_peer_identity="channel:501",
                peer_type="supergroup",
                telegram_peer_id=501,
                telegram_access_hash=5001,
                display_name="Private peer fixture",
                username=None,
                canonical_url=None,
                access_type="private",
                source_id=None,
                dedup_bucket="GENUINELY_NEW",
                collector_account_id=account_id,
            )
            job_id = await self.repository.enqueue_screen_job(
                connection,
                peer_id=peer.id,
                attempt_number=1,
            )

        client = Client()
        provider = FailingProvider()
        wake_calls = []

        async def on_watch(source_id):
            wake_calls.append(source_id)

        service = TelegramChatDiscoveryService(
            self.database,
            client,
            config=self._config(),
            collector_account_id=account_id,
            governor=TelegramRequestGovernor(
                self.database,
                account_id,
                self._config(),
                clock=lambda: NOW,
                random_uniform=lambda lower, _upper: lower,
            ),
            screen_provider=provider,
            watch_candidate_callback=on_watch,
        )
        await service.drain(
            worker_id="private-screen-test",
            job_type=SCREEN_JOB_TYPE,
            job_ids=(job_id,),
            timeout_seconds=10,
        )

        async with self.database.connect() as connection:
            persisted = await self.repository.get_peer(connection, peer.id)
            attempts = await connection.execute(
                sa.select(telegram_chat_discovery_screen_attempts).where(
                    telegram_chat_discovery_screen_attempts.c.peer_id == peer.id
                )
            )
            attempt = attempts.mappings().one()
            jobs = await self.repository.job_counts(connection)
            source = await SourceRepository().get_by_identity(
                connection,
                platform="telegram",
                external_id="channel:501",
            )

        self.assertEqual(persisted.screen_status, "SKIP")
        self.assertEqual(persisted.screen_attempt_count, 1)
        self.assertEqual(persisted.source_id, None)
        self.assertEqual(persisted.next_screen_at, None)
        self.assertEqual(attempt["reason_codes"], ["private_source_not_global"])
        self.assertEqual(attempt["history_request_count"], 0)
        self.assertEqual(attempt["ai_call_count"], 0)
        self.assertEqual(attempt["sample_count"], 0)
        self.assertIsNone(source)
        self.assertEqual(client.history_calls, 0)
        self.assertEqual(provider.calls, 0)
        self.assertEqual(wake_calls, [])
        self.assertEqual(jobs.get(f"{SCREEN_JOB_TYPE}:completed", 0), 1)

        self.assertEqual(await service.enqueue_pending_screens(limit=10), ())
        self.assertIsNone(await service.screen_peer(peer.id))

    async def test_private_peer_skips_before_unconfigured_provider_and_repeated_search_does_not_retry(self):
        account_id = await self._account("chat-discovery-private-no-ai")
        private = Channel(
            id=502,
            access_hash=5002,
            title="Private no-AI fixture",
            photo=None,
            date=None,
            username=None,
            megagroup=True,
        )

        class Client:
            def __init__(self):
                self.search_calls = 0

            async def __call__(self, _request):
                self.search_calls += 1
                return SimpleNamespace(
                    messages=(SimpleNamespace(id=1, chat=private),),
                    chats=(private,),
                )

            async def get_messages(self, _entity, *, limit):
                raise AssertionError("private peer must not read history")

        client = Client()
        config = self._config()
        async with self.database.transaction() as connection:
            topic = await self.repository.ensure_topic(
                connection,
                topic_text="private fixture",
                language="en",
                topic_kind="base",
                refresh_interval_seconds=300,
            )

        service = TelegramChatDiscoveryService(
            self.database,
            client,
            config=config,
            collector_account_id=account_id,
            governor=TelegramRequestGovernor(
                self.database,
                account_id,
                config,
                clock=lambda: NOW,
                random_uniform=lambda lower, _upper: lower,
            ),
            screen_provider=None,
        )
        first = await service.run_search(topic, search_budget=20, refresh_key="private-first")
        async with self.database.connect() as connection:
            pending = await self.repository.list_screen_pending(connection, now=NOW, limit=10)
        self.assertEqual(first.new_peers, 1)
        self.assertEqual(first.screen_jobs_created, 1)
        self.assertEqual(len(pending), 1)
        self.assertEqual((await service.screen_peer(pending[0].id)).status, "SKIP")

        repeated = await service.run_search(topic, search_budget=20, refresh_key="private-second")
        self.assertEqual(client.search_calls, 2)
        self.assertEqual(repeated.screen_jobs_created, 0)
        self.assertEqual(await service.enqueue_pending_screens(limit=10), ())
        async with self.database.connect() as connection:
            persisted = await self.repository.get_peer(connection, pending[0].id)
            source_count = await connection.scalar(
                sa.select(sa.func.count()).select_from(sources).where(
                    sources.c.platform == "telegram",
                    sources.c.external_id == "channel:502",
                )
            )
        self.assertEqual(persisted.screen_attempt_count, 1)
        self.assertEqual(persisted.screen_status, "SKIP")
        self.assertEqual(source_count, 0)

    async def test_chat_discovery_candidate_persistence_rejects_private_peer(self):
        account_id = await self._account("chat-discovery-private-candidate")
        async with self.database.transaction() as connection:
            peer, _created = await self.repository.upsert_peer(
                connection,
                canonical_peer_identity="channel:503",
                peer_type="supergroup",
                telegram_peer_id=503,
                telegram_access_hash=5003,
                display_name="Private candidate fixture",
                username=None,
                canonical_url=None,
                access_type="private",
                source_id=None,
                dedup_bucket="GENUINELY_NEW",
                collector_account_id=account_id,
            )
            service = TelegramChatDiscoveryService(
                self.database,
                SimpleNamespace(),
                config=self._config(),
                collector_account_id=account_id,
                governor=TelegramRequestGovernor(
                    self.database,
                    account_id,
                    self._config(),
                    clock=lambda: NOW,
                    random_uniform=lambda lower, _upper: lower,
                ),
                screen_provider=None,
            )
            with self.assertRaisesRegex(TelegramChatDiscoveryError, "private_source_not_global"):
                await service._persist_candidate(
                    connection,
                    peer=peer,
                    provider="telegram_chat_search",
                    policy_version="test-policy",
                )

            source_count = await connection.scalar(
                sa.select(sa.func.count()).select_from(sources).where(
                    sources.c.platform == "telegram",
                    sources.c.external_id == "channel:503",
                )
            )
        self.assertEqual(source_count, 0)

    async def test_watch_wake_audit_reload_and_catchup_reaches_dispatch_boundary(self):
        account_id = await self._account("watch-to-ingestion-fixture")

        async with self.database.transaction() as connection:
            peer, _created = await self.repository.upsert_peer(
                connection,
                canonical_peer_identity="peer:watch-to-ingestion",
                peer_type="group",
                telegram_peer_id=321,
                telegram_access_hash=654,
                display_name="Watch to ingestion",
                username="watch_to_ingestion",
                canonical_url="https://t.me/watch_to_ingestion",
                access_type="public",
                source_id=None,
                dedup_bucket="GENUINELY_NEW",
                collector_account_id=account_id,
            )
            await self.repository.enqueue_screen_job(
                connection,
                peer_id=peer.id,
                attempt_number=1,
            )

        class Client:
            async def get_messages(self, _entity, *, limit):
                return tuple(
                    SimpleNamespace(
                        id=index,
                        message="buyer needs a specialist",
                        date=NOW,
                    )
                    for index in range(1, min(limit, 10) + 1)
                )

            async def iter_messages(self, _entity, *, limit):
                for index in range(1, limit + 1):
                    yield SimpleNamespace(id=10_000 + index, date=NOW)

        client = Client()
        bot = LeadBot.__new__(LeadBot)
        bot.database = self.database
        bot.user_client = client
        bot.config = SimpleNamespace(
            send_catch_up=True,
            catch_up_limit=1,
            catch_up_source_limit=1,
            catch_up_newly_approved_sources_only=False,
            fresh_run_started_at=None,
            source_discovery_interval_seconds=3600,
        )
        bot._source_discovery_wake = asyncio.Event()
        bot._source_discovery_caught_up_keys = set()
        bot._active_sources = []
        bot._source_discovery_first_cycle = None
        bot._source_discovery_error = None
        bot.collector_account_id = account_id
        bot._dispatch_message = AsyncMock()

        async def wake(source_id):
            await bot._signal_source_discovery_wake(source_id)

        service = TelegramChatDiscoveryService(
            self.database,
            client,
            config=self._config(),
            collector_account_id=account_id,
            governor=TelegramRequestGovernor(
                self.database,
                account_id,
                self._config(),
                clock=lambda: NOW,
                random_uniform=lambda lower, _upper: lower,
            ),
            screen_provider=_ScreenProvider(),
            watch_candidate_callback=wake,
        )
        screened = await service.screen_peer(peer.id)
        self.assertIsNotNone(screened)
        self.assertTrue(bot._source_discovery_wake.is_set())

        async with self.database.connect() as connection:
            persisted_peer = await self.repository.get_peer(connection, peer.id)
        self.assertIsNotNone(persisted_peer.source_id)

        class FakeAuditingRuntime:
            def __init__(self):
                self.calls = 0

            async def run_once(self):
                self.calls += 1
                async with self.database.transaction() as connection:
                    await SourceRepository().transition(
                        connection,
                        persisted_peer.source_id,
                        SourceStatus.APPROVED,
                        reason="fake existing audit pipeline approved fixture",
                    )
                bot.source_discovery_runtime = None
                bot._source_discovery_wake.set()
                return SourceDiscoveryCycle(None, None, (), None, True)

        runtime = FakeAuditingRuntime()
        runtime.database = self.database
        bot.source_discovery_runtime = runtime

        class ApprovedSourceAdapter:
            def __init__(self):
                self.active_source = None

            async def list_for_session(self, _client):
                async with self.database.connect() as connection:
                    account = await CollectorAccountRepository().get(
                        connection,
                        account_id,
                    )
                    approved = await SourceRepository().list_sources(
                        connection,
                        status=SourceStatus.APPROVED,
                        platform="telegram",
                    )
                source = approved[0]
                self.active_source = TelegramCollectorSource(source, source.handle)
                return SimpleNamespace(
                    collector_account=account,
                    sources=(self.active_source,),
                )

        adapter = ApprovedSourceAdapter()
        adapter.database = self.database
        bot.source_adapter = adapter

        async def register_sources():
            return [(adapter.active_source, object())]

        bot._register_source_handlers = AsyncMock(side_effect=register_sources)
        task = asyncio.create_task(bot._run_source_discovery_loop())
        await asyncio.wait_for(task, timeout=2)

        self.assertEqual(runtime.calls, 1)
        bot._register_source_handlers.assert_awaited_once_with()
        bot._dispatch_message.assert_awaited_once()
        self.assertEqual(
            bot._dispatch_message.await_args.kwargs["origin"],
            "catch_up",
        )

    async def test_backpressure_pauses_new_topic_searches(self):
        account_id = await self._account("chat-discovery-backpressure")
        async with self.database.transaction() as connection:
            peer, _created = await self.repository.upsert_peer(
                connection,
                canonical_peer_identity="peer:backpressure",
                peer_type="group",
                telegram_peer_id=123,
                display_name="Backpressure fixture",
                username=None,
                canonical_url=None,
                access_type="private",
                source_id=None,
                dedup_bucket="GENUINELY_NEW",
                collector_account_id=account_id,
            )
            pressure = await self.repository.backpressure(
                connection,
                pending_screen_limit=1,
                source_audit_limit=100,
                ai_limit=100,
            )
        self.assertEqual(peer.screen_status, "SCREEN_PENDING")
        self.assertTrue(pressure.paused)
        self.assertIn("screen_backlog", pressure.reasons)

    async def test_two_collectors_have_independent_operation_state(self):
        first_id = await self._account("chat-discovery-collector-1")
        second_id = await self._account("chat-discovery-collector-2")
        self.assertNotEqual(first_id, second_id)
        async with self.database.connect() as connection:
            states = await TelegramCollectorOperationRepository().list_status(
                connection,
                now=NOW,
                limit=10,
            )
        by_id = {item.collector_account_id: item for item in states}
        self.assertEqual(by_id[first_id].status.value, "ready")
        self.assertEqual(by_id[second_id].status.value, "ready")

    async def test_screen_policy_keeps_small_ambiguous_samples_unclear_and_bad_samples_skip(self):
        policy = TelegramChatScreenPolicy(
            version="test.v1",
            minimum_sample=10,
            minimum_useful_messages=3,
            minimum_useful_ratio=0.12,
            minimum_confidence=0.65,
            maximum_seller_ratio=0.70,
            maximum_spam_ratio=0.70,
        )
        unclear = policy.evaluate(
            sample_count=2,
            classification=ScreenClassification(
                decision="WATCH",
                confidence=0.99,
                labels=("BUYER_TO_SPECIALIST", "BUYER_TO_SPECIALIST"),
            ),
        )
        skipped = policy.evaluate(
            sample_count=10,
            classification=ScreenClassification(
                decision="SKIP",
                confidence=0.99,
                labels=tuple("SELLER_SELF_PROMO" for _ in range(10)),
            ),
        )
        self.assertEqual(unclear[0], "UNCLEAR")
        self.assertEqual(skipped[0], "SKIP")


if __name__ == "__main__":
    unittest.main()
