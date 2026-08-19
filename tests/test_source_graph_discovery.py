import asyncio
from datetime import datetime, timezone
from types import SimpleNamespace
import unittest

import sqlalchemy as sa

from freelancer_bot.discovery import DiscoveryProvider, DiscoveryRequest
from freelancer_bot.discovery_runner import DiscoveryRunner
from freelancer_bot.persistence.collector_accounts import (
    CollectorAccessStatus,
    CollectorAccountRepository,
)
from freelancer_bot.persistence.database import Database
from freelancer_bot.persistence.schema import source_collector_access, sources
from freelancer_bot.persistence.source_repository import SourceRepository, SourceStatus
from freelancer_bot.persistence.source_seed import SourceSeedImporter
from freelancer_bot.source_graph_discovery import (
    GraphReferenceKind,
    PostgresSourceGraphSeedResolver,
    SourceGraphBackend,
    SourceGraphDiscoveryProvider,
    GraphSeedSelectionError,
    SourceGraphObservation,
    SourceGraphSeed,
    SourceGraphTarget,
    TelethonSourceGraphBackend,
)
from postgres_support import ROOT, TEST_DATABASE_URL, migrate_to_head, temporary_database


NOW = datetime(2026, 8, 9, 17, 0, tzinfo=timezone.utc)
SOURCES_PATH = ROOT / "config" / "sources.json"


class FakeTelethonGraphClient:
    def __init__(self, *, entities, histories):
        self.entities = entities
        self.histories = histories
        self.entity_calls = []
        self.iter_calls = []

    async def get_entity(self, lookup):
        self.entity_calls.append(lookup)
        return self.entities[lookup]

    def iter_messages(self, entity, *, limit):
        self.iter_calls.append((entity, limit))

        async def iterate():
            for message in self.histories.get(entity.id, ())[:limit]:
                yield message

        return iterate()


class RecordingGraphBackend:
    def __init__(self, responses):
        self.responses = responses
        self.calls = []

    async def scan(self, seed, *, message_limit):
        self.calls.append((seed.id, message_limit))
        return tuple(self.responses.get(seed.id, ()))


class RecordingRequestGovernor:
    def __init__(self):
        self.categories = []

    async def run(self, category, operation):
        self.categories.append(category)
        return await operation()


class StaticSeedResolver:
    def __init__(self, seeds):
        self.seeds = tuple(seeds)

    async def resolve(self, seed_source_ids):
        return self.seeds


class TelethonSourceGraphBackendTest(unittest.IsolatedAsyncioTestCase):
    async def test_extracts_links_mentions_forwards_and_accessible_invites(self):
        seed_entity = _entity(1, "Seed Community", "seed_source")
        mentioned = _entity(2, "Related Builders", "related_builders")
        linked = _entity(3, "Linked Group", "linked_group")
        hidden = _entity(4, "Hidden Link Group", "hidden_group")
        forwarded = _entity(5, "Forwarded Channel", "forwarded_channel")
        private = _entity(6, "Private Peer", None)
        invite = "https://t.me/+SecretInviteHash"
        message = SimpleNamespace(
            id=44,
            date=NOW,
            message=(
                "See @Related_Builders and https://t.me/linked_group "
                f"plus {invite}"
            ),
            entities=(SimpleNamespace(url="https://t.me/hidden_group"),),
            forward=SimpleNamespace(chat=forwarded),
            fwd_from=None,
        )
        client = FakeTelethonGraphClient(
            entities={
                "@seed_source": seed_entity,
                "@Related_Builders": mentioned,
                "@linked_group": linked,
                "@hidden_group": hidden,
                invite: private,
            },
            histories={seed_entity.id: (message,)},
        )
        backend = TelethonSourceGraphBackend(client)
        seed = SourceGraphSeed(
            id=10,
            platform="telegram",
            external_id="username:seed_source",
            access_type="public",
            display_name="Seed Community",
            handle="@seed_source",
            canonical_url="https://t.me/seed_source",
        )

        observations = await backend.scan(seed, message_limit=25)

        self.assertEqual(len(observations), 4)
        self.assertEqual(
            {observation.kind for observation in observations},
            {
                GraphReferenceKind.LINK,
                GraphReferenceKind.FORWARD,
                GraphReferenceKind.INVITE,
            },
        )
        self.assertEqual(client.iter_calls, [(seed_entity, 25)])
        self.assertTrue(
            any(
                observation.target.external_id == "username:linked_group"
                for observation in observations
            )
        )
        private_observation = next(
            observation
            for observation in observations
            if observation.kind is GraphReferenceKind.INVITE
        )
        self.assertEqual(private_observation.target.access_type, "private")
        self.assertTrue(private_observation.reference.startswith("invite:sha256:"))
        self.assertNotIn("SecretInviteHash", private_observation.reference)
        self.assertNotIn("@Related_Builders", client.entity_calls)
        self.assertNotIn("@helper_bot", client.entity_calls)

    async def test_local_filter_known_source_noise_and_budget_happen_before_resolution(self):
        seed_entity = _entity(1, "Seed Community", "seed_source")
        second = _entity(2, "Second Group", "second_group")
        third = _entity(3, "Third Group", "third_group")
        message = SimpleNamespace(
            id=45,
            date=NOW,
            message=(
                "https://t.me/valid_group https://t.me/valid_group/123 "
                "@ordinary_person @helper_bot https://t.me/no "
                "https://example.com/not_telegram https://t.me/known_source "
                "https://t.me/second_group https://t.me/third_group"
            ),
            entities=(),
            forward=None,
            fwd_from=None,
        )
        client = FakeTelethonGraphClient(
            entities={
                "@seed_source": seed_entity,
                "@second_group": second,
                "@third_group": third,
            },
            histories={seed_entity.id: (message,)},
        )
        backend = TelethonSourceGraphBackend(
            client,
            known_source_identities=("username:known_source",),
            entity_resolution_budget=2,
        )
        seed = _seed()

        observations = await backend.scan(seed, message_limit=25)

        self.assertEqual(
            [observation.target.external_id for observation in observations],
            ["username:second_group", "username:third_group"],
        )
        self.assertEqual(
            client.entity_calls,
            ["@seed_source", "@second_group", "@third_group"],
        )
        self.assertEqual(
            backend.last_observability.to_payload(),
            {
                "messages_sampled": 1,
                "raw_references_extracted": 7,
                "references_after_local_validation": 5,
                "references_after_dedup": 4,
                "known_sources_removed": 1,
                "entity_resolve_attempts": 2,
                "entity_resolve_successes": 2,
                "entity_resolve_errors": 0,
                "candidate_sources_created": 0,
                "entity_resolve_error_categories": {},
                "reference_kinds_after_local_validation": {"link": 5},
            },
        )

    async def test_duplicate_reference_is_resolved_once_across_seeds(self):
        first_seed = _entity(1, "First Seed", "first_seed")
        second_seed = _entity(2, "Second Seed", "second_seed")
        related = _entity(3, "Related Group", "related_group")
        message = SimpleNamespace(
            id=46,
            date=NOW,
            message="https://t.me/related_group",
            entities=(),
            forward=None,
            fwd_from=None,
        )
        client = FakeTelethonGraphClient(
            entities={
                "@first_seed": first_seed,
                "@second_seed": second_seed,
                "@related_group": related,
            },
            histories={first_seed.id: (message,), second_seed.id: (message,)},
        )
        backend = TelethonSourceGraphBackend(
            client,
            entity_resolution_budget=1,
        )
        backend.begin_run()

        await backend.scan(_seed(id=10, handle="@first_seed"), message_limit=25)
        observations = await backend.scan(
            _seed(id=11, handle="@second_seed"),
            message_limit=25,
        )

        self.assertEqual(client.entity_calls, ["@first_seed", "@related_group", "@second_seed"])
        self.assertEqual(len(observations), 1)
        self.assertEqual(backend.run_observability.entity_resolve_attempts, 1)

    async def test_provider_refuses_to_scan_without_approved_accessible_seeds(self):
        backend = RecordingGraphBackend({})
        provider = SourceGraphDiscoveryProvider(StaticSeedResolver(()), backend)
        with self.assertRaises(GraphSeedSelectionError):
            await provider.discover(
                DiscoveryRequest(
                    parameters={},
                    requested_at=NOW,
                    seed_source_ids=(999,),
                )
            )
        self.assertEqual(backend.calls, [])

    async def test_governed_scan_caps_history_and_does_not_nest_request_lock(self):
        seed_entity = _entity(1, "Seed Community", "seed_source")
        related = _entity(2, "Related Builders", "related_builders")
        message = SimpleNamespace(
            id=44,
            date=NOW,
            message="See https://t.me/related_builders",
            entities=(),
            forward=None,
            fwd_from=None,
        )
        client = FakeTelethonGraphClient(
            entities={"@seed_source": seed_entity, "@related_builders": related},
            histories={seed_entity.id: (message,)},
        )
        governor = RecordingRequestGovernor()
        backend = TelethonSourceGraphBackend(
            client,
            governor=governor,
            max_message_limit=20,
        )
        seed = SourceGraphSeed(
            id=10,
            platform="telegram",
            external_id="username:seed_source",
            access_type="public",
            display_name="Seed Community",
            handle="@seed_source",
            canonical_url="https://t.me/seed_source",
        )

        observations = await backend.scan(seed, message_limit=25)

        self.assertEqual(len(observations), 1)
        self.assertEqual(client.iter_calls, [(seed_entity, 20)])
        self.assertEqual(
            governor.categories,
            ["entity_access", "graph_history", "entity_access"],
        )


@unittest.skipUnless(TEST_DATABASE_URL, "TEST_DATABASE_URL is not configured")
class SourceGraphPostgresIntegrationTest(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.database_context = temporary_database()
        self.database_url = self.database_context.__enter__()
        migrate_to_head(self.database_url)
        self.database = Database(self.database_url, pool_size=4, max_overflow=8)
        self.sources = SourceRepository()
        self.accounts = CollectorAccountRepository()

    async def asyncTearDown(self):
        await self.database.close()
        self.database_context.__exit__(None, None, None)

    async def test_deduplicates_candidates_from_only_approved_accessible_seeds(self):
        seeded = await SourceSeedImporter(self.database).import_file(SOURCES_PATH)
        self.assertEqual((seeded.created, seeded.total), (15, 15))
        seed_ids, before = await self._repository_seed_snapshot()
        approved_public_ids = [
            source_id
            for source_id, status, _ in before
            if status == SourceStatus.APPROVED.value
        ]
        candidate_seed_id = next(
            source_id
            for source_id, status, _ in before
            if status == SourceStatus.CANDIDATE.value
        )
        self.assertGreaterEqual(len(approved_public_ids), 1)

        async with self.database.transaction() as connection:
            account = await self.accounts.ensure(
                connection,
                platform="telegram",
                external_account_id="graph-collector-1",
                display_name="Graph collector",
            )
            permitted_private = await self._create_private_seed(
                connection,
                "permitted-private-seed",
            )
            inaccessible_private = await self._create_private_seed(
                connection,
                "inaccessible-private-seed",
            )
            await self.accounts.record_source_access(
                connection,
                source_id=permitted_private.id,
                collector_account_id=account.id,
                access_status=CollectorAccessStatus.PERMITTED,
                checked_at=NOW,
                checked_by="graph-test",
            )

        public_seed_id = approved_public_ids[0]
        public_target = SourceGraphTarget(
            external_id="username:related_product_builders",
            access_type="public",
            display_name="Related Product Builders",
            handle="@related_product_builders",
            canonical_url="https://t.me/related_product_builders",
        )
        private_target = SourceGraphTarget(
            external_id="peer:-100900001",
            access_type="private",
            display_name="Accessible Invite Community",
        )
        public_seed = await self._get_source(public_seed_id)
        self_target = SourceGraphTarget(
            external_id=public_seed.external_id,
            access_type="public",
            display_name=public_seed.display_name,
            handle=public_seed.handle,
            canonical_url=public_seed.canonical_url,
        )
        backend = RecordingGraphBackend(
            {
                public_seed_id: (
                    _observation(public_target, "mention", "@related_product_builders", 10),
                    _observation(public_target, "link", "https://t.me/related_product_builders", 11),
                    _observation(self_target, "mention", public_seed.handle, 12),
                ),
                permitted_private.id: (
                    _observation(public_target, "forward", "forward:public", 20),
                    _observation(private_target, "invite", "invite:sha256:fixture", 21),
                ),
                inaccessible_private.id: (
                    _observation(private_target, "link", "must-not-run", 30),
                ),
                candidate_seed_id: (
                    _observation(private_target, "link", "must-not-run", 31),
                ),
            }
        )
        resolver = PostgresSourceGraphSeedResolver(
            self.database,
            collector_account_id=account.id,
        )
        provider = SourceGraphDiscoveryProvider(
            resolver,
            backend,
            message_limit_per_seed=50,
        )
        self.assertIsInstance(backend, SourceGraphBackend)
        self.assertIsInstance(provider, DiscoveryProvider)

        execution = await DiscoveryRunner(
            self.database,
            clock=lambda: NOW,
        ).run(
            provider,
            run_key="approved-source-graph-v1",
            request=DiscoveryRequest(
                parameters={"purpose": "graph fixture"},
                requested_at=NOW,
                seed_source_ids=(
                    public_seed_id,
                    candidate_seed_id,
                    permitted_private.id,
                    inaccessible_private.id,
                ),
            ),
        )

        self.assertEqual(
            backend.calls,
            [(public_seed_id, 50), (permitted_private.id, 50)],
        )
        self.assertEqual(execution.run.provider, "telegram_source_graph")
        self.assertEqual(execution.run.provider_kind, "source_graph")
        self.assertEqual(execution.run.result_count, 2)
        self.assertEqual(len(execution.results), 2)

        async with self.database.connect() as connection:
            discovered = [
                await self.sources.get(connection, result.source_id)
                for result in execution.results
            ]
            lineages = [
                (await self.sources.list_lineage(connection, source.id))[0]
                for source in discovered
            ]
            private_candidate_id = next(
                source.id for source in discovered if source.access_type == "private"
            )
            inferred_access = await connection.scalar(
                sa.select(sa.func.count())
                .select_from(source_collector_access)
                .where(source_collector_access.c.source_id == private_candidate_id)
            )

        self.assertTrue(
            all(source.lifecycle_status is SourceStatus.CANDIDATE for source in discovered)
        )
        self.assertEqual(inferred_access, 0)
        self.assertTrue(
            all(lineage.discovery_run_id == execution.run.id for lineage in lineages)
        )
        self.assertTrue(
            all(lineage.provider == "telegram_source_graph" for lineage in lineages)
        )
        public_lineage = next(
            lineage
            for lineage in lineages
            if len(lineage.context["observations"]) == 3
        )
        self.assertEqual(
            {
                observation["seed_source_id"]
                for observation in public_lineage.context["observations"]
            },
            {public_seed_id, permitted_private.id},
        )

        after_ids, after = await self._repository_seed_snapshot()
        self.assertEqual(after_ids, seed_ids)
        self.assertEqual(after, before)
        repeated = await SourceSeedImporter(self.database).import_file(SOURCES_PATH)
        self.assertEqual(
            (repeated.created, repeated.updated, repeated.unchanged),
            (0, 0, 15),
        )

    async def _create_private_seed(self, connection, suffix):
        source = await self.sources.create_candidate(
            connection,
            platform="telegram",
            external_id=f"peer:{suffix}",
            access_type="private",
            display_name=suffix,
            provider="graph_fixture_seed",
            lineage_key=suffix,
        )
        return await self.sources.transition(
            connection,
            source.id,
            SourceStatus.APPROVED,
            reason="graph fixture approved seed",
        )

    async def _get_source(self, source_id):
        async with self.database.connect() as connection:
            return await self.sources.get(connection, source_id)

    async def _repository_seed_snapshot(self):
        async with self.database.connect() as connection:
            result = await connection.execute(
                sa.select(
                    sources.c.id,
                    sources.c.lifecycle_status,
                    sources.c.external_id,
                ).where(sources.c.id <= 15).order_by(sources.c.id)
            )
            rows = result.all()
        return (
            tuple(row.id for row in rows),
            tuple((row.id, row.lifecycle_status, row.external_id) for row in rows),
        )


def _entity(entity_id, title, username):
    return SimpleNamespace(
        id=entity_id,
        title=title,
        username=username,
        usernames=(),
        broadcast=True,
        megagroup=False,
        _graph_community=True,
    )


def _seed(*, id=10, handle="@seed_source"):
    username = handle.removeprefix("@")
    return SourceGraphSeed(
        id=id,
        platform="telegram",
        external_id=f"username:{username}",
        access_type="public",
        display_name="Seed Community",
        handle=handle,
        canonical_url=f"https://t.me/{username}",
    )


def _observation(target, kind, reference, message_id):
    return SourceGraphObservation(
        target=target,
        kind=kind,
        reference=reference,
        observed_at=NOW,
        message_id=message_id,
    )


if __name__ == "__main__":
    unittest.main()
