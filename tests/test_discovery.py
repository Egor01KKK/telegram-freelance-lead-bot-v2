import asyncio
import unittest
from datetime import datetime, timezone

import sqlalchemy as sa
from alembic import command

from freelancer_bot.discovery import (
    DiscoveredSourceCandidate,
    DiscoveryProvider,
    DiscoveryRequest,
)
from freelancer_bot.discovery_runner import DiscoveryExecutionError, DiscoveryRunner
from freelancer_bot.persistence.database import Database
from freelancer_bot.persistence.discovery import (
    DiscoveryResultOutcome,
    DiscoveryRunConflict,
    DiscoveryRunRepository,
    DiscoveryRunStateError,
    DiscoveryRunStatus,
)
from freelancer_bot.persistence.schema import source_discovery_lineage, sources
from freelancer_bot.persistence.source_repository import SourceRepository, SourceStatus
from freelancer_bot.persistence.source_seed import SourceSeedImporter
from postgres_support import (
    ROOT,
    TEST_DATABASE_URL,
    alembic_config,
    migrate_to_head,
    temporary_database,
)


NOW = datetime(2026, 8, 9, 12, 0, tzinfo=timezone.utc)
SOURCES_PATH = ROOT / "config" / "sources.json"


class StubDiscoveryProvider:
    def __init__(self, name, kind, candidates=(), error=None, observability=None):
        self._name = name
        self._kind = kind
        self._candidates = tuple(candidates)
        self._error = error
        self.observability = observability
        self.calls = 0

    @property
    def name(self):
        return self._name

    @property
    def kind(self):
        return self._kind

    async def discover(self, request):
        self.calls += 1
        if self._error is not None:
            raise self._error
        return self._candidates


@unittest.skipUnless(TEST_DATABASE_URL, "TEST_DATABASE_URL is not configured")
class DiscoveryIntegrationTest(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.database_context = temporary_database()
        self.database_url = self.database_context.__enter__()
        migrate_to_head(self.database_url)
        self.database = Database(self.database_url, pool_size=4, max_overflow=8)
        self.runs = DiscoveryRunRepository()
        self.sources = SourceRepository()
        self.runner = DiscoveryRunner(
            self.database,
            runs=self.runs,
            sources=self.sources,
            clock=lambda: NOW,
        )

    async def asyncTearDown(self):
        await self.database.close()
        self.database_context.__exit__(None, None, None)

    async def test_seed_web_and_graph_contracts_persist_normalized_linked_candidates(self):
        first_seed = await SourceSeedImporter(self.database).import_file(SOURCES_PATH)
        self.assertEqual((first_seed.created, first_seed.total), (15, 15))
        original = await self._source_snapshot()

        seed_candidate = self._candidate(
            "seed-result-1",
            "username:contract_seed_candidate",
            "@CONTRACT_SEED_CANDIDATE",
        )
        seed_provider = StubDiscoveryProvider(
            "Repository_Seed_Contract",
            "Seed",
            [seed_candidate],
        )
        self.assertIsInstance(seed_provider, DiscoveryProvider)
        seed_execution = await self.runner.run(
            seed_provider,
            run_key="seed-run-1",
            request=self._request("seed fixture"),
        )
        seed_source_id = seed_execution.results[0].source_id

        web_candidate = self._candidate(
            "web-result-1",
            "username:contract_web_candidate",
            "@CONTRACT_WEB_CANDIDATE",
            seed_source_id=seed_source_id,
            seed_reference="https://example.test/search?q=telegram",
        )
        web_provider = StubDiscoveryProvider("Web_Contract", "Web", [web_candidate])
        graph_candidate = self._candidate(
            "graph-result-1",
            "username:contract_graph_candidate",
            "@CONTRACT_GRAPH_CANDIDATE",
            seed_source_id=seed_source_id,
            seed_reference="forward:seed-result-1",
        )
        graph_provider = StubDiscoveryProvider(
            "Source_Graph_Contract",
            "Source_Graph",
            [graph_candidate],
        )

        web_execution = await self.runner.run(
            web_provider,
            run_key="web-run-1",
            request=self._request("web fixture", seed_source_id),
        )
        graph_execution = await self.runner.run(
            graph_provider,
            run_key="graph-run-1",
            request=self._request("graph fixture", seed_source_id),
        )

        for execution in (seed_execution, web_execution, graph_execution):
            self.assertEqual(execution.run.status, DiscoveryRunStatus.COMPLETED)
            self.assertEqual(execution.run.result_count, 1)
            self.assertEqual(execution.run.materialized_count, 1)
            self.assertEqual(execution.results[0].outcome, DiscoveryResultOutcome.CREATED)
            self.assertEqual(execution.results[0].platform, "telegram")
            self.assertEqual(execution.results[0].handle, execution.results[0].handle.lower())

        async with self.database.connect() as connection:
            web_source = await self.sources.get(
                connection,
                web_execution.results[0].source_id,
            )
            web_lineage = await self.sources.list_lineage(connection, web_source.id)
            source_count = await connection.scalar(
                sa.select(sa.func.count()).select_from(sources)
            )

        self.assertEqual(web_source.lifecycle_status, SourceStatus.CANDIDATE)
        self.assertEqual(web_lineage[0].provider, "web_contract")
        self.assertEqual(web_lineage[0].discovery_run_id, web_execution.run.id)
        self.assertEqual(web_lineage[0].provider_run_id, str(web_execution.run.id))
        self.assertEqual(web_lineage[0].seed_source_id, seed_source_id)
        self.assertEqual(
            web_lineage[0].seed_reference,
            "https://example.test/search?q=telegram",
        )
        self.assertEqual(source_count, 18)
        original_ids = {int(row.id) for row in original}
        self.assertEqual(
            await self._source_snapshot(limit_ids=original_ids),
            original,
        )

        repeated_seed = await SourceSeedImporter(self.database).import_file(SOURCES_PATH)
        self.assertEqual(
            (repeated_seed.created, repeated_seed.updated, repeated_seed.unchanged),
            (0, 0, 15),
        )
        dependency_text = (
            (ROOT / "pyproject.toml").read_text()
            + (ROOT / "uv.lock").read_text()
        ).lower()
        self.assertNotIn("tgstat", dependency_text)
        self.assertNotIn("telemetr", dependency_text)

    async def test_run_key_is_idempotent_and_existing_source_gets_new_lineage(self):
        candidate = self._candidate(
            "stable-result",
            "username:stable_discovery_candidate",
            "@STABLE_DISCOVERY_CANDIDATE",
        )
        provider = StubDiscoveryProvider("web_fixture", "web", [candidate])
        request = self._request("stable")
        first = await self.runner.run(
            provider,
            run_key="stable-run",
            request=request,
        )
        repeated = await self.runner.run(
            provider,
            run_key="stable-run",
            request=request,
        )

        self.assertEqual(provider.calls, 1)
        self.assertEqual(repeated.run.id, first.run.id)
        self.assertEqual(repeated.results, first.results)
        with self.assertRaises(DiscoveryRunStateError):
            async with self.database.transaction() as connection:
                await self.runs.record_result(
                    connection,
                    run_id=first.run.id,
                    candidate=candidate,
                    source_id=first.results[0].source_id,
                    outcome=DiscoveryResultOutcome.EXISTING,
                )

        second_provider = StubDiscoveryProvider(
            "graph_fixture",
            "source_graph",
            [
                self._candidate(
                    "graph-reference",
                    candidate.external_id,
                    candidate.handle,
                )
            ],
        )
        second = await self.runner.run(
            second_provider,
            run_key="graph-run",
            request=self._request("graph stable"),
        )
        self.assertEqual(second.results[0].source_id, first.results[0].source_id)
        self.assertEqual(second.results[0].outcome, DiscoveryResultOutcome.EXISTING)
        async with self.database.connect() as connection:
            lineage = await self.sources.list_lineage(
                connection,
                first.results[0].source_id,
            )
        self.assertEqual([item.provider for item in lineage], ["web_fixture", "graph_fixture"])

        with self.assertRaises(DiscoveryRunConflict):
            await self.runner.run(
                StubDiscoveryProvider("web_fixture", "web", [candidate]),
                run_key="stable-run",
                request=self._request("different request"),
            )

    async def test_provider_observability_is_persisted_and_does_not_break_idempotency(self):
        candidate = self._candidate(
            "observability-result",
            "username:observability_candidate",
            "@OBSERVABILITY_CANDIDATE",
        )
        observability = {
            "messages_sampled": 25,
            "raw_references_extracted": 17,
            "references_after_local_validation": 6,
            "references_after_dedup": 3,
            "known_sources_removed": 0,
            "entity_resolve_attempts": 3,
            "entity_resolve_successes": 2,
            "entity_resolve_errors": 1,
            "candidate_sources_created": 0,
            "entity_resolve_error_categories": {"invalid_username": 1},
            "reference_kinds_after_local_validation": {"link": 6},
        }
        provider = StubDiscoveryProvider(
            "graph_observable",
            "source_graph",
            [candidate],
            observability=observability,
        )
        request = self._request("observable")

        first = await self.runner.run(
            provider,
            run_key="observable-run",
            request=request,
        )
        repeated = await self.runner.run(
            StubDiscoveryProvider(
                "graph_observable",
                "source_graph",
                [candidate],
                observability=observability,
            ),
            run_key="observable-run",
            request=request,
        )

        self.assertEqual(
            first.run.request["observability"]["candidate_sources_created"],
            1,
        )
        self.assertEqual(
            first.run.request["observability"]["entity_resolve_error_categories"],
            {"invalid_username": 1},
        )
        self.assertEqual(repeated.run.id, first.run.id)
        self.assertEqual(repeated.results, first.results)

    async def test_provider_failure_is_persisted_without_partial_results(self):
        provider = StubDiscoveryProvider(
            "failing_web_fixture",
            "web",
            error=RuntimeError("credential-like detail must not be persisted"),
        )
        with self.assertRaises(DiscoveryExecutionError) as raised:
            await self.runner.run(
                provider,
                run_key="failed-run",
                request=self._request("failure"),
            )

        async with self.database.connect() as connection:
            run = await self.runs.get_by_key(
                connection,
                provider="failing_web_fixture",
                run_key="failed-run",
            )
            results = await self.runs.list_results(connection, run.id)
        self.assertEqual(run.status, DiscoveryRunStatus.FAILED)
        self.assertEqual(run.failure_code, "provider.runtime_error")
        self.assertEqual(raised.exception.run_id, run.id)
        self.assertEqual(results, [])
        self.assertNotIn("credential-like", run.failure_code)

    async def test_provider_failure_persists_safe_observability(self):
        provider = StubDiscoveryProvider(
            "failing_graph_fixture",
            "source_graph",
            error=RuntimeError("provider failure"),
            observability={
                "messages_sampled": 25,
                "raw_references_extracted": 8,
                "references_after_local_validation": 2,
                "references_after_dedup": 1,
                "known_sources_removed": 1,
                "entity_resolve_attempts": 0,
                "entity_resolve_successes": 0,
                "entity_resolve_errors": 0,
                "candidate_sources_created": 0,
                "entity_resolve_error_categories": {},
                "reference_kinds_after_local_validation": {"link": 2},
            },
        )
        with self.assertRaises(DiscoveryExecutionError):
            await self.runner.run(
                provider,
                run_key="failed-observable-graph-run",
                request=self._request("observable failure"),
            )

        async with self.database.connect() as connection:
            run = await self.runs.get_by_key(
                connection,
                provider="failing_graph_fixture",
                run_key="failed-observable-graph-run",
            )
        self.assertEqual(run.status, DiscoveryRunStatus.FAILED)
        self.assertEqual(
            run.request["observability"]["references_after_dedup"],
            1,
        )
        self.assertEqual(
            run.request["observability"]["reference_kinds_after_local_validation"],
            {"link": 2},
        )

    def _candidate(
        self,
        result_key,
        external_id,
        handle,
        *,
        seed_source_id=None,
        seed_reference=None,
    ):
        return DiscoveredSourceCandidate(
            result_key=result_key,
            platform="TELEGRAM",
            external_id=external_id,
            access_type="PUBLIC",
            display_name=f"Candidate {result_key}",
            handle=handle,
            canonical_url=f"https://t.me/{handle.removeprefix('@').lower()}",
            discovered_at=NOW,
            seed_source_id=seed_source_id,
            seed_reference=seed_reference,
            context={"fixture": result_key},
        )

    def _request(self, query, *seed_source_ids):
        return DiscoveryRequest(
            parameters={"query": query},
            requested_at=NOW,
            seed_source_ids=tuple(seed_source_ids),
        )

    async def _source_snapshot(self, limit_ids=None):
        async with self.database.connect() as connection:
            statement = sa.select(
                sources.c.id,
                sources.c.platform,
                sources.c.external_id,
                sources.c.access_type,
                sources.c.lifecycle_status,
                sources.c.display_name,
                sources.c.handle,
                sources.c.canonical_url,
            ).order_by(sources.c.id)
            if limit_ids is not None:
                statement = statement.where(sources.c.id.in_(limit_ids))
            rows = await connection.execute(statement)
            return rows.all()


@unittest.skipUnless(TEST_DATABASE_URL, "TEST_DATABASE_URL is not configured")
class DiscoveryMigrationCompatibilityTest(unittest.TestCase):
    def test_existing_seed_sources_and_lineage_survive_discovery_migration(self):
        with temporary_database() as database_url:
            config = alembic_config(database_url)
            command.upgrade(config, "20260809_0006")
            first = asyncio.run(_import_seed(database_url))
            before = _sync_source_snapshot(database_url)

            command.upgrade(config, "head")
            after = _sync_source_snapshot(database_url)
            repeated = asyncio.run(_import_seed(database_url))

            engine = sa.create_engine(database_url)
            try:
                with engine.connect() as connection:
                    run_ids = connection.scalars(
                        sa.select(source_discovery_lineage.c.discovery_run_id)
                    ).all()
            finally:
                engine.dispose()

            self.assertEqual((first.created, first.total), (15, 15))
            self.assertEqual(before, after)
            self.assertEqual(len(after), 15)
            self.assertEqual(run_ids, [None] * 15)
            self.assertEqual(
                (repeated.created, repeated.updated, repeated.unchanged),
                (0, 0, 15),
            )


async def _import_seed(database_url):
    database = Database(database_url)
    try:
        return await SourceSeedImporter(database).import_file(SOURCES_PATH)
    finally:
        await database.close()


def _sync_source_snapshot(database_url):
    engine = sa.create_engine(database_url)
    try:
        with engine.connect() as connection:
            return connection.execute(
                sa.select(
                    sources.c.id,
                    sources.c.platform,
                    sources.c.external_id,
                    sources.c.access_type,
                    sources.c.lifecycle_status,
                    sources.c.display_name,
                    sources.c.handle,
                    sources.c.canonical_url,
                ).order_by(sources.c.id)
            ).all()
    finally:
        engine.dispose()


if __name__ == "__main__":
    unittest.main()
