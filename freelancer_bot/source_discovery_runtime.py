"""Bounded autonomous source discovery/audit orchestration.

This module composes the existing discovery, source graph, audit and
collector-catalog services.  It intentionally owns no discovery heuristics or
database writes of its own; all candidates, lineage and lifecycle transitions
flow through the existing repositories and pipelines.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime, timezone
import logging
from typing import Any

from telethon.errors import RPCError

from .config import RuntimeConfig
from .discovery import DiscoveryRequest
from .discovery_runner import DiscoveryExecution, DiscoveryExecutionError, DiscoveryRunner
from .observability import log_event
from .persistence.database import Database
from .persistence.source_repository import (
    SourceRepository,
    SourceStatus,
)
from .persistence.search_profiles import SearchProfileRepository
from .persistence.telegram_chat_discovery import TelegramChatDiscoveryRepository
from .profile_discovery import ProfileDiscoveryExecution, ProfileDiscoveryService
from .source_audit import (
    OpenAISourceAuditProvider,
    SourceAuditPipeline,
    SourceAuditRunResult,
)
from .source_audit_sampler import (
    SourceAuditTarget,
    SourceAuditPolicy,
    SourceAuditSampler,
    TelethonSourceAuditHistoryReader,
)
from .source_graph_discovery import (
    PostgresSourceGraphSeedResolver,
    SourceGraphDiscoveryProvider,
    TelethonSourceGraphBackend,
)
from .source_reaudit import SourceReauditBatch, SourceReauditPolicy, SourceReauditScheduler
from .telegram_collector import ApprovedTelegramSourceAdapter
from .telegram_chat_discovery import input_entity_for_peer
from .telegram_profile_discovery import TelegramGlobalSearchPageCache
from .telegram_request_governor import TelegramRequestGovernor
from .web_discovery import (
    SearxngSearchBackend,
    WebDiscoveryGovernor,
    WebDiscoveryProvider,
)
from .web_provider_chain import build_web_search_backends, web_discovery_readiness


@dataclass(frozen=True)
class SourceDiscoveryCycle:
    graph: DiscoveryExecution | None
    web: DiscoveryExecution | None
    audits: tuple[SourceAuditRunResult, ...]
    reaudit: SourceReauditBatch | None
    reload_required: bool
    profile_web: tuple[ProfileDiscoveryExecution, ...] = ()
    profile_telegram: tuple[ProfileDiscoveryExecution, ...] = ()


class AutonomousSourceDiscoveryRuntime:
    """Run one bounded discovery/audit/reconciliation cycle.

    The caller controls cadence and process lifetime.  A deterministic bucketed
    run key makes retries and concurrent runtime instances converge on the
    existing DiscoveryRunner idempotency boundary.
    """

    def __init__(
        self,
        database: Database,
        client: Any,
        config: RuntimeConfig,
        *,
        source_adapter: ApprovedTelegramSourceAdapter | None = None,
        runner: DiscoveryRunner | None = None,
        logger: logging.Logger | None = None,
        clock: Callable[[], datetime] | None = None,
        governor: TelegramRequestGovernor | None = None,
    ) -> None:
        if not hasattr(client, "get_entity") or not hasattr(client, "iter_messages"):
            raise TypeError("discovery client must expose get_entity and iter_messages")
        self._database = database
        self._client = client
        self._config = config
        self._source_adapter = source_adapter or ApprovedTelegramSourceAdapter(database)
        self._runner = runner or DiscoveryRunner(database)
        self._logger = logger or logging.getLogger("freelancer_bot")
        self._clock = clock or (lambda: datetime.now(timezone.utc))
        self._governor = governor
        self._web_governor = WebDiscoveryGovernor.from_config(
            config,
            database=database,
        )
        self._profile_discovery = ProfileDiscoveryService(
            database,
            runner=self._runner,
            web_governor=self._web_governor,
        )

    async def run_once(self) -> SourceDiscoveryCycle:
        if not self._config.source_discovery_enabled:
            log_event(
                self._logger,
                logging.INFO,
                "source.discovery.disabled",
                reason="SOURCE_DISCOVERY_ENABLED=false",
            )
            return SourceDiscoveryCycle(None, None, (), None, False)

        now = _aware_now(self._clock())
        snapshot = await self._source_adapter.list_for_session(self._client)
        if (
            self._governor is None
            or self._governor.collector_account_id != snapshot.collector_account.id
        ):
            self._governor = TelegramRequestGovernor(
                self._database,
                snapshot.collector_account.id,
                self._config,
            )
        seed_ids = tuple(
            source.record.id
            for source in snapshot.sources[
                : min(
                    self._config.source_discovery_seed_limit,
                    self._config.telegram_graph_seeds_per_pass,
                )
            ]
        )
        bucket = int(now.timestamp()) // self._config.source_discovery_interval_seconds
        profile_telegram = await self._run_profile_telegram(
            requested_at=now,
            bucket=bucket,
            governor=self._governor,
            page_cache=TelegramGlobalSearchPageCache(),
        )
        graph = await self._run_graph(snapshot.collector_account.id, seed_ids, now, bucket)
        web = await self._run_web(now, bucket)
        profile_web = await self._run_profile_web(now, bucket)

        audit_pipeline = self._build_audit_pipeline()
        audits: list[SourceAuditRunResult] = []
        candidate_ids = _merge_candidate_source_ids(
            _candidate_source_ids(graph, web, profile_telegram),
            (
                ()
                if self._config.source_discovery_audit_new_candidates_only
                else await self._pending_candidate_ids()
            ),
        )
        if audit_pipeline is not None:
            audit_limit = min(
                self._config.source_discovery_audit_limit,
                self._config.telegram_max_audits_per_batch,
            )
            for source_id in candidate_ids[:audit_limit]:
                result = await self._audit_candidate(
                    source_id,
                    collector_account_id=snapshot.collector_account.id,
                    pipeline=audit_pipeline,
                    audited_at=now,
                )
                if result is not None:
                    audits.append(result)
        else:
            log_event(
                self._logger,
                logging.INFO,
                "source.audit.disabled",
                reason=(
                    "SOURCE_AUDIT_ENABLED=false"
                    if not self._config.source_audit_enabled
                    else "OPENAI_API_KEY_unconfigured"
                ),
                candidate_count=len(candidate_ids),
            )

        reaudit = None
        if (
            audit_pipeline is not None
            and not self._config.source_discovery_audit_new_candidates_only
        ):
            scheduler = SourceReauditScheduler(
                self._database,
                audit_pipeline,
                collector_account_id=snapshot.collector_account.id,
                policy=SourceReauditPolicy.from_config(self._config),
            )
            try:
                reaudit = await scheduler.run_once(
                    as_of=now,
                    limit=min(
                        self._config.source_discovery_reaudit_limit,
                        self._config.telegram_max_audits_per_batch,
                    ),
                )
            except Exception as exc:
                log_event(
                    self._logger,
                    logging.WARNING,
                    "source.reaudit_cycle_failed",
                    error=exc,
                )

        reload_required = any(
            result.lifecycle_changed
            and result.source.lifecycle_status
            in {SourceStatus.APPROVED, SourceStatus.PAUSED, SourceStatus.REJECTED}
            for result in audits
        ) or bool(reaudit and (reaudit.completed or reaudit.failures))
        log_event(
            self._logger,
            logging.INFO,
            "source.discovery.cycle_completed",
            graph_result_count=0 if graph is None else len(graph.results),
            web_result_count=0 if web is None else len(web.results),
            profile_web_result_count=sum(len(item.execution.results) for item in profile_web),
            profile_telegram_result_count=sum(
                len(item.execution.results) for item in profile_telegram
            ),
            profile_count=len(profile_web),
            audited_count=len(audits),
            reaudit_due=0 if reaudit is None else len(reaudit.due),
            reaudit_completed=0 if reaudit is None else len(reaudit.completed),
            reaudit_failures=0 if reaudit is None else len(reaudit.failures),
            reload_required=reload_required,
        )
        return SourceDiscoveryCycle(
            graph=graph,
            web=web,
            audits=tuple(audits),
            reaudit=reaudit,
            reload_required=reload_required,
            profile_web=profile_web,
            profile_telegram=profile_telegram,
        )

    async def _run_profile_telegram(
        self,
        *,
        requested_at: datetime,
        bucket: int,
        governor: TelegramRequestGovernor,
        page_cache: TelegramGlobalSearchPageCache,
    ) -> tuple[ProfileDiscoveryExecution, ...]:
        if not self._config.telegram_global_discovery_enabled:
            log_event(
                self._logger,
                logging.INFO,
                "profile.discovery.telegram_disabled",
                reason="TELEGRAM_GLOBAL_DISCOVERY_ENABLED=false",
            )
            return ()
        if not hasattr(self._client, "get_messages"):
            log_event(
                self._logger,
                logging.INFO,
                "profile.discovery.telegram_unavailable",
                reason="collector_client_has_no_global_search_capability",
            )
            return ()
        async with self._database.connect() as connection:
            profiles = await SearchProfileRepository().list_active(connection)
        executions: list[ProfileDiscoveryExecution] = []
        for profile in profiles:
            try:
                executions.append(
                    await self._profile_discovery.discover_telegram_profile(
                        profile,
                        requested_at=requested_at,
                        run_key=(
                            f"profile-telegram-discovery:{profile.id}:"
                            f"{profile.revision}:{bucket}"
                        ),
                        client=self._client,
                        governor=governor,
                        max_candidates=min(
                            100,
                            self._config.source_discovery_max_candidates,
                        ),
                        fresh_run_id=self._config.fresh_run_id,
                        fresh_run_started_at=self._config.fresh_run_started_at,
                        page_cache=page_cache,
                    )
                )
            except DiscoveryExecutionError as exc:
                log_event(
                    self._logger,
                    logging.WARNING,
                    "profile.discovery.telegram_failed",
                    profile_id=profile.id,
                    run_id=exc.run_id,
                    failure_code=exc.failure_code,
                )
                if "flood" in exc.failure_code.casefold():
                    raise
        return tuple(executions)

    async def _run_profile_web(
        self,
        requested_at: datetime,
        bucket: int,
    ) -> tuple[ProfileDiscoveryExecution, ...]:
        backends = build_web_search_backends(self._config)
        if not backends:
            log_event(
                self._logger,
                logging.INFO,
                "profile.discovery.web_unavailable",
                reason="WEB_PRIMARY_SEARCH_URL_and_SEARXNG_URL_not_configured",
            )
            return ()
        async with self._database.connect() as connection:
            profiles = await SearchProfileRepository().list_active(connection)
        executions: list[ProfileDiscoveryExecution] = []
        for profile in profiles:
            try:
                executions.append(
                    await self._profile_discovery.discover_profile(
                        profile,
                        requested_at=requested_at,
                        run_key=(
                            f"profile-web-discovery:{profile.id}:"
                            f"{profile.revision}:{bucket}"
                        ),
                        backend=backends[0] if len(backends) == 1 else backends,
                        searxng_url=None,
                        max_candidates=self._config.source_discovery_max_candidates,
                    )
                )
            except DiscoveryExecutionError as exc:
                log_event(
                    self._logger,
                    logging.WARNING,
                    "profile.discovery.web_failed",
                    profile_id=profile.id,
                    run_id=exc.run_id,
                    failure_code=exc.failure_code,
                )
        return tuple(executions)

    async def _run_graph(
        self,
        collector_account_id: int,
        seed_ids: tuple[int, ...],
        requested_at: datetime,
        bucket: int,
    ) -> DiscoveryExecution | None:
        if not self._config.source_graph_discovery_enabled:
            log_event(
                self._logger,
                logging.INFO,
                "source.discovery.graph_disabled",
                reason="SOURCE_GRAPH_DISCOVERY_ENABLED=false",
            )
            return None
        if not seed_ids:
            log_event(
                self._logger,
                logging.INFO,
                "source.discovery.graph_no_seeds",
                reason="no_approved_accessible_seeds",
            )
            return None
        seed_resolver = PostgresSourceGraphSeedResolver(
            self._database,
            collector_account_id=collector_account_id,
        )
        provider = SourceGraphDiscoveryProvider(
            seed_resolver,
            TelethonSourceGraphBackend(
                self._client,
                governor=self._governor,
                max_message_limit=self._config.telegram_max_history_messages_per_pass,
                known_source_identities=await seed_resolver.list_known_source_identities(),
                entity_resolution_budget=(
                    self._config.telegram_max_entity_resolves_per_graph_pass
                ),
            ),
            message_limit_per_seed=min(
                self._config.source_discovery_message_limit_per_seed,
                self._config.telegram_max_history_messages_per_pass,
            ),
            max_candidates=self._config.source_discovery_max_candidates,
            max_observations=self._config.source_discovery_max_observations,
        )
        try:
            return await self._runner.run(
                provider,
                run_key=f"autonomous-source-graph:{bucket}",
                request=DiscoveryRequest(
                    parameters={"trigger": "runtime_cycle"},
                    requested_at=requested_at,
                    seed_source_ids=seed_ids,
                ),
            )
        except DiscoveryExecutionError as exc:
            log_event(
                self._logger,
                logging.WARNING,
                "source.discovery.graph_failed",
                run_id=exc.run_id,
                failure_code=exc.failure_code,
            )
            return None

    async def _run_web(
        self,
        requested_at: datetime,
        bucket: int,
    ) -> DiscoveryExecution | None:
        backends = build_web_search_backends(self._config)
        if not backends:
            log_event(
                self._logger,
                logging.INFO,
                "source.discovery.web_unavailable",
                reason="WEB_PRIMARY_SEARCH_URL_and_SEARXNG_URL_not_configured",
            )
            return None
        try:
            return await self._runner.run(
                WebDiscoveryProvider(
                    backends,
                    governor=self._web_governor,
                ),
                run_key=f"autonomous-web-discovery:{bucket}",
                request=DiscoveryRequest(
                    parameters={"trigger": "runtime_cycle"},
                    requested_at=requested_at,
                ),
            )
        except DiscoveryExecutionError as exc:
            log_event(
                self._logger,
                logging.WARNING,
                "source.discovery.web_failed",
                run_id=exc.run_id,
                failure_code=exc.failure_code,
            )
            return None

    def _build_audit_pipeline(self) -> SourceAuditPipeline | None:
        if not self._config.source_audit_enabled:
            return None
        key = self._config.openai_api_key
        if key is None or not key.get_secret_value().strip():
            return None
        return SourceAuditPipeline(
            self._database,
            SourceAuditSampler(
                TelethonSourceAuditHistoryReader(
                    self._client,
                    governor=self._governor,
                    max_messages_per_pass=self._config.source_audit_sample_size,
                ),
                policy=SourceAuditPolicy(
                    sample_size=self._config.source_audit_sample_size,
                    minimum_evidence_messages=30,
                    distribution_buckets=min(
                        2,
                        self._config.source_audit_sample_size,
                    ),
                ),
            ),
            OpenAISourceAuditProvider(
                api_key=key.get_secret_value(),
                model=self._config.source_audit_model,
                temperature=self._config.source_audit_temperature,
                timeout_seconds=self._config.source_audit_timeout_seconds,
            ),
            lifecycle_actor_kind="system",
            lifecycle_actor_id="autonomous_source_discovery",
        )

    async def _audit_candidate(
        self,
        source_id: int,
        *,
        collector_account_id: int,
        pipeline: SourceAuditPipeline,
        audited_at: datetime,
    ) -> SourceAuditRunResult | None:
        repository = SourceRepository()
        async with self._database.connect() as connection:
            source = await repository.get(connection, source_id)
            if source.lifecycle_status not in {
                SourceStatus.CANDIDATE,
                SourceStatus.NEEDS_REVIEW,
            }:
                return None
            if not await repository.is_accessible_to_collector(
                connection,
                source_id=source_id,
                collector_account_id=collector_account_id,
                platform="telegram",
            ):
                log_event(
                    self._logger,
                    logging.INFO,
                    "source.audit.inaccessible",
                    source_id=source_id,
                )
                return None
            chat_peer = await TelegramChatDiscoveryRepository().get_peer_for_source(
                connection,
                source_id=source_id,
            )
        lookup = _source_audit_lookup(source, chat_peer)
        try:
            return await pipeline.run(
                SourceAuditTarget(
                    source_id=source_id,
                    platform=source.platform,
                    lookup=lookup,
                ),
                audited_at=audited_at,
            )
        except (RPCError, ValueError, TypeError, KeyError, PermissionError) as exc:
            log_event(
                self._logger,
                logging.INFO,
                "source.audit.access_check_failed",
                source_id=source_id,
                error=exc,
            )
        except Exception as exc:
            log_event(
                self._logger,
                logging.WARNING,
                "source.audit.failed",
                source_id=source_id,
                error=exc,
            )
        return None

    async def _pending_candidate_ids(self) -> tuple[int, ...]:
        repository = SourceRepository()
        values: list[int] = []
        async with self._database.connect() as connection:
            for status in (SourceStatus.CANDIDATE, SourceStatus.NEEDS_REVIEW):
                records = await repository.list_sources(
                    connection,
                    status=status,
                    platform="telegram",
                    limit=self._config.source_discovery_max_candidates,
                )
                values.extend(record.id for record in records if record.id not in values)
        return tuple(values)


def _source_audit_lookup(source, chat_peer):
    """Prefer persisted peer metadata for private Chat Discovery sources."""

    if (
        source.platform == "telegram"
        and source.access_type == "private"
        and chat_peer is not None
    ):
        return input_entity_for_peer(chat_peer)
    return source.handle or source.canonical_url or source.external_id


def _candidate_source_ids(
    graph: DiscoveryExecution | None,
    web: DiscoveryExecution | None,
    profile_telegram: tuple[ProfileDiscoveryExecution, ...] = (),
) -> tuple[int, ...]:
    values: list[int] = []
    for execution in (graph, web):
        if execution is None:
            continue
        for result in execution.results:
            if result.source_id not in values:
                values.append(result.source_id)
    for profile_execution in profile_telegram:
        for result in profile_execution.execution.results:
            if result.source_id not in values:
                values.append(result.source_id)
    return tuple(values)


def _merge_candidate_source_ids(
    primary: tuple[int, ...],
    pending: tuple[int, ...],
) -> tuple[int, ...]:
    values = list(primary)
    values.extend(source_id for source_id in pending if source_id not in values)
    return tuple(values)


def _aware_now(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("discovery clock must return a timezone-aware datetime")
    return value
