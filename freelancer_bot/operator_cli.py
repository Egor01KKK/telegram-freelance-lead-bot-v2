"""Bounded, read-safe operator commands for the first controlled live test.

The commands in this module are adapters around the existing discovery, audit,
source-lifecycle, matching, delivery and product-metrics services. They never
write PostgreSQL directly and intentionally omit message bodies, AI payloads,
tokens and session material from their output.
"""

from __future__ import annotations

import argparse
import asyncio
from collections import Counter, defaultdict
from collections.abc import Mapping, Sequence
from dataclasses import asdict
from datetime import datetime, timezone
from decimal import Decimal
import hashlib
import json
import sys
from types import SimpleNamespace
from typing import Any
from uuid import UUID

import sqlalchemy as sa
from telethon import TelegramClient
from telethon.errors import FloodWaitError

from .config import ConfigurationError, RuntimeConfig, RuntimeMode
from .discovery import DiscoveryRequest
from .discovery_runner import DiscoveryExecution, DiscoveryRunner
from .persistence.database import Database
from .persistence.deliveries import DeliveryStatus, PersonalizedDeliveryRepository
from .persistence.discovery import DiscoveryRunRepository
from .persistence.matches import MatchTraceRepository
from .persistence.opportunities import CanonicalOpportunityRepository
from .persistence.product_metrics import ProductMetricsRepository
from .persistence.raw_messages import RawMessageRepository
from .persistence.schema import source_discovery_evidence, source_reference_aliases
from .persistence.source_audits import SourceAuditRepository
from .persistence.source_repository import SourceRepository, SourceStatus
from .persistence.search_profiles import SearchProfileRepository
from .source_audit import OpenAISourceAuditProvider, SourceAuditPipeline
from .source_audit_sampler import (
    SourceAuditPolicy,
    SourceAuditTarget,
    SourceAuditSampler,
)
from .source_audit_sampler import TelethonSourceAuditHistoryReader
from .source_graph_discovery import (
    PostgresSourceGraphSeedResolver,
    SourceGraphDiscoveryProvider,
    TelethonSourceGraphBackend,
)
from .telegram_collector import ApprovedTelegramSourceAdapter
from .telegram_request_governor import TelegramRequestGovernor
from .persistence.telegram_operation_state import TelegramCollectorOperationRepository
from .profile_discovery import (
    ProfileDiscoveryExecution,
    ProfileDiscoveryIntent,
    ProfileSourceCoverage,
    ProfileDiscoveryService,
    _lineages_for_intent,
    build_evaluation_intent,
    evaluation_profile_specs,
    evaluate_source_relevance,
    evaluate_source_relevance_legacy,
    coverage_from_evaluations,
    web_strategy_for_intent,
)
from .telegram_session import TelegramSessionFileLock
from .telegram_references import InvalidTelegramReference, normalize_telegram_reference
from .telegram_source_validation import TelegramSourceValidationService
from .web_discovery import (
    SearxngSearchBackend,
    WebDiscoveryGovernor,
    WebDiscoveryProvider,
    collapse_near_duplicate_queries,
)
from .source_bootstrap import GlobalSourceLibraryService
from .collector_only import CollectorOnlyRuntime
from .telegram_chat_discovery import (
    SCREEN_JOB_TYPE,
    SEARCH_JOB_TYPE,
    TelegramChatDiscoveryService,
)
from .persistence.telegram_chat_discovery import TelegramChatDiscoveryRepository
from .global_source_library import (
    CandidatePriority,
    prioritize_candidate,
    run_offline_scale_test,
    source_graph_campaign_spec,
)
from .persistence.discovery_campaigns import DiscoveryCampaignRepository


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m freelancer_bot.operator_cli",
        description="Safe PostgreSQL-backed operator commands for the V2 live test.",
    )
    commands = parser.add_subparsers(dest="command", required=True)

    collectors = commands.add_parser(
        "collectors",
        help="inspect durable Telegram collector request-governor state",
    )
    collector_commands = collectors.add_subparsers(
        dest="collectors_command",
        required=True,
    )
    collector_status = collector_commands.add_parser("status")
    _add_limit(collector_status)

    telegram_discovery = commands.add_parser(
        "telegram-discovery",
        help="run and inspect bounded Telegram chat discovery",
    )
    telegram_discovery_commands = telegram_discovery.add_subparsers(
        dest="telegram_discovery_command",
        required=True,
    )
    telegram_discovery_commands.add_parser(
        "status",
        help="show topic, dedup, screen, job and governor-safe counters",
    )
    telegram_topics = telegram_discovery_commands.add_parser(
        "topics",
        help="list normalized global discovery topics",
    )
    _add_limit(telegram_topics)
    telegram_run = telegram_discovery_commands.add_parser(
        "run",
        help="enqueue and drain a bounded due-topic search batch",
    )
    telegram_run.add_argument("--max-topics", type=_positive_int, default=5)
    telegram_run.add_argument("--collector-account-id", type=_positive_int)
    telegram_screen = telegram_discovery_commands.add_parser(
        "screen-pending",
        help="enqueue and drain a bounded new-chat screen batch",
    )
    _add_limit(telegram_screen)
    telegram_screen.add_argument("--collector-account-id", type=_positive_int)

    sources = commands.add_parser("sources", help="inspect and transition sources")
    source_commands = sources.add_subparsers(dest="sources_command", required=True)
    source_list = source_commands.add_parser("list")
    source_list.add_argument("--status", choices=[status.value for status in SourceStatus])
    source_list.add_argument("--platform")
    _add_limit(source_list)
    source_show = source_commands.add_parser("show")
    source_show.add_argument("--source-id", type=_positive_int, required=True)
    source_audits = source_commands.add_parser("audits")
    source_audits.add_argument("--source-id", type=_positive_int)
    source_audits.add_argument(
        "--decision",
        choices=("approved", "rejected", "needs_review"),
    )
    _add_limit(source_audits)
    transition = source_commands.add_parser(
        "transition",
        help="record an explicit operator approve/reject/pause decision",
    )
    transition.add_argument("--source-id", type=_positive_int, required=True)
    transition.add_argument(
        "--target",
        choices=("approved", "rejected", "paused"),
        required=True,
    )
    transition.add_argument("--actor", required=True)
    transition.add_argument("--reason", required=True)

    discovery = commands.add_parser("discovery", help="run and inspect discovery")
    discovery_commands = discovery.add_subparsers(
        dest="discovery_command", required=True
    )
    web = discovery_commands.add_parser("web", help="run Web Discovery through SearXNG")
    web.add_argument("--searxng-url", required=True)
    web.add_argument("--run-key", required=True)
    web.add_argument("--parameters-json", default="{}")
    web.add_argument("--timeout-seconds", type=float, default=15.0)
    graph = discovery_commands.add_parser(
        "graph",
        help="run Telegram Source Graph Discovery from approved accessible seeds",
    )
    graph.add_argument("--run-key", required=True)
    graph.add_argument("--seed-source-id", type=_positive_int, action="append", required=True)
    graph.add_argument("--collector-account-id", type=_positive_int)
    graph.add_argument("--parameters-json", default="{}")
    graph.add_argument("--message-limit-per-seed", type=_positive_int, default=100)
    graph.add_argument("--max-candidates", type=_positive_int, default=100)
    graph.add_argument("--max-observations", type=_positive_int, default=1000)
    discovery_runs = discovery_commands.add_parser("runs")
    discovery_runs.add_argument("--provider")
    discovery_runs.add_argument("--status", choices=("running", "completed", "failed"))
    _add_limit(discovery_runs)
    discovery_results = discovery_commands.add_parser("results")
    discovery_results.add_argument("--run-id", type=_uuid_arg, required=True)

    audit = commands.add_parser("audit", help="run and inspect source audits")
    audit_commands = audit.add_subparsers(dest="audit_command", required=True)
    for name in ("run", "re-audit"):
        audit_run = audit_commands.add_parser(name)
        audit_run.add_argument("--source-id", type=_positive_int, required=True)
    audit_list = audit_commands.add_parser("list")
    audit_list.add_argument("--source-id", type=_positive_int)
    audit_list.add_argument(
        "--decision",
        choices=("approved", "rejected", "needs_review"),
    )
    _add_limit(audit_list)

    match = commands.add_parser("match", help="inspect persisted matching evidence")
    match_commands = match.add_subparsers(dest="match_command", required=True)
    match_runs = match_commands.add_parser("runs")
    _add_limit(match_runs)
    match_traces = match_commands.add_parser("traces")
    match_traces.add_argument("--run-id", type=_uuid_arg)
    match_traces.add_argument("--opportunity-id", type=_uuid_arg)
    _add_limit(match_traces)

    delivery = commands.add_parser("delivery", help="inspect persisted deliveries")
    delivery_commands = delivery.add_subparsers(dest="delivery_command", required=True)
    delivery_list = delivery_commands.add_parser("list")
    delivery_list.add_argument(
        "--status",
        choices=[status.value for status in DeliveryStatus],
    )
    _add_limit(delivery_list)

    observe = commands.add_parser(
        "observe",
        help="safe, body-free pipeline and product-metrics observations",
    )
    observe_commands = observe.add_subparsers(dest="observe_command", required=True)
    raw = observe_commands.add_parser("raw")
    _add_limit(raw)
    opportunities = observe_commands.add_parser("opportunities")
    _add_limit(opportunities)
    metrics = observe_commands.add_parser("metrics")
    metrics.add_argument("--since", type=_timestamp_arg, required=True)
    metrics.add_argument("--until", type=_timestamp_arg, required=True)

    profile_discovery = commands.add_parser(
        "profile-discovery",
        help="run and inspect profile-driven Web Discovery without Telegram access",
    )
    profile_discovery_commands = profile_discovery.add_subparsers(
        dest="profile_discovery_command",
        required=True,
    )
    evaluate_profiles = profile_discovery_commands.add_parser(
        "evaluate",
        help="evaluate ten bounded non-user discovery profiles through SearXNG",
    )
    evaluate_profiles.add_argument("--searxng-url")
    evaluate_profiles.add_argument("--results-per-query", type=_positive_int, default=10)
    evaluate_profiles.add_argument("--max-candidates", type=_positive_int, default=100)
    evaluate_profiles.add_argument("--run-prefix", default="profile-evaluation-web-v1")
    evaluate_profiles.add_argument(
        "--profile-key",
        action="append",
        dest="profile_keys",
        help="limit governed evaluation to one or more evaluation profile keys",
    )
    canary = profile_discovery_commands.add_parser(
        "canary",
        help="run a six-query governed Python/Telegram Web canary",
    )
    canary.add_argument("--searxng-url")
    canary.add_argument("--run-key")
    run_profile = profile_discovery_commands.add_parser(
        "run",
        help="run profile-driven Web Discovery for one active confirmed profile",
    )
    run_profile.add_argument("--profile-id", type=_uuid_arg, required=True)
    run_profile.add_argument("--searxng-url")
    run_profile.add_argument("--run-key")
    run_profile.add_argument("--results-per-query", type=_positive_int, default=10)
    run_profile.add_argument("--max-candidates", type=_positive_int, default=100)
    coverage = profile_discovery_commands.add_parser(
        "coverage",
        help="project approved-source coverage for one active confirmed profile",
    )
    coverage.add_argument("--profile-id", type=_uuid_arg, required=True)
    intents = profile_discovery_commands.add_parser("intents")
    intents.add_argument("--profile-id", type=_uuid_arg)
    _add_limit(intents)
    profile_discovery_commands.add_parser(
        "calibrate",
        help="offline relevance/query calibration; never calls Web or Telegram",
    )

    bootstrap = commands.add_parser(
        "source-bootstrap",
        help="plan, inspect and run durable Global Source Library campaigns",
    )
    bootstrap_commands = bootstrap.add_subparsers(dest="source_bootstrap_command", required=True)
    bootstrap_start = bootstrap_commands.add_parser("start")
    bootstrap_start.add_argument("--target-unique-candidates", type=_positive_int, default=1000)
    bootstrap_start.add_argument("--target-validated-sources", type=_positive_int, default=500)
    bootstrap_start.add_argument("--target-approved-sources", type=_positive_int, default=100)
    bootstrap_start.add_argument("--priority", type=int, default=50)
    bootstrap_status = bootstrap_commands.add_parser("status")
    bootstrap_status.add_argument("--status", choices=("planned", "running", "paused", "completed", "failed"))
    _add_limit(bootstrap_status)
    for name in ("pause", "resume"):
        item = bootstrap_commands.add_parser(name)
        item.add_argument("--campaign-key", required=True)
        if name == "pause":
            item.add_argument("--reason", required=True)
    bootstrap_run = bootstrap_commands.add_parser("run")
    bootstrap_run.add_argument("--campaign-key", required=True)
    bootstrap_run.add_argument("--max-queries", type=_positive_int, default=20)
    bootstrap_run.add_argument("--results-per-query", type=_positive_int, default=10)
    bootstrap_run.add_argument("--max-candidates", type=_positive_int, default=100)
    bootstrap_run.add_argument("--max-page-fetches", type=_positive_int, default=100)

    library = commands.add_parser(
        "source-library",
        help="safe Global Source Library statistics and offline infrastructure checks",
    )
    library_commands = library.add_subparsers(dest="source_library_command", required=True)
    library_commands.add_parser("stats")
    library_coverage = library_commands.add_parser("coverage")
    library_coverage.add_argument("--profile-id", type=_uuid_arg, required=True)
    library_validate = library_commands.add_parser(
        "validate",
        help="resolve one Telegram candidate through the configured collector governor",
    )
    library_validate.add_argument("--source-id", type=_positive_int, required=True)
    library_validate.add_argument("--collector-account-id", type=_positive_int)
    library_validate.add_argument("--actor", default="operator")
    library_canary = library_commands.add_parser(
        "validate-candidates",
        help="run one bounded priority-balanced Telegram candidate validation/audit canary",
    )
    library_canary.add_argument("--collector-account-id", type=_positive_int, required=True)
    library_canary.add_argument("--limit", type=_positive_int, default=30)
    library_canary.add_argument("--audit-limit", type=_nonnegative_int, default=10)
    library_canary.add_argument("--actor", default="operator")
    library_commands.add_parser(
        "query-dedup",
        help="report durable bootstrap query exact/near/executable counts",
    )
    library_commands.add_parser(
        "backfill-legacy-evidence",
        help="reconcile immutable pre-v1 discovery lineage into v1 evidence",
    )
    library_commands.add_parser(
        "rerank-candidates",
        help="offline rerank of candidate sources; never calls Telegram or Web",
    )
    library_commands.add_parser("offline-scale")

    return parser


async def _dispatch(args: argparse.Namespace) -> None:
    if args.command == "sources":
        await _sources_command(args)
    elif args.command == "collectors":
        await _collectors_command(args)
    elif args.command == "telegram-discovery":
        await _telegram_discovery_command(args)
    elif args.command == "discovery":
        await _discovery_command(args)
    elif args.command == "audit":
        await _audit_command(args)
    elif args.command == "match":
        await _match_command(args)
    elif args.command == "delivery":
        await _delivery_command(args)
    elif args.command == "observe":
        await _observe_command(args)
    elif args.command == "profile-discovery":
        await _profile_discovery_command(args)
    elif args.command == "source-bootstrap":
        await _source_bootstrap_command(args)
    elif args.command == "source-library":
        await _source_library_command(args)
    else:  # pragma: no cover - argparse enforces the command set
        raise ValueError(f"unsupported command: {args.command}")


async def _source_bootstrap_command(args: argparse.Namespace) -> None:
    config = _database_config()
    database = Database(config.postgresql_url())
    service = GlobalSourceLibraryService(database, config)
    try:
        if args.source_bootstrap_command == "start":
            result = await service.start_bootstrap(
                target_unique_candidates=args.target_unique_candidates,
                target_validated_sources=args.target_validated_sources,
                target_approved_sources=args.target_approved_sources,
                priority=args.priority,
            )
            _emit(
                {
                    "campaigns_created_or_reused": len(result.campaigns),
                    "campaign_keys": [item.campaign_key for item in result.campaigns],
                    "queries_created": result.queries_created,
                    "durable_plan_jobs": result.jobs_created,
                    "web_readiness": result.web_readiness,
                }
            )
            return
        if args.source_bootstrap_command == "status":
            async with database.connect() as connection:
                campaigns = await DiscoveryCampaignRepository().list_campaigns(
                    connection,
                    status=args.status,
                    limit=args.limit,
                )
            _emit({"count": len(campaigns), "campaigns": [_campaign_payload(item) for item in campaigns]})
            return
        if args.source_bootstrap_command in {"pause", "resume"}:
            async with database.connect() as connection:
                campaign = await DiscoveryCampaignRepository().get_by_key(connection, args.campaign_key)
            if campaign is None:
                raise ValueError("discovery campaign not found")
            async with database.transaction() as connection:
                updated = await DiscoveryCampaignRepository().set_status(
                    connection,
                    campaign_id=campaign.id,
                    status="paused" if args.source_bootstrap_command == "pause" else "planned",
                    reason=getattr(args, "reason", None),
                )
                if args.source_bootstrap_command == "resume":
                    await DiscoveryCampaignRepository().enqueue_campaign_plan(
                        connection,
                        campaign=updated,
                        batch_key=str(updated.progress.get("completed_query_count", 0)),
                    )
            _emit({"campaign": _campaign_payload(updated)})
            return
        if args.source_bootstrap_command == "run":
            execution = await service.run_campaign(
                args.campaign_key,
                max_queries=args.max_queries,
                results_per_query=args.results_per_query,
                max_candidates=args.max_candidates,
                max_page_fetches=args.max_page_fetches,
            )
            _emit(_discovery_execution_payload(execution))
            return
        raise ValueError(f"unsupported source-bootstrap command: {args.source_bootstrap_command}")
    finally:
        await database.close()


async def _source_library_command(args: argparse.Namespace) -> None:
    if args.source_library_command == "offline-scale":
        _emit(run_offline_scale_test())
        return
    config = (
        _sources_config()
        if args.source_library_command in {"validate", "validate-candidates"}
        else _database_config()
    )
    database = Database(config.postgresql_url())
    try:
        service = GlobalSourceLibraryService(database, config)
        if args.source_library_command == "stats":
            stats = await service.stats()
            _emit(
                {
                    "campaigns": dict(stats.campaigns),
                    "queries": dict(stats.queries),
                    "validation_states": dict(stats.validation_states),
                    "source_lifecycle": dict(stats.source_lifecycle),
                    "coverage": {
                        dimension: dict(values)
                        for dimension, values in stats.coverage.items()
                    },
                    "monitoring": dict(stats.monitoring),
                    "provider_health": {
                        backend: dict(values)
                        for backend, values in stats.provider_health.items()
                    },
                    "cost_summary": {
                        key: dict(values)
                        for key, values in stats.cost_summary.items()
                    },
                }
            )
            return
        if args.source_library_command == "coverage":
            async with database.connect() as connection:
                profile = await SearchProfileRepository().get(connection, args.profile_id)
            coverage = await ProfileDiscoveryService(database).coverage_for_profile(profile)
            _emit(asdict(coverage))
            return
        if args.source_library_command == "validate":
            if config.api_id is None:
                raise ConfigurationError(
                    "TELEGRAM_API_ID/API_ID is required for Telegram validation"
                )
            config.user_session_path.parent.mkdir(parents=True, exist_ok=True)
            with TelegramSessionFileLock(config.user_session_path, role="source_validate"):
                client = TelegramClient(
                    str(config.user_session_path),
                    config.api_id,
                    _secret(config.api_hash, "TELEGRAM_API_HASH/API_HASH"),
                    flood_sleep_threshold=0,
                )
                try:
                    await client.start()
                    snapshot = await ApprovedTelegramSourceAdapter(database).list_for_session(client)
                    if (
                        args.collector_account_id is not None
                        and args.collector_account_id != snapshot.collector_account.id
                    ):
                        raise ConfigurationError(
                            "--collector-account-id does not match the authenticated Telegram session"
                        )
                    result = await TelegramSourceValidationService(database).validate(
                        source_id=args.source_id,
                        collector_account_id=snapshot.collector_account.id,
                        client=client,
                        governor=TelegramRequestGovernor(
                            database,
                            snapshot.collector_account.id,
                            config,
                        ),
                        checked_by=args.actor,
                    )
                    _emit(asdict(result))
                finally:
                    await client.disconnect()
            return
        if args.source_library_command == "validate-candidates":
            result = await _validate_candidate_canary(
                database,
                config,
                collector_account_id=args.collector_account_id,
                limit=args.limit,
                audit_limit=args.audit_limit,
                actor=args.actor,
            )
            _emit(result)
            return
        if args.source_library_command == "query-dedup":
            _emit(await _query_dedup_report(database))
            return
        if args.source_library_command == "backfill-legacy-evidence":
            async with database.transaction() as connection:
                result = await service.repository.backfill_legacy_evidence(connection)
            _emit(asdict(result))
            return
        if args.source_library_command == "rerank-candidates":
            _emit(await _rerank_candidate_report(database))
            return
        raise ValueError(f"unsupported source-library command: {args.source_library_command}")
    finally:
        await database.close()


async def _validate_candidate_canary(
    database: Database,
    config: RuntimeConfig,
    *,
    collector_account_id: int,
    limit: int,
    audit_limit: int,
    actor: str,
) -> dict[str, object]:
    if not 1 <= limit <= 30:
        raise ValueError("candidate canary limit must be between 1 and 30")
    if not 0 <= audit_limit <= 10:
        raise ValueError("candidate canary audit limit must be between 0 and 10")
    sources = SourceRepository()
    async with database.connect() as connection:
        pool = await sources.list_sources(
            connection,
            status=SourceStatus.CANDIDATE,
            platform="telegram",
            limit=1000,
        )
        source_ids = [source.id for source in pool]
        evidence_rows = (
            await connection.execute(
                sa.select(source_discovery_evidence).where(
                    source_discovery_evidence.c.source_id.in_(source_ids)
                )
            )
        ).mappings().all() if source_ids else []
        alias_rows = (
            await connection.execute(
                sa.select(source_reference_aliases).where(
                    source_reference_aliases.c.source_id.in_(source_ids)
                )
            )
        ).mappings().all() if source_ids else []
        evidence_by_source: dict[int, list[Mapping[str, Any]]] = defaultdict(list)
        aliases_by_source: dict[int, list[Mapping[str, Any]]] = defaultdict(list)
        for row in evidence_rows:
            evidence_by_source[int(row["source_id"])].append(row)
        for row in alias_rows:
            aliases_by_source[int(row["source_id"])].append(row)
        candidates: list[tuple[object, CandidatePriority]] = []
        for source in pool:
            lineage = await sources.list_lineage(connection, source.id)
            context = _candidate_priority_context(
                lineage,
                source=source,
                evidence=evidence_by_source.get(source.id, ()),
                aliases=aliases_by_source.get(source.id, ()),
            )
            candidates.append((source, prioritize_candidate(context)))
        approved_before = len(
            await sources.list_sources(
                connection,
                status=SourceStatus.APPROVED,
                platform="telegram",
                limit=1000,
            )
        )

    distribution = Counter(priority.value for _, priority in candidates)
    selected = _select_candidate_canary(candidates, limit=limit)
    result: dict[str, object] = {
        "pool_size": len(candidates),
        "priority_distribution": dict(sorted(distribution.items())),
        "selected": len(selected),
        "selected_priority_distribution": dict(
            sorted(Counter(priority.value for _, priority in selected).items())
        ),
        "validation": {
            "attempted": 0,
                "states": {},
                "failure_classes": {},
                "post_validation_failure_classes": {},
            "access_mode": {},
            "stopped_on_floodwait": False,
        },
        "audit": {
            "attempted": 0,
            "decisions": {},
            "sampled_messages": 0,
            "buyer_opportunities": 0,
            "buyer_intent": 0,
            "seller_self_promotion": 0,
            "ads_spam": 0,
            "duplicate": 0,
            "reason_codes": {},
            "vacancy_and_recommendation_counts": "not_separately_measured_by_source_audit.v1",
            "stopped_on_floodwait": False,
            "skipped_reason": None,
        },
        "approved_total_before": approved_before,
        "approved_from_canary": 0,
        "approved_total_after": approved_before,
    }
    if not selected:
        return result
    if config.api_id is None:
        raise ConfigurationError("TELEGRAM_API_ID/API_ID is required for candidate canary")

    config.user_session_path.parent.mkdir(parents=True, exist_ok=True)
    with TelegramSessionFileLock(config.user_session_path, role="source_canary"):
        client = TelegramClient(
            str(config.user_session_path),
            config.api_id,
            _secret(config.api_hash, "TELEGRAM_API_HASH/API_HASH"),
            flood_sleep_threshold=0,
        )
        try:
            await client.start()
            snapshot = await ApprovedTelegramSourceAdapter(database).list_for_session(client)
            if snapshot.collector_account.id != collector_account_id:
                raise ConfigurationError(
                    "--collector-account-id does not match the authenticated Telegram session"
                )
            governor = TelegramRequestGovernor(database, collector_account_id, config)
            validation_service = TelegramSourceValidationService(database)
            accessible: list[object] = []
            for source, priority in selected:
                validation = result["validation"]
                if not isinstance(validation, dict):
                    raise RuntimeError("candidate canary result shape is invalid")
                validation["attempted"] = int(validation["attempted"]) + 1
                try:
                    outcome = await validation_service.validate(
                        source_id=source.id,
                        collector_account_id=collector_account_id,
                        client=client,
                        governor=governor,
                        checked_by=actor,
                    )
                except FloodWaitError:
                    validation["stopped_on_floodwait"] = True
                    break
                except Exception as exc:
                    _increment_count(
                        validation["failure_classes"],
                        _safe_failure_class_name(exc),
                    )
                    continue
                try:
                    state = str(outcome.state)
                    access_mode = str(outcome.access_mode)
                    _increment_count(validation["states"], state)
                    _increment_count(validation["access_mode"], access_mode)
                    if state == "accessible":
                        accessible.append((source, priority, outcome))
                except Exception as exc:
                    _increment_count(
                        validation["post_validation_failure_classes"],
                        _safe_failure_class_name(exc),
                    )

            audit = result["audit"]
            if not isinstance(audit, dict):
                raise RuntimeError("candidate canary audit result shape is invalid")
            if audit_limit == 0:
                audit["skipped_reason"] = "audit_limit_zero"
            elif config.openai_api_key is None or not config.openai_api_key.get_secret_value().strip():
                audit["skipped_reason"] = "OPENAI_API_KEY_NOT_CONFIGURED"
            else:
                pipeline = SourceAuditPipeline(
                    database,
                    SourceAuditSampler(
                        TelethonSourceAuditHistoryReader(
                            client,
                            governor=governor,
                            max_messages_per_pass=config.source_audit_sample_size,
                        ),
                        policy=SourceAuditPolicy(
                            sample_size=config.source_audit_sample_size,
                            minimum_evidence_messages=30,
                            distribution_buckets=min(
                                2,
                                config.source_audit_sample_size,
                            ),
                        ),
                    ),
                    OpenAISourceAuditProvider(
                        api_key=config.openai_api_key.get_secret_value(),
                        model=config.source_audit_model,
                        temperature=config.source_audit_temperature,
                        timeout_seconds=config.source_audit_timeout_seconds,
                    ),
                )
                for source, _priority, _validation in accessible[:audit_limit]:
                    audit["attempted"] = int(audit["attempted"]) + 1
                    try:
                        target = SourceAuditTarget(
                            source_id=source.id,
                            platform=source.platform,
                            lookup=source.handle or source.canonical_url or source.external_id,
                        )
                        outcome = await pipeline.run(
                            target,
                            audited_at=datetime.now(timezone.utc),
                        )
                    except FloodWaitError:
                        audit["stopped_on_floodwait"] = True
                        break
                    except Exception as exc:
                        _increment_count(audit["reason_codes"], _safe_failure_class_name(exc))
                        continue
                    _increment_count(audit["decisions"], outcome.audit.decision)
                    audit["sampled_messages"] = int(audit["sampled_messages"]) + outcome.audit.sampled_message_count
                    for field, key in (
                        ("commercial_opportunity_count", "buyer_opportunities"),
                        ("buyer_intent_count", "buyer_intent"),
                        ("seller_promotion_count", "seller_self_promotion"),
                        ("ads_spam_count", "ads_spam"),
                        ("duplicate_count", "duplicate"),
                    ):
                        audit[key] = int(audit[key]) + int(getattr(outcome.audit, field))
                    for reason_code in outcome.audit.reason_codes:
                        _increment_count(audit["reason_codes"], reason_code)
                    async with database.transaction() as connection:
                        await DiscoveryCampaignRepository().upsert_validation(
                            connection,
                            source_id=source.id,
                            collector_account_id=collector_account_id,
                            state=outcome.audit.decision,
                            access_mode=(
                                "public_readable" if source.access_type == "public" else "joined"
                            ),
                            checked_at=outcome.audit.audited_at,
                            checked_by=actor,
                        )
        finally:
            await client.disconnect()

    async with database.connect() as connection:
        approved_after = len(
            await sources.list_sources(
                connection,
                status=SourceStatus.APPROVED,
                platform="telegram",
                limit=1000,
            )
        )
    result["approved_total_after"] = approved_after
    result["approved_from_canary"] = max(0, approved_after - approved_before)
    return result


async def _rerank_candidate_report(database: Database) -> dict[str, object]:
    sources = SourceRepository()
    async with database.connect() as connection:
        pool = await sources.list_sources(
            connection,
            status=SourceStatus.CANDIDATE,
            platform="telegram",
            limit=1000,
        )
        source_ids = [source.id for source in pool]
        evidence_rows = (
            await connection.execute(
                sa.select(source_discovery_evidence).where(
                    source_discovery_evidence.c.source_id.in_(source_ids)
                )
            )
        ).mappings().all() if source_ids else []
        alias_rows = (
            await connection.execute(
                sa.select(source_reference_aliases).where(
                    source_reference_aliases.c.source_id.in_(source_ids)
                )
            )
        ).mappings().all() if source_ids else []
        evidence_by_source: dict[int, list[Mapping[str, Any]]] = defaultdict(list)
        aliases_by_source: dict[int, list[Mapping[str, Any]]] = defaultdict(list)
        for row in evidence_rows:
            evidence_by_source[int(row["source_id"])].append(row)
        for row in alias_rows:
            aliases_by_source[int(row["source_id"])].append(row)
        distributions: Counter[str] = Counter()
        patterns: dict[str, Counter[str]] = defaultdict(Counter)
        for source in pool:
            lineage = await sources.list_lineage(connection, source.id)
            context = _candidate_priority_context(
                lineage,
                source=source,
                evidence=evidence_by_source.get(source.id, ()),
                aliases=aliases_by_source.get(source.id, ()),
            )
            priority = prioritize_candidate(context)
            distributions[priority.value] += 1
            signals = []
            if context.get("normalized_reference"):
                signals.append("normalized_reference")
            if context.get("direct_telegram_result"):
                signals.append("direct_telegram_result")
            if context.get("source_graph_provenance"):
                signals.append("source_graph_provenance")
            if int(context.get("independent_domains", 0) or 0) > 0:
                signals.append("independent_domain")
            if int(context.get("profile_gap_count", 0) or 0) > 0:
                signals.append("profile_gap")
            patterns[priority.value]["+".join(signals) or "none"] += 1
    return {
        "candidate_count": len(pool),
        "priority_distribution": dict(sorted(distributions.items())),
        "evidence_signal_patterns": {
            tier: dict(sorted(values.items()))
            for tier, values in sorted(patterns.items())
        },
    }


def _candidate_priority_context(
    lineage: Sequence[Any],
    *,
    source: Any | None = None,
    evidence: Sequence[Mapping[str, Any]] = (),
    aliases: Sequence[Mapping[str, Any]] = (),
) -> dict[str, object]:
    context: dict[str, object] = {}
    independent_domain_values: set[str] = set()
    profile_gap_count = 0
    for item in lineage:
        values = item.context if isinstance(item.context, Mapping) else {}
        for key in (
            "normalized_reference",
            "telegram_reference",
            "direct_telegram_result",
            "source_graph_provenance",
            "bot_like",
            "contact_like",
            "spam_directory",
        ):
            if values.get(key):
                context[key] = values[key]
        independent_domains = int(values.get("independent_domains", 0) or 0)
        if independent_domains > len(independent_domain_values):
            independent_domain_values.update(
                f"legacy-domain-{index}"
                for index in range(independent_domains - len(independent_domain_values))
            )
        for match in values.get("matches", ()):
            if not isinstance(match, Mapping):
                continue
            domain = _result_domain(match.get("result_url"))
            if domain:
                independent_domain_values.add(domain)
        profile_gap_values = values.get("profile_gap_keys")
        if isinstance(profile_gap_values, (list, tuple, set)):
            profile_gap_count = max(profile_gap_count, len(profile_gap_values))

    if source is not None:
        reference_values = (
            getattr(source, "handle", None),
            getattr(source, "canonical_url", None),
            getattr(source, "external_id", None),
        )
        for value in reference_values:
            if not isinstance(value, str) or not value.strip():
                continue
            try:
                normalize_telegram_reference(value)
            except InvalidTelegramReference:
                continue
            context["normalized_reference"] = True
            context["telegram_reference"] = True
            break
    if aliases:
        context["normalized_reference"] = True
        context["telegram_reference"] = True
    for row in evidence:
        extraction_kind = str(row.get("extraction_kind") or "")
        if extraction_kind == "direct_result":
            context["direct_telegram_result"] = True
        if extraction_kind == "source_graph" and row.get("source_graph_provenance"):
            context["source_graph_provenance"] = row["source_graph_provenance"]
        domain = row.get("result_domain")
        if isinstance(domain, str) and domain.strip():
            independent_domain_values.add(domain.strip().casefold())
        profile_gap_values = row.get("profile_gap_keys")
        if isinstance(profile_gap_values, (list, tuple, set)):
            profile_gap_count = max(profile_gap_count, len(profile_gap_values))

    context["independent_domains"] = len(independent_domain_values)
    context["profile_gap_count"] = profile_gap_count
    return context


def _result_domain(value: object) -> str | None:
    if not isinstance(value, str) or not value.strip():
        return None
    try:
        from urllib.parse import urlsplit

        hostname = urlsplit(value).hostname
    except ValueError:
        return None
    return None if not hostname else hostname.casefold()


def _select_candidate_canary(
    candidates: Sequence[tuple[Any, CandidatePriority]],
    *,
    limit: int,
) -> tuple[tuple[Any, CandidatePriority], ...]:
    buckets = {
        priority: [item for item in candidates if item[1] is priority]
        for priority in CandidatePriority
    }
    high_quota = min(len(buckets[CandidatePriority.HIGH]), max(1, int(limit * 0.60)))
    medium_quota = min(len(buckets[CandidatePriority.MEDIUM]), max(0, int(limit * 0.25)))
    selected = (
        buckets[CandidatePriority.HIGH][:high_quota]
        + buckets[CandidatePriority.MEDIUM][:medium_quota]
    )
    remaining = limit - len(selected)
    exploration = (
        buckets[CandidatePriority.LOW] + buckets[CandidatePriority.INSUFFICIENT]
    )
    selected.extend(exploration[: max(0, remaining)])
    if len(selected) < limit:
        used = {item[0].id for item in selected}
        for item in candidates:
            if item[0].id in used:
                continue
            selected.append(item)
            used.add(item[0].id)
            if len(selected) >= limit:
                break
    return tuple(selected[:limit])


def _increment_count(target: object, key: str) -> None:
    if not isinstance(target, dict):
        return
    target[key] = int(target.get(key, 0)) + 1


def _safe_failure_class_name(exc: Exception) -> str:
    value = type(exc).__name__.casefold()
    return "".join(char if char.isalnum() or char in "_.-" else "_" for char in value)[:64]


async def _query_dedup_report(database: Database) -> dict[str, object]:
    repository = DiscoveryCampaignRepository()
    async with database.connect() as connection:
        rows = await repository.list_query_rows(connection)
    by_campaign: defaultdict[str, list[Mapping[str, object]]] = defaultdict(list)
    for row in rows:
        by_campaign[str(row["campaign_key"])].append(row)
    campaigns: dict[str, object] = {}
    total_planned = 0
    total_exact = 0
    total_near = 0
    for campaign_key, campaign_rows in by_campaign.items():
        normalized_counts = Counter(str(row["normalized_query_key"]) for row in campaign_rows)
        hash_counts = Counter(str(row["query_sha256"]) for row in campaign_rows)
        exact = sum(max(0, count - 1) for count in normalized_counts.values())
        exact = max(exact, sum(max(0, count - 1) for count in hash_counts.values()))
        # The durable plan is populated only after collapse_campaign_queries;
        # no pre-collapse rows are claimable.  A same-family/language text
        # collision would therefore be represented by the exact counter.
        near = 0
        executable = max(0, len(campaign_rows) - exact - near)
        campaigns[campaign_key] = {
            "total_planned": len(campaign_rows),
            "exact_duplicates": exact,
            "near_duplicates": near,
            "executable_unique": executable,
            "status_counts": dict(
                sorted(Counter(str(row["status"]) for row in campaign_rows).items())
            ),
            "dedup_evidence": "durable_queries_are_post_collapse_campaign_claims",
        }
        total_planned += len(campaign_rows)
        total_exact += exact
        total_near += near
    return {
        "campaigns": campaigns,
        "total_planned": total_planned,
        "exact_duplicates": total_exact,
        "near_duplicates": total_near,
        "executable_unique": max(0, total_planned - total_exact - total_near),
        "idempotency": {
            "unique_key": "campaign_id+normalized_query_key",
            "claim_statuses": ["queued", "running", "completed", "failed"],
            "successful_completed_queries_are_not_reclaimed": True,
        },
    }


async def _sources_command(args: argparse.Namespace) -> None:
    config = _database_config()
    database = Database(config.postgresql_url())
    repository = SourceRepository()
    try:
        if args.sources_command == "list":
            async with database.connect() as connection:
                records = await repository.list_sources(
                    connection,
                    status=args.status,
                    platform=args.platform,
                    limit=args.limit,
                )
            _emit({"count": len(records), "sources": [_source_payload(item) for item in records]})
            return
        if args.sources_command == "show":
            async with database.connect() as connection:
                source = await repository.get(connection, args.source_id)
                lineage = await repository.list_lineage(connection, args.source_id)
                events = await repository.list_lifecycle_events(connection, args.source_id)
            _emit(
                {
                    "source": _source_payload(source),
                    "lineage": [_lineage_payload(item) for item in lineage],
                    "lifecycle_events": [_lifecycle_payload(item) for item in events],
                }
            )
            return
        if args.sources_command == "audits":
            async with database.connect() as connection:
                audits = await SourceAuditRepository().list_audits(
                    connection,
                    source_id=args.source_id,
                    decision=args.decision,
                    limit=args.limit,
                )
            _emit({"count": len(audits), "audits": [_audit_payload(item) for item in audits]})
            return
        if args.sources_command == "transition":
            async with database.transaction() as connection:
                source = await repository.override(
                    connection,
                    args.source_id,
                    args.target,
                    operator_id=args.actor,
                    reason=args.reason,
                )
                events = await repository.list_lifecycle_events(connection, args.source_id)
            _emit(
                {
                    "source": _source_payload(source),
                    "recorded_event": _lifecycle_payload(events[-1]),
                }
            )
            return
        raise ValueError(f"unsupported source command: {args.sources_command}")
    finally:
        await database.close()


async def _collectors_command(args: argparse.Namespace) -> None:
    if args.collectors_command != "status":
        raise ValueError(f"unsupported collector command: {args.collectors_command}")
    config = _database_config()
    database = Database(config.postgresql_url())
    now = datetime.now(timezone.utc)
    try:
        async with database.connect() as connection:
            records = await TelegramCollectorOperationRepository().list_status(
                connection,
                now=now,
                limit=args.limit,
            )
        _emit(
            {
                "count": len(records),
                "collectors": [
                    {
                        "collector_account_id": record.collector_account_id,
                        "mode": "telegram_collector",
                        "state": record.status.value,
                        "active_request_category": record.active_request_category,
                        "last_request_at": record.last_request_at,
                        "next_allowed_request_at": record.next_allowed_request_at,
                        "next_allowed_remaining_seconds": _remaining_seconds(
                            record.next_allowed_request_at,
                            now,
                        ),
                        "cooldown_until": record.cooldown_until,
                        "cooldown_remaining_seconds": _remaining_seconds(
                            record.cooldown_until,
                            now,
                        ),
                        "requests_last_5m": record.requests_last_5m,
                        "last_floodwait_detected_at": record.last_floodwait_detected_at,
                        "last_floodwait_seconds": record.last_floodwait_seconds,
                    }
                    for record in records
                ],
            }
        )
    finally:
        await database.close()


async def _telegram_discovery_command(args: argparse.Namespace) -> None:
    command = args.telegram_discovery_command
    if command in {"status", "topics"}:
        config = _database_config()
        database = Database(config.postgresql_url())
        repository = TelegramChatDiscoveryRepository()
        try:
            async with database.transaction() as connection:
                await repository.ensure_base_topics(
                    connection,
                    refresh_interval_seconds=config.telegram_chat_discovery_refresh_interval_seconds,
                )
            async with database.connect() as connection:
                if command == "topics":
                    topics = await repository.list_topics(
                        connection,
                        limit=args.limit,
                        active_only=True,
                    )
                    _emit(
                        {
                            "count": len(topics),
                            "topics": [
                                {
                                    "id": topic.id,
                                    "topic": topic.topic_text,
                                    "language": topic.language,
                                    "kind": topic.topic_kind,
                                    "priority": topic.priority,
                                    "status": topic.search_status,
                                    "search_count": topic.search_count,
                                    "last_searched_at": topic.last_searched_at,
                                    "next_eligible_at": topic.next_eligible_at,
                                    "new_peers": topic.new_peer_count,
                                }
                                for topic in topics
                            ],
                        }
                    )
                    return
                snapshot = await repository.status_snapshot(
                    connection,
                    now=datetime.now(timezone.utc),
                )
                pressure = await repository.backpressure(
                    connection,
                    pending_screen_limit=config.telegram_chat_discovery_max_pending_screens,
                    source_audit_limit=config.source_audit_calls_per_day,
                    ai_limit=config.opportunity_analysis_backlog_threshold,
                )
                governor = await TelegramCollectorOperationRepository().list_status(
                    connection,
                    now=datetime.now(timezone.utc),
                    limit=100,
                )
            _emit(
                {
                    "runtime_mode": "collector_only",
                    "chat_discovery_enabled": config.telegram_chat_discovery_enabled,
                    **snapshot,
                    "backpressure": {
                        "paused": pressure.paused,
                        "reasons": pressure.reasons,
                        "pending_screens": pressure.pending_screens,
                        "source_audit_backlog": pressure.source_audit_backlog,
                        "ai_backlog": pressure.ai_backlog,
                    },
                    "collectors": [
                        {
                            "collector_account_id": record.collector_account_id,
                            "state": record.status.value,
                            "active_request_category": record.active_request_category,
                            "next_allowed_remaining_seconds": _remaining_seconds(
                                record.next_allowed_request_at,
                                datetime.now(timezone.utc),
                            ),
                            "cooldown_remaining_seconds": _remaining_seconds(
                                record.cooldown_until,
                                datetime.now(timezone.utc),
                            ),
                            "last_floodwait_seconds": record.last_floodwait_seconds,
                        }
                        for record in governor
                    ],
                }
            )
        finally:
            await database.close()
        return

    config = RuntimeConfig.from_env(mode=RuntimeMode.COLLECTOR_ONLY)
    runtime = CollectorOnlyRuntime(config)
    try:
        snapshot = await runtime.start()
        actual_account_id = snapshot.collector_account.id
        requested_account_id = getattr(args, "collector_account_id", None)
        if requested_account_id is not None and requested_account_id != actual_account_id:
            raise ConfigurationError(
                "--collector-account-id does not match the authenticated Telegram session"
            )
        if runtime.governor is None:
            raise RuntimeError("collector governor was not initialized")
        service = TelegramChatDiscoveryService(
            runtime.database,
            runtime.client,
            config=config,
            collector_account_id=actual_account_id,
            governor=runtime.governor,
        )
        worker_id = f"operator-telegram-chat-discovery-{actual_account_id}"
        if command == "run":
            job_ids = await service.schedule_due_searches(max_topics=args.max_topics)
            await service.drain(
                worker_id=worker_id,
                job_type=SEARCH_JOB_TYPE,
                job_ids=job_ids,
                timeout_seconds=max(60.0, args.max_topics * 90.0),
            )
            _emit(
                {
                    "collector_account_id": actual_account_id,
                    "topics_enqueued": len(job_ids),
                    "search_job_ids": job_ids,
                    "jobs_drained": SEARCH_JOB_TYPE,
                }
            )
            return
        if command == "screen-pending":
            job_ids = await service.enqueue_pending_screens(limit=args.limit)
            await service.drain(
                worker_id=worker_id,
                job_type=SCREEN_JOB_TYPE,
                job_ids=job_ids,
                timeout_seconds=max(60.0, args.limit * 90.0),
            )
            _emit(
                {
                    "collector_account_id": actual_account_id,
                    "screens_enqueued": len(job_ids),
                    "screen_job_ids": job_ids,
                    "jobs_drained": SCREEN_JOB_TYPE,
                }
            )
            return
        raise ValueError(f"unsupported telegram-discovery command: {command}")
    finally:
        await runtime.stop()


async def _discovery_command(args: argparse.Namespace) -> None:
    if args.discovery_command == "runs":
        config = _database_config()
        database = Database(config.postgresql_url())
        try:
            async with database.connect() as connection:
                runs = await DiscoveryRunRepository().list_runs(
                    connection,
                    provider=args.provider,
                    status=args.status,
                    limit=args.limit,
                )
            _emit({"count": len(runs), "runs": [_discovery_run_payload(item) for item in runs]})
        finally:
            await database.close()
        return
    if args.discovery_command == "results":
        config = _database_config()
        database = Database(config.postgresql_url())
        try:
            async with database.connect() as connection:
                results = await DiscoveryRunRepository().list_results(
                    connection,
                    args.run_id,
                )
            _emit(
                {
                    "run_id": str(args.run_id),
                    "count": len(results),
                    "results": [_discovery_result_payload(item) for item in results],
                }
            )
        finally:
            await database.close()
        return
    if args.discovery_command == "web":
        await _run_web_discovery(args)
        return
    if args.discovery_command == "graph":
        await _run_graph_discovery(args)
        return
    raise ValueError(f"unsupported discovery command: {args.discovery_command}")


async def _run_web_discovery(args: argparse.Namespace) -> None:
    config = _database_config()
    parameters = _json_mapping(args.parameters_json, "parameters-json")
    database = Database(config.postgresql_url())
    try:
        provider = WebDiscoveryProvider(
            SearxngSearchBackend(
                args.searxng_url,
                timeout_seconds=args.timeout_seconds,
            ),
            governor=WebDiscoveryGovernor.from_config(config, database=database),
        )
        execution = await DiscoveryRunner(database).run(
            provider,
            run_key=args.run_key,
            request=DiscoveryRequest(
                parameters=parameters,
                requested_at=datetime.now(timezone.utc),
            ),
        )
        _emit(_discovery_execution_payload(execution))
    finally:
        await database.close()


async def _run_graph_discovery(args: argparse.Namespace) -> None:
    config = _sources_config()
    if config.api_id is None:
        raise ConfigurationError("TELEGRAM_API_ID/API_ID is required for Source Graph Discovery")
    config.user_session_path.parent.mkdir(parents=True, exist_ok=True)
    with TelegramSessionFileLock(config.user_session_path, role="source_graph"):
        database = Database(config.postgresql_url())
        client = TelegramClient(
            str(config.user_session_path),
            config.api_id,
            _secret(config.api_hash, "TELEGRAM_API_HASH/API_HASH"),
            flood_sleep_threshold=0,
        )
        try:
            await client.start()
            snapshot = await ApprovedTelegramSourceAdapter(database).list_for_session(client)
            if (
                args.collector_account_id is not None
                and args.collector_account_id != snapshot.collector_account.id
            ):
                raise ConfigurationError(
                    "--collector-account-id does not match the authenticated Telegram session"
                )
            seed_ids = tuple(args.seed_source_id[: config.telegram_graph_seeds_per_pass])
            graph_spec = source_graph_campaign_spec(seed_ids)
            async with database.transaction() as connection:
                graph_campaign = await DiscoveryCampaignRepository().ensure_campaign(
                    connection,
                    graph_spec,
                )
                await DiscoveryCampaignRepository().set_status(
                    connection,
                    campaign_id=graph_campaign.id,
                    status="running",
                )
            seed_resolver = PostgresSourceGraphSeedResolver(
                database,
                collector_account_id=snapshot.collector_account.id,
            )
            governor = TelegramRequestGovernor(
                database,
                snapshot.collector_account.id,
                config,
            )
            provider = SourceGraphDiscoveryProvider(
                seed_resolver,
                TelethonSourceGraphBackend(
                    client,
                    governor=governor,
                    max_message_limit=config.telegram_max_history_messages_per_pass,
                    known_source_identities=await seed_resolver.list_known_source_identities(),
                    entity_resolution_budget=(
                        config.telegram_max_entity_resolves_per_graph_pass
                    ),
                ),
                message_limit_per_seed=min(
                    args.message_limit_per_seed,
                    config.telegram_max_history_messages_per_pass,
                ),
                max_candidates=args.max_candidates,
                max_observations=args.max_observations,
            )
            parameters = _json_mapping(args.parameters_json, "parameters-json")
            parameters.update(
                {
                    "campaign_id": str(graph_campaign.id),
                    "campaign_key": graph_campaign.campaign_key,
                    "trigger": "source_graph_expansion",
                }
            )
            try:
                execution = await DiscoveryRunner(database).run(
                    provider,
                    run_key=args.run_key,
                    request=DiscoveryRequest(
                        parameters=parameters,
                        requested_at=datetime.now(timezone.utc),
                        seed_source_ids=seed_ids,
                    ),
                )
            except Exception:
                async with database.transaction() as connection:
                    await DiscoveryCampaignRepository().set_status(
                        connection,
                        campaign_id=graph_campaign.id,
                        status="failed",
                    )
                raise
            async with database.transaction() as connection:
                await DiscoveryCampaignRepository().set_status(
                    connection,
                    campaign_id=graph_campaign.id,
                    status="completed",
                    progress={
                        "seed_count": len(seed_ids),
                        "result_count": execution.run.result_count,
                        "materialized_count": execution.run.materialized_count,
                    },
                    last_run_at=execution.run.finished_at or execution.run.started_at,
                )
            _emit(
                {
                    "collector_account_id": snapshot.collector_account.id,
                    **_discovery_execution_payload(execution),
                }
            )
        finally:
            await client.disconnect()
            await database.close()


async def _audit_command(args: argparse.Namespace) -> None:
    if args.audit_command == "list":
        config = _database_config()
        database = Database(config.postgresql_url())
        try:
            async with database.connect() as connection:
                audits = await SourceAuditRepository().list_audits(
                    connection,
                    source_id=args.source_id,
                    decision=args.decision,
                    limit=args.limit,
                )
            _emit({"count": len(audits), "audits": [_audit_payload(item) for item in audits]})
        finally:
            await database.close()
        return

    config = _sources_config()
    if config.api_id is None:
        raise ConfigurationError("TELEGRAM_API_ID/API_ID is required for Source Audit")
    if config.openai_api_key is None or not config.openai_api_key.get_secret_value().strip():
        raise ConfigurationError("OPENAI_API_KEY is required for Source Audit")
    config.user_session_path.parent.mkdir(parents=True, exist_ok=True)
    with TelegramSessionFileLock(config.user_session_path, role="source_audit"):
        database = Database(config.postgresql_url())
        client = TelegramClient(
            str(config.user_session_path),
            config.api_id,
            _secret(config.api_hash, "TELEGRAM_API_HASH/API_HASH"),
            flood_sleep_threshold=0,
        )
        try:
            await client.start()
            snapshot = await ApprovedTelegramSourceAdapter(database).list_for_session(client)
            source_repository = SourceRepository()
            async with database.connect() as connection:
                source = await source_repository.get(connection, args.source_id)
                if source.platform != "telegram":
                    raise ValueError("Source Audit currently supports Telegram sources only")
                if not await source_repository.is_accessible_to_collector(
                    connection,
                    source_id=source.id,
                    collector_account_id=snapshot.collector_account.id,
                    platform="telegram",
                ):
                    raise PermissionError(
                        "The authenticated collector account has no permitted access to this source"
                    )
            lookup = source.handle or source.canonical_url or source.external_id
            provider = OpenAISourceAuditProvider(
                api_key=config.openai_api_key.get_secret_value(),
                model=config.source_audit_model,
                temperature=config.source_audit_temperature,
                timeout_seconds=config.source_audit_timeout_seconds,
            )
            pipeline = SourceAuditPipeline(
                database,
                SourceAuditSampler(
                    TelethonSourceAuditHistoryReader(
                        client,
                        governor=TelegramRequestGovernor(
                            database,
                            snapshot.collector_account.id,
                            config,
                        ),
                        max_messages_per_pass=config.source_audit_sample_size,
                    ),
                    policy=SourceAuditPolicy(
                        sample_size=config.source_audit_sample_size,
                        minimum_evidence_messages=30,
                        distribution_buckets=min(
                            2,
                            config.source_audit_sample_size,
                        ),
                    ),
                ),
                provider,
            )
            target = SourceAuditTarget(source_id=source.id, platform=source.platform, lookup=lookup)
            if args.audit_command == "run":
                result = await pipeline.run(target, audited_at=datetime.now(timezone.utc))
            elif args.audit_command == "re-audit":
                result = await pipeline.re_audit(target, audited_at=datetime.now(timezone.utc))
            else:
                raise ValueError(f"unsupported audit command: {args.audit_command}")
            async with database.transaction() as connection:
                await DiscoveryCampaignRepository().upsert_validation(
                    connection,
                    source_id=source.id,
                    collector_account_id=snapshot.collector_account.id,
                    state=result.audit.decision,
                    access_mode=(
                        "public_readable"
                        if source.access_type == "public"
                        else "joined"
                    ),
                    checked_at=result.audit.audited_at,
                    checked_by="operator",
                )
            _emit(
                {
                    "collector_account_id": snapshot.collector_account.id,
                    "audit": _audit_payload(result.audit),
                    "source": _source_payload(result.source),
                    "created": result.created,
                    "lifecycle_changed": result.lifecycle_changed,
                }
            )
        finally:
            await client.disconnect()
            await database.close()


async def _match_command(args: argparse.Namespace) -> None:
    config = _database_config()
    database = Database(config.postgresql_url())
    repository = MatchTraceRepository()
    try:
        async with database.connect() as connection:
            if args.match_command == "runs":
                runs = await repository.list_runs(connection, limit=args.limit)
                _emit({"count": len(runs), "runs": [_match_run_payload(item) for item in runs]})
            elif args.match_command == "traces":
                traces = await repository.list_traces(
                    connection,
                    run_id=args.run_id,
                    opportunity_id=args.opportunity_id,
                    limit=args.limit,
                )
                _emit({"count": len(traces), "traces": [_match_trace_payload(item) for item in traces]})
            else:
                raise ValueError(f"unsupported match command: {args.match_command}")
    finally:
        await database.close()


async def _delivery_command(args: argparse.Namespace) -> None:
    config = _database_config()
    database = Database(config.postgresql_url())
    try:
        async with database.connect() as connection:
            deliveries = await PersonalizedDeliveryRepository().list_deliveries(
                connection,
                status=args.status,
                limit=args.limit,
            )
        _emit(
            {
                "count": len(deliveries),
                "deliveries": [_delivery_payload(item) for item in deliveries],
            }
        )
    finally:
        await database.close()


async def _observe_command(args: argparse.Namespace) -> None:
    config = _database_config()
    database = Database(config.postgresql_url())
    try:
        async with database.connect() as connection:
            if args.observe_command == "raw":
                records = await RawMessageRepository().list_recent(
                    connection,
                    limit=args.limit,
                )
                _emit(
                    {
                        "count": len(records),
                        "raw_messages": [_raw_payload(item) for item in records],
                    }
                )
            elif args.observe_command == "opportunities":
                records = await CanonicalOpportunityRepository().list_recent(
                    connection,
                    limit=args.limit,
                )
                _emit(
                    {
                        "count": len(records),
                        "opportunities": [_opportunity_payload(item) for item in records],
                    }
                )
            elif args.observe_command == "metrics":
                report = await ProductMetricsRepository().build_report(
                    connection,
                    window_started_at=args.since,
                    window_ended_at=args.until,
                )
                _emit(_metrics_payload(report))
            else:
                raise ValueError(f"unsupported observe command: {args.observe_command}")
    finally:
        await database.close()


async def _profile_discovery_command(args: argparse.Namespace) -> None:
    config = _database_config()
    database = Database(config.postgresql_url())
    governor = WebDiscoveryGovernor.from_config(config, database=database)
    service = ProfileDiscoveryService(database, web_governor=governor)
    try:
        if args.profile_discovery_command == "intents":
            async with database.connect() as connection:
                records = await service.list_intents(
                    connection,
                    search_profile_id=args.profile_id,
                    limit=args.limit,
                )
            _emit(
                {
                    "count": len(records),
                    "intents": [_intent_payload(item) for item in records],
                }
            )
            return

        if args.profile_discovery_command == "coverage":
            async with database.connect() as connection:
                profile = await SearchProfileRepository().get(connection, args.profile_id)
            if not profile.is_active or profile.confirmation_status.value != "confirmed":
                raise ValueError("profile-discovery coverage requires an active confirmed profile")
            coverage = await service.coverage_for_profile(profile)
            _emit({"profile_id": profile.id, "coverage": _coverage_payload(coverage)})
            return

        if args.profile_discovery_command == "calibrate":
            _emit(await _profile_calibration_payload(database))
            return

        if args.profile_discovery_command == "canary":
            searxng_url = args.searxng_url or config.searxng_url
            if not searxng_url:
                raise ConfigurationError("SEARXNG_URL is required for Web canary")
            _emit(
                await _run_profile_web_canary(
                    database,
                    config,
                    governor,
                    searxng_url=searxng_url,
                    run_key=args.run_key,
                )
            )
            return

        searxng_url = args.searxng_url or config.searxng_url
        if not searxng_url:
            raise ConfigurationError("SEARXNG_URL is required for profile Web Discovery")
        now = datetime.now(timezone.utc)
        if args.profile_discovery_command == "run":
            async with database.connect() as connection:
                profile = await SearchProfileRepository().get(connection, args.profile_id)
            if not profile.is_active or profile.confirmation_status.value != "confirmed":
                raise ValueError("profile Web Discovery requires an active confirmed profile")
            run_key = args.run_key or (
                f"profile-web-discovery:{profile.id}:{profile.revision}:"
                f"{int(now.timestamp()) // config.source_discovery_interval_seconds}"
            )
            execution = await service.discover_profile(
                profile,
                requested_at=now,
                run_key=run_key,
                searxng_url=searxng_url,
                results_per_query=args.results_per_query,
                max_candidates=args.max_candidates,
            )
            _emit(_profile_execution_payload(execution))
            return

        if args.profile_discovery_command == "evaluate":
            backend = SearxngSearchBackend(searxng_url)
            seen_source_ids: set[int] = set()
            executions = []
            candidate_samples: dict[str, list[dict[str, object]]] = {}
            specs = evaluation_profile_specs()
            if args.profile_keys:
                unknown = set(args.profile_keys) - {spec.key for spec in specs}
                if unknown:
                    raise ValueError(
                        "unknown evaluation profile key: "
                        + ", ".join(sorted(unknown))
                    )
                specs = tuple(spec for spec in specs if spec.key in args.profile_keys)
            for spec in specs:
                execution = await service.discover_evaluation_profile(
                    spec,
                    requested_at=now,
                    run_key=f"{args.run_prefix}:{spec.key}",
                    backend=backend,
                    results_per_query=args.results_per_query,
                    max_candidates=args.max_candidates,
                    previous_source_ids=seen_source_ids,
                )
                executions.append((spec, execution))
                candidate_samples[spec.key] = await _candidate_quality_sample(
                    database,
                    spec,
                    execution,
                )
                seen_source_ids.update(
                    result.source_id for result in execution.execution.results
                )
            _emit(_profile_evaluation_payload(executions, candidate_samples))
            return
        raise ValueError(
            f"unsupported profile-discovery command: {args.profile_discovery_command}"
        )
    finally:
        await database.close()


async def _run_profile_web_canary(
    database: Database,
    config: RuntimeConfig,
    governor: WebDiscoveryGovernor,
    *,
    searxng_url: str,
    run_key: str | None,
) -> dict[str, object]:
    spec = next(
        item
        for item in evaluation_profile_specs()
        if item.key == "python-telegram-developer"
    )
    intent = build_evaluation_intent(spec)
    full_strategy = web_strategy_for_intent(
        intent,
        results_per_query=5,
        max_candidates=20,
    )
    collapsed = collapse_near_duplicate_queries(
        full_strategy.build_queries(
            DiscoveryRequest(
                parameters={},
                requested_at=datetime.now(timezone.utc),
            )
        )
    )
    selected = tuple(
        query
        for angle in ("direct", "buyer_habitat", "adjacent")
        for query in collapsed.queries
        if query.angle == angle
    )
    selected_by_angle: dict[str, int] = defaultdict(int)
    bounded_queries = []
    for query in selected:
        if selected_by_angle[query.angle] >= 2:
            continue
        selected_by_angle[query.angle] += 1
        bounded_queries.append(query)
    now = datetime.now(timezone.utc)
    provider = WebDiscoveryProvider(
        SearxngSearchBackend(searxng_url),
        strategy=full_strategy,
        governor=governor,
        queries=tuple(bounded_queries),
    )
    execution = await DiscoveryRunner(database).run(
        provider,
        run_key=run_key or f"web-canary:python-telegram-developer:{int(now.timestamp())}",
        request=DiscoveryRequest(
            parameters={
                "trigger": "profile_discovery_canary",
                "profile_discovery": {
                    "intent_id": str(intent.id),
                    "profile_revision": intent.profile_revision,
                    "intent_version": intent.version,
                },
            },
            requested_at=now,
        ),
    )
    observability = provider.observability
    return {
        "profile": spec.key,
        "max_executable_queries": 6,
        "selected_queries_by_angle": dict(selected_by_angle),
        "discovery_run": _discovery_run_payload(execution.run),
        "result_count": len(execution.results),
        "provider_observability": dict(observability),
    }


async def _profile_calibration_payload(database: Database) -> dict[str, object]:
    source_repository = SourceRepository()
    discovery_repository = DiscoveryRunRepository()
    specs = evaluation_profile_specs()
    spec_by_key = {spec.key: spec for spec in specs}
    async with database.connect() as connection:
        all_sources = await source_repository.list_sources(
            connection,
            platform="telegram",
            limit=1000,
        )
        approved_sources = tuple(
            source
            for source in all_sources
            if source.lifecycle_status is SourceStatus.APPROVED
        )
        source_by_id = {source.id: source for source in all_sources}
        runs = await discovery_repository.list_runs(
            connection,
            provider="web_search",
            limit=1000,
        )
        exploratory: dict[tuple[str, int], list[Mapping[str, object]]] = defaultdict(list)
        exploratory_row_counts: Counter[str] = Counter()
        for run in runs:
            if not run.run_key.startswith("profile-evaluation-web-"):
                continue
            profile_key = run.run_key.rsplit(":", 1)[-1]
            if profile_key not in spec_by_key:
                continue
            results = await discovery_repository.list_results(connection, run.id)
            exploratory_row_counts[profile_key] += len(results)
            for result in results:
                if result.source_id in source_by_id:
                    exploratory[(profile_key, result.source_id)].append(result.context)

        approved_rows: list[dict[str, object]] = []
        approved_summary: dict[str, object] = {}
        product_ux_rows: list[dict[str, object]] = []
        for spec in specs:
            intent = build_evaluation_intent(spec)
            before_evaluations = []
            after_evaluations = []
            for source in approved_sources:
                lineages = _lineages_for_intent(
                    intent,
                    await source_repository.list_lineage(connection, source.id),
                )
                before = evaluate_source_relevance_legacy(intent, source, lineages)
                after = evaluate_source_relevance(intent, source, lineages)
                before_evaluations.append(before)
                after_evaluations.append(after)
                row = _calibration_row(
                    spec.key,
                    "approved_library",
                    before,
                    after,
                )
                approved_rows.append(row)
                if spec.key == "product-ux-designer":
                    product_ux_rows.append(row)
            coverage = coverage_from_evaluations(
                len(approved_sources),
                intent,
                after_evaluations,
            )
            before_priority = Counter(
                _evaluation_priority(item) for item in before_evaluations
            )
            after_priority = Counter(
                _evaluation_priority(item) for item in after_evaluations
            )
            approved_summary[spec.key] = {
                "profile": spec.label,
                "approved_sources_total": len(approved_sources),
                "relevant": coverage.relevant,
                "high": after_priority["HIGH"],
                "medium": after_priority["MEDIUM"],
                "low": after_priority["LOW"],
                "insufficient": after_priority["INSUFFICIENT"],
                "coverage_status": _coverage_status(coverage),
                "discovery_priority": coverage.discovery_priority,
                "before": dict(before_priority),
                "after": dict(after_priority),
                "before_relevance_classes": dict(
                    Counter(item.relevance_class for item in before_evaluations)
                ),
                "after_relevance_classes": dict(
                    Counter(item.relevance_class for item in after_evaluations)
                ),
            }

        exploratory_rows: list[dict[str, object]] = []
        exploratory_records: list[dict[str, object]] = []
        exploratory_before = Counter()
        exploratory_after = Counter()
        exploratory_labels = Counter()
        exploratory_by_profile_before: dict[str, Counter[str]] = defaultdict(Counter)
        exploratory_by_profile_after: dict[str, Counter[str]] = defaultdict(Counter)
        high_root_cause = Counter()
        for (profile_key, source_id), contexts in sorted(exploratory.items()):
            spec = spec_by_key[profile_key]
            intent = build_evaluation_intent(spec)
            source = source_by_id[source_id]
            lineages = tuple(
                SimpleNamespace(context=context, provider="web_search")
                for context in contexts
            )
            before = evaluate_source_relevance_legacy(intent, source, lineages)
            after = evaluate_source_relevance(intent, source, lineages)
            before_priority = _evaluation_priority(before)
            after_priority = _evaluation_priority(after)
            exploratory_before[before_priority] += 1
            exploratory_after[after_priority] += 1
            label = (
                after.explanation.diagnostic_label
                if after.explanation is not None
                else "CLEARLY_IRRELEVANT"
            )
            exploratory_labels[label] += 1
            exploratory_by_profile_before[profile_key][before_priority] += 1
            exploratory_by_profile_after[profile_key][after_priority] += 1
            explanation = after.explanation
            if after_priority == "HIGH" and explanation is not None:
                direct = bool(
                    explanation.direct_profession_hits
                    or explanation.direct_service_hits
                )
                high_root_cause[
                    "high_with_direct_evidence"
                    if direct
                    else "high_without_direct_evidence"
                ] += 1
                if explanation.primary_evidence_family == "specific_buyer_habitat":
                    high_root_cause["high_mainly_buyer_habitat"] += 1
                if explanation.primary_evidence_family == "adjacent_context":
                    high_root_cause["high_mainly_adjacent"] += 1
                high_root_cause[
                    "high_one_evidence_family"
                    if explanation.independent_evidence_families == 1
                    else "high_two_or_more_evidence_families"
                ] += 1
            row = _calibration_row(
                profile_key,
                "exploratory_persisted_observation",
                before,
                after,
            )
            row["semantic_description"] = _safe_semantic_description(
                spec,
                source,
                contexts,
                after,
            )
            exploratory_rows.append(row)
            exploratory_records.append(row)

        high_sample = _diverse_calibration_sample(
            exploratory_records,
            priorities={"HIGH"},
            limit=20,
        )
        low_sample = _diverse_calibration_sample(
            exploratory_records,
            priorities={"LOW", "INSUFFICIENT"},
            limit=20,
        )

        return {
            "formula": {
                "before_version": "source-profile-relevance.v1",
                "after_version": "source-profile-relevance.v2",
                "before": {
                    "semantic_corpus": "display_name + external_id + every scalar lineage value",
                    "buyer_floor": "buyer_habitat/query_angle could create one buyer hit",
                    "query_diversity": "two lineage matches added 0.05",
                    "classification": "strong>=0.75, adequate>=0.45, weak<0.45",
                },
                "after": {
                    "semantic_corpus": "display_name + curated seed tags/reasons + result title/snippet",
                    "provenance": "query/topic/URL/handle never create direct semantic hits",
                    "generic_buyer": "bounded 0.04 contribution; never high alone",
                    "direct_profession": "0.76 base plus bounded independent-signal additions",
                    "direct_service": "0.74 base plus bounded independent-signal additions",
                    "priority": "HIGH=strong; MEDIUM=adequate; LOW=weak with evidence; INSUFFICIENT=weak without semantic evidence",
                    "classification": "strong>=0.75, adequate>=0.45, weak<0.45",
                },
            },
            "approved_library": {
                "profiles": approved_summary,
                "matrix": approved_rows,
            },
            "exploratory_rescore": {
                "raw_persisted_rows_by_profile": dict(exploratory_row_counts),
                "unique_profile_source_pairs": len(exploratory_rows),
                "before": dict(exploratory_before),
                "after": dict(exploratory_after),
                "insufficient_or_rejected": exploratory_after["INSUFFICIENT"],
                "diagnostic_labels": dict(exploratory_labels),
                "by_profile": {
                    key: {
                        "before": dict(exploratory_by_profile_before.get(key, {})),
                        "after": dict(exploratory_by_profile_after.get(key, {})),
                    }
                    for key in sorted(spec_by_key)
                },
                "high_priority_root_cause": dict(high_root_cause),
                "human_sense_sample": {
                    "high_requested": 20,
                    "high_returned": len(high_sample),
                    "low_or_insufficient_requested": 20,
                    "low_or_insufficient_returned": len(low_sample),
                    "high": high_sample,
                    "low_or_insufficient": low_sample,
                },
                "matrix": exploratory_rows,
            },
            "product_ux_13_of_13": {
                "rows": product_ux_rows,
                "after_categories": dict(
                    Counter(row["source_semantic_category"] for row in product_ux_rows)
                ),
                "after_diagnostic_labels": dict(
                    Counter(row["diagnostic_label"] for row in product_ux_rows)
                ),
            },
            "query_redundancy": _query_redundancy_payload(specs),
        }


async def _candidate_quality_sample(
    database: Database,
    spec: Any,
    execution: ProfileDiscoveryExecution,
) -> list[dict[str, object]]:
    source_repository = SourceRepository()
    evaluations = []
    async with database.connect() as connection:
        for result in execution.execution.results:
            source = await source_repository.get(connection, result.source_id)
            lineages = (
                SimpleNamespace(
                    context=result.context,
                    provider="web_search",
                ),
            )
            evaluations.append(evaluate_source_relevance(execution.intent, source, lineages))
    evaluations.sort(key=lambda item: (-item.relevance_score, item.source_id))
    sample: list[dict[str, object]] = []
    for evaluation in evaluations[:20]:
        explanation = evaluation.explanation
        sample.append(
            {
                "profile": spec.key,
                "source_semantic_category": (
                    explanation.semantic_category
                    if explanation is not None
                    else "unrelated_or_unknown"
                ),
                "evidence_types": evaluation.evidence_categories,
                "priority": _evaluation_priority(evaluation),
                "diagnostic_label": _validation_label(
                    explanation,
                    _evaluation_priority(evaluation),
                ),
                "evidence_diversity": (
                    0
                    if explanation is None
                    else explanation.independent_evidence_families
                ),
                "why": explanation.why if explanation is not None else "no semantic evidence",
            }
        )
    return sample


def _calibration_row(
    profile_key: str,
    observation_kind: str,
    before: Any,
    after: Any,
) -> dict[str, object]:
    explanation = after.explanation
    priority = _evaluation_priority(after)
    before_priority = _evaluation_priority(before)
    return {
        "profile": profile_key,
        "observation_kind": observation_kind,
        "source_semantic_category": (
            explanation.semantic_category
            if explanation is not None
            else "unrelated_or_unknown"
        ),
        "evidence_types": after.evidence_categories,
        "relevance_score": after.relevance_score,
        "relevance_class": after.relevance_class,
        "priority_class": priority,
        "validation_label": _validation_label(explanation, priority),
        "direct_role_evidence": (
            0 if explanation is None else explanation.direct_profession_hits
        ),
        "direct_service_evidence": (
            0 if explanation is None else explanation.direct_service_hits
        ),
        "specific_buyer_habitat_evidence": (
            0 if explanation is None else explanation.specific_buyer_hits
        ),
        "generic_buyer_habitat_evidence": (
            0 if explanation is None else explanation.generic_buyer_hits
        ),
        "adjacent_evidence": 0 if explanation is None else explanation.adjacent_hits,
        "evidence_diversity": (
            0
            if explanation is None
            else explanation.independent_evidence_families
        ),
        "primary_evidence_family": (
            "none" if explanation is None else explanation.primary_evidence_family
        ),
        "query_signal_count": (
            0 if explanation is None else explanation.query_signal_count
        ),
        "result_title_snippet_hits": (
            0 if explanation is None else explanation.result_title_snippet_hits
        ),
        "score_components": (
            []
            if explanation is None
            else [
                {"name": name, "value": value}
                for name, value in explanation.score_components
            ]
        ),
        "diagnostic_label": (
            explanation.diagnostic_label
            if explanation is not None
            else "CLEARLY_IRRELEVANT"
        ),
        "why": explanation.why if explanation is not None else "no semantic evidence",
        "before_relevance_score": before.relevance_score,
        "before_relevance_class": before.relevance_class,
        "before_priority_class": before_priority,
    }


def _evaluation_priority(evaluation: Any) -> str:
    explanation = evaluation.explanation
    if explanation is not None:
        return explanation.priority_class
    if evaluation.relevance_class == "strong":
        return "HIGH"
    if evaluation.relevance_class == "adequate":
        return "MEDIUM"
    semantic_evidence = {
        value
        for value in evaluation.evidence_categories
        if value
        not in {"telegram_source_likeness", "query_diversity"}
    }
    return "LOW" if semantic_evidence else "INSUFFICIENT"


def _validation_label(explanation: Any, priority: str) -> str:
    if priority == "HIGH" and explanation is not None:
        if explanation.direct_profession_hits or explanation.direct_service_hits:
            return "DEFINITELY_WORTH_TELEGRAM_VALIDATION"
        return "MAYBE_WORTH_VALIDATION"
    return "PROBABLY_TOO_BROAD"


def _safe_semantic_description(
    spec: Any,
    source: Any,
    contexts: Sequence[Mapping[str, object]],
    evaluation: Any,
) -> str:
    text_values = [str(getattr(source, "display_name", ""))]
    for context in contexts:
        matches = context.get("matches") if isinstance(context, Mapping) else None
        if isinstance(matches, list):
            for match in matches:
                if isinstance(match, Mapping):
                    for key in ("result_title", "result_snippet"):
                        value = match.get(key)
                        if isinstance(value, str):
                            text_values.append(value)
    corpus = " ".join(text_values).casefold()
    if any(token in corpus for token in ("job", "jobs", "hiring", "ищу", "ваканс")):
        habitat = "specialist hiring/jobs community"
    elif any(token in corpus for token in ("founder", "startup", "стартап")):
        habitat = "founder/startup community"
    elif any(token in corpus for token in ("marketing", "smm", "маркетинг", "agency")):
        habitat = "general marketing discussion community"
    elif any(token in corpus for token in ("product", "ux", "design", "дизайн")):
        habitat = "product/design community"
    elif any(token in corpus for token in ("telegram", "telethon", "python", "postgres")):
        habitat = "Telegram development/automation community"
    elif evaluation.explanation is not None and evaluation.explanation.semantic_category == "generic_business":
        habitat = "broad business discussion community"
    elif evaluation.explanation is not None and evaluation.explanation.semantic_category == "unrelated_or_unknown":
        habitat = "unrelated Telegram community for this profile"
    else:
        habitat = "profession-adjacent Telegram community"
    return f"{spec.label} — {habitat}"


def _diverse_calibration_sample(
    rows: Sequence[Mapping[str, object]],
    *,
    priorities: set[str],
    limit: int,
) -> list[dict[str, object]]:
    grouped: dict[str, list[Mapping[str, object]]] = defaultdict(list)
    for row in rows:
        if row.get("priority_class") in priorities:
            grouped[str(row["profile"])].append(row)
    selected: list[dict[str, object]] = []
    while len(selected) < limit and grouped:
        for profile in sorted(tuple(grouped)):
            values = grouped[profile]
            if not values:
                del grouped[profile]
                continue
            selected.append(dict(values.pop(0)))
            if len(selected) >= limit:
                break
        for profile in tuple(grouped):
            if not grouped[profile]:
                del grouped[profile]
    return selected


def _priority_name(relevance_class: str) -> str:
    return {
        "strong": "high",
        "adequate": "medium",
        "weak": "low",
    }.get(relevance_class, "insufficient")


def _coverage_status(coverage: ProfileSourceCoverage) -> str:
    if coverage.relevant == 0:
        return "NO_RELEVANT_APPROVED_SOURCE"
    if coverage.direct == 0:
        return "BUYER_OR_ADJACENT_ONLY"
    return "DIRECT_EVIDENCE_PRESENT"


def _query_redundancy_payload(specs: Sequence[Any]) -> dict[str, object]:
    per_profile: dict[str, object] = {}
    all_queries: dict[str, set[str]] = defaultdict(set)
    for spec in specs:
        intent = build_evaluation_intent(spec)
        queries = web_strategy_for_intent(intent).build_queries(
            DiscoveryRequest(parameters={}, requested_at=datetime.now(timezone.utc))
        )
        collapsed = collapse_near_duplicate_queries(queries)
        normalized = [" ".join(query.text.casefold().split()) for query in queries]
        for query in queries:
            all_queries[" ".join(query.text.casefold().split())].add(spec.key)
        per_profile[spec.key] = {
            "generated": len(queries),
            "normalized_unique": len(set(normalized)),
            "near_duplicate_pairs": collapsed.near_duplicates,
            "exact_duplicates_collapsed": collapsed.exact_duplicates,
            "near_deduplicated_executable": len(collapsed.queries),
            "executable_by_angle": dict(collapsed.executable_by_angle),
            "direct": sum(query.angle == "direct" for query in queries),
            "buyer_habitat": sum(query.angle == "buyer_habitat" for query in queries),
            "adjacent": sum(query.angle == "adjacent" for query in queries),
        }
    cross_profile = {
        key: len(profiles)
        for key, profiles in all_queries.items()
        if len(profiles) > 1
    }
    return {
        "per_profile": per_profile,
        "cross_profile_duplicate_query_keys": len(cross_profile),
        "cross_profile_duplicate_occurrences": sum(cross_profile.values()),
        "global_normalized_unique": len(all_queries),
        "estimated_executable_requests": sum(
            value["near_deduplicated_executable"]
            for value in per_profile.values()
        ),
    }


def _intent_payload(intent: ProfileDiscoveryIntent) -> dict[str, object]:
    return {
        "id": intent.id,
        "search_profile_id": intent.search_profile_id,
        "profile_revision": intent.profile_revision,
        "version": intent.version,
        "roles_count": len(intent.roles),
        "services_count": len(intent.services),
        "skills_count": len(intent.skills),
        "industries_count": len(intent.industries),
        "languages": intent.languages,
        "geographies": intent.geo_remote.get("geographies", []),
        "work_modes": intent.geo_remote.get("work_modes", []),
        "buyer_role_count": len(intent.likely_buyer_roles),
        "buyer_habitat_count": len(intent.buyer_habitats),
        "literal_angle_count": len(intent.literal_concepts),
        "adjacent_angle_count": len(intent.adjacent_concepts),
        "generated_web_query_count": len(intent.generated_web_queries),
    }


def _coverage_payload(coverage: ProfileSourceCoverage) -> dict[str, object]:
    return {
        "approved_total": coverage.approved_total,
        "relevant": coverage.relevant,
        "direct": coverage.direct,
        "buyer_habitat": coverage.buyer_habitat,
        "weak": coverage.weak,
        "adequate": coverage.adequate,
        "strong": coverage.strong,
        "discovery_priority": coverage.discovery_priority,
    }


def _profile_execution_payload(
    execution: ProfileDiscoveryExecution,
) -> dict[str, object]:
    return {
        "profile_key": execution.profile_key,
        "intent": _intent_payload(execution.intent),
        "discovery_run": _discovery_run_payload(execution.execution.run),
        "generated_query_count": execution.generated_query_count,
        "direct_query_count": execution.direct_query_count,
        "buyer_habitat_query_count": execution.buyer_habitat_query_count,
        "adjacent_query_count": execution.adjacent_query_count,
        "search_results_considered": execution.search_results_considered,
        "telegram_like_candidates": execution.telegram_like_candidates,
        "unique_candidates": execution.unique_candidates,
        "known_candidates": execution.known_candidates,
        "new_candidates": execution.new_candidates,
        "overlap_with_previous_profiles": execution.overlap_with_previous_profiles,
        "candidate_queue": dict(execution.candidate_priority_counts),
        "provider_observability": dict(execution.provider_observability),
        "coverage": (
            None if execution.coverage is None else _coverage_payload(execution.coverage)
        ),
    }


def _profile_evaluation_payload(
    executions: Sequence[tuple[object, ProfileDiscoveryExecution]],
    candidate_samples: Mapping[str, Sequence[Mapping[str, object]]] | None = None,
) -> dict[str, object]:
    candidate_samples = candidate_samples or {}
    source_sets = {
        execution.profile_key: {
            result.source_id for result in execution.execution.results
        }
        for _, execution in executions
    }
    pairwise: dict[str, int] = {}
    keys = tuple(source_sets)
    for index, left in enumerate(keys):
        for right in keys[index + 1:]:
            pairwise[f"{left}__{right}"] = len(source_sets[left] & source_sets[right])
    return {
        "profiles": [
            {
                "profile": spec.label,
                "profile_key": spec.key,
                "specialist_concepts": execution.intent.literal_concepts,
                "services": execution.intent.services,
                "likely_buyers": execution.intent.likely_buyer_roles,
                "buyer_habitats": execution.intent.buyer_habitats,
                "literal_search_angles": execution.intent.literal_concepts,
                "non_obvious_search_angles": execution.intent.adjacent_concepts,
                "language_geo": {
                    "languages": execution.intent.languages,
                    "geo_remote": execution.intent.geo_remote,
                },
                "web_discovery": _profile_execution_payload(execution),
                "candidate_quality_sample": list(
                    candidate_samples.get(execution.profile_key, ())
                ),
            }
            for spec, execution in executions
        ],
        "global_candidate_dedup": {
            "raw_profile_candidate_observations": sum(
                execution.unique_candidates for _, execution in executions
            ),
            "unique_global_candidates": len(set().union(*source_sets.values()))
            if source_sets
            else 0,
            "known_candidates": sum(
                execution.known_candidates for _, execution in executions
            ),
            "new_candidates": sum(
                execution.new_candidates for _, execution in executions
            ),
            "cross_profile_overlap_observations": sum(
                execution.overlap_with_previous_profiles for _, execution in executions
            ),
            "pairwise_overlap_counts": pairwise,
        },
        "existing_library_coverage": {
            execution.profile_key: (
                None
                if execution.coverage is None
                else _coverage_payload(execution.coverage)
            )
            for _, execution in executions
        },
        "new_candidate_counts": {
            execution.profile_key: {
                "unique": execution.unique_candidates,
                "known": execution.known_candidates,
                "new": execution.new_candidates,
                "high_priority": execution.candidate_priority_counts.get("high", 0),
                "medium_priority": execution.candidate_priority_counts.get("medium", 0),
                "low_priority": execution.candidate_priority_counts.get("low", 0),
            }
            for _, execution in executions
        },
    }


def _database_config() -> RuntimeConfig:
    config = RuntimeConfig.from_env(mode=RuntimeMode.DATABASE)
    config.postgresql_url()
    return config


def _sources_config() -> RuntimeConfig:
    config = RuntimeConfig.from_env(mode=RuntimeMode.CHECK_SOURCES)
    config.postgresql_url()
    return config


def _add_limit(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--limit", type=_positive_int, default=100)


def _positive_int(value: str) -> int:
    parsed = int(value)
    if parsed <= 0:
        raise argparse.ArgumentTypeError("must be positive")
    return parsed


def _nonnegative_int(value: str) -> int:
    parsed = int(value)
    if parsed < 0:
        raise argparse.ArgumentTypeError("must be non-negative")
    return parsed


def _remaining_seconds(value: datetime | None, now: datetime) -> int:
    if value is None:
        return 0
    return max(0, int((value - now).total_seconds()))


def _uuid_arg(value: str) -> UUID:
    try:
        return UUID(value)
    except ValueError:
        raise argparse.ArgumentTypeError("must be a UUID") from None


def _timestamp_arg(value: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        raise argparse.ArgumentTypeError("must be an ISO-8601 timestamp") from None
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise argparse.ArgumentTypeError("timestamp must include a timezone")
    return parsed


def _json_mapping(value: str, field: str) -> dict[str, Any]:
    try:
        parsed = json.loads(value)
    except json.JSONDecodeError as exc:
        raise ValueError(f"{field} must be valid JSON: {exc.msg}") from None
    if not isinstance(parsed, Mapping):
        raise ValueError(f"{field} must be a JSON object")
    return dict(parsed)


def _secret(value, name: str) -> str:
    if value is None or not value.get_secret_value().strip():
        raise ConfigurationError(f"{name} is required")
    return value.get_secret_value()


def _emit(value: object) -> None:
    print(json.dumps(value, ensure_ascii=False, sort_keys=True, default=_json_default))


def _json_default(value: object) -> str:
    if isinstance(value, (datetime, UUID, Decimal)):
        return str(value)
    return str(value)


def _source_payload(source) -> dict[str, object]:
    return {
        "id": source.id,
        "platform": source.platform,
        "external_id": source.external_id,
        "access_type": source.access_type,
        "lifecycle_status": source.lifecycle_status.value,
        "display_name": source.display_name,
        "handle": source.handle,
        "canonical_url": source.canonical_url,
        "created_at": source.created_at,
        "updated_at": source.updated_at,
    }


def _lineage_payload(lineage) -> dict[str, object]:
    return {
        "id": lineage.id,
        "source_id": lineage.source_id,
        "provider": lineage.provider,
        "lineage_key": lineage.lineage_key,
        "provider_run_id": lineage.provider_run_id,
        "discovery_run_id": lineage.discovery_run_id,
        "seed_source_id": lineage.seed_source_id,
        "seed_reference": lineage.seed_reference,
        "discovered_at": lineage.discovered_at,
    }


def _lifecycle_payload(event) -> dict[str, object]:
    return {
        "id": event.id,
        "source_id": event.source_id,
        "from_status": None if event.from_status is None else event.from_status.value,
        "to_status": event.to_status.value,
        "actor_kind": event.actor_kind,
        "actor_id": event.actor_id,
        "reason": event.reason,
        "is_override": event.is_override,
        "source_audit_id": event.source_audit_id,
        "changed_at": event.changed_at,
    }


def _audit_payload(audit) -> dict[str, object]:
    return {
        "id": audit.id,
        "source_id": audit.source_id,
        "audit_key": audit.audit_key,
        "provider": audit.provider,
        "model": audit.model,
        "analyzer_version": audit.analyzer_version,
        "audited_at": audit.audited_at,
        "sampled_message_count": audit.sampled_message_count,
        "probe_message_count": audit.probe_message_count,
        "expanded": audit.expanded,
        "high_volume": audit.high_volume,
        "commercial_opportunity_count": audit.commercial_opportunity_count,
        "buyer_intent_count": audit.buyer_intent_count,
        "seller_promotion_count": audit.seller_promotion_count,
        "ads_spam_count": audit.ads_spam_count,
        "duplicate_count": audit.duplicate_count,
        "decision": audit.decision,
        "reason_codes": audit.reason_codes,
    }


def _discovery_run_payload(run) -> dict[str, object]:
    return {
        "id": run.id,
        "provider": run.provider,
        "provider_kind": run.provider_kind,
        "run_key": run.run_key,
        "status": run.status.value,
        "result_count": run.result_count,
        "materialized_count": run.materialized_count,
        "failure_code": run.failure_code,
        "started_at": run.started_at,
        "finished_at": run.finished_at,
        "created_at": run.created_at,
        "observability": run.request.get("observability"),
    }


def _discovery_result_payload(result) -> dict[str, object]:
    return {
        "id": result.id,
        "run_id": result.run_id,
        "provider_result_key": result.provider_result_key,
        "source_id": result.source_id,
        "outcome": result.outcome.value,
        "platform": result.platform,
        "external_id": result.external_id,
        "access_type": result.access_type,
        "display_name": result.display_name,
        "handle": result.handle,
        "canonical_url": result.canonical_url,
        "discovered_at": result.discovered_at,
        "seed_source_id": result.seed_source_id,
        "seed_reference": result.seed_reference,
    }


def _discovery_execution_payload(execution: DiscoveryExecution) -> dict[str, object]:
    return {
        "run": _discovery_run_payload(execution.run),
        "result_count": len(execution.results),
        "results": [_discovery_result_payload(item) for item in execution.results],
    }


def _campaign_payload(campaign) -> dict[str, object]:
    return {
        "id": campaign.id,
        "campaign_key": campaign.campaign_key,
        "campaign_type": campaign.campaign_type,
        "status": campaign.status,
        "languages": campaign.languages,
        "geo_constraints": campaign.geo_constraints,
        "buyer_habitats": campaign.buyer_habitats,
        "industry_contexts": campaign.industry_contexts,
        "priority": campaign.priority,
        "created_from": campaign.created_from,
        "query_strategy_version": campaign.query_strategy_version,
        "budget": campaign.budget,
        "progress": campaign.progress,
        "created_at": campaign.created_at,
        "updated_at": campaign.updated_at,
        "last_run_at": campaign.last_run_at,
        "next_run_at": campaign.next_run_at,
    }


def _match_run_payload(run) -> dict[str, object]:
    return {
        "id": run.id,
        "idempotency_key": run.idempotency_key,
        "schema_version": run.schema_version,
        "algorithm_version": run.algorithm_version,
        "policy_version": run.policy_version,
        "policy_config": run.policy_config,
        "evaluated_at": run.evaluated_at,
        "trace_count": run.trace_count,
        "created_at": run.created_at,
    }


def _match_trace_payload(record) -> dict[str, object]:
    trace = record.trace
    return {
        "id": record.id,
        "run_id": record.run_id,
        "opportunity_id": trace.opportunity_id,
        "search_profile_id": trace.search_profile_id,
        "profile_revision": trace.profile_revision,
        "hard_filter_eligible": trace.hard_filter_eligible,
        "hard_filter_reasons": trace.hard_filter_reasons,
        "semantic_status": trace.semantic_status,
        "combined_relevance_score": trace.combined_relevance_score,
        "final_rank_score": trace.final_rank_score,
        "decision_code": trace.decision_code.value,
        "eligible": trace.eligible,
        "rank": trace.rank,
        "evaluated_at": trace.evaluated_at,
        "created_at": record.created_at,
    }


def _delivery_payload(delivery) -> dict[str, object]:
    return {
        "id": delivery.id,
        "idempotency_key": delivery.idempotency_key,
        "match_trace_id": delivery.match_trace_id,
        "match_run_id": delivery.match_run_id,
        "opportunity_id": delivery.opportunity_id,
        "search_profile_id": delivery.search_profile_id,
        "profile_revision": delivery.profile_revision,
        "status": delivery.status.value,
        "recipient_platform": delivery.recipient_platform,
        "recipient_external_user_id": delivery.recipient_external_user_id,
        "job_id": delivery.job_id,
        "rendered_at": delivery.rendered_at,
        "attempt_count": delivery.attempt_count,
        "failure_code": delivery.failure_code,
        "telegram_message_id": delivery.telegram_message_id,
        "sent_at": delivery.sent_at,
        "created_at": delivery.created_at,
        "updated_at": delivery.updated_at,
        "card_body_sha256": hashlib.sha256(delivery.card_body_html.encode("utf-8")).hexdigest(),
        "card_body_length": len(delivery.card_body_html),
    }


def _raw_payload(message) -> dict[str, object]:
    return {
        "id": message.id,
        "source_id": message.source_id,
        "collector_account_id": message.collector_account_id,
        "processing_job_id": message.processing_job_id,
        "external_message_id": message.external_message_id,
        "message_date": message.message_date,
        "observed_at": message.observed_at,
        "message_url": message.message_url,
        "ingestion_origin": message.ingestion_origin.value,
        "correlation_id": message.correlation_id,
        "content_sha256": hashlib.sha256(message.content.encode("utf-8")).hexdigest(),
        "content_length": len(message.content),
    }


def _opportunity_payload(opportunity) -> dict[str, object]:
    analysis_json = opportunity.analysis.model_dump_json()
    return {
        "id": opportunity.id,
        "schema_version": opportunity.schema_version,
        "lifecycle_status": opportunity.lifecycle_status.value,
        "first_seen_at": opportunity.first_seen_at,
        "last_seen_at": opportunity.last_seen_at,
        "created_at": opportunity.created_at,
        "updated_at": opportunity.updated_at,
        "canonical_title_present": opportunity.canonical_title is not None,
        "task_summary_present": opportunity.task_summary is not None,
        "analysis_sha256": hashlib.sha256(analysis_json.encode("utf-8")).hexdigest(),
        "raw_message_count": len(opportunity.raw_message_ids),
        "analysis_cache_count": len(opportunity.analysis_cache_ids),
        "source_observation_count": len(opportunity.source_observations),
        "preferred_source_id": (
            None
            if opportunity.preferred_source is None
            else opportunity.preferred_source.source_id
        ),
    }


def _metrics_payload(report) -> dict[str, object]:
    return {
        "schema_version": report.schema_version,
        "window": {
            "started_at": report.window.started_at,
            "ended_at": report.window.ended_at,
        },
        "funnel": asdict(report.funnel),
        "source_performance": [asdict(item) for item in report.source_performance],
        "unattributed_scheduled_deliveries": report.unattributed_scheduled_deliveries,
        "unattributed_sent_deliveries": report.unattributed_sent_deliveries,
        "evidence_tables": report.evidence_tables,
    }


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        asyncio.run(_dispatch(args))
    except Exception as exc:  # keep operator output bounded and secret-free
        print(
            json.dumps(
                {
                    "error_type": type(exc).__name__,
                    "error": str(exc)[:512],
                },
                ensure_ascii=False,
            ),
            file=sys.stderr,
        )
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
