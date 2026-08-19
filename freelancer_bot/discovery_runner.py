from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timezone
import hashlib
import re
from typing import Any
from uuid import UUID

from .discovery import (
    DiscoveredSourceCandidate,
    DiscoveryProvider,
    DiscoveryRequest,
    normalize_provider_kind,
    normalize_provider_name,
)
from .persistence.database import Database
from .persistence.discovery import (
    DiscoveryResultOutcome,
    DiscoveryResultRecord,
    DiscoveryRunRecord,
    DiscoveryRunRepository,
)
from .persistence.source_repository import (
    SourceIdentityConflict,
    SourceRepository,
)
from .persistence.discovery_campaigns import DiscoveryCampaignRepository
from .telegram_references import InvalidTelegramReference, normalize_telegram_reference


class DiscoveryExecutionError(RuntimeError):
    def __init__(self, run_id: UUID, failure_code: str):
        super().__init__(f"Discovery run {run_id} failed with code {failure_code}")
        self.run_id = run_id
        self.failure_code = failure_code


@dataclass(frozen=True)
class DiscoveryExecution:
    run: DiscoveryRunRecord
    results: tuple[DiscoveryResultRecord, ...]


class DiscoveryRunner:
    def __init__(
        self,
        database: Database,
        *,
        runs: DiscoveryRunRepository | None = None,
        sources: SourceRepository | None = None,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        self._database = database
        self._runs = runs or DiscoveryRunRepository()
        self._sources = sources or SourceRepository()
        self._library = DiscoveryCampaignRepository()
        self._clock = clock or (lambda: datetime.now(timezone.utc))

    async def run(
        self,
        provider: DiscoveryProvider,
        *,
        run_key: str,
        request: DiscoveryRequest,
    ) -> DiscoveryExecution:
        provider_name = normalize_provider_name(provider.name)
        provider_kind = normalize_provider_kind(provider.kind)
        campaign_id = _optional_uuid(request.parameters.get("campaign_id"))
        started_at = self._now()
        async with self._database.transaction() as connection:
            started = await self._runs.start(
                connection,
                provider=provider_name,
                provider_kind=provider_kind,
                run_key=run_key,
                request=request.to_payload(),
                started_at=started_at,
            )
            if not started.created:
                results = await self._runs.list_results(connection, started.run.id)
                return DiscoveryExecution(started.run, tuple(results))

        try:
            candidates = _candidate_batch(await provider.discover(request))
        except Exception as exc:
            observability = _provider_observability(provider)
            _finalize_provider_funnel(
                provider,
                {
                    str(item.get("reference_sha256")): {
                        "bucket": "OTHER_REJECTION",
                        "reason": _failure_code("provider", exc),
                    }
                    for item in _funnel_observations(observability)
                    if item.get("reference_sha256")
                },
            )
            observability = _provider_observability(provider)
            raise await self._fail(
                started.run.id,
                "provider",
                exc,
                observability=observability,
            ) from exc

        try:
            async with self._database.transaction() as connection:
                results: list[DiscoveryResultRecord] = []
                created_count = 0
                funnel_classifications: dict[str, dict[str, object]] = {}
                for candidate in candidates:
                    reference_hash = _candidate_reference_hash(candidate)
                    classification: dict[str, object] = {
                        "bucket": "OTHER_REJECTION",
                        "reason": "candidate_not_materialized",
                    }
                    if candidate.platform != "telegram":
                        classification = {
                            "bucket": "INVALID_SOURCE_TYPE",
                            "reason": "unsupported_platform",
                        }
                    existing = await self._sources.get_by_identity(
                        connection,
                        platform=candidate.platform,
                        external_id=candidate.external_id,
                    )
                    aliases = _candidate_telegram_aliases(candidate)
                    alias_match = False
                    if existing is None and aliases:
                        for alias, _kind in aliases:
                            alias_source_id = await self._library.source_for_alias(
                                connection,
                                platform=candidate.platform,
                                normalized_reference=alias,
                            )
                            if alias_source_id is not None:
                                existing = await self._sources.get(
                                    connection, int(alias_source_id)
                                )
                                alias_match = True
                                break
                    if existing is None:
                        canonical_peer_identity = _candidate_peer_identity(candidate)
                        if canonical_peer_identity:
                            peer_source_id = await self._library.source_for_canonical_peer(
                                connection,
                                platform=candidate.platform,
                                canonical_peer_identity=canonical_peer_identity,
                            )
                            if peer_source_id is not None:
                                existing = await self._sources.get(
                                    connection, int(peer_source_id)
                                )
                                alias_match = True
                    created = existing is None
                    if existing is None:
                        try:
                            source = await self._sources.create_candidate(
                                connection,
                                platform=candidate.platform,
                                external_id=candidate.external_id,
                                access_type=candidate.access_type,
                                display_name=candidate.display_name,
                                provider=provider_name,
                                lineage_key=candidate.result_key,
                                handle=candidate.handle,
                                canonical_url=candidate.canonical_url,
                                provider_run_id=str(started.run.id),
                                discovery_run_id=started.run.id,
                                seed_source_id=candidate.seed_source_id,
                                seed_reference=candidate.seed_reference,
                                discovered_at=candidate.discovered_at,
                                context=candidate.context,
                            )
                        except SourceIdentityConflict:
                            source = await self._sources.get_by_identity(
                                connection,
                                platform=candidate.platform,
                                external_id=candidate.external_id,
                            )
                            if source is None:
                                raise
                            created = False
                            existing = source
                    else:
                        source = existing
                        await self._sources.record_lineage(
                            connection,
                            source_id=source.id,
                            provider=provider_name,
                            lineage_key=candidate.result_key,
                            provider_run_id=str(started.run.id),
                            discovery_run_id=started.run.id,
                            seed_source_id=candidate.seed_source_id,
                            seed_reference=candidate.seed_reference,
                            discovered_at=candidate.discovered_at,
                            context=candidate.context,
                        )

                    if candidate.platform == "telegram":
                        if created:
                            classification = {
                                "bucket": "PERSISTED_NEW",
                                "genuinely_new": True,
                            }
                        else:
                            status = getattr(existing.lifecycle_status, "value", str(existing.lifecycle_status))
                            if status in {"approved", "active"}:
                                bucket = "ALREADY_APPROVED_SOURCE"
                            elif status == "rejected":
                                bucket = "PREVIOUSLY_REJECTED"
                            elif alias_match:
                                bucket = "ALIAS_OF_EXISTING_SOURCE"
                            else:
                                bucket = "ALREADY_EXISTING_CANDIDATE"
                            classification = {"bucket": bucket}
                    if reference_hash:
                        funnel_classifications[reference_hash] = classification

                    for alias, kind in aliases:
                        await self._library.record_alias(
                            connection,
                            source_id=source.id,
                            platform=candidate.platform,
                            normalized_reference=alias,
                            reference_kind=kind,
                            canonical_peer_identity=_candidate_peer_identity(candidate),
                            seen_at=candidate.discovered_at,
                        )
                    await self._record_library_evidence(
                        connection,
                        candidate=candidate,
                        provider=provider_name,
                        provider_kind=provider_kind,
                        run_id=started.run.id,
                        source_id=source.id,
                        campaign_id=campaign_id,
                    )

                    result = await self._runs.record_result(
                        connection,
                        run_id=started.run.id,
                        candidate=candidate,
                        source_id=source.id,
                        outcome=(
                            DiscoveryResultOutcome.CREATED
                            if created
                            else DiscoveryResultOutcome.EXISTING
                        ),
                    )
                    results.append(result)
                    if created:
                        created_count += 1

                observability = _provider_observability(provider)
                if observability is not None:
                    _finalize_provider_funnel(provider, funnel_classifications)
                    observability = _provider_observability(provider)
                    if observability is not None:
                        observability["candidate_sources_created"] = created_count
                    request_payload = dict(started.run.request)
                    request_payload["observability"] = observability
                    await self._runs.update_request(
                        connection,
                        run_id=started.run.id,
                        request=request_payload,
                    )

                completed = await self._runs.complete(
                    connection,
                    run_id=started.run.id,
                    result_count=len(results),
                    finished_at=self._now(not_before=started.run.started_at),
                )
                return DiscoveryExecution(completed, tuple(results))
        except Exception as exc:
            raise await self._fail(started.run.id, "persistence", exc) from exc

    async def _fail(
        self,
        run_id: UUID,
        stage: str,
        error: Exception,
        *,
        observability: Mapping[str, Any] | None = None,
    ) -> DiscoveryExecutionError:
        failure_code = _failure_code(stage, error)
        async with self._database.transaction() as connection:
            run = await self._runs.get(connection, run_id)
            if observability is not None:
                request_payload = dict(run.request)
                request_payload["observability"] = dict(observability)
                await self._runs.update_request(
                    connection,
                    run_id=run_id,
                    request=request_payload,
                )
            await self._runs.fail(
                connection,
                run_id=run_id,
                failure_code=failure_code,
                finished_at=self._now(not_before=run.started_at),
            )
        return DiscoveryExecutionError(run_id, failure_code)

    def _now(self, *, not_before: datetime | None = None) -> datetime:
        value = self._clock()
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("DiscoveryRunner clock must return a timezone-aware time")
        if not_before is not None and value < not_before:
            return not_before
        return value

    async def _record_library_evidence(
        self,
        connection: Any,
        *,
        candidate: DiscoveredSourceCandidate,
        provider: str,
        provider_kind: str,
        run_id: UUID,
        source_id: int,
        campaign_id: UUID | None = None,
    ) -> None:
        context = candidate.context
        extraction_kind = "source_graph" if (
            "graph" in provider_kind
            or "graph" in str(context.get("discovery_method", "")).casefold()
        ) else "direct_result"
        if context.get("extraction_kind") in {
            "page_extracted",
            "source_graph",
            "global_search",
            "operator",
        }:
            extraction_kind = str(context["extraction_kind"])
        independent_key = str(
            context.get("independent_evidence_key")
            or context.get("result_domain")
            or candidate.result_key
        )[:255]
        evidence_items = context.get("evidence_items")
        if not isinstance(evidence_items, Sequence) or isinstance(evidence_items, (str, bytes)):
            evidence_items = (context,)
        for item in evidence_items:
            if not isinstance(item, Mapping):
                continue
            item_key = str(
                item.get("independent_evidence_key")
                or item.get("result_domain")
                or independent_key
            )[:255]
            await self._library.record_evidence(
                connection,
                source_id=source_id,
                campaign_id=campaign_id,
                provider=provider,
                provider_kind=provider_kind,
                independent_evidence_key=item_key,
                extraction_kind=str(item.get("extraction_kind") or extraction_kind),
                discovery_run_id=run_id,
                query_family=(str(item["query_family"]) if item.get("query_family") is not None else None),
                query_key=(str(item["query_key"]) if item.get("query_key") is not None else None),
                query_sha256=(str(item["query_sha256"]) if item.get("query_sha256") is not None else None),
                result_domain=(str(item["result_domain"]) if item.get("result_domain") is not None else None),
                profile_gap_keys=tuple(
                    str(value)
                    for value in item.get("profile_gap_keys", ())
                    if isinstance(value, str)
                ),
                source_graph_provenance=(
                    item.get("source_graph_provenance")
                    if isinstance(item.get("source_graph_provenance"), Mapping)
                    else None
                ),
            )


def _candidate_batch(
    values: Sequence[DiscoveredSourceCandidate],
) -> tuple[DiscoveredSourceCandidate, ...]:
    candidates = tuple(values)
    if any(not isinstance(candidate, DiscoveredSourceCandidate) for candidate in candidates):
        raise TypeError("Discovery providers must return normalized source candidates")
    keys = [candidate.result_key for candidate in candidates]
    if len(keys) != len(set(keys)):
        raise ValueError("Discovery provider returned duplicate result keys")
    return candidates


def _candidate_telegram_aliases(
    candidate: DiscoveredSourceCandidate,
) -> tuple[tuple[str, str], ...]:
    if candidate.platform != "telegram":
        return ()
    values: list[tuple[str, str]] = []
    raw_values: list[tuple[str, str]] = []
    if candidate.canonical_url:
        raw_values.append((candidate.canonical_url, "source"))
    if candidate.handle:
        raw_values.append((f"https://t.me/{candidate.handle.lstrip('@')}", "username"))
    if candidate.external_id.startswith("username:"):
        raw_values.append(
            (
                f"https://t.me/{candidate.external_id.split(':', 1)[1]}",
                "username",
            )
        )
    raw_context_reference = candidate.context.get("telegram_reference")
    if isinstance(raw_context_reference, str):
        raw_values.append((raw_context_reference, "source"))
    for raw, fallback_kind in raw_values:
        try:
            reference = normalize_telegram_reference(raw)
        except InvalidTelegramReference:
            continue
        kind = fallback_kind if fallback_kind == "username" else reference.reference_kind
        values.append((reference.source_key, kind))
    return tuple(dict.fromkeys(values))


def _candidate_peer_identity(candidate: DiscoveredSourceCandidate) -> str | None:
    for key in ("canonical_peer_identity", "telegram_peer_id", "peer_id"):
        value = candidate.context.get(key)
        if value is not None and str(value).strip():
            return str(value).strip()[:255]
    return None


def _failure_code(stage: str, error: Exception) -> str:
    error_name = re.sub(
        r"(?<!^)(?=[A-Z])",
        "_",
        error.__class__.__name__,
    ).lower()
    normalized = re.sub(r"[^a-z0-9_.-]", "_", f"{stage}.{error_name}")
    return normalized[:64]


def _optional_uuid(value: Any) -> UUID | None:
    if value is None:
        return None
    try:
        return UUID(str(value))
    except (ValueError, TypeError, AttributeError):
        raise ValueError("discovery campaign_id must be a UUID") from None


def _provider_observability(
    provider: DiscoveryProvider,
) -> dict[str, Any] | None:
    value = getattr(provider, "observability", None)
    if value is None:
        return None
    if not isinstance(value, Mapping):
        raise TypeError("Discovery provider observability must be a mapping")
    payload: dict[str, Any] = {}
    for key, item in value.items():
        if not isinstance(key, str) or not key:
            raise TypeError("Discovery provider observability keys must be non-empty strings")
        payload[key] = _safe_observability_value(item)
    return payload


def _funnel_observations(observability: Mapping[str, Any] | None) -> tuple[Mapping[str, Any], ...]:
    if not isinstance(observability, Mapping):
        return ()
    funnel = observability.get("candidate_funnel")
    if not isinstance(funnel, Mapping):
        return ()
    observations = funnel.get("reference_observations")
    if not isinstance(observations, (list, tuple)):
        return ()
    return tuple(item for item in observations if isinstance(item, Mapping))


def _finalize_provider_funnel(
    provider: DiscoveryProvider,
    classifications: Mapping[str, Mapping[str, object]],
) -> None:
    finalize = getattr(provider, "finalize_candidate_funnel", None)
    if callable(finalize):
        finalize(classifications)


def _candidate_reference_hash(candidate: DiscoveredSourceCandidate) -> str | None:
    raw = candidate.context.get("telegram_reference")
    if not isinstance(raw, str) or not raw.strip():
        raw = candidate.external_id
    try:
        reference = normalize_telegram_reference(raw)
        value = reference.source_key
    except InvalidTelegramReference:
        value = str(raw).strip().casefold()
    return hashlib.sha256(value.encode("utf-8")).hexdigest() if value else None


def _safe_observability_value(value: Any, *, depth: int = 0) -> Any:
    if depth > 4:
        raise TypeError("Discovery provider observability is too deeply nested")
    if value is None or isinstance(value, (bool, int, float, str)):
        return value
    if isinstance(value, Mapping):
        result: dict[str, Any] = {}
        for key, item in value.items():
            if not isinstance(key, str) or not key:
                raise TypeError(
                    "Discovery provider observability mapping keys must be strings"
                )
            result[key] = _safe_observability_value(item, depth=depth + 1)
        return result
    if isinstance(value, (list, tuple)):
        if len(value) > 1000:
            raise TypeError("Discovery provider observability list is too large")
        return [
            _safe_observability_value(item, depth=depth + 1)
            for item in value
        ]
    raise TypeError("Discovery provider observability contains an unsafe value")
