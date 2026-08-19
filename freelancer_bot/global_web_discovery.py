"""Bounded Web acquisition for the Global Source Library."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import datetime
import hashlib
import re
from typing import Any
from urllib.parse import urlsplit
from uuid import UUID

from .discovery import DiscoveredSourceCandidate, DiscoveryRequest
from .global_source_library import GlobalDiscoveryQuery, prioritize_candidate
from .telegram_references import InvalidTelegramReference, normalize_telegram_reference
from .web_discovery import WebDiscoveryGovernor, WebSearchBackend, WebSearchBackendError, WebSearchResult
from .web_page_extraction import (
    ExtractedTelegramLink,
    PageFetchError,
    SafeWebPageFetcher,
    UnsafePageURL,
)


@dataclass
class _ObservedReference:
    reference_key: str
    url: str
    display_name: str
    query: GlobalDiscoveryQuery
    extraction_kind: str
    result_domain: str
    independent_evidence_key: str
    page_url: str | None = None
    profile_gap_keys: tuple[str, ...] = ()
    independent_domains: set[str] = field(default_factory=set)
    independent_pages: set[str] = field(default_factory=set)
    evidence_items: list[dict[str, object]] = field(default_factory=list)


class GlobalWebDiscoveryProvider:
    """Search, optionally inspect one public page, and return source candidates."""

    name = "web_global_source_library"
    kind = "web"

    def __init__(
        self,
        backends: Sequence[WebSearchBackend],
        *,
        governor: WebDiscoveryGovernor,
        queries: Sequence[GlobalDiscoveryQuery],
        results_per_query: int = 10,
        max_candidates: int = 100,
        max_page_fetches: int = 100,
        page_fetcher: SafeWebPageFetcher | None = None,
        campaign_id: UUID | None = None,
        before_backend_request: Callable[
            [WebSearchBackend, GlobalDiscoveryQuery, int], Awaitable[None]
        ] | None = None,
        provider_costs_usd: Mapping[str, float] | None = None,
    ) -> None:
        if not backends:
            raise ValueError("at least one Web backend is required")
        if not 1 <= results_per_query <= 50 or not 1 <= max_candidates <= 1000:
            raise ValueError("Web discovery bounds are invalid")
        self._backends = tuple(backends)
        self._governor = governor
        self._queries = tuple(queries)
        self._results_per_query = results_per_query
        self._max_candidates = max_candidates
        self._max_page_fetches = max_page_fetches
        self._page_fetcher = page_fetcher or SafeWebPageFetcher()
        self._campaign_id = campaign_id
        self._before_backend_request = before_backend_request
        self._provider_costs_usd = {
            str(key): float(value)
            for key, value in (provider_costs_usd or {}).items()
        }
        self._observability: dict[str, object] = {
            "queries_executed": 0,
            "search_results_considered": 0,
            "direct_telegram_references": 0,
            "page_fetches": 0,
            "page_extracted_telegram_references": 0,
            "unique_candidates": 0,
            "backend_failures": 0,
            "backend_attempts": 0,
            "backend_successes": 0,
            "failure_classes": {},
            "provider_health": {},
            "provider_usage": {},
            "candidate_funnel": _empty_candidate_funnel(),
            "provider_state": "READY",
        }

    @property
    def observability(self) -> Mapping[str, object]:
        return dict(self._observability)

    async def discover(self, request: DiscoveryRequest) -> Sequence[DiscoveredSourceCandidate]:
        observed: dict[str, _ObservedReference] = {}
        page_fetches = 0
        for query in self._queries:
            self._observability["queries_executed"] = int(self._observability["queries_executed"]) + 1
            results = await self._search_with_fallback(query)
            for result in results:
                self._observability["search_results_considered"] = int(self._observability["search_results_considered"]) + 1
                direct = _parse_source(result.url)
                if direct is not None:
                    self._record_valid_reference(direct, "direct_result")
                    self._observability["direct_telegram_references"] = int(self._observability["direct_telegram_references"]) + 1
                    _merge_observed(
                        observed,
                        direct.source_key,
                        _observed_from_reference(
                            direct,
                            query,
                            result.url,
                            result.title,
                            "direct_result",
                            result.url,
                        ),
                    )
                elif _looks_like_telegram_reference(result.url):
                    self._record_invalid_reference(result.url, "INVALID_SOURCE_TYPE")
                elif page_fetches < self._max_page_fetches:
                    page_fetches += 1
                    self._observability["page_fetches"] = page_fetches
                    remaining_candidates = self._max_candidates - len(observed)
                    if remaining_candidates <= 0:
                        break
                    try:
                        links = await asyncio.to_thread(
                            self._page_fetcher.extract_telegram_links,
                            result_url=result.url,
                            page_url=result.url,
                            max_links=remaining_candidates,
                        )
                    except UnsafePageURL:
                        self._record_failure("unsafe_page_url")
                        continue
                    except PageFetchError as exc:
                        self._record_failure(exc.failure_class)
                        continue
                    for link in links:
                        self._record_valid_reference(link.reference, "page_extracted")
                        self._observability["page_extracted_telegram_references"] = int(self._observability["page_extracted_telegram_references"]) + 1
                        _merge_observed(
                            observed,
                            link.reference.source_key,
                            _observed_from_extracted(link, query),
                        )
                        if len(observed) >= self._max_candidates:
                            break
                if len(observed) >= self._max_candidates:
                    break
            if len(observed) >= self._max_candidates:
                break
        self._observability["unique_candidates"] = len(observed)
        self._observability["provider_state"] = self._governor.observability().get("state", "READY")
        self._observability["provider_health"] = self._governor.observability().get("backend_health", {})
        funnel = self._observability["candidate_funnel"]
        if isinstance(funnel, dict):
            funnel["NORMALIZED_UNIQUE"] = len(observed)
        candidates: list[DiscoveredSourceCandidate] = []
        for item in observed.values():
            reference = normalize_telegram_reference(item.url)
            external_id = (
                f"username:{reference.handle}"
                if reference.handle is not None
                else reference.source_key
            )
            context = {
                "discovery_method": "global_web_source_library",
                "query_family": item.query.family.value,
                "query_key": item.query.normalized_query_key,
                "query_sha256": hashlib.sha256(item.query.text.encode()).hexdigest(),
                "result_domain": item.result_domain,
                "extraction_kind": item.extraction_kind,
                "independent_evidence_key": item.independent_evidence_key,
                "direct_telegram_result": item.extraction_kind == "direct_result",
                # Keep a URL-shaped reference for invite/numeric peers too;
                # their normalized source keys (``invite:…``/``peer:…``) are
                # intentionally not URLs and cannot be fed to the local alias
                # parser as if they were one.
                "telegram_reference": reference.raw,
                "profile_gap_keys": list(item.profile_gap_keys),
                "page_url": item.page_url,
                "independent_domains": len(item.independent_domains),
                "independent_result_pages": len(item.independent_pages),
                "evidence_items": item.evidence_items[:50],
            }
            context["candidate_priority"] = prioritize_candidate(context).value
            if self._campaign_id is not None:
                context["campaign_id"] = str(self._campaign_id)
            candidates.append(
                DiscoveredSourceCandidate(
                    result_key=f"global-web:{reference.source_key}",
                    platform="telegram",
                    external_id=external_id,
                    access_type="private" if reference.is_invite else "public",
                    display_name=item.display_name or f"Telegram {reference.handle or reference.source_key}",
                    handle=None if reference.handle is None else f"@{reference.handle}",
                    canonical_url=(
                        f"https://t.me/{reference.handle}"
                        if reference.handle is not None
                        else None
                    ),
                    discovered_at=request.requested_at,
                    seed_reference=item.query.text,
                    context=context,
                )
            )
        return tuple(candidates)

    async def _search_with_fallback(self, query: GlobalDiscoveryQuery) -> tuple[WebSearchResult, ...]:
        last_failure: WebSearchBackendError | None = None
        for backend in self._backends:
            backend_id = getattr(backend, "health_identity", backend.__class__.__name__)
            self._record_provider_attempt(str(backend_id))
            try:
                if self._before_backend_request is not None:
                    await self._before_backend_request(backend, query, self._results_per_query)
                results = tuple(
                    await self._governor.search(
                        backend,
                        query.text,
                        language=query.language,
                        limit=self._results_per_query,
                    )
                )
                self._observability["backend_successes"] = int(self._observability["backend_successes"]) + 1
                self._record_provider_success(str(backend_id))
                return results
            except WebSearchBackendError as exc:
                last_failure = exc
                self._record_failure(exc.failure_class)
                self._record_provider_failure(str(backend_id))
                self._observability["provider_health"] = self._governor.observability().get("backend_health", {})
        if last_failure is not None:
            raise WebSearchBackendError(
                "all configured Web Discovery backends failed",
                failure_class="all_backends_failed",
            ) from last_failure
        return ()

    def _record_provider_attempt(self, backend_id: str) -> None:
        self._observability["backend_attempts"] = int(self._observability["backend_attempts"]) + 1
        usage = self._observability["provider_usage"]
        if not isinstance(usage, dict):
            usage = {}
            self._observability["provider_usage"] = usage
        entry = usage.setdefault(
            backend_id,
            {"requests_attempted": 0, "requests_successful": 0, "provider_failures": 0},
        )
        if isinstance(entry, dict):
            entry["requests_attempted"] = int(entry.get("requests_attempted", 0)) + 1
            entry["estimated_cost_usd"] = round(
                int(entry["requests_attempted"]) * self._provider_costs_usd.get(backend_id, 0.0),
                9,
            )

    def _record_provider_success(self, backend_id: str) -> None:
        usage = self._observability["provider_usage"]
        entry = usage.get(backend_id) if isinstance(usage, dict) else None
        if isinstance(entry, dict):
            entry["requests_successful"] = int(entry.get("requests_successful", 0)) + 1

    def _record_provider_failure(self, backend_id: str) -> None:
        usage = self._observability["provider_usage"]
        entry = usage.get(backend_id) if isinstance(usage, dict) else None
        if isinstance(entry, dict):
            entry["provider_failures"] = int(entry.get("provider_failures", 0)) + 1

    def _record_failure(self, failure_class: str) -> None:
        self._observability["backend_failures"] = int(self._observability["backend_failures"]) + 1
        failures = self._observability["failure_classes"]
        if isinstance(failures, dict):
            failures[failure_class] = failures.get(failure_class, 0) + 1

    def _record_valid_reference(self, reference: Any, extraction_kind: str) -> None:
        funnel = self._observability["candidate_funnel"]
        if not isinstance(funnel, dict):
            return
        funnel["RAW_TELEGRAM_REFERENCES"] = int(funnel.get("RAW_TELEGRAM_REFERENCES", 0)) + 1
        funnel["LOCAL_STRUCTURALLY_VALID"] = int(funnel.get("LOCAL_STRUCTURALLY_VALID", 0)) + 1
        funnel["NORMALIZED_REFERENCES"] = int(funnel.get("NORMALIZED_REFERENCES", 0)) + 1
        reference_hash = hashlib.sha256(reference.source_key.encode("utf-8")).hexdigest()
        observations = funnel.get("reference_observations")
        if not isinstance(observations, list):
            return
        existing = next(
            (item for item in observations if item.get("reference_sha256") == reference_hash),
            None,
        )
        if existing is not None:
            existing["occurrence_count"] = int(existing.get("occurrence_count", 1)) + 1
            return
        if len(observations) < 1000:
            observations.append(
                {
                    "reference_sha256": reference_hash,
                    "extraction_kind": extraction_kind,
                    "occurrence_count": 1,
                }
            )

    def _record_invalid_reference(self, raw_url: str, bucket: str) -> None:
        funnel = self._observability["candidate_funnel"]
        if not isinstance(funnel, dict):
            return
        funnel["RAW_TELEGRAM_REFERENCES"] = int(funnel.get("RAW_TELEGRAM_REFERENCES", 0)) + 1
        funnel["LOCAL_REJECTED"] = int(funnel.get("LOCAL_REJECTED", 0)) + 1
        reference_hash = hashlib.sha256(raw_url.strip().casefold().encode("utf-8")).hexdigest()
        observations = funnel.get("reference_observations")
        if isinstance(observations, list) and len(observations) < 1000:
            observations.append(
                {
                    "reference_sha256": reference_hash,
                    "extraction_kind": "direct_result",
                    "occurrence_count": 1,
                    "terminal_bucket": bucket,
                }
            )

    def finalize_candidate_funnel(
        self,
        classifications: Mapping[str, Mapping[str, object]],
    ) -> None:
        funnel = self._observability["candidate_funnel"]
        if not isinstance(funnel, dict):
            return
        observations = funnel.get("reference_observations")
        if not isinstance(observations, list):
            return
        terminal = funnel.setdefault("terminal_buckets", {})
        if not isinstance(terminal, dict):
            terminal = {}
            funnel["terminal_buckets"] = terminal
        for item in observations:
            if item.get("terminal_bucket"):
                continue
            reference_hash = str(item.get("reference_sha256", ""))
            resolution = classifications.get(reference_hash, {})
            bucket = str(resolution.get("bucket") or "OTHER_REJECTION")
            item["terminal_bucket"] = bucket
            reason = resolution.get("reason")
            if isinstance(reason, str) and reason:
                item["reason"] = reason[:128]
            terminal[bucket] = int(terminal.get(bucket, 0)) + int(item.get("occurrence_count", 1))


def _empty_candidate_funnel() -> dict[str, object]:
    return {
        "version": "candidate-funnel.v1",
        "RAW_TELEGRAM_REFERENCES": 0,
        "LOCAL_STRUCTURALLY_VALID": 0,
        "LOCAL_REJECTED": 0,
        "NORMALIZED_REFERENCES": 0,
        "NORMALIZED_UNIQUE": 0,
        "ALREADY_APPROVED_SOURCE": 0,
        "ALREADY_EXISTING_CANDIDATE": 0,
        "ALIAS_OF_EXISTING_SOURCE": 0,
        "PREVIOUSLY_REJECTED": 0,
        "INVALID_SOURCE_TYPE": 0,
        "OTHER_REJECTION": 0,
        "GENUINELY_NEW": 0,
        "PERSISTED_NEW": 0,
        "terminal_buckets": {},
        "reference_observations": [],
    }


def _looks_like_telegram_reference(url: str) -> bool:
    try:
        host = (urlsplit(url).hostname or "").casefold().removeprefix("www.")
    except ValueError:
        return False
    return host in {"t.me", "telegram.me", "telegram.dog"}


def _parse_source(url: str):
    try:
        return normalize_telegram_reference(url)
    except InvalidTelegramReference:
        return None


def _observed_from_reference(reference, query: GlobalDiscoveryQuery, url: str, title: str, extraction_kind: str, result_url: str) -> _ObservedReference:
    domain = (urlsplit(result_url).hostname or "").casefold()
    evidence_key = _evidence_key(domain, result_url, reference.source_key)
    return _ObservedReference(
        reference_key=reference.source_key,
        url=url,
        display_name=title.strip()[:255],
        query=query,
        extraction_kind=extraction_kind,
        result_domain=domain,
        independent_evidence_key=evidence_key,
        profile_gap_keys=(),
        independent_domains={domain} if domain else set(),
        independent_pages={result_url},
        evidence_items=[_evidence_item(query, extraction_kind, evidence_key, domain)],
    )


def _observed_from_extracted(link: ExtractedTelegramLink, query: GlobalDiscoveryQuery) -> _ObservedReference:
    reference = link.reference
    evidence_key = _evidence_key(link.domain, link.page_url, reference.source_key)
    return _ObservedReference(
        reference_key=reference.source_key,
        # ``normalized`` is a source key for numeric peers (``peer:-100…``),
        # not a URL.  Keep the validated original URL for the later provider
        # normalization pass while using ``source_key`` for deduplication.
        url=reference.raw,
        display_name="",
        query=query,
        extraction_kind="page_extracted",
        result_domain=link.domain,
        independent_evidence_key=evidence_key,
        page_url=link.page_url,
        independent_domains={link.domain} if link.domain else set(),
        independent_pages={link.page_url},
        evidence_items=[_evidence_item(query, "page_extracted", evidence_key, link.domain)],
    )


def _merge_observed(
    observed: dict[str, _ObservedReference],
    key: str,
    incoming: _ObservedReference,
) -> None:
    existing = observed.get(key)
    if existing is None:
        observed[key] = incoming
        return
    existing.independent_domains.update(incoming.independent_domains)
    existing.independent_pages.update(incoming.independent_pages)
    known_keys = {
        str(item.get("independent_evidence_key"))
        for item in existing.evidence_items
        if item.get("independent_evidence_key")
    }
    for item in incoming.evidence_items:
        item_key = str(item.get("independent_evidence_key") or "")
        if item_key and item_key not in known_keys:
            existing.evidence_items.append(item)
            known_keys.add(item_key)


def _evidence_key(domain: str, page_url: str, source_key: str) -> str:
    page_digest = hashlib.sha256(page_url.encode("utf-8")).hexdigest()[:16]
    return f"{domain or 'unknown'}:{page_digest}:{source_key}"[:255]


def _evidence_item(
    query: GlobalDiscoveryQuery,
    extraction_kind: str,
    independent_evidence_key: str,
    result_domain: str,
) -> dict[str, object]:
    return {
        "query_family": query.family.value,
        "query_key": query.normalized_query_key,
        "query_sha256": hashlib.sha256(query.text.encode("utf-8")).hexdigest(),
        "extraction_kind": extraction_kind,
        "independent_evidence_key": independent_evidence_key,
        "result_domain": result_domain,
        "profile_gap_keys": (),
    }
