from __future__ import annotations

from collections import defaultdict
from collections.abc import Mapping
from dataclasses import dataclass
from threading import Lock
from typing import Protocol


MetricTags = Mapping[str, str | int | bool]


class MetricNames:
    JOBS_CREATED = "jobs.created"
    JOBS_CLAIMED = "jobs.claimed"
    JOBS_COMPLETED = "jobs.completed"
    JOBS_RETRIED = "jobs.retried"
    JOBS_FAILED = "jobs.failed"
    JOBS_LEASE_RECLAIMED = "jobs.lease_reclaimed"
    JOB_PROCESSING_SECONDS = "jobs.processing_seconds"

    ACTIVE_SOURCES = "pipeline.active_sources"
    MESSAGES = "pipeline.messages"
    CANDIDATES = "pipeline.candidates"
    ANALYSES = "pipeline.analyses"
    OPPORTUNITIES = "pipeline.opportunities"
    MATCHES = "pipeline.matches"
    DELIVERIES = "pipeline.deliveries"
    FEEDBACK = "pipeline.feedback"

    MATCHING_BATCHES = "matching.batches"
    MATCHING_BATCH_FAILURES = "matching.batch_failures"
    MATCHING_BATCH_SECONDS = "matching.batch_seconds"
    MATCHING_OPPORTUNITIES = "matching.opportunities"
    MATCHING_ACTIVE_PROFILES = "matching.active_profiles"
    MATCHING_PAIRS_EVALUATED = "matching.pairs_evaluated"
    MATCHING_TRACES_CREATED = "matching.traces_created"
    MATCHING_TRACES_REUSED = "matching.traces_reused"
    MATCHING_USER_SPECIFIC_LLM_CALLS = "matching.user_specific_llm_calls"
    MATCHING_OPPORTUNITY_ANALYZER_CALLS = "matching.opportunity_analyzer_calls"

    DELIVERIES_SCHEDULED = "delivery.scheduled"
    DELIVERIES_REUSED = "delivery.reused"
    DELIVERY_SCHEDULE_FAILURES = "delivery.schedule_failures"
    DELIVERIES_SENT = "delivery.sent"
    DELIVERIES_RETRIED = "delivery.retried"
    DELIVERIES_FAILED = "delivery.failed"
    DELIVERIES_SUPPRESSED = "delivery.suppressed"
    DELIVERY_SEND_SECONDS = "delivery.send_seconds"
    DELIVERY_ACTIONS = "delivery.actions"
    DELIVERY_ACTIONS_REUSED = "delivery.actions_reused"


class MetricsSink(Protocol):
    def increment(self, name: str, value: int = 1, *, tags: MetricTags | None = None) -> None: ...

    def gauge(self, name: str, value: float, *, tags: MetricTags | None = None) -> None: ...

    def observe(self, name: str, value: float, *, tags: MetricTags | None = None) -> None: ...


class NoOpMetrics:
    def increment(self, name: str, value: int = 1, *, tags: MetricTags | None = None) -> None:
        pass

    def gauge(self, name: str, value: float, *, tags: MetricTags | None = None) -> None:
        pass

    def observe(self, name: str, value: float, *, tags: MetricTags | None = None) -> None:
        pass


@dataclass(frozen=True)
class MetricSnapshot:
    counters: dict[tuple[str, tuple[tuple[str, str], ...]], int]
    gauges: dict[tuple[str, tuple[tuple[str, str], ...]], float]
    observations: dict[tuple[str, tuple[tuple[str, str], ...]], tuple[float, ...]]


class InMemoryMetrics:
    def __init__(self) -> None:
        self._lock = Lock()
        self._counters: defaultdict[tuple[str, tuple[tuple[str, str], ...]], int] = defaultdict(int)
        self._gauges: dict[tuple[str, tuple[tuple[str, str], ...]], float] = {}
        self._observations: defaultdict[
            tuple[str, tuple[tuple[str, str], ...]], list[float]
        ] = defaultdict(list)

    def increment(self, name: str, value: int = 1, *, tags: MetricTags | None = None) -> None:
        with self._lock:
            self._counters[_key(name, tags)] += value

    def gauge(self, name: str, value: float, *, tags: MetricTags | None = None) -> None:
        with self._lock:
            self._gauges[_key(name, tags)] = value

    def observe(self, name: str, value: float, *, tags: MetricTags | None = None) -> None:
        with self._lock:
            self._observations[_key(name, tags)].append(value)

    def counter(self, name: str, *, tags: MetricTags | None = None) -> int:
        with self._lock:
            return self._counters[_key(name, tags)]

    def observations(self, name: str, *, tags: MetricTags | None = None) -> tuple[float, ...]:
        with self._lock:
            return tuple(self._observations[_key(name, tags)])

    def snapshot(self) -> MetricSnapshot:
        with self._lock:
            return MetricSnapshot(
                counters=dict(self._counters),
                gauges=dict(self._gauges),
                observations={key: tuple(values) for key, values in self._observations.items()},
            )


def _key(name: str, tags: MetricTags | None) -> tuple[str, tuple[tuple[str, str], ...]]:
    normalized = tuple(sorted((str(key), str(value)) for key, value in (tags or {}).items()))
    return name, normalized
