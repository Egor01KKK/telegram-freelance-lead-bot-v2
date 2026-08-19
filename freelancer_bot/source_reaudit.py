from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from decimal import Decimal
import re
from typing import Protocol, runtime_checkable

from .config import RuntimeConfig
from .persistence.database import Database
from .persistence.source_metrics import (
    SourceMetricsRepository,
    SourceReauditCandidate,
)
from .persistence.source_repository import SourceRecord, SourceRepository
from .source_audit import SourceAuditPipeline, SourceAuditRunResult
from .source_audit_sampler import SourceAuditTarget


@dataclass(frozen=True)
class SourceReauditPolicy:
    degraded_cadence_days: int = 7
    high_activity_cadence_days: int = 7
    normal_cadence_days: int = 14
    quiet_cadence_days: int = 30
    high_activity_messages_per_day: Decimal | float | int = 50
    quiet_activity_messages_per_day: Decimal | float | int = 5

    def __post_init__(self) -> None:
        for field in (
            "degraded_cadence_days",
            "high_activity_cadence_days",
            "normal_cadence_days",
            "quiet_cadence_days",
        ):
            value = getattr(self, field)
            if not 7 <= value <= 30:
                raise ValueError(f"{field} must be between 7 and 30")
        high = Decimal(str(self.high_activity_messages_per_day))
        quiet = Decimal(str(self.quiet_activity_messages_per_day))
        if not high.is_finite() or not quiet.is_finite() or quiet < 0:
            raise ValueError("activity thresholds must be finite and nonnegative")
        if quiet >= high:
            raise ValueError(
                "quiet activity threshold must be below high activity threshold"
            )

    @classmethod
    def from_config(cls, config: RuntimeConfig) -> "SourceReauditPolicy":
        return cls(
            degraded_cadence_days=config.source_reaudit_degraded_cadence_days,
            high_activity_cadence_days=(
                config.source_reaudit_high_activity_cadence_days
            ),
            normal_cadence_days=config.source_reaudit_normal_cadence_days,
            quiet_cadence_days=config.source_reaudit_quiet_cadence_days,
            high_activity_messages_per_day=(
                config.source_reaudit_high_activity_messages_per_day
            ),
            quiet_activity_messages_per_day=(
                config.source_reaudit_quiet_activity_messages_per_day
            ),
        )

    def repository_arguments(self) -> dict[str, object]:
        return {
            "degraded_cadence": timedelta(days=self.degraded_cadence_days),
            "high_activity_cadence": timedelta(
                days=self.high_activity_cadence_days
            ),
            "normal_cadence": timedelta(days=self.normal_cadence_days),
            "quiet_cadence": timedelta(days=self.quiet_cadence_days),
            "high_activity_messages_per_day": self.high_activity_messages_per_day,
            "quiet_activity_messages_per_day": self.quiet_activity_messages_per_day,
        }


@runtime_checkable
class SourceReauditTargetResolver(Protocol):
    def resolve(self, source: SourceRecord) -> SourceAuditTarget | None: ...


class DefaultSourceReauditTargetResolver:
    def resolve(self, source: SourceRecord) -> SourceAuditTarget | None:
        lookup = source.handle or source.canonical_url
        if lookup is None and _safe_external_lookup(source.external_id):
            lookup = source.external_id
        if lookup is None:
            return None
        return SourceAuditTarget(
            source_id=source.id,
            platform=source.platform,
            lookup=lookup,
        )


@dataclass(frozen=True)
class SourceReauditFailure:
    source_id: int
    code: str


@dataclass(frozen=True)
class SourceReauditBatch:
    as_of: datetime
    due: tuple[SourceReauditCandidate, ...]
    completed: tuple[SourceAuditRunResult, ...]
    failures: tuple[SourceReauditFailure, ...]


class SourceReauditScheduler:
    def __init__(
        self,
        database: Database,
        audit_pipeline: SourceAuditPipeline,
        *,
        collector_account_id: int,
        policy: SourceReauditPolicy | None = None,
        target_resolver: SourceReauditTargetResolver | None = None,
        platform: str = "telegram",
        metrics: SourceMetricsRepository | None = None,
        sources: SourceRepository | None = None,
    ) -> None:
        if not isinstance(audit_pipeline, SourceAuditPipeline):
            raise TypeError("audit_pipeline must be SourceAuditPipeline")
        if collector_account_id <= 0:
            raise ValueError("collector_account_id must be positive")
        resolver = target_resolver or DefaultSourceReauditTargetResolver()
        if not isinstance(resolver, SourceReauditTargetResolver):
            raise TypeError("target_resolver must implement SourceReauditTargetResolver")
        normalized_platform = platform.strip().lower()
        if not normalized_platform:
            raise ValueError("platform must not be blank")
        self._database = database
        self._audit_pipeline = audit_pipeline
        self._collector_account_id = collector_account_id
        self._policy = policy or SourceReauditPolicy()
        self._target_resolver = resolver
        self._platform = normalized_platform
        self._metrics = metrics or SourceMetricsRepository()
        self._sources = sources or SourceRepository()

    async def run_once(
        self,
        *,
        as_of: datetime,
        limit: int = 100,
    ) -> SourceReauditBatch:
        if limit <= 0:
            raise ValueError("limit must be positive")
        async with self._database.connect() as connection:
            eligible = await self._sources.list_for_collector(
                connection,
                collector_account_id=self._collector_account_id,
                platform=self._platform,
            )
            eligible_by_id = {source.id: source for source in eligible}
            due = await self._metrics.list_due_for_periodic_reaudit(
                connection,
                eligible_source_ids=tuple(eligible_by_id),
                as_of=as_of,
                limit=limit,
                **self._policy.repository_arguments(),
            )

        completed: list[SourceAuditRunResult] = []
        failures: list[SourceReauditFailure] = []
        for candidate in due:
            source = eligible_by_id[candidate.source_id]
            target = self._target_resolver.resolve(source)
            if target is None:
                failures.append(
                    SourceReauditFailure(
                        source_id=source.id,
                        code="lookup_unavailable",
                    )
                )
                continue
            try:
                completed.append(
                    await self._audit_pipeline.re_audit(
                        target,
                        audited_at=as_of,
                    )
                )
            except Exception as exc:
                failures.append(
                    SourceReauditFailure(
                        source_id=source.id,
                        code=_failure_code(exc),
                    )
                )
        return SourceReauditBatch(
            as_of=as_of,
            due=tuple(due),
            completed=tuple(completed),
            failures=tuple(failures),
        )


def _safe_external_lookup(external_id: str) -> bool:
    lowered = external_id.lower()
    return not any(
        marker in lowered
        for marker in ("sha256", "invite_hash", "secret_hash")
    )


def _failure_code(error: Exception) -> str:
    name = re.sub(r"(?<!^)(?=[A-Z])", "_", type(error).__name__).lower()
    return re.sub(r"[^a-z0-9_.-]", "_", name)[:64] or "reaudit_error"
