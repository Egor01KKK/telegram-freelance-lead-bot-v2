from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Any
from uuid import UUID

import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncConnection

from .database import Database
from .schema import (
    collector_accounts,
    source_collector_access,
    source_discovery_lineage,
    source_lifecycle_events,
    sources,
)


class SourceStatus(str, Enum):
    CANDIDATE = "candidate"
    APPROVED = "approved"
    ACTIVE = "active"
    DEGRADED = "degraded"
    PAUSED = "paused"
    REJECTED = "rejected"
    NEEDS_REVIEW = "needs_review"
    REVIEW_REQUIRED = "review_required"
    RETIRED = "retired"


class SourceNotFound(LookupError):
    pass


class SourceIdentityConflict(RuntimeError):
    pass


class InvalidSourceTransition(ValueError):
    pass


@dataclass(frozen=True)
class SourceRecord:
    id: int
    platform: str
    external_id: str
    access_type: str
    lifecycle_status: SourceStatus
    display_name: str
    handle: str | None
    canonical_url: str | None
    created_at: datetime
    updated_at: datetime


@dataclass(frozen=True)
class SourceLineageRecord:
    id: int
    source_id: int
    provider: str
    lineage_key: str
    provider_run_id: str | None
    discovery_run_id: UUID | None
    seed_source_id: int | None
    seed_reference: str | None
    discovered_at: datetime
    context: Mapping[str, Any]


@dataclass(frozen=True)
class SourceLifecycleEvent:
    id: int
    source_id: int
    from_status: SourceStatus | None
    to_status: SourceStatus
    actor_kind: str
    actor_id: str | None
    reason: str
    is_override: bool
    source_audit_id: UUID | None
    changed_at: datetime


@dataclass(frozen=True)
class SeedSource:
    platform: str
    external_id: str
    access_type: str
    initial_status: SourceStatus
    display_name: str
    handle: str | None
    canonical_url: str | None
    provider: str
    lineage_key: str
    provider_run_id: str
    seed_reference: str
    context: Mapping[str, Any]


@dataclass(frozen=True)
class SeedUpsertOutcome:
    source: SourceRecord
    created: bool
    updated: bool
    lineage_created: bool


_VALID_TRANSITIONS: dict[SourceStatus, frozenset[SourceStatus]] = {
    SourceStatus.CANDIDATE: frozenset(
        {SourceStatus.APPROVED, SourceStatus.REJECTED, SourceStatus.NEEDS_REVIEW}
    ),
    SourceStatus.NEEDS_REVIEW: frozenset(
        {
            SourceStatus.CANDIDATE,
            SourceStatus.APPROVED,
            SourceStatus.ACTIVE,
            SourceStatus.REVIEW_REQUIRED,
            SourceStatus.REJECTED,
        }
    ),
    SourceStatus.REVIEW_REQUIRED: frozenset(
        {
            SourceStatus.CANDIDATE,
            SourceStatus.APPROVED,
            SourceStatus.ACTIVE,
            SourceStatus.DEGRADED,
            SourceStatus.REJECTED,
            SourceStatus.RETIRED,
        }
    ),
    SourceStatus.APPROVED: frozenset(
        {
            SourceStatus.ACTIVE,
            SourceStatus.DEGRADED,
            SourceStatus.PAUSED,
            SourceStatus.REVIEW_REQUIRED,
            SourceStatus.RETIRED,
        }
    ),
    SourceStatus.ACTIVE: frozenset(
        {
            SourceStatus.APPROVED,
            SourceStatus.DEGRADED,
            SourceStatus.PAUSED,
            SourceStatus.REVIEW_REQUIRED,
            SourceStatus.RETIRED,
        }
    ),
    SourceStatus.DEGRADED: frozenset(
        {
            SourceStatus.ACTIVE,
            SourceStatus.APPROVED,
            SourceStatus.PAUSED,
            SourceStatus.REVIEW_REQUIRED,
            SourceStatus.RETIRED,
        }
    ),
    SourceStatus.PAUSED: frozenset(
        {
            SourceStatus.APPROVED,
            SourceStatus.ACTIVE,
            SourceStatus.REJECTED,
            SourceStatus.RETIRED,
        }
    ),
    SourceStatus.REJECTED: frozenset(
        {SourceStatus.CANDIDATE, SourceStatus.NEEDS_REVIEW}
    ),
    SourceStatus.RETIRED: frozenset(
        {SourceStatus.CANDIDATE, SourceStatus.REVIEW_REQUIRED}
    ),
}
_MANUAL_OVERRIDE_TARGETS = frozenset(
    {
        SourceStatus.APPROVED,
        SourceStatus.ACTIVE,
        SourceStatus.DEGRADED,
        SourceStatus.PAUSED,
        SourceStatus.REJECTED,
        SourceStatus.REVIEW_REQUIRED,
        SourceStatus.RETIRED,
    }
)


class SourceRepository:
    async def list_sources(
        self,
        connection: AsyncConnection,
        *,
        status: SourceStatus | str | None = None,
        platform: str | None = None,
        limit: int = 100,
    ) -> tuple[SourceRecord, ...]:
        if not 1 <= limit <= 1000:
            raise ValueError("limit must be between 1 and 1000")
        statement = sa.select(sources)
        if status is not None:
            statement = statement.where(sources.c.lifecycle_status == _status(status).value)
        if platform is not None:
            statement = statement.where(sources.c.platform == _platform(platform))
        rows = await connection.execute(
            statement.order_by(sources.c.updated_at.desc(), sources.c.id).limit(limit)
        )
        return tuple(_source_record(row) for row in rows.mappings())

    async def list_identity_values(
        self,
        connection: AsyncConnection,
        *,
        platform: str | None = None,
    ) -> tuple[str, ...]:
        """Return persisted source identity values for local graph filtering.

        The graph crawler uses these values only as a local, read-only
        known-source filter.  It never treats them as fresh Telegram access
        evidence.
        """

        statement = sa.select(
            sources.c.external_id,
            sources.c.handle,
            sources.c.canonical_url,
        )
        if platform is not None:
            statement = statement.where(sources.c.platform == _platform(platform))
        rows = await connection.execute(statement)
        values: set[str] = set()
        for row in rows.mappings():
            for field in ("external_id", "handle", "canonical_url"):
                value = row[field]
                if isinstance(value, str) and value.strip():
                    values.add(value.strip())
        return tuple(sorted(values))

    async def create_candidate(
        self,
        connection: AsyncConnection,
        *,
        platform: str,
        external_id: str,
        access_type: str,
        display_name: str,
        provider: str,
        lineage_key: str,
        handle: str | None = None,
        canonical_url: str | None = None,
        provider_run_id: str | None = None,
        discovery_run_id: UUID | None = None,
        seed_source_id: int | None = None,
        seed_reference: str | None = None,
        discovered_at: datetime | None = None,
        context: Mapping[str, Any] | None = None,
    ) -> SourceRecord:
        values = _source_values(
            platform=platform,
            external_id=external_id,
            access_type=access_type,
            display_name=display_name,
            handle=handle,
            canonical_url=canonical_url,
        )
        statement = (
            pg_insert(sources)
            .values(**values, lifecycle_status=SourceStatus.CANDIDATE.value)
            .on_conflict_do_nothing(
                constraint="uq_sources_platform_external_id"
            )
            .returning(sources.c.id)
        )
        source_id = await connection.scalar(statement)
        if source_id is None:
            raise SourceIdentityConflict(
                "A source already exists for this platform/external identity"
            )

        await self._record_event(
            connection,
            source_id=int(source_id),
            from_status=None,
            to_status=SourceStatus.CANDIDATE,
            actor_kind="system",
            actor_id=None,
            reason="source discovered",
            is_override=False,
        )
        await self.record_lineage(
            connection,
            source_id=int(source_id),
            provider=provider,
            lineage_key=lineage_key,
            provider_run_id=provider_run_id,
            discovery_run_id=discovery_run_id,
            seed_source_id=seed_source_id,
            seed_reference=seed_reference,
            discovered_at=discovered_at,
            context=context,
        )
        return await self.get(connection, int(source_id))

    async def get(self, connection: AsyncConnection, source_id: int) -> SourceRecord:
        row = (
            await connection.execute(sa.select(sources).where(sources.c.id == source_id))
        ).mappings().one_or_none()
        if row is None:
            raise SourceNotFound(f"Source {source_id} does not exist")
        return _source_record(row)

    async def get_by_identity(
        self,
        connection: AsyncConnection,
        *,
        platform: str,
        external_id: str,
    ) -> SourceRecord | None:
        row = (
            await connection.execute(
                sa.select(sources).where(
                    sources.c.platform == _platform(platform),
                    sources.c.external_id == _required_text(external_id, "external_id"),
                )
            )
        ).mappings().one_or_none()
        return None if row is None else _source_record(row)

    async def list_for_collector(
        self,
        connection: AsyncConnection,
        *,
        collector_account_id: int,
        platform: str | None = None,
    ) -> list[SourceRecord]:
        statement = sa.select(sources).where(
            *_collector_eligibility(collector_account_id)
        )
        if platform is not None:
            statement = statement.where(sources.c.platform == _platform(platform))
        rows = await connection.execute(statement.order_by(sources.c.id))
        return [_source_record(row) for row in rows.mappings()]

    async def get_for_collector(
        self,
        connection: AsyncConnection,
        *,
        source_id: int,
        collector_account_id: int,
        platform: str | None = None,
        lock: bool = False,
    ) -> SourceRecord | None:
        if source_id <= 0 or collector_account_id <= 0:
            raise ValueError("source and collector account identifiers must be positive")
        statement = sa.select(sources).where(
            sources.c.id == source_id,
            *_collector_eligibility(collector_account_id),
        )
        if platform is not None:
            statement = statement.where(sources.c.platform == _platform(platform))
        if lock:
            statement = statement.with_for_update()
        row = (await connection.execute(statement)).mappings().one_or_none()
        return None if row is None else _source_record(row)

    async def is_accessible_to_collector(
        self,
        connection: AsyncConnection,
        *,
        source_id: int,
        collector_account_id: int,
        platform: str | None = None,
    ) -> bool:
        """Check account access without relaxing the approved-source catalog."""

        if source_id <= 0 or collector_account_id <= 0:
            raise ValueError("source and collector account identifiers must be positive")
        statement = sa.select(sources.c.id).where(
            sources.c.id == source_id,
            sa.exists(
                sa.select(1).where(
                    collector_accounts.c.id == collector_account_id,
                    collector_accounts.c.is_active.is_(True),
                    collector_accounts.c.platform == sources.c.platform,
                )
            ),
            sa.or_(
                sources.c.access_type == "public",
                sa.and_(
                    sources.c.access_type == "private",
                    sa.exists(
                        sa.select(1).where(
                            source_collector_access.c.source_id == sources.c.id,
                            source_collector_access.c.collector_account_id
                            == collector_account_id,
                            source_collector_access.c.access_status == "permitted",
                        )
                    ),
                ),
            ),
        )
        if platform is not None:
            statement = statement.where(sources.c.platform == _platform(platform))
        return await connection.scalar(statement) is not None

    async def update_metadata(
        self,
        connection: AsyncConnection,
        source_id: int,
        *,
        display_name: str,
        access_type: str,
        handle: str | None,
        canonical_url: str | None,
    ) -> SourceRecord:
        values = {
            "access_type": _access_type(access_type),
            "display_name": _required_text(display_name, "display_name"),
            "handle": (
                None if handle is None else _required_text(handle, "handle").lower()
            ),
            "canonical_url": _optional_text(canonical_url),
        }
        result = await connection.execute(
            sa.update(sources)
            .where(sources.c.id == source_id)
            .values(**values, updated_at=sa.func.now())
        )
        if result.rowcount != 1:
            raise SourceNotFound(f"Source {source_id} does not exist")
        return await self.get(connection, source_id)

    async def transition(
        self,
        connection: AsyncConnection,
        source_id: int,
        target: SourceStatus | str,
        *,
        reason: str,
        source_audit_id: UUID | None = None,
        actor_kind: str = "system",
        actor_id: str | None = None,
    ) -> SourceRecord:
        target_status = _status(target)
        current_status = await self._locked_status(connection, source_id)
        if target_status not in _VALID_TRANSITIONS[current_status]:
            raise InvalidSourceTransition(
                f"Invalid source lifecycle transition: "
                f"{current_status.value} -> {target_status.value}"
            )
        await self._apply_status_change(
            connection,
            source_id=source_id,
            current_status=current_status,
            target_status=target_status,
            actor_kind=_required_text(actor_kind, "actor_kind"),
            actor_id=None if actor_id is None else _required_text(actor_id, "actor_id"),
            reason=_required_text(reason, "reason"),
            is_override=False,
            source_audit_id=source_audit_id,
        )
        return await self.get(connection, source_id)

    async def override(
        self,
        connection: AsyncConnection,
        source_id: int,
        target: SourceStatus | str,
        *,
        operator_id: str,
        reason: str,
    ) -> SourceRecord:
        target_status = _status(target)
        if target_status not in _MANUAL_OVERRIDE_TARGETS:
            allowed = ", ".join(sorted(status.value for status in _MANUAL_OVERRIDE_TARGETS))
            raise InvalidSourceTransition(
                f"Manual override target must be one of: {allowed}"
            )
        current_status = await self._locked_status(connection, source_id)
        if current_status == target_status:
            raise InvalidSourceTransition(
                f"Source is already {target_status.value}; no lifecycle change to record"
            )
        await self._apply_status_change(
            connection,
            source_id=source_id,
            current_status=current_status,
            target_status=target_status,
            actor_kind="operator",
            actor_id=_required_text(operator_id, "operator_id"),
            reason=_required_text(reason, "reason"),
            is_override=True,
            source_audit_id=None,
        )
        return await self.get(connection, source_id)

    async def record_lineage(
        self,
        connection: AsyncConnection,
        *,
        source_id: int,
        provider: str,
        lineage_key: str,
        provider_run_id: str | None = None,
        discovery_run_id: UUID | None = None,
        seed_source_id: int | None = None,
        seed_reference: str | None = None,
        discovered_at: datetime | None = None,
        context: Mapping[str, Any] | None = None,
    ) -> bool:
        values: dict[str, Any] = {
            "source_id": source_id,
            "provider": _provider(provider),
            "lineage_key": _required_text(lineage_key, "lineage_key"),
            "provider_run_id": _optional_text(provider_run_id),
            "seed_source_id": seed_source_id,
            "seed_reference": _optional_text(seed_reference),
            "context": dict(context or {}),
        }
        if discovery_run_id is not None:
            values["discovery_run_id"] = discovery_run_id
        if discovered_at is not None:
            values["discovered_at"] = discovered_at
        lineage_id = await connection.scalar(
            pg_insert(source_discovery_lineage)
            .values(**values)
            .on_conflict_do_nothing(
                constraint="uq_source_discovery_lineage_source_provider_key"
            )
            .returning(source_discovery_lineage.c.id)
        )
        return lineage_id is not None

    async def list_lineage(
        self,
        connection: AsyncConnection,
        source_id: int,
    ) -> list[SourceLineageRecord]:
        rows = await connection.execute(
            sa.select(source_discovery_lineage)
            .where(source_discovery_lineage.c.source_id == source_id)
            .order_by(
                source_discovery_lineage.c.discovered_at,
                source_discovery_lineage.c.id,
            )
        )
        return [_lineage_record(row) for row in rows.mappings()]

    async def list_lifecycle_events(
        self,
        connection: AsyncConnection,
        source_id: int,
    ) -> list[SourceLifecycleEvent]:
        rows = await connection.execute(
            sa.select(source_lifecycle_events)
            .where(source_lifecycle_events.c.source_id == source_id)
            .order_by(
                source_lifecycle_events.c.changed_at,
                source_lifecycle_events.c.id,
            )
        )
        return [_event_record(row) for row in rows.mappings()]

    async def upsert_seed(
        self,
        connection: AsyncConnection,
        seed: SeedSource,
    ) -> SeedUpsertOutcome:
        if seed.initial_status not in {SourceStatus.CANDIDATE, SourceStatus.APPROVED}:
            raise ValueError("Seed source initial status must be candidate or approved")

        values = _source_values(
            platform=seed.platform,
            external_id=seed.external_id,
            access_type=seed.access_type,
            display_name=seed.display_name,
            handle=seed.handle,
            canonical_url=seed.canonical_url,
        )
        existing = (
            await connection.execute(
                sa.select(sources)
                .where(
                    sources.c.platform == values["platform"],
                    sources.c.external_id == values["external_id"],
                )
                .with_for_update()
            )
        ).mappings().one_or_none()

        created = existing is None
        updated = False
        if existing is None:
            source_id = await connection.scalar(
                sa.insert(sources)
                .values(**values, lifecycle_status=seed.initial_status.value)
                .returning(sources.c.id)
            )
            if source_id is None:
                raise RuntimeError("Source seed insert returned no identifier")
            source_id = int(source_id)
            await self._record_event(
                connection,
                source_id=source_id,
                from_status=None,
                to_status=seed.initial_status,
                actor_kind="seed",
                actor_id=None,
                reason="repository source seed import",
                is_override=False,
            )
        else:
            source_id = int(existing["id"])
            metadata_values = {
                key: value
                for key, value in values.items()
                if key not in {"platform", "external_id"}
            }
            updated = any(existing[key] != value for key, value in metadata_values.items())
            if updated:
                await connection.execute(
                    sa.update(sources)
                    .where(sources.c.id == source_id)
                    .values(**metadata_values, updated_at=sa.func.now())
                )

        lineage_created = await self.record_lineage(
            connection,
            source_id=source_id,
            provider=seed.provider,
            lineage_key=seed.lineage_key,
            provider_run_id=seed.provider_run_id,
            seed_reference=seed.seed_reference,
            context=seed.context,
        )
        return SeedUpsertOutcome(
            source=await self.get(connection, source_id),
            created=created,
            updated=updated,
            lineage_created=lineage_created,
        )

    async def _locked_status(
        self,
        connection: AsyncConnection,
        source_id: int,
    ) -> SourceStatus:
        value = await connection.scalar(
            sa.select(sources.c.lifecycle_status)
            .where(sources.c.id == source_id)
            .with_for_update()
        )
        if value is None:
            raise SourceNotFound(f"Source {source_id} does not exist")
        return SourceStatus(value)

    async def _apply_status_change(
        self,
        connection: AsyncConnection,
        *,
        source_id: int,
        current_status: SourceStatus,
        target_status: SourceStatus,
        actor_kind: str,
        actor_id: str | None,
        reason: str,
        is_override: bool,
        source_audit_id: UUID | None,
    ) -> None:
        await connection.execute(
            sa.update(sources)
            .where(sources.c.id == source_id)
            .values(lifecycle_status=target_status.value, updated_at=sa.func.now())
        )
        await self._record_event(
            connection,
            source_id=source_id,
            from_status=current_status,
            to_status=target_status,
            actor_kind=actor_kind,
            actor_id=actor_id,
            reason=reason,
            is_override=is_override,
            source_audit_id=source_audit_id,
        )

    async def _record_event(
        self,
        connection: AsyncConnection,
        *,
        source_id: int,
        from_status: SourceStatus | None,
        to_status: SourceStatus,
        actor_kind: str,
        actor_id: str | None,
        reason: str,
        is_override: bool,
        source_audit_id: UUID | None = None,
    ) -> None:
        values = {
            "source_id": source_id,
            "from_status": None if from_status is None else from_status.value,
            "to_status": to_status.value,
            "actor_kind": actor_kind,
            "actor_id": actor_id,
            "reason": reason,
            "is_override": is_override,
        }
        if source_audit_id is not None:
            values["source_audit_id"] = source_audit_id
        await connection.execute(
            sa.insert(source_lifecycle_events).values(**values)
        )


class PostgresSourceCatalog:
    """Collector-facing catalog that can return only approved PostgreSQL sources."""

    def __init__(
        self,
        database: Database,
        repository: SourceRepository | None = None,
    ) -> None:
        self._database = database
        self._repository = repository or SourceRepository()

    async def list_approved(
        self,
        *,
        collector_account_id: int,
        platform: str | None = None,
    ) -> list[SourceRecord]:
        async with self._database.connect() as connection:
            return await self._repository.list_for_collector(
                connection,
                collector_account_id=collector_account_id,
                platform=platform,
            )

    async def list_known_source_identities(
        self,
        *,
        platform: str | None = None,
    ) -> tuple[str, ...]:
        async with self._database.connect() as connection:
            return await self._repository.list_identity_values(
                connection,
                platform=platform,
            )


def _collector_eligibility(
    collector_account_id: int,
) -> tuple[sa.ColumnElement[bool], ...]:
    if collector_account_id <= 0:
        raise ValueError("collector_account_id must be positive")
    active_compatible_account = sa.exists(
        sa.select(1).where(
            collector_accounts.c.id == collector_account_id,
            collector_accounts.c.is_active.is_(True),
            collector_accounts.c.platform == sources.c.platform,
        )
    )
    permitted_private_access = sa.exists(
        sa.select(1).where(
            source_collector_access.c.source_id == sources.c.id,
            source_collector_access.c.collector_account_id == collector_account_id,
            source_collector_access.c.access_status == "permitted",
        )
    )
    return (
        sources.c.lifecycle_status.in_(
            (
                SourceStatus.APPROVED.value,
                SourceStatus.ACTIVE.value,
                SourceStatus.DEGRADED.value,
            )
        ),
        active_compatible_account,
        sa.or_(
            sources.c.access_type == "public",
            sa.and_(
                sources.c.access_type == "private",
                permitted_private_access,
            ),
        ),
    )


def _source_values(
    *,
    platform: str,
    external_id: str,
    access_type: str,
    display_name: str,
    handle: str | None,
    canonical_url: str | None,
) -> dict[str, Any]:
    return {
        "platform": _platform(platform),
        "external_id": _required_text(external_id, "external_id"),
        "access_type": _access_type(access_type),
        "display_name": _required_text(display_name, "display_name"),
        "handle": None if handle is None else _required_text(handle, "handle").lower(),
        "canonical_url": _optional_text(canonical_url),
    }


def _required_text(value: str, field: str) -> str:
    normalized = value.strip()
    if not normalized:
        raise ValueError(f"{field} must not be blank")
    return normalized


def _optional_text(value: str | None) -> str | None:
    return None if value is None else _required_text(value, "optional text")


def _platform(value: str) -> str:
    return _required_text(value, "platform").lower()


def _provider(value: str) -> str:
    return _required_text(value, "provider").lower()


def _access_type(value: str) -> str:
    normalized = _required_text(value, "access_type").lower()
    if normalized not in {"public", "private"}:
        raise ValueError("access_type must be public or private")
    return normalized


def _status(value: SourceStatus | str) -> SourceStatus:
    try:
        return SourceStatus(value)
    except ValueError:
        raise InvalidSourceTransition(f"Unknown source lifecycle status: {value}") from None


def _source_record(row: Mapping[str, Any]) -> SourceRecord:
    return SourceRecord(
        id=int(row["id"]),
        platform=str(row["platform"]),
        external_id=str(row["external_id"]),
        access_type=str(row["access_type"]),
        lifecycle_status=SourceStatus(row["lifecycle_status"]),
        display_name=str(row["display_name"]),
        handle=row["handle"],
        canonical_url=row["canonical_url"],
        created_at=row["created_at"],
        updated_at=row["updated_at"],
    )


def _lineage_record(row: Mapping[str, Any]) -> SourceLineageRecord:
    return SourceLineageRecord(
        id=int(row["id"]),
        source_id=int(row["source_id"]),
        provider=str(row["provider"]),
        lineage_key=str(row["lineage_key"]),
        provider_run_id=row["provider_run_id"],
        discovery_run_id=row["discovery_run_id"],
        seed_source_id=row["seed_source_id"],
        seed_reference=row["seed_reference"],
        discovered_at=row["discovered_at"],
        context=dict(row["context"]),
    )


def _event_record(row: Mapping[str, Any]) -> SourceLifecycleEvent:
    return SourceLifecycleEvent(
        id=int(row["id"]),
        source_id=int(row["source_id"]),
        from_status=(
            None if row["from_status"] is None else SourceStatus(row["from_status"])
        ),
        to_status=SourceStatus(row["to_status"]),
        actor_kind=str(row["actor_kind"]),
        actor_id=row["actor_id"],
        reason=str(row["reason"]),
        is_override=bool(row["is_override"]),
        source_audit_id=row["source_audit_id"],
        changed_at=row["changed_at"],
    )
