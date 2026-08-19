from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
import re
from typing import Any

import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncConnection

from .schema import collector_accounts, source_collector_access, sources
from .source_repository import SourceNotFound, SourceStatus


class CollectorAccessStatus(str, Enum):
    PERMITTED = "permitted"
    INACCESSIBLE = "inaccessible"
    REVOKED = "revoked"


class CollectorAccountNotFound(LookupError):
    pass


class InvalidCollectorAccess(ValueError):
    pass


@dataclass(frozen=True)
class CollectorAccountRecord:
    id: int
    platform: str
    external_account_id: str
    display_name: str
    is_active: bool
    created_at: datetime
    updated_at: datetime


@dataclass(frozen=True)
class SourceCollectorAccessRecord:
    source_id: int
    collector_account_id: int
    access_status: CollectorAccessStatus
    checked_at: datetime
    checked_by: str
    created_at: datetime
    updated_at: datetime


@dataclass(frozen=True)
class SourceAccessResolution:
    source_id: int
    lifecycle_status: SourceStatus
    access_type: str
    collector_accounts: tuple[CollectorAccountRecord, ...]

    @property
    def collection_allowed(self) -> bool:
        return (
            self.lifecycle_status is SourceStatus.APPROVED
            and bool(self.collector_accounts)
        )


class CollectorAccountRepository:
    async def ensure(
        self,
        connection: AsyncConnection,
        *,
        platform: str,
        external_account_id: str,
        display_name: str,
        active_on_create: bool = True,
    ) -> CollectorAccountRecord:
        values = {
            "platform": _platform(platform),
            "external_account_id": _required_text(
                external_account_id,
                "external_account_id",
            ),
            "display_name": _required_text(display_name, "display_name"),
            "is_active": active_on_create,
        }
        statement = pg_insert(collector_accounts).values(**values)
        statement = statement.on_conflict_do_update(
            constraint="uq_collector_accounts_platform_external_account_id",
            set_={
                "display_name": statement.excluded.display_name,
                "updated_at": sa.func.now(),
            },
        ).returning(collector_accounts.c.id)
        account_id = await connection.scalar(statement)
        if account_id is None:
            raise RuntimeError("Collector account upsert returned no identifier")
        return await self.get(connection, int(account_id))

    async def get(
        self,
        connection: AsyncConnection,
        collector_account_id: int,
    ) -> CollectorAccountRecord:
        row = (
            await connection.execute(
                sa.select(collector_accounts).where(
                    collector_accounts.c.id == collector_account_id
                )
            )
        ).mappings().one_or_none()
        if row is None:
            raise CollectorAccountNotFound(
                f"Collector account {collector_account_id} does not exist"
            )
        return _account_record(row)

    async def set_active(
        self,
        connection: AsyncConnection,
        collector_account_id: int,
        *,
        active: bool,
    ) -> CollectorAccountRecord:
        result = await connection.execute(
            sa.update(collector_accounts)
            .where(collector_accounts.c.id == collector_account_id)
            .values(is_active=active, updated_at=sa.func.now())
        )
        if result.rowcount != 1:
            raise CollectorAccountNotFound(
                f"Collector account {collector_account_id} does not exist"
            )
        return await self.get(connection, collector_account_id)

    async def record_source_access(
        self,
        connection: AsyncConnection,
        *,
        source_id: int,
        collector_account_id: int,
        access_status: CollectorAccessStatus | str,
        checked_at: datetime,
        checked_by: str,
    ) -> SourceCollectorAccessRecord:
        status = _access_status(access_status)
        if checked_at.tzinfo is None or checked_at.utcoffset() is None:
            raise InvalidCollectorAccess("checked_at must include a timezone")
        actor = _checked_by(checked_by)

        source = (
            await connection.execute(
                sa.select(sources.c.platform, sources.c.access_type)
                .where(sources.c.id == source_id)
                .with_for_update()
            )
        ).one_or_none()
        if source is None:
            raise SourceNotFound(f"Source {source_id} does not exist")
        account = (
            await connection.execute(
                sa.select(collector_accounts.c.platform)
                .where(collector_accounts.c.id == collector_account_id)
                .with_for_update()
            )
        ).one_or_none()
        if account is None:
            raise CollectorAccountNotFound(
                f"Collector account {collector_account_id} does not exist"
            )
        if source.access_type != "private":
            raise InvalidCollectorAccess(
                "Explicit collector access records are only valid for private sources"
            )
        if source.platform != account.platform:
            raise InvalidCollectorAccess(
                "Collector account platform does not match source platform"
            )

        statement = pg_insert(source_collector_access).values(
            source_id=source_id,
            collector_account_id=collector_account_id,
            access_status=status.value,
            checked_at=checked_at,
            checked_by=actor,
        )
        statement = statement.on_conflict_do_update(
            constraint="pk_source_collector_access",
            set_={
                "access_status": statement.excluded.access_status,
                "checked_at": statement.excluded.checked_at,
                "checked_by": statement.excluded.checked_by,
                "updated_at": sa.func.now(),
            },
        )
        await connection.execute(statement)
        return await self.get_source_access(
            connection,
            source_id=source_id,
            collector_account_id=collector_account_id,
        )

    async def get_source_access(
        self,
        connection: AsyncConnection,
        *,
        source_id: int,
        collector_account_id: int,
    ) -> SourceCollectorAccessRecord:
        row = (
            await connection.execute(
                sa.select(source_collector_access).where(
                    source_collector_access.c.source_id == source_id,
                    source_collector_access.c.collector_account_id
                    == collector_account_id,
                )
            )
        ).mappings().one_or_none()
        if row is None:
            raise InvalidCollectorAccess(
                "No access record exists for this source and collector account"
            )
        return _access_record(row)

    async def resolve_source_access(
        self,
        connection: AsyncConnection,
        source_id: int,
    ) -> SourceAccessResolution:
        source = (
            await connection.execute(
                sa.select(
                    sources.c.id,
                    sources.c.platform,
                    sources.c.access_type,
                    sources.c.lifecycle_status,
                ).where(sources.c.id == source_id)
            )
        ).one_or_none()
        if source is None:
            raise SourceNotFound(f"Source {source_id} does not exist")

        accounts: list[CollectorAccountRecord] = []
        if source.lifecycle_status == SourceStatus.APPROVED.value:
            statement = sa.select(collector_accounts).where(
                collector_accounts.c.platform == source.platform,
                collector_accounts.c.is_active.is_(True),
            )
            if source.access_type == "private":
                statement = statement.join(
                    source_collector_access,
                    sa.and_(
                        source_collector_access.c.collector_account_id
                        == collector_accounts.c.id,
                        source_collector_access.c.source_id == source_id,
                        source_collector_access.c.access_status == "permitted",
                    ),
                )
            rows = await connection.execute(statement.order_by(collector_accounts.c.id))
            accounts = [_account_record(row) for row in rows.mappings()]

        return SourceAccessResolution(
            source_id=int(source.id),
            lifecycle_status=SourceStatus(source.lifecycle_status),
            access_type=str(source.access_type),
            collector_accounts=tuple(accounts),
        )


def _required_text(value: str, field: str) -> str:
    normalized = value.strip()
    if not normalized:
        raise ValueError(f"{field} must not be blank")
    return normalized


def _platform(value: str) -> str:
    return _required_text(value, "platform").lower()


def _checked_by(value: str) -> str:
    normalized = _required_text(value, "checked_by")
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.:@-]{0,127}", normalized):
        raise InvalidCollectorAccess("checked_by must be a safe opaque actor identifier")
    return normalized


def _access_status(value: CollectorAccessStatus | str) -> CollectorAccessStatus:
    try:
        return CollectorAccessStatus(value)
    except ValueError:
        raise InvalidCollectorAccess(f"Unknown collector access status: {value}") from None


def _account_record(row: Mapping[str, Any]) -> CollectorAccountRecord:
    return CollectorAccountRecord(
        id=int(row["id"]),
        platform=str(row["platform"]),
        external_account_id=str(row["external_account_id"]),
        display_name=str(row["display_name"]),
        is_active=bool(row["is_active"]),
        created_at=row["created_at"],
        updated_at=row["updated_at"],
    )


def _access_record(row: Mapping[str, Any]) -> SourceCollectorAccessRecord:
    return SourceCollectorAccessRecord(
        source_id=int(row["source_id"]),
        collector_account_id=int(row["collector_account_id"]),
        access_status=CollectorAccessStatus(row["access_status"]),
        checked_at=row["checked_at"],
        checked_by=str(row["checked_by"]),
        created_at=row["created_at"],
        updated_at=row["updated_at"],
    )
