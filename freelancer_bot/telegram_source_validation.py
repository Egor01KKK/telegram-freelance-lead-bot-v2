"""Bounded, governed Telegram validation for Global Source Library candidates."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import re
from collections.abc import Mapping
from typing import Any

import sqlalchemy as sa
from telethon.errors import FloodWaitError

from .persistence.database import Database
from .persistence.discovery_campaigns import DiscoveryCampaignRepository
from .persistence.schema import source_reference_aliases, telegram_source_validations
from .persistence.source_repository import SourceRepository, SourceStatus
from .telegram_references import InvalidTelegramReference, normalize_telegram_reference
from .telegram_request_governor import TelegramRequestCategory, TelegramRequestGovernor


@dataclass(frozen=True)
class TelegramSourceValidationResult:
    source_id: int
    collector_account_id: int
    state: str
    access_mode: str
    canonical_peer_identity: str | None
    failure_code: str | None
    duplicate_of_source_id: int | None = None


class TelegramSourceValidationService:
    """Validate one candidate without joining it or bypassing the governor."""

    def __init__(
        self,
        database: Database,
        *,
        source_repository: SourceRepository | None = None,
        library_repository: DiscoveryCampaignRepository | None = None,
    ) -> None:
        self._database = database
        self._sources = source_repository or SourceRepository()
        self._library = library_repository or DiscoveryCampaignRepository()

    async def validate(
        self,
        *,
        source_id: int,
        collector_account_id: int,
        client: Any,
        governor: TelegramRequestGovernor,
        checked_by: str = "operator",
    ) -> TelegramSourceValidationResult:
        async with self._database.connect() as connection:
            source = await self._sources.get(connection, source_id)
            if source.platform != "telegram":
                raise ValueError("Telegram validation supports Telegram sources only")
            permitted = await self._sources.is_accessible_to_collector(
                connection,
                source_id=source.id,
                collector_account_id=collector_account_id,
                platform="telegram",
            )
        if not permitted:
            return await self._record_failure(
                source_id=source_id,
                collector_account_id=collector_account_id,
                state="unavailable",
                access_mode="unavailable",
                failure_code="access_not_permitted",
                checked_by=checked_by,
            )

        lookup = source.handle or source.canonical_url or source.external_id
        ready, cached = await self._preflight_resolution(
            source=source,
            collector_account_id=collector_account_id,
            checked_by=checked_by,
        )
        if ready:
            return cached  # type: ignore[return-value]
        try:
            return await governor.run(
                TelegramRequestCategory.ENTITY_ACCESS,
                lambda: self._resolve_and_persist(
                    source=source,
                    lookup=lookup,
                    collector_account_id=collector_account_id,
                    client=client,
                    checked_by=checked_by,
                ),
                before_reserve=lambda: self._preflight_resolution(
                    source=source,
                    collector_account_id=collector_account_id,
                    checked_by=checked_by,
                ),
            )
        except FloodWaitError:
            await self._record_failure(
                source_id=source_id,
                collector_account_id=collector_account_id,
                state="unavailable",
                access_mode="unavailable",
                failure_code="telegram_floodwait",
                checked_by=checked_by,
            )
            raise
        except Exception as exc:
            return await self._record_failure(
                source_id=source_id,
                collector_account_id=collector_account_id,
                state="unavailable",
                access_mode="unavailable",
                failure_code=_failure_code(exc),
                checked_by=checked_by,
            )

    async def _resolve_and_persist(
        self,
        *,
        source: Any,
        lookup: str,
        collector_account_id: int,
        client: Any,
        checked_by: str,
    ) -> TelegramSourceValidationResult:
        """Resolve once, then persist the result before releasing the governor."""

        ready, cached = await self._preflight_resolution(
            source=source,
            collector_account_id=collector_account_id,
            checked_by=checked_by,
        )
        if ready:
            return cached  # type: ignore[return-value]

        entity = await client.get_entity(lookup)
        canonical_peer_identity = _canonical_peer_identity(entity)
        async with self._database.transaction() as connection:
            existing = await self._library.source_for_canonical_peer(
                connection,
                platform="telegram",
                canonical_peer_identity=canonical_peer_identity,
            )
            if existing is not None and int(existing) != int(source.id):
                return await self._record_duplicate_in_connection(
                    connection,
                    source=source,
                    source_id=source.id,
                    collector_account_id=collector_account_id,
                    canonical_peer_identity=canonical_peer_identity,
                    checked_by=checked_by,
                    duplicate_of_source_id=int(existing),
                )

            for raw, kind in _source_aliases(source):
                try:
                    reference = normalize_telegram_reference(raw)
                except InvalidTelegramReference:
                    continue
                await self._library.record_alias(
                    connection,
                    source_id=source.id,
                    platform="telegram",
                    normalized_reference=reference.source_key,
                    reference_kind=kind or reference.reference_kind,
                    canonical_peer_identity=canonical_peer_identity,
                )
            await self._library.upsert_validation(
                connection,
                source_id=source.id,
                collector_account_id=collector_account_id,
                state="accessible",
                access_mode=("public_readable" if source.access_type == "public" else "joined"),
                canonical_peer_identity=canonical_peer_identity,
                checked_at=datetime.now(timezone.utc),
                checked_by=checked_by,
            )
        return TelegramSourceValidationResult(
            source_id=source.id,
            collector_account_id=collector_account_id,
            state="accessible",
            access_mode=("public_readable" if source.access_type == "public" else "joined"),
            canonical_peer_identity=canonical_peer_identity,
            failure_code=None,
        )

    async def _preflight_resolution(
        self,
        *,
        source: Any,
        collector_account_id: int,
        checked_by: str,
    ) -> tuple[bool, TelegramSourceValidationResult | None]:
        """Reuse terminal state or a trusted canonical alias before Telegram."""

        async with self._database.connect() as connection:
            cached = await self._reusable_validation(
                connection,
                source_id=source.id,
                collector_account_id=collector_account_id,
            )
            if cached is not None:
                return True, cached
            duplicate = await self._alias_duplicate(
                connection,
                source=source,
            )
        if duplicate is None:
            return False, None
        duplicate_source_id, canonical_peer_identity = duplicate
        return True, await self._record_duplicate(
            source=source,
            source_id=source.id,
            collector_account_id=collector_account_id,
            canonical_peer_identity=canonical_peer_identity,
            checked_by=checked_by,
            duplicate_of_source_id=duplicate_source_id,
        )

    async def _reusable_validation(
        self,
        connection: Any,
        *,
        source_id: int,
        collector_account_id: int,
    ) -> TelegramSourceValidationResult | None:
        row = (
            await connection.execute(
                sa.select(telegram_source_validations).where(
                    telegram_source_validations.c.source_id == source_id,
                    telegram_source_validations.c.collector_account_id
                    == collector_account_id,
                )
            )
        ).mappings().one_or_none()
        if row is None or not _is_reusable_validation(row):
            return None
        return TelegramSourceValidationResult(
            source_id=source_id,
            collector_account_id=collector_account_id,
            state=str(row["state"]),
            access_mode=str(row["access_mode"] or "unavailable"),
            canonical_peer_identity=row["canonical_peer_identity"],
            failure_code=row["failure_code"],
        )

    async def _alias_duplicate(
        self,
        connection: Any,
        *,
        source: Any,
    ) -> tuple[int, str] | None:
        for raw, _kind in _source_aliases(source):
            try:
                reference = normalize_telegram_reference(raw)
            except InvalidTelegramReference:
                continue
            row = (
                await connection.execute(
                    sa.select(
                        source_reference_aliases.c.source_id,
                        source_reference_aliases.c.canonical_peer_identity,
                    ).where(
                        source_reference_aliases.c.platform == "telegram",
                        source_reference_aliases.c.normalized_reference
                        == reference.source_key,
                    )
                )
            ).mappings().one_or_none()
            if row is None or int(row["source_id"]) == int(source.id):
                continue
            canonical = row["canonical_peer_identity"]
            if canonical:
                return int(row["source_id"]), str(canonical)
        return None

    async def _record_duplicate(
        self,
        *,
        source: Any,
        source_id: int,
        collector_account_id: int,
        canonical_peer_identity: str,
        checked_by: str,
        duplicate_of_source_id: int,
    ) -> TelegramSourceValidationResult:
        async with self._database.transaction() as connection:
            return await self._record_duplicate_in_connection(
                connection,
                source=source,
                source_id=source_id,
                collector_account_id=collector_account_id,
                canonical_peer_identity=canonical_peer_identity,
                checked_by=checked_by,
                duplicate_of_source_id=duplicate_of_source_id,
            )

    async def _record_duplicate_in_connection(
        self,
        connection: Any,
        *,
        source: Any,
        source_id: int,
        collector_account_id: int,
        canonical_peer_identity: str,
        checked_by: str,
        duplicate_of_source_id: int,
    ) -> TelegramSourceValidationResult:
        await self._library.upsert_validation(
            connection,
            source_id=source_id,
            collector_account_id=collector_account_id,
            state="rejected",
            access_mode="unavailable",
            canonical_peer_identity=canonical_peer_identity,
            failure_code="canonical_alias_existing",
            checked_at=datetime.now(timezone.utc),
            checked_by=checked_by,
        )
        if source.lifecycle_status in {
            SourceStatus.CANDIDATE,
            SourceStatus.NEEDS_REVIEW,
        }:
            await self._sources.transition(
                connection,
                source_id,
                SourceStatus.REJECTED,
                reason="resolved Telegram peer is already a global source",
                actor_kind="system",
                actor_id=checked_by,
            )
        return TelegramSourceValidationResult(
            source_id=source_id,
            collector_account_id=collector_account_id,
            state="rejected",
            access_mode="unavailable",
            canonical_peer_identity=canonical_peer_identity,
            failure_code="canonical_alias_existing",
            duplicate_of_source_id=duplicate_of_source_id,
        )

    async def _record_failure(
        self,
        *,
        source_id: int,
        collector_account_id: int,
        state: str,
        access_mode: str,
        failure_code: str,
        checked_by: str,
    ) -> TelegramSourceValidationResult:
        async with self._database.transaction() as connection:
            await self._library.upsert_validation(
                connection,
                source_id=source_id,
                collector_account_id=collector_account_id,
                state=state,
                access_mode=access_mode,
                failure_code=failure_code,
                checked_at=datetime.now(timezone.utc),
                checked_by=checked_by,
            )
        return TelegramSourceValidationResult(
            source_id=source_id,
            collector_account_id=collector_account_id,
            state=state,
            access_mode=access_mode,
            canonical_peer_identity=None,
            failure_code=failure_code,
        )


def _canonical_peer_identity(entity: Any) -> str:
    value = getattr(entity, "id", None)
    if isinstance(value, bool) or not isinstance(value, int) or value == 0:
        raise ValueError("Telegram entity has no stable peer identity")
    return f"peer:{value}"


def _source_aliases(source: Any) -> tuple[tuple[str, str | None], ...]:
    values: list[tuple[str, str | None]] = []
    if source.canonical_url:
        values.append((source.canonical_url, "source"))
    if source.handle:
        values.append((f"https://t.me/{source.handle.removeprefix('@')}", "username"))
    if source.external_id.startswith("username:"):
        values.append((f"https://t.me/{source.external_id.split(':', 1)[1]}", "username"))
    return tuple(values)


def _failure_code(error: Exception) -> str:
    value = re.sub(r"(?<!^)(?=[A-Z])", "_", error.__class__.__name__).lower()
    value = re.sub(r"[^a-z0-9_.-]", "_", value)
    return value[:64] or "telegram_validation_error"


def _is_reusable_validation(row: Mapping[str, Any]) -> bool:
    if row.get("checked_at") is None:
        return False
    state = str(row.get("state") or "")
    if state in {"accessible", "rejected"}:
        return True
    if state != "unavailable":
        return False
    failure_code = str(row.get("failure_code") or "").casefold()
    return failure_code not in {
        "telegram_floodwait",
        "timeout",
        "network_error",
        "connection_error",
    }
