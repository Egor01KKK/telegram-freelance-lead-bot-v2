from __future__ import annotations

from dataclasses import dataclass
import logging
from urllib.parse import urlparse
from typing import Any, Protocol

from .observability import log_event
from .persistence.collector_accounts import (
    CollectorAccountRecord,
    CollectorAccountRepository,
)
from .persistence.database import Database
from .persistence.source_repository import PostgresSourceCatalog, SourceRecord
from .persistence.telegram_operation_state import TelegramCollectorOperationRepository
from .sources import HANDLE_RE, Source


LOGGER = logging.getLogger("freelancer_bot")


class TelegramSessionClient(Protocol):
    async def get_me(self) -> Any: ...


@dataclass(frozen=True)
class TelegramCollectorSource:
    record: SourceRecord
    lookup: str

    @property
    def handle(self) -> str:
        if self.record.handle is not None:
            return self.record.handle
        username = _canonical_username(self.lookup)
        if username is None:
            raise RuntimeError("Collector source has no legacy-compatible identity")
        return f"@{username}"

    @property
    def title(self) -> str:
        return self.record.display_name

    def legacy_source(self) -> Source:
        return Source(
            handle=self.handle,
            title=self.record.display_name,
            reason="PostgreSQL approved source",
        )

    def message_url(self, external_message_id: int) -> str:
        if external_message_id <= 0:
            raise ValueError("external_message_id must be positive")
        return f"https://t.me/{self.handle.removeprefix('@')}/{external_message_id}"


@dataclass(frozen=True)
class TelegramCollectorSourceSnapshot:
    collector_account: CollectorAccountRecord
    sources: tuple[TelegramCollectorSource, ...]


class ApprovedTelegramSourceAdapter:
    """Bind one Telethon user session to its approved PostgreSQL sources."""

    def __init__(
        self,
        database: Database,
        *,
        accounts: CollectorAccountRepository | None = None,
        catalog: PostgresSourceCatalog | None = None,
    ) -> None:
        self._database = database
        self._accounts = accounts or CollectorAccountRepository()
        self._catalog = catalog or PostgresSourceCatalog(database)

    async def list_for_session(
        self,
        client: TelegramSessionClient,
    ) -> TelegramCollectorSourceSnapshot:
        session = await client.get_me()
        external_account_id = _session_account_id(session)
        async with self._database.transaction() as connection:
            account = await self._accounts.ensure(
                connection,
                platform="telegram",
                external_account_id=external_account_id,
                display_name=f"Telethon collector {external_account_id}",
            )
            await TelegramCollectorOperationRepository().ensure(
                connection,
                collector_account_id=account.id,
            )

        records = await self._catalog.list_approved(
            collector_account_id=account.id,
            platform="telegram",
        )
        targets: list[TelegramCollectorSource] = []
        for record in records:
            lookup = _source_lookup(record)
            if lookup is None:
                log_event(
                    LOGGER,
                    logging.WARNING,
                    "telegram.collector.source_lookup_unavailable",
                    source_id=record.id,
                    collector_account_id=account.id,
                )
                continue
            targets.append(TelegramCollectorSource(record=record, lookup=lookup))

        log_event(
            LOGGER,
            logging.INFO,
            "telegram.collector.source_catalog_loaded",
            collector_account_id=account.id,
            source_count=len(targets),
        )
        return TelegramCollectorSourceSnapshot(
            collector_account=account,
            sources=tuple(targets),
        )


def _session_account_id(session: Any) -> str:
    value = getattr(session, "id", None)
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise RuntimeError("Telethon user session returned no stable account identifier")
    return str(value)


def _source_lookup(source: SourceRecord) -> str | None:
    if source.handle is not None:
        return source.handle
    if source.canonical_url is None:
        return None
    return (
        source.canonical_url
        if _canonical_username(source.canonical_url) is not None
        else None
    )


def _canonical_username(value: str) -> str | None:
    parsed = urlparse(value)
    path = parsed.path.strip("/")
    username = path.removeprefix("@")
    if parsed.scheme.lower() != "https":
        return None
    if parsed.netloc.lower() not in {"t.me", "telegram.me"}:
        return None
    if "/" in path or not HANDLE_RE.fullmatch(username):
        return None
    return username
