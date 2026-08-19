from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime

import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncConnection

from .schema import web_provider_health


@dataclass(frozen=True)
class WebProviderHealthRecord:
    provider: str
    backend: str
    state: str
    successful_searches: int
    http_403: int
    http_429: int
    captcha_or_suspension: int
    consecutive_failures: int
    last_failure_category: str | None
    last_failure_at: datetime | None
    backoff_until: datetime | None
    last_success_at: datetime | None
    updated_at: datetime


class WebProviderHealthRepository:
    async def list_for_provider(
        self,
        connection: AsyncConnection,
        *,
        provider: str,
    ) -> tuple[WebProviderHealthRecord, ...]:
        provider = _safe_identifier(provider, "provider", 64)
        rows = await connection.execute(
            sa.select(web_provider_health)
            .where(web_provider_health.c.provider == provider)
            .order_by(web_provider_health.c.backend)
        )
        return tuple(_record(row) for row in rows.mappings())

    async def upsert(
        self,
        connection: AsyncConnection,
        *,
        provider: str,
        backend: str,
        state: str,
        successful_searches: int,
        http_403: int,
        http_429: int,
        captcha_or_suspension: int,
        consecutive_failures: int,
        last_failure_category: str | None,
        last_failure_at: datetime | None,
        backoff_until: datetime | None,
        last_success_at: datetime | None,
    ) -> WebProviderHealthRecord:
        provider = _safe_identifier(provider, "provider", 64)
        backend = _safe_identifier(backend, "backend", 64)
        state = _safe_state(state)
        for name, value in (
            ("successful_searches", successful_searches),
            ("http_403", http_403),
            ("http_429", http_429),
            ("captcha_or_suspension", captcha_or_suspension),
            ("consecutive_failures", consecutive_failures),
        ):
            if value < 0:
                raise ValueError(f"{name} must be nonnegative")
        for name, value in (
            ("last_failure_at", last_failure_at),
            ("backoff_until", backoff_until),
            ("last_success_at", last_success_at),
        ):
            if value is not None and (value.tzinfo is None or value.utcoffset() is None):
                raise ValueError(f"{name} must include a timezone")
        if last_failure_category is not None:
            last_failure_category = _safe_identifier(
                last_failure_category,
                "last_failure_category",
                64,
            )
        values = {
            "provider": provider,
            "backend": backend,
            "state": state,
            "successful_searches": successful_searches,
            "http_403": http_403,
            "http_429": http_429,
            "captcha_or_suspension": captcha_or_suspension,
            "consecutive_failures": consecutive_failures,
            "last_failure_category": last_failure_category,
            "last_failure_at": last_failure_at,
            "backoff_until": backoff_until,
            "last_success_at": last_success_at,
        }
        statement = pg_insert(web_provider_health).values(**values)
        statement = statement.on_conflict_do_update(
            index_elements=[
                web_provider_health.c.provider,
                web_provider_health.c.backend,
            ],
            set_={
                key: getattr(statement.excluded, key)
                for key in values
                if key not in {"provider", "backend"}
            },
        )
        await connection.execute(statement)
        row = (
            await connection.execute(
                sa.select(web_provider_health).where(
                    web_provider_health.c.provider == provider,
                    web_provider_health.c.backend == backend,
                )
            )
        ).mappings().one()
        return _record(row)


def _record(row: Mapping[str, object]) -> WebProviderHealthRecord:
    return WebProviderHealthRecord(
        provider=str(row["provider"]),
        backend=str(row["backend"]),
        state=str(row["state"]),
        successful_searches=int(row["successful_searches"]),
        http_403=int(row["http_403"]),
        http_429=int(row["http_429"]),
        captcha_or_suspension=int(row["captcha_or_suspension"]),
        consecutive_failures=int(row["consecutive_failures"]),
        last_failure_category=row["last_failure_category"],
        last_failure_at=row["last_failure_at"],
        backoff_until=row["backoff_until"],
        last_success_at=row["last_success_at"],
        updated_at=row["updated_at"],
    )


def _safe_identifier(value: str, field: str, maximum: int) -> str:
    normalized = value.strip().casefold()
    if (
        not normalized
        or len(normalized) > maximum
        or not all(character.isalnum() or character in "_-" for character in normalized)
        or not normalized[0].isalpha()
    ):
        raise ValueError(f"{field} must be a safe lowercase identifier")
    return normalized


def _safe_state(value: str) -> str:
    normalized = value.strip().upper()
    if normalized not in {"READY", "DEGRADED", "BACKOFF", "UNAVAILABLE"}:
        raise ValueError("state is not a supported Web provider state")
    return normalized
