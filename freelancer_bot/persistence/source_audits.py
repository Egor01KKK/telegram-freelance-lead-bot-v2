from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime
from typing import Any
from uuid import UUID, uuid4

import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncConnection

from .schema import (
    source_audits,
    source_taxonomy_assignments,
    source_taxonomy_terms,
    sources,
)
from .source_repository import SourceNotFound


class SourceAuditConflict(RuntimeError):
    pass


@dataclass(frozen=True)
class SourceAuditRecord:
    id: UUID
    source_id: int
    audit_key: str
    schema_version: str
    provider: str
    model: str
    analyzer_version: str
    audited_at: datetime
    window_started_at: datetime
    window_ended_at: datetime
    sampled_from: datetime | None
    sampled_to: datetime | None
    sampled_message_count: int
    probe_message_count: int
    expanded: bool
    high_volume: bool
    sample_fingerprint: str
    commercial_opportunity_count: int
    buyer_intent_count: int
    seller_promotion_count: int
    ads_spam_count: int
    duplicate_count: int
    content_mix: Mapping[str, float]
    primary_language: str | None
    languages: tuple[Mapping[str, str], ...]
    categories: tuple[Mapping[str, str], ...]
    decision_policy: Mapping[str, Any]
    decision: str
    reason_codes: tuple[str, ...]
    reasons: tuple[Mapping[str, Any], ...]
    created_at: datetime


@dataclass(frozen=True)
class SourceAuditWrite:
    source_id: int
    audit_key: str
    schema_version: str
    provider: str
    model: str
    analyzer_version: str
    audited_at: datetime
    window_started_at: datetime
    window_ended_at: datetime
    sampled_from: datetime | None
    sampled_to: datetime | None
    sampled_message_count: int
    probe_message_count: int
    expanded: bool
    high_volume: bool
    sample_fingerprint: str
    commercial_opportunity_count: int
    buyer_intent_count: int
    seller_promotion_count: int
    ads_spam_count: int
    duplicate_count: int
    content_mix: Mapping[str, float]
    primary_language: str | None
    languages: Sequence[Mapping[str, str]]
    categories: Sequence[Mapping[str, str]]
    decision_policy: Mapping[str, Any]
    decision: str
    reasons: Sequence[Mapping[str, Any]]


@dataclass(frozen=True)
class SourceAuditWriteOutcome:
    audit: SourceAuditRecord
    created: bool


class SourceAuditRepository:
    async def list_audits(
        self,
        connection: AsyncConnection,
        *,
        source_id: int | None = None,
        decision: str | None = None,
        limit: int = 100,
    ) -> tuple[SourceAuditRecord, ...]:
        if not 1 <= limit <= 1000:
            raise ValueError("limit must be between 1 and 1000")
        statement = sa.select(source_audits)
        if source_id is not None:
            if source_id <= 0:
                raise ValueError("source_id must be positive")
            statement = statement.where(source_audits.c.source_id == source_id)
        if decision is not None:
            normalized_decision = decision.strip().lower()
            if normalized_decision not in {"approved", "rejected", "needs_review"}:
                raise ValueError(f"unknown source audit decision: {decision}")
            statement = statement.where(source_audits.c.decision == normalized_decision)
        rows = await connection.execute(
            statement.order_by(
                source_audits.c.audited_at.desc(), source_audits.c.id
            ).limit(limit)
        )
        return tuple(_record(row) for row in rows.mappings())

    async def get_by_key(
        self,
        connection: AsyncConnection,
        *,
        source_id: int,
        audit_key: str,
    ) -> SourceAuditRecord | None:
        row = (
            await connection.execute(
                sa.select(source_audits).where(
                    source_audits.c.source_id == source_id,
                    source_audits.c.audit_key == audit_key,
                )
            )
        ).mappings().one_or_none()
        return None if row is None else _record(row)

    async def list_for_source(
        self,
        connection: AsyncConnection,
        source_id: int,
    ) -> list[SourceAuditRecord]:
        rows = await connection.execute(
            sa.select(source_audits)
            .where(source_audits.c.source_id == source_id)
            .order_by(source_audits.c.audited_at, source_audits.c.id)
        )
        return [_record(row) for row in rows.mappings()]

    async def record(
        self,
        connection: AsyncConnection,
        audit: SourceAuditWrite,
    ) -> SourceAuditWriteOutcome:
        if await connection.scalar(
            sa.select(sources.c.id).where(sources.c.id == audit.source_id)
        ) is None:
            raise SourceNotFound(f"Source {audit.source_id} does not exist")

        values = _write_values(audit)
        audit_id = uuid4()
        inserted_id = await connection.scalar(
            pg_insert(source_audits)
            .values(id=audit_id, **values)
            .on_conflict_do_nothing(
                constraint="uq_source_audits_source_audit_key"
            )
            .returning(source_audits.c.id)
        )
        row = (
            await connection.execute(
                sa.select(source_audits).where(
                    source_audits.c.source_id == audit.source_id,
                    source_audits.c.audit_key == audit.audit_key,
                )
            )
        ).mappings().one()
        if inserted_id is None and not _matches(row, values):
            raise SourceAuditConflict(
                "Audit key already exists with a different strict source-audit result"
            )
        return SourceAuditWriteOutcome(
            audit=_record(row),
            created=inserted_id is not None,
        )

    async def assign_taxonomy(
        self,
        connection: AsyncConnection,
        *,
        source_id: int,
        dimension: str,
        terms: Sequence[Mapping[str, str]],
    ) -> None:
        for term in terms:
            key = str(term["key"])
            display_name = str(term["display_name"])
            term_id = await connection.scalar(
                pg_insert(source_taxonomy_terms)
                .values(
                    dimension=dimension,
                    key=key,
                    display_name=display_name,
                )
                .on_conflict_do_nothing(
                    constraint="uq_source_taxonomy_terms_dimension_key"
                )
                .returning(source_taxonomy_terms.c.id)
            )
            if term_id is None:
                term_id = await connection.scalar(
                    sa.select(source_taxonomy_terms.c.id).where(
                        source_taxonomy_terms.c.dimension == dimension,
                        source_taxonomy_terms.c.key == key,
                    )
                )
            await connection.execute(
                pg_insert(source_taxonomy_assignments)
                .values(source_id=source_id, term_id=term_id)
                .on_conflict_do_nothing()
            )


def _write_values(audit: SourceAuditWrite) -> dict[str, Any]:
    reasons = [dict(reason) for reason in audit.reasons]
    return {
        "source_id": audit.source_id,
        "audit_key": audit.audit_key,
        "schema_version": audit.schema_version,
        "provider": audit.provider,
        "model": audit.model,
        "analyzer_version": audit.analyzer_version,
        "audited_at": audit.audited_at,
        "window_started_at": audit.window_started_at,
        "window_ended_at": audit.window_ended_at,
        "sampled_from": audit.sampled_from,
        "sampled_to": audit.sampled_to,
        "sampled_message_count": audit.sampled_message_count,
        "probe_message_count": audit.probe_message_count,
        "expanded": audit.expanded,
        "high_volume": audit.high_volume,
        "sample_fingerprint": audit.sample_fingerprint,
        "commercial_opportunity_count": audit.commercial_opportunity_count,
        "buyer_intent_count": audit.buyer_intent_count,
        "seller_promotion_count": audit.seller_promotion_count,
        "ads_spam_count": audit.ads_spam_count,
        "duplicate_count": audit.duplicate_count,
        "content_mix": dict(audit.content_mix),
        "primary_language": audit.primary_language,
        "languages": [dict(term) for term in audit.languages],
        "categories": [dict(term) for term in audit.categories],
        "decision_policy": dict(audit.decision_policy),
        "decision": audit.decision,
        "reason_codes": [str(reason["code"]) for reason in reasons],
        "reasons": reasons,
    }


def _matches(row: Mapping[str, Any], values: Mapping[str, Any]) -> bool:
    return all(row[field] == value for field, value in values.items())


def _record(row: Mapping[str, Any]) -> SourceAuditRecord:
    return SourceAuditRecord(
        id=row["id"],
        source_id=int(row["source_id"]),
        audit_key=str(row["audit_key"]),
        schema_version=str(row["schema_version"]),
        provider=str(row["provider"]),
        model=str(row["model"]),
        analyzer_version=str(row["analyzer_version"]),
        audited_at=row["audited_at"],
        window_started_at=row["window_started_at"],
        window_ended_at=row["window_ended_at"],
        sampled_from=row["sampled_from"],
        sampled_to=row["sampled_to"],
        sampled_message_count=int(row["sampled_message_count"]),
        probe_message_count=int(row["probe_message_count"]),
        expanded=bool(row["expanded"]),
        high_volume=bool(row["high_volume"]),
        sample_fingerprint=str(row["sample_fingerprint"]),
        commercial_opportunity_count=int(row["commercial_opportunity_count"]),
        buyer_intent_count=int(row["buyer_intent_count"]),
        seller_promotion_count=int(row["seller_promotion_count"]),
        ads_spam_count=int(row["ads_spam_count"]),
        duplicate_count=int(row["duplicate_count"]),
        content_mix=dict(row["content_mix"]),
        primary_language=row["primary_language"],
        languages=tuple(dict(term) for term in row["languages"]),
        categories=tuple(dict(term) for term in row["categories"]),
        decision_policy=dict(row["decision_policy"]),
        decision=str(row["decision"]),
        reason_codes=tuple(str(code) for code in row["reason_codes"]),
        reasons=tuple(dict(reason) for reason in row["reasons"]),
        created_at=row["created_at"],
    )
