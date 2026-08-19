from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from hashlib import sha256
import json
import re
from uuid import UUID, uuid4

import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncConnection

from ..billing import EntitlementChecker, EntitlementDecision
from ..lead_renderer import TelegramLeadCard
from .jobs import DurableJobRepository, JobClaim
from .matches import MatchTraceRecord
from .schema import (
    match_traces,
    opportunities,
    personalized_deliveries,
    search_profiles,
    users,
)
from .search_profiles import SearchProfileConfirmationStatus, UserRecord
from .entitlements import TrialEntitlementChecker


PERSONALIZED_DELIVERY_SCHEMA_VERSION = "personalized-delivery.v1"
PERSONALIZED_DELIVERY_JOB_TYPE = "telegram.personalized_delivery.v1"
PERSONALIZED_DELIVERY_MAX_ATTEMPTS = 3
_FAILURE_CODE = re.compile(r"^[A-Za-z][A-Za-z0-9_.-]{0,63}$")


class DeliveryPersistenceConflict(RuntimeError):
    pass


class DeliveryStatus(str, Enum):
    QUEUED = "queued"
    SENDING = "sending"
    SENT = "sent"
    FAILED = "failed"
    SUPPRESSED = "suppressed"


@dataclass(frozen=True)
class PersonalizedDeliveryRecord:
    id: UUID
    idempotency_key: str
    schema_version: str
    renderer_schema_version: str
    match_trace_id: UUID
    match_run_id: UUID
    opportunity_id: UUID
    search_profile_id: UUID
    profile_revision: int
    user_id: UUID
    recipient_platform: str
    recipient_external_user_id: str
    job_id: UUID
    status: DeliveryStatus
    card_body_html: str
    source_url: str | None
    parse_mode: str
    link_preview: bool
    rendered_at: datetime
    attempt_count: int
    last_attempt_at: datetime | None
    failure_code: str | None
    telegram_message_id: int | None
    sent_at: datetime | None
    created_at: datetime
    updated_at: datetime


@dataclass(frozen=True)
class PersonalizedDeliveryWriteOutcome:
    delivery: PersonalizedDeliveryRecord
    created: bool


@dataclass(frozen=True)
class DeliveryAttempt:
    delivery: PersonalizedDeliveryRecord
    recipient_chat_id: int


class PersonalizedDeliveryRepository:
    def __init__(
        self,
        *,
        entitlement_checker: EntitlementChecker | None = None,
    ) -> None:
        self._entitlements = entitlement_checker or TrialEntitlementChecker()

    async def ensure(
        self,
        connection: AsyncConnection,
        *,
        jobs: DurableJobRepository,
        match: MatchTraceRecord,
        user: UserRecord,
        card: TelegramLeadCard,
        rendered_at: datetime,
    ) -> PersonalizedDeliveryWriteOutcome:
        recipient_chat_id = _telegram_chat_id(user)
        trace = match.trace
        if card.match_trace_id != match.id:
            raise ValueError("lead card does not belong to the match trace")
        if card.opportunity_id != trace.opportunity_id:
            raise ValueError("lead card does not belong to the Opportunity")
        if card.search_profile_id != trace.search_profile_id:
            raise ValueError("lead card does not belong to the SearchProfile")
        if card.profile_revision != trace.profile_revision:
            raise ValueError("lead card profile revision does not match the trace")

        # The match trace is audit evidence, not delivery identity.  A normal
        # Opportunity run and a profile rematch can independently produce
        # eligible traces for the same logical recipient.  Serialize the
        # logical identity so the lookup and insert are one concurrency-safe
        # boundary, including rows written before the logical key existed.
        idempotency_key = delivery_idempotency_key(match, card)
        await _lock_logical_delivery(connection, idempotency_key)
        existing = await self.get_by_logical_identity(
            connection,
            opportunity_id=trace.opportunity_id,
            search_profile_id=trace.search_profile_id,
            profile_revision=trace.profile_revision,
            renderer_schema_version=card.schema_version,
        )
        if existing is not None:
            _validate_existing(existing, match, user, card)
            return PersonalizedDeliveryWriteOutcome(
                delivery=existing,
                created=False,
            )

        delivery_id = uuid4()
        job_id = await jobs.enqueue(
            connection,
            job_type=PERSONALIZED_DELIVERY_JOB_TYPE,
            idempotency_key=idempotency_key,
            max_attempts=PERSONALIZED_DELIVERY_MAX_ATTEMPTS,
            correlation_id=delivery_id,
        )
        inserted_id = await connection.scalar(
            pg_insert(personalized_deliveries)
            .values(
                id=delivery_id,
                idempotency_key=idempotency_key,
                schema_version=PERSONALIZED_DELIVERY_SCHEMA_VERSION,
                renderer_schema_version=card.schema_version,
                match_trace_id=match.id,
                match_run_id=match.run_id,
                opportunity_id=trace.opportunity_id,
                search_profile_id=trace.search_profile_id,
                profile_revision=trace.profile_revision,
                user_id=user.id,
                recipient_platform=user.platform,
                recipient_external_user_id=str(recipient_chat_id),
                job_id=job_id,
                card_body_html=card.body_html,
                source_url=card.source_url,
                parse_mode=card.parse_mode,
                link_preview=card.link_preview,
                rendered_at=rendered_at,
            )
            .on_conflict_do_nothing(
                constraint="uq_personalized_deliveries_idempotency_key"
            )
            .returning(personalized_deliveries.c.id)
        )
        record = await self.get_by_idempotency_key(connection, idempotency_key)
        if record is None:
            raise DeliveryPersistenceConflict("delivery insert returned no record")
        _validate_existing(record, match, user, card)
        return PersonalizedDeliveryWriteOutcome(
            delivery=record,
            created=inserted_id is not None,
        )

    async def get(
        self,
        connection: AsyncConnection,
        delivery_id: UUID,
    ) -> PersonalizedDeliveryRecord | None:
        row = (
            await connection.execute(
                sa.select(personalized_deliveries).where(
                    personalized_deliveries.c.id == delivery_id
                )
            )
        ).mappings().one_or_none()
        return None if row is None else _record(row)

    async def get_by_idempotency_key(
        self,
        connection: AsyncConnection,
        idempotency_key: str,
    ) -> PersonalizedDeliveryRecord | None:
        row = (
            await connection.execute(
                sa.select(personalized_deliveries).where(
                    personalized_deliveries.c.idempotency_key == idempotency_key
                )
            )
        ).mappings().one_or_none()
        return None if row is None else _record(row)

    async def get_by_logical_identity(
        self,
        connection: AsyncConnection,
        *,
        opportunity_id: UUID,
        search_profile_id: UUID,
        profile_revision: int,
        renderer_schema_version: str,
    ) -> PersonalizedDeliveryRecord | None:
        row = (
            await connection.execute(
                sa.select(personalized_deliveries)
                .where(
                    personalized_deliveries.c.opportunity_id == opportunity_id,
                    personalized_deliveries.c.search_profile_id == search_profile_id,
                    personalized_deliveries.c.profile_revision == profile_revision,
                    personalized_deliveries.c.schema_version
                    == PERSONALIZED_DELIVERY_SCHEMA_VERSION,
                    personalized_deliveries.c.renderer_schema_version
                    == renderer_schema_version,
                )
                .order_by(
                    personalized_deliveries.c.created_at,
                    personalized_deliveries.c.id,
                )
                .limit(1)
            )
        ).mappings().one_or_none()
        return None if row is None else _record(row)

    async def get_by_job(
        self,
        connection: AsyncConnection,
        job_id: UUID,
    ) -> PersonalizedDeliveryRecord | None:
        row = (
            await connection.execute(
                sa.select(personalized_deliveries).where(
                    personalized_deliveries.c.job_id == job_id
                )
            )
        ).mappings().one_or_none()
        return None if row is None else _record(row)

    async def list_for_run(
        self,
        connection: AsyncConnection,
        run_id: UUID,
    ) -> tuple[PersonalizedDeliveryRecord, ...]:
        rows = (
            await connection.execute(
                sa.select(personalized_deliveries)
                .where(personalized_deliveries.c.match_run_id == run_id)
                .order_by(
                    personalized_deliveries.c.search_profile_id,
                    personalized_deliveries.c.opportunity_id,
                    personalized_deliveries.c.id,
                )
            )
        ).mappings().all()
        return tuple(_record(row) for row in rows)

    async def list_deliveries(
        self,
        connection: AsyncConnection,
        *,
        status: DeliveryStatus | str | None = None,
        limit: int = 100,
    ) -> tuple[PersonalizedDeliveryRecord, ...]:
        if not 1 <= limit <= 1000:
            raise ValueError("limit must be between 1 and 1000")
        statement = sa.select(personalized_deliveries)
        if status is not None:
            try:
                normalized_status = DeliveryStatus(status)
            except ValueError:
                raise ValueError(f"unknown delivery status: {status}") from None
            statement = statement.where(
                personalized_deliveries.c.status == normalized_status.value
            )
        rows = await connection.execute(
            statement.order_by(
                personalized_deliveries.c.created_at.desc(),
                personalized_deliveries.c.id,
            ).limit(limit)
        )
        return tuple(_record(row) for row in rows.mappings())

    async def prepare_attempt(
        self,
        connection: AsyncConnection,
        claim: JobClaim,
    ) -> DeliveryAttempt | None:
        delivery_row = (
            await connection.execute(
                sa.select(personalized_deliveries)
                .where(personalized_deliveries.c.job_id == claim.id)
                .with_for_update()
            )
        ).mappings().one_or_none()
        if delivery_row is None:
            raise DeliveryPersistenceConflict("delivery job has no delivery record")
        current = _record(delivery_row)
        if current.status in {
            DeliveryStatus.SENT,
            DeliveryStatus.FAILED,
            DeliveryStatus.SUPPRESSED,
        }:
            return None
        if (
            current.status is DeliveryStatus.SENDING
            and current.attempt_count >= claim.attempt_count
        ):
            return None

        eligibility = (
            await connection.execute(
                sa.select(
                    search_profiles.c.user_id.label("profile_user_id"),
                    search_profiles.c.revision.label("current_profile_revision"),
                    search_profiles.c.confirmation_status,
                    search_profiles.c.is_active,
                    search_profiles.c.is_primary,
                    users.c.platform.label("current_recipient_platform"),
                    users.c.external_user_id.label("current_external_user_id"),
                    opportunities.c.lifecycle_status,
                    opportunities.c.last_seen_at.label("current_last_seen_at"),
                    match_traces.c.eligible.label("trace_eligible"),
                    match_traces.c.hard_filter_eligible,
                    match_traces.c.decision_code,
                    match_traces.c.opportunity_last_seen_at,
                )
                .select_from(
                    personalized_deliveries.join(
                        search_profiles,
                        personalized_deliveries.c.search_profile_id
                        == search_profiles.c.id,
                    )
                    .join(users, personalized_deliveries.c.user_id == users.c.id)
                    .join(
                        opportunities,
                        personalized_deliveries.c.opportunity_id
                        == opportunities.c.id,
                    )
                    .join(
                        match_traces,
                        personalized_deliveries.c.match_trace_id
                        == match_traces.c.id,
                    )
                )
                .where(personalized_deliveries.c.id == current.id)
            )
        ).mappings().one()
        entitlement = await self._entitlements.check(connection, current.user_id)
        suppression_code = _suppression_code(
            current,
            eligibility,
            entitlement=entitlement,
        )
        if suppression_code is not None:
            await connection.execute(
                personalized_deliveries.update()
                .where(personalized_deliveries.c.id == current.id)
                .values(
                    status=DeliveryStatus.SUPPRESSED.value,
                    attempt_count=claim.attempt_count,
                    last_attempt_at=sa.func.now(),
                    failure_code=suppression_code,
                    updated_at=sa.func.now(),
                )
            )
            return None

        row = (
            await connection.execute(
                personalized_deliveries.update()
                .where(personalized_deliveries.c.id == current.id)
                .values(
                    status=DeliveryStatus.SENDING.value,
                    attempt_count=claim.attempt_count,
                    last_attempt_at=sa.func.now(),
                    failure_code=None,
                    updated_at=sa.func.now(),
                )
                .returning(personalized_deliveries)
            )
        ).mappings().one()
        return DeliveryAttempt(
            delivery=_record(row),
            recipient_chat_id=int(current.recipient_external_user_id),
        )

    async def mark_sent(
        self,
        connection: AsyncConnection,
        claim: JobClaim,
        *,
        telegram_message_id: int,
    ) -> PersonalizedDeliveryRecord:
        if telegram_message_id <= 0:
            raise ValueError("telegram_message_id must be positive")
        row = (
            await connection.execute(
                personalized_deliveries.update()
                .where(
                    personalized_deliveries.c.job_id == claim.id,
                    personalized_deliveries.c.status
                    == DeliveryStatus.SENDING.value,
                    personalized_deliveries.c.attempt_count
                    == claim.attempt_count,
                )
                .values(
                    status=DeliveryStatus.SENT.value,
                    telegram_message_id=telegram_message_id,
                    sent_at=sa.func.now(),
                    failure_code=None,
                    updated_at=sa.func.now(),
                )
                .returning(personalized_deliveries)
            )
        ).mappings().one_or_none()
        if row is not None:
            return _record(row)
        existing = await self.get_by_job(connection, claim.id)
        if existing is not None and existing.status is DeliveryStatus.SENT:
            return existing
        raise DeliveryPersistenceConflict("delivery send confirmation lost ownership")

    async def mark_attempt_failed(
        self,
        connection: AsyncConnection,
        claim: JobClaim,
        *,
        failure_code: str,
    ) -> PersonalizedDeliveryRecord:
        code = _safe_failure_code(failure_code)
        terminal = claim.attempt_count >= claim.max_attempts
        row = (
            await connection.execute(
                personalized_deliveries.update()
                .where(
                    personalized_deliveries.c.job_id == claim.id,
                    personalized_deliveries.c.status
                    == DeliveryStatus.SENDING.value,
                    personalized_deliveries.c.attempt_count
                    == claim.attempt_count,
                )
                .values(
                    status=(
                        DeliveryStatus.FAILED.value
                        if terminal
                        else DeliveryStatus.QUEUED.value
                    ),
                    failure_code=code,
                    updated_at=sa.func.now(),
                )
                .returning(personalized_deliveries)
            )
        ).mappings().one_or_none()
        if row is None:
            raise DeliveryPersistenceConflict("delivery failure lost ownership")
        return _record(row)


def delivery_idempotency_key(
    match: MatchTraceRecord,
    card: TelegramLeadCard,
) -> str:
    payload = {
        "delivery_schema_version": PERSONALIZED_DELIVERY_SCHEMA_VERSION,
        "delivery_identity_version": "opportunity-profile-revision.v1",
        "renderer_schema_version": card.schema_version,
        "opportunity_id": str(match.trace.opportunity_id),
        "search_profile_id": str(match.trace.search_profile_id),
        "profile_revision": match.trace.profile_revision,
    }
    encoded = json.dumps(
        payload,
        ensure_ascii=True,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return sha256(encoded).hexdigest()


async def _lock_logical_delivery(
    connection: AsyncConnection,
    idempotency_key: str,
) -> None:
    await connection.execute(
        sa.text(
            "SELECT pg_advisory_xact_lock(hashtextextended(:lock_key, 0))"
        ),
        {"lock_key": f"personalized-delivery:{idempotency_key}"},
    )


def _telegram_chat_id(user: UserRecord) -> int:
    if user.platform != "telegram":
        raise ValueError("personalized Telegram delivery requires a Telegram user")
    if not user.external_user_id.isascii() or not user.external_user_id.isdecimal():
        raise ValueError("Telegram user identity must be a positive numeric chat id")
    chat_id = int(user.external_user_id)
    if (
        chat_id <= 0
        or len(user.external_user_id) > 20
        or str(chat_id) != user.external_user_id
    ):
        raise ValueError("Telegram user identity must be a positive numeric chat id")
    return chat_id


def _suppression_code(
    current: PersonalizedDeliveryRecord,
    row,
    *,
    entitlement: EntitlementDecision,
) -> str | None:
    if row["profile_user_id"] != current.user_id:
        return "ProfileOwnerChanged"
    if not entitlement.can_receive_deliveries:
        return entitlement.failure_code or "EntitlementRequired"
    if (
        row["current_profile_revision"] != current.profile_revision
        or row["confirmation_status"]
        != SearchProfileConfirmationStatus.CONFIRMED.value
        or not row["is_active"]
        or not row["is_primary"]
    ):
        return "ProfileIneligible"
    if (
        row["current_recipient_platform"] != current.recipient_platform
        or row["current_external_user_id"]
        != current.recipient_external_user_id
    ):
        return "RecipientChanged"
    if row["lifecycle_status"] != "active":
        return "OpportunityIneligible"
    if (
        not row["trace_eligible"]
        or not row["hard_filter_eligible"]
        or row["decision_code"] != "eligible"
        or row["current_last_seen_at"] != row["opportunity_last_seen_at"]
    ):
        return "MatchTraceStale"
    return None


def _validate_existing(
    record: PersonalizedDeliveryRecord,
    match: MatchTraceRecord,
    user: UserRecord,
    card: TelegramLeadCard,
) -> None:
    expected = (
        match.trace.opportunity_id,
        match.trace.search_profile_id,
        match.trace.profile_revision,
        user.id,
        card.schema_version,
    )
    actual = (
        record.opportunity_id,
        record.search_profile_id,
        record.profile_revision,
        record.user_id,
        record.renderer_schema_version,
    )
    if actual != expected:
        raise DeliveryPersistenceConflict(
            "delivery logical identity exists with different content"
        )


def _safe_failure_code(value: str) -> str:
    return value if _FAILURE_CODE.fullmatch(value) else "DeliverySendError"


def _record(row) -> PersonalizedDeliveryRecord:
    return PersonalizedDeliveryRecord(
        id=row["id"],
        idempotency_key=row["idempotency_key"],
        schema_version=row["schema_version"],
        renderer_schema_version=row["renderer_schema_version"],
        match_trace_id=row["match_trace_id"],
        match_run_id=row["match_run_id"],
        opportunity_id=row["opportunity_id"],
        search_profile_id=row["search_profile_id"],
        profile_revision=row["profile_revision"],
        user_id=row["user_id"],
        recipient_platform=row["recipient_platform"],
        recipient_external_user_id=row["recipient_external_user_id"],
        job_id=row["job_id"],
        status=DeliveryStatus(row["status"]),
        card_body_html=row["card_body_html"],
        source_url=row["source_url"],
        parse_mode=row["parse_mode"],
        link_preview=row["link_preview"],
        rendered_at=row["rendered_at"],
        attempt_count=row["attempt_count"],
        last_attempt_at=row["last_attempt_at"],
        failure_code=row["failure_code"],
        telegram_message_id=row["telegram_message_id"],
        sent_at=row["sent_at"],
        created_at=row["created_at"],
        updated_at=row["updated_at"],
    )
