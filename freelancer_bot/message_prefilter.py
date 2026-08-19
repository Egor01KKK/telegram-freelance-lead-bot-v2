from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import Enum
from hashlib import sha256
import unicodedata
from uuid import UUID

from sqlalchemy.ext.asyncio import AsyncConnection

from .observability import log_event
from .opportunity_analysis import (
    OPPORTUNITY_ANALYSIS_SCHEMA_VERSION,
    OPPORTUNITY_ANALYZER_VERSION,
)
from .persistence.database import Database
from .persistence.jobs import DurableJobRepository, JobClaim
from .persistence.message_prefilter import (
    MessagePrefilterRepository,
    PrefilterResultRecord,
)
from .persistence.raw_messages import (
    RAW_MESSAGE_JOB_TYPE,
    RawMessageRecord,
    RawMessageRepository,
)


PREFILTER_SCHEMA_VERSION = "message-prefilter.v1"
OPPORTUNITY_ANALYSIS_JOB_TYPE = "opportunity.analysis.v1"
LOGGER = logging.getLogger("freelancer_bot")


class PrefilterDecision(str, Enum):
    PASSED = "passed"
    REJECTED = "rejected"


class PrefilterReason(str, Enum):
    EMPTY_CONTENT = "empty_content"
    SERVICE_EVENT = "service_event"


@dataclass(frozen=True)
class PrefilterEvaluation:
    decision: PrefilterDecision
    reason_codes: tuple[PrefilterReason, ...] = ()


@dataclass(frozen=True)
class AnalyzerMessage:
    raw_message_id: UUID
    source_id: int
    external_source_id: str
    external_message_id: int
    message_date: datetime
    message_url: str
    content: str


@dataclass(frozen=True)
class MinimalAnalyzerInput:
    current: AnalyzerMessage
    parent: AnalyzerMessage | None


@dataclass(frozen=True)
class ExactDedupPolicy:
    window: timedelta = timedelta(days=7)

    def __post_init__(self) -> None:
        if self.window <= timedelta(0):
            raise ValueError("Exact dedup window must be positive")
        if self.window.total_seconds() != int(self.window.total_seconds()):
            raise ValueError("Exact dedup window must use whole seconds")

    @property
    def window_seconds(self) -> int:
        return int(self.window.total_seconds())


@dataclass(frozen=True)
class ExactDedupRoute:
    analysis_job_id: UUID
    canonical_prefilter_result_id: UUID | None
    normalized_content: str
    normalized_content_sha256: str
    analysis_input_sha256: str
    dedup_relation: str


class CheapMessagePrefilter:
    """Reject only transport-level cases that cannot contain a text opportunity."""

    def evaluate(self, message: RawMessageRecord) -> PrefilterEvaluation:
        service_type = message.transport_metadata.get("service_action_type")
        if isinstance(service_type, str) and service_type.strip():
            return PrefilterEvaluation(
                PrefilterDecision.REJECTED,
                (PrefilterReason.SERVICE_EVENT,),
            )
        if not message.content.strip():
            return PrefilterEvaluation(
                PrefilterDecision.REJECTED,
                (PrefilterReason.EMPTY_CONTENT,),
            )
        return PrefilterEvaluation(PrefilterDecision.PASSED)


class RawMessagePrefilterProcessor:
    def __init__(
        self,
        database: Database,
        *,
        prefilter: CheapMessagePrefilter | None = None,
        messages: RawMessageRepository | None = None,
        results: MessagePrefilterRepository | None = None,
        jobs: DurableJobRepository | None = None,
        dedup_policy: ExactDedupPolicy | None = None,
        analyzer_version: str = OPPORTUNITY_ANALYZER_VERSION,
        analysis_schema_version: str = OPPORTUNITY_ANALYSIS_SCHEMA_VERSION,
        logger: logging.Logger | None = None,
    ) -> None:
        self._database = database
        self._prefilter = prefilter or CheapMessagePrefilter()
        self._messages = messages or RawMessageRepository()
        self._results = results or MessagePrefilterRepository()
        self._jobs = jobs or DurableJobRepository()
        self._dedup_policy = dedup_policy or ExactDedupPolicy()
        self._analyzer_version = _required_version(
            analyzer_version,
            "analyzer_version",
            64,
        )
        self._analysis_schema_version = _required_version(
            analysis_schema_version,
            "analysis_schema_version",
            32,
        )
        self._logger = logger or LOGGER

    async def __call__(self, claim: JobClaim) -> None:
        await self.process(claim)

    async def process(self, claim: JobClaim) -> PrefilterResultRecord:
        if claim.job_type != RAW_MESSAGE_JOB_TYPE:
            raise ValueError("Prefilter processor received an unsupported job type")

        async with self._database.transaction() as connection:
            raw = await self._messages.get_for_job(connection, claim.id)
            if raw is None:
                raise LookupError("Raw message is missing for durable ingestion job")
            existing = await self._results.get_for_raw(
                connection,
                raw_message_id=raw.id,
                schema_version=PREFILTER_SCHEMA_VERSION,
            )
            if existing is not None:
                result = existing
                created = False
            else:
                evaluation = self._prefilter.evaluate(raw)
                parent_raw_message_id = None
                analysis_job_id = None
                canonical_prefilter_result_id = None
                normalized_content = None
                normalized_content_sha256 = None
                analysis_input_sha256 = None
                dedup_relation = None
                if evaluation.decision is PrefilterDecision.PASSED:
                    parent = await self._direct_parent(connection, raw)
                    parent_raw_message_id = None if parent is None else parent.id
                    route = await self._route_exact_content(
                        connection,
                        raw=raw,
                        parent=parent,
                        correlation_id=claim.correlation_id,
                    )
                    analysis_job_id = route.analysis_job_id
                    canonical_prefilter_result_id = (
                        route.canonical_prefilter_result_id
                    )
                    normalized_content = route.normalized_content
                    normalized_content_sha256 = route.normalized_content_sha256
                    analysis_input_sha256 = route.analysis_input_sha256
                    dedup_relation = route.dedup_relation
                outcome = await self._results.record(
                    connection,
                    raw_message_id=raw.id,
                    parent_raw_message_id=parent_raw_message_id,
                    analysis_job_id=analysis_job_id,
                    schema_version=PREFILTER_SCHEMA_VERSION,
                    decision=evaluation.decision.value,
                    reason_codes=tuple(
                        reason.value for reason in evaluation.reason_codes
                    ),
                    canonical_prefilter_result_id=canonical_prefilter_result_id,
                    normalized_content=normalized_content,
                    normalized_content_sha256=normalized_content_sha256,
                    analysis_input_sha256=analysis_input_sha256,
                    analyzer_version=(
                        self._analyzer_version
                        if analysis_job_id is not None
                        else None
                    ),
                    analysis_schema_version=(
                        self._analysis_schema_version
                        if analysis_job_id is not None
                        else None
                    ),
                    dedup_relation=dedup_relation,
                    dedup_window_seconds=(
                        self._dedup_policy.window_seconds
                        if analysis_job_id is not None
                        else None
                    ),
                )
                result = outcome.result
                created = outcome.created

        log_event(
            self._logger,
            logging.INFO,
            "telegram.prefilter.completed",
            raw_message_id=result.raw_message_id,
            decision=result.decision,
            reason_codes=result.reason_codes,
            analysis_job_id=result.analysis_job_id,
            dedup_relation=result.dedup_relation,
            parent_context_included=result.parent_raw_message_id is not None,
            created=created,
        )
        return result

    async def _direct_parent(
        self,
        connection: AsyncConnection,
        raw: RawMessageRecord,
    ) -> RawMessageRecord | None:
        reply_to_message_id = _reply_to_message_id(raw)
        if reply_to_message_id is None:
            return None
        return await self._messages.get_by_source_message(
            connection,
            source_id=raw.source_id,
            external_message_id=reply_to_message_id,
        )

    async def _route_exact_content(
        self,
        connection: AsyncConnection,
        *,
        raw: RawMessageRecord,
        parent: RawMessageRecord | None,
        correlation_id: UUID,
    ) -> ExactDedupRoute:
        normalized = normalize_exact_content(raw.content)
        normalized_parent = (
            None if parent is None else normalize_exact_content(parent.content)
        )
        content_hash = _sha256(normalized)
        input_hash = _analysis_input_hash(normalized, normalized_parent)
        compatibility_key = ":".join(
            (
                PREFILTER_SCHEMA_VERSION,
                self._analyzer_version,
                self._analysis_schema_version,
                content_hash,
                input_hash,
            )
        )
        await self._results.lock_exact_key(connection, compatibility_key)
        concurrent_result = await self._results.get_for_raw(
            connection,
            raw_message_id=raw.id,
            schema_version=PREFILTER_SCHEMA_VERSION,
        )
        if concurrent_result is not None:
            required = (
                concurrent_result.analysis_job_id,
                concurrent_result.normalized_content,
                concurrent_result.normalized_content_sha256,
                concurrent_result.analysis_input_sha256,
                concurrent_result.dedup_relation,
            )
            if (
                concurrent_result.decision != PrefilterDecision.PASSED.value
                or any(value is None for value in required)
            ):
                raise RuntimeError("Concurrent prefilter result is incompatible")
            return ExactDedupRoute(
                analysis_job_id=concurrent_result.analysis_job_id,
                canonical_prefilter_result_id=(
                    concurrent_result.canonical_prefilter_result_id
                ),
                normalized_content=concurrent_result.normalized_content,
                normalized_content_sha256=(
                    concurrent_result.normalized_content_sha256
                ),
                analysis_input_sha256=concurrent_result.analysis_input_sha256,
                dedup_relation=concurrent_result.dedup_relation,
            )
        canonical = await self._results.find_recent_canonical(
            connection,
            normalized_content=normalized,
            normalized_content_sha256=content_hash,
            analysis_input_sha256=input_hash,
            analyzer_version=self._analyzer_version,
            analysis_schema_version=self._analysis_schema_version,
            window=self._dedup_policy.window,
        )
        if canonical is not None:
            if canonical.analysis_job_id is None:
                raise RuntimeError("Canonical prefilter result has no analysis job")
            return ExactDedupRoute(
                analysis_job_id=canonical.analysis_job_id,
                canonical_prefilter_result_id=canonical.id,
                normalized_content=normalized,
                normalized_content_sha256=content_hash,
                analysis_input_sha256=input_hash,
                dedup_relation="exact_duplicate",
            )

        analysis_job_id = await self._jobs.enqueue(
            connection,
            job_type=OPPORTUNITY_ANALYSIS_JOB_TYPE,
            idempotency_key=f"{PREFILTER_SCHEMA_VERSION}:{raw.id}",
            correlation_id=correlation_id,
        )
        return ExactDedupRoute(
            analysis_job_id=analysis_job_id,
            canonical_prefilter_result_id=None,
            normalized_content=normalized,
            normalized_content_sha256=content_hash,
            analysis_input_sha256=input_hash,
            dedup_relation="canonical",
        )


class AnalyzerInputLoader:
    def __init__(
        self,
        database: Database,
        *,
        messages: RawMessageRepository | None = None,
        results: MessagePrefilterRepository | None = None,
    ) -> None:
        self._database = database
        self._messages = messages or RawMessageRepository()
        self._results = results or MessagePrefilterRepository()

    async def load(self, analysis_job_id: UUID) -> MinimalAnalyzerInput:
        async with self._database.connect() as connection:
            result = await self._results.get_canonical_for_analysis_job(
                connection,
                analysis_job_id,
            )
            if result is None or result.decision != PrefilterDecision.PASSED.value:
                raise LookupError("Analysis job has no passed prefilter input")
            current = await self._messages.get_by_id(
                connection,
                result.raw_message_id,
            )
            if current is None:
                raise LookupError("Current raw message is missing")
            parent = (
                None
                if result.parent_raw_message_id is None
                else await self._messages.get_by_id(
                    connection,
                    result.parent_raw_message_id,
                )
            )
            if result.parent_raw_message_id is not None and parent is None:
                raise LookupError("Parent raw message is missing")
            return MinimalAnalyzerInput(
                current=_analyzer_message(current),
                parent=None if parent is None else _analyzer_message(parent),
            )


def _reply_to_message_id(message: RawMessageRecord) -> int | None:
    value = message.transport_metadata.get("reply_to_msg_id")
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        return None
    return value


def _analyzer_message(message: RawMessageRecord) -> AnalyzerMessage:
    return AnalyzerMessage(
        raw_message_id=message.id,
        source_id=message.source_id,
        external_source_id=message.external_source_id,
        external_message_id=message.external_message_id,
        message_date=message.message_date,
        message_url=message.message_url,
        content=message.content,
    )


def normalize_exact_content(content: str) -> str:
    """Normalize only deterministic Unicode, case and whitespace differences."""
    return " ".join(unicodedata.normalize("NFKC", content).casefold().split())


def _sha256(value: str) -> str:
    return sha256(value.encode("utf-8")).hexdigest()


def _analysis_input_hash(current: str, parent: str | None) -> str:
    parent_value = "<none>" if parent is None else parent
    return _sha256(f"current\0{current}\0parent\0{parent_value}")


def _required_version(value: str, name: str, limit: int) -> str:
    normalized = value.strip().lower()
    if (
        not normalized
        or len(normalized) > limit
        or not normalized[0].isalnum()
        or any(
            not character.isascii()
            or (not character.isalnum() and character not in "._-")
            for character in normalized
        )
    ):
        raise ValueError(f"{name} must be a safe version identifier")
    return normalized
