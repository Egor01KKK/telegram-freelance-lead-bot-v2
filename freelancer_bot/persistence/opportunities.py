from __future__ import annotations

from collections import Counter
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta
from decimal import Decimal
from difflib import SequenceMatcher
from enum import Enum
from hashlib import sha256
import json
import re
from typing import Any
import unicodedata
from uuid import UUID, uuid4

import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncConnection

from ..opportunity_dedup import (
    PREFERRED_SOURCE_POLICY_VERSION,
    STRUCTURED_DEDUP_ALGORITHM_VERSION,
    STRUCTURED_DEDUP_RELATION,
    StructuredDedupPolicy,
    evaluate_structured_duplicate,
)
from ..opportunity_analysis import OpportunityAnalysis
from .schema import (
    opportunities,
    opportunity_analysis_cache,
    opportunity_analysis_links,
    opportunity_lifecycle_events,
    opportunity_source_messages,
    raw_messages,
    sources,
)


CANONICAL_OPPORTUNITY_SCHEMA_VERSION = "canonical_opportunity.v1"
OPPORTUNITY_DEDUP_ALGORITHM_VERSION = "canonical-opportunity-dedup.v1"
CANONICAL_DEDUP_RELATION = "canonical"
EXACT_DEDUP_RELATION = "exact_duplicate"
NEAR_DEDUP_RELATION = "near_duplicate"

_DEDUP_TOKEN_PATTERN = re.compile(r"[^\W_]+(?:[.+#-][^\W_]+)*", re.UNICODE)


class OpportunityPersistenceError(RuntimeError):
    pass


class OpportunityLinkConflict(OpportunityPersistenceError):
    pass


class OpportunityNotFound(LookupError):
    pass


class InvalidOpportunityTransition(ValueError):
    pass


class OpportunityLifecycleStatus(str, Enum):
    ACTIVE = "active"
    STALE = "stale"
    CLOSED = "closed"
    RETRACTED = "retracted"
    SUPPRESSED = "suppressed"


_VALID_LIFECYCLE_TRANSITIONS: dict[
    OpportunityLifecycleStatus,
    frozenset[OpportunityLifecycleStatus],
] = {
    OpportunityLifecycleStatus.ACTIVE: frozenset(
        {
            OpportunityLifecycleStatus.STALE,
            OpportunityLifecycleStatus.CLOSED,
            OpportunityLifecycleStatus.RETRACTED,
            OpportunityLifecycleStatus.SUPPRESSED,
        }
    ),
    OpportunityLifecycleStatus.STALE: frozenset(
        {
            OpportunityLifecycleStatus.ACTIVE,
            OpportunityLifecycleStatus.CLOSED,
            OpportunityLifecycleStatus.RETRACTED,
            OpportunityLifecycleStatus.SUPPRESSED,
        }
    ),
    OpportunityLifecycleStatus.CLOSED: frozenset(
        {
            OpportunityLifecycleStatus.ACTIVE,
            OpportunityLifecycleStatus.RETRACTED,
            OpportunityLifecycleStatus.SUPPRESSED,
        }
    ),
    OpportunityLifecycleStatus.RETRACTED: frozenset(
        {
            OpportunityLifecycleStatus.ACTIVE,
            OpportunityLifecycleStatus.SUPPRESSED,
        }
    ),
    OpportunityLifecycleStatus.SUPPRESSED: frozenset(
        {OpportunityLifecycleStatus.ACTIVE}
    ),
}


@dataclass(frozen=True)
class OpportunityDedupPolicy:
    window: timedelta = timedelta(days=7)
    near_similarity_threshold: float = 0.92
    minimum_near_tokens: int = 8
    candidate_limit: int = 500
    structured: StructuredDedupPolicy = StructuredDedupPolicy()

    def __post_init__(self) -> None:
        if self.window <= timedelta(0):
            raise ValueError("Opportunity dedup window must be positive")
        if self.window.total_seconds() != int(self.window.total_seconds()):
            raise ValueError("Opportunity dedup window must use whole seconds")
        if not 0.5 <= self.near_similarity_threshold < 1:
            raise ValueError("Near-duplicate threshold must be between 0.5 and 1")
        if self.minimum_near_tokens < 3:
            raise ValueError("Near-duplicate minimum token count must be at least 3")
        if not 1 <= self.candidate_limit <= 2_000:
            raise ValueError("Near-duplicate candidate limit must be 1..2000")

    @property
    def window_seconds(self) -> int:
        return int(self.window.total_seconds())


@dataclass(frozen=True)
class OpportunityAnalysisLinkRecord:
    analysis_cache_id: UUID
    dedup_relation: str
    dedup_algorithm_version: str
    normalized_text_sha256: str
    dedup_similarity: float | None
    dedup_window_seconds: int
    dedup_evidence: Mapping[str, Any]
    matched_analysis_cache_id: UUID | None
    linked_at: datetime


@dataclass(frozen=True)
class OpportunitySourceObservationRecord:
    raw_message_id: UUID
    source_id: int
    platform: str
    external_source_id: str
    source_display_name: str
    source_handle: str | None
    source_canonical_url: str | None
    message_url: str
    message_date: datetime
    observed_at: datetime
    linked_at: datetime
    is_preferred: bool


@dataclass(frozen=True)
class OpportunityLifecycleEventRecord:
    id: int
    opportunity_id: UUID
    from_status: OpportunityLifecycleStatus | None
    to_status: OpportunityLifecycleStatus
    evidence_raw_message_id: UUID | None
    actor_kind: str
    actor_id: str | None
    reason: str
    changed_at: datetime


@dataclass(frozen=True)
class CanonicalOpportunityRecord:
    id: UUID
    schema_version: str
    canonical_title: str | None
    task_summary: str | None
    analysis: OpportunityAnalysis
    first_seen_at: datetime
    last_seen_at: datetime
    lifecycle_status: OpportunityLifecycleStatus
    lifecycle_changed_at: datetime
    raw_message_ids: tuple[UUID, ...]
    analysis_cache_ids: tuple[UUID, ...]
    analysis_links: tuple[OpportunityAnalysisLinkRecord, ...]
    preferred_source_policy_version: str | None
    preferred_source: OpportunitySourceObservationRecord | None
    source_observations: tuple[OpportunitySourceObservationRecord, ...]
    lifecycle_events: tuple[OpportunityLifecycleEventRecord, ...]
    created_at: datetime
    updated_at: datetime

    @property
    def alternate_sources(self) -> tuple[OpportunitySourceObservationRecord, ...]:
        return tuple(
            observation
            for observation in self.source_observations
            if not observation.is_preferred
        )

    @property
    def source_message_urls(self) -> tuple[str, ...]:
        return tuple(
            observation.message_url for observation in self.source_observations
        )


@dataclass(frozen=True)
class CanonicalOpportunityWriteOutcome:
    opportunity: CanonicalOpportunityRecord
    created: bool
    linked_message_count: int
    dedup_relation: str
    dedup_similarity: float | None
    dedup_evidence: Mapping[str, Any]


@dataclass(frozen=True)
class OpportunityLifecycleWriteOutcome:
    opportunity: CanonicalOpportunityRecord
    changed: bool
    event: OpportunityLifecycleEventRecord | None


@dataclass(frozen=True)
class _DedupMatch:
    opportunity_id: UUID
    analysis_cache_id: UUID
    relation: str
    similarity: float
    algorithm_version: str
    evidence: Mapping[str, Any]


class CanonicalOpportunityRepository:
    def __init__(self, *, dedup_policy: OpportunityDedupPolicy | None = None) -> None:
        self._dedup_policy = dedup_policy or OpportunityDedupPolicy()

    async def ensure_from_analysis(
        self,
        connection: AsyncConnection,
        *,
        analysis_cache_id: UUID,
        raw_message_ids: Sequence[UUID],
        analysis: OpportunityAnalysis,
    ) -> CanonicalOpportunityWriteOutcome:
        if not analysis.is_opportunity:
            raise ValueError("Only positive buyer-demand analysis can be canonicalized")
        message_ids = tuple(dict.fromkeys(raw_message_ids))
        if not message_ids:
            raise ValueError("Canonical opportunity requires at least one raw message")

        cache = (
            await connection.execute(
                sa.select(
                    opportunity_analysis_cache.c.id,
                    opportunity_analysis_cache.c.normalized_content,
                ).where(opportunity_analysis_cache.c.id == analysis_cache_id)
            )
        ).mappings().one_or_none()
        if cache is None:
            raise LookupError("Opportunity analysis cache entry does not exist")
        normalized_text = normalize_opportunity_dedup_text(cache["normalized_content"])
        normalized_text_hash = _sha256(normalized_text)

        observations = (
            await connection.execute(
                sa.select(raw_messages.c.id, raw_messages.c.observed_at)
                .where(raw_messages.c.id.in_(message_ids))
                .with_for_update()
            )
        ).mappings().all()
        if len(observations) != len(message_ids):
            raise LookupError("One or more raw opportunity messages do not exist")
        first_seen_at = min(row["observed_at"] for row in observations)
        last_seen_at = max(row["observed_at"] for row in observations)
        observation_times = {
            row["id"]: row["observed_at"] for row in observations
        }

        # Near variants have different hashes, so candidate selection and link
        # creation share one versioned transaction lock.
        await connection.execute(
            sa.select(
                sa.func.pg_advisory_xact_lock(
                    sa.func.hashtextextended(OPPORTUNITY_DEDUP_ALGORITHM_VERSION, 0)
                )
            )
        )
        existing_link = (
            await connection.execute(
                sa.select(opportunity_analysis_links).where(
                    opportunity_analysis_links.c.analysis_cache_id
                    == analysis_cache_id
                )
            )
        ).mappings().one_or_none()
        opportunity_id = (
            None if existing_link is None else existing_link["opportunity_id"]
        )
        created = False
        if existing_link is None:
            match = await self._find_duplicate(
                connection,
                analysis_cache_id=analysis_cache_id,
                normalized_text=normalized_text,
                normalized_text_hash=normalized_text_hash,
                first_seen_at=first_seen_at,
                last_seen_at=last_seen_at,
                analysis=analysis,
            )
            opportunity_id = None if match is None else match.opportunity_id
            relation = CANONICAL_DEDUP_RELATION if match is None else match.relation
            similarity = None if match is None else match.similarity
            matched_cache_id = None if match is None else match.analysis_cache_id
            algorithm_version = (
                OPPORTUNITY_DEDUP_ALGORITHM_VERSION
                if match is None
                else match.algorithm_version
            )
            dedup_evidence: Mapping[str, Any] = (
                {} if match is None else match.evidence
            )
        else:
            relation = str(existing_link["dedup_relation"])
            similarity = _float(existing_link["dedup_similarity"])
            matched_cache_id = existing_link["matched_analysis_cache_id"]
            algorithm_version = str(existing_link["dedup_algorithm_version"])
            dedup_evidence = dict(existing_link["dedup_evidence"])

        if opportunity_id is None:
            opportunity_id = uuid4()
            created = True
            await connection.execute(
                opportunities.insert().values(
                    id=opportunity_id,
                    first_seen_at=first_seen_at,
                    last_seen_at=last_seen_at,
                    **_analysis_values(analysis),
                )
            )

        if existing_link is None:
            await connection.execute(
                opportunity_analysis_links.insert().values(
                    analysis_cache_id=analysis_cache_id,
                    opportunity_id=opportunity_id,
                    dedup_relation=relation,
                    dedup_algorithm_version=algorithm_version,
                    normalized_text_sha256=normalized_text_hash,
                    dedup_similarity=(
                        None if similarity is None else Decimal(str(similarity))
                    ),
                    dedup_window_seconds=self._dedup_policy.window_seconds,
                    dedup_evidence=dict(dedup_evidence),
                    matched_analysis_cache_id=matched_cache_id,
                )
            )

        linked_message_ids: list[UUID] = []
        for raw_message_id in message_ids:
            inserted = await connection.scalar(
                pg_insert(opportunity_source_messages)
                .values(
                    raw_message_id=raw_message_id,
                    opportunity_id=opportunity_id,
                )
                .on_conflict_do_nothing(
                    index_elements=[opportunity_source_messages.c.raw_message_id]
                )
                .returning(opportunity_source_messages.c.raw_message_id)
            )
            if inserted is not None:
                linked_message_ids.append(raw_message_id)
                continue
            linked_opportunity_id = await connection.scalar(
                sa.select(opportunity_source_messages.c.opportunity_id).where(
                    opportunity_source_messages.c.raw_message_id == raw_message_id
                )
            )
            if linked_opportunity_id != opportunity_id:
                raise OpportunityLinkConflict(
                    "Raw message is already linked to another canonical opportunity"
                )

        if linked_message_ids:
            await connection.execute(
                opportunities.update()
                .where(opportunities.c.id == opportunity_id)
                .values(
                    first_seen_at=sa.func.least(
                        opportunities.c.first_seen_at,
                        first_seen_at,
                    ),
                    last_seen_at=sa.func.greatest(
                        opportunities.c.last_seen_at,
                        last_seen_at,
                    ),
                    updated_at=sa.func.now(),
                )
            )
        if created:
            initial_evidence = min(
                message_ids,
                key=lambda raw_id: (observation_times[raw_id], str(raw_id)),
            )
            await self._record_lifecycle_event(
                connection,
                opportunity_id=opportunity_id,
                from_status=None,
                to_status=OpportunityLifecycleStatus.ACTIVE,
                evidence_raw_message_id=initial_evidence,
                actor_kind="system",
                actor_id=None,
                reason="canonical opportunity created",
            )
        elif linked_message_ids:
            latest_evidence = max(
                linked_message_ids,
                key=lambda raw_id: (observation_times[raw_id], str(raw_id)),
            )
            await self._reactivate_stale_for_new_observation(
                connection,
                opportunity_id=opportunity_id,
                evidence_raw_message_id=latest_evidence,
            )
        await self._refresh_preferred_source(connection, opportunity_id)
        opportunity = await self.get(connection, opportunity_id)
        if opportunity is None:
            raise OpportunityPersistenceError("Canonical opportunity disappeared")
        return CanonicalOpportunityWriteOutcome(
            opportunity=opportunity,
            created=created,
            linked_message_count=len(linked_message_ids),
            dedup_relation=relation,
            dedup_similarity=similarity,
            dedup_evidence=dedup_evidence,
        )

    async def transition_lifecycle(
        self,
        connection: AsyncConnection,
        opportunity_id: UUID,
        target: OpportunityLifecycleStatus | str,
        *,
        reason: str,
        evidence_raw_message_id: UUID | None = None,
    ) -> OpportunityLifecycleWriteOutcome:
        return await self._transition_lifecycle(
            connection,
            opportunity_id=opportunity_id,
            target=_lifecycle_status(target),
            reason=_required_text(reason, "reason"),
            evidence_raw_message_id=evidence_raw_message_id,
            actor_kind="system",
            actor_id=None,
            allow_override=False,
        )

    async def override_lifecycle(
        self,
        connection: AsyncConnection,
        opportunity_id: UUID,
        target: OpportunityLifecycleStatus | str,
        *,
        operator_id: str,
        reason: str,
        evidence_raw_message_id: UUID | None = None,
    ) -> OpportunityLifecycleWriteOutcome:
        return await self._transition_lifecycle(
            connection,
            opportunity_id=opportunity_id,
            target=_lifecycle_status(target),
            reason=_required_text(reason, "reason"),
            evidence_raw_message_id=evidence_raw_message_id,
            actor_kind="operator",
            actor_id=_required_text(operator_id, "operator_id"),
            allow_override=True,
        )

    async def _transition_lifecycle(
        self,
        connection: AsyncConnection,
        *,
        opportunity_id: UUID,
        target: OpportunityLifecycleStatus,
        reason: str,
        evidence_raw_message_id: UUID | None,
        actor_kind: str,
        actor_id: str | None,
        allow_override: bool,
    ) -> OpportunityLifecycleWriteOutcome:
        current = await self._locked_lifecycle_status(connection, opportunity_id)
        if current == target:
            opportunity = await self.get(connection, opportunity_id)
            if opportunity is None:
                raise OpportunityNotFound(
                    f"Opportunity {opportunity_id} does not exist"
                )
            return OpportunityLifecycleWriteOutcome(
                opportunity=opportunity,
                changed=False,
                event=None,
            )
        if not allow_override and target not in _VALID_LIFECYCLE_TRANSITIONS[current]:
            raise InvalidOpportunityTransition(
                "Invalid opportunity lifecycle transition: "
                f"{current.value} -> {target.value}"
            )
        await self._require_linked_evidence(
            connection,
            opportunity_id=opportunity_id,
            raw_message_id=evidence_raw_message_id,
        )
        await connection.execute(
            opportunities.update()
            .where(opportunities.c.id == opportunity_id)
            .values(
                lifecycle_status=target.value,
                lifecycle_changed_at=sa.func.now(),
                updated_at=sa.func.now(),
            )
        )
        event = await self._record_lifecycle_event(
            connection,
            opportunity_id=opportunity_id,
            from_status=current,
            to_status=target,
            evidence_raw_message_id=evidence_raw_message_id,
            actor_kind=actor_kind,
            actor_id=actor_id,
            reason=reason,
        )
        opportunity = await self.get(connection, opportunity_id)
        if opportunity is None:
            raise OpportunityNotFound(f"Opportunity {opportunity_id} does not exist")
        return OpportunityLifecycleWriteOutcome(
            opportunity=opportunity,
            changed=True,
            event=event,
        )

    async def _reactivate_stale_for_new_observation(
        self,
        connection: AsyncConnection,
        *,
        opportunity_id: UUID,
        evidence_raw_message_id: UUID,
    ) -> None:
        current = await self._locked_lifecycle_status(connection, opportunity_id)
        if current is not OpportunityLifecycleStatus.STALE:
            return
        await connection.execute(
            opportunities.update()
            .where(opportunities.c.id == opportunity_id)
            .values(
                lifecycle_status=OpportunityLifecycleStatus.ACTIVE.value,
                lifecycle_changed_at=sa.func.now(),
                updated_at=sa.func.now(),
            )
        )
        await self._record_lifecycle_event(
            connection,
            opportunity_id=opportunity_id,
            from_status=OpportunityLifecycleStatus.STALE,
            to_status=OpportunityLifecycleStatus.ACTIVE,
            evidence_raw_message_id=evidence_raw_message_id,
            actor_kind="system",
            actor_id=None,
            reason="new source observation",
        )

    async def _locked_lifecycle_status(
        self,
        connection: AsyncConnection,
        opportunity_id: UUID,
    ) -> OpportunityLifecycleStatus:
        value = await connection.scalar(
            sa.select(opportunities.c.lifecycle_status)
            .where(opportunities.c.id == opportunity_id)
            .with_for_update()
        )
        if value is None:
            raise OpportunityNotFound(f"Opportunity {opportunity_id} does not exist")
        return OpportunityLifecycleStatus(str(value))

    async def _require_linked_evidence(
        self,
        connection: AsyncConnection,
        *,
        opportunity_id: UUID,
        raw_message_id: UUID | None,
    ) -> None:
        if raw_message_id is None:
            return
        linked_opportunity_id = await connection.scalar(
            sa.select(opportunity_source_messages.c.opportunity_id).where(
                opportunity_source_messages.c.raw_message_id == raw_message_id
            )
        )
        if linked_opportunity_id != opportunity_id:
            raise OpportunityLinkConflict(
                "Lifecycle evidence raw message is not linked to the opportunity"
            )

    async def _record_lifecycle_event(
        self,
        connection: AsyncConnection,
        *,
        opportunity_id: UUID,
        from_status: OpportunityLifecycleStatus | None,
        to_status: OpportunityLifecycleStatus,
        evidence_raw_message_id: UUID | None,
        actor_kind: str,
        actor_id: str | None,
        reason: str,
    ) -> OpportunityLifecycleEventRecord:
        row = (
            await connection.execute(
                opportunity_lifecycle_events.insert()
                .values(
                    opportunity_id=opportunity_id,
                    from_status=(
                        None if from_status is None else from_status.value
                    ),
                    to_status=to_status.value,
                    evidence_raw_message_id=evidence_raw_message_id,
                    actor_kind=actor_kind,
                    actor_id=actor_id,
                    reason=reason,
                )
                .returning(opportunity_lifecycle_events)
            )
        ).mappings().one()
        return _lifecycle_event_record(row)

    async def _find_duplicate(
        self,
        connection: AsyncConnection,
        *,
        analysis_cache_id: UUID,
        normalized_text: str,
        normalized_text_hash: str,
        first_seen_at: datetime,
        last_seen_at: datetime,
        analysis: OpportunityAnalysis,
    ) -> _DedupMatch | None:
        time_predicates = (
            opportunities.c.first_seen_at
            >= first_seen_at - self._dedup_policy.window,
            opportunities.c.first_seen_at <= last_seen_at + self._dedup_policy.window,
        )
        exact = (
            await connection.execute(
                sa.select(
                    opportunity_analysis_links.c.opportunity_id,
                    opportunity_analysis_links.c.analysis_cache_id,
                )
                .join(
                    opportunities,
                    opportunities.c.id == opportunity_analysis_links.c.opportunity_id,
                )
                .where(
                    opportunity_analysis_links.c.analysis_cache_id
                    != analysis_cache_id,
                    opportunity_analysis_links.c.normalized_text_sha256
                    == normalized_text_hash,
                    *time_predicates,
                )
                .order_by(
                    opportunities.c.first_seen_at,
                    opportunity_analysis_links.c.linked_at,
                    opportunity_analysis_links.c.analysis_cache_id,
                )
                .limit(1)
            )
        ).mappings().one_or_none()
        if exact is not None:
            return _DedupMatch(
                opportunity_id=exact["opportunity_id"],
                analysis_cache_id=exact["analysis_cache_id"],
                relation=EXACT_DEDUP_RELATION,
                similarity=1.0,
                algorithm_version=OPPORTUNITY_DEDUP_ALGORITHM_VERSION,
                evidence={"decision_rule": "normalized_text_hash"},
            )

        candidates = (
            await connection.execute(
                sa.select(
                    opportunities,
                    opportunity_analysis_links.c.analysis_cache_id.label(
                        "candidate_analysis_cache_id"
                    ),
                    opportunity_analysis_cache.c.normalized_content.label(
                        "candidate_normalized_content"
                    ),
                )
                .join(
                    opportunities,
                    opportunities.c.id == opportunity_analysis_links.c.opportunity_id,
                )
                .join(
                    opportunity_analysis_cache,
                    opportunity_analysis_cache.c.id
                    == opportunity_analysis_links.c.analysis_cache_id,
                )
                .where(
                    opportunity_analysis_links.c.analysis_cache_id
                    != analysis_cache_id,
                    *time_predicates,
                )
                .order_by(
                    opportunities.c.last_seen_at.desc(),
                    opportunity_analysis_links.c.linked_at.desc(),
                    opportunity_analysis_links.c.analysis_cache_id,
                )
                .limit(self._dedup_policy.candidate_limit)
            )
        ).mappings().all()
        near_matches: list[tuple[float, datetime, str, Mapping[str, Any]]] = []
        for candidate in candidates:
            similarity = _near_text_similarity(
                normalized_text,
                normalize_opportunity_dedup_text(
                    candidate["candidate_normalized_content"]
                ),
                minimum_tokens=self._dedup_policy.minimum_near_tokens,
            )
            if similarity >= self._dedup_policy.near_similarity_threshold:
                near_matches.append(
                    (
                        similarity,
                        candidate["first_seen_at"],
                        str(candidate["candidate_analysis_cache_id"]),
                        candidate,
                    )
                )
        if near_matches:
            similarity, _, _, best = min(
                near_matches,
                key=lambda item: (-item[0], item[1], item[2]),
            )
            return _DedupMatch(
                opportunity_id=best["id"],
                analysis_cache_id=best["candidate_analysis_cache_id"],
                relation=NEAR_DEDUP_RELATION,
                similarity=similarity,
                algorithm_version=OPPORTUNITY_DEDUP_ALGORITHM_VERSION,
                evidence={
                    "decision_rule": "near_normalized_text",
                    "text_similarity": round(similarity, 6),
                },
            )

        structured_matches = []
        for candidate in candidates:
            decision = evaluate_structured_duplicate(
                analysis,
                _analysis_from_row(candidate),
                policy=self._dedup_policy.structured,
            )
            if decision is not None:
                structured_matches.append(
                    (
                        decision.similarity,
                        candidate["first_seen_at"],
                        str(candidate["candidate_analysis_cache_id"]),
                        candidate,
                        decision,
                    )
                )
        if not structured_matches:
            return None
        similarity, _, _, best, decision = min(
            structured_matches,
            key=lambda item: (-item[0], item[1], item[2]),
        )
        return _DedupMatch(
            opportunity_id=best["id"],
            analysis_cache_id=best["candidate_analysis_cache_id"],
            relation=STRUCTURED_DEDUP_RELATION,
            similarity=similarity,
            algorithm_version=STRUCTURED_DEDUP_ALGORITHM_VERSION,
            evidence=decision.evidence,
        )

    async def _refresh_preferred_source(
        self,
        connection: AsyncConnection,
        opportunity_id: UUID,
    ) -> None:
        preferred_raw_message_id = await connection.scalar(
            sa.select(raw_messages.c.id)
            .join(
                opportunity_source_messages,
                opportunity_source_messages.c.raw_message_id == raw_messages.c.id,
            )
            .where(opportunity_source_messages.c.opportunity_id == opportunity_id)
            .order_by(
                raw_messages.c.message_date,
                raw_messages.c.observed_at,
                raw_messages.c.source_id,
                raw_messages.c.external_message_id,
                raw_messages.c.id,
            )
            .limit(1)
        )
        if preferred_raw_message_id is None:
            return
        current = (
            await connection.execute(
                sa.select(
                    opportunities.c.preferred_raw_message_id,
                    opportunities.c.preferred_source_policy_version,
                ).where(opportunities.c.id == opportunity_id)
            )
        ).one()
        if (
            current.preferred_raw_message_id == preferred_raw_message_id
            and current.preferred_source_policy_version
            == PREFERRED_SOURCE_POLICY_VERSION
        ):
            return
        await connection.execute(
            opportunities.update()
            .where(opportunities.c.id == opportunity_id)
            .values(
                preferred_raw_message_id=preferred_raw_message_id,
                preferred_source_policy_version=PREFERRED_SOURCE_POLICY_VERSION,
                updated_at=sa.func.now(),
            )
        )

    async def get(
        self,
        connection: AsyncConnection,
        opportunity_id: UUID,
    ) -> CanonicalOpportunityRecord | None:
        row = (
            (
                await connection.execute(
                    sa.select(opportunities).where(opportunities.c.id == opportunity_id)
                )
            )
            .mappings()
            .one_or_none()
        )
        if row is None:
            return None
        return await self._record_with_links(connection, row)

    async def get_for_raw_message(
        self,
        connection: AsyncConnection,
        raw_message_id: UUID,
    ) -> CanonicalOpportunityRecord | None:
        opportunity_id = await connection.scalar(
            sa.select(opportunity_source_messages.c.opportunity_id).where(
                opportunity_source_messages.c.raw_message_id == raw_message_id
            )
        )
        if opportunity_id is None:
            return None
        return await self.get(connection, opportunity_id)

    async def get_for_analysis_cache(
        self,
        connection: AsyncConnection,
        analysis_cache_id: UUID,
    ) -> CanonicalOpportunityRecord | None:
        opportunity_id = await connection.scalar(
            sa.select(opportunity_analysis_links.c.opportunity_id).where(
                opportunity_analysis_links.c.analysis_cache_id == analysis_cache_id
            )
        )
        if opportunity_id is None:
            return None
        return await self.get(connection, opportunity_id)

    async def list_observed_since(
        self,
        connection: AsyncConnection,
        observed_since: datetime,
        *,
        limit: int = 100,
    ) -> tuple[CanonicalOpportunityRecord, ...]:
        if observed_since.tzinfo is None or observed_since.utcoffset() is None:
            raise ValueError("observed_since must include a timezone")
        if not 1 <= limit <= 500:
            raise ValueError("limit must be between 1 and 500")
        rows = (
            (
                await connection.execute(
                    sa.select(opportunities)
                    .where(opportunities.c.last_seen_at >= observed_since)
                    .order_by(
                        opportunities.c.last_seen_at.desc(),
                        opportunities.c.id,
                    )
                    .limit(limit)
                )
            )
            .mappings()
            .all()
        )
        records = []
        for row in rows:
            records.append(await self._record_with_links(connection, row))
        return tuple(records)

    async def list_recent_for_matching(
        self,
        connection: AsyncConnection,
        *,
        as_of: datetime,
        maximum_age_seconds: int,
        limit: int = 500,
    ) -> tuple[CanonicalOpportunityRecord, ...]:
        if as_of.tzinfo is None or as_of.utcoffset() is None:
            raise ValueError("as_of must include a timezone")
        if maximum_age_seconds < 60:
            raise ValueError("maximum_age_seconds must be at least 60")
        if not 1 <= limit <= 500:
            raise ValueError("limit must be between 1 and 500")
        cutoff = as_of - timedelta(seconds=maximum_age_seconds)
        excluded_statuses = (
            OpportunityLifecycleStatus.CLOSED.value,
            OpportunityLifecycleStatus.RETRACTED.value,
            OpportunityLifecycleStatus.SUPPRESSED.value,
        )
        rows = (
            await connection.execute(
                sa.select(opportunities)
                .where(
                    opportunities.c.last_seen_at >= cutoff,
                    opportunities.c.last_seen_at <= as_of,
                    opportunities.c.lifecycle_status.not_in(excluded_statuses),
                )
                .order_by(
                    opportunities.c.last_seen_at.desc(),
                    opportunities.c.id,
                )
                .limit(limit)
            )
        ).mappings().all()
        records = []
        for row in rows:
            records.append(await self._record_with_links(connection, row))
        return tuple(records)

    async def list_recent(
        self,
        connection: AsyncConnection,
        *,
        limit: int = 100,
    ) -> tuple[CanonicalOpportunityRecord, ...]:
        if not 1 <= limit <= 500:
            raise ValueError("limit must be between 1 and 500")
        rows = (
            await connection.execute(
                sa.select(opportunities)
                .order_by(
                    opportunities.c.last_seen_at.desc(),
                    opportunities.c.id,
                )
                .limit(limit)
            )
        ).mappings().all()
        records = []
        for row in rows:
            records.append(await self._record_with_links(connection, row))
        return tuple(records)

    async def _record_with_links(
        self,
        connection: AsyncConnection,
        row: Mapping[str, Any],
    ) -> CanonicalOpportunityRecord:
        raw_ids = (
            (
                await connection.execute(
                    sa.select(opportunity_source_messages.c.raw_message_id)
                    .where(opportunity_source_messages.c.opportunity_id == row["id"])
                    .order_by(
                        opportunity_source_messages.c.linked_at,
                        opportunity_source_messages.c.raw_message_id,
                    )
                )
            )
            .scalars()
            .all()
        )
        link_rows = (
            (
                await connection.execute(
                    sa.select(opportunity_analysis_links)
                    .where(opportunity_analysis_links.c.opportunity_id == row["id"])
                    .order_by(
                        opportunity_analysis_links.c.linked_at,
                        opportunity_analysis_links.c.analysis_cache_id,
                    )
                )
            )
            .mappings()
            .all()
        )
        links = tuple(_analysis_link_record(link) for link in link_rows)
        observation_rows = (
            await connection.execute(
                sa.select(
                    raw_messages.c.id.label("raw_message_id"),
                    raw_messages.c.source_id,
                    raw_messages.c.platform,
                    raw_messages.c.external_source_id,
                    raw_messages.c.message_url,
                    raw_messages.c.message_date,
                    raw_messages.c.observed_at,
                    opportunity_source_messages.c.linked_at,
                    sources.c.display_name.label("source_display_name"),
                    sources.c.handle.label("source_handle"),
                    sources.c.canonical_url.label("source_canonical_url"),
                )
                .join(sources, sources.c.id == raw_messages.c.source_id)
                .join(
                    opportunity_source_messages,
                    opportunity_source_messages.c.raw_message_id == raw_messages.c.id,
                )
                .where(opportunity_source_messages.c.opportunity_id == row["id"])
                .order_by(
                    raw_messages.c.message_date,
                    raw_messages.c.observed_at,
                    raw_messages.c.source_id,
                    raw_messages.c.external_message_id,
                    raw_messages.c.id,
                )
            )
        ).mappings().all()
        source_observations = tuple(
            _source_observation_record(
                observation,
                preferred_raw_message_id=row["preferred_raw_message_id"],
            )
            for observation in observation_rows
        )
        preferred_source = next(
            (
                observation
                for observation in source_observations
                if observation.is_preferred
            ),
            None,
        )
        lifecycle_rows = (
            await connection.execute(
                sa.select(opportunity_lifecycle_events)
                .where(opportunity_lifecycle_events.c.opportunity_id == row["id"])
                .order_by(
                    opportunity_lifecycle_events.c.changed_at,
                    opportunity_lifecycle_events.c.id,
                )
            )
        ).mappings().all()
        lifecycle_events = tuple(
            _lifecycle_event_record(event) for event in lifecycle_rows
        )
        return _record(
            row,
            tuple(raw_ids),
            tuple(link.analysis_cache_id for link in links),
            links,
            preferred_source,
            source_observations,
            lifecycle_events,
        )


def _analysis_values(analysis: OpportunityAnalysis) -> dict[str, Any]:
    budget = analysis.budget
    work = analysis.work
    contact = analysis.contact
    quality = analysis.quality
    title_source = analysis.role_title or analysis.task_summary
    return {
        "schema_version": CANONICAL_OPPORTUNITY_SCHEMA_VERSION,
        "canonical_title": _canonical_title(title_source),
        "task_summary": analysis.task_summary,
        "market_direction": analysis.market_direction.value,
        "intent_stage": analysis.intent_stage.value,
        "opportunity_type": analysis.opportunity_type.value,
        "category": analysis.category,
        "role_title": analysis.role_title,
        "skills": list(analysis.skills),
        "budget_known": budget.known,
        "budget_min": _decimal(budget.min),
        "budget_max": _decimal(budget.max),
        "budget_currency": budget.currency,
        "budget_period": budget.period,
        "budget_explicit": budget.explicit,
        "work_remote": work.remote,
        "work_location": work.location,
        "work_full_time": work.full_time,
        "work_part_time": work.part_time,
        "language": analysis.language,
        "contact_telegram": contact.telegram,
        "contact_email": contact.email,
        "contact_url": contact.url,
        "analysis_confidence": _decimal(analysis.confidence),
        "quality_actionability": _decimal(quality.actionability),
        "quality_commercial_plausibility": _decimal(quality.commercial_plausibility),
        "quality_specificity": _decimal(quality.specificity),
        "quality_credibility": _decimal(quality.credibility),
        "red_flags": list(analysis.red_flags),
    }


def _record(
    row: Mapping[str, Any],
    raw_message_ids: tuple[UUID, ...],
    analysis_cache_ids: tuple[UUID, ...],
    analysis_links: tuple[OpportunityAnalysisLinkRecord, ...],
    preferred_source: OpportunitySourceObservationRecord | None,
    source_observations: tuple[OpportunitySourceObservationRecord, ...],
    lifecycle_events: tuple[OpportunityLifecycleEventRecord, ...],
) -> CanonicalOpportunityRecord:
    analysis = _analysis_from_row(row)
    return CanonicalOpportunityRecord(
        id=row["id"],
        schema_version=row["schema_version"],
        canonical_title=row["canonical_title"],
        task_summary=row["task_summary"],
        analysis=analysis,
        first_seen_at=row["first_seen_at"],
        last_seen_at=row["last_seen_at"],
        lifecycle_status=OpportunityLifecycleStatus(row["lifecycle_status"]),
        lifecycle_changed_at=row["lifecycle_changed_at"],
        raw_message_ids=raw_message_ids,
        analysis_cache_ids=analysis_cache_ids,
        analysis_links=analysis_links,
        preferred_source_policy_version=row["preferred_source_policy_version"],
        preferred_source=preferred_source,
        source_observations=source_observations,
        lifecycle_events=lifecycle_events,
        created_at=row["created_at"],
        updated_at=row["updated_at"],
    )


def _analysis_from_row(row: Mapping[str, Any]) -> OpportunityAnalysis:
    return OpportunityAnalysis.model_validate_json(
        json.dumps(
            {
                "schema_version": "opportunity_analysis.v1",
                "is_opportunity": True,
                "confidence": float(row["analysis_confidence"]),
                "market_direction": row["market_direction"],
                "intent_stage": row["intent_stage"],
                "opportunity_type": row["opportunity_type"],
                "category": row["category"],
                "role_title": row["role_title"],
                "skills": list(row["skills"]),
                "task_summary": row["task_summary"],
                "budget": {
                    "known": row["budget_known"],
                    "min": _float(row["budget_min"]),
                    "max": _float(row["budget_max"]),
                    "currency": row["budget_currency"],
                    "period": row["budget_period"],
                    "explicit": row["budget_explicit"],
                },
                "work": {
                    "remote": row["work_remote"],
                    "location": row["work_location"],
                    "full_time": row["work_full_time"],
                    "part_time": row["work_part_time"],
                },
                "language": row["language"],
                "contact": {
                    "telegram": row["contact_telegram"],
                    "email": row["contact_email"],
                    "url": row["contact_url"],
                },
                "quality": {
                    "actionability": float(row["quality_actionability"]),
                    "commercial_plausibility": float(
                        row["quality_commercial_plausibility"]
                    ),
                    "specificity": float(row["quality_specificity"]),
                    "credibility": float(row["quality_credibility"]),
                },
                "red_flags": list(row["red_flags"]),
            }
        ),
        strict=True,
    )


def _canonical_title(value: str | None) -> str | None:
    if value is None:
        return None
    return value[:240].rstrip()


def normalize_opportunity_dedup_text(value: str) -> str:
    normalized = unicodedata.normalize("NFKC", value).casefold()
    return " ".join(_DEDUP_TOKEN_PATTERN.findall(normalized))


def _near_text_similarity(left: str, right: str, *, minimum_tokens: int) -> float:
    left_tokens = left.split()
    right_tokens = right.split()
    if min(len(left_tokens), len(right_tokens)) < minimum_tokens:
        return 0.0
    if _numeric_tokens(left_tokens) != _numeric_tokens(right_tokens):
        return 0.0
    left_counts = Counter(left_tokens)
    right_counts = Counter(right_tokens)
    overlap = sum((left_counts & right_counts).values())
    multiset_similarity = overlap / max(len(left_tokens), len(right_tokens))
    sequence_similarity = SequenceMatcher(
        None,
        left_tokens,
        right_tokens,
        autojunk=False,
    ).ratio()
    return min(multiset_similarity, sequence_similarity)


def _numeric_tokens(tokens: Sequence[str]) -> Counter[str]:
    return Counter(
        token for token in tokens if any(character.isdigit() for character in token)
    )


def _analysis_link_record(row: Mapping[str, Any]) -> OpportunityAnalysisLinkRecord:
    return OpportunityAnalysisLinkRecord(
        analysis_cache_id=row["analysis_cache_id"],
        dedup_relation=str(row["dedup_relation"]),
        dedup_algorithm_version=str(row["dedup_algorithm_version"]),
        normalized_text_sha256=str(row["normalized_text_sha256"]),
        dedup_similarity=_float(row["dedup_similarity"]),
        dedup_window_seconds=int(row["dedup_window_seconds"]),
        dedup_evidence=dict(row["dedup_evidence"]),
        matched_analysis_cache_id=row["matched_analysis_cache_id"],
        linked_at=row["linked_at"],
    )


def _source_observation_record(
    row: Mapping[str, Any],
    *,
    preferred_raw_message_id: UUID | None,
) -> OpportunitySourceObservationRecord:
    return OpportunitySourceObservationRecord(
        raw_message_id=row["raw_message_id"],
        source_id=int(row["source_id"]),
        platform=str(row["platform"]),
        external_source_id=str(row["external_source_id"]),
        source_display_name=str(row["source_display_name"]),
        source_handle=row["source_handle"],
        source_canonical_url=row["source_canonical_url"],
        message_url=str(row["message_url"]),
        message_date=row["message_date"],
        observed_at=row["observed_at"],
        linked_at=row["linked_at"],
        is_preferred=row["raw_message_id"] == preferred_raw_message_id,
    )


def _lifecycle_event_record(
    row: Mapping[str, Any],
) -> OpportunityLifecycleEventRecord:
    from_status = row["from_status"]
    return OpportunityLifecycleEventRecord(
        id=int(row["id"]),
        opportunity_id=row["opportunity_id"],
        from_status=(
            None if from_status is None else OpportunityLifecycleStatus(from_status)
        ),
        to_status=OpportunityLifecycleStatus(row["to_status"]),
        evidence_raw_message_id=row["evidence_raw_message_id"],
        actor_kind=str(row["actor_kind"]),
        actor_id=row["actor_id"],
        reason=str(row["reason"]),
        changed_at=row["changed_at"],
    )


def _lifecycle_status(
    value: OpportunityLifecycleStatus | str,
) -> OpportunityLifecycleStatus:
    try:
        return OpportunityLifecycleStatus(value)
    except ValueError as error:
        allowed = ", ".join(status.value for status in OpportunityLifecycleStatus)
        raise ValueError(f"Opportunity lifecycle status must be one of: {allowed}") from error


def _required_text(value: str, field: str) -> str:
    normalized = value.strip()
    if not normalized:
        raise ValueError(f"{field} must not be empty")
    return normalized


def _sha256(value: str) -> str:
    return sha256(value.encode("utf-8")).hexdigest()


def _decimal(value: float | None) -> Decimal | None:
    return None if value is None else Decimal(str(value))


def _float(value: Decimal | None) -> float | None:
    return None if value is None else float(value)
