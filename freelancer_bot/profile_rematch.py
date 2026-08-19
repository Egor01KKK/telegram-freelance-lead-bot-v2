from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import logging
import re
from uuid import UUID

from .config import RuntimeConfig
from .delivery import DeliveryScheduleReport, PersonalizedDeliveryService
from .match_decisions import match_decision_policy_from_config
from .matching_service import (
    CandidateMatchingService,
    StaleSearchProfileRevision,
)
from .observability import log_event
from .persistence.database import Database
from .persistence.jobs import DurableJobRepository, JobClaim
from .persistence.opportunities import CanonicalOpportunityRepository
from .persistence.search_profiles import (
    SearchProfileConfirmationStatus,
    SearchProfileNotFound,
    SearchProfileRepository,
)


PROFILE_REMATCH_JOB_TYPE = "profile.rematch_recent.v1"
PROFILE_REMATCH_MAX_ATTEMPTS = 5
PROFILE_REMATCH_MAX_OPPORTUNITIES = 500
_PROFILE_REMATCH_KEY_PATTERN = re.compile(
    r"^profile:([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}):revision:([1-9][0-9]*)$"
)


@dataclass(frozen=True)
class ProfileRematchOutcome:
    profile_id: UUID
    profile_revision: int
    opportunity_count: int
    match_run_id: UUID | None
    delivery: DeliveryScheduleReport | None
    skipped: bool


def profile_rematch_job_key(profile_id: UUID, profile_revision: int) -> str:
    if profile_revision < 1:
        raise ValueError("profile_revision must be positive")
    return f"profile:{profile_id}:revision:{profile_revision}"


def parse_profile_rematch_job_key(value: str) -> tuple[UUID, int]:
    match = _PROFILE_REMATCH_KEY_PATTERN.fullmatch(value)
    if match is None:
        raise ValueError("invalid profile rematch job identity")
    try:
        profile_id = UUID(match.group(1))
    except ValueError:
        raise ValueError("invalid profile rematch profile id") from None
    return profile_id, int(match.group(2))


class ProfileRematchJobProcessor:
    """Rematch one activated profile against a bounded recent Opportunity set."""

    def __init__(
        self,
        database: Database,
        config: RuntimeConfig,
        *,
        matching: CandidateMatchingService | None = None,
        deliveries: PersonalizedDeliveryService | None = None,
        jobs: DurableJobRepository | None = None,
        logger: logging.Logger | None = None,
        max_opportunities: int = PROFILE_REMATCH_MAX_OPPORTUNITIES,
    ) -> None:
        if not 1 <= max_opportunities <= PROFILE_REMATCH_MAX_OPPORTUNITIES:
            raise ValueError(
                "max_opportunities must be between 1 and "
                f"{PROFILE_REMATCH_MAX_OPPORTUNITIES}"
            )
        self._database = database
        self._config = config
        self._matching = matching or CandidateMatchingService(database, logger=logger)
        self._deliveries = deliveries or PersonalizedDeliveryService(
            database,
            logger=logger,
        )
        self._jobs = jobs or DurableJobRepository()
        self._logger = logger or logging.getLogger(__name__)
        self._max_opportunities = max_opportunities

    async def __call__(self, claim: JobClaim) -> ProfileRematchOutcome:
        return await self.process(claim)

    async def process(self, claim: JobClaim) -> ProfileRematchOutcome:
        if claim.job_type != PROFILE_REMATCH_JOB_TYPE:
            raise ValueError("profile rematch processor received an unsupported job type")
        profile_id, profile_revision = parse_profile_rematch_job_key(
            claim.idempotency_key
        )

        async with self._database.connect() as connection:
            job = await self._jobs.get(connection, claim.id)
            if job is None:
                raise LookupError(f"profile rematch job {claim.id} does not exist")
            try:
                profile = await SearchProfileRepository().get(connection, profile_id)
            except SearchProfileNotFound:
                return self._skipped(profile_id, profile_revision, "profile_missing")
            if not _is_current_profile(profile, profile_revision):
                return self._skipped(profile_id, profile_revision, "profile_revision_stale")
            evaluated_at = _aware(job["created_at"])
            opportunities = await CanonicalOpportunityRepository().list_recent_for_matching(
                connection,
                as_of=evaluated_at,
                maximum_age_seconds=self._config.matching_maximum_age_seconds,
                limit=self._max_opportunities,
            )

        opportunity_ids = tuple(opportunity.id for opportunity in opportunities)
        if not opportunity_ids:
            log_event(
                self._logger,
                logging.INFO,
                "profile.rematch_recent.completed",
                profile_id=profile_id,
                profile_revision=profile_revision,
                opportunity_count=0,
                match_run_id=None,
                delivery_created_count=0,
            )
            return ProfileRematchOutcome(
                profile_id=profile_id,
                profile_revision=profile_revision,
                opportunity_count=0,
                match_run_id=None,
                delivery=None,
                skipped=False,
            )

        try:
            generated = await self._matching.generate_matches(
                opportunity_ids,
                evaluated_at=evaluated_at,
                decision_policy=match_decision_policy_from_config(self._config),
                profile_id=profile_id,
                profile_revision=profile_revision,
            )
        except StaleSearchProfileRevision:
            return self._skipped(profile_id, profile_revision, "profile_revision_stale")

        delivery = await self._deliveries.schedule_run(
            generated.persistence.run.id,
            rendered_at=datetime.now(timezone.utc),
        )
        log_event(
            self._logger,
            logging.INFO,
            "profile.rematch_recent.completed",
            profile_id=profile_id,
            profile_revision=profile_revision,
            opportunity_count=len(opportunity_ids),
            match_run_id=generated.persistence.run.id,
            match_run_created=generated.persistence.created,
            trace_count=len(generated.persistence.traces),
            eligible_match_count=generated.report.eligible_match_count,
            delivery_created_count=delivery.created_count,
            delivery_reused_count=delivery.reused_count,
            delivery_failure_count=len(delivery.failures),
            user_specific_llm_calls=generated.report.user_specific_llm_calls,
            opportunity_analyzer_calls=generated.report.opportunity_analyzer_calls,
        )
        return ProfileRematchOutcome(
            profile_id=profile_id,
            profile_revision=profile_revision,
            opportunity_count=len(opportunity_ids),
            match_run_id=generated.persistence.run.id,
            delivery=delivery,
            skipped=False,
        )

    def _skipped(
        self,
        profile_id: UUID,
        profile_revision: int,
        reason: str,
    ) -> ProfileRematchOutcome:
        log_event(
            self._logger,
            logging.INFO,
            "profile.rematch_recent.skipped",
            profile_id=profile_id,
            profile_revision=profile_revision,
            reason=reason,
        )
        return ProfileRematchOutcome(
            profile_id=profile_id,
            profile_revision=profile_revision,
            opportunity_count=0,
            match_run_id=None,
            delivery=None,
            skipped=True,
        )


def _is_current_profile(profile, expected_revision: int) -> bool:
    return (
        profile.is_active
        and profile.is_primary
        and profile.confirmation_status is SearchProfileConfirmationStatus.CONFIRMED
        and profile.revision == expected_revision
    )


def _aware(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("profile rematch job created_at must include a timezone")
    return value
