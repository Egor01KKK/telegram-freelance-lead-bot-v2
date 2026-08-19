from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
import logging
from time import monotonic
from typing import Protocol
from uuid import UUID

from .billing import EntitlementChecker
from .delivery_actions import TelegramDeliveryActionButton, delivery_action_buttons
from .lead_renderer import render_telegram_lead_card
from .metrics import MetricNames, MetricsSink, NoOpMetrics
from .observability import log_event
from .persistence.database import Database
from .persistence.deliveries import (
    PERSONALIZED_DELIVERY_JOB_TYPE,
    DeliveryStatus,
    PersonalizedDeliveryRecord,
    PersonalizedDeliveryRepository,
    PersonalizedDeliveryWriteOutcome,
)
from .persistence.entitlements import TrialEntitlementChecker
from .persistence.jobs import DurableJobRepository, JobClaim
from .persistence.matches import MatchTraceRepository
from .persistence.opportunities import (
    CanonicalOpportunityRepository,
    OpportunityNotFound,
)
from .persistence.search_profiles import SearchProfileRepository, UserRepository


@dataclass(frozen=True)
class TelegramSendReceipt:
    message_id: int

    def __post_init__(self) -> None:
        if self.message_id <= 0:
            raise ValueError("Telegram message id must be positive")


class TelegramDeliverySender(Protocol):
    async def send(
        self,
        *,
        recipient_chat_id: int,
        body_html: str,
        parse_mode: str,
        link_preview: bool,
        idempotency_key: str,
        buttons: tuple[tuple[TelegramDeliveryActionButton, ...], ...],
    ) -> TelegramSendReceipt: ...


class DeliverySchedulingError(RuntimeError):
    pass


class DeliverySendError(RuntimeError):
    pass


@dataclass(frozen=True)
class DeliveryScheduleFailure:
    match_trace_id: UUID
    failure_code: str


@dataclass(frozen=True)
class DeliveryScheduleReport:
    run_id: UUID
    deliveries: tuple[PersonalizedDeliveryWriteOutcome, ...]
    failures: tuple[DeliveryScheduleFailure, ...]

    @property
    def created_count(self) -> int:
        return sum(item.created for item in self.deliveries)

    @property
    def reused_count(self) -> int:
        return len(self.deliveries) - self.created_count


class PersonalizedDeliveryService:
    def __init__(
        self,
        database: Database,
        *,
        matches: MatchTraceRepository | None = None,
        profiles: SearchProfileRepository | None = None,
        users: UserRepository | None = None,
        opportunities: CanonicalOpportunityRepository | None = None,
        deliveries: PersonalizedDeliveryRepository | None = None,
        jobs: DurableJobRepository | None = None,
        entitlement_checker: EntitlementChecker | None = None,
        metrics: MetricsSink | None = None,
        logger: logging.Logger | None = None,
    ) -> None:
        self._database = database
        self._matches = matches or MatchTraceRepository()
        self._profiles = profiles or SearchProfileRepository()
        self._users = users or UserRepository()
        self._opportunities = opportunities or CanonicalOpportunityRepository()
        self._entitlements = entitlement_checker or TrialEntitlementChecker()
        self._deliveries = deliveries or PersonalizedDeliveryRepository(
            entitlement_checker=self._entitlements,
        )
        self._jobs = jobs or DurableJobRepository()
        self._metrics = metrics or NoOpMetrics()
        self._logger = logger or logging.getLogger(__name__)

    async def schedule_run(
        self,
        run_id: UUID,
        *,
        rendered_at: datetime,
    ) -> DeliveryScheduleReport:
        async with self._database.connect() as connection:
            matches = await self._matches.list_eligible_for_run(connection, run_id)
        deliveries: list[PersonalizedDeliveryWriteOutcome] = []
        failures: list[DeliveryScheduleFailure] = []
        for match in matches:
            try:
                outcome = await self.schedule_trace(
                    match.id,
                    rendered_at=rendered_at,
                )
            except Exception as error:
                code = type(error).__name__
                failures.append(DeliveryScheduleFailure(match.id, code))
                self._metrics.increment(
                    MetricNames.DELIVERY_SCHEDULE_FAILURES,
                    tags={"failure_code": code},
                )
                log_event(
                    self._logger,
                    logging.WARNING,
                    "delivery.schedule_failed",
                    match_trace_id=match.id,
                    match_run_id=run_id,
                    failure_code=code,
                )
                continue
            deliveries.append(outcome)
        return DeliveryScheduleReport(
            run_id=run_id,
            deliveries=tuple(deliveries),
            failures=tuple(failures),
        )

    async def schedule_trace(
        self,
        match_trace_id: UUID,
        *,
        rendered_at: datetime,
    ) -> PersonalizedDeliveryWriteOutcome:
        async with self._database.transaction() as connection:
            match = await self._matches.get(connection, match_trace_id)
            if match is None:
                raise DeliverySchedulingError("match trace does not exist")
            profile = await self._profiles.get(
                connection,
                match.trace.search_profile_id,
            )
            user = await self._users.get(connection, profile.user_id)
            opportunity = await self._opportunities.get(
                connection,
                match.trace.opportunity_id,
            )
            if opportunity is None:
                raise OpportunityNotFound(
                    f"Opportunity {match.trace.opportunity_id} does not exist"
                )
            if not profile.is_active or profile.revision != match.trace.profile_revision:
                raise DeliverySchedulingError(
                    "match trace does not describe the active SearchProfile"
                )
            entitlement = await self._entitlements.check(connection, user.id)
            if not entitlement.can_receive_deliveries:
                raise DeliverySchedulingError(entitlement.failure_code or "EntitlementRequired")
            card = render_telegram_lead_card(
                opportunity,
                match,
                rendered_at=rendered_at,
            )
            outcome = await self._deliveries.ensure(
                connection,
                jobs=self._jobs,
                match=match,
                user=user,
                card=card,
                rendered_at=rendered_at,
            )
        metric = (
            MetricNames.DELIVERIES_SCHEDULED
            if outcome.created
            else MetricNames.DELIVERIES_REUSED
        )
        self._metrics.increment(metric)
        log_event(
            self._logger,
            logging.INFO,
            "delivery.scheduled" if outcome.created else "delivery.reused",
            delivery_id=outcome.delivery.id,
            match_trace_id=outcome.delivery.match_trace_id,
            match_run_id=outcome.delivery.match_run_id,
            opportunity_id=outcome.delivery.opportunity_id,
            search_profile_id=outcome.delivery.search_profile_id,
            user_id=outcome.delivery.user_id,
            job_id=outcome.delivery.job_id,
            renderer_schema_version=outcome.delivery.renderer_schema_version,
        )
        return outcome


class PersonalizedDeliveryJobProcessor:
    def __init__(
        self,
        database: Database,
        sender: TelegramDeliverySender,
        *,
        deliveries: PersonalizedDeliveryRepository | None = None,
        entitlement_checker: EntitlementChecker | None = None,
        metrics: MetricsSink | None = None,
        logger: logging.Logger | None = None,
    ) -> None:
        self._database = database
        self._sender = sender
        self._entitlements = entitlement_checker or TrialEntitlementChecker()
        self._deliveries = deliveries or PersonalizedDeliveryRepository(
            entitlement_checker=self._entitlements,
        )
        self._metrics = metrics or NoOpMetrics()
        self._logger = logger or logging.getLogger(__name__)

    async def __call__(self, claim: JobClaim) -> None:
        await self.process(claim)

    async def process(self, claim: JobClaim) -> PersonalizedDeliveryRecord | None:
        if claim.job_type != PERSONALIZED_DELIVERY_JOB_TYPE:
            raise ValueError("processor received an unsupported job type")
        async with self._database.transaction() as connection:
            attempt = await self._deliveries.prepare_attempt(connection, claim)
            if attempt is None:
                current = await self._deliveries.get_by_job(connection, claim.id)
        if attempt is None:
            if current is not None and current.status is DeliveryStatus.SUPPRESSED:
                self._metrics.increment(
                    MetricNames.DELIVERIES_SUPPRESSED,
                    tags={"reason": current.failure_code or "Unknown"},
                )
                log_event(
                    self._logger,
                    logging.INFO,
                    "delivery.suppressed",
                    delivery_id=current.id,
                    match_trace_id=current.match_trace_id,
                    opportunity_id=current.opportunity_id,
                    search_profile_id=current.search_profile_id,
                    user_id=current.user_id,
                    attempt=claim.attempt_count,
                    reason=current.failure_code,
                )
            return current

        delivery = attempt.delivery
        started = monotonic()
        try:
            receipt = await self._sender.send(
                recipient_chat_id=attempt.recipient_chat_id,
                body_html=delivery.card_body_html,
                parse_mode=delivery.parse_mode,
                link_preview=delivery.link_preview,
                idempotency_key=delivery.idempotency_key,
                buttons=delivery_action_buttons(
                    delivery.id,
                    source_available=delivery.source_url is not None,
                ),
            )
        except Exception as error:
            failure_code = type(error).__name__
            async with self._database.transaction() as connection:
                failed = await self._deliveries.mark_attempt_failed(
                    connection,
                    claim,
                    failure_code=failure_code,
                )
            metric = (
                MetricNames.DELIVERIES_FAILED
                if failed.status is DeliveryStatus.FAILED
                else MetricNames.DELIVERIES_RETRIED
            )
            self._metrics.increment(metric, tags={"failure_code": failure_code})
            log_event(
                self._logger,
                logging.WARNING,
                "delivery.failed"
                if failed.status is DeliveryStatus.FAILED
                else "delivery.retry_scheduled",
                delivery_id=delivery.id,
                match_trace_id=delivery.match_trace_id,
                opportunity_id=delivery.opportunity_id,
                search_profile_id=delivery.search_profile_id,
                user_id=delivery.user_id,
                attempt=claim.attempt_count,
                max_attempts=claim.max_attempts,
                failure_code=failure_code,
            )
            raise DeliverySendError(failure_code) from None

        async with self._database.transaction() as connection:
            sent = await self._deliveries.mark_sent(
                connection,
                claim,
                telegram_message_id=receipt.message_id,
            )
        self._metrics.increment(MetricNames.DELIVERIES)
        self._metrics.increment(MetricNames.DELIVERIES_SENT)
        self._metrics.observe(
            MetricNames.DELIVERY_SEND_SECONDS,
            monotonic() - started,
        )
        log_event(
            self._logger,
            logging.INFO,
            "delivery.sent",
            delivery_id=sent.id,
            match_trace_id=sent.match_trace_id,
            opportunity_id=sent.opportunity_id,
            search_profile_id=sent.search_profile_id,
            user_id=sent.user_id,
            telegram_message_id=sent.telegram_message_id,
            attempt=claim.attempt_count,
        )
        return sent
