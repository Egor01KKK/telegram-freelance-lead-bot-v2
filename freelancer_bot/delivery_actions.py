from __future__ import annotations

from dataclasses import dataclass
import logging
import re
from uuid import UUID

from .metrics import MetricNames, MetricsSink, NoOpMetrics
from .observability import log_event
from .persistence.database import Database
from .persistence.delivery_actions import (
    DeliveryActionRepository,
    DeliveryActionType,
    DeliveryActionWriteOutcome,
)
from .persistence.feedback import FeedbackRepository


DELIVERY_ACTION_CALLBACK_VERSION = "lda1"
DELIVERY_ACTION_CALLBACK_PATTERN = rb"^lda1:([ong]):([0-9a-f]{32})$"
_CALLBACK = re.compile(DELIVERY_ACTION_CALLBACK_PATTERN)
_ACTION_CODES = {
    DeliveryActionType.OPEN: "o",
    DeliveryActionType.NOT_SUITABLE: "n",
    DeliveryActionType.GOT_JOB: "g",
}
_CODE_ACTIONS = {code: action for action, code in _ACTION_CODES.items()}


@dataclass(frozen=True)
class TelegramDeliveryActionButton:
    label: str
    data: bytes


class DeliveryActionService:
    def __init__(
        self,
        database: Database,
        *,
        repository: DeliveryActionRepository | None = None,
        feedback: FeedbackRepository | None = None,
        metrics: MetricsSink | None = None,
        logger: logging.Logger | None = None,
    ) -> None:
        self._database = database
        self._repository = repository or DeliveryActionRepository()
        self._feedback = feedback or FeedbackRepository()
        self._metrics = metrics or NoOpMetrics()
        self._logger = logger or logging.getLogger(__name__)

    async def record(
        self,
        *,
        delivery_id: UUID,
        action_type: DeliveryActionType,
        actor_external_user_id: str,
    ) -> DeliveryActionWriteOutcome:
        async with self._database.transaction() as connection:
            outcome = await self._repository.record(
                connection,
                delivery_id=delivery_id,
                action_type=action_type,
                actor_external_user_id=actor_external_user_id,
            )
            feedback = await self._feedback.record_for_action_event(
                connection,
                delivery_action_event_id=outcome.event.id,
            )
        self._metrics.increment(
            (
                MetricNames.DELIVERY_ACTIONS
                if outcome.created
                else MetricNames.DELIVERY_ACTIONS_REUSED
            ),
            tags={"action_type": action_type.value},
        )
        if feedback is not None and feedback.created:
            self._metrics.increment(
                MetricNames.FEEDBACK,
                tags={"action_type": action_type.value},
            )
        event = outcome.event
        log_event(
            self._logger,
            logging.INFO,
            "delivery.action_recorded" if outcome.created else "delivery.action_reused",
            delivery_action_id=event.id,
            action_type=event.action_type.value,
            delivery_id=event.delivery_id,
            match_trace_id=event.match_trace_id,
            opportunity_id=event.opportunity_id,
            search_profile_id=event.search_profile_id,
            profile_revision=event.profile_revision,
            user_id=event.user_id,
            source_id=event.source_id,
        )
        return outcome


def delivery_action_buttons(
    delivery_id: UUID,
    *,
    source_available: bool,
) -> tuple[tuple[TelegramDeliveryActionButton, ...], ...]:
    rows: list[tuple[TelegramDeliveryActionButton, ...]] = []
    if source_available:
        rows.append(
            (
                TelegramDeliveryActionButton(
                    "Открыть",
                    encode_delivery_action_callback(
                        delivery_id,
                        DeliveryActionType.OPEN,
                    ),
                ),
            )
        )
    rows.append(
        (
            TelegramDeliveryActionButton(
                "Не подходит",
                encode_delivery_action_callback(
                    delivery_id,
                    DeliveryActionType.NOT_SUITABLE,
                ),
            ),
            TelegramDeliveryActionButton(
                "Получил заказ",
                encode_delivery_action_callback(
                    delivery_id,
                    DeliveryActionType.GOT_JOB,
                ),
            ),
        )
    )
    return tuple(rows)


def encode_delivery_action_callback(
    delivery_id: UUID,
    action_type: DeliveryActionType,
) -> bytes:
    data = (
        f"{DELIVERY_ACTION_CALLBACK_VERSION}:"
        f"{_ACTION_CODES[action_type]}:{delivery_id.hex}"
    ).encode("ascii")
    if len(data) > 64:
        raise ValueError("Telegram callback payload exceeds 64 bytes")
    return data


def decode_delivery_action_callback(data: bytes) -> tuple[DeliveryActionType, UUID]:
    if not isinstance(data, bytes):
        raise ValueError("invalid delivery action callback")
    matched = _CALLBACK.fullmatch(data)
    if matched is None:
        raise ValueError("invalid delivery action callback")
    code = matched.group(1).decode("ascii")
    return _CODE_ACTIONS[code], UUID(hex=matched.group(2).decode("ascii"))
