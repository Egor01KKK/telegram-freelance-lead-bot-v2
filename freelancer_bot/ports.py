from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Protocol, TYPE_CHECKING

if TYPE_CHECKING:
    from .replies import ReplyDraft
    from .sources import Source
    from .storage import LeadRecord, StoredLead


@dataclass(frozen=True)
class CollectedMessage:
    """Small transport-neutral input used only by the V1 compatibility flow."""

    source: Source
    message_id: int
    text: str
    message_date: datetime


class LegacyCollectedMessageHandler(Protocol):
    async def handle(self, message: CollectedMessage) -> None: ...


class LegacyLeadRepository(Protocol):
    def record_or_should_retry(self, lead: LeadRecord) -> int | None: ...

    def mark_notification_message(
        self,
        lead_id: int,
        chat_id: int,
        telegram_message_id: int,
    ) -> None: ...


class LegacySubscriptionRepository(Protocol):
    def subscribers(self) -> list[int]: ...


class LegacyLeadDelivery(Protocol):
    async def deliver_lead(self, chat_id: int, body: str, lead_id: int) -> int | None: ...


class ReplyDraftProvider(Protocol):
    """Generate a manual V1 reply draft, not canonical opportunity analysis or matching."""

    def generate(self, lead: StoredLead, freelancer_profile: dict[str, Any]) -> ReplyDraft: ...
