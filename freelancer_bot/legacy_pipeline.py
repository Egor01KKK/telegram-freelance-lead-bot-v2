from __future__ import annotations

import logging

from .filters import FilterConfig, match_text
from .formatting import format_lead
from .ports import (
    CollectedMessage,
    LegacyLeadDelivery,
    LegacyLeadRepository,
    LegacySubscriptionRepository,
)
from .storage import LeadRecord


LOGGER = logging.getLogger("freelancer_bot")


class LegacyLeadProcessor:
    """Preserve the V1 keyword, SQLite and Telegram-delivery sequence during G0."""

    def __init__(
        self,
        filter_config: FilterConfig,
        lead_repository: LegacyLeadRepository,
        subscription_repository: LegacySubscriptionRepository,
        delivery: LegacyLeadDelivery,
    ):
        self.filter_config = filter_config
        self.lead_repository = lead_repository
        self.subscription_repository = subscription_repository
        self.delivery = delivery

    async def handle(self, message: CollectedMessage) -> None:
        text = message.text
        if not text.strip():
            return

        match = match_text(text, self.filter_config)
        if not match.accepted:
            return

        link = f"https://t.me/{message.source.username}/{message.message_id}"
        lead = LeadRecord(
            source=message.source.handle,
            message_id=message.message_id,
            link=link,
            text=text,
            score=match.score,
            keywords=match.matched_keywords,
            message_date=message.message_date.isoformat(),
        )

        lead_id = self.lead_repository.record_or_should_retry(lead)
        if lead_id is None:
            return

        subscribers = self.subscription_repository.subscribers()
        if not subscribers:
            LOGGER.warning("Lead matched, but no subscribers are configured yet: %s", link)
            return

        body = format_lead(message.source, lead, lead_id=lead_id)
        delivered = False
        for chat_id in subscribers:
            telegram_message_id = await self.delivery.deliver_lead(chat_id, body, lead_id)
            if telegram_message_id is None:
                continue
            delivered = True
            self.lead_repository.mark_notification_message(
                lead_id,
                chat_id,
                telegram_message_id,
            )

        if delivered:
            LOGGER.info(
                "Delivered lead from %s message %s",
                message.source.handle,
                message.message_id,
            )
