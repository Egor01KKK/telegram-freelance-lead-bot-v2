from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
import html
import re
from urllib.parse import urlsplit
from uuid import UUID

from .match_decisions import MatchDecisionCode
from .opportunity_analysis import OpportunityType
from .persistence.matches import MatchTraceRecord
from .persistence.opportunities import (
    CanonicalOpportunityRecord,
    OpportunityLifecycleStatus,
)


TELEGRAM_LEAD_CARD_SCHEMA_VERSION = "telegram-lead-card.v1"
TELEGRAM_MESSAGE_LIMIT = 4096
_TITLE_LIMIT = 240
_SUMMARY_LIMIT = 700
_SKILL_LIMIT = 5

_OPPORTUNITY_TYPE_LABELS = {
    OpportunityType.ONE_OFF_ORDER: "заказ",
    OpportunityType.PROJECT: "проект",
    OpportunityType.VACANCY: "вакансия",
    OpportunityType.PART_TIME_CONTRACTOR: "частичная занятость",
    OpportunityType.CONSULTATION: "консультация",
}
_DEFAULT_TITLES = {
    OpportunityType.ONE_OFF_ORDER: "Новый заказ",
    OpportunityType.PROJECT: "Новый проект",
    OpportunityType.VACANCY: "Новая вакансия",
    OpportunityType.PART_TIME_CONTRACTOR: "Новая подработка",
    OpportunityType.CONSULTATION: "Запрос на консультацию",
    OpportunityType.UNKNOWN: "Новая возможность",
}
_CURRENCY_LABELS = {
    "RUB": "₽",
    "USD": "$",
    "EUR": "€",
    "GBP": "£",
}
_BUDGET_PERIOD_LABELS = {
    "hour": "/час",
    "day": "/день",
    "week": "/нед.",
    "month": "/мес.",
    "year": "/год",
}


class LeadCardRenderError(ValueError):
    pass


@dataclass(frozen=True)
class TelegramLeadCard:
    schema_version: str
    opportunity_id: UUID
    match_trace_id: UUID
    search_profile_id: UUID
    profile_revision: int
    body_html: str
    source_url: str | None
    parse_mode: str = "html"
    link_preview: bool = False

    def __post_init__(self) -> None:
        if self.schema_version != TELEGRAM_LEAD_CARD_SCHEMA_VERSION:
            raise ValueError("unsupported Telegram lead-card schema")
        if not self.body_html.strip():
            raise ValueError("Telegram lead-card body must not be empty")
        if len(self.body_html) > TELEGRAM_MESSAGE_LIMIT:
            raise ValueError("Telegram lead-card body exceeds the message limit")
        if self.parse_mode != "html":
            raise ValueError("Telegram lead cards require HTML parse mode")


def render_telegram_lead_card(
    opportunity: CanonicalOpportunityRecord,
    match: MatchTraceRecord,
    *,
    rendered_at: datetime,
) -> TelegramLeadCard:
    rendered_at = _aware_utc(rendered_at, "rendered_at")
    trace = match.trace
    _validate_render_input(opportunity, match)

    title = _lead_title(opportunity)
    metadata = _lead_metadata(opportunity)
    summary = _clean_text(
        opportunity.task_summary or opportunity.analysis.task_summary,
        limit=_SUMMARY_LIMIT,
    )
    skills = _skills_line(opportunity.analysis.skills)
    source_url = _safe_source_url(opportunity)
    footer = _footer(
        source_url=source_url,
        last_seen_at=opportunity.last_seen_at,
        rendered_at=rendered_at,
    )

    sections = [f"<b>{html.escape(title)}</b>"]
    if metadata:
        sections.append(" · ".join(html.escape(item) for item in metadata))
    description = tuple(item for item in (summary, skills) if item)
    if description:
        sections.append("\n".join(html.escape(item) for item in description))
    sections.append(footer)
    body_html = "\n\n".join(sections)
    if len(body_html) > TELEGRAM_MESSAGE_LIMIT:
        raise LeadCardRenderError("rendered Telegram lead card is too long")

    return TelegramLeadCard(
        schema_version=TELEGRAM_LEAD_CARD_SCHEMA_VERSION,
        opportunity_id=opportunity.id,
        match_trace_id=match.id,
        search_profile_id=trace.search_profile_id,
        profile_revision=trace.profile_revision,
        body_html=body_html,
        source_url=source_url,
    )


def _validate_render_input(
    opportunity: CanonicalOpportunityRecord,
    match: MatchTraceRecord,
) -> None:
    trace = match.trace
    if trace.opportunity_id != opportunity.id:
        raise LeadCardRenderError("match trace belongs to another Opportunity")
    if (
        not trace.hard_filter_eligible
        or not trace.eligible
        or trace.decision_code is not MatchDecisionCode.ELIGIBLE
    ):
        raise LeadCardRenderError("only an eligible persisted match may be rendered")
    if opportunity.lifecycle_status is not OpportunityLifecycleStatus.ACTIVE:
        raise LeadCardRenderError("inactive Opportunity cannot be rendered")
    if trace.opportunity_lifecycle_status != OpportunityLifecycleStatus.ACTIVE.value:
        raise LeadCardRenderError("match trace was not evaluated for an active Opportunity")
    if trace.opportunity_last_seen_at != opportunity.last_seen_at:
        raise LeadCardRenderError("match trace does not describe the current Opportunity")


def _lead_title(opportunity: CanonicalOpportunityRecord) -> str:
    explicit = _clean_text(
        opportunity.canonical_title or opportunity.analysis.role_title,
        limit=_TITLE_LIMIT,
    )
    if explicit:
        return explicit
    return _DEFAULT_TITLES[opportunity.analysis.opportunity_type]


def _lead_metadata(opportunity: CanonicalOpportunityRecord) -> tuple[str, ...]:
    analysis = opportunity.analysis
    values: list[str] = []
    budget = _budget_label(analysis.budget)
    if budget:
        values.append(budget)
    opportunity_type = _OPPORTUNITY_TYPE_LABELS.get(analysis.opportunity_type)
    if opportunity_type:
        values.append(opportunity_type)
    if analysis.work.remote is True:
        values.append("удалённо")
    elif analysis.work.remote is False:
        values.append("очно")
    location = _clean_text(analysis.work.location, limit=120)
    if location:
        values.append(location)
    if analysis.work.full_time is True:
        values.append("полная занятость")
    elif analysis.work.part_time is True:
        values.append("частичная занятость")
    language = _clean_text(analysis.language, limit=64)
    if language:
        values.append(language)
    return _deduplicate(values)


def _budget_label(budget) -> str | None:
    if not budget.known:
        return None
    minimum = None if budget.min is None else Decimal(str(budget.min))
    maximum = None if budget.max is None else Decimal(str(budget.max))
    amounts = tuple(value for value in (minimum, maximum) if value is not None)
    if not amounts:
        return None
    abbreviated = all(
        value >= Decimal("10000") and value % Decimal("1000") == 0
        for value in amounts
    )

    def amount(value: Decimal) -> str:
        rendered = value / Decimal("1000") if abbreviated else value
        return _decimal_label(rendered)

    if minimum is not None and maximum is not None:
        numeric = (
            amount(minimum)
            if minimum == maximum
            else f"{amount(minimum)}–{amount(maximum)}"
        )
    elif minimum is not None:
        numeric = f"от {amount(minimum)}"
    else:
        numeric = f"до {amount(maximum)}"
    if abbreviated:
        numeric = f"{numeric} тыс."
    currency = _currency_label(budget.currency)
    period = _BUDGET_PERIOD_LABELS.get((budget.period or "").casefold(), "")
    return " ".join(item for item in (numeric, currency) if item) + period


def _currency_label(currency: str | None) -> str:
    if not currency:
        return ""
    normalized = currency.upper()
    return _CURRENCY_LABELS.get(normalized, normalized)


def _decimal_label(value: Decimal) -> str:
    normalized = value.normalize()
    rendered = format(normalized, "f")
    integer, separator, fraction = rendered.partition(".")
    grouped = f"{int(integer):,}".replace(",", " ")
    return grouped if not separator else f"{grouped},{fraction.rstrip('0')}"


def _skills_line(skills: tuple[str, ...]) -> str | None:
    normalized = _deduplicate(
        item
        for item in (
            _clean_text(skill, limit=120)
            for skill in skills
        )
        if item
    )
    if not normalized:
        return None
    return ", ".join(normalized[:_SKILL_LIMIT])


def _safe_source_url(opportunity: CanonicalOpportunityRecord) -> str | None:
    source = opportunity.preferred_source
    if source is None:
        return None
    value = source.message_url.strip()
    parsed = urlsplit(value)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        return None
    return value


def _footer(
    *,
    source_url: str | None,
    last_seen_at: datetime,
    rendered_at: datetime,
) -> str:
    freshness = _freshness_label(last_seen_at, rendered_at)
    if source_url is None:
        return freshness
    return f'<a href="{html.escape(source_url, quote=True)}">Источник</a> · {freshness}'


def _freshness_label(last_seen_at: datetime, rendered_at: datetime) -> str:
    observed_at = _aware_utc(last_seen_at, "last_seen_at")
    seconds = max(0, int((rendered_at - observed_at).total_seconds()))
    if seconds < 60:
        return "только что"
    minutes = seconds // 60
    if minutes < 60:
        return f"{minutes} {_plural(minutes, 'минуту', 'минуты', 'минут')} назад"
    hours = minutes // 60
    if hours < 24:
        return f"{hours} {_plural(hours, 'час', 'часа', 'часов')} назад"
    days = hours // 24
    return f"{days} {_plural(days, 'день', 'дня', 'дней')} назад"


def _plural(value: int, one: str, few: str, many: str) -> str:
    remainder = value % 100
    if 11 <= remainder <= 14:
        return many
    remainder = value % 10
    if remainder == 1:
        return one
    if 2 <= remainder <= 4:
        return few
    return many


def _clean_text(value: str | None, *, limit: int) -> str | None:
    if value is None:
        return None
    normalized = re.sub(r"\s+", " ", value).strip()
    if not normalized:
        return None
    if len(normalized) <= limit:
        return normalized
    return normalized[: limit - 1].rstrip() + "…"


def _deduplicate(values) -> tuple[str, ...]:
    result: list[str] = []
    seen: set[str] = set()
    for value in values:
        identity = value.casefold()
        if identity in seen:
            continue
        seen.add(identity)
        result.append(value)
    return tuple(result)


def _aware_utc(value: datetime, field: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise LeadCardRenderError(f"{field} must include a timezone")
    return value.astimezone(timezone.utc)
