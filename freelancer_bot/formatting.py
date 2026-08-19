from __future__ import annotations

import html
import re
from datetime import datetime

from .sources import Source
from .storage import LeadRecord, StoredLead
from .replies import ReplyDraft


CONTACT_RE = re.compile(
    r"(?P<username>@[A-Za-z0-9_]{5,32})|(?P<email>[\w.+-]+@[\w-]+\.[\w.-]+)|(?P<url>https?://\S+)"
)


def truncate(text: str, limit: int = 1100) -> str:
    cleaned = re.sub(r"\s+", " ", text).strip()
    if len(cleaned) <= limit:
        return cleaned
    return cleaned[: limit - 1].rstrip() + "..."


def extract_contacts(text: str) -> tuple[str, ...]:
    contacts: list[str] = []
    for match in CONTACT_RE.finditer(text):
        value = match.group(0).rstrip(".,;)")
        if value not in contacts:
            contacts.append(value)
    return tuple(contacts[:8])


def format_lead(source: Source, lead: LeadRecord | StoredLead, lead_id: int | None = None) -> str:
    contacts = extract_contacts(lead.text)
    contact_line = ", ".join(html.escape(item) for item in contacts) if contacts else "-"
    keywords = ", ".join(html.escape(item) for item in lead.keywords[:6]) or "-"
    date_text = lead.message_date
    try:
        date_text = datetime.fromisoformat(lead.message_date).strftime("%Y-%m-%d %H:%M")
    except ValueError:
        pass

    link_line = f'<a href="{html.escape(lead.link)}">Открыть оригинал</a>' if lead.link else ""
    excerpt = html.escape(truncate(lead.text))

    id_line = f"#{lead_id}" if lead_id is not None else "-"

    return (
        f"<b>📌 Лид {id_line}</b> · score {lead.score}\n"
        f"<b>Источник:</b> {html.escape(source.title)}\n"
        f"<b>Дата:</b> {html.escape(date_text)}\n"
        f"<b>Контакты:</b> {contact_line}\n"
        f"<b>Почему попал:</b> {keywords}\n"
        f"{link_line}\n\n"
        f"<b>Текст заявки</b>\n"
        f"<blockquote>{excerpt}</blockquote>"
    )


def format_reply_draft(lead: StoredLead, draft: ReplyDraft) -> str:
    risks = "\n".join(f"• {html.escape(item)}" for item in draft.risks) or "• не отмечены"
    questions = "\n".join(f"• {html.escape(item)}" for item in draft.questions_to_client) or "• не нужны"
    return (
        f"<b>✍️ Черновик отклика</b> · лид #{lead.id}\n"
        f"<b>Fit:</b> {draft.fit_score}/100\n\n"
        f"<b>Отправить можно так</b>\n"
        f"<blockquote>{html.escape(draft.proposal_draft)}</blockquote>\n\n"
        f"<b>Короткая версия</b>\n"
        f"<blockquote>{html.escape(draft.short_reply)}</blockquote>\n\n"
        f"<b>Вопросы</b>\n{questions}\n\n"
        f"<b>Риски</b>\n{risks}\n\n"
        f"<b>Почему подходит</b>\n{html.escape(draft.fit_summary)}\n\n"
        f'<a href="{html.escape(lead.link)}">Открыть пост</a>'
    )
