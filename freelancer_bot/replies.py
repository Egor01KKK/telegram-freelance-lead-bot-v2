from __future__ import annotations

import json
import urllib.error
import urllib.request
from dataclasses import dataclass
from typing import Any

from .storage import StoredLead


OPENAI_CHAT_COMPLETIONS_URL = "https://api.openai.com/v1/chat/completions"


@dataclass(frozen=True)
class ReplyDraft:
    fit_summary: str
    fit_score: int
    risks: tuple[str, ...]
    questions_to_client: tuple[str, ...]
    proposal_draft: str
    short_reply: str

    def as_dict(self) -> dict[str, Any]:
        return {
            "fit_summary": self.fit_summary,
            "fit_score": self.fit_score,
            "risks": list(self.risks),
            "questions_to_client": list(self.questions_to_client),
            "proposal_draft": self.proposal_draft,
            "short_reply": self.short_reply,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "ReplyDraft":
        return cls(
            fit_summary=str(data.get("fit_summary", "")).strip(),
            fit_score=_safe_int(data.get("fit_score"), default=0),
            risks=_string_tuple(data.get("risks")),
            questions_to_client=_string_tuple(data.get("questions_to_client")),
            proposal_draft=str(data.get("proposal_draft", "")).strip(),
            short_reply=str(data.get("short_reply", "")).strip(),
        )

    def sanitized(self, freelancer_profile: dict[str, Any]) -> "ReplyDraft":
        return ReplyDraft(
            fit_summary=_sanitize_text(self.fit_summary, freelancer_profile),
            fit_score=self.fit_score,
            risks=tuple(_sanitize_text(item, freelancer_profile) for item in self.risks),
            questions_to_client=tuple(_sanitize_text(item, freelancer_profile) for item in self.questions_to_client),
            proposal_draft=_sanitize_text(self.proposal_draft, freelancer_profile),
            short_reply=_sanitize_text(self.short_reply, freelancer_profile),
        )


class ReplyDraftError(RuntimeError):
    pass


class OpenAIReplyDraftGenerator:
    def __init__(
        self,
        api_key: str,
        model: str,
        *,
        temperature: float = 0.3,
        timeout_seconds: int = 45,
    ):
        if not api_key:
            raise ReplyDraftError("OPENAI_API_KEY is not configured")
        self._api_key = api_key
        self.model = model
        self.temperature = temperature
        self.timeout_seconds = timeout_seconds

    def generate(self, lead: StoredLead, freelancer_profile: dict[str, Any]) -> ReplyDraft:
        payload = {
            "model": self.model,
            "temperature": self.temperature,
            "response_format": {"type": "json_object"},
            "messages": [
                {
                    "role": "system",
                    "content": (
                        "Ты пишешь короткий человеческий отклик фрилансера на заказ. "
                        "Цель отклика - получить ответ клиента, а не пересказать его пост. "
                        "Можно начать с нейтрального 'Здравствуйте.'. "
                        "Не начинай с 'Понял', 'Здравствуйте, меня заинтересовал', 'Здравствуйте! Ваш проект', 'Я увидел ваш проект'. "
                        "После приветствия сразу переходи к практическому действию или подходу. "
                        "Не повторяй очевидные детали из заявки длинной фразой. "
                        "Сразу покажи, какой первый практический шаг ты бы сделал. "
                        "Не говори клиенту о слабостях фрилансера, несовпадении специализации или низком fit score. "
                        "Если релевантность низкая, напиши аккуратный диагностический отклик без самоунижения. "
                        "Не выдумывай опыт, сроки, цены, клиентов и кейсы. "
                        "Если заказ мутный или рискованный, укажи риск только в поле risks, не в тексте отклика. "
                        "Строго соблюдай style_rules, banned_words, banned_phrases и banned_punctuation из профиля. "
                        "Если в профиле есть reply_structure, используй её как порядок мысли, но не добавляй заголовки без просьбы. "
                        "Верни только JSON с ключами: fit_summary, fit_score, risks, "
                        "questions_to_client, proposal_draft, short_reply."
                    ),
                },
                {
                    "role": "user",
                    "content": json.dumps(
                        {
                            "freelancer_profile": freelancer_profile,
                            "lead": {
                                "source": lead.source,
                                "link": lead.link,
                                "score": lead.score,
                                "keywords": list(lead.keywords),
                                "text": lead.text,
                            },
                            "rules": [
                                "proposal_draft должен быть живым русскоязычным откликом на 3-6 коротких предложений.",
                                "Первое предложение должно быть действием или подходом, а не пересказом задачи.",
                                "Если используешь приветствие, пиши 'Здравствуйте.' и сразу переходи к делу.",
                                "Не пиши 'Понял', 'Понимаю', 'Вам нужно', 'Вы ищете' в начале.",
                                "Не пиши 'хотя моя специализация', 'я больше по', 'это не совсем мой профиль'.",
                                "Не перечисляй весь стек, если он не нужен для конкретной задачи.",
                                "Добавь 1-3 уточняющих вопроса только если они помогают начать диагностику или оценку.",
                                "Не обещай отправку, созвон, цену или сроки, если этого нет в профиле.",
                                "Не используй длинное тире, если оно есть в banned_punctuation.",
                                "Не используй двоеточия, если ':' есть в banned_punctuation.",
                                "Не используй фразы из banned_phrases даже в измененном виде.",
                                "Если examples.good_reply и examples.bad_reply есть в профиле, подражай good_reply и избегай bad_reply.",
                                "short_reply должен быть короткой версией для Telegram.",
                                "fit_score от 0 до 100.",
                            ],
                        },
                        ensure_ascii=False,
                    ),
                },
            ],
        }
        request = urllib.request.Request(
            OPENAI_CHAT_COMPLETIONS_URL,
            data=json.dumps(payload).encode("utf-8"),
            headers={
                "Authorization": f"Bearer {self._api_key}",
                "Content-Type": "application/json",
            },
            method="POST",
        )
        try:
            with urllib.request.urlopen(request, timeout=self.timeout_seconds) as response:
                raw = response.read().decode("utf-8")
        except urllib.error.HTTPError as exc:
            detail = exc.read().decode("utf-8", errors="replace")
            raise ReplyDraftError(f"OpenAI request failed: {exc.code} {detail}") from exc
        except urllib.error.URLError as exc:
            raise ReplyDraftError(f"OpenAI request failed: {exc}") from exc

        data = json.loads(raw)
        content = data["choices"][0]["message"]["content"]
        return ReplyDraft.from_dict(json.loads(content)).sanitized(freelancer_profile)


def _safe_int(value: Any, default: int) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _string_tuple(value: Any) -> tuple[str, ...]:
    if value is None:
        return ()
    if isinstance(value, str):
        value = [value]
    if not isinstance(value, list):
        value = [value]
    return tuple(str(item).strip() for item in value if str(item).strip())


def _sanitize_text(text: str, freelancer_profile: dict[str, Any]) -> str:
    result = text
    banned_starts = freelancer_profile.get("banned_starts", [])
    default_banned_starts = [
        "Понял,",
        "Понял.",
        "Понимаю,",
        "Понимаю.",
        "Здравствуйте! Меня заинтересовал",
        "Здравствуйте, меня заинтересовал",
        "Я увидел ваш проект",
    ]
    for phrase in [*default_banned_starts, *banned_starts]:
        if result.startswith(str(phrase)):
            result = result[len(str(phrase)) :].lstrip(" ,.!-")
    for punctuation in freelancer_profile.get("banned_punctuation", []):
        if punctuation == "—":
            result = result.replace("—", "-")
        elif punctuation == ":":
            result = result.replace(":", ".")
        else:
            result = result.replace(str(punctuation), "")
    for phrase in freelancer_profile.get("banned_phrases", []):
        result = result.replace(str(phrase), "")
    for word in freelancer_profile.get("banned_words", []):
        result = result.replace(str(word), "")
    return " ".join(result.split())
