import json
import unittest
from unittest.mock import MagicMock, patch

from freelancer_bot.replies import OpenAIReplyDraftGenerator, ReplyDraft
from freelancer_bot.storage import StoredLead


class ReplyDraftTest(unittest.TestCase):
    def test_round_trips_draft_dict(self):
        data = {
            "fit_summary": "Заказ подходит под Telegram-бота",
            "fit_score": "82",
            "risks": ["мало деталей"],
            "questions_to_client": ["Есть ли ТЗ?"],
            "proposal_draft": "Здравствуйте, могу помочь.",
            "short_reply": "Могу помочь, есть пару вопросов.",
        }

        draft = ReplyDraft.from_dict(data)

        self.assertEqual(draft.fit_score, 82)
        self.assertEqual(draft.risks, ("мало деталей",))
        self.assertEqual(draft.as_dict()["questions_to_client"], ["Есть ли ТЗ?"])

    def test_accepts_string_risks_and_questions(self):
        draft = ReplyDraft.from_dict(
            {
                "fit_summary": "Подходит",
                "fit_score": 80,
                "risks": "мало деталей",
                "questions_to_client": "Есть ли макет?",
                "proposal_draft": "Отклик",
                "short_reply": "Коротко",
            }
        )

        self.assertEqual(draft.risks, ("мало деталей",))
        self.assertEqual(draft.questions_to_client, ("Есть ли макет?",))

    def test_sanitizes_banned_punctuation_and_phrases(self):
        draft = ReplyDraft.from_dict(
            {
                "fit_summary": "Подходит: можно сделать",
                "fit_score": 80,
                "risks": ["Нужно уточнить — форму"],
                "questions_to_client": [],
                "proposal_draft": "Здравствуйте: ваш проект мне очень интересен — могу помочь",
                "short_reply": "Могу помочь: уточню",
            }
        ).sanitized(
            {
                "banned_punctuation": ["—", ":"],
                "banned_phrases": ["ваш проект мне очень интересен"],
            }
        )

        self.assertNotIn("—", draft.proposal_draft)
        self.assertNotIn(":", draft.short_reply)
        self.assertNotIn("ваш проект мне очень интересен", draft.proposal_draft)

    def test_removes_bad_opening_phrases(self):
        draft = ReplyDraft.from_dict(
            {
                "fit_summary": "Понял, нужно починить бота",
                "fit_score": 70,
                "risks": [],
                "questions_to_client": [],
                "proposal_draft": "Понял, нужно починить телеграмм бота. Могу помочь.",
                "short_reply": "Понимаю, могу помочь.",
            }
        ).sanitized({"banned_starts": ["Понял", "Понимаю"]})

        self.assertFalse(draft.proposal_draft.startswith("Понял"))
        self.assertFalse(draft.short_reply.startswith("Понимаю"))

    def test_openai_adapter_uses_configured_model_temperature_and_timeout(self):
        api_response = {
            "choices": [
                {
                    "message": {
                        "content": json.dumps(
                            {
                                "fit_summary": "Подходит",
                                "fit_score": 80,
                                "risks": [],
                                "questions_to_client": [],
                                "proposal_draft": "Могу помочь.",
                                "short_reply": "Могу помочь.",
                            }
                        )
                    }
                }
            ]
        }
        response = MagicMock()
        response.read.return_value = json.dumps(api_response).encode("utf-8")
        response_context = MagicMock()
        response_context.__enter__.return_value = response
        generator = OpenAIReplyDraftGenerator(
            "test-api-key",
            "configured-model",
            temperature=0.7,
            timeout_seconds=12,
        )

        with patch("urllib.request.urlopen", return_value=response_context) as urlopen:
            draft = generator.generate(_stored_lead(), {})

        request = urlopen.call_args.args[0]
        payload = json.loads(request.data.decode("utf-8"))
        self.assertEqual(payload["model"], "configured-model")
        self.assertEqual(payload["temperature"], 0.7)
        self.assertEqual(urlopen.call_args.kwargs["timeout"], 12)
        self.assertEqual(draft.fit_score, 80)


def _stored_lead():
    return StoredLead(
        id=1,
        source="@test",
        message_id=2,
        link="https://t.me/test/2",
        text="Нужен Telegram-бот",
        score=7,
        keywords=("telegram",),
        message_date="2026-08-08T12:00:00+00:00",
        status="new",
        ai_draft_json=None,
        notification_chat_id=None,
        notification_message_id=None,
    )


if __name__ == "__main__":
    unittest.main()
