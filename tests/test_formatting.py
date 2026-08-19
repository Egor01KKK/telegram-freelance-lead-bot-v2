import unittest

from freelancer_bot.formatting import format_lead, format_reply_draft
from freelancer_bot.replies import ReplyDraft
from freelancer_bot.sources import Source
from freelancer_bot.storage import LeadRecord, StoredLead


class FormattingTest(unittest.TestCase):
    def test_formats_lead_as_readable_card(self):
        body = format_lead(
            Source("@test", "Test Source", "demo"),
            LeadRecord(
                source="@test",
                message_id=1,
                link="https://t.me/test/1",
                text="Нужно сделать сайт. Контакт @client",
                score=9,
                keywords=("сайт", "нужно сделать"),
                message_date="2026-05-10T12:00:00+00:00",
            ),
            lead_id=12,
        )

        self.assertIn("📌 Лид #12", body)
        self.assertIn("Текст заявки", body)
        self.assertIn("<blockquote>", body)
        self.assertIn("Открыть оригинал", body)

    def test_formats_draft_as_reply_card(self):
        body = format_reply_draft(
            StoredLead(
                id=12,
                source="@test",
                message_id=1,
                link="https://t.me/test/1",
                text="Нужно сделать сайт",
                score=9,
                keywords=("сайт",),
                message_date="2026-05-10T12:00:00+00:00",
                status="draft_ready",
                ai_draft_json=None,
                notification_chat_id=123,
                notification_message_id=456,
            ),
            ReplyDraft(
                fit_summary="Подходит",
                fit_score=90,
                risks=(),
                questions_to_client=("Есть ли макет?",),
                proposal_draft="Здравствуйте. Могу помочь.",
                short_reply="Здравствуйте. Могу помочь.",
            ),
        )

        self.assertIn("✍️ Черновик отклика", body)
        self.assertIn("Отправить можно так", body)
        self.assertIn("<blockquote>", body)


if __name__ == "__main__":
    unittest.main()
