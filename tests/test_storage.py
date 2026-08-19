import tempfile
import unittest
from pathlib import Path

from freelancer_bot.storage import LeadRecord, Storage


class StorageTest(unittest.TestCase):
    def test_records_lead_and_returns_id(self):
        with tempfile.TemporaryDirectory() as tmp:
            storage = Storage(Path(tmp) / "leads.sqlite3")
            lead_id = storage.record_or_should_retry(_lead())

            self.assertIsInstance(lead_id, int)
            stored = storage.get_lead(lead_id)
            self.assertIsNotNone(stored)
            self.assertEqual(stored.text, "Нужно разработать телеграм бот")
            self.assertEqual(stored.status, "new")

            storage.close()

    def test_draft_is_cached_for_lead(self):
        with tempfile.TemporaryDirectory() as tmp:
            storage = Storage(Path(tmp) / "leads.sqlite3")
            lead_id = storage.record_or_should_retry(_lead())
            draft = {
                "fit_summary": "Подходит",
                "fit_score": 85,
                "risks": [],
                "questions_to_client": ["Есть ли ТЗ?"],
                "proposal_draft": "Здравствуйте...",
                "short_reply": "Готов обсудить.",
            }

            storage.mark_draft_requested(lead_id)
            storage.save_ai_draft(lead_id, draft)

            self.assertEqual(storage.get_ai_draft(lead_id), draft)
            self.assertEqual(storage.get_lead(lead_id).status, "draft_ready")

            storage.close()

    def test_can_mark_lead_ignored(self):
        with tempfile.TemporaryDirectory() as tmp:
            storage = Storage(Path(tmp) / "leads.sqlite3")
            lead_id = storage.record_or_should_retry(_lead())

            storage.mark_ignored(lead_id)

            self.assertEqual(storage.get_lead(lead_id).status, "ignored")
            storage.close()

    def test_saves_notification_message_for_reply_threading(self):
        with tempfile.TemporaryDirectory() as tmp:
            storage = Storage(Path(tmp) / "leads.sqlite3")
            lead_id = storage.record_or_should_retry(_lead())

            storage.mark_notification_message(lead_id, chat_id=123, telegram_message_id=456)

            stored = storage.get_lead(lead_id)
            self.assertEqual(stored.notification_chat_id, 123)
            self.assertEqual(stored.notification_message_id, 456)
            storage.close()


def _lead() -> LeadRecord:
    return LeadRecord(
        source="@test",
        message_id=1,
        link="https://t.me/test/1",
        text="Нужно разработать телеграм бот",
        score=10,
        keywords=("телеграм бот",),
        message_date="2026-05-10T12:00:00+00:00",
    )


if __name__ == "__main__":
    unittest.main()
