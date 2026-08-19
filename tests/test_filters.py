import unittest

from freelancer_bot.filters import match_text


class FilterTest(unittest.TestCase):
    def test_accepts_telegram_bot_project(self):
        result = match_text("Нужно разработать телеграм бот на Python с оплатой и админкой")

        self.assertTrue(result.accepted)
        self.assertGreaterEqual(result.score, 3)
        self.assertIn("телеграм бот", result.matched_keywords)

    def test_accepts_parser_project(self):
        result = match_text("Ищем разработчика: парсер сайта и уведомления в Telegram")

        self.assertTrue(result.accepted)
        self.assertIn("парсер", result.matched_keywords)

    def test_rejects_smm_noise(self):
        result = match_text("Нужен SMM специалист для ведения Instagram и Telegram")

        self.assertFalse(result.accepted)
        self.assertIn("smm", result.rejected_by)

    def test_rejects_low_score(self):
        result = match_text("Нужен ответственный человек на удаленку")

        self.assertFalse(result.accepted)
        self.assertEqual(result.score, 0)


if __name__ == "__main__":
    unittest.main()

