import json
import tempfile
import unittest
from pathlib import Path

from freelancer_bot.filters import load_filter_config, match_text


class FilterConfigTest(unittest.TestCase):
    def test_loads_custom_rules_from_json(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "filters.json"
            path.write_text(
                json.dumps(
                    {
                        "min_score": 5,
                        "keywords": {"custom order": 5, "python": 1},
                        "stop_words": ["casino"],
                    }
                ),
                encoding="utf-8",
            )

            config = load_filter_config(path)
            result = match_text("Новый custom order на Python", config)

            self.assertEqual(config.min_score, 5)
            self.assertTrue(result.accepted)
            self.assertEqual(result.matched_keywords, ("custom order", "python"))

    def test_rejects_invalid_min_score(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "filters.json"
            path.write_text(
                json.dumps({"min_score": 0, "keywords": {"order": 1}, "stop_words": []}),
                encoding="utf-8",
            )

            with self.assertRaisesRegex(ValueError, "min_score"):
                load_filter_config(path)


if __name__ == "__main__":
    unittest.main()
