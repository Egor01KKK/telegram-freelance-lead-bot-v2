import json
import tempfile
import unittest
from pathlib import Path

from freelancer_bot.sources import enabled_sources, load_sources


class SourcesConfigTest(unittest.TestCase):
    def test_loads_sources_and_normalizes_handles(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "sources.json"
            path.write_text(
                json.dumps(
                    [
                        {
                            "handle": "example_source",
                            "title": "Example",
                            "reason": "test source",
                            "enabled": True,
                            "tags": ["test", "development"],
                        },
                        {
                            "handle": "@disabled_source",
                            "title": "Disabled",
                            "reason": "not ready",
                            "enabled": False,
                        },
                    ]
                ),
                encoding="utf-8",
            )

            sources = load_sources(path)

            self.assertEqual(sources[0].handle, "@example_source")
            self.assertEqual(sources[0].tags, ("test", "development"))
            self.assertEqual([source.handle for source in enabled_sources(path)], ["@example_source"])

    def test_rejects_duplicate_handles(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "sources.json"
            path.write_text(
                json.dumps(
                    [
                        {"handle": "example_source", "title": "One", "reason": "test"},
                        {"handle": "@EXAMPLE_SOURCE", "title": "Two", "reason": "test"},
                    ]
                ),
                encoding="utf-8",
            )

            with self.assertRaisesRegex(ValueError, "duplicate handle"):
                load_sources(path)

    def test_rejects_invalid_username(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "sources.json"
            path.write_text(
                json.dumps([{"handle": "bad-name", "title": "Bad", "reason": "test"}]),
                encoding="utf-8",
            )

            with self.assertRaisesRegex(ValueError, "invalid Telegram username"):
                load_sources(path)


if __name__ == "__main__":
    unittest.main()
