import json
import tempfile
import unittest
from pathlib import Path

from freelancer_bot.profile import load_freelancer_profile


class FreelancerProfileTest(unittest.TestCase):
    def test_returns_default_profile_when_file_is_missing(self):
        profile = load_freelancer_profile(Path("missing-profile.json"))

        self.assertIn("services", profile)
        self.assertIn("do_not_claim", profile)

    def test_loads_profile_from_json(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "profile.json"
            path.write_text(json.dumps({"name": "Egor", "services": ["боты"]}), encoding="utf-8")

            profile = load_freelancer_profile(path)

            self.assertEqual(profile["name"], "Egor")
            self.assertEqual(profile["services"], ["боты"])
            self.assertIn("do_not_claim", profile)


if __name__ == "__main__":
    unittest.main()
