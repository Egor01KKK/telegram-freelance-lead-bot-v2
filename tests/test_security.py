import re
import subprocess
import sys
import tomllib
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


class RepositoryHygieneTest(unittest.TestCase):
    def test_runtime_matches_pinned_python(self):
        pinned = (ROOT / ".python-version").read_text(encoding="utf-8").strip()
        self.assertEqual(pinned, "3.14.7")
        self.assertEqual(".".join(map(str, sys.version_info[:3])), pinned)

    def test_uv_and_public_registry_are_reproducibly_pinned(self):
        pyproject = tomllib.loads((ROOT / "pyproject.toml").read_text(encoding="utf-8"))
        self.assertEqual(pyproject["tool"]["uv"]["required-version"], "==0.12.2")

        workflow = (ROOT / ".github/workflows/tests.yml").read_text(encoding="utf-8")
        self.assertIn('version: "0.12.2"', workflow)
        self.assertIn("image: postgres:18.4-alpine", workflow)

        compose = (ROOT / "compose.yaml").read_text(encoding="utf-8")
        self.assertIn("image: postgres:18.4-alpine", compose)
        self.assertIn("postgres-data:/var/lib/postgresql", compose)

        lock = tomllib.loads((ROOT / "uv.lock").read_text(encoding="utf-8"))
        packages = {package["name"]: package for package in lock["package"]}
        expected = {
            "alembic": "1.18.5",
            "pydantic": "2.13.4",
            "pydantic-settings": "2.15.0",
            "pydantic-core": "2.46.4",
            "psycopg": "3.3.4",
            "psycopg-binary": "3.3.4",
            "sqlalchemy": "2.0.51",
        }
        for name, version in expected.items():
            with self.subTest(package=name):
                self.assertEqual(packages[name]["version"], version)
                self.assertEqual(
                    packages[name]["source"],
                    {"registry": "https://pypi.org/simple"},
                )

    def test_postgres_runtime_never_creates_or_alters_schema(self):
        persistence_root = ROOT / "freelancer_bot" / "persistence"
        forbidden = ("create_all", "CREATE TABLE", "ALTER TABLE")
        findings = []
        for path in persistence_root.glob("*.py"):
            text = path.read_text(encoding="utf-8")
            for marker in forbidden:
                if marker in text:
                    findings.append(f"{path.name}: {marker}")
        self.assertEqual(findings, [])

    def test_secret_session_and_database_paths_are_ignored(self):
        for relative_path in (
            ".env",
            ".env.production",
            "sessions/probe.session",
            "data/probe.sqlite3",
            "probe.db",
            "freelancer_profile.json",
            "artifacts/live-report.md",
            "logs/runtime.log",
            ".env.safe-view",
        ):
            with self.subTest(path=relative_path):
                result = subprocess.run(
                    ["git", "check-ignore", "--no-index", "--quiet", relative_path],
                    cwd=ROOT,
                    check=False,
                )
                self.assertEqual(result.returncode, 0, f"Expected ignored path: {relative_path}")

    def test_repository_contains_no_forbidden_runtime_files_or_high_confidence_tokens(self):
        forbidden_paths = []
        token_findings = []
        patterns = {
            "telegram_bot_token": re.compile(r"\b\d{6,}:[A-Za-z0-9_-]{20,}\b"),
            "openai_api_key": re.compile(r"\bsk-(?:proj-)?[A-Za-z0-9_-]{20,}\b"),
            "credentialed_postgres_dsn": re.compile(
                r"postgres(?:ql)?(?:\+[a-z0-9_]+)?://[^:\s]+:[^@\s]{8,}@",
                re.IGNORECASE,
            ),
        }

        for path in _repository_files():
            relative = path.relative_to(ROOT)
            normalized = relative.as_posix()
            name = relative.name
            if (
                normalized == ".env"
                or (name.startswith(".env.") and name != ".env.example")
                or "sessions" in relative.parts
                or name == "freelancer_profile.json"
                or name.endswith((".session", ".session-journal", ".db", ".sqlite", ".sqlite3"))
                or ".db-" in name
                or ".sqlite-" in name
                or ".sqlite3-" in name
            ):
                forbidden_paths.append(normalized)
                continue

            try:
                text = path.read_text(encoding="utf-8")
            except (UnicodeDecodeError, OSError):
                continue
            for label, pattern in patterns.items():
                if pattern.search(text):
                    token_findings.append(f"{normalized}: {label}")

        self.assertEqual(forbidden_paths, [], f"Forbidden runtime files: {forbidden_paths}")
        self.assertEqual(token_findings, [], f"Potential committed secrets: {token_findings}")


def _repository_files():
    result = subprocess.run(
        ["git", "ls-files", "--cached", "--others", "--exclude-standard", "-z"],
        cwd=ROOT,
        check=True,
        capture_output=True,
    )
    return [ROOT / item for item in result.stdout.decode("utf-8").split("\0") if item]


if __name__ == "__main__":
    unittest.main()
