# Contributing

## Scope

Keep changes small, auditable, and provider-neutral. Do not add live credentials, Telegram sessions, captured private messages, database dumps, or generated reports to a pull request.

## Local setup

```bash
uv sync --locked
cp .env.example .env
uv run --frozen python -m freelancer_bot
```

The no-argument command is intentionally safe help. Use `--bot-only`, `--collector-only`, or `--run` only for an explicitly controlled local test.

## Verification

Before opening a pull request:

```bash
uv run --frozen python -m unittest discover -s tests
uv run --frozen python -m py_compile freelancer_bot/*.py freelancer_bot/persistence/*.py migrations/*.py migrations/versions/*.py
uv run --frozen alembic check
git diff --check
```

PostgreSQL-backed tests use `TEST_DATABASE_URL`. Provider, Telegram, payment, and Web tests must use fakes or local fixtures. Do not run paid or live network calls from CI.

## Data and secrets

Use temporary test databases and temporary session paths. Keep `.env`, sessions, SQLite files, logs, reports, and artifacts ignored. If a secret is exposed, rotate it and report only the credential class and affected scope.

## Changes

Add regression coverage for behavior changes. Preserve append-only evidence, idempotency keys, ownership checks, SearchProfile isolation, and PostgreSQL V2 boundaries. Do not weaken thresholds or turn synthetic fixtures into production claims.
