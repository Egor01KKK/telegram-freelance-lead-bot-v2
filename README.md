# Telegram Freelance Lead Bot

An opt-in Telegram collector and PostgreSQL-backed lead pipeline for a single operator or a small controlled deployment.

This repository is published as an engineering baseline, not as a hosted service. It does not include Telegram sessions, database dumps, API keys, captured messages, or production-quality guarantees.

## Safety first

The default command is intentionally non-operational:

```bash
uv run --frozen python -m freelancer_bot
```

It prints help and exits. It does not connect to Telegram, PostgreSQL, OpenAI, DeepSeek, Web Search, or start workers. Network and paid work require an explicit mode.

The supported modes are:

```bash
# User-facing Telegram UI only. No user collector, catch-up, discovery,
# matching worker, or delivery worker is started.
uv run --frozen python -m freelancer_bot --bot-only

# Full collector + durable pipeline + matching/delivery runtime.
uv run --frozen python -m freelancer_bot --run

# Authenticated user collector and source-discovery runtime only.
uv run --frozen python -m freelancer_bot --collector-only
```

Use `--run` only after reviewing the source catalog, session permissions, catch-up settings, AI budget, and delivery recipient. Keep `SEND_CATCH_UP=false` and `SOURCE_DISCOVERY_ENABLED=false` for an initial setup.

## What the system contains

- A Telethon user session reads approved Telegram sources that the account can access.
- A separate Telethon bot session serves the user-facing UI and sends cards.
- PostgreSQL is the source of truth for V2 users, profiles, raw messages, durable jobs, Opportunities, matching, deliveries, feedback, sources, and billing evidence.
- SQLite is legacy V1 storage only. It is not used as V2 storage and is not required by `--bot-only`.
- The durable worker prefilters raw messages, optionally analyzes them with a configured AI provider, materializes canonical Opportunities, evaluates matches, and schedules personalized deliveries.
- Source discovery and audit are explicit, bounded operator capabilities. They are disabled by default in the public example configuration.

Stable core: PostgreSQL migrations/repositories, durable jobs, deterministic filtering/matching, profile confirmation, delivery idempotency, redacted structured logs, and provider-neutral boundaries.

Experimental or operationally sensitive: live Telegram collection, Web/Brave/SearXNG discovery, Telegram global/chat discovery, Source Audit, OpenAI-compatible onboarding providers other than OpenAI, and payment-provider adapters. These require explicit configuration and real-world verification.

## Requirements

- Python 3.14.7 (pinned in `.python-version`)
- uv 0.12.2 (pinned in `pyproject.toml` and CI)
- PostgreSQL 18.x for V2
- Telegram API ID/hash and a user session for collection
- A Telegram bot token for `--bot-only` or `--run`
- An OpenAI-compatible key only for AI features; AI is BYOK and optional

## Setup

```bash
git clone https://github.com/Egor01KKK/telegram-freelance-lead-bot.git
cd telegram-freelance-lead-bot
uv python install 3.14.7
uv sync --locked
cp .env.example .env
docker compose up -d postgres
uv run --frozen alembic upgrade head
uv run --frozen python -m freelancer_bot.persistence.source_seed \
  --sources-json config/sources.json
uv run --frozen python -m freelancer_bot --check-config
```

Fill `.env` locally. Never commit it. Use a fresh Telegram user session path and a separate bot session path; never run two processes against the same Telethon session file.

Minimum values for V2 UI:

```dotenv
TELEGRAM_API_ID=...
TELEGRAM_API_HASH=...
TELEGRAM_BOT_TOKEN=...
DATABASE_URL=<your-postgresql-dsn>
```

`DATABASE_URL` must use the `postgresql+psycopg://` scheme. The production application does not create or alter PostgreSQL schema at startup; run Alembic first.

## Configuration by mode

| Mode | Required | External work |
| --- | --- | --- |
| `--check-config`, `--check-filter` | no credentials | local file validation only |
| `--bot-only` | Telegram API ID/hash, bot token, PostgreSQL | bot UI; user text may invoke one configured onboarding AI call |
| `--collector-only` | Telegram API ID/hash, PostgreSQL, user session | user-session catalog/collection; discovery only when explicitly enabled |
| `--run` | Telegram API ID/hash, bot token, PostgreSQL, user session | collector, durable workers, matching/delivery; catch-up/discovery/AI remain separately configured |
| `--check-sources` | Telegram API ID/hash, user session | bounded Telegram entity checks |

`OPENAI_API_KEY`, `DEEPSEEK_API_KEY`, `TOKENROUTER_API_KEY`, and `BRAVE_SEARCH_API_KEY` are never required for a fresh clone. Missing AI keys fail closed for the corresponding feature. Missing Web providers produce an unavailable/skip outcome; they do not cause a retry storm.

## Operator commands

All commands below are explicit and PostgreSQL-backed. They emit body-free or bounded operational data where the command supports it; do not paste private message contents into issue reports.

```bash
# Source catalog and auditable lifecycle transitions
uv run --frozen python -m freelancer_bot.operator_cli sources list --limit 100
uv run --frozen python -m freelancer_bot.operator_cli sources audits --limit 100
uv run --frozen python -m freelancer_bot.operator_cli sources transition \
  --source-id 123 --target paused --actor operator --reason "manual review"

# Web/Telegram discovery and durable run inspection
uv run --frozen python -m freelancer_bot.operator_cli discovery runs --limit 100
uv run --frozen python -m freelancer_bot.operator_cli discovery results --run-id UUID
uv run --frozen python -m freelancer_bot.operator_cli discovery graph \
  --run-key graph-canary-1 --seed-source-id 123 \
  --message-limit-per-seed 25 --max-candidates 5 --max-observations 100
uv run --frozen python -m freelancer_bot.operator_cli discovery web \
  --searxng-url http://127.0.0.1:8080 --run-key web-canary-1

# Source Audit (requires an authenticated collector, OpenAI key, and access)
uv run --frozen python -m freelancer_bot.operator_cli audit list --limit 100
uv run --frozen python -m freelancer_bot.operator_cli audit run --source-id 123

# Matching, delivery, and body-free product observations
uv run --frozen python -m freelancer_bot.operator_cli match runs --limit 100
uv run --frozen python -m freelancer_bot.operator_cli match traces --limit 100
uv run --frozen python -m freelancer_bot.operator_cli delivery list --limit 100
uv run --frozen python -m freelancer_bot.operator_cli observe raw --limit 100
uv run --frozen python -m freelancer_bot.operator_cli observe opportunities --limit 100
uv run --frozen python -m freelancer_bot.operator_cli observe metrics \
  --since 2026-01-01T00:00:00+00:00 --until 2026-01-02T00:00:00+00:00
```

Discovery and audit never approve a source by bypassing the lifecycle. Use the repository command to record an actor and reason. Public sources can be inspected by an authenticated collector; private sources still require explicit per-account access.

## AI and cost controls

The public defaults are deliberately conservative:

- AI reply generation is off.
- Opportunity fallback is off because it can multiply calls.
- Per-onboarding analyzer budget defaults to 10 calls per process.
- Opportunity analysis has default daily/monthly spend limits of USD 1/10, with reserves for in-flight calls.
- Structured-output retries are bounded by configuration.
- CI contains no provider credentials and makes no provider calls.

Bring your own key and set a budget appropriate for your account before enabling AI. Free or experimental providers are not treated as production-compatible merely because their endpoint accepts an OpenAI-shaped request. Provider selection must remain isolated and failures must fail closed.

## Telegram sessions and privacy

Telethon user sessions are bearer credentials. Store them outside Git, use separate paths per process/account, and revoke a session in Telegram Settings → Devices if it is exposed. The bot token, API hash, database DSN, provider keys, cookies, phone numbers, access hashes, message bodies, and private source identities must never be pasted into a public issue or chat.

Structured logs redact configured secrets, DSNs, authorization values, Telegram bot tokens, and AI keys. Message content fields are redacted. Logs and operator exports are local-only and ignored by Git.

## Known limitations

This project is not a hosted, turnkey marketplace. See [docs/KNOWN_LIMITATIONS.md](docs/KNOWN_LIMITATIONS.md) for the product audit. Important limitations include:

- useful results depend on an active confirmed SearchProfile, approved readable sources, enough naturally relevant Opportunities, and valid entitlement;
- matching thresholds are intentionally not tuned to guarantee a delivery;
- synthetic evaluation fixtures are not production quality evidence;
- source quality, Telegram availability, provider latency, and AI output quality require live verification;
- paid subscription and provider operations remain deployment-specific;
- V2 historical evidence is append-only/auditable, but the external Telegram send plus PostgreSQL state update cannot be exactly-once.

## Development and tests

```bash
uv sync --locked
uv run --frozen python -m unittest discover -s tests
uv run --frozen python -m py_compile freelancer_bot/*.py freelancer_bot/persistence/*.py migrations/*.py migrations/versions/*.py
uv run --frozen alembic check
```

PostgreSQL tests use `TEST_DATABASE_URL`; they are skipped or fail clearly when the database is unavailable. Tests use fakes for AI, Telegram, Web, and payment providers. Do not run a live collector or a provider-backed command in CI.

## License

MIT. See [LICENSE](LICENSE).
