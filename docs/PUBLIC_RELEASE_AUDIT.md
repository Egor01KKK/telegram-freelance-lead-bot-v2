# Public release audit

## Scope and method

Release base: `a378cf4`. Release branch: `public-release-prep`.

The audit is offline and read-only with respect to external services. It covers the current source tree, tracked/untracked publication candidates, GitHub workflow, configuration, runtime entrypoints, persistence boundaries, logs, session handling, Web fetchers, Telegram handlers, and tests. The standard Codex Security scan completed with zero findings and six reviewed surfaces; its worker preflight was degraded, so the result is supplemented by the parent-led checks below. Deep Scan was not retried.

No live Telegram, OpenAI, DeepSeek, Web, Brave, payment, or discovery calls are part of release validation.

## Secret and history status

- Working-tree scan: local `.env`, Telethon sessions, legacy database, live artifacts, and profile data are untracked/ignored; values were not printed.
- Reachable-history scan: 1,047 reachable Git blobs were checked with deterministic key, Telegram-token, credentialed-PostgreSQL-DSN and bearer-token patterns; all four match counts were zero. gitleaks/trufflehog were unavailable and binary/entropy/provider-specific coverage is incomplete.
- Classification: `HISTORY_SCAN_PARTIAL`, not `HISTORY_SAFE`.
- Required rotation: credentials previously pasted into chat or used in live tests must be rotated before any public use. This includes Telegram bot/API/session credentials, database credentials, and any OpenAI/DeepSeek/TokenRouter/Brave/payment keys in the local environment.
- Publication strategy: `PUBLIC_ORPHAN_SNAPSHOT_RECOMMENDED`. The repository history contains internal Execution Pack and live-operation material even though no secret was confirmed by the checked patterns. Do not rewrite or push remote history from this task.

## Security review

Source-backed checks covered credential logging/redaction, subprocess use, SQL construction, file/session locks, JSON deserialization, Web URL fetch/redirect validation, Telegram HTML rendering, authorization/ownership checks, webhook/payment boundaries, and dangerous file operations.

No release-blocking security finding was confirmed in the current publication tree. The standard scan reported zero findings. The Web fetcher validates public DNS/IP destinations and each redirect; DNS rebinding remains a low-confidence hardening caveat because urllib resolves the hostname at connect time after validation. It is documented rather than represented as a production guarantee.

## Functional safety result

- No-argument startup is help-only.
- `--bot-only` does not create SQLite legacy storage, user-session clients, collectors, durable workers, matching workers, or delivery workers.
- `--run` and `--collector-only` remain explicit opt-in network modes.
- Missing AI/Web credentials fail closed or skip the optional capability.
- CI has no external provider credentials and exercises only deterministic/fake-provider paths.

## Database and migration validation

The migration graph has one head, `20260818_0036`. The fresh temporary-PostgreSQL migration test passed clean upgrade, current-head check, autogenerate check, downgrade-to-base, and re-upgrade-to-head. The local operator database is an older live/WIP environment and `alembic check` there reports leftover `source_audits_v2` objects from that WIP line; that database was not modified or downgraded. Use a fresh database or the documented migration path for publication validation. No SQLite V2 path is added.

## Deterministic release checks

- focused CLI/config/bot-only/security/opportunity tests: **57 passed, 0 failed**;
- full PostgreSQL regression: **484 passed, 0 failed** with `TEST_DATABASE_URL` set to the local PostgreSQL service; the suite used temporary databases and deterministic/fake providers;
- no-argument startup: exit 0, help-only, no runtime started;
- `git diff --check`: passed;
- synthetic evaluation output remains test-fixture evidence and is not a production-quality claim.

The Codex Security standard scan was sealed as scan `1de78144-3d2e-4260-8167-53e75acfac2f` with zero findings. Its report covers the original base snapshot, and the worktree changed during the scan; the final handoff therefore relies on the supplementary current-tree checks as well.

## Review checklist

- [ ] Rotate all credentials previously exposed in chat/live testing.
- [ ] Review `README.md`, `SECURITY.md`, `CONTRIBUTING.md`, and this audit.
- [ ] Review the orphan-publication recommendation before creating a GitHub repository/release.
- [ ] Run a fresh sanitized-clone test with no `.env`, sessions, API keys, or local artifacts.
- [ ] Perform live Telegram/provider verification separately and label it as live evidence.
