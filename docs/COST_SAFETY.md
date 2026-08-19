# Cost and external-work safety

## Fresh clone defaults

A fresh clone has no credentials and the no-argument CLI exits after printing help. CI does not run `--run`, collector-only, discovery, audit, or provider calls.

Public configuration defaults:

- `AI_REPLY_ENABLED=false`
- `SEND_CATCH_UP=false`
- `SOURCE_DISCOVERY_ENABLED=false`
- `SOURCE_AUDIT_ENABLED=false`
- `SOURCE_GRAPH_DISCOVERY_ENABLED=false`
- `TELEGRAM_CHAT_DISCOVERY_ENABLED=false`
- Opportunity fallback disabled
- `MAX_AI_CALLS_PER_RUN=10`
- Opportunity analysis spend guard: USD 1 daily / USD 10 monthly
- bounded output and transport retries

These are safety defaults, not a promise that a deployment cannot spend money after an operator changes `.env`.

## Before enabling AI

1. Use a dedicated BYOK key with a low provider-side limit.
2. Set the model, provider, timeout, retry, and spend values explicitly.
3. Keep fallback disabled until its cost and output behavior are verified.
4. Start with a small approved source catalog and `SEND_CATCH_UP=false`.
5. Inspect AI telemetry and durable-job counts before increasing limits.

Missing keys fail closed. Do not substitute a key from a chat transcript or commit a key to the repository.

## Before enabling Telegram/Web discovery

Use a separate Telethon session per process and an authenticated account with only the intended source permissions. Keep governor pacing/cooldowns and repository limits enabled. Configure only the Web provider needed for the controlled test. A missing Web provider is an unavailable optional capability, not a signal to retry continuously.

## CI invariant

The GitHub workflow contains no provider credentials and sets external-work flags false. It validates configuration, migrations, imports, compilation, and deterministic tests only.
