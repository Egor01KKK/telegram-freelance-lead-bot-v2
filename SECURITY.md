# Security policy

This repository is an opt-in Telegram collector. Treat every Telegram session, bot token, provider key, and database credential as a bearer secret.

## Never publish

Do not commit or paste:

- `.env` files or secret previews;
- Telethon `*.session`, lock, journal, WAL, or SQLite files;
- PostgreSQL DSNs containing credentials;
- Telegram API hashes, bot tokens, phone numbers, access hashes, or session strings;
- OpenAI, DeepSeek, TokenRouter, Brave, SearXNG, OAuth, webhook, or cookie credentials;
- raw Telegram messages, private source identities, personal contacts, or captured live-test reports.

The repository's public example files contain placeholders only. Tests may contain deliberately synthetic token-shaped fixtures; they are not credentials.

## If a secret is exposed

Rotate the credential before continuing:

1. Telegram bot token: revoke it with BotFather and create a replacement.
2. Telegram user/API credentials: terminate the affected Telegram session in Settings → Devices, then create a new local session path. Treat a leaked API hash as compromised.
3. AI/Web/payment keys: revoke and replace them at the provider.
4. Database credentials: rotate the user password and invalidate leaked DSNs.
5. Remove the secret from local logs and operator exports. Do not rely on deleting a later commit; reachable Git history may retain it.

Do not send replacement secrets in chat or issue comments. Put them only in a local ignored `.env` or an external secret manager.

## Runtime boundaries

- PostgreSQL is the V2 source of truth. Alembic is the only schema change path.
- SQLite is legacy V1 storage/import input, never V2 persistence.
- The no-argument CLI is safe help only.
- `--bot-only`, `--collector-only`, and `--run` are explicit network modes.
- A Telegram user session and a bot session must use different files and processes must not share a session path.
- Source approval/rejection/pause must go through the audited operator lifecycle; do not edit production tables manually.
- Missing AI/Web credentials fail closed or skip the optional capability. They must not trigger an unbounded retry loop.
- Logs are structured and redacted, but operators must still avoid logging message bodies or secrets.

## Reporting

For a suspected vulnerability, provide a minimal reproduction, affected file/symbol, impact, and a proposed fix without including credentials or private data. Use a private security channel configured by the repository owner; if none is configured, open a non-public maintainer contact before filing a public issue.

This project is not intended for spam, unauthorized scraping, credential sharing, or bypassing Telegram limits. Respect Telegram and provider terms, source access permissions, and applicable law.

