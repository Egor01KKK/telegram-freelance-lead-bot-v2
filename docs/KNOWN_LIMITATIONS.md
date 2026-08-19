# Known limitations and product audit

This is a code-backed release note, not a claim that the product is production-quality.

## Product path

`/start` opens the navigation service. “Новый поиск” enters natural-language onboarding and creates a PostgreSQL SearchProfile draft through the configured OpenAI-compatible analyzer. The user confirms and activates the draft. Activation creates the profile discovery intent and durable discovery job. The live source path then depends on approved readable sources, raw ingestion, prefilter, a configured AI analyzer, canonical Opportunity creation, matching, valid entitlement, and the personalized delivery worker.

## Answers to the release audit questions

A. Multiple SearchProfiles are supported per user; repository ownership checks isolate them.

B. A newly activated profile creates discovery work. Existing Opportunities are not automatically re-evaluated merely because a profile was activated; matching is driven by the matching-delivery job path.

C. Natural-language onboarding is the default “Новый поиск” path. `/profile_manual` is an explicit manual command only.

D. Yes. `--bot-only` starts the bot UI without the user collector, catch-up, discovery, durable ingestion worker, matching worker, or delivery sender. It still needs PostgreSQL and Telegram bot/API credentials.

E. No. The no-argument command now prints safe help. `--run` is explicit and may start external work according to its configuration.

F. Telegram Global/SearchGlobal and chat discovery are implemented behind explicit, bounded discovery services and jobs. They are not a promise of source quality or continuous coverage.

G. A discovered source becomes usable only after durable materialization, access validation for the selected collector, Source Audit/lifecycle policy, and an explicit approved state. Runtime catalog reload uses PostgreSQL; JSON is only seed/diagnostic input.

H. Source Audit enforces bounded samples and an evidence floor. The policy can return approved, rejected, or needs_review; insufficient or inaccessible evidence is not silently approved.

I. Approved sources are read by the explicit collector runtime. No-argument and bot-only modes do not read them.

J. Canonical Opportunity creation enqueues the existing matching/delivery job path. The durable job and persisted match run/trace keys are idempotent.

K. Zero eligible matches is a valid result when hard filters, structured score, semantic score, freshness, rank threshold, profile preferences, entitlement, or lifecycle state reject every candidate. Thresholds are intentionally not weakened to force a card.

L. When semantic data is absent, the deterministic structured path remains authoritative; it does not invent semantic similarity.

M. Invalid AI output and transport retries are bounded by configured attempt counts. Fallback is disabled by public default because it can multiply paid calls. Spend guards and reserves are applied to configured Opportunity analysis.

N. Durable jobs use leases, heartbeats, retry states, and stale-lease reclamation. A restart can reclaim an expired lease; external side effects remain at-least-once with idempotent database boundaries.

O. Opportunity analysis is a global durable-job path keyed by raw-message/dedup identity, not one AI call per user. Profile-specific matching does not re-run Opportunity analysis.

P. A new source must be discovered, persisted with provenance, resolved by an authenticated collector, sampled/audited, approved through lifecycle, loaded into the collector catalog, produce raw messages, pass prefilter and analysis, create an Opportunity, pass a user's match/entitlement checks, schedule a delivery, and complete a Telegram send.

## Top product blockers

1. **P0 — Sparse or zero useful results:** no active profile, no approved readable source, no naturally relevant Opportunity, or a strict match/entitlement decision yields no card. This is expected behavior, but live operation needs observability and an operator-controlled source/profile setup.
2. **P0 — External Telegram availability:** FloodWait, inaccessible/private sources, and account permissions can stop discovery or ingestion independently of PostgreSQL.
3. **P1 — AI provider dependency:** without a configured BYOK provider, analysis jobs remain queued and natural-language profile extraction cannot complete.
4. **P1 — Source quality variance:** candidate discovery does not guarantee an active, high-yield source; audit may reject or defer most candidates.
5. **P1 — No automatic rematch guarantee for old Opportunities:** activation/discovery and matching are separate durable paths.
6. **P1 — At-least-once external delivery:** Telegram send and PostgreSQL confirmation cannot be one atomic transaction, so a crash window remains despite idempotent keys.
7. **P2 — Evaluation evidence is not live evidence:** synthetic fixtures and captured samples do not establish general production quality.
8. **P2 — Billing/provider deployment choices remain external:** provider-neutral core and adapters do not constitute a configured production payment operation.
9. **P2 — Legacy V1 compatibility remains:** full runtime can still construct legacy SQLite components, although SQLite is not V2 storage and legacy delivery is off by default.
10. **P2 — Web discovery is optional:** without Brave/primary/SearXNG configuration it skips as unavailable; it cannot supply sources by itself.
