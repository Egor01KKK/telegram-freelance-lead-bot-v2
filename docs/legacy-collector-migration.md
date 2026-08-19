# Telegram V1 collector migration constraints

This note records behavior characterized at G0. It is compatibility evidence
for the later migration, not a V2 product requirement or a new collector design.

## Current guarantees

- Startup creates separate Telethon user and bot clients, starts both, resolves
  every enabled configured source, registers one `NewMessage` handler per
  resolved source, optionally catches up, and only then enters steady-state
  waiting. A source that fails resolution is logged and skipped.
- Live and catch-up messages keep the configured source, Telegram message ID,
  message text and Telegram date. Missing text becomes an empty string and is
  ignored. A missing date receives the current UTC time.
- The V1 keyword score and stop-word rules run before persistence. Rejected,
  empty and non-text messages create no SQLite lead and no Telegram delivery.
  This filter is preserved only as V1 compatibility behavior.
- An accepted lead uses `(source handle, Telegram message ID)` as its SQLite
  identity and builds the original link from source username and message ID.
  A successfully notified identity is not delivered again when observed live,
  during catch-up, or after reopening the same SQLite database.
- Catch-up runs only when `SEND_CATCH_UP` is enabled and `CATCH_UP_LIMIT` is
  greater than zero. It asks Telethon for at most that many messages per
  resolved source, buffers those bounded results, and processes the combined
  set in ascending message-date order.
- Delivery visits the current SQLite subscriber list sequentially. It keeps the
  existing HTML lead card, disables link preview and includes the existing
  inline draft/ignore buttons. Telegram `RPCError` delivery failures are logged
  and leave the lead pending if no recipient succeeds.
- Normal shutdown disconnects the user client, disconnects the bot client, and
  then closes SQLite. Automated compatibility tests use only fake clients and
  temporary SQLite databases; they never open repository session files or
  `job_parser.db`.

## Known V1 limitations

- The catch-up limit is per source, so the combined maximum is the number of
  resolved sources multiplied by `CATCH_UP_LIMIT`. There is no durable Telegram
  cursor. Downtime beyond the fetched history can therefore leave a gap.
- SQLite has one notification chat/message pair per lead, not one delivery row
  per subscriber. Multiple successful sends overwrite that pair with the last
  success. Any single success sets the lead as notified, so recipients that
  failed during a partial-success attempt are not retried on rediscovery.
- If every delivery fails, or no subscriber exists, the accepted lead remains
  pending and is retried when the same Telegram identity is observed again.
- Telegram send and SQLite notification marking are not atomic. A process crash
  after Telegram accepts a send but before SQLite commits can cause a duplicate
  external notification after restart.
- Expected source-resolution, catch-up and delivery RPC failures are isolated as
  described above. Unexpected handler exceptions are not durably queued, and a
  failure during the sequential shutdown steps can prevent later cleanup steps.
- G0 did not route the collector through `DurableWorker`, did not write raw
  messages to PostgreSQL, and did not add Telegram production side effects.

## G3-T01 source-selection cutover

- The production Telegram runtime no longer reads `config/sources.json` when
  registering handlers. JSON remains seed/diagnostic input only.
- After the existing Telethon user client starts, its real Telegram account ID
  is idempotently bound to `collector_accounts`. PostgreSQL returns only
  `approved` Telegram sources available to that active account. A private
  source additionally requires explicit `source_collector_access=permitted`.
- Candidate, paused, rejected, inaccessible, revoked and unverified private
  sources never reach entity resolution or handler registration. An inactive
  collector account receives no sources. PostgreSQL errors abort startup; no
  JSON fallback can silently widen the monitored set.
- Each returned source is still resolved by the single existing Telethon user
  session. Resolution failure is isolated to that source. Private invite URLs
  are not used as lookup material or written to logs.
- Live handlers only dispatch to the compatibility handler and receive a
  correlation ID. Catch-up remains bounded per source and globally ordered by
  message date. Message text is not placed in normal collector logs.
- G3-T01 intentionally does not add `raw_messages`, enqueue ingestion jobs or
  start another worker framework. The V1 SQLite lead/subscriber/delivery path
  remains read/write so its historical `(source handle, message ID)` markers
  continue preventing accidental redelivery until the bounded raw-ingestion
  and cutover tasks replace it.

## G3-T02 raw-ingestion cutover

- Every live or bounded catch-up message from a registered PostgreSQL source is
  rechecked against current lifecycle and collector access before processing.
  Only `approved` public sources or explicitly `permitted` private sources pass.
- The collector writes immutable raw content, source/message identity, dates,
  reconstructable URL, bounded transport metadata, origin and correlation ID to
  PostgreSQL. It atomically creates one `telegram.raw_message.v1` durable job in
  the same transaction. A PostgreSQL failure commits neither record and prevents
  the compatibility handler from running.
- The `(source_id, external_message_id)` identity and matching job idempotency
  key converge live/catch-up races and restart rediscovery on one raw row and one
  job. A worker can claim that job and reconstruct the complete input from
  PostgreSQL without fetching Telegram again.
- Normal structured logs contain identifiers and correlation fields but never
  raw message content. No new queue framework or unbounded history read exists.
- G3-T02 does not yet run a downstream raw-message worker. After successful raw
  persistence the existing keyword/SQLite/delivery handler still runs as a
  compatibility path. The historical SQLite file remains read/write for this V1
  behavior and is neither migrated, deleted nor bulk-mutated.

## G3-T03 high-recall prefilter

- `telegram.raw_message.v1` now has a handler for the existing `DurableWorker`.
  It records one versioned PostgreSQL prefilter result per raw message and
  atomically creates one `opportunity.analysis.v1` job for passed text.
- The V2 prefilter does not import or evaluate the legacy keyword list or
  `MIN_SCORE`. It rejects only whitespace/empty content and Telegram service
  events, with stable `empty_content` or `service_event` reason codes. All other
  text remains eligible for later semantic analysis.
- Reply context is bounded to one exact parent raw message from the same source,
  when `reply_to_msg_id` is present and that parent already exists. Analyzer
  input contains current plus optional direct parent only; it never scans chat
  history or refetches Telegram.
- Prefilter result and downstream job creation share one transaction and stable
  idempotency identity. Retry/restart returns the existing result/job. Workers
  claim only job types for which they have registered handlers, so a raw worker
  leaves future analysis jobs queued.
- The main application does not yet start the durable worker process. Its
  current compatibility branch still uses legacy keywords, SQLite and Telegram
  delivery after raw persistence. That branch does not decide V2 analysis
  eligibility, and G3-T03 does not migrate or mutate historical SQLite data.

## G3-T04 exact deduplication and analysis cache

- Passed V2 messages are normalized with Unicode NFKC, case folding and
  whitespace collapsing. Exact current content and the optional direct parent
  form a versioned analysis-input fingerprint; punctuation and URLs remain
  significant and there is no semantic or full-history comparison.
- A PostgreSQL transaction-scoped advisory lock makes concurrent live/catch-up
  or cross-source observations converge. Within the default seven-day window,
  one canonical prefilter result owns the `opportunity.analysis.v1` job and
  each duplicate has its own raw/source-linked result referencing that
  canonical row. Outside the window, a new canonical job is allowed.
- `opportunity_analysis_cache` is keyed by exact normalized input plus analyzer
  and result-schema versions. Its repository accepts only passed deduplicated
  inputs and rejects conflicting writes for an occupied compatible key.
- G3-T04 does not invoke an AI provider or create opportunities. G4 integrates
  the analyzer and finally verifies cache reuse without a model call.
- The legacy SQLite compatibility path and historical database are unchanged.

## G3-T05 production pipeline runtime

- `python -m freelancer_bot` starts one existing `DurableWorker` wrapper before
  live handler registration and bounded catch-up. It claims only
  `telegram.raw_message.v1`; later analysis jobs remain queued for their
  assigned Gate worker.
- Live events and per-source bounded catch-up both call the same collector
  dispatch. PostgreSQL raw identity, durable enqueue, high-recall prefilter and
  exact dedup therefore have identical downstream semantics, including when
  both paths observe one Telegram message concurrently.
- Persisted raw jobs are processed after process restart without Telegram
  refetch. Handler failure follows bounded durable retry, and shutdown stops new
  claims, drains within configured time, then requeues consistent unfinished
  work. An unexpected raw-worker exit is surfaced to the collector process.
- The persisted correlation ID follows collector dispatch, raw persistence,
  worker claim, prefilter/dedup and completion logs. Integrated canary fixtures
  prove raw content and configured secrets remain absent from message, error,
  cause and stack output.
- SQLite still participates only after successful PostgreSQL dispatch in the
  parallel legacy `keyword filter -> lead reservation -> delivery` branch. This
  preserves current user delivery and its source/message exactly-once guard;
  its keywords never gate or remove work from the V2 PostgreSQL pipeline.
- Opportunity analysis, canonical opportunity creation and V2 delivery remain
  later-Gate work. G3-T05 adds no AI inference or new queue framework.

## G4-T02 durable opportunity classification

- When an opportunity analyzer is configured, the existing collector-owned
  `DurableWorker` also registers `opportunity.analysis.v1`; no second queue or
  worker framework is introduced. Without an OpenAI key, analysis jobs remain
  durable queued while raw persistence, prefilter and legacy delivery continue.
- `OpportunityAnalysisJobProcessor` reconstructs the canonical current message
  and at most one already-persisted direct parent through `AnalyzerInputLoader`.
  It performs no Telegram refetch, source-history scan or per-user model call.
- The strict classifier records `is_opportunity`, market direction, intent
  stage and opportunity type. Seller self-promotion cannot validate as an
  opportunity. Research remains a distinct label; later policy may decide how
  to treat it without changing the classifier output.
- A successful call stores one strict cache envelope containing
  `opportunity_analysis.v1` plus provider, requested/provider-reported model,
  analyzer/prompt/schema versions, attempts and token usage. The compatibility
  key fingerprints provider/model/analyzer/prompt and keeps configured model
  routes in separate namespaces; exact duplicates reuse the canonical cache.
- Provider failures use the existing durable job retry limit. Permanent failure
  stores no partial cache. Structured logs include IDs and enum classifications
  only, never current/parent Telegram content. SQLite remains outside V2 AI.

## Later migration contract

Subsequent Gate work must preserve source/message identity, bounded restart
catch-up, successful duplicate suppression, source failure isolation and the
verified common live/catch-up dispatch. It must replace the remaining legacy
keyword/SQLite/delivery compatibility branch deliberately, without restoring
the V1 global keyword gate in the V2 pipeline.

Later delivery work must intentionally replace the single-recipient metadata and
partial-success behavior with per-recipient idempotency and retry evidence. The
legacy crash window and catch-up gap must remain explicit until their replacement
is verified at the release gates assigned by the project.
