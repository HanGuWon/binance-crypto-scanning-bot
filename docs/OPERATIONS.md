# Operations

## Security and scope

V1 accepts only a Discord webhook secret through
`SIGNALBOT_DISCORD_WEBHOOK_URL`; it does not accept Binance credentials. Never
commit or log the webhook. The scanner reads public market data and emits
alerts. It does not place Spot or Futures orders.

The read-only API exposes `/health/live`, `/health/ready`, `/outbox/summary`,
`/signals/recent` and the protection-context endpoints.

Logging redacts Discord webhook URLs: the `httpx`/`httpcore` loggers are held at
WARNING and a filter on the root handler rewrites `/webhooks/<id>/<token>` to
`/webhooks/<id>/<redacted>` in messages, arguments and exception text. A token
that appears anywhere in a log line is a bug; report it. Keep the host NTP-synchronized. Discord displays UTC and
Asia/Seoul while internal timestamps remain UTC Unix milliseconds.

## Position Guardian shadow alerts, durable authority, and restart recovery

L60-06 keeps Guardian alerting strictly on the read/observe side while adding a
restart-authoritative persistence boundary. Alert construction still never
places, amends, cancels, or closes an exchange order. The existing operations
report remains a pure projection; durable alert materialization is owned by the
Guardian ledger/outbox path instead.

The frozen L60-05 alert set is:

- `WOULD_UPDATE_STOP`: a validated shadow intent would tighten the protective
  stop; the message explicitly states that no exchange order was sent.
- `STALE_CONTEXT`: public protection context is stale, so stop planning remains
  blocked.
- `MANUAL_SIZE_INCREASE`: private reconciliation observed a manual position add;
  Guardian does not expand protection automatically.
- `SIDE_FLIP`: the managed side changed and the old adoption generation requires
  release/re-approval.
- `PROTECTION_MISSING`: the adopted position has no confirmed protective order.
- `RECONCILIATION_UNCERTAIN`: private reconciliation or the persisted protection
  context cursor is uncertain, so operator review is required before protection
  changes.

For new L60-06 observations, immutable Guardian ledger evidence is the alert
authority. Reconciliation `ACCOUNT_SNAPSHOT` events seal the pre-update managed
quantity/state plus the observed protection-confirmation and uncertainty state.
The adoption event also binds the Binance private position `updateTime` and
seeds a monotonic private-snapshot cursor before the identity becomes managed.
An upgraded legacy projection with no such cursor fails closed instead of
accepting an unfenced first private snapshot.
Shadow stop intents carry an explicit alert-source schema marker, while stale
context and uncertain-context rejections receive their own immutable
`SHADOW_ALERT_SOURCE` event. A stale or conflicting private position snapshot is
rejected before projection mutation and produces durable
`RECONCILIATION_UNCERTAIN` evidence instead of rolling the managed state backward.
A terminal side flip or full close seals the RELEASE event ID/reason into the
snapshot evidence and commits the snapshot plus RELEASE transition atomically.
The RELEASE carries the snapshot event as its causal parent, and ledger listing
orders that parent before its same-time terminal child. Full closes persist
`CLOSED`; side flips persist `RELEASED`.

Alert IDs are rebuilt only from the persisted source event ID/time and sanitized
content. Restart recovery therefore never treats a freshly recomputed
`ReconciliationResult` as historical authority. It first quarantines any
`sending` Guardian alert as `uncertain`, then scans immutable source events and
idempotently restores any missing outbox intent. The outbox ordering timestamp is
the original source-event persistence time, never the restart clock.

L60-06 intentionally creates Guardian outbox rows as `disabled`: no Guardian
Discord transport owner exists in this phase, so there is no automatic send or
retry path to activate accidentally. The pending/sending/uncertain/delivered/dead
state machinery is present for fault-model tests and the later transport phase,
but moving L60-06 rows into a delivery-capable path requires a new explicit
contract; the L60-06 repository API cannot transition a `disabled` row to
`pending`. Pre-L60-06 in-process alerts are not backfilled because their complete
alert-driving evidence was not durably preserved.

This is restart-authoritative alert-intent recovery, not a claim of exactly-once
Discord delivery. A future Guardian process owner must run recovery before it
may drain any pending transport queue. `sending` is never reset to `pending` after
restart, and `uncertain` is never retried blindly.

The rendered alert/report surface intentionally omits `account_alias`, canonical
`position_ref`, API key/secret, database URL, wallet/account balances, and any
free-form unknown reason text. Position quantity is allowed because it is the
observed managed-position size, not an account balance. Unknown reconciliation
alert strings are collapsed to `UNKNOWN_RECONCILIATION_ALERT`, and unknown shadow
reason text becomes `UNSPECIFIED` rather than being echoed to an operator channel.

Treat any non-zero `exchange_write_calls` or an already-placed shadow intent as a
contract violation: alert/report construction fails closed. In shadow mode the
expected report-level `exchange_write_calls` is always `0`.

## Preflight and rollout

Validate the effective configuration before every rollout:

```bash
uv run signalbot validate-config --config config/settings.yaml
uv run signalbot run --config config/settings.yaml --dry-run
```

For the prospective R2 candidate, verify that the effective configuration keeps
`entry_policy: r2_pit_htf_exec`, `confirmation_mode: explicit_trigger`, and a
new `rule_version` for every rule-contract change. The example requires a fresh
observed BBO no older than 2 seconds, spread no wider than 15 bps, and at least
100 USDT of side-appropriate top-of-book quote capacity. A proxy or missing BBO
must fail the candidate.

Roll out as an observation service: run tests and replay, operate with Discord
disabled, enable a private channel, then collect prospective decision-time BBO
evidence across several regimes. A retrospective C0/H1 pass is not approval for
live execution or order placement.

If PAPER technical exits are enabled, treat them only as alert lifecycle
diagnostics. Their per-symbol pending/open state is bounded but memory-only and
is intentionally not restored from the database after restart. A restart can
therefore make a previously alerted PAPER position disappear from lifecycle
tracking; it never closes or changes an exchange position. A primary-candle
gap fail-closes tracked PAPER state at the first post-gap open and records the
modeled fill separately from the closed-candle alert observation time.

## PostgreSQL schema: `rule_version` width

`signals.rule_version` and `shadow_campaigns.rule_version` are `VARCHAR(64)`.
Frozen campaign rule versions (for example the 35-character
`v4.3.0-causal-structure-diagnostics`) do not fit the former `VARCHAR(32)`, and
PostgreSQL enforces the length (SQLite does not). Rule version strings are
campaign identities and are never shortened.

`create_all` does not alter existing tables. A database created by an earlier
build must be altered once, before starting the service:

```sql
ALTER TABLE signals ALTER COLUMN rule_version TYPE VARCHAR(64);
ALTER TABLE shadow_campaigns ALTER COLUMN rule_version TYPE VARCHAR(64);
```

On startup the repository inspects these columns on PostgreSQL only. If a column
is narrower than 64 it raises `SchemaMigrationRequiredError` naming the exact
statements above and the service does not start. Nothing is migrated
automatically. `SignalDecision.rule_version` is validated to 1-64 characters.

## Discord delivery runbook

The `signals` row and `alert_outbox` intent are atomic. Inspect both
`alert_outbox.status` and the append-only `alerts` attempt history when an alert
appears missing or duplicated.

- `pending`: safe to dispatch. The supervised, cancellable background dispatcher
  (`discord-outbox-drain`) drains bounded batches, including the startup backlog;
  scanner startup no longer waits on Discord. Only `recover_inflight()` runs
  synchronously before scanners.
- `sending`: temporarily claimed. On restart it is changed to `uncertain`, not
  replayed.
- `delivered`: Discord returned a message ID after a `wait=true` request.
- `uncertain`: the HTTP outcome may already have created a Discord message.
  Reconcile it against the channel and `event_id`; never bulk-reset these rows
  to `pending` or retry them blindly.
- `dead`: a definitive, non-retryable failure (for example an HTTP 4xx other
  than 429). HTTP 429 never produces `dead`.
- `expired`: terminal. The alert was older than the delivery limit when it was
  about to be sent, so it was never sent. It does not count toward
  `outbox_max_active_items` and an `expired` audit row is appended to `alerts`.
- `disabled`: signal persistence was enabled while Discord delivery was off.

Transport failures, HTTP 5xx, and 2xx responses without a message ID become
`uncertain`. HTTP 429 means Discord did not process the message: it is retried
up to `max_attempts` with the server-directed delay (capped at 30 s). After that
the item stays `pending` and the notifier pauses all deliveries for the
server-directed `retry_after` (capped at 300 s, in memory only, cancelled by
shutdown). Nothing waits forever: expiry still applies.

Delivery age limits (`alerts.max_delivery_delay_seconds`, default 900, minimum
60; `alerts.risk_max_delivery_delay_seconds`, default 180, minimum 30, applied
to `PUMP_RISK`/`CRASH_RISK` and never above the general limit) are measured as
`now - signals.event_time_ms`; the class is read from the stored signal row. An
item exactly at the limit is still delivered; one millisecond older is expired.
Both settings are excluded from `Settings.model_dump()` so frozen settings
hashes do not change.

Delivery guarantee: at most once per `event_id`. `uncertain` remains ambiguous
because the Discord Execute Webhook API has no idempotency key; a message may
or may not exist. Resolve each `uncertain` item
after checking the channel with `signalbot outbox resolve` (see "Pipeline readiness
and outbox operations").

`outbox_max_active_items` is a hard limit over `pending`, `sending`, and
`uncertain`. If it is reached, the service refuses the new signal/outbox pair
before commit. Investigate Discord availability, reconcile all `uncertain`
items, and preserve the database before any manual repair. Raising the limit is
not a substitute for resolving an accumulating delivery failure.

For duplicate alerts, compare the deterministic `event_id`, payload hash, and
Discord message ID. A repeated identical event is an idempotent no-op; the same
event ID with different content is a hard data conflict.

## Pipeline readiness and outbox operations

The live process writes liveness evidence to the `runtime_heartbeats` table (one
row per market: `last_ws_message_ms`, `last_closed_candle_ms`,
`last_decision_ms`, `last_outbox_drain_ms`, `max_loop_lag_ms`, `updated_at_ms`).
Writes are throttled to at most one per market per 15 s plus one per outbox
drain cycle, and a failed write is logged at ERROR without blocking ingestion.
`create_all` adds the table to existing databases; no manual migration is needed.
`max_loop_lag_ms` is reserved for the event-loop lag monitor and stays empty
until that monitor is enabled.

`signalbot serve-api` now reports pipeline readiness:

- `GET /health/live` is unchanged (`{"status":"alive"}`).
- `GET /health/ready` returns 200 only if every configured market has a WebSocket
  heartbeat no older than `runtime.ready_max_staleness_seconds` (default 120,
  minimum 15). Otherwise it returns 503 with a JSON `reasons` list. A heartbeat
  exactly at the limit is still ready. The setting is excluded from
  `Settings.model_dump()` so frozen settings hashes do not change.
- `GET /outbox/summary` returns counts by status, the age of the oldest
  `pending` item and the `uncertain` count. It never returns payloads, URLs or
  detail text.

Operator commands (no network access; they only touch the database):

```bash
signalbot outbox status --config config/settings.yaml
signalbot outbox resolve --config config/settings.yaml --event-id EVENT_ID \
    --as delivered --reason "found in channel" --message-id MESSAGE_ID
```

`resolve` accepts only `uncertain` rows (guarded update), requires `--reason`,
sets the row to `delivered` or `dead`, and appends a `resolved_delivered` or
`resolved_dead` audit row to `alerts` without overwriting earlier attempts.
Reconcile each `uncertain` item against the Discord channel before resolving it.

## PAPER tracking-reset notices

The PAPER technical-exit lifecycle is in memory only, so a restart silently
drops exit tracking for entries that were still open. When
`signals.technical_exit.enabled` is set, startup persists exactly one
notice-only alert per such entry: a persisted CONFIRMED, non-informational,
non-risk entry on the primary interval whose `event_time` is within
`max_holding_bars x primary interval` and that has no `TECHNICAL_EXIT` row
referencing it (`metadata.entry_event_id`). The notice reads
`PAPER 추적 중단 — 이 진입의 청산 알림은 더 이상 오지 않습니다`, is not an exit or a
recommendation, and never becomes a PAPER position.

- Event ID: `sha256(market|symbol|technical_exit|entry_event_id|tracking_reset|rule_version)[:24]`,
  so repeated restarts never emit a second notice for the same entry.
- The scan is bounded (500 entries per market per startup). If more entries are
  open, the remainder is not covered.
- Make-before-break WebSocket rotation (avoiding self-inflicted `DATA_GAP`
  exits) is not implemented; it is a possible follow-up.

## Alert presentation notes

Embeds carry a fixed `검증 상태` field taken from `alerts.validation_notice`
(default: `회고 검증 FAIL(R2) · prospective 검증 전 — 기대수익·확률 아님`). The
setting is excluded from `Settings.model_dump()`. The embed footer records a
presentation version (`view vN`); see `docs/ARCHITECTURE.md` for how a
presentation-only difference is treated when an event ID is persisted again.

## Failure handling and supervision

- WebSocket consumers reconnect with bounded backoff only on transport failures. An
  exception raised by the message handler is a pipeline failure and is not a
  reconnect: outbox capacity, event-ID or candle conflicts, database errors and the
  PAPER lifecycle symbol bound stop that scanner, log CRITICAL, and stop the whole
  application (fail closed, operator attention required).
- The Discord outbox drain (`discord-outbox-drain`) catches errors per batch, logs
  them at ERROR with the traceback, and retries with exponential backoff from 1 s up
  to 60 s (interrupted by shutdown). An item that was `sending` when an error hit is
  not retried by the loop; it becomes `uncertain` at the next restart. If the drain
  task ends without a stop request, or the event-loop lag monitor does, the
  application logs CRITICAL and stops. Any exception collected during teardown is
  logged.
- `binance.bootstrap_close_margin_ms` (default 2000, 0-60000) drops REST bootstrap
  candles that close within the margin of local now, so a local clock slightly ahead
  of Binance cannot admit a still-open candle (which would later conflict with the
  closed WebSocket candle). Gap recovery is unchanged. The effective value is logged
  once per market at startup and, like every setting below, is excluded from
  `Settings.model_dump()` so frozen settings hashes do not change.

## Diagnostics: event-loop lag and slow handlers

Feature computation runs on the event loop, so a candle-boundary burst can delay
WebSocket processing. Two diagnostics make that visible; neither changes any gate
or timestamp.

- `runtime.loop_lag_warning_ms` (default 500): a periodic task measures how late it
  resumes. A lag above the threshold logs one WARNING per 60 s with the maximum seen
  since the last warning; the rolling maximum is written to
  `runtime_heartbeats.max_loop_lag_ms` (existing 15 s write throttle).
- `runtime.handler_slow_warning_ms` (default 1000): one WARNING per stream per 60 s
  when a message spends longer than the threshold in the handler, or when more than
  the threshold passes between pulling the frame from the connection and the handler
  starting (the library's internal receive queue is not visible to this measurement).

Both effective values are logged once at startup. Intervals shorter than
`binance.primary_interval` are no longer featurized (their candles are still stored
and persisted); BTCUSDT 1h is kept because it feeds the regime. Optimizing the
feature computation itself is not done: it lives in sources frozen by the Guardian
policy contract.

## Retention: `prune-candles`

```bash
signalbot prune-candles --config config/settings.yaml --older-than-days 30          # dry-run
signalbot prune-candles --config config/settings.yaml --older-than-days 30 --apply  # delete
```

Dry-run is the default and prints counts per market/interval. `--apply` deletes in
batches of 5,000 rows, one transaction per batch, and reports totals. It touches
only the `candles` table (never signals, alerts, the outbox, heartbeats or shadow
tables) and refuses `--older-than-days` below 1. A candle closing exactly at the
cutoff is kept. Do not prune candles that an active retest/shadow campaign still
needs (`runtime.persist_candles` feeds causal retest observation); preserve the
database before the first `--apply`.

## Configuration restrictions and startup warnings

- `shadow.directional_observation_enabled` cannot be combined with
  `alerts.discord_enabled`: directional observation is shadow-only. Setting the
  `SIGNALBOT_DISCORD_WEBHOOK_URL` environment variable enables Discord
  automatically, so unset it when running directional observation. The directional
  deployment template validates when that variable is absent.
- If pullback alerts are on (`signals.pullback_alert_mode` other than `off`) and
  `signals.pullback_intervals` lists intervals other than `binance.primary_interval`,
  a WARNING is logged once at startup: rules run only on the primary-interval candle,
  so those entries are never evaluated live. The configuration is not rejected.
- Open product decision: `RequiredUniverseUnavailableError` currently stops both
  markets when a required directional symbol is unavailable. That behavior is
  unchanged and awaits a decision.

## Continuous integration

The `test` job runs ruff, pyright, the full pytest suite, compileall and the CLI
checks. A separate `postgres` job starts a `postgres:17` service and runs only
`tests/integration/test_postgres_smoke.py` with `SIGNALBOT_TEST_POSTGRES_URL` set:
it initializes the schema, stores a signal whose `rule_version` is exactly 64
characters and checks the startup width check. The test is skipped when the
variable is unset, so local runs are unaffected.

## Raw-event evidence capacity

Raw capture is opt-in:

```yaml
runtime:
  record_raw_events: true
  raw_event_directory: ./var/raw-events
  raw_event_max_bytes: 10737418240
```

Size `raw_event_max_bytes` for the entire prospective capture window and monitor
the directory on the same filesystem. The recorder includes existing files in
its initial accounting. When the next JSONL record would exceed the configured
quota, it logs a critical error and stops the shared scanner instead of dropping
evidence silently. After a capacity stop, preserve or archive the evidence under
an explicit retention policy, reclaim space outside the configured directory,
and restart so capacity is re-accounted. Do not remove evidence while a study is
running.

## Frozen R2 retrospective procedure

Do not change
`artifacts/backtest/2026-07-16-r2/experiment_plan.md` or
`artifacts/backtest/2026-07-16-r2/feature_contract.md` after observing results.
Run each frozen variant twice into distinct directories:

```bash
uv run signalbot backtest-run --config config/settings.example.yaml --spec config/backtest.5m.r2-c0-corrected.yaml --data-dir data/backtest --output-dir artifacts/backtest/2026-07-16-r2/c0-a
uv run signalbot backtest-run --config config/settings.example.yaml --spec config/backtest.5m.r2-c0-corrected.yaml --data-dir data/backtest --output-dir artifacts/backtest/2026-07-16-r2/c0-b
uv run signalbot backtest-run --config config/settings.example.yaml --spec config/backtest.5m.r2-h1-strict-htf.yaml --data-dir data/backtest --output-dir artifacts/backtest/2026-07-16-r2/h1-a
uv run signalbot backtest-run --config config/settings.example.yaml --spec config/backtest.5m.r2-h1-strict-htf.yaml --data-dir data/backtest --output-dir artifacts/backtest/2026-07-16-r2/h1-b

uv run signalbot backtest-r2-analyze --c0-a-dir artifacts/backtest/2026-07-16-r2/c0-a --c0-b-dir artifacts/backtest/2026-07-16-r2/c0-b --h1-a-dir artifacts/backtest/2026-07-16-r2/h1-a --h1-b-dir artifacts/backtest/2026-07-16-r2/h1-b --samples 50000 --seed 20260716 --output artifacts/backtest/2026-07-16-r2/r2_analysis.json
```

The analyzer fails provenance validation if A/B outputs differ or the shared
code, effective settings, frozen plan, input manifests, or `uv.lock` identity
does not match. Do not compare hand-edited CSV files.

Interpret statuses conservatively:

- `INVALID`: integrity or contract failure; do not interpret partial metrics.
- `INCONCLUSIVE`: the frozen information thresholds were not met.
- `FAIL`: a sufficiently informed pre-registered efficacy test failed.
- `RETROSPECTIVE_SCREEN_PASS`: the historical C0/H1 diagnostic passed its
  frozen conditions; this is not deployment approval.

The full prospective candidate remains
`INCONCLUSIVE_NO_HISTORICAL_BBO` for every retrospective result because kline
history cannot test decision-time BBO freshness, spread, quantity/depth, or
receipt time.

## General incident checks

For missing candles, inspect gap-recovery logs and require recovery before a
new evaluation. For unexpected Futures silence, verify `/market` and `/public`
routes rather than legacy unrouted URLs. For unexpected candidate silence,
inspect gate reasons first: a missing strict-prior context or BBO is an expected
fail-closed rejection, not evidence that the score should be lowered.
