# Operations

## Security and scope

V1 accepts only a Discord webhook secret through
`SIGNALBOT_DISCORD_WEBHOOK_URL`; it does not accept Binance credentials. Never
commit or log the webhook. The scanner reads public market data and emits
alerts. It does not place Spot or Futures orders.

The read-only API exposes `/health/live`, `/health/ready`, and
`/signals/recent`. Keep the host NTP-synchronized. Discord displays UTC and
Asia/Seoul while internal timestamps remain UTC Unix milliseconds.

## Position Guardian shadow alerts and operations report

L60-05 keeps Guardian alerting strictly on the read/observe side. The alert and
operations-report builders are pure projections over the already validated
L60-04 shadow result and the existing reconciliation result. They do not send a
Discord message, mutate the Guardian ledger, or place/amend/cancel an exchange
order. A later delivery layer may transport these sanitized payloads, but it
must not turn alert construction into execution capability.

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

Alert IDs are deterministic hashes of the alert schema, durable managed identity,
stable caller-supplied source reference, event time, and sanitized alert content.
Identical alert-builder inputs therefore produce the same alert ID. The source
reference itself is never rendered. Operations reports likewise hash their
canonical sanitized content to a deterministic `report_id`.

L60-05 does not claim durable exactly-once alert delivery across a restart.
`ReconciliationResult.alerts` is an in-process projection and can differ after an
already-applied snapshot is replayed; some alert facts (for example the current
`protective_order_confirmed` observation) are not yet persisted as immutable
ledger evidence. Restart/fault replay and recovery behavior is deliberately the
next L60-06 task. Do not reconstruct an L60-05 alert after restart by treating a
freshly recomputed `ReconciliationResult` as immutable historical authority.

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

## Discord delivery runbook

The `signals` row and `alert_outbox` intent are atomic. Inspect both
`alert_outbox.status` and the append-only `alerts` attempt history when an alert
appears missing or duplicated.

- `pending`: safe to dispatch; startup drains a bounded batch before scanners
  start and the cancellable background dispatcher continues draining bounded
  batches during operation.
- `sending`: temporarily claimed. On restart it is changed to `uncertain`, not
  replayed.
- `delivered`: Discord returned a message ID after a `wait=true` request.
- `uncertain`: the HTTP outcome may already have created a Discord message.
  Reconcile it against the channel and `event_id`; never bulk-reset these rows
  to `pending` or retry them blindly.
- `dead`: a definitive, non-retryable failure or exhausted 429 retry budget.
- `disabled`: signal persistence was enabled while Discord delivery was off.

Transport failures, HTTP 5xx, and 2xx responses without a message ID become
`uncertain`. Only HTTP 429 is retried automatically, up to `max_attempts`, with a
bounded server-directed delay.

`outbox_max_active_items` is a hard limit over `pending`, `sending`, and
`uncertain`. If it is reached, the service refuses the new signal/outbox pair
before commit. Investigate Discord availability, reconcile all `uncertain`
items, and preserve the database before any manual repair. Raising the limit is
not a substitute for resolving an accumulating delivery failure.

For duplicate alerts, compare the deterministic `event_id`, payload hash, and
Discord message ID. A repeated identical event is an idempotent no-op; the same
event ID with different content is a hard data conflict.

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
