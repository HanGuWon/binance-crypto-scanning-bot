# Operations

## Security and scope

V1 accepts only a Discord webhook secret through
`SIGNALBOT_DISCORD_WEBHOOK_URL`; it does not accept Binance credentials. Never
commit or log the webhook. The scanner reads public market data and emits
alerts. It does not place Spot or Futures orders.

The read-only API exposes `/health/live`, `/health/ready`, and
`/signals/recent`. Keep the host NTP-synchronized. Discord displays UTC and
Asia/Seoul while internal timestamps remain UTC Unix milliseconds.

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

### Readable Discord messages

`alerts.message_style` defaults to `simple`: Korean event title, at most four
fields, explicit candidate/PAPER scope, invalidation or observation-release
condition, UTC/KST event time and event/rule identifiers. Market-watch messages
show situation, trade pressure, and what to re-check. Retained symbols add expiry.
Set `detailed` to restore the existing indicator/evidence layout. The public
formatter helper retains its detailed default for existing callers; both actual
payload producers pass the configured style. Previously queued payloads are sent
unchanged. Original reasons and diagnostic metadata remain in `signals.payload_json`.

The OCI configuration enables `alerts.market_watch.minute_pressure_enabled`.
This is explanatory context using three contiguous, fully closed 1m candles,
with no rows later than the 5m decision and at most 60 seconds of lag. Other
series, unfinished and future candles are excluded; duplicates, gaps, invalid
volumes, zero-volume bars and insufficient/stale history produce unavailable
context. It uses quote-volume-weighted buyer-initiated share, last-minute share,
and price change over the same three-minute window. Ratios >=55%/<=45% label
buyer/seller initiative; price change >+0.05%/<-0.05% labels movement, with both
boundaries flat. These are unvalidated descriptive seeds, not probabilities.

The message shows the actual KST window and labels a one-minute lag. Missing
minute context falls back to explicitly labelled 5m flow if available; it never
implies neutral flow. Disagreement between trade initiative and price movement
is an observation, not proof of absorption or reversal. There is no L2 order-book
analysis in this addition. It changes no event IDs, ranking/retention logic,
entry/exit rule, eligibility, PAPER behavior or delivery frequency. A valid
historical minute snapshot is shared by coincident market-watch events.

### Focused Binance mover alerts

`alerts.delivery_mode: focused` keeps every signal in SQLite but limits Discord
delivery to confirmed directional entries, confirmed technical exits, and the
focused market-watch events below. Informational WATCH/SETUP/INVALIDATED state
changes plus intrabar `PUMP_RISK`/`CRASH_RISK` remain queryable without filling
the operator channel.

`alerts.market_watch` ranks the configured single market by Binance's 24-hour
`priceChangePercent`. Only positive movers whose 24-hour `quoteVolume` is
strictly greater than `min_quote_volume_usdt` are eligible. The OCI focused
profile uses Futures, top 5, and `100000000` USDT, so exactly 100M is excluded.
It has no listing-age cutoff; a newly listed contract is evaluated only after
enough fully closed candles are available for the technical indicators.
Priority gainers are forced into the detailed top-N universe and a newly added
priority symbol bypasses the ordinary universe-rotation confirmation delay.

Market-watch technical analysis is evaluated only on the fully closed primary
candle. Bollinger levels are the standard 20-close mean plus/minus two
population standard deviations. An upper touch uses the closed candle high; a
lower touch uses its low. Trend is labelled rising, falling, or mixed/sideways
from EMA20/EMA50 alignment, normalized EMA20 slope, MACD histogram, RSI14, and
ADX14. One continuous event episode emits once and uses the configured cooldown
to suppress rapid leave/re-enter flapping. The runtime also checks the last
delivered market/symbol/family event in SQLite before creating a new Discord
intent, so a service restart cannot reset the 30-minute delivery cooldown.

The OCI profile additionally enables `retention_enabled`: a top-5 coin observed
with quote turnover **>=1 billion USDT** qualifies for retained monitoring until
48 hours after its last top-5 observation, even if turnover subsequently falls.
The rolling 24h turnover is an observed snapshot, not cumulative trading volume.
At most 10 alumni are kept (newest top-5 observation, then symbol); cap eviction
ends qualification. Qualification, expiry and event episodes survive restart in
SQLite. Delisted/excluded contracts and expired qualifications are pruned.
Retained symbols receive candle-only subscriptions independently of the tradable
20-symbol R2/PAPER panel. Expiry still applies if REST refreshes fail. Below or
equal to 100M current turnover, monitoring continues but new watch alerts stop.

`context_alerts_enabled` adds rising/falling trend observations (existing trend
votes plus ADX>=25) and informational range LONG/SHORT candidates. Range rules
require 20 contiguous prior closed candles, repeated tests of both edges,
ADX<20, ER20<=0.30, EMA20 slope <=0.25 ATR in absolute value and width>=2 ATR.
The current candle must reject an edge, close inside the corresponding 20-35%
zone, have the matching candle body and closed-kline aggressive buy share
>=55% for LONG or <=45% for SHORT. Candles touching both edge zones are excluded.
The first target is the range midpoint; distance must cover twice the configured
estimated round-trip cost (26bps by default). Invalidation is lower edge-0.25ATR
or upper edge+0.25ATR. These are **unvalidated observation candidates**, not
confirmed entries, probabilities or production orders. Aggressive trade share
does not measure order-book resting liquidity.

Ranking snapshots never apply to earlier candle closes. Up to four snapshots
are retained; older delayed candles without matching evidence are skipped.
True event episodes and their signal/outbox intent commit atomically, including
pending intents; false observations re-arm without sending a message, preserve
cooldowns and cannot rewind newer state. Departing symbols are also re-armed.
The episode is preserved across rule-version updates. Only one new market-watch
message per symbol/closed candle is delivered, preferring range, band, trend,
then ranking. Every message carries trend, band and aggressive-flow context;
other coincident event decisions remain stored with disabled delivery intent.
Incomplete exchange/ticker snapshots cannot erase qualification. The current
snapshot's volume floor also acts as an operational notification gate for
delayed candles, while their evidence remains from the causal older snapshot.
Membership exit stamps a time barrier once; repeated refreshes do not keep
expired episode records alive. First-upgrade cooldown recovery is limited to
recent delivery intents; old historical messages cannot block re-entry.

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
## Bollinger contacts and moving averages

Market-watch observations now include `ma_context` (`bb-ma-context-v1`). On the
same fully closed primary candle, EMA20 above EMA50 with positive EMA20 slope
is an upward array; the opposite is downward. Equality, zero slope or conflicting
array/slope is mixed. This is separate from the existing composite trend label.
The 20/50 periods count candles: on 5m charts they represent 100/250 minutes.
The EMA20 slope compares the current value with three closed candles earlier.

Band contact plus this array and the close's position distinguishes continuation,
pullback/rebound and weakening observations. Both-band contact takes precedence
and defers direction judgment. Price equality is preserved as equality. The
original Bollinger center stays SMA20; it is not replaced by EMA20. These related
price indicators do not add independent confidence points or claim a newly
formed golden/death cross, a confirmed reversal, or validated profitability.

Simple messages keep three fields (four for retained watches). The market field
adds one array line; the conditions field adds the band interpretation and the
MA price to check on subsequent closed candles. Existing episode reset and
candidate invalidation conditions remain distinct. Detailed messages include
price relations to EMA20, EMA50 and SMA20. Missing/invalid/older context preserves
the previous message. Signal families, IDs, delivery counts and entry rules stay
unchanged. MA analysis is lagging descriptive context, not order execution.
