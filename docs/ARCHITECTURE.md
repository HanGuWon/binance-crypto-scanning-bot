# Architecture

Each configured Binance market runs as an independent scanner while sharing
persistence, notification, and shutdown services.

## Live data and decision path

1. Public REST discovers instruments and separates three roles: a bounded
   surveillance universe for broad anomaly detection, a smaller tradable
   universe ranked by 24-hour quote volume, and an independent BTCUSDT context
   universe for benchmark candles and market regime. Context symbols receive
   candle data but never become recommendation candidates, BBO/order-flow
   owners, funding owners, or PAPER positions unless they are also tradable.
2. Public REST bootstraps closed candles for every configured timeframe. Gap
   recovery replays missing closed candles sequentially before evaluation.
3. WebSocket plans use the routed endpoints: Spot uses its combined endpoint;
   USD-M Futures klines, aggregate trades, and all-market mini tickers use
   `/market`; book tickers use `/public`.
4. Bounded candle, trade, book, funding, feature, anomaly, regime, and state
   stores accept only events for the active universe and prune stale symbols.
5. Features are calculated only after closed-kline events. The 5-minute interval
   is the default decision clock. Higher-timeframe context lookup uses the most
   recent mature snapshot whose event time is strictly earlier than the primary
   decision.
6. The R2 rule path combines the closed-candle C0 trigger, strict-prior 15-minute
   and 1-hour trend alignment, data completeness, and a fresh observed BBO. BBO
   freshness is based on local receipt time; a repeated Binance source cursor
   with different prices or quantities is a hard conflict, while an exact
   duplicate does not refresh freshness.
7. Rapid all-market moves are evaluated independently as intrabar
   `PUMP_RISK`/`CRASH_RISK` warnings.
8. Rule evaluations enter a bounded state machine that emits state changes,
   deterministic event IDs, reasons, invalidation, rule version, and cooldown
   behavior.

No component in this path calls an order endpoint.

Live Discord titles translate the existing final decision state into a direct
Korean recommendation (`상승 예상`, `하락 예상`, or `진입 보류`). The displayed
0–100 value remains rule-evidence strength, not a calibrated probability, and
only `CONFIRMED` directional decisions are rendered as entry candidates.

## Signal persistence and Discord outbox

A signal row and its immutable Discord payload intent are committed in one
database transaction. Replaying the same event ID and byte-equivalent payload
is a no-op. Reusing an event ID for different signal or alert content raises a
hard conflict.

The durable outbox state flow is:

`pending -> sending -> delivered | uncertain | dead`

`disabled` is written when Discord delivery is disabled. A worker atomically
claims one `pending` row, increments its attempt counter, and calls the webhook
with `wait=true`. A 2xx response counts as delivered only when Discord returns a
message ID. Rate limiting (HTTP 429) is the only automatically retried response,
and retry count and delay are bounded. Transport errors, 5xx responses, or a 2xx
response without a message ID have ambiguous delivery outcome and are
quarantined as `uncertain`; blindly retrying them could duplicate a Discord
message. Other non-retryable 4xx responses become `dead`.

At startup, any row left in `sending` by a process interruption becomes
`uncertain`. A bounded batch of unambiguous `pending` rows is dispatched before
market scanners start, then a cancellable background loop keeps draining
bounded batches while the service runs. The active outbox limit counts
`pending`, `sending`, and `uncertain` rows. Reaching
`outbox_max_active_items` raises before either the new signal or its outbox
intent is committed, preserving their atomic relationship.

## Evidence recording and capacity

Optional raw-event recording writes public payloads as daily, per-market JSONL
with a receipt timestamp. Spot and Futures scanners share one recorder, lock,
and byte counter. Before an append, the recorder accounts for the configured
raw-event directory and refuses a write that would exceed
`raw_event_max_bytes`. Capacity exhaustion logs a critical error, sets the
shared stop event, and does not forward that payload into the decision runtime.
This is a fail-closed evidence boundary, not silent truncation or automatic
rotation.

All in-memory queues, histories, maps, and caches are bounded or pruned. Exchange
boundary prices use `Decimal`; numerical indicators use floats; timestamps are
UTC Unix milliseconds. No future row, centered window, unclosed candle, or
unclosed higher-timeframe value is admitted to a decision.

## Retrospective analysis boundary

The frozen R2 research runner is a separate, order-free path. It aligns C0 and
H1 on immutable opportunity IDs, isolates declared chronological splits, enters
at the next 5-minute open, applies frozen F60 and T72 outcomes, and records cost
components explicitly. The analyzer verifies A/B output identity plus shared
code, settings, experiment-plan, input-data, and `uv.lock` provenance before
analysis. It uses circular UTC-day block bootstrap sensitivity at 7, 14, and 28
days and Holm correction across the four pre-registered entry/exit by
Spot/Futures composites.

This path has no historical decision-time BBO, depth, or receipt timestamps, so
it cannot validate the full live R2 execution gate. That limitation is encoded
in the result rather than filled with a proxy.

## Prospective R4b evidence boundary

The R4b recorder is a separate foreground, public-data-only process. It shares
neither scanner decisions nor order behavior. Three routed WebSocket owners and
one bounded REST scheduler persist raw combined frames, connection transitions,
and every snapshot attempt in one process-local ingest order. Depth range
callbacks run only after raw offer, when the frame iterator resumes for the next
item; stop, recycle, or cancellation immediately after the final offer may omit
that generation-local operational callback while leaving the raw frame stored.
Offline raw replay remains authoritative. REST `response_completed_*` is a
conservative local admission receipt sampled after bounded close handling and
immediately before sequence allocation and offer, not an exchange or exact
last-byte timestamp. The scheduler maintains six fixed,
generation-bound bootstrap states and applies venue-specific rules
independently. Spot uses the frozen DESIGN `lastUpdateId + 1` reconciliation
after discarding `u <= lastUpdateId`; USD-M Futures bridges on
`lastUpdateId` with `pu` continuity. A gap, stale generation, overflow, or
exhausted bounded snapshot cycle fails closed.

Spot snapshot adapter admissions are globally serialized at a frozen
3.2-second monotonic cadence after semaphore and rate-gate admission; the lock
remains held through the attempt and the next cadence starts only after the
attempt terminates, so adapter-internal waits or a queued backlog cannot burst.
Successful Spot REST responses must supply one exact one-minute used-weight header.
High-water, rate-limit-contract drift, a body cap, 418, or an invalid/missing
429 `Retry-After` quarantines the capture after its evidence record is
persisted. A 429 with a valid bounded `Retry-After` imposes a process-local
embargo and bounded retry instead; any persisted 429 still makes the closed
capacity report fail. This process-local guard does not claim control over
other processes sharing the public IP, so a high-water observation remains a
session validity failure.

Persisted evidence remains authoritative. An offline six-book materializer
rechecks connection generations, snapshot request-time barriers, `U/u` and
Futures `pu`, exact decimal updates, all memory bounds, an initial
`ingest_seq=1`, and contiguous replay. Reconstruction invalidation remains
unresolved until a successful bridge and independently blocks a completed pass.
A common closed-evidence reader verifies canonical session and external-audit
documents, the exact canonical source-manifest binding, the segment/manifest
hash chain beginning at ingest one, and each typed record before and after
streaming. This does not independently attest the report runtime against that
capture-time manifest or pin one data descriptor throughout consumption. The
legacy capacity/schema report remains payload-blind. A separate
depth reconstruction report distinguishes sequence-valid coverage from fresh
two-sided-uncrossed coverage. Fresh means no more than exactly 2,000 ms since
the latest locally received applied snapshot/depth evidence; it is not Binance
event time, BBO quote age, or fill evidence. Both dimensions must reach 995,000
ppm for the 24-hour infrastructure gate, while 999,000 ppm remains only a later
prospective diagnostic. A third clock-health report interprets only the two
public venue-time roles, keeps Spot and Futures separate, and derives
request-start-to-header clock intervals that become available at response
completion. It audits 2,000 ms RTT, 60-second causal age, ±1,000 ppm venue
continuity, 2/100 ms wall-versus-monotonic boundaries, and 999,000 ppm per-venue
coverage. Its pure cutoff mapper uses only a completed prior sample and exposes
`ADMISSIBLE`, `LATE`, or `CLOCK_INCONCLUSIVE`; it is not wired into alert or
PAPER execution. None of the three reports authorizes Family B, efficacy,
PAPER profitability, promotion, or live orders, and an infrastructure pass
does not establish positive expectancy.

## Failure behavior

WebSockets reconnect with capped exponential backoff and jitter, Futures
connections recycle before 24 hours, missing candles are recovered before
evaluation, malformed payloads are rejected, and shutdown is cancellable.
Discord delivery does not block ingestion indefinitely, but an outbox hard-limit
or raw-evidence hard-limit is deliberately fail-closed and requires operator
attention.

The market-ingestion coroutine stops at that durable transaction and never
awaits Discord HTTP. A separate outbox worker owns provider I/O, so webhook
latency, rate limiting, and ambiguous transport outcomes cannot block Binance
WebSocket processing. The worker sends only already-persisted immutable alert
intents.
