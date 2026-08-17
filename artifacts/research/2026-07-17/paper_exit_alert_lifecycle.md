# PAPER technical-exit alert lifecycle

## Scope

The bot now has an optional, alert-only PAPER lifecycle for technical exits.
It does not persist a position or order, call an authenticated/private Binance
endpoint, or place a Spot or Futures order. The only durable records are the
existing immutable signal decision and Discord outbox intent.

## Causal timing

- Only a newly persisted `CONFIRMED` technical entry with a valid structural
  stop is eligible. Risk warnings and technical-exit decisions cannot recurse.
- An entry confirmed on a fully closed primary candle is modeled at the next
  primary candle open.
- Initial and already-active trailing stops are known before a bar. A gap
  through the stop fills at the bar open; a within-bar touch fills at the stop
  and is reported only after the candle closes.
- A trailing stop calculated after a close cannot act on that same candle.
- Opposite confirmation, the configured consecutive trend-failure count, and
  the maximum holding-bar boundary schedule an exit at the next candle open.
- Each exit records the modeled fill time separately from the fully closed
  candle time at which the alert could actually be observed.
- Regime evidence has an explicit source and as-of time. Open fills use the
  strict-prior primary close, intrabar observations use the current closed
  primary candle, and gap exits use the last available pre-gap primary close.

## State, gaps, and idempotency

There is at most one pending or open PAPER position per symbol and no more
symbol states than the configured tradable-universe bound. Universe rotation
prunes removed symbols. A primary-candle gap cancels a pending entry and
fail-closes an open PAPER position at the first post-gap open. Replayed candles
are ignored, while deterministic event IDs and the existing atomic
signal/outbox repository make replayed exit alerts idempotent.

One bounded per-symbol checkpoint protects the memory-only transition until an
exit signal/outbox intent is durably stored. A persistence failure restores the
prior PAPER state. Once persistence succeeds (or an identical exit is already
durable), the transition is committed before the notification handler runs, so
a downstream handler failure cannot resurrect an already-exited position.

The lifecycle is deliberately memory-only. A process restart does not rebuild
pending entries or open PAPER positions from signal history, so a previously
alerted PAPER position can disappear from tracking after restart. This is an
explicit V1 limitation, not an exchange action or a claim that any real
position was closed.

## Action labels

- Spot tracked-long exit: `SPOT_EXIT`
- Futures tracked-long exit: `FUTURES_LONG_EXIT`
- Futures tracked-short exit: `FUTURES_SHORT_EXIT`

All three use the `TECHNICAL_EXIT` family and carry the reason, active
invalidation/stop, rule and lifecycle versions, entry event ID, PAPER execution
model, holding bars, and `paper_only=true` / `order_placed=false` metadata.
Regime fields also carry `regime_context_source` and
`regime_observed_at_ms`, preventing post-fill close context from being
presented as if it were known at an open fill.

## Verification

Focused tests cover disabled operation, next-open entry, inclusive initial and
trailing stops, pending trend/time/opposite exits, the exact holding-bar
boundary, gap reset, universe pruning, hard symbol bounds, replay deduplication,
non-recursion, action labels, Discord timing/scope text, and runtime persistence.
The R3 plan documents this runtime boundary and the example-settings hash was
re-frozen before replay, but the R3 strategy, spec, opportunity estimand, costs,
labels, and analyzer screen were not changed by this feature.
