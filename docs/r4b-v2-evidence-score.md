# R4B V2 Evidence Score V1

Status: factory/ownership boundary implemented as a disconnected,
non-promoting shadow annotation. Price, participation, volatility, and
target-excluded cross-sectional calculation documents exist. The derivatives-
positioning and liquidity-execution producers and all six family-to-envelope
authority adapters remain unimplemented. Strict combined-query USD-M
`aggTrade`/`kline_5m` M1 parsing and several capture/M2 prerequisites now
exist. They enforce the exact V2 factory/lifecycle/ingress boundary, one sealed
persisted start receipt per lease acquisition, pre/post-handshake authority and
runtime-readiness admission, opened-root mutation checks, and real-disk
SOURCE_GAP lifecycle coverage. None is bound to a score producer, and a
complete M2 source-census/cursor-finality certificate is still missing.

This score answers only: **how strongly do distinct, capped information
families agree on direction at one causal 5-minute decision slot?** It is not
a probability, entry rule, portfolio selector, or claim of future profit.

## Authority boundary

- Only evidence bound to the public USD-M Futures promoting capture plan is
  accepted.
- Spot records are rejected rather than silently downgraded or mixed in.
- The annotation cannot open, close, suppress, rank, or filter Family A, B, or
  C positions or alerts.
- It cannot enter the current attempt's alpha, NAV, PnL, or efficacy statistic.
- A future promoting use requires a separately sealed successor rule and a
  strictly later, non-overlapping qualification and prospective attempt.

## Fixed information families

Each decision requires exactly one observation from each family:

1. `PRICE_STRUCTURE_MOMENTUM`
2. `PARTICIPATION_FLOW`
3. `VOLATILITY_REGIME`
4. `DERIVATIVES_POSITIONING`
5. `LIQUIDITY_EXECUTION`
6. `CROSS_SECTIONAL_CONTEXT`

RSI, MACD, moving averages, and other transforms of the same price history do
not each receive an extra vote. Their producer must collapse them into the one
`PRICE_STRUCTURE_MOMENTUM` contribution. Factory-sealed dependency claims bind
each family to an allowlisted economic dependency class and exact slice; a
renamed copy of the same slice or producer evidence cannot enter two families.
Shared raw lineage is allowed only when the economic slices are distinct, such
as close-path price evidence and high-low volatility evidence.

## Causal and readiness contract

- The candle must be fully closed.
- The bar is exactly 5 minutes and the decision cutoff is exactly `k.T + 2001
  ms`.
- Every economic data-through or transaction time must be `<= k.T`. Binance
  exchange observation time may follow `k.T`, but exchange observation and
  local receipt/completion must be `<= D`; equality passes and one millisecond
  later fails.
- A family reports one of `READY`, `FEATURE_NOT_READY`, `INCONCLUSIVE_DATA`, or
  `DATA_INVALID`.
- Any non-ready family withholds the whole score. Missing inputs are never
  replaced with zero-strength neutral evidence.

## Exact score

Every ready family contributes:

- `direction` in `{-1, 0, +1}`;
- `strength_micros` in `[0, 1_000_000]`;
- fixed weight 1 and a single capped contribution.

The implementation records the exact numerator and denominator:

```text
score_numerator_micros = sum(direction_g * strength_micros_g)
score_denominator = 6
evidence_score_micros = nearest_integer(score_numerator_micros / 6)
```

Ties round away from zero. Classification uses the exact numerator, not the
rounded display value:

- bullish/bearish lean: magnitude at least `2_000_000` and at least three
  same-direction families;
- bullish/bearish strong: magnitude at least `4_000_000`, at least four
  same-direction families, and at most one opposing family;
- otherwise neutral.

These labels mean evidence agreement only. They must never be rendered with a
percent sign or described as calibrated confidence.

## Audit and replay

Producer envelopes and family observations cannot be directly constructed or
mutated with `dataclasses.replace`. A bounded per-decision ownership ledger
accepts exactly one envelope per family, rejects mixed slots and cross-family
slice aliases, finalizes all six atomically, and derives the score input's
closed/complete flags from the sealed leaves. Its canonical state restore
requires externally pinned scope, count, capacity, finalized state, and replay
root.

The logical event ID is a domain-separated RFC 8785 SHA-256 over attempt,
symbol, bar slot, role, and rule version. Feature values are deliberately not
part of that ID, so conflicting recomputation at the same logical slot collides
instead of creating a second event. The full canonical payload has a separate
SHA-256.

The bounded registry treats an identical event and payload as an idempotent
duplicate, rejects the same event ID with a different payload, and fails closed
when its configured capacity is exhausted. Durable delivery integration must
preserve the same semantics before this annotation is connected to Discord.

## What remains before probability language

The four existing shadow calculation rules still need M0/M1/M2 authority
bindings and family-to-envelope adapters; the derivatives-positioning and
liquidity-execution producer rules still need to be implemented, frozen, and
tested. Probability language is permitted only after all six authoritative
producers are connected and an untouched prospective sample supports a
pre-specified mapping from score, regime, and horizon to **after-cost profitable
outcome**, with calibration metrics and sample counts. Until then the only
permitted UI label is `Evidence Score (not a probability)`.
