# Pump-fade v2b shadow previews — synthetic examples only

These English examples illustrate the state contract. They are **synthetic fixtures**, not recorded Binance events, live Discord deliveries or trading recommendations. No individual outcome probability is inferred.

### WAIT with continuation-risk observations

```text
PUMP V2b / EXPLORATORY / ABCUSDT / WAIT
Event: synthetic closed 5-minute +30%/24h, quote-volume threshold met.
Measured WAIT: trailing closed 1-hour return +6.2% > +5%.
Continuation risk: observed new event high and forced BUY liquidation sample.
Liquidation feed: censored; sample is NOT the total number of liquidations.
Action eligibility: WAIT. Risk-increasing ladder additions: zero.
Expiry: event time +24h UTC; KST is UTC+09:00.
```

### FADE-CANDIDATE with one release observation

```text
PUMP V2b / EXPLORATORY / ABCUSDT / FADE-CANDIDATE
Release R1: 60 minutes without an observed new high in fully closed 5-minute bars.
Current WAIT checks: cleared under complete and fresh inputs.
Spread: illustrative 10 bps; round-trip cost scenario: illustrative 30 bps.
Invalidation: synthetic observed event-to-date peak 130.00.
Decision is a research candidate; efficacy and individual probability UNAVAILABLE.
This preview creates no order and is not delivered to production Discord.
```

### SKIP after crash/rebound and missing evidence

```text
PUMP V2b / EXPLORATORY / ABCUSDT / SKIP
Observed synthetic peak-to-trough drawdown: 31% (threshold 30%).
Observed synthetic trough-to-latest close rebound: 11% (threshold 10%).
Primary joint SKIP condition satisfied. SKIP is absorbing for this episode.
Risk-increasing ladder additions: zero.
```

```text
PUMP V2b / EXPLORATORY / ABCUSDT / WAIT_UNAVAILABLE
Missing or stale point-in-time 5-minute continuity / OI / observed funding metadata
/ BBO depth. No false eligible release or false lack of continuation risk asserted.
No candidate, no ladder addition. Retain receipt-time/coverage-gap evidence.
```

The generated machine-readable single example is `docs/pump_fade_v2/release/sample_shadow_payload.json`. Its event ID is intentionally `SYNTHETIC_EXAMPLE_ONLY_DO_NOT_TRADE`, with `delivery_enabled=false`.
