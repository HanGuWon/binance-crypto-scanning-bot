# R4B V2 Historical Three-Family Consensus Experiment

Status: pre-outcome implementation contract. This sibling experiment is
historical-only, non-promoting, uncalibrated, and not an order instruction. It
does not modify Evidence Score V1, the live six-family producer, or any A/B/C
decision rule. The executable code, this contract, and every admitted input
must be hash-frozen before the new consensus is joined to any forward return.

## Question

For an already emitted USD-M Futures alert, does agreement among three capped,
non-duplicated directional state families improve after-cost directional
outcomes, and does complete `3/3` agreement outperform clean `2/3` agreement?

The experiment must not answer this question by counting RSI, MACD, EMA, or
other transforms of the same target-price path as separate confirmations. It
uses exactly one vote from each of:

1. `PRICE_STRUCTURE_MOMENTUM`;
2. `PARTICIPATION_FLOW`;
3. `CROSS_SECTIONAL_CONTEXT_EX_TARGET`.

Volatility, derivatives crowding, and execution cost remain non-directional
context. They cannot cast a vote or silently change admission.

## Outcome-blind anchor population

The population is the latest authenticated Indicator Discriminator V1A
Amendment 1 recommendation rows with `market == futures`, before any V1A fitted
score or outcome-based selection. The source recommendation and replay
manifest hashes are bound into the run manifest. The current expected unique
anchor counts on `(asset, direction, decision_time_ms)` are:

- development: 2,263;
- validation: 2,087;
- retrospective test: 1,991.

All three intervals are already exposed and therefore descriptive. They may
debug the formula and estimate historical behavior; they cannot establish a
prospective efficacy or calibrated-probability claim.

The fixed Futures panel is the seven V1A instruments in canonical order:
`1000BONKUSDT`, `ENAUSDT`, `WIFUSDT`, `1000FLOKIUSDT`, `ARBUSDT`, `OPUSDT`, and
`SEIUSDT`. Each target's cross-sectional family must exclude the target before
every market median, scale, breadth, and root is calculated. The remaining six
members are peers, not target-return inputs.

## Causal historical leaves

Every leaf is evaluated at the alert's fully closed five-minute decision bar.
No row later than that bar may enter a leaf.

- Price consumes exactly 8,653 ordered target closes and reuses
  `calculate_price_close_path_v2` without an alternate formula.
- Participation consumes the current target kline plus exactly 8,640 prior
  klines and reuses `calculate_participation_flow_v2`. Its explicit historical
  assumption is `ALL_TRADES_ASSUMED_NORMAL_KLINE_PROXY`; it is not exact
  aggregate-trade M1 evidence.
- Cross-section consumes exactly six target-excluded peer paths of 8,644
  klines, derives three-bar log returns, the per-time peer median, 8,640 prior
  median absolute deviations scaled by 1.4826, current shock, and
  sign-consistent breadth. It is a seven-asset historical proxy and does not
  weaken the live minimum-universe rule.

Every proxy must fail closed on the wrong venue, symbol drift, open or
misaligned candles, duplicate slots, internal gaps, missing edge slots,
non-finite economics, or a row outside its exact causal window. A non-ready
leaf is unknown, not neutral.

## Frozen consensus arithmetic

Each READY leaf contributes an absolute market direction in `{-1, 0, +1}` and
a capped magnitude in `[0, 1_000_000]`. The target alert's long/short side is
not used to calculate the leaf direction.

```text
numerator_micros = sum(direction_g * strength_micros_g for g in three_families)
denominator = 3
agreement_micros = nearest_integer_ties_away_from_zero(
    numerator_micros / denominator
)
```

Classification is sign-count based, in this order:

- three bullish, no bearish: `BROAD_BULLISH_STATE`;
- at least two bullish, no bearish: `BULLISH_STATE_TILT`;
- three bearish, no bullish: `BROAD_BEARISH_STATE`;
- at least two bearish, no bullish: `BEARISH_STATE_TILT`;
- any opposing-family combination: `MIXED_OR_NEUTRAL_STATE`;
- any unavailable required leaf: `WITHHELD`.

Magnitude orders states inside a class but cannot overpower an opposing leaf
or manufacture a broad class. The primary matched audit admits only a clean
tilt or broad state that supports the source alert's direction. Opposing,
mixed, neutral, and withheld states remain in a hash-bound all-anchor census;
they are not silently dropped.

The outcome-blind census is also a feasibility gate. If a side/bucket is
structurally empty or too sparse, the `3/3 - 2/3` comparison is reported as
inconclusive and no outcome is opened merely to redesign the bucket. A
majority-with-one-opponent state is not relabeled as clean `2/3`. Studying that
conflicted state requires the separately versioned, pre-outcome contract in
`r4b-v2-historical-three-family-topology-preoutcome-amendment.md`; it remains a
distinct, non-admitted comparator and is never pooled with clean `2 + neutral`.

## Cost-survival context

The decision payload reports, but does not select on, a context-only
cost-survival calculation. Under the frozen volatile Futures schedule of 5 bp
fee plus 8 bp slippage per side, the zero-move round trip is 26 bp, or 2,600
return micros. The payload also reports `ATR / decision_price` and one-ATR
headroom after that round-trip cost when both inputs are valid.

This context is not a fourth directional family, an expected-return estimate,
or an admission veto. Historical funding is excluded from the decision-time
context; realized fixed-horizon outcomes include funding only under their
separately frozen execution contract.

## Identity and audit trail

One deterministic event ID binds the experiment version, source protocol and
rule, source event ID, split, asset, symbol, USD-M venue, and five-minute
decision scope. Leaf payload hashes are deliberately not part of the identity:
the same economic event with altered evidence must collide in the bounded
event registry instead of becoming a second event.

The canonical payload binds the exact three leaf versions and hashes, target-
excluded peer set/root, source row and replay-manifest authority, aggregate
state, primary relationship, cost context, reasons, invalidation, and fixed
false claims for promotion, probability, and order placement.

## Fixed-horizon outcomes

Admitted supporting events receive exactly five independently recomputed
directional outcomes: 1, 3, 6, 12, and 72 closed five-minute bars, corresponding
to 5, 15, 30, 60, and 360 minutes. Entry is the next contiguous bar open.
After-cost return is signed in the alert direction and includes the frozen
fee/slippage contract plus causally attributable funding. A gap, split
boundary, missing next open, unavailable funding requirement, or other
execution failure is an explicit exclusion, never a zero return.

Primary descriptive comparisons are, separately for bullish and bearish
alerts and for every horizon:

- mean, median, strict positive hit rate, profit factor, event count, and
  evaluable coverage for clean `2/3` and broad `3/3`;
- `mean(3/3) - mean(2/3)`;
- a shared-calendar seven-day circular moving-block bootstrap that retains
  zero-alert days and uses the same draws for all cells.

Because all historical intervals are exposed, bootstrap intervals and
p-values remain diagnostics. No multiplicity, efficacy, probability, or
promotion claim becomes true from this run.

## TE0 technical-exit output

A secondary, overlapping counterfactual is evaluated for each admitted event
under `TE0_NO_OPPOSITE_SIGNAL`. It reuses the existing causal technical-exit
evaluator:

- next-contiguous-open entry;
- source structural invalidation as the initial stop;
- trailing activation at 1R and a two-ATR trail;
- three consecutive closed-bar trend-failure observations;
- maximum 72 completed five-minute bars;
- fees, slippage, and causally attributable funding;
- no opposite-signal exit, no order placement, and no portfolio-equity claim.

An invalid or wrong-side source stop, a feature mismatch, a data gap, or an
unopenable next bar is an explicit TE0 exclusion. TE0 does not replace the five
fixed horizons or select which consensus states enter their audit.

## Required artifacts and promotion boundary

One frozen run must reconcile and hash at least:

- `consensus.csv` for every anchor;
- `fixed_horizon_outcomes.csv`, exactly five rows per admitted event;
- `technical_exit_te0.csv`, exactly one result or exclusion per admitted event;
- `results.json`, a Korean report, and a run manifest binding code, contract,
  inputs, cost rules, counts, exclusions, and output hashes.

Only a later untouched PAPER/BBO interval can justify probability calibration.
Before displaying a percentage, a separately frozen model must demonstrate
time-ordered after-cost calibration, publish the horizon and target definition,
sample count, uncertainty and calibration error, and reproduce positive value
outside every interval used to design or inspect this historical experiment.
