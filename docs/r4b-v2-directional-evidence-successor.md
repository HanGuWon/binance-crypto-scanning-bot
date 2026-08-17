# R4B V2 Directional Evidence Panel Successor

Status: deterministic calculation contract and sibling shadow renderer are
implemented, but they remain disconnected, M0/M1/M2-unbound, non-promoting,
and not a probability model. This document does not change the frozen Evidence
Score V1 rule or any Family A/B/C decision.

## Implementation checkpoint

The calculation shell now selects exactly the three directional families,
exhaustively classifies all 27 sign combinations, applies exact signed-magnitude
arithmetic, excludes all three context families from the numerator, emits a
canonical deterministic payload, and uses a bounded duplicate/conflict gate.
The renderer shows UTC/KST, the uncalibrated index, family counts, context
readiness, invalidation, reasons, and rule version without `/100`, percent,
expected-return, or order language.

This is not a live evidence path. Current inputs are legacy factory-sealed
observations, and the payload therefore states
`LEGACY_OBSERVATIONS_M0_M1_M2_UNBOUND`. The canonical payload separates the
exchange-event and local-receipt fields, but certified `data_through_ms` remains
null with `UNBOUND_M2`; the legacy closed-bar boundary is retained only as
`assumed_closed_bar_through_ms`. These fields are not yet rendered in Discord.
A real
A/B/C decision is not yet bound, so every circularity label is fixed to
`PRIMARY_BINDING_UNAVAILABLE`. Typed volatility, derivatives crowding,
execution-quality, and book-pressure values are also not connected; legacy
context direction and strength are omitted from the successor canonical
surface rather than exposed as return votes.

A first real price-source vertical slice now exists beside this still-
disconnected panel. It consumes exactly 8,654 canonical final `kline_5m` M1
rows (one continuity anchor plus 8,653 calculation closes), retains separate
data, exchange-event, and receipt clocks, and reuses the frozen close-path
calculation without inventing an opening-event identifier. Its authority is
structurally fixed to `M1_ONLY_UNBOUND`; `data_through_ms` and the M2 certificate
remain null, while causal completeness, producer readiness, and promotion are
false.

An exact aggregate-trade M1-only participation slice now also exists. It
requires the current slot plus 8,640 prior nonempty five-minute slots, preserves
every retained aggregate trade and its aggregate/raw trade IDs, binds an
explicit contract-multiplier authority, and reuses the same sealed numeric
calculation as the older shadow evidence. A missing slot is
`UNKNOWN_NOT_ZERO`; an observed aggregate/raw ID gap withholds calculation.
The slice is still `M1_ONLY_UNBOUND`: it cannot infer exchange completeness
from absence, has no M2 certificate or `data_through_ms`, and cannot populate a
producer envelope. Retaining both full lineage and economic rows for a real
30-day BTC trade window also has a known memory/canonical-payload cost; a
streaming compact-root successor is required rather than an arbitrary row cap.

For exposed historical research only, a separate outcome-blind kline proxy
reuses that participation calculation with the explicit
`ALL_TRADES_ASSUMED_NORMAL_KLINE_PROXY` assumption. It uses only closed
five-minute `quote_volume` and `taker_buy_quote_volume`, never labels or forward
returns, and seals that it is not equivalent to exact aggregate-trade M1.
This permits an honestly labeled retrospective diagnostic but cannot supply
live source authority.

A frozen ex-target cross-sectional directional candidate also now exists as a
disconnected sibling. It consumes only the existing canonical target-excluded
context, maps robust shock and sign-consistent breadth exactly once under
Decimal34, and preserves the market sign independently of strength
quantization. A nonzero sign with zero sub-quantum magnitude is valid here;
non-ready context is explicit `NOT_READY`, never a neutral replacement. The
object binds the source-context hash, ex-target roots,
reasons, and clocks while fixing M0/M1/M2, producer, promotion, probability,
target-return, primary-direction, and outcome claims to false. This is the
numeric pre-outcome adapter, not the still-missing raw-authority/M1/M2 adapter,
and it has no efficacy claim. Because the current family-observation contract
couples zero direction and zero strength, any future producer conversion must
freeze that boundary under a new rule version rather than silently erasing the
candidate sign.

The remaining three-family connection is therefore the target-excluded
cross-sectional raw/M1/M2 adapter plus rule-versioned conversion of all three
numeric candidates into producer observations. Until that exists, the panel
cannot release a live directional annotation.

A frozen matched-outcome audit contract now requires one stable event identity,
one execution/cost-contract hash, and all 5/15/30/60/360-minute rows for every
admitted 2-of-3 or 3-of-3 state. A sibling shared seven-day circular UTC
moving-block bootstrap uses one draw schedule across all side, horizon, and
agreement cells and retains zero-alert days. It reports the point
`mean(3/3)-mean(2/3)` contrast and uncertainty diagnostics while fixing
inference, efficacy, and probability claims to false. No real successor
outcomes have yet populated these contracts.

The historical alert-replay runner now derives its rule settings from the
frozen backtest specification, records that specification's rule version, and
evaluates 5, 15, 30, 60, and 360 minute fixed horizons. A separate pure
counterfactual technical-exit evaluator enters only at the next contiguous
five-minute open and applies initial/trailing stops, trend failure, time exit,
fees, slippage, and funding causally. It is intentionally not joined to the
alert replay yet: opposite-signal exits need the matched later-decision stream,
and historical BBO, depth, latency, and impact evidence is absent.

## Decision

The bot may mechanically report that more distinct, capped evidence families
point in the same direction. It must not count raw indicators, describe the
result as independent confirmation, or translate the result into a probability
before prospective after-cost calibration.

RSI, MACD, EMA ordering, and target returns are transforms of the same price
path. They may influence one `PRICE_STRUCTURE_MOMENTUM` composite, but they do
not receive separate votes. The same single-vote rule applies to related flow,
book, positioning, volatility, and market-panel features.

## Why V1 is not the final user-facing score

Evidence Score V1 divides signed strength by six and requires all six families
to be ready. That boundary remains useful for ownership and replay tests, but
its semantics are not suitable for a directional alert grade:

- volatility has no honest bullish or bearish sign;
- open interest has no long/short sign, while funding and basis can describe
  carry or crowding without choosing continuation versus reversal;
- displayed depth and spread primarily describe execution risk; signed book
  pressure is only a short-horizon forecast candidate;
- `/100` can be mistaken for a calibrated probability;
- when volatility and positioning are conservatively neutral, the current
  `STRONG` numerator requires all four remaining families to reach their
  maximum. The implemented price and participation strength transforms are
  strictly below their cap for every finite input, so that configuration cannot
  attain `STRONG`;
- primary A/B/C rules already own some of the same features, so a large score
  can restate why the setup fired rather than add independent evidence.

The latest completed legacy alert replay cannot adjudicate score monotonicity.
It generated 9,723 recommendations, every score was `100`, every event was an
information-only setup, and there were no confirmed PAPER alerts. Its 60-minute
mean after-cost directional return was -31.26 bp with a 25.98% strict hit rate,
but an audit proved that the runner used the settings file's older
`v4.3.0-causal-structure-diagnostics` contract rather than the intended frozen
specification. That run also predated the one-bar 5-minute outcome. The wiring
and horizon contract are now fixed, but a new matched replay is required; the
old result is neither evidence against nor evidence for this disconnected
successor panel.

## Successor schema

### Directional state panel

The initial eligible set contains exactly three signed state families:

1. `PRICE_STRUCTURE_MOMENTUM`
2. `PARTICIPATION_FLOW`
3. `CROSS_SECTIONAL_CONTEXT_EX_TARGET`

Each family emits one absolute market-state sign in `{-1, 0, +1}` and one
capped magnitude in `[0, 1_000_000]`. All three must be READY for a complete
panel. A missing family is not replaced by a neutral zero.

For an eligible set `G` fixed by the rule version:

```text
directional_numerator_micros = sum(sign_g * magnitude_micros_g for g in G)
directional_denominator = len(G)
directional_agreement_micros = nearest_integer_ties_away_from_zero(
    directional_numerator_micros / len(G)
)
```

The value is an uncalibrated descriptive agreement index in `[-1, +1]` after
display scaling. It is not a forecast probability or expected return.

Mechanical display classes for the initial three-family version are evaluated
in the following order:

- `BROAD_BULLISH_STATE`: three bullish and zero bearish families;
- `BULLISH_STATE_TILT`: at least two bullish and zero bearish families;
- `BROAD_BEARISH_STATE`: three bearish and zero bullish families;
- `BEARISH_STATE_TILT`: at least two bearish and zero bullish families;
- `MIXED_OR_NEUTRAL_STATE`: every other complete combination;
- `WITHHELD`: any required family is unavailable or fails causal authority.

Strength changes the continuous index and ordering inside a class. It does not
allow one strong family to manufacture a broad class against an opposing
family.

### Shadow directional candidate

`BOOK_PRESSURE` is initially separate from the eligible denominator. It may
show signed, side-symmetric, duration-weighted pressure only after a
sequence-valid standard book proves both sides of the required band. It remains
`SHADOW_CANDIDATE` until a frozen 5-minute after-cost replay and untouched
prospective sample justify adding it to a later rule version.

The book candidate must combine imbalance, persistence/cancellation, spread,
and coverage once. Those fields cannot become multiple votes. Literature that
supports next-tick or next-few-price-change prediction does not by itself prove
a five-minute forecast.

### Non-directional context panel

The following fields never enter the signed numerator in the initial version:

- `VOLATILITY_REGIME`: magnitude, percentile, and calm/normal/elevated/extreme
  state;
- `DERIVATIVES_CROWDING`: OI, basis, and funding composite reported as
  neutral/long-crowded/short-crowded with intensity, not return direction;
- `EXECUTION_QUALITY`: spread, symmetric depth, staleness, and source
  continuity reported as pass/warn/fail.

A future context veto or conditional directional mapping requires a new frozen
rule, qualification data that were not used to select the rule, and a later
prospective attempt.

### Dependency and circularity labels

Every displayed family also carries:

- an economic dependency cluster (`TARGET_PRICE_PATH`, `TRADE_FLOW`,
  `MARKET_COMMON_FACTOR`, `ORDER_BOOK`, `DERIVATIVES`, or `RANGE_VOLATILITY`);
- whether the active primary A/B/C rule already owns that feature group;
- causal data-through, exchange event, and local receipt cutoffs;
- readiness, reasons, invalidation, rule version, and deterministic event ID.

The UI may say `setup-owned evidence` or `additional context`. It must not say
`N independent confirmations` because price and cross-sectional state can share
a common market factor, and flow and book pressure can share microstructure
drivers.

## Alert wording

An allowed shadow annotation is structurally similar to:

```text
BTCUSDT | BROAD_BULLISH_STATE
Directional agreement: +0.71 (uncalibrated descriptive index)
State families: bullish 3 | bearish 0 | neutral 0
Price +0.82 | normal trade flow +0.69 | ex-target market +0.61
Book pressure: +0.54 SHADOW_CANDIDATE
Context: volatility ELEVATED | derivatives LONG_CROWDED | execution PASS
Not a probability, expected return, or order instruction.
```

`high probability`, a percent sign, and expected-profit language are forbidden
until the calibration gate below passes.

## Calibration and promotion gate

1. Connect every panel leaf to exact M0/M1/M2 source authority and closed
   five-minute cutoffs.
2. Freeze formulas and the eligible family set before inspecting outcomes.
3. Replay one-bar 5-minute plus 15, 30, 60, and 360 minute outcomes with
   observed or conservatively reconstructed BBO/depth, fees, slippage, funding,
   exits, and unresolved executions.
4. Report after-cost hit rate, mean/median net return, payoff ratio, profit
   factor, drawdown, alert rate, and confidence intervals by rule family,
   direction, agreement bucket, volatility/crowding context, and liquidity
   bucket.
5. Correct for the complete strategy and threshold search, use time-ordered
   walk-forward validation, and retain an untouched prospective PAPER/BBO
   interval.
6. Only if higher frozen agreement buckets show stable incremental after-cost
   value may a later version fit and validate a probability mapping. The
   mapping must publish its horizon, outcome definition, sample count,
   calibration error, and uncertainty interval.

## Research boundary

Primary references supporting the conservative boundary include Cont,
Kukanov, and Stoikov on short-interval order-flow imbalance
(`10.1093/jjfinec/nbt003`), Kolm et al. on the short effective horizon of LOB
alpha (`10.1111/mafi.12413`), He et al. on perpetual funding and basis economics
(`arXiv:2212.06888`), Sullivan, Timmermann, and White on technical-rule data
snooping (`10.1111/0022-1082.00163`), and Bailey et al. on backtest overfitting
(`10.21314/JCF.2016.322`). These sources motivate candidates and controls; they
do not validate this bot's five-minute after-cost performance.
