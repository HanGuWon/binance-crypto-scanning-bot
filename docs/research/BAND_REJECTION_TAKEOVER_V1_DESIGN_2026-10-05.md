# Band rejection and directional takeover: research design v1

## Material Passport

- Origin skill: academic-research-suite, experiment design following literature review.
- Origin date: 2026-10-05, Asia/Seoul.
- Version: `band_rejection_takeover_v1_draft`.
- Verification status: **UNVERIFIED — design only; no backtest executed**.
- Authority: proposed research protocol, not a frozen registration or promotion receipt.
- Scope: public market data, research and future shadow alerts; no order execution.
- Thresholds below are explicit experimental seeds, not established market constants.

## 1. Research question and interpretation

Does repeated rejection near a band-contact price, followed by a causal break of
the opposing edge of the observation range, improve cost-aware directional
outcomes beyond the same price-structure trigger without wick conditions?

The proposed sequence is approach -> contact -> failed progress -> opposite-side
takeover -> executable research entry. An upper contact studies a bearish reversal;
a lower contact studies a bullish reversal. Treat these as separate hypotheses.
Wicks describe rejected price excursions; they do not identify participants,
measure their inventories, or establish the cause of a move.

There are two separate experiments:

1. **Candle experiment:** reproducible from closed 1m OHLCV; tests the user's wick
   observation and price-level handover proxy.
2. **Microstructure extension:** public trades, BBO and reconstructed depth; tests
   whether actual flow reversal adds information to the candle experiment.

Passing the first cannot validate the second. Missing depth is unknown, not neutral
flow. Price-level handover in OHLCV must not be called verified order-book takeover.

## 2. Evidence and boundaries

- Chung & Bellotti (2021), *Evidence and Behaviour of Support and Resistance Levels
  in Financial Time Series*: repeated prior bounces correlate with subsequent
  bounces and effects can decay. The minute-level sample is EURUSD, LLOY and BRENT
  in 2018, not Binance crypto. This motivates price-zone and recency measurements,
  not a profitable strategy claim. [Original paper](https://arxiv.org/html/2101.07410v1).
- John Bollinger's rules distinguish band tags from signals and describe band
  walking during trends. Use contact as context and independently verify reversal.
  [Original guidance](https://www.bollingerbands.com/bollinger-band-rules).
- Cont, Kukanov & Stoikov (2014) relate short-interval price changes to order-flow
  imbalance and depth. This is support for examining flow and liquidity; contemporaneous
  price impact is not proof of a forward trading edge.
  [Original paper metadata and abstract](https://arxiv.org/abs/1011.6402).

The previous conversation reviewed both positive and negative candle-pattern
studies. No verified source establishes this exact 1m Binance combination.

## 3. Data, clocks and features

Primary bars are 1m, closed, contiguous and venue-specific. Never reconstruct 1m
wicks from 5m bars. Spot and USD-M perpetuals have separate panels. All canonical
times are UTC Unix milliseconds. Record exchange time and local availability
time when using captured streams; display UTC and Asia/Seoul in review outputs.

Define each bar's range, upper wick, lower wick, close location and body fraction:

```
range_i = H_i - L_i
u_i = (H_i - max(O_i, C_i)) / range_i
l_i = (min(O_i, C_i) - L_i) / range_i
clv_i = (C_i - L_i) / range_i
body_i = abs(C_i - O_i) / range_i
```

Require finite positive valid OHLC prices and nonnegative volumes. For a zero-range
bar, set wick fractions, body fraction and clv to unavailable; it cannot count as
a rejection or confirmation. Keep it as a timestamped observation, not a gap.

Bands: SMA20 of closes plus/minus 2 population standard deviations. At bar i use
`U[i-1], M[i-1], LBand[i-1]` to assess that bar's contact and return. ATR14 uses
Wilder smoothing. Use a declared deterministic seed and 200 contiguous prior 1m
bars for warmup. The choice of lagged bands prevents the tested bar from moving
its own contact threshold; it differs intentionally from a chart's live band.

At first contact t0 freeze `A0 = ATR14[t0-1]` and contemporaneously available tick
size. Define `delta = max(2*tick_size, 0.10*A0)`. Align price barriers outward to the
tick grid. No future symbol metadata or full-sample normalization is allowed.

## 4. Symmetric candle strategy

### 4.1 Parent episode and approach

One active episode per market and symbol. Before t0 require three consecutive
closed bars with `LBand[i-1] < C_i < U[i-1]`. At t0 require exactly one band contact:
upper if `H_t0 >= U[t0-1]`, lower if `L_t0 <= LBand[t0-1]`. A bar touching both is
recorded as `BOTH_BANDS_AMBIGUOUS` and is not a directional parent.

For an upper parent require `C[t0-1] - C[t0-6] >= 0.50*A0`; mirror for a lower
parent. This defines an actual approach rather than an unrelated isolated wick.
Freeze upper anchor `Z = H_t0` or lower anchor `Z = L_t0`.

If there is no valid approach, retain the contact record with its rejection reason.
This parent population is separate from the project's existing frozen raw-C0
population; do not replace or relabel that population.

### 4.2 Fixed observation window

Observe exactly five closed bars `W = [t0, ..., t0+4]`. Let `tL = t0+4` be the fixed
landmark. All primary ablations use this same window and timestamp. Waiting is
explicit; no candidate may backdate a decision to t0.

At every close, upper episodes fail if `C_i > Z+delta`, lower episodes fail if
`C_i < Z-delta`. Missing bars or invalid OHLC make the episode unevaluable.
Failure is terminal even if prices subsequently reverse; retain those failures.

The common price-structure baseline requires the landmark close to be inside
lagged bands. Freeze at tL:

- Upper: takeover barrier `Q = min(L_i for i in W)`, invalidation
  `S = max(H_i for i in W) + 0.15*A0`.
- Lower: takeover barrier `Q = max(H_i for i in W)`, invalidation
  `S = min(L_i for i in W) - 0.15*A0`.

The barriers exclude all subsequent confirmation bars. Never move Q to include
the bar that is supposed to cross it.

### 4.3 Rejection counts and magnitude

A meaningful upper rejection must satisfy all of:

- `range_i >= max(4*tick_size, 0.25*A0)`;
- `u_i >= 0.40`, `l_i <= 0.20`, `clv_i <= 0.40`;
- `abs(H_i - Z) <= 0.25*A0`.

Mirror for a lower rejection:

- same minimum range;
- `l_i >= 0.40`, `u_i <= 0.20`, `clv_i >= 0.60`;
- `abs(L_i - Z) <= 0.25*A0`.

Count qualifying upper/lower rejections as `NU`, `NL`. Define directional wick
magnitude over W as the arithmetic mean of `u_i-l_i` for upper episodes and
`l_i-u_i` for lower episodes, using positive-range bars only. Record how many such
bars exist; require at least three for a magnitude verdict. Zero-range bars cannot
increase counts. Two-sided large wicks cannot pass either rejection predicate.

Full candidate requires relevant rejection count >=2 AND magnitude >=0.15.
Also record close progress, extremum progress and distance from Z in A0 units.
These diagnostics help identify rising-high/rising-close sequences that merely
contain upper wicks. They are not extra optimized gates in v1.

Consecutive qualifying bars count as separate bar observations, not independent
attack attempts. Independent touch/retreat cycles require trades/BBO and are
tested only in the extension.

### 4.4 Opposite-side takeover

After tL, examine at most three additional closed bars, j in `[tL+1, tL+3]`.
The first qualifying j is the signal timestamp for the common baseline and all
its wick ablations:

| Condition | Upper episode / bearish candidate | Lower episode / bullish candidate |
|---|---|---|
| Range handover | `C_j < Q-delta` | `C_j > Q+delta` |
| Candle direction | `C_j < O_j` | `C_j > O_j` |
| Body strength | `body_j >= 0.50` | `body_j >= 0.50` |
| Close location | `clv_j <= 0.30` | `clv_j >= 0.70` |
| Band location | `LBand[j-1] < C_j < U[j-1]` | same |

This deliberately requires both weak original progress and actual opposite
progress. A close already outside the opposite band is rejected as a late chase.
The candidate is not an intrabar warning or entry.

Process terminal invalidation before confirmation on every bar. In addition to
the anchor-close cancellation, upper episodes invalidate if `H_j >= S`, lower
episodes if `L_j <= S`. Thus an OHLC bar containing both pre-entry invalidation
and takeover cannot qualify. No confirmation by tL+3 means `EXPIRED`.

End each episode after invalidation, expiry or first confirmation. Enforce a
12-bar cooldown after its end and the three-inside-closes rearm requirement.
Never retroactively revive an episode. Replays produce the same event identity.

### 4.5 Research entry and risk constraints

Enter only at the next bar's open, adjusted for adverse execution costs. A captured
stream replay uses the first executable bid/ask after all required inputs are
actually available, never a quote preceding receipt of the closed signal candle.

Reject an entry if the unadjusted next open differs from signal close by more than
0.25*A0, or crosses invalidation. With executable entry P, structural risk distance
is `S-P` for short and `P-S` for long; require `[0.25*A0, 2.00*A0]` inclusive.
Reject when estimated roundtrip friction exceeds 25% of that distance. Record raw
signal alpha, execution eligibility and cost-avoidance effects separately.

Spot bearish results are `SPOT_EXIT` / avoid-long diagnostics, not executable
spot-short P&L. Both futures directions may be studied, but existing direction
promotion restrictions remain in force. No leverage assumption or order adapter.

## 5. Microstructure extension: what the trader sees in the book

Use a new separately scoped capture/replay contract with synchronized snapshots
and depth deltas, BBO, public aggregate trades and closed 1m bars. The existing
capture foundation documents a frozen 5m evidence set; it must not be silently
expanded by this design. Audit coverage before efficacy work. OHLCV does not
reconstruct past book queues, cancellation behavior or quote dwell times.

Measure these quantities independently:

1. **Failed attack cycles:** define a fixed anchor zone `[Z-delta, Z+delta]`.
   For upper episodes, midpoint enters from below, does not dwell above the upper
   edge for 1 second, then exits below the lower edge and remains there for 1
   second. Mirror for lower episodes. A cycle becomes observable only after the
   final dwell; repeated quotes within a zone do not produce new attempts.
   If the midpoint dwells through the far edge for 1 second, mark sustained
   penetration instead of failed attack. Missing/stale BBO invalidates dwell
   continuity; do not bridge silence. At least two distinct failed cycles is
   an exploratory count condition, not a frozen v1 gate.
2. **Aggressive trade imbalance:** quote-notional taker buying minus selling,
   divided by their sum. No trades means unavailable. Use `m=true` in Binance
   aggregate trades as buyer-maker / seller-aggressor; verify the venue's schema.
3. **Displayed depth imbalance:** quote notional within 5 bps on each side of
   current midpoint, `(B-A)/(B+A)`. Time-weight it on the observed valid timeline;
   report missing coverage. This is displayed liquidity, not committed demand.
4. **OFI:** aggregate best-quote price/quantity changes using a versioned CKS
   definition. Normalize with a strictly available depth measure. It includes
   replenishment/withdrawal effects; do not substitute trade imbalance for OFI.
5. **Progress versus effort:** pair aggressive flow with signed midpoint progress
   in A0 units. Strong buying without upward progress is consistent with
   absorption but is not proof of a particular seller. Never divide by near-zero
   price progress to manufacture an extreme score.

An initial, separately registered flow gate can compare the 10-second intervals
ending at the closed confirmation boundary b:

- Short: taker imbalance in `[b-20s,b-10s)` >=+0.20 and `[b-10s,b)` <=-0.20.
- Long: mirror the signs.
- Book state valid throughout both windows, BBO age <=1 second at evaluation,
  positive traded notional in each window. Late-arriving trades are not
  retroactively included in the original live decision.

This is an incremental flow-reversal test. Depth imbalance, OFI and cycle count
stay diagnostic initially to avoid a many-condition optimizer. Evaluate price
handover without flow, with flow, and with magnitude-matched unrelated flows.
Do not infer queue depletion from a wall disappearing: it may be cancelled.

Reconstruct each venue's book under its own update-ID rules. Futures `pu` must
continue from prior `u`; gaps require resynchronization. Use the project's proven
capture owners if implementation is later authorized. Sequence-invalid periods
remain unavailable. Historical exchange time alone does not prove live availability.
[Binance Spot stream contract](https://developers.binance.com/en/docs/catalog/core-trading-spot-trading/api/ws-streams/~)
and [USD-M book reconstruction](https://developers.binance.com/en/docs/products/derivatives-trading-usds-futures/websocket-market-streams/How-to-manage-a-local-order-book-correctly).

## 6. Backtest comparison design

Use the same frozen parent contacts, tL landmarks, Q/S barriers, confirmation j,
entry timing and execution rules across the candle family:

| ID | Variant | Question |
|---|---|---|
| C0 | Band context + range takeover, no wick gate | Does price handover alone work? |
| C1 | C0 + relevant rejection count >=2 | Does repeated rejection help? |
| C2 | C0 + magnitude >=0.15 | Does size asymmetry help? |
| C3 | C0 + both gates | Does the full candidate improve C0? |

C3 versus C0 at 12 bars is the primary comparison, separately for upper and lower.
C1/C2 are secondary mechanism tests. A contact-only next-open contrarian strategy
is a descriptive timing benchmark, not the primary control. Never compare C3's
later entry to a t0 entry and attribute all differences to wick information.

Also register a secondary **EARLY versus TAKEOVER** timing comparison to directly
test whether weakness alone is a sufficient entry condition. EARLY uses the same
C3 wick predicate and S frozen at tL, but attempts entry at tL+1 open without
requiring a subsequent takeover. TAKEOVER uses C3 as defined above. Apply the same
entry-risk/cost constraints, stop/target and maximum holding length to both.
Compare their contributions over all common landmark-qualified parent episodes,
counting genuine abstentions as zero and retaining EARLY losses on episodes where
TAKEOVER never confirms. Report delayed-entry cost, missed profitable reversals
and avoided failed reversals. Do not condition the EARLY sample on later takeover;
that would grant it future information. This compares complete timing policies,
not the isolated informational effect of a wick.

On the same valid-book parent subset compare C3 with and without the registered
flow gate. Report unavailable-book parents separately and also compare candle-only
performance on available versus unavailable periods. Do not fill absent OFI with
zero or selectively drop book coverage only from the comparator.

Retain every parent's failure, expiration, rejected signal and execution exclusion.
Record downstream C0 opportunities even when C3 rejects them. A trade-capable
ablation does not change whether the common baseline produces an opportunity.

## 7. Outcomes and executable simulation

### 7.1 Signal and selection panel

At each common confirmation j record next-open-entry signed terminal returns at
1/3/6/12 bars, MFE/MAE and a separately versioned 72-bar path. The 12-bar return
is primary. All bar counts refer to this new **1m experiment**; do not compare them
as identical durations to the existing 5m experiment.

Terminal horizon h ends at the close of the h-th bar starting at entry bar j+1.
Deduct per-side fees, adverse slippage and settled futures funding crossed by the
hypothetical holding interval. Avoid charging spread twice when executable BBO
already accounts for it. Frozen historical slippage proxies are scenarios, not
measured historical quotes.

Primary paired policy contribution on each executable C0 opportunity:

```
y0 = cost-adjusted signed 12-bar terminal return
y3 = y0 if C3 accepts, otherwise 0 for abstention
delta_y = y3 - y0
```

Report mean delta_y over the SAME C0 opportunity denominator, mean accepted y0,
mean rejected y0, coverage and opportunities per parent/day. This explicitly
separates selecting useful signals from avoiding losing signals. An improved
abstention policy with negative accepted expectancy is not an entry edge.

Use a shared complete-outcome mask across ablations; count missing horizons as
unevaluable, never as abstention zero. Do not count terminal expiry as a trade.
Incomplete raw paths remain explicit. Fixed-horizon overlapping counterfactual
returns are not a portfolio equity curve.

### 7.2 Risk-constrained paper portfolio

Secondary economic check: frozen structural stop S, fixed 1.5R target from actual
entry, maximum 12 entry bars, no trailing or break-even rule. One position per
market/symbol; no pyramiding. Size each entry at 0.10% of current simulated equity
at risk, cap each symbol at 10% equity notional and total open risk at 0.50%.
Initialize equity at 100,000 USDT. Skip infeasible lot/notional sizes; no leverage.
Report this sizing scenario rather than claiming it is an optimal allocation.

For simultaneous eligible entries sort by signal availability time, then market,
then symbol; do not prioritize using future returns. Each ablation simulates its
own resulting portfolio with the same rule; do not copy the baseline occupancy.

On an OHLC bar hitting target and stop, label order ambiguous and show both
pessimistic stop-first and optimistic target-first results. Promotion requires
acceptable pessimistic results. Fill stop gaps at adverse open, not the ideal stop;
do not grant favorable target-gap improvement. For shorts mirror highs/lows and
execution signs. Apply time exit at the 12th entry-bar close. Funding uses actual
settlements, not a single current funding rate across history.

Report fees, turnover, drawdown, tail losses, profit factor, expectancy in bps and R,
exposure, rejected opportunities and the count affected by collision assumptions.
Stress slippage at 2x, and entry latency by one full additional minute for OHLCV.
Use finer delay scenarios only when recorded executable quotes support them.

## 8. Dataset roles, inference and adoption criteria

Before any outcome-driven tuning, version the dataset manifests, assets, periods,
thresholds, family comparisons, cost assumptions and exclusions. Review 1m data
coverage without looking at future returns. An initial resource-bounded universe
can be BTCUSDT, ETHUSDT and SOLUSDT per market; expansion is a new declared cohort,
not evidence of full dynamic-universe parity.

Previously examined project data is development/falsification data even if it has
a file or split named holdout. Do not claim fresh independence from changing its
timeframe. Use genuinely untouched evidence where demonstrable, followed by
post-freeze prospective observations. Existing locked campaigns stay untouched.

Use chronological development/validation/evaluation with purge/embargo at least
the longest label path (72 minutes); enlarge if another preregistered dependency
requires it. No random bar split. Warmup features may use preceding closed history;
outcome labels must not cross a split boundary. Exclude episodes spanning a split.

Use shared UTC-calendar moving-block bootstrap draws across variants and symbols:
7-day blocks primary, 14/28-day sensitivity, 2,000 resamples, fixed recorded seed.
Inspect whether period length and serial dependence support those choices; report
an underpowered design rather than treating the defaults as a guarantee.

For primary upper/lower C3-C0 hypotheses use Holm correction at family alpha 0.05
and report effect estimates with 95% uncertainty intervals. C1/C2, extra horizons,
parameter sensitivities and any extension actually used for selection expand the
registered selection family. Report all attempted variants, including failures.

Outcome-blind diagnostics: symbol/calendar coverage, missingness, stage frequencies,
spread, volume, lagged 15m ADX/EMA slope and bandwidth regime. Classify trend strength
with a documented closed-15m ADX25 cut as an initial reporting slice, not a hindsight
filter. No open higher-timeframe bars. Threshold sweeps are not permitted on final
evaluation data; each sensitivity is prespecified and cannot silently replace v1.

Draft adoption gates, to freeze before evaluation:

1. Primary C3-C0 effect is positive with corrected support and a 95% lower bound >0.
2. Accepted 12-bar net expectancy exceeds a predeclared practical margin; draft
   margin is 5 bps, with uncertainty reported. This is NOT a claimed calibrated
   hit probability or an existing project promotion rule.
3. The pessimistic executable simulation has positive net expectancy; 2x-slippage
   and latency sensitivities do not reverse the claimed practical usefulness.
4. Publish leave-one-symbol-out and period/regime results; do not promote a broad
   claim that depends on one symbol or an isolated period. A narrowed scope is a
   new candidate requiring its own evidence, not a rescued v1 result.
5. New prospective shadow observations confirm direction-specific effects and
   live availability. Collection ends at a preregistered precision/power contract,
   not an arbitrary number of days. Estimate effective episode/block sample size
   using development data, not the nominal count of correlated bars.

Failing a gate yields reject/inconclusive, not extra tuning on final outcomes.
Operational recommendation wiring requires existing direction-specific receipts;
scanner production order execution remains prohibited.

## 9. Outputs and implementation acceptance criteria

Proposed future outputs, not files claimed to exist now:

- `manifest.json`: data/parameter/code hashes, provenance and coverage.
- `episodes.parquet`: every contact, landmark, frozen references and terminal state.
- `opportunities.parquet`: C0 signals, all variant decisions/reasons and availability.
- `outcomes.parquet`: immutable raw paths and versioned cost/label derivatives.
- `paper_trades.parquet`: executable assumptions, risk, collisions and exclusions.
- `summary.json` and `report.md`: full comparison family and uncertainty.

Every research decision carries deterministic parent/decision identity, rule version,
feature values, blockers, invalidation, expiry and source timestamps. Event identity
includes market, symbol, t0, side, protocol hash and decision stage. Stochastic
analysis uses recorded seeds; decisions themselves are deterministic. Bound each
episode buffer and microstructure window; prune expired symbols/state.

Required future tests include positive short and mirrored long sequences; opposite
wick/doji rejection; small-range artifacts; exactly-on-threshold behavior; two-band
contact; sustained original-direction breakout; no takeover; same-bar invalidation
and takeover; post-signal gap; zero-range and missing bars; duplicate/reordered
events; closed-HTF alignment; buyer-maker sign; missing book/dwell continuity;
funding crossing; short P&L sign; stop/target collision; split purge; reconnect replay.

A prefix-invariance check must show that appending future rows cannot change any
earlier features, states or decisions. Independently hand-check small price
sequences before running a large study. Standard repository checks apply when
implementation is later undertaken. No existing CLI command is claimed to execute
this new strategy; the current runner must first be checked for compatible 1m and
episode semantics. Do not disguise an unrelated existing backtest as this study.

## 10. Scope and next implementation order

1. Outcome-blind 1m availability and metadata audit; identify contaminated datasets.
2. Freeze this draft's operational choices and comparison family as a separate protocol.
3. Implement a pure causal episode evaluator and independent test fixtures.
4. Run historical falsification, then one frozen evaluation and prospective shadow.
5. Register the microstructure extension only after capture availability qualification.
6. Consider adding validated evidence to alerts under existing promotion authority.

This document creates no candidate registration, data capture, trading permission,
backtest result, Discord output or deployment change. It preserves the current
contracts in `docs/RESEARCH_GOVERNANCE.md`, `docs/BACKTEST_SPEC.md`,
`docs/TRADING_CAPABILITY_MATRIX.md` and `docs/TRADING_MODE_PROMOTION.md`.
