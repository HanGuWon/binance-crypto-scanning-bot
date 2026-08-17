# D1 SCEFB-5M development preregistration v0

## Material Passport

- Origin Skill: academic-research-suite / experiment-agent
- Origin Mode: plan
- Origin Date: 2026-07-21
- Verification Status: `UNVERIFIED_OUTCOME_BLIND`
- Version Label: `D1_SCEFB_5M_PREREG_V0`
- Activation Status: `DISCONNECTED_SHADOW_NOT_IN_CURRENT_ATTEMPT`
- Contamination Boundary: no outcome, PnL, hit-rate, profit-factor,
  prediction, recommendation, or backtest-result artifact was inspected while
  choosing this rule

## Experiment overview

- Title: Sparse Compression-Expansion Flow-confirmed Breakout, 5 minute
- Objective: test whether a sparse first range break after volatility
  compression, aligned with strict-prior hourly trend and directional taker
  flow, can survive public-data PAPER execution costs
- Hypothesis: the frozen setup has positive after-cost expectancy under a
  3-ATR target, 0.8-ATR adverse exit, structural failure exit, and 24-bar hard
  horizon
- Type: disconnected strategy-development simulation followed, if not
  rejected, by a separately frozen prospective PAPER/BBO experiment

This is a conjunction, not indicator voting. Price structure and hourly trend
are one directional economic group. Volatility is a nondirectional eligibility
gate. Taker participation is one directional confirmation group. Liquidity and
book depth govern PAPER admission only. Their source overlap must be disclosed;
the number of passed conditions is not a probability.

## Frozen timing and universe

Let `t` be a fully closed UTC-aligned USD-M five-minute candle.

```text
D = close_time(t) + 2,001 ms
TR_j = max(H_j-L_j, abs(H_j-C_(j-1)), abs(L_j-C_(j-1)))
```

All source data-through times must be at or before `close_time(t)`. Every
exchange observation and local receipt used for the decision must be at or
before `D`. No unclosed candle or higher-timeframe candle may enter.

The fixed target universe is:

```text
BTCUSDT, ETHUSDT, BNBUSDT, SOLUSDT, XRPUSDT,
DOGEUSDT, ARBUSDT, OPUSDT, SUIUSDT, WIFUSDT
```

Eligibility requires:

- exactly 289 contiguous prior five-minute candles: one anchor close followed
  by exactly 288 calculation/participation candles;
- at least 250 contiguous, fully closed one-hour candles;
- prior 288-bar median quote volume at least `100,000 USDT`, exactly 100
  times the frozen `1,000 USDT` capacity cell;
- no gap, duplicate, zero denominator, invalid Decimal value, or late source.

Missing or invalid data withholds the setup. It is never converted to neutral.

## G1: price structure and strict-prior hourly trend

Compute Wilder ATR14 through `t-1` from the exact 288 calculation bars. The
additional anchor supplies the previous close for the first true range. Seed
ATR with the simple mean of the first 14 true ranges, then update as:

```text
ATR_new = (13 * ATR_previous + TR_current) / 14
```

Define the prior-only 24-bar channel and frozen ATR:

```text
U_t = max(H_(t-24), ..., H_(t-1))  # 24 bars, both endpoints inclusive
L_t = min(L_(t-24), ..., L_(t-1))  # 24 bars, both endpoints inclusive
A_t = ATR14 at t-1
```

Long first-cross:

```text
C_(t-1) <= U_t
C_t > U_t
0.10 <= (C_t-U_t)/A_t <= 0.50
(C_t-Low_t)/(H_t-Low_t) >= 0.75
```

Short is the exact mirror:

```text
C_(t-1) >= L_t
C_t < L_t
0.10 <= (L_t-C_t)/A_t <= 0.50
(H_t-C_t)/(H_t-Low_t) >= 0.75
```

`H_t == Low_t` is non-ready.

The latest causally available, fully closed one-hour candle must satisfy:

```text
LONG:  close_1h > EMA20_1h > EMA50_1h
SHORT: close_1h < EMA20_1h < EMA50_1h
```

EMA arithmetic uses the protocol Decimal context, `alpha=2/(n+1)`, the SMA of
the first `n` closes in the exact 250-hour window as seed, and recursive updates
through the final available closed hour.

## G2: volatility eligibility

Use only pre-signal ranges to define compression:

```text
compression = median(TR_(t-12), ..., TR_(t-1)) /  # 12 recent prior bars
              median(TR_(t-72), ..., TR_(t-13))   # 60 earlier prior bars

compression <= 0.70
1.50 <= TR_t/A_t <= 3.00
A_t/C_(t-1) >= 0.0035
```

The 35 bp ATR floor is cost-derived rather than an outcome percentile:
`3 * 35 bp = 105 bp`, more than four times the current 26 bp historical proxy
round trip. Volatility contributes no bullish or bearish vote.

## G3: participation-flow confirmation

From the final USD-M kline:

```text
Q_t  = quote_volume_t
TB_t = taker_buy_quote_volume_t
s_t  = (2*TB_t - Q_t) / Q_t
```

From exactly 288 prior calculation bars, excluding the anchor:

```text
m_s      = median(s_prior)
scale_s  = 1.4826 * MAD(s_prior)
z_s      = (s_t-m_s) / scale_s
activity = Q_t / median(Q_prior)
```

Zero `Q_t`, zero prior quote-volume median, or zero MAD is non-ready.

Long requires:

```text
s_t >= +0.20
z_s >= +2.00
activity >= 2.00
```

Short requires the exact sign mirror:

```text
s_t <= -0.20
z_s <= -2.00
activity >= 2.00
```

`abs(s_t) >= 0.20` means at least a 60/40 directional taker-quote split. This
entire calculation remains one participation group.

## Signal and alert semantics

`D1_FULL_SETUP` exists only when G1, G2, and G3 all pass. Partial combinations
may be retained as non-actionable diagnostics but are not variants. The system
must not display a passed-gate count as probability, and no score can promote a
failed conjunction.

## PAPER entry

After durable alert acknowledgement, evaluate at the existing PAPER FOK target
`ACK + 10,000 ms` for both frozen sizing cells, `100 USDT` and `1,000 USDT`.

- floor quantity to the common lot/market-lot grid;
- require the complete quantity and never admit a partial fill;
- apply the existing 50% visible-depth per-level haircut;
- causal mark age must be at most 2,000 ms;
- executable VWAP must remain inside the existing 10 bp price cap;
- additionally require `abs(VWAP-C_t) <= 0.50*A_t`;
- report no-fill in fill-rate and coverage, not as a zero-return episode;
- never place a production order.

Allow one active D1 position per symbol and no pyramiding. Another entry
requires a new first-cross after the prior position terminates.

## Exit and invalidation

Freeze `A_t`, `U_t`, and `L_t` at signal time. Anchor price thresholds to the
actual PAPER entry VWAP `E`. At each subsequent fully closed five-minute bar,
apply this priority:

1. required data or authority lost: mandatory PAPER exit and mark the interval
   inconclusive;
2. adverse close: long `C_j <= E-0.80*A_t`; short `C_j >= E+0.80*A_t`;
3. structural failure: long `C_j <= U_t`; short `C_j >= L_t`;
4. profit close: long `C_j >= E+3.00*A_t`; short `C_j <= E-3.00*A_t`;
5. hard horizon: exit after exactly 24 fully closed five-minute bars following
   entry.

Every exit uses another causal public-depth PAPER attempt. No trailing-stop,
RSI, opposite-signal, discretionary extension, or parameter-grid variant is
part of D1.

## Cost-survival geometry, not efficacy evidence

At the minimum admissible ATR, the gross target is 105 bp and the adverse
boundary is 28 bp. Under the current 26 bp historical proxy cost, the target
and adverse net values before funding are approximately +79 bp and -54 bp,
with binary break-even hit rate about 40.60%. Under the approximate 31 bp
1.5-fee cell they are +74 bp and -59 bp, with break-even hit rate about 44.36%.

This proves only that the payoff geometry can exceed costs. It does not prove
that the target is reached frequently enough. Exact public fees, public depth,
and settled funding remain mandatory.

## Data boundary and proxy rule

Metadata-only verification of the ten selected USD-M five-minute manifests
shows, for each target:

```text
first_open_time_ms = 1709251200000
last_close_time_ms = 1782863999999
row_count = 245376
gap_count = 0
missing_intervals = 0
```

This is 2024-03-01 through 2026-06-30 UTC.

- warm-up only: `[2024-03-01, 2024-07-01)`;
- outcome-readable development proxy: `[2024-07-01, 2026-07-01)`;
- no existing historical split may be relabeled untouched;
- no post-2026-07-01 outcome may be inspected during D1 implementation or
  data-only qualification;
- authoritative untouched interval: a future UTC-midnight `H_start` through
  exactly 365 calendar days, set only after code freeze and 30 consecutive
  data-only qualification days.

Historical files lack decision-time BBO/depth. A diagnostic proxy must enter at
`open(t+2)`, because `open(t+1)` precedes the `+2,001 ms` decision cutoff.
Closed-bar exits likewise use the first recorded open strictly after their
cutoff. Historical results must be labelled
`INCONCLUSIVE_NO_HISTORICAL_BBO`: they may reject D1 but cannot promote it.

### Outcome-blind implementation amendment A0

This amendment was fixed on 2026-07-21 before the development outcome runner
was allowed to open any row in the declared outcome-readable interval. It
resolves execution details that the prose above left implicit; it changes no
signal threshold, universe member, direction, target, stop, or horizon.

- The ten exact five-minute gzip files, their sidecar manifests, the ten
  authenticated one-hour gzip files and manifests, and the ten exact funding
  gzip files are byte-hash inputs to the development code freeze. Each
  five-minute input must contain exactly the declared 2024-03-01 through
  2026-06-30 UTC boundary (`245,376` rows), with at most `256 MiB` of
  decompressed CSV. An authenticated one-hour source may contain a finite
  pre-2024-03-01 prefix, but is capped at `30,000` rows and `64 MiB`
  decompressed; the runner selects the exact `20,448`-row declared-boundary
  subset and excludes every prefix row from every D1 feature and outcome
  input. No one-hour row after the declared boundary is permitted. Each
  funding input is capped at `10,000` rows and `16 MiB` decompressed. Within
  those rules, the runner rejects a different file, symbol, interval, request
  range, gap, duplicate, unclosed row, row-count overflow, decompressed-byte
  overflow, or disallowed boundary row.
- A historical row's synthetic `receipt_ms` and `data_through_ms` equal its
  exchange close timestamp. This is only a causal replay convention and is
  never described as observed local receipt evidence.
- `open(t+2)` is the entry reference price. The frozen 8 bp adverse slippage
  transforms it to the historical executable entry price: multiply by
  `1.0008` for a long and `0.9992` for a short. This transformed price is `E`
  for the exit rule and must itself satisfy `abs(E-C_t) <= 0.50*A_t`.
- The first post-entry exit observation is the fully closed bar whose open is
  the entry reference time. The 24th such bar is the hard-horizon observation.
  An exit decided from bar `j` executes at the first recorded open strictly
  after `close(j)+2,001 ms`, normally `open(j+2)`. The exit reference price is
  transformed by the same adverse 8 bp rule: multiply by `0.9992` for a long
  and `1.0008` for a short.
- One pending or active D1 position reserves its symbol. After an exit fills,
  the first newly eligible signal bar is the bar whose decision cutoff is
  strictly later than that fill timestamp. Signals suppressed while reserved
  are counted and are not zero-return episodes.
- Gross return uses the two unadjusted open proxies and the entry-reference
  denominator. Slippage is the difference between gross and the return from
  the two adverse executable prices. Public fee is exactly 5 bp of each
  executable notional, normalized by that same entry-reference denominator.
  Thus a zero-move round trip is exactly 26 bp; slippage is not subtracted a
  second time. The declared 1.5-fee stress cell uses exactly 7.5 bp per
  executable side and therefore exactly 31 bp at zero move. Both fee cells are
  evaluated from the same episode and share its statistical-unit ID.
- Settled funding uses the recorded public rate and mark price, normalized by
  the entry reference price. Positive rates debit longs and credit shorts.
  Only funding timestamps strictly inside `(entry_time, exit_time)` are used.
  Equality at either endpoint is an ordering ambiguity and makes that episode
  inconclusive rather than selecting a favorable convention.
- Funding coverage has a separate integrity screen. A file that represents the
  standard schedule must contain the complete exact eight-hour UTC grid
  (`timestamp mod 28,800,000 ms == 0`) throughout the outcome-readable
  development interval, including its required boundary grid slots; an
  expected missing timestamp fails coverage. Pre-development warm-up funding
  rows do not determine this outcome-interval screen. A nonstandard timestamp,
  adjusted cadence, or missing standard timestamp cannot prove completeness
  unless a separately frozen, independent schedule authority establishes the
  applicable adjusted schedule. With no such authority, all episodes for that
  symbol are labelled `FUNDING_COVERAGE_UNAVAILABLE`, remain inconclusive, and
  cannot contribute to a promoting screen. The hash-bound summary emits one
  fixed-order coverage status for every universe symbol even when that symbol
  produces no episode. This screen does not claim that the standard-grid check
  can establish completeness during an adjusted funding schedule; rates and
  marks are never interpolated or replaced.

For `s=+1` long and `s=-1` short, the exact Decimal calculation is:

```text
r_entry = open(t+2)
E       = r_entry * (1 + s*0.0008)
r_exit  = first open strictly after the exit-decision cutoff
X       = r_exit * (1 - s*0.0008)

gross       = s * (r_exit-r_entry) / r_entry
execution   = s * (X-E) / r_entry
slippage    = gross-execution
fee         = fee_rate_per_side * (E+X) / r_entry
funding     = sum(-s * rate_k * mark_k / r_entry)
net         = execution-fee+funding
```

Missing funding mark prices are not replaced by entry price; they make the
episode inconclusive. The primary reject rule below uses the 1.0-fee series.
The descriptive screen-pass state requires both the 1.0- and 1.5-fee series to
pass; nominal-size projections do not create additional return series.
- The `100 USDT` and `1,000 USDT` cells are deterministic PnL projections of
  the same percentage-return episode because historical depth is absent. They
  are not two independent observations and never duplicate statistical `N`.
- A cheap scan may omit a row only by proving a necessary D1 condition false
  (prior-channel break, direction-matched 60/40 taker split, 75% close
  location, and the `200,000 USDT` current-volume lower bound implied jointly
  by the frozen liquidity and activity gates). Every surviving row is passed
  through the sealed Decimal D1 evaluator, which remains final rule authority.
- Development artifacts report all exclusions, pending/active suppression,
  exit reasons, raw and after-cost returns, funding, both notional projections,
  and source/code hashes. They keep `probability_claim=false`,
  `efficacy_claim=false`, `prospective=false`, and
  `production_order_placement=false` regardless of the observed result.
- The development disposition is fixed before outcome access. Raw evaluable
  `N` counts the already non-pyramided, non-overlapping episodes within each
  symbol; simultaneous episodes in different symbols remain distinct in this
  raw count. A separate cross-symbol correlation guard selects from all raw
  evaluable episodes by deterministic earliest-exit interval scheduling: sort
  by exit time (then entry time, symbol, and statistical-unit ID for ties),
  select the earliest exit, and accept each later candidate only when
  `entry_time >= prior_selected_exit_time`. Fewer than 150 episodes selected by
  this global guard produces `INCONCLUSIVE_LOW_INFORMATION`. At a global-guard
  `N` of 150 or more, both a strictly negative primary after-cost mean and
  primary profit factor below 1.00 produce
  `RETROSPECTIVE_PROXY_REJECT`. A descriptive
  `RETROSPECTIVE_PROXY_SCREEN_PASS_INCONCLUSIVE` requires both raw per-symbol
  evaluable `N >= 500` and global-guard `N >= 150`, plus at least 100 long and
  100 short raw evaluable episodes, 45 active UTC days, positive total PnL,
  mean return at least 5 bp, profit factor at least 1.20, at least 6/10 symbols
  positive, positive PnL after removing the top three symbols, and positive
  PnL after removing the top ten episodes. Every other combination is
  `INCONCLUSIVE_MIXED_PROXY_EVIDENCE`. Even the descriptive screen-pass state
  cannot authorize deployment or replace the later bootstrap, BBO, coverage,
  concentration, quarter, and Holm gates.

## Analysis and success gates

Data-only qualification before `H_start` requires:

- 30 consecutive days;
- required-field availability at least 99.9%;
- actionability at least 99%;
- zero unresolved sequence gaps or schema shifts;
- no access to PnL or outcome labels.

The later, separately frozen prospective test requires exactly 365 calendar
days, at least 500 executed episodes, at least 150 non-overlapping episodes,
at least 45 active UTC days, at least 100 long and 100 short episodes, all four
quarters represented with at least three positive, and at least 6/10 symbols
positive. No symbol may contribute over 20% of positive PnL. Net PnL must
remain positive after removing the top three symbols and top ten trades.

Every `100/1,000 USDT x 1.0/1.5 fee` cell must pass all of:

- cumulative net PnL strictly above zero;
- mean net return at least 5 bp;
- synchronized seven-day, 20,000-resample one-sided block-bootstrap lower
  confidence bound above zero;
- point profit factor at least 1.20;
- block-bootstrap profit-factor lower bound at least 1.05.

D1 is one hypothesis. Long/short, assets, and size/fee cells are declared
strata or stress cells, not variants. If D1 is evaluated with A/B/C, the future
contract must apply Holm step-down across A/B/C/D1. The current A/B/C-only gate
cannot be silently reused. Any outcome-informed threshold, exit, direction,
endpoint, or exclusion change creates D2 and needs a strictly later untouched
attempt.

## Current blockers

- D1 feature documents are not yet bound to complete M0/M1/M2 authority.
- Historical BBO/full-depth execution does not exist.
- The current prospective efficacy contract contains only A/B/C.
- D1 therefore remains a disconnected shadow family and is not part of the
  active frozen prospective attempt.

## Expected artifacts before any run

| Artifact | Required property |
|---|---|
| D1 rule module and tests | exact Decimal arithmetic; positive, negative, and boundary cases |
| D1 canonical rule document | byte-stable hash included in a new code freeze |
| development runner | reads only the declared historical proxy interval |
| contamination manifest | proves no post-2026-07-01 outcome input |
| development result | may reject D1; cannot authorize prospective promotion |
| prospective capture qualification | 30-day outcome-blind coverage gate |
| prospective efficacy artifact | emitted exactly once after the fixed 365-day horizon |
