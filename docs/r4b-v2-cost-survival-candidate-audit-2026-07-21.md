# R4B V2 cost-survival candidate audit — 2026-07-21

## Decision

`NO_CANDIDATE`

No already-computed strategy candidate is eligible for prospective promotion.
All results in this audit are exposed historical diagnostics. They establish
neither predictive probability nor prospective efficacy.

The frozen historical execution approximation charges 5 bp fee plus 8 bp
adverse slippage per side. A zero-move round trip therefore costs 26 bp before
any applicable funding. The observed gross directional edge of the broad
three-family consensus is close to zero and does not survive that cost.

## Evidence inspected

- `artifacts/backtest/2026-07-20-historical-three-family-consensus-v1-census/consensus.csv`
- `artifacts/backtest/2026-07-20-historical-three-family-fixed-horizons-v2/fixed_horizon_outcomes.csv`
- `artifacts/backtest/2026-07-20-historical-three-family-fixed-horizons-v2/results.json`
- `artifacts/backtest/2026-07-20-historical-three-family-analysis-v2/bootstrap.json`
- `artifacts/backtest/2026-07-20-historical-three-family-te0-v2/technical_exit_te0.csv`
- `artifacts/backtest/2026-07-21-historical-three-family-frozen-walk-forward-v1/results.json`
- `artifacts/backtest/2026-07-21-historical-three-family-frozen-walk-forward-v1/predictions.csv`
- the default, block-14, and block-28 Indicator Discriminator analyses
- exposed pullback development, validation, and retrospective recommendation
  files used only to inspect their recorded regime fields
- the frozen consensus, outcome, evidence-score, and market-regime source code

The fixed-horizon sample contains only BONK, ENA, WIF, FLOKI, ARB, OP, and SEI.
It cannot establish generalization to BTC or ETH.

## Broad 3-of-3 consensus

Values are basis points. Confidence intervals are the existing 10,000-draw,
seven-UTC-day moving-block bootstrap intervals for mean net return. They are
below zero even before a multiplicity correction.

| Horizon | Side | n | Mean gross | Mean net | PF | Strict hit | Mean net 95% CI |
|---:|:---:|---:|---:|---:|---:|---:|:---|
| 5m | Long | 1,644 | -0.05 | -26.06 | 0.157 | 16.91% | [-28.23, -23.88] |
| 5m | Short | 2,082 | -0.55 | -26.55 | 0.139 | 16.19% | [-28.16, -24.96] |
| 15m | Long | 1,644 | +0.18 | -25.83 | 0.318 | 26.89% | [-29.10, -22.50] |
| 15m | Short | 2,082 | -0.09 | -26.09 | 0.313 | 25.31% | [-29.02, -23.30] |
| 30m | Long | 1,644 | +0.35 | -25.67 | 0.441 | 30.60% | [-30.53, -20.81] |
| 30m | Short | 2,082 | +1.43 | -24.57 | 0.436 | 31.99% | [-28.21, -21.09] |
| 60m | Long | 1,644 | +1.12 | -24.90 | 0.557 | 35.77% | [-31.25, -18.55] |
| 60m | Short | 2,081 | +0.85 | -25.18 | 0.542 | 37.05% | [-30.87, -19.53] |
| 360m | Long | 1,643 | -15.50 | -41.44 | 0.665 | 40.72% | [-59.45, -23.39] |
| 360m | Short | 2,080 | -2.21 | -28.52 | 0.757 | 44.71% | [-47.30, -9.64] |

## Rejected interpretations

### More agreeing indicators imply a better trade

The after-cost difference between broad 3-of-3 and conflicted 2-of-3 buckets
ranges from approximately -14.45 bp to +3.96 bp across side and horizon. Every
comparison interval crosses zero. Monotonic improvement with additional votes
is not established.

The Indicator Discriminator top quartile retained 799 observations. Its strict
directional accuracy was 48.69%, its uplift was +1.019 percentage points, and
the uplift 95% interval was [-2.005, +4.012] percentage points. Median
directional return was zero. The gate failed.

### Reverse every consensus direction

All ten side-by-horizon reversal cells remain negative after 26 bp cost. The
least negative cell, reversal of the 360-minute long consensus, has n=1,643,
mean gross +15.50 bp, mean net -10.51 bp, PF 0.901, and a 49.18% strict hit
rate.

The exposed 360-minute bearish conflicted 2-of-3 cell has n=19, mean net
+69.73 bp, and PF 2.909. It is below even a 30-observation screening floor and
is uncorrected for the search that discovered it. It is not a candidate.

### Existing technical exits or learned cost filter

The TE0 technical exit gives mean gross/net -1.00/-27.00 bp and PF 0.595 for
3-of-3 longs (n=1,643), and +2.40/-23.67 bp and PF 0.647 for 3-of-3 shorts
(n=2,082).

The 19-fold frozen walk-forward model, which already includes ATR cost
headroom and all three family strengths, produced n=2,869, mean net -25.53 bp,
PF 0.529, and a 36.67% strict hit rate. Every fold had negative aggregate PnL
and no fixed gate was selected.

The exposed consensus rows record `regime=neutral` and `btc_trend=neutral` for
100% of observations. They cannot support a post-hoc regime split. Applying
the already-recorded 0.55/0.45 breadth alignment still leaves the 60-minute
long/short means at -22.16/-28.27 bp and the 360-minute means at
-38.88/-29.06 bp.

## Development-only falsification specification

The following is not a promotion candidate. It is the single least-bad,
pre-declared falsification target available without adding another post-hoc
search dimension.

`DEV-C1_LONG_60_COST_STRENGTH`

- side: long only
- agreement: `BROAD_3_OF_3`
- fixed exit: 60 minutes
- `atr_fraction_micros >= 5200`, so one ATR is at least twice the 26 bp cost
- `abs(directional_agreement_micros) >= 500000`
- no asset exclusion and no threshold, side, or horizon changes

Its already-exposed result is n=241, mean gross +21.25 bp, mean net -4.74 bp,
PF 0.928, and strict hit 43.15%. Chronological slices deteriorate from
+15.14 bp in development to -11.47 bp in validation and -29.49 bp in
retrospective data. That deterioration is evidence against the hypothesis.

Any independent test of this specification must use previously unused
PAPER/BBO outcomes and require all of the following:

- at least 300 observations, at least 50 in each of three chronological
  subperiods, and at least five assets;
- no asset contributes more than 35% of observations or aggregate profit;
- a seven-UTC-day moving-block bootstrap;
- Holm family-wise error control at 5% over every candidate and endpoint
  actually inspected;
- adjusted one-sided lower confidence bounds above zero for mean net return
  and above one for profit factor under the 26 bp cell;
- a mean-net lower confidence bound above zero under a 34 bp cost stress;
- a new preregistration and fresh untouched sample after any threshold,
  horizon, direction, endpoint, or exclusion change.

No second development candidate is registered. Adding another weak hypothesis
would increase multiplicity without evidence that the 26 bp hurdle is
survivable.
