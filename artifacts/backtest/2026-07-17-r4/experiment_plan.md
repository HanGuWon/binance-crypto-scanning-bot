# R4a selective forecast gate — frozen exploratory plan

## Scope and claim boundary

R4a is a fast, deterministic selection diagnostic over the immutable R3 C0
opportunity ledger. It does not regenerate candles, alter the R3 files, search
all bars, or add missing Spot-short/Futures-long candidates. It may therefore
support only the claim that causal context can or cannot select a better subset
of the existing Spot breakout-long and Futures breakdown-short events.

The full 2024-07-01 through 2026-07-01 interval has already been exposed. Every
R4a result is `EXPLORATORY_WALK_FORWARD`; no result may be called untouched OOS,
generalized, deployable, or a recommendation.

## Frozen target and features

The sole primary horizon is 12 closed 5-minute bars (60 minutes), entered at the
next contiguous 5-minute open. The binary target is aligned-direction realized
net return strictly above +5 bp after the frozen R3 fee, slippage, and funding
model.

The estimator may use only fields already present at decision time in the R3
ledger:

- cohort, regime, BTC trend, and strict-prior HTF acceptance;
- breadth ratio;
- taker delta over 3 and 12 bars;
- normalized VPCI, its signal, and its 3-bar slope.

Market-specific models are fitted with the same feature contract. Asset identity,
setup strength, outcome-derived ADX thresholds, RSI thresholds, breakdown fades,
future funding, future excursions, and all return fields are prohibited as
predictors. Fixed transforms are identity for bounded ratios/deltas and `tanh`
for normalized VPCI fields.

## Estimator and calibration

The frozen estimator is a standard-library Gaussian/categorical naive Bayes
classifier with Laplace alpha 1 and variance floor 1e-4. Each monthly fold uses
an expanding training window ending before a two-month calibration window. A
sigmoid calibrator selects one temperature and intercept from the frozen grids
by minimum calibration log loss. It is then applied to the next one-month test
window.

All train/calibration/test boundaries purge labels that reach the next window
and embargo the first 12 bars of the following window. Models are fitted
separately for Spot and Futures. Exact ties and non-finite or missing predictors
fail closed.

For the aligned direction, expected net return is the calibrated edge
probability weighted average of train-only class-conditional net means within
market/cohort, with a market fallback when a group has fewer than 100 rows.
Because the target returns are already net of costs, costs must not be subtracted
again.

## Frozen selection rule

An existing C0 event is selected only when both conditions hold:

1. calibrated probability of net return above +5 bp is at least 0.60; and
2. predicted expected net return is strictly above +5 bp.

Spot events remain long-or-abstain. Futures events remain short-or-abstain.
R4a may not reverse direction. Abstention is a first-class outcome, not a loss
or a hidden dropped row.

## Baselines and metrics

The primary baselines are the unfiltered legacy C0 panel, train-only prevalence
for Brier/log-loss, count-matched random alerts within month/market/cohort, and
the zero-action policy.

Report coverage, alerts/day, Brier score and skill, log loss, equal-count
10-bin ECE, realized gross/fee/slippage/funding/net, 2x-slippage net, win rate,
profit factor, UTC days, asset/regime breakdown, unconditional contribution per
raw candidate, positive-contribution concentration, and matched-random uplift.
Use shared-calendar circular 7-day block bootstrap for mean net and unconditional
contribution.

## Acceptance and stopping

Spot-long and Futures-short are judged separately. A family is exploratory-pass
only if it has at least 500 selected events on at least 120 UTC days, 2–30%
coverage, mean net above +5 bp, a one-sided 95% calendar-block lower bound above
zero, PF above 1.05, non-negative mean under 2x slippage, at least 6/8 positive
assets, no asset above 35% of positive contribution, positive unconditional
contribution with lower bound above zero, positive matched-random uplift with
lower bound above zero, Brier skill above zero, log loss below climatology, and
ECE no greater than 0.05. Familywise inference applies Holm adjustment across
the two R4a families.

The first frozen R4a failure stops threshold/feature retuning on this ledger.
Only a new preregistered experiment may follow. Even a pass cannot be promoted
to live alerts until R4b symmetric feature-panel replay, prospective public-BBO
shadow execution, and at least 120 untouched days with 500 events pass.

