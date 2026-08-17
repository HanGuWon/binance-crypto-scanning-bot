# Code Experiment Plan

## Material Passport

- Origin Skill: academic-research-suite / experiment-agent
- Origin Mode: plan
- Origin Date: 2026-07-20T00:00:00+09:00
- Verification Status: PREREGISTERED_UNVERIFIED
- Version Label: indicator_discriminator_v1

## Experiment Overview

- **Title**: Explainable pullback-signal discriminator on high-volatility Binance pairs
- **Objective**: Determine whether causal trend-quality, directional-momentum, participation,
  and pullback-contraction evidence separates the existing saturated 100-point alerts into a
  materially more accurate subset.
- **Hypothesis**: The frozen upper-quartile quality subset improves 60-minute direction-only
  accuracy by at least 3 percentage points over the unfiltered alerts while retaining at least
  20% of deduplicated alerts.
- **Type**: historical simulation and chronological validation

## Frozen Scope and Leakage Controls

- Assets: PENGU, BONK, ENA, WIF, FLOKI, ARB, OP, SEI.
- Base interval: 5 minutes; only fully closed candles through the decision candle may be read.
- Development: `[2024-07-01, 2025-03-01)` UTC. This is the only fit period.
- Validation: `[2025-03-01, 2025-11-01)` UTC. Apply the frozen transform and cutoff once.
- Retrospective stress: `[2025-11-01, 2026-07-01)` UTC. This interval has already been seen and
  may veto a rule but may not establish independent performance.
- The existing setup score is ignored because all indexed pullback alerts necessarily score 100.
- Spot/futures duplicates use `(asset, direction, decision_time_ms)` with spot priority.
- No search over periods, feature subsets, weights, or cutoffs is permitted in v1.

## Frozen Indicator Contract

The replay ledger stores these decision-time-only values:

1. `efficiency_ratio_20`: Kaufman ER over the latest 20 price changes.
2. `directional_di_balance`: recommendation-direction-aligned Wilder +DI/-DI balance.
3. `adx_delta`: current ADX minus previous ADX.
4. `directional_macd_delta_atr`: direction-aligned one-bar MACD-histogram change divided by ATR.
5. `directional_taker_delta`: direction-aligned three-bar closed-kline taker delta, with current
   closed-kline imbalance as an explicit fallback.
6. `volume_zscore`: point-in-time trailing volume surprise.
7. `pullback_range_contraction`: `1 - pullback_range_ratio`.
8. `pullback_volume_contraction`: `1 - pullback_quote_volume_ratio`.

For every field, the development empirical CDF maps larger-is-better raw values into `[0, 1]`.
The four equal-weight axes are:

- trend quality = mean(ER, directional DI balance, ADX delta)
- resumption = directional MACD delta / ATR
- participation = mean(directional taker delta, volume z-score)
- orderly pullback = mean(range contraction, quote-volume contraction)

`quality_score = 100 * mean(the four axes)`. The sole filter cutoff is the 75th percentile of
the complete-case development scores. This fitted CDF state and numeric cutoff must be persisted
and reused unchanged for validation and retrospective stress.

## Outcome Contract

- Primary horizon: 12 subsequent 5-minute candles (60 minutes).
- Reference: recommendation candle close, not the next-bar execution price.
- LONG is correct only when future close is strictly higher; SHORT only when strictly lower.
- A tie is incorrect. An unavailable or split-crossing horizon is excluded and coverage reported.
- Primary statistic: pooled strict direction accuracy.
- Secondary statistics: 15/30/360-minute accuracy, directional-return median, LONG/SHORT results,
  per-asset results, retention, and 7/14/28-day shared UTC block-bootstrap intervals.

## Historical Success Gate

All of the following must hold on validation; otherwise the score remains experimental and must
not suppress or promote live Discord alerts:

- filtered 60-minute accuracy >= 52.5%
- 7-day block-bootstrap 95% lower bound > 50%
- lift over the paired unfiltered baseline >= 3 percentage points and lift lower bound > 0
- filtered median direction-aligned return > 0
- LONG and SHORT accuracy each > 50%
- retention >= 20%, evaluable N >= 300
- positive lift in at least 6 of 8 assets

## Setup

- **Language/Framework**: Python 3.12, project `uv` environment
- **Working Directory**: `C:\Users\user\Documents\Binance bot-2`
- **Replay Commands** (run concurrently; each split receives the same 40-day causal warm-up):
  - `uv run signalbot backtest-alert-replay --config config/settings.example.yaml --spec config/backtest.5m.indicator-discriminator-v1.yaml --data-dir data/backtest --output-dir artifacts/backtest/2026-07-20-indicator-discriminator-v1/replay-development --split development`
  - `uv run signalbot backtest-alert-replay --config config/settings.example.yaml --spec config/backtest.5m.indicator-discriminator-v1.yaml --data-dir data/backtest --output-dir artifacts/backtest/2026-07-20-indicator-discriminator-v1/replay-validation --split validation`
  - `uv run signalbot backtest-alert-replay --config config/settings.example.yaml --spec config/backtest.5m.indicator-discriminator-v1.yaml --data-dir data/backtest --output-dir artifacts/backtest/2026-07-20-indicator-discriminator-v1/replay-retrospective --split retrospective_test`
- **Analysis Command**:
  `uv run python -m signalbot.backtest.indicator_analysis --replay-dir artifacts/backtest/2026-07-20-indicator-discriminator-v1/replay-development --replay-dir artifacts/backtest/2026-07-20-indicator-discriminator-v1/replay-validation --replay-dir artifacts/backtest/2026-07-20-indicator-discriminator-v1/replay-retrospective --output-dir artifacts/backtest/2026-07-20-indicator-discriminator-v1/analysis --assets PENGU BONK ENA WIF FLOKI ARB OP SEI --bootstrap-samples 10000 --bootstrap-block-days 7 --seed 20260720`
- **Dependencies**: locked by `uv.lock`
- **Environment**: Windows, CPU-only

## Expected Outputs

| Output | Path | Format | Success Criterion |
|---|---|---|---|
| enriched alert ledgers | `replay-*/recommendations.csv` | CSV | baseline event count unchanged; all experimental columns present |
| outcome ledgers | `replay-*/outcomes.csv` | CSV | 60-minute outcomes join one-to-one by event ID |
| fitted score contract | `analysis/fitted_score.json` | JSON | development CDF state and cutoff persisted |
| results | `analysis/results.json` | JSON | baseline/filtered metrics for all three chronological splits |
| Korean report | `analysis/report_ko.md` | Markdown | success gate and limitations explicitly stated |

## Monitoring Configuration

- **Timeout**: 120 minutes for replay; 15 minutes for analysis
- **Monitor files**: replay and analysis output directories plus command stdout
- **Experiment type override**: historical simulation
- **Metric file**: `analysis/results.json`
- **Metric key**: validation.filtered.horizon_60m.accuracy

## Analysis Plan

- **Primary metric**: validation 60-minute strict direction accuracy and paired lift.
- **Success threshold**: every item in the Historical Success Gate, not merely point accuracy.
- **Comparison**: the same deduplicated events before filtering.
- **Interpretation**: validation is historical reproduction, retrospective is exposed stress data,
  and neither is independently verified forward performance.
