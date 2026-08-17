# Replay and outcome specification

## Purpose

Backtesting answers whether a recorded setup had useful forward excursion after realistic filtering. It must not convert rule strength into an assumed win probability, and it must not use future data while constructing a signal.

## Availability rules

- Candle-family rules may consume only `Candle.is_closed == true` observations.
- A higher-timeframe snapshot is available only when its `event_time_ms` is less than or equal to the primary feature timestamp.
- Range highs, range lows, and volume baselines exclude the signal candle.
- Recorded events are replayed in monotonically nondecreasing exchange-event time through `ReplayClock`.
- Out-of-order mini-ticker and aggregate-trade updates are ignored; duplicate closed candles and alert IDs are idempotent.
- No centered rolling windows, negative shifts, backfilled future values, or full-dataset scaling are permitted.

## Forward labels

For entry price `P0` and an observation horizon, the evaluator stores:

- MFE: maximum future high divided by `P0`, minus one.
- MAE: minimum future low divided by `P0`, minus one.
- Close return: final fully observed candle close divided by `P0`, minus one.
- Observed-until timestamp, so incomplete horizons can be identified.

Recommended horizons are 15 minutes, 1 hour, 4 hours, 12 hours, and 24 hours.
`RecommendationOutcomeEvaluator` provides the cost-aware layer for current alert
audits. It requires a complete, contiguous bar horizon, enters at the next bar
open, mirrors excursions for long and short directions, and applies round-trip
fees, adverse slippage, and settled Futures funding. Missing next bars, partial
horizons, and gaps are explicit exclusions rather than shortened observations.

## Current-alert audit contract

The alert-replay panel and the frozen strategy opportunity panel answer different
questions and must remain separate:

- Actionable audit index: a `CONFIRMED` transition.
- Informational pullback index: the first `SETUP` transition in an episode.
- `WATCH` is an early-warning diagnostic, not a headline recommendation.
- Primary horizon: 12 bars; secondary horizons: 3 and 6 bars.
- Path horizon: 72 bars with a 72-bar split-start purge/embargo.
- Primary hit margin: 5 bps after costs. Above +5 bps is `HIT`, below -5 bps
  is `MISS`, and the closed interval between them is `AMBIGUOUS`.
- Strict hit rate is `HIT / (HIT + MISS + AMBIGUOUS + UNEVALUABLE)`; resolved
  accuracy and coverage must be displayed beside it and never substituted for it.
- One-R target is based on the next-open entry and decision-time invalidation.
  Target and invalidation in the same OHLC candle are a `collision` because
  their intrabar order is unknowable.

Fixed-horizon alerts can overlap and therefore do not form a realizable equity
curve. Information-only returns are counterfactual expectancy, not trade P&L.
Spot short-direction results measure a decline/exit warning and must not be
described as executable spot-short profit. Historical rule replay is not the
same as auditing persisted, delivered Discord outbox events.

An alert replay covers only the assets declared by its backtest spec. A fixed
research universe must not be described as full parity with the live dynamic
top-N universe.

Uncertainty uses one shared circular UTC-calendar moving-block draw across
panels, with 7 days primary and 14/28-day sensitivity. A historical screen cannot
be called independently validated when the rule was designed after the tested
period; that claim requires post-freeze prospective shadow observations.

## Evaluation slices

Report at least signal family, stage, score bucket, symbol liquidity bucket, market, market regime, timeframe, calendar period, and long/short direction. Primary quality measures are precision at an explicit event threshold, median MFE, median MAE, MFE-to-absolute-MAE ratio, false alerts per day, and cost-adjusted expectancy.

## Independent verification

1. Replay recorded Binance payloads through the scanner.
2. Reproduce candle-family signals in the Freqtrade sidecar.
3. Run Freqtrade `lookahead-analysis` and `recursive-analysis`.
4. Compare timestamps, direction, and rule version rather than aggregate profit alone.
5. Keep a final chronological holdout untouched by rule or threshold selection.
