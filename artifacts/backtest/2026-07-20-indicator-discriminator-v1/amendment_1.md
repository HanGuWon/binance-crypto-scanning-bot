# Preregistered input amendment 1

## Material Passport

- Origin: experiment-agent failure audit
- Created: 2026-07-20 after replay generation but before any direction outcome analysis
- Status: FROZEN_INPUT_CORRECTION
- Performance labels inspected before amendment: none

## Trigger

The original development replay stopped before producing an output because PENGU has no candles
in the 40-day warm-up preceding 2024-07-01. Validation and retrospective replay completed.

The completed ledgers also contained current actionable breakout/squeeze recommendations in
addition to the intended information-only pullback population. This is configuration scope, not
an indicator effect. In the retrospective ledger, the information-only pullback subset contains
exactly 4,385 events, matching the already-audited prior high-volatility pullback population.

## Frozen correction

1. Re-run development once with the same contract and seven available assets, omitting PENGU.
   PENGU would contribute zero development observations even if the replay loader tolerated an
   unlisted asset, so this does not remove an observed outcome.
2. Before fitting or reading outcomes, derive input ledgers containing only rows where:
   - `information_only == True`
   - `stage == setup`
   - `family` is `pullback_long` or `pullback_short`
   - `score == 100`
3. Retain all five outcome horizons for only those event IDs. The analysis module will still use
   the preregistered 12-bar endpoint.
4. No feature period, orientation, axis weight, cutoff, asset in validation/stress, metric, or
   success threshold changes.

## Corrected development command

`uv run signalbot backtest-alert-replay --config config/settings.example.yaml --spec config/backtest.5m.indicator-discriminator-v1-development-amendment.yaml --data-dir data/backtest --output-dir artifacts/backtest/2026-07-20-indicator-discriminator-v1/replay-development-amendment --split development`

## Derived-ledger directory names

- `pullback-development`
- `pullback-validation`
- `pullback-retrospective`

The derivation must assert that every selected recommendation has all four frozen score axes
available and that the source recommendation/outcome files remain unmodified.
