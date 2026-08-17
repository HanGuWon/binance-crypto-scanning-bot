# Indicator Discriminator V1A Amendment 1 — One-R Float Boundary Validator

- Status: `FROZEN_BEFORE_AMENDMENT_1_REPLAY`
- Parent freeze SHA-256: `00279835ad6cbaf6c7d88f5c8aca77c49bea6c88339fe9427fb74fdae239e15e`
- Scope: historical-only validator correction; no signal, score, selection, exit,
  cost, or efficacy-rule change

## Trigger and exposure

The three parent-frozen replays completed without stderr. Before model fitting,
score selection, bootstrap inference, or any efficacy result was published, the
V1A analyzer stopped on development `outcomes.csv` line 2808, event
`030615b50ac016602612bc90`. The producer recorded `timeout` with
`maximum_rise = 0.004563786881766063` and
`one_r_risk_fraction = 0.0045637868817661655`. The validator's one-sided
lowered tolerance converted this near-but-below excursion into a touch and
therefore rejected a producer-valid timeout.

The integrity-only diagnosis read recommendation identity, direction, one-R
status, extrema, and risk fields, but did not read or compare net returns,
score quartiles, selected-population performance, bootstrap results, or gate
outcomes. The failed analysis target was never created. A full boundary scan
found the same old-validator false positive in 44 development rows from 34
events, 56 validation rows from 44 events, and 178 retrospective rows from 118
events. All 278 rows and 196 events are outside the frozen V1A analysis
population.

The parent evidence remains immutable:

- development run-manifest SHA-256:
  `45ba9c1897eb27f7bfdffe4c04b2b9a80c78ea57391f27f51846b9fde3a35b47`
- validation run-manifest SHA-256:
  `4feb9d16b7759262c39c90caf4a92838bc4328f2c00e7fd7bc9420da29c21b9b`
- retrospective run-manifest SHA-256:
  `9f6ee790b01d0fa2eeb08de97ad23d7035e04f8d2290f0463492554ae3054d2c`

## Frozen correction

The validator now maps the direction-adjusted target and invalidation
excursions to a three-state relation against the risk fraction recomputed from
authenticated recommendation metadata and entry price:

```text
0  when math.isclose(excursion, expected risk, rel_tol=1e-12, abs_tol=1e-12)
+1 when excursion is above that boundary
-1 when excursion is below that boundary
```

`target_first` requires target relation `>= 0`; `invalidation_first` requires
invalidation relation `>= 0`; `collision` requires both relations `>= 0`; and
`timeout` requires both relations `<= 0`. The zero state deliberately supports
either producer result because serialized excursion ratios cannot preserve the
exact raw-price comparison bit at a floating boundary. Geometry, timestamp,
collision-close, costs, path containment, and every other validator remain
unchanged.

The exact regression boundary, the inverse positive-status boundary, both
sides of the tolerance band, and collision support have positive, negative,
and boundary tests. A read-only audit confirmed that these semantics reconcile
all 658,380 valid one-R rows in the three parent replays; the old rule rejects
278 valid timeouts, while strict ratio comparison rejects 1,777 valid positive
statuses.

## Unchanged experiment authority

The ordered seven-asset universe, spot/futures markets, three chronological
splits, closed 5-minute candles, population predicate, eight features, four
equal-weight axes, development-only ECDF fit, type-7 quartile cutoff,
5/15/30/60/360-minute horizons, fee/slippage/funding model, deduplication,
bootstrap schedule, and twelve-condition historical gate are unchanged. This
amendment cannot alter recommendations or outcomes.

Because the repository source digest changes, the parent replay outputs are
retained only as immutable diagnostic evidence and are not analyzed under the
amended freeze. All three splits must be replayed into new `*-amendment-1`
directories. Before analysis, every amended `recommendations.csv` and
`outcomes.csv` must be byte-identical to its parent replay. Any difference
halts the amendment. Runtime-bearing `results.json` is compared semantically
with timing fields excluded.

The amendment freeze and every later artifact retain
`historical_only: true`, `external_anchor: false`,
`probability_calibrated: false`, and `deployment_approved: false`. No
independent validation, live promotion, order execution, or future-profit
claim is permitted.

## Amended commands

```powershell
uv run signalbot backtest-alert-replay --config config/settings.example.yaml --spec config/backtest.5m.indicator-discriminator-v1a-7asset.yaml --data-dir data/backtest --output-dir artifacts/backtest/2026-07-20-indicator-discriminator-v1a-7asset/replay-development-amendment-1 --split development
uv run signalbot backtest-alert-replay --config config/settings.example.yaml --spec config/backtest.5m.indicator-discriminator-v1a-7asset.yaml --data-dir data/backtest --output-dir artifacts/backtest/2026-07-20-indicator-discriminator-v1a-7asset/replay-validation-amendment-1 --split validation
uv run signalbot backtest-alert-replay --config config/settings.example.yaml --spec config/backtest.5m.indicator-discriminator-v1a-7asset.yaml --data-dir data/backtest --output-dir artifacts/backtest/2026-07-20-indicator-discriminator-v1a-7asset/replay-retrospective-amendment-1 --split retrospective_test
uv run python -m signalbot.backtest.indicator_analysis_v1a --freeze-manifest artifacts/backtest/2026-07-20-indicator-discriminator-v1a-7asset/amendment_1_freeze.json --replay-dir artifacts/backtest/2026-07-20-indicator-discriminator-v1a-7asset/replay-development-amendment-1 --replay-dir artifacts/backtest/2026-07-20-indicator-discriminator-v1a-7asset/replay-validation-amendment-1 --replay-dir artifacts/backtest/2026-07-20-indicator-discriminator-v1a-7asset/replay-retrospective-amendment-1 --output-dir artifacts/backtest/2026-07-20-indicator-discriminator-v1a-7asset/analysis-amendment-1 --samples 10000 --seed 20260720
```

If another integrity defect appears, Amendment 1 is not edited after its new
freeze. Work stops and any correction requires a separately disclosed
Amendment 2.
