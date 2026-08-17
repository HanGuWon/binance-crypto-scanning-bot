# Frozen 5m Volume Research Plan — R1

Status: **FROZEN BEFORE R1 CODE RESULTS AND BACKTEST OUTPUTS**  
Freeze date: 2026-07-15  
Research status: exploratory; the full 2024-07-01 through 2026-07-01 period has already been observed.

## Objective

Repair the confirmed gate-confirmation and closed-kline flow-parity defects, then test two
predeclared volume filters independently against one corrected price-trigger baseline. This study
is a falsification exercise. It is not evidence for production execution and cannot establish a
deployable edge without a later untouched prospective paper-alert period.

## Fixed panel and directions

- Assets: BTC, ETH, BNB, SOL, XRP, DOGE, SUI, WIF. No removal or replacement after results.
- Interval: fully closed 5m Binance candles only.
- Data warm-up: 2024-03-01T00:00:00Z onward.
- Evaluation: 2024-07-01T00:00:00Z through 2026-07-01T00:00:00Z.
- Spot: long entries only.
- USDⓈ-M futures: short entries only.
- Directions and venues are reported separately and never pooled.

## Shared corrected trigger (C0)

At closed candle `t`, the 20-bar high/low excludes `t`.

Long trigger requires all of:

1. `close[t] > max(high[t-20:t])` and `close[t-1] <= that prior high`;
2. EMA20 > EMA50;
3. MACD histogram > 0 and greater than its prior value;
4. ADX14 >= 20.

Short trigger uses the exact directional inverse. Squeeze and RSI-reversal families cannot create
entries in R1. Entry is the open of `t+1`; an absent or non-contiguous next bar cancels entry.
Eligibility gates may reject a trigger but must never overwrite or promote setup strength.
The alert state machine cooldown is not an execution filter in this research protocol: when flat,
every current `triggered && eligible` event may schedule the next-open entry. Cooldown remains only
an alert de-duplication concern.

## Frozen variants

| ID | Added condition | Primary contrast |
|---|---|---|
| C0 | none | corrected baseline |
| G2 | closed-5m kline taker delta, D12 direction-aligned and D3 >= 0.10 | G2 − C0 |
| G4 | directionally positive normalized VPCI, above signal, rising over 3 bars | G4 − C0 |

G2 and G4 are never combined in R1. No threshold search, sign inversion, alternate window, or
substitute indicator is permitted after seeing outcomes.

## Frozen exits and costs

- Initial technical invalidation/stop from the triggering rule.
- Trailing activation: 1R; trailing distance: 2 ATR.
- Trend-failure exit: 3 closed bars.
- Maximum holding: 72 bars.
- Opposite actual breakout event may schedule an exit; squeeze is not an opposite entry trigger.
- Fixed notional: 100 USDT per trade.
- Spot fee: 10 bps per side.
- Futures fee: 5 bps per side.
- Spot slippage: 5 bps per side for anchor/major, 10 bps for volatile.
- Futures slippage: 3 bps per side for anchor/major, 8 bps for volatile.
- Strict-prior funding remains included for futures. Funding-parity limitations are reported; R1
  does not use funding as an entry gate.
- Historical spread remains the explicit 11.25-bp proxy and never counts as observed book data.

Entry and exit rules are fixed together before any R1 output. Exit attribution is descriptive and
must not be used to tune exits in this study.

## Opportunity-panel estimands

Every actual C0 price trigger is recorded, including triggers rejected by G2/G4. The common key is
market, symbol, family, and decision close time. Each row records feature availability, values,
eligibility, next-open entry, and direction-adjusted close returns after 3, 12, and 72 bars:

`r_h = direction * (close[t+h] / open[t+1] - 1)`.

The primary predictive horizon is 12 bars (1 hour). Three and 72 bars are secondary. A label is
unavailable if the path has a gap, lacks the full horizon, crosses a declared chronological split,
or lies in the first 72 bars after a split boundary. MFE/MAE and executed-trade P&L are secondary.
Availability and exclusion reasons are computed independently for h3, h12, and h72; h72 future
availability must never select the h3 or h12 sample. The legacy `analysis_eligible` CSV field is an
explicit alias of `analysis_eligible_12`.

## Inference and multiplicity

- Resampling unit: complete UTC calendar days with all symbols retained.
- Moving-block bootstrap: 7-day primary blocks, 50,000 shared draws.
- Sensitivity block lengths: 14 and 28 days when computationally feasible.
- Four primary direction-specific contrasts: G2−C0 and G4−C0 for Spot-long and Futures-short.
- Simultaneous one-sided lower bounds use Bonferroni alpha `0.05 / 4` at minimum; a bootstrap
  fraction is never called a p-value or probability of future profitability.
- Report conditional accepted-opportunity means and zero-contribution common-panel uplift; do not
  call differently selected trades matched pairs.
- The 50,000 shared draws apply to the primary common-opportunity comparison. Each individual
  run's many descriptive trade-group CIs use 2,000 seeded draws; those groupwise intervals are not
  used for the feature-hypothesis stop/pass decision.

## Reproducibility requirements

- C0, G2, and G4 each run as A and B from unchanged inputs.
- `trades.csv`, `results.json`, `opportunities.csv`, and declared deterministic artifacts must be
  byte-identical within each A/B pair.
- Record code, config, plan, lockfile, and raw-data hashes.
- Required checks: Ruff, Pyright, pytest, and compileall.
- Preserve all previous 2026-07-15 artifacts; write only under `2026-07-15-volume-r1`.

## Stop/reject rules

R1 rejects a feature-direction hypothesis if any of the following holds:

1. feature availability is below 95% among otherwise valid C0 opportunities;
2. the multiplicity-adjusted lower bound for primary 12-bar common-panel uplift is not above zero;
3. base-cost net P&L is non-positive or mean net return is below +0.05% per executed trade;
4. profit factor is not above 1.05;
5. results depend on one symbol for more than 35% of uplift or fewer than six assets have positive
   point estimates;
6. deterministic replay, leakage, or parity checks fail.

Because the historical period is exposed, even a retrospective pass only promotes the feature to a
future paper-alert candidate. It does not authorize orders or live capital.

## Explicitly deferred

- Same-UTC-slot G1, efficiency G3, dry-up G5, and all combinations.
- CMF, MFI, OBV, VWAP, WVAD, TTI, AVSL, and adaptive threshold searches.
- Historical reconstruction of sub-minute trades, books, OI, liquidations, or top-trader data.
- Full persistent episode migration and live paper-position persistence.
- Any production order placement.
