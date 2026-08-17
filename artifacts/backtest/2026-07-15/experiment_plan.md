# Frozen 5-minute experiment plan — 2026-07-15

Status: frozen before data download or result inspection.

## Question

For an alert-first Binance scanner, what historical paper-trade return followed a
fully closed 5-minute technical setup when the bot predicted an upward move
(Spot long) or a downward move (USD-M futures short), and how did the result change
as public-data feature families were added?

## Fixed panel and time axis

- Anchors: BTC, ETH.
- Majors: BNB, SOL, XRP, DOGE.
- Volatile/current-listing examples: SUI, WIF.
- Data warm-up: 2024-03-01 through 2024-06-30 UTC.
- Evaluation: 2024-07-01 through 2026-06-30 UTC.
- Splits: development, validation, retrospective_test (eight months each).
- The prior 1h study exposed this calendar. `retrospective_test` is therefore not
  described as an untouched holdout.
- The fixed current-listings panel has survivorship and post-selection bias.

## Point-in-time protocol

- Signals use fully closed 5m bars only.
- 15m and 1h bars are gap-safe aggregates of 5m bars.
- A higher-timeframe close equal to the decision close is unavailable until the
  next 5m decision.
- Entry is the next available 5m open; any data gap cancels a pending entry.
- Historical spread is unavailable and is labelled as an 11.25 bps proxy. It earns
  only the minimum execution score; missing live books fail closed.
- Futures funding is joined strictly after its event timestamp. Missing is not zero.
- No production exchange order is sent.

## Strategy and exits

- Candidate families: squeeze long/short and range breakout/breakdown.
- RSI exhaustion/capitulation is disabled in the 5m headline and all primary
  ablations. It may be tested later only as an explicitly exploratory ablation.
- Independent entry gates: Trend >=60; Participation >=60; Crowding risk <75;
  Execution >=65; Completeness >=95.
- Position exits: initial invalidation/stop, three consecutive trend-failure closes,
  1R activation plus 2 ATR trailing stop, opposite confirmed signal, or 72 bars
  (six hours), whichever occurs first.

## Frozen ablations

- B0: price structure/trend only; participation, crowding and HTF gates pass through.
- B3: B0 plus kline volume/trade-count and taker/CVD participation.
- B2: B3 plus point-in-time funding crowding risk for futures.
- Headline: B2 plus strictly available 15m and 1h confirmation.
- Top-Trader ratios (B4) are prospective-only because no trustworthy historical
  archive was collected.
- Full order-book/liquidation features (B5) are prospective-only for the same reason.

## Costs and uncertainty

- 100 USDT notional per independent paper trade.
- Spot: 10 bps fee per side; 5 bps slippage per side for anchor/major and 10 bps
  for volatile names.
- Futures: 5 bps fee per side; 3 bps slippage per side for anchor/major and 8 bps
  for volatile names; realized funding included.
- Report gross and net, 0x/2x slippage, win rate, expectancy, profit factor, trade
  count, MFE/MAE, technical exit, asset, family and split.
- Seven-day moving-block bootstrap is primary; 14/28-day sensitivity is an audit.
- Interpret confidence intervals as historical dependence-aware uncertainty, not a
  future-return guarantee. No multiple-testing-adjusted discovery claim is allowed.

## Stop conditions

- Any manifest/request mismatch, future HTF row, unclosed bar, unreconciled gap,
  duplicate timestamp, non-deterministic rerun, or failed quality gate blocks a
  headline claim.
