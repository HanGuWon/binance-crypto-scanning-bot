# Linked ChatGPT volume-analysis context

## Provenance

- Source conversation: `https://chatgpt.com/c/6a571ced-89b0-83ee-93d0-cf5488c49e8a`
- Retrieved read-only from the user's logged-in ChatGPT session on 2026-07-15.
- Conversation state: one user turn and one completed GPT-5.6 Pro response; not streaming.
- No follow-up was sent and the conversation was not modified.
- The user supplied a structurally damaged `Investing with Volume Analysis.pdf` and asked for a deep English analysis plus bot-ready implementations.
- The response explicitly said it reconstructed material from Pearson/CMT, academic, and official sources rather than claiming a complete direct reading of the damaged book.
- This file is a faithful structured extraction for Oracle review. It distinguishes hypotheses and generated code from measured evidence.

## Downloaded artifacts and hashes

| Artifact | Bytes | SHA-256 | Validation |
|---|---:|---|---|
| `C:/Users/user/Downloads/Investing_with_Volume_Analysis_Detailed_Report_EN.pdf` | 2,517,271 | `f3a5f01ded07267eab16337d42dae7372b3a4e9f492abae4b2032815ef3237af` | downloaded report |
| `C:/Users/user/Downloads/Investing_with_Volume_Analysis_Detailed_Report_EN.docx` | 1,404,185 | `44269d5f828dd9567e70a0aace0baea9496584449efb7b75e94264b5b1d45a36` | 2,377 non-empty paragraphs extracted |
| `C:/Users/user/Downloads/volume_analysis.py` | 38,894 | `b5b7d82064484024d997fd85601f4eb38ec52c524faf7b80f934ec32359a6ece` | Python AST parse passed |
| `C:/Users/user/Downloads/strategy.py` | 3,542 | `b96f4c9b8b12b22c431739e430882e8b3df6017d5465b954a0efe5808c8982f7` | Python AST parse passed |
| `C:/Users/user/Downloads/README(1).md` | 7,763 | `179a14e1a8303844022d7ed0edfa8b7e1333cea91b60db7b014494ca8357f50c` | text inspected |
| `C:/Users/user/Downloads/SOURCES.md` | 3,516 | `1c1dae62170134208faf63e7e4d213f51ec4e37a8d1df8b518e3ae23af815538` | text inspected |

The original damaged PDF mentioned in the conversation was 13,018,434 bytes and had SHA-256 beginning `84ad963e` and ending `b1228c7` in the provider report. It is not treated as a machine-readable source here.

## Core price-volume interpretation

The response used a four-state descriptive framework:

1. Price up + volume up: demand participation confirms the move.
2. Price up + volume down: participation is weakening.
3. Price down + volume up: supply participation confirms the decline.
4. Price down + volume down: selling pressure may be weakening.

It repeatedly warned that these are not standalone trade commands. High volume late in a mature trend can be climax rather than continuation. High volume with little price progress can be absorption or churn.

## Exact formulas and implementation notes

### Relative volume

`RVOL_t = V_t / baseline(V_{t-1}, ..., V_{t-n})`

- The current candle is excluded from the baseline.
- Downloaded code default: `n=20`, median baseline; mean is optional.
- The code also computes a prior-only z-score of `log1p(volume)` with default length 63.
- Proposed crypto comparison input: quote-notional volume or a clearly declared single volume type, normalized per symbol and venue.

### VPCI

The downloaded code and report define:

- `VPC = VWMA(close, volume, L) - SMA(close, L)`
- `VPR = VWMA(close, volume, S) / SMA(close, S)`
- `VM = SMA(volume, S) / SMA(volume, L)`
- `VPCI = VPC * VPR * VM`
- `signal = VWMA(VPCI, volume, S)`

Default windows are `S=5`, `L=20`.

Important: the web response's rendered fraction appeared inverted. The downloaded code formula above is the intended definition.

VPCI has price units, so a common absolute threshold across assets is invalid. Suggested normalizations were `VPCI / close`, `VPCI / ATR`, or a prior-only z-score/percentile of `VPCI / close`, with one example using a 126-bar z-score. Preserve the states `VPCI > 0`, `VPCI > signal`, and positive slope separately rather than collapsing them into one opaque number.

### Additional candidates

- `VWMA20 - SMA20`
- volume-weighted MACD with `(12, 26, 9)`, requiring an equal-window ordinary MACD ablation
- CMF20; ±0.10 was presented only as a reference line, not a universal threshold
- MFI14
- OBV or VPT slope/divergence
- WVAD and close-location multiplied by volume
- five-bar price-volume phase
- 63-bar volume percentile
- volume acceleration and volatility-normalized volume

Do not stack several cumulative signed-volume indicators because they are likely redundant.

### Bar-state hypotheses

The provider proposed exploratory reference values, explicitly not established truths:

- true-range percentile at least 0.80
- RVOL at least 1.50
- close location at least 0.75 or at most 0.25
- 63-bar volume percentile high at least 0.70, low at most 0.30

An absorption candidate was high RVOL plus narrow normalized range plus extreme rejection, followed by a separate confirmation such as reclaim or event-high break. A bullish example used close location at least 0.65 before the confirmation stage.

### TTI and AVSL cautions

The supplied Trend Thrust Indicator was an engineering interpretation rather than a verified exact replication. Its defaults were 12/26/9, volume multiple `SMA(volume,12)/SMA(volume,26)`, clipped to 0.5–2.0, enhanced fast `FastVWMA * VM^2`, enhanced slow `SlowVWMA * (1/VM)^2`, and spread fast minus slow. Sign wording was inconsistent across artifacts. It requires separate normalization and ablation if considered at all.

The printed AVSL formula was rejected because it can diverge around `VPC≈0` and has unit/sign ambiguity. The code's AVSL-inspired version is a different hypothesis: ATR14, structural low10, base 2.5 ATR, `tanh(VPCI / prior_sigma)` adjustment 0.75, clamp 1.25–4.0, monotonic long stop. Another report example used normalized VPCI state with 1.5–3.5 ATR and 10–40-bar lookbacks. These are not the same method and must not be blended or substituted post hoc.

## Generated strategy templates

These are hypotheses, not demonstrated profitable strategies.

### Breakout

- Prior high lookback 55, excluding the current candle.
- Close above SMA100.
- RVOL20 at least 1.5.
- VPCI positive, above signal, and rising.
- Signal only after candle close; earliest fill is next candle open.
- Suggested exits: close below SMA20, VPCI negative and below signal, ATR/structure trail, or 60-bar maximum holding.
- This should be tested as an independent candidate and not copied wholesale into the current bot.

### Low-volume pullback

- Established uptrend.
- Prior impulse occurred on above-normal volume.
- Pullback has RVOL no more than 0.85 or declining volume and range contraction.
- Trigger only when close breaks a previously known pullback high and VPCI improves or exceeds signal.
- Do not use a future-confirmed pullback low to define the entry.

### Absorption reversal

- Extreme RVOL.
- Directional attempt into support/resistance.
- Poor price progress or rejection.
- Mandatory second-stage confirmation such as reclaim or event-high break.

### VPCI regime confidence

- Full confidence if VPCI is positive and above signal.
- Reduced confidence if positive but below signal.
- Zero only if VPCI is negative and price trend is weak.
- For this alert-only project, use alert confidence/gates rather than order sizing.

## Binance 5-minute adaptation

- Spot crypto has no consolidated tape. Preserve venue, symbol, spot/perpetual market, base volume, quote notional, and trade count separately.
- Declare one volume type for exact VPCI replication. Use per-symbol RVOL or quote-notional percentile for cross-asset activity comparison.
- Crypto is 24/7. Use UTC for deterministic boundaries and run sensitivity checks.
- Replace equity-market open baselines with hour-of-week or exact 5-minute-slot historical baselines made only from completed prior candles.
- Keep local rolling RVOL and same-slot seasonal RVOL as separate features.
- Perpetual-futures volume alone is incomplete. Funding, OI change, liquidations, basis, mark-index deviation, and venue concentration belong to separate crowding/risk features.
- The current archive lacks reliable OI and liquidation history. Do not infer or backfill them from candles.
- Use the current completed candle over previous completed candles, next-5m-open fills, and fully completed higher-timeframe candles only.

## Proposed research design in the linked response

1. Hold exits and sizing fixed.
2. Compare price-only, price + RVOL, price + VPCI, and price + RVOL + VPCI.
3. Only afterward evaluate VWMA/VW-MACD/CMF/effort-result state one at a time.
4. Run an event study before another trading backtest: forward returns 1/5/10/20/60 bars after the completed signal for price breakout only, +RVOL, +VPCI, and both, with matched trend/volatility/liquidity controls.
5. Use walk-forward purge/embargo, one-bar execution delay, fees, funding, slippage, time-block bootstrap, price-only control, and multiple-testing correction.
6. Stress by removing the best 5/10 trades, applying a volume-capacity assumption, and testing time-shift or sign-flip placebos.

The response listed a sensitivity grid — VPCI short `{3,5,7,10}`, long `{15,20,30,40}`, RVOL `{1.2,1.5,2.0}`, breakout `{20,40,55,100}`, exit MA `{10,20,40}`, ATR multiplier `{1.5,2,2.5,3}` — but explicitly prohibited exhaustive combination optimization and recommended looking for a broad plateau. Because the current project has already exposed its final period, any result on the same calendar remains exploratory and requires a future paper-trading holdout.

## Reliability limitations and contradictions

- The toolkit includes no actual live-market performance evidence. Its synthetic example backtest loses money.
- Historical SPY VPCI figures such as profit factor 2.47 for 1993–2006 were author-reported, not a modern independent reproduction with current costs and dividends.
- Test-count claims conflict: final summary/chat says 17 passed while report remnants say 11 or 11/16.
- Pine output was not compiled on the hosted platform.
- TTI and AVSL are engineering interpretations, not established exact formulas.
- The artifacts contain Alpaca/CCXT adapters and live-lock concepts. They must not be imported into this repository because the project is alert-first and prohibits production order placement.

## Questions Oracle must resolve

1. Does the current negative gross edge make indicator addition scientifically unjustified without first tightening the setup/event definition?
2. Which one or two volume features are least redundant with the existing activity/taker/CVD inputs?
3. Should participation use quote volume rather than base volume, and should hour-of-week seasonality be removed before normalization?
4. Is normalized VPCI a useful price-volume response slot or merely a nonlinear re-expression of moving-average trend and volume activity?
5. Which candidate features can be computed identically in live and backtest paths from closed Binance klines?
6. What minimal preregistered ablation can reject the volume hypothesis without another search over exposed data?
