# Project Pro R4b handoff — implementation extract

Captured: 2026-07-17  
Conversation: https://chatgpt.com/c/6a595ea0-3e5c-83ee-8a20-8c7d679e43e4  
Web-AI session: `01KXPXQ2FJVT8MHN2EHFG1CPZ9`  
Answer UTF-8 character count: 53,377  
Answer SHA-256: `2d15198cedf2dfd7a64738fee248bc09d9de71afe790d382fa8ce71506efc095`

## Provenance boundary

The answer was generated in the user's signed-in ChatGPT project. The page and
session envelope visibly identified the selected mode as Pro, but the browser
automation model-picker probe could not independently verify a more specific
model/effort label. This extract therefore calls it the project Pro answer and
does not assert a hidden model identifier.

The full answer remains in the linked conversation. This file records only the
parts that govern implementation and the limitations that must accompany the
historical diagnostic.

## Main decision

R4a is a valid null for its narrow C0 universe. Its maximum predicted net return
was negative in both markets, so relaxing the probability threshold cannot fix
the economic failure. A successor must change the event mechanism, entry
location, direction coverage, and holding period. It must not reactivate the
retired G2/G4 volume gates or privilege RSI.

The Pro answer rejects ML selection for the primary R4b experiment and ranks
five deterministic hypotheses:

1. Spot cross-sectional relative-momentum long.
2. Futures higher-timeframe trend pullback/reclaim long.
3. Futures higher-timeframe downtrend rally/failure short.
4. Spot false-downside-break reclaim long.
5. A paired deterioration exit overlay for the identical H1 entries.

RSI, Stochastic RSI, VPCI, MACD/Force-Index divergence, fixed ADX thresholds,
Bollinger-squeeze variants, maker fills, historical order-book imbalance, and
parameter optimization are prohibited in the primary family.

## Frozen deterministic formulas

All decisions use fully closed 5-minute candles and strictly completed 1-hour
aggregates. Historical execution is the next 5-minute open plus frozen costs;
prospective execution must archive BBO and use the first executable quote.

### H1 — Spot relative momentum

- Formation return: `log(C[t-72] / C[t-2088])`, a seven-day formation window
  ending six hours before the decision.
- Subtract the contemporaneous eligible-universe median and compute the
  cross-sectional percentile rank.
- Trigger only when rank crosses from `<0.90` to `>=0.90`.
- Require the universe-median 24-hour return to be at or above the 50th
  percentile of its strictly prior 180-day empirical distribution.
- Require 48-hour realized-variance distance in basis points to be at least four
  times the ex-ante round-trip cost.
- Frozen stop: entry minus `1.5 * ATR288`.
- Exit after rank `<0.70`; thesis invalidation is rank `<0.50`.
- Timeout: 576 bars, or 48 hours.

### H2 — Futures pullback/reclaim long

- On completed 1-hour bars, trend score is
  `(EMA24 - EMA96) / ATR96`; require cross-sectional percentile `>=0.80`,
  rising EMA24 versus six hours earlier, and the cross-sectional median trend
  score at or above its strictly prior 60-day median.
- Tactical value score is `(close - VWMA48) / ATR48` on 5-minute bars.
- Trigger when prior z is in `[-1,0]`, current z is in `(0,1]`, and current
  close is above the previous bar's high.
- Stop: minimum low of the last six bars minus `0.25 * ATR48`.
- Target: maximum high of the prior 96 bars, excluding the current bar.
- Accept only if the favorable target is at least `1.5R` plus cost and its
  gross distance is at least three times estimated round-trip cost.
- Exit after two consecutive closes below VWMA48; timeout 72 bars.

### H3 — Futures rally/failure short

- Use the same 1-hour score; require percentile `<=0.20`, falling EMA24 versus
  six hours earlier, and the median trend score at or below its prior 60-day
  median.
- Trigger when prior z is in `[0,1]`, current z is in `[-1,0)`, and current
  close is below the previous bar's low.
- Stop: maximum high of the last six bars plus `0.25 * ATR48`.
- Target: minimum low of the prior 96 bars.
- Apply the same `1.5R + cost` and `3x cost` survival gates.
- Exit after two consecutive closes above VWMA48; timeout 72 bars.

### H4 — Spot false-downside-break reclaim

- Causal support excludes the most recent hour:
  `min(low[t-288:t-12])`, or indices t-288 through t-13 inclusive.
- At least one of t-2, t-1, t must penetrate support by an inclusive
  `[0.10,0.75] * ATR48`.
- Trigger only when the decision close is strictly above support and the prior
  bar high. Require the Spot 1-hour trend percentile to be `>0.20`.
- For deterministic implementation, the earliest qualifying penetration bar
  is j. Stop is `min(low[j:t]) - 0.25 * ATR48[t]`.
- Target is current VWMA96, subject to the same risk/cost-survival gates.
- Exit after two consecutive closes below frozen support; timeout 36 bars.

### H5 — identical-entry H1 exit overlay

H1 entries must be replayed under both the base exit and the overlay. The
overlay is inactive for the first 12 bars and then exits at the next open only
when all of the following hold: H1 relative rank `<0.50`, two consecutive
closes below VWMA48, and the broad-market regime percentile `<0.50`.

The paired mean improvement must exceed 2 bp with a one-sided block-bootstrap
lower bound above zero; p90 MAE must fall by at least 10%; and at least 75% of
the base policy's upper-decile return must remain. The combined H1+H5 policy
must still have positive after-cost expectancy.

## Statistical and execution boundary

The five hypotheses form one Holm-corrected family. The answer also requests a
complete trial ledger, within-R4b PBO, DSR, and Hansen SPA. Robustness variants
are veto tests only: a favorable neighboring parameter may not replace the
primary rule.

Primary prospective gates include at least 200 non-overlapping trades, at
least 90 UTC event days, mean after-cost return above 5 bp, one-sided 95% block
lower bound above zero, PF at least 1.10, stress survival, matched-random
superiority, concentration limits, and portfolio drawdown limits. A pass only
permits a later shadow/PAPER experiment; it does not authorize live trading.

## Local feasibility decision

The Pro specification's confirmatory universe requires daily point-in-time
exchange metadata, every eligible Spot/USDT and USDⓈ-M perpetual symbol,
historical BBO/depth, and a nine-month untouched prospective holdout. The local
archive instead contains eight fixed assets and kline/funding history through
2026-06-30. Therefore:

- the exact full-universe experiment cannot be claimed from the local archive;
- an eight-asset implementation is explicitly a historical, exposed-sample
  feasibility diagnostic;
- kline execution validity remains inconclusive;
- any passing rule must be frozen again before prospective public-BBO PAPER
  collection;
- no local historical result can be called actual realized profit.

C1 funding/basis carry remains a separate single-hypothesis family and is not
combined post hoc with H1--H5.
