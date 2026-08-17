# A. Executive verdict

## Overall decision

**Reject the current rule version as a scientifically valid live-equivalent trading signal. Retain the substantive conclusion that the current strategy has no demonstrated economic edge.**

That distinction matters:

1. **The negative-edge conclusion is probably directionally correct.** Spot-long gross performance is already negative, and futures-short gross performance is effectively zero. Eliminating slippage does not restore profitability. No defect found plausibly converts the reported results into a strong positive gross edge.

2. **The precise trade counts, gate effects, funding effect, confirmation semantics, and live/backtest-parity claims are not reliable.** The most serious defect promotes every nonzero candidate that passes the gates directly to the confirmation threshold. For gated breakout/breakdown rules, this promotion is not incidental: their maximum raw score is only 65, below the configured confirmation threshold of 80.

3. **Costs amplify the failure; they do not cause it.** On the reported Headline runs:

   * Spot-long gross expectancy is approximately **−1.14 bp per trade**, while the total cost drag is approximately **32.19 bp per trade**.
   * Futures-short gross expectancy is approximately **+0.16 bp per trade**, while the total cost drag is approximately **18.50 bp per trade**.
   * At unchanged turnover and costs, an amendment must improve average returns by approximately **33.33 bp per spot trade** and **18.33 bp per futures trade merely to reach net break-even**.

4. **The primary diagnosis is weak/non-predictive entry selection combined with permissive confirmation semantics and high turnover.** Participation features are overlapping, poorly normalized for crypto time-of-week effects, and defined differently historically and live. That makes them scientifically weak, but there is no evidence that replacing them with more volume indicators will generate enough alpha to meet the economic hurdle.

5. **The next change should be architectural, not an indicator expansion:** one shared point-in-time feature assembler, explicit availability states, non-promoting gates, an episode-based signal state machine, and actual live paper-position parity.

6. **No currently reviewed volume indicator is likely, by itself, to produce an economically meaningful edge.** A small frozen test of seasonal quote-volume surprise, taker delta, price-volume efficiency, normalized VPCI, and dry-up/trigger sequencing is defensible only after the P0 corrections below.

## Provenance assessment

**Observed:** the repository source digest recomputes to the digest reported in the supplied artifacts. The linked-context file and the downloaded `volume_analysis.py`, `strategy.py`, and `SOURCES.md` also match their stated hashes.

**Limitation:** the bundle does not contain the original `uv.lock`, the linked `README(1).md`, or the raw `trades.csv`/`results.json` outputs for all B0/B3/B2/Headline A/B runs. It therefore supports provenance and deterministic-code claims but does **not** permit independent end-to-end reproduction of the reported metrics or byte-identical rerun claim.

The linked conversation extraction and downloaded scripts are treated as **hypotheses and reference implementations**, not empirical evidence. Recommendations below marked **†** originate materially from that linked context.

---

# B. Severity-ranked confirmed defects and risks with exact file/function references

## B1. Confirmed defects

| Rank | Severity        | Confirmed defect                                                                                                   | Exact references                                                                                                                                                                                                                                                                                                                                    | Consequence and likely bias                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    |
| ---: | --------------- | ------------------------------------------------------------------------------------------------------------------ | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
|    1 | **Critical**    | **Passing the gates promotes any nonzero setup directly to the confirmation score.**                               | `src/signalbot/signals/rules.py`, `SignalRuleEngine._apply_gate()`, lines 47–77, especially line 72. `SignalStateMachine._desired()`, `state_machine.py:43–50`.                                                                                                                                                                                     | A raw score of 30, 60, or 65 becomes 80 and therefore `CONFIRMED`. The score no longer represents setup strength. This bypasses `WATCH` and `SETUP`, inflates confirmation counts, and makes many candidates tie at 80. Likely increases weak entries and turnover, but the net bias can be either direction because tie selection can also suppress better candidates.                                                                                                                                                                                        |
|    2 | **Critical**    | **Gated breakout/breakdown cannot reach confirmation on their own.**                                               | `rules.py`, `_breakout()`, lines 188–225; `_breakdown()`, lines 227–264.                                                                                                                                                                                                                                                                            | With gates enabled, volume, taker-flow, and regime points are disabled. The maximum raw breakout score is 65: break 30 + MACD 15 + ADX 10 + EMA 10. Thus every gated breakout confirmation depends on the hidden score promotion. Removing line 72 without redesign would eliminate all breakout confirmations rather than restore intended semantics.                                                                                                                                                                                                         |
|    3 | **High**        | **The squeeze setup can be confirmed before any breakout.**                                                        | `rules.py`, `_squeeze()`, lines 140–186.                                                                                                                                                                                                                                                                                                            | Compression plus proximity to a range boundary gives a mandatory raw score of 60. Gate passage promotes that to 80 even though price has not crossed the boundary. The system therefore treats a pre-breakout condition as a confirmed directional entry. This is a strong candidate for excessive low-quality entries.                                                                                                                                                                                                                                        |
|    4 | **High**        | **The claimed live/backtest exit policy is not actually shared with live operation.**                              | `src/signalbot/signals/positions.py`, `TechnicalExitEngine`, especially lines 56–61. Repository usage shows `PaperPosition` and `TechnicalExitEngine` are used by `src/signalbot/backtest/engine.py`, not by `MarketRuntime`.                                                                                                                       | Live alerts do not operate the backtested initial stop, 1R trailing activation, three-bar trend failure, time exit, or position-level opposite-signal lifecycle. The docstring’s live/paper/backtest-parity claim is false. Entry alerts may still operate, but the backtest is not a replay of the deployed alert lifecycle.                                                                                                                                                                                                                                  |
|    5 | **High**        | **Historical and live funding snapshots implement different availability rules.**                                  | Live: `src/signalbot/data/funding.py`, funding snapshot logic, lines 137–175; `runtime.py:226–238`. Historical: `src/signalbot/backtest/engine.py`, historical funding construction, lines 561–603.                                                                                                                                                 | Live requires strict-prior data, minimum history, configured lookback, and freshness. Historical code hard-codes 30 days, returns a neutral z-score when history is immature or variance is zero, and carries the last observation indefinitely without a freshness limit. B2 and Headline futures results are not live-equivalent. Bias can be positive or negative. Strict-prior timestamp selection itself is correct. Binance’s funding endpoint supplies event timestamps, rates, and mark prices suitable for a strict-prior join. ([Binance 개발자 센터][1]) |
|    6 | **High**        | **Missing or invalid inputs are often converted to neutral numeric values.**                                       | `src/signalbot/domain/models.py`: `FrozenModel` lines 11–12; `Candle` defaults around lines 31–46; `FeatureSnapshot` defaults lines 151–160. `src/signalbot/exchange/binance/schemas.py`, `_parse_kline()`, lines 73–96. `src/signalbot/indicators/core.py`, `_point_in_time_zscore()` lines 183–190, RVOL logic around 375–376, CVD logic 409–415. | Missing quote volume, trade count, taker volume, immature z-scores, zero variance, and unavailable denominators can look like valid neutral observations. This violates the invariant that missing or stale data must not silently become zero. Binance kline responses explicitly contain quote volume, trade count, taker-buy base volume, and taker-buy quote volume; these should be required validated fields, not optional zero defaults. ([Binance 개발자 센터][2])                                                                                          |
|    7 | **High**        | **Completeness is synthetic and does not measure component-level validity.**                                       | `indicators/core.py`, completeness construction around lines 453–458; historical order-flow construction in `backtest/engine.py:176–189`.                                                                                                                                                                                                           | The score is effectively 70 base points, 20 for flow availability, and 10 for spread. Historical flow is always marked available and the spread proxy is always present, so historical completeness is mechanically 100 rather than a point-in-time audit of every required input. The `Completeness >= 95` gate therefore adds no historical discrimination.                                                                                                                                                                                                  |
|    8 | **High**        | **Historical and live participation use different information horizons and definitions.**                          | Live: `src/signalbot/runtime.py`, recent-flow extraction around lines 204–207. Historical: `backtest/engine.py:176–189`. Report admission: `artifacts/backtest/2026-07-15/final_report.md:117–123`.                                                                                                                                                 | Live uses roughly the most recent 60 seconds of `aggTrade` flow, while the backtest uses full closed-5m kline taker volume. The historical B3/B2/Headline policies do not test the live participation gate. The main comparable path should use the same closed-5m kline fields in live and historical operation; sub-minute flow must be a separate prospective feature family.                                                                                                                                                                               |
|    9 | **High**        | **Book timestamps conflate source time, receive time, and decision time.**                                         | `runtime.py:199–214`, especially assignment of `as_of_ms=self.clock.now_ms()`; `src/signalbot/data/microstructure.py`, book storage around lines 67–96.                                                                                                                                                                                             | Only the latest book is retained. There is no `last_at_or_before(cutoff)` lookup and no reliable source-versus-receive-time separation. A delayed closed-candle decision can therefore attach a book observed after the candle close while labeling it as contemporaneous decision evidence. That is not necessarily future access relative to actual execution time, but it is timestamp-ambiguous, irreproducible, and not replay-equivalent.                                                                                                                |
|   10 | **High**        | **The state machine has incomplete episode and invalidation semantics.**                                           | `src/signalbot/signals/state_machine.py`, `SignalStateMachine.process()`, lines 23–41.                                                                                                                                                                                                                                                              | `WATCH`/`SETUP → IDLE` emits `INVALIDATED`, while `CONFIRMED → IDLE` silently disappears. Direct `IDLE → CONFIRMED` is allowed. Cooldown can suppress valid lifecycle changes. State is in-memory only, so restart can re-alert a continuing setup. The event ID is deterministic for a transition timestamp, but it is not a persistent setup/episode identity.                                                                                                                                                                                               |
|   11 | **High**        | **Not every emitted decision has an invalidation condition.**                                                      | `rules.py`, `_idle()` lines 104–113; `state_machine.py:29–32`; `src/signalbot/data/anomaly.py`, warning construction around lines 79–95; `domain/models.py`, `RuleEvaluation.invalidation` and `SignalDecision.invalidation` are optional.                                                                                                          | An invalidated state can inherit an evaluation with `invalidation=None`; anomaly decisions also have no invalidation or expiry. This directly conflicts with the project invariant that every decision must state invalidation. For informational warnings, an explicit expiry/resolution criterion is required rather than a price stop.                                                                                                                                                                                                                      |
|   12 | **High**        | **The “opposite signal” exit reacts only to a newly emitted opposite confirmation transition.**                    | `backtest/engine.py`, decision collection and opposite-signal check around lines 451–486. `state_machine.py:36–37`.                                                                                                                                                                                                                                 | A still-active opposite confirmed condition produces no new decision and therefore may not exit an existing position. The implemented exit is “new opposite-confirmation transition,” not “opposite signal remains confirmed.” Exact effect is direction-dependent.                                                                                                                                                                                                                                                                                            |
|   13 | **High**        | **The bootstrap implementation differs from the frozen protocol.**                                                 | Frozen plan: `artifacts/backtest/2026-07-15/experiment_plan.md:57–68`, especially line 66 specifying a seven-day moving-block bootstrap. Implementation: `src/signalbot/backtest/comparison.py:53–157`; `analysis.py:_block_bootstrap_mean()`, lines 68–98. Report: `final_report.md:47–60`.                                                        | The implementation uses fixed, non-overlapping blocks rather than a moving-block bootstrap. That is a confirmed preregistration deviation. It does not automatically reverse the findings, but the stated inferential protocol was not implemented.                                                                                                                                                                                                                                                                                                            |
|   14 | **High**        | **The reported “paired” comparisons are calendar-block synchronized, not trade-paired.**                           | `backtest/comparison.py`, ratio construction and resampling, lines 46–157.                                                                                                                                                                                                                                                                          | Variants select different trades and trade counts. Shared calendar draws preserve common market periods, which is useful, but there is no one-to-one matched-trade estimand. The ratio-of-sums estimates average outcome among each variant’s selected trades, not incremental policy value on a common opportunity or capital calendar.                                                                                                                                                                                                                       |
|   15 | **High**        | **The current portfolio summaries are not capital-aware.**                                                         | Report: `final_report.md:32–35`. `src/signalbot/backtest/analysis.py`, `sleeve_portfolios()`, lines 180–207.                                                                                                                                                                                                                                        | Trade P&L is summed without a simultaneous capital constraint. The sleeve calculation compounds each trade return into an asset sleeve despite the stated fixed 100-USDT notional. This does not invalidate per-trade gross results, but it overstates deployability and is not a realized account return.                                                                                                                                                                                                                                                     |
|   16 | **Medium–High** | **Intrabar stop timing and funding timing are internally ambiguous.**                                              | Stop handling: `positions.py:96–110`; backtest close/exit timing around `engine.py:410–447`. Funding return calculation around `engine.py:95–111`.                                                                                                                                                                                                  | A stop can be filled at an intrabar price but timestamped at the candle close. A funding event later in that same candle can then be charged even though the stop may have occurred first. OHLC cannot determine ordering. Lower- and upper-bound conventions are needed.                                                                                                                                                                                                                                                                                      |
|   17 | **Medium–High** | **Trade attribution across chronological splits is not purged.**                                                   | Split attribution in `backtest/engine.py` around line 700; maximum holding period is 72 bars.                                                                                                                                                                                                                                                       | A position can enter before a boundary and exit after it, yet be assigned by entry time. Positions from one split can also occupy opportunity/capital in the next. That contaminates split-specific estimates unless trades and labels crossing boundaries are purged and the next 72 bars embargoed.                                                                                                                                                                                                                                                          |
|   18 | **Medium–High** | **The live regime calculation is arrival-order dependent and differs from the synchronous historical panel.**      | Historical regime construction: `backtest/engine.py:226–273`. Live: `src/signalbot/regime/market.py`, lines 14–39; runtime bootstrap around `runtime.py:77–81`.                                                                                                                                                                                     | Historical breadth uses a synchronous full panel. Live updates symbols one at a time, potentially mixing current bars for some assets with prior bars for others. Startup also lacks a replayed regime state. The current Headline gate does not rely heavily on this value, so economic impact is probably secondary.                                                                                                                                                                                                                                         |
|   19 | **Medium**      | **The higher-timeframe increment changes normalization as well as adding context.**                                | `src/signalbot/signals/gates.py`, `_trend_score()`, lines 18–50, especially lines 38–39.                                                                                                                                                                                                                                                            | When HTF is disabled, the local 70-point maximum is rescaled to 100. When HTF is enabled, local points remain on a 70-point scale and 15m/1h add 15 each. `Headline − B2` is therefore a compound policy change, not a clean estimate of adding HTF confirmation alone.                                                                                                                                                                                                                                                                                        |
|   20 | **Medium**      | **Execution and completeness gates are nearly constants in the frozen historical run.**                            | `gates.py`, execution score around lines 85–91; `core.py` completeness around 453–458; report’s fixed 11.25-bp proxy at `final_report.md:24–30`.                                                                                                                                                                                                    | The proxy receives exactly the minimum execution score of 65, and completeness is generally 100. These gates document an assumption but do not provide observed historical filtering evidence.                                                                                                                                                                                                                                                                                                                                                                 |
|   21 | **Medium**      | **Histories are bounded per active key, but total key cardinality, disk, and database retention are not bounded.** | `src/signalbot/data/raw_events.py:12–32`; `data/microstructure.py` per-key dictionaries; `data/candles.py`; runtime feature dictionaries; `data/anomaly.py`; `persistence/models.py`.                                                                                                                                                               | Daily files have no retention policy, database tables have no pruning policy, and per-symbol maps can grow with universe churn. This violates the stated bounded-state invariant even though individual deques are bounded.                                                                                                                                                                                                                                                                                                                                    |
|   22 | **Medium**      | **The delivered repository does not pass its declared validation commands as packaged.**                           | `tests/unit/test_backtest_analysis.py`; `tests/unit/test_backtest_runner.py`; `README.md:42–52`; `AGENTS.md:29–31`; `src/signalbot/backtest/runner.py:273–307`.                                                                                                                                                                                     | As delivered, pytest reports 69 passes and 3 failures because tests and README reference missing `config/backtest.research.yaml` rather than `config/backtest.5m.research.yaml`. After a compatibility copy, two runner tests still fail because their temporary workspace lacks the required experiment plan. Whole-tree Ruff also fails in the downloaded reference scripts, although `ruff check src tests` and Pyright pass.                                                                                                                               |
|   23 | **Medium**      | **The artifact bundle cannot independently substantiate the full reproducibility claim.**                          | `final_report.md:126–140`; `verification.json`; bundle file inventory.                                                                                                                                                                                                                                                                              | The source digest is reproducible, but the original `uv.lock`, linked `README(1).md`, and raw outputs for all eight runs are absent. A/B byte identity is reported, not independently recomputable from the bundle. Deterministic replay also establishes repeatability, not correctness.                                                                                                                                                                                                                                                                      |

## B2. Confirmed defects in the downloaded reference scripts—not in the current Headline backtester

These files came from the linked conversation and must not be imported without repair:

| Severity | Reference defect                                                                                                                                     | Reference                                                                                                                           |
| -------- | ---------------------------------------------------------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------- |
| High     | `trend_confirmation_strategy()` can generate an entry and evaluate exits on the same candle.                                                         | `Downloads/strategy.py:32–109`, especially entry around 72–76 and exits around 79–93.                                               |
| High     | The corresponding simple position builder gives entry precedence while flat, so a same-bar exit may be silently ignored.                             | `Downloads/volume_analysis.py`, `build_position_from_signals()`, approximately lines 933–955.                                       |
| High     | The AVSL-inspired long stop applies `raw_stop.cummax()` across the entire dataframe rather than resetting per position.                              | `Downloads/volume_analysis.py`, `avsl_inspired_long_stop()`, approximately lines 770–817, especially the global cumulative maximum. |
| Medium   | The downloaded W-bottom marks a second low test without requiring an intervening recovery, so it is not a genuine W-shaped state sequence.           | `Downloads/volume_analysis.py`, `vpci_w_bottom()`, approximately lines 711–749.                                                     |
| Medium   | The TTI implementation explicitly resolves a sign-wording inconsistency by engineering choice.                                                       | `Downloads/volume_analysis.py`, `trend_thrust_indicator()`, approximately lines 503–557.                                            |
| Medium   | The downloaded trailing stop in `strategy.py` can loosen when current ATR rises because it is recomputed from current ATR without a monotonic clamp. | `Downloads/strategy.py:79–93`. The current `TechnicalExitEngine` does not have this defect; its stop is monotonic.                  |

## B3. Material risks and limitations, not confirmed result-changing defects

1. **Post-selection and survivorship:** the fixed panel was chosen retrospectively and includes current/high-volatility examples. Results are conditional on these eight assets, not a point-in-time Binance universe.

2. **Direction and venue are confounded:** Spot is tested only long and USDⓈ-M only short. A Spot-long versus futures-short comparison cannot identify pure long/short asymmetry because venue, feed, costs, and funding differ.

3. **Historical spread and impact are unobserved:** the fixed proxy cannot reconstruct state-dependent spread, depth, or impact. Actual costs could be higher or lower. The zero-slippage losses show this is not a plausible rescue of the present policy.

4. **Funding coverage completeness is not fully audited by cadence:** the loader validates ordering and values but does not establish that every expected event was obtained. There is no evidence of missing events in the supplied audit, but the validation contract is incomplete.

5. **The live top-N universe is not replayed point in time:** current 24-hour turnover and listing status cannot be used to infer the historical investable universe without a point-in-time archive.

6. **The “candidate rate” denominator is ambiguous:** the numerator counts family-level candidates, so multiple signal families on one symbol-bar can contribute several candidates. It is not simply the percentage of unique symbol-bars with at least one candidate.

7. **The anomaly detector uses a weak time anchor:** `data/anomaly.py:47–65` can select an arbitrarily old point near a target horizon and applies square-root-of-time scaling to irregularly sampled returns. This affects live warning validity, not the frozen 5m backtest.

## B4. Controls that passed adversarial review

I did **not** find a confirmed defect in the following paths:

* Historical decisions skip unclosed candles.
* Live candle processing ignores open candles.
* Entry occurs at the next 5m open.
* Gap-safe 15m and 1h aggregation is causal.
* A higher-timeframe candle with a close equal to the current 5m close is excluded until a later 5m decision.
* Historical funding selection uses strict `< decision_time`; equal-time funding is excluded.
* Recent-high/recent-low and volume baselines exclude the current bar where required.
* The current `TechnicalExitEngine` trailing stop is monotonic and cannot act on the same bar in which it is updated.
* Gap-through-stop fills use the next open rather than the stale stop price.
* Fee/slippage signs for long and short appear internally consistent.
* No production order-placement path was found.
* The currently configured futures WebSocket routing is consistent with Binance’s current separation of book/depth and market streams. ([Binance 개발자 센터][3])

---

# C. Diagnosis of the current negative gross and net results

## C1. What the gross results establish

**Observed from the supplied report:**

| Policy                 | Trades |    Gross P&L |         Gross return/trade | Net return/trade |
| ---------------------- | -----: | -----------: | -------------------------: | ---------------: |
| Headline Spot long     | 26,526 | −303.03 USDT | approximately **−0.0114%** |         −0.3333% |
| Headline futures short | 26,817 |  +44.03 USDT | approximately **+0.0016%** |         −0.1833% |

The gross results are more important diagnostically than the net results:

* Spot entries lose before fees or slippage.
* Futures shorts are almost exactly flat before costs.
* B0 is similarly weak: Spot gross −386.57 USDT and futures gross +50.52 USDT.
* Adding the current participation gate changes trade selection substantially but does not produce a material gross improvement.
* Funding/crowding and HTF increments are on the order of a few tenths of a basis point per trade and are not established after the reported multiplicity adjustment.

The raw trade files are absent, so I could not independently recalculate the distributions or confidence intervals. Nothing in the code review, however, offers a credible mechanism for a hidden positive edge of the 18–33 bp per-trade magnitude needed for net break-even.

## C2. Ranked causal diagnosis

| Rank | Diagnosis                                                  | Evidence                                                                                                                                                                                                                                                                | Confidence                                          |
| ---: | ---------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | --------------------------------------------------- |
|    1 | **Non-predictive or weakly predictive entries**            | Gross Spot is negative; gross futures is approximately zero; B0 is already weak; all reported fee-free improvements are tiny relative to the required hurdle.                                                                                                           | **High**                                            |
|    2 | **Overly permissive setup-to-confirmation transition**     | Gate passage promotes raw 30/60/65 signals to 80. Breakouts otherwise cannot confirm; squeeze can confirm before breaking the range.                                                                                                                                    | **High, confirmed mechanism**                       |
|    3 | **Excessive turnover relative to signal strength**         | Roughly 36 trades per market-day, about 26,000 trades per direction, with only about 11% exposure. Weak gross expectancy is repeatedly paid through fees and adverse fills.                                                                                             | **High**                                            |
|    4 | **Redundant and poorly normalized participation features** | Volume z-score, trade-count z-score, taker imbalance, and 20-bar CVD derive from overlapping activity/flow data. Current volume normalization does not account for UTC time-of-week seasonality or quote-notional comparability. B3 did not materially improve results. | **High–moderate**                                   |
|    5 | **State-machine and family-selection distortions**         | Many candidates tie at the promoted score of 80. Deterministic family/event ordering then selects among tied candidates rather than selecting the strongest raw setup. Episodes are not represented explicitly.                                                         | **Moderate–high**                                   |
|    6 | **Regime mixing and universe conditioning**                | A single rule is applied across majors, memecoins, different listing ages, and very different volatility/liquidity regimes. This can dilute conditional effects, but retrospective regime slicing cannot be used to rescue the policy.                                  | **Moderate inference**                              |
|    7 | **Exit-policy mismatch or poor fit**                       | Exits can shape return distribution, but the absence of entry gross edge cannot be repaired merely by inspecting favorable exit-reason slices. The positive trailing-stop slice is selected by path and is not causal evidence.                                         | **Moderate**                                        |
|    8 | **Simulation artifacts**                                   | Funding freshness, intrabar ordering, split crossings, and capital treatment change exact magnitudes. They can bias either way. None found is plausibly large enough to turn the current gross results into strong alpha.                                               | **High conclusion; individual magnitude uncertain** |
|    9 | **Long/short asymmetry**                                   | Spot-long is slightly negative gross while futures-short is nearly flat. Venue and direction are confounded, so this does not establish that shorts are intrinsically better.                                                                                           | **Low–moderate inference**                          |

## C3. Implications for future work

A valid next study must first answer:

> Among a common set of fully causal price-structure opportunities, does an added feature select materially better forward returns before costs?

It should not start by asking:

> Which indicator and threshold make the historical equity curve look best?

Before changing exits, calculate fixed-horizon direction-adjusted returns, MFE, and MAE for the unchanged entry opportunity set. If the accepted entries do not show better 15-minute, 60-minute, and 6-hour gross distributions than rejected opportunities and falsified controls, exit tuning is not justified.

---

# D. Volume-indicator evidence matrix

## D1. Common mathematical and availability contract

For a fully closed 5m candle (t), define:

* (O_t,H_t,L_t,C_t): open, high, low, close.
* (V_t): base-asset volume.
* (Q_t): quote-asset volume.
* (N_t): trade count.
* (B_t): taker-buy quote volume.
* (\Delta_t=2B_t-Q_t): signed taker quote-volume proxy.
* (TP_t=(H_t+L_t+C_t)/3).
* (TR_t=\max(H_t-L_t,\lvert H_t-C_{t-1}\rvert,\lvert L_t-C_{t-1}\rvert)).
* (d=+1) for long and (d=-1) for short.
* (s(t)\in{0,\ldots,2015}): UTC 5m slot of week.

These kline fields are directly available from Binance’s kline response; no trade-by-trade reconstruction is required for (Q_t,N_t,B_t). ([Binance 개발자 센터][2])

Define a prior-only robust standardization:

[
RZ_W(x_t)=\operatorname{clip}\left(
\frac{x_t-\operatorname{median}(x_{t-W:t-1})}
{s_{t,W}},-5,5\right)
]

where (s_{t,W}=1.4826,MAD); if MAD is zero, use (IQR/1.349); if both are zero or minimum history is absent, the value is **unavailable**, not zero.

Universal rules:

* The current fully closed candle may contribute its (Q_t,B_t,N_t,OHLC).
* Every baseline, scale, regression, percentile, or threshold uses only (t-1) and earlier.
* The earliest entry is the open of (t+1).
* A missing mandatory field, stale source, invalid relationship such as (B_t>Q_t), or an internal gap invalidates the feature.
* Valid zero activity is represented as zero only where mathematically meaningful. Ratios with a zero denominator are unavailable.
* No forward-fill across gaps.
* Standardized outputs are clipped only after causal scaling.
* Kline-derived features have one-closed-bar latency plus measured ingestion/processing latency. If the closed bar is not received before the entry deadline, the decision is withheld.

Status:

1. **Existing-data implementable**
2. **Implementable but materially redundant with current features**
3. **Invalid, misleading, nonstationary, or out of scope as a historical proxy**
4. **Prospective-only with the currently frozen dataset**

## D2. Existing-data candidates

| Candidate                                            | Exact definition, inputs, window, minimum history                                                                                                                                                                                                                                                                                                             | Availability, normalization, latency                                                                                                                   | Relationship and status                                                                                                                                                                     |
| ---------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **1. Robust local log quote-volume surprise†**       | (x_t=\log(1+Q_t)); (Z^Q_t=RZ_{2016}(x_t)). Target 2,016 prior bars; minimum 1,008 contiguous bars.                                                                                                                                                                                                                                                            | Current (Q_t) may be used; all median/scale observations end at (t-1). Missing/gap/zero scale → unavailable. Clip to ([-5,5]).                         | Better cross-symbol scale than raw/base volume and addresses heavy tails. Still an activity feature. **Status 1.**                                                                          |
| **2. UTC slot-of-week volume surprise†**             | For the same slot, (S_t={x_{t-2016k}:k=1,\ldots,12}). (Z^{slot}_t=\operatorname{clip}[(x_t-\operatorname{median}S_t)/robustScale(S_t),-5,5]). Require at least 8 of 12 prior weeks.                                                                                                                                                                           | Strictly prior same-slot observations; no interpolation from future weeks. Zero scale → unavailable.                                                   | Addresses crypto intraday/weekend seasonality absent from current volume normalization. **Status 1.**                                                                                       |
| **3. Prior-only median RVOL20†**                     | (RVOL^{med}*{20,t}=Q_t/\operatorname{median}(Q*{t-20:t-1})). Require 20 prior contiguous bars; denominator (>0); cap at 10.                                                                                                                                                                                                                                   | Current activity over prior baseline. Missing denominator → unavailable.                                                                               | Current code already has a mean-based base-volume RVOL. Use only as a replacement comparator. **Status 2.**                                                                                 |
| **4. Quote-volume percentile**                       | (P^Q_{63,t}=(1+\sum_{i=1}^{63}\mathbf1[Q_{t-i}\le Q_t])/64). Require 63 prior bars.                                                                                                                                                                                                                                                                           | Causal empirical rank; no fitted cross-symbol scale.                                                                                                   | Monotone re-expression of activity surprise. **Status 2.**                                                                                                                                  |
| **5. Single-bar taker imbalance**                    | (I_t=(2B_t-Q_t)/Q_t), clipped to ([-1,1]), requiring (Q_t>0) and (0\le B_t\le Q_t).                                                                                                                                                                                                                                                                           | No carry-forward. Current closed bar only.                                                                                                             | Already represented by `taker_imbalance`/`taker_buy_ratio`. **Status 2.**                                                                                                                   |
| **6. Multi-horizon normalized taker delta**          | (D_{h,t}=\frac{\sum_{j=0}^{h-1}\Delta_{t-j}}{\sum_{j=0}^{h-1}Q_{t-j}}), (h\in{1,3,12}). Require every bar valid and total quote volume (>0).                                                                                                                                                                                                                  | Fully closed bars only; no partial-window substitution.                                                                                                | Cleaner horizon-specific replacement for single-bar imbalance plus 20-bar CVD. Still substantially overlapping. **Status 2.**                                                               |
| **7. Price-volume efficiency / effort-result proxy** | (u_t=d(C_t-C_{t-1})/ATR_{14,t}). Seasonal effort (a_t=\operatorname{clip}[\exp(x_t-\operatorname{median}S_t),0.1,10]). (E_t=u_t/a_t); score (RZ_{2016}(E_t)). Require ATR plus at least eight seasonal observations and 1,008 prior efficiency values for fitted scoring.                                                                                     | Current return and volume are closed-bar values; all scales prior-only. Invalid ATR or seasonal baseline → unavailable.                                | More orthogonal than raw activity: asks how much directional price progress occurred per unit of relative effort. It is **not** true market impact. **Status 1.**                           |
| **8. Normalized VPCI†**                              | (VWMA_n(C,Q)=\sum CQ/\sum Q). With (S=5,L=20): (VPC=VWMA_L-SMA_L); (VPR=VWMA_S/SMA_S); (VM=SMA_S(Q)/SMA_L(Q)); (VPCI=VPC\cdot VPR\cdot VM). (nVPCI=VPCI/ATR_{20}). Signal (=VWMA_5(nVPCI,Q)). Frozen state: (d,nVPCI>0), (d(nVPCI-signal)>0), and (d(nVPCI_t-nVPCI_{t-3})>0). Approximately 25 candles minimum under the specified ATR and signal definition. | All denominator failures unavailable. Do not use a fixed raw VPCI threshold across symbols. Optional prior-only (RZ_{504}) only for calibration plots. | Nonlinear combination of trend and volume; likely redundant with EMA/MACD/activity. Test only as a replacement or secondary falsification. **Status 2.**                                    |
| **9. VPCI bands and V-bottom†**                      | On (nVPCI), prior rolling band (m_{20}\pm2\sigma_{20}). A V-bottom requires (nVPCI_{t-1}<lower_{t-1}) and (nVPCI_t\ge lower_t). Require approximately 45 candles including VPCI warm-up and band history.                                                                                                                                                     | Band moments must be causal; zero variance unavailable.                                                                                                | A state transition rather than an independent feature; overlaps reversal logic and is not part of the primary continuation hypothesis. **Status 2.**                                        |
| **10. Downloaded VPCI W-bottom†**                    | Downloaded implementation identifies two low-band tests within 30 bars.                                                                                                                                                                                                                                                                                       | It does not require an intervening recovery and uses a fixed raw floor, which is not scale invariant.                                                  | The supplied implementation is not a valid W-pattern definition. **Status 3.**                                                                                                              |
| **11. Rolling VWAP location and slope**              | (VWAP_{20,t}=\sum_{j=0}^{19}TP_{t-j}Q_{t-j}/\sum Q_{t-j}). Location (L_t=(C_t-VWAP_{20,t})/ATR_{14,t}). Slope (S_t=(VWAP_{20,t}-VWAP_{20,t-3})/ATR_{14,t}). Require 23 bars plus ATR seed.                                                                                                                                                                    | All bars fully closed; zero total volume or ATR → unavailable.                                                                                         | Primarily another price-trend/location transform. **Status 2.**                                                                                                                             |
| **12. UTC-daily anchored VWAP**                      | Accumulate (TP_jQ_j/\sum Q_j) from 00:00 UTC through (t); use deviation/ATR and three-bar slope. Require at least 12 bars in the UTC session.                                                                                                                                                                                                                 | Reset exactly at UTC midnight; no use before minimum session history.                                                                                  | The UTC reset is operationally convenient but economically arbitrary in a 24/7 market. Highly correlated with trend/location. **Status 2.**                                                 |
| **13. VWMA–SMA spread and VW-MACD**                  | Spread: ([VWMA_{20}(C,Q)-SMA_{20}(C)]/ATR_{14}). VW-MACD: ([VWMA_{12}-VWMA_{26}]/ATR_{14}), with a causal EMA9 signal. Minimum approximately 35 bars.                                                                                                                                                                                                         | Closed bars only; zero-volume windows unavailable.                                                                                                     | Volume-weighted reformulations of existing trend/MACD. Must be compared against ordinary MACD on identical windows. **Status 2.**                                                           |
| **14. CMF20**                                        | Money-flow multiplier (m_t=(2C_t-H_t-L_t)/(H_t-L_t)). (CMF_{20}=\sum m_jQ_j/\sum Q_j). Require 20 valid bars. A zero-range bar is invalid, not assigned multiplier zero.                                                                                                                                                                                      | No partial valid-window averaging.                                                                                                                     | Combines close location and volume, overlapping taker flow, wick features, and trend. **Status 2.**                                                                                         |
| **15. Cumulative accumulation/distribution**         | (AD_t=AD_{t-1}+m_tQ_t).                                                                                                                                                                                                                                                                                                                                       | Depends on arbitrary start date and grows nonstationarily.                                                                                             | A rolling normalized form reduces algebraically toward CMF. Standard cumulative level is misleading for cross-period research. **Status 3.**                                                |
| **16. WVAD20**                                       | (WVAD_{20,t}=\sum_{j=0}^{19}[(C_j-O_j)/(H_j-L_j)]Q_j\big/\sum Q_j). Require 20 valid nonzero-range bars.                                                                                                                                                                                                                                                      | Zero-range bars unavailable, not zero.                                                                                                                 | Another price-location × volume construction, strongly overlapping CMF and wick/rejection features. **Status 2.**                                                                           |
| **17. Standard cumulative OBV**                      | (OBV_t=OBV_{t-1}+\operatorname{sign}(C_t-C_{t-1})Q_t).                                                                                                                                                                                                                                                                                                        | Arbitrary origin and nonstationary cumulative level.                                                                                                   | Unsuitable as an additive feature across symbols or long periods. **Status 3.**                                                                                                             |
| **18. Rolling OBV pressure**                         | (OBP_{20,t}=\sum_{j=0}^{19}\operatorname{sign}(C_j-C_{j-1})Q_j/\sum_{j=0}^{19}Q_j). Require 21 candles.                                                                                                                                                                                                                                                       | Closed, contiguous window; denominator (>0).                                                                                                           | Implementable but essentially direction-signed activity, overlapping taker/CVD. **Status 2.**                                                                                               |
| **19. VPT, PVI, and NVI cumulative levels**          | VPT increments (Q_t(C_t/C_{t-1}-1)); PVI/NVI accumulate returns conditional on volume rising/falling.                                                                                                                                                                                                                                                         | Cumulative levels depend on arbitrary initialization; volume-rise classification is seasonally biased.                                                 | Rolling forms are implementable but redundant with returns × volume/phase. Standard cumulative versions are misleading. **Status 3.**                                                       |
| **20. Notional MFI14**                               | Let positive flow be (Q_t) when (TP_t>TP_{t-1}), negative flow when (TP_t<TP_{t-1}). (MFI=100P/(P+N)) over 14 transitions. Require 15 candles. If (P+N=0), unavailable; if only one side is zero, return 0 or 100 explicitly.                                                                                                                                 | Uses quote notional directly rather than multiplying quote volume by price again.                                                                      | Oscillator combining price direction and activity; likely redundant with price momentum and flow. It must not replace the deliberate exclusion of RSI as the main foundation. **Status 2.** |
| **21. Market Facilitation Index**                    | (MFI^{BW}_t=(H_t-L_t)/Q_t).                                                                                                                                                                                                                                                                                                                                   | Undefined at zero quote volume; highly scale- and price-dependent.                                                                                     | This is not a portable cross-symbol liquidity or impact measure. **Status 3.**                                                                                                              |
| **22. Five-bar price-volume phase†**                 | Compare (\Delta^5 C=C_t/C_{t-5}-1) and (\Delta^5 Q=Q_t/Q_{t-5}-1); classify up/up, up/down, down/up, down/down.                                                                                                                                                                                                                                               | Require six valid candles; exact zero changes produce an indeterminate state.                                                                          | Coarse binning of information already present in trend and activity features. **Status 2.**                                                                                                 |
| **23. Volume acceleration**                          | (A_t=\operatorname{median}(Z^{slot}*{t:t-2})-\operatorname{median}(Z^{slot}*{t-3:t-11})). Require 12 consecutive seasonal scores.                                                                                                                                                                                                                             | Entire window closed; any invalid member makes the value unavailable.                                                                                  | Derivative of the activity feature, not orthogonal. **Status 2.**                                                                                                                           |
| **24. Breakout participation†**                      | Existing prior-boundary break plus (Z^{slot}*t) above a train-fitted threshold, (dD*{3,t}) above a fixed/train-fitted threshold, and direction-adjusted close location (d(2C-H-L)/(H-L)>0).                                                                                                                                                                   | Boundary uses only (t-1) and earlier; participation uses closed (t). Zero-range bar unavailable.                                                       | A coherent setup rule but overlaps the current breakout, activity, and taker gate. Test as a replacement, not an additional point stack. **Status 2.**                                      |
| **25. Pre-trigger dry-up†**                          | (Dry_t=-\operatorname{median}(Z^{slot}_{t-3:t-1})). A separate trigger on (t) requires a boundary break, high (Z^{slot}*t), and aligned (D*{3,t}).                                                                                                                                                                                                            | Dry-up explicitly excludes the trigger bar. Missing any precondition or trigger component fails closed.                                                | Temporal sequencing makes this more orthogonal than adding another same-bar volume score. **Status 1.**                                                                                     |
| **26. Effort/result rejection proxy**                | Candidate condition: high (Z^{slot}*t), (TR_t/ATR*{14,t}) below a predeclared threshold, and extreme direction-adjusted close location. Confirmation must occur on a later closed candle through reclaim/break of the known rejection boundary.                                                                                                               | Requires one additional closed confirmation candle; earliest entry is the following open.                                                              | Implementable as a kline **rejection proxy**, not evidence of passive absorption. **Status 1.**                                                                                             |
| **27. “True absorption” inferred from 5m candles**   | Any claim that high volume and small range proves hidden passive buying/selling.                                                                                                                                                                                                                                                                              | Klines cannot distinguish absorption from two-sided churn, fragmented activity, or rapid reversal.                                                     | The causal interpretation is not identified. **Status 3.**                                                                                                                                  |
| **28. Low-volume pullback continuation†**            | Fixed causal trend; prior impulse with high seasonal activity; a pullback of fixed length with low (Z^{slot}) and range contraction; trigger closes above a previously known pullback high, with optional aligned (D_3).                                                                                                                                      | All setup boundaries known before trigger; no use of future swing labels. Suggested minimum trend history 100 bars.                                    | A complete setup family, not an orthogonal indicator. It would alter entry semantics and should not be mixed into a one-indicator ablation. **Status 2.**                                   |
| **29. Volatility-residualized quote volume**         | In prior (W=2016) bars, winsorize (x_j=\log(1+Q_j)) and (a_j=\log(ATR_{14,j}/C_j)) at prior-window 1%/99% bounds; fit (x_j=\alpha+\beta a_j+\epsilon_j). Current residual (e_t=x_t-\hat\alpha-\hat\beta a_t); robust-standardize against prior residuals. Minimum 1,008, target 2,016.                                                                        | Fit and winsorization bounds strictly prior. Degenerate regression or residual scale → unavailable.                                                    | A dimensionally coherent alternative to dividing volume directly by ATR. More orthogonal than raw activity. **Status 1.**                                                                   |

## D3. Invalid, out-of-scope, or prospective-only candidates

| Candidate                                                          | Exact definition or requirement                                                                                                                                                                                                                             | Availability and latency                                                                                                                                                                                                                           | Status                                                                            |
| ------------------------------------------------------------------ | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------- |
| **30. TTI†**                                                       | Downloaded version uses (VWMA_{12}), (VWMA_{26}), volume multiple (VM=SMA_{12}(Q)/SMA_{26}(Q)), enhanced fast (=VWMA_{12}VM^2), enhanced slow (=VWMA_{26}/VM^2), spread = fast − slow, and an adaptive 2–27-bar signal. Maximum warm-up is roughly 52 bars. | Formula contains an acknowledged source sign ambiguity and aggressive nonlinear volume scaling.                                                                                                                                                    | **Status 3.** Do not include in the primary frozen experiment.                    |
| **31. AVSL-inspired stop†**                                        | Volatility/structure/VPCI-dependent trailing stop.                                                                                                                                                                                                          | It is an exit rule, not a volume entry feature; the downloaded implementation also fails to reset its cumulative maximum per trade.                                                                                                                | **Status 3.** Separate exit hypothesis only.                                      |
| **32. Cap-weighted volume or breadth†**                            | Requires point-in-time market capitalization and point-in-time universe membership for every asset and timestamp.                                                                                                                                           | Neither is in the frozen panel. Retrospective current caps would leak future information.                                                                                                                                                          | **Status 4.**                                                                     |
| **33. Sub-minute `aggTrade` flow**                                 | Examples: last-60s taker imbalance, trade intensity, burstiness, signed-notional acceleration, large-trade share.                                                                                                                                           | Not present in the frozen kline/funding dataset. Binance public archives can constitute a new data-acquisition study, but ingestion must normalize timestamp units; archived Spot data from 2025 onward uses microsecond timestamps. ([GitHub][4]) | **Status 4.** Do not approximate it with full-5m kline taker volume.              |
| **34. Order-book spread, depth, imbalance, and replenishment**     | Requires source-timestamped book snapshots or diffs and a deterministic reconstruction policy.                                                                                                                                                              | No historical point-in-time book dataset is supplied. Latest-book live state is not a historical proxy.                                                                                                                                            | **Status 4.**                                                                     |
| **35. Liquidation flow**                                           | Requires timestamped liquidation events and documented coverage/gap handling.                                                                                                                                                                               | Not in the frozen dataset. A current live stream cannot be backfilled into 2024–2026 decisions.                                                                                                                                                    | **Status 4.**                                                                     |
| **36. Open-interest level/change**                                 | Examples: (\Delta\log OI), price × OI state, OI-adjusted funding.                                                                                                                                                                                           | Binance’s current historical OI endpoint exposes only limited recent history—currently approximately one month—so it cannot support the frozen 2024–2026 panel. ([Binance 개발자 센터][5])                                                              | **Status 4.**                                                                     |
| **37. Top-trader ratios**                                          | Top-account and top-position long/short ratios.                                                                                                                                                                                                             | The inspected current documentation limits the available window to approximately 30 days; the current endpoint documentation also introduces authentication constraints inconsistent with the no-key project invariant. ([Binance 개발자 센터][6])      | **Status 4.**                                                                     |
| **38. Futures aggregate taker buy/sell statistics**                | Separate futures taker buy/sell account or volume ratios.                                                                                                                                                                                                   | Binance’s current historical endpoint exposes only the most recent approximately 30 days. It is not a valid reconstruction for 2024–2026. ([Binance 개발자 센터][7])                                                                                    | **Status 4.**                                                                     |
| **39. Funding crowding**                                           | Existing strict-prior funding rate and robust prior-window score. Funding is not a volume indicator.                                                                                                                                                        | Must use one shared live/historical function with minimum history, freshness, and no neutral substitution.                                                                                                                                         | **Status 2** as an existing separate crowding input, not an added volume feature. |
| **40. Risk sizing and broker adapters from the linked artifacts†** | Dynamic position sizing, broker execution, or order routing.                                                                                                                                                                                                | Conflicts with fixed 100-USDT research notional and alert-only/no-production-order invariants.                                                                                                                                                     | **Status 3.**                                                                     |

---

# E. Prioritized P0/P1/P2 implementation plan with acceptance tests

## Architectural principle

The smallest coherent change is:

> Replace separate live/backtest feature construction and score promotion with one pure point-in-time `DecisionFrame`, explicit availability states, and an episode-based trigger/gate lifecycle.

A new indicator should not be implemented before this unit is complete.

## E1. P0 — correctness, parity, and scientific-validity repairs

| P0 change                                                  | Exact targets                                                                                                                                                                                  | Required semantics and failure behavior                                                                                                                                                                                                                                                                                                                                 | Rule/config effect                                                                                                              |
| ---------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------- |
| **P0.1 Introduce explicit availability metadata**          | `src/signalbot/domain/models.py`: `Candle`, `FeatureSnapshot`, `RuleEvaluation`, `SignalDecision`; new `FeatureValue`/`Availability` models. `exchange/binance/schemas.py:_parse_kline()`.     | Each feature carries `value`, `status ∈ {AVAILABLE,MISSING,STALE,INVALID,PROXY}`, `source_event_time_ms`, `observed_at_ms`, `sample_count`, and optional `fresh_until_ms`. Required kline fields have no zero defaults. Raw exchange DTOs may be permissive; validated domain models should use `extra="forbid"`.                                                       | Major domain migration. Persisted records need schema version.                                                                  |
| **P0.2 Create one shared causal feature assembler**        | Refactor `indicators/core.py` around `FeatureEngine.compute()`/its point-in-time helper; use it from `runtime.py:_refresh_feature()` and `backtest/engine.py` continuous-feature construction. | Main participation uses full, closed-5m (Q,N,B) identically live and historically. The last-60s stream becomes a separate optional `microstructure.*` namespace and is never substituted for kline participation.                                                                                                                                                       | Add `participation_source: closed_kline`. Frozen research sets prospective microstructure off.                                  |
| **P0.3 Separate trigger strength from gate eligibility**   | `signals/rules.py:_apply_gate()`, `_squeeze()`, `_breakout()`, `_breakdown()`; `signals/gates.py`; `domain/models.py`.                                                                         | Gates return eligibility only. They may veto but never overwrite `setup_strength`. Replace overloaded `score` with at least `triggered`, `setup_strength`, `eligible`, and `confirmation_state`.                                                                                                                                                                        | Remove hidden promotion. Because breakout maxes at 65, redesign its explicit trigger state rather than merely deleting line 72. |
| **P0.4 Implement episode-based signal transitions**        | Replace/extend `signals/state_machine.py:SignalStateMachine`; persistence repository.                                                                                                          | Squeeze: compression/proximity creates `WATCH` or `SETUP`; only a later closed-bar break within a fixed expiry confirms. Breakout: an actual boundary cross creates a trigger; gates determine eligibility. Persist `episode_id`, start, last update, strongest strength, expiry, invalidation, resolution reason. `CONFIRMED → RESOLVED/INVALIDATED` must be explicit. | Add `episode_expiry_bars`, fixed family priority, and persistent bounded state.                                                 |
| **P0.5 Unify funding snapshot logic**                      | Extract a pure function used by `data/funding.py` and `backtest/engine.py`.                                                                                                                    | Strict event time `< decision time`; configured UTC lookback; minimum sample count; robust scale; maximum age; no carry beyond freshness; missing/immature/zero-scale status explicit.                                                                                                                                                                                  | One configuration source for live and backtest.                                                                                 |
| **P0.6 Make book state point-in-time**                     | `data/microstructure.py`; `runtime.py` book attachment.                                                                                                                                        | Bounded per-symbol deque with source time and receive time. Query `last_at_or_before(cutoff)`. Reject stale, future, out-of-order, or source-time-unknown observations. Record whether cutoff is candle close or actual decision time.                                                                                                                                  | Add `book_max_age_ms`, `book_history_capacity`, and an explicit as-of policy.                                                   |
| **P0.7 Implement actual live paper-position parity**       | New bounded `LivePaperPositionManager` using `signals/positions.py:TechnicalExitEngine`; integrate with `MarketRuntime` and alert persistence.                                                 | It may emit paper exit alerts but must never call an order API. The same candle/decision stream must produce byte-identical position transitions and exits in live replay and backtest. Alternatively, remove every claim that exits are live-equivalent and label them research-only.                                                                                  | Position state requires persistence and a rule-versioned migration.                                                             |
| **P0.8 Repair opposite-signal and stop/funding semantics** | `backtest/engine.py`; `positions.py`.                                                                                                                                                          | Opposite exit consults currently active opposite state, not merely a newly emitted transition. For intrabar stop/funding ambiguity, report lower and upper bounds; do not invent an event order.                                                                                                                                                                        | Add explicit `intrabar_funding_convention`.                                                                                     |
| **P0.9 Make anomaly warnings temporally valid**            | `data/anomaly.py`.                                                                                                                                                                             | Fixed observation buckets or nearest-anchor tolerance on both sides; no arbitrarily old anchor; scaling based on a documented fixed return interval. Every warning has an expiry/resolution condition.                                                                                                                                                                  | Warning rule version changes.                                                                                                   |
| **P0.10 Enforce global boundedness**                       | `raw_events.py`, persistence repositories, candle/order-flow/book/anomaly/runtime stores.                                                                                                      | Maximum active keys, LRU/explicit eviction, file retention days/bytes, DB pruning, bounded dead-letter queues, and metrics when eviction occurs. Retries remain cancellable and bounded.                                                                                                                                                                                | Add retention/capacity settings with finite required values.                                                                    |
| **P0.11 Add a common opportunity and capital ledger**      | `backtest/engine.py`, `backtest/analysis.py`, new capital-policy module.                                                                                                                       | Record every eligible symbol-bar opportunity, accepted/rejected reason, and deterministic capital decision. Model fixed 100-USDT positions under an explicit capital/concurrency limit. Do not relabel independent ledgers as account returns.                                                                                                                          | Add capital settings; keep raw unconstrained trade ledger as a diagnostic.                                                      |
| **P0.12 Restore reproducible packaging**                   | Config paths, README, tests, runner manifest, archive process.                                                                                                                                 | Include the exact `uv.lock`, frozen plan, raw result files, bootstrap draws or draw seed/hash, and all referenced artifacts. Required commands must pass from a clean checkout.                                                                                                                                                                                         | No economic result is “final” until package validation is green.                                                                |

### Score and threshold semantics after P0

Recommended semantics:

* **Setup strength:** descriptive 0–100 score composed only of evidence within the signal family.
* **Eligibility gates:** non-compensating Boolean requirements with separate component scores and availability states.
* **Confirmation:** a named state transition caused by a defined price event, not by reaching a promoted numeric score.
* **Tie-breaking:** highest unpromoted setup strength, then fixed versioned family priority, then symbol, then deterministic event ID.
* **Completeness:** percentage of required components that are valid is insufficient by itself; any mandatory unavailable component fails its associated gate.
* **Execution:** historical proxy status remains explicit. It cannot earn a “tight observed market” bonus.

Suggested new rule version: **`v3.0.0-pit-episodes`**. Event IDs, episode IDs, and results must not be compared as though they were v2 events.

## E2. P0 acceptance tests

| Area                | Positive test                                                                                      | Negative test                                                                                    | Boundary/adversarial test                                                                                                  |
| ------------------- | -------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------ | -------------------------------------------------------------------------------------------------------------------------- |
| Gate semantics      | A genuinely triggered raw-strength-80 setup passing all gates confirms.                            | A raw-strength-30 breakout passing all gates does not become confirmed solely from gate passage. | Strengths 79 and 80 produce the exact documented states; gate pass never changes strength.                                 |
| Squeeze lifecycle   | Compression setup followed by a later closed-bar boundary break within expiry confirms.            | Compression without a later break never confirms.                                                | Break exactly on expiry and break one bar after expiry have deterministic opposite outcomes.                               |
| Breakout            | A close strictly beyond a prior-only boundary triggers.                                            | Touching but not crossing does not.                                                              | Prior high/low excludes current bar; equality behavior is explicit.                                                        |
| Missingness         | A valid zero-volume candle is represented distinctly where allowed.                                | Missing quote/taker/trade fields fail validation.                                                | (B=Q), (B=0), (B>Q), zero range, zero denominators, NaN, and infinity each have explicit expected results.                 |
| Feature parity      | The same closed-kline history produces byte-identical live-replay and historical `DecisionFrame`s. | A 60-second stream cannot silently replace a closed-5m feature.                                  | Late-arriving closed candle causes a withheld decision, not a stale/future substitution.                                   |
| Funding             | Sufficient fresh strict-prior history yields the same snapshot live and historical.                | Immature, stale, missing, or future/equal-time observations are unavailable.                     | Test `min_history−1`, `min_history`, exact freshness boundary, equal event time, out-of-order rows, and zero robust scale. |
| HTF                 | A completed 15m/1h candle is usable on the next 5m decision.                                       | Equal-close context is unavailable.                                                              | Missing constituent 5m candle invalidates the aggregate.                                                                   |
| Book                | Last source-timestamped book before cutoff is returned.                                            | Book after cutoff or beyond freshness is rejected.                                               | Exact cutoff equality, duplicate update, out-of-order update, and capacity eviction.                                       |
| Position parity     | Live replay and backtest produce the same entry, stop state, trailing state, and exits.            | No production order side effect is reachable.                                                    | Gap-through stop, 1R exact activation, trend-failure count 2/3, bar 71/72, restart recovery.                               |
| Opposite exit       | An active opposite-confirmed episode exits a position.                                             | An inactive or expired opposite episode does not.                                                | No new transition is required when opposite state remains active.                                                          |
| Intrabar funding    | Lower- and upper-bound conventions agree when no same-bar funding event exists.                    | No single invented ordering is reported when ambiguous.                                          | Funding event at bar open, within bar, and bar close.                                                                      |
| Episode persistence | Restart reloads the active episode without duplicate alert.                                        | Corrupt/expired episode is not silently resumed.                                                 | Rule-version change creates a new identity and migration record.                                                           |
| Boundedness         | State remains below configured capacity during churn.                                              | Capacity breach cannot grow memory/disk silently.                                                | Eviction order, retention cutoff, database prune, and dead-letter saturation.                                              |
| Reproducibility     | Two clean runs produce identical declared artifacts.                                               | Missing lock, plan, or raw result blocks the final claim.                                        | Manifest changes when any config, code, data, or plan byte changes.                                                        |

## E3. P1 — minimal signal-quality additions after P0

Implement only the following:

1. **Seasonal log quote-volume surprise†** as the primary activity replacement.
2. **Multi-horizon kline taker delta** as the flow replacement.
3. **Price-volume efficiency** as an orthogonal effort/result candidate.
4. **Normalized VPCI†** only as a secondary, deliberately redundant comparator.
5. **Pre-trigger dry-up followed by a directional participation trigger†** as a temporal-sequence candidate.

Targets:

* New pure modules under `src/signalbot/indicators/volume.py`.
* Frozen typed configuration under the research/backtest config.
* No generic “indicator registry” that permits unregistered substitution.
* Feature outputs must use the same availability model as P0.
* Each rolling object must have a fixed finite capacity derived from its maximum window.
* Every feature must expose its effective sample count and scale status.

Do not add CMF, OBV, MFI, VWAP, VPCI, RVOL, and volume acceleration simultaneously. That would create a compensating cluster of near-duplicate features and an unmanageable search space.

## E4. P2 — prospective microstructure research

Only after P0/P1:

* Collect `aggTrade`, source-timestamped order-book data, liquidations, and any permitted OI data into append-only versioned files.
* Record `source_event_time`, `received_at`, sequence/aggregate IDs, gap status, schema version, and file hash.
* Bound active buffers and on-disk retention.
* Maintain shadow features and paper alerts only.
* Do not retroactively splice these data into the frozen kline experiment.
* Do not add top-trader data while the no-key invariant and current endpoint limitations make collection noncompliant.
* No order execution code.

---

# F. Frozen experiment matrix, estimands, multiplicity treatment, and pass/reject criteria

## F1. Comparator and feature matrix

`L-*` denotes exact legacy replay after packaging repair. It is a descriptive bridge only because its semantics are defective. `C-*` denotes the corrected P0 architecture.

| ID       | Price trigger/episode | Current participation         | Funding/crowding | HTF            | New feature                          | Primary contrast                    |
| -------- | --------------------- | ----------------------------- | ---------------- | -------------- | ------------------------------------ | ----------------------------------- |
| **L-B0** | Legacy                | Off                           | Off              | Off            | None                                 | Descriptive legacy comparator       |
| **L-H**  | Legacy                | Current legacy                | Current legacy   | Current legacy | None                                 | Descriptive legacy comparator       |
| **C0**   | Corrected             | Off                           | Off              | Off            | None                                 | Corrected primary baseline          |
| **C-H**  | Corrected             | Corrected current replacement | Corrected        | Corrected      | None                                 | `C-H − C0`, protocol-control family |
| **G1†**  | Corrected             | Off                           | Off              | Off            | Seasonal log quote-volume surprise   | `G1 − C0`                           |
| **G2**   | Corrected             | Off                           | Off              | Off            | Multi-horizon normalized taker delta | `G2 − C0`                           |
| **G3**   | Corrected             | Off                           | Off              | Off            | Price-volume efficiency              | `G3 − C0`                           |
| **G4†**  | Corrected             | Off                           | Off              | Off            | Normalized VPCI state                | `G4 − C0`                           |
| **G5†**  | Corrected             | Off                           | Off              | Off            | Prior dry-up + current trigger       | `G5 − C0`                           |
| **K1†**  | Corrected             | Off                           | Off              | Off            | G1 + G3                              | `K1 − C0`; interaction secondary    |
| **K2†**  | Corrected             | Off                           | Off              | Off            | G2 + G5                              | `K2 − C0`; interaction secondary    |

No other combination is allowed. All rows run for both predeclared deployment directions:

* Spot long.
* USDⓈ-M futures short.

They are reported separately and never pooled to manufacture significance.

## F2. Locked predictive hypotheses and exact configurations

| ID      | Locked hypothesis                                                                                                                                             | Fixed formula and permitted development-only grid                                                                                                                           |
| ------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **G1†** | Breakouts occurring with positive activity relative to the same UTC slot of prior weeks have better direction-adjusted forward returns than C0 opportunities. | (Z^{slot}_t). Acceptance threshold is the development-fold quantile (p\in{0.60,0.75,0.90}).                                                                                 |
| **G2**  | Direction-aligned short-horizon taker delta adds continuation information beyond price structure.                                                             | Require (dD_{12,t}\ge0) and (dD_{3,t}\ge a), (a\in{0,0.10,0.20}).                                                                                                           |
| **G3**  | High directional price progress per unit of seasonal volume effort predicts continuation rather than immediate reversal.                                      | (E_t=d(C_t-C_{t-1})/[ATR_{14,t}a_t]). Threshold is development-fold quantile (p\in{0.60,0.75,0.90}). The sign is locked as “higher predicts continuation.”                  |
| **G4†** | Directionally positive normalized VPCI above its signal and rising over three bars selects stronger continuation.                                             | No fitted threshold: (d,nVPCI>0), (d(nVPCI-signal)>0), (d(nVPCI_t-nVPCI_{t-3})>0).                                                                                          |
| **G5†** | Low activity during the three bars before a setup, followed by a high-activity aligned break, selects cleaner expansion.                                      | Two permitted pairs only: prior median (Z^{slot}\le q_{0.40}) with current (Z^{slot}\ge q_{0.75}), or prior (\le q_{0.25}) with current (\ge q_{0.90}); require (dD_3\ge0). |
| **K1†** | Activity surprise and price-volume efficiency contain complementary information.                                                                              | Conjunction of the independently selected G1 and G3 development settings.                                                                                                   |
| **K2†** | Aligned taker flow is most informative when it follows a low-activity precondition.                                                                           | Conjunction of independently selected G2 and G5 settings.                                                                                                                   |

A negative sign for G3, an alternate VPCI floor, a different dry-up duration, or another indicator may not be substituted after results are seen.

## F3. Deterministic threshold-fitting procedure

Thresholds are fitted only inside the designated development data:

1. Evaluate only the finite grids above.
2. Require a development alert rate between **1 and 20 accepted entries per market-day** and at least **200 completed trades per direction**.
3. For each setting, calculate the primary capital-aware estimand in four inner expanding validation folds.
4. A setting remains eligible only if its uplift is positive in at least **three of four** inner folds.
5. Select the setting with the highest median inner-fold uplift.
6. If settings are within 1 bp per trade of one another, choose:

   1. fewer conditions;
   2. lower turnover;
   3. lexicographically smallest frozen configuration ID.
7. If no setting qualifies, the feature is rejected in that outer fold. It does not receive a broader grid.
8. Outer assessment, prospective test, or placebo outcomes may never influence threshold selection.

## F4. Retrospective nested walk-forward protocol

The historical period is already exposed; these results are **development evidence only**, not a final holdout.

Seven rolling outer folds:

| Fold | Outer train           | Validation            | Assessment            |
| ---: | --------------------- | --------------------- | --------------------- |
|    1 | 2024-07-01–2025-03-01 | 2025-03-01–2025-05-01 | 2025-05-01–2025-07-01 |
|    2 | 2024-09-01–2025-05-01 | 2025-05-01–2025-07-01 | 2025-07-01–2025-09-01 |
|    3 | 2024-11-01–2025-07-01 | 2025-07-01–2025-09-01 | 2025-09-01–2025-11-01 |
|    4 | 2025-01-01–2025-09-01 | 2025-09-01–2025-11-01 | 2025-11-01–2026-01-01 |
|    5 | 2025-03-01–2025-11-01 | 2025-11-01–2026-01-01 | 2026-01-01–2026-03-01 |
|    6 | 2025-05-01–2026-01-01 | 2026-01-01–2026-03-01 | 2026-03-01–2026-05-01 |
|    7 | 2025-07-01–2026-03-01 | 2026-03-01–2026-05-01 | 2026-05-01–2026-07-01 |

Within each eight-month outer training period, use four expanding inner folds: first four months train/next month validate, then five/one, six/one, and seven/one.

The March–June 2024 data may initialize causal rolling features for the first fold, but no outcome from outside the current training fold may be used for threshold selection.

## F5. Purge and embargo

At every inner and outer boundary:

* Purge any opportunity whose next-open entry or maximum 72-bar label/trade horizon crosses the boundary.
* Purge any already-open simulated position that crosses the boundary.
* Embargo the first **72 5m bars = 6 hours** after the boundary from threshold fitting and assessment.
* Long feature warm-up may use prior candles because every transform is point-in-time, but those earlier outcomes cannot enter model or threshold selection.
* Split assignment is based on the complete opportunity/label interval, not only entry or exit timestamp.

## F6. Primary estimand and capital policy

For each feature-direction pair:

[
\theta =
\frac{\sum_{d\in test}
(PnL^{variant}_d-PnL^{C0}_d)}
{\text{number of common eligible symbol-days}}
]

where P&L is produced by a deterministic capital policy:

* Separate 10,000-USDT ledger for each deployment direction.
* Fixed 100-USDT notional per accepted trade.
* Maximum eight simultaneous positions.
* No leverage in the capital-account calculation.
* When capacity is unavailable, opportunities are ranked by:

  1. decision timestamp;
  2. unpromoted setup strength descending;
  3. symbol;
  4. fixed family priority;
  5. event ID.
* Rejected-for-capacity opportunities contribute zero P&L and remain in the opportunity panel.
* Report utilization and capacity rejections.

This estimand measures policy value on a common calendar rather than mean return among a variant-specific selected-trade sample.

Secondary estimands:

* Direction-adjusted gross return from (t+1) open to 3, 12, and 72 bars.
* Net and gross expectancy per executed trade.
* Profit factor, win rate, MFE, MAE, drawdown, and bars held.
* Alerts per market-day, turnover, exposure, time in market, concurrency, and capacity rejection rate.
* Selection uplift among the common C0 candidate-opportunity set.

## F7. Shared resampling and multiplicity control

* Resampling unit: complete UTC calendar blocks containing all symbols, families, opportunities, and capital decisions.
* Primary block length: seven days using a moving-block construction.
* Sensitivities: 14 and 28 days.
* Use at least **50,000** shared resamples. The same block draws are used for every comparator.
* Keep cross-symbol and cross-family dependence within each block.
* Do not resample isolated trades.
* Use a studentized **max-T** procedure or Romano–Wolf step-down adjustment.

Multiplicity families:

1. **Protocol-control family:** `C-H − C0` for two directions.
2. **Feature family:** seven policies—G1 through G5, K1, K2—times two directions = **14 primary tests**.
3. Legacy `L-B0` and `L-H` are descriptive and make no discovery claim.
4. Secondary horizon, symbol, regime, and attribution results are tested only for a feature-direction pair that passes the primary family. Use Holm adjustment within that secondary family.

`probability_positive` from bootstrap draws must not be labeled a p-value, posterior probability, or probability of future profitability.

## F8. Robustness and stress tests

Every primary feature-direction result must report:

### Structural robustness

* All eight fixed symbols; none may be removed.
* Leave-one-symbol-out results, descriptive but mandatory.
* Listing age at decision: 90–180, 181–365, and >365 days.
* PIT BTC trend regime and volatility tercile.
* Chronological outer fold.
* Signal family.
* Spot-long and futures-short separately.

### Cost and execution stresses

* Base fees, slippage, and strict-prior funding.
* Zero slippage as a diagnostic only, never as a pass condition.
* Two-times adverse slippage.
* 1.5-times fee schedule.
* Combined 1.5-times fees and two-times slippage.
* Additional 5-bp and 10-bp round-trip latency shocks.
* Spread uncertainty:

  * existing all-in slippage assumption;
  * slippage replaced by `max(cohort slippage, 5.625 bp per side)`;
  * conservative slippage plus 5.625 bp per side.
* Funding:

  * strict-prior realized convention;
  * no-funding diagnostic;
  * lower and upper bounds for same-bar stop/funding ambiguity;
  * two-times funding-rate stress.

### Falsification tests

1. Shift each feature forward by one week relative to returns.
2. Permute the feature within symbol × calendar month × UTC time-of-week slot.
3. Sign-flip taker delta.
4. Block-shuffle volume states while preserving alert rate and price opportunities.
5. Apply the accepted feature sign to the opposite direction.
6. Compare true uplift with the maximum placebo uplift under the same multiplicity procedure.

## F9. Genuinely untouched future holdout

Freeze code, lockfile, data schema, rule version, feature formulas, threshold-selection procedure, selected configurations, seeds, and manifest hashes no later than **2026-07-15**.

Prospective confirmatory interval:

* **Start:** 2026-07-16 00:00:00 UTC.
* **End:** 2027-01-16 00:00:00 UTC.
* Fixed eight-symbol panel.
* No replacement for a delisted, unavailable, or data-deficient symbol.
* Operational dashboards may show ingestion health, missingness, alert counts, and latency only.
* No variant-level P&L, expectancy, threshold comparison, symbol contribution, or interim hypothesis result may be inspected.
* Perform one final confirmatory analysis after the fixed end timestamp.
* Do not extend the interval because results are close to significance.
* If there are fewer than 400 common base opportunities or 200 executed trades in a tested direction, label the feature inconclusive and treat it as rejected for deployment.

## F10. Pass and rejection rules

A feature-direction hypothesis passes only if **all** of the following hold in the untouched prospective period:

1. Max-T/Romano–Wolf adjusted one-sided 95% lower bound for the primary (\theta) is above zero.
2. Base-cost total net P&L is positive.
3. Mean net return is at least **+0.05% per trade**, and profit factor exceeds **1.05**.
4. The predeclared fixed-horizon gross effect has the predicted sign.
5. The adjusted lower bound for true-feature uplift minus maximum placebo uplift is above zero.
6. Net performance is nonnegative under the combined 1.5-times-fee and two-times-slippage stress.
7. At least six of eight symbols have positive point estimates.
8. No single symbol supplies more than 35% of total uplift.
9. All three prospective two-month thirds have nonnegative point estimates.
10. Data availability, parity, and deterministic-replay audits pass with no critical exception.

Failure of any condition is a rejection. An inconclusive result is also a rejection for deployment. It must not trigger a new threshold, symbol removal, period extension, indicator substitution, or exit search.

---

# G. Required output tables, plots, audit artifacts, and reproducibility metadata

## G1. Required machine-readable artifacts

1. **`research_manifest.json`**

   * Code-tree SHA-256.
   * Git commit, dirty status, and patch hash.
   * Python version and platform.
   * Exact `uv.lock` hash.
   * Config, experiment specification, threshold grid, rule-version, and family-priority hashes.
   * Raw-data and normalized-data file hashes.
   * Random seeds and resample-draw hash.
   * Start/end timestamps in UTC milliseconds.

2. **`data_quality.json`**

   * Rows expected/observed per symbol, market, and day.
   * Duplicates, ordering errors, grid gaps, incomplete candles.
   * OHLC violations.
   * Negative volumes.
   * (B_t>Q_t), taker-base > base volume, and inconsistent quote/base taker fields.
   * Funding cadence and maximum event gap.
   * Listing-age source and calculation.

3. **`availability.parquet`**

   * Decision key.
   * Feature name.
   * Value.
   * Availability status.
   * Source event time.
   * Receive/observed time.
   * Freshness age.
   * Sample count.
   * Proxy flag.
   * Failure reason.

4. **`opportunity_panel.parquet`**

   * Every evaluated symbol-bar and family.
   * Prior boundary and trigger state.
   * Raw setup strength.
   * Gate component scores and statuses.
   * Accepted/rejected reason.
   * Capacity decision.
   * Forward 3/12/72-bar labels.
   * Feature and configuration version.

5. **`episodes.parquet`**

   * Episode ID.
   * Start/update/confirm/expire/resolve times.
   * Strongest setup strength.
   * Invalidation and expiry.
   * Resolution reason.
   * Restart/recovery metadata.

6. **`decisions.parquet`**

   * Deterministic event ID.
   * Episode ID.
   * Reasons.
   * Invalidation.
   * Rule version.
   * Full as-of audit fields.

7. **`trades.parquet`**

   * Entry and exit event IDs.
   * Next-open entry.
   * Initial and active stops.
   * Exit reason.
   * Gross, fees, slippage, spread stress, funding, and net.
   * MFE/MAE.
   * Position path audit.
   * Split/fold and purge status.

8. **`capital_ledger.parquet`**

   * Cash and notional by timestamp.
   * Open positions.
   * Utilization.
   * Concurrency.
   * Capacity rejections.
   * Realized and unrealized P&L.

9. **`bootstrap_draws.bin` or equivalent**

   * Exact shared block draws or deterministic seed plus implementation/version hash.
   * Max-T studentization data.
   * Family membership.

10. **`verification.json`**

    * Clean-checkout test results.
    * Ruff/Pyright/pytest/compile results.
    * Live-replay versus backtest feature parity.
    * Deterministic rerun hashes.
    * No-order-path static and integration checks.
    * Critical exception count.

## G2. Required tables

1. Severity-ranked defect disposition: open/fixed/accepted risk.
2. Data coverage and point-in-time availability by symbol/day/source.
3. Feature missing, stale, invalid, and proxy rates.
4. Legacy versus corrected comparator metrics.
5. Candidate → eligible → confirmed → scheduled → entered → exited funnel.
6. Gross-to-net cost waterfall.
7. Primary estimand with adjusted confidence intervals.
8. Threshold chosen in every inner/outer fold.
9. Alert rate, turnover, exposure, time in market, and concurrency.
10. Capital utilization and rejected-opportunity table.
11. Symbol, listing-age, direction, regime, and chronological robustness.
12. Fee, slippage, spread, latency, and funding stresses.
13. Falsification and placebo results.
14. Leave-one-symbol-out contribution.
15. Fixed-horizon return, MFE, and MAE table.
16. Exit-reason table clearly labeled as post-entry attribution, not causal exit comparison.
17. Protocol deviations and their disposition.
18. Reproducibility artifact inventory with every expected hash.

## G3. Required plots

* Gross and net cumulative P&L on the capital-aware ledger.
* Thirty-day rolling gross and net expectancy.
* Drawdown and capital utilization.
* Fixed-horizon direction-adjusted return distributions.
* MFE and MAE distributions.
* Setup-strength decile calibration.
* Accepted versus rejected C0 opportunity returns.
* Alert rate, turnover, exposure, and concurrency through time.
* Feature missingness/freshness through time.
* UTC time-of-week activity heatmap.
* Feature correlations and hierarchical redundancy clusters.
* Seasonal-volume and taker-delta drift by year.
* Fold-by-fold and symbol-by-symbol forest plots.
* True-feature versus placebo uplift distributions.
* Cost waterfall at base and stress assumptions.
* Threshold-selection stability across inner folds.

Every plot must show sample counts, exact period, direction, and whether it is exploratory or confirmatory.

## G4. Reproducibility requirements

* One documented clean command must rebuild normalized data, run the frozen experiment, and verify artifact hashes.
* Raw result files must accompany reports; reports alone are insufficient.
* The original lockfile—not a regenerated approximation—must be included.
* The experiment plan and implementation must agree on moving versus fixed blocks.
* The independent verifier must run from a fresh checkout and an empty output directory.
* A/B determinism must be checked after P0 parity repairs.
* Any manual exclusion, repair, or rerun must appear in an append-only audit ledger with reason and hash.
* The published report must distinguish:

  * observed results;
  * code-confirmed defects;
  * inferential diagnoses;
  * exploratory slices;
  * prospective hypotheses.

---

# H. Explicit do-not-do list

* Do not add production order placement, broker adapters, leverage, liquidation, or risk-sizing execution paths.
* Do not make RSI the default 5m foundation.
* Do not interpret the negative net result as primarily a fee or slippage problem.
* Do not assume that another volume indicator creates alpha.
* Do not preserve the current gate-to-confirmation score promotion.
* Do not add volume, trade count, RVOL, CMF, OBV, MFI, VPCI, VWAP, and CVD as compensating points in one composite score.
* Do not treat a full-5m kline taker ratio as equivalent to the latest 60 seconds of `aggTrade`.
* Do not infer true absorption, depth, replenishment, liquidation pressure, or large-trader behavior from 5m OHLCV alone.
* Do not backfill historical order book, liquidations, OI, top-trader ratios, or sub-minute flow with contemporary data or synthetic zeros.
* Do not convert missing, stale, immature, zero-variance, or denominator-invalid observations to neutral zero.
* Do not carry funding or book state beyond a documented freshness limit.
* Do not use a funding event whose timestamp equals or follows the decision timestamp.
* Do not use a higher-timeframe candle at the same 5m close at which it completes.
* Do not use current-bar data in a rolling baseline, fitted scaler, percentile threshold, or regression.
* Do not enter on the signal candle.
* Do not alter entries and exits in the same ablation.
* Do not use favorable exit-reason slices to claim that an exit rule is causally superior.
* Do not call calendar-synchronized but differently selected trade samples “matched trades.”
* Do not call a bootstrap fraction a p-value, posterior probability, or probability of future profit.
* Do not use trade-level IID tests or isolated-trade resampling.
* Do not report an unconstrained sum of fixed-notional trades as account return.
* Do not pool Spot-long and futures-short results to conceal direction or venue asymmetry.
* Do not remove WIF, SUI, a losing symbol, a bad month, or a regime after seeing results.
* Do not select a current top-volume universe and represent it as historically point-in-time.
* Do not search additional thresholds when the frozen grid fails.
* Do not invert a feature’s predicted sign after observing outcomes.
* Do not substitute CMF, MFI, OBV, VWAP, or another indicator when a preregistered volume hypothesis fails.
* Do not extend the prospective period because the result is nearly significant.
* Do not inspect interim variant-level P&L during the untouched holdout.
* Do not call any portion of 2024-07-01 through 2026-07-01 an untouched holdout.
* Do not claim live/backtest exit parity until a live paper-position manager actually uses the shared exit engine.
* Do not claim full reproducibility while the exact lockfile or raw run outputs are absent.
* Do not permit unbounded symbol maps, raw-event storage, database growth, or retained alert history.
* Do not treat deterministic reruns as evidence that the underlying logic is correct.
* Do not deploy a new volume policy unless it passes the untouched prospective rejection rule under base and stressed costs.

**Bottom line:** the present evidence rejects the current strategy, not merely its cost assumptions. Correcting score semantics and point-in-time parity is mandatory, but it is unlikely by itself to reveal a substantial hidden edge. The proposed volume ablation is justified as a tightly bounded falsification exercise—not as a likely route to profitability.

[1]: https://developers.binance.com/docs/derivatives/usds-margined-futures/market-data/rest-api/Get-Funding-Rate-History "Market Data - Futures (USDⓈ-M) REST API | Binance Developer Docs"
[2]: https://developers.binance.com/en/docs/catalog/core-trading-spot-trading/api/rest-api/market "Market - Spot REST API | Binance Developer Docs"
[3]: https://developers.binance.com/en/docs/products/derivatives-trading-usds-futures/websocket-market-streams/Important-WebSocket-Change-Notice "Important WebSocket Change Notice — Base URL Split & Migration | Binance Developer Docs"
[4]: https://github.com/binance/binance-public-data "GitHub - binance/binance-public-data: Details on how to get Binance public data · GitHub"
[5]: https://developers.binance.com/en/docs/catalog/core-trading-derivatives-trading-usd-s-m-futures/api/rest-api/market-data "Market Data - Futures (USDⓈ-M) REST API | Binance Developer Docs"
[6]: https://developers.binance.com/docs/derivatives/usds-margined-futures/market-data/rest-api/Top-Trader-Long-Short-Ratio "Market Data - Futures (USDⓈ-M) REST API | Binance Developer Docs"
[7]: https://developers.binance.com/legacy-docs/derivatives/usds-margined-futures/market-data/rest-api/Taker-BuySell-Volume "Taker Buysell Volume | Binance Open Platform"
