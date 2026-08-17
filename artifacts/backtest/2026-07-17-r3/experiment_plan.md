# R3 exposed-sample diagnostic protocol

Frozen before the R3 replay and before any R3 outcome was inspected.

## 1. Scientific status and scope

- Protocol: `r3_exposed_kline_proxy_diagnostic_v1`
- Rule: `v3.2.0-r3-c0-causal-labels`
- Purpose: repair causal/provenance defects and describe the already exposed
  2024-07-01 through 2026-07-01 sample.
- This is not an untouched holdout, confirmation, trading recommendation, or
  execution validation. Every chronological segment has already been viewed.
- The service remains public-data and alert-only. No private endpoint, account,
  balance, order, signer, or production execution code may be added.

## 2. Inputs and fixed panel

- Closed Binance 5-minute klines only; decision time is the closed candle time.
- Entry proxy is the next contiguous 5-minute candle open.
- Assets are frozen: BTC, ETH, BNB, SOL, XRP, DOGE, SUI, and WIF.
- Markets are reported separately:
  - Spot: existing C0 long trigger only.
  - USDⓈ-M Futures: existing C0 short trigger only.
- The exact 24-file kline/funding input panel is frozen before replay in
  `artifacts/backtest/2026-07-17-r3/input_panel.sha256.json` (SHA-256
  `f382cbe8af0f5c70127984a2fe84766bbc9a3f9b52804dba8444d27971f37045`).
  Final analysis must match the run manifest to this ledger and independently
  re-hash every corresponding file under `data/backtest`.
- Fixed chronological segments:
  - development: 2024-07-01 to 2025-03-01 UTC
  - validation: 2025-03-01 to 2025-11-01 UTC
  - retrospective_test: 2025-11-01 to 2026-07-01 UTC
- Segment names are descriptive. `retrospective_test` must not be called OOS or
  holdout.

## 3. Frozen strategy treatment

- Reuse the corrected C0 20-bar breakout/breakdown trigger and its existing
  EMA20/EMA50, improving MACD-histogram, and ADX >= 20 conditions.
- Do not reactivate or retune H1, G2, or G4. Their exposed R1/R2 results were
  negative after costs and remained negative at zero modeled slippage.
- Do not add an RSI reversal or tune an RSI threshold. RSI may remain an
  informational feature only.
- Do not add Logistic Regression, Random Forest, LightGBM, XGBoost, neural
  networks, ensembles, or probability thresholds to live/Discord behavior in
  this protocol. The current sample has no remaining untouched model-selection
  window.
- A runtime technical-exit alert lifecycle may mirror the frozen paper exit
  policy, but it is PAPER-only, closed-candle, bounded in memory, and cannot call
  an order/account/private endpoint. Runtime exit alerts do not alter R3 entry
  selection, fixed-horizon labels, or the historical T72 estimand. Because the
  lifecycle is not restored across a process restart, alerts must disclose that
  operational limitation rather than imply exchange-position management.

## 4. Causal feature contract

- A decision at time `T` may use only fully closed information strictly
  available before or at that decision under the field's declared clock.
- Cross-symbol breadth at `T` uses, for every symbol, its latest two closed
  5-minute prices with close time strictly less than `T`. Same-close updates are
  excluded and input dictionary/arrival order must not affect the result.
- BTC hourly trend at `T` uses the latest complete hourly trend point strictly
  before `T` and carries that point through a missing hourly bucket.
- Higher-timeframe features remain strict `< T`; no unclosed aggregate candle,
  centered window, future row, or future funding event is allowed.

## 5. Fixed horizons and eligibility

For `H in {3, 6, 12}` bars:

- H=3 is 15 minutes, H=6 is 30 minutes, and H=12 is 60 minutes.
- Entry is `O[t+1]`; fixed-horizon exit is `C[t+H]`.
- The path from the decision through the horizon must be contiguous and remain
  inside one frozen segment.
- The common prediction panel requires the full 12-bar path, a 12-bar
  split-start embargo, and a split-contained exit. It must not require a
  72-bar path.
- The legacy 72-bar technical lifecycle remains a separate sequential ledger.
  It is secondary descriptive output only.
- R2's existing C0 T72 result is labeled `PROTOCOL_MISMATCH` because a
  one-position sequential replay was substituted for the frozen independent
  episode estimand. R3 must not relabel that result as a valid R2 primary test.
  A future independent-episode test requires a separate complete episode ledger
  and a new frozen protocol.

## 6. Costs and outcome labels

The existing fixed cost contract is retained:

- Spot fee: 10 bp per fill.
- Futures fee: 5 bp per fill.
- Spot adverse slippage per fill: 5 bp for anchor/major, 10 bp for volatile.
- Futures adverse slippage per fill: 3 bp for anchor/major, 8 bp for volatile.
- Futures funding includes only events satisfying
  `entry_time < funding_time < exit_time`.

For each eligible horizon, calculate both a long and a short kline-proxy net
return with the applicable market/cohort fee, adverse-execution, and funding
contract. Store both raw values. Freeze the edge margin at 0 bp:

- `KLINE_PROXY_LONG` iff long net > 0 and long net > short net.
- `KLINE_PROXY_SHORT` iff short net > 0 and short net > long net.
- `KLINE_PROXY_FLAT` otherwise.

Equality to zero, equality between actions, and two negative actions are FLAT.
Spot SHORT is a research label meaning decline/no new Spot-long alert or an exit
warning for an existing holding; it is never a new Spot short order. Futures
SHORT remains distinct from a Spot exit.

## 7. Endpoints fixed before replay

Primary descriptive efficacy endpoints, kept separate by market:

- 60-minute mean net return of the allowed C0 direction over the common 12-bar
  raw-opportunity panel.
- 60-minute fixed-notional P&L and profit factor for the same observations.

Secondary endpoints:

- The same metrics at 15 and 30 minutes.
- Label prevalence and directional hit/abstention rates at all three horizons.
- Fee, adverse-execution, funding, and gross-return decomposition.
- 0x and 2x adverse-slippage sensitivity.
- Asset, chronological segment, market-regime, and BTC-trend breakdowns.
- Sequential 72-bar technical-exit trade metrics and exit reasons, explicitly
  labeled non-independent and non-primary.

No horizon may be promoted after viewing its result. Accuracy is not a headline
endpoint and cannot override negative net expectancy.

## 8. Dependence and uncertainty

- Preserve all assets and both markets together within a shared UTC-calendar-day
  moving-block bootstrap.
- Primary block length: 7 days. Sensitivities: 14 and 28 days.
- Replicates: 50,000. Seed: `20260716`.
- Report point estimates, two-sided 95% intervals, invalid-replicate counts, and
  Monte Carlo resolution. Do not use iid row confidence intervals.
- Any one-sided primary decision is descriptive on an exposed sample. If used,
  the Spot and Futures primary family is Holm-adjusted, but p-values do not
  remove researcher exposure or create a confirmatory result.

## 9. Integrity and provenance gates

- Every opportunity has a deterministic `opportunity_id`, rule version, reasons,
  invalidation, decision time, next-open time, segment, and horizon exclusion.
- Every sequential trade stores its originating `opportunity_id`, segment, exit
  reason, and an explicit `split_contained` Boolean. Missing provenance must fail
  closed; no tuple fallback and no default `split_contained=true` are allowed in
  a future inferential analyzer.
- Funding provenance in R3 is fixed at the per-symbol input-file level: the
  analyzer re-hashes the frozen funding files and validates each row's aggregate
  signed funding arithmetic. Opportunity/trade rows do not store individual
  included funding-event IDs or digests, so R3 does not claim independent
  event-level funding reconstruction.
- Under the frozen C0 scheduling contract, a secondary T72 trade may open only
  when its complete 72-bar path and the required following open are contiguous,
  past the 72-bar split-start embargo, and inside one split. Therefore every R3
  T72 trade must have `analysis_eligible_72=true` and `split_contained=true`;
  the engine's generic `split_boundary` exit is not an admissible R3 artifact.
- Replays must remain deterministic, closed-candle only, gap-aware, bounded, and
  free of production order code.
- The prospective `r2_pit_htf_exec` gate must reject BBO age below zero or above
  `book_maximum_age_ms`, including exact boundary tests.

## 10. Status language

R3 reports separate axes rather than one ambiguous PASS/FAIL:

- `data_integrity`: PASS or FAIL
- `kline_proxy_efficacy`: EXPLORATORY_SCREEN_PASS,
  EXPLORATORY_FAIL, or INCONCLUSIVE_LOW_INFORMATION
- `execution_validity`: INCONCLUSIVE_NO_HISTORICAL_BBO
- `generalization`: INCONCLUSIVE_NO_UNTOUCHED_OOS
- `deployment`: NOT_APPROVED

`EXPLORATORY_SCREEN_PASS` requires, for both Spot-long and Futures-short at the
60-minute primary horizon, all of the following: at least 500 eligible
opportunities, at least 120 represented UTC days, mean net return greater than
5 bp, the 7-day one-sided 95% basic-bootstrap lower bound greater than zero,
profit factor greater than 1.05, nonnegative mean at 2x adverse slippage, at
least six of eight assets with positive summed contribution, and no single
positive asset contributing more than 35% of total positive contribution. The
two primary market p-values, if reported, are Holm-adjusted. A 14- or 28-day
sensitivity crossing zero is a fragility flag but is not silently substituted
for the frozen 7-day decision.

If information thresholds are met and either market has nonpositive primary
mean, `kline_proxy_efficacy` is `EXPLORATORY_FAIL`. If bootstrap invalidity
exceeds 0.1%, or an information threshold is not met without a clearly adverse
point estimate, it is `INCONCLUSIVE_LOW_INFORMATION`.

Official archived USDⓈ-M daily bookTicker data ends on 2024-03-30 for this
panel, before the evaluation window, and the official Spot public archive does
not supply a matching historical decision-time bookTicker ledger. Kline-proxy
success cannot change execution validity. Negative proxy expectancy may still
support `EXPLORATORY_FAIL`.

## 11. Stop conditions

- Any future row, same-close cross-symbol leak, unclosed candle, split crossing,
  `split_contained=false` R3 trade, missing required funding input-file provenance,
  input-ledger mismatch, nondeterministic output, or order-related code is an
  integrity failure.
- Regardless of proxy performance, deployment remains NOT_APPROVED until a new
  prospective public-BBO paper period and a genuinely untouched evaluation
  window exist.
