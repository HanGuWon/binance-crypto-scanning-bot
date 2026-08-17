You are GPT-5.6 performing a one-shot, adversarial research and engineering review of an alert-first Binance technical-signal service.

PROJECT AND INVARIANTS
The repository is Python 3.12+ on Windows and uses uv, Ruff, Pyright, and pytest. It consumes public Binance Spot and USDⓈ-M data without API keys and sends evidence-based Discord alerts. It MUST NOT place production orders. Candle decisions may use only fully closed candles. Timestamps are UTC Unix milliseconds. Higher-timeframe candles, funding, trades, and book state must obey strict point-in-time/as-of availability. Missing or stale data must not be silently converted to zero. Histories, queues, caches, and retries must be bounded. Every decision needs reasons, invalidation, rule version, and a deterministic event ID.

CURRENT STRATEGY
The primary decision clock is 5 minutes. Current features cover price/trend, participation, funding/crowding, execution, and completeness. Four non-compensating gates currently require Trend >= 60, Participation >= 60, Crowding Risk < 75, Execution >= 65, and Completeness >= 95. Signal families include squeeze and breakout/breakdown; RSI was deliberately excluded from the main 5m research path and must not become the default foundation merely because it appeared as an example in prior discussion.

The backtest enters at the next 5m open. A closed 15m or 1h candle becomes usable only on a later 5m decision. Funding is strict-prior. Each simulated trade uses fixed 100-USDT notional. Fees, adverse slippage, and futures funding are included. Exits include initial invalidation, opposite signal, three-bar trend failure, a 2-ATR trailing stop activated after 1R, and a maximum duration of 72 bars or 6 hours.

DATA AND RESULTS
The frozen panel is BTC, ETH, BNB, SOL, XRP, DOGE, SUI, and WIF. Data spans 2024-03-01 through 2026-07-01, with evaluation from 2024-07-01 through 2026-07-01. The dataset contains 3,924,696 validated 5m candles and 23,003 funding events. The audit found no duplicate, ordering, time-grid, candle-gap, OHLC, negative-volume, or taker-volume violations. The existing chronological splits have all been inspected retrospectively and are not a genuinely untouched holdout. Deterministic A/B reruns were byte-identical.

Current Headline results are decisively negative:
- Spot long: 26,526 trades; gross P&L -303.03 USDT; net P&L -8,841.02; mean net return -0.3333%; profit factor 0.293.
- Futures short: 26,817 trades; gross P&L +44.03 USDT; net P&L -4,916.39; mean net return -0.1833%; profit factor 0.497.
- With zero slippage, total P&L remains -5,607.93 and -2,632.40 USDT respectively.
- B0 price-only, B3 participation, B2 crowding, and Headline higher-timeframe increments did not establish economically material improvement after paired calendar-block bootstrap and Bonferroni simultaneous intervals.
- Per-symbol, signal-family, exit-reason, and period slices are exploratory.

Critique this negative gross edge before proposing additional indicators. Spot-long gross performance is already negative and futures-short gross performance is approximately zero, so costs are not the primary failure. Do not assume that adding volume indicators will create alpha.

LINK CONTEXT
Read the attached `chatgpt-6a571ced-volume-indicators.md` in full. It contains a faithful structured extraction of the linked conversation plus verified artifact hashes. Also inspect the attached downloaded `volume_analysis.py`, `strategy.py`, README, and SOURCES files. Distinguish the user's hypotheses and examples from the previous GPT's claims. Do not accept either as evidence without checking formulas, data availability, redundancy, point-in-time feasibility, and measured results.

TASK 1 — CORRECTNESS AUDIT
Audit feature construction, gates, score semantics, state transitions, entries, exits, live/backtest parity, and statistical implementation. Look for confirmed leakage, future-row access, timestamp ambiguity, use of unclosed higher-timeframe candles, unavailable funding or flow, hidden selection bias, pseudo-replication, overlapping-position and capital distortions, multiplicity, unrealistic fills or costs, inconsistent live and historical flow definitions, and any issue that could bias the negative result in either direction. Cite exact files, classes, and functions. Separate confirmed defects from plausible hypotheses.

TASK 2 — NEGATIVE-EDGE DIAGNOSIS
Diagnose whether the evidence points primarily to non-predictive entries, overly permissive setup/state transitions, redundant or poorly normalized features, regime mixing, long/short asymmetry, unsuitable exits, excessive turnover, or simulation artifacts. Do not use post-hoc stories, cherry-picked symbols, selected periods, or unregistered threshold searches to rescue the strategy.

TASK 3 — VOLUME-INDICATOR REVIEW
Evaluate every volume-derived idea in the linked context against available historical 5m kline, taker-volume, and funding data. Where justified, consider relative log quote volume with robust point-in-time scaling; time-of-week seasonality; taker imbalance and multi-horizon normalized delta; price-volume efficiency or impact; normalized VPCI; VWAP location/slope/deviation; CMF or accumulation/distribution; OBV; MFI; volume acceleration; breakout participation; dry-up or absorption proxies; and volatility-normalized volume.

For every candidate provide its exact mathematical definition, input columns, rolling windows, minimum history, lag/as-of rule, missingness and freshness behavior, winsorization or robust normalization, expected latency, relationship to current features, and likely redundancy. Classify it as: (1) implementable from existing historical data; (2) redundant with an existing feature; (3) invalid or misleading as a historical proxy; or (4) prospective-only because it needs aggTrade, order-book, liquidation, open-interest, or top-trader history.

TASK 4 — IMPLEMENTATION PLAN
Propose the smallest coherent architecture change that could improve scientific validity and signal quality. Preserve alert-only behavior and every project invariant. For each change identify exact target modules/classes/functions, domain-model and configuration changes, live/backtest parity requirements, bounded-state requirements, positive/negative/boundary tests, failure and missing-data behavior, and any migration or rule-version effect. Prefer orthogonal feature groups and non-compensating availability gates. Explain any change to thresholds, score semantics, state transitions, and exits. Do not provide production order-execution code.

TASK 5 — FROZEN 5M ABLATION
Produce an implementable, preregisterable volume-feature ablation plan. Include a locked predictive hypothesis for every feature group; the current B0 and Headline comparators; one-at-a-time tests and only a small number of predeclared combinations; exact configurations and contrasts; nested walk-forward or development/validation/test rules; purge and embargo rules for overlapping trades or labels; shared calendar-block resampling; multiplicity control; symbol, listing-age, direction, and regime robustness; fee, slippage, spread, and funding stresses; turnover, alert rate, time-in-market, concurrency, and capital-aware portfolio metrics; placebo, sign-flip, permutation, or time-shift falsification tests; and an explicit rejection and stopping rule.

Thresholds may be fitted only within designated development folds under a predeclared deterministic procedure. Prohibit post-hoc threshold tuning, symbol removal, period selection, indicator substitution, repeated test-set inspection, and optional stopping. Because the old final period is exposed, specify how to obtain a genuinely untouched future holdout or prospective paper-trading period. A negative or inconclusive result must reject that hypothesis rather than trigger another unregistered search.

REQUIRED OUTPUT ORDER
A. Executive verdict.
B. Severity-ranked confirmed defects and risks with exact file/function references.
C. Diagnosis of the current negative gross and net results.
D. Volume-indicator evidence matrix containing formula, data, latency, redundancy, feasibility, and status.
E. Prioritized P0/P1/P2 implementation plan with acceptance tests.
F. Frozen experiment matrix, estimands, multiplicity treatment, and pass/reject criteria.
G. Required output tables, plots, audit artifacts, and reproducibility metadata.
H. Explicit do-not-do list.

Label uncertainty. Distinguish observed evidence from inference. Identify every recommendation that depends on the linked context. Be candid if no indicator addition is likely to produce an economically meaningful edge.
