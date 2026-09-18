# Directional Signal Algorithm Review and Referee Report

**Repository:** `<repo-root>`
**Review date:** 2026-09-02
**Report language:** English
**Review type:** Code audit, methodological referee review, and targeted primary-source research
**Decision:** **MAJOR REVISION — REJECT AS A CURRENT PERFORMANCE CLAIM**

## 1. Executive referee decision

The repository should be retained and developed as a safety-conscious market
research and alerting platform. It should **not** presently be described as
having an accurate, calibrated, or profitable upward/downward prediction
algorithm.

The most important conclusion is not that the implementation is careless. The
opposite is closer to the truth: the code contains unusually strong controls for
closed-bar timing, strictly prior higher-timeframe context, fresh best-bid/offer
(BBO) evidence, deterministic identities, immutable provenance, fail-closed
states, and separation of research-only outputs from live candidates. Those are
valuable foundations.

The problem is efficacy. None of the currently exposed algorithms has
established all of the following at once:

1. directional discrimination better than an appropriate no-skill baseline;
2. calibrated probabilities on unseen data;
3. positive expectancy after fees, slippage, and funding;
4. executable decision-time market evidence; and
5. untouched prospective confirmation with dependence and multiplicity handled.

The exposed historical evidence is mostly negative. The frozen R2/R3 screens
showed negative after-cost outcomes and could not establish historical BBO
execution validity. The R4a selector emitted no selections in its retained
report. The Indicator Discriminator failed its validation gate. The retained
three-family studies found gross directional effects too small to survive the
26 bp round-trip execution assumption. R4B V2 Families A/B/C are carefully
engineered hypotheses, but their data binding, inference, and efficacy work is
incomplete.

Accordingly, the referee decisions are:

| Claim or artifact | Decision | Reason |
|---|---|---|
| Repository as causal research infrastructure | **Conditionally retain** | Strong timing, provenance, fail-closed, and non-promotion controls |
| Live R2 as a narrow evidence-first alert candidate | **Major revision** | Causally defined, but efficacy and generalization are not established |
| Existing scores as probabilities | **Reject** | Several scores are explicitly non-probabilistic; calibration is absent or exploratory |
| Existing historical results as proof of accuracy or profit | **Reject** | Results are exposed, negative, incomplete for execution, or not independently reproduced |
| R4B V2 Families A/B/C as validated predictors | **Reject for now** | Mechanisms are frozen, but M2/data integration, inference, and prospective efficacy are incomplete |
| Further prospective research | **Accept with controls** | A locked, parsimonious, outcome-blind program can answer the remaining questions |
| Production order execution | **Out of scope and unauthorized** | The project mission is alert-first; no report finding authorizes order placement |

No methodological change can promise higher accuracy. The recommended program
is designed to make any future improvement measurable, reproducible, and hard
to obtain by chance. It may instead produce a well-supported negative result;
that is a valid and useful outcome.

## 2. Scope, safety boundary, and non-claims

### 2.1 Review scope

This review reconstructs every material repository path that predicts, filters,
ranks, or describes market direction:

- the intrabar `PUMP_RISK`/`CRASH_RISK` anomaly-warning path;
- the frozen live R2 candidate;
- legacy rule families and gates;
- `shadow_er_context_v1`;
- `causal_retest_v1`;
- R4a selective forecasting;
- the Indicator Discriminator/V1A experiment;
- the historical three-family consensus and walk-forward probe;
- R4B V2 Families A, B, and C; and
- Evidence Score V1 plus the directional-evidence successor.

This is a review, not an implementation change. No rule, threshold, model,
production configuration, research result, database, or active campaign was
modified.

### 2.2 Outcome-blind boundary

The active prospective Phase R campaign was treated as sealed. Its structural
contracts may be reviewed, but **no realized Phase R outcome value was read,
summarized, compared, or used to formulate a threshold or model recommendation**.
This is essential: once an outcome is inspected, it is no longer an untouched
test observation.

Historical R2/R3/R4/R4B and indicator results that were already exposed are
used only as retrospective falsification or diagnostic evidence. They are not
re-labelled as independent out-of-sample confirmation.

### 2.3 Mission boundary

The authoritative mission remains a public-data, alert-first Binance market
signal service. This report does not authorize private-account data, order
placement, leverage, portfolio execution, or claims of investment suitability.

## 3. Review method and evidence hierarchy

### 3.1 Method

The review traced each algorithm through six layers:

1. source and event-time authority;
2. feature construction;
3. trigger or model decision;
4. gating, abstention, and state transition;
5. target/outcome definition; and
6. statistical and economic evaluation.

Code was compared with the repository's frozen specifications and retained
reports. The external research review prioritized peer-reviewed papers,
original-author working papers, and official Binance documentation. Marketing
pages, trading blogs, leaderboard claims, and studies whose data horizon could
not be related to this project were excluded from substantive support.

### 3.2 Evidence-authority grades

| Grade | Meaning | Examples in this review |
|---|---|---|
| **A — Current structural evidence** | Directly inspectable code/specification proving what the system computes or forbids | R2 trigger, strict-prior HTF gate, fresh-BBO contract, R4B rule constants |
| **B — Retained frozen result** | Versioned report/artifact with recorded protocol and provenance, not recomputed in this review | R2/R3 retained backtest reports; Indicator V1A retained audit |
| **C — Report-stated result** | A retained report states the value, but this review did not reconstruct it from the raw ledger and model artifacts | R4a exact selection counts |
| **D — Prior probe / unreproduced** | Useful historical negative evidence, but the exact original transform or solver contract was not preserved | Retained 19-fold three-family figures |
| **P — Prospective sealed evidence** | Potentially confirmatory, but deliberately not visible to this review | Active Phase R realized outcomes |

The grade describes evidence authority, not whether a result is favorable. A
negative Grade B result is stronger evidence than a favorable Grade D result.

### 3.3 Independent referee check

An independent, outcome-blind referee reviewed the algorithm map and report
plan. It confirmed the main decision but required two evidentiary corrections:

- the retained 19-fold figures must be downgraded to a prior unreproduced probe
  because the exact transform, intercept penalty, and solver were not preserved
  (`src/signalbot/backtest/historical_three_family_walk_forward.py:1201-1213,
  1453-1457`); and
- the R4a counts must be described as values reported by the retained report,
  not as values independently recomputed in this review.

Those corrections are incorporated throughout this report.

## 4. What is actually being predicted?

The repository contains several outputs that look superficially similar but
answer different questions. Treating them as interchangeable is the main route
to an invalid accuracy claim.

| Quantity | Proper interpretation | Not equivalent to |
|---|---|---|
| Rule score | Deterministic strength or condition summary | Probability of an up/down move |
| Direction label | Sign of a return under a specified horizon and neutral rule | Positive after-cost expectancy |
| Edge label | Whether net return exceeds a specified margin | Raw directional correctness |
| Retest status | Whether a causal touch/recovery lifecycle completed | Probability that the subsequent trade wins |
| Evidence agreement | Signed average of selected descriptive families | Independent votes or calibrated confidence |
| BBO/actionability pass | Market evidence was fresh, narrow, and large enough at decision time | Directional alpha |
| Event-level net return | Outcome of one possibly overlapping opportunity | Realizable portfolio equity |
| Alert precision | Correct alerts divided by emitted evaluable alerts | Recall over all market opportunities |

For a five-minute decision bar closing at time `t`, the new protocol should
define the quantities explicitly. One possible notation is:

- `P_entry(t)`: frozen next-open or decision-time executable reference;
- `r(t,h) = log(P(t+h) / P_entry(t))`: raw horizon return;
- `Y_dir(t,h) in {DOWN, NEUTRAL, UP}`: raw direction after a predeclared neutral
  band;
- `C(t,h)`: fee, adverse slippage, spread/impact, and funding cost under the
  frozen execution contract;
- `r_net(t,h,s) = s * r(t,h) - C(t,h)`, where `s` is long or short; and
- `Y_edge(t,h,s) = 1{r_net(t,h,s) > m}`, for a predeclared economic margin `m`.

A model may estimate `P(Y_dir=UP)`, `P(Y_dir=DOWN)`, or
`P(Y_edge=1 | side=s)`. Those are different estimands and require separate
calibration claims. The current repository sometimes preserves this distinction
well in documentation, but historical reporting can still be misread because
direction, strict hit, edge, and expectancy appear beside one another.

Statistical symmetry does not imply action symmetry. A Spot downside label or
short-direction analysis maps to `SPOT_EXIT`, not a short sale. Only Futures may
map a short-direction result to `FUTURES_SHORT`. The authoritative action contract
makes this distinction explicit and also states that anomaly risk warnings are
not entries (`docs/SIGNAL_SPEC.md:3-10`).

## 5. System map and status

| Algorithm/surface | Population and side | Core output | Horizon | Current authority |
|---|---|---|---|---|
| Intrabar anomaly detector | Allowed liquid symbols; upward and downward realized rapid moves | `PUMP_RISK/RISK_UP` or `CRASH_RISK/RISK_DOWN` warning | Configured seconds | Live surveillance warning, not entry or forecast |
| Live R2 `r2_pit_htf_exec` | Raw Spot breakout-long and Futures breakdown-short only | Boolean candidate plus rule/gate diagnostics | Alert-time; historically evaluated at several horizons | Frozen live candidate, not validated probability |
| Legacy rules | Eight rule families across directions, depending on configuration | Heuristic scores and triggers | Family-specific/alert-time | Available code, not the frozen R2 candidate set |
| `shadow_er_context_v1` | Same raw R2 C0 opportunities | Eight-gate pass/fail | Same event, informational | `unvalidated_shadow_seed`, non-promoting |
| `causal_retest_v1` | Armed raw R2 C0 opportunities | ARMED → TOUCH → READY/terminal lifecycle | Frozen retest lifecycle | Research-only; realized Phase R outcomes sealed |
| R4a | Archived C0 Spot-long/Futures-short events | Calibrated exploratory edge score and selective abstention | 12 bars | Exposed retrospective failure |
| Indicator Discriminator V1A | Frozen seven-asset complete-case event population | Label-free four-axis ECDF score; top quartile | Primary 12 bars, other paths retained | Exposed historical validation failure |
| Three-family consensus/model | Seven alt assets; price, participation, cross-section agreement | Consensus direction or ridge/logistic screen | Primarily 12 bars | Exposed; key 19-fold numbers are prior-probe Grade D |
| R4B Family A | USD-M crowded-position deleveraging reversal | Long/short signal after robust-z gates | 12 bars | Causal engine; efficacy/integration incomplete |
| R4B Family B | Flow/depth continuation or absorption/reversal | B1 same-flow or B2 opposite-flow signal | 3 bars | Causal engine; efficacy/integration incomplete |
| R4B Family C | Cross-sectional common shock and laggard catch-up | Same-shock direction for top-decile laggards | 6 bars | Causal engine; target self-inclusion and integration issues |
| Evidence Score V1 | Six information families | Signed capped average and bias | Descriptive | Explicitly non-probabilistic |
| Directional successor | Price, participation, target-excluded cross-section; context separated | Three-family descriptive agreement | Descriptive | Non-promoting and M0/M1/M2-unbound |

## 6. Algorithm-by-algorithm review

### 6.1 Frozen live R2 candidate

#### Implemented logic

The authoritative specification restricts R2 to Spot `BREAKOUT_LONG` and
Futures `BREAKDOWN_SHORT` (`docs/SIGNAL_SPEC.md:23-53`). The underlying rule is
a closed-bar conjunction:

- Spot long: close above the prior lookback high while the previous close was
  not above that high; positive and improving MACD histogram; `ADX >= 20`; and
  `EMA20 > EMA50` (`src/signalbot/signals/rules.py:450-494`).
- Futures short: the symmetric breakdown, negative and weakening MACD,
  `ADX >= 20`, and `EMA20 < EMA50`
  (`src/signalbot/signals/rules.py:496-540`).

The recent high/low excludes the current bar
(`src/signalbot/indicators/core.py:422-428`). R2 then requires strictly earlier
15-minute and one-hour contexts, with close/EMA20/EMA50 aligned in the signal
direction (`src/signalbot/signals/gates.py:107-141`). It also requires observed,
non-proxy BBO evidence, a bounded non-negative age, a maximum spread, and enough
top-of-book quote capacity for the configured notional
(`src/signalbot/signals/gates.py:47-104`).

The example configuration enables this gate, disables participation and
crowding selection, and uses explicit-trigger confirmation
(`config/settings.example.yaml:45-69`). The state machine confirms only a
triggered and eligible non-informational evaluation
(`src/signalbot/signals/state_machine.py:91-141`).

#### Strengths

- The breakout boundary is point-in-time and excludes the current close.
- The higher-timeframe condition is explicitly *strictly prior*, blocking a
  common same-close look-ahead error.
- BBO freshness and capacity fail closed rather than falling back to a candle
  proxy.
- The trigger is non-compensating: a high score cannot repair a failed core
  condition.
- The specification says that score values are rule strengths, not
  probabilities (`docs/SIGNAL_SPEC.md:17-21`).

#### Limitations

1. **It is not a general rise/fall classifier.** It emits only one side per
   market. Spot/long and Futures/short are confounded with venue, fee structure,
   funding, and market microstructure. It cannot measure recall over all upward
   or downward market moves.
2. **The triggered rule score has no ranking resolution under frozen R2.** With
   `gate_enabled=true`, the optional relative-volume, taker-ratio, and regime
   score components are excluded. Every fully triggered breakout/breakdown has
   the same `30 + 15 + 10 + 10 = 65` rule score. The score can describe partial
   setups, but it cannot rank confirmed R2 candidates
   (`src/signalbot/signals/rules.py:450-540`).
3. **HTF and BBO gates answer different questions.** HTF may alter directional
   selection; BBO primarily establishes present actionability. Passing BBO does
   not create alpha.
4. **Historical execution parity is absent.** The retrospective R2 protocol
   lacks decision-time BBO, depth, and receipt time, so its full live eligibility
   is always inconclusive (`docs/SIGNAL_SPEC.md:124-148`).
5. **Alert-state selection matters.** Cooldown and explicit state transitions
   change which otherwise eligible events become emitted alerts. Research must
   report both raw-opportunity and emitted-alert populations.
6. **The completeness score is not temporal completeness.** It is a weighted
   field-presence score: 70 base points, 20 for canonical flow, and 10 for spread
   (`src/signalbot/indicators/core.py:567-572`). It does not prove contiguous
   candles, exchange completeness, or absence of source gaps.

#### Referee assessment

R2 is a defensible causal *candidate generator*, not a validated predictor. Its
binary structure is worth preserving as a benchmark. It should not be optimized
by adding more hand-set conjuncts until a common population, target, baseline,
and untouched evaluation protocol are in place.

### 6.2 Intrabar `PUMP_RISK` and `CRASH_RISK` anomaly warnings

`AnomalyDetector` maintains bounded mini-ticker price histories for allowed
symbols and evaluates both upward and downward risk families after each valid,
in-order observation. Invalid, disallowed, out-of-order, or liquidity-unknown
observations cannot silently clear a prior warning
(`src/signalbot/data/anomaly.py:21-44`).

Once there are enough incremental returns, it compares the realized return over
the configured seconds horizon with an absolute-return floor and a robust
median/MAD z-score threshold (`src/signalbot/data/anomaly.py:80-107`). A positive
realized anomaly becomes `PUMP_RISK`/`RISK_UP`; a negative anomaly becomes
`CRASH_RISK`/`RISK_DOWN`. Its 70–100 score is a bounded severity construction
from excess robust-z and absolute realized return
(`src/signalbot/data/anomaly.py:161-195`).

This path detects a move that is already occurring. It does **not** forecast the
next bar, estimate a probability, imply continuation or reversal, or authorize
an entry. It belongs in operational anomaly detection rather than the predictive
model leaderboard. Useful validation endpoints are detection latency, duplicate
suppression, false warnings per day, liquidity/missing-data behavior, and whether
the warning arrives before a separately defined adverse-risk event. Future
direction after a warning may be studied only as a new, explicitly labelled
research question.

### 6.3 Legacy rule families and gates

`RuleEngine.evaluate` constructs eight families, including breakout,
breakdown, pullback, squeeze, and reversal-style rules
(`src/signalbot/signals/rules.py:40-71`). These paths use additive heuristic
weights, fixed technical thresholds, and configuration-dependent gate behavior.
Pullbacks have additional causal structure safeguards, but they do not expand
the frozen live R2 set (`docs/SIGNAL_SPEC.md:100-122`).

These rules are useful as a catalog of hypotheses and as fixed baselines. They
are not a coherent probabilistic ensemble. Adding several correlated trend
features to a score does not produce independent evidence, and comparing many
families, thresholds, sides, horizons, and regimes on the same exposed period
creates a large effective multiplicity family.

Recommended disposition:

- freeze the legacy outputs as named baselines;
- prohibit post-hoc rescue by changing weights or thresholds on exposed data;
- evaluate every family on the same all-opportunity diagnostic population;
- retain family-specific causal and execution metadata; and
- do not average legacy scores into a probability.

### 6.4 `shadow_er_context_v1`

The shadow successor is an eight-part Boolean conjunction over the frozen raw
trigger: raw completeness, strictly prior HTF alignment, optional BTC context,
EMA/ER20 trend efficiency, relative volume, an ATR anti-chase bound, ATR cost
headroom, and fresh BBO evidence
(`src/signalbot/signals/shadow_policy.py:59-140`). Defaults include ER20 `0.40`,
maximum breakout distance `0.50 ATR`, 26 bp round-trip cost, and `2x` cost
headroom (`src/signalbot/config.py:88-92`).

The design correctly labels the result `informational_only` and
`unvalidated_shadow_seed` (`src/signalbot/signals/shadow_policy.py:148-176`).
That protection must remain.

The main methodological issue is that the shadow is a manually specified
selection policy with several correlated volatility/trend/activity conditions.
Every added conjunction can improve apparent precision simply by reducing
coverage. Its evaluation therefore requires a risk-coverage curve, alert-day
coverage, event counts by side and asset, and a paired comparison on the *same*
raw-C0 opportunities. A higher hit rate at near-zero coverage is not an
improvement.

Recommended experiment: an immutable staged ablation on a common population:

`C0 → +HTF → +ER/context → +anti-chase/headroom → +BBO`.

The directional contribution and the execution contribution must be shown
separately. The active prospective campaign must remain sealed until its frozen
analysis unlock.

### 6.5 `causal_retest_v1`

The retest path is a research-only lifecycle, not a direction model. It arms
only the same Spot-long/Futures-short R2 families and freezes the recent
high/low breakout level (`src/signalbot/prospective/retest.py:1-11,261-306`). A
later long bar touches when its close is at or below the breakout level and
recovers when a subsequent close is above it; short is symmetric
(`src/signalbot/prospective/retest.py:417-426`). It enforces strictly later and
monotonic times, an idempotent bar fingerprint, a hard horizon, and censorship
when causal READY evidence is missing (`src/signalbot/prospective/retest.py:451-550`).

This is strong causal engineering. The deliberate close-only semantics avoid
inventing an intrabar sequence from OHLC data. They also miss a bar that trades
through the level and closes recovered, because the same close cannot satisfy
both mutually exclusive conditions. That is not a defect if documented; it is
a specific estimand.

The future protocol should compare two preregistered versions only after fresh
data authority exists:

- `close_touch/close_recovery`, the current conservative lifecycle; and
- an exact trade- or sequence-authoritative intrabar touch/recovery lifecycle.

OHLC high/low ordering must not be used to simulate the second version. The
evaluation should measure whether retesting changes conditional direction and
after-cost edge, not merely whether READY events look visually cleaner.

### 6.6 R4a selective forecast

R4a predicts whether archived C0 net return exceeds a +5 bp edge margin. It uses
six numeric features—breadth, taker delta over 3 and 12 bars, and three
transformed VPCI terms—plus cohort, regime, BTC trend, and HTF acceptance
(`src/signalbot/backtest/r4.py:136-176,566-579`). Asset identity, setup strength,
ADX threshold, and future returns are explicitly prohibited features.

The fold structure is a genuine strength: expanding training, a separate
calibration window, monthly tests, and horizon gaps prevent direct label overlap
(`src/signalbot/backtest/r4.py:342-420`). The classifier is mixed
Gaussian/categorical naive Bayes. A temperature/intercept pair is chosen on the
calibration slice by log loss (`src/signalbot/backtest/calibration.py:31-221`).
Selection requires both a probability threshold and expected-net threshold
(`src/signalbot/backtest/r4.py:444-475,646-657`).

The retained R4a report states that Spot selected `0/25,593` and Futures selected
`0/26,514`, with small positive all-population Brier skill but no selected-event
sample (`artifacts/backtest/2026-07-17-r4/r4a/r4_report_ko.md:10-13`). These are
**Grade C report-stated values**, not independently recomputed figures. The same
report labels the experiment `EXPLORATORY_FAIL`, without untouched OOS or
historical BBO authority (`r4_report_ko.md:3-6,61-64`).

Major limitations:

- Gaussian naive Bayes assumes class-conditional feature independence. D3,
  D12, VPCI level, signal, and slope are economically and statistically
  related. The resulting log odds can double-count evidence.
- Global temperature/intercept calibration can correct average scale and shift,
  but cannot create missing feature interactions or repair subgroup-specific
  miscalibration.
- The grid is coarse and was part of an exposed research process. Low ECE alone
  is not proof of useful tail calibration.
- Expected net return is a probability-weighted mixture of cohort/class sample
  means (`src/signalbot/backtest/r4.py:616-643`). Tail selection can be unstable
  when those means are sparse or nonstationary.
- The matched-random comparator matches only fold and cohort, not asset,
  direction, regime, event time, or opportunity density
  (`src/signalbot/backtest/r4.py:814-845`).
- `one_sided_p_value` is the fraction of ordinary, uncentred bootstrap means at
  or below zero (`src/signalbot/backtest/r4.py:758-810`). It is not an explicit
  null-centred hypothesis test. Passing that quantity into Holm does not turn it
  into a valid family-wise p-value.
- The implemented Holm family contains only Spot and Futures
  (`src/signalbot/backtest/r4.py:848-865`), not the models, calibration grids,
  thresholds, horizons, features, and earlier variants examined during research.

R4a should remain a failed, useful baseline. It should not be rescued by lowering
the selection threshold on the exposed sample.

### 6.7 Indicator Discriminator and V1A

The Indicator Discriminator uses eight features organized into four axes. It
fits label-free empirical CDFs on development data, averages within each axis,
then equally averages the four axis percentiles. The top-quartile cutoff is also
fit on development. The source explicitly sets `score_is_probability=false`
(`src/signalbot/backtest/indicator_analysis.py:21-50,109-145,219-250`).

The label-free design substantially limits direct outcome overfitting. V1A also
freezes three chronological splits, seven assets, 1/3/6/12/72-bar outcomes, a
72-bar split-start embargo, and strict complete-case handling
(`src/signalbot/backtest/indicator_analysis_v1a.py:1-120,294-355`). These are
strong controls.

The retained validation audit reports that the top quartile retained 799
observations, achieved 48.69% strict directional accuracy, and improved by
1.019 percentage points with a 95% interval of `[-2.005, +4.012]` points. The
median directional return was zero and the gate failed
(`docs/r4b-v2-cost-survival-candidate-audit-2026-07-21.md:61-64`).

The correct interpretation is that an intuitively reasonable equal-weight
indicator score did **not** establish a monotonic or statistically reliable
ranking of directional correctness. The score may remain useful as a descriptive
annotation, but it should not be promoted or relabelled. A new weighted score
fit to these exposed outcomes would be a new model requiring a new candidate
registration and fresh confirmation.

### 6.8 Historical three-family consensus and walk-forward probe

The three-family surface combines price, participation, and cross-sectional
directional strengths. The broad `3-of-3` consensus was tested on only BONK,
ENA, WIF, FLOKI, ARB, OP, and SEI, so it cannot establish BTC/ETH or broad-universe
generalization (`docs/r4b-v2-cost-survival-candidate-audit-2026-07-21.md:30-31`).

The retained audit shows that gross mean returns were close to zero at the
5/15/30/60-minute horizons, while a 26 bp zero-move round trip made every broad
side/horizon cell negative after cost. More agreeing families did not show
monotonic improvement over conflicted `2-of-3` buckets, and reversing every
direction also left all ten tested cells negative after cost
(`r4b-v2-cost-survival-candidate-audit-2026-07-21.md:33-75`).

The current walk-forward implementation specifies a 180-day initial training
window, 72-bar embargo, 30-day test windows, a 12-bar target, ridge and logistic
regularization of 10, and the gate `ridge expected net > 0` plus uncalibrated
logistic score `> 0.5`
(`src/signalbot/backtest/historical_three_family_walk_forward.py:42-54,420-478`).
It uses side, the three signed strengths, absolute agreement, cost/ATR, and
training-universe asset one-hot features. An unseen asset becomes an all-zero
one-hot vector (`historical_three_family_walk_forward.py:263-305`).

The often-cited prior 19-fold figures—`n=2,869`, mean net `-25.53 bp`, PF
`0.529`, and strict hit `36.67%`, with every fold aggregate negative—remain
useful negative context (`docs/r4b-v2-cost-survival-candidate-audit-2026-07-21.md:77-86`).
They are **Grade D**, however: current code explicitly records the prior probe as
unreproduced/not authoritative because its exact scaler, solver, and penalty
contract was not retained
(`src/signalbot/backtest/historical_three_family_walk_forward.py:1201-1213,
1453-1457`).

This distinction matters. The negative result argues against assuming that
agreement creates cost-surviving edge, but it should not be cited as a newly
reproduced benchmark. A new run must use the current frozen implementation and
produce new immutable artifacts; because the history is already exposed, that
run is still diagnostic rather than confirmatory.

### 6.9 R4B V2 Family A: crowded deleveraging reversal

Family A is a 12-bar USD-M hypothesis. It requires strong prior 12-bar return,
open-interest, basis, and funding alignment, followed by a one-bar reversal,
open-interest contraction, and opposing flow. The frozen thresholds are robust-z
values `1.5`, `1.5`, `1.5`, `1.0`, `-0.5`, `-1.0`, and `-0.35`
(`src/signalbot/r4b_v2/strategy/family_a.py:37-50,2662-2698`). If the crowd sign
is positive it selects short; if negative it selects long
(`family_a.py:2490-2566`).

The causal interpretation is coherent: identify a crowded trend and act in the
opposite direction only after deleveraging/reversal evidence. The implementation
also treats missing feature readiness and active positions explicitly.

What remains unknown is whether these seven thresholds identify a repeatable
mechanism, whether the robust-z reference windows are stable across symbols, and
whether the 12-bar exit horizon matches the mechanism. Threshold provenance and
the complete candidate family must be registered before efficacy analysis.

### 6.10 R4B V2 Family B: flow/depth continuation versus absorption

Family B has a three-bar hard horizon. It requires absolute robust-z flow
imbalance of at least `2.0` and spread95 no greater than 20 bp. B1 follows flow
when return is aligned, opposing depth is depleted, and recovery remains limited.
B2 takes the opposite side when price response is small and depth replenishes
(`src/signalbot/r4b_v2/strategy/family_b.py:38-49,2295-2357`).

Separating continuation (B1) from absorption/reversal (B2) is preferable to
forcing one global interpretation of order flow. The key empirical risk is that
depth features depend on exact sequence-valid order-book reconstruction and
latency. A candle proxy or incomplete diff-depth book would change the feature,
not merely add noise. Family B therefore cannot receive an efficacy verdict
before M2 source completeness, snapshot bridging, receipt clocks, and execution
parity are complete.

### 6.11 R4B V2 Family C: common shock and laggard catch-up

Family C uses 8,640 prior observations, requires at least 20 panel members, has
a six-bar hard horizon, clips beta to `[0.25, 2.5]`, requires a market shock
score of `2.5`, 70% breadth, and a lag score of `1.5`. It selects a fixed top
decile of laggards and trades in the direction of the market shock
(`src/signalbot/r4b_v2/strategy/family_c.py:28-42,3307-3453`).

The current core constructs each historical market return and current three-bar
shock as a median over **all** panel members, including the target symbol. It
then estimates that same target's beta/residual and ranks it against the
endogenous market series (`family_c.py:3166-3218`). This is not future leakage;
all data are contemporaneously available. It is target self-inclusion. The
effect is largest in smaller panels and can change residual scale, lag score,
rank, and threshold passage.

The successor cross-sectional evidence already adopts a target-excluded design.
Any new Family C version should freeze target-excluded market factors and run a
paired ablation against the current frozen version. The current rule must not be
silently edited because doing so would destroy version identity.

### 6.12 Evidence Score V1 and the directional successor

Evidence Score V1 atomically requires six capped information families and
prevents exact feature-slice ownership collisions
(`src/signalbot/r4b_v2/strategy/evidence_producer.py:327-487`). It averages the
six signed strengths and explicitly says it creates neither a probability nor a
signal (`src/signalbot/r4b_v2/strategy/evidence_score.py:378-480`).

The original conceptual problem is that volatility, derivatives positioning,
and liquidity/execution do not always possess an honest bullish/bearish sign.
Including them in a signed six-family average can obscure rather than clarify
direction. The successor correctly limits directional aggregation to price,
participation, and cross-sectional state, while treating volatility,
derivatives, and liquidity as context
(`src/signalbot/r4b_v2/strategy/directional_evidence.py:19-42,365-440`).

The successor is still explicitly non-promoting and has source status
`LEGACY_OBSERVATIONS_M0_M1_M2_UNBOUND`. The repository completion matrix records
missing participation/cross real adapters, primary binding, M2 source census,
calibration, and final inference (`docs/r4b-v2-completion-matrix.md:17,27-28`).

Dependency ownership prevents the *same economic slice* from being counted by
two named families. It does not prove statistical independence. Price momentum,
taker participation, and a cross-sectional shock can remain highly correlated.
The future model must estimate incremental value through locked ablations or a
regularized joint model rather than treating three family names as three
independent votes.

## 7. Historical evidence synthesis

The following table deliberately distinguishes retained evidence from a fresh
reproduction.

| Surface | Retained result | Authority | Referee interpretation |
|---|---|---|---|
| R2 C0, 60-minute fixed horizon | Spot long about `-0.324%`; Futures short about `-0.234%` | Grade B retained report | Raw candidate did not show cost-surviving edge |
| R2 H1/technical exits | All tested assets negative in key T72 summaries; 0x-slippage P&L still negative | Grade B retained report | HTF abstention reduced exposure but did not establish positive conditional efficacy |
| R3 proxy | Spot long `n=39,705`, `-32.4346 bp`, PF `0.4198`; Futures short `n=41,237`, `-23.4364 bp`, PF `0.5532` | Grade B retained report | Broadly negative across assets/splits/regimes; no untouched OOS or BBO |
| R4a | Reported `0/25,593` Spot and `0/26,514` Futures selected | Grade C report-stated | No selected sample on which to claim selective precision |
| Indicator V1A top quartile | `n=799`, strict accuracy `48.69%`, uplift `+1.019 pp`, 95% interval `[-2.005,+4.012] pp` | Grade B retained audit | Failed to establish ranking value |
| Broad three-family 3-of-3 | Gross near zero at short horizons; every 26 bp after-cost cell negative | Grade B retained audit | Agreement did not clear cost; more votes not monotonic |
| Three-family 19-fold figures | `n=2,869`, `-25.53 bp`, PF `0.529`, strict hit `36.67%`; no fixed gate selected | Grade D prior probe | Negative context only; not authoritative/reproduced under current contract |
| R4B A/B/C | No completed efficacy evidence in the current completion matrix | Grade A structural status | Engineering tests must not be translated into alpha |
| Active Phase R | Not inspected | Grade P sealed | The only currently collecting prospective evidence under its frozen protocol; future newly frozen intervals can also be untouched |

Sources: `artifacts/backtest/2026-07-16-r2/final_report_ko.md:4-18,173-182,244-280`;
`artifacts/backtest/2026-07-17-r3/run/r3_final_report_ko.md:3-21,43-114`;
`artifacts/backtest/2026-07-17-r4/r4a/r4_report_ko.md:3-19,61-64`;
`docs/r4b-v2-cost-survival-candidate-audit-2026-07-21.md:1-127`.

The practical conclusion is that the dominant problem is not yet fine threshold
tuning. At the retained 26 bp round-trip assumption, the tested gross signal is
usually close to zero relative to the cost hurdle. A model must first establish
incremental *gross* directional information, then demonstrate that selection
concentrates enough of it to survive execution costs.

## 8. Ranked referee findings

### Critical 1 — No validated directional-performance claim exists

**Evidence.** Retained R2/R3 results are negative after cost; R4a selects none;
the indicator gate fails; the three-family gross edge is small; R4B efficacy is
unfinished; prospective outcomes remain sealed.

**Consequence.** Words such as “accurate,” “high probability,” “predictive
edge,” or “profitable” would exceed the evidence.

**Falsification test.** A locked candidate must beat preregistered baselines on
an untouched prospective interval, under exact execution evidence, calibrated
losses, dependence-aware confidence bounds, and the full multiplicity ledger.

**Remediation.** Keep all current outputs research-only and follow the staged
program in Sections 11–14.

### Critical 2 — Historical candle replays cannot establish live actionability

**Evidence.** The frozen R2 retrospective screen lacks decision-time BBO,
top-of-book quantity/depth, and receipt time; the specification therefore marks
full R2 status inconclusive. Family B additionally requires exact sequence-valid
depth.

**Consequence.** A candle-level directional result cannot answer whether an
alert was executable at the displayed cost and size.

**Falsification test.** Reproduce the decision from exact closed candles plus
source-event/receipt clocks, BBO/depth state, fee version, funding state, and
unresolved-fill policy.

**Remediation.** Finish and certify public-data capture before economic claims;
retain candle replay only as an alpha diagnostic.

### Critical 3 — The active prospective sample must remain untouched

**Evidence.** Research governance freezes thresholds and forbids outcome-driven
selection before unlock (`docs/RESEARCH_GOVERNANCE.md:21-34,62-75`).

**Consequence.** Inspecting Phase R to choose features or thresholds would turn
the only prospective sample into development data.

**Falsification test.** Verify that the code/configuration hash, candidate
ledger, unlock rule, and source authority predate outcome access.

**Remediation.** Complete research design without opening outcomes. If any
candidate changes, start a new untouched confirmation interval.

### Major 1 — Selection-conditioned alerts are being confused with general direction

**Evidence.** R2 and R4a cover only Spot-long/Futures-short raw C0 events.

**Consequence.** Neither overall market direction accuracy nor missed-move
recall is identifiable. Venue and side effects are confounded.

**Falsification test.** Create an all-bar diagnostic population and symmetric
long/short labels while preserving the opportunity-conditioned alert study.

**Remediation.** Report two panels: all-bar directional discrimination and
opportunity-conditioned selective precision. Never merge their denominators.
For action reporting, map a Spot downside result to `SPOT_EXIT`; only Futures
short-direction results may map to `FUTURES_SHORT`.

### Major 2 — Score, agreement, and calibrated probability are not interchangeable

**Evidence.** R2 trigger scores can be constant; the Indicator and Evidence
Score explicitly say they are not probabilities; the three-family logistic
score is uncalibrated; R4a calibration is exploratory.

**Consequence.** A `/100` display or sigmoid output can be mistaken for a
frequency claim it has not earned.

**Falsification test.** On outer unseen folds, measure Brier/log-loss skill,
calibration intercept/slope, and reliability by side, market, horizon, and
selectivity bucket.

**Remediation.** Reserve the word “probability” for a versioned calibrated model
that passes those tests. Render every other value as `strength`, `agreement`, or
`descriptive score`.

### Major 3 — Accuracy can be increased mechanically by abstaining

**Evidence.** Shadow, R4a, and top-quartile filters select subsets. R4a selected
zero events in the retained report.

**Consequence.** Precision without coverage can be undefined, unstable, or
operationally useless.

**Falsification test.** Plot error/precision against coverage and alert budget,
including zero-alert days and bootstrap uncertainty.

**Remediation.** Freeze minimum event, day, side, asset, and regime coverage.
Report precision, recall, false alerts/day, abstention reasons, and risk-coverage
together.

### Major 4 — Overlap and clustered dependence invalidate row-wise inference

**Evidence.** Fixed-horizon events overlap and repository specifications already
warn that their sums are not a realizable portfolio
(`docs/BACKTEST_SPEC.md:40-50`). Crypto symbols also share common shocks.

**Consequence.** Row counts overstate effective sample size; confidence intervals
can become too narrow; simultaneous events can dominate results.

**Falsification test.** Use a shared calendar-block schedule, sensitivity across
block lengths, event-cluster summaries, and a non-overlapping position ledger.

**Remediation.** Keep event-level discrimination and portfolio/equity analysis
separate. Purge labels and bootstrap calendar days including no-alert days.

### Major 5 — The true multiplicity family is much larger than two markets

**Evidence.** The research history spans families, sides, horizons, filters,
thresholds, calibration grids, regime splits, reversals, and exit rules. R4a
Holm adjusts only Spot/Futures.

**Consequence.** A nominally significant result can be the best survivor of a
large search.

**Falsification test.** Reconstruct a candidate ledger containing every tested
hypothesis and endpoint; apply a valid family procedure to the registered
family and reserve final confirmation for new data.

**Remediation.** Freeze a small model ladder and primary endpoint. Use White's
Reality Check or Hansen's SPA only when their assumptions and the complete model
universe are defensible; use PBO and the
[Deflated Sharpe Ratio](https://doi.org/10.2139/ssrn.2460551) as overfit
diagnostics, not as substitutes for prospective confirmation.

### Major 6 — R4a inference and comparison need repair

**Evidence.** Correlated naive-Bayes features, coarse global calibration,
uncentred bootstrap tail labelled a p-value, sparse conditional means, and weak
fold/cohort-only matching.

**Consequence.** Apparently calibrated scores may be misranked in the tail, and
Holm-adjusted “significance” would not have a sound null basis.

**Falsification test.** On identical outer folds compare climatology, regularized
logistic, current NB, and shallow boosted trees; use cross-fitted calibration and
a valid paired loss/return inference procedure.

**Remediation.** Retain R4a unchanged as a failed baseline; implement a new
version rather than editing the exposed one.

### Major 7 — Feature ownership does not imply independent evidence

**Evidence.** The V2 ledger blocks exact slice aliases, but price, participation,
and cross-sectional features can respond to the same market shock.

**Consequence.** Equal voting can double-count a latent factor and make agreement
look stronger than it is.

**Falsification test.** Measure foldwise correlation, conditional incremental
loss improvement, permutation/leave-one-family-out effects, and stability.

**Remediation.** Use one capped feature block per economic family and a
regularized joint model or locked ablation. Do not award an automatic vote per
family name.

### Major 8 — Targets conflate direction, magnitude, cost, and path

**Evidence.** Historical studies report sign, strict hit, fixed-horizon net
return, MFE/MAE, and technical exits. R4a directly targets net return above a
margin.

**Consequence.** A model can be directionally correct but economically negative,
or miss the terminal sign while identifying a valuable intrahorizon path.

**Falsification test.** Preserve raw multi-horizon paths and compare separate
direction, magnitude, path/barrier, and after-cost targets.

**Remediation.** Use a primary 12-bar direction target plus family-specific
3/6/12 horizons and a separate economic head. Missing/unevaluable outcomes must
remain explicit, never silently zero.

### Major 9 — Cross-sectional self-inclusion biases Family C's benchmark

**Evidence.** Current per-time market medians contain the target whose residual
and lag rank are subsequently calculated.

**Consequence.** The target partly determines its own benchmark; effect size and
ranking can change with panel size and composition.

**Falsification test.** Run a frozen paired current-versus-target-excluded
ablation on the same causal panels and folds.

**Remediation.** Version a target-excluded factor for new research while
preserving the current rule's identity.

### Major 10 — Asset and regime generalization remain unknown

**Evidence.** Several studies use fixed seven/eight-asset panels; the historical
three-family panel omits BTC/ETH. R4a pools assets while prohibiting identity.

**Consequence.** Results may reflect a particular liquidity tier, listing cohort,
or common calendar period.

**Falsification test.** Add leave-one-asset-out diagnostics, liquidity/cohort
holdouts, calendar stress periods, and a frozen point-in-time universe.

**Remediation.** Prefer partial pooling or carefully regularized group effects to
unrestricted symbol-specific fitting. Report concentration and unseen-asset
performance.

### Moderate 1 — `data_completeness` is a field-presence score

**Evidence.** The value is constructed from fixed 70/20/10 weights rather than
interval continuity or source-census evidence.

**Consequence.** The name can overstate data authority.

**Remediation.** Rename it `feature_presence_score` in a versioned schema and add
separate contiguous-bar, source-gap, receipt-latency, and M2 completeness fields.

### Moderate 2 — Bollinger-width percentile includes the current observation

**Evidence.** The reference slice ends at `index + 1`
(`src/signalbot/indicators/core.py:429-434`), unlike point-in-time z-scores that
use prior values only (`core.py:454-460`).

**Consequence.** This is not future leakage, but the current value helps define
its own empirical percentile and weakens comparability with strictly prior
features.

**Remediation.** Freeze a prior-only version and test the difference; do not
silently change historical rule identity.

### Moderate 3 — Close-only retest semantics omit intrabar recoveries

**Evidence.** Touch and recovery use mutually exclusive terminal closes.

**Consequence.** Valid intrabar retests are censored or delayed; selection can
differ materially in volatile symbols.

**Remediation.** Keep the conservative version and test a separately versioned
trade-sequence implementation once exact source authority exists.

### Moderate 4 — The model lacks explicit drift and calibration monitoring

**Evidence.** The repository freezes experiments well, but a validated model
would still operate under changing volatility, liquidity, fee, and participant
regimes.

**Consequence.** Historical calibration can decay even when code is unchanged.

**Remediation.** Monitor feature distributions, outcome prevalence, calibration
intercept/slope, residuals, coverage, and cost error. Drift alarms should pause
claims or start a new research version, not automatically retune production.

## 9. Primary-source research synthesis

The literature supports a disciplined evaluation architecture and several
candidate feature families. It does **not** establish that those features will
improve this five-minute Binance system.

| Research result | Implication for this project | Transfer limitation |
|---|---|---|
| Proper scoring rules reward honest probabilistic forecasts ([Gneiting & Raftery, 2007](https://doi.org/10.1198/016214506000001437)) | Use log loss and Brier skill for probability claims, not accuracy alone | Proper scoring does not prove economic value |
| Common classifiers often need post-hoc calibration; naive Bayes can be overconfident ([Niculescu-Mizil & Caruana, 2005](https://doi.org/10.1145/1102351.1102430)) | Compare Platt/logistic, isotonic, and simple temperature scaling on unseen calibration folds | Dataset and model behavior differ from crypto time series |
| ECE can conceal material calibration differences ([Nixon et al., 2019](https://openaccess.thecvf.com/content_CVPRW_2019/html/Uncertainty_and_Robustness_in_Deep_Visual_Learning/Nixon_Measuring_Calibration_in_Deep_Learning_CVPRW_2019_paper.html)) | Report slope/intercept and reliability curves in addition to ECE | Paper examples are image classifiers, so metric critique transfers more than performance results |
| Selective classifiers must be judged by risk versus coverage ([Geifman & El-Yaniv, 2017](https://papers.neurips.cc/paper_files/paper/2017/hash/4a8423d5e91fda00bb7e46540e2b0cf1-Abstract.html)) | Evaluate alert precision/error across the entire coverage range and fixed alert budgets | Formal risk guarantees do not imply profit |
| PR curves are more informative under heavy class imbalance ([Saito & Rehmsmeier, 2015](https://doi.org/10.1371/journal.pone.0118432)) | Add PR-AUC and class-specific precision/recall for rare edge events | PR-AUC depends on prevalence and is not comparable across changed populations without context |
| MCC is a balanced binary summary when class sizes differ ([Chicco & Jurman, 2020](https://doi.org/10.1186/s12864-019-6413-7)) | Use MCC as a secondary discriminator metric | It still ignores costs, time dependence, and abstention unless adapted carefully |
| Ordinary cross-validation for time series is safe only under restrictive error conditions ([Bergmeir, Hyndman & Koo, 2018](https://robjhyndman.com/publications/cv-time-series/)) | Retain chronological walk-forward, label purging, and embargo | Even rolling tests do not remove common-shock dependence |
| Comparing many strategies creates data-snooping bias ([White, 2000](https://doi.org/10.1111/1468-0262.00152); [Hansen, 2005](https://doi.org/10.1198/073500105000000063)) | Maintain the full candidate ledger and use registered reality-check/SPA procedures where applicable | Neither procedure repairs an incomplete candidate universe or reused holdout |
| Backtest overfitting probability rises with selection among variants ([Bailey et al., 2016](https://escholarship.org/uc/item/4w1110bb)) | Use PBO as a research-overfit diagnostic and limit the model ladder | PBO estimates do not substitute for fresh prospective proof |
| Multiple testing materially raises required significance for financial factors ([Harvey, Liu & Zhu, 2016](https://www.nber.org/papers/w20592)) | Treat thresholds, horizons, assets, and model variants as one research family | Asset-pricing factor tests differ from intraday event forecasts |
| Apparent technical-rule performance can vanish after data-snooping correction ([Sullivan, Timmermann & White, 1999](https://doi.org/10.1111/0022-1082.00163)) | Preserve failed rules and prior attempts in the multiplicity ledger | Historical equity rules and crypto microstructure differ |
| Forecast losses can be compared with serial-dependence-aware tests ([Diebold & Mariano, 1995](https://fedinprint.org/item/fedmem/38937)) | Use paired outer-fold loss differentials for a small preregistered model comparison | Many models/endpoints still require multiplicity control |
| Shallow trees and neural networks can capture nonlinear interactions in asset returns ([Gu, Kelly & Xiu, 2020](https://doi.org/10.1093/rfs/hhaa009)) | A shallow regularized tree is a justified model-ladder step after logistic regression | Their monthly US equity setting does not validate five-minute crypto prediction |
| Gradient boosting offers regularized nonlinear interactions ([Chen & Guestrin, 2016](https://doi.org/10.1145/2939672.2939785)) | Test a tightly constrained shallow booster with fixed hyperparameter budget | Flexibility sharply increases research degrees of freedom |
| Order-flow imbalance relates to short-interval price changes and depth ([Cont, Kukanov & Stoikov, 2014](https://doi.org/10.1093/jjfinec/nbt003)) | Exact OFI, normalized by available depth, is a plausible Family B feature | Evidence is from equities and short intervals, not 60-minute crypto edge |
| DeepLOB learns spatial/temporal LOB structure ([Zhang, Zohren & Roberts, 2019](https://arxiv.org/abs/1808.03668)) | Deep sequence models are a later candidate after exact book reconstruction and large data volume | Reported labels/horizons and equity venues differ; leakage controls must be rebuilt |
| Deep order-flow representations can outperform raw-book representations at short horizons ([Kolm, Turiel & Westray, 2023](https://doi.org/10.1111/mafi.12413)) | Prioritize order-flow state before high-capacity raw-depth models | Forecast horizon is approximately a few price changes, not this system's after-cost alert horizon |
| Cross-instrument LOB regularities can be learned in large pooled samples ([Sirignano & Cont, 2019](https://arxiv.org/abs/1803.06917)) | Pooling across assets may help when identity leakage and liquidity differences are controlled | The study uses hundreds of US equities and next-price-move targets |
| Microprice refines the mid using imbalance for very short-term prediction ([Stoikov, 2018](https://doi.org/10.1080/14697688.2018.1489139)) | Add microprice displacement as an execution/very-short-horizon feature candidate | It should not be assumed to predict 12-bar returns |
| Adaptive conformal methods address changing sequences ([Gibbs & Candès, 2021](https://papers.neurips.cc/paper_files/paper/2021/hash/0d441de75945e5acbc865406fc9a2559-Abstract.html)) | A later research track may quantify time-varying coverage after a base model is valid | Coverage guarantees are not profitability guarantees and dependence assumptions require care |
| Concept drift requires explicit detection and adaptation policy ([Gama et al., 2014](https://doi.org/10.1145/2523813)) | Monitor feature, prevalence, calibration, and cost drift | Automatic adaptation would violate this project's frozen-version governance unless separately controlled |

### 9.1 Exchange-data implications

Official Binance documentation creates a practical data-retention constraint.
The USD-M market-data catalog documents limited recent-history windows for
several derivatives endpoints, including one month for open-interest history,
30 days for basis history, and 48 hours for USD-M aggregate-trade REST queries.
These sources should be archived continuously under the project's
existing provenance model rather than fetched only when an experiment begins
([Binance USD-M market data documentation](https://developers.binance.com/en/docs/catalog/core-trading-derivatives-trading-usd-s-m-futures/api/rest-api/market-data)).

For Spot, Binance documents real-time `bookTicker` updates and sequence-based
diff-depth processing. Exact local-book reconstruction must follow update-ID
rules and resnapshot on gaps; a recent-looking but sequence-broken book is not
valid evidence
([Binance Spot WebSocket streams](https://developers.binance.com/en/docs/catalog/core-trading-spot-trading/api/ws-streams/~)).

These facts favor finishing public-data archival and M2 authority before trying
large model classes. A deep model cannot recover information that was never
captured or whose sequence validity is unknown.

## 10. Recommended target architecture

The future system should preserve the repository's strongest separation of
concerns:

```text
public Binance sources + event/receipt clocks
                    |
          immutable causal feature blocks
                    |
    frozen opportunity generators (R2, A, B, C)
                    |
     direction/magnitude model on common folds
                    |
        outer-fold probability calibration
                    |
    selective abstention at a fixed alert budget
                    |
  separate actionability gate (BBO/depth/cost)
                    |
       evidence-first Discord alert only
                    |
  prospective outcome + non-overlapping audit ledger
```

The actionability gate should never feed back into claims that the directional
model itself improved. Conversely, a good directional model that cannot clear
the user's actual cost and size should abstain rather than be called profitable.
The intrabar anomaly detector should remain a parallel surveillance-warning
channel outside this forecast/calibration pipeline. Statistical downside labels
remain valid for Spot evaluation, but their action semantics are `SPOT_EXIT`,
never a synthetic Spot short.

### 10.1 Opportunity populations

Maintain two immutable populations:

1. **All-bar diagnostic population.** Every eligible closed five-minute bar in a
   fixed point-in-time universe. This measures general up/down discrimination,
   prevalence, and missed opportunities.
2. **Opportunity-conditioned population.** Every frozen R2/A/B/C raw opportunity,
   including those later rejected by context, calibration, BBO, cooldown, or
   alert budget. This measures whether selection adds value.

Both should include explicit `FEATURE_NOT_READY`, `INCONCLUSIVE_DATA`,
`UNEVALUABLE_OUTCOME`, and `ABSTAIN` states. Complete-case deletion must be
reported as a selection step.

### 10.2 Target set

Retain immutable raw paths at `1, 3, 6, 12, 72` bars. Use:

- primary common target: 12-bar raw direction with a preregistered neutral band;
- Family B mechanism target: 3-bar direction/path;
- Family C mechanism target: 6-bar catch-up and residual closure;
- Family A/R2 target: 12-bar direction plus raw MFE/MAE path;
- separate after-cost edge label under the exact fee/slippage/funding contract;
- separate time-to-touch/recovery or barrier outcome for retest research; and
- a non-overlapping position ledger only for portfolio-style summaries.

Do not replace raw outcomes when target/stop parameters change. Derived labels
must point back to the immutable raw path, consistent with
`docs/RESEARCH_GOVERNANCE.md:101-104`.

## 11. Data and feature program

### 11.1 Priority 0: authority before modeling

- Complete M0/M1/M2 binding for every promoting feature.
- Archive closed klines, aggregate trades, BBO, sequence-valid depth, mark/index,
  predicted/settled funding, and public open interest continuously.
- Store exchange event time, transaction time where supplied, local receipt time,
  decision cutoff, and completeness/finality evidence separately.
- Freeze point-in-time universe membership, symbol status, contract multiplier,
  tick/lot rules, and delisting reason.
- Maintain exact fee-version and funding accounting; never impute an execution
  result from candle range.
- Split field presence, interval continuity, upstream completeness, parser
  health, and receipt latency into distinct fields.

### 11.2 Economically separated feature blocks

| Block | Candidate features | Controls |
|---|---|---|
| Price state | multi-horizon returns, distance from prior-only levels, EMA slope, ER, volatility-scaled trend | one capped block; prior-only reference windows |
| Trade flow | signed taker volume, OFI, trade intensity, buy/sell persistence | exact aggregate-trade completeness; normalize by activity/depth |
| LOB state | microprice displacement, queue imbalance, opposing-depth slope, replenishment/depletion | sequence-valid book; latency and resnapshot flags |
| Cross-section | target-excluded median/factor return, beta residual, breadth, dispersion, lag rank | frozen point-in-time universe; leave-target-out calculation |
| Derivatives context | OI change, basis, predicted/settled funding, liquidation proxies if public and authoritative | context by default; direction only if mechanism specifies sign |
| Regime | market volatility, breadth, time-of-day/day-of-week, liquidity tier | diagnostic/interaction block; no post-hoc regime cherry-picking |
| Actionability | spread, top-of-book capacity, impact proxy, fee/funding cost, receipt age | separate from directional score and calibrated probability |

High-dimensional indicators should not be added merely because they are
available. Each block needs a mechanism, causal clock, missingness policy, and a
leave-one-block-out test.

### 11.3 Specific code-level recommendations

- `src/signalbot/indicators/core.py`: version `data_completeness` into explicit
  presence and continuity/source-authority fields; add a prior-only
  Bollinger-percentile feature rather than changing the existing value.
- `src/signalbot/r4b_v2/strategy/family_c.py`: add a separately versioned
  target-excluded factor implementation and frozen paired ablation.
- `src/signalbot/r4b_v2/strategy/directional_evidence.py`: preserve the
  directional/context split; do not restore context signs merely to increase the
  displayed score.
- R4B producer/adapters: complete real participation and cross-sectional
  bindings, primary-decision binding, and M2 source-census/finality proof before
  any promotion test.
- Historical datasets: add point-in-time universe snapshots and exact alert-state
  lineage so raw opportunity, gate pass, cooldown suppression, and alert emission
  can be compared without denominator drift.

## 12. Parsimonious model ladder

Every model must receive the same causal rows, targets, folds, costs, and
selection budget. Advancing complexity without this equality makes model
comparison uninterpretable.

### Level 0 — no-skill and rule baselines

- training-fold class prevalence by market/side/horizon;
- regime-conditioned prevalence fit only on prior training data;
- previous-return sign persistence;
- simple one-bar reversal;
- stratified random selection preserving fold/cohort/asset and alert count;
- raw R2 C0; and
- frozen legacy/shadow rules without retuning.

### Level 1 — regularized linear models

- elastic-net multinomial logistic model for `UP/NEUTRAL/DOWN`; or
- two one-versus-rest heads for long and short edge, with abstention when neither
  passes.

Use standardized continuous blocks, missingness indicators, limited predeclared
interactions, and partial pooling/group intercepts only when supported by the
training population. This level is the primary interpretable benchmark.

### Level 2 — constrained shallow boosting

Use shallow trees, strong row/column regularization, a small fixed hyperparameter
grid, minimum leaf sizes, and optional monotonic constraints only where an
economic direction is defensible. The full grid counts toward multiplicity.

### Level 3 — rule plus meta-filter

Retain R2/A/B/C as opportunity generators. Train a common calibrated meta-filter
to abstain, not to rewrite rule identity. Compare its incremental value with the
same raw opportunities and alert budget.

### Level 4 — deep sequence models, conditional only

DeepLOB-style CNN/LSTM/transformer work is blocked until:

- sequence-valid L2 data and exact labels cover a sufficiently large fresh
  period;
- Levels 0–3 have authoritative benchmarks;
- symbol/venue identity leakage tests pass;
- model selection budget and compute are preregistered; and
- calibration and inference remain outer-fold and prospective.

This ordering reflects evidence, not a presumption that simpler models will win.
It prevents model capacity from becoming a substitute for data authority.

## 13. Validation and inference design

### 13.1 Nested chronological walk-forward

For each outer test month or preregistered block:

1. fit feature transforms and the model only on earlier training data;
2. purge at least the maximum target/path horizon from boundaries;
3. reserve a strictly later, non-overlapping calibration slice;
4. select calibration method and alert threshold without outer outcomes;
5. evaluate once on the outer block;
6. append the locked result to an immutable ledger; and
7. never reuse the outer block to alter the same candidate.

Use expanding and rolling-window sensitivity as separately registered variants,
not whichever looks better afterward. Share identical outer folds and bootstrap
draws across models to improve paired comparisons.

### 13.2 Calibration

Compare, within the calibration slice only:

- logistic/Platt scaling;
- temperature scaling when appropriate; and
- isotonic regression only when the calibration sample is large enough.

Report on outer rows:

- Brier score and skill versus fold prevalence;
- log loss and skill;
- calibration intercept and slope;
- adaptive/equal-count reliability diagrams with counts;
- direction-, market-, asset-tier-, and horizon-specific diagnostics; and
- the effect of calibration on the risk-coverage curve.

A low aggregate ECE is insufficient. No calibration method can repair a model
with no discrimination.

### 13.3 Discrimination and selective alerts

Minimum reporting set:

- class-specific precision and recall;
- balanced accuracy and MCC where binary summaries are appropriate;
- ROC-AUC plus PR-AUC with prevalence stated;
- coverage, abstention rate, alert count, alerts/day, and zero-alert days;
- risk-coverage curve and area under that curve;
- results at a frozen alert budget as well as a frozen probability threshold;
- confusion matrices for `UP/NEUTRAL/DOWN`; and
- missing/unevaluable outcome counts outside the denominator.

### 13.4 Economic evaluation

Keep these columns separate:

- gross directional return;
- spread and adverse slippage;
- fees;
- funding;
- net return;
- 0x/base/2x cost stress;
- profit factor and average win/loss ratio;
- MFE, MAE, and time to extremes;
- asset/day/regime concentration; and
- non-overlapping ledger return, drawdown, and exposure.

Alert correctness is not portfolio performance. The project should continue to
state this explicitly.

### 13.5 Dependence-aware inference

- Use circular or moving calendar blocks that retain no-alert days.
- Predeclare block length and include 7/14/28-day sensitivity where justified.
- Cluster descriptive summaries by decision time/common market shock and symbol.
- Compare models using paired outer-fold loss/return differences.
- Use an explicit null-centred resampling test or confidence-bound decision; do
  not call an uncentred bootstrap tail a p-value.
- Apply Holm or a stronger registered procedure over the complete candidate and
  primary-endpoint family.
- Track effective rather than raw sample size and enforce diversity floors.

### 13.6 Generalization and drift

- leave-one-asset-out and unseen-asset diagnostics;
- fixed liquidity/cohort strata;
- stablecoin/venue and contract-rule changes as explicit breaks;
- rolling feature population-stability diagnostics;
- outcome prevalence and calibration drift;
- observed-versus-assumed cost error; and
- a fail-closed response: pause performance language, investigate, and create a
  new version. Do not auto-retune the live candidate.

## 14. Phased research roadmap

### E0 — Seal the question and evidence authority

**Deliverables**

- one estimand document per family;
- immutable candidate/multiplicity ledger;
- source/cursor/receipt/M2 completeness matrix;
- point-in-time universe contract;
- explicit all-bar and opportunity-conditioned populations; and
- no access to active Phase R outcomes.

**Stop rule:** no modeling if target, source clock, missingness, or execution
reference is ambiguous.

### E1 — Reproduce the baseline ladder

**Deliverables**

- current-contract reruns of no-skill, C0, legacy, shadow, indicator, NB, and
  regularized-logistic baselines on identical folds;
- separate gross and cost panels;
- exact provenance and deterministic rerun hashes; and
- correction or retirement of unreproduced prior-probe numbers.

**Stop rule:** if no model improves outer-fold proper loss or raw directional
metrics over climatology/C0, do not tune an economic selection threshold.

### E2 — Causal feature ablations

**Deliverables**

- C0 → HTF → ER/context → retest staged ablation;
- price → +flow → +target-excluded cross-section → +derivatives-context
  ablation;
- current versus target-excluded Family C;
- Family B exact-depth readiness and latency sensitivity; and
- coverage and missingness shifts for every addition.

**Stop rule:** reject a feature block that lacks stable incremental outer-fold
loss improvement or merely removes most observations.

### E3 — Constrained nonlinear and calibrated selective model

**Deliverables**

- fixed shallow-boosting grid;
- outer-fold calibration comparison;
- locked alert-budget risk-coverage analysis;
- paired model comparison and full multiplicity adjustment; and
- asset/regime/concentration stress panels.

**Stop rule:** do not advance a model whose calibration, coverage, or cost-stress
gate fails, even if headline accuracy rises.

### E4 — Prospective shadow

**Deliverables**

- new code/config/model hash frozen before T0;
- no production eligibility or order placement;
- exact BBO/depth/fees/funding and alert-state capture;
- minimum sample, duration, asset, side, and regime floors; and
- a preregistered analysis unlock.

**Stop rule:** any threshold, target, feature, model, universe, or exclusion
change ends confirmatory status and requires a new T0.

### E5 — Promotion decision

Promotion can be considered only if the untouched prospective sample passes
every intersection gate below. Failure is recorded without a rescue analysis.
Exploratory follow-up must receive a new version and new prospective sample.

## 15. Proposed acceptance gates

Exact numeric thresholds should be determined by an outcome-blind power and
operational-capacity analysis before T0. The logical gates should be fixed now:

| Gate | Required evidence |
|---|---|
| Data authority | Complete required-source census, causal clocks, sequence-valid depth where used, no unresolved integrity gap |
| Population | Frozen universe and denominators; missingness/abstention fully accounted for |
| Sample diversity | Predeclared minimum events, calendar blocks, assets, sides, and regimes; concentration caps |
| Discrimination | Improvement over registered climatology and rule baselines on paired outer/prospective data |
| Calibration | Better proper-score performance, credible slope/intercept, and stable reliability at selected coverage |
| Selectivity | Precision/error improvement at nontrivial frozen coverage and alert budget |
| Economics | Positive lower confidence bound for net mean under base and stress cost; PF and payoff requirements |
| Dependence | Calendar-block inference, non-overlapping ledger confirmation, sensitivity to block length |
| Multiplicity | Adjustment covers every registered model, family, side, horizon, threshold, and primary endpoint |
| Stability | No single asset/period drives the result; unseen-asset and drift diagnostics acceptable |
| Prospective integrity | Candidate and analysis frozen before T0; outcome not used for redesign |
| Mission | Alert-only, public data, no production order execution |

The pre-existing development-only `DEV-C1` audit used floors such as 300 events,
50 in each of three periods, five assets, 35% concentration caps, base/stress
cost gates, and Holm correction
(`docs/r4b-v2-cost-survival-candidate-audit-2026-07-21.md:114-127`). Those values
remain specific to that frozen candidate. A new model should use a new
outcome-blind power analysis rather than borrowing thresholds opportunistically.

## 16. Priority recommendations by expected information value

### Immediate

1. Preserve Phase R blindness and all frozen algorithm identities.
2. Publish this report's evidence-authority distinctions beside future results.
3. Finish exact public-data archival, M2 binding, and execution parity.
4. Define the all-bar and opportunity-conditioned populations plus separate
   direction and after-cost estimands.
5. Repair inference: no uncentred pseudo-p-values; full candidate-ledger
   multiplicity; shared paired folds and blocks.

### Next research cycle

6. Reproduce the common baseline ladder, beginning with climatology and
   regularized logistic regression.
7. Run target-excluded Family C and staged feature/gate ablations.
8. Add cross-fitted calibration and risk-coverage analysis.
9. Evaluate one constrained shallow booster only after the linear benchmark is
   authoritative.
10. Start a new prospective shadow interval for any changed candidate.

### Deferred

11. Deep LOB or transformer models.
12. Adaptive/conformal methods.
13. Online retraining or automatic regime adaptation.

The deferred items are scientifically interesting but presently have lower
information value than resolving data authority, target definition, baseline
performance, and prospective integrity.

## 17. Strengths that must not be lost during improvement

- strictly prior higher-timeframe context;
- current-bar exclusion from breakout boundaries;
- explicit event, transaction, receipt, and decision clocks;
- fresh non-proxy BBO and capacity checks;
- fail-closed missing data and conflict-loud replay;
- immutable rule/model/protocol versions and deterministic identities;
- informational-only state locks;
- separation of directional, contextual, execution, and cost evidence;
- raw multi-horizon path preservation;
- explicit `score_is_probability=false` contracts;
- exposure of negative results and development exhaustion; and
- the prohibition on outcome-driven changes before prospective unlock.

Replacing these controls with a more complex model would be a regression even
if a backtest headline improved.

## 18. Validation context for this review

This report did not rerun the active prospective campaign or historical outcome
pipelines. Clean isolated Python 3.12.13 runs executed the focused unit tests
for the anomaly detector, signal gates, shadow policy, retest lifecycle, R4a,
Indicator/V1A, the current three-family walk-forward implementation, R4B
Families A/B/C, Evidence Score, and the directional successor: **399 passed in
56.53 seconds**, plus **5 anomaly tests passed in 1.19 seconds**. These tests
verify implemented contracts, not predictive efficacy.

For broader repository context, a fresh clean Python 3.12.13 environment audit
performed on 2026-09-02 immediately before this report recorded:

- environment synchronization: pass;
- Pyright: pass;
- `compileall`: pass;
- full pytest: `1 failed, 4,166 passed, 21 skipped`;
- the failure was the operational-health FAST latency check, measured at about
  38.99 seconds versus a 30-second budget;
- full Ruff: 79 findings; and
- Ruff restricted to `src` and `tests`: one import-order finding.

These software-gate results are not evidence for or against directional alpha.
They do mean the current working tree must not be described as having a clean
release gate. See
`docs/PROJECT_LIMITATIONS_AND_IMPROVEMENT_REPORT_2026-09-02.md` for the broader
project audit.

## 19. Final referee statement

The repository has built much of the machinery needed to conduct credible
algorithmic market research, but machinery is not predictive evidence. The
intrabar anomaly path recognizes realized rapid moves but does not predict their
continuation or reversal. The live R2 path is a narrow causal screen, not a
general up/down classifier. Its
confirmed rule score does not rank candidates. The shadow and retest paths are
properly non-promoting but unvalidated. R4a, the Indicator Discriminator, and
the historical three-family work do not establish a cost-surviving edge. R4B
V2's mechanisms remain hypotheses pending complete data authority and untouched
inference.

The best route to more precise alerts is therefore not broader indicator search
or immediate deep learning. It is to define the estimand, capture authoritative
microstructure evidence, compare a small model ladder on identical causal folds,
calibrate on unseen data, evaluate precision together with coverage and cost,
control the complete research search, and confirm exactly once on fresh
prospective observations.

**Final verdict: MAJOR REVISION — REJECT AS A CURRENT PERFORMANCE CLAIM.**

The engineering platform is worth retaining. Any future accuracy, probability,
or profitability claim remains contingent on the locked prospective program
described above.

## 20. Primary references

1. Gneiting, T., & Raftery, A. E. (2007). [Strictly Proper Scoring Rules, Prediction, and Estimation](https://doi.org/10.1198/016214506000001437).
2. Niculescu-Mizil, A., & Caruana, R. (2005). [Predicting Good Probabilities with Supervised Learning](https://doi.org/10.1145/1102351.1102430).
3. Geifman, Y., & El-Yaniv, R. (2017). [Selective Classification for Deep Neural Networks](https://papers.neurips.cc/paper_files/paper/2017/hash/4a8423d5e91fda00bb7e46540e2b0cf1-Abstract.html).
4. Gibbs, I., & Candès, E. (2021). [Adaptive Conformal Inference Under Distribution Shift](https://papers.neurips.cc/paper_files/paper/2021/hash/0d441de75945e5acbc865406fc9a2559-Abstract.html).
5. White, H. (2000). [A Reality Check for Data Snooping](https://doi.org/10.1111/1468-0262.00152).
6. Hansen, P. R. (2005). [A Test for Superior Predictive Ability](https://doi.org/10.1198/073500105000000063).
7. Bailey, D. H., Borwein, J. M., López de Prado, M., & Zhu, Q. J. (2016). [The Probability of Backtest Overfitting](https://escholarship.org/uc/item/4w1110bb).
8. Harvey, C. R., Liu, Y., & Zhu, H. (2016). [\"... and the Cross-Section of Expected Returns\"](https://www.nber.org/papers/w20592).
9. Sullivan, R., Timmermann, A., & White, H. (1999). [Data-Snooping, Technical Trading Rule Performance, and the Bootstrap](https://doi.org/10.1111/0022-1082.00163).
10. Diebold, F. X., & Mariano, R. S. (1995). [Comparing Predictive Accuracy](https://fedinprint.org/item/fedmem/38937).
11. Bergmeir, C., Hyndman, R. J., & Koo, B. (2018). [A Note on the Validity of Cross-Validation for Evaluating Autoregressive Time Series Prediction](https://robjhyndman.com/publications/cv-time-series/).
12. Gu, S., Kelly, B., & Xiu, D. (2020). [Empirical Asset Pricing via Machine Learning](https://doi.org/10.1093/rfs/hhaa009).
13. Chen, T., & Guestrin, C. (2016). [XGBoost: A Scalable Tree Boosting System](https://doi.org/10.1145/2939672.2939785).
14. Cont, R., Kukanov, A., & Stoikov, S. (2014). [The Price Impact of Order Book Events](https://doi.org/10.1093/jjfinec/nbt003).
15. Zhang, Z., Zohren, S., & Roberts, S. (2019). [DeepLOB: Deep Convolutional Neural Networks for Limit Order Books](https://arxiv.org/abs/1808.03668).
16. Kolm, P. N., Turiel, J., & Westray, N. (2023). [Deep Order Flow Imbalance](https://doi.org/10.1111/mafi.12413).
17. Sirignano, J., & Cont, R. (2019). [Universal Features of Price Formation in Financial Markets](https://arxiv.org/abs/1803.06917).
18. Stoikov, S. (2018). [The Micro-Price: A High-Frequency Estimator of Future Prices](https://doi.org/10.1080/14697688.2018.1489139).
19. Saito, T., & Rehmsmeier, M. (2015). [The Precision-Recall Plot Is More Informative than the ROC Plot When Evaluating Binary Classifiers on Imbalanced Datasets](https://doi.org/10.1371/journal.pone.0118432).
20. Chicco, D., & Jurman, G. (2020). [The Advantages of the Matthews Correlation Coefficient over F1 Score and Accuracy in Binary Classification Evaluation](https://doi.org/10.1186/s12864-019-6413-7).
21. Nixon, J. et al. (2019). [Measuring Calibration in Deep Learning](https://openaccess.thecvf.com/content_CVPRW_2019/html/Uncertainty_and_Robustness_in_Deep_Visual_Learning/Nixon_Measuring_Calibration_in_Deep_Learning_CVPRW_2019_paper.html).
22. Gama, J. et al. (2014). [A Survey on Concept Drift Adaptation](https://doi.org/10.1145/2523813).
23. Bailey, D. H., & López de Prado, M. (2014). [The Deflated Sharpe Ratio: Correcting for Selection Bias, Backtest Overfitting, and Non-Normality](https://doi.org/10.2139/ssrn.2460551).
24. Binance. [USD-M Futures Market Data Documentation](https://developers.binance.com/en/docs/catalog/core-trading-derivatives-trading-usd-s-m-futures/api/rest-api/market-data).
25. Binance. [Spot WebSocket Streams](https://developers.binance.com/en/docs/catalog/core-trading-spot-trading/api/ws-streams/~).
