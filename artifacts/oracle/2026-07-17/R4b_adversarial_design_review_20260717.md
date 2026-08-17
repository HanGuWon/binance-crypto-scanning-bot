# R4b Independent Adversarial Design Review

**Protocol:** `R4B_CAUSAL_V1`  
**Review freeze:** 2026-07-17 11:52:53 KST  
**Specification SHA-256:** `9c925f5988e65a1371e8859dd00ea6a61db0c3b9ea34622432e9a28a3bab297b`  
**Mandate:** research-only alert discovery; no private endpoints, signed requests, live orders, leverage above 1×, profit promise, or post-result rescue.

## Executive decision

No reproducible positive-expectancy R4b strategy is established by the project evidence I could access. The correct current state is **`INCONCLUSIVE_DATA / UNTESTED`**, not “promising,” because the two strongest remaining causal mechanisms require historical or prospective trades, book-ticker/depth, open-interest, mark/index, and timestamped funding-estimate data that the accessible kline-centric project artifacts do not contain.

The user-stipulated R4a result is decisive for its own ledger: a closed-5m C0 gate over Spot breakout-long and Futures breakdown-short produced 52,107 out-of-fold predictions, selected none, and had a negative maximum predicted net return. That result was not independently reproducible here because the raw R4a ledger and model artifacts were not accessible. Taking it as stipulated, however, **no threshold change can turn that sealed prediction vector into a predicted-positive subset**. R4a therefore closes the C0 retuning, indicator-stacking, and meta-label-rescue branch. It does not prove that every possible crypto mechanism has zero expectancy.

R4b should test only three fresh, orthogonal mechanism families:

1. **Derivatives crowding → forced-deleveraging transition** — strongest causal candidate; requires OI, basis, timestamped next-funding estimate, aggressive trade flow, BBO/depth, and realized funding.
2. **Order-flow/liquidity dislocation** — depletion continuation versus absorption reversal; requires trades and sequence-consistent local-book data, not klines.
3. **Cross-sectional common-shock lag/catch-up** — the only family that can be screened with point-in-time klines, but it still requires BBO/depth to support an executable-net claim.

A family is rejected before machine learning when its fixed-rule gross event-study lower confidence bound is not positive. A family is rejected at execution replay when the edge disappears at next executable quotes or 1.5× baseline costs. A sealed future holdout is evaluated once; failure is not followed by threshold adjustment.

---

## 1. Evidence boundary and exact source-access ledger

### 1.1 Access semantics

I did not have a mounted repository tree or unrestricted conversation export. I had semantic File Library access to individual uploads/snippets and summary-level personal conversation context. Duplicate uploads were collapsed below; duplicate copies do not add independent evidence. Irrelevant File Library results were excluded. This is an exact ledger of the **project-relevant logical sources actually accessed for this review**, not a claim that it is the project’s complete filesystem inventory.

### 1.2 Directly accessible project evidence

| # | Logical source accessed | Locator used | What it can establish | Evidentiary use |
|---:|---|---|---|---|
| 1 | `R2_independent_audit_ko.md` | §§1–3, 8–9, 11–15 | R1/R2 evidence boundary; C0 exact rule; frozen HTF/BBO candidate; quote-to-quote outcome; prospective design; engineering blockers | Core project evidence |
| 2 | `experiment_plan.md` — duplicate uploads dated 2026-07-16 | §§1–3, 9–10, bias checklist, Material Passport | Exposed retrospective status; fixed 8-asset survivor panel; closed-5m timing; C0 corrected rule; no post-result change | Core project evidence; retrospective only |
| 3 | `proposed_r2_preregistered_spec.yaml` | top-level engineering, data, prospective, inference, and safety sections | Machine-readable R2 proposal and prospective controls | Design evidence, not efficacy evidence |
| 4 | `current_research_manifest_20260707.md` | `Scope`, `Current Baseline Results`, `Current Evaluation Files`, `Current Verdict`, `Next Gate` | Current Freqtrade results, small-sample status, failed variants, missing forward signals | Core project evidence |
| 5 | `final_pro_synthesis_20260712.md` | §§6.3–6.5 | Fixed observation clocks, synchronized daily P&L, block bootstrap/Holm framework, economic and tail gates | Prior design recommendation, not an edge result |
| 6 | `attachments-bundle.txt`, upload dated 2026-07-01 | `Pro Extended Follow-Up`, `Anti-Chase Probe`, `High-Score Probe`, rolling/split conclusions | Anti-chase failure; high-score instability; current no-go decisions | Core exposed-result evidence |
| 7 | `attachments-bundle.txt`, upload dated 2026-07-05 | embedded `oracle_crypto_strategy_review_prompt.md` and public-data/backtest context | Earlier market-pressure architecture, OI/funding/book placeholders, broad 1h result context | Context only; prompt text is not proof |
| 8 | `attachments-bundle.txt`, upload dated 2026-07-07 | embedded validation requirement table and audit summaries | Scope, dry-run/no-order controls, forward-journal readiness | Engineering/context evidence |
| 9 | `attachments-bundle.txt`, upload dated 2026-07-13 | embedded book-breakout-retest audit, metrics, capability/data-gap excerpts | Exact retest/reclaim intervention; 37/138 selection; negative 2024 split; concentration and top-winner failure | Core exposed-result evidence |
| 10 | `Investing_with_Volume_Analysis_Detailed_Report_EN.pdf` — duplicate 77-page copies | PDF pp.2, 39, 50–56, 76–77 | Reconstructed source caveat; event/available-time model; paper-broker limits; validation ladder; block bootstrap; no performance claim | Methodology only |
| 11 | `Investing_with_Volume_Analysis_Detailed_Report_EN.docx` | pp.57–62, 81–84 | Duplicate/expanded rendering of validation, promotion, and toolkit limitations | Methodology only; not independent of #10 |
| 12 | `How charts can help you in the stock market` — duplicate PDFs | PDF pp.30, 38, 79 | Historical trendline/channel/double-top illustrations | Practitioner examples, not efficacy evidence |
| 13 | `The New Sell and Sell Short, Second Edition` — duplicate PDFs | PDF p.216, “Shorting Tops” | Practitioner warning about volatile tops, wider stops, re-entry | Operational hypothesis only |
| 14 | `Wilder W. - New Concepts in Technical Trading Systems` | file-level semantic access; exact page locator was not reliably extracted | RSI/ATR/DMI formula heritage | Partial access; no empirical edge claim used |
| 15 | `README_MVP.md` | current candidate/verification sections | Observation-only status and cost/threshold/sample sensitivity | Navigation/context |
| 16 | `replay.py` | semantic snippet access | Earlier next-open replay assumptions and optional/missing BBO/funding treatment | Implementation context; not independently executed |
| 17 | `trend_timing_v1_preregistration_20260710.md` | semantic snippet access | Earlier frozen timing experiment structure | Design context |
| 18 | `cross_project_failure_evidence_20260711.md` | semantic snippet access | Adaptive-search and multiplicity warnings across prior projects | Exposed diagnostic evidence |

### 1.3 Accessible prompt/navigation artifacts, excluded from efficacy evidence

The following were accessible but used only to understand requested scope or source provenance: `README.md`, `MANIFEST.json`, `09_Five_Source_Cross_Book_Synthesis_Prompt_EN.md`, `NEW_PROMPTS_ONLY_EN.md`, `06_Elder...md`, `08_Elder...md`, `oracle_goal_prompt_request_20260707.md`, `ALL_PROMPTS_EN.md`, `gpt_pro_review_context.md`, `gpt_prediction_improvement_request.md`, and generated Wilder reconstruction Markdown files. They contain instructions, summaries, or prompts rather than independent return evidence.

### 1.4 Conversation access

Only summary-level context was accessible, not line-addressable transcripts:

- 2026-07-01: short-horizon ≥5% move research; deterministic target rejected, volatility/regime/conditional direction proposed.
- 2026-07-02, **“Research Summary for Crypto-Trading Project”**: no-go on durable +5% net-per-trade claim; research engine, event-level net EV, realistic funding/liquidity/slippage, train-only allowlists, and ML only as a meta-filter.
- 2026-07-13, **“Oracle Review Request: Freqtrade 4h Cross-Asset Trend Research”**: no rule changes; shadow telemetry; negative recent splits and interaction risk.
- 2026-07-16, **“Crypto Market Briefing & Bot Analysis”**: monitoring context, not strategy-validation evidence.

These summaries were treated as navigation/context, not confirmatory evidence.

### 1.5 Inaccessible or not independently inspectable

| Source or evidence | Status | Consequence |
|---|---|---|
| R4a raw opportunity/prediction ledger, labels, fold assignments, model, calibration, feature contract, costs, code/config/container hashes | **Inaccessible**; exact searches for `R4a`, `R4b`, `52,107`, and `52107` found no matching artifact | R4a result is user-stipulated, not independently verified; no “R4b beats R4a” claim |
| R4a training history and full attempted-model registry | **Inaccessible** | R4a multiplicity/PBO/DSR cannot be recomputed |
| Named Freqtrade result ZIPs in the manifest | Names visible; raw ZIP contents not independently parsed in this review | Reported figures can be cited as project claims, not recomputed results |
| R1 raw inputs, G2/G4 settings, run manifest, opportunities/trades/results, comparison JSON, feature contract, lockfile, and full 176-test environment | **Inaccessible**, as the R2 audit itself records | R1 “verified/reproducible” status cannot be certified |
| Historical per-event BBO, depth, local receipt times, sequence continuity, OI, mark/index, complete liquidation, and timestamped funding-estimate archives covering the project test period | **Inaccessible / absent from accessible project data** | Families A/B cannot receive an honest historical net-execution result; Family C can only receive a kline mechanism screen |
| Original uploaded Dormeier book PDF | **Corrupt/non-renderable**, according to the reconstructed report’s PDF p.2 | The volume report is a reconstruction, not page-level access to the original book |
| Full conversation transcripts | **Inaccessible** | No quote- or line-level conversation citation |
| `verification_summary.json`, `reproduce_findings_output.json`, and several audit outputs named in summaries | File names surfaced; full contents were not reliably opened here | No finding is based solely on them |
| Account-specific Binance fee tier, rebates, BNB discount, or actual queue priority | **Not available** | V1 assumes taker fees with no VIP/token discount and forbids maker-fill claims |

---

## 2. What R4a closes—and what it does not

### 2.1 Closed branch

Under the stipulated R4a model, every one of 52,107 out-of-fold predicted net returns was negative. Therefore:

- Any monotone probability/return threshold selects either zero events or a subset the model itself still predicts to be negative.
- Relaxing the threshold, adding RSI, changing calibration, or choosing “the least negative” tail is not discovery of positive expectancy.
- Retuning the same exposed ledger converts the exercise into model-selection on noise.
- A second-stage classifier cannot manufacture return information absent from the event universe; it can only abstain or reorder it.

The R2 audit already froze C0 and prohibited G2/G4 reuse or threshold adjustment (§8), and the current manifest separately shows that a simple 5m Donchian-style breakout implementation overtraded badly: 9,547 trades, −899.181 USDT, PF 0.712 (`current_research_manifest_20260707.md`, `Current Baseline Results`). Those are different experiments, but they point in the same practical direction: do not spend R4b’s error budget on another OHLC indicator gate.

### 2.2 Open branch

R4a does not test state variables outside its ledger. The strongest open questions concern:

- whether **leveraged positioning is being forcibly reduced**, observable through price, OI, basis, estimated funding, and aggressive flow;
- whether **visible liquidity is being depleted or replenished**, observable through trades and a sequence-consistent local book;
- whether a **market-wide common shock propagates asynchronously** across a point-in-time cross-section.

These are not RSI variants. They posit different causal state variables and different falsifiers.

---

## 3. Red-team criticism of prior recommendations

| Prior idea | Adversarial assessment | Disposition |
|---|---|---|
| Donchian breakout/breakdown + EMA20/50 + MACD histogram + ADX | Four transformations of the same OHLC path. They can reduce frequency but are not four independent causal channels. The project’s 5m implementation was strongly cost/noise dominated. | Do not retune or stack further |
| RSI 30/70, RSI reversal, StochRSI | Threshold folklore unless a mechanism, direction, horizon, and cost-aware falsifier are preregistered. RSI is not privileged and mostly re-encodes recent signed returns. | Exclude as primary alpha |
| Bollinger squeeze/BBW, ATR compression, “volatility expansion” alone | Compression identifies a state, not direction. Adding a direction indicator returns to correlated price transforms. At 5m, spread/slippage can dominate the conditional move. | Diagnostic covariate only unless coupled to independent flow/liquidity data |
| G2/G4 volume confirmation, VPCI/relative-volume confirmation | R1/R2 records keep G2/G4 rejected and prohibit retuning. The volume-analysis report itself makes no profitability claim and says its paper broker is not an execution simulator (PDF pp.50–56, 76–77). | Closed; no rebranding |
| HTF 15m/1h confirmation | Another price-path AND gate. It may select zero, delay entries, or concentrate regimes; it is not an orthogonal mechanism. The R2 retrospective plan labels its history exposed and lacks historical BBO for the full candidate. | Negative control/diagnostic only |
| Breakout retest/reclaim and failed-breakout/trap | Direct project tests rejected these. The exact retest admitted 37/138 stress trades, worsened entry timing on matched events, failed 2024/concentration/top-winner gates, and was closed without tuning. | Do not reuse on the same history |
| Anti-chase veto | The project probe had 35 trades, −131.06 USDT, expectancy −0.3057R, PF 0.428, and `NO_GO`; vetoed signals were not demonstrably worse. | Diagnostic-only; closed on exposed data |
| Jiler trendlines/channels/double tops and Elder “shorting tops”/re-entry | Historical chart illustrations and practitioner risk advice, not base-rate-controlled efficacy evidence. Pattern recognition also invites future-pivot and discretionary labeling leakage. | Operational hypotheses only; no promotion |
| Funding extreme alone | Funding is a carry/crowding measure, not a transition trigger. A high rate can persist while price trends. Directional use without OI, basis, and flow confounds crowded continuation with forced unwind. | Require the full Family A transition |
| Book imbalance snapshot alone | A single snapshot is manipulable/noisy and ignores queue evolution. Touching a limit does not prove a fill. | Require sequence-consistent depth, trades, and executable replay |
| Next-open or fixed-slippage fills | Not an executable 5m claim. The accessible volume report explicitly classifies its fixed-slippage paper broker as a software test double (PDF p.50) and recommends one-bar delay/cost stress (pp.54–55). | Prohibited for confirmatory net results |
| ML/meta-labeling as a rescue | ML can rank or abstain only after a fixed family has positive executable net expectancy. A model zoo on the same exposed history increases selection bias. | One penalized model at most, late in the ladder |
| “Read more books / add indicators” | More correlated transformations increase researcher degrees of freedom, not evidence. | Stop |

The project’s own methodological material is directionally correct on one point: event time must be separated from availability/receipt time (volume report PDF p.39), and overlapping trades require time-block inference rather than IID trade tests (pp.54–55). R4b adopts those controls but rejects the report’s example indicator stack as an edge hypothesis.

---

## 4. External mechanism check and data reality

### 4.1 What primary research can and cannot support

Crypto return research documents momentum and common factors mainly at daily or weekly horizons, not at closed-5m execution horizons. Liu and Tsyvinski study crypto-specific momentum/attention; Liu, Tsyvinski, and Wu report weekly factor structure and 1–4-week momentum. These papers motivate controls and priors but do not validate a 5m alert. [Liu & Tsyvinski, NBER working paper](https://www.nber.org/system/files/working_papers/w24877/w24877.pdf); [Liu, Tsyvinski & Wu, NBER working paper](https://www.nber.org/system/files/working_papers/w25882/w25882.pdf).

Makarov and Schoar provide a stronger microstructure motivation: common signed flow is associated with common crypto returns, while idiosyncratic flow helps explain cross-exchange spreads in their sample. That supports measuring flow and common/idiosyncratic components, not a guaranteed strategy. [Primary paper](https://papers.ssrn.com/sol3/papers.cfm?abstract_id=3171204).

Perpetual-futures theory and evidence support funding as an anchoring/carry mechanism for the perp-spot gap, but not a standalone reversal rule. [He, Manela, Ross & von Wachter, “Fundamentals of Perpetual Futures”](https://papers.ssrn.com/sol3/papers.cfm?abstract_id=4301150). Research on perpetual markets also documents liquidation/cascade mechanisms, which motivates Family A’s transition structure. [Albers et al., arXiv 2108.09750](https://arxiv.org/pdf/2108.09750).

Order-flow imbalance has a documented price-impact relationship in limit-order-book research, but the classic Cont–Kukanov–Stoikov evidence is from equities, not Binance 5m crypto. It justifies a measurement design, not transfer of an expected return. [Primary paper](https://arxiv.org/abs/1011.6402).

### 4.2 Binance data implications

The official Binance public archive provides daily/monthly downloadable market files and checksums, especially klines, trades, and aggregate trades. It is not a complete historical archive of per-event BBO, depth, OI, or liquidation streams. [Official repository](https://github.com/binance/binance-public-data).

Current official USDⓈ-M documentation exposes BBO, partial depth, diff-depth sequence fields, mark/index/funding streams, aggregate trades, OI statistics, basis, and liquidation streams. Several REST statistical histories have short retention windows—OI statistics are limited to the latest month, while multiple basis/taker/ratio endpoints are limited to roughly 30 days—so a reproducible multi-year Family A/B test requires prospective capture or an independently audited point-in-time vendor archive. [Official REST market data](https://developers.binance.com/en/docs/catalog/core-trading-derivatives-trading-usd-s-m-futures/api/rest-api/market-data); [official public WebSocket streams](https://developers.binance.com/en/docs/catalog/core-trading-derivatives-trading-usd-s-m-futures/api/ws-streams/public); [official market WebSocket streams](https://developers.binance.com/en/docs/catalog/core-trading-derivatives-trading-usd-s-m-futures/api/ws-streams/market).

The mark-price stream publishes event time, mark price, index price, an estimated funding rate, and next funding time. R4b may use only the last estimate actually received before the decision cutoff; it may use realized funding history only later as a P&L cashflow. The liquidation stream publishes at most the latest liquidation order for a symbol within a 1,000ms window, so it is censored and cannot be treated as complete liquidation volume. Those interface properties are the reason liquidation is diagnostic rather than a gate in Family A.

---

## 5. Data-requirement matrix

`F` = required as a feature; `E` = required for executable P&L; `D` = optional diagnostic; `–` = not required.

| Data class | Family A | Family B | Family C | Klines-only claim possible? |
|---|---:|---:|---:|---|
| Closed 5m OHLCV | F | supporting | F | Family C mechanism screen only |
| Individual/aggregate trades with aggressor proxy | F | F | optional diagnostic | No for A/B |
| Book ticker/BBO with event and receipt time | E | F+E | E | No executable-net claim without it |
| Sequence-consistent depth/local book | E | F+E | E | No |
| Open interest/value | F | – | – | No for A |
| Mark price and index price/basis | F | – | optional | No for A |
| Timestamped next-funding estimate | F | – | – | No for A |
| Realized funding settlements | E for futures | E for futures | E for futures | No full-net claim without it |
| Liquidation/force-order stream | D | – | – | Not used as a gate |
| Point-in-time symbol status/listing history | F | F | F | Required for all |
| Actual fee schedule snapshot | E | E | E | Required for all promotion claims |

**Immediate consequence:** the available kline-centric project data cannot support a historical efficacy verdict for Families A or B. Substituting candle volume, wick shape, next-open, or a fixed spread proxy would change the estimand and must be reported as `INCONCLUSIVE_DATA`, not a negative or positive result.

---

## 6. Frozen global event and execution contract

### 6.1 Point-in-time universe

At 00:00 UTC each day, construct separate Spot-USDT and USDⓈ-M linear-perpetual universes from symbol metadata and only prior complete data:

- status `TRADING`; no current top-N backfill;
- frozen exclusions for stablecoin/fiat bases, leveraged tokens, delisting/suspension, redenomination, and contract migrations;
- at least 30 complete prior days and 8,640 closed 5m bars;
- prior-30d median daily quote volume ≥10 million USDT;
- prior-30d median BBO spread ≤10bp and 99th-percentile spread ≤50bp;
- at least 20 eligible symbols for cross-sectional Family C.

The liquidity thresholds are frozen engineering/capacity floors, not alpha parameters. A new listing cannot be synthesized backward, and a current survivor list cannot define a historical universe.

### 6.2 Timing

For the 5m bar ending at exchange time `T`:

- feature decision cutoff `D = T + 2,000ms`;
- a feature record is admissible only when `exchange_event_time ≤ T` and `local_receipt_time ≤ D`;
- all robust locations/scales, betas, and universe screens exclude the current observation;
- baseline entry occurs at the first valid executable quote at or after `D + 250ms`;
- any exit decision is made from a fully closed bar, then filled at the next executable opposite quote;
- stale quote (>2s), crossed/locked book, depth-sequence gap, or insufficient depth is `NON_EXECUTABLE`, never zero return.

### 6.3 Robust normalization

For any feature `x`, with `W=8,640` prior closed 5m bars:

```text
RZ_t(x) = [x_t − median(x_{t−W:t−1})] /
          [1.4826 × MAD(x_{t−W:t−1}) + 1e−12]
```

No global full-sample scaler is permitted.

---

## 7. Strategy Family A — derivatives crowding to forced deleveraging

### 7.1 Mechanism

A directional move accompanied by rising OI, one-sided basis, and one-sided estimated funding indicates leveraged position accumulation. The trade is not taken while that state persists. The trigger requires a **transition**: price moves against the crowd, OI falls, and aggressive flow flips against the crowded direction. The falsifiable implication is a short-lived continuation of forced position reduction after the first closed transition bar.

### 7.2 Features

For each futures symbol:

```text
r1_t      = ln(C_t / C_t−1)
r12_t     = ln(C_t / C_t−12)
dOI1_t    = ln(OIvalue_t / OIvalue_t−1)
dOI12_t   = ln(OIvalue_t / OIvalue_t−12)
basis_t   = ln(mark_t / index_t)
flow_t    = (aggressive_buy_notional − aggressive_sell_notional) /
            (aggressive_buy_notional + aggressive_sell_notional + ε)
c          = sign(r12_t−1)
```

`fund_est_t` is the last next-funding estimate from the mark-price stream received by the cutoff. Future realized funding is forbidden as a feature.

### 7.3 Precondition and trigger

At `t−1`:

```text
abs(RZ(r12)) ≥ 1.5
RZ(dOI12) ≥ 1.5
c × RZ(basis) ≥ 1.5
c × RZ(fund_est) ≥ 1.0
```

At closed bar `t`:

```text
c × RZ(r1) ≤ −0.5
RZ(dOI1) ≤ −1.0
c × flow_t ≤ −0.35
```

Actions:

- `c=+1` crowded-long unwind: **Futures short**, **Spot exit-risk**.
- `c=−1` crowded-short unwind: **Futures long**, **Spot long**.

### 7.4 Exits

- hard exit: 12 bars / 60 minutes;
- normalization: `c × RZ(basis_current) ≤ 0`;
- flow reversal: `c × flow_current ≥ +0.20` for two consecutive closed bars;
- adverse invalidation: a crowded-long short exits if a closed close exceeds the maximum high of the 12-bar precondition window; a crowded-short long exits if it closes below the corresponding minimum low.

Actual funding settlements during the hold are P&L cashflows. Liquidation stream observations are reported only as lower-bound attribution because the official feed is censored.

### 7.5 Required data and current status

Requires klines, OI, mark/index, timestamped funding estimates, trades/aggTrades, BBO, depth, fees, and realized funding. **Current historical verdict: `INCONCLUSIVE_DATA`; no proxy substitution allowed.**

---

## 8. Strategy Family B — order-flow and visible-liquidity dislocation

### 8.1 Mechanism

Aggressive flow can produce continuation when opposing depth is depleted and does not replenish, or reversal when large flow fails to move price and opposing depth rapidly replenishes. These are mutually interpretable child mechanisms inside one family, not a collection of indicators.

A sequence-consistent local book is mandatory. Diff-depth update identifiers must reconcile; the selected standard-depth or RPI-depth stream must be frozen before T0. “Visible depth” means only what that stream publishes.

### 8.2 Closed-bar measurements

For each 5m bar:

```text
I_t       = (Qbuy − Qsell) / (Qbuy + Qsell + ε)
M_start   = median BBO mid over first 5 seconds
M_end     = median BBO mid over last 5 seconds
r_bar     = ln(M_end / M_start)
D_start   = median opposing-side depth within 10bp over first 30 seconds
D_low     = 5th percentile opposing-side depth within 10bp during the bar
D_end     = median opposing-side depth within 10bp over last 30 seconds
spread95  = 95th percentile BBO spread during the bar
```

For a positive imbalance, opposing depth is ask depth; for a negative imbalance, it is bid depth.

### 8.3 B1 depletion continuation

```text
abs(RZ(I)) ≥ 2.0
sign(I) × RZ(r_bar) ≥ 1.0
D_low / D_start ≤ 0.50
D_end / D_low < 1.20
spread95 ≤ 20bp
```

Trade in `sign(I)`.

### 8.4 B2 absorption reversal

```text
abs(RZ(I)) ≥ 2.0
sign(I) × RZ(r_bar) ≤ 0.25
abs(RZ(r_bar)) ≤ 0.75
D_end / D_low ≥ 1.50
spread95 ≤ 20bp
```

Trade against `sign(I)`.

Direction mapping:

- positive: Spot long / Futures long;
- negative: Spot exit-risk / Futures short.

### 8.5 Exits

- hard exit: 3 bars / 15 minutes;
- flow reversal: `position_sign × I_current ≤ −0.30` on a closed bar;
- adverse invalidation: close moves against the position by one event-bar true range from the entry reference, then exit at the next quote.

B1 and B2 share equal ex-ante risk inside the family. The union is the family-level primary portfolio; child claims use Bonferroni `α=0.025`.

### 8.6 Required data and current status

Requires trades/aggTrades, BBO, sequence-consistent depth, fees, and futures funding. **Current historical verdict: `INCONCLUSIVE_DATA`.** Candle taker-buy volume without receipt time and local-book evolution is not an acceptable replacement.

---

## 9. Strategy Family C — cross-sectional common-shock lag/catch-up

### 9.1 Mechanism

A broad market shock may reach assets asynchronously. The family trades liquid laggards in the direction of a common shock and exits when the frozen residual gap closes. Unlike ordinary momentum, the signal is relative to a point-in-time cross-sectional common component and a lagged beta.

Makarov–Schoar’s common/idiosyncratic flow decomposition motivates the distinction, while crypto factor papers warn that observed momentum is horizon-dependent. Neither establishes a 5m edge; this family must survive its own tests.

### 9.2 Features

Within each venue separately:

```text
r_i3,t    = ln(C_i,t / C_i,t−3)
m3_t      = median_i(r_i3,t)
beta_i,t  = clip[Cov(r_i,m)/Var(m), 0.25, 2.5]
            estimated only on the previous 8,640 bars
e_i3,t    = r_i3,t − beta_i,t × m3_t
sigma_i,t = 1.4826 × MAD(previous 8,640 e_i3)
L_i,t     = sign(m3_t) × (beta_i,t × m3_t − r_i3,t) / (sigma_i,t + ε)
```

Common shock gate:

```text
abs(m3_t) / robust_scale_30d(m3) ≥ 2.5
same-sign cross-sectional breadth ≥ 70%
eligible symbols ≥ 20
```

Select the top decile by `L`, requiring `L≥1.5`.

Actions:

- positive common shock: Spot long / Futures long;
- negative common shock: Spot exit-risk / Futures short.

### 9.3 Frozen residual-gap exit

At entry:

```text
g0 = sign(m3_t) × (beta_i,t × m3_t − r_i3,t) > 0
```

For later closed bar `t+h`:

```text
catch_h = sign(m3_t) × [ln(C_i,t+h/C_i,t)
          − beta_i,t × median_j ln(C_j,t+h/C_j,t)]
```

Exit when `catch_h ≥ 0.75g0`; adverse exit when `catch_h ≤ −0.50g0`; hard exit after 6 bars / 30 minutes.

### 9.4 Required data and current status

Point-in-time klines are sufficient only for a gross mechanism screen. BBO/depth, fees, and funding are mandatory for executable net expectancy. A kline-only positive result remains **exploratory** and cannot pass promotion gates.

---

## 10. Spot exit-risk estimand

A spot `EXIT_RISK` alert is not a synthetic short. It is evaluated on a standardized existing 100-USDT long inventory:

1. sell at the next executable bid after the alert;
2. remain flat until the family’s frozen exit/horizon;
3. rebuy at the next executable ask;
4. compute terminal wealth difference versus uninterrupted holding, including the two extra fee/slippage legs.

With no existing inventory, the output is an alert only and produces no simulated spot-short P&L.

---

## 11. Preregistered sequential testing ladder

### Stage 0 — data and artifact qualification

Require immutable source/data manifests and hashes; event/transaction/receipt/persistence timestamps; schema tests; local-book sequence tests; point-in-time universe reconstruction; no private/order dependencies; and a sealed R4a namespace. Failure stops the experiment before return analysis.

### Stage 1 — exposed-development mechanism sanity

Use only an explicitly exposed development segment to verify units, signs, event paths, pre-trends, matched placebos, and falsifying controls. No threshold may change after this stage.

### Stage 2 — fixed-rule gross edge

For each family, test the preregistered fixed rule before any classifier. The one-sided 95% synchronized time-block-bootstrap lower bound of gross mean must exceed zero. Failure rejects the family and forbids ML rescue.

### Stage 3 — executable replay

Use next BBO, depth walking, actual fees, and actual funding. Required data missing means `INCONCLUSIVE_DATA`; next-open, candle high/low, fixed spread, or zero-return missingness is forbidden.

### Stage 4 — leakage-safe anchored walk-forward

```text
train = 360 days
calibration = 90 days
test = 90 days
step = 90 days
```

All symbols sharing a UTC timestamp remain in the same fold. Purge the maximum label/holding horizon around boundaries and add a one-bar embargo. Fit robust scalers, beta, universe-dependent transforms, models, and calibration only on prior data.

### Stage 5 — optional single simple model

Activated only after the fixed family is executable-net positive. Permit one penalized logistic regression per family targeting positive executable net return, with isotonic calibration only when estimable. It may rank or abstain; no model zoo, feature search, or negative-family rescue.

### Stage 6 — non-overlapping portfolio, capacity, and stress

Run the frozen portfolio arbitration and the full latency/cost/depth grid.

### Stage 7 — prospective sealed holdout

Evaluate exactly once under the untouched-future policy in §17.

---

## 12. Leakage-safe validation and negative controls

Mandatory controls:

- `event_time`, `transaction_time`, `receipt_time`, and `available_time` are separate fields;
- current bar excluded from all rolling baselines;
- universe eligibility reconstructed as of each day;
- funding feature uses only an estimate published before the cutoff; realized rate enters P&L only after settlement;
- all cross-asset records with the same UTC time remain in one split;
- open episodes crossing split boundaries are purged or right-censored under a frozen rule;
- one-bar future shift must intentionally trigger the leakage test;
- sign-randomized flow, within-day timestamp shuffle, pre-event placebo, and stale/crossed-quote injection must eliminate or invalidate the signal;
- no future pivot, ZigZag, full-sample rank, current survivor list, or post-event liquidity filter.

A negative control is not an alpha candidate and cannot be promoted.

---

## 13. Multiple testing and backtest-overfitting controls

- Three primary family hypotheses, one synchronized daily net endpoint each.
- One-sided Holm familywise error control at `α=0.05` across A/B/C. [Holm primary paper](https://www.ime.usp.br/~abe/lista/pdf4R8xPVzCnX.pdf).
- B1/B2 child claims use Bonferroni `α=0.025`; the union portfolio remains Family B’s primary endpoint.
- Circular moving-block bootstrap on synchronized UTC daily portfolio P&L: 50,000 repetitions, seven-day primary blocks, 14/28-day sensitivity, seed `20260717`. Time-blocking is required because overlapping and clustered trades are not IID. [Künsch 1989](https://projecteuclid.org/journals/annals-of-statistics/volume-17/issue-3/The-Jackknife-and-the-Bootstrap-for-General-Stationary-Observations/10.1214/aos/1176347265.full).
- Deflated Sharpe probability ≥0.95. [Bailey & López de Prado](https://papers.ssrn.com/sol3/papers.cfm?abstract_id=2460551).
- Probability of backtest overfitting ≤0.10 over every R4b attempt, including failures. [Bailey et al.](https://papers.ssrn.com/sol3/papers.cfm?abstract_id=2326253).
- White Reality Check and Hansen Superior Predictive Ability test over the complete immutable strategy library. [White 2000](https://www.ssc.wisc.edu/~bhansen/718/White2000.pdf); [Hansen SPA](https://papers.ssrn.com/sol3/papers.cfm?abstract_id=264569).
- Matched nulls conditioned on symbol, venue, UTC time-of-week, side, duration, liquidity/spread decile, volatility regime, and event cluster.
- Trade-level IID t-tests and “best Sharpe among variants” are forbidden.

---

## 14. Non-overlapping portfolio simulation

- Three ex-ante family risk sleeves: one-third each.
- One active position per venue-symbol.
- Same-family repeats ignored until flat plus one closed-5m cooldown.
- Same symbol and direction across families are aggregated, not duplicated.
- Opposite new signals at the same decision cancel the new trade; spot exit-risk overrides spot entry.
- Futures leverage ≤1×; gross notional ≤1 NAV; combined symbol notional ≤10% NAV.
- Target 10% annualized portfolio volatility using only lagged 30-day daily returns.
- Mark every active position to executable BBO and include actual funding.
- The opportunity ledger remains fixed at 100 USDT for comparable event statistics; portfolio results are a separate synchronized ledger.

This prevents overlapping signals from being counted as independent trades or from silently multiplying exposure.

---

## 15. Execution sensitivity

Baseline is taker-only, actual fee schedule frozen and hashed, no VIP or BNB discount, full visible-depth walk, and 250ms post-decision latency.

Mandatory grid:

| Dimension | Values |
|---|---|
| Latency | 250ms, 1s, 3s, 10s |
| All-in cost multiplier | 1×, 1.5×, 2×, 3× |
| Visible-depth haircut | 0%, 50%, 80% |
| Notional | 100, 1,000, 10,000 USDT |

A trade is additionally capped at 1% of visible depth within 10bp and 1% of closed-5m quote volume. Promotion gates must pass at 100 and 1,000 USDT; 10,000 USDT is a capacity report only. Maker/queue models require a separate preregistration.

Hard stops and technical exits fill at the next quote, never at a candle high/low or an already observed close/open.

---

## 16. Minimum gates: sample, stability, drawdown, calibration, PF, and risk-adjusted return

All gates are conjunctive.

### 16.1 Data and sample

Historical OOF per family:

- ≥1,000 executed alerts;
- ≥300 non-overlapping episodes;
- ≥365 calendar days and four complete calendar quarters.

Prospective sealed holdout per family:

- ≥500 executions;
- ≥150 non-overlapping episodes;
- ≥180 days and two complete quarters;
- maximum wait 365 days; otherwise `INCONCLUSIVE_COVERAGE`.

Each reported venue/action sleeve needs ≥250 historical executions over ≥75 active days and ≥100 prospective executions over ≥45 days. Otherwise it is disabled and receives no claim.

### 16.2 Economic and profit-factor gates

After fees, slippage, depth walking, and funding:

- one-sided 95% synchronized-block-bootstrap lower bound of mean net return >0;
- mean net return ≥5bp per execution;
- gross edge / baseline all-in cost ≥1.5;
- cumulative net P&L >0;
- PF point estimate ≥1.20 and block-bootstrap 95% lower bound >1.05;
- at 2× costs, mean net ≥0 and PF ≥1.0;
- 3× costs reported, not a pass gate.

### 16.3 Stability and concentration gates

- positive in ≥75% of OOF folds and at least three of four evaluated quarters;
- no single quarter >35% of positive P&L;
- no single symbol >20% of positive P&L;
- ≥60% of event-bearing symbols positive;
- net remains positive after removing the top three symbols and the top ten trades.

### 16.4 Drawdown and risk-adjusted gates

On synchronized daily net returns:

- Sharpe ≥1.0;
- Sortino ≥1.25;
- deflated-Sharpe probability ≥0.95;
- 2×-cost Sharpe ≥0;
- at 10% volatility target, maximum drawdown ≤15%;
- Calmar ≥0.75.

These are research promotion gates, not a profit guarantee.

### 16.5 Calibration gates, only if a model is used

- positive Brier skill versus a rolling base-rate model;
- log loss below intercept-only model;
- calibration slope 0.8–1.2;
- absolute intercept ≤0.05;
- ECE ≤0.03;
- ≥100 observations per probability decile;
- monotonic predicted-to-realized net return;
- accepted bucket’s predicted-net 95% lower bound >0.

Rule-only families receive no probability-calibration claim.

---

## 17. Untouched future holdout policy

`H_start` is the first 00:00 UTC after all three conditions hold:

1. external timestamp of signed spec, code, config, schema, container/lockfile, endpoint registry, seed, and source/data hashes;
2. 30 consecutive days of all required streams at ≥99.9% coverage with sequence continuity;
3. no open P0/P1 defect.

During the holdout, operators may inspect data coverage, parser/sequence health, duplicate/outbox metrics, and resource bounds. They may not inspect labels, family/sleeve P&L, threshold-conditioned outcomes, or calibration results.

Evaluate once after 180 days **and** all sample gates. Maximum duration is 365 days. Open positions at cutoff are right-censored at the last executable BBO; no future price is used to force-close them retrospectively.

Any change to a feature, threshold, action mapping, exit, horizon, fee/slippage model, universe rule, data source/schema, or any outcome-affecting bug creates a new protocol version and resets the clock. Receipt time, BBO/depth, or liquidation completeness may not be backfilled with proxies.

---

## 18. Hard reasons to stop

Stop and issue the listed terminal state when any condition applies:

1. **No auditable R4a artifacts:** no comparative “beats R4a” claim.
2. **Required raw data absent:** `INCONCLUSIVE_DATA`; no kline proxy.
3. **Gross lower bound ≤0:** reject before ML.
4. **Edge disappears at next BBO or 1.5× cost:** reject.
5. **2× cost net <0 or PF <1:** reject.
6. **Any primary sealed-holdout gate fails:** reject; no threshold rescue.
7. **Stability, concentration, DSR, or PBO gate fails:** reject.
8. **Observed slippage exceeds the model by 25% or two standard errors over a rolling 30-day window:** suspend and void efficacy inference until a new version.
9. **Material data gap, schema shift, or exchange market-structure change:** stop/version-reset.
10. **Any post-hoc parameter modification:** void the protocol.
11. **Minimum prospective sample not reached by day 365:** `INCONCLUSIVE_COVERAGE`.
12. **All three families fail Stage 2 or the sealed holdout:** conclude that the available data/version cannot support a reproducible positive-expectancy alert strategy and stop searching this dataset.

---

## 19. Frozen decision statement

The rational R4b program is not “find a better indicator.” It is:

- seal R4a;
- collect or acquire auditable non-kline data;
- test three mechanism families under fixed rules;
- reject gross-negative families before ML;
- replay at executable quotes with costs and funding;
- synchronize portfolio inference by time;
- control the complete attempt library for multiplicity and overfitting;
- evaluate one untouched future holdout;
- stop on failure.

Until that process produces a pass, the only defensible conclusion is **no demonstrated positive expectancy**.

---

## 20. Verified external primary sources

- 🛡️ [Binance Public Data — official downloadable archive and checksums](https://github.com/binance/binance-public-data)
- 🛡️ [Binance USDⓈ-M Futures REST Market Data — official documentation](https://developers.binance.com/en/docs/catalog/core-trading-derivatives-trading-usd-s-m-futures/api/rest-api/market-data)
- 🛡️ [Binance USDⓈ-M Futures Public WebSocket Streams — BBO and depth](https://developers.binance.com/en/docs/catalog/core-trading-derivatives-trading-usd-s-m-futures/api/ws-streams/public)
- 🛡️ [Binance USDⓈ-M Futures Market WebSocket Streams — mark/funding and liquidation](https://developers.binance.com/en/docs/catalog/core-trading-derivatives-trading-usd-s-m-futures/api/ws-streams/market)
- 🏛️ [Liu & Tsyvinski, “Risks and Returns of Cryptocurrency,” NBER w24877](https://www.nber.org/system/files/working_papers/w24877/w24877.pdf)
- 🏛️ [Liu, Tsyvinski & Wu, “Common Risk Factors in Cryptocurrency,” NBER w25882](https://www.nber.org/system/files/working_papers/w25882/w25882.pdf)
- 🏛️ [Makarov & Schoar, “Trading and Arbitrage in Cryptocurrency Markets”](https://papers.ssrn.com/sol3/papers.cfm?abstract_id=3171204)
- 🏛️ [He, Manela, Ross & von Wachter, “Fundamentals of Perpetual Futures”](https://papers.ssrn.com/sol3/papers.cfm?abstract_id=4301150)
- 🏛️ [Albers et al., perpetual-market and liquidation-cascade research, arXiv:2108.09750](https://arxiv.org/pdf/2108.09750)
- 🏛️ [Cont, Kukanov & Stoikov, “The Price Impact of Order Book Events”](https://arxiv.org/abs/1011.6402)
- 🏛️ [White, “A Reality Check for Data Snooping”](https://www.ssc.wisc.edu/~bhansen/718/White2000.pdf)
- 🏛️ [Hansen, “A Test for Superior Predictive Ability”](https://papers.ssrn.com/sol3/papers.cfm?abstract_id=264569)
- 🏛️ [Bailey & López de Prado, “The Deflated Sharpe Ratio”](https://papers.ssrn.com/sol3/papers.cfm?abstract_id=2460551)
- 🏛️ [Bailey et al., “The Probability of Backtest Overfitting”](https://papers.ssrn.com/sol3/papers.cfm?abstract_id=2326253)
- 🏛️ [Holm, “A Simple Sequentially Rejective Multiple Test Procedure”](https://www.ime.usp.br/~abe/lista/pdf4R8xPVzCnX.pdf)
- 🏛️ [Künsch, “The Jackknife and the Bootstrap for General Stationary Observations”](https://projecteuclid.org/journals/annals-of-statistics/volume-17/issue-3/The-Jackknife-and-the-Bootstrap-for-General-Stationary-Observations/10.1214/aos/1176347265.full)

## 21. Companion artifact

The exact machine-readable specification is `R4b_frozen_experiment_spec_v1.yaml`, SHA-256 `9c925f5988e65a1371e8859dd00ea6a61db0c3b9ea34622432e9a28a3bab297b`.
