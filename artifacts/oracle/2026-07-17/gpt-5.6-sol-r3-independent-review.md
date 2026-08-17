[🟢 Online Mode | 26.07.17_00:52:16]

[🔬 Research Mode]

> 🔍 Search Strategy
>
> * Keywords: Binance public-data contracts, closed-kline determinism, event-time barriers, Discord transactional outbox, block bootstrap, Holm intersection-union tests, cryptocurrency forecasting costs and leakage
> * Focus: code safety, runtime determinism, statistical estimands, result provenance, economic realism, literature validity, and preservation of the alert-only/no-order boundary

[🌐 웹 서치 적용: official Binance and Discord documentation and primary research were cross-checked against the supplied File Library artifacts.]

# Independent adversarial audit

## Executive verdict

The project’s **economic restraint is stronger than its scientific and operational evidence**. Keeping G2/G4 disabled is justified. Calling the earlier work fully verified is not. The present runtime is not ready for unattended public alert-service deployment because alert outcomes can depend on event arrival order, state may fail to rearm, and delivery is not transactionally auditable.

The frozen R2 plan is a materially better retrospective diagnostic design. It does not constitute a confirmatory out-of-sample experiment, and I could not locate a result bundle that permits independent reconstruction of its reported statistics. The full execution-feasibility candidate must remain `INCONCLUSIVE_NO_HISTORICAL_BBO`, exactly as the plan states.

| Audit object                             | Verdict                                               | Consequence                                                                                                  |
| ---------------------------------------- | ----------------------------------------------------- | ------------------------------------------------------------------------------------------------------------ |
| Visible public-data/no-order boundary    | **PASS — static scope only**                          | No visible private/auth/signing/order path was found, but omitted modules prevent an end-to-end certificate. |
| R1 G2/G4 activation                      | **REJECT; keep disabled**                             | The supplied results do not show a post-cost economic edge.                                                  |
| R1 “VERIFIED / fully reproducible” claim | **NOT CERTIFIED**                                     | Critical inputs, manifests, run outputs, environment lock and tests are absent.                              |
| Current unattended alert runtime         | **RELEASE BLOCKED**                                   | Event-time, lifecycle, state disposal and delivery defects remain.                                           |
| Frozen R2 plan                           | **ACCEPTABLE AS AN EXPOSED RETROSPECTIVE DIAGNOSTIC** | It may support regression/negative-control conclusions, not confirmation.                                    |
| R2 efficacy result                       | **NOT AUDITABLE FROM THE SUPPLIED ARTIFACTS**         | No independent PASS/FAIL conclusion can be issued.                                                           |
| Full `R2_PIT_HTF_EXEC`                   | **`INCONCLUSIVE_NO_HISTORICAL_BBO`**                  | Historical klines and funding cannot reconstruct decision-time BBO, quantity, depth or receipt time.         |
| Production order placement               | **PROHIBITED AND OUT OF SCOPE**                       | The architecture should permanently exclude private endpoints, signers, API-key custody and order clients.   |

The supplied independent-audit artifact reaches a compatible high-level conclusion: G2/G4 rejection should stand, reproducibility is not certified, the current alert engine is blocked, and order placement must remain structurally absent. 

---

## 1. Evidence boundary and reproducibility

The supplied package identifies 31 original files under SHA-256 `4835cce5808a6deaf2e743e393f9c116b44f9563a3f18414cd03360a000ee218`. Its embedded context copy reportedly matched 30/30 files modulo terminal newlines. That establishes internal package consistency, not run reproducibility. 

Material omissions include:

* `src/signalbot/clock.py`
* `src/signalbot/data/candles.py`
* `src/signalbot/exchange/binance/schemas.py`
* `src/signalbot/persistence/repository.py`
* `tests/conftest.py`
* `uv.lock`
* the R1 experiment plan, feature contract and run ledger
* raw Binance inputs and their checksums
* G2/G4 effective specifications
* run manifests, opportunities, trades and result files
* the 7/14/28-day comparison outputs and verification JSON

Those omissions prevent verification of websocket parsing, timestamp units, candle deduplication, gap handling, database uniqueness and transactions, Discord delivery, dependency resolution, or the precise code/configuration that generated the claimed R1 results. 

The independently recorded execution evidence is narrower:

| Check                                                         | Observed result                                                      |
| ------------------------------------------------------------- | -------------------------------------------------------------------- |
| Python compilation                                            | `compileall` passed                                                  |
| Isolated state-machine tests                                  | 7 passed                                                             |
| Full pytest collection                                        | Failed with seven collection errors because `conftest.py` was absent |
| Runtime import                                                | Not fully reproducible because imported modules were absent          |
| Claimed 176 tests, ruff and pyright                           | Not reproducible from the supplied package                           |
| Visible static search for private/auth/order/signing patterns | No hit in the visible scope                                          |

The verification log directly shows the missing-`conftest` collection failures.  

**Audit implication:** the correct label is not “verified.” It is:

> **Internally consistent partial package; selected source behavior inspected; full build, run and result provenance not reproduced.**

The static no-order result is favorable, but it must remain qualified because the omitted runtime and exchange modules are precisely where endpoint selection and network behavior would ordinarily be implemented.

---

## 2. Severity-ranked Python and alert-service findings

### P0 — release blockers

#### P0-1. Alert treatment depends on network arrival order

The current design evaluates mutable state as individual events arrive rather than finalizing one immutable decision snapshot for each closed 5-minute cutoff. Therefore identical exchange data can produce different eligibility or alert outcomes when candle, HTF, regime, order-flow or quote messages arrive in a different order.

This is both an engineering defect and a scientific defect: it changes which observations receive the H1 treatment.

The correction is an explicit event-time coordinator:

1. Accept a 5-minute decision only when Binance marks the kline closed.
2. Assign every input an envelope containing distinct exchange-event, object and receipt clocks.
3. Finalize the decision after the required same-cutoff inputs arrive or a fixed timeout expires.
4. Use only HTF snapshots with `context_close_time < decision_time`.
5. Store missing context as unavailable rather than silently using a partial cross-section.
6. Prevent late events from retroactively changing an already finalized alert.

Binance exposes a specific closed-kline Boolean, so closed-candle processing need not be inferred from wall-clock timing. ([바이낸스 개발자 센터][1])

#### P0-2. Anomaly alerts can fail to rearm

The supplied reproduction shows that the anomaly detector returns no explicit normal/clear evaluation. Once the state machine reaches `CONFIRMED`, ordinary subsequent observations do not return it to `IDLE`. A later independent anomaly in the same direction can therefore be suppressed. 

Required lifecycle semantics are:

* `TRIGGER`
* explicit `CLEAR`
* expiry/TTL
* heartbeat and source-coverage state
* generation-aware pruning
* a regression test for anomaly → normalization → second anomaly

This is not a cosmetic issue: it creates silent false negatives while leaving the service apparently healthy.

#### P0-3. Alert persistence and Discord delivery are not one auditable transaction

A decision can be recorded without a message being delivered, or delivered before durable state is committed. Blind retries after an ambiguous timeout can also duplicate alerts.

The alert-only solution is a transactional outbox:

* persist the decision and outbox row in one database transaction;
* use states such as `PENDING`, `SENDING`, `DELIVERED`, `UNCERTAIN`, and `DEAD`;
* execute the Discord webhook with `wait=true`;
* persist the returned message identifier and response hash;
* retry definite failures and documented rate-limit responses;
* classify connection loss after transmission as `UNCERTAIN`, rather than blindly resending.

Discord documents that `wait=true` waits for server confirmation and returns the created message body. Its documented Execute Webhook contract does not expose an idempotency key, so provable exactly-once delivery across ambiguous network failure cannot be claimed; the uncertainty must instead be represented explicitly. ([Documentation - Discord][2])

#### P0-4. The supplied runtime cannot receive an end-to-end certificate

Because critical imported modules and the common test fixture are missing, the package cannot prove:

* valid Binance schema parsing;
* closed-candle deduplication;
* persistence uniqueness;
* atomic commits;
* reconnect/gap behavior;
* Discord sender behavior;
* endpoint allowlisting.

This blocks an unattended alert-service release independently of signal quality.

---

### P1 — scientific and operational blockers

| Finding                                                           | Audit effect                                                                                                                                         | Required alert-only correction                                                                                               |
| ----------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------- |
| Universe changes only assign a new symbol set                     | Removed symbols can leave feature, funding, anomaly, state-machine and paper-ledger state behind; late events from a prior universe may be accepted. | Issue a universe generation ID, atomically reconcile every state owner, prune removed keys and reject old-generation events. |
| Bootstrap/gap replay differs from live processing                 | A replay can produce features or alerts not obtainable through the live path.                                                                        | Feed bootstrap, gap recovery, deterministic replay and live events through one sequential pipeline.                          |
| Live and backtest confirmation/funding contracts differ           | The purportedly tested policy is not the policy emitted by the service.                                                                              | Use one pure point-in-time calculator and one immutable eligibility implementation in live, replay and backtest.             |
| Split-boundary state is not fully purged                          | Positions or pending state may cross a split even when opportunity labels appear split-clean.                                                        | Cancel pending state, close or exclude paper positions, reset the state machine and assert zero boundary-crossing episodes.  |
| No durable paper outcome ledger is wired into the visible runtime | Discord counts cannot establish post-alert performance.                                                                                              | Maintain a persistent quote-based paper ledger with provenance; it must never call an order endpoint.                        |

The split problem invalidates an interpretation of the R1 split tables as independent clean segments. It does not rescue G2/G4 because the supplied aggregate and split results are consistently adverse. 

#### Clock provenance requires particular care

Spot `bookTicker` supplies update ID, best bid and ask prices, and their quantities, but its documented payload does not contain an exchange-event timestamp. Treating a local receipt timestamp as exchange event time fabricates precision. `exchange_event_time` should be null and `receipt_time` should be populated explicitly. ([바이낸스 개발자 센터][1])

The supplied audit also records exchange, object and receipt clocks being mixed in the current runtime, along with different funding contracts in live and backtest paths. 

---

### P2 — correctness, interpretation and provenance defects

1. **Short EMA equality bug.** The short alignment path uses the negation of `ema20 > ema50`, which treats equality as bearish alignment. The exact mirror is `ema20 < ema50`.

2. **“95 completeness” is not 95% completeness.** The visible implementation is a heuristic score composed of fixed points, flow presence and spread presence. It should be replaced by field-level availability and freshness masks. 

3. **Bootstrap provenance mismatch.** The supplied C0 configuration specifies 2,000 bootstrap samples, while the report claims 50,000. An undocumented runner override is possible, but without the run manifest it remains unresolved. 

4. **Changed-only ticker observations are not a regular 1 Hz series.** Binance states that the all-market mini-ticker array contains only symbols that changed. Anomaly statistics must use an explicit grid, heartbeat and coverage model rather than interpreting each arrival as an evenly spaced sample. ([바이낸스 개발자 센터][1])

5. **OHLC data cannot resolve every same-bar path.** Initial stops, trailing updates, opposite triggers and trend-failure exits require an immutable conservative priority rule and an ambiguity sensitivity.

6. **Fixed current assets introduce survivor selection.** The eight-asset panel is useful for a fixed diagnostic but does not establish performance in the historical investable universe.

7. **Recursive indicators require parity tests.** A bounded streaming history can diverge from a full batch calculation, especially for recursively seeded EMA/MACD/ADX implementations.

---

## 3. R1 economic conclusion

The earlier G2/G4 rejection should stand.

According to the supplied ledgers and audit:

* all six C0/G2/G4 × Spot-long/Futures-short technical combinations had negative average gross and net returns;
* all had profit factor below one;
* the zero-slippage sensitivity remained negative;
* every reported asset-direction combination lost money;
* G4 Spot’s fixed-horizon `+0.7443 bp` estimate is a different estimand and is economically negligible beside its reported technical-trade mean of `−32.6200 bp` and approximately `32.29 bp` of cost drag.  

Consequently:

* **Do not reactivate G2 or G4.**
* **Do not treat R1 as validation of the present live gate stack.**
* **Use its period only as a regression fixture and negative control.**
* **Downgrade “fully verified” to “reported negative result with incomplete reproducibility evidence.”**

The discovered implementation and provenance defects do not create positive evidence. They reduce confidence in exact numerical attribution and split-level interpretation.

---

## 4. Frozen R2 plan: scientific audit

### 4.1 What the plan gets right

The plan is unusually explicit about its limitations:

* it labels the sample an already exposed retrospective regression/negative control;
* it forbids calling the result untouched OOS, a confirmed edge, a recommendation or live approval;
* it freezes C0 and adds only a Boolean strict-prior 15-minute/1-hour HTF condition;
* it separates Spot long and USDⓈ-M Futures short;
* it uses a common C0 opportunity-ID panel;
* it records rejected H1 opportunities as zero policy contribution rather than deleting them;
* it includes fee, adverse-execution and funding assumptions;
* it jointly resamples assets and markets by UTC day;
* it controls a four-composite primary family;
* it states in advance that the full execution-feasibility candidate is inconclusive without historical BBO and receipt-time data.  

Those are meaningful improvements over post-hoc threshold searching.

### 4.2 The sample remains retrospective in its entirety

The names `development`, `validation`, and `retrospective_test` do not create a fresh test set when all three periods were already exposed to prior strategy development. The last segment may be useful for software regression and robustness description, but it cannot restore confirmatory status.

Any amendment made after viewing outcomes must be:

* recorded as a new immutable protocol version;
* timestamped;
* accompanied by a reason and content hash;
* described as another exposed analysis.

It cannot retroactively become preregistration.

### 4.3 Estimands are defensible but need exact labels

For the common C0 panel,

[
\theta_{C0}=E[R], \qquad
\theta_{H1}=E[IR], \qquad
\mu_{H1}=E[R\mid I=1].
]

Because rejected opportunities receive zero:

[
\delta_{\text{entry}}
=\theta_{H1}-\theta_{C0}
=-E[(1-I)R].
]

Therefore:

* `mu_H1 > 0` tests whether accepted alerts have positive mean net return;
* `delta_entry > 0` tests whether zeroing the rejected opportunities improves contribution per original C0 opportunity;
* `delta_entry` does **not** mean the accepted subset’s average return is greater than C0’s unconditional average;
* `theta_H1` assumes idle capital earns zero and is a return contribution per raw opportunity, not a return per accepted alert.

The two-part entry composite is coherent when reported in those terms.

### 4.4 Independent overlapping episodes are not a realizable portfolio

Treating every common trigger as an independent episode is appropriate for an alert-level event study. It is not appropriate for interpreting cumulative P&L or profit factor as deployable portfolio performance when episodes overlap.

For example, multiple 100 USDT opportunities can be open simultaneously in the independent panel. Summing them implicitly permits unconstrained concurrent notional and repeated exposure to the same market move.

The distinction should be:

* **Primary alert estimand:** all raw opportunities, including overlaps.
* **Primary capital/economic estimand:** one-position sequential replay with frozen concurrency and conflict rules.
* **Secondary descriptive metric:** independent-episode cumulative P&L and PF, explicitly marked non-portfolio.

The present plan reverses the first two for economic interpretation.

### 4.5 Multiplicity logic is mostly sound; confidence-bound wording is not yet operational

For a composite requiring both component effects to be positive, using

[
p_{\text{composite}}=\max(p_1,p_2)
]

is a valid intersection-union test. Applying Holm’s step-down procedure to the four composite p-values can control the familywise error rate under arbitrary dependence. ([JSTOR][3])

The unresolved part is the phrase “both Holm-adjusted lower bounds.” Holm is directly a multiple-hypothesis decision procedure. Simultaneous lower confidence bounds require a specified inversion of that procedure or another explicit familywise construction.

Until such an algorithm is frozen, the defensible claims are:

* Holm-adjusted composite rejection decisions;
* endpoint point estimates;
* endpoint bootstrap bounds whose coverage level is stated without implying unproved familywise simultaneous coverage.

A versioned specification should define exactly how rank-dependent Holm thresholds are converted into endpoint lower bounds.

### 4.6 Bootstrap requirements

The synchronous UTC-day moving-block design is sensible because it preserves broad cross-asset dependence. Moving-block bootstrap methodology is, however, based on an approximate stationary dependence structure; a two-year crypto sample contains substantial regime variation. ([Project Euclid][4])

The implementation must therefore:

* retain every calendar day, including zero-opportunity days;
* resample all assets and both markets with the same date blocks;
* preserve the validity/gap mask;
* recompute accepted means, PF, concentration and all ratio statistics inside each replicate;
* predefine behavior for zero accepted trades or zero losses;
* report the fixed 7-, 14- and 28-day results separately;
* treat the circular endpoint connection as a modeling choice, not a fact;
* report Monte Carlo error and invalid-replicate counts.

Fifty thousand replicates reduce simulation noise. They do not compensate for inadequate days, poor coverage or dependence-driven low effective sample size.

### 4.7 Information thresholds are not power analysis

The thresholds of 500 accepted/openable episodes, 120 valid days and 10% coverage prevent extremely sparse analysis. They do not demonstrate adequate power for a 5 bp net effect.

Before interpreting a failure to reject, the analysis needs a cluster-aware minimum-detectable-effect calculation using the observed daily dependence and coverage. The result can then distinguish:

* evidence against an economically relevant effect;
* insufficient information to detect that effect;
* a numerically positive but fragile estimate.

The information thresholds must also apply per relevant primary market/composite, not after pooling Spot and Futures.

### 4.8 Cost hurdle and execution interpretation

The frozen assumptions imply the following nominal round-trip friction before Futures funding:

| Market/class         | Fee and adverse execution per side |          Round trip |
| -------------------- | ---------------------------------: | ------------------: |
| Spot major/anchor    |                       10 bp + 5 bp |           **30 bp** |
| Spot volatile        |                      10 bp + 10 bp |           **40 bp** |
| Futures major/anchor |                        5 bp + 3 bp | **16 bp + funding** |
| Futures volatile     |                        5 bp + 8 bp | **26 bp + funding** |

These are conservative model inputs, not observed fills. The major/volatile asset mapping must be frozen and output in the manifest. Because the plan’s 5 bp threshold is net, the gross movement must first overcome the corresponding friction hurdle.

Binance does provide public historical funding records with `fundingRate`, `fundingTime` and associated mark price. The strict inequality governing whether a settled funding event lies inside an episode should be tested against exchange timestamps. ([바이낸스 개발자 센터][5])

Historical BBO feasibility cannot be reconstructed from the official public archive. The archive documents klines, trades and aggregate trades, and Spot timestamps from 2025 onward use microseconds; it does not provide historical decision-time `bookTicker` receipt records. Archive files may also later be replaced following corrections, so input checksums and retrieval metadata are mandatory. ([GitHub][6])

### 4.9 Exit path needs an immutable conservative convention

Kline OHLC cannot establish whether multiple intrabar events occurred in this order:

* initial stop;
* 1R activation;
* trailing stop;
* trend-failure completion;
* eligible opposite breakout.

The plan correctly prevents a close-updated trail from acting on the same candle, but every remaining collision needs a fixed priority. Ambiguous bars should also be rerun under a uniformly adverse priority as a sensitivity.

---

## 5. R2 results status

I located the frozen `experiment_plan.md` and the pasted literature narrative. I did **not** locate an independently auditable R2 result bundle containing raw opportunities, a run manifest, canonical daily panels, bootstrap output or result hashes.

This is not proof that no result was ever produced. It means no result claim can be independently certified from the accessible artifacts.

A minimally auditable result package must contain:

1. **Immutable run manifest**
   Protocol version, code hash, configuration hash, lockfile hash, interpreter and package versions, seed, execution time, input checksums and timestamp-unit declarations.

2. **Common opportunity ledger**
   Opportunity ID, symbol, market, direction, decision and entry times, split, C0 Boolean, H1 Boolean, exact HTF source timestamps, availability flags, exclusion reason, F60 and T72 components, fees, adverse execution, funding and net return.

3. **Canonical daily panel**
   Every UTC day, including zero-opportunity days, with shared cross-market resampling keys and source-validity masks.

4. **Result record**
   Counts, coverage, accepted and policy-contribution means, cumulative fixed-notional figures, PF, 0×/1×/2× cost results, per-asset contributions and concentration.

5. **Inference audit**
   Block lengths, seed, 50,000 replicate confirmation, invalid replicate count, endpoint p-values, composite p-values, Holm ordering, adjusted decisions and the exact confidence-bound algorithm.

6. **Parity and safety evidence**
   A/B canonical hashes, full test logs, static endpoint scan, import success and an explicit `INCONCLUSIVE_NO_HISTORICAL_BBO` field for the full candidate.

Until that package exists, the only defensible R2 result verdict is:

> **`R2_RESULT_NOT_AUDITABLE`**

No `RETROSPECTIVE_SCREEN_PASS`, efficacy claim or alert-service approval is supported.

---

## 6. Public-data, alert-only target architecture

The corrective architecture should contain no order abstraction at all.

1. **Schema-validated `EventEnvelope`**
   `exchange_event_time | null`, `object_time`, `receipt_time`, stream session, sequence/update ID, universe generation and raw-payload hash.

2. **Deterministic event-time coordinator**
   Closed 5-minute candles trigger decisions; HTF and global context are immutable strict-prior snapshots; late data cannot silently rewrite a decision.

3. **Shared point-in-time feature implementation**
   Live, historical replay, gap recovery and backtest call the same pure calculation path.

4. **Separate raw opportunity and eligibility engines**
   C0 creation and H1 acceptance are separate Boolean records. A strong score in one dimension must never compensate for a failed mandatory gate.

5. **Durable public-quote paper ledger**
   Alert entry and exit outcomes are calculated from recorded public quotes or conservative kline proxies. This ledger has no Binance account, balance, position or order concept.

6. **Transactional Discord outbox**
   Durable decision/outbox commit, explicit uncertainty state, bounded retries, message-ID provenance and backlog/resource limits.

7. **Hard CI safety boundary**
   Fail the build on signers, API-key configuration, private/account/order endpoint paths, order clients or dependencies that introduce execution behavior. Network integration tests should allow only approved public market-data hosts and the configured Discord webhook.

### Alert-service release gates

Before unattended Discord alerts are enabled, the service should demonstrate:

* identical decision hashes under permuted same-cutoff event arrival;
* replay/live feature parity;
* no split or universe-generation state leakage;
* repeated anomaly rearming;
* crash-matrix outbox tests before and after transmission;
* bounded memory, disk and outbox backlog;
* complete import and test collection;
* zero private/auth/order functionality.

| Operating mode                                            | Disposition                                         |
| --------------------------------------------------------- | --------------------------------------------------- |
| Offline research and negative-control replay              | **GO**                                              |
| Public historical-data ingestion                          | **GO with checksum and timestamp validation**       |
| Prospective public BBO/depth capture with alerts disabled | **CONDITIONAL GO after schema and retention tests** |
| Unattended Discord alert emission                         | **BLOCKED pending P0 fixes**                        |
| R2 efficacy labeling                                      | **BLOCKED pending auditable result bundle**         |
| Production order placement                                | **PERMANENTLY PROHIBITED**                          |

---

## 7. Audit of the pasted literature claims

The pasted review says it cross-checked 18 core papers, but the exact 18-record bibliography, query log, inclusion criteria, version ledger and extracted evidence table were not supplied. That search-process claim is therefore not reproducible from the pasted text itself. 

### Claim-by-claim verdict

| Pasted claim                                                                                             | Verdict                                                  | Correct interpretation                                                                                                                                                                                                        |
| -------------------------------------------------------------------------------------------------------- | -------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Reproducible directional accuracy is generally 52–60%                                                    | **Overgeneralized**                                      | One strong daily top-100 cryptocurrency study reported 52.9–54.1% over all predictions and 57.5–59.5% for its top-confidence subset. That is evidence from one design, not a universal field-wide range. ([ScienceDirect][7]) |
| Studies reporting 80–96% often reflect price-level targets, rule imitation, weak splits or missing costs | **Useful red flag, too broad as a conclusion**           | Each paper must be classified by target, horizon, temporal split, normalization, tuning and cost treatment. Some high-number papers do exhibit serious target or leakage concerns; not all omit costs.                        |
| At 1-minute and 5-minute horizons, costs can overwhelm predictability                                    | **Strongly supported as a warning, not a universal law** | A 1–60 minute Bitcoin study found a profitable pre-cost strategy became negative after costs. Other research evaluates numerous daily/intraday rules with transaction costs and data-mining controls. ([ScienceDirect][8])    |
| Selective, high-confidence alerts are more defensible than predicting every candle                       | **Conditionally supported**                              | Confidence selection can raise classification accuracy, but thresholds, coverage, calibration, cost and drift must be frozen and reported.                                                                                    |
| A 5-minute decision with a 15-minute-to-hours outcome is the literature-supported best design            | **Not established**                                      | This is a reasonable R2 hypothesis, not a demonstrated universal optimum. Horizons must be compared prospectively under identical costs and treatment rules.                                                                  |
| LightGBM/Random Forest with trend, volume, volatility and regime should be prioritized over RSI/MACD     | **Reasonable benchmark plan, not settled superiority**   | Models and indicators are different layers. Frozen rules, regularized logistic regression, RF and gradient boosting should be compared chronologically; no class receives priority based only on literature reputation.       |

### Specific paper corrections

#### Jaquart et al.

The 2022 daily-market study is the principal support for the pasted 52–60% range. Its full-sample accuracy was 52.9–54.1%, while the 10% highest-confidence predictions reached 57.5–59.5%. It also reports post-cost long-short portfolio results, so it should not be reduced to classification accuracy alone. ([ScienceDirect][7])

The related 2021 high-frequency study covered 1–60 minute horizons and found that predictability generally improved at longer horizons. A short-holding strategy that appeared highly profitable before costs became negative after costs. ([ScienceDirect][8])

#### Hafid et al. and the 92.4% figure

The cited 92.4% classifier is not clean evidence of future return-direction forecasting. The paper selects an MA(10,60) rule to create balanced buy/sell signals and then trains technical-indicator classifiers to reproduce that label. The reported accuracy is therefore substantially a rule-label imitation result, not a 92.4% probability of correctly forecasting a future tradable move. ([arXiv][9])

#### Dashtaki et al. and the 96.8% figure

The pasted criticism that this study omitted transaction costs is incorrect: its trading simulation assumes a 0.1% commission. ([arXiv][10])

There is, however, a material reproducibility ambiguity. One methods passage says the full input is min-max normalized and then partitioned using a stratified 70/15/15 ratio, while a later passage says the combined data were chronologically partitioned. Without the implementation, the order of scaling and the true split process cannot be resolved. The paper predicts the subsequent trading day, not a 5-minute move. ([arXiv][10])

#### CryptoPulse

CryptoPulse forecasts the next day’s closing price. It does not directly validate a five-minute alert horizon. ([arXiv][11])

#### Alonso-Monsalve et al.

The one-minute CNN study explicitly describes itself as an instrument-comparison study rather than a profitable trading-system design and states that practical deployment would need decision logic, liquidity, timing and transaction-cost controls. Its classification results should not be treated as demonstrated net trading performance. ([ScienceDirect][12])

#### Broad surveys

The Fang et al. survey covers 146 papers across trading systems, systematic strategies, forecasting, portfolios, risk and other topics. It is a taxonomy and literature map, not evidence for one universal accuracy band or preferred model. ([arXiv][13])

### Correct literature conclusion for this project

The defensible synthesis is:

> Public technical and microstructure variables may contain weak, horizon-dependent predictive information. High classification accuracy is not equivalent to future-return accuracy, and future-return accuracy is not equivalent to positive post-cost alert utility. For a five-minute Binance alert service, strict point-in-time processing, selective coverage, realistic public-quote paper outcomes, temporal validation and complete provenance matter more than choosing a more complex model.

---

## 8. Final disposition

1. **Keep G2/G4 rejected.**
2. **Do not certify R1 as fully reproducible.**
3. **Do not issue an R2 efficacy verdict without the result bundle.**
4. **Keep full R2 execution feasibility at `INCONCLUSIVE_NO_HISTORICAL_BBO`.**
5. **Complete event-time, state-rearm, universe-reconciliation, parity and outbox work before unattended alerts.**
6. **Preserve a structural zero-order boundary: public market data, paper outcomes and alerts only.**

### 🔗 참조 출처 (Verified Sources)

* 📎 Supplied frozen R2 experiment plan. 
* 📎 Supplied independent R2 technical and experimental-design audit. 
* 📎 Supplied verification summary and pytest collection output. 
* 📎 Supplied pasted literature claims. 
* 🛡️ Binance Spot WebSocket Market Streams — closed-kline and book-ticker contracts. ([바이낸스 개발자 센터][1])
* 🛡️ Binance Public Data archive — historical file types, timestamp units and checksums. ([GitHub][6])
* 🛡️ Binance USDⓈ-M Funding Rate History. ([바이낸스 개발자 센터][5])
* 🛡️ Discord Webhook Resource — Execute Webhook confirmation behavior. ([Documentation - Discord][2])
* 🏛️ Holm (1979), “A Simple Sequentially Rejective Multiple Test Procedure,” *Scandinavian Journal of Statistics*. ([JSTOR][3])
* 🏛️ Künsch (1989), “The Jackknife and the Bootstrap for General Stationary Observations,” *The Annals of Statistics*. ([Project Euclid][4])
* 🏛️ Jaquart, Köpke and Weinhardt (2022), “Machine learning for cryptocurrency market prediction and trading,” *The Journal of Finance and Data Science*. ([ScienceDirect][7])
* 🏛️ Jaquart, Dann and Weinhardt (2021), “Short-term bitcoin market prediction via machine learning,” *The Journal of Finance and Data Science*. ([ScienceDirect][8])
* 🏛️ Svogun and Bazán-Palomino (2022), “Technical analysis in cryptocurrency markets: Do transaction costs and bubbles matter?”, *Journal of International Financial Markets, Institutions and Money*. ([FacultyUP — Universidad del Pacífico][14])
* 🏛️ Deprez and Frömmel (2024), “Are simple technical trading rules profitable in bitcoin markets?”, *International Review of Economics & Finance*. ([Ghent University Bibliography][15])
* 🏛️ Hafid et al., “Predicting Market Trends with Enhanced Technical Indicator Integration and Classification Models,” primary preprint. ([arXiv][9])
* 🏛️ Dashtaki et al., “A Multisource Fusion Framework for Cryptocurrency Price Movement Prediction,” primary preprint. ([arXiv][10])
* 🏛️ Kumar et al., “CryptoPulse: Short-Term Cryptocurrency Forecasting with Dual-Prediction and Cross-Correlated Market Indicators,” IEEE BigData/arXiv. ([arXiv][11])
* 🏛️ Alonso-Monsalve et al. (2020), “Convolution on neural networks for high-frequency trend prediction of cryptocurrency exchange rates using technical indicators,” *Expert Systems with Applications*. ([ScienceDirect][12])
* 🏛️ Fang et al., “Cryptocurrency Trading: A Comprehensive Survey.” ([arXiv][13])

[1]: https://developers.binance.com/docs/binance-spot-api-docs/web-socket-streams "Spot WebSocket Market Streams | Binance Developer Docs"
[2]: https://docs.discord.com/developers/resources/webhook "Webhook Resource - Documentation - Discord"
[3]: https://www.jstor.org/stable/4615733 "A Simple Sequentially Rejective Multiple Test Procedure | JSTOR"
[4]: https://projecteuclid.org/journals/annals-of-statistics/volume-17/issue-3/The-Jackknife-and-the-Bootstrap-for-General-Stationary-Observations/10.1214/aos/1176347265.full "projecteuclid.org"
[5]: https://developers.binance.com/docs/derivatives/usds-margined-futures/market-data/rest-api/Get-Funding-Rate-History "Market Data - Futures (USDⓈ-M) REST API | Binance Developer Docs"
[6]: https://github.com/binance/binance-public-data "GitHub - binance/binance-public-data: Details on how to get Binance public data · GitHub"
[7]: https://www.sciencedirect.com/science/article/pii/S2405918822000174 "Machine learning for cryptocurrency market prediction and trading - ScienceDirect"
[8]: https://www.sciencedirect.com/science/article/pii/S2405918821000027 "Short-term bitcoin market prediction via machine learning - ScienceDirect"
[9]: https://arxiv.org/pdf/2410.06935 "Predicting Market Trends with Enhanced Technical Indicator Integration and Classification Models"
[10]: https://arxiv.org/html/2409.18895v2 "A Multisource Fusion Framework for Cryptocurrency Price Movement Prediction"
[11]: https://arxiv.org/abs/2502.19349 "[2502.19349] CryptoPulse: Short-Term Cryptocurrency Forecasting with Dual-Prediction and Cross-Correlated Market Indicators"
[12]: https://www.sciencedirect.com/science/article/abs/pii/S0957417420300750 "Convolution on neural networks for high-frequency trend prediction of cryptocurrency exchange rates using technical indicators - ScienceDirect"
[13]: https://arxiv.org/pdf/2003.11352 "Cryptocurrency Trading: A Comprehensive Survey"
[14]: https://faculty.up.edu.pe/en/publications/technical-analysis-in-cryptocurrency-markets-do-transaction-costs/ "
        Technical analysis in cryptocurrency markets: Do transaction costs and bubbles matter?
      \-  FacultyUP — Universidad del Pacífico"
[15]: https://biblio.ugent.be/publication/01HY3C3S169G1N6QNYR55NZMFB "Are simple technical trading rules profitable in bitcoin markets?"
