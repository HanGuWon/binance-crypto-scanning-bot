[🟢 Online Mode | 26.07.17_12:59:31]

# 1. Verdict

**RETIRE C1 UNCHANGED.**

The scientifically correct result is:

> **ZERO-TRADE / INCONCLUSIVE LOW INFORMATION — not profitable, not unprofitable, and not deployable.**

The included core implementation is causally coherent, its cost units are internally consistent, the short-funding sign is correct downstream, and I found no P0 code or statistical defect that mechanically forces every decision to reject. I also executed the exact attached `_entry_decision` logic on a synthetic high-edge case; it accepted the candidate, ruling out a hard-coded or unconditional rejection path.

The zero-trade output itself is strongly verified:

* Reconstructing the exact header-only `c1_trades.csv` from the frozen `CarryTrade` schema and writer produced SHA-256
  `42d745e520423127d5c30fb4c714b3c8e499a1516e67a99f05f8fe5860b54e2c`, exactly matching the manifest.
* Reconstructing the complete 3,896-row, all-zero daily panel produced SHA-256
  `937bd8f50cf48367da5edc01114f2fec134ac4ea9d3b5fe471654a385644d978`, also exactly matching the manifest.

Therefore, the recorded output really was a zero-row trade ledger—not a positive or negative backtest whose trades were omitted from the report.

The qualification is important: **the attachment is not sufficient for a clean-room replay of why all decisions rejected.** It omits the 19,709-row decision ledger, all 40 raw input files, two frozen data-ingestion modules, and the unit tests. The assertion that every decision failed the economic hurdle is internally consistent and plausible, but not independently row-recomputed from this bundle.

## Profit taxonomy

| Concept                   | Finding                                                                                                                                                                               |
| ------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Realized profit**       | **None.** There were no live orders or transactions, and therefore no realized trading-P&L observation.                                                                               |
| **Backtest profit**       | **None estimated.** There were zero completed simulated episodes. The reported USDT totals of `0` are empty-set sums; mean return, profit factor, and confidence bound are undefined. |
| **Efficacy conclusion**   | **Inconclusive due to zero trades.** The entry rule’s historical activity was zero on the processed sample; exit efficacy and funding-P&L efficacy were never exercised.              |
| **Governance conclusion** | **Frozen-screen failure.** Irrespective of statistical inconclusiveness, C1 failed its prespecified acceptance gates and must be retired without alteration.                          |
| **Execution conclusion**  | Historical next-open values are non-executable kline proxies. Even a positive result would not establish executable performance.                                                      |

The artifact itself correctly reports zero episodes with `NA` mean/PF and `INCONCLUSIVE_LOW_INFORMATION`, rather than claiming a 0% return. See [`c1_analysis.json`](sandbox:/mnt/data/c1_audit_work/artifacts/backtest/2026-07-17-c1/run/c1_analysis.json):493–567 and 790–795, and [`c1_report_ko.md`](sandbox:/mnt/data/c1_audit_work/artifacts/backtest/2026-07-17-c1/run/c1_report_ko.md):3–24.

---

# 2. Verification results

## Causal timing — passes on the included code

The entry chronology is point-in-time and does not use next-open prices to decide:

1. Funding events are selected over a half-open interval ending strictly before the current closed-candle timestamp. An event equal to the decision close is excluded until the following candle.
2. The triggering event is the latest newly settled event.
3. Basis history is exactly the preceding 8,640 common bars, excluding the current bar.
4. Acceptance is determined before the next-open prices are assigned to a position.
5. A successful decision fills at the immediately contiguous next common open.

Evidence:

* Strict funding observability helper: [`carry.py`](sandbox:/mnt/data/c1_audit_work/src/signalbot/backtest/carry.py):630–638.
* First post-settlement decision detection: `run_carry_pair`, lines 887–917.
* Next-open position construction only after acceptance: lines 918–929.
* Prior basis window excludes the current bar: lines 894–900.
* Split-start purge, split-end purge, cooldown, and causal next-open guard: `_entry_decision`, lines 533–547.

Exit sequencing is also causal:

* Exit predicates use the current fully closed common bar.
* Priority is `STOP → FUNDING_FLIP → CONVERGENCE → TIME`.
* Ordinary exits fill at the next contiguous common open.
* A same-open reversal is prevented by the cooldown because the prospective new entry time equals the just-recorded exit time and therefore fails the 24-hour test.

Evidence: `run_carry_pair`, lines 816–885.

The event-count arithmetic is independently consistent with one decision per funding settlement:

* The three splits contain 243, 245, and 242 days.
* At an 8-hour schedule that gives 729, 735, and 726 decisions for each of seven assets.
* At a 4-hour schedule WIF gives 1,458, 1,470, and normally 1,452 decisions. The retrospective count is 1,451 because of the exact documented missing 2026-06-24 04:00 UTC WIF settlement.
* These counts sum to exactly 19,709.

The frozen schedule is in [`carry_runner.py`](sandbox:/mnt/data/c1_audit_work/src/signalbot/backtest/carry_runner.py):66–75; the WIF exception is lines 57–65 and is supported by [`binance_wif_funding_2026-06-23_2026-06-25.json`](sandbox:/mnt/data/c1_audit_work/artifacts/source_snapshots/binance_wif_funding_2026-06-23_2026-06-25.json):13–22 and 36.

## Cost formulas — core calculation passes

The position uses equal base quantity and treats 100 USDT as total pair capital:

[
q=\frac{100}{S_{\text{entry}}+F_{\text{entry}}}.
]

That implementation is in `_close_trade`, [`carry.py`](sandbox:/mnt/data/c1_audit_work/src/signalbot/backtest/carry.py):660–679.

The execution helper:

* adversely moves both entry and exit prices,
* computes gross return from unslipped prices,
* computes slippage as the difference between gross and slipped execution return,
* charges the fee on slipped execution notionals,
* and returns execution net of fees.

See [`engine.py`](sandbox:/mnt/data/c1_audit_work/src/signalbot/backtest/engine.py):58–101.

`_close_trade` then computes:

[
\text{net}=\text{gross}-\text{slippage}-\text{fees}+\text{funding},
]

once and only once. There is no double subtraction. See `carry.py`:680–704.

Independent flat-price checks at zero basis produce:

| Cohort         |  Base round-trip cost | 2x-slippage cost |
| -------------- | --------------------: | ---------------: |
| Anchor / major | 23 bp of pair capital |            31 bp |
| Volatile       |                 33 bp |            51 bp |

Those values agree with the frozen entry hurdle in `_entry_decision`, `carry.py`:577–592. The bps-to-rate conversions and denominator (2+b) are dimensionally correct.

The attached audit reports a maximum expected edge of 14.7392 bp and minimum 2x cost hurdle of 30.9872 bp, so—assuming its omitted row-level recomputation is accurate—no decision reached even the cost hurdle before applying the additional 10 bp margin. See [`c1_independent_audit.json`](sandbox:/mnt/data/c1_audit_work/artifacts/backtest/2026-07-17-c1/run/c1_independent_audit.json):33–41. I could not independently reproduce those extrema without `c1_decisions.csv`.

## Funding sign and event inclusion — downstream code passes; end-to-end evidence is incomplete

The sign is correct in `calculate_funding_return`:

* Long direction has sign (+1), so positive funding contributes negatively.
* Short direction has sign (-1), so positive funding contributes positively.
* Negative funding therefore costs the short.

See `engine.py`:118–134.

For each included event, the implementation produces:

[
\text{short funding P&L}
= q \times \text{mark price} \times \text{funding rate}.
]

That is the correct directional cash-flow identity for the rule’s stated convention.

Event inclusion in `_close_trade` is:

[
[\text{entry time}+5\text{ minutes},\ \text{exit decision time}),
]

followed by the helper’s strict `entry < event < exit` check. Consequently:

* event at entry: excluded;
* event inside the first five minutes: excluded;
* event exactly at entry plus five minutes: included;
* event before exit decision: included;
* event exactly at exit decision: excluded.

See `carry.py`:690–703 and `engine.py`:129–134. Funding-flip state uses the same first-five-minute exclusion in `carry.py`:817–830.

This logic is consistent with the frozen plan’s conservative ordering convention. It cannot validate real settlement/fill ordering, but it does not claim to do so.

## Zero-trade classification — correct

`evaluate_c1_acceptance` returns `INCONCLUSIVE_LOW_INFORMATION` when completed episodes are below 100, no bootstrap replicate is valid, or the invalid fraction is too high. With zero pair capital in every day, every bootstrap denominator is zero, so 50,000 of 50,000 replicates are correctly invalid. See [`carry_runner.py`](sandbox:/mnt/data/c1_audit_work/src/signalbot/backtest/carry_runner.py):621–701 and 712–780.

This is the correct statistical classification. It does **not** mean the exact rule survives the candidate screen: the frozen gates failed, so the protocol-level action is retirement.

---

# 3. P0 / P1 / P2 findings

## P0 — none identified

I found no included-code defect that invalidates the recorded zero-row trade output or converts it into evidence of positive performance.

In particular:

* no look-ahead entry pricing was found;
* funding equality boundaries are handled causally;
* the entry predicate is not hard-wired to reject;
* cost units are not off by 100 or 10,000;
* the 100 USDT notional is not mistakenly applied to each leg;
* gross, slippage, fees, and funding are not double-counted;
* zero episodes do not produce a numerical mean, PF, or confidence bound.

## P1-1 — the empirical decision result is not clean-room reproducible from the attachment

The manifest names **40 input files** and five outputs, including `c1_decisions.csv`, `c1_trades.csv`, and `c1_daily_pair_pnl.csv`. See [`c1_run_manifest.json`](sandbox:/mnt/data/c1_audit_work/artifacts/backtest/2026-07-17-c1/run/c1_run_manifest.json):89–143.

The attachment omits:

* all 40 kline/funding inputs and kline manifests;
* `c1_decisions.csv`;
* `c1_trades.csv`;
* `c1_daily_pair_pnl.csv`;
* frozen `dataset.py`;
* frozen `funding.py`;
* both frozen C1 unit-test files.

The zero-row trade and zero daily outputs can be proven from their hashes, as described above. What cannot be independently verified is:

* each of the 19,709 decision inputs;
* each rejection-reason vector;
* the stated 500 decisions rejected only by the economic hurdle;
* maximum expected edge and minimum hurdle;
* whether raw funding rates entered the strategy with their original signs;
* whether price and timestamp parsing matched the claimed raw source.

This is an **audit-evidence deficiency**, not evidence that the zero-trade result is wrong.

## P1-2 — funding integrity is not recomputed by the ledger validator

`_close_trade` computes funding from source events correctly, but `CarryTrade` stores only aggregate funding P&L and an event count. It does not preserve the included event timestamps, rates, or mark prices. See `CarryTrade` in `carry.py`:297–332.

`_validate_carry_ledgers` independently recomputes gross P&L, slippage, and fees from the recorded prices. For funding it checks only that a zero event count cannot have nonzero funding, then verifies the algebraic net-P&L identity. It does **not** recompute funding from source rows or validate event boundaries, sign, rate, mark, or count. See `carry_runner.py`:429–476, especially 463–470.

Therefore, a future nonzero-trade ledger could contain an incorrect nonzero funding value and still receive the generated `data_integrity: PASS`, provided net P&L was adjusted consistently. The status is assigned at `carry_runner.py`:853–864.

This defect is dormant for this run because there are no trades or funding P&L. It cannot change the current zero-trade output, but it prevents the existing validator from certifying funding correctness in a nonzero run.

## P1-3 — freeze and run are internally consistent but not fully cryptographically bound

The pre-outcome freeze hashes the plan, spec, selected source files, two data modules, and two tests. See [`c1_preoutcome_freeze.json`](sandbox:/mnt/data/c1_audit_work/artifacts/backtest/2026-07-17-c1/c1_preoutcome_freeze.json):8–23.

For every frozen file that is present in the attachment, I recalculated an exact hash match:

* plan;
* spec;
* `carry.py`;
* `carry_runner.py`;
* `engine.py`.

The timestamps are also coherent:

* freeze: 2026-07-17 03:36:20.633 UTC;
* recorded run start: 03:36:47.983862 UTC, 27.350862 seconds later;
* inferred finish from duration: approximately 03:41:19.351869 UTC;
* independent audit: 03:42:38.678 UTC.

However:

* the freeze does not record the manifest’s aggregate source-code digest;
* input hashes first appear in the post-run manifest rather than the pre-outcome freeze;
* several runtime dependencies are not individually frozen;
* the omitted files prevent recomputing the aggregate source digest;
* `outcome_seen_before_freeze: false` remains a procedural attestation, not externally provable evidence.

Accordingly, “single no-retuning execution” is **consistent with the record but not cryptographically proven end to end**. Since the whole sample was already exposed, the artifact correctly avoids making a confirmatory claim.

## P2-1 — “decision funnel” is not a sequential funnel

`_entry_decision` appends every failed predicate to one decision, so a row can contribute to several reason counts. See `carry.py`:494–592.

`analyze_carry_ledgers` then calls `Counter.update` on every decision’s complete reason tuple. See `carry_runner.py`:845–847.

Therefore, the counts in `decision_funnel.rejection_reasons` are **overlapping marginal failure counts**, not stage-by-stage attrition. For example, the 17,440 basis-q90 failures and 16,311 nonpositive-basis failures cannot be subtracted sequentially.

The only direct inference from the reported counts is:

* 19,709 decisions;
* zero accepted;
* every decision carried the economic-hurdle rejection reason.

The claim that exactly 500 were rejected **only** by that reason comes from the omitted independent decision-ledger analysis and cannot be reconstructed from the published aggregate counts.

## P2-2 — empty-sample labels could be more explicit

`_profit_factor` returns state `NO_POSITIVE_RETURNS` both when returns exist but none are positive and when the input sequence is empty. See `carry_runner.py`:523–530.

For zero episodes, `NO_EPISODES` or `NOT_ESTIMABLE` would be more precise. Likewise:

* `outcome_unobservable_equals_zero` passes vacuously because no decision was accepted;
* the report displays net P&L `0.0000`, which is an empty sum, not an observed zero-return strategy.

The surrounding `NA` fields and `INCONCLUSIVE_LOW_INFORMATION` status prevent a substantive misclassification, so this is a reporting-semantic issue rather than a result error.

## P2-3 — “2x slippage” is not a literal full helper recomputation

The stressed result is calculated as:

```python
trade.net_pnl_usdt - trade.slippage_usdt
```

See `_stress_pnl`, `carry_runner.py`:519–520.

This exactly doubles the stored slippage dollars, but it leaves fees fixed at the amount calculated on base-slippage execution prices. The execution helper’s fee amount depends slightly on the slipped entry and exit prices; see `engine.py`:85–100. A literal “recompute at 2x slippage” would rerun both legs with doubled slippage and then recompute fees.

The difference is second-order and cannot affect this zero-trade result. It should nevertheless be corrected in any new protocol rather than silently carried forward.

## P2-4 — the frozen plan describes gap handling that the runner generally prevents

The plan permits internal common-bar gaps and specifies analysis-ineligible gap exits. See [`experiment_plan.md`](sandbox:/mnt/data/c1_audit_work/artifacts/backtest/2026-07-17-c1/experiment_plan.md):32–34 and 113–120.

The runner’s input coverage validator rejects any internal kline gap before replay. See `carry_runner.py`:139–170. Thus most documented gap-handling paths cannot occur in a normal C1 run.

All recorded replay diagnostics show zero gaps, so this inconsistency did not affect the result.

---

# 4. Can a code or statistical bug have made the zero-trade conclusion false?

**No demonstrated P0 bug does so.**

The strongest positive evidence is:

1. The trade-ledger hash is exactly the hash of a header-only file.
2. The daily-ledger hash is exactly the hash of 3,896 zero cells.
3. Decision scheduling counts match the frozen settlement schedules exactly.
4. The attached entry function can accept a synthetic qualifying candidate.
5. The cost formula and bps units are coherent.
6. The status logic correctly treats zero trades as undefined/inconclusive.

The residual false-zero pathways are evidentiary:

* an error or sign transformation in the omitted funding parser;
* incorrect raw funding or kline files;
* a decision-row calculation error that cannot be checked because `c1_decisions.csv` is absent;
* runtime dependency drift not excluded by the freeze record.

Accordingly, the defensible statement is:

> **The attachment strongly establishes that the recorded run output contained zero trades and that the included core code is capable of accepting trades. It does not fully establish, independently of the omitted data and decision ledger, that every historical rejection input was correct.**

Funding and exit-code defects cannot turn zero trades into hidden profit because those paths were never reached. They matter only to hypothetical trades that did not occur.

---

# 5. Must C1 be retired unchanged?

**Yes.**

The frozen plan explicitly states that failure retires the exact rules and does not authorize tuning on the exposed ledger. See `experiment_plan.md`:144–148.

Retirement means:

* preserve the exact code, config, hashes, and artifacts as an immutable research record;
* do not lower the economic hurdle, fee assumptions, slippage, funding requirements, q90 threshold, positivity fraction, margin, or purge periods;
* do not relabel the zero-trade output as zero return;
* do not use the exposed decision ledger to repair C1;
* do not promote it to a live, paper-execution, or production-order candidate.

Any altered rule is a new protocol—such as C2—with a new freeze, new version, and a genuinely post-freeze evaluation period.

---

# 6. Minimal prospective work scientifically justified next

A C1-specific efficacy trial is **not** justified. The plan’s six-month prospective clause was conditional on C1 passing; C1 did not pass.

The minimal justified work is infrastructure and audit capture for a separately frozen future hypothesis:

### Complete the reproducibility record first

Archive, under content hashes:

* `c1_decisions.csv`;
* all 40 manifest inputs and kline manifests, or immutable content-addressed access to them;
* `dataset.py` and `funding.py`;
* the two C1 test files;
* the independent-audit program, not only its JSON output;
* environment lock and the exact source-digest algorithm.

Add a funding-attribution ledger containing, for every simulated episode, each included event’s timestamp, original rate, mark price, inclusion/exclusion reason, and directional P&L. This addresses P1-2 without modifying C1’s result.

### Capture public microstructure data continuously and hypothesis-agnostically

For Spot and perpetual simultaneously, retain:

* public BBO with bid/ask prices and displayed quantities;
* sufficient public depth to calculate a conservative crossable quote for the fixed research notional;
* public trades or aggregate trades;
* exchange event timestamps, sequence identifiers, local monotonic receive timestamps, and clock-offset diagnostics;
* disconnects, sequence gaps, snapshot resets, and stale-quote flags;
* public funding settlement rows, mark price, index price, premium index, funding interval, caps/floors, and metadata changes.

Capture continuously rather than only when C1 would alert. Alert-conditioned capture would create missing counterfactuals and selection bias.

### For a future C2, freeze the alert ledger before its prospective test starts

Each alert should record:

* feature cutoff and last observable source event;
* decision generation timestamp;
* earliest public quote received strictly after the decision;
* quote age and both-leg simultaneity tolerance;
* bid/ask quantities available at the research notional;
* modeled fee and quote-crossing cost;
* data-quality state;
* deterministic alert ID and source/config hashes.

Any data seen before the C2 freeze is exposed development data. Only data collected after the C2 freeze may support prospective evaluation.

The future system must remain **public-data, alert-only**: no API keys, account state, balances, private endpoints, order submission, amendment, cancellation, or production execution. Public BBO observations can support an executable-price proxy; they must never be described as realized fills.
