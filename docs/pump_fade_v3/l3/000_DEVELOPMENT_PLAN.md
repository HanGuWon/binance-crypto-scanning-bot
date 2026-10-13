# Pump Fade v3 L3 — Development Replay Plan

Status: **planning and Phase 0 documentation only** (2026-10-09). This document is a reviewable continuation contract for the development-only L3 study. It is subordinate to the immutable L1 policy and preregistration; it does not amend strategy rules, authorize production order execution, or claim an implementation, validation, or efficacy result.

## Objective, boundary, and authority

Implement a causal, gap-aware L3 replay over the authorized development data, preserve observable original parents and arm-specific availability, and report engineering and statistical evidence without overstating incomplete coverage. The separate research package and generated study artifacts remain authoritative for executable code and measurements. This repository document publishes the decisions, evidence map, limitations, and handoff contract for human review.

Development interval is UTC `[2025-01-01T00:00:00Z, 2026-06-30T23:59:59.999Z]`, represented by integer milliseconds `[1735689600000, 1782864000000)`.

Frozen authority identities verified for this plan:

| Authority | Identity |
|---|---|
| L1 freeze commit | `3fe2481ea07a4b38e2768d647747e75bfe77bdbe` |
| `config/pump_fade_v3/freeze_manifest_v3.json` | SHA-256 `b928a9b280c3828e9affdde00b6adb97b63cfb239f6dfebd03945cdbf5730b26` |
| `config/pump_fade_v3/policy_v3.json` | SHA-256 `8f01bbc7d628d144fe13ce966b9ba6456795fc905d819ff2b476ec192a0a8f56` |
| `docs/pump_fade_v3/PREREGISTRATION_V3.md` | SHA-256 `39b26304b574292c287a23ce02f4b35b947a4a4a0e441c178559dbe92c4aef7f` |
| `src/signalbot/pump_fade_v3/holdout_guard.py` | SHA-256 `f4944a4402736bf1a5932436e87739efe89d4450968b9df8787824bee1401064` |
| External `L1_TRUSTED_FREEZE.json` | SHA-256 `1e85c566abf0cd5e5c7ec8c05685b371fb01932610197b21f96446ab694a6db6` |
| External `data_manifest.json` | SHA-256 `52b441e3449f4f067be13df4c1d00d8131c0ad4cd5664ae0b4e83a032c8db7a1` |
| External `data/development/source_plan.json` | SHA-256 `0afcbb6209b96d9a0fe97d2227d866c00d12a32de93c7289a0a6cf8293589054` |
| External `data/development/download_index.jsonl` | SHA-256 `41d62bd0108ee2fa8d4a7b319347029fedd27cae82edad516dd4c19fe589d332` |

The L1 commit and four bot-worktree file hashes, plus the external trusted-pin, manifest, source-plan and index hashes were recomputed at replan and matched these values. No ZIP, outcome data, or sealed/holdout path was opened. The preregistration header still says “L1 draft, precommit”; this stale status label does not override the matching committed freeze and byte pins. The pinned document remains unchanged.

Non-goals: no L1/L2 edit; no holdout path access, token, unseal claim or holdout result; no L3b/L4/L5; no bot runtime or production dependency change; no private-account/API key, OCI, Discord, testnet, order, staging, commit, push, PR or deployment activity. Later Git finalization belongs to Sonnet. No production order placement is part of this alert-first project.

## Frozen data and decision contracts

The L2 manifest uses schema `pump_v3_data_manifest_v1`, status `VALIDATED_WITH_RECORDED_GAPS`, and `development_only=true`. `VALIDATED` and `VALIDATED_WITH_RECORDED_GAPS` remain distinct admissible states; neither guarantees every time interval is usable. The recorded L2 inventory has 312,805 unique artifact keys across 764 symbols: metrics 274,235; funding 9,219; contract 10,043; mark 10,040; premium 9,268. The preflight reports 9,635 ambiguous metrics files affecting 478 symbols, 90 cross-file continuity violations in premium, internal adjacent-step violations (metrics 47,911; contract 9; mark 766; premium 1,192), and source-order reversals in 29,013 files totaling 1,190,269 adjacent inversions. These are input-quality counts, not L3 parent-loss or outcome counts; exact ranges and analytical impact must be measured before conclusions.

Admission is development-only and versioned. Validate schema/status, authority identities, receipts, bounds, unique artifact keys, path containment and bytes before consuming rows. Reject forbidden roots/windows before resolving, listing, statting or opening those targets. Verify each artifact digest before row consumption; keep checkpoints bounded, atomic and identity-bound. The supported execution surface is an **E1 development-only CLI admission guard**: lower-level parser calls or another process/code version can bypass it; no OS-level holdout ACL exists. Do not describe it as universal enforcement.

Temporal/data rules:

- Use only fully closed candles and causal as-of reads. Preserve source timestamp, normalized effective period end and usable-at timestamp independently. Apply the frozen total 600,000 ms availability lag exactly once to the normalized period end. A consumed value must be available by the decision cutoff; future-row perturbation must not alter earlier decisions.
- Reconstruct family-specific contiguous intervals from rows. A five-minute transition is contiguous only at exact adjacency. Never bridge a gap, interpolate, fill with zero, synthesize OHLC, borrow a future value, carry stale metrics past the frozen limit, or replace a missing immediate next open with a later open.
- Unknown/ambiguous metrics conventions fail closed. Exclude ambiguous metrics files from all metrics-derived fields together (OI, account/position long-short ratios, global long-short and taker ratios) and record exact source intervals. Reject conflicting duplicate timestamps; identical-row deduplication is permitted only with deterministic counted handling.
- Premium gaps affect premium-dependent calculations, not unrelated price/volume observability. Preserve every observable parent and prior decision; an affected future outcome is `CENSORED`/`UNAVAILABLE` with dependency and reason, never a zero or retrospective deletion of the parent. Account for intervals where parent detection itself is unobservable separately.
- Missing shared OI continuation-risk evidence makes the full policy decision unavailable for R1/R2/R4; a named price-only calculation is only a non-actionable descriptive ablation. Unavailable runtime gates do not silently clear a full-policy gate or erase otherwise measurable parents.
- The first archived contract candle is only a first-available-data proxy, not a listing/onboarding timestamp. R5 and historical funding-cap/interval chronology are `UNTESTABLE`; realized settlements price only actual observed arm holdings. Missing liquidation samples, account liquidation price, BBO/depth remain unknown, not zero.

Frozen rule crosswalk: parent candidates use a 24-hour closed-5m return `>=30%`, 288-bar trailing quote volume `>=2,000,000 USDT`, 60-minute cooldown and 24-hour expiry. WAIT A is closed 4h return `>15%`; B is closed 1h return `>5%`; D is latest fully closed 1h quote volume `>=2x` the preceding 24 complete non-overlapping hourly mean; D12 is a separately labeled prior-12h median sensitivity. R1 is 60m without a higher closed 5m high and resets on a new high. R2 is the frozen closed-15m breakdown/retest rule. R3 uses observable event baseline, one-row lag, `>=5%` OI decline from observable peak and maximum 15-minute staleness. R4 is a closed-15m close below AVWAP anchored at the first fully closed post-event 5m bar. R5 remains untestable. SKIP measures all nine joint drawdown `{20,30,40}%` × rebound `{5,10,15}%` cells, plus a distinct joint `{25%,8%}` and unconditional 40% drawdown sensitivity. No result-selected release combinations.

## Frozen ladder/risk and pre-outcome gates

The primary ladder uses `k=0.15` (sensitivities `0.10/0.25`), frozen risk budget `R=1%` of initial parent notional, first-only, primary two-add, three-add sensitivity, and legacy eight-add comparison. The frozen invalidation is at least 15% above observed mark high. Entry proxy is the immediate next 5m open after a closed decision; slippage uses only the preceding fully closed 5m range. Apply 15/30/60 bp round-trip costs consistently to filled/remaining notional, and charge funding only to each arm’s actual holdings at observed settlements. Do not share funding cash flows across different exit paths.

The literal budget check is `sum(q_i * (abs(I - P_i) + 0.003 * P_i)) <= R`, with explicit quantity, price, notional and denominator units. Before any outcome replay, trace `initial parent notional`, `q0`, fill price, invalidation and observed mark high from frozen binding text. At `P=100`, `mark_high=100`, `I=115`, `q=1`, per-unit risk is `15.3` notional units. Generic checks: budget `16` is feasible; `15.3` is exact equality; `15.2` is infeasible. If `N0=q0*P0=100`, `q0=1` gives `R=1`, so the initial fill is infeasible (`15.3>1`). Do not retune `k`, redefine/shrink the denominator, or skip the initial-fill check. If the frozen equation permits no fills, complete state/coverage and report trading metrics as NA.

Resolve and fixture-test these before inspecting outcome values: (1) R3 event-baseline compatibility with the total 600,000 ms lag and first-event 5m bound; (2) exact admission receipt/path preflight contract; (3) interval endpoint representation; (4) all risk-equation units and `q0`/initial-notional denominator; (5) paired-bootstrap estimand/statistic and frozen Holm family membership; (6) duplicate parent natural key `(run_identity_id,symbol,event_close_ms,event_kind)` and digest fixture. A true frozen contradiction is recorded and escalated, not silently tuned. Preserve empty, unknown, unavailable and censored cells.

## Dependency-ordered work phases and external artifacts

All research source, tests, data and generated outputs are under the external research project root `D:\Binance-trade-analysis\2026-10-08_pump-v3`; run commands from that exact cwd. Never copy raw research data into this bot repository.

| Phase | Main external source/test/output scope | Completion evidence and public record |
|---|---|---|
| 1 — baseline and feasibility | Modify `src/pump_v3_replay/data.py`, `src/pump_v3_replay/models.py`; add `tests/test_l3_feasibility.py`; create `l3/FEASIBILITY_AUDIT.md` and `l3/CURRENT_STATUS.md`. Fix six Ruff and 17 strict Pyright baseline findings without ignores or weakened runtime validation; preserve L2 behavior; evaluate exact risk equation with Decimal positive/equality/negative/q0 cases before outcomes. | Raw full Ruff, Pyright, pytest, compileall and focused feasibility results/logs; source/test before/after inventory and hashes/diffs; risk disposition; external document hashes. Only then create bot record `docs/pump_fade_v3/l3/010_BASELINE_FEASIBILITY_RECORD.md`. |
| 2 — admission and coverage | Add `src/pump_v3_replay/admission.py`, `coverage.py`, `parents.py`, `checkpoint.py`, `coverage_cli.py`; modify `data.py`, `models.py`; add admission/coverage/causality/resume fixture tests. Create `l3/ADMISSION_PROTOCOL.md`, `admission_manifest.json`, coverage-only `COVERAGE_REPORT.md`, `interval_exclusions.csv`, `parent_coverage.csv`, `dependency_coverage.csv`. | Run synthetic smoke before development scan; review protocol/identity before any archive-row scan. Then run development-only coverage command below. Save source/test/config inventory/digests, commands/logs, unchanged L1/L2 identities, run identity and output hashes, exact affected ranges and measured parent/dependency availability. No outcomes. Only then create `020_ADMISSION_COVERAGE_RECORD.md`. |
| 3 — replay and study | Add causal/replay/rule/ladder/metrics/bootstrap/report/CLI modules and tests; modify the coverage report only to append measured outcome/censor reconciliation. Create deterministic parent decisions, arm outcomes and SKIP cells, `RESULTS_L3.md`, `RUN_MANIFEST.json`, `NEXT_HANDOFF.md`. | Run fixture smoke first; settle every pre-outcome gate before outcome access; then execute one bounded identity-bound development replay. Preserve original-parent denominators, paired samples, NA/censored/empty cells and power limits; independently review admission, causality, denominators, risk, funding, Holm/bootstrap and resume. Only then create `030_REPLAY_RESULTS_RECORD.md`. |

### Exact planned commands and verifier scope

Phase 1 from the external project cwd: `uv run ruff check .`; `uv run pyright`; `uv run pytest -q`; `python -m compileall -q src tests`; focused `uv run pytest -q tests/test_l3_feasibility.py`. These are repository-wide gates over the external research package; baseline evidence recorded at planning is Ruff exit 1 (six findings in `data.py`), Pyright exit 1 (17 strict diagnostics in `data.py`/`models.py`), pytest exit 0 (11 passed), and compileall exit 0. Those baseline failures remain pending; they are not Phase 0 failures or passes. Final L3 requires every registered full research gate to pass with full logs and exit codes.

Phase 2 synthetic smoke from external project cwd:

```powershell
uv run python -c "import sys; sys.path.insert(0, 'src'); from pump_v3_replay.coverage_cli import main; raise SystemExit(main())" smoke --output-dir l3/coverage-smoke --checkpoint-dir l3/coverage-smoke/checkpoints
```

Only after protocol and admission identity are reviewed, the Phase 2 development coverage run is:

```powershell
uv run python -c "import sys; sys.path.insert(0, 'src'); from pump_v3_replay.coverage_cli import main; raise SystemExit(main())" run --manifest data_manifest.json --source-plan data/development/source_plan.json --index data/development/download_index.jsonl --data-root data/development --start-ms 1735689600000 --end-exclusive-ms 1782864000000 --output-dir l3 --checkpoint-dir l3/coverage-checkpoints
```

Phase 3 fixture smoke from external project cwd:

```powershell
uv run python -c "import sys; sys.path.insert(0, 'src'); from pump_v3_replay.cli import main; raise SystemExit(main())" smoke --fixture-dir tests/fixtures/l3 --output-dir l3/smoke --checkpoint-dir l3/smoke/checkpoints
```

After Phase 2 identity/coverage and all pre-outcome definitions are frozen, the one full L3 run is:

```powershell
uv run python -c "import sys; sys.path.insert(0, 'src'); from pump_v3_replay.cli import main; raise SystemExit(main())" run --manifest data_manifest.json --source-plan data/development/source_plan.json --index data/development/download_index.jsonl --data-root data/development --start-ms 1735689600000 --end-exclusive-ms 1782864000000 --output-dir l3 --checkpoint-dir l3/checkpoints --resume
```

These module/CLI invocations are future acceptance commands; Phase 2/3 CLIs do not exist yet. Every new gate must be run from the exact external project cwd and its output, exit status, durable log and target observation scope must be recorded. Phase 0 checks only the public document path, substantive contents, whitespace, hash and unignored Git visibility plus unchanged frozen identities; it does not run or claim research gates.

## Outcomes, denominators, and statistical reporting

Phase 3 keeps original-parent accounting separate from eligible available-outcome estimates. Preserve observable WAIT, SKIP, never-released, expired, nonfill and censored parents. Report dependency availability and parent loss by reason; pairwise jointly observable samples; 4h squeeze (`>=15%` from registered origin), 24h results, MAE, net bp, positive-loss q99, missed fade and nonfill. Account liquidation proximity remains UNKNOWN without account evidence; hypothetical distance stresses are separate.

Compare first-only, primary two-add, three-add sensitivity and legacy eight-add under the same frozen budget/invalidation. Apply every applicable risk/confirmation/WAIT/SKIP/continuation gate to initial fills and adds. Use per-arm actual fills, exits, costs and observed funding. Same-bar ordering uncertainty is explicit; missing next open is not replaced. Report truly observed no-trade zero P&L separately from unknown P&L.

Statistics use 10,000 paired original UTC parent-day draws, seed `20261007`, with all symbols resampled together; preserve overlapping-event dependence and disclose its limitation. Apply Holm only within the frozen families determined before outcomes. Keep empty cells, insufficient tail/cluster power, NA and UNAVAILABLE/CENSORED explicit; none is a loss, zero, pass or failed rule. A clean-coverage sensitivity is predeclared and cannot be selected based on returns. Never present a clean-survivor estimate as full-universe expectancy.

## Review history, open gates, and interpretation

The first roadmap A review had three accepted High findings (diff-level interfaces, exact CLI/output locations, complete run identity), all folded into the plan. Round 2 found a wrong run-identity test-vector digest; it was independently recomputed and corrected. Round 3 passed that prior roadmap revision. That revision’s only B artifacts were ignored under `/devlog/`; the supported B→C attempt correctly refused to advance, and Phase 0 did not reach C/D. The subsequent user-authorized replan added this substantive nonignored public layer without changing `.gitignore` or L1/L2. The same architect reflected the amendment as ALIGNED; a fresh independent amended-plan A review returned PASS with no blockers. These are planning-review results only, not implementation or efficacy evidence.

Scientific gates still open for the owning phase: R3 baseline/lag interpretation; receipt and path contract; interval endpoint representation; frozen risk denominator and `q0`; bootstrap estimand/statistic and Holm families; parent-key fixture; and measured row-level gap ranges/impact. If any cannot be settled from frozen authority plus synthetic cases, stop before outcome access and report the contradiction. Engineering acceptance, research reproducibility and strategy efficacy are separate judgments.

## Reviewable handoff map

External authoritative L3 deliverables, all under `D:\Binance-trade-analysis\2026-10-08_pump-v3\l3\`, are `ADMISSION_PROTOCOL.md`, `admission_manifest.json`, `COVERAGE_REPORT.md`, `interval_exclusions.csv`, `parent_coverage.csv`, `dependency_coverage.csv`, deterministic parent/decision/outcome tables and `skip_cells.csv`, `FEASIBILITY_AUDIT.md`, `CURRENT_STATUS.md`, `RESULTS_L3.md`, `RUN_MANIFEST.json`, and `NEXT_HANDOFF.md`, together with the external `src/pump_v3_replay/` code and tests. The separate bot review records are created phase-by-phase only after their underlying evidence exists:

- `docs/pump_fade_v3/l3/010_BASELINE_FEASIBILITY_RECORD.md`
- `docs/pump_fade_v3/l3/020_ADMISSION_COVERAGE_RECORD.md`
- `docs/pump_fade_v3/l3/030_REPLAY_RESULTS_RECORD.md`

L3b is not started here. Its next preparation should compare all-entry E0 versus runner fairly, include a separately labeled common-policy landmark study, measure arm-scoped feature coverage, and explicitly account for unavailable forced-liquidation/funding inputs. L4/L5, holdout, OCI, Discord, account and testnet work remain outside this authorization.

## Local plan-pack provenance at publication

The five finalized local numbered plan documents below were SHA-256 hashed immediately before this public document was created in Phase 0 B. They are local ignored working records; their material decisions are summarized above so this review surface does not depend on access to them.

| Local artifact | SHA-256 |
|---|---|
| `devlog/_plan/261009_l3_gap_aware/000_plan.md` | `4f389abfc909777bb85aa0b1040870cf49b6ba1f15739e5c39041d0baeedbc4f` |
| `devlog/_plan/261009_l3_gap_aware/005_plan_audit.md` | `ed9475605cb8e88f14ce97f0399146aeb28f269094b5a0ad2656ecbe1c3c8f30` |
| `devlog/_plan/261009_l3_gap_aware/010_baseline_feasibility.md` | `c4ca443370a353eab1d12502586068821db658fe59415f845ef81cb18973d3dd` |
| `devlog/_plan/261009_l3_gap_aware/020_admission_coverage.md` | `214f42d6354292b70332f5ad5823a04801b0f06e1eb68121a428c41411670254` |
| `devlog/_plan/261009_l3_gap_aware/030_replay_study.md` | `714690da974aa43ac680bb7987bdb10f5373c32da3352dfd495353795bad4d4e` |
