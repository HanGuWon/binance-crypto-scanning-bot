# Pump-fade v2b implementation and evidence matrix

**Date:** 2026-10-07 KST. **Local implementation status:** substantial isolated engineering components built and targeted-tested; **standalone live shadow runtime, end-to-end pump-specific REST scheduler and OCI deployment are not complete**. **Empirical trading strategy status:** DATA_INCOMPLETE / NO POSITIVE EXPECTANCY CONFIRMATION. Historical proxy execution in this session was blocked after a Python editable-path mismatch. The project makes **no** profitability promise or trading recommendation.

## Exact source and frozen identity

- Parent repository: `D:\Binance bot-2`; observed clean starting source `fix/review-session-c`, `c06e4ced3005bb8b28f3a75d94ffd25cbfa0c3fd`. Branch for isolated implementation: `codex/pump-fade-v2-20261007`; worktree `D:\Binance bot-2\.worktrees\pump-fade-v2-20261007`. Uncommitted changes are local to this worktree.
- Original referenced private plan: `D:\Binance-trade-analysis\handoff\STRATEGY_PLAN_v2_2026-10-07.md` outside connector-approved roots. Prompt-quoted SHA `bb0504050b157c0456fe309731a4ea2954cbbb9a1d2e4773d21edbbab522e986` cannot be verified. Its roadmap/Oct7 continuation, private order accounting and OCI closeout receipts are also inaccessible. The **user-supplied identical shared contract** is the current accessible authority; source conflicts and limits are in `BASELINE_AND_CONFLICTS.md`.
- Frozen policy A for history: `config/pump_fade_v2/policy_v2.json` SHA256 `0d94fef1a3ad866dfedbc3dc80bb73e52f66adeac54b134bfff7d96d7341f806`. A before-historical-outcomes amendment B is active: `config/pump_fade_v2/policy_v2b.json` SHA256 `207fe5fd1e5c97155c7c52dae425918851ade771e40211b1619ae2428f5fe27d`; frozen 2026-10-07 03:49:36 UTC by `freeze_manifest_v2b.json`. Base preregistration hash `075f5d4655dc6ccd61a0e7ae57aab19d0f0b46d4a39efdc451395d7935c12c8e`; amendment B hash `b72c1633817050aafe6f9fbf023fa94d369dfe8039d2afa06194fd1f07410002`. All experiment families use deterministic bootstrap seed 20261007.
- Source/config environment SHA package: `docs/pump_fade_v2/release/release_manifest.json`, **successfully generated** by `src/signalbot/pump_fade_v2/release.py` with `deployment_enabled=false`. The composite source/config tree SHA256 is `016a25b5de7004885196e226ef57d3f70588209e647dd8aac60abdf3da98a8ec`; release manifest SHA256 is `0c854e71935a4e912b327e7e9a5debf031cec71bfe779d73047a1db1b41efa61`; synthetic preview SHA256 `17a08da1d33dfad5f23d9d11177fe5469e8b4cafeae18a8c8b19105f64e6eddc`. The existing unchanged `uv.lock` SHA256 is `ba19a6092d7cfa03a8299edafaaf0d959effb90099968778f8929793eb6bee0f`. No OCI deployed release SHA has been inspected.

## Common requirement-to-file/test/evidence matrix

| Contract | Files and relevant test | Actual local implementation | Empirical evidence / promotion |
|---|---|---|---|
| Source baseline, remediation continuity, private-data boundary | `BASELINE_AND_CONFLICTS.md`; clean parent branch/HEAD and existing `capture`, `prospective`, `backtest` owners inspected | **PARTIAL** — parent source reconciled; private plan/OCI receipts inaccessible | **UNAVAILABLE** original plan hash, fee/P3 fill semantics, current OCI source |
| Frozen event, state, overrides and SKIP cells | `policy_v2b.json`, `PREREGISTRATION_V2.md`, `PREREGISTRATION_AMENDMENT_B.md`, two freeze manifests; `state.py`; `test_pump_fade_v2.py` | **IMPLEMENTED as pure local state core**, with closed-event 24h >=30% + quote >=$2m + 60m cooldown, observed risk, missing evidence WAIT_UNAVAILABLE, joint SKIP and expiry | **UNAVAILABLE** PIT historical outcome validation |
| Five causal releases and continuation risk | `state.py`; closed15m R2, 60m peak R1, lagged OI R3, event AVWAP R4, observed funding cap normalization R5 and liquidation BUY overlay tests | **IMPLEMENTED as research decision rules**; no LONG auto recommendation; R2 pivot simplification remains preregistered research default | **UNAVAILABLE** actual predictive association, returns, alpha |
| Public feeds, censor and metadata | `public_capture.py`, additive allowlist `capture/websocket.py`, reused existing `capture/rest.py`, `capture/pipeline.py`; tests cover route and BUY-to-short-liquidation | **PARTIAL** — bounded keyless capture/poll plans, parsers, actual adjusted cap/floor metadata, deterministic IDs and existing adapter integration seam; no independent production subscription/poll ownership | **UNAVAILABLE** new v2 historical raw coverage, live reconnect and cap-change data |
| Isolated durable shadow and samples | `shadow.py`; `ShadowEpisodeEngine`, `ShadowPreviewStore`; restart/conflict/capacity/transaction tests; generated `sample_shadow_payload.json` | **IMPLEMENTED as local preview-only atomic SQLite**; 128 episodes and 50k preview cap; no transport/send method | **UNAVAILABLE** authorized live channel observation; actual OCI shadow service absent |
| Manual bounded ladder | `ladder.py`; tests for adverse short stop, frozen q0/I/R, partial fills, two additions, state block, conservative 30bps | **IMPLEMENTED as manual adapter**; q0 initial price risk applies once, fills aggregate by tranche; no production Guardian edit | **UNAVAILABLE** P3 private fill-counter to addition reconciliation; 3-add sensitivity not run |
| Manual LIQ risk and mark | `ladder.py`, `labels.py` and tests | **IMPLEMENTED** UNKNOWN on missing/stale supplied price and side-aware scenarios; no exact leverage formula | **UNAVAILABLE** user account liquidation accuracy and 4h adverse-path calibration |
| Versioned outcome labeling | `labels.py`; 4/24h event and alert origins, mark squeeze/MAE/MFE, 14/20/25/33 near-LIQ scenarios, triple-barrier; pending/completed SQLite tests | **IMPLEMENTED as reusable labeler**; executable quote+verified funding required for scenario net, mark is risk path only | **UNAVAILABLE** real public mark/BBO coverage, real cash flow and funding settlement completeness |
| R1 same-parent comparative replay, R2/R3/R4 arms | `replay.py`; synthetic gzip+SHA replay test; `OFFLINE_RESULTS_V2.md`, `offline_results_v2.json` | **PARTIAL** — supports exposed kline proxy R1/limited R3 landmark and day-block bootstrap/Holm helper; full P2/P3 execution, R2 counterfactual q99 and R4 LONG refutation require missing data | **DATA_INCOMPLETE**; no historical runner output, zero claims of net expectancy |
| Statistics and offline gates | `policy_v2b.json`, prereg, `replay.py` day clustering and Holm, synthetic tests | **IMPLEMENTED statistical utilities and registrations**, 10,000 target replicates, parent-day pairing, R1/R2/R3 thresholds frozen | **UNAVAILABLE** observed p-values, power, q99, CI and gate pass/fail |
| Forward holdout and statistical promotion | `forward_evaluation_v2b.json`, `FORWARD_EVALUATION_RUNBOOK.md` | **PREPARED, DISABLED**; start max(freeze, 2026-10-08, readiness, offline decision), 8wk and 150 valid parents, plus clustered power and lower-confidence-bound criteria | **NOT STARTED**, no future alerts or confirmation |
| OCI release canary/rollback | `release.py`, `OCI_RELEASE_RUNBOOK.md`, hash manifest and synthetic payload; test `test_release_manifest...` | **LOCAL PACKAGE PREPARED**; no production source/timer/Discord changes; separate DB/cgroup file-cache plan | **NOT DEPLOYED** and needs fresh OCI receipts, integration adapter completion, human authorization |

## Tests and checks

Validated Windows environment: locked `D:\Binance bot-2\.venv-codex`, `Python 3.12.13`, `uv 0.11.22`. Use `UV_PROJECT_ENVIRONMENT='D:\Binance bot-2\.venv-codex'` with `uv run --frozen --no-sync` to avoid the preexisting parent worktree's Linux virtualenv or dependency sync. Repo `uv.lock` was not altered. Command results are recorded after completion; **do not infer success from an in-progress command**.

| Exact validation | Result |
|---|---|
| `uv run --frozen --no-sync ruff check .` | PASS, raw exit 0 |
| `uv run --frozen --no-sync pyright` | PASS, 0 errors / 0 warnings, raw exit 0 |
| `uv run --frozen --no-sync pytest -q tests/unit/test_pump_fade_v2.py` | PASS, 15 tests at latest focused run |
| `uv run --frozen --no-sync pytest -q` | PENDING full suite at report draft |
| `.venv-codex\Scripts\python.exe -m compileall -q src tests` | PASS, raw exit 0 |
| `python -m signalbot.pump_fade_v2.release --repo-root . --output-dir docs/pump_fade_v2/release` with frozen UV env and worktree PYTHONPATH | PASS, raw exit 0, local disabled manifest and synthetic preview generated |
| `git diff --check` | Last checked PASS; recheck final state |

## Correctness/evidence constraints discovered during independent audit

- Same-candle peak/high and low have unknown ordering; new-high event resets trough to its observed close. Incomplete event-to-current 5m history blocks release, as do future/aged BBO and missing metadata. Stale wide spread cannot trigger false SKIP or false eligibility.
- A funding cap-hit can clear only after three spaced observed normalized predicted rates; a shortened observed funding interval requires a later restored observed interval. Otherwise R5 was logically impossible inside event TTL; amendment B captured this before label access.
- R3 skip membership is established **at four hours** and squeezes are evaluated only in the subsequent 20h; selecting skipped events from their full 24h outcomes would be leakage.
- Forced liquidation `!forceOrder@arr` is a censored one-per-symbol-one-second sample. Forced BUY means closing a short. Neither silence nor an L/S ratio is proof of liquidation exhaustion or future price direction.
- Fills and executed additions are distinct: several actual fills can belong to one addition; later final size must not be retroactively charged against earlier fills. Budget R is a configured stop-distance/cost allowance, not a gap-proof maximum realized loss.
- This local preview engine is isolated from the real Discord outbox. Local atomic SQLite preview tests do not certify live delivery/retries under OCI conditions; the package intentionally has no authorized delivery transport.

## Remaining data, implementation and authorization dependencies

1. **Blocked by roots:** verify original private plan/supporting documents and true SHA, P3 fee/side/fill accounting, historical outcome exposure provenance, 2026-10-07 OCI closeout and current deployed source; avoid copying private records to a public repository.
2. **Blocked by host operation:** successful historical v2 runner invocation from correct worktree import path; no data examined or outcomes measured. Source files' mere presence proves no eligible sample. See `OFFLINE_RESULTS_V2.md` for reproducible command and machine-readable unavailable statuses.
3. **Engineering work remaining:** an opt-in pump-specific owned WS reconnect/REST scheduler, durable raw-to-decision input bridge, proper historical OI/publication lag/metadata coverage, terminal event archival and complete P2/P3/R2/R4 replay adapters. Complete these and run causal operator canary before declaring an OCI-run-ready collector.
4. **Human future action:** authorize a **specific** isolated OCI shadow canary after source conflict resolution and engineering audit. Additional approval required for Discord webhook delivery or any production policy/wording changes. None occurred here.

**Next exact action:** review `docs/pump_fade_v2/release/release_manifest.json` and file SHAs against frozen config, resolve the inaccessible v2 plan and data chronology, complete the opt-in research transport owner and source-specific adapters, then rerun full validation and decide whether a separately authorized shadow canary is justified. A negative/null empirical result remains a valid outcome; a positive retrospective point estimate is never forward confirmation.
