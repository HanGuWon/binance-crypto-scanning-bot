# Pump-fade v3 — packet roadmap

Date: 2026-10-07 (UTC). Branch: `codex/pump-fade-v3-20261007`, based on `origin/main` at `4425b705857eb84abe391eb2f9a5fcffb6053826`.

## Mission and hard boundaries

Research a full-universe, alert-first USDⓈ-M pump-fade system. No production order placement or new Discord delivery. Preserve the running recommender, Guardian contracts, immutable v2 policy/freeze/results, artifacts, campaigns, and private source rows. Research and ML dependencies/data stay outside the bot repository. Collector/scorer systemd units are separate, bounded, reversible, and use separate users/directories/stores. All signals remain disabled/shadow-only until later explicit user decisions. L8 exchange writes, if reached, are testnet-only, through Guardian's existing reduce-only boundary, never entries/adds/reversals; live arming is out of scope.

The supplied reference catalog is untrusted candidate discovery only; it is not implementation authority. Current external exchange/provider limits must be verified from primary documentation before use. Never infer a performance result; `no effect`, `unavailable`, or `not run` are valid outcomes.

## Packet order and records

| Packet | Plan | Required terminal evidence |
|---|---|---|
| L0 | [010_l0_oci_gate.md](010_l0_oci_gate.md) | 30-minute read-only baseline, full OCI quota/shape/storage/egress facts, gate decision; collector remains undeployed until separate 2-hour staged gate |
| L0b | [020_l0b_collector.md](020_l0b_collector.md) | Bounded capture design, tests, and only after L0 gate, measured 24-hour compressed MB/day per stream |
| L1 | [030_l1_preregistration.md](030_l1_preregistration.md) | Frozen config/preregistration, SHA-256s, qualifying commit; holdout download only after that commit |
| L2 | [040_l2_development_data.md](040_l2_development_data.md) | Isolated research project, reproducible public-data manifests/quality report; no private rows |
| L3 | [050_l3_rule_replay.md](050_l3_rule_replay.md) | Full-universe rule/ladder replay, per-arm estimates/CIs, 9 SKIP cells and sensitivities |
| L3b | [060_l3b_exit_management.md](060_l3b_exit_management.md) | State-aware exits, label ambiguity/balance, registered exit-arm metrics and promotion decision |
| L4 | [070_l4_prediction.md](070_l4_prediction.md) | M0–M4 walk-forward comparison, contamination/feasibility audit, at most one frozen model or rules-only |
| L4b | [080_l4b_position_models.md](080_l4b_position_models.md) | Rule-only then model cut-vs-hold tables, false alarms and selection verdict |
| L5 | [090_l5_holdout.md](090_l5_holdout.md) | Exactly one sealed evaluation with freeze SHA, hashes, all prespecified results/failures |
| L6 | [100_l6_scorer_shadow.md](100_l6_scorer_shadow.md) | Optional scorer package/runbook; unit disabled, Discord disabled |
| L7 | [110_l7_position_tracker.md](110_l7_position_tracker.md) | Disabled shadow tracker/warnings, reconciliation tests, sample embed/runbook; no key values |
| L8 | [120_l8_guardian_testnet.md](120_l8_guardian_testnet.md) | Guardian-only testnet qualification matrix/receipt; no production request and no live arm |

All phase plans are written before implementation. Each repository packet is a separate commit. Before every bot-repository commit, the complete `uv run pytest -q` suite must pass and the post-commit tree must be clean. Other packet gates from `AGENTS.md` apply. No push or PR is authorized.

## Verified local baseline / open gates

- `origin/main` resolves locally to `4425b705857eb84abe391eb2f9a5fcffb6053826`.
- The development cutoff was computed from 33 public-data manifests in the primary checkout (metadata only): latest `last_close_time_ms=1782863999999`, ADAUSDT 1h, `2026-06-30T23:59:59.999Z`. Hence holdout is exactly `[2026-07-01T00:00:00Z, 2026-10-06T23:59:59Z]`. The new branch does not contain those ignored data files; the measured cutoff is not substituted for evidence hashes in L1.
- OCI CLI is installed but `oci iam region list` failed because required local profile fields are missing. No profile/key file was opened, no remote connection was attempted, and L0 is BLOCKED pending local access setup and measurement.
- Baseline full pytest: sandboxed RTK invocation failed before tests because uv could not write its interpreter cache. The project venv initially lacked the optional test dependencies, so the approved complete baseline command was `uv run --extra dev pytest -q`; result: `4756 passed, 34 skipped in 1648.25s` (2026-10-07). Skips were optional PostgreSQL/local-corpus tests and host-specific Windows symlink/open-file operations. No product test failed.
- `devlog/_plan/260917_luna-trading-bot-roadmap/070_guardian_testnet_execution.md` and `080_guardian_limited_live.md` remain the execution authority. Their testnet/live gates are not weakened by this project.

## Pre-registered research values to carry unchanged

The complete task contract is the user-supplied Pump-fade v3 brief. The user approved these four defaults for preregistration: R=1% of initial notional per parent; extra publication-lag margin=5 minutes beyond the stated 5-minute row delay (10 minutes total from row timestamp); L4b maximum false-alarm rate=20 per 100 positions; E3 chandelier trail=3× closed-5m ATR. At minimum, L1 must also encode development start 2025-01-01; holdout as above; A/B/D marker definitions with D-as-validated (2× prior-24h mean) primary and v2d 12h median sensitivity; all release and SKIP arms; I=`max(event-to-date mark high at first fill) × (1+k)`, primary k=0.15 and sensitivities 0.10/0.25; first-fill, 2-add primary, 3-add and legacy 8-add ladders; 15/30/60 bp costs plus funding; day bootstrap 10,000 draws, seed 20261007, Holm families; L3b thresholds and arms; L4/L4b labels, walk-forward/purge/embargo, model-selection and OCI thresholds; L5 one-shot rule; L7 warning validity; and L8 A1/A2/A3 action gates. If another ambiguity prevents an exact deterministic registration, stop and ask before data exposure.

## No-go rules

No holdout bytes may be downloaded/read by analysis until the freeze commit SHA exists and the sealed-directory guard accepts exactly that SHA. No L0 deployment without both 30-minute baseline and passing 2-hour staged gate. No private P3 rows in repository/worktree/logs; aggregate only. No ML runtime dependency in `pyproject.toml` or `uv.lock`. No exchange writes unless the exact L8 stage/testnet gates pass and user-provided testnet credentials are already installed through the approved local mechanism. A missing precondition yields an explicit blocked/not-run receipt, not a simulated pass.
