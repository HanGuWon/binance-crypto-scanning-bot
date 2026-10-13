# Phase 2 bounded funding-index rework — 2026-10-10

This B rework follows the exact-identity `773802d06316253ed47f058dfce9e53f075df0d905e9468169021bb1c5a8d31e` coverage attempt. That implementation had an independent PASS, entered the real development coverage path, and stopped with `coverage interval count exceeds the bounded evidence limit` (exit 1). Its canonical manifest remains `INCOMPLETE`; all 1,301 identity-scoped checkpoints (499,228,975 bytes) are preserved and must not be reused under the repaired identity.

## Root-cause hypotheses and falsifiers

- H1: funding settlement points consumed the generic interval cap. Falsifier: non-funding segments dominate or funding instants are not counted one-for-one. Evidence: 999,506 funding points, 773 contract segments, and 9 exclusions; supported.
- H2: five-minute gap fragmentation or exclusions crossed the cap. Falsifier: ordinary segment/exclusion counts remain below their bounds. Evidence: 773 non-funding segments and 9 exclusions; rejected.
- H3: resume duplicated groups or corrupt checkpoints inflated totals. Falsifier: checkpoints do not uniquely reconcile to valid family/symbol groups in the exact identity namespace. Evidence: 764 contract and 537 funding checkpoints (1,301 total), all identity-scoped to `773802d...`; rejected.

This is a representation/limit mismatch, not malformed source data or storage-floor exhaustion. Frozen metadata records 1,429,746 funding rows, an input-bound upper bound on distinct settlements. The repair retains the one-million limit for ordinary coverage segments and exclusions, keeps each exact funding instant in a sorted packed signed-64-bit array with a separate 2,000,000-point hard cap (16,000,000 maximum timestamp payload bytes), and answers availability by `start_ms <= settlement_ms < end_exclusive_ms`. It introduces no cadence, interpolation, rounding, or event substitution. Reported family totals continue to include funding instants explicitly.

## Implementation and proof

Changed research files: `src/pump_v3_replay/coverage.py`, `src/pump_v3_replay/coverage_run.py`, `l3/ADMISSION_PROTOCOL_V2.md`, `tests/test_l3_coverage.py`, and `tests/test_l3_coverage_run.py`. The protocol addition documents storage representation/capacity only; scientific funding semantics are unchanged. The new code and protocol hashes require a new canonical identity and independent PASS. The old PASS does not transfer.

The red regression is at `D:\Binance-trade-analysis\2026-10-08_pump-v3\l3\scratch\20261010-phase2-amendment\final-validation-773802d0-r4\logs\funding-index-red.log` (missing builder, exit 2). The corrected focused synthetic suite is `...\focused-funding-green-r3.log` (30 passed, exit 0); it checks exact half-open event boundaries, a non-grid settlement, no cadence bridging, a tiny injected cap with no partial mutation, and the end-to-end run at the ordinary segment cap with funding separately counted.

Fresh whole-research checks used the existing Python 3.12.14 interpreter, without dependency sync/install: `...\full-ruff.log` (exit 0, `src tests`), `...\full-pyright.log` (exit 0; 0 errors/warnings/informations), `...\full-pytest.log` (exit 0; 145 passed), and `...\full-compileall.log` (exit 0). The logs live under `D:\Binance-trade-analysis\2026-10-08_pump-v3\l3\scratch\20261010-phase2-amendment\final-validation-773802d0-r4\logs`.

Next: recompute the executable inventory, protocol digest, and metadata-only identity; preserve the old incomplete manifest/checkpoints; obtain independent PASS on the exact new identity; then scan only into a new full-identity checkpoint directory. No raw rows beyond the already completed failed run, no outcomes, and no holdout data were used for this rework or its tests.
