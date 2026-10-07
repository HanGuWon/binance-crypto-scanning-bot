# Pump-fade v2c local remediation report

**Rule identity:** `pump-fade-v2-20261007-remediation-c`. **Classification:** post-exposure engineering remediation, local-only, public-data-only, disabled, no orders. It is not a new pre-outcome preregistration or confirmatory strategy result. The previous v2b freeze and exposed output are preserved.

## Completed implementation scope

- The verified strategy plan selects a 15% primary adverse-mark squeeze; v2c labels the exact boundary inclusively and derives label/replay thresholds from the v2c policy. The 20% output is explicitly a sensitivity.
- Funding cap-hit clearance processes a hit preceding event admission and clears only that reason after three observed normalized samples spaced by at least 60 seconds. A repeated hit resets the streak. Funding metadata/rates use decimal-fraction units with range validation. Interval restoration anchors to the original observed interval before the first shortening (8→4→2→4 remains WAIT).
- Runtime, public REST scheduler, bounded raw capture, generation ledger, segment verification, raw materializer, causal bar assembly, durable preview/decision storage and hash-acknowledged terminal archive are local-only. Metadata replay cannot rewind latest funding state; stale/missing evidence remains unavailable. Preview storage has no Discord sender.
- Replay preserves the fixed original-parent cohort, records abstentions separately from trade PnL and missed-fade opportunity, reports 4h/24h kline proxy labels, evaluates the registered 15% primary and 20% sensitivity, and reports R3 only as disjoint-landmark association. Unsupported PIT and private execution arms are explicit `UNAVAILABLE`.
- The independent 20-case matrix is `REMEDIATION_ACCEPTANCE_MATRIX.md`. Each row must be marked only from final successful test output; see final verification receipt below.

## Empirical status

The corrected v2c replay uses 17 locally manifested USD-M 5m kline datasets: 744 complete 24h proxy parents, 85 UTC-day clusters, evidence class `RETROSPECTIVE_EXPOSED_KLINE_PROXY_NOT_PIT_ELIGIBLE`. Its files and hashes are under `offline_public_kline_proxy_v2c_20261007/`. It does not reconstruct historical listing/OI/funding/mark/BBO/receipt order, private P3 fills, fees, funding cash flow, queue fills or causal skip value. R1/R2/R3/R4 outputs are descriptive proxies; official gates remain DATA_INCOMPLETE. P1 and v2b outcomes were already exposed. No alpha, net expectancy, q99 gate, or promotion is claimed.

## Operational status

The v2c forward config, local release manifest and sample payload are disabled. No live capture, Discord message, OCI operation, account access, order path, Guardian edit, or deployment was performed. The v2c package is not a production scanner change. New external review was not dispatched under the single-agent scope.

## Final verification

Validation environment: Python 3.12.13 in `D:\Binance bot-2\.venv-codex`; `uv` was invoked with the active frozen environment and its cache redirected to the authorized visualization workspace because its default cache path is sandbox-protected.

| Check | Result |
|---|---|
| `uv run --active --frozen --no-sync python -m ruff check .` | PASS — final run after implementation |
| `uv run --active --frozen --no-sync python -m pyright` | PASS — 0 errors, 0 warnings, 0 informations |
| `uv run --active --frozen --no-sync python -m pytest -q tests/unit/test_pump_fade_v2.py` | PASS — 25 passed |
| `uv run --active --frozen --no-sync python -m compileall -q src tests` | PASS |
| `uv run --active --frozen --no-sync python -m pytest -vv -o faulthandler_timeout=15` | BLOCKED by host sandbox: 4,759 tests collected; the first benchmark test passed, then pytest-asyncio blocked constructing a Windows event loop at loopback `socket.accept` for its internal socketpair. No external API test was run. The agent-interrupted invocation is not reported as a pass. |
| `git diff --check` | PASS — no whitespace errors; Git reported only the existing LF→CRLF worktree warning for the preserved `capture/websocket.py` change |
| `docs/pump_fade_v2/release_v2c/release_manifest.json` | LOCAL_PREPARED_NO_DEPLOYMENT — separate v2c hash closure; deployment false |

The repository-wide pytest blocker is specific to this host's denied local socketpair initialization; isolated Pump-fade fake REST tests avoid creating an event loop and pass. Full-suite status remains unverified, not passed. No package, replay, or targeted test result constitutes strategy efficacy evidence.
