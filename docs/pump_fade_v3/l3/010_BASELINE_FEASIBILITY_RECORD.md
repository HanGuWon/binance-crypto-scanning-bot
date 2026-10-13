# L3 Phase 1 — baseline repair and feasibility record

## Current engineering disposition — 2026-10-11

This dated disposition supersedes the initial-cycle status below where the two
conflict. Phase 1 baseline repair and the bounded arithmetic remediation have
engineering acceptance: the final independent review of the remediation
source/tests reported no residual, and the fresh Phase 1 C-phase static, type,
pytest and compile checks are recorded below and in the external research
package's `l3/CURRENT_STATUS.md`. The original arithmetic rounding and
underflow findings remain as historical first-cycle evidence, not current open
findings.

The Phase 1 numerical-seconds loader limitation is also historical. The current
Phase 2 pre-replay normalizer in
`D:\Binance-trade-analysis\2026-10-08_pump-v3\src\pump_v3_replay\data.py`
converts 10-digit epoch seconds to milliseconds, retains 13-digit milliseconds,
and rejects unsupported numeric widths/forms before scan consumption; its
callers cover candle, funding and metrics timestamps. Boundary evidence is in
`tests/test_l3_baseline.py`. This does not change the frozen L1/L2 inputs.

As of this disposition, Phase 2 has a metadata-only admission identity
`6025bb226fd12b1b8c07f5f272b996a8f3669d0b24291c73ae197165ef08d2a7` for the
312,805 indexed artifacts. It records `coverage=NOT_RUN` and
`outcomes=NOT_COMPUTED_PHASE_2`; the freeze read zero archive rows. Fresh
research checks report Ruff exit 0, Pyright exit 0, pytest 170 passed, and
Python 3.14.4 compileall exit 0; detailed commands and logs are indexed in
`D:\Binance-trade-analysis\2026-10-08_pump-v3\l3\evidence\phase2_current_source_test_inventory_20261010.md`.
The exact-identity implementation review is still pending at the time of this
record update, so no archive scan or Phase 2 closure is claimed here. This is a
development engineering record, not a strategy efficacy result; no holdout or
outcome data are reported.

Initial-cycle status: implementation and local checks completed, but the first
independent implementation review found unresolved arithmetic precision issues.
See the current remediation addendum below; Phase 1 remains pending independent
re-review and supported C/D closure. This is a development engineering record,
not a strategy efficacy result.

## Frozen authority and study boundary

The work was performed against bot worktree branch
`codex/pump-fade-v3-20261007`, L1 freeze commit
`3fe2481ea07a4b38e2768d647747e75bfe77bdbe`. At Phase 1 revalidation, pinned
SHA-256 values remained unchanged:

| Frozen L1 file | SHA-256 |
|---|---|
| `config/pump_fade_v3/freeze_manifest_v3.json` | `b928a9b280c3828e9affdde00b6adb97b63cfb239f6dfebd03945cdbf5730b26` |
| `config/pump_fade_v3/policy_v3.json` | `8f01bbc7d628d144fe13ce966b9ba6456795fc905d819ff2b476ec192a0a8f56` |
| `docs/pump_fade_v3/PREREGISTRATION_V3.md` | `39b26304b574292c287a23ce02f4b35b947a4a4a0e441c178559dbe92c4aef7f` |
| `src/signalbot/pump_fade_v3/holdout_guard.py` | `f4944a4402736bf1a5932436e87739efe89d4450968b9df8787824bee1401064` |

The external non-holdout pin `L1_TRUSTED_FREEZE.json` is 783 bytes,
SHA-256 `1e85c566abf0cd5e5c7ec8c05685b371fb01932610197b21f96446ab694a6db6`.
L2 identity remains: `data_manifest.json` 551,119,400 bytes,
`52b441e3449f4f067be13df4c1d00d8131c0ad4cd5664ae0b4e83a032c8db7a1`;
`data/development/source_plan.json` 139,671,153 bytes,
`0afcbb6209b96d9a0fe97d2227d866c00d12a32de93c7289a0a6cf8293589054`;
`data/development/download_index.jsonl` 268,894,290 bytes,
`41d62bd0108ee2fa8d4a7b319347029fedd27cae82edad516dd4c19fe589d332`.
No sealed or holdout path was accessed. No L1/L2 file, `.gitignore`, bot
production dependency, or external `pyproject.toml`/`uv.lock` was changed.

## Actual external change inventory

External research cwd: `D:\Binance-trade-analysis\2026-10-08_pump-v3`.
That directory is not a Git repository; `git rev-parse --is-inside-work-tree`
returned the expected fatal “not a git repository.” Do not interpret these
records as an external Git diff. `l3/evidence/phase1/CHANGE_RECORD.md` contains
the source-level changed-symbol summary and full before/after inventory.

| External artifact | Bytes | SHA-256 |
|---|---:|---|
| `src/pump_v3_replay/data.py` | 6,566 | `f91961b7aea6eefc475e529860f1712a86d030bdc0872dc350efcc32723bbdcc` |
| `src/pump_v3_replay/models.py` | 1,690 | `53c8f4eb8a383ff3b7934dba2f4e827bce9637c15457dad7adb4da5086c37eb8` |
| `src/pump_v3_replay/feasibility.py` | 3,533 | `517ee33cf89cb2f3ae045686bc2d29d4730e73b898ffd5f39535cc88085b7f06` |
| `tests/test_l3_feasibility.py` | 4,839 | `f179eeeafb7d41302a8f86788c88479b03d3d8739db49884a288ea03b1b8fdbb` |
| `tests/test_l3_baseline.py` | 2,135 | `50a21c9f22837b937534a37cf91da06608477624e2c0fee47c2cc727cf10c7c9` |
| `l3/FEASIBILITY_AUDIT.md` | 2,262 | `ccf5c00db34161da319c81d8271cb76d67643dcbd2c5dc6486f078684862c878` |
| `l3/CURRENT_STATUS.md` | 1,688 | `b68210217c2cc3dde70333f26d7adbc3fba386849a96229766c0908733fb1299` |
| `l3/evidence/phase1/CHANGE_RECORD.md` | 5,177 | `68ab325cb760422ecff8e42103f8dd5b32a853da4946229d1f34c64eba77f58d` |

The original L2 test file remains byte-identical at 11,184 bytes, SHA-256
`caa7979a9a824b7abbb702f4c6c7c97973d957285d34525a3702fbb1ccf046e2`.
External configuration and lock remain byte-identical: `pyproject.toml`
499 bytes / `fbc47c3a92923de6379ee77b98ade690cbadb38c5e2c5ed83572aeb60929ec7c`;
`uv.lock` 12,790 bytes /
`94d173dfc1b4d78acbad4292cffd6252b9cd2d97ae8e988e0b2aed3d7a251947`.

## Implemented baseline repair

`data.py` now has recursive JSON types, explicit runtime validation for
manifest/artifact/metrics receipt container shapes, direct ISO parsing (including
`Z`) with naive timestamps still treated as UTC, and an equivalent `pairwise`
duplicate-kline check. Existing manifest-status admission and metrics
convention behavior are preserved for this phase. `models.py` gives
`ParentStudy`'s three mapping default factories explicit generic types.
`feasibility.py` provides exact-Decimal runtime tags for base quantity,
quote-per-base price, dimensionless rate and quote notional. It computes price
move risk and reserve cost separately with local precision derived from operand
significance, and tests `total_risk <= budget`; it does not infer the budget
denominator. A 29-digit quantity is verified under a deliberately reduced
ambient Decimal precision, with direct wrong-unit tests for each risk argument
and both feasibility operands.

One inherited data-loader limitation was made explicit by a compatibility
test: numeric epoch seconds currently pass through `_ms` unchanged instead of
being converted to milliseconds. Numeric millisecond values remain
milliseconds. Phase 1 did not alter this behavior or scan source rows; Phase 2
must bind accepted timestamp formats/units and reject or normalize seconds
before any replay consumes them.

## Frozen first-fill inequality

The audited expression is
`sum_i q_i * (abs(I - P_i) + 0.003 * P_i) <= R`.
`q_i` is base quantity, `I`/`P_i` are quote-per-base prices, the `0.003`
reserve rate is dimensionless, and the resulting risk and `R` are quote
notional. With `P_i=100`, mark high `100`, `I=115`, `q_i=1`, the separate
components are 15 and 0.3, total 15.3. Generic budgets 16, 15.3 and 15.2
yield feasible, equality-feasible and infeasible results, respectively.

The frozen text says `R=1%` of “initial parent notional,” but does not define
that quantity as `q0*P0`. Conditional illustration only: if
`N0=q0*P0=100`, `q0=1`, then `R=1` and 15.3 is infeasible. This conditional
case is not promoted to the primary interpretation. Primary feasibility is
`UNRESOLVED`, not PASS or FAIL, until the binding denominator is defined. The
audit is synthetic arithmetic; no market outcome rows were used.

## Verification and durable logs

Successful offline checks, run from the external cwd using the authorized host
execution path:

| Command | Exit | Result | Log SHA-256 |
|---|---:|---|---|
| `uv run ruff check .` | 0 | All checks passed | `8e727a4e348755c735aaf913d016ece4e0c331fbfb4d60c94b6ab5be1de789f0` |
| `uv run pyright` | 0 | 0 errors, warnings or information | `0196f9a7e6c710bb725f243c9ed3df4294d9c82aec9fb38526c5d97b9d7ce3b4` |
| `uv run pytest -q` | 0 | 39 passed, including all 11 existing tests | `cbc62bc3116674b7f4a1afd73ca64262dfd9e18488e2520f04aaf6975fcb1658` |
| `python -m compileall -q src tests` | 0 | success | `c0a7aa32281690f79f30454cb778d0e437db54b61e61f5530e77045e7bda3089` |
| `uv run pytest -q tests/test_l3_feasibility.py` | 0 | 19 passed | `c684b4249ed76d4d75676561e7c671c8005f7072db664b73ee0f14cb131d22ee` |
| `uv run pytest -q tests/test_l3_baseline.py` | 0 | 9 passed | `6a20966799cb5810c03aaa5805c3e7aa40d94541d1446d1830723de8353f9454` |

Each exact command, raw output and exit code is preserved at the matching
`D:\Binance-trade-analysis\2026-10-08_pump-v3\l3\evidence\phase1\*.log`.
The first sandbox attempts could not access the external interpreter or
bytecode cache; those environment failures are not included as successful
checks. All six final command runs succeeded through the host execution path.

## Independent review and remaining limits

The Phase 1 plan received an independent A-phase PASS before implementation;
that is planning review only. The first independent C implementation review
returned FAIL. It found (High) that summing the exactly calculated components
outside the local Decimal context rounds a 29-digit total and can change an
equality-boundary decision; and (Medium) that arbitrarily extreme but finite
Decimal exponents remain accepted and may underflow to zero. It also found a
Medium inherited numeric-seconds timestamp limitation, explicitly deferred to
Phase 2 before replay, and a stale test-count sentence, corrected in this
record. The C review's source/test/log ledger is in the reviewer report recorded
in the local Phase 1 audit notes. These High/Medium arithmetic issues remain
open; passing static checks do not close them. The 39 tests establish local
package checks only, not replay correctness, coverage, fills or efficacy.
Phase 2 admission/coverage and Phase 3 development replay remain unperformed.
No results are inferred from this record.

## Phase 1 arithmetic remediation addendum — 2026-10-09

The first C implementation review's FAIL remains in force as the historical
finding that triggered this remediation. Its High finding was that exact
per-fill components could round when added outside the local Decimal context;
its Medium finding was that arbitrarily small finite Decimal inputs could
underflow. The remediation changes the existing external files
`D:\Binance-trade-analysis\2026-10-08_pump-v3\src\pump_v3_replay\feasibility.py`
and `D:\Binance-trade-analysis\2026-10-08_pump-v3\tests\test_l3_feasibility.py`;
these are modifications to the already-existing Phase 1 files, not new files.

The helper now enforces exact built-in Decimal values; input coefficient/exponent
bounds (64 digits, [-1000, 1000]); component coefficient limit (2,128 digits);
quote-notional limit (8,192 digits, [-3000, 3000]); and a 1,000,000-fill cap.
Exact outer and nested tuples, tags and components are preflighted before the
named `_sum_preflighted_components` arithmetic seam. `risk_components` uses a
derived local precision and traps `Inexact`, `Rounded`, `Underflow` and
`Overflow`. Aggregate precision is bounded at 8,135 digits from 2,128 component
digits, 6,000 digits of exponent alignment, and 7 carry digits for two million
nonnegative components. Empty input returns typed quote-notional zero, and the
comparison remains exactly `total_risk <= budget`.

New offline tests add a hard-coded large arithmetic oracle, ambient precision 2,
exact-equality/adjacent budgets, numeric-envelope endpoints and just-outside
rejection, 2,128/2,129 component digits, fill-count cap, strict Decimal/tuple/tag
rejection, typed empty total, and a late-invalid sentinel proving all components
are checked before the arithmetic seam. Current changed-file identities are:

| File | Bytes | SHA-256 |
|---|---:|---|
| `src/pump_v3_replay/feasibility.py` | 8,014 | `419210ebdc86e8f0c6542390f709e5d0783dbe4b4c95c603dac5b6a4364a50a4` |
| `tests/test_l3_feasibility.py` | 10,987 | `98f78e7ffb9649c1f97adfd2b7f6a5faf408830641e118ce08211986cc887622` |

Fresh local checks from `D:\Binance-trade-analysis\2026-10-08_pump-v3`:

| Command | Exit | Result |
|---|---:|---|
| `uv run ruff check .` | 0 | All checks passed |
| `uv run pyright` | 0 | 0 errors, 0 warnings, 0 informations |
| `uv run pytest -q` | 0 | 49 passed |
| `& 'D:\Binance-trade-analysis\2026-10-08_pump-v3\.venv\Scripts\python.exe' --version` | 0 | Python 3.14.4 |
| `& 'D:\Binance-trade-analysis\2026-10-08_pump-v3\.venv\Scripts\python.exe' -m compileall -q src tests` | 0 | success; empty stdout |
| `uv run pytest -q tests/test_l3_feasibility.py` | 0 | 29 passed |
| `uv run pytest -q tests/test_l3_baseline.py` | 0 | 9 passed |

Complete command outputs and exit codes are in external
`l3/evidence/phase1/{ruff,pyright,pytest,compileall,test_l3_feasibility,test_l3_baseline}.log`;
their hashes are in the refreshed external change record. Generic Python 3.11
compileall evidence is historical; the current compilation used the verified
project Python 3.14.4 executable. No L1 or L2 hash changed: trusted freeze
commit remains `3fe2481ea07a4b38e2768d647747e75bfe77bdbe`; the four pinned L1
files match `L1_TRUSTED_FREEZE.json`; manifest SHA-256 remains
`52b441e3449f4f067be13df4c1d00d8131c0ad4cd5664ae0b4e83a032c8db7a1`, source
plan remains `0afcbb6209b96d9a0fe97d2227d866c00d12a32de93c7289a0a6cf8293589054`,
and development download index remains
`41d62bd0108ee2fa8d4a7b319347029fedd27cae82edad516dd4c19fe589d332`.

Independent reviewer Aristotle confirmed the former rounding and underflow
defects are closed within the declared numeric bounds. Its single Medium
finding—missing tests for a late oversized numeric component before the
arithmetic seam, tuple subclasses, and custom Sequence rejection—was accepted;
the remediation added those reachable cases, and the reviewer then closed it
with no residual. Fresh C-phase checks after that repair all exit 0. Phase 1
engineering acceptance is complete; this record is being finalized in C, and
the supported D transition remains the formal cycle-closure step. Primary parent-notional feasibility remains `UNRESOLVED`: the
binding text does not define `initial parent notional` as `q0 * P0`. Numeric
epoch-seconds normalization/rejection remains a Phase 2 pre-replay gate. No
market outcomes, holdout data or seal metadata were accessed for this
remediation. No replay, coverage result, fill, or strategy efficacy claim is
made here.
