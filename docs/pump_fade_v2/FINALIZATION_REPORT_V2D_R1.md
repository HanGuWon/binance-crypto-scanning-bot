# Pump-fade v2d finalization report (revision r1)

Scope: technical finalization of the disabled v2d engineering package. This is not a
research result and creates no new hypothesis: the registered v2d policy, freeze and
contract bytes are unchanged. Primary squeeze threshold stays 15% (inclusive); the 20%
case remains a labeled sensitivity. Retrospective proxy evidence (744 parents, 85 UTC-day
clusters) is exposed development data. No net expectancy, alpha, q99 efficacy, clean
holdout or live confirmation is established by engineering tests.

## Corrections made in this finalization

**F5 - per-arm counterfactual funding (execution_replay.py).** Reproduced first: with a
stop exit at BASE+300 ms and the only settlement at BASE+1000 ms every arm still received
0.05 USDT, and arms holding 1.0 and 1.4 units received identical funding. The unbound
account cash amount was replaced by observed settlement rate and mark valuation, and each
arm's funding is recomputed from its own open quantity:

    cash = side_sign * quantity_open * mark_price * rate     (short receives when rate > 0)

- Quantity open at a settlement = the arm's accepted fills with `at_ms` strictly before
  it; zero after the simulated exit. Equality convention (declared before outcomes were
  inspected, identical to `labels.label_outcome` using `origin < settlement <= expiry`): a
  fill at exactly the settlement is not exposed; a position exiting at exactly the
  settlement still settles.
- Coverage is explicit: `FundingCoverage(start, end, scheduled settlements)` must span the
  arm's [first fill, exit] and every exposed scheduled settlement must have an observed
  rate and valuation. Otherwise the arm's funding and net PnL are `None` with a reason in
  `funding_status`; missing coverage is never zero. Verified coverage with no exposed
  settlement is a verified zero.
- Additions can no longer fill at or after the simulated exit.
- Downstream propagation: arm summaries report `net_complete_paths`,
  `funding_unavailable_paths` and `net_inference_status`; the mean net, the q99 tail and
  the paired noninferiority/Holm/q99-reduction inference become unavailable (not computed
  on a surviving subset) when any executable-complete paired parent lacks funding.
- Regression tests (22): stop exit before settlement, unequal quantities, additions before
  versus after a settlement, multiple settlements with distinct rate and valuation, side and
  sign, entry/exit/add timestamp boundaries (-1/0/+1 ms), no scheduled settlement, missing
  coverage, coverage not spanning the holding window, missing rate or valuation, input
  validation, summary and paired-inference propagation.

**Causal stop exit (execution_replay.py).** A stop-bar exit could use a quote stamped
between the bar's close and its receipt, before the stop was knowable. Reproduced with a
failing test; the exit now requires `quote.at_ms >= max(bar.close_ms, bar.received_ms)`.

**Release identity (release.py).** Identities now hash LF-canonical bytes (CRLF normalized
to LF, equal to the Git blob). Windows `core.autocrlf` checkouts previously produced
digests no other platform could reproduce. An optional `--revision` names a rebuilt
candidate without touching historical outputs. A rebuild from a Git-only LF export of the
staged index reproduced this manifest and package digest exactly.

## Identities (revision r1; LF-canonical, not comparable to the raw-byte baseline digests)

| Item | SHA-256 |
|---|---|
| Policy `policy_v2d.json` | `4485cfa163b85b984437e23563285d341467dd4b793a4c6c8db640325d095caf` |
| Freeze manifest `freeze_manifest_v2d.json` | `ed4f5bba1737b258c703b9db579194c233f2f7a2db93b2992d45effadfff3da7` |
| Remediation contract D | `b06a828281b68158ba11ddc6fea28c14583ee29eaa0589ea765e264da43f36df` |
| Executable source closure (107 files) | `b43c53cf3cb87da2bad9cb3f2aaf6c887a6e98b095be6de6b4f720b10556c572` |
| Executable tree (closure + lock/project + v2d inputs) | `a363ffbd9210692c63cad17297357ee1cce063fa71320f65044944492908239e` |
| Deterministic package `pump_fade_v2d-r1_package.zip` | `20791489f97a6dc7bc5c18ca7fed4bde21cd11b80d5708c2c9b597a2606cbccb` |
| Research-evidence tree (separate by design) | `cd1007121df6fd1e1e321d3bc3803ce69f5c727fbcb620300c5b89b946f1ff45` |
| `release_v2d_r1/release_manifest.json` | `513ed3eeaf4236d350efea883d204dedf888de15dd1249054a03a4f2648438db` |

Baseline v2d (pre-finalization, raw bytes; kept unchanged for history): tree
`356c61d31319e763061e61204161f762e1445763598de9713213891045c31aa3`, package `20cd48f0ad6800fe0f0efe5fd1e4c74d35cdf72bfb2bf27decca3bf30a67c190`. Superseded
by r1 because execution_replay.py and release.py changed.

Independent verification: all 128 manifest-listed files matched; the archive contains
exactly the listed executable inputs; extracted and imported with `python -I`, all 29
loaded `signalbot` modules resolve inside the archive.

## Publication file selection

Staged by explicit allowlist (never `git add .`): `src/signalbot/pump_fade_v2/*.py`,
`src/signalbot/capture/websocket.py` (the pre-existing additive diff widening the futures
market-stream allowlist to 1m/5m/15m/1h/4h klines and forceOrder; reviewed, public data
only, behavior preserved), `tests/unit/test_pump_fade_v2.py`, `config/pump_fade_v2/*.json`,
and `docs/pump_fade_v2/**` (contracts, reports, runbooks, evidence inputs, manifests).

Deliberately not published: both ZIP packages (reproducible from the builder, digests
documented above), `SONNET_FINALIZATION_HANDOFF.md` (internal handoff with local paths),
`var/`, `.tmp`, `.venv-codex`, caches, pyc, capture databases, review packets and any
private export. No raw private export was read. Historical v2b/v2c/v2d policies, freezes,
results and manifests are unchanged byte for byte. `SOURCE_VERIFICATION_20261007.json`
records local document paths and SHA-256 values only, no document content.

## Local validation (Windows, Python 3.12.13, `.venv-codex`, worktree `src` imported)

| Check | Result |
|---|---|
| `uv run --frozen --no-sync ruff check .` | exit 0 |
| `uv run --frozen --no-sync python -m pyright --pythonpath <venv python>` | 0 errors, 0 warnings, exit 0 |
| `uv run --frozen --no-sync pytest -q -p no:cacheprovider` (default temp dir) | 4756 passed, 34 skipped, exit 0 (27m58s) |
| Pump-fade suite `tests/unit/test_pump_fade_v2.py` | 56 passed, 0 skipped |
| `python -m compileall -q src tests` | exit 0 |
| `git diff --check` | exit 0 |

The 34 skips are platform or data related (Windows symlink and open-inode limits,
POSIX-only tests, PostgreSQL smoke without `SIGNALBOT_TEST_POSTGRES_URL`, unavailable local
indicator data); none are in the Pump-fade suite. An earlier full run that scoped `TEMP`
to a directory inside the worktree failed two unrelated tests that assume a temp dir
outside any Git repository; both pass with the default temp dir and the full run above
used it.

## Limitations

- Official point-in-time quote, mark, funding and universe inputs are not available, so
  the executable comparison is an engineering mechanism; synthetic fixtures are not
  evidence.
- Forward evaluation has not started and remains disabled; capture, Discord, orders,
  scanner/Guardian behavior and OCI deployment are not enabled. Nothing was deployed.
- Digest comparison across revisions requires the same canonicalization.
