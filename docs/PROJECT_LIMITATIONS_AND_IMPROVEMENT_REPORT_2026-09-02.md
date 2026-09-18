# Binance Signal Bot: Current Limitations and Improvement Report

- **Audit date:** 2 September 2026
- **Repository:** Binance bot-2
- **Inspected revision:** 39c17021b6dee4ae8b23b3af45456ca77ef080ef plus the dirty working tree
- **Assessment type:** Source, configuration, test, research-governance, deployment, and operational-readiness audit
- **Intended system role:** Public-data, alert-first Binance market signal service

## Executive assessment

The project has a stronger safety and evidence foundation than a typical trading-bot prototype. The inspected runtime remains public-data-only and alert-only; no production order-placement path or private-account dependency was found. Closed-candle processing, strict-prior higher-timeframe context, deterministic event identifiers, conflict-loud signal persistence, a transactional Discord outbox, and conservative handling of ambiguous deliveries are real strengths. The R4B V2 research code goes substantially further with raw-evidence lineage, causal clocks, bounded writers, immutable contracts, Decimal arithmetic, and adversarial tests.

The project is nevertheless **not ready to be represented as a clean release, a validated profitable strategy, or a production trading system**. That conclusion does not come from one catastrophic bug. It comes from the combination of:

- an unclean and non-reproducible working-tree snapshot;
- mixed fresh verification results, including a reproducible performance-test failure and a failing official Ruff gate;
- direct trust-boundary weaknesses around Boolean parsing and bootstrap candle continuity;
- restart-discontinuous V1 alert/PAPER state;
- synchronous database operations on asynchronous ingestion paths;
- incomplete PostgreSQL schema evolution;
- a configuration typo path that can silently downgrade evidence storage;
- known limitations in the legacy raw recorder;
- current dependency advisories;
- insufficient cross-platform, migration, load, and security automation; and
- an intentionally unfinished prospective research chain that does not yet support probability, expectancy, profitability, or promotion claims.

No Critical defect was established in this audit. The report records **10 High, 12 Medium, and 4 Low findings**. “High” here means that the issue can break a declared invariant, block a defensible release, or invalidate a material evidence claim. It does not mean that exchange funds are presently at risk: the inspected system has no production order path.

The appropriate current operating posture is:

1. keep the system observation-only;
2. do not introduce production order execution;
3. do not deploy or promote from the current dirty working tree;
4. preserve the active Phase R campaign without outcome-driven tuning;
5. repair trust boundaries, release reproducibility, persistence, and observability before expanding features; and
6. complete the preregistered evidence chain before making any performance claim.

## 1. Scope, evidence standard, and limitations of this audit

### 1.1 Snapshot boundary

This is a **working-tree assessment**, not a clean-HEAD assessment. At audit kickoff, <code>git status --porcelain=v1 -uall</code> produced 472 status rows: 18 paths had staged changes, 13 had unstaged changes, and 449 were untracked. Staged and unstaged categories can overlap for the same path. Audit records added during this review increased the untracked count further. The branch was <code>codex/btc-context-discord-recommendations</code>.

Consequently:

- a file observed in this report may not exist in the inspected Git commit;
- a staged version, working-tree version, deployed version, and research source manifest may differ;
- historical documentation that says a gate passed does not prove that the current working tree passes it; and
- Phase R configuration found in the tree expresses configuration intent, not proof of the configuration running on a remote host.

The checkout-local <code>.venv</code> was also not usable on this Windows host. Its <code>pyvenv.cfg</code> points to <code>/usr/bin</code>, and plain <code>uv run</code> failed while attempting to repair a Linux-origin <code>.venv/lib64</code> link. Fresh verification was therefore run in an isolated Python 3.12.13 environment outside the repository. This is a local reproducibility defect, not by itself a source-code test failure.

### 1.2 What was inspected

The audit covered:

- repository and working-tree state;
- V1 REST/WebSocket ingestion, provider parsing, candle storage, feature construction, rule evaluation, state transitions, persistence, and Discord delivery;
- legacy JSONL and segmented evidence-recording paths;
- R4B V2 completion status and Phase R/Phase S research-governance artifacts;
- configuration validation and defaults;
- SQLite and documented PostgreSQL paths;
- API, Docker, CI, tests, dependency state, and operational documentation;
- bounded-resource and causal-timing invariants; and
- current official Binance USDⓈ-M WebSocket routing documentation.

The audit did **not**:

- place an order, access a private Binance endpoint, or use a Binance API key;
- send a Discord message;
- alter the active prospective campaign or inspect prohibited future outcomes;
- observe the claimed OCI deployment directly;
- perform a live exchange smoke test;
- conduct a full Git-history secret scan, penetration test, or formal proof; or
- attempt to establish strategy profitability.

### 1.3 Evidence classification

Findings are labeled implicitly by their evidence:

- **Verified defect:** reproduced by a focused probe or fresh failing gate.
- **Documented limitation:** explicitly acknowledged by the project itself.
- **Conditional operational risk:** reachable under a deployment, restart, malformed-input, or load condition not exercised live during this audit.
- **Design recommendation:** defense-in-depth improvement without a demonstrated current failure.

File references use the current working-tree line numbers. Command results are fresh as of the audit date unless explicitly described as historical.

## 2. System boundary and architecture

The principal V1 path is:

    Public Binance REST / WebSocket
                    |
                    v
          provider schema parsing
                    |
                    v
       bounded candle / trade / BBO stores
                    |
                    v
      features + regime + strict-prior HTF
                    |
                    v
         rule engine + state machine
                    |
                    v
       SQL signal + immutable outbox intent
                    |
                    v
        independent Discord outbox worker

    Raw public frames --------------------------------> optional evidence recorder

R4B V2 is best understood as a parallel research/evidence program rather than a fully integrated replacement for every V1 runtime surface. It has stronger source membership, clock, writer, signing, and replay contracts, but its own completion matrix says that important M2, session-closure, execution/NAV, qualification, and efficacy authorities are still missing.

This separation matters throughout the report:

- **V1 live scanner:** provider parsing, bootstrap, alert state, PAPER lifecycle, synchronous persistence, and generic API.
- **Legacy JSONL recorder:** the flush/close durability concerns in section 7.
- **Segmented Phase R recorder:** stronger fsync, atomic-replace, hash-chain, recovery, and quota behavior; it is not assigned the legacy recorder's defects.
- **R4B V2 successor:** stronger research contracts, but not yet a promoting producer with complete efficacy evidence.

## 3. Fresh verification status

The official project contract names Ruff, Pyright, pytest, and compileall. The current snapshot does not pass that complete contract.

| Check | Fresh result | Interpretation |
| --- | --- | --- |
| <code>uv sync --extra dev --frozen</code> in isolated Python 3.12.13 environment | PASS; 44 packages installed | The lock resolves reproducibly outside the broken local virtual environment. |
| <code>uv run --frozen ruff check .</code> | FAIL; 79 diagnostics | The official full-tree lint gate is red. |
| <code>uv run --frozen ruff check src tests</code> | FAIL; one I001 at <code>tests/unit/test_phase_r_sqlite_remediation.py:3</code> | Even the core source/test scope is not entirely clean. |
| <code>uv run --frozen pyright</code> | PASS; 0 errors, 0 warnings, 0 information | Static typing passes under the configured standard-mode policy. |
| <code>uv run --frozen pytest -q</code> | FAIL; 1 failed, 4,166 passed, 21 skipped in 988.67 s | The suite is broad, but the snapshot is not fully green. |
| Focused operational-health performance test | FAIL again; 38.990 s versus a 30 s limit | The full-suite failure is reproducible, not a one-off collection artifact. |
| <code>uv run --frozen python -m compileall -q src tests</code> | PASS | All inspected source and test modules compile. |
| Configuration validation | PASS | The example configuration validates. |
| CLI dry run | PASS | Startup planning completed without starting live scanners. |
| Recorded replay fixture | PASS; 3 events, 0 parse errors, 0 decisions | The small fixture is internally replayable; it is not an efficacy test. |
| Production-lock <code>pip-audit</code> | FAIL; 9 rows / 7 unique advisories for <code>cryptography==46.0.3</code> | Advisories require triage and upgrade; reachability was not established. |
| Targeted tracked-file secret-pattern scan | No matches; command exit 1 means no match | Useful negative evidence, not a history-aware secret audit. |

The 79 full-tree Ruff diagnostics were concentrated in untracked or auxiliary material: 40 E501, 8 I001, 8 UP017, 7 syntax errors, and smaller groups of E402, UP031, B905, F401, RUF100, E401, RUF059, UP015, and UP035. Two syntactically invalid examples are <code>Binance bot-2.codexclaw_patch_incident.py</code> and <code>health/_dev/_tmp_patch.py</code>. The current <code>.gitignore</code> does not exclude all generated evidence, development scratch, or local orchestration paths, while CI invokes <code>ruff check .</code>. That mismatch makes release scope ambiguous.

The one pytest failure is at <code>tests/unit/test_oci_operational_health.py:303-353</code>. The middle scan took 39.861 seconds in the full suite and 38.990 seconds in a focused rerun. The test limit is 30 seconds. The implementation still performs a full manifest directory glob and sort at <code>tools/oci_operational_health.py:697-708</code>, then reads and verifies every newly discovered manifest/data pair at lines 720-806.

Historical claims must not be substituted for this table. For example, <code>PLAN.md:25-42</code> records 48 passing tests in an old Linux packaging context, while <code>docs/r4b-v2-completion-matrix.md:108-111</code> records 2,466 passing tests at a later checkpoint. Both are historical, and neither describes the current dirty snapshot with 4,166 passing cases and one failure.

## 4. Notable strengths that should be preserved

### 4.1 Alert-only and public-data boundary

No production order endpoint or private-account dependency was found in the inspected runtime. <code>docs/ARCHITECTURE.md:36</code> states the boundary, and the source search found public market-data adapters and explicit rejection of private/sensitive capture fields, but no order-placement owner. This materially reduces financial blast radius.

### 4.2 Closed-candle and causal context after parsing

The runtime rejects open candles at <code>src/signalbot/runtime.py:300-305</code>. Feature construction rejects any non-closed candle prefix at <code>src/signalbot/indicators/core.py:267-279</code>. Higher-timeframe lookup uses only snapshots with event time strictly less than the primary decision time at <code>src/signalbot/runtime.py:494-506</code>. No ordinary future-row or centered-window defect was established. The strict-Boolean parser issue in H-03 is the important trust-boundary exception.

### 4.3 Deterministic identities and conflict-loud persistence

Signal identity is deterministically derived from market, symbol, family, timeframe, stage, event time, and rule version at <code>src/signalbot/signals/state_machine.py:150-162</code>. The repository treats one event ID mapped to different signal or alert content as a hard conflict, while exact replays are no-ops at <code>src/signalbot/persistence/repository.py:1137-1221</code>. This is a sound idempotency baseline.

### 4.4 Atomic and conservative Discord delivery

The signal and immutable outbox intent are written in one transaction. Ambiguous transport errors, HTTP 5xx, and successful responses without a Discord message ID become <code>uncertain</code> rather than being blindly retried; only HTTP 429 is automatically retried with bounded attempts. See <code>docs/ARCHITECTURE.md:43-69</code> and <code>docs/OPERATIONS.md:43-73</code>. The limitation is operator reconciliation and health visibility, not a generally duplicate-prone design.

### 4.5 Timestamp and alert evidence

Discord payloads include UTC and Asia/Seoul time at <code>src/signalbot/alerts/embeds.py:132-135</code>, plus reasons, invalidation, event ID, and rule version at lines 435-555. PAPER exits explicitly say that no exchange order was placed and that state is not restored after restart.

### 4.6 Boundedness is a first-class concern

Candle histories, anomaly points, state, features, WebSocket queues, outbox activity, capture queues, and evidence storage have explicit limits or pruning mechanisms in many paths. The R4B V2 bounded-handoff code additionally tracks event and encoded-byte budgets. Some limits remain too generous or count-only, but the architecture treats unbounded growth as a defect rather than an acceptable default.

### 4.7 Strong segmented and R4B V2 evidence mechanisms

The active Phase R configuration selects <code>segmented_zstd_v1</code> at <code>config/prospective.causal-retest.phase-r.yaml:104-109</code>. That recorder has substantially stronger fsync, atomic replacement, hash chaining, recovery, and quota accounting than the legacy JSONL path. R4B V2 adds source membership, clock, writer lease, signed block, integrity ledger, Decimal, and fail-closed provenance contracts. These controls should be reused rather than rebuilt casually.

### 4.8 Honest research governance

<code>docs/RESEARCH_GOVERNANCE.md:3-4</code> explicitly labels the successor as shadow-only and denies trading or promotion authority. Lines 34-35 prohibit outcome-driven threshold selection before unlock; lines 48-76 separate development data from prospective proof and require multiplicity, chronological, dependence-aware, and calibration controls. This intellectual honesty is one of the project's most valuable assets.

### 4.9 Broad automated test investment

The current tree contains 228 Python test files and 3,051 textual test-function definitions. Parameterization produces more collected cases, as shown by the 4,166 passing cases. The suite contains extensive negative and adversarial tests around causality, storage, clock ownership, path traversal, cancellation, and evidence conflicts. The main issue is coverage topology and deployment realism, not lack of testing effort.

### 4.10 Current Binance Futures routing

The USDⓈ-M stream split is current, not stale. <code>src/signalbot/exchange/binance/endpoints.py:13-14</code> defines <code>/public</code> and <code>/market</code> combined endpoints, and lines 92-127 route klines, aggregate trades, and all-market mini tickers to market while routing book tickers to public. This matches Binance's [official base-URL split and migration notice](https://developers.binance.com/en/docs/products/derivatives-trading-usds-futures/websocket-market-streams/Important-WebSocket-Change-Notice), checked on 2 September 2026.

## 5. Prioritized finding register

| ID | Severity | Area | Finding | Primary consequence |
| --- | --- | --- | --- | --- |
| H-01 | High | Research | Prospective efficacy and profitability are unproven by design | No promotion, probability, expectancy, or profit claim is supportable |
| H-02 | High | Release | The working tree is not a reproducible release and official gates are red | Results cannot be tied cleanly to one shipped source artifact |
| H-03 | High | Correctness | Provider Booleans use Python truthiness instead of strict type validation | Malformed <code>\"false\"</code> can cross the closed-candle boundary as true |
| H-04 | High | Data integrity | Bootstrap does not prove exact candle-grid continuity | Indicators can treat nonadjacent observations as adjacent while completeness appears high |
| H-05 | High | Reliability | V1 alert cooldown and PAPER lifecycle are memory-only | Restarts change alert/lifecycle semantics and censor PAPER tracking |
| H-06 | High | Performance | Synchronous SQL runs on async ingestion paths | Database stalls can create market-data backlog and stale decisions |
| H-07 | High | Deployment | PostgreSQL has no supported versioned migration path | Existing PostgreSQL deployments can retain incompatible schemas |
| H-08 | High | Configuration | Unknown storage modes silently select legacy JSONL | A typo can downgrade evidence durability without startup failure |
| H-09 | High | Durability | Legacy JSONL “durable” accounting is flush-only and close can detach a writer | Acknowledged evidence may be lost or incompletely sealed |
| H-10 | High | Research durability | R4B V2 absolute monotonic nanoseconds exceed JSON safe integers after about 104 days | A 365-day evidence campaign requires a pre-deadline migration |
| M-01 | Medium | Security | The pinned cryptography package has current advisories | Release carries avoidable known dependency risk |
| M-02 | Medium | Operations | FAST manifest health scan exceeds its 30-second budget on Windows | Health monitoring can miss its own cadence as evidence grows |
| M-03 | Medium | Data integrity | Conflicting finalized candles silently overwrite prior values | Historical inputs can change after a decision without correction lineage |
| M-04 | Medium | Correctness | Sparse anomaly sampling can nearly double the advertised horizon | Alert reasons can describe a different interval from the one measured |
| M-05 | Medium | Universe | Spot age authority is process-local, sequential, and unbounded; malformed onboard fields can abort parsing | Refresh is rate-sensitive, restart-expensive, and brittle |
| M-06 | Medium | Rate limiting | REST throttling is reactive and client-local | Multiple clients can overshoot a shared provider budget |
| M-07 | Medium | Provenance | V1 rule version is manually assigned, not bound to effective decision configuration | Different semantics can share one version label |
| M-08 | Medium | Lifecycle | PAPER continuation can defer an entire universe rotation indefinitely | Stale/delisted symbols can block fresh universe adoption |
| M-09 | Medium | Alerting | Uncertain Discord rows require manual reconciliation and count against a hard cap | Delivery ambiguity can eventually stop new signal persistence |
| M-10 | Medium | Observability | Generic readiness reflects only repository initialization | A stale feed or blocked evidence/output path can still report ready |
| M-11 | Medium | Verification | CI lacks platform, PostgreSQL, security, coverage, load, and Freqtrade-analysis gates | Important deployment and scientific regressions can escape |
| M-12 | Medium | Maintainability | Active code and frozen research contracts are structurally very large and version-dense | Review cost, coupling, and inconsistent evolution risk are high |
| L-01 | Low | API/security | Recent signals and arbitrary HTTPS webhook targets rely on deployment discipline | External exposure or misconfiguration can disclose signal data |
| L-02 | Low | Database | Signal/alert/outbox/outcome relationships have no foreign keys | Manual or partial writes can create orphan rows |
| L-03 | Low | Resource control | Some queue limits are count-only or permit very large theoretical byte use | Burst memory can be much larger than the nominal item count suggests |
| L-04 | Low | Policy/docs | The default entry policy is legacy and several documents are stale | Omitted configuration or old guidance can select/describe the wrong contract |

## 6. Scientific and research limitations

### H-01 — Prospective efficacy and profitability are unproven

**Evidence.** The project says this plainly. <code>docs/r4b-v2-completion-matrix.md:3-5</code> warns that engineering test success is not profitable prospective performance. Lines 18-29 list material missing evidence:

- the corrected matched replay is not complete;
- the final-panel 24-hour capture and WAL qualification are missing;
- authoritative clock membership and finalization-grace anchoring remain incomplete;
- fee capture, funding authority, mandatory-exit integration, and isolated NAV remain incomplete;
- authoritative alert actionability replay and final inference are missing;
- the pre-T0 seal and fresh 30-day qualification are missing; and
- the untouched 365-calendar-day after-cost efficacy sample does not exist.

Lines 113-123 further state that repository checks prove consistency only, not 24-hour, 30-day, or 365-day requirements. <code>docs/RESEARCH_GOVERNANCE.md:46-76</code> correctly classifies earlier development data as exhausted for final proof and prohibits retroactive “untouched” claims.

The current Phase S plan is also intentionally outcome-blind. <code>devlog/_plan/260902_phase-s-forward-evidence/000_plan.md:7-19</code> excludes numeric future outcomes, profitability, tuning, collector changes, and fabricated future checkpoints. It records T+7 operational review at 8 September 2026 17:08:52.601 UTC and earliest T+14 eligibility at 15 September 2026 17:08:52.601 UTC at lines 82-88. The final handoff says accumulation is active and no future artifact exists at <code>devlog/_plan/260902_phase-s-forward-evidence/041_review-final-validation.md:3-7</code>. This audit did not independently observe that remote collector.

**Impact.** The system can be discussed as an alert and evidence-collection platform. It cannot be described as:

- profitable;
- expected to have positive after-cost expectancy;
- independently validated;
- a calibrated probability estimator;
- ready for production promotion; or
- evidence for executing real long or short orders.

**Required improvement.**

1. Preserve the frozen prospective contract and current outcome blindness.
2. Complete source census, M2 interpretation/finality, clock, fee, funding, execution, exit, and NAV authorities before T0.
3. Produce actual 24-hour capture qualification and fresh 30-day qualification.
4. Complete a corrected matched replay for diagnostic parity without treating it as final proof.
5. Lock the candidate and run the preregistered untouched confirmation campaign.
6. Apply dependence-aware uncertainty, chronological separation, and multiplicity correction.
7. Report signal alpha, execution quality, and cost avoidance separately, as required by governance.

**Exit criterion.** Promotion remains prohibited until every authoritative artifact named by the completion matrix exists, independently verifies, and the untouched after-cost analysis passes the preregistered criteria. A negative or inconclusive result is a valid terminal outcome; it must not trigger post-hoc threshold tuning on the proof cohort.

### H-10 — The long-duration R4B V2 clock representation has a known deadline

**Evidence.** <code>docs/r4b-v2-completion-matrix.md:120-123</code> states that a vNext migration is required before approximately 104 days of host uptime because absolute monotonic nanoseconds cease to fit the RFC 8785 JSON safe-integer domain. The project simultaneously requires an untouched 365-day evidence horizon.

**Impact.** A long campaign can reach a known representational boundary before its required efficacy horizon. Waiting until the boundary is near would force a high-risk mid-campaign format change or make later evidence unverifiable under the frozen contract.

**Required improvement.** Define a versioned representation before the deadline, such as a bounded session-relative monotonic offset plus a separately bound epoch/anchor, or a canonical string/integer encoding whose range is unambiguous. Specify migration, dual-read, old/new signature-domain separation, replay, and rollback behavior. Do not silently change an existing canonical payload.

**Exit criterion.** A hostile-boundary test must cover values below, at, and above the former safe-integer limit; old and new artifacts must verify only under their declared schema; a 365-day simulated timeline must preserve ordering and canonical hashes; and the active campaign must either remain entirely on one safe version or use a preregistered, independently verified transition.

## 7. High-severity software and operational limitations

### H-02 — The current snapshot is not a reproducible release

**Evidence.**

- The audit began with 472 Git status rows, dominated by 449 untracked paths.
- Core runtime files had both staged and unstaged changes.
- The Phase R candidate configuration and research-governance material were untracked.
- The local virtual environment was a Linux-origin environment unusable on Windows.
- The official Ruff gate fails with 79 diagnostics, and the full test gate has one reproducible failure.
- <code>.gitignore:21-29</code> excludes only selected research outputs; it does not define a coherent boundary for all local <code>.codexclaw</code>, <code>artifacts/evidence</code>, <code>health/_dev</code>, and scratch Python files that the full-tree lint command discovers.
- <code>PLAN.md:3</code> still assumes a Linux sandbox under <code>/mnt/data</code>, lines 46-53 describe a one-off ZIP packaging workflow, and line 42 reports only 48 tests.
- <code>docs/runtime-repair-plan-2026-08-17.md:28-29</code> says the workspace has no Git commit even though the current repository has one, and lines 43-44 still show universe refresh as incomplete despite current rotation code.

**Impact.** No reviewer can reliably answer “which exact source, config, dependency lock, container, and research contract produced this behavior?” A passing subset of tests on the mutable working tree cannot be tied to a release artifact, and a deployment could differ from the audited content without visible provenance.

**Required improvement.**

1. Classify every current untracked path as authoritative source, retained evidence, generated output, or disposable scratch.
2. Move or exclude generated/scratch paths from source gates without hiding authoritative tools.
3. Make <code>ruff check .</code>, Pyright, pytest, and compileall green from a clean clone.
4. Recreate the virtual environment locally rather than carrying a platform-specific environment.
5. Freeze a clean commit/tag plus source tree hash, configuration hash, <code>uv.lock</code> hash, container digest, and rule/research contract hashes.
6. Update or archive stale status documents so that one current source of truth exists.

**Exit criterion.** A new machine can check out the tag, run <code>uv sync --extra dev --frozen</code>, reproduce all gates with zero failures, build the same package/container digest, and verify that the deployed startup receipt names the same source/config/lock identities.

### H-03 — Boolean parsing fails open at the provider boundary

**Evidence.** <code>src/signalbot/exchange/binance/schemas.py:80-107</code> uses:

- <code>is_closed=bool(kline.get("x", False))</code>; and
- <code>is_buyer_maker=bool(payload["m"])</code>.

In Python, <code>bool("false")</code>, <code>bool("0")</code>, and <code>bool(1)</code> are true. A focused audit probe changed a recorded open-kline fixture to <code>"x": "false"</code>; the parser produced <code>is_closed=True</code>. The runtime then trusts that field at <code>src/signalbot/runtime.py:300-302</code>. Contract tests at <code>tests/contract/test_binance_payloads.py:17-80</code> cover valid values and missing fields, but not wrong-typed Booleans.

Binance's official schemas currently describe kline <code>x</code> and aggregate-trade <code>m</code> as Boolean values. This lowers the normal-path likelihood but does not justify fail-open behavior at an external trust boundary.

**Impact.** A malformed, proxy-altered, schema-drifted, or incorrectly recorded payload can:

- admit an unclosed candle into the closed-only signal path; or
- invert buyer-maker interpretation and distort order-flow evidence.

The first consequence directly violates a non-negotiable project invariant.

**Required improvement.** Introduce one strict decoder that accepts only <code>type(value) is bool</code>. Decide explicitly whether a missing field is a parse error or an allowed default; for kline closure, fail closed. Reject strings, integers, null, lists, and objects with a sanitized parse category and bounded counter.

**Exit criterion.** Positive, negative, missing, null, string, integer, and object boundary tests exist for both fields. Wrong types never produce a domain event, increment a visible parse-error metric, and cannot reach candle storage or order-flow aggregation.

### H-04 — Bootstrap “completeness” does not prove temporal completeness

**Evidence.**

- Bootstrap admission at <code>src/signalbot/runtime.py:172-184</code> filters for closure, market, symbol, and configured interval, then inserts the sorted records. It does not require unique, strictly contiguous interval slots.
- REST kline parsing at <code>src/signalbot/exchange/binance/rest.py:191-218</code> parses list rows and removes candles not yet closed relative to <code>now_ms</code>, but it does not prove an exact returned time grid.
- Feature construction at <code>src/signalbot/indicators/core.py:267-279</code> checks length and closure, not adjacency.
- The value named <code>data_completeness</code> is simply 70 base points, 20 for canonical flow availability, and 10 for spread availability at <code>src/signalbot/indicators/core.py:567-572</code>.
- The gate converts that value to a percentage and compares it with a threshold at <code>src/signalbot/signals/gates.py:327-362</code>.

Live ingestion does detect a gap relative to the latest stored candle, but that does not repair an internal hole already present in one bootstrap response.

**Impact.** A sequence with a missing internal candle can satisfy minimum length, produce indicators as though nonadjacent observations were adjacent, and display 100% completeness when flow and spread are present. This can distort EMA, ATR, volatility, range, momentum, and trigger timing while overstating evidence quality.

**Required improvement.**

1. Validate strict increasing order and uniqueness.
2. Validate exact interval geometry for every adjacent open time and expected close time.
3. Bind the requested REST window, returned first/last slot, and expected count.
4. Recover missing pages/slots before feature reconstruction, or mark the symbol/timeframe not ready.
5. Rename the current score to feature-source availability and introduce a separate temporal/source completeness contract.

**Exit criterion.** Positive, missing-middle, duplicate, overlapping, off-grid, wrong-close-time, partial-page, and boundary-window tests prove fail-closed bootstrap admission. Readiness cannot become true until the entire required indicator prefix and higher-timeframe context have contiguous, closed, authoritative slots.

### H-05 — V1 state and PAPER lifecycle are not restart-continuous

**Evidence.** <code>src/signalbot/signals/state_machine.py:13-24</code> stores stage, cooldown, and invalidation in an in-memory dictionary. Lines 58-89 apply transitions and cooldowns. <code>src/signalbot/runtime.py:88-95</code> constructs a new in-memory PAPER lifecycle at startup. The design is explicitly documented at <code>docs/SIGNAL_SPEC.md:90-95</code> and <code>docs/OPERATIONS.md:35-41</code>: restart forgets pending entries and open PAPER positions.

Deterministic persisted event IDs prevent exact same-identity rewrites, but they do not reconstruct the prior semantic state for the next candle.

**Impact.**

- cooldowns reset and can change alert frequency;
- WATCH/SETUP/CONFIRMED transition semantics become process-uptime-dependent;
- pending or open PAPER positions disappear from lifecycle tracking;
- outcome and censor accounting can become incomplete; and
- operators can mistake restart-induced disappearance for a modeled exit.

No exchange position is affected because these positions are paper-only.

**Required improvement.** Choose one explicit contract:

- persist state-machine and PAPER transitions transactionally and rehydrate them on startup; or
- declare restart a censor boundary, persist a restart-censor record for every affected lifecycle, suppress new transitions during a deterministic warm-up, and make alerts/health display the discontinuity.

**Exit criterion.** Crash-at-every-transaction-boundary integration tests show that restart yields either exact state continuation or an explicit, idempotent censor/reset artifact. Cooldowns, open PAPER positions, outbox records, and outcome accounting cannot silently diverge.

### H-06 — Synchronous persistence blocks asynchronous ingestion

**Evidence.**

- The async candle handler calls synchronous <code>repository.save_candle</code> at <code>src/signalbot/runtime.py:300-332</code>.
- Decision publication synchronously calls <code>save_signal_and_enqueue</code> at <code>src/signalbot/runtime.py:515-541</code>.
- Bootstrap coroutines synchronously call <code>save_candles</code> after each REST response at <code>src/signalbot/scanner.py:455-476</code>.
- SQLAlchemy uses a synchronous engine/session at <code>src/signalbot/persistence/repository.py:458-483</code>.
- Batch save runs one transaction but calls an existing-row SELECT/upsert for every candle at lines 1091-1105.
- The open repair plan acknowledges remaining blocking database work at <code>docs/runtime-repair-plan-2026-08-17.md:49-54</code>.

**Impact.** Lock contention, slow storage, PostgreSQL latency, or a large bootstrap/gap-recovery batch can block the event loop. That increases WebSocket queue age, makes BBO and market context stale, delays shutdown, and can convert a transient database slowdown into input gaps.

**Required improvement.** Introduce a single bounded persistence owner:

- one serialized writer queue for SQLite, with item and byte limits;
- explicit backpressure and fatal/censor behavior when the queue cannot admit data;
- dialect-native bulk insert/upsert with immutable conflict comparison;
- a transactional envelope for related signal/outbox/state changes;
- no unconstrained <code>to_thread</code> fan-out that creates concurrent SQLite writers; and
- measured database latency, queue age, and ingestion lag.

A true async PostgreSQL repository can be a later implementation behind the same port, but the queue and semantic transaction boundaries should be defined first.

**Exit criterion.** Under a recorded burst, induced 2-second database stalls, gap recovery, and shutdown cancellation, the event loop stays within the declared lag SLO, queues remain bounded, no admitted record is silently lost, and every failure produces a deterministic health/censor state.

### H-07 — PostgreSQL schema evolution is unsupported

**Evidence.** Startup calls <code>Base.metadata.create_all</code> at <code>src/signalbot/persistence/repository.py:471-483</code>. SQLAlchemy <code>create_all</code> creates missing tables but does not alter an existing table into a new model. The only in-place shadow-schema helper returns immediately for non-SQLite dialects at lines 256-260. PostgreSQL is nevertheless a documented deployment target in <code>docs/adr/0004-storage-portability.md:1-3</code> and <code>docker-compose.yml:1-20</code>. No PostgreSQL upgrade test was found; ordinary repository tests use SQLite.

**Impact.** A clean PostgreSQL database may initialize, while an older real deployment retains missing or incompatible columns and fails only when a newer query or write is exercised. This is a release and rollback risk, not a Phase R SQLite defect.

**Required improvement.**

1. Add an explicit schema version and Alembic-style forward migrations.
2. Refuse startup when the binary expects an unsupported schema.
3. Test upgrades from every supported prior release and test downgrade/rollback policy.
4. Add PostgreSQL service-container integration tests for constraints, transactions, outbox claiming, isolation, and migration.
5. Back up and verify before applying destructive or long-running migrations.

**Exit criterion.** A CI matrix creates each supported historical schema, migrates it to current, verifies row/hash preservation and application operations, and exercises the documented rollback or forward-only recovery procedure on the same PostgreSQL major version used in deployment.

### H-08 — Unknown storage modes silently downgrade evidence recording

**Evidence.** <code>RuntimeSettings.storage_mode</code> is an unrestricted string at <code>src/signalbot/config.py:289-298</code>. <code>src/signalbot/app.py:33-58</code> selects segmented storage only for exact equality with <code>segmented_zstd_v1</code>; every other string falls through to <code>RawEventRecorder</code> when recording is enabled. A focused probe confirmed that a typo such as <code>segmneted_zstd_v1</code> validates.

**Impact.** An operator intending the stronger segmented evidence path can make a spelling error and silently receive the weaker legacy path. The result changes durability, file format, recovery, and evidence semantics without a startup failure.

**Required improvement.** Make storage mode a closed Literal/enum, reject unknown values, and require explicit mode when raw recording is enabled. Emit effective mode, storage schema, source/config hash, quota, and root path in a sanitized startup receipt and readiness state.

**Exit criterion.** Every supported value has a positive test; every typo/case variant/unknown value has a negative test; the selected concrete recorder type is asserted; and startup cannot record one event before the effective backend identity is durable and observable.

### H-09 — Legacy JSONL durability claims exceed actual guarantees

**Evidence.**

- <code>src/signalbot/data/raw_events.py:150-168</code> writes and calls <code>flush()</code>, but never <code>os.fsync()</code>.
- Lines 142-146 count the batch as durable immediately afterward.
- Status and drain APIs call the counter <code>durable_records</code> at lines 327-363.
- On join timeout, lines 227-248 explicitly leave the shielded writer task detached and then close the handle defensively.
- The first append performs a synchronous recursive directory-size scan at lines 315-320 and 366-381.

The active Phase R candidate selects segmented storage, so this finding applies only to <code>legacy_jsonl_v1</code> or an accidental fallback into it.

**Impact.** Process flush is not power-loss durability. Records reported as durable may remain only in the OS cache. A slow or hung write can also outlive task ownership while shutdown closes the handle, creating an ambiguous terminal state. The first append can pause the event loop on a large directory.

**Required improvement.**

- Rename the current watermark to written/flushed unless fsync semantics are added.
- Fsync at a bounded cadence and on segment/day closure, including parent-directory durability where relevant.
- Retain writer-task ownership until a terminal success or fatal evidence-loss receipt exists.
- Return a nonzero/fatal shutdown result when evidence cannot be sealed.
- Replace recursive first-append scans with an initialized manifest/index or perform them off-loop before admission opens.
- Prefer the segmented recorder for any campaign that requires forensic evidence.

**Exit criterion.** Power-cut/fault-injection tests distinguish accepted, queued, written, fsynced, and sealed watermarks; restart recovers only the documented durable prefix; a hung writer cannot mutate a closed handle; and shutdown cannot report success without a terminal evidence receipt.

## 8. Medium-severity limitations

### M-01 — Pinned cryptography dependency has current advisories

**Evidence.** <code>pyproject.toml:14</code> pins <code>cryptography==46.0.3</code>. A fresh audit of the production dependency export returned nine vulnerability rows representing seven unique advisory identifiers. Reported fixed versions ranged from 46.0.5 to 50.0.0. The project directly uses cryptography for Ed25519 block signing/verification at <code>src/signalbot/r4b_v2/capture/block_container.py:13-18</code>.

The maintainer's [security advisory list](https://github.com/pyca/cryptography/security/advisories) includes Low, Moderate, and High issues published after 46.0.3. The [official changelog](https://cryptography.io/en/latest/changelog/) records fixes in later versions. One reported issue concerns uncommon binary elliptic curves, while the inspected project use is Ed25519; other advisories concern X.509, PKCS#7, Python buffers, and bundled OpenSSL. This audit did not establish exploit reachability in the bot.

**Impact.** A known-vulnerable cryptographic package is an avoidable supply-chain and release risk even when the currently identified call path is not demonstrably exploitable. Keeping an exact old pin also prevents normal security resolution.

**Required improvement.** Upgrade to the lowest compatible version that clears every current advisory—at audit time, the highest reported minimum fix was 50.0.0—then regenerate <code>uv.lock</code>. Run signature generation/verification, canonical block compatibility, old-artifact verification, wheel/platform, and container tests. Add recurring dependency audit and advisory triage to CI; record accepted residuals with expiry rather than silently ignoring them.

**Exit criterion.** Production and development lock exports return zero unwaived known advisories; all waivers name reachability evidence, owner, expiry, and compensating control; and old signed evidence still verifies under the declared compatibility contract.

### M-02 — FAST operational-health scan misses its own latency target

**Evidence.** The test at <code>tests/unit/test_oci_operational_health.py:303-353</code> creates 5,000 then 10,000 manifests and requires each health scan to finish in under 30 seconds. Fresh full-suite execution measured the first scan at 39.861 seconds. A focused rerun measured 38.990 seconds. Both failed at line 352. The implementation at <code>tools/oci_operational_health.py:697-708</code> still glob-sorts the complete directory on each scan, and lines 720-806 synchronously read manifests, stat files, and hash all new data files.

**Impact.** As evidence grows, health checks can exceed their intended cadence, overlap, produce stale status, or consume disproportionate filesystem I/O. The “incremental” cursor avoids rehashing old data but not full directory enumeration/sorting, and each large new batch remains expensive.

**Required improvement.** Use an authenticated append index or partitioned manifest hierarchy; persist the last verified directory/page cursor and cumulative totals; bound work per health cycle; and separate quick freshness/readiness checks from deep integrity sweeps. Preserve tamper detection by binding index entries to the underlying manifests and periodically revalidating sampled or sealed partitions.

**Exit criterion.** On every supported filesystem/platform, a 10,000-manifest fixture completes under the declared limit with headroom, a larger asymptotic test demonstrates bounded incremental cost, tampered/skipped/reordered manifests are still detected, and monitor overlap is impossible.

### M-03 — Conflicting finalized candles silently rewrite history

**Evidence.** <code>src/signalbot/data/candles.py:42-56</code> treats an exact same-open-time candle as a duplicate but replaces the stored candle when any field differs. <code>src/signalbot/persistence/repository.py:420-455</code> similarly updates every persisted field for an existing market/symbol/interval/open-time row. A focused probe inserted two different closed candles for one slot and observed both admissions succeed with the second close becoming authoritative.

**Impact.** A provider correction, corrupted replay, or reconnect conflict can change the input history after an earlier decision was created. Existing decisions remain immutable, but a later replay can compute from different candles without an explicit correction record, weakening forensic reproducibility.

**Required improvement.** Treat exact finalized duplicates as no-ops and nonidentical finalized duplicates as hard evidence conflicts by default. Quarantine the competing payload with source/receipt/cursor lineage. If corrections are a supported business rule, model them as immutable versions with an explicit correction authority and “as known at” replay semantics.

**Exit criterion.** Positive, exact-duplicate, conflicting-OHLCV, conflicting-close-time, REST-versus-WebSocket, and reconnect tests prove that no finalized input is silently overwritten and every decision can resolve its exact candle version.

### M-04 — Sparse anomaly sampling can mislabel the measured horizon

**Evidence.** <code>src/signalbot/data/anomaly.py:73-85</code> retains a broad window, selects the point nearest <code>now - horizon</code>, and checks only that the anchor is no later than target plus five seconds. There is no symmetric lower staleness bound. Because the current point competes in the nearest-point selection, the chosen anchor is not unboundedly old, but the actual span can approach twice the configured horizon. A focused 30-second-horizon probe emitted a “30s return +2.00%” reason from a 55-second-old anchor. Existing regular-cadence tests do not cover that sparse boundary.

**Impact.** <code>PUMP_RISK</code>/<code>CRASH_RISK</code> warnings can state one horizon while measuring another. These are warnings rather than entry orders, but mislabeled evidence makes thresholds, operator interpretation, and backtest/live comparison inconsistent.

**Required improvement.** Require an anchor within an explicit two-sided tolerance, require minimum sampling coverage/cadence, and carry the actual interval into metadata and human-readable reasons. Decide whether the valid anchor must be at-or-before target to avoid using information newer than the nominal start.

**Exit criterion.** Boundary tests cover exact target, both tolerance edges, sparse intervals, duplicates, out-of-order events, and no-anchor conditions. A warning either reports an interval within tolerance or is suppressed with a visible insufficient-history reason.

### M-05 — Universe age authority is restart-expensive, unbounded, and brittle

**Evidence.**

- <code>src/signalbot/exchange/binance/universe.py:43-48</code> holds Spot age anchors in a process-local dictionary with no persisted provenance, pruning, or maximum size.
- Lines 70-125 resolve one uncached symbol at a time by awaiting the earliest daily kline in a sequential loop.
- <code>config/settings.example.yaml:7-11</code> allows a 200-symbol surveillance panel, so a cold refresh can require many public REST calls.
- Futures onboarding conversion uses <code>int(onboard_raw)</code> without a field-specific parse guard at <code>src/signalbot/exchange/binance/universe.py:178-202</code>. One malformed non-null field can raise and abort the selection loop.
- The persistent Spot age gap is already acknowledged at <code>docs/runtime-repair-plan-2026-08-17.md:43-48</code>.

**Impact.** Startup and refresh become rate-sensitive, age evidence is lost on every restart, stale/delisted symbols can remain cached indefinitely, and one malformed upstream row can terminate a refresh instead of quarantining that instrument.

**Required improvement.** Persist age authority with source, fetched-at time, and first-candle identity; prune against the current exchange universe and a hard cap; add bounded-concurrency lookups under the global REST budget; use bounded negative caching; and strictly parse each upstream field with per-symbol rejection.

**Exit criterion.** Cold-start, warm-start, delisting, malformed onboarding, rate-limit, partial-response, and cache-prune tests demonstrate a bounded call count and memory footprint. A single bad row cannot kill the refresh task, and every accepted age has durable provenance.

### M-06 — REST rate limiting is reactive and local to each client

**Evidence.** Each <code>BinanceRestClient</code> owns its own lock, embargo deadline, and observed weight headers at <code>src/signalbot/exchange/binance/rest.py:45-64</code>. Lines 107-118 record provider weight headers, but they are not used for proactive request admission. Lines 130-177 react to HTTP 418/429 with a capped embargo/retry loop. Separate scanners create separate market clients.

**Impact.** Spot, Futures, bootstrap, gap recovery, universe age, funding, and future depth requests can collectively consume an IP-scoped budget even when each client believes it is healthy. The first effective coordination happens after the provider rejects or bans requests.

**Required improvement.** Introduce a process-wide weighted rate-budget owner keyed by provider scope and endpoint weight. Reserve capacity before requests; reconcile against returned weight headers; enforce Retry-After and a bounded circuit state; prioritize freshness-critical recovery over background enrichment; and expose remaining budget/embargo metrics without logging secrets.

**Exit criterion.** A deterministic multi-client simulation never exceeds configured minute/second budgets, honors 418/429 across all callers, cancels cleanly, and demonstrates bounded recovery after embargo.

### M-07 — V1 rule provenance depends on manual version discipline

**Evidence.** <code>src/signalbot/config.py:301-305</code> defines a free string <code>rule_version</code>. Event identity includes that string at <code>src/signalbot/signals/state_machine.py:150-162</code>, and decisions store it at <code>src/signalbot/domain/models.py:393-409</code>. <code>docs/OPERATIONS.md:23-28</code> instructs operators to choose a new version whenever the rule contract changes. There is no canonical V1 hash binding effective thresholds, policy selection, relevant feature configuration, and parser semantics to the decision.

**Impact.** Two configurations can emit semantically different decisions under the same version label if an operator forgets to update it. Conversely, changing only a cosmetic field can create an unnecessary version. This complicates replay comparison and audit.

**Required improvement.** Define a canonical decision-contract projection and SHA-256 over every semantically relevant rule, feature, context, parser, and execution-gate setting. Store both a human release label and the machine contract hash in decisions, outbox payloads, campaign manifests, and startup receipts. CI should require an intentional version transition when the projection changes.

**Exit criterion.** Property tests show that every semantic field changes the hash, irrelevant operational fields do not, equivalent YAML orderings produce the same hash, and replay refuses to compare records with incompatible contracts without an explicit mapping.

### M-08 — An active PAPER lifecycle can defer the entire universe rotation

**Evidence.** <code>src/signalbot/scanner.py:230-244</code> computes outgoing symbols, intersects them with PAPER continuation symbols, clears the pending rotation candidate, and returns without applying any rotation whenever the intersection is nonempty.

**Impact.** A long-lived, stale, or delisted PAPER symbol can prevent unrelated new high-liquidity symbols from entering the tradable panel. If the old symbol stops receiving usable candles, the lifecycle may never reach its ordinary terminal condition.

**Required improvement.** Separate ranking adoption from lifecycle continuation. Keep a small bounded continuation-only subscription for outgoing PAPER symbols while rotating the main tradable panel, or enforce a deterministic censor/terminal timeout when the source can no longer support lifecycle evaluation. Count continuation symbols independently against a hard resource budget.

**Exit criterion.** Tests cover a normal exit, prolonged position, delisting, feed disappearance, reconnect, and maximum continuation capacity. The main universe rotates on schedule, while every displaced lifecycle either completes or receives an explicit censor record.

### M-09 — Discord uncertainty is safely quarantined but operationally incomplete

**Evidence.** The outbox model correctly quarantines ambiguous outcomes, but <code>docs/OPERATIONS.md:49-69</code> requires manual channel reconciliation. <code>pending</code>, <code>sending</code>, and <code>uncertain</code> rows all count against <code>outbox_max_active_items</code>; reaching the limit refuses the new signal/outbox transaction at <code>src/signalbot/persistence/repository.py:1171-1186</code>. The repair plan still lists operator tooling and metrics as open at <code>docs/runtime-repair-plan-2026-08-17.md:51-54</code>.

**Impact.** A Discord outage or ambiguous response cluster can accumulate uncertain rows until the hard cap stops new signal persistence. The safe “never blind retry” policy is correct, but a manual-only resolution path increases recovery time and human error.

**Required improvement.** Add a read-only inspection command and an evidence-gated reconciliation command. Resolution should require event ID, payload hash, provider message ID or explicit operator evidence, actor, time, and reason. Expose counts and oldest age by state, cap utilization, delivery latency, and dead/uncertain alerts. Never add automatic ambiguous retries.

**Exit criterion.** Integration tests cover transport loss before/after provider acceptance, crash while sending, 2xx without message ID, 429 exhaustion, manual delivered/dead resolution, and duplicate operator action. Every transition is append-only auditable and cap pressure is visible before admission stops.

### M-10 — Generic readiness is too shallow

**Evidence.** <code>src/signalbot/api/server.py:8-21</code> reports live unconditionally and ready whenever <code>repository.ready</code> is true. It does not evaluate:

- WebSocket connection and last-message freshness;
- primary/HTF candle freshness and unresolved gaps;
- parser error rate;
- event-loop or persistence queue lag;
- evidence-recorder fatal state, queue age, or disk runway;
- Discord outbox pressure; or
- whether the effective source/config/rule identities match the expected deployment.

Specialized capture and campaign health artifacts exist elsewhere in the repository. The limitation is that those signals are not composed into the generic service readiness contract.

**Impact.** An orchestrator or operator can see “ready” while the service has a stale feed, blocked database writer, failed raw recorder, exhausted disk, or saturated outbox.

**Required improvement.** Define separate liveness, startup readiness, data readiness, evidence health, and notification health. Compose them into a fail-closed overall status with reason codes and age. Export bounded metrics or structured health receipts without exposing URLs, tokens, payload contents, or proprietary signal detail.

**Exit criterion.** Fault-injection tests independently stop each critical dependency and prove the correct readiness degradation and recovery. A healthy status binds the effective source/config/rule identities and all freshness/queue/runway thresholds.

### M-11 — CI and test topology leave deployment and scientific gaps

**Evidence.**

- <code>.github/workflows/ci.yml:10-26</code> runs only on <code>ubuntu-latest</code>.
- Dependency synchronization omits <code>--frozen</code>.
- CI runs Ruff, Pyright, pytest, compileall, config validation, dry run, and a small replay, but no PostgreSQL migration test, dependency audit, SAST, secret scan, SBOM, coverage threshold, mutation test, load test, fault injection, or artifact publication.
- <code>docs/BACKTEST_SPEC.md:69-75</code> requires Freqtrade <code>lookahead-analysis</code> and <code>recursive-analysis</code>; CI compiles the sidecar but does not run those analyses.
- <code>pyproject.toml:57-63</code> uses Pyright standard mode, excludes integrations, and suppresses missing-stub and unknown-member diagnostics.
- No global <code>pytest-socket</code> or equivalent network-disable fixture was found. Unit tests generally use fakes, but the “no network in unit tests” invariant is convention rather than enforced isolation.
- The working tree has 228 Python test files: 222 unit, 2 integration, 1 contract, 1 benchmark, 1 replay, and one conftest. That is excellent unit depth but a narrow deployment-realistic surface.
- The Windows run skipped 21 cases, mostly symlink, open-inode replacement, and POSIX-only behavior. The remaining performance test failed reproducibly.

**Impact.** PostgreSQL evolution, Windows behavior, packaging drift, dependency vulnerabilities, untested network escape, concurrency races, and causal-analysis regressions can escape an otherwise large suite.

**Required improvement.**

1. Use <code>uv sync --frozen</code>.
2. Add supported-platform CI, or explicitly declare runtime support as Linux/WSL and keep Windows as editor-only.
3. Add PostgreSQL migration and transactional integration jobs.
4. Enforce unit-test network denial.
5. Add coverage plus diff-coverage and mutation checks focused on provider parsers, causal gates, idempotency, and state transitions.
6. Run the declared Freqtrade lookahead and recursive analyses on fixed fixtures.
7. Add dependency audit, secret/history scan, SAST, SBOM, and container scan.
8. Add bounded burst, database-stall, reconnect, shutdown, and storage fault tests.
9. Publish JUnit, coverage, benchmark, and build-provenance artifacts.
10. Pin CI actions and runtime images according to an explicit supply-chain policy; the Python Docker base is digest-pinned, while <code>postgres:17-alpine</code> is tag-only.

**Exit criterion.** A clean clone passes the same required matrix on every declared platform; PostgreSQL upgrade tests pass; no unit test can reach the network; security/coverage gates are explicit; and scientific sidecar analyses produce retained, version-bound receipts.

### M-12 — Structural complexity is exceptionally high

**Evidence.** A mechanical census of the current tree found:

| Metric | Source | Tests | Tools |
| --- | ---: | ---: | ---: |
| Python files | 237 | 228 | 38 |
| Physical lines | 213,058 | 125,723 | 9,835 |
| Files over 400 lines | 147 | 113 | 8 |
| Files over 1,000 lines | 77 | 35 | 1 |
| Files over 2,000 lines | 32 | 5 | 0 |

All 237 source modules parsed successfully. The AST census found 6,906 functions, including 920 over 50 lines and 255 over 100 lines; 490 functions had more than five parameters. It found 1,440 classes, including 22 with more than 20 methods. Examples include:

- <code>src/signalbot/backtest/r2.py:1327</code>, where <code>analyze_r2_retrospective</code> is approximately 772 lines with 18 parameters;
- <code>src/signalbot/r4b_v2/capture/rest_depth_bridge.py:177</code>, where <code>PublicDepthRestBridgeCoordinatorV8</code> has about 56 methods;
- <code>src/signalbot/r4b_v2/capture/integrity_ledger.py</code>, over 5,000 physical lines;
- <code>src/signalbot/persistence/repository.py:458</code>, where one repository class spans many storage domains.

A source census also found 1,026 versioned V/R-style function or class identifiers, 186 broad <code>Exception</code>/<code>BaseException</code> catch sites, and 37 <code>type: ignore</code> comments. No bare <code>except</code> and no runtime top-level import cycle were established.

**Interpretation.** Size alone is not a defect. Much of R4B V2 is deliberately immutable, versioned evidence code, and rewriting frozen contracts could destroy reproducibility. The risk is the combination of version density, very large active owners, repeated validation/serialization patterns, and unclear boundaries between runtime product code, frozen research protocols, generated evidence, and operations tooling.

**Required improvement.**

- Keep immutable frozen contracts intact and version-addressable.
- Separate active runtime, research archive, evidence schemas, generated artifacts, and operator tools into explicit packages/release surfaces.
- Refactor only active owners behind compatibility-preserving ports.
- Split validation, canonical serialization, persistence, state transition, and transport responsibilities.
- Generate repetitive schema/registry code only when the generator and generated hash are themselves authoritative.
- Add architecture ownership maps and automated dependency-direction checks.
- Narrow broad exception handling to documented failure taxonomies where feasible.

**Exit criterion.** New changes no longer add responsibilities to the largest owners; dependency rules are machine-checked; active modules have named owners and stable ports; frozen modules remain reproducible; and complexity trends are reported without using arbitrary line-count deletion as a goal.

## 9. Low-severity and defense-in-depth limitations

### L-01 — API and webhook boundaries rely on deployment discipline

<code>src/signalbot/api/server.py:19-21</code> serves recent signals without application authentication. <code>docker-compose.yml:28-46</code> binds the API port to loopback, which is a meaningful default mitigation, but any external reverse proxy or <code>0.0.0.0</code> deployment needs authenticated TLS termination, authorization, request limits, and audit logging.

<code>src/signalbot/config.py:258-280</code> accepts any absolute HTTPS hostname for a setting named Discord webhook. This is operator-controlled rather than attacker-controlled in the observed design, but a bad value can send proprietary signal payloads to the wrong host. Either restrict supported Discord hostnames or rename the field as a generic webhook and require an explicit allowlist.

**Exit criterion.** Default documentation keeps the API loopback-only; external deployment has an authenticated reference architecture; webhook destination policy is explicit; and tests prove secrets and full webhook URLs never appear in logs or health output.

### L-02 — Relational integrity is enforced mainly in application code

<code>src/signalbot/persistence/models.py:34-88</code> uses common event IDs across signals, alert attempts, outbox rows, and outcomes but defines no database foreign keys among them. Application transactions preserve important relationships, especially signal plus outbox, but manual repair, partial migration, or an auxiliary writer can create orphans.

Add foreign keys where retention/deletion semantics permit, or add a mandatory consistency verifier when cross-dialect portability makes constraints impractical. Define deletion policy explicitly; evidence tables should not accidentally cascade away forensic history.

**Exit criterion.** Orphan creation is rejected or detected, migration tests preserve relationships, and a documented repair procedure never mutates immutable signal payloads.

### L-03 — Some nominal bounds permit excessive byte use

The V1 WebSocket client defaults to 2,048 queued messages and a 4 MiB maximum message size at <code>src/signalbot/exchange/binance/websocket.py:20-50</code>. The theoretical upper bound is therefore large, even though typical Binance frames are much smaller. Legacy and segmented recorders also have high count limits; count-bounded is not always memory-bounded.

Add encoded-byte budgets, high/low watermarks, queue-age metrics, and overload policies. Preserve fail-closed evidence semantics: shedding unrecorded market data must stop or censor downstream decisions rather than silently continuing.

**Exit criterion.** Burst tests enforce both item and byte caps, memory stays within a declared process budget, and every overflow has a deterministic stop/censor outcome.

### L-04 — Legacy defaults and stale documentation create policy ambiguity

<code>src/signalbot/config.py:200-206</code> defaults <code>entry_policy</code> to <code>legacy_gates</code>, while <code>config/settings.example.yaml:47</code> explicitly selects <code>r2_pit_htf_exec</code>. A missing production field therefore selects the older contract. The example also contains a comment at lines 102-104 describing selection of <code>shadow_er_context_v1</code> as a legacy mode, while the current Literal no longer accepts that value.

Require explicit policy selection in production/campaign configurations, or make the safe frozen R2 path the only production default. Maintain one versioned status/index document and label old plans as historical rather than leaving obsolete pass counts and packaging assumptions in apparently current files.

**Exit criterion.** Omitted or unknown policy fails validation for production profiles; effective policy and contract hash are visible at startup; and documentation checks identify stale status claims and invalid configuration examples.

## 10. Security and safety posture

### 10.1 What is currently sound

- The scanner uses public Binance market data and no Binance key is required for V1.
- No production order-placement or private-account dependency was found.
- Discord URL material is represented as <code>SecretStr</code>, and the inspected configuration/CLI paths do not intentionally print the secret.
- Docker runs as a non-root user, and the Python base image is pinned by digest at <code>Dockerfile:1-15</code>.
- The API is bound to loopback in the supplied Compose file.
- A targeted tracked-file search found no obvious AWS access key, private-key header, or concrete Discord webhook secret.
- Retry loops inspected in the main REST path are capped, and there is no bare <code>except</code> in source.

### 10.2 What remains incomplete

- The cryptography pin has current advisories (M-01).
- CI has no dependency audit, SAST, history-aware secret scan, SBOM, provenance attestation, or container vulnerability scan (M-11).
- The API relies on network binding rather than application authentication (L-01).
- Webhook hostname policy is permissive (L-01).
- GitHub Actions are pinned to major tags rather than immutable action SHAs, and PostgreSQL is tag-pinned rather than digest-pinned.
- The dirty working tree prevents a meaningful signed release/provenance claim (H-02).

The targeted negative secret scan must not be interpreted as proof that Git history, ignored files, external deployment variables, or already published artifacts are clean. A release gate should scan the full reachable history and the built image while preserving secret values from logs.

## 11. Claims considered and rejected or qualified

A useful audit must record false positives as well as defects.

### 11.1 “The Binance Futures routes are stale” — rejected

Official Binance documentation checked on the audit date says public market-data streams are split between <code>wss://fstream.binance.com/market</code> and <code>wss://fstream.binance.com/public</code>, with combined streams under <code>/stream?streams=</code>. Kline, aggregate-trade, and mini-ticker streams use market; book ticker/depth streams use public. The implementation at <code>src/signalbot/exchange/binance/endpoints.py:13-14</code> and 92-127 matches that current routing. The official notice also states that legacy endpoints were decommissioned on 23 April 2026.

### 11.2 “The main feature path has obvious ordinary lookahead” — rejected

The runtime closure gate and strict-prior HTF lookup are present, feature construction uses closed prefixes, and no centered-window or future-row defect was established. H-03 remains material because malformed Boolean input can bypass the closure gate before those controls.

### 11.3 “Discord delivery is generally duplicate-prone” — rejected

The outbox design is conservative and treats ambiguous provider outcomes as uncertain instead of retrying blindly. M-09 is an operator-control and observability issue, not evidence that the existing transport automatically duplicates messages.

### 11.4 “Sparse anomaly anchors can be arbitrarily old” — rejected

The current point competes as the nearest anchor, so the effective span is bounded near twice the configured horizon rather than being unbounded. M-04 is precise horizon mislabeling.

### 11.5 “The repository has a proven runtime import cycle” — not established

A top-level runtime import graph that excluded <code>TYPE_CHECKING</code>-only edges found no cycle across the 237 source modules. Coupling and size remain real, but an import-time cycle should not be claimed without a reproducible failure.

### 11.6 “The legacy JSONL weakness affects the exact Phase R candidate” — rejected

The candidate file selects segmented storage and disables Discord. H-09 applies to the legacy path and H-08 to accidental fallback. The audit did not independently verify which configuration a remote process actually loaded, so it also does not claim that Phase R is definitely protected in deployment.

### 11.7 “R4B V2 engineering checks prove strategy efficacy” — rejected by project governance

The completion matrix itself rejects this inference. Strong causal/evidence code and thousands of tests are necessary but not sufficient for after-cost, prospective performance.

## 12. Dependency-ordered improvement roadmap

This roadmap is ordered by architectural dependency, not by estimated effort. Later phases consume the verified outputs of earlier phases. Production order execution remains outside scope.

### Phase 0 — Establish one reproducible source and evidence boundary

**Depends on:** nothing.

**Work.**

1. Inventory every staged, unstaged, and untracked path.
2. Classify runtime source, frozen research source, evidence, generated output, local orchestration state, and scratch material.
3. Repair the local environment workflow and require frozen dependency sync.
4. Decide the supported runtime platforms; if Windows is not supported, document WSL/Linux explicitly.
5. Resolve all Ruff and test failures without deleting authoritative evidence.
6. Create a canonical commit/tag and manifest binding source, configuration, rule/research contracts, dependency lock, and container digest.
7. Replace stale status documents with a current index that links historical plans as historical.

**Exit gate.** A clean clone produces zero status changes after sync/test/build, all official checks pass, and an independently recomputed source/config/lock manifest matches the release artifact.

### Phase 1 — Close market-data trust boundaries

**Depends on:** Phase 0's frozen baseline and test environment.

**Work.**

1. Add strict Boolean parsing and provider-field error taxonomy.
2. Add exact bootstrap candle-grid validation and recovery.
3. Separate temporal completeness from feature-source availability.
4. Make finalized candle conflicts immutable and auditable.
5. Correct anomaly horizon tolerance and report actual span.
6. Harden universe onboarding parsing and bounded age authority.
7. Bind V1 decisions to a canonical decision-contract hash.

**Exit gate.** Recorded-fixture, property, and boundary tests prove that malformed types, missing slots, duplicates, conflicts, off-grid timestamps, sparse anchors, and semantic config changes cannot silently alter a valid decision.

### Phase 2 — Make state and persistence restart-safe

**Depends on:** stable domain contracts from Phase 1.

**Work.**

1. Define the authoritative persisted state/censor model for alert cooldown and PAPER lifecycle.
2. Introduce a single bounded persistence owner and remove blocking SQL from ingestion callbacks.
3. Add immutable conflict-aware bulk persistence.
4. Add versioned PostgreSQL migrations and startup schema enforcement.
5. Add relational consistency constraints or verifiers.
6. Repair legacy recorder watermarks/close behavior or formally deprecate that backend.
7. Reject unknown storage modes and persist the effective backend identity.
8. Complete the safe monotonic-time representation before the R4B long-duration deadline.

**Exit gate.** Crash/restart, database-stall, migration, power-loss, cancellation, and replay tests preserve the exact admitted prefix and yield either exact continuation or explicit censor/fatal artifacts.

### Phase 3 — Compose operational health and operator controls

**Depends on:** Phase 2's explicit state, queues, and durability watermarks.

**Work.**

1. Define liveness, startup readiness, data readiness, evidence health, and notification health.
2. Export stream/candle/BBO freshness, unresolved gaps, parser errors, event-loop lag, DB latency, queue items/bytes/age, disk runway, and outbox pressure.
3. Replace full-directory FAST scan behavior with a bounded authenticated index/partition design.
4. Add global proactive Binance REST budgeting.
5. Add safe Discord uncertain-row inspection and evidence-gated resolution.
6. Separate main universe rotation from bounded PAPER continuation.
7. Add alerts and runbooks for every fatal/degraded state.

**Exit gate.** Fault injection for every dependency changes health within a declared detection time, never exposes a secret, and returns to healthy only after the underlying invariant is restored.

### Phase 4 — Make verification and supply-chain controls release-grade

**Depends on:** observable failure modes and stable migration/runtime contracts from Phases 2-3.

**Work.**

1. Add the declared platform matrix or formalize a narrower support policy.
2. Add PostgreSQL, reconnect, load, soak, storage, and shutdown integration jobs.
3. Mechanically block unit-test network access.
4. Add targeted mutation testing and coverage/diff-coverage.
5. Run Freqtrade lookahead and recursive analyses on fixed, versioned fixtures.
6. Upgrade cryptography, regenerate the lock, and enable continuous dependency triage.
7. Add history-aware secret scanning, SAST, SBOM, image scanning, and build provenance.
8. Retain test, coverage, benchmark, and security receipts tied to the source manifest.

**Exit gate.** Every release candidate passes the complete matrix from a clean clone, produces immutable verification artifacts, and has zero unexpired unowned security exceptions.

### Phase 5 — Complete scientific qualification without contaminating evidence

**Depends on:** frozen, reproducible, observable, and migration-safe producer from Phases 0-4.

**Work.**

1. Finish the declared M2 source census and finality chain.
2. Finish session closure, fees, funding, execution, mandatory exits, isolated NAV, and actionability authority.
3. Produce actual 24-hour capture/WAL qualification.
4. Produce fresh 30-day qualification and a pre-T0 final seal.
5. Preserve the current Phase R/Phase S outcome-blind contract and preregister every later analysis.
6. Run the untouched 365-day prospective evaluation.
7. Apply chronological, block-bootstrap, multiplicity, and calibration requirements exactly as registered.
8. Publish negative, inconclusive, or positive results with equal provenance.

**Exit gate.** The completion matrix is satisfied by actual artifacts rather than code that could create them. Only then may stakeholders discuss research promotion. Any future testnet or account-aware work requires a separate explicit requirement and security design; production order placement is still excluded.

## 13. Proposed operational KPIs and acceptance measures

These are engineering controls, not profitability targets. Exact latency thresholds should be frozen after a representative baseline and before using them as release gates. Existing preregistered research thresholds remain authoritative and must not be changed to make current observations pass.

| Domain | Measure | Proposed acceptance rule |
| --- | --- | --- |
| Reproducibility | Clean source/config/lock/container identity | 100% match across CI, startup receipt, and deployed artifact |
| Static/test gates | Ruff, Pyright, pytest, compileall | Zero failures on every declared platform |
| Provider parsing | Wrong-type and missing critical fields | 100% rejected before domain admission; reason counted |
| Candle closure | Open/unclosed candle admissions | Zero |
| Bootstrap continuity | Missing, duplicate, overlapping, or off-grid slots | Zero unexplained gaps in every required indicator prefix |
| Finalized candle integrity | Conflicting slot overwrite | Zero silent overwrites; every conflict quarantined |
| Causal context | Same/future HTF snapshot consumption | Zero |
| Rule provenance | Decision contract identity | Every decision carries a recomputable matching hash |
| Restart continuity | State/PAPER reconciliation | Every pre-crash lifecycle is restored or explicitly censored exactly once |
| Ingestion responsiveness | Event-loop lag and input-to-durable age | A preregistered p99 budget remains below the shortest freshness dependency, with zero silent drops |
| Persistence pressure | Queue item/byte utilization and oldest age | Bounded below high watermark in steady state; overload yields explicit degraded/fatal state |
| Stream health | Last valid event and unresolved gap age per source | Readiness fails when any required source exceeds its configured freshness/continuity bound |
| REST safety | Weighted budget, embargo, 418/429 count | Zero budget overshoot in deterministic simulation; all clients share embargo |
| Evidence durability | Accepted, written, fsynced, sealed watermarks | Monotonic, reconcilable, and equal at clean shutdown |
| Storage runway | Quota/disk headroom | At least the frozen reserve horizon; Phase S currently uses a 48-hour reserve formula |
| Discord | Pending/sending/uncertain/dead counts and oldest age | Cap pressure alerts before refusal; ambiguous outcomes are never auto-retried |
| Health monitor | FAST receipt duration | Under 30 seconds at the current 10,000-manifest test size on supported hosts |
| Security | Known dependency advisories | Zero unwaived advisories; every waiver owned and expiring |
| Unit isolation | Real network attempts from unit tests | Zero, mechanically enforced |
| Phase S early checkpoint | Calendar span/C0/READY/assets/regimes/censor/concentration | Preserve the frozen T+7 floors in <code>devlog/_plan/260902_phase-s-forward-evidence/000_plan.md:68,86</code>; this is operational evidence, not efficacy |
| Phase S eligibility | 14-day/C0/READY/diversity/independence gates | Preserve the frozen T+14 criteria in <code>devlog/_plan/260902_phase-s-forward-evidence/000_plan.md:68,87-88</code>; passing only authorizes registered analysis |
| Final research | Untouched after-cost confirmation | Meet the preregistered 365-day, cost, coverage, actionability, and multiplicity contract; otherwise no promotion |

## 14. Recommended decision

### Continue

- Continue developing the alert-first, public-data service.
- Continue outcome-blind prospective collection under the frozen campaign contract if independent operational health remains valid.
- Preserve the current causal, deterministic, conflict-loud, and conservative-delivery mechanisms.
- Reuse R4B V2's stronger evidence primitives where they can replace weaker V1/legacy boundaries without rewriting frozen historical contracts.

### Stop or prohibit for now

- Do not claim profit, positive expectancy, calibrated probability, or independent validation.
- Do not promote R4B V2 families into a production decision path.
- Do not deploy a release from the current dirty working tree.
- Do not rely on the generic readiness endpoint as evidence that market inputs and outputs are healthy.
- Do not run a 365-day R4B evidence horizon without resolving the known monotonic-time representation boundary.
- Do not add production order execution. Spot exit alerts and Futures short alerts must remain semantically distinct, and neither should become an exchange action without a new explicit project mandate.

### First implementation sequence

The first concrete implementation cycle should be Phase 0, followed by Phase 1's parser and candle-continuity contracts. That order creates a reproducible baseline before changing decision semantics and closes the two clearest direct data-integrity boundaries. Persistence/state work should follow only after those domain contracts are stable.

## 15. Overall conclusion

The project is a sophisticated experimental signal and evidence platform with unusually strong awareness of causality, idempotency, uncertainty, and research governance. Its main risk is not reckless order execution—the inspected code does not contain it. Its risk is that a very large, rapidly evolving, dirty research/runtime tree can look more operationally and scientifically complete than its actual end-to-end evidence warrants.

The correct next move is disciplined consolidation:

1. make the release boundary reproducible;
2. close provider and candle-history trust gaps;
3. make state, persistence, and storage semantics restart-safe;
4. expose truthful end-to-end readiness;
5. institutionalize cross-platform, migration, security, and scientific gates; and
6. let the preregistered prospective evidence, including a valid negative result, decide whether the strategy deserves promotion.

Until those conditions are met, the system is suitable for controlled observation and research, not for profitability claims or production trading.

## Appendix A — Evidence index

Primary current-source references:

- <code>src/signalbot/exchange/binance/schemas.py:73-107</code> — provider parsing and Boolean coercion.
- <code>src/signalbot/runtime.py:172-184,300-332,494-541</code> — bootstrap, closure gate, strict-prior context, and synchronous persistence.
- <code>src/signalbot/indicators/core.py:267-279,567-572</code> — feature prefix validation and completeness score.
- <code>src/signalbot/data/candles.py:42-56</code> — finalized candle replacement behavior.
- <code>src/signalbot/data/anomaly.py:73-107</code> — anomaly anchor selection.
- <code>src/signalbot/signals/state_machine.py:13-24,58-89,150-162</code> — in-memory state, cooldown, and deterministic event ID.
- <code>src/signalbot/persistence/repository.py:256-260,420-483,1091-1105,1137-1221</code> — migrations, candle upsert, synchronous persistence, and atomic outbox.
- <code>src/signalbot/data/raw_events.py:142-168,185-248,315-381</code> — legacy write, close, accounting, and quota bootstrap.
- <code>src/signalbot/exchange/binance/universe.py:43-48,70-125,167-202</code> — age cache and instrument parsing.
- <code>src/signalbot/exchange/binance/rest.py:45-64,107-177,191-218</code> — throttling and kline retrieval.
- <code>src/signalbot/scanner.py:230-244,455-476</code> — rotation deferral and bootstrap persistence.
- <code>src/signalbot/config.py:200-206,258-305</code> — policy, webhook, storage mode, and rule version.
- <code>src/signalbot/app.py:33-58</code> — recorder selection.
- <code>src/signalbot/api/server.py:8-21</code> — generic health and recent signals.
- <code>src/signalbot/exchange/binance/endpoints.py:8-14,92-127</code> — current endpoint routing.
- <code>.github/workflows/ci.yml:10-26</code>, <code>pyproject.toml:27-63</code>, <code>Dockerfile:1-15</code>, and <code>docker-compose.yml:1-46</code> — verification and deployment.

Primary governance/status references:

- <code>docs/RESEARCH_GOVERNANCE.md:1-76</code>.
- <code>docs/r4b-v2-completion-matrix.md:3-29,108-123</code>.
- <code>docs/SIGNAL_SPEC.md:80-95</code>.
- <code>docs/OPERATIONS.md:23-73</code>.
- <code>docs/ARCHITECTURE.md:6-69</code>.
- <code>docs/BACKTEST_SPEC.md:55-75</code>.
- <code>docs/runtime-repair-plan-2026-08-17.md:26-63</code>.
- <code>devlog/_plan/260902_phase-s-forward-evidence/000_plan.md:1-19,82-92</code>.
- <code>devlog/_plan/260902_phase-s-forward-evidence/041_review-final-validation.md:1-7</code>.

Fresh command evidence:

    uv sync --extra dev --frozen
    uv run --frozen ruff check .
    uv run --frozen ruff check src tests
    uv run --frozen pyright
    uv run --frozen pytest -q
    uv run --frozen pytest -q tests/unit/test_oci_operational_health.py::test_fast_manifest_cursor_scales_and_remains_incremental -s
    uv run --frozen python -m compileall -q src tests
    uv run --frozen signalbot validate-config --config config/settings.example.yaml
    uv run --frozen signalbot run --config config/settings.example.yaml --dry-run
    uv run --frozen signalbot replay --config config/settings.example.yaml --market spot --input tests/fixtures/replay/sample_events.jsonl
    pip-audit against a frozen production dependency export

External primary references checked on 2 September 2026:

- [Binance USDⓈ-M WebSocket base-URL split and migration notice](https://developers.binance.com/en/docs/products/derivatives-trading-usds-futures/websocket-market-streams/Important-WebSocket-Change-Notice).
- [pyca/cryptography security advisories](https://github.com/pyca/cryptography/security/advisories).
- [pyca/cryptography official changelog](https://cryptography.io/en/latest/changelog/).

## Appendix B — Independent review

An independent read-only reviewer challenged the plan and findings in two rounds. It added the bootstrap temporal-completeness and V1 rule-provenance findings, enforced separation of V1, legacy JSONL, R4B V2, and Phase R scopes, and reviewed the fresh dependency/lint/test evidence. Its final normalized result was:

    VERDICT: PASS

This means the report is sufficiently evidence-backed to deliver. It does not mean the repository, release, campaign, or strategy passed its own readiness gates.
