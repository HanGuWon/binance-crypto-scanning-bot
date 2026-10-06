# Complete the repository improvements in bounded, sequential model tasks

Date: 2026-10-06. Status: execution task pack; implementation has not started.

Reader: the project owner and the coding models executing these tasks. Use this pack to assign one task at a time, verify its result, and pass the accepted changes to the next model.

Use GPT-6 Luna for baseline reconciliation, design decisions, independent review, and research evaluation; Sonnet 5.5 for the main implementation; MiniMax 3.1 for narrowly specified presentation, documentation, and fixture work. This is a workflow allocation, not a claim that these models have a proven capability ranking on this repository.

## Contents

- [Execution order and finding coverage](#execution-order-and-finding-coverage)
- [Rules that override suggestions in the supplied audit](#rules-that-override-suggestions-in-the-supplied-audit)
- [Common prompt to prepend to every task](#common-prompt-to-prepend-to-every-task)
- [000: Luna establishes the current baseline](#000-luna-establishes-the-current-baseline)
- [010: Sonnet closes the logging leak](#010-sonnet-closes-the-logging-leak)
- [020: Sonnet makes task failure and processing failure explicit](#020-sonnet-makes-task-failure-and-processing-failure-explicit)
- [030: Sonnet repairs version storage across database backends](#030-sonnet-repairs-version-storage-across-database-backends)
- [040: Sonnet preserves closed-candle and PAPER recovery semantics](#040-sonnet-preserves-closed-candle-and-paper-recovery-semantics)
- [050: Sonnet makes delivery age and delivery health observable](#050-sonnet-makes-delivery-age-and-delivery-health-observable)
- [060: MiniMax improves alert metadata and reconciles documentation](#060-minimax-improves-alert-metadata-and-reconciles-documentation)
- [070: Luna specifies research boundaries and anomaly semantics](#070-luna-specifies-research-boundaries-and-anomaly-semantics)
- [080: Sonnet corrects receipt timing and then improves performance](#080-sonnet-corrects-receipt-timing-and-then-improves-performance)
- [090: Sonnet adds bounded database retention without deleting evidence](#090-sonnet-adds-bounded-database-retention-without-deleting-evidence)
- [100: The models build an isolated research and backtest path](#100-the-models-build-an-isolated-research-and-backtest-path)
- [110: Luna verifies integration and prepares the OCI release](#110-luna-verifies-integration-and-prepares-the-oci-release)
- [Review prompt and handoff format](#review-prompt-and-handoff-format)

## Execution order and finding coverage

| Packet | Owner | Findings | Depends on | Deliverable |
| --- | --- | --- | --- | --- |
| 000 | GPT-6 Luna | F-15; baseline for F-01–17 | None | Current-state issue ledger and source manifest |
| 010 | Sonnet 5.5 | F-01 | 000 | Secret-safe logging and regression evidence |
| 020 | Sonnet 5.5 | F-02, F-05 | 010 | Supervised task lifecycle and explicit failure policy |
| 030 | Sonnet 5.5 | F-03 | 020 | Compatible schema expansion and migration tests |
| 040A / 040B | Sonnet 5.5 | F-06 / F-10 | 030 | Conservative bootstrap; durable PAPER recovery |
| 050 | Sonnet 5.5 | F-07, F-11, F-17 | 020, 030, 040 | Bounded delivery lifecycle and operational health |
| 060 | MiniMax 3.1 | F-08, F-16 | 050 | Accurate market/data labels and current documentation |
| 070A / 070B | Luna design, Sonnet implementation | F-12 / F-13 | 000, 020, 040 | Research-universe isolation; specified anomaly correction |
| 080 | Sonnet 5.5 | F-09 | 020, 040, 050, 070 | Trustworthy receipt metadata and measured optimization |
| 090 | Sonnet 5.5 | F-14 | 030, 040, 050 | Dry-run-first, dependency-aware database retention |
| 100A / 100B / 100C | Luna / Sonnet / MiniMax | F-04; strategy research follow-on | 070, 080 | Evidence map; isolated backtest implementation; fixtures/reporting |
| 110 | GPT-6 Luna | Integration; closure of F-15 | All applicable packets | Final checks, issue closure matrix, OCI release/rollback package |

Run this as a sequential queue. Do not let several models write to the same checkout concurrently. Review 020, 030, 040, 050, 070, 080, 090, and 100B with Luna before accepting the packet. Keep reviews focused on the packet's changed lines and relevant contracts, rather than repeating the entire repository audit.

An issue is closed only after its behavior is reproduced and verified. A conditional issue can be marked not applicable only with evidence and a stated deployment scope. Missing PostgreSQL or OCI evidence is a separate pending check, not a passing result.

## Rules that override suggestions in the supplied audit

1. OCI hosting does not remove logging, worker supervision, stale-alert, or readiness defects. The checked-in systemd unit describes SQLite storage (`deploy/systemd/signalbot.service`), which can reduce the immediate relevance of the PostgreSQL defect. The actual deployed configuration and revision have not been inspected in this task.
2. Preserve the user's current Discord enablement and concise wording. Do not disable Discord, restore previously removed disclaimer text, or demote all live signals simply because the supplied audit recommends it. F-04 requires research evaluation; F-08 permits factual metadata corrections.
3. Do not apply a 32-character validator as the whole F-03 fix. The example rule version is already 35 characters. Expand storage through an explicit migration and align validation with the resulting contract. Never truncate a version or rewrite historical event IDs.
4. Do not silently replace a conflicting finalized REST candle with a WebSocket candle. Prevent premature bootstrap acceptance; preserve hard conflicts and deterministic historical evidence. An authority/reconciliation change requires a specified, tested policy.
5. Do not blindly reconstruct PAPER positions from the latest 10,000 mixed signal rows. Define an authoritative lifecycle checkpoint or a verified bounded reconstruction contract. A data gap means tracking uncertainty, not proof that a trade should exit.
6. Do not retry ambiguous Discord delivery or promise exactly-once delivery. Preserve `uncertain` quarantine. An operator resolution must not fabricate a Discord message ID or automatically resend a possibly delivered alert.
7. Do not regard changes to volatility normalization, anomaly thresholds, universe membership, or the wick strategy as presentation-only fixes. They change semantics and need separate versions and research treatment.
8. Database retention is separate from frozen research tape retention. Preserve research manifests, hashes, provenance, unresolved delivery, and open PAPER lifecycle dependencies. No live deletion is authorized by this planning document.

## Common prompt to prepend to every task

```text
You are working in D:\Binance bot-2. Complete ONLY the selected packet from
docs/research/REPOSITORY_REMEDIATION_MODEL_TASKPACK_2026-10-06.md.
All reports, handoffs, and new task documentation must be in English.

Read applicable AGENTS.md instructions and the relevant existing source and
contracts before editing. Treat the pasted independent audit as a set of leads:
verify the current checkout and do not assume its line numbers or conclusions
still match. Reuse existing module owners, migrations, tests, and operational
tools; search before introducing a new abstraction or command.

Preserve the working tree. Record the pre-existing changed/untracked files and
patch fingerprints before starting. Do not reset, clean, bulk-stage, or commit
someone else's changes. If you edit an already-modified file, attribute your
changes at hunk level. Identify HEAD plus the actual tested tree and config;
do not identify a dirty-tree result by HEAD alone. Preserve frozen research
artifacts and do not regenerate them to make an identity failure disappear.

Implement the packet rather than stopping at a proposal. Use focused meaningful
regression tests, including negative and boundary cases. New signal rules require
positive, negative, boundary, and causal-prefix tests. Use recorded fixtures and
mocked transports: no network calls in unit tests and no real Discord sends.
Run affected checks after the coherent change; avoid repeated whole-suite runs.
Report exact commands, environment/interpreter, exit codes, skips, and limits.

Maintain: public-data-only scanner, no API key requirement, no production orders,
UTC Unix-ms storage, UTC and Asia/Seoul alert times, fully closed causal candles,
strictly prior mature HTF context, distinct Spot exits and Futures shorts,
explicit PUMP_RISK/CRASH_RISK intrabar warnings, bounded queues/caches/retries,
reasons/invalidation/rule version/deterministic IDs, atomic signal+outbox intent,
and reconnect idempotency. Keep Guardian/private-read boundaries intact.

Keep current Discord enablement and wording preferences. Do not add previously
removed disclaimer text. Do not silently change scientific decision thresholds,
capture scope, live universe, rule eligibility, or historical campaign receipts.

Use Python 3.12+, uv, Ruff, Pyright, and pytest as configured. Inspect the actual
environment first: the supplied audit reported a Linux .venv on Windows and an
existing .venv-codex, but verify that afresh. Use a working environment without
rewriting the lockfile or rebuilding an unrelated environment. A missing checker
is not a pass; resolve the tooling prerequisite where authorized or state it.
Follow AGENTS.md RTK rules; retain raw final validation evidence.

Do not deploy, push, rotate credentials, send messages, migrate a live database,
delete live data, or create other tasks as part of this packet. Prepare concrete
reviewable artifacts and local disposable-database proofs where applicable.
Return the handoff format in this task pack. Stop after the packet is complete.
```

## 000: Luna establishes the current baseline

```text
PACKET 000 — Current-state reconciliation. Read-only source/research review;
write only the bounded issue ledger and handoff record in the existing docs
convention. Do not implement fixes or run all 4,500+ tests before triaging the
urgent leak and failure defects.

Establish branch, HEAD, working-tree patch/untracked-file identities, config
identity, Python/tool availability, and deployment assumptions. Read README,
ARCHITECTURE, RESEARCH_GOVERNANCE, capability/promotion documents, and the
existing migration and systemd conventions. Classify each F-01–F-17 as verified,
conditional, fixed already, not applicable, or not reproduced. Keep source-only
evidence separate from actual backend/OCI evidence. Inspect the uncommitted HTF
context correction and Ruff failure without reverting unrelated work.

Use the smallest offline repros for F-01, F-02, F-05, F-03, and F-06. Document
the earliest safe failure boundary and recovery expectation. Do not expose
secrets or copy raw environment values/logs into evidence.

If existing authorized OCI read access is available, compare the deployed
release/config/schema and actual service ownership to the local tree. Otherwise
produce precise read-only checks for packet 110; do not assert deployment parity.
Output a ledger of ID, current file:line, mechanism, affected mode/backend,
repro command, acceptance check, assigned packet, and remaining uncertainty.
```

## 010: Sonnet closes the logging leak

```text
PACKET 010 — Repair F-01 in the existing logging owner and Discord clients.
Primary owner: src/signalbot/observability/logging.py. Inspect the scanner
Discord client and the existing uncommitted position_guardian/discord.py.

Suppress unnecessary httpx/httpcore request logging and enforce redaction at
the effective output handler/formatter boundary, including formatted exception
text and structured fields. A filter attached only to the root logger does not
necessarily protect propagated child records. Preserve useful error context.

Use fake webhook tokens and MockTransport/captured log output to reproduce the
leak, then verify INFO, DEBUG, failed requests, and traceback formatting. Test
both client configurations without real sends. Confirm repeated logging setup
does not accumulate handlers. Do not print, rotate, or store real credentials.

Accept when the synthetic token is absent from every rendered log output and
delivery behavior is unchanged. Provide an optional read-only OCI check and
rotation recommendation only if historical exposure is actually established.
```

## 020: Sonnet makes task failure and processing failure explicit

```text
PACKET 020 — Repair F-02 and F-05. Owners: src/signalbot/app.py,
exchange/binance/websocket.py, alerts/discord.py, runtime.py, and the existing
persistence repository as required by the actual failure boundary.

Specify a small failure matrix before implementing: transport reconnect,
recoverable database failure, capacity exhaustion, event/candle hard conflict,
unexpected task return, explicit cancellation, and normal requested shutdown.
Narrow transport exception handling so handler RuntimeError/OSError/TimeoutError
are not accidentally treated as socket failures. A domain error must never
cause endless reconnects or silent task completion.

Supervise delivery when enabled; expose worker failure and bounded recovery.
Unexpected critical-task completion must produce a non-successful application
outcome, so systemd Restart=on-failure works. Log teardown failures while keeping
cancellation and stop deadlines correct. Disabled Discord must not require a
delivery heartbeat. Do not depend on Discord to report a Discord worker failure.

Audit commit/order/checkpoint boundaries: CandleStore acceptance must not make
an unsuccessfully persisted decision permanently appear processed. Prove safe
retry/recovery after failure between candle acceptance, decision persistence,
and outbox commit, or explicitly quarantine the processing gap and inhibit new
recommendations until reconciled. Never silently lose that decision boundary.

Inject each matrix case, including a task returning normally without a stop
request. Verify no unbounded reconnect, no swallowed exception, no duplicate
decision/outbox intent, bounded shutdown, and intended restart behavior.
```

## 030: Sonnet repairs version storage across database backends

```text
PACKET 030 — Repair F-03. Owners: src/signalbot/persistence/models.py,
repository.py, config.py, config/settings.example.yaml, existing migration
tests, and .github/workflows/ci.yml if backend coverage requires it.

Inventory all persisted version columns and real configuration values; do not
assume SignalRecord is the only affected table. Choose and document a bounded
version storage contract that accepts the existing 35-character rule version,
then align schema and configuration validation. Never truncate versions,
change event IDs, or modify historical rule contents to fit old storage.

Extend the repository's existing migration mechanism. create_all is not an
upgrade of an existing table. Prove fresh-schema creation and idempotent upgrade
from the old VARCHAR(32) schema with existing rows, indexes, and relationships.
Keep SQLite compatible. Specify transactional failure/recovery and rollback
compatibility; do not perform a destructive downgrade on live data.

Run actual insert and upgrade checks on disposable PostgreSQL, separate from
network-free unit tests, plus SQLite migration tests. If PostgreSQL cannot run,
provide a CI integration job/test and mark live backend proof pending; compiled
DDL is useful evidence but cannot close the PostgreSQL execution check.
```

## 040: Sonnet preserves closed-candle and PAPER recovery semantics

Run 040A and 040B as separate implementation turns.

```text
PACKET 040A — Repair F-06. Owners: exchange/binance/rest.py, schemas.py,
data/candles.py, scanner.py, and existing REST/runtime tests.

Define an exchange-time-aware conservative closure watermark, with explicit
uncertainty handling and a bounded fallback if time synchronization fails.
Test local clocks ahead/behind, request latency, exact close boundaries,
bootstrap/WS overlap, identical duplicates, and true conflicting final candles.
Exclude uncertain last bars rather than declaring a REST row closed from a
fast local clock. Preserve closed-only decisions and hard conflict detection.
Do not overwrite finalized evidence or evaluate a recovered gap using hindsight.
```

```text
PACKET 040B — Repair F-10. Owners: signals/positions.py, runtime.py,
scanner.py, persistence models/repository, and lifecycle/restart tests.

Define authoritative durable PAPER lifecycle state, its checkpoint timing,
parent-entry identity, exit identity, rule version, market, holding age, and
gap/recovery status. Reuse existing persistence primitives where suitable.
Prove restart reconstruction from a complete ordered history or persist a
transactional checkpoint; latest-N mixed signals alone is insufficient.

Cover restart after entry, before/after exit intent, universe removal, holding
expiry, missing historical bars, bootstrap, and reconnect. PAPER is simulated
state, not a real exchange position. No private account queries or orders.
Distinguish tracking interruption/DATA_GAP from a technical exit condition.
Handle unreconstructable legacy state explicitly and once per lifecycle.
Use bounded recovery and controlled handover; do not introduce unbounded
duplicate WebSocket subscriptions when improving universe rotation.

Accept when restart/replay preserves lifecycle identities and generates neither
duplicate alerts nor an exit inferred solely from an ingestion gap.
```

## 050: Sonnet makes delivery age and delivery health observable

```text
PACKET 050 — Repair F-07, F-11, and F-17. Owners: alerts/discord.py,
persistence models/repository, app.py, api/server.py, the existing CLI, and
tools/oci_operational_health.py. Split into delivery semantics and health/tooling
subchanges if a single diff is difficult to review.

Use the existing recommendation expiry contract where it applies. Define
family-specific age semantics: entry/risk candidates may expire; lifecycle
tracking and technical exits require their own policy. Use event time rather
than process restart time. Check expiry before claim/send and after every
rate-limit wait. Preserve the original immutable payload and audit identity;
record expiry as delivery metadata/state instead of rewriting a saved event.

Remove Discord backlog draining from the market-startup critical path while
preserving in-flight recovery. Make 429 handling bounded by cancellation,
maximum elapsed time, and expiry. Honor Retry-After and applicable bucket
headers without treating 429 as ambiguous. Preserve uncertain quarantine for
possibly accepted requests; never auto-retry uncertain delivery.

Add process-published heartbeat/status data consumed by readiness: scanner
task status, market message age, delivery worker state when enabled, oldest
pending age, uncertain count, retry/dead reasons, and schema/config identity.
A separate API process cannot infer scanner health from its own repository.ready.
No-trade intervals are not automatically unhealthy; stale critical streams are.

Extend the existing OCI health tool with these facts. Add audited manual outbox
resolution, with compare-and-set transitions, reason/note, original ambiguity,
and evidence reference. Never fabricate delivery evidence or implicitly resend.
Specify capacity treatment for terminal operator resolutions without erasing
the audit history. Preserve compatible output fields for current consumers.

Test expired startup backlog, a wait crossing expiry, repeated 429, ambiguous
timeout/5xx, successful ID receipt, transient DB failure, disabled Discord,
stale/missing scanner heartbeat, and concurrent/manual resolution conflicts.
Accept when startup stays bounded, every delivery outcome is observable, and
no retry policy can create a duplicate potentially delivered message.
```

## 060: MiniMax improves alert metadata and reconciles documentation

```text
PACKET 060 — Narrow implementation of F-08 and documentation repair for F-16.
Owners: src/signalbot/alerts/embeds.py and focused embed tests; docs/ARCHITECTURE.md,
docs/SIGNAL_SPEC.md, and deployment/runbook documentation when actually affected.

First read the actual alert input contract and accepted packet-050 handoff.
Write down expected payload examples, then implement only those examples:
clearly distinguish SPOT and USD-M Futures; display missing/not-evaluated gates
as unavailable rather than perfect 100/0 measurements; describe the precise
data-availability scope rather than claiming universal completeness. Render
expiry only from the accepted authoritative delivery/recommendation metadata.

Do not change gates, thresholds, eligibility, recommendation titles, market
selection, or default Discord enablement. Do not restore removed disclaimer text.
Do not infer that a gate was unevaluated merely because its numeric value is zero.
If an explicit evaluation-status field is missing, propose that exact minimal
contract addition for Sonnet rather than inventing an interpretation in the UI.

Reconcile documentation with primary-only pullback evaluation, strict-prior HTF,
the actual live projector/expiry path, and the actual restore query. Document
unsupported settings truthfully; do not make docs match a feature that was not
implemented. Preserve Discord component and shared 6,000-character limits.
Use positive/negative/boundary payload tests and existing realistic fixtures.
```

## 070: Luna specifies research boundaries and anomaly semantics

```text
PACKET 070 — Design/review F-12 and F-13 as separate changes. Read config.py,
exchange/binance/universe.py, scanner.py, data/anomaly.py, research configuration,
DIRECTIONAL_CANDIDATE_RESEARCH_PROTOCOL, and RESEARCH_GOVERNANCE.

070A: Trace intended research-required symbol coverage versus live tradable and
surveillance ownership. Preserve authorized research coverage. Specify an
explicit observation-only subscription boundary so a research flag cannot
silently enlarge the production recommendation universe. Define missing-symbol
campaign censoring/status without silently losing required research coverage
or stopping unrelated healthy markets. Do not solve this by globally disabling
Discord. Provide exact affected owners and acceptance tests for Sonnet.

070B: Reproduce zero/near-zero MAD and irregular sampling. Choose the return,
sampling, and tick-aware scale contract using dimensional reasoning. Specify
what unavailable dispersion means, how tick-size metadata arrives, and how
quantization differs from a real jump. Rendering caps alone cannot fix detector
semantics. Cover constant price, one-tick moves, genuine shocks, low-price coins,
duplicate timestamps, sparse observations, and unavailable tick metadata.

Separate numerical correctness from predictive validity. Changing anomaly
normalization or thresholds requires a new semantic/rule version and separate
evaluation; do not silently activate a newly tuned detector based on historical
optimization. Keep a corrected implementation explicitly versioned and in
shadow comparison until its promotion contract is satisfied.

Produce two small implementation contracts. Sonnet then implements 070A and
070B in separate turns using the common prompt and your exact acceptance cases;
Luna reviews their changed paths and verifies the stated boundaries.
```

## 080: Sonnet corrects receipt timing and then improves performance

```text
PACKET 080 — Repair F-09 without changing scientific features. Owners:
exchange/binance/websocket.py, scanner.py, feature engine owners, and runtime.py.
First establish the existing arrival/recording/time contracts and capture scope.

Attach local wall-clock Unix-ms and monotonic timing at the earliest supported
application receive boundary, before awaited handler work; propagate receipt
metadata without substituting handler start time. Expose exchange event time,
arrival/handler delay, and sequence information only when the source provides
them. API iteration may itself drain buffered old messages: an application
receive timestamp is not proof of wire arrival. Specify conservative stale
data treatment and queue/loop-delay measurements for that uncertainty.
Do not change frozen raw-tape schemas or historical receipt identities silently.

Benchmark a realistic synchronized multi-market 5m/1h/4h boundary replay.
Report workload, hardware, p50/p95/p99/max loop delay, backlog, and decision/BBO
age reasons. The audit's p99<250ms is a proposed target, not measured OCI proof;
freeze a realistic target before optimizing and retain the baseline trace.

Then remove demonstrably unused computation and use stable bounded incremental
calculations where justified. Trace every feature consumer first; 1m features
may be needed by packet 100's isolated research path even if not by live rules.
Avoid unbounded executor queues or sharing SQLAlchemy sessions across threads.
Compare all feature values and boundary decisions against a causal reference
implementation with justified tolerances, including near-threshold cases and
restart prefixes. Do not optimize by changing the indicator definition.

Accept when stale buffered messages cannot appear fresh merely from handler
delay, parity holds, boundedness holds, and the declared workload target passes.
Leave OCI-specific performance verification explicitly pending if unavailable.
```

## 090: Sonnet adds bounded database retention without deleting evidence

```text
PACKET 090 — Repair F-14. Read the repository's storage retention documents and
existing archivers first. Implement live SQL retention in the existing owners;
do not treat the research raw-tape archive policy as permission to delete SQL
or vice versa. Inspect downstream consumers and packet-040 lifecycle references.

Define per-table cutoffs, UTC age boundaries, retention horizons, reference
dependencies, and inactive/terminal status eligibility. Retain pending/sending/
uncertain outbox records, unresolved operator audit history, open PAPER parents,
needed reconstruction windows, and frozen campaign evidence. Do not disable
1m ingestion to reduce storage without checking every consumer and campaign.

Provide a default dry-run listing/count/size estimate, bounded indexed batches,
cancellation, idempotent progress, and transactional recovery. Any destructive
execution must be an explicit operator command with reviewed scope; there must
be no new automatic live purge enabled by this packet. Preserve long-term
deduplication identities/tombstones so old replays cannot resend purged events.

Prove eligibility on disposable databases, including unresolved and lifecycle
references, crashes between batches, exact cutoffs, repeated runs, and old-event
replay. Provide backup/restore and activation instructions for later deployment.
Accept code/tooling independently from production purge activation.
```

## 100: The models build an isolated research and backtest path

These are follow-on research packets, separate from operational repair. A completed backtest implementation does not establish strategy profitability or permit live promotion.

```text
PACKET 100A — GPT-6 Luna, research specification and evidence reconciliation.
Address F-04 without changing live Discord policy or wording. Read the original
R2/replay reports, their source identities and population definitions, and the
current governance/capture/capability documents. Distinguish a failed historical
baseline from an unmeasured BBO-filtered policy; do not equate either with proof
about every future signal family or claim a profitable edge from implementation.

Create a concise evidence-status map for each live family: measured population,
clock/horizon, costs, missing data, result, and permitted next inference. Specify
an event study for PUMP/CRASH risk using contemporaneous matched market controls,
including selection bias, time-of-day/volatility matching, overlap, costs, and
false-positive burden. Large absolute moves alone do not prove warning lead
time or directional value. Fix hypotheses and success criteria before outcomes.

For the user's symmetric upper/lower wick plus Bollinger exhaustion hypothesis,
start from docs/research/BAND_REJECTION_TAKEOVER_V1_DESIGN_2026-10-05.md. Audit its
causal clocks, episode construction, opposing wicks, fixed landmark, takeover,
early-versus-confirmed comparison, abstentions, execution ambiguity, and costs.
Finalize a reproducible offline implementation contract. Do not select a winner
after inspecting all parameters or reuse exhausted development data as holdout.
Keep this 1m experiment separate from the frozen existing 5m campaign.

Specify C0 contact baseline, C1 wick count, C2 wick magnitude, C3 both, and early
versus takeover policy comparisons on compatible parent opportunities. Separate
price-only inference from order-book/taker-flow confirmation. OHLCV cannot prove
queue absorption or order-book replenishment. L2 confirmation needs its own
validated capture/reconstruction scope and prospective preregistration.

Deliver the accepted contract, dataset availability/quality report, source/config
identity requirements, cost assumptions, sample-power/unlock criteria, and the
precise scope for 100B. No live threshold or eligibility changes.
```

```text
PACKET 100B — Sonnet 5.5, isolated offline implementation of accepted 100A.
Reuse the existing replay/backtest owners. Implement the accepted symmetric
episode detector, fixed-time parent opportunity export, ablation comparison,
realistic next-observable execution, and uncertainty-aware exits/returns.

Require fully closed input; freeze bands, ATR, price zones, and thresholds at
their specified timestamps. Include opposing-wick dominance, invalidation,
expiry, cooldown, duplicate/restart handling, and causal-prefix parity. Keep
the research rule disabled in production by default and do not alter live
notification generation or the frozen campaign's collection scope.

Export parent-level accepted/rejected outcomes, reasons, cost components,
coverage/missingness, MFE/MAE, direction/asset/regime breakdowns, and uncertainty.
Compare paired net outcomes across the same opportunities; do not report only
filtered winners. Purge overlapping labels at chronological splits, use a
prespecified block-bootstrap/multiplicity procedure, and separate development,
untouched temporal evaluation, and prospective evidence. Never tune on holdout.

Test long/short symmetry, double-sided wicks, no contact, trend band-walk,
insufficient warmup, invalidation at the boundary, takeover delay/expiry,
same-bar stop/target ambiguity, entry gaps, transaction costs, and missing
funding/book data. Simulated positions remain paper only. Missing historical
L2 is unavailable evidence, never a zero-valued confirmation feature.

Run on synthetic/fixture data first. Run a labeled development backtest only if
an identified admissible dataset is available. If it is absent, finish the
runner/tests/data contract and report DATA_REQUIRED; do not invent results or
call the strategy validated. Return actual command names only after implementing
and verifying them. Luna reviews the statistical and causal contracts.
```

```text
PACKET 100C — MiniMax 3.1, fixtures and report presentation after 100B is accepted.
Use the frozen detector/runner contract; do not modify it. Add small readable
recorded/synthetic examples for the approved edge cases and a deterministic
report using actual runner output. Separate gross/net returns, opportunity-
level effects, traded-only effects, abstention rates, missingness, uncertainty,
and development versus untouched evaluation. Report losing and empty results
truthfully. Include source/config/data identities and the exact runner command.
Do not add thresholds, run a parameter search, fill missing values with zero,
claim statistical significance from unadjusted slices, or change production.
```

## 110: Luna verifies integration and prepares the OCI release

```text
PACKET 110 — Final independent integration review. Inspect accepted diffs and
handoffs, then verify cross-packet failure, migration, lifecycle, expiry, timing,
retention, and research boundaries. Close F-01–F-17 with fresh evidence or an
explicit remaining limitation. Fix only narrow integration defects; route
substantive regressions back to the responsible bounded implementation packet.

Run final raw checks in the identified working environment:
  uv run ruff check .
  uv run pyright
  uv run pytest -q
  python -m compileall -q src tests
Use an explicit working-environment invocation if necessary and record it.
Run existing validate-config/dry-run/replay CLI checks and the affected migration,
performance, and statistical-contract checks. Do not claim a checker passed
because it was unavailable or skipped. Run Linux CI for platform-specific
tests skipped on Windows. Distinguish actual PostgreSQL execution from DDL-only
coverage and a local replay benchmark from OCI hardware measurements.

Build an OCI release package: exact source/tree/config/schema identities,
deployment manifest, service separation, migration order, persistent paths,
secret-safe configuration requirements, preflight checks, backup instructions,
bounded stop/start sequence, heartbeat/outbox acceptance checks, and rollback
compatibility after schema expansion. Include read-only service/log checks that
never print complete webhook URLs or environment secrets.

Do not deploy or migrate live data in this packet. If read access is available,
verify release parity read-only; otherwise provide executable operator checks.
Keep operational-ready, research-implemented, strategy-validated, and deployed
statuses separate. Engineering repair cannot turn a failed backtest into alpha.
```

## Review prompt and handoff format

Use this after an implementation packet, especially a schema, lifecycle, scientific, or timing change:

```text
You are GPT-6 Luna, reviewing the selected completed packet independently.
Read its common contract, task specification, baseline, changed hunks, and
verification evidence. Verify only the changed behavior and its directly
affected consumers. Treat the implementer's summary as a claim to check.

Try to falsify the acceptance criteria through realistic failure/boundary cases.
Check causal timing, closed/HTF selection, restart idempotency, schema upgrade,
atomicity, cancellation, capacity, uncertain delivery, scientific versioning,
and dirty-tree provenance where relevant. Do not rerun the whole suite unless
a new integration risk justifies it. Do not create unrelated refactors.

Return ACCEPTED, CHANGES_REQUIRED, or PENDING_EXTERNAL_VERIFICATION. Attach
file:line, mechanism, repro/evidence, and the smallest corrective task for each
material finding. Missing backend/OCI evidence stays explicitly pending even
if the local implementation is accepted. Do not edit live systems.
```

Each executor's handoff must contain:

1. Packet ID, start baseline, resulting source/tree/config identity, and scope.
2. Finding closure status, before/after behavior, and exact changed files/hunks.
3. Reproduction and regression checks, commands, exit codes, skips, and evidence paths.
4. Migration/config/API/alert/scientific contract changes and compatibility implications.
5. Any pending backend/data/OCI proof, with the concrete next check and its owner.
6. The next packet and the facts it needs. No unsupported completion or profitability claims.

## Model settings and source notes

Where the host exposes effort settings, start Sonnet implementation at Medium and use High for migrations, lifecycle recovery, and timing changes; use Luna High for design/research review, Medium for baseline reconciliation; use MiniMax Medium for bounded presentation/documentation work. These are starting settings to adjust from observed task results, not guarantees or benchmark conclusions. Verify host alias resolution; do not silently substitute another model.

Anthropic describes Sonnet 5.5 as suited to well-scoped tasks and bug fixing, and its launch notes warn that maximal effort can introduce out-of-scope work. This supports bounded implementation packets. Source opened 2026-10-06: [Sonnet 5.5 launch](https://www.anthropic.com/claude-sonnet-5-5).

MiniMax's official documentation currently names `MiniMax-M3.1-Flash-Preview`, supports tunable effort, and identifies subscription/host availability restrictions. Verify whether the user's “MiniMax 3.1” selector resolves to that model. Its large context window is not a reason to load the whole repository for a small task. Source opened 2026-10-06: [MiniMax model invocation](https://platform.minimax.io/docs/guides/text-generation).

The GPT-6 Luna role here is an explicit project workflow choice. This task pack makes no unsupported claim about its public benchmark ranking or proprietary internal behavior.

This file is newly authored task planning derived from the supplied audit summary, selected current source checks, existing project contracts, and the earlier wick research design. The pasted private audit transcript is not copied into the repository. Source code, deployment configuration, and live services were not changed while preparing this pack.
