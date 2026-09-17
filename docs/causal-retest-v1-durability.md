# causal_retest_v1 durability contract

`causal_retest_v1` is a shadow/research protocol. It does not modify frozen
`r2_pit_htf_exec`, `SignalStateMachine`, PAPER, Discord, or any order path.

## Phase E evidence freeze

Prospective retest campaigns must freeze the complete runtime/scientific source
set with `signalbot.prospective.source_freeze.freeze_source`. The identity is
`worktree-source-v1:<source_root_sha256>` and is computed from sorted SHA-256
file entries for `src/signalbot/**/*.py`, `pyproject.toml`, and `uv.lock`.
Symlink and Windows reparse-point inputs fail closed; Git HEAD is retained as
separate provenance and is not the source identity.

`shadow_observation_v2` carries an immutable `decision_bbo_v1` object containing
the exact bid/ask prices and quantities, exchange/receipt clocks, update ID, and
age. Derived spread, age, capacities, and raw evidence all come from the same
`BookState.snapshot`. v1 rows remain readable but cannot be used as exact-BBO
references; a prospective retest campaign with retest observation enabled must
register v2, must set `runtime.persist_candles=true`, and is rejected at
observer startup unless the running checkout computes the exact configured
`worktree-source-v1:<64 lowercase hex>` identity. Source-root, every source
file, and every parent path are checked for symlink/reparse escapes before
hashing.

`signalbot.prospective.retest_outcomes` is an offline deterministic evaluator
for pre-registered 5m horizons 1/3/6/12 bars. Its effective reference time is
`max(decision_time, exact_BBO_receipt_time)`; the first included candle must be
a closed bar whose open is strictly after that time. This prevents a BBO
received during a still-forming candle from leaking that candle's path into
the outcome. A missing BBO receipt clock keeps the descriptive close-path
return but sets the executable-BBO return to `None` with an explicit diagnostic.
The evaluator requires contiguous bars, reports explicit `DATA_GAP`/
`INSUFFICIENT_HORIZON` outcomes, and keeps descriptive close-path, exact-BBO-
entry, and 26 bps cost-model-adjusted research returns separate. It never
claims a candle-close return is a fill and does not infer intrabar order.

`signalbot.prospective.retest_replay` supplies the forward parity boundary:
`RepositoryLiveLikeAdapter` reads the exact campaign-bound durable lifecycle
written by the observer, while the replay adapter independently rebuilds the
lifecycle from causal features, strict-prior contexts, closed primary bars,
and recorded BBO. The former does not consume replay output; the fixed-output
adapter is test-only. Historical rows without raw decision-time BBO remain
`INCONCLUSIVE_NO_HISTORICAL_BBO` forever.

## Durable lifecycle

Each admitted opportunity has one current row in `retest_lifecycles` and an
append-only history in `retest_transitions`. A transition and its current
snapshot are committed in one database transaction. The transition identity
binds campaign ID, campaign manifest SHA, opportunity ID, protocol version,
retest policy SHA, source/destination stages, transition/decision/bar clocks,
and the canonical payload SHA. The logical transition ID deliberately excludes
`persisted_at_ms`; storage latency is provenance, not scientific event identity.
The append-only row retains both `payload_json` and its SHA, so the transition
can be audited without trusting the mutable current row.

`RetestPolicy` binds `causal_retest_v1`, horizon, touch semantics, recovery
semantics, and invalidation/evidence-failure semantics. Its deterministic SHA
is persisted with every arm, lifecycle, and transition. A parameter change
therefore creates a distinct scientific identity.

Replaying the same logical transition and content is an idempotent no-op.
Reusing a logical transition with different content is a hard conflict. Each
write supplies the expected previous lifecycle SHA; a stale writer is rejected
inside the same transaction that appends the transition and updates the
current snapshot. A transition must start at the durable current stage, use the
registered campaign manifest/protocol/policy, and cannot follow a terminal
stage. External censor events may have `bar_close_ms = null`.

## Restart and denominator

`serialize_lifecycle`/`restore_lifecycle` preserve ARMED and RETEST_TOUCH state
and reject unsupported snapshot schemas or missing policy provenance. The
prospective observer loads only exact campaign/manifest/policy rows. When
completed-bar continuity is not explicitly proven at startup, active rows are
durably terminalized as typed `CENSORED(RESTART_GAP)` with no fabricated bar
close; a durable RAW_C0 row is never silently promoted to ARMED.

`SqlRepository.retest_lifecycle_counts` reports active, touched, and each
terminal state (`READY`, `INVALID`, `TIMEOUT`, `CENSORED`) for one registered
campaign manifest. Terminal rows are immutable, so every admitted opportunity
remains visible in the denominator and can receive only one terminal state.

`SqlRepository.audit_retest_denominator` is the stronger campaign audit. It
uses durable `ShadowObservationRow.opportunity_id` values as the expected
registry and compares them with current lifecycle rows bound to the exact
campaign manifest and retest-policy SHA. It reports missing and unexpected
IDs as well as duplicate evidence IDs; READY-only rows are never used as the
denominator. A base observation persistence exception therefore cannot create
an orphan retest lifecycle.

The observer integration is controlled by
`shadow.retest_observation_enabled` (default `false`). When disabled, no
retest lifecycle or transition is created. When enabled, the observer reuses
the existing raw-C0 comparator opportunity ID, arms exactly once, advances only
on later completed primary bars, and remains failure-isolated from R2, PAPER,
and Discord.

The observer keeps an index by `(market, symbol)` for active lifecycles, so a
completed bar only visits matching active opportunities. A confirmed scanner
universe rotation calls the research-only censor boundary for outgoing
tradable symbols. It writes `CENSORED(UNIVERSE_MEMBERSHIP_LOSS)` with a real
external transition time and `bar_close_ms = null`; an unconfirmed candidate
universe does not censor anything. If that research write fails, the scanner
still rotates and the observer retains the in-memory lifecycle for retry or
explicit shutdown handling.

Runtime startup does not currently prove exact completed-bar continuity. It
therefore preserves the conservative `CENSORED(RESTART_GAP)` behavior; the
helper-level restore support must not be read as proof of live exact resume.

## Live/replay parity

`run_retest_adapter_parity` requires distinct live and replay adapters. It
compares only cases with recorded decision-time historical BBO. Missing BBO is
never replaced with a proxy; those cases produce
`INCONCLUSIVE_NO_HISTORICAL_BBO`, and a complete PASS is impossible while any
case remains unobservable. This parity result is research evidence only and
does not activate or promote the successor. The current generic parity harness
does not claim live-vs-replay ingestion parity; absence of recorded historical
BBO remains an explicit ceiling.

Scientific status: `SHADOW_SUCCESSOR_ONLY`. No automatic order, sizing,
leverage, or production entry path is enabled by this protocol.

## Phase F smoke qualification

Retest-enabled smoke uses the same evidence contract as a prospective campaign:
`shadow_observation_v2`, persisted candles, and an exact running
`worktree-source-v1:<64 lowercase hex>` identity are required before campaign
registration. `signalbot audit-retest-smoke` is read-only against the campaign
database and writes only the requested canonical audit artifact. It verifies
campaign/source/policy identities, sealed coverage, the complete retest
denominator, exact BBO/receipt evidence, closed-candle outcomes, and explicit
parity taxonomy; it never treats profitability as an integrity gate.

Public smoke raw events are stored in an isolated bounded JSONL directory.
`ProspectiveRawTapeReplay` advances `ReplayClock` from the envelope's recorded
local `received_at_ms`, not Binance payload exchange time, and rejects market
mismatches, malformed receipts, and backwards receipt clocks. This provides
receipt-time software coverage without claiming independent funding or
historical execution parity. The smoke remains Discord-disabled,
order-free, and research-only; a natural absence of raw-C0 is inconclusive,
and any unresolved lifecycle is explicitly censored at graceful shutdown.
