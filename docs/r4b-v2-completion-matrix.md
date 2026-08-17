# R4B V2 Completion Matrix

This matrix prevents engineering test success from being mistaken for evidence
of profitable prospective performance. `COMPLETE` requires the listed
authoritative artifact, not merely code or a unit test that could produce it.

| Requirement | Completion evidence | Current state |
|---|---|---|
| Alert-only/public-data boundary | Source audit shows no production order path, no private account dependency, and no secret in logs | Implemented invariant; repeat at final seal |
| Closed 5-minute causal decisions | Raw-record replay proves candle closed, economic data-through/transaction time `<= k.T`, exchange observation/receipt/completion `<= D=k.T+2001`, and no future HTF row | Local A/B/C cutoff and fixed-D replay tests pass. Signed-block exact-record membership, the kline M1 parser, and an exact 8,654-row M1-only price projection now exist. The projection preserves `k.T`, exchange event, and receipt separately and never upgrades numeric readiness to producer readiness; A/B/C evidence factories are still not wired through an M2 clock/cursor-finality certificate |
| Exact raw-record membership and interpretation | Public-key verification binds one canonical `RawRecordV2` line to the current signed grouped-block chain, storage-root authority, finalized reference, and non-VOID integrity-ledger prefix; M1 binds exact source interpretation | M0 is implemented with mandatory same-operation live reverification. Combined-query USD-M `aggTrade`/`kline_5m` M1 is implemented with exact wrapper/field parsing, current `nq`/`st`, parser-contract hashing, factory-only rows, and no current/cursor/causal authority claim. The integrity ledger now distinguishes exact local `DATA_GAP` intervals from two-phase, unknown-count upstream `SOURCE_GAP` intervals. For BOUNDED it owns root-bound signed-block reads under the append lock, derives both endpoint cursors, stores compact block/ingest/exact-line-hash locators rather than record-sized certificates, exposes only a point-in-time assertion that rechecks both roots, the ledger tip, exact endpoints, and post-scan VOID evidence, returns detached event copies, and reserves record-size-independent closure capacity. The existing bounded handoff and single writer worker also support an exact positive, non-terminal causal finality fence: it requires requested tail = accepted tail = WAL ACK = signed-block tail, revalidates current single/dual WAL and block root bindings, byte-compares the WAL/block prefix, binds the final manifest/container/signing authority, and returns a repeat-stable prefix proof while leaving the writer live. Per-route V2 adapters now inject raw-frame retention and the recovery lifecycle into the existing sole WebSocket owner: receipt capture is first after iterator yield, one exact `SharedWebSocketIngressV2` starts at the recovered WAL tail and is shared across route adapters, raw bytes are offered before parsing, and cancellation before bounded retention fails closed. The owner recognizes only the exact V2 frame-factory/lifecycle pair, rejects partial pairs and V2 subclasses, and forces the exact production composition guard; structural substitutes cannot opt into this boundary. A write-once session-start authority requires an exact held OS writer lease, binds the plan/protocol, qualified dual-WAL durability, block/signing/ledger contracts and four pairwise non-nested storage roots inside the canonical lease scope, and irreversibly consumes one canonical start claim per lease acquisition. Successful persistence returns a factory-only sealed receipt binding the original path, canonical bytes/hash, file identity/link count, and lease claim; unlink or failed issuance cannot be followed by a second start under that acquisition. The sealed per-route composition accepts only that exact receipt and exact owner/factory/lifecycle/ingress/pipeline/WAL/block/ledger types, binds their construction-time opened-root identities, and requires a running healthy worker, accepting handoff, open durable writers, held lease, and aligned recovered cursors. Connector admission holds the lease operation guard across asynchronous context entry and revalidates both before and after the handshake, before `CONNECTED` or any frame consumption. WAL, grouped-block, and integrity-ledger mutation paths recheck current binding bytes and opened directory/binding-file identities before and after mutation; recreated same-path roots with copied bindings cannot produce an ACK or accepted ledger event. Unit tests prove type, route, lineage, tail, lease, receipt, path/root identity, readiness, and handshake-window drift fail closed before a connected transition or frame. A real-disk integration test proves durable `SOURCE_GAP OPEN` before the connector, retained-successor WAL/block finality followed by `BOUNDED` and current/restart assertion, plus cancellation after durable offer without a false `BOUNDED`; that test uses a single WAL and is not a production-composition certificate. These are M2 prerequisites, not M2. The exact top-level runtime now assembles both WebSocket routes and the public OI REST scheduler over one shared authority and implements bounded normal-stop draining; the OI census and schedule/body verifiers also support bounded stateful push verification. Public OI now has a strict factory-only M1 body row bound to live-reverified M0 membership, exact plan/census hashes, and completion admission, while explicitly withholding schedule-cell, freshness, cursor, and M2 claims. V8 depth REST now has an exact public/no-key retained-attempt payload, admission receipt through the shared global sequencer/bounded queue, a strict successful-body semantic verifier, and a factory-sealed process-local schedule authority that requires explicit generation revocation, binds each symbol's `(generation, first U)` and bridge-attempt sequence, and permits another bridge attempt only after an exact terminal shared-ingress receipt. The schedule authority performs no HTTP and grants no book, M2, strategy, PAPER, PnL, or order authority; WAL finality, freshness, book bridging, and M2 stay false. Still missing are the actual depth HTTP adapter/rebridge owner, runtime-owned supported V2 session closure, observed required-source census/parser-health cursor certification, remaining source parsers, an anchored M2 certificate, and the complete M2 verifier |
| V2 exact-tail and local clean-closure prerequisite | One atomic admission freeze binds the accepted terminal tail to finality and ordered `STOP`; clean dual-WAL, block, integrity-ledger, and session-closure artifacts remain current under the same held writer lease | The bounded pipeline now freezes admission synchronously, queues finality plus `STOP` in reserved control capacity, and proves exact equality among accepted, WAL, and block tails before clean close. Stop is owned by a shielded task, so caller cancellation cannot detach a live writer or release its executor early, and a stopped pipeline cannot restart. A normal close makes the dual WAL attestable; a distinct verification-only reopen can later re-read the same finalized copies without creating, recovering, syncing, or appending and does not itself claim a prior clean stop. Before the integrity ledger writes its `CLEAN` seal, the grouped-block owner durably terminalizes the exact finality tail and thereafter rejects every commit. The seal binds that terminal-marker hash and requires zero unmatched `SOURCE_GAP` and zero `VOID`. Fresh verification-only WAL, block, and ledger owners can reprove the retained seal, while new seal issuance still requires the live normally closed WAL. All combined ledger paths use the fixed `WriterLease` then ledger-lock order. A separate write-once local V2 session-closure authority binds the start receipt, planned source census, finality proof, and ledger seal while the lease is held. Exact factory-sealed `owner_stop` receipts now bind each retained WebSocket route cursor to the common finality tail, the full runtime returns the canonical market/public pair, and an optional canonical pair can be persisted for restart verification. Cancellation, empty cursors, pending gaps, swapped routes, foreign finality, and duplicate issuance fail closed. This remains a local prerequisite only: parser health, Binance upstream completeness, M2, and `SESSION_CLOSURE_SUPPORTED_V2` stay false. Observed M2 source census/certificate and verifier, independently anchored external audit/deletion tombstone, and any efficacy, PnL, expectancy, or profit evidence remain missing |
| Frozen Decimal arithmetic | One shared protocol context uses precision 34, `ROUND_HALF_EVEN`, required traps, and hostile ambient-context replay produces identical feature/execution roots | Shared owner is used by robust-z, Family A/B/C, PAPER FOK, and public-fee arithmetic; targeted hostile-context and repository gates pass |
| Family A engine | Raw USD-M kline/OI/mark/index/predicted-funding/`nq` evidence, exact selectors and staleness, exact prior windows, canonical decisions, conflict/replay and ordered episode tests | Fixed-D immutability, symbol-order independence, one sealed multiplier lineage, and checkpoint-bound restart-safe PAPER episode admission pass; the provisional promoting plan now requires exact public unauthenticated OI REST plus the two WS routes over one symbol census, but actual REST capture and feature inputs are not yet certificate-bound to the raw-record membership owner |
| Family B engine | Raw standard diff-depth and `nq`, exact right-continuous windows, 10 bp opposing depth, canonical decisions and replay root | Rule engine plus bounded atomic entry/admission/exit episode owner, registry-pinned full PAPER execution, active-position suppression, terminal release, and externally pinned restart pass; raw-membership and downstream cost/NAV wiring remain |
| Family C engine | Prior-only universe and exact timestamped member panel, current exclusion, canonical decisions and sequential h=1..6 ledger | Rule engine, sequential exit ledger, and atomic registry-pinned full PAPER admission pass; raw-membership and downstream cost/NAV wiring remain |
| Evidence Score annotation | Six capped information families, no duplicate vote, causal cutoff, fixed non-probability UI and payload conflict handling | Factory-only producer envelopes/observations, dependency-class allowlists, economic-slice alias rejection, exact-slot six-family atomic ledger, canonical pinned restore, derived causal flags, score, and UI pass. Exact 8,653-close price, exact 8,640-prior normal-flow participation, target-excluded cross-sectional, and anchored true-range documents exist as disconnected shadow projections. The successor calculation contract and sibling renderer now aggregate only price structure/momentum, participation flow, and target-excluded cross-sectional state; exhaustively tested class precedence, context isolation, exact arithmetic, deterministic payloads, and bounded duplicate/conflict handling pass. A first real price vertical slice now consumes exactly 8,654 canonical final-kline M1 rows (anchor plus 8,653 calculation closes), separates full source lineage from the close-only economic root, reuses the frozen numeric calculation, and seals `M1_ONLY_UNBOUND`, `data_through=None`, `causal_inputs_complete=False`, and `producer_ready=False`. It is not yet a producer envelope. Volatility, derivatives, and liquidity legacy signs are omitted from the successor context surface, book pressure remains unconnected, the successor payload remains `M0_M1_M2_UNBOUND`, and primary ownership is explicitly `PRIMARY_BINDING_UNAVAILABLE`. Participation/cross real adapters, typed context producers, sealed primary-decision binding, a complete M2 source-census/cursor-finality certificate, raw universe authority, and 5-minute after-cost calibration remain missing. Neither the current V1 score nor the successor shell may be presented as probability or used for promotion |
| Retrospective replay and technical exits | Frozen spec-derived rule settings and provenance; closed-bar next-open 5/15/30/60/360-minute outcomes; causal technical exits; costs and explicit exclusions | The replay runner now constructs rule engines and every post-gap state-machine reset from the frozen backtest specification, records that rule version, and includes the one-bar 5-minute horizon. The Korean report shows all horizons and the actual fixed-universe size. A pure counterfactual evaluator separately supports next-contiguous-open entry, initial/trailing stops, trend failure, time exit, fees, slippage, funding, and fail-closed split/gap/feature exclusions; it never places orders. It is not yet joined to replay because matched later decisions are required for opposite-signal exits and historical BBO/depth/latency/impact evidence is unavailable. The latest completed long run used the older miswired rule contract and is retained only as an audited invalid-for-intended-strategy diagnostic; a corrected matched replay remains missing |
| Depth capture boundedness | 10 bp feature band and guard representation, bounded buffers, fail-closed resnapshot/rebridge, zero silent loss | Code/tests pass; actual final-panel 24-hour proof missing |
| WAL/capture qualification | One selected `{10,50,100}ms × {256,1024,4096}` candidate from an actual 24-hour final-panel run with zero acknowledged loss/overflow and required margins | Model implemented; actual run missing |
| Target-time PAPER entry | Exact `D+10000ms`, sequence-valid frozen pre-target BBO/depth, continuity-only successor, level haircut, filters/bounds, FOK capacity outcomes and full-quantity VWAP | Hardened evaluator and externally pinned registry/certificate pass 62 targeted tests; authoritative clock-ledger membership and finalization-grace policy anchoring remain upstream P0 inputs |
| Mandatory exits | Exit target, partial fills, fees/funding, retry through `+30000ms`, unresolved residual retained, no spread/depth suppression | Bounded restart-pinned USD-M exit ledger now enforces ack `+10000ms`/missing-ack `+15000ms`, `h=.50` all-permissible-level walks, partial/retry sequencing through `+30000ms`, shadow-depth non-reuse, dust/non-dust residual retention, and no feature-spread/10-bp suppression; its checkpoint-bound fee certificate exposes the PAPER entry and every exact partial exit fill, while realized-funding attachment, NAV/H-end ownership, and authoritative capture/clock membership remain downstream P0 integrations |
| Public fee versioning | Fresh pre-T0 official Spot and USD-M artifacts plus exact parsed rates/hashes; 15-minute post-T0 polling and uncertain-interval classification | Exact-schedule/no-backfill, global as-of, complete-cadence finality, changed-source bracketing, and Decimal34 tests pass; final-timeline aggregation now re-resolves one PAPER entry plus `0..N` checkpoint-certified mandatory-exit slices across fee-version boundaries, retains resolved subtotals without numeric fallback, and marks only fully resolved zero-residual exits `BOTH_LEGS_COMPLETE`; authoritative capture adapter/trust anchor, frozen pre-T0 freshness, and fresh T0 captures remain missing |
| Funding cashflows | Exact funding rows and lineage, long/short sign, equality ownership convention, grace confirmation | Exact request/row/deadline, restart-pinned confirmation registry, Decimal34 sign arithmetic, and equal-ms adverse-only cashflow pass 28 tests; HTTP raw-record authority and the upstream execution-position/NAV ledger remain to be integrated |
| Isolated portfolios and NAV | Separate A/B/C × notional ledgers, fixed `25*N` capital, gross cap, idle cash, executable liquidation marks, fee reserve and daily return reconciliation | Missing |
| Alert actionability | Every promoting signal reconciled to a terminal signal-ledger root and authoritative clock ledger; exact on-time rate `>=0.99` | Arithmetic/census contract implemented; authoritative ledger replay missing |
| Multiplicity and attempt lifecycle | Fixed 365-day horizon, alpha spending registry, Holm across A/B/C, intersection-union fee/size cells, no overlap/retry gaming | Horizon/alpha primitives implemented; final inference engine missing |
| Pre-T0 final seal | Complete source/dependency/container/schema/panel/fee/rule/test manifests hashed; actual 24-hour and fresh 30-day qualification pass | Missing; T0 blocked |
| Prospective efficacy | One untouched 365-calendar-day USD-M PAPER/BBO sample reproduces positive after-cost expectancy under both 1.0x and 1.5x fees with coverage/actionability gates | Missing; no profit claim permitted |

## Latest additive checkpoint

The following completed sibling artifacts supersede the corresponding
"missing" phrases in the dense matrix rows above without changing any M2,
efficacy, probability, or deployment state:

- The V8 depth REST path now includes a real public/no-key HTTPX adapter. It
  freezes the exact built GET request at the send seam, rejects host, path,
  query, header, body, or credential drift, and claims its one-shot schedule
  token only after the concurrency gate immediately before send. Retained
  terminal evidence and admission acknowledgements bind the exact
  session/protocol/connection/generation/`first U` lineage. Canonical payloads
  use schema V9 and legacy V8 bytes fail closed; generation advance cannot
  discard a claimed attempt before terminal drain. The bounded runtime
  rebridge coordinator, persisted bridge-outcome proof, and full V8
  session/WAL/ledger/finality composition remain missing.
- A current-storage retained-market parser-health certificate re-verifies the
  CLEAN closure/finality cursor and every signed-block member, reparses every
  `usdm_market` row through strict M1, and binds full/per-stream rolling roots
  and counts. Parse failure, unknown stream, cursor conflict, or a missing
  planned stream produces a typed noncertifying result. Upstream losslessness,
  required-source completeness, OI completeness/freshness, M2, strategy, and
  PnL authority remain false.
- The combined-runtime ordering race exposed by that integration is resolved
  without changing receipt timestamps: every WebSocket, OI, depth, and census
  producer synchronously reserves its global ingest sequence immediately after
  receipt capture and before its first await, then enters the shared fair
  admission turn. Reservations are capped at 16 and released in `finally`;
  backwards clocks, overflow, pre-admission cancellation, offer failure, and
  turn mismatch permanently fail-close ingress. Gate-free OI plus three market
  frames, 137 related capture tests, and the full R4B V2 capture/lifecycle suite
  (976 passed, one Windows symlink-permission skip) now pass. This removes the
  local availability race but does not itself grant upstream losslessness, M2,
  strategy, efficacy, probability, or PnL authority.
- The exact participation M1-only slice now binds 8,641 nonempty slots, all
  retained aggregate trades/IDs/clocks, and multiplier authority. Missing
  slots and observed ID gaps remain unknown; M2, producer readiness, and
  promotion remain false. Full 30-day trade-row duplication is a known
  streaming/compact-root P1.
- The target-excluded cross-sectional directional formula is frozen as a
  disconnected, outcome-blind candidate. Its raw/M1/M2 adapter and its
  rule-versioned producer conversion remain missing.
- A historical-kline participation proxy uses only closed five-minute quote
  and taker-buy quote volume under an explicit all-trades-normal assumption.
  It is outcome-blind and explicitly not equivalent to exact aggregate-trade
  M1; it is suitable only for an exposed retrospective diagnostic.
- A directional-agreement outcome audit now requires one stable event
  identity, all 5/15/30/60/360-minute horizons, and execution/cost provenance.
  Its sibling seven-day shared UTC moving-block bootstrap retains zero-alert
  days and one draw schedule across all side/horizon/bucket cells. No matched
  successor outcomes have populated these contracts, and inference, efficacy,
  and probability remain false.

## Current verification checkpoint

At the checkpoint after the factory-only M0 membership leaf, strict USD-M
market M1 parser, compact-locator SOURCE_GAP precursor with immutable public
events and mixed-snapshot/VOID replay hardening, ordered causal-finality fence
with current root revalidation and exact WAL/block prefix proof, exact-type V2
raw-owner injection and shared ingress/recovery lifecycle, one-start-per-lease
persisted authority receipt, sealed per-route pre/post-handshake composition,
opened-root mutation guards and live readiness admission, real-disk SOURCE_GAP
lifecycle integration coverage, atomic terminal-tail shutdown with cancellation-
safe owned stop, verified normal-close dual-WAL post-close attestation, durable
clean integrity-ledger sealing, a local write-once V2 closure prerequisite,
factory-sealed per-route owner-stop cursors and their canonical runtime/closure
pair, a strict public-OI M1 body projection,
atomic Family B/C PAPER admission, mandatory-
exit, funding, multi-slice fee hardening, the factory-owned Evidence Score
boundary, OI REST plan authority, and price, participation, cross-sectional,
and volatility shadow-feature hardening, the exact three-producer full runtime,
bounded OI schedule/body and census push verification, the additive V8
depth-REST plan plus retained-attempt admission and strict body semantics, the
first exact 8,654-row final-kline M1-only price projection, the disconnected
directional successor calculation/renderer, corrected replay rule wiring with
a one-bar horizon, and the pure counterfactual technical-exit evaluator:

- `uv run ruff check .`: pass;
- `uv run pyright`: 0 errors/warnings;
- `uv run pytest -q`: 2,466 passed, 11 host-dependent skips;
- `uv run python -m compileall -q src tests`: pass.

These checks prove repository consistency only. They do not satisfy the actual
24-hour capture, fresh 30-day qualification, or untouched 365-day efficacy
requirements. They also do not change `SESSION_CLOSURE_SUPPORTED_V2=False`:
the full two-WebSocket-plus-public-OI-REST runtime now exists, and one-shot V8
depth REST attempts can cross the shared bounded admission gate and strict body
semantics. The depth trigger scheduler/runtime rebridge coordinator, observed M2 source census and
certificate, independently anchored external audit/deletion tombstone, and
efficacy/PnL/profit evidence remain absent. A separate vNext migration is also
required before approximately 104 days of host uptime so absolute monotonic
nanoseconds are not represented as RFC 8785 JSON numbers beyond its safe
integer domain.
