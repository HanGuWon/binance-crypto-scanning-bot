# causal_retest_v1 durability contract

`causal_retest_v1` is a shadow/research protocol. It does not modify frozen
`r2_pit_htf_exec`, `SignalStateMachine`, PAPER, Discord, or any order path.

## Durable lifecycle

Each admitted opportunity has one current row in `retest_lifecycles` and an
append-only history in `retest_transitions`. A transition and its current
snapshot are committed in one database transaction. The transition identity
binds campaign ID, campaign manifest SHA, opportunity ID, protocol version,
source/destination stages, decision/bar clocks, and persistence time through a
canonical payload SHA.

Replaying the same transition ID and content is an idempotent no-op. Reusing an
ID with any different bound content is a hard conflict. A transition must start
at the durable current stage, use the registered campaign manifest/protocol,
and cannot follow a terminal stage. This prevents duplicate or out-of-order
history from changing the denominator.

## Restart and denominator

`serialize_lifecycle`/`restore_lifecycle` preserve ARMED and RETEST_TOUCH
state. `recover_after_restart` continues only when completed-bar continuity is
proven; otherwise it emits typed `CENSORED(RESTART_GAP)`. A durable RAW_C0 row
is never silently promoted to ARMED.

`SqlRepository.retest_lifecycle_counts` reports active, touched, and each
terminal state (`READY`, `INVALID`, `TIMEOUT`, `CENSORED`) for one registered
campaign manifest. Terminal rows are immutable, so every admitted opportunity
remains visible in the denominator and can receive only one terminal state.

## Live/replay parity

`run_retest_adapter_parity` requires distinct live and replay adapters. It
compares only cases with recorded decision-time historical BBO. Missing BBO is
never replaced with a proxy; those cases produce
`INCONCLUSIVE_NO_HISTORICAL_BBO`, and a complete PASS is impossible while any
case remains unobservable. This parity result is research evidence only and
does not activate or promote the successor.
