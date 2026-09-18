# Runtime Repair Plan — 2026-08-17

This plan leaves the frozen R2 formulas, thresholds, market/direction restrictions,
and R4B efficacy gates unchanged. Repairs are separated from strategy research.

## Status — 2026-08-18 (prospective shadow observer)

Implemented as a single local source-freeze commit (not pushed): the
`shadow_er_context_v1` successor is no longer selectable as a production
`entry_policy`; it is a simultaneous observer gated by
`shadow.observation_enabled` (default `false`). Production keeps
`entry_policy: r2_pit_htf_exec`. The observer:

- evaluates R2 and the shadow on the SAME causal 5m feature/context cutoff;
- writes one durable comparator observation per raw C0 opportunity (idempotent,
  conflict-loud) and a per-close coverage ledger (raw C0 count == comparator
  rows) proving no silent observation holes;
- derives a frozen policy SHA-256 and config SHA-256 for immutable provenance;
- is informational-only: it cannot enter the state machine, create a PAPER
  position, or cause a Discord recommendation.

Scientific status remains `SHADOW_SUCCESSOR_ONLY`. A prospective campaign must
be preregistered with its own activation timestamp after source/config/policy
freeze; no prospective time has been started from this environment.

## P0 — release and prospective-governance blockers

1. Establish a canonical Git commit/tag and source manifest before another
   prospective efficacy run. The current workspace still has no Git commit.
2. Migrate R4B absolute monotonic-nanosecond serialization before a 365-day run
   can cross the RFC 8785 safe-integer boundary.
3. Complete the declared R4B M2, session-closure, terminal-position, funding,
   fee, and isolated-NAV authorities before Family A/B/C promotion.

## P1 — runtime correctness

- [x] Reject R2 score-based confirmation.
- [x] Reject R2 configurations without the frozen 5m decision clock and
  subscribed 15m/1h contexts.
- [x] Default missing historical spread evidence to `None`, never zero.
- [x] Stop market-ingestion callbacks after durable signal/outbox persistence;
  Discord provider I/O belongs to the independent outbox worker.
- [ ] Add periodic universe refresh with deterministic ranking hysteresis and
  bounded subscription rotation.
- [ ] Add persistent Spot first-seen age authority instead of relying on an
  optional exchange-info onboarding field.
- [ ] Centralize Binance REST rate-limit state, bounded embargo, and circuit
  behavior.
- [x] Batch bootstrap candle persistence into one transaction per bounded REST
  response instead of one commit per candle.
- [ ] Move remaining blocking database work off the asyncio market-ingestion
  path without introducing concurrent SQLite writers.
- [ ] Add explicit operator resolution tooling and metrics for `uncertain`
  Discord outbox rows.

## P1 — research/runtime boundary

- [ ] Keep R4B depth/FOK and successor evidence shadow-only until their declared
  authority and prospective gates are complete.
- [ ] Separate retrospective PAPER lifecycle alerts from any future
  user-tracked-position monitoring feature.
- [ ] Re-run the corrected matched alert replay before interpreting directional
  score monotonicity.

## Verification contract

Every operational patch set must run:

```text
uv run ruff check .
uv run pyright
uv run pytest -q
uv run python -m compileall -q src tests
uv run signalbot validate-config --config config/settings.example.yaml
uv run signalbot run --config config/settings.example.yaml --dry-run
```

A scientific-rule change requires a new protocol/rule version and a separate
pre-registered experiment. It must not be mixed into operational repair commits.
