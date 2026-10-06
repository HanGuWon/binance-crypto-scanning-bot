# Directional candidate coverage

This document records the actionable coverage audit for L35-01. A rule is an
operational entry candidate only after its own historical and prospective
receipts have been independently reviewed. A signal family appearing in the
scanner is not evidence that the family is promoted.

| Market / direction | Current rule family | Live data contract | Historical evidence | Prospective evidence | Promotion status | Blocker |
| --- | --- | --- | --- | --- | --- | --- |
| Spot LONG | `BREAKOUT_LONG` | Closed 5m candle; existing R2 gates | Frozen R2 structural/retrospective contract | Existing shadow observation only | Alert candidate / exit-warning capable | No directional promotion receipt |
| Spot SHORT | N/A | Spot short is not a futures-short action | Not applicable | Not applicable | Informational only | Spot exits and futures shorts are different actions |
| Futures LONG | `BREAKOUT_LONG` family shape | Candidate contract defined in `research.futures-bidirectional.v1.yaml` | Not authorized for use until successor campaign receipt | Not started | `WAITING_FOR_AUTHORITY` | No released historical authority and no forward receipt |
| Futures SHORT | `BREAKDOWN_SHORT` | Existing R2 family shape; closed 5m candle | Frozen R2 structural/retrospective contract | Existing shadow observation is not a promotion receipt | Alert candidate only | No successor directional promotion receipt |
| Futures LONG + SHORT pair | `futures-bidirectional-v1` | Strict-prior 15m/1h, observed BBO for live gate, next-bar-open entry | Successor preregistration only | Successor shadow/replay contract only | `WAITING_FOR_AUTHORITY` | Both directions need separate passing receipts |

The successor contract intentionally starts at `WAITING_FOR_AUTHORITY`. The
sealed Phase-S result payload is not read or treated as evidence here. The
research runner must create source, data-authority, universe, and trial hashes
before any result is inspected. Historical absence of exact BBO evidence stays
explicit and cannot be converted into a passing live execution gate.

The executable Phase 1 receipt is written to
`artifacts/prospective/futures-bidirectional-v1/`. It contains
`source-freeze.json`, `data-authority.json`, `fixture-replay.json`,
`historical-summary.json`, `promotion-evidence.json`, and
`validation-manifest.json`. These machine-local receipts are intentionally
excluded from the repository and bind the run to the existing local
`data/backtest` junction. The Google Drive `BINANCE_CRYPTO_BACKTESTING_ARCHIVE`
is retained as cold archive; no raw shard is added to the local workspace for
this campaign.

Forward shadow status is separate from historical status. A historical PASS
does not make the successor active, and a short smoke process does not satisfy
the preregistered forward observation window. Independent review must recompute
the receipt hashes before promotion can move beyond `CONTINUE_OBSERVING`.

The successor runtime observer is now implemented but remains disabled by
default. It is enabled only with a distinct `futures-bidirectional-*` campaign
ID, a matching `worktree-source-v1` identity, and a future activation boundary.
It records both Futures directions in the existing idempotent shadow evidence
store and cannot create a production decision, paper position, Discord message,
or exchange order.

Promotion is per direction. A Futures LONG receipt cannot promote Futures
SHORT, and a Freqtrade result cannot substitute for the scanner's canonical
replay and forward evidence.
