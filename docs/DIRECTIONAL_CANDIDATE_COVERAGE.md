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

Promotion is per direction. A Futures LONG receipt cannot promote Futures
SHORT, and a Freqtrade result cannot substitute for the scanner's canonical
replay and forward evidence.
