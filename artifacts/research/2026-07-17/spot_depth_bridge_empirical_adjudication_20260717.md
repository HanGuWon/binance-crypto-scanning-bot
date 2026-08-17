# Spot depth bridge empirical adjudication — 2026-07-17

Status: post-smoke issue record; not a protocol revision and not efficacy data.

## Evidence identity

- Failed public-data smoke session:
  `1784276271768-0090f7080dee4e90bf3e3eddea619e2`
- Raw output root:
  `C:/Users/user/Documents/Binance-bot-2-smoke-output-1784276270172`
- External audit root:
  `C:/Users/user/Documents/Binance-bot-2-smoke-audit-1784276270172`
- Canonical record range: `ingest_seq=1..7202`
- Segment SHA-256:
  `34743b45daab452e71fd43ff9e03ad597982b4999fbd8ebd791b90129afe133a`
- Manifest SHA-256:
  `ba4e1f85b4796b5831c0b5e3acb33142b5c23a7cdce46fbdf617412bfa69fbaf`
- Source-manifest SHA-256:
  `e91259f010459bd77dc4d10278174d3300fee7ea964659538b4b871f0fb4cdec`
- Independent replay-report SHA-256:
  `f7fa32e3958fa9d5eb6f385329a020a0857f47eb3d1e5194b8e75e94cca5042d`

The session is permanently classified `fatal=true` with
`stop_reason=capture_failure`. It must not be reused as a canary pass, a
prospective holdout, or profitability evidence.

## Observed Spot boundary

For each candidate snapshot, replay applied the documented Spot discard rule
`u <= L`, where `L=lastUpdateId`. In all nine observations the final discarded
event ended at `u=L`, and the first retained event began at exactly `U=L+1`.
All retained frames had already been persisted before the corresponding REST
snapshot admission decision.

| Symbol / cycle | `L` | last discarded `[U,u]` | first retained `[U,u]` |
|---|---:|---|---|
| ETH / 1 | 78896562817 | `[78896562791,78896562817]` | `[78896562818,78896562832]` |
| SOL / 1 | 29601413474 | `[29601413472,29601413474]` | `[29601413475,29601413477]` |
| BTC / 1 | 97532164629 | `[97532164621,97532164629]` | `[97532164630,97532164651]` |
| ETH / 2 | 78896562986 | `[78896562957,78896562986]` | `[78896562987,78896562998]` |
| SOL / 2 | 29601413492 | `[29601413492,29601413492]` | `[29601413493,29601413493]` |
| BTC / 2 | 97532164989 | `[97532164970,97532164989]` | `[97532164990,97532165004]` |
| ETH / 3 | 78896563111 | `[78896563095,78896563111]` | `[78896563112,78896563129]` |
| BTC / 3 | 97532165096 | `[97532165079,97532165096]` | `[97532165097,97532165124]` |
| SOL / 3 | 29601413495 | `[29601413494,29601413495]` | `[29601413496,29601413496]` |

The literal runtime predicate `U <= L <= u` rejected 9/9 candidates. The
pre-existing DESIGN predicate recorded in
`r4b_protocol_errata_spot_depth_v1.yaml`,
`u > L && U <= L + 1 && L + 1 <= u`, accepted 9/9 candidates.

The three USDⓈ-M Futures books accepted their first snapshots under the
unchanged Futures predicate `U <= L <= u`; their subsequent `pu == previous u`
links were continuous.

## Adjudication

The runtime and offline replay must implement the already-frozen DESIGN
reconciliation consistently:

- Spot: discard `u <= L`; bridge target is `L+1`.
- USDⓈ-M Futures: discard `u < L`; bridge target remains `L`, with `pu`
  continuation.
- Spot `U <= L+1 <= u` is accepted; `U > L+1` is stale.
- Futures `U=L+1` with `L` below the range remains stale.
- If every buffered Spot event has `u <= L`, the current bounded snapshot cycle
  waits for a persisted successor instead of accepting or inventing one.

This adjudication is labelled **DESIGN**, not **OFFICIAL**. It reconciles the
official Spot instructions that simultaneously require discarding `u <= L`,
say the first retained event contains `L`, and define a live gap only when
`U > local_id+1`. It does not synthesize Spot `pu`, enable Spot Family B, amend
the downloaded Pro YAML, start a prospective holdout, or establish positive
expectancy.

## Required post-fix smoke checks

Run in a new raw and audit root. The run must retain closed evidence, have no
fatal closure, bridge all six books, use one successful initial depth-snapshot
cycle per book under normal conditions, and preserve online/offline bridge
parity. Any truncated required REST payload, unresolved reconstruction,
sequence gap, malformed record, overflow, ingest gap, or integrity failure is a
fail-closed result. A short smoke remains infrastructure evidence only.
