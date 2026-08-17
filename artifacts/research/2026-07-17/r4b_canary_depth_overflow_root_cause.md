# R4b canary Futures BTCUSDT depth-overflow root cause

Status: infrastructure diagnosis only; no efficacy or profitability claim.

## Evidence authority

- Capture session: `1784282167461-c9a46255b0424aa2bb80deb9c40dc101`
- Closed capture range: ingest sequence `1..9410158`
- Final segment SHA-256: `faaa3b7f...` (full digest remains in the closed manifest)
- Prior closed-authority depth report SHA-256: `afba6db442897ae7c996a3ed1a76d9a12cd17ec3fe12b13c91cb2e70fa4b1a35`
- Replay runtime: CPython `3.12.13`
- Replay scope: all 9,410,158 ingest positions were checked in order. Only Futures `BTCUSDT@depth@100ms`, its connection transitions, and its REST depth snapshots were interpreted as book state; other records advanced the contiguous ingest cursor without affecting this book.
- Outer-frame checks: every consumed record retained the capture storage frame digest, zstd checksum, content-size, and one-record framing checks performed by `consume_segment_lines`.

The capture had already passed full closed-authority and segment-chain verification. This focused replay is a cause diagnostic and is not a replacement authority report.

## First failure: level bound

- Ingest sequence: `5677224`
- Wall receipt (UTC milliseconds): `1784289667887`
- Monotonic receipt (ns): `224831199443600`
- Connection / frame: `capture-futures-public-1-g000003` / `1403005`
- Prior state: `valid`
- Prior bid / ask levels: `9976 / 8636`
- Event sequence: `U=11069591313039`, `u=11069591387069`, `pu=11069591312900`
- Event bid / ask changes: `893 / 928`
- Raw event bytes: `38466`
- Result: configured `10000` levels-per-side bound rejected the delta and cleared the reconstruction into `awaiting_snapshot / level_overflow`.

This is the causal first failure. It occurred before either the local reconstruction buffer or the capture handoff queue overflowed.

## Second failure: reconstruction buffer bound

- Ingest sequence: `8793303`
- Wall receipt (UTC milliseconds): `1784293271517`
- Monotonic receipt (ns): `228434829848700`
- Connection / frame: `capture-futures-public-1-g000003` / `3661749`
- State immediately before the rejected append: `awaiting_snapshot / level_overflow`
- Buffered events: `35326`
- Buffered bytes: `67105571`
- Incoming event bytes: `14010`
- Configured byte bound: `67108864` (64 MiB)
- Projected bytes: `67119581`, or `10717` bytes above the bound
- Event sequence: `U=11069933332901`, `u=11069933354423`, `pu=11069933332864`
- Event bid / ask changes: `333 / 324`
- Result: `awaiting_snapshot / buffer_overflow`, with the bounded buffer cleared.

Elapsed monotonic time from the level overflow to the buffer overflow was `3603.6354051` seconds. No accepted resynchronization snapshot restored this book during that interval.

## Conclusion

Increasing the queue or reconstruction-buffer cap alone is not a valid repair. The observed chain was:

1. an arbitrary 10,000-level state bound invalidated an otherwise sequence-contiguous Futures book;
2. the invalidation did not produce an accepted live resnapshot/rebridge;
3. the reconstruction buffer accumulated for about one hour;
4. its exact 64 MiB byte bound then failed closed.

The V2 repair must therefore address both bounded book representation and resynchronization. It must preserve sequence causality, make 10 bp feature-band completeness explicit, request/retry a fresh snapshot on any representation overflow or guard-band exhaustion, and prove final-panel recovery and zero unresolved gaps in the 24-hour qualification. Merely raising either cap is forbidden by the precision-adjudicated protocol.

