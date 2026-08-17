# R4B V2 price kline M1 projection

`PriceKlineM1ProjectionV2` is the first source-specific vertical slice for the
`PRICE_STRUCTURE_MOMENTUM` calculation. It is deliberately not a producer
envelope, M2 certificate, probability estimate, alert promotion, or order
instruction.

## Exact input

- Exactly 8,654 factory-sealed `UsdmKline5mM1V2` final updates are required.
- The rows are canonicalized by `bar_open_ms`; duplicate or conflicting slots
  are rejected.
- The first row is a continuity anchor. The remaining 8,653 ordered closes are
  the complete economic input to the frozen R1/R12 calculation.
- Slots must be contiguous, `x` must be true, and symbol, USD-M route,
  `<symbol>@kline_5m` stream, promoting plan, protocol, and strict parser must
  agree.
- Session and connection changes are permitted. Frame sequences need only be
  ordered within one connection generation; they are not treated as a global
  gap-free cursor.

Each source entry preserves Binance exchange event `E`, candle data time `k.T`,
local receipt wall time, and receipt monotonic time as separate fields. The
causal ordering is `k.T <= E <= receipt <= D`, where `D` is the current final
bar's `k.T + 2,001 ms` cutoff.

## Lineage and economics

The ordered source root binds every M1 canonical digest and M1/M0 source
identity, including leaf/raw/M1 hashes, capture/protocol/parser/plan hashes,
session, connection generation, frame and ingest sequence, clocks, slot, close,
and decision scope. It does not certify that every exchange event was captured.

The separate economic root contains only the 8,653 calculation slots and
ordered target closes, plus target identity and plan. Consequently a high/low
change alters full source lineage but cannot alter the close economic root or
the price calculation.

The projection reuses the same factory-sealed `PriceClosePathCalculationV2`
used by the pre-existing Family B shadow path. No `open_event_id` is synthesized
from a final M1 update.

## Authority boundary

The projection seals all of the following, even when its numeric calculation
has status `READY`:

- `authority_status = M1_ONLY_UNBOUND`
- `data_through_ms = null`
- `m2_certificate_sha256 = null`
- `causal_inputs_complete = false`
- `producer_ready = false`
- `promoting_eligible = false`

`assumed_closed_bar_through_ms` records the current final candle's `k.T`
separately and is not a certified M2 data-through claim. A later adapter must
join live-reverified membership, route finality/census, and the complete causal
input set before a producer envelope can become eligible.

The projection canonicalizer inherits the upstream M1 RFC 8785 safe-integer
domain because every row is canonically reserialized before projection. Its
derived decision cutoff is checked against the same domain before hashing.
