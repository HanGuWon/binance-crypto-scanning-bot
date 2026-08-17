# R4B V2 participation aggTrade M1 projection

`ParticipationAggTradeM1ProjectionV2` connects retained strict
`UsdmAggTradeM1V2` rows to the frozen participation-flow arithmetic. It remains
an observed M1 calculation, not a producer envelope, exchange completeness
certificate, probability estimate, promotion, or order instruction.

## Window and assignment

- The decision scope is the current closed 5-minute slot plus exactly 8,640
  prior slots.
- Trades are assigned by Binance transaction time `T` to right-continuous
  intervals `[slot_open, slot_open + 5m)`. A trade exactly on a 5-minute
  boundary belongs to the new slot.
- Every retained row preserves aggregate ID, first/last raw trade IDs, `p`,
  `q`, `nq`, buyer-maker side, `T`, exchange event `E`, both local receipt
  clocks, and all M0/M1 source hashes.
- `T`, `E`, and receipt remain separate clocks. Rows must satisfy
  `T <= E <= receipt <= current D`.
- Sessions and connections may change. Frame order is checked only within one
  connection generation, and no global frame continuity is invented.

An absent slot is recorded as `UNKNOWN_NOT_ZERO`. It is never converted into a
zero-volume or neutral flow bar. Observed aggregate-ID or raw-trade-ID gaps also
withhold the numeric calculation. Even contiguous IDs and nonempty slots do not
prove that Binance emitted no uncaptured event; only a later M2 source census
and finality join can establish that authority.

## Multiplier and economics

Notional is computed only with a factory-sealed
`FamilyAContractMultiplierV2` whose symbol, venue, promoting plan, attempt, and
effective interval cover the full 8,641-slot window. The projection never
injects an unexplained multiplier of one.

For each slot the exact retained trades produce:

- signed normal notional: aggressive normal buys minus aggressive normal sells;
- absolute normal notional: aggressive normal buys plus sells;
- total trade notional from full `q`;
- signed share: signed normal notional divided by total trade notional.

All-`nq=0` slots remain numerically inconclusive. The same factory-sealed
`ParticipationFlowCalculationV2` is used by the existing Family-B shadow path
and this M1 adapter, so the prior-only MAD, activity support, direction, and
strength formula are not duplicated.

The full source root binds every M1 canonical/source field and the multiplier
authority. The separate economic root binds only the ordered trade economics,
derived slot values, and multiplier version. Receipt or source-lineage changes
therefore cannot silently change the economic identity.

## Authority boundary

The projection always seals:

- `authority_status = M1_ONLY_UNBOUND`
- `data_through_ms = null`
- `m2_certificate_sha256 = null`
- `exchange_trade_capture_complete = false`
- `causal_inputs_complete = false`
- `producer_ready = false`
- `promoting_eligible = false`

`NUMERIC_READY_M1_ONLY` means only that the retained observed rows satisfy the
frozen arithmetic inputs. It is not a claim that the exchange trade set is
complete or that the family is eligible for an alert decision.
