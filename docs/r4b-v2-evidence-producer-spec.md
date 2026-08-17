# R4B V2 Evidence Producer Specification

Status: disconnected shadow implementation. The factory-owned score boundary,
dependency ledger, price, participation, volatility, and target-excluded
cross-sectional calculation documents exist. Derivatives-positioning and side-
symmetric liquidity-execution producers and all six family-to-envelope
authority adapters remain unimplemented. Combined-query USD-M
`aggTrade`/`kline_5m` M1 parsing and capture/M2 prerequisites now include fail-
closed SOURCE_GAP handling, an exact-type per-route raw-owner/ingress boundary,
one sealed persisted start receipt per lease acquisition, pre/post-handshake
authority and runtime-readiness admission, opened-root mutation checks, and
real-disk lifecycle coverage. No score producer is bound to those authorities,
the top-level full-plan runtime and complete M2 source-census/cursor certificate
remain missing, and this score therefore cannot promote, suppress, or rank an
alert.

This specification turns the idea "more indicators point the same way, so show
stronger agreement" into a causal and auditable design. It does **not** count
indicator names. It counts at most one capped contribution from each distinct
information family. It was written from source contracts and rule definitions,
without consulting backtest PnL, win rate, or prospective outcomes.

## Decision and current boundary

The idea is suitable for an explanatory shadow score, with one important
qualification: several familiar indicators are correlated transforms of the
same observations. EMA, MACD, RSI, and recent returns therefore cannot be four
votes. The existing `EvidenceScoreDecisionV2` already enforces exactly six
families, a per-family cap, unique source-feature IDs, causal timestamps, and a
non-promoting result. `EvidenceAlertAnnotationV2` already labels the result as
agreement rather than probability and cannot alter a primary A/B/C decision.

Four calculation documents exist, but none has the authority adapter that
converts live-reverified M0/M1/M2 evidence into its common producer envelope;
the other two family calculations do not yet exist. Until all six producers and
adapters are implemented, qualified, and frozen, callers must not fabricate
observations directly and the score must remain disconnected from Discord and
all primary decisions.

Two status labels are used throughout this document:

- `FROZEN`: already required by the implemented V2 protocol or an existing
  sealed A/B/C feature contract. An implementation must preserve it.
- `RESEARCH_REQUIRED`: a candidate design choice that must be specified on
  development data without inspecting the outcome being reserved for efficacy
  evaluation. It is not an approved threshold or rule.

## Global producer contract

The following rules apply to all six producers.

### Identity, time, and readiness

- `FROZEN`: venue is public Binance USD-M Futures and the symbol is a normalized
  `*USDT` contract. Spot evidence cannot enter this score.
- `FROZEN`: the decision slot is one fully closed 5-minute bar `[k.t, k.T]` and
  the cutoff is `D = k.T + 2,001 ms`.
- `FROZEN`: every economic data-through or transaction time is at or before
  `k.T`. A Binance exchange observation time such as kline `E` may be after
  `k.T`, but it and every local receipt or completion must be at or before `D`.
  All rolling inputs are prior or current closed bars. Equality at `D` is valid;
  one millisecond later is invalid.
- `FROZEN`: gaps, incomplete candidate sets, unavailable normal-quantity
  conversions, invalid decimals, zero normalization scale, or a missing
  required member fail closed. Missingness is never converted into a neutral
  vote.
- `FROZEN`: all arithmetic entering a V2 evidence document uses the protocol
  Decimal context and sealed capture/schema/clock lineage. The float-based
  legacy indicator engine is not an authoritative V2 producer.
- `FROZEN`: producer outputs use the existing readiness values `READY`,
  `FEATURE_NOT_READY`, `INCONCLUSIVE_DATA`, or `DATA_INVALID`. A market regime
  that is merely unattractive is not missing data and must not be disguised as
  a readiness failure.

### Absolute direction and strength

- `FROZEN`: direction is absolute market direction: `+1` means the family itself
  points bullish for the named symbol, `-1` means bearish, and `0` means no
  directional contribution. It is never "agrees with the primary LONG/SHORT".
- `FROZEN`: a producer may not receive primary side, primary entry status,
  decision reasons, future returns, fills, NAV, PnL, or an efficacy label as an
  input. Flipping a feature because the primary side is SHORT is forbidden.
- `FROZEN`: one family contributes at most `1_000_000` strength micros. A ready
  neutral contribution has direction zero and strength zero under the current
  schema.
- `RESEARCH_REQUIRED`: the monotone map from a family scalar to strength micros,
  including any dead zone, scale, clipping rule, and treatment of conflicting
  subfeatures. The common candidate is a causal robust magnitude or a sealed
  prior empirical rank followed by a cap; no numerical cut point is selected
  here.

### Source-feature ownership and no duplicate votes

`source_feature_ids` must identify the canonical **economic feature slice**, not
merely the final indicator name. Naming the same close history
`RSI14`, `MACD_HIST`, and `EMA_CROSS` does not create three independent IDs.

Each producer must emit:

1. a source lineage root;
2. a canonical feature-slice root;
3. the atomic dependency classes it consumed, such as target close path,
   normal aggTrade flow, mark/OI positioning, standard diff-depth, or
   target-excluded cross-section; and
4. a deterministic producer-version ID.

`FROZEN`: the aggregator rejects an identical `source_feature_id` or evidence
hash in two families. The implemented factory-sealed producer layer and bounded
ownership ledger also reject aliases of the same economic slice and finalize
exactly six families atomically. Shared capture records may support genuinely
different facts, such as close return and high-low range, but the dependency
overlap must be explicit. Such overlap is correlation, not extra independence.

### Raw-authority layers

- `M0` is implemented: one factory-only membership snapshot can be minted only
  after the existing live verifier proves an exact canonical raw line in the
  current signed, non-VOID block and integrity chain. It binds the trusted WAL
  authority, certificate, storage roots, stream/segment, record envelope, and
  raw payload hash. Because a later integrity observation can VOID that prefix,
  the durable leaf seals `current_authority_claimed=false` and
  `live_reverification_required=true`; every authority use must re-run the live
  verifier in the same consuming operation. The reverification assertion
  returns no reusable current-authority token.
- `M1` is partially implemented for the current combined-query USD-M market
  route. Its one consuming operation live-reverifies the M0 leaf and parses only
  retained raw bytes. It accepts exact `{stream,data}` wrappers for
  `aggTrade` and `kline_5m`, preserves `q`, RPI-excluded `nq`, `st`, kline `E`,
  `k.T`, and `k.x`, and emits factory-only canonical typed rows. Missing,
  duplicate, or unknown keys, non-string decimals, wrong symbols, wrong routes,
  and `st != 1` fail closed. The exact inner-key requirement and local numeric
  invariants are an explicitly frozen project contract stricter than Binance's
  official inner JSON Schema, not a claim about that schema. Raw bytes remain
  retained in M0 on parser rejection.
- The M1 v1 access contract is combined-query only and is plan-hash-bound to
  `wss://fstream.binance.com/market/stream?streams=`. The capture envelope keeps
  `symbol=None` before decode; M1 derives the symbol by matching the combined
  stream, outer payload symbol, and nested kline symbol. Raw-mode support would
  require a separate plan and parser version.
- An M2 precursor is implemented in the capture-integrity ledger. Exact local
  allocated-ingest loss remains `DATA_GAP`; unknown upstream loss at session
  start, disconnect, or proactive recycle is a two-phase `SOURCE_GAP`. Its
  `OPEN` event is durable before recovery can be claimed, and its `BOUNDED`
  event is issued by the ledger while holding its append lock: the ledger uses
  its root-bound block policy, signing authority, stream group, and segment to
  reload the first retained successor (and predecessor when one exists) from
  the current signed chain. Callers supply only the right ingest ID, never
  connection, generation, frame, receipt time, block hash, or record hash.
  Record-sized membership certificates are not embedded. The event stores
  compact block/ingest/exact-line-hash locators, so closure capacity is bounded
  independently of accepted raw-record size. Current-authority replay reloads
  the signed records and compares both locators and cursors. Public event values
  are detached deep copies, source message count remains explicitly unknown,
  and plan/route/stream census and process boot cannot drift. Capacity for each
  unmatched closure is reserved.
- A second M2 precursor is implemented at the existing bounded handoff and
  single durable writer boundary. One positive, non-terminal ordered fence is
  admitted only when its requested ingest sequence equals the current accepted
  tail. The writer requires the same sequence to equal the exact WAL durable ACK
  and finalized signed-block tail, re-reads the current single/dual WAL and
  grouped-block root-binding bytes, compares the complete canonical WAL/block
  prefix, verifies the final manifest/container/signing authority, and remains
  writable afterward. The event receipt retains its ordered monotonic time,
  while `prefix_proof_sha256` excludes operation timing and is stable for the
  same authority and exact prefix. The artifact verifier can recompute a
  historical fence prefix even after later blocks have been appended. This
  proves storage finality for an exact retained prefix; it does not prove source
  census completeness, parser health, or absence of upstream loss.
- Additional M2 prerequisites are implemented but not assembled into the top-
  level capture runtime. Per-route V2 adapters inject raw retention and recovery
  hooks into the existing sole WebSocket owner; only the exact V2 factory/
  lifecycle pair and exact shared ingress are admitted, and the ingress starts
  after the recovered WAL tail. Every producer captures its local receipt and
  synchronously reserves the next global ingest sequence before its first
  await; the bounded admission gate then serves those reservations in order.
  WebSocket receipt capture remains the first statement after iterator yield.
  OI and depth REST `completion_admission_*` fields identify this local
  reservation receipt (the separately retained attempt-end clocks still identify
  HTTP completion), not the later instant at which a blocked queue offer runs.
  Recovery successors retain the admission gate through causal finality and
  SOURCE_GAP bounding. Pending reservations are capped at 16; a backwards local
  receipt, cap overflow, pre-admission cancellation, turn mismatch, or failed
  bounded offer permanently fail-closes that ingress so a later record cannot
  cross an allocated sequence hole. This proves deterministic process-local
  receipt order, not Binance emission order or upstream losslessness. Each exact
  OS writer-lease acquisition can
  irreversibly claim only one canonical session start. Successful persistence
  yields a factory-only receipt that seals its path, bytes/hash, file identity/
  link count, and exact lease claim while binding the plan/protocol, qualified
  dual-WAL roots, grouped-block/signing authority, integrity-ledger contract,
  and canonical storage paths. The sealed per-route composition requires the
  exact receipt and production types, verifies construction-time opened-root
  identities and live pipeline/ledger readiness, holds the lease across the
  asynchronous connector handshake, and revalidates both before and after
  context entry before `CONNECTED` or frame consumption. WAL, block, and ledger
  mutations recheck current binding bytes and their opened root identities
  before and after mutation. Real-disk lifecycle coverage proves durable
  SOURCE_GAP `OPEN`, successor WAL/block finality, `BOUNDED` plus restart
  assertion, and no false `BOUNDED` on cancellation; that integration fixture
  uses a single WAL and is not a production certificate.
- `M2` is still missing: one signed ingress cursor/finality certificate must
  prove the complete required source census through the decision cutoff across
  both WebSocket routes and public OI REST, parser health, durable
  acknowledgement, block-tail finality, and recovery serialization. The top-
  level full-plan runtime, actual OI REST capture, remaining parsers, and V2
  session closure are also missing.

M0 proves retained-record membership only. M1 proves exact interpretation of
one retained member at issuance and deliberately still reports current
authority, cursor, and causal-input completeness as false. No producer may
become READY for promotion until its M1 rows are bound to a live-reverified M2
source census and finality certificate. A bounded `SOURCE_GAP` proves its two
retained endpoints, not that Binance emitted no uncaptured messages between
them, so it is not itself an M2 completeness certificate.

## Producer specifications

There are exactly six information families. The reusable fields named below
are evidence already sealed by A/B/C factories; they are candidates for reuse,
not permission to count the same field again.

### 1. `PRICE_STRUCTURE_MOMENTUM`

#### Price raw source

- `FROZEN`: public USD-M WebSocket route `usdm_market`, stream
  `<symbol>@kline_5m`, fully closed candles only.
- The owned economic slice is the target symbol's ordered close-price path.
  High-low range belongs to volatility and taker flow belongs to participation.

#### Price reusable sealed fields

- Family A: `r12_previous`, `rz_r12_previous`, and `rz_r1_current` in
  `FamilyAEntryFeatureEvidenceV2`.
- Family B: `bar_return_current` and `rz_bar_return_current` in
  `FamilyBFeatureEvidenceV2`. These are first/last book-mid return features, so
  their dependency is standard diff-depth rather than the kline close path and
  must not be silently substituted for a close-return feature.
- Family C: each member's `current_three_bar_return` in
  `FamilyCFeatureSnapshotV2`.

No one existing field is a universal, primary-independent V2 price producer.
The legacy `FeatureEngine` computes EMA 9/20/50/200, Wilder RSI 14, and MACD
12/26/9 with floats and without V2 lineage; those values are research references
only.

#### Price shadow factory

- `FROZEN` for the disconnected shadow version: one factory-sealed document
  owns exactly 8,653 contiguous closed 5-minute kline rows. Its economic slice
  contains symbol, venue, plan, slot, and ordered closes only; high/low changes
  alter full raw lineage but cannot alter the price slice or feature values.
- For `h in {1, 12}`, it forms 8,640 prior log returns and one current log
  return. The prior-only scale is `1.4826 * MAD(prior_h)`. The current signed
  magnitudes are `u1 = r1_current / scale1` and
  `u12 = r12_current / scale12`; the composite is `(u1 + u12) / 2`.
- Strength is
  `floor(1_000_000 * abs(composite) / (1 + abs(composite)))`. Direction is the
  composite sign only when this quantized strength is positive; sub-micro
  magnitudes are sealed as neutral `0/0` so they can enter the common READY
  contract without a directional zero-strength contradiction.
- The factory records prior location and MAD for audit but deliberately does
  not center current direction on the prior median. Centering would describe
  surprise relative to recent drift rather than absolute price direction.
- EMA/RSI/MACD are not inputs to this shadow version. If later retained, their
  Decimal formulas and conflict rule require a new producer version and still
  collapse to this single price-family vote.
- Raw M0 membership, the implemented kline M1 rows, and M2 cursor finality are
  not yet bound to this document; it is explicitly non-promoting and has no
  envelope adapter.

#### Price causal window and missingness

- `FROZEN`: a reused V2 robust-z value has exactly 8,640 prior observations and
  excludes the current observation from its location and scale. Family A entry
  history uses 8,653 prior bars because its 12-bar transform also needs the
  exact 8,640-value normalization window.
- `FROZEN` for this shadow version: exactly 8,653 closes produce aligned
  `r1[t-8640..t]` and `r12[t-8640..t]` series, with the current observation
  excluded from both 8,640-value normalization windows.
- Any unclosed candle, non-contiguous slot, non-positive close, late receipt,
  insufficient warm-up, or zero robust scale makes the family non-ready.

#### Price absolute direction

- Direction is the sign of one price-only composite, computed without the
  primary side. An oscillator state such as "oversold" is not automatically a
  bullish forecast; the mapping from level, slope, and price structure must be
  predeclared in the producer version.

#### Price strength

- `FROZEN` for the shadow version: the prior-only MAD scaling, two-horizon
  average, monotone saturating curve, floor quantization, and `1_000_000` cap
  above. Their predictive efficacy is not frozen or claimed; it remains a
  qualification and prospective question.

#### Price duplicate prevention

- EMA levels/crosses, MACD line/signal/histogram, RSI level/slope/divergence,
  and close returns all share one target-price-path ownership group. They may
  influence the one composite, but only one price-family vote is emitted.
- A target close used here must not be reintroduced as a separate
  cross-sectional target vote. The cross-sectional producer below therefore
  requires target-excluded context.

#### Price circularity

- A: **direct/high**. A entry already uses `abs(rz_r12_previous)` and
  crowd-aligned `rz_r1_current`.
- B: **direct/high** if the B return feature is reused; B1/B2 already branch on
  `rz_bar_return_current` aligned to flow.
- C: **direct/high**. The target's three-bar return is inside `g0` and
  `lag_score`.

### 2. `PARTICIPATION_FLOW`

#### Participation raw source

- Preferred candidate source: public USD-M WebSocket route `usdm_market`,
  `<symbol>@aggTrade`, restricted to normal futures quantity with the sealed
  contract multiplier and a complete closed-bar trade window.
- Alternative research source: closed 5-minute kline quote volume and taker-buy
  quote volume. A producer version must select one canonical source model; it
  cannot present aggTrade imbalance and kline taker delta as independent votes.

#### Participation reusable sealed fields

- Family A: `flow_current` in entry evidence and `flow_previous`/
  `flow_current` in exit evidence.
- Family B: `flow_imbalance_current` and
  `rz_flow_imbalance_current`.
- The legacy volume module has causal D3/D12 normalized taker delta and a
  normalized VPCI candidate with short 5, long 20, ATR 20, quote-VWMA 5, and
  slope lag 3. Its outputs are floats and are not sealed V2 evidence.

#### Participation shadow factory

- `FROZEN` for the disconnected shadow version: a factory-owned flow-only bar
  projection reuses the exact normal-aggTrade and closure validators, binds the
  canonical trades-plus-closure raw slice, and exposes signed normal notional,
  normal notional, total trade notional, normal-flow imbalance, and signed
  share. It contains no price-return or order-book feature.
- A second factory consumes exactly 8,640 prior flow-only bars plus the current
  closed bar. The directional scalar is current signed share; absolute
  direction is never flipped to match a primary side.
- Empty bars, zero total notional, or all-`nq=0` bars are inconclusive rather
  than fabricated as a neutral flow observation. Any such member in the exact
  prior window withholds the family.
- The raw trades/closure slice root, flow projection root, prior projection
  root, and composite feature root are separately domain-bound. Renaming a
  projection cannot turn it into independent price or liquidity evidence.
- D3/D12, VPCI, and kline-volume alternatives are not inputs to this version.
  Selecting any of them requires a new producer version; they remain
  subfeatures of this one family, not extra votes.
- Verified M0 raw membership, the implemented aggTrade M1 rows, and M2 cursor
  finality are all unbound and therefore sealed false on this document. It is
  projection-only shadow evidence and has no common-envelope adapter.

#### Participation causal window and missingness

- `FROZEN` for the existing A/B path: trades belong to the exact current closed
  bar; transaction/data-through times are no later than `k.T`; Binance exchange
  observation, local receipt, and completeness times are no later than `D`;
  normal-quantity conversion and the whole candidate window are complete.
- `FROZEN` for this shadow version: exactly 8,640 complete, contiguous prior
  flow-only bars, excluding the current bar from location, MAD, and activity
  normalization.
- The legacy alternative first needs 12 contiguous bars for D3/D12 and 24
  contiguous bars for its full VPCI state. Those periods are not promoted by
  this document and remain `RESEARCH_REQUIRED` for a V2 producer.
- A gap, incomplete closure, invalid multiplier, absent normal quantity, zero
  volume denominator, insufficient history, or zero robust scale fails closed.

#### Participation absolute direction

- Positive normal aggressive buy imbalance is bullish participation pressure;
  negative normal aggressive sell imbalance is bearish participation pressure.
  The sign is not reversed to match a B2 fade or any primary side.

#### Participation strength

- `FROZEN` for the shadow version:
  `scale = 1.4826 * MAD(prior signed_share)`,
  `u = current signed_share / scale`, and
  `activity_support = min(current total_notional /
  median(prior total_notional), 1)`.
- Strength is
  `floor(1_000_000 * abs(u)/(1+abs(u)) * activity_support)`. Direction is the
  current signed-share sign only when strength is positive; a sub-micro result
  is READY neutral `0/0`. The rule is mechanically monotone but has no claimed
  probability meaning until calibrated on untouched outcomes.

#### Participation duplicate prevention

- A `flow_current`, B `flow_imbalance_current`, its robust z, D3/D12 taker
  delta, VPCI, relative volume, and CVD-style pressure cannot become separate
  votes. The producer version owns exactly one participation slice and one
  output.
- A flow-conditioned order-book statistic is not independent liquidity
  evidence. It must be replaced by the side-symmetric liquidity fields below.

#### Participation circularity

- A: **direct/high**. Entry already requires crowd-aligned `flow_current`.
- B: **direct/high**. Flow sign defines both B child tests and, with the child,
  the position side.
- C: **low under the present rule**. C entry does not use trade-flow fields,
  although shared market stress can still correlate the families.

### 3. `VOLATILITY_REGIME`

#### Volatility raw source

- `FROZEN`: the target's closed USD-M `<symbol>@kline_5m` high, low, and prior
  close, with explicit target-range ownership distinct from the close-return
  feature ID.

#### Volatility reusable sealed fields

- Family B seals `high_current`, `low_current`, and `previous_close` and derives
  the event true range used by its position/exit contract.
- The legacy engine has ATR 14 and Bollinger-width state, but those float fields
  are not authoritative V2 evidence.

#### Volatility factory gap

- The shadow V2 volatility document now seals one anchor candle plus 8,640
  prior true ranges and the current true range. Every computed true range is
  therefore anchored by a close inside the same exact contiguous slice.
- Raw signed-block membership, capture-cursor completeness, and raw-parser
  authority are still not connected. Until those certificates are required by
  the factory, this document is not an authoritative live producer.
- The current `EvidenceFamilyObservationV2` cannot carry non-directional
  intensity because a ready neutral observation must have strength zero. A
  successor schema would need a separate `regime_strength_micros` or risk-state
  field if that intensity is to be displayed.

#### Volatility causal window and missingness

- `FROZEN` for the current shadow version: one anchor plus exactly 8,640 prior
  true ranges and one current true range, all from contiguous closed 5-minute
  bars. Each bar's data-through boundary is its close; Binance kline observation
  `E` may follow that close but it and the local receipt are at or before `D`.
  Normalization is the shared prior-only 8,640-observation robust z-score.
- Alternative high-low, realized-variation, width, or ATR formulations remain
  `RESEARCH_REQUIRED` and require a new rule version.
- Invalid OHLC geometry, missing prior close, a gap, insufficient warm-up, or
  zero scale makes the regime unavailable.

#### Volatility absolute direction

- Volatility is intrinsically non-directional. High or rising volatility does
  not imply bullish or bearish price direction. Assigning its sign from the
  primary side, candle color, or a recent return would duplicate price evidence
  and is forbidden.

#### Volatility neutral, veto, and context choices

1. **Current-schema neutral — recommended now.** Emit `READY`, direction `0`,
   strength `0`; retain regime diagnostics in sealed reasons/evidence. This is
   honest but deliberately adds no numerator information.
2. **Readiness veto — prohibited.** Marking valid high volatility as
   `INCONCLUSIVE_DATA` would misuse data quality to withhold the entire score.
3. **Risk veto — research successor only.** A separately named, predeclared
   alert-risk gate could veto future promotion, but the current shadow
   annotation is contractually unable to suppress a primary alert.
4. **Non-directional context — preferred successor.** Carry capped regime
   intensity outside the directional numerator and calibrate score buckets
   conditional on regime. This requires a new sealed schema and prospective
   qualification.

#### Volatility strength candidate

- `FROZEN` for the current score: strength is zero because direction is neutral.
- `RESEARCH_REQUIRED` for a successor: a capped prior percentile or robust
  magnitude in `[0, 1_000_000]`, used only as context or a risk input, never as
  a signed vote.

#### Volatility duplicate prevention

- True range, ATR, realized variation, and Bollinger width are one volatility
  ownership group. None may be rebranded as another price vote.
- Shared kline lineage with price must be declared. The range feature may use
  high/low while the price feature owns the close-return path; the neutral
  current-schema treatment prevents the shared source from manufacturing an
  extra directional confirmation.

#### Volatility circularity

- A: **partial/source overlap**. A uses return magnitude and price references,
  but not the proposed high-low regime as a separate entry condition.
- B: **direct for position outcome/exit, not child selection**. Event true range
  sets invalidation and later exit behavior.
- C: **low to partial** for a target high-low regime; C uses close-return and
  residual scales, so a close-based volatility alternative would create direct
  overlap.

### 4. `DERIVATIVES_POSITIONING`

#### Positioning raw source

- Public USD-M HTTPS `usdm_public_rest`, `/fapi/v1/openInterest`, plus public
  USD-M WebSocket `usdm_market`, `<symbol>@markPrice@1s`, carrying mark, index,
  and predicted funding.
- Family A's route-bound factories are the current sealed authority for these
  records. The provisional promoting plan now atomically requires the two
  public WebSocket routes plus exact unauthenticated
  `GET /fapi/v1/openInterest` for the same symbol census. Actual REST capture,
  signed raw-record membership, and cursor-completeness authority remain
  integration gaps before connection.

#### Positioning reusable sealed fields

- Family A entry: `rz_doi12_previous`, `rz_doi1_current`,
  `rz_basis_previous`, and `rz_funding_previous`.
- Family A exit: `rz_basis_current`.
- The underlying prior-bar evidence also seals selected OI, log basis, predicted
  funding, source roots, and latest event/receipt times.

#### Positioning factory gap

- `RESEARCH_REQUIRED`: a primary-independent positioning evidence factory that
  exposes one composite and binds the exact OI/mark/index/funding slices without
  requiring a Family A entry decision.
- The factory must record whether its direction means descriptive crowding or
  a return forecast. Those meanings cannot be mixed under one version.

#### Positioning causal window and missingness

- `FROZEN` for reuse of Family A entry evidence: 8,653 prior bars, incorporating
  the exact 8,640-observation robust-z window and 12-bar transforms.
- `FROZEN`: OI staleness tolerance is 10,000 ms and mark/index/funding
  staleness tolerance is 2,000 ms in the Family A feature contract; selected
  source events and all receipts remain bounded by `k.T` and `D`.
- Missing or ambiguous candidate selection, stale OI/mark, non-positive mark or
  index, a gap, incomplete history, or zero robust scale fails closed.

#### Positioning absolute direction

- This family is directionally ambiguous. Positive basis/funding and rising OI
  can describe long crowding, but the return hypothesis may be continuation or
  contrarian unwind. Descriptive crowding sign must not be advertised as a
  bullish-return vote.
- `RESEARCH_REQUIRED`: freeze one forecast interpretation and composite on
  development data before emitting a nonzero direction. Until then the safe
  observation is neutral/non-promoting, never a sign chosen to agree with the
  primary signal.

#### Positioning strength candidate

- Candidate: one bounded composite of OI change, basis, and predicted funding,
  each normalized only from causal prior observations, capped once at
  `1_000_000`. Counting all three as votes or taking whichever has the desired
  sign is forbidden. Formula and forecast mapping are `RESEARCH_REQUIRED`.

#### Positioning duplicate prevention

- Current/12-bar OI change, basis, mark-index spread, and predicted funding are
  one positioning ownership group. Their subfeature IDs appear together under
  one family observation and nowhere else.

#### Positioning circularity

- A: **direct/high**. OI, basis, and funding are core entry conditions and
  basis is also an exit condition.
- B: **low under the present rule**. B has no derivatives-positioning entry
  field.
- C: **low under the present rule**. C has no OI, basis, or funding entry field.

### 5. `LIQUIDITY_EXECUTION`

#### Liquidity raw source

- `FROZEN`: public USD-M standard diff-depth route `usdm_public`, stream
  `<symbol>@depth@100ms`, with sequence-valid reconstructed bid and ask levels.
  RPI is not accepted by the current Family B feature builder.
- The additive v8 planning authority also binds public USD-M
  `GET /fapi/v1/depth` through `usdm_public_depth_rest` for the identical symbol
  census, with exact `symbol,limit`, fixed `limit=1000`, public no-key access,
  and bounded startup/reconnect/sequence-gap bridge attempts.
- Periodic re-anchoring remains explicitly unset and non-promoting until an
  infrastructure qualification selects it from coverage, request weight, and
  storage evidence without using PnL. This plan slice is not a runtime, M2
  certificate, liquidity producer, or PAPER/BBO fill claim.
- `build_provisional_promoting_capture_plans_v8` retains the additive lineage
  name, but its fourth `ProvisionalDepthRestQualificationPlanV8` role is sealed
  with `promoting=False` and `promotion_ready=False`; the three-role v7 APIs
  and golden hashes remain independent and unchanged.
- The v8 depth role now has a disconnected one-shot retained-attempt boundary:
  exact public/no-key `symbol+limit=1000` payloads cross the existing shared
  receipt-order reservation authority, admission gate, global ingest sequencer,
  and bounded queue-admission receipt.
  Successful, HTTP-failed, and admission-cancelled attempts are all retained;
  only an uncancelled, error-free HTTP 200 body matching exact
  `{lastUpdateId,E,T,bids,asks}` semantics receives a qualification result.
- This boundary still has no live HTTP adapter or trigger scheduler. Its result
  explicitly leaves WAL durability, finality, freshness, coverage, M2, book
  bridge, liquidity signal, promotion, PAPER fill, and order execution
  unverified or false.

#### Liquidity reusable sealed fields

- `FamilyBBookStateV2` seals standard diff-depth bid/ask levels, prices,
  quantities, contract multipliers, event/receipt times, sequence IDs, and raw
  evidence hashes.
- `FamilyBFeatureEvidenceV2` seals `d_start`, `d_low`, `d_end`, and
  `spread95_bps`. The `d_*` values are opposing depth chosen after observing
  flow sign, so they are not an absolute, independent liquidity direction.

#### Liquidity factory gap

- A side-symmetric factory must expose bid depth and ask depth within the same
  fixed price band, their bounded imbalance, spread, band completeness, and
  duration-weighted current-bar summaries before any primary or flow sign is
  supplied.
- It also needs a canonical standard-depth slice root and explicit sequence/
  snapshot recovery evidence.

#### Liquidity causal window and missingness

- `FROZEN` from Family B: right-continuous states cover the exact current
  closed bar; first/last subwindows and full-bar summaries use only events at or
  before `k.T`, received by `D`; the 10-basis-point band is complete; update
  sequence is valid.
- A current-bar symmetric imbalance needs no historical lookback. Any historical
  normalization lookback is `RESEARCH_REQUIRED`; an exact 8,640-prior-bar robust
  window is the consistent V2 candidate, not an approval.
- Missing depth, an unrecovered sequence gap, crossed/invalid geometry,
  incomplete 10-basis-point band, zero reference depth, or late state fails
  closed.

#### Liquidity absolute direction

- Direction is the sign of side-symmetric executable support, such as bid-depth
  dominance versus ask-depth dominance, computed before the primary side or
  flow sign is known. It is not "depth opposing the attempted trade."
- The predictive mapping remains `RESEARCH_REQUIRED` because displayed depth
  can cancel and raw imbalance is not automatically a return forecast.

#### Liquidity strength candidate

- Candidate: absolute symmetric depth imbalance, reduced monotonically for
  wider spread or incomplete time coverage, capped once at `1_000_000`.
  Exact aggregation and cap curves are `RESEARCH_REQUIRED`.

#### Liquidity duplicate prevention

- Bid/ask imbalance, depth depletion/recovery, spread, and book pressure are
  one liquidity ownership group.
- B's flow-conditioned `d_*` values cannot be paired with the participation
  vote as independent confirmation. The new symmetric fields are required to
  remove that mechanical dependence.

#### Liquidity circularity

- A: **low for entry logic**, although the same book can affect PAPER
  executability and therefore realized outcomes.
- B: **direct/high**. B1/B2 already use opposing-depth depletion/recovery and
  `spread95_bps`.
- C: **low for entry logic**, again with possible shared PAPER execution data.

### 6. `CROSS_SECTIONAL_CONTEXT`

#### Cross-sectional raw source

- `FROZEN`: Family C's prior-only daily USD-M universe and member-complete,
  contiguous panel of closed `<member>@kline_5m` candles.
- For a target-specific evidence score, the owned economic slice must exclude
  the target symbol when forming market direction and breadth. This prevents
  the target price path from voting in both price and context.

#### Cross-sectional reusable sealed fields

- Family C seals `m3_current`, `shock_scale`, `shock_score`, `breadth_count`,
  the member set, current closes, each member's three-bar return, beta,
  residual scale, `g0`, and `lag_score`.
- These current fields include the target when it belongs to the universe and
  directly define primary C entries; they are therefore useful sealed inputs
  but are not yet a target-excluded, primary-independent producer.

#### Cross-sectional factory gap

- The shadow target-excluded factory now seals `m3_ex_target`, shock scale/score,
  sign-consistent breadth count and denominator, target-excluded member root,
  and the exact ex-target slice for each target. It preserves the parent
  prior-only universe/panel roots for audit while excluding the target before
  every market scalar and freshness maximum.
- A sibling pre-outcome directional candidate now accepts only the canonical
  target-excluded context object. It binds that object's evidence hash,
  target-excluded member/slice roots, source and decision clocks, source and
  candidate reasons, and both rule versions. It does not accept target returns,
  a primary-rule direction, or an outcome.
- Raw signed-block membership and prior-only universe-selection certificates
  are still not required by the inherited Family C constructors. That is an
  integration blocker, not evidence of live source authority.

#### Cross-sectional causal window and missingness

- `FROZEN` for existing Family C evidence: at least 20 prior-selected members
  before target exclusion; exactly 8,644 contiguous candles per member through
  the current bar; exactly 8,640 prior observations for scale and beta; current
  three-bar return uses four closes. Every candle event is at or before its
  close and every receipt entering the panel is at or before `D`.
- `FROZEN` before outcome inspection as a structural-computability floor for
  this shadow version: the original prior-selected panel has at least 20
  members and the deterministic ex-target panel has at least 19. Thus a
  19-member original panel with an absent target is rejected, while a valid
  20-member panel may remain computable after removing its target.
- The 19-member floor is not a statistical-sufficiency, calibration, or
  promotion claim. Coverage and after-cost efficacy at each post-exclusion
  census remain `RESEARCH_REQUIRED` on frozen qualification/prospective data.
- The universe eligibility cutoff is strictly before the effective UTC day.
  Removing a member because its current data is absent is forbidden.
- A missing member/slot, duplicate slot, late candle, membership change, an
  original census below 20, a target-excluded census below 19, or zero
  market/residual scale fails closed.

#### Cross-sectional absolute direction

- Direction is the sign of target-excluded cross-sectional market movement,
  independent of the target's primary side. Breadth confirms the same market
  sign inside the one context composite; it is not a second vote.
- The frozen candidate preserves that sign independently of final strength
  quantization. A sub-quantum nonzero move may therefore emit direction `-1`
  or `+1` with strength `0`; direction `0` means `m3_ex_target == 0`. A
  non-ready source emits explicit `NOT_READY` with `0/0`; that status is
  withholding, not a neutral numeric observation.

#### Cross-sectional strength candidate

- `FROZEN` before outcome inspection under Decimal34 arithmetic:
  `shock_magnitude = shock_score / (1 + shock_score)` and
  `breadth_support = breadth_count / breadth_denominator`.
- The one family magnitude is
  `floor(1_000_000 * shock_magnitude * breadth_support)`. Robust shock and
  sign-consistent breadth therefore enter exactly once in one side-symmetric
  composite. Decimal34 rounding can make the bounded shock magnitude exactly
  one for a sufficiently large finite score, so the inclusive strength cap is
  `1_000_000`.
- This object remains M0/M1/M2-unbound, `producer_ready=false`,
  `promoting=false`, and `probability=false`. The mapping is a frozen
  pre-outcome research candidate, not evidence of efficacy, calibration, or
  after-cost profitability.
- `EvidenceFamilyObservationV2` currently couples zero strength and zero
  direction. This candidate must not be coerced across that boundary. A future
  producer adapter must freeze the sub-quantum policy under a new rule version
  before converting this candidate into an eligible family observation.

#### Cross-sectional duplicate prevention

- Market median return, shock score, breadth, beta/residual context, and member
  ranks form one cross-sectional ownership group and produce one vote.
- Excluding the target prevents its close return from also entering the price
  family. All remaining member candles still share one cross-sectional slice;
  member count does not multiply votes.

#### Cross-sectional circularity

- A: **low after target exclusion**. A has no market-panel entry condition.
- B: **low after target exclusion**. B has no cross-sectional entry condition.
- C: **direct/high even after target exclusion**. Primary C already requires
  `m3_current`, shock, breadth, beta/residual lag, and selects direction from
  market sign. Target exclusion reduces self-inclusion but does not turn the
  evidence into independent confirmation.

## RSI, MACD, and EMA are one price-path vote

<!-- markdownlint-disable MD013 -->

| Transform | Economic dependency | Permitted producer role | Duplicate-vote rule | Status |
| --- | --- | --- | --- | --- |
| RSI | Ordered target close returns; Wilder state in the legacy engine | One bounded subfeature of the price composite; level alone is not a forecast | RSI level, slope, overbought/oversold, and divergence cannot emit separate observations | Formula exists in legacy float code; V2 Decimal mapping is `RESEARCH_REQUIRED` |
| MACD | Fast/slow EMAs and signal EMA of the same target closes | One trend/momentum subfeature inside the same price composite | MACD line, signal, histogram, cross, and slope are one dependency group | Legacy 12/26/9 exists; V2 initialization and mapping are `RESEARCH_REQUIRED` |
| EMA | Recursive averages of the same target closes | One structure/trend subfeature inside the same price composite | EMA 9/20/50/200 levels, slopes, ordering, and crosses cannot each vote | Legacy periods exist; V2 periods and composite rule are `RESEARCH_REQUIRED` |

<!-- markdownlint-enable MD013 -->

The producer emits one `PRICE_STRUCTURE_MOMENTUM` direction and one capped
strength even when all three transforms agree. Agreement among them may affect
the internal composite only under one frozen rule; it never increases the
family count.

## Primary-strategy circularity ledger

This table summarizes whether an evidence family adds independent context or
mostly restates a primary rule.

<!-- markdownlint-disable MD013 -->

| Evidence family | Primary A | Primary B | Primary C |
| --- | --- | --- | --- |
| `PRICE_STRUCTURE_MOMENTUM` | Direct entry reuse | Direct entry reuse | Direct target-return reuse |
| `PARTICIPATION_FLOW` | Direct entry reuse | Direct entry and side reuse | No current rule field |
| `VOLATILITY_REGIME` | Shared price source; no proposed range entry rule | Direct invalidation/exit reuse | Partial if close-based; lower if high-low only |
| `DERIVATIVES_POSITIONING` | Direct entry and exit reuse | No current rule field | No current rule field |
| `LIQUIDITY_EXECUTION` | No entry rule; shared execution | Direct entry reuse | No entry rule; shared execution |
| `CROSS_SECTIONAL_CONTEXT` | No current rule after target exclusion | No current rule after target exclusion | Direct entry and direction reuse |

<!-- markdownlint-enable MD013 -->

Therefore a high shadow score is not automatically "six independent
confirmations." For A, B, and C it may partly explain why the primary rule fired.
That is acceptable for a non-promoting explanation, but a future promotion must
either:

- use a family-specific successor score that excludes primary-owned feature
  groups;
- replace them with orthogonal, predeclared inputs; or
- call the result a composite of correlated entry evidence and prove
  incremental after-cost value prospectively, rather than claiming independent
  confirmation.

Changing the current exact-six denominator or excluding a primary-owned family
requires a new rule version; it cannot be done silently per alert.

## Promotion and calibration gate

The following gate must complete in order. Failure at any step leaves the
Evidence Score as a non-promoting, non-probability annotation.

1. Freeze one factory-sealed producer version for all six families, including
   source routes, exact feature slices, lookbacks, Decimal formulas, freshness,
   missingness, direction semantics, strength normalization, and dependency
   ownership.
2. Add positive, negative, boundary, gap, stale-event, late-receipt, zero-scale,
   alias-duplication, and deterministic-replay tests. Verify that producers
   cannot receive a primary side or outcome.
3. Run a data-only qualification interval to assess coverage, numerical
   stability, cap saturation, disagreement, and dependency overlap. Do not
   inspect the reserved efficacy outcome while revising producers.
4. Freeze family-specific A/B/C horizons, exits, PAPER execution, fees,
   funding, and an after-cost outcome. Preserve the existing isolated strategy
   portfolios.
5. On development data, test whether the score has incremental value conditional
   on each primary rule and circularity class. Predeclare score/regime bins and
   a monotone calibration mapping; record count, base rate, Brier score,
   calibration error, uncertainty, expectancy, drawdown, and tail loss.
6. Seal code, producer versions, ownership map, panels, cost cells, bins, and
   calibration hash. Evaluate once on a strictly later untouched prospective
   interval without refitting or merging bins.
7. Permit stronger alert language only if predeclared coverage, calibration,
   positive after-cost expectancy, and portfolio-risk gates all pass. A high hit
   rate alone is insufficient. Production order execution remains out of scope.

## Implementation order

1. **Implemented, shadow:** shared producer envelope, six-family atomic
   dependency-ownership ledger, factory-only observations, and the unchanged
   score/alert boundary.
2. **Implemented, calculation-only shadow:**
   `PRICE_STRUCTURE_MOMENTUM` as one collapsed close-path composite and
   `PARTICIPATION_FLOW` as one canonical normal-flow source. Their M0/M1/M2
   authority bindings and family-to-envelope adapters are absent.
3. **Missing producer and adapter:** implement side-symmetric
   `LIQUIDITY_EXECUTION`; do not reuse B's flow-conditioned opposing-depth
   fields as independent evidence.
4. **Implemented, calculation-only shadow:** target-excluded
   `CROSS_SECTIONAL_CONTEXT` from the existing prior-only Family C panel; its
   raw panel/universe authority and family-to-envelope adapter remain absent.
5. **Missing producer and adapter:** close open-interest
   capture/parser/cursor authority, then research and freeze the forecast
   semantics for `DERIVATIVES_POSITIONING`.
6. **Implemented, calculation-only shadow:** `VOLATILITY_REGIME` is current-
   schema neutral, but its authority binding and family-to-envelope adapter are
   absent. Design a separate non-directional context field or future risk veto
   only in a successor rule.
7. Implement all six family-to-envelope authority adapters, connect the six
   producers to the existing shadow aggregator, qualify replay and data
   coverage, and only then expose the annotation in Discord. Keep it non-
   promoting until the full prospective promotion gate passes.
