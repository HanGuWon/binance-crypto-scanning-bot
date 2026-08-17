# Signal Specification

## Action and timing semantics

Spot long maps to `SPOT_BUY`; Spot short-direction analysis maps to
`SPOT_EXIT`. Futures long and short map to `FUTURES_LONG` and
`FUTURES_SHORT`. PAPER lifecycle exits map to `SPOT_EXIT`,
`FUTURES_LONG_EXIT`, or `FUTURES_SHORT_EXIT` according to the position being
closed. A Spot exit is not a Futures short. `PUMP_RISK` and
`CRASH_RISK` are intrabar warnings, not entries.

Candle-family decisions use only fully closed candles. The example
configuration uses the closed 5-minute candle as its decision clock. A 15-minute
or 1-hour context is usable only after that candle has closed and only when its
feature timestamp is strictly earlier than the 5-minute decision timestamp.

The state model is `IDLE -> WATCH -> SETUP -> CONFIRMED`; an active
pre-confirmation setup falling below threshold emits `INVALIDATED`. Scores are
rule-strength values, not calibrated probabilities. With
`confirmation_mode: explicit_trigger`, a score cannot promote a setup into a
confirmed entry: the rule's explicit trigger and every required gate must pass.

## Frozen live R2 candidate

`entry_policy: r2_pit_htf_exec` implements the prospective
`R2_PIT_HTF_EXEC` alert candidate. It is a non-compensating conjunction; a
strong value in one component cannot offset a failed component.

1. The raw C0 trigger must complete on the closed primary candle.
   - Spot long: price closes above the prior `breakout_lookback` high after the
     previous close was not above it; the MACD histogram is positive and
     improving, ADX is at least 20, and EMA20 is above EMA50.
   - Futures short: price closes below the prior `breakout_lookback` low after
     the previous close was not below it; the MACD histogram is negative and
     weakening, ADX is at least 20, and EMA20 is below EMA50.
2. Both mature, strictly-prior 15-minute and 1-hour contexts must align.
   - Long: `close > EMA20 > EMA50` on both contexts.
   - Short: `close < EMA20 < EMA50` on both contexts.
3. Decision-time execution evidence must be observed rather than proxied.
   - BBO receipt age must be non-negative and no greater than
     `book_maximum_age_ms` (2 seconds in the example).
   - Spread must be no greater than `maximum_spread_bps` (15 bps in the
     example).
   - Ask price times ask quantity for a long, or bid price times bid quantity
     for a short, must cover `execution_notional_usdt` (100 USDT in the
     example).
4. Data completeness must meet `completeness_gate` (95 in the example).

Only Spot `BREAKOUT_LONG` and Futures `BREAKDOWN_SHORT` may qualify under this
policy. Participation, funding/crowding, volume-feature, breadth/regime,
squeeze, and RSI-reversal conditions are not R2 candidate gates. They must not
be silently reintroduced without a new, pre-registered experiment and rule
version. The example also disables reversal families explicitly.

The example's `PULLBACK_LONG` and `PULLBACK_SHORT` families are a separate
informational path. They deliberately fail the eligibility gate, retain their
raw rule score for `WATCH`/`SETUP` explanation, and can never reach
`CONFIRMED`. The non-promotion lock is applied even when ordinary entry gates
are disabled, and is repeated in the state machine and decision model. They do
not expand the frozen R2 candidate set.

Every emitted decision retains its evidence, failed or passed gate diagnostics,
informational ATR/structure invalidation, rule version, and deterministic event
ID. It remains an alert and never places an exchange order.

## Alert-only PAPER technical exits

When `signals.technical_exit.enabled` is true, only a newly persisted
`CONFIRMED` technical entry with a directionally valid stop can schedule a
bounded, in-memory PAPER position. Spot schedules long positions only; a Spot
short-direction confirmation remains `SPOT_EXIT` evidence and can confirm an
opposite-signal exit. Futures may track either direction. Risk warnings and
`TECHNICAL_EXIT` decisions can never schedule another position.

The entry is modeled at the next primary-candle open. A stop known before a bar
fills at that bar's open after a gap or at the stop on a within-bar touch. A
trailing stop is updated only after the current candle closes and cannot act on
that same candle. Opposite confirmation, three consecutive trend failures, and
the 72-closed-bar limit schedule a PAPER exit at the next bar's open. Alerts are
created only while handling a fully closed primary candle and record both the
modeled fill time and the closed-candle observation time.

Regime context is timestamped separately from the modeled fill. An open fill
uses the strict-prior primary close; an intrabar stop uses the current closed
primary candle at alert observation; and a gap exit uses the last available
pre-gap primary close. PAPER state transitions that emit an exit are retained
until the immutable signal/outbox intent is durable; notification-handler
failure after persistence does not roll the transition back.

Primary-candle gaps cancel pending entries and fail-close an open PAPER
position at the first post-gap open. Universe rotation prunes its symbol state.
The lifecycle is deliberately not persisted or reconstructed: a process
restart forgets pending entries and open PAPER positions. Every exit is marked
`paper_only`, says that no order was placed, and uses a deterministic event ID;
the scanner still contains no order-placement or private-account API path.

## Other rule families

The legacy rule engine can still evaluate the following families when a
non-R2 policy enables them:

- Squeeze: low Bollinger-width percentile near a prior range boundary.
- Breakout/breakdown: a closed range escape with directional momentum and trend
  confirmation.
- Pullback: prominence-qualified 2-left/2-right pivots are usable only after
  their right-window confirmation and are frozen again at `t-1`. A bullish
  candidate requires HH/HL structure, a prior 2-ATR impulse, a 20–60%
  retracement lasting at most 12 bars, intact structure, proximity to the
  EMA20 frozen immediately before the observed pullback extremum, and a close
  above the prior candle high. Impulse size is normalized with ATR frozen when
  its ending pivot becomes available. Short is the exact mirror. These
  thresholds are unvalidated research seeds and the family is
  informational-only. Public feature/structure entry points reject any open
  candle in the prefix used for a decision.
- Exhaustion: high RSI only activates the setup; divergence, rejection wick,
  weakening MACD, EMA9 loss, sell flow, and volume provide corroboration.
  Strong bullish context penalizes shorts.
- Capitulation: the inverse recovery setup after low RSI.
- Pump/crash risk: absolute short-window return plus robust z-score from
  all-market mini-ticker updates; always tagged intrabar.

These descriptions do not expand the frozen R2 candidate set.

## Frozen R2 retrospective screen

The 5-minute retrospective protocol deliberately tests less than the live R2
candidate:

- `C0_CORRECTED` preserves every explicit raw C0 price trigger.
- `H1_STRICT_PRIOR_HTF` applies only the strict-prior 15-minute and 1-hour
  Boolean filter to the same C0 opportunity IDs.
- `F60` enters at the next 5-minute open and exits at the 12th subsequent
  5-minute close, with fees, adverse slippage, and applicable Futures funding
  recorded separately and in net return.
- `T72` uses the frozen technical lifecycle: structural initial stop, trailing
  activation after 1R with a 2-ATR trail, three consecutive trend-failure bars,
  an eligible opposite trigger, or at most 72 closed bars.
- The primary panel requires a continuous next-open-through-T72 path wholly
  inside one declared split. H1 rejections remain in the common panel with zero
  policy contribution rather than disappearing from the denominator.

The historical files contain klines and public funding, but no decision-time
BBO, top-of-book quantity/depth, or receipt timestamp. A historical spread
proxy cannot satisfy the live execution-evidence claim. Therefore
`full_r2_status` is always `INCONCLUSIVE_NO_HISTORICAL_BBO`, regardless of the
C0/H1 result. `RETROSPECTIVE_SCREEN_PASS`, if reached, means only that the
pre-registered historical diagnostic passed; it is not live-deployment approval
and not evidence that a 100 USDT order was executable at the modeled price.
