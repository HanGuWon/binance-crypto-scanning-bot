# Guardian policy selection preregistration

guardian-policy-selection-v1 freezes the L60 policy-comparison decision rule
before any policy outcome computation. The machine-readable authority is
config/guardian-policy-selection.v1.json.

Contract identity is SHA-256 over the ASCII domain separator
GUARDIAN_POLICY_SELECTION_CONTRACT_V1 followed by one NUL byte and then the
UTF-8 bytes of JSON serialized with sorted keys, ensure_ascii=true, and compact
separators comma/colon. The frozen semantic SHA-256 is
2e19fd7a597dd78a0372753da50fd54dcfacceea6e9482bb34aac606c507a923.
Two post-normalization independent reviews reproduced this identity and
recorded PASS before any policy outcome computation.

Every repository text-source SHA bound inside the contract is LF-canonical:
replace each CRLF byte pair with LF, perform no other transformation, and then
compute SHA-256. This keeps source identity invariant across Windows
`core.autocrlf=true` and Linux checkouts.

This preregistration compares four stop-management policies on the same Futures
positions, with identical entry time, quantity, market path, and cost rows. The
current delayed ATR trail is the reference baseline. The three challengers are
an admission-stop-only policy, a confirmed-swing plus ATR trail, and the
weakening-sensitive policy shape introduced by L60-01.

The historical comparison cohort is also frozen here. L60-03 must load
config/backtest.5m.r2-c0-corrected.yaml at SHA-256
2dce99a243c4f94c446cf48a0edcb103093db9cecf0e6da261c35b45fa235c7f
and derive the comparison spec by changing exactly one field:
direction_scope=futures_bidirectional. Every other BacktestSpec field must
remain byte-semantically equivalent to the loaded base spec. L60-03 then uses
every executable USDⓈ-M Futures entry produced across that spec's development,
validation, and retrospective_test windows before substituting the four exit
policies. It may not filter entries by realized return, MFE, profit, later
price path, or candidate-policy outcome.
The frozen symbol-to-cost cohorts are BTC/ETH = anchor, BNB/SOL/XRP/DOGE =
major, and SUI/WIF = volatile.

Those baseline-generated entry rows are frozen once. Every policy is replayed
on every row as an independent exit-policy episode; a candidate's longer or
shorter holding period cannot suppress or create later cohort entries. The
result is exit-policy evidence, not a capital-capacity portfolio claim.

For the later external L60-07 shadow campaign, admission is likewise decided
before policy outcomes: every explicitly approved adoption generation that
first reaches MANAGED_SHADOW after campaign activation is included. Current
profit, MFE, age in position, subsequent outcome, symbol performance, and
policy availability are not cohort filters. Outcome-based early stopping is
forbidden. Its trend-state stratum is the latest compatible
protection-context-v1 row already persisted before the MANAGED_SHADOW
transition for the same Futures symbol and 5m primary interval, with candle
close strictly before the transition. Both transition age and
context_freshness_ms must be at most 600,000 ms. A close-time tie must resolve
to the same context_id or fail closed. If no qualifying context exists, the
position stays in the cohort as stratum_unavailable and cannot satisfy the
trend-state coverage floor. The campaign admits positions for exactly 60 UTC
days: campaign_end_ms = activation_ms + 60 * 86,400,000. It never stops early
or extends because of outcomes, sample counts, performance, or guardrail
values. After campaign_end_ms no new position is admitted. Already-admitted
episodes receive only the fixed 21,600,000 ms tail required by the 72 x 5m
maximum holding horizon; unresolved episodes after that tail are censored.

No policy is selected by this document. If no challenger clears every hard
guardrail and demonstrates a strictly positive paired after-cost improvement
over the delayed ATR baseline, L60-08 must close as NO_POLICY_PROMOTION.

## Frozen primary decision

The primary metric is the equal-weight per-position paired mean after-cost
return difference, in basis points, challenger minus delayed_atr_trail_v1.
Uncertainty uses one shared circular seven-UTC-day calendar-block bootstrap
schedule keyed by entry UTC day: 10,000 replicates, seed 20260921, and at least
8,000 globally valid replicates. The sampled calendar runs continuously from
the first through last admitted cohort entry day before policy-specific
censoring and retains zero-entry days. Historical and external populations
build separate calendars, but every challenger within one population uses the
identical common calendar. Replicate
ordinal r is zero-based from 0 through samples-1 and block ordinal b is
zero-based from 0. Each block start hashes the exact ASCII bytes
GUARDIAN_POLICY_BOOTSTRAP_V1|<seed>|<r>|<b> with no trailing newline. seed, r,
and b are unsigned base-10 ASCII integers with no leading plus sign and no
leading zeros except the single digit 0. The first eight SHA-256 digest bytes
are interpreted as an unsigned big-endian integer, reduced modulo the
calendar-day count, then expanded as a circular seven-day block. Blocks are
appended until the original calendar length is reached and the final block is
truncated exactly to that length.

All three challengers use the same draw. For each replicate, each challenger's
paired mean delta is centered on its point estimate and the maximum centered
error across challengers is retained. The 95% familywise critical value is the
nearest-rank q95 of those maximum errors. A challenger's simultaneous lower
bound is point estimate minus that critical value. A replicate is globally
invalid if any challenger has no finite paired primary row. Promotion requires
both a strictly positive point estimate and a strictly positive simultaneous
lower bound.

If more than one challenger survives, selection uses this fixed order:

1. higher paired mean after-cost return delta;
2. lower maximum drawdown;
3. less-negative 5% CVaR;
4. lower premature-stop rate;
5. lower mean MFE giveback;
6. lower mean stop-update frequency;
7. lexicographically smaller policy ID.

There is no post-outcome tolerance band and no threshold relaxation.

The external shadow population is the adjudication population. It must satisfy
its evidence floor and every hard guardrail, including a strictly positive
primary point estimate and familywise simultaneous lower bound. The historical
harness is a mandatory veto population: it must satisfy its evidence floor and
the drawdown, CVaR, premature-stop, MFE-giveback, stop-update-frequency,
censoring, funding, and cost guardrails. Its primary point estimate and
bootstrap lower bound are reported but are not promotion gates. Tie-breaks are
computed only on the external shadow population. For every challenger versus
baseline comparison, after-cost return, drawdown, CVaR, premature-stop rate,
MFE giveback, and stop-update-frequency values for both policies are computed
on the exact same paired-valid position intersection. Absolute challenger caps
and candidate-minus-baseline deterioration limits use that same intersection.
Only censor fraction uses all admitted cohort positions as its denominator.

## Frozen hard guardrails

Every challenger must satisfy all of the following on the same evaluation
population:

- maximum drawdown may worsen by at most 2.0 percentage points versus the
  delayed ATR baseline;
- 5% CVaR may worsen by at most 25.0 bps versus the baseline;
- premature-stop rate must be at most 15% and may worsen by at most 5
  percentage points versus the baseline;
- mean MFE giveback must be at most 0.60 and may worsen by at most 0.05 versus
  the baseline;
- mean stop updates must be at most 24 per 24 hours of position exposure;
- primary-event censoring must be at most 10%;
- realized USDⓈ-M funding must be included for each policy's actual holding
  interval;
- each stop update incurs a modeled 0.10 bp operational cost in addition to
  the frozen Futures fee/slippage model in the contract.

For each challenger-baseline pair, the historical harness must contain at least
300 paired-valid positions overall, 100 per direction, four symbols per
direction, 90 qualifying UTC entry dates, and all three ProtectionContext trend
states: bullish, mixed, and bearish. The external shadow campaign must contain
at least 30 paired-valid positions overall, 10 per direction, two symbols per
direction, 30 qualifying UTC entry dates, and the same three trend states. Both
population floors must pass. Trend-state authority is
protection-context-v1 in src/signalbot/signals/protection_context.py at SHA-256
9d514265f28e03fdac06288ea16aa8029f36f37d4a4d6698a3521840987522a8.
Every evidence floor is evaluated separately for each challenger versus
baseline on that comparison's paired-valid primary-position intersection.
Direction, symbol, trend-state, and calendar-day coverage all use those rows;
calendar days are distinct UTC entry dates containing at least one paired-valid
position. Censor fraction remains the separate all-admitted denominator.

After-cost return is directional entry-to-exit return minus the frozen entry
and exit fee/slippage rows, plus realized signed USDⓈ-M funding over the actual
holding interval, minus 0.10 bp for every stop update. Missing mandatory
funding makes that position primary-censored. A paired primary row exists only
when both the challenger and delayed ATR baseline have valid outcomes for the
same cohort position.

Funding arithmetic is dimensionally explicit. Each strict-interior settlement
first produces a decimal return
-side_sign * rate * funding_mark / entry_price. The sum is multiplied by
10,000 exactly once to produce realized_signed_funding_bps before it is added
to after-cost return in bps.

The execution fee/slippage values are copied into this contract from
config/backtest.5m.r2-c0-corrected.yaml costs at freeze time: 5 bp Futures fee
per side and adverse slippage of 3 bp for anchor/major symbols or 8 bp for
volatile symbols per side. The 0.10 bp stop-update term is a preregistered
operational burden proxy, not an exchange fee.

The 5% CVaR is the arithmetic mean of the worst ceil(5% * N) per-position
after-cost returns. MFE is measured separately for each policy from the common
entry through that policy's own exit without guessing OHLC order. Full high/low
extremes are used only for completed bars strictly before the exit bar. A
gap-through exit contributes only its open. An intrabar stop exit uses the
exit-bar open and stop execution price but excludes that bar's favorable
high/low because its ordering relative to the stop is unknowable. Where MFE is
positive, giveback is 1 - clamp(realized directional return, 0, MFE) / MFE.
Premature-stop rate is premature events divided by all valid paired primary
positions for that challenger. Candidate and baseline exits are ordered by
(bar_index, phase), with OPEN < INTRABAR < CLOSE; two INTRABAR exits in the
same OHLC bar are unordered. The 12-bar rebound window uses only complete
contiguous bars with bar_index strictly greater than the candidate exit bar and
strictly less than the baseline exit bar. It never uses the remainder of the
candidate exit bar or the baseline exit bar, even when the baseline exits at
CLOSE. Stop-update frequency counts only post-admission amendments that become
effective; the cohort-entry active stop and duplicate/idempotent intents do not
count. Frequency is total counted updates divided by total exposure days;
nonpositive exposure is primary-censored.

Maximum drawdown uses the repository's equal-weight sleeve convention but is
stored as a positive loss magnitude so smaller is better. Each symbol sleeve
starts at 1.0 and completed position returns are applied multiplicatively in
deterministic exit-time and position-ID order using
1 + after_cost_return_bps / 10,000. Portfolio equity is the arithmetic mean of
all symbol sleeves after each exit. At each step drawdown magnitude is
max(0, 1 - portfolio / previous_peak); maximum drawdown is the largest such
magnitude. Therefore the frozen tie-break lower_maximum_drawdown is
directionally correct.

All metric arithmetic, means, bootstrap centering, guardrail deltas, strict
positive tests, and tie comparisons use Decimal with precision 34 and
ROUND_HALF_EVEN. Source numerics enter as Decimal(str(value)); there is no
intermediate quantization. Display rounding is never used for pass/fail or tie
detection.

## Frozen execution semantics

A policy decision formed from fully closed candle t can affect execution only
from the next candle open. A stop computed at t cannot be applied retroactively
inside t. If the previously active stop was touched during t, that prior stop
exit wins. Unresolved same-bar ambiguity is not permitted in a primary row.

For an already-active stop, a bar that opens through the stop fills at the bar
open, preserving adverse gap-through slippage. Otherwise a bar whose range
crosses the stop fills at the stop price. This valid price-gap case requires
consecutive 5m slots. If one or more expected 5m slots are absent, no fill is
inferred across the missing-data interval and the affected outcome is
primary-censored. The shared maximum holding period is 72 bars.

No profit target closes an L60 policy episode. For the roadmap's same-bar
collision diagnostic, a one-R target is still derived from the common entry and
cohort-entry active stop. If that target and an already-active stop are both
touched in one OHLC bar, the ambiguity count increments and the row is never
called a target win; the target does not alter policy PnL. If an active stop is
touched in the same bar that reaches the shared max-holding terminal, the stop
exit wins over the close-based max-holding exit.

Funding follows the repository's strict-interior convention. Only events with
entry_time_ms < funding_time_ms < exit_time_ms are included; exact equality
with entry or exit is primary-censored. LONG has side sign +1 and SHORT -1,
with funding return per event equal to -side_sign * rate * funding_mark /
entry_price, where funding_mark is the positive mark price when present and
entry_price otherwise.

Historical funding authority is the existing per-symbol FundingDataset resolved
by signalbot.backtest.runner.funding_path. L60-03 must verify each file with
verify_funding_dataset for the exact symbol and frozen data_start through
evaluation_end - 1, and bind every file SHA-256 in its run manifest. The
authority implementation is src/signalbot/backtest/funding.py at SHA-256
c2f13af537eb663c7de3cab7ed595518463000a365fd51ac113e86d910394c19.
For L60-07, anonymous public /fapi/v1/fundingRate history is paginated with
startTime/endTime and limit 1000 from the earliest admitted entry through
campaign_end_ms + 21,600,000; each next cursor is newest fundingTime + 1 and
the capture ends only after the requested range is exhausted or a successful
page has fewer than 1000 rows. Exact query/response hashes are retained.
Missing, malformed, non-unique, range-mismatched, or incomplete funding
evidence censors the affected outcome. A zero funding return is valid only when
complete verified coverage contains no strict-interior funding event.

A premature stop is defined before outcomes are read: the candidate must exit
strictly earlier than the delayed ATR baseline under the frozen bar/phase
ordering. Starting with the first contiguous bar after the candidate exit bar,
the favorable excursion must reach at least one candidate-exit-time ATR from
the candidate exit price within the next 12 full bars and strictly before the
baseline exit. Same-bar INTRABAR candidate/baseline exits are unordered and do
not count as premature.

## Frozen policy set

delayed_atr_trail_v1 is the baseline and keeps the existing L60-01 numeric
defaults: 1R activation when original risk is known, 1 ATR activation when it
is not, a 2 ATR trail, 5 bp minimum price gap, and 1 bp minimum improvement. It
does not consume confirmed structure or momentum weakening.

initial_stop_only_v1 holds the stop observed when the position enters the
comparison cohort and never updates it.

confirmed_swing_atr_trail_v1 uses the same ATR parameters. When confirmed
structure is available, LONG uses the more protective of the ATR candidate and
confirmed swing support; SHORT uses the more protective of the ATR candidate
and confirmed swing resistance. Missing structure falls back to the ATR
candidate. If the ordinary ATR/R activation gate has not produced an ATR
candidate yet, this policy makes no stop update; structure alone cannot bypass
activation.

weakening_sensitive_adaptive_trail_v1 uses the delayed ATR baseline in normal
states. Confirmed structure can replace the normal candidate only when explicit
momentum weakening is simultaneously present, matching the L60-01 weakening
gate. That weakening state has priority over the ordinary ATR/R activation
gate, matching the L60-01 implementation, so confirmed structure plus explicit
weakening may tighten a stop before normal ATR activation. Missing either input
keeps the delayed ATR path.

Every updating policy also inherits the L60-01 common safety constraints bound
to src/signalbot/signals/position_management.py at SHA-256
5de30eb8406d534ca3033edc0ac0f2376311d6a8d2f96e0bd8e5d4d6c433f018:
LONG stops never loosen downward, SHORT stops never loosen upward, a protection
floor cannot be crossed in the loosening direction, candidates that cross the
current reference price fail closed, stale/uncertain context produces no
update, and the 5 bp price-gap and 1 bp improvement minima apply.

## Change control

Any change to a policy definition, metric, threshold, bootstrap setting, cost
assumption, sample floor, same-bar rule, or gap-through rule requires a new
versioned preregistration and a new contract identity before outcome
computation. L60-03 and later tasks must bind the exact v1 contract hash in
their run manifests and receipts.

This change-control rule becomes active only when the L60-02 task/review receipt
records the exact canonical hash. Draft hashes produced before that receipt are
non-authoritative; no policy outcomes may be computed from a draft.
