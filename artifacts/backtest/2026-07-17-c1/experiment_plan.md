# C1 exposed funding/basis carry experiment — frozen plan

Frozen before C1 outcomes are computed or inspected. C1 is a new economic
mechanism, not a threshold repair of the failed C0 technical breakout family.

## Claim boundary

- C1 is research-only, public-data, long-Spot/short-USDⓈ-M perpetual paper
  accounting. It contains no account, key, balance, private endpoint, order, or
  execution action.
- The 2024-07-01 through 2026-07-01 sample has been exposed by earlier studies.
  Every C1 result is exploratory and cannot establish generalization or approve
  deployment.
- Historical execution is a closed-5m-kline next-open proxy. There is no
  historical decision-time BBO, depth, impact, partial-fill, borrow, custody,
  liquidation, or latency ledger. A positive result remains inconclusive on
  executable live performance.

## Fixed panel and clocks

- Assets: BTC, ETH, BNB, SOL, XRP, DOGE, SUI, and WIF, paired by the same USDT
  Spot and USDⓈ-M perpetual base asset.
- Data: fully closed public Binance 5m Spot/perpetual klines and settled public
  funding rows. All timestamps are Unix milliseconds UTC.
- Evaluation splits are the three exposed intervals in the YAML configuration.
  Each split starts with no position and no inherited cooldown.
- A 30-day warm-up before a split may supply price and funding predictors. It
  may not supply a position, trade result, or outcome-conditioned state.
- Every entry fill must be at or after split start plus 7 UTC days. It must also
  satisfy `entry + 7 days < split end`; equality is rejected because split end
  is exclusive. This symmetric seven-day purge isolates all realized paths.
- Missing or nonmatching market bars break the common 5m segment. No rolling
  statistic may bridge the break. A pending entry is cancelled; an open pair is
  closed at the first post-gap common open and marked analysis-ineligible.

## Point-in-time entry rule

Entry is evaluated only on the first common fully closed 5m candle after one or
more newly settled funding events become strictly observable. The recorded
trigger is the latest such funding event. A funding row is usable only when
`funding_time_ms < decision_close_time_ms`; equality waits for the next closed
candle. Duplicate 5m entry evaluations between funding events are prohibited.

At that decision, require all of the following:

1. Exactly 8,640 immediately prior, contiguous common 5m basis observations,
   excluding the current bar. Basis is `(perpetual_close - spot_close) /
   spot_close`. Quantiles use deterministic type-7 linear interpolation.
2. Current basis is positive and at least the prior 30-day basis q90.
3. The strictly prior 30-UTC-day funding window has at least 60 settled events.
   The latest rate and prior q25 are both strictly positive, and at least 75%
   of the window's rates are positive.
4. No pair is open and the next fill is at least 24 hours after the preceding
   exit fill in that split.
5. The next Spot and perpetual opens form the same immediately contiguous 5m
   bar and satisfy the split purges.

Let `b` be current basis, `m` its prior median, `f25` the prior funding q25,
and `c` the median spacing of strictly prior funding events. Freeze
`n_hat = max(0, floor(7 days / c) - 1)`. The expected full-pair-capital edge is

`E = [0.5 * max(b - m, 0) + n_hat * f25 * (1 + b)] / (2 + b)`.

For the asset cohort, convert every bps input to a rate and define the frozen
2x-slippage stress hurdle

`C = 2 * [(spot_fee + 2*spot_slippage) +
          (1+b)*(futures_fee + 2*futures_slippage)] / (2+b)`.

Entry requires `E > C + 0.001` (a strict additional 10 bp margin). Equality
abstains. The decision ledger records the triggering funding timestamp, every
input statistic, the two edges, frozen levels, rejection reasons, rule version,
and deterministic ID.

## Pair construction, costs, and funding

- Fill both legs at the next common 5m open: long Spot and short the perpetual.
- Use equal base-asset quantity. The configured 100 USDT is full unlevered pair
  capital, so `q = 100 / (spot_entry_open + perpetual_entry_open)`. It is not
  100 USDT per leg and uses no leverage or compounding.
- Apply the frozen per-fill fee and adverse-slippage assumptions separately to
  both entry and exit on both legs using the existing execution helper.
- Funding P&L is directional short-perpetual cash flow and includes only events
  strictly inside the eligible holding interval. Events in the first 5m after
  entry fill are excluded to avoid ambiguous fill/settlement ordering. Mark
  price is used when recorded; entry perpetual price is the documented fallback.
- Store gross, slippage, fees, funding, and net both in USDT and as a return on
  full pair capital. Never subtract the cost components twice.

## Frozen exits

At entry, freeze:

- convergence target = prior 30-day basis median;
- adverse stop = `entry_signal_basis + max(0.005,
  3 * 1.4826 * prior_basis_MAD)`.

Evaluate exits on every common fully closed 5m bar and fill at the next common
open. Funding remains strict `< exit decision close`. Ignore funding events in
the first 5m after the entry fill for both funding-flip state and P&L. Exit on:

1. `STOP` when basis is at or above the frozen stop;
2. `FUNDING_FLIP` on the first newly observed eligible settled rate at or below
   zero;
3. `CONVERGENCE` when basis is at or below the frozen target;
4. `TIME` at seven days.

The listed order is the deterministic same-bar priority. There is no same-open
reversal. After a valid or gap-forced exit, the 24-hour cooldown applies.

## Outputs, acceptance, and stopping rule

The core output is an auditable decision ledger plus a nonoverlapping per-asset
trade ledger. Analysis must preserve all abstentions, report base-cost component
and net metrics before stressed metrics, and keep assets and splits visible.
Every accepted decision must map to exactly one observed trade; an accepted
decision with no trade is `OUTCOME_UNOBSERVABLE`, and more than one trade is an
integrity failure. A rejected decision mapping to any trade is also an integrity
failure. `DATA_GAP` trades remain in the audit ledger but are analysis-ineligible
and separately counted.

The primary pooled efficacy panel is `validation ∪ retrospective_test`;
`development` is diagnostic only. The primary decision metric recomputes every
eligible episode at 2x the frozen adverse slippage. Uncertainty uses 50,000
shared-asset circular UTC-calendar bootstrap replicates of seven-day blocks,
seed `20260717`. Report the one-sided 95% basic lower bound and a null-centered
one-sided p-value. If more than 0.1% of replicates are invalid, status is
`INCONCLUSIVE`. Attribute each completed episode's entire stressed P&L to the
UTC day of `entry_decision_time_ms`. The primary calendar contains every UTC
day from 2025-03-01 inclusive through 2026-07-01 exclusive, including explicit
zero-event days after applying split-purge eligibility. Exit-day attribution is
descriptive only. C1 is an exploratory pass only if all gates hold:

- at least 100 completed, analysis-eligible episodes and exactly zero
  `OUTCOME_UNOBSERVABLE` episodes;
- primary 2x-slippage mean full-pair-capital return at least 10 bp;
- one-sided 95% shared-UTC-calendar seven-day block-bootstrap lower bound above
  zero;
- 2x-slippage profit factor at least 1.25;
- both `validation` and `retrospective_test` have positive 2x-slippage aggregate
  P&L and positive 2x-slippage mean return;
- no asset supplies more than 50% of total positive 2x-slippage P&L.

No result may be used to retune C1 on this exposed ledger. A failure retires
these exact rules. A pass remains `EXPLORATORY` and `NOT_DEPLOYABLE`; it can
justify only a separately frozen prospective public-BBO shadow study whose
strategy/config/source hashes are fixed before collecting at least six months
of new data.
