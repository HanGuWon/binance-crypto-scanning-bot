# Independent Project Pro adversarial R4b review

Captured: 2026-07-17  
Conversation: <https://chatgpt.com/g/g-p-69a9f92954288191a063fd1eea40b983-gasanghwapye-teureiding/c/6a599091-df04-83ee-a7b7-f6475d3de4bc>  
Visible processing time: 58m 31s  
Visible composer mode: Pro

## Provenance boundary

The answer was read from the user's signed-in ChatGPT project. The page visibly
showed `Pro`, but the earlier web-automation session timed out before completion
and did not independently verify a hidden model or effort identifier. This memo
therefore calls it an independent Project Pro review, not a verified claim about
an internal `5.6 SOL PRO` model string.

The response displayed two generated deliverables:

- `R4b_adversarial_design_review_20260717.md`, displayed SHA-256
  `0d55c65a359e82698b913fecaa38b1818ae464fc2436fc28cd79e52aa11bcfba`
- `R4b_frozen_experiment_spec_v1.yaml`, displayed SHA-256
  `9c925f5988e65a1371e8859dd00ea6a61db0c3b9ea34622432e9a28a3bab297b`

Both artifacts were subsequently recovered from the authenticated ChatGPT
artifact responses on 2026-07-17 and saved under
`artifacts/oracle/2026-07-17/` and `C:/Users/user/Downloads/`. Their locally
recomputed SHA-256 digests exactly match the provider-displayed values above;
the YAML also parses successfully as a mapping. Retrieval evidence is recorded
in `artifacts/oracle/2026-07-17/r4b_project_pro_download_manifest.json`.

## Decision

The review did not establish a profitable R4b strategy. It assigned:

```text
R4A_SEALED_READ_ONLY
R4B_CAUSAL_V1 = INCONCLUSIVE_DATA / UNTESTED
```

R4a cannot be rescued by threshold relaxation, recalibration, RSI, or another
meta-label on the same 52,107-event ledger. The proposed successor is limited
to three mechanisms that introduce state variables absent from the retired C0
event family:

1. derivatives crowding transitioning to forced deleveraging;
2. aggressive flow interacting with visible-liquidity depletion or absorption;
3. asynchronous cross-sectional propagation of a broad common shock.

Families A and B require historical point-in-time microstructure data that the
local archive does not contain. Family C permits only a gross kline mechanism
screen; an executable-net claim still requires historical or prospective BBO.

## Family A: crowding to deleveraging

The precondition uses a 12-bar return, 12-bar open-interest change, mark/index
basis, and the last funding estimate actually received before the decision.
All are robustly normalized against 8,640 strictly prior 5-minute observations.
The directional crowding state requires absolute return and OI z-scores at least
1.5, aligned basis at least 1.5, and aligned funding estimate at least 1.0.

The closed-bar transition requires a one-bar price reversal, falling OI, and
opposing aggressive flow. A crowded-long unwind maps to Futures short plus Spot
exit-risk; a crowded-short unwind maps to Futures long plus Spot long. Frozen
exits are basis normalization, two consecutive bars of flow reversal, price
invalidation, or 12 bars/60 minutes.

Required data: klines, timestamped OI, mark/index, strict-prior funding
estimates, trades or aggTrades, BBO, sequence-consistent depth, actual fees and
realized funding. The liquidation stream is diagnostic only because its public
stream is censored rather than a complete liquidation ledger.

Local status: `INCONCLUSIVE_DATA`.

## Family B: flow and visible-liquidity dislocation

The family measures aggressive buy/sell imbalance, the BBO-mid move from the
first to last five seconds of the completed bar, opposing depth within 10 bp,
depth depletion/replenishment, and the bar's 95th-percentile spread.

- Depletion continuation requires extreme flow, aligned price response,
  opposing depth falling to at most half its start level, little replenishment,
  and spread no greater than 20 bp.
- Absorption reversal requires extreme flow with muted price response, strong
  opposing-depth replenishment, and the same spread cap.

The two children form one primary family and use a fixed within-family
Bonferroni correction. Frozen exits are a three-bar/15-minute horizon, opposing
flow, or one event-bar true range of adverse movement.

Required data: receipt-timestamped trades/aggTrades, BBO and sequence-consistent
diff-depth for both the feature and execution, plus actual fees and realized
funding for Futures. Five-minute taker volume is not a substitute for book
evolution or receipt time.

Local status: `INCONCLUSIVE_DATA`.

## Family C: common-shock lag/catch-up

For each venue, the family forms each symbol's three-bar log return and the
cross-sectional median return. A beta to that common component is estimated
only from the preceding 8,640 bars and clipped to `[0.25, 2.5]`. The residual
lag score divides the beta-implied return gap by a strictly prior robust
residual scale.

The frozen common-shock gate requires:

- common return robust z-score at least 2.5 in magnitude;
- same-sign breadth at least 70%;
- at least 20 point-in-time eligible symbols;
- top-decile lag score and score at least 1.5.

Positive shocks map to Spot/Futures long; negative shocks map to Spot exit-risk
and Futures short. Frozen exits are 75% gap closure, a 50% adverse overshoot, or
six bars/30 minutes.

The current eight-asset local archive cannot satisfy the frozen 20-symbol gate.
Changing that minimum after seeing an eight-asset outcome would be a different
protocol. Family C is therefore not a valid local R4b efficacy test yet.

## Observation and execution contract

For a bar ending at exchange time `T`, the proposed decision cutoff is
`T + 2,000 ms`; baseline execution is the first valid quote at least 250 ms
later. A feature event must have exchange event time no later than `T` and local
receipt time no later than the cutoff. Stale, crossed/locked, sequence-gapped,
or insufficient-depth quotes are `NON_EXECUTABLE`, not zero-return trades.

Spot `EXIT_RISK` is explicitly not a synthetic Spot short. It is evaluated only
against an existing standardized Spot inventory: sell at the executable bid,
remain flat until exit, and rebuy at the executable ask, including both extra
fee/slippage legs. Without inventory it remains an alert with no Spot-short P&L.

## Validation and stopping rules adopted

- Exactly three primary families with one-sided Holm FWER 0.05; Family B's two
  children use Bonferroni 0.025.
- Synchronized UTC daily portfolio P&L, 50,000 circular seven-day block
  bootstrap replicates, and 14/28-day sensitivity.
- Immutable attempt ledger, White Reality Check, Hansen SPA, deflated-Sharpe
  probability at least 0.95, and PBO no greater than 0.10 before promotion.
- At least 1,000 historical executions, 300 non-overlapping historical
  episodes, 365 calendar days and four complete quarters per family.
- At least 500 prospective executions, 150 non-overlapping episodes, 180 days
  and two quarters, with a maximum wait of 365 days.
- After all execution costs: mean at least 5 bp, one-sided lower mean above
  zero, PF at least 1.20 with block lower bound above 1.05, and nonnegative
  mean/PF at 2x cost.
- Concentration, quarter/symbol stability, top-trade removal, Sharpe/Sortino,
  drawdown and Calmar gates are conjunctive rather than selectable.
- Any outcome-affecting code, feature, threshold, universe, exit, cost or schema
  change starts a new version and resets the prospective clock.
- Missing required microstructure data yields `INCONCLUSIVE_DATA`, never a
  kline proxy. Failure of a gross lower bound, executable-BBO edge, cost stress,
  stability, multiplicity, or the one sealed holdout is terminal for that
  version.

## Integration decision for this repository

1. Do not run or promote the existing H1--H5 draft as R4b. Independent code
   review found that H5's `rank < 0.50` cannot precede H1's existing
   `rank < 0.70` exit, and some draft reward/cost gates depend on the next open.
2. Keep C1 funding/basis carry as a separate, single-hypothesis exposed-sample
   diagnostic. It is not evidence for Families A--C and cannot be combined
   post hoc with them.
3. Add prospective public stream capture for A/B observables and executable
   BBO before efficacy testing. Preserve public-data/no-key/no-order invariants.
4. Do not weaken Family C's 20-symbol point-in-time universe to fit the current
   eight-asset archive. Either acquire the frozen universe history or record
   insufficient data.
