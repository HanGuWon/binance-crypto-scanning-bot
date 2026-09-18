# Research Governance (Phase H preregistration)

Status: SHADOW_SUCCESSOR_ONLY. This document binds future analysis and
candidate work. It does not authorize trading or promotion.

## 1. Three economic axes

Every candidate report MUST separate:

- SIGNAL ALPHA: directional terminal return, MFE, MAE, selection
  discrimination on the frozen 1/3/6/12-bar raw path authority.
- EXECUTION QUALITY: close-reference vs executable bid/ask reference,
  spread, BBO age, directional displayed capacity.
- COST AVOIDANCE: whether rejecting an otherwise attractive opportunity
  avoided a sufficiently expensive execution state.

These axes are never collapsed into one headline metric.

## 2. First prospective analysis contract (FROZEN)

Population: all policy-neutral raw-C0 opportunities of the qualifying
prospective campaign. Per opportunity preserve: incumbent R2 result,
shadow_er_context_v1 pass/fail, causal_retest_v1 lifecycle, exact BBO
when available, 1/3/6/12 future paths.

Primary comparisons:

1. Frozen R2 descriptive performance.
2. shadow_er_context_v1 pass vs fail on the SAME raw-C0 population.
3. Retest waiting effect: for the subset that reaches READY,
   B = RAW_C0-time outcome vs C = READY-time outcome.
   B-vs-C is the retest effect; A-vs-C is NOT.

No outcome-driven threshold selection before the analysis-unlock
contract (section 5) is satisfied.

## 3. Regime & readiness observability (diagnostics only)

Outcome-blind prospective distributions for: regime label
(risk_on/neutral/risk_off), BTC trend label, breadth ratio histogram +
quantiles + time coverage; readiness frequencies for primary causal
observation, htf15, htf1h, BBO, closed-kline flow, intrabar flow,
funding, structure, causal pullback. Reported by market, calendar
period, asset. No regime logic changes before these measurements exist.

## 4. Historical data roles

- DEVELOPMENT_EXHAUSTED: all datasets used in R2/R3/R4/R4B/C1 work.
  Allowed: debugging, sanity checks, falsification, reproduction.
  Forbidden: reuse as final proof data for new adaptive candidates.
- PROSPECTIVE_QUALIFICATION: the Phase-H smoke campaign.
- PROSPECTIVE_CONFIRMATION: the first real prospective campaign.
- LOCKED_EVALUATION: any dataset explicitly locked by a future contract.

No dataset may be both discovery and final-proof data for repeated
adaptive candidate creation. No retroactive "untouched" claims.

## 5. Analysis unlock (no arbitrary day counts)

Collection continues until a separately preregistered unlock contract is
met. The unlock report must justify, with precision/power reasoning on
the PRIMARY comparison only: calendar coverage, raw-C0 count, READY
count, asset diversity, chronological spread, regime representation,
censor rate. No "90 days == enough" style rules.

## 6. Multiple testing / model selection

- Chronological splits / walk-forward; purge/embargo where labels overlap.
- Block-bootstrap uncertainty appropriate to dependent crypto data.
- Holm correction across the actually-tested candidate family.
- One locked evaluation per candidate; later prospective confirmation.
- No ritual PBO/SPA/DSR unless the design makes them informative.

Probability calibration is never assumed; any probability claim requires
a separate calibration protocol (Platt/isotonic) evaluated on unseen
chronological data with reliability curves and proper scoring rules.

## 7. Candidate registry (prepared, NOT activated)

Priority order:

1. FAILED_BREAKOUT_REVERSAL_V1 - studies why current raw C0 fails;
   causal real-time definition; no hindsight leakage; same raw outcome
   authority. Thresholds chosen BEFORE any smoke outcome inspection.
2. REGIME_CONTEXT_SUCCESSOR_V1 - BTC realized vol, cross-sectional
   dispersion, funding stress, relative state; outcome-blind
   availability study FIRST.
3. L2_EXECUTION_PILOT_V1 - can L2 estimate execution quality beyond L1?
   Reuse validated capture owners (src/signalbot/capture/local_book.py);
   resource-bounded; fixed outcome-blind symbol rule; directional L2
   analysis SECONDARY.
4. SELECTIVE_LR_GBDT_V1 - LONG/SHORT/ABSTAIN architecture. Baseline LR
   then small GBDT comparator. Requires NEW prospective data.
   Calibration not assumed.
5. AUTO_PROTECT_TRACK - separate execution-safety program
   (ACCOUNT_OBSERVE -> reconciliation -> PAPER guardian -> testnet
   protect-only -> AUTO_PROTECT). No private APIs now.

## 8. Label governance

Authoritative outcomes remain the raw 1/3/6/12-bar path, terminal
return, MFE, MAE. A triple-barrier label may be derived as a VERSIONED
research label from immutable raw path evidence; changing target/stop
parameters must never destroy raw outcome authority.
