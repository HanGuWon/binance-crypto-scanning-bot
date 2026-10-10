# Pump-fade L3 risk-unit amendment v3.1

**Decision date:** 2026-10-10  
**Status:** user-authorized pre-outcome amendment; applies to the Phase 3 development replay only.  
**Supersedes:** the infeasible literal interpretation of “R = 1% of initial notional” for L3 v3.1 calculations.  
**Frozen-source rule:** do not edit the frozen preregistration, L1/L2 artifacts, source archives, or prior run/review evidence. This file is a separate versioned amendment.

## Risk unit and first fill

`R` is an abstract risk unit with numeric value `R = 1`; it is not a percentage of initial notional. For each parent, with frozen invalidation price `I` and first-fill reference price `P0`, define the first-fill quantity once:

```text
q0 = 0.5 * R / (abs(I - P0) + 0.003 * P0)
```

The first fill therefore consumes one half of the one-R risk budget under the frozen adverse-move-plus-fee expression. Retain the original `I`, `P0`, fee coefficient, order timing, and all other frozen policy rules. Do not retune these inputs to obtain fills.

## Adds and feasibility

Each candidate add quantity must satisfy `q_i <= 0.5 * q0`. Admit an add only when the resulting cumulative position satisfies:

```text
sum_i q_i * (abs(I - P_i) + 0.003 * P_i) <= R
```

If a candidate add violates either bound, reject the entire add without resizing or partial fill, and record it as infeasible. Preserve the rejected candidate, reason, parent, arm, decision time, and remaining budget in the replay evidence. The same parent-level `q0` is used by every arm for that parent; arm-specific sizing is prohibited.

For the legacy eight-add comparator, any candidate add with `P_i >= I` is infeasible and is recorded as truncated. Do not execute or silently omit that candidate. Other add-feasibility rules above remain in force.

## Reporting

Report realized and marked PnL in both:

1. abstract risk units (`PnL / R`); and
2. basis points of the parent’s initial notional (`q0 * P0`).

Use the same parent-level `q0` denominator across arms. Report rejected/infeasible and legacy-truncated adds separately from filled adds, and preserve unavailable/censored states. This amendment changes the sizing/risk-unit interpretation only; it does not authorize outcome-driven policy selection, holdout access, production execution, or order placement.

## Evidence and review boundary

Apply this version before reading Phase 3 outcome rows. Hash this exact file and commit only this amendment file; do not stage unrelated pre-existing changes. Then include its commit and SHA-256 in the Phase 3 plan/review identity. An independent methodological/code review must verify this amendment and its implementation before any outcome access. If a frozen source contradicts this amendment beyond the explicit v3.1 change above, stop before outcomes and report the conflict rather than editing frozen files.
