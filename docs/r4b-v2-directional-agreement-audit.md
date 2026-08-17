# R4B V2 Directional Agreement Audit

Status: frozen structural and shared-calendar bootstrap formula contracts are
implemented; no matched real outcomes have been admitted and no efficacy,
complete-inference, or probability claim is permitted.

This audit is the first downstream consumer of the directional panel's
mechanical `2/3` and `3/3` states. It does not choose indicator weights,
thresholds, assets, horizons, or exits from observed returns.

## Frozen comparison

- `TILT_2_OF_3` means two directional evidence families agree and the third is
  neutral. Any opposing family is a mixed state and is not admitted.
- `BROAD_3_OF_3` means all three directional evidence families agree.
- bullish and bearish sides are kept separate;
- the required fixed horizons are exactly 1, 3, 6, 12, and 72 closed
  five-minute bars (5, 15, 30, 60, and 360 minutes);
- each event must contain all five horizons, one row per horizon, with one
  stable event identity and one bound execution/cost contract;
- unevaluable execution or data evidence stays missing with an exclusion
  reason. It is never converted to a zero return;
- strict after-cost hits require net return greater than zero. Zero is not a
  hit;
- means, medians, coverage, hit rates, and profit factors use deterministic
  integer micro-return arithmetic.

The initial monotonic diagnostic is the point difference
`mean(BROAD_3_OF_3) - mean(TILT_2_OF_3)` for each side and horizon. A positive
point difference is not validation. The audit and bootstrap deliberately
report inference as incomplete until multiplicity control, time-ordered
validation, and an untouched prospective interval are connected.

## Frozen shared UTC-calendar bootstrap

- The caller must explicitly supply an inclusive UTC-midnight calendar start,
  an exclusive UTC-midnight calendar end, the sample count, and seed. The
  calendar contains at least seven days. Rows outside it fail closed.
- The primary circular moving block is fixed at seven UTC days. Each replicate
  contains as many complete seven-day blocks as fit the calendar plus one
  final fixed-length remainder block when necessary. Every start is sampled
  from the same circular calendar.
- One pseudorandom start schedule is generated per replicate and reused for
  all 20 `side x horizon x bucket` cells. One schedule hash is repeated on the
  top-level result, every cell, and every contrast. Sorting or reversing input
  rows cannot change the schedule or result.
- Every calendar day is materialized before aggregation. A day without an
  alert remains a zero-count day that can be drawn. An alert with an
  unevaluable outcome is not a zero-alert day, but its missing return is never
  converted to zero. A cell or contrast replicate with no evaluable
  denominator is explicitly invalid.
- The point remains the structural audit's deterministic integer-micro-return
  difference. Replicates use the exact event-weighted broad mean minus the
  exact event-weighted tilt mean on the shared resampled calendar.
- The two-sided 95% interval is the type-7 linearly interpolated percentile
  interval at 2.5% and 97.5%. The one-sided 95% basic lower bound is
  `2 * point - q95(bootstrap difference)`.
- The null-centered one-sided p-value is `1` when the point is nonpositive.
  Otherwise it is
  `(1 + count((bootstrap_difference - point) >= point)) / (valid_replicates + 1)`.
  Fraction arithmetic is exact internally and
  exposed values are converted under the protocol Decimal34 context.
- These outputs come from an exposed retrospective contract. Top-level,
  per-cell, and per-contrast `inference_complete`,
  `frozen_formula_efficacy_validated`,
  `probability`, and `probability_calibrated` remain `false`, regardless of
  interval or p-value. No threshold, promotion, or trading action follows.

Fixed-horizon observations can overlap. Therefore this contract explicitly
does not call a serial sum of those returns a portfolio equity curve or a valid
maximum drawdown. Technical-exit and non-overlapping PAPER portfolio ledgers
remain separate downstream evaluations.

## Fail-closed boundaries

The audit rejects partial horizon sets, duplicate event/horizon rows, identity
drift between horizons, two event IDs for the same deterministic panel
identity, unsupported directional states, direction/sign disagreement,
unbound execution contracts, and malformed evaluable/excluded outcome pairs.
The bootstrap first runs that complete audit and additionally requires exactly
one execution/cost-contract hash across the compared population. Two different
well-formed cost hashes are not pooled. Calendar, sample, seed, out-of-range
row, and Decimal34 representation failures also fail closed.

The current historical R3 opportunity artifact cannot populate this audit:
it lacks the new robust price evidence, exact aggTrade participation evidence,
and target-excluded directional cross-sectional candidate. Using proxy columns
as if they were the successor panel would produce an unmatched result.
