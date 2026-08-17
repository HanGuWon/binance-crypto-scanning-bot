# R4B V2 Evidence-to-Probability Calibration Plan

Status: design only. The implemented Evidence Score remains a non-promoting
agreement score and must not display a probability.

## Why raw indicator counts are insufficient

EMA, MACD, RSI, Bollinger position, and recent returns are mostly transforms of
the same price path. Counting each as an independent vote overstates the amount
of information. V2 first collapses observations into six capped information
families, then records agreement and opposition once per family.

## Frozen development sequence

1. Define one causal raw-data producer per information family, including source
   stream, closed timeframe, exact lookback, freshness, missingness, scaling,
   cap, and lineage root.
2. Use qualification data only to check coverage and numerical behavior. Do not
   inspect the prospective efficacy outcome while changing producers or bins.
3. Freeze one after-cost outcome per strategy family. Family A, B, and C keep
   their own fixed 5-minute-bar exit rules and isolated PAPER portfolios.
4. Fit a predeclared monotone calibration mapping from Evidence Score bucket to
   `P(after-cost outcome > 0)` on development data. Preserve sample count,
   base rate, Brier score, calibration error, and uncertainty for every bucket.
5. Seal code, bins, costs, panel, manifests, and mapping. Evaluate it once on a
   strictly later, untouched prospective interval. Do not refit or merge bins
   after seeing those outcomes.
6. Permit a probability label only if the untouched evaluation meets the
   predeclared calibration and coverage gates. Otherwise retain the agreement
   score and report probability as unavailable.

## Alert presentation before calibration

```text
Evidence Score (not a probability): agreement=+66.6667/100
Agreement: bullish=4 | bearish=0 | neutral=2
Data readiness: 6/6 causal families
Primary strategy: unchanged
```

The alert should also show the strongest supporting family, any opposing
family, invalidation, UTC/KST decision time, and source/rule version. Missing or
invalid input withholds the whole score; it is never imputed as neutral.

## Alert presentation after successful prospective calibration

A future successor version may add a separate line such as:

```text
Calibrated after-cost success frequency: 0.57
Prospective bucket count: 1,240
Calibration version: <sealed hash>
```

That line must identify the exact venue, family, horizon/exit policy, fee cell,
PAPER execution model, sample count, and calibration version. It cannot be
called a guaranteed win rate, and it cannot silently pool Spot with USD-M,
families A/B/C, different exits, or different fee scenarios.

## Probability is not the optimization target

A high hit rate can still lose money when losses are larger than wins. Promotion
therefore requires positive after-cost expectancy and portfolio-level risk
checks in addition to calibration. Evidence agreement is useful for ranking and
explanation; it does not replace executable depth, fees, funding, exits, NAV,
or the fixed prospective efficacy test.
