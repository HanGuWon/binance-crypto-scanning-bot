# v2c post-exposure rule remediation (not a new confirmatory preregistration)

Recorded 2026-10-07. This document creates an explicit, versioned engineering rule identity after the v2b historical kline proxy and earlier P1 outcomes were exposed. It is not represented as preregistered before those outcomes, and no corrected retrospective output is confirmatory.

## Authority and immutable history

The authoritative strategy plan (`bb0504050b157c0456fe309731a4ea2954cbbb9a1d2e4773d21edbbab522e986`) specifies a +15% adverse mark excursion for the primary squeeze outcome. v2b instead encoded 20%, which was a contract conflict. v2c resolves primary to **15%**, including an excursion exactly equal to 15% (`>=`). A 20% result may be reported only under the separate registered sensitivity label. The v2b policy, freeze, preregistrations, proxy events and results remain unchanged historical artifacts.

## Funding state contract

An observed cap hit in the preceding 24 hours remains a WAIT reason until three chronologically subsequent, observed predicted-funding samples are at least 0.001 below the applicable observed cap, each at least 60 seconds apart. The hit may predate event admission. Any repeated hit clears the normalization streak. Missing or stale cap metadata does not qualify a sample and cannot be used to infer a historical cap. Clearance applies only to the cap-hit reason; stale inputs and all other WAIT reasons remain active.

For interval shortening, restoration is measured against the fresh observed interval immediately preceding the first shortening in the current lookback window. Thus 8h→4h→2h→4h remains WAIT; fresh observed restoration to 8h clears this interval-risk reason. This is an explicit conservative engineering clarification, not historical evidence.

## Data and decision boundaries

All alert decisions remain disabled, public-data-only, closed-candle-only and no-orders. Event/alert outcomes retain 4h and 24h horizons and explicit pending/censored/unavailable values. R1 comparisons use the same original parent cohort and retain abstentions; R2 first-fill, eight-addition primary and separate three-addition sensitivity remain distinct; R3 includes the nine joint cells and separately labeled 25/8 and unconditional-40 sensitivities; R4 is a counterfactual/refutation analysis, not LONG advice. Bootstrap units remain UTC event-day clusters, 10,000 paired draws and registered Holm families. Missing mark/BBO/OI/funding/receipt/P3 fields remain UNAVAILABLE; no synthetic values are imputed.

Forward evaluation remains disabled and not started. Promotion requires an independently eligible future start, at least 56 days and 150 valid parent alerts, cluster/power adequacy, and a strictly positive one-sided 95% clustered lower confidence bound. No package build or release receipt authorizes OCI deployment, Discord delivery, or production behavior.
