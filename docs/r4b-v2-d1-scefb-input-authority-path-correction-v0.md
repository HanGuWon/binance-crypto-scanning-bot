# D1 SCEFB input-authority path correction v0

This outcome-blind correction was recorded on 2026-07-21 before the first D1
input-authority publication, development code freeze, attempt arm, or outcome
run. The audit hashed compressed funding files and kline sidecar manifests as
opaque bytes only. It did not decompress or inspect a funding row, kline row,
or D1 outcome.

The operator initially selected
`data/backtest/funding/{alias}__{symbol}.csv.gz`, while its literal funding and
derived input-authority pins had been computed from
`data/backtest/funding/{alias}__{symbol}__5m.csv.gz`. The mismatch stopped
preparation before publication. It did not consume the one-shot attempt.

The `__5m` family is retained for these non-outcome reasons:

- D1 is the preregistered SCEFB-5M development family.
- The existing shared `funding_path(..., interval="5m")` convention resolves
  to the `__5m` family.
- The unchanged literal pins exactly authenticate that family: funding
  authority `b128bf30c6f23141e638248e47352eee4b6532317e5c8379cc04a262228fb4e8`,
  input-authority domain
  `c33a77f4223dcf2b90fbf79853beb4818af105ccb65bf248daa273a3a4089f62`,
  input-authority file
  `f22655f7a3327ed176c5bdcffb565914fe0807586338f688253208a7ea7cabd5`,
  and input-authority size `4550` bytes.
- The alternative unsuffixed family deterministically produces a different
  funding authority (`a1c68ddaf7df41799ddfc33b054db7a806fd99e29a64af343127a74dafbafabb`).

Accordingly, only the operator path helper is corrected to append `__5m`.
The preregistration and all four literal input pins remain unchanged. A frozen
unit regression reconstructs the authority from recorded opaque-byte hashes
and fails if either the selected path family or a literal pin drifts.
