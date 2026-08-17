# D1 SCEFB development-freeze policy-order correction A1

This outcome-blind governance correction was recorded after the first freeze
publication and before any D1 attempt arm, START append, outcome-access grant,
row access, runner invocation, output publication, or production order.

The first freeze publication is preserved without deletion, replacement, or
same-path retry:

- path:
  `artifacts/backtest/2026-07-21-d1-scefb-v0-development-freeze/freeze_manifest.json`
- SHA-256:
  `328899911e4b1dd3acd9f12b5f1d8cd1f08f5df08b55d350670215649efa8316`
- created at: `2026-07-21T12:26:19.543925+00:00`
- frozen files: `229`
- frozen bytes: `8,283,427`
- retirement status: `RETIRED_PRE_ARM_OUTCOME_BLIND`
- reason: `INCLUDE_FILES_POLICY_ORDER_MISMATCH`

The generic freeze writer canonicalized the requested file sequence
lexicographically. Its durable manifest therefore listed the preregistration
document before the input-authority path-correction document. The D1 policy
constant held those two paths in the opposite order. Every other policy
predicate passed, and the generic loader verified every recorded file byte
before the D1-specific sequence comparison rejected the publication.

The first attempt and output paths remained absent. In particular:

- `attempt_armed = false`
- `start_append_attempted = false`
- `outcome_access_grant_issued = false`
- `outcome_rows_opened = false`
- `runner_invoked = false`
- `output_publication_attempted = false`
- `efficacy_claim = false`
- `probability_claim = false`
- `profitability_claim = false`
- `production_order_placement = false`

The successor keeps the preregistered D1 rule and literal input pins unchanged.
It uses purpose
`D1_SCEFB_HISTORICAL_DEVELOPMENT_OUTCOME_BLIND_A1_POLICY_ORDER_CORRECTION`,
freeze path
`artifacts/backtest/2026-07-21-d1-scefb-v0-development-freeze-002/freeze_manifest.json`,
run ID `d1-scefb-v0-development-run-002`, and matching attempt/output paths.
The successor file list is already canonical, explicitly includes this record
and the retired manifest, and binds the retired manifest hash again as
`d1_predecessor_freeze_001` upstream provenance.
