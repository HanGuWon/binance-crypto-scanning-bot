# R4B V2 position-terminal authority plan

## Current verdict

The repository now has factory-sealed A/B/C lifecycle receipts, a typed
position-terminal calculation, a mirrored attempt-wide outcome WAL with
clean-prefix replay, and one restart-safe typed lifecycle owner above that WAL.
The owner durably reconciles open/exit PREPARE and DISPOSITION records, four
signed cashflow classes, complete or incomplete terminals, and exact restart
state. It still lacks the upstream ledger-owned exit preview and the
mandatory-exit, fee, and funding replay authorities needed to make the final
outcome authoritative. The current position terminal deliberately keeps
`position_terminal_authoritative=false`, `typed_wal_replay_authoritative=false`,
and `efficacy_eligible=false`.

## Required authority chain

```text
durable typed PAPER entry terminal
  -> exact full-fill certificate
  -> sealed family admission receipt
  -> sealed family exit receipt
  -> mandatory-exit position and intent
  -> causal exit book generation(s)
  -> mandatory-exit terminal and fee certificate
  -> final public fee resolutions for entry and every exit slice
  -> complete funding-time census and realized cashflows
  -> factory-sealed POSITION_TERMINAL
```

A no-position result may close as `SUPPRESSED` with exact zero cashflows. A
position can close as `COMPLETE` only when all of the following are true:

- full-quantity PAPER FOK entry;
- exact family admission and terminal exit mutation;
- mandatory exit status `EXITED_FULL` with zero residual;
- every entry and exit fee resolved from the final public fee timeline;
- every funding timestamp in the holding interval enumerated and resolved;
- no family, data, filter, execution, fee, or funding inconclusive state.

Anything else is `INCOMPLETE`; after-cost PnL and return must be null. Dust or
residual inventory cannot be valued with a fallback mark to manufacture
completion.

## Reusable sealed components

- `PaperFokEntryDecisionV2` and `PaperFokFullFillCertificateV2`
- `MandatoryExitLedgerV2` and `MandatoryExitFeeCertificateV2`
- `FilledPositionFeeV2` from `calculate_filled_position_fee_v2`
- `FundingPositionSnapshotV2`, `RealizedFundingCashflowV2`, and
  `calculate_realized_funding_cashflow_v2`
- `ProspectiveDecisionTransactionOwnerV2` and its exact A/B/C authority tokens
- factory-sealed Family A/B/C admission and terminal-exit mutation receipts
- `ProspectivePositionTerminalPayloadV2` for exact bounded cashflow arithmetic
- `ProspectiveOutcomeWalStoreV2` for the single attempt-wide mirrored chain and
  structural clean-prefix replay

## Missing owners

### Typed lifecycle owner

Implemented. The owner exclusively holds the attempt-wide WAL capability,
parses every typed payload, reconstructs semantic state after restart, enforces
monotone transition times, permits one-use exact `PREEXISTING` reconciliation
only for recovered pending mutations, and proves complete terminals against
the preceding durable cashflows. It can also drain an opened position as an
auditable `INCOMPLETE` terminal without inventing final arithmetic. This is
typed durability, not efficacy authority: exit PREPARE remains a deliberately
non-authoritative decision projection until the family ledgers expose a common
non-mutating preview/commit seam.

### Mandatory-exit causal authority

Caller-supplied target roots and book generations cannot authorize outcome
records. Exit evaluation needs the same one-use, current signed-prefix
reverification pattern used by PAPER entry, followed by a durable receipt.

### Funding census

Individual funding rows are insufficient. A position-bound certificate must
seal the expected funding timestamps, signed quantity immediately before each
timestamp, terminal confirmation or inconclusive status, cashflow root and
sum, and the exact position/funding registry checkpoints.

### Attempt-wide result persistence

Final fee evidence can become available long after the origin UTC shard closes.
The daily decision WAL therefore cannot own final position results. The new
attempt-wide outcome vocabulary is:

```text
POSITION_OPEN_PREPARE
POSITION_OPEN_DISPOSITION
FAMILY_EXIT_PREPARE
FAMILY_EXIT_DISPOSITION
POSITION_CASHFLOW*
POSITION_TERMINAL
```

Each record retains the origin segment, cell, and sizing identity but belongs
to one attempt-wide hash chain. The record envelope, mirrored store/replay
checkpoint, typed position-lifecycle owner, and bounded payload semantics
across all six record kinds are implemented. The remaining upstream replay
authorities are incomplete, so the owner and terminal still fix both
`typed_payload_semantics_authoritative=false` and `efficacy_eligible=false`.

## Position terminal fields

The final payload must remain below 64 KiB by referring to canonical object
hashes and checkpoints rather than embedding large books or ledgers.

- identity: attempt/plan/execution contract, origin segment/cell/sizing,
  family, symbol, schema/rule/payload hash;
- status: complete/suppressed/incomplete, reasons, invalidation,
  position-opened and executed-episode eligibility;
- entry: entry terminal/record/certificate, side, quantity, VWAP, notional,
  signed cashflow;
- family lifecycle: admission/exit receipt and pre/post state bindings;
- exit: intent/cursor/terminal/certificate/checkpoint hashes, slice root,
  quantities, notionals, cashflows, and residual class;
- fees: scenario/multiplier, final timeline checkpoint, status, entry/exit/total;
- funding: census certificate, expected/confirmed counts, cashflow root and sum;
- PnL: gross, funding, fee, after-cost PnL, denominator, return;
- safety: no production order and no private account fee claim.

## Frozen arithmetic

```text
long entry cashflow  = -entry_notional
short entry cashflow = +entry_notional
exit cashflow        = sum(signed mandatory-exit gross cashflows)
gross_pnl            = entry_cashflow + exit_cashflow
after_cost_pnl       = gross_pnl + realized_funding_cashflow - final_total_fee
after_cost_return    = after_cost_pnl / entry_executable_notional
```

Entry and exit VWAP already reflect book impact and slippage. Diagnostic
slippage versus a reference price must not be subtracted a second time.

## Implementation order

1. A/B/C sealed admission and exit receipts. **Implemented.**
2. Extend the existing decision owner into a position-lifecycle owner.
   **Implemented as a non-authoritative typed durability owner.**
3. Add typed outcome payloads, mirrored attempt-wide WAL, and restart parser.
   **Implemented, including complete/incomplete terminal replay and crash-window
   reconciliation.**
4. Add one-use mandatory-exit causal authority and atomic exit transitions.
5. Add inventory checkpoint and funding census certificate.
6. Compose the factory-sealed `POSITION_TERMINAL` and exact PnL arithmetic.
   **Implemented for typed arithmetic; authoritative upstream certificates are
   still pending.**
7. Add authoritative final seal, recovery, duplicate prevention, and Hmax drain.
8. Only then feed complete terminals to the preregistered efficacy evaluator.

## Current verification evidence

- A/B/C receipt, lifecycle-owner, terminal, and outcome-store regression:
  `88 passed` in an independent run.
- Lifecycle-owner focused tests: `8 passed`, including opened-incomplete drain,
  restart reconciliation, time monotonicity, no-position replay, exact
  cashflow reconciliation, and duplicate-terminal rejection.
- Earlier position-terminal plus A/B/C lifecycle regression: `194 passed`; this
  overlaps the newer suites and is not added as a unique-test total.
- Focused Ruff and Pyright checks for those modules pass, and the source/test
  tree compiles. These checks prove local contracts only; they do not prove
  prospective efficacy or the still-missing typed end-to-end owner.

## Mandatory adversarial coverage

- forged or foreign family receipt and Family B exit decision;
- fault injection between every WAL and family mutation boundary;
- expired/replayed exit capability and caller-built book generation;
- partial/multi-exit conservation, overfill, duplicate generation, dust;
- unresolved fee interval, changed fee bracket, missing leg, wrong multiplier;
- missing/duplicate/conflicting funding timestamp or signed quantity;
- equal-time funding convention and long/short cashflow signs;
- negative after-cost PnL as a valid complete numeric result;
- finalization on a later date while preserving origin-cell uniqueness;
- every restart point either reconstructs exactly or fails closed;
- 64 KiB payload and bounded cache/ledger limits.
