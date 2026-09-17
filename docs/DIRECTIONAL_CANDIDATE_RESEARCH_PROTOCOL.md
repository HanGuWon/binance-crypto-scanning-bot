# Futures bidirectional research protocol

The successor candidate is `futures-bidirectional-v1` and is defined by
`config/research.futures-bidirectional.v1.yaml`. It tests the existing causal
breakout/breakdown family in both directions while keeping the existing R2 and
Phase-R/Phase-S artifacts immutable.

The protocol freezes the candidate before results are read:

- 5m fully closed candles with strict-prior 15m and 1h context.
- `BREAKOUT_LONG` and `BREAKDOWN_SHORT` are separate directional hypotheses.
- Entry is the next bar open; technical exit is capped at 72 bars.
- Outcomes are recorded at 1, 3, 6, and 12 bars with the preregistered 26 bps
  research round-trip cost. This is an analysis cost, not an order permission.
- The universe and three contiguous time splits are copied into the successor
  contract by identity, rather than changing the frozen R2 configuration.
- Exact observed BBO is required for a live gate. Historical lack of BBO is an
  explicit limitation and produces an inconclusive research condition where it
  applies.

The implementation sequence is:

1. Freeze the preregistration and its configuration hash.
2. Build a manifest containing source, data-authority, universe, and trial
   registry identities before reading outcomes.
3. Run bounded deterministic fixture replay. Duplicate IDs are idempotent only
   when their payload is identical; changed payloads fail the replay.
4. Run the external historical campaign only after fixture parity passes.
5. Produce a cost-aware, direction-separated analysis and an independent review
   receipt. A failed screen yields `NO_QUALIFIED_CANDIDATE`.
6. Activate prospective shadow observation without orders or Discord messages,
   then produce a forward receipt after the preregistered gate is open.
7. Evaluate promotion. Both LONG and SHORT must meet the minimum evidence,
   data-quality, operational-health, and independent-review requirements.

The code in `signalbot.prospective.directional_candidates` implements the
identity, replay, and fail-closed adjudication contracts. It does not create
historical results, inspect sealed Phase-S values, change the recommendation
registry, or place orders. Only a receipt with `PROMOTE` may authorize a later
versioned recommendation wiring change, and even that does not grant order
permission.
