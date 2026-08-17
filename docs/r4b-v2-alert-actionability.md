# R4B V2 Alert Actionability

Status: transport-neutral contract and exact co-gate implemented; Discord
adapter and prospective qualification remain outstanding.

Every emitted promoting Family A/B/C signal must create one visible-alert
attempt. Alert transport is measured separately from the fixed PAPER target:
late or missing Discord delivery cannot add, remove, reprice, or otherwise
change a PAPER position or its PnL root.

## Required local timestamps

One attempt retains these Unix-millisecond UTC observations:

1. durable outbox enqueue;
2. send start;
3. response first byte, when a response exists;
4. provider acceptance completion, when accepted;
5. request completion, when completed;
6. observable delivery or acknowledgement, when available.

The target `r_tau` is the local causal cursor corresponding to the fixed venue
target `D + 10,000 ms`; it is not a Discord timestamp. A target is accepted only
with a root-bound, contiguous two-point clock witness whose prior venue lower
bound is strictly below the target and whose next lower bound is at or above it.
Transport observations must be ordered and cannot occur after the record's
inclusive finalization cursor.

## Classification

- acceptance `<= r_tau`: `ALERT_ON_TIME`;
- `r_tau < acceptance <= r_tau + 30,000 ms`: `ALERT_LATE`;
- no acceptance by `r_tau + 30,000 ms`, or acceptance later than that:
  `ALERT_MISSING`;
- before that deadline with no acceptance: read-only `PENDING` monitoring view.

`PENDING` cannot enter the terminal registry. Raw timestamp observations must
stay append-only; the derived terminal record is registered exactly once.
Identical terminal duplicates are idempotent, while the same logical alert
attempt with a different payload fails closed.

## Exact co-gate

The final actionability co-gate is:

```text
N_ALERT_ON_TIME / N_ALERT_ATTEMPTED >= 99 / 100
```

It is evaluated by cross-multiplying integers, so exact 99% passes without
binary-float ambiguity. Its denominator comes from a canonical census bound to
the complete promoting-signal ledger root, not from the subset of transport
records supplied by a caller. An expected signal with no transport record is
`PENDING` before its deadline and `ALERT_MISSING` afterward, including failures
that happened before durable outbox enqueue. A summary with pending attempts is
`NOT_FINALIZED`; an empty census is `INCONCLUSIVE_NO_ATTEMPTS` rather than a
vacuous pass.

This gate measures delivery actionability only. It cannot be used to delete
late alerts from the primary PAPER portfolio. A delivered-only portfolio, if
reported later, must remain explicitly non-promoting.
