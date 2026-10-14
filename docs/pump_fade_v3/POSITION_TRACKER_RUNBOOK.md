# L7 position tracker runbook

L7-G1 contains an offline ledger and warning engine. Configuration is disabled
by default, `RuleContext` is `UNAVAILABLE`, records remain `UNVALIDATED`, and no
Discord module or delivery path is connected. The packaged systemd unit is not
installed or enabled by this change.

## Supported inputs

- Synthetic or recorded fill events, closed 5-minute mark candles, and explicit
  account/market health observations.
- The Guardian library now exposes private reads for position snapshots,
  account and symbol configuration, and user-trade recovery. G1 injects those
  sources into the offline engine; its disabled service stub does not connect
  them. A later operator-managed setup must use a read-only key.
- Manual position/plan data supplied by the operator. A manually maintained
  position has no exchange liquidation evidence, so its liquidation status is
  `UNKNOWN`.

## Records and recovery

SQLite stores immutable record payloads keyed by deterministic `record_id`; the
same records are mirrored to UTC-day JSONL files. Replaying a record with the
same ID and bytes is a no-op. Reusing an ID with different bytes is an error.
User-trade recovery is bounded by a last-seen trade ID and replay uses trade-ID
deduplication. REST reconciliation treats a missing v3 position row as zero,
defers known REST/stream races, and requires two consecutive mismatches before
degrading the ledger. Fenced stream gaps must be recovered before state warnings
resume; degraded intervals are never backfilled with warnings.

## Warning interpretation

Warnings are decision records, not trade instructions. `THESIS-BROKEN`,
`ADD-BLOCKED`, `ADD-DETECTED`, and `LIQ-PROXIMITY` use the exact preregistered
identifiers and carry source values, reasons, invalidation/recheck rules, policy
hash, rule version, UTC and Asia/Seoul timestamps, and `UNVALIDATED` status.
Long episodes receive only liquidation proximity and fill-count reporting.
`ADVERSE-CONTINUATION`, `RUNNER-HOLD`, and `RUNNER-EXIT` stay inactive until a
selected rule or promoted arm hash is supplied.

## Not part of this gate

No exchange order path, Discord delivery, credential installation, deployment,
systemd install/activation, or rules-live implementation is included. L7-G2
must port `rules_live` only after the L3 G2 gate and add byte-equality fixtures
against the research implementation.
