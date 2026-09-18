# Storage Retention Policy (Frozen)

## Scope

This document defines the operational retention semantics for prospective
campaign raw-evidence storage. It is frozen for Phase J and later campaigns
until explicitly amended by a new signed policy document.

## Storage tiers

| Tier | Applies to | Format | Retention |
|------|-----------|--------|-----------|
| Active | Current UTC day | Uncompressed JSONL | Always present while campaign runs |
| Closed-unarchived | Previous UTC day(s) not yet archived | Uncompressed JSONL | Preserved until archive verified |
| Archive authority | All closed days with verified archive | gzip level 9 (.jsonl.gz) | Permanent; never auto-deleted |

## Protocol

1. At each UTC day rollover, the previous day's JSONL becomes a candidate for archival.
2. Run `tools/archive_raw_tape_day.py` per market per closed day.
3. The archiver verifies byte-exact roundtrip before retiring the original.
4. If roundtrip fails, the original is preserved, the archive attempt is marked FAILED, and no deletion occurs.
5. Archived authority files are immutable once sealed.

## Quota management

- `raw_event_max_bytes` in deployment config is a hard ceiling; the recorder
  fails closed when reached.
- Storage states: HEALTHY (<70% quota), WARNING (>=70%), CRITICAL (>=90%).
- At CRITICAL: stop admission gracefully, flush all pending writes, preserve evidence.
- Never delete unverified or active files to save space.

## Planning parameters (measured Phase-I)

| Parameter | Value | Source |
|-----------|-------|--------|
| Combined raw rate | ~47 GiB/day uncompressed | Phase-I measured |
| gzip compression ratio | ~0.119 (Phase-H seed); verify on first real archive | Phase-H historical |
| Effective post-archive rate | ~5.6 GiB/day if ratio holds | Computed |
| D: free at Phase-J start | ~117.9 GiB | Measured 2026-08-24 |
| Days of headroom (no archive) | ~2.3 days on 110 GiB quota | Computed |
| Days of headroom (with daily archive) | ~20 days on same quota | Computed assuming 0.119 ratio |

## Operational rules

1. Daily receipts record raw bytes, archive bytes, and verification status.
2. Archive failures trigger an operator alert (not silent retry).
3. No compressed authority file may be deleted without explicit user authorization.
4. The C: rollback tree must never be used as evidence storage.
