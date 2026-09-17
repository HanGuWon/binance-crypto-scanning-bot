# Storage Retention Policy V2 (Phase-L)

Status: ACTIVE for Phase-L+ campaigns. Supersedes planning figures in
docs/STORAGE_RETENTION_POLICY.md (Phase-I era, ~47 GiB/day) which remain
frozen as historical record. This V2 policy uses MEASURED Phase-K data.

## Measured ingress rates (Phase-K prospective v1 pilot)

Source: exact byte counts and first/last received_at_ms from the terminated
campaign's raw tape (131 minutes of collection).

| Market | Bytes | GiB/day binary | GB/day decimal |
|--------|-------|----------------|----------------|
| Futures combined | 8,565,353,910 | 92.93 | 99.78 |
| Spot combined | 2,172,064,100 | 23.70 | 25.45 |
| **Combined** | **10,737,418,010** | **~116.6** | **~125.2** |

Peak observed 5-minute window: 635 MB (~170 GiB/day-equivalent burst).
Sustained rate is what matters for sizing; bursts are absorbed by bounded
queues and segment rotation.

## Compression outlook

Phase-K tapes are raw JSONL. Phase-L segmented storage adds zstd
(level frozen after benchmark on real Phase-K slices). Expected ratio for
repetitive market JSONL: ~4-8x, i.e. physical ~15-30 GiB/day.
Exact numbers come from the Phase-L benchmark receipt; this section is
updated after wp3.

## Headroom projection (compressed, assuming 5x ratio => ~23 GiB/day)

| Horizon | Physical need |
|---------|---------------|
| 24h | ~23 GiB |
| 3d | ~70 GiB |
| 7d | ~161 GiB |
| 14d | ~322 GiB |
| 30d | ~690 GiB |

Reserves to add: active partial segment (<=512 MiB logical), compression
transient, DB/log growth, safety margin >=20%.

## Rules

1. Legacy Phase H-K evidence is never converted or deleted; archived via the
   legacy day-archiver with byte-exact verification only.
2. Phase-L+ campaigns use segmented_zstd_v1 with per-segment manifests.
3. Active day/partials never count as archived.
4. If D: free space cannot hold the desired horizon plus reserves, state it
   explicitly and pause collection at quota — never silently delete verified
   evidence.
5. Offload (moving verified segment bundles off-box) requires explicit
   authorization; no cloud credentials in this phase.
