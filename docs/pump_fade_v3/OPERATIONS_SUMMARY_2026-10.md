# Pump Fade v3 operations summary (October 2026)

Public summary of facts only. Hosts, addresses, keys, credentials and internal paths are deliberately omitted; detailed operator receipts are kept privately.

## Collector
- The v3 public-data collector started operating in early October 2026. Its forward evaluation window starts at `2026-10-10T16:23:05.435Z`.
- There is a data gap on 2026-10-09, and five incomplete capture generations are excluded from evaluation.

## Archival
- Captured data is bundled once per day and uploaded to cloud storage by a verified uploader (archive is checked against its manifest before upload).
- Local data is pruned after seven days, by whole date, and only after a verified upload.

## Deployed source commits
| Component | Commit |
|---|---|
| Collector | `3185186` |
| Uploader | `4efc4a7` |
| Installer | `dcf2f27` |

## Recommendation bot
- The deployed recommendation bot predated the log-redaction fix (`70496be`). The hotfix applied only that commit's `logging.py` to the running server.
- The notification webhook was rotated afterwards. No further detail is published.

## Phase 2 coverage
- The Phase 2 coverage scan (run `5335fc5c…`) completed: 40,812 parents, status `VALIDATED_WITH_RECORDED_GAPS`. Outcomes have not been computed.

## Scope
Nothing here asserts strategy efficacy. Phase 3 has not started.
