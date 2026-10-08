# L0 — OCI read-only capacity gate

No repository/config/service mutation. Read only the running OCI host's proc/sys/service telemetry and OCI tenancy metadata. Do not restart, reconfigure, load-test, or redeploy the recommender. Target identity must be independently confirmed before SSH. Measure continuously for at least 30 minutes; retain timestamped samples and a redacted receipt.

## PAYG account, Always Free-only objective

The account is PAYG, but the operating objective is to remain within Always Free entitlements. Do not create/resize/upgrade OCI resources or change billing, budgets, or quotas. The collector uses the existing VM and creates no OCI resource; it does not change provisioned OCPU/GB-hours. Inbound data is free. Expected outbound is Binance requests plus the scheduled PC pull, capped by plan at ≤45 GB/month versus the 10 TB allowance. Cost verification is a separate open item and does not block the collector: retry OCI Usage and Monitoring APIs with a 120-second timeout, scoped to the single compartment, month-to-date, daily granularity. If still unavailable, mark “user to verify in OCI Console > Billing > Cost Analysis”; do not infer zero cost. Do not change budgets or alerts.

Official Always Free reference checked 2026-10-07: A1 monthly grants are 1,500 OCPU-hours and 9,000 GB-hours; combined boot + block storage allowance is 200 GB; outbound data transfer allowance is 10 TB/month. Current inventory is one 2-OCPU/12-GB A1 Flex VM, a 47-GB boot volume, and a 150-GB block volume (197 GB provisioned total). For a 31-day month, this VM uses 1,488 OCPU-hours and 8,928 GB-hours, within 1,500/9,000. A second A1, a resize, or another volume is forbidden. The present volumes leave ~3 GB nominal storage allowance. These figures alone do not prove all tenancy usage is free. Oracle budget alerts are soft notifications and do not prevent charges. Sources: [Always Free resources](https://docs.oracle.com/en-us/iaas/Content/FreeTier/freetier_topic-Always_Free_Resources.htm), [Budgets overview](https://docs.oracle.com/en-us/iaas/Content/Billing/Concepts/budgetsoverview.htm).

Record shape/architecture, volume cap/use, `nproc`, `free -m`, `df -h`, `uptime`, recommender process CPU% and context switches, host run-queue, CPU steal, CPU/memory/I/O PSI, WebSocket connections, and REST weight/429/418 from existing logs. For each closed 5m kline, derive close-to-evaluation/log delay p50/p99 if a matching recommender log line exists; otherwise say unavailable and use process/host metrics only. Do not print environment variables or credentials. Compare seven-day CPU/network/A1-memory telemetry to Oracle's reclaim criteria; low utilization is reclaim risk, not a cost. Retry one-compartment, month-to-date, daily OCI Monitoring query with a 120-second timeout; if unavailable, record it as a user Console check. Never inject synthetic traffic.

Gate deliverable is a Korean status table in the final report. L0 may be PASS-WITH-OPEN-ITEMS when the observed baseline meets A1/A3; billing and seven-day reclaim telemetry remain separate open items and do not block L0b. Measure the following identically before and during the staged run: (i) recommender process CPU%/context switches; (ii) host run-queue, CPU steal, and CPU/memory/I/O PSI; (iii) per-closed-5m-kline close-to-evaluation/log-delay p50/p99 when logs permit. If (iii) is unavailable, require recommender CPU increase ≤5 percentage points. In all cases require `MemAvailable` ≥3 GB, no swap-in growth or OOM, memory PSI `some avg10` <5%, load ≤1.6, and no new 429/418. The user confirmed `MemAvailable` (not literal `MemFree`) governs the memory threshold. If any available comparison breaches a limit, stop/disable only the new collector and report; never tune/restart/reconfigure the recommender or deploy current main to OCI.

Collector storage/retention contract: data directory is on the DATA filesystem only, never root. Hard cap is 16 GiB; keep at least 12 GiB free on the data filesystem at all times. On either limit, drop event-scoped depth first, then stop writing and emit CRITICAL; never delete unarchived data. Keep newest 7 days on OCI. A user-run/scheduled PC-side pull script copies older verified segments to `D:\Binance-trade-analysis\oci-capture\` over the existing SSH route; only after hash-verified readback may the OCI copy be pruned, reusing v2d hash-acknowledged prune logic. Target <1.5 GB/day compressed. The decision allowed `depth5@1000ms` or dropping depth if measured 24h exceeds 2.0 GB/day; current USDⓈ-M documentation does not advertise a 1000ms partial-depth speed, so the implemented fail-safe is to drop event-scoped depth entirely and report. Do not add/resize volumes.

## Read-only observation receipt — 2026-10-07

Thirty-one once-per-minute samples ran from approximately 14:12 to 14:42 UTC against the independently confirmed running host. No configuration, service, resource, or application data was changed.

| Observation | Result |
|---|---|
| Shape | `VM.Standard.A1.Flex`, 2 OCPU, 12 GB RAM; one running instance found in the queried root scope |
| Volumes | 47 GB boot + 150 GB block = 197 GB provisioned; 200 GB combined allowance leaves ~3 GB nominal headroom |
| Memory | `MemAvailable` stayed around 11.09–11.10 million kB (~10.6 GiB), above the user-confirmed 3 GB gate; `MemFree` stayed around 1.83 million kB (~1.75 GiB) |
| Load | Maximum sampled 1/5/15-minute load was approximately 0.56/0.40/0.40; below the 1.6 gate |
| Disk | Root free ~15.1 GiB; data filesystem free ~32.4 GiB (about 77% used). The previous 40 GiB statement was a collector cap, not a free-space floor; revised collector cap/floor are 16 GiB/12 GiB, and no volume change is needed |
| Recommender | `signalbot.service` stayed active with zero observed restarts; process RSS ~539,208 kB (~526 MiB) |
| Runtime/API symptoms | Each sampled recent-one-minute journal window had zero 429, 418, or request-weight lines. Established remote :443 sockets varied from 3 to 10 |
| Loop-lag metric | Unavailable: the live `signalbot.db` did not contain the documented `runtime_heartbeats` table; do not infer p99 lag |
| OCI billing / 7-day monitoring | Not verified: prior read-only Usage Summary and Monitoring API calls did not return within 25 s / 30 s. Retry once with a 120 s timeout and narrowed query; if still unavailable, user verifies in Console. This is an open item, not an L0b blocker |
| Egress and complete tenancy inventory | Not verified from returned usage/billing data and complete all-compartment resource evidence |

The short host sample does not substitute for seven-day OCI Monitoring statistics, and API call timeouts or an empty Usage result do not mean zero usage or zero charges. The 40 GiB value is not a required free-space amount. L0 is PASS-WITH-OPEN-ITEMS after the separate 2026-10-08 A3 baseline below; billing and seven-day reclaim telemetry remain user-verifiable open items. Do not create or resize OCI resources or alter billing/budget/quota configuration.

## Read-only A3 baseline receipt — 2026-10-08

Thirty-one timestamped samples ran once per minute from 02:05 through 02:35 UTC (30 valid one-minute deltas). The SSH route used keepalives; the sampling command only read `/proc`, systemd properties, socket counts, and aggregate journal classifiers. No host or recommender state changed.

| Measure | Baseline | Gate / interpretation |
|---|---:|---|
| Recommender CPU, normalized to 2 OCPU host | p50 14.39%, p95 17.77%, max 20.98% | Staged run must use the identical measure; if evaluation-delay proxy remains unavailable, p95 rise must be ≤5 percentage points (ceiling 22.77%) |
| Recommender context-switch deltas | voluntary p50 34,193/min, p95 37,480/min; involuntary p50 105/min, p95 226/min | Baseline captured for identical before/during comparison |
| Host load | max 1/5/15-minute values 0.49 / 0.37 / 0.31 | ≤1.6 |
| Run queue | max 3 runnable / 246 total tasks | Record identically during stage |
| CPU steal | max 2.552% | Recorded; no gate breach observed |
| PSI `some avg10` | CPU max 0.72%; memory max 0%; I/O max 1.26% | Memory must remain <5% |
| `MemAvailable` | minimum 11,396,528 kB = 10.87 GiB | ≥3 GiB; user confirmed `MemAvailable` governs, not `MemFree` |
| Swap-in / OOM / service restarts | 0 / 0 / 0 | No growth / no OOM / no restarts |
| Closed-5m close-to-evaluation/log delay | unavailable: some kline-close candidates appeared, but no matching recommender evaluation timestamp was logged | Do not infer p50/p99; use CPU p95 comparison |
| Rate-limit evidence | explicit HTTP 429 / HTTP 418 / request-weight classifier: 0 / 0 / 0; one unclassified journal `429` token did not match an HTTP status classifier | No confirmed new HTTP 429/418; keep classifiers identical during stage |

OCI query retries were attempted with 120-second read timeouts and narrowed to one compartment / month-to-date / daily granularity. The Usage query returned an empty result (not a verified $0); the Monitoring CPU query returned `NotAuthorizedOrNotFound`. Billing and seven-day reclaim metrics remain open: user to verify in OCI Console > Billing > Cost Analysis and Monitoring. These are not L0b blockers under the user's directive. The documented L0b egress plan remains ≤45 GB/month against the 10 TB allowance; this is a plan bound, not measured tenancy usage.

Current official documentation snapshot (2026-10-07): Binance USDⓈ-M public endpoint info reports 2,400 request-weight/minute; General Info says IP-shared limits and 429 backoff/418 bans. Oracle's Always Free page says an A1 instance may be reclaimed after a 7-day window only when CPU p95 <20%, network <20%, and memory <20%. Exact active stream constraints and endpoint weights are checked in L0b before deployment. Sources: `https://developers.binance.com/en/docs/products/derivatives-trading-usds-futures/common-definition`, `https://developers.binance.com/en/docs/products/derivatives-trading-usds-futures/general-info`, `https://docs.oracle.com/en-us/iaas/Content/FreeTier/freetier_topic-Always_Free_Resources.htm`.
