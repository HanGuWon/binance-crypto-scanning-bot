# OCI collector-fix A/B stage receipt — 2026-10-10

## Immutable candidate

- Host: `trendguard-osaka-a1` via `trendguard-osaka-a1` SSH alias. Verify hostname, A1 shape, and known_hosts fingerprint before any OCI change.
- Collector source SHA: `3185186378f0ab6a8b6ab272d92beec349d9fd70`.
- Uploader and Drive-auth helper source SHA: `2cb1b676b75c07d40771ee1e4ab761363b9c0d9d` (installed at `/opt/pump-fade-v3-uploader`; separate unit and timer are present). The uploader service is inactive/static and its timer is inactive/disabled. Do not enable the timer until the collector is enabled and one manual upload is verified.
- Independent review: passed on this exact SHA. Review confirmed retry attempts are bucketed by attempt-start minute, used-weight headers by response-completion minute, the six-generation restart boundary, and decimal-GB retention arithmetic. Review note: systemd's six-start rate limit may deny a later start before the app's CRITICAL threshold executes; monitor failed state and restart count as required.
- Full verification: `4,830 passed, 37 skipped` with `TEMP`, `TMP`, and `--basetemp` under `D:\Binance-trade-analysis\2026-10-08_pump-v3\l3\scratch\collector-fix`.
- Gate amendment: the prior absolute `22.77%` recommender CPU p95 ceiling is withdrawn. Use four 30-minute blocks `OFF/ON/OFF/ON`; pooled ON-minus-OFF recommender CPU p95 <= `+5 pp`; collector CPU p95 <= `10%` of one core; pooled CPU PSI `some avg10` p95 delta <= `+5 pp`; retain all absolute gates in `LUNA_COLLECTOR_RESTAGE_2026-10-10.md`. A single ON block recommender CPU p95 > `60%` or any absolute-gate breach is an immediate collector-only stop.

## Preflight and stage observations

Host identity was confirmed before changes over `trendguard-osaka-a1` with strict host-key checking. Known-hosts ED25519 fingerprint: `<redacted>`; hostname `trendguard-osaka-a1`, Ubuntu 24.04, aarch64 Neoverse-N1, 2 OCPU. `signalbot.service` was active with `NRestarts=0` before and after staging.

DATA mount: `/dev/sdb` ext4 at `/var/lib/binance-bot-2/prospective-r`. Preflight free bytes `33,521,500,160` = `31.25 GiB`; effective budget `min(16, 31.25 - 12) = 16 GiB`. Preflight MemAvailable `11,133,444 kB` (~10.62 GiB), load `0.26`. Initial collector data was present and preserved; its measured directory size at the start of the post-stage measurement was `1,279,292,570` bytes. At that snapshot DATA had `33,500,397,568` bytes free, so the effective budget remained `16 GiB`.

The archive was generated with `git archive 3185186378f0ab6a8b6ab272d92beec349d9fd70`; OCI archive SHA-256 `5DC68CF2C397CE6C3EFC73E5F45370C5D7F83FB979E55B5E29CE27EFDE443293`. `/opt/pump-fade-v3` contains that exact commit. The previous tree is preserved at `/opt/pump-fade-v3.pre-3185186378f0ab6a8b6ab272d92beec349d9fd70`. Frozen no-dev sync passed. The default uv-managed interpreter initially resolved under `/root/.local/share/uv` and failed with systemd `203/EXEC` under `ProtectHome=true`; collector alone was stopped, no restart/capture occurred, and its venv was rebuilt using Python 3.12.13 installed within `/opt/pump-fade-v3/.python`. `pumpfadev3` successfully imported the collector from the rebuilt venv. The original root-managed venv and failed system-Python attempt are preserved under `/opt/pump-fade-v3/.venv.root-managed-backup` and `.venv.systempython-attempt`.

Installed unit passed `systemd-analyze verify` (only unrelated existing monitoring-agent executable-bit warnings). Observed limits: `MemoryMax=2147483648`, `CPUQuotaPerSecUSec=600ms` (60%), `Nice=10`, `IOWeight=50`, and `ReadWritePaths=/var/lib/binance-bot-2/prospective-r/pump-fade-v3`. Unit remained `static` and was not enabled. The uploader timer remained `disabled`.

Drive-auth helper is present at `/usr/local/libexec/install_pump_fade_v3_rclone_config` (`root:root`, mode `0755`); `rclone v1.60.1-DEV` is installed. `pumpfadev3` is non-login (`/usr/sbin/nologin`), and `/etc/pump-fade-v3` is `root:root`, mode `0755`. No auth configuration was read or displayed.

Four blocks completed in order OFF1 / ON2 / OFF3 / ON4, with 30 minute-spaced samples per block. Per-minute JSONL samples for OFF3, ON2, and ON4 are in the local untracked `.pytest-tmp` directory; OFF1 samples were captured in the session transcript, excluding its initial sub-minute CPU sample and including a one-minute endpoint sample. Pooled percentiles use nearest-rank p95 and median p50.

Context-switch means and run-queue/CPU-steal extrema use 59 eligible OFF samples because OFF1's initial sub-minute sample was excluded; CPU/PSI percentiles use 60 pooled samples for each condition.

| Gate / metric | OFF pooled | ON pooled | Delta / worst | Result |
|---|---:|---:|---:|---|
| Recommender normalized CPU p50 | 20.122% | 17.554% | — | — |
| Recommender normalized CPU p95 | 23.867% | 20.117% | -3.750 pp | PASS (<= +5 pp) |
| CPU PSI `some avg10` p95 | 0.32% | 0.13% | -0.19 pp | PASS (<= +5 pp) |
| Collector CPU p95, one core | — | 0.817% | — | PASS (<= 10%) |
| Collector RSS peak | — | 130,524 kB (~127.5 MiB) | — | recorded |
| Recommender voluntary context switches, mean/minute | 34,972 | 33,189 | — | recorded |
| Recommender involuntary context switches, mean/minute | 163 | 167 | — | recorded |
| Run queue maximum | 4 | 5 | — | recorded |
| CPU steal delta maximum, ticks/minute | 314 | 297 | — | recorded |
| Load 1m maximum | 0.80 | 0.91 | — | PASS (<= 1.6) |
| MemAvailable minimum | 11,112,208 kB (~10.60 GiB) | 11,020,056 kB (~10.51 GiB) | — | PASS (>= 3 GiB) |
| Memory PSI `some avg10` maximum | 0% | 0% | — | PASS (< 5%) |
| Swap-in / OOM growth | 0 / 0 | 0 / 0 | 0 / 0 | PASS |
| `signalbot.service` / collector restarts | 0 / 0 | 0 / 0 | 0 / 0 | PASS |
| New classified HTTP 418 / 429 | 0 / 0 | 0 / 0 | 0 / 0 | PASS |
| Collector overflow | none | none | — | PASS |

ON2 wrote `5,681,994` additional bytes to the collector data tree in its block; ON4 wrote `6,249,580` bytes. CPU PSI had transient OFF spikes (18.7% in OFF3) as well as an ON spike (15.6% in ON4); the paired p95 gate passed. The REST summary `INFO` lines were absent from journald (Python's default logging level does not emit `INFO`), so I measured them from hash-verified closed capture segments without changing collector code. ON2: 189 clean generations / 195 verified segments, 205 REST attempts, total request weight 437, peak 9 attempts and 16 weight in any start-minute, latest completion-minute `X-MBX-USED-WEIGHT-1M=1` (peak observed header 57), and captured HTTP 418/429 = 0/0. ON4: 189 clean generations / 195 verified segments, 237 REST attempts, total request weight 491, peak 9 attempts and 17 weight in any start-minute, latest completion-minute header `56` (peak observed header 58), captured HTTP 418/429 = 0/0. `signalbot.service` journal classification remained 0/0 in all samples.

The ON4 generation has remained active since `2026-10-10T16:22:54Z`; the two-hour A/B stage passed at `2026-10-10T16:53:41Z`. The dedicated 24-hour monitor began at `2026-10-10T17:06:31Z` with collector directory `1,280,214,671` bytes, recommender and collector active, and both restart counts zero. It samples absolute gates once per minute, reports hourly aggregates, stops only the collector on a hard-gate breach, and is scheduled to close the collector cleanly at `2026-10-11T17:06:31Z`. Local monitor session ID: `82790`.

Interim only: at `2026-10-10T17:13:17Z`, collector data was `1,281,730,511` bytes (`+1,515,840` bytes over 6m46s, a simple extrapolation of `0.323` decimal GB/day). This is not the 24-hour rate. Generations `g0296`–`g0302` had `CLEAN_CLOSED` terminal receipts and `g0303` was open; these are plan-change generations, while both systemd restart counts remained zero.

## Measurement, disposition, and follow-up

24-hour per-stream bytes/day, aggregate decimal GB/day, admissions/evictions, gaps, final clean terminal receipt, depth rule, retention `L`, enable status, first clean-generation timestamp, and uploader manual verification remain pending. The uploader timer must remain disabled until collector enable and one manual upload is verified.

Rollback script: `/usr/local/sbin/rollback-pump-fade-v3`; its dry run was previously verified clean. It stops/removes only the collector unit and preserves collector data. Do not alter `signalbot.service`, its DB/env/campaign data, or uploader timer.
