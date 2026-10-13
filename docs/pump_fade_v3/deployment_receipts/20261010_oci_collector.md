# OCI deployment receipt — Pump Fade v3 — 2026-10-10

## Immutable deployment identities

- Host: `trendguard-osaka-a1` (`VM.Standard.A1.Flex`, A1 2 OCPU / 12 GB); known_hosts ED25519 fingerprint: `<redacted>`.
- Collector source SHA: `3fe2481ea07a4b38e2768d647747e75bfe77bdbe` (`3fe2481`). Archive SHA-256: `d6c1c728ca87baa7ae39b7fec22fd3ad35c495359f994e3b29c6a461c6e69965`.
- Uploader source SHA: `2cb1b676b75c07d40771ee1e4ab761363b9c0d9d` (`2cb1b67`). Uploader archive SHA-256: `721b3143278771daffa26178dd07a18b46353188f3cf650341f087ed887ca6061`.
- Collector protected-path comparison from `3fe2481` to uploader commit passed with no differences.

## Preflight and storage

- `findmnt -T /var/lib/binance-bot-2/prospective-r`: `/dev/sdb`, ext4, DATA filesystem.
- Fresh pre-stage free space: `33,643,229,184` bytes (31.33 GiB). Effective storage budget: `min(16, 31.33 - 12) = 16 GiB`; the 12 GiB free-space floor was satisfied.
- `signalbot.service` was active with `NRestarts=0`; it remains active with `NRestarts=0` after rollback.
- `pumpfadev3` is a non-login account (`/nonexistent`, `/usr/sbin/nologin`).

## Reversible deployment and rollback

- Rollback script: `/usr/local/sbin/rollback-pump-fade-v3`; SHA-256 `2fdebc8fa16ac9c419999429c12fc705da48662f982af4c73e688aed11d675d6`.
- Dry-run passed before staging. Execute command: `sudo -n /usr/local/sbin/rollback-pump-fade-v3 --execute`.
- The first start attempt failed with `203/EXEC` because the venv Python symlink pointed into root-only `/root/.local`. Rebuilt the collector venv with accessible managed Python 3.12.13 under `/opt/pump-fade-v3/python-runtime`; verified import as `pumpfadev3`, then reinstalled and verified the collector unit. No recommender changes were made.

## Staging result

- Fresh baseline immediately before the successful start: `2026-10-10T04:11:05Z`; recommender CPU p50 `16.80%`, p95 `34.38%`. The stricter limit remained the A3 threshold, `22.77%`.
- Collector ran from `2026-10-10T04:41:22Z` to rollback at approximately `2026-10-10T05:05:48Z`; 24 minute samples were recorded.
- Stopped at sample 23 when rolling recommender CPU p95 reached `25.75%`, above `22.77%`. The partial 24-sample p50/p95 were `16.07%` / `33.37%`.
- Other observed gates passed: maximum load `0.94`; minimum MemAvailable `10.52 GiB`; maximum memory PSI some avg10 `0.0%`; swap-in delta `0`; OOM delta `0`; recommender restarts `0`; classified HTTP 418/429 `0`.
- Collector peak RSS `112.81 MiB`; CPU p50 `1.69%`, maximum `2.61%`; bytes written during the partial stage `52,980,893` (52.98 MB decimal). No REST used-weight header values appeared in the monitored journal samples.
- Rollback stopped only the collector, removed only its unit/drop-in, and preserved code/data. Collector unit is not installed after rollback. The DATA directory remains at about `1.2 GB`.
- The 2-hour stage did not pass. The 24-hour measurement and collector enable were therefore not performed. No 24-hour stream MB/day or retention-days figure is available.

## Uploader deployment

- Uploader code: `/opt/pump-fade-v3-uploader`; separate service and timer units installed and verified with `systemd-analyze verify`.
- `rclone` present from Ubuntu package: `1.60.1+dfsg-3ubuntu0.24.04.6`.
- `pump-fade-v3-uploader.timer` is disabled pending user-run Drive authorization. No token was created/read by this deployment. No uploader-triggered deletion has occurred; prune remains disabled.
- PC pull-script dry-run was attempted and failed because the SSH user `ubuntu` cannot traverse the private collector capture directory. No execute copy was performed. A temporary read-only ACL request was rejected by automatic review; the user later withdrew the request. No ACL change was applied.
- Authorization script is installed at `D:\Binance-trade-analysis\setup\3_authorize-drive-for-oci.ps1`, SHA-256 `60B146030C06BB9584CBEA01844501D7817AA97AED8D0AE5D3147A2EEBC60EA7`; repository commit `ed2f8f63aae2929dab88f541f9d1449e45e1eef4`.

## Open items

- User: check OCI Console cost.
- After safe archival becomes available: collect seven-day reclaim telemetry.
- Forward-evaluation start timestamp is defined as the first clean generation after collector enable; not set because enable did not occur.

## Gate amendment — A/B re-stage (2026-10-10)

The former absolute recommender CPU ceiling of `22.77%` is withdrawn. It was a reviewer gate-design error: the fresh OFF baseline had p95 `34.38%`, demonstrating that the fixed ceiling reflected recommender load variation rather than collector impact. The partial prior ON stage had recommender p95 `33.37%` (−1.0 pp vs that OFF baseline) and collector CPU p50 `1.69%`, max `2.61%`, RSS about `113 MiB`.

Re-stage protocol authorized by the user: four interleaved 30-minute blocks `OFF/ON/OFF/ON`, sampled each minute using the A3 metric definitions. Pass requires pooled ON-minus-OFF recommender CPU p95 ≤ `+5 pp`, collector CPU p95 ≤ `10%` of one core, and ON-minus-OFF CPU PSI `some avg10` p95 ≤ `+5 pp`. Absolute gates remain `MemAvailable ≥3 GiB`, memory PSI `some avg10 <5%`, load ≤`1.6`, no swap-in/OOM growth, no recommender restart, and no new classified HTTP 418/429. Hard stop on any absolute-gate breach or if a single ON block recommender CPU p95 exceeds `60%` normalized. The 2026-10-08 baseline remains comparison context; it is not an absolute ceiling.

The DATA filesystem effective budget is recalculated before this re-stage as `min(16, F − 12) GiB`.

## Revised interleaved A/B re-stage — aborted FAIL (2026-10-10)

Pre-stage identity was reverified before changes: `trendguard-osaka-a1`, `VM.Standard.A1.Flex`, 2 OCPU / 12 GB, Ubuntu 24.04.4 LTS, host key ED25519 fingerprint matched known_hosts. `/opt/pump-fade-v3/DEPLOYED_COMMIT` reported `3fe2481ea07a4b38e2768d647747e75bfe77bdbe`. The existing managed Python 3.12 venv remained executable. Effective unit caps verified with systemd: `MemoryMax=2G`, `CPUQuota=60%`, `Nice=10`, `IOWeight=50`, `User=pumpfadev3`, `ReadWritePaths=/var/lib/binance-bot-2/prospective-r/pump-fade-v3`; unit was `static` and inactive before the run. `systemd-analyze verify` succeeded (unrelated existing OCI monitoring unit permission warnings only). Rollback dry-run after unit installation showed it would remove only the collector unit and preserve data.

Fresh pre-stage DATA state: `/dev/sdb` ext4 mounted at `/var/lib/binance-bot-2/prospective-r`, free `33,588,109,312` bytes (`31.28 GiB`), so effective budget `min(16, 31.28 - 12) = 16 GiB`. `MemAvailable=11,133,164 kB`, load `0.09`, `signalbot.service=active`, `NRestarts=0`.

| Metric | OFF block 1 (30 samples) | ON block 2 partial (19 samples) | Comparison / status |
|---|---:|---:|---|
| Recommender CPU p50 / p95, normalized to 2 OCPU | 16.71% / 19.14% | 17.54% / 22.04% | Partial p95 delta `+2.91 pp`; ON block incomplete |
| CPU PSI `some avg10` p95 | 0.73% | 5.79% | Partial delta `+5.06 pp`, above the `+5 pp` comparison gate; incomplete pooled A/B |
| Recommender voluntary context switches p50 / p95 per min | 31,258 / 36,101 | 35,108 / 42,649 | Recorded |
| Recommender involuntary context switches p50 / p95 per min | 279 / 1,591 | 890 / 1,544 | Recorded |
| Run queue max runnable / load max | 5 / 0.88 | 6 / 0.61 | Pass (`load ≤1.6`) |
| CPU steal max | 5.69% | 2.44% | Recorded |
| Memory PSI `some avg10` max | 0.00% | 0.00% | Pass (`<5%`) |
| I/O PSI `some avg10` p95 | 3.18% | 0.06% | Recorded |
| MemAvailable minimum | 10.53 GiB | 10.53 GiB | Pass (`≥3 GiB`) |
| Swap-in / OOM growth | 0 / 0 | 0 / 0 | Pass |
| Recommender restarts | 0 | 0 | Pass |
| Classified HTTP 418 / 429 (both units) | 0 / 0 | 0 / 0 | Pass |
| Collector CPU p50 / p95, one core | — | 4.32% / 7.53% | Partial p95 below 10%; incomplete block |
| Collector RSS p50 / max | — | 95.34 / 107.96 MiB | Recorded |

The collector failed at `2026-10-10T07:25:00Z` during ON block 2 (19/30), with `signalbot.capture.errors.CaptureQueueOverflow: capture handoff exceeded its event or encoded-byte bound`. The same failure had appeared in the prior partial run, so this is a reproduced exact-commit blocker. The A/B protocol did not reach OFF/ON blocks 3–4; the formal pooled comparison is incomplete and the stage is **FAIL**, not PASS. No 24-hour measurement, depth change, collector enable, or forward-evaluation start was performed.

The external reader parsed 102 finalized/partial segment files from the current boot without a torn tail. It found 145 REST envelopes over the observed ON minutes (2–10 per minute), zero captured HTTP 418/429, maximum observed `X-MBX-USED-WEIGHT-1M` value 59, and last observed value 17 at `2026-10-10T07:24Z`. The collector itself does not emit the requested one-minute REST summary; these values were measured offline from its persisted allowlisted response headers without modifying collector code. The stopped generation `g0099` has a start receipt but no terminal receipt, so it is incomplete and not archive/prune eligible. Earlier generations `g0096`–`g0098` have clean terminal receipts.

The partial ON run persisted `64,927,356` bytes in about 18.8 minutes. A rough extrapolation is `4.98 GB/day` decimal and about `3.45 days` to consume the 16 GiB budget, but this is startup/partial-run data, not a valid 24-hour rate; no policy retention `L` is set. No depth setting was changed. Collector-only rollback completed at `2026-10-10T07:25Z`: the unit is absent, code and data are preserved, and a subsequent dry-run reports no active collector/unit to remove. Post-rollback `signalbot.service` remained active with `NRestarts=0`; DATA remains on `/dev/sdb` with `33,521,500,160` bytes free and the collector data directory at `1,265,155,007` bytes.

Uploader `2cb1b676b75c07d40771ee1e4ab761363b9c0d9d` and the root-owned Drive auth helper remain installed; no installer or token was run/created. The uploader timer remains disabled, pending user auth, collector enable, and a verified manual upload. The Windows pull-script dry-run from the earlier attempt failed on `ubuntu` directory traversal; no execute copy or ACL change occurred. The fixed reviewed PC auth script is `D:\Binance-trade-analysis\setup\3_authorize-drive-for-oci.ps1` (SHA-256 recorded in the uploader/auth section above).

## Current disposition

- Collector: rolled back; unit absent; data retained; exact deployed source remains `3fe2481`.
- A/B: failed due reproducible `CaptureQueueOverflow`; no 24-hour phase or enable.
- Uploader: installed, timer disabled; **READY for user auth**.
- Open: OCI Console cost check (user); seven-day reclaim telemetry; forward-evaluation start timestamp (first clean generation after enable, still unset).
- Further collection requires a collector fix and a new separately reviewed SHA; deploying a modified tree under the `3fe2481` identity is not acceptable.


