# Pump Fade v3 collector overflow diagnosis receipt — 2026-10-10

## Input and integrity

- Baseline implementation: collector commit `8984e6a`, source tree `3fe2481`.
- Offline input: the four approved OCI generations g0096–g0099, boot `7c01bc11870d436fabd376427c6502b9`.
- PC copy: `D:\Binance-trade-analysis\oci-capture\diag-20261010`.
- Transfer receipt: `transfer-receipt.json`; all 15 files passed per-file SHA-256 comparison. Actual size was 5,472,258 bytes (about 5.22 MiB), below the earlier 1.27 GB estimate.
- Captured record span: 2026-10-10 07:23:50.109–07:24:24.164 UTC. It does not include the reported 07:25:00 UTC overflow instant; the replay diagnoses recorded load, not that exact second.
- The copy was only replayed locally. No capture data was uploaded or added to Git.

## Replay findings

At 1x with the original 2,048-event / 32 MiB handoff, replay reached 2,047 queued events (99.95%) but only 2,362,614 bytes (7.04%) and failed at g0099, ingest sequence 2437, on `magicusdt@bookTicker`. Consumer p50/p99/max latency was 676.5/1,724.0/1,772.4 ms. Event-loop lag max was 31 ms, and there were no measured synchronous calls over 100 ms.

The recorded stream profile contained 6,032 `magicusdt@bookTicker` messages (177.8/s average, 1,414 messages in its peak one-second bucket, 2,029 in a rolling ten-second window). Across all symbols, `@bookTicker` supplied 7,964 of 8,149 records. This supports input event rate exceeding consumer throughput (hypothesis a); it does not support a periodic event-loop writer stall (hypothesis b). The writer already calls `append` and `close` through `asyncio.to_thread`, as the user noted.

With only `@bookTicker` records omitted and the remaining records fed through the same handoff and consumer, both 1x and 3x replays passed. At 3x, queue high-water was 9/2,048 events (0.44%) and 233,504/33,554,432 bytes (0.70%); consumer record latency p50/p99/max was 1.88/8.27/10.12 ms, event-loop lag max was 28 ms, and there were no calls over 100 ms.

The replay wrote 141,612 compressed bytes over the 34.055-second captured span without `@bookTicker`. Linear extrapolation is approximately **0.359 decimal GB/day**. This short trace implies a large reduction against the previous approximately 5 GB/day partial-run estimate, but it is not a 24-hour measurement and must not replace the post-stage measurement. Since 16 GiB is approximately 17.18 decimal GB, the theoretical cap-only estimate is about **47.9 days** at this rate; the DATA free-space floor and other retained files can shorten it.

## Change and verification

- Removed event-symbol `@bookTicker`; event-symbol plans retain `@markPrice@1s` and optional `@depth5@500ms` for best bid/ask and sizes.
- The minute-level `!miniTicker@arr` admission gate already drops unselected frames before `pipeline.offer`; no code change was needed.
- The writer already runs via `asyncio.to_thread`; no writer architecture change was needed.
- Added collector-only `Restart=on-failure`, `RestartSec=30`, one-hour start limit of six, restart-gap counting with CRITICAL logging at six incomplete generations, and a one-minute REST request/weight/last-header summary. Each request attempt is bucketed by its attempt-start minute and its used-weight header by response-completion minute.
- Updated the stream contract, systemd assertions, queue-plan test, restart-gap window test, REST summary log test, and PowerShell authorization stubs for the current PC script.
- The first independent review of commit `885d9f6d8611874b5cb832560d096f97bc30f6eb` held staging for minute-boundary request accounting, the restart threshold edge, and a GiB/GB conversion error. The fixes bucket each attempt from its monotonic offset from the poll's UTC timestamp, bucket used-weight headers using the response completion timestamp, stop on six incomplete generations, and convert 16 GiB to decimal GB for the retention estimate. Added retry-boundary/header and restart-threshold tests.
- Final full suite after those fixes with TEMP, TMP, and `--basetemp` under `D:\Binance-trade-analysis\2026-10-08_pump-v3\l3\scratch\collector-fix`: **4,830 passed, 37 skipped** in 1,873.40 seconds. The skips are host/platform constraints. The earlier full run's Windows fatal access violation did not recur in the completed run.
- Final checks: `rtk ruff check .` passed; `compileall` passed; targeted Pyright for all changed Python modules/tests and the replay harness passed with 0 errors, 0 warnings, and 0 informations; `git diff --check` passed. The PowerShell auth script SHA-256 matches the current PC source (`01FDAE8F5C1CF119745AE7814AC6D355259F860F300327B421A242D2C3D06088`) and its AST parses cleanly.

## Deployment gate

This receipt documents an offline fix only. The approved stage gate is the interleaved A/B amendment in the current runbook and user-approved restage directive; the earlier 22.77% absolute ceiling is withdrawn. The corrected commit SHA must receive a new independent review before deployment. Then rerun the approved interleaved OCI A/B stage before continuing to the 24-hour measurement and collector enable. Keep the uploader timer disabled until the collector is enabled and one manual upload is verified.
