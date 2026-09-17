# Prospective Campaign Operations Runbook (causal_retest_v1, Phase H/I)

Status: SHADOW_SUCCESSOR_ONLY. This runbook describes operating the
prospective shadow evidence collector alongside frozen production R2.
It never authorizes order placement, promotion, or threshold changes.

QUALIFICATION STATUS: the Phase-H qualifying smoke
(smoke-causal-retest-phase-h-final-20260823-v1) PASSED with
futures 34 / spot 22 consecutive 5m cells (>= 18 required each),
OPEN cells after stop = 0, raw tape malformed = 0, market mismatch = 0,
backwards receipt clocks = 0, denominator exact, and INFRA_SMOKE = PASS.
Scientific status stays SHADOW_SUCCESSOR_ONLY: nothing above is a claim of
profitability, validated alpha, or validated execution. Operational status is
READY_FOR_CONTINUOUS_OBSERVATION +
EXECUTION_PARITY_PENDING_FORWARD_EVIDENCE; all execution-parity axes remain
INCONCLUSIVE until real forward evidence exists.

## 0. Preconditions

- Windows or Linux host, Python 3.12, uv environment installed.
- Disk headroom >= raw tape projection (see section 6) plus DB growth.
- Discord remains disabled for initial prospective commissioning
  (alerts.discord_enabled: false); no webhook secret is required for the
  research collector.
- OCI receipt tools read connection details from the environment. Set
  `BINANCE_BOT_OCI_HOST`, `BINANCE_BOT_OCI_SSH_KEY`, and
  `BINANCE_BOT_OCI_FILESYSTEM_UUID` before running them. The volume receipt
  additionally requires the `BINANCE_BOT_OCI_*` compartment, instance,
  volume, device, and expected service-state values named in
  `tools/phase_r_volume_receipt.py`. Keep those deployment values outside
  source control.

## 1. Preregistration (one-time)

1. Freeze source and record identity:

       python -c "from signalbot.prospective.source_freeze import default_source_root, freeze_source; print(freeze_source(default_source_root()).source_identity)"

2. Fill shadow.source_identity, shadow.campaign_id
   (e.g. causal-retest-prospective-phase-i-YYYYMMDD-v1),
   shadow.campaign_created_at_ms (actual registration time) in a
   deployment copy of config/prospective.causal-retest.phase-h.yaml.
   The deployment copy must set shadow.observation_enabled: true together
   with retest_observation_enabled: true (the Settings validator rejects
   retest observation without the base observation switch).
3. Validate:

       signalbot validate-config --config <deploy-config.yaml>
       signalbot run --config <deploy-config.yaml> --dry-run

## 2. Preflight

- Confirm DB path is dedicated to this campaign (no prior rows).
- Confirm raw-event directory is empty/dedicated.
- Confirm disk free >= projected 7-day tape size x safety margin (>=2x).
- Record config SHA256 and source identity into the campaign worklog.

## 3. Start

    signalbot run --config <deploy-config.yaml>

(Linux: use systemd with Restart=on-failure; Windows: Task Scheduler or
a service wrapper. Keep stdout/stderr captured to rotating log files.)

## 4. Graceful stop

POSIX: send SIGINT/SIGTERM. Windows bounded qualification runs: use the
internal --stop-after-minutes flag, which sets the application stop event
and runs the full graceful path (scanner.close, shadow flush, recorder
drain, DB close).

Do NOT rely on Windows console CTRL_BREAK: a prior attempt failed to run
graceful flush after console detach and left non-terminal lifecycles.
For long-running Windows deployment require a tested process/service owner
that sets the application stop event and VERIFIES scanner.close,
shadow flush, recorder drain, and DB close. Never claim graceful shutdown
until that owner is tested.
A graceful stop:

- seals open coverage cells,
- censors unresolved retest lifecycles as CENSORED(CAMPAIGN_SHUTDOWN).

Bounded runs for testing may also use --stop-after-minutes N directly.

## 5. Restart semantics

On restart with the same campaign, any active retest lifecycle whose bar
continuity cannot be proven is censored as CENSORED(RESTART_GAP). This is
by design; do not weaken it. Prove continuity only from durable transition
clocks covering every intervening completed primary bar.

## 6. Storage plan

Measure actual tape rate from the qualified smoke (bytes/hour per market),
then project 24h / 7d / 30d. The default 10 GiB raw quota suits smoke runs,
NOT necessarily a 30-day prospective campaign. Before activation either
raise the quota on a large volume or plan lossless daily compression
(original content hash retained in an immutable manifest).
Do not delete prospective evidence.

## 7. Daily health audit (non-promoting)

Daily, record:

- process alive; log tail freshness (<5 min old);
- source identity unchanged (recompute freeze_source);
- last sealed coverage close per market;
- OPEN cell count (must be 0 outside an active window);
- evidence failures count;
- raw-C0 count; lifecycle stage counts; active unresolved count;
- denominator missing/unexpected (tools/audit_phase_f_final_smoke.py);
- raw tape bytes vs quota.

Any anomaly goes to the human operator. No automatic tuning exists.

## 8. Campaign shutdown

Graceful stop, then run the forensic auditor over the final DB + tape and
archive: config, DB, logs, tape hashes, auditor JSON, source freeze JSON.

## 9. Coverage gap detection

Daily (and at shutdown) run the read-only continuity report:

    python tools/campaign_continuity_report.py --db <campaign-db>

Missing expected 5m decision-close windows are explicit evidence gaps.
They are never back-filled and no opportunities are manufactured for
them. Repeated gaps in one market indicate an infrastructure defect.

## 10. Raw tape archival

Raw tape growth must be re-measured from each qualifying smoke rather than
reused from older phases. Phase-H measured rates (2,904,028,623 futures bytes
+ 1,429,635,138 spot bytes over its receipt-clock span) are substantially
higher than the older Phase-G projections; compute bytes/hour per market from
the actual first/last receipt clocks in var/evidence/
phase-h-post-smoke-audit-frozen-v1.json and size quotas from those numbers.
gzip ratio measured 0.119 on a 64 MiB futures sample with verified lossless
round-trip; use it only as a planning estimate, not a guarantee per day.

For closed days only:
1. hash the original JSONL (SHA256),
2. gzip it,
3. decompress-verify the restored SHA equals the original,
4. record original/compressed bytes, ratio, timestamps, hashes in an
   immutable manifest next to the archive.

Never delete originals automatically. Compressed projections:
~2 GiB/day, ~14 GiB/week, ~61 GiB/month, ~184 GiB/90d. OCI disk
recommendation: >=60 GB free for a 30-day campaign WITH daily
compression; >=550 GB if raw retention is required instead.

## 11. Hard rules

- Never modify scientific parameters during an active campaign.
- Parameter change => new policy SHA => new freeze => new campaign ID.
- Smoke data never enters the prospective denominator.
- No automatic trading path exists or may be added under this runbook.
