# Pump-fade v2 baseline and source conflicts

Recorded 2026-10-07 KST. Repository starting branch: `fix/review-session-c`, HEAD `c06e4ced3005bb8b28f3a75d94ffd25cbfa0c3fd`, clean at inspection. Work is isolated in `.worktrees/pump-fade-v2-20261007`, branch `codex/pump-fade-v2-20261007` from that commit. This is a **local research build**, not the OCI deployed scanner source.

## Authority and verified source split

The authoritative strategy plan was read from `D:\Binance-trade-analysis\handoff\STRATEGY_PLAN_v2_2026-10-07.md` using read-only terminal access. Its SHA256 is **`bb0504050b157c0456fe309731a4ea2954cbbb9a1d2e4773d21edbbab522e986`**, exactly matching the planning snapshot supplied by the user. Supporting roadmap, handoff, prior pump-validation, order-history accounting and OCI closeout documents were also read and hashed. `SOURCE_VERIFICATION_20261007.json` records those post-freeze hashes and the verification chronology; the earlier `freeze_manifest_v2b.json` is intentionally not rewritten.

There are three distinct source identities which must not be conflated:

- **Local remediation/main lineage:** starting source `c06e4ced3005bb8b28f3a75d94ffd25cbfa0c3fd` on `fix/review-session-c`; Pump-fade work is isolated on `codex/pump-fade-v2-20261007`.
- **GitHub main at the Oct-7 closeout:** `00aa27298424ec4ef18114d9270689f03331f1b2`, reported passing CI in the verified roadmap/closeout evidence. It is not the source of this uncommitted worktree.
- **Incumbent OCI shadow release:** `/opt/signalbot/releases/shadow-v2-3f4cabf6`, based on `3395a04449e03c46d108f414d00e36dccb41fb7e` plus separately manifested uncommitted C1/C2/C3 shadow changes. The completed campaign `oci-public-shadow-v2-20261003T051324Z` is terminal and must never be recycled for Pump-fade v2.

## Observed owners and protections

- Existing `signalbot.capture` includes receipt-time stamped websocket/raw REST envelopes, bounded queue and storage, resilient live canary, depth book snapshots, public REST polling for OI, premium and `fundingInfo`. Existing canary plan is frozen; leave it untouched. New pump capture plans must be opt-in and use the existing capture adapter/transport.
- `signalbot.exchange.binance.endpoints` routes USD-M `/market` and `/public` separately. `signalbot.capture.websocket` allowlists streams; v2 additive routes must be tested without loosening the frozen canary.
- `signalbot.prospective` has receipt-aware replay and separate shadow observer; `signalbot.backtest.dataset` validates zipped closed klines and SHA manifests; `signalbot.backtest.guardian_policy` is an existing unrelated frozen guardian experiment.
- Incumbent scanner, `signals/positions.py`, Guardian v2, existing Discord outbox and stopped campaigns remain unchanged. Existing `public-structure-flow-shadow-20261001` worktree is independent and untouched.
- October 6 remediation is already complete in the local lineage and is not redeployed by this task. Do not create a duplicate remediation PR. The OCI closeout explicitly records the old 72-hour campaign as stopped with its `pilot_window_complete` latch preserved.

## Preregistered resolutions and reconciled ambiguities

1. `TAKE` is renamed `FADE-CANDIDATE`: candidate is never an authorized trade. WAIT risk overlays remain research labels, never flip into production LONG.
2. The **nine joint** 20/30/40% drawdown × 5/10/15% rebound cells are a Holm family. Primary descriptive SKIP default is joint 30/10, with prose joint 25/8 and unconditional 40% as separately marked sensitivity arms in the same expanded R3 family. They are **not** interchangeable.
3. Release triggers R1–R5 are independent development hypotheses. A later selected two-trigger combination needs new freeze and a disjoint future evaluation. No selected pair is represented as confirmatory here.
4. Historical P1 outcomes have already been opened; all retrospective P1 here is exposed/development. P3 June–October order/account hypotheses are in-sample. Different market data are not automatically a clean holdout.
5. Most conservative missing-input policy: absent observed funding cap/interval, OI, BBO/depth, receipt timestamps or PIT listing metadata makes eligibility **UNAVAILABLE**; missing evidence cannot silently clear WAIT/SKIP. Liquidation stream silence cannot prove no liquidations.
6. The source strategy specifies D as one-hour quote volume at least two times a causal reference but does not choose the reference estimator. Before the v2 replay outcomes were opened, the preregistration froze **the median of the previous 12 non-overlapping complete one-hour periods**. This is an engineering/research assumption, not an optimized result. It remains unchanged after source-plan verification.
7. The source strategy does not resolve the logical conflict between a 24-hour funding-cap-hit WAIT and an R5 normalization release inside the same 24-hour event TTL. Amendment B, frozen before this v2 historical replay, permits sustained observed normalization to clear the cap-hit WAIT while interval shortening still requires an observed restoration. No post-outcome rule byte was changed.

## Exposure/provenance ledger

| Source / cohort | Verified evidence | Outcome exposure | Authorized use |
|---|---|---|---|
| P1 prior pump-validation | `2026-10-06_pump-validation`: prereg SHA `17779457...`; 9,887 thinned pump points, 548 perps, 506 UTC days; A/B/D already opened | **Exposed** | Development / replication only |
| P2 post-crash | The 17-file 5m replay supplies only retrospective kline landmarks; PIT OI/funding/BBO/listing/execution panel is absent | Exposed retrospective proxy | Kline association only, never causal efficacy |
| P3 private fills/accounting | Verified accounting summary: 2,508 executed orders and 550 reconstructed round trips; heavy-averaging results already opened. Original raw order-history CSV was not found in the analysis archive/expected Downloads path during this continuation | **In-sample June–October** | Aggregate descriptive accounting only; official R2 gate unavailable |
| Existing `data/backtest/futures` | 17 5m gzip datasets and all companion manifests were read and verified by the v2 replay; 744 complete parent opportunities / 85 UTC-day clusters | Exposed retrospective | Descriptive kline proxy only |
| Oct-3 to Oct-6 OCI public shadow archive | 5,878 linked terminal C1/C2/C3 observations; 118 compressed archive chunks; executable BBO/funding outcome coverage absent | Previously observed and unrelated strategy family | Engineering/operational provenance only, not Pump-fade efficacy |
| Forward v2 | Not started and not automatically eligible | Future only after freeze/readiness/eligibility | Independent evaluation subject to gates |

Remaining evidence needed for an empirical gate is not source-document access: it is **prospective point-in-time data** with receipt/gap manifests, executable BBO/depth and funding/OI coverage, plus reconciled P3 risk-budget inputs for R2. Promotion remains prohibited until those data and the independent forward gate exist.

## Post-exposure v2c remediation identity

The later local remediation is recorded separately as `pump-fade-v2-20261007-remediation-c`; it does not mutate the immutable v2b freeze. The verified strategy plan selects a 15% primary adverse-mark squeeze threshold with an inclusive boundary, correcting v2b's conflicting 20% primary. The 20% outcome is only a sensitivity. The v2c chronology clarification permits three spaced post-hit normalization observations to clear a pre-event funding-cap-hit reason only; shortened funding intervals remain WAIT until the fresh observed pre-shortening interval is restored. Historical outcomes were already exposed, so v2c's corrected 744-parent/85-day kline output is development-only, not confirmation. See `PREREGISTRATION_REMEDIATION_C.md` and `OFFLINE_RESULTS_V2C.md`.
