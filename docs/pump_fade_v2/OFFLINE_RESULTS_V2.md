# Pump-fade v2 offline results and data limitations

**Result as of 2026-10-07 KST:** Strategy efficacy is **DATA_INCOMPLETE / UNAVAILABLE**. No net profitability, loss-tail reduction, squeeze avoidance, positive causal effect or statistical significance has been established. The test suite uses synthetic examples for behavioral correctness; synthetic outcomes are excluded from research denominators.

The public 5-minute proxy replay **did complete successfully** against all 17 named USD-M gzip datasets in `D:\Binance bot-2\data\backtest\futures`. Every companion source manifest verified. The replay found **744 complete original parent opportunities across 85 UTC-day clusters**. Its evidence class is deliberately `RETROSPECTIVE_EXPOSED_KLINE_PROXY_NOT_PIT_ELIGIBLE`: historical listing state, point-in-time funding cap/interval changes, OI publication/receipt clocks, forced-liquidation observations, BBO/depth, mark paths and executable fills are not present in this dataset.

The exact output is under `docs/pump_fade_v2/offline_public_kline_proxy_20261007/`: `results.json`, `proxy_events.jsonl` and `data_manifest.json`. The manifest records proxy-event SHA256 `d303aa4be1d3c7cbff5c20b618d6fadb5efd633161387030fe9e80caf69bb9ec` and result SHA256 `f59aa4d7e877b358b8ff0155d6b0de8330f1ad29876ccca4c8f011798620e223`. The immediate next-bar-open 24h **price-only short-return proxy** averaged -92.45 bps; its 10,000-replicate paired UTC-day bootstrap 95% interval is **[-428.82, +241.50] bps**. That is not an executable net return and does not establish positive expectancy.

## Registered research arms and actual statuses

| Family / arm | Input needed beyond synthetic tests | Historical result | Inference |
|---|---|---|---|
| R1: no high 60m | Full PIT WAIT/funding/quality inputs and executable fills are still missing | Kline proxy: 744/744 released; median 95m; proxy squeeze RR 0.971; paired mean delta -30.92bp, 95% [-93.50,+36.75] | **Exposed kline proxy only; official R1 gate unavailable and frozen squeeze gate not met by point estimate** |
| R1: 15m low break / failed retest | Same plus full causal input panel | Kline proxy: 744/744 released; median 170m; squeeze RR 0.912; paired mean delta +13.36bp, 95% [-102.53,+118.76] | Exposed proxy only; official gate unavailable |
| R1: lagged OI drop 5% | OI snapshots including actual publication and local receipt | **0 observed proxy releases** because kline dataset has no OI evidence | UNAVAILABLE; abstentions are retained, not interpreted as successful gating |
| R1: 15m close below event AVWAP | Full PIT WAIT/quality/execution panel | Kline proxy: 738/744 released; median 40m; squeeze RR 0.942; paired mean delta -10.51bp, 95% [-74.22,+85.93] | Exposed proxy only; official gate unavailable |
| R1: predicted funding normalization | Historical `fundingInfo` changes plus predicted-funding receipts | **0 observed proxy releases** because kline dataset has no funding evidence | UNAVAILABLE |
| R2: primary bounded two-add ladder | Reconciled P3 actual risk-increasing fills, initial risk R, stop, quantity, costs and slippage | UNAVAILABLE | No q99 loss or net-noninferiority test |
| R2: first-fill-only comparator | Same reconciled P3 parent population/first fills and executable inputs | UNAVAILABLE | No test |
| R2: eight equal adds +5% from last fill | Same plus full counterfactual price/queue/funding and comparable R | UNAVAILABLE | No test |
| R3: joint 20/30/40% drawdown × 5/10/15% rebound (nine cells) | Full PIT cohort/quality evidence is missing | Four-hour landmark + disjoint 20h **kline associations**: 20/5 RR 2.009 (17 skipped/7 squeeze), 20/10 RR 1.727 (14/5), 20/15 RR 1.601 (9/3); all 30%/40% cells had zero skipped parents | Descriptive exposed association only; official Holm family p-values are unavailable |
| R3: joint 25/8 sensitivity | Same | 3 skipped / 2 post-landmark proxy squeezes, RR 3.208 | Sensitivity only; tiny N, no causal skip-value claim |
| R3: unconditional 40% sensitivity | Same | 0 skipped parents in this proxy cohort | No estimable RR |
| R4: continuation LONG refutation | PIT WAIT inputs, costs and viable long execution | New-high WAIT kline proxy: 571 signals / 173 no-signal; per-parent abstention-zero mean +11.27bp; 95% [-244.76,+270.59] | Refutation proxy is inconclusive; never a live LONG recommendation |
| P2 post-crash cohort | Defined PIT post-crash records with complete exposure provenance | Kline landmark proxy above is measurable; PIT treatment/eligibility is not | Exposed development only |
| P3 private counterfactuals | Original raw order-history export plus frozen I/R and executable cost semantics | Verified prior aggregate accounting exists, but original raw CSV was not found in the analysis archive/expected Downloads location during this continuation | Official R2 counterfactual remains UNAVAILABLE; no private rows copied into repo |

### Outcomes, uncertainty and bias

The replay legitimately measures **price-only** parent/release proxies and their paired day-block intervals. The official mark-path MAE, executable net bps, funding cash flows, ladder 24h loss q99, actual liquidation-distance outcomes, rejected/nonfilled opportunity costs, Holm-adjusted p-values and P1-tail comparison remain **UNAVAILABLE** from an eligible source panel. No IID fallback confidence claim is substituted. Exposed P1 and in-sample June–October P3 evidence cannot be promoted as untouched confirmation. A present-day funding-cap response cannot be projected backward as a historical rule input. Public `!forceOrder@arr` is censored, so unseen forced liquidations remain unknown rather than zero.

The runner separates directional price proxy returns, future mark-risk labels and executable-cost evidence. A next-bar open fill is a *hypothetical proxy*, not a proven executable price; funding cash flows, spread/impact and maker queue positions remain missing. It never reports a positive net expectation from its kline proxy.

### Reproduction command used successfully

From `D:\Binance bot-2\.worktrees\pump-fade-v2-20261007`, using the verified Windows Python 3.12 environment, preserve the worktree source path and frozen environment:

```powershell
$env:UV_PROJECT_ENVIRONMENT = 'D:\Binance bot-2\.venv-codex'
$env:PYTHONPATH = (Join-Path (Get-Location) 'src')
& 'D:\Binance bot-2\.venv-codex\Scripts\python.exe' -m signalbot.pump_fade_v2.replay --data-dir 'D:\Binance bot-2\data\backtest\futures' --policy 'config\pump_fade_v2\policy_v2b.json' --output-dir 'docs\pump_fade_v2\offline_public_kline_proxy_20261007'
```

The worktree `PYTHONPATH` was set to `src` before this command. Exit status was 0. Inspect the exact generated files rather than copying these rounded numbers into another analysis. Never re-label this exposed retrospective result as point-in-time or forward evidence.
