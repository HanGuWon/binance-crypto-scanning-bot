# Guardian policy historical evaluation

L60-03 implements the identical-entry historical exit-policy harness bound to
the frozen L60-02 policy-selection contract. It does not select a policy and it
does not place, amend, or cancel exchange orders.

## Frozen execution authority

Historical execution is bound to the independently reviewed L60-02 v2 contract
at semantic SHA-256
`23140ebd342ccf5e2b6c1ba9a6f8b180ece420cf8277db7fe944c0b3790fefb7`.
Its frozen parent is v1 at
`2e19fd7a597dd78a0372753da50fd54dcfacceea6e9482bb34aac606c507a923`.
The v2 receipt records that no real Guardian policy outcome was computed before
the amendment was reviewed and pinned.

Before historical market data are read, `backtest-guardian-policy` now fails
closed unless all of these authorities match the preregistration:

- the exact v2 semantic contract identity and v1 parent provenance;
- every LF-canonical bound source identity;
- the clean Git/LF identities of the frozen entry/feature producer sources at
  commit `2bb0d1eb075a6cf34d094cacfd902abb939d7bf1`;
- exact config path `config/settings.example.yaml` and its LF-canonical hash;
- the canonical parsed `Settings` model hash; and
- equality between the caller-supplied settings object and the freshly loaded
  frozen settings object.

The bound historical weakening authority is rule ID
`technical_exit_one_bar_trend_failure_v1`, which maps exactly to the existing
causal `TechnicalExitEngine` one-bar trend-failure predicate:

- LONG: `feature.price < feature.ema20 and feature.macd_histogram < 0`
- SHORT: `feature.price > feature.ema20 and feature.macd_histogram > 0`

No trend-failure-count threshold or opposite-signal condition is added. The
same fully closed `FeatureSnapshot` supplies structure and weakening inputs; if
that close-time context is unavailable, no stop update is created.

## Historical entry authority

The harness authenticates the v2 authority and then loads the frozen
`config/backtest.5m.r2-c0-corrected.yaml` and changes exactly one
`BacktestSpec` field: `direction_scope=futures_bidirectional`.

The existing `ResearchBacktester` is run once per USDⓈ-M Futures symbol. Its
baseline-generated executable trades are used only to identify entry rows.
Each source trade is reduced to a `GuardianEntryRow` containing entry-time
facts only; source exit time, exit price, PnL, MFE, MAE, exit reason, and source
trade ID do not participate in the Guardian position identity.

The research quantity is frozen once as
`source_spec_notional_usdt / raw_entry_price` and is shared by all four policy
episodes. Returns remain normalized in basis points, so quantity is provenance
rather than a policy discriminator.

After the entry cohort is frozen, every entry is replayed independently under
all four policies. A shorter or longer candidate episode can never suppress or
create another entry.

## Frozen policy adapters

`initial_stop_only_v1` keeps the cohort-entry active stop and creates no later
amendments.

`delayed_atr_trail_v1` calls the existing `ProtectiveStopPlanner` with the
frozen 1R activation, 2 ATR trail, 5 bp reference-price gap, and 1 bp minimum
improvement. It supplies no structure or weakening input.

`confirmed_swing_atr_trail_v1` first requires the ordinary ATR/R candidate from
`calculate_trailing_stop_candidate`. Confirmed structure cannot activate the
trail by itself. Once an ATR candidate exists, LONG uses the more protective of
the ATR candidate and confirmed swing support, and SHORT uses the more
protective of the ATR candidate and confirmed swing resistance. The merged
candidate is then passed through the same L60-01 monotonicity,
protection-floor, crossed-reference, minimum-gap, and minimum-improvement
constraints.

`weakening_sensitive_adaptive_trail_v1` uses the existing
`ProtectiveStopPlanner`. Confirmed structure is supplied together with the
historical `momentum_weakened` input from the v2-bound one-bar predicate.

## Causal replay order

For each admitted entry, the cohort-entry stop is active immediately. Every
subsequent stop amendment is formed only after a fully closed 5m candle and
becomes active at the next contiguous candle open.

Each active bar is processed in this order:

1. detect a missing expected 5m slot; if present, censor without inferring a
   stop fill across the gap;
2. make the prior close's pending stop amendment effective;
3. test an adverse gap-through of the already-active stop at the bar open;
4. test an intrabar touch of the already-active stop;
5. on held bar 72, exit at the close if no stop already won;
6. otherwise include the completed bar in historical excursion state and form
   a close-time stop amendment for the next candle only.

The max-holding bar never creates a stop amendment. An active stop on that bar
wins over the close terminal.

MFE does not infer an OHLC sequence. Completed bars strictly before the exit
bar may contribute their full high/low. A gap exit contributes only its open.
An intrabar stop exit contributes only the exit-bar open and stop price. A
close terminal uses the exit-bar open and close, not the unknown ordering of
its high/low.

The premature-stop diagnostic uses a separately frozen candidate-exit ATR. For
an OPEN or INTRABAR challenger stop, that value is the positive ATR from the
immediately prior, ready, fully closed and contiguous 5m Guardian context. The
current exit bar is never used. Missing/nonpositive prior context primary-censors
that challenger episode; an older stale ATR is never carried forward and
missing ATR is never coerced to a non-premature result. Same-bar INTRABAR exits
remain unordered.

The one-R target remains diagnostic only. If the target and an already-active
non-gap stop both lie inside the same OHLC range, the ambiguity counter is
incremented; no target exit or target win is created.

Adverse gap-through slippage is diagnostic basis points relative to entry:

- LONG: `max(0, active_stop - open) / entry_price * 10000`
- SHORT: `max(0, open - active_stop) / entry_price * 10000`

## Funding and arithmetic

Historical funding files remain the frozen per-symbol `FundingDataset`
authority and must pass exact symbol/range verification before replay. Funding
events are strict-interior only. An event exactly at entry or exit censors that
policy outcome. A positive mark price is used when present; otherwise entry
price is the fallback. Funding return is converted to basis points exactly
once with `* 10000`.

Contract metrics use Decimal precision 34 with `ROUND_HALF_EVEN`,
`Decimal(str(value))`, and no intermediate quantization. After-cost return uses
the contract's fixed entry/exit fee and cohort slippage basis points plus
realized funding and the 0.10 bp cost per effective post-admission stop update.

## Pairing and statistics

Every challenger is compared with `delayed_atr_trail_v1` on that pair's exact
paired-valid position intersection. Censor fraction alone uses all admitted
entry rows. The harness computes the preregistered historical metrics:

- paired mean after-cost return delta;
- positive maximum drawdown magnitude;
- worst-5% CVaR;
- premature-stop rate;
- mean MFE giveback;
- effective stop updates per 24 hours of exposure;
- paired total stop-update count and mean gap-through slippage;
- paired same-bar ambiguity counts for candidate and baseline;
- pair-specific evidence counts by direction, symbol, UTC entry day, and entry
  trend state.

L60-03 also writes preregistered report-only strata on the exact same paired-valid
intersection. They never admit, remove, reweight, select, or guardrail rows:

- direction: `LONG` / `SHORT`;
- source entry regime: `risk_on`, `neutral`, `risk_off`;
- UTC entry buckets: `[00:00,08:00)`, `[08:00,16:00)`, `[16:00,24:00)`;
- decision-candle `atr_percent`: `<0.50%`, `[0.50%,1.00%)`, `>=1.00%`.

Unexpected regime or unavailable volatility values are reported as
`stratum_unavailable` without changing primary validity. The bins are frozen
before outcomes and cannot be retuned from observed results.

The deterministic familywise bootstrap follows the L60-02 SHA-based circular
seven-UTC-day draw schedule. All challengers share each draw and a replicate is
discarded for the family if any challenger has no paired row under that draw.
L60-03 reports the historical point estimates and simultaneous lower bounds but
does not apply a promotion decision.

## CLI and artifacts

The only L60-03 CLI surface is:

```text
signalbot backtest-guardian-policy \
  --config config/settings.example.yaml \
  --contract config/guardian-policy-selection.v2.json \
  --data-dir <historical-data-dir> \
  --output-dir <output-dir>
```

There are deliberately no operator overrides for policy, threshold, costs,
direction scope, max holding, bootstrap samples, bootstrap seed, or block size.
Those values belong to the preregistration.

A successful run writes `entry_rows.jsonl`, `policy_episodes.jsonl`,
`historical_metrics.json`, `run_manifest.json`, and `report.md`.
`historical_metrics.json` includes paired primary metrics, the fixed report-only
strata, and deterministic familywise-bootstrap evidence. The manifest records
the exact v2/parent identities, settings authority, entry-producer authority,
data hashes, and LF-canonical Guardian implementation source hash. It does not
include the repository-wide legacy raw-byte source digest because unrelated
Python files or CRLF/LF checkout conversion must not perturb this scientific
manifest. Wall-clock run time is deliberately absent so an exact replay can be
compared byte-for-byte. These artifacts contain no selected policy, winner, or
promotion verdict. Policy adjudication remains L60-08.
