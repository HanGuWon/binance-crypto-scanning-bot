# Guardian policy selection preregistration v2

`guardian-policy-selection-v2` is a narrow, outcome-blind amendment to the
frozen L60-02 v1 preregistration. It binds the historical producer of the
`momentum_weakened` boolean consumed by `weakening_sensitive_adaptive_trail_v1`,
binds the exact Settings and clean source authorities required to reproduce the
historical source entry cohort, and closes deterministic L60-03 implementation
gaps in already-frozen MFE/funding/exposure/premature-stop semantics before any
real historical policy outcome is computed.

The machine-readable authority is
`config/guardian-policy-selection.v2.json`. Except for the version metadata,
the historical momentum-weakening authority, the exact historical Settings and
clean source authorities, and the explicit historical execution conventions
defined below, v2 inherits every cohort rule, policy, threshold, cost, guardrail,
evidence floor, bootstrap rule, selection rule, and external-shadow rule from v1
unchanged.

## Parent contract

- Parent: `guardian-policy-selection-v1`
- Parent semantic SHA-256:
  `2e19fd7a597dd78a0372753da50fd54dcfacceea6e9482bb34aac606c507a923`

The v2 identity uses SHA-256 over the ASCII domain separator
`GUARDIAN_POLICY_SELECTION_CONTRACT_V2`, followed by one NUL byte and the UTF-8
bytes of canonical JSON serialized with sorted keys, `ensure_ascii=true`, and
compact comma/colon separators.

Final independently reviewed semantic SHA-256:
`23140ebd342ccf5e2b6c1ba9a6f8b180ece420cf8277db7fe944c0b3790fefb7`.

All bound repository text-source hashes retain the v1 LF-canonical rule: replace
each CRLF byte pair with LF, perform no other transformation, then SHA-256.

## Historical momentum-weakening authority

The weakening-sensitive policy authority added in v2 is:

- Rule ID: `technical_exit_one_bar_trend_failure_v1`
- Owner: `signalbot.signals.positions.TechnicalExitEngine.after_close`
- Source: `src/signalbot/signals/positions.py`
- LF-canonical source SHA-256:
  `fab4a5198b1ebd065f7ab4848c3898c7f5fa35b421580d697d74461d5f1d0beb`

The boolean is the existing one-bar `failed` predicate already used by
`TechnicalExitEngine.after_close`:

- LONG: `feature.price < feature.ema20 and feature.macd_histogram < 0`
- SHORT: `feature.price > feature.ema20 and feature.macd_histogram > 0`

This v2 binding is deliberately the one-bar predicate only. It does **not**
require `trend_failure_count >= trend_failure_bars`, and it does not include
opposite-signal logic. Those are separate existing exit-engine behaviors and
are not part of the Guardian weakening input.

The feature clock is closed-candle causal. The boolean and confirmed structure
used by a weakening-sensitive stop update must come from the same fully closed
5m `FeatureSnapshot`. A stop derived from that snapshot can first become active
at the next contiguous 5m candle open. The current candle can never use its own
close-time update retroactively.

If the causal `FeatureSnapshot` required for a close-time update is unavailable,
the Guardian context is not ready and no stop update is created. Unavailable is
not coerced to `False` for the purpose of continuing an otherwise-computable
close-time update.

## Historical Settings authority

The frozen base `BacktestSpec` does not itself contain every setting consumed by
`ResearchBacktester`. In particular, `ResearchBacktester` starts from
`settings.signals` before applying its explicit `BacktestSpec` overrides, and
`FeatureEngine` uses inherited settings such as `breakout_lookback` when it
constructs the raw C0 feature tape. Allowing an arbitrary CLI `--config` would
therefore make the supposedly frozen entry cohort mutable.

Before any historical market-data read or policy outcome computation, v2 binds:

- Settings path: `config/settings.example.yaml`
- LF-canonical SHA-256:
  `2889de620152b22821281d031e7532afef885897321850721fd28bd15da25843`

The L60-03 runner must require the CLI config path to resolve to that exact
repository file, verify the CRLF-to-LF-only hash, load it through the normal
`load_settings` parser, and require the already-supplied `Settings` model to be
identical to the bound file's model. Any path, byte, or parsed-settings drift
fails closed before historical kline/funding data are read.

Parser drift is also closed explicitly. The loaded `Settings.model_dump(mode="json")`
is serialized as sorted compact ASCII JSON and SHA-256 hashed. Its frozen
effective-settings identity is
`b01aa1f2434feb03f1fce70866059c21dcca613235f2e764874739f36ba31839`.
Thus a future `config.py` parser/model change that changes the effective values
fails closed even if the YAML bytes themselves are unchanged.

Archived July R2 C0 manifests also name `config/settings.example.yaml` as their
configuration input, but their historical raw-byte hash predates the current
repository import/history. v2 therefore does not pretend those unavailable raw
bytes are present. It freezes the current repository's canonical settings file
before outcomes, while preserving the already-frozen C0 BacktestSpec and its
entry semantics. This authority is prospective to this L60-03 evidence run and
must not be changed after outcomes are observed.

The entry producer implementation is also frozen before outcomes. The following
clean source files must match both their LF-canonical worktree SHA and their Git
blob at commit `2bb0d1eb075a6cf34d094cacfd902abb939d7bf1`:

| Source | SHA-256 |
| --- | --- |
| `src/signalbot/backtest/config.py` | `6844f58a25b6686b464c8106264213c78b9e82bb557fdbde6cb1969bc13d21e2` |
| `src/signalbot/backtest/engine.py` | `76fc746916d52c9a6e3cdf61e9b60ce494e27273a28714128e5cbe4a35a1f963` |
| `src/signalbot/data/microstructure.py` | `bf76b8206b154b0ed2be39848e6f3abed2719d891f3a0228e73d87980f24d0f8` |
| `src/signalbot/indicators/core.py` | `7ac200b898bcf7afc8834500cec34b620c4666318ff6d093d5060cac03e9fd6e` |
| `src/signalbot/indicators/structure.py` | `fc29b1e15bc6e2a077293f924379d8188fa532491732da27a6f1577d5e737d05` |
| `src/signalbot/signals/gates.py` | `b34da1259d5e8c1c1201d1e0b19c471704554244cd370405fb911e572f9d0924` |
| `src/signalbot/signals/rules.py` | `cfd7eb9c95094e259984ad7649dd28d1f0ea8f10d78c5641140758bb7595a72d` |
| `src/signalbot/signals/state_machine.py` | `a0b8a792088bf057c7c543eed2d71f9256c815e517282129bdd5a08b222b7bc7` |

`positions.py`, `funding.py`, `protection_context.py`, the base BacktestSpec, and
the Settings file are already bound separately by this contract. This narrow
source list prevents unrelated dirty Directional/OCI files from becoming an
implicit dependency of the L60 historical cohort.

## Historical diagnostic strata

L60-03 requires direction, volatility/regime, and time-of-day breakdowns. To
prevent outcome-driven binning, v2 freezes these as report-only diagnostics
before the run. They never admit/remove/reweight entries and never become a
promotion or hard-guardrail gate.

- Direction: exact entry `LONG` / `SHORT`.
- Regime: source `Trade.regime` at entry (`risk_on`, `neutral`, `risk_off`);
  unexpected values are `stratum_unavailable` rather than post-hoc remapped.
- UTC time of day from `entry_time_ms`: `[00:00,08:00)`, `[08:00,16:00)`, and
  `[16:00,24:00)`.
- Volatility: `FeatureSnapshot.atr_percent` from the same fully closed 5m
  decision candle that scheduled the entry, with fixed bins `<0.50%`,
  `[0.50%,1.00%)`, and `>=1.00%`. Missing/non-finite values are
  `stratum_unavailable` and do not change primary validity.

For each challenger versus baseline, every stratum uses that comparison's same
paired-valid primary intersection and reports count, candidate/baseline mean
after-cost return, paired mean delta, candidate premature-stop rate,
candidate/baseline MFE giveback, and candidate/baseline stop-update frequency.
Bins or labels must not be changed after outcomes are observed.

## Historical execution conventions

These conventions do not change a policy, threshold, fee, slippage row, or
selection gate. They make the already-frozen historical metric definitions
single-valued for L60-03.

The source run's `Trade.entry_execution_price` is an entry fact and is frozen
into the `GuardianEntryRow` together with the raw entry price. Every policy uses
the same value. Historical research quantity is frozen once as
`Decimal(str(source_spec_notional_usdt)) / Decimal(str(raw_entry_price))`; it is
provenance only because the selection metrics are normalized in basis points.

For MFE diagnostics, direction sign is +1 for LONG and -1 for SHORT. MFE in
basis points is:

`max(0, direction_sign * (best_allowed_favorable_market_price - common_entry_execution_price) / common_entry_execution_price * 10000)`.

The allowed favorable prices are still exactly those from v1: completed bars
strictly before the exit bar may contribute full OHLC extremes; a gap exit uses
only the exit open; an intrabar stop uses only the exit-bar open and stop fill;
and the max-holding CLOSE bar does not infer its high/low ordering.

For MFE giveback only, `realized_directional_return_bps` uses the same entry
execution anchor and the raw policy fill price before modeled fee/slippage:

`direction_sign * (policy_exit_price - common_entry_execution_price) / common_entry_execution_price * 10000`.

The primary after-cost return remains the unchanged v1 fixed-bps formula; this
MFE diagnostic convention does not replace or alter it.

Historical exit timestamps are deterministic because an OHLC bar does not
contain the exact tick time of an intrabar stop:

- OPEN exit: `exit_time_ms = candle.open_time_ms`
- INTRABAR stop exit: `exit_time_ms = candle.close_time_ms`
- CLOSE/max-holding exit: `exit_time_ms = candle.close_time_ms`

That timestamp is used for strict-interior funding, exposure duration, and
drawdown ordering. Exit phase remains a separate field and still defines
`OPEN < INTRABAR < CLOSE` for same-bar premature-stop diagnostics.

For the premature-stop diagnostic, “candidate-exit-time ATR” is causal and
phase-specific. An OPEN or INTRABAR stop uses the positive ATR from the latest
ready fully closed contiguous 5m Guardian context strictly before that exit bar.
The current exit bar's ATR is unavailable at OPEN/INTRABAR and must not be used.
A CLOSE exit may observe its just-closed context, but max-holding CLOSE is not a
candidate stop for the premature numerator. If an OPEN/INTRABAR candidate stop
lacks the immediately-prior ready positive ATR context, its policy episode is
primary-censored before paired metrics. The runner must not carry an older stale
ATR forward and must not turn an unavailable diagnostic into `False`.

## Amendment exclusivity

Before v2 can authorize any real L60-03 outcomes, the identity test must prove
that normalizing these v2-only fields back to v1 reconstructs the exact frozen
v1 machine contract:

- `schema_version`
- `contract_version`
- `parent_contract`
- `amendment_scope`
- `historical_momentum_weakening_authority`
- `historical_entry_producer_authority`
- `historical_diagnostic_strata`
- `historical_execution_semantics`
- `cohort_authority.historical_harness.settings_authority_rule`
- `cohort_authority.historical_harness.settings_path`
- `cohort_authority.historical_harness.settings_sha256`
- `cohort_authority.historical_harness.effective_settings_hash_rule`
- `cohort_authority.historical_harness.effective_settings_sha256`
- `outcome_semantics.premature_stop_definition.candidate_exit_atr_rule`
- `outcome_semantics.premature_stop_definition.missing_candidate_exit_atr_rule`

No existing v1 policy or selection field may differ.

The historical momentum source, entry-producer sources, and Settings SHA above
must independently match both the current worktree after CRLF-to-LF-only
normalization and their repository Git/LF blobs at the frozen commit.

## Outcome-blind gate

No L60-03 real historical policy outcome may be computed until:

1. the v2 semantic contract SHA-256 is independently recomputed;
2. the v2 identity/source/exclusivity tests are green; and
3. an independent read-only review records no P0/P1 blocker.

L60-03 remains evidence generation only. v2 does not select a policy, relax a
guardrail, alter a threshold, or move policy adjudication out of L60-08.
