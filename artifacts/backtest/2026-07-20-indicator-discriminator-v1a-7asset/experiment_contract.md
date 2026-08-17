# Indicator Discriminator V1A — Seven-Asset After-Cost Historical Contract

## Material passport

- Status: `FROZEN_BEFORE_ANY_V1A_REPLAY`
- Role: exposed historical diagnostic only
- Production impact: none
- Order execution: forbidden
- Probability claim: forbidden
- Parent artifacts: the original eight-asset V1 and its development-only
  amendment are retained unchanged and are not V1A inputs.

## Why this sibling exists

The original V1 development replay failed before creating outcomes because
PENGU Spot begins at `2024-12-17T14:00:00Z` and PENGU Futures begins at
`2024-12-17T16:15:00Z`. Neither can supply the required 40-day causal warm-up
before the frozen development boundary `2024-07-01T00:00:00Z`.

The original eight-asset validation and retrospective outputs already exist.
They are classified
`EXPOSED_8_ASSET_DIAGNOSTIC_NOT_COMPARABLE_NOT_V1A_INPUT`. Removing PENGU rows
from those files is forbidden because the replay constructs strict-prior
breadth/regime features from the complete configured universe; changing that
universe can change every remaining asset's features, decisions, and events.

A development-only seven-asset amendment was also started before this sibling
contract. Its output is not a clean V1A input because its protocol and plan say
that only development changes. V1A requires the same seven-asset universe in
all three splits.

## Frozen replay universe and chronology

The exact ordered universe is:

1. BONK
2. ENA
3. WIF
4. FLOKI
5. ARB
6. OP
7. SEI

Both Spot and USDⓈ-M Futures are replayed for every asset. The exact splits are:

- development: `[2024-07-01T00:00:00Z, 2025-03-01T00:00:00Z)`
- validation: `[2025-03-01T00:00:00Z, 2025-11-01T00:00:00Z)`
- retrospective stress: `[2025-11-01T00:00:00Z, 2026-07-01T00:00:00Z)`

Only fully closed five-minute candles may contribute to a recommendation.
Each split is replayed independently with the same 40-day causal warm-up and
the exact same specification, source-code digest, configuration digest, input
file keys, and input file hashes.

## Frozen analysis population

Before fitting the score or consulting any outcome, an event must satisfy all
of the following exact predicates:

```text
information_only == True
stage == "setup"
family in {"pullback_long", "pullback_short"}
score == 100
family == "pullback_long"  iff direction == "long"
family == "pullback_short" iff direction == "short"
asset in the exact seven-asset universe
split equals the replay directory's one declared split
```

Score eligibility requires all eight finite decision-time features. A row that
meets the population predicate but lacks any feature becomes an explicit
`FEATURE_NOT_READY` no-call before any outcome lookup; its event ID, reason,
count, and deterministic audit hash are retained. It remains in the retention
denominator. No imputation, neutral-zero substitution, outcome-dependent row
deletion, or partial-axis averaging is permitted. Spot has deterministic
priority when Spot and Futures share `(asset, direction, decision_time_ms)`.

The eight features and four equally weighted axes are unchanged in economic
meaning from V1:

- trend quality: efficiency ratio 20, directional DI balance, ADX delta
- resumption: direction-aligned MACD-histogram delta divided by ATR
- participation: directional taker delta, volume z-score
- orderly pullback: range contraction, quote-volume contraction

Development-only empirical-CDF midranks transform every feature. The score is
`100 * mean(the four equal-weight axis means)`. The single candidate selection
is `score >= type-7 75th percentile of development scores`. Feature fitting is
label-free; validation and retrospective features cannot alter the fitted
distributions or cutoff.

As a bounded descriptive check of whether stronger multi-indicator agreement
has a monotone after-cost outcome gradient, development-only type-7 score
cutoffs `q25`, `q50`, and `q75` define `Q1: score < q25`,
`Q2: q25 <= score < q50`, `Q3: q50 <= score < q75`, and
`Q4: score >= q75`. The `q75` value must equal the frozen selection cutoff.
For every split and horizon, overall after-cost point metrics are reported for
all four bins, including explicit empty bins. This table is
`DESCRIPTIVE_ONLY`, receives no inferential bootstrap, is not part of the
confirmatory gate, is not a probability calibration, and cannot promote the
rule.

## Frozen execution and outcome contract

Entry is the next contiguous five-minute open after the closed recommendation
candle. The replay's existing market-specific fees, slippage assumptions, and
funding are retained. Each event must have exactly one outcome for every
horizon `{1, 3, 6, 12, 72}`, corresponding to `{5, 15, 30, 60, 360}` minutes.

V1A reads the replay's after-cost `net_return`, not recommendation-close raw
direction. CSV decimal text is deterministically rounded half-even to signed
integer return micros. Missing or unevaluable outcomes remain missing and are
never replaced by zero.

Every outcome is joined to its authenticated recommendation, including rows
outside the eventual V1A score population. A decision timestamp must be the
close of a five-minute candle. An evaluable row must have
`entry_time_ms = decision_time_ms + 1` and
`exit_time_ms = entry_time_ms + horizon_bars * 300000 - 1`, remain wholly
inside the recommendation's frozen split, and begin no earlier than the exact
72-bar split-start embargo boundary. A determinable pre-embargo row must retain
the exact `split_start_embargo` reason. A path whose derived exit reaches the
split end must be unevaluable and retain only a producer-reachable reason from
the frozen check order: `horizon_crosses_split`, its horizon-specific
`insufficient_*_bar_horizon`, or `data_gap_in_horizon`; when the derived entry
itself is at or beyond the boundary, `split_start_embargo` and
`outside_declared_split` are also possible. Every unevaluable row has one
allowed exclusion reason, empty numeric/path fields, four `unevaluable` hit
statuses, and an `unevaluable` one-R status.

For evaluable rows, the analyzer recomputes internal arithmetic from the
authenticated row metadata and frozen cost schedule: volatile Spot uses
10 bps fee and 10 bps slippage; volatile Futures uses 5 bps fee and 8 bps
slippage. It checks raw close return, direction-signed gross return,
slippage, fees, and `net = net_before_funding + funding`. Spot funding must be
exactly zero. All finite-float reconciliation uses deterministic
`math.isclose(rel_tol=1e-12, abs_tol=1e-12)`. Maximum rise/drop must contain
the raw open-to-close move; MFE/MAE must match their LONG/SHORT transforms;
and 0/5/10/25 bps statuses are recomputed with equality classified
`ambiguous`.

One-R target/risk fields are recomputed from recommendation invalidation and
entry price. Status, path-extrema support, and observed timestamp combinations
are checked conservatively; aggregate extrema cannot establish intrabar order.
This is an authenticated-row consistency audit, not an independent replay:
V1A does not re-read raw candles or funding records to recompute path extrema,
funding cash flows, first-touch order, or source completeness.

The sole confirmatory historical endpoint is validation, horizon 12 (60
minutes), overall top-quartile after-cost mean net return. Horizons 1, 3, 6,
and 72 and all subgroup tables are descriptive controls. They cannot create a
success claim.

## Dependence, uncertainty, and multiplicity

Each chronological split uses one shared deterministic circular UTC-calendar
moving-block schedule for all horizons, selections, directions, and assets:

- block length: 7 calendar days
- samples: 10,000
- seed: 20260720
- zero-alert days retained
- complete frozen split boundaries used, not first/last observed alert

The schedule hash, valid/invalid replicate counts, two-sided percentile
intervals, and a one-sided 95% basic-bootstrap lower confidence bound are
retained. There is exactly one frozen
confirmatory horizon. Secondary horizons are labelled descriptive and receive
no inferential claim. Direction and asset requirements below are an
intersection gate, not separately reported discoveries; failure of any gate
rejects the historical candidate.

The validation-horizon-12 selected-mean and paired selected-minus-baseline
mean endpoints must each have all `10,000/10,000` bootstrap replicates valid.
An invalid replicate is never silently conditioned away for the confirmatory
claim. Sparse secondary cells may retain invalid replicate counts but remain
descriptive only.

## Historical validation gate

Every condition must hold on validation at horizon 12:

1. selected mean after-cost net return is strictly positive;
2. its one-sided 95% lower confidence bound is strictly positive;
3. selected-minus-baseline mean after-cost return is strictly positive;
4. the corresponding one-sided 95% lower bound is strictly positive;
5. selected profit factor is strictly greater than 1;
6. selected median after-cost return is strictly positive;
7. LONG and SHORT selected mean after-cost returns are each strictly positive;
8. selected retention is at least 20%;
9. selected evaluable sample size is at least 300;
10. selected-minus-baseline mean return is positive in at least 6 of 7 assets.
11. complete-case feature coverage is at least 99%.
12. selected-mean and paired-uplift bootstraps each have 10,000 valid
    replicates out of 10,000 requested.

Passing this gate does not establish independent validation, probability,
deployment approval, or future profitability. V1A is historical and its
predecessor validation interval has already been exposed. It may reject a
candidate or justify carrying an unchanged rule into a later untouched PAPER/
BBO interval; it cannot promote live alerts by itself.

## Fail-closed artifact authority

The analyzer binds `workspace_root` to the repository that owns the executing
`indicator_analysis_v1a.py`; a caller-selected lookalike workspace is invalid.
Freeze and replay manifest timestamps use canonical `datetime.isoformat()` UTC
text with the explicit `+00:00` offset. `Z`, naive timestamps, and non-UTC
offsets are invalid, and ordering is strictly
`freeze.created_at_utc < replay.started_at_utc <= replay.completed_at_utc`.
Declared replay duration must be finite, nonnegative, and consistent with that
UTC interval. The freeze also records exact machine-readable
`historical_only: true` and `external_anchor: false` fields.

The frozen backtest YAML is parsed as `BacktestSpec` and checked field by
field against this contract. The settings YAML is parsed directly with
`Settings.model_validate()` without environment overrides. Canonical semantic
hashes of both parsed models accompany their exact byte hashes. Source
authority additionally includes `pyproject.toml`, `uv.lock`, and
`.python-version`. At analysis load time all 21 files below
`data/backtest` are streamed through SHA-256 again; agreement between two
manifests alone is insufficient.

Before analysis, all three replay directories must prove:

- exactly one expected split and the exact ordered seven-asset universe;
- identical spec/config/code/rule/protocol and input key/hash sets;
- manifest output hashes equal the current output bytes;
- exactly 21 input artifacts: seven Spot, seven Futures, seven funding files;
- `outcome_rows == events * 5`;
- globally unique event IDs;
- exactly one `{1,3,6,12,72}` outcome set per event;
- identical interval, markets, horizon, and cost contract;
- an exact ordered 14-row `per_symbol` panel (Spot seven, then Futures seven),
  including zero-event combinations, whose event and outcome totals reconcile;
- recommendation `(asset, cohort, market, symbol)` matches the frozen seven-
  asset symbol map even when a combination has zero events;
- recommendation and outcome CSV headers exactly equal their producer
  dataclass field order, with duplicate columns and surplus cells rejected;
- every outcome joins to authenticated recommendation timing, direction,
  market, cohort, and invalidation metadata before population filtering, and
  passes the timing, exclusion, cost, path, hit-status, and one-R consistency
  checks specified above;
- no unknown, missing, or extra asset/split silently filtered.

The final analysis records every source manifest and CSV hash, the frozen score
artifact, the shared draw hash, population exclusions, and all gate decisions.
For each split it separately records canonical SHA-256 values for intended
pre-dedup rows, retained spot-priority rows, and actually dropped rows, and
requires `intended == retained + dropped`.

### Fresh atomic analysis publication

The requested analysis target must not already exist, even as an empty
directory or symlink. The analyzer writes into a uniquely named sibling
temporary directory on the same filesystem. It writes exact UTF-8, LF-only
bytes for `fitted_score.json`, `results.json`, and `report_ko.md`, flushes and
file-syncs each file where the platform supports `fsync`, and then writes
`analysis_manifest.json` the same way. Before and after publication it requires
the exact four-file set and rehashes all three payloads. It publishes only by
an atomic directory rename; write, validation, race, or rename failure removes
only the analyzer-owned incomplete directory and never exposes a partial
analysis target.

The manifest contains schema and protocol versions, a canonical `+00:00` UTC
completion timestamp, the freeze-manifest hash, a canonical hash of the exact
`input_authority` object in deterministic `results.json`, and exact hashes for
the three payloads. It also records `historical_only: true`,
`external_anchor: false`, `deployment_approved: false`, and
`probability_calibrated: false`. The manifest never includes its own hash.
Payload bytes remain deterministic; only the publication-manifest completion
timestamp may differ between otherwise identical runs.

### Residual runtime provenance

The explicit freeze manifest seals this contract before every V1A replay.
Replay manifests freeze dependency files but do not record interpreter,
platform, or installed-package runtime provenance. Cross-machine bitwise
reproducibility is therefore not claimed; that limitation must remain visible
in any later frozen authority and report.

## Commands to run only after the freeze manifest is sealed

```powershell
uv run signalbot backtest-alert-replay --config config/settings.example.yaml --spec config/backtest.5m.indicator-discriminator-v1a-7asset.yaml --data-dir data/backtest --output-dir artifacts/backtest/2026-07-20-indicator-discriminator-v1a-7asset/replay-development --split development
uv run signalbot backtest-alert-replay --config config/settings.example.yaml --spec config/backtest.5m.indicator-discriminator-v1a-7asset.yaml --data-dir data/backtest --output-dir artifacts/backtest/2026-07-20-indicator-discriminator-v1a-7asset/replay-validation --split validation
uv run signalbot backtest-alert-replay --config config/settings.example.yaml --spec config/backtest.5m.indicator-discriminator-v1a-7asset.yaml --data-dir data/backtest --output-dir artifacts/backtest/2026-07-20-indicator-discriminator-v1a-7asset/replay-retrospective --split retrospective_test
uv run python -m signalbot.backtest.indicator_analysis_v1a --freeze-manifest artifacts/backtest/2026-07-20-indicator-discriminator-v1a-7asset/freeze_manifest.json --replay-dir artifacts/backtest/2026-07-20-indicator-discriminator-v1a-7asset/replay-development --replay-dir artifacts/backtest/2026-07-20-indicator-discriminator-v1a-7asset/replay-validation --replay-dir artifacts/backtest/2026-07-20-indicator-discriminator-v1a-7asset/replay-retrospective --output-dir artifacts/backtest/2026-07-20-indicator-discriminator-v1a-7asset/analysis --samples 10000 --seed 20260720
```
