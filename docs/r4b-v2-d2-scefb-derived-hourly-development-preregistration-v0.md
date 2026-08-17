# D2 SCEFB derived-hourly development preregistration v0

## Material passport

- Origin skill: academic-research-suite / experiment-agent
- Origin mode: plan
- Origin date: 2026-07-21
- Verification status: `UNVERIFIED_PRE_D2_OUTCOME_ACCESS`
- Version label: `D2_SCEFB_DERIVED_1H_POST_D1_DEV_V0`
- Adaptation label: `POST_D1_FAILURE_ADAPTATION`
- Activation status: `DISCONNECTED_HISTORICAL_DIAGNOSTIC_NOT_ARMED`
- Economic rule inherited unchanged:
  `D1_SCEFB_5M_PREREG_V0`
- Historical role:
  `POST_D1_HISTORICAL_DEVELOPMENT_DIAGNOSTIC_ONLY`

This document is fixed before any D2 strategy replay, signal count, episode,
return, hit-rate, profit-factor, or disposition is computed. The source-policy
change is informed by the terminal D1 input failure described below. The
historical interval is therefore development-contaminated and cannot provide
untouched confirmation.

## Predecessor and contamination boundary

D1 run `d1-scefb-v0-development-run-002` permanently crossed START and ended
`FAILED` before strategy calculation. It produced no result and no result
artifact manifest. The immutable predecessor bindings are:

| Binding | SHA-256 |
| --- | --- |
| D1 preregistration | `af69c262282144432e6adbf1e01406c7334e37176dd83ce6f9666adc49b6899d` |
| D1 input-authority domain | `c33a77f4223dcf2b90fbf79853beb4818af105ccb65bf248daa273a3a4089f62` |
| D1 run-002 freeze | `bdf6f495762371281a137c32d57066602578a47598303d2ce4830d5e977b161a` |
| D1 run-002 START record | `1eb5d24f79c43bbdb80e7fdcb479a606fa92be6aa76e95c657f09509ecbe4c5d` |
| D1 run-002 terminal FAILED record | `81948df00e0a11812d9088239712d145ba8ce0daa21fffefe4ab06573626b369` |
| D1 failure-evidence manifest | `15988eec55f311cfc95273eca17848328a6fa24ab8b315f9c354c3e869a51e72` |
| D1 frozen failure-evidence archive | `f44e4c38aefeb5542c8875e3625ab01e82cde1fd4ff7738e26684b9895a25592` |

The post-terminal audit found exactly two native-1h versus final-5m close
mismatches per symbol, all at the same two UTC hours, across the fixed ten
symbol universe. That observation selects the D2 source policy. D2 is not a
D1 retry, repair, continuation, or result.

No D2 implementation may retry, resume, replace, or relabel the consumed D1
attempt. Native 1h outcome bytes are not a D2 feature input and must not be
opened by the D2 runner. They may be referenced only by the already completed
failure audit.

## Fixed scientific question

Holding the D1 economic rule, universe, signal thresholds, timing, position
lifecycle, exit priority, execution proxy, funding arithmetic, fee cells,
screens, and non-claim semantics unchanged, what historical diagnostic is
produced when the sole higher-timeframe price authority is a deterministic
UTC-aligned aggregation of the authenticated closed 5m authority?

The alternative source policy is not itself a hypothesis that the strategy is
profitable. It removes an internally contradictory dual-source equality gate
and makes live and replay price-candle ownership consistent.

## Frozen universe and interval

The universe remains exactly:

```text
BTCUSDT, ETHUSDT, BNBUSDT, SOLUSDT, XRPUSDT,
DOGEUSDT, ARBUSDT, OPUSDT, SUIUSDT, WIFUSDT
```

The authenticated 5m data boundary remains
`[2024-03-01T00:00:00Z, 2026-07-01T00:00:00Z)`, exactly 245,376 contiguous
closed rows per symbol. Warm-up remains
`[2024-03-01T00:00:00Z, 2024-07-01T00:00:00Z)`. The D2 development replay
interval remains
`[2024-07-01T00:00:00Z, 2026-07-01T00:00:00Z)`.

No post-2026-07-01 candle, funding row, result artifact, or prospective label
may enter the D2 historical diagnostic.

## D2 input authority

The D2 authority must contain exactly:

- the ten fixed authenticated 5m sidecar-manifest byte hashes, in universe
  order; and
- the existing canonical funding-authority manifest byte hash and its ten
  exact funding gzip byte hashes.

It must contain no native 1h manifest, native 1h data hash, native 1h path, or
alternate candle source. The canonical D2 authority hash uses a new domain and
schema. A D1 authority hash may be recorded as predecessor provenance but may
not substitute for the D2 authority.

Opening any outcome row is prohibited until the D2 authority has been
published, the complete implementation and tests have been frozen, and the
one-shot attempt has durably appended START.

## Derived-hourly contract

For each symbol, sort is forbidden as a repair: the authenticated 5m source
must already be strictly ordered, unique, closed, and contiguous. For every UTC
hour with open `h`, derive exactly one hour only from all twelve slots:

```text
h + 0m, h + 5m, ..., h + 55m
```

The aggregate is fixed as:

```text
open        = first constituent open
high        = max(constituent highs)
low         = min(constituent lows)
close       = final constituent close
base volume = sum(constituent base volumes)
quote volume = sum(constituent quote volumes)
trade count = sum(constituent trade counts)
taker-buy base volume = sum(constituent taker-buy base volumes)
taker-buy quote volume = sum(constituent taker-buy quote volumes)
```

The historical proxy `data_through_ms` and `receipt_ms` equal the derived hour
close timestamp. This remains a replay convention, not observed local receipt
evidence. A gap, duplicate, unclosed row, mixed symbol/market/interval,
misaligned slot, partial UTC hour, wrong boundary, wrong count, or noncanonical
source fails closed.

Exactly 245,376 source rows must produce exactly 20,448 derived hours. Each
derived close must equal its twelfth constituent close. The runner computes a
canonical per-symbol derived-hour manifest containing the 5m manifest hash,
5m compressed-data hash, row boundaries and counts, derivation policy version,
and an ordered canonical sequence root. The manifest hash, not a native 1h
hash, occupies the replay core's higher-timeframe provenance slot.

At a signal bar ending at `hh:59:59.999`, the just-completed derived hour is
eligible. At earlier five-minute closes within an hour, that incomplete hour
does not exist and the preceding completed hour is the latest eligible hour.
No centered window, forward fill, future row, or unclosed higher-timeframe
candle is permitted.

## Economic rule held fixed

Every mathematical and economic rule in
`D1_SCEFB_5M_PREREG_V0`, including amendment A0, is inherited byte-for-byte in
meaning. In particular, D2 changes none of:

- the exact 289 prior 5m bars, 250 completed hours, ATR, channel,
  compression/expansion, volume, taker-flow, EMA20/EMA50, or first-cross
  thresholds;
- the conjunction semantics: a count of passed indicators is not a
  probability and cannot promote a failed gate;
- `open(t+2)` historical entry, 8 bp adverse slippage per side, fee/funding
  arithmetic, `100/1,000 USDT` display cells, or global non-overlap guard;
- the 0.8-ATR adverse close, channel structural failure, 3-ATR profit close,
  24-bar hard horizon, exit priority, or next eligible fill timing;
- one pending/active position per symbol and no pyramiding; or
- the predeclared retrospective reject, low-information, mixed, and
  screen-pass-inconclusive thresholds.

The implementation may extract a shared replay core to avoid copying the D1
state machine. The frozen D1 native-1h loader and mismatch rejection must
remain behaviorally available. Regression tests must prove that the extracted
core produces byte-identical D1 episodes, censors, and summary for identical
validated inputs.

## One-shot execution and artifacts

D2 requires a fresh preregistration hash, D2 input authority, broad code
freeze, run ID, attempt directory, output directory, and append-only attempt
WAL. The order is:

1. publish and verify the D2 input authority without opening gzip rows;
2. complete implementation, tests, and full verification;
3. create and verify a fresh code freeze bound to this preregistration,
   authority, predecessor failure evidence, source-policy version, and exact
   toolchain files;
4. arm a fresh attempt without row access;
5. append and fsync `STARTED_BEFORE_OUTCOME_ACCESS` before any gzip row opens;
6. consume the start grant exactly once, run sequentially by symbol, publish
   to a fresh no-replace output, revalidate serialized artifacts, and append a
   terminal receipt; and
7. never retry the same attempt after any START append is attempted, including
   process death or publication ambiguity.

After START, failures must emit a bounded canonical error receipt containing
at least the run ID, phase, typed code, sanitized context, and receipt hash.
The terminal WAL binds that receipt hash. A CLI must preserve this typed cause;
it must not collapse it into a generic argument-parser error.

## Fixed outputs and interpretation

The D2 result may report the predeclared D1 descriptive statistics, including
episode counts, side/symbol/exit breakdowns, gross and after-cost returns,
funding coverage, hit rate, mean/median, profit factor, concentration removals,
and both fee/notional projections. These are development diagnostics, not
independent tests and not probabilities.

All result, episode, report, and manifest layers must force:

```text
historical_bbo_available=false
paper_fill_claim=false
execution_conclusive=false
probability_claim=false
efficacy_claim=false
promoting=false
prospective=false
production_order_placement=false
```

The historical status remains `INCONCLUSIVE_NO_HISTORICAL_BBO`. Even a
descriptive screen pass cannot authorize a live recommendation, PAPER
promotion, or production order.

The deterministic D2 replay must be repeated independently from the serialized
authority/freeze inputs. Exact episode, censor, summary, result, and manifest
hashes must match. Any mismatch invalidates the diagnostic.

## Statistical safeguards

No threshold is selected from D2 outcome. No parameter grid, RSI variant,
asset exclusion, direction exclusion, alternative endpoint, best-period
selection, or failed-run retry is permitted. The D2 report must explicitly
scan for and reject these interpretation errors:

1. outcome leakage or look-ahead;
2. treating correlated symbols or overlapping episodes as independent;
3. pseudoreplication of notional or fee display cells;
4. post hoc subgroup or direction selection;
5. multiple testing without the declared family correction;
6. p-value or confidence-bound misinterpretation;
7. practical-significance claims from small effects;
8. survivorship or universe substitution;
9. ignored transaction costs, funding, capacity, or no-fill selection;
10. extrapolation from historical proxy fills to executable PAPER fills; and
11. extrapolation from a development-contaminated interval to prospective
    efficacy.

If further outcome-informed changes are made after the D2 result, they create
D3 and require a strictly later untouched attempt.

## Prospective boundary

Historical D2 can reject this rule descriptively; it cannot confirm it. The
same derived-hour policy must later be implemented from canonical final
`kline_5m` M1 evidence, with full lineage and actual receipt times, without
adding a direct `@kline_1h` stream. That M1 projection remains non-promoting
until M2 completeness and decision authority are independently bound.

Only after a new freeze may a 30-consecutive-day data-only qualification begin.
No PnL or labels may be inspected during qualification. An authoritative
untouched efficacy test requires a strictly later 365-calendar-day
prospective PAPER/BBO interval and all sample-size, quarter, side, symbol,
concentration, cost, synchronized block-bootstrap, profit-factor, and Holm
family gates declared by the inherited D1 preregistration. The active A/B/C
contract cannot silently admit D2; its family and multiplicity contract must be
re-frozen first.

No production order placement is authorized.
