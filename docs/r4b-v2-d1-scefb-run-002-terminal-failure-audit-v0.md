# D1 SCEFB run-002 terminal-failure audit v0

This post-terminal record documents the immutable failure of
`d1-scefb-v0-development-run-002`. It was written after the permanent START
record and terminal `FAILED` record. The diagnosis below is development-observed
information. It is not a D1 result, a repair of D1, or evidence of strategy
efficacy.

## Terminal disposition

The one authorized run-002 invocation crossed START and terminated without a
result or artifact manifest. Its durable state sequence is:

`ARMED -> STARTED_BEFORE_OUTCOME_ACCESS -> FAILED`

The terminal detail code is `RUN_OR_PUBLICATION_FAILED_NO_RETRY`. The result
SHA-256 and artifact-manifest SHA-256 are both null. The intended output and
staging paths are absent. Run-002 is permanently consumed and must not be
retried, resumed, relabelled, or replaced at the same paths.

The exact local evidence bindings are:

| Evidence | Local path or role | SHA-256 |
| --- | --- | --- |
| Funding authority file (1,729 bytes) | `artifacts/backtest/2026-07-21-d1-scefb-v0-input-authority/funding_authority.jsonl` | `b128bf30c6f23141e638248e47352eee4b6532317e5c8379cc04a262228fb4e8` |
| Input-authority file (4,550 bytes) | `artifacts/backtest/2026-07-21-d1-scefb-v0-input-authority/input_authority.jsonl` | `f22655f7a3327ed176c5bdcffb565914fe0807586338f688253208a7ea7cabd5` |
| Input-authority domain | canonical authority payload | `c33a77f4223dcf2b90fbf79853beb4818af105ccb65bf248daa273a3a4089f62` |
| Retired pre-arm freeze-001 | `artifacts/backtest/2026-07-21-d1-scefb-v0-development-freeze/freeze_manifest.json` | `328899911e4b1dd3acd9f12b5f1d8cd1f08f5df08b55d350670215649efa8316` |
| Active run-002 freeze (231 files) | `artifacts/backtest/2026-07-21-d1-scefb-v0-development-freeze-002/freeze_manifest.json` | `bdf6f495762371281a137c32d57066602578a47598303d2ce4830d5e977b161a` |
| Preregistration bound by the WAL | `docs/r4b-v2-d1-scefb-5m-preregistration-v0.md` | `af69c262282144432e6adbf1e01406c7334e37176dd83ce6f9666adc49b6899d` |
| Attempt WAL file (4,069 bytes) | `artifacts/backtest/2026-07-21-d1-scefb-v0-development-run-002-attempt/attempt.wal` | `59e65fcc54e648444b16449952c262c277f712e0c024180f514882e7963f2130` |
| ARMED record | WAL sequence 0 | `5cc98c47b03e58b74aa907a75f5d621ed6239c0d23b9726736e9a91b6f05aa0a` |
| START record | WAL sequence 1 | `1eb5d24f79c43bbdb80e7fdcb479a606fa92be6aa76e95c657f09509ecbe4c5d` |
| START seal file (897 bytes) | `artifacts/backtest/2026-07-21-d1-scefb-v0-development-run-002-attempt/start.seal` | `88e8f73b5fa90b398f783907524d4fb825b88a59cbd7d7fc67352d8c0524c91d` |
| START seal payload | `seal_sha256` inside `start.seal` | `f6f23e0cfd8207970f860c9d430f71b3a4c46628c41e989dde8c4917de6e15c0` |
| Terminal FAILED record | WAL sequence 2 | `81948df00e0a11812d9088239712d145ba8ce0daa21fffefe4ab06573626b369` |
| Failure-evidence manifest (257 entries) | `artifacts/backtest/2026-07-21-d1-scefb-v0-development-run-002-failure-evidence/evidence-manifest.jsonl` | `15988eec55f311cfc95273eca17848328a6fa24ab8b315f9c354c3e869a51e72` |
| Failure-evidence seal file | `artifacts/backtest/2026-07-21-d1-scefb-v0-development-run-002-failure-evidence/evidence.seal` | `db2dfbe5575667a49987ff92bea419680eae06d560eb295b733563f42bf586ba` |
| Frozen failure-evidence archive | `artifacts/backtest/2026-07-21-d1-scefb-v0-development-run-002-failure-evidence/frozen-failure-evidence.zip` | `f44e4c38aefeb5542c8875e3625ab01e82cde1fd4ff7738e26684b9895a25592` |

The evidence archive preserves the 231-member active freeze, the attempt
protocol evidence, the authority files, all 20 kline sidecar manifests, and the
preservation tool. Its manifest explicitly records
`rerunnable_outcome_bundle=false` and `outcome_data_gzip_included=false`.
Therefore the archive is a failure reconstruction and chain-of-custody bundle,
not a standalone rerunnable outcome-data package.

## Post-terminal failure diagnosis

The runner stopped during input consistency validation, before strategy
calculation. It required each native Binance USD-M 1h close to equal the close
of the final authenticated 5m candle in the corresponding hour. The first
failure was BTCUSDT at `2024-10-28T20:00:00Z` through
`2024-10-28T20:59:59.999Z` (`2024-10-29 05:00` Asia/Seoul): native 1h close
`69500.00`, final 5m close `69623.00`, delta `-123.00`. The symbol and interval
timestamps aligned; the close values did not.

A read-only post-terminal census checked the complete authority-bound inputs
for all ten symbols. Each symbol has 245,376 authenticated 5m rows and 20,448
authenticated native 1h rows. There were zero timestamp mismatches and exactly
two close mismatches per symbol: 20 of 204,480 compared hourly closes, or
0.0097814%. All 20 occurred at the same two hourly opens:

- `2024-10-28T20:00:00Z` (`2024-10-29 05:00` Asia/Seoul)
- `2024-10-28T21:00:00Z` (`2024-10-29 06:00` Asia/Seoul)

The values below are `native 1h close -> final 5m close`:

| Symbol | 20:00 UTC | 21:00 UTC | Close mismatches / compared hours |
| --- | ---: | ---: | ---: |
| BTCUSDT | `69500.00 -> 69623.00` | `69650.00 -> 69770.20` | `2 / 20,448` |
| ETHUSDT | `2516.41 -> 2515.97` | `2518.92 -> 2543.70` | `2 / 20,448` |
| BNBUSDT | `598.170 -> 598.300` | `598.660 -> 601.340` | `2 / 20,448` |
| SOLUSDT | `176.3100 -> 176.5500` | `176.5500 -> 177.5200` | `2 / 20,448` |
| XRPUSDT | `0.5185 -> 0.5182` | `0.5172 -> 0.5191` | `2 / 20,448` |
| DOGEUSDT | `0.156990 -> 0.157940` | `0.159300 -> 0.160780` | `2 / 20,448` |
| ARBUSDT | `0.516200 -> 0.516800` | `0.517500 -> 0.522800` | `2 / 20,448` |
| OPUSDT | `1.5790000 -> 1.5792000` | `1.5818000 -> 1.6123000` | `2 / 20,448` |
| SUIUSDT | `1.715300 -> 1.711900` | `1.711500 -> 1.722900` | `2 / 20,448` |
| WIFUSDT | `2.3628000 -> 2.3679000` | `2.3898000 -> 2.4987000` | `2 / 20,448` |

As a development-observed corroboration only, a post-terminal requery of the
current public Binance USD-M `/fapi/v1/klines` endpoint reproduced the first
BTC discrepancy. The 1h request for open time `1730145600000` returned close
`69500.00`; the 5m request for the final candle open time `1730148900000`
returned close `69623.00`. The relevant public requests were:

- `https://fapi.binance.com/fapi/v1/klines?symbol=BTCUSDT&interval=1h&startTime=1730145600000&limit=1`
- `https://fapi.binance.com/fapi/v1/klines?symbol=BTCUSDT&interval=5m&startTime=1730148900000&limit=1`

This mutable endpoint observation was not preregistered, was not bound into the
D1 input authority, and is not included as response bytes in the failure archive.
It corroborates the diagnosed cross-interval source contradiction; it does not
convert the failed run into an outcome or establish a general Binance semantic
guarantee.

## Explicit non-claims

No D1 strategy result was produced. In particular, this record makes no claim
about return, expected value, hit rate, drawdown, statistical significance,
signal probability, efficacy, profitability, or promotion eligibility. The
0.0097814% figure is only an input-consistency diagnostic. It is not an error
rate for the strategy and not evidence for or against trading performance.

No production order was placed. The alert-first and public-market-data-only
boundaries remain unchanged.

## D2 contamination and validation boundary

Selecting authenticated closed 5m candles as the sole price-candle authority
and deriving complete aligned 1h bars from them is a response to information
observed after D1 START. It is therefore an outcome-informed data-policy change
and defines a distinct D2 development family. It must never be represented as a
D1 retry, amendment, result, or efficacy-preserving repair.

Any D2 implementation must, at minimum:

- derive a 1h bar only from all 12 fully closed, contiguous, aligned 5m candles;
- reject gaps, duplicates, partial hours, and unclosed 5m or derived 1h candles;
- make a derived hour available only after its final 5m candle is closed;
- give the data-authority and derivation policy new version identifiers;
- receive a new preregistration, fresh code freeze, input authority, run ID,
  attempt path, and output path; and
- retain native 1h data, if used at all, as diagnostic evidence rather than an
  equality gate or alternate feature authority.

All D2 evaluation on the already inspected historical interval is development
and contamination-aware. It may diagnose implementation behavior, but it cannot
provide untouched confirmation. After D2 is frozen, any efficacy or net-positive
expectancy claim requires a strictly later, previously unused prospective
PAPER/BBO interval under the frozen policy, including the preregistered trading
costs, execution rules, multiplicity controls, and qualification gates. No
production order execution is authorized by this boundary.
