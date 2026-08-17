# R4B V2 historical-kline participation proxy

`ParticipationHistoricalKlineProxyV2` is an outcome-blind retrospective
adapter for diagnosing the frozen participation formula against the existing
`data/backtest/futures/*__5m.csv.gz` files. It is not an exact aggregate-trade
M1 reconstruction and must not be used as live producer evidence.

## Frozen proxy mapping

For each fully closed USD-M futures 5m kline, the adapter reads only these two
economic fields:

- `total_trade_notional = quote_volume`
- `normal_notional = quote_volume`
- `signed_normal_notional = 2 * taker_buy_quote_volume - quote_volume`
- `signed_share = signed_normal_notional / total_trade_notional`
- if `quote_volume == 0`, `signed_share = UNKNOWN` (`None`), never neutral zero

The mapping is sealed as `ALL_TRADES_ASSUMED_NORMAL_KLINE_PROXY`. The resulting
`ParticipationFlowBarValueV2` values are passed without an alternate formula to
the shared `calculate_participation_flow_v2` factory: current 5m bar plus exactly
8,640 prior 5m bars.

This assumption is materially weaker than exact aggTrade M1. Klines do not
identify individual trades, aggregate/raw trade ID continuity, normal-quantity
classification, receipt clocks, parser lineage, or causal capture finality.

## Outcome isolation

The builder accepts only:

- an attempt identifier;
- a caller-verified dataset SHA-256 claim;
- the current bar open timestamp;
- an immutable tuple of `Candle` rows.

There is no outcome, forward return, fill, label, threshold, or probability
input, and the module performs no filesystem access. It therefore cannot tune
the participation calculation against the later outcome inside this adapter.
The caller must verify the file and manifest before passing rows. A typical
read-only backtest call path is:

```python
dataset = read_kline_csv(data_path)
manifest = read_dataset_manifest(manifest_path)
verify_dataset_manifest(data_path, manifest)

window = tuple(
    candle
    for candle in dataset.candles
    if first_required_open_ms <= candle.open_time_ms <= current_open_ms
)
proxy = build_participation_historical_kline_proxy_v2(
    attempt_id=attempt_id,
    dataset_sha256=manifest.sha256,
    bar_open_ms=current_open_ms,
    rows=window,
)
```

The adapter binds `dataset_sha256` into the source root. It does not claim to
have verified the external artifact itself.

## Missing rows, gaps, and range

Rows are sorted by `open_time_ms` after validation. Every row must be a closed,
aligned futures `5m` candle for one symbol and must lie inside the exact 8,641
slot window. Duplicate or conflicting slots and out-of-range rows fail closed.

- A missing edge slot produces `UNAVAILABLE_MISSING_SLOT_UNKNOWN`.
- A missing slot between observed rows produces
  `UNAVAILABLE_INTERNAL_GAP_UNKNOWN`.
- Missing slots are listed explicitly and use `UNKNOWN_NOT_ZERO` semantics.
- Calculation runs only when all expected slots are present.
- A present zero-quote-volume candle remains present, but its share is unknown;
  the shared calculation returns `INCONCLUSIVE_DATA`.

All timestamps stay as UTC Unix milliseconds. Decimal arithmetic uses the R4B
V2 protocol context. Non-finite values, inconsistent taker-buy totals, and
arithmetic outside the frozen Decimal domain fail closed.

## Hash domains

The source lineage root includes the dataset digest claim, attempt, exact
window, proxy assumption, outcome-read flag, and every original kline field.

The economic root excludes source-only OHLC/base-volume/trade-count fields,
attempt identity, and dataset digest. It includes only the exact ordered
`quote_volume`/`taker_buy_quote_volume` rows, their proxy derivations, and the
sealed shared bar-value hashes. Thus a source-only metadata/OHLC change changes
the source root but not the economic root; an economic volume change changes
both.

## Authority boundary

Even when the numeric formula is `READY`, every projection seals:

- `historical_diagnostic_only = true`
- `outcome_data_read = false`
- `exact_agg_trade_m1_equivalent = false`
- M0 membership, M1 parser, and M2 finality bindings = `false`
- `causal_inputs_complete = false`
- `producer_ready = false`
- `promoting_eligible = false`
- `probability_eligible = false`
- `data_through_ms`, M0 root, M1 payload, and M2 certificate = `None`

The numeric direction/strength can be compared retrospectively with the other
families, but this adapter cannot emit a live alert, promote a decision, or
claim a calibrated probability.

## P1 follow-up: exact M1 volume at scale

The exact aggTrade M1 projection currently retains complete source lineage and
economic rows simultaneously. A 30-day, high-volume BTC slice can therefore
create a large in-memory object and a very large canonical payload. A vNext P1
should evaluate a streaming verifier plus compact, domain-separated incremental
roots while retaining auditable chunk manifests and exact replay. Do not impose
an arbitrary row cap that rejects otherwise valid BTC history; the resource
contract must be designed and measured first. This proxy does not modify that
M1 path.
