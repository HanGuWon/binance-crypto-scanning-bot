# Implementation and Packaging Plan

Assumption: implementation and verification run in a Linux sandbox. All authored files live under `/mnt/data/workdir`; the final distributable is `/mnt/data/result.zip`.

## Durable checklist

- [x] 1. Freeze scope, invariants, source references, and repository layout.
- [x] 2. Create the Python package, configuration model, CLI, and domain contracts.
- [x] 3. Implement Binance public REST/WebSocket adapters, universe selection, parsing, reconnect, and deduplication.
- [x] 4. Implement candle history, indicators, regime context, signal rules, scoring, and state transitions.
- [x] 5. Implement Discord delivery, persistence, replay/outcome utilities, fixtures, Freqtrade sidecar, API, and operational files.
- [x] 6. Add unit/contract/integration/replay tests and run static/runtime verification.
- [x] 7. Review independently for lookahead, timestamp, duplicate-event, bounded-memory, route, and secret-handling defects.
- [x] 8. Remove generated artifacts, update this plan, create the single source ZIP, and audit its contents and path.

## Implemented sequence

1. Kept V1 alert-only: public Binance market data, no Binance API key, and no order placement.
2. Added separate Binance Spot and USDⓈ-M scanner instances, current routed Futures `/market` and `/public` WebSocket URLs, bounded stream batches, planned connection recycling, reconnect backoff, and payload fixtures.
3. Added liquidity-ranked tradable and surveillance universes, closed-candle storage, gap detection/recovery, aggregate-trade flow, spread state, and bounded intrabar anomaly windows.
4. Added EMA, RSI, MACD histogram, ATR, ADX, Bollinger width, relative volume, range structure, wick, divergence, market-regime, and higher-timeframe features without future-candle access.
5. Added squeeze, breakout, breakdown, exhaustion, capitulation, pump-risk, and crash-risk scoring; deterministic state transitions; SQL persistence; idempotent Discord delivery; replay; MFE/MAE evaluation; read-only API; Docker Compose; and a Freqtrade dry-run/backtest sidecar.
6. Added provider-contract, unit, integration, replay, CLI, persistence, API, WebSocket, Discord, and anti-lookahead tests.

## Verification executed

The following commands were executed successfully in the Linux sandbox:

```text
uv sync --extra dev
uv run ruff check .
uv run pyright
uv run pytest -q
uv run python -m compileall -q src tests integrations/freqtrade/user_data/strategies
uv run signalbot validate-config --config config/settings.example.yaml
uv run signalbot run --config config/settings.example.yaml --dry-run
uv run signalbot replay --config config/settings.example.yaml --market spot --input tests/fixtures/replay/sample_events.jsonl
uv run signalbot evaluate-outcomes --config config/settings.example.yaml --input tests/fixtures/outcomes/sample.json --horizons 900 3600
uv build
```

Final results before cleanup: Ruff passed; Pyright reported 0 errors and 0 warnings; Pytest reported 48 passed; package compilation passed; all four CLI outputs parsed as valid JSON; source and wheel builds succeeded.

An independent source audit also rejected any negative time shift, centered rolling window, backfill marker, or legacy unrouted Futures WebSocket base in executable source. Manual review confirmed closed-candle gating, timestamp-bounded higher-timeframe contexts, range/volume baselines excluding the signal candle, deterministic replay time, bounded deques/WebSocket queues, provider parsing normalization, Discord secret masking, and current `/market` versus `/public` route separation.

## Packaging criteria

- Include only authored source, tests, fixtures, configuration, documentation, manifests, CI, and operational text files.
- Exclude virtual environments, dependency trees, lock/build output, caches, bytecode, coverage, runtime databases, logs, and VCS metadata.
- Delete every existing `/mnt/data/*.zip` immediately before final packaging.
- Create exactly `/mnt/data/result.zip` with `container.exec` from the contents of `/mnt/data/workdir`, so `PLAN.md` is at ZIP root.
- Audit the archive for excluded paths and required `PLAN.md`.
- Run `find /mnt/data -maxdepth 1 -name "*.zip" -print` and require the sole output to be `/mnt/data/result.zip`.
