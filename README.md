# Binance Signal Bot

An executable, alert-first Python service for Binance Spot and USDⓈ-M perpetual markets. It discovers liquid USDT instruments, bootstraps closed candles, consumes public WebSocket streams, computes multi-timeframe features, detects rapid intrabar anomalies, and emits evidence-based Discord alerts.

This repository intentionally **does not place orders**. Spot sell/exit suggestions and futures short suggestions are distinct. Public market data requires no Binance API key.

## Implemented vertical slice

- Binance Spot and USDⓈ-M public REST adapters.
- Current routed Futures WebSocket endpoints (`/public` and `/market`).
- Liquidity-ranked tradable universe plus broad surveillance universe.
- Closed-candle-only calculation for 1m, 5m, 15m, and 1h; feature and structure APIs reject an open candle anywhere in the prefix they use.
- EMA, RSI, MACD histogram, ATR, ADX, Bollinger-width percentile, relative volume, candle structure, divergence, spread, and canonical closed-kline taker-flow features.
- Causal HH/HL or LH/LL structure from prominence-qualified 2x2 pivots, low-confidence projected trendline diagnostics, and ATR-normalized pullback/recovery measurements. Pivot references are frozen at `t-1`; impulse ATR is frozen at pivot confirmation and EMA/ATR confluence at the pullback extremum; active ZigZag legs are never used.
- Optional point-in-time volume ablations: normalized 3/12-bar taker delta and ATR-normalized quote-volume VPCI. Last-60s `aggTrade` flow remains separately labeled intrabar evidence.
- Squeeze, breakout, breakdown, exhaustion, capitulation, pump-risk, and crash-risk rules, plus informational-only causal pullback WATCH/SETUP alerts.
- Weighted scoring, state transitions, cooldown, deterministic event IDs, SQL persistence, optional Discord, replay, and MFE/MAE evaluation.
- Discord candle alerts show one promotion-safe representative long and short rule beside the closed-candle EMA, RSI, MACD, ADX, ATR, volume, flow, spread, funding, confirmed swings, projected lines, and pullback values used at decision time. Different rule scores are not subtracted into a directional edge.
- Discord payloads enforce component limits and the shared 6,000-character embed budget; usernames and HTTPS webhook URLs are validated at configuration time.
- SQLite for zero-setup local runs; PostgreSQL through Docker Compose.

## Quick start

```bash
cp config/settings.example.yaml config/settings.yaml
cp .env.example .env
uv sync --extra dev
uv run signalbot validate-config --config config/settings.yaml
uv run signalbot run --config config/settings.yaml --dry-run
uv run signalbot run --config config/settings.yaml
```

Set `SIGNALBOT_DISCORD_WEBHOOK_URL` to enable Discord. Without it, decisions are logged and persisted. `docker compose up --build` starts the scanner, PostgreSQL, and a read-only API bound to `127.0.0.1:8080`.

```bash
uv run signalbot replay --config config/settings.example.yaml --market spot --input tests/fixtures/replay/sample_events.jsonl
uv run signalbot evaluate-outcomes --config config/settings.example.yaml --input tests/fixtures/outcomes/sample.json --horizons 900 3600
uv run signalbot serve-api --config config/settings.example.yaml --host 0.0.0.0 --port 8080
```

## Cost-aware research backtest

The research runner downloads only public Binance market data. It never reads an
account, places an order, or sends a Discord alert.

```bash
uv run signalbot backtest-download \
  --spec config/backtest.research.yaml \
  --data-dir data/backtest \
  --concurrency 2

uv run signalbot backtest-run \
  --config config/settings.example.yaml \
  --spec config/backtest.research.yaml \
  --data-dir data/backtest \
  --output-dir artifacts/backtest/latest
```

`signals/positions.py` owns the paper-position technical exit policy: structural
invalidation, next-bar trend/opposite-signal exits, delayed ATR trailing stops,
and a maximum holding period. `backtest/engine.py` executes signals at the next
bar open, handles long/short signs separately, and deducts fees, adverse slippage,
and public futures funding. Each compressed dataset has a deterministic SHA-256
manifest; exchange gaps remain explicit and are never filled from another venue.

To audit the recommendations that the current Discord lifecycle would index,
including informational pullbacks that the frozen breakout studies intentionally
exclude, run the separate alert replay. It records the first informational
`SETUP` per pullback episode and actionable `CONFIRMED` transitions without
promoting either one into an exchange order:

```bash
uv run signalbot backtest-alert-replay \
  --config config/settings.example.yaml \
  --spec config/backtest.5m.research.yaml \
  --data-dir data/backtest \
  --output-dir artifacts/backtest/alert-replay \
  --split retrospective_test
```

The audit writes `recommendations.csv`, long-form `outcomes.csv`, `results.json`,
`report_ko.md`, and a hash manifest. Its primary label is the after-cost signed
return at 12 bars with a symmetric 5-bp no-call zone. It also reports 3/6/72-bar
returns, MFE/MAE, maximum rise/drop, and whether a 1R target or the recorded
invalidation was touched first. A target/stop collision inside one OHLC bar is
ambiguous, never a win. Spot short rows are decline/exit-warning diagnostics,
not realizable spot-short P&L. The supplied 5-minute spec is a fixed eight-asset
research universe; it does not claim coverage of every symbol that the live
dynamic top-N selector may rotate into.

The frozen 5m volume study runs one corrected price-trigger control and two
single-feature ablations. Each writes `opportunities.csv` so rejected triggers
remain in the same analysis panel:

```bash
uv run signalbot backtest-run --config config/settings.example.yaml --spec config/backtest.5m.volume-c0.yaml --data-dir data/backtest --output-dir artifacts/backtest/volume-r1/c0
uv run signalbot backtest-run --config config/settings.example.yaml --spec config/backtest.5m.volume-g2.yaml --data-dir data/backtest --output-dir artifacts/backtest/volume-r1/g2
uv run signalbot backtest-run --config config/settings.example.yaml --spec config/backtest.5m.volume-g4.yaml --data-dir data/backtest --output-dir artifacts/backtest/volume-r1/g4
uv run signalbot backtest-volume-compare --spec config/backtest.5m.volume-c0.yaml --c0-dir artifacts/backtest/volume-r1/c0 --g2-dir artifacts/backtest/volume-r1/g2 --g4-dir artifacts/backtest/volume-r1/g4 --output artifacts/backtest/volume-r1/comparison.json
```

In explicit-trigger mode, gates can accept or reject a completed breakout or
breakdown but cannot promote a squeeze/setup score into an entry.

The frozen example is a 1h research experiment. It does not claim exact parity
with the production example's 5m plus higher-timeframe microstructure path.

## Interpretation

High RSI is a setup condition, not an automatic short. Exhaustion confirmation requires corroborating price structure, momentum deterioration, order flow, trend, and liquidity. Discord reports one representative rule per direction and does not add correlated setup families or subtract unlike rule scales into a directional edge. Scores are rule-strength values, **not calibrated probabilities**.

The example enables causal pullback diagnostics in `informational` mode. Even a
100/100 pullback rule remains `SETUP`, is marked `진입 승인 아님`, and cannot
become a confirmed entry in either confirmation mode, even when entry gates are
disabled. Its 0.75-ATR pivot prominence, 2-ATR impulse,
20–60% depth, 12-bar duration, and 0.25-ATR proximity values are transparent
research seeds, not universal market constants.

Candle decisions use only kline payloads with `x=true`. Rapid all-market price moves are separate `PUMP_RISK` or `CRASH_RISK` warnings rather than guaranteed advance predictions.

## Verification

```bash
uv run ruff check .
uv run pyright
uv run pytest -q
python -m compileall -q src tests
```

Read `PLAN.md` and `docs/` for architecture, signal semantics, operations, and verified primary references.
