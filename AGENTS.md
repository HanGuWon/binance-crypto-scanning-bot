# Project mission

Build an alert-first Binance market signal service. Detect technical setups, rapid market anomalies, and possible long/short opportunities, then send evidence-based Discord alerts. Do not add production order execution without a later explicit requirement.

# Non-negotiable invariants

- V1 uses public Binance market data only and requires no Binance API key.
- Never implement production order placement in this scanner.
- Store timestamps as Unix milliseconds in UTC; show UTC and Asia/Seoul in alerts.
- Candle signals may use only fully closed candles.
- Intrabar warnings are explicitly `PUMP_RISK` or `CRASH_RISK`.
- Never use future rows, centered windows, or unclosed higher-timeframe candles.
- Spot exits and futures shorts are different actions.
- Every decision includes reasons, invalidation, rule version, and deterministic event ID.
- Reconnects must not duplicate candles, decisions, trades, or Discord messages.
- Queues, deques, and caches must be bounded or pruned.
- Secrets are environment variables and must never be logged.
- Use current Binance routed USDⓈ-M WebSocket paths.

# Engineering standards

- Python 3.12+, uv, Ruff, Pyright, pytest.
- Public functions and domain models require type annotations.
- No network calls in unit tests; use recorded JSON fixtures.
- New rules require positive, negative, and boundary tests.
- Retry loops need a cap or cancellation path.
- No bare `except`; do not silently swallow exceptions.

# Verification

`uv run ruff check .`, `uv run pyright`, `uv run pytest -q`, and `python -m compileall -q src tests`.
