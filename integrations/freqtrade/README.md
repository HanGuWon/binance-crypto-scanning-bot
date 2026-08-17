# Freqtrade validation sidecar

This directory is deliberately separate from the scanner. It is a dry-run/backtest comparison harness, not a production execution path. The scanner remains public-data-only and alert-first.

The sample futures strategy approximates the closed-candle breakout, breakdown, exhaustion, and capitulation families with pandas-only indicators. Its pandas EWM initialization is not numerically identical to the scanner's seeded EMA/Wilder implementation. It also cannot reproduce the scanner's second-level mini-ticker anomaly detector or aggregate-trade flow, so treat it as a candle-family baseline rather than literal indicator parity.

Typical workflow from a Freqtrade checkout or container:

```bash
freqtrade download-data \
	--config integrations/freqtrade/config-futures-validation-static.json \
	--timeframes 5m 15m 1h

freqtrade backtesting \
	--config integrations/freqtrade/config-futures-validation-static.json \
  --strategy-path integrations/freqtrade/user_data/strategies \
  --strategy SignalParityFuturesStrategy

freqtrade lookahead-analysis \
	--config integrations/freqtrade/config-futures-validation-static.json \
  --strategy-path integrations/freqtrade/user_data/strategies \
  --strategy SignalParityFuturesStrategy

freqtrade recursive-analysis \
	--config integrations/freqtrade/config-futures-validation-static.json \
  --strategy-path integrations/freqtrade/user_data/strategies \
  --strategy SignalParityFuturesStrategy
```

Keep `dry_run` true. This package intentionally contains no exchange credentials. Review current Freqtrade and Binance documentation before use because exchange and framework contracts evolve.

The original `config-futures-dryrun.json` uses a dynamic `VolumePairList` and is
only for observation-mode dry runs. Freqtrade backtesting and FreqAI validation
must use the checked-in static universe config shown above; a current dynamic
universe cannot be reconstructed as a historical point-in-time universe.

## FreqAI technical shadow pipeline

The imported and hardened FreqAI research pipeline lives in
[`reality_sync_freqai/`](reality_sync_freqai/README.md). It is a second,
self-contained sidecar: the original `SignalParityFuturesStrategy` remains the
deterministic 5m parity harness, while `RealitySyncFreqAIStrategy` owns 15m/1h/4h
model research and explainable Discord/Telegram shadow recommendations.

The FreqAI sidecar is pinned to dry-run, carries no live override or exchange
credential path, and does not modify the native scanner under `src/signalbot`.
