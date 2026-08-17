from __future__ import annotations

import math
import sys
from decimal import Decimal
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))
from signalbot.domain.enums import Market  # noqa: E402
from signalbot.domain.models import Candle, FeatureSnapshot, MarketRegime  # noqa: E402


def make_candle(
    index: int,
    *,
    market: Market = Market.SPOT,
    symbol: str = "BTCUSDT",
    interval: str = "5m",
    step_ms: int = 300_000,
    close: float | None = None,
    volume: float = 100.0,
    is_closed: bool = True,
) -> Candle:
    value = close if close is not None else 100 + math.sin(index / 5)
    open_value = value - 0.1
    return Candle(
        market=market,
        symbol=symbol,
        interval=interval,
        open_time_ms=index * step_ms,
        close_time_ms=(index + 1) * step_ms - 1,
        open=Decimal(str(open_value)),
        high=Decimal(str(value + 0.5)),
        low=Decimal(str(value - 0.5)),
        close=Decimal(str(value)),
        volume=Decimal(str(volume)),
        quote_volume=Decimal(str(volume * value)),
        trade_count=100,
        taker_buy_base_volume=Decimal(str(volume * 0.55)),
        taker_buy_quote_volume=Decimal(str(volume * value * 0.55)),
        is_closed=is_closed,
    )


def make_feature(**updates: object) -> FeatureSnapshot:
    values: dict[str, object] = {
        "market": Market.FUTURES,
        "symbol": "TESTUSDT",
        "interval": "5m",
        "event_time_ms": 1_710_000_000_000,
        "price": 100.0,
        "previous_close": 100.0,
        "ema9": 100.0,
        "ema20": 101.0,
        "ema50": 99.0,
        "ema200": 95.0,
        "rsi": 50.0,
        "rsi_previous": 50.0,
        "macd_histogram": 0.1,
        "macd_histogram_previous": 0.0,
        "macd_histogram_previous2": -0.1,
        "atr": 2.0,
        "atr_percent": 2.0,
        "adx": 25.0,
        "bollinger_width": 0.03,
        "bollinger_width_percentile": 50.0,
        "relative_volume": 1.0,
        "recent_high": 105.0,
        "recent_low": 95.0,
        "upper_wick_ratio": 0.1,
        "lower_wick_ratio": 0.1,
        "bearish_divergence": False,
        "bullish_divergence": False,
        "taker_buy_ratio": 0.5,
        "spread_bps": 2.0,
        "regime": MarketRegime(),
    }
    values.update(updates)
    return FeatureSnapshot.model_validate(values)


def make_decision(**updates: object):
    from signalbot.domain.enums import Direction, SignalFamily, SignalStage
    from signalbot.domain.models import SignalDecision

    values: dict[str, object] = {
        "event_id": "event-1",
        "market": Market.FUTURES,
        "symbol": "BTCUSDT",
        "family": SignalFamily.BREAKOUT_LONG,
        "stage": SignalStage.CONFIRMED,
        "direction": Direction.LONG,
        "timeframe": "5m",
        "event_time_ms": 600_000,
        "score": 85,
        "price": Decimal("100"),
        "reasons": ("closed above range", "relative volume 2.0x"),
        "invalidation": Decimal("98"),
        "regime": MarketRegime(label="risk_on", btc_trend="bullish", breadth_ratio=0.7),
        "rule_version": "test-v1",
        "metadata": {},
    }
    values.update(updates)
    return SignalDecision.model_validate(values)
