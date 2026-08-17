import random

from conftest import make_candle, make_feature
from signalbot.domain.enums import Market
from signalbot.regime.market import MarketRegimeEngine


def test_same_close_breadth_and_btc_trend_are_strictly_excluded() -> None:
    engine = MarketRegimeEngine()
    engine.update_candle(make_candle(0, close=100))
    engine.update_candle(make_candle(1, close=101))
    engine.update_feature(
        make_feature(
            market=Market.SPOT,
            symbol="BTCUSDT",
            interval="1h",
            event_time_ms=599_999,
            price=105,
            ema20=102,
            ema50=100,
        )
    )

    same_close = engine.snapshot(Market.SPOT, 599_999)
    assert same_close.breadth_ratio == 0.5
    assert same_close.btc_trend == "neutral"

    strictly_after = engine.snapshot(Market.SPOT, 600_000)
    assert strictly_after.breadth_ratio == 1.0
    assert strictly_after.btc_trend == "bullish"
    assert strictly_after.label == "risk_on"


def test_regime_snapshot_is_independent_of_cross_symbol_arrival_order() -> None:
    candles = [
        make_candle(index, symbol=symbol, close=price)
        for symbol, prices in (
            ("BTCUSDT", (100, 101, 102)),
            ("ETHUSDT", (100, 99, 98)),
        )
        for index, price in enumerate(prices)
    ]
    expected = None
    for seed in range(10):
        shuffled = list(candles)
        random.Random(seed).shuffle(shuffled)
        engine = MarketRegimeEngine()
        for candle in shuffled:
            engine.update_candle(candle)
        snapshot = engine.snapshot(Market.SPOT, 1_000_000)
        expected = expected or snapshot
        assert snapshot == expected
    assert expected is not None
    assert expected.breadth_ratio == 0.5


def test_regime_histories_are_bounded_and_rotation_prunes_removed_symbols() -> None:
    engine = MarketRegimeEngine(maximum_points_per_symbol=2)
    for index in range(3):
        engine.update_candle(make_candle(index, close=100 + index))
    engine.update_candle(make_candle(0, symbol="ETHUSDT", close=100))
    engine.update_candle(make_candle(1, symbol="ETHUSDT", close=99))

    # The oldest BTC point was pruned, so only one point exists before this boundary.
    assert engine.snapshot(Market.SPOT, 600_000).breadth_ratio == 0.0
    assert engine.retain_symbols(Market.SPOT, frozenset({"BTCUSDT"})) == 1
    assert engine.snapshot(Market.SPOT, 1_000_000).breadth_ratio == 1.0
