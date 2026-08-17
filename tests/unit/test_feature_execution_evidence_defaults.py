from conftest import make_candle
from signalbot.config import SignalSettings
from signalbot.indicators.core import FeatureEngine


def test_historical_feature_series_does_not_invent_zero_spread() -> None:
    candles = [make_candle(index) for index in range(220)]
    features = FeatureEngine(SignalSettings()).compute_series(candles)

    latest = features[-1]
    assert latest is not None
    assert latest.spread_bps is None
    assert latest.spread_is_proxy is False
    assert latest.book_age_ms is None
    assert latest.bid_quote_capacity is None
    assert latest.ask_quote_capacity is None
