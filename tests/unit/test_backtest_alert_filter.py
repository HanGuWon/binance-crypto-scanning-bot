from __future__ import annotations

from decimal import Decimal

import pytest

from conftest import make_candle, make_feature
from signalbot.backtest.alert_filter import (
    alert_filter_snapshot_at,
    compute_alert_filter_series,
    kaufman_efficiency_ratio_series,
    wilder_dmi_series,
)
from signalbot.domain.enums import Direction, Market
from signalbot.domain.models import Candle, ChartStructureSnapshot


def _candle(
    index: int,
    *,
    high: float,
    low: float,
    close: float,
    market: Market = Market.FUTURES,
    is_closed: bool = True,
) -> Candle:
    return make_candle(
        index,
        market=market,
        close=close,
        is_closed=is_closed,
    ).model_copy(
        update={
            "open": Decimal(str(close)),
            "high": Decimal(str(high)),
            "low": Decimal(str(low)),
            "close": Decimal(str(close)),
        }
    )


def _smooth_candles(size: int) -> list[Candle]:
    return [
        _candle(
            index,
            high=100 + index * 0.3 + (index % 4) * 0.2 + 0.8,
            low=100 + index * 0.3 + (index % 4) * 0.2 - 0.7,
            close=100 + index * 0.3 + (index % 4) * 0.2,
        )
        for index in range(size)
    ]


def test_kaufman_efficiency_ratio_reference_and_boundaries() -> None:
    zigzag = [
        _candle(index, high=close + 0.5, low=close - 0.5, close=close)
        for index, close in enumerate((10.0, 11.0, 10.0, 12.0))
    ]
    flat = [
        _candle(index, high=10.0, low=10.0, close=10.0) for index in range(4)
    ]
    straight = [
        _candle(index, high=close, low=close, close=close)
        for index, close in enumerate((10.0, 11.0, 12.0, 13.0))
    ]

    assert kaufman_efficiency_ratio_series(zigzag, period=3) == (
        None,
        None,
        None,
        0.5,
    )
    assert kaufman_efficiency_ratio_series(flat, period=3)[-1] == 0.0
    assert kaufman_efficiency_ratio_series(straight, period=3)[-1] == 1.0
    assert kaufman_efficiency_ratio_series(zigzag[:3], period=3) == (
        None,
        None,
        None,
    )
    with pytest.raises(ValueError, match="period must be positive"):
        kaufman_efficiency_ratio_series(zigzag, period=0)


def test_wilder_dmi_reference_outside_inside_days_and_adx_delta() -> None:
    candles = [
        _candle(0, high=10, low=8, close=9),
        _candle(1, high=13, low=7, close=10),
        _candle(2, high=12, low=8, close=10),
        _candle(3, high=13, low=6, close=9),
    ]

    result = wilder_dmi_series(candles, period=2)

    assert result.plus_di[:1] == (None,)
    assert result.plus_di[1] == pytest.approx(37.5)
    assert result.plus_di[2] == pytest.approx(18.75)
    assert result.plus_di[3] == pytest.approx(75 / 11)
    assert result.minus_di[1:3] == (0.0, 0.0)
    assert result.minus_di[3] == pytest.approx(200 / 11)
    assert result.adx == pytest.approx((None, None, 100.0, 800 / 11))
    assert result.adx_delta[:3] == (None, None, None)
    assert result.adx_delta[3] == pytest.approx(-300 / 11)


def test_experimental_series_is_prefix_stable_when_future_candles_are_appended() -> None:
    prefix = _smooth_candles(50)
    future = [
        _candle(50, high=180, low=90, close=170),
        _candle(51, high=210, low=130, close=140),
    ]

    prefix_result = compute_alert_filter_series(prefix)
    full_result = compute_alert_filter_series([*prefix, *future])

    assert full_result.close_time_ms[: len(prefix)] == prefix_result.close_time_ms
    for field_name in (
        "efficiency_ratio_20",
        "plus_di_14",
        "minus_di_14",
        "adx_14",
        "adx_delta",
    ):
        prefix_values = getattr(prefix_result, field_name)
        full_values = getattr(full_result, field_name)
        assert full_values[: len(prefix)] == pytest.approx(prefix_values)


def test_snapshot_directional_fields_mirror_and_contractions_increase_as_ratios_fall() -> None:
    candles = _smooth_candles(50)
    series = compute_alert_filter_series(candles)
    last = candles[-1]
    feature = make_feature(
        market=last.market,
        symbol=last.symbol,
        interval=last.interval,
        event_time_ms=last.close_time_ms,
        atr=2.0,
        macd_histogram=0.4,
        macd_histogram_previous=0.1,
        taker_delta_3=0.2,
        taker_imbalance=-0.7,
        volume_zscore=1.5,
        chart_structure=ChartStructureSnapshot(
            pullback_range_ratio=0.6,
            pullback_quote_volume_ratio=0.75,
        ),
    )

    long = alert_filter_snapshot_at(series, feature, Direction.LONG, len(candles) - 1)
    short = alert_filter_snapshot_at(series, feature, Direction.SHORT, len(candles) - 1)

    assert long.directional_di_balance is not None
    assert short.directional_di_balance is not None
    assert long.directional_di_spread is not None
    assert short.directional_di_spread is not None
    assert long.directional_di_balance == pytest.approx(-short.directional_di_balance)
    assert long.directional_di_spread == pytest.approx(-short.directional_di_spread)
    assert long.directional_macd_delta_atr == pytest.approx(0.15)
    assert short.directional_macd_delta_atr == pytest.approx(-0.15)
    assert long.directional_taker_delta == pytest.approx(0.2)
    assert short.directional_taker_delta == pytest.approx(-0.2)
    assert long.directional_taker_delta_source == "taker_delta_3"
    assert long.efficiency_ratio_20 == short.efficiency_ratio_20
    assert long.adx_delta == short.adx_delta
    assert long.volume_zscore == short.volume_zscore == 1.5
    assert long.pullback_range_contraction == pytest.approx(0.4)
    assert long.pullback_volume_contraction == pytest.approx(0.25)


def test_snapshot_fallback_none_and_alignment_boundaries() -> None:
    candles = [_candle(0, high=10, low=9, close=9.5)]
    series = compute_alert_filter_series(candles)
    candle = candles[0]
    feature = make_feature(
        market=candle.market,
        symbol=candle.symbol,
        interval=candle.interval,
        event_time_ms=candle.close_time_ms,
        atr=0.0,
        taker_delta_3=None,
        taker_imbalance=-0.3,
        closed_kline_flow_available=True,
        chart_structure=ChartStructureSnapshot(),
    )

    snapshot = alert_filter_snapshot_at(series, feature, Direction.LONG, 0)

    assert snapshot.efficiency_ratio_20 is None
    assert snapshot.plus_di_14 is None
    assert snapshot.minus_di_14 is None
    assert snapshot.adx_14 is None
    assert snapshot.adx_delta is None
    assert snapshot.directional_di_balance is None
    assert snapshot.directional_di_spread is None
    assert snapshot.directional_macd_delta_atr is None
    assert snapshot.directional_taker_delta == pytest.approx(-0.3)
    assert snapshot.directional_taker_delta_source == "current_taker_imbalance"
    assert snapshot.pullback_range_contraction is None
    assert snapshot.pullback_volume_contraction is None

    unavailable = feature.model_copy(update={"closed_kline_flow_available": False})
    unavailable_snapshot = alert_filter_snapshot_at(
        series, unavailable, Direction.LONG, 0
    )
    assert unavailable_snapshot.directional_taker_delta is None
    assert unavailable_snapshot.directional_taker_delta_source == "unavailable"

    with pytest.raises(ValueError, match="only long or short"):
        alert_filter_snapshot_at(series, feature, Direction.RISK_UP, 0)
    with pytest.raises(ValueError, match="event time"):
        alert_filter_snapshot_at(
            series,
            feature.model_copy(update={"event_time_ms": feature.event_time_ms + 1}),
            Direction.LONG,
            0,
        )
    with pytest.raises(IndexError, match="out of range"):
        alert_filter_snapshot_at(series, feature, Direction.LONG, -1)


def test_experimental_indicators_reject_unclosed_or_mixed_streams() -> None:
    closed = _candle(0, high=10, low=9, close=9.5)
    unclosed = _candle(1, high=11, low=9, close=10, is_closed=False)
    with pytest.raises(ValueError, match="fully closed"):
        compute_alert_filter_series([closed, unclosed])

    mixed_symbol = _candle(1, high=11, low=9, close=10).model_copy(
        update={"symbol": "ETHUSDT"}
    )
    with pytest.raises(ValueError, match="one market, symbol, and interval"):
        compute_alert_filter_series([closed, mixed_symbol])
