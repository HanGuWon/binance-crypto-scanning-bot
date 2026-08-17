from decimal import Decimal

import pytest

from conftest import make_candle
from signalbot.domain.models import Candle
from signalbot.indicators.core import adx_series, atr_series, rsi_series, true_ranges


def _candle(index: int, *, high: float, low: float, close: float) -> Candle:
    return make_candle(index, close=close).model_copy(
        update={
            "open": Decimal(str(close)),
            "high": Decimal(str(high)),
            "low": Decimal(str(low)),
            "close": Decimal(str(close)),
        }
    )


def test_true_range_uses_largest_gap_distance() -> None:
    candles = [
        _candle(0, high=10, low=8, close=9),
        _candle(1, high=12, low=11, close=11.5),
        _candle(2, high=10, low=7, close=8),
    ]

    assert true_ranges(candles) == [2.0, 3.0, 4.5]


def test_atr_uses_sma_seed_then_wilder_recursion() -> None:
    candles = [
        _candle(0, high=10.5, low=9.5, close=10),
        _candle(1, high=11, low=9, close=10),
        _candle(2, high=11.5, low=8.5, close=10),
        _candle(3, high=13, low=7, close=10),
    ]

    result = atr_series(candles, period=3)

    assert result[:2] == [None, None]
    assert result[2] == pytest.approx(2.0)
    assert result[3] == pytest.approx(10 / 3)


def test_rsi_uses_initial_average_then_wilder_recursion() -> None:
    result = rsi_series([10.0, 11.0, 10.0, 12.0, 11.0], period=3)

    assert result[:3] == [None, None, None]
    assert result[3] == pytest.approx(75.0)
    assert result[4] == pytest.approx(600 / 11)


@pytest.mark.parametrize(
    ("values", "expected"),
    [
        ([10.0, 10.0, 10.0, 10.0], 50.0),
        ([10.0, 11.0, 12.0, 13.0], 100.0),
        ([13.0, 12.0, 11.0, 10.0], 0.0),
    ],
)
def test_rsi_flat_rising_and_falling_boundaries(
    values: list[float], expected: float
) -> None:
    assert rsi_series(values, period=3)[-1] == expected


def test_adx_outside_inside_day_reference_and_prefix_stability() -> None:
    candles = [
        _candle(0, high=10, low=8, close=9),
        # Outside day: +DM=3 wins over -DM=1, so only +DM is retained.
        _candle(1, high=13, low=7, close=10),
        # Inside day: both directional movements are zero.
        _candle(2, high=12, low=8, close=10),
        # Outside day: -DM=2 wins over +DM=1, so only -DM is retained.
        _candle(3, high=13, low=6, close=9),
    ]

    prefix = adx_series(candles[:3], period=2)
    full = adx_series(candles, period=2)

    assert prefix == [None, None, 100.0]
    assert full[:3] == prefix
    assert full[3] == pytest.approx(800 / 11)


def test_adx_non_positive_period_returns_unavailable_values() -> None:
    candles = [_candle(0, high=10, low=8, close=9)]

    assert adx_series(candles, period=0) == [None]
    assert adx_series(candles, period=-1) == [None]
