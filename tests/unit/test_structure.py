from decimal import Decimal

import pytest
from pydantic import ValidationError

from conftest import make_candle
from signalbot.domain.models import ChartStructureSnapshot
from signalbot.indicators.structure import (
    MINIMUM_PROMINENCE_ATR,
    PIVOT_TTL_BARS,
    ConfirmedPivot,
    _qualified_pivots,
    chart_structure_snapshot,
    confirmed_pivots,
)


def _candle(
    index: int,
    *,
    high: float,
    low: float,
    close: float,
    quote_volume: float = 1_000,
):
    return make_candle(index, close=close).model_copy(
        update={
            "open": Decimal(str((high + low) / 2)),
            "high": Decimal(str(high)),
            "low": Decimal(str(low)),
            "close": Decimal(str(close)),
            "quote_volume": Decimal(str(quote_volume)),
            "taker_buy_quote_volume": Decimal(str(quote_volume * 0.55)),
        }
    )


def _bullish_pullback_candles():
    rows = [
        (10.0, 9.0, 9.5),
        (11.0, 10.0, 10.5),
        (14.0, 11.0, 13.0),
        (12.0, 10.0, 11.0),
        (11.0, 8.0, 9.0),
        (14.0, 10.0, 13.0),
        (18.0, 12.0, 17.0),
        (17.0, 16.0, 16.5),
        (16.0, 15.0, 15.5),
        (15.5, 14.0, 15.0),
        (16.2, 15.0, 15.8),
        (16.5, 15.5, 16.0),
        (17.5, 16.0, 17.0),
    ]
    return [
        _candle(index, high=high, low=low, close=close)
        for index, (high, low, close) in enumerate(rows)
    ]


def test_pivot_confirmation_is_delayed_and_plateau_uses_earliest_point() -> None:
    candles = [
        _candle(index, high=high, low=low, close=(high + low) / 2)
        for index, (high, low) in enumerate(
            [(2, 1), (3, 2), (6, 3), (6, 4), (5, 3), (4, 2)]
        )
    ]

    highs = [pivot for pivot in confirmed_pivots(candles) if pivot.kind == "high"]

    assert [(pivot.index, pivot.availability_index) for pivot in highs] == [(2, 4)]


def test_pivot_confirmation_rejects_an_open_right_confirmation_bar() -> None:
    candles = [
        _candle(index, high=high, low=low, close=(high + low) / 2)
        for index, (high, low) in enumerate([(2, 1), (3, 2), (6, 3), (5, 3), (4, 2)])
    ]
    candles[-1] = candles[-1].model_copy(update={"is_closed": False})

    with pytest.raises(ValueError, match="fully closed candle sequence"):
        confirmed_pivots(candles)


def test_qualified_pivot_index_slice_matches_the_causal_full_scan() -> None:
    pivots = tuple(
        ConfirmedPivot(
            kind="high" if index % 2 else "low",
            index=index,
            price=100.0 + index,
            availability_index=index + 2,
            prominence=1.0 if index % 3 else 0.5,
        )
        for index in range(2, 250, 3)
    )
    atr = [1.0] * 260

    for reference_index in range(1, 259):
        first_index = max(0, reference_index - PIVOT_TTL_BARS + 1)
        expected = [
            pivot
            for pivot in pivots
            if pivot.availability_index <= reference_index
            and pivot.index >= first_index
            and pivot.prominence / atr[pivot.availability_index]
            >= MINIMUM_PROMINENCE_ATR
        ]
        assert _qualified_pivots(
            pivots,
            atr,
            reference_index=reference_index,
        ) == expected


def test_chart_structure_freezes_pivots_at_t_minus_one() -> None:
    candles = _bullish_pullback_candles()
    atr = [1.0] * len(candles)
    ema20 = [14.0] * len(candles)

    before_extra_freeze = chart_structure_snapshot(
        candles,
        index=11,
        atr=atr,
        ema20=ema20,
    )
    after_extra_freeze = chart_structure_snapshot(
        candles,
        index=12,
        atr=atr,
        ema20=ema20,
    )

    assert before_extra_freeze.state == "unavailable"
    assert after_extra_freeze.state == "bullish"
    assert after_extra_freeze.latest_swing_high == 18.0
    assert after_extra_freeze.latest_swing_low == 14.0


def test_bullish_pullback_reports_raw_metrics_and_ready_recovery() -> None:
    candles = _bullish_pullback_candles()
    atr = [1.0] * len(candles)
    ema20 = [14.0] * len(candles)

    result = chart_structure_snapshot(candles, index=12, atr=atr, ema20=ema20)

    assert result.state == "bullish"
    assert result.swing_high_change_atr == 4.0
    assert result.swing_low_change_atr == 6.0
    assert result.pullback_direction == "long"
    assert result.pullback_status == "ready"
    assert result.impulse_start == 8.0
    assert result.impulse_end == 18.0
    assert result.impulse_size_atr == 10.0
    assert result.pullback_depth == 0.4
    assert result.pullback_duration_bars == 6
    assert result.confluence_distance_atr == 0.0
    assert result.recovery_confirmed
    assert result.structure_intact


def test_chart_structure_at_index_is_unchanged_by_later_candles() -> None:
    candles = _bullish_pullback_candles()
    atr = [1.0] * len(candles)
    ema20 = [14.0] * len(candles)
    before = chart_structure_snapshot(candles, index=12, atr=atr, ema20=ema20)
    future = _candle(13, high=100, low=1, close=50)

    after = chart_structure_snapshot(
        [*candles, future],
        index=12,
        atr=[*atr, 1.0],
        ema20=[*ema20, 14.0],
    )

    assert after == before


def test_chart_structure_rejects_an_open_candle_in_the_used_prefix() -> None:
    candles = _bullish_pullback_candles()
    candles[-1] = candles[-1].model_copy(update={"is_closed": False})

    with pytest.raises(ValueError, match="requires closed candles"):
        chart_structure_snapshot(
            candles,
            index=12,
            atr=[1.0] * len(candles),
            ema20=[14.0] * len(candles),
        )


def test_pullback_normalization_is_frozen_at_the_historical_event() -> None:
    candles = [
        *_bullish_pullback_candles(),
        _candle(13, high=18.0, low=16.0, close=17.6),
    ]
    atr = [1.0] * len(candles)
    ema20 = [14.0] * len(candles)
    atr[12] = 6.0
    ema20[12] = 100.0

    result = chart_structure_snapshot(candles, index=13, atr=atr, ema20=ema20)

    assert result.state == "bullish"
    assert result.impulse_size_atr == 10.0
    assert result.confluence_distance_atr == 0.0
    assert result.pullback_status == "ready"


@pytest.mark.parametrize("value", [float("nan"), float("inf"), float("-inf")])
def test_chart_structure_model_rejects_non_finite_metrics(value: float) -> None:
    with pytest.raises(ValidationError):
        ChartStructureSnapshot(swing_high_change_atr=value)


@pytest.mark.parametrize("value", [float("nan"), float("inf"), float("-inf")])
def test_chart_structure_rejects_non_finite_reference_series(value: float) -> None:
    candles = _bullish_pullback_candles()
    atr = [1.0] * len(candles)
    ema20 = [14.0] * len(candles)
    ema20[11] = value

    with pytest.raises(ValueError, match="must be finite"):
        chart_structure_snapshot(candles, index=12, atr=atr, ema20=ema20)
