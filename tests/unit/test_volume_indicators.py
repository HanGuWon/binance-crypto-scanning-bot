from __future__ import annotations

import math
from decimal import Decimal

import pytest

from conftest import make_candle
from signalbot.domain.models import Candle
from signalbot.indicators.volume import (
    compute_normalized_taker_delta,
    compute_normalized_vpci,
    normalized_taker_delta_series,
    normalized_vpci_series,
    normalized_vpci_unavailable_reason,
    taker_delta_unavailable_reason,
)


def _with_quote_flow(candle: Candle, quote: float, buy_fraction: float) -> Candle:
    return candle.model_copy(
        update={
            "quote_volume": Decimal(str(quote)),
            "taker_buy_quote_volume": Decimal(str(quote * buy_fraction)),
        }
    )


def _vpci_candles(*, increasing_volume: bool, count: int = 30) -> list[Candle]:
    candles = []
    for index in range(count):
        quote = float(index + 1 if increasing_volume else count - index) * 1_000
        candles.append(_with_quote_flow(make_candle(index, close=100 + index), quote, 0.55))
    return candles


def test_taker_delta_positive_and_negative_direction() -> None:
    positive = [
        _with_quote_flow(make_candle(index), 100, 0.5 if index < 9 else 0.8)
        for index in range(12)
    ]
    negative = [_with_quote_flow(make_candle(index), 100, 0.2) for index in range(12)]

    positive_result = compute_normalized_taker_delta(positive)
    negative_result = compute_normalized_taker_delta(negative)

    assert positive_result is not None
    assert positive_result.d3 == pytest.approx(0.6)
    assert positive_result.d12 == pytest.approx(0.15)
    assert negative_result is not None
    assert negative_result.d3 == pytest.approx(-0.6)
    assert negative_result.d12 == pytest.approx(-0.6)


def test_taker_delta_requires_twelve_contiguous_closed_candles() -> None:
    eleven = [_with_quote_flow(make_candle(index), 100, 0.6) for index in range(11)]
    twelve = [*eleven, _with_quote_flow(make_candle(11), 100, 0.6)]
    gap = list(twelve)
    gap[6] = gap[6].model_copy(
        update={
            "open_time_ms": gap[6].open_time_ms + 300_000,
            "close_time_ms": gap[6].close_time_ms + 300_000,
        }
    )
    unclosed = [*twelve[:-1], twelve[-1].model_copy(update={"is_closed": False})]

    assert compute_normalized_taker_delta(eleven) is None
    assert compute_normalized_taker_delta(twelve) is not None
    assert compute_normalized_taker_delta(gap) is None
    assert compute_normalized_taker_delta(unclosed) is None


def test_taker_delta_rejects_zero_denominator_and_invalid_buy_volume() -> None:
    zero = [_with_quote_flow(make_candle(index), 0, 0) for index in range(12)]
    zero_short = [
        _with_quote_flow(make_candle(index), 100 if index < 9 else 0, 0.5)
        for index in range(12)
    ]
    invalid = [_with_quote_flow(make_candle(index), 100, 0.5) for index in range(12)]
    invalid[-1] = invalid[-1].model_copy(
        update={"taker_buy_quote_volume": Decimal("101")}
    )

    assert compute_normalized_taker_delta(zero) is None
    assert compute_normalized_taker_delta(zero_short) is None
    assert compute_normalized_taker_delta(invalid) is None


def test_taker_delta_accepts_exact_all_buy_and_all_sell_boundaries() -> None:
    all_buy = [_with_quote_flow(make_candle(index), 100, 1.0) for index in range(12)]
    all_sell = [_with_quote_flow(make_candle(index), 100, 0.0) for index in range(12)]

    buy_result = compute_normalized_taker_delta(all_buy)
    sell_result = compute_normalized_taker_delta(all_sell)

    assert buy_result is not None and buy_result.d3 == buy_result.d12 == 1.0
    assert sell_result is not None and sell_result.d3 == sell_result.d12 == -1.0


def test_normalized_vpci_tracks_positive_and_negative_price_volume_association() -> None:
    positive = compute_normalized_vpci(_vpci_candles(increasing_volume=True))
    negative = compute_normalized_vpci(_vpci_candles(increasing_volume=False))

    assert positive is not None and positive.value > 0
    assert negative is not None and negative.value < 0
    assert all(
        math.isfinite(value)
        for snapshot in (positive, negative)
        for value in (snapshot.value, snapshot.signal, snapshot.slope_3)
    )


def test_normalized_vpci_first_available_on_twenty_fourth_candle() -> None:
    candles = _vpci_candles(increasing_volume=True, count=24)

    series = normalized_vpci_series(candles)

    assert all(value is None for value in series[:23])
    assert series[23] is not None
    assert compute_normalized_vpci(candles[:-1]) is None
    assert compute_normalized_vpci(candles) == series[-1]


def test_normalized_vpci_resets_after_gap_and_requires_fresh_history() -> None:
    before = _vpci_candles(increasing_volume=True, count=24)
    after = [
        _with_quote_flow(
            make_candle(index, close=100 + index),
            float(index - 24) * 1_000,
            0.55,
        )
        for index in range(25, 49)
    ]
    candles = [*before, *after]

    series = normalized_vpci_series(candles)

    assert series[23] is not None
    assert all(value is None for value in series[24:47])
    assert series[47] is not None


def test_normalized_vpci_rejects_zero_quote_volume_and_zero_atr() -> None:
    zero_volume = [
        _with_quote_flow(make_candle(index, close=100 + index), 0, 0)
        for index in range(24)
    ]
    flat = [
        _with_quote_flow(make_candle(index, close=100), 1_000 + index, 0.5).model_copy(
            update={
                "open": Decimal("100"),
                "high": Decimal("100"),
                "low": Decimal("100"),
                "close": Decimal("100"),
            }
        )
        for index in range(24)
    ]
    zero_signal = _vpci_candles(increasing_volume=True, count=24)
    zero_signal[-5:] = [
        _with_quote_flow(candle, 0, 0) for candle in zero_signal[-5:]
    ]

    assert compute_normalized_vpci(zero_volume) is None
    assert compute_normalized_vpci(flat) is None
    assert compute_normalized_vpci(zero_signal) is None


def test_future_row_mutation_does_not_change_point_in_time_values() -> None:
    candles = _vpci_candles(increasing_volume=True, count=36)
    target_index = 29
    taker_before = compute_normalized_taker_delta(candles, index=target_index)
    vpci_before = compute_normalized_vpci(candles, index=target_index)

    mutated = list(candles)
    for index in range(target_index + 1, len(mutated)):
        mutated[index] = _with_quote_flow(
            mutated[index].model_copy(update={"close": Decimal("1000000")}),
            10_000_000_000,
            1.0,
        )

    assert compute_normalized_taker_delta(mutated, index=target_index) == taker_before
    assert compute_normalized_vpci(mutated, index=target_index) == vpci_before


def test_series_outputs_align_one_to_one_with_input() -> None:
    candles = _vpci_candles(increasing_volume=True, count=30)

    assert len(normalized_taker_delta_series(candles)) == len(candles)
    assert len(normalized_vpci_series(candles)) == len(candles)


def test_normalized_vpci_is_invariant_to_older_bounded_history_prefix() -> None:
    candles = _vpci_candles(increasing_volume=True, count=650)

    full = normalized_vpci_series(candles)[-1]
    bounded = normalized_vpci_series(candles[-600:])[-1]

    assert full is not None
    assert bounded == full


def test_volume_unavailable_reasons_preserve_failure_class() -> None:
    eleven = [_with_quote_flow(make_candle(index), 100, 0.5) for index in range(11)]
    zero_delta = [_with_quote_flow(make_candle(index), 0, 0) for index in range(12)]
    gap_delta = [_with_quote_flow(make_candle(index), 100, 0.5) for index in range(12)]
    gap_delta[-1] = gap_delta[-1].model_copy(
        update={
            "open_time_ms": gap_delta[-1].open_time_ms + 300_000,
            "close_time_ms": gap_delta[-1].close_time_ms + 300_000,
        }
    )
    flat_vpci = [
        _with_quote_flow(make_candle(index, close=100), 1_000, 0.5).model_copy(
            update={
                "open": Decimal("100"),
                "high": Decimal("100"),
                "low": Decimal("100"),
                "close": Decimal("100"),
            }
        )
        for index in range(24)
    ]

    assert taker_delta_unavailable_reason(eleven) == "immature_window"
    assert (
        taker_delta_unavailable_reason(zero_delta)
        == "zero_quote_volume_denominator"
    )
    assert taker_delta_unavailable_reason(gap_delta) == "noncontiguous_window"
    assert normalized_vpci_unavailable_reason(flat_vpci) == "zero_atr"
    assert normalized_vpci_unavailable_reason(_vpci_candles(increasing_volume=True)) is None
