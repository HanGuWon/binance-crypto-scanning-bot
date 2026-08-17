from __future__ import annotations

import math
from decimal import Decimal
from typing import cast

import pytest

from signalbot.backtest.r4b import (
    DEFAULT_R4B_SPEC,
    R4B_DRAFT_BLOCKERS,
    R4B_DRAFT_STATUS,
    R4bDiagnosticSpec,
    R4bExitReason,
    R4bHourlyContext,
    R4bRole,
    R4bSide,
    assert_r4b_draft_runnable,
    cross_sectional_percentiles,
    empirical_cdf,
    h1_cross_sectional_state,
    h1_exit_reason,
    h1_passes_cost_survival,
    h1_realized_volatility_bps,
    h1_realized_volatility_bps_from_candles,
    h1_relative_momentum,
    h1_relative_momentum_from_candles,
    h1_spot_entry_signal,
    h2_futures_long_entry_signal,
    h3_futures_short_entry_signal,
    h4_causal_support,
    h4_spot_false_break_entry_signal,
    h5_paired_exit,
    hourly_trend_series,
    latest_strictly_completed_index,
    passes_reward_cost_gate,
    prior_rolling_median,
    r4b_event_id,
    two_close_vwma_exit,
    vwma_series,
    wilder_atr_series,
)
from signalbot.domain.enums import Market
from signalbot.domain.models import Candle


def test_r4b_deterministic_draft_is_quarantined_before_any_outcome_run() -> None:
    assert R4B_DRAFT_STATUS == "QUARANTINED_UNRUNNABLE"
    assert len(R4B_DRAFT_BLOCKERS) == 4
    with pytest.raises(RuntimeError, match=r"H5 rank<0\.50"):
        assert_r4b_draft_runnable()


def _candle(
    index: int,
    *,
    market: Market = Market.SPOT,
    interval: str = "5m",
    close: float = 100.0,
    high: float | None = None,
    low: float | None = None,
    volume: float = 100.0,
    is_closed: bool = True,
) -> Candle:
    step_ms = 300_000 if interval == "5m" else 3_600_000
    high_value = close + 0.5 if high is None else high
    low_value = close - 0.5 if low is None else low
    return Candle(
        market=market,
        symbol="BTCUSDT",
        interval=interval,
        open_time_ms=index * step_ms,
        close_time_ms=(index + 1) * step_ms - 1,
        open=Decimal(str(close)),
        high=Decimal(str(high_value)),
        low=Decimal(str(low_value)),
        close=Decimal(str(close)),
        volume=Decimal(str(volume)),
        quote_volume=Decimal(str(volume * close)),
        trade_count=100,
        taker_buy_base_volume=Decimal(str(volume / 2)),
        taker_buy_quote_volume=Decimal(str(volume * close / 2)),
        is_closed=is_closed,
    )


def _series(
    count: int,
    *,
    market: Market = Market.SPOT,
    interval: str = "5m",
    close: float = 100.0,
    high: float | None = None,
    low: float | None = None,
) -> list[Candle]:
    return [
        _candle(
            index,
            market=market,
            interval=interval,
            close=close,
            high=high,
            low=low,
        )
        for index in range(count)
    ]


def _replace(candle: Candle, **updates: object) -> Candle:
    values = candle.model_dump()
    values.update(updates)
    return Candle.model_validate(values)


def _hourly_context(decision: Candle, **updates: object) -> R4bHourlyContext:
    values: dict[str, object] = {
        "as_of_ms": decision.close_time_ms - 1,
        "trend_percentile": 0.8125,
        "ema24_now": 2.0,
        "ema24_six_hours_prior": 1.0,
        "cross_section_median_trend": 0.5,
        "prior_60d_median_trend": 0.5,
    }
    values.update(updates)
    return R4bHourlyContext(
        as_of_ms=cast(int, values["as_of_ms"]),
        trend_percentile=cast(float, values["trend_percentile"]),
        ema24_now=cast(float, values["ema24_now"]),
        ema24_six_hours_prior=cast(float, values["ema24_six_hours_prior"]),
        cross_section_median_trend=cast(
            float, values["cross_section_median_trend"]
        ),
        prior_60d_median_trend=cast(float, values["prior_60d_median_trend"]),
    )


def test_common_wilder_atr_and_vwma_are_causal_and_require_closed_contiguous_data() -> None:
    prefix = [
        _candle(index, close=100 + index, volume=index + 1)
        for index in range(12)
    ]
    extended = [*prefix, _candle(12, close=1_000, volume=10_000)]

    assert wilder_atr_series(prefix, 3)[-1] == wilder_atr_series(extended, 3)[11]
    assert vwma_series(prefix, 3)[-1] == vwma_series(extended, 3)[11]
    assert vwma_series(prefix[:2], 3) == (None, None)

    unclosed = [*prefix[:-1], _replace(prefix[-1], is_closed=False)]
    with pytest.raises(ValueError, match="closed candles"):
        vwma_series(unclosed, 3)
    gap = [
        prefix[0],
        _replace(prefix[1], open_time_ms=900_000, close_time_ms=1_199_999),
    ]
    with pytest.raises(ValueError, match="contiguous"):
        wilder_atr_series(gap, 2)


def test_vwma_uses_quote_volume_when_base_and_quote_weights_disagree() -> None:
    first = _replace(
        _candle(0, close=100.0, volume=100.0),
        quote_volume=Decimal("1"),
        taker_buy_quote_volume=Decimal("0.5"),
    )
    second = _replace(
        _candle(1, close=200.0, volume=1.0),
        quote_volume=Decimal("100"),
        taker_buy_quote_volume=Decimal("50"),
    )

    quote_weighted = (100.0 * 1.0 + 200.0 * 100.0) / 101.0
    base_weighted = (100.0 * 100.0 + 200.0 * 1.0) / 101.0
    result = vwma_series([first, second], 2)[1]
    assert result == pytest.approx(quote_weighted)
    assert result != pytest.approx(base_weighted)


def test_ecdf_percentiles_and_prior_median_use_strictly_prior_values() -> None:
    assert empirical_cdf(2.0, [1.0, 2.0, 3.0]) == pytest.approx(2 / 3)
    assert cross_sectional_percentiles({"ethusdt": 1.0, "btcusdt": 2.0}) == {
        "BTCUSDT": 0.75,
        "ETHUSDT": 0.25,
    }
    assert cross_sectional_percentiles(
        {"A": 1.0, "B": 1.0, "C": 3.0, "D": 4.0}
    ) == {"A": 0.25, "B": 0.25, "C": 0.625, "D": 0.875}
    assert prior_rolling_median([1.0, 3.0, 1_000.0], 2, 2) == 2.0
    assert prior_rolling_median([1.0, 3.0], 1, 2) is None
    with pytest.raises(ValueError, match="exactly 8"):
        cross_sectional_percentiles({"BTCUSDT": 1.0}, expected_size=8)


def test_h1_relative_momentum_uses_7d_formation_ending_6h_early_and_no_future() -> None:
    closes = [100.0] * 2_200
    index = 2_100
    closes[index - 2_088] = 100.0
    closes[index - 72] = 121.0

    expected = math.log(1.21)
    assert h1_relative_momentum(closes, index) == pytest.approx(expected)
    closes[index + 1 :] = [1.0] * (len(closes) - index - 1)
    assert h1_relative_momentum(closes, index) == pytest.approx(expected)
    assert h1_relative_momentum(closes, 2_087) is None


def test_h1_realized_volatility_has_explicit_radical_and_cost_boundary() -> None:
    returns = [0.001, -0.002] * 288
    closes = [100.0]
    for value in returns:
        closes.append(closes[-1] * math.exp(value))
    expected = 10_000 * math.sqrt(sum(value * value for value in returns))

    realized = h1_realized_volatility_bps(closes, 576)
    assert realized == pytest.approx(expected)
    closes.extend([1_000.0, 2_000.0])
    assert h1_realized_volatility_bps(closes, 576) == pytest.approx(expected)
    assert h1_realized_volatility_bps(closes, 575) is None
    assert h1_passes_cost_survival(120.0, 30.0)
    assert not h1_passes_cost_survival(119.999, 30.0)

    candles = [
        _candle(index, close=closes[index], market=Market.SPOT)
        for index in range(577)
    ]
    assert h1_realized_volatility_bps_from_candles(candles, 576) == pytest.approx(
        expected
    )
    unclosed = [*candles]
    unclosed[400] = _replace(unclosed[400], is_closed=False)
    with pytest.raises(ValueError, match="closed candles"):
        h1_realized_volatility_bps_from_candles(unclosed, 576)


def test_h1_candle_contract_rejects_gaps_and_unclosed_history() -> None:
    index = 2_088
    candles = _series(index + 1, market=Market.SPOT)
    assert h1_relative_momentum_from_candles(candles, index) == 0.0

    gap = [*candles]
    gap[100] = _replace(
        gap[100],
        open_time_ms=gap[100].open_time_ms + 300_000,
        close_time_ms=gap[100].close_time_ms + 300_000,
    )
    with pytest.raises(ValueError, match="contiguous"):
        h1_relative_momentum_from_candles(gap, index)


def test_frozen_spec_rejects_threshold_changes() -> None:
    with pytest.raises(ValueError, match="thresholds changed"):
        R4bDiagnosticSpec(
            **{**DEFAULT_R4B_SPEC.model_dump(), "h1_entry_rank": 0.8}
        )


def test_h1_cross_section_and_entry_boundaries_are_exact_and_deterministic() -> None:
    state = h1_cross_sectional_state(
        {f"A{index}USDT": float(index) for index in range(8)}
    )
    assert state["A7USDT"].cross_section_percentile == 0.9375
    assert state["A6USDT"].cross_section_percentile == 0.8125
    assert state["A0USDT"].cross_section_percentile == 0.0625
    assert state["A0USDT"].demeaned_momentum == -3.5

    signal = h1_spot_entry_signal(
        symbol="btcusdt",
        decision_time_ms=1_000,
        decision_close=100.0,
        entry_price_estimate=101.0,
        atr288=2.0,
        current_rank=0.9375,
        prior_rank=0.8125,
        market_regime_ecdf=0.5,
        realized_volatility_bps=120.0,
        estimated_roundtrip_cost_bps=30.0,
    )
    assert signal is not None
    assert signal.decision_price == 100.0
    assert signal.entry_price_estimate == 101.0
    assert signal.stop_price == 98.0
    assert signal.role is R4bRole.H1_SPOT_LONG_ENTRY
    assert signal.event_id == r4b_event_id(
        DEFAULT_R4B_SPEC.protocol_version,
        R4bRole.H1_SPOT_LONG_ENTRY,
        Market.SPOT,
        "BTCUSDT",
        1_000,
    )
    assert signal.event_id == h1_spot_entry_signal(
        symbol="BTCUSDT",
        decision_time_ms=1_000,
        decision_close=100.0,
        entry_price_estimate=101.0,
        atr288=2.0,
        current_rank=0.9375,
        prior_rank=0.8125,
        market_regime_ecdf=0.5,
        realized_volatility_bps=120.0,
        estimated_roundtrip_cost_bps=30.0,
    ).event_id  # type: ignore[union-attr]

    base = {
        "symbol": "BTCUSDT",
        "decision_time_ms": 1_000,
        "decision_close": 100.0,
        "entry_price_estimate": 101.0,
        "atr288": 2.0,
        "current_rank": 0.9375,
        "prior_rank": 0.8125,
        "market_regime_ecdf": 0.5,
        "realized_volatility_bps": 120.0,
        "estimated_roundtrip_cost_bps": 30.0,
    }
    assert h1_spot_entry_signal(**{**base, "current_rank": 0.8125}) is None
    assert h1_spot_entry_signal(**{**base, "prior_rank": 0.9375}) is None
    assert h1_spot_entry_signal(**{**base, "market_regime_ecdf": 0.499}) is None
    assert h1_spot_entry_signal(**{**base, "realized_volatility_bps": 119.999}) is None


def test_h1_exit_boundaries_prioritize_invalidation_then_rank_then_timeout() -> None:
    assert h1_exit_reason(0.4375, 1) is R4bExitReason.H1_RANK_INVALIDATION
    assert h1_exit_reason(0.5625, 1) is R4bExitReason.H1_RANK_EXIT
    assert h1_exit_reason(0.6875, 1) is R4bExitReason.H1_RANK_EXIT
    assert h1_exit_reason(0.8125, 575) is None
    assert h1_exit_reason(0.8125, 576) is R4bExitReason.TIMEOUT


def test_completed_hour_trend_and_strict_asof_exclude_equal_close_time() -> None:
    candles = [
        _candle(
            index,
            market=Market.FUTURES,
            interval="1h",
            close=100 + index * 0.1,
        )
        for index in range(110)
    ]
    points = hourly_trend_series(candles)
    assert points[94] is None
    assert points[95] is not None
    decision_time = candles[100].close_time_ms
    assert latest_strictly_completed_index(candles, decision_time) == 99

    prefix_value = hourly_trend_series(candles[:101])[100]
    changed_future = [
        *candles[:101],
        *[
            _replace(candle, close=Decimal("1000"), high=Decimal("1001"))
            for candle in candles[101:]
        ],
    ]
    assert hourly_trend_series(changed_future)[100] == prefix_value


def test_reward_cost_gate_preserves_inclusive_gate_boundaries() -> None:
    assert passes_reward_cost_gate(R4bSide.LONG, 100.0, 99.0, 101.5, 0.0)
    assert not passes_reward_cost_gate(R4bSide.LONG, 100.0, 99.0, 101.499, 0.0)

    # Both the price-distance and 3x-cost comparisons land exactly on binary values.
    assert passes_reward_cost_gate(
        R4bSide.LONG,
        entry_price=128.0,
        stop_price=127.875,
        target_price=128.375,
        estimated_cost_bps=9.765625,
    )
    assert not passes_reward_cost_gate(R4bSide.LONG, 100.0, 99.0, 100.0, 0.0)


def test_h2_long_positive_case_and_all_inclusive_exclusive_boundaries() -> None:
    index = 100
    candles = _series(105, market=Market.FUTURES)
    candles[index - 50] = _replace(candles[index - 50], high=Decimal("110"))
    candles[index] = _replace(
        candles[index],
        open=Decimal("100"),
        close=Decimal("101"),
        high=Decimal("101.5"),
        low=Decimal("99"),
    )
    atr = [1.0] * len(candles)
    vwma = [100.0] * len(candles)
    vwma[index - 1] = 101.0  # prior z=-1, inclusive
    vwma[index] = 100.0  # current z=1, inclusive
    hourly = _hourly_context(candles[index])

    signal = h2_futures_long_entry_signal(
        candles,
        index,
        vwma48=vwma,
        atr48=atr,
        hourly=hourly,
        entry_price_estimate=102.0,
        estimated_cost_bps=16.0,
    )
    assert signal is not None
    assert signal.role is R4bRole.H2_FUTURES_LONG_ENTRY
    assert signal.decision_price == 101.0
    assert signal.entry_price_estimate == 102.0
    assert signal.target_price == 110.0
    assert signal.timeout_bars == 72

    zero_current_z = [*vwma]
    zero_current_z[index] = 101.0
    assert h2_futures_long_entry_signal(
        candles,
        index,
        vwma48=zero_current_z,
        atr48=atr,
        hourly=hourly,
        entry_price_estimate=102.0,
        estimated_cost_bps=16.0,
    ) is None
    assert h2_futures_long_entry_signal(
        candles,
        index,
        vwma48=vwma,
        atr48=atr,
        hourly=_hourly_context(candles[index], trend_percentile=0.6875),
        entry_price_estimate=102.0,
        estimated_cost_bps=16.0,
    ) is None
    assert h2_futures_long_entry_signal(
        candles,
        index,
        vwma48=vwma,
        atr48=atr,
        hourly=hourly,
        entry_price_estimate=109.0,
        estimated_cost_bps=16.0,
    ) is None
    assert h2_futures_long_entry_signal(
        candles,
        index,
        vwma48=vwma,
        atr48=atr,
        hourly=_hourly_context(candles[index], ema24_now=1.0),
        entry_price_estimate=102.0,
        estimated_cost_bps=16.0,
    ) is None


def test_h3_short_positive_case_and_exclusive_zero_boundary() -> None:
    index = 100
    candles = _series(105, market=Market.FUTURES)
    candles[index - 50] = _replace(candles[index - 50], low=Decimal("90"))
    candles[index] = _replace(
        candles[index],
        open=Decimal("100"),
        close=Decimal("99"),
        high=Decimal("101"),
        low=Decimal("98.5"),
    )
    atr = [1.0] * len(candles)
    vwma = [100.0] * len(candles)
    vwma[index - 1] = 99.0  # prior z=1, inclusive
    vwma[index] = 100.0  # current z=-1, inclusive
    hourly = _hourly_context(
        candles[index],
        trend_percentile=0.1875,
        ema24_now=1.0,
        ema24_six_hours_prior=2.0,
    )

    signal = h3_futures_short_entry_signal(
        candles,
        index,
        vwma48=vwma,
        atr48=atr,
        hourly=hourly,
        entry_price_estimate=98.0,
        estimated_cost_bps=16.0,
    )
    assert signal is not None
    assert signal.role is R4bRole.H3_FUTURES_SHORT_ENTRY
    assert signal.decision_price == 99.0
    assert signal.entry_price_estimate == 98.0
    assert signal.target_price == 90.0

    zero_current_z = [*vwma]
    zero_current_z[index] = 99.0
    assert h3_futures_short_entry_signal(
        candles,
        index,
        vwma48=zero_current_z,
        atr48=atr,
        hourly=hourly,
        entry_price_estimate=98.0,
        estimated_cost_bps=16.0,
    ) is None
    assert h3_futures_short_entry_signal(
        candles,
        index,
        vwma48=vwma,
        atr48=atr,
        hourly=_hourly_context(
            candles[index],
            trend_percentile=0.3125,
            ema24_now=1.0,
            ema24_six_hours_prior=2.0,
        ),
        entry_price_estimate=98.0,
        estimated_cost_bps=16.0,
    ) is None
    assert h3_futures_short_entry_signal(
        candles,
        index,
        vwma48=vwma,
        atr48=atr,
        hourly=hourly,
        entry_price_estimate=91.0,
        estimated_cost_bps=16.0,
    ) is None


def test_h2_h3_require_strictly_prior_hour_context_and_ignore_future_bars() -> None:
    index = 100
    candles = _series(105, market=Market.FUTURES)
    candles[index - 50] = _replace(candles[index - 50], high=Decimal("110"))
    candles[index] = _replace(
        candles[index], close=Decimal("101"), high=Decimal("102"), low=Decimal("99")
    )
    atr = [1.0] * len(candles)
    vwma = [100.0] * len(candles)
    vwma[index - 1] = 100.5
    hourly = _hourly_context(candles[index])
    baseline = h2_futures_long_entry_signal(
        candles,
        index,
        vwma48=vwma,
        atr48=atr,
        hourly=hourly,
        entry_price_estimate=102.0,
        estimated_cost_bps=16.0,
    )
    assert baseline is not None

    changed = [*candles]
    changed[index + 1 :] = [
        _replace(candle, close=Decimal("1000"), high=Decimal("1001"))
        for candle in candles[index + 1 :]
    ]
    assert h2_futures_long_entry_signal(
        changed,
        index,
        vwma48=vwma,
        atr48=atr,
        hourly=hourly,
        entry_price_estimate=102.0,
        estimated_cost_bps=16.0,
    ) == baseline

    with pytest.raises(ValueError, match="strictly before"):
        h2_futures_long_entry_signal(
            candles,
            index,
            vwma48=vwma,
            atr48=atr,
            hourly=_hourly_context(
                candles[index], as_of_ms=candles[index].close_time_ms
            ),
            entry_price_estimate=102.0,
            estimated_cost_bps=16.0,
        )


def _h4_inputs(penetration_low: float) -> tuple[list[Candle], int, list[float], list[float]]:
    index = 300
    candles = _series(305, market=Market.SPOT, close=101.0, high=102.0, low=100.0)
    candles[index - 3] = _replace(
        candles[index - 3], close=Decimal("100"), high=Decimal("101"), low=Decimal("80")
    )
    candles[index - 2] = _replace(
        candles[index - 2],
        close=Decimal("100"),
        high=Decimal("101"),
        low=Decimal(str(penetration_low)),
    )
    candles[index - 1] = _replace(
        candles[index - 1],
        open=Decimal("100.4"),
        close=Decimal("100.4"),
        high=Decimal("100.5"),
        low=Decimal("99.5"),
    )
    candles[index] = _replace(
        candles[index],
        close=Decimal("101"),
        high=Decimal("102"),
        low=Decimal("99.8"),
    )
    atr = [10.0] * len(candles)
    target = [130.0] * len(candles)
    return candles, index, atr, target


def test_h4_support_slice_is_t_minus_288_through_t_minus_13_inclusive() -> None:
    candles, index, _, _ = _h4_inputs(99.0)
    candles[index - 13] = _replace(candles[index - 13], low=Decimal("90"))
    candles[index - 12] = _replace(candles[index - 12], low=Decimal("50"))

    assert h4_causal_support(candles, index) == 90.0
    assert h4_causal_support(candles, 287) is None


@pytest.mark.parametrize("penetration_low", [99.0, 92.5])
def test_h4_inclusive_penetration_boundaries_trigger(penetration_low: float) -> None:
    candles, index, atr, target = _h4_inputs(penetration_low)
    signal = h4_spot_false_break_entry_signal(
        candles,
        index,
        atr48=atr,
        vwma96=target,
        hourly_trend_percentile=0.3125,
        entry_price_estimate=102.0,
        estimated_cost_bps=30.0,
    )
    assert signal is not None
    assert signal.role is R4bRole.H4_SPOT_LONG_ENTRY
    assert signal.structural_start_index == index - 2


@pytest.mark.parametrize("penetration_low", [99.01, 92.49])
def test_h4_outside_penetration_boundaries_fails_closed(penetration_low: float) -> None:
    candles, index, atr, target = _h4_inputs(penetration_low)
    # Remove later qualifying penetrations so only the parametrized bar is evaluated.
    candles[index - 1] = _replace(candles[index - 1], low=Decimal("100"))
    candles[index] = _replace(candles[index], low=Decimal("100"))
    assert h4_spot_false_break_entry_signal(
        candles,
        index,
        atr48=atr,
        vwma96=target,
        hourly_trend_percentile=0.3125,
        entry_price_estimate=102.0,
        estimated_cost_bps=30.0,
    ) is None


def test_h4_earliest_qualifier_controls_stop_window_and_future_is_ignored() -> None:
    candles, index, atr, target = _h4_inputs(99.0)
    candles[index - 1] = _replace(candles[index - 1], low=Decimal("95"))
    candles[index] = _replace(candles[index], low=Decimal("92.5"))
    signal = h4_spot_false_break_entry_signal(
        candles,
        index,
        atr48=atr,
        vwma96=target,
        hourly_trend_percentile=0.3125,
        entry_price_estimate=102.0,
        estimated_cost_bps=0.0,
    )
    assert signal is not None
    assert signal.structural_start_index == index - 2
    assert signal.stop_price == 90.0
    assert signal.stop_price > 80.0 - 2.5  # t-3 low is not part of the stop window.

    changed = [*candles]
    changed[index + 1 :] = [
        _replace(candle, close=Decimal("1000"), high=Decimal("1001"))
        for candle in candles[index + 1 :]
    ]
    assert h4_spot_false_break_entry_signal(
        changed,
        index,
        atr48=atr,
        vwma96=target,
        hourly_trend_percentile=0.3125,
        entry_price_estimate=102.0,
        estimated_cost_bps=0.0,
    ) == signal


def test_h4_reclaim_and_trend_veto_equalities_fail_closed() -> None:
    candles, index, atr, target = _h4_inputs(99.0)
    assert h4_spot_false_break_entry_signal(
        candles,
        index,
        atr48=atr,
        vwma96=target,
        hourly_trend_percentile=0.1875,
        entry_price_estimate=102.0,
        estimated_cost_bps=0.0,
    ) is None

    assert h4_spot_false_break_entry_signal(
        candles,
        index,
        atr48=atr,
        vwma96=target,
        hourly_trend_percentile=0.3125,
        entry_price_estimate=129.0,
        estimated_cost_bps=0.0,
    ) is None

    equal_prior_high = [*candles]
    equal_prior_high[index - 1] = _replace(
        equal_prior_high[index - 1], high=equal_prior_high[index].close
    )
    assert h4_spot_false_break_entry_signal(
        equal_prior_high,
        index,
        atr48=atr,
        vwma96=target,
        hourly_trend_percentile=0.3125,
        entry_price_estimate=102.0,
        estimated_cost_bps=0.0,
    ) is None


def test_two_close_and_h5_exit_boundaries_are_strict() -> None:
    assert two_close_vwma_exit(R4bSide.LONG, 99.0, 98.0, 100.0, 100.0)
    assert two_close_vwma_exit(R4bSide.SHORT, 101.0, 102.0, 100.0, 100.0)
    assert not two_close_vwma_exit(R4bSide.LONG, 100.0, 98.0, 100.0, 100.0)

    values = {
        "held_bars": 12,
        "h1_rank": 0.4375,
        "previous_close": 99.0,
        "current_close": 98.0,
        "previous_vwma48": 100.0,
        "current_vwma48": 100.0,
        "market_regime_ecdf": 0.499,
    }
    assert h5_paired_exit(**values)
    assert not h5_paired_exit(**{**values, "held_bars": 11})
    assert not h5_paired_exit(**{**values, "h1_rank": 0.5})
    assert not h5_paired_exit(**{**values, "market_regime_ecdf": 0.5})
    assert not h5_paired_exit(**{**values, "previous_close": 100.0})
