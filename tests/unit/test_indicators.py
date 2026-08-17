import math

import pytest

from conftest import make_candle
from signalbot.config import SignalSettings
from signalbot.data.microstructure import OrderFlowSnapshot
from signalbot.domain.models import MarketRegime
from signalbot.indicators.core import FeatureEngine, ema_series, rsi_series


def test_ema_uses_sma_seed() -> None:
    assert ema_series([1, 2, 3, 4, 5], 3) == [None, None, 2.0, 3.0, 4.0]


def test_rsi_on_strict_rise_reaches_100() -> None:
    values = [float(i) for i in range(1, 20)]
    result = rsi_series(values, 14)
    assert result[-1] == 100.0


def test_feature_engine_excludes_current_bar_from_range_boundary() -> None:
    settings = SignalSettings()
    engine = FeatureEngine(settings)
    candles = [make_candle(i, close=100 + math.sin(i / 5), volume=100) for i in range(219)]
    final = make_candle(219, close=140, volume=1000).model_copy(
        update={"high": make_candle(219, close=140).high + 10}
    )
    feature = engine.compute(
        [*candles, final],
        OrderFlowSnapshot(taker_buy_ratio=0.7, trade_count=10),
        2.0,
        MarketRegime(label="neutral"),
    )
    assert feature is not None
    assert feature.recent_high < 110
    assert feature.price == 140
    assert feature.relative_volume > 5
    assert feature.spread_bps == 2.0


def test_feature_series_matches_scalar_snapshot() -> None:
    engine = FeatureEngine(SignalSettings())
    candles = [
        make_candle(i, close=100 + math.sin(i / 7), volume=100 + i % 11)
        for i in range(240)
    ]
    flows = [OrderFlowSnapshot(taker_buy_ratio=0.4 + (i % 3) / 10) for i in range(240)]
    regimes = [MarketRegime(label="neutral", breadth_ratio=0.5) for _ in candles]

    series = engine.compute_series(candles, flows, 11.25, regimes)
    scalar = engine.compute(candles, flows[-1], 11.25, regimes[-1])

    assert series[-1] == scalar
    assert all(value is None for value in series[: engine.minimum_history - 1])


def test_feature_engine_compute_at_indices_matches_series() -> None:
    engine = FeatureEngine(SignalSettings())
    candles = [
        make_candle(i, close=100 + math.sin(i / 7), volume=100 + i % 11)
        for i in range(240)
    ]
    flows = [OrderFlowSnapshot(taker_buy_ratio=0.4 + (i % 3) / 10) for i in range(240)]
    regimes = [MarketRegime(label="neutral", breadth_ratio=0.5) for _ in candles]
    indices = [engine.minimum_history - 2, engine.minimum_history - 1, 225, 239]

    selected = engine.compute_at_indices(candles, indices, flows, 11.25, regimes, True)
    series = engine.compute_series(candles, flows, 11.25, regimes, True)

    assert selected == [series[index] for index in indices]
    assert selected[0] is None
    assert all(snapshot is not None for snapshot in selected[1:])


def test_feature_engine_compute_at_indices_does_not_use_future_candles() -> None:
    engine = FeatureEngine(SignalSettings())
    candles = [make_candle(i, close=100 + math.sin(i / 5)) for i in range(230)]
    selected_index = 219
    candles[-1] = candles[-1].model_copy(update={"is_closed": False})

    before_future = engine.compute_at_indices(candles[: selected_index + 1], [selected_index])
    with_future = engine.compute_at_indices(candles, [selected_index])

    assert before_future == with_future
    assert before_future[0] is not None


def test_feature_engine_rejects_an_open_candle_in_the_used_prefix() -> None:
    engine = FeatureEngine(SignalSettings())
    candles = [make_candle(i, close=100 + math.sin(i / 5)) for i in range(220)]
    candles[-1] = candles[-1].model_copy(update={"is_closed": False})

    with pytest.raises(ValueError, match="fully closed candle prefix"):
        engine.compute(
            candles,
            OrderFlowSnapshot(),
            2.0,
            MarketRegime(),
        )
    with pytest.raises(ValueError, match="fully closed candle prefix"):
        engine.compute_at_indices(candles, [219])


def test_feature_engine_compute_at_indices_accepts_empty_selection() -> None:
    engine = FeatureEngine(SignalSettings())

    assert engine.compute_at_indices([], []) == []
    assert engine.compute_at_indices([make_candle(0)], []) == []


@pytest.mark.parametrize("indices", [[-1], [3], [2, 1], [1, 1]])
def test_feature_engine_compute_at_indices_rejects_invalid_indices(indices: list[int]) -> None:
    engine = FeatureEngine(SignalSettings())
    candles = [make_candle(i) for i in range(3)]

    with pytest.raises(ValueError):
        engine.compute_at_indices(candles, indices)


def test_feature_engine_compute_at_indices_rejects_misaligned_context() -> None:
    engine = FeatureEngine(SignalSettings())
    candles = [make_candle(i) for i in range(3)]

    with pytest.raises(ValueError, match="align one-to-one with candles"):
        engine.compute_at_indices(candles, [], order_flows=[OrderFlowSnapshot()])
    with pytest.raises(ValueError, match="align one-to-one with candles"):
        engine.compute_at_indices(candles, [], regimes=[MarketRegime()])


def test_feature_completeness_uses_closed_kline_flow_not_intrabar_flow() -> None:
    engine = FeatureEngine(SignalSettings())
    candles = [make_candle(i) for i in range(220)]
    zero_activity = make_candle(219, volume=0)

    missing = engine.compute(
        [*candles[:-1], zero_activity],
        OrderFlowSnapshot(available=True),
        None,
        MarketRegime(),
    )
    complete = engine.compute(
        candles,
        OrderFlowSnapshot(available=True),
        11.25,
        MarketRegime(),
        spread_is_proxy=True,
    )

    assert missing is not None and missing.data_completeness == 0.7
    assert not missing.closed_kline_flow_available
    assert complete is not None and complete.data_completeness == 1.0
    assert complete.closed_kline_flow_available


def test_canonical_features_ignore_intrabar_flow_but_expose_it_separately() -> None:
    engine = FeatureEngine(SignalSettings())
    candles = [make_candle(i) for i in range(220)]

    buy_intrabar = engine.compute(
        candles,
        OrderFlowSnapshot(taker_buy_ratio=1.0, available=True),
        2.0,
        MarketRegime(),
    )
    sell_intrabar = engine.compute(
        candles,
        OrderFlowSnapshot(taker_buy_ratio=0.0, available=True),
        2.0,
        MarketRegime(),
    )

    assert buy_intrabar is not None and sell_intrabar is not None
    assert buy_intrabar.taker_buy_ratio == sell_intrabar.taker_buy_ratio == 0.55
    assert buy_intrabar.taker_imbalance == sell_intrabar.taker_imbalance
    assert buy_intrabar.taker_delta_3 == sell_intrabar.taker_delta_3
    assert buy_intrabar.taker_delta_12 == sell_intrabar.taker_delta_12
    assert buy_intrabar.normalized_vpci == sell_intrabar.normalized_vpci
    assert buy_intrabar.intrabar_taker_imbalance_60s == 1.0
    assert sell_intrabar.intrabar_taker_imbalance_60s == -1.0
