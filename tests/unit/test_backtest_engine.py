from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

from conftest import make_candle, make_feature
from signalbot.backtest.config import (
    BacktestAsset,
    BacktestSpec,
    BacktestSplit,
    CostSettings,
    ExitPolicySettings,
)
from signalbot.backtest.context import StrictContextIndex
from signalbot.backtest.engine import (
    FundingRate,
    ResearchBacktester,
    build_market_regimes,
    calculate_execution_returns,
    calculate_funding_return,
    count_held_bars,
    directional_excursion,
)
from signalbot.backtest.runner import _build_opportunity_summary
from signalbot.config import Settings
from signalbot.domain.enums import Direction, Market, SignalFamily
from signalbot.domain.models import FeatureSnapshot, MarketRegime, RuleEvaluation
from signalbot.regime.market import MarketRegimeEngine


def test_long_and_short_gross_returns_are_directional() -> None:
    long = calculate_execution_returns(Direction.LONG, 100, 110, 0, 0)
    short = calculate_execution_returns(Direction.SHORT, 100, 90, 0, 0)
    assert long.gross_return == pytest.approx(0.10)
    assert short.gross_return == pytest.approx(0.10)
    assert long.net_before_funding == pytest.approx(0.10)
    assert short.net_before_funding == pytest.approx(0.10)


def test_round_trip_costs_are_charged_on_both_sides() -> None:
    long = calculate_execution_returns(Direction.LONG, 100, 100, 10, 5)
    short = calculate_execution_returns(Direction.SHORT, 100, 100, 10, 5)

    assert long.gross_return == 0
    assert short.gross_return == 0
    assert long.slippage_return == pytest.approx(0.001)
    assert short.slippage_return == pytest.approx(0.001)
    assert long.fee_return == pytest.approx(0.002)
    assert short.fee_return == pytest.approx(0.002)
    assert long.net_before_funding == pytest.approx(-0.003)
    assert short.net_before_funding == pytest.approx(-0.003)


def test_execution_rejects_risk_directions() -> None:
    with pytest.raises(ValueError, match="long or short"):
        calculate_execution_returns(Direction.RISK_UP, 100, 101, 0, 0)


@pytest.mark.parametrize(
    ("entry", "exit_price", "fee", "slippage"),
    [
        (float("nan"), 100.0, 0.0, 0.0),
        (100.0, float("inf"), 0.0, 0.0),
        (100.0, 100.0, float("nan"), 0.0),
        (100.0, 100.0, 0.0, float("inf")),
    ],
)
def test_execution_rejects_nonfinite_prices_and_costs(
    entry: float,
    exit_price: float,
    fee: float,
    slippage: float,
) -> None:
    with pytest.raises(ValueError, match="finite"):
        calculate_execution_returns(
            Direction.LONG,
            entry,
            exit_price,
            fee,
            slippage,
        )


def test_cost_settings_reject_nonfinite_slippage_mapping() -> None:
    with pytest.raises(ValueError, match="finite"):
        CostSettings(
            spot_slippage_bps={
                "anchor": float("nan"),
                "major": 5.0,
                "volatile": 10.0,
            }
        )


def test_outcome_edge_margin_rejects_nonfinite_values() -> None:
    with pytest.raises(ValueError, match="outcome_edge_margin_bps"):
        BacktestSpec(
            protocol_version="invalid-margin",
            rule_version="invalid-margin",
            data_start=datetime(2019, 1, 1, tzinfo=UTC),
            evaluation_start=datetime(2020, 1, 1, tzinfo=UTC),
            evaluation_end=datetime(2021, 1, 1, tzinfo=UTC),
            outcome_edge_margin_bps=float("nan"),
            assets=[
                BacktestAsset(
                    asset="BTC",
                    cohort="anchor",
                    spot_symbol="BTCUSDT",
                    futures_symbol="BTCUSDT",
                )
            ],
            splits=[],
        )


def test_next_open_exit_counts_only_completed_exposure_bars() -> None:
    assert count_held_bars(10, 34, exit_on_open=True) == 24
    assert count_held_bars(10, 10, exit_on_open=False) == 1


def test_long_stop_excursion_records_adverse_move() -> None:
    mfe, mae = directional_excursion(Direction.LONG, 100, 98, 100)
    assert mfe == 0
    assert mae == pytest.approx(-0.02)

    short_mfe, short_mae = directional_excursion(Direction.SHORT, 100, 100, 102)
    assert short_mfe == 0
    assert short_mae == pytest.approx(-0.02)


def test_funding_is_directional_and_uses_strict_position_boundaries() -> None:
    rates = [
        FundingRate(0, 0.01, 100),
        FundingRate(5, 0.01, 110),
        FundingRate(10, 0.01, 100),
    ]
    short = calculate_funding_return(Direction.SHORT, 0, 10, 100, rates)
    long = calculate_funding_return(Direction.LONG, 0, 10, 100, rates)
    assert short == pytest.approx(0.011)
    assert long == pytest.approx(-0.011)


def test_funding_feature_zscore_uses_a_30_day_utc_window() -> None:
    day_ms = 86_400_000
    asset = BacktestAsset(
        asset="BTC",
        cohort="anchor",
        spot_symbol="BTCUSDT",
        futures_symbol="BTCUSDT",
    )
    spec = BacktestSpec(
        protocol_version="funding-parity",
        rule_version="funding-parity",
        interval="5m",
        data_start=datetime(1969, 12, 1, tzinfo=UTC),
        evaluation_start=datetime(1970, 1, 1, tzinfo=UTC),
        evaluation_end=datetime(1970, 3, 1, tzinfo=UTC),
        assets=[asset],
        splits=[],
    )
    settings = Settings.model_validate(
        {
            "binance": {"funding_history_points": 3},
            "signals": {"funding_zscore_minimum_history": 2},
        }
    )
    backtester = ResearchBacktester(settings, spec)
    inside = backtester._with_funding_features(
        [make_feature(event_time_ms=30 * day_ms + 2)],
        [
            FundingRate(0, 0.0),
            FundingRate(1, 2.0),
            FundingRate(30 * day_ms, 3.0),
        ],
        "TESTUSDT",
    )[0]
    outside = backtester._with_funding_features(
        [make_feature(event_time_ms=30 * day_ms + 3)],
        [
            FundingRate(0, 0.0),
            FundingRate(1, 2.0),
            FundingRate(30 * day_ms + 1, 3.0),
        ],
        "TESTUSDT",
    )[0]

    assert inside is not None and inside.funding_zscore == pytest.approx(2.0)
    assert outside is not None and outside.funding_zscore is None


def test_five_minute_regime_waits_for_210_complete_hourly_features() -> None:
    candles = [make_candle(index, close=100 + index) for index in range(2_521)]

    regimes = build_market_regimes({"BTC": candles})["BTC"]

    # Same-close hourly points remain unavailable to a 5m decision. The first
    # live-parity hourly feature is the 210th complete hour and acts one 5m bar later.
    for index in (587, 588, 599, 600, 2_507, 2_508, 2_519):
        assert regimes[index].btc_trend == "neutral"
    assert regimes[2_520].btc_trend == "bullish"
    assert regimes[2_520].label == "risk_on"


def test_five_minute_regime_excludes_same_close_breadth_reversal() -> None:
    btc = [
        make_candle(index, symbol="BTCUSDT", close=price)
        for index, price in enumerate((100, 102, 99, 98))
    ]
    eth = [
        make_candle(index, symbol="ETHUSDT", close=price)
        for index, price in enumerate((50, 51, 49, 48))
    ]

    regimes = build_market_regimes({"BTC": btc, "ETH": eth})

    assert regimes["BTC"][1].breadth_ratio == 0.5
    assert regimes["BTC"][2].breadth_ratio == 1.0
    assert regimes["BTC"][3].breadth_ratio == 0.0
    assert regimes["ETH"][2].breadth_ratio == 1.0


def test_five_minute_regime_is_independent_of_cross_symbol_input_order() -> None:
    btc = [
        make_candle(index, symbol="BTCUSDT", close=price)
        for index, price in enumerate((100, 101, 102, 103))
    ]
    eth = [
        make_candle(index, symbol="ETHUSDT", close=price)
        for index, price in enumerate((100, 99, 98, 97))
    ]

    forward = build_market_regimes({"BTC": btc, "ETH": eth})
    reversed_panel = build_market_regimes({"ETH": eth, "BTC": btc})

    assert forward["BTC"] == reversed_panel["BTC"]
    assert forward["ETH"] == reversed_panel["ETH"]
    assert forward["BTC"][2].breadth_ratio == 0.5


def test_five_minute_regime_carries_stale_hourly_trend_with_live_parity() -> None:
    btc = [
        make_candle(index, symbol="BTCUSDT", close=100 + index)
        for index in (*range(2_532), *range(2_544, 2_556))
    ]
    eth = [
        make_candle(index, symbol="ETHUSDT", close=1_000 + index)
        for index in range(2_557)
    ]

    offline = build_market_regimes({"BTC": btc, "ETH": eth})["ETH"]

    live = MarketRegimeEngine(maximum_points_per_symbol=3_000)
    for candle in (*btc, *eth):
        live.update_candle(candle)
    for completed_hour in (210, 211, 213):
        live.update_feature(
            make_feature(
                market=Market.SPOT,
                symbol="BTCUSDT",
                interval="1h",
                event_time_ms=completed_hour * 3_600_000 - 1,
                price=1_000,
                ema20=900,
                ema50=800,
            )
        )

    assert offline[2_519].btc_trend == "neutral"
    for index in (2_520, 2_532, 2_543, 2_555, 2_556):
        assert offline[index].btc_trend == "bullish"
        assert offline[index] == live.snapshot(
            Market.SPOT, eth[index].close_time_ms
        )


def test_hourly_regime_behavior_remains_on_the_current_closed_candle() -> None:
    candles = [
        make_candle(
            index,
            interval="1h",
            step_ms=3_600_000,
            close=100 + index,
        )
        for index in range(50)
    ]

    regimes = build_market_regimes({"BTC": candles})["BTC"]

    assert regimes[49].btc_trend == "bullish"


def test_five_minute_backtester_builds_strict_15m_and_1h_feature_contexts() -> None:
    asset = BacktestAsset(
        asset="BTC",
        cohort="anchor",
        spot_symbol="BTCUSDT",
        futures_symbol="BTCUSDT",
    )
    spec = BacktestSpec(
        protocol_version="test-5m",
        rule_version="test-5m",
        interval="5m",
        data_start=datetime(2020, 1, 1, tzinfo=UTC),
        evaluation_start=datetime(2020, 2, 1, tzinfo=UTC),
        evaluation_end=datetime(2020, 3, 1, tzinfo=UTC),
        assets=[asset],
        splits=[],
    )
    backtester = ResearchBacktester(Settings(), spec)
    candles = [make_candle(index) for index in range(2_521)]
    regimes = [MarketRegime() for _ in candles]

    context_index = backtester._higher_timeframe_context_index(candles, regimes)

    at_hourly_close = context_index.at(candles[2_519].close_time_ms)
    on_next_primary_bar = context_index.at(candles[2_520].close_time_ms)
    assert "1h" not in at_hourly_close
    assert on_next_primary_bar["1h"].event_time_ms == candles[2_519].close_time_ms
    assert on_next_primary_bar["15m"].event_time_ms < candles[2_520].close_time_ms


def test_volume_research_opportunity_panel_uses_only_tradable_direction() -> None:
    asset = BacktestAsset(
        asset="BTC",
        cohort="anchor",
        spot_symbol="BTCUSDT",
        futures_symbol="BTCUSDT",
    )
    spec = BacktestSpec(
        protocol_version="test-volume-panel",
        rule_version="test-volume-panel",
        interval="5m",
        data_start=datetime(1969, 12, 1, tzinfo=UTC),
        evaluation_start=datetime(1970, 1, 1, tzinfo=UTC),
        evaluation_end=datetime(1970, 1, 3, tzinfo=UTC),
        minimum_age_days=0,
        strategy_mode="pit_breakout_volume",
        gate_use_participation=False,
        gate_use_crowding=False,
        gate_use_higher_timeframes=False,
        include_rsi_reversals=False,
        trend_gate=0,
        completeness_gate=70,
        assets=[asset],
        splits=[
            BacktestSplit(
                name="test",
                start=datetime(1970, 1, 1, tzinfo=UTC),
                end=datetime(1970, 1, 3, tzinfo=UTC),
            )
        ],
    )
    backtester = ResearchBacktester(Settings(), spec)
    spot = [
        make_candle(index, close=100 * 1.01**index)
        for index in range(400)
    ]
    futures = [
        make_candle(
            index,
            market=Market.FUTURES,
            close=10_000 - 0.02 * index**2,
        )
        for index in range(400)
    ]
    regimes = [MarketRegime() for _ in spot]

    spot_result = backtester.run_symbol(asset, Market.SPOT, spot, regimes)
    futures_result = backtester.run_symbol(
        asset, Market.FUTURES, futures, regimes
    )

    assert spot_result.opportunities
    assert {item.direction for item in spot_result.opportunities} == {"long"}
    assert futures_result.opportunities
    assert {item.direction for item in futures_result.opportunities} == {"short"}
    assert any(
        item.analysis_eligible_12 and not item.analysis_eligible_72
        for item in spot_result.opportunities
    )
    assert all(
        item.analysis_eligible == item.analysis_eligible_12
        and item.analysis_exclusion == item.analysis_exclusion_12
        for item in (*spot_result.opportunities, *futures_result.opportunities)
    )
    f60 = next(item for item in spot_result.opportunities if item.analysis_eligible_12)
    assert f60.execution_observed is False
    assert f60.full_r2_eligible is None
    assert f60.f60_execution_model == "next_5m_open_to_12th_close_kline_proxy"
    assert f60.f60_gross_return is not None
    assert f60.f60_gross_return == pytest.approx(f60.forward_return_12)
    assert f60.f60_fee_return is not None and f60.f60_fee_return > 0
    assert f60.f60_slippage_return is not None and f60.f60_slippage_return > 0
    assert f60.f60_funding_return == 0
    assert f60.f60_net_return == pytest.approx(
        f60.f60_gross_return
        - f60.f60_fee_return
        - f60.f60_slippage_return
        + f60.f60_funding_return
    )
    assert f60.f60_gross_return == f60.signal_gross_return_12
    assert f60.f60_fee_return == f60.signal_fee_return_12
    assert f60.f60_slippage_return == f60.signal_slippage_return_12
    assert f60.f60_funding_return == f60.signal_funding_return_12
    assert f60.f60_net_return == f60.signal_net_return_12
    summary = _build_opportunity_summary([f60], spec)
    assert summary[0]["horizons"]["12"]["horizon_minutes"] == 60


def test_r3_panel_uses_exact_next_open_and_3_6_12_close_indices() -> None:
    epoch = datetime(1970, 1, 1, tzinfo=UTC)
    step = timedelta(minutes=5)
    asset = BacktestAsset(
        asset="BTC",
        cohort="anchor",
        spot_symbol="BTCUSDT",
        futures_symbol="BTCUSDT",
    )
    spec = BacktestSpec(
        protocol_version="test-r3-panel",
        rule_version="test-r3-panel",
        interval="5m",
        data_start=epoch - timedelta(days=1),
        evaluation_start=epoch,
        evaluation_end=epoch + 100 * step,
        minimum_age_days=0,
        minimum_history_bars=50,
        strategy_mode="pit_breakout_volume",
        candidate_policy="c0_frozen",
        opportunity_panel_horizon_bars=12,
        confirmation_mode="explicit_trigger",
        gate_use_participation=False,
        gate_use_crowding=False,
        gate_use_higher_timeframes=False,
        include_rsi_reversals=False,
        trend_gate=0,
        completeness_gate=70,
        assets=[asset],
        splits=[
            BacktestSplit(
                name="only",
                start=epoch,
                end=epoch + 100 * step,
            )
        ],
    )
    candles = [make_candle(index, close=100.0) for index in range(100)]
    candles[81] = candles[81].model_copy(update={"open": Decimal("100")})
    candles[85] = candles[85].model_copy(update={"close": Decimal("150")})
    candles[86] = candles[86].model_copy(update={"close": Decimal("101")})
    candles[87] = candles[87].model_copy(update={"close": Decimal("50")})
    feature = make_feature(
        market=Market.SPOT,
        symbol="BTCUSDT",
        event_time_ms=candles[80].close_time_ms,
        price=100.0,
        regime=MarketRegime(
            label="risk_on", btc_trend="bullish", breadth_ratio=0.75
        ),
    )
    evaluation = RuleEvaluation(
        market=Market.SPOT,
        symbol="BTCUSDT",
        family=SignalFamily.BREAKOUT_LONG,
        direction=Direction.LONG,
        timeframe="5m",
        event_time_ms=feature.event_time_ms,
        score=65,
        triggered=True,
        eligible=False,
        price=Decimal("100"),
        invalidation=Decimal("90"),
    )
    backtester = ResearchBacktester(Settings(), spec)
    candidate = backtester._apply_candidate_policy(feature, evaluation, {})

    opportunity = backtester._build_opportunity(
        asset,
        Market.SPOT,
        candles,
        80,
        feature,
        candidate,
        [],
    )

    assert opportunity.next_open_time_ms == candles[81].open_time_ms
    assert opportunity.reasons == ""
    assert opportunity.invalidation == 90.0
    assert opportunity.analysis_eligible_3
    assert opportunity.analysis_eligible_6
    assert opportunity.analysis_eligible_12
    assert not opportunity.analysis_eligible_72
    assert opportunity.forward_return_6 == pytest.approx(0.01)
    assert opportunity.signal_gross_return_6 == pytest.approx(0.01)
    assert opportunity.outcome_label_3 == "KLINE_PROXY_FLAT"
    assert opportunity.outcome_label_6 == "KLINE_PROXY_LONG"
    assert opportunity.long_net_return_6 is not None
    assert opportunity.short_net_return_6 is not None
    assert opportunity.long_net_return_6 > 0 > opportunity.short_net_return_6
    assert opportunity.regime == "risk_on"
    assert opportunity.btc_trend == "bullish"
    assert opportunity.breadth_ratio == 0.75

    margin_backtester = ResearchBacktester(
        Settings(), spec.model_copy(update={"outcome_edge_margin_bps": 100.0})
    )
    margin_opportunity = margin_backtester._build_opportunity(
        asset,
        Market.SPOT,
        candles,
        80,
        feature,
        candidate,
        [],
    )
    assert margin_opportunity.outcome_label_6 == "KLINE_PROXY_FLAT"

    fifteen_minute_spec = spec.model_copy(
        update={
            "interval": "15m",
            "evaluation_end": epoch + 100 * timedelta(minutes=15),
            "splits": [
                BacktestSplit(
                    name="only",
                    start=epoch,
                    end=epoch + 100 * timedelta(minutes=15),
                )
            ],
        }
    )
    fifteen_minute_candles = [
        make_candle(
            index,
            interval="15m",
            step_ms=900_000,
            close=100.0,
        )
        for index in range(100)
    ]
    fifteen_minute_feature = feature.model_copy(
        update={
            "interval": "15m",
            "event_time_ms": fifteen_minute_candles[80].close_time_ms,
        }
    )
    fifteen_minute_evaluation = evaluation.model_copy(
        update={
            "timeframe": "15m",
            "event_time_ms": fifteen_minute_feature.event_time_ms,
        }
    )
    fifteen_minute_backtester = ResearchBacktester(Settings(), fifteen_minute_spec)
    fifteen_minute_opportunity = fifteen_minute_backtester._build_opportunity(
        asset,
        Market.SPOT,
        fifteen_minute_candles,
        80,
        fifteen_minute_feature,
        fifteen_minute_backtester._apply_candidate_policy(
            fifteen_minute_feature,
            fifteen_minute_evaluation,
            {},
        ),
        [],
    )
    assert fifteen_minute_opportunity.analysis_eligible_12
    assert fifteen_minute_opportunity.signal_net_return_12 is not None
    assert fifteen_minute_opportunity.f60_execution_model == "unavailable_non_5m_interval"
    assert fifteen_minute_opportunity.f60_gross_return is None
    assert fifteen_minute_opportunity.f60_fee_return is None
    assert fifteen_minute_opportunity.f60_slippage_return is None
    assert fifteen_minute_opportunity.f60_funding_return is None
    assert fifteen_minute_opportunity.f60_net_return is None
    fifteen_minute_summary = _build_opportunity_summary(
        [fifteen_minute_opportunity], fifteen_minute_spec
    )
    assert fifteen_minute_summary[0]["horizons"]["12"]["horizon_minutes"] == 180


def test_common_h12_panel_split_and_gap_boundaries() -> None:
    epoch = datetime(1970, 1, 1, tzinfo=UTC)
    step = timedelta(minutes=5)
    asset = BacktestAsset(
        asset="BTC",
        cohort="anchor",
        spot_symbol="BTCUSDT",
        futures_symbol="BTCUSDT",
    )
    spec = BacktestSpec(
        protocol_version="test-common-h12-boundaries",
        rule_version="test-common-h12-boundaries",
        interval="5m",
        data_start=epoch - timedelta(days=1),
        evaluation_start=epoch,
        evaluation_end=epoch + 40 * step,
        minimum_age_days=0,
        minimum_history_bars=50,
        strategy_mode="pit_breakout_volume",
        candidate_policy="c0_frozen",
        opportunity_panel_horizon_bars=12,
        confirmation_mode="explicit_trigger",
        gate_use_participation=False,
        gate_use_crowding=False,
        gate_use_higher_timeframes=False,
        include_rsi_reversals=False,
        trend_gate=0,
        completeness_gate=70,
        assets=[asset],
        splits=[
            BacktestSplit(
                name="only",
                start=epoch,
                end=epoch + 40 * step,
            )
        ],
    )
    candles = [make_candle(index, close=100.0) for index in range(60)]
    backtester = ResearchBacktester(Settings(), spec)

    assert backtester._analysis_horizon_status(candles, 10, 12) == (
        False,
        "split_start_embargo",
    )
    assert backtester._analysis_horizon_status(candles, 11, 12) == (True, "")
    assert backtester._analysis_horizon_status(candles, 27, 12) == (True, "")
    assert backtester._analysis_horizon_status(candles, 28, 12) == (
        False,
        "horizon_crosses_split",
    )

    gapped = [candle for index, candle in enumerate(candles) if index != 20]
    feature = make_feature(
        market=Market.SPOT,
        symbol="BTCUSDT",
        interval="5m",
        event_time_ms=gapped[11].close_time_ms,
        price=100.0,
    )
    evaluation = RuleEvaluation(
        market=Market.SPOT,
        symbol="BTCUSDT",
        family=SignalFamily.BREAKOUT_LONG,
        direction=Direction.LONG,
        timeframe="5m",
        event_time_ms=feature.event_time_ms,
        score=65,
        triggered=True,
        eligible=False,
        price=Decimal("100"),
        invalidation=Decimal("90"),
    )
    opportunity = backtester._build_opportunity(
        asset,
        Market.SPOT,
        gapped,
        11,
        feature,
        backtester._apply_candidate_policy(feature, evaluation, {}),
        [],
    )

    assert not opportunity.analysis_eligible_3
    assert not opportunity.analysis_eligible_6
    assert not opportunity.analysis_eligible_12
    assert opportunity.analysis_exclusion_3 == "data_gap_in_horizon"
    assert opportunity.analysis_exclusion_6 == "data_gap_in_horizon"
    assert opportunity.analysis_exclusion_12 == "data_gap_in_horizon"


@pytest.mark.parametrize(
    ("opposite_eligible", "expected_reason"),
    [(False, "end_of_data"), (True, "opposite_signal")],
)
def test_opposite_breakout_exit_requires_an_eligible_trigger(
    monkeypatch: pytest.MonkeyPatch,
    opposite_eligible: bool,
    expected_reason: str,
) -> None:
    asset = BacktestAsset(
        asset="BTC",
        cohort="anchor",
        spot_symbol="BTCUSDT",
        futures_symbol="BTCUSDT",
    )
    spec = BacktestSpec(
        protocol_version="test-opposite-exit",
        rule_version="test-opposite-exit",
        interval="5m",
        data_start=datetime(1969, 12, 1, tzinfo=UTC),
        evaluation_start=datetime(1970, 1, 1, tzinfo=UTC),
        evaluation_end=datetime(1970, 1, 3, tzinfo=UTC),
        minimum_age_days=0,
        strategy_mode="pit_breakout_volume",
        gate_use_participation=False,
        gate_use_crowding=False,
        gate_use_higher_timeframes=False,
        include_rsi_reversals=False,
        trend_gate=0,
        completeness_gate=70,
        assets=[asset],
        splits=[],
    )
    candles = [make_candle(index) for index in range(220)]
    features: list[FeatureSnapshot | None] = [None] * len(candles)
    for index in range(210, len(candles)):
        features[index] = make_feature(
            market=Market.SPOT,
            symbol="BTCUSDT",
            event_time_ms=candles[index].close_time_ms,
            price=float(candles[index].close),
        )
    long = RuleEvaluation(
        market=Market.SPOT,
        symbol="BTCUSDT",
        family=SignalFamily.BREAKOUT_LONG,
        direction=Direction.LONG,
        timeframe="5m",
        event_time_ms=candles[210].close_time_ms,
        score=65,
        triggered=True,
        eligible=True,
        price=candles[210].close,
        invalidation=Decimal("90"),
    )
    opposite_short = RuleEvaluation(
        market=Market.SPOT,
        symbol="BTCUSDT",
        family=SignalFamily.BREAKDOWN_SHORT,
        direction=Direction.SHORT,
        timeframe="5m",
        event_time_ms=candles[212].close_time_ms,
        score=65,
        triggered=True,
        eligible=opposite_eligible,
        price=candles[212].close,
        invalidation=Decimal("110"),
    )

    class StubRuleEngine:
        def __init__(self, _: object) -> None:
            pass

        def evaluate(
            self, feature: FeatureSnapshot, _: object
        ) -> list[RuleEvaluation]:
            if feature.event_time_ms == long.event_time_ms:
                return [long]
            if feature.event_time_ms == opposite_short.event_time_ms:
                return [opposite_short]
            return []

    backtester = ResearchBacktester(Settings(), spec)
    monkeypatch.setattr(
        backtester,
        "_continuous_features",
        lambda _candles, _flows, _regimes: features,
    )
    monkeypatch.setattr(
        backtester,
        "_higher_timeframe_context_index",
        lambda _candles, _regimes: StrictContextIndex({}),
    )
    monkeypatch.setattr("signalbot.backtest.engine.SignalRuleEngine", StubRuleEngine)

    result = backtester.run_symbol(
        asset,
        Market.SPOT,
        candles,
        [MarketRegime() for _ in candles],
    )

    assert len(result.trades) == 1
    assert result.trades[0].exit_reason == expected_reason
    if opposite_eligible:
        assert result.trades[0].exit_time_ms == candles[213].open_time_ms


def test_r2_common_purge_and_split_boundary_isolate_positions(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    epoch = datetime(1970, 1, 1, tzinfo=UTC)
    step = timedelta(minutes=5)
    asset = BacktestAsset(
        asset="BTC",
        cohort="anchor",
        spot_symbol="BTCUSDT",
        futures_symbol="BTCUSDT",
    )
    spec = BacktestSpec(
        protocol_version="test-r2-split-isolation",
        rule_version="test-r2-split-isolation",
        interval="5m",
        data_start=epoch - timedelta(days=1),
        evaluation_start=epoch,
        evaluation_end=epoch + 260 * step,
        minimum_age_days=0,
        minimum_history_bars=50,
        strategy_mode="pit_breakout_volume",
        candidate_policy="c0_frozen",
        confirmation_mode="explicit_trigger",
        gate_use_participation=False,
        gate_use_crowding=False,
        gate_use_higher_timeframes=False,
        include_rsi_reversals=False,
        trend_gate=0,
        completeness_gate=70,
        assets=[asset],
        splits=[
            BacktestSplit(
                name="first",
                start=epoch,
                end=epoch + 200 * step,
            ),
            BacktestSplit(
                name="second",
                start=epoch + 200 * step,
                end=epoch + 260 * step,
            ),
        ],
        exits=ExitPolicySettings(max_holding_bars=1_000),
    )
    candles = [make_candle(index) for index in range(260)]
    features: list[FeatureSnapshot | None] = [None] * len(candles)
    for index in range(50, len(candles)):
        features[index] = make_feature(
            market=Market.SPOT,
            symbol="BTCUSDT",
            event_time_ms=candles[index].close_time_ms,
            price=float(candles[index].close),
        )
    trigger = RuleEvaluation(
        market=Market.SPOT,
        symbol="BTCUSDT",
        family=SignalFamily.BREAKOUT_LONG,
        direction=Direction.LONG,
        timeframe="5m",
        event_time_ms=candles[80].close_time_ms,
        score=65,
        triggered=True,
        eligible=False,
        price=candles[80].close,
        invalidation=Decimal("90"),
    )
    triggers = [trigger]

    class StubRuleEngine:
        def __init__(self, _: object) -> None:
            pass

        def evaluate(
            self,
            feature: FeatureSnapshot,
            _: object,
        ) -> list[RuleEvaluation]:
            return [
                item for item in triggers if feature.event_time_ms == item.event_time_ms
            ]

    backtester = ResearchBacktester(Settings(), spec)
    assert spec.opportunity_panel_horizon_bars == 72
    assert not backtester._common_72_path_eligible(candles, 70)
    assert backtester._common_72_path_eligible(candles, 71)
    assert backtester._common_72_path_eligible(candles, 126)
    assert not backtester._common_72_path_eligible(candles, 127)
    monkeypatch.setattr(
        backtester,
        "_higher_timeframe_context_index",
        lambda _candles, _regimes: StrictContextIndex({}),
    )
    monkeypatch.setattr(
        backtester,
        "_continuous_features",
        lambda _candles, _flows, _regimes: features,
    )
    monkeypatch.setattr("signalbot.backtest.engine.SignalRuleEngine", StubRuleEngine)

    result = backtester.run_symbol(
        asset,
        Market.SPOT,
        candles,
        [MarketRegime() for _ in candles],
    )

    assert len(result.opportunities) == 1
    opportunity = result.opportunities[0]
    assert opportunity.eligible
    assert opportunity.analysis_eligible_3
    assert opportunity.analysis_eligible_6
    assert opportunity.analysis_eligible_12
    assert opportunity.analysis_eligible_72
    assert len(result.trades) == 1
    assert result.trades[0].opportunity_id == opportunity.opportunity_id
    assert result.trades[0].split == "first"
    assert result.trades[0].split_contained is False
    assert result.trades[0].exit_reason == "split_boundary"
    assert result.trades[0].exit_time_ms == candles[200].open_time_ms

    triggers = [
        trigger.model_copy(
            update={
                "event_time_ms": candles[index].close_time_ms,
                "price": candles[index].close,
            }
        )
        for index in (70, 127)
    ]
    boundary_result = backtester.run_symbol(
        asset,
        Market.SPOT,
        candles,
        [MarketRegime() for _ in candles],
    )

    assert len(boundary_result.opportunities) == 2
    assert all(
        not opportunity.analysis_eligible_72
        for opportunity in boundary_result.opportunities
    )
    assert all(
        not opportunity.analysis_eligible_3
        and not opportunity.analysis_eligible_6
        and not opportunity.analysis_eligible_12
        for opportunity in boundary_result.opportunities
    )
    assert boundary_result.scheduled_entries == 0
    assert boundary_result.trades == ()
