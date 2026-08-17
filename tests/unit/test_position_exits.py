from decimal import Decimal

import pytest

from conftest import make_candle, make_decision, make_feature
from signalbot.backtest.config import ExitPolicySettings
from signalbot.config import TechnicalExitSettings
from signalbot.domain.enums import Direction, Market, SignalFamily, SignalStage
from signalbot.domain.models import (
    Candle,
    FeatureSnapshot,
    MarketRegime,
    SignalDecision,
)
from signalbot.signals.positions import (
    ExitReason,
    PaperPositionLifecycle,
    TechnicalExitEngine,
)


def _feature(candle: Candle, **updates: object) -> FeatureSnapshot:
    return make_feature(
        market=candle.market,
        symbol=candle.symbol,
        interval=candle.interval,
        event_time_ms=candle.close_time_ms,
        price=float(candle.close),
        **updates,
    )


def _entry(
    candle: Candle,
    *,
    event_id: str = "entry-1",
    direction: Direction = Direction.LONG,
    invalidation: Decimal = Decimal("90"),
) -> SignalDecision:
    family = (
        SignalFamily.BREAKOUT_LONG
        if direction is Direction.LONG
        else SignalFamily.BREAKDOWN_SHORT
    )
    return make_decision(
        event_id=event_id,
        market=candle.market,
        symbol=candle.symbol,
        family=family,
        stage=SignalStage.CONFIRMED,
        direction=direction,
        timeframe=candle.interval,
        event_time_ms=candle.close_time_ms,
        price=candle.close,
        invalidation=invalidation,
    )


def _lifecycle(
    *,
    market: Market = Market.SPOT,
    enabled: bool = True,
    trend_failure_bars: int = 3,
    trailing_activation_r: float = 1.0,
    trailing_atr_multiple: float = 2.0,
    max_holding_bars: int = 72,
    maximum_symbols: int = 2,
) -> PaperPositionLifecycle:
    return PaperPositionLifecycle(
        TechnicalExitSettings(
            enabled=enabled,
            trend_failure_bars=trend_failure_bars,
            trailing_activation_r=trailing_activation_r,
            trailing_atr_multiple=trailing_atr_multiple,
            max_holding_bars=max_holding_bars,
        ),
        rule_version="test-paper-v1",
        market=market,
        primary_interval="5m",
        maximum_symbols=maximum_symbols,
    )


def test_long_gap_beyond_invalidation_cancels_entry() -> None:
    engine = TechnicalExitEngine(ExitPolicySettings())
    decision = make_decision(direction=Direction.LONG, invalidation=Decimal("98"))
    candle = make_candle(3, close=97).model_copy(update={"open": Decimal("97")})
    assert engine.open_position(decision, candle, 3) is None


def test_short_stop_is_mirrored_and_gap_fills_at_open() -> None:
    engine = TechnicalExitEngine(ExitPolicySettings())
    decision = make_decision(
        family=SignalFamily.BREAKDOWN_SHORT,
        direction=Direction.SHORT,
        invalidation=Decimal("102"),
    )
    entry = make_candle(3, close=100).model_copy(update={"open": Decimal("100")})
    position = engine.open_position(decision, entry, 3)
    assert position is not None

    gap = engine.stop_at_open(position, 103.0)
    assert gap is not None
    assert gap.price == 103.0
    assert gap.reason is ExitReason.INITIAL_STOP


def test_paper_exit_engine_revalidates_model_copy_updates() -> None:
    engine = TechnicalExitEngine(ExitPolicySettings())
    malformed = make_decision().model_copy(update={"direction": Direction.SHORT})

    with pytest.raises(ValueError, match="incompatible with direction"):
        engine.open_position(malformed, make_candle(3), 3)


def test_trailing_stop_uses_closed_bar_then_acts_on_next_bar() -> None:
    engine = TechnicalExitEngine(
        ExitPolicySettings(trailing_activation_r=1.0, trailing_atr_multiple=2.0)
    )
    decision = make_decision(direction=Direction.LONG, invalidation=Decimal("98"))
    entry = make_candle(3, close=100).model_copy(
        update={"open": Decimal("100"), "high": Decimal("103"), "low": Decimal("99")}
    )
    position = engine.open_position(decision, entry, 3)
    assert position is not None
    assert engine.stop_in_bar(position, entry) is None

    reason = engine.after_close(position, entry, make_feature(price=102, atr=1), 3, False)
    assert reason is None
    assert position.active_stop == 101.0
    assert position.active_stop_reason is ExitReason.TRAILING_STOP

    next_bar = make_candle(4, close=101).model_copy(
        update={"open": Decimal("102"), "low": Decimal("100.5")}
    )
    fill = engine.stop_in_bar(position, next_bar)
    assert fill is not None
    assert fill.price == 101.0


def test_two_trend_failures_schedule_exit() -> None:
    engine = TechnicalExitEngine(ExitPolicySettings(trend_failure_bars=2))
    decision = make_decision(direction=Direction.LONG, invalidation=Decimal("90"))
    candle = make_candle(3, close=100).model_copy(update={"open": Decimal("100")})
    position = engine.open_position(decision, candle, 3)
    assert position is not None
    feature = make_feature(price=99, ema20=100, macd_histogram=-0.1)

    assert engine.after_close(position, candle, feature, 3, False) is None
    assert engine.after_close(position, candle, feature, 4, False) is ExitReason.TREND_FAILURE


def test_opposite_confirmed_has_priority_over_trend_and_time() -> None:
    engine = TechnicalExitEngine(
        ExitPolicySettings(trend_failure_bars=1, max_holding_bars=1)
    )
    decision = make_decision(
        family=SignalFamily.BREAKDOWN_SHORT,
        direction=Direction.SHORT,
        invalidation=Decimal("110"),
    )
    candle = make_candle(3, close=100).model_copy(update={"open": Decimal("100")})
    position = engine.open_position(decision, candle, 3)
    assert position is not None
    feature = make_feature(price=101, ema20=100, macd_histogram=0.1)

    assert (
        engine.after_close(position, candle, feature, 3, True)
        is ExitReason.OPPOSITE_SIGNAL
    )


def test_time_exit_occurs_exactly_at_max_holding_boundary() -> None:
    engine = TechnicalExitEngine(
        ExitPolicySettings(trend_failure_bars=2, max_holding_bars=3)
    )
    decision = make_decision(direction=Direction.LONG, invalidation=Decimal("90"))
    candle = make_candle(10, close=100).model_copy(update={"open": Decimal("100")})
    position = engine.open_position(decision, candle, 10)
    assert position is not None
    healthy = make_feature(price=101, ema20=100, macd_histogram=0.1)

    assert engine.after_close(position, candle, healthy, 10, False) is None
    assert engine.after_close(position, candle, healthy, 11, False) is None
    assert (
        engine.after_close(position, candle, healthy, 12, False)
        is ExitReason.TIME_EXIT
    )


def test_paper_lifecycle_enters_at_next_bar_open_and_initial_stop_is_inclusive() -> None:
    lifecycle = _lifecycle()
    signal_bar = make_candle(0, close=100)
    entry = _entry(signal_bar, invalidation=Decimal("99"))

    assert lifecycle.on_closed_candle(signal_bar, _feature(signal_bar), [entry]) == []
    assert lifecycle.pending_entry_count == 1
    assert lifecycle.active_position_count == 0

    entry_bar = make_candle(1, close=100).model_copy(
        update={
            "open": Decimal("100"),
            "high": Decimal("101"),
            "low": Decimal("99"),
        }
    )
    exits = lifecycle.on_closed_candle(entry_bar, _feature(entry_bar), [])

    assert len(exits) == 1
    assert exits[0].metadata["entry_time_ms"] == entry_bar.open_time_ms
    assert exits[0].metadata["exit_reason"] == ExitReason.INITIAL_STOP.value
    assert exits[0].metadata["execution_model"] == "paper_stop_touch_in_entry_bar"
    assert exits[0].metadata["held_bars"] == 1
    assert exits[0].action_label == "SPOT_EXIT"
    assert exits[0].metadata["paper_only"] is True
    assert exits[0].metadata["order_placed"] is False
    assert exits[0].invalidation == Decimal("99.0")


def test_paper_trailing_stop_updates_after_close_and_acts_on_next_bar() -> None:
    lifecycle = _lifecycle(trailing_activation_r=1.0, trailing_atr_multiple=2.0)
    signal_bar = make_candle(0, close=100)
    lifecycle.on_closed_candle(
        signal_bar,
        _feature(signal_bar),
        [_entry(signal_bar, invalidation=Decimal("98"))],
    )
    activation_bar = make_candle(1, close=102).model_copy(
        update={
            "open": Decimal("100"),
            "high": Decimal("103"),
            "low": Decimal("99"),
        }
    )
    assert lifecycle.on_closed_candle(
        activation_bar,
        _feature(activation_bar, atr=1, ema20=100, macd_histogram=0.1),
        [],
    ) == []

    touch_bar = make_candle(2, close=102).model_copy(
        update={
            "open": Decimal("102"),
            "high": Decimal("103"),
            "low": Decimal("101"),
        }
    )
    exits = lifecycle.on_closed_candle(touch_bar, _feature(touch_bar, atr=1), [])

    assert len(exits) == 1
    assert exits[0].metadata["exit_reason"] == ExitReason.TRAILING_STOP.value
    assert exits[0].price == Decimal("101.0")
    assert exits[0].event_time_ms == touch_bar.close_time_ms
    assert exits[0].metadata["regime_context_source"] == "observation_closed_primary"
    assert exits[0].metadata["regime_observed_at_ms"] == touch_bar.close_time_ms


def test_trend_failure_is_pending_until_next_open_with_completed_bar_count() -> None:
    lifecycle = _lifecycle(trend_failure_bars=2)
    signal_bar = make_candle(0, close=100)
    lifecycle.on_closed_candle(signal_bar, _feature(signal_bar), [_entry(signal_bar)])
    failing = {"ema20": 101, "macd_histogram": -0.1, "atr": 1}

    first = make_candle(1, close=99.5)
    second = make_candle(2, close=99.0)
    assert lifecycle.on_closed_candle(first, _feature(first, **failing), []) == []
    prior_regime = MarketRegime(
        label="pre_exit",
        btc_trend="bearish",
        breadth_ratio=0.25,
    )
    assert lifecycle.on_closed_candle(
        second,
        _feature(second, **failing, regime=prior_regime),
        [],
    ) == []

    exit_bar = make_candle(3, close=98.5).model_copy(
        update={"open": Decimal("98.75"), "low": Decimal("98")}
    )
    exits = lifecycle.on_closed_candle(
        exit_bar,
        _feature(
            exit_bar,
            regime=MarketRegime(
                label="post_fill",
                btc_trend="bullish",
                breadth_ratio=0.8,
            ),
        ),
        [],
    )

    assert len(exits) == 1
    assert exits[0].metadata["exit_reason"] == ExitReason.TREND_FAILURE.value
    assert exits[0].metadata["execution_model"] == "paper_next_bar_open"
    assert exits[0].metadata["held_bars"] == 2
    assert exits[0].event_time_ms == exit_bar.open_time_ms
    assert exits[0].metadata["observed_at_closed_candle_ms"] == exit_bar.close_time_ms
    assert exits[0].regime == prior_regime
    assert exits[0].metadata["regime_context_source"] == "strict_prior_closed_primary"
    assert exits[0].metadata["regime_observed_at_ms"] == second.close_time_ms


def test_time_exit_boundary_counts_closed_bars_and_fills_at_next_open() -> None:
    lifecycle = _lifecycle(max_holding_bars=2)
    signal_bar = make_candle(0, close=100)
    lifecycle.on_closed_candle(signal_bar, _feature(signal_bar), [_entry(signal_bar)])
    healthy = {"ema20": 99, "macd_histogram": 0.1, "atr": 1}

    first = make_candle(1, close=100.5)
    boundary = make_candle(2, close=101)
    assert lifecycle.on_closed_candle(first, _feature(first, **healthy), []) == []
    assert lifecycle.on_closed_candle(boundary, _feature(boundary, **healthy), []) == []

    next_bar = make_candle(3, close=101.5).model_copy(
        update={"open": Decimal("101.25")}
    )
    exits = lifecycle.on_closed_candle(next_bar, _feature(next_bar), [])

    assert len(exits) == 1
    assert exits[0].metadata["exit_reason"] == ExitReason.TIME_EXIT.value
    assert exits[0].metadata["held_bars"] == 2
    assert exits[0].price == Decimal("101.25")


def test_spot_opposite_confirmed_exit_is_pending_and_short_is_not_new_entry() -> None:
    lifecycle = _lifecycle()
    signal_bar = make_candle(0, close=100)
    lifecycle.on_closed_candle(signal_bar, _feature(signal_bar), [_entry(signal_bar)])

    entry_bar = make_candle(1, close=100)
    opposite = _entry(
        entry_bar,
        event_id="opposite-short",
        direction=Direction.SHORT,
        invalidation=Decimal("110"),
    )
    assert lifecycle.on_closed_candle(entry_bar, _feature(entry_bar), [opposite]) == []

    exit_bar = make_candle(2, close=99).model_copy(
        update={"open": Decimal("99.25")}
    )
    exits = lifecycle.on_closed_candle(exit_bar, _feature(exit_bar), [])
    assert len(exits) == 1
    assert exits[0].metadata["exit_reason"] == ExitReason.OPPOSITE_SIGNAL.value
    assert exits[0].action_label == "SPOT_EXIT"
    assert lifecycle.pending_entry_count == 0


def test_disabled_lifecycle_does_not_track_or_emit() -> None:
    lifecycle = _lifecycle(enabled=False)
    candle = make_candle(0)
    assert lifecycle.on_closed_candle(candle, _feature(candle), [_entry(candle)]) == []
    assert lifecycle.tracked_symbol_count == 0
    assert lifecycle.reset_for_gap(make_candle(2)) == []


def test_gap_fail_closes_position_and_cancels_symbol_state() -> None:
    lifecycle = _lifecycle()
    signal_bar = make_candle(0, close=100)
    lifecycle.on_closed_candle(signal_bar, _feature(signal_bar), [_entry(signal_bar)])
    entry_bar = make_candle(1, close=100)
    pre_gap_regime = MarketRegime(
        label="pre_gap",
        btc_trend="bearish",
        breadth_ratio=0.2,
    )
    lifecycle.on_closed_candle(
        entry_bar,
        _feature(entry_bar, regime=pre_gap_regime),
        [],
    )
    assert lifecycle.active_position_count == 1

    post_gap = make_candle(3, close=97).model_copy(
        update={"open": Decimal("98")}
    )
    exits = lifecycle.on_closed_candle(post_gap, _feature(post_gap), [])

    assert len(exits) == 1
    assert exits[0].metadata["exit_reason"] == ExitReason.DATA_GAP.value
    assert exits[0].metadata["execution_model"] == "paper_first_post_gap_open"
    assert exits[0].metadata["held_bars"] == 1
    assert exits[0].regime == pre_gap_regime
    assert exits[0].metadata["regime_context_source"] == "last_pre_gap_closed_primary"
    assert exits[0].metadata["regime_observed_at_ms"] == entry_bar.close_time_ms
    assert lifecycle.active_position_count == 0
    assert lifecycle.pending_entry_count == 0


def test_prune_removes_active_and_pending_state_and_enforces_bound() -> None:
    lifecycle = _lifecycle(maximum_symbols=1)
    btc = make_candle(0, symbol="BTCUSDT")
    lifecycle.on_closed_candle(btc, _feature(btc), [_entry(btc)])
    assert lifecycle.tracked_symbol_count == 1

    eth = make_candle(0, symbol="ETHUSDT")
    with pytest.raises(RuntimeError, match="symbol bound"):
        lifecycle.on_closed_candle(eth, _feature(eth), [])

    assert lifecycle.prune_symbols({"ETHUSDT"}) == 1
    assert lifecycle.tracked_symbol_count == 0


def test_replayed_closed_candles_do_not_duplicate_or_advance_time_exit() -> None:
    lifecycle = _lifecycle(max_holding_bars=2)
    signal_bar = make_candle(0)
    entry = _entry(signal_bar)
    lifecycle.on_closed_candle(signal_bar, _feature(signal_bar), [entry])
    assert lifecycle.on_closed_candle(signal_bar, _feature(signal_bar), [entry]) == []
    assert lifecycle.pending_entry_count == 1

    first = make_candle(1, close=100.5)
    lifecycle.on_closed_candle(first, _feature(first, ema20=99), [])
    assert lifecycle.on_closed_candle(first, _feature(first, ema20=99), []) == []
    boundary = make_candle(2, close=101)
    assert lifecycle.on_closed_candle(boundary, _feature(boundary, ema20=99), []) == []
    next_bar = make_candle(3, close=101.5)
    exits = lifecycle.on_closed_candle(next_bar, _feature(next_bar), [])
    assert len(exits) == 1
    assert exits[0].metadata["held_bars"] == 2


def test_risk_exit_recursion_and_invalid_stops_never_schedule_entries() -> None:
    lifecycle = _lifecycle(market=Market.FUTURES)
    candle = make_candle(0, market=Market.FUTURES)
    risk = _entry(candle).model_copy(
        update={
            "family": SignalFamily.PUMP_RISK,
            "direction": Direction.RISK_UP,
        }
    )
    technical_exit = _entry(candle).model_copy(
        update={"family": SignalFamily.TECHNICAL_EXIT}
    )
    invalid_stop = _entry(candle, invalidation=Decimal("110"))

    lifecycle.on_closed_candle(
        candle,
        _feature(candle),
        [risk, technical_exit, invalid_stop],
    )
    assert lifecycle.pending_entry_count == 0


def test_technical_exit_action_labels_distinguish_market_and_position_side() -> None:
    base = make_decision(family=SignalFamily.TECHNICAL_EXIT)
    spot = base.model_copy(update={"market": Market.SPOT, "direction": Direction.LONG})
    futures_long = base.model_copy(
        update={"market": Market.FUTURES, "direction": Direction.LONG}
    )
    futures_short = base.model_copy(
        update={"market": Market.FUTURES, "direction": Direction.SHORT}
    )

    assert spot.action_label == "SPOT_EXIT"
    assert futures_long.action_label == "FUTURES_LONG_EXIT"
    assert futures_short.action_label == "FUTURES_SHORT_EXIT"
