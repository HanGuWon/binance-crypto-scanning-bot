from typing import Literal

import pytest

from conftest import make_feature
from signalbot.config import SignalSettings
from signalbot.domain.enums import SignalFamily, SignalStage
from signalbot.domain.models import (
    DIRECTIONAL_DIAGNOSTICS_METADATA_KEY,
    DirectionalDiagnostics,
    MarketRegime,
)
from signalbot.signals.rules import SignalRuleEngine
from signalbot.signals.state_machine import SignalStateMachine


def evaluation(feature, family: SignalFamily):
    values = SignalRuleEngine(SignalSettings()).evaluate(feature)
    return next(value for value in values if value.family is family)


def directional_diagnostics(feature) -> DirectionalDiagnostics:
    values = SignalRuleEngine(SignalSettings()).evaluate(feature)
    raw = values[0].metadata[DIRECTIONAL_DIAGNOSTICS_METADATA_KEY]
    assert all(
        value.metadata[DIRECTIONAL_DIAGNOSTICS_METADATA_KEY] == raw
        for value in values
    )
    return DirectionalDiagnostics.model_validate(raw)


def test_directional_diagnostics_reports_existing_rules_without_rescoring() -> None:
    feature = make_feature(
        price=106,
        previous_close=104,
        recent_high=105,
        relative_volume=2.2,
        macd_histogram=0.3,
        macd_histogram_previous=0.1,
        adx=30,
        taker_buy_ratio=0.65,
        ema20=102,
        ema50=100,
    )

    diagnostics = directional_diagnostics(feature)

    assert diagnostics.long.family is SignalFamily.BREAKOUT_LONG
    assert diagnostics.long.raw_score == 100
    assert diagnostics.long.triggered
    assert diagnostics.short.raw_score == 0
    assert diagnostics.feature == feature
    assert diagnostics.score_is_probability is False


def test_directional_diagnostics_mirrors_a_downward_break() -> None:
    diagnostics = directional_diagnostics(
        make_feature(
            price=94,
            previous_close=96,
            recent_low=95,
            relative_volume=2.2,
            macd_histogram=-0.3,
            macd_histogram_previous=-0.1,
            adx=30,
            taker_buy_ratio=0.35,
            ema20=98,
            ema50=100,
        )
    )

    assert diagnostics.short.family is SignalFamily.BREAKDOWN_SHORT
    assert diagnostics.short.raw_score == 100
    assert diagnostics.short.triggered
    assert diagnostics.long.raw_score == 0


def test_directional_scores_are_independent_during_conflicting_reversal_evidence() -> None:
    diagnostics = directional_diagnostics(
        make_feature(
            rsi=80,
            rsi_previous=20,
            macd_histogram=0,
            macd_histogram_previous=0,
            macd_histogram_previous2=0,
            bearish_divergence=True,
            bullish_divergence=True,
            upper_wick_ratio=0.60,
            lower_wick_ratio=0.60,
        )
    )

    assert diagnostics.long.raw_score == 65
    assert diagnostics.short.raw_score == 65
    assert diagnostics.long.raw_score + diagnostics.short.raw_score != 100


def _pullback_structure(direction: str, *, depth: float = 0.40, status: str = "ready"):
    bullish = direction == "long"
    return {
        "state": "bullish" if bullish else "bearish",
        "qualified_high_count": 2,
        "qualified_low_count": 2,
        "previous_swing_high": 100,
        "latest_swing_high": 110 if bullish else 95,
        "previous_swing_low": 90 if bullish else 100,
        "latest_swing_low": 95 if bullish else 90,
        "pullback_direction": direction,
        "pullback_status": status,
        "impulse_start": 90 if bullish else 110,
        "impulse_end": 110 if bullish else 90,
        "impulse_size_atr": 10,
        "pullback_depth": depth,
        "pullback_duration_bars": 5,
        "confluence_distance_atr": 0.20,
        "recovery_confirmed": True,
        "structure_intact": True,
    }


@pytest.mark.parametrize(
    ("family", "feature_updates", "direction"),
    [
        (
            SignalFamily.PULLBACK_LONG,
            {"price": 105, "ema20": 103, "ema50": 100, "ema20_slope_atr": 0.5},
            "long",
        ),
        (
            SignalFamily.PULLBACK_SHORT,
            {"price": 95, "ema20": 97, "ema50": 100, "ema20_slope_atr": -0.5},
            "short",
        ),
    ],
)
def test_causal_pullback_is_an_informational_setup_never_a_confirmed_entry(
    family: SignalFamily,
    feature_updates: dict[str, float],
    direction: str,
) -> None:
    settings = SignalSettings(
        gate_enabled=True,
        entry_policy="r2_pit_htf_exec",
        pullback_alert_mode="informational",
    )
    feature = make_feature(
        **feature_updates,
        chart_structure=_pullback_structure(direction),
    )
    result = next(
        item
        for item in SignalRuleEngine(settings).evaluate(feature)
        if item.family is family
    )

    assert result.metadata["raw_signal_score"] == 100
    assert result.metadata["informational_only"] is True
    assert result.metadata["threshold_status"] == "unvalidated_research_seed"
    assert result.score == 100
    assert result.triggered
    assert not result.eligible
    assert result.gate is not None and not result.gate.passed

    decision = SignalStateMachine(settings, "test-structure-v1").process(result)
    assert decision is not None
    assert decision.stage is SignalStage.SETUP


@pytest.mark.parametrize("confirmation_mode", ["explicit_trigger", "score"])
def test_gate_disabled_still_locks_informational_pullback_from_confirmation(
    confirmation_mode: Literal["explicit_trigger", "score"],
) -> None:
    settings = SignalSettings(
        confirmation_mode=confirmation_mode,
        gate_enabled=False,
        pullback_alert_mode="informational",
    )
    feature = make_feature(
        price=105,
        ema20=103,
        ema50=100,
        ema20_slope_atr=0.5,
        chart_structure=_pullback_structure("long"),
    )
    result = next(
        item
        for item in SignalRuleEngine(settings).evaluate(feature)
        if item.family is SignalFamily.PULLBACK_LONG
    )

    assert result.score == 100
    assert result.triggered
    assert not result.eligible
    decision = SignalStateMachine(settings, "test-structure-v1").process(result)
    assert decision is not None
    assert decision.stage is SignalStage.SETUP


def test_directional_diagnostics_prioritizes_promotable_breakout_over_research_score() -> None:
    settings = SignalSettings(
        confirmation_mode="explicit_trigger",
        gate_enabled=True,
        gate_use_participation=False,
        gate_use_crowding=False,
        gate_use_higher_timeframes=False,
        pullback_alert_mode="informational",
    )
    feature = make_feature(
        price=106,
        previous_close=104,
        recent_high=105,
        macd_histogram=0.3,
        macd_histogram_previous=0.1,
        adx=30,
        ema20=102,
        ema50=100,
        ema20_slope_atr=0.5,
        spread_bps=2,
        chart_structure=_pullback_structure("long"),
    )
    values = SignalRuleEngine(settings).evaluate(feature)
    breakout = next(
        item for item in values if item.family is SignalFamily.BREAKOUT_LONG
    )
    pullback = next(
        item for item in values if item.family is SignalFamily.PULLBACK_LONG
    )
    diagnostics = DirectionalDiagnostics.model_validate(
        breakout.metadata[DIRECTIONAL_DIAGNOSTICS_METADATA_KEY]
    )

    assert breakout.metadata["raw_signal_score"] == 65
    assert breakout.triggered and breakout.eligible
    assert pullback.metadata["raw_signal_score"] == 100
    assert pullback.triggered and not pullback.eligible
    assert diagnostics.long.family is SignalFamily.BREAKOUT_LONG
    assert diagnostics.long.raw_score == 65
    assert diagnostics.long.eligible


def test_pullback_depth_boundary_does_not_trigger_and_is_capped_below_setup() -> None:
    settings = SignalSettings(
        gate_enabled=True,
        entry_policy="r2_pit_htf_exec",
        pullback_alert_mode="informational",
    )
    feature = make_feature(
        price=105,
        ema20=103,
        ema50=100,
        ema20_slope_atr=0.5,
        chart_structure=_pullback_structure(
            "long", depth=0.6001, status="developing"
        ),
    )
    result = next(
        item
        for item in SignalRuleEngine(settings).evaluate(feature)
        if item.family is SignalFamily.PULLBACK_LONG
    )

    assert result.metadata["raw_signal_score"] == settings.setup_score - 1
    assert result.score == settings.setup_score - 1
    assert not result.triggered
    assert not result.eligible


def test_rsi_overbought_alone_is_not_a_short_confirmation() -> None:
    result = evaluation(make_feature(rsi=82), SignalFamily.EXHAUSTION_SHORT)
    assert result.score == 25
    assert result.score < SignalSettings().watch_score


def test_exhaustion_requires_multiple_independent_confirmations() -> None:
    feature = make_feature(
        rsi=82,
        bearish_divergence=True,
        upper_wick_ratio=0.60,
        macd_histogram=-0.3,
        macd_histogram_previous=-0.2,
        macd_histogram_previous2=-0.1,
        price=97,
        ema9=99,
        taker_buy_ratio=0.35,
        relative_volume=2.5,
    )
    result = evaluation(feature, SignalFamily.EXHAUSTION_SHORT)
    assert result.score == 100
    assert len(result.reasons) >= 6


def test_strong_higher_timeframe_trend_penalizes_countertrend_entry() -> None:
    base = dict(
        rsi=82,
        bearish_divergence=True,
        upper_wick_ratio=0.60,
        macd_histogram=-0.3,
        macd_histogram_previous=-0.2,
        macd_histogram_previous2=-0.1,
        price=97,
        ema9=99,
        taker_buy_ratio=0.35,
        relative_volume=2.5,
        adx=40,
    )
    neutral = evaluation(make_feature(**base), SignalFamily.EXHAUSTION_SHORT)
    bullish = evaluation(
        make_feature(**base, regime=MarketRegime(btc_trend="bullish")),
        SignalFamily.EXHAUSTION_SHORT,
    )
    assert neutral.score == 100
    assert bullish.score == 85
    assert neutral.score - bullish.score == 15  # raw 25-point penalty is partly hidden by cap
    assert any("penalty" in reason for reason in bullish.reasons)


def test_breakout_confirmation_and_spread_hard_gate() -> None:
    feature = make_feature(
        price=106,
        previous_close=104,
        recent_high=105,
        relative_volume=2.2,
        macd_histogram=0.3,
        macd_histogram_previous=0.1,
        adx=30,
        taker_buy_ratio=0.65,
        ema20=102,
        ema50=100,
        spread_bps=2,
    )
    result = evaluation(feature, SignalFamily.BREAKOUT_LONG)
    assert result.score >= 80
    blocked = SignalRuleEngine(SignalSettings()).evaluate(
        feature.model_copy(update={"spread_bps": 25.0})
    )
    assert all(value.score == 0 for value in blocked)


def test_squeeze_watch_stops_after_range_has_already_broken() -> None:
    feature = make_feature(
        price=106,
        recent_high=105,
        recent_low=95,
        bollinger_width_percentile=5,
        atr=2,
        ema20=102,
        ema50=100,
        taker_buy_ratio=0.60,
        spread_bps=2,
    )
    result = evaluation(feature, SignalFamily.SQUEEZE_LONG)
    assert result.score == 0


def test_exhaustion_can_confirm_on_bar_immediately_after_rsi_leaves_overbought() -> None:
    feature = make_feature(
        rsi=72,
        rsi_previous=82,
        bearish_divergence=True,
        upper_wick_ratio=0.60,
        macd_histogram=-0.3,
        macd_histogram_previous=-0.2,
        macd_histogram_previous2=-0.1,
        price=97,
        ema9=99,
        taker_buy_ratio=0.35,
        relative_volume=2.5,
    )
    result = evaluation(feature, SignalFamily.EXHAUSTION_SHORT)
    assert result.score >= 80


def test_gate_v2_disables_five_minute_rsi_reversal_family() -> None:
    settings = SignalSettings(
        gate_enabled=True,
        gate_use_higher_timeframes=False,
        reversal_intervals=["1h", "4h"],
    )
    values = SignalRuleEngine(settings).evaluate(make_feature(rsi=90))
    result = next(
        item for item in values if item.family is SignalFamily.EXHAUSTION_SHORT
    )

    assert result.score == 0
    assert "disabled" in result.reasons[0]


def test_gate_v2_is_non_compensating() -> None:
    settings = SignalSettings(
        gate_enabled=True,
        gate_use_higher_timeframes=True,
        reversal_intervals=["1h", "4h"],
    )
    feature = make_feature(
        price=106,
        previous_close=104,
        recent_high=105,
        macd_histogram=0.3,
        macd_histogram_previous=0.1,
        adx=30,
        ema20=102,
        ema50=100,
        ema20_slope_atr=0.5,
        volume_zscore=-1.0,
        trade_count_zscore=-1.0,
        taker_imbalance=-0.2,
        cvd_pressure=-0.1,
        spread_bps=11.25,
        spread_is_proxy=True,
    )
    contexts = {
        "15m": make_feature(interval="15m", price=103, ema20=102, ema50=100),
        "1h": make_feature(interval="1h", price=103, ema20=102, ema50=100),
    }
    result = next(
        item
        for item in SignalRuleEngine(settings).evaluate(feature, contexts)
        if item.family is SignalFamily.BREAKOUT_LONG
    )

    assert result.gate is not None
    assert result.gate.trend_score >= settings.trend_gate
    assert result.gate.participation_score < settings.participation_gate
    assert result.score == 65
    assert result.triggered is True
    assert result.eligible is False


def test_gate_v2_htf_off_does_not_leak_context_into_raw_score() -> None:
    settings = SignalSettings(
        gate_enabled=True,
        gate_use_participation=False,
        gate_use_crowding=False,
        gate_use_higher_timeframes=False,
        reversal_intervals=["1h", "4h"],
    )
    feature = make_feature(
        price=106,
        previous_close=104,
        recent_high=105,
        macd_histogram=0.3,
        macd_histogram_previous=0.1,
        adx=30,
        ema20=102,
        ema50=100,
        ema20_slope_atr=0.5,
        spread_bps=11.25,
        spread_is_proxy=True,
    )
    opposing = {
        "15m": make_feature(interval="15m", price=95, ema20=98, ema50=100),
        "1h": make_feature(interval="1h", price=95, ema20=98, ema50=100),
    }
    engine = SignalRuleEngine(settings)
    without_context = next(
        item
        for item in engine.evaluate(feature, {})
        if item.family is SignalFamily.BREAKOUT_LONG
    )
    with_context = next(
        item
        for item in engine.evaluate(feature, opposing)
        if item.family is SignalFamily.BREAKOUT_LONG
    )

    assert without_context.metadata["raw_signal_score"] == 65
    assert with_context.metadata["raw_signal_score"] == 65
    assert without_context.score == 65
    assert with_context.score == 65
    assert with_context.triggered is True
    assert with_context.eligible is True
    assert with_context.gate is not None and with_context.gate.passed


def test_gate_v2_score_mode_preserves_legacy_confirmation_promotion() -> None:
    settings = SignalSettings(
        confirmation_mode="score",
        gate_enabled=True,
        gate_use_participation=True,
        gate_use_crowding=False,
        gate_use_higher_timeframes=False,
    )
    feature = make_feature(
        price=106,
        previous_close=104,
        recent_high=105,
        macd_histogram=0.3,
        macd_histogram_previous=0.1,
        adx=30,
        ema20=102,
        ema50=100,
        ema20_slope_atr=0.5,
        volume_zscore=1.0,
        taker_imbalance=0.10,
        cvd_pressure=0.05,
        spread_bps=2,
    )
    result = next(
        item
        for item in SignalRuleEngine(settings).evaluate(feature)
        if item.family is SignalFamily.BREAKOUT_LONG
    )

    assert result.metadata["raw_signal_score"] == 65
    assert result.score == settings.confirmed_score
    assert result.eligible is True

    failed = next(
        item
        for item in SignalRuleEngine(settings).evaluate(
            feature.model_copy(
                update={
                    "volume_zscore": -1.0,
                    "trade_count_zscore": -1.0,
                    "taker_imbalance": -0.2,
                    "cvd_pressure": -0.1,
                }
            )
        )
        if item.family is SignalFamily.BREAKOUT_LONG
    )
    assert failed.metadata["raw_signal_score"] == 65
    assert failed.score == 0
    assert failed.eligible is False


def test_explicit_squeeze_keeps_raw_85_as_an_untriggered_setup() -> None:
    settings = SignalSettings(
        confirmation_mode="explicit_trigger",
        gate_enabled=True,
        gate_use_participation=False,
        gate_use_crowding=False,
        gate_use_higher_timeframes=False,
    )
    feature = make_feature(
        price=104,
        recent_high=105,
        recent_low=95,
        bollinger_width_percentile=5,
        atr=2,
        atr_percent=2,
        ema20=102,
        ema50=100,
        ema20_slope_atr=0.5,
        adx=30,
        spread_bps=2,
    )
    result = next(
        item
        for item in SignalRuleEngine(settings).evaluate(feature)
        if item.family is SignalFamily.SQUEEZE_LONG
    )

    assert result.score == 85
    assert result.triggered is False
    assert result.eligible is True


@pytest.mark.parametrize(
    "changes",
    [
        {"price": 105.0},
        {"macd_histogram": 0.1, "macd_histogram_previous": 0.1},
        {"adx": 19.999},
        {"ema20": 100.0, "ema50": 100.0},
    ],
)
def test_explicit_breakout_trigger_requires_crossing_and_all_confirmations(
    changes: dict[str, float],
) -> None:
    settings = SignalSettings(
        confirmation_mode="explicit_trigger",
        gate_enabled=True,
        gate_use_participation=False,
        gate_use_crowding=False,
        gate_use_higher_timeframes=False,
    )
    values = {
        "price": 106.0,
        "previous_close": 104.0,
        "recent_high": 105.0,
        "macd_histogram": 0.3,
        "macd_histogram_previous": 0.1,
        "adx": 30.0,
        "ema20": 102.0,
        "ema50": 100.0,
        "ema20_slope_atr": 0.5,
        "spread_bps": 2.0,
    }
    values.update(changes)
    result = next(
        item
        for item in SignalRuleEngine(settings).evaluate(make_feature(**values))
        if item.family is SignalFamily.BREAKOUT_LONG
    )

    assert result.triggered is False


def test_explicit_breakdown_trigger_is_the_exact_mirror() -> None:
    settings = SignalSettings(
        confirmation_mode="explicit_trigger",
        gate_enabled=True,
        gate_use_participation=False,
        gate_use_crowding=False,
        gate_use_higher_timeframes=False,
    )
    feature = make_feature(
        price=94,
        previous_close=96,
        recent_low=95,
        macd_histogram=-0.3,
        macd_histogram_previous=-0.1,
        adx=30,
        ema20=98,
        ema50=100,
        ema20_slope_atr=-0.5,
        spread_bps=2,
    )
    result = next(
        item
        for item in SignalRuleEngine(settings).evaluate(feature)
        if item.family is SignalFamily.BREAKDOWN_SHORT
    )

    assert result.score == 65
    assert result.triggered is True
    assert result.eligible is True


def test_r2_live_policy_emits_only_executable_spot_long_candidate() -> None:
    settings = SignalSettings(
        gate_enabled=True,
        entry_policy="r2_pit_htf_exec",
        gate_use_participation=False,
        gate_use_crowding=False,
        gate_use_higher_timeframes=True,
        reversal_intervals=[],
    )
    feature = make_feature(
        market="spot",
        price=106,
        previous_close=104,
        recent_high=105,
        macd_histogram=0.3,
        macd_histogram_previous=0.1,
        adx=30,
        ema20=102,
        ema50=100,
        spread_bps=10,
        spread_is_proxy=False,
        book_age_ms=2_000,
        ask_quote_capacity=100,
        bid_quote_capacity=100,
    )
    contexts = {
        "15m": make_feature(
            interval="15m",
            event_time_ms=1_709_999_000_000,
            price=103,
            ema20=102,
            ema50=100,
        ),
        "1h": make_feature(
            interval="1h",
            event_time_ms=1_709_996_000_000,
            price=103,
            ema20=102,
            ema50=100,
        ),
    }
    values = SignalRuleEngine(settings).evaluate(feature, contexts)
    breakout = next(
        item for item in values if item.family is SignalFamily.BREAKOUT_LONG
    )

    assert breakout.triggered
    assert breakout.eligible
    assert breakout.gate is not None and breakout.gate.passed

    missing_quantity = next(
        item
        for item in SignalRuleEngine(settings).evaluate(
            feature.model_copy(update={"ask_quote_capacity": None}),
            contexts,
        )
        if item.family is SignalFamily.BREAKOUT_LONG
    )
    assert missing_quantity.score == 0
    assert not missing_quantity.eligible
