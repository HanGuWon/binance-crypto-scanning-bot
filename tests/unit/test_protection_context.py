from __future__ import annotations

import pytest

from conftest import make_candle, make_feature
from signalbot.domain.enums import Market
from signalbot.domain.models import ChartStructureSnapshot
from signalbot.signals.protection_context import (
    PROTECTION_CONTEXT_VERSION,
    ProtectionContext,
    ProtectionTrendState,
)


def _primary(*, is_closed: bool = True, structure: ChartStructureSnapshot | None = None):
    candle = make_candle(
        100,
        market=Market.FUTURES,
        symbol="BTCUSDT",
        is_closed=is_closed,
        close=100.0,
    )
    feature = make_feature(
        market=Market.FUTURES,
        symbol="BTCUSDT",
        interval="5m",
        event_time_ms=candle.close_time_ms,
        price=100.0,
        ema20=99.0,
        ema50=98.0,
        atr=2.0,
        data_completeness=0.99,
        chart_structure=structure or ChartStructureSnapshot(),
    )
    return candle, feature


def _htf(interval: str, event_time_ms: int):
    return make_feature(
        market=Market.FUTURES,
        symbol="BTCUSDT",
        interval=interval,
        event_time_ms=event_time_ms,
        price=101.0,
        ema20=100.0,
        ema50=99.0,
        atr=3.0,
        data_completeness=1.0,
    )


def test_builds_positive_closed_candle_context_from_existing_features() -> None:
    structure = ChartStructureSnapshot(
        state="bullish",
        qualified_high_count=2,
        qualified_low_count=2,
        latest_swing_high=105.0,
        latest_swing_low=96.0,
    )
    candle, feature = _primary(structure=structure)
    context = ProtectionContext.from_closed_candle(
        candle=candle,
        feature=feature,
        higher_timeframe_contexts={
            "15m": _htf("15m", candle.close_time_ms - 900_000),
            "1h": _htf("1h", candle.close_time_ms - 3_600_000),
        },
        source_decision_clock_id="futures:BTCUSDT:5m:30299999",
        consecutive_trend_failure_count=1,
        context_freshness_ms=250,
    )

    assert context.context_version == PROTECTION_CONTEXT_VERSION
    assert context.symbol == "BTCUSDT"
    assert context.close == 100.0
    assert context.atr == 2.0
    assert context.confirmed_swing_support == 96.0
    assert context.confirmed_swing_resistance == 105.0
    assert context.trend_state is ProtectionTrendState.BULLISH
    assert context.consecutive_trend_failure_count == 1
    assert context.data_completeness == 0.99
    assert context.context_freshness_ms == 250
    assert [item.interval for item in context.higher_timeframes] == ["15m", "1h"]
    assert len(context.context_id) == 64


def test_open_candle_is_rejected() -> None:
    candle, feature = _primary(is_closed=False)
    with pytest.raises(ValueError, match="fully closed candle"):
        ProtectionContext.from_closed_candle(
            candle=candle,
            feature=feature,
            higher_timeframe_contexts={},
            source_decision_clock_id="clock-1",
        )


def test_stale_higher_timeframe_context_is_rejected() -> None:
    candle, feature = _primary()
    stale_15m = _htf("15m", candle.close_time_ms - 1_800_001)
    with pytest.raises(ValueError, match="higher-timeframe context is stale"):
        ProtectionContext.from_closed_candle(
            candle=candle,
            feature=feature,
            higher_timeframe_contexts={"15m": stale_15m},
            source_decision_clock_id="clock-1",
        )


def test_unavailable_structure_remains_none() -> None:
    candle, feature = _primary(structure=ChartStructureSnapshot(state="unavailable"))
    context = ProtectionContext.from_closed_candle(
        candle=candle,
        feature=feature,
        higher_timeframe_contexts={},
        source_decision_clock_id="clock-1",
    )

    assert context.confirmed_swing_support is None
    assert context.confirmed_swing_resistance is None


def test_context_id_is_deterministic_and_payload_sensitive() -> None:
    candle, feature = _primary()
    kwargs = {
        "candle": candle,
        "feature": feature,
        "higher_timeframe_contexts": {
            "15m": _htf("15m", candle.close_time_ms - 900_000),
        },
        "source_decision_clock_id": "clock-1",
    }
    first = ProtectionContext.from_closed_candle(**kwargs)
    second = ProtectionContext.from_closed_candle(**kwargs)
    changed = ProtectionContext.from_closed_candle(
        **kwargs,
        consecutive_trend_failure_count=1,
    )

    assert first.context_id == second.context_id
    assert first.context_id != changed.context_id

    payload = first.model_dump(mode="json")
    payload["context_id"] = "0" * 64
    with pytest.raises(ValueError, match="deterministic payload identity"):
        ProtectionContext.model_validate(payload)


def test_higher_timeframe_must_be_strictly_prior() -> None:
    candle, feature = _primary()
    same_close = _htf("15m", candle.close_time_ms)
    with pytest.raises(ValueError, match="strictly prior"):
        ProtectionContext.from_closed_candle(
            candle=candle,
            feature=feature,
            higher_timeframe_contexts={"15m": same_close},
            source_decision_clock_id="clock-1",
        )
