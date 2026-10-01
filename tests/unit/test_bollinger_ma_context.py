from __future__ import annotations

import json
from decimal import Decimal
from typing import Any, cast

import pytest

from conftest import make_candle, make_feature
from signalbot.alerts.embeds import build_discord_payload
from signalbot.alerts.policy import should_deliver_discord
from signalbot.config import AlertSettings, MarketWatchSettings, SignalSettings
from signalbot.domain.enums import Market, SignalFamily
from signalbot.domain.models import Candle, FeatureSnapshot, Instrument, RuleEvaluation
from signalbot.persistence.repository import SqlRepository
from signalbot.signals.market_watch import MarketWatchEngine
from signalbot.signals.state_machine import SignalStateMachine


def scenario(
    price: float = 110,
    fast: float = 105,
    slow: float = 100,
    slope: float = 0.4,
    *,
    upper: bool = True,
    lower: bool = False,
) -> tuple[MarketWatchEngine, Candle, FeatureSnapshot]:
    engine = MarketWatchEngine(MarketWatchSettings(enabled=True, context_alerts_enabled=True))
    engine.set_priority_gainers(
        (
            Instrument(
                market=Market.FUTURES,
                symbol="TESTUSDT",
                base_asset="TEST",
                quote_asset="USDT",
                status="TRADING",
                quote_volume=Decimal(200_000_000),
                price_change_percent_24h=12,
            ),
        )
    )
    candle = make_candle(220, market=Market.FUTURES, symbol="TESTUSDT", close=price)
    candle = candle.model_copy(
        update={"high": Decimal(str(price + 2)), "low": Decimal(str(price - 2))}
    )
    feature = make_feature(
        market=Market.FUTURES,
        symbol="TESTUSDT",
        event_time_ms=candle.close_time_ms,
        price=price,
        ema20=fast,
        ema50=slow,
        ema20_slope_atr=slope,
        macd_histogram=0.5,
        rsi=61,
        bollinger_middle=price + 0.25,
        bollinger_upper=price + (1 if upper else 10),
        bollinger_lower=price - (1 if lower else 10),
    )
    return engine, candle, feature


def context(evaluation: RuleEvaluation) -> dict[str, Any]:
    return cast(dict[str, Any], evaluation.metadata["ma_context"])


@pytest.mark.parametrize(
    ("price", "fast", "slow", "slope", "upper", "lower", "setup"),
    [
        (110, 105, 100, 0.4, True, False, "up_continuation"),
        (104, 105, 100, 0.4, True, False, "up_upper_weakening"),
        (104, 105, 100, 0.4, False, True, "up_pullback"),
        (99, 105, 100, 0.4, False, True, "up_lower_weakening"),
        (90, 95, 100, -0.4, False, True, "down_continuation"),
        (96, 95, 100, -0.4, False, True, "down_lower_weakening"),
        (96, 95, 100, -0.4, True, False, "down_rebound"),
        (101, 95, 100, -0.4, True, False, "down_upper_weakening"),
        (110, 105, 105, 0.4, True, False, "mixed"),
        (110, 105, 100, 0, True, False, "mixed"),
        (110, 105, 100, -0.4, True, False, "mixed"),
        (110, 105, 100, 0.4, True, True, "both"),
        (110, 105, 100, 0.4, False, False, "none"),
    ],
)
def test_closed_band_contact_uses_array_slope_and_price_conditions(
    price: float,
    fast: float,
    slow: float,
    slope: float,
    upper: bool,
    lower: bool,
    setup: str,
) -> None:
    engine, candle, feature = scenario(price, fast, slow, slope, upper=upper, lower=lower)
    evaluations = engine.evaluate(candle, feature)
    result = context(evaluations[0])
    assert result["band_setup"] == setup
    assert result["as_of_ms"] == candle.close_time_ms
    assert all(context(item) == result for item in evaluations)
    assert all(not item.eligible for item in evaluations)
    json.dumps(result, allow_nan=False)
    assert "반전 확정" not in result["band_interpretation"]
    if setup == "both":
        assert "방향 판단 보류" in result["band_interpretation"]


@pytest.mark.parametrize(
    ("price", "upper", "setup", "equal_line"),
    [
        (105, True, "up_continuation", "price_vs_ema20"),
        (100, False, "up_pullback", "price_vs_ema50"),
    ],
)
def test_exact_ma_boundary_is_preserved_as_equal(
    price: float,
    upper: bool,
    setup: str,
    equal_line: str,
) -> None:
    engine, candle, feature = scenario(price, upper=upper, lower=not upper)
    result = context(engine.evaluate(candle, feature)[0])
    assert result[equal_line] == "equal" and result["band_setup"] == setup


def test_sma_middle_is_distinct_optional_and_not_replaced_by_ema() -> None:
    engine, candle, feature = scenario()
    result = context(engine.evaluate(candle, feature)[0])
    assert result["price_vs_ema20"] == "above" and result["price_vs_sma20"] == "below"
    missing = feature.model_copy(update={"bollinger_middle": None, "bollinger_upper": None})
    evaluations = engine.evaluate(candle, missing)
    assert context(evaluations[0])["price_vs_sma20"] == "unavailable"
    assert context(evaluations[0])["band_setup"] == "none"
    assert all(item.family is not SignalFamily.BOLLINGER_UPPER_TOUCH for item in evaluations)


def test_composite_bullish_label_can_disagree_with_strict_ma_context() -> None:
    engine, candle, feature = scenario(fast=100, slow=105)
    top = engine.evaluate(candle, feature)[0]
    assert top.metadata["trend_label"] == "상승 추세"
    assert context(top)["alignment"] == "mixed"


def test_bad_numeric_data_is_unavailable_not_mixed() -> None:
    engine, candle, feature = scenario()
    invalid = feature.model_copy(update={"ema20": float("nan")})
    result = context(engine.evaluate(candle, invalid)[0])
    assert result["alignment"] == "unavailable" and result["band_setup"] == "unavailable"
    assert result["price_vs_ema20"] == "unavailable"
    json.dumps(result, allow_nan=False)


def test_context_is_causal_and_unclosed_or_mismatched_features_are_rejected() -> None:
    engine, candle, feature = scenario()
    original = engine.evaluate(candle, feature)
    future = candle.model_copy(
        update={
            "open_time_ms": candle.open_time_ms + 300_000,
            "close_time_ms": candle.close_time_ms + 300_000,
        }
    )
    assert engine.evaluate(candle, feature, [future]) == original
    assert engine.evaluate(candle.model_copy(update={"is_closed": False}), feature) == ()
    assert (
        engine.evaluate(candle, feature.model_copy(update={"event_time_ms": future.close_time_ms}))
        == ()
    )


def test_formatter_and_repository_preserve_events_and_delivery_count() -> None:
    engine, candle, feature = scenario()
    rules = [item for item in engine.evaluate(candle, feature) if item.triggered]
    state = SignalStateMachine(SignalSettings(), "test-v1")
    plain_repo, enriched_repo = (
        SqlRepository("sqlite:///:memory:"),
        SqlRepository("sqlite:///:memory:"),
    )
    for repo in (plain_repo, enriched_repo):
        repo.initialize()
    try:
        for item in rules:
            plain = item.model_copy(
                update={"metadata": {k: v for k, v in item.metadata.items() if k != "ma_context"}}
            )
            before, after = (
                state.decision_for_observation(plain),
                state.decision_for_observation(item),
            )
            assert before.event_id == after.event_id
            restored = after.model_copy(update={"metadata": before.metadata})
            assert restored.model_dump(mode="json") == before.model_dump(mode="json")
            assert should_deliver_discord(before, AlertSettings(delivery_mode="focused"))
            assert should_deliver_discord(after, AlertSettings(delivery_mode="focused"))
            for repo, decision in ((plain_repo, before), (enriched_repo, after)):
                payload = build_discord_payload(decision, "test", message_style="simple")
                assert repo.save_signal_and_enqueue(
                    decision,
                    payload,
                    candle.close_time_ms,
                    delivery_enabled=True,
                )
                assert not repo.save_signal_and_enqueue(
                    decision,
                    payload,
                    candle.close_time_ms,
                    delivery_enabled=True,
                )
            rendered = build_discord_payload(after, "test", message_style="simple")
            embed = cast(list[dict[str, Any]], rendered["embeds"])[0]
            assert len(embed["fields"]) == 3 and "이평선:" in str(embed)
            if item.family is SignalFamily.BOLLINGER_UPPER_TOUCH:
                assert "추세 확인선 EMA20 105 USDT" in str(embed)
                assert "상단 밴드 아래로 복귀" in str(embed)
                assert "SMA20 아래" in str(build_discord_payload(after, "test"))
                bad_context = {**context(item), "price_vs_ema20": []}
                corrupted = after.model_copy(
                    update={"metadata": {**after.metadata, "ma_context": bad_context}}
                )
                assert build_discord_payload(
                    corrupted, "test", message_style="simple"
                ) == build_discord_payload(before, "test", message_style="simple")
                assert build_discord_payload(corrupted, "test") == build_discord_payload(
                    before, "test"
                )
        assert len(plain_repo.pending_outbox()) == len(enriched_repo.pending_outbox()) == len(rules)
    finally:
        plain_repo.close()
        enriched_repo.close()
