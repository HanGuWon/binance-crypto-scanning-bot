from __future__ import annotations

import statistics
from decimal import Decimal

from conftest import make_candle, make_decision, make_feature
from signalbot.alerts.embeds import build_discord_payload
from signalbot.alerts.policy import should_deliver_discord
from signalbot.clock import ReplayClock
from signalbot.config import AlertSettings, MarketWatchSettings, Settings, SignalSettings
from signalbot.data.microstructure import OrderFlowSnapshot
from signalbot.domain.enums import Direction, Market, SignalFamily, SignalStage
from signalbot.domain.models import Instrument, RuleEvaluation
from signalbot.indicators.core import FeatureEngine
from signalbot.persistence.repository import SqlRepository
from signalbot.runtime import MarketRuntime
from signalbot.signals.market_watch import MarketWatchEngine, classify_trend
from signalbot.signals.state_machine import SignalStateMachine


def _instrument(
    symbol: str = "TESTUSDT",
    *,
    quote_volume: str = "150000000",
    change: float = 12.5,
) -> Instrument:
    return Instrument(
        market=Market.FUTURES,
        symbol=symbol,
        base_asset=symbol.removesuffix("USDT"),
        quote_asset="USDT",
        status="TRADING",
        quote_volume=Decimal(quote_volume),
        price_change_percent_24h=change,
        onboard_time_ms=1_600_000_000_000,
        contract_type="PERPETUAL",
    )


def test_feature_engine_exposes_standard_closed_candle_bollinger_levels() -> None:
    candles = [make_candle(i, close=100 + (i % 7) * 0.4) for i in range(220)]
    feature = FeatureEngine(SignalSettings()).compute(
        candles,
        order_flow=OrderFlowSnapshot(),
        spread_bps=2.0,
        regime=make_feature().regime,
    )

    assert feature is not None
    closes = [float(candle.close) for candle in candles[-20:]]
    middle = statistics.fmean(closes)
    deviation = statistics.pstdev(closes)
    assert feature.bollinger_middle == middle
    assert feature.bollinger_upper == middle + 2 * deviation
    assert feature.bollinger_lower == middle - 2 * deviation


def test_market_watch_emits_top_gainer_and_upper_touch_with_bullish_trend() -> None:
    settings = MarketWatchSettings(enabled=True, market=Market.FUTURES)
    engine = MarketWatchEngine(settings)
    engine.set_priority_gainers((_instrument(),))
    candle = make_candle(220, market=Market.FUTURES, symbol="TESTUSDT", close=110)
    feature = make_feature(
        market=Market.FUTURES,
        symbol="TESTUSDT",
        event_time_ms=candle.close_time_ms,
        price=110,
        ema20=105,
        ema50=100,
        ema20_slope_atr=0.5,
        macd_histogram=1.2,
        rsi=66,
        adx=31,
        bollinger_middle=100,
        bollinger_upper=108,
        bollinger_lower=92,
    )

    evaluations = engine.evaluate(candle, feature)

    assert [item.family for item in evaluations] == [
        SignalFamily.TOP_GAINER,
        SignalFamily.BOLLINGER_UPPER_TOUCH,
        SignalFamily.BOLLINGER_LOWER_TOUCH,
    ]
    assert evaluations[0].metadata["gainer_rank"] == 1
    assert evaluations[0].metadata["trend_label"] == "상승 추세"
    assert "상승 순위 5위 밖" in evaluations[0].metadata["invalidation_condition"]
    assert evaluations[1].triggered is True
    assert "상단 밴드 아래로 복귀" in evaluations[1].metadata["invalidation_condition"]
    assert evaluations[2].triggered is False


def test_market_watch_only_evaluates_current_priority_symbols() -> None:
    engine = MarketWatchEngine(MarketWatchSettings(enabled=True))
    engine.set_priority_gainers((_instrument("AAAUSDT"),))
    candle = make_candle(220, market=Market.FUTURES, symbol="BBBUSDT", close=100)
    feature = make_feature(
        market=Market.FUTURES,
        symbol="BBBUSDT",
        event_time_ms=candle.close_time_ms,
        bollinger_middle=100,
        bollinger_upper=105,
        bollinger_lower=95,
    )

    assert engine.evaluate(candle, feature) == ()


def test_market_watch_detects_lower_band_touch_with_bearish_trend() -> None:
    engine = MarketWatchEngine(MarketWatchSettings(enabled=True))
    engine.set_priority_gainers((_instrument(),))
    candle = make_candle(220, market=Market.FUTURES, symbol="TESTUSDT", close=90)
    feature = make_feature(
        market=Market.FUTURES,
        symbol="TESTUSDT",
        event_time_ms=candle.close_time_ms,
        price=90,
        ema20=95,
        ema50=100,
        ema20_slope_atr=-0.5,
        macd_histogram=-1.2,
        rsi=34,
        adx=29,
        bollinger_middle=100,
        bollinger_upper=108,
        bollinger_lower=92,
    )

    evaluations = engine.evaluate(candle, feature)

    upper = next(item for item in evaluations if item.family is SignalFamily.BOLLINGER_UPPER_TOUCH)
    lower = next(item for item in evaluations if item.family is SignalFamily.BOLLINGER_LOWER_TOUCH)
    assert upper.triggered is False
    assert lower.triggered is True
    assert lower.metadata["trend_label"] == "하락 추세"


def test_bollinger_touch_includes_exact_band_boundary() -> None:
    engine = MarketWatchEngine(MarketWatchSettings(enabled=True))
    engine.set_priority_gainers((_instrument(),))
    candle = make_candle(220, market=Market.FUTURES, symbol="TESTUSDT", close=100)
    feature = make_feature(
        market=Market.FUTURES,
        symbol="TESTUSDT",
        event_time_ms=candle.close_time_ms,
        bollinger_middle=100,
        bollinger_upper=100.5,
        bollinger_lower=99.5,
    )

    evaluations = engine.evaluate(candle, feature)

    assert next(
        item for item in evaluations if item.family is SignalFamily.BOLLINGER_UPPER_TOUCH
    ).triggered
    assert next(
        item for item in evaluations if item.family is SignalFamily.BOLLINGER_LOWER_TOUCH
    ).triggered


def test_trend_classifier_requires_broad_alignment() -> None:
    bullish = make_feature(
        price=110,
        ema20=105,
        ema50=100,
        ema20_slope_atr=0.4,
        macd_histogram=0.5,
        rsi=61,
        adx=30,
    )
    bearish = bullish.model_copy(
        update={
            "price": 90,
            "ema20": 95,
            "ema50": 100,
            "ema20_slope_atr": -0.4,
            "macd_histogram": -0.5,
            "rsi": 39,
        }
    )
    mixed = bullish.model_copy(update={"ema20_slope_atr": -0.2, "macd_histogram": -0.2, "rsi": 50})

    assert classify_trend(bullish)[0] == "상승 추세"
    assert classify_trend(bearish)[0] == "하락 추세"
    assert classify_trend(mixed)[0] == "혼조/횡보"


def test_event_only_state_rearms_silently_and_respects_cooldown() -> None:
    machine = SignalStateMachine(SignalSettings(), "market-watch-v1")

    def evaluation(timestamp: int, *, touched: bool) -> RuleEvaluation:
        return RuleEvaluation(
            market=Market.FUTURES,
            symbol="TESTUSDT",
            family=SignalFamily.BOLLINGER_UPPER_TOUCH,
            direction=Direction.RISK_UP,
            timeframe="5m",
            event_time_ms=timestamp,
            score=100 if touched else 0,
            triggered=touched,
            eligible=False,
            price=Decimal("100"),
            metadata={
                "informational_only": True,
                "event_only": True,
                "event_cooldown_seconds": 1800,
            },
        )

    first = machine.process(evaluation(1_000, touched=True))
    assert first is not None and first.stage is SignalStage.SETUP
    assert machine.process(evaluation(301_000, touched=True)) is None
    assert machine.process(evaluation(601_000, touched=False)) is None
    assert machine.process(evaluation(901_000, touched=True)) is None
    assert machine.process(evaluation(1_201_000, touched=False)) is None
    second = machine.process(evaluation(1_901_001, touched=True))
    assert second is not None and second.stage is SignalStage.SETUP


def test_focused_discord_policy_keeps_only_high_value_legacy_events() -> None:
    settings = AlertSettings(delivery_mode="focused")
    assert not should_deliver_discord(
        make_decision(
            family=SignalFamily.PULLBACK_LONG,
            stage=SignalStage.WATCH,
        ),
        settings,
    )
    assert not should_deliver_discord(
        make_decision(
            family=SignalFamily.PUMP_RISK,
            direction=Direction.RISK_UP,
            stage=SignalStage.CONFIRMED,
        ),
        settings,
    )
    assert should_deliver_discord(make_decision(), settings)
    assert should_deliver_discord(
        make_decision(
            family=SignalFamily.TOP_GAINER,
            direction=Direction.RISK_UP,
            stage=SignalStage.SETUP,
            metadata={"informational_only": True},
        ),
        settings,
    )


def test_market_watch_embed_shows_rank_volume_bands_and_trend() -> None:
    decision = make_decision(
        family=SignalFamily.BOLLINGER_UPPER_TOUCH,
        direction=Direction.RISK_UP,
        stage=SignalStage.SETUP,
        invalidation=None,
        metadata={
            "informational_only": True,
            "market_watch": True,
            "gainer_rank": 2,
            "price_change_percent_24h": 14.25,
            "quote_volume_24h_usdt": 345_000_000.0,
            "trend_label": "상승 추세",
            "trend_evidence": ("EMA20/50 101/99", "RSI14 64.0"),
            "bollinger_lower": 90.0,
            "bollinger_middle": 100.0,
            "bollinger_upper": 110.0,
            "candle_low": 104.0,
            "candle_high": 111.0,
            "candle_close": 109.0,
            "invalidation_condition": "다음 폐봉 고가가 상단 밴드 아래로 복귀",
        },
    )
    payload = build_discord_payload(decision, "Test Bot")
    embed = payload["embeds"][0]  # type: ignore[index]
    fields = {item["name"]: item["value"] for item in embed["fields"]}  # type: ignore[index]

    assert "Bollinger 상단 터치 · 상승 추세" in embed["title"]  # type: ignore[index]
    assert "24h 상승 순위 #2" in fields["급상승 우선순위"]
    assert "345M USDT" in fields["급상승 우선순위"]
    assert "하단 90 · 중심 100 · 상단 110" in fields["Bollinger 위치"]
    assert "판정: **상승 추세**" in fields["폐봉 기술 추세"]
    assert "상단 밴드 아래로 복귀" in fields["Invalidation"]


def test_focused_runtime_persists_noisy_events_without_enabling_delivery() -> None:
    settings = Settings.model_validate(
        {
            "binance": {
                "markets": ["futures"],
                "intervals": ["5m"],
                "primary_interval": "5m",
            },
            "alerts": {
                "discord_enabled": True,
                "discord_webhook_url": "https://discord.test/webhook",
                "delivery_mode": "focused",
            },
            "storage": {"url": "sqlite:///:memory:"},
        }
    )
    repository = SqlRepository(settings.storage.url)
    repository.initialize()

    async def collected(_decision: object) -> None:
        return None

    runtime = MarketRuntime(
        Market.FUTURES,
        settings,
        repository,
        ReplayClock(1_000),
        collected,
    )
    noisy = make_decision(
        event_id="focused-noisy",
        family=SignalFamily.PUMP_RISK,
        direction=Direction.RISK_UP,
    )
    priority = make_decision(
        event_id="focused-priority",
        family=SignalFamily.TOP_GAINER,
        direction=Direction.RISK_UP,
        stage=SignalStage.SETUP,
        metadata={"informational_only": True},
    )

    assert runtime._persist_decision(noisy) is True
    assert runtime._persist_decision(priority) is True
    assert repository.get_outbox("focused-noisy").status == "disabled"  # type: ignore[union-attr]
    assert repository.get_outbox("focused-priority").status == "pending"  # type: ignore[union-attr]
    repository.close()


def test_delivered_market_watch_cooldown_survives_runtime_restart() -> None:
    settings = Settings.model_validate(
        {
            "binance": {
                "markets": ["futures"],
                "intervals": ["5m"],
                "primary_interval": "5m",
            },
            "alerts": {
                "discord_enabled": True,
                "discord_webhook_url": "https://discord.test/webhook",
                "delivery_mode": "focused",
                "market_watch": {"enabled": True, "event_cooldown_seconds": 1800},
            },
            "storage": {"url": "sqlite:///:memory:"},
        }
    )
    repository = SqlRepository(settings.storage.url)
    repository.initialize()

    async def collected(_decision: object) -> None:
        return None

    def runtime() -> MarketRuntime:
        return MarketRuntime(
            Market.FUTURES,
            settings,
            repository,
            ReplayClock(1_000),
            collected,
        )

    def event(event_id: str, timestamp: int):
        return make_decision(
            event_id=event_id,
            event_time_ms=timestamp,
            family=SignalFamily.TOP_GAINER,
            direction=Direction.RISK_UP,
            stage=SignalStage.SETUP,
            metadata={"informational_only": True, "market_watch": True},
        )

    assert runtime()._persist_decision(event("first-gainer", 600_000))
    assert repository.mark_outbox("first-gainer", "delivered", 600_001, expected_status="pending")
    restarted = runtime()
    assert restarted._persist_decision(event("cooldown-gainer", 900_000))
    assert repository.get_outbox("cooldown-gainer").status == "disabled"  # type: ignore[union-attr]
    assert restarted._persist_decision(event("boundary-gainer", 2_400_000))
    assert repository.get_outbox("boundary-gainer").status == "pending"  # type: ignore[union-attr]
    repository.close()
