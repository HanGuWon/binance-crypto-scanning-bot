from dataclasses import asdict
from decimal import Decimal
from typing import Any, cast

import pytest
from pydantic import ValidationError

from conftest import make_candle, make_decision
from signalbot.alerts.embeds import build_discord_payload
from signalbot.clock import ReplayClock
from signalbot.config import AlertSettings, Settings
from signalbot.data.microstructure import minute_pressure_snapshot
from signalbot.domain.enums import Direction, Market, SignalFamily, SignalStage
from signalbot.domain.models import Instrument, SignalDecision
from signalbot.persistence.repository import SqlRepository
from signalbot.runtime import MarketRuntime


def watch(**updates: Any) -> SignalDecision:
    values = dict(
        family=SignalFamily.BOLLINGER_UPPER_TOUCH,
        direction=Direction.RISK_UP,
        stage=SignalStage.SETUP,
        invalidation=None,
        metadata={
            "market_watch": True,
            "informational_only": True,
            "gainer_rank": 2,
            "trend_label": "상승 추세",
            "price_change_percent_24h": 14.25,
            "quote_volume_24h_usdt": 345_000_000.0,
            "bollinger_upper": 101.0,
            "flow_interval": "5m",
            "taker_buy_ratio": 0.6,
            "invalidation_condition": "다음 폐봉 고가가 상단 밴드 아래로 복귀",
        },
    )
    values.update(updates)
    return make_decision(**values)


def embed(decision: SignalDecision, style: str = "simple") -> dict[str, Any]:
    payload = build_discord_payload(decision, "test", message_style=cast(Any, style))
    assert payload["allowed_mentions"] == {"parse": []}
    return cast(list[dict[str, Any]], payload["embeds"])[0]


def test_simple_preserves_essential_information_without_indicator_dump() -> None:
    decision = watch()
    original = decision.model_dump(mode="json")
    output = embed(decision)
    text = str(output)
    assert "볼린저 상단 접촉" in output["title"]
    assert "상승 추세" in text and "14.25%" in text and "3.45억 USDT" in text
    assert "기준 가격 100 USDT" in text and "5분봉 기준 매수 체결 우세" in text
    assert "조건 해제" in text and "상단 밴드 아래로 복귀" in text
    assert all(s in text for s in ("UTC", "KST", decision.event_id, decision.rule_version))
    assert len(output["fields"]) == 3
    assert all(s not in text for s in ("EMA20/50", "MACD", "Market regime", "근거 강도"))
    assert decision.model_dump(mode="json") == original
    assert "폐봉 기술 추세" in str(embed(decision, "detailed"))


@pytest.mark.parametrize(
    ("family", "direction", "label"),
    [
        (SignalFamily.TOP_GAINER, Direction.RISK_UP, "급상승 종목 관찰"),
        (SignalFamily.BOLLINGER_LOWER_TOUCH, Direction.RISK_DOWN, "볼린저 하단 접촉"),
        (SignalFamily.WATCH_TREND_UP, Direction.RISK_UP, "상승 추세 관찰"),
        (SignalFamily.WATCH_TREND_DOWN, Direction.RISK_DOWN, "하락 추세 관찰"),
        (SignalFamily.RANGE_LONG, Direction.LONG, "박스권 매수 후보"),
        (SignalFamily.RANGE_SHORT, Direction.SHORT, "박스권 숏 후보"),
    ],
)
def test_simple_family_semantics(family: SignalFamily, direction: Direction, label: str) -> None:
    decision = watch(family=family, direction=direction)
    output = embed(decision)
    assert label in output["title"]
    assert (
        "검증 전 후보" in output["description"]
        if family
        in {
            SignalFamily.RANGE_LONG,
            SignalFamily.RANGE_SHORT,
        }
        else "관찰 알림" in output["description"]
    )
    assert len(output["fields"]) <= 4


def test_range_and_retention_show_target_invalidation_and_expiry() -> None:
    decision = watch(
        family=SignalFamily.RANGE_LONG, direction=Direction.LONG, invalidation=Decimal("89.5")
    )
    decision = decision.model_copy(
        update={
            "metadata": {
                **decision.metadata,
                "range_target": 100.0,
                "retained_gainer": True,
                "watch_expires_at_ms": 172_800_000,
            }
        }
    )
    output = embed(decision)
    text = str(output)
    assert "목표 100 USDT" in text and "후보 무효화 가격 89.5 USDT" in text
    assert "유지 추적" in text and "추적 기한" in text
    assert len(output["fields"]) == 4


def test_missing_minute_data_uses_explicitly_labelled_five_minute_fallback() -> None:
    decision = watch()
    decision = decision.model_copy(
        update={
            "metadata": {
                **decision.metadata,
                "minute_pressure": {"available": False, "reason": "stale_bars"},
            }
        }
    )
    assert "1분 자료 부족 · 5분봉 기준" in str(embed(decision))
    decision = decision.model_copy(
        update={"metadata": {**decision.metadata, "taker_buy_ratio": None}}
    )
    assert "우세를 판단할 수 없습니다" in str(embed(decision))
    assert "체결 균형" not in str(embed(decision))


def test_pressure_is_causal_and_displayed_as_three_minute_context() -> None:
    candles = [
        make_candle(i, market=Market.FUTURES, interval="1m", step_ms=60_000) for i in range(7, 10)
    ]
    pressure = minute_pressure_snapshot(candles, Market.FUTURES, "BTCUSDT", 599_999)
    assert pressure.available
    decision = watch(event_time_ms=599_999)
    decision = decision.model_copy(
        update={
            "metadata": {
                **decision.metadata,
                "minute_pressure": asdict(pressure),
            }
        }
    )
    text = str(embed(decision))
    assert "최근 3분 매수" in text and "마지막 1분" in text and "KST 09:07-09:09" in text
    wrong = {**asdict(pressure), "end_ms": 659_999}
    decision = decision.model_copy(
        update={"metadata": {**decision.metadata, "minute_pressure": wrong}}
    )
    assert "1분 자료 부족" in str(embed(decision))


def test_paper_and_intrabar_scope_remain_explicit() -> None:
    paper = make_decision(
        family=SignalFamily.TECHNICAL_EXIT,
        metadata={"paper_only": True, "informational_only": True},
        stage=SignalStage.SETUP,
    )
    assert "모의" in embed(paper)["description"] and "모의" in embed(paper)["title"]
    risk = make_decision(family=SignalFamily.PUMP_RISK, direction=Direction.RISK_UP)
    assert "실시간 위험 관찰" in embed(risk)["footer"]["text"]
    assert "폐봉" not in embed(risk)["footer"]["text"]
    with pytest.raises(ValueError):
        embed(watch().model_copy(update={"stage": SignalStage.CONFIRMED}))


def test_simple_keeps_secondary_band_contact_and_actual_paper_exit_reason() -> None:
    decision = watch(family=SignalFamily.RANGE_LONG, direction=Direction.LONG)
    decision = decision.model_copy(
        update={
            "metadata": {
                **decision.metadata,
                "concurrent_watch_events": ["range_long", "bollinger_lower_touch"],
            }
        }
    )
    assert "볼린저 하단 접촉" in embed(decision)["description"]
    paper = make_decision(
        family=SignalFamily.TECHNICAL_EXIT,
        stage=SignalStage.SETUP,
        metadata={"paper_only": True, "exit_reason": "trailing_stop"},
    )
    text = str(embed(paper))
    assert "추적 손절 조건" in text and "조건 무효화" not in text


def test_style_contract_and_discord_limits() -> None:
    assert AlertSettings().message_style == "simple"
    with pytest.raises(ValidationError):
        AlertSettings.model_validate({"message_style": "invalid"})
    with pytest.raises(ValueError, match="unknown"):
        embed(watch(), "invalid")
    decision = watch()
    decision = decision.model_copy(
        update={
            "metadata": {
                **decision.metadata,
                "trend_label": "긴 문장" * 4000,
                "invalidation_condition": "관찰 조건" * 4000,
            }
        }
    )
    output = embed(decision)
    size = len(output["title"]) + len(output["description"]) + len(output["footer"]["text"])
    size += sum(len(f["name"]) + len(f["value"]) for f in output["fields"])
    assert size <= 6000 and all(len(f["value"]) <= 1024 for f in output["fields"])


def test_confirmed_signal_keeps_two_specific_reasons_in_plain_language() -> None:
    decision = make_decision(
        stage=SignalStage.CONFIRMED,
        reasons=("closed above 20-bar high", "relative volume 2.35x", "EMA20 above EMA50"),
    )
    original = decision.model_dump(mode="json")
    reasons = embed(decision)["fields"][1]["value"]
    assert reasons == "최근 20개 봉의 고점 위에서 마감\n거래량이 비교 기준의 2.35배"
    assert decision.model_dump(mode="json") == original


def test_unknown_reasons_remain_specific_and_bounded() -> None:
    decision = make_decision(reasons=("specific measured evidence " + "x" * 200,))
    reasons = embed(decision)["fields"][1]["value"]
    assert reasons.startswith("specific measured evidence ")
    assert len(reasons) == 160 and reasons.endswith("…")
    empty = make_decision(reasons=())
    assert "조건을 관찰했습니다" in embed(empty)["fields"][1]["value"]


@pytest.mark.asyncio
async def test_runtime_enrichment_preserves_decisions_and_delivery_count() -> None:
    async def run(enabled: bool) -> tuple[list[SignalDecision], int]:
        settings = Settings.model_validate(
            {
                "binance": {"markets": ["futures"], "intervals": ["1m", "5m"]},
                "alerts": {
                    "discord_enabled": True,
                    "discord_webhook_url": "https://discord.test/hook",
                    "delivery_mode": "focused",
                    "market_watch": {
                        "enabled": True,
                        "context_alerts_enabled": True,
                        "minute_pressure_enabled": enabled,
                    },
                },
            }
        )
        repo = SqlRepository("sqlite:///:memory:")
        repo.initialize()
        decisions: list[SignalDecision] = []

        async def collect(d: SignalDecision) -> None:
            decisions.append(d)

        runtime = MarketRuntime(Market.FUTURES, settings, repo, ReplayClock(1), collect)
        runtime.set_active_symbols(frozenset({"TESTUSDT"}), frozenset({"TESTUSDT"}))
        runtime.set_priority_gainers(
            (
                Instrument(
                    market=Market.FUTURES,
                    symbol="TESTUSDT",
                    base_asset="TEST",
                    quote_asset="USDT",
                    status="TRADING",
                    quote_volume=Decimal(200_000_000),
                ),
            )
        )
        history = [
            make_candle(i, market=Market.FUTURES, symbol="TESTUSDT", close=100 + i * 0.1)
            for i in range(260)
        ]
        runtime.bootstrap(history)
        minute_rows = [
            make_candle(
                i,
                market=Market.FUTURES,
                symbol="TESTUSDT",
                interval="1m",
                step_ms=60_000,
                close=126.0,
            )
            for i in range(1301, 1304)
        ]
        runtime.bootstrap(minute_rows)
        await runtime.handle_event(
            make_candle(260, market=Market.FUTURES, symbol="TESTUSDT", close=126.0)
        )
        count = len(repo.pending_outbox())
        repo.close()
        return decisions, count

    plain, plain_count = await run(False)
    enriched, enriched_count = await run(True)
    assert plain_count == enriched_count == 1
    assert [d.event_id for d in plain] == [d.event_id for d in enriched]
    for before, after in zip(plain, enriched, strict=True):
        if after.metadata.get("market_watch"):
            pressure = cast(dict[str, Any], after.metadata["minute_pressure"])
            assert pressure["available"] and pressure["lag_ms"] == 60_000
            assert before.family == after.family and before.stage == after.stage
            after = after.model_copy(
                update={
                    "metadata": {k: v for k, v in after.metadata.items() if k != "minute_pressure"}
                }
            )
        assert before.model_dump(mode="json") == after.model_dump(mode="json")
