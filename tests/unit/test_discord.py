import asyncio

import httpx
import pytest
from pydantic import SecretStr

from conftest import make_decision, make_feature
from signalbot.alerts.discord import DiscordNotifier
from signalbot.alerts.embeds import build_discord_payload
from signalbot.clock import ReplayClock
from signalbot.config import AlertSettings
from signalbot.domain.enums import Direction, Market, SignalFamily, SignalStage
from signalbot.domain.models import (
    DIRECTIONAL_DIAGNOSTICS_METADATA_KEY,
    DirectionalDiagnostics,
    DirectionalSetupScore,
    GateEvaluation,
    MarketRegime,
)
from signalbot.persistence.repository import SqlRepository


def repository() -> SqlRepository:
    value = SqlRepository("sqlite:///:memory:")
    value.initialize()
    return value


def directional_metadata(
    market: Market = Market.FUTURES, **feature_updates: object
) -> dict[str, object]:
    feature_values: dict[str, object] = {
        "market": market,
        "symbol": "BTCUSDT",
        "interval": "5m",
        "event_time_ms": 600_000,
        "price": 100,
        "ema9": 104,
        "ema20": 102,
        "ema50": 100,
        "ema200": 95,
        "rsi": 62.4,
        "rsi_previous": 58.2,
        "macd_histogram": 0.00123,
        "macd_histogram_previous": 0.0008,
        "adx": 27.1,
        "atr": 2.2,
        "atr_percent": 2.08,
        "bollinger_width": 0.034,
        "bollinger_width_percentile": 34,
        "relative_volume": 1.82,
        "taker_buy_ratio": 0.57,
        "taker_imbalance": 0.14,
        "cvd_pressure": 0.05,
        "spread_bps": 2.3,
    }
    feature_values.update(feature_updates)
    feature = make_feature(**feature_values)
    diagnostics = DirectionalDiagnostics(
        long=DirectionalSetupScore(
            family=SignalFamily.BREAKOUT_LONG,
            raw_score=74,
            decision_score=80,
            triggered=True,
            eligible=True,
        ),
        short=DirectionalSetupScore(
            family=SignalFamily.EXHAUSTION_SHORT,
            raw_score=31,
            decision_score=31,
            triggered=False,
            eligible=False,
        ),
        feature=feature,
    )
    return {DIRECTIONAL_DIAGNOSTICS_METADATA_KEY: diagnostics.model_dump(mode="json")}


def test_embed_contains_evidence_but_no_mass_mentions() -> None:
    payload = build_discord_payload(make_decision(), "Test Bot")
    assert payload["allowed_mentions"] == {"parse": []}
    embed = payload["embeds"][0]  # type: ignore[index]
    assert "🟢 추천: 상승 예상 · LONG 후보" in embed["title"]  # type: ignore[index]
    assert "근거 강도: 85/100" in embed["description"]  # type: ignore[index]
    assert "not a probability" not in embed["description"]  # type: ignore[index]


def test_confirmed_futures_short_is_a_direct_downside_recommendation() -> None:
    payload = build_discord_payload(
        make_decision(
            family=SignalFamily.BREAKDOWN_SHORT,
            direction=Direction.SHORT,
        ),
        "Test Bot",
    )
    embed = payload["embeds"][0]  # type: ignore[index]

    assert "🔴 추천: 하락 예상 · SHORT 후보" in embed["title"]  # type: ignore[index]
    assert "상태: CONFIRMED" in embed["description"]  # type: ignore[index]
    assert embed["color"] == 0xE74C3C


def test_spot_short_is_a_bearish_hold_not_a_short_candidate() -> None:
    payload = build_discord_payload(
        make_decision(
            market=Market.SPOT,
            family=SignalFamily.BREAKDOWN_SHORT,
            direction=Direction.SHORT,
            metadata=directional_metadata(Market.SPOT),
        ),
        "Test Bot",
    )
    embed = payload["embeds"][0]  # type: ignore[index]

    assert "🔴 추천: 하락 예상 · 신규 매수 보류" in embed["title"]  # type: ignore[index]
    assert "SHORT 후보" not in embed["title"]  # type: ignore[index]


def test_risk_and_invalidated_events_remain_no_entry_recommendations() -> None:
    risk_payload = build_discord_payload(
        make_decision(
            family=SignalFamily.PUMP_RISK,
            direction=Direction.RISK_UP,
            stage=SignalStage.WATCH,
        ),
        "Test Bot",
    )
    invalidated_payload = build_discord_payload(
        make_decision(
            family=SignalFamily.PULLBACK_LONG,
            direction=Direction.LONG,
            stage=SignalStage.INVALIDATED,
            metadata={"informational_only": True},
        ),
        "Test Bot",
    )

    risk_embed = risk_payload["embeds"][0]  # type: ignore[index]
    invalidated_embed = invalidated_payload["embeds"][0]  # type: ignore[index]
    assert "⚠️ 추천: 진입 보류 · 단기 급등 위험" in risk_embed["title"]  # type: ignore[index]
    assert risk_embed["color"] == 0xF39C12
    assert "⏸️ 추천: 진입 보류 · 직전 진입 조건 무효화" in invalidated_embed[  # type: ignore[index]
        "title"
    ]


def test_embed_reports_directional_scores_and_closed_candle_indicator_values() -> None:
    payload = build_discord_payload(
        make_decision(metadata=directional_metadata()), "Test Bot"
    )
    embed = payload["embeds"][0]  # type: ignore[index]
    fields = {item["name"]: item["value"] for item in embed["fields"]}  # type: ignore[index]

    scores = fields["상승·하락 근거 강도"]
    assert "LONG 근거 74/100" in scores
    assert "SHORT 근거 31/100" in scores
    assert "게이트 반영 80/100" in scores
    assert "진입 여부는 추천 요약과 게이트 상태를 따릅니다" in scores

    trend = fields["추세·모멘텀 실제값"]
    assert "EMA9/20/50/200 104 / 102 / 100 / 95" in trend
    assert "RSI14 62.4 (Δ +4.2)" in trend
    assert "ADX14 27.1" in trend
    assert "MACD hist +0.00123" in trend

    flow = fields["변동성·수급·체결 실제값"]
    assert "ATR14 2.2 (2.08%)" in flow
    assert "상대거래량 1.82x" in flow
    assert "Taker buy 57.0%" in flow
    assert "Spread 2.3 bps (observed BBO)" in flow
    assert "폐봉 기준" in flow
    assert all(len(value) <= 1024 for value in fields.values())


def test_directional_diagnostics_rejects_a_price_mismatch() -> None:
    with pytest.raises(ValueError, match="price does not match"):
        build_discord_payload(
            make_decision(metadata=directional_metadata(price=101)),
            "Test Bot",
        )


def test_signal_models_reject_family_direction_mismatches() -> None:
    with pytest.raises(ValueError, match="incompatible with direction"):
        make_decision(
            family=SignalFamily.BREAKOUT_LONG,
            direction=Direction.SHORT,
        )
    with pytest.raises(ValueError, match="incompatible with direction"):
        DirectionalDiagnostics(
            long=DirectionalSetupScore(
                family=SignalFamily.BREAKDOWN_SHORT,
                raw_score=70,
                decision_score=70,
                triggered=True,
                eligible=True,
            ),
            short=DirectionalSetupScore(
                family=SignalFamily.BREAKDOWN_SHORT,
                raw_score=70,
                decision_score=70,
                triggered=True,
                eligible=True,
            ),
            feature=make_feature(),
        )


def test_discord_revalidates_model_copy_updates_at_its_trust_boundary() -> None:
    wrong_direction = make_decision().model_copy(
        update={"direction": Direction.SHORT}
    )
    wrong_stage = make_decision().model_copy(update={"stage": "not-a-stage"})

    with pytest.raises(ValueError, match="incompatible with direction"):
        build_discord_payload(wrong_direction, "Test Bot")
    with pytest.raises(ValueError):
        build_discord_payload(wrong_stage, "Test Bot")


def test_discord_payload_enforces_component_and_total_text_limits() -> None:
    gate = GateEvaluation(
        trend_score=0,
        participation_score=0,
        crowding_risk_score=100,
        execution_score=0,
        completeness_score=0,
        passed=False,
        failures=tuple("failure-" + "x" * 1_000 for _ in range(5)),
    )
    metadata = directional_metadata()
    metadata.update(
        {
            "paper_only": True,
            "exit_reason": "x" * 2_000,
            "execution_model": "x" * 2_000,
            "entry_event_id": "x" * 2_000,
        }
    )
    payload = build_discord_payload(
        make_decision(
            family=SignalFamily.TECHNICAL_EXIT,
            reasons=tuple("evidence-" + "x" * 1_000 for _ in range(10)),
            gate=gate,
            regime=MarketRegime(
                label="x" * 2_000,
                btc_trend="x" * 2_000,
                breadth_ratio=0.5,
            ),
            event_id="e" * 3_000,
            rule_version="r" * 3_000,
            metadata=metadata,
        ),
        "u" * 200,
    )
    embed = payload["embeds"][0]  # type: ignore[index]
    fields = embed["fields"]  # type: ignore[index]
    footer = embed["footer"]["text"]  # type: ignore[index]
    total = (
        len(embed["title"])  # type: ignore[index]
        + len(embed["description"])  # type: ignore[index]
        + len(footer)
        + sum(len(field["name"]) + len(field["value"]) for field in fields)
    )

    assert len(payload["username"]) == 80  # type: ignore[arg-type]
    assert len(embed["title"]) <= 256  # type: ignore[index]
    assert len(embed["description"]) <= 4_096  # type: ignore[index]
    assert len(footer) <= 2_048
    assert len(fields) <= 25
    assert all(len(field["name"]) <= 256 for field in fields)
    assert all(len(field["value"]) <= 1_024 for field in fields)
    assert total <= 6_000


def test_spot_embed_does_not_present_bearish_evidence_as_a_short_order() -> None:
    payload = build_discord_payload(
        make_decision(
            market=Market.SPOT,
            metadata=directional_metadata(Market.SPOT),
        ),
        "Test Bot",
    )
    embed = payload["embeds"][0]  # type: ignore[index]
    fields = {item["name"]: item["value"] for item in embed["fields"]}  # type: ignore[index]
    scores = fields["상승·하락 근거 강도"]

    assert "매수·상승 근거" in scores
    assert "하락·신규매수 보류 근거" in scores
    assert "SHORT 근거" not in scores


def test_missing_optional_indicator_values_are_explicit() -> None:
    payload = build_discord_payload(
        make_decision(
            metadata=directional_metadata(
                ema200=None,
                spread_bps=None,
                closed_kline_flow_available=False,
            )
        ),
        "Test Bot",
    )
    embed = payload["embeds"][0]  # type: ignore[index]
    fields = {item["name"]: item["value"] for item in embed["fields"]}  # type: ignore[index]

    assert "EMA9/20/50/200 104 / 102 / 100 / n/a" in fields["추세·모멘텀 실제값"]
    flow = fields["변동성·수급·체결 실제값"]
    assert "Taker buy / imbalance / CVD n/a" in flow
    assert "Spread n/a bps" in flow


def test_embed_reports_causal_chart_structure_and_pullback_raw_values() -> None:
    payload = build_discord_payload(
        make_decision(
            family=SignalFamily.PULLBACK_LONG,
            metadata=directional_metadata(
                ema20_distance_atr=0.31,
                chart_structure={
                    "state": "bullish",
                    "qualified_high_count": 3,
                    "qualified_low_count": 2,
                    "previous_swing_high": 100,
                    "latest_swing_high": 110,
                    "previous_swing_low": 90,
                    "latest_swing_low": 95,
                    "swing_high_change_atr": 5,
                    "swing_low_change_atr": 2.5,
                    "projected_support": 101,
                    "projected_resistance": 111,
                    "price_minus_support_atr": 0.4,
                    "resistance_minus_price_atr": 0.6,
                    "pullback_direction": "long",
                    "pullback_status": "ready",
                    "impulse_size_atr": 4.2,
                    "pullback_depth": 0.38,
                    "pullback_duration_bars": 6,
                    "confluence_distance_atr": 0.18,
                    "recovery_confirmed": True,
                    "structure_intact": True,
                },
            ),
        ),
        "Test Bot",
    )
    embed = payload["embeds"][0]  # type: ignore[index]
    fields = {item["name"]: item["value"] for item in embed["fields"]}  # type: ignore[index]
    structure = fields["차트 구조·눌림목 실제값"]

    assert "구조 bullish · 확정 high/low 3/2" in structure
    assert "Swing H 100 → 110 (Δ +5 ATR)" in structure
    assert "눌림 long/ready · impulse 4.2 ATR · depth 38.00% · duration 6봉" in structure
    assert "confluence 0.18 ATR · 회복종가 yes" in structure
    assert "t-1 고정 · 2오른쪽봉 확정" in structure
    assert len(structure) <= 1024


def test_informational_pullback_embed_cannot_be_mistaken_for_entry_approval() -> None:
    metadata = directional_metadata()
    metadata["informational_only"] = True
    decision = make_decision(
        family=SignalFamily.PULLBACK_LONG,
        stage=SignalStage.SETUP,
        score=100,
        metadata=metadata,
    )
    payload = build_discord_payload(
        decision,
        "Test Bot",
    )
    embed = payload["embeds"][0]  # type: ignore[index]

    title = embed["title"]  # type: ignore[index]
    description = embed["description"]  # type: ignore[index]
    assert "⏸️ 추천: 진입 보류 · 상승 조건 관찰 중" in title
    assert "상태: SETUP · 근거 강도: 100/100" in description
    assert "FUTURES_LONG" not in title
    assert "not a probability" not in description
    assert embed["color"] == 0x3498DB
    assert decision.action_label == "INFORMATION_ONLY"


def test_discord_rejects_an_informational_decision_marked_confirmed() -> None:
    with pytest.raises(ValueError, match="cannot be CONFIRMED"):
        build_discord_payload(
            make_decision(
                family=SignalFamily.PULLBACK_LONG,
                metadata={"informational_only": True},
            ),
            "Test Bot",
        )


def test_paper_exit_embed_separates_fill_and_closed_candle_observation() -> None:
    decision = make_decision(
        family=SignalFamily.TECHNICAL_EXIT,
        event_time_ms=300_000,
        metadata={
            "paper_only": True,
            "exit_reason": "time_exit",
            "execution_model": "paper_next_bar_open",
            "entry_event_id": "entry-1",
            "fill_time_ms": 300_000,
            "observed_at_closed_candle_ms": 599_999,
            "regime_observed_at_ms": 299_999,
            "regime_context_source": "strict_prior_closed_primary",
        },
    )
    payload = build_discord_payload(decision, "Test Bot")
    embed = payload["embeds"][0]  # type: ignore[index]
    description = embed["description"]  # type: ignore[index]
    fields = {item["name"]: item["value"] for item in embed["fields"]}  # type: ignore[index]

    assert "🟠 추천: 기존 LONG 정리 검토 · 신규 진입 보류" in embed["title"]  # type: ignore[index]
    assert "PAPER 포지션 종료 추적" in description
    assert "Rule strength" not in description
    assert "no exchange order was placed" in fields["Execution scope"]
    assert "not restored after restart" in fields["Execution scope"]
    assert "entry-1" in fields["Exit model"]
    assert "Paper fill:" in fields["Paper timing"]
    assert "Alert observation (closed candle):" in fields["Paper timing"]
    assert "strict_prior_closed_primary" in fields["Regime provenance"]
    assert "1970-01-01 00:04:59" in fields["Regime provenance"]


@pytest.mark.asyncio
async def test_disabled_notifier_persists_status_without_network() -> None:
    repo = repository()
    notifier = DiscordNotifier(AlertSettings(), repo, ReplayClock(10))
    try:
        result = await notifier.send(make_decision())
        assert result.status == "disabled"
        assert result.attempts == 0
    finally:
        await notifier.close()
        repo.close()


@pytest.mark.asyncio
async def test_successful_notifier_is_idempotent() -> None:
    requests = 0

    async def handler(request: httpx.Request) -> httpx.Response:
        nonlocal requests
        requests += 1
        assert request.url.host == "discord.test"
        assert request.url.params["wait"] == "true"
        return httpx.Response(200, json={"id": "discord-message-1"})

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    repo = repository()
    settings = AlertSettings(
        discord_enabled=True,
        discord_webhook_url=SecretStr("https://discord.test/webhook"),
        max_attempts=2,
    )
    notifier = DiscordNotifier(settings, repo, ReplayClock(10), client)
    try:
        first = await notifier.send(make_decision())
        second = await notifier.send(make_decision())
        assert first.status == "sent"
        assert second.status == "duplicate"
        assert requests == 1
    finally:
        await client.aclose()
        repo.close()


@pytest.mark.asyncio
async def test_success_without_message_id_is_quarantined() -> None:
    async def handler(_: httpx.Request) -> httpx.Response:
        return httpx.Response(204)

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    repo = repository()
    notifier = DiscordNotifier(
        AlertSettings(
            discord_enabled=True,
            discord_webhook_url=SecretStr("https://discord.test/webhook"),
            max_attempts=3,
        ),
        repo,
        ReplayClock(10),
        client,
    )
    try:
        result = await notifier.send(make_decision())
        replay = await notifier.send(make_decision())
        assert result.status == "uncertain"
        assert replay.status == "uncertain"
        assert result.attempts == 1
        assert repo.get_outbox(make_decision().event_id).status == "uncertain"  # type: ignore[union-attr]
    finally:
        await client.aclose()
        repo.close()


@pytest.mark.asyncio
async def test_transport_error_is_not_blindly_retried() -> None:
    requests = 0

    async def handler(request: httpx.Request) -> httpx.Response:
        nonlocal requests
        requests += 1
        raise httpx.ReadTimeout("ambiguous timeout", request=request)

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    repo = repository()
    notifier = DiscordNotifier(
        AlertSettings(
            discord_enabled=True,
            discord_webhook_url=SecretStr("https://discord.test/webhook"),
            max_attempts=3,
        ),
        repo,
        ReplayClock(10),
        client,
    )
    try:
        result = await notifier.send(make_decision())
        assert result.status == "uncertain"
        assert result.attempts == 1
        assert requests == 1
    finally:
        await client.aclose()
        repo.close()


@pytest.mark.asyncio
async def test_server_error_is_quarantined_without_retry() -> None:
    requests = 0

    async def handler(_: httpx.Request) -> httpx.Response:
        nonlocal requests
        requests += 1
        return httpx.Response(503, text="unknown upstream outcome")

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    repo = repository()
    notifier = DiscordNotifier(
        AlertSettings(
            discord_enabled=True,
            discord_webhook_url=SecretStr("https://discord.test/webhook"),
            max_attempts=3,
        ),
        repo,
        ReplayClock(10),
        client,
    )
    try:
        result = await notifier.send(make_decision())
        assert result.status == "uncertain"
        assert result.response_code == 503
        assert requests == 1
    finally:
        await client.aclose()
        repo.close()


@pytest.mark.asyncio
async def test_dispatch_loop_drains_more_than_one_bounded_batch() -> None:
    requests = 0
    stop_event = asyncio.Event()

    async def handler(_: httpx.Request) -> httpx.Response:
        nonlocal requests
        requests += 1
        if requests == 3:
            stop_event.set()
        return httpx.Response(200, json={"id": f"message-{requests}"})

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    repo = repository()
    settings = AlertSettings(
        discord_enabled=True,
        discord_webhook_url=SecretStr("https://discord.test/webhook"),
    )
    notifier = DiscordNotifier(settings, repo, ReplayClock(10), client)
    try:
        for index in range(3):
            decision = make_decision(event_id=f"event-{index}")
            repo.save_signal_and_enqueue(
                decision,
                build_discord_payload(decision, "Test Bot"),
                10,
                delivery_enabled=True,
                maximum_active_items=100,
            )

        await notifier.run_dispatch_loop(
            stop_event,
            batch_limit=2,
            idle_seconds=0.01,
        )

        assert requests == 3
        assert all(
            repo.get_outbox(f"event-{index}").status == "delivered"  # type: ignore[union-attr]
            for index in range(3)
        )
    finally:
        await client.aclose()
        repo.close()
