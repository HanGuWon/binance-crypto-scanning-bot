from __future__ import annotations

import asyncio
import hashlib
import json
import logging
from decimal import Decimal
from typing import Any

import pytest

from conftest import make_candle, make_decision, make_feature
from signalbot.alerts.embeds import TRACKING_RESET_NOTICE_TEXT, build_discord_payload
from signalbot.clock import ReplayClock
from signalbot.config import Settings
from signalbot.domain.enums import Direction, Market, SignalFamily, SignalStage
from signalbot.persistence.repository import EventIdConflictError, SqlRepository
from signalbot.runtime import MarketRuntime
from signalbot.signals.paper_recovery import (
    build_tracking_reset_notice,
    emit_tracking_reset_notices,
    pending_tracking_resets,
    tracking_reset_event_id,
)
from signalbot.signals.positions import PaperPositionLifecycle

INTERVAL_MS = 300_000
MAX_BARS = 72
WINDOW_MS = MAX_BARS * INTERVAL_MS
NOW_MS = 10 * WINDOW_MS


def _settings(enabled: bool = True, discord: bool = False) -> Settings:
    alerts: dict[str, Any] = {}
    if discord:
        alerts = {
            "discord_enabled": True,
            "discord_webhook_url": "https://discord.test/api/webhooks/1/secret",
        }
    return Settings.model_validate(
        {
            "alerts": alerts,
            "signals": {"technical_exit": {"enabled": enabled, "max_holding_bars": MAX_BARS}},
        }
    )


def _runtime(settings: Settings | None = None) -> MarketRuntime:
    repo = SqlRepository("sqlite:///:memory:")
    repo.initialize()
    return MarketRuntime(
        Market.FUTURES,
        settings or _settings(),
        repo,
        ReplayClock(NOW_MS),
        lambda decision: asyncio.sleep(0),
    )


def _entry(event_id: str = "entry-1", age_ms: int = 60_000, **updates: Any) -> Any:
    return make_decision(event_id=event_id, event_time_ms=NOW_MS - age_ms, **updates)


def _notice_rows(runtime: MarketRuntime) -> list[Any]:
    return [
        d
        for d in runtime.repository.recent_signals(limit=100)
        if d.metadata.get("notice_only") is True
    ]


def test_open_entry_gets_exactly_one_notice_across_two_restarts() -> None:
    runtime = _runtime()
    runtime.repository.save_signal(_entry())
    assert emit_tracking_reset_notices(runtime, NOW_MS) == 1
    assert emit_tracking_reset_notices(runtime, NOW_MS + 120_000) == 0  # second restart
    assert emit_tracking_reset_notices(runtime, NOW_MS + 600_000) == 0  # third restart
    notices = _notice_rows(runtime)
    assert len(notices) == 1
    assert notices[0].metadata["entry_event_id"] == "entry-1"
    assert runtime.repository.get_outbox(notices[0].event_id) is not None
    runtime.repository.close()


def test_notice_has_the_frozen_shape_and_deterministic_event_id() -> None:
    runtime = _runtime()
    entry = _entry(direction=Direction.SHORT, family=SignalFamily.BREAKDOWN_SHORT)
    entry = entry.model_copy(update={"invalidation": Decimal("102")})
    notice = build_tracking_reset_notice(entry, rule_version="rv-9", now_ms=NOW_MS)
    expected_id = hashlib.sha256(
        b"futures|BTCUSDT|technical_exit|entry-1|tracking_reset|rv-9"
    ).hexdigest()[:24]
    assert notice.event_id == expected_id
    assert notice.event_id == tracking_reset_event_id(Market.FUTURES, "BTCUSDT", "entry-1", "rv-9")
    assert (notice.family, notice.stage, notice.direction) == (
        SignalFamily.TECHNICAL_EXIT,
        SignalStage.CONFIRMED,
        Direction.SHORT,
    )
    assert notice.event_time_ms == NOW_MS  # fresh, so M5 delivery expiry does not drop it
    for key, value in {
        "paper_only": True,
        "order_placed": False,
        "exit_reason": "tracking_reset",
        "notice_only": True,
        "entry_event_id": "entry-1",
    }.items():
        assert notice.metadata[key] == value
    assert notice.action_label == "PAPER_TRACKING_NOTICE"
    other_rule = tracking_reset_event_id(Market.FUTURES, "BTCUSDT", "entry-1", "rv-10")
    assert other_rule != expected_id
    runtime.repository.close()


def test_closed_entry_gets_no_notice() -> None:
    runtime = _runtime()
    runtime.repository.save_signal(_entry())
    runtime.repository.save_signal(
        make_decision(
            event_id="exit-1",
            family=SignalFamily.TECHNICAL_EXIT,
            direction=Direction.LONG,
            event_time_ms=NOW_MS - 30_000,
            metadata={"paper_only": True, "entry_event_id": "entry-1"},
        )
    )
    assert emit_tracking_reset_notices(runtime, NOW_MS) == 0
    assert _notice_rows(runtime) == []
    runtime.repository.close()


@pytest.mark.parametrize(("offset_ms", "included"), [(-1, False), (0, True), (1, True)])
def test_window_boundary(offset_ms: int, included: bool) -> None:
    runtime = _runtime()
    runtime.repository.save_signal(_entry(age_ms=WINDOW_MS - offset_ms))
    expected = 1 if included else 0
    assert emit_tracking_reset_notices(runtime, NOW_MS) == expected
    runtime.repository.close()


def test_entries_outside_window_get_no_notice() -> None:
    runtime = _runtime()
    runtime.repository.save_signal(_entry("old", age_ms=WINDOW_MS * 3))
    assert emit_tracking_reset_notices(runtime, NOW_MS) == 0
    runtime.repository.close()


def test_non_entries_get_no_notice() -> None:
    runtime = _runtime()
    repo = runtime.repository
    repo.save_signal(
        _entry(
            "risk",
            family=SignalFamily.PUMP_RISK,
            direction=Direction.RISK_UP,
            stage=SignalStage.CONFIRMED,
        )
    )
    repo.save_signal(_entry("setup", stage=SignalStage.SETUP))
    repo.save_signal(_entry("info", stage=SignalStage.SETUP, metadata={"informational_only": True}))
    repo.save_signal(_entry("other-tf", timeframe="15m"))
    repo.save_signal(_entry("bad-stop", invalidation=Decimal("101")))  # LONG stop above price
    repo.save_signal(_entry("no-stop", invalidation=None))
    repo.save_signal(
        make_decision(
            event_id="spot-short",
            market=Market.SPOT,
            family=SignalFamily.BREAKDOWN_SHORT,
            direction=Direction.SHORT,
            event_time_ms=NOW_MS - 60_000,
            invalidation=Decimal("102"),
        )
    )
    repo.save_signal(
        _entry(
            "prior-notice-like-exit",
            family=SignalFamily.TECHNICAL_EXIT,
            metadata={"paper_only": True},
        )
    )
    assert emit_tracking_reset_notices(runtime, NOW_MS) == 0
    repo.close()


def test_disabled_technical_exit_emits_nothing() -> None:
    runtime = _runtime(_settings(enabled=False))
    runtime.repository.save_signal(_entry())
    assert emit_tracking_reset_notices(runtime, NOW_MS) == 0
    runtime.repository.close()


def test_query_is_bounded() -> None:
    runtime = _runtime()
    for index in range(5):
        runtime.repository.save_signal(_entry(f"entry-{index}", age_ms=60_000 + index))
    pending = pending_tracking_resets(
        runtime.repository,
        market=Market.FUTURES,
        primary_interval="5m",
        max_holding_bars=MAX_BARS,
        now_ms=NOW_MS,
        limit=2,
    )
    assert len(pending) == 2
    runtime.repository.close()


def test_notice_is_queued_for_delivery_when_discord_is_enabled() -> None:
    runtime = _runtime(_settings(discord=True))
    runtime.repository.save_signal(_entry())
    assert emit_tracking_reset_notices(runtime, NOW_MS) == 1
    notice = _notice_rows(runtime)[0]
    item = runtime.repository.get_outbox(notice.event_id)
    assert item is not None and item.status == "pending"
    runtime.repository.close()


def test_existing_notice_with_different_bytes_is_not_reemitted_and_logs(
    caplog: pytest.LogCaptureFixture, monkeypatch: pytest.MonkeyPatch
) -> None:
    runtime = _runtime()
    runtime.repository.save_signal(_entry())

    def conflicting(_decision: Any) -> bool:
        raise EventIdConflictError("already stored with other bytes")

    monkeypatch.setattr(runtime, "persist_notice", conflicting)
    with caplog.at_level(logging.WARNING):
        assert emit_tracking_reset_notices(runtime, NOW_MS) == 0
    assert any("already exists" in r.getMessage() for r in caplog.records)
    runtime.repository.close()


def test_notice_is_not_a_paper_position() -> None:
    lifecycle = PaperPositionLifecycle(
        _settings().signals.technical_exit,
        rule_version="test-v1",
        market=Market.FUTURES,
        primary_interval="5m",
        maximum_symbols=10,
    )
    candle = make_candle(5, market=Market.FUTURES, symbol="BTCUSDT")
    candle = candle.model_copy(update={"interval": "5m"})
    feature = make_feature(
        market=Market.FUTURES, symbol="BTCUSDT", interval="5m", event_time_ms=candle.close_time_ms
    )
    notice = build_tracking_reset_notice(_entry(), rule_version="test-v1", now_ms=NOW_MS)
    notice = notice.model_copy(update={"event_time_ms": candle.close_time_ms})
    exits = lifecycle.on_closed_candle(candle, feature, [notice])
    assert exits == []
    assert lifecycle.active_position_count == 0
    assert lifecycle.pending_entry_count == 0
    assert lifecycle.continuation_symbols == frozenset()


def test_embed_renders_the_notice_without_exit_recommendation_wording() -> None:
    notice = build_tracking_reset_notice(_entry(), rule_version="test-v1", now_ms=NOW_MS)
    embed = build_discord_payload(notice, "Bot")["embeds"][0]  # type: ignore[index]
    assert embed["title"].startswith("[USDⓈ-M] BTCUSDT · ")  # type: ignore[index]
    assert TRACKING_RESET_NOTICE_TEXT in embed["title"]  # type: ignore[index]
    assert TRACKING_RESET_NOTICE_TEXT == (
        "PAPER 추적 중단 — 이 진입의 청산 알림은 더 이상 오지 않습니다"
    )
    text = json.dumps(embed, ensure_ascii=False)
    forbidden_terms = (
        "정리 검토", "추천", "예상", "후보", "Exit model", "Paper timing", "청산 가격",
    )  # fmt: skip
    for forbidden in forbidden_terms:
        assert forbidden not in text, forbidden
    assert "no exchange order was placed" in text
    assert "청산 권고 아님" in embed["description"]  # type: ignore[index]
    names = [f["name"] for f in embed["fields"]]  # type: ignore[index]
    assert "Tracked entry" in names and "검증 상태" in names


def test_regular_paper_exit_embed_is_unchanged_by_the_notice_path() -> None:
    exit_decision = make_decision(
        event_id="exit-9",
        family=SignalFamily.TECHNICAL_EXIT,
        direction=Direction.LONG,
        metadata={
            "paper_only": True,
            "exit_reason": "max_holding",
            "execution_model": "next_open",
            "entry_event_id": "entry-1",
        },
    )
    embed = build_discord_payload(exit_decision, "Bot")["embeds"][0]  # type: ignore[index]
    assert "정리 검토" in embed["title"]  # type: ignore[index]
    names = [f["name"] for f in embed["fields"]]  # type: ignore[index]
    assert "Exit model" in names and "Paper timing" in names
    assert "Tracked entry" not in names


@pytest.mark.asyncio
async def test_application_startup_emits_one_notice_per_orphaned_entry_across_restarts(
    tmp_path: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    import signalbot.app as app_module
    from signalbot.app import SignalApplication

    class StubNotifier:
        def __init__(self, *_a: Any, **_k: Any) -> None:
            pass

        def recover_inflight(self) -> int:
            return 0

        async def close(self) -> None:
            return None

        async def run_dispatch_loop(self, stop_event: asyncio.Event, **_k: Any) -> None:
            await stop_event.wait()

    class StubScanner:
        def __init__(self, market: Any, stop_event: asyncio.Event) -> None:
            self.market = market
            self._stop = stop_event

        async def run(self) -> None:
            await self._stop.wait()

        async def close(self) -> None:
            return None

    monkeypatch.setattr(app_module, "DiscordNotifier", StubNotifier)
    monkeypatch.setattr(
        app_module,
        "MarketScanner",
        lambda market, settings, clock, runtime, stop_event, **kw: StubScanner(market, stop_event),
    )
    db_url = f"sqlite:///{tmp_path / 'restart.db'}"
    settings = Settings.model_validate(
        {
            "binance": {"markets": ["futures"]},
            "storage": {"url": db_url},
            "signals": {"technical_exit": {"enabled": True, "max_holding_bars": MAX_BARS}},
        }
    )
    seed = SqlRepository(db_url)
    seed.initialize()
    now_ms = SignalApplication(settings).clock.now_ms()
    seed.save_signal(make_decision(event_id="entry-live", event_time_ms=now_ms - 60_000))
    seed.close()

    for _restart in range(2):
        app = SignalApplication(settings, stop_after_minutes=0)
        await asyncio.wait_for(app.run(), timeout=30)

    check = SqlRepository(db_url)
    check.initialize()
    notices = [
        d for d in check.recent_signals(limit=50) if d.metadata.get("notice_only") is True
    ]
    check.close()
    assert len(notices) == 1
    assert notices[0].metadata["entry_event_id"] == "entry-live"
