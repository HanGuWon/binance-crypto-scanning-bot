"""Stage 1 alert changes: wording, position line, repeat suppression (all off by default)."""

from __future__ import annotations

import json
from decimal import Decimal
from pathlib import Path
from typing import Any

import pytest
from stage1_golden_gen import CASES
from stage1_scenario import STEP_MS, T0_MS, run_scenario

from conftest import make_candle, make_decision, make_feature
from signalbot.alerts.embeds import build_discord_payload
from signalbot.domain.enums import Direction, Market, SignalFamily, SignalStage
from signalbot.signals.episodes import (
    TrendZoneTracker,
    build_position_line,
)

GOLDEN = Path(__file__).resolve().parents[1] / "fixtures" / "stage1"


def _title(payload: dict[str, object]) -> str:
    embeds: Any = payload["embeds"]
    return str(embeds[0]["title"])


def _fields(payload: dict[str, object]) -> dict[str, str]:
    embeds: Any = payload["embeds"]
    return {str(item["name"]): str(item["value"]) for item in embeds[0]["fields"]}


def _short(**updates: object) -> Any:
    values: dict[str, object] = {
        "family": SignalFamily.BREAKDOWN_SHORT,
        "direction": Direction.SHORT,
        "invalidation": Decimal("102"),
    }
    values.update(updates)
    return make_decision(**values)


# --- A1 wording --------------------------------------------------------------------------


def test_default_wording_is_unchanged() -> None:
    assert _title(build_discord_payload(_short(), "bot", message_style="simple")).endswith(
        "🔴 추천: 하락 예상 · SHORT 후보"
    )
    assert _title(build_discord_payload(make_decision(), "bot", message_style="simple")).endswith(
        "🟢 추천: 상승 예상 · LONG 후보"
    )


@pytest.mark.parametrize("style", ["simple", "detailed"])
def test_wording_v2_snapshot_futures(style: str) -> None:
    short = build_discord_payload(_short(), "bot", message_style=style, wording_v2=True)  # type: ignore[arg-type]
    long = build_discord_payload(make_decision(), "bot", message_style=style, wording_v2=True)  # type: ignore[arg-type]
    assert _title(short) == "BTCUSDT · 🔻 하락 추세 확인 (5m 저점 이탈) · 신규 숏은 위치 확인 필요"
    assert _title(long) == "BTCUSDT · 🔺 상승 추세 확인 (5m 고점 돌파) · 신규 롱은 위치 확인 필요"
    assert "예상" not in _title(short) + _title(long)
    assert "추천" not in _title(short) + _title(long)


def test_wording_v2_spot_keeps_spot_meaning() -> None:
    spot_long = build_discord_payload(
        make_decision(market=Market.SPOT), "bot", message_style="simple", wording_v2=True
    )
    spot_short = build_discord_payload(
        _short(market=Market.SPOT), "bot", message_style="simple", wording_v2=True
    )
    assert _title(spot_long).endswith(
        "🔺 상승 추세 확인 (5m 고점 돌파) · 신규 매수는 위치 확인 필요"
    )
    assert _title(spot_short).endswith("🔻 하락 추세 확인 (5m 저점 이탈) · 신규 매수 보류")


def test_wording_v2_leaves_non_confirmed_and_exits_alone() -> None:
    watch = _short(stage=SignalStage.WATCH)
    assert _title(build_discord_payload(watch, "bot", message_style="simple", wording_v2=True)) == (
        _title(build_discord_payload(watch, "bot", message_style="simple"))
    )
    exit_decision = make_decision(
        family=SignalFamily.TECHNICAL_EXIT,
        metadata={"paper_only": True, "exit_reason": "trailing_stop"},
    )
    assert build_discord_payload(
        exit_decision, "bot", message_style="simple", wording_v2=True
    ) == build_discord_payload(exit_decision, "bot", message_style="simple")


# --- A2 position line (hand calculated) -----------------------------------------------


def _window(high_close: float = 97.5, other: float = 90.0) -> list[Any]:
    """48 closed bars; the highest high is high_close + 0.5, the lowest low other - 0.5."""
    bars = [make_candle(i, market=Market.FUTURES, close=other) for i in range(47)]
    bars.append(make_candle(47, market=Market.FUTURES, close=high_close))
    return bars


def test_position_line_hand_example_short() -> None:
    feature = make_feature(price=92.0, ema20=95.0, atr=1.0, rsi=24.4)
    line = build_position_line(
        direction=Direction.SHORT, ordinal=3, feature=feature, candles=_window()
    )
    # 4h high = 98.0 -> (92 / 98 - 1) * 100 = -6.12 -> -6.1 ; (92 - 95) / 1 = -3.0 ATR
    assert line.text.splitlines()[0] == (
        "이번 하락 구간 3번째 알림 · 4시간 고점 대비 -6.1% · EMA20 대비 -3.0ATR · RSI 24"
    )
    assert line.text.splitlines()[1] == "⚠️ 추격 위험: 이미 많이 내림"
    assert line.chase_risk


def test_position_line_hand_example_long() -> None:
    bars = [make_candle(i, market=Market.FUTURES, close=100.0) for i in range(47)]
    bars.append(make_candle(47, market=Market.FUTURES, close=92.5))  # lowest low = 92.0
    feature = make_feature(price=96.6, ema20=95.0, atr=2.0, rsi=61.0)
    line = build_position_line(direction=Direction.LONG, ordinal=1, feature=feature, candles=bars)
    # (96.6 / 92.0 - 1) * 100 = +5.0 ; (96.6 - 95) / 2 = +0.8 ATR ; no chase tag
    assert line.text == (
        "이번 상승 구간 1번째 알림 · 4시간 저점 대비 +5.0% · EMA20 대비 +0.8ATR · RSI 61"
    )
    assert not line.chase_risk


def _short_line(**updates: float) -> Any:
    values: dict[str, float] = {"price": 97.5, "ema20": 98.0, "atr": 1.0, "rsi": 45.0}
    values.update(updates)
    return build_position_line(
        direction=Direction.SHORT,
        ordinal=1,
        feature=make_feature(**values),
        candles=_window(high_close=100.0),  # 4h high 100.5 -> 3.0 ATR below
    )


def test_chase_risk_thresholds_and_boundaries_short() -> None:
    assert not _short_line().chase_risk
    assert not _short_line(rsi=30.0).chase_risk  # RSI < 30 is strict
    assert _short_line(rsi=29.9).chase_risk
    assert not _short_line(ema20=99.9).chase_risk  # -2.4 ATR
    assert _short_line(ema20=100.0).chase_risk  # exactly -2.5 ATR
    assert _short_line(price=97.5, ema20=98.0).chase_risk is False
    far = build_position_line(  # exactly 6 ATR under the 4h high
        direction=Direction.SHORT,
        ordinal=1,
        feature=make_feature(price=97.5, ema20=98.0, atr=1.0, rsi=45.0),
        candles=_window(high_close=103.0),
    )
    assert far.chase_risk
    near = build_position_line(
        direction=Direction.SHORT,
        ordinal=1,
        feature=make_feature(price=97.5, ema20=98.0, atr=1.0, rsi=45.0),
        candles=_window(high_close=102.9),
    )
    assert not near.chase_risk
    second = build_position_line(
        direction=Direction.SHORT,
        ordinal=2,
        feature=make_feature(price=97.5, ema20=98.0, atr=1.0, rsi=45.0),
        candles=_window(high_close=100.0),
    )
    assert second.chase_risk


def test_position_line_without_full_window_or_atr_degrades_to_na() -> None:
    line = build_position_line(
        direction=Direction.SHORT,
        ordinal=1,
        feature=make_feature(price=92.0, ema20=95.0, atr=0.0, rsi=50.0),
        candles=_window()[:10],
    )
    assert "4시간 고점 대비 n/a" in line.text
    assert "EMA20 대비 n/a" in line.text
    assert not line.chase_risk


def test_position_line_is_a_payload_field_only_when_given() -> None:
    plain = build_discord_payload(_short(), "bot", message_style="simple")
    shown = build_discord_payload(
        _short(), "bot", message_style="simple", position_line="이번 하락 구간 1번째 알림"
    )
    assert "현재 위치" not in _fields(plain)
    assert _fields(shown)["현재 위치"] == "이번 하락 구간 1번째 알림"
    detailed = build_discord_payload(
        _short(), "bot", message_style="detailed", position_line="x"
    )
    assert _fields(detailed)["Position"] == "x"


# --- A3 zone tracker (end conditions) ---------------------------------------------------


def _tracker(*, suppress: bool = True) -> TrendZoneTracker:
    return TrendZoneTracker(suppress=suppress, max_hours=12, maximum_symbols=5)


def _confirmed(minute: int, **updates: object) -> Any:
    return _short(event_time_ms=minute * 60_000, event_id=f"e{minute}", **updates)


def test_tracker_counts_and_suppresses_repeats_and_is_idempotent() -> None:
    tracker = _tracker()
    first = tracker.register(_confirmed(10))
    again = tracker.register(_confirmed(10))
    second = tracker.register(_confirmed(40))
    third = tracker.register(_confirmed(70))
    assert (first.ordinal, first.suppressed) == (1, False)
    assert again == first
    assert (second.ordinal, second.suppressed) == (2, True)
    assert (third.ordinal, third.suppressed) == (3, True)
    assert third.start_ms == 10 * 60_000


def test_tracker_without_suppression_counts_but_never_suppresses() -> None:
    tracker = _tracker(suppress=False)
    assert not tracker.register(_confirmed(10)).suppressed
    second = tracker.register(_confirmed(40))
    assert second.ordinal == 2
    assert not second.suppressed


def _bar(minute: float, **updates: object) -> Any:
    return make_feature(symbol="BTCUSDT", event_time_ms=int(minute * 60_000), **updates)


def test_zone_ends_when_close_recovers_above_ema20() -> None:
    tracker = _tracker()
    tracker.register(_confirmed(10))
    tracker.update(_bar(15, price=99.0, ema20=100.0), {})
    assert tracker.active_zone_count == 1  # below EMA20: still in the zone
    tracker.update(_bar(20, price=100.0, ema20=100.0), {})
    assert tracker.active_zone_count == 1  # exactly on EMA20 is not a recovery
    tracker.update(_bar(25, price=100.1, ema20=100.0), {})
    assert tracker.active_zone_count == 0
    assert tracker.register(_confirmed(100)).ordinal == 1


def test_zone_ends_when_hourly_alignment_is_lost() -> None:
    tracker = _tracker()
    tracker.register(_confirmed(10))
    aligned = make_feature(interval="1h", price=90.0, ema20=95.0, ema50=99.0, event_time_ms=1)
    tracker.update(_bar(15, price=90.0, ema20=100.0), {"1h": aligned})
    assert tracker.active_zone_count == 1
    broken = make_feature(interval="1h", price=96.0, ema20=95.0, ema50=99.0, event_time_ms=1)
    tracker.update(_bar(20, price=90.0, ema20=100.0), {"1h": broken})
    assert tracker.active_zone_count == 0


def test_zone_ends_after_twelve_hours_since_last_sent_alert() -> None:
    tracker = _tracker()
    tracker.register(_confirmed(10))
    tracker.register(_confirmed(10 + 6 * 60))  # suppressed: does not extend the clock
    just_before = make_feature(
        symbol="BTCUSDT", event_time_ms=(10 + 12 * 60) * 60_000 - 1, price=90.0, ema20=100.0
    )
    tracker.update(just_before, {})
    assert tracker.active_zone_count == 1
    tracker.update(_bar(10 + 12 * 60, price=90.0, ema20=100.0), {})
    assert tracker.active_zone_count == 0


def test_register_after_twelve_hours_starts_a_new_zone_even_without_update() -> None:
    tracker = _tracker()
    tracker.register(_confirmed(10))
    late = tracker.register(_confirmed(10 + 12 * 60))
    assert (late.ordinal, late.suppressed) == (1, False)


def test_zones_are_per_symbol_and_direction_and_pruned() -> None:
    tracker = _tracker()
    tracker.register(_confirmed(10))
    other = tracker.register(_confirmed(20, symbol="ETHUSDT"))
    long = tracker.register(make_decision(event_time_ms=30 * 60_000, event_id="l1"))
    assert other.ordinal == 1
    assert long.ordinal == 1  # long zone of BTCUSDT is separate from its short zone
    assert tracker.prune_symbols({"BTCUSDT"}) == 1
    assert tracker.active_zone_count == 2


def test_tracker_is_bounded() -> None:
    tracker = TrendZoneTracker(suppress=True, max_hours=12, maximum_symbols=2)
    for index in range(12):
        tracker.register(_confirmed(10 + index, symbol=f"S{index}USDT"))
    assert tracker.active_zone_count <= 4


def test_only_confirmed_trend_alerts_join_a_zone() -> None:
    tracker = _tracker()
    assert not tracker.applies(_short(stage=SignalStage.WATCH))
    assert not tracker.applies(make_decision(family=SignalFamily.PULLBACK_LONG))
    with pytest.raises(ValueError):
        tracker.register(_short(stage=SignalStage.SETUP))


# --- scenario level -------------------------------------------------------------------


@pytest.mark.parametrize("name", sorted(CASES))
async def test_all_stage1_switches_off_reproduce_the_baseline_bytes(name: str) -> None:
    raw = dict(CASES[name])
    if "family" in raw:
        raw["family"] = SignalFamily(str(raw["family"]))
    result = await run_scenario({"runtime": {"paper_gap_stats_split": False}}, **raw)  # type: ignore[arg-type]
    try:
        assert result.canonical() + "\n" == (GOLDEN / f"{name}.json").read_text(encoding="utf-8")
    finally:
        result.close()


def _by_family(result: Any, family: str) -> list[dict[str, Any]]:
    return [row for row in result.signals if row["family"] == family]


async def test_repeat_suppression_stores_every_repeat_but_delivers_only_the_zone_start() -> None:
    off = await run_scenario({"runtime": {"paper_gap_stats_split": False}})
    on = await run_scenario(
        {"alerts": {"repeat_suppression": True}, "runtime": {"paper_gap_stats_split": False}}
    )
    try:
        assert {row["event_id"] for row in on.signals} == {row["event_id"] for row in off.signals}
        shorts_on = sorted(_by_family(on, "breakdown_short"), key=lambda row: row["event_time_ms"])
        assert len(shorts_on) == 4
        status = {row["event_id"]: row["status"] for row in on.outbox}
        statuses = [status[row["event_id"]] for row in shorts_on]
        assert statuses == ["pending", "disabled", "disabled", "disabled"]
        flags = [json.loads(row["payload_json"])["metadata"] for row in shorts_on]
        assert [meta.get("suppressed_repeat") for meta in flags] == [None, True, True, True]
        assert [meta["alert_episode_ordinal"] for meta in flags] == [1, 2, 3, 4]
        # PAPER outcome and its exit alerts are untouched by the suppression.
        assert _by_family(on, "technical_exit") == _by_family(off, "technical_exit")
        assert [row["status"] for row in on.outbox if row["event_id"] in {
            r["event_id"] for r in _by_family(on, "technical_exit")
        }] == ["pending"]
        # Without the switch every alert is delivered.
        assert {row["status"] for row in off.outbox} == {"pending"}
    finally:
        on.close()
        off.close()


async def test_zone_restarts_after_the_price_recovers_above_ema20() -> None:
    result = await run_scenario(
        {"alerts": {"repeat_suppression": True}}, triggers=(250, 258, 330)
    )
    try:
        shorts = sorted(_by_family(result, "breakdown_short"), key=lambda r: r["event_time_ms"])
        assert len(shorts) == 3
        status = {row["event_id"]: row["status"] for row in result.outbox}
        # The synthetic path rebounds above EMA20 after bar 300, which closes the first zone.
        assert [status[row["event_id"]] for row in shorts] == ["pending", "disabled", "pending"]
        ordinals = [
            json.loads(row["payload_json"])["metadata"]["alert_episode_ordinal"] for row in shorts
        ]
        assert ordinals == [1, 2, 1]
    finally:
        result.close()


async def test_wording_and_position_line_reach_the_stored_payload_only_when_enabled() -> None:
    result = await run_scenario(
        {"alerts": {"alert_wording_v2": True, "alert_position_line": True}}
    )
    try:
        outbox = {row["event_id"]: row for row in result.outbox}
        shorts = sorted(_by_family(result, "breakdown_short"), key=lambda r: r["event_time_ms"])
        payloads = [json.loads(outbox[row["event_id"]]["payload_json"]) for row in shorts]
        titles = [_title(item) for item in payloads]
        assert all(
            title == "BTCUSDT · 🔻 하락 추세 확인 (5m 저점 이탈) · 신규 숏은 위치 확인 필요"
            for title in titles
        )
        lines = [_fields(item)["현재 위치"] for item in payloads]
        assert lines[0].startswith("이번 하락 구간 1번째 알림 · 4시간 고점 대비 ")
        assert "ATR" in lines[0]
        assert "RSI" in lines[0]
        assert [line.startswith(f"이번 하락 구간 {i + 1}번째") for i, line in enumerate(lines)] == [
            True
        ] * 4
        assert "⚠️ 추격 위험: 이미 많이 내림" in lines[1]  # second alert of the zone
        # Suppression is a separate switch: with only wording/line on, all four are delivered.
        assert {outbox[row["event_id"]]["status"] for row in shorts} == {"pending"}
        # Exit alerts do not get a position line.
        exits = _by_family(result, "technical_exit")
        assert exits
        assert all("현재 위치" not in _fields(json.loads(outbox[r["event_id"]]["payload_json"]))
                   for r in exits)
    finally:
        result.close()


async def test_long_alert_is_mirrored_in_wording_and_position_line() -> None:
    result = await run_scenario(
        {"alerts": {"repeat_suppression": True, "alert_wording_v2": True,
                    "alert_position_line": True}},
        family=SignalFamily.BREAKOUT_LONG,
    )
    try:
        longs = sorted(_by_family(result, "breakout_long"), key=lambda r: r["event_time_ms"])
        status = {row["event_id"]: row for row in result.outbox}
        payload = json.loads(status[longs[0]["event_id"]]["payload_json"])
        assert status[longs[0]["event_id"]]["status"] == "pending"
        assert _title(payload) == (
            "BTCUSDT · 🔺 상승 추세 확인 (5m 고점 돌파) · 신규 롱은 위치 확인 필요"
        )
        assert _fields(payload)["현재 위치"].startswith(
            "이번 상승 구간 1번째 알림 · 4시간 저점 대비 "
        )
    finally:
        result.close()


def test_long_zone_ends_when_close_falls_below_ema20() -> None:
    tracker = _tracker()
    tracker.register(make_decision(event_time_ms=10 * 60_000, event_id="l1"))
    tracker.update(_bar(15, price=101.0, ema20=100.0), {})
    assert tracker.active_zone_count == 1
    tracker.update(_bar(20, price=99.9, ema20=100.0), {})
    assert tracker.active_zone_count == 0


def test_scenario_time_constants_are_aligned() -> None:
    assert T0_MS % STEP_MS == 0
