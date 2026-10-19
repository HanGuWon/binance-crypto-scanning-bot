"""Stage 1 PAPER data_gap changes: B1 labelling, B2 recovery order, B3 log line, B4 continuity."""

from __future__ import annotations

import json
import logging
from typing import Any

import pytest
from stage1_scenario import (
    SYMBOL,
    Scenario,
    make_bar,
    run_scenario,
)

from signalbot.alerts.embeds import build_discord_payload
from signalbot.data.candles import CandleStore
from signalbot.domain.enums import Market, SignalFamily
from signalbot.observability.logging import MASK, JsonFormatter, mask_value
from signalbot.signals.paper_stats import classify_paper_exit, summarize_paper_exits
from signalbot.signals.positions import PaperRecoveryError

OFF = {"runtime": {"paper_gap_stats_split": False}}
ON = {"runtime": {"paper_gap_recovery": True}}


def _exits(result: Any) -> list[dict[str, Any]]:
    rows = [json.loads(row["payload_json"]) for row in result.signals]
    return [row for row in rows if row["family"] == "technical_exit"]


def _reasons(result: Any) -> list[str]:
    return sorted(str(row["metadata"]["exit_reason"]) for row in _exits(result))


def _outbox(result: Any, event_id: str) -> dict[str, Any]:
    return json.loads(next(r for r in result.outbox if r["event_id"] == event_id)["payload_json"])


def _title(payload: dict[str, Any]) -> str:
    return str(payload["embeds"][0]["title"])


def _fields(payload: dict[str, Any]) -> dict[str, str]:
    return {str(f["name"]): str(f["value"]) for f in payload["embeds"][0]["fields"]}


# --- B1 statistics separation ------------------------------------------------------------


async def test_data_gap_exit_is_tagged_as_interrupted_by_default() -> None:
    result = await run_scenario(missing=[256])
    try:
        gap = [row for row in _exits(result) if row["metadata"]["exit_reason"] == "data_gap"]
        assert len(gap) == 1
        meta = gap[0]["metadata"]
        assert meta["exit_class"] == "tracking_interrupted"
        assert meta["censored"] is True
        # reason, price, fill time and the event id itself stay what they were
        plain = await run_scenario(OFF, missing=[256])
        try:
            old = [row for row in _exits(plain) if row["metadata"]["exit_reason"] == "data_gap"]
            assert old[0]["event_id"] == gap[0]["event_id"]
            assert old[0]["price"] == gap[0]["price"]
            assert old[0]["event_time_ms"] == gap[0]["event_time_ms"]
            stripped = {k: v for k, v in meta.items() if k not in {"exit_class", "censored"}}
            assert stripped == old[0]["metadata"]
        finally:
            plain.close()
    finally:
        result.close()


async def test_interrupted_track_is_labelled_separately_in_the_alert() -> None:
    result = await run_scenario(missing=[256])
    try:
        gap = next(
            row for row in _exits(result) if row["metadata"]["exit_reason"] == "data_gap"
        )
        payload = _outbox(result, gap["event_id"])
        assert "추적 중단 (데이터 공백)" in _title(payload)
        assert "정상 청산과 따로 집계" in _fields(payload)["모의 종료 이유"]
        assert "청산이 아닌 추적 중단" in payload["embeds"][0]["description"]
        normal = await run_scenario()
        try:
            other = _exits(normal)[0]
            assert "포지션 종료" in _title(_outbox(normal, other["event_id"]))
        finally:
            normal.close()
    finally:
        result.close()


def test_classification_works_on_old_and_new_rows_and_counts_only() -> None:
    rows = [
        {"paper_only": True, "exit_reason": "initial_stop"},
        {"paper_only": True, "exit_reason": "trailing_stop"},
        {"paper_only": True, "exit_reason": "trailing_stop"},
        {"paper_only": True, "exit_reason": "data_gap"},  # written by the previous release
        {"paper_only": True, "exit_reason": "data_gap", "exit_class": "tracking_interrupted"},
        {"paper_only": True, "exit_reason": "trend_failure"},
        {"exit_reason": "initial_stop"},  # not a PAPER row
    ]
    assert classify_paper_exit(rows[3]) == "tracking_interrupted"
    assert classify_paper_exit(rows[0]) == "completed"
    summary = summarize_paper_exits(rows)
    assert summary == {
        "completed_total": 4,
        "completed_by_reason": {"initial_stop": 1, "trailing_stop": 2, "trend_failure": 1},
        "tracking_interrupted_total": 2,
        "tracking_interrupted_by_reason": {"data_gap": 2},
        "all_tracks": 6,
    }


# --- B2 recovery order ---------------------------------------------------------------------


async def test_recovery_on_continues_the_position_through_a_recovered_bar() -> None:
    plain = await run_scenario()
    recovered = await run_scenario(ON, missing=[256])
    legacy = await run_scenario(OFF, missing=[256])
    try:
        # (the previous release ends the track; later triggers open unrelated new tracks)
        assert "data_gap" in _reasons(legacy)
        assert "data_gap" not in _reasons(recovered)
        assert _reasons(recovered) == _reasons(plain) == ["trailing_stop"]
        # The continued track ends exactly like the track that never had a gap.
        assert _exits(recovered)[0]["event_id"] == _exits(plain)[0]["event_id"]
        assert _exits(recovered)[0]["metadata"] == _exits(plain)[0]["metadata"]
        assert recovered.runtime.candles.missing_opens(Market.FUTURES, SYMBOL, "5m", 210) == []
    finally:
        for item in (plain, recovered, legacy):
            item.close()


async def test_recovery_on_multi_bar_gap_is_replayed_in_order() -> None:
    result = await run_scenario(ON, missing=[255, 256, 257])
    plain = await run_scenario()
    try:
        assert _reasons(result) == ["trailing_stop"]
        assert _exits(result)[0]["event_id"] == _exits(plain)[0]["event_id"]
        assert [(g.start_time_ms, g.end_time_ms) for g in result.recoverer_calls] == [
            (make_bar(255).open_time_ms, make_bar(258).open_time_ms - 1)
        ]
    finally:
        result.close()
        plain.close()


async def test_exit_inside_a_recovered_bar_is_labelled_as_late_observation() -> None:
    plain = await run_scenario()
    late = await run_scenario(ON, missing=[303])
    try:
        exit_plain = _exits(plain)[0]
        exit_late = _exits(late)[0]
        assert exit_late["event_id"] == exit_plain["event_id"]
        assert exit_late["event_time_ms"] == exit_plain["event_time_ms"]  # bar time, unchanged
        meta = dict(exit_late["metadata"])
        observed = meta.pop("observed_wall_time_ms")
        assert meta.pop("late_observation") is True
        assert meta.pop("gap_recovery_policy") == "paper_gap_recovery_v1"
        assert meta == exit_plain["metadata"]
        # learned when bar 304 arrived, one bar after the bar in which the stop was touched
        assert observed == make_bar(304).close_time_ms + 1_000
        assert observed > exit_late["event_time_ms"]
        payload = _outbox(late, exit_late["event_id"])
        assert "데이터 복구 후 늦게 확인됨" in payload["embeds"][0]["description"]
        assert "체결된 것이 아닙니다" in _fields(payload)["확인 시각 (복구 후)"]
        late_decision = next(
            item for item in late.emitted if item.family is SignalFamily.TECHNICAL_EXIT
        )
        detailed = build_discord_payload(late_decision, "bot", message_style="detailed")
        timing = _fields(detailed)["Paper timing"]
        assert "Observed after gap recovery (not a live fill)" in timing
    finally:
        plain.close()
        late.close()


async def test_recovery_never_creates_an_entry_or_signal_from_a_past_bar() -> None:
    # a trigger on the missing bar itself, and a pending entry from the bar before it
    pending = await run_scenario(ON, triggers=(255, 256), missing=[256])
    legacy = await run_scenario(OFF, triggers=(255, 256), missing=[256])
    try:
        for item in (pending, legacy):
            assert [row["event_time_ms"] for row in item.signals] == [make_bar(255).close_time_ms]
            assert _exits(item) == []
            assert item.runtime.paper_positions.active_position_count == 0
            assert item.runtime.paper_positions.pending_entry_count == 0
        assert pending.canonical() == legacy.canonical()
    finally:
        pending.close()
        legacy.close()


@pytest.mark.parametrize("recover", ["empty", "none"])
async def test_failed_recovery_ends_the_track_like_the_previous_release(recover: str) -> None:
    on = await run_scenario(
        {"runtime": {"paper_gap_recovery": True, "paper_gap_stats_split": False}},
        missing=[256],
        recover=recover,
    )
    off = await run_scenario(OFF, missing=[256], recover=recover)
    try:
        assert _reasons(on) == ["data_gap"]
        assert on.canonical() == off.canonical()
    finally:
        on.close()
        off.close()


async def test_recovery_failure_with_stats_split_still_ends_as_interrupted() -> None:
    result = await run_scenario(ON, missing=[256], recover="empty")
    try:
        gap = _exits(result)[0]
        assert gap["metadata"]["exit_reason"] == "data_gap"
        assert gap["metadata"]["exit_class"] == "tracking_interrupted"
    finally:
        result.close()


async def test_missing_indicator_row_for_a_recovered_bar_falls_back_to_data_gap() -> None:
    scenario = Scenario(ON, missing=[256])
    await scenario.feed(0, 256)
    real = scenario.runtime._update_derived_for_candle
    scenario.runtime._update_derived_for_candle = (  # type: ignore[method-assign]
        lambda candle: None if candle.open_time_ms == make_bar(256).open_time_ms else real(candle)
    )
    await scenario.feed(256, 262)
    result = scenario.result()
    try:
        assert _reasons(result) == ["data_gap"]
    finally:
        result.close()


async def test_persistence_failure_during_recovery_restores_the_paper_state() -> None:
    scenario = Scenario(ON, missing=[303])
    await scenario.feed(0, 303)
    paper = scenario.runtime.paper_positions
    assert paper.has_position(SYMBOL)
    before = paper.checkpoint_symbol(SYMBOL)._state
    real = scenario.runtime._persist_decision

    def explode(decision: Any, **kwargs: Any) -> bool:
        if decision.family is SignalFamily.TECHNICAL_EXIT:
            raise RuntimeError("database unavailable")
        return real(decision, **kwargs)

    scenario.runtime._persist_decision = explode  # type: ignore[method-assign]
    with pytest.raises(RuntimeError, match="database unavailable"):
        await scenario.feed(304, 305)
    after = paper.checkpoint_symbol(SYMBOL)._state
    assert paper.has_position(SYMBOL)
    assert after is not None and before is not None
    assert after.last_open_time_ms == before.last_open_time_ms
    assert after.bar_index == before.bar_index
    scenario.result().close()


async def test_recovered_bars_are_replayed_without_duplicate_exit_rows() -> None:
    result = await run_scenario(ON, missing=[303, 304])
    try:
        assert _reasons(result) == ["trailing_stop"]
        assert len({row["event_id"] for row in result.signals}) == len(result.signals)
    finally:
        result.close()


def test_advance_recovered_candle_rejects_a_non_consecutive_bar() -> None:
    from conftest import make_feature
    from signalbot.config import TechnicalExitSettings
    from signalbot.signals.positions import PaperPositionLifecycle

    lifecycle = PaperPositionLifecycle(
        TechnicalExitSettings(enabled=True),
        rule_version="t",
        market=Market.FUTURES,
        primary_interval="5m",
        maximum_symbols=3,
    )
    bar0 = make_bar(10)
    lifecycle.on_closed_candle(
        bar0, make_feature(symbol=SYMBOL, event_time_ms=bar0.close_time_ms), []
    )
    skip = make_bar(12)
    with pytest.raises(PaperRecoveryError):
        lifecycle.advance_recovered_candle(
            skip, make_feature(symbol=SYMBOL, event_time_ms=skip.close_time_ms)
        )
    assert lifecycle.advance_recovered_candle(
        make_bar(10), make_feature(symbol=SYMBOL, event_time_ms=bar0.close_time_ms)
    ) == []


# --- B3 observation log ---------------------------------------------------------------------


def _gap_lines(records: list[logging.LogRecord]) -> list[dict[str, Any]]:
    formatter = JsonFormatter()
    return [
        json.loads(formatter.format(record))
        for record in records
        if getattr(record, "fields", {}).get("event") == "candle_gap"
    ]


async def test_gap_line_records_range_result_duration_and_paper_outcome(
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.INFO, logger="signalbot.runtime")
    on = await run_scenario(ON, missing=[256, 257])
    off = await run_scenario(missing=[300])
    try:
        lines = _gap_lines(caplog.records)
        assert len(lines) == 2
        recovered, legacy = lines
        fields = recovered["fields"]
        assert recovered["symbol"] == SYMBOL
        assert recovered["market"] == "futures"
        assert fields["gap_start_open_ms"] == make_bar(256).open_time_ms
        assert fields["gap_end_ms"] == make_bar(258).open_time_ms - 1
        assert fields["missing_bars"] == 2
        assert fields["first_return_open_ms"] == make_bar(258).open_time_ms
        assert fields["rest_ok"] is True
        assert fields["recovered_bars"] == 2
        assert isinstance(fields["elapsed_ms"], int) and fields["elapsed_ms"] >= 0
        assert fields["paper_policy"] == "paper_gap_recovery_v1"
        assert fields["paper_result"] == "continued"
        assert legacy["fields"]["paper_policy"] == "legacy"
        assert legacy["fields"]["rest_ok"] is True
    finally:
        on.close()
        off.close()


async def test_gap_line_for_a_failed_recovery_and_a_data_gap(
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.INFO, logger="signalbot.runtime")
    result = await run_scenario(ON, missing=[256], recover="empty")
    try:
        first, *later = _gap_lines(caplog.records)  # every later bar retries the same gap
        assert first["fields"]["rest_ok"] is False
        assert first["fields"]["recovered_bars"] == 0
        assert first["fields"]["paper_result"] == "data_gap"
        assert first["fields"]["paper_exit_reasons"] == ["data_gap"]
        assert later
        assert {line["fields"]["paper_result"] for line in later} == {"no_position"}
    finally:
        result.close()


async def test_gap_log_can_be_switched_off(caplog: pytest.LogCaptureFixture) -> None:
    caplog.set_level(logging.INFO, logger="signalbot.runtime")
    result = await run_scenario({"runtime": {"paper_gap_log": False}}, missing=[256])
    try:
        assert _gap_lines(caplog.records) == []
    finally:
        result.close()


def _fake_hook() -> str:
    """Synthetic URL shaped like a chat webhook, assembled so no secret-like literal exists."""

    return "https://" + "chat.invalid" + "/api/" + "webhooks" + "/" + "7" * 18 + "/" + "z" * 40


def test_log_fields_never_carry_secrets() -> None:
    hook = _fake_hook()
    token = "A" * 48
    masked = mask_value(
        {
            "event": "candle_gap",
            "note": f"posted to {hook}",
            "api_key": "k-1234",
            "Authorization": "Bearer xyz",
            "nested": {"discord_webhook_url": hook, "values": [token, 5, "ok"]},
        }
    )
    dumped = json.dumps(masked)
    assert hook not in dumped
    assert token not in dumped
    assert "/webhooks/" not in dumped
    assert masked["api_key"] == MASK
    assert masked["Authorization"] == MASK
    assert masked["nested"]["discord_webhook_url"] == MASK
    assert masked["event"] == "candle_gap"
    assert masked["nested"]["values"] == [MASK, 5, "ok"]


def test_formatter_masks_fields_and_ignores_records_without_fields() -> None:
    formatter = JsonFormatter()
    record = logging.LogRecord("x", logging.INFO, __file__, 1, "msg", None, None)
    assert "fields" not in json.loads(formatter.format(record))
    record.fields = {"webhook": _fake_hook(), "n": 3}  # type: ignore[attr-defined]
    parsed = json.loads(formatter.format(record))
    assert parsed["fields"] == {"webhook": MASK, "n": 3}


# --- B4 indicator continuity ---------------------------------------------------------------------


def test_missing_opens_reports_holes_between_stored_bars_only() -> None:
    store = CandleStore(600)
    for index in [*range(0, 10), *range(13, 20)]:
        store.add(make_bar(index))
    opens = store.missing_opens(Market.FUTURES, SYMBOL, "5m", 210)
    assert opens == [make_bar(i).open_time_ms for i in (10, 11, 12)]
    # a hole older than the window does not count
    assert store.missing_opens(Market.FUTURES, SYMBOL, "5m", 5) == []
    # history that starts late is not a hole
    late = CandleStore(600)
    for index in range(100, 110):
        late.add(make_bar(index))
    assert late.missing_opens(Market.FUTURES, SYMBOL, "5m", 210) == []
    with pytest.raises(ValueError):
        store.missing_opens(Market.FUTURES, SYMBOL, "5m", 1)


async def _with_hole(overrides: dict[str, Any], triggers: tuple[int, ...]) -> Scenario:
    scenario = Scenario(
        {"runtime": {"require_contiguous_bars": True, **overrides}},
        triggers=triggers,
    )
    await scenario.feed(0, 270)
    series = scenario.runtime.candles._series[(Market.FUTURES, SYMBOL, "5m")]
    del series[[c.open_time_ms for c in series].index(make_bar(262).open_time_ms)]
    return scenario


async def test_hold_blocks_signal_evaluation_but_paper_keeps_tracking() -> None:
    scenario = await _with_hole({}, (250, 275))
    await scenario.feed(270, 320)
    result = scenario.result()
    try:
        families = [(r["family"], r["event_time_ms"]) for r in result.signals]
        assert ("breakdown_short", make_bar(250).close_time_ms) in families
        assert ("breakdown_short", make_bar(275).close_time_ms) not in families  # held
        assert _reasons(result) == ["trailing_stop"]  # the open PAPER position was not frozen
        assert "data_gap" not in _reasons(result)
    finally:
        result.close()


async def test_hold_is_released_after_the_limit_so_a_symbol_cannot_freeze() -> None:
    scenario = await _with_hole({"contiguity_hold_limit_bars": 5}, (272, 280))
    await scenario.feed(270, 300)
    result = scenario.result()
    try:
        times = {r["event_time_ms"] for r in result.signals if r["family"] == "breakdown_short"}
        assert make_bar(272).close_time_ms not in times  # held (bars 270..274)
        assert make_bar(280).close_time_ms in times  # released after 5 held bars
        assert scenario.runtime._contiguity[(SYMBOL, "5m")].released_through_open_ms == (
            make_bar(262).open_time_ms
        )
    finally:
        result.close()


async def test_hold_clears_when_the_hole_leaves_the_window() -> None:
    scenario = await _with_hole(
        {"contiguity_window_bars": 20, "contiguity_hold_limit_bars": 600}, (272, 285)
    )
    await scenario.feed(270, 300)
    result = scenario.result()
    try:
        times = {r["event_time_ms"] for r in result.signals if r["family"] == "breakdown_short"}
        assert make_bar(272).close_time_ms not in times
        assert make_bar(285).close_time_ms in times  # hole (262) is outside the 20-bar window
        assert (SYMBOL, "5m") not in scenario.runtime._contiguity
    finally:
        result.close()


async def test_switch_off_never_holds_even_with_a_hole() -> None:
    scenario = Scenario(triggers=(250, 275))
    await scenario.feed(0, 270)
    series = scenario.runtime.candles._series[(Market.FUTURES, SYMBOL, "5m")]
    del series[[c.open_time_ms for c in series].index(make_bar(262).open_time_ms)]
    await scenario.feed(270, 280)
    result = scenario.result()
    try:
        times = {r["event_time_ms"] for r in result.signals if r["family"] == "breakdown_short"}
        assert make_bar(275).close_time_ms in times
    finally:
        result.close()


async def test_hold_start_and_release_are_logged_once(caplog: pytest.LogCaptureFixture) -> None:
    caplog.set_level(logging.WARNING, logger="signalbot.runtime")
    scenario = await _with_hole({"contiguity_hold_limit_bars": 3}, ())
    await scenario.feed(270, 290)
    scenario.result().close()
    formatter = JsonFormatter()
    events = [
        json.loads(formatter.format(record))["fields"]["event"]
        for record in caplog.records
        if getattr(record, "fields", {}).get("event", "").startswith("contiguity")
    ]
    assert events == ["contiguity_hold", "contiguity_released"]

