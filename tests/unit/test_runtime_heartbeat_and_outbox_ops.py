from __future__ import annotations

import argparse
import asyncio
import logging
from pathlib import Path
from typing import Any

import httpx
import pytest
from sqlalchemy import create_engine, inspect, select
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import Session

from conftest import make_decision
from signalbot.alerts.discord import DiscordNotifier
from signalbot.alerts.embeds import build_discord_payload
from signalbot.api.server import create_api
from signalbot.cli import _parser
from signalbot.clock import ReplayClock
from signalbot.config import AlertSettings, RuntimeSettings, Settings
from signalbot.domain.enums import Market
from signalbot.heartbeat import HeartbeatRecorder, record_outbox_drain
from signalbot.outbox_cli import run_outbox_command
from signalbot.persistence.models import AlertRow, Base, RuntimeHeartbeatRow
from signalbot.persistence.repository import OutboxResolveError, SqlRepository
from signalbot.runtime import MarketRuntime

SECRET_MARKER = "SECRET-PAYLOAD-AND-URL-MARKER"


def _repo() -> SqlRepository:
    repo = SqlRepository("sqlite:///:memory:")
    repo.initialize()
    return repo


def _enqueue(repo: SqlRepository, event_id: str, status: str = "pending") -> None:
    decision = make_decision(event_id=event_id)
    payload = build_discord_payload(decision, "Test Bot")
    payload["content"] = f"https://discord.com/api/webhooks/1/{SECRET_MARKER}"
    repo.save_signal_and_enqueue(
        decision, payload, 1_000, delivery_enabled=True, maximum_active_items=100
    )
    if status == "uncertain":
        assert repo.claim_outbox(event_id, 2_000) is not None
        assert repo.mark_outbox(event_id, "uncertain", 3_000, detail="ambiguous")
    elif status == "delivered":
        assert repo.claim_outbox(event_id, 2_000) is not None
        assert repo.mark_outbox(event_id, "delivered", 3_000, message_id="m-1")


# --------------------------------------------------------------------------
# Recorder throttling and failure isolation
# --------------------------------------------------------------------------


def test_heartbeat_writes_are_throttled_per_market() -> None:
    repo = _repo()
    clock = ReplayClock(1_000_000)
    recorder = HeartbeatRecorder(repo, "spot", clock)
    recorder.note_ws_message()
    assert recorder.write_count == 1
    clock.current_ms = 1_000_000 + 14_999
    recorder.note_ws_message()
    recorder.note_closed_candle(123)
    recorder.note_decision()
    assert recorder.write_count == 1
    clock.current_ms = 1_000_000 + 15_000
    recorder.note_ws_message()
    assert recorder.write_count == 2
    record = repo.get_heartbeats()["spot"]
    assert record.last_ws_message_ms == 1_015_000
    assert record.last_closed_candle_ms == 123
    assert record.last_decision_ms == 1_014_999
    repo.close()


def test_heartbeat_burst_of_messages_writes_once_per_interval() -> None:
    repo = _repo()
    clock = ReplayClock(0)
    recorder = HeartbeatRecorder(repo, "futures", clock)
    for step in range(1_000):
        clock.current_ms = step  # 1 s of traffic
        recorder.note_ws_message()
    assert recorder.write_count == 1
    repo.close()


def test_heartbeat_rejects_non_positive_interval() -> None:
    repo = _repo()
    with pytest.raises(ValueError):
        HeartbeatRecorder(repo, "spot", ReplayClock(0), min_interval_ms=0)
    repo.close()


def test_heartbeat_write_failure_is_logged_and_never_raised(
    caplog: pytest.LogCaptureFixture,
) -> None:
    repo = _repo()
    attempts = 0

    def broken(*_a: Any, **_k: Any) -> None:
        nonlocal attempts
        attempts += 1
        raise OperationalError("UPSERT", {}, Exception("database is locked"))

    repo.upsert_heartbeat = broken  # type: ignore[method-assign]
    clock = ReplayClock(0)
    recorder = HeartbeatRecorder(repo, "spot", clock)
    with caplog.at_level(logging.ERROR):
        recorder.note_ws_message()
        recorder.note_ws_message()  # within the interval: no second attempt
        assert attempts == 1
        clock.current_ms = 15_000
        recorder.note_ws_message()
    assert attempts == 2
    errors = [r for r in caplog.records if r.levelno == logging.ERROR]
    assert len(errors) == 2 and all(r.exc_info for r in errors)
    assert recorder.write_count == 0
    repo.close()


def test_outbox_drain_heartbeat_writes_all_markets_and_survives_failure(
    caplog: pytest.LogCaptureFixture,
) -> None:
    repo = _repo()
    record_outbox_drain(repo, ["spot", "futures"], ReplayClock(5_000))
    heartbeats = repo.get_heartbeats()
    assert {m: h.last_outbox_drain_ms for m, h in heartbeats.items()} == {
        "spot": 5_000,
        "futures": 5_000,
    }

    def broken(*_a: Any, **_k: Any) -> None:
        raise OperationalError("UPSERT", {}, Exception("locked"))

    repo.upsert_heartbeat = broken  # type: ignore[method-assign]
    with caplog.at_level(logging.ERROR):
        record_outbox_drain(repo, ["spot"], ReplayClock(6_000))
    assert any(r.levelno == logging.ERROR for r in caplog.records)
    repo.close()


def test_runtime_records_ws_heartbeat_even_for_malformed_payloads() -> None:
    repo = _repo()
    clock = ReplayClock(42_000)
    runtime = MarketRuntime(
        Market.SPOT, Settings(), repo, clock, lambda decision: asyncio.sleep(0)
    )
    runtime.heartbeat = HeartbeatRecorder(repo, "spot", clock)
    asyncio.run(runtime.handle_payload({"unrecognized": True}))
    assert repo.get_heartbeats()["spot"].last_ws_message_ms == 42_000
    repo.close()


def test_runtime_without_heartbeat_attribute_set_does_not_write() -> None:
    repo = _repo()
    runtime = MarketRuntime(
        Market.SPOT, Settings(), repo, ReplayClock(1), lambda decision: asyncio.sleep(0)
    )
    asyncio.run(runtime.handle_payload({"unrecognized": True}))
    assert repo.get_heartbeats() == {}
    repo.close()


def test_create_all_adds_heartbeat_table_to_an_existing_database(tmp_path: Path) -> None:
    url = f"sqlite:///{tmp_path / 'old.db'}"
    engine = create_engine(url)
    old_tables = [t for t in Base.metadata.sorted_tables if t.name != "runtime_heartbeats"]
    Base.metadata.create_all(engine, tables=old_tables)
    assert "runtime_heartbeats" not in inspect(engine).get_table_names()
    engine.dispose()
    repo = SqlRepository(url)
    repo.initialize()
    assert "runtime_heartbeats" in inspect(repo.engine).get_table_names()
    repo.upsert_heartbeat("spot", 1, last_ws_message_ms=1)
    with Session(repo.engine) as session:
        assert session.scalars(select(RuntimeHeartbeatRow)).one().market == "spot"
    repo.close()


def test_upsert_merges_fields_and_keeps_a_single_row_per_market() -> None:
    repo = _repo()
    repo.upsert_heartbeat("spot", 10, last_ws_message_ms=5)
    repo.upsert_heartbeat("spot", 20, last_decision_ms=7)
    record = repo.get_heartbeats()["spot"]
    assert (record.last_ws_message_ms, record.last_decision_ms, record.updated_at_ms) == (5, 7, 20)
    assert len(repo.get_heartbeats()) == 1
    repo.close()


# --------------------------------------------------------------------------
# Readiness and summary endpoints
# --------------------------------------------------------------------------


async def _get(app: Any, path: str) -> httpx.Response:
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        return await client.get(path)


@pytest.mark.asyncio
@pytest.mark.parametrize(("offset_ms", "status"), [(-1, 200), (0, 200), (1, 503)])
async def test_readiness_staleness_boundary(offset_ms: int, status: int) -> None:
    repo = _repo()
    heartbeat_ms = 1_000_000
    for market in ("spot", "futures"):
        repo.upsert_heartbeat(market, heartbeat_ms, last_ws_message_ms=heartbeat_ms)
    now = heartbeat_ms + 120_000 + offset_ms
    app = create_api(
        repo,
        markets=[Market.SPOT, Market.FUTURES],
        ready_max_staleness_seconds=120,
        now_ms=lambda: now,
    )
    response = await _get(app, "/health/ready")
    assert response.status_code == status
    body = response.json()
    assert body["status"] == ("ready" if status == 200 else "not_ready")
    if status == 503:
        assert body["reasons"]
    repo.close()


@pytest.mark.asyncio
async def test_readiness_requires_every_configured_market() -> None:
    repo = _repo()
    repo.upsert_heartbeat("spot", 1_000, last_ws_message_ms=1_000)
    app = create_api(repo, markets=[Market.SPOT, Market.FUTURES], now_ms=lambda: 2_000)
    response = await _get(app, "/health/ready")
    assert response.status_code == 503
    assert any("futures" in reason for reason in response.json()["reasons"])
    repo.close()


@pytest.mark.asyncio
async def test_readiness_503_when_heartbeat_row_has_no_ws_message() -> None:
    repo = _repo()
    repo.upsert_heartbeat("spot", 1_000, last_outbox_drain_ms=1_000)
    app = create_api(repo, markets=[Market.SPOT], now_ms=lambda: 2_000)
    assert (await _get(app, "/health/ready")).status_code == 503
    repo.close()


@pytest.mark.asyncio
async def test_readiness_503_when_repository_not_ready() -> None:
    repo = SqlRepository("sqlite:///:memory:")
    app = create_api(repo, markets=[Market.SPOT])
    response = await _get(app, "/health/ready")
    assert response.status_code == 503
    assert "repository" in response.json()["reasons"][0]


@pytest.mark.asyncio
async def test_live_and_legacy_ready_are_unchanged() -> None:
    repo = _repo()
    app = create_api(repo)
    assert (await _get(app, "/health/live")).json() == {"status": "alive"}
    assert (await _get(app, "/health/ready")).json() == {"status": "ready"}
    configured = create_api(repo, markets=[Market.SPOT])
    assert (await _get(configured, "/health/live")).json() == {"status": "alive"}
    repo.close()


@pytest.mark.asyncio
async def test_outbox_summary_has_counts_and_ages_but_no_payload_or_urls() -> None:
    repo = _repo()
    _enqueue(repo, "p1")
    _enqueue(repo, "u1", "uncertain")
    _enqueue(repo, "d1", "delivered")
    app = create_api(repo, now_ms=lambda: 61_000)
    response = await _get(app, "/outbox/summary")
    assert response.status_code == 200
    body = response.json()
    assert body["counts_by_status"] == {"delivered": 1, "pending": 1, "uncertain": 1}
    assert body["uncertain_count"] == 1
    assert body["oldest_pending_age_ms"] == 60_000
    text = response.text
    assert SECRET_MARKER not in text
    assert "webhooks" not in text
    assert "ambiguous" not in text
    repo.close()


# --------------------------------------------------------------------------
# outbox status / resolve CLI
# --------------------------------------------------------------------------


def _settings(tmp_path: Path) -> Settings:
    return Settings.model_validate({"storage": {"url": f"sqlite:///{tmp_path / 'ops.db'}"}})


def _resolve_args(event_id: str, resolution: str = "delivered", reason: str = "checked", **kw: Any):
    return argparse.Namespace(
        outbox_command="resolve",
        event_id=event_id,
        resolution=resolution,
        reason=reason,
        message_id=kw.get("message_id"),
    )


def _seed(settings: Settings, event_id: str, status: str) -> None:
    repo = SqlRepository(settings.storage.url)
    repo.initialize()
    _enqueue(repo, event_id, status)
    if status == "uncertain":
        repo.record_alert(event_id, 1, "uncertain", 3_000, None, "ambiguous")
    repo.close()


def test_resolve_uncertain_as_delivered_writes_audit_and_keeps_history(tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    _seed(settings, "u1", "uncertain")
    code = run_outbox_command(
        _resolve_args("u1", "delivered", "found in channel", message_id="msg-9"), settings
    )
    assert code == 0
    repo = SqlRepository(settings.storage.url)
    repo.initialize()
    item = repo.get_outbox("u1")
    assert item is not None and item.status == "delivered" and item.message_id == "msg-9"
    with Session(repo.engine) as session:
        rows = session.scalars(select(AlertRow).where(AlertRow.event_id == "u1")).all()
    assert [(r.attempt, r.status) for r in sorted(rows, key=lambda r: r.attempt)] == [
        (1, "uncertain"),
        (2, "resolved_delivered"),
    ]
    assert "found in channel" in (rows[-1].detail or "")
    repo.close()


def test_resolve_uncertain_as_dead(tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    _seed(settings, "u2", "uncertain")
    assert run_outbox_command(_resolve_args("u2", "dead", "not in channel"), settings) == 0
    repo = SqlRepository(settings.storage.url)
    repo.initialize()
    item = repo.get_outbox("u2")
    assert item is not None and item.status == "dead" and item.message_id is None
    repo.close()


@pytest.mark.parametrize("status", ["pending", "delivered"])
def test_resolve_rejects_non_uncertain_rows(
    tmp_path: Path, status: str, capsys: pytest.CaptureFixture[str]
) -> None:
    settings = _settings(tmp_path)
    _seed(settings, "x1", status)
    assert run_outbox_command(_resolve_args("x1"), settings) == 2
    assert "only uncertain" in capsys.readouterr().err
    repo = SqlRepository(settings.storage.url)
    repo.initialize()
    item = repo.get_outbox("x1")
    assert item is not None and item.status == status
    repo.close()


def test_resolve_rejects_unknown_event_and_blank_reason(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    settings = _settings(tmp_path)
    _seed(settings, "u3", "uncertain")
    assert run_outbox_command(_resolve_args("missing"), settings) == 2
    assert "unknown" in capsys.readouterr().err
    assert run_outbox_command(_resolve_args("u3", reason="   "), settings) == 2
    repo = SqlRepository(settings.storage.url)
    repo.initialize()
    item = repo.get_outbox("u3")
    assert item is not None and item.status == "uncertain"
    repo.close()


def test_resolve_cli_requires_reason_and_valid_resolution() -> None:
    parser = _parser()
    base = ["outbox", "resolve", "--config", "c.yaml", "--event-id", "e1"]
    with pytest.raises(SystemExit):
        parser.parse_args([*base, "--as", "delivered"])
    with pytest.raises(SystemExit):
        parser.parse_args([*base, "--as", "pending", "--reason", "x"])
    args = parser.parse_args([*base, "--as", "dead", "--reason", "x"])
    assert (args.resolution, args.reason, args.message_id) == ("dead", "x", None)


def test_resolve_repository_error_types() -> None:
    repo = _repo()
    with pytest.raises(OutboxResolveError):
        repo.resolve_uncertain_outbox("nope", "dead", "r", 1)
    with pytest.raises(ValueError):
        repo.resolve_uncertain_outbox("nope", "pending", "r", 1)
    repo.close()


def test_outbox_status_command_prints_the_same_summary_as_json(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    settings = _settings(tmp_path)
    _seed(settings, "p1", "pending")
    _seed(settings, "u1", "uncertain")
    assert run_outbox_command(argparse.Namespace(outbox_command="status"), settings) == 0
    import json

    summary = json.loads(capsys.readouterr().out)
    assert summary["counts_by_status"] == {"pending": 1, "uncertain": 1}
    assert SECRET_MARKER not in json.dumps(summary)


# --------------------------------------------------------------------------
# Drain cycle callback + settings
# --------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_dispatch_loop_calls_cycle_callback_and_isolates_callback_failures(
    caplog: pytest.LogCaptureFixture,
) -> None:
    repo = _repo()
    client = httpx.AsyncClient(transport=httpx.MockTransport(lambda r: httpx.Response(200)))
    notifier = DiscordNotifier(AlertSettings(), repo, ReplayClock(0), client)
    cycles = 0

    def callback() -> None:
        nonlocal cycles
        cycles += 1
        raise RuntimeError("callback bug")

    stop = asyncio.Event()
    try:
        with caplog.at_level(logging.ERROR):
            task = asyncio.create_task(
                notifier.run_dispatch_loop(stop, idle_seconds=0.01, on_cycle=callback)
            )
            await asyncio.sleep(0.1)
            stop.set()
            await asyncio.wait_for(task, timeout=2)
        assert cycles >= 2  # the loop survived the failing callback
        assert any("cycle callback failed" in r.getMessage() for r in caplog.records)
    finally:
        await client.aclose()
        repo.close()


def test_ready_staleness_setting_bounds_default_and_dump_exclusion() -> None:
    assert RuntimeSettings().ready_max_staleness_seconds == 120
    assert RuntimeSettings(ready_max_staleness_seconds=15).ready_max_staleness_seconds == 15
    with pytest.raises(ValueError):
        RuntimeSettings(ready_max_staleness_seconds=14)
    assert "ready_max_staleness_seconds" not in RuntimeSettings().model_dump()
