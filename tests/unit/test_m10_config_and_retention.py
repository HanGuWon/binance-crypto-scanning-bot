from __future__ import annotations

import asyncio
import json
import logging
from pathlib import Path
from typing import Any

import pytest
import yaml

from conftest import make_candle, make_decision
from signalbot.alerts.embeds import build_discord_payload
from signalbot.cli import _parser
from signalbot.config import Settings, load_settings, unevaluated_pullback_intervals
from signalbot.domain.enums import Market
from signalbot.persistence.models import (
    AlertOutboxRow,
    AlertRow,
    CandleRow,
    RuntimeHeartbeatRow,
    SignalRow,
)
from signalbot.persistence.repository import SqlRepository
from signalbot.retention import MS_PER_DAY, prune_candles, run_prune_command

ROOT = Path(__file__).resolve().parents[2]
WEBHOOK = "https://discord.com/api/webhooks/1/test-token"


# --------------------------------------------------------------------------
# F-12: directional observation is shadow-only
# --------------------------------------------------------------------------


def _render_directional_template(tmp_path: Path, **alert_overrides: Any) -> Path:
    template = ROOT / "deployment" / "config" / "prospective-futures-bidirectional-v1.yaml.template"
    text = template.read_text(encoding="utf-8")
    for token, value in {
        "__CAMPAIGN_ID__": "futures-bidirectional-test",
        "__SOURCE_IDENTITY_HEX__": "a" * 64,
        "__CREATED_AT_MS__": "1700000000000",
        "__ACTIVATION_MS__": "1700000300000",
    }.items():
        text = text.replace(token, value)
    data = yaml.safe_load(text)
    data["alerts"].update(alert_overrides)
    path = tmp_path / "directional.yaml"
    path.write_text(yaml.safe_dump(data), encoding="utf-8")
    return path


def test_directional_template_validates_when_the_webhook_env_var_is_absent(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.delenv("SIGNALBOT_DISCORD_WEBHOOK_URL", raising=False)
    settings = load_settings(_render_directional_template(tmp_path))
    assert settings.shadow.directional_observation_enabled is True
    assert settings.alerts.discord_enabled is False


def test_directional_with_discord_enabled_in_config_is_rejected(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.delenv("SIGNALBOT_DISCORD_WEBHOOK_URL", raising=False)
    path = _render_directional_template(
        tmp_path, discord_enabled=True, discord_webhook_url=WEBHOOK
    )
    with pytest.raises(ValueError, match="directional_observation_enabled"):
        load_settings(path)


def test_webhook_env_var_auto_enable_is_named_in_the_error(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = _render_directional_template(tmp_path)
    monkeypatch.setenv("SIGNALBOT_DISCORD_WEBHOOK_URL", WEBHOOK)
    with pytest.raises(ValueError) as caught:
        load_settings(path)
    assert "SIGNALBOT_DISCORD_WEBHOOK_URL" in str(caught.value)
    assert "alerts.discord_enabled" in str(caught.value)


def test_discord_without_directional_observation_is_still_valid(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("SIGNALBOT_DISCORD_WEBHOOK_URL", raising=False)
    settings = Settings.model_validate(
        {"alerts": {"discord_enabled": True, "discord_webhook_url": WEBHOOK}}
    )
    assert settings.alerts.discord_enabled is True
    # and the example config is unaffected
    assert load_settings(ROOT / "config" / "settings.example.yaml") is not None


# --------------------------------------------------------------------------
# pullback_intervals startup warning
# --------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("mode", "intervals", "expected"),
    [
        ("off", ["5m", "15m", "1h", "4h"], []),
        ("informational", ["5m", "15m", "1h", "4h"], ["15m", "1h", "4h"]),
        ("informational", ["5m"], []),
        ("informational", ["15m"], ["15m"]),
    ],
)
def test_unevaluated_pullback_intervals(
    mode: str, intervals: list[str], expected: list[str]
) -> None:
    settings = Settings.model_validate(
        {"signals": {"pullback_alert_mode": mode, "pullback_intervals": intervals}}
    )
    assert unevaluated_pullback_intervals(settings) == expected


@pytest.mark.asyncio
@pytest.mark.parametrize(("mode", "warns"), [("informational", True), ("off", False)])
async def test_startup_logs_the_pullback_warning_once(
    mode: str,
    warns: bool,
    tmp_path: Any,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    from test_discord_dispatch_supervision import _make_app

    app = _make_app(tmp_path, monkeypatch, "wait")
    app.settings = app.settings.model_copy(
        update={
            "signals": app.settings.signals.model_copy(update={"pullback_alert_mode": mode})
        }
    )
    app.stop_after_minutes = 0
    with caplog.at_level(logging.INFO):
        await asyncio.wait_for(app.run(), timeout=15)
    logged = [r for r in caplog.records if "never evaluated live" in r.getMessage()]
    assert len(logged) == (1 if warns else 0)
    if warns:
        assert logged[0].levelno == logging.WARNING
        assert "15m" in logged[0].getMessage()


# --------------------------------------------------------------------------
# prune-candles
# --------------------------------------------------------------------------

CUTOFF_MS = 10_000_000_000  # arbitrary instant; close times are built around it
DAYS = 7
NOW_MS = CUTOFF_MS + DAYS * MS_PER_DAY


def _repo(tmp_path: Path) -> SqlRepository:
    repo = SqlRepository(f"sqlite:///{tmp_path / 'retention.db'}")
    repo.initialize()
    return repo


def _candle_closing_at(close_ms: int, *, market: Market = Market.SPOT, interval: str = "5m") -> Any:
    step = 300_000
    index = (close_ms + 1) // step - 1  # close_time_ms == (index + 1) * step - 1
    candle = make_candle(index, market=market, interval=interval, step_ms=step)
    assert candle.close_time_ms == close_ms
    return candle


def _seed(repo: SqlRepository) -> None:
    aligned = (CUTOFF_MS // 300_000) * 300_000 - 1  # a real close time at/below the cutoff
    assert aligned < CUTOFF_MS
    candles = [
        _candle_closing_at(aligned - 600_000),  # old
        _candle_closing_at(aligned - 300_000),  # old
        _candle_closing_at(aligned + 300_000 * 5),  # newer than the cutoff
        _candle_closing_at(aligned - 300_000, market=Market.FUTURES),  # old, other market
    ]
    repo.save_candles(candles)
    decision = make_decision(event_id="keep-me")
    repo.save_signal_and_enqueue(
        decision,
        build_discord_payload(decision, "Bot"),
        1,
        delivery_enabled=True,
        maximum_active_items=100,
    )
    repo.record_alert("keep-me", 1, "sent", 2)
    repo.upsert_heartbeat("spot", 3, last_ws_message_ms=3)


def _table_counts(repo: SqlRepository) -> dict[str, int]:
    from sqlalchemy import func, select
    from sqlalchemy.orm import Session

    out: dict[str, int] = {}
    with Session(repo.engine) as session:
        for model in (CandleRow, SignalRow, AlertRow, AlertOutboxRow, RuntimeHeartbeatRow):
            out[model.__tablename__] = int(
                session.scalar(select(func.count()).select_from(model)) or 0
            )
    return out


def test_dry_run_reports_counts_and_changes_nothing(tmp_path: Path) -> None:
    repo = _repo(tmp_path)
    _seed(repo)
    before = _table_counts(repo)
    report = prune_candles(repo, older_than_days=DAYS, now_ms=NOW_MS, apply=False)
    assert report["mode"] == "dry-run"
    assert report["candidates_by_market_interval"] == {"futures/5m": 1, "spot/5m": 2}
    assert report["candidate_total"] == 3 and report["deleted_total"] == 0
    assert _table_counts(repo) == before
    repo.close()


def test_apply_deletes_only_older_candles_and_leaves_other_tables(tmp_path: Path) -> None:
    repo = _repo(tmp_path)
    _seed(repo)
    before = _table_counts(repo)
    report = prune_candles(repo, older_than_days=DAYS, now_ms=NOW_MS, apply=True)
    assert report["mode"] == "apply" and report["deleted_total"] == 3
    after = _table_counts(repo)
    assert after["candles"] == before["candles"] - 3 == 1
    for table in ("signals", "alerts", "alert_outbox", "runtime_heartbeats"):
        assert after[table] == before[table], table
    # idempotent
    again = prune_candles(repo, older_than_days=DAYS, now_ms=NOW_MS, apply=True)
    assert again["deleted_total"] == 0
    repo.close()


@pytest.mark.parametrize(("offset_ms", "deleted"), [(-1, 0), (0, 0), (1, 1)])
def test_cutoff_boundary_row_is_kept(tmp_path: Path, offset_ms: int, deleted: int) -> None:
    repo = _repo(tmp_path)
    close_ms = 300_000 * 40_000 - 1
    repo.save_candles([_candle_closing_at(close_ms)])
    now_ms = close_ms + offset_ms + DAYS * MS_PER_DAY  # cutoff = close_ms + offset
    report = prune_candles(repo, older_than_days=DAYS, now_ms=now_ms, apply=True)
    assert report["deleted_total"] == deleted
    repo.close()


def test_apply_deletes_in_bounded_batches(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    repo = _repo(tmp_path)
    base = 300_000 * 40_000
    repo.save_candles([_candle_closing_at(base + index * 300_000 - 1) for index in range(5)])
    batch_sizes: list[int] = []
    original = repo.delete_candles_before

    def spy(cutoff_ms: int, limit: int) -> int:
        deleted = original(cutoff_ms, limit)
        batch_sizes.append(deleted)
        return deleted

    monkeypatch.setattr(repo, "delete_candles_before", spy)
    far_future = base + 100 * 300_000 + DAYS * MS_PER_DAY
    report = prune_candles(repo, older_than_days=DAYS, now_ms=far_future, apply=True, batch_size=2)
    assert batch_sizes == [2, 2, 1]
    assert report["deleted_total"] == 5 and report["batches"] == 3
    repo.close()


def test_batch_size_and_days_are_validated(tmp_path: Path) -> None:
    repo = _repo(tmp_path)
    with pytest.raises(ValueError):
        prune_candles(repo, older_than_days=0, now_ms=NOW_MS, apply=False)
    with pytest.raises(ValueError):
        prune_candles(repo, older_than_days=1, now_ms=NOW_MS, apply=False, batch_size=0)
    with pytest.raises(ValueError):
        repo.delete_candles_before(1, 0)
    repo.close()


def test_cli_parsing_and_refusal(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    parser = _parser()
    args = parser.parse_args(["prune-candles", "--config", "c.yaml", "--older-than-days", "30"])
    assert (args.older_than_days, args.apply) == (30, False)
    assert parser.parse_args(
        ["prune-candles", "--config", "c.yaml", "--older-than-days", "30", "--apply"]
    ).apply is True
    with pytest.raises(SystemExit):
        parser.parse_args(["prune-candles", "--config", "c.yaml"])
    settings = Settings.model_validate({"storage": {"url": f"sqlite:///{tmp_path / 'cli.db'}"}})
    refused = parser.parse_args(["prune-candles", "--config", "c.yaml", "--older-than-days", "0"])
    assert run_prune_command(refused, settings) == 2
    assert "at least 1" in capsys.readouterr().err


def test_cli_dry_run_prints_json_and_applies_only_with_the_flag(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    settings = Settings.model_validate({"storage": {"url": f"sqlite:///{tmp_path / 'cli.db'}"}})
    repo = SqlRepository(settings.storage.url)
    repo.initialize()
    repo.save_candles([_candle_closing_at(300_000 * 100 - 1)])  # epoch-era: very old
    repo.close()
    parser = _parser()
    dry = parser.parse_args(["prune-candles", "--config", "c.yaml", "--older-than-days", "1"])
    assert run_prune_command(dry, settings) == 0
    assert json.loads(capsys.readouterr().out)["candidate_total"] == 1
    check = SqlRepository(settings.storage.url)
    check.initialize()
    assert check.count_candles_before(10**15) == {("spot", "5m"): 1}  # nothing deleted
    check.close()
    live = parser.parse_args(
        ["prune-candles", "--config", "c.yaml", "--older-than-days", "1", "--apply"]
    )
    assert run_prune_command(live, settings) == 0
    assert json.loads(capsys.readouterr().out)["deleted_total"] == 1
