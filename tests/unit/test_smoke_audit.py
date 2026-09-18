from __future__ import annotations

import json
from decimal import Decimal
from pathlib import Path

from signalbot.clock import ReplayClock
from signalbot.config import Settings
from signalbot.domain.models import ObservedBboSnapshot
from signalbot.persistence.repository import SqlRepository
from signalbot.prospective.observer import ShadowObserver
from signalbot.prospective.smoke_audit import build_smoke_audit, build_smoke_audit_v2
from signalbot.prospective.source_freeze import freeze_source
from signalbot.signals.rules import SignalRuleEngine


def _settings() -> Settings:
    source_identity = freeze_source(Path(__file__).parents[2]).source_identity
    return Settings.model_validate(
        {
            "binance": {
                "markets": ["spot"],
                "top_n": 1,
                "surveillance_n": 1,
                "intervals": ["5m", "15m", "1h"],
                "primary_interval": "5m",
            },
            "signals": {
                "entry_policy": "r2_pit_htf_exec",
                "confirmation_mode": "explicit_trigger",
                "gate_enabled": True,
            },
            "storage": {"url": "sqlite:///:memory:"},
            "runtime": {
                "persist_candles": True,
                "record_raw_events": True,
                "raw_event_directory": "./var/smoke-audit-test",
            },
            "alerts": {"discord_enabled": False},
            "shadow": {
                "observation_enabled": True,
                "observation_schema_version": "shadow_observation_v2",
                "campaign_mode": "smoke",
                "campaign_id": "smoke-audit-test",
                "source_identity": source_identity,
                "campaign_created_at_ms": 1,
                "retest_observation_enabled": True,
                "retest_horizon_bars": 72,
            },
        }
    )


def test_smoke_audit_is_read_only_and_hashes_canonical_report() -> None:
    settings = _settings()
    repository = SqlRepository("sqlite:///:memory:")
    repository.initialize()
    observer = ShadowObserver(
        settings,
        SignalRuleEngine(settings.signals, settings.shadow),
        repository,
        clock=ReplayClock(1),
    )
    before = repository.get_shadow_campaign("smoke-audit-test")
    assert before is not None
    report = build_smoke_audit(
        settings,
        repository,
        campaign_id="smoke-audit-test",
        generated_at_ms=123,
    )
    after = repository.get_shadow_campaign("smoke-audit-test")

    assert observer.campaign_manifest_sha256 == before["manifest_sha256"]
    assert before == after
    assert report["audit_schema_version"] == "causal_retest_smoke_audit_v1"
    assert report["integrity"]["pass"] is True
    assert report["coverage"]["total_cells"] == 0
    assert report["retest"]["raw_c0_opportunities"] == 0
    assert len(report["report_sha256"]) == 64
    # An empty freshly-registered campaign is never a successful smoke.
    assert report["qualification"]["infra_smoke"] == "NOT_RUN"
    assert report["qualification"]["integrity_p0_present"] is False


def test_smoke_audit_bbo_comparison_is_not_hidden_by_float_conversion() -> None:
    """Decimal source evidence with float-unsafe representations must still be
    compared exactly; naive float equality collapses distinct values."""

    from signalbot.prospective.smoke_audit import _decimal_equal

    left = "100000000000000000001"
    right = 100000000000000000000.0  # float rounds to the same binary value
    assert float(left) == right
    snapshot = ObservedBboSnapshot(
        bid_price=Decimal(left),
        bid_quantity=Decimal("1"),
        ask_price=Decimal(left),
        ask_quantity=Decimal("1"),
        age_ms=1,
    )
    assert not _decimal_equal(left, right)
    assert _decimal_equal(snapshot.ask_price, left)


def test_smoke_audit_reports_raw_tape_statistics(tmp_path: Path) -> None:
    settings = _settings()
    repository = SqlRepository("sqlite:///:memory:")
    repository.initialize()
    observer = ShadowObserver(
        settings,
        SignalRuleEngine(settings.signals, settings.shadow),
        repository,
        clock=ReplayClock(1),
    )
    assert observer.campaign_manifest_sha256
    tape_dir = tmp_path / "raw-events"
    (tape_dir / "spot").mkdir(parents=True)
    lines = [
        '{"market":"spot","received_at_ms":1000,"payload":{"stream":"s","data":{}}}',
        '{"market":"spot","received_at_ms":900,"payload":{"stream":"s","data":{}}}',
        "{not json",
        '{"market":"futures","received_at_ms":1100,"payload":{"stream":"s","data":{}}}',
    ]
    (tape_dir / "spot" / "2026-08-21.jsonl").write_text(
        "\n".join(lines) + "\n", encoding="utf-8"
    )
    report = build_smoke_audit(
        settings,
        repository,
        campaign_id="smoke-audit-test",
        raw_event_directory=tape_dir,
        generated_at_ms=123,
    )
    tape = report["raw_tape"]
    assert tape["files"] == 1
    assert tape["total_records"] == 4
    assert tape["records_by_market"] == {"spot": 2}
    assert tape["malformed_records"] == 1
    assert tape["market_mismatches"] == 1
    assert tape["backwards_receipt_clocks"] == 1
    assert tape["first_received_at_ms"] == 900
    # Market-mismatched records are excluded from receipt-clock statistics.
    assert tape["last_received_at_ms"] == 1000


def test_smoke_audit_v2_hard_gates_fail_on_corrupt_segment(tmp_path: Path) -> None:
    settings = _settings()
    repository = SqlRepository("sqlite:///:memory:")
    repository.initialize()
    observer = ShadowObserver(
        settings,
        SignalRuleEngine(settings.signals, settings.shadow),
        repository,
        clock=ReplayClock(1),
    )
    assert observer.campaign_manifest_sha256
    from signalbot.domain.enums import Market as MarketEnum
    from signalbot.prospective.segmented_storage import (
        ProspectiveSegmentedWriter,
    )
    source_identity = settings.shadow.source_identity
    if not source_identity:
        raise ValueError("test requires a concrete source_identity")
    market_dir = tmp_path / "raw-events" / "spot"
    market_dir.mkdir(parents=True)
    writer = ProspectiveSegmentedWriter(
        market_dir,
        campaign_id="smoke-audit-test",
        market=MarketEnum.SPOT,
        source_identity=source_identity,
        rotation_interval_ms=60_000,
    )
    for i in range(3):
        ts = 1_710_000_000_000 + i * 1_000
        writer.append(json.dumps({"market": "spot", "received_at_ms": ts, "payload": {"i": i}}), ts)
    writer.close()
    segs = sorted(market_dir.glob("*.jsonl.zst"))
    corrupted = bytearray(segs[0].read_bytes())
    corrupted[-1] ^= 0xFF
    segs[0].write_bytes(bytes(corrupted))
    report = build_smoke_audit_v2(
        settings,
        repository,
        campaign_id="smoke-audit-test",
        raw_event_directory=tmp_path / "raw-events",
        generated_at_ms=123,
    )
    assert report["audit_schema_version"] == "causal_retest_smoke_audit_v2"
    assert report["raw_tape_hard_gates"]["pass"] is False
    assert report["qualification"]["infra_smoke"] == "FAIL"


def test_smoke_audit_v2_passes_clean_tape(tmp_path: Path) -> None:
    settings = _settings()
    repository = SqlRepository("sqlite:///:memory:")
    repository.initialize()
    ShadowObserver(
        settings,
        SignalRuleEngine(settings.signals, settings.shadow),
        repository,
        clock=ReplayClock(1),
    )
    market_dir = tmp_path / "raw-events" / "spot"
    market_dir.mkdir(parents=True)
    (market_dir / "2026-08-26.jsonl").write_text(
        json.dumps({"market": "spot", "received_at_ms": 1000, "payload": {}}) + "\n",
        encoding="utf-8",
    )
    report = build_smoke_audit_v2(
        settings,
        repository,
        campaign_id="smoke-audit-test",
        raw_event_directory=tmp_path / "raw-events",
        generated_at_ms=123,
    )
    assert report["raw_tape_hard_gates"]["pass"] is True
    assert report["raw_tape_hard_gates"]["markets_streamed"] == {"spot": 1}
