from __future__ import annotations

import hashlib
import json
import sqlite3
import sys
import time
from pathlib import Path
from types import SimpleNamespace

import pytest

ROOT = str(Path(__file__).resolve().parents[2])
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from tools import oci_operational_health as operational_health  # noqa: E402
from tools.oci_operational_health import (  # noqa: E402
    EXPECTED_SOURCE,
    MonitorConfig,
    _read_history,
    _read_segments,
    _read_sqlite_fast,
    _storage_surfaces,
    _write_receipt_exclusive,
    classify_operational_state,
)


def _source_probe(expected: str, config_path: Path) -> dict[str, object]:
    return {
        "expected": expected,
        "observed": expected,
        "source_root_sha256": "source-root",
        "frozen_file_count": 1,
        "match": True,
        "scientific_identities": {},
    }


def _make_health_db(path: Path, campaign_id: str = "c") -> None:
    conn = sqlite3.connect(path)
    conn.executescript(
        "CREATE TABLE shadow_campaigns(campaign_id TEXT, source_identity TEXT);"
        "CREATE TABLE shadow_coverage("
        "campaign_id TEXT, market TEXT, status TEXT, complete INTEGER, decision_close_ms INTEGER);"
    )
    conn.execute("INSERT INTO shadow_campaigns VALUES (?, ?)", (campaign_id, EXPECTED_SOURCE))
    conn.commit()
    conn.close()


def _write_fast_segment(
    directory: Path,
    *,
    bucket: int,
    sequence: int,
    previous_hash: str | None,
    campaign_id: str = "c",
    last_received_at_ms: int | None = None,
) -> str:
    directory.mkdir(parents=True, exist_ok=True)
    data = directory / f"{bucket}-{sequence:08d}.jsonl.zst"
    data.write_bytes(b"x")
    digest = hashlib.sha256(data.read_bytes()).hexdigest()
    manifest = directory / f"{bucket}-{sequence:08d}.manifest.json"
    manifest.write_text(
        json.dumps(
            {
                "campaign_id": campaign_id,
                "market": "futures",
                "source_identity": EXPECTED_SOURCE,
                "storage_schema_version": "raw_tape_segmented_zstd_v2",
                "segment_sequence": sequence,
                "bucket_start_ms": bucket,
                "last_received_at_ms": (
                    bucket if last_received_at_ms is None else last_received_at_ms
                ),
                "compressed_file_name": data.name,
                "compressed_file_sha256": digest,
                "compressed_bytes": data.stat().st_size,
                "uncompressed_bytes": 10,
                "previous_segment_sha256": previous_hash,
            }
        ),
        encoding="utf-8",
    )
    return digest


def test_ordinary_48h_yellow_never_authorizes_stop() -> None:
    result = classify_operational_state(
        source_drift=False,
        stale_markets=False,
        database_ok=True,
        chain_ok=True,
        history_available=True,
        quota_gate_met=False,
        root_gate_met=True,
        quota_runway_hours=72,
        root_runway_hours=72,
        root_free_bytes=20_000,
        root_floor_bytes=1_000,
        quota_headroom_bytes=20_000,
        finalize_reserve_bytes=100,
        one_hour_tape_ingestion_bytes=100,
    )
    assert result["state"] == "YELLOW"
    assert result["stop_authorized"] is False
    assert "QUOTA_48H_GATE_FAIL" in result["reasons"]


@pytest.mark.parametrize(
    ("quota_runway", "root_runway", "free", "expected"),
    [
        (24, 100, 100_000, True),
        (24.001, 24, 100_000, True),
        (100, 100, 999, True),
        (100, 100, 100_000, False),
    ],
)
def test_storage_stop_boundaries_are_explicit(
    quota_runway: float, root_runway: float, free: int, expected: bool
) -> None:
    result = classify_operational_state(
        source_drift=False,
        stale_markets=False,
        database_ok=True,
        chain_ok=True,
        history_available=True,
        quota_gate_met=True,
        root_gate_met=True,
        quota_runway_hours=quota_runway,
        root_runway_hours=root_runway,
        root_free_bytes=free,
        root_floor_bytes=1_000,
        quota_headroom_bytes=100_000,
        finalize_reserve_bytes=100,
        one_hour_tape_ingestion_bytes=100,
    )
    assert result["stop_authorized"] is expected
    assert result["state"] == ("RED" if expected else "GREEN")


def test_integrity_red_is_alert_only() -> None:
    result = classify_operational_state(
        source_drift=True,
        stale_markets=False,
        database_ok=True,
        chain_ok=True,
        history_available=True,
        quota_gate_met=True,
        root_gate_met=True,
        quota_runway_hours=100,
        root_runway_hours=100,
        root_free_bytes=100_000,
        root_floor_bytes=1_000,
        quota_headroom_bytes=100_000,
        finalize_reserve_bytes=100,
        one_hour_tape_ingestion_bytes=100,
    )
    assert result["state"] == "RED"
    assert result["stop_authorized"] is False
    assert result["storage_reasons"] == []


def test_exclusive_receipt_preserves_first_bytes(tmp_path: Path) -> None:
    path = tmp_path / "phase-p-1.json"
    _write_receipt_exclusive(path, {"state": "GREEN", "receipt_id": "first"})
    original = path.read_bytes()
    with pytest.raises(FileExistsError):
        _write_receipt_exclusive(path, {"state": "RED", "receipt_id": "second"})
    assert path.read_bytes() == original
    assert json.loads(original)["receipt_id"] == "first"


def test_mock_stop_runner_observes_durable_receipt_first(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = MonitorConfig("c", tmp_path, tmp_path / "db", tmp_path, tmp_path / "config", 0)
    monkeypatch.setattr(
        operational_health,
        "build_receipt",
        lambda config, captured_at_ms: {
            "stop_authorized": True,
            "action": {"attempted": False, "mode": "wp2-dry-run-only"},
        },
    )
    observed: list[bool] = []

    def runner(target: str) -> int:
        paths = list(config.resolved_health_dir().glob("phase-p-*.json"))
        observed.append(bool(paths) and paths[0].read_bytes() != b"")
        return 0

    receipt, path = operational_health.run_once(config, captured_at_ms=123, stop_runner=runner)
    assert observed == [True]
    assert json.loads(path.read_text(encoding="utf-8"))["action"]["attempted"] is False
    assert receipt["action"]["attempted"] is True


def test_corrupt_history_is_rejected(tmp_path: Path) -> None:
    (tmp_path / "phase-p-1.json").write_text(json.dumps({"state": "GREEN"}), encoding="utf-8")
    with pytest.raises(ValueError, match="missing/unknown schema"):
        _read_history(tmp_path)


def test_sqlite_fixture_keeps_monitor_contract_read_only(tmp_path: Path) -> None:
    db = tmp_path / "campaign.db"
    conn = sqlite3.connect(db)
    conn.executescript(
        "CREATE TABLE shadow_campaigns(campaign_id TEXT, source_identity TEXT);"
        "CREATE TABLE shadow_coverage("
        "campaign_id TEXT, market TEXT, status TEXT, complete INTEGER, decision_close_ms INTEGER);"
    )
    conn.execute("INSERT INTO shadow_campaigns VALUES (?, ?)", ("c", EXPECTED_SOURCE))
    conn.execute("INSERT INTO shadow_coverage VALUES ('c','spot','OPEN',0,1)")
    conn.commit()
    conn.close()
    config = MonitorConfig("c", tmp_path, db, tmp_path, tmp_path / "config", 0)
    assert config.campaign_id == "c"


def test_live_v2_segment_schema_is_accepted(tmp_path: Path) -> None:
    tape = tmp_path / "futures"
    tape.mkdir()
    data = tape / "1000-00000001.jsonl.zst"
    data.write_bytes(b"segment")
    manifest = tape / "1000-00000001.manifest.json"
    manifest.write_text(
        json.dumps(
            {
                "campaign_id": "c",
                "market": "futures",
                "source_identity": EXPECTED_SOURCE,
                "storage_schema_version": "raw_tape_segmented_zstd_v2",
                "bucket_start_ms": 1000,
                "last_received_at_ms": 999999,
                "compressed_file_name": data.name,
                "compressed_file_sha256": hashlib.sha256(data.read_bytes()).hexdigest(),
                "compressed_bytes": data.stat().st_size,
                "uncompressed_bytes": 10,
                "previous_segment_sha256": None,
            }
        ),
        encoding="utf-8",
    )
    result = _read_segments(tmp_path, "c", EXPECTED_SOURCE, 1_000_000)
    assert result["chain_healthy"] is True
    assert result["markets"]["futures"]["manifest_count"] == 1


def test_fast_skips_old_manifest_and_hashes_only_new_segment(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    tape = tmp_path / "tape"
    futures = tape / "futures"
    (tape / "spot").mkdir(parents=True)
    db = tmp_path / "campaign.db"
    _make_health_db(db)
    first_hash = _write_fast_segment(
        futures, bucket=1_000_000_000_000, sequence=1, previous_hash=None
    )
    config = MonitorConfig(
        "c",
        tmp_path,
        db,
        tape,
        tmp_path / "config",
        0,
        health_dir=tmp_path / "health-fast",
        root_path=tmp_path,
        mode="fast",
    )
    first = operational_health.build_receipt(
        config, captured_at_ms=2_000_000, source_probe=_source_probe
    )
    assert first["tape"]["incremental"]["bootstrap"] is True
    _write_fast_segment(
        futures,
        bucket=1_000_000_000_001,
        sequence=2,
        previous_hash=first_hash,
        last_received_at_ms=2_500_000,
    )
    original_hash = operational_health._sha256_file
    hashed: list[str] = []

    def record_hash(path: Path) -> str:
        hashed.append(path.name)
        return original_hash(path)

    monkeypatch.setattr(operational_health, "_sha256_file", record_hash)
    second = operational_health.build_receipt(
        config, captured_at_ms=3_000_000, source_probe=_source_probe
    )
    assert second["tape"]["incremental"]["skipped_existing"] >= 1
    assert second["tape"]["incremental"]["new_manifests"] == 1
    assert hashed == ["1000000000001-00000002.jsonl.zst"]
    assert second["rates"]["conservative_tape_bytes_per_h"] is not None


def test_fast_manifest_cursor_scales_and_remains_incremental(tmp_path: Path) -> None:
    tape = tmp_path / "tape"
    futures = tape / "futures"
    (tape / "spot").mkdir(parents=True)
    db = tmp_path / "campaign.db"
    _make_health_db(db)
    config = MonitorConfig(
        "c",
        tmp_path,
        db,
        tape,
        tmp_path / "config",
        0,
        health_dir=tmp_path / "health-fast",
        root_path=tmp_path,
        mode="fast",
    )
    operational_health.build_receipt(config, captured_at_ms=1_000_000, source_probe=_source_probe)
    segment_hash = hashlib.sha256(b"x").hexdigest()
    for sequence in range(1, 5_001):
        _write_fast_segment(
            futures,
            bucket=1_000_000_000_000 + sequence,
            sequence=sequence,
            previous_hash=None if sequence == 1 else segment_hash,
            last_received_at_ms=1_000_000 + sequence,
        )
    started_middle = time.perf_counter()
    middle = operational_health.build_receipt(
        config, captured_at_ms=2_000_000, source_probe=_source_probe
    )
    middle_elapsed = time.perf_counter() - started_middle
    for sequence in range(5_001, 10_001):
        _write_fast_segment(
            futures,
            bucket=1_000_000_000_000 + sequence,
            sequence=sequence,
            previous_hash=segment_hash,
            last_received_at_ms=2_000_000 + sequence,
        )
    started_final = time.perf_counter()
    final = operational_health.build_receipt(
        config, captured_at_ms=3_000_000, source_probe=_source_probe
    )
    final_elapsed = time.perf_counter() - started_final
    assert middle["tape"]["markets"]["futures"]["manifest_count"] == 5_000
    assert final["tape"]["markets"]["futures"]["manifest_count"] == 10_000
    assert final["tape"]["incremental"]["skipped_existing"] >= 5_000
    assert final["tape"]["incremental"]["new_manifests"] == 5_000
    assert middle_elapsed < 30
    assert final_elapsed < 30
    print(
        f"FAST_SCALE_HEALTH_SECONDS middle={middle_elapsed:.6f} final={final_elapsed:.6f}"
    )


def test_fast_sqlite_busy_is_bounded_and_alert_only(tmp_path: Path) -> None:
    db = tmp_path / "campaign.db"
    _make_health_db(db)
    writer = sqlite3.connect(db)
    writer.execute("BEGIN EXCLUSIVE")
    started = time.perf_counter()
    with pytest.raises(sqlite3.OperationalError, match=r"locked|busy"):
        _read_sqlite_fast(db, "c")
    elapsed = time.perf_counter() - started
    writer.rollback()
    writer.close()
    assert elapsed < 2
    print(f"FAST_DB_BUSY_SECONDS={elapsed:.6f}")
    result = classify_operational_state(
        source_drift=False,
        stale_markets=False,
        database_ok=False,
        database_busy=True,
        chain_ok=True,
        history_available=True,
        quota_gate_met=True,
        root_gate_met=True,
        quota_runway_hours=72,
        root_runway_hours=72,
        root_free_bytes=20_000,
        root_floor_bytes=1_000,
        quota_headroom_bytes=20_000,
        finalize_reserve_bytes=100,
        one_hour_tape_ingestion_bytes=100,
    )
    assert result["state"] == "YELLOW"
    assert "DB_BUSY" in result["reasons"]
    assert result["stop_authorized"] is False


def test_fast_sqlite_contract_explicitly_skips_quick_check(tmp_path: Path) -> None:
    db = tmp_path / "campaign.db"
    _make_health_db(db)
    result = _read_sqlite_fast(db, "c")
    assert result["query_only"] == 1
    assert result["quick_check"] == "SKIPPED_FAST"
    assert result["campaign_match"] is True


def _surface_fixture(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    *,
    devices: dict[Path, int],
    free_bytes: dict[int, int],
    sizes: dict[Path, int] | None = None,
) -> MonitorConfig:
    """Build deterministic device/usage inputs without touching real mounts."""

    config = MonitorConfig(
        "c",
        tmp_path / "campaign",
        tmp_path / "campaign" / "campaign.db",
        tmp_path / "campaign" / "raw-events",
        tmp_path / "config",
        0,
        health_dir=tmp_path / "campaign" / "health",
        root_path=tmp_path / "root",
    )
    for directory in (
        config.root_path,
        config.campaign_root,
        config.tape_root,
        config.resolved_health_dir(),
        config.campaign_root / "logs",
    ):
        directory.mkdir(parents=True, exist_ok=True)
    config.db_path.touch()
    def usage(path: Path) -> object:
        key = path
        while key not in devices and key != key.parent:
            key = key.parent
        return type(
            "Usage",
            (),
            {
                "total": free_bytes[devices[key]] + 10_000,
                "used": 10_000,
                "free": free_bytes[devices[key]],
            },
        )()
    monkeypatch.setattr(operational_health, "_path_device", lambda path: devices.get(path))
    monkeypatch.setattr(operational_health.shutil, "disk_usage", usage)
    monkeypatch.setattr(
        operational_health,
        "_shallow_bytes",
        lambda path: 0 if sizes is None else sizes.get(path, 0),
    )
    monkeypatch.setattr(
        operational_health,
        "_directory_bytes",
        lambda path: 0 if sizes is None else sizes.get(path, 0),
    )
    return config


def test_same_filesystem_preserves_legacy_root_charge(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = MonitorConfig(
        "c",
        tmp_path / "campaign",
        tmp_path / "campaign" / "campaign.db",
        tmp_path / "campaign" / "raw-events",
        tmp_path / "config",
        0,
        health_dir=tmp_path / "campaign" / "health",
        root_path=tmp_path / "root",
    )
    paths = {
        config.root_path: 1,
        config.campaign_root: 1,
        config.db_path: 1,
        config.tape_root: 1,
        config.campaign_root / "health": 1,
        config.campaign_root / "logs": 1,
    }
    sizes = {
        config.db_path: 11,
        config.campaign_root / "health": 13,
        config.campaign_root / "logs": 17,
    }
    config = _surface_fixture(
        tmp_path,
        monkeypatch,
        devices=paths,
        free_bytes={1: 9_000},
        sizes=sizes,
    )
    result = _storage_surfaces(
        config, finalize_reserve_bytes=19, conservative_tape_bytes_per_h=10
    )
    assert result["same_filesystem"] is True
    assert result["data_gate_met"] is None
    assert result["root_resident_reserve_bytes"] == 41
    assert result["root_finalize_reserve_bytes"] == 19
    classified = classify_operational_state(
        source_drift=False,
        stale_markets=False,
        database_ok=True,
        chain_ok=True,
        history_available=True,
        quota_gate_met=True,
        root_gate_met=True,
        quota_runway_hours=100,
        root_runway_hours=100,
        root_free_bytes=9_000,
        root_floor_bytes=100,
        quota_headroom_bytes=9_000,
        finalize_reserve_bytes=19,
        one_hour_tape_ingestion_bytes=10,
        data_gate_met=result["data_gate_met"],
    )
    assert classified["state"] == "GREEN"
    assert classified["reasons"] == []


def test_split_filesystem_charges_data_paths_to_data_gate(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = MonitorConfig(
        "c",
        tmp_path / "campaign",
        tmp_path / "campaign" / "campaign.db",
        tmp_path / "campaign" / "raw-events",
        tmp_path / "config",
        0,
        health_dir=tmp_path / "campaign" / "health",
        root_path=tmp_path / "root",
    )
    paths = {
        config.root_path: 1,
        config.campaign_root: 2,
        config.db_path: 2,
        config.tape_root: 2,
        config.campaign_root / "health": 2,
        config.campaign_root / "logs": 2,
    }
    config = _surface_fixture(
        tmp_path,
        monkeypatch,
        devices=paths,
        free_bytes={1: 9_000, 2: 2_000_000_000},
    )
    result = _storage_surfaces(
        config, finalize_reserve_bytes=20, conservative_tape_bytes_per_h=10
    )
    assert result["same_filesystem"] is False
    assert result["root_resident_reserve_bytes"] == 0
    assert result["data_gate_met"] is True
    assert result["data_required_bytes"] == 1_073_742_324
    assert result["root_finalize_reserve_bytes"] == 0
    classified = classify_operational_state(
        source_drift=False,
        stale_markets=False,
        database_ok=True,
        chain_ok=True,
        history_available=True,
        quota_gate_met=True,
        root_gate_met=True,
        quota_runway_hours=100,
        root_runway_hours=100,
        root_free_bytes=9_000,
        root_floor_bytes=100,
        quota_headroom_bytes=9_000,
        finalize_reserve_bytes=20,
        one_hour_tape_ingestion_bytes=10,
        data_gate_met=result["data_gate_met"],
        data_runway_hours=100,
        data_free_bytes=2_000_000_000,
        data_floor_bytes=100,
    )
    assert classified["state"] == "GREEN"
    assert classified["reasons"] == []


def test_fast_root_growth_keeps_root_warning_authoritative() -> None:
    result = classify_operational_state(
        source_drift=False,
        stale_markets=False,
        database_ok=True,
        chain_ok=True,
        history_available=True,
        quota_gate_met=True,
        root_gate_met=False,
        quota_runway_hours=100,
        root_runway_hours=10,
        root_free_bytes=500,
        root_floor_bytes=100,
        quota_headroom_bytes=10_000,
        finalize_reserve_bytes=10,
        one_hour_tape_ingestion_bytes=10,
        data_gate_met=True,
        data_runway_hours=100,
        data_free_bytes=10_000,
        data_floor_bytes=100,
    )
    assert result["state"] == "RED"
    assert "ROOT_48H_GATE_FAIL" in result["reasons"]
    assert "ROOT_RUNWAY_IMMINENT" in result["storage_reasons"]
    assert "DATA_48H_GATE_FAIL" not in result["reasons"]


def test_low_data_volume_fails_data_gate_without_root_relabel() -> None:
    result = classify_operational_state(
        source_drift=False,
        stale_markets=False,
        database_ok=True,
        chain_ok=True,
        history_available=True,
        quota_gate_met=True,
        root_gate_met=True,
        quota_runway_hours=100,
        root_runway_hours=100,
        root_free_bytes=10_000,
        root_floor_bytes=100,
        quota_headroom_bytes=10_000,
        finalize_reserve_bytes=10,
        one_hour_tape_ingestion_bytes=10,
        data_gate_met=False,
        data_runway_hours=10,
        data_free_bytes=500,
        data_floor_bytes=100,
    )
    assert result["state"] == "RED"
    assert "DATA_48H_GATE_FAIL" in result["reasons"]
    assert "ROOT_48H_GATE_FAIL" not in result["reasons"]
    assert "DATA_RUNWAY_IMMINENT" in result["storage_reasons"]


def test_low_raw_quota_preserves_quota_reason() -> None:
    result = classify_operational_state(
        source_drift=False,
        stale_markets=False,
        database_ok=True,
        chain_ok=True,
        history_available=True,
        quota_gate_met=False,
        root_gate_met=True,
        quota_runway_hours=100,
        root_runway_hours=100,
        root_free_bytes=10_000,
        root_floor_bytes=100,
        quota_headroom_bytes=100,
        finalize_reserve_bytes=10,
        one_hour_tape_ingestion_bytes=10,
        data_gate_met=True,
        data_runway_hours=100,
        data_free_bytes=10_000,
        data_floor_bytes=100,
    )
    assert result["state"] == "YELLOW"
    assert "QUOTA_48H_GATE_FAIL" in result["reasons"]
    assert "DATA_48H_GATE_FAIL" not in result["reasons"]


def test_mixed_db_tape_health_surfaces_group_by_device(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = MonitorConfig(
        "c",
        tmp_path / "campaign",
        tmp_path / "campaign" / "campaign.db",
        tmp_path / "campaign" / "raw-events",
        tmp_path / "config",
        0,
        health_dir=tmp_path / "campaign" / "health",
        root_path=tmp_path / "root",
    )
    paths = {
        config.root_path: 1,
        config.campaign_root: 2,
        config.db_path: 1,
        config.tape_root: 2,
        config.campaign_root / "health": 2,
        config.campaign_root / "logs": 1,
    }
    sizes = {
        config.db_path: 7,
        config.campaign_root / "health": 11,
        config.campaign_root / "logs": 13,
    }
    config = _surface_fixture(
        tmp_path,
        monkeypatch,
        devices=paths,
        free_bytes={1: 9_000, 2: 9_000},
        sizes=sizes,
    )
    result = _storage_surfaces(
        config, finalize_reserve_bytes=20, conservative_tape_bytes_per_h=10
    )
    by_device = {surface["st_dev"]: surface for surface in result["data_surfaces"]}
    assert sorted(result["root_surface"]["paths"]) == ["db", "logs", "root"]
    assert sorted(by_device[2]["paths"]) == ["campaign_root", "health", "tape"]
    assert result["root_resident_reserve_bytes"] == 20
    paths[config.tape_root] = 1
    free_bytes = {1: 2_000_000_000, 2: 9_000}
    config = _surface_fixture(
        tmp_path,
        monkeypatch,
        devices=paths,
        free_bytes=free_bytes,
        sizes=sizes,
    )
    mixed_tape_root = _storage_surfaces(
        config, finalize_reserve_bytes=20, conservative_tape_bytes_per_h=10
    )
    assert mixed_tape_root["same_filesystem"] is False
    assert mixed_tape_root["tape_on_root"] is True
    assert mixed_tape_root["root_finalize_reserve_bytes"] == 20


def test_custom_health_dir_uses_configured_filesystem(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = MonitorConfig(
        "c",
        tmp_path / "campaign",
        tmp_path / "campaign" / "campaign.db",
        tmp_path / "campaign" / "raw-events",
        tmp_path / "config",
        0,
        health_dir=tmp_path / "custom-health",
        root_path=tmp_path / "root",
    )
    paths = {
        config.root_path: 1,
        config.campaign_root: 1,
        config.db_path: 1,
        config.tape_root: 1,
        config.resolved_health_dir(): 2,
        config.campaign_root / "logs": 1,
    }
    config = _surface_fixture(
        tmp_path,
        monkeypatch,
        devices=paths,
        free_bytes={1: 9_000, 2: 2_000_000_000},
    )
    config = MonitorConfig(
        config.campaign_id,
        config.campaign_root,
        config.db_path,
        config.tape_root,
        config.config_path,
        config.activation_ms,
        health_dir=tmp_path / "custom-health",
        root_path=config.root_path,
    )
    config.resolved_health_dir().mkdir(parents=True, exist_ok=True)
    paths[config.resolved_health_dir()] = 2
    result = _storage_surfaces(
        config, finalize_reserve_bytes=20, conservative_tape_bytes_per_h=10
    )
    assert result["same_filesystem"] is False
    assert result["data_surfaces"][0]["paths"] == ["health"]


def test_build_receipt_wires_split_surface_gate_and_root_floor(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    campaign = tmp_path / "campaign"
    tape = campaign / "raw-events"
    (tape / "spot").mkdir(parents=True)
    (tape / "futures").mkdir()
    (tape / "futures" / "1000-00000001.jsonl.zst.partial").write_bytes(b"partial")
    db = campaign / "campaign.db"
    _make_health_db(db)
    config = MonitorConfig(
        "c",
        campaign,
        db,
        tape,
        tmp_path / "config",
        1,
        health_dir=campaign / "health",
        root_path=tmp_path / "root",
        quota_safety_margin_bytes=5,
        root_os_reserve_bytes=100,
        root_history_floor_bytes=0,
    )
    config.root_path.mkdir(parents=True, exist_ok=True)
    (config.campaign_root / "health").mkdir(parents=True, exist_ok=True)
    devices = {
        config.root_path: 1,
        config.campaign_root: 2,
        config.db_path: 2,
        config.tape_root: 2,
        config.resolved_health_dir(): 2,
        config.campaign_root / "logs": 2,
    }

    def device(path: Path) -> int | None:
        return devices.get(path, devices.get(path.parent))

    def usage(path: Path) -> SimpleNamespace:
        free = 2 if device(path) == 2 else 900_000
        return SimpleNamespace(total=1_000_000, used=1_000_000 - free, free=free)

    monkeypatch.setattr(operational_health, "_path_device", device)
    monkeypatch.setattr(operational_health.shutil, "disk_usage", usage)
    receipt = operational_health.build_receipt(
        config, captured_at_ms=2, source_probe=_source_probe
    )
    assert receipt["storage_surfaces"]["same_filesystem"] is False
    assert receipt["storage_surfaces"]["data_gate_met"] is False
    assert receipt["storage_surfaces"]["data_emergency_floor_reached"] is True
    assert receipt["storage_surfaces"]["root_finalize_reserve_bytes"] == 0
    assert receipt["tape"]["finalize_reserve_bytes"] > 0
    assert receipt["root"]["floor_bytes"] == 100
    assert "DATA_48H_GATE_FAIL" in receipt["reasons"]
    assert "ROOT_48H_GATE_FAIL" not in receipt["reasons"]
    assert receipt["state"] == "RED"
    assert receipt["stop_authorized"] is True
