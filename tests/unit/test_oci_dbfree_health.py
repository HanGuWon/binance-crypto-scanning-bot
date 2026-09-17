# ruff: noqa
from __future__ import annotations

# ruff: noqa: E501
import argparse
import hashlib
import json
from collections.abc import Sequence
from pathlib import Path

from tools.oci_dbfree_health import (
    CommandRunner,
    _atomic_json,
    _growth,
    _load_cursor,
    build_receipt,
    classify,
    inspect_market,
    inspect_storage,
    parse_args,
    publish_receipt,
    read_collector_state,
    read_journal,
)


class FakeRunner(CommandRunner):
    def __init__(self, journal: str = "") -> None:
        self.journal = journal

    def run(self, args: Sequence[str]) -> str:
        if args[0] == "systemctl":
            return "ActiveState=active\nSubState=running\nMainPID=42\nNRestarts=3\nExecMainStartTimestamp=now\nResult=success\n"
        return self.journal


def _manifest(root: Path, *, market: str = "spot", name: str = "100-0001") -> Path:
    directory = root / market
    directory.mkdir(parents=True, exist_ok=True)
    segment = directory / f"{name}.jsonl.zst"
    segment.write_bytes(b"payload")
    payload = {
        "campaign_id": "camp",
        "source_identity": "src",
        "compressed_file_name": segment.name,
        "compressed_bytes": segment.stat().st_size,
        "compressed_file_sha256": hashlib.sha256(segment.read_bytes()).hexdigest(),
        "last_received_at_ms": 1_000,
    }
    manifest = directory / f"{name}.manifest.json"
    manifest.write_text(json.dumps(payload), encoding="utf-8")
    return manifest


def test_collector_state_and_journal_detection() -> None:
    state = read_collector_state("collector.service", FakeRunner(), 2)
    assert state.main_pid == 42 and state.restart_delta == 1
    journal = read_journal("collector.service", FakeRunner("database is locked\nOut of memory\nNo space left on device\nI/O error"), None, 2_000)
    assert journal["database_locked_count"] == 1
    assert journal["oom_count"] == 1 and journal["enospc_count"] == 1 and journal["io_error_count"] == 1


def test_manifest_validation_and_missing_market(tmp_path: Path) -> None:
    tape = tmp_path / "raw"
    _manifest(tape)
    result, observations = inspect_market("spot", tape, "camp", "src", 2_000, None, True)
    assert result["healthy"] and result["new_manifests"] == 1
    assert observations[0].actual_hash == observations[0].declared_hash
    missing, _ = inspect_market("futures", tape, "camp", "src", 2_000, None, False)
    assert missing["latest_manifest_path"] is None and not missing["actively_advancing"]


def test_manifest_mismatch_and_incremental_cursor(tmp_path: Path) -> None:
    tape = tmp_path / "raw"
    manifest = _manifest(tape)
    payload = json.loads(manifest.read_text(encoding="utf-8"))
    payload["campaign_id"] = "wrong"
    manifest.write_text(json.dumps(payload), encoding="utf-8")
    result, _ = inspect_market("spot", tape, "camp", "src", 2_000, manifest.name, True)
    assert result["campaign_mismatches"] == 0
    result, _ = inspect_market("spot", tape, "camp", "src", 2_000, None, True)
    assert result["campaign_mismatches"] == 1 and not result["healthy"]


def test_growth_returns_null_without_history_and_rate_with_history() -> None:
    empty = _growth({}, 3_600_000, 100, 1_000)
    assert empty["conservative_bytes_per_hour"] is None
    prior = {"observations": [{"captured_at_ms": 0, "physical_bytes": 0}]}
    measured = _growth(prior, 3_600_000, 100, 1_000)
    assert measured["sample_count"] == 1 and measured["conservative_bytes_per_hour"] == 100
    assert measured["projected_free_48h_bytes"] == -3800


def test_receipt_is_exclusive_and_hash_chained(tmp_path: Path) -> None:
    first = {"schema": "x", "captured_at_ms": 1, "receipt_sha256": None}
    path = publish_receipt(tmp_path, "fast", first)
    assert path.exists() and first["receipt_sha256"]
    try:
        publish_receipt(tmp_path, "fast", {"schema": "x", "captured_at_ms": 1, "receipt_sha256": None})
    except Exception as exc:
        assert "already exists" in str(exc)
    else:
        raise AssertionError("overwrite was permitted")


def test_cursor_recovery_and_storage(tmp_path: Path) -> None:
    cursor = tmp_path / "cursor-v1.json"
    cursor.write_text("not json", encoding="utf-8")
    assert _load_cursor(cursor)["previous_capture_ms"] is None
    stats = inspect_storage(tmp_path, tmp_path, tmp_path)
    assert "same_filesystem" in stats and stats["data_total_bytes"] is not None








def test_cli_has_no_database_option() -> None:
    args = parse_args([
        "--mode", "fast", "--campaign-id", "c", "--campaign-root", ".",
        "--tape-root", ".", "--config", "pyproject.toml", "--expected-source", "s",
        "--health-dir", "health/phase-fast-v2", "--collector-unit", "collector.service",
        "--data-mount", ".",
    ])
    assert args.mode == "fast"


def test_classification_inactive_and_stale() -> None:
    state = read_collector_state("collector.service", FakeRunner(), 3)
    markets = {"spot": {"freshness_age_ms": 0}, "futures": {"freshness_age_ms": 0}}
    storage = {}
    journal = {"oom_count": 0, "enospc_count": 0, "io_error_count": 0, "collector_unexpected_exit_count": 0, "raw_recorder_fatal_count": 0}
    integrity = {"healthy": True}
    growth = {"conservative_bytes_per_hour": 1, "projected_free_48h_bytes": 100}
    growth["runway_hours"] = 72
    assert classify(state, markets, storage, journal, integrity, growth, "fast")["state"] == "GREEN"
    assert classify(state.__class__(state.unit, "inactive", "dead", state.main_pid, state.n_restarts, 0, state.exec_main_start_timestamp, state.result), markets, storage, journal, integrity, growth, "fast")["state"] == "RED"
    markets["spot"]["freshness_age_ms"] = 10_000_000
    assert classify(state, markets, storage, journal, integrity, growth, "fast")["state"] == "YELLOW"


def test_size_hash_missing_and_predecessor_failures(tmp_path: Path) -> None:
    tape = tmp_path / "raw"
    manifest = _manifest(tape)
    payload = json.loads(manifest.read_text(encoding="utf-8"))
    payload["compressed_bytes"] = 999
    payload["compressed_file_sha256"] = "0" * 64
    payload["predecessor"] = "not-the-previous-manifest"
    manifest.write_text(json.dumps(payload), encoding="utf-8")
    result, _ = inspect_market("spot", tape, "camp", "src", 2_000, None, True)
    assert result["size_mismatches"] == 1 and result["hash_mismatches"] == 1


def test_fast_bootstrap_is_bounded(tmp_path: Path) -> None:
    tape = tmp_path / "raw"
    for index in range(140):
        _manifest(tape, name=f"{index:04d}-0001")
    result, observations = inspect_market("spot", tape, "camp", "src", 2_000, None, False)
    assert result["new_manifests"] == 128
    assert all(item.actual_hash is not None for item in observations)


def test_atomic_cursor_write(tmp_path: Path) -> None:
    path = tmp_path / "health" / "cursor-v1.json"
    _atomic_json(path, {"previous_capture_ms": 10})
    assert json.loads(path.read_text(encoding="utf-8"))["previous_capture_ms"] == 10
    assert not list(path.parent.glob(".*cursor-v1.json.*"))



def test_build_receipt_end_to_end(tmp_path: Path) -> None:
    tape = tmp_path / "raw"
    for market in ("spot", "futures"):
        _manifest(tape, market=market)
    config = tmp_path / "config.yaml"
    config.write_text("mode: public", encoding="utf-8")
    args = argparse.Namespace(
        mode="fast",
        captured_at_ms=2_000,
        campaign_root=tmp_path,
        tape_root=tape,
        config=config,
        campaign_id="camp",
        expected_source="src",
        health_dir=tmp_path / "health/phase-fast-v2",
        collector_unit="collector.service",
        data_mount=tmp_path,
    )
    receipt, path, cursor = build_receipt(args, FakeRunner())
    assert receipt["schema"] == "oci_dbfree_health_receipt_v2"
    assert path.exists() and cursor.exists()




def test_source_identity_is_observed_not_cli_asserted(tmp_path: Path) -> None:
    tape = tmp_path / "raw"; manifest = _manifest(tape); payload = json.loads(manifest.read_text()); payload["source_identity"] = "different"; manifest.write_text(json.dumps(payload))
    result, _ = inspect_market("spot", tape, "camp", "src", 2_000, None, False)
    assert result["observed_source_identity"] == "different" and result["all_new_source_match"] is False


def test_actively_advancing_requires_cursor_movement(tmp_path: Path) -> None:
    tape = tmp_path / "raw"; manifest = _manifest(tape)
    result, _ = inspect_market("spot", tape, "camp", "src", 2_000, manifest.name, False, cursor_summary={"markets": {"spot": {"last_manifest": manifest.name, "latest_received_at_ms": 1_000}}, "partial_sizes": {"spot": 0}})
    assert result["actively_advancing"] is False


def test_real_predecessor_hash_and_sequence_are_checked(tmp_path: Path) -> None:
    tape = tmp_path / "raw"; first = _manifest(tape, name="0001-0001"); first_payload = json.loads(first.read_text()); first_hash = first_payload["compressed_file_sha256"]
    second = _manifest(tape, name="0002-0001"); second_payload = json.loads(second.read_text()); second_payload["previous_segment_sha256"] = first_hash; second_payload["segment_sequence"] = 2; second.write_text(json.dumps(second_payload))
    result, _ = inspect_market("spot", tape, "camp", "src", 2_000, None, True)
    assert result["predecessor_breaks"] == 0


def test_deep_hashing_is_bounded(tmp_path: Path, monkeypatch) -> None:
    tape = tmp_path / "raw"
    for index in range(20): _manifest(tape, name=f"{index:04d}-0001")
    import tools.oci_dbfree_health as health
    calls = []
    original = health._sha256_file
    monkeypatch.setattr(health, "_sha256_file", lambda path: (calls.append(path), original(path))[1])
    inspect_market("spot", tape, "camp", "src", 2_000, None, True, max_historical_segments=3, max_historical_bytes=100)
    assert len(calls) <= 3
