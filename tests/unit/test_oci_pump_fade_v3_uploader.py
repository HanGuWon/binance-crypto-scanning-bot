from __future__ import annotations

import hashlib
import importlib.util
import json
import os
import sys
import tarfile
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

MODULE_PATH = (
    Path(__file__).resolve().parents[2] / "scripts" / "oci_capture" / "pump_fade_v3_uploader.py"
)
SPEC = importlib.util.spec_from_file_location("pump_fade_v3_uploader", MODULE_PATH)
assert SPEC is not None and SPEC.loader is not None
UPLOADER = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = UPLOADER
SPEC.loader.exec_module(UPLOADER)


def _generation(
    root: Path,
    *,
    generation_id: str = "g0001",
    terminal_at_ms: int | None = None,
) -> Path:
    terminal_at_ms = terminal_at_ms or int(datetime(2026, 10, 1, 12, tzinfo=UTC).timestamp() * 1000)
    name = f"capture-0123456789abcdef0123456789abcdef-{generation_id}"
    directory = root / name
    directory.mkdir(parents=True)
    start_bytes = b'{"schema":"pump_fade_v3_generation_receipt_v1"}\n'
    (directory / "generation-receipt.json").write_bytes(start_bytes)
    data = b"recorded public market data"
    data_name = "1791443400000-00000001.jsonl.zst"
    (directory / data_name).write_bytes(data)
    (directory / f"{data_name}.manifest.json").write_text(
        json.dumps(
            {
                "data_file": data_name,
                "sha256": hashlib.sha256(data).hexdigest(),
                "last_received_at_ms": terminal_at_ms,
            }
        ),
        encoding="utf-8",
    )
    terminal = {
        "schema": "pump_fade_v3_generation_terminal_v1",
        "status": "CLEAN_CLOSED",
        "recovered_crash": False,
        "archive_clean": True,
        "generation_directory": name,
        "start_sha256": hashlib.sha256(start_bytes).hexdigest(),
        "plan_sha256": "a" * 64,
        "process_boot_id": "test-boot",
        "terminal_at_ms": terminal_at_ms,
        "compressed_bytes_by_stream": {"!forceOrder@arr": len(data)},
    }
    (directory / "generation-terminal.json").write_text(json.dumps(terminal), encoding="utf-8")
    return directory


def _config(tmp_path: Path, *, keep_days: int = 7) -> UPLOADER.UploadConfig:
    capture_root = tmp_path / "capture"
    capture_root.mkdir(exist_ok=True)
    return UPLOADER.UploadConfig(
        capture_root=capture_root,
        receipt_root=tmp_path / "receipts",
        rclone_config=tmp_path / "rclone.conf",
        remote="ocidrive",
        host="trendguard-osaka-a1",
        prune_enabled=False,
        keep_days=keep_days,
    )


def test_daily_manifest_binds_clean_generation_inventory_and_hashes(
    tmp_path: Path,
) -> None:
    first = _generation(tmp_path, generation_id="g0001")
    second = _generation(tmp_path, generation_id="g0002")
    raw = UPLOADER.build_daily_manifest(
        [second, first], utc_date="2026-10-01", host="trendguard-osaka-a1"
    )
    manifest = json.loads(raw)

    assert manifest["schema"] == "pump_fade_v3_drive_daily_manifest_v1"
    assert manifest["generation_count"] == 2
    assert manifest["file_count"] == 8
    assert len(manifest["generations"][0]["files"]) == 4
    assert all(
        UPLOADER.SHA256_RE.fullmatch(entry["sha256"])
        for generation in manifest["generations"]
        for entry in generation["files"]
    )


@pytest.mark.parametrize(
    ("field", "value"),
    [("status", "RECOVERED_CRASH_INCOMPLETE"), ("recovered_crash", True)],
)
def test_daily_manifest_rejects_non_clean_generation(
    tmp_path: Path, field: str, value: object
) -> None:
    generation = _generation(tmp_path)
    terminal_path = generation / "generation-terminal.json"
    terminal: dict[str, Any] = json.loads(terminal_path.read_text(encoding="utf-8"))
    terminal[field] = value
    terminal_path.write_text(json.dumps(terminal), encoding="utf-8")

    with pytest.raises(UPLOADER.UploaderError):
        UPLOADER.build_daily_manifest(
            [generation], utc_date="2026-10-01", host="trendguard-osaka-a1"
        )


def test_daily_manifest_rejects_generation_outside_utc_date(tmp_path: Path) -> None:
    generation = _generation(tmp_path)
    with pytest.raises(UPLOADER.UploaderError, match="outside"):
        UPLOADER.build_daily_manifest(
            [generation], utc_date="2026-10-02", host="trendguard-osaka-a1"
        )


def test_daily_tar_is_uncompressed_and_contains_generation_inventory(
    tmp_path: Path,
) -> None:
    config = _config(tmp_path)
    generation = _generation(config.capture_root)
    manifest = UPLOADER.build_daily_manifest([generation], utc_date="2026-10-01", host=config.host)
    tar_path = UPLOADER._create_daily_tar(config, utc_date="2026-10-01", manifest=manifest)
    second_tar_path: Path | None = None
    try:
        with tarfile.open(tar_path, mode="r:") as archive:
            assert set(archive.getnames()) == {
                f"{generation.name}/generation-receipt.json",
                f"{generation.name}/generation-terminal.json",
                f"{generation.name}/1791443400000-00000001.jsonl.zst",
                f"{generation.name}/1791443400000-00000001.jsonl.zst.manifest.json",
            }
        source = generation / "1791443400000-00000001.jsonl.zst"
        os.utime(source, (1_800_000_000, 1_800_000_000))
        source.chmod(0o600)
        second_tar_path = UPLOADER._create_daily_tar(
            config, utc_date="2026-10-01", manifest=manifest
        )
        assert second_tar_path.read_bytes() == tar_path.read_bytes()
    finally:
        tar_path.unlink()
        if second_tar_path is not None:
            second_tar_path.unlink()


def test_daily_upload_verifies_tar_manifest_and_remote_size(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = _config(tmp_path)
    generation = _generation(config.capture_root)
    observed: list[tuple[str, ...]] = []
    uploaded_tar_size = 0

    def fake_rclone(_config: UPLOADER.UploadConfig, *args: str, **_kwargs: object) -> bytes:
        nonlocal uploaded_tar_size
        observed.append(args)
        if args[0] == "copyto" and args[2].endswith(".tar"):
            uploaded_tar_size = Path(args[1]).stat().st_size
            return b""
        if args[0] == "cat":
            return Path(config.receipt_root / "daily" / "2026-10-01.manifest.json").read_bytes()
        if args[:2] == ("lsjson", "--stat"):
            return json.dumps({"Size": uploaded_tar_size}).encode()
        return b""

    monkeypatch.setattr(UPLOADER, "_rclone", fake_rclone)

    manifest, receipt = UPLOADER._upload_daily_bundle(
        config, "2026-10-01", [generation]
    )

    assert receipt["verified_on_drive"] is True
    assert receipt["rclone_check"] == "PASS"
    assert receipt["tar_size_bytes"] == uploaded_tar_size
    assert receipt["manifest_sha256"] == hashlib.sha256(
        (config.receipt_root / "daily" / "2026-10-01.manifest.json").read_bytes()
    ).hexdigest()
    check = next(call for call in observed if call[0] == "check")
    assert "--checksum" in check and "--one-way" in check
    assert manifest["generation_count"] == 1
    assert len([call for call in observed if call[0] == "copyto"]) == 3


def test_prune_requires_a_matching_verified_daily_receipt(tmp_path: Path) -> None:
    config = _config(tmp_path)
    old_ms = int(datetime(2026, 10, 1, tzinfo=UTC).timestamp() * 1000)
    generation = _generation(config.capture_root, terminal_at_ms=old_ms)
    manifest = json.loads(
        UPLOADER.build_daily_manifest([generation], utc_date="2026-10-01", host=config.host)
    )
    receipt: dict[str, object] = {
        "schema": "pump_fade_v3_drive_daily_receipt_v1",
        "utc_date": "2026-10-01",
        "host": config.host,
        "remote_tar": UPLOADER._remote_daily_object(config, "2026-10-01", "tar"),
        "remote_manifest": UPLOADER._remote_daily_object(config, "2026-10-01", "manifest.json"),
        "remote_receipt": UPLOADER._remote_daily_object(config, "2026-10-01", "receipt.json"),
        "verified_on_drive": False,
        "rclone_check": "FAIL",
        "manifest_sha256": hashlib.sha256(UPLOADER._canonical_json(manifest)).hexdigest(),
    }
    with pytest.raises(UPLOADER.UploaderError):
        UPLOADER._safe_prune(
            config,
            generation,
            manifest,
            receipt,
            now_ms=int(datetime(2026, 10, 10, tzinfo=UTC).timestamp() * 1000),
        )
    assert generation.exists()


def test_prune_removes_only_old_generation_with_matching_verified_receipt(
    tmp_path: Path,
) -> None:
    config = _config(tmp_path)
    old_ms = int(datetime(2026, 10, 1, tzinfo=UTC).timestamp() * 1000)
    generation = _generation(config.capture_root, terminal_at_ms=old_ms)
    manifest = json.loads(
        UPLOADER.build_daily_manifest([generation], utc_date="2026-10-01", host=config.host)
    )
    receipt = {
        "schema": "pump_fade_v3_drive_daily_receipt_v1",
        "utc_date": "2026-10-01",
        "host": config.host,
        "remote_tar": UPLOADER._remote_daily_object(config, "2026-10-01", "tar"),
        "remote_manifest": UPLOADER._remote_daily_object(config, "2026-10-01", "manifest.json"),
        "remote_receipt": UPLOADER._remote_daily_object(config, "2026-10-01", "receipt.json"),
        "verified_on_drive": True,
        "rclone_check": "PASS",
        "manifest_sha256": hashlib.sha256(UPLOADER._canonical_json(manifest)).hexdigest(),
    }
    pruned = UPLOADER._safe_prune(
        config,
        generation,
        manifest,
        receipt,
        now_ms=int(datetime(2026, 10, 10, tzinfo=UTC).timestamp() * 1000),
    )
    assert pruned is True
    assert not generation.exists()


def test_prune_keeps_generation_within_retention_window(tmp_path: Path) -> None:
    config = _config(tmp_path)
    recent_ms = int(datetime(2026, 10, 5, tzinfo=UTC).timestamp() * 1000)
    generation = _generation(config.capture_root, terminal_at_ms=recent_ms)
    manifest = json.loads(
        UPLOADER.build_daily_manifest([generation], utc_date="2026-10-05", host=config.host)
    )
    assert not UPLOADER._safe_prune(
        config,
        generation,
        manifest,
        {},
        now_ms=int(datetime(2026, 10, 10, tzinfo=UTC).timestamp() * 1000),
    )
    assert generation.exists()


def test_rclone_always_sets_tpslimit_and_retries_rate_limits(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = _config(tmp_path)
    observed: list[list[str]] = []
    results = iter(
        [
            SimpleNamespace(returncode=1, stderr=b"rateLimitExceeded", stdout=b""),
            SimpleNamespace(returncode=0, stderr=b"", stdout=b"OK"),
        ]
    )
    monkeypatch.setattr(
        UPLOADER.subprocess,
        "run",
        lambda command, **_: observed.append(command) or next(results),
    )
    delays: list[int] = []
    monkeypatch.setattr(UPLOADER.time, "sleep", delays.append)

    assert UPLOADER._rclone(config, "lsd", "ocidrive:") == b"OK"
    assert len(observed) == 2
    for command in observed:
        position = command.index("--tpslimit")
        assert command[position + 1] == "2"
    assert delays == [2]


def test_rclone_raises_critical_after_three_consecutive_rate_limit_failures(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    config = _config(tmp_path)
    monkeypatch.setattr(
        UPLOADER.subprocess,
        "run",
        lambda *_args, **_kwargs: SimpleNamespace(
            returncode=1, stderr=b"rateLimitExceeded", stdout=b""
        ),
    )
    monkeypatch.setattr(UPLOADER.time, "sleep", lambda _: None)

    with pytest.raises(UPLOADER.UploaderError, match="retries exhausted"):
        UPLOADER._rclone(config, "copyto", "source", "remote")
    assert "CRITICAL" in caplog.text
    assert "source data retained" in caplog.text
