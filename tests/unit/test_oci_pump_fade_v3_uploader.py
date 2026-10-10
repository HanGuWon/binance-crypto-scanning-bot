from __future__ import annotations

import hashlib
import importlib.util
import json
import os
import sys
import time
from pathlib import Path
from typing import Any

import pytest

MODULE_PATH = (
    Path(__file__).resolve().parents[2]
    / "scripts"
    / "oci_capture"
    / "pump_fade_v3_uploader.py"
)
SPEC = importlib.util.spec_from_file_location("pump_fade_v3_uploader", MODULE_PATH)
assert SPEC is not None and SPEC.loader is not None
UPLOADER = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = UPLOADER
SPEC.loader.exec_module(UPLOADER)


def _generation(tmp_path: Path, *, age_days: int = 1) -> Path:
    name = "capture-0123456789abcdef0123456789abcdef-g0001"
    directory = tmp_path / name
    directory.mkdir()
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
                "last_received_at_ms": int(time.time() * 1000) - age_days * 86_400_000,
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
        "terminal_at_ms": int(time.time() * 1000) - age_days * 86_400_000,
        "compressed_bytes_by_stream": {"!forceOrder@arr": len(data)},
    }
    (directory / "generation-terminal.json").write_text(
        json.dumps(terminal), encoding="utf-8"
    )
    return directory


def test_build_manifest_binds_every_file_and_clean_receipts(tmp_path: Path) -> None:
    generation = _generation(tmp_path)

    manifest = json.loads(UPLOADER.build_generation_manifest(generation))

    assert manifest["schema"] == "pump_fade_v3_drive_manifest_v1"
    assert manifest["generation"] == generation.name
    assert {entry["path"] for entry in manifest["files"]} == {
        "generation-receipt.json",
        "generation-terminal.json",
        "1791443400000-00000001.jsonl.zst",
        "1791443400000-00000001.jsonl.zst.manifest.json",
    }


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("status", "RECOVERED_CRASH_INCOMPLETE"),
        ("recovered_crash", True),
        ("archive_clean", False),
    ],
)
def test_rejects_non_clean_terminal(tmp_path: Path, field: str, value: object) -> None:
    generation = _generation(tmp_path)
    terminal_path = generation / "generation-terminal.json"
    terminal: dict[str, Any] = json.loads(terminal_path.read_text(encoding="utf-8"))
    terminal[field] = value
    terminal_path.write_text(json.dumps(terminal), encoding="utf-8")

    with pytest.raises(UPLOADER.UploaderError):
        UPLOADER.build_generation_manifest(generation)


def test_rejects_unsafe_generation_name_and_symlink(tmp_path: Path) -> None:
    unsafe = tmp_path / "capture-invalid-g0001"
    unsafe.mkdir()
    with pytest.raises(UPLOADER.UploaderError):
        UPLOADER.build_generation_manifest(unsafe)

    (tmp_path / "real").mkdir()
    generation = _generation(tmp_path / "real")
    outside = tmp_path / "outside.bin"
    outside.write_bytes(b"outside")
    if hasattr(os, "symlink"):
        try:
            (generation / "link.bin").symlink_to(outside)
        except OSError:
            pytest.skip("symlink creation is unavailable on this host")
        with pytest.raises(UPLOADER.UploaderError):
            UPLOADER.build_generation_manifest(generation)


def test_prune_requires_verified_drive_receipt_and_seven_days(tmp_path: Path) -> None:
    capture_root = tmp_path / "capture"
    capture_root.mkdir()
    generation = _generation(capture_root, age_days=8)
    receipt_root = tmp_path / "receipts"
    manifests = receipt_root / "manifests"
    manifests.mkdir(parents=True)
    manifest = UPLOADER.build_generation_manifest(generation)
    manifest_path = manifests / f"{generation.name}.json"
    manifest_path.write_bytes(manifest)
    config = UPLOADER.UploadConfig(
        capture_root=capture_root,
        receipt_root=receipt_root,
        rclone_config=tmp_path / "rclone.conf",
        remote="ocidrive",
        host="trendguard-osaka-a1",
        prune_enabled=True,
    )
    receipt: dict[str, object] = {
        "verified_on_drive": False,
        "rclone_check": "FAIL",
        "manifest_sha256": hashlib.sha256(manifest).hexdigest(),
    }

    with pytest.raises(UPLOADER.UploaderError):
        UPLOADER._safe_prune(config, generation, manifest, receipt)
    assert generation.exists()


def test_prune_preserves_generations_inside_the_seven_day_window(tmp_path: Path) -> None:
    capture_root = tmp_path / "capture"
    capture_root.mkdir()
    generation = _generation(capture_root, age_days=6)
    receipt_root = tmp_path / "receipts"
    manifests = receipt_root / "manifests"
    manifests.mkdir(parents=True)
    manifest = UPLOADER.build_generation_manifest(generation)
    (manifests / f"{generation.name}.json").write_bytes(manifest)
    config = UPLOADER.UploadConfig(
        capture_root=capture_root,
        receipt_root=receipt_root,
        rclone_config=tmp_path / "rclone.conf",
        remote="ocidrive",
        host="trendguard-osaka-a1",
        prune_enabled=True,
    )
    receipt = {
        "verified_on_drive": True,
        "rclone_check": "PASS",
        "manifest_sha256": hashlib.sha256(manifest).hexdigest(),
    }

    UPLOADER._safe_prune(config, generation, manifest, receipt)

    assert generation.exists()


def test_prune_quarantines_then_removes_only_a_verified_old_generation(tmp_path: Path) -> None:
    capture_root = tmp_path / "capture"
    capture_root.mkdir()
    generation = _generation(capture_root, age_days=8)
    receipt_root = tmp_path / "receipts"
    manifests = receipt_root / "manifests"
    manifests.mkdir(parents=True)
    manifest = UPLOADER.build_generation_manifest(generation)
    (manifests / f"{generation.name}.json").write_bytes(manifest)
    config = UPLOADER.UploadConfig(
        capture_root=capture_root,
        receipt_root=receipt_root,
        rclone_config=tmp_path / "rclone.conf",
        remote="ocidrive",
        host="trendguard-osaka-a1",
        prune_enabled=True,
    )
    receipt = {
        "verified_on_drive": True,
        "rclone_check": "PASS",
        "manifest_sha256": hashlib.sha256(manifest).hexdigest(),
    }

    UPLOADER._safe_prune(config, generation, manifest, receipt)

    assert not generation.exists()
    assert list((capture_root / ".prune-staging").iterdir()) == []


def test_rclone_operations_have_requested_transfer_limits(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = UPLOADER.UploadConfig(
        capture_root=tmp_path / "capture",
        receipt_root=tmp_path / "receipts",
        rclone_config=tmp_path / "rclone.conf",
        remote="ocidrive",
        host="trendguard-osaka-a1",
        prune_enabled=False,
    )
    observed: list[str] = []

    class Result:
        returncode = 0
        stdout = b"OK"

    def fake_run(command: list[str], **_kwargs: object) -> Result:
        observed.extend(command)
        return Result()

    monkeypatch.setattr(UPLOADER.subprocess, "run", fake_run)
    assert UPLOADER._rclone(config, "lsd", "ocidrive:") == b"OK"
    assert "--transfers" in observed and observed[observed.index("--transfers") + 1] == "2"
    assert "--bwlimit" in observed and observed[observed.index("--bwlimit") + 1] == "4M"

