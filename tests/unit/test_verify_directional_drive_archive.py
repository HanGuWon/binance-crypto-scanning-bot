import hashlib
import json
from pathlib import Path

import pytest
from tools.verify_directional_drive_archive import finalize_control, verify_payload


def _check_receipt(tmp_path: Path) -> Path:
    path = tmp_path / "rclone-check.json"
    path.write_text(
        json.dumps(
            {
                "exit_code": 0,
                "destination": "gdrive:archive/payload",
                "source_root_name": "campaign",
                "source": ":sftp:/archive/campaign",
                "argv": [
                    "rclone", "check", ":sftp:/archive/campaign",
                    "gdrive:archive/payload", "--download", "--one-way",
                ],
            }
        ),
        encoding="utf-8",
    )
    return path


def _closeout_receipt(control: Path, payload_manifest: Path) -> None:
    control.mkdir(exist_ok=True)
    manifest = json.loads(payload_manifest.read_text(encoding="utf-8"))
    (control / "closeout-receipt.json").write_text(
        json.dumps({
            "schema": "oci_directional_closeout_v1",
            "payload_manifest_sha256": hashlib.sha256(payload_manifest.read_bytes()).hexdigest(),
            "campaign_root": "/archive/campaign",
            "inventory": {
                "file_count": len(manifest["files"]),
                "total_bytes": manifest["total_bytes"],
            },
        }),
        encoding="utf-8",
    )


def test_verify_payload_seals_control_objects(tmp_path: Path) -> None:
    source = tmp_path / "source.bin"
    source.write_bytes(b"payload")
    digest = hashlib.sha256(source.read_bytes()).hexdigest()
    payload_manifest = tmp_path / "payload-manifest.json"
    payload_manifest.write_text(
        json.dumps(
            {
                "root_name": "campaign",
                "total_bytes": source.stat().st_size,
                "files": [{"path": "source.bin", "size": source.stat().st_size, "sha256": digest}],
            }
        ),
        encoding="utf-8",
    )
    inventory = tmp_path / "inventory.json"
    inventory.write_text(json.dumps([{"Path": "source.bin", "Size": 7}]), encoding="utf-8")
    remote_sha = tmp_path / "sha256.txt"
    remote_sha.write_text(f"{digest}  source.bin\n", encoding="utf-8")
    control = tmp_path / "control"
    _closeout_receipt(control, payload_manifest)
    check = _check_receipt(tmp_path)
    check_value = json.loads(check.read_text(encoding="utf-8"))
    check_value["argv"][-2] = "--checksum"
    check.write_text(json.dumps(check_value), encoding="utf-8")

    receipt = verify_payload(
        payload_manifest=payload_manifest,
        remote_inventory=inventory,
        remote_sha256=remote_sha,
        rclone_check_receipt=check,
        control_dir=control,
        destination="gdrive:archive/payload",
    )

    assert receipt["rclone_check"] == "PASS"
    assert receipt["rclone_check_mode"] == "checksum"
    assert (control / "control-manifest.json").is_file()
    assert not (control / "ARCHIVE_COMPLETE.json").exists()

    names = [
        "payload-manifest.json",
        "closeout-receipt.json",
        "archive-verification-receipt.json",
        "control-manifest.json",
    ]
    control_inventory = tmp_path / "control-inventory.json"
    control_inventory.write_text(
        json.dumps(
            [
                {
                    "Path": name,
                    "Size": (
                        payload_manifest if name == "payload-manifest.json" else control / name
                    )
                    .stat()
                    .st_size,
                }
                for name in names
            ]
        ),
        encoding="utf-8",
    )
    control_hashes = tmp_path / "control-sha256.txt"
    hash_lines = []
    for name in names:
        path = payload_manifest if name == "payload-manifest.json" else control / name
        hash_lines.append(f"{hashlib.sha256(path.read_bytes()).hexdigest()}  {name}\n")
    control_hashes.write_text("".join(hash_lines), encoding="utf-8")
    marker = finalize_control(
        control_dir=control,
        remote_inventory=control_inventory,
        remote_sha256=control_hashes,
    )
    assert marker["schema"] == "directional_archive_complete_v1"
    assert (control / "ARCHIVE_COMPLETE.json").is_file()


def test_verify_payload_rejects_changed_remote_object(tmp_path: Path) -> None:
    payload_manifest = tmp_path / "payload-manifest.json"
    payload_manifest.write_text(
        json.dumps(
            {
                "root_name": "campaign",
                "total_bytes": 7,
                "files": [{"path": "source.bin", "size": 7, "sha256": "a" * 64}],
            }
        ),
        encoding="utf-8",
    )
    inventory = tmp_path / "inventory.json"
    inventory.write_text(json.dumps([{"Path": "source.bin", "Size": 7}]), encoding="utf-8")
    remote_sha = tmp_path / "sha256.txt"
    remote_sha.write_text("b" * 64 + "  source.bin\n", encoding="utf-8")
    _closeout_receipt(tmp_path / "control", payload_manifest)

    with pytest.raises(RuntimeError, match="SHA-256 mismatch"):
        verify_payload(
            payload_manifest=payload_manifest,
            remote_inventory=inventory,
            remote_sha256=remote_sha,
            rclone_check_receipt=_check_receipt(tmp_path),
            control_dir=tmp_path / "control",
            destination="gdrive:archive/payload",
        )


@pytest.mark.parametrize("corruption", ["closeout", "command"])
def test_verify_payload_rejects_unbound_evidence(tmp_path: Path, corruption: str) -> None:
    manifest = tmp_path / "payload-manifest.json"
    manifest.write_text(
        json.dumps({"root_name": "campaign", "total_bytes": 0, "files": []}),
        encoding="utf-8",
    )
    control = tmp_path / "control"
    _closeout_receipt(control, manifest)
    check = _check_receipt(tmp_path)
    if corruption == "closeout":
        receipt = control / "closeout-receipt.json"
        value = json.loads(receipt.read_text(encoding="utf-8"))
        value["payload_manifest_sha256"] = "0" * 64
        receipt.write_text(json.dumps(value), encoding="utf-8")
    else:
        value = json.loads(check.read_text(encoding="utf-8"))
        value["argv"] = ["rclone", "lsf", "gdrive:archive/payload"]
        check.write_text(json.dumps(value), encoding="utf-8")
    inventory = tmp_path / "inventory.json"
    inventory.write_text("[]", encoding="utf-8")
    hashes = tmp_path / "sha256.txt"
    hashes.write_text("{}", encoding="utf-8")
    with pytest.raises(RuntimeError, match=r"closeout receipt|rclone check receipt"):
        verify_payload(
            payload_manifest=manifest,
            remote_inventory=inventory,
            remote_sha256=hashes,
            rclone_check_receipt=check,
            control_dir=control,
            destination="gdrive:archive/payload",
        )


def test_verify_payload_rejects_unproven_rclone_check(tmp_path: Path) -> None:
    manifest = tmp_path / "payload-manifest.json"
    manifest.write_text(
        json.dumps({"root_name": "campaign", "total_bytes": 0, "files": []}), encoding="utf-8"
    )
    inventory = tmp_path / "inventory.json"
    inventory.write_text("[]", encoding="utf-8")
    hashes = tmp_path / "sha256.txt"
    hashes.write_text("{}", encoding="utf-8")
    check = _check_receipt(tmp_path)
    check.write_text(
        json.dumps(
            {
                "exit_code": 1,
                "destination": "gdrive:archive/payload",
                "source_root_name": "campaign",
                "command": "rclone check",
            }
        ),
        encoding="utf-8",
    )
    with pytest.raises(RuntimeError, match="rclone check receipt"):
        verify_payload(
            payload_manifest=manifest,
            remote_inventory=inventory,
            remote_sha256=hashes,
            rclone_check_receipt=check,
            control_dir=tmp_path / "control",
            destination="gdrive:archive/payload",
        )
