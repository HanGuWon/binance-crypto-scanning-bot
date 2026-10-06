"""Verify the exact Drive payload and seal separate control receipts."""

from __future__ import annotations

import argparse
import hashlib
import json
import tempfile
from pathlib import Path
from typing import Any

SCHEMA = "directional_drive_archive_verification_v1"


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        "w", encoding="utf-8", dir=path.parent, delete=False
    ) as handle:
        json.dump(payload, handle, indent=2, sort_keys=True)
        handle.write("\n")
        temporary = Path(handle.name)
    temporary.replace(path)


def _read_inventory(path: Path) -> dict[str, dict[str, Any]]:
    value = json.loads(path.read_text(encoding="utf-8"))
    rows = value if isinstance(value, list) else value.get("objects", value.get("entries", []))
    if not isinstance(rows, list):
        raise ValueError("remote inventory must be a list or an object containing objects/entries")
    inventory: dict[str, dict[str, Any]] = {}
    for row in rows:
        if not isinstance(row, dict):
            raise ValueError("remote inventory row is not an object")
        relative = str(row.get("Path", row.get("path", ""))).lstrip("/")
        if relative and not row.get("IsDir", row.get("is_dir", False)):
            if relative in inventory:
                raise ValueError(f"duplicate remote inventory path: {relative}")
            inventory[relative] = row
    return inventory


def _remote_hashes(path: Path) -> dict[str, str]:
    result: dict[str, str] = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        fields = line.strip().split(maxsplit=1)
        if len(fields) == 2 and len(fields[0]) == 64:
            relative = fields[1].lstrip("*").lstrip("/")
            if relative in result:
                raise ValueError(f"duplicate remote hash path: {relative}")
            result[relative] = fields[0].lower()
    if not result:
        value = json.loads(path.read_text(encoding="utf-8"))
        if isinstance(value, dict):
            result = {str(key).lstrip("/"): str(item).lower() for key, item in value.items()}
    return result


def verify_payload(
    *,
    payload_manifest: Path,
    remote_inventory: Path,
    remote_sha256: Path,
    rclone_check_receipt: Path,
    control_dir: Path,
    destination: str,
) -> dict[str, Any]:
    manifest = json.loads(payload_manifest.read_text(encoding="utf-8"))
    expected = {str(item["path"]): item for item in manifest["files"]}
    actual_inventory = _read_inventory(remote_inventory)
    actual_hashes = _remote_hashes(remote_sha256)
    check = json.loads(rclone_check_receipt.read_text(encoding="utf-8"))
    argv = check.get("argv")
    check_mode = "download" if isinstance(argv, list) and "--download" in argv else "checksum"
    if (
        type(check.get("exit_code")) is not int
        or check["exit_code"] != 0
        or check.get("destination") != destination
        or check.get("source_root_name") != manifest.get("root_name")
        or not isinstance(argv, list)
        or argv[:2] != ["rclone", "check"]
        or not all(isinstance(arg, str) for arg in argv)
        or not isinstance(check.get("source"), str)
        or not check["source"].rstrip("/").endswith("/" + manifest["root_name"])
        or check["source"] not in argv
        or destination not in argv
        or not ({"--download", "--checksum"} & set(argv))
        or "--one-way" not in argv
    ):
        raise RuntimeError("rclone check receipt does not prove this payload")
    closeout_path = control_dir / "closeout-receipt.json"
    closeout = json.loads(closeout_path.read_text(encoding="utf-8"))
    if (
        closeout.get("schema") != "oci_directional_closeout_v1"
        or closeout.get("payload_manifest_sha256") != _sha256(payload_manifest)
        or Path(str(closeout.get("campaign_root", ""))).name != manifest["root_name"]
        or closeout.get("inventory", {}).get("file_count") != len(expected)
        or closeout.get("inventory", {}).get("total_bytes") != manifest["total_bytes"]
    ):
        raise RuntimeError("closeout receipt does not bind this payload manifest")
    if set(expected) != set(actual_inventory):
        raise RuntimeError("Drive payload path set does not equal the source manifest")
    if set(expected) != set(actual_hashes):
        raise RuntimeError("Drive payload hash path set does not equal the source manifest")
    if sum(int(item["size"]) for item in expected.values()) != int(manifest["total_bytes"]):
        raise RuntimeError("source manifest total bytes mismatch")
    for relative, item in expected.items():
        remote = actual_inventory[relative]
        if int(remote.get("Size", remote.get("size", -1))) != int(item["size"]):
            raise RuntimeError(f"Drive payload size mismatch: {relative}")
        if actual_hashes.get(relative) != str(item["sha256"]).lower():
            raise RuntimeError(f"Drive payload SHA-256 mismatch: {relative}")
    receipt = {
        "schema": SCHEMA,
        "storage_semantics": "point_in_time_verified_mutable_replica",
        "persistent_object_lock": False,
        "destination": destination,
        "payload_manifest_sha256": _sha256(payload_manifest),
        "source_file_count": len(expected),
        "remote_file_count": len(actual_inventory),
        "source_total_bytes": int(manifest["total_bytes"]),
        "remote_total_bytes": sum(
            int(row.get("Size", row.get("size", 0)))
            for row in actual_inventory.values()
        ),
        "remote_sha256_set_sha256": hashlib.sha256(
            json.dumps(actual_hashes, sort_keys=True, separators=(",", ":")).encode("utf-8")
        ).hexdigest(),
        "rclone_check": "PASS",
        "rclone_check_mode": check_mode,
        "rclone_check_receipt_sha256": _sha256(rclone_check_receipt),
    }
    receipt_path = control_dir / "archive-verification-receipt.json"
    _write_json(receipt_path, receipt)
    control_inputs = [payload_manifest, closeout_path, receipt_path]
    control_manifest = {
        "schema": "directional_archive_control_manifest_v1",
        "objects": [
            {"path": path.name, "size": path.stat().st_size, "sha256": _sha256(path)}
            for path in control_inputs
        ],
    }
    control_manifest_path = control_dir / "control-manifest.json"
    _write_json(control_manifest_path, control_manifest)
    return receipt


def finalize_control(
    *, control_dir: Path, remote_inventory: Path, remote_sha256: Path
) -> dict[str, Any]:
    control_manifest_path = control_dir / "control-manifest.json"
    control_manifest = json.loads(control_manifest_path.read_text(encoding="utf-8"))
    expected = {
        item["path"]: item for item in control_manifest["objects"]
    }
    expected[control_manifest_path.name] = {
        "size": control_manifest_path.stat().st_size,
        "sha256": _sha256(control_manifest_path),
    }
    inventory = _read_inventory(remote_inventory)
    hashes = _remote_hashes(remote_sha256)
    if set(expected) != set(inventory) or set(expected) != set(hashes):
        raise RuntimeError("Drive control path set mismatch")
    for name, item in expected.items():
        remote = inventory[name]
        if int(remote.get("Size", remote.get("size", -1))) != int(item["size"]):
            raise RuntimeError(f"Drive control size mismatch: {name}")
        if hashes[name] != item["sha256"]:
            raise RuntimeError(f"Drive control SHA-256 mismatch: {name}")
    marker = {
        "schema": "directional_archive_complete_v1",
        "control_manifest_sha256": _sha256(control_manifest_path),
        "storage_semantics": "point_in_time_verified_mutable_replica",
    }
    _write_json(control_dir / "ARCHIVE_COMPLETE.json", marker)
    return marker


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--stage", choices=("payload", "control"), default="payload")
    parser.add_argument("--payload-manifest", type=Path)
    parser.add_argument("--remote-payload-inventory", type=Path)
    parser.add_argument("--remote-payload-sha256", type=Path)
    parser.add_argument("--rclone-check-receipt", type=Path)
    parser.add_argument("--remote-control-inventory", type=Path)
    parser.add_argument("--remote-control-sha256", type=Path)
    parser.add_argument("--control-dir", type=Path, required=True)
    parser.add_argument("--destination")
    args = parser.parse_args()
    if args.stage == "control":
        if args.remote_control_inventory is None or args.remote_control_sha256 is None:
            parser.error("control stage requires remote control inventory and SHA-256")
        result = finalize_control(
            control_dir=args.control_dir,
            remote_inventory=args.remote_control_inventory,
            remote_sha256=args.remote_control_sha256,
        )
    else:
        if (
            args.payload_manifest is None
            or args.remote_payload_inventory is None
            or args.remote_payload_sha256 is None
            or args.rclone_check_receipt is None
            or args.destination is None
        ):
            parser.error("payload stage requires manifest, inventory, SHA-256, check, destination")
        result = verify_payload(
            payload_manifest=args.payload_manifest,
            remote_inventory=args.remote_payload_inventory,
            remote_sha256=args.remote_payload_sha256,
            rclone_check_receipt=args.rclone_check_receipt,
            control_dir=args.control_dir,
            destination=args.destination,
        )
    print(
        json.dumps(
            result,
            indent=2,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
