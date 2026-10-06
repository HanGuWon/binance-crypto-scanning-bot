"""Fail-closed closeout verifier for a stopped OCI prospective campaign."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import sqlite3
import subprocess
import tempfile
from pathlib import Path
from typing import Any

SCHEMA = "oci_directional_closeout_v1"
WRITER_UNITS = (
    "phase-r-binance-bot-2-dbfree-health.timer",
    "phase-r-binance-bot-2-dbfree-health.service",
    "phase-r-binance-bot-2-dbfree-deep-health.timer",
    "phase-r-binance-bot-2-dbfree-deep-health.service",
    "phase-r-binance-bot-2-health.timer",
    "phase-r-binance-bot-2-health.service",
    "phase-r-binance-bot-2-deep-health.timer",
    "phase-r-binance-bot-2-deep-health.service",
    "binance-bot-2-health.timer",
    "binance-bot-2-health.service",
    "binance-bot-2-deep-health.timer",
    "binance-bot-2-deep-health.service",
)


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        "w", encoding="utf-8", dir=path.parent, delete=False, suffix=".tmp"
    ) as handle:
        json.dump(payload, handle, indent=2, sort_keys=True)
        handle.write("\n")
        temporary = Path(handle.name)
    temporary.replace(path)


def _systemd_state(unit: str) -> dict[str, str]:
    result = subprocess.run(
        ["systemctl", "show", unit, "--property=ActiveState,SubState,MainPID"],
        check=False,
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        raise RuntimeError(f"systemctl show failed for {unit}: {result.stderr.strip()}")
    state: dict[str, str] = {}
    for line in result.stdout.splitlines():
        key, separator, value = line.partition("=")
        if separator:
            state[key] = value
    if state.get("ActiveState") != "inactive" or state.get("MainPID") != "0":
        raise RuntimeError(f"service is not stopped: {state}")
    return state


def _assert_no_other_writers(root: Path) -> None:
    for unit in WRITER_UNITS:
        result = subprocess.run(
            ["systemctl", "show", unit, "--property=ActiveState,MainPID"],
            check=False,
            capture_output=True,
            text=True,
        )
        if result.returncode != 0:
            raise RuntimeError(f"writer unit state unavailable: {unit}")
        state = dict(line.split("=", 1) for line in result.stdout.splitlines() if "=" in line)
        if state.get("ActiveState") != "inactive" or state.get("MainPID") not in {
            None, "", "0"
        }:
            raise RuntimeError(f"writer unit remains active: {unit}: {state}")

    proc = Path("/proc")
    if not proc.is_dir():
        raise RuntimeError("/proc is unavailable for open-descriptor verification")
    prefix = str(root.resolve())
    for process in proc.iterdir():
        if not process.name.isdigit():
            continue
        candidates = [process / "cwd", *(process / "fd").glob("*")]
        for candidate in candidates:
            try:
                target = os.readlink(candidate)
            except (FileNotFoundError, ProcessLookupError):
                continue
            except PermissionError as exc:
                raise RuntimeError(f"cannot inspect process descriptor: {candidate}") from exc
            if target == prefix or target.startswith(prefix + "/"):
                raise RuntimeError(f"campaign has open process handle: {candidate}")


def _inventory(root: Path) -> tuple[list[dict[str, Any]], int]:
    entries: list[dict[str, Any]] = []
    total_bytes = 0
    for path in sorted(root.rglob("*")):
        if path.is_symlink():
            raise RuntimeError(f"symlink in campaign payload: {path}")
        if not path.is_file():
            continue
        relative = path.relative_to(root).as_posix()
        size = path.stat().st_size
        entries.append({"path": relative, "size": size, "sha256": _sha256(path)})
        total_bytes += size
    return entries, total_bytes


def _database_receipt(
    root: Path, expected_campaign: str, expected_source: str
) -> dict[str, Any]:
    databases = sorted(root.rglob("campaign.db"))
    if len(databases) != 1:
        raise RuntimeError(f"expected exactly one campaign.db, found {len(databases)}")
    database = databases[0]
    uri = f"file:{database.as_posix()}?mode=ro"
    with sqlite3.connect(uri, uri=True) as connection:
        connection.execute("PRAGMA query_only=ON")
        quick = connection.execute("PRAGMA quick_check").fetchone()[0]
        integrity = connection.execute("PRAGMA integrity_check").fetchone()[0]
        if quick != "ok" or integrity != "ok":
            raise RuntimeError(f"SQLite integrity failure: {quick!r}, {integrity!r}")
        campaign_rows = connection.execute(
            "SELECT campaign_id, source_identity, rule_version, policy_sha256, config_sha256 "
            "FROM shadow_campaigns"
        ).fetchall()
        if len(campaign_rows) != 1 or campaign_rows[0][:2] != (
            expected_campaign,
            expected_source,
        ):
            raise RuntimeError("SQLite campaign/source identity mismatch")
        tables = [
            row[0]
            for row in connection.execute(
                "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
            )
        ]
        counts: dict[str, int] = {}
        for table in tables:
            if table.startswith("sqlite_"):
                continue
            safe_table = '"' + table.replace('"', '""') + '"'
            counts[table] = int(
                connection.execute(f"SELECT COUNT(*) FROM {safe_table}").fetchone()[0]
            )
    sidecars = {}
    for suffix in ("-wal", "-shm"):
        sidecar = Path(str(database) + suffix)
        if sidecar.exists():
            sidecars[suffix] = {"size": sidecar.stat().st_size, "sha256": _sha256(sidecar)}
    return {
        "path": database.relative_to(root).as_posix(),
        "size": database.stat().st_size,
        "sha256": _sha256(database),
        "quick_check": quick,
        "integrity_check": integrity,
        "table_counts": counts,
        "campaign": dict(
            zip(
                (
                    "campaign_id", "source_identity", "rule_version",
                    "policy_sha256", "config_sha256",
                ),
                campaign_rows[0],
                strict=True,
            )
        ),
        "sidecars": sidecars,
    }


def _segment_receipt(
    root: Path,
    expected_campaign: str,
    expected_source: str,
    inventory: list[dict[str, Any]],
) -> dict[str, Any]:
    manifests = sorted(root.rglob("*.manifest.json"))
    observations: list[dict[str, Any]] = []
    by_market: dict[str, list[tuple[int, str, str | None]]] = {}
    hashes = {item["path"]: item["sha256"] for item in inventory}
    data_files = {path for path in hashes if path.endswith(".jsonl.zst")}
    matched_data: set[str] = set()
    for manifest_path in manifests:
        payload = json.loads(manifest_path.read_text(encoding="utf-8"))
        if not isinstance(payload, dict):
            raise RuntimeError(f"manifest is not an object: {manifest_path}")
        if payload.get("campaign_id") != expected_campaign:
            raise RuntimeError(f"campaign mismatch in {manifest_path}")
        if payload.get("source_identity") != expected_source:
            raise RuntimeError(f"source mismatch in {manifest_path}")
        market = payload.get("market")
        if market not in {"spot", "futures"} or manifest_path.parent.name != market:
            raise RuntimeError(f"market mismatch in {manifest_path}")
        sequence = payload.get("segment_sequence")
        if type(sequence) is not int or sequence < 0:
            raise RuntimeError(f"invalid segment sequence in {manifest_path}")
        filename = re.fullmatch(r"(\d{13})-(\d{8})\.manifest\.json", manifest_path.name)
        if (
            filename is None
            or int(filename.group(1)) != payload.get("bucket_start_ms")
            or int(filename.group(2)) != sequence
        ):
            raise RuntimeError(f"segment filename/manifest binding mismatch: {manifest_path}")
        data_path = manifest_path.with_name(
            manifest_path.name.removesuffix(".manifest.json") + ".jsonl.zst"
        )
        if not data_path.is_file():
            raise RuntimeError(f"segment data missing for {manifest_path}")
        expected_size = payload.get("compressed_bytes")
        if type(expected_size) is not int or expected_size != data_path.stat().st_size:
            raise RuntimeError(f"segment size mismatch for {manifest_path}")
        relative_data = data_path.relative_to(root).as_posix()
        actual_hash = hashes.get(relative_data)
        expected_hash = payload.get("compressed_file_sha256")
        if (
            actual_hash is None
            or not isinstance(expected_hash, str)
            or expected_hash != actual_hash
        ):
            raise RuntimeError(f"segment hash mismatch for {manifest_path}")
        predecessor = payload.get("previous_segment_sha256")
        if predecessor is not None and not isinstance(predecessor, str):
            raise RuntimeError(f"invalid predecessor hash in {manifest_path}")
        matched_data.add(relative_data)
        by_market.setdefault(market, []).append(
            (sequence, actual_hash, predecessor)
        )
        observations.append(
            {
                "manifest": manifest_path.relative_to(root).as_posix(),
                "data": relative_data,
                "sequence": sequence,
                "size": data_path.stat().st_size,
                "sha256": actual_hash,
            }
        )
    if data_files != matched_data:
        raise RuntimeError("segment data/manifest pairing mismatch")
    for market, rows in by_market.items():
        rows.sort(key=lambda row: row[0])
        if rows[0][0] != 1 or rows[0][2] is not None:
            raise RuntimeError(f"{market} segment chain prefix is missing")
        for index, (sequence, _actual_hash, predecessor) in enumerate(rows):
            if index and sequence != rows[index - 1][0] + 1:
                raise RuntimeError(f"{market} segment sequence has a gap")
            if index and predecessor != rows[index - 1][1]:
                raise RuntimeError(f"{market} predecessor hash mismatch")
    return {
        "manifest_count": len(observations),
        "sequence_bounds_by_market": {
            market: [rows[0][0], rows[-1][0]] for market, rows in by_market.items()
        },
        "segments": observations,
    }


def closeout(
    *,
    service: str,
    campaign_root: Path,
    control_dir: Path,
    payload_manifest: Path,
    expected_campaign: str,
    expected_source: str,
) -> dict[str, Any]:
    if not campaign_root.is_dir():
        raise RuntimeError(f"campaign root does not exist: {campaign_root}")
    if control_dir.resolve().is_relative_to(campaign_root.resolve()):
        raise RuntimeError("control directory must be outside campaign root")
    if payload_manifest.resolve().is_relative_to(campaign_root.resolve()):
        raise RuntimeError("payload manifest must be outside campaign root")
    service_state = _systemd_state(service)
    _assert_no_other_writers(campaign_root)
    partials = sorted(
        path.relative_to(campaign_root).as_posix()
        for path in campaign_root.rglob("*")
        if path.is_file() and (path.name.endswith(".partial") or path.name.endswith(".tmp"))
    )
    if partials:
        raise RuntimeError(f"unfinished files remain: {partials[:5]}")
    files, total_bytes = _inventory(campaign_root)
    database = _database_receipt(campaign_root, expected_campaign, expected_source)
    segments = _segment_receipt(campaign_root, expected_campaign, expected_source, files)
    _assert_no_other_writers(campaign_root)
    manifest = {
        "schema": "oci_directional_payload_manifest_v1",
        "root_name": campaign_root.name,
        "expected_campaign": expected_campaign,
        "expected_source": expected_source,
        "file_count": len(files),
        "total_bytes": total_bytes,
        "files": files,
    }
    _write_json(payload_manifest, manifest)
    receipt = {
        "schema": SCHEMA,
        "campaign_id": expected_campaign,
        "source_identity": expected_source,
        "service": service,
        "service_state": service_state,
        "campaign_root": str(campaign_root),
        "payload_manifest": str(payload_manifest),
        "payload_manifest_sha256": _sha256(payload_manifest),
        "inventory": {"file_count": len(files), "total_bytes": total_bytes},
        "database": database,
        "segments": segments,
    }
    receipt_path = control_dir / "closeout-receipt.json"
    _write_json(receipt_path, receipt)
    return receipt


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--service", required=True)
    parser.add_argument("--campaign-root", type=Path, required=True)
    parser.add_argument("--control-dir", type=Path, required=True)
    parser.add_argument("--payload-manifest", type=Path, required=True)
    parser.add_argument("--expected-campaign", required=True)
    parser.add_argument("--expected-source", required=True)
    args = parser.parse_args()
    receipt = closeout(
        service=args.service,
        campaign_root=args.campaign_root,
        control_dir=args.control_dir,
        payload_manifest=args.payload_manifest,
        expected_campaign=args.expected_campaign,
        expected_source=args.expected_source,
    )
    print(json.dumps(receipt, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
