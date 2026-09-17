"""Read-only hourly operational health receipts for the OCI collector.

The CLI writes one append-only receipt and never invokes systemctl. A stop runner
is injectable only for tests; live stop authority is a later WP3 concern.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import shutil
import sqlite3
import time
from collections.abc import Callable, Iterator, Mapping, Sequence
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Protocol

try:
    from tools.oci_phase_p_storage_math import (
        GrowthSample,
        SegmentSample,
        compute_gates,
        compute_tape_rates,
        conservative_growth_rate,
        deterministic_finalize_reserve,
    )
except ImportError:
    from oci_phase_p_storage_math import (
        GrowthSample,
        SegmentSample,
        compute_gates,
        compute_tape_rates,
        conservative_growth_rate,
        deterministic_finalize_reserve,
    )

GIB = 1024**3
HOUR_MS = 3_600_000
EXPECTED_SOURCE = (
    "worktree-source-v1:650fe603f79812e79cd9390c0d2e94fbab8071a4eab1cc95f40fc229c7f403dd"
)
SUCCESSOR_UNIT = "binance-bot-2-prospective.service"
RECEIPT_SCHEMA = "oci_operational_health_v1"
SEGMENT_SCHEMA = "raw_tape_segmented_zstd_v2"
STATES = {"GREEN", "YELLOW", "RED"}
HEALTH_MODES = {"fast", "deep"}
FAST_STATE_SCHEMA = "oci_fast_health_state_v1"
FAST_RECENT_SAMPLE_LIMIT = 256
FAST_ROOT_SAMPLE_LIMIT = 16
FAST_MANIFEST_READ_WORKERS = 8
FAST_MANIFEST_READ_BATCH_SIZE = 128
HEALTH_SQLITE_TIMEOUT_SECONDS = 0.5
REASONS = {
    "SOURCE_DRIFT",
    "STALE_MARKET",
    "DATABASE_FAILURE",
    "DB_BUSY",
    "CHAIN_FAILURE",
    "HISTORY_CORRUPT",
    "ROOT_HISTORY_UNAVAILABLE",
    "QUOTA_48H_GATE_FAIL",
    "ROOT_48H_GATE_FAIL",
    "QUOTA_RUNWAY_IMMINENT",
    "ROOT_RUNWAY_IMMINENT",
    "ROOT_EMERGENCY_FLOOR_REACHED",
    "QUOTA_PARTIAL_FINALIZE_HEADROOM_LOW",
    "DATA_48H_GATE_FAIL",
    "DATA_RUNWAY_IMMINENT",
    "DATA_EMERGENCY_FLOOR_REACHED",
}
REQUIRED_HISTORY_KEYS = {"schema_version", "receipt_id", "captured_at_ms", "root", "state", "rates"}
PARTIAL_RE = re.compile(r"^([0-9]+)-([0-9]+)\.jsonl\.zst\.partial$")


class StopRunner(Protocol):
    """Injected, non-production runner used only by deterministic tests."""

    def __call__(self, target: str) -> int: ...


@dataclass(frozen=True)
class MonitorConfig:
    """Fixed campaign inputs and operational thresholds."""

    campaign_id: str
    campaign_root: Path
    db_path: Path
    tape_root: Path
    config_path: Path
    activation_ms: int
    expected_source: str = EXPECTED_SOURCE
    quota_bytes: int = 28 * GIB
    quota_safety_margin_bytes: int = GIB
    root_os_reserve_bytes: int = 8 * GIB
    root_history_floor_bytes: int = 2 * GIB
    health_dir: Path | None = None
    root_path: Path = Path("/")
    mode: str = "deep"

    def resolved_health_dir(self) -> Path:
        if self.health_dir is not None:
            return self.health_dir
        directory_name = "phase-fast" if self.mode == "fast" else "phase-p"
        return self.campaign_root / "health" / directory_name

    def resolved_state_path(self) -> Path:
        return self.resolved_health_dir() / "fast-state.json"


class HistoryError(ValueError):
    """A previous receipt cannot safely contribute to a rate."""


class FastStateError(ValueError):
    """FAST cursor state cannot safely contribute to an incremental receipt."""


def _iso(ms: int) -> str:
    return datetime.fromtimestamp(ms / 1000, tz=UTC).isoformat()


def _sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _directory_bytes(path: Path) -> int:
    if not path.exists():
        return 0
    if path.is_file():
        return path.stat().st_size
    return sum(item.stat().st_size for item in path.rglob("*") if item.is_file())


def _path_device(path: Path) -> int | None:
    """Return a path's filesystem device, or None when ownership is unavailable."""

    try:
        return path.stat().st_dev
    except OSError:
        try:
            return path.parent.stat().st_dev
        except OSError:
            return None


def _storage_surfaces(
    config: MonitorConfig,
    *,
    finalize_reserve_bytes: int,
    conservative_tape_bytes_per_h: float,
) -> dict[str, Any]:
    """Group health paths by device and derive independent capacity surfaces."""

    root_usage = shutil.disk_usage(config.root_path)
    root_device = _path_device(config.root_path)
    named_paths = {
        "root": config.root_path,
        "campaign_root": config.campaign_root,
        "db": config.db_path,
        "tape": config.tape_root,
        "health": config.resolved_health_dir(),
        "logs": config.campaign_root / "logs",
    }
    grouped: dict[int, dict[str, Any]] = {}
    ownership_complete = root_device is not None
    for name, path in named_paths.items():
        device = _path_device(path)
        if device is None:
            if name != "logs" or path.exists():
                ownership_complete = False
            continue
        surface = grouped.setdefault(
            device,
            {
                "st_dev": device,
                "paths": [],
                "resident_bytes": 0,
                "usage": shutil.disk_usage(path if path.exists() else path.parent),
            },
        )
        surface["paths"].append(name)
        surface["resident_bytes"] += _shallow_bytes(path)

    root_surface = grouped.get(root_device) if root_device is not None else None
    same_filesystem = (
        ownership_complete
        and root_surface is not None
        and not any(device != root_device for device in grouped)
    )
    reserve_size = _directory_bytes if config.mode == "deep" else _shallow_bytes
    health_reserve_path = (
        config.campaign_root / "health"
        if config.health_dir is None
        else config.resolved_health_dir()
    )
    root_resident_reserve = sum(
        reserve_size(path)
        for path in (config.db_path, health_reserve_path, config.campaign_root / "logs")
        if _path_device(path) == root_device
    )
    if not ownership_complete:
        # Unknown ownership must retain the conservative legacy charge.
        root_resident_reserve = sum(
            reserve_size(path)
            for path in (config.db_path, health_reserve_path, config.campaign_root / "logs")
        )
    tape_device = _path_device(config.tape_root)
    tape_on_root = (
        not ownership_complete
        or (tape_device is not None and tape_device == root_device)
    )
    data_surfaces = [
        surface
        for device, surface in grouped.items()
        if root_device is None or device != root_device
    ]
    data_surface = next(
        (surface for surface in data_surfaces if "tape" in surface["paths"]),
        None,
    )
    data_required = (
        conservative_tape_bytes_per_h * 48
        + finalize_reserve_bytes
        + config.quota_safety_margin_bytes
    )
    data_gate_met: bool | None = None
    data_free_bytes: int | None = None
    data_runway_hours: float | None = None
    data_emergency_floor_reached: bool | None = None
    if data_surfaces and ownership_complete:
        data_requirements = {
            id(surface): (
                data_required
                if "tape" in surface["paths"]
                else config.quota_safety_margin_bytes
            )
            for surface in data_surfaces
        }
        data_free_bytes = min(int(surface["usage"].free) for surface in data_surfaces)
        data_gate_met = all(
            int(surface["usage"].free) >= data_requirements[id(surface)]
            for surface in data_surfaces
        )
        data_emergency_floor_reached = any(
            int(surface["usage"].free)
            <= (
                finalize_reserve_bytes + config.quota_safety_margin_bytes
                if "tape" in surface["paths"]
                else config.quota_safety_margin_bytes
            )
            for surface in data_surfaces
        )
        for surface in data_surfaces:
            surface["required_bytes"] = data_requirements[id(surface)]
        if data_surface is not None:
            tape_free_bytes = int(data_surface["usage"].free)
            data_runway_hours = (
                max(
                    tape_free_bytes
                    - finalize_reserve_bytes
                    - config.quota_safety_margin_bytes,
                    0,
                )
                / conservative_tape_bytes_per_h
                if conservative_tape_bytes_per_h > 0
                else None
            )
    return {
        "root_usage": root_usage,
        "root_surface": root_surface,
        "root_device": root_device,
        "same_filesystem": same_filesystem,
        "tape_on_root": tape_on_root,
        "root_finalize_reserve_bytes": finalize_reserve_bytes if tape_on_root else 0,
        "ownership_complete": ownership_complete,
        "root_resident_reserve_bytes": root_resident_reserve,
        "data_surfaces": data_surfaces,
        "data_surface": data_surface,
        "data_free_bytes": data_free_bytes,
        "data_required_bytes": data_required,
        "data_gate_met": data_gate_met,
        "data_runway_hours": data_runway_hours,
        "data_emergency_floor_reached": data_emergency_floor_reached,
    }


def _surface_receipt(surface: Mapping[str, Any]) -> dict[str, Any]:
    """Serialize one filesystem surface without exposing path contents."""

    usage = surface["usage"]
    return {
        "st_dev": surface["st_dev"],
        "paths": sorted(surface["paths"]),
        "resident_bytes": surface["resident_bytes"],
        "required_bytes": surface.get("required_bytes"),
        "total_bytes": usage.total,
        "used_bytes": usage.used,
        "available_bytes": usage.free,
    }


def _partial_bucket(name: str) -> int | None:
    match = PARTIAL_RE.fullmatch(name)
    return int(match.group(1)) if match else None


def _read_source_and_identities(expected_source: str, config_path: Path) -> dict[str, Any]:
    """Recompute source and scientific identities through the installed app."""

    from oci_preregistration import _scientific_identities

    from signalbot.config import load_settings
    from signalbot.prospective.source_freeze import default_source_root, freeze_source

    freeze = freeze_source(default_source_root())
    settings = load_settings(config_path)
    identities = _scientific_identities(settings)
    return {
        "expected": expected_source,
        "observed": freeze.source_identity,
        "source_root_sha256": freeze.source_root_sha256,
        "frozen_file_count": len(freeze.files),
        "match": freeze.source_identity == expected_source,
        "scientific_identities": identities,
    }


def _read_sqlite(db_path: Path, campaign_id: str) -> dict[str, Any]:
    """Read campaign state with explicit SQLite read-only/query-only checks."""

    conn = sqlite3.connect(
        f"file:{db_path.as_posix()}?mode=ro",
        uri=True,
        timeout=HEALTH_SQLITE_TIMEOUT_SECONDS,
    )
    try:
        conn.execute("PRAGMA query_only=ON")
        query_only = int(conn.execute("PRAGMA query_only").fetchone()[0])
        quick_check = str(conn.execute("PRAGMA quick_check").fetchone()[0])
        campaign = conn.execute(
            "SELECT campaign_id, source_identity FROM shadow_campaigns WHERE campaign_id=?",
            (campaign_id,),
        ).fetchone()
        rows = conn.execute(
            "SELECT market, status, complete, COUNT(*), "
            "MIN(decision_close_ms), MAX(decision_close_ms) "
            "FROM shadow_coverage WHERE campaign_id=? "
            "GROUP BY market, status, complete ORDER BY market, status, complete",
            (campaign_id,),
        ).fetchall()
        return {
            "mode": "ro",
            "query_only": query_only,
            "quick_check": quick_check,
            "campaign": list(campaign) if campaign else None,
            "campaign_match": campaign is not None,
            "coverage_rows": [list(row) for row in rows],
        }
    finally:
        conn.close()


def _read_sqlite_fast(db_path: Path, campaign_id: str) -> dict[str, Any]:
    """Read only bounded SQLite metadata for the hourly FAST path.

    FAST deliberately omits ``quick_check``.  A temporary writer lock is
    surfaced to the caller as ``sqlite3.OperationalError`` so the receipt can
    classify it as DB_BUSY without treating an observability collision as
    corruption or issuing a service action.
    """

    conn = sqlite3.connect(
        f"file:{db_path.as_posix()}?mode=ro",
        uri=True,
        timeout=HEALTH_SQLITE_TIMEOUT_SECONDS,
    )
    try:
        conn.execute("PRAGMA query_only=ON")
        query_only = int(conn.execute("PRAGMA query_only").fetchone()[0])
        campaign = conn.execute(
            "SELECT campaign_id, source_identity FROM shadow_campaigns WHERE campaign_id=?",
            (campaign_id,),
        ).fetchone()
        rows = conn.execute(
            "SELECT market, status, complete, COUNT(*), "
            "MIN(decision_close_ms), MAX(decision_close_ms) "
            "FROM shadow_coverage WHERE campaign_id=? "
            "GROUP BY market, status, complete ORDER BY market, status, complete",
            (campaign_id,),
        ).fetchall()
        return {
            "mode": "ro",
            "query_only": query_only,
            "quick_check": "SKIPPED_FAST",
            "campaign": list(campaign) if campaign else None,
            "campaign_match": campaign is not None,
            "coverage_rows": [list(row) for row in rows],
            "db_busy": False,
        }
    finally:
        conn.close()


def _is_sqlite_busy(exc: BaseException) -> bool:
    """Return whether SQLite rejected a read because a lock was held."""

    return isinstance(exc, sqlite3.OperationalError) and any(
        marker in str(exc).lower() for marker in ("database is locked", "database is busy")
    )


def _read_segments(
    tape_root: Path, campaign_id: str, expected_source: str, captured_at_ms: int
) -> dict[str, Any]:
    """Inspect manifests, data hashes, chains, and partials without repair."""

    errors: list[dict[str, Any]] = []
    samples: list[SegmentSample] = []
    partials: dict[tuple[str, int], int] = {}
    markets: dict[str, dict[str, Any]] = {}
    for market in ("futures", "spot"):
        directory = tape_root / market
        manifests = sorted(directory.glob("*.manifest.json")) if directory.is_dir() else []
        previous_hash: str | None = None
        parsed: list[dict[str, Any]] = []
        for manifest_path in manifests:
            try:
                payload = json.loads(manifest_path.read_text(encoding="utf-8"))
                data_name = str(payload.get("compressed_file_name", ""))
                data_path = (
                    directory / data_name
                    if data_name
                    else directory / manifest_path.name.replace(".manifest.json", ".jsonl.zst")
                )
                actual_hash = _sha256_file(data_path) if data_path.is_file() else None
                if actual_hash != payload.get("compressed_file_sha256"):
                    errors.append(
                        {"market": market, "manifest": manifest_path.name, "reason": "DATA_HASH"}
                    )
                if payload.get("previous_segment_sha256") != previous_hash:
                    errors.append(
                        {
                            "market": market,
                            "manifest": manifest_path.name,
                            "reason": "PREVIOUS_HASH",
                        }
                    )
                for field, expected in (
                    ("campaign_id", campaign_id),
                    ("market", market),
                    ("source_identity", expected_source),
                    ("storage_schema_version", SEGMENT_SCHEMA),
                ):
                    if payload.get(field) != expected:
                        errors.append(
                            {
                                "market": market,
                                "manifest": manifest_path.name,
                                "reason": field.upper(),
                            }
                        )
                bucket = int(payload["bucket_start_ms"])
                last_received = int(payload["last_received_at_ms"])
                samples.append(
                    SegmentSample(
                        bucket_start_ms=bucket,
                        last_received_at_ms=last_received,
                        compressed_bytes=int(payload.get("compressed_bytes") or 0),
                        manifest_bytes=manifest_path.stat().st_size,
                        uncompressed_bytes=int(payload.get("uncompressed_bytes") or 0),
                        healthy=True,
                    )
                )
                parsed.append(payload)
                previous_hash = actual_hash
            except (OSError, ValueError, KeyError, TypeError, json.JSONDecodeError) as exc:
                errors.append(
                    {
                        "market": market,
                        "manifest": manifest_path.name,
                        "reason": "MALFORMED",
                        "detail": type(exc).__name__,
                    }
                )
        partial_paths = sorted(directory.glob("*.jsonl.zst.partial")) if directory.is_dir() else []
        partial_sizes: dict[str, int] = {}
        for path in partial_paths:
            bucket = _partial_bucket(path.name)
            if bucket is None:
                errors.append({"market": market, "partial": path.name, "reason": "PARTIAL_NAME"})
                continue
            partials[(market, bucket)] = path.stat().st_size
            partial_sizes[path.name] = path.stat().st_size
        latest = max((int(item["last_received_at_ms"]) for item in parsed), default=None)
        markets[market] = {
            "manifest_count": len(parsed),
            "sealed_segment_count": len(parsed),
            "active_partial_count": len(partial_paths),
            "active_partial_bytes": sum(partial_sizes.values()),
            "active_partials": partial_sizes,
            "latest_last_received_at_ms": latest,
            "fresh": latest is not None and captured_at_ms - latest <= 15 * 60_000,
        }
    return {
        "snapshot_id": (
            f"{captured_at_ms}-{_sha256_bytes(json.dumps(markets, sort_keys=True).encode())[:16]}"
        ),
        "markets": markets,
        "samples": samples,
        "active_partials": partials,
        "chain_errors": errors,
        "chain_healthy": not errors,
    }


def _fast_market_state() -> dict[str, Any]:
    """Return an empty bounded cursor for one market."""

    return {
        "last_manifest_name": None,
        "last_segment_sha256": None,
        "manifest_count": 0,
        "finalized_bytes": 0,
        "manifest_bytes": 0,
        "max_uncompressed_bytes": 0,
        "latest_last_received_at_ms": None,
        "recent_samples": [],
        "bootstrap_metadata_only": False,
    }


def _new_fast_state(campaign_id: str, expected_source: str) -> dict[str, Any]:
    """Return the on-disk state envelope used by FAST HEALTH."""

    return {
        "schema_version": FAST_STATE_SCHEMA,
        "campaign_id": campaign_id,
        "expected_source": expected_source,
        "markets": {market: _fast_market_state() for market in ("futures", "spot")},
        "root_samples": [],
    }


def _state_int(value: Any, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise FastStateError(f"invalid FAST state integer: {field}")
    return value


def _state_sample(value: Any) -> SegmentSample:
    if not isinstance(value, dict):
        raise FastStateError("invalid FAST state sample")
    try:
        return SegmentSample(
            bucket_start_ms=_state_int(value["bucket_start_ms"], "bucket_start_ms"),
            last_received_at_ms=_state_int(value["last_received_at_ms"], "last_received_at_ms"),
            compressed_bytes=_state_int(value["compressed_bytes"], "compressed_bytes"),
            manifest_bytes=_state_int(value["manifest_bytes"], "manifest_bytes"),
            uncompressed_bytes=_state_int(value["uncompressed_bytes"], "uncompressed_bytes"),
            healthy=value.get("healthy") is True,
        )
    except (KeyError, TypeError) as exc:
        raise FastStateError("invalid FAST state sample") from exc


def _serialize_state_sample(sample: SegmentSample) -> dict[str, Any]:
    return {
        "bucket_start_ms": sample.bucket_start_ms,
        "last_received_at_ms": sample.last_received_at_ms,
        "compressed_bytes": sample.compressed_bytes,
        "manifest_bytes": sample.manifest_bytes,
        "uncompressed_bytes": sample.uncompressed_bytes,
        "healthy": sample.healthy,
    }


def _load_fast_state(path: Path, campaign_id: str, expected_source: str) -> dict[str, Any]:
    """Load and validate FAST state without repairing it."""

    if not path.exists():
        return _new_fast_state(campaign_id, expected_source)
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise FastStateError(f"invalid FAST state: {type(exc).__name__}") from exc
    if not isinstance(payload, dict):
        raise FastStateError("invalid FAST state envelope")
    if (
        payload.get("schema_version") != FAST_STATE_SCHEMA
        or payload.get("campaign_id") != campaign_id
        or payload.get("expected_source") != expected_source
    ):
        raise FastStateError("FAST state identity mismatch")
    markets = payload.get("markets")
    if not isinstance(markets, dict):
        raise FastStateError("FAST state markets missing")
    for market in ("futures", "spot"):
        item = markets.get(market)
        if not isinstance(item, dict):
            raise FastStateError(f"FAST state market missing: {market}")
        for field in (
            "manifest_count",
            "finalized_bytes",
            "manifest_bytes",
            "max_uncompressed_bytes",
        ):
            _state_int(item.get(field), f"{market}.{field}")
        latest = item.get("latest_last_received_at_ms")
        if latest is not None:
            _state_int(latest, f"{market}.latest_last_received_at_ms")
        cursor = item.get("last_manifest_name")
        if cursor is not None and not isinstance(cursor, str):
            raise FastStateError(f"invalid FAST cursor: {market}")
        last_hash = item.get("last_segment_sha256")
        if last_hash is not None and not isinstance(last_hash, str):
            raise FastStateError(f"invalid FAST predecessor hash: {market}")
        samples = item.get("recent_samples")
        if not isinstance(samples, list) or len(samples) > FAST_RECENT_SAMPLE_LIMIT:
            raise FastStateError(f"invalid FAST recent sample set: {market}")
        for sample in samples:
            _state_sample(sample)
    roots = payload.get("root_samples", [])
    if not isinstance(roots, list) or len(roots) > FAST_ROOT_SAMPLE_LIMIT:
        raise FastStateError("invalid FAST root sample set")
    for sample in roots:
        if not isinstance(sample, dict):
            raise FastStateError("invalid FAST root sample")
        _state_int(sample.get("captured_at_ms"), "root.captured_at_ms")
        _state_int(sample.get("used_bytes"), "root.used_bytes")
        if sample.get("surface_id") != "root":
            raise FastStateError("invalid FAST root surface")
    return payload


def _write_json_atomic(path: Path, payload: Mapping[str, Any]) -> None:
    """Atomically persist a health-owned state file and its parent directory."""

    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    encoded = (json.dumps(payload, indent=2, sort_keys=True) + "\n").encode("utf-8")
    try:
        with temporary.open("wb") as handle:
            handle.write(encoded)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
        if os.name != "nt":
            directory_fd = os.open(path.parent, os.O_RDONLY)
            try:
                os.fsync(directory_fd)
            finally:
                os.close(directory_fd)
    finally:
        temporary.unlink(missing_ok=True)


def _fast_data_path(directory: Path, manifest_path: Path, payload: Mapping[str, Any]) -> Path:
    data_name = str(payload.get("compressed_file_name", ""))
    if not data_name:
        data_name = manifest_path.name.removesuffix(".manifest.json") + ".jsonl.zst"
    relative = Path(data_name)
    if relative.is_absolute() or relative.name != data_name:
        raise ValueError("data path must remain inside the market directory")
    return directory / data_name


def _read_fast_manifest_bytes(
    paths: Sequence[Path],
) -> Iterator[tuple[Path, bytes | None, OSError | None]]:
    """Read manifests concurrently in bounded batches while preserving name order."""

    if not paths:
        return
    worker_count = min(FAST_MANIFEST_READ_WORKERS, len(paths))
    with ThreadPoolExecutor(max_workers=worker_count) as executor:
        for start in range(0, len(paths), FAST_MANIFEST_READ_BATCH_SIZE):
            batch = paths[start : start + FAST_MANIFEST_READ_BATCH_SIZE]
            futures = [executor.submit(path.read_bytes) for path in batch]
            for path, future in zip(batch, futures, strict=True):
                try:
                    yield path, future.result(), None
                except OSError as exc:
                    yield path, None, exc


def _read_segments_fast(
    tape_root: Path,
    campaign_id: str,
    expected_source: str,
    captured_at_ms: int,
    state_path: Path,
) -> dict[str, Any]:
    """Inspect only new manifests plus active partials using bounded state."""

    state = _load_fast_state(state_path, campaign_id, expected_source)
    errors: list[dict[str, Any]] = []
    samples: list[SegmentSample] = []
    partials: dict[tuple[str, int], int] = {}
    markets: dict[str, dict[str, Any]] = {}
    total_skipped = 0
    total_new = 0
    total_hash_verified = 0
    bootstrap = False

    for market in ("futures", "spot"):
        directory = tape_root / market
        manifest_paths: list[Path] = []
        partial_paths: list[Path] = []
        sealed_data_sizes: dict[str, int] = {}
        if directory.is_dir():
            with os.scandir(directory) as entries:
                for entry in entries:
                    if entry.name.endswith(".manifest.json"):
                        manifest_paths.append(Path(entry.path))
                    elif entry.name.endswith(".jsonl.zst.partial"):
                        partial_paths.append(Path(entry.path))
                    elif entry.name.endswith(".jsonl.zst"):
                        sealed_data_sizes[entry.name] = entry.stat().st_size
            manifest_paths.sort(key=lambda path: path.name)
            partial_paths.sort(key=lambda path: path.name)
        prior = dict(state["markets"][market])
        cursor = prior.get("last_manifest_name")
        stored_count = int(prior["manifest_count"])
        if cursor is None:
            bootstrap = True
        if cursor is not None and len(manifest_paths) < stored_count:
            errors.append({"market": market, "reason": "MANIFEST_COUNT_DECREASED"})
        new_paths = [path for path in manifest_paths if cursor is None or path.name > cursor]
        skipped = len(manifest_paths) - len(new_paths)
        total_skipped += skipped
        total_new += len(new_paths)
        if cursor is not None and len(manifest_paths) != stored_count + len(new_paths):
            errors.append({"market": market, "reason": "MANIFEST_COUNT_CHANGED"})

        previous_hash = prior.get("last_segment_sha256")
        market_samples = [_state_sample(item) for item in prior.get("recent_samples", [])]
        finalized_bytes = int(prior["finalized_bytes"])
        manifest_bytes_total = int(prior["manifest_bytes"])
        max_uncompressed = int(prior["max_uncompressed_bytes"])
        latest = prior.get("latest_last_received_at_ms")
        market_error_start = len(errors)
        hash_verified = 0
        for manifest_path, manifest_bytes, read_error in _read_fast_manifest_bytes(new_paths):
            if read_error is not None:
                errors.append(
                    {
                        "market": market,
                        "manifest": manifest_path.name,
                        "reason": "MALFORMED",
                        "detail": type(read_error).__name__,
                    }
                )
                continue
            assert manifest_bytes is not None
            try:
                payload = json.loads(manifest_bytes)
                if not isinstance(payload, dict):
                    raise ValueError("manifest is not an object")
                data_path = _fast_data_path(directory, manifest_path, payload)
                try:
                    data_size = sealed_data_sizes[data_path.name]
                except KeyError as exc:
                    raise FileNotFoundError(data_path) from exc
                manifest_size = len(manifest_bytes)
                declared_hash = payload.get("compressed_file_sha256")
                declared_size = payload.get("compressed_bytes")
                if not isinstance(declared_hash, str) or not declared_hash:
                    raise ValueError("manifest compressed hash missing")
                if not isinstance(declared_size, int) or declared_size < 0:
                    raise ValueError("manifest compressed size invalid")
                if data_size != declared_size:
                    errors.append(
                        {"market": market, "manifest": manifest_path.name, "reason": "DATA_SIZE"}
                    )
                for field, expected in (
                    ("campaign_id", campaign_id),
                    ("market", market),
                    ("source_identity", expected_source),
                    ("storage_schema_version", SEGMENT_SCHEMA),
                ):
                    if payload.get(field) != expected:
                        errors.append(
                            {
                                "market": market,
                                "manifest": manifest_path.name,
                                "reason": field.upper(),
                            }
                        )
                bucket = payload.get("bucket_start_ms")
                last_received = payload.get("last_received_at_ms")
                uncompressed = payload.get("uncompressed_bytes")
                if isinstance(bucket, bool) or not isinstance(bucket, int) or bucket < 0:
                    raise ValueError("manifest bucket field invalid")
                if (
                    isinstance(last_received, bool)
                    or not isinstance(last_received, int)
                    or last_received < 0
                ):
                    raise ValueError("manifest received timestamp invalid")
                if (
                    isinstance(uncompressed, bool)
                    or not isinstance(uncompressed, int)
                    or uncompressed < 0
                ):
                    raise ValueError("manifest timestamp/size field invalid")
                if payload.get("previous_segment_sha256") != previous_hash:
                    errors.append(
                        {
                            "market": market,
                            "manifest": manifest_path.name,
                            "reason": "PREVIOUS_HASH",
                        }
                    )
                metadata_only = cursor is None and stored_count == 0
                actual_hash = declared_hash
                if not metadata_only:
                    actual_hash = _sha256_file(data_path)
                    hash_verified += 1
                    total_hash_verified += 1
                    if actual_hash != declared_hash:
                        errors.append(
                            {
                                "market": market,
                                "manifest": manifest_path.name,
                                "reason": "DATA_HASH",
                            }
                        )
                sample = SegmentSample(
                    bucket_start_ms=bucket,
                    last_received_at_ms=last_received,
                    compressed_bytes=data_size,
                    manifest_bytes=manifest_size,
                    uncompressed_bytes=uncompressed,
                    healthy=True,
                )
                market_samples.append(sample)
                finalized_bytes += data_size
                manifest_bytes_total += manifest_size
                max_uncompressed = max(max_uncompressed, uncompressed)
                latest = max(latest, last_received) if latest is not None else last_received
                previous_hash = actual_hash
            except (OSError, ValueError, KeyError, TypeError, json.JSONDecodeError) as exc:
                errors.append(
                    {
                        "market": market,
                        "manifest": manifest_path.name,
                        "reason": "MALFORMED",
                        "detail": type(exc).__name__,
                    }
                )
        cutoff = captured_at_ms - 2 * HOUR_MS
        market_samples = [item for item in market_samples if item.last_received_at_ms >= cutoff]
        if len(market_samples) > FAST_RECENT_SAMPLE_LIMIT:
            market_samples = market_samples[-FAST_RECENT_SAMPLE_LIMIT:]
        state["markets"][market] = {
            **prior,
            "last_manifest_name": (
                new_paths[-1].name if new_paths and len(errors) == market_error_start else cursor
            ),
            "last_segment_sha256": previous_hash,
            "manifest_count": stored_count + len(new_paths),
            "finalized_bytes": finalized_bytes,
            "manifest_bytes": manifest_bytes_total,
            "max_uncompressed_bytes": max_uncompressed,
            "latest_last_received_at_ms": latest,
            "recent_samples": [_serialize_state_sample(item) for item in market_samples],
            "bootstrap_metadata_only": bool(prior.get("bootstrap_metadata_only")) or (
                cursor is None and stored_count == 0 and bool(new_paths)
            ),
        }
        partial_sizes: dict[str, int] = {}
        for partial_path in partial_paths:
            bucket = _partial_bucket(partial_path.name)
            if bucket is None:
                errors.append(
                    {"market": market, "partial": partial_path.name, "reason": "PARTIAL_NAME"}
                )
                continue
            try:
                size = partial_path.stat().st_size
            except OSError as exc:
                errors.append(
                    {
                        "market": market,
                        "partial": partial_path.name,
                        "reason": "PARTIAL_STAT",
                        "detail": type(exc).__name__,
                    }
                )
                continue
            partials[(market, bucket)] = size
            partial_sizes[partial_path.name] = size
        markets[market] = {
            "manifest_count": int(state["markets"][market]["manifest_count"]),
            "sealed_segment_count": int(state["markets"][market]["manifest_count"]),
            "active_partial_count": len(partial_paths),
            "active_partial_bytes": sum(partial_sizes.values()),
            "active_partials": partial_sizes,
            "latest_last_received_at_ms": state["markets"][market]["latest_last_received_at_ms"],
            "fresh": (
                state["markets"][market]["latest_last_received_at_ms"] is not None
                and captured_at_ms - state["markets"][market]["latest_last_received_at_ms"]
                <= 15 * 60_000
            ),
            "cursor": cursor,
            "skipped_existing": skipped,
            "new_manifests": len(new_paths),
            "hash_verified": hash_verified,
            "bootstrap_metadata_only": bool(state["markets"][market]["bootstrap_metadata_only"]),
        }
        samples.extend(market_samples)

    if not errors:
        _write_json_atomic(state_path, state)
    roots = [
        _growth_sample(
            int(item["captured_at_ms"]), int(item["used_bytes"]), str(item["surface_id"])
        )
        for item in state.get("root_samples", [])
    ]
    return {
        "snapshot_id": (
            f"{captured_at_ms}-"
            f"{_sha256_bytes(json.dumps(markets, sort_keys=True).encode())[:16]}"
        ),
        "markets": markets,
        "state_markets": [state["markets"][market] for market in ("futures", "spot")],
        "samples": samples,
        "active_partials": partials,
        "chain_errors": errors,
        "chain_healthy": not errors,
        "root_samples": roots,
        "incremental": {
            "state_path": str(state_path),
            "state_written": not errors,
            "bootstrap": bootstrap,
            "skipped_existing": total_skipped,
            "new_manifests": total_new,
            "hash_verified": total_hash_verified,
            "recent_sample_limit": FAST_RECENT_SAMPLE_LIMIT,
        },
    }


def _fast_rates(
    *,
    activation_ms: int,
    captured_at_ms: int,
    segments: Mapping[str, Any],
) -> dict[str, float | int | None]:
    """Combine cumulative cursor bytes with bounded recent rate samples."""

    elapsed_ms = captured_at_ms - activation_ms
    if elapsed_ms <= 0:
        raise ValueError("capture must follow activation")
    finalized_bytes = sum(int(item["finalized_bytes"]) for item in segments["state_markets"])
    manifest_bytes = sum(int(item["manifest_bytes"]) for item in segments["state_markets"])
    partial_bytes = sum(int(value) for value in segments["active_partials"].values())
    whole_rate = (finalized_bytes + partial_bytes) * HOUR_MS / elapsed_ms
    finalized_rate = finalized_bytes * HOUR_MS / elapsed_ms
    recent_math = compute_tape_rates(
        activation_ms=activation_ms,
        captured_at_ms=captured_at_ms,
        segments=segments["samples"],
        active_partials=segments["active_partials"],
    )
    candidates = [
        value
        for value in (
            whole_rate,
            recent_math["recent_bytes_per_h"],
            recent_math["healthy_bytes_per_h"],
            recent_math["p95_bytes_per_h"],
        )
        if value is not None and value > 0
    ]
    return {
        "finalized_bytes": finalized_bytes,
        "manifest_bytes": manifest_bytes,
        "active_partial_bytes": partial_bytes,
        "whole_clock_bytes_per_h": whole_rate,
        "finalized_bytes_per_h": finalized_rate,
        "recent_bytes_per_h": recent_math["recent_bytes_per_h"],
        "healthy_bytes_per_h": recent_math["healthy_bytes_per_h"],
        "p95_bytes_per_h": recent_math["p95_bytes_per_h"],
        "conservative_tape_bytes_per_h": max(candidates) if candidates else None,
        "observed_max_terminal_plaintext_bytes": max(
            (int(item["max_uncompressed_bytes"]) for item in segments["state_markets"]),
            default=0,
        ),
        "p95_window_count": recent_math["p95_window_count"],
    }


def _shallow_bytes(path: Path) -> int:
    """Read one file/directory inode size without recursively scanning history."""

    try:
        return path.stat().st_size if path.exists() else 0
    except OSError:
        return 0


def _record_fast_root_sample(path: Path, captured_at_ms: int, used_bytes: int) -> None:
    """Append one bounded root sample after the FAST tape state is durable."""

    state = json.loads(path.read_text(encoding="utf-8"))
    roots = list(state.get("root_samples", []))
    roots.append({"captured_at_ms": captured_at_ms, "used_bytes": used_bytes, "surface_id": "root"})
    state["root_samples"] = roots[-FAST_ROOT_SAMPLE_LIMIT:]
    _write_json_atomic(path, state)


def _read_history(receipt_dir: Path) -> list[dict[str, Any]]:
    """Read strict monitor receipts; corrupt history is never treated as zero."""

    history: list[dict[str, Any]] = []
    if not receipt_dir.is_dir():
        return history
    for path in sorted(receipt_dir.glob("phase-p-*.json")):
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise HistoryError(f"invalid receipt {path.name}: {type(exc).__name__}") from exc
        if (
            REQUIRED_HISTORY_KEYS - payload.keys()
            or payload.get("schema_version") != RECEIPT_SCHEMA
        ):
            raise HistoryError(f"invalid receipt {path.name}: missing/unknown schema")
        if payload.get("state") not in STATES:
            raise HistoryError(f"invalid receipt {path.name}: unknown state")
        rates = payload.get("rates")
        if (
            not isinstance(rates, dict)
            or not {"conservative_tape_bytes_per_h", "available"} <= rates.keys()
        ):
            raise HistoryError(f"invalid receipt {path.name}: unknown rate schema")
        history.append(payload)
    return history


def classify_operational_state(
    *,
    source_drift: bool,
    stale_markets: bool,
    database_ok: bool,
    chain_ok: bool,
    history_available: bool,
    quota_gate_met: bool,
    root_gate_met: bool | None,
    quota_runway_hours: float | None,
    root_runway_hours: float | None,
    root_free_bytes: int,
    root_floor_bytes: int,
    quota_headroom_bytes: int,
    finalize_reserve_bytes: int,
    one_hour_tape_ingestion_bytes: float,
    database_busy: bool = False,
    data_gate_met: bool | None = None,
    data_runway_hours: float | None = None,
    data_free_bytes: int | None = None,
    data_floor_bytes: int = 0,
    data_emergency_floor_reached: bool | None = None,
) -> dict[str, Any]:
    """Return state/reason/action semantics; no command is executed here."""

    reasons: list[str] = []
    if source_drift:
        reasons.append("SOURCE_DRIFT")
    if stale_markets:
        reasons.append("STALE_MARKET")
    if not database_ok:
        reasons.append("DATABASE_FAILURE")
    if database_busy:
        reasons.append("DB_BUSY")
    if not chain_ok:
        reasons.append("CHAIN_FAILURE")
    if not history_available:
        reasons.append("ROOT_HISTORY_UNAVAILABLE")
    if not quota_gate_met:
        reasons.append("QUOTA_48H_GATE_FAIL")
    if root_gate_met is False:
        reasons.append("ROOT_48H_GATE_FAIL")
    storage_reasons: list[str] = []
    if quota_runway_hours is not None and quota_runway_hours <= 24:
        storage_reasons.append("QUOTA_RUNWAY_IMMINENT")
    if root_runway_hours is not None and root_runway_hours <= 24:
        storage_reasons.append("ROOT_RUNWAY_IMMINENT")
    if root_free_bytes <= root_floor_bytes:
        storage_reasons.append("ROOT_EMERGENCY_FLOOR_REACHED")
    if quota_headroom_bytes < finalize_reserve_bytes + one_hour_tape_ingestion_bytes:
        storage_reasons.append("QUOTA_PARTIAL_FINALIZE_HEADROOM_LOW")
    if data_gate_met is False:
        reasons.append("DATA_48H_GATE_FAIL")
    if data_runway_hours is not None and data_runway_hours <= 24:
        storage_reasons.append("DATA_RUNWAY_IMMINENT")
    if (
        data_emergency_floor_reached is True
        or (
            data_emergency_floor_reached is None
            and data_free_bytes is not None
            and data_free_bytes <= data_floor_bytes
        )
    ):
        storage_reasons.append("DATA_EMERGENCY_FLOOR_REACHED")
    reasons.extend(storage_reasons)
    integrity_red = (
        source_drift
        or stale_markets
        or (not database_ok and not database_busy)
        or not chain_ok
    )
    storage_imminent = bool(storage_reasons)
    state = (
        "RED"
        if integrity_red or storage_imminent
        else "YELLOW"
        if (
            database_busy
            or not history_available
            or not quota_gate_met
            or root_gate_met is not True
            or data_gate_met is False
        )
        else "GREEN"
    )
    return {
        "state": state,
        "reasons": sorted(set(reasons) & REASONS),
        "storage_reasons": sorted(set(storage_reasons)),
        "stop_authorized": storage_imminent and not database_busy,
    }


def _write_receipt_exclusive(path: Path, payload: Mapping[str, Any]) -> None:
    """Create one durable receipt and refuse to overwrite an existing path."""

    path.parent.mkdir(parents=True, exist_ok=True)
    encoded = (json.dumps(payload, indent=2, sort_keys=True) + "\n").encode("utf-8")
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    try:
        with os.fdopen(descriptor, "wb") as handle:
            descriptor = -1
            handle.write(encoded)
            handle.flush()
            os.fsync(handle.fileno())
    finally:
        if descriptor != -1:
            os.close(descriptor)
    if os.name == "nt":
        # Windows does not expose directory handles with the flags needed for
        # fsync; the file itself is already flushed durably above. Linux OCI
        # hosts take the parent-directory fsync path below.
        return
    directory_fd = os.open(path.parent, os.O_RDONLY)
    try:
        os.fsync(directory_fd)
    finally:
        os.close(directory_fd)


def _new_receipt_path(receipt_dir: Path, captured_at_ms: int) -> Path:
    return receipt_dir / f"phase-p-{captured_at_ms}-{os.getpid()}.json"


def _growth_sample(captured_at_ms: int, used_bytes: int, surface_id: str) -> GrowthSample:
    return GrowthSample(captured_at_ms, used_bytes, surface_id)


def _empty_fast_segments(state_path: Path, detail: str) -> dict[str, Any]:
    return {
        "snapshot_id": "fast-state-error",
        "markets": {
            market: {
                "manifest_count": 0,
                "sealed_segment_count": 0,
                "active_partial_count": 0,
                "active_partial_bytes": 0,
                "active_partials": {},
                "latest_last_received_at_ms": None,
                "fresh": False,
                "cursor": None,
                "skipped_existing": 0,
                "new_manifests": 0,
                "hash_verified": 0,
                "bootstrap_metadata_only": False,
            }
            for market in ("futures", "spot")
        },
        "state_markets": [_fast_market_state(), _fast_market_state()],
        "samples": [],
        "active_partials": {},
        "chain_errors": [{"reason": "FAST_STATE", "detail": detail}],
        "chain_healthy": False,
        "root_samples": [],
        "incremental": {
            "state_path": str(state_path),
            "state_written": False,
            "bootstrap": False,
            "skipped_existing": 0,
            "new_manifests": 0,
            "hash_verified": 0,
            "recent_sample_limit": FAST_RECENT_SAMPLE_LIMIT,
        },
    }


def _build_fast_receipt(
    config: MonitorConfig,
    *,
    captured_at_ms: int,
    source_probe: Callable[[str, Path], dict[str, Any]],
) -> dict[str, Any]:
    """Build the bounded hourly FAST HEALTH receipt."""

    started_ns = time.monotonic_ns()
    source_error: str | None = None
    try:
        source = source_probe(config.expected_source, config.config_path)
    except (ImportError, OSError, ValueError, RuntimeError, SystemExit) as exc:
        source_error = f"{type(exc).__name__}: {exc}"
        source = {
            "expected": config.expected_source,
            "observed": None,
            "source_root_sha256": None,
            "frozen_file_count": None,
            "match": False,
            "scientific_identities": {},
        }

    sqlite_error: str | None = None
    database_busy = False
    try:
        sqlite = _read_sqlite_fast(config.db_path, config.campaign_id)
    except (OSError, sqlite3.Error) as exc:
        sqlite_error = f"{type(exc).__name__}: {exc}"
        database_busy = _is_sqlite_busy(exc)
        sqlite = {
            "mode": "ro",
            "query_only": None,
            "quick_check": "SKIPPED_FAST",
            "campaign_match": False,
            "coverage_rows": [],
            "db_busy": database_busy,
        }

    state_error: str | None = None
    try:
        segments = _read_segments_fast(
            config.tape_root,
            config.campaign_id,
            config.expected_source,
            captured_at_ms,
            config.resolved_state_path(),
        )
    except (FastStateError, OSError, ValueError) as exc:
        state_error = f"{type(exc).__name__}: {exc}"
        segments = _empty_fast_segments(config.resolved_state_path(), state_error)

    rates_raw = _fast_rates(
        activation_ms=config.activation_ms,
        captured_at_ms=captured_at_ms,
        segments=segments,
    )
    reserve = deterministic_finalize_reserve(segments["active_partials"].values())
    physical_bytes = int(rates_raw["finalized_bytes"] or 0) + int(
        rates_raw["active_partial_bytes"] or 0
    )
    conservative_tape = float(rates_raw["conservative_tape_bytes_per_h"] or 0.0)
    surfaces = _storage_surfaces(
        config,
        finalize_reserve_bytes=reserve,
        conservative_tape_bytes_per_h=conservative_tape,
    )
    root_usage = surfaces["root_usage"]
    root_samples = list(segments.get("root_samples", []))
    if segments["incremental"].get("state_written"):
        try:
            _record_fast_root_sample(config.resolved_state_path(), captured_at_ms, root_usage.used)
            root_samples.append(_growth_sample(captured_at_ms, root_usage.used, "root"))
        except (FastStateError, OSError, json.JSONDecodeError, TypeError, KeyError) as exc:
            state_error = state_error or f"{type(exc).__name__}: {exc}"
    root_rate = conservative_growth_rate(root_samples)
    source_drift = source.get("match") is not True or source_error is not None
    database_ok = sqlite.get("query_only") == 1 and sqlite.get("campaign_match") is True
    history_available = root_rate is not None and state_error is None
    reserve_floor_bytes = surfaces["root_resident_reserve_bytes"]
    root_floor = config.root_os_reserve_bytes + max(
        reserve_floor_bytes, config.root_history_floor_bytes
    ) + surfaces["root_finalize_reserve_bytes"]
    conservative_root = (
        max(conservative_tape if surfaces["tape_on_root"] else 0.0, root_rate or 0.0)
        if history_available
        else None
    )
    gates = compute_gates(
        quota_bytes=config.quota_bytes,
        current_physical_quota_bytes=physical_bytes,
        conservative_tape_bytes_per_h=conservative_tape,
        finalize_reserve_bytes=reserve,
        quota_safety_margin_bytes=config.quota_safety_margin_bytes,
        root_free_bytes=root_usage.free,
        conservative_root_growth_bytes_per_h=conservative_root,
        root_emergency_floor_bytes=root_floor,
    )
    quota_runway = (
        max(config.quota_bytes - physical_bytes, 0) / conservative_tape
        if conservative_tape > 0
        else None
    )
    root_runway = (
        max(root_usage.free - root_floor, 0) / conservative_root
        if conservative_root and conservative_root > 0
        else None
    )
    classification = classify_operational_state(
        source_drift=source_drift,
        stale_markets=not all(item["fresh"] for item in segments["markets"].values()),
        database_ok=database_ok,
        database_busy=database_busy,
        chain_ok=segments["chain_healthy"],
        history_available=history_available,
        quota_gate_met=bool(gates["quota_gate_met"]),
        root_gate_met=gates["root_gate_met"],
        quota_runway_hours=quota_runway,
        root_runway_hours=root_runway,
        root_free_bytes=root_usage.free,
        root_floor_bytes=root_floor,
        quota_headroom_bytes=int(gates["quota_headroom_bytes"]),
        finalize_reserve_bytes=reserve,
        one_hour_tape_ingestion_bytes=conservative_tape,
        data_gate_met=surfaces["data_gate_met"],
        data_runway_hours=surfaces["data_runway_hours"],
        data_free_bytes=surfaces["data_free_bytes"],
        data_floor_bytes=reserve + config.quota_safety_margin_bytes,
        data_emergency_floor_reached=surfaces["data_emergency_floor_reached"],
    )
    if sqlite_error and not database_busy:
        classification["reasons"] = sorted(
            set(classification["reasons"] + ["DATABASE_FAILURE"])
        )
        classification["state"], classification["stop_authorized"] = "RED", False
    if state_error:
        classification["reasons"] = sorted(set(classification["reasons"] + ["CHAIN_FAILURE"]))
        classification["state"], classification["stop_authorized"] = "RED", False
    # FAST is an alert-only observer.  Any storage stop authority remains a
    # DEEP/explicit operational decision and is never acted on by this path.
    classification["stop_authorized"] = False
    elapsed_ms = (time.monotonic_ns() - started_ns) // 1_000_000
    return {
        "schema_version": RECEIPT_SCHEMA,
        "mode": "fast",
        "receipt_id": f"phase-fast-{captured_at_ms}",
        "captured_at_ms": captured_at_ms,
        "captured_at_utc": _iso(captured_at_ms),
        "campaign_id": config.campaign_id,
        "activation_ms": config.activation_ms,
        "source_identity": source,
        "scientific_identities": source.get("scientific_identities", {}),
        "sqlite": sqlite,
        "tape": {
            "snapshot_id": segments["snapshot_id"],
            "markets": segments["markets"],
            "chain_healthy": segments["chain_healthy"],
            "chain_errors": segments["chain_errors"],
            "physical_bytes": physical_bytes,
            "finalize_reserve_bytes": reserve,
            "incremental": segments["incremental"],
        },
        "rates": {
            "whole_clock_bytes_per_h": rates_raw["whole_clock_bytes_per_h"],
            "finalized_bytes_per_h": rates_raw["finalized_bytes_per_h"],
            "recent_bytes_per_h": rates_raw["recent_bytes_per_h"],
            "healthy_bytes_per_h": rates_raw["healthy_bytes_per_h"],
            "p95_bytes_per_h": rates_raw["p95_bytes_per_h"],
            "conservative_tape_bytes_per_h": rates_raw["conservative_tape_bytes_per_h"],
            "conservative_tape_gib_per_h": conservative_tape / GIB,
            "available": rates_raw["conservative_tape_bytes_per_h"] is not None,
            "p95_window_count": rates_raw["p95_window_count"],
            "provenance": (
                "FAST cursor cumulative bytes plus bounded recent V2 manifest samples; "
                "full data hashing belongs to DEEP AUDIT"
            ),
        },
        "quota": {
            "quota_bytes": config.quota_bytes,
            "current_physical_bytes": physical_bytes,
            "headroom_bytes": gates["quota_headroom_bytes"],
            "required_48h_bytes": gates["quota_required_bytes"],
            "gate_met": gates["quota_gate_met"],
            "runway_hours": quota_runway,
            "safety_margin_bytes": config.quota_safety_margin_bytes,
            "finalize_reserve_bytes": reserve,
        },
        "root": {
            "surface_id": "root",
            "device_free_bytes": root_usage.free,
            "used_bytes": root_usage.used,
            "total_bytes": root_usage.total,
            "rate_bytes_per_h": conservative_root,
            "floor_bytes": root_floor,
            "required_48h_bytes": gates["root_required_bytes"],
            "gate_met": gates["root_gate_met"],
            "runway_hours": root_runway,
            "history_available": history_available,
        },
        "storage_surfaces": {
            "ownership_complete": surfaces["ownership_complete"],
            "same_filesystem": surfaces["same_filesystem"],
            "tape_on_root": surfaces["tape_on_root"],
            "root_finalize_reserve_bytes": surfaces["root_finalize_reserve_bytes"],
            "surfaces": [
                _surface_receipt(surface)
                for surface in [
                    *([surfaces["root_surface"]] if surfaces["root_surface"] is not None else []),
                    *surfaces["data_surfaces"],
                ]
            ],
            "data_gate_met": surfaces["data_gate_met"],
            "data_free_bytes": surfaces["data_free_bytes"],
            "data_required_48h_bytes": surfaces["data_required_bytes"],
            "data_runway_hours": surfaces["data_runway_hours"],
            "data_emergency_floor_reached": surfaces["data_emergency_floor_reached"],
        },
        "state": classification["state"],
        "reasons": classification["reasons"],
        "stop_authorized": classification["stop_authorized"],
        "action": {
            "attempted": False,
            "would_authorize": classification["stop_authorized"],
            "mode": "fast-alert-only",
            "target": SUCCESSOR_UNIT,
            "exit_code": None,
        },
        "durability": {
            "exclusive_create": True,
            "file_fsync": True,
            "directory_fsync": True,
            "state_atomic_replace": True,
            "receipt_directory": str(config.resolved_health_dir()),
        },
        "errors": {
            "source": source_error,
            "sqlite": sqlite_error,
            "history": None,
            "fast_state": state_error,
        },
        "governance": {
            "read_only_inputs": True,
            "no_source_mutation": True,
            "no_evidence_deletion": True,
            "private_api_or_orders": False,
            "real_service_action": False,
        },
        "health_budget_ms": elapsed_ms,
    }


def build_receipt(
    config: MonitorConfig,
    *,
    captured_at_ms: int | None = None,
    source_probe: Callable[[str, Path], dict[str, Any]] = _read_source_and_identities,
) -> dict[str, Any]:
    """Build a full receipt from current read-only inputs."""

    now_ms = int(time.time() * 1000) if captured_at_ms is None else captured_at_ms
    if config.mode not in HEALTH_MODES:
        raise ValueError(f"unknown health mode: {config.mode}")
    if config.mode == "fast":
        return _build_fast_receipt(config, captured_at_ms=now_ms, source_probe=source_probe)
    source_error: str | None = None
    try:
        source = source_probe(config.expected_source, config.config_path)
    except (ImportError, OSError, ValueError, RuntimeError, SystemExit) as exc:
        source_error = f"{type(exc).__name__}: {exc}"
        source = {
            "expected": config.expected_source,
            "observed": None,
            "source_root_sha256": None,
            "frozen_file_count": None,
            "match": False,
            "scientific_identities": {},
        }
    sqlite_error: str | None = None
    try:
        sqlite = _read_sqlite(config.db_path, config.campaign_id)
    except (OSError, sqlite3.Error) as exc:
        sqlite_error = f"{type(exc).__name__}: {exc}"
        sqlite = {
            "mode": "ro",
            "query_only": None,
            "quick_check": None,
            "campaign_match": False,
            "coverage_rows": [],
        }
    segments = _read_segments(config.tape_root, config.campaign_id, config.expected_source, now_ms)
    rates_raw = compute_tape_rates(
        activation_ms=config.activation_ms,
        captured_at_ms=now_ms,
        segments=segments["samples"],
        active_partials=segments["active_partials"],
    )
    reserve = deterministic_finalize_reserve(segments["active_partials"].values())
    physical_bytes = int(rates_raw["finalized_bytes"] or 0) + int(
        rates_raw["active_partial_bytes"] or 0
    )
    conservative_tape = float(rates_raw["conservative_tape_bytes_per_h"] or 0.0)
    surfaces = _storage_surfaces(
        config,
        finalize_reserve_bytes=reserve,
        conservative_tape_bytes_per_h=conservative_tape,
    )
    root_usage = surfaces["root_usage"]
    receipt_dir = config.resolved_health_dir()
    history_error: str | None = None
    try:
        history = _read_history(receipt_dir)
    except HistoryError as exc:
        history, history_error = [], str(exc)
    root_samples = [
        _growth_sample(
            item["captured_at_ms"],
            item["root"]["used_bytes"],
            item["root"].get("surface_id", "root"),
        )
        for item in history
        if isinstance(item.get("root"), dict) and isinstance(item["root"].get("used_bytes"), int)
    ]
    root_samples.append(_growth_sample(now_ms, root_usage.used, "root"))
    root_rate = conservative_growth_rate(root_samples)
    database_ok = (
        sqlite.get("quick_check") == "ok"
        and sqlite.get("query_only") == 1
        and sqlite.get("campaign_match") is True
    )
    source_drift = source.get("match") is not True or source_error is not None
    history_available = root_rate is not None and history_error is None
    root_floor = (
        config.root_os_reserve_bytes
        + max(surfaces["root_resident_reserve_bytes"], config.root_history_floor_bytes)
        + surfaces["root_finalize_reserve_bytes"]
    )
    conservative_root = (
        max(conservative_tape if surfaces["tape_on_root"] else 0.0, root_rate or 0.0)
        if history_available
        else None
    )
    gates = compute_gates(
        quota_bytes=config.quota_bytes,
        current_physical_quota_bytes=physical_bytes,
        conservative_tape_bytes_per_h=conservative_tape,
        finalize_reserve_bytes=reserve,
        quota_safety_margin_bytes=config.quota_safety_margin_bytes,
        root_free_bytes=root_usage.free,
        conservative_root_growth_bytes_per_h=conservative_root,
        root_emergency_floor_bytes=root_floor,
    )
    quota_runway = (
        max(config.quota_bytes - physical_bytes, 0) / conservative_tape
        if conservative_tape > 0
        else None
    )
    root_runway = (
        max(root_usage.free - root_floor, 0) / conservative_root
        if conservative_root and conservative_root > 0
        else None
    )
    classification = classify_operational_state(
        source_drift=source_drift,
        stale_markets=not all(item["fresh"] for item in segments["markets"].values()),
        database_ok=database_ok,
        chain_ok=segments["chain_healthy"],
        history_available=history_available,
        quota_gate_met=bool(gates["quota_gate_met"]),
        root_gate_met=gates["root_gate_met"],
        quota_runway_hours=quota_runway,
        root_runway_hours=root_runway,
        root_free_bytes=root_usage.free,
        root_floor_bytes=root_floor,
        quota_headroom_bytes=int(gates["quota_headroom_bytes"]),
        finalize_reserve_bytes=reserve,
        one_hour_tape_ingestion_bytes=conservative_tape,
        data_gate_met=surfaces["data_gate_met"],
        data_runway_hours=surfaces["data_runway_hours"],
        data_free_bytes=surfaces["data_free_bytes"],
        data_floor_bytes=reserve + config.quota_safety_margin_bytes,
        data_emergency_floor_reached=surfaces["data_emergency_floor_reached"],
    )
    if sqlite_error:
        classification["reasons"] = sorted(set(classification["reasons"] + ["DATABASE_FAILURE"]))
        classification["state"], classification["stop_authorized"] = "RED", False
    if history_error:
        classification["reasons"] = sorted(set(classification["reasons"] + ["HISTORY_CORRUPT"]))
        classification["state"], classification["stop_authorized"] = "RED", False
    return {
        "schema_version": RECEIPT_SCHEMA,
        "receipt_id": f"phase-p-{now_ms}",
        "captured_at_ms": now_ms,
        "captured_at_utc": _iso(now_ms),
        "campaign_id": config.campaign_id,
        "activation_ms": config.activation_ms,
        "source_identity": source,
        "scientific_identities": source.get("scientific_identities", {}),
        "sqlite": sqlite,
        "tape": {
            "snapshot_id": segments["snapshot_id"],
            "markets": segments["markets"],
            "chain_healthy": segments["chain_healthy"],
            "chain_errors": segments["chain_errors"],
            "physical_bytes": physical_bytes,
            "finalize_reserve_bytes": reserve,
        },
        "rates": {
            "whole_clock_bytes_per_h": rates_raw["whole_clock_bytes_per_h"],
            "finalized_bytes_per_h": rates_raw["finalized_bytes_per_h"],
            "recent_bytes_per_h": rates_raw["recent_bytes_per_h"],
            "healthy_bytes_per_h": rates_raw["healthy_bytes_per_h"],
            "p95_bytes_per_h": rates_raw["p95_bytes_per_h"],
            "conservative_tape_bytes_per_h": rates_raw["conservative_tape_bytes_per_h"],
            "conservative_tape_gib_per_h": conservative_tape / GIB,
            "available": rates_raw["conservative_tape_bytes_per_h"] is not None,
            "p95_window_count": rates_raw["p95_window_count"],
            "provenance": (
                "tools/oci_phase_p_storage_math.py over V2 manifests and "
                "market/path-dimensioned partials"
            ),
        },
        "quota": {
            "quota_bytes": config.quota_bytes,
            "current_physical_bytes": physical_bytes,
            "headroom_bytes": gates["quota_headroom_bytes"],
            "required_48h_bytes": gates["quota_required_bytes"],
            "gate_met": gates["quota_gate_met"],
            "runway_hours": quota_runway,
            "safety_margin_bytes": config.quota_safety_margin_bytes,
            "finalize_reserve_bytes": reserve,
        },
        "root": {
            "surface_id": "root",
            "device_free_bytes": root_usage.free,
            "used_bytes": root_usage.used,
            "total_bytes": root_usage.total,
            "rate_bytes_per_h": conservative_root,
            "floor_bytes": root_floor,
            "required_48h_bytes": gates["root_required_bytes"],
            "gate_met": gates["root_gate_met"],
            "runway_hours": root_runway,
            "history_available": history_available,
        },
        "storage_surfaces": {
            "ownership_complete": surfaces["ownership_complete"],
            "same_filesystem": surfaces["same_filesystem"],
            "tape_on_root": surfaces["tape_on_root"],
            "root_finalize_reserve_bytes": surfaces["root_finalize_reserve_bytes"],
            "surfaces": [
                _surface_receipt(surface)
                for surface in [
                    *([surfaces["root_surface"]] if surfaces["root_surface"] is not None else []),
                    *surfaces["data_surfaces"],
                ]
            ],
            "data_gate_met": surfaces["data_gate_met"],
            "data_free_bytes": surfaces["data_free_bytes"],
            "data_required_48h_bytes": surfaces["data_required_bytes"],
            "data_runway_hours": surfaces["data_runway_hours"],
            "data_emergency_floor_reached": surfaces["data_emergency_floor_reached"],
        },
        "state": classification["state"],
        "reasons": classification["reasons"],
        "stop_authorized": classification["stop_authorized"],
        "action": {
            "attempted": False,
            "would_authorize": classification["stop_authorized"],
            "mode": "wp2-dry-run-only",
            "target": SUCCESSOR_UNIT,
            "exit_code": None,
        },
        "durability": {
            "exclusive_create": True,
            "file_fsync": True,
            "directory_fsync": True,
            "receipt_directory": str(receipt_dir),
        },
        "errors": {"source": source_error, "sqlite": sqlite_error, "history": history_error},
        "governance": {
            "read_only_inputs": True,
            "no_source_mutation": True,
            "no_evidence_deletion": True,
            "private_api_or_orders": False,
            "real_service_action": False,
        },
    }


def run_once(
    config: MonitorConfig,
    *,
    captured_at_ms: int | None = None,
    stop_runner: StopRunner | None = None,
) -> tuple[dict[str, Any], Path]:
    """Write one unique durable receipt, then optionally call an injected mock."""

    now_ms = int(time.time() * 1000) if captured_at_ms is None else captured_at_ms
    receipt = build_receipt(config, captured_at_ms=now_ms)
    path = _new_receipt_path(config.resolved_health_dir(), now_ms)
    _write_receipt_exclusive(path, receipt)
    if stop_runner is not None and receipt["stop_authorized"]:
        receipt["action"] = {
            **receipt["action"],
            "attempted": True,
            "mode": "mock",
            "exit_code": int(stop_runner(SUCCESSOR_UNIT)),
        }
    return receipt, path


def _parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--campaign-id", required=True)
    parser.add_argument("--campaign-root", type=Path, required=True)
    parser.add_argument("--db", type=Path, required=True)
    parser.add_argument("--tape-root", type=Path, required=True)
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--activation-ms", type=int, required=True)
    parser.add_argument("--expected-source", default=EXPECTED_SOURCE)
    parser.add_argument("--quota-bytes", type=int, default=28 * GIB)
    parser.add_argument("--health-dir", type=Path, default=None)
    parser.add_argument("--mode", choices=sorted(HEALTH_MODES), default="deep")
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = _parse_args(argv)
    config = MonitorConfig(
        campaign_id=args.campaign_id,
        campaign_root=args.campaign_root,
        db_path=args.db,
        tape_root=args.tape_root,
        config_path=args.config,
        activation_ms=args.activation_ms,
        expected_source=args.expected_source,
        quota_bytes=args.quota_bytes,
        health_dir=args.health_dir,
        mode=args.mode,
    )
    receipt, path = run_once(config)
    print(
        json.dumps(
            {"receipt": str(path), "state": receipt["state"], "reasons": receipt["reasons"]},
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
