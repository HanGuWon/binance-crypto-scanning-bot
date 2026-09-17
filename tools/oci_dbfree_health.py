# ruff: noqa
"""DB-free operational health receipts for the Phase-R collector (V2.1)."""
from __future__ import annotations
import argparse, hashlib, json, os, shutil, subprocess, sys, tempfile, time
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

SCHEMA = "oci_dbfree_health_receipt_v2"
MODES = {"fast", "deep"}
MARKETS = ("spot", "futures")
CLASSIFICATIONS = {"GREEN", "YELLOW", "RED"}
MAX_FAST_SCAN = 128
MAX_CURSOR_OBSERVATIONS = 96
DEFAULT_DEEP_SEGMENTS = 256
DEFAULT_DEEP_BYTES = 64 * 1024 * 1024
GIB = 1024**3

class HealthError(RuntimeError):
    """Raised for invalid input or an unrecoverable health operation."""

class CommandRunner:
    def run(self, args: Sequence[str]) -> str:
        completed = subprocess.run(list(args), check=True, capture_output=True, text=True, encoding="utf-8")
        return completed.stdout

@dataclass(frozen=True)
class CollectorState:
    unit: str
    active_state: str | None
    sub_state: str | None
    main_pid: int | None
    n_restarts: int | None
    restart_delta: int | None
    exec_main_start_timestamp: str | None
    result: str | None

@dataclass(frozen=True)
class ManifestObservation:
    market: str
    manifest_path: Path
    segment_path: Path | None
    partial_path: Path | None
    latest_received_at_ms: int | None
    manifest_mtime_ms: int
    partial_mtime_ms: int | None
    parsed: bool
    campaign_match: bool
    source_match: bool
    declared_size: int | None
    actual_size: int | None
    declared_hash: str | None
    actual_hash: str | None
    predecessor: str | None
    predecessor_break: bool
    errors: tuple[str, ...]
    source_identity: str | None = None
    previous_segment_sha256: str | None = None
    segment_sequence: int | None = None

@dataclass(frozen=True)
class FileSystemStats:
    path: str
    total_bytes: int | None
    free_bytes: int | None
    free_inodes: int | None
    device_id: int | None

def _now_ms() -> int: return int(time.time() * 1000)
def _canonical(value: Mapping[str, Any]) -> bytes:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False).encode("utf-8")
def _sha256_bytes(value: bytes) -> str: return hashlib.sha256(value).hexdigest()
def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""): digest.update(block)
    return digest.hexdigest()
def _mtime_ms(path: Path) -> int: return int(path.stat().st_mtime_ns // 1_000_000)
def _as_int(value: Any) -> int | None:
    if value is None or isinstance(value, bool): return None
    try: return int(value)
    except (TypeError, ValueError): return None

def _parse_systemd(text: str, unit: str, previous_restarts: int | None) -> CollectorState:
    values: dict[str, str] = {}
    for line in text.splitlines():
        if "=" in line:
            key, value = line.split("=", 1); values[key] = value
    restarts = _as_int(values.get("NRestarts"))
    delta = None if restarts is None or previous_restarts is None else max(0, restarts - previous_restarts)
    return CollectorState(unit, values.get("ActiveState"), values.get("SubState"), _as_int(values.get("MainPID")), restarts, delta, values.get("ExecMainStartTimestamp") or None, values.get("Result") or None)

def read_collector_state(unit: str, runner: CommandRunner, previous_restarts: int | None = None) -> CollectorState:
    output = runner.run(["systemctl", "show", unit, "--no-pager", "--property=ActiveState,SubState,MainPID,NRestarts,ExecMainStartTimestamp,Result"])
    return _parse_systemd(output, unit, previous_restarts)

def _manifest_segment_path(payload: Mapping[str, Any], manifest: Path) -> Path:
    name = payload.get("compressed_file_name") or payload.get("segment_file_name")
    if isinstance(name, str) and name:
        return manifest.parent / name
    stem = manifest.name.removesuffix(".manifest.json")
    standard = manifest.with_name(stem + ".jsonl.zst")
    return standard if standard.exists() else manifest.with_name(stem)
def _find_partial(directory: Path, segment: Path | None) -> Path | None:
    if segment is not None and segment.with_name(segment.name + ".partial").is_file(): return segment.with_name(segment.name + ".partial")
    partials = sorted((p for p in directory.glob("*.jsonl.zst.partial") if p.is_file()), key=lambda p: p.stat().st_mtime_ns)
    return partials[-1] if partials else None
def _load_json(path: Path) -> Mapping[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict): raise ValueError("manifest must be a JSON object")
    return value

def inspect_market(market: str, tape_root: Path, campaign_id: str, expected_source: str, captured_at_ms: int,
                   previous_manifest: str | None, deep: bool, hash_new: bool = True,
                   max_historical_segments: int = DEFAULT_DEEP_SEGMENTS, max_historical_bytes: int = DEFAULT_DEEP_BYTES,
                   cursor_summary: Mapping[str, Any] | None = None) -> tuple[dict[str, Any], list[ManifestObservation]]:
    directory = tape_root / market
    manifests = sorted(directory.glob("*.manifest.json"), key=lambda p: p.name)
    scan_complete = True
    if len(manifests) > MAX_FAST_SCAN and not deep:
        manifests = manifests[-MAX_FAST_SCAN:]; scan_complete = False
    prior_name = previous_manifest
    observations: list[ManifestObservation] = []
    historical_hashed = 0; historical_bytes = 0; new_hashed = 0; new_bytes = 0
    for index, manifest in enumerate(manifests):
        is_new = prior_name is None or manifest.name > prior_name
        try:
            payload = _load_json(manifest)
            segment = _manifest_segment_path(payload, manifest)
            partial = _find_partial(directory, segment)
            received = _as_int(payload.get("last_received_at_ms"))
            declared_size = _as_int(payload.get("compressed_bytes"))
            declared_hash = payload.get("compressed_file_sha256") if isinstance(payload.get("compressed_file_sha256"), str) else None
            source_identity = payload.get("source_identity") if isinstance(payload.get("source_identity"), str) else None
            predecessor_hash = payload.get("previous_segment_sha256") if isinstance(payload.get("previous_segment_sha256"), str) else None
            sequence = _as_int(payload.get("segment_sequence"))
            actual_size = segment.stat().st_size if segment.is_file() else None
            should_hash = bool(segment.is_file() and hash_new and is_new)
            if deep and is_new and (new_hashed >= max_historical_segments or new_bytes + (actual_size or 0) > max_historical_bytes):
                should_hash = False
            if should_hash and is_new and deep:
                new_hashed += 1; new_bytes += actual_size or 0
            if deep and segment.is_file() and not is_new and historical_hashed < max_historical_segments and historical_bytes + (actual_size or 0) <= max_historical_bytes:
                should_hash = True; historical_hashed += 1; historical_bytes += actual_size or 0
            actual_hash = _sha256_file(segment) if should_hash else None
            errors: list[str] = []
            if payload.get("campaign_id") != campaign_id: errors.append("campaign_mismatch")
            if source_identity != expected_source: errors.append("source_mismatch")
            if not segment.is_file(): errors.append("missing_segment")
            if declared_size is not None and actual_size is not None and declared_size != actual_size: errors.append("size_mismatch")
            if declared_hash and actual_hash and declared_hash != actual_hash: errors.append("hash_mismatch")
            previous_obs = observations[-1] if observations else None
            if index > 0:
                if predecessor_hash is None: errors.append("predecessor_missing")
                elif previous_obs is None or previous_obs.declared_hash is None or predecessor_hash != (previous_obs.actual_hash or previous_obs.declared_hash): errors.append("predecessor_mismatch")
                if sequence is not None and previous_obs and previous_obs.segment_sequence is not None and sequence != previous_obs.segment_sequence + 1: errors.append("sequence_break")
            elif predecessor_hash is not None and prior_name is None and sequence == 1: errors.append("genesis_predecessor")
            observations.append(ManifestObservation(market, manifest, segment, partial, received, _mtime_ms(manifest), _mtime_ms(partial) if partial else None, True,
                "campaign_mismatch" not in errors, "source_mismatch" not in errors, declared_size, actual_size, declared_hash, actual_hash,
                None, any(e in errors for e in ("predecessor_missing", "predecessor_mismatch", "sequence_break", "genesis_predecessor")), tuple(errors), source_identity, predecessor_hash, sequence))
        except (OSError, ValueError, json.JSONDecodeError, TypeError) as exc:
            observations.append(ManifestObservation(market, manifest, None, _find_partial(directory, None), None, _mtime_ms(manifest), None, False, False, False, None, None, None, None, None, False, (f"malformed_manifest:{type(exc).__name__}",)))
    latest = observations[-1] if observations else None
    new = [item for item in observations if prior_name is None or item.manifest_path.name > prior_name]
    latest_received = max((item.latest_received_at_ms for item in observations if item.latest_received_at_ms is not None), default=None)
    latest_partial = latest.partial_path if latest else _find_partial(directory, None)
    errors = [error for item in new for error in item.errors]
    previous_partial_size = _as_int((cursor_summary or {}).get("partial_sizes", {}).get(market))
    current_partial_size = latest_partial.stat().st_size if latest_partial and latest_partial.is_file() else 0
    previous_marker = ((cursor_summary or {}).get("markets", {}).get(market) or {})
    marker_changed = (latest is not None and latest.manifest_path.name != previous_marker.get("last_manifest")) or latest_received != _as_int(previous_marker.get("latest_received_at_ms"))
    if previous_partial_size is not None:
        marker_changed = marker_changed or current_partial_size != previous_partial_size
    elif not previous_marker and previous_manifest is None:
        marker_changed = bool(latest or latest_partial)
    source_values = [item.source_identity for item in new if item.source_identity is not None]
    observed_source = latest.source_identity if latest else None
    result = {
        "latest_manifest_path": str(latest.manifest_path) if latest else None,
        "latest_segment_path": str(latest.segment_path) if latest and latest.segment_path else None,
        "latest_partial_path": str(latest_partial) if latest_partial else None,
        "latest_received_at_ms": latest_received,
        "latest_manifest_mtime_ms": _mtime_ms(latest.manifest_path) if latest else None,
        "latest_partial_mtime_ms": _mtime_ms(latest_partial) if latest_partial else None,
        "freshness_age_ms": None if latest_received is None else max(0, captured_at_ms - latest_received),
        "partial_freshness_age_ms": None if latest_partial is None else max(0, captured_at_ms - _mtime_ms(latest_partial)),
        "manifest_count_delta": len(new), "finalized_bytes_delta": sum(item.actual_size or 0 for item in new if item.segment_path and item.segment_path.is_file()),
        "partial_bytes": current_partial_size, "actively_advancing": bool(marker_changed and (latest is not None or latest_partial is not None)),
        "parsed_manifests": sum(item.parsed for item in new), "new_manifests": len(new), "malformed_manifests": sum(not item.parsed for item in new),
        "missing_segments": errors.count("missing_segment"), "size_mismatches": errors.count("size_mismatch"), "hash_mismatches": errors.count("hash_mismatch"),
        "predecessor_breaks": sum(error in {"predecessor_missing", "predecessor_mismatch", "sequence_break", "genesis_predecessor"} for error in errors),
        "campaign_mismatches": errors.count("campaign_mismatch"), "source_mismatches": errors.count("source_mismatch"),
        "observed_source_identity": observed_source, "new_source_identities": sorted(set(source_values)), "all_new_source_match": bool(source_values) and all(item == expected_source for item in source_values) if source_values else None,
        "scan_complete": scan_complete, "historical_hashed_segments": historical_hashed, "historical_hashed_bytes": historical_bytes, "healthy": not errors,
    }
    return result, observations

def _statvfs(path: Path) -> FileSystemStats:
    try:
        statvfs = getattr(os, "statvfs", None)
        if statvfs is None: raise AttributeError("statvfs unavailable")
        stat = statvfs(path); return FileSystemStats(str(path), stat.f_blocks * stat.f_frsize, stat.f_bavail * stat.f_frsize, stat.f_favail, path.stat().st_dev)
    except (AttributeError, OSError):
        try:
            usage = shutil.disk_usage(path); return FileSystemStats(str(path), usage.total, usage.free, None, path.stat().st_dev)
        except OSError: return FileSystemStats(str(path), None, None, None, None)

def _tree_bytes(root: Path, limit: int = 20_000) -> int | None:
    total, seen = 0, 0
    try:
        for path in root.rglob("*"):
            if path.is_file():
                total += path.stat().st_size; seen += 1
                if seen >= limit: break
        return total
    except OSError: return None

def inspect_storage(campaign_root: Path, data_mount: Path, health_dir: Path) -> dict[str, Any]:
    root, data = _statvfs(Path("/")), _statvfs(data_mount)
    campaign_bytes, health_bytes = _tree_bytes(campaign_root), _tree_bytes(health_dir)
    return {"root_path": root.path, "root_total_bytes": root.total_bytes, "root_free_bytes": root.free_bytes, "root_free_inodes": root.free_inodes,
        "data_path": data.path, "data_total_bytes": data.total_bytes, "data_free_bytes": data.free_bytes, "data_free_inodes": data.free_inodes,
        "same_filesystem": root.device_id is not None and root.device_id == data.device_id, "campaign_bytes": campaign_bytes, "campaign_bytes_complete": campaign_bytes is not None and campaign_bytes < 20_000,
        "health_bytes": health_bytes, "health_bytes_complete": health_bytes is not None and health_bytes < 20_000}

def read_journal(unit: str, runner: CommandRunner, since_ms: int | None, captured_at_ms: int) -> dict[str, Any]:
    args = ["journalctl", "-u", unit, "--no-pager", "--output=cat"]
    if since_ms is not None: args.extend(["--since", datetime.fromtimestamp(since_ms / 1000, tz=UTC).isoformat()])
    try: text = runner.run(args)
    except (OSError, subprocess.CalledProcessError): text = ""
    lower = text.lower(); patterns = {"database_locked_count": "database is locked", "database_busy_count": "database is busy", "oom_count": "out of memory", "enospc_count": "no space left on device", "io_error_count": "i/o error", "collector_unexpected_exit_count": "unexpected exit", "raw_recorder_fatal_count": "raw-recorder fatal"}
    counts = {key: lower.count(value) for key, value in patterns.items()}
    critical = [line for line in text.splitlines() if any(token in line.lower() for token in ("out of memory", "no space left", "i/o error", "unexpected exit", "raw-recorder fatal"))]
    return {"window_start_ms": since_ms, "window_end_ms": captured_at_ms, **counts, "latest_critical_event_ms": None if not critical else captured_at_ms}

def _blank_cursor() -> dict[str, Any]:
    return {"cursor_schema": "v2.1", "previous_capture_ms": None, "journal_cursor": None, "n_restarts": None, "observations": [], "cumulative_finalized_bytes": 0, "partial_sizes": {}, "markets": {}, "receipt_sequence": 0}

def _receipt_digest(value: Mapping[str, Any]) -> str | None:
    digest = value.get("receipt_sha256")
    normalized = dict(value)
    normalized["receipt_sha256"] = None
    return digest if isinstance(digest, str) and _sha256_bytes(_canonical(normalized)) == digest else None

def _verified_receipts(health_dir: Path, mode: str) -> list[Mapping[str, Any]]:
    files = sorted(p for p in health_dir.glob(f"{mode}-v2-*.json") if p.is_file())
    records: list[Mapping[str, Any]] = []; previous: str | None = None
    for path in files:
        try: value = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, ValueError, json.JSONDecodeError) as exc: raise HealthError(f"corrupt receipt {path.name}") from exc
        if (not isinstance(value, dict) or value.get("schema") != SCHEMA or value.get("mode") != mode or value.get("receipt_id") != path.stem or _receipt_digest(value) is None or value.get("previous_receipt_sha256") != previous): raise HealthError(f"receipt chain invalid: {path.name}")
        records.append(value); previous = value["receipt_sha256"]
    return records

def _latest_receipt(health_dir: Path, mode: str) -> tuple[Path, Mapping[str, Any]] | None:
    records = _verified_receipts(health_dir, mode)
    if not records: return None
    value = records[-1]; return health_dir / f"{mode}-v2-{value['captured_at_ms']}.json", value

def _load_cursor(path: Path, health_dir: Path | None = None, mode: str | None = None) -> dict[str, Any]:
    if path.is_file():
        try:
            value = json.loads(path.read_text(encoding="utf-8"))
            if isinstance(value, dict) and isinstance(value.get("observations", []), list): return value
        except (OSError, ValueError, json.JSONDecodeError, TypeError): pass
    if health_dir is not None and mode is not None:
        try:
            latest = _latest_receipt(health_dir, mode)
            if latest and isinstance(latest[1].get("cursor"), dict):
                recovered = dict(latest[1]["cursor"]); recovered["recovered_from_receipt"] = True; return recovered
        except HealthError: raise
    return _blank_cursor()

def _atomic_json(path: Path, value: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True); fd, temp_name = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            json.dump(value, handle, indent=2, sort_keys=True, ensure_ascii=False, allow_nan=False); handle.write("\n"); handle.flush(); os.fsync(handle.fileno())
        os.replace(temp_name, path)
    finally:
        if os.path.exists(temp_name): os.unlink(temp_name)

def _growth(cursor: Mapping[str, Any], captured_at_ms: int, physical_bytes: int, free_bytes: int | None) -> dict[str, Any]:
    history = [item for item in cursor.get("observations", []) if isinstance(item, dict)]
    history.append({"captured_at_ms": captured_at_ms, "physical_bytes": physical_bytes}); history = history[-MAX_CURSOR_OBSERVATIONS:]
    rates: list[float] = []; sample_times: list[int] = []
    for previous in history[:-1]:
        previous_time, previous_bytes = _as_int(previous.get("captured_at_ms")), _as_int(previous.get("physical_bytes"))
        if previous_time is not None and previous_bytes is not None and captured_at_ms > previous_time and physical_bytes >= previous_bytes:
            hours = (captured_at_ms - previous_time) / 3_600_000; rates.append((physical_bytes - previous_bytes) / hours); sample_times.append(previous_time)
    maximum = max(rates) if rates else None; conservative = maximum
    projected = {f"projected_free_{days}d_bytes": None if free_bytes is None or conservative is None else int(free_bytes - conservative * 24 * days) for days in (7, 14, 30, 60)}
    return {"sample_count": len(rates), "window_hours": None if not sample_times else round((captured_at_ms - min(sample_times)) / 3_600_000, 6), "mean_bytes_per_hour": sum(rates) / len(rates) if rates else None,
        "max_observed_bytes_per_hour": maximum, "conservative_bytes_per_hour": conservative, "runway_hours": None if free_bytes is None or conservative in (None, 0) else free_bytes / conservative,
        "projected_free_48h_bytes": None if free_bytes is None or conservative is None else int(free_bytes - conservative * 48), "complete": bool(cursor.get("growth_complete", True)), **projected, "observations": history}

def classify(collector: CollectorState, markets: Mapping[str, Mapping[str, Any]], storage: Mapping[str, Any], journal: Mapping[str, Any], manifest_integrity: Mapping[str, Any], growth: Mapping[str, Any], mode: str) -> dict[str, Any]:
    reasons: list[str] = []
    if collector.active_state != "active" or collector.sub_state not in {"running", "listening"}: reasons.append("COLLECTOR_NOT_ACTIVE")
    if not bool(manifest_integrity.get("healthy")): reasons.append("MANIFEST_INTEGRITY_FAILURE")
    if any(int(journal.get(k, 0) or 0) for k in ("oom_count", "enospc_count", "io_error_count", "collector_unexpected_exit_count", "raw_recorder_fatal_count")): reasons.append("CRITICAL_JOURNAL_EVENT")
    if collector.restart_delta is not None and collector.restart_delta > 1: reasons.append("RESTART_BURST")
    threshold = 26 * 3_600_000 if mode == "deep" else 2 * 3_600_000
    stale = [v.get("freshness_age_ms") is None or v["freshness_age_ms"] > threshold for v in markets.values()]
    if len(stale) != 2 or all(stale): reasons.append("MARKET_DATA_STALE")
    elif any(stale): reasons.append("MARKET_DATA_PARTIAL_STALE")
    runway = growth.get("runway_hours")
    if runway is None: reasons.append("RUNWAY_UNKNOWN")
    elif runway < 24: reasons.append("RUNWAY_CRITICALLY_SHORT")
    elif runway < 48: reasons.append("RUNWAY_BELOW_48H")
    if not growth.get("complete", True): reasons.append("GROWTH_TRUNCATED")
    if reasons and any(code in reasons for code in ("COLLECTOR_NOT_ACTIVE", "MANIFEST_INTEGRITY_FAILURE", "CRITICAL_JOURNAL_EVENT", "MARKET_DATA_STALE", "RUNWAY_CRITICALLY_SHORT")): state = "RED"
    elif reasons: state = "YELLOW"
    else: state = "GREEN"
    return {"state": state, "reasons": reasons}

def verify_source_inventory(path: Path, release_root: Path, expected: str) -> dict[str, Any]:
    try: payload = _load_json(path)
    except (OSError, ValueError, json.JSONDecodeError, TypeError) as exc: raise HealthError("source inventory unreadable") from exc
    files = payload.get("files")
    if not isinstance(files, list): raise HealthError("source inventory files missing")
    missing, drift = [], []
    for item in files:
        if not isinstance(item, dict) or not isinstance(item.get("path"), str) or not isinstance(item.get("sha256"), str): raise HealthError("source inventory entry invalid")
        target = release_root / item["path"]
        if not target.is_file(): missing.append(item["path"])
        elif _sha256_file(target) != item["sha256"]: drift.append(item["path"])
    observed = payload.get("source_identity") if isinstance(payload.get("source_identity"), str) else None
    return {"observed_source_identity": observed, "source_identity_match": observed == expected and not missing and not drift, "missing": missing, "drift": drift}

def publish_receipt(health_dir: Path, mode: str, receipt: dict[str, Any]) -> Path:
    health_dir.mkdir(parents=True, exist_ok=True); receipt["receipt_sha256"] = None; receipt["receipt_sha256"] = _sha256_bytes(_canonical(receipt))
    path = health_dir / f"{mode}-v2-{receipt['captured_at_ms']}.json"
    try:
        with path.open("x", encoding="utf-8") as handle: json.dump(receipt, handle, indent=2, sort_keys=True, ensure_ascii=False, allow_nan=False); handle.write("\n"); handle.flush(); os.fsync(handle.fileno())
    except FileExistsError as exc: raise HealthError(f"receipt already exists: {path.name}") from exc
    return path

def build_receipt(args: argparse.Namespace, runner: CommandRunner) -> tuple[dict[str, Any], Path, Path]:
    if args.mode not in MODES: raise HealthError("mode must be fast or deep")
    captured = args.captured_at_ms if args.captured_at_ms is not None else _now_ms()
    health_dir = args.campaign_root / ("health/phase-fast-v2" if args.mode == "fast" else "health/phase-deep-v2")
    if args.health_dir != health_dir: raise HealthError("health-dir must match the selected mode")
    cursor_path = health_dir / "cursor-v1.json"; cursor = _load_cursor(cursor_path, health_dir, args.mode)
    collector = read_collector_state(args.collector_unit, runner, _as_int(cursor.get("n_restarts")))
    markets: dict[str, dict[str, Any]] = {}; all_obs: dict[str, list[ManifestObservation]] = {}
    for market in MARKETS:
        markets[market], all_obs[market] = inspect_market(market, args.tape_root, args.campaign_id, args.expected_source, captured, ((cursor.get("markets") or {}).get(market) or {}).get("last_manifest") or cursor.get(f"{market}_last_manifest"), args.mode == "deep", True, getattr(args, "deep_max_historical_segments", DEFAULT_DEEP_SEGMENTS), getattr(args, "deep_max_historical_bytes", DEFAULT_DEEP_BYTES), cursor)
    storage = inspect_storage(args.campaign_root, args.data_mount, health_dir)
    cumulative = _as_int(cursor.get("cumulative_finalized_bytes")) or 0; cumulative += sum(int(markets[m].get("finalized_bytes_delta", 0) or 0) for m in MARKETS)
    partial_sizes = {m: int(markets[m].get("partial_bytes", 0) or 0) for m in MARKETS}; physical = cumulative + sum(partial_sizes.values())
    growth = _growth(cursor, captured, physical, _as_int(storage.get("data_free_bytes"))); growth_complete = all(bool(markets[m].get("scan_complete")) for m in MARKETS); growth["complete"] = growth_complete
    journal = read_journal(args.collector_unit, runner, _as_int(cursor.get("journal_cursor")), captured)
    integrity = {"healthy": all(bool(markets[m].get("healthy")) for m in MARKETS), "db_integrity": "DEFERRED_TO_COORDINATED_SNAPSHOT", "parsed_manifests": sum(int(markets[m].get("parsed_manifests", 0)) for m in MARKETS), "new_manifests": sum(int(markets[m].get("new_manifests", 0)) for m in MARKETS), "malformed_manifests": sum(int(markets[m].get("malformed_manifests", 0)) for m in MARKETS), "missing_segments": sum(int(markets[m].get("missing_segments", 0)) for m in MARKETS), "size_mismatches": sum(int(markets[m].get("size_mismatches", 0)) for m in MARKETS), "hash_mismatches": sum(int(markets[m].get("hash_mismatches", 0)) for m in MARKETS), "predecessor_breaks": sum(int(markets[m].get("predecessor_breaks", 0)) for m in MARKETS), "campaign_mismatches": sum(int(markets[m].get("campaign_mismatches", 0)) for m in MARKETS), "source_mismatches": sum(int(markets[m].get("source_mismatches", 0)) for m in MARKETS)}
    source_values = [markets[m].get("observed_source_identity") for m in MARKETS if markets[m].get("observed_source_identity")]
    observed_source = source_values[-1] if source_values else None; source_match: bool | None = None if not source_values else all(value == args.expected_source for value in source_values)
    all_new_source_match = all(markets[m].get("all_new_source_match") is not False for m in MARKETS) if any(markets[m].get("all_new_source_match") is not None for m in MARKETS) else None
    inventory = None
    if args.mode == "deep" and getattr(args, "source_inventory", None) and getattr(args, "release_root", None): inventory = verify_source_inventory(args.source_inventory, args.release_root, args.expected_source); source_match = bool(source_match and inventory["source_identity_match"])
    classification = classify(collector, markets, storage, journal, integrity, growth, args.mode); previous = _latest_receipt(health_dir, args.mode)
    cursor_summary = {"cursor_schema": "v2.1", "captured_at_ms": captured, "journal_cursor_ms": captured, "markets": {m: {"last_manifest": all_obs[m][-1].manifest_path.name if all_obs[m] else ((cursor.get("markets") or {}).get(m) or {}).get("last_manifest"), "latest_received_at_ms": markets[m].get("latest_received_at_ms")} for m in MARKETS}, "cumulative_finalized_bytes": cumulative, "partial_sizes": partial_sizes, "receipt_sequence": (_as_int(cursor.get("receipt_sequence")) or 0) + 1}
    receipt: dict[str, Any] = {"schema": SCHEMA, "mode": args.mode, "receipt_id": f"{args.mode}-v2-{captured}", "captured_at_ms": captured, "campaign_id": args.campaign_id, "expected_source_identity": args.expected_source, "observed_source_identity": observed_source, "source_identity_match": source_match, "source_identity_all_new_match": all_new_source_match, "collector": {"unit": collector.unit, "active_state": collector.active_state, "sub_state": collector.sub_state, "main_pid": collector.main_pid, "n_restarts": collector.n_restarts, "restart_delta": collector.restart_delta, "exec_main_start_timestamp": collector.exec_main_start_timestamp, "result": collector.result}, "markets": markets, "storage": storage, "journal": journal, "manifest_integrity": integrity, "growth": {k: v for k, v in growth.items() if k != "observations"}, "classification": classification, "cursor": cursor_summary, "previous_receipt_sha256": previous[1].get("receipt_sha256") if previous else None, "receipt_sha256": None}
    receipt_path = publish_receipt(health_dir, args.mode, receipt)
    next_cursor = dict(cursor_summary); next_cursor.update({"previous_capture_ms": captured, "journal_cursor": captured, "n_restarts": collector.n_restarts, "growth_complete": growth_complete, "observations": growth["observations"]}); _atomic_json(cursor_path, next_cursor)
    return receipt, receipt_path, cursor_path

def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__); parser.add_argument("--mode", choices=sorted(MODES), required=True); parser.add_argument("--campaign-id", required=True); parser.add_argument("--campaign-root", type=Path, required=True); parser.add_argument("--tape-root", type=Path, required=True); parser.add_argument("--config", type=Path, required=True); parser.add_argument("--expected-source", required=True); parser.add_argument("--health-dir", type=Path, required=True); parser.add_argument("--collector-unit", required=True); parser.add_argument("--data-mount", type=Path, required=True); parser.add_argument("--captured-at-ms", type=int); parser.add_argument("--source-inventory", type=Path); parser.add_argument("--release-root", type=Path); parser.add_argument("--deep-max-historical-segments", type=int, default=DEFAULT_DEEP_SEGMENTS); parser.add_argument("--deep-max-historical-bytes", type=int, default=DEFAULT_DEEP_BYTES); return parser.parse_args(argv)

def main(argv: Sequence[str] | None = None, runner: CommandRunner | None = None) -> int:
    try:
        args = parse_args(argv)
        if not args.config.is_file(): raise HealthError(f"config not found: {args.config}")
        receipt, receipt_path, _ = build_receipt(args, runner or CommandRunner()); print(json.dumps({"receipt": str(receipt_path), "classification": receipt["classification"]}, sort_keys=True)); return 0
    except (HealthError, OSError, ValueError, subprocess.CalledProcessError) as exc: print(f"health execution failed: {exc}", file=sys.stderr); return 2

if __name__ == "__main__": raise SystemExit(main())
