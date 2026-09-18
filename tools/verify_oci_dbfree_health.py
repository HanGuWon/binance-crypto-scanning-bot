# ruff: noqa
"""Independent strict verifier for DB-free health receipt chains."""
from __future__ import annotations
import argparse, hashlib, json, sys
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

SCHEMA = "oci_dbfree_health_receipt_v2"
MODES = {"fast", "deep"}
STATES = {"GREEN", "YELLOW", "RED"}
REQUIRED = {"schema", "mode", "receipt_id", "captured_at_ms", "campaign_id", "expected_source_identity", "observed_source_identity", "source_identity_match", "source_identity_all_new_match", "collector", "markets", "storage", "journal", "manifest_integrity", "growth", "classification", "cursor", "previous_receipt_sha256", "receipt_sha256"}
COLLECTOR_FIELDS = {"unit", "active_state", "sub_state", "main_pid", "n_restarts", "restart_delta", "exec_main_start_timestamp", "result"}
MARKET_FIELDS = {"latest_manifest_path", "latest_segment_path", "latest_partial_path", "latest_received_at_ms", "latest_manifest_mtime_ms", "latest_partial_mtime_ms", "freshness_age_ms", "partial_freshness_age_ms", "manifest_count_delta", "finalized_bytes_delta", "partial_bytes", "actively_advancing", "parsed_manifests", "new_manifests", "malformed_manifests", "missing_segments", "size_mismatches", "hash_mismatches", "predecessor_breaks", "campaign_mismatches", "source_mismatches", "observed_source_identity", "new_source_identities", "all_new_source_match", "scan_complete", "historical_hashed_segments", "historical_hashed_bytes", "healthy"}
CURSOR_FIELDS = {"cursor_schema", "captured_at_ms", "journal_cursor_ms", "markets", "cumulative_finalized_bytes", "partial_sizes", "receipt_sequence"}

def canonical(value: Mapping[str, Any]) -> bytes:
    normalized = dict(value); normalized["receipt_sha256"] = None
    return json.dumps(normalized, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False).encode("utf-8")

def _load(path: Path) -> Mapping[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict): raise ValueError("object expected")
    return value

def _int(value: Any) -> bool: return isinstance(value, int) and not isinstance(value, bool)
def _nullable_int(value: Any) -> bool: return value is None or _int(value)
def _nullable_str(value: Any) -> bool: return value is None or isinstance(value, str)
def _valid_nested(value: Mapping[str, Any], fields: set[str], label: str, errors: list[str], path: str) -> None:
    missing = fields - set(value)
    if missing: errors.append(f"{path}: missing fields {sorted(missing)}")
    if not value: errors.append(f"{path}: empty object")

def _validate_record(value: Mapping[str, Any], mode: str, path: str, errors: list[str]) -> None:
    missing = REQUIRED - set(value)
    if missing: errors.append(f"{path}: missing fields {sorted(missing)}"); return
    if value.get("schema") != SCHEMA or value.get("mode") != mode: errors.append(f"{path}: schema/mode mismatch")
    captured, receipt_id = value.get("captured_at_ms"), value.get("receipt_id")
    if not _int(captured) or not isinstance(receipt_id, str): errors.append(f"{path}: invalid id/time type")
    elif Path(path).stem != f"{mode}-v2-{captured}": errors.append(f"{path}: filename/time mismatch")
    if not isinstance(value.get("campaign_id"), str): errors.append(f"{path}: invalid campaign_id")
    if not isinstance(value.get("expected_source_identity"), str): errors.append(f"{path}: invalid expected source")
    if not _nullable_str(value.get("observed_source_identity")): errors.append(f"{path}: invalid observed source")
    if value.get("source_identity_match") is not None and not isinstance(value.get("source_identity_match"), bool): errors.append(f"{path}: invalid source match")
    if value.get("source_identity_all_new_match") is not None and not isinstance(value.get("source_identity_all_new_match"), bool): errors.append(f"{path}: invalid all-new source match")
    collector = value.get("collector")
    if not isinstance(collector, dict): errors.append(f"{path}: collector object required")
    else:
        _valid_nested(collector, COLLECTOR_FIELDS, "collector", errors, path)
        if not isinstance(collector.get("unit"), str): errors.append(f"{path}: collector.unit invalid")
        for key in ("main_pid", "n_restarts", "restart_delta"):
            if not _nullable_int(collector.get(key)): errors.append(f"{path}: collector.{key} invalid")
    markets = value.get("markets")
    if not isinstance(markets, dict) or set(markets) != {"spot", "futures"}: errors.append(f"{path}: market contract mismatch")
    else:
        for market in ("spot", "futures"):
            item = markets[market]
            if not isinstance(item, dict): errors.append(f"{path}: markets.{market} object required"); continue
            _valid_nested(item, MARKET_FIELDS, "market", errors, f"{path}:markets.{market}")
            for key in ("latest_received_at_ms", "latest_manifest_mtime_ms", "latest_partial_mtime_ms", "freshness_age_ms", "partial_freshness_age_ms"):
                if not _nullable_int(item.get(key)): errors.append(f"{path}: markets.{market}.{key} invalid")
            for key in ("actively_advancing", "scan_complete", "healthy"):
                if not isinstance(item.get(key), bool): errors.append(f"{path}: markets.{market}.{key} invalid")
            if not isinstance(item.get("new_source_identities"), list) or not all(isinstance(x, str) for x in item.get("new_source_identities", [])): errors.append(f"{path}: markets.{market}.new_source_identities invalid")
    for section in ("storage", "journal", "manifest_integrity", "growth"):
        if not isinstance(value.get(section), dict) or not value.get(section): errors.append(f"{path}: {section} object required")
    classification = value.get("classification")
    if not isinstance(classification, dict) or set(classification) != {"state", "reasons"} or classification.get("state") not in STATES or not isinstance(classification.get("reasons"), list) or not all(isinstance(x, str) for x in classification.get("reasons", [])): errors.append(f"{path}: classification contract mismatch")
    cursor = value.get("cursor")
    if not isinstance(cursor, dict): errors.append(f"{path}: cursor object required")
    else:
        _valid_nested(cursor, CURSOR_FIELDS, "cursor", errors, path)
        if cursor.get("cursor_schema") != "v2.1" or not _int(cursor.get("captured_at_ms")) or not _int(cursor.get("journal_cursor_ms")) or not isinstance(cursor.get("markets"), dict) or not _int(cursor.get("cumulative_finalized_bytes")) or not isinstance(cursor.get("partial_sizes"), dict) or not _int(cursor.get("receipt_sequence")): errors.append(f"{path}: cursor types invalid")
    digest = value.get("receipt_sha256")
    if not isinstance(digest, str) or hashlib.sha256(canonical(value)).hexdigest() != digest: errors.append(f"{path}: canonical hash mismatch")

def verify_receipts(health_dir: Path, mode: str, latest_only: bool = False) -> list[str]:
    if mode not in MODES: return ["mode must be fast or deep"]
    if not health_dir.is_dir(): return ["health directory missing"]
    paths = sorted(p for p in health_dir.glob(f"{mode}-v2-*.json") if p.is_file())
    if not paths: return ["health directory contains no receipts"]
    check_paths = paths if not latest_only else paths[max(0, len(paths) - 2):]
    errors: list[str] = []; previous_hash: str | None = None; previous_time: int | None = None; seen_ids: set[str] = set(); campaign: str | None = None; source: str | None = None
    if latest_only and len(paths) > 2:
        try:
            prior = _load(paths[-3]); previous_hash = prior.get("receipt_sha256") if isinstance(prior.get("receipt_sha256"), str) else None; previous_time = prior.get("captured_at_ms") if _int(prior.get("captured_at_ms")) else None
        except (OSError, ValueError, json.JSONDecodeError, TypeError): errors.append(f"{paths[-3].name}: predecessor unreadable")
    for path in check_paths:
        try:
            value = _load(path); _validate_record(value, mode, path.name, errors)
            rid, captured = value.get("receipt_id"), value.get("captured_at_ms")
            if isinstance(rid, str):
                if rid in seen_ids: errors.append(f"{path.name}: duplicate receipt id")
                seen_ids.add(rid)
            captured_int = captured if isinstance(captured, int) and not isinstance(captured, bool) else None
            if captured_int is not None:
                if previous_time is not None and captured_int <= previous_time: errors.append(f"{path.name}: non-monotonic timestamp")
                previous_time = captured_int
            if campaign is None: campaign = value.get("campaign_id")
            elif value.get("campaign_id") != campaign: errors.append(f"{path.name}: campaign mismatch")
            if source is None: source = value.get("expected_source_identity")
            elif value.get("expected_source_identity") != source: errors.append(f"{path.name}: source mismatch")
            if value.get("previous_receipt_sha256") != previous_hash: errors.append(f"{path.name}: predecessor hash mismatch")
            previous_hash = value.get("receipt_sha256") if isinstance(value.get("receipt_sha256"), str) else None
        except (OSError, ValueError, json.JSONDecodeError, TypeError, KeyError) as exc: errors.append(f"{path.name}: {exc}")
    return errors

def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__); parser.add_argument("--health-dir", type=Path, required=True); parser.add_argument("--mode", choices=sorted(MODES), required=True); parser.add_argument("--latest-only", action="store_true"); return parser.parse_args(argv)

def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv); errors = verify_receipts(args.health_dir, args.mode, args.latest_only)
    if errors:
        for error in errors: print(error, file=sys.stderr)
        return 1
    print("valid"); return 0

if __name__ == "__main__": raise SystemExit(main())
