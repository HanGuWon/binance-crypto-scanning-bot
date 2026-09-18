"""Read-only live storage-rate and runway receipt for the OCI campaign."""

from __future__ import annotations

import argparse
import hashlib
import json
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from signalbot.prospective.source_freeze import default_source_root, freeze_source

GIB = 1024**3


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def build_storage_health(
    *,
    campaign_id: str,
    tape_root: Path,
    activation_ms: int,
    quota_bytes: int,
    transient_finalize_reserve_bytes: int,
    safety_margin_bytes: int,
    expected_source: str,
) -> dict[str, Any]:
    now_ms = int(time.time() * 1000)
    elapsed_h = (now_ms - activation_ms) / 3_600_000
    if elapsed_h <= 0:
        raise ValueError("activation must precede storage measurement")
    source = freeze_source(default_source_root()).source_identity
    markets: dict[str, dict[str, Any]] = {}
    sealed_bytes = 0
    active_partial_bytes = 0
    for market in ("futures", "spot"):
        directory = tape_root / market
        sealed = sorted(directory.glob("*.jsonl.zst")) if directory.is_dir() else []
        partials = (
            sorted(directory.glob("*.jsonl.zst.partial"))
            if directory.is_dir()
            else []
        )
        market_sealed_bytes = sum(path.stat().st_size for path in sealed)
        market_partial_bytes = sum(path.stat().st_size for path in partials)
        sealed_bytes += market_sealed_bytes
        active_partial_bytes += market_partial_bytes
        markets[market] = {
            "sealed_segment_count": len(sealed),
            "sealed_compressed_bytes": market_sealed_bytes,
            "active_partial_count": len(partials),
            "active_partial_bytes": market_partial_bytes,
            "latest_sealed_mtime_utc": (
                datetime.fromtimestamp(
                    max(path.stat().st_mtime for path in sealed), tz=UTC
                ).isoformat()
                if sealed
                else None
            ),
        }
    physical_bytes = sealed_bytes + active_partial_bytes
    conservative_rate = physical_bytes / GIB / elapsed_h
    remaining_bytes = max(quota_bytes - physical_bytes, 0)
    required_bytes = (
        conservative_rate * 48 * GIB
        + transient_finalize_reserve_bytes
        + safety_margin_bytes
    )
    manifest_path = tape_root.parent / "preregistration.json"
    result: dict[str, Any] = {
        "schema_version": "oci_live_storage_health_v1",
        "captured_at_ms": now_ms,
        "captured_at_utc": datetime.fromtimestamp(now_ms / 1000, tz=UTC).isoformat(),
        "campaign_id": campaign_id,
        "activation_ms": activation_ms,
        "elapsed_since_activation_hours": round(elapsed_h, 6),
        "source_identity": source,
        "expected_source_identity": expected_source,
        "source_identity_match": source == expected_source,
        "markets": markets,
        "physical_storage": {
            "sealed_compressed_bytes": sealed_bytes,
            "active_partial_bytes": active_partial_bytes,
            "physical_bytes": physical_bytes,
            "physical_gib": round(physical_bytes / GIB, 6),
            "conservative_physical_rate_gib_per_h": round(conservative_rate, 8),
            "quota_bytes": quota_bytes,
            "quota_gib": round(quota_bytes / GIB, 6),
            "remaining_quota_bytes": remaining_bytes,
            "remaining_quota_gib": round(remaining_bytes / GIB, 6),
        },
        "rollover_gate": {
            "formula": (
                "remaining_quota_gib >= conservative_physical_rate_gib_per_h*48 "
                "+ transient_finalize_reserve_gib + safety_margin_gib"
            ),
            "gate_hours": 48,
            "transient_finalize_reserve_gib": round(
                transient_finalize_reserve_bytes / GIB, 6
            ),
            "safety_margin_gib": round(safety_margin_bytes / GIB, 6),
            "required_bytes": round(required_bytes),
            "required_gib": round(required_bytes / GIB, 6),
            "gate_met": remaining_bytes >= required_bytes,
            "decision": (
                "CONTINUE_PHASE_O5_MONITORING"
                if remaining_bytes >= required_bytes
                else "PLANNED_PHASE_P_ROLLOVER"
            ),
        },
        "preregistration": {
            "path": str(manifest_path),
            "sha256": _sha256(manifest_path) if manifest_path.is_file() else None,
        },
    }
    result["verdict"] = (
        "PASS"
        if result["source_identity_match"]
        and result["rollover_gate"]["gate_met"]
        else "FAIL"
    )
    return result


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--campaign-id", required=True)
    parser.add_argument("--tape-root", type=Path, required=True)
    parser.add_argument("--activation-ms", type=int, required=True)
    parser.add_argument("--quota-bytes", type=int, required=True)
    parser.add_argument("--transient-finalize-reserve-bytes", type=int, required=True)
    parser.add_argument("--safety-margin-bytes", type=int, required=True)
    parser.add_argument("--expected-source", required=True)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    result = build_storage_health(
        campaign_id=args.campaign_id,
        tape_root=args.tape_root,
        activation_ms=args.activation_ms,
        quota_bytes=args.quota_bytes,
        transient_finalize_reserve_bytes=args.transient_finalize_reserve_bytes,
        safety_margin_bytes=args.safety_margin_bytes,
        expected_source=args.expected_source,
    )
    text = json.dumps(result, indent=2, sort_keys=True) + "\n"
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(text, encoding="utf-8")
    print(text, end="")
    return 0 if result["verdict"] == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
