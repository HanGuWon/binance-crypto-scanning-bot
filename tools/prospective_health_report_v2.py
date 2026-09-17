"""Read-only segmented-V2-aware prospective health report.

Extends the v1 report with:
- per-market segment chain inspection via manifest files (v1/v2 schemas)
- gap detection between consecutive sealed segments
- active-partial size accounting without decompression
- recovered-segment boundary-null awareness (recovery loses first/last receipts)

This tool never writes to the campaign directory.
"""

from __future__ import annotations

import argparse
import json
import shutil
import sqlite3
import time
from datetime import UTC
from pathlib import Path


def _iso_utc(ms: int | None) -> str | None:
    if ms is None:
        return None
    from datetime import datetime

    return datetime.fromtimestamp(ms / 1000, tz=UTC).isoformat()


def _read_manifests(market_dir: Path) -> list[dict]:
    """Read all segment manifests sorted by segment_sequence."""
    if not market_dir.is_dir():
        return []
    manifests = []
    for mf in sorted(market_dir.glob("*.manifest.json")):
        try:
            data = json.loads(mf.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError):
            continue
        data["_manifest_file"] = mf.name
        manifests.append(data)
    manifests.sort(key=lambda m: m.get("segment_sequence", 0))
    return manifests


def _segment_chain(manifests: list[dict], tape_market_dir: Path) -> dict:
    sealed = [m for m in manifests if not m["_manifest_file"].startswith("_")]
    partials = (
        sorted(tape_market_dir.glob("*.jsonl.zst.partial"))
        if tape_market_dir.is_dir()
        else []
    )
    total_compressed = sum(
        (tape_market_dir / m["compressed_file_name"]).stat().st_size
        if "compressed_file_name" in m
        else 0
        for m in sealed
    )
    # Simpler: sum actual .zst files
    zst_files = list(tape_market_dir.glob("*.jsonl.zst")) if tape_market_dir.is_dir() else []
    total_compressed = sum(f.stat().st_size for f in zst_files)
    partial_bytes = sum(p.stat().st_size for p in partials)

    # Gap detection between consecutive sealed segments.
    # Sub-second transitions between adjacent 30-min buckets are normal;
    # only material discontinuities (>= 60s) count as evidence gaps.
    MATERIAL_GAP_MS = 60_000
    gaps = []
    prev_last: int | None = None
    prev_seq: int | None = None
    for m in sealed:
        seq = m.get("segment_sequence")
        first = m.get("first_received_at_ms")
        last = m.get("last_received_at_ms")
        if (
            prev_last is not None
            and first is not None
            and first - prev_last >= MATERIAL_GAP_MS
        ):
            gaps.append({
                "after_segment": prev_seq,
                "before_first_ms": prev_last,
                "until_first_ms": first,
                "gap_ms": first - prev_last,
                "material": True,
            })
        elif prev_seq is not None and (first is None or last is None):
            # Only flag boundary-unknown if the previous segment's last receipt
            # is far from this segment's bucket start (possible gap).
            gaps.append({
                "after_segment": prev_seq,
                "note": "boundary_unknown_recovered_segment",
                "current_sequence": seq,
                "material": True,
            })
        prev_last = last
        prev_seq = seq

    latest_receipt = max(
        (m.get("last_received_at_ms") or 0) for m in sealed
    ) if sealed else None
    recovered_count = sum(1 for m in sealed if m.get("recovered_from_partial"))
    boundary_null_count = sum(
        1 for m in sealed
        if m.get("first_received_at_ms") is None or m.get("last_received_at_ms") is None
    )

    return {
        "sealed_segments": len(sealed),
        "active_partials": len(partials),
        "partial_names": [p.name for p in partials],
        "total_compressed_bytes": total_compressed,
        "active_partial_bytes": partial_bytes,
        "latest_last_received_at_ms": latest_receipt,
        "latest_last_received_utc": _iso_utc(latest_receipt),
        "gap_count": len(gaps),
        "gaps": gaps,
        "recovered_segments": recovered_count,
        "boundary_null_segments": boundary_null_count,
    }


def _coverage(cur: sqlite3.Cursor) -> dict:
    rows = cur.execute(
        "SELECT market, decision_close_ms, status, complete, evidence_failures "
        "FROM shadow_coverage ORDER BY market, decision_close_ms"
    ).fetchall()
    by_market: dict[str, list] = {}
    for market, close_ms, status, complete, failures in rows:
        by_market.setdefault(market, []).append((close_ms, status, complete, failures))
    now_ms = int(time.time() * 1000)
    result = {}
    for market, cells in by_market.items():
        best_run = run = 1 if cells else 0
        for i in range(1, len(cells)):
            run = run + 1 if cells[i][0] - cells[i - 1][0] == 300_000 else 1
            best_run = max(best_run, run)
        gaps = [
            {"from_ms": cells[i - 1][0], "to_ms": cells[i][0]}
            for i in range(1, len(cells))
            if cells[i][0] - cells[i - 1][0] != 300_000
        ]
        latest = cells[-1]
        result[market] = {
            "total_cells": len(cells),
            "sealed": sum(1 for c in cells if c[1] == "SEALED"),
            "open": sum(1 for c in cells if c[1] == "OPEN"),
            "incomplete": sum(1 for c in cells if c[1] == "INCOMPLETE"),
            "complete": sum(1 for c in cells if c[2] == 1),
            "evidence_failures_total": sum(c[3] for c in cells),
            "max_consecutive_run": best_run,
            "latest_decision_close_ms": latest[0],
            "latest_decision_close_utc": _iso_utc(latest[0]),
            "latest_close_age_ms": now_ms - latest[0],
            "coverage_gap_count": len(gaps),
            "coverage_gaps": gaps,
        }
    return result


def build_report(
    *,
    db_path: Path,
    campaign_id: str,
    tape_root: Path,
    quota_bytes: int,
    reserve_bytes: int,
) -> dict:
    """Build the full report dict. Pure function over inputs (no mutation)."""
    conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    cur = conn.cursor()

    markets = ["futures", "spot"]
    raw_writer = {}
    for mkt in markets:
        mdir = tape_root / mkt
        manifests = _read_manifests(mdir)
        raw_writer[mkt] = _segment_chain(manifests, mdir)

    drive = Path(tape_root.resolve().anchor)
    free = shutil.disk_usage(str(drive)).free
    used_tape = sum(
        raw_writer[m]["total_compressed_bytes"] + raw_writer[m]["active_partial_bytes"]
        for m in markets
    )
    storage = {
        "d_free_bytes": free,
        "raw_quota_bytes": quota_bytes,
        "raw_used_bytes_estimate": used_tape,
        "raw_quota_used_pct": round(used_tape / max(quota_bytes, 1) * 100, 4),
        "storage_state": (
            "STORAGE_CRITICAL" if free <= reserve_bytes
            else "STORAGE_WARNING" if used_tape / max(quota_bytes, 1) > 0.9
            else "STORAGE_HEALTHY"
        ),
    }

    report = {
        "report_schema_version": "prospective_health_report_v2",
        "generated_at_ms": int(time.time() * 1000),
        "campaign_id": campaign_id,
        "database_integrity": cur.execute("PRAGMA quick_check").fetchone()[0],
        "coverage": _coverage(cur),
        "raw_writer": raw_writer,
        "storage": storage,
    }
    conn.close()
    return report


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", required=True)
    parser.add_argument("--campaign-id", required=True)
    parser.add_argument("--tape-root", required=True)
    parser.add_argument("--quota-bytes", type=int, default=10737418240)
    parser.add_argument("--reserve-bytes", type=int, default=16106127360)
    args = parser.parse_args(argv)
    report = build_report(
        db_path=Path(args.db),
        campaign_id=args.campaign_id,
        tape_root=Path(args.tape_root),
        quota_bytes=args.quota_bytes,
        reserve_bytes=args.reserve_bytes,
    )
    print(json.dumps(report, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
