"""Read-only long-run health report for a prospective campaign.

Reports campaign identity, coverage continuity, retest lifecycle state,
BBO quality, raw-writer health, and D: storage runway. No outcome or
profitability gating is performed here.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import shutil
import sqlite3
import time
from pathlib import Path


def _retest(cur, campaign_id: str) -> dict:
    rows = cur.execute(
        "SELECT stage, COUNT(*) FROM retest_lifecycles WHERE campaign_id = ? GROUP BY stage",
        (campaign_id,),
    ).fetchall()
    stages = dict(rows)
    active = stages.get("ARMED", 0) + stages.get("RETEST_TOUCH", 0)
    return {"stage_counts": stages, "active_unresolved": active}


def _raw_writer(tape_root: Path) -> dict:
    if not tape_root.is_dir():
        return {"present": False}
    files = sorted(tape_root.rglob("*.jsonl"))
    total_bytes = sum(f.stat().st_size for f in files)
    latest_receipt = None
    for f in files:
        with f.open("rb") as handle:
            try:
                handle.seek(max(0, handle.seek(0, 2) - 65536))
                tail = handle.read().decode("utf-8", errors="replace").strip().splitlines()
                while tail and not tail[-1].strip():
                    tail.pop()
                if tail:
                    record = json.loads(tail[-1])
                    receipt = record.get("received_at_ms")
                    if receipt is not None:
                        latest_receipt = max(latest_receipt or 0, receipt)
            except Exception:
                continue
    return {
        "present": True,
        "file_count": len(files),
        "total_bytes": total_bytes,
        "latest_received_at_ms": latest_receipt,
    }


def _storage(db_path: Path, tape_root: Path, quota_bytes: int, reserve_bytes: int) -> dict:
    free = shutil.disk_usage(str(db_path.anchor or "D:/")).free
    used = (
        sum(f.stat().st_size for f in tape_root.rglob("*") if f.is_file())
        if tape_root.is_dir()
        else 0
    )
    quota_used_pct = round(used / max(quota_bytes, 1) * 100, 3)
    return {
        "d_free_bytes": free,
        "raw_quota_bytes": quota_bytes,
        "raw_quota_used_bytes": used,
        "raw_quota_used_pct": quota_used_pct,
        "reserve_free_bytes": reserve_bytes,
        "storage_state": (
            "STORAGE_CRITICAL" if free <= reserve_bytes
            else "STORAGE_WARNING" if used / max(quota_bytes, 1) > 0.9
            else "STORAGE_HEALTHY"
        ),
    }


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _coverage(cur) -> dict:
    rows = cur.execute(
        "SELECT market, decision_close_ms, status, complete, evidence_failures "
        "FROM shadow_coverage ORDER BY market, decision_close_ms"
    ).fetchall()
    by_market: dict[str, list] = {}
    for market, close_ms, status, complete, failures in rows:
        by_market.setdefault(market, []).append((close_ms, status, complete, failures))
    coverage = {}
    now_ms = int(time.time() * 1000)
    for market, cells in by_market.items():
        best_run = run = 1
        for i in range(1, len(cells)):
            run = run + 1 if cells[i][0] - cells[i - 1][0] == 300_000 else 1
            best_run = max(best_run, run)
        latest = cells[-1]
        gaps = [
            (cells[i][0], cells[i - 1][0])
            for i in range(1, len(cells))
            if cells[i][0] - cells[i - 1][0] != 300_000
        ]
        coverage[market] = {
            "total_cells": len(cells),
            "sealed": sum(1 for c in cells if c[1] == "SEALED"),
            "open": sum(1 for c in cells if c[1] == "OPEN"),
            "complete": sum(1 for c in cells if c[2] == 1),
            "evidence_failures_total": sum(c[3] for c in cells),
            "max_consecutive_run": best_run,
            "latest_decision_close_ms": latest[0],
            "latest_close_age_ms": now_ms - latest[0],
            "gap_count": len(gaps),
        }
    return coverage


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", required=True)
    parser.add_argument("--campaign-id", required=True)
    parser.add_argument("--tape-root", required=True)
    parser.add_argument("--quota-bytes", type=int, default=118111600640)
    parser.add_argument("--reserve-bytes", type=int, default=16106127360)
    args = parser.parse_args(argv)

    db_path = Path(args.db)
    tape_root = Path(args.tape_root)
    conn = sqlite3.connect("file:" + str(db_path).replace("\\", "/") + "?mode=ro", uri=True)
    cur = conn.cursor()

    manifest_row = cur.execute(
        "SELECT campaign_manifest_sha256 FROM shadow_coverage LIMIT 1"
    ).fetchone()

    report = {
        "report_schema_version": "prospective_health_report_v1",
        "generated_at_ms": int(time.time() * 1000),
        "campaign": {
            "id": args.campaign_id,
            "manifest_sha256": manifest_row[0] if manifest_row else None,
            "database_integrity": cur.execute("PRAGMA integrity_check").fetchone()[0],
        },
        "coverage": _coverage(cur),
        "retest": _retest(cur, args.campaign_id),
        "bbo": {
            "note": (
                "exact-BBO and receipt-clock quality live inside "
                "shadow_observations payload_json; aggregate audit is performed "
                "by the smoke auditor"
            ),
            "observation_rows": cur.execute(
                "SELECT COUNT(*) FROM shadow_observations WHERE campaign_id = ?",
                (args.campaign_id,),
            ).fetchone()[0],
        },
        "raw_writer": _raw_writer(tape_root),
        "storage": _storage(
            db_path, tape_root, args.quota_bytes, args.reserve_bytes
        ),
    }
    conn.close()

    print(json.dumps(report, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
