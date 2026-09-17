"""Read-only forensic auditor for Phase F final smoke evidence.

This tool is intentionally OUTSIDE src/signalbot so it never changes the
frozen runtime source identity of a smoke campaign. Deterministic,
non-promoting; emits one canonical JSON report.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sqlite3
import sys
from collections import defaultdict
from pathlib import Path

AUDITOR_SCHEMA_VERSION = "phase_f_forensic_audit_v2"


def _sha256_file(path):
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _canonical_sha256(payload):
    canonical = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def audit_coverage(cur):
    per_market = {}
    cur.execute(
        "SELECT market, decision_close_ms, status, complete FROM shadow_coverage "
        "ORDER BY market, decision_close_ms"
    )
    rows = cur.fetchall()
    by_market = defaultdict(list)
    for market, close_ms, status, complete in rows:
        by_market[market].append((close_ms, status, complete))
    for market, cells in by_market.items():
        best_len = 0
        best_start = None
        best_end = None
        run_len = 0
        run_start = None
        prev = None
        for close_ms, _status, _complete in cells:
            if prev is not None and close_ms - prev == 300000:
                run_len += 1
            else:
                run_len = 1
                run_start = close_ms
            if run_len > best_len:
                best_len = run_len
                best_start = run_start
                best_end = close_ms
            prev = close_ms
        open_cells = sum(1 for _, s, _c in cells if s == "OPEN")
        per_market[market] = {
            "total_cells": len(cells),
            "sealed": sum(1 for _, s, _c in cells if s == "SEALED"),
            "open": open_cells,
            "incomplete_sealed": sum(1 for _, s, c in cells if s == "SEALED" and c == 0),
            "max_consecutive_run": best_len,
            "qualifying_run_start_ms": best_start,
            "qualifying_run_end_ms": best_end,
            "meets_18_consecutive": best_len >= 18,
        }
    return per_market


def audit_denominator(cur, campaign_id):
    cur.execute(
        "SELECT opportunity_id, payload_json FROM shadow_observations "
        "WHERE campaign_id = ?",
        (campaign_id,),
    )
    expected = set()
    for oid, payload in cur.fetchall():
        try:
            data = json.loads(payload)
        except ValueError:
            continue
        incumbent = data.get("incumbent_r2") if isinstance(data, dict) else None
        if isinstance(incumbent, dict) and incumbent.get("raw_c0_triggered") is True:
            expected.add(oid)
    cur.execute(
        "SELECT opportunity_id, stage FROM retest_lifecycles WHERE campaign_id = ?",
        (campaign_id,),
    )
    lifecycle_rows = cur.fetchall()
    observed = [row[0] for row in lifecycle_rows]
    missing = sorted(expected - set(observed))
    unexpected = sorted(set(observed) - expected)
    duplicates = sorted({oid for oid in observed if observed.count(oid) > 1})
    stages = defaultdict(int)
    for _oid, stage in lifecycle_rows:
        stages[stage] += 1
    # Phase-H fix: READY is a TERMINAL stage in causal_retest_v1.
    # Active unresolved = ARMED + RETEST_TOUCH only.
    active = stages.get("ARMED", 0) + stages.get("RETEST_TOUCH", 0)
    return {
        "expected_raw_c0": len(expected),
        "lifecycle_rows": len(lifecycle_rows),
        "missing": missing,
        "unexpected": unexpected,
        "duplicate": duplicates,
        "stage_counts": dict(stages),
        "active_unresolved": active,
    }


def audit_raw_tape(tape_root):
    if not tape_root.is_dir():
        return {"present": False}
    per_market = {}
    for market_dir in sorted(p for p in tape_root.iterdir() if p.is_dir()):
        market = market_dir.name
        files = sorted(market_dir.glob("*.jsonl"))
        total_rows = 0
        malformed = 0
        mismatch = 0
        invalid_clock = 0
        backwards = 0
        last_receipt = None
        first_receipt = None
        total_bytes = 0
        file_records = []
        for fp in files:
            total_bytes += fp.stat().st_size
            rows_in_file = 0
            with fp.open("rb") as handle:
                for raw in handle:
                    raw = raw.strip()
                    if not raw:
                        continue
                    rows_in_file += 1
                    try:
                        obj = json.loads(raw)
                        if not isinstance(obj, dict):
                            raise ValueError("not object")
                    except ValueError:
                        malformed += 1
                        continue
                    if obj.get("market") != market:
                        mismatch += 1
                    receipt = obj.get("received_at_ms")
                    if isinstance(receipt, bool) or not isinstance(receipt, int):
                        invalid_clock += 1
                        continue
                    if first_receipt is None:
                        first_receipt = receipt
                    if last_receipt is not None and receipt < last_receipt:
                        backwards += 1
                    last_receipt = receipt
            total_rows += rows_in_file
            file_records.append({
                "path": fp.name,
                "bytes": fp.stat().st_size,
                "sha256": _sha256_file(fp),
                "rows": rows_in_file,
            })
        per_market[market] = {
            "files": len(files),
            "total_bytes": total_bytes,
            "total_rows": total_rows,
            "malformed": malformed,
            "market_mismatch": mismatch,
            "invalid_receipt_clocks": invalid_clock,
            "backwards_receipts": backwards,
            "first_receipt_ms": first_receipt,
            "last_receipt_ms": last_receipt,
            "file_records": file_records,
        }
    return {"present": True, "markets": per_market}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", required=True)
    parser.add_argument("--campaign-id", required=True)
    parser.add_argument("--raw-tape-root", required=True)
    parser.add_argument("--output", required=True)
    args = parser.parse_args(argv)

    db_path = Path(args.db)
    tape_root = Path(args.raw_tape_root)
    conn = sqlite3.connect("file:" + str(db_path).replace("\\", "/") + "?mode=ro", uri=True)
    cur = conn.cursor()
    integrity = cur.execute("PRAGMA integrity_check").fetchone()[0]
    coverage = audit_coverage(cur)
    denominator = audit_denominator(cur, args.campaign_id)
    conn.close()
    raw_tape = audit_raw_tape(tape_root)

    infra_pass = bool(coverage) and integrity == "ok" and all(
        m["meets_18_consecutive"] and m["open"] == 0 for m in coverage.values()
    )
    markets = raw_tape.get("markets", {})
    tape_clean = bool(raw_tape.get("present")) and all(
        m["malformed"] == 0
        and m["market_mismatch"] == 0
        and m["invalid_receipt_clocks"] == 0
        and m["backwards_receipts"] == 0
        for m in markets.values()
    )

    report = {
        "auditor_schema_version": AUDITOR_SCHEMA_VERSION,
        "campaign_id": args.campaign_id,
        "database_integrity": "PASS" if integrity == "ok" else "FAIL",
        "infra_smoke": "PASS" if infra_pass else ("FAIL" if coverage else "NOT_RUN"),
        "raw_tape_integrity": (
            "PASS" if tape_clean else ("FAIL" if raw_tape.get("present") else "NOT_RUN")
        ),
        "coverage": coverage,
        "denominator": denominator,
        "raw_tape": raw_tape,
    }
    content_sha = _canonical_sha256(report)
    report["audit_content_sha256"] = content_sha

    out = Path(args.output)
    out.parent.mkdir(parents=True, exist_ok=True)
    text = json.dumps(report, indent=2, sort_keys=True) + chr(10)
    out.write_text(text, encoding="utf-8")
    print(json.dumps({
        "database_integrity": report["database_integrity"],
        "infra_smoke": report["infra_smoke"],
        "raw_tape_integrity": report["raw_tape_integrity"],
        "audit_content_sha256": content_sha,
    }, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
