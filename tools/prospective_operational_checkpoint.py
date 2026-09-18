"""Deterministic operational checkpoint receipt generator.

Reads a prospective campaign DB + tape root and emits a timestamped
operational checkpoint artifact (programmatic UTC only, deterministic schema).
Outcome-blind: never reads signal/observation payloads.

Usage:
    uv run python tools/prospective_operational_checkpoint.py \
        --db <campaign.db> --campaign-id <id> --tape-root <raw-events> \
        --activation-ms 1787734450558 --output health/receipt.json
"""

from __future__ import annotations

import argparse
import json
import sqlite3
import time
from datetime import UTC, datetime, timezone
from pathlib import Path


def _iso_utc(ms: int) -> str:
    return datetime.fromtimestamp(ms / 1000, tz=UTC).isoformat()


def _iso_kst(ms: int) -> str:
    return datetime.fromtimestamp(ms / 1000, tz=UTC).astimezone(
        timezone(timedelta(hours=9))
    ).isoformat()


from datetime import timedelta  # noqa: E402


def build_checkpoint(
    *,
    db_path: Path,
    campaign_id: str,
    tape_root: Path,
    activation_ms: int,
) -> dict:
    """Build a deterministic operational checkpoint dict."""
    now_ms = int(time.time() * 1000)
    conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    cur = conn.cursor()

    coverage_by_market = {}
    for market in ("futures", "spot"):
        row = cur.execute(
            "SELECT COUNT(*), "
            "SUM(CASE WHEN status='SEALED' THEN 1 ELSE 0 END), "
            "SUM(CASE WHEN status='OPEN' THEN 1 ELSE 0 END), "
            "SUM(CASE WHEN status='INCOMPLETE' THEN 1 ELSE 0 END) "
            "FROM shadow_coverage WHERE market=?",
            (market,),
        ).fetchone()
        total, sealed, open_, incomplete = row
        coverage_by_market[market] = {
            "total_cells": total or 0,
            "sealed": sealed or 0,
            "open": open_ or 0,
            "incomplete": incomplete or 0,
        }

    tape_status = {}
    for market in ("futures", "spot"):
        mdir = tape_root / market
        zst = list(mdir.glob("*.jsonl.zst")) if mdir.is_dir() else []
        partials = list(mdir.glob("*.jsonl.zst.partial")) if mdir.is_dir() else []
        manifests = list(mdir.glob("*.manifest.json")) if mdir.is_dir() else []
        latest_receipt = None
        for mf in manifests:
            try:
                data = json.loads(mf.read_text(encoding="utf-8"))
                lr = data.get("last_received_at_ms")
                if lr is not None and (latest_receipt is None or lr > latest_receipt):
                    latest_receipt = lr
            except (json.JSONDecodeError, OSError):
                continue
        tape_status[market] = {
            "sealed_segment_count": len(zst),
            "manifest_count": len(manifests),
            "active_partial_count": len(partials),
            "active_partial_bytes": sum(p.stat().st_size for p in partials),
            "latest_last_received_at_ms": latest_receipt,
            "latest_last_received_utc": _iso_utc(latest_receipt) if latest_receipt else None,
        }

    integrity = cur.execute("PRAGMA quick_check").fetchone()[0]
    conn.close()

    elapsed_since_activation_ms = now_ms - activation_ms
    checkpoint = {
        "checkpoint_schema_version": "prospective_operational_checkpoint_v1",
        "captured_at_ms": now_ms,
        "captured_at_utc": _iso_utc(now_ms),
        "captured_at_kst": _iso_kst(now_ms),
        "campaign_id": campaign_id,
        "activation_ms": activation_ms,
        "activation_utc": _iso_utc(activation_ms),
        "activation_kst": _iso_kst(activation_ms),
        "elapsed_since_activation_ms": elapsed_since_activation_ms,
        "elapsed_since_activation_hours": round(elapsed_since_activation_ms / 3_600_000, 4),
        "database_integrity": integrity,
        "coverage": coverage_by_market,
        "raw_tape": tape_status,
    }
    return checkpoint


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", required=True)
    parser.add_argument("--campaign-id", required=True)
    parser.add_argument("--tape-root", required=True)
    parser.add_argument("--activation-ms", type=int, required=True)
    parser.add_argument("--output", default=None)
    args = parser.parse_args(argv)

    cp = build_checkpoint(
        db_path=Path(args.db),
        campaign_id=args.campaign_id,
        tape_root=Path(args.tape_root),
        activation_ms=args.activation_ms,
    )
    text_out = json.dumps(cp, indent=2)
    if args.output:
        out_path = Path(args.output)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(text_out + "\n", encoding="utf-8")
        print(f"checkpoint written to {out_path}")
    else:
        print(text_out)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
