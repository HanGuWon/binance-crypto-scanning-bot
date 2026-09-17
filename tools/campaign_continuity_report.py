"""Read-only campaign coverage-continuity report.

Identifies missing expected 5m decision-close windows per market from the
durable shadow_coverage rows. It NEVER manufactures opportunities or
coverage cells; missing windows stay explicit evidence gaps.

Usage:
    python tools/campaign_continuity_report.py --db <db> [--market spot]
"""

from __future__ import annotations

import argparse
import json
import sqlite3
from datetime import UTC


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", required=True)
    parser.add_argument("--market", default=None)
    args = parser.parse_args()

    conn = sqlite3.connect("file:" + args.db.replace("\\", "/") + "?mode=ro", uri=True)
    cur = conn.cursor()
    query = (
        "SELECT market, decision_close_ms, status FROM shadow_coverage"
    )
    params: tuple = ()
    if args.market:
        query += " WHERE market = ?"
        params = (args.market,)
    rows = cur.execute(query + " ORDER BY market, decision_close_ms", params).fetchall()
    conn.close()

    report: dict = {"gaps": [], "total_cells": len(rows)}
    prev: dict[str, tuple[int, str]] = {}
    from datetime import datetime as _dt

    def _iso(ms: int) -> str:
        return _dt.fromtimestamp(ms / 1000, tz=UTC).strftime(
            "%Y-%m-%dT%H:%M:%SZ"
        )

    for market, close_ms, status in rows:
        if market in prev:
            prev_close, _prev_status = prev[market]
            delta = close_ms - prev_close
            if delta != 300_000 and delta >= 300_000:
                missing = delta // 300_000 - 1
                report["gaps"].append(
                    {
                        "market": market,
                        "gap_start": _iso(prev_close + 300_000),
                        "gap_end": _iso(close_ms - 300_000),
                        "duration_s": delta // 1000,
                        "missing_windows": missing,
                    }
                )
        prev[market] = (close_ms, status)

    print(json.dumps(report, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
