"""Outcome-blind readiness observability for a prospective campaign.

Reads shadow_research_context_v2 readiness fields from decision-time
payloads only. No future outcomes, no production gate changes. Reports
availability frequencies by market and UTC day.
"""

from __future__ import annotations

import argparse
import json
import sqlite3
from collections import Counter, defaultdict
from datetime import UTC, datetime
from pathlib import Path

READINESS_FIELDS = (
    "htf_15m_available",
    "htf_1h_available",
    "fresh_bbo_available",
    "closed_kline_flow_available",
    "intrabar_flow_available",
    "funding_available",
    "structure_available",
    "causal_pullback_available",
)


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", required=True)
    parser.add_argument("--campaign-id", required=True)
    args = parser.parse_args(argv)

    conn = sqlite3.connect(
        "file:" + str(Path(args.db)).replace("\\", "/") + "?mode=ro", uri=True
    )
    cur = conn.cursor()
    projections = ", ".join(
        (
            "CASE WHEN json_valid(payload_json) THEN "
            f"json_extract(payload_json, '$.research_context.readiness.{field}') END"
        )
        for field in READINESS_FIELDS
    )
    rows = cur.execute(
        "SELECT market, decision_time_ms, " + projections + " "
        "FROM shadow_observations WHERE campaign_id = ?",
        (args.campaign_id,),
    ).fetchall()
    conn.close()

    by_market: dict[str, dict[str, Counter]] = defaultdict(
        lambda: {field: Counter() for field in READINESS_FIELDS}
    )
    totals: dict[str, int] = defaultdict(int)

    for row in rows:
        market, decision_ms, *values = row
        day = datetime.fromtimestamp(decision_ms / 1000, tz=UTC).strftime("%Y-%m-%d")
        key = market + "|" + day
        for field, value in zip(READINESS_FIELDS, values, strict=False):
            if value is None:
                by_market[key][field]["missing"] += 1
            elif bool(value):
                by_market[key][field]["true"] += 1
            else:
                by_market[key][field]["false"] += 1
        totals[key] += 1

    def market_name(market):
        return {"spot": "Spot", "futures": "Futures"}.get(str(market).lower(), str(market))

    rows_by_market_day = []
    available_counts_by_market_day = []
    for key in sorted(totals):
        market, day = key.split("|", 1)
        counts = [
            {
                "field": field,
                "true": by_market[key][field].get("true", 0),
                "false": by_market[key][field].get("false", 0),
                "missing": by_market[key][field].get("missing", 0),
            }
            for field in READINESS_FIELDS
        ]
        rows_by_market_day.append(
            {"market": market_name(market), "utc_day": day, "rows": totals[key]}
        )
        available_counts_by_market_day.append(
            {"market": market_name(market), "utc_day": day, "counts": counts}
        )

    report = {
        "report_schema_version": "prospective_readiness_observability_v1",
        "campaign_id": args.campaign_id,
        "outcome_blind": True,
        "source_paths": ["research_context.readiness." + field for field in READINESS_FIELDS],
        "fields": list(READINESS_FIELDS),
        "rows_by_market_day": rows_by_market_day,
        "available_counts_by_market_day": available_counts_by_market_day,
    }
    print(json.dumps(report, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
