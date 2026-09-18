"""Outcome-blind regime observability for a prospective campaign.

Reads only shadow_observations payload_json (decision-time context) and
never touches future outcomes or profitability. Reports live distribution
of regime labels, BTC trend labels, and breadth ratio by market and UTC day.
"""

from __future__ import annotations

import argparse
import json
import sqlite3
from collections import Counter, defaultdict
from datetime import UTC, datetime
from pathlib import Path


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", required=True)
    parser.add_argument("--campaign-id", required=True)
    args = parser.parse_args(argv)

    conn = sqlite3.connect(
        "file:" + str(Path(args.db)).replace("\\", "/") + "?mode=ro", uri=True
    )
    cur = conn.cursor()
    rows = cur.execute(
        "SELECT market, symbol, decision_time_ms, "
        "CASE WHEN json_valid(payload_json) THEN "
        "json_extract(payload_json, '$.research_context.regime.label') END, "
        "CASE WHEN json_valid(payload_json) THEN "
        "json_extract(payload_json, '$.research_context.regime.btc_trend') END, "
        "CASE WHEN json_valid(payload_json) THEN "
        "json_extract(payload_json, '$.research_context.regime.breadth_ratio') END "
        "FROM shadow_observations WHERE campaign_id = ?",
        (args.campaign_id,),
    ).fetchall()
    conn.close()

    label_counts: dict[str, Counter] = defaultdict(Counter)
    btc_counts: dict[str, Counter] = defaultdict(Counter)
    breadth_by_market: dict[str, list] = defaultdict(list)
    by_day: Counter = Counter()
    by_symbol: dict[str, Counter] = defaultdict(Counter)
    qualifying_symbols: set[str] = set()

    for market, symbol, decision_ms, label, btc, breadth in rows:
        label = label if isinstance(label, str) and label.strip() else "unknown"
        btc = btc if isinstance(btc, str) and btc.strip() else "unknown"
        day = datetime.fromtimestamp(decision_ms / 1000, tz=UTC).strftime("%Y-%m-%d")
        label_counts[market][label] += 1
        btc_counts[market][btc] += 1
        if isinstance(breadth, (int, float)):
            breadth_by_market[market].append(breadth)
        by_day[day + "|" + label] += 1
        by_symbol[symbol][label] += 1
        if label.lower() != "unknown":
            qualifying_symbols.add(symbol)

    def quantiles(values):
        ordered = sorted(values)
        if not ordered:
            return {}

        def pct(p):
            index = min(len(ordered) - 1, int(len(ordered) * p))
            return round(ordered[index], 6)

        return {"p05": pct(0.05), "p50": pct(0.5), "p95": pct(0.95)}

    def market_name(market):
        return {"spot": "Spot", "futures": "Futures"}.get(str(market).lower(), str(market))

    def labels(counter):
        return [{"label": k, "count": v} for k, v in sorted(counter.items())]

    qualifying = Counter()
    unknown_count = 0
    for counter in label_counts.values():
        for label, count in counter.items():
            if label.lower() == "unknown":
                unknown_count += count
            else:
                qualifying[label] += count

    day_rows = []
    for day in sorted({key.split("|", 1)[0] for key in by_day}):
        day_counts = Counter(
            {
                key.split("|", 1)[1]: count
                for key, count in by_day.items()
                if key.startswith(day + "|")
            }
        )
        day_rows.append({
            "utc_day": day,
            "qualifying": sum(v for k, v in day_counts.items() if k.lower() != "unknown"),
            "unknown_missing_null": day_counts.get("unknown", 0),
            "labels": labels(day_counts),
        })

    btc_rows = []
    for market, counter in sorted(btc_counts.items()):
        total = sum(counter.values())
        known = total - counter.get("unknown", 0)
        btc_rows.append(
            {
                "market": market_name(market),
                "available": known,
                "missing": total - known,
                "share": known / total if total else 0.0,
            }
        )

    breadth_rows = [
        {
            "market": market_name(market),
            "available": len(values),
            "missing": 0,
            "quantiles": quantiles(values),
        }
        for market, values in sorted(breadth_by_market.items())
    ]

    report = {
        "report_schema_version": "prospective_regime_observability_v1",
        "campaign_id": args.campaign_id,
        "outcome_blind": True,
        "source_paths": [
            "research_context.regime.label",
            "research_context.regime.btc_trend",
            "research_context.regime.breadth_ratio",
        ],
        "qualifying_labels": labels(qualifying),
        "unknown_missing_null_count": unknown_count,
        "btc_trend_share_by_market": btc_rows,
        "breadth_quantiles_by_market": breadth_rows,
        "label_by_utc_day": day_rows,
        "label_by_symbol_count": {
            "qualifying": len(qualifying_symbols),
            "unknown_missing_null": len(by_symbol) - len(qualifying_symbols),
        },
    }
    print(json.dumps(report, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
