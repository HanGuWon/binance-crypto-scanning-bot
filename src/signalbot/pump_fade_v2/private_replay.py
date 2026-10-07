"""Local-only P3 order-history accounting for risk-increasing additions.

The input is a user-supplied Binance USD-M order-history CSV.  Output contains
aggregate statistics only: no symbol, order number, timestamp, fill row or UID
is serialized.  It reconciles execution/order counts with risk-increasing
position additions before any ladder calibration is attempted.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import math
from collections import Counter, defaultdict
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from statistics import median
from typing import Any

KST = timezone(timedelta(hours=9))
EPS = 1e-9


@dataclass(frozen=True, slots=True)
class OrderExecution:
    symbol: str
    at_ms: int
    side: str
    quantity: float
    price: float


@dataclass(frozen=True, slots=True)
class AdditionTrip:
    direction: str
    risk_increasing_orders: int
    addition_orders: int
    initial_quantity: float
    total_entry_quantity: float
    largest_addition_initial_fraction: float
    final_entry_initial_multiple: float
    adverse_spacing_pct: tuple[float, ...]


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _timestamp_ms(text: str) -> int:
    return int(
        datetime.strptime(text, "%Y-%m-%d %H:%M:%S")
        .replace(tzinfo=KST)
        .timestamp()
        * 1000
    )


def load_order_history(path: str | Path) -> tuple[OrderExecution, ...]:
    """Load aggregate executed order rows without retaining account identifiers."""

    source = Path(path)
    rows: list[OrderExecution] = []
    with source.open(encoding="utf-8-sig", newline="") as handle:
        for raw in csv.DictReader(handle):
            quantity = float(raw.get("Executed Amount") or 0)
            if quantity <= 0:
                continue
            quote = float(raw["Executed Quote Amount"])
            side = str(raw["Side"]).upper()
            if side not in {"BUY", "SELL"} or quote <= 0:
                raise ValueError("invalid executed order row")
            rows.append(OrderExecution(
                symbol=str(raw["Symbol"]),
                at_ms=_timestamp_ms(raw.get("Update Time") or raw["Time"]),
                side=side,
                quantity=quantity,
                price=quote / quantity,
            ))
    rows.sort(key=lambda item: (item.at_ms, item.symbol))
    return tuple(rows)


def _adverse_spacing(direction: str, previous_price: float, price: float) -> float:
    if direction == "SHORT":
        return (price / previous_price - 1) * 100
    return (previous_price / price - 1) * 100


def reconstruct_addition_trips(rows: tuple[OrderExecution, ...]) -> tuple[AdditionTrip, ...]:
    """Reconstruct one-way position episodes and classify only risk-increasing rows.

    Each Binance order-history row is an aggregate executed order, so the result
    deliberately calls these *addition orders*.  It does not invent logical
    tranche/fill counts from execution rows.
    """

    by_symbol: dict[str, list[OrderExecution]] = defaultdict(list)
    for row in rows:
        by_symbol[row.symbol].append(row)
    result: list[AdditionTrip] = []
    for executions in by_symbol.values():
        position = 0.0
        direction = ""
        initial_qty = 0.0
        total_entry_qty = 0.0
        additions: list[float] = []
        addition_qty: list[float] = []
        previous_entry_price = 0.0

        def close_trip() -> None:
            nonlocal direction, initial_qty, total_entry_qty, additions
            nonlocal addition_qty, previous_entry_price
            if initial_qty <= 0:
                return
            result.append(AdditionTrip(
                direction=direction,
                risk_increasing_orders=1 + len(addition_qty),
                addition_orders=len(addition_qty),
                initial_quantity=initial_qty,
                total_entry_quantity=total_entry_qty,
                largest_addition_initial_fraction=(
                    max(addition_qty, default=0.0) / initial_qty
                ),
                final_entry_initial_multiple=total_entry_qty / initial_qty,
                adverse_spacing_pct=tuple(additions),
            ))
            direction = ""
            initial_qty = 0.0
            total_entry_qty = 0.0
            additions = []
            addition_qty = []
            previous_entry_price = 0.0

        for execution in executions:
            signed = execution.quantity if execution.side == "BUY" else -execution.quantity
            if abs(position) <= EPS:
                direction = "LONG" if signed > 0 else "SHORT"
                initial_qty = execution.quantity
                total_entry_qty = execution.quantity
                previous_entry_price = execution.price
                position = signed
                continue
            same_direction = (position > 0 and signed > 0) or (position < 0 and signed < 0)
            if same_direction:
                additions.append(_adverse_spacing(direction, previous_entry_price,
                                                  execution.price))
                addition_qty.append(execution.quantity)
                total_entry_qty += execution.quantity
                previous_entry_price = execution.price
                position += signed
                continue

            closing_qty = min(abs(signed), abs(position))
            position += closing_qty if signed > 0 else -closing_qty
            remainder = execution.quantity - closing_qty
            if abs(position) <= EPS * max(1.0, total_entry_qty):
                position = 0.0
                close_trip()
                if remainder > EPS:
                    direction = "LONG" if signed > 0 else "SHORT"
                    initial_qty = remainder
                    total_entry_qty = remainder
                    previous_entry_price = execution.price
                    position = remainder if signed > 0 else -remainder
        if abs(position) > EPS:
            close_trip()
    return tuple(result)


def _quantile(values: Sequence[float], q: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    index = (len(ordered) - 1) * q
    lower = math.floor(index)
    upper = math.ceil(index)
    if lower == upper:
        return ordered[lower]
    weight = index - lower
    return ordered[lower] * (1 - weight) + ordered[upper] * weight


def aggregate_private_additions(
    order_history: str | Path,
) -> dict[str, Any]:
    """Return de-identified P3 accounting sufficient to bound ladder calibration."""

    source = Path(order_history).absolute()
    if not source.is_file():
        raise FileNotFoundError("private order-history input does not exist")
    rows = load_order_history(source)
    trips = reconstruct_addition_trips(rows)
    counts = Counter(trip.addition_orders for trip in trips)
    addition_counts = [float(trip.addition_orders) for trip in trips]
    largest = [trip.largest_addition_initial_fraction for trip in trips
               if trip.addition_orders]
    multiples = [trip.final_entry_initial_multiple for trip in trips]
    spacings = [spacing for trip in trips for spacing in trip.adverse_spacing_pct]
    structurally_within_primary = sum(
        trip.addition_orders <= 2
        and trip.largest_addition_initial_fraction <= 0.5 + 1e-12
        and trip.final_entry_initial_multiple <= 2.0 + 1e-12
        for trip in trips
    )
    direction = Counter(trip.direction for trip in trips)
    return {
        "schema_version": "pump_fade_v2_private_addition_accounting_v1",
        "evidence_class": "PRIVATE_IN_SAMPLE_DESCRIPTIVE_NOT_HOLDOUT",
        "source_sha256": _sha256(source),
        "executed_order_rows": len(rows),
        "reconstructed_position_episodes": len(trips),
        "direction_counts": dict(sorted(direction.items())),
        "addition_order_count_distribution": {
            str(key): counts[key] for key in sorted(counts)
        },
        "episodes_with_any_addition": sum(value for key, value in counts.items() if key >= 1),
        "episodes_with_more_than_two_addition_orders": sum(
            value for key, value in counts.items() if key > 2
        ),
        "episodes_with_eight_or_more_addition_orders": sum(
            value for key, value in counts.items() if key >= 8
        ),
        "addition_orders_per_episode": {
            "median": median(addition_counts) if addition_counts else None,
            "q90": _quantile(addition_counts, 0.90),
            "q99": _quantile(addition_counts, 0.99),
            "max": max(addition_counts, default=None),
        },
        "largest_addition_over_initial_quantity": {
            "median": median(largest) if largest else None,
            "q90": _quantile(largest, 0.90),
            "q99": _quantile(largest, 0.99),
            "max": max(largest, default=None),
        },
        "total_entry_quantity_over_initial": {
            "median": median(multiples) if multiples else None,
            "q90": _quantile(multiples, 0.90),
            "q99": _quantile(multiples, 0.99),
            "max": max(multiples, default=None),
        },
        "adverse_spacing_from_previous_risk_increase_pct": {
            "median": median(spacings) if spacings else None,
            "q90": _quantile(spacings, 0.90),
            "q99": _quantile(spacings, 0.99),
            "max": max(spacings, default=None),
        },
        "episodes_structurally_within_primary_quantity_limits": structurally_within_primary,
        "risk_budget_assessment": "UNAVAILABLE_NO_RECORDED_FROZEN_INVALIDATION_AND_R_BUDGET",
        "official_r2_gate": "UNAVAILABLE",
        "privacy": "aggregate output only; no symbol/order/timestamp/UID rows serialized",
    }


def main(argv: Sequence[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--order-history", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args(argv)
    result = aggregate_private_additions(args.order_history)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n",
                           encoding="utf-8")
    print(json.dumps({
        "status": result["official_r2_gate"],
        "executed_order_rows": result["executed_order_rows"],
        "reconstructed_position_episodes": result["reconstructed_position_episodes"],
        "output": str(args.output),
    }, indent=2))


if __name__ == "__main__":
    main()
