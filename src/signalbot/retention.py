"""Candle retention: ``signalbot prune-candles`` (dry-run by default).

Only the ``candles`` table is ever touched. Signals, alerts, the alert outbox,
runtime heartbeats and shadow/campaign tables are never read or modified here.
Deletes are issued in bounded batches, one transaction per batch.
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from typing import Any

from signalbot.config import Settings
from signalbot.persistence.repository import SqlRepository

CANDLE_PRUNE_BATCH_SIZE = 5_000
MS_PER_DAY = 86_400_000


def register_prune_parser(subs: argparse._SubParsersAction) -> None:  # type: ignore[type-arg]
    prune = subs.add_parser("prune-candles")
    prune.add_argument("--config", required=True)
    prune.add_argument("--older-than-days", type=int, required=True)
    prune.add_argument(
        "--apply",
        action="store_true",
        help="actually delete; without it the command only reports what would be deleted",
    )


def prune_candles(
    repository: SqlRepository,
    *,
    older_than_days: int,
    now_ms: int,
    apply: bool,
    batch_size: int = CANDLE_PRUNE_BATCH_SIZE,
) -> dict[str, Any]:
    """Report (and with ``apply`` delete) candles whose close time precedes the cutoff.

    A candle closing exactly at the cutoff is kept.
    """

    if older_than_days < 1:
        raise ValueError("--older-than-days must be at least 1")
    if batch_size < 1:
        raise ValueError("batch size must be positive")
    cutoff_ms = now_ms - older_than_days * MS_PER_DAY
    counts = repository.count_candles_before(cutoff_ms)
    candidate_total = sum(counts.values())
    deleted_total = 0
    batches = 0
    if apply:
        # Bounded: at most one batch per full batch of candidates, plus the final one.
        for _ in range(candidate_total // batch_size + 2):
            deleted = repository.delete_candles_before(cutoff_ms, batch_size)
            batches += 1
            deleted_total += deleted
            if deleted < batch_size:
                break
    return {
        "mode": "apply" if apply else "dry-run",
        "older_than_days": older_than_days,
        "cutoff_ms": cutoff_ms,
        "candidates_by_market_interval": {
            f"{market}/{interval}": count for (market, interval), count in sorted(counts.items())
        },
        "candidate_total": candidate_total,
        "deleted_total": deleted_total,
        "batches": batches,
    }


def run_prune_command(args: argparse.Namespace, settings: Settings) -> int:
    """Run the prune subcommand and return the process exit code."""

    if args.older_than_days < 1:
        print("prune-candles refused: --older-than-days must be at least 1", file=sys.stderr)
        return 2
    repository = SqlRepository(settings.storage.url, settings.storage.echo_sql)
    repository.initialize()
    try:
        report = prune_candles(
            repository,
            older_than_days=args.older_than_days,
            now_ms=time.time_ns() // 1_000_000,
            apply=args.apply,
        )
    finally:
        repository.close()
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0
