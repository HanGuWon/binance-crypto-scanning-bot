"""Operator CLI for the Discord outbox: ``signalbot outbox status|resolve``."""

from __future__ import annotations

import argparse
import json
import sys
import time

from signalbot.config import Settings
from signalbot.persistence.repository import OutboxResolveError, SqlRepository


def register_outbox_parser(subs: argparse._SubParsersAction) -> None:  # type: ignore[type-arg]
    outbox = subs.add_parser("outbox")
    outbox_subs = outbox.add_subparsers(dest="outbox_command", required=True)
    status = outbox_subs.add_parser("status")
    status.add_argument("--config", required=True)
    resolve = outbox_subs.add_parser("resolve")
    resolve.add_argument("--config", required=True)
    resolve.add_argument("--event-id", required=True)
    resolve.add_argument("--as", dest="resolution", required=True, choices=["delivered", "dead"])
    resolve.add_argument("--reason", required=True)
    resolve.add_argument("--message-id", default=None)


def _now_ms() -> int:
    return time.time_ns() // 1_000_000


def run_outbox_command(args: argparse.Namespace, settings: Settings) -> int:
    """Run one outbox subcommand and return the process exit code."""

    repository = SqlRepository(settings.storage.url, settings.storage.echo_sql)
    repository.initialize()
    try:
        if args.outbox_command == "status":
            summary = repository.outbox_summary(_now_ms())
            print(json.dumps(summary, indent=2, sort_keys=True))
            return 0
        try:
            repository.resolve_uncertain_outbox(
                args.event_id,
                args.resolution,
                args.reason,
                _now_ms(),
                message_id=args.message_id,
            )
        except (OutboxResolveError, ValueError) as exc:
            print(f"outbox resolve refused: {exc}", file=sys.stderr)
            return 2
        print(
            json.dumps(
                {"event_id": args.event_id, "status": args.resolution}, sort_keys=True
            )
        )
        return 0
    finally:
        repository.close()
