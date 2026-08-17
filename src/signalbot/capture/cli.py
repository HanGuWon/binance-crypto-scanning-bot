from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
from collections.abc import Sequence
from pathlib import Path
from typing import Any

from signalbot.capture.live import (
    CANARY_DURATION_SECONDS,
    MAXIMUM_SMOKE_SECONDS,
    MINIMUM_SMOKE_SECONDS,
    PUBLIC_NETWORK_CONFIRMATION,
    run_foreground_capture,
)
from signalbot.capture.plans import build_prospective_capture_plans
from signalbot.capture.websocket import validate_public_websocket_plan


def validate_capture_configuration(
    *,
    symbols: tuple[str, ...],
    plan_file: str | Path,
    output_directory: str | Path,
    batch_size: int,
    queue_max_events: int,
    queue_max_bytes: int,
    maximum_total_bytes: int,
    emergency_reserve_bytes: int,
    canary_hours: int,
) -> dict[str, Any]:
    """Validate only; this function cannot open a socket or start a writer."""

    plan_path = Path(plan_file)
    if not plan_path.is_file():
        raise ValueError("capture plan_file must be an existing regular file")
    output_path = Path(output_directory)
    if output_path.exists() and not output_path.is_dir():
        raise ValueError("capture output_directory must not be a regular file")
    if queue_max_events < 2:
        raise ValueError("queue_max_events must be at least 2")
    if queue_max_bytes < 1:
        raise ValueError("queue_max_bytes must be positive")
    if emergency_reserve_bytes < 1024:
        raise ValueError("emergency_reserve_bytes must be at least 1024")
    if maximum_total_bytes <= emergency_reserve_bytes:
        raise ValueError("maximum_total_bytes must exceed emergency reserve")
    if not 1 <= canary_hours <= 168:
        raise ValueError("canary_hours must be between 1 and 168")
    plans = build_prospective_capture_plans(symbols, batch_size=batch_size)
    for plan in plans:
        validate_public_websocket_plan(plan)
    return {
        "schema_version": "capture_config_validation_v1",
        "mode": "validation_only",
        "network_calls": False,
        "live_capture_started": False,
        "order_execution": False,
        "plan_sha256": _sha256_file(plan_path),
        "output_directory": str(output_path.resolve()),
        "symbols": list(symbols),
        "canary_hours_if_later_started": canary_hours,
        "queue": {
            "max_events": queue_max_events,
            "max_bytes": queue_max_bytes,
        },
        "storage": {
            "maximum_total_bytes": maximum_total_bytes,
            "emergency_reserve_bytes": emergency_reserve_bytes,
            "compression": "zstd-independent-checksummed-frames",
            "rotation": {
                "utc_ms": 300_000,
                "uncompressed_bytes": 256 * 1024 * 1024,
                "frames": 1_000_000,
            },
        },
        "websocket_plans": [
            {
                "name": plan.name,
                "market": plan.market.value,
                "route": plan.route,
                "stream_count": len(plan.streams),
            }
            for plan in plans
        ],
    }


def main(argv: Sequence[str] | None = None) -> None:
    parser = argparse.ArgumentParser(
        prog="signalbot-capture",
        description=(
            "Validate settings or explicitly run a foreground, public-data-only capture."
        ),
    )
    subparsers = parser.add_subparsers(dest="command", required=True)
    validate = subparsers.add_parser("validate-config")
    validate.add_argument("--symbols", nargs="+", required=True)
    validate.add_argument("--plan-file", required=True)
    validate.add_argument("--output-directory", required=True)
    validate.add_argument("--batch-size", type=int, default=25)
    validate.add_argument("--queue-max-events", type=int, default=100_000)
    validate.add_argument("--queue-max-bytes", type=int, default=256 * 1024 * 1024)
    validate.add_argument("--maximum-total-bytes", type=int, default=100 * 1024**3)
    validate.add_argument("--emergency-reserve-bytes", type=int, default=512 * 1024**2)
    validate.add_argument("--canary-hours", type=int, default=24)
    start_canary = subparsers.add_parser(
        "start-canary",
        help="run the exact 86400-second infrastructure canary in the foreground",
    )
    _add_start_arguments(start_canary)
    start_smoke = subparsers.add_parser(
        "start-smoke",
        help="run a 10-300 second operator smoke capture in the foreground",
    )
    _add_start_arguments(start_smoke)
    start_smoke.add_argument(
        "--seconds",
        type=_smoke_seconds,
        required=True,
        metavar="10..300",
    )
    args = parser.parse_args(argv)
    if args.command == "validate-config":
        result = validate_capture_configuration(
            symbols=tuple(args.symbols),
            plan_file=args.plan_file,
            output_directory=args.output_directory,
            batch_size=args.batch_size,
            queue_max_events=args.queue_max_events,
            queue_max_bytes=args.queue_max_bytes,
            maximum_total_bytes=args.maximum_total_bytes,
            emergency_reserve_bytes=args.emergency_reserve_bytes,
            canary_hours=args.canary_hours,
        )
        print(json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True))
        return

    if not args.allow_public_network:
        parser.error(f"{args.command} requires --allow-public-network")
    if args.confirm != PUBLIC_NETWORK_CONFIRMATION:
        parser.error(
            f"{args.command} requires --confirm {PUBLIC_NETWORK_CONFIRMATION}"
        )
    mode = "canary" if args.command == "start-canary" else "smoke"
    duration_seconds = (
        CANARY_DURATION_SECONDS if mode == "canary" else int(args.seconds)
    )
    capture_result = asyncio.run(
        run_foreground_capture(
            workspace_root=args.workspace_root,
            config_file=args.config_file,
            protocol_file=args.protocol_file,
            output_base=args.output_base,
            external_audit_root=args.external_audit_root,
            mode=mode,
            duration_seconds=duration_seconds,
        )
    )
    print(
        json.dumps(
            capture_result.to_document(),
            ensure_ascii=False,
            indent=2,
            sort_keys=True,
        )
    )


def _add_start_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--workspace-root", required=True)
    parser.add_argument("--config-file", required=True)
    parser.add_argument("--protocol-file", required=True)
    parser.add_argument("--output-base", required=True)
    parser.add_argument("--external-audit-root", required=True)
    parser.add_argument("--allow-public-network", action="store_true")
    parser.add_argument("--confirm", required=True)


def _smoke_seconds(value: str) -> int:
    try:
        seconds = int(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("smoke seconds must be an integer") from exc
    if not MINIMUM_SMOKE_SECONDS <= seconds <= MAXIMUM_SMOKE_SECONDS:
        raise argparse.ArgumentTypeError("smoke seconds must be between 10 and 300")
    return seconds


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()
