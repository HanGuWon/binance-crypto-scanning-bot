from __future__ import annotations

import argparse
import asyncio
import json
from collections.abc import Sequence

from position_guardian.config import GuardianSettings, load_guardian_settings, settings_summary
from position_guardian.discord import GuardianDiscordNotifier
from position_guardian.persistence.repository import GuardianRepository
from position_guardian.runtime import build_dry_run_report
from signalbot.clock import SystemClock


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="position-guardian",
        description="Validate Position Guardian configuration or run an order-free dry run.",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    validate = subparsers.add_parser("validate-config")
    validate.add_argument("--config", required=True)

    run = subparsers.add_parser("run")
    run.add_argument("--config", required=True)
    run.add_argument("--dry-run", action="store_true")
    dispatch = subparsers.add_parser("dispatch-alerts")
    dispatch.add_argument("--config", required=True)
    return parser


def main(argv: Sequence[str] | None = None) -> None:
    parser = _parser()
    args = parser.parse_args(argv)
    settings = load_guardian_settings(args.config)

    if args.command == "validate-config":
        print(json.dumps(settings_summary(settings), indent=2, sort_keys=True))
        return

    if args.command == "dispatch-alerts":
        print(
            json.dumps(
                asyncio.run(_dispatch_alerts_once(settings)),
                indent=2,
                sort_keys=True,
            )
        )
        return

    if not args.dry_run:
        parser.error("run currently requires --dry-run")
    print(json.dumps(build_dry_run_report(settings), indent=2, sort_keys=True))


async def _dispatch_alerts_once(settings: GuardianSettings) -> dict[str, object]:
    with GuardianRepository(settings.database_url.get_secret_value()) as repository:
        notifier = GuardianDiscordNotifier(
            settings.alerts,
            repository,
            SystemClock(),
        )
        try:
            result = await notifier.startup_and_dispatch()
        finally:
            await notifier.close()
    return {
        "schema_version": "position_guardian_alert_dispatch_v1",
        "recovery": {
            "source_events_scanned": result.recovery.source_events_scanned,
            "alerts_projected": result.recovery.alerts_projected,
            "alerts_inserted": result.recovery.alerts_inserted,
            "inflight_quarantined": result.recovery.inflight_quarantined,
            "unauthorized_pending_disabled": result.recovery.unauthorized_pending_disabled,
        },
        "delivery_results": [
            {
                "status": delivery.status,
                "attempts": delivery.attempts,
                "response_code": delivery.response_code,
                "message_id": delivery.message_id,
                "detail_code": delivery.detail_code,
            }
            for delivery in result.deliveries
        ],
        "exchange_write_calls": result.exchange_write_calls,
    }
