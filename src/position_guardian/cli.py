from __future__ import annotations

import argparse
import json
from collections.abc import Sequence

from position_guardian.config import load_guardian_settings, settings_summary
from position_guardian.runtime import build_dry_run_report


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
    return parser


def main(argv: Sequence[str] | None = None) -> None:
    parser = _parser()
    args = parser.parse_args(argv)
    settings = load_guardian_settings(args.config)

    if args.command == "validate-config":
        print(json.dumps(settings_summary(settings), indent=2, sort_keys=True))
        return

    if not args.dry_run:
        parser.error("run currently requires --dry-run")
    print(json.dumps(build_dry_run_report(settings), indent=2, sort_keys=True))
