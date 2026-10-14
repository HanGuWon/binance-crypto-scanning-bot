"""Fail-closed systemd entry point reserved for a later runtime gate."""

from __future__ import annotations

import argparse
from pathlib import Path

from signalbot.pump_fade_v3.tracker_config import load_position_tracker_config


def main() -> int:
    parser = argparse.ArgumentParser(description="L7 position tracker startup gate")
    parser.add_argument("--config", type=Path, required=True)
    args = parser.parse_args()
    config = load_position_tracker_config(args.config)
    if not config.enabled:
        return 0
    raise RuntimeError("L7-G1 ships no live tracker runtime; keep the service disabled")


if __name__ == "__main__":
    raise SystemExit(main())
