"""Fail-closed config loading for the disabled L7 shadow tracker."""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class PositionTrackerConfig:
    enabled: bool
    rule_context: str
    adverse_continuation_enabled: bool
    adverse_continuation_hash: str | None
    runner_enabled: bool
    runner_hash: str | None


def load_position_tracker_config(path: Path) -> PositionTrackerConfig:
    raw = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        raise ValueError("position tracker config must be an object")
    active = raw.get("activation")
    if not isinstance(active, dict):
        raise ValueError("position tracker activation config is required")
    adverse = active.get("adverse_continuation")
    runner = active.get("runner")
    if not isinstance(adverse, dict) or not isinstance(runner, dict):
        raise ValueError("activation entries must be objects")
    if any(
        not isinstance(entry.get("enabled"), bool)
        or (entry.get("rule_hash") is not None
        and not isinstance(entry.get("rule_hash"), str))
        for entry in (adverse, runner)
    ):
        raise ValueError("activation fields have invalid types")
    adverse_enabled = adverse.get("enabled") is True
    runner_enabled = runner.get("enabled") is True
    adverse_hash = adverse.get("rule_hash")
    runner_hash = runner.get("rule_hash")
    if adverse_enabled and (not isinstance(adverse_hash, str) or not adverse_hash.strip()):
        raise ValueError("ADVERSE-CONTINUATION requires a selected rule hash")
    if runner_enabled and (not isinstance(runner_hash, str) or not runner_hash.strip()):
        raise ValueError("RUNNER requires a promoted arm hash")
    mode = raw.get("rule_context")
    if mode != "UNAVAILABLE":
        raise ValueError("L7-G1 requires RuleContext UNAVAILABLE")
    if not isinstance(raw.get("enabled"), bool):
        raise ValueError("enabled must be boolean")
    return PositionTrackerConfig(
        enabled=raw.get("enabled") is True,
        rule_context=mode,
        adverse_continuation_enabled=adverse_enabled,
        adverse_continuation_hash=adverse_hash if isinstance(adverse_hash, str) else None,
        runner_enabled=runner_enabled,
        runner_hash=runner_hash if isinstance(runner_hash, str) else None,
    )
