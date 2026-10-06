from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parents[2]))

from tools.guardian_l50_phase_gate_replay import run_replay_once


def test_l50_replay_covers_required_scenarios_and_has_zero_write_path(tmp_path: Path) -> None:
    result = run_replay_once(tmp_path / "replay")

    assert result["scenario_count"] == 18
    assert result["all_scenarios_pass"] is True
    assert result["zero_write_proof"] is True
    assert result["structural_forbidden_write_calls"] == []
    assert result["total_exchange_trading_write_calls"] == 0
