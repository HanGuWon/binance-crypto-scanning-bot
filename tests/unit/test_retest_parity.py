"""Independent parity and no-fabricated-BBO contracts."""

from __future__ import annotations

import pytest

from signalbot.prospective.retest_parity import (
    RetestParityCase,
    RetestParityOutput,
    RetestParityStatus,
    run_retest_adapter_parity,
)


def _output(stage="READY"):
    return RetestParityOutput(
        stage=stage,
        terminal_reason=None,
        ready_snapshot_sha256="r" if stage == "READY" else None,
    )


def _case(bbo=True):
    return RetestParityCase(
        opportunity_id="opp",
        input_payload={"bar": 1},
        historical_decision_time_bbo={"bid": "1", "ask": "1.1"}
        if bbo
        else None,
    )


def test_distinct_adapters_matching_with_historical_bbo_pass():
    def live(case):
        return _output()

    def replay(case):
        return _output()

    result = run_retest_adapter_parity(
        [_case()], live_adapter=live, replay_adapter=replay
    )
    assert result.status is RetestParityStatus.PASS
    assert result.comparable_cases == 1


def test_missing_historical_bbo_is_inconclusive_and_not_fabricated():
    def live(case):
        raise AssertionError("adapter must not run without historical BBO")

    def replay(case):
        raise AssertionError("adapter must not run without historical BBO")

    result = run_retest_adapter_parity(
        [_case(bbo=False)], live_adapter=live, replay_adapter=replay
    )
    assert result.status is RetestParityStatus.INCONCLUSIVE_NO_HISTORICAL_BBO
    assert result.missing_historical_bbo_cases == 1


def test_mismatch_fails_even_if_another_case_lacks_bbo():
    def live(case):
        return _output("READY")

    def replay(case):
        return _output("INVALID")

    result = run_retest_adapter_parity(
        [_case(), _case(bbo=False)], live_adapter=live, replay_adapter=replay
    )
    assert result.status is RetestParityStatus.FAIL
    assert result.comparable_cases == 1
    assert len(result.mismatches) == 1


def test_same_adapter_is_not_parity():
    def adapter(case):
        return _output()

    with pytest.raises(ValueError, match="independent"):
        run_retest_adapter_parity(
            [_case()], live_adapter=adapter, replay_adapter=adapter
        )
