"""Phase 3 restart recovery: restore ARMING/RETEST_TOUCH from durable state,
censor RESTART_GAP when completed-bar continuity cannot be proven, and never
silently resume from an assumed ARMED state."""

from __future__ import annotations

from signalbot.domain.enums import Direction, Market, SignalFamily
from signalbot.prospective.retest import (
    RetestArm,
    RetestCensorReason,
    RetestStage,
    arm_from_raw_c0,
    on_completed_bar,
    recover_after_restart,
    restore_lifecycle,
    serialize_lifecycle,
)

MANIFEST = "m" * 64
ARM_TIME = 1_710_000_000_000
STEP = 300_000


def _arm() -> RetestArm:
    return RetestArm(
        opportunity_id="opp1",
        campaign_id="retest-campaign",
        campaign_manifest_sha256=MANIFEST,
        market=Market.SPOT,
        symbol="BTCUSDT",
        family=SignalFamily.BREAKOUT_LONG,
        direction=Direction.LONG,
        primary_interval="5m",
        breakout_level=105.0,
        arm_price=100.0,
        arm_atr=2.0,
        arm_decision_time_ms=ARM_TIME,
        retest_horizon_bars=72,
    )


def _bar(lifecycle, *, index: int, close: float) -> None:
    on_completed_bar(
        lifecycle,
        decision_time_ms=ARM_TIME + STEP * index,
        bar_close_ms=ARM_TIME + STEP * (index + 1),
        close=close,
    )


def test_serialize_restore_round_trips_armed_exactly():
    life = arm_from_raw_c0(_arm())
    data = serialize_lifecycle(life)
    restored = restore_lifecycle(data)
    assert restored.stage is RetestStage.ARMED
    assert restored.arm.opportunity_id == life.arm.opportunity_id
    assert restored.arm.breakout_level == life.arm.breakout_level
    assert restored.arm.direction is Direction.LONG


def test_serialize_restore_round_trips_retest_touch():
    life = arm_from_raw_c0(_arm())
    _bar(life, index=1, close=104.0)
    assert life.stage is RetestStage.RETEST_TOUCH
    restored = restore_lifecycle(serialize_lifecycle(life))
    assert restored.stage is RetestStage.RETEST_TOUCH
    assert restored.touch_bar_close_ms == ARM_TIME + STEP * 2
    assert restored.touch_price == 104.0


def test_recover_armed_with_continuity_resumes_unchanged():
    life = arm_from_raw_c0(_arm())
    life = recover_after_restart(
        serialize_lifecycle(life),
        continuity_proven=True,
        resume_decision_time_ms=ARM_TIME + STEP,
    )
    assert life.stage is RetestStage.ARMED
    assert not life.terminal


def test_recover_mid_flight_without_continuity_censors_restart_gap():
    life = arm_from_raw_c0(_arm())
    _bar(life, index=1, close=104.0)
    assert life.stage is RetestStage.RETEST_TOUCH
    resumed = recover_after_restart(
        serialize_lifecycle(life),
        continuity_proven=False,
        resume_decision_time_ms=ARM_TIME + STEP * 3,
    )
    assert resumed.stage is RetestStage.CENSORED
    assert resumed.terminal
    assert resumed.terminal_reason == RetestCensorReason.RESTART_GAP.value


def test_terminal_lifecycle_is_returned_untouched():
    life = arm_from_raw_c0(_arm())
    _bar(life, index=1, close=104.0)
    _bar(life, index=2, close=106.0)
    assert life.stage is RetestStage.CENSORED
    resumed = recover_after_restart(
        serialize_lifecycle(life),
        continuity_proven=True,
        resume_decision_time_ms=ARM_TIME + STEP * 5,
    )
    assert resumed.terminal
    assert resumed.terminal_reason
    assert resumed.terminal_reason != RetestCensorReason.RESTART_GAP.value


def test_raw_c0_durable_row_is_never_promoted_to_armed():
    life = arm_from_raw_c0(_arm())
    data = serialize_lifecycle(life)
    data["stage"] = RetestStage.RAW_C0.value
    resumed = recover_after_restart(
        data,
        continuity_proven=True,
        resume_decision_time_ms=ARM_TIME + STEP,
    )
    assert resumed.stage is RetestStage.CENSORED
    assert resumed.terminal_reason == RetestCensorReason.RESTART_GAP.value
