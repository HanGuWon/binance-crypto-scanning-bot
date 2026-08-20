"""Adversarial core contracts for causal_retest_v1."""

from __future__ import annotations

import math

import pytest

from conftest import make_feature
from signalbot.config import Settings
from signalbot.domain.enums import Direction, Market, SignalFamily
from signalbot.domain.models import ComparatorCandidate
from signalbot.prospective.retest import (
    RetestArm,
    RetestCensorReason,
    RetestConflictError,
    RetestOutOfOrderError,
    RetestStage,
    arm_from_candidate,
    arm_from_raw_c0,
    build_ready_snapshot,
    censor_lifecycle,
    on_completed_bar,
)

MANIFEST = "m" * 64
ARM_TIME = 1_710_000_000_000
STEP = 300_000


def _settings() -> Settings:
    return Settings.model_validate(
        {
            "binance": {
                "markets": ["spot", "futures"],
                "top_n": 5,
                "surveillance_n": 10,
                "intervals": ["5m", "15m", "1h"],
                "primary_interval": "5m",
                "min_quote_volume": 0,
            },
            "signals": {
                "entry_policy": "r2_pit_htf_exec",
                "gate_enabled": True,
                "confirmation_mode": "explicit_trigger",
            },
        }
    )


def _arm(
    *,
    direction: Direction = Direction.LONG,
    breakout_level: float | None = None,
    arm_price: float = 100.0,
    arm_atr: float = 2.0,
    arm_decision_time_ms: int = ARM_TIME,
    horizon: int = 72,
) -> RetestArm:
    long = direction is Direction.LONG
    return RetestArm(
        opportunity_id="opp1",
        campaign_id="retest-campaign",
        campaign_manifest_sha256=MANIFEST,
        market=Market.SPOT if long else Market.FUTURES,
        symbol="BTCUSDT",
        family=SignalFamily.BREAKOUT_LONG if long else SignalFamily.BREAKDOWN_SHORT,
        direction=direction,
        primary_interval="5m",
        breakout_level=(
            105.0 if breakout_level is None and long
            else 95.0 if breakout_level is None
            else breakout_level
        ),
        arm_price=arm_price,
        arm_atr=arm_atr,
        arm_decision_time_ms=arm_decision_time_ms,
        retest_horizon_bars=horizon,
    )


def _bar(
    lifecycle,
    *,
    index: int,
    close: float,
    decision_time_ms: int | None = None,
    bar_close_ms: int | None = None,
    ready_snapshot=None,
) -> None:
    dt = ARM_TIME + STEP * index if decision_time_ms is None else decision_time_ms
    bt = dt if bar_close_ms is None else bar_close_ms
    on_completed_bar(
        lifecycle,
        decision_time_ms=dt,
        bar_close_ms=bt,
        close=close,
        ready_snapshot=ready_snapshot,
    )


def _candidate(feature, *, family=None, direction=None, raw_c0=True):
    if family is None:
        family = (
            SignalFamily.BREAKOUT_LONG
            if feature.market is Market.SPOT
            else SignalFamily.BREAKDOWN_SHORT
        )
    if direction is None:
        direction = (
            Direction.LONG if feature.market is Market.SPOT else Direction.SHORT
        )
    return ComparatorCandidate(
        market=feature.market,
        symbol=feature.symbol,
        family=family,
        direction=direction,
        decision_time_ms=feature.event_time_ms,
        primary_interval=feature.interval,
        raw_c0_triggered=raw_c0,
        raw_score=55,
        r2_passed=False,
        shadow_passed=False,
    )


def _ready_feature(*, market=Market.SPOT, price=106.0, event_time_ms=None):
    long = market is Market.SPOT
    return make_feature(
        market=market,
        symbol="BTCUSDT",
        interval="5m",
        event_time_ms=(
            ARM_TIME + STEP * 3 if event_time_ms is None else event_time_ms
        ),
        price=price,
        recent_high=105.0,
        recent_low=95.0,
        spread_bps=2.0,
        spread_is_proxy=False,
        book_age_ms=100,
        bid_quote_capacity=10_000.0,
        ask_quote_capacity=10_000.0,
        funding_rate=0.0001 if not long else None,
    )


def _contexts(feature):
    return {
        "15m": make_feature(
            market=feature.market,
            symbol=feature.symbol,
            interval="15m",
            event_time_ms=feature.event_time_ms - 1,
        ),
        "1h": make_feature(
            market=feature.market,
            symbol=feature.symbol,
            interval="1h",
            event_time_ms=feature.event_time_ms - 1,
        ),
    }


def test_spot_authoritative_factory_freezes_recent_high():
    feature = make_feature(
        market=Market.SPOT,
        symbol="BTCUSDT",
        event_time_ms=ARM_TIME,
        recent_high=107.25,
        recent_low=90.0,
        price=108.0,
        atr=2.5,
    )
    lifecycle = arm_from_candidate(
        _candidate(feature),
        feature,
        campaign_id="c",
        campaign_manifest_sha256=MANIFEST,
        opportunity_id="opp",
        retest_horizon_bars=12,
    )
    assert lifecycle.arm.breakout_level == 107.25
    assert lifecycle.arm.family is SignalFamily.BREAKOUT_LONG
    assert lifecycle.arm.direction is Direction.LONG


def test_futures_authoritative_factory_freezes_recent_low():
    feature = make_feature(
        market=Market.FUTURES,
        symbol="BTCUSDT",
        event_time_ms=ARM_TIME,
        recent_high=110.0,
        recent_low=93.75,
        price=92.0,
        atr=2.5,
    )
    lifecycle = arm_from_candidate(
        _candidate(feature),
        feature,
        campaign_id="c",
        campaign_manifest_sha256=MANIFEST,
        opportunity_id="opp",
        retest_horizon_bars=12,
    )
    assert lifecycle.arm.breakout_level == 93.75
    assert lifecycle.arm.family is SignalFamily.BREAKDOWN_SHORT
    assert lifecycle.arm.direction is Direction.SHORT


def test_authoritative_factory_rejects_market_family_direction_mismatch():
    feature = make_feature(
        market=Market.SPOT,
        symbol="BTCUSDT",
        event_time_ms=ARM_TIME,
    )
    bad = _candidate(
        feature,
        family=SignalFamily.BREAKDOWN_SHORT,
        direction=Direction.SHORT,
    )
    with pytest.raises(ValueError):
        arm_from_candidate(
            bad,
            feature,
            campaign_id="c",
            campaign_manifest_sha256=MANIFEST,
            opportunity_id="opp",
            retest_horizon_bars=12,
        )


def test_authoritative_factory_requires_raw_c0():
    feature = make_feature(
        market=Market.SPOT,
        symbol="BTCUSDT",
        event_time_ms=ARM_TIME,
    )
    with pytest.raises(ValueError):
        arm_from_candidate(
            _candidate(feature, raw_c0=False),
            feature,
            campaign_id="c",
            campaign_manifest_sha256=MANIFEST,
            opportunity_id="opp",
            retest_horizon_bars=12,
        )


@pytest.mark.parametrize("value", [math.nan, math.inf, -math.inf, 0.0, -1.0])
def test_arm_rejects_nonfinite_or_nonpositive_scientific_values(value):
    with pytest.raises(ValueError):
        arm_from_raw_c0(_arm(breakout_level=value))


def test_original_c0_bar_cannot_self_confirm_retest():
    lifecycle = arm_from_raw_c0(_arm())
    with pytest.raises(RetestOutOfOrderError):
        on_completed_bar(
            lifecycle,
            decision_time_ms=ARM_TIME,
            bar_close_ms=ARM_TIME,
            close=104.0,
        )


def test_frozen_breakout_level_does_not_drift():
    lifecycle = arm_from_raw_c0(_arm(breakout_level=105.0))
    _bar(lifecycle, index=1, close=106.0)
    _bar(lifecycle, index=2, close=104.0)
    assert lifecycle.stage is RetestStage.RETEST_TOUCH
    assert lifecycle.touch_price == 104.0
    assert lifecycle.arm.breakout_level == 105.0


def test_identical_duplicate_is_idempotent():
    lifecycle = arm_from_raw_c0(_arm())
    _bar(lifecycle, index=1, close=106.0)
    before = lifecycle.elapsed_bars
    _bar(lifecycle, index=1, close=106.0)
    assert lifecycle.elapsed_bars == before


def test_conflicting_duplicate_is_loud():
    lifecycle = arm_from_raw_c0(_arm())
    _bar(lifecycle, index=1, close=106.0)
    with pytest.raises(RetestConflictError):
        _bar(lifecycle, index=1, close=104.0)


def test_bar_close_regression_is_rejected():
    lifecycle = arm_from_raw_c0(_arm())
    _bar(lifecycle, index=2, close=106.0)
    with pytest.raises(RetestOutOfOrderError):
        _bar(
            lifecycle,
            index=3,
            close=106.0,
            bar_close_ms=ARM_TIME + STEP,
        )


def test_decision_time_regression_is_rejected_even_if_bar_close_advances():
    lifecycle = arm_from_raw_c0(_arm())
    _bar(lifecycle, index=2, close=106.0)
    with pytest.raises(RetestOutOfOrderError):
        _bar(
            lifecycle,
            index=3,
            close=106.0,
            decision_time_ms=ARM_TIME + STEP,
            bar_close_ms=ARM_TIME + STEP * 3,
        )


@pytest.mark.parametrize("close", [math.nan, math.inf, -math.inf, -1.0, 0.0])
def test_invalid_completed_bar_terminalizes_invalid(close):
    lifecycle = arm_from_raw_c0(_arm())
    _bar(lifecycle, index=1, close=close)
    assert lifecycle.stage is RetestStage.INVALID
    assert lifecycle.terminal


def test_explicit_censor_transition_is_terminal_and_typed():
    lifecycle = arm_from_raw_c0(_arm())
    censor_lifecycle(
        lifecycle,
        reason=RetestCensorReason.RESTART_GAP,
        decision_time_ms=ARM_TIME + STEP,
    )
    assert lifecycle.stage is RetestStage.CENSORED
    assert lifecycle.terminal_reason == RetestCensorReason.RESTART_GAP.value
    assert lifecycle.terminal_time_ms == ARM_TIME + STEP


def test_terminal_censor_cannot_be_overwritten():
    lifecycle = arm_from_raw_c0(_arm())
    censor_lifecycle(
        lifecycle,
        reason=RetestCensorReason.RESTART_GAP,
        decision_time_ms=ARM_TIME + STEP,
    )
    censor_lifecycle(
        lifecycle,
        reason=RetestCensorReason.CAMPAIGN_SHUTDOWN,
        decision_time_ms=ARM_TIME + STEP * 2,
    )
    assert lifecycle.terminal_reason == RetestCensorReason.RESTART_GAP.value


def test_recovery_without_ready_evidence_censors_instead_of_claiming_ready():
    lifecycle = arm_from_raw_c0(_arm())
    _bar(lifecycle, index=1, close=104.0)
    assert lifecycle.stage is RetestStage.RETEST_TOUCH
    _bar(lifecycle, index=2, close=106.0)
    assert lifecycle.stage is RetestStage.CENSORED
    assert (
        lifecycle.terminal_reason
        == RetestCensorReason.CAUSAL_CONTEXT_UNAVAILABLE.value
    )


def test_ready_snapshot_freezes_ready_time_price_bbo_and_context():
    settings = _settings()
    lifecycle = arm_from_raw_c0(_arm())
    _bar(lifecycle, index=1, close=104.0)
    feature = _ready_feature(price=106.5, event_time_ms=ARM_TIME + STEP * 2)
    snapshot = build_ready_snapshot(
        lifecycle,
        feature,
        _contexts(feature),
        settings,
        bar_close_ms=feature.event_time_ms,
    )
    _bar(
        lifecycle,
        index=2,
        close=feature.price,
        ready_snapshot=snapshot,
    )
    assert lifecycle.stage is RetestStage.READY
    assert lifecycle.ready_price == 106.5
    assert lifecycle.ready_price != lifecycle.arm.arm_price
    assert lifecycle.ready_snapshot is snapshot
    assert snapshot.bbo.eligible is True
    assert snapshot.research_context_sha256
    assert snapshot.content_sha256


def test_ready_snapshot_uses_ready_time_bbo_not_arm_time_bbo():
    settings = _settings()
    lifecycle = arm_from_raw_c0(_arm())
    _bar(lifecycle, index=1, close=104.0)
    feature = _ready_feature(price=106.0, event_time_ms=ARM_TIME + STEP * 2)
    feature = feature.model_copy(update={"book_age_ms": 10_000})
    snapshot = build_ready_snapshot(
        lifecycle,
        feature,
        _contexts(feature),
        settings,
        bar_close_ms=feature.event_time_ms,
    )
    assert snapshot.bbo.eligible is False


def test_ready_snapshot_must_match_recovery_bar_identity():
    settings = _settings()
    lifecycle = arm_from_raw_c0(_arm())
    _bar(lifecycle, index=1, close=104.0)
    feature = _ready_feature(price=106.0, event_time_ms=ARM_TIME + STEP * 2)
    snapshot = build_ready_snapshot(
        lifecycle,
        feature,
        _contexts(feature),
        settings,
        bar_close_ms=feature.event_time_ms,
    )
    with pytest.raises(RetestConflictError):
        on_completed_bar(
            lifecycle,
            decision_time_ms=feature.event_time_ms,
            bar_close_ms=feature.event_time_ms,
            close=106.1,
            ready_snapshot=snapshot,
        )


def test_short_touch_then_ready_uses_ready_snapshot():
    settings = _settings()
    lifecycle = arm_from_raw_c0(_arm(direction=Direction.SHORT))
    _bar(lifecycle, index=1, close=96.0)
    assert lifecycle.stage is RetestStage.RETEST_TOUCH
    feature = _ready_feature(
        market=Market.FUTURES,
        price=94.5,
        event_time_ms=ARM_TIME + STEP * 2,
    )
    snapshot = build_ready_snapshot(
        lifecycle,
        feature,
        _contexts(feature),
        settings,
        bar_close_ms=feature.event_time_ms,
    )
    _bar(
        lifecycle,
        index=2,
        close=feature.price,
        ready_snapshot=snapshot,
    )
    assert lifecycle.stage is RetestStage.READY
    assert lifecycle.ready_price == 94.5


def test_horizon_exact_is_allowed_but_next_bar_times_out():
    lifecycle = arm_from_raw_c0(_arm(horizon=2))
    _bar(lifecycle, index=1, close=106.0)
    _bar(lifecycle, index=2, close=104.0)
    assert lifecycle.elapsed_bars == 2
    assert lifecycle.stage is RetestStage.RETEST_TOUCH
    _bar(lifecycle, index=3, close=106.0)
    assert lifecycle.stage is RetestStage.TIMEOUT


def test_terminal_cannot_regress_to_nonterminal():
    lifecycle = arm_from_raw_c0(_arm(horizon=1))
    _bar(lifecycle, index=1, close=106.0)
    _bar(lifecycle, index=2, close=107.0)
    assert lifecycle.stage is RetestStage.TIMEOUT
    _bar(lifecycle, index=3, close=104.0)
    assert lifecycle.stage is RetestStage.TIMEOUT
