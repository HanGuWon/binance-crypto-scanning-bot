from __future__ import annotations

import pytest

from signalbot.domain.enums import Direction, Market
from signalbot.signals.position_management import (
    GuardianPolicyState,
    ManagedPositionSnapshot,
    ProtectiveStopPlanner,
    ProtectiveStopPolicy,
)


def _snapshot(**updates: object) -> ManagedPositionSnapshot:
    values: dict[str, object] = {
        "position_ref": "manual:BTCUSDT:long",
        "market": Market.FUTURES,
        "symbol": "btcusdt",
        "direction": Direction.LONG,
        "entry_price": 100.0,
        "initial_stop": 98.0,
        "active_stop": 98.0,
        "reference_price": 102.0,
        "highest_price": 103.0,
        "lowest_price": 99.0,
        "observed_at_ms": 1_700_000_000_000,
    }
    values.update(updates)
    return ManagedPositionSnapshot(**values)  # type: ignore[arg-type]


def test_long_position_proposes_monotonic_reduce_only_trailing_stop() -> None:
    intent = ProtectiveStopPlanner().plan(_snapshot(), atr=1.0)

    assert intent is not None
    assert intent.symbol == "BTCUSDT"
    assert intent.previous_stop == pytest.approx(98.0)
    assert intent.proposed_stop == pytest.approx(101.0)
    assert intent.reduce_only is True
    assert intent.close_position is False
    assert intent.order_placed is False


def test_short_position_proposes_monotonic_trailing_stop() -> None:
    snapshot = _snapshot(
        position_ref="manual:BTCUSDT:short",
        direction=Direction.SHORT,
        entry_price=100.0,
        initial_stop=102.0,
        active_stop=102.0,
        reference_price=98.0,
        highest_price=101.0,
        lowest_price=97.0,
    )

    intent = ProtectiveStopPlanner().plan(snapshot, atr=1.0)

    assert intent is not None
    assert intent.proposed_stop == pytest.approx(99.0)
    assert intent.proposed_stop < intent.previous_stop


def test_trailing_stop_waits_until_activation_excursion() -> None:
    snapshot = _snapshot(highest_price=101.9, reference_price=101.5)

    assert ProtectiveStopPlanner().plan(snapshot, atr=1.0) is None


def test_candidate_is_capped_away_from_current_price() -> None:
    planner = ProtectiveStopPlanner(
        ProtectiveStopPolicy(minimum_price_gap_bps=100.0, minimum_improvement_bps=0.0)
    )
    snapshot = _snapshot(reference_price=100.5, highest_price=104.0)

    intent = planner.plan(snapshot, atr=0.5)

    assert intent is not None
    assert intent.proposed_stop == pytest.approx(99.495)
    assert intent.proposed_stop < snapshot.reference_price


def test_same_observation_produces_same_intent_id() -> None:
    planner = ProtectiveStopPlanner()

    first = planner.plan(_snapshot(), atr=1.0)
    second = planner.plan(_snapshot(), atr=1.0)

    assert first is not None and second is not None
    assert first.intent_id == second.intent_id


def test_invalid_or_looser_managed_position_fails_closed() -> None:
    with pytest.raises(ValueError, match="cannot loosen"):
        _snapshot(active_stop=97.0)


def test_non_futures_managed_position_is_rejected() -> None:
    with pytest.raises(ValueError, match="futures"):
        _snapshot(market=Market.SPOT)


def test_profitable_manual_long_uses_floor_without_estimating_risk() -> None:
    snapshot = _snapshot(
        initial_stop=None,
        original_risk_stop=None,
        protection_floor=101.0,
        active_stop=100.0,
        reference_price=105.0,
        highest_price=106.0,
        lowest_price=99.0,
    )

    intent = ProtectiveStopPlanner().plan(snapshot, atr=1.0)

    assert intent is not None
    assert intent.policy_state is GuardianPolicyState.PROFIT_PROTECTION
    assert intent.proposed_stop == pytest.approx(104.0)
    assert snapshot.protection_floor is not None
    assert intent.proposed_stop >= snapshot.protection_floor


def test_profitable_manual_short_mirrors_floor_without_estimating_risk() -> None:
    snapshot = _snapshot(
        position_ref="manual:BTCUSDT:short",
        direction=Direction.SHORT,
        entry_price=100.0,
        initial_stop=None,
        original_risk_stop=None,
        active_stop=100.0,
        protection_floor=99.0,
        reference_price=95.0,
        highest_price=101.0,
        lowest_price=94.0,
    )

    intent = ProtectiveStopPlanner().plan(snapshot, atr=1.0)

    assert intent is not None
    assert intent.policy_state is GuardianPolicyState.PROFIT_PROTECTION
    assert intent.proposed_stop == pytest.approx(96.0)
    assert snapshot.protection_floor is not None
    assert intent.proposed_stop <= snapshot.protection_floor


def test_original_risk_and_floor_are_distinct() -> None:
    snapshot = _snapshot(
        original_risk_stop=98.0,
        protection_floor=101.0,
        active_stop=100.0,
    )

    intent = ProtectiveStopPlanner().plan(snapshot, atr=1.0)

    assert intent is not None
    assert intent.policy_state is GuardianPolicyState.PROFIT_PROTECTION
    assert intent.proposed_stop >= 101.0


def test_floor_that_is_already_beyond_current_price_suppresses_intent() -> None:
    snapshot = _snapshot(
        protection_floor=102.0,
        active_stop=100.0,
        reference_price=102.0,
        highest_price=104.0,
    )

    assert ProtectiveStopPlanner().plan(snapshot, atr=1.0) is None


def test_stale_or_uncertain_context_fails_closed() -> None:
    snapshot = _snapshot(context_state="STALE_OR_UNCERTAIN")

    planner = ProtectiveStopPlanner()

    assert (
        planner.classify_policy_state(snapshot, atr=1.0)
        is GuardianPolicyState.STALE_OR_UNCERTAIN
    )
    assert planner.plan(snapshot, atr=1.0) is None


def test_confirmed_structure_and_momentum_weakening_selects_structure_trail() -> None:
    snapshot = _snapshot(
        active_stop=100.0,
        protection_floor=100.5,
        reference_price=105.0,
        highest_price=106.0,
    )

    intent = ProtectiveStopPlanner().plan(
        snapshot,
        atr=1.0,
        confirmed_structure_stop=103.0,
        momentum_weakened=True,
    )

    assert intent is not None
    assert intent.policy_state is GuardianPolicyState.TREND_WEAKENING
    assert intent.reason == "confirmed_structure_weakening"
    assert intent.proposed_stop == pytest.approx(103.0)


def test_structure_value_without_momentum_weakening_does_not_trigger_weakening_state() -> None:
    snapshot = _snapshot(active_stop=100.0, protection_floor=100.5)

    intent = ProtectiveStopPlanner().plan(
        snapshot,
        atr=1.0,
        confirmed_structure_stop=103.0,
        momentum_weakened=False,
    )

    assert intent is not None
    assert intent.policy_state is GuardianPolicyState.PROFIT_PROTECTION


def test_stop_intent_is_explicitly_deferred_to_the_next_candle() -> None:
    intent = ProtectiveStopPlanner().plan(_snapshot(), atr=1.0)

    assert intent is not None
    assert intent.effective_from_next_candle is True


def test_initial_risk_state_is_reported_before_r_activation() -> None:
    snapshot = _snapshot(highest_price=101.5, reference_price=101.0)

    planner = ProtectiveStopPlanner()

    assert (
        planner.classify_policy_state(snapshot, atr=1.0)
        is GuardianPolicyState.INITIAL_RISK
    )
    assert planner.plan(snapshot, atr=1.0) is None


def test_mismatched_legacy_and_explicit_risk_stops_are_rejected() -> None:
    with pytest.raises(ValueError, match="must agree"):
        _snapshot(initial_stop=98.0, original_risk_stop=97.0)
