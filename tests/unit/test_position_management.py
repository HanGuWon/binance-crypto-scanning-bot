from __future__ import annotations

import pytest

from signalbot.domain.enums import Direction, Market
from signalbot.signals.position_management import (
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
