from __future__ import annotations

from decimal import Decimal

import pytest

from position_guardian.adoption import evaluate_adoption
from position_guardian.domain import ManagedPositionAllowlist
from position_guardian.exchange.protocol import (
    AlgoOrderSnapshot,
    OpenOrderSnapshot,
    PositionModeSnapshot,
    PositionSnapshot,
)


def _position(
    *,
    amount: str,
    side: str = "BOTH",
    symbol: str = "BTCUSDT",
    entry: str = "60000",
    mark: str = "61000",
) -> PositionSnapshot:
    return PositionSnapshot(
        symbol=symbol,
        position_side=side,  # type: ignore[arg-type]
        position_amount=Decimal(amount),
        entry_price=Decimal(entry),
        mark_price=Decimal(mark),
        unrealized_profit=Decimal("10"),
        update_time_ms=1700000000000,
    )


def _stop(
    *,
    price: str,
    side: str,
    position_side: str = "BOTH",
    order_id: int = 1,
    quantity: str = "0.01",
    close_position: bool = False,
) -> OpenOrderSnapshot:
    return OpenOrderSnapshot(
        order_id=order_id,
        symbol="BTCUSDT",
        position_side=position_side,  # type: ignore[arg-type]
        side=side,  # type: ignore[arg-type]
        order_type="STOP_MARKET",
        status="NEW",
        quantity=Decimal(quantity),
        stop_price=Decimal(price),
        close_position=close_position,
        reduce_only=True,
    )


def _allowlist(
    side: str,
    *,
    floor: str | None = None,
    generation: int = 7,
) -> ManagedPositionAllowlist:
    return ManagedPositionAllowlist(
        account_alias="manual-main",
        symbol="btcusdt",
        position_side=side,  # type: ignore[arg-type]
        adoption_generation=generation,
        protection_floor=Decimal(floor) if floor else None,
    )


def test_one_way_long_and_short_are_adoptable_with_active_stops() -> None:
    long_decision = evaluate_adoption(
        allowlist=_allowlist("LONG"),
        mode=PositionModeSnapshot(mode="one_way"),
        position=_position(amount="0.01"),
        open_orders=[_stop(price="59000", side="SELL")],
    )
    short_decision = evaluate_adoption(
        allowlist=_allowlist("SHORT"),
        mode=PositionModeSnapshot(mode="one_way"),
        position=_position(amount="-0.01", entry="60000", mark="59000"),
        open_orders=[_stop(price="61000", side="BUY")],
    )

    assert long_decision.adoptable and short_decision.adoptable
    assert long_decision.candidate is not None
    assert short_decision.candidate is not None
    assert long_decision.candidate.identity.position_side == "LONG"
    assert short_decision.candidate.identity.position_side == "SHORT"
    assert short_decision.candidate.quantity == Decimal("0.01")


@pytest.mark.parametrize("side,amount", [("LONG", "0.01"), ("SHORT", "0.01")])
def test_hedge_long_and_short_use_exchange_position_side(side: str, amount: str) -> None:
    decision = evaluate_adoption(
        allowlist=_allowlist(side),
        mode=PositionModeSnapshot(mode="hedge"),
        position=_position(amount=amount, side=side),
        open_orders=[
            _stop(
                price="59000" if side == "LONG" else "61000",
                side="SELL" if side == "LONG" else "BUY",
                position_side=side,
            )
        ],
    )

    assert decision.adoptable
    assert decision.candidate is not None
    assert decision.candidate.identity.adoption_generation == 7


def test_no_stop_is_adoptable_only_with_explicit_user_floor() -> None:
    decision = evaluate_adoption(
        allowlist=_allowlist("LONG", floor="60500"),
        mode=PositionModeSnapshot(mode="one_way"),
        position=_position(amount="0.01"),
    )

    assert decision.adoptable
    assert decision.candidate is not None
    assert decision.candidate.original_risk_stop is None
    assert decision.candidate.protection_floor == Decimal("60500")
    assert decision.candidate.protection_source == "user_floor"


def test_no_stop_without_floor_is_rejected() -> None:
    decision = evaluate_adoption(
        allowlist=_allowlist("LONG"),
        mode=PositionModeSnapshot(mode="one_way"),
        position=_position(amount="0.01"),
    )

    assert not decision.adoptable
    assert decision.rejection_reason == "MISSING_PROTECTION"


def test_duplicate_and_conflicting_stops_are_not_auto_adopted() -> None:
    duplicate = evaluate_adoption(
        allowlist=_allowlist("LONG"),
        mode=PositionModeSnapshot(mode="one_way"),
        position=_position(amount="0.01"),
        open_orders=[
            _stop(price="59000", side="SELL", order_id=1),
            _stop(price="59000", side="SELL", order_id=2),
        ],
    )
    conflicting = evaluate_adoption(
        allowlist=_allowlist("LONG"),
        mode=PositionModeSnapshot(mode="one_way"),
        position=_position(amount="0.01"),
        open_orders=[
            _stop(price="59000", side="SELL", order_id=1),
            _stop(price="58000", side="SELL", order_id=2),
        ],
    )

    assert duplicate.rejection_reason == "DUPLICATE_PROTECTION"
    assert conflicting.rejection_reason == "CONFLICTING_PROTECTION"


def test_partial_position_is_adopted_and_zero_position_is_rejected() -> None:
    partial = evaluate_adoption(
        allowlist=_allowlist("LONG"),
        mode=PositionModeSnapshot(mode="one_way"),
        position=_position(amount="0.001"),
        open_orders=[_stop(price="59000", side="SELL", quantity="0.001")],
    )
    zero = evaluate_adoption(
        allowlist=_allowlist("LONG"),
        mode=PositionModeSnapshot(mode="one_way"),
        position=_position(amount="0"),
        open_orders=[_stop(price="59000", side="SELL")],
    )

    assert partial.adoptable
    assert partial.candidate is not None
    assert partial.candidate.quantity == Decimal("0.001")
    assert zero.rejection_reason == "ZERO_QUANTITY"


def test_flipped_one_way_side_is_rejected() -> None:
    decision = evaluate_adoption(
        allowlist=_allowlist("LONG"),
        mode=PositionModeSnapshot(mode="one_way"),
        position=_position(amount="-0.01", mark="59000"),
        open_orders=[_stop(price="61000", side="BUY")],
    )

    assert decision.rejection_reason == "SIDE_FLIP"


def test_profitable_long_and_short_floors_can_cross_entry_in_protective_direction() -> None:
    long_decision = evaluate_adoption(
        allowlist=_allowlist("LONG", floor="60500"),
        mode=PositionModeSnapshot(mode="one_way"),
        position=_position(amount="0.01", entry="60000", mark="62000"),
    )
    short_decision = evaluate_adoption(
        allowlist=_allowlist("SHORT", floor="59500"),
        mode=PositionModeSnapshot(mode="one_way"),
        position=_position(amount="-0.01", entry="60000", mark="58000"),
    )

    assert long_decision.candidate is not None
    assert short_decision.candidate is not None
    assert long_decision.candidate.protection_floor > long_decision.candidate.entry_price
    assert short_decision.candidate.protection_floor < short_decision.candidate.entry_price


def test_user_floor_cannot_weaken_an_exchange_stop() -> None:
    decision = evaluate_adoption(
        allowlist=_allowlist("LONG", floor="58000"),
        mode=PositionModeSnapshot(mode="one_way"),
        position=_position(amount="0.01"),
        open_orders=[_stop(price="59000", side="SELL")],
    )

    assert decision.rejection_reason == "PROTECTION_FLOOR_CONFLICT"


def test_algo_stop_is_supported_and_unrelated_order_is_ignored() -> None:
    algo = AlgoOrderSnapshot(
        algo_id=44,
        symbol="BTCUSDT",
        position_side="BOTH",
        side="SELL",
        order_type="STOP_MARKET",
        status="TRIGGER_PENDING",
        quantity=Decimal("0"),
        trigger_price=Decimal("59000"),
        close_position=True,
        reduce_only=False,
    )
    unrelated = _stop(price="59000", side="BUY", order_id=99)
    decision = evaluate_adoption(
        allowlist=_allowlist("LONG"),
        mode=PositionModeSnapshot(mode="one_way"),
        position=_position(amount="0.01"),
        open_orders=[unrelated],
        open_algo_orders=[algo],
    )

    assert decision.adoptable
    assert decision.candidate is not None
    assert decision.candidate.protective_order is not None
    assert decision.candidate.protective_order.source == "algo_order"
