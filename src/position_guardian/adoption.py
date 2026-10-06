from __future__ import annotations

from collections.abc import Iterable
from decimal import Decimal

from position_guardian.domain import (
    AdoptionCandidate,
    AdoptionDecision,
    AdoptionRejectionReason,
    ManagedPositionAllowlist,
    ManagedPositionSide,
    ProtectiveOrderReference,
)
from position_guardian.exchange.protocol import (
    AlgoOrderSnapshot,
    OpenOrderSnapshot,
    PositionModeSnapshot,
    PositionSnapshot,
)

_ACTIVE_STATUSES = frozenset({"NEW", "PARTIALLY_FILLED", "TRIGGER_PENDING"})
_STOP_ORDER_TYPES = frozenset({"STOP", "STOP_MARKET", "TRAILING_STOP_MARKET"})


def evaluate_adoption(
    *,
    allowlist: ManagedPositionAllowlist,
    mode: PositionModeSnapshot,
    position: PositionSnapshot,
    open_orders: Iterable[OpenOrderSnapshot] = (),
    open_algo_orders: Iterable[AlgoOrderSnapshot] = (),
) -> AdoptionDecision:
    """Return a safe candidate for an explicitly allowlisted live position.

    This function only evaluates exchange snapshots.  It never creates, cancels,
    or changes an order or position.
    """

    if position.symbol.strip().upper() != allowlist.symbol:
        return _reject("ALLOWLIST_MISMATCH")

    if (
        not position.position_amount.is_finite()
        or not position.entry_price.is_finite()
        or not position.mark_price.is_finite()
        or position.entry_price <= 0
        or position.mark_price <= 0
    ):
        return _reject("INVALID_POSITION")

    if mode.mode not in {"one_way", "hedge"}:
        return _reject("UNSUPPORTED_POSITION_MODE")

    side = _resolve_position_side(allowlist, mode, position)
    if isinstance(side, AdoptionDecision):
        return side

    quantity = abs(position.position_amount)
    if quantity == 0:
        return _reject("ZERO_QUANTITY")
    protective_orders = _protective_orders(
        symbol=allowlist.symbol,
        position_side=side,
        mode=mode,
        open_orders=open_orders,
        open_algo_orders=open_algo_orders,
    )
    if len(protective_orders) > 1:
        prices = {order.trigger_price for order in protective_orders}
        return _reject("DUPLICATE_PROTECTION" if len(prices) == 1 else "CONFLICTING_PROTECTION")

    exchange_stop = protective_orders[0] if protective_orders else None
    user_floor = allowlist.protection_floor
    if exchange_stop is None and user_floor is None:
        return _reject("MISSING_PROTECTION")

    if exchange_stop is not None and user_floor is not None:
        if _is_less_protective(user_floor, exchange_stop.trigger_price, side):
            return _reject("PROTECTION_FLOOR_CONFLICT")

    floor = _select_floor(side, exchange_stop, user_floor)
    assert floor is not None
    source = "exchange_stop" if user_floor is None else "user_floor"
    return AdoptionDecision(
        candidate=AdoptionCandidate(
            identity=allowlist.identity,
            quantity=quantity,
            entry_price=position.entry_price,
            mark_price=position.mark_price,
            source_update_time_ms=position.update_time_ms,
            original_risk_stop=(exchange_stop.trigger_price if exchange_stop else None),
            protection_floor=floor,
            protection_source=source,
            protective_order=exchange_stop,
        )
    )


def _resolve_position_side(
    allowlist: ManagedPositionAllowlist,
    mode: PositionModeSnapshot,
    position: PositionSnapshot,
) -> ManagedPositionSide | AdoptionDecision:
    if mode.mode == "one_way":
        if position.position_side != "BOTH":
            return _reject("POSITION_SIDE_MISMATCH")
        if position.position_amount > 0:
            actual_side: ManagedPositionSide = "LONG"
        elif position.position_amount < 0:
            actual_side = "SHORT"
        else:
            return _reject("ZERO_QUANTITY")
        if actual_side != allowlist.position_side:
            return _reject("SIDE_FLIP")
        return actual_side

    if position.position_side not in {"LONG", "SHORT"}:
        return _reject("POSITION_SIDE_MISMATCH")
    if position.position_amount <= 0:
        return _reject("SIDE_FLIP" if position.position_amount < 0 else "ZERO_QUANTITY")
    if position.position_side != allowlist.position_side:
        return _reject("SIDE_FLIP")
    if position.position_side == "LONG":
        return "LONG"
    return "SHORT"


def _protective_orders(
    *,
    symbol: str,
    position_side: ManagedPositionSide,
    mode: PositionModeSnapshot,
    open_orders: Iterable[OpenOrderSnapshot],
    open_algo_orders: Iterable[AlgoOrderSnapshot],
) -> list[ProtectiveOrderReference]:
    expected_exchange_side = "SELL" if position_side == "LONG" else "BUY"
    expected_position_side = "BOTH" if mode.mode == "one_way" else position_side
    found: list[ProtectiveOrderReference] = []

    for order in open_orders:
        if (
            order.symbol.upper() == symbol
            and order.position_side == expected_position_side
            and order.side == expected_exchange_side
            and order.status in _ACTIVE_STATUSES
            and order.order_type in _STOP_ORDER_TYPES
            and (order.reduce_only or order.close_position)
            and (order.close_position or order.quantity > 0)
            and order.stop_price is not None
            and order.stop_price > 0
        ):
            found.append(
                ProtectiveOrderReference(
                    source="open_order",
                    order_id=order.order_id,
                    trigger_price=order.stop_price,
                )
            )

    for order in open_algo_orders:
        if (
            order.symbol.upper() == symbol
            and order.position_side == expected_position_side
            and order.side == expected_exchange_side
            and order.status in _ACTIVE_STATUSES
            and order.order_type in _STOP_ORDER_TYPES
            and (order.reduce_only or order.close_position)
            and (order.close_position or order.quantity > 0)
            and order.trigger_price is not None
            and order.trigger_price > 0
        ):
            found.append(
                ProtectiveOrderReference(
                    source="algo_order",
                    order_id=order.algo_id,
                    trigger_price=order.trigger_price,
                )
            )
    return found


def _is_less_protective(
    requested_floor: Decimal,
    exchange_stop: Decimal,
    side: ManagedPositionSide,
) -> bool:
    return requested_floor < exchange_stop if side == "LONG" else requested_floor > exchange_stop


def _select_floor(
    side: ManagedPositionSide,
    exchange_stop: ProtectiveOrderReference | None,
    user_floor: Decimal | None,
) -> Decimal | None:
    values = [
        value
        for value in (exchange_stop.trigger_price if exchange_stop else None, user_floor)
        if value
    ]
    if not values:
        return None
    return max(values) if side == "LONG" else min(values)


def _reject(reason: AdoptionRejectionReason) -> AdoptionDecision:
    return AdoptionDecision(candidate=None, rejection_reason=reason)
