"""Manual, nonexecuting ladder budget and supplied-price liquidation advisory."""

from __future__ import annotations

import math
from dataclasses import dataclass
from enum import StrEnum

from signalbot.pump_fade_v2.state import State


class Side(StrEnum):
    SHORT = "SHORT"
    LONG = "LONG"


@dataclass(frozen=True, slots=True)
class Fill:
    """One actual execution. Multiple executions may belong to one addition ID."""

    fill_id: str
    tranche_id: str
    quantity: float
    price: float
    at_ms: int

    def __post_init__(self) -> None:
        if (not self.fill_id or not self.tranche_id or self.quantity <= 0
                or self.price <= 0 or not math.isfinite(self.quantity)
                or not math.isfinite(self.price)):
            raise ValueError("fill identity, quantity and price must be positive and finite")


@dataclass(frozen=True, slots=True)
class FrozenLadder:
    """Initial quantity, invalidation and *USDT* risk fixed at first entry."""

    side: Side
    initial_fill: Fill
    invalidation_price: float
    max_loss_usdt: float
    conservative_round_trip_cost_bps: float = 30

    def __post_init__(self) -> None:
        if not (math.isfinite(self.max_loss_usdt) and self.max_loss_usdt > 0
                and math.isfinite(self.invalidation_price)
                and self.invalidation_price > 0
                and 0 <= self.conservative_round_trip_cost_bps <= 10_000):
            raise ValueError("invalid risk budget, stop, or cost allowance")
        _require_adverse_stop(self.side, self.invalidation_price, self.initial_fill.price)


@dataclass(frozen=True, slots=True)
class LadderAssessment:
    allowed: bool
    reasons: tuple[str, ...]
    admitted_additions: int
    used_quantity: float
    remaining_quantity: float
    used_risk_usdt: float
    remaining_risk_usdt: float
    invalidation_price: float
    maximum_proposed_quantity: float | None
    next_condition: str


def _require_adverse_stop(side: Side, stop: float, price: float) -> None:
    if (side is Side.SHORT and stop <= price) or (side is Side.LONG and stop >= price):
        raise ValueError("invalidation must be on the adverse side of every fill")


def _risk_usdt(plan: FrozenLadder, fill: Fill) -> float:
    _require_adverse_stop(plan.side, plan.invalidation_price, fill.price)
    stop_distance = abs(plan.invalidation_price - fill.price)
    fees_and_slip = plan.conservative_round_trip_cost_bps / 10_000 * fill.price
    return fill.quantity * (stop_distance + fees_and_slip)


def assess_ladder(
    plan: FrozenLadder,
    existing: tuple[Fill, ...],
    *,
    proposed: Fill | None = None,
    candidate_state: State = State.FADE_CANDIDATE,
    continuation_risk: bool = False,
    renewed_confirmation: bool = False,
) -> LadderAssessment:
    """Price and size check for new *risk increasing* additions.

    Existing fills are immutable records. Proposed partial fills sharing a tranche
    count towards its total, but one tranche counts as exactly one addition.
    """

    if len({f.fill_id for f in (plan.initial_fill, *existing)}) != 1 + len(existing):
        raise ValueError("duplicate actual fill identity")
    initial = plan.initial_fill
    if any(fill.tranche_id == initial.tranche_id for fill in existing):
        raise ValueError("initial fill aggregation must be frozen at plan creation")
    counts: dict[str, float] = {}
    for fill in existing:
        counts[fill.tranche_id] = counts.get(fill.tranche_id, 0.0) + fill.quantity
    if len(counts) > 2 or any(quantity > 0.5 * initial.quantity + 1e-12
                              for quantity in counts.values()):
        raise ValueError("existing additions exceed the frozen primary policy")
    existing_quantity = initial.quantity + sum(fill.quantity for fill in existing)
    used_risk = sum(_risk_usdt(plan, fill) for fill in (initial, *existing))
    if (existing_quantity > 2 * initial.quantity + 1e-12
            or used_risk > plan.max_loss_usdt + 1e-10):
        raise ValueError("existing risk has already breached frozen plan")
    reasons: list[str] = []
    allowed = proposed is not None
    maximum_proposed_quantity: float | None = None
    if proposed is not None:
        if proposed.fill_id in {f.fill_id for f in (initial, *existing)}:
            raise ValueError("proposed fill ID already exists")
        if proposed.tranche_id == initial.tranche_id:
            reasons.append("initial_tranche_is_frozen")
        if candidate_state is not State.FADE_CANDIDATE or continuation_risk:
            reasons.append("new_risk_blocked_by_state")
        if not renewed_confirmation:
            reasons.append("renewed_confirmation_required")
        proposed_tranche_total = counts.get(proposed.tranche_id, 0.0) + proposed.quantity
        if proposed_tranche_total > 0.5 * initial.quantity + 1e-12:
            reasons.append("addition_exceeds_half_initial_quantity")
        if proposed.tranche_id not in counts and len(counts) >= 2:
            reasons.append("addition_count_limit")
        if existing_quantity + proposed.quantity > 2 * initial.quantity + 1e-12:
            reasons.append("total_quantity_limit")
        try:
            proposed_risk = _risk_usdt(plan, proposed)
        except ValueError:
            reasons.append("invalidation_not_adverse_to_proposed_price")
        else:
            unit_risk = proposed_risk / proposed.quantity
            tranche_room = max(
                0.0,
                0.5 * initial.quantity - counts.get(proposed.tranche_id, 0.0),
            )
            addition_slot_room = (
                tranche_room
                if proposed.tranche_id in counts or len(counts) < 2
                else 0.0
            )
            maximum_proposed_quantity = max(
                0.0,
                min(
                    2 * initial.quantity - existing_quantity,
                    addition_slot_room,
                    (plan.max_loss_usdt - used_risk) / unit_risk,
                ),
            )
            if used_risk + proposed_risk > plan.max_loss_usdt + 1e-10:
                reasons.append("frozen_loss_budget_exceeded")
        allowed = not reasons
    return LadderAssessment(
        allowed=allowed, reasons=tuple(reasons), admitted_additions=len(counts),
        used_quantity=existing_quantity,
        remaining_quantity=max(0, 2 * initial.quantity - existing_quantity),
        used_risk_usdt=used_risk,
        remaining_risk_usdt=max(0, plan.max_loss_usdt - used_risk),
        invalidation_price=plan.invalidation_price,
        maximum_proposed_quantity=maximum_proposed_quantity,
        next_condition="fresh FADE-CANDIDATE release and budget check",
    )


@dataclass(frozen=True, slots=True)
class LiquidationAdvisory:
    status: str
    side: Side
    liquidation_price: float | None
    mark_price: float | None
    adverse_distance_pct: float | None
    margin_usdt: float | None


def supplied_liquidation_advisory(
    side: Side,
    *,
    liquidation_price: float | None,
    mark_price: float | None,
    mark_received_ms: int | None,
    now_ms: int,
    margin_usdt: float | None = None,
) -> LiquidationAdvisory:
    """Use user-supplied liquidation price; never derive price from leverage."""

    if (liquidation_price is None or mark_price is None or mark_received_ms is None
            or not 0 <= now_ms - mark_received_ms <= 5_000
            or liquidation_price <= 0 or mark_price <= 0
            or (side is Side.SHORT and liquidation_price <= mark_price)
            or (side is Side.LONG and liquidation_price >= mark_price)):
        return LiquidationAdvisory("UNKNOWN", side, None, mark_price, None, margin_usdt)
    distance = (liquidation_price - mark_price if side is Side.SHORT
                else mark_price - liquidation_price) / mark_price * 100
    return LiquidationAdvisory("USER_SUPPLIED_ESTIMATE", side, liquidation_price,
                               mark_price, distance, margin_usdt)
