from __future__ import annotations

from dataclasses import dataclass, replace
from decimal import Decimal
from typing import Literal

from position_guardian.domain import AdoptionCandidate, ManagedPositionIdentity
from position_guardian.exchange.protocol import PositionSnapshot
from position_guardian.persistence.repository import GuardianRepository

ReconciliationState = Literal[
    "DISCOVERED",
    "ADOPTABLE",
    "OBSERVED",
    "MANAGED_SHADOW",
    "DEGRADED",
    "RELEASED",
    "CLOSED",
]
UncertaintyState = Literal["CERTAIN", "DEGRADED"]


@dataclass(frozen=True)
class ReconciliationRequest:
    identity: ManagedPositionIdentity
    position: PositionSnapshot
    snapshot_event_id: str
    event_time_ms: int
    created_at_ms: int
    protective_order_confirmed: bool
    shadow_mode: bool = True
    uncertainty_state: UncertaintyState = "CERTAIN"
    adoption_candidate: AdoptionCandidate | None = None
    release_event_id: str | None = None


@dataclass(frozen=True)
class ReconciliationResult:
    state: ReconciliationState
    quantity: Decimal | None
    alerts: tuple[str, ...]
    operator_attention: bool
    snapshot_event_accepted: bool
    exchange_write_calls: int = 0


class ReconciliationError(RuntimeError):
    """Raised when a terminal reconciliation needs an explicit event ID."""


def reconcile_once(
    repository: GuardianRepository,
    request: ReconciliationRequest,
) -> ReconciliationResult:
    """Reconcile one private snapshot without creating or changing an order."""

    projection = repository.get_projection(request.identity)
    snapshot_accepted = repository.record_account_snapshot(
        event_id=request.snapshot_event_id,
        event_time_ms=request.event_time_ms,
        created_at_ms=request.created_at_ms,
        identity=request.identity,
        position=request.position,
    )

    if projection is None:
        if (
            request.adoption_candidate is not None
            and request.adoption_candidate.identity == request.identity
        ):
            return _result(
                state="ADOPTABLE",
                quantity=_position_quantity(request.position),
                alerts=("ADOPTION_REQUIRES_EXPLICIT_COMMIT",),
                snapshot_event_accepted=snapshot_accepted,
            )
        return _result(
            state="DISCOVERED",
            quantity=_position_quantity(request.position),
            alerts=("NO_DURABLE_ADOPTION",),
            snapshot_event_accepted=snapshot_accepted,
        )

    if projection.state == "RELEASED":
        return _result(
            state="RELEASED",
            quantity=_position_quantity(request.position),
            alerts=("IDENTITY_RELEASED",),
            snapshot_event_accepted=snapshot_accepted,
        )
    if projection.state == "CLOSED":
        return _result(
            state="CLOSED",
            quantity=Decimal("0"),
            alerts=("IDENTITY_CLOSED",),
            snapshot_event_accepted=snapshot_accepted,
        )

    if request.uncertainty_state == "DEGRADED":
        return _result(
            state="DEGRADED",
            quantity=_position_quantity(request.position),
            alerts=("RECONCILIATION_UNCERTAIN",),
            operator_attention=True,
            snapshot_event_accepted=snapshot_accepted,
        )

    quantity = _position_quantity(request.position)
    actual_side = _actual_side(request.position)
    if quantity == 0:
        _release(
            repository,
            request,
            reason="POSITION_CLOSED",
        )
        return _result(
            state="CLOSED",
            quantity=Decimal("0"),
            alerts=("POSITION_CLOSED",),
            operator_attention=True,
            snapshot_event_accepted=snapshot_accepted,
        )
    if actual_side != request.identity.position_side:
        _release(
            repository,
            request,
            reason="SIDE_FLIP_REQUIRES_REAPPROVAL",
        )
        return _result(
            state="RELEASED",
            quantity=quantity,
            alerts=("SIDE_FLIP_REQUIRES_REAPPROVAL",),
            operator_attention=True,
            snapshot_event_accepted=snapshot_accepted,
        )

    alerts: list[str] = []
    previous_quantity = Decimal(projection.quantity)
    if quantity < previous_quantity:
        alerts.append("MANUAL_PARTIAL_CLOSE")
    elif quantity > previous_quantity:
        alerts.append("MANUAL_POSITION_ADD")
    if not request.protective_order_confirmed:
        alerts.append("PROTECTIVE_ORDER_MISSING")
    state: ReconciliationState = "MANAGED_SHADOW" if request.shadow_mode else "OBSERVED"
    return _result(
        state=state,
        quantity=quantity,
        alerts=tuple(alerts),
        operator_attention=bool(alerts),
        snapshot_event_accepted=snapshot_accepted,
    )


def apply_stream_health(
    request: ReconciliationRequest,
    health: Literal["HEALTHY", "DEGRADED", "DISCONNECTED", "EXPIRED"],
) -> ReconciliationRequest:
    """Convert stream loss/expiry into the existing REST-resync uncertainty gate."""

    return replace(request, uncertainty_state="CERTAIN" if health == "HEALTHY" else "DEGRADED")


def _release(
    repository: GuardianRepository,
    request: ReconciliationRequest,
    *,
    reason: str,
) -> None:
    if request.release_event_id is None:
        raise ReconciliationError(f"{reason} requires release_event_id")
    repository.record_release(
        event_id=request.release_event_id,
        event_time_ms=request.event_time_ms,
        created_at_ms=request.created_at_ms,
        identity=request.identity,
        reason=reason,
    )


def _actual_side(position: PositionSnapshot) -> Literal["LONG", "SHORT"] | None:
    if position.position_amount == 0:
        return None
    if position.position_side == "BOTH":
        return "LONG" if position.position_amount > 0 else "SHORT"
    if position.position_side in {"LONG", "SHORT"} and position.position_amount > 0:
        return "LONG" if position.position_side == "LONG" else "SHORT"
    return None


def _position_quantity(position: PositionSnapshot) -> Decimal:
    return abs(position.position_amount)


def _result(
    *,
    state: ReconciliationState,
    quantity: Decimal | None,
    alerts: tuple[str, ...],
    snapshot_event_accepted: bool,
    operator_attention: bool = False,
) -> ReconciliationResult:
    return ReconciliationResult(
        state=state,
        quantity=quantity,
        alerts=alerts,
        operator_attention=operator_attention,
        snapshot_event_accepted=snapshot_event_accepted,
    )
