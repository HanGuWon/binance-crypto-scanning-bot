from __future__ import annotations

import hashlib
from dataclasses import dataclass, replace
from decimal import Decimal
from typing import Literal

from position_guardian.alert_contract import (
    GUARDIAN_RECONCILIATION_REJECTION_ALERT_SOURCE_V1,
)
from position_guardian.domain import AdoptionCandidate, ManagedPositionIdentity
from position_guardian.exchange.binance_user_stream import RestResyncResult
from position_guardian.exchange.protocol import PositionSnapshot
from position_guardian.persistence.repository import (
    GuardianRepository,
    GuardianSnapshotOrderError,
)

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
    terminal_reason: str | None = None
    if projection is not None and request.uncertainty_state == "CERTAIN":
        quantity_before_write = _position_quantity(request.position)
        actual_side_before_write = _actual_side(request.position)
        terminal_reason = (
            "POSITION_CLOSED"
            if quantity_before_write == 0
            else (
                "SIDE_FLIP_REQUIRES_REAPPROVAL"
                if actual_side_before_write != request.identity.position_side
                else None
            )
        )
        if (
            projection.state not in {"RELEASED", "CLOSED"}
            and terminal_reason is not None
            and request.release_event_id is None
        ):
            raise ReconciliationError(f"{terminal_reason} requires release_event_id")
        if projection.state in {"RELEASED", "CLOSED"} and request.release_event_id is None:
            terminal_reason = None

    try:
        snapshot_accepted = repository.record_account_snapshot(
            event_id=request.snapshot_event_id,
            event_time_ms=request.event_time_ms,
            created_at_ms=request.created_at_ms,
            identity=request.identity,
            position=request.position,
            protective_order_confirmed=request.protective_order_confirmed,
            uncertainty_state=request.uncertainty_state,
            shadow_mode=request.shadow_mode,
            terminal_release_event_id=(
                request.release_event_id if terminal_reason is not None else None
            ),
            terminal_release_reason=terminal_reason,
        )
    except GuardianSnapshotOrderError as exc:
        reason = (
            "STALE_PRIVATE_SNAPSHOT"
            if "regressed" in str(exc)
            else (
                "MISSING_PRIVATE_SNAPSHOT_CURSOR"
                if "missing" in str(exc)
                else "CONFLICTING_PRIVATE_SNAPSHOT_CURSOR"
            )
        )
        source_event_id = hashlib.sha256(
            (
                "GUARDIAN_RECONCILIATION_REJECTION_V1\0"
                f"{request.snapshot_event_id}|{request.event_time_ms}|{reason}"
            ).encode()
        ).hexdigest()
        repository.record_reconciliation_alert_source(
            event_id=source_event_id,
            event_time_ms=request.event_time_ms,
            created_at_ms=request.created_at_ms,
            identity=request.identity,
            payload={
                "schema_version": GUARDIAN_RECONCILIATION_REJECTION_ALERT_SOURCE_V1,
                "reason": reason,
                "snapshot_event_id": request.snapshot_event_id,
                "position_side": request.position.position_side,
                "position_amount": str(request.position.position_amount),
                "entry_price": str(request.position.entry_price),
                "update_time_ms": request.position.update_time_ms,
                "protective_order_confirmed": request.protective_order_confirmed,
                "uncertainty_state": request.uncertainty_state,
                "shadow_mode": request.shadow_mode,
            },
        )
        _materialize_reconciliation_alerts_from_source(
            repository,
            source_event_id=source_event_id,
        )
        return _result(
            state="DEGRADED",
            quantity=_position_quantity(request.position),
            alerts=("RECONCILIATION_UNCERTAIN",),
            operator_attention=True,
            snapshot_event_accepted=False,
        )
    _materialize_reconciliation_alerts(repository, request)

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
        return _result(
            state="CLOSED",
            quantity=Decimal("0"),
            alerts=("POSITION_CLOSED",),
            operator_attention=True,
            snapshot_event_accepted=snapshot_accepted,
        )
    if actual_side != request.identity.position_side:
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
    """Convert stream loss into uncertainty; health alone cannot prove certainty."""

    del health
    return replace(request, uncertainty_state="DEGRADED")


def apply_rest_resync_result(
    request: ReconciliationRequest,
    result: RestResyncResult,
) -> ReconciliationRequest:
    """Apply only the buffer's explicit authoritative REST fence result."""

    return replace(
        request,
        uncertainty_state="CERTAIN" if result.certainty_restored else "DEGRADED",
    )


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


def _materialize_reconciliation_alerts(
    repository: GuardianRepository,
    request: ReconciliationRequest,
) -> None:
    from position_guardian.alert_recovery import materialize_guardian_alerts_for_source_event

    materialize_guardian_alerts_for_source_event(
        repository,
        source_event_id=request.snapshot_event_id,
    )


def _materialize_reconciliation_alerts_from_source(
    repository: GuardianRepository,
    *,
    source_event_id: str,
) -> None:
    from position_guardian.alert_recovery import materialize_guardian_alerts_for_source_event

    materialize_guardian_alerts_for_source_event(
        repository,
        source_event_id=source_event_id,
    )
