from __future__ import annotations

import hashlib
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Literal

from position_guardian.config import GuardianSettings, settings_summary
from position_guardian.context_client import (
    ProtectionContextRejected,
    validate_protection_context,
)
from position_guardian.domain import ManagedPositionIdentity
from position_guardian.persistence.repository import GuardianRepository
from position_guardian.planner import ShadowPlanningError, plan_shadow_stop
from position_guardian.reconcile import ReconciliationResult
from signalbot.domain.enums import Direction, Market
from signalbot.signals.position_management import ManagedPositionSnapshot, StopUpdateIntent
from signalbot.signals.protection_context import ProtectionContext

ShadowRuntimeDisposition = Literal[
    "STOP_UPDATE_INTENT",
    "NO_STOP_UPDATE",
    "CONTEXT_REJECTED",
    "SNAPSHOT_REJECTED",
]


@dataclass(frozen=True, slots=True)
class ShadowPlanningRequest:
    """One deterministic shadow-planning unit with no exchange write capability."""

    identity: ManagedPositionIdentity
    snapshot: ManagedPositionSnapshot
    context_payload: Mapping[str, object] | ProtectionContext
    now_ms: int
    max_context_age_ms: int
    primary_interval: str = "5m"


@dataclass(frozen=True, slots=True)
class ShadowPlanningResult:
    disposition: ShadowRuntimeDisposition
    reason: str
    context_id: str | None
    intent: StopUpdateIntent | None
    intent_event_inserted: bool
    cursor_event_inserted: bool
    exchange_write_calls: int = 0


def build_dry_run_report(settings: GuardianSettings) -> dict[str, object]:
    """Build a deterministic L50-01 runtime report without any I/O side effects."""

    return {
        "schema_version": "position_guardian_dry_run_v1",
        "mode": "dry_run",
        "configuration": settings_summary(settings),
        "network_calls": 0,
        "private_account_reads": 0,
        "exchange_write_calls": 0,
        "runtime_started": False,
    }


def build_reconciliation_report(
    settings: GuardianSettings,
    result: ReconciliationResult,
) -> dict[str, object]:
    """Project a read-only reconciliation result without exchange side effects."""

    return {
        "schema_version": "position_guardian_reconciliation_v1",
        "configuration": settings_summary(settings),
        "state": result.state,
        "quantity": str(result.quantity) if result.quantity is not None else None,
        "alerts": list(result.alerts),
        "operator_attention": result.operator_attention,
        "snapshot_event_accepted": result.snapshot_event_accepted,
        "exchange_write_calls": result.exchange_write_calls,
    }


def plan_shadow_once(
    repository: GuardianRepository,
    request: ShadowPlanningRequest,
) -> ShadowPlanningResult:
    """Validate, plan and persist one Guardian shadow intent without exchange I/O."""

    snapshot_rejection = _snapshot_rejection_reason(repository, request)
    if snapshot_rejection is not None:
        return ShadowPlanningResult(
            disposition="SNAPSHOT_REJECTED",
            reason=snapshot_rejection,
            context_id=None,
            intent=None,
            intent_event_inserted=False,
            cursor_event_inserted=False,
        )

    cursor_name = _context_cursor_name(
        market=request.snapshot.market,
        symbol=request.snapshot.symbol,
        primary_interval=request.primary_interval,
    )
    previous_cursor = repository.get_cursor(cursor_name)
    if previous_cursor is not None and previous_cursor.uncertainty_state != "CERTAIN":
        return ShadowPlanningResult(
            disposition="CONTEXT_REJECTED",
            reason="CURSOR_UNCERTAIN",
            context_id=None,
            intent=None,
            intent_event_inserted=False,
            cursor_event_inserted=False,
        )

    try:
        validated = validate_protection_context(
            request.context_payload,
            expected_market=request.snapshot.market,
            expected_symbol=request.snapshot.symbol,
            expected_primary_interval=request.primary_interval,
            now_ms=request.now_ms,
            max_age_ms=request.max_context_age_ms,
            previous_cursor=(previous_cursor.cursor_value if previous_cursor is not None else None),
        )
        shadow_plan = plan_shadow_stop(request.snapshot, validated)
    except ProtectionContextRejected as exc:
        return ShadowPlanningResult(
            disposition="CONTEXT_REJECTED",
            reason=exc.reason,
            context_id=None,
            intent=None,
            intent_event_inserted=False,
            cursor_event_inserted=False,
        )
    except ShadowPlanningError as exc:
        return ShadowPlanningResult(
            disposition="SNAPSHOT_REJECTED",
            reason=str(exc),
            context_id=None,
            intent=None,
            intent_event_inserted=False,
            cursor_event_inserted=False,
        )

    intent_inserted = False
    if shadow_plan.intent is not None:
        intent_inserted = repository.record_planned_intent(
            event_id=_planned_intent_event_id(request.identity, shadow_plan.intent.intent_id),
            event_time_ms=validated.context.candle_close_time_ms,
            created_at_ms=request.now_ms,
            identity=request.identity,
            intent_type="STOP_ADJUSTMENT_SHADOW",
            payload=_intent_payload(shadow_plan.intent, context_id=validated.context.context_id),
        )

    cursor_value = validated.cursor.serialize()
    cursor_inserted = repository.record_reconciliation_cursor(
        event_id=_context_cursor_event_id(cursor_name, validated.context.context_id),
        event_time_ms=validated.context.candle_close_time_ms,
        created_at_ms=request.now_ms,
        cursor_name=cursor_name,
        cursor_value=cursor_value,
        uncertainty_state="CERTAIN",
    )
    return ShadowPlanningResult(
        disposition=shadow_plan.disposition,
        reason=shadow_plan.disposition,
        context_id=validated.context.context_id,
        intent=shadow_plan.intent,
        intent_event_inserted=intent_inserted,
        cursor_event_inserted=cursor_inserted,
    )


def managed_position_ref(identity: ManagedPositionIdentity) -> str:
    """Return the canonical Guardian reference for one adopted position generation."""

    return ":".join(
        (
            identity.account_alias,
            identity.symbol,
            identity.position_side,
            str(identity.adoption_generation),
        )
    )


def _snapshot_rejection_reason(
    repository: GuardianRepository,
    request: ShadowPlanningRequest,
) -> str | None:
    projection = repository.get_projection(request.identity)
    if projection is None or projection.state != "ADOPTED":
        return "POSITION_NOT_MANAGED"
    if request.snapshot.position_ref != managed_position_ref(request.identity):
        return "MANAGED_IDENTITY_POSITION_REF_MISMATCH"
    if request.identity.symbol != request.snapshot.symbol:
        return "MANAGED_IDENTITY_SYMBOL_MISMATCH"
    expected_direction = (
        Direction.LONG if request.identity.position_side == "LONG" else Direction.SHORT
    )
    if request.snapshot.direction is not expected_direction:
        return "MANAGED_IDENTITY_SIDE_MISMATCH"
    if request.snapshot.market is not Market.FUTURES:
        return "MANAGED_IDENTITY_MARKET_MISMATCH"
    return None


def _context_cursor_name(*, market: Market, symbol: str, primary_interval: str) -> str:
    return f"protection-context:{market.value}:{symbol}:{primary_interval}"


def _context_cursor_event_id(cursor_name: str, context_id: str) -> str:
    return hashlib.sha256(f"{cursor_name}|{context_id}".encode()).hexdigest()


def _planned_intent_event_id(identity: ManagedPositionIdentity, intent_id: str) -> str:
    payload = "|".join(
        (
            identity.account_alias,
            identity.symbol,
            identity.position_side,
            str(identity.adoption_generation),
            intent_id,
        )
    )
    return hashlib.sha256(payload.encode()).hexdigest()


def _intent_payload(intent: StopUpdateIntent, *, context_id: str) -> dict[str, object]:
    return {
        "intent_id": intent.intent_id,
        "context_id": context_id,
        "policy_version": intent.policy_version,
        "position_ref": intent.position_ref,
        "market": intent.market.value,
        "symbol": intent.symbol,
        "direction": intent.direction.value,
        "previous_stop": intent.previous_stop,
        "proposed_stop": intent.proposed_stop,
        "reference_price": intent.reference_price,
        "observed_at_ms": intent.observed_at_ms,
        "reason": intent.reason,
        "reduce_only": intent.reduce_only,
        "close_position": intent.close_position,
        "order_placed": intent.order_placed,
        "policy_state": intent.policy_state.value,
        "effective_from_next_candle": intent.effective_from_next_candle,
        "exchange_write_calls": 0,
    }
