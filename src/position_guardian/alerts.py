from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import Literal, cast

from position_guardian.alert_contract import (
    GUARDIAN_RECONCILIATION_ALERT_SOURCE_V1,
    GUARDIAN_RECONCILIATION_REJECTION_ALERT_SOURCE_V1,
    GUARDIAN_SHADOW_CONTEXT_ALERT_SOURCE_V1,
    GUARDIAN_SHADOW_INTENT_ALERT_SOURCE_V1,
)
from position_guardian.domain import ManagedPositionIdentity, ManagedPositionSide
from position_guardian.reconcile import ReconciliationResult
from position_guardian.runtime import ShadowPlanningResult, managed_position_ref
from signalbot.domain.enums import Direction, Market

GUARDIAN_ALERT_SCHEMA_VERSION = "position_guardian_alert_v1"

GuardianAlertCode = Literal[
    "WOULD_UPDATE_STOP",
    "STALE_CONTEXT",
    "MANUAL_SIZE_INCREASE",
    "SIDE_FLIP",
    "PROTECTION_MISSING",
    "RECONCILIATION_UNCERTAIN",
]
GuardianAlertSeverity = Literal["INFO", "WARNING", "CRITICAL"]


@dataclass(frozen=True, slots=True)
class GuardianAlert:
    """Sanitized, deterministic shadow-only operator alert."""

    alert_id: str
    code: GuardianAlertCode
    severity: GuardianAlertSeverity
    event_time_ms: int
    symbol: str
    position_side: ManagedPositionSide
    adoption_generation: int
    message: str
    details: tuple[tuple[str, str], ...] = ()
    exchange_write_calls: int = 0

    def as_dict(self) -> dict[str, object]:
        return {
            "schema_version": GUARDIAN_ALERT_SCHEMA_VERSION,
            "alert_id": self.alert_id,
            "code": self.code,
            "severity": self.severity,
            "event_time_ms": self.event_time_ms,
            "symbol": self.symbol,
            "position_side": self.position_side,
            "adoption_generation": self.adoption_generation,
            "message": self.message,
            "details": dict(self.details),
            "exchange_write_calls": self.exchange_write_calls,
        }


def alerts_from_shadow_result(
    identity: ManagedPositionIdentity,
    result: ShadowPlanningResult,
    *,
    source_ref: str,
    event_time_ms: int,
) -> tuple[GuardianAlert, ...]:
    """Project actionable L60-04 shadow outcomes without dispatching or trading."""

    _validate_source(source_ref=source_ref, event_time_ms=event_time_ms)
    if result.exchange_write_calls != 0:
        raise ValueError("shadow alert source must report zero exchange writes")

    if result.disposition == "STOP_UPDATE_INTENT":
        intent = result.intent
        if intent is None:
            raise ValueError("STOP_UPDATE_INTENT result must include an intent")
        if intent.order_placed:
            raise ValueError("shadow alert source cannot contain an already placed order")
        if intent.market is not Market.FUTURES:
            raise ValueError("Guardian stop alerts require USD-M futures intents")
        expected_direction = Direction.LONG if identity.position_side == "LONG" else Direction.SHORT
        if intent.symbol != identity.symbol or intent.direction is not expected_direction:
            raise ValueError("shadow intent identity does not match managed position")
        if intent.position_ref != managed_position_ref(identity):
            raise ValueError("shadow intent position_ref does not match managed position")
        if not intent.reduce_only or intent.close_position or not intent.effective_from_next_candle:
            raise ValueError("shadow stop intent violates the protection-only contract")
        if result.context_id is None or not result.context_id.strip():
            raise ValueError("STOP_UPDATE_INTENT result must include context_id")
        details = (
            ("context_id", result.context_id),
            ("intent_id", intent.intent_id),
            ("policy_state", intent.policy_state.value),
            ("previous_stop", _number_text(intent.previous_stop)),
            ("proposed_stop", _number_text(intent.proposed_stop)),
            (
                "effective_from_next_candle",
                "true" if intent.effective_from_next_candle else "false",
            ),
        )
        return (
            _make_alert(
                identity=identity,
                code="WOULD_UPDATE_STOP",
                severity="INFO",
                source_ref=source_ref,
                event_time_ms=event_time_ms,
                message=(
                    f"[SHADOW] {identity.symbol} {identity.position_side} would update "
                    f"protective stop {_number_text(intent.previous_stop)} -> "
                    f"{_number_text(intent.proposed_stop)}; no exchange order was sent."
                ),
                details=details,
            ),
        )

    if result.intent is not None:
        raise ValueError("non-STOP_UPDATE_INTENT result must not include an intent")

    if result.disposition == "CONTEXT_REJECTED" and result.reason == "STALE_CONTEXT":
        return (
            _make_alert(
                identity=identity,
                code="STALE_CONTEXT",
                severity="WARNING",
                source_ref=source_ref,
                event_time_ms=event_time_ms,
                message=(
                    f"{identity.symbol} {identity.position_side} protection context is stale; "
                    "shadow stop planning remains blocked."
                ),
                details=(("reason", "STALE_CONTEXT"),),
            ),
        )

    if result.disposition == "CONTEXT_REJECTED" and result.reason == "CURSOR_UNCERTAIN":
        return (
            _make_alert(
                identity=identity,
                code="RECONCILIATION_UNCERTAIN",
                severity="CRITICAL",
                source_ref=source_ref,
                event_time_ms=event_time_ms,
                message=(
                    f"{identity.symbol} {identity.position_side} protection-context cursor is "
                    "uncertain; shadow stop planning remains blocked."
                ),
                details=(("reason", "CURSOR_UNCERTAIN"),),
            ),
        )

    return ()


def alerts_from_reconciliation_result(
    identity: ManagedPositionIdentity,
    result: ReconciliationResult,
    *,
    source_ref: str,
    event_time_ms: int,
) -> tuple[GuardianAlert, ...]:
    """Translate existing reconciliation signals into the frozen L60-05 alert set."""

    _validate_source(source_ref=source_ref, event_time_ms=event_time_ms)
    if result.exchange_write_calls != 0:
        raise ValueError("reconciliation alert source must report zero exchange writes")

    source_codes = set(result.alerts)
    if result.state == "DEGRADED":
        source_codes.add("RECONCILIATION_UNCERTAIN")

    alerts: list[GuardianAlert] = []
    if "RECONCILIATION_UNCERTAIN" in source_codes:
        alerts.append(
            _make_alert(
                identity=identity,
                code="RECONCILIATION_UNCERTAIN",
                severity="CRITICAL",
                source_ref=source_ref,
                event_time_ms=event_time_ms,
                message=(
                    f"{identity.symbol} {identity.position_side} reconciliation is uncertain; "
                    "operator review is required before protection changes."
                ),
                details=(("state", result.state),),
            )
        )
    if "SIDE_FLIP_REQUIRES_REAPPROVAL" in source_codes:
        alerts.append(
            _make_alert(
                identity=identity,
                code="SIDE_FLIP",
                severity="CRITICAL",
                source_ref=source_ref,
                event_time_ms=event_time_ms,
                message=(
                    f"{identity.symbol} managed side changed from {identity.position_side}; "
                    "the old generation requires release/re-approval."
                ),
                details=(("state", result.state),),
            )
        )
    if "PROTECTIVE_ORDER_MISSING" in source_codes:
        alerts.append(
            _make_alert(
                identity=identity,
                code="PROTECTION_MISSING",
                severity="CRITICAL",
                source_ref=source_ref,
                event_time_ms=event_time_ms,
                message=(
                    f"{identity.symbol} {identity.position_side} has no confirmed protective "
                    "order; Guardian remains alert-only."
                ),
                details=(("state", result.state),),
            )
        )
    if "MANUAL_POSITION_ADD" in source_codes:
        quantity = "UNKNOWN" if result.quantity is None else str(result.quantity)
        alerts.append(
            _make_alert(
                identity=identity,
                code="MANUAL_SIZE_INCREASE",
                severity="WARNING",
                source_ref=source_ref,
                event_time_ms=event_time_ms,
                message=(
                    f"{identity.symbol} {identity.position_side} manual position size increased; "
                    "Guardian will not expand protection automatically."
                ),
                details=(("observed_quantity", quantity), ("state", result.state)),
            )
        )
    return tuple(alerts)


def alerts_from_durable_event(
    *,
    event_type: str,
    payload_json: str,
    source_event_id: str,
    event_time_ms: int,
) -> tuple[GuardianAlert, ...]:
    """Rebuild L60-05 alerts only from immutable L60-06 ledger evidence."""

    _validate_source(source_ref=source_event_id, event_time_ms=event_time_ms)
    try:
        payload = json.loads(payload_json)
    except json.JSONDecodeError as exc:
        raise ValueError("durable Guardian alert source contains invalid JSON") from exc
    if not isinstance(payload, dict):
        raise ValueError("durable Guardian alert source must be a JSON object")

    if event_type == "ACCOUNT_SNAPSHOT":
        return _alerts_from_durable_account_snapshot(
            payload,
            source_event_id=source_event_id,
            event_time_ms=event_time_ms,
        )
    if event_type == "PLANNED_INTENT":
        return _alerts_from_durable_planned_intent(
            payload,
            source_event_id=source_event_id,
            event_time_ms=event_time_ms,
        )
    if event_type == "RECONCILIATION_ALERT_SOURCE":
        return _alerts_from_durable_reconciliation_rejection(
            payload,
            source_event_id=source_event_id,
            event_time_ms=event_time_ms,
        )
    if event_type == "SHADOW_ALERT_SOURCE":
        return _alerts_from_durable_shadow_source(
            payload,
            source_event_id=source_event_id,
            event_time_ms=event_time_ms,
        )
    return ()


def _alerts_from_durable_account_snapshot(
    payload: dict[str, object],
    *,
    source_event_id: str,
    event_time_ms: int,
) -> tuple[GuardianAlert, ...]:
    source = payload.get("alert_source")
    if source is None:
        return ()
    if not isinstance(source, dict):
        raise ValueError("ACCOUNT_SNAPSHOT alert_source must be an object")
    if source.get("schema_version") != GUARDIAN_RECONCILIATION_ALERT_SOURCE_V1:
        return ()
    identity = _identity_from_payload(payload.get("identity"))
    position = _required_dict(payload.get("position"), "ACCOUNT_SNAPSHOT position")
    projection_state = source.get("projection_state_before")
    if projection_state is None or projection_state in {"RELEASED", "CLOSED"}:
        return ()
    if projection_state != "ADOPTED":
        raise ValueError("unsupported durable reconciliation projection state")

    uncertainty_state = source.get("uncertainty_state")
    if uncertainty_state not in {"CERTAIN", "DEGRADED"}:
        raise ValueError("invalid durable reconciliation uncertainty state")
    protective_order_confirmed = source.get("protective_order_confirmed")
    shadow_mode = source.get("shadow_mode")
    if not isinstance(protective_order_confirmed, bool) or not isinstance(shadow_mode, bool):
        raise ValueError("invalid durable reconciliation boolean evidence")

    quantity = _decimal_from_payload(position.get("position_amount"), "position_amount").copy_abs()
    if uncertainty_state == "DEGRADED":
        result = ReconciliationResult(
            state="DEGRADED",
            quantity=quantity,
            alerts=("RECONCILIATION_UNCERTAIN",),
            operator_attention=True,
            snapshot_event_accepted=True,
        )
        return alerts_from_reconciliation_result(
            identity,
            result,
            source_ref=source_event_id,
            event_time_ms=event_time_ms,
        )
    if quantity == 0:
        return ()

    actual_side = _durable_position_side(position)
    if actual_side != identity.position_side:
        result = ReconciliationResult(
            state="RELEASED",
            quantity=quantity,
            alerts=("SIDE_FLIP_REQUIRES_REAPPROVAL",),
            operator_attention=True,
            snapshot_event_accepted=True,
        )
        return alerts_from_reconciliation_result(
            identity,
            result,
            source_ref=source_event_id,
            event_time_ms=event_time_ms,
        )

    source_alerts: list[str] = []
    previous_quantity_value = source.get("previous_quantity")
    if previous_quantity_value is not None:
        previous_quantity = _decimal_from_payload(
            previous_quantity_value, "previous_quantity"
        ).copy_abs()
        if quantity > previous_quantity:
            source_alerts.append("MANUAL_POSITION_ADD")
    if not protective_order_confirmed:
        source_alerts.append("PROTECTIVE_ORDER_MISSING")
    result = ReconciliationResult(
        state="MANAGED_SHADOW" if shadow_mode else "OBSERVED",
        quantity=quantity,
        alerts=tuple(source_alerts),
        operator_attention=bool(source_alerts),
        snapshot_event_accepted=True,
    )
    return alerts_from_reconciliation_result(
        identity,
        result,
        source_ref=source_event_id,
        event_time_ms=event_time_ms,
    )


def _alerts_from_durable_reconciliation_rejection(
    payload: dict[str, object],
    *,
    source_event_id: str,
    event_time_ms: int,
) -> tuple[GuardianAlert, ...]:
    identity = _identity_from_payload(payload.get("identity"))
    source = _required_dict(payload.get("payload"), "RECONCILIATION_ALERT_SOURCE payload")
    if source.get("schema_version") != GUARDIAN_RECONCILIATION_REJECTION_ALERT_SOURCE_V1:
        return ()
    reason = _required_text(source.get("reason"), "reason")
    if reason not in {
        "STALE_PRIVATE_SNAPSHOT",
        "CONFLICTING_PRIVATE_SNAPSHOT_CURSOR",
        "MISSING_PRIVATE_SNAPSHOT_CURSOR",
    }:
        raise ValueError("unsupported durable reconciliation rejection reason")
    result = ReconciliationResult(
        state="DEGRADED",
        quantity=None,
        alerts=("RECONCILIATION_UNCERTAIN",),
        operator_attention=True,
        snapshot_event_accepted=False,
    )
    return alerts_from_reconciliation_result(
        identity,
        result,
        source_ref=source_event_id,
        event_time_ms=event_time_ms,
    )


def _alerts_from_durable_planned_intent(
    payload: dict[str, object],
    *,
    source_event_id: str,
    event_time_ms: int,
) -> tuple[GuardianAlert, ...]:
    if payload.get("intent_type") != "STOP_ADJUSTMENT_SHADOW":
        return ()
    identity = _identity_from_payload(payload.get("identity"))
    intent = _required_dict(payload.get("payload"), "PLANNED_INTENT payload")
    if intent.get("alert_source_schema_version") != GUARDIAN_SHADOW_INTENT_ALERT_SOURCE_V1:
        return ()
    if intent.get("exchange_write_calls") != 0 or intent.get("order_placed") is not False:
        raise ValueError("durable shadow intent violates zero-write authority")
    if (
        intent.get("market") != Market.FUTURES.value
        or intent.get("symbol") != identity.symbol
        or intent.get("position_ref") != managed_position_ref(identity)
        or intent.get("reduce_only") is not True
        or intent.get("close_position") is not False
        or intent.get("effective_from_next_candle") is not True
    ):
        raise ValueError("durable shadow intent violates protection-only identity contract")
    expected_direction = (
        Direction.LONG.value if identity.position_side == "LONG" else Direction.SHORT.value
    )
    if intent.get("direction") != expected_direction:
        raise ValueError("durable shadow intent direction does not match managed identity")
    context_id = _required_text(intent.get("context_id"), "context_id")
    intent_id = _required_text(intent.get("intent_id"), "intent_id")
    policy_state = _required_text(intent.get("policy_state"), "policy_state")
    previous_stop = _number_payload_text(intent.get("previous_stop"), "previous_stop")
    proposed_stop = _number_payload_text(intent.get("proposed_stop"), "proposed_stop")
    details = (
        ("context_id", context_id),
        ("intent_id", intent_id),
        ("policy_state", policy_state),
        ("previous_stop", previous_stop),
        ("proposed_stop", proposed_stop),
        ("effective_from_next_candle", "true"),
    )
    return (
        _make_alert(
            identity=identity,
            code="WOULD_UPDATE_STOP",
            severity="INFO",
            source_ref=source_event_id,
            event_time_ms=event_time_ms,
            message=(
                f"[SHADOW] {identity.symbol} {identity.position_side} would update "
                f"protective stop {previous_stop} -> {proposed_stop}; "
                "no exchange order was sent."
            ),
            details=details,
        ),
    )


def _alerts_from_durable_shadow_source(
    payload: dict[str, object],
    *,
    source_event_id: str,
    event_time_ms: int,
) -> tuple[GuardianAlert, ...]:
    identity = _identity_from_payload(payload.get("identity"))
    source = _required_dict(payload.get("payload"), "SHADOW_ALERT_SOURCE payload")
    if source.get("schema_version") != GUARDIAN_SHADOW_CONTEXT_ALERT_SOURCE_V1:
        return ()
    reason = _required_text(source.get("reason"), "reason")
    if reason not in {"STALE_CONTEXT", "CURSOR_UNCERTAIN"}:
        return ()
    result = ShadowPlanningResult(
        disposition="CONTEXT_REJECTED",
        reason=reason,
        context_id=None,
        intent=None,
        intent_event_inserted=False,
        cursor_event_inserted=False,
    )
    return alerts_from_shadow_result(
        identity,
        result,
        source_ref=source_event_id,
        event_time_ms=event_time_ms,
    )


def _identity_from_payload(value: object) -> ManagedPositionIdentity:
    raw = _required_dict(value, "managed identity")
    account_alias = _required_text(raw.get("account_alias"), "account_alias")
    symbol = _required_text(raw.get("symbol"), "symbol")
    position_side = raw.get("position_side")
    generation = raw.get("adoption_generation")
    if position_side not in {"LONG", "SHORT"} or not isinstance(generation, int):
        raise ValueError("invalid durable managed identity")
    return ManagedPositionIdentity(
        account_alias=account_alias,
        symbol=symbol,
        position_side=cast(ManagedPositionSide, position_side),
        adoption_generation=generation,
    )


def _durable_position_side(position: dict[str, object]) -> ManagedPositionSide | None:
    amount = _decimal_from_payload(position.get("position_amount"), "position_amount")
    if amount == 0:
        return None
    position_side = position.get("position_side")
    if position_side == "BOTH":
        return "LONG" if amount > 0 else "SHORT"
    if position_side in {"LONG", "SHORT"} and amount > 0:
        return cast(ManagedPositionSide, position_side)
    return None


def _required_dict(value: object, field_name: str) -> dict[str, object]:
    if not isinstance(value, dict):
        raise ValueError(f"{field_name} must be an object")
    return value


def _required_text(value: object, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field_name} must be a nonblank string")
    return value


def _decimal_from_payload(value: object, field_name: str) -> Decimal:
    if not isinstance(value, (str, int, float)) or isinstance(value, bool):
        raise ValueError(f"{field_name} must be numeric")
    try:
        parsed = Decimal(str(value))
    except InvalidOperation as exc:
        raise ValueError(f"{field_name} must be numeric") from exc
    if not parsed.is_finite():
        raise ValueError(f"{field_name} must be finite")
    return parsed


def _number_payload_text(value: object, field_name: str) -> str:
    return format(_decimal_from_payload(value, field_name).normalize(), "f")


def _validate_source(*, source_ref: str, event_time_ms: int) -> None:
    if not source_ref.strip():
        raise ValueError("source_ref must not be blank")
    if len(source_ref) > 256:
        raise ValueError("source_ref must be at most 256 characters")
    if event_time_ms < 0:
        raise ValueError("event_time_ms must be non-negative")


def _make_alert(
    *,
    identity: ManagedPositionIdentity,
    code: GuardianAlertCode,
    severity: GuardianAlertSeverity,
    source_ref: str,
    event_time_ms: int,
    message: str,
    details: tuple[tuple[str, str], ...],
) -> GuardianAlert:
    ordered_details = tuple(sorted(details))
    identity_payload = {
        "account_alias": identity.account_alias,
        "symbol": identity.symbol,
        "position_side": identity.position_side,
        "adoption_generation": identity.adoption_generation,
    }
    identity_material = json.dumps(
        {
            "schema_version": GUARDIAN_ALERT_SCHEMA_VERSION,
            "identity": identity_payload,
            "source_ref": source_ref,
            "event_time_ms": event_time_ms,
            "code": code,
            "severity": severity,
            "message": message,
            "details": dict(ordered_details),
        },
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    )
    alert_id = hashlib.sha256(
        b"POSITION_GUARDIAN_ALERT_V1\0" + identity_material.encode("utf-8")
    ).hexdigest()
    return GuardianAlert(
        alert_id=alert_id,
        code=code,
        severity=severity,
        event_time_ms=event_time_ms,
        symbol=identity.symbol,
        position_side=identity.position_side,
        adoption_generation=identity.adoption_generation,
        message=message,
        details=ordered_details,
    )


def _number_text(value: float) -> str:
    return format(value, ".15g")
