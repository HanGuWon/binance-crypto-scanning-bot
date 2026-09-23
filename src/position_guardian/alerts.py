from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from typing import Literal

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
