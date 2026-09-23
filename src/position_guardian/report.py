from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping

from position_guardian.alerts import (
    GuardianAlert,
    alerts_from_reconciliation_result,
    alerts_from_shadow_result,
)
from position_guardian.domain import ManagedPositionIdentity
from position_guardian.reconcile import ReconciliationResult
from position_guardian.runtime import ShadowPlanningResult

GUARDIAN_OPERATIONS_REPORT_SCHEMA_VERSION = "position_guardian_operations_report_v1"

_ALERT_CODES = (
    "WOULD_UPDATE_STOP",
    "STALE_CONTEXT",
    "MANUAL_SIZE_INCREASE",
    "SIDE_FLIP",
    "PROTECTION_MISSING",
    "RECONCILIATION_UNCERTAIN",
)
_SAFE_RECONCILIATION_CODES = frozenset(
    {
        "ADOPTION_REQUIRES_EXPLICIT_COMMIT",
        "NO_DURABLE_ADOPTION",
        "IDENTITY_RELEASED",
        "IDENTITY_CLOSED",
        "RECONCILIATION_UNCERTAIN",
        "POSITION_CLOSED",
        "SIDE_FLIP_REQUIRES_REAPPROVAL",
        "MANUAL_PARTIAL_CLOSE",
        "MANUAL_POSITION_ADD",
        "PROTECTIVE_ORDER_MISSING",
    }
)
_SAFE_SHADOW_REASON_CODES = frozenset(
    {
        "STOP_UPDATE_INTENT",
        "NO_STOP_UPDATE",
        "CURSOR_UNCERTAIN",
        "POSITION_NOT_MANAGED",
        "MANAGED_IDENTITY_POSITION_REF_MISMATCH",
        "MANAGED_IDENTITY_SYMBOL_MISMATCH",
        "MANAGED_IDENTITY_SIDE_MISMATCH",
        "MANAGED_IDENTITY_MARKET_MISMATCH",
        "CONTEXT_NOT_FOUND",
        "UNSUPPORTED_MAJOR_VERSION",
        "MALFORMED_CONTEXT",
        "NOT_CLOSED_CANDLE",
        "MARKET_MISMATCH",
        "SYMBOL_MISMATCH",
        "INTERVAL_MISMATCH",
        "FUTURE_OR_UNCLOSED_CURSOR",
        "STALE_CONTEXT",
        "CURSOR_INVALID",
        "CURSOR_REGRESSION",
        "CURSOR_CONFLICT",
    }
)
_SHADOW_REASON_ALIASES = {
    "Guardian shadow planning requires USD-M futures context": "SHADOW_MARKET_MISMATCH",
    "managed snapshot symbol does not match protection context": "SHADOW_SYMBOL_MISMATCH",
    "managed snapshot must be anchored to the same closed-candle cursor": (
        "SHADOW_CURSOR_ANCHOR_MISMATCH"
    ),
}
_SEVERITY_ORDER = {"CRITICAL": 0, "WARNING": 1, "INFO": 2}


def build_guardian_operations_report(
    identity: ManagedPositionIdentity,
    *,
    report_time_ms: int,
    source_ref: str,
    shadow_result: ShadowPlanningResult | None = None,
    reconciliation_result: ReconciliationResult | None = None,
) -> dict[str, object]:
    """Build one deterministic, sanitized L60-05 operator report with no I/O."""

    if report_time_ms < 0:
        raise ValueError("report_time_ms must be non-negative")
    if not source_ref.strip():
        raise ValueError("source_ref must not be blank")
    if shadow_result is None and reconciliation_result is None:
        raise ValueError("at least one Guardian result is required")

    alerts: list[GuardianAlert] = []
    if shadow_result is not None:
        alerts.extend(
            alerts_from_shadow_result(
                identity,
                shadow_result,
                source_ref=f"{source_ref}:shadow",
                event_time_ms=report_time_ms,
            )
        )
    if reconciliation_result is not None:
        alerts.extend(
            alerts_from_reconciliation_result(
                identity,
                reconciliation_result,
                source_ref=f"{source_ref}:reconciliation",
                event_time_ms=report_time_ms,
            )
        )
    alerts.sort(key=lambda alert: (_SEVERITY_ORDER[alert.severity], alert.code, alert.alert_id))

    report: dict[str, object] = {
        "schema_version": GUARDIAN_OPERATIONS_REPORT_SCHEMA_VERSION,
        "report_time_ms": report_time_ms,
        "mode": "shadow",
        "managed_position": {
            "symbol": identity.symbol,
            "position_side": identity.position_side,
            "adoption_generation": identity.adoption_generation,
        },
        "shadow": _shadow_summary(shadow_result),
        "reconciliation": _reconciliation_summary(reconciliation_result),
        "alerts": [alert.as_dict() for alert in alerts],
        "alert_counts": {
            code: sum(alert.code == code for alert in alerts) for code in _ALERT_CODES
        },
        "operator_attention": any(alert.severity != "INFO" for alert in alerts)
        or bool(reconciliation_result and reconciliation_result.operator_attention),
        "exchange_write_calls": 0,
    }
    canonical = json.dumps(
        report,
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    report_id = hashlib.sha256(
        b"POSITION_GUARDIAN_OPERATIONS_REPORT_V1\0" + canonical
    ).hexdigest()
    return {"report_id": report_id, **report}


def _shadow_summary(result: ShadowPlanningResult | None) -> Mapping[str, object] | None:
    if result is None:
        return None
    if result.exchange_write_calls != 0:
        raise ValueError("operations report accepts only zero-write shadow results")
    intent = result.intent
    if intent is not None and intent.order_placed:
        raise ValueError("operations report cannot contain an already placed shadow order")
    return {
        "disposition": result.disposition,
        "reason_code": _safe_shadow_reason(result.reason),
        "context_id": result.context_id,
        "intent": (
            {
                "intent_id": intent.intent_id,
                "policy_version": intent.policy_version,
                "previous_stop": format(intent.previous_stop, ".15g"),
                "proposed_stop": format(intent.proposed_stop, ".15g"),
                "policy_state": intent.policy_state.value,
                "effective_from_next_candle": intent.effective_from_next_candle,
                "order_placed": intent.order_placed,
            }
            if intent is not None
            else None
        ),
        "intent_event_inserted": result.intent_event_inserted,
        "cursor_event_inserted": result.cursor_event_inserted,
        "exchange_write_calls": 0,
    }


def _reconciliation_summary(
    result: ReconciliationResult | None,
) -> Mapping[str, object] | None:
    if result is None:
        return None
    if result.exchange_write_calls != 0:
        raise ValueError("operations report accepts only zero-write reconciliation results")
    safe_codes = [code for code in result.alerts if code in _SAFE_RECONCILIATION_CODES]
    if len(safe_codes) != len(result.alerts):
        safe_codes.append("UNKNOWN_RECONCILIATION_ALERT")
    return {
        "state": result.state,
        "quantity": str(result.quantity) if result.quantity is not None else None,
        "alert_codes": safe_codes,
        "operator_attention": result.operator_attention,
        "snapshot_event_accepted": result.snapshot_event_accepted,
        "exchange_write_calls": 0,
    }


def _safe_shadow_reason(reason: str) -> str:
    if reason in _SAFE_SHADOW_REASON_CODES:
        return reason
    return _SHADOW_REASON_ALIASES.get(reason, "UNSPECIFIED")
