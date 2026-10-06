from __future__ import annotations

import json
from dataclasses import replace
from decimal import Decimal

import pytest

from position_guardian.alerts import (
    alerts_from_reconciliation_result,
    alerts_from_shadow_result,
)
from position_guardian.domain import ManagedPositionIdentity
from position_guardian.reconcile import ReconciliationResult
from position_guardian.report import build_guardian_operations_report
from position_guardian.runtime import ShadowPlanningResult
from signalbot.domain.enums import Direction, Market
from signalbot.signals.position_management import GuardianPolicyState, StopUpdateIntent


def _identity(*, alias: str = "manual-main") -> ManagedPositionIdentity:
    return ManagedPositionIdentity(alias, "BTCUSDT", "LONG", 3)


def _intent(*, alias: str = "manual-main", order_placed: bool = False) -> StopUpdateIntent:
    return StopUpdateIntent(
        intent_id="intent-abc",
        policy_version="guardian-stop-policy-v1",
        position_ref=f"{alias}:BTCUSDT:LONG:3",
        market=Market.FUTURES,
        symbol="BTCUSDT",
        direction=Direction.LONG,
        previous_stop=60000.0,
        proposed_stop=60500.0,
        reference_price=62000.0,
        observed_at_ms=1700000299999,
        reason="profit_protection",
        order_placed=order_placed,
        policy_state=GuardianPolicyState.PROFIT_PROTECTION,
    )


def _shadow(
    *,
    disposition: str = "STOP_UPDATE_INTENT",
    reason: str = "STOP_UPDATE_INTENT",
    intent: StopUpdateIntent | None = None,
    exchange_write_calls: int = 0,
) -> ShadowPlanningResult:
    resolved_intent = (
        _intent() if intent is None and disposition == "STOP_UPDATE_INTENT" else intent
    )
    return ShadowPlanningResult(
        disposition=disposition,  # type: ignore[arg-type]
        reason=reason,
        context_id="context-123" if disposition == "STOP_UPDATE_INTENT" else None,
        intent=resolved_intent,
        intent_event_inserted=resolved_intent is not None,
        cursor_event_inserted=resolved_intent is not None,
        exchange_write_calls=exchange_write_calls,
    )


def _reconciliation(
    *alerts: str,
    state: str = "MANAGED_SHADOW",
    quantity: Decimal | None = Decimal("0.02"),
    operator_attention: bool = True,
    exchange_write_calls: int = 0,
) -> ReconciliationResult:
    return ReconciliationResult(
        state=state,  # type: ignore[arg-type]
        quantity=quantity,
        alerts=alerts,
        operator_attention=operator_attention,
        snapshot_event_accepted=True,
        exchange_write_calls=exchange_write_calls,
    )


def test_would_update_stop_alert_is_deterministic_and_shadow_only() -> None:
    first = alerts_from_shadow_result(
        _identity(), _shadow(), source_ref="closed-candle-1", event_time_ms=1700000301000
    )
    second = alerts_from_shadow_result(
        _identity(), _shadow(), source_ref="closed-candle-1", event_time_ms=1700000301000
    )

    assert first == second
    assert len(first) == 1
    alert = first[0]
    assert alert.code == "WOULD_UPDATE_STOP"
    assert alert.exchange_write_calls == 0
    assert "60000 -> 60500" in alert.message
    assert "no exchange order was sent" in alert.message
    assert "manual-main" not in json.dumps(alert.as_dict(), sort_keys=True)
    details = alert.as_dict()["details"]
    assert isinstance(details, dict)
    assert "position_ref" not in details


def test_stale_context_alert_is_explicit_and_contains_no_freeform_detail() -> None:
    result = _shadow(disposition="CONTEXT_REJECTED", reason="STALE_CONTEXT")

    alerts = alerts_from_shadow_result(
        _identity(), result, source_ref="stale-check-1", event_time_ms=1700000305000
    )

    assert [alert.code for alert in alerts] == ["STALE_CONTEXT"]
    assert alerts[0].severity == "WARNING"
    assert dict(alerts[0].details) == {"reason": "STALE_CONTEXT"}


def test_uncertain_context_cursor_uses_reconciliation_uncertain_alert() -> None:
    result = _shadow(disposition="CONTEXT_REJECTED", reason="CURSOR_UNCERTAIN")

    alerts = alerts_from_shadow_result(
        _identity(), result, source_ref="cursor-1", event_time_ms=1700000305000
    )

    assert [alert.code for alert in alerts] == ["RECONCILIATION_UNCERTAIN"]
    assert alerts[0].severity == "CRITICAL"


def test_reconciliation_maps_manual_add_and_missing_protection() -> None:
    result = _reconciliation("MANUAL_POSITION_ADD", "PROTECTIVE_ORDER_MISSING")

    alerts = alerts_from_reconciliation_result(
        _identity(), result, source_ref="snapshot-22", event_time_ms=1700000306000
    )

    assert [alert.code for alert in alerts] == ["PROTECTION_MISSING", "MANUAL_SIZE_INCREASE"]
    assert dict(alerts[1].details)["observed_quantity"] == "0.02"
    assert all(alert.exchange_write_calls == 0 for alert in alerts)


def test_reconciliation_maps_side_flip_and_degraded_state() -> None:
    side_flip = alerts_from_reconciliation_result(
        _identity(),
        _reconciliation("SIDE_FLIP_REQUIRES_REAPPROVAL", state="RELEASED"),
        source_ref="snapshot-flip",
        event_time_ms=1700000307000,
    )
    degraded = alerts_from_reconciliation_result(
        _identity(),
        _reconciliation(state="DEGRADED"),
        source_ref="snapshot-gap",
        event_time_ms=1700000308000,
    )

    assert [alert.code for alert in side_flip] == ["SIDE_FLIP"]
    assert [alert.code for alert in degraded] == ["RECONCILIATION_UNCERTAIN"]


def test_unrequired_reconciliation_notice_does_not_create_l60_05_alert() -> None:
    alerts = alerts_from_reconciliation_result(
        _identity(),
        _reconciliation("MANUAL_PARTIAL_CLOSE"),
        source_ref="snapshot-partial-close",
        event_time_ms=1700000309000,
    )

    assert alerts == ()


@pytest.mark.parametrize("source", ["shadow", "reconciliation"])
def test_alert_projection_rejects_any_exchange_write_count(source: str) -> None:
    if source == "shadow":
        with pytest.raises(ValueError, match="zero exchange writes"):
            alerts_from_shadow_result(
                _identity(),
                _shadow(exchange_write_calls=1),
                source_ref="unsafe-shadow",
                event_time_ms=1,
            )
    else:
        with pytest.raises(ValueError, match="zero exchange writes"):
            alerts_from_reconciliation_result(
                _identity(),
                _reconciliation("MANUAL_POSITION_ADD", exchange_write_calls=1),
                source_ref="unsafe-reconcile",
                event_time_ms=1,
            )


def test_shadow_alert_rejects_order_placed_intent() -> None:
    with pytest.raises(ValueError, match="already placed order"):
        alerts_from_shadow_result(
            _identity(),
            _shadow(intent=_intent(order_placed=True)),
            source_ref="unsafe-intent",
            event_time_ms=1,
        )


@pytest.mark.parametrize(
    "position_ref",
    [
        "other-account:BTCUSDT:LONG:3",
        "manual-main:BTCUSDT:LONG:99",
    ],
)
def test_shadow_alert_rejects_intent_from_another_managed_identity(
    position_ref: str,
) -> None:
    wrong_ref = replace(_intent(), position_ref=position_ref)

    with pytest.raises(ValueError, match="position_ref"):
        alerts_from_shadow_result(
            _identity(),
            _shadow(intent=wrong_ref),
            source_ref="wrong-identity",
            event_time_ms=1,
        )


@pytest.mark.parametrize(
    "unsafe_intent",
    [
        replace(_intent(), reduce_only=False),
        replace(_intent(), close_position=True),
        replace(_intent(), effective_from_next_candle=False),
    ],
)
def test_shadow_alert_rejects_intent_outside_protection_only_contract(
    unsafe_intent: StopUpdateIntent,
) -> None:
    with pytest.raises(ValueError, match="protection-only contract"):
        alerts_from_shadow_result(
            _identity(),
            _shadow(intent=unsafe_intent),
            source_ref="unsafe-protection-contract",
            event_time_ms=1,
        )


def test_stop_update_disposition_without_intent_fails_closed() -> None:
    inconsistent = ShadowPlanningResult(
        disposition="STOP_UPDATE_INTENT",
        reason="STOP_UPDATE_INTENT",
        context_id="context-123",
        intent=None,
        intent_event_inserted=False,
        cursor_event_inserted=False,
    )

    with pytest.raises(ValueError, match="must include an intent"):
        alerts_from_shadow_result(
            _identity(), inconsistent, source_ref="broken", event_time_ms=1
        )


def test_stop_update_without_context_id_fails_closed() -> None:
    inconsistent = ShadowPlanningResult(
        disposition="STOP_UPDATE_INTENT",
        reason="STOP_UPDATE_INTENT",
        context_id=None,
        intent=_intent(),
        intent_event_inserted=True,
        cursor_event_inserted=True,
    )

    with pytest.raises(ValueError, match="context_id"):
        alerts_from_shadow_result(
            _identity(), inconsistent, source_ref="missing-context", event_time_ms=1
        )


def test_non_stop_update_result_cannot_smuggle_an_intent() -> None:
    inconsistent = ShadowPlanningResult(
        disposition="NO_STOP_UPDATE",
        reason="NO_STOP_UPDATE",
        context_id="context-123",
        intent=_intent(),
        intent_event_inserted=False,
        cursor_event_inserted=True,
    )

    with pytest.raises(ValueError, match="must not include an intent"):
        alerts_from_shadow_result(
            _identity(), inconsistent, source_ref="smuggled-intent", event_time_ms=1
        )


def test_operations_report_is_deterministic_sanitized_and_complete() -> None:
    identity = _identity(alias="sensitive-account-name")
    shadow = _shadow(intent=_intent(alias="sensitive-account-name"))
    reconciliation = _reconciliation("MANUAL_POSITION_ADD", "PROTECTIVE_ORDER_MISSING")

    first = build_guardian_operations_report(
        identity,
        report_time_ms=1700000310000,
        source_ref="api-key-super-secret-source-ref",
        shadow_result=shadow,
        reconciliation_result=reconciliation,
    )
    second = build_guardian_operations_report(
        identity,
        report_time_ms=1700000310000,
        source_ref="api-key-super-secret-source-ref",
        shadow_result=shadow,
        reconciliation_result=reconciliation,
    )

    assert first == second
    assert len(first["report_id"]) == 64  # type: ignore[arg-type]
    assert first["schema_version"] == "position_guardian_operations_report_v1"
    assert first["exchange_write_calls"] == 0
    assert first["operator_attention"] is True
    assert first["alert_counts"] == {
        "WOULD_UPDATE_STOP": 1,
        "STALE_CONTEXT": 0,
        "MANUAL_SIZE_INCREASE": 1,
        "SIDE_FLIP": 0,
        "PROTECTION_MISSING": 1,
        "RECONCILIATION_UNCERTAIN": 0,
    }
    rendered = json.dumps(first, sort_keys=True)
    assert "sensitive-account-name" not in rendered
    assert "api-key-super-secret-source-ref" not in rendered
    assert "position_ref" not in rendered
    assert "credentials" not in rendered
    assert "balance" not in rendered


def test_operations_report_redacts_unknown_freeform_reason_and_alert_code() -> None:
    report = build_guardian_operations_report(
        _identity(),
        report_time_ms=1700000311000,
        source_ref="report-unsafe-input",
        shadow_result=_shadow(
            disposition="SNAPSHOT_REJECTED",
            reason="api_secret=do-not-render",
            intent=None,
        ),
        reconciliation_result=_reconciliation("api_key=do-not-render"),
    )

    rendered = json.dumps(report, sort_keys=True)
    assert "do-not-render" not in rendered
    assert report["shadow"]["reason_code"] == "UNSPECIFIED"  # type: ignore[index]
    assert report["reconciliation"]["alert_codes"] == [  # type: ignore[index]
        "UNKNOWN_RECONCILIATION_ALERT"
    ]


def test_report_identity_changes_with_source_evidence_without_exposing_source_ref() -> None:
    first = build_guardian_operations_report(
        _identity(),
        report_time_ms=1700000312000,
        source_ref="snapshot-a",
        reconciliation_result=_reconciliation("MANUAL_POSITION_ADD"),
    )
    second = build_guardian_operations_report(
        _identity(),
        report_time_ms=1700000312000,
        source_ref="snapshot-b",
        reconciliation_result=_reconciliation("MANUAL_POSITION_ADD"),
    )

    assert first["report_id"] != second["report_id"]
    assert "snapshot-a" not in json.dumps(first, sort_keys=True)
    assert "snapshot-b" not in json.dumps(second, sort_keys=True)


def test_report_requires_evidence_and_nonnegative_clock() -> None:
    with pytest.raises(ValueError, match="at least one Guardian result"):
        build_guardian_operations_report(
            _identity(), report_time_ms=1, source_ref="empty"
        )
    with pytest.raises(ValueError, match="non-negative"):
        build_guardian_operations_report(
            _identity(),
            report_time_ms=-1,
            source_ref="negative-clock",
            reconciliation_result=_reconciliation(),
        )


def test_alert_source_reference_must_be_nonblank() -> None:
    with pytest.raises(ValueError, match="source_ref must not be blank"):
        alerts_from_reconciliation_result(
            _identity(), _reconciliation(), source_ref="  ", event_time_ms=1
        )
