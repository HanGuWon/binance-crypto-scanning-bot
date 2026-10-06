from __future__ import annotations

import json
from typing import Literal, cast

GUARDIAN_RECONCILIATION_ALERT_SOURCE_V1 = "guardian_reconciliation_alert_source_v1"
GUARDIAN_RECONCILIATION_REJECTION_ALERT_SOURCE_V1 = (
    "guardian_reconciliation_rejection_alert_source_v1"
)
GUARDIAN_SHADOW_CONTEXT_ALERT_SOURCE_V1 = "guardian_shadow_context_alert_source_v1"
GUARDIAN_SHADOW_INTENT_ALERT_SOURCE_V1 = "guardian_shadow_intent_alert_source_v1"

GUARDIAN_ALERT_SOURCE_EVENT_TYPES = frozenset(
    {
        "ACCOUNT_SNAPSHOT",
        "PLANNED_INTENT",
        "RECONCILIATION_ALERT_SOURCE",
        "SHADOW_ALERT_SOURCE",
    }
)

GUARDIAN_ALERT_OUTBOX_ACTIVE_STATUSES = frozenset({"pending", "sending", "uncertain"})
GUARDIAN_ALERT_OUTBOX_TERMINAL_STATUSES = frozenset({"delivered", "dead"})

GuardianAlertDeliveryMode = Literal["disabled", "discord_v1"]
GUARDIAN_ALERT_DELIVERY_DISABLED: GuardianAlertDeliveryMode = "disabled"
GUARDIAN_ALERT_DELIVERY_DISCORD_V1: GuardianAlertDeliveryMode = "discord_v1"


def guardian_alert_delivery_mode(event_type: str, payload_json: str) -> GuardianAlertDeliveryMode:
    """Return immutable per-source delivery authority.

    Pre-L60-07 source events intentionally contain no delivery marker and therefore
    remain permanently disabled rather than becoming eligible after a later config change.
    """

    try:
        payload = json.loads(payload_json)
    except json.JSONDecodeError as exc:
        raise ValueError("Guardian alert source payload is invalid JSON") from exc
    if not isinstance(payload, dict):
        raise ValueError("Guardian alert source payload must be a JSON object")

    mode: object = GUARDIAN_ALERT_DELIVERY_DISABLED
    if event_type == "ACCOUNT_SNAPSHOT":
        source = payload.get("alert_source")
        if isinstance(source, dict):
            mode = source.get("delivery_mode", GUARDIAN_ALERT_DELIVERY_DISABLED)
    elif event_type in {"PLANNED_INTENT", "RECONCILIATION_ALERT_SOURCE", "SHADOW_ALERT_SOURCE"}:
        source = payload.get("payload")
        if isinstance(source, dict):
            mode = source.get("delivery_mode", GUARDIAN_ALERT_DELIVERY_DISABLED)
    if mode not in {GUARDIAN_ALERT_DELIVERY_DISABLED, GUARDIAN_ALERT_DELIVERY_DISCORD_V1}:
        raise ValueError("unsupported Guardian alert delivery mode")
    return cast(GuardianAlertDeliveryMode, mode)
