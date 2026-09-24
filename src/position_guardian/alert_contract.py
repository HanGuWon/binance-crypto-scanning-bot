from __future__ import annotations

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
