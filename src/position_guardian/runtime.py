from __future__ import annotations

from position_guardian.config import GuardianSettings, settings_summary
from position_guardian.reconcile import ReconciliationResult


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
