from __future__ import annotations

from position_guardian.config import GuardianSettings, settings_summary


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
