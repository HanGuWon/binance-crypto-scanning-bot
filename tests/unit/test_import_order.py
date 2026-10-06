from __future__ import annotations

import subprocess
import sys

import pytest

# Each module is imported first in a fresh interpreter: a circular import only shows
# up for particular first-import orders, which one shared pytest process hides.
ENTRY_MODULES = [
    "signalbot.persistence",
    "signalbot.persistence.repository",
    "signalbot.alerts",
    "signalbot.alerts.discord",
    "signalbot.alerts.embeds",
    "signalbot.heartbeat",
    "signalbot.errors",
    "signalbot.outbox_cli",
    "signalbot.api.server",
    "signalbot.runtime",
    "signalbot.signals.paper_recovery",
    "signalbot.app",
    "signalbot.cli",
]


@pytest.mark.parametrize("module", ENTRY_MODULES)
def test_module_imports_cleanly_as_the_first_import(module: str) -> None:
    result = subprocess.run(
        [sys.executable, "-c", f"import {module}"],
        capture_output=True,
        text=True,
        timeout=120,
        check=False,
    )
    assert result.returncode == 0, result.stderr[-2000:]
