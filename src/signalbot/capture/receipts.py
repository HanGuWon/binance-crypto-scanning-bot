from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Protocol


@dataclass(frozen=True, slots=True)
class ReceiptTimestamp:
    """One wall-clock/monotonic sample taken at an evidence receipt seam."""

    received_at_ms: int
    received_monotonic_ns: int


class ReceiptClock(Protocol):
    def capture(self) -> ReceiptTimestamp: ...


class SystemReceiptClock:
    def capture(self) -> ReceiptTimestamp:
        # The Unix-millisecond wall clock is authoritative for UTC rendering.
        # The monotonic sample is ordering evidence and is never rendered as UTC.
        return ReceiptTimestamp(
            received_at_ms=time.time_ns() // 1_000_000,
            received_monotonic_ns=time.monotonic_ns(),
        )


class IngestSequencer:
    """Process-local total ordering; process_boot_id disambiguates restarts.

    Callers must share one instance across all capture adapters in a process and
    call ``next`` without an intervening ``await`` before offering the record.
    """

    def __init__(self, initial_value: int = 0) -> None:
        if initial_value < 0:
            raise ValueError("initial_value must be nonnegative")
        self._value = initial_value

    def next(self) -> int:
        self._value += 1
        return self._value
