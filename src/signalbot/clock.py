from __future__ import annotations

import time
from dataclasses import dataclass


class Clock:
    def now_ms(self) -> int:
        raise NotImplementedError


class SystemClock(Clock):
    def now_ms(self) -> int:
        return time.time_ns() // 1_000_000


@dataclass(slots=True)
class ReplayClock(Clock):
    current_ms: int = 0

    def now_ms(self) -> int:
        return self.current_ms

    def advance_to(self, timestamp_ms: int) -> None:
        if timestamp_ms < self.current_ms:
            raise ValueError("replay clock cannot move backwards")
        self.current_ms = timestamp_ms
