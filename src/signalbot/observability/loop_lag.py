"""Event-loop scheduling-lag monitor (diagnostics only).

A periodic task sleeps for a fixed interval and measures how much later than
expected it actually resumed. A long synchronous step on the event loop (for
example a full-window feature computation at a candle boundary) shows up as lag.
The monitor never alters scanning behavior: it logs a rate-limited WARNING and
publishes the rolling maximum through the runtime heartbeat.
"""

from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import Callable, Sequence

from signalbot.heartbeat import HeartbeatRecorder

LOGGER = logging.getLogger(__name__)

DEFAULT_SAMPLE_INTERVAL_SECONDS = 0.25
DEFAULT_WINDOW_SECONDS = 60.0


class LoopLagMonitor:
    def __init__(
        self,
        *,
        warning_ms: int,
        stop_event: asyncio.Event,
        heartbeats: Sequence[HeartbeatRecorder] = (),
        interval_seconds: float = DEFAULT_SAMPLE_INTERVAL_SECONDS,
        window_seconds: float = DEFAULT_WINDOW_SECONDS,
        monotonic: Callable[[], float] = time.monotonic,
    ) -> None:
        if warning_ms < 1 or interval_seconds <= 0 or window_seconds <= 0:
            raise ValueError("loop-lag monitor limits must be positive")
        self.warning_ms = warning_ms
        self._stop_event = stop_event
        self._heartbeats = tuple(heartbeats)
        self._interval_seconds = interval_seconds
        self._window_seconds = window_seconds
        self._monotonic = monotonic
        self._window_started: float | None = None
        self._window_max_ms = 0
        self._previous_window_max_ms = 0
        self._since_warning_max_ms = 0
        self._last_warning_at: float | None = None
        self.warning_count = 0

    @property
    def rolling_max_ms(self) -> int:
        """Maximum lag over the current and previous window."""

        return max(self._window_max_ms, self._previous_window_max_ms)

    def observe(self, lag_ms: int, now_s: float) -> None:
        """Record one lag sample taken at monotonic time ``now_s``."""

        lag_ms = max(0, lag_ms)
        if self._window_started is None:
            self._window_started = now_s
        elif now_s - self._window_started >= self._window_seconds:
            self._previous_window_max_ms = self._window_max_ms
            self._window_max_ms = 0
            self._window_started = now_s
        self._window_max_ms = max(self._window_max_ms, lag_ms)
        self._since_warning_max_ms = max(self._since_warning_max_ms, lag_ms)
        for heartbeat in self._heartbeats:
            heartbeat.note_loop_lag(self.rolling_max_ms)
        if lag_ms <= self.warning_ms:
            return
        if (
            self._last_warning_at is not None
            and now_s - self._last_warning_at < self._window_seconds
        ):
            return
        self._last_warning_at = now_s
        self.warning_count += 1
        LOGGER.warning(
            "event loop blocked: scheduling lag %d ms (max since last warning %d ms, "
            "threshold %d ms)",
            lag_ms,
            self._since_warning_max_ms,
            self.warning_ms,
        )
        self._since_warning_max_ms = 0

    async def run(self) -> None:
        """Sample until ``stop_event`` is set; never catches cancellation."""

        while not self._stop_event.is_set():
            started = self._monotonic()
            try:
                await asyncio.wait_for(self._stop_event.wait(), timeout=self._interval_seconds)
            except TimeoutError:
                woke = self._monotonic()
                lag_ms = round((woke - started - self._interval_seconds) * 1000)
                self.observe(lag_ms, woke)
                continue
            return
