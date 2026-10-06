"""WebSocket handler timing diagnostics (diagnostics only).

Records, per message, how long the handler ran and how long passed between the
frame being pulled from the connection and the handler starting. Neither value is
used by any gate; in particular this does not touch the receipt timestamp that
``MarketScanner._handle_payload`` stamps for book-ticker handling.

The receive point is when the consumer loop obtains the frame from the
``websockets`` iterator. Time a frame spends queued inside the library before that
is not visible to this measurement.
"""

from __future__ import annotations

import logging
import time
from collections.abc import Callable

LOGGER = logging.getLogger(__name__)

DEFAULT_WARNING_INTERVAL_SECONDS = 60.0
MAX_TRACKED_STREAMS = 256


class HandlerDiagnostics:
    def __init__(
        self,
        *,
        slow_warning_ms: int,
        market: str,
        warning_interval_seconds: float = DEFAULT_WARNING_INTERVAL_SECONDS,
        monotonic: Callable[[], float] = time.monotonic,
    ) -> None:
        if slow_warning_ms < 1 or warning_interval_seconds <= 0:
            raise ValueError("handler diagnostics limits must be positive")
        self.slow_warning_ms = slow_warning_ms
        self.market = market
        self._warning_interval_seconds = warning_interval_seconds
        self.monotonic = monotonic
        self._last_slow_warning: dict[str, float] = {}
        self._last_delay_warning: dict[str, float] = {}
        self.max_receive_to_handler_ms = 0.0
        self.max_handler_ms = 0.0
        self.slow_warning_count = 0
        self.delay_warning_count = 0

    def record(self, stream: str, *, receive_to_handler_ms: float, handler_ms: float) -> None:
        self.max_receive_to_handler_ms = max(self.max_receive_to_handler_ms, receive_to_handler_ms)
        self.max_handler_ms = max(self.max_handler_ms, handler_ms)
        now = self.monotonic()
        if handler_ms > self.slow_warning_ms and self._due(self._last_slow_warning, stream, now):
            self.slow_warning_count += 1
            LOGGER.warning(
                "slow WebSocket handler: %.0f ms on stream %s (threshold %d ms, "
                "max receive-to-handler delay %.0f ms)",
                handler_ms,
                stream,
                self.slow_warning_ms,
                self.max_receive_to_handler_ms,
                extra={"market": self.market, "stream": stream},
            )
        if receive_to_handler_ms > self.slow_warning_ms and self._due(
            self._last_delay_warning, stream, now
        ):
            self.delay_warning_count += 1
            LOGGER.warning(
                "WebSocket receive-to-handler delay %.0f ms on stream %s (threshold %d ms)",
                receive_to_handler_ms,
                stream,
                self.slow_warning_ms,
                extra={"market": self.market, "stream": stream},
            )

    def _due(self, last: dict[str, float], stream: str, now: float) -> bool:
        key = stream if stream in last or len(last) < MAX_TRACKED_STREAMS else "other"
        previous = last.get(key)
        if previous is not None and now - previous < self._warning_interval_seconds:
            return False
        last[key] = now
        return True
