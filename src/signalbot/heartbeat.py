"""Throttled pipeline-liveness heartbeat writer.

Writes never block ingestion: a database failure is logged at ERROR and the
pending values are kept for the next attempt.
"""

from __future__ import annotations

import logging
from collections.abc import Iterable

from sqlalchemy.exc import SQLAlchemyError

from signalbot.clock import Clock
from signalbot.persistence.repository import SqlRepository

LOGGER = logging.getLogger(__name__)

HEARTBEAT_MIN_INTERVAL_MS = 15_000


class HeartbeatRecorder:
    def __init__(
        self,
        repository: SqlRepository,
        market: str,
        clock: Clock,
        *,
        min_interval_ms: int = HEARTBEAT_MIN_INTERVAL_MS,
    ) -> None:
        if min_interval_ms < 1:
            raise ValueError("heartbeat interval must be positive")
        self._repository = repository
        self._market = market
        self._clock = clock
        self._min_interval_ms = min_interval_ms
        self._last_write_ms: int | None = None
        self._ws_message_ms: int | None = None
        self._closed_candle_ms: int | None = None
        self._decision_ms: int | None = None
        self._max_loop_lag_ms: int | None = None
        self.write_count = 0

    def note_ws_message(self) -> None:
        self._ws_message_ms = self._clock.now_ms()
        self._maybe_flush()

    def note_closed_candle(self, close_time_ms: int) -> None:
        if self._closed_candle_ms is None or close_time_ms > self._closed_candle_ms:
            self._closed_candle_ms = close_time_ms
        self._maybe_flush()

    def note_decision(self) -> None:
        self._decision_ms = self._clock.now_ms()
        self._maybe_flush()

    def note_loop_lag(self, lag_ms: int) -> None:
        if self._max_loop_lag_ms is None or lag_ms > self._max_loop_lag_ms:
            self._max_loop_lag_ms = lag_ms
        self._maybe_flush()

    def _maybe_flush(self) -> None:
        now_ms = self._clock.now_ms()
        if (
            self._last_write_ms is not None
            and now_ms - self._last_write_ms < self._min_interval_ms
        ):
            return
        self._flush(now_ms)

    def _flush(self, now_ms: int) -> None:
        # Record the attempt first so a failing database is retried at most once
        # per interval instead of on every message.
        self._last_write_ms = now_ms
        try:
            self._repository.upsert_heartbeat(
                self._market,
                now_ms,
                last_ws_message_ms=self._ws_message_ms,
                last_closed_candle_ms=self._closed_candle_ms,
                last_decision_ms=self._decision_ms,
                max_loop_lag_ms=self._max_loop_lag_ms,
            )
        except SQLAlchemyError:
            # Liveness evidence must never stop ingestion; surface it loudly instead.
            LOGGER.error(
                "heartbeat write failed",
                extra={"market": self._market},
                exc_info=True,
            )
            return
        self.write_count += 1


def record_outbox_drain(
    repository: SqlRepository, markets: Iterable[str], clock: Clock
) -> None:
    """One heartbeat write per drain cycle; failures are logged, never raised."""

    now_ms = clock.now_ms()
    try:
        for market in markets:
            repository.upsert_heartbeat(market, now_ms, last_outbox_drain_ms=now_ms)
    except SQLAlchemyError:
        LOGGER.error("outbox drain heartbeat write failed", exc_info=True)
