from __future__ import annotations

import asyncio
from dataclasses import dataclass

from signalbot.capture.errors import (
    CaptureHandoffClosed,
    CaptureQueueOverflow,
    CaptureSerializationError,
)
from signalbot.capture.models import (
    CaptureRecord,
    CoverageReason,
    CoverageTransitionV1,
    invalidation_for_record,
    record_to_json_line,
)


@dataclass(frozen=True, slots=True)
class QueuedCaptureRecord:
    record: CaptureRecord
    encoded_line: bytes

    @property
    def byte_count(self) -> int:
        return len(self.encoded_line)


class _Control:
    pass


_STOP = _Control()
_FATAL = _Control()
HandoffItem = QueuedCaptureRecord | _Control


@dataclass(frozen=True, slots=True)
class CaptureFailure:
    transition: CoverageTransitionV1 | None
    cause: BaseException


class CaptureFatalState:
    """First-failure-wins state shared by producers, writer, and application."""

    def __init__(self, stop_event: asyncio.Event | None = None) -> None:
        self.stop_event = stop_event or asyncio.Event()
        self.failed_event = asyncio.Event()
        self._failure: CaptureFailure | None = None

    @property
    def failure(self) -> CaptureFailure | None:
        return self._failure

    @property
    def failed(self) -> bool:
        return self._failure is not None

    def trip(self, transition: CoverageTransitionV1, cause: BaseException) -> bool:
        if self._failure is not None:
            return False
        self._failure = CaptureFailure(transition=transition, cause=cause)
        # Stop is visible before the submitting frame can reach downstream code.
        self.stop_event.set()
        self.failed_event.set()
        return True

    def trip_unbound(self, cause: BaseException) -> bool:
        """Trip before any source record exists, so no bound transition is possible."""

        if self._failure is not None:
            return False
        self._failure = CaptureFailure(transition=None, cause=cause)
        self.stop_event.set()
        self.failed_event.set()
        return True

    def raise_if_failed(self) -> None:
        if self._failure is None:
            return
        raise self._failure.cause


class BoundedCaptureHandoff:
    """Single-loop, nonblocking handoff bounded by both records and encoded bytes.

    One queue slot is reserved for a stop/fatal control marker.  On overflow the
    rejected record is never queued, a bound coverage invalidation is retained in
    ``fatal_state``, shared stop is set, and ``CaptureQueueOverflow`` is raised.
    """

    def __init__(
        self,
        *,
        max_events: int,
        max_bytes: int,
        fatal_state: CaptureFatalState,
    ) -> None:
        if max_events < 2:
            raise ValueError("max_events must be at least 2 (one slot is reserved)")
        if max_bytes < 1:
            raise ValueError("max_bytes must be positive")
        self.max_events = max_events
        self.max_bytes = max_bytes
        self.fatal_state = fatal_state
        self._queue: asyncio.Queue[HandoffItem] = asyncio.Queue(maxsize=max_events)
        self._payload_limit = max_events - 1
        self._queued_events = 0
        self._queued_bytes = 0
        self._accepting = True

    @property
    def queued_events(self) -> int:
        return self._queued_events

    @property
    def queued_bytes(self) -> int:
        return self._queued_bytes

    @property
    def accepting(self) -> bool:
        return self._accepting

    def offer(self, record: CaptureRecord) -> None:
        """Put without awaiting; overflow is a synchronous fatal failure."""

        if not self._accepting or self.fatal_state.failed:
            raise CaptureHandoffClosed("capture handoff is stopped")
        try:
            encoded = record_to_json_line(record)
        except Exception as exc:
            self._accepting = False
            error = CaptureSerializationError(
                "capture record could not be encoded losslessly"
            )
            transition = invalidation_for_record(
                record,
                CoverageReason.SERIALIZATION_ERROR,
                "rejected before downstream: capture serialization failed",
            )
            self.fatal_state.trip(transition, error)
            self._queue.put_nowait(_FATAL)
            raise error from exc
        next_events = self._queued_events + 1
        next_bytes = self._queued_bytes + len(encoded)
        if next_events > self._payload_limit or next_bytes > self.max_bytes:
            self._accepting = False
            error = CaptureQueueOverflow(
                "capture handoff exceeded its event or encoded-byte bound"
            )
            transition = invalidation_for_record(
                record,
                CoverageReason.QUEUE_OVERFLOW,
                "rejected before downstream: bounded capture handoff overflow",
            )
            self.fatal_state.trip(transition, error)
            self._queue.put_nowait(_FATAL)
            raise error
        self._queue.put_nowait(QueuedCaptureRecord(record=record, encoded_line=encoded))
        self._queued_events = next_events
        self._queued_bytes = next_bytes

    def stop_producer(self) -> None:
        """Stop new submissions and place an ordered drain marker."""

        if not self._accepting:
            return
        self._accepting = False
        self._queue.put_nowait(_STOP)

    def fail_producer(self) -> None:
        """Stop submissions when a consumer-side failure already ended the worker."""

        self._accepting = False

    async def get(self) -> HandoffItem:
        return await self._queue.get()

    def complete(self, item: HandoffItem) -> None:
        if isinstance(item, QueuedCaptureRecord):
            self._queued_events -= 1
            self._queued_bytes -= item.byte_count
        self._queue.task_done()

    async def join(self) -> None:
        await self._queue.join()

    def discard_pending(self) -> None:
        """Release queued items after storage has become unusable.

        The shared fatal transition explicitly marks all such evidence invalid;
        callers must never treat this as a successful drain.
        """

        while True:
            try:
                item = self._queue.get_nowait()
            except asyncio.QueueEmpty:
                return
            self.complete(item)

    @staticmethod
    def is_stop(item: HandoffItem) -> bool:
        return item is _STOP

    @staticmethod
    def is_fatal(item: HandoffItem) -> bool:
        return item is _FATAL
