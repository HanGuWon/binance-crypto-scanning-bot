from __future__ import annotations

import asyncio
import logging
from typing import Protocol

from signalbot.capture.errors import (
    CaptureIntegrityError,
    CaptureShortWriteError,
    CaptureStorageCapacityError,
)
from signalbot.capture.handoff import (
    BoundedCaptureHandoff,
    CaptureFatalState,
    QueuedCaptureRecord,
)
from signalbot.capture.models import (
    CaptureRecord,
    CoverageReason,
    CoverageTransitionV1,
    invalidation_for_record,
    record_to_json_line,
)

LOGGER = logging.getLogger(__name__)


class CaptureWriter(Protocol):
    def append(self, record: CaptureRecord, encoded_line: bytes) -> None: ...

    def close(self) -> None: ...

    def abort(self) -> None: ...

    def write_emergency_transition(self, transition: CoverageTransitionV1) -> None: ...


class CapturePipeline:
    """Capture-only writer pipeline; every failure is application-visible.

    It intentionally has no scanner/runtime callback.  A later integration must
    observe ``fatal_state`` and define its own evidence-commit acknowledgement;
    this foundation cannot silently continue into alert or order logic.
    """

    def __init__(
        self,
        handoff: BoundedCaptureHandoff,
        writer: CaptureWriter,
    ) -> None:
        self.handoff = handoff
        self.writer = writer
        self._worker: asyncio.Task[None] | None = None
        self._last_record: CaptureRecord | None = None

    @property
    def fatal_state(self) -> CaptureFatalState:
        return self.handoff.fatal_state

    def start(self) -> None:
        if self._worker is not None:
            raise RuntimeError("capture pipeline already started")
        self._worker = asyncio.create_task(
            self._guarded_run(), name="capture-segment-writer"
        )

    def offer(self, record: CaptureRecord) -> None:
        if self._worker is None:
            raise RuntimeError("capture pipeline is not started")
        if self._worker.done():
            if not self.fatal_state.failed:
                self.fatal_state.trip_unbound(
                    RuntimeError("capture writer worker stopped unexpectedly")
                )
            self.fatal_state.raise_if_failed()
        self.handoff.offer(record)

    async def stop(self) -> None:
        """Stop producers, drain in order, fsync/finalize, then surface fatal state."""

        if self._worker is None:
            return
        if self._worker.done():
            await self._worker
            self._worker = None
            self.fatal_state.raise_if_failed()
            return
        self.handoff.stop_producer()
        await self.handoff.join()
        await self._worker
        self._worker = None
        self.fatal_state.raise_if_failed()

    async def wait_failed(self) -> None:
        await self.fatal_state.failed_event.wait()

    async def _guarded_run(self) -> None:
        try:
            await self._run()
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            if self._last_record is None:
                self.fatal_state.trip_unbound(exc)
                self.handoff.fail_producer()
                self.handoff.discard_pending()
                return
            await self._fail_writer(self._last_record, exc)

    async def _run(self) -> None:
        while True:
            item = await self.handoff.get()
            try:
                if self.handoff.is_stop(item):
                    try:
                        await asyncio.to_thread(self.writer.close)
                    except Exception as exc:
                        await self._fail_without_current_write(exc)
                    return
                if self.handoff.is_fatal(item):
                    failure = self.fatal_state.failure
                    if failure is None or failure.transition is None:
                        raise RuntimeError("fatal handoff marker lacks failure state")
                    try:
                        await asyncio.to_thread(
                            self.writer.append,
                            failure.transition,
                            record_to_json_line(failure.transition),
                        )
                        await asyncio.to_thread(self.writer.close)
                    except Exception:
                        LOGGER.exception(
                            "capture writer failed while sealing fatal coverage state"
                        )
                        await self._emergency(failure.transition)
                        await self._abort_writer()
                    return
                assert isinstance(item, QueuedCaptureRecord)
                self._last_record = item.record
                try:
                    await asyncio.to_thread(
                        self.writer.append, item.record, item.encoded_line
                    )
                except Exception as exc:
                    await self._fail_writer(item.record, exc)
                    return
            finally:
                self.handoff.complete(item)

    async def _fail_writer(self, record: CaptureRecord, exc: Exception) -> None:
        if isinstance(exc, CaptureStorageCapacityError):
            reason = CoverageReason.STORAGE_CAPACITY
        elif isinstance(exc, CaptureShortWriteError):
            reason = CoverageReason.SHORT_WRITE
        elif isinstance(exc, CaptureIntegrityError):
            reason = CoverageReason.HASH_INTEGRITY
        else:
            reason = CoverageReason.WRITER_ERROR
        transition = invalidation_for_record(
            record,
            reason,
            "capture writer failed before downstream dispatch",
        )
        self.fatal_state.trip(transition, exc)
        authoritative = self._authoritative_transition(transition)
        self.handoff.fail_producer()
        await self._emergency(authoritative)
        await self._abort_writer()
        self.handoff.discard_pending()

    async def _fail_without_current_write(self, exc: Exception) -> None:
        if self._last_record is None:
            # The concrete writer guarantees an empty close is a no-op.  A
            # foreign writer violating that contract still stops producers and
            # surfaces the original exception to the caller.
            self.fatal_state.trip_unbound(exc)
            self.handoff.fail_producer()
            self.handoff.discard_pending()
            return
        await self._fail_writer(self._last_record, exc)

    async def _abort_writer(self) -> None:
        try:
            await asyncio.to_thread(self.writer.abort)
        except Exception:
            LOGGER.exception("capture writer abort failed after fatal error")

    def _authoritative_transition(
        self, fallback: CoverageTransitionV1
    ) -> CoverageTransitionV1:
        failure = self.fatal_state.failure
        if failure is None or failure.transition is None:
            return fallback
        return failure.transition

    async def _emergency(self, transition: CoverageTransitionV1) -> None:
        try:
            await asyncio.to_thread(self.writer.write_emergency_transition, transition)
        except Exception:
            # The original exception remains authoritative and available through
            # fatal_state.  A failed disk cannot be assumed capable of recording
            # its own failure.
            LOGGER.exception("failed to write emergency capture coverage transition")
            return
