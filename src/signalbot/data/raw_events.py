from __future__ import annotations

import asyncio
import json
import logging
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from signalbot.domain.enums import Market

LOGGER = logging.getLogger(__name__)

_SENTINEL_PUT_ATTEMPTS = 3
_SENTINEL_PUT_TIMEOUT_SECONDS = 5.0
_TASK_JOIN_TIMEOUT_SECONDS = 5.0


class RawEventRecorderFatalError(RuntimeError):
    """Base for recorder conditions that must fail the whole process closed."""


class RawEventCapacityError(RawEventRecorderFatalError):
    """Raised before admission when the evidence quota would be exceeded."""


class RawEventOverflowError(RawEventRecorderFatalError):
    """Raised when a per-market writer queue is full (fail-closed)."""


class RawEventWriterError(RawEventRecorderFatalError):
    """Raised when the background writer failed or drained incompletely."""


class RawEventRecorderClosedError(RuntimeError):
    """Raised when appending after graceful close was requested."""


def _day_of(received_at_ms: int) -> str:
    return datetime.fromtimestamp(received_at_ms / 1000, tz=UTC).strftime(
        "%Y-%m-%d"
    )


class _MarketWriter:
    """Per-market ordered background writer owning one persistent file.

    Counter discipline (single-owner, no locks needed under asyncio):

    - append() on the event loop reserves bytes and enqueues.
    - the writer coroutine mutates durable/reserved counters ONLY after its
      to_thread write returns, never inside the thread-executed body.
    - _write_batch is a pure synchronous file-write function with no access
      to shared recorder counters.
    """

    def __init__(
        self,
        recorder: RawEventRecorder,
        market: Market,
        write_batch_hook=None,
    ) -> None:
        self.recorder = recorder
        self.market = market
        self.queue: asyncio.Queue[tuple[str, int] | None] = asyncio.Queue(
            maxsize=recorder.max_queue_size
        )
        self.task: asyncio.Task[None] | None = None
        self.fatal_error: RawEventRecorderFatalError | None = None
        self.durable_records = 0
        self._current_path: Path | None = None
        self._current_handle: Any = None
        self._current_day: str | None = None
        self._sentinel_seen = False
        # Test hook: optional async callable invoked in the writer task right
        # before each to_thread write. Deterministic gating lives HERE, at the
        # call site, never inside the thread body.
        self._write_batch_hook = write_batch_hook

    def start(self) -> None:
        if self.task is None or self.task.done():
            task = asyncio.get_running_loop().create_task(self._run())
            task.add_done_callback(self._task_done_callback)
            self.task = task

    def _task_done_callback(self, task: asyncio.Task[None]) -> None:
        if task.cancelled():
            return
        error = task.exception()
        if error is not None and self.fatal_error is None:
            LOGGER.critical(
                "raw-event writer task crashed",
                extra={"market": self.market.value},
                exc_info=error,
            )

    async def _run(self) -> None:
        while True:
            item = await self.queue.get()
            if item is None:
                break
            batch = [item]
            while True:
                try:
                    nxt = self.queue.get_nowait()
                except asyncio.QueueEmpty:
                    break
                if nxt is None:
                    self._sentinel_seen = True
                    break
                batch.append(nxt)
            encoded_total = sum(encoded for _, encoded in batch)
            try:
                if self._write_batch_hook is not None:
                    # Test gate lives in the writer TASK (event loop), never
                    # in the thread body; a raising hook is a writer failure.
                    await self._write_batch_hook(self)
                await asyncio.to_thread(self._write_batch, batch)
            except Exception as exc:
                if isinstance(exc, RawEventRecorderFatalError):
                    fatal = exc
                else:
                    fatal = RawEventWriterError(f"raw-event writer failed: {exc}")
                    fatal.__cause__ = exc
                # Persist the exact evidence-loss count in the fatal chain so
                # a post-mortem knows how many ACCEPTED records never became
                # DURABLE (P2 review finding).
                lost = (
                    self.recorder.accepted_records
                    - self.durable_records
                    - len(batch)
                )
                self.fatal_error = fatal
                self.recorder._signal_fatal(
                    type(fatal)(
                        f"{fatal} [accepted={self.recorder.accepted_records} "
                        f"durable_at_failure={self.durable_records} "
                        f"lost_including_batch={lost + len(batch)}]"
                    )
                )
                break
            # Event-loop-side accounting only; the thread body never touches
            # shared counters.
            self.recorder.reserved_bytes -= encoded_total
            self.recorder.durable_bytes += encoded_total
            self.durable_records += len(batch)
            if self._sentinel_seen:
                break

    def _write_batch(self, batch: list[tuple[str, int]]) -> None:
        """Pure synchronous disk write. No shared-counter access here.

        Partitions ALL lines by UTC day of received_at_ms (stable partition,
        never a reorder); per day-group performs ONE handle.write of the
        concatenated lines and ONE flush. Global record order across the two
        files is the enqueue order by construction; per-file order is exact.
        """
        groups: dict[str, list[str]] = {}
        for line, _encoded in batch:
            day = _day_of(json.loads(line)["received_at_ms"])
            groups.setdefault(day, []).append(line)
        for day, group_lines in groups.items():
            path = self.recorder.directory / self.market.value / f"{day}.jsonl"
            self._open_for(path, day)
            handle = self._current_handle
            assert handle is not None
            handle.write("".join(group_lines))
            handle.flush()

    def _open_for(self, path: Path, day: str) -> None:
        if self._current_handle is not None and self._current_day != day:
            self._current_handle.close()
            self._current_handle = None
        if self._current_handle is None:
            path.parent.mkdir(parents=True, exist_ok=True)
            self._current_handle = path.open("a", encoding="utf-8", newline="\n")
            self._current_path = path
            self._current_day = day

    def close_handle(self) -> None:
        if self._current_handle is not None:
            self._current_handle.close()
            self._current_handle = None

    async def close(self) -> None:
        """Bounded sentinel + join. Never hangs even with a dead/full queue."""
        if self.task is None or self.task.done():
            self.close_handle()
            return
        sentinel_delivered = False
        for _attempt in range(_SENTINEL_PUT_ATTEMPTS):
            if self.task.done():
                break
            try:
                await asyncio.wait_for(
                    self.queue.put(None), timeout=_SENTINEL_PUT_TIMEOUT_SECONDS
                )
                sentinel_delivered = True
                break
            except TimeoutError:
                continue
        if not sentinel_delivered:
            LOGGER.error(
                "abandoning raw-event writer sentinel after bounded retries",
                extra={"market": self.market.value},
            )
        try:
            await asyncio.wait_for(
                asyncio.shield(self.task), timeout=_TASK_JOIN_TIMEOUT_SECONDS
            )
        except asyncio.CancelledError:
            # An outer cancellation (e.g. app-level shutdown deadline) must
            # still record evidence loss and close handles before propagating.
            LOGGER.critical(
                "raw-event writer join cancelled; abandoning hung writer",
                extra={"market": self.market.value},
            )
            self.recorder._signal_fatal(
                RawEventWriterError(
                    f"writer join cancelled during close "
                    f"(market={self.market.value})"
                )
            )
            self.task = None
            self.close_handle()
            raise
        except TimeoutError:
            # A hung disk write must never stall application shutdown. The
            # writer task keeps running detached; the fatal path records the
            # evidence loss and the handle is closed defensively.
            LOGGER.critical(
                "raw-event writer join timed out; abandoning hung writer",
                extra={"market": self.market.value},
            )
            self.recorder._signal_fatal(
                RawEventWriterError(
                    f"writer join timed out after {_TASK_JOIN_TIMEOUT_SECONDS}s "
                    f"during close (market={self.market.value})"
                )
            )
        except Exception as exc:  # pragma: no cover - defensive
            LOGGER.warning(
                "raw-event writer task raised during close",
                extra={"market": self.market.value},
                exc_info=exc,
            )
        self.task = None
        self.close_handle()


class RawEventRecorder:
    """Fail-closed, non-blocking raw-evidence recorder.

    Contract:

    - append(): serialize, reserve quota, bounded-queue admit; returns
      WITHOUT waiting for disk durability. ACCEPTED != DURABLE.
    - durability is owned by background writer tasks plus a durable
      watermark; graceful close drains every accepted record.
    - any fatal condition (quota exhausted, queue overflow, writer crash)
      is observable via wait_failed()/status() and raises fail-closed.
    - quota invariant at admission, enforced on the event loop only:
      bootstrap_baseline + durable_bytes + reserved_bytes + encoded <= max.
      The baseline never grows because every later byte lands in exactly
      one of reserved or durable.
    """

    def __init__(
        self,
        directory: str | Path,
        maximum_total_bytes: int = 10_737_418_240,
        max_queue_size: int = 50_000,
        write_batch_hook=None,
    ) -> None:
        if maximum_total_bytes < 1:
            raise ValueError("maximum_total_bytes must be positive")
        if max_queue_size < 1:
            raise ValueError("max_queue_size must be positive")
        self.directory = Path(directory)
        self.maximum_total_bytes = maximum_total_bytes
        self.max_queue_size = max_queue_size
        self.accepted_records = 0
        self.overflow_count = 0
        self.reserved_bytes = 0
        self.durable_bytes = 0
        self.baseline_bytes = 0
        self._bootstrap_done = False
        self._fatal_error: RawEventRecorderFatalError | None = None
        self._fatal_event: asyncio.Event | None = None
        self._writers: dict[Market, _MarketWriter] = {}
        self._closed = False
        self._write_batch_hook = write_batch_hook

    def _writer_for(self, market: Market) -> _MarketWriter:
        writer = self._writers.get(market)
        if writer is None:
            writer = _MarketWriter(
                self, market, write_batch_hook=self._write_batch_hook
            )
            self._writers[market] = writer
        writer.start()
        return writer

    def _ensure_fatal_event(self) -> asyncio.Event:
        if self._fatal_event is None:
            self._fatal_event = asyncio.Event()
        return self._fatal_event

    def _signal_fatal(self, error: RawEventRecorderFatalError) -> None:
        if self._fatal_error is None:
            self._fatal_error = error
        event = self._ensure_fatal_event()
        event.set()

    def _bootstrap_quota(self) -> None:
        """One-time synchronous directory scan before first admission."""
        if self._bootstrap_done:
            return
        self.baseline_bytes = self.directory_size()
        self._bootstrap_done = True

    @property
    def fatal_error(self) -> RawEventRecorderFatalError | None:
        return self._fatal_error

    @property
    def durable_records(self) -> int:
        return sum(writer.durable_records for writer in self._writers.values())

    def status(self) -> dict[str, Any]:
        durable = self.durable_records
        return {
            "accepted_records": self.accepted_records,
            "durable_records": durable,
            "queued_records": self.accepted_records - durable,
            "reserved_bytes": self.reserved_bytes,
            "durable_bytes": self.durable_bytes,
            "baseline_bytes": self.baseline_bytes,
            "fatal_error": (
                None
                if self._fatal_error is None
                else f"{type(self._fatal_error).__name__}: {self._fatal_error}"
            ),
            "overflow_count": self.overflow_count,
        }

    async def wait_failed(self) -> None:
        await self._ensure_fatal_event().wait()

    def is_failed(self) -> bool:
        return self._fatal_event is not None and self._fatal_event.is_set()

    async def wait_drained(self, timeout_seconds: float = 30.0) -> bool:
        """Wait until every accepted record is durable. Returns success."""

        deadline = asyncio.get_running_loop().time() + timeout_seconds
        while self.durable_records < self.accepted_records:
            if self.is_failed():
                return False
            if asyncio.get_running_loop().time() > deadline:
                return False
            await asyncio.sleep(0.01)
        return True


    async def append(self, market: Market, payload: Any, event_time_ms: int) -> None:
        if self._closed:
            raise RawEventRecorderClosedError("raw event recorder already closed")
        if self._fatal_error is not None:
            raise self._fatal_error
        line = json.dumps(
            {
                "market": market.value,
                "received_at_ms": event_time_ms,
                "payload": payload,
            },
            separators=(",", ":"),
            ensure_ascii=False,
        ) + "\n"
        encoded_bytes = len(line.encode("utf-8"))
        self._bootstrap_quota()
        projected = (
            self.baseline_bytes
            + self.durable_bytes
            + self.reserved_bytes
            + encoded_bytes
        )
        if projected > self.maximum_total_bytes:
            self.overflow_count += 1
            raise RawEventCapacityError(
                f"raw-event admission would exceed hard byte quota "
                f"({projected} > {self.maximum_total_bytes})"
            )
        writer = self._writer_for(market)
        self.reserved_bytes += encoded_bytes
        try:
            writer.queue.put_nowait((line, encoded_bytes))
        except asyncio.QueueFull as exc:
            self.reserved_bytes -= encoded_bytes
            self.overflow_count += 1
            error = RawEventOverflowError(
                "raw-event writer queue overflow; failing closed"
            )
            self._signal_fatal(error)
            raise error from exc
        self.accepted_records += 1

    async def close(self) -> None:
        """Stop admission, drain accepted records, flush and close files.

        Idempotent and deterministic: a dead writer can never make close()
        wait forever, and handles are always closed exactly once.
        """
        if self._closed:
            return
        self._closed = True
        for writer in self._writers.values():
            await writer.close()
        if (
            self._fatal_error is None
            and self.durable_records != self.accepted_records
        ):
            mismatch = RawEventWriterError(
                f"recorder closed with undrained records: "
                f"accepted={self.accepted_records} "
                f"durable={self.durable_records}"
            )
            self._signal_fatal(mismatch)
            raise mismatch

    def directory_size(self) -> int:
        if not self.directory.exists():
            return 0
        return sum(
            path.stat().st_size
            for path in self.directory.rglob("*")
            if path.is_file()
        )
