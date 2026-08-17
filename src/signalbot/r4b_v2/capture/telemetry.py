from __future__ import annotations

from collections import deque
from dataclasses import asdict, dataclass
from enum import StrEnum
from itertools import islice


class RejectionBoundV2(StrEnum):
    EVENTS = "events"
    ENCODED_BYTES = "encoded_bytes"
    EVENTS_AND_ENCODED_BYTES = "events_and_encoded_bytes"
    BATCH_ENCODED_BYTES = "batch_encoded_bytes"
    INGEST_SEQUENCE = "ingest_sequence"
    SERIALIZATION = "serialization"
    MONOTONIC_CLOCK = "monotonic_clock"


@dataclass(frozen=True, slots=True)
class RecentSourceV2:
    source_kind: str
    source_logical_key: str | None
    ingest_seq: int


@dataclass(frozen=True, slots=True)
class CaptureHealthSnapshotV2:
    """Bounded capture-health state with no market outcome surface."""

    snapshot_monotonic_ns: int
    queue_max_events: int
    queue_max_encoded_bytes: int
    offers_events: int
    offers_encoded_bytes: int
    enqueued_events: int
    enqueued_encoded_bytes: int
    dequeued_events: int
    dequeued_encoded_bytes: int
    durable_acked_events: int
    durable_acked_encoded_bytes: int
    discarded_events: int
    discarded_encoded_bytes: int
    rejected_events: int
    rejected_encoded_bytes: int
    current_events: int
    current_encoded_bytes: int
    peak_events: int
    peak_encoded_bytes: int
    remaining_event_headroom: int
    remaining_encoded_byte_headroom: int
    last_rejection_bound: RejectionBoundV2 | None
    oldest_enqueued_age_ns: int | None
    consumer_lag_records: int
    consumer_lag_encoded_bytes: int
    time_since_last_dequeue_ns: int | None
    time_since_last_durable_ack_ns: int | None
    durable_ack_seq: int | None
    batches_started: int
    batches_completed: int
    batches_failed: int
    worker_crossings: int
    total_batch_records: int
    total_batch_encoded_bytes: int
    last_batch_records: int
    last_batch_encoded_bytes: int
    peak_batch_records: int
    peak_batch_encoded_bytes: int
    last_batch_queue_wait_ns: int | None
    peak_batch_queue_wait_ns: int
    last_batch_linger_ns: int | None
    peak_batch_linger_ns: int
    last_writer_latency_ns: int | None
    peak_writer_latency_ns: int
    total_writer_busy_ns: int
    recent_sources: tuple[RecentSourceV2, ...]
    schema_version: str = "r4b_v2_capture_health_snapshot_v2"

    def to_document(self) -> dict[str, object]:
        """Return a JSON-compatible document suitable for durable health reports."""

        return asdict(self)


@dataclass(frozen=True, slots=True)
class _PendingRecord:
    ingest_seq: int
    encoded_len: int
    enqueued_monotonic_ns: int


class CaptureTelemetryV2:
    """Single-event-loop owner for exact bounded queue and batch counters."""

    def __init__(
        self,
        *,
        queue_max_events: int,
        queue_max_encoded_bytes: int,
        recent_source_limit: int = 16,
    ) -> None:
        if queue_max_events < 1:
            raise ValueError("queue_max_events must be positive")
        if queue_max_encoded_bytes < 1:
            raise ValueError("queue_max_encoded_bytes must be positive")
        if not 1 <= recent_source_limit <= 64:
            raise ValueError("recent_source_limit must be between 1 and 64")
        self.queue_max_events = queue_max_events
        self.queue_max_encoded_bytes = queue_max_encoded_bytes
        self._pending: deque[_PendingRecord] = deque()
        self._recent_sources: deque[RecentSourceV2] = deque(maxlen=recent_source_limit)
        self._offers_events = 0
        self._offers_encoded_bytes = 0
        self._enqueued_events = 0
        self._enqueued_encoded_bytes = 0
        self._dequeued_events = 0
        self._dequeued_encoded_bytes = 0
        self._durable_acked_events = 0
        self._durable_acked_encoded_bytes = 0
        self._discarded_events = 0
        self._discarded_encoded_bytes = 0
        self._rejected_events = 0
        self._rejected_encoded_bytes = 0
        self._current_events = 0
        self._current_encoded_bytes = 0
        self._peak_events = 0
        self._peak_encoded_bytes = 0
        self._last_rejection_bound: RejectionBoundV2 | None = None
        self._last_dequeue_ns: int | None = None
        self._last_durable_ack_ns: int | None = None
        self._durable_ack_seq: int | None = None
        self._batches_started = 0
        self._batches_completed = 0
        self._batches_failed = 0
        self._worker_crossings = 0
        self._total_batch_records = 0
        self._total_batch_encoded_bytes = 0
        self._last_batch_records = 0
        self._last_batch_encoded_bytes = 0
        self._peak_batch_records = 0
        self._peak_batch_encoded_bytes = 0
        self._last_batch_queue_wait_ns: int | None = None
        self._peak_batch_queue_wait_ns = 0
        self._last_batch_linger_ns: int | None = None
        self._peak_batch_linger_ns = 0
        self._last_writer_latency_ns: int | None = None
        self._peak_writer_latency_ns = 0
        self._total_writer_busy_ns = 0

    def note_offer(
        self,
        *,
        ingest_seq: int,
        encoded_len: int,
        source_kind: str,
        source_logical_key: str | None,
    ) -> None:
        self._offers_events += 1
        self._offers_encoded_bytes += encoded_len
        self._remember_source(source_kind, source_logical_key, ingest_seq)

    def note_unencoded_offer(
        self,
        *,
        ingest_seq: int,
        source_kind: str,
        source_logical_key: str | None,
    ) -> None:
        self._offers_events += 1
        self._remember_source(source_kind, source_logical_key, ingest_seq)

    def note_enqueue(
        self,
        *,
        ingest_seq: int,
        encoded_len: int,
        enqueued_monotonic_ns: int,
    ) -> None:
        if len(self._pending) >= self.queue_max_events:
            raise RuntimeError("telemetry pending state exceeded its event bound")
        self._pending.append(
            _PendingRecord(
                ingest_seq=ingest_seq,
                encoded_len=encoded_len,
                enqueued_monotonic_ns=enqueued_monotonic_ns,
            )
        )
        self._enqueued_events += 1
        self._enqueued_encoded_bytes += encoded_len
        self._current_events += 1
        self._current_encoded_bytes += encoded_len
        self._peak_events = max(self._peak_events, self._current_events)
        self._peak_encoded_bytes = max(
            self._peak_encoded_bytes,
            self._current_encoded_bytes,
        )

    def note_rejection(
        self,
        *,
        encoded_len: int,
        bound: RejectionBoundV2,
    ) -> None:
        self._rejected_events += 1
        self._rejected_encoded_bytes += encoded_len
        self._last_rejection_bound = bound

    def note_dequeue(
        self,
        *,
        records: tuple[tuple[int, int, int], ...],
        dequeued_monotonic_ns: int,
        linger_ns: int,
    ) -> None:
        if not records:
            return
        self._require_pending_prefix(records)
        count = len(records)
        encoded_bytes = sum(item[1] for item in records)
        oldest_wait = max(0, dequeued_monotonic_ns - records[0][2])
        self._dequeued_events += count
        self._dequeued_encoded_bytes += encoded_bytes
        self._last_dequeue_ns = dequeued_monotonic_ns
        self._batches_started += 1
        self._last_batch_records = count
        self._last_batch_encoded_bytes = encoded_bytes
        self._peak_batch_records = max(self._peak_batch_records, count)
        self._peak_batch_encoded_bytes = max(
            self._peak_batch_encoded_bytes,
            encoded_bytes,
        )
        self._last_batch_queue_wait_ns = oldest_wait
        self._peak_batch_queue_wait_ns = max(
            self._peak_batch_queue_wait_ns,
            oldest_wait,
        )
        self._last_batch_linger_ns = linger_ns
        self._peak_batch_linger_ns = max(self._peak_batch_linger_ns, linger_ns)

    def note_worker_crossing(self) -> None:
        self._worker_crossings += 1

    def note_durable_ack(
        self,
        *,
        records: tuple[tuple[int, int, int], ...],
        durable_ack_seq: int,
        completed_monotonic_ns: int,
        writer_latency_ns: int,
    ) -> None:
        if not records:
            raise ValueError("a durable batch acknowledgement requires records")
        self._require_pending_prefix(records)
        if durable_ack_seq != records[-1][0]:
            raise ValueError("durable acknowledgement does not equal batch tail")
        if writer_latency_ns < 0:
            raise ValueError("writer latency must be nonnegative")
        count = len(records)
        encoded_bytes = sum(item[1] for item in records)
        for _item in records:
            self._pending.popleft()
        self._current_events -= count
        self._current_encoded_bytes -= encoded_bytes
        self._durable_acked_events += count
        self._durable_acked_encoded_bytes += encoded_bytes
        self._durable_ack_seq = durable_ack_seq
        self._last_durable_ack_ns = completed_monotonic_ns
        self._batches_completed += 1
        self._total_batch_records += count
        self._total_batch_encoded_bytes += encoded_bytes
        self._last_writer_latency_ns = writer_latency_ns
        self._peak_writer_latency_ns = max(
            self._peak_writer_latency_ns,
            writer_latency_ns,
        )
        self._total_writer_busy_ns += writer_latency_ns

    def note_batch_failure(self, *, writer_latency_ns: int) -> None:
        if writer_latency_ns < 0:
            raise ValueError("writer latency must be nonnegative")
        self._batches_failed += 1
        self._last_writer_latency_ns = writer_latency_ns
        self._peak_writer_latency_ns = max(
            self._peak_writer_latency_ns,
            writer_latency_ns,
        )
        self._total_writer_busy_ns += writer_latency_ns

    def discard_all_pending(self) -> None:
        self._discarded_events += self._current_events
        self._discarded_encoded_bytes += self._current_encoded_bytes
        self._current_events = 0
        self._current_encoded_bytes = 0
        self._pending.clear()

    def snapshot(self, *, monotonic_ns: int) -> CaptureHealthSnapshotV2:
        if monotonic_ns < 0:
            raise ValueError("snapshot monotonic time must be nonnegative")
        oldest_age = (
            None
            if not self._pending
            else max(0, monotonic_ns - self._pending[0].enqueued_monotonic_ns)
        )
        return CaptureHealthSnapshotV2(
            snapshot_monotonic_ns=monotonic_ns,
            queue_max_events=self.queue_max_events,
            queue_max_encoded_bytes=self.queue_max_encoded_bytes,
            offers_events=self._offers_events,
            offers_encoded_bytes=self._offers_encoded_bytes,
            enqueued_events=self._enqueued_events,
            enqueued_encoded_bytes=self._enqueued_encoded_bytes,
            dequeued_events=self._dequeued_events,
            dequeued_encoded_bytes=self._dequeued_encoded_bytes,
            durable_acked_events=self._durable_acked_events,
            durable_acked_encoded_bytes=self._durable_acked_encoded_bytes,
            discarded_events=self._discarded_events,
            discarded_encoded_bytes=self._discarded_encoded_bytes,
            rejected_events=self._rejected_events,
            rejected_encoded_bytes=self._rejected_encoded_bytes,
            current_events=self._current_events,
            current_encoded_bytes=self._current_encoded_bytes,
            peak_events=self._peak_events,
            peak_encoded_bytes=self._peak_encoded_bytes,
            remaining_event_headroom=self.queue_max_events - self._current_events,
            remaining_encoded_byte_headroom=(
                self.queue_max_encoded_bytes - self._current_encoded_bytes
            ),
            last_rejection_bound=self._last_rejection_bound,
            oldest_enqueued_age_ns=oldest_age,
            consumer_lag_records=self._current_events,
            consumer_lag_encoded_bytes=self._current_encoded_bytes,
            time_since_last_dequeue_ns=_age(monotonic_ns, self._last_dequeue_ns),
            time_since_last_durable_ack_ns=_age(
                monotonic_ns,
                self._last_durable_ack_ns,
            ),
            durable_ack_seq=self._durable_ack_seq,
            batches_started=self._batches_started,
            batches_completed=self._batches_completed,
            batches_failed=self._batches_failed,
            worker_crossings=self._worker_crossings,
            total_batch_records=self._total_batch_records,
            total_batch_encoded_bytes=self._total_batch_encoded_bytes,
            last_batch_records=self._last_batch_records,
            last_batch_encoded_bytes=self._last_batch_encoded_bytes,
            peak_batch_records=self._peak_batch_records,
            peak_batch_encoded_bytes=self._peak_batch_encoded_bytes,
            last_batch_queue_wait_ns=self._last_batch_queue_wait_ns,
            peak_batch_queue_wait_ns=self._peak_batch_queue_wait_ns,
            last_batch_linger_ns=self._last_batch_linger_ns,
            peak_batch_linger_ns=self._peak_batch_linger_ns,
            last_writer_latency_ns=self._last_writer_latency_ns,
            peak_writer_latency_ns=self._peak_writer_latency_ns,
            total_writer_busy_ns=self._total_writer_busy_ns,
            recent_sources=tuple(self._recent_sources),
        )

    def _remember_source(
        self,
        source_kind: str,
        source_logical_key: str | None,
        ingest_seq: int,
    ) -> None:
        if (
            not source_kind
            or source_kind.strip() != source_kind
            or len(source_kind) > 512
        ):
            raise ValueError("source_kind must be a bounded normalized health label")
        if source_logical_key is not None and (
            not source_logical_key
            or source_logical_key.strip() != source_logical_key
            or len(source_logical_key) > 512
        ):
            raise ValueError(
                "source_logical_key must be a bounded normalized health label"
            )
        self._recent_sources.append(
            RecentSourceV2(
                source_kind=source_kind,
                source_logical_key=source_logical_key,
                ingest_seq=ingest_seq,
            )
        )

    def _require_pending_prefix(
        self,
        records: tuple[tuple[int, int, int], ...],
    ) -> None:
        if len(records) > len(self._pending):
            raise RuntimeError("batch exceeds telemetry pending prefix")
        expected = tuple(
            (item.ingest_seq, item.encoded_len, item.enqueued_monotonic_ns)
            for item in islice(self._pending, len(records))
        )
        if expected != records:
            raise RuntimeError("batch is not the exact telemetry pending prefix")


def _age(now_ns: int, then_ns: int | None) -> int | None:
    if then_ns is None:
        return None
    return max(0, now_ns - then_ns)
