from __future__ import annotations

import asyncio
import hashlib
import hmac
import re
import time
from collections import deque
from collections.abc import Callable
from dataclasses import InitVar, dataclass, field
from enum import StrEnum
from itertools import pairwise

from signalbot.r4b_v2.canonical import canonical_json_line
from signalbot.r4b_v2.capture.models import RawRecordV2
from signalbot.r4b_v2.capture.telemetry import (
    CaptureHealthSnapshotV2,
    CaptureTelemetryV2,
    RejectionBoundV2,
)

_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_CAPTURE_QUEUE_ADMISSION_RECEIPT_FACTORY_TOKEN = object()


class CaptureBatchErrorV2(RuntimeError):
    """Base error for the disconnected V2 ingress substrate."""


class CaptureBatchClosedV2(CaptureBatchErrorV2):
    pass


class CaptureBatchOverflowV2(CaptureBatchErrorV2):
    def __init__(self, bound: RejectionBoundV2) -> None:
        self.bound = bound
        super().__init__(f"V2 capture handoff rejected the {bound.value} bound")


class CaptureBatchSequenceErrorV2(CaptureBatchErrorV2):
    pass


class CaptureBatchSerializationErrorV2(CaptureBatchErrorV2):
    pass


class CaptureBatchIntegrityErrorV2(CaptureBatchErrorV2):
    pass


class CaptureBatchAckErrorV2(CaptureBatchErrorV2):
    pass


class CaptureBatchClockErrorV2(CaptureBatchErrorV2):
    pass


class CaptureFinalityFenceErrorV2(CaptureBatchErrorV2):
    pass


@dataclass(frozen=True, slots=True)
class QueuedRawRecordV2:
    """Encode-once immutable queue item consumed verbatim by WAL/block writers."""

    record: RawRecordV2
    encoded_line: bytes
    encoded_len: int
    encoded_sha256: str
    raw_len: int
    ingest_seq: int
    enqueued_monotonic_ns: int

    def __post_init__(self) -> None:
        self.verify_integrity()

    @classmethod
    def encode(
        cls,
        record: RawRecordV2,
        *,
        enqueued_monotonic_ns: int | None = None,
    ) -> QueuedRawRecordV2:
        """Perform the sole canonical serialization for an accepted record."""

        enqueued_ns = (
            time.monotonic_ns() if enqueued_monotonic_ns is None else enqueued_monotonic_ns
        )
        encoded = canonical_json_line(record)
        return cls(
            record=record,
            encoded_line=encoded,
            encoded_len=len(encoded),
            encoded_sha256=hashlib.sha256(encoded).hexdigest(),
            raw_len=record.raw_len,
            ingest_seq=record.ingest_seq,
            enqueued_monotonic_ns=enqueued_ns,
        )

    def verify_integrity(self) -> None:
        """Verify retained bytes and mirrored hashes without reserializing record."""

        if type(self.enqueued_monotonic_ns) is not int or self.enqueued_monotonic_ns < 0:
            raise CaptureBatchIntegrityErrorV2(
                "enqueued_monotonic_ns must be a nonnegative integer"
            )
        if self.enqueued_monotonic_ns < self.record.receipt_monotonic_ns:
            raise CaptureBatchIntegrityErrorV2("enqueue time precedes the receipt timestamp")
        if self.ingest_seq != self.record.ingest_seq:
            raise CaptureBatchIntegrityErrorV2("queued ingest sequence differs from record")
        if self.raw_len != self.record.raw_len:
            raise CaptureBatchIntegrityErrorV2("queued raw length differs from record")
        if not isinstance(self.encoded_line, bytes):
            raise CaptureBatchIntegrityErrorV2("encoded_line must be immutable bytes")
        if type(self.encoded_len) is not int or self.encoded_len < 1:
            raise CaptureBatchIntegrityErrorV2("encoded_len must be a positive integer")
        if len(self.encoded_line) != self.encoded_len:
            raise CaptureBatchIntegrityErrorV2("encoded_len differs from encoded_line")
        if (
            not isinstance(self.encoded_sha256, str)
            or _SHA256_RE.fullmatch(self.encoded_sha256) is None
        ):
            raise CaptureBatchIntegrityErrorV2("encoded_sha256 is not a lowercase digest")
        actual = hashlib.sha256(self.encoded_line).hexdigest()
        if not hmac.compare_digest(actual, self.encoded_sha256):
            raise CaptureBatchIntegrityErrorV2("encoded_sha256 differs from encoded_line")
        if not self.encoded_line.endswith(b"\n") or self.encoded_line.count(b"\n") != 1:
            raise CaptureBatchIntegrityErrorV2("encoded_line must be exactly one JSONL record")


@dataclass(frozen=True, slots=True)
class CaptureQueueAdmissionReceiptV2:
    """Local proof minted only by the bounded handoff that accepted a record.

    The receipt proves synchronous in-memory queue admission, not WAL or block
    durability.  Its per-handoff seal prevents a publicly encodable
    ``QueuedRawRecordV2`` or a receipt from another handoff from substituting
    for the actual acceptance owner.
    """

    queued_record: QueuedRawRecordV2 = field(repr=False)
    accepted_tail_ingest_seq: int
    _handoff: BoundedBatchHandoffV2 = field(repr=False, compare=False)
    _handoff_seal: object = field(repr=False, compare=False)
    _factory_token: InitVar[object | None] = None
    _factory_seal: object = field(init=False, repr=False, compare=False)

    def __post_init__(self, _factory_token: object | None) -> None:
        if _factory_token is not _CAPTURE_QUEUE_ADMISSION_RECEIPT_FACTORY_TOKEN:
            raise TypeError(
                "CaptureQueueAdmissionReceiptV2 can only be created by its bounded handoff"
            )
        object.__setattr__(
            self,
            "_factory_seal",
            _CAPTURE_QUEUE_ADMISSION_RECEIPT_FACTORY_TOKEN,
        )
        _validate_capture_queue_admission_receipt_material_v2(self)

    @property
    def record(self) -> RawRecordV2:
        return self.queued_record.record


def validate_capture_queue_admission_receipt_v2(
    receipt: CaptureQueueAdmissionReceiptV2,
    *,
    handoff: BoundedBatchHandoffV2 | None = None,
) -> QueuedRawRecordV2:
    """Revalidate factory and per-handoff provenance for one queue admission."""

    if type(receipt) is not CaptureQueueAdmissionReceiptV2:
        raise TypeError("queue admission requires an exact CaptureQueueAdmissionReceiptV2")
    if (
        getattr(receipt, "_factory_seal", None)
        is not _CAPTURE_QUEUE_ADMISSION_RECEIPT_FACTORY_TOKEN
    ):
        raise ValueError("queue-admission receipt lacks bounded-handoff provenance")
    _validate_capture_queue_admission_receipt_material_v2(receipt)
    if handoff is not None:
        if type(handoff) is not BoundedBatchHandoffV2:
            raise TypeError("queue-admission handoff must be exact")
        if receipt._handoff is not handoff:
            raise ValueError("queue-admission receipt belongs to a different bounded handoff")
    return receipt.queued_record


def _validate_capture_queue_admission_receipt_material_v2(
    receipt: CaptureQueueAdmissionReceiptV2,
) -> None:
    if type(receipt.queued_record) is not QueuedRawRecordV2:
        raise TypeError("queue-admission receipt requires an exact queued record")
    receipt.queued_record.verify_integrity()
    if type(receipt.accepted_tail_ingest_seq) is not int:
        raise TypeError("queue-admission accepted tail must be an exact integer")
    if receipt.accepted_tail_ingest_seq != receipt.queued_record.ingest_seq:
        raise ValueError("queue-admission accepted tail differs from its queued record")
    if type(receipt._handoff) is not BoundedBatchHandoffV2:
        raise TypeError("queue-admission receipt requires its exact bounded handoff")
    if receipt._handoff_seal is not receipt._handoff._queue_admission_receipt_seal:
        raise ValueError("queue-admission receipt has the wrong bounded-handoff seal")


@dataclass(frozen=True, slots=True)
class BatchPolicyV2:
    max_records: int
    max_encoded_bytes: int
    max_linger_us: int
    queue_max_events: int
    queue_max_encoded_bytes: int
    low_water_events: int
    low_water_encoded_bytes: int
    qualification_id: str

    def __post_init__(self) -> None:
        _require_positive(self.max_records, "max_records")
        _require_positive(self.max_encoded_bytes, "max_encoded_bytes")
        _require_nonnegative(self.max_linger_us, "max_linger_us")
        _require_positive(self.queue_max_events, "queue_max_events")
        _require_positive(self.queue_max_encoded_bytes, "queue_max_encoded_bytes")
        _require_nonnegative(self.low_water_events, "low_water_events")
        _require_nonnegative(self.low_water_encoded_bytes, "low_water_encoded_bytes")
        if self.max_records > self.queue_max_events:
            raise ValueError("max_records cannot exceed queue_max_events")
        if self.max_encoded_bytes > self.queue_max_encoded_bytes:
            raise ValueError("max_encoded_bytes cannot exceed queue_max_encoded_bytes")
        if self.low_water_events >= self.queue_max_events:
            raise ValueError("low_water_events must be below the event bound")
        if self.low_water_encoded_bytes >= self.queue_max_encoded_bytes:
            raise ValueError("low_water_encoded_bytes must be below the byte bound")
        if (
            not self.qualification_id
            or self.qualification_id.strip() != self.qualification_id
            or len(self.qualification_id) > 256
        ):
            raise ValueError("qualification_id must be a bounded normalized identity")


class BatchTerminalV2(StrEnum):
    STOP = "stop"
    FATAL = "fatal"


@dataclass(frozen=True, slots=True)
class CaptureFinalityFenceRequestV2:
    """One ordered request for finality through an accepted ingest prefix."""

    requested_ingest_seq: int
    fence_ingest_seq: int
    fence_monotonic_ns: int

    def __post_init__(self) -> None:
        _require_positive(self.requested_ingest_seq, "requested_ingest_seq")
        _require_positive(self.fence_ingest_seq, "fence_ingest_seq")
        _require_nonnegative(self.fence_monotonic_ns, "fence_monotonic_ns")
        if self.requested_ingest_seq != self.fence_ingest_seq:
            raise ValueError("requested_ingest_seq must equal the exact ordered fence prefix")


@dataclass(frozen=True, slots=True)
class CaptureBatchV2:
    records: tuple[QueuedRawRecordV2, ...]
    terminal: BatchTerminalV2 | None
    dequeued_monotonic_ns: int
    linger_ns: int
    finality_fence: CaptureFinalityFenceRequestV2 | None = None

    def __post_init__(self) -> None:
        kind_count = (
            int(bool(self.records))
            + int(self.terminal is not None)
            + int(self.finality_fence is not None)
        )
        if kind_count != 1:
            raise ValueError(
                "capture batch must contain exactly one of records, terminal, or finality fence"
            )
        if self.terminal is not None and not isinstance(self.terminal, BatchTerminalV2):
            raise ValueError("terminal must be a BatchTerminalV2 value")
        if self.finality_fence is not None and not isinstance(
            self.finality_fence,
            CaptureFinalityFenceRequestV2,
        ):
            raise ValueError("finality_fence must be a CaptureFinalityFenceRequestV2 value")
        if type(self.dequeued_monotonic_ns) is not int or self.dequeued_monotonic_ns < 0:
            raise ValueError("dequeued_monotonic_ns must be nonnegative")
        if type(self.linger_ns) is not int or self.linger_ns < 0:
            raise ValueError("linger_ns must be nonnegative")
        sequences = tuple(record.ingest_seq for record in self.records)
        if any(right != left + 1 for left, right in pairwise(sequences)):
            raise ValueError("capture batch ingest sequence must be contiguous")

    @property
    def encoded_bytes(self) -> int:
        return sum(record.encoded_len for record in self.records)

    @property
    def last_ingest_seq(self) -> int | None:
        return None if not self.records else self.records[-1].ingest_seq

    @property
    def telemetry_records(self) -> tuple[tuple[int, int, int], ...]:
        return tuple(
            (record.ingest_seq, record.encoded_len, record.enqueued_monotonic_ns)
            for record in self.records
        )


@dataclass(frozen=True, slots=True)
class CaptureBatchFailureV2:
    cause: BaseException
    failing_ingest_seq: int | None
    rejection_bound: RejectionBoundV2 | None
    fatal_snapshot: CaptureHealthSnapshotV2


class CaptureFatalStateV2:
    """First-failure-wins state for producer and batch-writer boundaries."""

    def __init__(self, stop_event: asyncio.Event | None = None) -> None:
        self.stop_event = stop_event or asyncio.Event()
        self.failed_event = asyncio.Event()
        self._failure: CaptureBatchFailureV2 | None = None

    @property
    def failure(self) -> CaptureBatchFailureV2 | None:
        return self._failure

    @property
    def failed(self) -> bool:
        return self._failure is not None

    def trip(
        self,
        *,
        cause: BaseException,
        failing_ingest_seq: int | None,
        rejection_bound: RejectionBoundV2 | None,
        fatal_snapshot: CaptureHealthSnapshotV2,
    ) -> bool:
        if self._failure is not None:
            return False
        self._failure = CaptureBatchFailureV2(
            cause=cause,
            failing_ingest_seq=failing_ingest_seq,
            rejection_bound=rejection_bound,
            fatal_snapshot=fatal_snapshot,
        )
        self.stop_event.set()
        self.failed_event.set()
        return True

    def raise_if_failed(self) -> None:
        if self._failure is not None:
            raise self._failure.cause


class _ControlV2:
    def __init__(self, terminal: BatchTerminalV2) -> None:
        self.terminal = terminal


_STOP = _ControlV2(BatchTerminalV2.STOP)
_FATAL = _ControlV2(BatchTerminalV2.FATAL)
_HandoffItemV2 = QueuedRawRecordV2 | CaptureFinalityFenceRequestV2 | _ControlV2


class BoundedBatchHandoffV2:
    """Bound encoded evidence until exact durable batch acknowledgement."""

    def __init__(
        self,
        policy: BatchPolicyV2,
        *,
        fatal_state: CaptureFatalStateV2 | None = None,
        telemetry: CaptureTelemetryV2 | None = None,
        monotonic_ns: Callable[[], int] = time.monotonic_ns,
        expected_first_ingest_seq: int = 1,
    ) -> None:
        _require_positive(expected_first_ingest_seq, "expected_first_ingest_seq")
        self.policy = policy
        self.fatal_state = fatal_state or CaptureFatalStateV2()
        self.telemetry = telemetry or CaptureTelemetryV2(
            queue_max_events=policy.queue_max_events,
            queue_max_encoded_bytes=policy.queue_max_encoded_bytes,
        )
        if (
            self.telemetry.queue_max_events != policy.queue_max_events
            or self.telemetry.queue_max_encoded_bytes != policy.queue_max_encoded_bytes
        ):
            raise ValueError("telemetry bounds must equal the handoff policy")
        self._monotonic_ns = monotonic_ns
        self._last_now_ns: int | None = None
        # One physical slot is reserved for the sole in-flight finality fence and
        # another for a terminal marker. Logical record limits continue to count
        # dequeued-but-unacknowledged records and do not include either control.
        self._queue: asyncio.Queue[_HandoffItemV2] = asyncio.Queue(
            maxsize=policy.queue_max_events + 2
        )
        self._deferred: deque[_HandoffItemV2] = deque()
        self._accepting = True
        self._expected_ingest_seq = expected_first_ingest_seq
        self._unacked_events = 0
        self._unacked_encoded_bytes = 0
        self._active_batch: CaptureBatchV2 | None = None
        self._active_records_acknowledged = False
        self._inflight_finality_fence: CaptureFinalityFenceRequestV2 | None = None
        self._finality_fence_future: asyncio.Future[object] | None = None
        self._clean_tail_shutdown_started = False
        self._clean_tail_shutdown_request: CaptureFinalityFenceRequestV2 | None = None
        self._clean_tail_terminal_completed = False
        self._queue_admission_receipt_seal = object()

    @property
    def accepting(self) -> bool:
        return self._accepting

    @property
    def current_events(self) -> int:
        return self._unacked_events

    @property
    def current_encoded_bytes(self) -> int:
        return self._unacked_encoded_bytes

    @property
    def finality_fence_in_flight(self) -> bool:
        return self._inflight_finality_fence is not None

    @property
    def accepted_tail_ingest_seq(self) -> int:
        """Return the exact tail accepted by this handoff, or zero when empty."""

        return self._expected_ingest_seq - 1

    @property
    def clean_tail_shutdown_request(self) -> CaptureFinalityFenceRequestV2 | None:
        """Return the immutable request captured by the sole clean shutdown."""

        return self._clean_tail_shutdown_request

    def offer(self, record: RawRecordV2) -> QueuedRawRecordV2:
        """Encode and enqueue without awaiting; every rejection is fatal."""

        if not self._accepting or self.fatal_state.failed:
            raise CaptureBatchClosedV2("V2 capture handoff is stopped")
        try:
            now_ns = self._now()
        except CaptureBatchClockErrorV2 as error:
            self.telemetry.note_unencoded_offer(
                ingest_seq=record.ingest_seq,
                source_kind=record.source_kind,
                source_logical_key=record.source_logical_key,
            )
            self._reject_unencoded(
                record=record,
                error=error,
                bound=RejectionBoundV2.MONOTONIC_CLOCK,
                now_ns=self._failure_snapshot_ns(),
            )
            raise
        if record.ingest_seq != self._expected_ingest_seq:
            self.telemetry.note_unencoded_offer(
                ingest_seq=record.ingest_seq,
                source_kind=record.source_kind,
                source_logical_key=record.source_logical_key,
            )
            error = CaptureBatchSequenceErrorV2(
                "offered ingest sequence differs from the exact expected sequence"
            )
            self._reject_unencoded(
                record=record,
                error=error,
                bound=RejectionBoundV2.INGEST_SEQUENCE,
                now_ns=now_ns,
            )
            raise error
        try:
            item = QueuedRawRecordV2.encode(record, enqueued_monotonic_ns=now_ns)
        except Exception as exc:
            self.telemetry.note_unencoded_offer(
                ingest_seq=record.ingest_seq,
                source_kind=record.source_kind,
                source_logical_key=record.source_logical_key,
            )
            error = CaptureBatchSerializationErrorV2(
                "V2 raw record could not be encoded losslessly"
            )
            self._reject_unencoded(
                record=record,
                error=error,
                bound=RejectionBoundV2.SERIALIZATION,
                now_ns=now_ns,
            )
            raise error from exc
        self.telemetry.note_offer(
            ingest_seq=item.ingest_seq,
            encoded_len=item.encoded_len,
            source_kind=record.source_kind,
            source_logical_key=record.source_logical_key,
        )
        if item.encoded_len > self.policy.max_encoded_bytes:
            self._reject_encoded(
                item,
                RejectionBoundV2.BATCH_ENCODED_BYTES,
                now_ns,
            )
        next_events = self._unacked_events + 1
        next_bytes = self._unacked_encoded_bytes + item.encoded_len
        event_overflow = next_events > self.policy.queue_max_events
        byte_overflow = next_bytes > self.policy.queue_max_encoded_bytes
        if event_overflow or byte_overflow:
            bound = _overflow_bound(event_overflow, byte_overflow)
            self._reject_encoded(item, bound, now_ns)
        try:
            self._queue.put_nowait(item)
        except asyncio.QueueFull as exc:
            error = CaptureBatchOverflowV2(RejectionBoundV2.EVENTS)
            self.telemetry.note_rejection(
                encoded_len=item.encoded_len,
                bound=RejectionBoundV2.EVENTS,
            )
            self._trip(
                cause=error,
                failing_ingest_seq=item.ingest_seq,
                rejection_bound=RejectionBoundV2.EVENTS,
                now_ns=now_ns,
            )
            raise error from exc
        self._unacked_events = next_events
        self._unacked_encoded_bytes = next_bytes
        self._expected_ingest_seq += 1
        self.telemetry.note_enqueue(
            ingest_seq=item.ingest_seq,
            encoded_len=item.encoded_len,
            enqueued_monotonic_ns=item.enqueued_monotonic_ns,
        )
        return item

    def offer_with_admission_receipt(
        self,
        record: RawRecordV2,
    ) -> CaptureQueueAdmissionReceiptV2:
        """Accept once and mint this exact handoff's local admission proof."""

        queued_record = self.offer(record)
        return CaptureQueueAdmissionReceiptV2(
            queued_record=queued_record,
            accepted_tail_ingest_seq=self.accepted_tail_ingest_seq,
            _handoff=self,
            _handoff_seal=self._queue_admission_receipt_seal,
            _factory_token=_CAPTURE_QUEUE_ADMISSION_RECEIPT_FACTORY_TOKEN,
        )

    def validate_queue_admission_receipt_v2(
        self,
        receipt: CaptureQueueAdmissionReceiptV2,
    ) -> QueuedRawRecordV2:
        """Return the queued record only for this exact handoff's proof."""

        return validate_capture_queue_admission_receipt_v2(
            receipt,
            handoff=self,
        )

    def offer_finality_fence(
        self,
        requested_ingest_seq: int,
    ) -> asyncio.Future[object]:
        """Enqueue one non-terminal barrier after the exact accepted prefix."""

        _require_positive(requested_ingest_seq, "requested_ingest_seq")
        if self.fatal_state.failed:
            self.fatal_state.raise_if_failed()
        if not self._accepting:
            raise CaptureBatchClosedV2("V2 capture handoff is stopped")
        if self._inflight_finality_fence is not None:
            raise CaptureFinalityFenceErrorV2("only one V2 finality fence may be in flight")
        fence_ingest_seq = self._expected_ingest_seq - 1
        if requested_ingest_seq != fence_ingest_seq:
            raise CaptureFinalityFenceErrorV2(
                "requested_ingest_seq must equal the current accepted ingest tail"
            )
        try:
            fence_monotonic_ns = self._now()
        except CaptureBatchClockErrorV2 as error:
            self._trip(
                cause=error,
                failing_ingest_seq=None,
                rejection_bound=RejectionBoundV2.MONOTONIC_CLOCK,
                now_ns=self._failure_snapshot_ns(),
            )
            raise
        future: asyncio.Future[object] = asyncio.get_running_loop().create_future()
        request = CaptureFinalityFenceRequestV2(
            requested_ingest_seq=requested_ingest_seq,
            fence_ingest_seq=fence_ingest_seq,
            fence_monotonic_ns=fence_monotonic_ns,
        )
        self._inflight_finality_fence = request
        self._finality_fence_future = future
        try:
            self._queue.put_nowait(request)
        except asyncio.QueueFull as exc:
            error = CaptureFinalityFenceErrorV2("reserved V2 finality-fence slot was unavailable")
            self._trip(
                cause=error,
                failing_ingest_seq=None,
                rejection_bound=None,
                now_ns=fence_monotonic_ns,
            )
            raise error from exc
        return future

    def begin_clean_tail_shutdown(self) -> asyncio.Future[object]:
        """Atomically stop offers and enqueue the exact tail fence then STOP.

        The method contains no await point and therefore owns the event-loop
        linearization boundary between the last accepted record and shutdown.
        Empty clean shutdowns are intentionally unsupported until an explicit
        zero-tail finality contract exists.
        """

        if self.fatal_state.failed:
            self.fatal_state.raise_if_failed()
        if self._clean_tail_shutdown_started:
            raise CaptureFinalityFenceErrorV2("V2 clean tail shutdown may be started only once")
        if not self._accepting:
            raise CaptureBatchClosedV2("V2 capture handoff is stopped")
        if self._inflight_finality_fence is not None:
            raise CaptureFinalityFenceErrorV2(
                "clean tail shutdown requires no finality fence in flight"
            )

        self._clean_tail_shutdown_started = True
        self._accepting = False
        accepted_tail = self.accepted_tail_ingest_seq
        try:
            fence_monotonic_ns = self._now()
        except CaptureBatchClockErrorV2 as error:
            self._trip(
                cause=error,
                failing_ingest_seq=None,
                rejection_bound=RejectionBoundV2.MONOTONIC_CLOCK,
                now_ns=self._failure_snapshot_ns(),
            )
            raise
        if accepted_tail < 1:
            error = CaptureFinalityFenceErrorV2(
                "clean tail shutdown requires a positive accepted ingest tail"
            )
            self._trip(
                cause=error,
                failing_ingest_seq=None,
                rejection_bound=None,
                now_ns=fence_monotonic_ns,
            )
            raise error

        future: asyncio.Future[object] = asyncio.get_running_loop().create_future()
        request = CaptureFinalityFenceRequestV2(
            requested_ingest_seq=accepted_tail,
            fence_ingest_seq=accepted_tail,
            fence_monotonic_ns=fence_monotonic_ns,
        )
        self._clean_tail_shutdown_request = request
        self._inflight_finality_fence = request
        self._finality_fence_future = future
        try:
            self._queue.put_nowait(request)
            self._queue.put_nowait(_STOP)
        except asyncio.QueueFull as exc:
            error = CaptureFinalityFenceErrorV2(
                "reserved V2 clean-shutdown control slots were unavailable"
            )
            self._trip(
                cause=error,
                failing_ingest_seq=None,
                rejection_bound=None,
                now_ns=fence_monotonic_ns,
            )
            raise error from exc
        return future

    def stop_producer(self) -> None:
        if not self._accepting:
            return
        self._accepting = False
        self._put_control(_STOP)

    def fail_consumer(
        self,
        cause: BaseException,
        *,
        failing_ingest_seq: int | None,
    ) -> None:
        self._accepting = False
        try:
            now_ns = self._now()
        except CaptureBatchClockErrorV2:
            now_ns = self._failure_snapshot_ns()
        self._trip(
            cause=cause,
            failing_ingest_seq=failing_ingest_seq,
            rejection_bound=None,
            now_ns=now_ns,
        )

    async def join(self) -> None:
        await self._queue.join()

    def snapshot(self) -> CaptureHealthSnapshotV2:
        return self.telemetry.snapshot(monotonic_ns=self._now())

    def acknowledge_records(
        self,
        batch: CaptureBatchV2,
        *,
        durable_ack_seq: int,
        completed_monotonic_ns: int,
        writer_latency_ns: int,
    ) -> None:
        if not batch.records:
            raise CaptureBatchAckErrorV2("cannot acknowledge an empty record batch")
        if batch is not self._active_batch or self._active_records_acknowledged:
            raise CaptureBatchAckErrorV2(
                "only the exact active dequeued batch may be acknowledged once"
            )
        if durable_ack_seq != batch.last_ingest_seq:
            raise CaptureBatchAckErrorV2(
                "writer durable acknowledgement differs from the exact batch tail"
            )
        self.telemetry.note_durable_ack(
            records=batch.telemetry_records,
            durable_ack_seq=durable_ack_seq,
            completed_monotonic_ns=completed_monotonic_ns,
            writer_latency_ns=writer_latency_ns,
        )
        self._unacked_events -= len(batch.records)
        self._unacked_encoded_bytes -= batch.encoded_bytes
        for _record in batch.records:
            self._queue.task_done()
        self._active_records_acknowledged = True
        if batch.terminal is None:
            self._active_batch = None
            self._active_records_acknowledged = False

    def complete_terminal(self, batch: CaptureBatchV2) -> None:
        if batch.terminal is None:
            raise CaptureBatchAckErrorV2("batch has no terminal marker")
        if batch is not self._active_batch:
            raise CaptureBatchAckErrorV2("only the exact active terminal batch may be completed")
        if batch.records and not self._active_records_acknowledged:
            raise CaptureBatchAckErrorV2(
                "terminal cannot complete before its records are durably acknowledged"
            )
        self._queue.task_done()
        self._active_batch = None
        self._active_records_acknowledged = False
        if self._clean_tail_shutdown_started:
            if batch.terminal is not BatchTerminalV2.STOP:
                raise CaptureBatchAckErrorV2(
                    "clean tail shutdown completed with a non-STOP terminal"
                )
            if self._inflight_finality_fence is not None:
                raise CaptureBatchAckErrorV2("clean tail terminal preceded its finality fence")
            self._clean_tail_terminal_completed = True

    def assert_clean_stopped_current_tail_v2(
        self,
        request: CaptureFinalityFenceRequestV2,
    ) -> None:
        """Fail unless the exact captured clean tail is fully queue-drained."""

        if type(request) is not CaptureFinalityFenceRequestV2:
            raise TypeError("request must be an exact CaptureFinalityFenceRequestV2")
        if request != self._clean_tail_shutdown_request:
            raise CaptureFinalityFenceErrorV2(
                "request differs from the internally captured clean tail"
            )
        if self.fatal_state.failed:
            self.fatal_state.raise_if_failed()
        if self._accepting:
            raise CaptureFinalityFenceErrorV2("clean-stopped handoff is still accepting")
        if not self._clean_tail_terminal_completed:
            raise CaptureFinalityFenceErrorV2("clean STOP terminal is not complete")
        if self.accepted_tail_ingest_seq != request.fence_ingest_seq:
            raise CaptureFinalityFenceErrorV2(
                "accepted tail extended beyond the captured clean shutdown"
            )
        if self._inflight_finality_fence is not None or (self._finality_fence_future is not None):
            raise CaptureFinalityFenceErrorV2("clean-stopped finality fence remains active")
        if (
            self._unacked_events != 0
            or self._unacked_encoded_bytes != 0
            or self._active_batch is not None
            or self._deferred
            or not self._queue.empty()
        ):
            raise CaptureFinalityFenceErrorV2("clean-stopped handoff queue is not drained")

    def complete_finality_fence(
        self,
        batch: CaptureBatchV2,
        *,
        result: object,
    ) -> None:
        """Complete only the exact active ordered fence and release its queue item."""

        request = batch.finality_fence
        if request is None:
            raise CaptureBatchAckErrorV2("batch has no finality fence")
        if batch is not self._active_batch:
            raise CaptureBatchAckErrorV2(
                "only the exact active finality-fence batch may be completed"
            )
        if request is not self._inflight_finality_fence:
            raise CaptureBatchAckErrorV2("active finality fence differs from the in-flight request")
        future = self._require_finality_fence_future(request)
        failure = self.fatal_state.failure
        self._queue.task_done()
        self._active_batch = None
        self._active_records_acknowledged = False
        self._inflight_finality_fence = None
        self._finality_fence_future = None
        if failure is not None:
            self._set_future_exception_once(future, failure.cause)
            raise failure.cause
        if future.done():
            raise CaptureFinalityFenceErrorV2(
                "finality-fence future completed before its ordered fence"
            )
        future.set_result(result)

    def discard_all(
        self,
        *,
        active_batch: CaptureBatchV2 | None = None,
        active_records_acknowledged: bool = False,
    ) -> None:
        """Release queue accounting only after a fatal snapshot is retained."""

        failure = self.fatal_state.failure
        discard_cause: BaseException = (
            CaptureBatchClosedV2("V2 finality fence was discarded before completion")
            if failure is None
            else failure.cause
        )
        if active_batch is None and self._active_batch is not None:
            active_batch = self._active_batch
            active_records_acknowledged = self._active_records_acknowledged
        if active_batch is not None and active_batch is not self._active_batch:
            if not (
                active_records_acknowledged
                and active_batch.terminal is None
                and self._active_batch is None
            ):
                raise RuntimeError("discard batch differs from the active dequeued batch")
        if active_batch is not None:
            if not active_records_acknowledged:
                for _record in active_batch.records:
                    self._queue.task_done()
            if active_batch.terminal is not None:
                self._queue.task_done()
            if active_batch.finality_fence is not None:
                self._discard_finality_fence(
                    active_batch.finality_fence,
                    discard_cause,
                )
                self._queue.task_done()
        while self._deferred:
            item = self._deferred.popleft()
            if isinstance(item, CaptureFinalityFenceRequestV2):
                self._discard_finality_fence(item, discard_cause)
            self._queue.task_done()
        while True:
            try:
                item = self._queue.get_nowait()
            except asyncio.QueueEmpty:
                break
            if isinstance(item, CaptureFinalityFenceRequestV2):
                self._discard_finality_fence(item, discard_cause)
            self._queue.task_done()
        if self._inflight_finality_fence is not None:
            self._discard_finality_fence(
                self._inflight_finality_fence,
                discard_cause,
            )
        self._unacked_events = 0
        self._unacked_encoded_bytes = 0
        self._active_batch = None
        self._active_records_acknowledged = False
        self._inflight_finality_fence = None
        self._finality_fence_future = None
        self.telemetry.discard_all_pending()

    async def _take(self) -> _HandoffItemV2:
        if self._deferred:
            return self._deferred.popleft()
        return await self._queue.get()

    async def _take_until(self, deadline_ns: int) -> _HandoffItemV2 | None:
        remaining_ns = deadline_ns - self._now()
        if self.policy.max_linger_us > 0 and remaining_ns <= 0:
            return None
        if self._deferred:
            return self._deferred.popleft()
        try:
            return self._queue.get_nowait()
        except asyncio.QueueEmpty:
            pass
        if self.policy.max_linger_us == 0 or remaining_ns <= 0:
            return None
        try:
            return await asyncio.wait_for(
                self._queue.get(),
                timeout=remaining_ns / 1_000_000_000,
            )
        except TimeoutError:
            return None

    def _restore_front(self, items: list[_HandoffItemV2]) -> None:
        for item in reversed(items):
            self._deferred.appendleft(item)

    def _defer_front(self, item: _HandoffItemV2) -> None:
        self._deferred.appendleft(item)

    def _note_dequeue(self, batch: CaptureBatchV2) -> None:
        if self._active_batch is not None:
            raise RuntimeError("a V2 batch is already awaiting acknowledgement")
        self.telemetry.note_dequeue(
            records=batch.telemetry_records,
            dequeued_monotonic_ns=batch.dequeued_monotonic_ns,
            linger_ns=batch.linger_ns,
        )
        self._active_batch = batch
        self._active_records_acknowledged = False

    def _reject_unencoded(
        self,
        *,
        record: RawRecordV2,
        error: BaseException,
        bound: RejectionBoundV2,
        now_ns: int,
    ) -> None:
        self.telemetry.note_rejection(encoded_len=0, bound=bound)
        self._trip(
            cause=error,
            failing_ingest_seq=record.ingest_seq,
            rejection_bound=bound,
            now_ns=now_ns,
        )

    def _reject_encoded(
        self,
        item: QueuedRawRecordV2,
        bound: RejectionBoundV2,
        now_ns: int,
    ) -> None:
        error = CaptureBatchOverflowV2(bound)
        self.telemetry.note_rejection(encoded_len=item.encoded_len, bound=bound)
        self._trip(
            cause=error,
            failing_ingest_seq=item.ingest_seq,
            rejection_bound=bound,
            now_ns=now_ns,
        )
        raise error

    def _trip(
        self,
        *,
        cause: BaseException,
        failing_ingest_seq: int | None,
        rejection_bound: RejectionBoundV2 | None,
        now_ns: int,
    ) -> None:
        self._accepting = False
        first = self.fatal_state.trip(
            cause=cause,
            failing_ingest_seq=failing_ingest_seq,
            rejection_bound=rejection_bound,
            fatal_snapshot=self.telemetry.snapshot(monotonic_ns=now_ns),
        )
        if first:
            self._fail_inflight_finality_fence(cause)
            self._put_control(_FATAL)

    def _require_finality_fence_future(
        self,
        request: CaptureFinalityFenceRequestV2,
    ) -> asyncio.Future[object]:
        if request is not self._inflight_finality_fence or self._finality_fence_future is None:
            raise CaptureFinalityFenceErrorV2("finality fence has no matching in-flight future")
        return self._finality_fence_future

    def _fail_inflight_finality_fence(self, cause: BaseException) -> None:
        future = self._finality_fence_future
        if future is not None:
            self._set_future_exception_once(future, cause)

    def _discard_finality_fence(
        self,
        request: CaptureFinalityFenceRequestV2,
        cause: BaseException,
    ) -> None:
        if request is not self._inflight_finality_fence:
            raise RuntimeError("discarded finality fence differs from in-flight request")
        future = self._require_finality_fence_future(request)
        self._set_future_exception_once(future, cause)
        self._inflight_finality_fence = None
        self._finality_fence_future = None

    @staticmethod
    def _set_future_exception_once(
        future: asyncio.Future[object],
        cause: BaseException,
    ) -> None:
        if not future.done():
            future.set_exception(cause)

    def _put_control(self, control: _ControlV2) -> None:
        try:
            self._queue.put_nowait(control)
        except asyncio.QueueFull as exc:
            raise RuntimeError("reserved V2 terminal control slot was unavailable") from exc

    def _now(self) -> int:
        value = self._monotonic_ns()
        if type(value) is not int or value < 0:
            raise CaptureBatchClockErrorV2("monotonic clock returned an invalid value")
        if self._last_now_ns is not None and value < self._last_now_ns:
            raise CaptureBatchClockErrorV2("monotonic clock moved backwards")
        self._last_now_ns = value
        return value

    def _failure_snapshot_ns(self) -> int:
        return 0 if self._last_now_ns is None else self._last_now_ns


class BatchDrainerV2:
    """Drain one contiguous bounded batch with cancellation-safe pushback."""

    def __init__(self, handoff: BoundedBatchHandoffV2) -> None:
        self.handoff = handoff

    async def next_batch(self) -> CaptureBatchV2:
        acquired: list[_HandoffItemV2] = []
        records: list[QueuedRawRecordV2] = []
        encoded_bytes = 0
        terminal: BatchTerminalV2 | None = None
        finality_fence: CaptureFinalityFenceRequestV2 | None = None
        try:
            first = await self.handoff._take()
            acquired.append(first)
            started_ns = self.handoff._now()
            deadline_ns = started_ns + self.handoff.policy.max_linger_us * 1_000
            if isinstance(first, _ControlV2):
                terminal = first.terminal
            elif isinstance(first, CaptureFinalityFenceRequestV2):
                finality_fence = first
            else:
                records.append(first)
                encoded_bytes = first.encoded_len
            while records and self._can_grow(len(records), encoded_bytes):
                item = await self.handoff._take_until(deadline_ns)
                if item is None:
                    break
                if not isinstance(item, QueuedRawRecordV2):
                    self.handoff._defer_front(item)
                    break
                if not self._fits(len(records), encoded_bytes, item):
                    self.handoff._defer_front(item)
                    break
                acquired.append(item)
                records.append(item)
                encoded_bytes += item.encoded_len
            completed_ns = self.handoff._now()
            batch = CaptureBatchV2(
                records=tuple(records),
                terminal=terminal,
                dequeued_monotonic_ns=completed_ns,
                linger_ns=max(0, completed_ns - started_ns),
                finality_fence=finality_fence,
            )
            self.handoff._note_dequeue(batch)
            return batch
        except BaseException:
            self.handoff._restore_front(acquired)
            raise

    def _can_grow(self, record_count: int, encoded_bytes: int) -> bool:
        if record_count >= self.handoff.policy.max_records:
            return False
        return encoded_bytes < self.handoff.policy.max_encoded_bytes

    def _fits(
        self,
        record_count: int,
        encoded_bytes: int,
        candidate: QueuedRawRecordV2,
    ) -> bool:
        return (
            record_count < self.handoff.policy.max_records
            and encoded_bytes + candidate.encoded_len <= self.handoff.policy.max_encoded_bytes
        )


def _overflow_bound(event_overflow: bool, byte_overflow: bool) -> RejectionBoundV2:
    if event_overflow and byte_overflow:
        return RejectionBoundV2.EVENTS_AND_ENCODED_BYTES
    if event_overflow:
        return RejectionBoundV2.EVENTS
    return RejectionBoundV2.ENCODED_BYTES


def _require_positive(value: int, field: str) -> None:
    if type(value) is not int or value < 1:
        raise ValueError(f"{field} must be a positive integer")


def _require_nonnegative(value: int, field: str) -> None:
    if type(value) is not int or value < 0:
        raise ValueError(f"{field} must be a nonnegative integer")
