from __future__ import annotations

import asyncio
import csv
import hashlib
import io
import json
import threading
import time
from collections import Counter
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from fractions import Fraction
from itertools import pairwise
from pathlib import Path
from typing import cast

import pytest
import zstandard as zstd

from signalbot.r4b_v2.canonical import canonical_json_line
from signalbot.r4b_v2.capture.batching import (
    BatchPolicyV2,
    BoundedBatchHandoffV2,
    CaptureBatchOverflowV2,
    QueuedRawRecordV2,
)
from signalbot.r4b_v2.capture.models import RawRecordV2, TransportV2, VenueV2
from signalbot.r4b_v2.capture.pipeline import (
    CaptureBatchPipelineV2,
    CaptureFinalityFenceReceiptV2,
)
from signalbot.r4b_v2.capture.telemetry import (
    CaptureHealthSnapshotV2,
    RejectionBoundV2,
)

_FIXTURE_ROOT = Path(__file__).resolve().parents[2] / "fixtures" / "r4b_v2" / "capture"
_MANIFEST_PATH = _FIXTURE_ROOT / "queue_overflow_tail_trace_20260717.manifest.json"
_DIAGNOSTIC_BATCH_RECORDS = 4_096
_DIAGNOSTIC_BATCH_BYTES = 4 * 1_024 * 1_024
_DIAGNOSTIC_BATCH_LINGER_US = 1_000
_DIAGNOSTIC_STOP_WATCHDOG_SECONDS = 30.0
_FAULT_EVENT_BOUND = 64
_MAX_TEST_PLAN_PADDING = 64
_PROVISIONAL_MULTIPLIERS = (
    (Fraction(1, 1), "recorded_1x_diagnostic"),
    (Fraction(2, 1), "provisional_2x_burst_diagnostic"),
)


@dataclass(frozen=True, slots=True)
class TraceRow:
    ingest_seq: int
    received_at_ms: int
    received_monotonic_ns: int
    delta_received_at_ms: int | None
    delta_received_monotonic_ns: int | None
    encoded_line_bytes: int
    schema_version: str
    source: str
    market: str
    route: str
    transport: str
    source_kind: str
    role_or_stream: str

    @classmethod
    def from_csv(cls, row: Mapping[str, str | None]) -> TraceRow:
        def required(key: str) -> str:
            value = row.get(key)
            if value is None:
                raise ValueError(f"recorded trace is missing {key}")
            return value

        def optional_int(key: str) -> int | None:
            value = required(key)
            return None if value == "" else int(value)

        return cls(
            ingest_seq=int(required("ingest_seq")),
            received_at_ms=int(required("received_at_ms")),
            received_monotonic_ns=int(required("received_monotonic_ns")),
            delta_received_at_ms=optional_int("delta_received_at_ms"),
            delta_received_monotonic_ns=optional_int(
                "delta_received_monotonic_ns"
            ),
            encoded_line_bytes=int(required("encoded_line_bytes")),
            schema_version=required("schema_version"),
            source=required("source"),
            market=required("market"),
            route=required("route"),
            transport=required("transport"),
            source_kind=required("source_kind"),
            role_or_stream=required("role_or_stream"),
        )

    @property
    def source_evidence(self) -> tuple[str, str, str, str, str]:
        return (
            self.market,
            self.route,
            self.transport,
            self.source_kind,
            self.role_or_stream,
        )

    @property
    def source_evidence_json(self) -> str:
        return json.dumps(
            self.source_evidence,
            ensure_ascii=True,
            separators=(",", ":"),
        )

    @property
    def venue(self) -> VenueV2:
        if self.market == "spot":
            return VenueV2.SPOT
        if self.market == "futures":
            return VenueV2.USDM_FUTURES
        raise ValueError(f"unsupported recorded market {self.market!r}")

    @property
    def transport_v2(self) -> TransportV2:
        if self.transport == "websocket":
            return TransportV2.WEBSOCKET
        if self.transport == "https":
            return TransportV2.HTTPS
        raise ValueError(f"unsupported recorded transport {self.transport!r}")

    @property
    def route_id(self) -> str:
        return self.route or self.role_or_stream


@dataclass(frozen=True, slots=True)
class RecordedTrace:
    rows: tuple[TraceRow, ...]
    source_csv_sha256: str
    fixture_sha256: str
    order_route_size_sha256: str
    session_id: str
    capture_plan_sha256: str
    encoded_line_bytes_sum: int
    receipt_span_monotonic_ns: int
    queue_max_events: int
    queue_max_encoded_bytes: int
    low_water_events: int
    low_water_encoded_bytes: int
    qualification_status: str


@dataclass(frozen=True, slots=True)
class ReplayResult:
    snapshot: CaptureHealthSnapshotV2
    writer: TraceSinkWriter
    producer_elapsed_ns: int
    recovery_elapsed_ns: int
    scaled_source_span_ns: int
    max_offer_lateness_ns: int


class ExactSizedRecordFactory:
    """Build test-only envelopes whose canonical line matches recorded V1 size."""

    def __init__(self, trace: RecordedTrace) -> None:
        self._trace = trace
        self._shapes: dict[tuple[str, int, int, int, int], tuple[int, int]] = {}

    def build(self, row: TraceRow, *, receipt_monotonic_ns: int) -> RawRecordV2:
        shape_key = (
            row.source_evidence_json,
            len(str(row.ingest_seq)),
            len(str(row.received_at_ms)),
            len(str(receipt_monotonic_ns)),
            row.encoded_line_bytes,
        )
        shape = self._shapes.get(shape_key)
        if shape is None:
            shape = self._find_shape(row, receipt_monotonic_ns=receipt_monotonic_ns)
            self._shapes[shape_key] = shape
        raw_length, plan_padding = shape
        record = self._make_record(
            row,
            receipt_monotonic_ns=receipt_monotonic_ns,
            payload=b"x" * raw_length,
            plan_padding=plan_padding,
        )
        if len(canonical_json_line(record)) != row.encoded_line_bytes:
            raise AssertionError("cached diagnostic envelope shape changed canonical size")
        return record

    def _find_shape(
        self,
        row: TraceRow,
        *,
        receipt_monotonic_ns: int,
    ) -> tuple[int, int]:
        target = row.encoded_line_bytes

        def encoded_length(raw_length: int, plan_padding: int = 0) -> int:
            candidate = self._make_record(
                row,
                receipt_monotonic_ns=receipt_monotonic_ns,
                payload=b"x" * raw_length,
                plan_padding=plan_padding,
            )
            return len(canonical_json_line(candidate))

        empty_length = encoded_length(0)
        if empty_length > target:
            raise ValueError(
                "recorded encoded-line size is too small for the diagnostic V2 envelope"
            )
        low = 0
        high = max(1, target - empty_length)
        while encoded_length(high) <= target:
            low = high
            high *= 2
            if high > target * 4:
                raise ValueError("diagnostic envelope sizing failed to bracket target")
        while low + 1 < high:
            midpoint = (low + high) // 2
            if encoded_length(midpoint) <= target:
                low = midpoint
            else:
                high = midpoint

        for raw_length in range(max(0, low - 8), high + 8):
            base_length = encoded_length(raw_length)
            plan_padding = target - base_length
            if not 0 <= plan_padding <= _MAX_TEST_PLAN_PADDING:
                continue
            if encoded_length(raw_length, plan_padding) == target:
                return raw_length, plan_padding
        raise ValueError("no deterministic diagnostic envelope has the recorded size")

    def _make_record(
        self,
        row: TraceRow,
        *,
        receipt_monotonic_ns: int,
        payload: bytes,
        plan_padding: int,
    ) -> RawRecordV2:
        return RawRecordV2.from_payload(
            session_id=self._trace.session_id,
            plan_id="phase1-recorded-tail-diagnostic" + "p" * plan_padding,
            protocol_hash=self._trace.capture_plan_sha256,
            transport=row.transport_v2,
            venue=row.venue,
            route_id=row.route_id,
            symbol=None,
            connection_id=row.role_or_stream,
            generation=1,
            frame_seq=None,
            ingest_seq=row.ingest_seq,
            receipt_wall_ms=row.received_at_ms,
            receipt_monotonic_ns=receipt_monotonic_ns,
            raw_payload=payload,
            source_logical_key=row.source_evidence_json,
        )


class TraceSinkWriter:
    """Non-durable sink that verifies the public batch-writer boundary exactly."""

    def __init__(self, rows: Sequence[TraceRow]) -> None:
        self._rows = rows
        self._cursor = 0
        self._order_digest = hashlib.sha256()
        self.ack_tails: list[int] = []
        self.received_encoded_bytes = 0
        self.closed = False
        self.aborted = False

    @property
    def received_count(self) -> int:
        return self._cursor

    @property
    def order_route_size_sha256(self) -> str:
        return self._order_digest.hexdigest()

    def append_many(self, records: Sequence[QueuedRawRecordV2]) -> int:
        if not records:
            raise AssertionError("pipeline passed an empty record batch to the sink")
        for item in records:
            if self._cursor >= len(self._rows):
                raise AssertionError("pipeline wrote beyond the exact recorded trace")
            expected = self._rows[self._cursor]
            self._verify_item(item, expected)
            self._order_digest.update(_actual_order_route_size_line(item))
            self.received_encoded_bytes += item.encoded_len
            self._cursor += 1
        tail = records[-1].ingest_seq
        self.ack_tails.append(tail)
        return tail

    def finalize_through(
        self,
        *,
        requested_ingest_seq: int,
        fence_ingest_seq: int,
        fence_monotonic_ns: int,
    ) -> CaptureFinalityFenceReceiptV2:
        del requested_ingest_seq, fence_ingest_seq, fence_monotonic_ns
        raise AssertionError("recorded burst diagnostics do not request finality fences")

    def close(self) -> None:
        if self._cursor != len(self._rows):
            raise AssertionError("sink closed before the exact trace was acknowledged")
        self.closed = True

    def abort(self) -> None:
        self.aborted = True

    @staticmethod
    def _verify_item(item: QueuedRawRecordV2, expected: TraceRow) -> None:
        record = item.record
        if item.ingest_seq != expected.ingest_seq:
            raise AssertionError("recorded ingest order changed in the V2 pipeline")
        if item.encoded_len != expected.encoded_line_bytes:
            raise AssertionError("recorded encoded-line size changed in the V2 pipeline")
        if record.receipt_wall_ms != expected.received_at_ms:
            raise AssertionError("recorded wall receipt changed in the V2 pipeline")
        if record.source_logical_key != expected.source_evidence_json:
            raise AssertionError("recorded source/route evidence changed")
        if record.venue is not expected.venue:
            raise AssertionError("recorded venue mapping changed")
        if record.transport is not expected.transport_v2:
            raise AssertionError("recorded transport mapping changed")
        if record.route_id != expected.route_id:
            raise AssertionError("recorded route mapping changed")
        if record.connection_id != expected.role_or_stream:
            raise AssertionError("recorded role/stream mapping changed")
        if record.symbol is not None or record.frame_seq is not None:
            raise AssertionError("benchmark invented absent symbol/frame evidence")


class StallingTraceSinkWriter(TraceSinkWriter):
    """Fault injector: hold the first public writer call until the test releases it."""

    def __init__(self, rows: Sequence[TraceRow]) -> None:
        super().__init__(rows)
        self.entered = threading.Event()
        self.release = threading.Event()

    def append_many(self, records: Sequence[QueuedRawRecordV2]) -> int:
        self.entered.set()
        if not self.release.wait(timeout=_DIAGNOSTIC_STOP_WATCHDOG_SECONDS):
            raise TimeoutError("fault-injection writer was not released")
        return super().append_many(records)


@pytest.fixture(scope="module")
def recorded_trace() -> RecordedTrace:
    return _load_recorded_trace()


@pytest.fixture(scope="module")
def prepared_trace_records(
    recorded_trace: RecordedTrace,
) -> tuple[RawRecordV2, ...]:
    factory = ExactSizedRecordFactory(recorded_trace)
    return tuple(factory.build(row, receipt_monotonic_ns=0) for row in recorded_trace.rows)


def test_recorded_fixture_is_exact_bounded_and_explicitly_not_qualified(
    recorded_trace: RecordedTrace,
) -> None:
    assert recorded_trace.source_csv_sha256 == (
        "be62309809c6e80608334f97e926907df331e841575d1455e99514e7fc7f306c"
    )
    assert recorded_trace.fixture_sha256 == (
        "1c621bea6e28294c1e7cf847f43b98662ba9f567770f5d8cf434a4dc01b2c70d"
    )
    assert len(recorded_trace.rows) == 99_999
    assert recorded_trace.rows[0].ingest_seq == 9_310_159
    assert recorded_trace.rows[-1].ingest_seq == 9_410_157
    assert recorded_trace.encoded_line_bytes_sum == 96_189_433
    assert recorded_trace.qualification_status == "NOT_QUALIFIED"


@pytest.mark.parametrize(
    ("speed_multiplier", "replay_label"),
    _PROVISIONAL_MULTIPLIERS,
    ids=("recorded-1x-diagnostic", "provisional-2x-burst-diagnostic"),
)
async def test_recorded_trace_replays_without_drop_and_recovers_to_low_water(
    recorded_trace: RecordedTrace,
    prepared_trace_records: tuple[RawRecordV2, ...],
    speed_multiplier: Fraction,
    replay_label: str,
    record_property: Callable[[str, object], None],
) -> None:
    result = await _run_successful_replay(
        recorded_trace,
        prepared_trace_records,
        speed_multiplier,
    )
    snapshot = result.snapshot
    writer = result.writer

    assert writer.received_count == len(recorded_trace.rows)
    assert writer.received_encoded_bytes == recorded_trace.encoded_line_bytes_sum
    assert writer.order_route_size_sha256 == recorded_trace.order_route_size_sha256
    assert writer.closed
    assert not writer.aborted
    assert writer.ack_tails
    assert writer.ack_tails[-1] == recorded_trace.rows[-1].ingest_seq
    assert all(right > left for left, right in pairwise(writer.ack_tails))

    assert snapshot.offers_events == len(recorded_trace.rows)
    assert snapshot.enqueued_events == len(recorded_trace.rows)
    assert snapshot.durable_acked_events == len(recorded_trace.rows)
    assert snapshot.rejected_events == 0
    assert snapshot.discarded_events == 0
    assert snapshot.current_events <= recorded_trace.low_water_events
    assert snapshot.current_encoded_bytes <= recorded_trace.low_water_encoded_bytes
    assert snapshot.durable_ack_seq == recorded_trace.rows[-1].ingest_seq
    assert snapshot.peak_events > 0
    assert snapshot.peak_events <= recorded_trace.queue_max_events
    assert snapshot.peak_encoded_bytes > 0
    assert snapshot.peak_encoded_bytes <= recorded_trace.queue_max_encoded_bytes
    assert snapshot.worker_crossings == len(writer.ack_tails)
    assert snapshot.batches_completed == len(writer.ack_tails)
    assert snapshot.batches_failed == 0
    assert result.producer_elapsed_ns >= result.scaled_source_span_ns
    assert result.recovery_elapsed_ns >= 0

    peak_event_headroom = recorded_trace.queue_max_events - snapshot.peak_events
    peak_byte_headroom = (
        recorded_trace.queue_max_encoded_bytes - snapshot.peak_encoded_bytes
    )
    assert peak_event_headroom >= 0
    assert peak_byte_headroom >= 0

    record_property("classification", "DIAGNOSTIC_PROVISIONAL_NOT_QUALIFICATION")
    record_property("qualification_status", recorded_trace.qualification_status)
    record_property("replay_label", replay_label)
    record_property("speed_multiplier", str(speed_multiplier))
    record_property("source_csv_sha256", recorded_trace.source_csv_sha256)
    record_property("producer_elapsed_ns", result.producer_elapsed_ns)
    record_property("scaled_source_span_ns", result.scaled_source_span_ns)
    record_property("recovery_to_full_drain_ns", result.recovery_elapsed_ns)
    record_property("max_offer_lateness_ns", result.max_offer_lateness_ns)
    record_property("queue_peak_events", snapshot.peak_events)
    record_property("queue_peak_encoded_bytes", snapshot.peak_encoded_bytes)
    record_property("queue_event_headroom_at_peak", peak_event_headroom)
    record_property("queue_encoded_byte_headroom_at_peak", peak_byte_headroom)


async def test_stalled_sink_fails_closed_with_exact_fatal_and_ack_accounting(
    recorded_trace: RecordedTrace,
) -> None:
    rows = recorded_trace.rows
    policy = BatchPolicyV2(
        max_records=1,
        max_encoded_bytes=max(row.encoded_line_bytes for row in rows),
        max_linger_us=0,
        queue_max_events=_FAULT_EVENT_BOUND,
        queue_max_encoded_bytes=recorded_trace.encoded_line_bytes_sum,
        low_water_events=0,
        low_water_encoded_bytes=0,
        qualification_id="fault-injection-only-stalled-sink-not-qualified",
    )
    handoff = BoundedBatchHandoffV2(
        policy,
        expected_first_ingest_seq=rows[0].ingest_seq,
    )
    writer = StallingTraceSinkWriter(rows)
    pipeline = CaptureBatchPipelineV2(handoff, writer)
    factory = ExactSizedRecordFactory(recorded_trace)
    pipeline.start()

    first = factory.build(rows[0], receipt_monotonic_ns=time.monotonic_ns())
    pipeline.offer(first)
    await _wait_for_thread_event(writer.entered)

    next_index = 1
    while handoff.current_events < policy.queue_max_events:
        record = factory.build(
            rows[next_index],
            receipt_monotonic_ns=time.monotonic_ns(),
        )
        pipeline.offer(record)
        next_index += 1

    rejected_row = rows[next_index]
    rejected_record = factory.build(
        rejected_row,
        receipt_monotonic_ns=time.monotonic_ns(),
    )
    with pytest.raises(CaptureBatchOverflowV2) as offered_error:
        pipeline.offer(rejected_record)

    failure = handoff.fatal_state.failure
    assert failure is not None
    assert failure.cause is offered_error.value
    assert failure.failing_ingest_seq == rejected_row.ingest_seq
    assert failure.rejection_bound is RejectionBoundV2.EVENTS
    assert failure.fatal_snapshot.current_events == policy.queue_max_events
    assert failure.fatal_snapshot.peak_events == policy.queue_max_events
    assert failure.fatal_snapshot.remaining_event_headroom == 0
    assert failure.fatal_snapshot.durable_ack_seq is None
    assert failure.fatal_snapshot.rejected_events == 1

    writer.release.set()
    with pytest.raises(CaptureBatchOverflowV2) as stopped_error:
        await asyncio.wait_for(
            pipeline.stop(),
            timeout=_DIAGNOSTIC_STOP_WATCHDOG_SECONDS,
        )
    assert stopped_error.value is offered_error.value

    final = pipeline.health_snapshot()
    assert final.current_events == 0
    assert final.durable_acked_events == 1
    assert final.durable_ack_seq == rows[0].ingest_seq
    assert final.discarded_events == policy.queue_max_events - 1
    assert final.rejected_events == 1
    assert final.offers_events == final.enqueued_events + final.rejected_events
    assert final.enqueued_events == (
        final.durable_acked_events + final.discarded_events + final.current_events
    )
    assert writer.received_count == 1
    assert writer.ack_tails == [rows[0].ingest_seq]
    assert writer.aborted
    assert not writer.closed


async def _run_successful_replay(
    trace: RecordedTrace,
    prepared_records: tuple[RawRecordV2, ...],
    speed_multiplier: Fraction,
) -> ReplayResult:
    if speed_multiplier <= 0:
        raise ValueError("recorded replay speed multiplier must be positive")
    policy = BatchPolicyV2(
        max_records=_DIAGNOSTIC_BATCH_RECORDS,
        max_encoded_bytes=_DIAGNOSTIC_BATCH_BYTES,
        max_linger_us=_DIAGNOSTIC_BATCH_LINGER_US,
        queue_max_events=trace.queue_max_events,
        queue_max_encoded_bytes=trace.queue_max_encoded_bytes,
        low_water_events=trace.low_water_events,
        low_water_encoded_bytes=trace.low_water_encoded_bytes,
        qualification_id="recorded-tail-diagnostic-not-qualified",
    )
    handoff = BoundedBatchHandoffV2(
        policy,
        expected_first_ingest_seq=trace.rows[0].ingest_seq,
    )
    writer = TraceSinkWriter(trace.rows)
    pipeline = CaptureBatchPipelineV2(handoff, writer)
    if len(prepared_records) != len(trace.rows):
        raise ValueError("prepared diagnostic records do not cover the exact trace")
    pipeline.start()

    source_start_ns = trace.rows[0].received_monotonic_ns
    replay_start_ns = time.monotonic_ns()
    max_offer_lateness_ns = 0
    try:
        for index, (row, record) in enumerate(zip(trace.rows, prepared_records, strict=True)):
            source_offset_ns = row.received_monotonic_ns - source_start_ns
            scaled_offset_ns = (
                source_offset_ns * speed_multiplier.denominator
            ) // speed_multiplier.numerator
            due_ns = replay_start_ns + scaled_offset_ns
            await _sleep_until(due_ns)
            if index % 256 == 0:
                await asyncio.sleep(0)
            max_offer_lateness_ns = max(
                max_offer_lateness_ns,
                max(0, time.monotonic_ns() - due_ns),
            )
            item = pipeline.offer(record)
            assert item.encoded_len == row.encoded_line_bytes
    except BaseException:
        await _stop_after_unexpected_replay_error(pipeline)
        raise

    producer_completed_ns = time.monotonic_ns()
    await asyncio.wait_for(
        pipeline.stop(),
        timeout=_DIAGNOSTIC_STOP_WATCHDOG_SECONDS,
    )
    recovered_ns = time.monotonic_ns()
    scaled_source_span_ns = (
        trace.receipt_span_monotonic_ns * speed_multiplier.denominator
    ) // speed_multiplier.numerator
    return ReplayResult(
        snapshot=pipeline.health_snapshot(),
        writer=writer,
        producer_elapsed_ns=producer_completed_ns - replay_start_ns,
        recovery_elapsed_ns=recovered_ns - producer_completed_ns,
        scaled_source_span_ns=scaled_source_span_ns,
        max_offer_lateness_ns=max_offer_lateness_ns,
    )


async def _stop_after_unexpected_replay_error(
    pipeline: CaptureBatchPipelineV2,
) -> None:
    try:
        await asyncio.wait_for(
            pipeline.stop(),
            timeout=_DIAGNOSTIC_STOP_WATCHDOG_SECONDS,
        )
    except BaseException:
        pass


async def _sleep_until(due_ns: int) -> None:
    for _attempt in range(8):
        remaining_ns = due_ns - time.monotonic_ns()
        if remaining_ns <= 0:
            return
        await asyncio.sleep(remaining_ns / 1_000_000_000)
    raise TimeoutError("monotonic replay pacing did not reach its due time")


async def _wait_for_thread_event(event: threading.Event) -> None:
    deadline = time.monotonic() + 5.0
    while not event.is_set():
        if time.monotonic() >= deadline:
            raise TimeoutError("fault-injection writer did not enter append_many")
        await asyncio.sleep(0.001)


def _actual_order_route_size_line(item: QueuedRawRecordV2) -> bytes:
    source_logical_key = item.record.source_logical_key
    if source_logical_key is None:
        raise AssertionError("recorded source/route evidence was not retained")
    decoded: object = json.loads(source_logical_key)
    if not isinstance(decoded, list) or len(decoded) != 5:
        raise AssertionError("recorded source/route evidence has an invalid shape")
    evidence = [item.ingest_seq, item.encoded_len, *decoded]
    return _compact_json_line(evidence)


def _expected_order_route_size_line(row: TraceRow) -> bytes:
    evidence: list[object] = [
        row.ingest_seq,
        row.encoded_line_bytes,
        *row.source_evidence,
    ]
    return _compact_json_line(evidence)


def _compact_json_line(value: object) -> bytes:
    return (
        json.dumps(value, ensure_ascii=True, separators=(",", ":")) + "\n"
    ).encode("ascii")


def _load_recorded_trace() -> RecordedTrace:
    manifest_value: object = json.loads(_MANIFEST_PATH.read_text(encoding="utf-8"))
    manifest = _require_mapping(manifest_value, "fixture manifest")
    assert manifest["classification"] == "DIAGNOSTIC_PROVISIONAL_NOT_QUALIFICATION"
    assert manifest["contains_raw_market_payload"] is False
    assert manifest["contains_pnl_or_outcomes"] is False
    assert manifest["qualification_evidence"] is False

    source = _require_mapping(manifest["source"], "source")
    derivation = _require_mapping(manifest["derivation"], "derivation")
    fixture = _require_mapping(manifest["fixture"], "fixture")
    evidence = _require_mapping(manifest["evidence"], "evidence")
    replay = _require_mapping(manifest["replay_contract"], "replay_contract")
    gate = _require_mapping(
        manifest["authoritative_qualification_gate"],
        "authoritative_qualification_gate",
    )
    assert source["manifest_sha256"] == (
        "309468cd21c9a3df65ee1f6f2c8198276ca20ebcbc2a6f18deb1d7355b6c944b"
    )
    assert gate["sample"] == (
        "24 complete hours on the actual final 25+25 panel and final codec"
    )
    assert gate["record_drop_count"] == 0
    assert gate["p99_cpu_percent_max"] == 70
    assert gate["p99_queue_fraction_max"] == 0.5
    assert gate["p99_fsync_ms_max"] == 100
    assert gate["sustained_write_throughput_min"] == (
        "2 * p99_one_second_input_bytes"
    )
    assert gate["current_fixture_status"] == "NOT_QUALIFIED"

    fixture_name = _require_string(fixture["file"], "fixture.file")
    compressed = (_FIXTURE_ROOT / fixture_name).read_bytes()
    fixture_sha256 = hashlib.sha256(compressed).hexdigest()
    assert len(compressed) == _require_int(fixture["bytes"], "fixture.bytes")
    assert fixture_sha256 == _require_string(fixture["sha256"], "fixture.sha256")
    raw = zstd.ZstdDecompressor().decompress(compressed)
    source_csv_sha256 = hashlib.sha256(raw).hexdigest()
    assert len(raw) == _require_int(
        fixture["decompressed_bytes"],
        "fixture.decompressed_bytes",
    )
    assert source_csv_sha256 == fixture["decompressed_sha256"] == source["csv_sha256"]
    assert zstd.__version__ == derivation["python_zstandard_version"]
    assert ".".join(map(str, zstd.ZSTD_VERSION)) == derivation["backend_zstd_version"]
    reproduced = zstd.ZstdCompressor(
        level=_require_int(derivation["compression_level"], "compression_level"),
        threads=_require_int(derivation["threads"], "threads"),
        write_checksum=derivation["checksum"] is True,
        write_content_size=derivation["content_size"] is True,
        write_dict_id=derivation["dictionary_id"] is True,
    ).compress(raw)
    assert reproduced == compressed

    columns_value = evidence["columns"]
    if not isinstance(columns_value, list) or not all(
        isinstance(column, str) for column in columns_value
    ):
        raise TypeError("evidence.columns must be a list of strings")
    expected_columns = cast(list[str], columns_value)
    reader = csv.DictReader(io.StringIO(raw.decode("utf-8"), newline=""))
    assert reader.fieldnames == expected_columns
    rows = tuple(TraceRow.from_csv(row) for row in reader)
    _validate_trace_rows(rows, evidence)

    multiplier_documents = replay["multipliers"]
    if not isinstance(multiplier_documents, list):
        raise TypeError("replay_contract.multipliers must be a list")
    actual_multipliers = tuple(
        (
            Fraction(_require_int(document["value"], "multiplier.value"), 1),
            _require_string(document["label"], "multiplier.label"),
        )
        for document_value in multiplier_documents
        for document in [_require_mapping(document_value, "multiplier")]
    )
    assert actual_multipliers == _PROVISIONAL_MULTIPLIERS

    return RecordedTrace(
        rows=rows,
        source_csv_sha256=source_csv_sha256,
        fixture_sha256=fixture_sha256,
        order_route_size_sha256=_require_string(
            evidence["order_route_size_sha256"],
            "evidence.order_route_size_sha256",
        ),
        session_id=_require_string(source["session_id"], "source.session_id"),
        capture_plan_sha256=_require_string(
            source["capture_plan_sha256"],
            "source.capture_plan_sha256",
        ),
        encoded_line_bytes_sum=_require_int(
            evidence["encoded_line_bytes_sum"],
            "evidence.encoded_line_bytes_sum",
        ),
        receipt_span_monotonic_ns=_require_int(
            evidence["receipt_span_monotonic_ns"],
            "evidence.receipt_span_monotonic_ns",
        ),
        queue_max_events=_require_int(
            replay["queue_max_events"],
            "replay_contract.queue_max_events",
        ),
        queue_max_encoded_bytes=_require_int(
            replay["queue_max_encoded_bytes"],
            "replay_contract.queue_max_encoded_bytes",
        ),
        low_water_events=_require_int(
            replay["low_water_events"],
            "replay_contract.low_water_events",
        ),
        low_water_encoded_bytes=_require_int(
            replay["low_water_encoded_bytes"],
            "replay_contract.low_water_encoded_bytes",
        ),
        qualification_status=_require_string(
            gate["current_fixture_status"],
            "authoritative_qualification_gate.current_fixture_status",
        ),
    )


def _validate_trace_rows(
    rows: tuple[TraceRow, ...],
    evidence: Mapping[str, object],
) -> None:
    assert len(rows) == _require_int(evidence["row_count"], "evidence.row_count")
    assert rows
    first = rows[0]
    last = rows[-1]
    assert first.ingest_seq == evidence["first_ingest_seq"]
    assert last.ingest_seq == evidence["last_ingest_seq"]
    assert first.received_at_ms == evidence["first_received_at_ms"]
    assert last.received_at_ms == evidence["last_received_at_ms"]
    assert first.received_monotonic_ns == evidence["first_received_monotonic_ns"]
    assert last.received_monotonic_ns == evidence["last_received_monotonic_ns"]
    assert first.delta_received_at_ms is None
    assert first.delta_received_monotonic_ns is None

    for left, right in pairwise(rows):
        assert right.ingest_seq == left.ingest_seq + 1
        assert right.received_at_ms >= left.received_at_ms
        assert right.received_monotonic_ns >= left.received_monotonic_ns
        assert right.delta_received_at_ms == (
            right.received_at_ms - left.received_at_ms
        )
        assert right.delta_received_monotonic_ns == (
            right.received_monotonic_ns - left.received_monotonic_ns
        )

    schema_counts = Counter(row.schema_version for row in rows)
    assert dict(sorted(schema_counts.items())) == evidence["schema_counts"]
    assert all(row.source == "binance" for row in rows)
    assert all(row.encoded_line_bytes > 0 for row in rows)
    assert sum(row.encoded_line_bytes for row in rows) == evidence[
        "encoded_line_bytes_sum"
    ]
    assert min(row.encoded_line_bytes for row in rows) == evidence[
        "encoded_line_bytes_min"
    ]
    assert max(row.encoded_line_bytes for row in rows) == evidence[
        "encoded_line_bytes_max"
    ]
    assert last.received_monotonic_ns - first.received_monotonic_ns == evidence[
        "receipt_span_monotonic_ns"
    ]

    order_digest = hashlib.sha256()
    source_route_counts: Counter[tuple[str, str, str, str, str]] = Counter()
    for row in rows:
        order_digest.update(_expected_order_route_size_line(row))
        source_route_counts[row.source_evidence] += 1
    assert order_digest.hexdigest() == evidence["order_route_size_sha256"]
    expected_count_documents = [
        {
            "market": key[0],
            "route": key[1],
            "transport": key[2],
            "source_kind": key[3],
            "role_or_stream": key[4],
            "count": count,
        }
        for key, count in sorted(source_route_counts.items())
    ]
    assert expected_count_documents == evidence["source_route_counts"]


def _require_mapping(value: object, field: str) -> dict[str, object]:
    if not isinstance(value, dict) or not all(isinstance(key, str) for key in value):
        raise TypeError(f"{field} must be a string-keyed object")
    return cast(dict[str, object], value)


def _require_string(value: object, field: str) -> str:
    if not isinstance(value, str):
        raise TypeError(f"{field} must be a string")
    return value


def _require_int(value: object, field: str) -> int:
    if type(value) is not int:
        raise TypeError(f"{field} must be an integer")
    return cast(int, value)
