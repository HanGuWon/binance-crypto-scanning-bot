from __future__ import annotations

import ast
import asyncio
import hashlib
import threading
from collections.abc import AsyncIterator
from pathlib import Path

import pytest

from signalbot.capture.cli import validate_capture_configuration
from signalbot.capture.errors import (
    CaptureQueueOverflow,
    CaptureSerializationError,
    CaptureShortWriteError,
    CaptureStorageCapacityError,
)
from signalbot.capture.handoff import BoundedCaptureHandoff, CaptureFatalState
from signalbot.capture.models import (
    CaptureEnvelopeV1,
    CoverageReason,
    CoverageTransitionV1,
    RestEnvelopeV1,
    payload_bytes,
    record_to_json_line,
)
from signalbot.capture.pipeline import CapturePipeline
from signalbot.capture.plans import build_prospective_capture_plans
from signalbot.capture.websocket import (
    IngestSequencer,
    PublicWebSocketCaptureAdapter,
    ReceiptTimestamp,
    validate_public_websocket_plan,
)
from signalbot.domain.enums import Market
from signalbot.exchange.binance.endpoints import (
    SPOT_WS_COMBINED,
    SPOT_WS_MARKET_DATA_ONLY,
    WebSocketPlan,
    build_websocket_plans,
)

PLAN_SHA256 = hashlib.sha256(b"prospective-capture-pipeline-test").hexdigest()


def _frame(ingest_seq: int, raw_payload: str = "{}") -> CaptureEnvelopeV1:
    return CaptureEnvelopeV1(
        received_at_ms=1_000 + ingest_seq,
        received_monotonic_ns=2_000 + ingest_seq,
        plan_sha256=PLAN_SHA256,
        process_boot_id="boot-1",
        connection_id="connection-1",
        frame_seq=ingest_seq,
        ingest_seq=ingest_seq,
        market=Market.SPOT,
        route="spot",
        stream="btcusdt@aggTrade",
        subscription_streams=("btcusdt@aggTrade",),
        raw_payload=raw_payload,
    )


class RecordingWriter:
    def __init__(self) -> None:
        self.records: list[CaptureEnvelopeV1 | CoverageTransitionV1] = []
        self.events: list[str] = []
        self.emergency: list[CoverageTransitionV1] = []

    def append(self, record, encoded_line: bytes) -> None:
        assert encoded_line == record_to_json_line(record)
        self.records.append(record)
        self.events.append("append")

    def close(self) -> None:
        self.events.append("close")

    def abort(self) -> None:
        self.events.append("abort")

    def write_emergency_transition(self, transition: CoverageTransitionV1) -> None:
        self.emergency.append(transition)
        self.events.append("emergency")


class BlockingWriter(RecordingWriter):
    def __init__(self, *, fail_append: bool = False, fail_close: bool = False) -> None:
        super().__init__()
        self.started = threading.Event()
        self.release = threading.Event()
        self.fail_append = fail_append
        self.fail_close = fail_close

    def append(self, record, encoded_line: bytes) -> None:
        if not self.started.is_set():
            self.started.set()
            if not self.release.wait(timeout=5):
                raise TimeoutError("test writer was not released")
            if self.fail_append:
                raise CaptureShortWriteError("synthetic short write")
        super().append(record, encoded_line)

    def close(self) -> None:
        if self.fail_close:
            raise OSError("synthetic fsync failure")
        super().close()


class FailingWriter(RecordingWriter):
    def append(self, record, encoded_line: bytes) -> None:
        raise CaptureStorageCapacityError("synthetic disk quota")


def _pipeline(
    writer: RecordingWriter,
    *,
    max_events: int = 8,
    max_bytes: int = 64 * 1024,
) -> tuple[CapturePipeline, CaptureFatalState]:
    fatal = CaptureFatalState()
    handoff = BoundedCaptureHandoff(
        max_events=max_events,
        max_bytes=max_bytes,
        fatal_state=fatal,
    )
    return CapturePipeline(handoff, writer), fatal


@pytest.mark.asyncio
async def test_normal_shutdown_orders_producer_drain_then_writer_close() -> None:
    writer = RecordingWriter()
    pipeline, fatal = _pipeline(writer)
    pipeline.start()
    pipeline.offer(_frame(1))
    pipeline.offer(_frame(2))

    await pipeline.stop()

    assert writer.events == ["append", "append", "close"]
    assert [record.ingest_seq for record in writer.records] == [1, 2]
    assert fatal.failed is False


@pytest.mark.asyncio
async def test_writer_failure_trips_shared_stop_before_any_live_integration() -> None:
    writer = FailingWriter()
    pipeline, fatal = _pipeline(writer)
    pipeline.start()
    pipeline.offer(_frame(1))

    await asyncio.wait_for(pipeline.wait_failed(), timeout=2)
    assert fatal.stop_event.is_set()
    assert fatal.failure is not None
    assert fatal.failure.transition is not None
    assert fatal.failure.transition.reason is CoverageReason.STORAGE_CAPACITY
    with pytest.raises(CaptureStorageCapacityError, match="disk quota"):
        await pipeline.stop()
    assert writer.emergency[0].reason is CoverageReason.STORAGE_CAPACITY
    assert "abort" in writer.events


@pytest.mark.asyncio
async def test_queue_overflow_is_nonblocking_fatal_and_persists_invalidation() -> None:
    writer = BlockingWriter()
    pipeline, fatal = _pipeline(writer, max_events=3)
    pipeline.start()
    pipeline.offer(_frame(1))
    assert await asyncio.to_thread(writer.started.wait, 2)
    pipeline.offer(_frame(2))

    with pytest.raises(CaptureQueueOverflow, match="event or encoded-byte"):
        pipeline.offer(_frame(3))
    assert fatal.stop_event.is_set()
    assert fatal.failure is not None
    assert fatal.failure.transition is not None
    assert fatal.failure.transition.reason is CoverageReason.QUEUE_OVERFLOW
    writer.release.set()
    with pytest.raises(CaptureQueueOverflow):
        await pipeline.stop()
    assert isinstance(writer.records[-1], CoverageTransitionV1)
    assert writer.records[-1].reason is CoverageReason.QUEUE_OVERFLOW


def test_encoded_byte_overflow_rejects_boundary_plus_one_without_silent_drop() -> None:
    first = _frame(1)
    fatal = CaptureFatalState()
    handoff = BoundedCaptureHandoff(
        max_events=4,
        max_bytes=len(record_to_json_line(first)),
        fatal_state=fatal,
    )
    handoff.offer(first)

    with pytest.raises(CaptureQueueOverflow):
        handoff.offer(_frame(2))

    assert handoff.queued_events == 1
    assert handoff.queued_bytes == len(record_to_json_line(first))
    assert fatal.stop_event.is_set()


def test_unencodable_text_trips_fatal_instead_of_losing_frame() -> None:
    fatal = CaptureFatalState()
    handoff = BoundedCaptureHandoff(
        max_events=4,
        max_bytes=1024 * 1024,
        fatal_state=fatal,
    )

    with pytest.raises(CaptureSerializationError, match="losslessly"):
        handoff.offer(_frame(1, raw_payload="\ud800"))

    assert fatal.stop_event.is_set()
    assert fatal.failure is not None
    assert fatal.failure.transition is not None
    assert fatal.failure.transition.reason is CoverageReason.SERIALIZATION_ERROR


@pytest.mark.asyncio
async def test_first_failure_wins_over_concurrent_writer_error() -> None:
    writer = BlockingWriter(fail_append=True)
    pipeline, fatal = _pipeline(writer, max_events=3)
    pipeline.start()
    pipeline.offer(_frame(1))
    assert await asyncio.to_thread(writer.started.wait, 2)
    pipeline.offer(_frame(2))
    with pytest.raises(CaptureQueueOverflow):
        pipeline.offer(_frame(3))
    writer.release.set()

    with pytest.raises(CaptureQueueOverflow):
        await pipeline.stop()

    assert fatal.failure is not None
    assert isinstance(fatal.failure.cause, CaptureQueueOverflow)
    assert writer.emergency
    assert writer.emergency[-1].reason is CoverageReason.QUEUE_OVERFLOW


@pytest.mark.asyncio
async def test_fatal_seal_close_error_preserves_original_overflow() -> None:
    writer = BlockingWriter(fail_close=True)
    pipeline, fatal = _pipeline(writer, max_events=3)
    pipeline.start()
    pipeline.offer(_frame(1))
    assert await asyncio.to_thread(writer.started.wait, 2)
    pipeline.offer(_frame(2))
    with pytest.raises(CaptureQueueOverflow):
        pipeline.offer(_frame(3))
    writer.release.set()

    with pytest.raises(CaptureQueueOverflow):
        await pipeline.stop()

    assert fatal.failure is not None
    assert isinstance(fatal.failure.cause, CaptureQueueOverflow)
    assert writer.emergency[-1].reason is CoverageReason.QUEUE_OVERFLOW
    assert "abort" in writer.events


@pytest.mark.asyncio
async def test_normal_close_failure_is_visible_to_waiter_and_stop() -> None:
    writer = BlockingWriter(fail_close=True)
    writer.release.set()
    pipeline, fatal = _pipeline(writer)
    pipeline.start()
    pipeline.offer(_frame(1))

    stop_task = asyncio.create_task(pipeline.stop())
    await asyncio.wait_for(pipeline.wait_failed(), timeout=2)
    with pytest.raises(OSError, match="fsync failure"):
        await stop_task
    assert fatal.stop_event.is_set()
    assert fatal.failure is not None
    assert fatal.failure.transition is not None
    assert fatal.failure.transition.reason is CoverageReason.WRITER_ERROR


class FakeClock:
    def __init__(self) -> None:
        self.calls = 0

    def capture(self) -> ReceiptTimestamp:
        self.calls += 1
        return ReceiptTimestamp(1_710_000_000_123, 987_654_321)


async def _frames(*items: str | bytes) -> AsyncIterator[str | bytes]:
    for item in items:
        yield item


@pytest.mark.asyncio
async def test_receipt_is_sampled_once_and_invalid_json_and_binary_are_preserved() -> None:
    [plan] = build_prospective_capture_plans(("BTCUSDT",), batch_size=50)[:1]
    writer = RecordingWriter()
    pipeline, _fatal = _pipeline(writer)
    clock = FakeClock()
    pipeline.start()
    adapter = PublicWebSocketCaptureAdapter(
        plan,
        plan_sha256=PLAN_SHA256,
        process_boot_id="boot-1",
        connection_id="connection-1",
        pipeline=pipeline,
        clock=clock,
        sequencer=IngestSequencer(),
    )

    deeply_nested = "[" * 10_000 + "]" * 10_000
    await adapter.consume(_frames("not-json", deeply_nested, b"\xff\x00"))
    await pipeline.stop()

    assert clock.calls == 3
    first, nested, binary = writer.records
    assert isinstance(first, CaptureEnvelopeV1)
    assert first.received_at_ms == 1_710_000_000_123
    assert first.received_monotonic_ns == 987_654_321
    assert first.raw_payload == "not-json"
    assert first.stream.startswith("combined:")
    assert isinstance(nested, CaptureEnvelopeV1)
    assert nested.raw_payload == deeply_nested
    assert isinstance(binary, CaptureEnvelopeV1)
    assert payload_bytes(binary.raw_payload, binary.raw_payload_encoding) == b"\xff\x00"


def test_capture_plan_is_distinct_from_scanner_plan_and_public_only() -> None:
    plans = build_prospective_capture_plans(("BTCUSDT", "ETHUSDT"), batch_size=4)
    assert plans
    for plan in plans:
        validate_public_websocket_plan(plan)
        assert "!miniTicker" not in plan.streams
        assert all("kline_1m" not in stream for stream in plan.streams)
    spot_capture = next(plan for plan in plans if plan.market is Market.SPOT)
    assert spot_capture.url.startswith(SPOT_WS_MARKET_DATA_ONLY)
    assert not spot_capture.url.startswith(SPOT_WS_COMBINED)
    scanner_plans = build_websocket_plans(
        Market.SPOT,
        ["BTCUSDT"],
        ["1m", "5m"],
        batch_size=20,
    )
    with pytest.raises(ValueError, match="routed public plan URL"):
        validate_public_websocket_plan(scanner_plans[0])
    with pytest.raises(ValueError, match="non-allowlisted"):
        validate_public_websocket_plan(
            WebSocketPlan(
                name="non-5m-spot-stream",
                market=Market.SPOT,
                route="spot",
                streams=("btcusdt@kline_1m",),
                url=f"{SPOT_WS_MARKET_DATA_ONLY}btcusdt@kline_1m",
            )
        )
    with pytest.raises(ValueError, match="routed public plan URL"):
        validate_public_websocket_plan(
            WebSocketPlan(
                name="wrong-spot-host",
                market=Market.SPOT,
                route="spot",
                streams=("btcusdt@aggTrade",),
                url=f"{SPOT_WS_COMBINED}btcusdt@aggTrade",
            )
        )
    with pytest.raises(ValueError, match="public"):
        validate_public_websocket_plan(
            WebSocketPlan(
                name="private",
                market=Market.FUTURES,
                route="private",
                streams=("btcusdt@aggTrade",),
                url="wss://fstream.binance.com/private/stream?streams=btcusdt@aggTrade",
            )
        )


def test_rest_model_accepts_only_sorted_public_market_data_requests() -> None:
    envelope = RestEnvelopeV1(
        request_started_at_ms=100,
        request_started_monotonic_ns=1_000,
        response_received_at_ms=101,
        response_received_monotonic_ns=1_000,
        plan_sha256=PLAN_SHA256,
        process_boot_id="boot-1",
        request_id="request-1",
        attempt=1,
        ingest_seq=1,
        market=Market.FUTURES,
        endpoint_path="/futures/data/openInterestHist",
        canonical_query=(("period", "5m"), ("symbol", "BTCUSDT")),
        response_status=200,
        raw_payload="[]",
    )
    assert envelope.response_received_monotonic_ns == envelope.request_started_monotonic_ns
    with pytest.raises(ValueError, match="public market-data allowlist"):
        RestEnvelopeV1(
            request_started_at_ms=100,
            request_started_monotonic_ns=1_000,
            response_received_at_ms=101,
            response_received_monotonic_ns=1_001,
            plan_sha256=PLAN_SHA256,
            process_boot_id="boot-1",
            request_id="request-2",
            attempt=1,
            ingest_seq=2,
            market=Market.FUTURES,
            endpoint_path="/fapi/v1/order",
            canonical_query=(),
            response_status=200,
            raw_payload="{}",
        )


def test_capture_package_has_no_private_or_order_execution_imports() -> None:
    capture_root = Path(__file__).resolve().parents[2] / "src" / "signalbot" / "capture"
    forbidden_modules = {
        "signalbot.alerts",
        "signalbot.persistence",
        "signalbot.scanner",
        "signalbot.signals.positions",
    }
    imported: set[str] = set()
    for path in capture_root.glob("*.py"):
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        for node in ast.walk(tree):
            if isinstance(node, ast.ImportFrom) and node.module is not None:
                imported.add(node.module)
            elif isinstance(node, ast.Import):
                imported.update(alias.name for alias in node.names)
    violations = {
        module
        for module in imported
        if any(
            module == forbidden or module.startswith(f"{forbidden}.")
            for forbidden in forbidden_modules
        )
    }
    assert not violations


def test_capture_configuration_command_is_validation_only(tmp_path: Path) -> None:
    plan = tmp_path / "prospective-plan.md"
    plan.write_text("# frozen prospective plan\n", encoding="utf-8")
    output = tmp_path / "not-created-by-validation"

    result = validate_capture_configuration(
        symbols=("BTCUSDT", "ETHUSDT"),
        plan_file=plan,
        output_directory=output,
        batch_size=25,
        queue_max_events=100,
        queue_max_bytes=1024 * 1024,
        maximum_total_bytes=10 * 1024 * 1024,
        emergency_reserve_bytes=1024,
        canary_hours=24,
    )

    assert result["mode"] == "validation_only"
    assert result["network_calls"] is False
    assert result["live_capture_started"] is False
    assert result["order_execution"] is False
    assert not output.exists()
