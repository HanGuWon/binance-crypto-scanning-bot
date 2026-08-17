from __future__ import annotations

import asyncio
import base64
import hashlib
import json
import threading
from collections.abc import Sequence
from dataclasses import replace

import pytest

import signalbot.r4b_v2.capture.batching as batching_module
from signalbot.r4b_v2.capture.authority import StorageRootBindingV2
from signalbot.r4b_v2.capture.batching import (
    BatchDrainerV2,
    BatchPolicyV2,
    BatchTerminalV2,
    BoundedBatchHandoffV2,
    CaptureBatchAckErrorV2,
    CaptureBatchClockErrorV2,
    CaptureBatchClosedV2,
    CaptureBatchIntegrityErrorV2,
    CaptureBatchOverflowV2,
    CaptureBatchSequenceErrorV2,
    CaptureBatchV2,
    CaptureFinalityFenceErrorV2,
    CaptureFinalityFenceRequestV2,
    CaptureQueueAdmissionReceiptV2,
    QueuedRawRecordV2,
    validate_capture_queue_admission_receipt_v2,
)
from signalbot.r4b_v2.capture.models import (
    RawRecordV2,
    TransportV2,
    VenueV2,
    derive_raw_payload_hash,
)
from signalbot.r4b_v2.capture.pipeline import (
    CaptureBatchPipelineV2,
    CaptureFinalityFenceReceiptV2,
    CaptureFinalityFenceTimeoutV2,
)
from signalbot.r4b_v2.capture.telemetry import RejectionBoundV2
from signalbot.r4b_v2.capture.wal import WalDurabilityBindingV2

PROTOCOL_HASH = hashlib.sha256(b"r4b-v2-batching-test").hexdigest()
_FAKE_AUTHORITY_SHA256 = "a" * 64


def _fake_finality_receipt(
    requested_ingest_seq: int,
    fence_ingest_seq: int,
    fence_monotonic_ns: int,
) -> CaptureFinalityFenceReceiptV2:
    wal_root = StorageRootBindingV2(
        storage_kind="WAL",
        root_role="PROVISIONAL_SINGLE",
        failure_domain_id="unit-test-wal",
        authority_sha256=_FAKE_AUTHORITY_SHA256,
        contract_sha256="b" * 64,
    )
    return CaptureFinalityFenceReceiptV2(
        authority_sha256=_FAKE_AUTHORITY_SHA256,
        attempt_id="unit-test-attempt",
        qualification_id="unit-test-policy",
        requested_ingest_seq=requested_ingest_seq,
        fence_ingest_seq=fence_ingest_seq,
        fence_monotonic_ns=fence_monotonic_ns,
        writer_observed_monotonic_ns=fence_monotonic_ns,
        wal_durable_ack_seq=fence_ingest_seq,
        finalized_block_tail_ingest_seq=fence_ingest_seq,
        durable_record_count=fence_ingest_seq,
        exact_prefix_sha256="c" * 64,
        wal_durability_binding=WalDurabilityBindingV2(
            mode="SINGLE_ROOT",
            root_bindings=(wal_root,),
            qualification_selection_receipt_sha256=None,
            physical_failure_domain_independence_verified=False,
        ),
        grouped_block_root_binding=StorageRootBindingV2(
            storage_kind="GROUPED_BLOCK",
            root_role="PROVISIONAL_SINGLE",
            failure_domain_id="unit-test-block",
            authority_sha256=_FAKE_AUTHORITY_SHA256,
            contract_sha256="d" * 64,
        ),
        block_signing_authority_sha256="e" * 64,
        final_block_sequence=1,
        final_block_hash="f" * 64,
        final_block_manifest_sha256="1" * 64,
        final_block_container_sha256="2" * 64,
        target_last_receipt_wall_ms=1_700_000_000_000 + fence_ingest_seq,
        target_last_receipt_monotonic_ns=1_000,
        stream_group_id="unit-test-stream-group",
        segment_id="unit-test-segment",
    )


class ManualClock:
    def __init__(self, value: int = 10_000) -> None:
        self.value = value

    def __call__(self) -> int:
        return self.value

    def advance(self, nanoseconds: int) -> None:
        self.value += nanoseconds


class ScriptClock:
    def __init__(self, values: list[int]) -> None:
        self.values = values
        self.last = values[-1]

    def __call__(self) -> int:
        if self.values:
            self.last = self.values.pop(0)
        return self.last


def _record(
    ingest_seq: int,
    *,
    raw_payload: str | bytes = "{}",
    receipt_monotonic_ns: int = 1_000,
) -> RawRecordV2:
    return RawRecordV2.from_payload(
        session_id="session-v2-test",
        plan_id="plan-v2-test",
        protocol_hash=PROTOCOL_HASH,
        transport=TransportV2.WEBSOCKET,
        venue=VenueV2.USDM_FUTURES,
        route_id="usdm_public",
        symbol="BTCUSDT",
        connection_id="connection-v2-test",
        generation=1,
        frame_seq=ingest_seq,
        ingest_seq=ingest_seq,
        receipt_wall_ms=1_700_000_000_000 + ingest_seq,
        receipt_monotonic_ns=receipt_monotonic_ns,
        raw_payload=raw_payload,
        source_logical_key=f"depth:{ingest_seq}",
    )


def _policy(
    *,
    max_records: int = 2,
    max_encoded_bytes: int = 64 * 1024,
    max_linger_us: int = 0,
    queue_max_events: int = 8,
    queue_max_encoded_bytes: int = 512 * 1024,
) -> BatchPolicyV2:
    return BatchPolicyV2(
        max_records=max_records,
        max_encoded_bytes=max_encoded_bytes,
        max_linger_us=max_linger_us,
        queue_max_events=queue_max_events,
        queue_max_encoded_bytes=queue_max_encoded_bytes,
        low_water_events=0,
        low_water_encoded_bytes=0,
        qualification_id="unit-test-policy",
    )


def _handoff(
    policy: BatchPolicyV2 | None = None,
    *,
    clock: ManualClock | None = None,
) -> BoundedBatchHandoffV2:
    return BoundedBatchHandoffV2(
        policy or _policy(),
        monotonic_ns=clock or ManualClock(),
    )


def test_raw_record_retains_all_source_bytes_as_base64_without_stored_raw_hash() -> None:
    text = _record(1, raw_payload='{"price":"1"}')
    binary = _record(2, raw_payload=b"\xff\x00")

    assert text.payload_bytes() == b'{"price":"1"}'
    assert binary.payload_bytes() == b"\xff\x00"
    assert text.raw_len == len(text.payload_bytes())
    assert text.raw_encoding.value == binary.raw_encoding.value == "base64"
    assert text.raw_payload == base64.b64encode(b'{"price":"1"}').decode("ascii")
    assert binary.raw_payload == "/wA="
    persisted = json.loads(QueuedRawRecordV2.encode(text).encoded_line)
    assert "raw_sha256" not in persisted

    with pytest.raises(ValueError, match="raw_len"):
        replace(text, raw_len=text.raw_len + 1)


def test_raw_payload_hash_is_domain_separated_and_derived_only_on_demand() -> None:
    record = _record(1, raw_payload=b"\xff\x00")

    assert derive_raw_payload_hash("depth:BTCUSDT", b"\xff\x00") == (
        "d2b8a9938bf943fb776a601306431e9125d7ed39b49b653e919beb28183649e3"
    )
    assert record.derive_raw_payload_hash("depth:BTCUSDT") == derive_raw_payload_hash(
        b"depth:BTCUSDT", b"\xff\x00"
    )
    assert record.derive_raw_payload_hash("other-stream") != record.derive_raw_payload_hash(
        "depth:BTCUSDT"
    )


def test_queue_item_serializes_once_and_rejects_tampered_encoded_bytes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls = 0
    original = batching_module.canonical_json_line

    def counted(value: object) -> bytes:
        nonlocal calls
        calls += 1
        return original(value)

    monkeypatch.setattr(batching_module, "canonical_json_line", counted)
    handoff = _handoff()
    item = handoff.offer(_record(1))

    assert calls == 1
    item.verify_integrity()
    assert calls == 1
    with pytest.raises(CaptureBatchIntegrityErrorV2, match="encoded_len"):
        replace(item, encoded_line=item.encoded_line + b" ")
    handoff.discard_all()


def test_queue_admission_receipt_is_factory_owned_and_handoff_bound() -> None:
    admitted = _handoff()
    other = _handoff()

    receipt = admitted.offer_with_admission_receipt(_record(1))

    assert type(receipt) is CaptureQueueAdmissionReceiptV2
    assert receipt.record is receipt.queued_record.record
    assert receipt.accepted_tail_ingest_seq == 1
    assert admitted.accepted_tail_ingest_seq == 1
    assert validate_capture_queue_admission_receipt_v2(receipt) is receipt.queued_record
    assert admitted.validate_queue_admission_receipt_v2(receipt) is receipt.queued_record
    with pytest.raises(ValueError, match="different bounded handoff"):
        other.validate_queue_admission_receipt_v2(receipt)
    with pytest.raises(TypeError, match="only be created by its bounded handoff"):
        CaptureQueueAdmissionReceiptV2(
            queued_record=receipt.queued_record,
            accepted_tail_ingest_seq=1,
            _handoff=admitted,
            _handoff_seal=object(),
            _factory_token=object(),
        )
    with pytest.raises(TypeError, match="only be created by its bounded handoff"):
        replace(receipt)

    admitted.discard_all()
    other.discard_all()


@pytest.mark.asyncio
async def test_drainer_preserves_order_and_exact_record_and_byte_boundaries() -> None:
    records = (_record(1), _record(2), _record(3))
    encoded = tuple(
        QueuedRawRecordV2.encode(record, enqueued_monotonic_ns=10_000) for record in records
    )
    exact_two_bytes = encoded[0].encoded_len + encoded[1].encoded_len
    handoff = _handoff(
        _policy(
            max_records=3,
            max_encoded_bytes=exact_two_bytes,
            queue_max_events=3,
            queue_max_encoded_bytes=sum(item.encoded_len for item in encoded),
        )
    )
    for record in records:
        handoff.offer(record)
    handoff.stop_producer()
    drainer = BatchDrainerV2(handoff)

    first = await drainer.next_batch()
    assert [item.ingest_seq for item in first.records] == [1, 2]
    assert first.encoded_bytes == exact_two_bytes
    handoff.acknowledge_records(
        first,
        durable_ack_seq=2,
        completed_monotonic_ns=10_000,
        writer_latency_ns=0,
    )

    second = await drainer.next_batch()
    assert [item.ingest_seq for item in second.records] == [3]
    assert second.terminal is None
    handoff.acknowledge_records(
        second,
        durable_ack_seq=3,
        completed_monotonic_ns=10_000,
        writer_latency_ns=0,
    )

    terminal = await drainer.next_batch()
    assert terminal.records == ()
    assert terminal.terminal is BatchTerminalV2.STOP
    handoff.complete_terminal(terminal)
    await handoff.join()


@pytest.mark.asyncio
async def test_cancelled_linger_restores_the_exact_dequeued_prefix() -> None:
    handoff = _handoff(_policy(max_linger_us=1_000_000))
    handoff.offer(_record(1))
    drainer = BatchDrainerV2(handoff)
    pending = asyncio.create_task(drainer.next_batch())
    await asyncio.sleep(0)
    pending.cancel()
    with pytest.raises(asyncio.CancelledError):
        await pending

    handoff.stop_producer()
    recovered = await drainer.next_batch()
    assert [item.ingest_seq for item in recovered.records] == [1]
    assert recovered.terminal is None
    handoff.acknowledge_records(
        recovered,
        durable_ack_seq=1,
        completed_monotonic_ns=10_000,
        writer_latency_ns=0,
    )

    terminal = await drainer.next_batch()
    assert terminal.records == ()
    assert terminal.terminal is BatchTerminalV2.STOP
    handoff.complete_terminal(terminal)
    await handoff.join()


@pytest.mark.asyncio
async def test_linger_equality_closes_batch_before_an_already_queued_next_record() -> None:
    clock = ScriptClock([10_000, 10_000, 10_000, 11_000, 11_000, 11_000])
    handoff = BoundedBatchHandoffV2(
        _policy(max_records=2, max_linger_us=1),
        monotonic_ns=clock,
    )
    handoff.offer(_record(1))
    handoff.offer(_record(2))
    handoff.stop_producer()
    drainer = BatchDrainerV2(handoff)

    first = await drainer.next_batch()
    assert [item.ingest_seq for item in first.records] == [1]
    handoff.acknowledge_records(
        first,
        durable_ack_seq=1,
        completed_monotonic_ns=11_000,
        writer_latency_ns=0,
    )
    second = await drainer.next_batch()
    assert [item.ingest_seq for item in second.records] == [2]
    assert second.terminal is None
    handoff.acknowledge_records(
        second,
        durable_ack_seq=2,
        completed_monotonic_ns=11_000,
        writer_latency_ns=0,
    )

    terminal = await drainer.next_batch()
    assert terminal.records == ()
    assert terminal.terminal is BatchTerminalV2.STOP
    handoff.complete_terminal(terminal)
    await handoff.join()


def test_capture_batch_has_exactly_one_payload_kind_and_fence_is_exact() -> None:
    item = QueuedRawRecordV2.encode(_record(1), enqueued_monotonic_ns=10_000)
    fence = CaptureFinalityFenceRequestV2(
        requested_ingest_seq=1,
        fence_ingest_seq=1,
        fence_monotonic_ns=10_000,
    )

    fence_batch = CaptureBatchV2(
        records=(),
        terminal=None,
        dequeued_monotonic_ns=10_000,
        linger_ns=0,
        finality_fence=fence,
    )
    assert fence_batch.finality_fence is fence

    invalid_payloads = (
        ((), None, None),
        ((item,), BatchTerminalV2.STOP, None),
        ((item,), None, fence),
        ((), BatchTerminalV2.STOP, fence),
    )
    for records, terminal, finality_fence in invalid_payloads:
        with pytest.raises(ValueError, match="exactly one"):
            CaptureBatchV2(
                records=records,
                terminal=terminal,
                dequeued_monotonic_ns=10_000,
                linger_ns=0,
                finality_fence=finality_fence,
            )

    with pytest.raises(ValueError, match="must equal"):
        CaptureFinalityFenceRequestV2(
            requested_ingest_seq=1,
            fence_ingest_seq=2,
            fence_monotonic_ns=10_000,
        )
    with pytest.raises(ValueError, match="positive integer"):
        CaptureFinalityFenceRequestV2(
            requested_ingest_seq=0,
            fence_ingest_seq=0,
            fence_monotonic_ns=10_000,
        )


@pytest.mark.asyncio
async def test_ordered_finality_fence_splits_batches_and_returns_result() -> None:
    clock = ManualClock()
    handoff = _handoff(_policy(max_records=2), clock=clock)
    handoff.offer(_record(1))
    fence_future = handoff.offer_finality_fence(1)
    handoff.offer(_record(2))
    handoff.stop_producer()
    drainer = BatchDrainerV2(handoff)

    prefix = await drainer.next_batch()
    assert [item.ingest_seq for item in prefix.records] == [1]
    assert prefix.finality_fence is None
    handoff.acknowledge_records(
        prefix,
        durable_ack_seq=1,
        completed_monotonic_ns=clock.value,
        writer_latency_ns=0,
    )

    fence_batch = await drainer.next_batch()
    assert fence_batch.records == ()
    assert fence_batch.terminal is None
    assert fence_batch.finality_fence == CaptureFinalityFenceRequestV2(
        requested_ingest_seq=1,
        fence_ingest_seq=1,
        fence_monotonic_ns=clock.value,
    )
    assert not fence_future.done()
    result = object()
    handoff.complete_finality_fence(fence_batch, result=result)
    assert await fence_future is result
    assert not handoff.finality_fence_in_flight

    suffix = await drainer.next_batch()
    assert [item.ingest_seq for item in suffix.records] == [2]
    handoff.acknowledge_records(
        suffix,
        durable_ack_seq=2,
        completed_monotonic_ns=clock.value,
        writer_latency_ns=0,
    )
    terminal = await drainer.next_batch()
    assert terminal.terminal is BatchTerminalV2.STOP
    handoff.complete_terminal(terminal)
    await handoff.join()


@pytest.mark.asyncio
async def test_clean_tail_shutdown_atomically_fences_then_stops_once() -> None:
    clock = ManualClock()
    handoff = _handoff(
        _policy(max_records=2, queue_max_events=2),
        clock=clock,
    )
    handoff.offer(_record(1))
    handoff.offer(_record(2))

    fence_future = handoff.begin_clean_tail_shutdown()

    request = handoff.clean_tail_shutdown_request
    assert request == CaptureFinalityFenceRequestV2(
        requested_ingest_seq=2,
        fence_ingest_seq=2,
        fence_monotonic_ns=clock.value,
    )
    assert handoff.accepted_tail_ingest_seq == 2
    assert not handoff.accepting
    with pytest.raises(CaptureBatchClosedV2, match="stopped"):
        handoff.offer(_record(3))
    with pytest.raises(CaptureFinalityFenceErrorV2, match="only once"):
        handoff.begin_clean_tail_shutdown()

    drainer = BatchDrainerV2(handoff)
    records = await drainer.next_batch()
    assert tuple(item.ingest_seq for item in records.records) == (1, 2)
    handoff.acknowledge_records(
        records,
        durable_ack_seq=2,
        completed_monotonic_ns=clock.value,
        writer_latency_ns=0,
    )
    fence = await drainer.next_batch()
    assert fence.finality_fence is request
    receipt = _fake_finality_receipt(2, 2, clock.value)
    handoff.complete_finality_fence(fence, result=receipt)
    assert await fence_future is receipt
    terminal = await drainer.next_batch()
    assert terminal.terminal is BatchTerminalV2.STOP
    handoff.complete_terminal(terminal)
    assert request is not None
    handoff.assert_clean_stopped_current_tail_v2(request)
    await handoff.join()


@pytest.mark.asyncio
async def test_empty_clean_tail_shutdown_fails_closed() -> None:
    handoff = _handoff()

    with pytest.raises(CaptureFinalityFenceErrorV2, match="positive accepted") as caught:
        handoff.begin_clean_tail_shutdown()

    assert not handoff.accepting
    assert handoff.fatal_state.failure is not None
    assert handoff.fatal_state.failure.cause is caught.value
    with pytest.raises(CaptureFinalityFenceErrorV2, match="positive accepted"):
        handoff.fatal_state.raise_if_failed()
    handoff.discard_all()


@pytest.mark.asyncio
async def test_finality_fence_and_terminal_fit_beyond_logical_record_bound() -> None:
    handoff = _handoff(_policy(max_records=2, queue_max_events=2))
    handoff.offer(_record(1))
    handoff.offer(_record(2))
    fence_future = handoff.offer_finality_fence(2)
    handoff.stop_producer()

    assert handoff.current_events == 2
    assert handoff.finality_fence_in_flight
    drainer = BatchDrainerV2(handoff)
    records = await drainer.next_batch()
    assert [item.ingest_seq for item in records.records] == [1, 2]
    handoff.acknowledge_records(
        records,
        durable_ack_seq=2,
        completed_monotonic_ns=10_000,
        writer_latency_ns=0,
    )
    fence_batch = await drainer.next_batch()
    handoff.complete_finality_fence(fence_batch, result="durable-through-2")
    assert await fence_future == "durable-through-2"
    terminal = await drainer.next_batch()
    assert terminal.terminal is BatchTerminalV2.STOP
    handoff.complete_terminal(terminal)
    await handoff.join()
    assert handoff.current_events == 0


@pytest.mark.asyncio
async def test_only_one_finality_fence_can_be_in_flight() -> None:
    handoff = _handoff()
    handoff.offer(_record(1))
    first_future = handoff.offer_finality_fence(1)
    with pytest.raises(CaptureFinalityFenceErrorV2, match="only one"):
        handoff.offer_finality_fence(1)

    drainer = BatchDrainerV2(handoff)
    records = await drainer.next_batch()
    handoff.acknowledge_records(
        records,
        durable_ack_seq=1,
        completed_monotonic_ns=10_000,
        writer_latency_ns=0,
    )
    first_batch = await drainer.next_batch()
    handoff.complete_finality_fence(first_batch, result="first")
    assert await first_future == "first"

    second_future = handoff.offer_finality_fence(1)
    handoff.stop_producer()
    second_batch = await drainer.next_batch()
    handoff.complete_finality_fence(second_batch, result="second")
    assert await second_future == "second"
    terminal = await drainer.next_batch()
    handoff.complete_terminal(terminal)
    await handoff.join()


@pytest.mark.asyncio
async def test_finality_fence_rejects_stale_future_and_invalid_prefixes() -> None:
    handoff = _handoff()

    with pytest.raises(ValueError, match="positive integer"):
        handoff.offer_finality_fence(0)
    handoff.offer(_record(1))
    handoff.offer(_record(2))

    for requested_ingest_seq in (1, 3):
        with pytest.raises(CaptureFinalityFenceErrorV2, match="current accepted"):
            handoff.offer_finality_fence(requested_ingest_seq)
    for requested_ingest_seq in (-1, True):
        with pytest.raises(ValueError, match="positive integer"):
            handoff.offer_finality_fence(requested_ingest_seq)

    assert not handoff.finality_fence_in_flight
    handoff.discard_all()
    await handoff.join()


@pytest.mark.asyncio
async def test_fatal_failure_reaches_queued_fence_future_and_discard_joins() -> None:
    handoff = _handoff()
    handoff.offer(_record(1))
    fence_future = handoff.offer_finality_fence(1)
    original = OSError("synthetic failure after finality request")

    handoff.fail_consumer(original, failing_ingest_seq=1)
    with pytest.raises(OSError) as raised:
        await fence_future
    assert raised.value is original

    handoff.discard_all()
    await handoff.join()
    assert handoff.current_events == 0
    assert not handoff.finality_fence_in_flight
    assert handoff.snapshot().discarded_events == 1


@pytest.mark.asyncio
async def test_active_fence_discard_preserves_original_fatal_and_task_accounting() -> None:
    handoff = _handoff()
    handoff.offer(_record(1))
    fence_future = handoff.offer_finality_fence(1)
    drainer = BatchDrainerV2(handoff)
    records = await drainer.next_batch()
    handoff.acknowledge_records(
        records,
        durable_ack_seq=1,
        completed_monotonic_ns=10_000,
        writer_latency_ns=0,
    )
    active_fence = await drainer.next_batch()
    original = RuntimeError("synthetic active-fence failure")

    handoff.fail_consumer(original, failing_ingest_seq=None)
    handoff.discard_all(active_batch=active_fence)
    with pytest.raises(RuntimeError) as raised:
        await fence_future
    assert raised.value is original
    await handoff.join()
    assert not handoff.finality_fence_in_flight


@pytest.mark.asyncio
async def test_nonfatal_discard_closes_fence_future_and_releases_queue_item() -> None:
    handoff = _handoff()
    handoff.offer(_record(1))
    fence_future = handoff.offer_finality_fence(1)

    handoff.discard_all()
    with pytest.raises(CaptureBatchClosedV2, match="discarded"):
        await fence_future
    await handoff.join()
    assert not handoff.finality_fence_in_flight


def test_event_bound_equality_accepts_and_plus_one_fails_with_fatal_snapshot() -> None:
    handoff = _handoff(_policy(queue_max_events=2))
    handoff.offer(_record(1))
    handoff.offer(_record(2))

    with pytest.raises(CaptureBatchOverflowV2) as raised:
        handoff.offer(_record(3))

    assert raised.value.bound is RejectionBoundV2.EVENTS
    failure = handoff.fatal_state.failure
    assert failure is not None
    assert failure.failing_ingest_seq == 3
    assert failure.rejection_bound is RejectionBoundV2.EVENTS
    assert failure.fatal_snapshot.current_events == 2
    assert failure.fatal_snapshot.peak_events == 2
    handoff.discard_all()


def test_encoded_byte_bound_equality_accepts_and_plus_one_fails() -> None:
    first = QueuedRawRecordV2.encode(_record(1), enqueued_monotonic_ns=10_000)
    second = QueuedRawRecordV2.encode(_record(2), enqueued_monotonic_ns=10_000)
    assert first.encoded_len == second.encoded_len
    handoff = _handoff(
        _policy(
            max_encoded_bytes=first.encoded_len,
            queue_max_events=2,
            queue_max_encoded_bytes=first.encoded_len,
        )
    )
    handoff.offer(_record(1))

    with pytest.raises(CaptureBatchOverflowV2) as raised:
        handoff.offer(_record(2))

    assert raised.value.bound is RejectionBoundV2.ENCODED_BYTES
    assert handoff.current_encoded_bytes == first.encoded_len
    handoff.discard_all()


def test_single_record_above_batch_byte_bound_is_rejected_before_enqueue() -> None:
    item = QueuedRawRecordV2.encode(_record(1), enqueued_monotonic_ns=10_000)
    handoff = _handoff(
        _policy(
            max_encoded_bytes=item.encoded_len - 1,
            queue_max_encoded_bytes=item.encoded_len * 2,
        )
    )

    with pytest.raises(CaptureBatchOverflowV2) as raised:
        handoff.offer(_record(1))

    assert raised.value.bound is RejectionBoundV2.BATCH_ENCODED_BYTES
    failure = handoff.fatal_state.failure
    assert failure is not None
    assert failure.fatal_snapshot.enqueued_events == 0
    handoff.discard_all()


def test_ingest_gap_is_first_failure_and_never_enters_the_queue() -> None:
    handoff = _handoff()

    with pytest.raises(CaptureBatchSequenceErrorV2, match="expected sequence"):
        handoff.offer(_record(2))

    failure = handoff.fatal_state.failure
    assert failure is not None
    assert failure.failing_ingest_seq == 2
    assert failure.rejection_bound is RejectionBoundV2.INGEST_SEQUENCE
    assert failure.fatal_snapshot.enqueued_events == 0
    handoff.discard_all()


def test_durable_ack_cannot_skip_the_dequeue_boundary() -> None:
    handoff = _handoff()
    item = handoff.offer(_record(1))
    fabricated = CaptureBatchV2(
        records=(item,),
        terminal=None,
        dequeued_monotonic_ns=10_000,
        linger_ns=0,
    )

    with pytest.raises(CaptureBatchAckErrorV2, match="active dequeued batch"):
        handoff.acknowledge_records(
            fabricated,
            durable_ack_seq=1,
            completed_monotonic_ns=10_000,
            writer_latency_ns=0,
        )

    assert handoff.snapshot().durable_acked_events == 0
    handoff.discard_all()


def test_monotonic_clock_reversal_fails_the_offer_and_preserves_pending_prefix() -> None:
    clock = ScriptClock([10_000, 9_999])
    handoff = BoundedBatchHandoffV2(_policy(), monotonic_ns=clock)
    handoff.offer(_record(1))

    with pytest.raises(CaptureBatchClockErrorV2, match="moved backwards"):
        handoff.offer(_record(2))

    failure = handoff.fatal_state.failure
    assert failure is not None
    assert failure.rejection_bound is RejectionBoundV2.MONOTONIC_CLOCK
    assert failure.fatal_snapshot.current_events == 1
    assert failure.fatal_snapshot.snapshot_monotonic_ns == 10_000
    handoff.discard_all()


class RecordingBatchWriter:
    def __init__(self, *, ack_delta: int = 0) -> None:
        self.ack_delta = ack_delta
        self.batches: list[tuple[int, ...]] = []
        self.thread_ids: list[int] = []
        self.closed = False
        self.aborted = False
        self.close_thread_id: int | None = None
        self.finality_calls: list[tuple[int, int, int]] = []
        self.finality_thread_ids: list[int] = []

    def append_many(self, records: Sequence[QueuedRawRecordV2]) -> int:
        for record in records:
            record.verify_integrity()
        self.batches.append(tuple(record.ingest_seq for record in records))
        self.thread_ids.append(threading.get_ident())
        return records[-1].ingest_seq + self.ack_delta

    def finalize_through(
        self,
        *,
        requested_ingest_seq: int,
        fence_ingest_seq: int,
        fence_monotonic_ns: int,
    ) -> CaptureFinalityFenceReceiptV2:
        self.finality_calls.append((requested_ingest_seq, fence_ingest_seq, fence_monotonic_ns))
        self.finality_thread_ids.append(threading.get_ident())
        return _fake_finality_receipt(
            requested_ingest_seq,
            fence_ingest_seq,
            fence_monotonic_ns,
        )

    def close(self) -> None:
        self.close_thread_id = threading.get_ident()
        self.closed = True

    def abort(self) -> None:
        self.aborted = True


class FailingBatchWriter(RecordingBatchWriter):
    def append_many(self, records: Sequence[QueuedRawRecordV2]) -> int:
        del records
        raise OSError("synthetic V2 batch write failure")


class CloseFailingBatchWriter(RecordingBatchWriter):
    def close(self) -> None:
        raise OSError("synthetic V2 close failure")


class BlockingFailingBatchWriter(RecordingBatchWriter):
    def __init__(self) -> None:
        super().__init__()
        self.started = threading.Event()
        self.release = threading.Event()

    def append_many(self, records: Sequence[QueuedRawRecordV2]) -> int:
        del records
        self.started.set()
        if not self.release.wait(timeout=5):
            raise TimeoutError("test batch writer was not released")
        raise OSError("later writer failure")


class FailingFinalityWriter(RecordingBatchWriter):
    def finalize_through(
        self,
        *,
        requested_ingest_seq: int,
        fence_ingest_seq: int,
        fence_monotonic_ns: int,
    ) -> CaptureFinalityFenceReceiptV2:
        del requested_ingest_seq, fence_ingest_seq, fence_monotonic_ns
        raise OSError("synthetic V2 finality failure")


class BlockingFinalityWriter(RecordingBatchWriter):
    def __init__(self) -> None:
        super().__init__()
        self.finality_started = threading.Event()
        self.finality_release = threading.Event()

    def finalize_through(
        self,
        *,
        requested_ingest_seq: int,
        fence_ingest_seq: int,
        fence_monotonic_ns: int,
    ) -> CaptureFinalityFenceReceiptV2:
        self.finality_started.set()
        if not self.finality_release.wait(timeout=5):
            raise TimeoutError("test finality writer was not released")
        return super().finalize_through(
            requested_ingest_seq=requested_ingest_seq,
            fence_ingest_seq=fence_ingest_seq,
            fence_monotonic_ns=fence_monotonic_ns,
        )


class BlockingCloseWriter(RecordingBatchWriter):
    def __init__(self) -> None:
        super().__init__()
        self.close_started = threading.Event()
        self.close_release = threading.Event()

    def close(self) -> None:
        self.close_started.set()
        if not self.close_release.wait(timeout=5):
            raise TimeoutError("test close writer was not released")
        super().close()


@pytest.mark.asyncio
async def test_pipeline_uses_one_dedicated_worker_crossing_per_ordered_batch() -> None:
    handoff = _handoff(_policy(max_records=2))
    writer = RecordingBatchWriter()
    pipeline = CaptureBatchPipelineV2(handoff, writer)
    pipeline.start()
    for ingest_seq in range(1, 6):
        pipeline.offer(_record(ingest_seq))

    await pipeline.stop()

    assert writer.batches == [(1, 2), (3, 4), (5,)]
    assert len(set(writer.thread_ids)) == 1
    assert writer.close_thread_id == writer.thread_ids[0]
    assert writer.closed and not writer.aborted
    snapshot = pipeline.health_snapshot()
    assert snapshot.worker_crossings == 3
    assert snapshot.batches_completed == 3
    assert snapshot.durable_ack_seq == 5
    assert snapshot.current_events == 0


@pytest.mark.asyncio
async def test_pipeline_forwards_its_exact_handoff_admission_receipt() -> None:
    handoff = _handoff()
    writer = RecordingBatchWriter()
    pipeline = CaptureBatchPipelineV2(handoff, writer)
    pipeline.start()

    receipt = pipeline.offer_with_admission_receipt(_record(1))

    assert receipt.accepted_tail_ingest_seq == handoff.accepted_tail_ingest_seq == 1
    assert pipeline.validate_queue_admission_receipt_v2(receipt) is receipt.queued_record
    with pytest.raises(ValueError, match="different bounded handoff"):
        _handoff().validate_queue_admission_receipt_v2(receipt)
    await pipeline.stop()

    assert writer.batches == [(1,)]


@pytest.mark.asyncio
async def test_pipeline_finality_fence_uses_the_same_ordered_writer_thread() -> None:
    clock = ManualClock()
    handoff = _handoff(_policy(max_records=2), clock=clock)
    writer = RecordingBatchWriter()
    pipeline = CaptureBatchPipelineV2(handoff, writer)
    pipeline.start()
    pipeline.offer(_record(1))

    receipt = await pipeline.finalize_through(1, timeout_seconds=2)
    await pipeline.stop()

    assert writer.batches == [(1,)]
    assert writer.finality_calls == [(1, 1, 10_000)]
    assert writer.finality_thread_ids == writer.thread_ids
    assert receipt.fence_ingest_seq == 1
    assert receipt.fence_monotonic_ns == 10_000


@pytest.mark.asyncio
async def test_pipeline_clean_tail_api_awaits_finality_and_terminal_close() -> None:
    clock = ManualClock()
    handoff = _handoff(_policy(max_records=2), clock=clock)
    writer = RecordingBatchWriter()
    pipeline = CaptureBatchPipelineV2(handoff, writer)
    pipeline.start()
    pipeline.offer(_record(1))
    pipeline.offer(_record(2))

    receipt = await pipeline.finalize_current_tail_and_stop(timeout_seconds=2)

    assert receipt.fence_ingest_seq == 2
    assert writer.batches == [(1, 2)]
    assert writer.finality_calls == [(2, 2, clock.value)]
    assert writer.closed and not writer.aborted
    assert not handoff.accepting
    assert not handoff.finality_fence_in_flight
    await pipeline.stop()
    with pytest.raises(RuntimeError, match="already started"):
        pipeline.start()


@pytest.mark.asyncio
async def test_clean_tail_timeout_and_cancel_leave_ordered_shutdown_running() -> None:
    for cancel in (False, True):
        handoff = _handoff()
        writer = BlockingFinalityWriter()
        pipeline = CaptureBatchPipelineV2(handoff, writer)
        pipeline.start()
        pipeline.offer(_record(1))

        task = asyncio.create_task(
            pipeline.finalize_current_tail_and_stop(timeout_seconds=2 if cancel else 0.01)
        )
        assert await asyncio.to_thread(writer.finality_started.wait, 2)
        if cancel:
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
        else:
            with pytest.raises(CaptureFinalityFenceTimeoutV2, match="bounded wait"):
                await task
        assert not handoff.accepting
        writer.finality_release.set()
        await pipeline.stop()
        assert writer.closed and not writer.aborted


@pytest.mark.asyncio
async def test_cancel_during_clean_tail_close_keeps_one_owned_stop_task() -> None:
    handoff = _handoff()
    writer = BlockingCloseWriter()
    pipeline = CaptureBatchPipelineV2(handoff, writer)
    pipeline.start()
    pipeline.offer(_record(1))

    finalize = asyncio.create_task(pipeline.finalize_current_tail_and_stop(timeout_seconds=2))
    assert await asyncio.to_thread(writer.close_started.wait, 2)
    finalize.cancel()
    with pytest.raises(asyncio.CancelledError):
        await finalize

    cleanup = asyncio.create_task(pipeline.stop())
    await asyncio.sleep(0)
    assert not cleanup.done()
    writer.close_release.set()
    await cleanup
    await pipeline.stop()

    assert writer.closed and not writer.aborted


@pytest.mark.asyncio
async def test_clean_tail_close_failure_surfaces_original_error() -> None:
    handoff = _handoff()
    writer = CloseFailingBatchWriter()
    pipeline = CaptureBatchPipelineV2(handoff, writer)
    pipeline.start()
    pipeline.offer(_record(1))

    with pytest.raises(OSError, match="synthetic V2 close failure"):
        await pipeline.finalize_current_tail_and_stop(timeout_seconds=2)

    assert writer.aborted


@pytest.mark.asyncio
async def test_clean_tail_finality_failure_preserves_original_fatal() -> None:
    handoff = _handoff()
    writer = FailingFinalityWriter()
    pipeline = CaptureBatchPipelineV2(handoff, writer)
    pipeline.start()
    pipeline.offer(_record(1))

    with pytest.raises(OSError, match="synthetic V2 finality failure") as caught:
        await pipeline.finalize_current_tail_and_stop(timeout_seconds=2)
    with pytest.raises(OSError, match="synthetic V2 finality failure") as stopped:
        await pipeline.stop()

    assert stopped.value is caught.value
    assert writer.aborted


@pytest.mark.asyncio
async def test_finality_failure_is_fatal_and_surfaces_the_original_error() -> None:
    handoff = _handoff()
    writer = FailingFinalityWriter()
    pipeline = CaptureBatchPipelineV2(handoff, writer)
    pipeline.start()
    pipeline.offer(_record(1))

    with pytest.raises(OSError, match="synthetic V2 finality failure"):
        await pipeline.finalize_through(1, timeout_seconds=2)
    with pytest.raises(OSError, match="synthetic V2 finality failure"):
        await pipeline.stop()

    failure = handoff.fatal_state.failure
    assert failure is not None
    assert failure.failing_ingest_seq == 1
    assert writer.aborted


@pytest.mark.asyncio
async def test_finality_timeout_does_not_cancel_the_ordered_fence() -> None:
    handoff = _handoff()
    writer = BlockingFinalityWriter()
    pipeline = CaptureBatchPipelineV2(handoff, writer)
    pipeline.start()
    pipeline.offer(_record(1))

    with pytest.raises(CaptureFinalityFenceTimeoutV2, match="bounded wait"):
        await pipeline.finalize_through(1, timeout_seconds=0.01)
    assert await asyncio.to_thread(writer.finality_started.wait, 2)
    assert handoff.finality_fence_in_flight
    writer.finality_release.set()
    await pipeline.stop()

    assert writer.finality_calls == [(1, 1, 10_000)]
    assert writer.closed and not writer.aborted
    assert not handoff.finality_fence_in_flight


@pytest.mark.asyncio
async def test_finality_api_rejects_nonfinite_or_nonpositive_timeouts() -> None:
    pipeline = CaptureBatchPipelineV2(_handoff(), RecordingBatchWriter())
    pipeline.start()
    pipeline.offer(_record(1))
    for timeout in (0.0, -1.0, float("nan"), float("inf")):
        with pytest.raises(ValueError, match="timeout_seconds"):
            await pipeline.finalize_through(1, timeout_seconds=timeout)
    await pipeline.stop()


@pytest.mark.asyncio
async def test_worker_failure_discards_unacknowledged_tail_and_surfaces_original() -> None:
    handoff = _handoff(_policy(max_records=2))
    writer = FailingBatchWriter()
    pipeline = CaptureBatchPipelineV2(handoff, writer)
    pipeline.start()
    pipeline.offer(_record(1))
    pipeline.offer(_record(2))

    await asyncio.wait_for(pipeline.wait_failed(), timeout=2)
    with pytest.raises(OSError, match="synthetic V2 batch write failure"):
        await pipeline.stop()

    failure = handoff.fatal_state.failure
    assert failure is not None
    assert failure.failing_ingest_seq == 1
    assert failure.fatal_snapshot.current_events == 2
    assert writer.aborted
    snapshot = pipeline.health_snapshot()
    assert snapshot.current_events == 0
    assert snapshot.discarded_events == 2
    assert snapshot.durable_acked_events == 0


@pytest.mark.asyncio
async def test_nonexact_durable_ack_is_fatal_without_silent_fallback() -> None:
    handoff = _handoff(_policy(max_records=2))
    writer = RecordingBatchWriter(ack_delta=-1)
    pipeline = CaptureBatchPipelineV2(handoff, writer)
    pipeline.start()
    pipeline.offer(_record(1))

    with pytest.raises(CaptureBatchAckErrorV2, match="non-exact"):
        await pipeline.stop()

    assert writer.aborted
    assert pipeline.health_snapshot().durable_acked_events == 0


@pytest.mark.asyncio
async def test_close_failure_keeps_durable_prefix_but_fails_the_session() -> None:
    handoff = _handoff(_policy(max_records=2))
    writer = CloseFailingBatchWriter()
    pipeline = CaptureBatchPipelineV2(handoff, writer)
    pipeline.start()
    pipeline.offer(_record(1))

    with pytest.raises(OSError, match="synthetic V2 close failure"):
        await pipeline.stop()

    snapshot = pipeline.health_snapshot()
    assert snapshot.durable_acked_events == 1
    assert snapshot.durable_ack_seq == 1
    assert snapshot.discarded_events == 0
    assert writer.aborted


@pytest.mark.asyncio
async def test_queue_overflow_wins_over_later_batch_writer_failure() -> None:
    handoff = _handoff(_policy(max_records=1, queue_max_events=2))
    writer = BlockingFailingBatchWriter()
    pipeline = CaptureBatchPipelineV2(handoff, writer)
    pipeline.start()
    pipeline.offer(_record(1))
    assert await asyncio.to_thread(writer.started.wait, 2)
    pipeline.offer(_record(2))
    with pytest.raises(CaptureBatchOverflowV2):
        pipeline.offer(_record(3))
    writer.release.set()

    with pytest.raises(CaptureBatchOverflowV2):
        await pipeline.stop()

    failure = handoff.fatal_state.failure
    assert failure is not None
    assert isinstance(failure.cause, CaptureBatchOverflowV2)
    assert failure.failing_ingest_seq == 3
    assert writer.aborted
