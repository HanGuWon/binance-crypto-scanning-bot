from __future__ import annotations

import hashlib

from signalbot.r4b_v2.capture.batching import (
    BatchDrainerV2,
    BatchPolicyV2,
    BoundedBatchHandoffV2,
)
from signalbot.r4b_v2.capture.models import (
    RawRecordV2,
    TransportV2,
    VenueV2,
)
from signalbot.r4b_v2.capture.telemetry import CaptureTelemetryV2

PROTOCOL_HASH = hashlib.sha256(b"r4b-v2-telemetry-test").hexdigest()


class Clock:
    def __init__(self) -> None:
        self.now = 10_000

    def __call__(self) -> int:
        return self.now


def _record(ingest_seq: int) -> RawRecordV2:
    return RawRecordV2.from_payload(
        session_id="telemetry-session",
        plan_id="telemetry-plan",
        protocol_hash=PROTOCOL_HASH,
        transport=TransportV2.WEBSOCKET,
        venue=VenueV2.USDM_FUTURES,
        route_id="usdm_public",
        symbol="BTCUSDT",
        connection_id="telemetry-connection",
        generation=1,
        frame_seq=ingest_seq,
        ingest_seq=ingest_seq,
        receipt_wall_ms=1_700_000_000_000 + ingest_seq,
        receipt_monotonic_ns=1_000,
        raw_payload="{}",
        source_logical_key=f"key:{ingest_seq}",
    )


async def test_telemetry_conservation_peaks_lag_and_recent_ring_are_bounded() -> None:
    clock = Clock()
    policy = BatchPolicyV2(
        max_records=2,
        max_encoded_bytes=64 * 1024,
        max_linger_us=0,
        queue_max_events=3,
        queue_max_encoded_bytes=256 * 1024,
        low_water_events=0,
        low_water_encoded_bytes=0,
        qualification_id="telemetry-test",
    )
    telemetry = CaptureTelemetryV2(
        queue_max_events=3,
        queue_max_encoded_bytes=256 * 1024,
        recent_source_limit=2,
    )
    handoff = BoundedBatchHandoffV2(
        policy,
        telemetry=telemetry,
        monotonic_ns=clock,
    )
    for ingest_seq in range(1, 4):
        handoff.offer(_record(ingest_seq))
    offered = handoff.snapshot()
    assert offered.current_events == 3
    assert offered.peak_events == 3
    assert [source.ingest_seq for source in offered.recent_sources] == [2, 3]

    clock.now += 500
    aged = handoff.snapshot()
    assert aged.oldest_enqueued_age_ns == 500
    batch = await BatchDrainerV2(handoff).next_batch()
    dequeued = handoff.snapshot()
    assert dequeued.dequeued_events == 2
    assert dequeued.current_events == 3
    assert dequeued.consumer_lag_records == 3

    handoff.acknowledge_records(
        batch,
        durable_ack_seq=2,
        completed_monotonic_ns=clock.now,
        writer_latency_ns=7,
    )
    acknowledged = handoff.snapshot()
    assert acknowledged.current_events == 1
    assert acknowledged.durable_acked_events == 2
    assert acknowledged.durable_ack_seq == 2
    assert acknowledged.last_writer_latency_ns == 7

    handoff.stop_producer()
    handoff.discard_all()
    final = handoff.snapshot()
    assert final.current_events == 0
    assert final.discarded_events == 1
    assert final.enqueued_events == (
        final.durable_acked_events + final.discarded_events + final.current_events
    )
    assert final.offers_events == final.enqueued_events + final.rejected_events
    assert final.enqueued_encoded_bytes == (
        final.durable_acked_encoded_bytes
        + final.discarded_encoded_bytes
        + final.current_encoded_bytes
    )
    assert final.remaining_event_headroom == policy.queue_max_events
    document = final.to_document()
    assert not {
        key
        for key in document
        if any(token in key.casefold() for token in ("signal", "pnl", "outcome", "order"))
    }


def test_telemetry_rejects_mismatched_bounds_and_invalid_ring_size() -> None:
    policy = BatchPolicyV2(
        max_records=1,
        max_encoded_bytes=10,
        max_linger_us=0,
        queue_max_events=2,
        queue_max_encoded_bytes=20,
        low_water_events=0,
        low_water_encoded_bytes=0,
        qualification_id="bounds-test",
    )
    mismatched = CaptureTelemetryV2(
        queue_max_events=3,
        queue_max_encoded_bytes=20,
    )
    try:
        BoundedBatchHandoffV2(policy, telemetry=mismatched)
    except ValueError as exc:
        assert "bounds" in str(exc)
    else:
        raise AssertionError("mismatched telemetry bounds were accepted")

    try:
        CaptureTelemetryV2(
            queue_max_events=1,
            queue_max_encoded_bytes=1,
            recent_source_limit=65,
        )
    except ValueError as exc:
        assert "between 1 and 64" in str(exc)
    else:
        raise AssertionError("unbounded recent-source ring was accepted")

