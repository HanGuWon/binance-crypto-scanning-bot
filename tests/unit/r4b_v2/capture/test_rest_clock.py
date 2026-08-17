from __future__ import annotations

import hashlib
from dataclasses import dataclass, field, replace

import pytest

from signalbot.capture.receipts import ReceiptTimestamp
from signalbot.r4b_v2.capture.batching import (
    BatchPolicyV2,
    BoundedBatchHandoffV2,
    CaptureQueueAdmissionReceiptV2,
    QueuedRawRecordV2,
)
from signalbot.r4b_v2.capture.models import RawRecordV2, TransportV2
from signalbot.r4b_v2.capture.plans import (
    ProvisionalUsdmVenueClockRestCapturePlanV9,
    build_provisional_promoting_capture_plans_v9,
)
from signalbot.r4b_v2.capture.rest_clock import (
    PUBLIC_USDM_VENUE_CLOCK_SOURCE_LOGICAL_KEY_V9,
    PublicUsdmVenueClockRestAttemptPayloadV9,
    PublicUsdmVenueClockRestErrorCategoryV9,
    PublicUsdmVenueClockRestTerminalObservationV9,
)
from signalbot.r4b_v2.capture.websocket import (
    PublicUsdmVenueClockAdmissionReceiptV9,
    SharedWebSocketIngressV2,
    validate_public_usdm_venue_clock_admission_receipt_v9,
)

_PROTOCOL_HASH = hashlib.sha256(b"usdm-clock-rest-tests").hexdigest()
_SESSION_ID = "usdm-clock-rest-session"
_CONNECTION_ID = "usdm-clock-rest-connection"
_SLOT_MS = 1_710_000_000_000


def _plan() -> ProvisionalUsdmVenueClockRestCapturePlanV9:
    plans = build_provisional_promoting_capture_plans_v9(("BTCUSDT",))
    [plan] = [
        item
        for item in plans
        if type(item) is ProvisionalUsdmVenueClockRestCapturePlanV9
    ]
    return plan


def _observation(
    plan: ProvisionalUsdmVenueClockRestCapturePlanV9,
    *,
    body: bytes = b'{"serverTime":1710000000010}',
    response_status: int | None = 200,
    payload_complete: bool = True,
    error_category: PublicUsdmVenueClockRestErrorCategoryV9 | None = None,
    error_detail: str | None = None,
) -> PublicUsdmVenueClockRestTerminalObservationV9:
    first_wall = None if response_status is None else _SLOT_MS + 10
    first_monotonic = None if response_status is None else 10_000_000
    return PublicUsdmVenueClockRestTerminalObservationV9.for_plan(
        plan,
        session_id=_SESSION_ID,
        protocol_hash=_PROTOCOL_HASH,
        connection_id=_CONNECTION_ID,
        connection_generation=2,
        poll_cycle_seq=7,
        scheduled_slot_wall_ms=_SLOT_MS,
        request_started_wall_ms=_SLOT_MS,
        request_started_monotonic_ns=0,
        response_first_header_wall_ms=first_wall,
        response_first_header_monotonic_ns=first_monotonic,
        attempt_ended_wall_ms=_SLOT_MS + 11,
        attempt_ended_monotonic_ns=11_000_000,
        response_status=response_status,
        response_headers=(
            ()
            if response_status is None
            else (("content-type", "application/json"),)
        ),
        payload_complete=payload_complete,
        body=body,
        error_category=error_category,
        error_detail=error_detail,
    )


def _handoff() -> BoundedBatchHandoffV2:
    return BoundedBatchHandoffV2(
        BatchPolicyV2(
            max_records=8,
            max_encoded_bytes=100_000,
            max_linger_us=1_000,
            queue_max_events=32,
            queue_max_encoded_bytes=1_000_000,
            low_water_events=0,
            low_water_encoded_bytes=0,
            qualification_id="usdm-clock-rest-admission-tests",
        ),
        expected_first_ingest_seq=1,
    )


@dataclass(slots=True)
class _RecordingOfferer:
    handoff: BoundedBatchHandoffV2
    records: list[RawRecordV2] = field(default_factory=list)

    def offer(self, record: RawRecordV2) -> QueuedRawRecordV2:
        queued = self.handoff.offer(record)
        self.records.append(record)
        return queued

    def offer_with_admission_receipt(
        self,
        record: RawRecordV2,
    ) -> CaptureQueueAdmissionReceiptV2:
        receipt = self.handoff.offer_with_admission_receipt(record)
        self.records.append(record)
        return receipt

    def validate_queue_admission_receipt_v2(
        self,
        receipt: CaptureQueueAdmissionReceiptV2,
    ) -> QueuedRawRecordV2:
        return self.handoff.validate_queue_admission_receipt_v2(receipt)


@dataclass(slots=True)
class _FixedClock:
    receipt: ReceiptTimestamp

    def capture(self) -> ReceiptTimestamp:
        return self.receipt


def test_success_attempt_round_trips_exact_canonical_terminal_material() -> None:
    plan = _plan()
    observation = _observation(plan)
    completion = ReceiptTimestamp(_SLOT_MS + 12, 12_000_000)
    encoded = observation(completion)
    payload = PublicUsdmVenueClockRestAttemptPayloadV9.from_canonical_bytes(
        encoded,
        plan=plan,
    )

    assert payload.body_bytes() == b'{"serverTime":1710000000010}'
    assert payload.response_status == 200
    assert payload.canonical_query == ()
    assert payload.completion_admission_wall_ms == _SLOT_MS + 12
    assert payload.canonical_bytes() == encoded
    assert payload.infrastructure_clock_only and not payload.promoting
    assert not payload.causal_cursor_complete and not payload.order_execution_enabled


def test_failed_status_requires_and_round_trips_sanitized_error() -> None:
    plan = _plan()
    observation = _observation(
        plan,
        body=b'{"code":-1000}',
        response_status=500,
        error_category=PublicUsdmVenueClockRestErrorCategoryV9.HTTP_STATUS,
        error_detail="HTTP status 500",
    )
    payload = PublicUsdmVenueClockRestAttemptPayloadV9.from_canonical_bytes(
        observation(ReceiptTimestamp(_SLOT_MS + 12, 12_000_000)),
        plan=plan,
    )
    assert payload.response_status == 500
    assert payload.error_category is PublicUsdmVenueClockRestErrorCategoryV9.HTTP_STATUS

    with pytest.raises(ValueError, match="sanitized error"):
        _observation(plan, response_status=500)


def test_body_cap_canonical_mutation_and_plan_drift_fail_closed() -> None:
    plan = _plan()
    with pytest.raises(ValueError, match="body exceeds"):
        _observation(plan, body=b"x" * 4_097)

    encoded = _observation(plan)(ReceiptTimestamp(_SLOT_MS + 12, 12_000_000))
    with pytest.raises(ValueError, match="canonical"):
        PublicUsdmVenueClockRestAttemptPayloadV9.from_canonical_bytes(
            encoded.rstrip(b"\n") + b" \n",
            plan=plan,
        )
    with pytest.raises(ValueError, match="plan hash"):
        replace(
            PublicUsdmVenueClockRestAttemptPayloadV9.from_canonical_bytes(encoded),
            plan_sha256="f" * 64,
        ).validate_against_plan(plan)


@pytest.mark.asyncio
async def test_shared_ingress_admits_clock_attempt_into_exact_bounded_wal_queue_path() -> None:
    plan = _plan()
    offerer = _RecordingOfferer(_handoff())
    ingress = SharedWebSocketIngressV2(offerer, recovered_wal_tail_ingest_seq=0)
    receipt = await ingress.offer_usdm_venue_clock_https_attempt_v9(
        plan=plan,
        session_id=_SESSION_ID,
        protocol_hash=_PROTOCOL_HASH,
        connection_id=_CONNECTION_ID,
        generation=2,
        clock=_FixedClock(ReceiptTimestamp(_SLOT_MS + 12, 12_000_000)),
        observation=_observation(plan),
        source_logical_key=PUBLIC_USDM_VENUE_CLOCK_SOURCE_LOGICAL_KEY_V9,
    )

    assert type(receipt) is PublicUsdmVenueClockAdmissionReceiptV9
    record = validate_public_usdm_venue_clock_admission_receipt_v9(
        receipt,
        plan=plan,
    )
    assert receipt.accepted_ingest_seq == 1
    assert record is offerer.records[0]
    assert record.transport is TransportV2.HTTPS
    assert record.symbol is None
    assert record.source_logical_key == PUBLIC_USDM_VENUE_CLOCK_SOURCE_LOGICAL_KEY_V9
    assert offerer.handoff.current_events == 1


@pytest.mark.asyncio
async def test_shared_ingress_rejects_foreign_clock_source_key_before_admission() -> None:
    plan = _plan()
    offerer = _RecordingOfferer(_handoff())
    ingress = SharedWebSocketIngressV2(offerer, recovered_wal_tail_ingest_seq=0)
    with pytest.raises(ValueError, match="source key"):
        await ingress.offer_usdm_venue_clock_https_attempt_v9(
            plan=plan,
            session_id=_SESSION_ID,
            protocol_hash=_PROTOCOL_HASH,
            connection_id=_CONNECTION_ID,
            generation=2,
            clock=_FixedClock(ReceiptTimestamp(_SLOT_MS + 12, 12_000_000)),
            observation=_observation(plan),
            source_logical_key="venueTime:spot",
        )
    assert offerer.records == []
