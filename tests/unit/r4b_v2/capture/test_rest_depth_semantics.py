from __future__ import annotations

import asyncio
import hashlib
import json
from collections.abc import Sequence
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
    ProvisionalDepthRestQualificationPlanV8,
    ProvisionalPromotingCapturePlanV2,
    build_provisional_promoting_capture_plans_v8,
)
from signalbot.r4b_v2.capture.rest_depth import (
    PUBLIC_DEPTH_REST_MAXIMUM_BODY_BYTES_V8,
    PublicDepthRestAttemptPayloadV8,
    PublicDepthRestErrorCategoryV8,
    PublicDepthRestTerminalObservationV8,
    public_depth_rest_source_logical_key_v8,
)
from signalbot.r4b_v2.capture.rest_depth_semantics import (
    PublicDepthRestSnapshotSemanticErrorV8,
    VerifiedPublicDepthRestSnapshotV8,
    validate_verified_public_depth_rest_snapshot_v8,
    verify_admitted_public_depth_rest_snapshot_v8,
)
from signalbot.r4b_v2.capture.websocket import (
    PublicDepthRestAdmissionReceiptV8,
    SharedWebSocketIngressV2,
    validate_public_depth_rest_admission_receipt_v8,
)

_PROTOCOL_HASH = hashlib.sha256(b"depth-rest-v8-semantic-tests").hexdigest()
_SESSION_ID = "depth-rest-v8-session"
_CONNECTION_ID = "usdm-public-depth-rest-connection"
_BUILT_REQUEST_HEADERS = (
    ("accept", "application/json"),
    ("accept-encoding", "identity"),
    ("connection", "keep-alive"),
    ("host", "fapi.binance.com"),
    ("user-agent", "binance-signalbot-r4b-v2-capture/1"),
)


def _plans(
    symbols: tuple[str, ...] = ("BTCUSDT",),
) -> tuple[ProvisionalPromotingCapturePlanV2, ProvisionalDepthRestQualificationPlanV8]:
    plans = build_provisional_promoting_capture_plans_v8(symbols)
    [market_plan] = [
        item
        for item in plans
        if type(item) is ProvisionalPromotingCapturePlanV2
        and item.route_id == "usdm_market"
    ]
    [depth_plan] = [
        item for item in plans if type(item) is ProvisionalDepthRestQualificationPlanV8
    ]
    return market_plan, depth_plan


def _snapshot_body(
    *,
    bids: Sequence[Sequence[object]] | None = None,
    asks: Sequence[Sequence[object]] | None = None,
    extra: dict[str, object] | None = None,
    event_time: object = 1_589_436_923_972,
    transaction_time: object = 1_589_436_923_971,
    last_update_id: object = 1_027_024,
) -> bytes:
    document: dict[str, object] = {
        "lastUpdateId": last_update_id,
        "E": event_time,
        "T": transaction_time,
        "bids": bids
        if bids is not None
        else [["9517.80", "0.37"], ["9517.70", "1.25"]],
        "asks": asks
        if asks is not None
        else [["9517.90", "0.50"], ["9518.00", "2.00"]],
    }
    if extra is not None:
        document.update(extra)
    return json.dumps(document, separators=(",", ":")).encode()


def _observation(
    plan: ProvisionalDepthRestQualificationPlanV8,
    body: bytes,
    *,
    response_status: int | None = 200,
    payload_complete: bool = True,
    error_category: PublicDepthRestErrorCategoryV8 | None = None,
    error_detail: str | None = None,
) -> PublicDepthRestTerminalObservationV8:
    first_wall = None if response_status is None else 1_002
    first_monotonic = None if response_status is None else 10_002
    headers = () if response_status is None else (("content-type", "application/json"),)
    return PublicDepthRestTerminalObservationV8.for_plan(
        plan,
        session_id=_SESSION_ID,
        protocol_hash=_PROTOCOL_HASH,
        connection_id=_CONNECTION_ID,
        method="GET",
        base_url="https://fapi.binance.com",
        endpoint="/fapi/v1/depth",
        symbol="BTCUSDT",
        canonical_query=(("limit", "1000"), ("symbol", "BTCUSDT")),
        request_headers=_BUILT_REQUEST_HEADERS,
        trigger="startup",
        trigger_seq=1,
        connection_generation=3,
        first_buffered_u=100,
        symbol_ordinal=0,
        bridge_attempt=1,
        request_started_wall_ms=1_001,
        request_started_monotonic_ns=10_001,
        response_first_header_wall_ms=first_wall,
        response_first_header_monotonic_ns=first_monotonic,
        attempt_ended_wall_ms=1_003,
        attempt_ended_monotonic_ns=10_003,
        response_status=response_status,
        response_headers=headers,
        payload_complete=payload_complete,
        body=body,
        error_category=error_category,
        error_detail=error_detail,
    )


def _handoff(expected_first_ingest_seq: int = 1) -> BoundedBatchHandoffV2:
    return BoundedBatchHandoffV2(
        BatchPolicyV2(
            max_records=8,
            max_encoded_bytes=4_000_000,
            max_linger_us=1_000,
            queue_max_events=32,
            queue_max_encoded_bytes=16_000_000,
            low_water_events=0,
            low_water_encoded_bytes=0,
            qualification_id="depth-rest-v8-semantic-admission-tests",
        ),
        expected_first_ingest_seq=expected_first_ingest_seq,
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


async def _admit(
    body: bytes,
    *,
    response_status: int | None = 200,
    payload_complete: bool = True,
    error_category: PublicDepthRestErrorCategoryV8 | None = None,
    error_detail: str | None = None,
    cancellation_requested: asyncio.Event | None = None,
    recovered_tail: int = 0,
) -> tuple[
    ProvisionalDepthRestQualificationPlanV8,
    PublicDepthRestAdmissionReceiptV8,
    _RecordingOfferer,
]:
    _, plan = _plans()
    offerer = _RecordingOfferer(_handoff(recovered_tail + 1))
    ingress = SharedWebSocketIngressV2(
        offerer,
        recovered_wal_tail_ingest_seq=recovered_tail,
    )
    receipt = await ingress.offer_depth_https_attempt_v8(
        plan=plan,
        session_id=_SESSION_ID,
        protocol_hash=_PROTOCOL_HASH,
        connection_id=_CONNECTION_ID,
        generation=3,
        symbol="BTCUSDT",
        clock=_FixedClock(ReceiptTimestamp(1_004, 10_004)),
        observation=_observation(
            plan,
            body,
            response_status=response_status,
            payload_complete=payload_complete,
            error_category=error_category,
            error_detail=error_detail,
        ),
        source_logical_key=public_depth_rest_source_logical_key_v8("BTCUSDT"),
        cancellation_requested=cancellation_requested,
    )
    return plan, receipt, offerer


@pytest.mark.asyncio
async def test_actual_shared_ingress_admission_and_strict_semantics_succeed() -> None:
    plan, receipt, offerer = await _admit(_snapshot_body(), recovered_tail=7)

    record = validate_public_depth_rest_admission_receipt_v8(receipt, plan=plan)
    verified = verify_admitted_public_depth_rest_snapshot_v8(receipt, plan=plan)

    assert receipt.accepted_ingest_seq == 8
    assert record is offerer.records[0]
    assert record.transport is TransportV2.HTTPS
    assert record.route_id == "usdm_public_depth_rest"
    assert record.symbol == "BTCUSDT"
    assert record.source_logical_key == "depthSnapshot:BTCUSDT"
    payload = PublicDepthRestAttemptPayloadV8.from_canonical_bytes(
        record.payload_bytes(), plan=plan
    )
    assert plan.auth_mode == "NONE"
    assert plan.requires_api_key is False
    assert plan.is_private is False
    assert not hasattr(payload, "api_key")
    assert not hasattr(payload, "signature")
    assert payload.canonical_query == (("limit", "1000"), ("symbol", "BTCUSDT"))
    assert payload.completion_admission_wall_ms == record.receipt_wall_ms
    assert verified.last_update_id == 1_027_024
    assert verified.bids[0].price_text == "9517.80"
    assert verified.asks[0].quantity_text == "0.50"
    assert verified.queue_admission_verified is True
    assert verified.qualification_only is True
    assert verified.promoting is False
    assert verified.promotion_ready is False
    assert verified.wal_durability_verified is False
    assert verified.finality_fence_verified is False
    assert verified.freshness_verified is False
    assert verified.coverage_complete is False
    assert verified.m2_certified is False
    assert verified.book_bridge_certified is False
    assert verified.liquidity_signal_emitted is False
    assert verified.order_execution_enabled is False
    assert validate_verified_public_depth_rest_snapshot_v8(verified) == (
        verified.semantic_admission_sha256
    )


@pytest.mark.asyncio
async def test_depth_attempt_uses_same_global_sequence_as_websocket_frames() -> None:
    market_plan, depth_plan = _plans()
    offerer = _RecordingOfferer(_handoff())
    ingress = SharedWebSocketIngressV2(offerer, recovered_wal_tail_ingest_seq=0)
    frame = await ingress.offer_frame(
        plan=market_plan,
        session_id="depth-rest-v8-session",
        protocol_hash=_PROTOCOL_HASH,
        connection_id="usdm-market-connection",
        generation=1,
        frame_seq=1,
        receipt=ReceiptTimestamp(900, 9_000),
        raw_payload=b"{}",
    )
    depth_receipt = await ingress.offer_depth_https_attempt_v8(
        plan=depth_plan,
        session_id="depth-rest-v8-session",
        protocol_hash=_PROTOCOL_HASH,
        connection_id="usdm-public-depth-rest-connection",
        generation=3,
        symbol="BTCUSDT",
        clock=_FixedClock(ReceiptTimestamp(1_004, 10_004)),
        observation=_observation(depth_plan, _snapshot_body()),
        source_logical_key="depthSnapshot:BTCUSDT",
    )

    assert frame.ingest_seq == 1
    assert depth_receipt.accepted_ingest_seq == 2
    assert [record.ingest_seq for record in offerer.records] == [1, 2]


@pytest.mark.asyncio
async def test_cancellation_is_retained_but_excluded_from_semantics() -> None:
    cancellation = asyncio.Event()
    cancellation.set()
    plan, receipt, offerer = await _admit(
        _snapshot_body(), cancellation_requested=cancellation
    )

    payload = PublicDepthRestAttemptPayloadV8.from_canonical_bytes(
        receipt.record.payload_bytes(), plan=plan
    )

    assert len(offerer.records) == 1
    assert payload.admission_cancellation_requested is True
    with pytest.raises(PublicDepthRestSnapshotSemanticErrorV8, match="uncancelled"):
        verify_admitted_public_depth_rest_snapshot_v8(receipt)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("response_status", "payload_complete", "category", "detail", "body"),
    (
        (
            500,
            True,
            PublicDepthRestErrorCategoryV8.HTTP_STATUS,
            "server status 500",
            b'{"code":-1}',
        ),
        (
            None,
            False,
            PublicDepthRestErrorCategoryV8.NETWORK,
            "connection failed",
            b"",
        ),
    ),
)
async def test_failure_attempts_are_raw_admitted_but_semantically_excluded(
    response_status: int | None,
    payload_complete: bool,
    category: PublicDepthRestErrorCategoryV8,
    detail: str,
    body: bytes,
) -> None:
    _, receipt, offerer = await _admit(
        body,
        response_status=response_status,
        payload_complete=payload_complete,
        error_category=category,
        error_detail=detail,
    )

    assert len(offerer.records) == 1
    with pytest.raises(PublicDepthRestSnapshotSemanticErrorV8, match="HTTP 200"):
        verify_admitted_public_depth_rest_snapshot_v8(receipt)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "body",
    (
        b"[]",
        b"not-json",
        b'{"lastUpdateId":1,"E":2,"T":1,"bids":[],"asks":[["2","1"]]}',
        b'{"lastUpdateId":1,"lastUpdateId":2,"E":2,"T":1,"bids":[["1","1"]],"asks":[["2","1"]]}',
        _snapshot_body(extra={"newField": 1}),
        _snapshot_body(bids=[["9517.70", "1"], ["9517.80", "1"]]),
        _snapshot_body(asks=[["9518.00", "1"], ["9517.90", "1"]]),
        _snapshot_body(bids=[["9518.00", "1"]], asks=[["9518.00", "1"]]),
        _snapshot_body(bids=[["9517.80", "0"]]),
        _snapshot_body(event_time=1, transaction_time=2),
        _snapshot_body(last_update_id=True),
    ),
)
async def test_malformed_schema_geometry_types_and_duplicates_fail_closed(
    body: bytes,
) -> None:
    _, receipt, _ = await _admit(body)
    with pytest.raises(PublicDepthRestSnapshotSemanticErrorV8):
        verify_admitted_public_depth_rest_snapshot_v8(receipt)


@pytest.mark.asyncio
async def test_exact_1000_levels_per_side_are_accepted_and_1001_are_rejected() -> None:
    bids_1000 = [[str(2_000 - index), "1"] for index in range(1_000)]
    asks_1000 = [[str(2_001 + index), "1"] for index in range(1_000)]
    _, accepted, _ = await _admit(_snapshot_body(bids=bids_1000, asks=asks_1000))

    verified = verify_admitted_public_depth_rest_snapshot_v8(accepted)

    assert len(verified.bids) == 1_000
    assert len(verified.asks) == 1_000

    bids_1001 = [[str(2_001 - index), "1"] for index in range(1_001)]
    _, rejected, _ = await _admit(_snapshot_body(bids=bids_1001, asks=asks_1000))
    with pytest.raises(PublicDepthRestSnapshotSemanticErrorV8, match="1000"):
        verify_admitted_public_depth_rest_snapshot_v8(rejected)


@pytest.mark.asyncio
async def test_exact_body_byte_cap_can_be_admitted_and_verified() -> None:
    core = _snapshot_body()
    body = core + b" " * (PUBLIC_DEPTH_REST_MAXIMUM_BODY_BYTES_V8 - len(core))

    _, receipt, _ = await _admit(body)
    verified = verify_admitted_public_depth_rest_snapshot_v8(receipt)

    assert verified.body_semantics_valid is True


@pytest.mark.asyncio
async def test_max_signed_int64_source_values_hash_losslessly() -> None:
    maximum_int64 = (1 << 63) - 1
    body = _snapshot_body(
        last_update_id=maximum_int64,
        event_time=maximum_int64,
        transaction_time=maximum_int64,
    )

    _, receipt, _ = await _admit(body)
    verified = verify_admitted_public_depth_rest_snapshot_v8(receipt)

    assert verified.last_update_id == maximum_int64
    assert verified.event_time_ms == maximum_int64
    assert verified.transaction_time_ms == maximum_int64
    assert validate_verified_public_depth_rest_snapshot_v8(verified) == (
        verified.semantic_admission_sha256
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "body",
    (
        _snapshot_body(last_update_id=1 << 63),
        _snapshot_body(event_time=1 << 63),
        _snapshot_body(transaction_time=1 << 63),
    ),
)
async def test_source_integer_above_signed_int64_is_rejected(body: bytes) -> None:
    _, receipt, _ = await _admit(body)
    with pytest.raises(PublicDepthRestSnapshotSemanticErrorV8, match="int64"):
        verify_admitted_public_depth_rest_snapshot_v8(receipt)


@pytest.mark.asyncio
async def test_plan_source_key_generation_and_expected_plan_drift_fail_before_offer() -> None:
    _, plan = _plans()
    offerer = _RecordingOfferer(_handoff())
    ingress = SharedWebSocketIngressV2(offerer, recovered_wal_tail_ingest_seq=0)
    observation = _observation(plan, _snapshot_body())
    common = {
        "plan": plan,
        "session_id": "depth-rest-v8-session",
        "protocol_hash": _PROTOCOL_HASH,
        "connection_id": "usdm-public-depth-rest-connection",
        "generation": 3,
        "symbol": "BTCUSDT",
        "clock": _FixedClock(ReceiptTimestamp(1_004, 10_004)),
        "observation": observation,
        "source_logical_key": "depthSnapshot:BTCUSDT",
    }
    with pytest.raises(ValueError, match="stable per-symbol"):
        await ingress.offer_depth_https_attempt_v8(
            **{**common, "source_logical_key": "depthSnapshot:BTCUSDT:attempt-1"}
        )
    with pytest.raises(ValueError, match="generation"):
        await ingress.offer_depth_https_attempt_v8(**{**common, "generation": 4})
    with pytest.raises(ValueError, match="lineage"):
        await ingress.offer_depth_https_attempt_v8(
            **{**common, "session_id": "different-session"}
        )
    with pytest.raises(ValueError, match="lineage"):
        await ingress.offer_depth_https_attempt_v8(
            **{**common, "protocol_hash": "f" * 64}
        )
    with pytest.raises(ValueError, match="lineage"):
        await ingress.offer_depth_https_attempt_v8(
            **{**common, "connection_id": "different-connection"}
        )
    assert offerer.records == []

    _, receipt, _ = await _admit(_snapshot_body())
    _, other_plan = _plans(("ETHUSDT",))
    with pytest.raises(ValueError, match="different plan"):
        validate_public_depth_rest_admission_receipt_v8(receipt, plan=other_plan)


def test_semantic_result_and_queue_receipt_cannot_be_publicly_forged() -> None:
    with pytest.raises(TypeError, match="factory-sealed"):
        VerifiedPublicDepthRestSnapshotV8(
            symbol="BTCUSDT",
            last_update_id=1,
            event_time_ms=2,
            transaction_time_ms=1,
            bids=(),
            asks=(),
            plan_sha256="0" * 64,
            attempt_payload_sha256="0" * 64,
            raw_record_sha256="0" * 64,
            body_sha256="0" * 64,
        )

    _, plan = _plans()
    with pytest.raises(TypeError, match="shared ingress"):
        PublicDepthRestAdmissionReceiptV8(
            plan=plan,
            record=replace(
                RawRecordV2.from_payload(
                    session_id="s",
                    plan_id=plan.name,
                    protocol_hash="0" * 64,
                    transport=TransportV2.HTTPS,
                    venue=plan.venue,
                    route_id=plan.route_id,
                    symbol="BTCUSDT",
                    connection_id="c",
                    generation=1,
                    frame_seq=None,
                    ingest_seq=1,
                    receipt_wall_ms=1,
                    receipt_monotonic_ns=1,
                    raw_payload=b"{}",
                )
            ),
            queue_admission_receipt=object(),  # type: ignore[arg-type]
        )
