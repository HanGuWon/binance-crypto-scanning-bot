from __future__ import annotations

import asyncio
from dataclasses import dataclass, field, replace

import httpx
import pytest

import signalbot.r4b_v2.capture.rest_depth_adapter as rest_depth_adapter_module
from signalbot.capture.receipts import ReceiptClock, ReceiptTimestamp
from signalbot.r4b_v2.capture.batching import (
    BatchPolicyV2,
    BoundedBatchHandoffV2,
    CaptureQueueAdmissionReceiptV2,
    QueuedRawRecordV2,
)
from signalbot.r4b_v2.capture.models import RawRecordV2, TransportV2
from signalbot.r4b_v2.capture.plans import (
    ProvisionalDepthRestQualificationPlanV8,
    build_provisional_promoting_capture_plans_v8,
)
from signalbot.r4b_v2.capture.rest_attempt_owner import (
    RestCaptureOwnershipFailureV2,
    _RestRequestSpec,
)
from signalbot.r4b_v2.capture.rest_depth import (
    PUBLIC_DEPTH_REST_MAXIMUM_BODY_BYTES_V8,
    PublicDepthRestAttemptPayloadV8,
    PublicDepthRestErrorCategoryV8,
    PublicDepthSnapshotTriggerV8,
)
from signalbot.r4b_v2.capture.rest_depth_adapter import (
    PublicDepthRestCaptureAdapterV8,
    _DepthRestAttempt,
)
from signalbot.r4b_v2.capture.rest_depth_scheduler import (
    PublicDepthRestRegisteredCycleV8,
    PublicDepthRestScheduleAuthorityV8,
    PublicDepthRestScheduledAttemptOwnershipErrorV8,
    PublicDepthRestScheduledAttemptTokenV8,
    assert_public_depth_rest_scheduled_attempt_token_consumed_v8,
    create_public_depth_rest_schedule_authority_v8,
)
from signalbot.r4b_v2.capture.websocket import (
    HttpsRestWallClockRegressionErrorV2,
    PublicDepthRestAdmissionReceiptV8,
    SharedWebSocketIngressV2,
    validate_public_depth_rest_admission_receipt_v8,
)

_PROTOCOL_HASH = "d" * 64
_SESSION_ID = "session-depth-rest-adapter-test"


def _connection_id(generation: int) -> str:
    return f"usdm-public-g{generation:06d}"


def _handoff() -> BoundedBatchHandoffV2:
    return BoundedBatchHandoffV2(
        BatchPolicyV2(
            max_records=32,
            max_encoded_bytes=8_000_000,
            max_linger_us=1_000,
            queue_max_events=128,
            queue_max_encoded_bytes=64_000_000,
            low_water_events=0,
            low_water_encoded_bytes=0,
            qualification_id="depth-rest-adapter-test",
        )
    )


@dataclass(slots=True)
class RecordingOfferer:
    records: list[RawRecordV2] = field(default_factory=list)
    handoff: BoundedBatchHandoffV2 = field(default_factory=_handoff)

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
class RecordingFatalCoordinator:
    causes: list[BaseException] = field(default_factory=list)

    def trip_fatal(self, cause: BaseException) -> None:
        if not self.causes:
            self.causes.append(cause)

    def raise_if_failed(self) -> None:
        if self.causes:
            raise self.causes[0]


class IncrementingReceiptClock:
    def __init__(self) -> None:
        self._wall_ms = 1_700_000_000_000
        self._monotonic_ns = 10_000
        self.calls = 0

    def capture(self) -> ReceiptTimestamp:
        self.calls += 1
        receipt = ReceiptTimestamp(self._wall_ms, self._monotonic_ns)
        self._wall_ms += 1
        self._monotonic_ns += 1
        return receipt


class ScriptedReceiptClock:
    def __init__(self, *receipts: ReceiptTimestamp) -> None:
        self._receipts = receipts
        self.calls = 0

    def capture(self) -> ReceiptTimestamp:
        if self.calls >= len(self._receipts):
            raise AssertionError("scripted depth clock exhausted")
        receipt = self._receipts[self.calls]
        self.calls += 1
        return receipt


class GatedDepthTransport(httpx.AsyncBaseTransport):
    def __init__(self) -> None:
        self.request_started = asyncio.Event()
        self.request_release = asyncio.Event()
        self.requests: list[httpx.Request] = []

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        self.request_started.set()
        await self.request_release.wait()
        return httpx.Response(200, content=b"{}", request=request)


def _plan(
    symbols: tuple[str, ...] = ("BTCUSDT", "ETHUSDT"),
) -> ProvisionalDepthRestQualificationPlanV8:
    plans = build_provisional_promoting_capture_plans_v8(symbols)
    [plan] = [
        item for item in plans if type(item) is ProvisionalDepthRestQualificationPlanV8
    ]
    return plan


def _adapter(
    transport: httpx.AsyncBaseTransport,
    *,
    symbols: tuple[str, ...] = ("BTCUSDT", "ETHUSDT"),
    generation: int = 1,
    plan: ProvisionalDepthRestQualificationPlanV8 | None = None,
    clock: ReceiptClock | None = None,
) -> tuple[
    PublicDepthRestCaptureAdapterV8,
    PublicDepthRestScheduleAuthorityV8,
    ProvisionalDepthRestQualificationPlanV8,
    RecordingOfferer,
    RecordingFatalCoordinator,
]:
    selected_plan = _plan(symbols) if plan is None else plan
    authority = create_public_depth_rest_schedule_authority_v8(selected_plan)
    authority.advance_connection_generation(
        1,
        session_id=_SESSION_ID,
        protocol_hash=_PROTOCOL_HASH,
        connection_id=_connection_id(1),
    )
    offerer = RecordingOfferer()
    fatal = RecordingFatalCoordinator()
    adapter = PublicDepthRestCaptureAdapterV8(
        selected_plan,
        session_id=_SESSION_ID,
        protocol_hash=_PROTOCOL_HASH,
        connection_id=_connection_id(generation),
        generation=generation,
        clock=IncrementingReceiptClock() if clock is None else clock,
        ingress=SharedWebSocketIngressV2(
            offerer,
            recovered_wal_tail_ingest_seq=0,
        ),
        fatal_coordinator=fatal,
        transport=transport,
    )
    adapter.bind_schedule_authority(authority)
    return adapter, authority, selected_plan, offerer, fatal


def _issue(
    authority: PublicDepthRestScheduleAuthorityV8,
    *,
    symbol_ordinal: int = 0,
    trigger: PublicDepthSnapshotTriggerV8 = "startup",
    connection_generation: int = 1,
    first_buffered_u: int = 100,
    bridge_attempt: int = 1,
    registration: PublicDepthRestRegisteredCycleV8 | None = None,
) -> PublicDepthRestScheduledAttemptTokenV8:
    if registration is None:
        watermarks = (
            ((authority.plan.symbols[symbol_ordinal], first_buffered_u),)
            if trigger == "sequence_gap"
            else tuple((symbol, first_buffered_u) for symbol in authority.symbol_census)
        )
        registered = authority.register_trigger(
            trigger=trigger,
            connection_generation=connection_generation,
            symbol_watermarks=watermarks,
        )
        registration = (
            registered[0] if trigger == "sequence_gap" else registered[symbol_ordinal]
        )
    return authority.issue_attempt(
        registration=registration,
        bridge_attempt=bridge_attempt,
    )


def _payload(
    record: RawRecordV2,
    plan: ProvisionalDepthRestQualificationPlanV8,
) -> PublicDepthRestAttemptPayloadV8:
    return PublicDepthRestAttemptPayloadV8.from_canonical_bytes(
        record.payload_bytes(),
        plan=plan,
    )


@pytest.mark.asyncio
async def test_exact_keyless_depth_request_is_admitted_and_acknowledged() -> None:
    requests: list[httpx.Request] = []
    body = b'{"lastUpdateId":123,"bids":[],"asks":[]}'

    async def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(200, content=body, request=request)

    adapter, authority, plan, offerer, fatal = _adapter(
        httpx.MockTransport(handler)
    )
    token = _issue(authority)

    receipt = await adapter.capture_attempt(token)
    assert adapter.retained_terminal_admission_count == 0
    assert adapter.take_terminal_admission_after_failure(token) is None
    await adapter.aclose()

    assert type(receipt) is PublicDepthRestAdmissionReceiptV8
    assert validate_public_depth_rest_admission_receipt_v8(
        receipt,
        plan=plan,
    ) is receipt.record
    assert len(requests) == 1
    request = requests[0]
    assert request.method == "GET"
    assert request.url.scheme == "https"
    assert request.url.host == "fapi.binance.com"
    assert request.url.path == "/fapi/v1/depth"
    assert tuple(request.url.params.multi_items()) == (
        ("limit", "1000"),
        ("symbol", "BTCUSDT"),
    )
    assert request.headers["accept"] == "application/json"
    assert request.headers["accept-encoding"] == "identity"
    assert "authorization" not in request.headers
    assert "x-mbx-apikey" not in request.headers
    assert "signature" not in request.url.params
    assert "timestamp" not in request.url.params
    assert offerer.records == [receipt.record]
    assert receipt.record.transport is TransportV2.HTTPS
    assert receipt.record.connection_id == "usdm-public-g000001"
    assert receipt.record.generation == 1
    payload = _payload(receipt.record, plan)
    assert payload.body_bytes() == body
    assert payload.session_id == _SESSION_ID
    assert payload.protocol_hash == _PROTOCOL_HASH
    assert payload.connection_id == _connection_id(1)
    assert payload.method == request.method
    assert payload.base_url == f"{request.url.scheme}://{request.url.host}"
    assert payload.endpoint == request.url.path
    assert payload.canonical_query == tuple(request.url.params.multi_items())
    assert payload.request_headers == tuple(
        sorted((name.lower(), value) for name, value in request.headers.multi_items())
    )
    assert payload.payload_complete is True
    assert payload.error_category is None
    assert payload.trigger == "startup"
    assert payload.trigger_seq == 1
    assert payload.first_buffered_u == 100
    assert payload.bridge_attempt == 1
    assert payload.admission_cancellation_requested is False
    assert fatal.causes == []
    assert adapter.cleanly_closed

    second = _issue(
        authority,
        registration=token.registration,
        bridge_attempt=2,
    )
    assert second.bridge_attempt == 2


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "headers",
    [
        tuple((f"x-mbx-used-weight-{index:02d}", "1") for index in range(16)),
        (("x-mbx-used-weight-1m", "x" * 256),),
    ],
)
async def test_depth_response_header_count_and_value_boundaries_are_retained(
    headers: tuple[tuple[str, str], ...],
) -> None:
    adapter, authority, plan, offerer, fatal = _adapter(
        httpx.MockTransport(
            lambda request: httpx.Response(
                200,
                headers=headers,
                content=b"{}",
                request=request,
            )
        )
    )
    token = _issue(authority)

    receipt = await adapter.capture_attempt(token)
    await adapter.aclose()

    assert offerer.records == [receipt.record]
    payload = _payload(receipt.record, plan)
    assert payload.response_headers == tuple(sorted(headers))
    assert payload.error_category is None
    assert payload.payload_complete is True
    assert authority.claimed_token_count == 0
    assert fatal.causes == []


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "headers",
    [
        tuple((f"x-mbx-used-weight-{index:02d}", "1") for index in range(17)),
        (("x-mbx-used-weight-1m", "x" * 257),),
    ],
)
async def test_depth_response_header_overflow_retains_empty_terminal_row_and_fails(
    headers: tuple[tuple[str, str], ...],
) -> None:
    adapter, authority, plan, offerer, fatal = _adapter(
        httpx.MockTransport(
            lambda request: httpx.Response(
                200,
                headers=headers,
                content=b"body-must-not-be-read",
                request=request,
            )
        )
    )
    token = _issue(authority)

    with pytest.raises(
        RestCaptureOwnershipFailureV2,
        match="response headers violated the bounded normalization contract",
    ) as captured:
        await adapter.capture_attempt(token)
    with pytest.raises(RestCaptureOwnershipFailureV2) as close_failure:
        await adapter.aclose()

    assert close_failure.value is captured.value
    assert fatal.causes == [captured.value]
    assert len(offerer.records) == 1
    assert adapter.retained_terminal_admission_count == 1
    retained = adapter.take_terminal_admission_after_failure(token)
    assert retained is not None
    assert retained.record is offerer.records[0]
    assert adapter.retained_terminal_admission_count == 0
    assert adapter.take_terminal_admission_after_failure(token) is None
    payload = _payload(offerer.records[0], plan)
    assert payload.response_status == 200
    assert payload.response_headers == ()
    assert payload.error_category is PublicDepthRestErrorCategoryV8.RESPONSE_READ
    assert payload.payload_complete is False
    assert payload.body_bytes() == b""
    assert authority.claimed_token_count == 0
    assert_public_depth_rest_scheduled_attempt_token_consumed_v8(
        token,
        plan=plan,
        schedule_authority=authority,
    )


@pytest.mark.asyncio
async def test_adapter_lineage_properties_reject_assignment() -> None:
    adapter, _authority_value, plan, _offerer, _fatal = _adapter(
        httpx.MockTransport(
            lambda request: httpx.Response(200, content=b"{}", request=request)
        )
    )

    for property_name, replacement in (
        ("plan", _plan(("BTCUSDT",))),
        ("session_id", "replacement-session"),
        ("protocol_hash", "a" * 64),
        ("connection_id", "replacement-connection"),
        ("generation", 2),
    ):
        with pytest.raises(AttributeError):
            setattr(adapter, property_name, replacement)

    assert adapter.plan is plan
    assert adapter.session_id == _SESSION_ID
    assert adapter.protocol_hash == _PROTOCOL_HASH
    assert adapter.connection_id == _connection_id(1)
    assert adapter.generation == 1
    await adapter.aclose()


@pytest.mark.asyncio
async def test_claimed_attempt_cannot_redirect_frozen_adapter_dependencies() -> None:
    transport = GatedDepthTransport()
    original_clock = IncrementingReceiptClock()
    adapter, authority, plan, offerer, fatal = _adapter(
        transport,
        clock=original_clock,
    )
    token = _issue(authority)
    attempt = asyncio.create_task(adapter.capture_attempt(token))
    await asyncio.wait_for(transport.request_started.wait(), timeout=1)

    replacement_clock = IncrementingReceiptClock()
    replacement_offerer = RecordingOfferer()
    replacement_ingress = SharedWebSocketIngressV2(
        replacement_offerer,
        recovered_wal_tail_ingest_seq=0,
    )
    replacement_fatal = RecordingFatalCoordinator()
    assert authority.claimed_token_count == 1
    for property_name, replacement in (
        ("clock", replacement_clock),
        ("ingress", replacement_ingress),
        ("fatal_coordinator", replacement_fatal),
    ):
        with pytest.raises(AttributeError):
            setattr(adapter, property_name, replacement)

    transport.request_release.set()
    receipt = await asyncio.wait_for(attempt, timeout=1)
    await adapter.aclose()

    assert len(transport.requests) == 1
    assert offerer.records == [receipt.record]
    assert replacement_offerer.records == []
    assert authority.claimed_token_count == 0
    assert_public_depth_rest_scheduled_attempt_token_consumed_v8(
        token,
        plan=plan,
        schedule_authority=authority,
    )
    assert original_clock.calls == 4
    assert replacement_clock.calls == 0
    assert fatal.causes == []
    assert replacement_fatal.causes == []


@pytest.mark.asyncio
async def test_intra_attempt_wall_regression_retains_acks_then_fatalizes() -> None:
    clock = ScriptedReceiptClock(
        ReceiptTimestamp(100, 1_000),
        ReceiptTimestamp(99, 1_001),
        ReceiptTimestamp(98, 1_002),
        ReceiptTimestamp(97, 1_003),
    )
    adapter, authority, plan, offerer, fatal = _adapter(
        httpx.MockTransport(
            lambda request: httpx.Response(200, content=b"{}", request=request)
        ),
        clock=clock,
    )
    token = _issue(authority)

    with pytest.raises(HttpsRestWallClockRegressionErrorV2) as captured:
        await adapter.capture_attempt(token)
    with pytest.raises(HttpsRestWallClockRegressionErrorV2) as close_failure:
        await adapter.aclose()

    assert close_failure.value is captured.value
    assert fatal.causes == [captured.value]
    assert len(offerer.records) == 1
    payload = _payload(offerer.records[0], plan)
    assert (
        payload.request_started_wall_ms,
        payload.response_first_header_wall_ms,
        payload.attempt_ended_wall_ms,
        payload.completion_admission_wall_ms,
    ) == (100, 99, 98, 97)
    evidence = captured.value.evidence
    assert evidence.intra_attempt_regression is True
    assert evidence.prior_global_regression is False
    assert evidence.ingest_seq == offerer.records[0].ingest_seq
    assert adapter.retained_terminal_admission_count == 1
    retained = adapter.take_terminal_admission_after_failure(token)
    assert retained is not None
    assert retained.record is offerer.records[0]
    assert adapter.retained_terminal_admission_count == 0
    assert adapter.take_terminal_admission_after_failure(token) is None
    assert authority.claimed_token_count == 0
    assert_public_depth_rest_scheduled_attempt_token_consumed_v8(
        token,
        plan=plan,
        schedule_authority=authority,
    )


@pytest.mark.asyncio
async def test_failed_admission_recovery_uses_old_token_identity_after_promotion() -> None:
    clock = ScriptedReceiptClock(
        ReceiptTimestamp(100, 1_000),
        ReceiptTimestamp(99, 1_001),
        ReceiptTimestamp(98, 1_002),
        ReceiptTimestamp(97, 1_003),
    )
    transport = GatedDepthTransport()
    adapter, authority, plan, offerer, fatal = _adapter(transport, clock=clock)
    old_token = _issue(authority)
    attempt = asyncio.create_task(adapter.capture_attempt(old_token))
    await asyncio.wait_for(transport.request_started.wait(), timeout=1)
    [pending_registration] = authority.register_trigger(
        trigger="sequence_gap",
        connection_generation=1,
        symbol_watermarks=(("BTCUSDT", 101),),
    )
    transport.request_release.set()

    with pytest.raises(HttpsRestWallClockRegressionErrorV2):
        await attempt

    promoted_token = authority.issue_attempt(
        registration=pending_registration,
        bridge_attempt=1,
    )
    assert adapter.take_terminal_admission_after_failure(promoted_token) is None
    assert adapter.retained_terminal_admission_count == 1
    retained = adapter.take_terminal_admission_after_failure(old_token)
    assert retained is not None
    assert retained.record is offerer.records[0]
    assert adapter.take_terminal_admission_after_failure(old_token) is None
    with pytest.raises(HttpsRestWallClockRegressionErrorV2):
        await adapter.aclose()

    assert adapter.retained_terminal_admission_count == 0
    assert fatal.causes
    assert validate_public_depth_rest_admission_receipt_v8(
        retained,
        plan=plan,
    ) is retained.record


@pytest.mark.asyncio
async def test_ack_failure_cannot_hide_an_already_admitted_terminal_receipt() -> None:
    adapter, authority, plan, offerer, fatal = _adapter(
        httpx.MockTransport(
            lambda request: httpx.Response(200, content=b"{}", request=request)
        )
    )
    token = _issue(authority)

    def reject_ack(*_args: object, **_kwargs: object) -> None:
        raise RuntimeError("injected scheduler ACK failure")

    with pytest.MonkeyPatch.context() as patch_context:
        patch_context.setattr(
            rest_depth_adapter_module,
            "acknowledge_public_depth_rest_terminal_admission_v8",
            reject_ack,
        )
        with pytest.raises(RuntimeError, match="injected scheduler ACK failure"):
            await adapter.capture_attempt(token)

    assert len(offerer.records) == 1
    assert authority.claimed_token_count == 1
    assert adapter.retained_terminal_admission_count == 1
    retained = adapter.take_terminal_admission_after_failure(token)
    assert retained is not None
    assert retained.record is offerer.records[0]
    assert validate_public_depth_rest_admission_receipt_v8(
        retained,
        plan=plan,
    ) is retained.record
    assert adapter.take_terminal_admission_after_failure(token) is None
    with pytest.raises(RuntimeError, match="injected scheduler ACK failure"):
        await adapter.aclose()

    assert fatal.causes


@pytest.mark.asyncio
async def test_inflight_terminal_receipt_is_not_publicly_recoverable() -> None:
    adapter, authority, _plan_value, offerer, fatal = _adapter(
        httpx.MockTransport(
            lambda request: httpx.Response(200, content=b"{}", request=request)
        )
    )
    token = _issue(authority)
    original_ack = (
        rest_depth_adapter_module.acknowledge_public_depth_rest_terminal_admission_v8
    )
    early_takes: list[PublicDepthRestAdmissionReceiptV8 | None] = []

    def inspect_before_ack(
        token_value: PublicDepthRestScheduledAttemptTokenV8,
        receipt: PublicDepthRestAdmissionReceiptV8,
        *,
        plan: ProvisionalDepthRestQualificationPlanV8,
        schedule_authority: PublicDepthRestScheduleAuthorityV8,
    ) -> None:
        early_takes.append(adapter.take_terminal_admission_after_failure(token))
        original_ack(
            token_value,
            receipt,
            plan=plan,
            schedule_authority=schedule_authority,
        )

    with pytest.MonkeyPatch.context() as patch_context:
        patch_context.setattr(
            rest_depth_adapter_module,
            "acknowledge_public_depth_rest_terminal_admission_v8",
            inspect_before_ack,
        )
        receipt = await adapter.capture_attempt(token)
    await adapter.aclose()

    assert early_takes == [None]
    assert offerer.records == [receipt.record]
    assert adapter.retained_terminal_admission_count == 0
    assert adapter.take_terminal_admission_after_failure(token) is None
    assert fatal.causes == []
    assert adapter.cleanly_closed


@pytest.mark.asyncio
async def test_prior_global_wall_regression_retains_second_row_and_ack() -> None:
    clock = ScriptedReceiptClock(
        ReceiptTimestamp(200, 1_000),
        ReceiptTimestamp(201, 1_001),
        ReceiptTimestamp(202, 1_002),
        ReceiptTimestamp(203, 1_003),
        ReceiptTimestamp(150, 1_004),
        ReceiptTimestamp(151, 1_005),
        ReceiptTimestamp(152, 1_006),
        ReceiptTimestamp(153, 1_007),
    )
    adapter, authority, plan, offerer, fatal = _adapter(
        httpx.MockTransport(
            lambda request: httpx.Response(200, content=b"{}", request=request)
        ),
        clock=clock,
    )
    first_token = _issue(authority)
    first_receipt = await adapter.capture_attempt(first_token)
    [second_registration] = authority.register_trigger(
        trigger="sequence_gap",
        connection_generation=1,
        symbol_watermarks=(("BTCUSDT", 101),),
    )
    second_token = authority.issue_attempt(
        registration=second_registration,
        bridge_attempt=1,
    )

    with pytest.raises(HttpsRestWallClockRegressionErrorV2) as captured:
        await adapter.capture_attempt(second_token)
    with pytest.raises(HttpsRestWallClockRegressionErrorV2) as close_failure:
        await adapter.aclose()

    assert close_failure.value is captured.value
    assert fatal.causes == [captured.value]
    assert len(offerer.records) == 2
    assert first_receipt.record is offerer.records[0]
    assert [record.ingest_seq for record in offerer.records] == [1, 2]
    assert first_receipt.wall_clock_regression is None
    evidence = captured.value.evidence
    assert evidence.intra_attempt_regression is False
    assert evidence.prior_global_regression is True
    assert evidence.prior_global_wall_ms == 203
    assert evidence.completion_admission_wall_ms == 153
    assert adapter.retained_terminal_admission_count == 1
    retained = adapter.take_terminal_admission_after_failure(second_token)
    assert retained is not None
    assert retained.record is offerer.records[1]
    assert adapter.retained_terminal_admission_count == 0
    assert adapter.take_terminal_admission_after_failure(second_token) is None
    assert authority.claimed_token_count == 0
    assert_public_depth_rest_scheduled_attempt_token_consumed_v8(
        second_token,
        plan=plan,
        schedule_authority=authority,
    )


@pytest.mark.asyncio
async def test_intra_attempt_monotonic_regression_rejects_without_terminal_row() -> None:
    clock = ScriptedReceiptClock(
        ReceiptTimestamp(100, 1_000),
        ReceiptTimestamp(101, 999),
        ReceiptTimestamp(102, 1_001),
    )
    adapter, authority, plan, offerer, fatal = _adapter(
        httpx.MockTransport(
            lambda request: httpx.Response(200, content=b"{}", request=request)
        ),
        clock=clock,
    )
    token = _issue(authority)

    with pytest.raises(ValueError, match="monotonically") as captured:
        await adapter.capture_attempt(token)
    with pytest.raises(ValueError, match="monotonically") as close_failure:
        await adapter.aclose()

    assert close_failure.value is captured.value
    assert fatal.causes == [captured.value]
    assert offerer.records == []
    assert authority.claimed_token_count == 1
    assert_public_depth_rest_scheduled_attempt_token_consumed_v8(
        token,
        plan=plan,
        schedule_authority=authority,
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("status", [302, 429])
async def test_depth_http_status_is_retained_once_without_retry(status: int) -> None:
    requests: list[httpx.Request] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(status, content=b"status", request=request)

    adapter, authority, plan, offerer, fatal = _adapter(
        httpx.MockTransport(handler)
    )

    receipt = await adapter.capture_attempt(_issue(authority))
    await adapter.aclose()

    assert len(requests) == 1
    assert offerer.records == [receipt.record]
    payload = _payload(receipt.record, plan)
    assert payload.response_status == status
    assert payload.error_category is PublicDepthRestErrorCategoryV8.HTTP_STATUS
    assert payload.body_bytes() == b"status"
    assert fatal.causes == []


@pytest.mark.asyncio
async def test_depth_network_failure_maps_to_exact_depth_category() -> None:
    requests: list[httpx.Request] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        raise httpx.ConnectError("synthetic connect failure", request=request)

    adapter, authority, plan, offerer, fatal = _adapter(
        httpx.MockTransport(handler)
    )

    receipt = await adapter.capture_attempt(_issue(authority))
    await adapter.aclose()

    assert len(requests) == 1
    assert offerer.records == [receipt.record]
    payload = _payload(receipt.record, plan)
    assert payload.error_category is PublicDepthRestErrorCategoryV8.NETWORK
    assert payload.response_status is None
    assert payload.body_bytes() == b""
    assert fatal.causes == []


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("extra_bytes", "complete", "category"),
    [
        (0, True, None),
        (1, False, PublicDepthRestErrorCategoryV8.BODY_LIMIT),
    ],
)
async def test_depth_body_cap_boundary_is_exact(
    extra_bytes: int,
    complete: bool,
    category: PublicDepthRestErrorCategoryV8 | None,
) -> None:
    body = b"x" * (PUBLIC_DEPTH_REST_MAXIMUM_BODY_BYTES_V8 + extra_bytes)
    adapter, authority, plan, offerer, fatal = _adapter(
        httpx.MockTransport(
            lambda request: httpx.Response(200, content=body, request=request)
        )
    )

    receipt = await adapter.capture_attempt(_issue(authority))
    await adapter.aclose()

    assert offerer.records == [receipt.record]
    payload = _payload(receipt.record, plan)
    assert len(payload.body_bytes()) == PUBLIC_DEPTH_REST_MAXIMUM_BODY_BYTES_V8
    assert payload.payload_complete is complete
    assert payload.error_category is category
    assert fatal.causes == []


@pytest.mark.asyncio
async def test_replayed_depth_token_fails_before_second_http_or_admission() -> None:
    requests: list[httpx.Request] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(200, content=b"{}", request=request)

    adapter, authority, _plan_value, offerer, fatal = _adapter(
        httpx.MockTransport(handler)
    )
    token = _issue(authority)
    receipt = await adapter.capture_attempt(token)

    with pytest.raises(
        RestCaptureOwnershipFailureV2,
        match="foreign, stale, or replayed",
    ) as captured:
        await adapter.capture_attempt(token)
    with pytest.raises(RestCaptureOwnershipFailureV2) as close_failure:
        await adapter.aclose()

    assert close_failure.value is captured.value
    assert fatal.causes == [captured.value]
    assert len(requests) == 1
    assert offerer.records == [receipt.record]


@pytest.mark.asyncio
async def test_adapter_generation_mismatch_rejects_before_claim_or_http() -> None:
    requests: list[httpx.Request] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(200, content=b"{}", request=request)

    adapter, authority, plan, offerer, fatal = _adapter(
        httpx.MockTransport(handler),
        generation=2,
    )
    token = _issue(authority, connection_generation=1)

    with pytest.raises(
        RestCaptureOwnershipFailureV2,
        match="foreign, stale, or replayed",
    ) as captured:
        await adapter.capture_attempt(token)
    with pytest.raises(RestCaptureOwnershipFailureV2):
        await adapter.aclose()
    with pytest.raises(
        PublicDepthRestScheduledAttemptOwnershipErrorV8,
        match="has not been claimed",
    ):
        assert_public_depth_rest_scheduled_attempt_token_consumed_v8(
            token,
            plan=plan,
            schedule_authority=authority,
        )

    assert fatal.causes == [captured.value]
    assert requests == []
    assert offerer.records == []


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "drift",
    ["authorization", "api_key", "query", "host", "method"],
)
async def test_built_request_drift_fails_before_claim_http_or_admission(
    drift: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    requests: list[httpx.Request] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(200, content=b"{}", request=request)

    adapter, authority, plan, offerer, fatal = _adapter(httpx.MockTransport(handler))
    token = _issue(authority)
    binding = adapter._owner.binding
    original_request_spec = binding.request_spec

    def drifted_request_spec(attempt: _DepthRestAttempt) -> _RestRequestSpec:
        spec = original_request_spec(attempt)
        if drift == "authorization":
            return replace(
                spec,
                headers=(*spec.headers, ("authorization", "Bearer forbidden")),
            )
        if drift == "api_key":
            return replace(
                spec,
                headers=(*spec.headers, ("x-mbx-apikey", "forbidden")),
            )
        if drift == "query":
            return replace(
                spec,
                params=(
                    *spec.params,
                    ("signature", "forbidden"),
                    ("timestamp", "1700000000000"),
                ),
            )
        if drift == "host":
            return replace(spec, url="https://example.invalid/fapi/v1/depth")
        if drift == "method":
            return replace(spec, method="POST")
        raise AssertionError("unhandled request-drift test case")

    monkeypatch.setattr(binding, "request_spec", drifted_request_spec)
    with pytest.raises(
        RestCaptureOwnershipFailureV2,
        match="actual depth REST request differs",
    ) as captured:
        await adapter.capture_attempt(token)
    with pytest.raises(
        PublicDepthRestScheduledAttemptOwnershipErrorV8,
        match="has not been claimed",
    ):
        assert_public_depth_rest_scheduled_attempt_token_consumed_v8(
            token,
            plan=plan,
            schedule_authority=authority,
        )
    with pytest.raises(RestCaptureOwnershipFailureV2) as close_failure:
        await adapter.aclose()

    assert close_failure.value is captured.value
    assert fatal.causes == [captured.value]
    assert requests == []
    assert offerer.records == []


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "drift",
    ["authorization", "api_key", "query", "host", "method", "stream_in_place"],
)
async def test_transport_cannot_mutate_sent_request_into_false_retained_proof(
    drift: str,
) -> None:
    requests: list[httpx.Request] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        if drift == "authorization":
            request.headers["authorization"] = "Bearer forbidden"
        elif drift == "api_key":
            request.headers["x-mbx-apikey"] = "forbidden"
        elif drift == "query":
            request.url = request.url.copy_add_param(
                "signature", "forbidden"
            ).copy_add_param("timestamp", "1700000000000")
        elif drift == "host":
            request.url = request.url.copy_with(host="example.invalid")
        elif drift == "method":
            request.method = "POST"
        elif drift == "stream_in_place":
            request.stream.__dict__["_stream"] = b"forbidden-body"
        else:
            raise AssertionError("unhandled transport-drift test case")
        return httpx.Response(200, content=b"{}", request=request)

    adapter, authority, plan, offerer, fatal = _adapter(httpx.MockTransport(handler))
    token = _issue(authority)
    with pytest.raises(
        RestCaptureOwnershipFailureV2,
        match="request mutated before terminal evidence admission",
    ) as captured:
        await adapter.capture_attempt(token)
    assert_public_depth_rest_scheduled_attempt_token_consumed_v8(
        token,
        plan=plan,
        schedule_authority=authority,
    )
    with pytest.raises(RestCaptureOwnershipFailureV2) as close_failure:
        await adapter.aclose()

    assert close_failure.value is captured.value
    assert fatal.causes == [captured.value]
    assert len(requests) == 1
    assert offerer.records == []
    assert authority.claimed_token_count == 1


@pytest.mark.asyncio
async def test_cancellation_during_depth_admission_reuses_task_and_acknowledges() -> None:
    original_offer = SharedWebSocketIngressV2.offer_depth_https_attempt_v8
    admission_started = asyncio.Event()
    admission_release = asyncio.Event()
    admission_calls = 0

    async def delayed_offer(
        self: SharedWebSocketIngressV2,
        *,
        plan: ProvisionalDepthRestQualificationPlanV8,
        session_id: str,
        protocol_hash: str,
        connection_id: str,
        generation: int,
        symbol: str,
        clock: ReceiptClock,
        observation,
        source_logical_key: str,
        cancellation_requested: asyncio.Event | None = None,
    ) -> PublicDepthRestAdmissionReceiptV8:
        nonlocal admission_calls
        admission_calls += 1
        admission_started.set()
        await admission_release.wait()
        return await original_offer(
            self,
            plan=plan,
            session_id=session_id,
            protocol_hash=protocol_hash,
            connection_id=connection_id,
            generation=generation,
            symbol=symbol,
            clock=clock,
            observation=observation,
            source_logical_key=source_logical_key,
            cancellation_requested=cancellation_requested,
        )

    adapter, authority, plan, offerer, fatal = _adapter(
        httpx.MockTransport(
            lambda request: httpx.Response(200, content=b"{}", request=request)
        )
    )
    token = _issue(authority)
    with pytest.MonkeyPatch.context() as patch_context:
        patch_context.setattr(
            SharedWebSocketIngressV2,
            "offer_depth_https_attempt_v8",
            delayed_offer,
        )
        attempt = asyncio.create_task(adapter.capture_attempt(token))
        await asyncio.wait_for(admission_started.wait(), timeout=1)
        attempt.cancel()
        admission_release.set()
        with pytest.raises(asyncio.CancelledError):
            await attempt
    await adapter.aclose()

    assert admission_calls == 1
    assert len(offerer.records) == 1
    assert adapter.retained_terminal_admission_count == 1
    assert not adapter.fully_drained
    assert not adapter.cleanly_closed
    retained = adapter.take_terminal_admission_after_failure(token)
    assert retained is not None
    assert retained.record is offerer.records[0]
    assert adapter.retained_terminal_admission_count == 0
    assert adapter.take_terminal_admission_after_failure(token) is None
    assert adapter.fully_drained
    assert adapter.cleanly_closed
    payload = _payload(offerer.records[0], plan)
    assert payload.admission_cancellation_requested is True
    assert fatal.causes == []
    assert adapter.cleanly_closed
    second = _issue(
        authority,
        registration=token.registration,
        bridge_attempt=2,
    )
    assert second.bridge_attempt == 2


@pytest.mark.asyncio
async def test_cancellation_before_request_start_has_no_terminal_admission() -> None:
    requests: list[httpx.Request] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(200, content=b"{}", request=request)

    adapter, authority, _plan_value, offerer, fatal = _adapter(
        httpx.MockTransport(handler)
    )
    token = _issue(authority)
    attempt = asyncio.create_task(adapter.capture_attempt(token))
    attempt.cancel()
    with pytest.raises(asyncio.CancelledError):
        await attempt
    await adapter.aclose()

    assert requests == []
    assert offerer.records == []
    assert adapter.retained_terminal_admission_count == 0
    assert adapter.take_terminal_admission_after_failure(token) is None
    assert fatal.causes == []
    assert adapter.cleanly_closed


@pytest.mark.asyncio
async def test_four_unrecovered_cancellations_bound_fifth_before_http_io() -> None:
    original_offer = SharedWebSocketIngressV2.offer_depth_https_attempt_v8
    admission_started = asyncio.Event()
    admission_release = asyncio.Event()
    requests: list[httpx.Request] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(200, content=b"{}", request=request)

    async def delayed_offer(
        self: SharedWebSocketIngressV2,
        *,
        plan: ProvisionalDepthRestQualificationPlanV8,
        session_id: str,
        protocol_hash: str,
        connection_id: str,
        generation: int,
        symbol: str,
        clock: ReceiptClock,
        observation,
        source_logical_key: str,
        cancellation_requested: asyncio.Event | None = None,
    ) -> PublicDepthRestAdmissionReceiptV8:
        admission_started.set()
        await admission_release.wait()
        return await original_offer(
            self,
            plan=plan,
            session_id=session_id,
            protocol_hash=protocol_hash,
            connection_id=connection_id,
            generation=generation,
            symbol=symbol,
            clock=clock,
            observation=observation,
            source_logical_key=source_logical_key,
            cancellation_requested=cancellation_requested,
        )

    symbols = ("ADAUSDT", "BTCUSDT", "DOGEUSDT", "ETHUSDT", "SOLUSDT")
    adapter, authority, plan, offerer, fatal = _adapter(
        httpx.MockTransport(handler),
        symbols=symbols,
    )
    registrations = authority.register_trigger(
        trigger="startup",
        connection_generation=1,
        symbol_watermarks=tuple((symbol, 100) for symbol in symbols),
    )
    tokens = [
        _issue(authority, registration=registration)
        for registration in registrations
    ]

    with pytest.MonkeyPatch.context() as patch_context:
        patch_context.setattr(
            SharedWebSocketIngressV2,
            "offer_depth_https_attempt_v8",
            delayed_offer,
        )
        for token in tokens[:4]:
            attempt = asyncio.create_task(adapter.capture_attempt(token))
            await asyncio.wait_for(admission_started.wait(), timeout=1)
            attempt.cancel()
            admission_release.set()
            with pytest.raises(asyncio.CancelledError):
                await attempt
            admission_started = asyncio.Event()
            admission_release = asyncio.Event()

    assert len(requests) == 4
    assert len(offerer.records) == 4
    assert adapter.retained_terminal_admission_count == 4
    with pytest.raises(
        RestCaptureOwnershipFailureV2,
        match="recovery capacity was violated",
    ) as captured:
        await adapter.capture_attempt(tokens[4])
    assert len(requests) == 4
    with pytest.raises(
        PublicDepthRestScheduledAttemptOwnershipErrorV8,
        match="has not been claimed",
    ):
        assert_public_depth_rest_scheduled_attempt_token_consumed_v8(
            tokens[4],
            plan=plan,
            schedule_authority=authority,
        )

    recovered = tuple(
        adapter.take_terminal_admission_after_failure(token)
        for token in tokens[:4]
    )
    assert all(receipt is not None for receipt in recovered)
    assert adapter.retained_terminal_admission_count == 0
    with pytest.raises(RestCaptureOwnershipFailureV2) as close_failure:
        await adapter.aclose()

    assert close_failure.value is captured.value
    assert fatal.causes == [captured.value]


@pytest.mark.asyncio
async def test_generation_advance_waits_for_claimed_terminal_admission() -> None:
    original_offer = SharedWebSocketIngressV2.offer_depth_https_attempt_v8
    queue_accepted = asyncio.Event()
    return_release = asyncio.Event()
    admission_calls = 0

    async def accepted_then_delayed_offer(
        self: SharedWebSocketIngressV2,
        *,
        plan: ProvisionalDepthRestQualificationPlanV8,
        session_id: str,
        protocol_hash: str,
        connection_id: str,
        generation: int,
        symbol: str,
        clock: ReceiptClock,
        observation,
        source_logical_key: str,
        cancellation_requested: asyncio.Event | None = None,
    ) -> PublicDepthRestAdmissionReceiptV8:
        nonlocal admission_calls
        admission_calls += 1
        receipt = await original_offer(
            self,
            plan=plan,
            session_id=session_id,
            protocol_hash=protocol_hash,
            connection_id=connection_id,
            generation=generation,
            symbol=symbol,
            clock=clock,
            observation=observation,
            source_logical_key=source_logical_key,
            cancellation_requested=cancellation_requested,
        )
        queue_accepted.set()
        await return_release.wait()
        return receipt

    adapter, authority, _plan_value, offerer, fatal = _adapter(
        httpx.MockTransport(
            lambda request: httpx.Response(200, content=b"{}", request=request)
        )
    )
    token = _issue(authority)
    with pytest.MonkeyPatch.context() as patch_context:
        patch_context.setattr(
            SharedWebSocketIngressV2,
            "offer_depth_https_attempt_v8",
            accepted_then_delayed_offer,
        )
        attempt = asyncio.create_task(adapter.capture_attempt(token))
        await asyncio.wait_for(queue_accepted.wait(), timeout=1)
        with pytest.raises(
            PublicDepthRestScheduledAttemptOwnershipErrorV8,
            match="must drain",
        ):
            authority.retire_current_generation(
                session_id=_SESSION_ID,
                protocol_hash=_PROTOCOL_HASH,
                connection_id=_connection_id(1),
                connection_generation=1,
            )
        return_release.set()
        receipt = await attempt
    authority.retire_current_generation(
        session_id=_SESSION_ID,
        protocol_hash=_PROTOCOL_HASH,
        connection_id=_connection_id(1),
        connection_generation=1,
    )
    authority.advance_connection_generation(
        2,
        session_id=_SESSION_ID,
        protocol_hash=_PROTOCOL_HASH,
        connection_id=_connection_id(2),
    )
    await adapter.aclose()

    assert fatal.causes == []
    assert admission_calls == 1
    assert offerer.records == [receipt.record]
    assert adapter.closed
    assert adapter.fully_drained
    assert adapter.cleanly_closed


@pytest.mark.asyncio
async def test_depth_direct_concurrency_is_four_and_fifth_never_sends() -> None:
    entered = asyncio.Event()
    release = asyncio.Event()
    requests: list[httpx.Request] = []
    active = 0

    async def handler(request: httpx.Request) -> httpx.Response:
        nonlocal active
        requests.append(request)
        active += 1
        if active == 4:
            entered.set()
        try:
            await release.wait()
            return httpx.Response(200, content=b"{}", request=request)
        finally:
            active -= 1

    symbols = ("ADAUSDT", "BTCUSDT", "DOGEUSDT", "ETHUSDT", "SOLUSDT")
    adapter, authority, _plan_value, offerer, fatal = _adapter(
        httpx.MockTransport(handler),
        symbols=symbols,
    )
    registrations = authority.register_trigger(
        trigger="startup",
        connection_generation=1,
        symbol_watermarks=tuple((symbol, 100) for symbol in symbols),
    )
    tokens = [
        _issue(authority, registration=registration)
        for registration in registrations
    ]
    attempts = [
        asyncio.create_task(adapter.capture_attempt(token)) for token in tokens[:4]
    ]
    await asyncio.wait_for(entered.wait(), timeout=1)

    with pytest.raises(RestCaptureOwnershipFailureV2) as captured:
        await adapter.capture_attempt(tokens[4])
    with pytest.raises(
        PublicDepthRestScheduledAttemptOwnershipErrorV8,
        match="has not been claimed",
    ):
        assert_public_depth_rest_scheduled_attempt_token_consumed_v8(
            tokens[4],
            plan=adapter.plan,
            schedule_authority=authority,
        )
    release.set()
    receipts = await asyncio.gather(*attempts)
    with pytest.raises(RestCaptureOwnershipFailureV2) as close_failure:
        await adapter.aclose()

    assert close_failure.value is captured.value
    assert fatal.causes == [captured.value]
    assert len(requests) == 4
    assert len(offerer.records) == 4
    assert {receipt.record for receipt in receipts} == set(offerer.records)
