from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from dataclasses import dataclass, field, replace

import httpx
import pytest

from signalbot.capture.receipts import ReceiptClock, ReceiptTimestamp
from signalbot.r4b_v2.capture import rest_adapter as rest_adapter_module
from signalbot.r4b_v2.capture import rest_attempt_owner as rest_owner_module
from signalbot.r4b_v2.capture.batching import (
    BatchPolicyV2,
    BoundedBatchHandoffV2,
    CaptureQueueAdmissionReceiptV2,
    QueuedRawRecordV2,
)
from signalbot.r4b_v2.capture.models import RawRecordV2, TransportV2, VenueV2
from signalbot.r4b_v2.capture.plans import ProvisionalPromotingRestCapturePlanV2
from signalbot.r4b_v2.capture.rest import (
    PublicOiRestAttemptPayloadV2,
    PublicOiRestErrorCategoryV2,
    PublicOiRestMissedSlotV2,
    PublicOiRestTerminalObservationV2,
)
from signalbot.r4b_v2.capture.rest_adapter import (
    PublicOpenInterestRestCaptureAdapterV2,
    RestCaptureAdapterClosedV2,
    RestCaptureOwnershipFailureV2,
)
from signalbot.r4b_v2.capture.rest_scheduler import (
    PublicOiScheduledAttemptTokenV2,
    PublicOpenInterestRestSchedulerV2,
    _mint_public_oi_scheduled_attempt_token_v2,
    assert_public_oi_scheduled_attempt_token_consumed_v2,
)
from signalbot.r4b_v2.capture.websocket import (
    HttpsRestWallClockRegressionErrorV2,
    PublicOiAdmissionReceiptV2,
    SharedWebSocketIngressV2,
    validate_public_oi_admission_receipt_v2,
)

_SLOT = 1_700_000_000_000
_PROTOCOL_HASH = "a" * 64


def _admission_handoff() -> BoundedBatchHandoffV2:
    return BoundedBatchHandoffV2(
        BatchPolicyV2(
            max_records=64,
            max_encoded_bytes=1_000_000,
            max_linger_us=1_000,
            queue_max_events=1_024,
            queue_max_encoded_bytes=16_000_000,
            low_water_events=0,
            low_water_encoded_bytes=0,
            qualification_id="rest-adapter-recording-handoff",
        )
    )


@dataclass(slots=True)
class RecordingOfferer:
    records: list[RawRecordV2] = field(default_factory=list)
    handoff: BoundedBatchHandoffV2 = field(default_factory=_admission_handoff)

    def offer(self, record: RawRecordV2) -> QueuedRawRecordV2:
        queued_record = self.handoff.offer(record)
        self.records.append(record)
        return queued_record

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
class InvalidQueuedOfferer:
    records: list[RawRecordV2] = field(default_factory=list)
    handoff: BoundedBatchHandoffV2 = field(default_factory=_admission_handoff)

    def offer(self, record: RawRecordV2) -> object:
        del record
        raise AssertionError("ordinary offer is not used by this HTTPS test")

    def offer_with_admission_receipt(self, record: RawRecordV2) -> object:
        accepted = self.handoff.offer_with_admission_receipt(record)
        self.records.append(record)
        return QueuedRawRecordV2.encode(
            accepted.record,
            enqueued_monotonic_ns=accepted.queued_record.enqueued_monotonic_ns,
        )

    def validate_queue_admission_receipt_v2(
        self,
        receipt: CaptureQueueAdmissionReceiptV2,
    ) -> QueuedRawRecordV2:
        del receipt
        raise AssertionError("synthetic queued record must fail before validation")


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
    def __init__(self, *, first_wall_ms: int = _SLOT + 1) -> None:
        self._next_wall_ms = first_wall_ms
        self._next_monotonic_ns = 10_000

    def capture(self) -> ReceiptTimestamp:
        receipt = ReceiptTimestamp(
            self._next_wall_ms,
            self._next_monotonic_ns,
        )
        self._next_wall_ms += 1
        self._next_monotonic_ns += 1
        return receipt


class ScriptedReceiptClock:
    def __init__(self, receipts: tuple[ReceiptTimestamp, ...]) -> None:
        self._receipts = list(receipts)

    def capture(self) -> ReceiptTimestamp:
        if not self._receipts:
            raise AssertionError("scripted receipt clock was exhausted")
        return self._receipts.pop(0)


class CountingCloseTransport(httpx.AsyncBaseTransport):
    def __init__(self) -> None:
        self.close_calls = 0
        self.close_started = asyncio.Event()
        self.close_release = asyncio.Event()

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, content=b"{}", request=request)

    async def aclose(self) -> None:
        self.close_calls += 1
        self.close_started.set()
        await self.close_release.wait()


class CancellationSuppressingCloseTransport(httpx.AsyncBaseTransport):
    def __init__(self) -> None:
        self.close_calls = 0
        self.close_started = asyncio.Event()
        self.close_release = asyncio.Event()

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, content=b"{}", request=request)

    async def aclose(self) -> None:
        self.close_calls += 1
        self.close_started.set()
        while not self.close_release.is_set():
            try:
                await self.close_release.wait()
            except asyncio.CancelledError:
                continue


class PartialReadFailureStream(httpx.AsyncByteStream):
    def __init__(self, prefix: bytes) -> None:
        self.prefix = prefix
        self.close_calls = 0

    async def __aiter__(self):  # type: ignore[no-untyped-def]
        yield self.prefix
        raise httpx.ReadError("synthetic partial response failure")

    async def aclose(self) -> None:
        self.close_calls += 1


class FailingResponseCloseStream(httpx.AsyncByteStream):
    def __init__(self, body: bytes) -> None:
        self.body = body
        self.close_calls = 0

    async def __aiter__(self):  # type: ignore[no-untyped-def]
        yield self.body

    async def aclose(self) -> None:
        self.close_calls += 1
        raise httpx.ReadError("synthetic response close failure")


class BlockingReadStream(httpx.AsyncByteStream):
    def __init__(self, prefix: bytes) -> None:
        self.prefix = prefix
        self.prefix_consumed = asyncio.Event()
        self.release = asyncio.Event()
        self.close_calls = 0

    async def __aiter__(self) -> AsyncIterator[bytes]:
        yield self.prefix
        self.prefix_consumed.set()
        await self.release.wait()
        yield b"too-late"

    async def aclose(self) -> None:
        self.close_calls += 1


class CountingResponseCloseStream(httpx.AsyncByteStream):
    def __init__(self, body: bytes) -> None:
        self.body = body
        self.close_calls = 0

    async def __aiter__(self) -> AsyncIterator[bytes]:
        yield self.body

    async def aclose(self) -> None:
        self.close_calls += 1


@dataclass(slots=True)
class MutableLoopTime:
    now: float = 0.0

    def __call__(self) -> float:
        return self.now


class DeadlineAdvancingReadStream(httpx.AsyncByteStream):
    def __init__(self, body: bytes, loop_time: MutableLoopTime) -> None:
        self.body = body
        self.loop_time = loop_time
        self.close_calls = 0

    async def __aiter__(self) -> AsyncIterator[bytes]:
        self.loop_time.now = 1.0
        yield self.body

    async def aclose(self) -> None:
        self.close_calls += 1


class NonBytesReadFailureStream(httpx.AsyncByteStream):
    def __init__(self) -> None:
        self.close_calls = 0

    async def __aiter__(self) -> AsyncIterator[bytes]:
        yield "not-bytes"  # type: ignore[misc]

    async def aclose(self) -> None:
        self.close_calls += 1


class GatedSendTransport(httpx.AsyncBaseTransport):
    def __init__(self) -> None:
        self.requests: list[httpx.Request] = []
        self.request_started = asyncio.Event()
        self.request_release = asyncio.Event()
        self.close_calls = 0

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        self.request_started.set()
        await self.request_release.wait()
        return httpx.Response(200, content=b"{}", request=request)

    async def aclose(self) -> None:
        self.close_calls += 1


def _plan(
    symbols: tuple[str, ...] = ("BTCUSDT",),
) -> ProvisionalPromotingRestCapturePlanV2:
    return ProvisionalPromotingRestCapturePlanV2(
        name="v2-usdm-public-rest-oi-promoting-abc",
        venue=VenueV2.USDM_FUTURES,
        route_id="usdm_public_rest",
        method="GET",
        endpoint="/fapi/v1/openInterest",
        symbols=symbols,
    )


def _adapter(
    transport: httpx.AsyncBaseTransport,
    *,
    symbols: tuple[str, ...] = ("BTCUSDT",),
    clock: ReceiptClock | None = None,
    plan: ProvisionalPromotingRestCapturePlanV2 | None = None,
) -> tuple[
    PublicOpenInterestRestCaptureAdapterV2,
    PublicOpenInterestRestSchedulerV2,
    ProvisionalPromotingRestCapturePlanV2,
    RecordingOfferer,
    RecordingFatalCoordinator,
]:
    selected_plan = _plan(symbols) if plan is None else plan
    offerer = RecordingOfferer()
    fatal = RecordingFatalCoordinator()
    ingress = SharedWebSocketIngressV2(
        offerer,
        recovered_wal_tail_ingest_seq=0,
    )
    adapter = PublicOpenInterestRestCaptureAdapterV2(
        selected_plan,
        session_id="session-rest-adapter-test",
        protocol_hash=_PROTOCOL_HASH,
        connection_id="rest-connection-test",
        generation=1,
        clock=IncrementingReceiptClock() if clock is None else clock,
        ingress=ingress,
        fatal_coordinator=fatal,
        transport=transport,
    )
    scheduler = PublicOpenInterestRestSchedulerV2(selected_plan, adapter)
    return adapter, scheduler, selected_plan, offerer, fatal


def _payload(
    record: RawRecordV2,
    plan: ProvisionalPromotingRestCapturePlanV2,
) -> PublicOiRestAttemptPayloadV2:
    return PublicOiRestAttemptPayloadV2.from_canonical_bytes(
        record.payload_bytes(),
        plan=plan,
    )


def _token(
    scheduler: PublicOpenInterestRestSchedulerV2,
    *,
    poll_cycle_seq: int = 1,
    symbol_ordinal: int = 0,
    scheduled_slot_wall_ms: int = _SLOT,
) -> PublicOiScheduledAttemptTokenV2:
    plan = scheduler.plan
    return _mint_public_oi_scheduled_attempt_token_v2(
        plan,
        schedule_authority=scheduler.schedule_authority,
        symbol=plan.symbols[symbol_ordinal],
        poll_cycle_seq=poll_cycle_seq,
        symbol_ordinal=symbol_ordinal,
        scheduled_slot_wall_ms=scheduled_slot_wall_ms,
    )


async def _wait_for_full_drain(
    adapter: PublicOpenInterestRestCaptureAdapterV2,
) -> None:
    for _ in range(100):
        if adapter.fully_drained:
            return
        await asyncio.sleep(0)
    raise AssertionError("adapter retained a nonterminal owner after test release")


@pytest.mark.asyncio
async def test_exact_keyless_request_and_binary_200_are_retained_once() -> None:
    requests: list[httpx.Request] = []
    body = b"\x00\xffbinary-open-interest"

    async def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(
            200,
            headers={"content-type": "application/octet-stream"},
            content=body,
            request=request,
        )

    adapter, scheduler, plan, offerer, fatal = _adapter(httpx.MockTransport(handler))
    receipt = await adapter.capture_attempt(_token(scheduler))
    record = receipt.record
    await adapter.aclose()

    assert len(requests) == 1
    request = requests[0]
    assert request.method == "GET"
    assert request.url.scheme == "https"
    assert request.url.host == "fapi.binance.com"
    assert request.url.path == "/fapi/v1/openInterest"
    assert tuple(request.url.params.multi_items()) == (("symbol", "BTCUSDT"),)
    assert request.headers["accept"] == "application/json"
    assert request.headers["accept-encoding"] == "identity"
    assert request.headers["user-agent"] == "binance-signalbot-r4b-v2-capture/1"
    assert "authorization" not in request.headers
    assert "x-mbx-apikey" not in request.headers
    assert "signature" not in request.url.params
    assert record.transport is TransportV2.HTTPS
    assert record.frame_seq is None
    assert record.source_logical_key == "openInterest:BTCUSDT"
    assert offerer.records == [record]
    assert type(receipt) is PublicOiAdmissionReceiptV2
    assert validate_public_oi_admission_receipt_v2(receipt) is record
    assert receipt.accepted_ingest_seq == record.ingest_seq
    payload = _payload(record, plan)
    assert payload.body_bytes() == body
    assert payload.payload_complete is True
    assert payload.response_status == 200
    assert payload.error_category is None
    assert payload.admission_cancellation_requested is False
    assert payload.attempt_ended_monotonic_ns <= payload.completion_admission_monotonic_ns
    assert fatal.causes == []


@pytest.mark.asyncio
async def test_scheduled_attempt_tokens_are_factory_sealed_and_slot_bounded() -> None:
    adapter, scheduler, plan, _offerer, fatal = _adapter(
        httpx.MockTransport(lambda request: httpx.Response(200, content=b"{}", request=request))
    )
    token = _token(scheduler)

    with pytest.raises(TypeError, match="only be created by the OI scheduler"):
        PublicOiScheduledAttemptTokenV2(
            plan=plan,
            schedule_authority=scheduler.schedule_authority,
            symbol="BTCUSDT",
            poll_cycle_seq=1,
            symbol_ordinal=0,
            scheduled_slot_wall_ms=_SLOT,
            attempt=1,
            _factory_token=object(),
        )
    with pytest.raises(TypeError, match="only be created by the OI scheduler"):
        replace(token)
    with pytest.raises(ValueError, match="UTC-epoch aligned"):
        _mint_public_oi_scheduled_attempt_token_v2(
            plan,
            schedule_authority=scheduler.schedule_authority,
            symbol="BTCUSDT",
            poll_cycle_seq=1,
            symbol_ordinal=0,
            scheduled_slot_wall_ms=_SLOT + 1,
        )
    await adapter.aclose()
    assert fatal.causes == []


@pytest.mark.asyncio
async def test_token_from_second_scheduler_is_rejected_before_http() -> None:
    requests: list[httpx.Request] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(200, content=b"{}", request=request)

    shared_plan = _plan()
    adapter, scheduler, adapter_plan, offerer, fatal = _adapter(
        httpx.MockTransport(handler),
        plan=shared_plan,
    )
    foreign_adapter, foreign_scheduler, foreign_plan, foreign_offerer, foreign_fatal = _adapter(
        httpx.MockTransport(handler), plan=shared_plan
    )
    assert foreign_plan is adapter_plan
    assert foreign_scheduler.schedule_authority is not scheduler.schedule_authority

    with pytest.raises(
        RestCaptureOwnershipFailureV2,
        match="foreign or replayed schedule token",
    ) as captured:
        await adapter.capture_attempt(_token(foreign_scheduler))
    with pytest.raises(TypeError, match="positional-only"):
        await adapter.capture_attempt(token=_token(scheduler))  # type: ignore[call-arg]
    with pytest.raises(RestCaptureOwnershipFailureV2) as close_failure:
        await adapter.aclose()
    await foreign_adapter.aclose()

    assert requests == []
    assert offerer.records == []
    assert foreign_offerer.records == []
    assert close_failure.value is captured.value
    assert fatal.causes == [captured.value]
    assert foreign_fatal.causes == []


@pytest.mark.asyncio
async def test_adapter_rejects_second_scheduler_binding_before_http() -> None:
    requests: list[httpx.Request] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(200, content=b"{}", request=request)

    adapter, _scheduler, plan, offerer, fatal = _adapter(httpx.MockTransport(handler))

    with pytest.raises(
        RestCaptureOwnershipFailureV2,
        match="bound more than once",
    ) as captured:
        PublicOpenInterestRestSchedulerV2(plan, adapter)
    with pytest.raises(RestCaptureOwnershipFailureV2) as close_failure:
        await adapter.aclose()

    assert requests == []
    assert offerer.records == []
    assert close_failure.value is captured.value
    assert fatal.causes == [captured.value]


@pytest.mark.asyncio
async def test_sequential_token_replay_never_sends_or_admits_twice() -> None:
    requests: list[httpx.Request] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(200, content=b"{}", request=request)

    adapter, scheduler, _plan_value, offerer, fatal = _adapter(
        httpx.MockTransport(handler)
    )
    token = _token(scheduler)

    receipt = await adapter.capture_attempt(token)
    with pytest.raises(
        RestCaptureOwnershipFailureV2,
        match="foreign or replayed schedule token",
    ) as replay:
        await adapter.capture_attempt(token)
    with pytest.raises(RestCaptureOwnershipFailureV2) as close_failure:
        await adapter.aclose()

    assert len(requests) == 1
    assert offerer.records == [receipt.record]
    assert close_failure.value is replay.value
    assert fatal.causes == [replay.value]


@pytest.mark.asyncio
async def test_concurrent_token_replay_claims_before_any_extra_http_or_admission() -> None:
    request_started = asyncio.Event()
    request_release = asyncio.Event()
    requests: list[httpx.Request] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        request_started.set()
        await request_release.wait()
        return httpx.Response(200, content=b"{}", request=request)

    adapter, scheduler, _plan_value, offerer, fatal = _adapter(
        httpx.MockTransport(handler)
    )
    token = _token(scheduler)
    owner = asyncio.create_task(adapter.capture_attempt(token))
    await asyncio.wait_for(request_started.wait(), timeout=1)

    with pytest.raises(
        RestCaptureOwnershipFailureV2,
        match="foreign or replayed schedule token",
    ) as replay:
        await adapter.capture_attempt(token)
    request_release.set()
    receipt = await owner
    with pytest.raises(RestCaptureOwnershipFailureV2) as close_failure:
        await adapter.aclose()

    assert len(requests) == 1
    assert offerer.records == [receipt.record]
    assert close_failure.value is replay.value
    assert fatal.causes == [replay.value]


@pytest.mark.asyncio
async def test_invalid_post_accept_proof_fatalizes_without_duplicate_admission() -> None:
    plan = _plan()
    offerer = InvalidQueuedOfferer()
    fatal = RecordingFatalCoordinator()
    ingress = SharedWebSocketIngressV2(
        offerer,
        recovered_wal_tail_ingest_seq=0,
    )
    adapter = PublicOpenInterestRestCaptureAdapterV2(
        plan,
        session_id="session-invalid-queue-result",
        protocol_hash=_PROTOCOL_HASH,
        connection_id="rest-connection-test",
        generation=1,
        clock=IncrementingReceiptClock(),
        ingress=ingress,
        fatal_coordinator=fatal,
        transport=httpx.MockTransport(
            lambda request: httpx.Response(200, content=b"{}", request=request)
        ),
    )
    scheduler = PublicOpenInterestRestSchedulerV2(plan, adapter)

    with pytest.raises(TypeError, match="exact queue-admission receipt") as captured:
        await adapter.capture_attempt(_token(scheduler))
    with pytest.raises(TypeError) as close_failure:
        await adapter.aclose()

    assert close_failure.value is captured.value
    assert fatal.causes == [captured.value]
    assert len(offerer.records) == 1
    assert offerer.handoff.accepted_tail_ingest_seq == 1
    assert offerer.handoff.current_events == 1
    offerer.handoff.discard_all()


@pytest.mark.asyncio
@pytest.mark.parametrize("status", [418, 429])
async def test_http_status_is_retained_and_capture_continues(status: int) -> None:
    body = f"status-{status}".encode()
    transport = httpx.MockTransport(
        lambda request: httpx.Response(status, content=body, request=request)
    )
    adapter, scheduler, plan, offerer, fatal = _adapter(transport)

    receipt = await adapter.capture_attempt(_token(scheduler))
    record = receipt.record
    await adapter.aclose()

    payload = _payload(record, plan)
    assert payload.response_status == status
    assert payload.error_category is PublicOiRestErrorCategoryV2.HTTP_STATUS
    assert payload.payload_complete is True
    assert payload.body_bytes() == body
    assert offerer.records == [record]
    assert fatal.causes == []


@pytest.mark.asyncio
async def test_body_limit_retains_exact_prefix_and_capture_continues() -> None:
    body = bytes(range(256)) * 17
    transport = httpx.MockTransport(
        lambda request: httpx.Response(200, content=body, request=request)
    )
    adapter, scheduler, plan, offerer, fatal = _adapter(transport)

    receipt = await adapter.capture_attempt(_token(scheduler))
    record = receipt.record
    await adapter.aclose()

    payload = _payload(record, plan)
    assert payload.error_category is PublicOiRestErrorCategoryV2.BODY_LIMIT
    assert payload.payload_complete is False
    assert payload.body_bytes() == body[:4096]
    assert offerer.records == [record]
    assert fatal.causes == []


@pytest.mark.asyncio
async def test_missed_slot_does_not_send_retain_or_fatalize() -> None:
    requests: list[httpx.Request] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(200, content=b"{}", request=request)

    clock = IncrementingReceiptClock(first_wall_ms=_SLOT + 5_000)
    adapter, scheduler, _plan_value, offerer, fatal = _adapter(
        httpx.MockTransport(handler),
        clock=clock,
    )

    with pytest.raises(PublicOiRestMissedSlotV2):
        await adapter.capture_attempt(_token(scheduler))
    await adapter.aclose()

    assert requests == []
    assert offerer.records == []
    assert fatal.causes == []


@pytest.mark.asyncio
async def test_caller_cancellation_after_start_retains_exactly_one_cancelled_attempt() -> None:
    request_started = asyncio.Event()
    request_release = asyncio.Event()

    async def handler(request: httpx.Request) -> httpx.Response:
        request_started.set()
        await request_release.wait()
        return httpx.Response(200, content=b"too-late", request=request)

    adapter, scheduler, plan, offerer, fatal = _adapter(httpx.MockTransport(handler))
    attempt = asyncio.create_task(adapter.capture_attempt(_token(scheduler)))
    await asyncio.wait_for(request_started.wait(), timeout=1)
    attempt.cancel()
    attempt.cancel()
    with pytest.raises(asyncio.CancelledError):
        await attempt
    request_release.set()
    await adapter.aclose()

    assert len(offerer.records) == 1
    payload = _payload(offerer.records[0], plan)
    assert payload.error_category is PublicOiRestErrorCategoryV2.CANCELLED
    assert payload.admission_cancellation_requested is True
    assert payload.response_status is None
    assert payload.payload_complete is False
    assert fatal.causes == []


@pytest.mark.asyncio
async def test_direct_concurrency_never_exceeds_four_and_fifth_never_sends() -> None:
    entered = asyncio.Event()
    release = asyncio.Event()
    active = 0
    maximum_active = 0
    request_count = 0

    async def handler(request: httpx.Request) -> httpx.Response:
        nonlocal active, maximum_active, request_count
        request_count += 1
        active += 1
        maximum_active = max(maximum_active, active)
        if active == 4:
            entered.set()
        try:
            await release.wait()
            return httpx.Response(200, content=b"{}", request=request)
        finally:
            active -= 1

    symbols = ("ADAUSDT", "BTCUSDT", "ETHUSDT", "SOLUSDT")
    adapter, scheduler, _plan_value, _offerer, fatal = _adapter(
        httpx.MockTransport(handler),
        symbols=symbols,
    )
    attempts = [
        asyncio.create_task(adapter.capture_attempt(_token(scheduler, symbol_ordinal=ordinal)))
        for ordinal, _symbol in enumerate(symbols)
    ]
    await asyncio.wait_for(entered.wait(), timeout=1)

    with pytest.raises(RestCaptureOwnershipFailureV2):
        await adapter.capture_attempt(_token(scheduler, poll_cycle_seq=2))
    release.set()
    await asyncio.gather(*attempts)
    with pytest.raises(RestCaptureOwnershipFailureV2):
        await adapter.aclose()

    assert request_count == 4
    assert maximum_active == 4
    assert len(fatal.causes) == 1


@pytest.mark.asyncio
async def test_repeated_cancellation_bounds_suppressing_io_without_detaching_owner(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(rest_adapter_module, "_MILLISECONDS_PER_SECOND", 400_000)
    request_started = asyncio.Event()
    request_release = asyncio.Event()
    requests: list[httpx.Request] = []
    response_stream = CountingResponseCloseStream(b"{}")

    async def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        request_started.set()
        while not request_release.is_set():
            try:
                await request_release.wait()
            except asyncio.CancelledError:
                continue
        return httpx.Response(200, stream=response_stream, request=request)

    adapter, scheduler, _plan_value, offerer, fatal = _adapter(
        httpx.MockTransport(handler)
    )
    attempt = asyncio.create_task(adapter.capture_attempt(_token(scheduler)))
    await asyncio.wait_for(request_started.wait(), timeout=1)

    async def repeat_cancellation() -> None:
        for _ in range(20):
            attempt.cancel()
            await asyncio.sleep(0)

    cancellation = asyncio.create_task(repeat_cancellation())
    with pytest.raises(
        RestCaptureOwnershipFailureV2,
        match="bounded cleanup wait",
    ) as captured:
        await asyncio.wait_for(attempt, timeout=1)
    await cancellation

    assert len(requests) == 1
    assert len(offerer.records) == 1
    assert adapter.owned_io_task_count == 1
    assert adapter.pending_owner_task_count == 1
    assert not adapter.fully_drained
    assert adapter.ownership_dirty
    assert not adapter.cleanly_closed
    assert fatal.causes == [captured.value]

    started = asyncio.get_running_loop().time()
    with pytest.raises(RestCaptureOwnershipFailureV2) as close_failure:
        await adapter.aclose()
    assert asyncio.get_running_loop().time() - started < 0.2
    assert close_failure.value is captured.value
    assert len(requests) == 1
    assert len(offerer.records) == 1
    assert not adapter.fully_drained

    request_release.set()
    await _wait_for_full_drain(adapter)
    assert response_stream.close_calls == 1
    assert adapter.fully_drained
    assert adapter.ownership_dirty
    assert not adapter.cleanly_closed


@pytest.mark.asyncio
async def test_aclose_is_bounded_under_repeated_cancellation_and_stuck_admission(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(rest_adapter_module, "_MILLISECONDS_PER_SECOND", 400_000)
    original_offer = SharedWebSocketIngressV2.offer_https_attempt
    admission_started = asyncio.Event()
    admission_release = asyncio.Event()
    admission_calls = 0
    requests: list[httpx.Request] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(200, content=b"{}", request=request)

    async def suppressing_offer(
        self: SharedWebSocketIngressV2,
        *,
        plan: ProvisionalPromotingRestCapturePlanV2,
        session_id: str,
        protocol_hash: str,
        connection_id: str,
        generation: int,
        symbol: str,
        clock: ReceiptClock,
        observation: PublicOiRestTerminalObservationV2,
        source_logical_key: str,
        cancellation_requested: asyncio.Event | None = None,
    ) -> PublicOiAdmissionReceiptV2:
        nonlocal admission_calls
        admission_calls += 1
        admission_started.set()
        while not admission_release.is_set():
            try:
                await admission_release.wait()
            except asyncio.CancelledError:
                continue
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

    monkeypatch.setattr(
        SharedWebSocketIngressV2,
        "offer_https_attempt",
        suppressing_offer,
    )
    adapter, scheduler, _plan_value, offerer, fatal = _adapter(
        httpx.MockTransport(handler)
    )
    attempt = asyncio.create_task(adapter.capture_attempt(_token(scheduler)))
    await asyncio.wait_for(admission_started.wait(), timeout=1)
    closer = asyncio.create_task(adapter.aclose())
    await asyncio.sleep(0)
    await asyncio.sleep(0)
    assert adapter.pending_owner_task_count >= 3

    started = asyncio.get_running_loop().time()
    for _ in range(20):
        closer.cancel()
        attempt.cancel()
        await asyncio.sleep(0)
    with pytest.raises(RestCaptureOwnershipFailureV2) as close_failure:
        await asyncio.wait_for(closer, timeout=1)
    assert asyncio.get_running_loop().time() - started < 0.2
    with pytest.raises(RestCaptureOwnershipFailureV2):
        await asyncio.wait_for(attempt, timeout=1)

    assert len(requests) == 1
    assert admission_calls == 1
    assert offerer.records == []
    assert adapter.owned_admission_task_count == 1
    assert adapter.pending_owner_task_count == 1
    assert not adapter.fully_drained
    assert adapter.ownership_dirty
    assert not adapter.cleanly_closed
    assert fatal.causes == [close_failure.value]

    admission_release.set()
    await _wait_for_full_drain(adapter)
    assert len(requests) == 1
    assert admission_calls == 1
    assert len(offerer.records) == 1
    assert adapter.fully_drained
    assert adapter.ownership_dirty
    assert not adapter.cleanly_closed


@pytest.mark.asyncio
async def test_aclose_bounds_cancellation_suppressing_client_close_owner(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(rest_adapter_module, "_MILLISECONDS_PER_SECOND", 400_000)
    transport = CancellationSuppressingCloseTransport()
    adapter, _scheduler, _plan_value, offerer, fatal = _adapter(transport)

    started = asyncio.get_running_loop().time()
    with pytest.raises(
        RestCaptureOwnershipFailureV2,
        match="client close exceeded",
    ) as captured:
        await asyncio.wait_for(adapter.aclose(), timeout=1)
    assert asyncio.get_running_loop().time() - started < 0.2

    assert transport.close_calls == 1
    assert offerer.records == []
    assert adapter.pending_owner_task_count == 1
    assert not adapter.fully_drained
    assert adapter.ownership_dirty
    assert not adapter.cleanly_closed
    assert fatal.causes == [captured.value]

    transport.close_release.set()
    await _wait_for_full_drain(adapter)
    assert adapter.fully_drained
    assert adapter.ownership_dirty
    assert not adapter.cleanly_closed


@pytest.mark.asyncio
async def test_aclose_is_idempotent_and_calls_transport_close_once() -> None:
    transport = CountingCloseTransport()
    adapter, _scheduler, _plan_value, _offerer, fatal = _adapter(transport)
    assert adapter.accepting_attempts

    first = asyncio.create_task(adapter.aclose())
    await asyncio.wait_for(transport.close_started.wait(), timeout=1)
    assert not adapter.accepting_attempts
    second = asyncio.create_task(adapter.aclose())
    transport.close_release.set()
    await asyncio.gather(first, second)
    await adapter.aclose()

    assert transport.close_calls == 1
    assert fatal.causes == []
    assert adapter.fully_drained
    assert adapter.pending_owner_task_count == 0
    assert not adapter.ownership_dirty
    assert adapter.cleanly_closed
    assert not adapter.accepting_attempts


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("failure_factory", "expected_category"),
    [
        (
            lambda request: httpx.ConnectError(
                "synthetic connect failure",
                request=request,
            ),
            PublicOiRestErrorCategoryV2.NETWORK,
        ),
        (
            lambda request: httpx.ReadTimeout(
                "synthetic pre-header timeout",
                request=request,
            ),
            PublicOiRestErrorCategoryV2.TIMEOUT,
        ),
        (
            lambda request: httpx.RemoteProtocolError(
                "synthetic protocol failure",
                request=request,
            ),
            PublicOiRestErrorCategoryV2.PROTOCOL,
        ),
    ],
)
async def test_pre_header_transport_failure_is_retained_once_without_retry(
    failure_factory,
    expected_category: PublicOiRestErrorCategoryV2,
) -> None:
    requests: list[httpx.Request] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        raise failure_factory(request)

    adapter, scheduler, plan, offerer, fatal = _adapter(
        httpx.MockTransport(handler)
    )

    receipt = await adapter.capture_attempt(_token(scheduler))
    await adapter.aclose()

    assert len(requests) == 1
    assert offerer.records == [receipt.record]
    payload = _payload(receipt.record, plan)
    assert payload.error_category is expected_category
    assert payload.response_status is None
    assert payload.payload_complete is False
    assert payload.body_bytes() == b""
    assert fatal.causes == []


@pytest.mark.asyncio
async def test_late_send_resume_retains_timeout_and_closes_response(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(rest_adapter_module, "_MILLISECONDS_PER_SECOND", 400_000)
    loop_time = MutableLoopTime()
    monkeypatch.setattr(rest_owner_module, "_loop_time", loop_time)
    stream = CountingResponseCloseStream(b"must-not-be-read")

    async def handler(request: httpx.Request) -> httpx.Response:
        loop_time.now = 1.0
        return httpx.Response(200, stream=stream, request=request)

    adapter, scheduler, plan, offerer, fatal = _adapter(
        httpx.MockTransport(handler)
    )

    receipt = await adapter.capture_attempt(_token(scheduler))
    await adapter.aclose()

    assert offerer.records == [receipt.record]
    assert stream.close_calls == 1
    payload = _payload(receipt.record, plan)
    assert payload.response_status == 200
    assert payload.payload_complete is False
    assert payload.body_bytes() == b""
    assert payload.error_category is PublicOiRestErrorCategoryV2.TIMEOUT
    assert payload.error_detail == "event loop resumed after the total send deadline"
    assert fatal.causes == []


@pytest.mark.asyncio
@pytest.mark.parametrize("status", [200, 429])
async def test_late_read_resume_retains_complete_timeout_as_primary(
    monkeypatch: pytest.MonkeyPatch,
    status: int,
) -> None:
    monkeypatch.setattr(rest_adapter_module, "_MILLISECONDS_PER_SECOND", 400_000)
    loop_time = MutableLoopTime()
    monkeypatch.setattr(rest_owner_module, "_loop_time", loop_time)
    stream = DeadlineAdvancingReadStream(b"complete-after-policy-deadline", loop_time)

    adapter, scheduler, plan, offerer, fatal = _adapter(
        httpx.MockTransport(
            lambda request: httpx.Response(
                status,
                stream=stream,
                request=request,
            )
        )
    )

    receipt = await adapter.capture_attempt(_token(scheduler))
    await adapter.aclose()

    assert offerer.records == [receipt.record]
    assert stream.close_calls == 1
    payload = _payload(receipt.record, plan)
    assert payload.response_status == status
    assert payload.payload_complete is True
    assert payload.body_bytes() == b"complete-after-policy-deadline"
    assert payload.error_category is PublicOiRestErrorCategoryV2.TIMEOUT
    assert payload.error_detail == (
        "event loop resumed after the total response-body deadline"
    )
    assert fatal.causes == []


@pytest.mark.asyncio
async def test_intra_attempt_wall_regression_admits_then_surfaces_exact_fatal() -> None:
    walls = (_SLOT + 100, _SLOT + 99, _SLOT + 98, _SLOT + 97)
    clock = ScriptedReceiptClock(
        tuple(
            ReceiptTimestamp(wall_ms, 1_000 + index)
            for index, wall_ms in enumerate(walls)
        )
    )
    adapter, scheduler, plan, offerer, fatal = _adapter(
        httpx.MockTransport(
            lambda request: httpx.Response(200, content=b"{}", request=request)
        ),
        clock=clock,
    )
    token = _token(scheduler)

    with pytest.raises(HttpsRestWallClockRegressionErrorV2) as captured:
        await adapter.capture_attempt(token)
    with pytest.raises(HttpsRestWallClockRegressionErrorV2) as close_failure:
        await adapter.aclose()

    assert close_failure.value is captured.value
    assert len(offerer.records) == 1
    payload = _payload(offerer.records[0], plan)
    assert (
        payload.request_started_wall_ms,
        payload.response_first_header_wall_ms,
        payload.attempt_ended_wall_ms,
        payload.completion_admission_wall_ms,
    ) == walls
    evidence = captured.value.evidence
    assert evidence.intra_attempt_regression is True
    assert evidence.prior_global_regression is False
    assert evidence.prior_global_wall_ms is None
    assert fatal.causes == [captured.value]
    assert_public_oi_scheduled_attempt_token_consumed_v2(
        token,
        plan=plan,
        schedule_authority=scheduler.schedule_authority,
    )


@pytest.mark.asyncio
async def test_prior_global_wall_regression_admits_second_row_then_fatal() -> None:
    first_walls = tuple(_SLOT + value for value in (200, 201, 202, 203))
    second_walls = tuple(_SLOT + value for value in (150, 151, 152, 153))
    clock = ScriptedReceiptClock(
        tuple(
            ReceiptTimestamp(wall_ms, 2_000 + index)
            for index, wall_ms in enumerate(first_walls + second_walls)
        )
    )
    adapter, scheduler, plan, offerer, fatal = _adapter(
        httpx.MockTransport(
            lambda request: httpx.Response(200, content=b"{}", request=request)
        ),
        clock=clock,
    )
    first_token = _token(scheduler)
    second_token = _token(scheduler, poll_cycle_seq=2)

    first_receipt = await adapter.capture_attempt(first_token)
    with pytest.raises(HttpsRestWallClockRegressionErrorV2) as captured:
        await adapter.capture_attempt(second_token)
    with pytest.raises(HttpsRestWallClockRegressionErrorV2) as close_failure:
        await adapter.aclose()

    assert close_failure.value is captured.value
    assert first_receipt.wall_clock_regression is None
    assert len(offerer.records) == 2
    first_payload = _payload(offerer.records[0], plan)
    second_payload = _payload(offerer.records[1], plan)
    assert first_payload.completion_admission_wall_ms == _SLOT + 203
    assert second_payload.request_started_wall_ms == _SLOT + 150
    assert second_payload.completion_admission_wall_ms == _SLOT + 153
    evidence = captured.value.evidence
    assert evidence.intra_attempt_regression is False
    assert evidence.prior_global_regression is True
    assert evidence.prior_global_wall_ms == _SLOT + 203
    assert fatal.causes == [captured.value]
    for token in (first_token, second_token):
        assert_public_oi_scheduled_attempt_token_consumed_v2(
            token,
            plan=plan,
            schedule_authority=scheduler.schedule_authority,
        )


@pytest.mark.asyncio
async def test_monotonic_header_reversal_still_rejects_without_false_row() -> None:
    clock = ScriptedReceiptClock(
        (
            ReceiptTimestamp(_SLOT + 100, 3_000),
            ReceiptTimestamp(_SLOT + 101, 2_999),
            ReceiptTimestamp(_SLOT + 102, 3_001),
        )
    )
    adapter, scheduler, _plan_value, offerer, fatal = _adapter(
        httpx.MockTransport(
            lambda request: httpx.Response(200, content=b"{}", request=request)
        ),
        clock=clock,
    )

    with pytest.raises(
        ValueError,
        match="first header precedes request start monotonically",
    ) as captured:
        await adapter.capture_attempt(_token(scheduler))
    with pytest.raises(ValueError) as close_failure:
        await adapter.aclose()

    assert close_failure.value is captured.value
    assert offerer.records == []
    assert fatal.causes == [captured.value]


@pytest.mark.asyncio
async def test_send_task_creation_failure_retains_and_acknowledges_before_fatal(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    requests: list[httpx.Request] = []
    original_create_task = asyncio.create_task

    async def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(200, content=b"{}", request=request)

    def fail_send_task(coro, *, name=None, context=None):  # type: ignore[no-untyped-def]
        if type(name) is str and "-send-" in name:
            raise RuntimeError("synthetic send-task creation failure")
        return original_create_task(coro, name=name, context=context)

    monkeypatch.setattr(rest_owner_module.asyncio, "create_task", fail_send_task)
    adapter, scheduler, plan, offerer, fatal = _adapter(
        httpx.MockTransport(handler)
    )

    with pytest.raises(
        RestCaptureOwnershipFailureV2,
        match="HTTP send owner could not start",
    ) as captured:
        await adapter.capture_attempt(_token(scheduler))
    with pytest.raises(RestCaptureOwnershipFailureV2) as close_failure:
        await adapter.aclose()

    assert close_failure.value is captured.value
    assert requests == []
    assert len(offerer.records) == 1
    payload = _payload(offerer.records[0], plan)
    assert payload.response_status is None
    assert payload.payload_complete is False
    assert payload.error_category is PublicOiRestErrorCategoryV2.CANCELLED
    assert payload.error_detail == (
        "local HTTP send owner failed before transport entry"
    )
    assert fatal.causes == [captured.value]


@pytest.mark.asyncio
async def test_read_task_creation_failure_retains_closes_and_fatalizes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    original_create_task = asyncio.create_task
    stream = CountingResponseCloseStream(b"must-not-be-read")

    def fail_read_task(coro, *, name=None, context=None):  # type: ignore[no-untyped-def]
        if type(name) is str and "-read-" in name:
            raise RuntimeError("synthetic read-task creation failure")
        return original_create_task(coro, name=name, context=context)

    monkeypatch.setattr(rest_owner_module.asyncio, "create_task", fail_read_task)
    adapter, scheduler, plan, offerer, fatal = _adapter(
        httpx.MockTransport(
            lambda request: httpx.Response(200, stream=stream, request=request)
        )
    )

    with pytest.raises(
        RestCaptureOwnershipFailureV2,
        match="response-body owner could not start",
    ) as captured:
        await adapter.capture_attempt(_token(scheduler))
    with pytest.raises(RestCaptureOwnershipFailureV2) as close_failure:
        await adapter.aclose()

    assert close_failure.value is captured.value
    assert stream.close_calls == 1
    assert len(offerer.records) == 1
    payload = _payload(offerer.records[0], plan)
    assert payload.response_status == 200
    assert payload.payload_complete is False
    assert payload.body_bytes() == b""
    assert payload.error_category is PublicOiRestErrorCategoryV2.RESPONSE_READ
    assert payload.error_detail == "local response-body owner failed before body read"
    assert fatal.causes == [captured.value]


@pytest.mark.asyncio
async def test_close_task_factory_failure_closes_retains_then_fatalizes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    original_create_task = asyncio.create_task
    stream = CountingResponseCloseStream(b"complete")

    def fail_close_task(coro, *, name=None, context=None):  # type: ignore[no-untyped-def]
        if type(name) is str and name.endswith("-response-close"):
            raise RuntimeError("synthetic close-task factory failure")
        return original_create_task(coro, name=name, context=context)

    monkeypatch.setattr(rest_owner_module.asyncio, "create_task", fail_close_task)
    adapter, scheduler, plan, offerer, fatal = _adapter(
        httpx.MockTransport(
            lambda request: httpx.Response(200, stream=stream, request=request)
        )
    )

    with pytest.raises(
        RestCaptureOwnershipFailureV2,
        match="response-close task factory failed",
    ) as captured:
        await adapter.capture_attempt(_token(scheduler))
    with pytest.raises(RestCaptureOwnershipFailureV2) as close_failure:
        await adapter.aclose()

    assert close_failure.value is captured.value
    assert stream.close_calls == 1
    assert len(offerer.records) == 1
    payload = _payload(offerer.records[0], plan)
    assert payload.response_status == 200
    assert payload.payload_complete is True
    assert payload.body_bytes() == b"complete"
    assert payload.error_category is PublicOiRestErrorCategoryV2.RESPONSE_CLOSE
    assert payload.error_detail == "local response-close task factory failed"
    assert fatal.causes == [captured.value]


@pytest.mark.asyncio
async def test_admission_task_factory_failure_retains_and_acknowledges_before_fatal(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    original_create_task = asyncio.create_task

    def fail_admission_task(  # type: ignore[no-untyped-def]
        coro,
        *,
        name=None,
        context=None,
    ):
        if type(name) is str and "-admit-" in name:
            raise RuntimeError("synthetic admission-task factory failure")
        return original_create_task(coro, name=name, context=context)

    monkeypatch.setattr(
        rest_owner_module.asyncio,
        "create_task",
        fail_admission_task,
    )
    adapter, scheduler, plan, offerer, fatal = _adapter(
        httpx.MockTransport(
            lambda request: httpx.Response(200, content=b"{}", request=request)
        )
    )

    with pytest.raises(
        RestCaptureOwnershipFailureV2,
        match="terminal admission task factory failed",
    ) as captured:
        await adapter.capture_attempt(_token(scheduler))
    with pytest.raises(RestCaptureOwnershipFailureV2) as close_failure:
        await adapter.aclose()

    assert close_failure.value is captured.value
    assert len(offerer.records) == 1
    payload = _payload(offerer.records[0], plan)
    assert payload.response_status == 200
    assert payload.payload_complete is True
    assert payload.error_category is None
    assert fatal.causes == [captured.value]


@pytest.mark.asyncio
async def test_send_timeout_closes_response_returned_during_cancel_reap(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(rest_adapter_module, "_MILLISECONDS_PER_SECOND", 400_000)
    stream = CountingResponseCloseStream(b"late")
    request_started = asyncio.Event()

    async def handler(request: httpx.Request) -> httpx.Response:
        request_started.set()
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            return httpx.Response(200, stream=stream, request=request)
        raise AssertionError("unreachable cancellation-suppressing handler")

    adapter, scheduler, plan, offerer, fatal = _adapter(
        httpx.MockTransport(handler)
    )

    receipt = await adapter.capture_attempt(_token(scheduler))
    await adapter.aclose()

    assert request_started.is_set()
    assert stream.close_calls == 1
    assert offerer.records == [receipt.record]
    payload = _payload(receipt.record, plan)
    assert payload.response_status is None
    assert payload.payload_complete is False
    assert payload.error_category is PublicOiRestErrorCategoryV2.TIMEOUT
    assert fatal.causes == []


@pytest.mark.asyncio
async def test_cancel_send_completion_header_overflow_retains_safe_row_then_fatal(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    stream = CountingResponseCloseStream(b"not-read")
    attempt: asyncio.Task[PublicOiAdmissionReceiptV2]

    async def handler(request: httpx.Request) -> httpx.Response:
        asyncio.get_running_loop().call_soon(attempt.cancel)
        return httpx.Response(
            200,
            headers=[("x-mbx-used-weight-1m", str(index)) for index in range(17)],
            stream=stream,
            request=request,
        )

    adapter, scheduler, plan, offerer, fatal = _adapter(
        httpx.MockTransport(handler)
    )
    attempt = asyncio.create_task(adapter.capture_attempt(_token(scheduler)))

    with pytest.raises(
        RestCaptureOwnershipFailureV2,
        match="bounded normalization contract during cancellation",
    ) as captured:
        await attempt
    with pytest.raises(RestCaptureOwnershipFailureV2) as close_failure:
        await adapter.aclose()

    assert close_failure.value is captured.value
    assert stream.close_calls == 1
    assert len(offerer.records) == 1
    payload = _payload(offerer.records[0], plan)
    assert payload.response_status == 200
    assert payload.response_headers == ()
    assert payload.payload_complete is False
    assert payload.error_category is PublicOiRestErrorCategoryV2.CANCELLED
    assert fatal.causes == [captured.value]


@pytest.mark.asyncio
async def test_exact_body_cap_is_complete_and_does_not_report_body_limit() -> None:
    body = bytes(range(256)) * 16
    adapter, scheduler, plan, offerer, fatal = _adapter(
        httpx.MockTransport(
            lambda request: httpx.Response(200, content=body, request=request)
        )
    )

    receipt = await adapter.capture_attempt(_token(scheduler))
    await adapter.aclose()

    assert offerer.records == [receipt.record]
    payload = _payload(receipt.record, plan)
    assert len(payload.body_bytes()) == 4096
    assert payload.body_bytes() == body
    assert payload.payload_complete is True
    assert payload.error_category is None
    assert fatal.causes == []


@pytest.mark.asyncio
async def test_partial_read_failure_retains_prefix_and_closes_response_once() -> None:
    stream = PartialReadFailureStream(b"retained-prefix")
    requests: list[httpx.Request] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(200, stream=stream, request=request)

    adapter, scheduler, plan, offerer, fatal = _adapter(
        httpx.MockTransport(handler)
    )

    receipt = await adapter.capture_attempt(_token(scheduler))
    await adapter.aclose()

    assert len(requests) == 1
    assert stream.close_calls == 1
    assert offerer.records == [receipt.record]
    payload = _payload(receipt.record, plan)
    assert payload.body_bytes() == b"retained-prefix"
    assert payload.payload_complete is False
    assert payload.error_category is PublicOiRestErrorCategoryV2.RESPONSE_READ
    assert fatal.causes == []


@pytest.mark.asyncio
async def test_response_close_failure_without_primary_outcome_is_retained() -> None:
    stream = FailingResponseCloseStream(b"{}")

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, stream=stream, request=request)

    adapter, scheduler, plan, offerer, fatal = _adapter(
        httpx.MockTransport(handler)
    )

    receipt = await adapter.capture_attempt(_token(scheduler))
    await adapter.aclose()

    assert stream.close_calls == 1
    assert offerer.records == [receipt.record]
    payload = _payload(receipt.record, plan)
    assert payload.body_bytes() == b"{}"
    assert payload.payload_complete is True
    assert payload.error_category is PublicOiRestErrorCategoryV2.RESPONSE_CLOSE
    assert fatal.causes == []


@pytest.mark.asyncio
async def test_response_close_failure_after_http_status_retains_then_fatalizes() -> None:
    stream = FailingResponseCloseStream(b"rate-limited")

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(429, stream=stream, request=request)

    adapter, scheduler, plan, offerer, fatal = _adapter(
        httpx.MockTransport(handler)
    )

    with pytest.raises(
        RestCaptureOwnershipFailureV2,
        match="response close failed after a primary terminal outcome",
    ) as captured:
        await adapter.capture_attempt(_token(scheduler))
    with pytest.raises(RestCaptureOwnershipFailureV2) as close_failure:
        await adapter.aclose()

    assert close_failure.value is captured.value
    assert fatal.causes == [captured.value]
    assert stream.close_calls == 1
    assert len(offerer.records) == 1
    payload = _payload(offerer.records[0], plan)
    assert payload.body_bytes() == b"rate-limited"
    assert payload.payload_complete is True
    assert payload.error_category is PublicOiRestErrorCategoryV2.HTTP_STATUS


@pytest.mark.asyncio
async def test_caller_cancellation_during_admission_reuses_the_same_task() -> None:
    original_offer = SharedWebSocketIngressV2.offer_https_attempt
    admission_started = asyncio.Event()
    admission_release = asyncio.Event()
    admission_calls = 0

    async def delayed_offer(
        self: SharedWebSocketIngressV2,
        *,
        plan: ProvisionalPromotingRestCapturePlanV2,
        session_id: str,
        protocol_hash: str,
        connection_id: str,
        generation: int,
        symbol: str,
        clock: ReceiptClock,
        observation: PublicOiRestTerminalObservationV2,
        source_logical_key: str,
        cancellation_requested: asyncio.Event | None = None,
    ) -> PublicOiAdmissionReceiptV2:
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

    adapter, scheduler, plan, offerer, fatal = _adapter(
        httpx.MockTransport(
            lambda request: httpx.Response(200, content=b"{}", request=request)
        )
    )
    attempt = asyncio.create_task(adapter.capture_attempt(_token(scheduler)))
    with pytest.MonkeyPatch.context() as patch_context:
        patch_context.setattr(
            SharedWebSocketIngressV2,
            "offer_https_attempt",
            delayed_offer,
        )
        await asyncio.wait_for(admission_started.wait(), timeout=1)
        attempt.cancel()
        admission_release.set()
        with pytest.raises(asyncio.CancelledError):
            await attempt
    await adapter.aclose()

    assert admission_calls == 1
    assert len(offerer.records) == 1
    payload = _payload(offerer.records[0], plan)
    assert payload.error_category is None
    assert payload.admission_cancellation_requested is True
    assert fatal.causes == []


@pytest.mark.asyncio
async def test_caller_cancellation_during_read_retains_partial_body_once() -> None:
    stream = BlockingReadStream(b"partial-before-cancel")

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, stream=stream, request=request)

    adapter, scheduler, plan, offerer, fatal = _adapter(
        httpx.MockTransport(handler)
    )
    attempt = asyncio.create_task(adapter.capture_attempt(_token(scheduler)))
    await asyncio.wait_for(stream.prefix_consumed.wait(), timeout=1)

    attempt.cancel()
    with pytest.raises(asyncio.CancelledError):
        await attempt
    stream.release.set()
    await adapter.aclose()

    assert stream.close_calls == 1
    assert len(offerer.records) == 1
    payload = _payload(offerer.records[0], plan)
    assert payload.body_bytes() == b"partial-before-cancel"
    assert payload.payload_complete is False
    assert payload.error_category is PublicOiRestErrorCategoryV2.CANCELLED
    assert payload.error_detail == "request cancelled while reading response body"
    assert payload.admission_cancellation_requested is True
    assert fatal.causes == []
    assert adapter.cleanly_closed


@pytest.mark.asyncio
async def test_aclose_during_active_send_rejects_new_attempt_and_drains_first() -> None:
    transport = GatedSendTransport()
    adapter, scheduler, _plan_value, offerer, fatal = _adapter(transport)
    first_attempt = asyncio.create_task(
        adapter.capture_attempt(_token(scheduler))
    )
    await asyncio.wait_for(transport.request_started.wait(), timeout=1)

    closer = asyncio.create_task(adapter.aclose())
    for _ in range(100):
        if not adapter.accepting_attempts:
            break
        await asyncio.sleep(0)
    assert not adapter.accepting_attempts
    with pytest.raises(RestCaptureAdapterClosedV2):
        await adapter.capture_attempt(_token(scheduler, poll_cycle_seq=2))

    transport.request_release.set()
    receipt = await first_attempt
    await closer

    assert len(transport.requests) == 1
    assert transport.close_calls == 1
    assert offerer.records == [receipt.record]
    assert fatal.causes == []
    assert adapter.fully_drained
    assert adapter.cleanly_closed


@pytest.mark.asyncio
async def test_unexpected_read_failure_still_closes_response_before_fatal() -> None:
    stream = NonBytesReadFailureStream()

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, stream=stream, request=request)

    adapter, scheduler, plan, offerer, fatal = _adapter(
        httpx.MockTransport(handler)
    )

    with pytest.raises(TypeError, match="non-bytes") as captured:
        await adapter.capture_attempt(_token(scheduler))
    with pytest.raises(TypeError) as close_failure:
        await adapter.aclose()

    assert close_failure.value is captured.value
    assert fatal.causes == [captured.value]
    assert stream.close_calls == 1
    assert len(offerer.records) == 1
    payload = _payload(offerer.records[0], plan)
    assert payload.error_category is PublicOiRestErrorCategoryV2.RESPONSE_READ
    assert payload.payload_complete is False


@pytest.mark.asyncio
async def test_pending_close_failure_survives_cancellation_during_admission() -> None:
    original_offer = SharedWebSocketIngressV2.offer_https_attempt
    admission_started = asyncio.Event()
    admission_release = asyncio.Event()
    stream = FailingResponseCloseStream(b"rate-limited")
    admission_calls = 0

    async def delayed_offer(
        self: SharedWebSocketIngressV2,
        *,
        plan: ProvisionalPromotingRestCapturePlanV2,
        session_id: str,
        protocol_hash: str,
        connection_id: str,
        generation: int,
        symbol: str,
        clock: ReceiptClock,
        observation: PublicOiRestTerminalObservationV2,
        source_logical_key: str,
        cancellation_requested: asyncio.Event | None = None,
    ) -> PublicOiAdmissionReceiptV2:
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

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(429, stream=stream, request=request)

    adapter, scheduler, plan, offerer, fatal = _adapter(
        httpx.MockTransport(handler)
    )
    with pytest.MonkeyPatch.context() as patch_context:
        patch_context.setattr(
            SharedWebSocketIngressV2,
            "offer_https_attempt",
            delayed_offer,
        )
        attempt = asyncio.create_task(adapter.capture_attempt(_token(scheduler)))
        await asyncio.wait_for(admission_started.wait(), timeout=1)
        attempt.cancel()
        admission_release.set()
        with pytest.raises(
            RestCaptureOwnershipFailureV2,
            match="response close failed after a primary terminal outcome",
        ) as captured:
            await attempt
    with pytest.raises(RestCaptureOwnershipFailureV2) as close_failure:
        await adapter.aclose()

    assert close_failure.value is captured.value
    assert fatal.causes == [captured.value]
    assert admission_calls == 1
    assert stream.close_calls == 1
    assert len(offerer.records) == 1
    payload = _payload(offerer.records[0], plan)
    assert payload.error_category is PublicOiRestErrorCategoryV2.HTTP_STATUS
    assert payload.admission_cancellation_requested is True


@pytest.mark.asyncio
@pytest.mark.parametrize("drift", ["stream", "stream_in_place", "extensions"])
async def test_transport_request_stream_or_extensions_drift_cannot_admit_clean_proof(
    drift: str,
) -> None:
    requests: list[httpx.Request] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        if drift == "stream":
            request.stream = httpx.ByteStream(b"forbidden-body")
        elif drift == "stream_in_place":
            request.stream.__dict__["_stream"] = b"forbidden-body"
        else:
            request.extensions["forbidden"] = "transport-drift"
        return httpx.Response(200, content=b"{}", request=request)

    adapter, scheduler, _plan_value, offerer, fatal = _adapter(
        httpx.MockTransport(handler)
    )

    with pytest.raises(
        RestCaptureOwnershipFailureV2,
        match="request mutated before terminal evidence admission",
    ) as captured:
        await adapter.capture_attempt(_token(scheduler))
    with pytest.raises(RestCaptureOwnershipFailureV2) as close_failure:
        await adapter.aclose()

    assert close_failure.value is captured.value
    assert fatal.causes == [captured.value]
    assert len(requests) == 1
    assert offerer.records == []


@pytest.mark.asyncio
async def test_response_header_overflow_is_retained_once_then_fatal() -> None:
    headers = [("x-mbx-used-weight-1m", str(index)) for index in range(17)]
    adapter, scheduler, plan, offerer, fatal = _adapter(
        httpx.MockTransport(
            lambda request: httpx.Response(
                200,
                headers=headers,
                content=b"{}",
                request=request,
            )
        )
    )

    with pytest.raises(
        RestCaptureOwnershipFailureV2,
        match="response headers violated the bounded normalization contract",
    ) as captured:
        await adapter.capture_attempt(_token(scheduler))
    with pytest.raises(RestCaptureOwnershipFailureV2) as close_failure:
        await adapter.aclose()

    assert close_failure.value is captured.value
    assert fatal.causes == [captured.value]
    assert len(offerer.records) == 1
    payload = _payload(offerer.records[0], plan)
    assert payload.response_status == 200
    assert payload.response_headers == ()
    assert payload.payload_complete is False
    assert payload.body_bytes() == b""
    assert payload.error_category is PublicOiRestErrorCategoryV2.RESPONSE_READ
