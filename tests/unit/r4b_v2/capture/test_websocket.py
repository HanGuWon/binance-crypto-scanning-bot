from __future__ import annotations

import asyncio
import hashlib
from collections.abc import AsyncIterator
from contextlib import AbstractAsyncContextManager
from dataclasses import dataclass, field, replace

import pytest

from signalbot.capture.depth_sequence import DepthRangeObservation, DepthResyncRequest
from signalbot.capture.models import ConnectionState
from signalbot.capture.receipts import ReceiptTimestamp
from signalbot.capture.ws_owner import PublicWebSocketCaptureOwner, WebSocketOwnerSettings
from signalbot.domain.enums import Market
from signalbot.r4b_v2.capture import websocket as websocket_module
from signalbot.r4b_v2.capture.batching import (
    BatchPolicyV2,
    BoundedBatchHandoffV2,
    CaptureQueueAdmissionReceiptV2,
    QueuedRawRecordV2,
)
from signalbot.r4b_v2.capture.models import RawRecordV2, TransportV2, VenueV2
from signalbot.r4b_v2.capture.plans import (
    ProvisionalPromotingCapturePlanV2,
    ProvisionalPromotingRestCapturePlanV2,
    build_provisional_promoting_capture_plans_v2,
)
from signalbot.r4b_v2.capture.rest import (
    PublicOiRestAttemptPayloadV2,
    PublicOiRestTerminalObservationV2,
)
from signalbot.r4b_v2.capture.rest_census import (
    PublicOiRestCellOutcomeV2,
    PublicOiRestCoverageCloseV2,
    PublicOiRestForwardGapRangeV2,
    PublicOiRestSlotCensusEntryV2,
    PublicOiRestSlotCensusV2,
)
from signalbot.r4b_v2.capture.websocket import (
    PublicHttpsRestWallClockRegressionEvidenceV2,
    PublicOiAdmissionReceiptV2,
    PublicOiCensusAdmissionReceiptV2,
    PublicRetainedDepthRangeCallbackReceiptV2,
    PublicWebSocketCaptureAdapterV2,
    PublicWebSocketFrameAdapterFactoryV2,
    SharedIngressOrderingErrorV2,
    SharedWebSocketIngressV2,
    _mint_public_retained_depth_range_callback_receipt_v2,
    _mint_public_retained_depth_resync_callback_receipt_v2,
    build_public_websocket_owner_plan_v2,
    validate_public_https_rest_wall_clock_regression_evidence_v2,
    validate_public_oi_admission_receipt_v2,
    validate_public_oi_census_admission_receipt_v2,
    validate_public_retained_depth_range_callback_receipt_v2,
    validate_public_retained_depth_resync_callback_receipt_v2,
)

PROTOCOL_HASH = hashlib.sha256(b"r4b-v2-live-websocket-test").hexdigest()
_CENSUS_SLOT = 10_000
_SESSION_START_MANIFEST_SHA256 = "1" * 64
_PLAN_BUNDLE_SHA256 = "2" * 64


def _bounded_test_handoff(expected_first_ingest_seq: int) -> BoundedBatchHandoffV2:
    return BoundedBatchHandoffV2(
        BatchPolicyV2(
            max_records=64,
            max_encoded_bytes=1_000_000,
            max_linger_us=1_000,
            queue_max_events=1_024,
            queue_max_encoded_bytes=16_000_000,
            low_water_events=0,
            low_water_encoded_bytes=0,
            qualification_id="websocket-recording-offerer-handoff",
        ),
        expected_first_ingest_seq=expected_first_ingest_seq,
    )


class RecordingOfferer:
    def __init__(self, events: list[str] | None = None) -> None:
        self.records: list[RawRecordV2] = []
        self.events = events
        self._handoff: BoundedBatchHandoffV2 | None = None

    def _admission_handoff(self, record: RawRecordV2) -> BoundedBatchHandoffV2:
        if self._handoff is None:
            self._handoff = _bounded_test_handoff(record.ingest_seq)
        return self._handoff

    def offer(self, record: RawRecordV2) -> QueuedRawRecordV2:
        if self.events is not None:
            self.events.append("offer")
        queued_record = self._admission_handoff(record).offer(record)
        self.records.append(record)
        return queued_record

    def offer_with_admission_receipt(
        self,
        record: RawRecordV2,
    ) -> CaptureQueueAdmissionReceiptV2:
        if self.events is not None:
            self.events.append("offer")
        receipt = self._admission_handoff(record).offer_with_admission_receipt(record)
        self.records.append(record)
        return receipt

    def validate_queue_admission_receipt_v2(
        self,
        receipt: CaptureQueueAdmissionReceiptV2,
    ) -> QueuedRawRecordV2:
        if self._handoff is None:
            raise AssertionError("recording offerer has no admission handoff")
        return self._handoff.validate_queue_admission_receipt_v2(receipt)


class PassThroughRecoveryLifecycle:
    def __init__(self) -> None:
        self.fatal_causes: list[BaseException] = []

    async def complete_recovery_successor(self, record: RawRecordV2) -> None:
        del record

    def record_retained_frame(self, record: RawRecordV2) -> None:
        del record

    def trip_fatal(self, cause: BaseException) -> None:
        self.fatal_causes.append(cause)


class ScriptReceiptClock:
    def __init__(self, events: list[str] | None = None) -> None:
        self.calls = 0
        self.events = events

    def capture(self) -> ReceiptTimestamp:
        if self.events is not None:
            self.events.append("receipt")
        self.calls += 1
        return ReceiptTimestamp(
            received_at_ms=1_700_000_000_000 + self.calls,
            received_monotonic_ns=10_000 + self.calls,
        )


class NotifyingReceiptClock:
    def __init__(self, *, wall_ms: int, monotonic_ns: int) -> None:
        self.timestamp = ReceiptTimestamp(
            received_at_ms=wall_ms,
            received_monotonic_ns=monotonic_ns,
        )
        self.captured = asyncio.Event()

    def capture(self) -> ReceiptTimestamp:
        self.captured.set()
        return self.timestamp


class ScheduleCancellationOnCaptureClock:
    def __init__(
        self,
        cancellation_requested: asyncio.Event,
        *,
        wall_ms: int,
        monotonic_ns: int,
    ) -> None:
        self.cancellation_requested = cancellation_requested
        self.timestamp = ReceiptTimestamp(wall_ms, monotonic_ns)

    def capture(self) -> ReceiptTimestamp:
        asyncio.get_running_loop().call_soon(self.cancellation_requested.set)
        return self.timestamp


class ObservedText(str):
    events: list[str]

    def __new__(cls, value: str, events: list[str]) -> ObservedText:
        instance = super().__new__(cls, value)
        instance.events = events
        return instance

    def encode(self, encoding: str = "utf-8", errors: str = "strict") -> bytes:
        self.events.append("retain")
        return super().encode(encoding, errors)


class OfferRejected(RuntimeError):
    pass


class RejectingOfferer:
    def offer(self, record: RawRecordV2) -> object:
        del record
        raise OfferRejected("bounded V2 pipeline rejected the frame")


class OneFrameThenStop:
    def __init__(self, stop_event: asyncio.Event, frame: str | bytes) -> None:
        self.stop_event = stop_event
        self.frame = frame

    async def __aiter__(self) -> AsyncIterator[str | bytes]:
        yield self.frame
        self.stop_event.set()
        await asyncio.Event().wait()


class OwnerConnection(AbstractAsyncContextManager[OneFrameThenStop]):
    def __init__(self, frames: OneFrameThenStop) -> None:
        self.frames = frames

    async def __aenter__(self) -> OneFrameThenStop:
        return self.frames

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: object,
    ) -> None:
        return None


class OwnerConnector:
    def __init__(self, frames: OneFrameThenStop) -> None:
        self.frames = frames
        self.urls: list[str] = []

    def __call__(self, url: str) -> OwnerConnection:
        self.urls.append(url)
        return OwnerConnection(self.frames)


@dataclass
class OwnerLifecycle:
    stop_event: asyncio.Event = field(default_factory=asyncio.Event)
    failed: bool = False
    accepting: bool = True
    transitions: list[tuple[str, int, int, ConnectionState, str]] = field(default_factory=list)
    fatal_causes: list[BaseException] = field(default_factory=list)

    def record_transition(
        self,
        connection_id: str,
        *,
        generation: int,
        last_frame_seq: int,
        state: ConnectionState,
        reason: str,
    ) -> None:
        self.transitions.append((connection_id, generation, last_frame_seq, state, reason))

    def trip_fatal(self, cause: BaseException) -> None:
        if self.failed:
            return
        self.failed = True
        self.accepting = False
        self.fatal_causes.append(cause)
        self.stop_event.set()


def _websocket_plans() -> tuple[
    ProvisionalPromotingCapturePlanV2,
    ProvisionalPromotingCapturePlanV2,
]:
    websocket_plans = tuple(
        plan
        for plan in build_provisional_promoting_capture_plans_v2(("BTCUSDT",))
        if isinstance(plan, ProvisionalPromotingCapturePlanV2)
    )
    assert len(websocket_plans) == 2
    market = next(plan for plan in websocket_plans if plan.route_id == "usdm_market")
    public = next(plan for plan in websocket_plans if plan.route_id == "usdm_public")
    return market, public


def _rest_plan(
    symbols: tuple[str, ...] = ("BTCUSDT",),
) -> ProvisionalPromotingRestCapturePlanV2:
    plans = build_provisional_promoting_capture_plans_v2(symbols)
    rest = tuple(plan for plan in plans if type(plan) is ProvisionalPromotingRestCapturePlanV2)
    assert len(rest) == 1
    return rest[0]


def _rest_observation(
    plan: ProvisionalPromotingRestCapturePlanV2,
    *,
    symbol: str = "BTCUSDT",
    request_started_wall_ms: int = 10,
    request_started_monotonic_ns: int = 100,
) -> PublicOiRestTerminalObservationV2:
    return PublicOiRestTerminalObservationV2.for_plan(
        plan,
        symbol=symbol,
        poll_cycle_seq=1,
        symbol_ordinal=plan.symbols.index(symbol),
        scheduled_slot_wall_ms=(request_started_wall_ms - (request_started_wall_ms % 5_000)),
        attempt=1,
        request_started_wall_ms=request_started_wall_ms,
        request_started_monotonic_ns=request_started_monotonic_ns,
        response_first_header_wall_ms=request_started_wall_ms + 1,
        response_first_header_monotonic_ns=request_started_monotonic_ns + 1,
        attempt_ended_wall_ms=request_started_wall_ms + 2,
        attempt_ended_monotonic_ns=request_started_monotonic_ns + 2,
        response_status=200,
        response_headers=(),
        payload_complete=True,
        body=(f'{{"openInterest":"123.45","symbol":"{symbol}","time":1700000000000}}').encode(),
    )


def _rest_census_payloads(
    plan: ProvisionalPromotingRestCapturePlanV2,
    *,
    session_id: str = "session-census",
) -> tuple[
    PublicOiRestSlotCensusV2,
    PublicOiRestForwardGapRangeV2,
    PublicOiRestCoverageCloseV2,
]:
    entries = tuple(
        PublicOiRestSlotCensusEntryV2.for_plan(
            plan,
            session_start_manifest_sha256=_SESSION_START_MANIFEST_SHA256,
            plan_bundle_sha256=_PLAN_BUNDLE_SHA256,
            symbol_ordinal=ordinal,
            scheduled_slot_wall_ms=_CENSUS_SLOT,
            outcome=PublicOiRestCellOutcomeV2.UNSTARTED_NORMAL_STOP,
        )
        for ordinal in range(len(plan.symbols))
    )
    slot = PublicOiRestSlotCensusV2.for_plan(
        plan,
        session_id=session_id,
        session_start_manifest_sha256=_SESSION_START_MANIFEST_SHA256,
        plan_bundle_sha256=_PLAN_BUNDLE_SHA256,
        scheduled_slot_wall_ms=_CENSUS_SLOT,
        entries=entries,
        closed_wall_ms=_CENSUS_SLOT + 4_000,
        closed_monotonic_ns=1_000,
    )
    gap = PublicOiRestForwardGapRangeV2.for_plan(
        plan,
        session_id=session_id,
        session_start_manifest_sha256=_SESSION_START_MANIFEST_SHA256,
        plan_bundle_sha256=_PLAN_BUNDLE_SHA256,
        first_slot_wall_ms=_CENSUS_SLOT,
        end_slot_exclusive_wall_ms=_CENSUS_SLOT + 10_000,
        observed_wall_ms=_CENSUS_SLOT + 10_001,
        observed_monotonic_ns=1_001,
    )
    close = PublicOiRestCoverageCloseV2.for_plan(
        plan,
        session_id=session_id,
        session_start_manifest_sha256=_SESSION_START_MANIFEST_SHA256,
        plan_bundle_sha256=_PLAN_BUNDLE_SHA256,
        coverage_start_slot_wall_ms=_CENSUS_SLOT,
        stop_requested_wall_ms=_CENSUS_SLOT + 2_500,
        stop_requested_monotonic_ns=1_002,
        last_census_ingest_seq=1,
    )
    return slot, gap, close


def _retained_rest_payload(record: RawRecordV2) -> PublicOiRestAttemptPayloadV2:
    return PublicOiRestAttemptPayloadV2.from_canonical_bytes(record.payload_bytes())


async def _cooperative_frames(*frames: str | bytes) -> AsyncIterator[str | bytes]:
    for frame in frames:
        await asyncio.sleep(0)
        yield frame


def _futures_public_depth_frame(
    *,
    first_u: int = 10,
    final_u: int = 12,
    previous_u: int = 9,
) -> bytes:
    return (
        '{"stream":"btcusdt@depth@100ms","data":'
        '{"e":"depthUpdate","s":"BTCUSDT",'
        f'"U":{first_u},"u":{final_u},"pu":{previous_u},'
        '"ps":"BTCUSDT","st":1}}'
    ).encode()


def _owner_settings() -> WebSocketOwnerSettings:
    return WebSocketOwnerSettings(
        maximum_connection_age_seconds=1.0,
        connect_timeout_seconds=1.0,
        close_timeout_seconds=1.0,
        heartbeat_interval_seconds=1.0,
        pong_timeout_seconds=1.0,
        internal_queue_frames=8,
        maximum_frame_bytes=1024,
        maximum_reconnect_attempts=1,
        reconnect_delays_seconds=(0.0,),
        healthy_reset_seconds=0.5,
    )


def test_v2_plans_adapt_to_exact_existing_public_owner_routes_and_urls() -> None:
    market, public = _websocket_plans()

    market_owner = build_public_websocket_owner_plan_v2(market)
    public_owner = build_public_websocket_owner_plan_v2(public)

    assert market_owner.route == "market"
    assert market_owner.url == (
        "wss://fstream.binance.com/market/stream?streams="
        "btcusdt@kline_5m/btcusdt@aggTrade/btcusdt@markPrice@1s"
    )
    assert public_owner.route == "public"
    assert public_owner.url == (
        "wss://fstream.binance.com/public/stream?streams=btcusdt@depth@100ms"
    )
    assert market_owner.streams == market.streams
    assert public_owner.streams == public.streams


def test_exact_v2_factory_rejects_non_v2_outer_lifecycle_pair() -> None:
    market, _public = _websocket_plans()
    offerer = RecordingOfferer()
    ingress = SharedWebSocketIngressV2(
        offerer,
        recovered_wal_tail_ingest_seq=73,
    )
    factory = PublicWebSocketFrameAdapterFactoryV2(
        market,
        session_id="session-owner-integration",
        protocol_hash=PROTOCOL_HASH,
        clock=ScriptReceiptClock(),
        ingress=ingress,
        recovery_lifecycle=PassThroughRecoveryLifecycle(),
    )
    lifecycle = OwnerLifecycle()
    connector = OwnerConnector(
        OneFrameThenStop(
            lifecycle.stop_event,
            b'{"stream":"btcusdt@aggTrade","data":{"e":"aggTrade"}}',
        )
    )
    with pytest.raises(TypeError, match="must be paired exactly"):
        PublicWebSocketCaptureOwner(
            factory.owner_plan,
            plan_sha256=PROTOCOL_HASH,
            process_boot_id="boot-owner-integration",
            settings=_owner_settings(),
            connector=connector,
            frame_adapter_factory=factory,
            lifecycle_coordinator=lifecycle,
        )

    assert offerer.records == []
    assert connector.urls == []
    assert lifecycle.transitions == []


@pytest.mark.asyncio
async def test_receipt_is_first_action_then_raw_is_retained_before_exact_offer() -> None:
    market, _public = _websocket_plans()
    events: list[str] = []
    offerer = RecordingOfferer(events)
    ingress = SharedWebSocketIngressV2(
        offerer,
        recovered_wal_tail_ingest_seq=41,
    )
    factory = PublicWebSocketFrameAdapterFactoryV2(
        market,
        session_id="session-live-v2",
        protocol_hash=PROTOCOL_HASH,
        clock=ScriptReceiptClock(events),
        ingress=ingress,
        recovery_lifecycle=PassThroughRecoveryLifecycle(),
    )
    adapter = factory(connection_id="market-g000007", generation=7)

    async def frames() -> AsyncIterator[str | bytes]:
        events.append("yield")
        yield ObservedText('{"stream":"btcusdt@aggTrade","data":{"p":"1"}}', events)
        events.append("next_requested")

    await adapter.consume(frames())

    assert events == ["yield", "receipt", "retain", "offer", "next_requested"]
    assert adapter.frame_seq == 1
    assert len(offerer.records) == 1
    record = offerer.records[0]
    assert record.session_id == "session-live-v2"
    assert record.plan_id == market.name
    assert record.protocol_hash == PROTOCOL_HASH
    assert record.transport is TransportV2.WEBSOCKET
    assert record.venue is VenueV2.USDM_FUTURES
    assert record.route_id == "usdm_market"
    assert record.symbol is None
    assert record.connection_id == "market-g000007"
    assert record.generation == 7
    assert record.frame_seq == 1
    assert record.ingest_seq == 42
    assert record.receipt_wall_ms == 1_700_000_000_001
    assert record.receipt_monotonic_ns == 10_001
    assert record.payload_bytes() == (b'{"stream":"btcusdt@aggTrade","data":{"p":"1"}}')


@pytest.mark.asyncio
async def test_adapter_publishes_read_only_exact_record_only_after_lifecycle() -> None:
    market, _public = _websocket_plans()
    offerer = RecordingOfferer()
    ingress = SharedWebSocketIngressV2(offerer, recovered_wal_tail_ingest_seq=0)

    class ObservingLifecycle(PassThroughRecoveryLifecycle):
        adapter: PublicWebSocketCaptureAdapterV2 | None = None
        first_record: RawRecordV2 | None = None

        async def complete_recovery_successor(self, record: RawRecordV2) -> None:
            assert self.adapter is not None
            assert self.adapter.frame_seq == 0
            assert self.adapter.last_admitted_raw_record_v2 is None
            self.first_record = record

        def record_retained_frame(self, record: RawRecordV2) -> None:
            del record
            assert self.adapter is not None
            assert self.adapter.frame_seq == 1
            assert self.adapter.last_admitted_raw_record_v2 is self.first_record

    lifecycle = ObservingLifecycle()
    adapter = PublicWebSocketFrameAdapterFactoryV2(
        market,
        session_id="session-retained-tail",
        protocol_hash=PROTOCOL_HASH,
        clock=ScriptReceiptClock(),
        ingress=ingress,
        recovery_lifecycle=lifecycle,
    )(connection_id="market-g000004", generation=4)
    lifecycle.adapter = adapter

    assert adapter.last_admitted_raw_record_v2 is None
    with pytest.raises(AttributeError):
        adapter.last_admitted_raw_record_v2 = None  # pyright: ignore[reportAttributeAccessIssue]

    await adapter.consume(_cooperative_frames(b"first", b"second"))

    assert adapter.frame_seq == 2
    retained = adapter.last_admitted_raw_record_v2
    assert retained is offerer.records[-1]
    assert retained is not None and retained.payload_bytes() == b"second"


@pytest.mark.asyncio
async def test_cancelled_recovery_offer_never_publishes_retained_record() -> None:
    market, _public = _websocket_plans()
    offerer = RecordingOfferer()
    ingress = SharedWebSocketIngressV2(offerer, recovered_wal_tail_ingest_seq=0)

    class BlockingLifecycle(PassThroughRecoveryLifecycle):
        def __init__(self) -> None:
            super().__init__()
            self.started = asyncio.Event()

        async def complete_recovery_successor(self, record: RawRecordV2) -> None:
            del record
            self.started.set()
            await asyncio.Event().wait()

    lifecycle = BlockingLifecycle()
    adapter = PublicWebSocketFrameAdapterFactoryV2(
        market,
        session_id="session-cancelled-retained-tail",
        protocol_hash=PROTOCOL_HASH,
        clock=ScriptReceiptClock(),
        ingress=ingress,
        recovery_lifecycle=lifecycle,
    )(connection_id="market-g000001", generation=1)
    task = asyncio.create_task(adapter.consume(_cooperative_frames(b"offered")))
    await lifecycle.started.wait()

    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    assert len(offerer.records) == 1
    assert adapter.frame_seq == 0
    assert adapter.last_admitted_raw_record_v2 is None


@pytest.mark.asyncio
async def test_lifecycle_failures_never_publish_failed_frame_record() -> None:
    market, _public = _websocket_plans()
    first_failure = RuntimeError("recovery lifecycle failed")

    class FailingRecoveryLifecycle(PassThroughRecoveryLifecycle):
        async def complete_recovery_successor(self, record: RawRecordV2) -> None:
            del record
            raise first_failure

    first_offerer = RecordingOfferer()
    first_adapter = PublicWebSocketFrameAdapterFactoryV2(
        market,
        session_id="session-failed-recovery-publication",
        protocol_hash=PROTOCOL_HASH,
        clock=ScriptReceiptClock(),
        ingress=SharedWebSocketIngressV2(
            first_offerer,
            recovered_wal_tail_ingest_seq=0,
        ),
        recovery_lifecycle=FailingRecoveryLifecycle(),
    )(connection_id="market-g000001", generation=1)
    with pytest.raises(RuntimeError) as first_captured:
        await first_adapter.consume(_cooperative_frames(b"first"))
    assert first_captured.value is first_failure
    assert len(first_offerer.records) == 1
    assert first_adapter.frame_seq == 0
    assert first_adapter.last_admitted_raw_record_v2 is None

    later_failure = RuntimeError("later retained lifecycle failed")

    class FailingLaterLifecycle(PassThroughRecoveryLifecycle):
        def record_retained_frame(self, record: RawRecordV2) -> None:
            del record
            raise later_failure

    later_offerer = RecordingOfferer()
    later_adapter = PublicWebSocketFrameAdapterFactoryV2(
        market,
        session_id="session-failed-later-publication",
        protocol_hash=PROTOCOL_HASH,
        clock=ScriptReceiptClock(),
        ingress=SharedWebSocketIngressV2(
            later_offerer,
            recovered_wal_tail_ingest_seq=0,
        ),
        recovery_lifecycle=FailingLaterLifecycle(),
    )(connection_id="market-g000001", generation=1)
    with pytest.raises(RuntimeError) as later_captured:
        await later_adapter.consume(_cooperative_frames(b"first", b"second"))
    assert later_captured.value is later_failure
    assert len(later_offerer.records) == 2
    assert later_adapter.frame_seq == 1
    assert later_adapter.last_admitted_raw_record_v2 is later_offerer.records[0]


def test_v2_capture_adapter_is_factory_sealed() -> None:
    market, _public = _websocket_plans()
    with pytest.raises(TypeError, match="exact factory"):
        PublicWebSocketCaptureAdapterV2(
            market,
            session_id="session-synthetic-adapter",
            protocol_hash=PROTOCOL_HASH,
            connection_id="market-g000001",
            generation=1,
            clock=ScriptReceiptClock(),
            ingress=SharedWebSocketIngressV2(
                RecordingOfferer(),
                recovered_wal_tail_ingest_seq=0,
            ),
            recovery_lifecycle=PassThroughRecoveryLifecycle(),
        )


@pytest.mark.asyncio
async def test_retained_depth_receipts_bind_exact_frame_and_reject_replay_tamper() -> None:
    _market, public = _websocket_plans()
    raw = _futures_public_depth_frame()
    offerer = RecordingOfferer()
    factory = PublicWebSocketFrameAdapterFactoryV2(
        public,
        session_id="session-depth-callback",
        protocol_hash=PROTOCOL_HASH,
        clock=ScriptReceiptClock(),
        ingress=SharedWebSocketIngressV2(
            offerer,
            recovered_wal_tail_ingest_seq=80,
        ),
        recovery_lifecycle=PassThroughRecoveryLifecycle(),
    )
    adapter = factory(connection_id="public-g000003", generation=3)
    await adapter.consume(_cooperative_frames(raw))
    observation = DepthRangeObservation(
        market=Market.FUTURES,
        symbol="BTCUSDT",
        generation=3,
        U=10,
        u=12,
        reset=True,
    )
    with pytest.raises(TypeError, match="owner post-offer seam"):
        _mint_public_retained_depth_range_callback_receipt_v2(
            adapter=adapter,
            owner_plan=factory.owner_plan,
            session_id=factory.session_id,
            protocol_hash=factory.protocol_hash,
            connection_id="public-g000003",
            generation=3,
            frame_seq=1,
            raw=raw,
            observation=observation,
        )
    with pytest.raises(TypeError, match="owner post-offer seam"):
        _mint_public_retained_depth_range_callback_receipt_v2(
            adapter=adapter,
            owner_plan=factory.owner_plan,
            session_id=factory.session_id,
            protocol_hash=factory.protocol_hash,
            connection_id="public-g000003",
            generation=3,
            frame_seq=1,
            raw=raw,
            observation=replace(observation, reset=False),
            _owner_seam_token=object(),
        )
    range_receipt = _mint_public_retained_depth_range_callback_receipt_v2(
        adapter=adapter,
        owner_plan=factory.owner_plan,
        session_id=factory.session_id,
        protocol_hash=factory.protocol_hash,
        connection_id="public-g000003",
        generation=3,
        frame_seq=1,
        raw=raw,
        observation=observation,
        _owner_seam_token=websocket_module._RETAINED_DEPTH_OWNER_SEAM_TOKEN_V2,
    )
    request = DepthResyncRequest(
        event="reconnect",
        market=Market.FUTURES,
        generation=3,
        watermarks=(("BTCUSDT", 10),),
    )
    resync_receipt = _mint_public_retained_depth_resync_callback_receipt_v2(
        adapter=adapter,
        owner_plan=factory.owner_plan,
        session_id=factory.session_id,
        protocol_hash=factory.protocol_hash,
        connection_id="public-g000003",
        generation=3,
        frame_seq=1,
        raw=raw,
        request=request,
        preceding_range_receipt=range_receipt,
        _owner_seam_token=websocket_module._RETAINED_DEPTH_OWNER_SEAM_TOKEN_V2,
    )

    validate_public_retained_depth_range_callback_receipt_v2(range_receipt)
    validate_public_retained_depth_resync_callback_receipt_v2(resync_receipt)
    assert range_receipt.ingest_seq == 81
    assert range_receipt.raw_payload_sha256 == hashlib.sha256(raw).hexdigest()
    assert range_receipt.receipt_wall_ms == 1_700_000_000_001
    assert range_receipt.receipt_monotonic_ns == 10_001
    assert resync_receipt.frame_seq == range_receipt.frame_seq == 1
    assert resync_receipt.request is request

    with pytest.raises(RuntimeError, match="already minted"):
        _mint_public_retained_depth_resync_callback_receipt_v2(
            adapter=adapter,
            owner_plan=factory.owner_plan,
            session_id=factory.session_id,
            protocol_hash=factory.protocol_hash,
            connection_id="public-g000003",
            generation=3,
            frame_seq=1,
            raw=raw,
            request=request,
            preceding_range_receipt=range_receipt,
            _owner_seam_token=websocket_module._RETAINED_DEPTH_OWNER_SEAM_TOKEN_V2,
        )
    object.__setattr__(adapter, "_last_retained_depth_resync_receipt_frame_seq", 0)
    with pytest.raises(ValueError, match="mint cursor provenance"):
        _mint_public_retained_depth_resync_callback_receipt_v2(
            adapter=adapter,
            owner_plan=factory.owner_plan,
            session_id=factory.session_id,
            protocol_hash=factory.protocol_hash,
            connection_id="public-g000003",
            generation=3,
            frame_seq=1,
            raw=raw,
            request=request,
            preceding_range_receipt=range_receipt,
            _owner_seam_token=websocket_module._RETAINED_DEPTH_OWNER_SEAM_TOKEN_V2,
        )
    with pytest.raises(TypeError, match="only be minted"):
        replace(range_receipt)

    class RangeReceiptSubclass(PublicRetainedDepthRangeCallbackReceiptV2):
        pass

    forged_subclass = object.__new__(RangeReceiptSubclass)
    with pytest.raises(TypeError, match="exact type"):
        validate_public_retained_depth_range_callback_receipt_v2(forged_subclass)

    object.__setattr__(range_receipt, "ingest_seq", range_receipt.ingest_seq + 1)
    with pytest.raises(ValueError, match="material was mutated"):
        validate_public_retained_depth_range_callback_receipt_v2(range_receipt)
    object.__setattr__(
        resync_receipt,
        "request",
        replace(request, watermarks=(("BTCUSDT", 11),)),
    )
    with pytest.raises(ValueError, match="material was mutated"):
        validate_public_retained_depth_resync_callback_receipt_v2(resync_receipt)


@pytest.mark.asyncio
async def test_retained_depth_factory_rejects_missing_foreign_and_mutated_records() -> None:
    _market, public = _websocket_plans()
    raw = _futures_public_depth_frame()
    factory = PublicWebSocketFrameAdapterFactoryV2(
        public,
        session_id="session-depth-source-validation",
        protocol_hash=PROTOCOL_HASH,
        clock=ScriptReceiptClock(),
        ingress=SharedWebSocketIngressV2(
            RecordingOfferer(),
            recovered_wal_tail_ingest_seq=0,
        ),
        recovery_lifecycle=PassThroughRecoveryLifecycle(),
    )
    observation = DepthRangeObservation(
        market=Market.FUTURES,
        symbol="BTCUSDT",
        generation=1,
        U=10,
        u=12,
        reset=True,
    )
    adapter = factory(connection_id="public-g000001", generation=1)
    common = {
        "owner_plan": factory.owner_plan,
        "session_id": factory.session_id,
        "protocol_hash": factory.protocol_hash,
        "connection_id": "public-g000001",
        "generation": 1,
        "frame_seq": 1,
        "raw": raw,
        "observation": observation,
        "_owner_seam_token": websocket_module._RETAINED_DEPTH_OWNER_SEAM_TOKEN_V2,
    }
    await adapter.consume(_cooperative_frames(raw))
    retained_record = adapter.last_admitted_raw_record_v2
    assert retained_record is not None
    object.__setattr__(adapter, "_last_admitted_raw_record_v2", None)
    with pytest.raises(RuntimeError, match="no admitted raw record"):
        _mint_public_retained_depth_range_callback_receipt_v2(
            adapter=adapter,
            **common,  # pyright: ignore[reportArgumentType]
        )
    object.__setattr__(adapter, "_last_admitted_raw_record_v2", retained_record)

    class ForeignAdapter(PublicWebSocketCaptureAdapterV2):
        pass

    foreign = object.__new__(ForeignAdapter)
    with pytest.raises(TypeError, match="exact V2 frame adapter"):
        _mint_public_retained_depth_range_callback_receipt_v2(
            adapter=foreign,
            **common,  # pyright: ignore[reportArgumentType]
        )

    record = adapter.last_admitted_raw_record_v2
    assert record is not None
    object.__setattr__(record, "connection_id", "public-g999999")
    with pytest.raises(ValueError, match="publication provenance"):
        _mint_public_retained_depth_range_callback_receipt_v2(
            adapter=adapter,
            **common,  # pyright: ignore[reportArgumentType]
        )


@pytest.mark.asyncio
async def test_generation_resync_receipt_requires_exact_depth_stream_census() -> None:
    _market, public = _websocket_plans()
    raw = _futures_public_depth_frame()
    factory = PublicWebSocketFrameAdapterFactoryV2(
        public,
        session_id="session-depth-census",
        protocol_hash=PROTOCOL_HASH,
        clock=ScriptReceiptClock(),
        ingress=SharedWebSocketIngressV2(
            RecordingOfferer(),
            recovered_wal_tail_ingest_seq=0,
        ),
        recovery_lifecycle=PassThroughRecoveryLifecycle(),
    )
    adapter = factory(connection_id="public-g000002", generation=2)
    await adapter.consume(_cooperative_frames(raw))
    range_receipt = _mint_public_retained_depth_range_callback_receipt_v2(
        adapter=adapter,
        owner_plan=factory.owner_plan,
        session_id=factory.session_id,
        protocol_hash=factory.protocol_hash,
        connection_id="public-g000002",
        generation=2,
        frame_seq=1,
        raw=raw,
        observation=DepthRangeObservation(
            market=Market.FUTURES,
            symbol="BTCUSDT",
            generation=2,
            U=10,
            u=12,
            reset=True,
        ),
        _owner_seam_token=websocket_module._RETAINED_DEPTH_OWNER_SEAM_TOKEN_V2,
    )

    with pytest.raises(ValueError, match="exact depth-stream census"):
        _mint_public_retained_depth_resync_callback_receipt_v2(
            adapter=adapter,
            owner_plan=factory.owner_plan,
            session_id=factory.session_id,
            protocol_hash=factory.protocol_hash,
            connection_id="public-g000002",
            generation=2,
            frame_seq=1,
            raw=raw,
            request=DepthResyncRequest(
                event="reconnect",
                market=Market.FUTURES,
                generation=2,
                watermarks=(("BTCUSDT", 10), ("ETHUSDT", 20)),
            ),
            preceding_range_receipt=range_receipt,
            _owner_seam_token=websocket_module._RETAINED_DEPTH_OWNER_SEAM_TOKEN_V2,
        )


@pytest.mark.asyncio
async def test_two_route_factories_share_one_recovered_global_ingest_sequence() -> None:
    market, public = _websocket_plans()
    offerer = RecordingOfferer()
    ingress = SharedWebSocketIngressV2(
        offerer,
        recovered_wal_tail_ingest_seq=1_000,
    )
    assert ingress.pipeline is offerer
    assert ingress.recovered_wal_tail_ingest_seq == 1_000
    clock = ScriptReceiptClock()
    market_factory = PublicWebSocketFrameAdapterFactoryV2(
        market,
        session_id="session-two-routes",
        protocol_hash=PROTOCOL_HASH,
        clock=clock,
        ingress=ingress,
        recovery_lifecycle=PassThroughRecoveryLifecycle(),
    )
    public_factory = PublicWebSocketFrameAdapterFactoryV2(
        public,
        session_id="session-two-routes",
        protocol_hash=PROTOCOL_HASH,
        clock=clock,
        ingress=ingress,
        recovery_lifecycle=PassThroughRecoveryLifecycle(),
    )
    market_adapter = market_factory(connection_id="market-g000003", generation=3)
    public_adapter = public_factory(connection_id="public-g000009", generation=9)

    await asyncio.gather(
        market_adapter.consume(_cooperative_frames("시장-1", b"market-2")),
        public_adapter.consume(_cooperative_frames(b"public-1", "공개-2")),
    )

    assert [record.ingest_seq for record in offerer.records] == [1001, 1002, 1003, 1004]
    assert [record.receipt_monotonic_ns for record in offerer.records] == [
        10001,
        10002,
        10003,
        10004,
    ]
    assert market_adapter.frame_seq == 2
    assert public_adapter.frame_seq == 2
    market_records = [record for record in offerer.records if record.route_id == "usdm_market"]
    public_records = [record for record in offerer.records if record.route_id == "usdm_public"]
    assert [
        (record.connection_id, record.generation, record.frame_seq) for record in market_records
    ] == [
        ("market-g000003", 3, 1),
        ("market-g000003", 3, 2),
    ]
    assert [
        (record.connection_id, record.generation, record.frame_seq) for record in public_records
    ] == [
        ("public-g000009", 9, 1),
        ("public-g000009", 9, 2),
    ]
    assert [record.payload_bytes() for record in market_records] == [
        "시장-1".encode(),
        b"market-2",
    ]
    assert [record.payload_bytes() for record in public_records] == [
        b"public-1",
        "공개-2".encode(),
    ]


@pytest.mark.asyncio
async def test_contended_shared_gate_preserves_receipt_order_across_routes() -> None:
    market, public = _websocket_plans()
    offerer = RecordingOfferer()
    ingress = SharedWebSocketIngressV2(
        offerer,
        recovered_wal_tail_ingest_seq=8,
    )
    second_clock = NotifyingReceiptClock(wall_ms=102, monotonic_ns=1_002)
    second = PublicWebSocketFrameAdapterFactoryV2(
        public,
        session_id="session-contention",
        protocol_hash=PROTOCOL_HASH,
        clock=second_clock,
        ingress=ingress,
        recovery_lifecycle=PassThroughRecoveryLifecycle(),
    )(connection_id="public-g000001", generation=1)
    completion_started = asyncio.Event()
    completion_release = asyncio.Event()

    async def complete(record: RawRecordV2) -> None:
        assert record.ingest_seq == 9
        assert offerer.records == [record]
        completion_started.set()
        await completion_release.wait()

    first_task = asyncio.create_task(
        ingress.offer_recovery_successor(
            plan=market,
            session_id="session-contention",
            protocol_hash=PROTOCOL_HASH,
            connection_id="market-g000001",
            generation=1,
            frame_seq=1,
            receipt=ReceiptTimestamp(
                received_at_ms=101,
                received_monotonic_ns=1_001,
            ),
            raw_payload=b"first",
            complete=complete,
        )
    )
    await completion_started.wait()
    second_task = asyncio.create_task(second.consume(_cooperative_frames(b"second")))
    await second_clock.captured.wait()
    await asyncio.sleep(0)
    assert [record.ingest_seq for record in offerer.records] == [9]

    completion_release.set()
    await asyncio.gather(first_task, second_task)

    assert [record.ingest_seq for record in offerer.records] == [9, 10]
    assert [record.receipt_monotonic_ns for record in offerer.records] == [1_001, 1_002]
    assert [record.route_id for record in offerer.records] == ["usdm_market", "usdm_public"]


@pytest.mark.asyncio
async def test_oi_waiter_reserves_before_later_websocket_receipt() -> None:
    """Regress the old lock-wait/receipt-sample inversion across transports."""

    market, _public = _websocket_plans()
    rest = _rest_plan()
    offerer = RecordingOfferer()
    ingress = SharedWebSocketIngressV2(
        offerer,
        recovered_wal_tail_ingest_seq=0,
    )
    shared_clock = ScriptReceiptClock()
    websocket = PublicWebSocketFrameAdapterFactoryV2(
        market,
        session_id="session-cross-transport-race",
        protocol_hash=PROTOCOL_HASH,
        clock=shared_clock,
        ingress=ingress,
        recovery_lifecycle=PassThroughRecoveryLifecycle(),
    )(connection_id="market-g000001", generation=1)

    await ingress._ingress_lock.acquire()
    try:
        oi_task = asyncio.create_task(
            ingress.offer_https_attempt(
                plan=rest,
                session_id="session-cross-transport-race",
                protocol_hash=PROTOCOL_HASH,
                connection_id="oi-rest-producer",
                generation=1,
                symbol="BTCUSDT",
                clock=shared_clock,
                observation=_rest_observation(rest),
                source_logical_key="openInterest:BTCUSDT",
            )
        )
        await asyncio.sleep(0)
        oi_receipts_before_websocket = shared_clock.calls

        websocket_task = asyncio.create_task(
            websocket.consume(_cooperative_frames(b"market-after-oi"))
        )
        await asyncio.sleep(0)
        await asyncio.sleep(0)
        assert ingress.pending_reservation_count == 2
    finally:
        ingress._ingress_lock.release()

    oi_receipt, _ = await asyncio.gather(oi_task, websocket_task)

    assert oi_receipts_before_websocket == 1
    assert shared_clock.calls == 2
    assert [record.ingest_seq for record in offerer.records] == [1, 2]
    assert [record.route_id for record in offerer.records] == [
        "usdm_public_rest",
        "usdm_market",
    ]
    assert [record.receipt_monotonic_ns for record in offerer.records] == [10_001, 10_002]
    assert oi_receipt.record is offerer.records[0]
    assert ingress.pending_reservation_count == 0


@pytest.mark.asyncio
@pytest.mark.parametrize("backdated_dimension", ["wall", "monotonic"])
async def test_backdated_shared_receipt_fail_closes_following_ingress(
    backdated_dimension: str,
) -> None:
    market, _public = _websocket_plans()
    offerer = RecordingOfferer()
    ingress = SharedWebSocketIngressV2(
        offerer,
        recovered_wal_tail_ingest_seq=0,
    )
    await ingress.offer_frame(
        plan=market,
        session_id="session-backdated-shared-receipt",
        protocol_hash=PROTOCOL_HASH,
        connection_id="market-g000001",
        generation=1,
        frame_seq=1,
        receipt=ReceiptTimestamp(100, 1_000),
        raw_payload=b"first",
    )
    backdated = ReceiptTimestamp(
        99 if backdated_dimension == "wall" else 101,
        999 if backdated_dimension == "monotonic" else 1_001,
    )

    with pytest.raises(
        SharedIngressOrderingErrorV2,
        match=f"producer {backdated_dimension} receipt moved backwards",
    ):
        await ingress.offer_frame(
            plan=market,
            session_id="session-backdated-shared-receipt",
            protocol_hash=PROTOCOL_HASH,
            connection_id="market-g000001",
            generation=1,
            frame_seq=2,
            receipt=backdated,
            raw_payload=b"backdated",
        )

    with pytest.raises(
        SharedIngressOrderingErrorV2,
        match="shared ingress is fail-closed after an ordering failure",
    ):
        await ingress.offer_frame(
            plan=market,
            session_id="session-backdated-shared-receipt",
            protocol_hash=PROTOCOL_HASH,
            connection_id="market-g000001",
            generation=1,
            frame_seq=3,
            receipt=ReceiptTimestamp(102, 1_002),
            raw_payload=b"would-be-successor",
        )

    assert [record.ingest_seq for record in offerer.records] == [1]
    assert ingress.pending_reservation_count == 0


@pytest.mark.asyncio
async def test_https_intra_attempt_wall_regression_is_admitted_with_sealed_evidence() -> None:
    rest = _rest_plan()
    offerer = RecordingOfferer()
    ingress = SharedWebSocketIngressV2(
        offerer,
        recovered_wal_tail_ingest_seq=0,
    )
    observation = PublicOiRestTerminalObservationV2.for_plan(
        rest,
        symbol="BTCUSDT",
        poll_cycle_seq=1,
        symbol_ordinal=0,
        scheduled_slot_wall_ms=0,
        attempt=1,
        request_started_wall_ms=100,
        request_started_monotonic_ns=1_000,
        response_first_header_wall_ms=99,
        response_first_header_monotonic_ns=1_001,
        attempt_ended_wall_ms=98,
        attempt_ended_monotonic_ns=1_002,
        response_status=200,
        response_headers=(),
        payload_complete=True,
        body=b"{}",
    )

    receipt = await ingress.offer_https_attempt(
        plan=rest,
        session_id="session-intra-wall-regression",
        protocol_hash=PROTOCOL_HASH,
        connection_id="oi-rest-producer",
        generation=1,
        symbol="BTCUSDT",
        clock=NotifyingReceiptClock(wall_ms=97, monotonic_ns=1_003),
        observation=observation,
        source_logical_key="openInterest:BTCUSDT",
    )

    assert offerer.records == [receipt.record]
    evidence = receipt.wall_clock_regression
    assert type(evidence) is PublicHttpsRestWallClockRegressionEvidenceV2
    validate_public_https_rest_wall_clock_regression_evidence_v2(evidence)
    assert evidence.intra_attempt_regression is True
    assert evidence.prior_global_regression is False
    assert evidence.ingest_seq == receipt.record.ingest_seq == 1
    assert (
        evidence.request_started_wall_ms,
        evidence.response_first_header_wall_ms,
        evidence.attempt_ended_wall_ms,
        evidence.completion_admission_wall_ms,
    ) == (100, 99, 98, 97)
    assert validate_public_oi_admission_receipt_v2(receipt) is receipt.record
    with pytest.raises(TypeError, match="only be created by the shared ingress"):
        replace(evidence)


@pytest.mark.asyncio
async def test_https_prior_global_wall_regression_preserves_high_water_evidence() -> None:
    market, _public = _websocket_plans()
    rest = _rest_plan()
    offerer = RecordingOfferer()
    ingress = SharedWebSocketIngressV2(
        offerer,
        recovered_wal_tail_ingest_seq=0,
    )
    await ingress.offer_frame(
        plan=market,
        session_id="session-prior-global-wall-regression",
        protocol_hash=PROTOCOL_HASH,
        connection_id="market-g000001",
        generation=1,
        frame_seq=1,
        receipt=ReceiptTimestamp(200, 900),
        raw_payload=b"global-high-water",
    )

    receipt = await ingress.offer_https_attempt(
        plan=rest,
        session_id="session-prior-global-wall-regression",
        protocol_hash=PROTOCOL_HASH,
        connection_id="oi-rest-producer",
        generation=1,
        symbol="BTCUSDT",
        clock=NotifyingReceiptClock(wall_ms=150, monotonic_ns=1_003),
        observation=_rest_observation(
            rest,
            request_started_wall_ms=100,
            request_started_monotonic_ns=1_000,
        ),
        source_logical_key="openInterest:BTCUSDT",
    )

    assert [record.ingest_seq for record in offerer.records] == [1, 2]
    evidence = receipt.wall_clock_regression
    assert type(evidence) is PublicHttpsRestWallClockRegressionEvidenceV2
    assert evidence.intra_attempt_regression is False
    assert evidence.prior_global_regression is True
    assert evidence.prior_global_wall_ms == 200
    assert evidence.completion_admission_wall_ms == 150

    with pytest.raises(
        SharedIngressOrderingErrorV2,
        match="producer wall receipt moved backwards",
    ):
        await ingress.offer_frame(
            plan=market,
            session_id="session-prior-global-wall-regression",
            protocol_hash=PROTOCOL_HASH,
            connection_id="market-g000001",
            generation=1,
            frame_seq=2,
            receipt=ReceiptTimestamp(175, 1_004),
            raw_payload=b"websocket-remains-strict-against-high-water",
        )
    assert [record.ingest_seq for record in offerer.records] == [1, 2]


@pytest.mark.asyncio
async def test_https_equal_wall_boundaries_need_no_regression_evidence() -> None:
    market, _public = _websocket_plans()
    rest = _rest_plan()
    offerer = RecordingOfferer()
    ingress = SharedWebSocketIngressV2(
        offerer,
        recovered_wal_tail_ingest_seq=0,
    )
    await ingress.offer_frame(
        plan=market,
        session_id="session-equal-wall-boundary",
        protocol_hash=PROTOCOL_HASH,
        connection_id="market-g000001",
        generation=1,
        frame_seq=1,
        receipt=ReceiptTimestamp(100, 900),
        raw_payload=b"equal-wall-high-water",
    )
    observation = PublicOiRestTerminalObservationV2.for_plan(
        rest,
        symbol="BTCUSDT",
        poll_cycle_seq=1,
        symbol_ordinal=0,
        scheduled_slot_wall_ms=0,
        attempt=1,
        request_started_wall_ms=100,
        request_started_monotonic_ns=1_000,
        response_first_header_wall_ms=100,
        response_first_header_monotonic_ns=1_001,
        attempt_ended_wall_ms=100,
        attempt_ended_monotonic_ns=1_002,
        response_status=200,
        response_headers=(),
        payload_complete=True,
        body=b"{}",
    )

    receipt = await ingress.offer_https_attempt(
        plan=rest,
        session_id="session-equal-wall-boundary",
        protocol_hash=PROTOCOL_HASH,
        connection_id="oi-rest-producer",
        generation=1,
        symbol="BTCUSDT",
        clock=NotifyingReceiptClock(wall_ms=100, monotonic_ns=1_003),
        observation=observation,
        source_logical_key="openInterest:BTCUSDT",
    )

    assert receipt.wall_clock_regression is None
    assert [record.ingest_seq for record in offerer.records] == [1, 2]


@pytest.mark.asyncio
async def test_https_prior_global_monotonic_regression_still_fail_closes_without_row() -> None:
    market, _public = _websocket_plans()
    rest = _rest_plan()
    offerer = RecordingOfferer()
    ingress = SharedWebSocketIngressV2(
        offerer,
        recovered_wal_tail_ingest_seq=0,
    )
    await ingress.offer_frame(
        plan=market,
        session_id="session-prior-global-monotonic-regression",
        protocol_hash=PROTOCOL_HASH,
        connection_id="market-g000001",
        generation=1,
        frame_seq=1,
        receipt=ReceiptTimestamp(100, 1_000),
        raw_payload=b"monotonic-high-water",
    )

    with pytest.raises(
        SharedIngressOrderingErrorV2,
        match="producer monotonic receipt moved backwards",
    ):
        await ingress.offer_https_attempt(
            plan=rest,
            session_id="session-prior-global-monotonic-regression",
            protocol_hash=PROTOCOL_HASH,
            connection_id="oi-rest-producer",
            generation=1,
            symbol="BTCUSDT",
            clock=NotifyingReceiptClock(wall_ms=101, monotonic_ns=900),
            observation=_rest_observation(
                rest,
                request_started_wall_ms=100,
                request_started_monotonic_ns=800,
            ),
            source_logical_key="openInterest:BTCUSDT",
        )

    assert [record.ingest_seq for record in offerer.records] == [1]
    assert ingress.pending_reservation_count == 0


@pytest.mark.asyncio
async def test_cancelled_pending_reservation_fail_closes_without_sequence_hole() -> None:
    market, _public = _websocket_plans()
    offerer = RecordingOfferer()
    ingress = SharedWebSocketIngressV2(
        offerer,
        recovered_wal_tail_ingest_seq=0,
    )
    await ingress._ingress_lock.acquire()
    pending = asyncio.create_task(
        ingress.offer_frame(
            plan=market,
            session_id="session-cancelled-reservation",
            protocol_hash=PROTOCOL_HASH,
            connection_id="market-g000001",
            generation=1,
            frame_seq=1,
            receipt=ReceiptTimestamp(100, 1_000),
            raw_payload=b"cancel-before-admission",
        )
    )
    await asyncio.sleep(0)
    assert ingress.pending_reservation_count == 1

    pending.cancel()
    with pytest.raises(asyncio.CancelledError):
        await pending
    ingress._ingress_lock.release()

    assert ingress.pending_reservation_count == 0
    with pytest.raises(
        SharedIngressOrderingErrorV2,
        match="shared ingress is fail-closed after an ordering failure",
    ):
        await ingress.offer_frame(
            plan=market,
            session_id="session-cancelled-reservation",
            protocol_hash=PROTOCOL_HASH,
            connection_id="market-g000001",
            generation=1,
            frame_seq=2,
            receipt=ReceiptTimestamp(101, 1_001),
            raw_payload=b"must-not-cross-hole",
        )
    assert offerer.records == []


@pytest.mark.asyncio
async def test_pending_reservation_capacity_accepts_boundary_then_fail_closes() -> None:
    market, _public = _websocket_plans()
    offerer = RecordingOfferer()
    ingress = SharedWebSocketIngressV2(
        offerer,
        recovered_wal_tail_ingest_seq=0,
    )
    pending: list[asyncio.Task[RawRecordV2]] = []
    await ingress._ingress_lock.acquire()
    try:
        for ordinal in range(16):
            pending.append(
                asyncio.create_task(
                    ingress.offer_frame(
                        plan=market,
                        session_id="session-reservation-capacity",
                        protocol_hash=PROTOCOL_HASH,
                        connection_id="market-g000001",
                        generation=1,
                        frame_seq=ordinal + 1,
                        receipt=ReceiptTimestamp(100 + ordinal, 1_000 + ordinal),
                        raw_payload=f"pending-{ordinal + 1}".encode(),
                    )
                )
            )
            await asyncio.sleep(0)
        assert ingress.pending_reservation_count == 16

        with pytest.raises(
            SharedIngressOrderingErrorV2,
            match="bounded pending-reservation capacity",
        ):
            await ingress.offer_frame(
                plan=market,
                session_id="session-reservation-capacity",
                protocol_hash=PROTOCOL_HASH,
                connection_id="market-g000001",
                generation=1,
                frame_seq=17,
                receipt=ReceiptTimestamp(116, 1_016),
                raw_payload=b"overflow",
            )
    finally:
        for task in pending:
            task.cancel()
        await asyncio.gather(*pending, return_exceptions=True)
        ingress._ingress_lock.release()

    assert ingress.pending_reservation_count == 0
    assert offerer.records == []


@pytest.mark.asyncio
async def test_websocket_and_https_share_one_contiguous_recovered_sequence() -> None:
    market, _public = _websocket_plans()
    rest = _rest_plan()
    offerer = RecordingOfferer()
    ingress = SharedWebSocketIngressV2(
        offerer,
        recovered_wal_tail_ingest_seq=700,
    )
    rest_clock = ScriptReceiptClock()
    observation = _rest_observation(rest)

    websocket_before = await ingress.offer_frame(
        plan=market,
        session_id="session-cross-transport",
        protocol_hash=PROTOCOL_HASH,
        connection_id="market-g000001",
        generation=1,
        frame_seq=1,
        receipt=ReceiptTimestamp(1_699_999_999_999, 9_999),
        raw_payload=b"ws-before",
    )

    https_receipt = await ingress.offer_https_attempt(
        plan=rest,
        session_id="session-cross-transport",
        protocol_hash=PROTOCOL_HASH,
        connection_id="oi-rest-producer",
        generation=3,
        symbol="BTCUSDT",
        clock=rest_clock,
        observation=observation,
        source_logical_key="openInterest:BTCUSDT",
    )
    websocket_after = await ingress.offer_frame(
        plan=market,
        session_id="session-cross-transport",
        protocol_hash=PROTOCOL_HASH,
        connection_id="market-g000001",
        generation=1,
        frame_seq=2,
        receipt=ReceiptTimestamp(1_700_000_000_002, 10_002),
        raw_payload=b"ws-after",
    )

    assert [record.ingest_seq for record in offerer.records] == [701, 702, 703]
    https = https_receipt.record
    assert (websocket_before.ingest_seq, https.ingest_seq, websocket_after.ingest_seq) == (
        701,
        702,
        703,
    )
    assert https.session_id == "session-cross-transport"
    assert https.plan_id == rest.name
    assert https.protocol_hash == PROTOCOL_HASH
    assert https.transport is TransportV2.HTTPS
    assert https.venue is VenueV2.USDM_FUTURES
    assert https.route_id == "usdm_public_rest"
    assert https.symbol == "BTCUSDT"
    assert https.connection_id == "oi-rest-producer"
    assert https.generation == 3
    assert https.frame_seq is None
    assert https.receipt_wall_ms == 1_700_000_000_001
    assert https.receipt_monotonic_ns == 10_001
    assert https.source_logical_key == "openInterest:BTCUSDT"
    retained = _retained_rest_payload(https)
    assert retained.symbol == "BTCUSDT"
    assert retained.symbol_ordinal == 0
    assert retained.canonical_query == (("symbol", "BTCUSDT"),)
    assert retained.completion_admission_wall_ms == 1_700_000_000_001
    assert retained.completion_admission_monotonic_ns == 10_001
    assert retained.body_bytes() == observation.body
    assert type(https_receipt) is PublicOiAdmissionReceiptV2
    assert validate_public_oi_admission_receipt_v2(https_receipt) is https
    assert https_receipt.accepted_ingest_seq == 702
    assert https_receipt.queued_record.record is https
    with pytest.raises(TypeError, match="only be created by the shared ingress"):
        PublicOiAdmissionReceiptV2(
            record=https,
            queue_admission_receipt=https_receipt.queue_admission_receipt,
            _factory_token=object(),
        )
    with pytest.raises(TypeError, match="only be created by the shared ingress"):
        replace(https_receipt)


@pytest.mark.asyncio
async def test_https_census_admits_all_three_exact_schemas_with_fixed_identity() -> None:
    plan = _rest_plan(("BTCUSDT", "ETHUSDT"))
    payloads = _rest_census_payloads(plan)
    offerer = RecordingOfferer()
    ingress = SharedWebSocketIngressV2(
        offerer,
        recovered_wal_tail_ingest_seq=40,
    )
    clock = ScriptReceiptClock()

    receipts: list[PublicOiCensusAdmissionReceiptV2] = []
    for payload in payloads:
        receipts.append(
            await ingress.offer_https_census(
                plan=plan,
                session_id="session-census",
                protocol_hash=PROTOCOL_HASH,
                clock=clock,
                payload=payload,
            )
        )

    assert [record.ingest_seq for record in offerer.records] == [41, 42, 43]
    assert [receipt.accepted_ingest_seq for receipt in receipts] == [41, 42, 43]
    assert offerer._handoff is not None
    assert offerer._handoff.accepted_tail_ingest_seq == 43
    assert clock.calls == 3
    for receipt, payload in zip(receipts, payloads, strict=True):
        record = receipt.record
        assert type(receipt) is PublicOiCensusAdmissionReceiptV2
        assert validate_public_oi_census_admission_receipt_v2(receipt) is record
        assert receipt.queued_record.record is record
        assert record.session_id == "session-census"
        assert record.plan_id == plan.name
        assert record.protocol_hash == PROTOCOL_HASH
        assert record.transport is TransportV2.HTTPS
        assert record.venue is VenueV2.USDM_FUTURES
        assert record.route_id == plan.route_id
        assert record.symbol is None
        assert record.connection_id == "oi-rest-census"
        assert record.generation == 1
        assert record.frame_seq is None
        assert record.source_logical_key == "openInterest:census"
        assert record.payload_bytes() == payload.canonical_bytes()

    first = receipts[0]
    with pytest.raises(TypeError, match="only be created by the shared ingress"):
        PublicOiCensusAdmissionReceiptV2(
            record=first.record,
            queue_admission_receipt=first.queue_admission_receipt,
            _factory_token=object(),
        )
    with pytest.raises(TypeError, match="only be created by the shared ingress"):
        replace(first)
    with pytest.raises(ValueError, match="wrong fixed outer identity"):
        PublicOiCensusAdmissionReceiptV2(
            record=replace(first.record, connection_id="wrong-census-owner"),
            queue_admission_receipt=first.queue_admission_receipt,
            _factory_token=(websocket_module._PUBLIC_OI_CENSUS_ADMISSION_RECEIPT_FACTORY_TOKEN),
        )
    with pytest.raises(ValueError, match="payload identity differs"):
        PublicOiCensusAdmissionReceiptV2(
            record=replace(first.record, session_id="another-census-session"),
            queue_admission_receipt=first.queue_admission_receipt,
            _factory_token=(websocket_module._PUBLIC_OI_CENSUS_ADMISSION_RECEIPT_FACTORY_TOKEN),
        )


@pytest.mark.asyncio
async def test_https_census_reserves_receipt_before_waiting_for_admission_turn() -> None:
    market, _public = _websocket_plans()
    plan = _rest_plan()
    payload = _rest_census_payloads(plan)[0]
    offerer = RecordingOfferer()
    ingress = SharedWebSocketIngressV2(
        offerer,
        recovered_wal_tail_ingest_seq=0,
    )
    completion_started = asyncio.Event()
    completion_release = asyncio.Event()
    census_clock = NotifyingReceiptClock(
        wall_ms=_CENSUS_SLOT + 4_001,
        monotonic_ns=200_000,
    )

    async def complete(record: RawRecordV2) -> None:
        assert record.ingest_seq == 1
        completion_started.set()
        await completion_release.wait()

    predecessor = asyncio.create_task(
        ingress.offer_recovery_successor(
            plan=market,
            session_id="session-census",
            protocol_hash=PROTOCOL_HASH,
            connection_id="market-g000001",
            generation=1,
            frame_seq=1,
            receipt=ReceiptTimestamp(_CENSUS_SLOT + 4_000, 199_999),
            raw_payload=b"predecessor",
            complete=complete,
        )
    )
    await completion_started.wait()
    census = asyncio.create_task(
        ingress.offer_https_census(
            plan=plan,
            session_id="session-census",
            protocol_hash=PROTOCOL_HASH,
            clock=census_clock,
            payload=payload,
        )
    )
    await asyncio.sleep(0)

    assert census_clock.captured.is_set()
    assert ingress.pending_reservation_count == 2
    assert [record.ingest_seq for record in offerer.records] == [1]

    completion_release.set()
    predecessor_record, census_receipt = await asyncio.gather(predecessor, census)

    assert predecessor_record.ingest_seq == 1
    assert census_receipt.accepted_ingest_seq == 2
    assert [record.ingest_seq for record in offerer.records] == [1, 2]
    assert ingress.pending_reservation_count == 0


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("payload_index", "backdated_dimension"),
    tuple(
        (payload_index, dimension)
        for payload_index in range(3)
        for dimension in ("wall", "monotonic")
    ),
)
async def test_https_census_rejects_backdated_outer_receipt_without_sequence_gap(
    payload_index: int,
    backdated_dimension: str,
) -> None:
    plan = _rest_plan()
    payload = _rest_census_payloads(plan)[payload_index]
    if type(payload) is PublicOiRestSlotCensusV2:
        terminal_wall_ms = payload.closed_wall_ms
        terminal_monotonic_ns = payload.closed_monotonic_ns
    elif type(payload) is PublicOiRestForwardGapRangeV2:
        terminal_wall_ms = payload.observed_wall_ms
        terminal_monotonic_ns = payload.observed_monotonic_ns
    else:
        assert type(payload) is PublicOiRestCoverageCloseV2
        terminal_wall_ms = payload.stop_requested_wall_ms
        terminal_monotonic_ns = payload.stop_requested_monotonic_ns
    backdated_clock = NotifyingReceiptClock(
        wall_ms=terminal_wall_ms - (backdated_dimension == "wall"),
        monotonic_ns=(terminal_monotonic_ns - (backdated_dimension == "monotonic")),
    )
    offerer = RecordingOfferer()
    ingress = SharedWebSocketIngressV2(
        offerer,
        recovered_wal_tail_ingest_seq=0,
    )

    with pytest.raises(
        ValueError,
        match=f"outer {backdated_dimension} receipt precedes",
    ):
        await ingress.offer_https_census(
            plan=plan,
            session_id="session-census",
            protocol_hash=PROTOCOL_HASH,
            clock=backdated_clock,
            payload=payload,
        )

    assert backdated_clock.captured.is_set()
    assert offerer.records == []
    admitted = await ingress.offer_https_census(
        plan=plan,
        session_id="session-census",
        protocol_hash=PROTOCOL_HASH,
        clock=NotifyingReceiptClock(
            wall_ms=terminal_wall_ms,
            monotonic_ns=terminal_monotonic_ns,
        ),
        payload=payload,
    )
    assert admitted.accepted_ingest_seq == 1


@pytest.mark.asyncio
async def test_https_census_rejects_fake_wrong_plan_session_and_lineage_pre_admission() -> None:
    plan = _rest_plan()
    wrong_plan = _rest_plan(("ETHUSDT",))
    payload = _rest_census_payloads(plan)[0]
    offerer = RecordingOfferer()
    ingress = SharedWebSocketIngressV2(
        offerer,
        recovered_wal_tail_ingest_seq=0,
    )
    clock = ScriptReceiptClock()

    with pytest.raises(TypeError, match="exact slot, forward-gap, or coverage-close"):
        await ingress.offer_https_census(
            plan=plan,
            session_id="session-census",
            protocol_hash=PROTOCOL_HASH,
            clock=clock,
            payload=object(),  # type: ignore[arg-type]
        )
    with pytest.raises(ValueError, match="differs from the exact REST plan"):
        await ingress.offer_https_census(
            plan=wrong_plan,
            session_id="session-census",
            protocol_hash=PROTOCOL_HASH,
            clock=clock,
            payload=payload,
        )
    with pytest.raises(ValueError, match="differs from its outer session"):
        await ingress.offer_https_census(
            plan=plan,
            session_id="another-session",
            protocol_hash=PROTOCOL_HASH,
            clock=clock,
            payload=payload,
        )
    with pytest.raises(ValueError, match="session_id must be a bounded normalized identity"):
        await ingress.offer_https_census(
            plan=plan,
            session_id=" session-census",
            protocol_hash=PROTOCOL_HASH,
            clock=clock,
            payload=payload,
        )

    assert clock.calls == 0
    assert offerer.records == []

    admitted = await ingress.offer_https_census(
        plan=plan,
        session_id="session-census",
        protocol_hash=PROTOCOL_HASH,
        clock=clock,
        payload=payload,
    )
    assert admitted.accepted_ingest_seq == 1


@pytest.mark.asyncio
async def test_https_census_rejects_synthetic_queued_record_without_handoff_proof() -> None:
    plan = _rest_plan()
    payload = _rest_census_payloads(plan)[0]

    class SyntheticQueuedOfferer:
        def __init__(self) -> None:
            self.offer_calls = 0

        def offer(self, record: RawRecordV2) -> object:
            del record
            raise AssertionError("ordinary offer is not used by this HTTPS test")

        def offer_with_admission_receipt(self, record: RawRecordV2) -> object:
            self.offer_calls += 1
            return QueuedRawRecordV2.encode(
                record,
                enqueued_monotonic_ns=record.receipt_monotonic_ns,
            )

        def validate_queue_admission_receipt_v2(
            self,
            receipt: CaptureQueueAdmissionReceiptV2,
        ) -> QueuedRawRecordV2:
            del receipt
            raise AssertionError("synthetic queued record must fail before validation")

    offerer = SyntheticQueuedOfferer()
    ingress = SharedWebSocketIngressV2(
        offerer,
        recovered_wal_tail_ingest_seq=0,
    )

    with pytest.raises(TypeError, match="exact queue-admission receipt"):
        await ingress.offer_https_census(
            plan=plan,
            session_id="session-census",
            protocol_hash=PROTOCOL_HASH,
            clock=ScriptReceiptClock(),
            payload=payload,
        )

    assert offerer.offer_calls == 1


@pytest.mark.asyncio
async def test_https_census_cross_handoff_failure_has_one_admission_and_no_retry() -> None:
    plan = _rest_plan()
    payload = _rest_census_payloads(plan)[0]
    expected_handoff = _bounded_test_handoff(1)
    wrong_handoff = _bounded_test_handoff(1)

    class WrongHandoffOfferer:
        def __init__(self) -> None:
            self.offer_calls = 0

        def offer(self, record: RawRecordV2) -> QueuedRawRecordV2:
            return expected_handoff.offer(record)

        def offer_with_admission_receipt(
            self,
            record: RawRecordV2,
        ) -> CaptureQueueAdmissionReceiptV2:
            self.offer_calls += 1
            return wrong_handoff.offer_with_admission_receipt(record)

        def validate_queue_admission_receipt_v2(
            self,
            receipt: CaptureQueueAdmissionReceiptV2,
        ) -> QueuedRawRecordV2:
            return expected_handoff.validate_queue_admission_receipt_v2(receipt)

    offerer = WrongHandoffOfferer()
    ingress = SharedWebSocketIngressV2(
        offerer,
        recovered_wal_tail_ingest_seq=0,
    )

    with pytest.raises(ValueError, match="different bounded handoff"):
        await ingress.offer_https_census(
            plan=plan,
            session_id="session-census",
            protocol_hash=PROTOCOL_HASH,
            clock=ScriptReceiptClock(),
            payload=payload,
        )

    assert offerer.offer_calls == 1
    assert wrong_handoff.accepted_tail_ingest_seq == 1
    assert wrong_handoff.current_events == 1
    assert expected_handoff.accepted_tail_ingest_seq == 0
    assert expected_handoff.current_events == 0
    wrong_handoff.discard_all()
    expected_handoff.discard_all()


@pytest.mark.asyncio
async def test_https_rejects_synthetic_queued_record_without_handoff_proof() -> None:
    rest = _rest_plan()

    class InvalidQueuedOfferer:
        def __init__(self) -> None:
            self.records: list[RawRecordV2] = []

        def offer(self, record: RawRecordV2) -> object:
            del record
            raise AssertionError("ordinary offer is not used by this HTTPS test")

        def offer_with_admission_receipt(self, record: RawRecordV2) -> object:
            self.records.append(record)
            return QueuedRawRecordV2.encode(
                record,
                enqueued_monotonic_ns=record.receipt_monotonic_ns,
            )

        def validate_queue_admission_receipt_v2(
            self,
            receipt: CaptureQueueAdmissionReceiptV2,
        ) -> QueuedRawRecordV2:
            del receipt
            raise AssertionError("synthetic queued record must fail before validation")

    offerer = InvalidQueuedOfferer()
    ingress = SharedWebSocketIngressV2(
        offerer,
        recovered_wal_tail_ingest_seq=0,
    )

    with pytest.raises(TypeError, match="exact queue-admission receipt"):
        await ingress.offer_https_attempt(
            plan=rest,
            session_id="session-invalid-queued-offer",
            protocol_hash=PROTOCOL_HASH,
            connection_id="oi-rest-producer",
            generation=1,
            symbol="BTCUSDT",
            clock=ScriptReceiptClock(),
            observation=_rest_observation(rest),
            source_logical_key="openInterest:BTCUSDT",
        )

    assert len(offerer.records) == 1


@pytest.mark.asyncio
async def test_https_rejects_receipt_from_a_different_handoff_after_one_accept() -> None:
    rest = _rest_plan()
    expected_handoff = _bounded_test_handoff(1)
    wrong_handoff = _bounded_test_handoff(1)

    class WrongHandoffOfferer:
        def __init__(self) -> None:
            self.offer_calls = 0

        def offer(self, record: RawRecordV2) -> QueuedRawRecordV2:
            return expected_handoff.offer(record)

        def offer_with_admission_receipt(
            self,
            record: RawRecordV2,
        ) -> CaptureQueueAdmissionReceiptV2:
            self.offer_calls += 1
            return wrong_handoff.offer_with_admission_receipt(record)

        def validate_queue_admission_receipt_v2(
            self,
            receipt: CaptureQueueAdmissionReceiptV2,
        ) -> QueuedRawRecordV2:
            return expected_handoff.validate_queue_admission_receipt_v2(receipt)

    offerer = WrongHandoffOfferer()
    ingress = SharedWebSocketIngressV2(
        offerer,
        recovered_wal_tail_ingest_seq=0,
    )

    with pytest.raises(ValueError, match="different bounded handoff"):
        await ingress.offer_https_attempt(
            plan=rest,
            session_id="session-wrong-handoff-offer",
            protocol_hash=PROTOCOL_HASH,
            connection_id="oi-rest-producer",
            generation=1,
            symbol="BTCUSDT",
            clock=ScriptReceiptClock(),
            observation=_rest_observation(rest),
            source_logical_key="openInterest:BTCUSDT",
        )

    assert offerer.offer_calls == 1
    assert wrong_handoff.accepted_tail_ingest_seq == 1
    assert wrong_handoff.current_events == 1
    assert expected_handoff.accepted_tail_ingest_seq == 0
    assert expected_handoff.current_events == 0
    wrong_handoff.discard_all()
    expected_handoff.discard_all()


@pytest.mark.asyncio
async def test_https_receipt_equals_the_real_handoff_accepted_tail() -> None:
    rest = _rest_plan()
    handoff = BoundedBatchHandoffV2(
        BatchPolicyV2(
            max_records=4,
            max_encoded_bytes=80_000,
            max_linger_us=1_000,
            queue_max_events=8,
            queue_max_encoded_bytes=160_000,
            low_water_events=2,
            low_water_encoded_bytes=40_000,
            qualification_id="oi-admission-receipt-tail-test",
        ),
        monotonic_ns=lambda: 20_000,
        expected_first_ingest_seq=701,
    )
    ingress = SharedWebSocketIngressV2(
        handoff,
        recovered_wal_tail_ingest_seq=700,
    )

    receipt = await ingress.offer_https_attempt(
        plan=rest,
        session_id="session-real-handoff-tail",
        protocol_hash=PROTOCOL_HASH,
        connection_id="oi-rest-producer",
        generation=1,
        symbol="BTCUSDT",
        clock=ScriptReceiptClock(),
        observation=_rest_observation(rest),
        source_logical_key="openInterest:BTCUSDT",
    )

    assert receipt.accepted_ingest_seq == 701
    assert handoff.accepted_tail_ingest_seq == receipt.accepted_ingest_seq
    assert receipt.queued_record.record is receipt.record


@pytest.mark.asyncio
async def test_https_waits_for_recovery_gate_then_selects_admission_cancellation() -> None:
    market, _public = _websocket_plans()
    rest = _rest_plan()
    offerer = RecordingOfferer()
    ingress = SharedWebSocketIngressV2(
        offerer,
        recovered_wal_tail_ingest_seq=8,
    )
    recovery_started = asyncio.Event()
    release_recovery = asyncio.Event()

    async def complete(record: RawRecordV2) -> None:
        assert record.ingest_seq == 9
        recovery_started.set()
        await release_recovery.wait()

    recovery_task = asyncio.create_task(
        ingress.offer_recovery_successor(
            plan=market,
            session_id="session-rest-contention",
            protocol_hash=PROTOCOL_HASH,
            connection_id="market-g000001",
            generation=1,
            frame_seq=1,
            receipt=ReceiptTimestamp(101, 1_001),
            raw_payload=b"recovery",
            complete=complete,
        )
    )
    await recovery_started.wait()
    rest_clock = NotifyingReceiptClock(wall_ms=102, monotonic_ns=1_002)
    cancellation_requested = asyncio.Event()
    rest_task = asyncio.create_task(
        ingress.offer_https_attempt(
            plan=rest,
            session_id="session-rest-contention",
            protocol_hash=PROTOCOL_HASH,
            connection_id="oi-rest-producer",
            generation=1,
            symbol="BTCUSDT",
            clock=rest_clock,
            observation=_rest_observation(rest),
            source_logical_key="openInterest:BTCUSDT",
            cancellation_requested=cancellation_requested,
        )
    )
    await asyncio.sleep(0)

    assert rest_clock.captured.is_set()
    assert ingress.pending_reservation_count == 2
    assert [record.ingest_seq for record in offerer.records] == [9]

    cancellation_requested.set()
    release_recovery.set()
    _recovery, https_receipt = await asyncio.gather(recovery_task, rest_task)
    https = https_receipt.record

    assert [record.ingest_seq for record in offerer.records] == [9, 10]
    assert ingress.pending_reservation_count == 0
    assert https.receipt_monotonic_ns == 1_002
    retained = _retained_rest_payload(https)
    assert retained.completion_admission_wall_ms == 102
    assert retained.completion_admission_monotonic_ns == 1_002
    assert retained.admission_cancellation_requested is True
    assert retained.error_category is None
    assert retained.error_detail is None


@pytest.mark.asyncio
@pytest.mark.parametrize("cancelled", [False, True])
async def test_https_admission_cancellation_flag_selects_exact_payload(
    cancelled: bool,
) -> None:
    rest = _rest_plan()
    offerer = RecordingOfferer()
    ingress = SharedWebSocketIngressV2(
        offerer,
        recovered_wal_tail_ingest_seq=20,
    )
    cancellation_requested = asyncio.Event()
    if cancelled:
        cancellation_requested.set()

    receipt = await ingress.offer_https_attempt(
        plan=rest,
        session_id="session-rest-cancellation",
        protocol_hash=PROTOCOL_HASH,
        connection_id="oi-rest-producer",
        generation=1,
        symbol="BTCUSDT",
        clock=ScriptReceiptClock(),
        observation=_rest_observation(rest),
        source_logical_key="openInterest:BTCUSDT",
        cancellation_requested=cancellation_requested,
    )

    record = receipt.record
    payload = _retained_rest_payload(record)
    assert record.ingest_seq == 21
    assert payload.admission_cancellation_requested is cancelled
    assert payload.error_category is None
    assert payload.error_detail is None


@pytest.mark.asyncio
async def test_cancellation_scheduled_after_receipt_cannot_rewrite_retained_attempt() -> None:
    rest = _rest_plan()
    offerer = RecordingOfferer()
    ingress = SharedWebSocketIngressV2(
        offerer,
        recovered_wal_tail_ingest_seq=30,
    )
    cancellation_requested = asyncio.Event()
    clock = ScheduleCancellationOnCaptureClock(
        cancellation_requested,
        wall_ms=1_700_000_000_001,
        monotonic_ns=10_001,
    )

    receipt = await ingress.offer_https_attempt(
        plan=rest,
        session_id="session-rest-atomic-cancellation",
        protocol_hash=PROTOCOL_HASH,
        connection_id="oi-rest-producer",
        generation=1,
        symbol="BTCUSDT",
        clock=clock,
        observation=_rest_observation(rest),
        source_logical_key="openInterest:BTCUSDT",
        cancellation_requested=cancellation_requested,
    )
    retained_before_callback = _retained_rest_payload(receipt.record)

    await asyncio.sleep(0)

    assert cancellation_requested.is_set()
    assert retained_before_callback.admission_cancellation_requested is False
    assert retained_before_callback.error_category is None
    assert _retained_rest_payload(offerer.records[0]).admission_cancellation_requested is False
    assert _retained_rest_payload(offerer.records[0]).error_category is None


@pytest.mark.asyncio
async def test_https_malformed_and_backdated_payloads_consume_no_sequence(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    rest = _rest_plan()
    events: list[str] = []
    offerer = RecordingOfferer(events)
    ingress = SharedWebSocketIngressV2(
        offerer,
        recovered_wal_tail_ingest_seq=40,
    )
    clock = ScriptReceiptClock(events)

    backdated = _rest_observation(
        rest,
        request_started_wall_ms=1_800_000_000_000,
        request_started_monotonic_ns=20_000,
    )
    with pytest.raises(ValueError, match="completion admission precedes request start"):
        await ingress.offer_https_attempt(
            plan=rest,
            session_id="session-rest-validation-failure",
            protocol_hash=PROTOCOL_HASH,
            connection_id="oi-rest-producer",
            generation=1,
            symbol="BTCUSDT",
            clock=clock,
            observation=backdated,
            source_logical_key="openInterest:BTCUSDT",
        )

    assert offerer.records == []

    with monkeypatch.context() as scoped:
        scoped.setattr(
            PublicOiRestTerminalObservationV2,
            "__call__",
            lambda self, completion: b"not-canonical-json\n",
        )
        with pytest.raises(ValueError):
            await ingress.offer_https_attempt(
                plan=rest,
                session_id="session-rest-validation-failure",
                protocol_hash=PROTOCOL_HASH,
                connection_id="oi-rest-producer",
                generation=1,
                symbol="BTCUSDT",
                clock=clock,
                observation=_rest_observation(rest),
                source_logical_key="openInterest:BTCUSDT",
            )

    assert offerer.records == []

    receipt = await ingress.offer_https_attempt(
        plan=rest,
        session_id="session-rest-validation-failure",
        protocol_hash=PROTOCOL_HASH,
        connection_id="oi-rest-producer",
        generation=1,
        symbol="BTCUSDT",
        clock=clock,
        observation=_rest_observation(rest),
        source_logical_key="openInterest:BTCUSDT",
    )

    assert receipt.record.ingest_seq == 41
    assert [observed.ingest_seq for observed in offerer.records] == [41]
    assert events == ["receipt", "receipt", "receipt", "offer"]


@pytest.mark.asyncio
async def test_https_ingress_rejects_invalid_scope_and_nonexact_observations() -> None:
    market, _public = _websocket_plans()
    rest = _rest_plan()
    offerer = RecordingOfferer()
    ingress = SharedWebSocketIngressV2(
        offerer,
        recovered_wal_tail_ingest_seq=0,
    )
    clock = ScriptReceiptClock()

    class FakeObservation:
        def __call__(self, completion: ReceiptTimestamp) -> bytes:
            del completion
            return b"forbidden"

    common = {
        "session_id": "session-rest-validation",
        "protocol_hash": PROTOCOL_HASH,
        "connection_id": "oi-rest-producer",
        "generation": 1,
        "symbol": "BTCUSDT",
        "clock": clock,
        "observation": _rest_observation(rest),
        "source_logical_key": "openInterest:BTCUSDT",
    }
    with pytest.raises(TypeError, match="exact promoting OI REST plan"):
        await ingress.offer_https_attempt(plan=market, **common)  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="plan census"):
        await ingress.offer_https_attempt(plan=rest, **{**common, "symbol": "ETHUSDT"})
    with pytest.raises(ValueError, match="connection_id"):
        await ingress.offer_https_attempt(plan=rest, **{**common, "connection_id": ""})
    with pytest.raises(ValueError, match="generation"):
        await ingress.offer_https_attempt(plan=rest, **{**common, "generation": 0})
    with pytest.raises(ValueError, match="source_logical_key"):
        await ingress.offer_https_attempt(plan=rest, **{**common, "source_logical_key": ""})
    with pytest.raises(ValueError, match="stable openInterest"):
        await ingress.offer_https_attempt(
            plan=rest,
            **{**common, "source_logical_key": "openInterest:BTCUSDT:cycle-1"},
        )
    with pytest.raises(TypeError, match="exact public OI terminal observation"):
        await ingress.offer_https_attempt(
            plan=rest,
            **{**common, "observation": lambda completion: b"forbidden"},  # type: ignore[arg-type]
        )
    with pytest.raises(TypeError, match="exact public OI terminal observation"):
        await ingress.offer_https_attempt(
            plan=rest,
            **{**common, "observation": FakeObservation()},  # type: ignore[arg-type]
        )
    with pytest.raises(TypeError, match="ReceiptClock"):
        await ingress.offer_https_attempt(
            plan=rest,
            **{**common, "clock": None},  # type: ignore[arg-type]
        )
    with pytest.raises(TypeError, match=r"exact asyncio\.Event"):
        await ingress.offer_https_attempt(
            plan=rest,
            **{**common, "cancellation_requested": object()},  # type: ignore[arg-type]
        )

    two_symbol_plan = _rest_plan(("ETHUSDT", "BTCUSDT"))
    with pytest.raises(ValueError, match="observation symbol differs"):
        await ingress.offer_https_attempt(
            plan=two_symbol_plan,
            **{
                **common,
                "observation": _rest_observation(
                    two_symbol_plan,
                    symbol="ETHUSDT",
                ),
            },
        )

    assert clock.calls == 0
    assert offerer.records == []


@pytest.mark.asyncio
async def test_recovery_completion_failure_preserves_original_exception() -> None:
    market, _public = _websocket_plans()
    offerer = RecordingOfferer()
    ingress = SharedWebSocketIngressV2(
        offerer,
        recovered_wal_tail_ingest_seq=0,
    )
    failure = RuntimeError("synthetic finality failure")

    async def fail_after_offer(record: RawRecordV2) -> None:
        assert offerer.records == [record]
        raise failure

    with pytest.raises(RuntimeError) as captured:
        await ingress.offer_recovery_successor(
            plan=market,
            session_id="session-recovery-failure",
            protocol_hash=PROTOCOL_HASH,
            connection_id="market-g000001",
            generation=1,
            frame_seq=1,
            receipt=ReceiptTimestamp(
                received_at_ms=101,
                received_monotonic_ns=1_001,
            ),
            raw_payload=b"successor",
            complete=fail_after_offer,
        )

    assert captured.value is failure
    assert [record.ingest_seq for record in offerer.records] == [1]
    assert ingress.pending_reservation_count == 0
    with pytest.raises(
        SharedIngressOrderingErrorV2,
        match="shared ingress is fail-closed after an ordering failure",
    ):
        await ingress.offer_frame(
            plan=market,
            session_id="session-recovery-failure",
            protocol_hash=PROTOCOL_HASH,
            connection_id="market-g000001",
            generation=1,
            frame_seq=2,
            receipt=ReceiptTimestamp(102, 1_002),
            raw_payload=b"must-not-cross-finality-failure",
        )


@pytest.mark.asyncio
async def test_pipeline_offer_failure_propagates_without_requesting_another_frame() -> None:
    market, _public = _websocket_plans()
    ingress = SharedWebSocketIngressV2(
        RejectingOfferer(),
        recovered_wal_tail_ingest_seq=0,
    )
    factory = PublicWebSocketFrameAdapterFactoryV2(
        market,
        session_id="session-rejection",
        protocol_hash=PROTOCOL_HASH,
        clock=ScriptReceiptClock(),
        ingress=ingress,
        recovery_lifecycle=PassThroughRecoveryLifecycle(),
    )
    adapter = factory(connection_id="market-g000001", generation=1)
    requested: list[str] = []

    async def frames() -> AsyncIterator[str | bytes]:
        requested.append("first")
        yield b"first"
        requested.append("second")
        yield b"second"

    with pytest.raises(OfferRejected, match="rejected the frame"):
        await adapter.consume(frames())

    assert requested == ["first"]
    assert adapter.frame_seq == 0
    assert ingress.pending_reservation_count == 0
    with pytest.raises(
        SharedIngressOrderingErrorV2,
        match="shared ingress is fail-closed after an ordering failure",
    ):
        await ingress.offer_frame(
            plan=market,
            session_id="session-rejection",
            protocol_hash=PROTOCOL_HASH,
            connection_id="market-g000001",
            generation=1,
            frame_seq=2,
            receipt=ReceiptTimestamp(1_700_000_000_002, 10_002),
            raw_payload=b"must-not-cross-rejected-sequence",
        )


@pytest.mark.parametrize("recovered_tail", [-1, True, 1.5])
def test_shared_ingress_rejects_non_exact_recovered_wal_tail(recovered_tail: object) -> None:
    with pytest.raises(ValueError, match="WAL tail"):
        SharedWebSocketIngressV2(
            RecordingOfferer(),
            recovered_wal_tail_ingest_seq=recovered_tail,  # type: ignore[arg-type]
        )


def test_factory_rejects_invalid_lineage_and_generation_before_capture() -> None:
    market, _public = _websocket_plans()
    ingress = SharedWebSocketIngressV2(
        RecordingOfferer(),
        recovered_wal_tail_ingest_seq=0,
    )
    clock = ScriptReceiptClock()

    with pytest.raises(ValueError, match="session_id"):
        PublicWebSocketFrameAdapterFactoryV2(
            market,
            session_id=" session",
            protocol_hash=PROTOCOL_HASH,
            clock=clock,
            ingress=ingress,
            recovery_lifecycle=PassThroughRecoveryLifecycle(),
        )
    with pytest.raises(ValueError, match="protocol_hash"):
        PublicWebSocketFrameAdapterFactoryV2(
            market,
            session_id="session",
            protocol_hash="not-a-hash",
            clock=clock,
            ingress=ingress,
            recovery_lifecycle=PassThroughRecoveryLifecycle(),
        )
    with pytest.raises(TypeError, match="recovery lifecycle"):
        PublicWebSocketFrameAdapterFactoryV2(
            market,
            session_id="session",
            protocol_hash=PROTOCOL_HASH,
            clock=clock,
            ingress=ingress,
            recovery_lifecycle=None,  # type: ignore[arg-type]
        )

    factory = PublicWebSocketFrameAdapterFactoryV2(
        market,
        session_id="session",
        protocol_hash=PROTOCOL_HASH,
        clock=clock,
        ingress=ingress,
        recovery_lifecycle=PassThroughRecoveryLifecycle(),
    )
    with pytest.raises(ValueError, match="generation"):
        factory(connection_id="market-g000000", generation=0)
    with pytest.raises(ValueError, match="connection_id"):
        factory(connection_id="", generation=1)
