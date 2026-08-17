from __future__ import annotations

import asyncio
import hashlib
from collections.abc import AsyncIterator, Sequence
from contextlib import AbstractAsyncContextManager
from dataclasses import dataclass, field, replace
from typing import cast

import pytest

from signalbot.capture.models import ConnectionState
from signalbot.capture.receipts import ReceiptTimestamp
from signalbot.capture.ws_owner import PublicWebSocketCaptureOwner, WebSocketOwnerSettings
from signalbot.r4b_v2.capture.integrity_ledger import (
    CaptureIntegrityEventV2,
    CaptureIntegrityLedgerV2,
    SourceGapCauseV2,
)
from signalbot.r4b_v2.capture.models import RawRecordV2
from signalbot.r4b_v2.capture.pipeline import CaptureFinalityFenceReceiptV2
from signalbot.r4b_v2.capture.plans import (
    ProvisionalPromotingCapturePlanV2,
    ProvisionalPromotingPlanV2,
    ProvisionalPromotingPlanV8,
    build_provisional_promoting_capture_plans_v2,
    build_provisional_promoting_capture_plans_v8,
    provisional_promoting_plan_sha256_v8,
)
from signalbot.r4b_v2.capture.websocket import (
    PublicWebSocketFrameAdapterFactoryV2,
    SharedWebSocketIngressV2,
    WebSocketFrameRetentionCancelledV2,
)
from signalbot.r4b_v2.capture.websocket_finality import (
    WebSocketRouteStopReceiptV2,
    WebSocketRouteStopReceiptV8,
    validate_websocket_route_stop_receipt_v2,
    validate_websocket_route_stop_receipt_v8,
)
from signalbot.r4b_v2.capture.websocket_lifecycle import (
    WebSocketLifecycleFatalCoordinatorV2,
    WebSocketLifecycleFatalCoordinatorV8,
)

PROTOCOL_HASH = hashlib.sha256(b"r4b-v2-websocket-lifecycle-test").hexdigest()


@dataclass(slots=True)
class FakeFatalState:
    stop_event: asyncio.Event = field(default_factory=asyncio.Event)
    cause: BaseException | None = None

    @property
    def failed(self) -> bool:
        return self.cause is not None

    def raise_if_failed(self) -> None:
        if self.cause is not None:
            raise self.cause


@dataclass(slots=True)
class FakeHandoff:
    fatal_state: FakeFatalState = field(default_factory=FakeFatalState)
    accepting: bool = True
    failures: list[tuple[BaseException, int | None]] = field(default_factory=list)

    def fail_consumer(
        self,
        cause: BaseException,
        *,
        failing_ingest_seq: int | None,
    ) -> None:
        self.accepting = False
        if self.fatal_state.failed:
            return
        self.fatal_state.cause = cause
        self.failures.append((cause, failing_ingest_seq))
        self.fatal_state.stop_event.set()


@dataclass(frozen=True, slots=True)
class FakeFinalityReceipt:
    requested_ingest_seq: int
    fence_ingest_seq: int
    sha256: str = "a" * 64
    prefix_proof_sha256: str = "b" * 64


class FakePipeline:
    def __init__(self, events: list[str]) -> None:
        self.events = events
        self.handoff = FakeHandoff()
        self.records: list[RawRecordV2] = []
        self.finality_started = asyncio.Event()
        self.release_first_finality = asyncio.Event()
        self.block_first_finality = False
        self.blocked_finality_seq: int | None = None
        self.finality_error: BaseException | None = None

    def offer(self, record: RawRecordV2) -> object:
        self.records.append(record)
        self.events.append(f"offer:{record.route_id}:{record.ingest_seq}")
        return object()

    async def finalize_through(
        self,
        requested_ingest_seq: int,
        *,
        timeout_seconds: float,
    ) -> CaptureFinalityFenceReceiptV2:
        assert timeout_seconds == 1.0
        self.events.append(f"finalize:{requested_ingest_seq}")
        self.finality_started.set()
        if (
            self.block_first_finality and requested_ingest_seq == 1
        ) or requested_ingest_seq == self.blocked_finality_seq:
            await self.release_first_finality.wait()
        if self.finality_error is not None:
            raise self.finality_error
        return cast(
            CaptureFinalityFenceReceiptV2,
            FakeFinalityReceipt(requested_ingest_seq, requested_ingest_seq),
        )


class FakeLedger:
    def __init__(self, events: list[str]) -> None:
        self.events = events
        self.open_calls: list[dict[str, object]] = []
        self.open_v8_plan_bundles: list[tuple[ProvisionalPromotingPlanV8, ...]] = []
        self.bounded_calls: list[tuple[CaptureIntegrityEventV2, int, str]] = []
        self.open_error: BaseException | None = None
        self._sequence = 0

    def append_source_gap_open(
        self,
        promoting_plans: Sequence[ProvisionalPromotingPlanV2],
        selected_plan: ProvisionalPromotingCapturePlanV2,
        **kwargs: object,
    ) -> CaptureIntegrityEventV2:
        assert selected_plan in promoting_plans
        self.open_calls.append(dict(kwargs))
        self.events.append(f"open:{selected_plan.route_id}")
        if self.open_error is not None:
            raise self.open_error
        payload = dict(kwargs)
        payload["route_id"] = selected_plan.route_id
        return self._event("SOURCE_GAP", payload)

    def append_source_gap_open_v8(
        self,
        promoting_plans: tuple[ProvisionalPromotingPlanV8, ...],
        selected_plan: ProvisionalPromotingCapturePlanV2,
        **kwargs: object,
    ) -> CaptureIntegrityEventV2:
        assert any(candidate is selected_plan for candidate in promoting_plans)
        self.open_v8_plan_bundles.append(promoting_plans)
        self.open_calls.append(dict(kwargs))
        self.events.append(f"open-v8:{selected_plan.route_id}")
        if self.open_error is not None:
            raise self.open_error
        payload = dict(kwargs)
        payload["route_id"] = selected_plan.route_id
        return self._event("SOURCE_GAP", payload)

    def append_source_gap_bounded(
        self,
        open_event: CaptureIntegrityEventV2,
        *,
        right_ingest_seq: int,
        evidence_sha256: str,
    ) -> CaptureIntegrityEventV2:
        self.bounded_calls.append(
            (open_event, right_ingest_seq, evidence_sha256)
        )
        route = str(open_event.payload["route_id"])
        self.events.append(f"bounded:{route}:{right_ingest_seq}")
        return self._event("SOURCE_GAP", {"route_id": route, "phase": "BOUNDED"})

    def assert_source_gap_bounded_current_v2(
        self,
        bounded_event: CaptureIntegrityEventV2,
    ) -> None:
        route = str(bounded_event.payload["route_id"])
        self.events.append(f"assert:{route}")

    def _event(
        self,
        event_type: str,
        payload: dict[str, object],
    ) -> CaptureIntegrityEventV2:
        self._sequence += 1
        return CaptureIntegrityEventV2(
            event_sequence=self._sequence,
            previous_event_sha256=None,
            event_id=f"event-{self._sequence}",
            event_type=event_type,
            authority_sha256="1" * 64,
            ledger_root_binding_sha256="2" * 64,
            block_root_binding_sha256="3" * 64,
            block_root_path_sha256="4" * 64,
            recorded_wall_ms=10_000 + self._sequence,
            recorded_monotonic_ns=20_000 + self._sequence,
            payload=payload,
        )


class ScriptClock:
    def __init__(self, *, wall_ms: int = 1_000, monotonic_ns: int = 2_000) -> None:
        self.wall_ms = wall_ms
        self.monotonic_ns = monotonic_ns

    def capture(self) -> ReceiptTimestamp:
        self.wall_ms += 1
        self.monotonic_ns += 1
        return ReceiptTimestamp(self.wall_ms, self.monotonic_ns)


class StopAfterOneFrame:
    def __init__(self, stop_event: asyncio.Event) -> None:
        self.stop_event = stop_event

    async def __aiter__(self) -> AsyncIterator[bytes]:
        yield b'{"stream":"btcusdt@aggTrade","data":{}}'
        self.stop_event.set()
        await asyncio.Event().wait()


class OwnerConnection(AbstractAsyncContextManager[StopAfterOneFrame]):
    def __init__(self, frames: StopAfterOneFrame) -> None:
        self.frames = frames

    async def __aenter__(self) -> StopAfterOneFrame:
        return self.frames

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: object,
    ) -> None:
        return None


class OrderingConnector:
    def __init__(self, events: list[str], stop_event: asyncio.Event) -> None:
        self.events = events
        self.stop_event = stop_event

    def __call__(self, url: str) -> OwnerConnection:
        assert url.startswith("wss://fstream.binance.com/market/")
        self.events.append("connector")
        return OwnerConnection(StopAfterOneFrame(self.stop_event))


def _plans() -> tuple[
    tuple[ProvisionalPromotingPlanV2, ...],
    ProvisionalPromotingCapturePlanV2,
    ProvisionalPromotingCapturePlanV2,
]:
    plans = build_provisional_promoting_capture_plans_v2(("BTCUSDT",))
    market = next(
        plan
        for plan in plans
        if isinstance(plan, ProvisionalPromotingCapturePlanV2)
        and plan.route_id == "usdm_market"
    )
    public = next(
        plan
        for plan in plans
        if isinstance(plan, ProvisionalPromotingCapturePlanV2)
        and plan.route_id == "usdm_public"
    )
    return plans, market, public


def _coordinator(
    plans: tuple[ProvisionalPromotingPlanV2, ...],
    plan: ProvisionalPromotingCapturePlanV2,
    *,
    clock: ScriptClock,
    pipeline: FakePipeline,
    ledger: FakeLedger,
) -> WebSocketLifecycleFatalCoordinatorV2:
    return WebSocketLifecycleFatalCoordinatorV2(
        plans,
        plan,
        session_id="session-live-v2",
        process_boot_id="boot-live-v2",
        session_started_at=ReceiptTimestamp(900, 1_900),
        source_component=f"v2-owner-{plan.route_id}",
        clock=clock,
        pipeline=pipeline,
        integrity_ledger=cast(CaptureIntegrityLedgerV2, ledger),
        finality_timeout_seconds=1.0,
    )


def _coordinator_v8(
    plans: tuple[ProvisionalPromotingPlanV8, ...],
    plan: ProvisionalPromotingCapturePlanV2,
    *,
    clock: ScriptClock,
    pipeline: FakePipeline,
    ledger: FakeLedger,
) -> WebSocketLifecycleFatalCoordinatorV8:
    return WebSocketLifecycleFatalCoordinatorV8(
        plans,
        plan,
        session_id="session-live-v8",
        process_boot_id="boot-live-v8",
        session_started_at=ReceiptTimestamp(900, 1_900),
        source_component=f"v8-owner-{plan.route_id}",
        clock=clock,
        pipeline=pipeline,
        integrity_ledger=cast(CaptureIntegrityLedgerV2, ledger),
        finality_timeout_seconds=1.0,
    )


def _settings() -> WebSocketOwnerSettings:
    return WebSocketOwnerSettings(
        maximum_connection_age_seconds=1.0,
        connect_timeout_seconds=1.0,
        close_timeout_seconds=1.0,
        heartbeat_interval_seconds=1.0,
        pong_timeout_seconds=1.0,
        internal_queue_frames=8,
        maximum_frame_bytes=1_024,
        maximum_reconnect_attempts=1,
        reconnect_delays_seconds=(0.0,),
        healthy_reset_seconds=0.5,
    )


@pytest.mark.asyncio
async def test_raw_v2_owner_is_blocked_before_open_or_connector() -> None:
    plans, market, _public = _plans()
    events: list[str] = []
    pipeline = FakePipeline(events)
    ledger = FakeLedger(events)
    clock = ScriptClock()
    coordinator = _coordinator(
        plans,
        market,
        clock=clock,
        pipeline=pipeline,
        ledger=ledger,
    )
    ingress = SharedWebSocketIngressV2(
        pipeline,
        recovered_wal_tail_ingest_seq=0,
    )
    factory = PublicWebSocketFrameAdapterFactoryV2(
        market,
        session_id="session-live-v2",
        protocol_hash=PROTOCOL_HASH,
        clock=clock,
        ingress=ingress,
        recovery_lifecycle=coordinator,
    )
    owner = PublicWebSocketCaptureOwner(
        factory.owner_plan,
        plan_sha256=PROTOCOL_HASH,
        process_boot_id="boot-live-v2",
        settings=_settings(),
        connector=OrderingConnector(events, coordinator.stop_event),
        frame_adapter_factory=factory,
        lifecycle_coordinator=coordinator,
    )

    with pytest.raises(RuntimeError, match="requires a bound preconnect"):
        await owner.run(coordinator.stop_event)

    assert events == []
    assert ledger.open_calls == []
    assert coordinator.failed is True


@pytest.mark.asyncio
async def test_open_append_failure_trips_original_fatal_before_connector() -> None:
    plans, market, _public = _plans()
    events: list[str] = []
    pipeline = FakePipeline(events)
    ledger = FakeLedger(events)
    original = OSError("synthetic SOURCE_GAP OPEN fsync failure")
    ledger.open_error = original
    clock = ScriptClock()
    coordinator = _coordinator(
        plans,
        market,
        clock=clock,
        pipeline=pipeline,
        ledger=ledger,
    )
    with pytest.raises(OSError) as captured:
        coordinator.record_transition(
            f"{market.name}-g000001",
            generation=1,
            last_frame_seq=0,
            state=ConnectionState.CONNECTING,
            reason="connect_attempt",
        )

    assert captured.value is original
    assert pipeline.handoff.fatal_state.cause is original
    assert events == ["open:usdm_market"]


@pytest.mark.asyncio
async def test_recovery_gate_excludes_other_route_n_plus_one_until_bounded() -> None:
    plans, market, public = _plans()
    events: list[str] = []
    pipeline = FakePipeline(events)
    pipeline.block_first_finality = True
    ledger = FakeLedger(events)
    clock = ScriptClock()
    market_lifecycle = _coordinator(
        plans,
        market,
        clock=clock,
        pipeline=pipeline,
        ledger=ledger,
    )
    public_lifecycle = _coordinator(
        plans,
        public,
        clock=clock,
        pipeline=pipeline,
        ledger=ledger,
    )
    market_lifecycle.record_transition(
        "market-g000001",
        generation=1,
        last_frame_seq=0,
        state=ConnectionState.CONNECTING,
        reason="connect_attempt",
    )
    public_lifecycle.record_transition(
        "public-g000001",
        generation=1,
        last_frame_seq=0,
        state=ConnectionState.CONNECTING,
        reason="connect_attempt",
    )
    ingress = SharedWebSocketIngressV2(
        pipeline,
        recovered_wal_tail_ingest_seq=0,
    )
    market_adapter = PublicWebSocketFrameAdapterFactoryV2(
        market,
        session_id="session-live-v2",
        protocol_hash=PROTOCOL_HASH,
        clock=clock,
        ingress=ingress,
        recovery_lifecycle=market_lifecycle,
    )(connection_id="market-g000001", generation=1)
    public_adapter = PublicWebSocketFrameAdapterFactoryV2(
        public,
        session_id="session-live-v2",
        protocol_hash=PROTOCOL_HASH,
        clock=clock,
        ingress=ingress,
        recovery_lifecycle=public_lifecycle,
    )(connection_id="public-g000001", generation=1)

    first = asyncio.create_task(market_adapter.consume(_one_frame(b"market")))
    await pipeline.finality_started.wait()
    second = asyncio.create_task(public_adapter.consume(_one_frame(b"public")))
    await asyncio.sleep(0)

    assert [record.ingest_seq for record in pipeline.records] == [1]
    assert not any(event.startswith("bounded:usdm_market") for event in events)

    pipeline.release_first_finality.set()
    await asyncio.gather(first, second)

    assert [record.ingest_seq for record in pipeline.records] == [1, 2]
    assert events.index("bounded:usdm_market:1") < events.index(
        "offer:usdm_public:2"
    )


@pytest.mark.asyncio
async def test_cancelled_frame_waiting_behind_other_recovery_is_fatal_not_a_tail() -> None:
    plans, market, public = _plans()
    events: list[str] = []
    pipeline = FakePipeline(events)
    ledger = FakeLedger(events)
    clock = ScriptClock()
    market_lifecycle = _coordinator(
        plans,
        market,
        clock=clock,
        pipeline=pipeline,
        ledger=ledger,
    )
    public_lifecycle = _coordinator(
        plans,
        public,
        clock=clock,
        pipeline=pipeline,
        ledger=ledger,
    )
    market_lifecycle.record_transition(
        "market-g000001",
        generation=1,
        last_frame_seq=0,
        state=ConnectionState.CONNECTING,
        reason="connect_attempt",
    )
    public_lifecycle.record_transition(
        "public-g000001",
        generation=1,
        last_frame_seq=0,
        state=ConnectionState.CONNECTING,
        reason="connect_attempt",
    )
    ingress = SharedWebSocketIngressV2(
        pipeline,
        recovered_wal_tail_ingest_seq=0,
    )
    market_adapter = PublicWebSocketFrameAdapterFactoryV2(
        market,
        session_id="session-live-v2",
        protocol_hash=PROTOCOL_HASH,
        clock=clock,
        ingress=ingress,
        recovery_lifecycle=market_lifecycle,
    )(connection_id="market-g000001", generation=1)
    public_adapter = PublicWebSocketFrameAdapterFactoryV2(
        public,
        session_id="session-live-v2",
        protocol_hash=PROTOCOL_HASH,
        clock=clock,
        ingress=ingress,
        recovery_lifecycle=public_lifecycle,
    )(connection_id="public-g000001", generation=1)

    await public_adapter.consume(_one_frame(b"public-frame-1"))
    assert public_adapter.frame_seq == 1
    pipeline.blocked_finality_seq = 2
    pipeline.finality_started = asyncio.Event()
    market_task = asyncio.create_task(
        market_adapter.consume(_one_frame(b"market-recovery"))
    )
    await pipeline.finality_started.wait()
    public_frame_2 = asyncio.create_task(
        public_adapter.consume(_one_frame(b"public-frame-2"))
    )
    await asyncio.sleep(0)

    public_frame_2.cancel()
    with pytest.raises(asyncio.CancelledError):
        await public_frame_2

    assert public_adapter.frame_seq == 1
    assert [
        (record.route_id, record.frame_seq, record.ingest_seq)
        for record in pipeline.records
    ] == [
        ("usdm_public", 1, 1),
        ("usdm_market", 1, 2),
    ]
    failure = pipeline.handoff.fatal_state.cause
    assert isinstance(failure, WebSocketFrameRetentionCancelledV2)
    assert failure.route_id == "usdm_public"
    assert failure.candidate_frame_seq == 2
    assert failure.raw_payload_sha256 == hashlib.sha256(
        b"public-frame-2"
    ).hexdigest()
    assert pipeline.handoff.fatal_state.stop_event.is_set()
    with pytest.raises(WebSocketFrameRetentionCancelledV2) as transition_failure:
        public_lifecycle.record_transition(
            "public-g000001",
            generation=1,
            last_frame_seq=1,
            state=ConnectionState.DISCONNECTED,
            reason="owner_stop",
        )
    assert transition_failure.value is failure

    market_task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await market_task


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("boundary_state", "boundary_reason", "expected_cause"),
    [
        (
            ConnectionState.DISCONNECTED,
            "remote_stream_ended",
            SourceGapCauseV2.WEBSOCKET_DISCONNECT,
        ),
        (
            ConnectionState.RECYCLED,
            "proactive_lifetime_rotation",
            SourceGapCauseV2.PROACTIVE_RECYCLE,
        ),
    ],
)
async def test_repeated_zero_frame_failures_keep_one_open_then_next_boundary_opens(
    boundary_state: ConnectionState,
    boundary_reason: str,
    expected_cause: SourceGapCauseV2,
) -> None:
    plans, market, _public = _plans()
    events: list[str] = []
    pipeline = FakePipeline(events)
    ledger = FakeLedger(events)
    clock = ScriptClock()
    coordinator = _coordinator(
        plans,
        market,
        clock=clock,
        pipeline=pipeline,
        ledger=ledger,
    )
    for generation in (1, 2, 3):
        connection_id = f"market-g{generation:06d}"
        coordinator.record_transition(
            connection_id,
            generation=generation,
            last_frame_seq=0,
            state=ConnectionState.CONNECTING,
            reason="connect_attempt",
        )
        if generation < 3:
            coordinator.record_transition(
                connection_id,
                generation=generation,
                last_frame_seq=0,
                state=ConnectionState.DISCONNECTED,
                reason="connection_failure",
            )

    assert len(ledger.open_calls) == 1

    ingress = SharedWebSocketIngressV2(
        pipeline,
        recovered_wal_tail_ingest_seq=0,
    )
    adapter = PublicWebSocketFrameAdapterFactoryV2(
        market,
        session_id="session-live-v2",
        protocol_hash=PROTOCOL_HASH,
        clock=clock,
        ingress=ingress,
        recovery_lifecycle=coordinator,
    )(connection_id="market-g000003", generation=3)
    await adapter.consume(_one_frame(b"successor"))
    coordinator.record_transition(
        "market-g000003",
        generation=3,
        last_frame_seq=1,
        state=boundary_state,
        reason=boundary_reason,
    )

    assert len(ledger.open_calls) == 2
    reconnect_open = ledger.open_calls[-1]
    assert reconnect_open["cause"] is expected_cause
    assert reconnect_open["left_connection_id"] == "market-g000003"
    assert reconnect_open["left_frame_seq"] == 1
    assert reconnect_open["left_ingest_seq"] == 1


@pytest.mark.asyncio
async def test_followup_offer_advances_the_next_gap_left_cursor() -> None:
    plans, market, _public = _plans()
    events: list[str] = []
    pipeline = FakePipeline(events)
    ledger = FakeLedger(events)
    clock = ScriptClock()
    coordinator = _coordinator(
        plans,
        market,
        clock=clock,
        pipeline=pipeline,
        ledger=ledger,
    )
    coordinator.record_transition(
        "market-g000001",
        generation=1,
        last_frame_seq=0,
        state=ConnectionState.CONNECTING,
        reason="connect_attempt",
    )
    ingress = SharedWebSocketIngressV2(
        pipeline,
        recovered_wal_tail_ingest_seq=0,
    )
    adapter = PublicWebSocketFrameAdapterFactoryV2(
        market,
        session_id="session-live-v2",
        protocol_hash=PROTOCOL_HASH,
        clock=clock,
        ingress=ingress,
        recovery_lifecycle=coordinator,
    )(connection_id="market-g000001", generation=1)

    await adapter.consume(_frames(b"successor", b"followup"))
    coordinator.record_transition(
        "market-g000001",
        generation=1,
        last_frame_seq=2,
        state=ConnectionState.DISCONNECTED,
        reason="remote_stream_ended",
    )

    reconnect_open = ledger.open_calls[-1]
    assert reconnect_open["left_frame_seq"] == 2
    assert reconnect_open["left_ingest_seq"] == 2


@pytest.mark.asyncio
async def test_exact_owner_stop_issues_one_factory_sealed_terminal_cursor() -> None:
    plans, market, _public = _plans()
    events: list[str] = []
    pipeline = FakePipeline(events)
    ledger = FakeLedger(events)
    clock = ScriptClock()
    coordinator = _coordinator(
        plans,
        market,
        clock=clock,
        pipeline=pipeline,
        ledger=ledger,
    )
    coordinator.record_transition(
        "market-g000001",
        generation=1,
        last_frame_seq=0,
        state=ConnectionState.CONNECTING,
        reason="connect_attempt",
    )
    ingress = SharedWebSocketIngressV2(
        pipeline,
        recovered_wal_tail_ingest_seq=0,
    )
    adapter = PublicWebSocketFrameAdapterFactoryV2(
        market,
        session_id="session-live-v2",
        protocol_hash=PROTOCOL_HASH,
        clock=clock,
        ingress=ingress,
        recovery_lifecycle=coordinator,
    )(connection_id="market-g000001", generation=1)
    await adapter.consume(_frames(b"successor", b"followup"))

    assert coordinator.normal_stop_receipt is None
    coordinator.record_transition(
        "market-g000001",
        generation=1,
        last_frame_seq=2,
        state=ConnectionState.DISCONNECTED,
        reason="owner_stop",
    )

    receipt = coordinator.normal_stop_receipt
    assert type(receipt) is WebSocketRouteStopReceiptV2
    validate_websocket_route_stop_receipt_v2(
        receipt,
        promoting_plans=plans,
        plan=market,
    )
    assert receipt.route_id == "usdm_market"
    assert receipt.connection_id == "market-g000001"
    assert receipt.generation == 1
    assert receipt.last_frame_seq == 2
    assert receipt.last_ingest_seq == 2
    assert receipt.pending_source_gap is False
    assert receipt.retained_frame_parser_health_claimed is False
    assert receipt.upstream_message_completeness_claimed is False
    assert receipt.m2_certified is False
    with pytest.raises(TypeError, match="factory-sealed"):
        replace(receipt)


@pytest.mark.asyncio
async def test_v8_source_gap_and_owner_stop_retain_full_four_plan_authority() -> None:
    plans = build_provisional_promoting_capture_plans_v8(("BTCUSDT",))
    market = next(
        candidate
        for candidate in plans
        if type(candidate) is ProvisionalPromotingCapturePlanV2
        and candidate.route_id == "usdm_market"
    )
    events: list[str] = []
    pipeline = FakePipeline(events)
    ledger = FakeLedger(events)
    clock = ScriptClock()
    coordinator = _coordinator_v8(
        plans,
        market,
        clock=clock,
        pipeline=pipeline,
        ledger=ledger,
    )
    coordinator.record_transition(
        "market-g000001",
        generation=1,
        last_frame_seq=0,
        state=ConnectionState.CONNECTING,
        reason="connect_attempt",
    )
    assert ledger.open_v8_plan_bundles == [plans]
    assert ledger.open_v8_plan_bundles[0] is plans

    ingress = SharedWebSocketIngressV2(
        pipeline,
        recovered_wal_tail_ingest_seq=0,
    )
    adapter = PublicWebSocketFrameAdapterFactoryV2(
        market,
        session_id="session-live-v8",
        protocol_hash=PROTOCOL_HASH,
        clock=clock,
        ingress=ingress,
        recovery_lifecycle=coordinator,
    )(connection_id="market-g000001", generation=1)
    await adapter.consume(_frames(b"successor", b"followup"))
    coordinator.record_transition(
        "market-g000001",
        generation=1,
        last_frame_seq=2,
        state=ConnectionState.DISCONNECTED,
        reason="owner_stop",
    )

    receipt = coordinator.normal_stop_receipt_v8
    assert type(receipt) is WebSocketRouteStopReceiptV8
    validate_websocket_route_stop_receipt_v8(
        receipt,
        promoting_plans=plans,
        plan=market,
    )
    assert receipt.plan_bundle_sha256 == provisional_promoting_plan_sha256_v8(plans)
    assert receipt.depth_bridge_complete_claimed is False
    assert receipt.m2_certified is False
    assert coordinator.normal_stop_receipt is None
    with pytest.raises(TypeError, match="factory-sealed"):
        replace(receipt)


@pytest.mark.asyncio
async def test_owner_cancelled_with_retained_cursor_never_issues_stop_receipt() -> None:
    plans, market, _public = _plans()
    events: list[str] = []
    pipeline = FakePipeline(events)
    ledger = FakeLedger(events)
    clock = ScriptClock()
    coordinator = _coordinator(
        plans,
        market,
        clock=clock,
        pipeline=pipeline,
        ledger=ledger,
    )
    coordinator.record_transition(
        "market-g000001",
        generation=1,
        last_frame_seq=0,
        state=ConnectionState.CONNECTING,
        reason="connect_attempt",
    )
    ingress = SharedWebSocketIngressV2(
        pipeline,
        recovered_wal_tail_ingest_seq=0,
    )
    adapter = PublicWebSocketFrameAdapterFactoryV2(
        market,
        session_id="session-live-v2",
        protocol_hash=PROTOCOL_HASH,
        clock=clock,
        ingress=ingress,
        recovery_lifecycle=coordinator,
    )(connection_id="market-g000001", generation=1)
    await adapter.consume(_one_frame(b"successor"))

    coordinator.record_transition(
        "market-g000001",
        generation=1,
        last_frame_seq=1,
        state=ConnectionState.DISCONNECTED,
        reason="owner_cancelled",
    )

    assert coordinator.pending_source_gap is False
    assert coordinator.normal_stop_receipt is None


@pytest.mark.asyncio
async def test_owner_stop_receipt_is_write_once() -> None:
    plans, market, _public = _plans()
    events: list[str] = []
    pipeline = FakePipeline(events)
    ledger = FakeLedger(events)
    clock = ScriptClock()
    coordinator = _coordinator(
        plans,
        market,
        clock=clock,
        pipeline=pipeline,
        ledger=ledger,
    )
    coordinator.record_transition(
        "market-g000001",
        generation=1,
        last_frame_seq=0,
        state=ConnectionState.CONNECTING,
        reason="connect_attempt",
    )
    ingress = SharedWebSocketIngressV2(
        pipeline,
        recovered_wal_tail_ingest_seq=0,
    )
    adapter = PublicWebSocketFrameAdapterFactoryV2(
        market,
        session_id="session-live-v2",
        protocol_hash=PROTOCOL_HASH,
        clock=clock,
        ingress=ingress,
        recovery_lifecycle=coordinator,
    )(connection_id="market-g000001", generation=1)
    await adapter.consume(_one_frame(b"successor"))
    coordinator.record_transition(
        "market-g000001",
        generation=1,
        last_frame_seq=1,
        state=ConnectionState.DISCONNECTED,
        reason="owner_stop",
    )
    first = coordinator.normal_stop_receipt

    with pytest.raises(RuntimeError, match="write-once"):
        coordinator.record_transition(
            "market-g000001",
            generation=1,
            last_frame_seq=1,
            state=ConnectionState.DISCONNECTED,
            reason="owner_stop",
        )

    assert coordinator.normal_stop_receipt is first
    assert coordinator.failed is True


@pytest.mark.asyncio
@pytest.mark.parametrize("reason", ["owner_stop", "owner_cancelled"])
async def test_operator_cancel_during_successor_finality_leaves_open_without_fatal(
    reason: str,
) -> None:
    plans, market, _public = _plans()
    events: list[str] = []
    pipeline = FakePipeline(events)
    pipeline.block_first_finality = True
    ledger = FakeLedger(events)
    clock = ScriptClock()
    coordinator = _coordinator(
        plans,
        market,
        clock=clock,
        pipeline=pipeline,
        ledger=ledger,
    )
    coordinator.record_transition(
        "market-g000001",
        generation=1,
        last_frame_seq=0,
        state=ConnectionState.CONNECTING,
        reason="connect_attempt",
    )
    ingress = SharedWebSocketIngressV2(
        pipeline,
        recovered_wal_tail_ingest_seq=0,
    )
    adapter = PublicWebSocketFrameAdapterFactoryV2(
        market,
        session_id="session-live-v2",
        protocol_hash=PROTOCOL_HASH,
        clock=clock,
        ingress=ingress,
        recovery_lifecycle=coordinator,
    )(connection_id="market-g000001", generation=1)
    task = asyncio.create_task(adapter.consume(_one_frame(b"successor")))
    await pipeline.finality_started.wait()

    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert adapter.frame_seq == 0
    coordinator.record_transition(
        "market-g000001",
        generation=1,
        last_frame_seq=0,
        state=ConnectionState.DISCONNECTED,
        reason=reason,
    )

    assert coordinator.pending_source_gap is True
    assert coordinator.normal_stop_receipt is None
    assert coordinator.failed is False
    assert pipeline.handoff.failures == []
    assert ledger.bounded_calls == []


def test_connecting_generation_must_advance_by_exactly_one() -> None:
    plans, market, _public = _plans()
    events: list[str] = []
    pipeline = FakePipeline(events)
    ledger = FakeLedger(events)
    coordinator = _coordinator(
        plans,
        market,
        clock=ScriptClock(),
        pipeline=pipeline,
        ledger=ledger,
    )

    with pytest.raises(RuntimeError, match="exactly one") as captured:
        coordinator.record_transition(
            "market-g000002",
            generation=2,
            last_frame_seq=0,
            state=ConnectionState.CONNECTING,
            reason="connect_attempt",
        )

    assert pipeline.handoff.fatal_state.cause is captured.value
    assert ledger.open_calls == []


@pytest.mark.asyncio
async def test_finality_failure_preserves_original_fatal_and_requests_no_next_frame() -> None:
    plans, market, _public = _plans()
    events: list[str] = []
    pipeline = FakePipeline(events)
    original = RuntimeError("synthetic finality failure")
    pipeline.finality_error = original
    ledger = FakeLedger(events)
    clock = ScriptClock()
    coordinator = _coordinator(
        plans,
        market,
        clock=clock,
        pipeline=pipeline,
        ledger=ledger,
    )
    coordinator.record_transition(
        "market-g000001",
        generation=1,
        last_frame_seq=0,
        state=ConnectionState.CONNECTING,
        reason="connect_attempt",
    )
    ingress = SharedWebSocketIngressV2(
        pipeline,
        recovered_wal_tail_ingest_seq=0,
    )
    adapter = PublicWebSocketFrameAdapterFactoryV2(
        market,
        session_id="session-live-v2",
        protocol_hash=PROTOCOL_HASH,
        clock=clock,
        ingress=ingress,
        recovery_lifecycle=coordinator,
    )(connection_id="market-g000001", generation=1)
    requested: list[str] = []

    async def frames() -> AsyncIterator[bytes]:
        requested.append("first")
        yield b"first"
        requested.append("second")
        yield b"second"

    with pytest.raises(RuntimeError) as captured:
        await adapter.consume(frames())

    assert captured.value is original
    assert pipeline.handoff.fatal_state.cause is original
    assert pipeline.handoff.failures == [(original, 1)]
    assert pipeline.handoff.fatal_state.stop_event.is_set()
    assert requested == ["first"]
    assert ledger.bounded_calls == []


async def _one_frame(payload: bytes) -> AsyncIterator[bytes]:
    yield payload


async def _frames(*payloads: bytes) -> AsyncIterator[bytes]:
    for payload in payloads:
        yield payload
