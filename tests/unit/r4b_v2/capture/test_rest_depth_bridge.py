from __future__ import annotations

import asyncio
import hashlib
import json
from collections.abc import AsyncIterable, AsyncIterator, Callable
from contextlib import AbstractAsyncContextManager
from dataclasses import asdict, dataclass, field, fields, replace
from typing import Any, cast

import httpx
import pytest

from signalbot.capture.models import ConnectionState
from signalbot.capture.receipts import ReceiptTimestamp
from signalbot.capture.ws_owner import (
    PublicWebSocketCaptureOwner,
    WebSocketOwnerSettings,
)
from signalbot.r4b_v2.capture.batching import (
    BatchPolicyV2,
    BoundedBatchHandoffV2,
    CaptureQueueAdmissionReceiptV2,
    QueuedRawRecordV2,
)
from signalbot.r4b_v2.capture.integrity_ledger import (
    CaptureIntegrityEventV2,
    CaptureIntegrityLedgerV2,
)
from signalbot.r4b_v2.capture.models import RawRecordV2
from signalbot.r4b_v2.capture.plans import (
    ProvisionalDepthRestQualificationPlanV8,
    ProvisionalPromotingCapturePlanV2,
    ProvisionalPromotingPlanV8,
    build_provisional_promoting_capture_plans_v8,
    provisional_promoting_plan_sha256_v8,
)
from signalbot.r4b_v2.capture.rest_depth_bridge import (
    PublicDepthRestBridgeCoordinatorErrorV8,
    PublicDepthRestBridgeCoordinatorV8,
)
from signalbot.r4b_v2.capture.rest_depth_bridge_evidence import (
    DEPTH_BRIDGE_MAXIMUM_BUFFERED_RANGES_PER_SYMBOL_V8,
    DepthBridgeAttemptTerminalV8,
    DepthBridgeCoordinatorCleanCloseReceiptV8,
    DepthBridgeCoordinatorClosureEntryV8,
    DepthBridgeCycleTerminalV8,
    DepthBridgeEvidenceErrorV8,
    DepthBridgeEvidencePayloadV8,
    DepthBridgeGenerationDrainedV8,
    DepthBridgePhaseV8,
    DepthBridgeWaitTerminalV8,
    depth_bridge_coordinator_closure_entry_sha256_v8,
    depth_bridge_coordinator_closure_entry_v8,
    depth_bridge_evidence_census_v8,
    validate_depth_bridge_coordinator_clean_close_receipt_v8,
    validate_depth_bridge_coordinator_closure_entry_v8,
    validate_depth_bridge_evidence_order_v8,
    validate_depth_bridge_evidence_payload_v8,
)
from signalbot.r4b_v2.capture.websocket import (
    PublicWebSocketCaptureAdapterV2,
    PublicWebSocketFrameAdapterFactoryV2,
    SharedWebSocketIngressV2,
)

_SESSION_ID = "session-depth-bridge"
_PROTOCOL_HASH = "d" * 64


def _batch_handoff() -> BoundedBatchHandoffV2:
    return BoundedBatchHandoffV2(
        BatchPolicyV2(
            max_records=2_048,
            max_encoded_bytes=32_000_000,
            max_linger_us=1_000,
            queue_max_events=4_096,
            queue_max_encoded_bytes=128_000_000,
            low_water_events=0,
            low_water_encoded_bytes=0,
            qualification_id="depth-bridge-test",
        )
    )


@dataclass(slots=True)
class _RecordingOfferer:
    records: list[RawRecordV2] = field(default_factory=list)
    handoff: BoundedBatchHandoffV2 = field(default_factory=_batch_handoff)

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


class _Clock:
    def __init__(self) -> None:
        self._wall_ms = 1_800_000_000_000
        self._monotonic_ns = 1_000_000

    def capture(self) -> ReceiptTimestamp:
        self._wall_ms += 1
        self._monotonic_ns += 1
        return ReceiptTimestamp(self._wall_ms, self._monotonic_ns)


@dataclass(slots=True)
class _Lifecycle:
    stop_event: asyncio.Event = field(default_factory=asyncio.Event)
    failed: bool = False
    accepting: bool = True
    causes: list[BaseException] = field(default_factory=list)
    transitions: list[tuple[str, int, ConnectionState]] = field(default_factory=list)

    def record_transition(
        self,
        connection_id: str,
        *,
        generation: int,
        last_frame_seq: int,
        state: ConnectionState,
        reason: str,
    ) -> None:
        del last_frame_seq, reason
        self.transitions.append((connection_id, generation, state))

    def trip_fatal(self, cause: BaseException) -> None:
        if not self.causes:
            self.causes.append(cause)
        self.failed = True
        self.accepting = False
        self.stop_event.set()

    def raise_if_failed(self) -> None:
        if self.causes:
            raise self.causes[0]

    async def complete_recovery_successor(self, record: RawRecordV2) -> None:
        del record

    def record_retained_frame(self, record: RawRecordV2) -> None:
        del record


class _FactoryWrapper:
    def __init__(self, delegate: PublicWebSocketFrameAdapterFactoryV2) -> None:
        self.delegate = delegate
        self.session_id = delegate.session_id
        self.protocol_hash = delegate.protocol_hash
        self.adapters: list[PublicWebSocketCaptureAdapterV2] = []

    def __call__(
        self,
        *,
        connection_id: str,
        generation: int,
    ) -> PublicWebSocketCaptureAdapterV2:
        adapter = self.delegate(
            connection_id=connection_id,
            generation=generation,
        )
        self.adapters.append(adapter)
        return adapter


class _Frames:
    def __init__(
        self,
        frames: AsyncIterable[str] | tuple[str, ...],
        stop_event: asyncio.Event,
    ) -> None:
        self._frames = frames
        self._stop_event = stop_event

    async def __aiter__(self) -> AsyncIterator[str]:
        if isinstance(self._frames, tuple):
            for frame in self._frames:
                yield frame
        else:
            async for frame in self._frames:
                yield frame
        await self._stop_event.wait()


class _Connection(AbstractAsyncContextManager[AsyncIterable[str | bytes]]):
    def __init__(self, frames: AsyncIterable[str | bytes]) -> None:
        self._frames = frames

    async def __aenter__(self) -> AsyncIterable[str | bytes]:
        return self._frames

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: object,
    ) -> None:
        del exc_type, exc_value, traceback


class _Connector:
    def __init__(self, frames: _Frames) -> None:
        self._frames = frames

    def __call__(self, url: str) -> _Connection:
        del url
        return _Connection(self._frames)


class _FiniteFrames:
    def __init__(self, frames: tuple[str, ...]) -> None:
        self._frames = frames

    async def __aiter__(self) -> AsyncIterator[str]:
        for frame in self._frames:
            yield frame


class _ReconnectConnector:
    def __init__(
        self,
        first_frames: tuple[str, ...],
        successor_frames: tuple[str, ...],
        stop_event: asyncio.Event,
    ) -> None:
        self._first_frames = first_frames
        self._successor_frames = successor_frames
        self._stop_event = stop_event
        self.connection_count = 0

    def __call__(self, url: str) -> _Connection:
        del url
        self.connection_count += 1
        if self.connection_count == 1:
            return _Connection(_FiniteFrames(self._first_frames))
        return _Connection(_Frames(self._successor_frames, self._stop_event))


class _SnapshotTransport(httpx.AsyncBaseTransport):
    def __init__(
        self,
        last_update_id: int | Callable[[str, int], int],
        *,
        gate: asyncio.Event | None = None,
    ) -> None:
        self._last_update_id = last_update_id
        self._gate = gate
        self.requests: list[httpx.Request] = []
        self.active = 0
        self.maximum_active = 0
        self.request_started = asyncio.Event()

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        self.active += 1
        self.maximum_active = max(self.maximum_active, self.active)
        self.request_started.set()
        try:
            if self._gate is not None:
                await self._gate.wait()
            symbol = request.url.params["symbol"]
            assert symbol is not None
            attempt = sum(candidate.url.params["symbol"] == symbol for candidate in self.requests)
            last_update_id = (
                self._last_update_id(symbol, attempt)
                if callable(self._last_update_id)
                else self._last_update_id
            )
            body = json.dumps(
                {
                    "lastUpdateId": last_update_id,
                    "E": 2_000,
                    "T": 1_999,
                    "bids": [["1", "1"]],
                    "asks": [["2", "1"]],
                },
                separators=(",", ":"),
            ).encode()
            return httpx.Response(200, content=body, request=request)
        finally:
            self.active -= 1


@dataclass(slots=True)
class _System:
    plans: tuple[ProvisionalPromotingPlanV8, ...]
    depth_plan: ProvisionalDepthRestQualificationPlanV8
    lifecycle: _Lifecycle
    coordinator: PublicDepthRestBridgeCoordinatorV8
    owner: PublicWebSocketCaptureOwner
    payloads: list[DepthBridgeEvidencePayloadV8]
    transport: _SnapshotTransport | None


def _settings() -> WebSocketOwnerSettings:
    return WebSocketOwnerSettings(
        maximum_connection_age_seconds=30.0,
        connect_timeout_seconds=2.0,
        close_timeout_seconds=2.0,
        heartbeat_interval_seconds=30.0,
        pong_timeout_seconds=2.0,
        internal_queue_frames=2_048,
        maximum_frame_bytes=16_384,
        maximum_reconnect_attempts=2,
        reconnect_delays_seconds=(0.0, 0.0),
        healthy_reset_seconds=1.0,
    )


def _recording_ledger(
    plans: tuple[ProvisionalPromotingPlanV8, ...],
    depth_plan: ProvisionalDepthRestQualificationPlanV8,
) -> tuple[CaptureIntegrityLedgerV2, list[DepthBridgeEvidencePayloadV8]]:
    ledger = object.__new__(CaptureIntegrityLedgerV2)
    payloads: list[DepthBridgeEvidencePayloadV8] = []
    events: list[CaptureIntegrityEventV2] = []

    def append(
        payload: DepthBridgeEvidencePayloadV8,
        actual_plans: tuple[ProvisionalPromotingPlanV8, ...],
        actual_depth_plan: ProvisionalDepthRestQualificationPlanV8,
    ) -> CaptureIntegrityEventV2:
        assert actual_plans is plans
        assert actual_depth_plan is depth_plan
        validate_depth_bridge_evidence_payload_v8(
            payload,
            promoting_plans=plans,
            depth_plan=depth_plan,
        )
        validate_depth_bridge_evidence_order_v8(payload, prior=tuple(payloads))
        payloads.append(payload)
        normalized_payload = json.loads(json.dumps(asdict(payload)))
        assert isinstance(normalized_payload, dict)
        event = CaptureIntegrityEventV2(
            event_sequence=len(events) + 1,
            previous_event_sha256=events[-1].sha256 if events else None,
            event_id=hashlib.sha256(
                f"depth-bridge-event-{len(events) + 1}".encode()
            ).hexdigest(),
            event_type="DEPTH_BRIDGE",
            authority_sha256="a" * 64,
            ledger_root_binding_sha256="b" * 64,
            block_root_binding_sha256="c" * 64,
            block_root_path_sha256="d" * 64,
            recorded_wall_ms=1_700_000_000_000 + len(events),
            recorded_monotonic_ns=100_000 + len(events),
            payload=normalized_payload,
        )
        events.append(event)
        return event

    ledger.append_depth_bridge_v8 = append  # pyright: ignore[reportAttributeAccessIssue]
    return ledger, payloads


def _depth_frame(
    symbol: str,
    *,
    first_update_id: int,
    final_update_id: int,
    previous_final_update_id: int,
) -> str:
    return json.dumps(
        {
            "stream": f"{symbol.lower()}@depth@100ms",
            "data": {
                "e": "depthUpdate",
                "s": symbol,
                "U": first_update_id,
                "u": final_update_id,
                "pu": previous_final_update_id,
                "ps": symbol,
                "st": 1,
            },
        },
        separators=(",", ":"),
    )


def _build_system(
    symbols: tuple[str, ...],
    frames: AsyncIterable[str] | tuple[str, ...],
    *,
    transport: _SnapshotTransport | None = None,
    transport_factory: Callable[[], httpx.AsyncBaseTransport | None] | None = None,
    with_callbacks: bool = True,
    connector: (
        Callable[
            [str],
            AbstractAsyncContextManager[AsyncIterable[str | bytes]],
        ]
        | None
    ) = None,
) -> _System:
    plans = build_provisional_promoting_capture_plans_v8(symbols)
    depth_plan = next(
        plan for plan in plans if type(plan) is ProvisionalDepthRestQualificationPlanV8
    )
    public_plan = next(
        plan
        for plan in plans
        if type(plan) is ProvisionalPromotingCapturePlanV2 and plan.route_id == "usdm_public"
    )
    offerer = _RecordingOfferer()
    ingress = SharedWebSocketIngressV2(
        offerer,
        recovered_wal_tail_ingest_seq=0,
    )
    clock = _Clock()
    lifecycle = _Lifecycle()
    ledger, payloads = _recording_ledger(plans, depth_plan)
    selected_factory = transport_factory if transport_factory is not None else (lambda: transport)
    coordinator = PublicDepthRestBridgeCoordinatorV8(
        plans,
        depth_plan,
        ingress=ingress,
        clock=clock,
        fatal_coordinator=lifecycle,
        ledger=ledger,
        transport_factory=selected_factory,
    )
    exact_factory = PublicWebSocketFrameAdapterFactoryV2(
        public_plan,
        session_id=_SESSION_ID,
        protocol_hash=_PROTOCOL_HASH,
        clock=clock,
        ingress=ingress,
        recovery_lifecycle=lifecycle,
    )
    wrapper = _FactoryWrapper(exact_factory)
    selected_connector = (
        connector
        if connector is not None
        else _Connector(_Frames(frames, lifecycle.stop_event))
    )
    owner = PublicWebSocketCaptureOwner(
        exact_factory.owner_plan,
        plan_sha256=provisional_promoting_plan_sha256_v8(plans),
        process_boot_id="boot-depth-bridge",
        settings=_settings(),
        connector=selected_connector,
        frame_adapter_factory=wrapper,
        lifecycle_coordinator=lifecycle,
        preconnecting_generation_hook=coordinator,
        retained_depth_range_callback=(
            coordinator.retained_depth_range_callback if with_callbacks else None
        ),
        retained_depth_resync_callback=(
            coordinator.retained_depth_resync_callback if with_callbacks else None
        ),
    )
    return _System(
        plans=plans,
        depth_plan=depth_plan,
        lifecycle=lifecycle,
        coordinator=coordinator,
        owner=owner,
        payloads=payloads,
        transport=transport,
    )


async def _wait_for(
    predicate: Callable[[], bool],
    *,
    within_seconds: float = 5.0,
) -> None:
    async def wait() -> None:
        while not predicate():
            turn = asyncio.Event()
            asyncio.get_running_loop().call_soon(turn.set)
            await turn.wait()

    await asyncio.wait_for(wait(), timeout=within_seconds)


def _phases(system: _System) -> list[str]:
    return [payload.phase for payload in system.payloads]


async def _stop_owner(system: _System, owner_task: asyncio.Task[None]) -> None:
    system.lifecycle.stop_event.set()
    await asyncio.wait_for(owner_task, timeout=3)


async def _normally_close_empty_generation(
    system: _System,
) -> DepthBridgeCoordinatorCleanCloseReceiptV8:
    system.coordinator.bind_websocket_owner(system.owner)
    owner_task = asyncio.create_task(system.owner.run(system.lifecycle.stop_event))
    await _wait_for(lambda: system.coordinator.generation_open)
    await _stop_owner(system, owner_task)
    receipt = await system.coordinator.aclose()
    assert type(receipt) is DepthBridgeCoordinatorCleanCloseReceiptV8
    return receipt


@pytest.mark.asyncio
async def test_accepted_cycle_is_durable_before_clean_zero_count_drain() -> None:
    frame = _depth_frame(
        "BTCUSDT",
        first_update_id=100,
        final_update_id=101,
        previous_final_update_id=99,
    )
    transport = _SnapshotTransport(100)
    system = _build_system(("BTCUSDT",), (frame,), transport=transport)
    system.coordinator.bind_websocket_owner(system.owner)
    owner_task = asyncio.create_task(system.owner.run(system.lifecycle.stop_event))

    await _wait_for(lambda: DepthBridgePhaseV8.CYCLE_TERMINAL.value in _phases(system))
    await _stop_owner(system, owner_task)
    await system.coordinator.aclose()

    assert _phases(system) == [
        DepthBridgePhaseV8.GENERATION_STARTED.value,
        DepthBridgePhaseV8.TRIGGER_REGISTERED.value,
        DepthBridgePhaseV8.ATTEMPT_STARTED.value,
        DepthBridgePhaseV8.ATTEMPT_TERMINAL.value,
        DepthBridgePhaseV8.CYCLE_TERMINAL.value,
        DepthBridgePhaseV8.GENERATION_DRAINED.value,
    ]
    terminal = cast(DepthBridgeCycleTerminalV8, system.payloads[4].material)
    assert (terminal.outcome, terminal.reason) == (
        "accepted",
        "snapshot_range_bridge",
    )
    drain = cast(DepthBridgeGenerationDrainedV8, system.payloads[-1].material)
    assert (
        drain.worker_count,
        drain.permit_in_use_count,
        drain.retained_registration_count,
        drain.pending_registration_count,
        drain.retained_token_count,
        drain.claimed_token_count,
        drain.adapter_active_attempt_count,
        drain.adapter_pending_owner_task_count,
        drain.retained_terminal_admission_count,
    ) == (0,) * 9
    census = depth_bridge_evidence_census_v8(tuple(system.payloads))
    assert census.open_terminal_reservation_count == 0
    assert len(transport.requests) == 1


@pytest.mark.asyncio
async def test_stale_snapshot_retries_exactly_three_then_fails() -> None:
    frame = _depth_frame(
        "BTCUSDT",
        first_update_id=200,
        final_update_id=201,
        previous_final_update_id=199,
    )
    transport = _SnapshotTransport(100)
    system = _build_system(("BTCUSDT",), (frame,), transport=transport)
    system.coordinator.bind_websocket_owner(system.owner)
    owner_task = asyncio.create_task(system.owner.run(system.lifecycle.stop_event))

    await _wait_for(lambda: _phases(system).count("CYCLE_TERMINAL") == 1)
    await _stop_owner(system, owner_task)
    await system.coordinator.aclose()

    assert len(transport.requests) == 3
    assert _phases(system).count("ATTEMPT_STARTED") == 3
    assert _phases(system).count("ATTEMPT_TERMINAL") == 3
    attempts = [
        cast(DepthBridgeAttemptTerminalV8, payload.material)
        for payload in system.payloads
        if payload.phase == "ATTEMPT_TERMINAL"
    ]
    assert [attempt.classification for attempt in attempts] == ["stale"] * 3
    terminal = next(
        cast(DepthBridgeCycleTerminalV8, payload.material)
        for payload in system.payloads
        if payload.phase == "CYCLE_TERMINAL"
    )
    assert (terminal.outcome, terminal.reason, terminal.terminal_bridge_attempt) == (
        "failed",
        "attempts_exhausted_stale",
        3,
    )


@pytest.mark.asyncio
async def test_five_symbols_never_start_a_fifth_adapter_io() -> None:
    symbols = ("ADAUSDT", "BNBUSDT", "BTCUSDT", "ETHUSDT", "SOLUSDT")
    first_by_symbol = {symbol: (ordinal + 1) * 100 for ordinal, symbol in enumerate(symbols)}
    frames = tuple(
        _depth_frame(
            symbol,
            first_update_id=first_by_symbol[symbol],
            final_update_id=first_by_symbol[symbol] + 1,
            previous_final_update_id=first_by_symbol[symbol] - 1,
        )
        for symbol in symbols
    )
    gate = asyncio.Event()
    transport = _SnapshotTransport(
        lambda symbol, _attempt: first_by_symbol[symbol],
        gate=gate,
    )
    system = _build_system(symbols, frames, transport=transport)
    system.coordinator.bind_websocket_owner(system.owner)
    owner_task = asyncio.create_task(system.owner.run(system.lifecycle.stop_event))

    await _wait_for(lambda: transport.active == 4)
    await asyncio.sleep(0.01)
    assert len(transport.requests) == 4
    assert system.coordinator.permit_in_use_count == 4
    gate.set()
    await _wait_for(lambda: _phases(system).count("CYCLE_TERMINAL") == 5)
    await _stop_owner(system, owner_task)
    await system.coordinator.aclose()

    assert len(transport.requests) == 5
    assert transport.maximum_active == 4
    assert system.coordinator.worker_count == 0


@pytest.mark.asyncio
async def test_pretrigger_range_capacity_accepts_1024_and_rejects_1025() -> None:
    frames = tuple(
        _depth_frame(
            "BTCUSDT",
            first_update_id=100 + index,
            final_update_id=100 + index,
            previous_final_update_id=99 + index,
        )
        for index in range(DEPTH_BRIDGE_MAXIMUM_BUFFERED_RANGES_PER_SYMBOL_V8 + 1)
    )
    system = _build_system(
        ("BTCUSDT", "ETHUSDT"),
        frames,
        transport=_SnapshotTransport(100),
    )
    system.coordinator.bind_websocket_owner(system.owner)

    with pytest.raises(RuntimeError):
        await system.owner.run(system.lifecycle.stop_event)
    slot = system.coordinator._slot_by_symbol["BTCUSDT"]
    assert len(slot.ranges) == DEPTH_BRIDGE_MAXIMUM_BUFFERED_RANGES_PER_SYMBOL_V8
    assert slot.range_buffer_overflow is True
    await system.coordinator.aclose()
    assert _phases(system) == ["GENERATION_STARTED", "GENERATION_DRAINED"]
    drain = cast(DepthBridgeGenerationDrainedV8, system.payloads[-1].material)
    assert drain.reason == "fatal"
    assert drain.fatal_cause_code == "pretrigger_range_buffer_overflow"


def test_bind_requires_exact_hook_callbacks_and_public_stream_census() -> None:
    frame = _depth_frame(
        "BTCUSDT",
        first_update_id=100,
        final_update_id=101,
        previous_final_update_id=99,
    )
    missing = _build_system(
        ("BTCUSDT",),
        (frame,),
        transport=_SnapshotTransport(100),
        with_callbacks=False,
    )
    with pytest.raises(ValueError, match="retained range callback"):
        missing.coordinator.bind_websocket_owner(missing.owner)

    foreign = _build_system(
        ("BTCUSDT", "ETHUSDT"),
        (frame,),
        transport=_SnapshotTransport(100),
    )
    foreign.owner._preconnecting_generation_hook = missing.coordinator
    foreign.owner._retained_depth_range_callback = missing.coordinator.retained_depth_range_callback
    foreign.owner._retained_depth_resync_callback = (
        missing.coordinator.retained_depth_resync_callback
    )
    with pytest.raises(ValueError, match="stream census"):
        missing.coordinator.bind_websocket_owner(foreign.owner)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "transport_factory",
    [
        lambda: (_ for _ in ()).throw(RuntimeError("transport factory failed")),
        lambda: cast(Any, object()),
    ],
)
async def test_prestart_construction_failure_retires_scheduler_transaction(
    transport_factory: Callable[[], httpx.AsyncBaseTransport | None],
) -> None:
    frame = _depth_frame(
        "BTCUSDT",
        first_update_id=100,
        final_update_id=101,
        previous_final_update_id=99,
    )
    system = _build_system(
        ("BTCUSDT",),
        (frame,),
        transport_factory=transport_factory,
    )
    system.coordinator.bind_websocket_owner(system.owner)

    with pytest.raises((RuntimeError, TypeError)):
        await system.owner.run(system.lifecycle.stop_event)
    authority = system.coordinator.schedule_authority
    assert authority.generation_open is False
    assert authority.retained_registration_count == 0
    assert authority.pending_registration_count == 0
    assert authority.retained_token_count == 0
    assert authority.claimed_token_count == 0
    assert system.payloads == []


@pytest.mark.asyncio
async def test_worker_cancellation_propagates_after_terminal_recovery() -> None:
    frame = _depth_frame(
        "BTCUSDT",
        first_update_id=100,
        final_update_id=101,
        previous_final_update_id=99,
    )
    gate = asyncio.Event()
    transport = _SnapshotTransport(100, gate=gate)
    system = _build_system(("BTCUSDT",), (frame,), transport=transport)
    system.coordinator.bind_websocket_owner(system.owner)
    owner_task = asyncio.create_task(system.owner.run(system.lifecycle.stop_event))
    await asyncio.wait_for(transport.request_started.wait(), timeout=3)
    worker = system.coordinator._workers["BTCUSDT"]

    worker.cancel()
    with pytest.raises(asyncio.CancelledError):
        await worker
    await _wait_for(lambda: system.coordinator.worker_count == 0)
    assert system.coordinator.adapter is not None
    assert system.coordinator.adapter.retained_terminal_admission_count == 0
    await asyncio.gather(owner_task, return_exceptions=True)
    await system.coordinator.aclose()

    terminal_attempts = [
        cast(DepthBridgeAttemptTerminalV8, payload.material)
        for payload in system.payloads
        if payload.phase == "ATTEMPT_TERMINAL"
    ]
    assert len(terminal_attempts) == 1
    assert terminal_attempts[0].classification == "failed"
    assert terminal_attempts[0].failure_code == "admission_cancelled"
    assert worker.cancelled()


@pytest.mark.asyncio
async def test_validate_current_is_read_only_and_rejects_callback_tampering() -> None:
    frame = _depth_frame(
        "BTCUSDT",
        first_update_id=100,
        final_update_id=101,
        previous_final_update_id=99,
    )
    system = _build_system(
        ("BTCUSDT",),
        (frame,),
        transport=_SnapshotTransport(100),
    )
    system.coordinator.bind_websocket_owner(system.owner)
    system.coordinator.validate_current()
    owner_task = asyncio.create_task(system.owner.run(system.lifecycle.stop_event))

    await _wait_for(lambda: system.coordinator.generation_open)
    before = (
        system.coordinator.worker_count,
        system.coordinator.permit_in_use_count,
        len(system.payloads),
    )
    system.coordinator.validate_current()
    assert before == (
        system.coordinator.worker_count,
        system.coordinator.permit_in_use_count,
        len(system.payloads),
    )

    exact_callback = system.owner._retained_depth_range_callback

    def foreign_callback(_receipt: object) -> None:
        return

    system.owner._retained_depth_range_callback = foreign_callback
    with pytest.raises(ValueError, match="foreign retained range callback"):
        system.coordinator.validate_current()
    system.owner._retained_depth_range_callback = exact_callback

    exact_slot = system.coordinator._slot_by_symbol["BTCUSDT"]
    foreign_slot = _build_system(
        ("BTCUSDT",),
        (),
        transport=_SnapshotTransport(100),
    ).coordinator._slot_by_symbol["BTCUSDT"]
    system.coordinator._slot_by_symbol["BTCUSDT"] = foreign_slot
    with pytest.raises(RuntimeError, match="symbol-to-slot identity"):
        system.coordinator.validate_current()
    system.coordinator._slot_by_symbol["BTCUSDT"] = exact_slot

    await _wait_for(lambda: _phases(system).count("CYCLE_TERMINAL") == 1)
    await _stop_owner(system, owner_task)
    await system.coordinator.aclose()
    system.coordinator.validate_current()


def test_validate_runtime_bindings_requires_every_exact_shared_authority() -> None:
    system = _build_system(
        ("BTCUSDT",),
        (),
        transport=_SnapshotTransport(100),
    )
    system.coordinator.bind_websocket_owner(system.owner)

    exact = {
        "promoting_plans": system.plans,
        "depth_plan": system.depth_plan,
        "websocket_owner": system.owner,
        "ingress": system.coordinator._ingress,
        "clock": system.coordinator._clock,
        "fatal_coordinator": system.coordinator._fatal_coordinator,
        "ledger": system.coordinator._ledger,
    }
    system.coordinator.validate_runtime_bindings(**exact)

    foreign = _build_system(
        ("BTCUSDT",),
        (),
        transport=_SnapshotTransport(100),
    )
    foreign_values = {
        "promoting_plans": foreign.plans,
        "depth_plan": foreign.depth_plan,
        "websocket_owner": foreign.owner,
        "ingress": foreign.coordinator._ingress,
        "clock": foreign.coordinator._clock,
        "fatal_coordinator": foreign.coordinator._fatal_coordinator,
        "ledger": foreign.coordinator._ledger,
    }
    labels = {
        "promoting_plans": "promoting plan tuple",
        "depth_plan": "depth plan",
        "websocket_owner": "WebSocket owner",
        "ingress": "shared ingress",
        "clock": "receipt clock",
        "fatal_coordinator": "fatal coordinator",
        "ledger": "integrity ledger",
    }
    for field_name, foreign_value in foreign_values.items():
        supplied = dict(exact)
        supplied[field_name] = foreign_value
        with pytest.raises(ValueError, match=f"foreign {labels[field_name]}"):
            system.coordinator.validate_runtime_bindings(**supplied)


@pytest.mark.asyncio
async def test_new_trigger_supersedes_cycle_during_claimed_adapter_capture() -> None:
    first_frame = _depth_frame(
        "BTCUSDT",
        first_update_id=100,
        final_update_id=101,
        previous_final_update_id=99,
    )
    gap_frame = _depth_frame(
        "BTCUSDT",
        first_update_id=300,
        final_update_id=301,
        previous_final_update_id=999,
    )
    gate = asyncio.Event()
    transport = _SnapshotTransport(
        lambda _symbol, attempt: 100 if attempt == 1 else 300,
        gate=gate,
    )

    async def frames() -> AsyncIterator[str]:
        yield first_frame
        await transport.request_started.wait()
        yield gap_frame

    system = _build_system(("BTCUSDT",), frames(), transport=transport)
    system.coordinator.bind_websocket_owner(system.owner)
    owner_task = asyncio.create_task(system.owner.run(system.lifecycle.stop_event))

    await _wait_for(lambda: _phases(system).count("TRIGGER_REGISTERED") == 2)
    assert transport.active == 1
    assert system.coordinator.schedule_authority.claimed_token_count == 1
    gate.set()
    await _wait_for(lambda: _phases(system).count("CYCLE_TERMINAL") == 2)
    await _stop_owner(system, owner_task)
    await system.coordinator.aclose()

    terminals = [
        cast(DepthBridgeCycleTerminalV8, payload.material)
        for payload in system.payloads
        if payload.phase == "CYCLE_TERMINAL"
    ]
    assert [(terminal.outcome, terminal.reason) for terminal in terminals] == [
        ("superseded", "newer_trigger"),
        ("accepted", "snapshot_range_bridge"),
    ]
    second_trigger_index = [
        index
        for index, payload in enumerate(system.payloads)
        if payload.phase == "TRIGGER_REGISTERED"
    ][1]
    first_attempt_terminal_index = next(
        index
        for index, payload in enumerate(system.payloads)
        if payload.phase == "ATTEMPT_TERMINAL"
    )
    assert second_trigger_index < first_attempt_terminal_index


@pytest.mark.asyncio
async def test_waiting_snapshot_times_out_three_times_before_cycle_failure() -> None:
    frame = _depth_frame(
        "BTCUSDT",
        first_update_id=100,
        final_update_id=101,
        previous_final_update_id=99,
    )
    transport = _SnapshotTransport(200)
    system = _build_system(("BTCUSDT",), (frame,), transport=transport)
    system.coordinator.bind_websocket_owner(system.owner)
    owner_task = asyncio.create_task(system.owner.run(system.lifecycle.stop_event))

    await _wait_for(
        lambda: _phases(system).count("CYCLE_TERMINAL") == 1,
        within_seconds=9.0,
    )
    await _stop_owner(system, owner_task)
    await system.coordinator.aclose()

    waits = [
        cast(DepthBridgeWaitTerminalV8, payload.material)
        for payload in system.payloads
        if payload.phase == "WAIT_TERMINAL"
    ]
    assert len(transport.requests) == 3
    assert [wait.outcome for wait in waits] == ["timeout"] * 3
    terminal = next(
        cast(DepthBridgeCycleTerminalV8, payload.material)
        for payload in system.payloads
        if payload.phase == "CYCLE_TERMINAL"
    )
    assert (terminal.outcome, terminal.reason, terminal.terminal_bridge_attempt) == (
        "failed",
        "attempts_exhausted_timeout",
        3,
    )


@pytest.mark.asyncio
async def test_reconnect_drains_predecessor_before_successor_and_rejects_old_receipt() -> None:
    first_frame = _depth_frame(
        "BTCUSDT",
        first_update_id=100,
        final_update_id=101,
        previous_final_update_id=99,
    )
    successor_frame = _depth_frame(
        "BTCUSDT",
        first_update_id=200,
        final_update_id=201,
        previous_final_update_id=199,
    )
    transport = _SnapshotTransport(200)
    system = _build_system(
        ("BTCUSDT",),
        (),
        transport=transport,
    )
    connector = _ReconnectConnector(
        (first_frame,),
        (successor_frame,),
        system.lifecycle.stop_event,
    )
    system.owner.connector = connector
    captured_receipts: list[Any] = []
    retain = system.coordinator.retain_depth_range

    def record_then_retain(receipt: Any) -> None:
        captured_receipts.append(receipt)
        retain(receipt)

    system.coordinator.retain_depth_range = record_then_retain  # pyright: ignore[reportAttributeAccessIssue]
    system.coordinator.bind_websocket_owner(system.owner)
    owner_task = asyncio.create_task(system.owner.run(system.lifecycle.stop_event))

    await _wait_for(lambda: _phases(system).count("GENERATION_STARTED") == 2)
    await _wait_for(lambda: len(captured_receipts) >= 2)
    system.coordinator.validate_current()
    with pytest.raises(ValueError, match="differs from current lineage"):
        system.coordinator.retained_depth_range_callback(captured_receipts[0])
    await asyncio.gather(owner_task, return_exceptions=True)
    await system.coordinator.aclose()

    generation_phases = [
        (payload.phase, payload.connection_generation) for payload in system.payloads
    ]
    first_drain_index = generation_phases.index(("GENERATION_DRAINED", 1))
    successor_start_index = generation_phases.index(("GENERATION_STARTED", 2))
    assert first_drain_index < successor_start_index
    assert connector.connection_count == 2
    drains = [
        cast(DepthBridgeGenerationDrainedV8, payload.material)
        for payload in system.payloads
        if payload.phase == "GENERATION_DRAINED"
    ]
    assert [drain.reason for drain in drains] == ["reconnect", "fatal"]


@pytest.mark.asyncio
async def test_generation_drain_ledger_failure_retries_exact_payload_once() -> None:
    frame = _depth_frame(
        "BTCUSDT",
        first_update_id=100,
        final_update_id=101,
        previous_final_update_id=99,
    )
    system = _build_system(
        ("BTCUSDT",),
        (frame,),
        transport=_SnapshotTransport(100),
    )
    system.coordinator.bind_websocket_owner(system.owner)
    owner_task = asyncio.create_task(system.owner.run(system.lifecycle.stop_event))
    await _wait_for(lambda: _phases(system).count("CYCLE_TERMINAL") == 1)
    await _stop_owner(system, owner_task)

    ledger = system.coordinator._ledger
    append = ledger.append_depth_bridge_v8
    rejected: list[DepthBridgeEvidencePayloadV8] = []

    def fail_first_drain(
        payload: DepthBridgeEvidencePayloadV8,
        plans: tuple[ProvisionalPromotingPlanV8, ...],
        depth_plan: ProvisionalDepthRestQualificationPlanV8,
    ) -> object:
        if payload.phase == "GENERATION_DRAINED" and not rejected:
            rejected.append(payload)
            raise RuntimeError("ledger outage")
        return append(payload, plans, depth_plan)

    ledger.append_depth_bridge_v8 = fail_first_drain  # pyright: ignore[reportAttributeAccessIssue]
    with pytest.raises(RuntimeError, match="ledger outage"):
        await system.coordinator.aclose()

    assert system.coordinator.generation_open is True
    assert system.coordinator.schedule_authority.generation_open is False
    assert system.coordinator._pending_generation_drained_payload is rejected[0]
    system.coordinator.validate_current()

    await system.coordinator.aclose()
    assert system.coordinator.generation_open is False
    assert system.payloads[-1] is rejected[0]
    assert _phases(system).count("GENERATION_DRAINED") == 1
    system.coordinator.validate_current()


@pytest.mark.asyncio
async def test_prestart_cancelled_worker_is_removed_from_slot_and_registry() -> None:
    frame = _depth_frame(
        "BTCUSDT",
        first_update_id=100,
        final_update_id=101,
        previous_final_update_id=99,
    )
    transport = _SnapshotTransport(100)
    system = _build_system(("BTCUSDT",), (frame,), transport=transport)
    ensure_worker = system.coordinator._ensure_worker
    cancelled_tasks: list[asyncio.Task[None]] = []

    def create_then_cancel(slot: Any) -> None:
        ensure_worker(slot)
        task = slot.worker
        assert task is not None
        cancelled_tasks.append(task)
        task.cancel()

    system.coordinator._ensure_worker = create_then_cancel  # pyright: ignore[reportAttributeAccessIssue]
    system.coordinator.bind_websocket_owner(system.owner)
    owner_task = asyncio.create_task(system.owner.run(system.lifecycle.stop_event))

    await _wait_for(lambda: bool(cancelled_tasks))
    await _wait_for(lambda: system.coordinator.worker_count == 0)
    assert system.coordinator._slot_by_symbol["BTCUSDT"].worker is None
    assert cancelled_tasks[0].cancelled()
    assert transport.requests == []

    system.coordinator._ensure_worker = ensure_worker  # pyright: ignore[reportAttributeAccessIssue]
    await asyncio.gather(owner_task, return_exceptions=True)
    await system.coordinator.aclose()
    assert system.coordinator.worker_count == 0
    drain = cast(DepthBridgeGenerationDrainedV8, system.payloads[-1].material)
    assert drain.reason == "fatal"


@pytest.mark.asyncio
async def test_explicit_fatal_drain_latches_deterministic_synthetic_cause() -> None:
    system = _build_system(
        ("BTCUSDT",),
        (),
        transport=_SnapshotTransport(100),
    )
    system.coordinator.bind_websocket_owner(system.owner)
    owner_task = asyncio.create_task(system.owner.run(system.lifecycle.stop_event))
    await _wait_for(lambda: system.coordinator.generation_open)

    await system.coordinator.drain_generation("fatal")
    await asyncio.gather(owner_task, return_exceptions=True)

    assert _phases(system) == ["GENERATION_STARTED", "GENERATION_DRAINED"]
    drain = cast(DepthBridgeGenerationDrainedV8, system.payloads[-1].material)
    expected_type = (
        f"{PublicDepthRestBridgeCoordinatorV8.__module__}."
        "PublicDepthRestBridgeCoordinatorErrorV8"
    )
    expected_hash = hashlib.sha256(
        f"coordinator_failure\0{expected_type}".encode()
    ).hexdigest()
    assert (drain.reason, drain.fatal_cause_code, drain.fatal_cause_sha256) == (
        "fatal",
        "coordinator_failure",
        expected_hash,
    )


@pytest.mark.asyncio
async def test_caller_cancellation_abort_latches_actual_cause_then_propagates() -> None:
    system = _build_system(
        ("BTCUSDT",),
        (),
        transport=_SnapshotTransport(100),
    )
    system.coordinator.bind_websocket_owner(system.owner)
    owner_task = asyncio.create_task(system.owner.run(system.lifecycle.stop_event))
    await _wait_for(lambda: system.coordinator.generation_open)
    observed_causes: list[asyncio.CancelledError] = []

    async def cancellable_runtime() -> None:
        never = asyncio.Event()
        try:
            await never.wait()
        except asyncio.CancelledError as cause:
            observed_causes.append(cause)
            await system.coordinator.abort_and_drain(cause)
            raise

    runtime_task = asyncio.create_task(cancellable_runtime())
    await asyncio.sleep(0)
    runtime_task.cancel("caller cancelled")
    with pytest.raises(asyncio.CancelledError, match="caller cancelled"):
        await runtime_task
    await asyncio.gather(owner_task, return_exceptions=True)

    assert len(observed_causes) == 1
    drain = cast(DepthBridgeGenerationDrainedV8, system.payloads[-1].material)
    cause_type = type(observed_causes[0])
    expected_hash = hashlib.sha256(
        (
            "coordinator_failure\0"
            f"{cause_type.__module__}.{cause_type.__qualname__}"
        ).encode()
    ).hexdigest()
    assert (drain.reason, drain.fatal_cause_code, drain.fatal_cause_sha256) == (
        "fatal",
        "coordinator_failure",
        expected_hash,
    )


@pytest.mark.asyncio
async def test_normal_close_receipt_is_exact_idempotent_and_projects_canonically() -> None:
    system = _build_system(
        ("BTCUSDT",),
        (),
        transport=_SnapshotTransport(100),
    )
    receipt = await _normally_close_empty_generation(system)

    replay = await system.coordinator.aclose()
    assert replay is receipt
    assert system.coordinator.clean_close_receipt is receipt
    assert system.coordinator.permanently_closed is True
    assert (
        receipt.generation_started_count,
        receipt.generation_drained_count,
        receipt.fatal_generation_count,
        receipt.close_reason,
    ) == (1, 1, 0, "normal_stop")
    assert (
        receipt.worker_count,
        receipt.permit_in_use_count,
        receipt.retained_registration_count,
        receipt.pending_registration_count,
        receipt.retained_token_count,
        receipt.claimed_token_count,
        receipt.adapter_active_attempt_count,
        receipt.adapter_pending_owner_task_count,
        receipt.retained_terminal_admission_count,
    ) == (0,) * 9
    last_event = system.coordinator._last_generation_drained_event
    assert type(last_event) is CaptureIntegrityEventV2
    assert (
        receipt.last_generation_drained_event_sequence,
        receipt.last_generation_drained_event_sha256,
    ) == (last_event.event_sequence, last_event.sha256)
    assert receipt.close_wall_ms >= last_event.recorded_wall_ms
    assert receipt.close_monotonic_ns >= last_event.recorded_monotonic_ns
    validate_depth_bridge_coordinator_clean_close_receipt_v8(
        receipt,
        promoting_plans=system.plans,
        depth_plan=system.depth_plan,
    )

    entry = depth_bridge_coordinator_closure_entry_v8(
        receipt,
        promoting_plans=system.plans,
        depth_plan=system.depth_plan,
    )
    assert type(entry) is DepthBridgeCoordinatorClosureEntryV8
    validate_depth_bridge_coordinator_closure_entry_v8(
        entry,
        promoting_plans=system.plans,
        depth_plan=system.depth_plan,
    )
    assert len(
        depth_bridge_coordinator_closure_entry_sha256_v8(
            entry,
            promoting_plans=system.plans,
            depth_plan=system.depth_plan,
        )
    ) == 64


@pytest.mark.asyncio
async def test_clean_close_allows_prior_reconnect_but_binds_last_normal_generation() -> None:
    system = _build_system(
        ("BTCUSDT",),
        (),
        transport=_SnapshotTransport(100),
    )
    connector = _ReconnectConnector((), (), system.lifecycle.stop_event)
    system.owner.connector = connector
    system.coordinator.bind_websocket_owner(system.owner)
    owner_task = asyncio.create_task(system.owner.run(system.lifecycle.stop_event))

    await _wait_for(lambda: _phases(system).count("GENERATION_STARTED") == 2)
    await _stop_owner(system, owner_task)
    receipt = await system.coordinator.aclose()

    assert type(receipt) is DepthBridgeCoordinatorCleanCloseReceiptV8
    assert (
        receipt.generation_started_count,
        receipt.generation_drained_count,
        receipt.fatal_generation_count,
        receipt.last_connection_generation,
    ) == (2, 2, 0, 2)
    drains = [
        cast(DepthBridgeGenerationDrainedV8, payload.material)
        for payload in system.payloads
        if payload.phase == "GENERATION_DRAINED"
    ]
    assert [drain.reason for drain in drains] == ["reconnect", "normal_stop"]


@pytest.mark.asyncio
async def test_no_generation_and_reconnect_only_close_never_mint_clean_authority() -> None:
    unused = _build_system(
        ("BTCUSDT",),
        (),
        transport=_SnapshotTransport(100),
    )
    unused.coordinator.bind_websocket_owner(unused.owner)
    assert await unused.coordinator.aclose() is None
    assert await unused.coordinator.aclose() is None
    assert unused.coordinator.permanently_closed is True
    with pytest.raises(PublicDepthRestBridgeCoordinatorErrorV8, match="permanently closed"):
        await unused.coordinator(cast(Any, object()))

    reconnect_only = _build_system(
        ("BTCUSDT",),
        (),
        transport=_SnapshotTransport(100),
    )
    reconnect_only.coordinator.bind_websocket_owner(reconnect_only.owner)
    owner_task = asyncio.create_task(
        reconnect_only.owner.run(reconnect_only.lifecycle.stop_event)
    )
    await _wait_for(lambda: reconnect_only.coordinator.generation_open)
    await reconnect_only.coordinator.drain_generation("reconnect")
    await _stop_owner(reconnect_only, owner_task)
    assert await reconnect_only.coordinator.aclose() is None
    assert reconnect_only.coordinator.clean_close_receipt is None


@pytest.mark.asyncio
async def test_fatal_and_cancelled_close_never_mint_clean_authority() -> None:
    fatal = _build_system(
        ("BTCUSDT",),
        (),
        transport=_SnapshotTransport(100),
    )
    fatal.coordinator.bind_websocket_owner(fatal.owner)
    fatal_owner_task = asyncio.create_task(fatal.owner.run(fatal.lifecycle.stop_event))
    await _wait_for(lambda: fatal.coordinator.generation_open)
    await fatal.coordinator.drain_generation("fatal")
    await asyncio.gather(fatal_owner_task, return_exceptions=True)
    assert await fatal.coordinator.aclose() is None
    assert fatal.coordinator.clean_close_receipt is None

    frame = _depth_frame(
        "BTCUSDT",
        first_update_id=100,
        final_update_id=101,
        previous_final_update_id=99,
    )
    gate = asyncio.Event()
    transport = _SnapshotTransport(100, gate=gate)
    cancelled = _build_system(("BTCUSDT",), (frame,), transport=transport)
    cancelled.coordinator.bind_websocket_owner(cancelled.owner)
    cancelled_owner_task = asyncio.create_task(
        cancelled.owner.run(cancelled.lifecycle.stop_event)
    )
    await asyncio.wait_for(transport.request_started.wait(), timeout=3)
    close_task = asyncio.create_task(cancelled.coordinator.aclose())
    await _wait_for(lambda: not cancelled.coordinator._callbacks_accepting)
    close_task.cancel("cancel clean bridge close")
    with pytest.raises(asyncio.CancelledError, match="cancel clean bridge close"):
        await close_task
    gate.set()
    await asyncio.gather(cancelled_owner_task, return_exceptions=True)
    assert await cancelled.coordinator.aclose() is None
    assert cancelled.coordinator.clean_close_receipt is None
    assert cancelled.coordinator.permanently_closed is True


@pytest.mark.asyncio
async def test_closed_coordinator_rejects_callbacks_and_generation_replay() -> None:
    frame = _depth_frame(
        "BTCUSDT",
        first_update_id=100,
        final_update_id=101,
        previous_final_update_id=99,
    )
    system = _build_system(
        ("BTCUSDT",),
        (frame,),
        transport=_SnapshotTransport(100),
    )
    contexts: list[Any] = []
    callbacks: list[Any] = []
    begin = system.coordinator._begin_generation
    retain = system.coordinator.retain_depth_range

    async def capture_context(context: Any) -> None:
        contexts.append(context)
        await begin(context)

    def capture_callback(receipt: Any) -> None:
        callbacks.append(receipt)
        retain(receipt)

    system.coordinator._begin_generation = capture_context  # pyright: ignore[reportAttributeAccessIssue]
    system.coordinator.retain_depth_range = capture_callback  # pyright: ignore[reportAttributeAccessIssue]
    system.coordinator.bind_websocket_owner(system.owner)
    owner_task = asyncio.create_task(system.owner.run(system.lifecycle.stop_event))
    await _wait_for(lambda: bool(contexts) and bool(callbacks))
    await _wait_for(lambda: _phases(system).count("CYCLE_TERMINAL") == 1)
    await _stop_owner(system, owner_task)
    assert await system.coordinator.aclose() is not None

    with pytest.raises(RuntimeError, match="permanently closed"):
        system.coordinator.retained_depth_range_callback(callbacks[0])
    with pytest.raises(PublicDepthRestBridgeCoordinatorErrorV8, match="permanently closed"):
        await system.coordinator(contexts[0])


@pytest.mark.asyncio
async def test_clean_close_factory_replay_and_tamper_guards() -> None:
    system = _build_system(
        ("BTCUSDT",),
        (),
        transport=_SnapshotTransport(100),
    )
    receipt = await _normally_close_empty_generation(system)
    constructor_values = {
        value.name: getattr(receipt, value.name)
        for value in fields(receipt)
        if value.init
    }
    with pytest.raises(TypeError, match="factory-sealed"):
        DepthBridgeCoordinatorCleanCloseReceiptV8(**constructor_values)

    entry = depth_bridge_coordinator_closure_entry_v8(
        receipt,
        promoting_plans=system.plans,
        depth_plan=system.depth_plan,
    )
    tampered_entry = replace(entry, receipt_sha256="0" * 64)
    with pytest.raises(DepthBridgeEvidenceErrorV8, match="receipt digest changed"):
        validate_depth_bridge_coordinator_closure_entry_v8(
            tampered_entry,
            promoting_plans=system.plans,
            depth_plan=system.depth_plan,
        )
    foreign_entry = replace(entry, plan_bundle_sha256="0" * 64)
    with pytest.raises(DepthBridgeEvidenceErrorV8, match="foreign plan authority"):
        validate_depth_bridge_coordinator_closure_entry_v8(
            foreign_entry,
            promoting_plans=system.plans,
            depth_plan=system.depth_plan,
        )

    object.__setattr__(receipt, "receipt_sha256", "0" * 64)
    with pytest.raises(DepthBridgeEvidenceErrorV8, match="digest changed"):
        validate_depth_bridge_coordinator_clean_close_receipt_v8(
            receipt,
            promoting_plans=system.plans,
            depth_plan=system.depth_plan,
        )
