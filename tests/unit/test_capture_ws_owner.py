from __future__ import annotations

import asyncio
import hashlib
import json
import threading
from collections.abc import AsyncIterable, AsyncIterator
from contextlib import AbstractAsyncContextManager
from copy import copy
from dataclasses import FrozenInstanceError, dataclass, field, replace
from pathlib import Path
from typing import Any, cast

import pytest

import signalbot.capture.ws_owner as owner_module
from signalbot.capture.depth_sequence import (
    DepthRangeObservation,
    DepthResyncRequest,
    DepthResyncUnavailable,
    DepthSequenceError,
)
from signalbot.capture.handoff import BoundedCaptureHandoff, CaptureFatalState
from signalbot.capture.models import (
    CaptureEnvelopeV1,
    CaptureRecord,
    ConnectionState,
    ConnectionTransitionV1,
    CoverageTransitionV1,
)
from signalbot.capture.pipeline import CapturePipeline
from signalbot.capture.plans import build_prospective_capture_plans
from signalbot.capture.receipts import IngestSequencer, ReceiptTimestamp
from signalbot.capture.writer_lease import WriterLease, WriterLeaseError
from signalbot.capture.ws_owner import (
    CaptureReconnectExhausted,
    PreconnectingGenerationHookDrainTimeout,
    PublicWebSocketCaptureOwner,
    WebSocketOwnerSettings,
    WebSocketPreconnectingGenerationContext,
    WebsocketsPublicConnector,
    validate_websocket_preconnecting_generation_context,
)
from signalbot.domain.enums import Market
from signalbot.exchange.binance.endpoints import WebSocketPlan
from signalbot.r4b_v2.capture.models import RawRecordV2
from signalbot.r4b_v2.capture.plans import (
    ProvisionalPromotingCapturePlanV2,
    build_provisional_promoting_capture_plans_v2,
)
from signalbot.r4b_v2.capture.websocket import (
    PublicRetainedDepthRangeCallbackReceiptV2,
    PublicRetainedDepthResyncCallbackReceiptV2,
    PublicWebSocketCaptureAdapterV2,
    PublicWebSocketFrameAdapterFactoryV2,
    SharedWebSocketIngressV2,
    validate_public_retained_depth_range_callback_receipt_v2,
    validate_public_retained_depth_resync_callback_receipt_v2,
)

PLAN_SHA256 = hashlib.sha256(b"capture-ws-owner-test-plan").hexdigest()
PROTOCOL_HASH = hashlib.sha256(b"capture-ws-owner-test-protocol").hexdigest()
NON_DEPTH_FRAME = '{"stream":"btcusdt@aggTrade","data":{"e":"aggTrade","s":"BTCUSDT"}}'


def _spot_depth_frame(symbol: str, *, first_u: int, final_u: int) -> str:
    stream = f"{symbol.lower()}@depth@100ms"
    return (
        f'{{"stream":"{stream}","data":'
        f'{{"e":"depthUpdate","s":"{symbol}","U":{first_u},"u":{final_u}}}}}'
    )


class _Clock:
    def __init__(self) -> None:
        self.calls = 0

    def capture(self) -> ReceiptTimestamp:
        self.calls += 1
        return ReceiptTimestamp(
            received_at_ms=1_720_000_000_000 + self.calls,
            received_monotonic_ns=10_000 + self.calls,
        )


@dataclass
class _MemoryWriter:
    records: list[CaptureRecord] = field(default_factory=list)
    closed: bool = False

    def append(self, record: CaptureRecord, encoded_line: bytes) -> None:
        del encoded_line
        self.records.append(record)

    def close(self) -> None:
        self.closed = True

    def abort(self) -> None:
        self.closed = True

    def write_emergency_transition(self, transition: CoverageTransitionV1) -> None:
        self.records.append(transition)


@dataclass
class _TraceHandoff:
    accepting: bool = True


class _TracePipeline:
    def __init__(self) -> None:
        self.fatal_state = CaptureFatalState()
        self.handoff = _TraceHandoff()
        self.offered: list[CaptureRecord] = []

    def offer(self, record: CaptureRecord) -> None:
        self.offered.append(record)


class _Connection(AbstractAsyncContextManager[object]):
    def __init__(self, outcome: object | BaseException) -> None:
        self.outcome = outcome

    async def __aenter__(self) -> object:
        if isinstance(self.outcome, BaseException):
            raise self.outcome
        return self.outcome

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: object,
    ) -> None:
        return None


class _Connector:
    def __init__(self, *outcomes: object | BaseException) -> None:
        self.outcomes = list(outcomes)
        self.urls: list[str] = []

    def __call__(self, url: str) -> Any:
        self.urls.append(url)
        if not self.outcomes:
            raise AssertionError("test connector has no remaining outcome")
        return _Connection(self.outcomes.pop(0))


@dataclass
class _InjectedLifecycleCoordinator:
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


class _InjectedFrameConsumer:
    def __init__(self, *, failure: BaseException | None = None) -> None:
        self.failure = failure
        self.frame_seq = 0
        self.frames: list[str | bytes] = []

    async def consume(self, frames: AsyncIterable[str | bytes]) -> None:
        async for frame in frames:
            self.frame_seq += 1
            self.frames.append(frame)
            if self.failure is not None:
                raise self.failure


class _InjectedFrameAdapterFactory:
    def __init__(
        self,
        *,
        failure: BaseException | None = None,
        session_id: str | None = None,
        protocol_hash: str | None = None,
    ) -> None:
        self.failure = failure
        self.session_id = session_id
        self.protocol_hash = protocol_hash
        self.calls: list[tuple[str, int]] = []
        self.adapters: list[_InjectedFrameConsumer] = []

    def __call__(
        self,
        *,
        connection_id: str,
        generation: int,
    ) -> _InjectedFrameConsumer:
        self.calls.append((connection_id, generation))
        adapter = _InjectedFrameConsumer(failure=self.failure)
        self.adapters.append(adapter)
        return adapter


class _R4BRecordingOfferer:
    def __init__(self) -> None:
        self.records: list[RawRecordV2] = []

    def offer(self, record: RawRecordV2) -> object:
        self.records.append(record)
        return record


class _R4BPassThroughRecoveryLifecycle:
    def __init__(self) -> None:
        self.fatal_causes: list[BaseException] = []

    async def complete_recovery_successor(self, record: RawRecordV2) -> None:
        del record

    def record_retained_frame(self, record: RawRecordV2) -> None:
        del record

    def trip_fatal(self, cause: BaseException) -> None:
        self.fatal_causes.append(cause)


class _RetainedDepthFrameAdapterFactory:
    """Generic-owner wrapper whose adapters remain sealed by the exact V2 factory."""

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


class _LeaseAdmissionGuard:
    def __init__(self, lease: WriterLease) -> None:
        self.lease = lease
        self.validations = 0

    def validate_current(self) -> None:
        self.lease.assert_held()
        self.validations += 1

    def connector_admission_guard(self):  # type: ignore[no-untyped-def]
        return self.lease.operation_guard()


class _BlockingEnterConnection(AbstractAsyncContextManager[object]):
    def __init__(
        self,
        *,
        entered: asyncio.Event,
        allow_enter: asyncio.Event,
        frames: object,
    ) -> None:
        self.entered = entered
        self.allow_enter = allow_enter
        self.frames = frames

    async def __aenter__(self) -> object:
        self.entered.set()
        await self.allow_enter.wait()
        return self.frames

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: object,
    ) -> None:
        del exc_type, exc_value, traceback


class _BlockingEnterConnector:
    def __init__(self, connection: _BlockingEnterConnection) -> None:
        self.connection = connection
        self.urls: list[str] = []

    def __call__(self, url: str) -> _BlockingEnterConnection:
        self.urls.append(url)
        return self.connection


class _StopAfterOneFrame:
    def __init__(self, stop_event: asyncio.Event, payload: str = NON_DEPTH_FRAME) -> None:
        self.stop_event = stop_event
        self.payload = payload

    async def __aiter__(self) -> AsyncIterator[str]:
        yield self.payload
        self.stop_event.set()
        await asyncio.Event().wait()


class _OneFrameUntilCancelled:
    async def __aiter__(self) -> AsyncIterator[bytes]:
        yield NON_DEPTH_FRAME.encode()
        await asyncio.Event().wait()


class _SpotBaselineUntilCancelled:
    def __init__(self, stop_after_baseline: asyncio.Event | None = None) -> None:
        self.stop_after_baseline = stop_after_baseline
        self.yielded_symbols: list[str] = []

    async def __aiter__(self) -> AsyncIterator[str]:
        for index, symbol in enumerate(("BTCUSDT", "ETHUSDT", "SOLUSDT"), start=1):
            self.yielded_symbols.append(symbol)
            yield _spot_depth_frame(symbol, first_u=index * 10, final_u=index * 10 + 2)
        if self.stop_after_baseline is not None:
            self.stop_after_baseline.set()
        await asyncio.Event().wait()


class _SpotGapUntilCancelled:
    async def __aiter__(self) -> AsyncIterator[str]:
        yield (
            '{"stream":"btcusdt@depth@100ms","data":'
            '{"e":"depthUpdate","s":"BTCUSDT","U":10,"u":12}}'
        )
        yield (
            '{"stream":"btcusdt@depth@100ms","data":'
            '{"e":"depthUpdate","s":"BTCUSDT","U":14,"u":15}}'
        )
        await asyncio.Event().wait()


class _MalformedUntilCancelled:
    async def __aiter__(self) -> AsyncIterator[str]:
        yield "not-json"
        await asyncio.Event().wait()


class _FuturesPublicDepthUntilCancelled:
    raw = (
        '{"stream":"btcusdt@depth@100ms","data":'
        '{"e":"depthUpdate","s":"BTCUSDT","U":10,"u":12,"pu":9,'
        '"ps":"BTCUSDT","st":1}}'
    )

    async def __aiter__(self) -> AsyncIterator[str]:
        yield self.raw
        await asyncio.Event().wait()


def _settings(**overrides: object) -> WebSocketOwnerSettings:
    values: dict[str, object] = {
        "maximum_connection_age_seconds": 1.0,
        "connect_timeout_seconds": 1.0,
        "close_timeout_seconds": 1.0,
        "heartbeat_interval_seconds": 1.0,
        "pong_timeout_seconds": 1.0,
        "internal_queue_frames": 8,
        "maximum_frame_bytes": 1024,
        "maximum_reconnect_attempts": 2,
        "reconnect_delays_seconds": (0.0, 0.0),
        "healthy_reset_seconds": 0.5,
    }
    values.update(overrides)
    return WebSocketOwnerSettings(**values)  # pyright: ignore[reportArgumentType]


def _pipeline() -> tuple[CapturePipeline, CaptureFatalState, _MemoryWriter]:
    fatal = CaptureFatalState()
    handoff = BoundedCaptureHandoff(max_events=100, max_bytes=1024 * 1024, fatal_state=fatal)
    writer = _MemoryWriter()
    return CapturePipeline(handoff, writer), fatal, writer


def _spot_plan() -> WebSocketPlan:
    return build_prospective_capture_plans(
        ("BTCUSDT", "ETHUSDT", "SOLUSDT"),
        batch_size=25,
    )[0]


def _futures_public_depth_plan() -> ProvisionalPromotingCapturePlanV2:
    plans = build_provisional_promoting_capture_plans_v2(("BTCUSDT",))
    public = tuple(
        plan
        for plan in plans
        if type(plan) is ProvisionalPromotingCapturePlanV2 and plan.route_id == "usdm_public"
    )
    assert len(public) == 1
    return public[0]


@pytest.mark.asyncio
async def test_owner_records_one_global_sequence_and_stops_cleanly() -> None:
    pipeline, fatal, writer = _pipeline()
    stop_event = fatal.stop_event
    connector = _Connector(_StopAfterOneFrame(stop_event))
    pipeline.start()
    owner = PublicWebSocketCaptureOwner(
        _spot_plan(),
        plan_sha256=PLAN_SHA256,
        process_boot_id="boot-1",
        pipeline=pipeline,
        clock=_Clock(),
        sequencer=IngestSequencer(),
        settings=_settings(),
        connector=connector,
        depth_resync_callback=lambda _request: None,
    )
    assert owner.generation == 0

    await owner.run(stop_event)
    await pipeline.stop()

    assert owner.generation == 1
    assert writer.closed is True
    assert fatal.failed is False
    assert len(connector.urls) == 1
    assert [record.ingest_seq for record in writer.records] == [1, 2, 3, 4]
    assert [type(record) for record in writer.records] == [
        ConnectionTransitionV1,
        ConnectionTransitionV1,
        CaptureEnvelopeV1,
        ConnectionTransitionV1,
    ]
    transitions = [
        record for record in writer.records if isinstance(record, ConnectionTransitionV1)
    ]
    assert [transition.state for transition in transitions] == [
        ConnectionState.CONNECTING,
        ConnectionState.CONNECTED,
        ConnectionState.DISCONNECTED,
    ]
    assert transitions[-1].reason == "owner_stop"
    assert transitions[-1].last_frame_seq == 1


@pytest.mark.asyncio
async def test_injected_boundaries_reuse_the_owner_socket_and_stop_loop() -> None:
    lifecycle = _InjectedLifecycleCoordinator()
    adapter_factory = _InjectedFrameAdapterFactory()
    connector = _Connector(_StopAfterOneFrame(lifecycle.stop_event))
    owner = PublicWebSocketCaptureOwner(
        _spot_plan(),
        plan_sha256=PLAN_SHA256,
        process_boot_id="boot-injected",
        settings=_settings(),
        connector=connector,
        frame_adapter_factory=adapter_factory,
        lifecycle_coordinator=lifecycle,
    )
    assert owner.preconnecting_generation_hook is None

    await owner.run(lifecycle.stop_event)

    connection_id = "capture-spot-1-g000001"
    assert connector.urls == [_spot_plan().url]
    assert adapter_factory.calls == [(connection_id, 1)]
    assert len(adapter_factory.adapters) == 1
    assert adapter_factory.adapters[0].frames == [NON_DEPTH_FRAME]
    assert lifecycle.fatal_causes == []
    assert lifecycle.transitions == [
        (connection_id, 1, 0, ConnectionState.CONNECTING, "connect_attempt"),
        (connection_id, 1, 0, ConnectionState.CONNECTED, "public_session_open"),
        (connection_id, 1, 1, ConnectionState.DISCONNECTED, "owner_stop"),
    ]


def test_generation_context_rejects_direct_construction() -> None:
    plan = _spot_plan()

    with pytest.raises(TypeError, match="exact owner"):
        WebSocketPreconnectingGenerationContext(
            session_id="session-direct-construction",
            protocol_hash=PROTOCOL_HASH,
            market=Market.SPOT,
            route=plan.route,
            connection_id="capture-spot-1-g000001",
            generation=1,
        )


@pytest.mark.asyncio
async def test_generation_hook_gets_immutable_lineage_before_connecting() -> None:
    lifecycle = _InjectedLifecycleCoordinator()
    factory = _InjectedFrameAdapterFactory(
        session_id="session-generation-hook",
        protocol_hash=PROTOCOL_HASH,
    )
    connector = _Connector(_StopAfterOneFrame(lifecycle.stop_event))
    contexts: list[WebSocketPreconnectingGenerationContext] = []
    owners: list[PublicWebSocketCaptureOwner] = []

    async def hook(context: WebSocketPreconnectingGenerationContext) -> None:
        assert lifecycle.transitions == []
        assert connector.urls == []
        validate_websocket_preconnecting_generation_context(
            context,
            owner=owners[0],
            hook=hook,
        )
        with pytest.raises(FrozenInstanceError):
            context.generation = 99  # pyright: ignore[reportAttributeAccessIssue]
        contexts.append(context)

    plan = _spot_plan()
    owner = PublicWebSocketCaptureOwner(
        plan,
        plan_sha256=PLAN_SHA256,
        process_boot_id="boot-generation-hook",
        settings=_settings(),
        connector=connector,
        frame_adapter_factory=factory,
        lifecycle_coordinator=lifecycle,
        preconnecting_generation_hook=hook,
    )
    owners.append(owner)
    with pytest.raises(AttributeError):
        owner.preconnecting_generation_hook = None  # pyright: ignore[reportAttributeAccessIssue]
    assert owner.preconnecting_generation_hook is hook

    await owner.run(lifecycle.stop_event)

    assert len(contexts) == 1
    context = contexts[0]
    assert context.session_id == "session-generation-hook"
    assert context.protocol_hash == PROTOCOL_HASH
    assert context.market is Market.SPOT
    assert context.route == plan.route
    assert context.connection_id == "capture-spot-1-g000001"
    assert context.generation == 1
    with pytest.raises(RuntimeError, match="already completed"):
        validate_websocket_preconnecting_generation_context(
            context,
            owner=owner,
            hook=hook,
        )
    assert lifecycle.fatal_causes == []
    assert len(connector.urls) == 1


@pytest.mark.asyncio
async def test_generation_context_rejects_copy_subclass_foreign_and_tamper() -> None:
    lifecycle = _InjectedLifecycleCoordinator()
    factory = _InjectedFrameAdapterFactory(
        session_id="session-generation-capability",
        protocol_hash=PROTOCOL_HASH,
    )
    connector = _Connector(_StopAfterOneFrame(lifecycle.stop_event))
    plan = _spot_plan()
    owners: list[PublicWebSocketCaptureOwner] = []

    async def foreign_hook(context: WebSocketPreconnectingGenerationContext) -> None:
        del context

    async def hook(context: WebSocketPreconnectingGenerationContext) -> None:
        owner = owners[0]
        validate_websocket_preconnecting_generation_context(
            context,
            owner=owner,
            hook=hook,
        )

        copied = copy(context)
        assert copied is not context
        with pytest.raises(RuntimeError, match="not active"):
            validate_websocket_preconnecting_generation_context(
                copied,
                owner=owner,
                hook=hook,
            )
        with pytest.raises(TypeError, match="exact owner"):
            replace(context)

        class ForeignContext(WebSocketPreconnectingGenerationContext):
            pass

        foreign_context = object.__new__(ForeignContext)
        with pytest.raises(TypeError, match="foreign type"):
            validate_websocket_preconnecting_generation_context(
                foreign_context,
                owner=owner,
                hook=hook,
            )
        direct_uninitialized = object.__new__(WebSocketPreconnectingGenerationContext)
        with pytest.raises(TypeError, match="factory-sealed"):
            validate_websocket_preconnecting_generation_context(
                direct_uninitialized,
                owner=owner,
                hook=hook,
            )

        foreign_lifecycle = _InjectedLifecycleCoordinator()
        foreign_owner = PublicWebSocketCaptureOwner(
            plan,
            plan_sha256=PLAN_SHA256,
            process_boot_id="boot-generation-capability-foreign",
            settings=_settings(),
            connector=_Connector(),
            frame_adapter_factory=_InjectedFrameAdapterFactory(
                session_id="session-generation-capability",
                protocol_hash=PROTOCOL_HASH,
            ),
            lifecycle_coordinator=foreign_lifecycle,
            preconnecting_generation_hook=hook,
        )
        with pytest.raises(ValueError, match="another owner"):
            validate_websocket_preconnecting_generation_context(
                context,
                owner=foreign_owner,
                hook=hook,
            )
        with pytest.raises(ValueError, match="foreign hook"):
            validate_websocket_preconnecting_generation_context(
                context,
                owner=owner,
                hook=foreign_hook,
            )

        tampered_values: tuple[tuple[str, object], ...] = (
            ("session_id", "session-tampered"),
            ("protocol_hash", "0" * 64),
            ("market", Market.FUTURES),
            ("route", "market"),
            ("connection_id", "capture-spot-1-g999999"),
            ("generation", 2),
            ("_factory_seal", object()),
            ("_owner_capability", object()),
            ("_hook_identity", foreign_hook),
            ("_owner_plan", replace(plan, name="capture-spot-foreign")),
        )
        for field_name, tampered_value in tampered_values:
            original = getattr(context, field_name)
            object.__setattr__(context, field_name, tampered_value)
            with pytest.raises((TypeError, ValueError)):
                validate_websocket_preconnecting_generation_context(
                    context,
                    owner=owner,
                    hook=hook,
                )
            object.__setattr__(context, field_name, original)
            validate_websocket_preconnecting_generation_context(
                context,
                owner=owner,
                hook=hook,
            )

    owner = PublicWebSocketCaptureOwner(
        plan,
        plan_sha256=PLAN_SHA256,
        process_boot_id="boot-generation-capability",
        settings=_settings(),
        connector=connector,
        frame_adapter_factory=factory,
        lifecycle_coordinator=lifecycle,
        preconnecting_generation_hook=hook,
    )
    owners.append(owner)

    await owner.run(lifecycle.stop_event)

    assert lifecycle.fatal_causes == []
    assert len(connector.urls) == 1


@pytest.mark.asyncio
async def test_generation_hook_failure_is_fatal_before_connecting_without_retry() -> None:
    lifecycle = _InjectedLifecycleCoordinator()
    factory = _InjectedFrameAdapterFactory()
    connector = _Connector()
    failure = OSError("generation handoff failed")
    contexts: list[WebSocketPreconnectingGenerationContext] = []

    async def hook(context: WebSocketPreconnectingGenerationContext) -> None:
        contexts.append(context)
        raise failure

    owner = PublicWebSocketCaptureOwner(
        _spot_plan(),
        plan_sha256=PLAN_SHA256,
        process_boot_id="boot-generation-hook-failure",
        settings=_settings(),
        connector=connector,
        frame_adapter_factory=factory,
        lifecycle_coordinator=lifecycle,
        preconnecting_generation_hook=hook,
    )

    with pytest.raises(OSError) as captured:
        await owner.run(lifecycle.stop_event)

    assert captured.value is failure
    assert owner.generation == 1
    assert len(contexts) == 1
    assert lifecycle.fatal_causes == [failure]
    assert lifecycle.stop_event.is_set()
    assert lifecycle.transitions == []
    assert connector.urls == []
    assert factory.calls == []
    with pytest.raises(RuntimeError, match="already invalidated"):
        validate_websocket_preconnecting_generation_context(
            contexts[0],
            owner=owner,
            hook=hook,
        )


@pytest.mark.asyncio
async def test_generation_hook_cancellation_is_fatal_before_connecting() -> None:
    lifecycle = _InjectedLifecycleCoordinator()
    factory = _InjectedFrameAdapterFactory()
    connector = _Connector()
    entered = asyncio.Event()
    contexts: list[WebSocketPreconnectingGenerationContext] = []

    async def hook(context: WebSocketPreconnectingGenerationContext) -> None:
        contexts.append(context)
        entered.set()
        await asyncio.Event().wait()

    owner = PublicWebSocketCaptureOwner(
        _spot_plan(),
        plan_sha256=PLAN_SHA256,
        process_boot_id="boot-generation-hook-cancel",
        settings=_settings(),
        connector=connector,
        frame_adapter_factory=factory,
        lifecycle_coordinator=lifecycle,
        preconnecting_generation_hook=hook,
    )
    owner_task = asyncio.create_task(owner.run(lifecycle.stop_event))
    await asyncio.wait_for(entered.wait(), timeout=1)

    owner_task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await owner_task

    assert owner.generation == 1
    assert len(contexts) == 1
    assert len(lifecycle.fatal_causes) == 1
    assert isinstance(lifecycle.fatal_causes[0], asyncio.CancelledError)
    assert lifecycle.stop_event.is_set()
    assert lifecycle.transitions == []
    assert connector.urls == []
    assert factory.calls == []
    with pytest.raises(RuntimeError, match="already invalidated"):
        validate_websocket_preconnecting_generation_context(
            contexts[0],
            owner=owner,
            hook=hook,
        )


@pytest.mark.asyncio
async def test_stuck_generation_hook_drains_on_normal_stop_without_connecting() -> None:
    lifecycle = _InjectedLifecycleCoordinator()
    factory = _InjectedFrameAdapterFactory()
    connector = _Connector()
    entered = asyncio.Event()
    drained = asyncio.Event()
    hook_tasks: list[asyncio.Task[object]] = []

    async def hook(context: WebSocketPreconnectingGenerationContext) -> None:
        del context
        current = asyncio.current_task()
        assert current is not None
        hook_tasks.append(cast(asyncio.Task[object], current))
        entered.set()
        try:
            await asyncio.Event().wait()
        finally:
            drained.set()

    owner = PublicWebSocketCaptureOwner(
        _spot_plan(),
        plan_sha256=PLAN_SHA256,
        process_boot_id="boot-generation-hook-stop",
        settings=_settings(),
        connector=connector,
        frame_adapter_factory=factory,
        lifecycle_coordinator=lifecycle,
        preconnecting_generation_hook=hook,
    )
    owner_task = asyncio.create_task(owner.run(lifecycle.stop_event))
    await asyncio.wait_for(entered.wait(), timeout=1)

    lifecycle.stop_event.set()
    await asyncio.wait_for(owner_task, timeout=1)
    await asyncio.wait_for(drained.wait(), timeout=1)

    assert owner.generation == 1
    assert len(hook_tasks) == 1 and hook_tasks[0].done()
    assert lifecycle.fatal_causes == []
    assert lifecycle.transitions == []
    assert connector.urls == []
    assert factory.calls == []


@pytest.mark.asyncio
async def test_cancellation_suppressing_hook_marks_explicit_dirty_owned_task() -> None:
    lifecycle = _InjectedLifecycleCoordinator()
    factory = _InjectedFrameAdapterFactory()
    connector = _Connector()
    entered = asyncio.Event()
    release = asyncio.Event()
    repeated_cancel = asyncio.Event()
    hook_tasks: list[asyncio.Task[object]] = []
    cancellation_count = 0

    async def hook(context: WebSocketPreconnectingGenerationContext) -> None:
        nonlocal cancellation_count
        del context
        current = asyncio.current_task()
        assert current is not None
        hook_tasks.append(cast(asyncio.Task[object], current))
        entered.set()
        while not release.is_set():
            try:
                await release.wait()
            except asyncio.CancelledError:
                cancellation_count += 1
                if cancellation_count >= 2:
                    repeated_cancel.set()

    owner = PublicWebSocketCaptureOwner(
        _spot_plan(),
        plan_sha256=PLAN_SHA256,
        process_boot_id="boot-generation-hook-dirty",
        settings=replace(_settings(), close_timeout_seconds=0.01),
        connector=connector,
        frame_adapter_factory=factory,
        lifecycle_coordinator=lifecycle,
        preconnecting_generation_hook=hook,
    )
    owner_task = asyncio.create_task(owner.run(lifecycle.stop_event))
    await asyncio.wait_for(entered.wait(), timeout=1)

    lifecycle.stop_event.set()
    try:
        with pytest.raises(PreconnectingGenerationHookDrainTimeout) as captured:
            await asyncio.wait_for(owner_task, timeout=1)
        await asyncio.wait_for(repeated_cancel.wait(), timeout=1)

        pending = owner.pending_preconnecting_generation_hook_task
        assert pending is hook_tasks[0]
        assert pending is not None and not pending.done()
        assert owner.preconnecting_generation_hook_dirty_error is captured.value
        assert lifecycle.fatal_causes[-1] is captured.value
        assert lifecycle.transitions == []
        assert connector.urls == []
        assert factory.calls == []
    finally:
        release.set()
        if hook_tasks:
            await asyncio.wait_for(hook_tasks[0], timeout=1)

    assert owner.pending_preconnecting_generation_hook_task is None
    assert owner.preconnecting_generation_hook_dirty_error is not None


@pytest.mark.asyncio
async def test_generation_hook_timeout_is_fatal_and_drained_before_connecting() -> None:
    lifecycle = _InjectedLifecycleCoordinator()
    factory = _InjectedFrameAdapterFactory()
    connector = _Connector()
    drained = asyncio.Event()

    async def hook(context: WebSocketPreconnectingGenerationContext) -> None:
        del context
        try:
            await asyncio.Event().wait()
        finally:
            drained.set()

    owner = PublicWebSocketCaptureOwner(
        _spot_plan(),
        plan_sha256=PLAN_SHA256,
        process_boot_id="boot-generation-hook-timeout",
        settings=replace(_settings(), connect_timeout_seconds=0.01),
        connector=connector,
        frame_adapter_factory=factory,
        lifecycle_coordinator=lifecycle,
        preconnecting_generation_hook=hook,
    )

    with pytest.raises(TimeoutError, match="generation hook exceeded"):
        await owner.run(lifecycle.stop_event)
    await asyncio.wait_for(drained.wait(), timeout=1)

    assert owner.generation == 1
    assert len(lifecycle.fatal_causes) == 1
    assert isinstance(lifecycle.fatal_causes[0], TimeoutError)
    assert lifecycle.stop_event.is_set()
    assert lifecycle.transitions == []
    assert connector.urls == []
    assert factory.calls == []


@pytest.mark.asyncio
async def test_generation_hook_runs_once_before_each_reconnect_transition() -> None:
    lifecycle = _InjectedLifecycleCoordinator()
    factory = _InjectedFrameAdapterFactory(
        session_id="session-generation-reconnect",
        protocol_hash=PROTOCOL_HASH,
    )
    connector = _Connector(
        OSError("first connect fails"),
        _StopAfterOneFrame(lifecycle.stop_event),
    )
    contexts: list[WebSocketPreconnectingGenerationContext] = []
    owners: list[PublicWebSocketCaptureOwner] = []

    async def hook(context: WebSocketPreconnectingGenerationContext) -> None:
        assert len(connector.urls) == context.generation - 1
        assert all(transition[1] < context.generation for transition in lifecycle.transitions)
        validate_websocket_preconnecting_generation_context(
            context,
            owner=owners[0],
            hook=hook,
        )
        if contexts:
            with pytest.raises(RuntimeError, match="already completed"):
                validate_websocket_preconnecting_generation_context(
                    contexts[-1],
                    owner=owners[0],
                    hook=hook,
                )
        contexts.append(context)

    owner = PublicWebSocketCaptureOwner(
        _spot_plan(),
        plan_sha256=PLAN_SHA256,
        process_boot_id="boot-generation-hook-reconnect",
        settings=_settings(),
        connector=connector,
        frame_adapter_factory=factory,
        lifecycle_coordinator=lifecycle,
        preconnecting_generation_hook=hook,
    )
    owners.append(owner)

    await owner.run(lifecycle.stop_event)

    assert [context.generation for context in contexts] == [1, 2]
    assert [context.connection_id for context in contexts] == [
        "capture-spot-1-g000001",
        "capture-spot-1-g000002",
    ]
    assert all(
        context.session_id == "session-generation-reconnect"
        and context.protocol_hash == PROTOCOL_HASH
        for context in contexts
    )
    assert len(connector.urls) == 2
    assert [transition[3] for transition in lifecycle.transitions] == [
        ConnectionState.CONNECTING,
        ConnectionState.DISCONNECTED,
        ConnectionState.CONNECTING,
        ConnectionState.CONNECTED,
        ConnectionState.DISCONNECTED,
    ]
    assert lifecycle.fatal_causes == []


@pytest.mark.asyncio
async def test_required_unbound_admission_fails_before_transition_or_connector() -> None:
    lifecycle = _InjectedLifecycleCoordinator()
    factory = _InjectedFrameAdapterFactory()
    connector = _Connector()
    owner = PublicWebSocketCaptureOwner(
        _spot_plan(),
        plan_sha256=PLAN_SHA256,
        process_boot_id="boot-unbound-admission",
        settings=_settings(),
        connector=connector,
        frame_adapter_factory=factory,
        lifecycle_coordinator=lifecycle,
        requires_preconnect_admission=True,
    )

    with pytest.raises(RuntimeError, match="requires a bound preconnect"):
        await owner.run(lifecycle.stop_event)

    assert lifecycle.transitions == []
    assert len(lifecycle.fatal_causes) == 1
    assert connector.urls == []
    assert factory.calls == []


@pytest.mark.asyncio
async def test_connector_entry_excludes_cross_thread_writer_lease_release(
    tmp_path: Path,
) -> None:
    lease = WriterLease.acquire(tmp_path)
    lifecycle = _InjectedLifecycleCoordinator()
    entered = asyncio.Event()
    allow_enter = asyncio.Event()
    connection = _BlockingEnterConnection(
        entered=entered,
        allow_enter=allow_enter,
        frames=_StopAfterOneFrame(lifecycle.stop_event),
    )
    connector = _BlockingEnterConnector(connection)
    factory = _InjectedFrameAdapterFactory()
    guard = _LeaseAdmissionGuard(lease)
    owner = PublicWebSocketCaptureOwner(
        _spot_plan(),
        plan_sha256=PLAN_SHA256,
        process_boot_id="boot-atomic-admission-thread",
        settings=_settings(),
        connector=cast(Any, connector),
        frame_adapter_factory=factory,
        lifecycle_coordinator=lifecycle,
        requires_preconnect_admission=True,
    )
    owner.bind_preconnect_admission_guard(guard)
    released = threading.Event()
    release_errors: list[BaseException] = []

    def release() -> None:
        try:
            lease.release()
        except BaseException as exc:
            release_errors.append(exc)
        finally:
            released.set()

    owner_task = asyncio.create_task(owner.run(lifecycle.stop_event))
    await asyncio.wait_for(entered.wait(), timeout=1)
    release_thread = threading.Thread(target=release)
    release_thread.start()
    await asyncio.sleep(0.05)
    assert not released.is_set()

    allow_enter.set()
    await asyncio.wait_for(owner_task, timeout=1)
    release_thread.join(timeout=1)

    assert released.is_set()
    assert release_errors == []
    assert connector.urls == [_spot_plan().url]
    assert guard.validations == 3


@pytest.mark.asyncio
async def test_connector_entry_rejects_same_event_loop_reentrant_release(
    tmp_path: Path,
) -> None:
    lease = WriterLease.acquire(tmp_path)
    lifecycle = _InjectedLifecycleCoordinator()
    entered = asyncio.Event()
    allow_enter = asyncio.Event()
    connection = _BlockingEnterConnection(
        entered=entered,
        allow_enter=allow_enter,
        frames=_StopAfterOneFrame(lifecycle.stop_event),
    )
    connector = _BlockingEnterConnector(connection)
    guard = _LeaseAdmissionGuard(lease)
    owner = PublicWebSocketCaptureOwner(
        _spot_plan(),
        plan_sha256=PLAN_SHA256,
        process_boot_id="boot-atomic-admission-loop",
        settings=_settings(),
        connector=cast(Any, connector),
        frame_adapter_factory=_InjectedFrameAdapterFactory(),
        lifecycle_coordinator=lifecycle,
        requires_preconnect_admission=True,
    )
    owner.bind_preconnect_admission_guard(guard)
    owner_task = asyncio.create_task(owner.run(lifecycle.stop_event))
    await asyncio.wait_for(entered.wait(), timeout=1)

    with pytest.raises(WriterLeaseError, match="active storage operation"):
        lease.release()

    lease.assert_held()
    allow_enter.set()
    await asyncio.wait_for(owner_task, timeout=1)
    lease.release()
    assert connector.urls == [_spot_plan().url]


@pytest.mark.asyncio
async def test_injected_consumer_failure_trips_the_injected_fatal_coordinator() -> None:
    lifecycle = _InjectedLifecycleCoordinator()
    bug = RuntimeError("synthetic injected consumer failure")
    adapter_factory = _InjectedFrameAdapterFactory(failure=bug)
    owner = PublicWebSocketCaptureOwner(
        _spot_plan(),
        plan_sha256=PLAN_SHA256,
        process_boot_id="boot-injected-fatal",
        settings=_settings(),
        connector=_Connector(_OneFrameUntilCancelled()),
        frame_adapter_factory=adapter_factory,
        lifecycle_coordinator=lifecycle,
    )

    with pytest.raises(RuntimeError) as captured:
        await owner.run(lifecycle.stop_event)

    assert captured.value is bug
    assert lifecycle.fatal_causes == [bug]
    assert lifecycle.failed
    assert not lifecycle.accepting
    assert lifecycle.stop_event.is_set()
    assert adapter_factory.adapters[0].frame_seq == 1
    assert [transition[3] for transition in lifecycle.transitions] == [
        ConnectionState.CONNECTING,
        ConnectionState.CONNECTED,
    ]


def test_each_default_v1_boundary_requires_its_existing_dependencies() -> None:
    lifecycle = _InjectedLifecycleCoordinator()
    adapter_factory = _InjectedFrameAdapterFactory()

    with pytest.raises(ValueError, match="default V1 lifecycle coordinator"):
        PublicWebSocketCaptureOwner(
            _spot_plan(),
            plan_sha256=PLAN_SHA256,
            process_boot_id="boot-missing-lifecycle-dependencies",
            settings=_settings(),
            connector=_Connector(),
            frame_adapter_factory=adapter_factory,
        )
    with pytest.raises(ValueError, match="default V1 frame adapter"):
        PublicWebSocketCaptureOwner(
            _spot_plan(),
            plan_sha256=PLAN_SHA256,
            process_boot_id="boot-missing-adapter-dependencies",
            settings=_settings(),
            connector=_Connector(),
            lifecycle_coordinator=lifecycle,
        )


def test_retained_depth_callbacks_require_exact_sync_pair_and_lineage() -> None:
    lifecycle = _InjectedLifecycleCoordinator()
    factory = _InjectedFrameAdapterFactory(
        session_id="session-retained-callback-config",
        protocol_hash=PROTOCOL_HASH,
    )

    with pytest.raises(ValueError, match="configured together"):
        PublicWebSocketCaptureOwner(
            _spot_plan(),
            plan_sha256=PLAN_SHA256,
            process_boot_id="boot-retained-callback-half-pair",
            settings=_settings(),
            connector=_Connector(),
            frame_adapter_factory=factory,
            lifecycle_coordinator=lifecycle,
            retained_depth_range_callback=lambda _receipt: None,
        )
    with pytest.raises(ValueError, match="session_id"):
        PublicWebSocketCaptureOwner(
            _spot_plan(),
            plan_sha256=PLAN_SHA256,
            process_boot_id="boot-retained-callback-missing-lineage",
            settings=_settings(),
            connector=_Connector(),
            frame_adapter_factory=_InjectedFrameAdapterFactory(),
            lifecycle_coordinator=_InjectedLifecycleCoordinator(),
            retained_depth_range_callback=lambda _receipt: None,
            retained_depth_resync_callback=lambda _receipt: None,
        )

    async def asynchronous_callback(_receipt: object) -> None:
        return None

    with pytest.raises(TypeError, match="must be synchronous"):
        PublicWebSocketCaptureOwner(
            _spot_plan(),
            plan_sha256=PLAN_SHA256,
            process_boot_id="boot-retained-callback-async",
            settings=_settings(),
            connector=_Connector(),
            frame_adapter_factory=factory,
            lifecycle_coordinator=_InjectedLifecycleCoordinator(),
            retained_depth_range_callback=asynchronous_callback,  # pyright: ignore[reportArgumentType]
            retained_depth_resync_callback=lambda _receipt: None,
        )

    def range_callback(_receipt: object) -> None:
        return None

    def resync_callback(_receipt: object) -> None:
        return None

    owner = PublicWebSocketCaptureOwner(
        _spot_plan(),
        plan_sha256=PLAN_SHA256,
        process_boot_id="boot-retained-callback-read-only",
        settings=_settings(),
        connector=_Connector(),
        frame_adapter_factory=factory,
        lifecycle_coordinator=_InjectedLifecycleCoordinator(),
        retained_depth_range_callback=range_callback,
        retained_depth_resync_callback=resync_callback,
    )
    assert owner.retained_depth_range_callback is range_callback
    assert owner.retained_depth_resync_callback is resync_callback
    with pytest.raises(AttributeError):
        owner.retained_depth_range_callback = None  # pyright: ignore[reportAttributeAccessIssue]


@pytest.mark.asyncio
@pytest.mark.parametrize("with_legacy_callbacks", [False, True])
async def test_retained_depth_callbacks_are_sealed_synchronous_and_range_first(
    with_legacy_callbacks: bool,
) -> None:
    public_plan = _futures_public_depth_plan()
    offerer = _R4BRecordingOfferer()
    recovery_lifecycle = _R4BPassThroughRecoveryLifecycle()
    exact_factory = PublicWebSocketFrameAdapterFactoryV2(
        public_plan,
        session_id="session-retained-depth-owner",
        protocol_hash=PROTOCOL_HASH,
        clock=_Clock(),
        ingress=SharedWebSocketIngressV2(
            offerer,
            recovered_wal_tail_ingest_seq=0,
        ),
        recovery_lifecycle=recovery_lifecycle,
    )
    factory = _RetainedDepthFrameAdapterFactory(exact_factory)
    lifecycle = _InjectedLifecycleCoordinator()
    callback_order: list[str] = []
    range_receipts: list[PublicRetainedDepthRangeCallbackReceiptV2] = []
    resync_receipts: list[PublicRetainedDepthResyncCallbackReceiptV2] = []

    def on_retained_range(
        receipt: PublicRetainedDepthRangeCallbackReceiptV2,
    ) -> None:
        validate_public_retained_depth_range_callback_receipt_v2(receipt)
        assert factory.adapters[0].frame_seq == 1
        assert factory.adapters[0].last_admitted_raw_record_v2 is offerer.records[-1]
        callback_order.append("retained-range")
        range_receipts.append(receipt)

    def on_retained_resync(
        receipt: PublicRetainedDepthResyncCallbackReceiptV2,
    ) -> None:
        validate_public_retained_depth_resync_callback_receipt_v2(receipt)
        callback_order.append("retained-resync")
        resync_receipts.append(receipt)
        lifecycle.stop_event.set()

    def on_legacy_range(_observation: DepthRangeObservation) -> None:
        callback_order.append("legacy-range")

    def on_legacy_resync(_request: DepthResyncRequest) -> None:
        callback_order.append("legacy-resync")

    owner = PublicWebSocketCaptureOwner(
        exact_factory.owner_plan,
        plan_sha256=PLAN_SHA256,
        process_boot_id="boot-retained-depth-owner",
        settings=_settings(),
        connector=_Connector(_FuturesPublicDepthUntilCancelled()),
        frame_adapter_factory=factory,
        lifecycle_coordinator=lifecycle,
        depth_range_callback=on_legacy_range if with_legacy_callbacks else None,
        depth_resync_callback=on_legacy_resync if with_legacy_callbacks else None,
        retained_depth_range_callback=on_retained_range,
        retained_depth_resync_callback=on_retained_resync,
    )

    await owner.run(lifecycle.stop_event)

    assert lifecycle.failed is False
    assert recovery_lifecycle.fatal_causes == []
    assert callback_order == (
        [
            "legacy-range",
            "retained-range",
            "legacy-resync",
            "retained-resync",
        ]
        if with_legacy_callbacks
        else ["retained-range", "retained-resync"]
    )
    assert len(range_receipts) == len(resync_receipts) == 1
    range_receipt = range_receipts[0]
    resync_receipt = resync_receipts[0]
    assert range_receipt.session_id == "session-retained-depth-owner"
    assert range_receipt.protocol_hash == PROTOCOL_HASH
    assert range_receipt.market is Market.FUTURES
    assert range_receipt.route == "public"
    assert range_receipt.connection_id == f"{public_plan.name}-g000001"
    assert range_receipt.generation == range_receipt.frame_seq == 1
    assert range_receipt.ingest_seq == 1
    assert (
        range_receipt.raw_payload_sha256
        == hashlib.sha256(_FuturesPublicDepthUntilCancelled.raw.encode()).hexdigest()
    )
    assert range_receipt.receipt_wall_ms == 1_720_000_000_001
    assert range_receipt.receipt_monotonic_ns == 10_001
    assert range_receipt.observation == DepthRangeObservation(
        market=Market.FUTURES,
        symbol="BTCUSDT",
        generation=1,
        U=10,
        u=12,
        reset=True,
    )
    assert resync_receipt.request == DepthResyncRequest(
        event="startup",
        market=Market.FUTURES,
        generation=1,
        watermarks=(("BTCUSDT", 10),),
    )
    assert (
        resync_receipt.connection_id,
        resync_receipt.frame_seq,
        resync_receipt.ingest_seq,
        resync_receipt.raw_payload_sha256,
    ) == (
        range_receipt.connection_id,
        range_receipt.frame_seq,
        range_receipt.ingest_seq,
        range_receipt.raw_payload_sha256,
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("defect", ["legacy_mutation", "cancelled_error"])
async def test_retained_depth_callback_defects_fail_closed(
    defect: str,
) -> None:
    public_plan = _futures_public_depth_plan()
    offerer = _R4BRecordingOfferer()
    exact_factory = PublicWebSocketFrameAdapterFactoryV2(
        public_plan,
        session_id=f"session-retained-defect-{defect}",
        protocol_hash=PROTOCOL_HASH,
        clock=_Clock(),
        ingress=SharedWebSocketIngressV2(
            offerer,
            recovered_wal_tail_ingest_seq=0,
        ),
        recovery_lifecycle=_R4BPassThroughRecoveryLifecycle(),
    )
    lifecycle = _InjectedLifecycleCoordinator()
    retained_callbacks: list[object] = []

    def retained_range(receipt: object) -> None:
        if defect == "cancelled_error":
            raise asyncio.CancelledError
        retained_callbacks.append(receipt)

    def legacy_range(observation: DepthRangeObservation) -> None:
        if defect == "legacy_mutation":
            object.__setattr__(observation, "U", observation.U + 1)

    owner = PublicWebSocketCaptureOwner(
        exact_factory.owner_plan,
        plan_sha256=PLAN_SHA256,
        process_boot_id=f"boot-retained-defect-{defect}",
        settings=_settings(),
        connector=_Connector(_FuturesPublicDepthUntilCancelled()),
        frame_adapter_factory=_RetainedDepthFrameAdapterFactory(exact_factory),
        lifecycle_coordinator=lifecycle,
        depth_range_callback=(legacy_range if defect == "legacy_mutation" else None),
        depth_resync_callback=((lambda _request: None) if defect == "legacy_mutation" else None),
        retained_depth_range_callback=retained_range,
        retained_depth_resync_callback=retained_callbacks.append,
    )

    with pytest.raises((RuntimeError, ValueError)) as captured:
        await owner.run(lifecycle.stop_event)

    if defect == "legacy_mutation":
        assert "material was mutated" in str(captured.value)
    else:
        assert "raised CancelledError" in str(captured.value)
    assert retained_callbacks == []
    assert lifecycle.fatal_causes == [captured.value]
    assert lifecycle.failed is True


@pytest.mark.asyncio
async def test_retained_depth_foreign_adapter_fails_closed_before_callbacks() -> None:
    lifecycle = _InjectedLifecycleCoordinator()
    factory = _InjectedFrameAdapterFactory(
        session_id="session-foreign-retained-adapter",
        protocol_hash=PROTOCOL_HASH,
    )
    callback_receipts: list[object] = []
    owner = PublicWebSocketCaptureOwner(
        _spot_plan(),
        plan_sha256=PLAN_SHA256,
        process_boot_id="boot-foreign-retained-adapter",
        settings=_settings(),
        connector=_Connector(_SpotBaselineUntilCancelled()),
        frame_adapter_factory=factory,
        lifecycle_coordinator=lifecycle,
        retained_depth_range_callback=callback_receipts.append,
        retained_depth_resync_callback=callback_receipts.append,
    )

    with pytest.raises(TypeError, match="exact V2 frame adapter") as captured:
        await owner.run(lifecycle.stop_event)

    assert callback_receipts == []
    assert lifecycle.fatal_causes == [captured.value]
    assert lifecycle.failed is True
    assert lifecycle.stop_event.is_set()


@pytest.mark.asyncio
async def test_unproductive_reconnects_are_capped_and_fail_visible() -> None:
    pipeline, fatal, writer = _pipeline()
    connector = _Connector(OSError("first"), TimeoutError("second"))
    pipeline.start()
    owner = PublicWebSocketCaptureOwner(
        _spot_plan(),
        plan_sha256=PLAN_SHA256,
        process_boot_id="boot-2",
        pipeline=pipeline,
        clock=_Clock(),
        sequencer=IngestSequencer(),
        settings=_settings(),
        connector=connector,
    )

    with pytest.raises(CaptureReconnectExhausted, match="budget exhausted"):
        await owner.run(fatal.stop_event)
    with pytest.raises(CaptureReconnectExhausted, match="budget exhausted"):
        await pipeline.stop()

    assert fatal.failed is True
    assert len(connector.urls) == 2
    transitions = [
        record for record in writer.records if isinstance(record, ConnectionTransitionV1)
    ]
    assert [transition.state for transition in transitions] == [
        ConnectionState.CONNECTING,
        ConnectionState.DISCONNECTED,
        ConnectionState.CONNECTING,
        ConnectionState.DISCONNECTED,
    ]
    assert {transition.reason for transition in transitions} <= {
        "connect_attempt",
        "connection_failure",
    }
    assert "first" not in repr(writer.records)
    assert "second" not in repr(writer.records)


@pytest.mark.asyncio
async def test_lifetime_rotation_opens_a_new_generation_without_spending_retry_budget() -> None:
    pipeline, fatal, writer = _pipeline()
    connector = _Connector(
        _SpotBaselineUntilCancelled(),
        _SpotBaselineUntilCancelled(fatal.stop_event),
    )
    pipeline.start()
    depth_events: list[DepthResyncRequest] = []
    depth_ranges: list[DepthRangeObservation] = []
    owner = PublicWebSocketCaptureOwner(
        _spot_plan(),
        plan_sha256=PLAN_SHA256,
        process_boot_id="boot-3",
        pipeline=pipeline,
        clock=_Clock(),
        sequencer=IngestSequencer(),
        settings=_settings(
            maximum_connection_age_seconds=0.01,
            healthy_reset_seconds=0.005,
        ),
        connector=connector,
        depth_resync_callback=depth_events.append,
        depth_range_callback=depth_ranges.append,
    )

    await owner.run(fatal.stop_event)
    await pipeline.stop()

    transitions = [
        record for record in writer.records if isinstance(record, ConnectionTransitionV1)
    ]
    assert len(connector.urls) == 2
    assert any(transition.state is ConnectionState.RECYCLED for transition in transitions)
    assert {transition.connection_id for transition in transitions} == {
        "capture-spot-1-g000001",
        "capture-spot-1-g000002",
    }
    assert depth_events == [
        DepthResyncRequest(
            event="startup",
            market=Market.SPOT,
            generation=1,
            watermarks=(
                ("BTCUSDT", 10),
                ("ETHUSDT", 20),
                ("SOLUSDT", 30),
            ),
        ),
        DepthResyncRequest(
            event="reconnect",
            market=Market.SPOT,
            generation=2,
            watermarks=(
                ("BTCUSDT", 10),
                ("ETHUSDT", 20),
                ("SOLUSDT", 30),
            ),
        ),
    ]
    assert [observation.generation for observation in depth_ranges] == [1, 1, 1, 2, 2, 2]
    assert all(observation.reset for observation in depth_ranges)


@pytest.mark.asyncio
async def test_reconnect_callback_runs_only_after_depth_consumer_started() -> None:
    pipeline, fatal, _writer = _pipeline()
    first = _SpotBaselineUntilCancelled()
    second = _SpotBaselineUntilCancelled(fatal.stop_event)
    connector = _Connector(first, second)
    callback_events: list[DepthResyncRequest] = []
    range_events: list[DepthRangeObservation] = []
    pipeline.start()

    def on_resync(request: DepthResyncRequest) -> None:
        assert first.yielded_symbols == ["BTCUSDT", "ETHUSDT", "SOLUSDT"]
        callback_events.append(request)
        if request.event == "reconnect":
            assert second.yielded_symbols == ["BTCUSDT", "ETHUSDT", "SOLUSDT"]

    owner = PublicWebSocketCaptureOwner(
        _spot_plan(),
        plan_sha256=PLAN_SHA256,
        process_boot_id="boot-reconnect-order",
        pipeline=pipeline,
        clock=_Clock(),
        sequencer=IngestSequencer(),
        settings=_settings(
            maximum_connection_age_seconds=0.01,
            healthy_reset_seconds=0.005,
        ),
        connector=connector,
        depth_resync_callback=on_resync,
        depth_range_callback=range_events.append,
    )

    await owner.run(fatal.stop_event)
    await pipeline.stop()

    assert callback_events == [
        DepthResyncRequest(
            event="startup",
            market=Market.SPOT,
            generation=1,
            watermarks=(
                ("BTCUSDT", 10),
                ("ETHUSDT", 20),
                ("SOLUSDT", 30),
            ),
        ),
        DepthResyncRequest(
            event="reconnect",
            market=Market.SPOT,
            generation=2,
            watermarks=(
                ("BTCUSDT", 10),
                ("ETHUSDT", 20),
                ("SOLUSDT", 30),
            ),
        ),
    ]
    assert len(range_events) == 6


@pytest.mark.asyncio
async def test_depth_reconnect_without_scheduler_callback_fails_closed() -> None:
    pipeline, fatal, _writer = _pipeline()
    connector = _Connector(_SpotBaselineUntilCancelled())
    pipeline.start()
    owner = PublicWebSocketCaptureOwner(
        _spot_plan(),
        plan_sha256=PLAN_SHA256,
        process_boot_id="boot-missing-depth-callback",
        pipeline=pipeline,
        clock=_Clock(),
        sequencer=IngestSequencer(),
        settings=_settings(),
        connector=connector,
    )

    with pytest.raises(DepthResyncUnavailable):
        await owner.run(fatal.stop_event)
    assert fatal.failure is not None
    assert isinstance(fatal.failure.cause, DepthResyncUnavailable)
    with pytest.raises(DepthResyncUnavailable):
        await pipeline.stop()


@pytest.mark.asyncio
async def test_startup_callback_runs_after_every_baseline_frame_offer() -> None:
    trace = _TracePipeline()
    pipeline = cast(CapturePipeline, trace)
    fatal = trace.fatal_state
    callback_frames: list[CaptureEnvelopeV1] = []
    range_observations: list[DepthRangeObservation] = []

    def on_range(observation: DepthRangeObservation) -> None:
        offered_frames = [
            record for record in trace.offered if isinstance(record, CaptureEnvelopeV1)
        ]
        assert offered_frames[-1].frame_seq == len(range_observations) + 1
        range_observations.append(observation)

    def on_resync(request: DepthResyncRequest) -> None:
        assert request.event == "startup"
        assert request.market is Market.SPOT
        assert request.generation == 1
        assert len(range_observations) == 3
        assert request.watermarks == (
            ("BTCUSDT", 10),
            ("ETHUSDT", 20),
            ("SOLUSDT", 30),
        )
        callback_frames.extend(
            record for record in trace.offered if isinstance(record, CaptureEnvelopeV1)
        )
        fatal.stop_event.set()

    owner = PublicWebSocketCaptureOwner(
        _spot_plan(),
        plan_sha256=PLAN_SHA256,
        process_boot_id="boot-baseline-offer-order",
        pipeline=pipeline,
        clock=_Clock(),
        sequencer=IngestSequencer(),
        settings=_settings(),
        connector=_Connector(_SpotBaselineUntilCancelled()),
        depth_resync_callback=on_resync,
        depth_range_callback=on_range,
    )

    await owner.run(fatal.stop_event)

    assert [frame.frame_seq for frame in callback_frames] == [1, 2, 3]
    assert [json.loads(frame.raw_payload)["stream"] for frame in callback_frames] == [
        "btcusdt@depth@100ms",
        "ethusdt@depth@100ms",
        "solusdt@depth@100ms",
    ]


@pytest.mark.asyncio
async def test_sequence_gap_callback_runs_after_gap_frame_offer() -> None:
    trace = _TracePipeline()
    pipeline = cast(CapturePipeline, trace)
    fatal = trace.fatal_state
    offered_at_callback: list[CaptureRecord] = []
    callback_order: list[str] = []

    def on_range(observation: DepthRangeObservation) -> None:
        callback_order.append(f"range:{observation.U}:{observation.reset}")

    def on_resync(request: DepthResyncRequest) -> None:
        if request.event == "startup":
            return
        assert request.event == "sequence_gap"
        assert request.market is Market.SPOT
        assert request.generation == 1
        assert request.watermarks == (("BTCUSDT", 14),)
        callback_order.append("resync")
        offered_at_callback.extend(trace.offered)
        fatal.stop_event.set()

    owner = PublicWebSocketCaptureOwner(
        _spot_plan(),
        plan_sha256=PLAN_SHA256,
        process_boot_id="boot-gap-order",
        pipeline=pipeline,
        clock=_Clock(),
        sequencer=IngestSequencer(),
        settings=_settings(),
        connector=_Connector(_SpotGapUntilCancelled()),
        depth_resync_callback=on_resync,
        depth_range_callback=on_range,
    )

    await owner.run(fatal.stop_event)

    assert isinstance(offered_at_callback[-1], CaptureEnvelopeV1)
    assert offered_at_callback[-1].frame_seq == 2
    envelopes = [record for record in trace.offered if isinstance(record, CaptureEnvelopeV1)]
    assert [envelope.frame_seq for envelope in envelopes] == [1, 2]
    assert callback_order == ["range:10:True", "range:14:True", "resync"]


@pytest.mark.asyncio
async def test_malformed_combined_frame_is_preserved_then_fails_owner_closed() -> None:
    pipeline, fatal, writer = _pipeline()
    pipeline.start()
    owner = PublicWebSocketCaptureOwner(
        _spot_plan(),
        plan_sha256=PLAN_SHA256,
        process_boot_id="boot-malformed-depth",
        pipeline=pipeline,
        clock=_Clock(),
        sequencer=IngestSequencer(),
        settings=_settings(),
        connector=_Connector(_MalformedUntilCancelled()),
    )

    with pytest.raises(DepthSequenceError) as captured:
        await owner.run(fatal.stop_event)
    assert fatal.failure is not None and fatal.failure.cause is captured.value
    with pytest.raises(DepthSequenceError) as stopped:
        await pipeline.stop()
    assert stopped.value is captured.value
    envelopes = [record for record in writer.records if isinstance(record, CaptureEnvelopeV1)]
    assert len(envelopes) == 1
    assert envelopes[0].raw_payload == "not-json"


@pytest.mark.asyncio
async def test_unexpected_connector_error_preserves_original_and_does_not_retry() -> None:
    pipeline, fatal, _writer = _pipeline()
    bug = AssertionError("programming bug")
    connector = _Connector(bug, OSError("must not be reached"))
    pipeline.start()
    owner = PublicWebSocketCaptureOwner(
        _spot_plan(),
        plan_sha256=PLAN_SHA256,
        process_boot_id="boot-unexpected",
        pipeline=pipeline,
        clock=_Clock(),
        sequencer=IngestSequencer(),
        settings=_settings(),
        connector=connector,
    )

    with pytest.raises(AssertionError) as captured:
        await owner.run(fatal.stop_event)
    assert captured.value is bug
    assert fatal.failure is not None and fatal.failure.cause is bug
    assert len(connector.urls) == 1
    with pytest.raises(AssertionError) as stopped:
        await pipeline.stop()
    assert stopped.value is bug


@pytest.mark.asyncio
async def test_owner_rejects_a_stop_event_outside_shared_fatal_state() -> None:
    pipeline, fatal, _writer = _pipeline()
    owner = PublicWebSocketCaptureOwner(
        _spot_plan(),
        plan_sha256=PLAN_SHA256,
        process_boot_id="boot-split-stop",
        pipeline=pipeline,
        clock=_Clock(),
        sequencer=IngestSequencer(),
        settings=_settings(),
        connector=_Connector(),
    )

    with pytest.raises(ValueError, match="shared pipeline fatal stop event"):
        await owner.run(asyncio.Event())
    assert fatal.failed is False


@pytest.mark.parametrize(
    "overrides",
    [
        {"maximum_reconnect_attempts": 0, "reconnect_delays_seconds": ()},
        {"maximum_reconnect_attempts": 2, "reconnect_delays_seconds": (0.0,)},
        {"maximum_frame_bytes": 0},
        {"healthy_reset_seconds": 2.0},
        {"reconnect_delays_seconds": (0.0, -1.0)},
        {"maximum_connection_age_seconds": float("inf")},
        {"connect_timeout_seconds": float("nan")},
        {"heartbeat_interval_seconds": True},
        {"internal_queue_frames": True},
        {"maximum_frame_bytes": 16 * 1024 * 1024 + 1},
        {"maximum_reconnect_attempts": 33, "reconnect_delays_seconds": (0.0,) * 33},
        {"reconnect_delays_seconds": (0.0, 301.0)},
    ],
)
def test_owner_settings_reject_unbounded_or_incoherent_values(
    overrides: dict[str, object],
) -> None:
    with pytest.raises(ValueError):
        _settings(**overrides)


def test_owner_rejects_private_or_unknown_plan_before_connector_use() -> None:
    pipeline, _fatal, _writer = _pipeline()
    private = WebSocketPlan(
        name="private",
        market=Market.FUTURES,
        route="private",
        streams=("btcusdt@aggTrade",),
        url="wss://fstream.binance.com/private/stream?streams=btcusdt@aggTrade",
    )

    with pytest.raises(ValueError, match="not a public"):
        PublicWebSocketCaptureOwner(
            private,
            plan_sha256=PLAN_SHA256,
            process_boot_id="boot-4",
            pipeline=pipeline,
            clock=_Clock(),
            sequencer=IngestSequencer(),
            settings=_settings(),
            connector=_Connector(),
        )


def test_production_connector_is_bounded_and_disables_compression(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, object] = {}
    sentinel = _Connection(_OneFrameUntilCancelled())

    def fake_connect(url: str, **kwargs: object) -> object:
        captured["url"] = url
        captured.update(kwargs)
        return sentinel

    monkeypatch.setattr(owner_module, "connect", fake_connect)
    settings = _settings()
    connector = WebsocketsPublicConnector(settings)

    assert connector(_spot_plan().url) is sentinel
    assert captured["open_timeout"] == settings.connect_timeout_seconds
    assert captured["close_timeout"] == settings.close_timeout_seconds
    assert captured["ping_interval"] == settings.heartbeat_interval_seconds
    assert captured["ping_timeout"] == settings.pong_timeout_seconds
    assert captured["max_queue"] == settings.internal_queue_frames
    assert captured["max_size"] == settings.maximum_frame_bytes
    assert captured["compression"] is None
    assert captured["proxy"] is None
