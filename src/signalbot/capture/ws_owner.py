from __future__ import annotations

import asyncio
import contextlib
import inspect
import math
from collections.abc import AsyncIterable, AsyncIterator, Callable
from contextlib import AbstractAsyncContextManager, AbstractContextManager, AsyncExitStack
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Protocol, cast

from websockets.asyncio.client import connect
from websockets.exceptions import ConnectionClosed, InvalidHandshake

from signalbot.capture.depth_sequence import (
    DepthRangeCallback,
    DepthRangeObservation,
    DepthResyncCallback,
    DepthResyncRequest,
    DepthResyncUnavailable,
    DepthSequenceError,
    RawDepthContinuityMonitor,
)
from signalbot.capture.models import ConnectionState, ConnectionTransitionV1
from signalbot.capture.pipeline import CapturePipeline
from signalbot.capture.receipts import IngestSequencer, ReceiptClock
from signalbot.capture.websocket import (
    PublicWebSocketCaptureAdapter,
    validate_public_websocket_plan,
)
from signalbot.domain.enums import Market
from signalbot.exchange.binance.endpoints import WebSocketPlan

if TYPE_CHECKING:
    from signalbot.r4b_v2.capture.websocket import (
        PublicRetainedDepthRangeCallbackReceiptV2,
        PublicRetainedDepthResyncCallbackReceiptV2,
    )

FrameStream = AsyncIterable[str | bytes]
ConnectionContext = AbstractAsyncContextManager[FrameStream]
Connector = Callable[[str], ConnectionContext]
_WEBSOCKET_PRECONNECTING_GENERATION_CONTEXT_FACTORY_TOKEN = object()


class WebSocketFrameConsumer(Protocol):
    """Consume one generation and expose its synchronously accepted frame tail.

    Implementations must not request the next iterator item until the current
    frame's capture offer has returned. The owner resumes its depth observer at
    that exact iterator boundary.
    """

    @property
    def frame_seq(self) -> int: ...

    async def consume(self, frames: FrameStream) -> None: ...


class WebSocketFrameAdapterFactory(Protocol):
    """Build the frame consumer for one owner-assigned connection generation."""

    def __call__(
        self,
        *,
        connection_id: str,
        generation: int,
    ) -> WebSocketFrameConsumer: ...


class WebSocketLifecycleFatalCoordinator(Protocol):
    """Coordinate capture lifecycle evidence and first-failure-wins shutdown."""

    @property
    def stop_event(self) -> asyncio.Event: ...

    @property
    def failed(self) -> bool: ...

    @property
    def accepting(self) -> bool: ...

    def record_transition(
        self,
        connection_id: str,
        *,
        generation: int,
        last_frame_seq: int,
        state: ConnectionState,
        reason: str,
    ) -> None: ...

    def trip_fatal(self, cause: BaseException) -> None: ...


class WebSocketPreconnectAdmissionGuard(Protocol):
    """Revalidate a sealed composition immediately before every connector call."""

    def validate_current(self) -> None: ...

    def connector_admission_guard(self) -> AbstractContextManager[None]: ...


@dataclass(frozen=True, slots=True, init=False)
class WebSocketPreconnectingGenerationContext:
    """Factory-sealed owner lineage minted immediately before its exact hook.

    This one-shot process-local capability proves only the pre-CONNECTING owner
    handoff. It intentionally carries no symbol, depth watermark, bridge, M2,
    promotion, strategy, alert, or execution claim.
    """

    session_id: str | None
    protocol_hash: str | None
    market: Market
    route: str
    connection_id: str
    generation: int
    _factory_seal: object = field(init=False, repr=False, compare=False)
    _owner_capability: object = field(init=False, repr=False, compare=False)
    _hook_identity: object = field(init=False, repr=False, compare=False)
    _owner_plan: WebSocketPlan = field(init=False, repr=False, compare=False)
    _material_seal: tuple[object, ...] = field(
        init=False,
        repr=False,
        compare=False,
    )

    def __init__(
        self,
        *,
        session_id: str | None,
        protocol_hash: str | None,
        market: Market,
        route: str,
        connection_id: str,
        generation: int,
        _factory_token: object | None = None,
        _owner_capability: object | None = None,
        _hook_identity: object | None = None,
        _owner_plan: WebSocketPlan | None = None,
    ) -> None:
        if _factory_token is not _WEBSOCKET_PRECONNECTING_GENERATION_CONTEXT_FACTORY_TOKEN:
            raise TypeError(
                "WebSocketPreconnectingGenerationContext can only be minted by its exact owner"
            )
        if type(_owner_capability) is not object:
            raise TypeError("preconnecting generation context lacks owner capability")
        if not callable(_hook_identity):
            raise TypeError("preconnecting generation context lacks hook identity")
        if type(_owner_plan) is not WebSocketPlan:
            raise TypeError("preconnecting generation context requires exact owner plan")
        object.__setattr__(self, "session_id", session_id)
        object.__setattr__(self, "protocol_hash", protocol_hash)
        object.__setattr__(self, "market", market)
        object.__setattr__(self, "route", route)
        object.__setattr__(self, "connection_id", connection_id)
        object.__setattr__(self, "generation", generation)
        object.__setattr__(
            self,
            "_factory_seal",
            _WEBSOCKET_PRECONNECTING_GENERATION_CONTEXT_FACTORY_TOKEN,
        )
        object.__setattr__(self, "_owner_capability", _owner_capability)
        object.__setattr__(self, "_hook_identity", _hook_identity)
        object.__setattr__(self, "_owner_plan", _owner_plan)
        _validate_websocket_preconnecting_generation_context_material(self)
        object.__setattr__(
            self,
            "_material_seal",
            _websocket_preconnecting_generation_context_material(self),
        )


class WebSocketPreconnectingGenerationHook(Protocol):
    """Await one fail-closed generation handoff before any connect side effect."""

    async def __call__(
        self,
        context: WebSocketPreconnectingGenerationContext,
        /,
    ) -> None: ...


class RetainedDepthRangeCallbackV2(Protocol):
    """Synchronously accept one factory-sealed retained range source receipt."""

    def __call__(
        self,
        receipt: PublicRetainedDepthRangeCallbackReceiptV2,
        /,
    ) -> None: ...


class RetainedDepthResyncCallbackV2(Protocol):
    """Synchronously accept one factory-sealed retained resync source receipt."""

    def __call__(
        self,
        receipt: PublicRetainedDepthResyncCallbackReceiptV2,
        /,
    ) -> None: ...


@dataclass(slots=True)
class _RetainedDepthCallbackContextV2:
    adapter: WebSocketFrameConsumer
    connection_id: str
    generation: int
    raw: str | bytes
    range_receipt: PublicRetainedDepthRangeCallbackReceiptV2 | None = None


_MAXIMUM_CONNECTION_AGE_SECONDS = 86_400.0
_MAXIMUM_TIMEOUT_SECONDS = 60.0
_MAXIMUM_HEARTBEAT_INTERVAL_SECONDS = 300.0
_MAXIMUM_HEALTHY_RESET_SECONDS = 3_600.0
_MAXIMUM_INTERNAL_QUEUE_FRAMES = 65_536
_MAXIMUM_FRAME_BYTES = 16 * 1024 * 1024
_MAXIMUM_RECONNECT_ATTEMPTS = 32
_MAXIMUM_RECONNECT_DELAY_SECONDS = 300.0
_RECONNECTABLE_TRANSPORT_ERRORS = (
    OSError,
    TimeoutError,
    ConnectionClosed,
    InvalidHandshake,
)


@dataclass(frozen=True, slots=True)
class WebSocketOwnerSettings:
    maximum_connection_age_seconds: float
    connect_timeout_seconds: float
    close_timeout_seconds: float
    heartbeat_interval_seconds: float
    pong_timeout_seconds: float
    internal_queue_frames: int
    maximum_frame_bytes: int
    maximum_reconnect_attempts: int
    reconnect_delays_seconds: tuple[float, ...]
    healthy_reset_seconds: float = 60.0

    def __post_init__(self) -> None:
        positive_numbers = (
            ("maximum_connection_age_seconds", self.maximum_connection_age_seconds),
            ("connect_timeout_seconds", self.connect_timeout_seconds),
            ("close_timeout_seconds", self.close_timeout_seconds),
            ("heartbeat_interval_seconds", self.heartbeat_interval_seconds),
            ("pong_timeout_seconds", self.pong_timeout_seconds),
            ("healthy_reset_seconds", self.healthy_reset_seconds),
        )
        for name, value in positive_numbers:
            _require_finite_positive(value, name)
        upper_bounds = (
            (
                "maximum_connection_age_seconds",
                self.maximum_connection_age_seconds,
                _MAXIMUM_CONNECTION_AGE_SECONDS,
            ),
            ("connect_timeout_seconds", self.connect_timeout_seconds, _MAXIMUM_TIMEOUT_SECONDS),
            ("close_timeout_seconds", self.close_timeout_seconds, _MAXIMUM_TIMEOUT_SECONDS),
            (
                "heartbeat_interval_seconds",
                self.heartbeat_interval_seconds,
                _MAXIMUM_HEARTBEAT_INTERVAL_SECONDS,
            ),
            ("pong_timeout_seconds", self.pong_timeout_seconds, _MAXIMUM_TIMEOUT_SECONDS),
            (
                "healthy_reset_seconds",
                self.healthy_reset_seconds,
                _MAXIMUM_HEALTHY_RESET_SECONDS,
            ),
        )
        for name, value, maximum in upper_bounds:
            if value > maximum:
                raise ValueError(f"{name} exceeds its operational upper bound")
        if self.healthy_reset_seconds > self.maximum_connection_age_seconds:
            raise ValueError("healthy reset cannot exceed maximum connection age")
        _require_bounded_integer(
            self.internal_queue_frames,
            "internal_queue_frames",
            maximum=_MAXIMUM_INTERNAL_QUEUE_FRAMES,
        )
        _require_bounded_integer(
            self.maximum_frame_bytes,
            "maximum_frame_bytes",
            maximum=_MAXIMUM_FRAME_BYTES,
        )
        _require_bounded_integer(
            self.maximum_reconnect_attempts,
            "maximum_reconnect_attempts",
            maximum=_MAXIMUM_RECONNECT_ATTEMPTS,
        )
        if not isinstance(self.reconnect_delays_seconds, tuple):
            raise ValueError("reconnect_delays_seconds must be an immutable tuple")
        if len(self.reconnect_delays_seconds) != self.maximum_reconnect_attempts:
            raise ValueError("one bounded reconnect delay is required per attempt")
        for delay in self.reconnect_delays_seconds:
            if (
                isinstance(delay, bool)
                or not isinstance(delay, (int, float))
                or not math.isfinite(float(delay))
                or delay < 0
                or delay > _MAXIMUM_RECONNECT_DELAY_SECONDS
            ):
                raise ValueError("reconnect delays must be finite and operationally bounded")


class CaptureReconnectExhausted(RuntimeError):
    """Raised after the fixed consecutive unproductive reconnect budget is spent."""


class PreconnectingGenerationHookDrainTimeout(RuntimeError):
    """Raised when an owned generation hook refuses bounded cancellation."""


class WebsocketsPublicConnector:
    """Construct bounded, unauthenticated WebSocket contexts for validated plans."""

    def __init__(self, settings: WebSocketOwnerSettings) -> None:
        self.settings = settings

    def __call__(self, url: str) -> ConnectionContext:
        connection = connect(
            url,
            open_timeout=self.settings.connect_timeout_seconds,
            close_timeout=self.settings.close_timeout_seconds,
            ping_interval=self.settings.heartbeat_interval_seconds,
            ping_timeout=self.settings.pong_timeout_seconds,
            max_queue=self.settings.internal_queue_frames,
            max_size=self.settings.maximum_frame_bytes,
            compression=None,
            proxy=None,
        )
        return cast(ConnectionContext, connection)


@dataclass(frozen=True, slots=True)
class _V1WebSocketLifecycleFatalCoordinator:
    plan: WebSocketPlan
    plan_sha256: str
    process_boot_id: str
    pipeline: CapturePipeline
    clock: ReceiptClock
    sequencer: IngestSequencer

    @property
    def stop_event(self) -> asyncio.Event:
        return self.pipeline.fatal_state.stop_event

    @property
    def failed(self) -> bool:
        return self.pipeline.fatal_state.failed

    @property
    def accepting(self) -> bool:
        return self.pipeline.handoff.accepting

    def record_transition(
        self,
        connection_id: str,
        *,
        generation: int,
        last_frame_seq: int,
        state: ConnectionState,
        reason: str,
    ) -> None:
        del generation
        receipt = self.clock.capture()
        transition = ConnectionTransitionV1(
            received_at_ms=receipt.received_at_ms,
            received_monotonic_ns=receipt.received_monotonic_ns,
            plan_sha256=self.plan_sha256,
            process_boot_id=self.process_boot_id,
            connection_id=connection_id,
            ingest_seq=self.sequencer.next(),
            last_frame_seq=last_frame_seq,
            market=self.plan.market,
            route=self.plan.route,
            streams=self.plan.streams,
            state=state,
            reason=reason,
        )
        self.pipeline.offer(transition)

    def trip_fatal(self, cause: BaseException) -> None:
        self.pipeline.fatal_state.trip_unbound(cause)


class PublicWebSocketCaptureOwner:
    """Own one public plan with capped reconnects and auditable transitions."""

    def __init__(
        self,
        plan: WebSocketPlan,
        *,
        plan_sha256: str,
        process_boot_id: str,
        pipeline: CapturePipeline | None = None,
        clock: ReceiptClock | None = None,
        sequencer: IngestSequencer | None = None,
        settings: WebSocketOwnerSettings,
        connector: Connector | None = None,
        depth_resync_callback: DepthResyncCallback | None = None,
        depth_range_callback: DepthRangeCallback | None = None,
        retained_depth_resync_callback: RetainedDepthResyncCallbackV2 | None = None,
        retained_depth_range_callback: RetainedDepthRangeCallbackV2 | None = None,
        frame_adapter_factory: WebSocketFrameAdapterFactory | None = None,
        lifecycle_coordinator: WebSocketLifecycleFatalCoordinator | None = None,
        requires_preconnect_admission: bool = False,
        preconnecting_generation_hook: (WebSocketPreconnectingGenerationHook | None) = None,
    ) -> None:
        validate_public_websocket_plan(plan)
        if not process_boot_id:
            raise ValueError("process_boot_id must be non-empty")
        self.plan = plan
        self.plan_sha256 = plan_sha256
        self.process_boot_id = process_boot_id
        self.pipeline = pipeline
        self.clock = clock
        self.sequencer = sequencer
        self.settings = settings
        self.connector = connector or WebsocketsPublicConnector(settings)
        if lifecycle_coordinator is None:
            if pipeline is None or clock is None or sequencer is None:
                raise ValueError(
                    "default V1 lifecycle coordinator requires pipeline, clock, and sequencer"
                )
            lifecycle_coordinator = _V1WebSocketLifecycleFatalCoordinator(
                plan=plan,
                plan_sha256=plan_sha256,
                process_boot_id=process_boot_id,
                pipeline=pipeline,
                clock=clock,
                sequencer=sequencer,
            )
        self.lifecycle_coordinator = lifecycle_coordinator
        if type(requires_preconnect_admission) is not bool:
            raise TypeError("requires_preconnect_admission must be a boolean")
        self._requires_exact_v8_composition_guard = _is_exact_v8_boundary_pair(
            frame_adapter_factory,
            lifecycle_coordinator,
        )
        self._requires_exact_v2_composition_guard = (
            False
            if self._requires_exact_v8_composition_guard
            else _is_exact_v2_boundary_pair(
                frame_adapter_factory,
                lifecycle_coordinator,
            )
        )
        self._requires_preconnect_admission = (
            requires_preconnect_admission
            or self._requires_exact_v2_composition_guard
            or self._requires_exact_v8_composition_guard
        )
        self._preconnect_admission_guard: WebSocketPreconnectAdmissionGuard | None = None
        if frame_adapter_factory is None:
            if pipeline is None or clock is None or sequencer is None:
                raise ValueError("default V1 frame adapter requires pipeline, clock, and sequencer")
            frame_adapter_factory = self._create_v1_frame_adapter
        self.frame_adapter_factory = frame_adapter_factory
        self._retained_depth_callbacks_enabled = _validate_retained_depth_callback_pair_v2(
            retained_depth_range_callback,
            retained_depth_resync_callback,
        )
        self._retained_depth_range_callback = retained_depth_range_callback
        self._retained_depth_resync_callback = retained_depth_resync_callback
        if preconnecting_generation_hook is not None and not callable(
            preconnecting_generation_hook
        ):
            raise TypeError("preconnecting generation hook must be callable or None")
        self._preconnecting_generation_hook = preconnecting_generation_hook
        self._preconnecting_generation_owner_capability: object | None = (
            object() if preconnecting_generation_hook is not None else None
        )
        self._active_preconnecting_generation_context: (
            WebSocketPreconnectingGenerationContext | None
        ) = None
        self._last_terminal_preconnecting_generation_context: (
            WebSocketPreconnectingGenerationContext | None
        ) = None
        self._last_preconnecting_generation_completed = False
        self._abandoned_generation_hook_task: asyncio.Task[None] | None = None
        self._generation_hook_dirty_error: PreconnectingGenerationHookDrainTimeout | None = None
        lineage_required = (
            preconnecting_generation_hook is not None or self._retained_depth_callbacks_enabled
        )
        self._generation_hook_session_id = _available_generation_identity(
            "session_id",
            frame_adapter_factory,
            lifecycle_coordinator,
            required=lineage_required,
        )
        self._generation_hook_protocol_hash = _available_generation_identity(
            "protocol_hash",
            frame_adapter_factory,
            lifecycle_coordinator,
            required=lineage_required,
        )
        if self._retained_depth_callbacks_enabled:
            _validate_retained_depth_callback_lineage_v2(
                self._generation_hook_session_id,
                self._generation_hook_protocol_hash,
            )
        self.depth_resync_callback = depth_resync_callback
        self.depth_range_callback = depth_range_callback
        self.depth_monitor = RawDepthContinuityMonitor(
            plan,
            on_resync=self._notify_depth_resync,
            on_range=self._notify_depth_range,
        )
        self._active_retained_depth_callback_context: _RetainedDepthCallbackContextV2 | None = None
        self._generation = 0

    @property
    def requires_preconnect_admission(self) -> bool:
        return self._requires_preconnect_admission

    @property
    def preconnect_admission_guard(self) -> WebSocketPreconnectAdmissionGuard | None:
        return self._preconnect_admission_guard

    @property
    def generation(self) -> int:
        """Expose the monotonic owner generation cursor for ownership admission."""

        return self._generation

    @property
    def preconnecting_generation_hook(
        self,
    ) -> WebSocketPreconnectingGenerationHook | None:
        """Expose the construction-time hook without permitting replacement."""

        return self._preconnecting_generation_hook

    @property
    def pending_preconnecting_generation_hook_task(
        self,
    ) -> asyncio.Task[None] | None:
        """Expose an explicitly owned hook task that refused bounded draining."""

        task = self._abandoned_generation_hook_task
        if task is None or task.done():
            return None
        return task

    @property
    def preconnecting_generation_hook_dirty_error(
        self,
    ) -> PreconnectingGenerationHookDrainTimeout | None:
        """Retain fail-visible evidence that a hook task escaped its drain bound."""

        return self._generation_hook_dirty_error

    @property
    def retained_depth_range_callback(
        self,
    ) -> RetainedDepthRangeCallbackV2 | None:
        """Expose the construction-time exact range callback without replacement."""

        return self._retained_depth_range_callback

    @property
    def retained_depth_resync_callback(
        self,
    ) -> RetainedDepthResyncCallbackV2 | None:
        """Expose the construction-time exact resync callback without replacement."""

        return self._retained_depth_resync_callback

    def bind_preconnect_admission_guard(
        self,
        guard: WebSocketPreconnectAdmissionGuard,
    ) -> None:
        """Bind the sole V2 admission guard exactly once before owner startup."""

        if not self._requires_preconnect_admission:
            raise ValueError("this WebSocket owner does not require an admission guard")
        if not callable(getattr(guard, "validate_current", None)):
            raise TypeError("preconnect admission guard must expose validate_current")
        if not callable(getattr(guard, "connector_admission_guard", None)):
            raise TypeError("preconnect admission guard must expose connector_admission_guard")
        if self._requires_exact_v8_composition_guard:
            from signalbot.r4b_v2.capture.websocket_composition import (
                PublicWebSocketOwnerCompositionV8,
            )

            if type(guard) is not PublicWebSocketOwnerCompositionV8:
                raise TypeError("exact V8 WebSocket boundaries require the exact V8 composition")
        elif self._requires_exact_v2_composition_guard:
            from signalbot.r4b_v2.capture.websocket_composition import (
                PublicWebSocketOwnerCompositionV2,
            )

            if type(guard) is not PublicWebSocketOwnerCompositionV2:
                raise TypeError(
                    "exact V2 WebSocket boundaries require the exact production composition"
                )
        if self._preconnect_admission_guard is not None:
            raise RuntimeError("preconnect admission guard is already bound")
        self._preconnect_admission_guard = guard

    async def run(self, stop_event: asyncio.Event) -> None:
        """Run until requested stop or a consecutive reconnect budget is exhausted."""

        if stop_event is not self.lifecycle_coordinator.stop_event:
            raise ValueError("WebSocket owner requires the shared pipeline fatal stop event")
        if self._generation_hook_dirty_error is not None:
            self.lifecycle_coordinator.trip_fatal(self._generation_hook_dirty_error)
            raise self._generation_hook_dirty_error
        consecutive_failures = 0
        while not stop_event.is_set():
            if consecutive_failures >= self.settings.maximum_reconnect_attempts:
                error = CaptureReconnectExhausted(
                    f"public WebSocket reconnect budget exhausted for {self.plan.name}"
                )
                self.lifecycle_coordinator.trip_fatal(error)
                raise error
            try:
                self._validate_preconnect_admission()
            except Exception as exc:
                self.lifecycle_coordinator.trip_fatal(exc)
                raise
            self._generation += 1
            connection_id = f"{self.plan.name}-g{self._generation:06d}"
            try:
                generation_admitted = await self._notify_preconnecting_generation(
                    connection_id,
                    stop_event=stop_event,
                )
            except asyncio.CancelledError as exc:
                self.lifecycle_coordinator.trip_fatal(exc)
                raise
            except Exception as exc:
                self.lifecycle_coordinator.trip_fatal(exc)
                raise
            if not generation_admitted:
                return
            self._transition(
                connection_id,
                last_frame_seq=0,
                state=ConnectionState.CONNECTING,
                reason="connect_attempt",
            )
            adapter: WebSocketFrameConsumer | None = None
            connected_at = asyncio.get_running_loop().time()
            try:
                async with AsyncExitStack() as connection_stack:
                    with self._connector_admission_guard():
                        self._validate_preconnect_admission()
                        frames = await connection_stack.enter_async_context(
                            self.connector(self.plan.url)
                        )
                        # Handshake awaits permit unrelated filesystem actors
                        # to replace authority paths without touching the held
                        # writer lease. Revalidate after entry, while release
                        # remains excluded, before CONNECTED or frame consume.
                        self._validate_preconnect_admission()
                    connected_at = asyncio.get_running_loop().time()
                    self._transition(
                        connection_id,
                        last_frame_seq=0,
                        state=ConnectionState.CONNECTED,
                        reason="public_session_open",
                    )
                    adapter = self.frame_adapter_factory(
                        connection_id=connection_id,
                        generation=self._generation,
                    )
                    self.depth_monitor.start_generation(self._generation)
                    outcome = await self._consume_until_boundary(
                        adapter,
                        frames,
                        stop_event,
                        connection_id=connection_id,
                        generation=self._generation,
                    )
                if outcome == "stop":
                    self._transition(
                        connection_id,
                        last_frame_seq=adapter.frame_seq,
                        state=ConnectionState.DISCONNECTED,
                        reason="owner_stop",
                    )
                    return
                if outcome == "recycle":
                    self._transition(
                        connection_id,
                        last_frame_seq=adapter.frame_seq,
                        state=ConnectionState.RECYCLED,
                        reason="proactive_lifetime_rotation",
                    )
                    consecutive_failures = 0
                    continue
                self._transition(
                    connection_id,
                    last_frame_seq=adapter.frame_seq,
                    state=ConnectionState.DISCONNECTED,
                    reason="remote_stream_ended",
                )
            except asyncio.CancelledError:
                self._transition_if_possible(
                    connection_id,
                    adapter,
                    state=ConnectionState.DISCONNECTED,
                    reason="owner_cancelled",
                )
                raise
            except _RECONNECTABLE_TRANSPORT_ERRORS:
                self._transition_if_possible(
                    connection_id,
                    adapter,
                    state=ConnectionState.DISCONNECTED,
                    reason="connection_failure",
                )
            except Exception as exc:
                self.lifecycle_coordinator.trip_fatal(exc)
                raise

            connected_seconds = asyncio.get_running_loop().time() - connected_at
            productive = adapter is not None and adapter.frame_seq > 0
            if productive and connected_seconds >= self.settings.healthy_reset_seconds:
                consecutive_failures = 1
            else:
                consecutive_failures += 1
            if consecutive_failures >= self.settings.maximum_reconnect_attempts:
                continue
            delay = self.settings.reconnect_delays_seconds[consecutive_failures - 1]
            if await _wait_for_stop(stop_event, delay):
                return

    def _validate_preconnect_admission(self) -> None:
        if not self._requires_preconnect_admission:
            return
        guard = self._preconnect_admission_guard
        if guard is None:
            raise RuntimeError("V2 WebSocket owner requires a bound preconnect admission guard")
        guard.validate_current()

    async def _notify_preconnecting_generation(
        self,
        connection_id: str,
        *,
        stop_event: asyncio.Event,
    ) -> bool:
        hook = self._preconnecting_generation_hook
        if hook is None:
            return not stop_event.is_set()
        context = self._mint_preconnecting_generation_context(
            connection_id,
            hook=hook,
        )
        hook_task = asyncio.create_task(hook(context))
        stop_task = asyncio.create_task(stop_event.wait())
        hook_completed = False
        generation_admitted = False
        try:
            try:
                done, _pending = await asyncio.wait(
                    {hook_task, stop_task},
                    timeout=self.settings.connect_timeout_seconds,
                    return_when=asyncio.FIRST_COMPLETED,
                )
                if hook_task in done:
                    await hook_task
                    hook_completed = True
                    generation_admitted = not stop_event.is_set()
                elif stop_task in done:
                    generation_admitted = False
                else:
                    raise TimeoutError(
                        "preconnecting generation hook exceeded connect_timeout_seconds"
                    )
            finally:
                await self._reap_preconnecting_generation_tasks(
                    hook_task,
                    stop_task,
                )
        except BaseException:
            self._invalidate_preconnecting_generation_context(context)
            raise
        if not hook_completed:
            self._invalidate_preconnecting_generation_context(context)
            return False
        try:
            self._complete_preconnecting_generation_context(context, hook=hook)
        except BaseException:
            self._invalidate_preconnecting_generation_context(context)
            raise
        return generation_admitted

    def _mint_preconnecting_generation_context(
        self,
        connection_id: str,
        *,
        hook: WebSocketPreconnectingGenerationHook,
    ) -> WebSocketPreconnectingGenerationContext:
        if type(self) is not PublicWebSocketCaptureOwner:
            raise TypeError("only the exact WebSocket owner can mint generation contexts")
        if hook is not self._preconnecting_generation_hook:
            raise RuntimeError("generation context hook is not the owner's exact hook")
        owner_capability = self._preconnecting_generation_owner_capability
        if type(owner_capability) is not object:
            raise RuntimeError("generation context owner capability is unavailable")
        if self._active_preconnecting_generation_context is not None:
            raise RuntimeError("a preconnecting generation context is already active")
        if type(self.plan) is not WebSocketPlan:
            raise TypeError("generation context requires the owner's exact WebSocket plan")
        validate_public_websocket_plan(self.plan)
        context = WebSocketPreconnectingGenerationContext(
            session_id=self._generation_hook_session_id,
            protocol_hash=self._generation_hook_protocol_hash,
            market=self.plan.market,
            route=self.plan.route,
            connection_id=connection_id,
            generation=self._generation,
            _factory_token=(_WEBSOCKET_PRECONNECTING_GENERATION_CONTEXT_FACTORY_TOKEN),
            _owner_capability=owner_capability,
            _hook_identity=hook,
            _owner_plan=self.plan,
        )
        self._active_preconnecting_generation_context = context
        return context

    def _validate_preconnecting_generation_context(
        self,
        context: WebSocketPreconnectingGenerationContext,
        *,
        hook: WebSocketPreconnectingGenerationHook,
    ) -> None:
        if type(self) is not PublicWebSocketCaptureOwner:
            raise TypeError("generation contexts require the exact WebSocket owner")
        if type(context) is not WebSocketPreconnectingGenerationContext:
            raise TypeError("preconnecting generation context has a foreign type")
        if (
            getattr(context, "_factory_seal", None)
            is not _WEBSOCKET_PRECONNECTING_GENERATION_CONTEXT_FACTORY_TOKEN
        ):
            raise TypeError("preconnecting generation context is not factory-sealed")
        _validate_websocket_preconnecting_generation_context_material(context)
        if getattr(
            context, "_material_seal", None
        ) != _websocket_preconnecting_generation_context_material(context):
            raise ValueError("preconnecting generation context material was changed")
        owner_capability = self._preconnecting_generation_owner_capability
        if type(owner_capability) is not object:
            raise RuntimeError("generation context owner capability is unavailable")
        if context._owner_capability is not owner_capability:
            raise ValueError("preconnecting generation context belongs to another owner")
        if hook is not self._preconnecting_generation_hook:
            raise ValueError("preconnecting generation validator received a foreign hook")
        if context._hook_identity is not hook:
            raise ValueError("preconnecting generation context belongs to another hook")
        if type(self.plan) is not WebSocketPlan or context._owner_plan is not self.plan:
            raise ValueError("preconnecting generation context owner plan changed")
        validate_public_websocket_plan(self.plan)
        if context is not self._active_preconnecting_generation_context:
            if context is self._last_terminal_preconnecting_generation_context:
                disposition = (
                    "completed" if self._last_preconnecting_generation_completed else "invalidated"
                )
                raise RuntimeError(f"preconnecting generation context was already {disposition}")
            raise RuntimeError("preconnecting generation context is not active")
        if context.session_id != self._generation_hook_session_id:
            raise ValueError("preconnecting generation session_id is no longer current")
        if context.protocol_hash != self._generation_hook_protocol_hash:
            raise ValueError("preconnecting generation protocol_hash is no longer current")
        if context.market is not self.plan.market or context.route != self.plan.route:
            raise ValueError("preconnecting generation route differs from its owner plan")
        if context.generation != self._generation:
            raise ValueError("preconnecting generation is no longer current")
        expected_connection_id = f"{self.plan.name}-g{self._generation:06d}"
        if context.connection_id != expected_connection_id:
            raise ValueError("preconnecting generation connection_id is not current")

    def _complete_preconnecting_generation_context(
        self,
        context: WebSocketPreconnectingGenerationContext,
        *,
        hook: WebSocketPreconnectingGenerationHook,
    ) -> None:
        self._validate_preconnecting_generation_context(context, hook=hook)
        self._active_preconnecting_generation_context = None
        self._last_terminal_preconnecting_generation_context = context
        self._last_preconnecting_generation_completed = True

    def _invalidate_preconnecting_generation_context(
        self,
        context: WebSocketPreconnectingGenerationContext,
    ) -> None:
        if self._active_preconnecting_generation_context is not context:
            return
        self._active_preconnecting_generation_context = None
        self._last_terminal_preconnecting_generation_context = context
        self._last_preconnecting_generation_completed = False

    async def _reap_preconnecting_generation_tasks(
        self,
        hook_task: asyncio.Task[None],
        stop_task: asyncio.Task[bool],
    ) -> None:
        tasks = {hook_task, stop_task}
        for task in tasks:
            if not task.done():
                task.cancel()
        done, pending = await asyncio.wait(
            tasks,
            timeout=self.settings.close_timeout_seconds,
        )
        cleanup_failure: BaseException | None = None
        for task in done:
            failure = _completed_task_failure(task)
            if task is hook_task and failure is not None:
                cleanup_failure = failure
        if pending:
            for task in pending:
                task.cancel()
            error = PreconnectingGenerationHookDrainTimeout(
                "preconnecting generation hook task refused bounded cancellation; "
                "owner state is dirty"
            )
            self._generation_hook_dirty_error = error
            if hook_task in pending:
                self._abandoned_generation_hook_task = hook_task
                hook_task.add_done_callback(_observe_late_task_completion)
            if stop_task in pending:
                stop_task.add_done_callback(_observe_late_task_completion)
            raise error
        if cleanup_failure is not None:
            raise cleanup_failure

    def _connector_admission_guard(self) -> AbstractContextManager[None]:
        if not self._requires_preconnect_admission:
            return contextlib.nullcontext()
        guard = self._preconnect_admission_guard
        if guard is None:
            raise RuntimeError("V2 WebSocket owner requires a bound preconnect admission guard")
        return guard.connector_admission_guard()

    async def _consume_until_boundary(
        self,
        adapter: WebSocketFrameConsumer,
        frames: FrameStream,
        stop_event: asyncio.Event,
        *,
        connection_id: str,
        generation: int,
    ) -> str:
        observed_frames = (
            _observe_after_offer(
                frames,
                lambda raw: self._observe_retained_depth_after_offer(
                    adapter=adapter,
                    connection_id=connection_id,
                    generation=generation,
                    raw=raw,
                ),
            )
            if self.depth_monitor.has_depth
            else frames
        )
        consumer = asyncio.create_task(adapter.consume(observed_frames), name=self.plan.name)
        stopped = asyncio.create_task(stop_event.wait(), name=f"{self.plan.name}-stop")
        lifetime = asyncio.create_task(
            asyncio.sleep(self.settings.maximum_connection_age_seconds),
            name=f"{self.plan.name}-lifetime",
        )
        tasks = {consumer, stopped, lifetime}
        try:
            done, _pending = await asyncio.wait(
                tasks,
                return_when=asyncio.FIRST_COMPLETED,
            )
            if consumer in done:
                await consumer
                return "ended"
            consumer.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await consumer
            return "stop" if stopped in done else "recycle"
        finally:
            for task in tasks:
                if not task.done():
                    task.cancel()
            for task in tasks:
                with contextlib.suppress(asyncio.CancelledError):
                    await task

    def _observe_retained_depth_after_offer(
        self,
        *,
        adapter: WebSocketFrameConsumer,
        connection_id: str,
        generation: int,
        raw: str | bytes,
    ) -> None:
        if not self._retained_depth_callbacks_enabled:
            self.depth_monitor.observe_after_offer(raw)
            return
        if self._active_retained_depth_callback_context is not None:
            raise RuntimeError("retained depth callback observation cannot be re-entered")
        if generation != self._generation:
            raise RuntimeError("retained depth callback generation is no longer current")
        context = _RetainedDepthCallbackContextV2(
            adapter=adapter,
            connection_id=connection_id,
            generation=generation,
            raw=raw,
        )
        self._active_retained_depth_callback_context = context
        try:
            # The observer emits range first and only then an optional resync.
            # Both calls remain synchronous before the iterator requests the
            # next socket item.
            self.depth_monitor.observe_after_offer(raw)
        finally:
            self._active_retained_depth_callback_context = None

    def _notify_depth_resync(self, request: DepthResyncRequest) -> None:
        if request.market is not self.plan.market:
            raise DepthSequenceError("depth resync market differs from its WebSocket plan")
        retained_callback = self._retained_depth_resync_callback
        if self.depth_resync_callback is None and retained_callback is None:
            raise DepthResyncUnavailable(
                "depth resync callback is required before startup, reconnect, or sequence gap"
            )
        retained_receipt = None
        if retained_callback is not None:
            context = self._require_active_retained_depth_callback_context_v2()
            preceding_range_receipt = context.range_receipt
            if preceding_range_receipt is None:
                raise RuntimeError(
                    "retained depth resync callback requires the preceding range callback"
                )
            from signalbot.r4b_v2.capture.websocket import (
                _RETAINED_DEPTH_OWNER_SEAM_TOKEN_V2,
                PublicWebSocketCaptureAdapterV2,
                _mint_public_retained_depth_resync_callback_receipt_v2,
            )

            if type(context.adapter) is not PublicWebSocketCaptureAdapterV2:
                raise TypeError("retained depth callback requires the exact V2 frame adapter")
            retained_receipt = _mint_public_retained_depth_resync_callback_receipt_v2(
                adapter=context.adapter,
                owner_plan=self.plan,
                session_id=cast(str, self._generation_hook_session_id),
                protocol_hash=cast(str, self._generation_hook_protocol_hash),
                connection_id=context.connection_id,
                generation=context.generation,
                frame_seq=context.adapter.frame_seq,
                raw=context.raw,
                request=request,
                preceding_range_receipt=preceding_range_receipt,
                _owner_seam_token=_RETAINED_DEPTH_OWNER_SEAM_TOKEN_V2,
            )
        if self.depth_resync_callback is not None:
            self.depth_resync_callback(request)
        if retained_callback is not None:
            _invoke_synchronous_retained_depth_callback_v2(
                retained_callback,
                retained_receipt,
                label="resync",
            )

    def _notify_depth_range(self, observation: DepthRangeObservation) -> None:
        if observation.market is not self.plan.market:
            raise DepthSequenceError("depth range market differs from its WebSocket plan")
        retained_callback = self._retained_depth_range_callback
        if self.depth_range_callback is None and retained_callback is None:
            raise DepthResyncUnavailable(
                "depth range callback is required before depth bootstrap or resync"
            )
        retained_receipt = None
        context: _RetainedDepthCallbackContextV2 | None = None
        if retained_callback is not None:
            context = self._require_active_retained_depth_callback_context_v2()
            from signalbot.r4b_v2.capture.websocket import (
                _RETAINED_DEPTH_OWNER_SEAM_TOKEN_V2,
                PublicWebSocketCaptureAdapterV2,
                _mint_public_retained_depth_range_callback_receipt_v2,
            )

            if type(context.adapter) is not PublicWebSocketCaptureAdapterV2:
                raise TypeError("retained depth callback requires the exact V2 frame adapter")
            retained_receipt = _mint_public_retained_depth_range_callback_receipt_v2(
                adapter=context.adapter,
                owner_plan=self.plan,
                session_id=cast(str, self._generation_hook_session_id),
                protocol_hash=cast(str, self._generation_hook_protocol_hash),
                connection_id=context.connection_id,
                generation=context.generation,
                frame_seq=context.adapter.frame_seq,
                raw=context.raw,
                observation=observation,
                _owner_seam_token=_RETAINED_DEPTH_OWNER_SEAM_TOKEN_V2,
            )
        if self.depth_range_callback is not None:
            self.depth_range_callback(observation)
        if retained_callback is not None:
            _invoke_synchronous_retained_depth_callback_v2(
                retained_callback,
                retained_receipt,
                label="range",
            )
            if context is None:
                raise RuntimeError("retained depth callback context disappeared")
            context.range_receipt = retained_receipt

    def _require_active_retained_depth_callback_context_v2(
        self,
    ) -> _RetainedDepthCallbackContextV2:
        context = self._active_retained_depth_callback_context
        if context is None:
            raise RuntimeError(
                "retained depth callback was invoked outside the post-offer frame seam"
            )
        if context.generation != self._generation:
            raise RuntimeError("retained depth callback context is no longer current")
        return context

    def _transition_if_possible(
        self,
        connection_id: str,
        adapter: WebSocketFrameConsumer | None,
        *,
        state: ConnectionState,
        reason: str,
    ) -> None:
        if self.lifecycle_coordinator.failed or not self.lifecycle_coordinator.accepting:
            return
        self._transition(
            connection_id,
            last_frame_seq=0 if adapter is None else adapter.frame_seq,
            state=state,
            reason=reason,
        )

    def _transition(
        self,
        connection_id: str,
        *,
        last_frame_seq: int,
        state: ConnectionState,
        reason: str,
    ) -> None:
        self.lifecycle_coordinator.record_transition(
            connection_id,
            generation=self._generation,
            last_frame_seq=last_frame_seq,
            state=state,
            reason=reason,
        )

    def _create_v1_frame_adapter(
        self,
        *,
        connection_id: str,
        generation: int,
    ) -> WebSocketFrameConsumer:
        del generation
        if self.pipeline is None or self.clock is None or self.sequencer is None:
            raise RuntimeError("default V1 frame adapter dependencies are unavailable")
        return PublicWebSocketCaptureAdapter(
            self.plan,
            plan_sha256=self.plan_sha256,
            process_boot_id=self.process_boot_id,
            connection_id=connection_id,
            pipeline=self.pipeline,
            clock=self.clock,
            sequencer=self.sequencer,
        )


def validate_websocket_preconnecting_generation_context(
    context: WebSocketPreconnectingGenerationContext,
    *,
    owner: PublicWebSocketCaptureOwner,
    hook: WebSocketPreconnectingGenerationHook,
) -> None:
    """Validate one still-active receipt against its exact owner and hook."""

    if type(owner) is not PublicWebSocketCaptureOwner:
        raise TypeError("generation contexts require the exact WebSocket owner")
    if type(context) is not WebSocketPreconnectingGenerationContext:
        raise TypeError("preconnecting generation context has a foreign type")
    owner._validate_preconnecting_generation_context(context, hook=hook)


def _is_exact_v2_boundary_pair(
    frame_adapter_factory: WebSocketFrameAdapterFactory | None,
    lifecycle_coordinator: WebSocketLifecycleFatalCoordinator,
) -> bool:
    """Recognize production V2 seams lazily without a module import cycle."""

    from signalbot.r4b_v2.capture.websocket import (
        PublicWebSocketFrameAdapterFactoryV2,
    )
    from signalbot.r4b_v2.capture.websocket_lifecycle import (
        WebSocketLifecycleFatalCoordinatorV2,
    )

    factory_is_exact_v2 = type(frame_adapter_factory) is PublicWebSocketFrameAdapterFactoryV2
    lifecycle_is_exact_v2 = type(lifecycle_coordinator) is WebSocketLifecycleFatalCoordinatorV2
    factory_is_v2_family = isinstance(
        frame_adapter_factory,
        PublicWebSocketFrameAdapterFactoryV2,
    )
    lifecycle_is_v2_family = isinstance(
        lifecycle_coordinator,
        WebSocketLifecycleFatalCoordinatorV2,
    )
    if (factory_is_v2_family or lifecycle_is_v2_family) and not (
        factory_is_exact_v2 and lifecycle_is_exact_v2
    ):
        raise TypeError(
            "production V2 frame factory and lifecycle coordinator must be paired exactly"
        )
    return factory_is_exact_v2 and lifecycle_is_exact_v2


def _is_exact_v8_boundary_pair(
    frame_adapter_factory: WebSocketFrameAdapterFactory | None,
    lifecycle_coordinator: WebSocketLifecycleFatalCoordinator,
) -> bool:
    """Recognize only the sealed V8 factory/lifecycle pair as one boundary."""

    from signalbot.r4b_v2.capture.websocket_composition import (
        PublicWebSocketFrameAdapterFactoryV8,
    )
    from signalbot.r4b_v2.capture.websocket_lifecycle import (
        WebSocketLifecycleFatalCoordinatorV8,
    )

    factory_is_exact_v8 = type(frame_adapter_factory) is PublicWebSocketFrameAdapterFactoryV8
    lifecycle_is_exact_v8 = type(lifecycle_coordinator) is WebSocketLifecycleFatalCoordinatorV8
    factory_is_v8_family = isinstance(
        frame_adapter_factory,
        PublicWebSocketFrameAdapterFactoryV8,
    )
    lifecycle_is_v8_family = isinstance(
        lifecycle_coordinator,
        WebSocketLifecycleFatalCoordinatorV8,
    )
    if (factory_is_v8_family or lifecycle_is_v8_family) and not (
        factory_is_exact_v8 and lifecycle_is_exact_v8
    ):
        raise TypeError(
            "production V8 frame factory and lifecycle coordinator must be paired exactly"
        )
    return factory_is_exact_v8 and lifecycle_is_exact_v8


def _validate_retained_depth_callback_pair_v2(
    range_callback: RetainedDepthRangeCallbackV2 | None,
    resync_callback: RetainedDepthResyncCallbackV2 | None,
) -> bool:
    if (range_callback is None) != (resync_callback is None):
        raise ValueError("retained depth range and resync callbacks must be configured together")
    if range_callback is None:
        return False
    for label, callback in (
        ("range", range_callback),
        ("resync", resync_callback),
    ):
        if not callable(callback):
            raise TypeError(f"retained depth {label} callback must be callable")
        callback_method = type(callback).__call__
        if inspect.iscoroutinefunction(callback) or inspect.iscoroutinefunction(callback_method):
            raise TypeError(f"retained depth {label} callback must be synchronous")
    return True


def _validate_retained_depth_callback_lineage_v2(
    session_id: str | None,
    protocol_hash: str | None,
) -> None:
    if (
        type(session_id) is not str
        or not session_id
        or session_id.strip() != session_id
        or len(session_id) > 256
        or any(character in session_id for character in "\r\n\x00")
    ):
        raise ValueError("retained depth callbacks require an available normalized session_id")
    if (
        type(protocol_hash) is not str
        or len(protocol_hash) != 64
        or any(character not in "0123456789abcdef" for character in protocol_hash)
    ):
        raise ValueError("retained depth callbacks require an available lowercase protocol_hash")


def _invoke_synchronous_retained_depth_callback_v2(
    callback: RetainedDepthRangeCallbackV2 | RetainedDepthResyncCallbackV2,
    receipt: object | None,
    *,
    label: str,
) -> None:
    if receipt is None:
        raise RuntimeError(f"retained depth {label} callback receipt was not minted")
    from signalbot.r4b_v2.capture.websocket import (
        PublicRetainedDepthRangeCallbackReceiptV2,
        PublicRetainedDepthResyncCallbackReceiptV2,
        validate_public_retained_depth_range_callback_receipt_v2,
        validate_public_retained_depth_resync_callback_receipt_v2,
    )

    if type(receipt) is PublicRetainedDepthRangeCallbackReceiptV2:
        validate_public_retained_depth_range_callback_receipt_v2(receipt)
    elif type(receipt) is PublicRetainedDepthResyncCallbackReceiptV2:
        validate_public_retained_depth_resync_callback_receipt_v2(receipt)
    else:
        raise TypeError(f"retained depth {label} callback receipt has a foreign type")
    try:
        result = cast(Callable[[object], object], callback)(receipt)
    except asyncio.CancelledError as exc:
        raise RuntimeError(f"retained depth {label} callback raised CancelledError") from exc
    if inspect.isawaitable(result):
        if inspect.iscoroutine(result):
            result.close()
        elif isinstance(result, asyncio.Future):
            result.cancel()
        raise TypeError(f"retained depth {label} callback must complete synchronously")
    if result is not None:
        raise TypeError(f"retained depth {label} callback must return None")


def _available_generation_identity(
    field_name: str,
    *boundaries: object,
    required: bool,
) -> str | None:
    if not required:
        return None
    values: list[str] = []
    for boundary in boundaries:
        value = getattr(boundary, field_name, None)
        if value is None:
            continue
        if type(value) is not str or not value:
            raise TypeError(f"available WebSocket {field_name} must be a non-empty string")
        values.append(value)
    if len(set(values)) > 1:
        raise ValueError(f"WebSocket generation hook boundaries disagree on {field_name}")
    return values[0] if values else None


def _validate_websocket_preconnecting_generation_context_material(
    context: WebSocketPreconnectingGenerationContext,
) -> None:
    _require_optional_normalized_generation_identity(context.session_id, "session_id")
    if context.protocol_hash is not None and (
        type(context.protocol_hash) is not str
        or len(context.protocol_hash) != 64
        or any(character not in "0123456789abcdef" for character in context.protocol_hash)
    ):
        raise ValueError("preconnecting generation protocol_hash must be lowercase SHA-256")
    if type(context.market) is not Market:
        raise TypeError("preconnecting generation market has a foreign type")
    _require_normalized_generation_string(context.route, "route", maximum=64)
    _require_normalized_generation_string(
        context.connection_id,
        "connection_id",
        maximum=512,
    )
    if (
        type(context.generation) is not int
        or context.generation < 1
        or context.generation > 999_999_999_999
    ):
        raise ValueError("preconnecting generation must be a bounded positive integer")
    if type(context._owner_plan) is not WebSocketPlan:
        raise TypeError("preconnecting generation context has a foreign owner plan")
    validate_public_websocket_plan(context._owner_plan)
    _require_normalized_generation_string(
        context._owner_plan.name,
        "owner plan name",
        maximum=256,
    )
    if (
        context.market is not context._owner_plan.market
        or context.route != context._owner_plan.route
    ):
        raise ValueError("preconnecting generation route differs from its owner plan")


def _websocket_preconnecting_generation_context_material(
    context: WebSocketPreconnectingGenerationContext,
) -> tuple[object, ...]:
    return (
        id(context._factory_seal),
        id(context._owner_capability),
        id(context._hook_identity),
        id(context._owner_plan),
        context.session_id,
        context.protocol_hash,
        context.market,
        context.route,
        context.connection_id,
        context.generation,
        _websocket_owner_plan_material(context._owner_plan),
    )


def _websocket_owner_plan_material(plan: WebSocketPlan) -> tuple[object, ...]:
    return (
        plan.name,
        plan.market,
        plan.route,
        plan.streams,
        plan.url,
    )


def _require_optional_normalized_generation_identity(
    value: str | None,
    field_name: str,
) -> None:
    if value is None:
        return
    _require_normalized_generation_string(value, field_name, maximum=256)


def _require_normalized_generation_string(
    value: object,
    field_name: str,
    *,
    maximum: int,
) -> None:
    if (
        type(value) is not str
        or not value
        or value.strip() != value
        or len(value) > maximum
        or any(character in value for character in "\r\n\x00")
    ):
        raise ValueError(f"preconnecting generation {field_name} must be normalized and bounded")


def _completed_task_failure(
    task: asyncio.Task[None] | asyncio.Task[bool],
) -> BaseException | None:
    """Retrieve one completed owned-task exception without losing evidence."""

    if not task.done():
        raise RuntimeError("owned generation task is not complete")
    if task.cancelled():
        return None
    return task.exception()


def _observe_late_task_completion(
    task: asyncio.Task[None] | asyncio.Task[bool],
) -> None:
    """Retrieve a dirty task's eventual exception while dirty state stays sealed."""

    _completed_task_failure(task)


async def _observe_after_offer(
    frames: FrameStream,
    observer: Callable[[str | bytes], None],
) -> AsyncIterator[str | bytes]:
    async for raw in frames:
        yield raw
        # Resumption happens only when the adapter requests its next frame.
        # Its previous iteration has therefore completed pipeline.offer.
        observer(raw)


def _require_finite_positive(value: object, field: str) -> None:
    if (
        isinstance(value, bool)
        or not isinstance(value, (int, float))
        or not math.isfinite(float(value))
        or value <= 0
    ):
        raise ValueError(f"{field} must be a finite positive number")


def _require_bounded_integer(value: object, field: str, *, maximum: int) -> None:
    if isinstance(value, bool) or not isinstance(value, int) or not 1 <= value <= maximum:
        raise ValueError(f"{field} must be a positive integer within its operational bound")


async def _wait_for_stop(stop_event: asyncio.Event, delay_seconds: float) -> bool:
    if stop_event.is_set():
        return True
    try:
        await asyncio.wait_for(stop_event.wait(), timeout=delay_seconds)
    except TimeoutError:
        return False
    return True
