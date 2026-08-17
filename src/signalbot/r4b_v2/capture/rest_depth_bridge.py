from __future__ import annotations

import asyncio
import hashlib
import inspect
from collections import deque
from collections.abc import Callable
from dataclasses import asdict, dataclass, field
from typing import Literal, cast

import httpx

from signalbot.capture.depth_sequence import (
    DepthSequenceError,
    classify_depth_snapshot_bridge,
)
from signalbot.capture.receipts import ReceiptClock
from signalbot.capture.ws_owner import (
    PublicWebSocketCaptureOwner,
    WebSocketPreconnectingGenerationContext,
    WebSocketPreconnectingGenerationHook,
    validate_websocket_preconnecting_generation_context,
)
from signalbot.domain.enums import Market
from signalbot.r4b_v2.canonical import canonical_json_line
from signalbot.r4b_v2.capture.integrity_ledger import (
    CaptureIntegrityEventV2,
    CaptureIntegrityLedgerV2,
)
from signalbot.r4b_v2.capture.plans import (
    ProvisionalDepthRestQualificationPlanV8,
    ProvisionalPromotingCapturePlanV2,
    ProvisionalPromotingPlanV8,
    provisional_promoting_plan_sha256_v8,
    validate_provisional_promoting_capture_plans_v8,
)
from signalbot.r4b_v2.capture.rest_attempt_owner import (
    RestCaptureFatalCoordinatorV2,
)
from signalbot.r4b_v2.capture.rest_depth import (
    PublicDepthRestAttemptPayloadV8,
    PublicDepthRestErrorCategoryV8,
    public_depth_rest_attempt_payload_sha256_v8,
    public_depth_rest_plan_sha256_v8,
    validate_public_depth_rest_plan_v8,
)
from signalbot.r4b_v2.capture.rest_depth_adapter import (
    PublicDepthRestCaptureAdapterV8,
)
from signalbot.r4b_v2.capture.rest_depth_bridge_evidence import (
    DEPTH_BRIDGE_EVENT_TYPE_V8,
    DEPTH_BRIDGE_MAXIMUM_BUFFERED_RANGES_PER_SYMBOL_V8,
    DepthBridgeAttemptClassificationV8,
    DepthBridgeAttemptStartedV8,
    DepthBridgeAttemptTerminalV8,
    DepthBridgeCoordinatorCleanCloseReceiptV8,
    DepthBridgeCycleOutcomeV8,
    DepthBridgeCycleRefV8,
    DepthBridgeCycleTerminalV8,
    DepthBridgeEvidencePayloadV8,
    DepthBridgeGenerationDrainedV8,
    DepthBridgeGenerationStartedV8,
    DepthBridgePhaseMaterialV8,
    DepthBridgePhaseV8,
    DepthBridgeRangeSummaryV8,
    DepthBridgeRegisteredCycleV8,
    DepthBridgeRestSourceLocatorV8,
    DepthBridgeTriggerRegisteredV8,
    DepthBridgeWaitOutcomeV8,
    DepthBridgeWaitTerminalV8,
    DepthBridgeWebSocketSourceLocatorV8,
    _issue_depth_bridge_coordinator_clean_close_receipt_v8,
    build_depth_bridge_cycle_ref_v8,
    build_depth_bridge_evidence_payload_v8,
    build_depth_bridge_range_summary_v8,
    depth_bridge_symbol_census_sha256_v8,
    validate_depth_bridge_coordinator_clean_close_receipt_v8,
)
from signalbot.r4b_v2.capture.rest_depth_scheduler import (
    PublicDepthRestRegisteredCycleV8,
    PublicDepthRestRegistrationDispositionV8,
    PublicDepthRestScheduleAuthorityV8,
    PublicDepthRestScheduledAttemptTokenV8,
    create_public_depth_rest_schedule_authority_v8,
    public_depth_rest_registration_disposition_v8,
    validate_public_depth_rest_schedule_authority_v8,
)
from signalbot.r4b_v2.capture.rest_depth_semantics import (
    PublicDepthRestSnapshotSemanticErrorV8,
    VerifiedPublicDepthRestSnapshotV8,
    validate_verified_public_depth_rest_snapshot_v8,
    verify_admitted_public_depth_rest_snapshot_v8,
)
from signalbot.r4b_v2.capture.websocket import (
    PublicDepthRestAdmissionReceiptV8,
    PublicRetainedDepthRangeCallbackReceiptV2,
    PublicRetainedDepthResyncCallbackReceiptV2,
    SharedWebSocketIngressV2,
    build_public_websocket_owner_plan_v2,
    validate_public_depth_rest_admission_receipt_v8,
    validate_public_retained_depth_range_callback_receipt_v2,
    validate_public_retained_depth_resync_callback_receipt_v2,
)

_NANOSECONDS_PER_MILLISECOND = 1_000_000
_DRAIN_GRACE_SECONDS = 1.0

type DepthBridgeDrainReasonV8 = Literal["reconnect", "normal_stop", "fatal"]
type _CycleTerminalReasonV8 = Literal[
    "snapshot_range_bridge",
    "newer_trigger",
    "generation_draining",
    "http_terminal",
    "semantic_invalid",
    "attempts_exhausted_stale",
    "attempts_exhausted_timeout",
    "range_buffer_overflow",
    "owner_stopped_unresolved",
    "coordinator_fatal",
]
type _WaitAbortV8 = Literal[
    "superseded",
    "generation_draining",
    "owner_stopped",
    "coordinator_fatal",
    "range_buffer_overflow",
]
type DepthRestTransportFactoryV8 = Callable[[], httpx.AsyncBaseTransport | None]
type _FatalCauseCodeV8 = Literal[
    "pretrigger_range_buffer_overflow",
    "coordinator_failure",
    "adapter_failure",
    "ledger_failure",
]


class PublicDepthRestBridgeCoordinatorErrorV8(RuntimeError):
    """A bounded depth-bridge owner could not preserve its exact lifecycle."""


@dataclass(slots=True)
class _RegisteredCycleStateV8:
    registration: PublicDepthRestRegisteredCycleV8
    cycle: DepthBridgeCycleRefV8
    supersedes_cycle_id: str | None
    superseded_by_cycle_id: str | None = None
    terminal: bool = False
    last_bridge_attempt: int | None = None


@dataclass(slots=True)
class _SymbolBridgeStateV8:
    symbol: str
    symbol_ordinal: int
    ranges: deque[DepthBridgeWebSocketSourceLocatorV8] = field(default_factory=deque)
    changed: asyncio.Event = field(default_factory=asyncio.Event)
    active: _RegisteredCycleStateV8 | None = None
    pending: _RegisteredCycleStateV8 | None = None
    worker: asyncio.Task[None] | None = None
    range_buffer_overflow: bool = False


@dataclass(frozen=True, slots=True)
class _AttemptResultV8:
    receipt: PublicDepthRestAdmissionReceiptV8 | None
    capture_error: BaseException | None


@dataclass(frozen=True, slots=True)
class _SemanticAttemptV8:
    receipt: PublicDepthRestAdmissionReceiptV8
    payload: PublicDepthRestAttemptPayloadV8
    snapshot: VerifiedPublicDepthRestSnapshotV8
    rest_source: DepthBridgeRestSourceLocatorV8


class PublicDepthRestBridgeCoordinatorV8:
    """Bounded qualification-only REST/depth bridge owner for one WS generation.

    The coordinator owns one fixed symbol slot and at most one worker task per
    symbol.  Scheduler disposition is observation only; the task is the worker
    lease and ``issue_attempt`` is the sole atomic READY-to-ISSUED transition.
    This owner emits no book, M2, promotion, PnL, alert, or order authority.
    """

    def __init__(
        self,
        promoting_plans: tuple[ProvisionalPromotingPlanV8, ...],
        depth_plan: ProvisionalDepthRestQualificationPlanV8,
        *,
        ingress: SharedWebSocketIngressV2,
        clock: ReceiptClock,
        fatal_coordinator: RestCaptureFatalCoordinatorV2,
        ledger: CaptureIntegrityLedgerV2,
        transport_factory: DepthRestTransportFactoryV8 | None = None,
    ) -> None:
        if type(promoting_plans) is not tuple:
            raise TypeError("V8 promoting_plans must be an exact tuple")
        validate_provisional_promoting_capture_plans_v8(promoting_plans)
        if type(depth_plan) is not ProvisionalDepthRestQualificationPlanV8:
            raise TypeError("depth bridge requires the exact V8 depth plan")
        if not any(plan is depth_plan for plan in promoting_plans):
            raise ValueError("depth plan must be the exact member of promoting_plans")
        if type(ingress) is not SharedWebSocketIngressV2:
            raise TypeError("depth bridge requires exact shared WebSocket ingress")
        if type(ledger) is not CaptureIntegrityLedgerV2:
            raise TypeError("depth bridge requires the exact capture integrity ledger")
        _validate_clock(clock)
        _validate_fatal_coordinator(fatal_coordinator)
        if transport_factory is not None:
            if not callable(transport_factory) or inspect.iscoroutinefunction(transport_factory):
                raise TypeError("transport_factory must be a synchronous callable")

        self._promoting_plans = promoting_plans
        self._depth_plan = depth_plan
        public_websocket_plans = tuple(
            plan
            for plan in promoting_plans
            if type(plan) is ProvisionalPromotingCapturePlanV2 and plan.route_id == "usdm_public"
        )
        if len(public_websocket_plans) != 1:
            raise ValueError("depth bridge requires one exact USD-M public WS plan")
        self._public_websocket_owner_plan = build_public_websocket_owner_plan_v2(
            public_websocket_plans[0]
        )
        self._ingress = ingress
        self._clock = clock
        self._fatal_coordinator = fatal_coordinator
        self._ledger = ledger
        self._transport_factory = transport_factory
        self._plan_bundle_sha256 = provisional_promoting_plan_sha256_v8(promoting_plans)
        self._depth_plan_sha256 = public_depth_rest_plan_sha256_v8(depth_plan)
        self._schedule_authority = create_public_depth_rest_schedule_authority_v8(depth_plan)
        self._slots = tuple(
            _SymbolBridgeStateV8(symbol, ordinal)
            for ordinal, symbol in enumerate(depth_plan.symbols)
        )
        self._slot_by_symbol = {slot.symbol: slot for slot in self._slots}
        self._workers: dict[str, asyncio.Task[None]] = {}
        self._permit = asyncio.Semaphore(depth_plan.maximum_concurrency)
        self._permit_in_use_count = 0
        self._adapter: PublicDepthRestCaptureAdapterV8 | None = None
        self._websocket_owner: PublicWebSocketCaptureOwner | None = None
        self._generation_open = False
        self._callbacks_accepting = False
        self._generation_trigger_registered = False
        self._session_id: str | None = None
        self._protocol_hash: str | None = None
        self._connection_id: str | None = None
        self._connection_generation = 0
        self._drain_reason: DepthBridgeDrainReasonV8 | None = None
        self._fatal_error: BaseException | None = None
        self._fatal_cause_code: _FatalCauseCodeV8 | None = None
        self._fatal_cause_sha256: str | None = None
        self._registered_cycle_count = 0
        self._accepted_cycle_count = 0
        self._superseded_cycle_count = 0
        self._failed_cycle_count = 0
        self._generation_lock = asyncio.Lock()
        self._scheduler_generation_retired = False
        self._pending_generation_drained_payload: DepthBridgeEvidencePayloadV8 | None = None
        self._permanently_closed = False
        self._clean_close_receipt: DepthBridgeCoordinatorCleanCloseReceiptV8 | None = None
        self._persisted_generation_started_count = 0
        self._persisted_generation_drained_count = 0
        self._persisted_fatal_generation_count = 0
        self._last_generation_drained_event: CaptureIntegrityEventV2 | None = None
        self._last_generation_drain_reason: DepthBridgeDrainReasonV8 | None = None

        def retained_range_callback(
            receipt: PublicRetainedDepthRangeCallbackReceiptV2,
        ) -> None:
            self.retain_depth_range(receipt)

        def retained_resync_callback(
            receipt: PublicRetainedDepthResyncCallbackReceiptV2,
        ) -> None:
            self.register_depth_resync(receipt)

        self._retained_range_callback = retained_range_callback
        self._retained_resync_callback = retained_resync_callback

    async def __call__(
        self,
        context: WebSocketPreconnectingGenerationContext,
        /,
    ) -> None:
        """Consume one factory-sealed owner handoff using this exact hook identity."""

        await self._begin_generation(context)

    @property
    def preconnecting_generation_hook(self) -> WebSocketPreconnectingGenerationHook:
        """Return the stable exact hook identity to install on the WS owner."""

        return self

    @property
    def retained_depth_range_callback(
        self,
    ) -> Callable[[PublicRetainedDepthRangeCallbackReceiptV2], None]:
        return self._retained_range_callback

    @property
    def retained_depth_resync_callback(
        self,
    ) -> Callable[[PublicRetainedDepthResyncCallbackReceiptV2], None]:
        return self._retained_resync_callback

    @property
    def schedule_authority(self) -> PublicDepthRestScheduleAuthorityV8:
        return self._schedule_authority

    @property
    def generation_open(self) -> bool:
        return self._generation_open

    @property
    def permanently_closed(self) -> bool:
        return self._permanently_closed

    @property
    def clean_close_receipt(
        self,
    ) -> DepthBridgeCoordinatorCleanCloseReceiptV8 | None:
        return self._clean_close_receipt

    @property
    def connection_generation(self) -> int:
        return self._connection_generation

    @property
    def worker_count(self) -> int:
        return len(self._workers)

    @property
    def permit_in_use_count(self) -> int:
        return self._permit_in_use_count

    @property
    def adapter(self) -> PublicDepthRestCaptureAdapterV8 | None:
        return self._adapter

    def bind_websocket_owner(self, owner: PublicWebSocketCaptureOwner, /) -> None:
        """Bind once to the exact owner that mints this hook's generation receipt."""

        if type(owner) is not PublicWebSocketCaptureOwner:
            raise TypeError("depth bridge requires the exact WebSocket owner")
        if self._permanently_closed:
            raise RuntimeError("permanently closed depth bridge rejects owner binding")
        if self._websocket_owner is not None:
            raise RuntimeError("depth bridge WebSocket owner was already bound")
        self._validate_bound_websocket_owner(owner)
        self._websocket_owner = owner

    def validate_current(self) -> None:
        """Revalidate exact ownership and bounded state without mutating it."""

        owner = self._websocket_owner
        if type(owner) is not PublicWebSocketCaptureOwner:
            raise RuntimeError("depth bridge lacks its exact bound WebSocket owner")
        self._validate_bound_websocket_owner(owner)
        receipt = self._clean_close_receipt
        if receipt is not None:
            if not self._permanently_closed:
                raise RuntimeError("bridge clean-close receipt lacks the permanent close latch")
            validate_depth_bridge_coordinator_clean_close_receipt_v8(
                receipt,
                promoting_plans=self._promoting_plans,
                depth_plan=self._depth_plan,
            )
        validate_public_depth_rest_plan_v8(self._depth_plan)
        validate_public_depth_rest_schedule_authority_v8(
            self._schedule_authority,
            plan=self._depth_plan,
        )
        if len(self._slots) != len(self._depth_plan.symbols):
            raise RuntimeError("depth bridge fixed slot census changed")
        if set(self._slot_by_symbol) != set(self._depth_plan.symbols):
            raise RuntimeError("depth bridge symbol-to-slot census changed")
        for ordinal, (slot, symbol) in enumerate(
            zip(self._slots, self._depth_plan.symbols, strict=True)
        ):
            if slot.symbol != symbol or slot.symbol_ordinal != ordinal:
                raise RuntimeError("depth bridge fixed slot identity changed")
            if self._slot_by_symbol.get(symbol) is not slot:
                raise RuntimeError("depth bridge symbol-to-slot identity changed")
            if len(slot.ranges) > DEPTH_BRIDGE_MAXIMUM_BUFFERED_RANGES_PER_SYMBOL_V8:
                raise RuntimeError("depth bridge retained range capacity was exceeded")
            if any(locator.symbol != symbol for locator in slot.ranges):
                raise RuntimeError("depth bridge slot contains a foreign symbol range")
            ingest_sequences = tuple(locator.ingest_seq for locator in slot.ranges)
            if ingest_sequences != tuple(sorted(set(ingest_sequences))):
                raise RuntimeError("depth bridge slot range order changed")
            worker = self._workers.get(symbol)
            if slot.worker is not worker:
                raise RuntimeError("depth bridge slot and worker registry diverged")
        if set(self._workers) - set(self._depth_plan.symbols):
            raise RuntimeError("depth bridge worker registry exceeds its census")
        if not 0 <= self._permit_in_use_count <= self._depth_plan.maximum_concurrency:
            raise RuntimeError("depth bridge permit counter is outside its bound")

        if not self._generation_open:
            if (
                self._adapter is not None
                or self._schedule_authority.generation_open
                or self._workers
                or self._permit_in_use_count
                or self._callbacks_accepting
                or self._pending_generation_drained_payload is not None
            ):
                raise RuntimeError("closed depth bridge retains live generation state")
            lineage = (self._session_id, self._protocol_hash, self._connection_id)
            if self._connection_generation == 0:
                if any(value is not None for value in lineage):
                    raise RuntimeError("unused depth bridge retains partial lineage")
            elif self._connection_generation < 0 or any(
                value is None for value in lineage
            ):
                raise RuntimeError("retired depth bridge lacks exact lineage")
            if receipt is not None:
                last_event = self._last_generation_drained_event
                if (
                    type(last_event) is not CaptureIntegrityEventV2
                    or receipt.last_generation_drained_event_sequence
                    != last_event.event_sequence
                    or receipt.last_generation_drained_event_sha256
                    != last_event.sha256
                    or self._last_generation_drain_reason != "normal_stop"
                ):
                    raise RuntimeError(
                        "bridge clean-close receipt lost its exact persisted drain"
                    )
            return
        if (
            self._session_id is None
            or self._protocol_hash is None
            or self._connection_id is None
            or self._connection_generation < 1
        ):
            raise RuntimeError("open depth bridge lacks exact generation lineage")
        if owner.generation != self._connection_generation:
            raise RuntimeError("depth bridge and WebSocket owner generations differ")
        if self._scheduler_generation_retired:
            if (
                self._schedule_authority.generation_open
                or self._pending_generation_drained_payload is None
            ):
                raise RuntimeError("retired depth bridge drain state is incoherent")
            return
        if (
            not self._schedule_authority.generation_open
            or self._schedule_authority.current_connection_generation
            != self._connection_generation
            or self._pending_generation_drained_payload is not None
        ):
            raise RuntimeError("depth bridge scheduler generation is not current")
        adapter = self._require_adapter()
        adapter_counts = (
            adapter.active_attempt_count,
            adapter.pending_owner_task_count,
            adapter.retained_terminal_admission_count,
        )
        if (
            adapter.plan is not self._depth_plan
            or adapter.bound_schedule_authority is not self._schedule_authority
            or adapter.session_id != self._session_id
            or adapter.protocol_hash != self._protocol_hash
            or adapter.connection_id != self._connection_id
            or adapter.generation != self._connection_generation
            or any(
                count < 0 or count > self._depth_plan.maximum_concurrency
                for count in adapter_counts
            )
        ):
            raise RuntimeError("depth bridge adapter generation state is incoherent")

    def validate_runtime_bindings(
        self,
        *,
        promoting_plans: tuple[ProvisionalPromotingPlanV8, ...],
        depth_plan: ProvisionalDepthRestQualificationPlanV8,
        websocket_owner: PublicWebSocketCaptureOwner,
        ingress: SharedWebSocketIngressV2,
        clock: ReceiptClock,
        fatal_coordinator: RestCaptureFatalCoordinatorV2,
        ledger: CaptureIntegrityLedgerV2,
    ) -> None:
        """Prove one runtime shares this coordinator's exact capture authority.

        ``validate_current`` proves the coordinator is internally coherent.  A
        top-level runtime must additionally prove that it did not splice this
        coherent coordinator into a foreign plan bundle, owner, ingress,
        receipt clock, fatal coordinator, or integrity ledger.
        """

        self.validate_current()
        exact_bindings = (
            ("promoting plan tuple", promoting_plans, self._promoting_plans),
            ("depth plan", depth_plan, self._depth_plan),
            ("WebSocket owner", websocket_owner, self._websocket_owner),
            ("shared ingress", ingress, self._ingress),
            ("receipt clock", clock, self._clock),
            ("fatal coordinator", fatal_coordinator, self._fatal_coordinator),
            ("integrity ledger", ledger, self._ledger),
        )
        for label, supplied, expected in exact_bindings:
            if supplied is not expected:
                raise ValueError(f"depth bridge has a foreign {label}")

    def _validate_bound_websocket_owner(
        self,
        owner: PublicWebSocketCaptureOwner,
    ) -> None:
        if owner.preconnecting_generation_hook is not self:
            raise ValueError("WebSocket owner has a foreign generation hook")
        if owner.plan != self._public_websocket_owner_plan:
            raise ValueError("WebSocket owner has a foreign public depth stream census")
        if owner.retained_depth_range_callback is not self._retained_range_callback:
            raise ValueError("WebSocket owner has a foreign retained range callback")
        if owner.retained_depth_resync_callback is not self._retained_resync_callback:
            raise ValueError("WebSocket owner has a foreign retained resync callback")

    def retain_depth_range(
        self,
        receipt: PublicRetainedDepthRangeCallbackReceiptV2,
        /,
    ) -> None:
        """Synchronously retain one exact admitted WS range in its bounded slot."""

        try:
            self._require_callback_generation()
            validate_public_retained_depth_range_callback_receipt_v2(receipt)
            self._validate_retained_callback_lineage(receipt)
            observation = receipt.observation
            if observation.symbol not in self._slot_by_symbol:
                raise ValueError("retained depth symbol is outside the exact census")
            slot = self._slot_by_symbol[observation.symbol]
            locator = _websocket_source_locator(receipt)
            if slot.ranges and locator.ingest_seq <= slot.ranges[-1].ingest_seq:
                raise ValueError("retained depth ranges must strictly advance ingest_seq")
            if observation.reset:
                slot.ranges.clear()
            elif (
                self._generation_trigger_registered and slot.active is None and slot.pending is None
            ):
                slot.ranges.clear()
            if len(slot.ranges) >= DEPTH_BRIDGE_MAXIMUM_BUFFERED_RANGES_PER_SYMBOL_V8:
                slot.range_buffer_overflow = True
                slot.changed.set()
                error = PublicDepthRestBridgeCoordinatorErrorV8(
                    f"bounded depth range buffer overflowed for {slot.symbol}"
                )
                self._fail_closed(
                    error,
                    code=(
                        "coordinator_failure"
                        if self._generation_trigger_registered
                        else "pretrigger_range_buffer_overflow"
                    ),
                )
                raise error
            slot.ranges.append(locator)
            slot.changed.set()
        except BaseException as exc:
            if not isinstance(exc, asyncio.CancelledError):
                self._fail_closed(exc)
            raise

    def register_depth_resync(
        self,
        receipt: PublicRetainedDepthResyncCallbackReceiptV2,
        /,
    ) -> None:
        """Synchronously schedule and durably register one retained resync callback."""

        try:
            self._require_callback_generation()
            validate_public_retained_depth_resync_callback_receipt_v2(receipt)
            self._validate_retained_callback_lineage(receipt)
            request = receipt.request
            initial_sources = tuple(
                self._initial_source(symbol, first_buffered_u)
                for symbol, first_buffered_u in request.watermarks
            )
            if not any(
                source.frame_seq == receipt.frame_seq
                and source.ingest_seq == receipt.ingest_seq
                and source.raw_payload_sha256 == receipt.raw_payload_sha256
                and source.receipt_wall_ms == receipt.receipt_wall_ms
                and source.receipt_monotonic_ns == receipt.receipt_monotonic_ns
                for source in initial_sources
            ):
                raise ValueError("resync callback lacks its exact preceding retained range source")

            registrations = self._schedule_authority.register_trigger(
                trigger=request.event,
                connection_generation=request.generation,
                symbol_watermarks=request.watermarks,
            )
            cycle_states: list[_RegisteredCycleStateV8] = []
            evidence_cycles: list[DepthBridgeRegisteredCycleV8] = []
            for registration, initial_source in zip(
                registrations,
                initial_sources,
                strict=True,
            ):
                slot = self._slots[registration.symbol_ordinal]
                predecessor = slot.pending or slot.active
                supersedes_cycle_id = None if predecessor is None else predecessor.cycle.cycle_id
                cycle = self._cycle_ref(registration)
                state = _RegisteredCycleStateV8(
                    registration=registration,
                    cycle=cycle,
                    supersedes_cycle_id=supersedes_cycle_id,
                )
                cycle_states.append(state)
                evidence_cycles.append(
                    DepthBridgeRegisteredCycleV8(
                        cycle=cycle,
                        initial_range_source=initial_source,
                        supersedes_cycle_id=supersedes_cycle_id,
                    )
                )

            self._append(
                DepthBridgePhaseV8.TRIGGER_REGISTERED,
                DepthBridgeTriggerRegisteredV8(
                    trigger=request.event,
                    trigger_seq=registrations[0].trigger_seq,
                    cycles=tuple(evidence_cycles),
                ),
            )
            self._generation_trigger_registered = True
            for state in cycle_states:
                self._install_cycle(state)
            for state in cycle_states:
                self._ensure_worker(self._slots[state.registration.symbol_ordinal])
        except BaseException as exc:
            if not isinstance(exc, asyncio.CancelledError):
                self._fail_closed(exc)
            raise

    async def drain_generation(self, reason: DepthBridgeDrainReasonV8) -> None:
        """Resolve every cycle, close owned I/O, then persist the zero-count drain."""

        if reason not in ("reconnect", "normal_stop", "fatal"):
            raise ValueError("unsupported depth bridge drain reason")
        async with self._generation_lock:
            if reason == "fatal" and self._generation_open and self._fatal_error is None:
                self._fail_closed(
                    PublicDepthRestBridgeCoordinatorErrorV8(
                        "explicit fatal generation drain requested"
                    ),
                    code="coordinator_failure",
                )
            await self._drain_generation_locked(reason)

    async def abort_and_drain(self, cause: BaseException, /) -> None:
        """Latch an abnormal caller cause and boundedly seal the open generation."""

        if not isinstance(cause, BaseException):
            raise TypeError("depth bridge abort cause must be an exception")
        self._permanently_closed = True
        async with self._generation_lock:
            if self._generation_open:
                self._fail_closed(cause, code="coordinator_failure")
            await self._drain_generation_locked("fatal")

    async def aclose(self) -> DepthBridgeCoordinatorCleanCloseReceiptV8 | None:
        """Permanently stop and mint at most one exact normal-close receipt.

        Replays after a successful normal close return the same receipt object.
        A fatal, reconnect-only, cancelled, or never-started lifecycle remains
        closed but cannot mint clean bridge authority.
        """

        self._permanently_closed = True
        async with self._generation_lock:
            receipt = self._clean_close_receipt
            if receipt is not None:
                validate_depth_bridge_coordinator_clean_close_receipt_v8(
                    receipt,
                    promoting_plans=self._promoting_plans,
                    depth_plan=self._depth_plan,
                )
                return receipt
            if not self._generation_open:
                return None
            self._observe_external_fatal()
            reason: DepthBridgeDrainReasonV8 = (
                "fatal" if self._fatal_error is not None else "normal_stop"
            )
            await self._drain_generation_locked(reason)
            if self._fatal_error is not None or reason != "normal_stop":
                return None
            event = self._last_generation_drained_event
            if (
                type(event) is not CaptureIntegrityEventV2
                or self._last_generation_drain_reason != "normal_stop"
            ):
                raise PublicDepthRestBridgeCoordinatorErrorV8(
                    "normal bridge close lacks its exact persisted drain event"
                )
            self._validate_clean_close_zero_state()
            closed_at = self._clock.capture()
            receipt = _issue_depth_bridge_coordinator_clean_close_receipt_v8(
                session_id=_required_lineage(self._session_id, "session_id"),
                protocol_hash=_required_lineage(self._protocol_hash, "protocol_hash"),
                promoting_plans=self._promoting_plans,
                depth_plan=self._depth_plan,
                last_connection_id=_required_lineage(
                    self._connection_id,
                    "connection_id",
                ),
                last_connection_generation=self._connection_generation,
                generation_started_count=self._persisted_generation_started_count,
                generation_drained_count=self._persisted_generation_drained_count,
                fatal_generation_count=self._persisted_fatal_generation_count,
                last_generation_drained_event_sequence=event.event_sequence,
                last_generation_drained_event_sha256=event.sha256,
                last_generation_drained_recorded_wall_ms=event.recorded_wall_ms,
                last_generation_drained_recorded_monotonic_ns=(
                    event.recorded_monotonic_ns
                ),
                close_wall_ms=closed_at.received_at_ms,
                close_monotonic_ns=closed_at.received_monotonic_ns,
            )
            self._clean_close_receipt = receipt
            return receipt

    async def _begin_generation(
        self,
        context: WebSocketPreconnectingGenerationContext,
    ) -> None:
        if self._permanently_closed:
            raise PublicDepthRestBridgeCoordinatorErrorV8(
                "permanently closed depth bridge rejects a new generation"
            )
        owner = self._websocket_owner
        if owner is None:
            raise RuntimeError("depth bridge generation hook lacks its bound owner")
        validate_websocket_preconnecting_generation_context(
            context,
            owner=owner,
            hook=self,
        )
        if context.session_id is None or context.protocol_hash is None:
            raise ValueError("V8 depth bridge requires session and protocol lineage")
        if context.market is not Market.FUTURES or context.route != "public":
            raise ValueError("V8 depth bridge requires the routed USD-M public owner")

        async with self._generation_lock:
            if self._permanently_closed:
                raise PublicDepthRestBridgeCoordinatorErrorV8(
                    "permanently closed depth bridge rejects a new generation"
                )
            if self._generation_open:
                await self._drain_generation_locked("reconnect")
            if self._fatal_error is not None:
                raise PublicDepthRestBridgeCoordinatorErrorV8(
                    "fatal depth bridge cannot open a successor generation"
                ) from self._fatal_error
            self._reset_generation_state()
            self._schedule_authority.advance_connection_generation(
                context.generation,
                session_id=context.session_id,
                protocol_hash=context.protocol_hash,
                connection_id=context.connection_id,
            )
            adapter: PublicDepthRestCaptureAdapterV8 | None = None
            try:
                transport = None if self._transport_factory is None else self._transport_factory()
                if transport is not None and not isinstance(
                    transport,
                    httpx.AsyncBaseTransport,
                ):
                    raise TypeError("transport_factory returned a foreign transport")
                adapter = PublicDepthRestCaptureAdapterV8(
                    self._depth_plan,
                    session_id=context.session_id,
                    protocol_hash=context.protocol_hash,
                    connection_id=context.connection_id,
                    generation=context.generation,
                    clock=self._clock,
                    ingress=self._ingress,
                    fatal_coordinator=self._fatal_coordinator,
                    transport=transport,
                )
                adapter.bind_schedule_authority(self._schedule_authority)
                self._adapter = adapter
                self._session_id = context.session_id
                self._protocol_hash = context.protocol_hash
                self._connection_id = context.connection_id
                self._connection_generation = context.generation
                self._append(
                    DepthBridgePhaseV8.GENERATION_STARTED,
                    DepthBridgeGenerationStartedV8(
                        symbol_count=len(self._depth_plan.symbols),
                        symbol_census_sha256=depth_bridge_symbol_census_sha256_v8(
                            self._depth_plan.symbols
                        ),
                        maximum_concurrency=self._depth_plan.maximum_concurrency,
                        maximum_buffered_ranges_per_symbol=(
                            DEPTH_BRIDGE_MAXIMUM_BUFFERED_RANGES_PER_SYMBOL_V8
                        ),
                        bridge_maximum_attempts=(self._depth_plan.bridge_maximum_attempts),
                        bridge_wait_timeout_ms=(self._depth_plan.bridge_wait_timeout_ms),
                    ),
                )
            except BaseException as exc:
                if adapter is not None:
                    try:
                        await adapter.aclose()
                    except BaseException as close_error:
                        self._fail_closed(close_error, code="adapter_failure")
                if self._schedule_authority.generation_open:
                    self._schedule_authority.retire_current_generation(
                        session_id=context.session_id,
                        protocol_hash=context.protocol_hash,
                        connection_id=context.connection_id,
                        connection_generation=context.generation,
                    )
                self._adapter = None
                self._session_id = None
                self._protocol_hash = None
                self._connection_id = None
                self._connection_generation = 0
                self._callbacks_accepting = False
                self._fail_closed(exc)
                raise
            self._generation_open = True
            self._callbacks_accepting = True

    def _install_cycle(self, state: _RegisteredCycleStateV8) -> None:
        slot = self._slots[state.registration.symbol_ordinal]
        if slot.pending is not None:
            prior_pending = slot.pending
            prior_pending.superseded_by_cycle_id = state.cycle.cycle_id
            self._terminal_cycle(
                prior_pending,
                outcome=DepthBridgeCycleOutcomeV8.SUPERSEDED,
                reason="newer_trigger",
                terminal_bridge_attempt=prior_pending.last_bridge_attempt,
            )
            slot.pending = None
        if slot.active is None or slot.active.terminal:
            slot.active = state
        else:
            slot.active.superseded_by_cycle_id = state.cycle.cycle_id
            slot.pending = state
        self._registered_cycle_count += 1
        slot.changed.set()

    def _ensure_worker(self, slot: _SymbolBridgeStateV8) -> None:
        current = self._workers.get(slot.symbol)
        if current is not None and not current.done():
            slot.changed.set()
            return
        task = asyncio.create_task(
            self._run_symbol_worker(slot),
            name=f"r4b-v2-depth-bridge-{slot.symbol}",
        )
        slot.worker = task
        self._workers[slot.symbol] = task
        task.add_done_callback(
            lambda completed, owned_slot=slot: self._consume_worker_completion(
                owned_slot,
                completed,
            )
        )

    async def _run_symbol_worker(self, slot: _SymbolBridgeStateV8) -> None:
        try:
            while True:
                cycle = slot.active
                if cycle is None:
                    return
                if not cycle.terminal:
                    await self._run_cycle(slot, cycle)
                if slot.active is cycle:
                    slot.active = slot.pending
                    slot.pending = None
                if slot.active is None:
                    return
        except asyncio.CancelledError as exc:
            cycle = slot.active
            if cycle is not None and not cycle.terminal:
                self._terminal_cycle(
                    cycle,
                    outcome=DepthBridgeCycleOutcomeV8.FAILED,
                    reason="owner_stopped_unresolved",
                    terminal_bridge_attempt=cycle.last_bridge_attempt,
                )
            self._fail_closed(exc)
            raise
        except BaseException as exc:
            self._fail_closed(exc)
            cycle = slot.active
            if cycle is not None and not cycle.terminal:
                self._terminal_cycle(
                    cycle,
                    outcome=DepthBridgeCycleOutcomeV8.FAILED,
                    reason="coordinator_fatal",
                    terminal_bridge_attempt=cycle.last_bridge_attempt,
                )
        finally:
            current = asyncio.current_task()
            if self._workers.get(slot.symbol) is current:
                del self._workers[slot.symbol]
            if slot.worker is current:
                slot.worker = None

    async def _run_cycle(
        self,
        slot: _SymbolBridgeStateV8,
        cycle: _RegisteredCycleStateV8,
    ) -> None:
        for bridge_attempt in range(1, self._depth_plan.bridge_maximum_attempts + 1):
            abort = self._cycle_abort(slot, cycle)
            if abort is not None:
                self._terminal_cycle_from_abort(cycle, abort)
                return
            disposition = public_depth_rest_registration_disposition_v8(
                cycle.registration,
                plan=self._depth_plan,
                schedule_authority=self._schedule_authority,
            )
            if disposition is PublicDepthRestRegistrationDispositionV8.SUPERSEDED:
                self._terminal_cycle_from_abort(cycle, "superseded")
                return
            if disposition is PublicDepthRestRegistrationDispositionV8.PENDING:
                raise PublicDepthRestBridgeCoordinatorErrorV8(
                    "symbol worker attempted I/O for a pending registration"
                )
            if disposition not in (
                PublicDepthRestRegistrationDispositionV8.ACTIVE_READY,
                PublicDepthRestRegistrationDispositionV8.ACTIVE_TERMINAL_ADMITTED,
            ):
                raise PublicDepthRestBridgeCoordinatorErrorV8(
                    "symbol worker encountered an already-issued concurrent attempt"
                )

            await self._permit.acquire()
            self._permit_in_use_count += 1
            try:
                abort = self._cycle_abort(slot, cycle)
                if abort is not None:
                    self._terminal_cycle_from_abort(cycle, abort)
                    return
                token = self._schedule_authority.issue_attempt(
                    registration=cycle.registration,
                    bridge_attempt=bridge_attempt,
                )
                cycle.last_bridge_attempt = bridge_attempt
                self._append(
                    DepthBridgePhaseV8.ATTEMPT_STARTED,
                    DepthBridgeAttemptStartedV8(
                        cycle=cycle.cycle,
                        bridge_attempt=bridge_attempt,
                    ),
                )
                result = await self._capture_attempt(token)
            finally:
                self._permit_in_use_count -= 1
                self._permit.release()

            if result.receipt is None:
                self._append_failed_attempt(
                    cycle,
                    bridge_attempt=bridge_attempt,
                    failure_code="owner_failure",
                    ranges=tuple(slot.ranges),
                )
                abort = self._cycle_abort(slot, cycle)
                if abort is None:
                    self._terminal_cycle(
                        cycle,
                        outcome=DepthBridgeCycleOutcomeV8.FAILED,
                        reason="coordinator_fatal",
                        terminal_bridge_attempt=bridge_attempt,
                    )
                else:
                    self._terminal_cycle_from_abort(cycle, abort)
                error = PublicDepthRestBridgeCoordinatorErrorV8(
                    "depth adapter failed without a recoverable terminal admission"
                )
                if result.capture_error is not None:
                    error.__cause__ = result.capture_error
                self._fail_closed(error, code="adapter_failure")
                _propagate_capture_cancellation(result)
                return

            terminal = self._classify_semantic_attempt(result.receipt)
            if terminal is None:
                payload = PublicDepthRestAttemptPayloadV8.from_canonical_bytes(
                    result.receipt.record.payload_bytes(),
                    plan=self._depth_plan,
                )
                failure_code: Literal["http_terminal", "semantic_invalid", "admission_cancelled"]
                if (
                    payload.admission_cancellation_requested
                    or payload.error_category is PublicDepthRestErrorCategoryV8.CANCELLED
                ):
                    failure_code = "admission_cancelled"
                elif (
                    payload.response_status != 200
                    or not payload.payload_complete
                    or payload.error_category is not None
                ):
                    failure_code = "http_terminal"
                else:
                    failure_code = "semantic_invalid"
                self._append_failed_attempt(
                    cycle,
                    bridge_attempt=bridge_attempt,
                    failure_code=failure_code,
                    ranges=tuple(slot.ranges),
                    rest_source=_rest_source_from_receipt(
                        result.receipt,
                        plan=self._depth_plan,
                    ),
                )
                abort = self._cycle_abort(slot, cycle)
                if abort is not None:
                    self._terminal_cycle_from_abort(cycle, abort)
                else:
                    reason: _CycleTerminalReasonV8 = (
                        "owner_stopped_unresolved"
                        if failure_code == "admission_cancelled"
                        else failure_code
                    )
                    self._terminal_cycle(
                        cycle,
                        outcome=DepthBridgeCycleOutcomeV8.FAILED,
                        reason=reason,
                        terminal_bridge_attempt=bridge_attempt,
                    )
                if result.capture_error is not None and not isinstance(
                    result.capture_error,
                    asyncio.CancelledError,
                ):
                    self._fail_closed(result.capture_error, code="adapter_failure")
                _propagate_capture_cancellation(result)
                if failure_code == "admission_cancelled":
                    raise asyncio.CancelledError(
                        "depth bridge adapter admitted an externally cancelled attempt"
                    )
                return

            decision = self._classify_ranges(slot, terminal.snapshot.last_update_id)
            range_summary, discarded = self._commit_decision_ranges(slot, decision)
            if decision.status == "accepted":
                self._append_classified_attempt(
                    cycle,
                    bridge_attempt=bridge_attempt,
                    classification=DepthBridgeAttemptClassificationV8.ACCEPTED,
                    terminal=terminal,
                    target_update_id=decision.target_update_id,
                    discarded_range_count=discarded,
                    range_summary=range_summary,
                )
                self._terminal_accepted(
                    cycle,
                    bridge_attempt=bridge_attempt,
                    terminal=terminal,
                    target_update_id=decision.target_update_id,
                    range_summary=range_summary,
                )
                if result.capture_error is not None:
                    self._fail_closed(result.capture_error, code="adapter_failure")
                _propagate_capture_cancellation(result)
                return
            if decision.status == "stale":
                self._append_classified_attempt(
                    cycle,
                    bridge_attempt=bridge_attempt,
                    classification=DepthBridgeAttemptClassificationV8.STALE,
                    terminal=terminal,
                    target_update_id=decision.target_update_id,
                    discarded_range_count=discarded,
                    range_summary=range_summary,
                )
                if result.capture_error is not None:
                    self._fail_closed(result.capture_error, code="adapter_failure")
                    self._terminal_cycle(
                        cycle,
                        outcome=DepthBridgeCycleOutcomeV8.FAILED,
                        reason="coordinator_fatal",
                        terminal_bridge_attempt=bridge_attempt,
                    )
                    _propagate_capture_cancellation(result)
                    return
                if bridge_attempt == self._depth_plan.bridge_maximum_attempts:
                    self._terminal_cycle(
                        cycle,
                        outcome=DepthBridgeCycleOutcomeV8.FAILED,
                        reason="attempts_exhausted_stale",
                        terminal_bridge_attempt=bridge_attempt,
                    )
                    return
                continue

            wait_result = await self._wait_for_bridge(
                slot,
                cycle,
                bridge_attempt=bridge_attempt,
                terminal=terminal,
                initial_range_summary=range_summary,
                initial_discarded=discarded,
                cancellation_error=(
                    result.capture_error
                    if isinstance(result.capture_error, asyncio.CancelledError)
                    else None
                ),
            )
            if result.capture_error is not None:
                self._fail_closed(result.capture_error, code="adapter_failure")
            _propagate_capture_cancellation(result)
            if wait_result in ("accepted", "terminal"):
                return
            if wait_result == "stale":
                if bridge_attempt == self._depth_plan.bridge_maximum_attempts:
                    self._terminal_cycle(
                        cycle,
                        outcome=DepthBridgeCycleOutcomeV8.FAILED,
                        reason="attempts_exhausted_stale",
                        terminal_bridge_attempt=bridge_attempt,
                    )
                    return
                continue
            assert wait_result == "timeout"
            if bridge_attempt == self._depth_plan.bridge_maximum_attempts:
                self._terminal_cycle(
                    cycle,
                    outcome=DepthBridgeCycleOutcomeV8.FAILED,
                    reason="attempts_exhausted_timeout",
                    terminal_bridge_attempt=bridge_attempt,
                )
                return

    async def _capture_attempt(
        self,
        token: PublicDepthRestScheduledAttemptTokenV8,
    ) -> _AttemptResultV8:
        adapter = self._require_adapter()
        try:
            return _AttemptResultV8(
                receipt=await adapter.capture_attempt(token),
                capture_error=None,
            )
        except BaseException as exc:
            receipt = adapter.take_terminal_admission_after_failure(token)
            return _AttemptResultV8(receipt=receipt, capture_error=exc)

    async def _wait_for_bridge(
        self,
        slot: _SymbolBridgeStateV8,
        cycle: _RegisteredCycleStateV8,
        *,
        bridge_attempt: int,
        terminal: _SemanticAttemptV8,
        initial_range_summary: DepthBridgeRangeSummaryV8,
        initial_discarded: int,
        cancellation_error: asyncio.CancelledError | None,
    ) -> Literal["accepted", "stale", "timeout", "terminal"]:
        loop = asyncio.get_running_loop()
        wait_started_ns = int(loop.time() * 1_000_000_000)
        wait_deadline_ns = wait_started_ns + (
            self._depth_plan.bridge_wait_timeout_ms * _NANOSECONDS_PER_MILLISECOND
        )
        self._append_classified_attempt(
            cycle,
            bridge_attempt=bridge_attempt,
            classification=DepthBridgeAttemptClassificationV8.WAITING,
            terminal=terminal,
            target_update_id=terminal.snapshot.last_update_id,
            discarded_range_count=initial_discarded,
            range_summary=initial_range_summary,
            wait_started_monotonic_ns=wait_started_ns,
            wait_deadline_monotonic_ns=wait_deadline_ns,
        )
        if cancellation_error is not None:
            abort = self._cycle_abort(slot, cycle) or "owner_stopped"
            summary = build_depth_bridge_range_summary_v8(
                tuple(slot.ranges),
                symbol=slot.symbol,
            )
            self._append_wait_terminal(
                cycle,
                bridge_attempt=bridge_attempt,
                outcome=_wait_outcome_from_abort(abort),
                wait_started_ns=wait_started_ns,
                wait_deadline_ns=wait_deadline_ns,
                wait_ended_ns=wait_started_ns,
                target_update_id=terminal.snapshot.last_update_id,
                discarded_range_count=0,
                range_summary=summary,
            )
            self._terminal_cycle_from_abort(cycle, abort)
            raise cancellation_error

        while True:
            abort = self._cycle_abort(slot, cycle)
            if abort is not None:
                summary = build_depth_bridge_range_summary_v8(
                    tuple(slot.ranges),
                    symbol=slot.symbol,
                )
                wait_outcome = _wait_outcome_from_abort(abort)
                ended_ns = int(loop.time() * 1_000_000_000)
                self._append_wait_terminal(
                    cycle,
                    bridge_attempt=bridge_attempt,
                    outcome=wait_outcome,
                    wait_started_ns=wait_started_ns,
                    wait_deadline_ns=wait_deadline_ns,
                    wait_ended_ns=ended_ns,
                    target_update_id=terminal.snapshot.last_update_id,
                    discarded_range_count=0,
                    range_summary=summary,
                )
                self._terminal_cycle_from_abort(cycle, abort)
                return "terminal"

            decision = self._classify_ranges(slot, terminal.snapshot.last_update_id)
            if decision.status != "waiting":
                summary, discarded = self._commit_decision_ranges(slot, decision)
                outcome = (
                    DepthBridgeWaitOutcomeV8.ACCEPTED
                    if decision.status == "accepted"
                    else DepthBridgeWaitOutcomeV8.STALE
                )
                self._append_wait_terminal(
                    cycle,
                    bridge_attempt=bridge_attempt,
                    outcome=outcome,
                    wait_started_ns=wait_started_ns,
                    wait_deadline_ns=wait_deadline_ns,
                    wait_ended_ns=int(loop.time() * 1_000_000_000),
                    target_update_id=decision.target_update_id,
                    discarded_range_count=discarded,
                    range_summary=summary,
                )
                if decision.status == "accepted":
                    self._terminal_accepted(
                        cycle,
                        bridge_attempt=bridge_attempt,
                        terminal=terminal,
                        target_update_id=decision.target_update_id,
                        range_summary=summary,
                    )
                    return "accepted"
                return "stale"

            remaining = (wait_deadline_ns / 1_000_000_000) - loop.time()
            if remaining <= 0:
                summary = build_depth_bridge_range_summary_v8(
                    tuple(slot.ranges),
                    symbol=slot.symbol,
                )
                self._append_wait_terminal(
                    cycle,
                    bridge_attempt=bridge_attempt,
                    outcome=DepthBridgeWaitOutcomeV8.TIMEOUT,
                    wait_started_ns=wait_started_ns,
                    wait_deadline_ns=wait_deadline_ns,
                    wait_ended_ns=max(
                        wait_deadline_ns,
                        int(loop.time() * 1_000_000_000),
                    ),
                    target_update_id=decision.target_update_id,
                    discarded_range_count=0,
                    range_summary=summary,
                )
                return "timeout"
            slot.changed.clear()
            if self._cycle_abort(slot, cycle) is not None:
                continue
            try:
                await asyncio.wait_for(slot.changed.wait(), timeout=remaining)
            except TimeoutError:
                continue

    def _classify_semantic_attempt(
        self,
        receipt: PublicDepthRestAdmissionReceiptV8,
    ) -> _SemanticAttemptV8 | None:
        record = validate_public_depth_rest_admission_receipt_v8(
            receipt,
            plan=self._depth_plan,
        )
        payload = PublicDepthRestAttemptPayloadV8.from_canonical_bytes(
            record.payload_bytes(),
            plan=self._depth_plan,
        )
        try:
            snapshot = verify_admitted_public_depth_rest_snapshot_v8(
                receipt,
                plan=self._depth_plan,
            )
        except PublicDepthRestSnapshotSemanticErrorV8:
            return None
        validate_verified_public_depth_rest_snapshot_v8(snapshot)
        return _SemanticAttemptV8(
            receipt=receipt,
            payload=payload,
            snapshot=snapshot,
            rest_source=DepthBridgeRestSourceLocatorV8(
                symbol=snapshot.symbol,
                trigger_seq=payload.trigger_seq,
                first_buffered_u=payload.first_buffered_u,
                bridge_attempt=payload.bridge_attempt,
                ingest_seq=record.ingest_seq,
                raw_record_sha256=receipt.queued_record.encoded_sha256,
                attempt_payload_sha256=(public_depth_rest_attempt_payload_sha256_v8(payload)),
                receipt_wall_ms=record.receipt_wall_ms,
                receipt_monotonic_ns=record.receipt_monotonic_ns,
            ),
        )

    def _classify_ranges(
        self,
        slot: _SymbolBridgeStateV8,
        last_update_id: int,
    ):
        try:
            return classify_depth_snapshot_bridge(
                Market.FUTURES,
                last_update_id,
                tuple(
                    (locator.first_update_id, locator.final_update_id) for locator in slot.ranges
                ),
            )
        except DepthSequenceError as exc:
            raise PublicDepthRestBridgeCoordinatorErrorV8(
                "retained depth range ordering is not bridge-classifiable"
            ) from exc

    def _commit_decision_ranges(self, slot, decision):
        locators = tuple(slot.ranges)
        summary = build_depth_bridge_range_summary_v8(
            locators,
            symbol=slot.symbol,
        )
        for _ in range(decision.discarded_range_count):
            slot.ranges.popleft()
        return summary, decision.discarded_range_count

    def _append_classified_attempt(
        self,
        cycle: _RegisteredCycleStateV8,
        *,
        bridge_attempt: int,
        classification: DepthBridgeAttemptClassificationV8,
        terminal: _SemanticAttemptV8,
        target_update_id: int,
        discarded_range_count: int,
        range_summary: DepthBridgeRangeSummaryV8,
        wait_started_monotonic_ns: int | None = None,
        wait_deadline_monotonic_ns: int | None = None,
    ) -> None:
        self._append(
            DepthBridgePhaseV8.ATTEMPT_TERMINAL,
            DepthBridgeAttemptTerminalV8(
                cycle=cycle.cycle,
                bridge_attempt=bridge_attempt,
                classification=classification.value,
                rest_source=terminal.rest_source,
                semantic_admission_sha256=(terminal.snapshot.semantic_admission_sha256),
                last_update_id=terminal.snapshot.last_update_id,
                target_update_id=target_update_id,
                discarded_range_count=discarded_range_count,
                range_summary=range_summary,
                failure_code=None,
                wait_started_monotonic_ns=wait_started_monotonic_ns,
                wait_deadline_monotonic_ns=wait_deadline_monotonic_ns,
            ),
        )

    def _append_failed_attempt(
        self,
        cycle: _RegisteredCycleStateV8,
        *,
        bridge_attempt: int,
        failure_code: Literal[
            "http_terminal",
            "semantic_invalid",
            "admission_cancelled",
            "owner_failure",
        ],
        ranges: tuple[DepthBridgeWebSocketSourceLocatorV8, ...],
        rest_source: DepthBridgeRestSourceLocatorV8 | None = None,
    ) -> None:
        self._append(
            DepthBridgePhaseV8.ATTEMPT_TERMINAL,
            DepthBridgeAttemptTerminalV8(
                cycle=cycle.cycle,
                bridge_attempt=bridge_attempt,
                classification=DepthBridgeAttemptClassificationV8.FAILED.value,
                rest_source=rest_source,
                semantic_admission_sha256=None,
                last_update_id=None,
                target_update_id=None,
                discarded_range_count=0,
                range_summary=build_depth_bridge_range_summary_v8(
                    ranges,
                    symbol=cycle.cycle.symbol,
                ),
                failure_code=failure_code,
                wait_started_monotonic_ns=None,
                wait_deadline_monotonic_ns=None,
            ),
        )

    def _append_wait_terminal(
        self,
        cycle: _RegisteredCycleStateV8,
        *,
        bridge_attempt: int,
        outcome: DepthBridgeWaitOutcomeV8,
        wait_started_ns: int,
        wait_deadline_ns: int,
        wait_ended_ns: int,
        target_update_id: int,
        discarded_range_count: int,
        range_summary: DepthBridgeRangeSummaryV8,
    ) -> None:
        self._append(
            DepthBridgePhaseV8.WAIT_TERMINAL,
            DepthBridgeWaitTerminalV8(
                cycle=cycle.cycle,
                bridge_attempt=bridge_attempt,
                outcome=outcome.value,
                wait_started_monotonic_ns=wait_started_ns,
                wait_deadline_monotonic_ns=wait_deadline_ns,
                wait_ended_monotonic_ns=wait_ended_ns,
                target_update_id=target_update_id,
                discarded_range_count=discarded_range_count,
                range_summary=range_summary,
            ),
        )

    def _terminal_accepted(
        self,
        cycle: _RegisteredCycleStateV8,
        *,
        bridge_attempt: int,
        terminal: _SemanticAttemptV8,
        target_update_id: int,
        range_summary: DepthBridgeRangeSummaryV8,
    ) -> None:
        abort = self._cycle_abort(
            self._slots[cycle.registration.symbol_ordinal],
            cycle,
        )
        if abort is not None:
            self._terminal_cycle_from_abort(cycle, abort)
            return
        self._terminal_cycle(
            cycle,
            outcome=DepthBridgeCycleOutcomeV8.ACCEPTED,
            reason="snapshot_range_bridge",
            terminal_bridge_attempt=bridge_attempt,
            semantic_admission_sha256=terminal.snapshot.semantic_admission_sha256,
            target_update_id=target_update_id,
            bridging_range_summary=range_summary,
        )

    def _terminal_cycle_from_abort(
        self,
        cycle: _RegisteredCycleStateV8,
        abort: _WaitAbortV8,
    ) -> None:
        if abort == "superseded":
            outcome = DepthBridgeCycleOutcomeV8.SUPERSEDED
            reason: _CycleTerminalReasonV8 = "newer_trigger"
        elif abort == "generation_draining":
            outcome = DepthBridgeCycleOutcomeV8.SUPERSEDED
            reason = "generation_draining"
        elif abort == "owner_stopped":
            outcome = DepthBridgeCycleOutcomeV8.FAILED
            reason = "owner_stopped_unresolved"
        elif abort == "range_buffer_overflow":
            outcome = DepthBridgeCycleOutcomeV8.FAILED
            reason = "range_buffer_overflow"
        else:
            outcome = DepthBridgeCycleOutcomeV8.FAILED
            reason = "coordinator_fatal"
        self._terminal_cycle(
            cycle,
            outcome=outcome,
            reason=reason,
            terminal_bridge_attempt=cycle.last_bridge_attempt,
        )

    def _terminal_cycle(
        self,
        cycle: _RegisteredCycleStateV8,
        *,
        outcome: DepthBridgeCycleOutcomeV8,
        reason: _CycleTerminalReasonV8,
        terminal_bridge_attempt: int | None,
        semantic_admission_sha256: str | None = None,
        target_update_id: int | None = None,
        bridging_range_summary: DepthBridgeRangeSummaryV8 | None = None,
    ) -> None:
        if cycle.terminal:
            return
        self._append(
            DepthBridgePhaseV8.CYCLE_TERMINAL,
            DepthBridgeCycleTerminalV8(
                cycle=cycle.cycle,
                outcome=outcome.value,
                reason=reason,
                terminal_bridge_attempt=terminal_bridge_attempt,
                semantic_admission_sha256=semantic_admission_sha256,
                target_update_id=target_update_id,
                bridging_range_summary=bridging_range_summary,
            ),
        )
        cycle.terminal = True
        if outcome is DepthBridgeCycleOutcomeV8.ACCEPTED:
            self._accepted_cycle_count += 1
        elif outcome is DepthBridgeCycleOutcomeV8.SUPERSEDED:
            self._superseded_cycle_count += 1
        else:
            self._failed_cycle_count += 1

    def _cycle_abort(
        self,
        slot: _SymbolBridgeStateV8,
        cycle: _RegisteredCycleStateV8,
    ) -> _WaitAbortV8 | None:
        if cycle.superseded_by_cycle_id is not None:
            return "superseded"
        if slot.range_buffer_overflow:
            return "range_buffer_overflow"
        if self._fatal_error is not None or self._drain_reason == "fatal":
            return "coordinator_fatal"
        if self._drain_reason == "reconnect":
            return "generation_draining"
        if self._drain_reason == "normal_stop":
            return "owner_stopped"
        return None

    async def _drain_generation_locked(
        self,
        reason: DepthBridgeDrainReasonV8,
    ) -> None:
        if not self._generation_open:
            return
        if self._scheduler_generation_retired:
            payload = self._pending_generation_drained_payload
            if payload is None:
                raise PublicDepthRestBridgeCoordinatorErrorV8(
                    "retired depth scheduler lacks its exact pending drain payload"
                )
            self._append_prebuilt_drain_payload(payload)
            self._finalize_generation_after_drain()
            return
        effective_reason: DepthBridgeDrainReasonV8 = (
            "fatal" if self._fatal_error is not None else reason
        )
        self._callbacks_accepting = False
        self._drain_reason = effective_reason
        for slot in self._slots:
            slot.changed.set()
            if slot.active is not None and slot.worker is None:
                self._ensure_worker(slot)

        timeout_seconds = (
            self._depth_plan.request_timeout_ms + self._depth_plan.bridge_wait_timeout_ms
        ) / 1_000 + _DRAIN_GRACE_SECONDS
        tasks = tuple(self._workers.values())
        if tasks:
            try:
                await asyncio.wait_for(
                    asyncio.gather(*tasks, return_exceptions=True),
                    timeout=timeout_seconds,
                )
            except TimeoutError as exc:
                for task in tasks:
                    task.cancel()
                await asyncio.gather(*tasks, return_exceptions=True)
                self._fail_closed(
                    PublicDepthRestBridgeCoordinatorErrorV8(
                        "depth bridge workers exceeded bounded generation drain"
                    )
                )
                effective_reason = "fatal"
                self._drain_reason = "fatal"
                if self._fatal_error is None:
                    self._fatal_error = exc

        self._terminalize_unresolved_slots(effective_reason)
        adapter = self._require_adapter()
        close_error: BaseException | None = None
        try:
            await adapter.aclose()
        except BaseException as exc:
            close_error = exc
            self._fail_closed(exc, code="adapter_failure")
            effective_reason = "fatal"
        if self._workers:
            raise PublicDepthRestBridgeCoordinatorErrorV8(
                "depth bridge retained worker tasks after drain"
            )
        if self._permit_in_use_count != 0:
            raise PublicDepthRestBridgeCoordinatorErrorV8(
                "depth bridge retained permits after drain"
            )
        if (
            self._schedule_authority.claimed_token_count != 0
            or adapter.active_attempt_count != 0
            or adapter.pending_owner_task_count != 0
            or adapter.retained_terminal_admission_count != 0
        ):
            raise PublicDepthRestBridgeCoordinatorErrorV8(
                "depth bridge cannot seal a generation with live owned state"
            )
        session_id = _required_lineage(self._session_id, "session_id")
        protocol_hash = _required_lineage(self._protocol_hash, "protocol_hash")
        connection_id = _required_lineage(self._connection_id, "connection_id")
        self._schedule_authority.retire_current_generation(
            session_id=session_id,
            protocol_hash=protocol_hash,
            connection_id=connection_id,
            connection_generation=self._connection_generation,
        )
        self._scheduler_generation_retired = True
        retained_registration_count = self._schedule_authority.retained_registration_count
        pending_registration_count = self._schedule_authority.pending_registration_count
        retained_token_count = self._schedule_authority.retained_token_count
        claimed_token_count = self._schedule_authority.claimed_token_count
        if any(
            (
                retained_registration_count,
                pending_registration_count,
                retained_token_count,
                claimed_token_count,
            )
        ):
            raise PublicDepthRestBridgeCoordinatorErrorV8(
                "retired depth scheduler retained generation state"
            )
        drain_material = DepthBridgeGenerationDrainedV8(
            reason=effective_reason,
            fatal_cause_code=(self._fatal_cause_code if effective_reason == "fatal" else None),
            fatal_cause_sha256=(self._fatal_cause_sha256 if effective_reason == "fatal" else None),
            registered_cycle_count=self._registered_cycle_count,
            accepted_cycle_count=self._accepted_cycle_count,
            superseded_cycle_count=self._superseded_cycle_count,
            failed_cycle_count=self._failed_cycle_count,
            worker_count=0,
            permit_in_use_count=0,
            retained_registration_count=retained_registration_count,
            pending_registration_count=pending_registration_count,
            retained_token_count=retained_token_count,
            claimed_token_count=claimed_token_count,
            adapter_active_attempt_count=0,
            adapter_pending_owner_task_count=0,
            retained_terminal_admission_count=0,
            adapter_closed=adapter.closed,
            adapter_cleanly_closed=adapter.cleanly_closed,
        )
        payload = self._build_payload(
            DepthBridgePhaseV8.GENERATION_DRAINED,
            drain_material,
        )
        self._pending_generation_drained_payload = payload
        self._append_prebuilt_drain_payload(payload)
        self._finalize_generation_after_drain()
        if close_error is not None and reason != "reconnect":
            return

    def _terminalize_unresolved_slots(
        self,
        reason: DepthBridgeDrainReasonV8,
    ) -> None:
        for slot in self._slots:
            for cycle in (slot.active, slot.pending):
                if cycle is None or cycle.terminal:
                    continue
                if reason == "reconnect":
                    outcome = DepthBridgeCycleOutcomeV8.SUPERSEDED
                    terminal_reason: _CycleTerminalReasonV8 = "generation_draining"
                elif reason == "normal_stop":
                    outcome = DepthBridgeCycleOutcomeV8.FAILED
                    terminal_reason = "owner_stopped_unresolved"
                else:
                    outcome = DepthBridgeCycleOutcomeV8.FAILED
                    terminal_reason = "coordinator_fatal"
                self._terminal_cycle(
                    cycle,
                    outcome=outcome,
                    reason=terminal_reason,
                    terminal_bridge_attempt=cycle.last_bridge_attempt,
                )

    def _reset_generation_state(self) -> None:
        for slot in self._slots:
            slot.ranges.clear()
            slot.changed.clear()
            slot.active = None
            slot.pending = None
            slot.worker = None
            slot.range_buffer_overflow = False
        self._workers.clear()
        self._permit = asyncio.Semaphore(self._depth_plan.maximum_concurrency)
        self._permit_in_use_count = 0
        self._generation_trigger_registered = False
        self._drain_reason = None
        self._registered_cycle_count = 0
        self._accepted_cycle_count = 0
        self._superseded_cycle_count = 0
        self._failed_cycle_count = 0
        self._scheduler_generation_retired = False
        self._pending_generation_drained_payload = None

    def _initial_source(
        self,
        symbol: str,
        first_buffered_u: int,
    ) -> DepthBridgeWebSocketSourceLocatorV8:
        slot = self._slot_by_symbol.get(symbol)
        if slot is None:
            raise ValueError("depth resync symbol is outside the exact census")
        matches = tuple(
            locator for locator in slot.ranges if locator.first_update_id == first_buffered_u
        )
        if len(matches) != 1:
            raise ValueError("depth resync watermark lacks one exact retained initial range")
        return matches[0]

    def _cycle_ref(
        self,
        registration: PublicDepthRestRegisteredCycleV8,
    ) -> DepthBridgeCycleRefV8:
        return build_depth_bridge_cycle_ref_v8(
            session_id=registration.session_id,
            protocol_hash=registration.protocol_hash,
            plan_bundle_sha256=self._plan_bundle_sha256,
            depth_plan_sha256=self._depth_plan_sha256,
            connection_id=registration.connection_id,
            connection_generation=registration.connection_generation,
            symbol=registration.symbol,
            symbol_ordinal=registration.symbol_ordinal,
            trigger_seq=registration.trigger_seq,
            first_buffered_u=registration.first_buffered_u,
        )

    def _validate_retained_callback_lineage(
        self,
        receipt: (
            PublicRetainedDepthRangeCallbackReceiptV2 | PublicRetainedDepthResyncCallbackReceiptV2
        ),
    ) -> None:
        if (
            receipt.session_id != self._session_id
            or receipt.protocol_hash != self._protocol_hash
            or receipt.connection_id != self._connection_id
            or receipt.generation != self._connection_generation
            or receipt.market is not Market.FUTURES
            or receipt.route != "public"
        ):
            raise ValueError("retained depth callback differs from current lineage")

    def _require_callback_generation(self) -> None:
        if self._permanently_closed:
            raise RuntimeError("permanently closed depth bridge rejects callbacks")
        if not self._generation_open or not self._callbacks_accepting:
            raise RuntimeError("depth bridge is not accepting generation callbacks")
        self._observe_external_fatal()
        if self._fatal_error is not None:
            raise PublicDepthRestBridgeCoordinatorErrorV8(
                "depth bridge generation is fatal"
            ) from self._fatal_error

    def _observe_external_fatal(self) -> None:
        try:
            self._fatal_coordinator.raise_if_failed()
        except BaseException as exc:
            self._fail_closed(exc)

    def _require_adapter(self) -> PublicDepthRestCaptureAdapterV8:
        adapter = self._adapter
        if type(adapter) is not PublicDepthRestCaptureAdapterV8:
            raise RuntimeError("depth bridge has no exact generation adapter")
        return adapter

    def _validate_clean_close_zero_state(self) -> None:
        if (
            self._generation_open
            or self._callbacks_accepting
            or self._adapter is not None
            or self._workers
            or self._permit_in_use_count != 0
            or self._schedule_authority.generation_open
            or self._schedule_authority.retained_registration_count != 0
            or self._schedule_authority.pending_registration_count != 0
            or self._schedule_authority.retained_token_count != 0
            or self._schedule_authority.claimed_token_count != 0
        ):
            raise PublicDepthRestBridgeCoordinatorErrorV8(
                "normal bridge close retained live owned state"
            )
        if (
            not self._permanently_closed
            or self._persisted_generation_started_count < 1
            or self._persisted_generation_started_count
            != self._persisted_generation_drained_count
            or self._persisted_fatal_generation_count != 0
        ):
            raise PublicDepthRestBridgeCoordinatorErrorV8(
                "normal bridge close has an incomplete or fatal generation census"
            )

    def _append(
        self,
        phase: DepthBridgePhaseV8,
        material: DepthBridgePhaseMaterialV8,
    ) -> None:
        payload = self._build_payload(phase, material)
        try:
            event = self._ledger.append_depth_bridge_v8(
                payload,
                self._promoting_plans,
                self._depth_plan,
            )
            self._validate_persisted_bridge_event(event, payload)
        except BaseException as exc:
            self._fail_closed(exc, code="ledger_failure")
            raise
        if phase is DepthBridgePhaseV8.GENERATION_STARTED:
            self._persisted_generation_started_count += 1

    def _build_payload(
        self,
        phase: DepthBridgePhaseV8,
        material: DepthBridgePhaseMaterialV8,
    ) -> DepthBridgeEvidencePayloadV8:
        session_id = self._session_id
        protocol_hash = self._protocol_hash
        connection_id = self._connection_id
        if session_id is None or protocol_hash is None or connection_id is None:
            raise RuntimeError("depth bridge persistence lacks generation lineage")
        return build_depth_bridge_evidence_payload_v8(
            phase=phase,
            session_id=session_id,
            protocol_hash=protocol_hash,
            connection_id=connection_id,
            connection_generation=self._connection_generation,
            material=material,
            promoting_plans=self._promoting_plans,
            depth_plan=self._depth_plan,
        )

    def _append_prebuilt_drain_payload(
        self,
        payload: DepthBridgeEvidencePayloadV8,
    ) -> None:
        if payload is not self._pending_generation_drained_payload:
            raise RuntimeError("depth bridge drain retry lost exact payload identity")
        try:
            event = self._ledger.append_depth_bridge_v8(
                payload,
                self._promoting_plans,
                self._depth_plan,
            )
            self._validate_persisted_bridge_event(event, payload)
        except BaseException as exc:
            self._fail_closed(exc, code="ledger_failure")
            raise
        material = payload.material
        if type(material) is not DepthBridgeGenerationDrainedV8:
            raise RuntimeError("depth bridge drain payload lost its exact material")
        self._persisted_generation_drained_count += 1
        if material.reason == "fatal":
            self._persisted_fatal_generation_count += 1
        self._last_generation_drained_event = event
        self._last_generation_drain_reason = cast(
            DepthBridgeDrainReasonV8,
            material.reason,
        )

    @staticmethod
    def _validate_persisted_bridge_event(
        event: CaptureIntegrityEventV2,
        payload: DepthBridgeEvidencePayloadV8,
    ) -> None:
        if type(event) is not CaptureIntegrityEventV2:
            raise TypeError(
                "depth bridge ledger append must return an exact integrity event"
            )
        if (
            event.event_type != DEPTH_BRIDGE_EVENT_TYPE_V8
            or event.event_sequence < 1
            or canonical_json_line(event.payload)
            != canonical_json_line(asdict(payload))
        ):
            raise PublicDepthRestBridgeCoordinatorErrorV8(
                "depth bridge ledger returned a foreign persisted event"
            )

    def _finalize_generation_after_drain(self) -> None:
        self._generation_open = False
        self._adapter = None
        self._drain_reason = None
        self._pending_generation_drained_payload = None

    def _consume_worker_completion(
        self,
        slot: _SymbolBridgeStateV8,
        task: asyncio.Task[None],
    ) -> None:
        if self._workers.get(slot.symbol) is task:
            del self._workers[slot.symbol]
        if slot.worker is task:
            slot.worker = None
        if task.cancelled():
            self._fail_closed(
                asyncio.CancelledError(
                    f"depth bridge worker was cancelled before cleanup: {slot.symbol}"
                )
            )
            return
        exception = task.exception()
        if exception is not None:
            self._fail_closed(exception)

    def _fail_closed(
        self,
        cause: BaseException,
        *,
        code: _FatalCauseCodeV8 = "coordinator_failure",
    ) -> None:
        if self._fatal_error is None:
            self._fatal_error = cause
            self._fatal_cause_code = code
            type_name = f"{type(cause).__module__}.{type(cause).__qualname__}"
            self._fatal_cause_sha256 = hashlib.sha256(f"{code}\0{type_name}".encode()).hexdigest()
            try:
                self._fatal_coordinator.trip_fatal(cause)
            except BaseException as fatal_error:
                self._fatal_error = fatal_error
        for slot in self._slots:
            slot.changed.set()


def _websocket_source_locator(
    receipt: PublicRetainedDepthRangeCallbackReceiptV2,
) -> DepthBridgeWebSocketSourceLocatorV8:
    observation = receipt.observation
    return DepthBridgeWebSocketSourceLocatorV8(
        symbol=observation.symbol,
        frame_seq=receipt.frame_seq,
        ingest_seq=receipt.ingest_seq,
        raw_payload_sha256=receipt.raw_payload_sha256,
        receipt_wall_ms=receipt.receipt_wall_ms,
        receipt_monotonic_ns=receipt.receipt_monotonic_ns,
        first_update_id=observation.U,
        final_update_id=observation.u,
        reset=observation.reset,
    )


def _rest_source_from_receipt(
    receipt: PublicDepthRestAdmissionReceiptV8,
    *,
    plan: ProvisionalDepthRestQualificationPlanV8,
) -> DepthBridgeRestSourceLocatorV8:
    record = validate_public_depth_rest_admission_receipt_v8(receipt, plan=plan)
    payload = PublicDepthRestAttemptPayloadV8.from_canonical_bytes(
        record.payload_bytes(),
        plan=plan,
    )
    return DepthBridgeRestSourceLocatorV8(
        symbol=payload.symbol,
        trigger_seq=payload.trigger_seq,
        first_buffered_u=payload.first_buffered_u,
        bridge_attempt=payload.bridge_attempt,
        ingest_seq=record.ingest_seq,
        raw_record_sha256=receipt.queued_record.encoded_sha256,
        attempt_payload_sha256=public_depth_rest_attempt_payload_sha256_v8(payload),
        receipt_wall_ms=record.receipt_wall_ms,
        receipt_monotonic_ns=record.receipt_monotonic_ns,
    )


def _wait_outcome_from_abort(abort: _WaitAbortV8) -> DepthBridgeWaitOutcomeV8:
    if abort == "superseded":
        return DepthBridgeWaitOutcomeV8.SUPERSEDED
    if abort == "generation_draining":
        return DepthBridgeWaitOutcomeV8.GENERATION_DRAINING
    if abort == "owner_stopped":
        return DepthBridgeWaitOutcomeV8.OWNER_STOPPED
    if abort in ("coordinator_fatal", "range_buffer_overflow"):
        return DepthBridgeWaitOutcomeV8.OWNER_STOPPED
    raise AssertionError("unreachable depth bridge wait abort")


def _propagate_capture_cancellation(result: _AttemptResultV8) -> None:
    error = result.capture_error
    if isinstance(error, asyncio.CancelledError):
        raise error


def _validate_clock(clock: ReceiptClock) -> None:
    capture = getattr(clock, "capture", None)
    if not callable(capture) or inspect.iscoroutinefunction(capture):
        raise TypeError("depth bridge clock must expose synchronous capture")


def _validate_fatal_coordinator(
    coordinator: RestCaptureFatalCoordinatorV2,
) -> None:
    for name in ("trip_fatal", "raise_if_failed"):
        method = getattr(coordinator, name, None)
        if not callable(method) or inspect.iscoroutinefunction(method):
            raise TypeError(f"depth bridge fatal coordinator requires synchronous {name}")


def _required_lineage(value: str | None, field_name: str) -> str:
    if value is None:
        raise RuntimeError(f"depth bridge drain lacks {field_name}")
    return value
