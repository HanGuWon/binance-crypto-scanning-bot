"""Sealed admission boundary for one production V2 public WebSocket owner."""

from __future__ import annotations

import asyncio
import os
import threading
from contextlib import AbstractContextManager
from dataclasses import dataclass, field
from pathlib import Path
from typing import cast

from signalbot.capture.receipts import ReceiptClock, ReceiptTimestamp
from signalbot.capture.writer_lease import WriterLease
from signalbot.capture.ws_owner import PublicWebSocketCaptureOwner
from signalbot.exchange.binance.endpoints import WebSocketPlan
from signalbot.r4b_v2.capture.authority import StorageRootOpenedIdentityV2
from signalbot.r4b_v2.capture.blocks import GroupedBlockWriterV2
from signalbot.r4b_v2.capture.integrity_ledger import CaptureIntegrityLedgerV2
from signalbot.r4b_v2.capture.mirrored_wal import MirroredWalWriterV2
from signalbot.r4b_v2.capture.pipeline import (
    CaptureBatchPipelineV2,
    DurableCaptureBatchWriterV2,
)
from signalbot.r4b_v2.capture.plans import (
    ProvisionalPromotingCapturePlanV2,
    ProvisionalPromotingPlanV2,
    ProvisionalPromotingPlanV8,
    ProvisionalPromotingRestCapturePlanV2,
    provisional_promoting_plan_sha256_v2,
    provisional_promoting_plan_sha256_v8,
    validate_provisional_promoting_capture_plans_v2,
    validate_provisional_promoting_capture_plans_v8,
)
from signalbot.r4b_v2.capture.session import (
    PersistedSessionStartAuthorityV2,
    SessionStartManifestV2,
    SessionStorageRootReferenceV2,
    assert_persisted_session_start_authority_current_v2,
)
from signalbot.r4b_v2.capture.websocket import (
    PublicWebSocketCaptureAdapterV2,
    PublicWebSocketFrameAdapterFactoryV2,
    SharedWebSocketIngressV2,
    build_public_websocket_owner_plan_v2,
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


class PublicWebSocketCompositionErrorV2(RuntimeError):
    """Raised before connector admission when V2 owner lineage is not exact."""


class PublicWebSocketRuntimeClaimErrorV2(RuntimeError):
    """Raised when a composition is replayed or owned by another runtime."""


class PublicWebSocketCompositionErrorV8(RuntimeError):
    """Raised before connector admission when full V8 lineage is not exact."""


class PublicWebSocketRuntimeClaimErrorV8(RuntimeError):
    """Raised when a V8 composition is replayed or owned by another runtime."""


_PUBLIC_WEBSOCKET_RUNTIME_RUN_TOKEN_FACTORY = object()
_PUBLIC_WEBSOCKET_RUNTIME_RUN_TOKEN_FACTORY_V8 = object()
_PUBLIC_WEBSOCKET_RUNTIME_START_BARRIER_FACTORY_V8 = object()
_PUBLIC_WEBSOCKET_ACTIVE_RUNTIME_VALIDATION_V8 = object()
_PUBLIC_WEBSOCKET_FRAME_FACTORY_V8 = object()
_PUBLIC_WEBSOCKET_COMPOSITION_V8 = object()


@dataclass(frozen=True, slots=True, init=False)
class PublicWebSocketFrameAdapterFactoryV8:
    """Factory-sealed V8 boundary delegating frame mechanics to unchanged V2."""

    _delegate: PublicWebSocketFrameAdapterFactoryV2 = field(
        repr=False,
        compare=False,
    )
    _factory_seal: object = field(repr=False, compare=False)

    def __init__(
        self,
        plan: ProvisionalPromotingCapturePlanV2,
        *,
        session_id: str,
        protocol_hash: str,
        clock: ReceiptClock,
        ingress: SharedWebSocketIngressV2,
        recovery_lifecycle: WebSocketLifecycleFatalCoordinatorV8,
        _factory_token: object | None = None,
    ) -> None:
        if _factory_token is not _PUBLIC_WEBSOCKET_FRAME_FACTORY_V8:
            raise TypeError("PublicWebSocketFrameAdapterFactoryV8 is factory-sealed")
        if type(plan) is not ProvisionalPromotingCapturePlanV2:
            raise TypeError("V8 frame factory requires an exact WebSocket plan")
        if type(recovery_lifecycle) is not WebSocketLifecycleFatalCoordinatorV8:
            raise TypeError("V8 frame factory requires the exact V8 lifecycle")
        delegate = PublicWebSocketFrameAdapterFactoryV2(
            plan,
            session_id=session_id,
            protocol_hash=protocol_hash,
            clock=clock,
            ingress=ingress,
            recovery_lifecycle=recovery_lifecycle,
        )
        object.__setattr__(self, "_delegate", delegate)
        object.__setattr__(self, "_factory_seal", _PUBLIC_WEBSOCKET_FRAME_FACTORY_V8)

    @property
    def owner_plan(self) -> WebSocketPlan:
        return self._delegate.owner_plan

    @property
    def plan(self) -> ProvisionalPromotingCapturePlanV2:
        return self._delegate.plan

    @property
    def session_id(self) -> str:
        return self._delegate.session_id

    @property
    def protocol_hash(self) -> str:
        return self._delegate.protocol_hash

    @property
    def clock(self) -> ReceiptClock:
        return self._delegate.clock

    @property
    def ingress(self) -> SharedWebSocketIngressV2:
        return self._delegate.ingress

    @property
    def recovery_lifecycle(self) -> WebSocketLifecycleFatalCoordinatorV8:
        return cast(
            WebSocketLifecycleFatalCoordinatorV8,
            self._delegate.recovery_lifecycle,
        )

    def __call__(
        self,
        *,
        connection_id: str,
        generation: int,
    ) -> PublicWebSocketCaptureAdapterV2:
        return self._delegate(connection_id=connection_id, generation=generation)


def create_public_websocket_frame_adapter_factory_v8(
    plan: ProvisionalPromotingCapturePlanV2,
    *,
    session_id: str,
    protocol_hash: str,
    clock: ReceiptClock,
    ingress: SharedWebSocketIngressV2,
    recovery_lifecycle: WebSocketLifecycleFatalCoordinatorV8,
) -> PublicWebSocketFrameAdapterFactoryV8:
    """Create the only admitted V8 frame-factory boundary."""

    return PublicWebSocketFrameAdapterFactoryV8(
        plan,
        session_id=session_id,
        protocol_hash=protocol_hash,
        clock=clock,
        ingress=ingress,
        recovery_lifecycle=recovery_lifecycle,
        _factory_token=_PUBLIC_WEBSOCKET_FRAME_FACTORY_V8,
    )


@dataclass(frozen=True, slots=True, init=False)
class PublicWebSocketRuntimeRunTokenV2:
    """Factory-only capability for one runtime-owned composition run."""

    _composition: object = field(repr=False, compare=False)
    _runtime_owner: object = field(repr=False, compare=False)
    _factory_token: object = field(repr=False, compare=False)

    def __init__(
        self,
        *,
        composition: PublicWebSocketOwnerCompositionV2,
        runtime_owner: object,
        _factory_token: object,
    ) -> None:
        if _factory_token is not _PUBLIC_WEBSOCKET_RUNTIME_RUN_TOKEN_FACTORY:
            raise TypeError(
                "PublicWebSocketRuntimeRunTokenV2 can only be created by its "
                "composition"
            )
        object.__setattr__(self, "_composition", composition)
        object.__setattr__(self, "_runtime_owner", runtime_owner)
        object.__setattr__(self, "_factory_token", _factory_token)


@dataclass(frozen=True, slots=True, init=False)
class PublicWebSocketRuntimeRunTokenV8:
    """Factory-only capability for one runtime-owned full-V8 run."""

    _composition: object = field(repr=False, compare=False)
    _runtime_owner: object = field(repr=False, compare=False)
    _factory_token: object = field(repr=False, compare=False)

    def __init__(
        self,
        *,
        composition: PublicWebSocketOwnerCompositionV8,
        runtime_owner: object,
        _factory_token: object,
    ) -> None:
        if _factory_token is not _PUBLIC_WEBSOCKET_RUNTIME_RUN_TOKEN_FACTORY_V8:
            raise TypeError(
                "PublicWebSocketRuntimeRunTokenV8 can only be created by its "
                "composition"
            )
        object.__setattr__(self, "_composition", composition)
        object.__setattr__(self, "_runtime_owner", runtime_owner)
        object.__setattr__(self, "_factory_token", _factory_token)


@dataclass(slots=True, init=False)
class PublicWebSocketRuntimeStartBarrierV8:
    """Factory-sealed two-owner gate before either V8 socket may start."""

    _compositions: tuple[
        PublicWebSocketOwnerCompositionV8,
        PublicWebSocketOwnerCompositionV8,
    ]
    _runtime_owner: object
    _arrivals: dict[int, asyncio.Task[object]]
    _release_event: asyncio.Event
    _loop: asyncio.AbstractEventLoop | None
    _failed: bool
    _lock: threading.Lock
    _factory_token: object

    def __init__(
        self,
        *,
        compositions: tuple[
            PublicWebSocketOwnerCompositionV8,
            PublicWebSocketOwnerCompositionV8,
        ],
        runtime_owner: object,
        _factory_token: object,
    ) -> None:
        if _factory_token is not _PUBLIC_WEBSOCKET_RUNTIME_START_BARRIER_FACTORY_V8:
            raise TypeError(
                "PublicWebSocketRuntimeStartBarrierV8 can only be created by "
                "its factory"
            )
        _validate_runtime_start_barrier_members_v8(compositions)
        if runtime_owner is None:
            raise TypeError("runtime_owner must be a concrete object identity")
        self._compositions = compositions
        self._runtime_owner = runtime_owner
        self._arrivals = {}
        self._release_event = asyncio.Event()
        self._loop = None
        self._failed = False
        self._lock = threading.Lock()
        self._factory_token = _factory_token

    def validate_member(
        self,
        composition: PublicWebSocketOwnerCompositionV8,
        *,
        runtime_owner: object,
    ) -> int:
        if self._factory_token is not _PUBLIC_WEBSOCKET_RUNTIME_START_BARRIER_FACTORY_V8:
            raise PublicWebSocketRuntimeClaimErrorV8(
                "V8 runtime start barrier lacks factory provenance"
            )
        _validate_runtime_start_barrier_members_v8(self._compositions)
        if runtime_owner is not self._runtime_owner:
            raise PublicWebSocketRuntimeClaimErrorV8(
                "V8 runtime start barrier belongs to a foreign runtime"
            )
        matches = tuple(
            index
            for index, candidate in enumerate(self._compositions)
            if candidate is composition
        )
        if len(matches) != 1:
            raise PublicWebSocketRuntimeClaimErrorV8(
                "V8 runtime start barrier has a foreign composition"
            )
        return matches[0]

    async def arrive_and_wait(
        self,
        composition: PublicWebSocketOwnerCompositionV8,
        *,
        runtime_owner: object,
    ) -> None:
        """Release only after both exact authorized tasks pass strict preflight."""

        index = self.validate_member(
            composition,
            runtime_owner=runtime_owner,
        )
        task = _current_task()
        if task is None:
            raise PublicWebSocketRuntimeClaimErrorV8(
                "V8 runtime start barrier requires an asyncio task"
            )
        loop = asyncio.get_running_loop()
        with self._lock:
            if self._failed:
                raise PublicWebSocketRuntimeClaimErrorV8(
                    "V8 runtime start barrier is failure-latched"
                )
            if self._loop is None:
                self._loop = loop
            elif self._loop is not loop:
                raise PublicWebSocketRuntimeClaimErrorV8(
                    "V8 runtime start barrier cannot cross event loops"
                )
            if index in self._arrivals or task in self._arrivals.values():
                raise PublicWebSocketRuntimeClaimErrorV8(
                    "V8 runtime start barrier participant was replayed"
                )
            self._arrivals[index] = task
            if len(self._arrivals) == 2:
                self._release_event.set()
        try:
            await self._release_event.wait()
        except asyncio.CancelledError:
            with self._lock:
                if len(self._arrivals) < 2:
                    self._failed = True
                    self._release_event.set()
            raise
        with self._lock:
            if self._failed or len(self._arrivals) != 2:
                raise PublicWebSocketRuntimeClaimErrorV8(
                    "V8 runtime start barrier did not admit both owners"
                )


@dataclass(slots=True)
class _PublicWebSocketRuntimeOwnershipV2:
    lock: threading.Lock = field(default_factory=threading.Lock)
    runtime_owner: object | None = None
    token: PublicWebSocketRuntimeRunTokenV2 | None = None
    direct_run_started: bool = False
    claimed_run_started: bool = False
    authorized_task: asyncio.Task[object] | None = None


@dataclass(frozen=True, slots=True)
class PublicWebSocketOwnerCompositionV2:
    """One exact start-authority/plan/owner/factory/lifecycle admission.

    Construction and every ``run`` both revalidate the complete binding before
    delegating to the existing sole socket owner. The wrapper owns no socket or
    reconnect loop; V1 remains on the unchanged ``PublicWebSocketCaptureOwner``
    path.
    """

    session_start_authority: PersistedSessionStartAuthorityV2
    writer_lease: WriterLease
    promoting_plans: tuple[ProvisionalPromotingPlanV2, ...]
    plan: ProvisionalPromotingCapturePlanV2
    recovered_wal_tail_ingest_seq: int
    owner: PublicWebSocketCaptureOwner
    frame_adapter_factory: PublicWebSocketFrameAdapterFactoryV2
    lifecycle_coordinator: WebSocketLifecycleFatalCoordinatorV2
    _runtime_ownership: _PublicWebSocketRuntimeOwnershipV2 = field(
        default_factory=_PublicWebSocketRuntimeOwnershipV2,
        init=False,
        repr=False,
        compare=False,
    )

    def __post_init__(self) -> None:
        validate_public_websocket_owner_composition_v2(
            session_start_authority=self.session_start_authority,
            writer_lease=self.writer_lease,
            promoting_plans=self.promoting_plans,
            plan=self.plan,
            recovered_wal_tail_ingest_seq=self.recovered_wal_tail_ingest_seq,
            owner=self.owner,
            frame_adapter_factory=self.frame_adapter_factory,
            lifecycle_coordinator=self.lifecycle_coordinator,
        )
        if not self.owner.requires_preconnect_admission:
            raise PublicWebSocketCompositionErrorV2(
                "V2 owner must require preconnect admission"
            )
        if self.owner.preconnect_admission_guard is not None:
            raise PublicWebSocketCompositionErrorV2(
                "V2 owner preconnect admission guard was already bound"
            )
        self.owner.bind_preconnect_admission_guard(self)
        self.validate_current()

    def validate_current(self) -> None:
        """Validate connector admission, including an exclusive runtime task."""

        ownership = self._runtime_ownership
        with ownership.lock:
            if ownership.runtime_owner is not None:
                current_task = _current_task()
                if (
                    not ownership.claimed_run_started
                    or current_task is None
                    or ownership.authorized_task is not current_task
                ):
                    raise PublicWebSocketRuntimeClaimErrorV2(
                        "runtime-claimed WebSocket owner rejects a foreign task"
                    )
        self._validate_lineage_current_v2()

    def _validate_lineage_current_v2(self) -> None:
        """Revalidate lineage without granting connector admission."""

        if not self.owner.requires_preconnect_admission:
            raise PublicWebSocketCompositionErrorV2(
                "V2 owner no longer requires preconnect admission"
            )
        if self.owner.preconnect_admission_guard is not self:
            raise PublicWebSocketCompositionErrorV2(
                "V2 owner is not bound to this exact composition guard"
            )
        validate_public_websocket_owner_composition_v2(
            session_start_authority=self.session_start_authority,
            writer_lease=self.writer_lease,
            promoting_plans=self.promoting_plans,
            plan=self.plan,
            recovered_wal_tail_ingest_seq=self.recovered_wal_tail_ingest_seq,
            owner=self.owner,
            frame_adapter_factory=self.frame_adapter_factory,
            lifecycle_coordinator=self.lifecycle_coordinator,
        )

    def connector_admission_guard(self) -> AbstractContextManager[None]:
        """Exclude concurrent lease release through connector context entry."""

        if self.owner.preconnect_admission_guard is not self:
            raise PublicWebSocketCompositionErrorV2(
                "V2 owner is not bound to this exact composition guard"
            )
        return self.writer_lease.operation_guard()

    async def run(self) -> WebSocketRouteStopReceiptV2 | None:
        """Revalidate immediately before the sole existing owner may connect."""

        self._validate_lineage_current_v2()
        ownership = self._runtime_ownership
        with ownership.lock:
            if ownership.runtime_owner is not None:
                raise PublicWebSocketRuntimeClaimErrorV2(
                    "runtime-claimed WebSocket composition rejects direct run"
                )
            if ownership.direct_run_started:
                raise PublicWebSocketRuntimeClaimErrorV2(
                    "WebSocket composition direct run is one-shot"
                )
            if _owner_generation(self.owner) != 0:
                raise PublicWebSocketRuntimeClaimErrorV2(
                    "WebSocket owner already started outside this composition"
                )
            ownership.direct_run_started = True
        await self.owner.run(self.lifecycle_coordinator.stop_event)
        return self._validated_normal_stop_receipt()

    def claim_exclusive_runtime_v2(
        self,
        runtime_owner: object,
        /,
    ) -> PublicWebSocketRuntimeRunTokenV2:
        """Bind this unstarted composition to exactly one top-level runtime."""

        if runtime_owner is None:
            raise TypeError("runtime_owner must be a concrete object identity")
        self._validate_lineage_current_v2()
        ownership = self._runtime_ownership
        with ownership.lock:
            if ownership.direct_run_started or _owner_generation(self.owner) != 0:
                raise PublicWebSocketRuntimeClaimErrorV2(
                    "WebSocket composition or owner already started"
                )
            if ownership.runtime_owner is not None or ownership.token is not None:
                raise PublicWebSocketRuntimeClaimErrorV2(
                    "WebSocket composition already has an exclusive runtime owner"
                )
            token = PublicWebSocketRuntimeRunTokenV2(
                composition=self,
                runtime_owner=runtime_owner,
                _factory_token=_PUBLIC_WEBSOCKET_RUNTIME_RUN_TOKEN_FACTORY,
            )
            ownership.runtime_owner = runtime_owner
            ownership.token = token
            return token

    def validate_exclusive_runtime_claim_v2(
        self,
        token: PublicWebSocketRuntimeRunTokenV2,
        *,
        runtime_owner: object,
    ) -> None:
        """Revalidate one unconsumed exact runtime claim without starting it."""

        self._validate_lineage_current_v2()
        ownership = self._runtime_ownership
        with ownership.lock:
            _validate_runtime_token_unlocked(
                self,
                ownership,
                token=token,
                runtime_owner=runtime_owner,
            )
            if ownership.claimed_run_started:
                raise PublicWebSocketRuntimeClaimErrorV2(
                    "runtime-owned WebSocket composition already started"
                )
            if _owner_generation(self.owner) != 0:
                raise PublicWebSocketRuntimeClaimErrorV2(
                    "WebSocket owner already started outside its runtime claim"
                )

    async def run_exclusive_runtime_v2(
        self,
        token: PublicWebSocketRuntimeRunTokenV2,
        *,
        runtime_owner: object,
    ) -> WebSocketRouteStopReceiptV2 | None:
        """Consume the exact runtime capability before the owner may connect."""

        self._validate_lineage_current_v2()
        ownership = self._runtime_ownership
        with ownership.lock:
            _validate_runtime_token_unlocked(
                self,
                ownership,
                token=token,
                runtime_owner=runtime_owner,
            )
            if ownership.claimed_run_started:
                raise PublicWebSocketRuntimeClaimErrorV2(
                    "runtime-owned WebSocket composition run is one-shot"
                )
            if _owner_generation(self.owner) != 0:
                raise PublicWebSocketRuntimeClaimErrorV2(
                    "WebSocket owner already started outside its runtime claim"
                )
            current_task = _current_task()
            if current_task is None:
                raise PublicWebSocketRuntimeClaimErrorV2(
                    "runtime-owned WebSocket run requires an asyncio task"
                )
            ownership.claimed_run_started = True
            ownership.authorized_task = current_task
        await self.owner.run(self.lifecycle_coordinator.stop_event)
        return self._validated_normal_stop_receipt()

    def _validated_normal_stop_receipt(self) -> WebSocketRouteStopReceiptV2 | None:
        receipt = self.lifecycle_coordinator.normal_stop_receipt
        if receipt is None:
            return None
        validate_websocket_route_stop_receipt_v2(
            receipt,
            promoting_plans=self.promoting_plans,
            plan=self.plan,
        )
        return receipt

    def _release_exclusive_runtime_claim_v2(
        self,
        token: PublicWebSocketRuntimeRunTokenV2,
        *,
        runtime_owner: object,
    ) -> None:
        """Rollback a constructor transaction before either owner has started."""

        ownership = self._runtime_ownership
        with ownership.lock:
            _validate_runtime_token_unlocked(
                self,
                ownership,
                token=token,
                runtime_owner=runtime_owner,
            )
            if ownership.claimed_run_started:
                raise PublicWebSocketRuntimeClaimErrorV2(
                    "started runtime ownership cannot be released"
                )
            ownership.runtime_owner = None
            ownership.token = None
            ownership.authorized_task = None


@dataclass(slots=True)
class _PublicWebSocketRuntimeOwnershipV8:
    lock: threading.Lock = field(default_factory=threading.Lock)
    runtime_owner: object | None = None
    token: PublicWebSocketRuntimeRunTokenV8 | None = None
    direct_run_started: bool = False
    claimed_run_started: bool = False
    authorized_task: asyncio.Task[object] | None = None


@dataclass(frozen=True, slots=True, init=False)
class PublicWebSocketOwnerCompositionV8:
    """Factory-sealed one-shot admission for the exact four-plan V8 bundle."""

    session_start_authority: PersistedSessionStartAuthorityV2
    writer_lease: WriterLease
    promoting_plans: tuple[ProvisionalPromotingPlanV8, ...]
    plan: ProvisionalPromotingCapturePlanV2
    recovered_wal_tail_ingest_seq: int
    owner: PublicWebSocketCaptureOwner
    frame_adapter_factory: PublicWebSocketFrameAdapterFactoryV8
    lifecycle_coordinator: WebSocketLifecycleFatalCoordinatorV8
    _runtime_ownership: _PublicWebSocketRuntimeOwnershipV8 = field(
        repr=False,
        compare=False,
    )
    _factory_seal: object = field(repr=False, compare=False)

    def __init__(
        self,
        *,
        session_start_authority: PersistedSessionStartAuthorityV2,
        writer_lease: WriterLease,
        promoting_plans: tuple[ProvisionalPromotingPlanV8, ...],
        plan: ProvisionalPromotingCapturePlanV2,
        recovered_wal_tail_ingest_seq: int,
        owner: PublicWebSocketCaptureOwner,
        frame_adapter_factory: PublicWebSocketFrameAdapterFactoryV8,
        lifecycle_coordinator: WebSocketLifecycleFatalCoordinatorV8,
        _factory_token: object | None = None,
    ) -> None:
        if _factory_token is not _PUBLIC_WEBSOCKET_COMPOSITION_V8:
            raise TypeError("PublicWebSocketOwnerCompositionV8 is factory-sealed")
        object.__setattr__(self, "session_start_authority", session_start_authority)
        object.__setattr__(self, "writer_lease", writer_lease)
        object.__setattr__(self, "promoting_plans", promoting_plans)
        object.__setattr__(self, "plan", plan)
        object.__setattr__(
            self,
            "recovered_wal_tail_ingest_seq",
            recovered_wal_tail_ingest_seq,
        )
        object.__setattr__(self, "owner", owner)
        object.__setattr__(self, "frame_adapter_factory", frame_adapter_factory)
        object.__setattr__(self, "lifecycle_coordinator", lifecycle_coordinator)
        object.__setattr__(
            self,
            "_runtime_ownership",
            _PublicWebSocketRuntimeOwnershipV8(),
        )
        object.__setattr__(self, "_factory_seal", _PUBLIC_WEBSOCKET_COMPOSITION_V8)
        validate_public_websocket_owner_composition_v8(
            session_start_authority=session_start_authority,
            writer_lease=writer_lease,
            promoting_plans=promoting_plans,
            plan=plan,
            recovered_wal_tail_ingest_seq=recovered_wal_tail_ingest_seq,
            owner=owner,
            frame_adapter_factory=frame_adapter_factory,
            lifecycle_coordinator=lifecycle_coordinator,
        )
        if not owner.requires_preconnect_admission:
            raise PublicWebSocketCompositionErrorV8(
                "V8 owner must require preconnect admission"
            )
        if owner.preconnect_admission_guard is not None:
            raise PublicWebSocketCompositionErrorV8(
                "V8 owner preconnect admission guard was already bound"
            )
        owner.bind_preconnect_admission_guard(self)
        self.validate_current()

    def validate_current(self) -> None:
        """Validate full-V8 lineage, including an exclusive runtime task."""

        ownership = self._runtime_ownership
        active_runtime_task = False
        with ownership.lock:
            if ownership.runtime_owner is not None:
                current_task = _current_task()
                if (
                    not ownership.claimed_run_started
                    or current_task is None
                    or ownership.authorized_task is not current_task
                ):
                    raise PublicWebSocketRuntimeClaimErrorV8(
                        "runtime-claimed V8 WebSocket owner rejects a foreign task"
                    )
                active_runtime_task = True
        self._validate_lineage_current_v8(
            active_runtime_task=active_runtime_task,
        )

    def _validate_lineage_current_v8(
        self,
        *,
        active_runtime_task: bool = False,
    ) -> None:
        """Revalidate full-V8 lineage without granting connector admission."""

        if self._factory_seal is not _PUBLIC_WEBSOCKET_COMPOSITION_V8:
            raise PublicWebSocketCompositionErrorV8(
                "V8 composition lacks factory provenance"
            )
        if self.owner.preconnect_admission_guard is not self:
            raise PublicWebSocketCompositionErrorV8(
                "V8 owner is not bound to this exact composition guard"
            )
        validate_public_websocket_owner_composition_v8(
            session_start_authority=self.session_start_authority,
            writer_lease=self.writer_lease,
            promoting_plans=self.promoting_plans,
            plan=self.plan,
            recovered_wal_tail_ingest_seq=self.recovered_wal_tail_ingest_seq,
            owner=self.owner,
            frame_adapter_factory=self.frame_adapter_factory,
            lifecycle_coordinator=self.lifecycle_coordinator,
            _active_runtime_validation_token=(
                _PUBLIC_WEBSOCKET_ACTIVE_RUNTIME_VALIDATION_V8
                if active_runtime_task
                else None
            ),
        )

    def connector_admission_guard(self) -> AbstractContextManager[None]:
        """Hold the admitted writer lease across connector context entry."""

        if self.owner.preconnect_admission_guard is not self:
            raise PublicWebSocketCompositionErrorV8(
                "V8 owner is not bound to this exact composition guard"
            )
        return self.writer_lease.operation_guard()

    async def run(self) -> WebSocketRouteStopReceiptV8 | None:
        """Run the sole owner once and retain a full-V8 OWNER_STOP receipt."""

        self._validate_lineage_current_v8()
        ownership = self._runtime_ownership
        with ownership.lock:
            if ownership.runtime_owner is not None:
                raise PublicWebSocketRuntimeClaimErrorV8(
                    "runtime-claimed V8 WebSocket composition rejects direct run"
                )
            if ownership.direct_run_started:
                raise PublicWebSocketCompositionErrorV8(
                    "V8 WebSocket composition run is one-shot"
                )
            if _owner_generation_v8(self.owner) != 0:
                raise PublicWebSocketCompositionErrorV8(
                    "V8 WebSocket owner already started outside this composition"
                )
            ownership.direct_run_started = True
        await self.owner.run(self.lifecycle_coordinator.stop_event)
        return self._validated_normal_stop_receipt()

    def claim_exclusive_runtime_v8(
        self,
        runtime_owner: object,
        /,
    ) -> PublicWebSocketRuntimeRunTokenV8:
        """Bind this unstarted full-V8 composition to one runtime identity."""

        if runtime_owner is None:
            raise TypeError("runtime_owner must be a concrete object identity")
        self._validate_lineage_current_v8()
        ownership = self._runtime_ownership
        with ownership.lock:
            if ownership.direct_run_started or _owner_generation_v8(self.owner) != 0:
                raise PublicWebSocketRuntimeClaimErrorV8(
                    "V8 WebSocket composition or owner already started"
                )
            if ownership.runtime_owner is not None or ownership.token is not None:
                raise PublicWebSocketRuntimeClaimErrorV8(
                    "V8 WebSocket composition already has an exclusive runtime owner"
                )
            token = PublicWebSocketRuntimeRunTokenV8(
                composition=self,
                runtime_owner=runtime_owner,
                _factory_token=_PUBLIC_WEBSOCKET_RUNTIME_RUN_TOKEN_FACTORY_V8,
            )
            ownership.runtime_owner = runtime_owner
            ownership.token = token
            return token

    def validate_exclusive_runtime_claim_v8(
        self,
        token: PublicWebSocketRuntimeRunTokenV8,
        *,
        runtime_owner: object,
    ) -> None:
        """Revalidate one unconsumed exact full-V8 runtime claim."""

        self._validate_lineage_current_v8()
        ownership = self._runtime_ownership
        with ownership.lock:
            _validate_runtime_token_v8_unlocked(
                self,
                ownership,
                token=token,
                runtime_owner=runtime_owner,
            )
            if ownership.claimed_run_started:
                raise PublicWebSocketRuntimeClaimErrorV8(
                    "runtime-owned V8 WebSocket composition already started"
                )
            if _owner_generation_v8(self.owner) != 0:
                raise PublicWebSocketRuntimeClaimErrorV8(
                    "V8 WebSocket owner already started outside its runtime claim"
                )

    async def run_exclusive_runtime_v8(
        self,
        token: PublicWebSocketRuntimeRunTokenV8,
        *,
        runtime_owner: object,
        startup_barrier: PublicWebSocketRuntimeStartBarrierV8 | None = None,
    ) -> WebSocketRouteStopReceiptV8 | None:
        """Consume the exact full-V8 capability in the current asyncio task."""

        self._validate_lineage_current_v8()
        if startup_barrier is not None:
            if type(startup_barrier) is not PublicWebSocketRuntimeStartBarrierV8:
                raise TypeError(
                    "startup_barrier must be an exact "
                    "PublicWebSocketRuntimeStartBarrierV8"
                )
            startup_barrier.validate_member(
                self,
                runtime_owner=runtime_owner,
            )
        ownership = self._runtime_ownership
        with ownership.lock:
            _validate_runtime_token_v8_unlocked(
                self,
                ownership,
                token=token,
                runtime_owner=runtime_owner,
            )
            if ownership.claimed_run_started:
                raise PublicWebSocketRuntimeClaimErrorV8(
                    "runtime-owned V8 WebSocket composition run is one-shot"
                )
            if _owner_generation_v8(self.owner) != 0:
                raise PublicWebSocketRuntimeClaimErrorV8(
                    "V8 WebSocket owner already started outside its runtime claim"
                )
            current_task = _current_task()
            if current_task is None:
                raise PublicWebSocketRuntimeClaimErrorV8(
                    "runtime-owned V8 WebSocket run requires an asyncio task"
                )
            ownership.claimed_run_started = True
            ownership.authorized_task = current_task
        try:
            if startup_barrier is not None:
                await startup_barrier.arrive_and_wait(
                    self,
                    runtime_owner=runtime_owner,
                )
            await self.owner.run(self.lifecycle_coordinator.stop_event)
        finally:
            with ownership.lock:
                ownership.authorized_task = None
        return self._validated_normal_stop_receipt()

    def _release_exclusive_runtime_claim_v8(
        self,
        token: PublicWebSocketRuntimeRunTokenV8,
        *,
        runtime_owner: object,
    ) -> None:
        """Rollback a full-V8 runtime constructor before its owner starts."""

        ownership = self._runtime_ownership
        with ownership.lock:
            _validate_runtime_token_v8_unlocked(
                self,
                ownership,
                token=token,
                runtime_owner=runtime_owner,
            )
            if (
                ownership.claimed_run_started
                or _owner_generation_v8(self.owner) != 0
            ):
                raise PublicWebSocketRuntimeClaimErrorV8(
                    "started V8 runtime ownership cannot be released"
                )
            ownership.runtime_owner = None
            ownership.token = None
            ownership.authorized_task = None

    def _validated_normal_stop_receipt(self) -> WebSocketRouteStopReceiptV8 | None:
        receipt = self.lifecycle_coordinator.normal_stop_receipt_v8
        if receipt is None:
            return None
        validate_websocket_route_stop_receipt_v8(
            receipt,
            promoting_plans=self.promoting_plans,
            plan=self.plan,
        )
        return receipt


def create_public_websocket_owner_composition_v8(
    *,
    session_start_authority: PersistedSessionStartAuthorityV2,
    writer_lease: WriterLease,
    promoting_plans: tuple[ProvisionalPromotingPlanV8, ...],
    plan: ProvisionalPromotingCapturePlanV2,
    recovered_wal_tail_ingest_seq: int,
    owner: PublicWebSocketCaptureOwner,
    frame_adapter_factory: PublicWebSocketFrameAdapterFactoryV8,
    lifecycle_coordinator: WebSocketLifecycleFatalCoordinatorV8,
) -> PublicWebSocketOwnerCompositionV8:
    """Create the only admitted full-authority V8 owner composition."""

    return PublicWebSocketOwnerCompositionV8(
        session_start_authority=session_start_authority,
        writer_lease=writer_lease,
        promoting_plans=promoting_plans,
        plan=plan,
        recovered_wal_tail_ingest_seq=recovered_wal_tail_ingest_seq,
        owner=owner,
        frame_adapter_factory=frame_adapter_factory,
        lifecycle_coordinator=lifecycle_coordinator,
        _factory_token=_PUBLIC_WEBSOCKET_COMPOSITION_V8,
    )


def create_public_websocket_runtime_start_barrier_v8(
    compositions: tuple[
        PublicWebSocketOwnerCompositionV8,
        PublicWebSocketOwnerCompositionV8,
    ],
    *,
    runtime_owner: object,
) -> PublicWebSocketRuntimeStartBarrierV8:
    """Create one bounded canonical two-owner start gate for a V8 runtime."""

    return PublicWebSocketRuntimeStartBarrierV8(
        compositions=compositions,
        runtime_owner=runtime_owner,
        _factory_token=_PUBLIC_WEBSOCKET_RUNTIME_START_BARRIER_FACTORY_V8,
    )


def _validate_runtime_start_barrier_members_v8(
    compositions: tuple[
        PublicWebSocketOwnerCompositionV8,
        PublicWebSocketOwnerCompositionV8,
    ],
) -> None:
    if type(compositions) is not tuple or len(compositions) != 2:
        raise TypeError("V8 runtime start barrier requires an exact pair")
    if any(
        type(composition) is not PublicWebSocketOwnerCompositionV8
        for composition in compositions
    ):
        raise TypeError("V8 runtime start barrier requires exact V8 compositions")
    market, public = compositions
    if market is public or market.owner is public.owner:
        raise ValueError("V8 runtime start barrier requires two distinct owners")
    if (market.plan.route_id, public.plan.route_id) != (
        "usdm_market",
        "usdm_public",
    ):
        raise ValueError("V8 runtime start barrier requires canonical route order")
    if market.promoting_plans is not public.promoting_plans:
        raise ValueError("V8 runtime start barrier requires one plan tuple identity")


def _validate_runtime_token_unlocked(
    composition: PublicWebSocketOwnerCompositionV2,
    ownership: _PublicWebSocketRuntimeOwnershipV2,
    *,
    token: PublicWebSocketRuntimeRunTokenV2,
    runtime_owner: object,
) -> None:
    if type(token) is not PublicWebSocketRuntimeRunTokenV2:
        raise TypeError("token must be an exact PublicWebSocketRuntimeRunTokenV2")
    if (
        token._factory_token is not _PUBLIC_WEBSOCKET_RUNTIME_RUN_TOKEN_FACTORY
        or token._composition is not composition
        or token._runtime_owner is not runtime_owner
        or ownership.runtime_owner is not runtime_owner
        or ownership.token is not token
    ):
        raise PublicWebSocketRuntimeClaimErrorV2(
            "WebSocket runtime token is foreign, stale, or replayed"
        )


def _validate_runtime_token_v8_unlocked(
    composition: PublicWebSocketOwnerCompositionV8,
    ownership: _PublicWebSocketRuntimeOwnershipV8,
    *,
    token: PublicWebSocketRuntimeRunTokenV8,
    runtime_owner: object,
) -> None:
    if type(token) is not PublicWebSocketRuntimeRunTokenV8:
        raise TypeError("token must be an exact PublicWebSocketRuntimeRunTokenV8")
    if (
        token._factory_token is not _PUBLIC_WEBSOCKET_RUNTIME_RUN_TOKEN_FACTORY_V8
        or token._composition is not composition
        or token._runtime_owner is not runtime_owner
        or ownership.runtime_owner is not runtime_owner
        or ownership.token is not token
    ):
        raise PublicWebSocketRuntimeClaimErrorV8(
            "V8 WebSocket runtime token is foreign, stale, or replayed"
        )


def _owner_generation(owner: PublicWebSocketCaptureOwner) -> int:
    generation = owner.generation
    if type(generation) is not int or generation < 0:
        raise PublicWebSocketRuntimeClaimErrorV2(
            "WebSocket owner lacks an auditable generation cursor"
        )
    return generation


def _owner_generation_v8(owner: PublicWebSocketCaptureOwner) -> int:
    generation = owner.generation
    if type(generation) is not int or generation < 0:
        raise PublicWebSocketRuntimeClaimErrorV8(
            "V8 WebSocket owner lacks an auditable generation cursor"
        )
    return generation


def _current_task() -> asyncio.Task[object] | None:
    try:
        task = asyncio.current_task()
    except RuntimeError:
        return None
    return task


def validate_public_websocket_owner_composition_v2(
    *,
    session_start_authority: PersistedSessionStartAuthorityV2,
    writer_lease: WriterLease,
    promoting_plans: tuple[ProvisionalPromotingPlanV2, ...],
    plan: ProvisionalPromotingCapturePlanV2,
    recovered_wal_tail_ingest_seq: int,
    owner: PublicWebSocketCaptureOwner,
    frame_adapter_factory: PublicWebSocketFrameAdapterFactoryV2,
    lifecycle_coordinator: WebSocketLifecycleFatalCoordinatorV2,
) -> None:
    """Fail closed on any cross-layer drift before a connector can be called."""

    if type(session_start_authority) is not PersistedSessionStartAuthorityV2:
        raise TypeError(
            "session_start_authority must be an exact "
            "PersistedSessionStartAuthorityV2"
        )
    if type(writer_lease) is not WriterLease:
        raise TypeError("writer_lease must be a WriterLease")
    writer_lease.assert_held()
    if type(promoting_plans) is not tuple:
        raise TypeError("promoting_plans must be the exact immutable plan bundle")
    allowed_plan_types = (
        ProvisionalPromotingCapturePlanV2,
        ProvisionalPromotingRestCapturePlanV2,
    )
    if any(type(candidate) not in allowed_plan_types for candidate in promoting_plans):
        raise TypeError("promoting_plans must contain exact ProvisionalPromotingPlanV2 values")
    if type(plan) is not ProvisionalPromotingCapturePlanV2:
        raise TypeError("plan must be a promoting V2 WebSocket plan")
    if type(recovered_wal_tail_ingest_seq) is not int or recovered_wal_tail_ingest_seq < 0:
        raise ValueError("recovered WAL tail ingest sequence must be nonnegative")
    if type(owner) is not PublicWebSocketCaptureOwner:
        raise TypeError("owner must be the existing PublicWebSocketCaptureOwner")
    if type(frame_adapter_factory) is not PublicWebSocketFrameAdapterFactoryV2:
        raise TypeError("frame_adapter_factory must be the sealed V2 factory")
    if type(lifecycle_coordinator) is not WebSocketLifecycleFatalCoordinatorV2:
        raise TypeError("lifecycle_coordinator must be the sealed V2 coordinator")
    if not owner.requires_preconnect_admission:
        raise PublicWebSocketCompositionErrorV2(
            "V2 owner must require preconnect admission"
        )

    session_start = session_start_authority.manifest
    _validate_writer_lease(session_start=session_start, writer_lease=writer_lease)
    assert_persisted_session_start_authority_current_v2(
        session_start_authority,
        lease=writer_lease,
    )
    validate_provisional_promoting_capture_plans_v2(promoting_plans)
    if sum(candidate == plan for candidate in promoting_plans) != 1:
        raise PublicWebSocketCompositionErrorV2(
            "selected WebSocket plan must occur exactly once in the authority bundle"
        )
    plan_sha256 = provisional_promoting_plan_sha256_v2(promoting_plans)
    if plan_sha256 != session_start.wal_authority.plan_sha256:
        raise PublicWebSocketCompositionErrorV2(
            "plan bundle differs from the session-start WAL authority"
        )

    expected_owner_plan = build_public_websocket_owner_plan_v2(plan)
    if owner.plan != expected_owner_plan:
        raise PublicWebSocketCompositionErrorV2(
            "owner URL or route plan differs from the selected V2 plan"
        )
    if frame_adapter_factory.owner_plan != expected_owner_plan:
        raise PublicWebSocketCompositionErrorV2(
            "frame factory owner plan differs from the selected V2 plan"
        )
    if frame_adapter_factory.plan != plan:
        raise PublicWebSocketCompositionErrorV2(
            "frame factory route metadata differs from the selected V2 plan"
        )
    if lifecycle_coordinator.plan != plan:
        raise PublicWebSocketCompositionErrorV2(
            "lifecycle route metadata differs from the selected V2 plan"
        )
    if lifecycle_coordinator.promoting_plans != promoting_plans:
        raise PublicWebSocketCompositionErrorV2(
            "lifecycle authority bundle differs from the exact V2 plan bundle"
        )
    if owner.frame_adapter_factory is not frame_adapter_factory:
        raise PublicWebSocketCompositionErrorV2(
            "owner is not wired to the admitted V2 frame factory"
        )
    if owner.lifecycle_coordinator is not lifecycle_coordinator:
        raise PublicWebSocketCompositionErrorV2(
            "owner is not wired to the admitted V2 lifecycle coordinator"
        )
    if frame_adapter_factory.recovery_lifecycle is not lifecycle_coordinator:
        raise PublicWebSocketCompositionErrorV2(
            "frame factory is not wired to the admitted V2 lifecycle coordinator"
        )

    _validate_lineage(
        session_start=session_start,
        owner=owner,
        factory=frame_adapter_factory,
        lifecycle=lifecycle_coordinator,
    )
    _validate_capture_authority(
        session_start=session_start,
        writer_lease=writer_lease,
        recovered_wal_tail_ingest_seq=recovered_wal_tail_ingest_seq,
        factory=frame_adapter_factory,
        lifecycle=lifecycle_coordinator,
    )
    writer_lease.assert_held()


def validate_public_websocket_owner_composition_v8(
    *,
    session_start_authority: PersistedSessionStartAuthorityV2,
    writer_lease: WriterLease,
    promoting_plans: tuple[ProvisionalPromotingPlanV8, ...],
    plan: ProvisionalPromotingCapturePlanV2,
    recovered_wal_tail_ingest_seq: int,
    owner: PublicWebSocketCaptureOwner,
    frame_adapter_factory: PublicWebSocketFrameAdapterFactoryV8,
    lifecycle_coordinator: WebSocketLifecycleFatalCoordinatorV8,
    _active_runtime_validation_token: object | None = None,
) -> None:
    """Fail closed unless every live seam retains the exact four-plan authority."""

    if (
        _active_runtime_validation_token is not None
        and _active_runtime_validation_token
        is not _PUBLIC_WEBSOCKET_ACTIVE_RUNTIME_VALIDATION_V8
    ):
        raise TypeError("V8 active-runtime validation token is foreign")
    active_runtime_task = (
        _active_runtime_validation_token
        is _PUBLIC_WEBSOCKET_ACTIVE_RUNTIME_VALIDATION_V8
    )

    if type(session_start_authority) is not PersistedSessionStartAuthorityV2:
        raise TypeError(
            "session_start_authority must be an exact "
            "PersistedSessionStartAuthorityV2"
        )
    if type(writer_lease) is not WriterLease:
        raise TypeError("writer_lease must be a WriterLease")
    writer_lease.assert_held()
    if type(promoting_plans) is not tuple:
        raise TypeError("V8 promoting_plans must be the exact immutable tuple")
    validate_provisional_promoting_capture_plans_v8(promoting_plans)
    if type(plan) is not ProvisionalPromotingCapturePlanV2:
        raise TypeError("V8 plan must be an exact promoting WebSocket plan")
    if sum(candidate is plan for candidate in promoting_plans) != 1:
        raise PublicWebSocketCompositionErrorV8(
            "selected V8 WebSocket plan must be its exact authority object"
        )
    if (
        type(recovered_wal_tail_ingest_seq) is not int
        or recovered_wal_tail_ingest_seq < 0
    ):
        raise ValueError("recovered WAL tail ingest sequence must be nonnegative")
    if type(owner) is not PublicWebSocketCaptureOwner:
        raise TypeError("owner must be the existing PublicWebSocketCaptureOwner")
    if type(frame_adapter_factory) is not PublicWebSocketFrameAdapterFactoryV8:
        raise TypeError("frame_adapter_factory must be the sealed V8 factory")
    if (
        frame_adapter_factory._factory_seal
        is not _PUBLIC_WEBSOCKET_FRAME_FACTORY_V8
    ):
        raise PublicWebSocketCompositionErrorV8(
            "V8 frame factory lacks factory provenance"
        )
    if type(frame_adapter_factory._delegate) is not PublicWebSocketFrameAdapterFactoryV2:
        raise PublicWebSocketCompositionErrorV8(
            "V8 frame factory lacks its exact mechanical delegate"
        )
    if type(lifecycle_coordinator) is not WebSocketLifecycleFatalCoordinatorV8:
        raise TypeError("lifecycle_coordinator must be the exact V8 coordinator")
    if not owner.requires_preconnect_admission:
        raise PublicWebSocketCompositionErrorV8(
            "V8 owner must require preconnect admission"
        )

    session_start = session_start_authority.manifest
    try:
        _validate_writer_lease(
            session_start=session_start,
            writer_lease=writer_lease,
        )
        assert_persisted_session_start_authority_current_v2(
            session_start_authority,
            lease=writer_lease,
        )
    except PublicWebSocketCompositionErrorV2 as exc:
        raise PublicWebSocketCompositionErrorV8(str(exc)) from exc

    plan_sha256 = provisional_promoting_plan_sha256_v8(promoting_plans)
    if plan_sha256 != session_start.wal_authority.plan_sha256:
        raise PublicWebSocketCompositionErrorV8(
            "V8 plan bundle differs from the session-start WAL authority"
        )
    expected_owner_plan = build_public_websocket_owner_plan_v2(plan)
    if owner.plan != expected_owner_plan:
        raise PublicWebSocketCompositionErrorV8(
            "owner URL or route plan differs from the selected V8 plan"
        )
    if owner.plan_sha256 != plan_sha256:
        raise PublicWebSocketCompositionErrorV8(
            "owner plan hash differs from the full V8 authority"
        )
    if frame_adapter_factory.owner_plan != expected_owner_plan:
        raise PublicWebSocketCompositionErrorV8(
            "V8 frame factory owner plan differs from the selected plan"
        )
    if frame_adapter_factory.plan is not plan:
        raise PublicWebSocketCompositionErrorV8(
            "V8 frame factory did not retain the selected plan object"
        )
    if lifecycle_coordinator.plan is not plan:
        raise PublicWebSocketCompositionErrorV8(
            "V8 lifecycle did not retain the selected plan object"
        )
    if lifecycle_coordinator.promoting_plans_v8 is not promoting_plans:
        raise PublicWebSocketCompositionErrorV8(
            "V8 lifecycle did not retain the exact four-plan tuple object"
        )
    if owner.frame_adapter_factory is not frame_adapter_factory:
        raise PublicWebSocketCompositionErrorV8(
            "owner is not wired to the admitted V8 frame factory"
        )
    if owner.lifecycle_coordinator is not lifecycle_coordinator:
        raise PublicWebSocketCompositionErrorV8(
            "owner is not wired to the admitted V8 lifecycle coordinator"
        )
    if frame_adapter_factory.recovery_lifecycle is not lifecycle_coordinator:
        raise PublicWebSocketCompositionErrorV8(
            "V8 frame factory is not wired to the admitted lifecycle coordinator"
        )

    _validate_lineage_v8(
        session_start=session_start,
        owner=owner,
        factory=frame_adapter_factory,
        lifecycle=lifecycle_coordinator,
    )
    try:
        _validate_capture_authority(
            session_start=session_start,
            writer_lease=writer_lease,
            recovered_wal_tail_ingest_seq=recovered_wal_tail_ingest_seq,
            factory=frame_adapter_factory._delegate,
            lifecycle=lifecycle_coordinator,
            allow_advanced_tail=active_runtime_task,
        )
    except PublicWebSocketCompositionErrorV2 as exc:
        raise PublicWebSocketCompositionErrorV8(str(exc)) from exc
    writer_lease.assert_held()


def _validate_writer_lease(
    *,
    session_start: SessionStartManifestV2,
    writer_lease: WriterLease,
) -> None:
    binding = session_start.writer_lease
    if _canonical_path(writer_lease.scope_root) != binding.scope_canonical_path:
        raise PublicWebSocketCompositionErrorV2(
            "live writer-lease scope differs from the session start"
        )
    if writer_lease.owner_pid != binding.owner_pid:
        raise PublicWebSocketCompositionErrorV2(
            "live writer-lease owner PID differs from the session start"
        )
    if writer_lease.owner_id != binding.owner_id:
        raise PublicWebSocketCompositionErrorV2(
            "live writer-lease owner ID differs from the session start"
        )
    if writer_lease.backend != binding.backend:
        raise PublicWebSocketCompositionErrorV2(
            "live writer-lease backend differs from the session start"
        )
    if writer_lease.acquired_wall_ms != binding.acquired_wall_ms:
        raise PublicWebSocketCompositionErrorV2(
            "live writer-lease wall acquisition differs from the session start"
        )
    if writer_lease.acquired_monotonic_ns != binding.acquired_monotonic_ns:
        raise PublicWebSocketCompositionErrorV2(
            "live writer-lease monotonic acquisition differs from the session start"
        )


def _validate_lineage(
    *,
    session_start: SessionStartManifestV2,
    owner: PublicWebSocketCaptureOwner,
    factory: PublicWebSocketFrameAdapterFactoryV2,
    lifecycle: WebSocketLifecycleFatalCoordinatorV2,
) -> None:
    if owner.plan_sha256 != session_start.wal_authority.plan_sha256:
        raise PublicWebSocketCompositionErrorV2(
            "owner plan hash differs from the session-start authority"
        )
    if owner.process_boot_id != session_start.process_boot_id:
        raise PublicWebSocketCompositionErrorV2(
            "owner process boot ID differs from the session start"
        )
    if factory.session_id != session_start.session_id:
        raise PublicWebSocketCompositionErrorV2("factory session ID differs from the session start")
    if factory.protocol_hash != session_start.wal_authority.protocol_sha256:
        raise PublicWebSocketCompositionErrorV2(
            "factory protocol hash differs from the session-start authority"
        )
    if lifecycle.session_id != session_start.session_id:
        raise PublicWebSocketCompositionErrorV2(
            "lifecycle session ID differs from the session start"
        )
    if lifecycle.process_boot_id != session_start.process_boot_id:
        raise PublicWebSocketCompositionErrorV2(
            "lifecycle process boot ID differs from the session start"
        )
    expected_started_at = ReceiptTimestamp(
        session_start.started_wall_ms,
        session_start.started_monotonic_ns,
    )
    if lifecycle.session_started_at != expected_started_at:
        raise PublicWebSocketCompositionErrorV2(
            "lifecycle start clocks differ from the session start"
        )
    if lifecycle.source_component != f"v2-owner-{lifecycle.plan.route_id}":
        raise PublicWebSocketCompositionErrorV2(
            "lifecycle source component differs from its selected route"
        )
    if factory.clock is not lifecycle.clock:
        raise PublicWebSocketCompositionErrorV2(
            "factory and lifecycle must share one receipt clock"
        )


def _validate_lineage_v8(
    *,
    session_start: SessionStartManifestV2,
    owner: PublicWebSocketCaptureOwner,
    factory: PublicWebSocketFrameAdapterFactoryV8,
    lifecycle: WebSocketLifecycleFatalCoordinatorV8,
) -> None:
    if owner.process_boot_id != session_start.process_boot_id:
        raise PublicWebSocketCompositionErrorV8(
            "owner process boot ID differs from the session start"
        )
    if factory.session_id != session_start.session_id:
        raise PublicWebSocketCompositionErrorV8(
            "V8 factory session ID differs from the session start"
        )
    if factory.protocol_hash != session_start.wal_authority.protocol_sha256:
        raise PublicWebSocketCompositionErrorV8(
            "V8 factory protocol hash differs from session authority"
        )
    if lifecycle.session_id != session_start.session_id:
        raise PublicWebSocketCompositionErrorV8(
            "V8 lifecycle session ID differs from the session start"
        )
    if lifecycle.process_boot_id != session_start.process_boot_id:
        raise PublicWebSocketCompositionErrorV8(
            "V8 lifecycle process boot ID differs from the session start"
        )
    expected_started_at = ReceiptTimestamp(
        session_start.started_wall_ms,
        session_start.started_monotonic_ns,
    )
    if lifecycle.session_started_at != expected_started_at:
        raise PublicWebSocketCompositionErrorV8(
            "V8 lifecycle start clocks differ from the session start"
        )
    if lifecycle.source_component != f"v8-owner-{lifecycle.plan.route_id}":
        raise PublicWebSocketCompositionErrorV8(
            "V8 lifecycle source component differs from its selected route"
        )
    if factory.clock is not lifecycle.clock:
        raise PublicWebSocketCompositionErrorV8(
            "V8 factory and lifecycle must share one receipt clock"
        )


def _validate_capture_authority(
    *,
    session_start: SessionStartManifestV2,
    writer_lease: WriterLease,
    recovered_wal_tail_ingest_seq: int,
    factory: PublicWebSocketFrameAdapterFactoryV2,
    lifecycle: WebSocketLifecycleFatalCoordinatorV2,
    allow_advanced_tail: bool = False,
) -> None:
    if type(allow_advanced_tail) is not bool:
        raise TypeError("allow_advanced_tail must be a boolean")
    if type(factory.ingress) is not SharedWebSocketIngressV2:
        raise PublicWebSocketCompositionErrorV2(
            "V2 frame factory requires the exact SharedWebSocketIngressV2"
        )
    if factory.ingress.recovered_wal_tail_ingest_seq != recovered_wal_tail_ingest_seq:
        raise PublicWebSocketCompositionErrorV2(
            "factory ingress differs from the admitted recovered WAL tail"
        )
    if factory.ingress.pipeline is not lifecycle.pipeline:
        raise PublicWebSocketCompositionErrorV2(
            "factory ingress and lifecycle must share one capture pipeline"
        )
    pipeline = lifecycle.pipeline
    if type(pipeline) is not CaptureBatchPipelineV2:
        raise PublicWebSocketCompositionErrorV2(
            "V2 composition requires the exact CaptureBatchPipelineV2"
        )
    if type(pipeline.writer) is not DurableCaptureBatchWriterV2:
        raise PublicWebSocketCompositionErrorV2(
            "V2 composition requires the exact DurableCaptureBatchWriterV2"
        )
    if getattr(pipeline.writer, "writer_lease", None) is not writer_lease:
        raise PublicWebSocketCompositionErrorV2(
            "pipeline durable writer is not bound to the admitted writer lease"
        )
    admitted_wal_writer = pipeline.writer.wal_writer
    block_writer = pipeline.writer.block_writer
    if type(admitted_wal_writer) is not MirroredWalWriterV2:
        raise PublicWebSocketCompositionErrorV2(
            "V2 composition requires the exact MirroredWalWriterV2"
        )
    wal_writer = cast(MirroredWalWriterV2, admitted_wal_writer)
    if type(block_writer) is not GroupedBlockWriterV2:
        raise PublicWebSocketCompositionErrorV2(
            "V2 composition requires the exact GroupedBlockWriterV2"
        )
    if allow_advanced_tail:
        pipeline.assert_live_runtime_authority_v2()
    else:
        pipeline.assert_running_healthy_and_writer_open_v2()
    if wal_writer.authority != session_start.wal_authority:
        raise PublicWebSocketCompositionErrorV2(
            "pipeline WAL authority differs from the session start"
        )
    if wal_writer.durability_binding != session_start.wal_durability_binding:
        raise PublicWebSocketCompositionErrorV2(
            "pipeline WAL durability differs from the session start"
        )
    wal_directories = wal_writer.root_directories
    if len(wal_directories) != 2:
        raise PublicWebSocketCompositionErrorV2(
            "pipeline WAL writer lacks the exact ordered dual-root paths"
        )
    observed_wal_paths = tuple(_canonical_path(path) for path in wal_directories)
    expected_wal_paths = tuple(
        reference.canonical_path for reference in session_start.storage_roots[:2]
    )
    if observed_wal_paths != expected_wal_paths:
        raise PublicWebSocketCompositionErrorV2(
            "pipeline dual-WAL root paths differ from the session start"
        )
    opened_wal_identities = wal_writer.opened_root_identities
    if len(opened_wal_identities) != 2:
        raise PublicWebSocketCompositionErrorV2(
            "pipeline WAL writer lacks exact opened-root identities"
        )
    for reference, opened in zip(
        session_start.storage_roots[:2],
        opened_wal_identities,
        strict=True,
    ):
        _validate_opened_root_identity(reference, opened)
    expected_next_ingest_seq = recovered_wal_tail_ingest_seq + 1
    if (
        wal_writer.next_ingest_seq < expected_next_ingest_seq
        if allow_advanced_tail
        else wal_writer.next_ingest_seq != expected_next_ingest_seq
    ):
        raise PublicWebSocketCompositionErrorV2(
            "pipeline WAL cursor differs from the recovered tail"
        )
    if (
        block_writer.next_ingest_seq < expected_next_ingest_seq
        if allow_advanced_tail
        else block_writer.next_ingest_seq != expected_next_ingest_seq
    ):
        raise PublicWebSocketCompositionErrorV2(
            "grouped-block cursor differs from the recovered WAL tail"
        )
    if block_writer.authority != session_start.wal_authority:
        raise PublicWebSocketCompositionErrorV2(
            "grouped-block authority differs from the session start"
        )
    if block_writer.root_binding != session_start.storage_roots[2].root_binding:
        raise PublicWebSocketCompositionErrorV2("grouped-block root differs from the session start")
    if block_writer.signing_authority.sha256 != session_start.block_signing_authority_sha256:
        raise PublicWebSocketCompositionErrorV2(
            "block signing authority differs from the session start"
        )
    if (
        block_writer.stream_group_id != session_start.stream_group_id
        or block_writer.segment_id != session_start.segment_id
    ):
        raise PublicWebSocketCompositionErrorV2(
            "grouped-block stream or segment differs from the session start"
        )
    if _canonical_path(block_writer.opened_directory) != (
        session_start.storage_roots[2].canonical_path
    ):
        raise PublicWebSocketCompositionErrorV2("grouped-block path differs from the session start")
    _validate_opened_root_identity(
        session_start.storage_roots[2],
        block_writer.opened_root_identity,
    )

    ledger = lifecycle.integrity_ledger
    if type(ledger) is not CaptureIntegrityLedgerV2:
        raise PublicWebSocketCompositionErrorV2(
            "V2 composition requires the exact CaptureIntegrityLedgerV2"
        )
    if ledger.writer_lease is not writer_lease:
        raise PublicWebSocketCompositionErrorV2(
            "integrity ledger is not bound to the admitted writer lease"
        )
    ledger.assert_running_healthy_and_writer_open_v2()
    if ledger.authority != session_start.wal_authority:
        raise PublicWebSocketCompositionErrorV2(
            "integrity-ledger authority differs from the session start"
        )
    if ledger.root_binding != session_start.storage_roots[3].root_binding:
        raise PublicWebSocketCompositionErrorV2(
            "integrity-ledger root differs from the session start"
        )
    if ledger.block_root_binding != session_start.storage_roots[2].root_binding:
        raise PublicWebSocketCompositionErrorV2(
            "integrity-ledger block authority differs from the session start"
        )
    if _canonical_path(ledger.opened_directory) != (
        session_start.storage_roots[3].canonical_path
    ):
        raise PublicWebSocketCompositionErrorV2(
            "integrity-ledger path differs from the session start"
        )
    _validate_opened_root_identity(
        session_start.storage_roots[3],
        ledger.opened_root_identity,
    )
    if ledger.opened_block_root_identity != block_writer.opened_root_identity:
        raise PublicWebSocketCompositionErrorV2(
            "integrity ledger did not open the admitted grouped-block identity"
        )


def _validate_opened_root_identity(
    reference: SessionStorageRootReferenceV2,
    opened: StorageRootOpenedIdentityV2,
) -> None:
    if type(reference) is not SessionStorageRootReferenceV2:
        raise TypeError("storage-root reference must have its exact production type")
    if type(opened) is not StorageRootOpenedIdentityV2:
        raise TypeError("opened storage-root identity must have its exact production type")
    if (
        opened.canonical_path != reference.canonical_path
        or str(opened.root_device) != reference.root_device
        or str(opened.root_inode) != reference.root_inode
        or str(opened.binding_device) != reference.binding_device
        or str(opened.binding_inode) != reference.binding_inode
    ):
        raise PublicWebSocketCompositionErrorV2(
            "opened storage-root pathname identity differs from the session start"
        )


def _canonical_path(value: str | Path) -> str:
    return os.path.normcase(os.path.abspath(os.fspath(value)))
