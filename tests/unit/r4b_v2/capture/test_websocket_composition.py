from __future__ import annotations

import asyncio
import hashlib
import threading
from contextlib import nullcontext
from dataclasses import replace
from pathlib import Path
from types import MethodType
from typing import cast

import pytest

from signalbot.capture.models import ConnectionState
from signalbot.capture.pipeline import CapturePipeline
from signalbot.capture.receipts import IngestSequencer, ReceiptTimestamp
from signalbot.capture.writer_lease import WriterLease, WriterLeaseNotHeldError
from signalbot.capture.ws_owner import (
    Connector,
    PublicWebSocketCaptureOwner,
    WebSocketOwnerSettings,
)
from signalbot.r4b_v2.capture.batching import BatchPolicyV2, BoundedBatchHandoffV2
from signalbot.r4b_v2.capture.block_container import (
    BlockSigningAuthorityV2,
    Ed25519BlockSignerV2,
)
from signalbot.r4b_v2.capture.blocks import (
    BlockPolicyV2,
    GroupedBlockBuilderV2,
    GroupedBlockWriterV2,
)
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
    build_provisional_promoting_capture_plans_v2,
    build_provisional_promoting_capture_plans_v8,
    provisional_promoting_plan_sha256_v2,
    provisional_promoting_plan_sha256_v8,
)
from signalbot.r4b_v2.capture.session import (
    PersistedSessionStartAuthorityV2,
    SessionAuthorityExistsError,
    SessionAuthorityIntegrityError,
    assert_persisted_session_start_authority_current_v2,
    canonical_session_start_manifest_path_v2,
    write_session_start_manifest_v2,
)
from signalbot.r4b_v2.capture.wal import WalAuthorityV2, WalSyncPolicyV2
from signalbot.r4b_v2.capture.wal_qualification import (
    WAL_QUALIFICATION_DURATION_MS_V2,
    WAL_RECORD_CAP_CANDIDATES_V2,
    WAL_SYNC_CANDIDATES_MS_V2,
    WalCandidateMetricsV2,
    WalCandidateQualificationV2,
    WalQualificationRunV2,
    WalSelectionReceiptV2,
    select_wal_candidate_v2,
    wal_candidate_id_v2,
)
from signalbot.r4b_v2.capture.websocket import (
    PublicWebSocketFrameAdapterFactoryV2,
    SharedWebSocketIngressV2,
    build_public_websocket_owner_plan_v2,
)
from signalbot.r4b_v2.capture.websocket_composition import (
    PublicWebSocketCompositionErrorV2,
    PublicWebSocketCompositionErrorV8,
    PublicWebSocketFrameAdapterFactoryV8,
    PublicWebSocketOwnerCompositionV2,
    PublicWebSocketOwnerCompositionV8,
    PublicWebSocketRuntimeClaimErrorV2,
    PublicWebSocketRuntimeClaimErrorV8,
    PublicWebSocketRuntimeRunTokenV2,
    PublicWebSocketRuntimeRunTokenV8,
    PublicWebSocketRuntimeStartBarrierV8,
    create_public_websocket_frame_adapter_factory_v8,
    create_public_websocket_owner_composition_v8,
    create_public_websocket_runtime_start_barrier_v8,
)
from signalbot.r4b_v2.capture.websocket_lifecycle import (
    WebSocketLifecycleFatalCoordinatorV2,
    WebSocketLifecycleFatalCoordinatorV8,
)

HASH = "a" * 64
PROCESS_BOOT_ID = "0123456789abcdef0123456789abcdef"
STREAM_GROUP_ID = "binance-usdm-public-v2"
SEGMENT_ID = "segment-0001"
RECOVERED_TAIL = 0
QUALIFICATION = "composition-wal-24h-grid"
WINDOW_START_MS = 2_000_000_000_000
WINDOW_END_MS = WINDOW_START_MS + WAL_QUALIFICATION_DURATION_MS_V2
H_START_MS = WINDOW_END_MS + 60_000
MAXIMUM_BYTES = 64 * 1024 * 1024
RESERVE_BYTES = 1024


class _WriterLeaseSubclass(WriterLease):
    pass


class _FrameAdapterFactoryV2Subclass(PublicWebSocketFrameAdapterFactoryV2):
    pass


class _LifecycleCoordinatorV2Subclass(WebSocketLifecycleFatalCoordinatorV2):
    pass


class _LifecycleCoordinatorV8Subclass(WebSocketLifecycleFatalCoordinatorV8):
    pass


class _NoOpGuard:
    def validate_current(self) -> None:
        return

    def connector_admission_guard(self):  # type: ignore[no-untyped-def]
        return nullcontext()


class _NoOpIngress:
    async def offer_frame(self, **kwargs: object) -> object:
        del kwargs
        return object()

    async def offer_recovery_successor(self, **kwargs: object) -> object:
        del kwargs
        return object()


class _PartialLifecycle:
    def __init__(self) -> None:
        self.stop_event = asyncio.Event()
        self.failed = False
        self.accepting = True

    def record_transition(self, *args: object, **kwargs: object) -> None:
        del args, kwargs

    def trip_fatal(self, cause: BaseException) -> None:
        del cause


class _PartialFactory:
    def __call__(self, **kwargs: object) -> object:
        del kwargs
        raise AssertionError("partial factory must never be called")


class _NeverConsumedFrames:
    def __init__(self) -> None:
        self.iterated = False

    async def __aiter__(self):  # type: ignore[no-untyped-def]
        self.iterated = True
        yield b"forbidden-frame"


class _TamperingConnection:
    def __init__(self, path: Path) -> None:
        self.path = path
        self.frames = _NeverConsumedFrames()
        self.closed = False

    async def __aenter__(self) -> _NeverConsumedFrames:
        self.path.write_bytes(self.path.read_bytes() + b" ")
        return self.frames

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: object,
    ) -> None:
        del exc_type, exc_value, traceback
        self.closed = True


class _TamperingConnector:
    def __init__(self, connection: _TamperingConnection) -> None:
        self.connection = connection
        self.calls: list[str] = []

    def __call__(self, url: str) -> _TamperingConnection:
        self.calls.append(url)
        return self.connection


class _Clock:
    def __init__(self, wall_ms: int = 10_000, monotonic_ns: int = 20_000) -> None:
        self.wall_ms = wall_ms
        self.monotonic_ns = monotonic_ns

    def capture(self) -> ReceiptTimestamp:
        return ReceiptTimestamp(self.wall_ms, self.monotonic_ns)


class _CountingConnector:
    def __init__(self) -> None:
        self.calls: list[str] = []

    def __call__(self, url: str) -> object:
        self.calls.append(url)
        raise AssertionError("connector must not be reached by composition tests")


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


def _wal_policy(sync_ms: int, record_cap: int) -> WalSyncPolicyV2:
    return WalSyncPolicyV2(
        qualification_id=QUALIFICATION,
        fsync_candidate_id=wal_candidate_id_v2(
            sync_ms=sync_ms,
            record_cap=record_cap,
        ),
        interval_ms=sync_ms,
        max_unsynced_records=record_cap,
        max_unsynced_bytes=8_000_000,
        max_record_bytes=20_000,
        max_segment_bytes=16_000_000,
    )


def _candidate_metrics(*, passed: bool) -> WalCandidateMetricsV2:
    return WalCandidateMetricsV2(
        unresolved_overflow_or_drop_count=0 if passed else 1,
        p99_queue_fraction_ppm=500_000,
        maximum_queue_fraction_ppm=750_000,
        p99_enqueue_latency_ns=10_000_000,
        maximum_enqueue_latency_ns=100_000_000,
        p99_cpu_fraction_ppm=700_000,
        maximum_cpu_fraction_ppm=850_000,
        p99_fsync_latency_ns=100_000_000,
        maximum_fsync_latency_ns=500_000_000,
        service_rate_over_p99_ingress_ppm=2_000_000,
        service_rate_over_peak_1s_ingress_ppm=1_250_000,
        crash_replay_root_equality=True,
    )


def _selection_receipt() -> WalSelectionReceiptV2:
    selected = (10, 256)
    candidates = tuple(
        WalCandidateQualificationV2(
            policy=_wal_policy(sync_ms, record_cap),
            metrics=_candidate_metrics(passed=(sync_ms, record_cap) == selected),
            measurement_root_sha256=hashlib.sha256(
                f"{sync_ms}:{record_cap}".encode()
            ).hexdigest(),
        )
        for sync_ms in WAL_SYNC_CANDIDATES_MS_V2
        for record_cap in WAL_RECORD_CAP_CANDIDATES_V2
    )
    qualification = WalQualificationRunV2(
        qualification_id=QUALIFICATION,
        window_start_wall_ms=WINDOW_START_MS,
        window_end_wall_ms=WINDOW_END_MS,
        actual_final_panel_sha256="1" * 64,
        final_codec_sha256="2" * 64,
        source_manifest_sha256="3" * 64,
        runtime_manifest_sha256="4" * 64,
        independent_failure_domain_evidence_sha256="5" * 64,
        actual_final_panel_passed=True,
        final_codec_passed=True,
        independent_failure_domains_passed=True,
        engineering_only=True,
        strategy_or_outcome_data_accessed=False,
        candidates=candidates,
    )
    return select_wal_candidate_v2(
        qualification,
        selection_wall_ms=WINDOW_END_MS,
        h_start_wall_ms=H_START_MS,
    )


class _LoopThread:
    def __init__(self) -> None:
        self.loop = asyncio.new_event_loop()
        self.thread = threading.Thread(target=self._run, daemon=True)
        self.thread.start()

    def _run(self) -> None:
        asyncio.set_event_loop(self.loop)
        self.loop.run_forever()

    def start_pipeline(self, pipeline: CaptureBatchPipelineV2) -> None:
        async def start() -> None:
            pipeline.start()

        asyncio.run_coroutine_threadsafe(start(), self.loop).result(timeout=5)

    def stop_pipeline(self, pipeline: CaptureBatchPipelineV2) -> None:
        asyncio.run_coroutine_threadsafe(pipeline.stop(), self.loop).result(timeout=5)

    def close(self) -> None:
        self.loop.call_soon_threadsafe(self.loop.stop)
        self.thread.join(timeout=5)
        self.loop.close()


def _websocket_plan(
    plans: tuple[ProvisionalPromotingPlanV2, ...],
    route_id: str,
) -> ProvisionalPromotingCapturePlanV2:
    return next(
        plan
        for plan in plans
        if isinstance(plan, ProvisionalPromotingCapturePlanV2) and plan.route_id == route_id
    )


class _Fixture:
    def __init__(
        self,
        tmp_path: Path,
        *,
        authority_plan_sha256: str | None = None,
    ) -> None:
        self.scope = tmp_path / "scope"
        self.scope.mkdir()
        self.plans = build_provisional_promoting_capture_plans_v2(("BTCUSDT",))
        self.plan = _websocket_plan(self.plans, "usdm_market")
        self.authority = WalAuthorityV2(
            attempt_id="attempt-composition",
            protocol_sha256=HASH,
            plan_sha256=(
                authority_plan_sha256
                or provisional_promoting_plan_sha256_v2(self.plans)
            ),
            source_manifest_sha256="b" * 64,
            schema_sha256="c" * 64,
            runtime_manifest_sha256="d" * 64,
        )
        self.primary_path = self.scope / "wal-primary"
        self.mirror_path = self.scope / "wal-mirror"
        self.block_path = self.scope / "blocks"
        self.ledger_path = self.scope / "ledger"
        self.lease = WriterLease.acquire(self.scope)
        self.selection_receipt = _selection_receipt()
        selected_wal_policy = self.selection_receipt.selected_policy
        assert selected_wal_policy is not None
        self.wal_writer = MirroredWalWriterV2(
            self.primary_path,
            self.mirror_path,
            authority=self.authority,
            policy=selected_wal_policy,
            selection_receipt=self.selection_receipt,
            primary_maximum_total_bytes=MAXIMUM_BYTES,
            mirror_maximum_total_bytes=MAXIMUM_BYTES,
            primary_emergency_reserve_bytes=RESERVE_BYTES,
            mirror_emergency_reserve_bytes=RESERVE_BYTES,
            primary_failure_domain_id="device-primary",
            mirror_failure_domain_id="device-mirror",
        )
        self.primary, self.mirror = self.wal_writer.durability_binding.root_bindings
        self.durability = self.wal_writer.durability_binding
        self.signer = Ed25519BlockSignerV2.from_private_key_bytes(
            key_id="composition-test-writer",
            private_key_bytes=bytes(range(32)),
        )
        self.block_signing_authority = BlockSigningAuthorityV2.from_public_key_bytes(
            key_id=self.signer.key_id,
            public_key_bytes=self.signer.public_key_bytes,
        )
        self.block_policy = BlockPolicyV2(
            qualification_id=QUALIFICATION,
            codec_candidate_id="composition-zstd-candidate",
            compression_level=9,
            max_uncompressed_bytes=4 * 1024 * 1024,
            max_linger_ms=1_000,
        )
        self.block_writer = GroupedBlockWriterV2(
            self.block_path,
            authority=self.authority,
            policy=self.block_policy,
            signer=self.signer,
            signing_authority=self.block_signing_authority,
            stream_group_id=STREAM_GROUP_ID,
            segment_id=SEGMENT_ID,
            maximum_total_bytes=MAXIMUM_BYTES,
            emergency_reserve_bytes=RESERVE_BYTES,
            failure_domain_id="device-block",
        )
        self.block = self.block_writer.root_binding
        self.integrity_ledger_max_events = 10_000
        self.ledger = CaptureIntegrityLedgerV2(
            self.ledger_path,
            authority=self.authority,
            block_directory=self.block_path,
            block_root_binding=self.block,
            block_signing_authority=self.block_signing_authority,
            block_policy=self.block_policy,
            block_stream_group_id=STREAM_GROUP_ID,
            block_segment_id=SEGMENT_ID,
            maximum_total_bytes=MAXIMUM_BYTES,
            emergency_reserve_bytes=RESERVE_BYTES,
            max_events=self.integrity_ledger_max_events,
            failure_domain_id="device-ledger",
            writer_lease=self.lease,
        )
        self.ledger_binding = self.ledger.root_binding
        self.batch_policy = BatchPolicyV2(
            max_records=selected_wal_policy.max_unsynced_records,
            max_encoded_bytes=selected_wal_policy.max_unsynced_bytes,
            max_linger_us=selected_wal_policy.interval_ms * 1_000,
            queue_max_events=512,
            queue_max_encoded_bytes=16_000_000,
            low_water_events=128,
            low_water_encoded_bytes=4_000_000,
            qualification_id=QUALIFICATION,
        )
        self.durable_writer = DurableCaptureBatchWriterV2(
            batch_policy=self.batch_policy,
            wal_writer=self.wal_writer,
            block_builder=GroupedBlockBuilderV2(self.block_policy),
            block_writer=self.block_writer,
            writer_lease=self.lease,
        )
        self.handoff = BoundedBatchHandoffV2(
            self.batch_policy,
            expected_first_ingest_seq=RECOVERED_TAIL + 1,
        )
        self.pipeline = CaptureBatchPipelineV2(self.handoff, self.durable_writer)
        self.loop_thread = _LoopThread()
        self.loop_thread.start_pipeline(self.pipeline)
        self.started_wall_ms = self.lease.acquired_wall_ms + 1
        self.started_monotonic_ns = self.lease.acquired_monotonic_ns + 1
        self.session_id = f"{self.started_wall_ms}-{PROCESS_BOOT_ID}"
        self.session_start_path = canonical_session_start_manifest_path_v2(self.lease)
        self.session_start_authority = write_session_start_manifest_v2(
            self.session_start_path,
            lease=self.lease,
            session_id=self.session_id,
            process_boot_id=PROCESS_BOOT_ID,
            started_wall_ms=self.started_wall_ms,
            started_monotonic_ns=self.started_monotonic_ns,
            wal_authority=self.authority,
            wal_durability_binding=self.durability,
            block_policy=self.block_policy,
            block_signing_authority=self.block_signing_authority,
            stream_group_id=STREAM_GROUP_ID,
            segment_id=SEGMENT_ID,
            integrity_ledger_max_events=self.integrity_ledger_max_events,
            storage_root_directories=(
                self.primary_path,
                self.mirror_path,
                self.block_path,
                self.ledger_path,
            ),
            grouped_block_root_binding=self.block,
            integrity_ledger_root_binding=self.ledger_binding,
        )
        self.session_start = self.session_start_authority.manifest
        self.clock = _Clock(
            self.started_wall_ms + 1,
            self.started_monotonic_ns + 1,
        )
        self.connector = _CountingConnector()
        self.lifecycle = self.lifecycle_for(self.plan)
        self.ingress = SharedWebSocketIngressV2(
            self.pipeline,
            recovered_wal_tail_ingest_seq=RECOVERED_TAIL,
        )
        self.factory = self.factory_for(self.plan, lifecycle=self.lifecycle)
        self.owner = self.owner_for(self.plan, self.factory, self.lifecycle)

    def close(self) -> None:
        try:
            self.loop_thread.stop_pipeline(self.pipeline)
        except BaseException:
            pass
        self.loop_thread.close()
        try:
            self.lease.assert_held()
        except WriterLeaseNotHeldError:
            return
        self.lease.release()

    def lifecycle_for(
        self,
        plan: ProvisionalPromotingCapturePlanV2,
        *,
        session_id: str | None = None,
        process_boot_id: str = PROCESS_BOOT_ID,
        pipeline: CaptureBatchPipelineV2 | None = None,
    ) -> WebSocketLifecycleFatalCoordinatorV2:
        return WebSocketLifecycleFatalCoordinatorV2(
            self.plans,
            plan,
            session_id=session_id or self.session_id,
            process_boot_id=process_boot_id,
            session_started_at=ReceiptTimestamp(
                self.started_wall_ms,
                self.started_monotonic_ns,
            ),
            source_component=f"v2-owner-{plan.route_id}",
            clock=self.clock,
            pipeline=pipeline or self.pipeline,
            integrity_ledger=self.ledger,
            finality_timeout_seconds=1.0,
        )

    def factory_for(
        self,
        plan: ProvisionalPromotingCapturePlanV2,
        *,
        lifecycle: WebSocketLifecycleFatalCoordinatorV2,
        session_id: str | None = None,
        protocol_hash: str = HASH,
        ingress: SharedWebSocketIngressV2 | None = None,
    ) -> PublicWebSocketFrameAdapterFactoryV2:
        return PublicWebSocketFrameAdapterFactoryV2(
            plan,
            session_id=session_id or self.session_id,
            protocol_hash=protocol_hash,
            clock=self.clock,
            ingress=ingress or self.ingress,
            recovery_lifecycle=lifecycle,
        )

    def owner_for(
        self,
        plan: ProvisionalPromotingCapturePlanV2,
        factory: PublicWebSocketFrameAdapterFactoryV2,
        lifecycle: WebSocketLifecycleFatalCoordinatorV2,
        *,
        process_boot_id: str = PROCESS_BOOT_ID,
        connector: Connector | None = None,
        require_admission: bool = True,
    ) -> PublicWebSocketCaptureOwner:
        return PublicWebSocketCaptureOwner(
            build_public_websocket_owner_plan_v2(plan),
            plan_sha256=self.authority.plan_sha256,
            process_boot_id=process_boot_id,
            settings=_settings(),
            connector=connector or cast(Connector, self.connector),
            frame_adapter_factory=factory,
            lifecycle_coordinator=lifecycle,
            requires_preconnect_admission=require_admission,
        )

    def compose(
        self,
        *,
        plans: tuple[ProvisionalPromotingPlanV2, ...] | None = None,
        plan: ProvisionalPromotingCapturePlanV2 | None = None,
        recovered_tail: int = RECOVERED_TAIL,
        owner: PublicWebSocketCaptureOwner | None = None,
        factory: PublicWebSocketFrameAdapterFactoryV2 | None = None,
        lifecycle: WebSocketLifecycleFatalCoordinatorV2 | None = None,
        writer_lease: WriterLease | None = None,
        session_start_authority: PersistedSessionStartAuthorityV2 | None = None,
    ) -> PublicWebSocketOwnerCompositionV2:
        return PublicWebSocketOwnerCompositionV2(
            session_start_authority=(
                session_start_authority or self.session_start_authority
            ),
            writer_lease=writer_lease or self.lease,
            promoting_plans=plans or self.plans,
            plan=plan or self.plan,
            recovered_wal_tail_ingest_seq=recovered_tail,
            owner=owner or self.owner,
            frame_adapter_factory=factory or self.factory,
            lifecycle_coordinator=lifecycle or self.lifecycle,
        )


def _v8_websocket_plan(
    plans: tuple[ProvisionalPromotingPlanV8, ...],
    route_id: str = "usdm_market",
) -> ProvisionalPromotingCapturePlanV2:
    return next(
        candidate
        for candidate in plans
        if type(candidate) is ProvisionalPromotingCapturePlanV2
        and candidate.route_id == route_id
    )


def _v8_boundaries(
    fixture: _Fixture,
    plans: tuple[ProvisionalPromotingPlanV8, ...],
    plan: ProvisionalPromotingCapturePlanV2,
) -> tuple[
    WebSocketLifecycleFatalCoordinatorV8,
    PublicWebSocketFrameAdapterFactoryV8,
    PublicWebSocketCaptureOwner,
]:
    lifecycle = WebSocketLifecycleFatalCoordinatorV8(
        plans,
        plan,
        session_id=fixture.session_id,
        process_boot_id=PROCESS_BOOT_ID,
        session_started_at=ReceiptTimestamp(
            fixture.started_wall_ms,
            fixture.started_monotonic_ns,
        ),
        source_component=f"v8-owner-{plan.route_id}",
        clock=fixture.clock,
        pipeline=fixture.pipeline,
        integrity_ledger=fixture.ledger,
        finality_timeout_seconds=1.0,
    )
    factory = create_public_websocket_frame_adapter_factory_v8(
        plan,
        session_id=fixture.session_id,
        protocol_hash=HASH,
        clock=fixture.clock,
        ingress=fixture.ingress,
        recovery_lifecycle=lifecycle,
    )
    owner = PublicWebSocketCaptureOwner(
        build_public_websocket_owner_plan_v2(plan),
        plan_sha256=fixture.authority.plan_sha256,
        process_boot_id=PROCESS_BOOT_ID,
        settings=_settings(),
        connector=cast(Connector, fixture.connector),
        frame_adapter_factory=factory,
        lifecycle_coordinator=lifecycle,
    )
    return lifecycle, factory, owner


def _v8_composition(
    fixture: _Fixture,
    plans: tuple[ProvisionalPromotingPlanV8, ...],
    route_id: str = "usdm_market",
) -> PublicWebSocketOwnerCompositionV8:
    plan = _v8_websocket_plan(plans, route_id)
    lifecycle, factory, owner = _v8_boundaries(fixture, plans, plan)
    return create_public_websocket_owner_composition_v8(
        session_start_authority=fixture.session_start_authority,
        writer_lease=fixture.lease,
        promoting_plans=plans,
        plan=plan,
        recovered_wal_tail_ingest_seq=RECOVERED_TAIL,
        owner=owner,
        frame_adapter_factory=factory,
        lifecycle_coordinator=lifecycle,
    )


def test_exact_composition_is_admitted_without_calling_connector(
    tmp_path: Path,
) -> None:
    fixture = _Fixture(tmp_path)
    try:
        composition = fixture.compose()

        composition.validate_current()
        assert composition.owner is fixture.owner
        assert fixture.connector.calls == []
    finally:
        fixture.close()


@pytest.mark.asyncio
async def test_exact_v8_composition_retains_full_authority_and_is_one_shot(
    tmp_path: Path,
) -> None:
    plans = build_provisional_promoting_capture_plans_v8(("BTCUSDT",))
    fixture = _Fixture(
        tmp_path,
        authority_plan_sha256=provisional_promoting_plan_sha256_v8(plans),
    )
    try:
        plan = _v8_websocket_plan(plans)
        lifecycle, factory, owner = _v8_boundaries(fixture, plans, plan)
        composition = create_public_websocket_owner_composition_v8(
            session_start_authority=fixture.session_start_authority,
            writer_lease=fixture.lease,
            promoting_plans=plans,
            plan=plan,
            recovered_wal_tail_ingest_seq=RECOVERED_TAIL,
            owner=owner,
            frame_adapter_factory=factory,
            lifecycle_coordinator=lifecycle,
        )

        composition.validate_current()
        assert lifecycle.promoting_plans_v8 is plans
        assert lifecycle.plan is plan
        assert factory.plan is plan
        assert owner.preconnect_admission_guard is composition
        assert owner.requires_preconnect_admission is True
        assert fixture.connector.calls == []

        lifecycle.stop_event.set()
        assert await composition.run() is None
        with pytest.raises(PublicWebSocketCompositionErrorV8, match="one-shot"):
            await composition.run()
        assert fixture.connector.calls == []
    finally:
        fixture.close()


def test_v8_composition_rejects_v2_session_hash_before_connector(
    tmp_path: Path,
) -> None:
    plans = build_provisional_promoting_capture_plans_v8(("BTCUSDT",))
    fixture = _Fixture(tmp_path)
    try:
        plan = _v8_websocket_plan(plans)
        lifecycle, factory, owner = _v8_boundaries(fixture, plans, plan)

        with pytest.raises(
            PublicWebSocketCompositionErrorV8,
            match="session-start WAL authority",
        ):
            create_public_websocket_owner_composition_v8(
                session_start_authority=fixture.session_start_authority,
                writer_lease=fixture.lease,
                promoting_plans=plans,
                plan=plan,
                recovered_wal_tail_ingest_seq=RECOVERED_TAIL,
                owner=owner,
                frame_adapter_factory=factory,
                lifecycle_coordinator=lifecycle,
            )
        assert owner.generation == 0
        assert fixture.connector.calls == []
    finally:
        fixture.close()


def test_v8_source_gap_open_appends_under_real_full_hash_ledger(
    tmp_path: Path,
) -> None:
    plans = build_provisional_promoting_capture_plans_v8(("BTCUSDT",))
    expected_plan_hash = provisional_promoting_plan_sha256_v8(plans)
    fixture = _Fixture(tmp_path, authority_plan_sha256=expected_plan_hash)
    try:
        plan = _v8_websocket_plan(plans)
        lifecycle, factory, owner = _v8_boundaries(fixture, plans, plan)
        create_public_websocket_owner_composition_v8(
            session_start_authority=fixture.session_start_authority,
            writer_lease=fixture.lease,
            promoting_plans=plans,
            plan=plan,
            recovered_wal_tail_ingest_seq=RECOVERED_TAIL,
            owner=owner,
            frame_adapter_factory=factory,
            lifecycle_coordinator=lifecycle,
        )

        lifecycle.record_transition(
            "market-g000001",
            generation=1,
            last_frame_seq=0,
            state=ConnectionState.CONNECTING,
            reason="connect_attempt",
        )

        assert lifecycle.pending_source_gap is True
        assert lifecycle.failed is False
        assert fixture.ledger.authority.plan_sha256 == expected_plan_hash
        assert len(fixture.ledger.events) == 1
        event = fixture.ledger.events[0]
        assert event.event_type == "SOURCE_GAP"
        assert event.payload["plan_id"] == plan.name
        assert event.payload["source_component"] == "v8-owner-usdm_market"
        assert owner.generation == 0
        assert fixture.connector.calls == []
    finally:
        fixture.close()


def test_v8_factory_and_composition_are_factory_sealed(tmp_path: Path) -> None:
    plans = build_provisional_promoting_capture_plans_v8(("BTCUSDT",))
    fixture = _Fixture(
        tmp_path,
        authority_plan_sha256=provisional_promoting_plan_sha256_v8(plans),
    )
    try:
        plan = _v8_websocket_plan(plans)
        lifecycle = WebSocketLifecycleFatalCoordinatorV8(
            plans,
            plan,
            session_id=fixture.session_id,
            process_boot_id=PROCESS_BOOT_ID,
            session_started_at=ReceiptTimestamp(
                fixture.started_wall_ms,
                fixture.started_monotonic_ns,
            ),
            source_component=f"v8-owner-{plan.route_id}",
            clock=fixture.clock,
            pipeline=fixture.pipeline,
            integrity_ledger=fixture.ledger,
            finality_timeout_seconds=1.0,
        )
        with pytest.raises(TypeError, match="factory-sealed"):
            PublicWebSocketFrameAdapterFactoryV8(
                plan,
                session_id=fixture.session_id,
                protocol_hash=HASH,
                clock=fixture.clock,
                ingress=fixture.ingress,
                recovery_lifecycle=lifecycle,
            )
        factory = create_public_websocket_frame_adapter_factory_v8(
            plan,
            session_id=fixture.session_id,
            protocol_hash=HASH,
            clock=fixture.clock,
            ingress=fixture.ingress,
            recovery_lifecycle=lifecycle,
        )
        owner = PublicWebSocketCaptureOwner(
            build_public_websocket_owner_plan_v2(plan),
            plan_sha256=fixture.authority.plan_sha256,
            process_boot_id=PROCESS_BOOT_ID,
            settings=_settings(),
            connector=cast(Connector, fixture.connector),
            frame_adapter_factory=factory,
            lifecycle_coordinator=lifecycle,
        )
        with pytest.raises(TypeError, match="factory-sealed"):
            PublicWebSocketOwnerCompositionV8(
                session_start_authority=fixture.session_start_authority,
                writer_lease=fixture.lease,
                promoting_plans=plans,
                plan=plan,
                recovered_wal_tail_ingest_seq=RECOVERED_TAIL,
                owner=owner,
                frame_adapter_factory=factory,
                lifecycle_coordinator=lifecycle,
            )
        assert owner.generation == 0
        assert fixture.connector.calls == []
    finally:
        fixture.close()


def test_v8_rejects_foreign_equal_plan_mixed_boundaries_and_subclass(
    tmp_path: Path,
) -> None:
    plans = build_provisional_promoting_capture_plans_v8(("BTCUSDT",))
    fixture = _Fixture(
        tmp_path,
        authority_plan_sha256=provisional_promoting_plan_sha256_v8(plans),
    )
    try:
        plan = _v8_websocket_plan(plans)
        foreign_equal_plan = replace(plan)
        assert foreign_equal_plan == plan and foreign_equal_plan is not plan
        with pytest.raises(ValueError, match="authority object"):
            WebSocketLifecycleFatalCoordinatorV8(
                plans,
                foreign_equal_plan,
                session_id=fixture.session_id,
                process_boot_id=PROCESS_BOOT_ID,
                session_started_at=ReceiptTimestamp(
                    fixture.started_wall_ms,
                    fixture.started_monotonic_ns,
                ),
                source_component=f"v8-owner-{plan.route_id}",
                clock=fixture.clock,
                pipeline=fixture.pipeline,
                integrity_ledger=fixture.ledger,
                finality_timeout_seconds=1.0,
            )

        lifecycle, factory, _owner = _v8_boundaries(fixture, plans, plan)
        with pytest.raises(TypeError, match=r"V8 frame factory.*paired exactly"):
            PublicWebSocketCaptureOwner(
                build_public_websocket_owner_plan_v2(plan),
                plan_sha256=fixture.authority.plan_sha256,
                process_boot_id=PROCESS_BOOT_ID,
                settings=_settings(),
                connector=cast(Connector, fixture.connector),
                frame_adapter_factory=factory,
                lifecycle_coordinator=fixture.lifecycle,
            )
        with pytest.raises(TypeError, match=r"V8 frame factory.*paired exactly"):
            PublicWebSocketCaptureOwner(
                build_public_websocket_owner_plan_v2(plan),
                plan_sha256=fixture.authority.plan_sha256,
                process_boot_id=PROCESS_BOOT_ID,
                settings=_settings(),
                connector=cast(Connector, fixture.connector),
                frame_adapter_factory=fixture.factory,
                lifecycle_coordinator=lifecycle,
            )

        subclass_lifecycle = _LifecycleCoordinatorV8Subclass(
            plans,
            plan,
            session_id=fixture.session_id,
            process_boot_id=PROCESS_BOOT_ID,
            session_started_at=ReceiptTimestamp(
                fixture.started_wall_ms,
                fixture.started_monotonic_ns,
            ),
            source_component=f"v8-owner-{plan.route_id}",
            clock=fixture.clock,
            pipeline=fixture.pipeline,
            integrity_ledger=fixture.ledger,
            finality_timeout_seconds=1.0,
        )
        with pytest.raises(TypeError, match="exact V8 lifecycle"):
            create_public_websocket_frame_adapter_factory_v8(
                plan,
                session_id=fixture.session_id,
                protocol_hash=HASH,
                clock=fixture.clock,
                ingress=fixture.ingress,
                recovery_lifecycle=subclass_lifecycle,
            )
        assert fixture.connector.calls == []
    finally:
        fixture.close()


def test_v8_rejects_nonexact_bundle_shape_and_distinct_equal_tuple(
    tmp_path: Path,
) -> None:
    plans = build_provisional_promoting_capture_plans_v8(("BTCUSDT",))
    fixture = _Fixture(
        tmp_path,
        authority_plan_sha256=provisional_promoting_plan_sha256_v8(plans),
    )
    try:
        plan = _v8_websocket_plan(plans)
        for invalid_plans in (plans[:-1], (*plans, plans[-1])):
            with pytest.raises(ValueError, match="v8 authority requires exactly"):
                WebSocketLifecycleFatalCoordinatorV8(
                    cast(tuple[ProvisionalPromotingPlanV8, ...], invalid_plans),
                    plan,
                    session_id=fixture.session_id,
                    process_boot_id=PROCESS_BOOT_ID,
                    session_started_at=ReceiptTimestamp(
                        fixture.started_wall_ms,
                        fixture.started_monotonic_ns,
                    ),
                    source_component=f"v8-owner-{plan.route_id}",
                    clock=fixture.clock,
                    pipeline=fixture.pipeline,
                    integrity_ledger=fixture.ledger,
                    finality_timeout_seconds=1.0,
                )

        distinct_equal_plans = tuple(list(plans))
        assert distinct_equal_plans == plans and distinct_equal_plans is not plans
        lifecycle, factory, owner = _v8_boundaries(
            fixture,
            distinct_equal_plans,
            plan,
        )
        with pytest.raises(
            PublicWebSocketCompositionErrorV8,
            match="exact four-plan tuple object",
        ):
            create_public_websocket_owner_composition_v8(
                session_start_authority=fixture.session_start_authority,
                writer_lease=fixture.lease,
                promoting_plans=plans,
                plan=plan,
                recovered_wal_tail_ingest_seq=RECOVERED_TAIL,
                owner=owner,
                frame_adapter_factory=factory,
                lifecycle_coordinator=lifecycle,
            )
        assert fixture.connector.calls == []
    finally:
        fixture.close()


def test_v8_runtime_token_is_factory_sealed_and_claim_is_single_owner(
    tmp_path: Path,
) -> None:
    plans = build_provisional_promoting_capture_plans_v8(("BTCUSDT",))
    fixture = _Fixture(
        tmp_path,
        authority_plan_sha256=provisional_promoting_plan_sha256_v8(plans),
    )
    try:
        composition = _v8_composition(fixture, plans)
        runtime_owner = object()
        with pytest.raises(TypeError, match="can only be created"):
            PublicWebSocketRuntimeRunTokenV8(
                composition=composition,
                runtime_owner=runtime_owner,
                _factory_token=object(),
            )
        with pytest.raises(TypeError, match="concrete object identity"):
            composition.claim_exclusive_runtime_v8(None)  # type: ignore[arg-type]

        token = composition.claim_exclusive_runtime_v8(runtime_owner)
        composition.validate_exclusive_runtime_claim_v8(
            token,
            runtime_owner=runtime_owner,
        )
        with pytest.raises(
            PublicWebSocketRuntimeClaimErrorV8,
            match="already has an exclusive runtime owner",
        ):
            composition.claim_exclusive_runtime_v8(object())
        assert composition.owner.generation == 0
        assert fixture.connector.calls == []
    finally:
        fixture.close()


@pytest.mark.asyncio
async def test_v8_runtime_start_barrier_is_factory_sealed_and_releases_exact_pair(
    tmp_path: Path,
) -> None:
    plans = build_provisional_promoting_capture_plans_v8(("BTCUSDT",))
    fixture = _Fixture(
        tmp_path,
        authority_plan_sha256=provisional_promoting_plan_sha256_v8(plans),
    )
    try:
        market = _v8_composition(fixture, plans, "usdm_market")
        public = _v8_composition(fixture, plans, "usdm_public")
        compositions = (market, public)
        runtime_owner = object()

        with pytest.raises(TypeError, match="can only be created"):
            PublicWebSocketRuntimeStartBarrierV8(
                compositions=compositions,
                runtime_owner=runtime_owner,
                _factory_token=object(),
            )
        with pytest.raises(ValueError, match="two distinct owners"):
            create_public_websocket_runtime_start_barrier_v8(
                (market, market),
                runtime_owner=runtime_owner,
            )
        with pytest.raises(ValueError, match="canonical route order"):
            create_public_websocket_runtime_start_barrier_v8(
                (public, market),
                runtime_owner=runtime_owner,
            )

        barrier = create_public_websocket_runtime_start_barrier_v8(
            compositions,
            runtime_owner=runtime_owner,
        )
        assert barrier.validate_member(market, runtime_owner=runtime_owner) == 0
        assert barrier.validate_member(public, runtime_owner=runtime_owner) == 1
        with pytest.raises(
            PublicWebSocketRuntimeClaimErrorV8,
            match="foreign runtime",
        ):
            barrier.validate_member(market, runtime_owner=object())

        market_token = market.claim_exclusive_runtime_v8(runtime_owner)
        public_token = public.claim_exclusive_runtime_v8(runtime_owner)
        market.lifecycle_coordinator.stop_event.set()
        results = await asyncio.gather(
            market.run_exclusive_runtime_v8(
                market_token,
                runtime_owner=runtime_owner,
                startup_barrier=barrier,
            ),
            public.run_exclusive_runtime_v8(
                public_token,
                runtime_owner=runtime_owner,
                startup_barrier=barrier,
            ),
        )
        assert results == [None, None]
        assert market.owner.generation == 0
        assert public.owner.generation == 0
        assert fixture.connector.calls == []
    finally:
        fixture.close()


@pytest.mark.asyncio
async def test_v8_runtime_start_barrier_latches_pre_release_cancellation(
    tmp_path: Path,
) -> None:
    plans = build_provisional_promoting_capture_plans_v8(("BTCUSDT",))
    fixture = _Fixture(
        tmp_path,
        authority_plan_sha256=provisional_promoting_plan_sha256_v8(plans),
    )
    try:
        market = _v8_composition(fixture, plans, "usdm_market")
        public = _v8_composition(fixture, plans, "usdm_public")
        runtime_owner = object()
        barrier = create_public_websocket_runtime_start_barrier_v8(
            (market, public),
            runtime_owner=runtime_owner,
        )
        market_token = market.claim_exclusive_runtime_v8(runtime_owner)
        public_token = public.claim_exclusive_runtime_v8(runtime_owner)
        market_task = asyncio.create_task(
            market.run_exclusive_runtime_v8(
                market_token,
                runtime_owner=runtime_owner,
                startup_barrier=barrier,
            )
        )
        for _attempt in range(100):
            if market._runtime_ownership.authorized_task is market_task:
                break
            await asyncio.sleep(0)
        assert market._runtime_ownership.authorized_task is market_task
        assert not market_task.done()

        market_task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await market_task
        with pytest.raises(
            PublicWebSocketRuntimeClaimErrorV8,
            match="failure-latched",
        ):
            await public.run_exclusive_runtime_v8(
                public_token,
                runtime_owner=runtime_owner,
                startup_barrier=barrier,
            )
        assert market.owner.generation == 0
        assert public.owner.generation == 0
        assert fixture.connector.calls == []
    finally:
        fixture.close()


@pytest.mark.asyncio
async def test_v8_runtime_token_allows_one_owned_run_and_blocks_all_replay(
    tmp_path: Path,
) -> None:
    plans = build_provisional_promoting_capture_plans_v8(("BTCUSDT",))
    fixture = _Fixture(
        tmp_path,
        authority_plan_sha256=provisional_promoting_plan_sha256_v8(plans),
    )
    try:
        composition = _v8_composition(fixture, plans)
        runtime_owner = object()
        token = composition.claim_exclusive_runtime_v8(runtime_owner)
        composition.lifecycle_coordinator.stop_event.set()

        assert (
            await composition.run_exclusive_runtime_v8(
                token,
                runtime_owner=runtime_owner,
            )
            is None
        )
        with pytest.raises(
            PublicWebSocketRuntimeClaimErrorV8,
            match="direct run",
        ):
            await composition.run()
        with pytest.raises(
            PublicWebSocketRuntimeClaimErrorV8,
            match="one-shot",
        ):
            await composition.run_exclusive_runtime_v8(
                token,
                runtime_owner=runtime_owner,
            )
        with pytest.raises(
            PublicWebSocketRuntimeClaimErrorV8,
            match="cannot be released",
        ):
            composition._release_exclusive_runtime_claim_v8(
                token,
                runtime_owner=runtime_owner,
            )

        # Authorization belongs only to the active exclusive-run call, not to
        # the surrounding task forever.  Clearing the synthetic stop signal
        # makes a same-task direct owner splice reach the admission guard.
        composition.lifecycle_coordinator.stop_event.clear()
        with pytest.raises(
            PublicWebSocketRuntimeClaimErrorV8,
            match="foreign task",
        ):
            await composition.owner.run(
                composition.lifecycle_coordinator.stop_event
            )
        assert composition.owner.generation == 0
        assert fixture.connector.calls == []
    finally:
        fixture.close()


@pytest.mark.asyncio
async def test_v8_claim_rejects_direct_owner_task_before_any_transition(
    tmp_path: Path,
) -> None:
    plans = build_provisional_promoting_capture_plans_v8(("BTCUSDT",))
    fixture = _Fixture(
        tmp_path,
        authority_plan_sha256=provisional_promoting_plan_sha256_v8(plans),
    )
    try:
        composition = _v8_composition(fixture, plans)
        composition.claim_exclusive_runtime_v8(object())
        events_before = fixture.ledger.events

        with pytest.raises(
            PublicWebSocketRuntimeClaimErrorV8,
            match="foreign task",
        ):
            await composition.owner.run(
                composition.lifecycle_coordinator.stop_event
            )
        assert composition.owner.generation == 0
        assert fixture.connector.calls == []
        assert fixture.ledger.events == events_before
    finally:
        fixture.close()


@pytest.mark.asyncio
async def test_v8_active_runtime_claim_is_bound_to_its_exact_asyncio_task(
    tmp_path: Path,
) -> None:
    plans = build_provisional_promoting_capture_plans_v8(("BTCUSDT",))
    fixture = _Fixture(
        tmp_path,
        authority_plan_sha256=provisional_promoting_plan_sha256_v8(plans),
    )
    try:
        hook_entered = asyncio.Event()
        release_hook = asyncio.Event()

        async def block_preconnecting_generation(_context: object) -> None:
            hook_entered.set()
            await release_hook.wait()

        plan = _v8_websocket_plan(plans)
        lifecycle, factory, _unused_owner = _v8_boundaries(fixture, plans, plan)
        owner = PublicWebSocketCaptureOwner(
            build_public_websocket_owner_plan_v2(plan),
            plan_sha256=fixture.authority.plan_sha256,
            process_boot_id=PROCESS_BOOT_ID,
            settings=_settings(),
            connector=cast(Connector, fixture.connector),
            frame_adapter_factory=factory,
            lifecycle_coordinator=lifecycle,
            preconnecting_generation_hook=block_preconnecting_generation,
        )
        composition = create_public_websocket_owner_composition_v8(
            session_start_authority=fixture.session_start_authority,
            writer_lease=fixture.lease,
            promoting_plans=plans,
            plan=plan,
            recovered_wal_tail_ingest_seq=RECOVERED_TAIL,
            owner=owner,
            frame_adapter_factory=factory,
            lifecycle_coordinator=lifecycle,
        )
        runtime_owner = object()
        token = composition.claim_exclusive_runtime_v8(runtime_owner)
        run_task = asyncio.create_task(
            composition.run_exclusive_runtime_v8(
                token,
                runtime_owner=runtime_owner,
            )
        )
        try:
            await asyncio.wait_for(hook_entered.wait(), timeout=1.0)
            with pytest.raises(
                PublicWebSocketRuntimeClaimErrorV8,
                match="foreign task",
            ):
                composition.validate_current()
        finally:
            lifecycle.stop_event.set()
            release_hook.set()

        assert await asyncio.wait_for(run_task, timeout=1.0) is None
        assert owner.generation == 1
        assert fixture.connector.calls == []
        assert fixture.ledger.events == ()
    finally:
        fixture.close()


def test_v8_prestart_claim_rollback_invalidates_old_token_and_allows_reclaim(
    tmp_path: Path,
) -> None:
    plans = build_provisional_promoting_capture_plans_v8(("BTCUSDT",))
    fixture = _Fixture(
        tmp_path,
        authority_plan_sha256=provisional_promoting_plan_sha256_v8(plans),
    )
    try:
        composition = _v8_composition(fixture, plans)
        first_owner = object()
        first_token = composition.claim_exclusive_runtime_v8(first_owner)
        composition._release_exclusive_runtime_claim_v8(
            first_token,
            runtime_owner=first_owner,
        )
        with pytest.raises(
            PublicWebSocketRuntimeClaimErrorV8,
            match="foreign, stale, or replayed",
        ):
            composition.validate_exclusive_runtime_claim_v8(
                first_token,
                runtime_owner=first_owner,
            )

        second_owner = object()
        second_token = composition.claim_exclusive_runtime_v8(second_owner)
        composition.validate_exclusive_runtime_claim_v8(
            second_token,
            runtime_owner=second_owner,
        )
        assert second_token is not first_token
        assert composition.owner.generation == 0
        assert fixture.connector.calls == []
    finally:
        fixture.close()


def test_v8_claim_rejects_foreign_owner_and_foreign_composition_token(
    tmp_path: Path,
) -> None:
    first_path = tmp_path / "first"
    second_path = tmp_path / "second"
    first_path.mkdir()
    second_path.mkdir()
    plans = build_provisional_promoting_capture_plans_v8(("BTCUSDT",))
    plan_hash = provisional_promoting_plan_sha256_v8(plans)
    first = _Fixture(first_path, authority_plan_sha256=plan_hash)
    second = _Fixture(second_path, authority_plan_sha256=plan_hash)
    try:
        first_composition = _v8_composition(first, plans)
        second_composition = _v8_composition(second, plans)
        first_owner = object()
        first_token = first_composition.claim_exclusive_runtime_v8(first_owner)
        second_owner = object()
        second_token = second_composition.claim_exclusive_runtime_v8(second_owner)

        with pytest.raises(
            PublicWebSocketRuntimeClaimErrorV8,
            match="foreign, stale, or replayed",
        ):
            first_composition.validate_exclusive_runtime_claim_v8(
                first_token,
                runtime_owner=object(),
            )
        with pytest.raises(
            PublicWebSocketRuntimeClaimErrorV8,
            match="foreign, stale, or replayed",
        ):
            first_composition.validate_exclusive_runtime_claim_v8(
                second_token,
                runtime_owner=second_owner,
            )
        assert first.connector.calls == []
        assert second.connector.calls == []
    finally:
        second.close()
        first.close()


def test_v2_and_v8_runtime_tokens_cannot_cross_composition_versions(
    tmp_path: Path,
) -> None:
    v2_path = tmp_path / "v2"
    v8_path = tmp_path / "v8"
    v2_path.mkdir()
    v8_path.mkdir()
    plans_v8 = build_provisional_promoting_capture_plans_v8(("BTCUSDT",))
    v2_fixture = _Fixture(v2_path)
    v8_fixture = _Fixture(
        v8_path,
        authority_plan_sha256=provisional_promoting_plan_sha256_v8(plans_v8),
    )
    try:
        v2_composition = v2_fixture.compose()
        v8_composition = _v8_composition(v8_fixture, plans_v8)
        v2_owner = object()
        v8_owner = object()
        v2_token = v2_composition.claim_exclusive_runtime_v2(v2_owner)
        v8_token = v8_composition.claim_exclusive_runtime_v8(v8_owner)

        with pytest.raises(TypeError, match="exact PublicWebSocketRuntimeRunTokenV8"):
            v8_composition.validate_exclusive_runtime_claim_v8(
                cast(PublicWebSocketRuntimeRunTokenV8, v2_token),
                runtime_owner=v2_owner,
            )
        with pytest.raises(TypeError, match="exact PublicWebSocketRuntimeRunTokenV2"):
            v2_composition.validate_exclusive_runtime_claim_v2(
                cast(PublicWebSocketRuntimeRunTokenV2, v8_token),
                runtime_owner=v8_owner,
            )
        assert v2_fixture.connector.calls == []
        assert v8_fixture.connector.calls == []
    finally:
        v8_fixture.close()
        v2_fixture.close()


def test_v8_concurrent_claim_is_atomic(tmp_path: Path) -> None:
    plans = build_provisional_promoting_capture_plans_v8(("BTCUSDT",))
    fixture = _Fixture(
        tmp_path,
        authority_plan_sha256=provisional_promoting_plan_sha256_v8(plans),
    )
    try:
        composition = _v8_composition(fixture, plans)
        barrier = threading.Barrier(3)
        result_lock = threading.Lock()
        successes: list[tuple[object, PublicWebSocketRuntimeRunTokenV8]] = []
        failures: list[BaseException] = []

        def claim(runtime_owner: object) -> None:
            barrier.wait()
            try:
                token = composition.claim_exclusive_runtime_v8(runtime_owner)
            except BaseException as exc:
                with result_lock:
                    failures.append(exc)
            else:
                with result_lock:
                    successes.append((runtime_owner, token))

        threads = tuple(
            threading.Thread(target=claim, args=(object(),)) for _ in range(2)
        )
        for thread in threads:
            thread.start()
        barrier.wait()
        for thread in threads:
            thread.join(timeout=5)

        assert all(not thread.is_alive() for thread in threads)
        assert len(successes) == 1
        assert len(failures) == 1
        assert isinstance(failures[0], PublicWebSocketRuntimeClaimErrorV8)
        winner, token = successes[0]
        composition.validate_exclusive_runtime_claim_v8(
            token,
            runtime_owner=winner,
        )
        assert composition.owner.generation == 0
        assert fixture.connector.calls == []
    finally:
        fixture.close()


def test_v8_claim_rejects_owner_with_prior_generation(tmp_path: Path) -> None:
    plans = build_provisional_promoting_capture_plans_v8(("BTCUSDT",))
    fixture = _Fixture(
        tmp_path,
        authority_plan_sha256=provisional_promoting_plan_sha256_v8(plans),
    )
    try:
        composition = _v8_composition(fixture, plans)
        object.__setattr__(composition.owner, "_generation", 1)

        with pytest.raises(
            PublicWebSocketRuntimeClaimErrorV8,
            match="already started",
        ):
            composition.claim_exclusive_runtime_v8(object())
        assert fixture.connector.calls == []
    finally:
        fixture.close()


def test_v8_prestart_rollback_rejects_owner_generation_advance(
    tmp_path: Path,
) -> None:
    plans = build_provisional_promoting_capture_plans_v8(("BTCUSDT",))
    fixture = _Fixture(
        tmp_path,
        authority_plan_sha256=provisional_promoting_plan_sha256_v8(plans),
    )
    try:
        composition = _v8_composition(fixture, plans)
        runtime_owner = object()
        token = composition.claim_exclusive_runtime_v8(runtime_owner)
        object.__setattr__(composition.owner, "_generation", 1)

        with pytest.raises(
            PublicWebSocketRuntimeClaimErrorV8,
            match="cannot be released",
        ):
            composition._release_exclusive_runtime_claim_v8(
                token,
                runtime_owner=runtime_owner,
            )
        assert fixture.connector.calls == []
    finally:
        fixture.close()


@pytest.mark.asyncio
async def test_runtime_claim_rejects_direct_owner_before_generation_or_transition(
    tmp_path: Path,
) -> None:
    fixture = _Fixture(tmp_path)
    try:
        composition = fixture.compose()
        composition.claim_exclusive_runtime_v2(object())
        events_before = fixture.ledger.events

        with pytest.raises(
            PublicWebSocketRuntimeClaimErrorV2,
            match="foreign task",
        ):
            await composition.owner.run(composition.lifecycle_coordinator.stop_event)

        assert composition.owner.generation == 0
        assert fixture.connector.calls == []
        assert fixture.ledger.events == events_before
    finally:
        fixture.close()


@pytest.mark.asyncio
async def test_runtime_token_allows_one_owned_run_and_rejects_direct_or_replay(
    tmp_path: Path,
) -> None:
    fixture = _Fixture(tmp_path)
    try:
        composition = fixture.compose()
        runtime_owner = object()
        token = composition.claim_exclusive_runtime_v2(runtime_owner)
        composition.lifecycle_coordinator.stop_event.set()

        receipt = await composition.run_exclusive_runtime_v2(
            token,
            runtime_owner=runtime_owner,
        )
        assert receipt is None

        with pytest.raises(PublicWebSocketRuntimeClaimErrorV2, match="direct run"):
            await composition.run()
        with pytest.raises(PublicWebSocketRuntimeClaimErrorV2, match="one-shot"):
            await composition.run_exclusive_runtime_v2(
                token,
                runtime_owner=runtime_owner,
            )
        assert fixture.connector.calls == []
    finally:
        fixture.close()


def test_owner_url_plan_a_vs_factory_lifecycle_plan_b_rejects_before_connector(
    tmp_path: Path,
) -> None:
    fixture = _Fixture(tmp_path)
    try:
        public = _websocket_plan(fixture.plans, "usdm_public")
        bad_owner = fixture.owner_for(
            public,
            fixture.factory,
            fixture.lifecycle,
        )

        with pytest.raises(
            PublicWebSocketCompositionErrorV2,
            match="owner URL or route plan differs",
        ):
            fixture.compose(owner=bad_owner)
        assert fixture.connector.calls == []
    finally:
        fixture.close()


def test_lifecycle_route_a_vs_factory_owner_route_b_rejects_before_connector(
    tmp_path: Path,
) -> None:
    fixture = _Fixture(tmp_path)
    try:
        public = _websocket_plan(fixture.plans, "usdm_public")
        public_factory = fixture.factory_for(
            public,
            lifecycle=fixture.lifecycle,
        )
        public_owner = fixture.owner_for(
            public,
            public_factory,
            fixture.lifecycle,
        )

        with pytest.raises(
            PublicWebSocketCompositionErrorV2,
            match="lifecycle route metadata differs",
        ):
            fixture.compose(
                plan=public,
                owner=public_owner,
                factory=public_factory,
            )
        assert fixture.connector.calls == []
    finally:
        fixture.close()


@pytest.mark.parametrize(
    ("mismatch", "message"),
    [
        ("factory_session", "factory session ID differs"),
        ("factory_protocol", "factory protocol hash differs"),
        ("owner_boot", "owner process boot ID differs"),
        ("lifecycle_boot", "lifecycle process boot ID differs"),
    ],
)
def test_lineage_mismatch_rejects_before_connector(
    tmp_path: Path,
    mismatch: str,
    message: str,
) -> None:
    fixture = _Fixture(tmp_path)
    try:
        lifecycle = fixture.lifecycle
        if mismatch == "lifecycle_boot":
            lifecycle = fixture.lifecycle_for(
                fixture.plan,
                process_boot_id="fedcba9876543210fedcba9876543210",
            )
        factory = fixture.factory_for(
            fixture.plan,
            lifecycle=lifecycle,
            session_id=("wrong-session" if mismatch == "factory_session" else None),
            protocol_hash=("0" * 64 if mismatch == "factory_protocol" else HASH),
        )
        owner = fixture.owner_for(
            fixture.plan,
            factory,
            lifecycle,
            process_boot_id=(
                "fedcba9876543210fedcba9876543210" if mismatch == "owner_boot" else PROCESS_BOOT_ID
            ),
        )

        with pytest.raises(PublicWebSocketCompositionErrorV2, match=message):
            fixture.compose(owner=owner, factory=factory, lifecycle=lifecycle)
        assert fixture.connector.calls == []
    finally:
        fixture.close()


def test_recovered_tail_mismatch_rejects_before_connector(tmp_path: Path) -> None:
    fixture = _Fixture(tmp_path)
    try:
        with pytest.raises(
            PublicWebSocketCompositionErrorV2,
            match="ingress differs from the admitted recovered WAL tail",
        ):
            fixture.compose(recovered_tail=RECOVERED_TAIL + 1)
        assert fixture.connector.calls == []
    finally:
        fixture.close()


def test_released_or_different_writer_lease_rejects_before_connector(
    tmp_path: Path,
) -> None:
    fixture = _Fixture(tmp_path)
    other_scope = tmp_path / "other-scope"
    other_scope.mkdir()
    other_lease = WriterLease.acquire(other_scope)
    try:
        with pytest.raises(
            PublicWebSocketCompositionErrorV2,
            match="writer-lease scope differs",
        ):
            fixture.compose(writer_lease=other_lease)
        assert fixture.connector.calls == []

        fixture.lease.release()
        with pytest.raises(WriterLeaseNotHeldError, match="released"):
            fixture.compose()
        assert fixture.connector.calls == []
    finally:
        other_lease.release()
        fixture.close()


def test_missing_or_different_pipeline_writer_lease_rejects_before_connector(
    tmp_path: Path,
) -> None:
    fixture = _Fixture(tmp_path)
    other_scope = tmp_path / "pipeline-other-scope"
    other_scope.mkdir()
    other_lease = WriterLease.acquire(other_scope)
    try:
        fixture.durable_writer.writer_lease = None
        with pytest.raises(
            PublicWebSocketCompositionErrorV2,
            match="durable writer is not bound",
        ):
            fixture.compose()
        assert fixture.connector.calls == []

        fixture.durable_writer.writer_lease = other_lease
        with pytest.raises(
            PublicWebSocketCompositionErrorV2,
            match="durable writer is not bound",
        ):
            fixture.compose()
        assert fixture.connector.calls == []
    finally:
        other_lease.release()
        fixture.close()


def test_copied_bindings_in_different_wal_paths_reject_before_connector(
    tmp_path: Path,
) -> None:
    fixture = _Fixture(tmp_path)
    try:
        copied_primary = fixture.scope / "copied-primary"
        copied_mirror = fixture.scope / "copied-mirror"
        copied_primary.mkdir()
        copied_mirror.mkdir()
        for source, destination in (
            (fixture.primary_path, copied_primary),
            (fixture.mirror_path, copied_mirror),
        ):
            destination.joinpath("storage-root-binding.json").write_bytes(
                source.joinpath("storage-root-binding.json").read_bytes()
            )
        alternate = MirroredWalWriterV2(
            copied_primary,
            copied_mirror,
            authority=fixture.authority,
            policy=fixture.wal_writer.policy,
            selection_receipt=fixture.selection_receipt,
            primary_maximum_total_bytes=MAXIMUM_BYTES,
            mirror_maximum_total_bytes=MAXIMUM_BYTES,
            primary_emergency_reserve_bytes=RESERVE_BYTES,
            mirror_emergency_reserve_bytes=RESERVE_BYTES,
            primary_failure_domain_id="device-primary",
            mirror_failure_domain_id="device-mirror",
        )
        fixture.durable_writer.wal_writer = alternate

        with pytest.raises(
            PublicWebSocketCompositionErrorV2,
            match="dual-WAL root paths differ",
        ):
            fixture.compose()
        assert fixture.connector.calls == []
    finally:
        fixture.close()


def test_plan_bundle_authority_mismatch_rejects_before_connector(
    tmp_path: Path,
) -> None:
    fixture = _Fixture(tmp_path)
    try:
        other_plans = build_provisional_promoting_capture_plans_v2(("ETHUSDT",))
        other_plan = _websocket_plan(other_plans, "usdm_market")
        other_lifecycle = WebSocketLifecycleFatalCoordinatorV2(
            other_plans,
            other_plan,
            session_id=fixture.session_id,
            process_boot_id=PROCESS_BOOT_ID,
            session_started_at=ReceiptTimestamp(
                fixture.started_wall_ms,
                fixture.started_monotonic_ns,
            ),
            source_component="v2-owner-usdm_market",
            clock=fixture.clock,
            pipeline=fixture.pipeline,  # pyright: ignore[reportArgumentType]
            integrity_ledger=cast(CaptureIntegrityLedgerV2, fixture.ledger),
            finality_timeout_seconds=1.0,
        )
        other_factory = fixture.factory_for(
            other_plan,
            lifecycle=other_lifecycle,
        )
        other_owner = fixture.owner_for(
            other_plan,
            other_factory,
            other_lifecycle,
        )

        with pytest.raises(
            PublicWebSocketCompositionErrorV2,
            match="plan bundle differs from the session-start WAL authority",
        ):
            fixture.compose(
                plans=other_plans,
                plan=other_plan,
                owner=other_owner,
                factory=other_factory,
                lifecycle=other_lifecycle,
            )
        assert fixture.connector.calls == []
    finally:
        fixture.close()


def test_start_manifest_root_tamper_rejects_before_connector(tmp_path: Path) -> None:
    fixture = _Fixture(tmp_path)
    try:
        binding_path = fixture.ledger_path / "storage-root-binding.json"
        binding_path.write_bytes(binding_path.read_bytes() + b" ")

        with pytest.raises(
            SessionAuthorityIntegrityError,
            match="differs from its expected current bytes",
        ):
            fixture.compose()
        assert fixture.connector.calls == []
    finally:
        fixture.close()


def test_same_manifest_bytes_copied_to_another_path_do_not_admit(
    tmp_path: Path,
) -> None:
    fixture = _Fixture(tmp_path)
    try:
        copied_path = fixture.scope / "copied-session-start.json"
        copied_path.write_bytes(fixture.session_start_path.read_bytes())
        object.__setattr__(
            fixture.session_start_authority,
            "canonical_path",
            str(copied_path.resolve()).lower(),
        )

        with pytest.raises(
            SessionAuthorityIntegrityError,
            match="file identity differs from its receipt",
        ):
            fixture.compose()
        assert fixture.connector.calls == []
    finally:
        fixture.close()


def test_persisted_start_file_byte_tamper_rejects_before_connector(
    tmp_path: Path,
) -> None:
    fixture = _Fixture(tmp_path)
    try:
        fixture.session_start_path.write_bytes(fixture.session_start_path.read_bytes() + b" ")

        with pytest.raises(
            SessionAuthorityIntegrityError,
            match="byte length differs",
        ):
            fixture.compose()
        assert fixture.connector.calls == []
    finally:
        fixture.close()


def test_writer_lease_subclass_is_rejected_before_connector(tmp_path: Path) -> None:
    fixture = _Fixture(tmp_path)
    try:
        forged = cast(WriterLease, object.__new__(_WriterLeaseSubclass))
        with pytest.raises(TypeError, match="writer_lease must be a WriterLease"):
            fixture.compose(writer_lease=forged)
        assert fixture.connector.calls == []
    finally:
        fixture.close()


def test_raw_manifest_cannot_replace_factory_persisted_authority(
    tmp_path: Path,
) -> None:
    fixture = _Fixture(tmp_path)
    try:
        forged = cast(PersistedSessionStartAuthorityV2, fixture.session_start)
        with pytest.raises(TypeError, match="PersistedSessionStartAuthorityV2"):
            fixture.compose(session_start_authority=forged)
        assert fixture.connector.calls == []
    finally:
        fixture.close()


def test_unlinked_start_cannot_fork_receipt_or_reach_connector_or_ack(
    tmp_path: Path,
) -> None:
    fixture = _Fixture(tmp_path)
    try:
        original_receipt = fixture.session_start_authority
        fixture.session_start_path.unlink()

        with pytest.raises(SessionAuthorityExistsError, match="already consumed"):
            write_session_start_manifest_v2(
                fixture.session_start_path,
                lease=fixture.lease,
                session_id=fixture.session_id,
                process_boot_id=PROCESS_BOOT_ID,
                started_wall_ms=fixture.started_wall_ms,
                started_monotonic_ns=fixture.started_monotonic_ns,
                wal_authority=fixture.authority,
                wal_durability_binding=fixture.durability,
                block_policy=fixture.block_policy,
                block_signing_authority=fixture.block_signing_authority,
                stream_group_id=STREAM_GROUP_ID,
                segment_id=SEGMENT_ID,
                integrity_ledger_max_events=fixture.integrity_ledger_max_events,
                storage_root_directories=(
                    fixture.primary_path,
                    fixture.mirror_path,
                    fixture.block_path,
                    fixture.ledger_path,
                ),
                grouped_block_root_binding=fixture.block,
                integrity_ledger_root_binding=fixture.ledger_binding,
                previous_closure_sha256="9" * 64,
            )

        with pytest.raises(SessionAuthorityIntegrityError):
            assert_persisted_session_start_authority_current_v2(
                original_receipt,
                lease=fixture.lease,
            )
        with pytest.raises(SessionAuthorityIntegrityError):
            fixture.compose()

        assert not fixture.session_start_path.exists()
        assert fixture.connector.calls == []
        assert fixture.wal_writer.durable_ack_seq == 0
        assert fixture.ledger.events == ()
    finally:
        fixture.close()


def test_recreated_same_path_root_with_copied_binding_is_rejected_by_identity(
    tmp_path: Path,
) -> None:
    fixture = _Fixture(tmp_path)
    try:
        moved = fixture.scope / "ledger-moved"
        fixture.ledger_path.rename(moved)
        fixture.ledger_path.mkdir()
        fixture.ledger_path.joinpath("storage-root-binding.json").write_bytes(
            moved.joinpath("storage-root-binding.json").read_bytes()
        )

        with pytest.raises(
            SessionAuthorityIntegrityError,
            match="storage-root reference changed",
        ):
            fixture.compose()
        assert fixture.connector.calls == []
    finally:
        fixture.close()


def test_raw_exact_v2_owner_default_false_is_forced_unbound_and_never_connects(
    tmp_path: Path,
) -> None:
    fixture = _Fixture(tmp_path)
    try:
        owner = PublicWebSocketCaptureOwner(
            build_public_websocket_owner_plan_v2(fixture.plan),
            plan_sha256=fixture.authority.plan_sha256,
            process_boot_id=PROCESS_BOOT_ID,
            settings=_settings(),
            connector=cast(Connector, fixture.connector),
            frame_adapter_factory=fixture.factory,
            lifecycle_coordinator=fixture.lifecycle,
        )
        assert owner.requires_preconnect_admission

        future = asyncio.run_coroutine_threadsafe(
            owner.run(fixture.lifecycle.stop_event),
            fixture.loop_thread.loop,
        )
        with pytest.raises(RuntimeError, match="requires a bound preconnect"):
            future.result(timeout=2)

        assert fixture.connector.calls == []
        assert fixture.ledger.events == ()
    finally:
        fixture.close()


def test_exact_v2_owner_rejects_structural_noop_guard_and_never_connects(
    tmp_path: Path,
) -> None:
    fixture = _Fixture(tmp_path)
    try:
        owner = PublicWebSocketCaptureOwner(
            build_public_websocket_owner_plan_v2(fixture.plan),
            plan_sha256=fixture.authority.plan_sha256,
            process_boot_id=PROCESS_BOOT_ID,
            settings=_settings(),
            connector=cast(Connector, fixture.connector),
            frame_adapter_factory=fixture.factory,
            lifecycle_coordinator=fixture.lifecycle,
        )
        with pytest.raises(TypeError, match="exact production composition"):
            owner.bind_preconnect_admission_guard(cast(object, _NoOpGuard()))  # type: ignore[arg-type]

        future = asyncio.run_coroutine_threadsafe(
            owner.run(fixture.lifecycle.stop_event),
            fixture.loop_thread.loop,
        )
        with pytest.raises(RuntimeError, match="requires a bound preconnect"):
            future.result(timeout=2)
        assert fixture.connector.calls == []
        assert fixture.ledger.events == ()
    finally:
        fixture.close()


def test_partial_v2_factory_or_lifecycle_pairs_are_rejected_before_connector(
    tmp_path: Path,
) -> None:
    fixture = _Fixture(tmp_path)
    try:
        with pytest.raises(TypeError, match="must be paired exactly"):
            PublicWebSocketCaptureOwner(
                build_public_websocket_owner_plan_v2(fixture.plan),
                plan_sha256=fixture.authority.plan_sha256,
                process_boot_id=PROCESS_BOOT_ID,
                settings=_settings(),
                connector=cast(Connector, fixture.connector),
                frame_adapter_factory=fixture.factory,
                lifecycle_coordinator=cast(object, _PartialLifecycle()),  # type: ignore[arg-type]
            )
        with pytest.raises(TypeError, match="must be paired exactly"):
            PublicWebSocketCaptureOwner(
                build_public_websocket_owner_plan_v2(fixture.plan),
                plan_sha256=fixture.authority.plan_sha256,
                process_boot_id=PROCESS_BOOT_ID,
                settings=_settings(),
                connector=cast(Connector, fixture.connector),
                frame_adapter_factory=cast(object, _PartialFactory()),  # type: ignore[arg-type]
                lifecycle_coordinator=fixture.lifecycle,
            )
        assert fixture.connector.calls == []
        assert fixture.ledger.events == ()
    finally:
        fixture.close()


def test_both_v2_boundary_subclasses_are_rejected_before_connector(
    tmp_path: Path,
) -> None:
    fixture = _Fixture(tmp_path)
    try:
        lifecycle = _LifecycleCoordinatorV2Subclass(
            fixture.plans,
            fixture.plan,
            session_id=fixture.session_id,
            process_boot_id=PROCESS_BOOT_ID,
            session_started_at=ReceiptTimestamp(
                fixture.started_wall_ms,
                fixture.started_monotonic_ns,
            ),
            source_component=f"v2-owner-{fixture.plan.route_id}",
            clock=fixture.clock,
            pipeline=fixture.pipeline,
            integrity_ledger=fixture.ledger,
            finality_timeout_seconds=1.0,
        )
        factory = _FrameAdapterFactoryV2Subclass(
            fixture.plan,
            session_id=fixture.session_id,
            protocol_hash=HASH,
            clock=fixture.clock,
            ingress=fixture.ingress,
            recovery_lifecycle=lifecycle,
        )

        with pytest.raises(TypeError, match="must be paired exactly"):
            PublicWebSocketCaptureOwner(
                build_public_websocket_owner_plan_v2(fixture.plan),
                plan_sha256=fixture.authority.plan_sha256,
                process_boot_id=PROCESS_BOOT_ID,
                settings=_settings(),
                connector=cast(Connector, fixture.connector),
                frame_adapter_factory=factory,
                lifecycle_coordinator=lifecycle,
            )

        assert fixture.connector.calls == []
        assert fixture.ledger.events == ()
    finally:
        fixture.close()


def test_v2_factory_subclass_with_non_v2_lifecycle_is_rejected_before_connector(
    tmp_path: Path,
) -> None:
    fixture = _Fixture(tmp_path)
    try:
        factory = _FrameAdapterFactoryV2Subclass(
            fixture.plan,
            session_id=fixture.session_id,
            protocol_hash=HASH,
            clock=fixture.clock,
            ingress=fixture.ingress,
            recovery_lifecycle=fixture.lifecycle,
        )

        with pytest.raises(TypeError, match="must be paired exactly"):
            PublicWebSocketCaptureOwner(
                build_public_websocket_owner_plan_v2(fixture.plan),
                plan_sha256=fixture.authority.plan_sha256,
                process_boot_id=PROCESS_BOOT_ID,
                settings=_settings(),
                connector=cast(Connector, fixture.connector),
                frame_adapter_factory=factory,
                lifecycle_coordinator=cast(object, _PartialLifecycle()),  # type: ignore[arg-type]
            )
        with pytest.raises(TypeError, match="must be paired exactly"):
            PublicWebSocketCaptureOwner(
                build_public_websocket_owner_plan_v2(fixture.plan),
                plan_sha256=fixture.authority.plan_sha256,
                process_boot_id=PROCESS_BOOT_ID,
                pipeline=cast(CapturePipeline, fixture.pipeline),
                clock=fixture.clock,
                sequencer=IngestSequencer(),
                settings=_settings(),
                connector=cast(Connector, fixture.connector),
                frame_adapter_factory=factory,
            )

        assert fixture.connector.calls == []
        assert fixture.ledger.events == ()
    finally:
        fixture.close()


def test_v2_factory_rejects_structural_noop_ingress(tmp_path: Path) -> None:
    fixture = _Fixture(tmp_path)
    try:
        with pytest.raises(TypeError, match="exact SharedWebSocketIngressV2"):
            PublicWebSocketFrameAdapterFactoryV2(
                fixture.plan,
                session_id=fixture.session_id,
                protocol_hash=HASH,
                clock=fixture.clock,
                ingress=cast(SharedWebSocketIngressV2, _NoOpIngress()),
                recovery_lifecycle=fixture.lifecycle,
            )
        assert fixture.connector.calls == []
    finally:
        fixture.close()


def test_composition_rechecks_exact_ingress_after_factory_mutation(
    tmp_path: Path,
) -> None:
    fixture = _Fixture(tmp_path)
    try:
        fixture.factory.ingress = cast(SharedWebSocketIngressV2, _NoOpIngress())
        with pytest.raises(
            PublicWebSocketCompositionErrorV2,
            match="exact SharedWebSocketIngressV2",
        ):
            fixture.compose()
        assert fixture.connector.calls == []
    finally:
        fixture.close()


def test_unstarted_real_pipeline_rejects_before_connector(tmp_path: Path) -> None:
    fixture = _Fixture(tmp_path)
    try:
        handoff = BoundedBatchHandoffV2(
            fixture.batch_policy,
            expected_first_ingest_seq=1,
        )
        unstarted = CaptureBatchPipelineV2(handoff, fixture.durable_writer)
        lifecycle = fixture.lifecycle_for(fixture.plan, pipeline=unstarted)
        ingress = SharedWebSocketIngressV2(
            unstarted,
            recovered_wal_tail_ingest_seq=0,
        )
        factory = fixture.factory_for(
            fixture.plan,
            lifecycle=lifecycle,
            ingress=ingress,
        )
        owner = fixture.owner_for(fixture.plan, factory, lifecycle)

        with pytest.raises(RuntimeError, match="pipeline is not started"):
            fixture.compose(owner=owner, factory=factory, lifecycle=lifecycle)
        assert fixture.connector.calls == []
    finally:
        fixture.close()


def test_stopped_real_pipeline_and_closed_writer_reject_before_connector(
    tmp_path: Path,
) -> None:
    fixture = _Fixture(tmp_path)
    try:
        fixture.loop_thread.stop_pipeline(fixture.pipeline)
        with pytest.raises(RuntimeError, match="pipeline is not started"):
            fixture.compose()
        assert fixture.connector.calls == []
    finally:
        fixture.close()


def test_closed_real_writer_under_running_pipeline_rejects_before_connector(
    tmp_path: Path,
) -> None:
    fixture = _Fixture(tmp_path)
    try:
        fixture.durable_writer.abort()
        with pytest.raises(RuntimeError, match="writer is closed"):
            fixture.compose()
        assert fixture.connector.calls == []
    finally:
        fixture.close()


def test_all_websocket_routes_share_one_persisted_start_receipt_identity(
    tmp_path: Path,
) -> None:
    fixture = _Fixture(tmp_path)
    try:
        market_composition = fixture.compose()
        public = _websocket_plan(fixture.plans, "usdm_public")
        public_lifecycle = fixture.lifecycle_for(public)
        public_factory = fixture.factory_for(public, lifecycle=public_lifecycle)
        public_owner = fixture.owner_for(public, public_factory, public_lifecycle)
        public_composition = fixture.compose(
            plan=public,
            owner=public_owner,
            factory=public_factory,
            lifecycle=public_lifecycle,
        )

        assert (
            market_composition.session_start_authority
            is public_composition.session_start_authority
            is fixture.session_start_authority
        )
        assert fixture.connector.calls == []
    finally:
        fixture.close()


def test_handshake_window_manifest_tamper_closes_socket_before_connected_or_frame(
    tmp_path: Path,
) -> None:
    fixture = _Fixture(tmp_path)
    try:
        connection = _TamperingConnection(fixture.session_start_path)
        connector = _TamperingConnector(connection)
        owner = fixture.owner_for(
            fixture.plan,
            fixture.factory,
            fixture.lifecycle,
            connector=cast(Connector, connector),
        )
        states: list[ConnectionState] = []
        original = fixture.lifecycle.record_transition

        def traced_transition(
            _self: WebSocketLifecycleFatalCoordinatorV2,
            connection_id: str,
            *,
            generation: int,
            last_frame_seq: int,
            state: ConnectionState,
            reason: str,
        ) -> None:
            states.append(state)
            original(
                connection_id,
                generation=generation,
                last_frame_seq=last_frame_seq,
                state=state,
                reason=reason,
            )

        fixture.lifecycle.record_transition = MethodType(  # pyright: ignore[reportAttributeAccessIssue]
            traced_transition,
            fixture.lifecycle,
        )
        composition = fixture.compose(owner=owner)
        future = asyncio.run_coroutine_threadsafe(
            composition.run(),
            fixture.loop_thread.loop,
        )
        with pytest.raises(SessionAuthorityIntegrityError, match="byte length differs"):
            future.result(timeout=2)

        assert states == [ConnectionState.CONNECTING]
        assert connector.calls == [owner.plan.url]
        assert connection.closed
        assert not connection.frames.iterated
        assert len(fixture.ledger.events) == 1
    finally:
        fixture.close()
