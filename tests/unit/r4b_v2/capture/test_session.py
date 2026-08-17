from __future__ import annotations

import hashlib
import importlib.util
import os
import sys
import time
from dataclasses import replace
from pathlib import Path
from types import ModuleType
from typing import Any, cast

import pytest

from signalbot.capture.writer_lease import (
    WriterLease,
    WriterLeaseNotHeldError,
    WriterLeaseSessionClosureClaimError,
)
from signalbot.r4b_v2.canonical import canonical_json_line
from signalbot.r4b_v2.capture.authority import (
    StorageRootBindingV2,
    bind_storage_root_v2,
)
from signalbot.r4b_v2.capture.batching import (
    BatchPolicyV2,
    BoundedBatchHandoffV2,
)
from signalbot.r4b_v2.capture.block_container import (
    BlockSigningAuthorityV2,
    Ed25519BlockSignerV2,
)
from signalbot.r4b_v2.capture.blocks import (
    BlockPolicyV2,
    GroupedBlockBuilderV2,
    GroupedBlockWriterV2,
    grouped_block_root_contract_v2,
)
from signalbot.r4b_v2.capture.full_runtime import PublicCaptureRuntimeResultV8
from signalbot.r4b_v2.capture.integrity_ledger import (
    CaptureIntegrityLedgerV2,
    PersistedCaptureCleanClosureSealReceiptV2,
    PersistedCaptureCleanClosureSealReceiptV8,
    capture_integrity_ledger_root_contract_v2,
)
from signalbot.r4b_v2.capture.mirrored_wal import MirroredWalWriterV2
from signalbot.r4b_v2.capture.models import RawRecordV2, TransportV2, VenueV2
from signalbot.r4b_v2.capture.pipeline import (
    CaptureBatchPipelineV2,
    CaptureFinalityFenceReceiptV2,
    DurableCaptureBatchWriterV2,
)
from signalbot.r4b_v2.capture.plans import (
    ProvisionalDepthRestQualificationPlanV8,
    ProvisionalPromotingPlanV2,
    ProvisionalPromotingPlanV8,
    build_provisional_promoting_capture_plans_v2,
    build_provisional_promoting_capture_plans_v8,
    provisional_promoting_plan_sha256_v2,
    provisional_promoting_plan_sha256_v8,
)
from signalbot.r4b_v2.capture.rest_depth_bridge_evidence import (
    DepthBridgeCoordinatorCleanCloseReceiptV8,
    DepthBridgeCoordinatorClosureEntryV8,
)
from signalbot.r4b_v2.capture.session import (
    SESSION_CLOSURE_SUPPORTED_V2,
    PersistedSessionClosureAuthorityV2,
    PersistedSessionClosureAuthorityV8,
    PersistedSessionStartAuthorityV2,
    PlannedSourceCensusEntryV8,
    PlannedSourceCensusV8,
    SessionAuthorityExistsError,
    SessionAuthorityIntegrityError,
    SessionAuthorityWriteError,
    SessionClosureManifestV2,
    SessionClosureManifestV8,
    SessionStartManifestV2,
    _write_closure_once,
    assert_persisted_session_closure_authority_current_v2,
    assert_persisted_session_closure_authority_current_v8,
    assert_persisted_session_start_authority_current_v2,
    canonical_session_closure_manifest_path_v2,
    canonical_session_closure_manifest_path_v8,
    canonical_session_start_manifest_path_v2,
    write_session_closure_manifest_v2,
    write_session_closure_manifest_v8,
    write_session_start_manifest_v2,
)
from signalbot.r4b_v2.capture.wal import (
    WalAuthorityV2,
    WalDurabilityBindingV2,
    WalSyncPolicyV2,
)
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
from signalbot.r4b_v2.capture.websocket import PublicOiCensusAdmissionReceiptV2
from signalbot.r4b_v2.capture.websocket_finality import (
    FinalizedWebSocketRouteCursorPairV8,
    WebSocketRouteCursorClosureEntryV2,
)

HASH = "a" * 64
SELECTION_SHA256 = "f" * 64


def _authority() -> WalAuthorityV2:
    return WalAuthorityV2(
        attempt_id="attempt-session-authority",
        protocol_sha256=HASH,
        plan_sha256="b" * 64,
        source_manifest_sha256="c" * 64,
        schema_sha256="d" * 64,
        runtime_manifest_sha256="e" * 64,
    )


def _root_binding(
    root: Path,
    *,
    storage_kind: str,
    root_role: str,
    failure_domain_id: str,
    authority: WalAuthorityV2,
    contract: dict[str, object],
) -> StorageRootBindingV2:
    root.mkdir()
    return bind_storage_root_v2(
        root,
        storage_kind=storage_kind,
        root_role=root_role,
        failure_domain_id=failure_domain_id,
        authority_sha256=authority.sha256,
        contract=contract,
    )


class _Fixture:
    def __init__(self, tmp_path: Path) -> None:
        self.scope = tmp_path / "scope"
        self.scope.mkdir(parents=True)
        self.authority = _authority()
        self.wal_contract: dict[str, object] = {"qualified_policy": "sealed"}
        self.primary_path = self.scope / "wal-primary"
        self.mirror_path = self.scope / "wal-mirror"
        self.block_path = self.scope / "blocks"
        self.ledger_path = self.scope / "integrity-ledger"
        signer = Ed25519BlockSignerV2.from_private_key_bytes(
            key_id="session-test-writer",
            private_key_bytes=bytes(range(32)),
        )
        self.block_signing_authority = BlockSigningAuthorityV2.from_public_key_bytes(
            key_id=signer.key_id,
            public_key_bytes=signer.public_key_bytes,
        )
        self.block_policy = BlockPolicyV2(
            qualification_id="session-qualified-zstd",
            codec_candidate_id="session-zstd-candidate",
            compression_level=9,
            max_uncompressed_bytes=4 * 1024 * 1024,
            max_linger_ms=1_000,
        )
        self.stream_group_id = "binance-usdm-public-v2"
        self.segment_id = "segment-0001"
        self.integrity_ledger_max_events = 10_000
        self.primary = _root_binding(
            self.primary_path,
            storage_kind="WAL",
            root_role="PRIMARY",
            failure_domain_id="device-primary",
            authority=self.authority,
            contract=self.wal_contract,
        )
        self.mirror = _root_binding(
            self.mirror_path,
            storage_kind="WAL",
            root_role="INDEPENDENT_MIRROR",
            failure_domain_id="device-mirror",
            authority=self.authority,
            contract=self.wal_contract,
        )
        self.block = _root_binding(
            self.block_path,
            storage_kind="GROUPED_BLOCK",
            root_role="PROVISIONAL_SINGLE",
            failure_domain_id="device-block",
            authority=self.authority,
            contract=grouped_block_root_contract_v2(
                self.block_policy,
                self.block_signing_authority,
                self.stream_group_id,
                self.segment_id,
            ),
        )
        self.ledger = _root_binding(
            self.ledger_path,
            storage_kind="CAPTURE_INTEGRITY_LEDGER",
            root_role="APPEND_ONLY_PRIMARY",
            failure_domain_id="device-ledger",
            authority=self.authority,
            contract=capture_integrity_ledger_root_contract_v2(
                block_root_binding=self.block,
                block_directory=self.block_path,
                block_signing_authority=self.block_signing_authority,
                max_events=self.integrity_ledger_max_events,
            ),
        )
        self.durability = WalDurabilityBindingV2(
            mode="QUALIFIED_DUAL_OWNER",
            root_bindings=(self.primary, self.mirror),
            qualification_selection_receipt_sha256=SELECTION_SHA256,
            physical_failure_domain_independence_verified=False,
        )
        self.lease = WriterLease.acquire(self.scope)
        self.session_start_path = canonical_session_start_manifest_path_v2(self.lease)
        self.last_persisted: PersistedSessionStartAuthorityV2 | None = None

    def close(self) -> None:
        self.lease.release()

    def write(
        self,
        path: Path | None = None,
        *,
        lease: WriterLease | None = None,
        durability: WalDurabilityBindingV2 | None = None,
        block_policy: BlockPolicyV2 | None = None,
        block_signing_authority: BlockSigningAuthorityV2 | None = None,
        stream_group_id: str | None = None,
        segment_id: str | None = None,
        integrity_ledger_max_events: int | None = None,
        previous_closure_sha256: str | None = None,
        directories: tuple[
            str | Path,
            str | Path,
            str | Path,
            str | Path,
        ]
        | None = None,
    ) -> SessionStartManifestV2:
        selected_lease = lease or self.lease
        started_wall_ms = selected_lease.acquired_wall_ms + 1
        process_boot_id = "0123456789abcdef0123456789abcdef"
        persisted = write_session_start_manifest_v2(
            path or self.session_start_path,
            lease=selected_lease,
            session_id=f"{started_wall_ms}-{process_boot_id}",
            process_boot_id=process_boot_id,
            started_wall_ms=started_wall_ms,
            started_monotonic_ns=selected_lease.acquired_monotonic_ns + 1,
            wal_authority=self.authority,
            wal_durability_binding=durability or self.durability,
            block_policy=block_policy or self.block_policy,
            block_signing_authority=(block_signing_authority or self.block_signing_authority),
            stream_group_id=stream_group_id or self.stream_group_id,
            segment_id=segment_id or self.segment_id,
            integrity_ledger_max_events=(
                self.integrity_ledger_max_events
                if integrity_ledger_max_events is None
                else integrity_ledger_max_events
            ),
            storage_root_directories=directories
            or (
                self.primary_path,
                self.mirror_path,
                self.block_path,
                self.ledger_path,
            ),
            grouped_block_root_binding=self.block,
            integrity_ledger_root_binding=self.ledger,
            previous_closure_sha256=previous_closure_sha256,
        )
        self.last_persisted = persisted
        return persisted.manifest


CLOSURE_QUALIFICATION = "session-closure-wal-grid"
CLOSURE_WINDOW_START_MS = 2_000_000_000_000
CLOSURE_WINDOW_END_MS = CLOSURE_WINDOW_START_MS + WAL_QUALIFICATION_DURATION_MS_V2
CLOSURE_H_START_MS = CLOSURE_WINDOW_END_MS + 60_000
CLOSURE_MAXIMUM_BYTES = 64 * 1024 * 1024
CLOSURE_RESERVE_BYTES = 1_024


def _closure_wal_policy(sync_ms: int, record_cap: int) -> WalSyncPolicyV2:
    return WalSyncPolicyV2(
        qualification_id=CLOSURE_QUALIFICATION,
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


def _closure_candidate_metrics(*, passed: bool) -> WalCandidateMetricsV2:
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


def _closure_selection_receipt() -> WalSelectionReceiptV2:
    selected = (10, 256)
    candidates = tuple(
        WalCandidateQualificationV2(
            policy=_closure_wal_policy(sync_ms, record_cap),
            metrics=_closure_candidate_metrics(passed=(sync_ms, record_cap) == selected),
            measurement_root_sha256=hashlib.sha256(f"{sync_ms}:{record_cap}".encode()).hexdigest(),
        )
        for sync_ms in WAL_SYNC_CANDIDATES_MS_V2
        for record_cap in WAL_RECORD_CAP_CANDIDATES_V2
    )
    qualification = WalQualificationRunV2(
        qualification_id=CLOSURE_QUALIFICATION,
        window_start_wall_ms=CLOSURE_WINDOW_START_MS,
        window_end_wall_ms=CLOSURE_WINDOW_END_MS,
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
        selection_wall_ms=CLOSURE_WINDOW_END_MS,
        h_start_wall_ms=CLOSURE_H_START_MS,
    )


class _ClosureFixture:
    def __init__(self, tmp_path: Path) -> None:
        self.scope = tmp_path / "scope"
        self.scope.mkdir(parents=True)
        self.lease = WriterLease.acquire(self.scope)
        self.plans: tuple[ProvisionalPromotingPlanV2, ...] = (
            build_provisional_promoting_capture_plans_v2(("BTCUSDT",))
        )
        self.authority = WalAuthorityV2(
            attempt_id="attempt-session-closure",
            protocol_sha256=HASH,
            plan_sha256=provisional_promoting_plan_sha256_v2(self.plans),
            source_manifest_sha256="c" * 64,
            schema_sha256="d" * 64,
            runtime_manifest_sha256="e" * 64,
        )
        self.primary_path = self.scope / "wal-primary"
        self.mirror_path = self.scope / "wal-mirror"
        self.block_path = self.scope / "blocks"
        self.ledger_path = self.scope / "ledger"
        selection = _closure_selection_receipt()
        wal_policy = selection.selected_policy
        assert wal_policy is not None
        self.wal_writer = MirroredWalWriterV2(
            self.primary_path,
            self.mirror_path,
            authority=self.authority,
            policy=wal_policy,
            selection_receipt=selection,
            primary_maximum_total_bytes=CLOSURE_MAXIMUM_BYTES,
            mirror_maximum_total_bytes=CLOSURE_MAXIMUM_BYTES,
            primary_emergency_reserve_bytes=CLOSURE_RESERVE_BYTES,
            mirror_emergency_reserve_bytes=CLOSURE_RESERVE_BYTES,
            primary_failure_domain_id="session-closure-primary-device",
            mirror_failure_domain_id="session-closure-mirror-device",
        )
        self.signer = Ed25519BlockSignerV2.from_private_key_bytes(
            key_id="session-closure-writer",
            private_key_bytes=b"\x0b" * 32,
        )
        self.block_signing_authority = BlockSigningAuthorityV2.from_public_key_bytes(
            key_id=self.signer.key_id,
            public_key_bytes=self.signer.public_key_bytes,
        )
        self.block_policy = BlockPolicyV2(
            qualification_id=CLOSURE_QUALIFICATION,
            codec_candidate_id="session-closure-zstd",
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
            stream_group_id="binance-usdm-public-v2",
            segment_id="segment-closure-0001",
            maximum_total_bytes=CLOSURE_MAXIMUM_BYTES,
            emergency_reserve_bytes=CLOSURE_RESERVE_BYTES,
            failure_domain_id="session-closure-block-device",
        )
        self.integrity_ledger_max_events = 10_000
        self.ledger = CaptureIntegrityLedgerV2(
            self.ledger_path,
            authority=self.authority,
            block_directory=self.block_path,
            block_root_binding=self.block_writer.root_binding,
            block_signing_authority=self.block_signing_authority,
            block_policy=self.block_policy,
            block_stream_group_id=self.block_writer.stream_group_id,
            block_segment_id=self.block_writer.segment_id,
            maximum_total_bytes=CLOSURE_MAXIMUM_BYTES,
            emergency_reserve_bytes=CLOSURE_RESERVE_BYTES,
            max_events=self.integrity_ledger_max_events,
            failure_domain_id="session-closure-ledger-device",
            writer_lease=self.lease,
        )
        self.batch_policy = BatchPolicyV2(
            max_records=wal_policy.max_unsynced_records,
            max_encoded_bytes=wal_policy.max_unsynced_bytes,
            max_linger_us=wal_policy.interval_ms * 1_000,
            queue_max_events=512,
            queue_max_encoded_bytes=16_000_000,
            low_water_events=128,
            low_water_encoded_bytes=4_000_000,
            qualification_id=CLOSURE_QUALIFICATION,
        )
        self.durable_writer = DurableCaptureBatchWriterV2(
            batch_policy=self.batch_policy,
            wal_writer=self.wal_writer,
            block_builder=GroupedBlockBuilderV2(self.block_policy),
            block_writer=self.block_writer,
            writer_lease=self.lease,
        )
        self.handoff = BoundedBatchHandoffV2(self.batch_policy)
        self.pipeline = CaptureBatchPipelineV2(self.handoff, self.durable_writer)
        self.started_wall_ms = self.lease.acquired_wall_ms + 1
        self.started_monotonic_ns = self.lease.acquired_monotonic_ns + 1
        self.process_boot_id = "fedcba9876543210fedcba9876543210"
        self.session_id = f"{self.started_wall_ms}-{self.process_boot_id}"
        self.start_path = canonical_session_start_manifest_path_v2(self.lease)
        self.start = write_session_start_manifest_v2(
            self.start_path,
            lease=self.lease,
            session_id=self.session_id,
            process_boot_id=self.process_boot_id,
            started_wall_ms=self.started_wall_ms,
            started_monotonic_ns=self.started_monotonic_ns,
            wal_authority=self.authority,
            wal_durability_binding=self.wal_writer.durability_binding,
            block_policy=self.block_policy,
            block_signing_authority=self.block_signing_authority,
            stream_group_id=self.block_writer.stream_group_id,
            segment_id=self.block_writer.segment_id,
            integrity_ledger_max_events=self.integrity_ledger_max_events,
            storage_root_directories=(
                self.primary_path,
                self.mirror_path,
                self.block_path,
                self.ledger_path,
            ),
            grouped_block_root_binding=self.block_writer.root_binding,
            integrity_ledger_root_binding=self.ledger.root_binding,
        )
        self.closure_path = canonical_session_closure_manifest_path_v2(self.lease)
        self.finality: CaptureFinalityFenceReceiptV2
        self.ledger_seal: PersistedCaptureCleanClosureSealReceiptV2
        self.persisted_closure: PersistedSessionClosureAuthorityV2 | None = None

    async def finalize(self) -> None:
        self.pipeline.start()
        receipt_monotonic_ns = max(
            self.started_monotonic_ns + 1,
            time.monotonic_ns(),
        )
        record = RawRecordV2.from_payload(
            session_id=self.session_id,
            plan_id=self.plans[0].name,
            protocol_hash=HASH,
            transport=TransportV2.WEBSOCKET,
            venue=VenueV2.USDM_FUTURES,
            route_id="usdm_market",
            symbol="BTCUSDT",
            connection_id="session-closure-connection",
            generation=1,
            frame_seq=1,
            ingest_seq=1,
            receipt_wall_ms=self.started_wall_ms + 1,
            receipt_monotonic_ns=receipt_monotonic_ns,
            raw_payload='{"e":"aggTrade","s":"BTCUSDT"}',
            source_logical_key="session-closure-record-1",
        )
        self.pipeline.offer(record)
        self.finality = await self.pipeline.finalize_current_tail_and_stop(
            timeout_seconds=5,
        )
        self.ledger_seal = self.ledger.seal_clean_closure_v2(
            promoting_plans=self.plans,
            finality_receipt=self.finality,
            wal_writer=self.wal_writer,
            block_writer=self.block_writer,
            session_id=self.session_id,
            process_boot_id=self.process_boot_id,
            seal_wall_ms=self.finality.target_last_receipt_wall_ms + 1,
            seal_monotonic_ns=self.finality.writer_observed_monotonic_ns + 1,
        )

    def write(
        self,
        path: Path | None = None,
        *,
        stop_reason: str = "COMPLETED_DURATION",
        closed_wall_ms: int | None = None,
        closed_monotonic_ns: int | None = None,
        lease: WriterLease | None = None,
        start: PersistedSessionStartAuthorityV2 | None = None,
        plans: tuple[ProvisionalPromotingPlanV2, ...] | None = None,
    ) -> PersistedSessionClosureAuthorityV2:
        assert self.finality is not None
        assert self.ledger_seal is not None
        receipt = write_session_closure_manifest_v2(
            path or self.closure_path,
            lease=lease or self.lease,
            session_start_authority=start or self.start,
            promoting_plans=plans or self.plans,
            finality_receipt=self.finality,
            pipeline=self.pipeline,
            ledger_seal_receipt=self.ledger_seal,
            ledger=self.ledger,
            stop_reason=stop_reason,
            closed_wall_ms=(
                self.ledger_seal.seal.seal_wall_ms + 1 if closed_wall_ms is None else closed_wall_ms
            ),
            closed_monotonic_ns=(
                self.ledger_seal.seal.seal_monotonic_ns + 1
                if closed_monotonic_ns is None
                else closed_monotonic_ns
            ),
        )
        self.persisted_closure = receipt
        return receipt

    def assert_current(self, receipt: PersistedSessionClosureAuthorityV2) -> None:
        assert self.finality is not None
        assert self.ledger_seal is not None
        assert_persisted_session_closure_authority_current_v2(
            receipt,
            lease=self.lease,
            session_start_authority=self.start,
            promoting_plans=self.plans,
            finality_receipt=self.finality,
            pipeline=self.pipeline,
            ledger_seal_receipt=self.ledger_seal,
            ledger=self.ledger,
        )

    async def close(self) -> None:
        await self.pipeline.stop()
        self.lease.release()


def _load_v8_runtime_fixture_module() -> ModuleType:
    module_name = "_signalbot_test_session_v8_runtime_fixture"
    path = Path(__file__).with_name("test_full_runtime_v8.py")
    spec = importlib.util.spec_from_file_location(module_name, path)
    if spec is None or spec.loader is None:
        raise RuntimeError("could not load the adjacent V8 runtime fixture")
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


_v8_runtime_fixture_module = _load_v8_runtime_fixture_module()
_V8RuntimeFixture = cast(Any, _v8_runtime_fixture_module._V8Fixture)


class _SessionClosureV8Fixture:
    def __init__(self, tmp_path: Path) -> None:
        self.runtime_fixture = _V8RuntimeFixture(tmp_path)
        self.plans: tuple[ProvisionalPromotingPlanV8, ...] = (
            self.runtime_fixture.v8_plans
        )
        self.depth_plan: ProvisionalDepthRestQualificationPlanV8 = (
            self.runtime_fixture.depth_plan
        )
        self.result: PublicCaptureRuntimeResultV8
        self.ledger_seal: PersistedCaptureCleanClosureSealReceiptV8
        self.closure_path = canonical_session_closure_manifest_path_v8(
            self.runtime_fixture.lease
        )
        self.persisted: PersistedSessionClosureAuthorityV8 | None = None

    async def finalize(self) -> None:
        _, self.result = await _v8_runtime_fixture_module._run_normal_v8(
            self.runtime_fixture
        )
        bridge = self.result.depth_bridge_close_receipt
        finality = self.result.finality_receipt
        self.ledger_seal = self.runtime_fixture.ledger.seal_clean_closure_v8(
            promoting_plans=self.plans,
            depth_plan=self.depth_plan,
            depth_bridge_close_receipt=bridge,
            finalized_websocket_cursor_pair=self.result.websocket_route_cursors,
            finality_receipt=finality,
            wal_writer=self.runtime_fixture.wal_writer,
            block_writer=self.runtime_fixture.block_writer,
            session_id=self.runtime_fixture.session_id,
            process_boot_id=self.runtime_fixture.process_boot_id,
            seal_wall_ms=max(
                bridge.close_wall_ms,
                finality.target_last_receipt_wall_ms,
            )
            + 1,
            seal_monotonic_ns=max(
                bridge.close_monotonic_ns,
                finality.writer_observed_monotonic_ns,
            )
            + 1,
        )

    def write(
        self,
        path: Path | None = None,
        *,
        plans: tuple[ProvisionalPromotingPlanV8, ...] | None = None,
        depth_plan: ProvisionalDepthRestQualificationPlanV8 | None = None,
        ledger_seal: PersistedCaptureCleanClosureSealReceiptV8 | None = None,
        bridge_receipt: DepthBridgeCoordinatorCleanCloseReceiptV8 | None = None,
        bridge_entry: DepthBridgeCoordinatorClosureEntryV8 | None = None,
        websocket_cursors: FinalizedWebSocketRouteCursorPairV8 | None = None,
        oi_receipt: PublicOiCensusAdmissionReceiptV2 | None = None,
        stop_reason: str = "COMPLETED_DURATION",
        closed_wall_ms: int | None = None,
        closed_monotonic_ns: int | None = None,
    ) -> PersistedSessionClosureAuthorityV8:
        seal = ledger_seal or self.ledger_seal
        result = self.result
        receipt = write_session_closure_manifest_v8(
            path or self.closure_path,
            lease=self.runtime_fixture.lease,
            session_start_authority=self.runtime_fixture.start_authority,
            promoting_plans=plans or self.plans,
            depth_plan=depth_plan or self.depth_plan,
            finality_receipt=result.finality_receipt,
            pipeline=self.runtime_fixture.pipeline,
            ledger_seal_receipt=seal,
            ledger=self.runtime_fixture.ledger,
            depth_bridge_close_receipt=(
                bridge_receipt or result.depth_bridge_close_receipt
            ),
            depth_bridge_closure_entry=(
                bridge_entry or seal.seal.depth_bridge_closure_entry
            ),
            finalized_websocket_route_cursors=(
                websocket_cursors or result.websocket_route_cursors
            ),
            oi_coverage_close_receipt=(
                oi_receipt or result.oi_coverage_close_receipt
            ),
            stop_reason=stop_reason,
            closed_wall_ms=(
                seal.seal.seal_wall_ms + 1
                if closed_wall_ms is None
                else closed_wall_ms
            ),
            closed_monotonic_ns=(
                seal.seal.seal_monotonic_ns + 1
                if closed_monotonic_ns is None
                else closed_monotonic_ns
            ),
        )
        self.persisted = receipt
        return receipt

    def assert_current(
        self,
        authority: PersistedSessionClosureAuthorityV8,
        *,
        ledger: CaptureIntegrityLedgerV2 | None = None,
    ) -> None:
        result = self.result
        assert_persisted_session_closure_authority_current_v8(
            authority,
            lease=self.runtime_fixture.lease,
            session_start_authority=self.runtime_fixture.start_authority,
            promoting_plans=self.plans,
            depth_plan=self.depth_plan,
            finality_receipt=result.finality_receipt,
            pipeline=self.runtime_fixture.pipeline,
            ledger_seal_receipt=self.ledger_seal,
            ledger=ledger or self.runtime_fixture.ledger,
            depth_bridge_closure_entry=(
                self.ledger_seal.seal.depth_bridge_closure_entry
            ),
            finalized_websocket_route_cursors=result.websocket_route_cursors,
            oi_coverage_close_receipt=result.oi_coverage_close_receipt,
        )

    def reopen_ledger(self) -> CaptureIntegrityLedgerV2:
        fixture = self.runtime_fixture
        start = fixture.start_authority.manifest
        return CaptureIntegrityLedgerV2(
            fixture.ledger_path,
            authority=fixture.authority,
            block_directory=fixture.block_path,
            block_root_binding=fixture.block_writer.root_binding,
            block_signing_authority=start.block_signing_authority,
            block_policy=start.block_policy,
            block_stream_group_id=start.stream_group_id,
            block_segment_id=start.segment_id,
            maximum_total_bytes=fixture.ledger.maximum_total_bytes,
            emergency_reserve_bytes=fixture.ledger.emergency_reserve_bytes,
            max_events=start.integrity_ledger_max_events,
            failure_domain_id=fixture.ledger.root_binding.failure_domain_id,
            writer_lease=fixture.lease,
        )

    async def close(self) -> None:
        await self.runtime_fixture.close()


def test_start_manifest_is_exact_canonical_write_once_authority(
    tmp_path: Path,
) -> None:
    fixture = _Fixture(tmp_path)
    try:
        output = fixture.session_start_path
        manifest = fixture.write(output)
        encoded = canonical_json_line(manifest)

        assert manifest.encoded_line == encoded
        assert manifest.encoded_line == encoded
        assert manifest.sha256 == hashlib.sha256(encoded).hexdigest()
        assert manifest.sha256 == hashlib.sha256(manifest.encoded_line).hexdigest()
        assert output.read_bytes() == encoded
        assert manifest.attempt_id == fixture.authority.attempt_id
        assert manifest.wal_authority == fixture.authority
        assert manifest.wal_authority_sha256 == fixture.authority.sha256
        assert manifest.wal_durability_binding == fixture.durability
        assert manifest.wal_durability_binding_sha256 == fixture.durability.sha256
        assert manifest.qualification_selection_receipt_sha256 == SELECTION_SHA256
        assert manifest.block_policy == fixture.block_policy
        assert manifest.block_signing_authority == fixture.block_signing_authority
        assert manifest.block_signing_authority_sha256 == fixture.block_signing_authority.sha256
        assert manifest.integrity_ledger_max_events == fixture.integrity_ledger_max_events
        assert tuple(root.root_binding for root in manifest.storage_roots) == (
            fixture.primary,
            fixture.mirror,
            fixture.block,
            fixture.ledger,
        )
        assert manifest.writer_lease.scope_canonical_path == os.path.normcase(
            os.path.abspath(fixture.scope)
        )
        assert manifest.writer_lease.owner_pid == fixture.lease.owner_pid
        assert manifest.writer_lease.owner_id == fixture.lease.owner_id
        assert manifest.writer_lease.backend == fixture.lease.backend
        assert manifest.writer_lease.acquired_wall_ms == fixture.lease.acquired_wall_ms
        assert manifest.production_order_execution_enabled is False
        assert manifest.private_credentials_permitted is False
        assert manifest.previous_closure_sha256 is None
        assert SESSION_CLOSURE_SUPPORTED_V2 is False

        original = output.read_bytes()
        with pytest.raises(SessionAuthorityExistsError, match="already exists"):
            fixture.write(output)
        assert output.read_bytes() == original
    finally:
        fixture.close()


def test_persisted_authority_is_factory_only_and_revalidates_original_file(
    tmp_path: Path,
) -> None:
    fixture = _Fixture(tmp_path)
    try:
        fixture.write()
        authority = fixture.last_persisted
        assert authority is not None
        assert_persisted_session_start_authority_current_v2(
            authority,
            lease=fixture.lease,
        )

        with pytest.raises(TypeError, match="only be created by the durable writer"):
            PersistedSessionStartAuthorityV2(
                manifest=authority.manifest,
                canonical_path=authority.canonical_path,
                manifest_sha256=authority.manifest_sha256,
                byte_count=authority.byte_count,
                file_device=authority.file_device,
                file_inode=authority.file_inode,
                file_nlink=authority.file_nlink,
                writer_lease=authority.writer_lease,
                _factory_token=object(),
            )
    finally:
        fixture.close()


def test_noncanonical_start_manifest_path_is_rejected_before_write(
    tmp_path: Path,
) -> None:
    fixture = _Fixture(tmp_path)
    output = fixture.primary_path / "session-start.json"
    try:
        with pytest.raises(
            SessionAuthorityIntegrityError,
            match="lease-acquisition canonical path",
        ):
            fixture.write(output)
        assert not output.exists()
        with pytest.raises(SessionAuthorityExistsError, match="already consumed"):
            fixture.write()
        assert not fixture.session_start_path.exists()
    finally:
        fixture.close()


def test_one_lease_acquisition_cannot_fork_session_start_authority(
    tmp_path: Path,
) -> None:
    fixture = _Fixture(tmp_path)
    alternate_path = fixture.scope / "alternate-session-start.jsonl"
    try:
        original = fixture.write()
        original_receipt = fixture.last_persisted
        assert original_receipt is not None

        with pytest.raises(SessionAuthorityExistsError, match="already consumed"):
            fixture.write(
                alternate_path,
                previous_closure_sha256="8" * 64,
            )
        with pytest.raises(SessionAuthorityExistsError, match="already exists"):
            fixture.write(previous_closure_sha256="9" * 64)

        assert not alternate_path.exists()
        assert fixture.session_start_path.read_bytes() == original.encoded_line
        assert_persisted_session_start_authority_current_v2(
            original_receipt,
            lease=fixture.lease,
        )
    finally:
        fixture.close()


def test_unlinked_start_cannot_be_reissued_under_the_same_lease(
    tmp_path: Path,
) -> None:
    fixture = _Fixture(tmp_path)
    try:
        original = fixture.write()
        original_receipt = fixture.last_persisted
        assert original_receipt is not None
        original_identity = (
            original_receipt.file_device,
            original_receipt.file_inode,
        )

        fixture.session_start_path.unlink()
        with pytest.raises(SessionAuthorityExistsError, match="already consumed"):
            fixture.write(previous_closure_sha256="9" * 64)

        assert not fixture.session_start_path.exists()
        assert original.previous_closure_sha256 is None
        assert (
            original_receipt.file_device,
            original_receipt.file_inode,
        ) == original_identity
        with pytest.raises(
            SessionAuthorityIntegrityError,
            match=r"does not exist|existing regular file",
        ):
            assert_persisted_session_start_authority_current_v2(
                original_receipt,
                lease=fixture.lease,
            )
    finally:
        fixture.close()


def test_start_manifest_rejects_single_root_durability(tmp_path: Path) -> None:
    fixture = _Fixture(tmp_path)
    try:
        single = WalDurabilityBindingV2(
            mode="SINGLE_ROOT",
            root_bindings=(fixture.primary,),
            qualification_selection_receipt_sha256=SELECTION_SHA256,
            physical_failure_domain_independence_verified=False,
        )

        with pytest.raises(ValueError, match="QUALIFIED_DUAL_OWNER"):
            fixture.write(durability=single)
        assert not fixture.session_start_path.exists()
    finally:
        fixture.close()


def test_start_manifest_rejects_released_lease_and_out_of_scope_output(
    tmp_path: Path,
) -> None:
    fixture = _Fixture(tmp_path)
    outside = tmp_path / "outside.json"
    try:
        with pytest.raises(
            SessionAuthorityIntegrityError,
            match="lease-acquisition canonical path",
        ):
            fixture.write(outside)
        assert not outside.exists()
        fixture.lease.release()
        with pytest.raises(WriterLeaseNotHeldError, match="released"):
            fixture.write()
    finally:
        try:
            fixture.lease.assert_held()
        except WriterLeaseNotHeldError:
            pass
        else:
            fixture.close()


def test_start_manifest_rejects_same_roots_under_a_different_lease_scope(
    tmp_path: Path,
) -> None:
    fixture = _Fixture(tmp_path)
    other_scope = tmp_path / "other-scope"
    other_scope.mkdir()
    other_lease = WriterLease.acquire(other_scope)
    try:
        output = canonical_session_start_manifest_path_v2(other_lease)
        with pytest.raises(
            SessionAuthorityIntegrityError,
            match=r"storage root.*strict descendant.*lease scope",
        ):
            fixture.write(output, lease=other_lease)
        assert not output.exists()
    finally:
        other_lease.release()
        fixture.close()


def test_start_manifest_rejects_storage_root_outside_lease_scope(
    tmp_path: Path,
) -> None:
    fixture = _Fixture(tmp_path)
    outside_primary = tmp_path / "outside-primary"
    outside_primary.mkdir()
    bind_storage_root_v2(
        outside_primary,
        storage_kind="WAL",
        root_role="PRIMARY",
        failure_domain_id="device-primary",
        authority_sha256=fixture.authority.sha256,
        contract=fixture.wal_contract,
    )
    try:
        with pytest.raises(
            SessionAuthorityIntegrityError,
            match=r"storage root.*strict descendant.*lease scope",
        ):
            fixture.write(
                directories=(
                    outside_primary,
                    fixture.mirror_path,
                    fixture.block_path,
                    fixture.ledger_path,
                )
            )
        assert not fixture.session_start_path.exists()
    finally:
        fixture.close()


def test_start_manifest_rejects_storage_root_equal_to_lease_scope(
    tmp_path: Path,
) -> None:
    fixture = _Fixture(tmp_path)
    (fixture.scope / "storage-root-binding.json").write_bytes(
        (fixture.primary_path / "storage-root-binding.json").read_bytes()
    )
    try:
        with pytest.raises(
            SessionAuthorityIntegrityError,
            match=r"storage root.*strict descendant.*lease scope",
        ):
            fixture.write(
                directories=(
                    fixture.scope,
                    fixture.mirror_path,
                    fixture.block_path,
                    fixture.ledger_path,
                )
            )
        assert not fixture.session_start_path.exists()
    finally:
        fixture.close()


@pytest.mark.parametrize("root_attribute", ["mirror_path", "ledger_path"])
def test_start_manifest_rejects_current_root_binding_tamper(
    tmp_path: Path,
    root_attribute: str,
) -> None:
    fixture = _Fixture(tmp_path)
    try:
        root_path = getattr(fixture, root_attribute)
        assert isinstance(root_path, Path)
        binding_path = root_path / "storage-root-binding.json"
        binding_path.write_bytes(binding_path.read_bytes() + b" ")

        with pytest.raises(
            SessionAuthorityIntegrityError,
            match="differs from its expected current bytes",
        ):
            fixture.write()
        assert not fixture.session_start_path.exists()
    finally:
        fixture.close()


def test_start_manifest_rejects_symlinked_storage_root(tmp_path: Path) -> None:
    fixture = _Fixture(tmp_path)
    linked_root = fixture.scope / "linked-primary"
    try:
        try:
            linked_root.symlink_to(fixture.primary_path, target_is_directory=True)
        except OSError as exc:
            pytest.skip(f"directory symlinks are unavailable: {exc}")

        with pytest.raises(
            SessionAuthorityIntegrityError,
            match=r"symbolic-link|reparse-point",
        ):
            fixture.write(
                directories=(
                    linked_root,
                    fixture.mirror_path,
                    fixture.block_path,
                    fixture.ledger_path,
                )
            )
        assert not fixture.session_start_path.exists()
    finally:
        fixture.close()


def test_start_manifest_rejects_wrong_storage_root_order(tmp_path: Path) -> None:
    fixture = _Fixture(tmp_path)
    try:
        with pytest.raises(
            SessionAuthorityIntegrityError,
            match="differs from its expected current bytes",
        ):
            fixture.write(
                directories=(
                    fixture.mirror_path,
                    fixture.primary_path,
                    fixture.block_path,
                    fixture.ledger_path,
                )
            )
        assert not fixture.session_start_path.exists()
    finally:
        fixture.close()


def test_start_manifest_rejects_grouped_block_signer_contract_drift(
    tmp_path: Path,
) -> None:
    fixture = _Fixture(tmp_path)
    alternate_signer = Ed25519BlockSignerV2.from_private_key_bytes(
        key_id="alternate-session-writer",
        private_key_bytes=bytes(reversed(range(32))),
    )
    alternate_authority = BlockSigningAuthorityV2.from_public_key_bytes(
        key_id=alternate_signer.key_id,
        public_key_bytes=alternate_signer.public_key_bytes,
    )
    try:
        with pytest.raises(ValueError, match="grouped-block root contract differs"):
            fixture.write(block_signing_authority=alternate_authority)
        assert not fixture.session_start_path.exists()
    finally:
        fixture.close()


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("stream_group_id", "other-stream-group"),
        ("segment_id", "other-segment"),
    ],
)
def test_start_manifest_rejects_grouped_block_scope_contract_drift(
    tmp_path: Path,
    field: str,
    value: str,
) -> None:
    fixture = _Fixture(tmp_path)
    try:
        kwargs = {field: value}
        with pytest.raises(ValueError, match="grouped-block root contract differs"):
            fixture.write(**kwargs)  # type: ignore[arg-type]
        assert not fixture.session_start_path.exists()
    finally:
        fixture.close()


def test_start_manifest_rejects_integrity_ledger_contract_drift(
    tmp_path: Path,
) -> None:
    fixture = _Fixture(tmp_path)
    try:
        with pytest.raises(ValueError, match="integrity-ledger root contract differs"):
            fixture.write(integrity_ledger_max_events=fixture.integrity_ledger_max_events + 1)
        assert not fixture.session_start_path.exists()
    finally:
        fixture.close()


def test_start_manifest_model_rejects_order_private_credentials_and_orders(
    tmp_path: Path,
) -> None:
    fixture = _Fixture(tmp_path)
    try:
        manifest = fixture.write()

        with pytest.raises(ValueError, match="production order execution is forbidden"):
            replace(manifest, production_order_execution_enabled=True)
        with pytest.raises(ValueError, match="private credentials are forbidden"):
            replace(manifest, private_credentials_permitted=True)
        with pytest.raises(ValueError, match="lowercase UUID hex"):
            replace(manifest, process_boot_id="A" * 32)
        with pytest.raises(ValueError, match="must bind the UTC wall start"):
            replace(manifest, session_id="detached-session-id")
        with pytest.raises(ValueError, match="exact durability order"):
            replace(
                manifest,
                storage_roots=(
                    manifest.storage_roots[1],
                    manifest.storage_roots[0],
                    manifest.storage_roots[2],
                    manifest.storage_roots[3],
                ),
            )
        with pytest.raises(ValueError, match="path hash differs"):
            replace(
                manifest.storage_roots[0],
                path_sha256="0" * 64,
            )
        nested_path = os.path.normcase(
            os.path.abspath(fixture.primary_path / "nested-grouped-block")
        )
        nested_reference = replace(
            manifest.storage_roots[2],
            canonical_path=nested_path,
            path_sha256=hashlib.sha256(
                b"R4B2-SESSION-CANONICAL-PATH-V1\0"
                + canonical_json_line({"canonical_path": nested_path})
            ).hexdigest(),
        )
        with pytest.raises(ValueError, match="pairwise non-nested"):
            replace(
                manifest,
                storage_roots=(
                    manifest.storage_roots[0],
                    manifest.storage_roots[1],
                    nested_reference,
                    manifest.storage_roots[3],
                ),
            )
    finally:
        fixture.close()


@pytest.mark.asyncio
async def test_clean_closure_binds_exact_stopped_tail_ledger_start_and_plan(
    tmp_path: Path,
) -> None:
    fixture = _ClosureFixture(tmp_path)
    try:
        await fixture.finalize()
        receipt = fixture.write()
        manifest = receipt.manifest

        assert Path(receipt.canonical_path) == fixture.closure_path.absolute()
        assert fixture.closure_path.read_bytes() == manifest.encoded_line
        assert receipt.manifest_sha256 == manifest.sha256
        assert manifest.closure_status == "CLEAN"
        assert manifest.fatal is False
        assert manifest.stop_reason == "COMPLETED_DURATION"
        assert manifest.production_order_execution_enabled is False
        assert manifest.private_credentials_permitted is False
        assert manifest.session_start_manifest == fixture.start.manifest
        assert manifest.session_start_manifest_sha256 == fixture.start.manifest_sha256
        assert manifest.session_start_file_inode == str(fixture.start.file_inode)
        assert manifest.finality_receipt == fixture.finality
        assert manifest.finality_tail_ingest_seq == 1
        assert manifest.finality_prefix_proof_sha256 == fixture.finality.prefix_proof_sha256
        assert manifest.wal_durable_ack_seq == 1
        assert manifest.finalized_block_tail_ingest_seq == 1
        assert manifest.ledger_clean_closure_seal == fixture.ledger_seal.seal
        assert manifest.ledger_clean_closure_seal.event_count == 0
        assert manifest.ledger_clean_closure_seal.event_tip_sha256 is None
        assert manifest.ledger_clean_closure_seal.unmatched_source_gap_open_count == 0
        assert manifest.ledger_clean_closure_seal.void_count == 0
        assert manifest.ledger_clean_closure_receipt_sha256 == fixture.ledger_seal.sha256
        assert tuple(entry.route_id for entry in manifest.planned_source_census.entries) == (
            "usdm_market",
            "usdm_public",
            "usdm_public_rest",
        )
        assert manifest.planned_source_census.observed_source_completeness_claimed is False
        assert manifest.planned_source_census.m2_certified is False
        assert manifest.websocket_route_cursors == ()
        assert manifest.websocket_route_cursors_sha256 is None
        assert manifest.websocket_route_cursor_finality_persisted is False
        assert SESSION_CLOSURE_SUPPORTED_V2 is False

        document = manifest.encoded_line.decode("utf-8").casefold()
        for forbidden in (
            '"api_key"',
            '"secret"',
            '"strategy_efficacy"',
            '"pnl"',
            '"order_id"',
        ):
            assert forbidden not in document
        fixture.assert_current(receipt)
    finally:
        await fixture.close()


@pytest.mark.asyncio
async def test_persisted_clean_closure_authority_is_factory_only(
    tmp_path: Path,
) -> None:
    fixture = _ClosureFixture(tmp_path)
    try:
        await fixture.finalize()
        receipt = fixture.write(stop_reason="OPERATOR_REQUESTED")
        with pytest.raises(TypeError, match="only be created by the durable writer"):
            PersistedSessionClosureAuthorityV2(
                manifest=receipt.manifest,
                canonical_path=receipt.canonical_path,
                manifest_sha256=receipt.manifest_sha256,
                byte_count=receipt.byte_count,
                file_device=receipt.file_device,
                file_inode=receipt.file_inode,
                file_nlink=receipt.file_nlink,
                writer_lease=receipt.writer_lease,
                session_start_authority=fixture.start,
                ledger_seal_receipt=fixture.ledger_seal,
                _factory_token=object(),
            )
    finally:
        await fixture.close()


@pytest.mark.asyncio
async def test_clean_closure_claim_is_consumed_by_wrong_path_and_failed_clock(
    tmp_path: Path,
) -> None:
    wrong_path_fixture = _ClosureFixture(tmp_path / "wrong-path")
    clock_fixture = _ClosureFixture(tmp_path / "clock")
    try:
        await wrong_path_fixture.finalize()
        wrong_path = wrong_path_fixture.scope / "alternate-closure.jsonl"
        with pytest.raises(
            SessionAuthorityIntegrityError,
            match="lease-acquisition canonical path",
        ):
            wrong_path_fixture.write(wrong_path)
        assert not wrong_path.exists()
        with pytest.raises(SessionAuthorityExistsError, match="already consumed"):
            wrong_path_fixture.write()

        await clock_fixture.finalize()
        assert clock_fixture.ledger_seal is not None
        with pytest.raises(ValueError, match="wall clock precedes"):
            clock_fixture.write(
                closed_wall_ms=clock_fixture.ledger_seal.seal.seal_wall_ms - 1,
            )
        assert not clock_fixture.closure_path.exists()
        with pytest.raises(SessionAuthorityExistsError, match="already consumed"):
            clock_fixture.write()
    finally:
        await wrong_path_fixture.close()
        await clock_fixture.close()


@pytest.mark.asyncio
async def test_unlinked_clean_closure_cannot_be_reissued_under_same_lease(
    tmp_path: Path,
) -> None:
    fixture = _ClosureFixture(tmp_path)
    try:
        await fixture.finalize()
        receipt = fixture.write()
        fixture.closure_path.unlink()

        with pytest.raises(SessionAuthorityExistsError, match="already consumed"):
            fixture.write(stop_reason="OPERATOR_REQUESTED")
        with pytest.raises(
            SessionAuthorityIntegrityError,
            match=r"does not exist|existing regular file",
        ):
            fixture.assert_current(receipt)
    finally:
        await fixture.close()


@pytest.mark.asyncio
async def test_clean_closure_rejects_fatal_status_and_plan_drift(
    tmp_path: Path,
) -> None:
    status_fixture = _ClosureFixture(tmp_path / "status")
    plan_fixture = _ClosureFixture(tmp_path / "plan")
    try:
        await status_fixture.finalize()
        with pytest.raises(ValueError, match="normal stop reason"):
            status_fixture.write(stop_reason="FATAL_ERROR")

        await plan_fixture.finalize()
        different_plans = build_provisional_promoting_capture_plans_v2(("ETHUSDT",))
        with pytest.raises(
            SessionAuthorityIntegrityError,
            match="plan bundle differs",
        ):
            plan_fixture.write(plans=different_plans)
    finally:
        await status_fixture.close()
        await plan_fixture.close()


@pytest.mark.asyncio
async def test_clean_closure_model_rejects_stale_heads_and_nonclaims(
    tmp_path: Path,
) -> None:
    fixture = _ClosureFixture(tmp_path)
    try:
        await fixture.finalize()
        receipt = fixture.write()
        manifest: SessionClosureManifestV2 = receipt.manifest

        with pytest.raises(ValueError, match="WAL/block heads differ"):
            replace(manifest, wal_durable_ack_seq=2)
        with pytest.raises(ValueError, match="finality receipt hash differs"):
            replace(manifest, finality_receipt_sha256="0" * 64)
        with pytest.raises(ValueError, match="ledger seal hash differs"):
            replace(manifest, ledger_clean_closure_seal_sha256="0" * 64)
        with pytest.raises(ValueError, match="forbids fatal=true"):
            replace(manifest, fatal=True)
        with pytest.raises(ValueError, match="cannot claim observed completeness or M2"):
            replace(
                manifest,
                planned_source_census=replace(
                    manifest.planned_source_census,
                    m2_certified=True,
                ),
            )
        with pytest.raises(ValueError, match="monotonic clock precedes"):
            replace(
                manifest,
                closed_monotonic_ns=(manifest.ledger_clean_closure_seal.seal_monotonic_ns - 1),
            )
        boundary = replace(
            manifest,
            closed_wall_ms=manifest.ledger_clean_closure_seal.seal_wall_ms,
            closed_monotonic_ns=(manifest.ledger_clean_closure_seal.seal_monotonic_ns),
        )
        assert boundary.closed_wall_ms == manifest.ledger_clean_closure_seal.seal_wall_ms

        large_identity = str(2**63 + 123)
        large_identity_manifest = replace(
            manifest,
            session_start_file_inode=large_identity,
            ledger_clean_closure_file_inode=large_identity,
        )
        assert large_identity_manifest.encoded_line == large_identity_manifest.encoded_line
        assert large_identity.encode("ascii") in large_identity_manifest.encoded_line
    finally:
        await fixture.close()


@pytest.mark.asyncio
async def test_current_clean_closure_rejects_wrong_lease_start_finality_and_ledger(
    tmp_path: Path,
) -> None:
    fixture = _ClosureFixture(tmp_path / "main")
    other = _ClosureFixture(tmp_path / "other")
    other_scope = tmp_path / "other-lease"
    other_scope.mkdir()
    other_lease = WriterLease.acquire(other_scope)
    try:
        await fixture.finalize()
        receipt = fixture.write()
        await other.finalize()

        with pytest.raises(SessionAuthorityIntegrityError, match="lease scope differs"):
            assert_persisted_session_closure_authority_current_v2(
                receipt,
                lease=other_lease,
                session_start_authority=fixture.start,
                promoting_plans=fixture.plans,
                finality_receipt=fixture.finality,
                pipeline=fixture.pipeline,
                ledger_seal_receipt=fixture.ledger_seal,
                ledger=fixture.ledger,
            )
        with pytest.raises(SessionAuthorityIntegrityError):
            assert_persisted_session_closure_authority_current_v2(
                receipt,
                lease=fixture.lease,
                session_start_authority=other.start,
                promoting_plans=fixture.plans,
                finality_receipt=fixture.finality,
                pipeline=fixture.pipeline,
                ledger_seal_receipt=fixture.ledger_seal,
                ledger=fixture.ledger,
            )
        with pytest.raises(SessionAuthorityIntegrityError, match="finality receipt differs"):
            assert_persisted_session_closure_authority_current_v2(
                receipt,
                lease=fixture.lease,
                session_start_authority=fixture.start,
                promoting_plans=fixture.plans,
                finality_receipt=other.finality,
                pipeline=fixture.pipeline,
                ledger_seal_receipt=fixture.ledger_seal,
                ledger=fixture.ledger,
            )
        with pytest.raises(SessionAuthorityIntegrityError, match="ledger CLEAN seal differs"):
            assert_persisted_session_closure_authority_current_v2(
                receipt,
                lease=fixture.lease,
                session_start_authority=fixture.start,
                promoting_plans=fixture.plans,
                finality_receipt=fixture.finality,
                pipeline=fixture.pipeline,
                ledger_seal_receipt=other.ledger_seal,
                ledger=fixture.ledger,
            )
    finally:
        other_lease.release()
        await fixture.close()
        await other.close()


@pytest.mark.asyncio
async def test_current_clean_closure_rejects_file_and_root_identity_replacement(
    tmp_path: Path,
) -> None:
    file_fixture = _ClosureFixture(tmp_path / "file")
    root_fixture = _ClosureFixture(tmp_path / "root")
    try:
        await file_fixture.finalize()
        file_receipt = file_fixture.write()
        replacement = file_fixture.scope / "replacement-closure.jsonl"
        replacement.write_bytes(file_fixture.closure_path.read_bytes())
        os.replace(replacement, file_fixture.closure_path)
        with pytest.raises(
            SessionAuthorityIntegrityError,
            match="file identity differs",
        ):
            file_fixture.assert_current(file_receipt)

        await root_fixture.finalize()
        root_receipt = root_fixture.write()
        binding_path = root_fixture.ledger_path / "storage-root-binding.json"
        replacement_binding = root_fixture.ledger_path / "replacement-binding.json"
        replacement_binding.write_bytes(binding_path.read_bytes())
        os.replace(replacement_binding, binding_path)
        with pytest.raises(
            SessionAuthorityIntegrityError,
            match=r"storage-root reference changed|identity",
        ):
            root_fixture.assert_current(root_receipt)
    finally:
        await file_fixture.close()
        await root_fixture.close()


@pytest.mark.asyncio
async def test_current_clean_closure_rejects_an_added_hard_link(
    tmp_path: Path,
) -> None:
    fixture = _ClosureFixture(tmp_path)
    try:
        await fixture.finalize()
        receipt = fixture.write()
        alias = fixture.scope / "closure-hard-link.jsonl"
        try:
            os.link(fixture.closure_path, alias)
        except OSError as exc:
            pytest.skip(f"hard links are unavailable: {exc}")
        with pytest.raises(
            SessionAuthorityIntegrityError,
            match="exactly one hard link",
        ):
            fixture.assert_current(receipt)
    finally:
        await fixture.close()


@pytest.mark.asyncio
async def test_v8_session_closure_persists_exact_four_source_infrastructure_clean(
    tmp_path: Path,
) -> None:
    fixture = _SessionClosureV8Fixture(tmp_path)
    try:
        await fixture.finalize()
        assert fixture.ledger_seal.seal.closure_status == "CLEAN"
        assert not fixture.closure_path.exists()
        assert canonical_session_closure_manifest_path_v2(
            fixture.runtime_fixture.lease
        ) == canonical_session_closure_manifest_path_v8(fixture.runtime_fixture.lease)

        receipt = fixture.write()
        manifest = receipt.manifest
        assert type(manifest) is SessionClosureManifestV8
        assert receipt.canonical_path == os.path.normcase(str(fixture.closure_path.resolve()))
        assert receipt.manifest_sha256 == manifest.sha256
        assert fixture.closure_path.read_bytes() == manifest.encoded_line
        assert manifest.encoded_line == canonical_json_line(manifest)
        assert manifest.plan_bundle_sha256 == provisional_promoting_plan_sha256_v8(
            fixture.plans
        )
        assert type(manifest.planned_source_census) is PlannedSourceCensusV8
        assert all(
            type(entry) is PlannedSourceCensusEntryV8
            for entry in manifest.planned_source_census.entries
        )
        assert tuple(
            (entry.route_id, entry.authority_role)
            for entry in manifest.planned_source_census.entries
        ) == (
            ("usdm_market", "PROMOTING"),
            ("usdm_public", "PROMOTING"),
            ("usdm_public_rest", "PROMOTING"),
            ("usdm_public_depth_rest", "QUALIFICATION_ONLY"),
        )
        public_cursor = manifest.websocket_route_cursors[1]
        bridge = manifest.depth_bridge_closure_entry
        assert bridge.last_connection_id == public_cursor.connection_id
        assert bridge.last_connection_generation == public_cursor.generation
        assert manifest.oi_coverage_close_accepted_ingest_seq <= (
            manifest.finality_tail_ingest_seq
        )
        assert manifest.depth_bridge_lifecycle_cleanly_closed is True
        for field_name in (
            "fatal",
            "production_order_execution_enabled",
            "private_credentials_permitted",
            "retained_frame_parser_health_claimed",
            "observed_source_completeness_claimed",
            "book_completeness_claimed",
            "m2_certified",
            "paper_execution_enabled",
            "promotion_ready",
        ):
            assert getattr(manifest, field_name) is False

        fixture.assert_current(receipt)
        reopened_ledger = fixture.reopen_ledger()
        fixture.assert_current(receipt, ledger=reopened_ledger)
    finally:
        await fixture.close()


@pytest.mark.asyncio
async def test_v8_session_closure_model_rejects_claim_inflation_and_v2_cursor_cast(
    tmp_path: Path,
) -> None:
    fixture = _SessionClosureV8Fixture(tmp_path)
    try:
        await fixture.finalize()
        receipt = fixture.write()
        manifest = receipt.manifest
        for field_name in (
            "fatal",
            "production_order_execution_enabled",
            "private_credentials_permitted",
            "retained_frame_parser_health_claimed",
            "observed_source_completeness_claimed",
            "book_completeness_claimed",
            "m2_certified",
            "paper_execution_enabled",
            "promotion_ready",
        ):
            with pytest.raises(ValueError, match="forbids"):
                replace(manifest, **{field_name: True})
        with pytest.raises(ValueError, match="cleanly closed depth bridge"):
            replace(manifest, depth_bridge_lifecycle_cleanly_closed=False)
        with pytest.raises(ValueError, match="persisted WebSocket cursor finality"):
            replace(manifest, websocket_route_cursor_finality_persisted=False)

        v2_entry = object.__new__(WebSocketRouteCursorClosureEntryV2)
        v2_entries = (v2_entry, v2_entry)
        with pytest.raises(TypeError, match="foreign type"):
            replace(
                manifest,
                websocket_route_cursors=cast(Any, v2_entries),
            )
        with pytest.raises(TypeError, match="exact PersistedSessionClosureAuthorityV2"):
            assert_persisted_session_closure_authority_current_v2(
                cast(Any, receipt),
                lease=fixture.runtime_fixture.lease,
                session_start_authority=fixture.runtime_fixture.start_authority,
                promoting_plans=cast(Any, fixture.plans),
                finality_receipt=fixture.result.finality_receipt,
                pipeline=fixture.runtime_fixture.pipeline,
                ledger_seal_receipt=cast(Any, fixture.ledger_seal),
                ledger=fixture.runtime_fixture.ledger,
            )
    finally:
        await fixture.close()


@pytest.mark.asyncio
async def test_v8_session_closure_rejects_permutation_clone_foreign_and_downgrade(
    tmp_path: Path,
) -> None:
    permutation = _SessionClosureV8Fixture(tmp_path / "permutation")
    cloned_depth = _SessionClosureV8Fixture(tmp_path / "clone")
    foreign_depth = _SessionClosureV8Fixture(tmp_path / "foreign")
    v2_fixture = _ClosureFixture(tmp_path / "v2")
    try:
        await permutation.finalize()
        plans = permutation.plans
        permuted = (plans[1], plans[0], plans[2], plans[3])
        with pytest.raises(ValueError, match="permuted"):
            permutation.write(plans=permuted)
        assert not permutation.closure_path.exists()
        with pytest.raises(SessionAuthorityExistsError, match="already consumed"):
            permutation.write()

        await cloned_depth.finalize()
        clone = replace(cloned_depth.depth_plan)
        assert clone == cloned_depth.depth_plan
        assert clone is not cloned_depth.depth_plan
        with pytest.raises(ValueError, match="exact depth plan member identity"):
            cloned_depth.write(depth_plan=clone)
        assert not cloned_depth.closure_path.exists()

        await foreign_depth.finalize()
        foreign_plans = build_provisional_promoting_capture_plans_v8(("ETHUSDT",))
        foreign = cast(ProvisionalDepthRestQualificationPlanV8, foreign_plans[3])
        with pytest.raises(ValueError, match="exact depth plan member identity"):
            foreign_depth.write(depth_plan=foreign)
        assert not foreign_depth.closure_path.exists()

        await v2_fixture.finalize()
        v2_authority = v2_fixture.write()
        with pytest.raises(TypeError, match="exact PersistedSessionClosureAuthorityV8"):
            assert_persisted_session_closure_authority_current_v8(
                cast(Any, v2_authority),
                lease=v2_fixture.lease,
                session_start_authority=v2_fixture.start,
                promoting_plans=cast(Any, v2_fixture.plans),
                depth_plan=cast(Any, object()),
                finality_receipt=v2_fixture.finality,
                pipeline=v2_fixture.pipeline,
                ledger_seal_receipt=cast(Any, v2_fixture.ledger_seal),
                ledger=v2_fixture.ledger,
                depth_bridge_closure_entry=cast(Any, object()),
                finalized_websocket_route_cursors=cast(Any, (object(), object())),
                oi_coverage_close_receipt=cast(Any, object()),
            )
    finally:
        await permutation.close()
        await cloned_depth.close()
        await foreign_depth.close()
        await v2_fixture.close()


@pytest.mark.asyncio
async def test_v2_and_v8_writers_share_one_irreversible_closure_claim(
    tmp_path: Path,
) -> None:
    fixture = _SessionClosureV8Fixture(tmp_path)
    try:
        await fixture.finalize()
        receipt = fixture.write()
        assert receipt.canonical_path == os.path.normcase(str(fixture.closure_path.resolve()))
        with pytest.raises(SessionAuthorityExistsError, match="already consumed"):
            write_session_closure_manifest_v2(
                canonical_session_closure_manifest_path_v2(
                    fixture.runtime_fixture.lease
                ),
                lease=fixture.runtime_fixture.lease,
                session_start_authority=fixture.runtime_fixture.start_authority,
                promoting_plans=cast(Any, fixture.plans),
                finality_receipt=fixture.result.finality_receipt,
                pipeline=fixture.runtime_fixture.pipeline,
                ledger_seal_receipt=cast(Any, fixture.ledger_seal),
                ledger=fixture.runtime_fixture.ledger,
                stop_reason="COMPLETED_DURATION",
                closed_wall_ms=receipt.manifest.closed_wall_ms + 1,
                closed_monotonic_ns=receipt.manifest.closed_monotonic_ns + 1,
            )
    finally:
        await fixture.close()


@pytest.mark.asyncio
async def test_v8_closure_reader_rejects_unknown_schema_partial_hardlink_and_symlink(
    tmp_path: Path,
) -> None:
    fixture = _SessionClosureV8Fixture(tmp_path)
    try:
        await fixture.finalize()
        receipt = fixture.write()
        original = fixture.closure_path.read_bytes()
        unknown = original.replace(
            b"r4b_v2_capture_session_closure_manifest_v8",
            b"r4b_v2_capture_session_closure_manifest_v9",
        )
        assert len(unknown) == len(original)
        fixture.closure_path.write_bytes(unknown)
        with pytest.raises(SessionAuthorityIntegrityError, match="unknown schema"):
            fixture.assert_current(receipt)

        fixture.closure_path.write_bytes(original)
        partial = fixture.closure_path.with_name(fixture.closure_path.name + ".partial")
        partial.write_bytes(original)
        with pytest.raises(SessionAuthorityIntegrityError, match="partial artifact"):
            fixture.assert_current(receipt)
        partial.unlink()

        alias = fixture.closure_path.with_name("v8-closure-hard-link.jsonl")
        try:
            os.link(fixture.closure_path, alias)
        except OSError as exc:
            pytest.skip(f"hard links are unavailable: {exc}")
        with pytest.raises(SessionAuthorityIntegrityError, match="exactly one hard link"):
            fixture.assert_current(receipt)
        alias.unlink()

        target = fixture.closure_path.with_name("partial-target.jsonl")
        target.write_bytes(original)
        try:
            partial.symlink_to(target)
        except OSError:
            pass
        else:
            with pytest.raises(SessionAuthorityIntegrityError, match="symbolic link"):
                fixture.assert_current(receipt)
            partial.unlink()
    finally:
        await fixture.close()


def test_session_closure_write_once_helper_rejects_short_write_and_fsync_failure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class _ShortHandle:
        def __enter__(self) -> _ShortHandle:
            return self

        def __exit__(self, *_args: object) -> None:
            return None

        def write(self, encoded: bytes) -> int:
            return len(encoded) - 1

    class _ShortPath:
        def open(self, *_args: object, **_kwargs: object) -> _ShortHandle:
            return _ShortHandle()

    with pytest.raises(SessionAuthorityWriteError, match="short"):
        _write_closure_once(cast(Any, _ShortPath()), b"canonical\n")

    path = tmp_path / "fsync-failure.closure.jsonl"

    def fail_fsync(_descriptor: int) -> None:
        raise OSError("injected fsync failure")

    monkeypatch.setattr(os, "fsync", fail_fsync)
    with pytest.raises(SessionAuthorityWriteError, match="made durable"):
        _write_closure_once(path, b"canonical\n")
    assert path.exists()


@pytest.mark.asyncio
async def test_v8_claim_is_consumed_when_lease_seal_fails_after_file_write(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fixture = _SessionClosureV8Fixture(tmp_path)

    def fail_claim_seal(
        _lease: WriterLease,
        **_identity: object,
    ) -> None:
        raise WriterLeaseSessionClosureClaimError("injected claim seal failure")

    try:
        await fixture.finalize()
        monkeypatch.setattr(
            WriterLease,
            "seal_session_closure_authority",
            fail_claim_seal,
        )
        with pytest.raises(
            SessionAuthorityIntegrityError,
            match="could not seal its writer-lease claim",
        ):
            fixture.write()
        assert fixture.closure_path.exists()
        with pytest.raises(SessionAuthorityExistsError, match="already consumed"):
            fixture.write()
    finally:
        await fixture.close()
