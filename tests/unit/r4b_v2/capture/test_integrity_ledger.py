from __future__ import annotations

import base64
import hashlib
import json
import os
import threading
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import asdict, replace
from pathlib import Path

import pytest

import signalbot.r4b_v2.capture.integrity_ledger as integrity_ledger_module
from signalbot.capture.receipts import ReceiptTimestamp
from signalbot.capture.writer_lease import WriterLease
from signalbot.r4b_v2.canonical import canonical_json_line
from signalbot.r4b_v2.capture.batching import BatchPolicyV2, QueuedRawRecordV2
from signalbot.r4b_v2.capture.block_container import (
    BlockSigningAuthorityV2,
    Ed25519BlockSignerV2,
)
from signalbot.r4b_v2.capture.blocks import (
    BlockIntegrityError,
    BlockPolicyV2,
    GroupedBlockBuilderV2,
    GroupedBlockWriterV2,
    verify_grouped_blocks,
)
from signalbot.r4b_v2.capture.integrity_ledger import (
    CaptureCleanClosureSealV8,
    CaptureIntegrityEventV2,
    CaptureIntegrityLedgerCapacityError,
    CaptureIntegrityLedgerError,
    CaptureIntegrityLedgerIntegrityError,
    CaptureIntegrityLedgerV2,
    DataGapCauseV2,
    DataGapPayloadV2,
    FinalizedRecordLocatorV2,
    PersistedCaptureCleanClosureSealReceiptV2,
    PersistedCaptureCleanClosureSealReceiptV8,
    SourceGapCauseV2,
    SourceGapLeftBoundaryV2,
    SourceGapPayloadV2,
    SourceGapPhaseV2,
    attest_finalized_block_v2,
    verify_persisted_capture_clean_closure_seal_receipt_v2,
    verify_persisted_capture_clean_closure_seal_receipt_v8,
)
from signalbot.r4b_v2.capture.mirrored_wal import MirroredWalWriterV2
from signalbot.r4b_v2.capture.models import RawRecordV2, TransportV2, VenueV2
from signalbot.r4b_v2.capture.pipeline import DurableCaptureBatchWriterV2
from signalbot.r4b_v2.capture.plans import (
    ProvisionalDepthRestQualificationPlanV8,
    ProvisionalPromotingCapturePlanV2,
    build_provisional_promoting_capture_plans_v2,
    build_provisional_promoting_capture_plans_v8,
    provisional_promoting_plan_sha256_v2,
    provisional_promoting_plan_sha256_v8,
)
from signalbot.r4b_v2.capture.rest_depth import public_depth_rest_plan_sha256_v8
from signalbot.r4b_v2.capture.rest_depth_bridge_evidence import (
    DEPTH_BRIDGE_MAXIMUM_BUFFERED_RANGES_PER_SYMBOL_V8,
    DepthBridgeAttemptStartedV8,
    DepthBridgeAttemptTerminalV8,
    DepthBridgeCycleOutcomeV8,
    DepthBridgeCycleRefV8,
    DepthBridgeCycleTerminalV8,
    DepthBridgeEvidenceErrorV8,
    DepthBridgeEvidencePayloadV8,
    DepthBridgeGenerationDrainedV8,
    DepthBridgeGenerationStartedV8,
    DepthBridgePhaseV8,
    DepthBridgeRangeSummaryV8,
    DepthBridgeRegisteredCycleV8,
    DepthBridgeRestSourceLocatorV8,
    DepthBridgeTriggerRegisteredV8,
    DepthBridgeWaitTerminalV8,
    DepthBridgeWebSocketSourceLocatorV8,
    _issue_depth_bridge_coordinator_clean_close_receipt_v8,
    build_depth_bridge_cycle_ref_v8,
    build_depth_bridge_evidence_payload_v8,
    build_depth_bridge_range_summary_v8,
    depth_bridge_evidence_census_v8,
    depth_bridge_symbol_census_sha256_v8,
    parse_depth_bridge_evidence_payload_v8,
    validate_depth_bridge_evidence_payload_v8,
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
from signalbot.r4b_v2.capture.websocket_finality import (
    _issue_websocket_route_stop_receipt_v2,
    _issue_websocket_route_stop_receipt_v8,
    finalize_websocket_route_cursor_pair_v2,
    finalize_websocket_route_cursor_pair_v8,
)

HASH = "a" * 64
MAXIMUM_BYTES = 8 * 1024 * 1024
RESERVE_BYTES = 1024
MAX_EVENTS = 32
PROMOTING_PLANS = build_provisional_promoting_capture_plans_v2(("BTCUSDT",))
PROMOTING_PLAN_SHA256 = provisional_promoting_plan_sha256_v2(PROMOTING_PLANS)
PROMOTING_PLANS_V8 = build_provisional_promoting_capture_plans_v8(("BTCUSDT",))
PROMOTING_PLAN_SHA256_V8 = provisional_promoting_plan_sha256_v8(
    PROMOTING_PLANS_V8
)
DEPTH_PLAN_V8 = next(
    plan
    for plan in PROMOTING_PLANS_V8
    if type(plan) is ProvisionalDepthRestQualificationPlanV8
)
assert isinstance(DEPTH_PLAN_V8, ProvisionalDepthRestQualificationPlanV8)
LOCATOR_DOMAIN = b"R4B_V2_FINALIZED_RECORD_LOCATOR\0"
CLOSURE_WINDOW_START_MS = 2_000_000_000_000
CLOSURE_WINDOW_END_MS = (
    CLOSURE_WINDOW_START_MS + WAL_QUALIFICATION_DURATION_MS_V2
)
CLOSURE_H_START_MS = CLOSURE_WINDOW_END_MS + 60_000


class _Clock:
    def __init__(self) -> None:
        self.wall_value = 50_002
        self.monotonic_value = 60_002

    def wall_ms(self) -> int:
        return self.wall_value

    def monotonic_ns(self) -> int:
        return self.monotonic_value


def _authority(
    plan_sha256: str = PROMOTING_PLAN_SHA256,
) -> WalAuthorityV2:
    return WalAuthorityV2(
        attempt_id="attempt-integrity-ledger",
        protocol_sha256=HASH,
        plan_sha256=plan_sha256,
        source_manifest_sha256="c" * 64,
        schema_sha256="d" * 64,
        runtime_manifest_sha256="e" * 64,
    )


def _signer() -> Ed25519BlockSignerV2:
    return Ed25519BlockSignerV2.from_private_key_bytes(
        key_id="writer-key-integrity",
        private_key_bytes=b"\x07" * 32,
    )


def _signing_authority() -> BlockSigningAuthorityV2:
    signer = _signer()
    return BlockSigningAuthorityV2.from_public_key_bytes(
        key_id=signer.key_id,
        public_key_bytes=signer.public_key_bytes,
    )


def _policy() -> BlockPolicyV2:
    return BlockPolicyV2(
        qualification_id="sealed-zstd-1.5.7-l9",
        codec_candidate_id="zstd-1.5.7-l9-w0-checksum-content-size",
        compression_level=9,
        max_uncompressed_bytes=4_194_304,
        max_linger_ms=1_000,
    )


def _queued(ingest_seq: int) -> QueuedRawRecordV2:
    monotonic_ns = 1_000_000 + ingest_seq
    record = RawRecordV2.from_payload(
        session_id="session-integrity",
        plan_id="plan-integrity",
        protocol_hash=HASH,
        transport=TransportV2.WEBSOCKET,
        venue=VenueV2.USDM_FUTURES,
        route_id="futures-market",
        symbol="BTCUSDT",
        connection_id="connection-integrity",
        generation=1,
        frame_seq=ingest_seq,
        ingest_seq=ingest_seq,
        receipt_wall_ms=1_000 + ingest_seq,
        receipt_monotonic_ns=monotonic_ns,
        raw_payload='{"p":"100","q":"1"}',
        source_logical_key=f"trade-{ingest_seq}",
    )
    return QueuedRawRecordV2.encode(
        record,
        enqueued_monotonic_ns=monotonic_ns + 1,
    )


def _block_writer(
    directory: Path,
    *,
    verification_only: bool = False,
    authority: WalAuthorityV2 | None = None,
) -> GroupedBlockWriterV2:
    return GroupedBlockWriterV2(
        directory,
        authority=authority or _authority(),
        policy=_policy(),
        signer=_signer(),
        signing_authority=_signing_authority(),
        stream_group_id="futures-depth-group",
        segment_id="segment-000001",
        maximum_total_bytes=MAXIMUM_BYTES,
        emergency_reserve_bytes=RESERVE_BYTES,
        verification_only=verification_only,
    )


def _commit_one(writer: GroupedBlockWriterV2):  # type: ignore[no-untyped-def]
    builder = GroupedBlockBuilderV2(writer.policy)
    builder.offer(_queued(1), now_ns=1_000_001)
    block = builder.flush_tail(now_ns=1_000_002)
    assert block is not None
    return writer.commit(block)


def _ledger(
    root: Path,
    writer: GroupedBlockWriterV2,
    *,
    clock: _Clock | None = None,
    max_events: int = MAX_EVENTS,
    maximum_total_bytes: int = MAXIMUM_BYTES,
    fault_hook: object | None = None,
    writer_lease: WriterLease | None = None,
    authority: WalAuthorityV2 | None = None,
) -> CaptureIntegrityLedgerV2:
    observed_clock = clock or _Clock()
    return CaptureIntegrityLedgerV2(
        root,
        authority=authority or _authority(),
        block_directory=writer.directory,
        block_root_binding=writer.root_binding,
        block_signing_authority=writer.signing_authority,
        block_policy=writer.policy,
        block_stream_group_id=writer.stream_group_id,
        block_segment_id=writer.segment_id,
        maximum_total_bytes=maximum_total_bytes,
        emergency_reserve_bytes=RESERVE_BYTES,
        max_events=max_events,
        failure_domain_id="declared-integrity-ledger-device",
        writer_lease=writer_lease,
        wall_clock_ms=observed_clock.wall_ms,
        monotonic_clock_ns=observed_clock.monotonic_ns,
        fault_hook=fault_hook,  # type: ignore[arg-type]
    )


def _closure_wal_policy(sync_ms: int, record_cap: int) -> WalSyncPolicyV2:
    return WalSyncPolicyV2(
        qualification_id=_policy().qualification_id,
        fsync_candidate_id=wal_candidate_id_v2(
            sync_ms=sync_ms,
            record_cap=record_cap,
        ),
        interval_ms=sync_ms,
        max_unsynced_records=record_cap,
        max_unsynced_bytes=100_000,
        max_record_bytes=20_000,
        max_segment_bytes=1_000_000,
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
    candidates = tuple(
        WalCandidateQualificationV2(
            policy=_closure_wal_policy(sync_ms, record_cap),
            metrics=_closure_candidate_metrics(
                passed=(sync_ms, record_cap) == (10, 256)
            ),
            measurement_root_sha256=hashlib.sha256(
                f"{sync_ms}:{record_cap}".encode()
            ).hexdigest(),
        )
        for sync_ms in WAL_SYNC_CANDIDATES_MS_V2
        for record_cap in WAL_RECORD_CAP_CANDIDATES_V2
    )
    qualification = WalQualificationRunV2(
        qualification_id=_policy().qualification_id,
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


def _closure_batch_policy() -> BatchPolicyV2:
    return BatchPolicyV2(
        max_records=256,
        max_encoded_bytes=100_000,
        max_linger_us=10_000,
        queue_max_events=512,
        queue_max_encoded_bytes=1_000_000,
        low_water_events=10,
        low_water_encoded_bytes=100_000,
        qualification_id=_policy().qualification_id,
    )


def _closure_stack(
    root: Path,
    lease: WriterLease,
    *,
    ledger_fault_hook: object | None = None,
    close_after_finality: bool = True,
    authority: WalAuthorityV2 | None = None,
):  # type: ignore[no-untyped-def]
    selected_authority = authority or _authority()
    selection = _closure_selection_receipt()
    wal_policy = selection.selected_policy
    assert wal_policy is not None
    wal_writer = MirroredWalWriterV2(
        root / "wal-primary",
        root / "wal-mirror",
        authority=selected_authority,
        policy=wal_policy,
        selection_receipt=selection,
        primary_maximum_total_bytes=MAXIMUM_BYTES,
        mirror_maximum_total_bytes=MAXIMUM_BYTES,
        primary_emergency_reserve_bytes=RESERVE_BYTES,
        mirror_emergency_reserve_bytes=RESERVE_BYTES,
        primary_failure_domain_id="closure-primary-device",
        mirror_failure_domain_id="closure-mirror-device",
        clock_ns=lambda: 10_000_000,
    )
    block_writer = _block_writer(
        root / "blocks",
        authority=selected_authority,
    )
    durable_writer = DurableCaptureBatchWriterV2(
        batch_policy=_closure_batch_policy(),
        wal_writer=wal_writer,
        block_builder=GroupedBlockBuilderV2(block_writer.policy),
        block_writer=block_writer,
        clock_ns=lambda: 10_000_000,
        writer_lease=lease,
    )
    assert durable_writer.append_many((_queued(1),)) == 1
    finality_receipt = durable_writer.finalize_through(
        requested_ingest_seq=1,
        fence_ingest_seq=1,
        fence_monotonic_ns=10_000_000,
    )
    if close_after_finality:
        durable_writer.close()
    ledger = _ledger(
        root / "ledger",
        block_writer,
        writer_lease=lease,
        fault_hook=ledger_fault_hook,
        authority=selected_authority,
    )
    return durable_writer, wal_writer, block_writer, ledger, finality_receipt


def _verification_only_wal(
    wal_writer: MirroredWalWriterV2,
) -> MirroredWalWriterV2:
    primary_directory, mirror_directory = wal_writer.root_directories
    return MirroredWalWriterV2.open_verification_only_v2(
        primary_directory,
        mirror_directory,
        authority=wal_writer.authority,
        policy=wal_writer.policy,
        selection_receipt=wal_writer.selection_receipt,
        primary_maximum_total_bytes=MAXIMUM_BYTES,
        mirror_maximum_total_bytes=MAXIMUM_BYTES,
        primary_emergency_reserve_bytes=RESERVE_BYTES,
        mirror_emergency_reserve_bytes=RESERVE_BYTES,
        primary_failure_domain_id=(
            wal_writer.primary_root_binding.failure_domain_id
        ),
        mirror_failure_domain_id=(
            wal_writer.mirror_root_binding.failure_domain_id
        ),
        clock_ns=lambda: 10_000_000,
    )


def _verification_only_closure_owners(
    wal_writer: MirroredWalWriterV2,
    block_writer: GroupedBlockWriterV2,
) -> tuple[MirroredWalWriterV2, GroupedBlockWriterV2]:
    verification_wal = _verification_only_wal(wal_writer)
    verification_blocks = GroupedBlockWriterV2(
        block_writer.directory,
        authority=block_writer.authority,
        policy=block_writer.policy,
        signer=block_writer.signer,
        signing_authority=block_writer.signing_authority,
        stream_group_id=block_writer.stream_group_id,
        segment_id=block_writer.segment_id,
        maximum_total_bytes=block_writer.maximum_total_bytes,
        emergency_reserve_bytes=block_writer.emergency_reserve_bytes,
        root_role=block_writer.root_binding.root_role,
        failure_domain_id=block_writer.root_binding.failure_domain_id,
        verification_only=True,
    )
    return verification_wal, verification_blocks


def _acquire_closure_lease(root: Path) -> WriterLease:
    scope = root / "lease"
    scope.mkdir(parents=True)
    return WriterLease.acquire(scope)


def _seal_closure(
    ledger: CaptureIntegrityLedgerV2,
    wal_writer: MirroredWalWriterV2,
    block_writer: GroupedBlockWriterV2,
    finality_receipt,  # type: ignore[no-untyped-def]
):  # type: ignore[no-untyped-def]
    return ledger.seal_clean_closure_v2(
        promoting_plans=PROMOTING_PLANS,
        finality_receipt=finality_receipt,
        wal_writer=wal_writer,
        block_writer=block_writer,
        session_id="session-integrity",
        process_boot_id="boot-integrity",
        seal_wall_ms=100_000,
        seal_monotonic_ns=10_000_000,
    )


def _append_gap(
    ledger: CaptureIntegrityLedgerV2,
    first: int,
    last: int,
    *,
    evidence_sha256: str = "1" * 64,
    cause: DataGapCauseV2 = DataGapCauseV2.BOUNDED_QUEUE_OVERFLOW,
):  # type: ignore[no-untyped-def]
    return ledger.append_data_gap(
        first_missing_ingest_seq=first,
        last_missing_ingest_seq=last,
        receipt_wall_lower_bound_ms=10_000,
        receipt_wall_upper_bound_ms=10_000,
        receipt_monotonic_lower_bound_ns=20_000,
        receipt_monotonic_upper_bound_ns=20_000,
        cause=cause,
        source_component="bounded-ws-handoff",
        evidence_sha256=evidence_sha256,
    )


def _append_source_gap_open(
    ledger: CaptureIntegrityLedgerV2,
    *,
    route_id: str = "usdm_market",
    left_connection_id: str | None = "connection-before",
    left_generation: int | None = 1,
    left_frame_seq: int | None = 50,
    left_ingest_seq: int | None = 100,
    left_wall_ms: int = 50_000,
    left_monotonic_ns: int = 60_000,
    detected_wall_ms: int = 50_001,
    detected_monotonic_ns: int = 60_001,
    cause: SourceGapCauseV2 = SourceGapCauseV2.WEBSOCKET_DISCONNECT,
    left_boundary_kind: SourceGapLeftBoundaryV2 = (
        SourceGapLeftBoundaryV2.RETAINED_FRAME
    ),
    evidence_sha256: str = "3" * 64,
):  # type: ignore[no-untyped-def]
    selected_plan = next(
        plan
        for plan in PROMOTING_PLANS
        if isinstance(plan, ProvisionalPromotingCapturePlanV2)
        and plan.route_id == route_id
    )
    return ledger.append_source_gap_open(
        PROMOTING_PLANS,
        selected_plan,
        session_id="session-integrity",
        process_boot_id="boot-integrity",
        cause=cause,
        left_boundary_kind=left_boundary_kind,
        left_connection_id=left_connection_id,
        left_generation=left_generation,
        left_frame_seq=left_frame_seq,
        left_ingest_seq=left_ingest_seq,
        left_wall_ms=left_wall_ms,
        left_monotonic_ns=left_monotonic_ns,
        detected_wall_ms=detected_wall_ms,
        detected_monotonic_ns=detected_monotonic_ns,
        source_component="v2-usdm-websocket-owner",
        evidence_sha256=evidence_sha256,
    )


def _bound_source_gap(
    ledger: CaptureIntegrityLedgerV2,
    open_event: CaptureIntegrityEventV2,
    *,
    clock: _Clock | None = None,
    right_connection_id: str = "connection-after",
    right_generation: int = 2,
    right_frame_seq: int = 1,
    right_ingest_seq: int = 101,
    right_wall_ms: int = 50_010,
    right_monotonic_ns: int = 60_010,
    evidence_sha256: str = "5" * 64,
) -> CaptureIntegrityEventV2:
    if clock is not None:
        clock.wall_value = max(clock.wall_value, right_wall_ms + 1)
        clock.monotonic_value = max(
            clock.monotonic_value,
            right_monotonic_ns + 1,
        )
    open_payload = SourceGapPayloadV2(**open_event.payload)  # type: ignore[arg-type]
    right_record = RawRecordV2.from_payload(
        session_id=open_payload.session_id,
        plan_id=open_payload.plan_id,
        protocol_hash=ledger.authority.protocol_sha256,
        transport=TransportV2.WEBSOCKET,
        venue=VenueV2.USDM_FUTURES,
        route_id=open_payload.route_id,
        symbol=None,
        connection_id=right_connection_id,
        generation=right_generation,
        frame_seq=right_frame_seq,
        ingest_seq=right_ingest_seq,
        receipt_wall_ms=right_wall_ms,
        receipt_monotonic_ns=right_monotonic_ns,
        raw_payload=b'{"synthetic":"right"}',
        source_logical_key=None,
    )
    endpoints = {
        right_ingest_seq: (right_record, _locator(right_ingest_seq)),
    }
    if open_payload.left_ingest_seq is not None:
        assert open_payload.left_connection_id is not None
        assert open_payload.left_generation is not None
        assert open_payload.left_frame_seq is not None
        left_record = RawRecordV2.from_payload(
            session_id=open_payload.session_id,
            plan_id=open_payload.plan_id,
            protocol_hash=ledger.authority.protocol_sha256,
            transport=TransportV2.WEBSOCKET,
            venue=VenueV2.USDM_FUTURES,
            route_id=open_payload.route_id,
            symbol=None,
            connection_id=open_payload.left_connection_id,
            generation=open_payload.left_generation,
            frame_seq=open_payload.left_frame_seq,
            ingest_seq=open_payload.left_ingest_seq,
            receipt_wall_ms=open_payload.left_wall_ms,
            receipt_monotonic_ns=open_payload.left_monotonic_ns,
            raw_payload=b'{"synthetic":"left"}',
            source_logical_key=None,
        )
        endpoints[open_payload.left_ingest_seq] = (
            left_record,
            _locator(open_payload.left_ingest_seq),
        )

    def read_endpoints(
        ingest_seqs: tuple[int, ...],
    ) -> dict[int, tuple[RawRecordV2, FinalizedRecordLocatorV2]]:
        return {ingest_seq: endpoints[ingest_seq] for ingest_seq in ingest_seqs}

    ledger._read_source_gap_endpoints_unlocked = read_endpoints  # type: ignore[method-assign]
    return ledger.append_source_gap_bounded(
        open_event,
        right_ingest_seq=right_ingest_seq,
        evidence_sha256=evidence_sha256,
    )


def _locator(ingest_seq: int) -> FinalizedRecordLocatorV2:
    fields: dict[str, object] = {
        "authority_sha256": _authority().sha256,
        "block_sequence": 1,
        "block_hash": "b" * 64,
        "ingest_seq": ingest_seq,
        "record_jsonl_sha256": hashlib.sha256(
            f"synthetic-{ingest_seq}".encode()
        ).hexdigest(),
        "schema_version": "r4b_v2_finalized_record_locator_v1",
    }
    locator_sha256 = hashlib.sha256(
        LOCATOR_DOMAIN + canonical_json_line(fields)
    ).hexdigest()
    return FinalizedRecordLocatorV2(
        **fields,  # type: ignore[arg-type]
        locator_sha256=locator_sha256,
    )


def _establish_source_session(
    ledger: CaptureIntegrityLedgerV2,
    clock: _Clock,
    *,
    route_id: str = "usdm_market",
    connection_id: str = "connection-before",
    right_ingest_seq: int = 1,
    base_wall_ms: int = 49_000,
    base_monotonic_ns: int = 59_000,
):  # type: ignore[no-untyped-def]
    clock.wall_value = base_wall_ms + 1
    clock.monotonic_value = base_monotonic_ns + 1
    opened = _append_source_gap_open(
        ledger,
        route_id=route_id,
        left_connection_id=None,
        left_generation=None,
        left_frame_seq=None,
        left_ingest_seq=None,
        left_wall_ms=base_wall_ms,
        left_monotonic_ns=base_monotonic_ns,
        detected_wall_ms=base_wall_ms,
        detected_monotonic_ns=base_monotonic_ns,
        cause=SourceGapCauseV2.SESSION_START_PENDING,
        left_boundary_kind=SourceGapLeftBoundaryV2.SESSION_START,
    )
    bounded = _bound_source_gap(
        ledger,
        opened,
        clock=clock,
        right_connection_id=connection_id,
        right_generation=1,
        right_ingest_seq=right_ingest_seq,
        right_wall_ms=base_wall_ms + 10,
        right_monotonic_ns=base_monotonic_ns + 10,
    )
    return opened, bounded


def test_data_gap_is_exact_jcs_deterministic_and_retry_idempotent(
    tmp_path: Path,
) -> None:
    writer = _block_writer(tmp_path / "blocks")
    clock = _Clock()
    ledger = _ledger(tmp_path / "ledger", writer, clock=clock)
    event = _append_gap(ledger, 5, 7)
    assert event.event_type == "DATA_GAP"
    assert event.payload["missing_count"] == 3
    assert event.previous_event_sha256 is None
    path = tmp_path / "ledger" / "integrity-event-00000001.json"
    encoded = path.read_bytes()
    assert canonical_json_line(json.loads(encoded)) == encoded

    clock.wall_value += 99
    clock.monotonic_value += 99
    duplicate = _append_gap(ledger, 5, 7)
    assert duplicate == event
    assert ledger.next_event_sequence == 2
    assert len(list((tmp_path / "ledger").glob("integrity-event-*.json"))) == 1

    reopened = _ledger(tmp_path / "ledger", writer)
    assert reopened.events == (event,)
    assert reopened.last_event_sha256 == event.sha256


def test_data_gap_equality_adjacency_overlap_and_order_boundaries(
    tmp_path: Path,
) -> None:
    writer = _block_writer(tmp_path / "blocks")
    ledger = _ledger(tmp_path / "ledger", writer)
    first = _append_gap(ledger, 10, 12)
    adjacent = _append_gap(ledger, 13, 13, evidence_sha256="2" * 64)
    assert first.payload["receipt_wall_lower_bound_ms"] == 10_000
    assert adjacent.payload["missing_count"] == 1

    for lower, upper in ((13, 13), (11, 11), (1, 2), (12, 14)):
        with pytest.raises(
            CaptureIntegrityLedgerIntegrityError,
            match=r"overlap|order",
        ):
            _append_gap(
                ledger,
                lower,
                upper,
                evidence_sha256=hashlib.sha256(f"{lower}:{upper}".encode()).hexdigest(),
            )
    assert len(ledger.events) == 2


def test_data_gap_conflicting_duplicate_fails_closed(tmp_path: Path) -> None:
    writer = _block_writer(tmp_path / "blocks")
    ledger = _ledger(tmp_path / "ledger", writer)
    _append_gap(ledger, 20, 21)
    with pytest.raises(CaptureIntegrityLedgerIntegrityError, match=r"overlap|order"):
        _append_gap(ledger, 20, 21, evidence_sha256="f" * 64)
    with pytest.raises(CaptureIntegrityLedgerIntegrityError, match=r"overlap|order"):
        _append_gap(
            ledger,
            20,
            21,
            cause=DataGapCauseV2.UNRECOVERABLE_PARTIAL_APPEND,
        )
    assert len(ledger.events) == 1


def test_data_gap_rejects_inexact_count_interval_and_receipt_bounds() -> None:
    valid = DataGapPayloadV2(
        first_missing_ingest_seq=1,
        last_missing_ingest_seq=2,
        missing_count=2,
        receipt_wall_lower_bound_ms=10,
        receipt_wall_upper_bound_ms=10,
        receipt_monotonic_lower_bound_ns=20,
        receipt_monotonic_upper_bound_ns=20,
        cause=DataGapCauseV2.BOUNDED_QUEUE_OVERFLOW.value,
        source_component="queue",
        evidence_sha256="1" * 64,
    )
    with pytest.raises(ValueError, match="missing_count"):
        replace(valid, missing_count=1)
    with pytest.raises(ValueError, match="ingest interval"):
        replace(valid, first_missing_ingest_seq=3)
    with pytest.raises(ValueError, match="wall bounds"):
        replace(valid, receipt_wall_lower_bound_ms=11)
    with pytest.raises(ValueError, match="sealed cause"):
        replace(valid, cause="OTHER")


def test_source_gap_open_and_bounded_are_unknown_count_jcs_and_idempotent(
    tmp_path: Path,
) -> None:
    writer = _block_writer(tmp_path / "blocks")
    clock = _Clock()
    ledger = _ledger(tmp_path / "ledger", writer, clock=clock)
    session_open, session_bounded = _establish_source_session(ledger, clock)
    clock.wall_value = 50_002
    clock.monotonic_value = 60_002

    opened = _append_source_gap_open(ledger)
    assert opened.event_type == "SOURCE_GAP"
    assert opened.payload["phase"] == SourceGapPhaseV2.OPEN.value
    assert opened.payload["source_message_count_known"] is False
    assert opened.payload["missing_source_message_count"] is None
    assert opened.payload["left_ingest_seq"] == 100
    assert opened.payload["right_ingest_seq"] is None
    encoded = (tmp_path / "ledger" / "integrity-event-00000003.json").read_bytes()
    assert canonical_json_line(json.loads(encoded)) == encoded

    clock.wall_value += 99
    clock.monotonic_value += 99
    assert _append_source_gap_open(ledger) == opened
    bounded = _bound_source_gap(ledger, opened, clock=clock)
    assert bounded.payload["phase"] == SourceGapPhaseV2.BOUNDED.value
    assert bounded.payload["gap_id"] == opened.payload["gap_id"]
    assert bounded.payload["open_event_sha256"] == opened.sha256
    assert bounded.payload["right_ingest_seq"] == 101
    assert _bound_source_gap(ledger, opened) == bounded
    assert len(ledger.events) == 4
    assert _ledger(tmp_path / "ledger", writer).events == (
        session_open,
        session_bounded,
        opened,
        bounded,
    )


def test_source_gap_rejects_fabricated_count_and_invalid_boundaries(
    tmp_path: Path,
) -> None:
    writer = _block_writer(tmp_path / "blocks")
    clock = _Clock()
    ledger = _ledger(tmp_path / "ledger", writer, clock=clock)
    _establish_source_session(ledger, clock)
    clock.wall_value = 50_002
    clock.monotonic_value = 60_002
    opened = _append_source_gap_open(ledger)
    bounded = _bound_source_gap(ledger, opened, clock=clock)
    open_payload = SourceGapPayloadV2(**opened.payload)  # type: ignore[arg-type]
    bounded_payload = SourceGapPayloadV2(**bounded.payload)  # type: ignore[arg-type]

    with pytest.raises(ValueError, match="remain unknown"):
        replace(open_payload, source_message_count_known=True)
    with pytest.raises(ValueError, match="cannot claim"):
        replace(open_payload, missing_source_message_count=1)
    with pytest.raises(ValueError, match="sealed phase"):
        replace(open_payload, phase="OTHER")
    with pytest.raises(ValueError, match="stream_count"):
        replace(open_payload, affected_stream_count=0)
    with pytest.raises(ValueError, match="sealed cause"):
        replace(open_payload, cause="OTHER")
    with pytest.raises(ValueError, match="cannot claim a right cursor"):
        replace(open_payload, right_connection_id="too-early")
    with pytest.raises(ValueError, match="generation must advance"):
        replace(bounded_payload, right_generation=1)
    with pytest.raises(ValueError, match="successor frame 1"):
        replace(bounded_payload, right_frame_seq=2)
    with pytest.raises(ValueError, match="distinct connection"):
        replace(bounded_payload, right_connection_id="connection-before")
    with pytest.raises(ValueError, match=r"locator differs|cursors must advance"):
        replace(bounded_payload, right_ingest_seq=100)
    with pytest.raises(ValueError, match="follow detection"):
        replace(bounded_payload, right_monotonic_ns=60_001)
    with pytest.raises(ValueError, match="OPEN event hash"):
        replace(bounded_payload, open_event_sha256=None)


def test_source_gap_requires_open_before_close_and_one_open_per_scope(
    tmp_path: Path,
) -> None:
    writer = _block_writer(tmp_path / "blocks")
    clock = _Clock()
    ledger = _ledger(tmp_path / "ledger", writer, clock=clock)
    _establish_source_session(ledger, clock)
    clock.wall_value = 50_002
    clock.monotonic_value = 60_002
    first_open = _append_source_gap_open(ledger)
    with pytest.raises(CaptureIntegrityLedgerIntegrityError, match="unbounded OPEN"):
        _append_source_gap_open(
            ledger,
            detected_wall_ms=50_002,
            detected_monotonic_ns=60_002,
            evidence_sha256="6" * 64,
        )
    first_bounded = _bound_source_gap(ledger, first_open, clock=clock)
    with pytest.raises(CaptureIntegrityLedgerIntegrityError, match="unmatched OPEN"):
        _bound_source_gap(
            ledger,
            first_open,
            clock=clock,
            evidence_sha256="7" * 64,
        )

    clock.wall_value = 50_012
    clock.monotonic_value = 60_012
    second_open = _append_source_gap_open(
        ledger,
        left_connection_id="connection-after",
        left_generation=2,
        left_frame_seq=1,
        left_ingest_seq=101,
        left_wall_ms=50_010,
        left_monotonic_ns=60_010,
        detected_wall_ms=50_011,
        detected_monotonic_ns=60_011,
        evidence_sha256="8" * 64,
    )
    second_bounded = _bound_source_gap(
        ledger,
        second_open,
        clock=clock,
        right_connection_id="connection-third",
        right_generation=3,
        right_ingest_seq=102,
        right_wall_ms=50_020,
        right_monotonic_ns=60_020,
        evidence_sha256="9" * 64,
    )
    assert first_open.event_sequence == 3
    assert first_bounded.event_sequence == 4
    assert second_open.event_sequence == 5
    assert second_bounded.event_sequence == 6

    _establish_source_session(
        ledger,
        clock,
        route_id="usdm_public",
        connection_id="public-before",
        right_ingest_seq=10,
        base_wall_ms=50_030,
        base_monotonic_ns=60_030,
    )
    clock.wall_value = 50_042
    clock.monotonic_value = 60_042
    independent_route = _append_source_gap_open(
        ledger,
        route_id="usdm_public",
        left_connection_id="public-before",
        left_generation=1,
        left_frame_seq=10,
        left_ingest_seq=10,
        left_wall_ms=50_040,
        left_monotonic_ns=60_040,
        detected_wall_ms=50_041,
        detected_monotonic_ns=60_041,
        evidence_sha256="a" * 64,
    )
    assert independent_route.event_sequence == 9

    with pytest.raises(CaptureIntegrityLedgerIntegrityError, match=r"continue|order"):
        _append_source_gap_open(
            ledger,
            left_connection_id="stale-before",
            left_generation=1,
            left_frame_seq=1,
            left_ingest_seq=99,
            left_monotonic_ns=59_999,
            detected_monotonic_ns=60_000,
            evidence_sha256="b" * 64,
        )


def test_session_start_source_gap_opens_before_first_frame(tmp_path: Path) -> None:
    writer = _block_writer(tmp_path / "blocks")
    clock = _Clock()
    clock.wall_value = 49_001
    clock.monotonic_value = 59_001
    ledger = _ledger(tmp_path / "ledger", writer, clock=clock)
    opened = _append_source_gap_open(
        ledger,
        left_connection_id=None,
        left_generation=None,
        left_frame_seq=None,
        left_ingest_seq=None,
        left_wall_ms=49_000,
        left_monotonic_ns=59_000,
        detected_wall_ms=49_000,
        detected_monotonic_ns=59_000,
        cause=SourceGapCauseV2.SESSION_START_PENDING,
        left_boundary_kind=SourceGapLeftBoundaryV2.SESSION_START,
    )
    bounded = _bound_source_gap(
        ledger,
        opened,
        clock=clock,
        right_connection_id="connection-first",
        right_generation=3,
        right_ingest_seq=1,
        right_wall_ms=49_010,
        right_monotonic_ns=59_010,
    )
    assert bounded.payload["right_generation"] == 3
    with pytest.raises(ValueError, match="SESSION_START_PENDING"):
        _append_source_gap_open(
            _ledger(tmp_path / "other-ledger", writer),
            left_connection_id=None,
            left_generation=None,
            left_frame_seq=None,
            left_ingest_seq=None,
            cause=SourceGapCauseV2.WEBSOCKET_DISCONNECT,
            left_boundary_kind=SourceGapLeftBoundaryV2.SESSION_START,
        )


def test_source_gap_open_is_plan_bound_and_recording_causal(tmp_path: Path) -> None:
    writer = _block_writer(tmp_path / "blocks")
    clock = _Clock()
    ledger = _ledger(tmp_path / "ledger", writer, clock=clock)
    wrong_plans = build_provisional_promoting_capture_plans_v2(("ETHUSDT",))
    wrong_market = next(
        plan
        for plan in wrong_plans
        if isinstance(plan, ProvisionalPromotingCapturePlanV2)
        and plan.route_id == "usdm_market"
    )
    with pytest.raises(CaptureIntegrityLedgerIntegrityError, match="plan bundle"):
        ledger.append_source_gap_open(
            wrong_plans,
            wrong_market,
            session_id="session-integrity",
            process_boot_id="boot-integrity",
            cause=SourceGapCauseV2.SESSION_START_PENDING,
            left_boundary_kind=SourceGapLeftBoundaryV2.SESSION_START,
            left_connection_id=None,
            left_generation=None,
            left_frame_seq=None,
            left_ingest_seq=None,
            left_wall_ms=50_000,
            left_monotonic_ns=60_000,
            detected_wall_ms=50_000,
            detected_monotonic_ns=60_000,
            source_component="v2-usdm-websocket-owner",
            evidence_sha256="1" * 64,
        )

    clock.monotonic_value = 59_999
    with pytest.raises(
        CaptureIntegrityLedgerIntegrityError,
        match="after its durable event recording",
    ):
        _append_source_gap_open(
            ledger,
            left_connection_id=None,
            left_generation=None,
            left_frame_seq=None,
            left_ingest_seq=None,
            left_wall_ms=50_000,
            left_monotonic_ns=59_999,
            detected_wall_ms=50_000,
            detected_monotonic_ns=60_000,
            cause=SourceGapCauseV2.SESSION_START_PENDING,
            left_boundary_kind=SourceGapLeftBoundaryV2.SESSION_START,
        )
    assert not ledger.events


def test_source_gap_rejects_process_boot_drift_and_ledger_clock_regression(
    tmp_path: Path,
) -> None:
    writer = _block_writer(tmp_path / "blocks")
    clock = _Clock()
    ledger = _ledger(tmp_path / "ledger", writer, clock=clock)
    _establish_source_session(ledger, clock)
    clock.wall_value = 50_002
    clock.monotonic_value = 60_002
    with pytest.raises(
        CaptureIntegrityLedgerIntegrityError,
        match=r"process boots|owner drifted",
    ):
        selected_plan = next(
            plan
            for plan in PROMOTING_PLANS
            if isinstance(plan, ProvisionalPromotingCapturePlanV2)
            and plan.route_id == "usdm_market"
        )
        ledger.append_source_gap_open(
            PROMOTING_PLANS,
            selected_plan,
            session_id="session-integrity",
            process_boot_id="different-boot",
            cause=SourceGapCauseV2.WEBSOCKET_DISCONNECT,
            left_boundary_kind=SourceGapLeftBoundaryV2.RETAINED_FRAME,
            left_connection_id="connection-before",
            left_generation=1,
            left_frame_seq=1,
            left_ingest_seq=1,
            left_wall_ms=49_010,
            left_monotonic_ns=59_010,
            detected_wall_ms=50_001,
            detected_monotonic_ns=60_001,
            source_component="v2-usdm-websocket-owner",
            evidence_sha256="2" * 64,
        )

    clock.monotonic_value = 59_000
    with pytest.raises(CaptureIntegrityLedgerIntegrityError, match="moved backwards"):
        _append_gap(ledger, 10, 10)


def test_source_gap_open_partial_is_recovered_and_retry_is_idempotent(
    tmp_path: Path,
) -> None:
    writer = _block_writer(tmp_path / "blocks")
    clock = _Clock()

    def crash(point: str) -> None:
        if point == "after_event_write":
            raise OSError("synthetic SOURCE_GAP OPEN crash")

    failed = _ledger(
        tmp_path / "ledger",
        writer,
        clock=clock,
        fault_hook=crash,
    )
    with pytest.raises(OSError, match="OPEN crash"):
        _append_source_gap_open(
            failed,
            left_connection_id=None,
            left_generation=None,
            left_frame_seq=None,
            left_ingest_seq=None,
            left_wall_ms=50_000,
            left_monotonic_ns=60_000,
            detected_wall_ms=50_000,
            detected_monotonic_ns=60_000,
            cause=SourceGapCauseV2.SESSION_START_PENDING,
            left_boundary_kind=SourceGapLeftBoundaryV2.SESSION_START,
        )
    recovered = _ledger(tmp_path / "ledger", writer, clock=clock)
    assert recovered.events[0].payload["phase"] == SourceGapPhaseV2.OPEN.value
    assert _append_source_gap_open(
        recovered,
        left_connection_id=None,
        left_generation=None,
        left_frame_seq=None,
        left_ingest_seq=None,
        left_wall_ms=50_000,
        left_monotonic_ns=60_000,
        detected_wall_ms=50_000,
        detected_monotonic_ns=60_000,
        cause=SourceGapCauseV2.SESSION_START_PENDING,
        left_boundary_kind=SourceGapLeftBoundaryV2.SESSION_START,
    ) == recovered.events[0]


def test_source_gap_bounded_partial_is_recovered_and_retry_is_idempotent(
    tmp_path: Path,
) -> None:
    writer = _block_writer(tmp_path / "blocks")
    clock = _Clock()
    ledger = _ledger(tmp_path / "ledger", writer, clock=clock)
    _establish_source_session(ledger, clock)
    clock.wall_value = 50_002
    clock.monotonic_value = 60_002
    opened = _append_source_gap_open(ledger)

    def crash(point: str) -> None:
        if point == "after_event_write":
            raise OSError("synthetic SOURCE_GAP BOUNDED crash")

    clock.wall_value = 50_011
    clock.monotonic_value = 60_011
    failed = _ledger(
        tmp_path / "ledger",
        writer,
        clock=clock,
        fault_hook=crash,
    )
    with pytest.raises(OSError, match="BOUNDED crash"):
        _bound_source_gap(failed, opened, clock=clock)
    recovered = _ledger(tmp_path / "ledger", writer, clock=clock)
    assert recovered.events[-1].payload["phase"] == SourceGapPhaseV2.BOUNDED.value
    assert _bound_source_gap(recovered, opened, clock=clock) == recovered.events[-1]


def test_source_gap_open_reserves_its_bounded_event_slot(tmp_path: Path) -> None:
    writer = _block_writer(tmp_path / "blocks")
    clock = _Clock()
    too_small = _ledger(
        tmp_path / "too-small-ledger",
        writer,
        clock=clock,
        max_events=1,
    )
    with pytest.raises(CaptureIntegrityLedgerCapacityError, match="closure slots"):
        _append_source_gap_open(
            too_small,
            left_connection_id=None,
            left_generation=None,
            left_frame_seq=None,
            left_ingest_seq=None,
            left_wall_ms=50_000,
            left_monotonic_ns=60_000,
            detected_wall_ms=50_000,
            detected_monotonic_ns=60_000,
            cause=SourceGapCauseV2.SESSION_START_PENDING,
            left_boundary_kind=SourceGapLeftBoundaryV2.SESSION_START,
        )
    assert not too_small.events

    ledger = _ledger(
        tmp_path / "exact-ledger",
        writer,
        clock=clock,
        max_events=2,
    )
    opened = _append_source_gap_open(
        ledger,
        left_connection_id=None,
        left_generation=None,
        left_frame_seq=None,
        left_ingest_seq=None,
        left_wall_ms=50_000,
        left_monotonic_ns=60_000,
        detected_wall_ms=50_000,
        detected_monotonic_ns=60_000,
        cause=SourceGapCauseV2.SESSION_START_PENDING,
        left_boundary_kind=SourceGapLeftBoundaryV2.SESSION_START,
    )
    with pytest.raises(CaptureIntegrityLedgerCapacityError, match="closure slots"):
        _append_gap(ledger, 1, 1)
    bounded = _bound_source_gap(
        ledger,
        opened,
        clock=clock,
        right_connection_id="connection-first",
        right_generation=1,
        right_ingest_seq=1,
    )
    assert bounded.event_sequence == 2


def test_source_gap_bounded_uses_current_ledger_after_unrelated_append(
    tmp_path: Path,
) -> None:
    writer = _block_writer(tmp_path / "blocks")
    clock = _Clock()
    ledger = _ledger(tmp_path / "ledger", writer, clock=clock)
    _establish_source_session(ledger, clock)
    clock.wall_value = 50_002
    clock.monotonic_value = 60_002
    opened = _append_source_gap_open(ledger)
    _append_gap(ledger, 200, 200, evidence_sha256="b" * 64)
    bounded = _bound_source_gap(ledger, opened, clock=clock)
    assert bounded.previous_event_sha256 == ledger.events[-2].sha256


def test_finalized_record_locator_rejects_hash_tamper(
    tmp_path: Path,
) -> None:
    writer = _block_writer(tmp_path / "blocks")
    clock = _Clock()
    ledger = _ledger(tmp_path / "ledger", writer, clock=clock)
    _establish_source_session(ledger, clock)
    clock.wall_value = 50_002
    clock.monotonic_value = 60_002
    opened = _append_source_gap_open(ledger)
    bounded = _bound_source_gap(ledger, opened, clock=clock)
    document = bounded.payload["right_record_locator"]
    assert isinstance(document, dict)
    tampered = dict(document)
    tampered["record_jsonl_sha256"] = "f" * 64

    with pytest.raises(ValueError, match="locator hash"):
        FinalizedRecordLocatorV2(**tampered)  # type: ignore[arg-type]


def test_generic_append_cannot_fabricate_bounded_source_gap(tmp_path: Path) -> None:
    writer = _block_writer(tmp_path / "blocks")
    clock = _Clock()
    ledger = _ledger(tmp_path / "ledger", writer, clock=clock)
    _establish_source_session(ledger, clock)
    clock.wall_value = 50_002
    clock.monotonic_value = 60_002
    opened = _append_source_gap_open(ledger)
    bounded = _bound_source_gap(ledger, opened, clock=clock)

    with pytest.raises(
        CaptureIntegrityLedgerIntegrityError,
        match="ledger-owned endpoint resolution",
    ):
        ledger._append("SOURCE_GAP", bounded.payload)


def test_lowest_append_boundary_reverifies_bounded_source_gap_locator(
    tmp_path: Path,
) -> None:
    writer = _block_writer(tmp_path / "blocks")
    clock = _Clock()
    ledger = _ledger(tmp_path / "ledger", writer, clock=clock)
    _establish_source_session(ledger, clock)
    clock.wall_value = 50_002
    clock.monotonic_value = 60_002
    opened = _append_source_gap_open(ledger)
    bounded = _bound_source_gap(ledger, opened, clock=clock)
    tampered = json.loads(canonical_json_line(bounded.payload))
    assert isinstance(tampered, dict)
    right_locator = tampered["right_record_locator"]
    assert isinstance(right_locator, dict)
    right_locator["record_jsonl_sha256"] = "f" * 64
    right_locator.pop("locator_sha256")
    right_locator["locator_sha256"] = hashlib.sha256(
        LOCATOR_DOMAIN + canonical_json_line(right_locator)
    ).hexdigest()

    with ledger._writer_lease_operation():  # type: ignore[attr-defined]
        with ledger._lock, pytest.raises(  # type: ignore[attr-defined]
            CaptureIntegrityLedgerIntegrityError,
            match="right locator or cursor differs",
        ):
            ledger._append_unlocked_guarded(  # type: ignore[attr-defined]
                "SOURCE_GAP",
                tampered,
            )


def test_source_gap_current_assertion_rejects_void_appended_during_endpoint_scan(
    tmp_path: Path,
) -> None:
    writer = _block_writer(tmp_path / "blocks")
    manifest = _commit_one(writer)
    reference = attest_finalized_block_v2(writer, manifest)
    clock = _Clock()
    ledger = _ledger(tmp_path / "ledger", writer, clock=clock)
    _establish_source_session(ledger, clock)
    clock.wall_value = 50_002
    clock.monotonic_value = 60_002
    opened = _append_source_gap_open(ledger)
    bounded = _bound_source_gap(ledger, opened, clock=clock)

    other_clock = _Clock()
    other_clock.wall_value = 100_000
    other_clock.monotonic_value = 100_000
    other_ledger = _ledger(
        tmp_path / "ledger",
        writer,
        clock=other_clock,
    )
    original_read = ledger._read_source_gap_endpoints_unlocked
    data_path = writer.directory / manifest.data_file
    original_data = data_path.read_bytes()
    void_appended = False

    def read_then_append_void(
        ingest_seqs: tuple[int, ...],
    ) -> dict[int, tuple[RawRecordV2, FinalizedRecordLocatorV2]]:
        nonlocal void_appended
        endpoints = original_read(ingest_seqs)
        if not void_appended:
            void_appended = True
            data_path.write_bytes(original_data + b"corruption")
            try:
                other_ledger.append_void_for_finalized_block(
                    reference,
                    detector_component="source-gap-replay-race-test",
                    detection_evidence_sha256="7" * 64,
                )
            finally:
                data_path.write_bytes(original_data)
        return endpoints

    ledger._read_source_gap_endpoints_unlocked = read_then_append_void  # type: ignore[method-assign]
    with pytest.raises(
        CaptureIntegrityLedgerIntegrityError,
        match="current VOID evidence",
    ):
        ledger.assert_source_gap_bounded_current_v2(bounded)

    assert void_appended
    assert ledger.events[-1].event_type == "VOID"


def test_public_events_are_detached_from_ledger_owned_hash_chain(tmp_path: Path) -> None:
    writer = _block_writer(tmp_path / "blocks")
    ledger = _ledger(tmp_path / "ledger", writer)
    returned = _append_gap(ledger, 1, 1)
    original_sha256 = returned.sha256
    returned.payload["evidence_sha256"] = "f" * 64
    projected = ledger.events[0]
    projected.payload["evidence_sha256"] = "e" * 64

    second = _append_gap(ledger, 2, 2, evidence_sha256="2" * 64)
    assert second.previous_event_sha256 == original_sha256
    reopened = _ledger(tmp_path / "ledger", writer)
    assert reopened.events[0].payload["evidence_sha256"] == "1" * 64
    assert reopened.events[1].previous_event_sha256 == reopened.events[0].sha256


def test_source_gap_rejects_mutated_detached_open_reference(tmp_path: Path) -> None:
    writer = _block_writer(tmp_path / "blocks")
    clock = _Clock()
    ledger = _ledger(tmp_path / "ledger", writer, clock=clock)
    _establish_source_session(ledger, clock)
    clock.wall_value = 50_002
    clock.monotonic_value = 60_002
    opened = _append_source_gap_open(ledger)
    opened.payload["evidence_sha256"] = "f" * 64

    with pytest.raises(
        CaptureIntegrityLedgerIntegrityError,
        match="differs from this current ledger",
    ):
        _bound_source_gap(ledger, opened, clock=clock)


def test_event_payload_or_hash_chain_tamper_is_rejected_on_every_reopen(
    tmp_path: Path,
) -> None:
    writer = _block_writer(tmp_path / "blocks")
    ledger = _ledger(tmp_path / "ledger", writer)
    _append_gap(ledger, 1, 1)
    _append_gap(ledger, 2, 2, evidence_sha256="2" * 64)
    first_path = tmp_path / "ledger" / "integrity-event-00000001.json"
    raw = json.loads(first_path.read_bytes())
    raw["payload"]["evidence_sha256"] = "f" * 64
    first_path.write_bytes(canonical_json_line(raw))
    for _ in range(2):
        with pytest.raises(
            CaptureIntegrityLedgerIntegrityError,
            match="deterministic ID",
        ):
            _ledger(tmp_path / "ledger", writer)


def test_noncanonical_event_and_unknown_artifact_fail_closed(tmp_path: Path) -> None:
    writer = _block_writer(tmp_path / "blocks")
    ledger = _ledger(tmp_path / "ledger", writer)
    _append_gap(ledger, 1, 1)
    event_path = tmp_path / "ledger" / "integrity-event-00000001.json"
    raw = json.loads(event_path.read_bytes())
    event_path.write_text(json.dumps(raw, indent=2), encoding="utf-8")
    with pytest.raises(CaptureIntegrityLedgerIntegrityError, match="invalid integrity"):
        _ledger(tmp_path / "ledger", writer)

    separate = tmp_path / "unknown"
    _ledger(separate, writer)
    (separate / "integrity-event-surprise").write_bytes(b"x")
    with pytest.raises(CaptureIntegrityLedgerIntegrityError, match="unknown"):
        _ledger(separate, writer)


@pytest.mark.parametrize("fault_point", ["after_event_write", "after_event_fsync"])
def test_complete_partial_event_is_recovered_after_append_crash(
    tmp_path: Path,
    fault_point: str,
) -> None:
    writer = _block_writer(tmp_path / "blocks")

    def crash(point: str) -> None:
        if point == fault_point:
            raise OSError(f"synthetic {fault_point}")

    failed = _ledger(tmp_path / "ledger", writer, fault_hook=crash)
    with pytest.raises(OSError, match=fault_point):
        _append_gap(failed, 30, 31)
    with pytest.raises(CaptureIntegrityLedgerError, match="fault-latched"):
        _append_gap(failed, 30, 31)
    assert (tmp_path / "ledger" / "integrity-event-00000001.json.partial").exists()

    recovered = _ledger(tmp_path / "ledger", writer)
    assert len(recovered.events) == 1
    assert not (
        tmp_path / "ledger" / "integrity-event-00000001.json.partial"
    ).exists()
    assert _append_gap(recovered, 30, 31) == recovered.events[0]


def test_corrupt_partial_event_is_never_repaired_or_discarded(tmp_path: Path) -> None:
    writer = _block_writer(tmp_path / "blocks")

    def crash(point: str) -> None:
        if point == "after_event_write":
            raise OSError("synthetic partial crash")

    failed = _ledger(tmp_path / "ledger", writer, fault_hook=crash)
    with pytest.raises(OSError):
        _append_gap(failed, 1, 2)
    partial = tmp_path / "ledger" / "integrity-event-00000001.json.partial"
    corrupted = partial.read_bytes()[:-3]
    partial.write_bytes(corrupted)
    for _ in range(2):
        with pytest.raises(CaptureIntegrityLedgerIntegrityError, match="invalid integrity"):
            _ledger(tmp_path / "ledger", writer)
        assert partial.read_bytes() == corrupted


def test_final_file_is_replayed_after_crash_between_rename_and_parent_fsync(
    tmp_path: Path,
) -> None:
    writer = _block_writer(tmp_path / "blocks")

    def crash(point: str) -> None:
        if point == "after_event_rename":
            raise OSError("synthetic rename crash")

    failed = _ledger(tmp_path / "ledger", writer, fault_hook=crash)
    with pytest.raises(OSError, match="rename crash"):
        _append_gap(failed, 1, 1)
    assert (tmp_path / "ledger" / "integrity-event-00000001.json").exists()
    assert not (
        tmp_path / "ledger" / "integrity-event-00000001.json.partial"
    ).exists()
    assert len(_ledger(tmp_path / "ledger", writer).events) == 1


def test_ledger_and_block_root_binding_tamper_prevents_append(tmp_path: Path) -> None:
    writer = _block_writer(tmp_path / "blocks")
    ledger = _ledger(tmp_path / "ledger", writer)
    ledger_binding = tmp_path / "ledger" / "storage-root-binding.json"
    ledger_binding.write_bytes(b"{}\n")
    with pytest.raises(CaptureIntegrityLedgerIntegrityError, match="ledger root binding"):
        _append_gap(ledger, 1, 1)
    assert not list((tmp_path / "ledger").glob("integrity-event-*.json"))

    other_writer = _block_writer(tmp_path / "other-blocks")
    other_ledger = _ledger(tmp_path / "other-ledger", other_writer)
    block_binding = tmp_path / "other-blocks" / "storage-root-binding.json"
    block_binding.write_bytes(b"{}\n")
    with pytest.raises(CaptureIntegrityLedgerIntegrityError, match="block root binding"):
        _append_gap(other_ledger, 1, 1)
    assert not list((tmp_path / "other-ledger").glob("integrity-event-*.json"))


@pytest.mark.parametrize("binding_owner", ["ledger", "blocks"])
def test_source_gap_current_assertion_rechecks_root_bindings(
    tmp_path: Path,
    binding_owner: str,
) -> None:
    writer = _block_writer(tmp_path / "blocks")
    clock = _Clock()
    ledger = _ledger(tmp_path / "ledger", writer, clock=clock)
    _establish_source_session(ledger, clock)
    clock.wall_value = 50_002
    clock.monotonic_value = 60_002
    opened = _append_source_gap_open(ledger)
    bounded = _bound_source_gap(ledger, opened, clock=clock)
    binding_path = (
        tmp_path / "ledger" / "storage-root-binding.json"
        if binding_owner == "ledger"
        else writer.directory / "storage-root-binding.json"
    )
    original = binding_path.read_bytes()
    binding_path.write_bytes(b"{}\n")
    try:
        with pytest.raises(
            CaptureIntegrityLedgerIntegrityError,
            match="root binding",
        ):
            ledger.assert_source_gap_bounded_current_v2(bounded)
    finally:
        binding_path.write_bytes(original)


def test_sealed_event_count_and_root_contract_are_enforced(tmp_path: Path) -> None:
    writer = _block_writer(tmp_path / "blocks")
    ledger = _ledger(tmp_path / "ledger", writer, max_events=1)
    _append_gap(ledger, 1, 1)
    with pytest.raises(CaptureIntegrityLedgerCapacityError, match="count bound"):
        _append_gap(ledger, 2, 2, evidence_sha256="2" * 64)
    with pytest.raises(CaptureIntegrityLedgerIntegrityError, match="binding differs"):
        _ledger(tmp_path / "ledger", writer, max_events=2)


def test_void_records_data_corruption_without_repair_and_is_idempotent(
    tmp_path: Path,
) -> None:
    writer = _block_writer(tmp_path / "blocks")
    manifest = _commit_one(writer)
    reference = attest_finalized_block_v2(writer, manifest)
    ledger = _ledger(tmp_path / "ledger", writer)
    data_path = writer.directory / manifest.data_file
    corrupted = bytearray(data_path.read_bytes())
    corrupted[len(corrupted) // 2] ^= 1
    data_path.write_bytes(corrupted)
    before = data_path.read_bytes()

    event = ledger.append_void_for_finalized_block(
        reference,
        detector_component="signed-block-auditor",
        detection_evidence_sha256="8" * 64,
    )
    assert event.event_type == "VOID"
    assert event.payload["corruption_kinds"] == ["DATA_SHA256_MISMATCH"]
    assert data_path.read_bytes() == before
    with pytest.raises(BlockIntegrityError):
        verify_grouped_blocks(
            writer.directory,
            authority=writer.authority,
            policy=writer.policy,
            signing_authority=writer.signing_authority,
            stream_group_id=writer.stream_group_id,
            segment_id=writer.segment_id,
        )
    duplicate = ledger.append_void_for_finalized_block(
        reference,
        detector_component="signed-block-auditor",
        detection_evidence_sha256="8" * 64,
    )
    assert duplicate == event
    assert len(_ledger(tmp_path / "ledger", writer).events) == 1


@pytest.mark.parametrize(
    ("artifact", "expected_kind"),
    [
        ("data", "DATA_MISSING"),
        ("manifest", "MANIFEST_MISSING"),
    ],
)
def test_void_records_exact_missing_finalized_artifact(
    tmp_path: Path,
    artifact: str,
    expected_kind: str,
) -> None:
    writer = _block_writer(tmp_path / "blocks")
    manifest = _commit_one(writer)
    reference = attest_finalized_block_v2(writer, manifest)
    ledger = _ledger(tmp_path / "ledger", writer)
    target = (
        writer.directory / reference.data_file
        if artifact == "data"
        else writer.directory / reference.manifest_file
    )
    target.unlink()
    event = ledger.append_void_for_finalized_block(
        reference,
        detector_component="signed-block-auditor",
        detection_evidence_sha256="9" * 64,
    )
    assert event.payload["corruption_kinds"] == [expected_kind]
    assert not target.exists()


def test_void_rejects_intact_wrong_root_bad_signature_and_conflicting_evidence(
    tmp_path: Path,
) -> None:
    writer = _block_writer(tmp_path / "blocks")
    manifest = _commit_one(writer)
    reference = attest_finalized_block_v2(writer, manifest)
    ledger = _ledger(tmp_path / "ledger", writer)
    with pytest.raises(CaptureIntegrityLedgerIntegrityError, match="intact"):
        ledger.append_void_for_finalized_block(
            reference,
            detector_component="auditor",
            detection_evidence_sha256="1" * 64,
        )

    other_writer = _block_writer(tmp_path / "other-blocks")
    _commit_one(other_writer)
    other_ledger = _ledger(tmp_path / "other-ledger", other_writer)
    with pytest.raises(CaptureIntegrityLedgerIntegrityError, match=r"different.*root"):
        other_ledger.append_void_for_finalized_block(
            reference,
            detector_component="auditor",
            detection_evidence_sha256="1" * 64,
        )

    data_path = writer.directory / reference.data_file
    data_path.write_bytes(data_path.read_bytes() + b"x")
    bad_signature = replace(
        reference,
        writer_ed25519_signature=base64.b64encode(b"\x00" * 64).decode("ascii"),
    )
    with pytest.raises(CaptureIntegrityLedgerIntegrityError, match="signature"):
        ledger.append_void_for_finalized_block(
            bad_signature,
            detector_component="auditor",
            detection_evidence_sha256="1" * 64,
        )
    first = ledger.append_void_for_finalized_block(
        reference,
        detector_component="auditor",
        detection_evidence_sha256="1" * 64,
    )
    assert first.event_type == "VOID"
    with pytest.raises(CaptureIntegrityLedgerIntegrityError, match="different VOID"):
        ledger.append_void_for_finalized_block(
            reference,
            detector_component="auditor",
            detection_evidence_sha256="2" * 64,
        )


def test_finalized_reference_can_only_be_attested_while_chain_is_healthy(
    tmp_path: Path,
) -> None:
    writer = _block_writer(tmp_path / "blocks")
    manifest = _commit_one(writer)
    data_path = writer.directory / manifest.data_file
    data_path.write_bytes(data_path.read_bytes() + b"tamper")
    with pytest.raises(BlockIntegrityError):
        attest_finalized_block_v2(writer, manifest)


def test_clean_closure_seal_is_durable_factory_bound_and_restart_verifiable(
    tmp_path: Path,
) -> None:
    lease = _acquire_closure_lease(tmp_path)
    try:
        _, wal_writer, block_writer, ledger, finality = _closure_stack(
            tmp_path,
            lease,
        )
        _append_gap(ledger, 10, 10)
        persisted = _seal_closure(
            ledger,
            wal_writer,
            block_writer,
            finality,
        )
        assert persisted.seal.closure_status == "CLEAN"
        assert persisted.seal.event_count == 1
        assert persisted.seal.data_gap_count == 1
        assert persisted.seal.finality_tail_ingest_seq == 1
        assert (
            persisted.seal.block_clean_tail_terminal_sha256
            == block_writer.assert_clean_tail_terminal_and_current_v2(finality)
        )
        assert persisted.encoded_line == Path(persisted.canonical_path).read_bytes()
        builder = GroupedBlockBuilderV2(block_writer.policy)
        builder.offer(_queued(2), now_ns=10_000_001)
        later = builder.flush_tail(now_ns=10_000_002)
        assert later is not None
        with pytest.raises(
            BlockIntegrityError,
            match="irreversibly clean-tail terminal",
        ):
            block_writer.commit(later)
        assert (
            verify_persisted_capture_clean_closure_seal_receipt_v2(
                persisted,
                promoting_plans=PROMOTING_PLANS,
                ledger=ledger,
            )
            == persisted.seal_sha256
        )
        verification_block_writer = _block_writer(
            tmp_path / "blocks",
            verification_only=True,
        )
        restarted = _ledger(
            tmp_path / "ledger",
            verification_block_writer,
            writer_lease=lease,
        )
        restarted_receipt = restarted.verify_current_clean_closure_seal_v2(
            promoting_plans=PROMOTING_PLANS,
            wal_writer=wal_writer,
            block_writer=verification_block_writer,
            session_id="session-integrity",
            process_boot_id="boot-integrity",
        )
        assert restarted_receipt == persisted
        with pytest.raises(
            CaptureIntegrityLedgerIntegrityError,
            match="session or process boot",
        ):
            restarted.verify_current_clean_closure_seal_v2(
                promoting_plans=PROMOTING_PLANS,
                wal_writer=wal_writer,
                block_writer=verification_block_writer,
                session_id="session-integrity",
                process_boot_id="different-boot",
            )
        with pytest.raises(TypeError, match="durable ledger owner"):
            PersistedCaptureCleanClosureSealReceiptV2(
                seal=persisted.seal,
                canonical_path=persisted.canonical_path,
                file_name=persisted.file_name,
                seal_sha256=persisted.seal_sha256,
                byte_count=persisted.byte_count,
                file_device=persisted.file_device,
                file_inode=persisted.file_inode,
                file_nlink=persisted.file_nlink,
                _factory_token=object(),
            )
    finally:
        lease.release()


def test_clean_closure_receipt_hash_preserves_large_native_file_identity(
    tmp_path: Path,
) -> None:
    lease = _acquire_closure_lease(tmp_path)
    try:
        _, wal_writer, block_writer, ledger, finality = _closure_stack(
            tmp_path,
            lease,
        )
        persisted = _seal_closure(
            ledger,
            wal_writer,
            block_writer,
            finality,
        )
        large_native_identity = (1 << 63) + 17
        object.__setattr__(persisted, "file_device", large_native_identity)
        object.__setattr__(persisted, "file_inode", large_native_identity + 1)

        expected_document = {
            "schema_version": persisted.schema_version,
            "seal_sha256": persisted.seal_sha256,
            "canonical_path": persisted.canonical_path,
            "file_name": persisted.file_name,
            "byte_count": persisted.byte_count,
            "file_device": str(large_native_identity),
            "file_inode": str(large_native_identity + 1),
            "file_nlink": "1",
        }
        expected = hashlib.sha256(
            b"R4B_V2_PERSISTED_CAPTURE_CLEAN_CLOSURE_SEAL_RECEIPT\0"
            + canonical_json_line(expected_document)
        ).hexdigest()

        assert persisted.sha256 == expected
    finally:
        lease.release()


def test_clean_closure_fresh_read_only_owners_reprove_restart(
    tmp_path: Path,
) -> None:
    lease = _acquire_closure_lease(tmp_path)
    try:
        _, wal_writer, block_writer, ledger, finality = _closure_stack(
            tmp_path,
            lease,
        )
        persisted = _seal_closure(
            ledger,
            wal_writer,
            block_writer,
            finality,
        )
        verification_wal, verification_blocks = _verification_only_closure_owners(
            wal_writer,
            block_writer,
        )
        restarted_ledger = _ledger(
            tmp_path / "ledger",
            verification_blocks,
            writer_lease=lease,
        )

        assert verification_wal is not wal_writer
        assert verification_blocks is not block_writer
        assert restarted_ledger is not ledger
        assert verification_wal.verification_only is True
        assert (
            restarted_ledger.verify_current_clean_closure_seal_v2(
                promoting_plans=PROMOTING_PLANS,
                wal_writer=verification_wal,
                block_writer=verification_blocks,
                session_id="session-integrity",
                process_boot_id="boot-integrity",
            )
            == persisted
        )
    finally:
        lease.release()


def test_clean_closure_issuance_rejects_verification_only_wal(
    tmp_path: Path,
) -> None:
    lease = _acquire_closure_lease(tmp_path)
    try:
        _, wal_writer, block_writer, ledger, finality = _closure_stack(
            tmp_path,
            lease,
        )
        verification_wal = _verification_only_wal(wal_writer)

        with pytest.raises(
            CaptureIntegrityLedgerIntegrityError,
            match="issuance rejects a verification-only WAL",
        ):
            _seal_closure(
                ledger,
                verification_wal,
                block_writer,
                finality,
            )
        block_writer.assert_running_healthy_and_writer_open_v2()
    finally:
        lease.release()


def test_clean_closure_rejects_unmatched_source_gap_and_void_prefix(
    tmp_path: Path,
) -> None:
    gap_root = tmp_path / "gap"
    gap_root.mkdir()
    gap_lease = _acquire_closure_lease(gap_root)
    try:
        _, wal_writer, block_writer, ledger, finality = _closure_stack(
            gap_root,
            gap_lease,
        )
        clock = _Clock()
        ledger._wall_clock_ms = clock.wall_ms
        ledger._monotonic_clock_ns = clock.monotonic_ns
        _establish_source_session(ledger, clock, right_ingest_seq=1)
        clock.wall_value = 50_002
        clock.monotonic_value = 60_002
        _append_source_gap_open(ledger)
        with pytest.raises(
            CaptureIntegrityLedgerIntegrityError,
            match="unmatched SOURCE_GAP",
        ):
            _seal_closure(ledger, wal_writer, block_writer, finality)
    finally:
        gap_lease.release()

    void_root = tmp_path / "void"
    void_root.mkdir()
    void_lease = _acquire_closure_lease(void_root)
    try:
        _, wal_writer, block_writer, ledger, finality = _closure_stack(
            void_root,
            void_lease,
        )
        manifest = verify_grouped_blocks(
            block_writer.directory,
            authority=block_writer.authority,
            policy=block_writer.policy,
            signing_authority=block_writer.signing_authority,
            stream_group_id=block_writer.stream_group_id,
            segment_id=block_writer.segment_id,
        )[-1]
        reference = attest_finalized_block_v2(block_writer, manifest)
        data_path = block_writer.directory / manifest.data_file
        data_path.write_bytes(data_path.read_bytes() + b"corruption")
        ledger.append_void_for_finalized_block(
            reference,
            detector_component="signed-block-auditor",
            detection_evidence_sha256="8" * 64,
        )
        with pytest.raises(
            CaptureIntegrityLedgerIntegrityError,
            match="VOID-poisoned",
        ):
            _seal_closure(ledger, wal_writer, block_writer, finality)
    finally:
        void_lease.release()


def test_clean_closure_rejects_stale_finality_after_current_tail_extension(
    tmp_path: Path,
) -> None:
    lease = _acquire_closure_lease(tmp_path)
    try:
        durable, wal_writer, block_writer, ledger, stale_finality = _closure_stack(
            tmp_path,
            lease,
            close_after_finality=False,
        )
        assert durable.append_many((_queued(2),)) == 2
        durable.close()
        with pytest.raises(
            CaptureIntegrityLedgerIntegrityError,
            match="exact current storage tail",
        ):
            _seal_closure(
                ledger,
                wal_writer,
                block_writer,
                stale_finality,
            )
    finally:
        lease.release()


def test_clean_closure_blocks_all_later_appends_duplicate_and_unlink_reissue(
    tmp_path: Path,
) -> None:
    lease = _acquire_closure_lease(tmp_path)
    try:
        _, wal_writer, block_writer, ledger, finality = _closure_stack(
            tmp_path,
            lease,
        )
        persisted = _seal_closure(
            ledger,
            wal_writer,
            block_writer,
            finality,
        )
        with pytest.raises(CaptureIntegrityLedgerError, match="rejects append"):
            _append_gap(ledger, 2, 2)
        with pytest.raises(
            CaptureIntegrityLedgerIntegrityError,
            match="already consumed",
        ):
            _seal_closure(ledger, wal_writer, block_writer, finality)

        restarted = _ledger(
            tmp_path / "ledger",
            block_writer,
            writer_lease=lease,
        )
        with pytest.raises(CaptureIntegrityLedgerError, match="rejects append"):
            _append_gap(restarted, 2, 2)

        Path(persisted.canonical_path).unlink()
        with pytest.raises(
            CaptureIntegrityLedgerIntegrityError,
            match="removed",
        ):
            _seal_closure(ledger, wal_writer, block_writer, finality)
        with pytest.raises(CaptureIntegrityLedgerIntegrityError):
            verify_persisted_capture_clean_closure_seal_receipt_v2(
                persisted,
                promoting_plans=PROMOTING_PLANS,
                ledger=ledger,
            )
    finally:
        lease.release()


def test_clean_closure_lease_precedes_ledger_lock_against_late_append(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    lease = _acquire_closure_lease(tmp_path)
    append_attempted_lease = threading.Event()
    allow_append_lease = threading.Event()
    append_results: list[CaptureIntegrityEventV2] = []
    append_errors: list[Exception] = []
    late_append_thread: threading.Thread | None = None
    original_operation = CaptureIntegrityLedgerV2._writer_lease_operation

    @contextmanager
    def staged_operation(ledger: CaptureIntegrityLedgerV2) -> Iterator[None]:
        if threading.current_thread() is late_append_thread:
            append_attempted_lease.set()
            if not allow_append_lease.wait(timeout=5):
                raise AssertionError("late append was not released by the closure test")
        with original_operation(ledger):
            yield

    try:
        _, wal_writer, block_writer, ledger, finality = _closure_stack(
            tmp_path,
            lease,
        )
        monkeypatch.setattr(
            CaptureIntegrityLedgerV2,
            "_writer_lease_operation",
            staged_operation,
        )

        def append_late() -> None:
            try:
                append_results.append(_append_gap(ledger, 2, 2))
            except Exception as exc:
                append_errors.append(exc)

        late_append_thread = threading.Thread(
            target=append_late,
            name="late-integrity-ledger-append",
            daemon=True,
        )
        lock_was_available_to_closure = False
        persisted: PersistedCaptureCleanClosureSealReceiptV2 | None = None
        with lease.operation_guard():
            late_append_thread.start()
            assert append_attempted_lease.wait(timeout=5)
            lock_was_available_to_closure = ledger._lock.acquire(  # type: ignore[attr-defined]
                blocking=False
            )
            if lock_was_available_to_closure:
                ledger._lock.release()  # type: ignore[attr-defined]
                persisted = _seal_closure(
                    ledger,
                    wal_writer,
                    block_writer,
                    finality,
                )
            allow_append_lease.set()

        late_append_thread.join(timeout=5)
        assert not late_append_thread.is_alive()
        assert lock_was_available_to_closure, (
            "late append acquired the ledger lock before the writer lease"
        )
        assert persisted is not None
        assert append_results == []
        assert len(append_errors) == 1
        assert isinstance(append_errors[0], CaptureIntegrityLedgerError)
        assert "rejects append" in str(append_errors[0])
    finally:
        allow_append_lease.set()
        if late_append_thread is not None:
            late_append_thread.join(timeout=5)
        lease.release()


def test_clean_closure_complete_partial_is_recovered_after_restart(
    tmp_path: Path,
) -> None:
    def crash(point: str) -> None:
        if point == "after_clean_closure_seal_fsync":
            raise OSError("synthetic CLEAN seal crash")

    lease = _acquire_closure_lease(tmp_path)
    try:
        _, wal_writer, block_writer, ledger, finality = _closure_stack(
            tmp_path,
            lease,
            ledger_fault_hook=crash,
        )
        with pytest.raises(OSError, match="CLEAN seal crash"):
            _seal_closure(ledger, wal_writer, block_writer, finality)
        assert block_writer.assert_clean_tail_terminal_and_current_v2(finality)
        builder = GroupedBlockBuilderV2(block_writer.policy)
        builder.offer(_queued(2), now_ns=10_000_001)
        later = builder.flush_tail(now_ns=10_000_002)
        assert later is not None
        with pytest.raises(BlockIntegrityError, match="irreversibly clean-tail terminal"):
            block_writer.commit(later)
        partial = (
            tmp_path / "ledger" / "capture-clean-closure-seal.json.partial"
        )
        assert partial.is_file()
        restarted = _ledger(
            tmp_path / "ledger",
            block_writer,
            writer_lease=lease,
        )
        assert not partial.exists()
        persisted = restarted.verify_current_clean_closure_seal_v2(
            promoting_plans=PROMOTING_PLANS,
            wal_writer=wal_writer,
            block_writer=block_writer,
            session_id="session-integrity",
            process_boot_id="boot-integrity",
        )
        assert Path(persisted.canonical_path).is_file()
        with pytest.raises(CaptureIntegrityLedgerError, match="rejects append"):
            _append_gap(restarted, 2, 2)
    finally:
        lease.release()


def test_clean_closure_verifiers_reject_root_and_seal_file_replacement(
    tmp_path: Path,
) -> None:
    lease = _acquire_closure_lease(tmp_path)
    try:
        _, wal_writer, block_writer, ledger, finality = _closure_stack(
            tmp_path,
            lease,
        )
        persisted = _seal_closure(
            ledger,
            wal_writer,
            block_writer,
            finality,
        )
        binding_path = tmp_path / "ledger" / "storage-root-binding.json"
        binding_bytes = binding_path.read_bytes()
        binding_path.write_bytes(b"{}\n")
        with pytest.raises(
            CaptureIntegrityLedgerIntegrityError,
            match="root binding",
        ):
            ledger.verify_current_clean_closure_seal_v2(
                promoting_plans=PROMOTING_PLANS,
                wal_writer=wal_writer,
                block_writer=block_writer,
                session_id="session-integrity",
                process_boot_id="boot-integrity",
            )
        binding_path.write_bytes(binding_bytes)

        seal_path = Path(persisted.canonical_path)
        replacement = seal_path.with_name("replacement-seal.json")
        replacement.write_bytes(seal_path.read_bytes())
        os.replace(replacement, seal_path)
        with pytest.raises(
            CaptureIntegrityLedgerIntegrityError,
            match="identity changed",
        ):
            verify_persisted_capture_clean_closure_seal_receipt_v2(
                persisted,
                promoting_plans=PROMOTING_PLANS,
                ledger=ledger,
            )
    finally:
        lease.release()


def test_clean_closure_current_verifier_requires_retained_block_terminal(
    tmp_path: Path,
) -> None:
    lease = _acquire_closure_lease(tmp_path)
    try:
        _, wal_writer, block_writer, ledger, finality = _closure_stack(
            tmp_path,
            lease,
        )
        _seal_closure(ledger, wal_writer, block_writer, finality)
        terminal_path = block_writer.directory / "block-clean-tail-terminal.json"
        terminal_path.unlink()

        with pytest.raises(
            CaptureIntegrityLedgerIntegrityError,
            match="finality evidence failed current verification",
        ):
            ledger.verify_current_clean_closure_seal_v2(
                promoting_plans=PROMOTING_PLANS,
                wal_writer=wal_writer,
                block_writer=block_writer,
                session_id="session-integrity",
                process_boot_id="boot-integrity",
            )
    finally:
        lease.release()


def _depth_bridge_cycle() -> DepthBridgeCycleRefV8:
    return build_depth_bridge_cycle_ref_v8(
        session_id="session-integrity",
        protocol_hash=HASH,
        plan_bundle_sha256=PROMOTING_PLAN_SHA256_V8,
        depth_plan_sha256=public_depth_rest_plan_sha256_v8(DEPTH_PLAN_V8),
        connection_id="connection-depth-1",
        connection_generation=1,
        symbol="BTCUSDT",
        symbol_ordinal=0,
        trigger_seq=1,
        first_buffered_u=100,
    )


def _depth_bridge_ws_source() -> DepthBridgeWebSocketSourceLocatorV8:
    return DepthBridgeWebSocketSourceLocatorV8(
        symbol="BTCUSDT",
        frame_seq=1,
        ingest_seq=2,
        raw_payload_sha256="1" * 64,
        receipt_wall_ms=1_000,
        receipt_monotonic_ns=2_000,
        first_update_id=100,
        final_update_id=101,
        reset=False,
    )


def _depth_bridge_payload(
    phase: DepthBridgePhaseV8,
    material: object,
) -> DepthBridgeEvidencePayloadV8:
    return build_depth_bridge_evidence_payload_v8(
        phase=phase,
        session_id="session-integrity",
        protocol_hash=HASH,
        connection_id="connection-depth-1",
        connection_generation=1,
        material=material,  # type: ignore[arg-type]
        promoting_plans=PROMOTING_PLANS_V8,
        depth_plan=DEPTH_PLAN_V8,
    )


def _depth_bridge_lifecycle() -> tuple[DepthBridgeEvidencePayloadV8, ...]:
    cycle = _depth_bridge_cycle()
    ws_source = _depth_bridge_ws_source()
    range_summary = build_depth_bridge_range_summary_v8((ws_source,))
    semantic_sha256 = "2" * 64
    return (
        _depth_bridge_payload(
            DepthBridgePhaseV8.GENERATION_STARTED,
            DepthBridgeGenerationStartedV8(
                symbol_count=1,
                symbol_census_sha256=depth_bridge_symbol_census_sha256_v8(
                    DEPTH_PLAN_V8.symbols
                ),
                maximum_concurrency=DEPTH_PLAN_V8.maximum_concurrency,
                maximum_buffered_ranges_per_symbol=(
                    DEPTH_BRIDGE_MAXIMUM_BUFFERED_RANGES_PER_SYMBOL_V8
                ),
                bridge_maximum_attempts=DEPTH_PLAN_V8.bridge_maximum_attempts,
                bridge_wait_timeout_ms=DEPTH_PLAN_V8.bridge_wait_timeout_ms,
            ),
        ),
        _depth_bridge_payload(
            DepthBridgePhaseV8.TRIGGER_REGISTERED,
            DepthBridgeTriggerRegisteredV8(
                trigger="startup",
                trigger_seq=1,
                cycles=(
                    DepthBridgeRegisteredCycleV8(
                        cycle=cycle,
                        initial_range_source=ws_source,
                        supersedes_cycle_id=None,
                    ),
                ),
            ),
        ),
        _depth_bridge_payload(
            DepthBridgePhaseV8.ATTEMPT_STARTED,
            DepthBridgeAttemptStartedV8(
                cycle=cycle,
                bridge_attempt=1,
            ),
        ),
        _depth_bridge_payload(
            DepthBridgePhaseV8.ATTEMPT_TERMINAL,
            DepthBridgeAttemptTerminalV8(
                cycle=cycle,
                bridge_attempt=1,
                classification="accepted",
                rest_source=DepthBridgeRestSourceLocatorV8(
                    symbol="BTCUSDT",
                    trigger_seq=1,
                    first_buffered_u=100,
                    bridge_attempt=1,
                    ingest_seq=3,
                    raw_record_sha256="3" * 64,
                    attempt_payload_sha256="4" * 64,
                    receipt_wall_ms=1_001,
                    receipt_monotonic_ns=2_001,
                ),
                semantic_admission_sha256=semantic_sha256,
                last_update_id=100,
                target_update_id=100,
                discarded_range_count=0,
                range_summary=range_summary,
                failure_code=None,
                wait_started_monotonic_ns=None,
                wait_deadline_monotonic_ns=None,
            ),
        ),
        _depth_bridge_payload(
            DepthBridgePhaseV8.CYCLE_TERMINAL,
            DepthBridgeCycleTerminalV8(
                cycle=cycle,
                outcome=DepthBridgeCycleOutcomeV8.ACCEPTED.value,
                reason="snapshot_range_bridge",
                terminal_bridge_attempt=1,
                semantic_admission_sha256=semantic_sha256,
                target_update_id=100,
                bridging_range_summary=range_summary,
            ),
        ),
        _depth_bridge_payload(
            DepthBridgePhaseV8.GENERATION_DRAINED,
            DepthBridgeGenerationDrainedV8(
                reason="normal_stop",
                fatal_cause_code=None,
                fatal_cause_sha256=None,
                registered_cycle_count=1,
                accepted_cycle_count=1,
                superseded_cycle_count=0,
                failed_cycle_count=0,
                worker_count=0,
                permit_in_use_count=0,
                retained_registration_count=0,
                pending_registration_count=0,
                retained_token_count=0,
                claimed_token_count=0,
                adapter_active_attempt_count=0,
                adapter_pending_owner_task_count=0,
                retained_terminal_admission_count=0,
                adapter_closed=True,
                adapter_cleanly_closed=True,
            ),
        ),
    )


def _depth_bridge_failed_lifecycle() -> tuple[DepthBridgeEvidencePayloadV8, ...]:
    start, trigger, attempt_start, *_ = _depth_bridge_lifecycle()
    cycle = _depth_bridge_cycle()
    range_summary = build_depth_bridge_range_summary_v8(
        (_depth_bridge_ws_source(),)
    )
    rest_source = DepthBridgeRestSourceLocatorV8(
        symbol="BTCUSDT",
        trigger_seq=1,
        first_buffered_u=100,
        bridge_attempt=1,
        ingest_seq=3,
        raw_record_sha256="3" * 64,
        attempt_payload_sha256="4" * 64,
        receipt_wall_ms=1_001,
        receipt_monotonic_ns=2_001,
    )
    failed_attempt = _depth_bridge_payload(
        DepthBridgePhaseV8.ATTEMPT_TERMINAL,
        DepthBridgeAttemptTerminalV8(
            cycle=cycle,
            bridge_attempt=1,
            classification="failed",
            rest_source=rest_source,
            semantic_admission_sha256=None,
            last_update_id=None,
            target_update_id=None,
            discarded_range_count=0,
            range_summary=range_summary,
            failure_code="http_terminal",
            wait_started_monotonic_ns=None,
            wait_deadline_monotonic_ns=None,
        ),
    )
    failed_cycle = _depth_bridge_payload(
        DepthBridgePhaseV8.CYCLE_TERMINAL,
        DepthBridgeCycleTerminalV8(
            cycle=cycle,
            outcome="failed",
            reason="http_terminal",
            terminal_bridge_attempt=1,
            semantic_admission_sha256=None,
            target_update_id=None,
            bridging_range_summary=None,
        ),
    )
    failed_drain = _depth_bridge_payload(
        DepthBridgePhaseV8.GENERATION_DRAINED,
        replace(
            _depth_bridge_lifecycle()[-1].material,
            accepted_cycle_count=0,
            failed_cycle_count=1,
        ),
    )
    return start, trigger, attempt_start, failed_attempt, failed_cycle, failed_drain


def _depth_bridge_ledger(
    root: Path,
    *,
    max_events: int = MAX_EVENTS,
    maximum_total_bytes: int = MAXIMUM_BYTES,
) -> tuple[GroupedBlockWriterV2, CaptureIntegrityLedgerV2]:
    authority = _authority(PROMOTING_PLAN_SHA256_V8)
    writer = _block_writer(root / "blocks", authority=authority)
    ledger = _ledger(
        root / "ledger",
        writer,
        max_events=max_events,
        maximum_total_bytes=maximum_total_bytes,
        authority=authority,
    )
    return writer, ledger


def _append_depth_bridge_lifecycle(
    ledger: CaptureIntegrityLedgerV2,
) -> tuple[CaptureIntegrityEventV2, ...]:
    return tuple(
        ledger.append_depth_bridge_v8(
            payload,
            PROMOTING_PLANS_V8,
            DEPTH_PLAN_V8,
        )
        for payload in _depth_bridge_lifecycle()
    )


def _test_v8_bridge_receipt(
    event: CaptureIntegrityEventV2,
    *,
    connection_id: str = "connection-depth-1",
    generation: int = 1,
    last_event_sequence: int | None = None,
    last_event_sha256: str | None = None,
    last_recorded_wall_ms: int | None = None,
    last_recorded_monotonic_ns: int | None = None,
    close_wall_ms: int = 90_000,
    close_monotonic_ns: int = 9_000_000,
):  # type: ignore[no-untyped-def]
    return _issue_depth_bridge_coordinator_clean_close_receipt_v8(
        session_id="session-integrity",
        protocol_hash=HASH,
        promoting_plans=PROMOTING_PLANS_V8,
        depth_plan=DEPTH_PLAN_V8,
        last_connection_id=connection_id,
        last_connection_generation=generation,
        generation_started_count=1,
        generation_drained_count=1,
        fatal_generation_count=0,
        last_generation_drained_event_sequence=(
            event.event_sequence
            if last_event_sequence is None
            else last_event_sequence
        ),
        last_generation_drained_event_sha256=(
            event.sha256 if last_event_sha256 is None else last_event_sha256
        ),
        last_generation_drained_recorded_wall_ms=(
            event.recorded_wall_ms
            if last_recorded_wall_ms is None
            else last_recorded_wall_ms
        ),
        last_generation_drained_recorded_monotonic_ns=(
            event.recorded_monotonic_ns
            if last_recorded_monotonic_ns is None
            else last_recorded_monotonic_ns
        ),
        close_wall_ms=close_wall_ms,
        close_monotonic_ns=close_monotonic_ns,
    )


def _test_v8_websocket_cursors(
    finality_receipt,  # type: ignore[no-untyped-def]
    *,
    public_connection_id: str = "connection-depth-1",
    public_generation: int = 1,
):  # type: ignore[no-untyped-def]
    websocket_plans = tuple(
        plan
        for plan in PROMOTING_PLANS_V8
        if type(plan) is ProvisionalPromotingCapturePlanV2
    )
    assert tuple(plan.route_id for plan in websocket_plans) == (
        "usdm_market",
        "usdm_public",
    )
    stop_receipts = tuple(
        _issue_websocket_route_stop_receipt_v8(
            PROMOTING_PLANS_V8,
            plan,
            session_id="session-integrity",
            process_boot_id="boot-integrity",
            connection_id=(
                public_connection_id
                if plan.route_id == "usdm_public"
                else "connection-market-1"
            ),
            generation=(
                public_generation if plan.route_id == "usdm_public" else 1
            ),
            last_frame_seq=1,
            last_ingest_seq=1,
            last_receipt_wall_ms=1_001,
            last_receipt_monotonic_ns=1_000_001,
            stop_observed=ReceiptTimestamp(
                received_at_ms=80_000,
                received_monotonic_ns=8_000_000,
            ),
        )
        for plan in websocket_plans
    )
    return finalize_websocket_route_cursor_pair_v8(
        stop_receipts,  # type: ignore[arg-type]
        finality_receipt=finality_receipt,
        promoting_plans=PROMOTING_PLANS_V8,
    )


def _test_v2_websocket_cursors(finality_receipt):  # type: ignore[no-untyped-def]
    websocket_plans = tuple(
        plan
        for plan in PROMOTING_PLANS
        if type(plan) is ProvisionalPromotingCapturePlanV2
    )
    stop_receipts = tuple(
        _issue_websocket_route_stop_receipt_v2(
            PROMOTING_PLANS,
            plan,
            session_id="session-integrity",
            process_boot_id="boot-integrity",
            connection_id=f"connection-{plan.route_id}",
            generation=1,
            last_frame_seq=1,
            last_ingest_seq=1,
            last_receipt_wall_ms=1_001,
            last_receipt_monotonic_ns=1_000_001,
            stop_observed=ReceiptTimestamp(
                received_at_ms=80_000,
                received_monotonic_ns=8_000_000,
            ),
        )
        for plan in websocket_plans
    )
    return finalize_websocket_route_cursor_pair_v2(
        stop_receipts,  # type: ignore[arg-type]
        finality_receipt=finality_receipt,
        promoting_plans=PROMOTING_PLANS,
    )


def _v8_closure_stack(
    root: Path,
    lease: WriterLease,
    *,
    ledger_fault_hook: object | None = None,
    lifecycle: tuple[DepthBridgeEvidencePayloadV8, ...] | None = None,
):  # type: ignore[no-untyped-def]
    authority = _authority(PROMOTING_PLAN_SHA256_V8)
    _, wal_writer, block_writer, ledger, finality = _closure_stack(
        root,
        lease,
        ledger_fault_hook=ledger_fault_hook,
        authority=authority,
    )
    payloads = lifecycle or _depth_bridge_lifecycle()
    events = tuple(
        ledger.append_depth_bridge_v8(
            payload,
            PROMOTING_PLANS_V8,
            DEPTH_PLAN_V8,
        )
        for payload in payloads
    )
    last_drain = events[-1]
    bridge_receipt = _test_v8_bridge_receipt(last_drain)
    cursors = _test_v8_websocket_cursors(finality)
    return (
        wal_writer,
        block_writer,
        ledger,
        finality,
        bridge_receipt,
        cursors,
    )


def _seal_closure_v8(
    ledger: CaptureIntegrityLedgerV2,
    wal_writer: MirroredWalWriterV2,
    block_writer: GroupedBlockWriterV2,
    finality_receipt,  # type: ignore[no-untyped-def]
    bridge_receipt,  # type: ignore[no-untyped-def]
    cursors,  # type: ignore[no-untyped-def]
    *,
    promoting_plans=PROMOTING_PLANS_V8,  # type: ignore[no-untyped-def]
    depth_plan=DEPTH_PLAN_V8,  # type: ignore[no-untyped-def]
    seal_wall_ms: int = 100_000,
    seal_monotonic_ns: int = 10_000_000,
):  # type: ignore[no-untyped-def]
    return ledger.seal_clean_closure_v8(
        promoting_plans=promoting_plans,
        depth_plan=depth_plan,
        depth_bridge_close_receipt=bridge_receipt,
        finalized_websocket_cursor_pair=cursors,
        finality_receipt=finality_receipt,
        wal_writer=wal_writer,
        block_writer=block_writer,
        session_id="session-integrity",
        process_boot_id="boot-integrity",
        seal_wall_ms=seal_wall_ms,
        seal_monotonic_ns=seal_monotonic_ns,
    )


def test_v8_clean_closure_is_durable_factory_bound_and_restart_verifiable(
    tmp_path: Path,
) -> None:
    lease = _acquire_closure_lease(tmp_path)
    try:
        (
            wal_writer,
            block_writer,
            ledger,
            finality,
            bridge_receipt,
            cursors,
        ) = _v8_closure_stack(tmp_path, lease)
        persisted = _seal_closure_v8(
            ledger,
            wal_writer,
            block_writer,
            finality,
            bridge_receipt,
            cursors,
        )

        assert type(persisted) is PersistedCaptureCleanClosureSealReceiptV8
        assert type(persisted.seal) is CaptureCleanClosureSealV8
        assert persisted.file_name == "capture-clean-closure-seal.json"
        assert persisted.seal.event_count == 6
        assert persisted.seal.depth_bridge_event_count == 6
        assert persisted.seal.depth_bridge_generation_started_count == 1
        assert persisted.seal.depth_bridge_generation_drained_count == 1
        assert persisted.seal.depth_bridge_fatal_generation_count == 0
        assert persisted.seal.depth_bridge_open_generation_count == 0
        assert persisted.seal.depth_bridge_failed_cycle_count == 0
        assert (
            persisted.seal.qualification_complete_claimed,
            persisted.seal.promoting,
            persisted.seal.book_bridge_certified,
            persisted.seal.m2_certified,
            persisted.seal.order_execution_enabled,
        ) == (False,) * 5
        assert Path(persisted.canonical_path).read_bytes() == persisted.encoded_line
        assert (
            verify_persisted_capture_clean_closure_seal_receipt_v8(
                persisted,
                promoting_plans=PROMOTING_PLANS_V8,
                depth_plan=DEPTH_PLAN_V8,
                ledger=ledger,
            )
            == persisted.seal_sha256
        )

        verification_wal, verification_blocks = _verification_only_closure_owners(
            wal_writer,
            block_writer,
        )
        restarted = _ledger(
            tmp_path / "ledger",
            verification_blocks,
            writer_lease=lease,
            authority=_authority(PROMOTING_PLAN_SHA256_V8),
        )
        restarted_receipt = restarted.verify_current_clean_closure_seal_v8(
            promoting_plans=PROMOTING_PLANS_V8,
            depth_plan=DEPTH_PLAN_V8,
            wal_writer=verification_wal,
            block_writer=verification_blocks,
            session_id="session-integrity",
            process_boot_id="boot-integrity",
        )
        assert restarted_receipt == persisted
        with pytest.raises(TypeError, match="durable ledger owner"):
            PersistedCaptureCleanClosureSealReceiptV8(
                seal=persisted.seal,
                canonical_path=persisted.canonical_path,
                file_name=persisted.file_name,
                seal_sha256=persisted.seal_sha256,
                byte_count=persisted.byte_count,
                file_device=persisted.file_device,
                file_inode=persisted.file_inode,
                file_nlink=persisted.file_nlink,
                _factory_token=object(),
            )
    finally:
        lease.release()


def test_v2_v8_clean_closure_exact_dispatch_rejects_downgrade_before_terminal(
    tmp_path: Path,
) -> None:
    v8_root = tmp_path / "v8"
    v8_root.mkdir()
    v8_lease = _acquire_closure_lease(v8_root)
    try:
        (
            wal_writer,
            block_writer,
            ledger,
            finality,
            bridge_receipt,
            cursors,
        ) = _v8_closure_stack(v8_root, v8_lease)
        with pytest.raises(TypeError, match="rejects non-exact plan members"):
            ledger.seal_clean_closure_v2(
                promoting_plans=PROMOTING_PLANS_V8,  # type: ignore[arg-type]
                finality_receipt=finality,
                wal_writer=wal_writer,
                block_writer=block_writer,
                session_id="session-integrity",
                process_boot_id="boot-integrity",
                seal_wall_ms=100_000,
                seal_monotonic_ns=10_000_000,
            )
        block_writer.assert_running_healthy_and_writer_open_v2()
        persisted_v8 = _seal_closure_v8(
            ledger,
            wal_writer,
            block_writer,
            finality,
            bridge_receipt,
            cursors,
        )
        with pytest.raises(TypeError, match=r"exact Persisted.*V2"):
            verify_persisted_capture_clean_closure_seal_receipt_v2(
                persisted_v8,  # type: ignore[arg-type]
                promoting_plans=PROMOTING_PLANS,
                ledger=ledger,
            )
        with pytest.raises(
            CaptureIntegrityLedgerIntegrityError,
            match="plan authority differs",
        ):
            ledger.verify_current_clean_closure_seal_v2(
                promoting_plans=PROMOTING_PLANS,
                wal_writer=wal_writer,
                block_writer=block_writer,
                session_id="session-integrity",
                process_boot_id="boot-integrity",
            )
    finally:
        v8_lease.release()

    v2_root = tmp_path / "v2"
    v2_root.mkdir()
    v2_lease = _acquire_closure_lease(v2_root)
    try:
        _, wal_writer, block_writer, ledger, finality = _closure_stack(
            v2_root,
            v2_lease,
        )
        foreign_root = tmp_path / "foreign-v8"
        foreign_root.mkdir()
        foreign_lease = _acquire_closure_lease(foreign_root)
        try:
            *_, bridge_receipt, cursors = _v8_closure_stack(
                foreign_root,
                foreign_lease,
            )
            with pytest.raises(
                CaptureIntegrityLedgerIntegrityError,
                match="plan/depth authority differs",
            ):
                ledger.seal_clean_closure_v8(
                    promoting_plans=PROMOTING_PLANS_V8,
                    depth_plan=DEPTH_PLAN_V8,
                    depth_bridge_close_receipt=bridge_receipt,
                    finalized_websocket_cursor_pair=cursors,
                    finality_receipt=finality,
                    wal_writer=wal_writer,
                    block_writer=block_writer,
                    session_id="session-integrity",
                    process_boot_id="boot-integrity",
                    seal_wall_ms=100_000,
                    seal_monotonic_ns=10_000_000,
                )
            block_writer.assert_running_healthy_and_writer_open_v2()
        finally:
            foreign_lease.release()
        persisted_v2 = _seal_closure(
            ledger,
            wal_writer,
            block_writer,
            finality,
        )
        with pytest.raises(TypeError, match=r"exact Persisted.*V8"):
            verify_persisted_capture_clean_closure_seal_receipt_v8(
                persisted_v2,  # type: ignore[arg-type]
                promoting_plans=PROMOTING_PLANS_V8,
                depth_plan=DEPTH_PLAN_V8,
                ledger=ledger,
            )
    finally:
        v2_lease.release()


@pytest.mark.parametrize("bridge_state", ["none", "open", "reconnect", "fatal"])
def test_v8_clean_closure_rejects_nonclean_bridge_census_before_terminal(
    tmp_path: Path,
    bridge_state: str,
) -> None:
    lease = _acquire_closure_lease(tmp_path)
    try:
        authority = _authority(PROMOTING_PLAN_SHA256_V8)
        _, wal_writer, block_writer, ledger, finality = _closure_stack(
            tmp_path,
            lease,
            authority=authority,
        )
        start = _depth_bridge_lifecycle()[0]
        clean_drain = replace(
            _depth_bridge_lifecycle()[-1],
            material=replace(
                _depth_bridge_lifecycle()[-1].material,
                registered_cycle_count=0,
                accepted_cycle_count=0,
            ),
        )
        payloads: tuple[DepthBridgeEvidencePayloadV8, ...]
        if bridge_state == "none":
            payloads = ()
        elif bridge_state == "open":
            payloads = (start,)
        elif bridge_state == "reconnect":
            payloads = (
                start,
                replace(
                    clean_drain,
                    material=replace(clean_drain.material, reason="reconnect"),
                ),
            )
        else:
            payloads = (
                start,
                replace(
                    clean_drain,
                    material=replace(
                        clean_drain.material,
                        reason="fatal",
                        fatal_cause_code="coordinator_failure",
                        fatal_cause_sha256="9" * 64,
                        adapter_cleanly_closed=False,
                    ),
                ),
            )
        events = tuple(
            ledger.append_depth_bridge_v8(
                payload,
                PROMOTING_PLANS_V8,
                DEPTH_PLAN_V8,
            )
            for payload in payloads
        )
        if events:
            locator_event = events[-1]
        else:
            material = json.loads(
                canonical_json_line(asdict(_depth_bridge_lifecycle()[-1]))
            )
            assert isinstance(material, dict)
            locator_event = CaptureIntegrityEventV2(
                event_sequence=1,
                previous_event_sha256=None,
                event_id="1" * 64,
                event_type="DEPTH_BRIDGE",
                authority_sha256="2" * 64,
                ledger_root_binding_sha256="3" * 64,
                block_root_binding_sha256="4" * 64,
                block_root_path_sha256="5" * 64,
                recorded_wall_ms=50_002,
                recorded_monotonic_ns=60_002,
                payload=material,
            )
        bridge_receipt = _test_v8_bridge_receipt(locator_event)
        cursors = _test_v8_websocket_cursors(finality)
        with pytest.raises(
            CaptureIntegrityLedgerIntegrityError,
            match="fully drained nonfatal depth bridge",
        ):
            _seal_closure_v8(
                ledger,
                wal_writer,
                block_writer,
                finality,
                bridge_receipt,
                cursors,
            )
        block_writer.assert_running_healthy_and_writer_open_v2()
    finally:
        lease.release()


@pytest.mark.parametrize(
    ("public_connection_id", "public_generation"),
    [("foreign-public", 1), ("connection-depth-1", 2)],
)
def test_v8_clean_closure_rejects_public_cursor_bridge_lineage_mismatch(
    tmp_path: Path,
    public_connection_id: str,
    public_generation: int,
) -> None:
    lease = _acquire_closure_lease(tmp_path)
    try:
        (
            wal_writer,
            block_writer,
            ledger,
            finality,
            bridge_receipt,
            _,
        ) = _v8_closure_stack(tmp_path, lease)
        cursors = _test_v8_websocket_cursors(
            finality,
            public_connection_id=public_connection_id,
            public_generation=public_generation,
        )
        with pytest.raises(
            CaptureIntegrityLedgerIntegrityError,
            match="bridge/public cursor lineage differs",
        ):
            _seal_closure_v8(
                ledger,
                wal_writer,
                block_writer,
                finality,
                bridge_receipt,
                cursors,
            )
        block_writer.assert_running_healthy_and_writer_open_v2()
    finally:
        lease.release()


@pytest.mark.parametrize("authority_case", ["reordered", "cloned_depth", "foreign"])
def test_v8_clean_closure_rejects_nonexact_plan_authority_before_terminal(
    tmp_path: Path,
    authority_case: str,
) -> None:
    lease = _acquire_closure_lease(tmp_path)
    try:
        (
            wal_writer,
            block_writer,
            ledger,
            finality,
            bridge_receipt,
            cursors,
        ) = _v8_closure_stack(tmp_path, lease)
        plans = PROMOTING_PLANS_V8
        depth_plan = DEPTH_PLAN_V8
        if authority_case == "reordered":
            plans = (
                PROMOTING_PLANS_V8[1],
                PROMOTING_PLANS_V8[0],
                PROMOTING_PLANS_V8[2],
                PROMOTING_PLANS_V8[3],
            )
        elif authority_case == "cloned_depth":
            depth_plan = replace(DEPTH_PLAN_V8)
        else:
            plans = build_provisional_promoting_capture_plans_v8(("ETHUSDT",))
            depth_plan = next(
                plan
                for plan in plans
                if type(plan) is ProvisionalDepthRestQualificationPlanV8
            )
            assert isinstance(depth_plan, ProvisionalDepthRestQualificationPlanV8)

        with pytest.raises(
            CaptureIntegrityLedgerIntegrityError,
            match="plan/depth authority differs",
        ):
            _seal_closure_v8(
                ledger,
                wal_writer,
                block_writer,
                finality,
                bridge_receipt,
                cursors,
                promoting_plans=plans,
                depth_plan=depth_plan,
            )
        assert not (
            block_writer.directory / "block-clean-tail-terminal.json"
        ).exists()
        block_writer.assert_running_healthy_and_writer_open_v2()
    finally:
        lease.release()


def test_v8_clean_closure_rejects_bridge_clone_tamper_and_v2_cursors(
    tmp_path: Path,
) -> None:
    lease = _acquire_closure_lease(tmp_path)
    try:
        (
            wal_writer,
            block_writer,
            ledger,
            finality,
            bridge_receipt,
            cursors,
        ) = _v8_closure_stack(tmp_path, lease)
        with pytest.raises(TypeError, match="factory-sealed"):
            replace(bridge_receipt)

        original_digest = bridge_receipt.receipt_sha256
        object.__setattr__(bridge_receipt, "receipt_sha256", "f" * 64)
        with pytest.raises(DepthBridgeEvidenceErrorV8, match="digest changed"):
            _seal_closure_v8(
                ledger,
                wal_writer,
                block_writer,
                finality,
                bridge_receipt,
                cursors,
            )
        object.__setattr__(bridge_receipt, "receipt_sha256", original_digest)

        v2_cursors = _test_v2_websocket_cursors(finality)
        with pytest.raises(TypeError, match="foreign type"):
            _seal_closure_v8(
                ledger,
                wal_writer,
                block_writer,
                finality,
                bridge_receipt,
                v2_cursors,
            )
        assert not (
            block_writer.directory / "block-clean-tail-terminal.json"
        ).exists()
        block_writer.assert_running_healthy_and_writer_open_v2()
    finally:
        lease.release()


@pytest.mark.parametrize("locator_field", ["sequence", "sha256", "wall", "monotonic"])
def test_v8_clean_closure_rejects_valid_foreign_last_drain_locator(
    tmp_path: Path,
    locator_field: str,
) -> None:
    lease = _acquire_closure_lease(tmp_path)
    try:
        (
            wal_writer,
            block_writer,
            ledger,
            finality,
            _,
            cursors,
        ) = _v8_closure_stack(tmp_path, lease)
        last_drain = ledger.events[-1]
        overrides: dict[str, object] = {}
        if locator_field == "sequence":
            overrides["last_event_sequence"] = last_drain.event_sequence + 1
        elif locator_field == "sha256":
            overrides["last_event_sha256"] = "f" * 64
        elif locator_field == "wall":
            overrides["last_recorded_wall_ms"] = last_drain.recorded_wall_ms + 1
        else:
            overrides["last_recorded_monotonic_ns"] = (
                last_drain.recorded_monotonic_ns + 1
            )
        bridge_receipt = _test_v8_bridge_receipt(
            last_drain,
            **overrides,  # type: ignore[arg-type]
        )

        with pytest.raises(
            CaptureIntegrityLedgerIntegrityError,
            match="differs from the last durable drain",
        ):
            _seal_closure_v8(
                ledger,
                wal_writer,
                block_writer,
                finality,
                bridge_receipt,
                cursors,
            )
        assert not (
            block_writer.directory / "block-clean-tail-terminal.json"
        ).exists()
    finally:
        lease.release()


@pytest.mark.parametrize("clock_case", ["bridge_after_fence", "seal_wall", "seal_monotonic"])
def test_v8_clean_closure_rejects_misordered_clocks_before_terminal(
    tmp_path: Path,
    clock_case: str,
) -> None:
    lease = _acquire_closure_lease(tmp_path)
    try:
        (
            wal_writer,
            block_writer,
            ledger,
            finality,
            bridge_receipt,
            cursors,
        ) = _v8_closure_stack(tmp_path, lease)
        seal_wall_ms = 100_000
        seal_monotonic_ns = 10_000_000
        if clock_case == "bridge_after_fence":
            bridge_receipt = _test_v8_bridge_receipt(
                ledger.events[-1],
                close_monotonic_ns=10_000_001,
            )
            seal_monotonic_ns = 10_000_001
        elif clock_case == "seal_wall":
            seal_wall_ms = 89_999
        else:
            seal_monotonic_ns = 9_999_999

        with pytest.raises(
            CaptureIntegrityLedgerIntegrityError,
            match="clocks are misordered",
        ):
            _seal_closure_v8(
                ledger,
                wal_writer,
                block_writer,
                finality,
                bridge_receipt,
                cursors,
                seal_wall_ms=seal_wall_ms,
                seal_monotonic_ns=seal_monotonic_ns,
            )
        assert not (
            block_writer.directory / "block-clean-tail-terminal.json"
        ).exists()
    finally:
        lease.release()


def test_v8_clean_closure_allows_failed_cycle_without_strategy_authority(
    tmp_path: Path,
) -> None:
    lease = _acquire_closure_lease(tmp_path)
    try:
        (
            wal_writer,
            block_writer,
            ledger,
            finality,
            bridge_receipt,
            cursors,
        ) = _v8_closure_stack(
            tmp_path,
            lease,
            lifecycle=_depth_bridge_failed_lifecycle(),
        )
        persisted = _seal_closure_v8(
            ledger,
            wal_writer,
            block_writer,
            finality,
            bridge_receipt,
            cursors,
        )

        assert persisted.seal.depth_bridge_failed_cycle_count == 1
        assert persisted.seal.depth_bridge_open_cycle_count == 0
        assert (
            persisted.seal.qualification_complete_claimed,
            persisted.seal.promoting,
            persisted.seal.book_bridge_certified,
            persisted.seal.m2_certified,
            persisted.seal.order_execution_enabled,
        ) == (False,) * 5
    finally:
        lease.release()


def test_v8_clean_closure_complete_fsynced_partial_recovers_after_restart(
    tmp_path: Path,
) -> None:
    def crash(point: str) -> None:
        if point == "after_clean_closure_seal_fsync":
            raise OSError("synthetic V8 CLEAN fsync crash")

    lease = _acquire_closure_lease(tmp_path)
    try:
        (
            wal_writer,
            block_writer,
            ledger,
            finality,
            bridge_receipt,
            cursors,
        ) = _v8_closure_stack(tmp_path, lease, ledger_fault_hook=crash)
        with pytest.raises(OSError, match="V8 CLEAN fsync crash"):
            _seal_closure_v8(
                ledger,
                wal_writer,
                block_writer,
                finality,
                bridge_receipt,
                cursors,
            )
        partial = tmp_path / "ledger" / "capture-clean-closure-seal.json.partial"
        final = tmp_path / "ledger" / "capture-clean-closure-seal.json"
        assert partial.is_file()
        assert not final.exists()

        verification_wal, verification_blocks = _verification_only_closure_owners(
            wal_writer,
            block_writer,
        )
        restarted = _ledger(
            tmp_path / "ledger",
            verification_blocks,
            writer_lease=lease,
            authority=_authority(PROMOTING_PLAN_SHA256_V8),
        )
        persisted = restarted.verify_current_clean_closure_seal_v8(
            promoting_plans=PROMOTING_PLANS_V8,
            depth_plan=DEPTH_PLAN_V8,
            wal_writer=verification_wal,
            block_writer=verification_blocks,
            session_id="session-integrity",
            process_boot_id="boot-integrity",
        )
        assert type(persisted) is PersistedCaptureCleanClosureSealReceiptV8
        assert final.is_file()
        assert not partial.exists()
    finally:
        lease.release()


def test_v8_clean_closure_write_fault_cannot_claim_current_clean(
    tmp_path: Path,
) -> None:
    def crash(point: str) -> None:
        if point == "after_clean_closure_seal_write":
            raise OSError("synthetic V8 CLEAN write crash")

    lease = _acquire_closure_lease(tmp_path)
    try:
        (
            wal_writer,
            block_writer,
            ledger,
            finality,
            bridge_receipt,
            cursors,
        ) = _v8_closure_stack(tmp_path, lease, ledger_fault_hook=crash)
        with pytest.raises(OSError, match="V8 CLEAN write crash"):
            _seal_closure_v8(
                ledger,
                wal_writer,
                block_writer,
                finality,
                bridge_receipt,
                cursors,
            )
        assert not (
            tmp_path / "ledger" / "capture-clean-closure-seal.json"
        ).exists()
        assert ledger._clean_closure_receipt is None  # type: ignore[attr-defined]
        with pytest.raises(CaptureIntegrityLedgerError, match="fault-latched"):
            ledger.verify_current_clean_closure_seal_v8(
                promoting_plans=PROMOTING_PLANS_V8,
                depth_plan=DEPTH_PLAN_V8,
                wal_writer=wal_writer,
                block_writer=block_writer,
                session_id="session-integrity",
                process_boot_id="boot-integrity",
            )
    finally:
        lease.release()


@pytest.mark.parametrize(
    "tamper_case",
    ["bridge_receipt", "bridge_hash", "websocket_tail", "last_drain"],
)
def test_v8_persisted_clean_closure_projection_tamper_is_rejected(
    tmp_path: Path,
    tamper_case: str,
) -> None:
    lease = _acquire_closure_lease(tmp_path)
    try:
        (
            wal_writer,
            block_writer,
            ledger,
            finality,
            bridge_receipt,
            cursors,
        ) = _v8_closure_stack(tmp_path, lease)
        persisted = _seal_closure_v8(
            ledger,
            wal_writer,
            block_writer,
            finality,
            bridge_receipt,
            cursors,
        )
        document = json.loads(persisted.encoded_line)
        assert isinstance(document, dict)
        if tamper_case == "bridge_receipt":
            entry = document["depth_bridge_closure_entry"]
            assert isinstance(entry, dict)
            entry["receipt_sha256"] = "f" * 64
        elif tamper_case == "bridge_hash":
            document["depth_bridge_closure_entry_sha256"] = "f" * 64
        elif tamper_case == "websocket_tail":
            cursor_pair = document["websocket_route_cursor_closure_pair"]
            assert isinstance(cursor_pair, list)
            public_cursor = cursor_pair[1]
            assert isinstance(public_cursor, dict)
            public_cursor["finality_tail_ingest_seq"] = 2
        else:
            document["last_depth_bridge_drain_event_sha256"] = "f" * 64
        Path(persisted.canonical_path).write_bytes(canonical_json_line(document))

        with pytest.raises(
            CaptureIntegrityLedgerIntegrityError,
            match="invalid CLEAN closure seal",
        ):
            _ledger(
                tmp_path / "ledger",
                block_writer,
                writer_lease=lease,
                authority=_authority(PROMOTING_PLAN_SHA256_V8),
            )
    finally:
        lease.release()


@pytest.mark.parametrize("artifact", ["final", "partial"])
def test_v8_clean_closure_unknown_schema_is_rejected_for_each_fixed_artifact(
    tmp_path: Path,
    artifact: str,
) -> None:
    lease = _acquire_closure_lease(tmp_path)
    try:
        (
            wal_writer,
            block_writer,
            ledger,
            finality,
            bridge_receipt,
            cursors,
        ) = _v8_closure_stack(tmp_path, lease)
        persisted = _seal_closure_v8(
            ledger,
            wal_writer,
            block_writer,
            finality,
            bridge_receipt,
            cursors,
        )
        final_path = Path(persisted.canonical_path)
        document = json.loads(final_path.read_bytes())
        assert isinstance(document, dict)
        document["schema_version"] = "r4b_unknown_clean_closure_v999"
        target = (
            final_path
            if artifact == "final"
            else final_path.with_name("capture-clean-closure-seal.json.partial")
        )
        target.write_bytes(canonical_json_line(document))
        if artifact == "partial":
            final_path.unlink()

        with pytest.raises(
            CaptureIntegrityLedgerIntegrityError,
            match="invalid CLEAN closure seal",
        ):
            _ledger(
                tmp_path / "ledger",
                block_writer,
                writer_lease=lease,
                authority=_authority(PROMOTING_PLAN_SHA256_V8),
            )
    finally:
        lease.release()


def test_clean_closure_rejects_simultaneous_final_and_partial_artifacts(
    tmp_path: Path,
) -> None:
    lease = _acquire_closure_lease(tmp_path)
    try:
        (
            wal_writer,
            block_writer,
            ledger,
            finality,
            bridge_receipt,
            cursors,
        ) = _v8_closure_stack(tmp_path, lease)
        persisted = _seal_closure_v8(
            ledger,
            wal_writer,
            block_writer,
            finality,
            bridge_receipt,
            cursors,
        )
        final_path = Path(persisted.canonical_path)
        final_path.with_name("capture-clean-closure-seal.json.partial").write_bytes(
            final_path.read_bytes()
        )

        with pytest.raises(
            CaptureIntegrityLedgerIntegrityError,
            match="both final and partial",
        ):
            _ledger(
                tmp_path / "ledger",
                block_writer,
                writer_lease=lease,
                authority=_authority(PROMOTING_PLAN_SHA256_V8),
            )
    finally:
        lease.release()


def test_v2_v8_clean_closure_schema_dispatch_is_exact(tmp_path: Path) -> None:
    v2_root = tmp_path / "v2"
    v2_root.mkdir()
    v2_lease = _acquire_closure_lease(v2_root)
    try:
        _, wal_writer, block_writer, ledger, finality = _closure_stack(
            v2_root,
            v2_lease,
        )
        persisted_v2 = _seal_closure(
            ledger,
            wal_writer,
            block_writer,
            finality,
        )
        v2_path = Path(persisted_v2.canonical_path)
        parsed_v2 = integrity_ledger_module._read_clean_closure_seal(v2_path)
        assert type(parsed_v2) is type(persisted_v2.seal)
        v2_as_v8 = json.loads(persisted_v2.encoded_line)
        assert isinstance(v2_as_v8, dict)
        v2_as_v8["schema_version"] = (
            "r4b_v2_capture_clean_closure_seal_v8"
        )
        v2_path.write_bytes(canonical_json_line(v2_as_v8))
        with pytest.raises(
            CaptureIntegrityLedgerIntegrityError,
            match="invalid CLEAN closure seal",
        ):
            integrity_ledger_module._read_clean_closure_seal(v2_path)
    finally:
        v2_lease.release()

    v8_root = tmp_path / "v8"
    v8_root.mkdir()
    v8_lease = _acquire_closure_lease(v8_root)
    try:
        (
            wal_writer,
            block_writer,
            ledger,
            finality,
            bridge_receipt,
            cursors,
        ) = _v8_closure_stack(v8_root, v8_lease)
        persisted_v8 = _seal_closure_v8(
            ledger,
            wal_writer,
            block_writer,
            finality,
            bridge_receipt,
            cursors,
        )
        v8_path = Path(persisted_v8.canonical_path)
        parsed_v8 = integrity_ledger_module._read_clean_closure_seal(v8_path)
        assert type(parsed_v8) is CaptureCleanClosureSealV8
        v8_as_v2 = json.loads(persisted_v8.encoded_line)
        assert isinstance(v8_as_v2, dict)
        v8_as_v2["schema_version"] = "r4b_v2_capture_clean_closure_seal_v2"
        v8_path.write_bytes(canonical_json_line(v8_as_v2))
        with pytest.raises(
            CaptureIntegrityLedgerIntegrityError,
            match="invalid CLEAN closure seal",
        ):
            integrity_ledger_module._read_clean_closure_seal(v8_path)
    finally:
        v8_lease.release()


def test_v8_clean_closure_concurrent_issuance_is_one_shot(tmp_path: Path) -> None:
    lease = _acquire_closure_lease(tmp_path)
    receipts: list[PersistedCaptureCleanClosureSealReceiptV8] = []
    errors: list[Exception] = []
    try:
        (
            wal_writer,
            block_writer,
            ledger,
            finality,
            bridge_receipt,
            cursors,
        ) = _v8_closure_stack(tmp_path, lease)

        def issue() -> None:
            try:
                receipts.append(
                    _seal_closure_v8(
                        ledger,
                        wal_writer,
                        block_writer,
                        finality,
                        bridge_receipt,
                        cursors,
                    )
                )
            except Exception as exc:
                errors.append(exc)

        threads = tuple(
            threading.Thread(target=issue, name=f"v8-clean-issuer-{index}")
            for index in range(2)
        )
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=5)

        assert all(not thread.is_alive() for thread in threads)
        assert len(receipts) == 1
        assert len(errors) == 1
        assert isinstance(errors[0], CaptureIntegrityLedgerIntegrityError)
        assert "already consumed" in str(errors[0])
    finally:
        lease.release()


def test_depth_bridge_canonical_parse_tamper_and_foreign_plan() -> None:
    payload = _depth_bridge_lifecycle()[0]
    document = json.loads(canonical_json_line(asdict(payload)))
    assert isinstance(document, dict)
    assert parse_depth_bridge_evidence_payload_v8(document) == payload

    document["promoting"] = True
    with pytest.raises(DepthBridgeEvidenceErrorV8, match="qualification-only"):
        parse_depth_bridge_evidence_payload_v8(document)

    foreign_plans = build_provisional_promoting_capture_plans_v8(("ETHUSDT",))
    foreign_depth = next(
        plan
        for plan in foreign_plans
        if type(plan) is ProvisionalDepthRestQualificationPlanV8
    )
    assert isinstance(foreign_depth, ProvisionalDepthRestQualificationPlanV8)
    with pytest.raises(DepthBridgeEvidenceErrorV8, match="foreign v8 plan"):
        validate_depth_bridge_evidence_payload_v8(
            payload,
            promoting_plans=foreign_plans,
            depth_plan=foreign_depth,
        )


def test_depth_bridge_happy_lifecycle_deduplicates_and_reloads(
    tmp_path: Path,
) -> None:
    writer, ledger = _depth_bridge_ledger(tmp_path)
    lifecycle = _depth_bridge_lifecycle()

    first = ledger.append_depth_bridge_v8(
        lifecycle[0],
        PROMOTING_PLANS_V8,
        DEPTH_PLAN_V8,
    )
    duplicate = ledger.append_depth_bridge_v8(
        lifecycle[0],
        PROMOTING_PLANS_V8,
        DEPTH_PLAN_V8,
    )
    assert duplicate == first
    assert len(ledger.events) == 1
    for payload in lifecycle[1:]:
        ledger.append_depth_bridge_v8(
            payload,
            PROMOTING_PLANS_V8,
            DEPTH_PLAN_V8,
        )

    restarted = _ledger(
        tmp_path / "ledger",
        writer,
        authority=_authority(PROMOTING_PLAN_SHA256_V8),
    )
    assert restarted.events == ledger.events
    assert len(restarted.events) == 6
    assert all(
        event.event_type == "DEPTH_BRIDGE" for event in restarted.events
    )


def test_depth_bridge_rejects_invalid_phase_order_and_event_id_collision(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _, ledger = _depth_bridge_ledger(tmp_path)
    lifecycle = _depth_bridge_lifecycle()
    with pytest.raises(
        CaptureIntegrityLedgerIntegrityError,
        match="phase order",
    ):
        ledger.append_depth_bridge_v8(
            lifecycle[1],
            PROMOTING_PLANS_V8,
            DEPTH_PLAN_V8,
        )

    first = ledger.append_depth_bridge_v8(
        lifecycle[0],
        PROMOTING_PLANS_V8,
        DEPTH_PLAN_V8,
    )
    monkeypatch.setattr(
        integrity_ledger_module,
        "_event_id",
        lambda **_kwargs: first.event_id,
    )
    with pytest.raises(
        CaptureIntegrityLedgerIntegrityError,
        match="collides",
    ):
        ledger.append_depth_bridge_v8(
            lifecycle[1],
            PROMOTING_PLANS_V8,
            DEPTH_PLAN_V8,
        )


def test_depth_bridge_reserves_every_open_terminal_slot(tmp_path: Path) -> None:
    _, too_small = _depth_bridge_ledger(tmp_path / "too-small", max_events=1)
    with pytest.raises(
        CaptureIntegrityLedgerCapacityError,
        match="closure slots",
    ):
        too_small.append_depth_bridge_v8(
            _depth_bridge_lifecycle()[0],
            PROMOTING_PLANS_V8,
            DEPTH_PLAN_V8,
        )
    assert too_small.events == ()

    _, attempt_too_small = _depth_bridge_ledger(
        tmp_path / "attempt-too-small",
        max_events=5,
    )
    lifecycle = _depth_bridge_lifecycle()
    for payload in lifecycle[:2]:
        attempt_too_small.append_depth_bridge_v8(
            payload,
            PROMOTING_PLANS_V8,
            DEPTH_PLAN_V8,
        )
    with pytest.raises(
        CaptureIntegrityLedgerCapacityError,
        match="closure slots",
    ):
        attempt_too_small.append_depth_bridge_v8(
            lifecycle[2],
            PROMOTING_PLANS_V8,
            DEPTH_PLAN_V8,
        )
    assert len(attempt_too_small.events) == 2

    _, exact = _depth_bridge_ledger(tmp_path / "exact", max_events=6)
    assert len(_append_depth_bridge_lifecycle(exact)) == 6

    _, byte_too_small = _depth_bridge_ledger(
        tmp_path / "byte-too-small",
        maximum_total_bytes=128 * 1024,
    )
    with pytest.raises(
        CaptureIntegrityLedgerCapacityError,
        match="reserved disk budget",
    ):
        byte_too_small.append_depth_bridge_v8(
            lifecycle[0],
            PROMOTING_PLANS_V8,
            DEPTH_PLAN_V8,
        )
    assert byte_too_small.events == ()


def test_depth_bridge_reload_rejects_broken_hash_chain(tmp_path: Path) -> None:
    writer, ledger = _depth_bridge_ledger(tmp_path)
    _append_depth_bridge_lifecycle(ledger)
    second_path = tmp_path / "ledger" / "integrity-event-00000002.json"
    document = json.loads(second_path.read_bytes())
    assert isinstance(document, dict)
    document["previous_event_sha256"] = "f" * 64
    second_path.write_bytes(canonical_json_line(document))

    with pytest.raises(
        CaptureIntegrityLedgerIntegrityError,
        match="hash chain",
    ):
        _ledger(
            tmp_path / "ledger",
            writer,
            authority=_authority(PROMOTING_PLAN_SHA256_V8),
        )


def test_depth_bridge_fatal_generation_can_close_after_full_drain(
    tmp_path: Path,
) -> None:
    _, ledger = _depth_bridge_ledger(tmp_path, max_events=2)
    start = _depth_bridge_lifecycle()[0]
    fatal_drain = _depth_bridge_payload(
        DepthBridgePhaseV8.GENERATION_DRAINED,
        DepthBridgeGenerationDrainedV8(
            reason="fatal",
            fatal_cause_code="coordinator_failure",
            fatal_cause_sha256="5" * 64,
            registered_cycle_count=0,
            accepted_cycle_count=0,
            superseded_cycle_count=0,
            failed_cycle_count=0,
            worker_count=0,
            permit_in_use_count=0,
            retained_registration_count=0,
            pending_registration_count=0,
            retained_token_count=0,
            claimed_token_count=0,
            adapter_active_attempt_count=0,
            adapter_pending_owner_task_count=0,
            retained_terminal_admission_count=0,
            adapter_closed=True,
            adapter_cleanly_closed=False,
        ),
    )
    ledger.append_depth_bridge_v8(
        start,
        PROMOTING_PLANS_V8,
        DEPTH_PLAN_V8,
    )
    ledger.append_depth_bridge_v8(
        fatal_drain,
        PROMOTING_PLANS_V8,
        DEPTH_PLAN_V8,
    )
    assert len(ledger.events) == 2
    census = depth_bridge_evidence_census_v8((start, fatal_drain))
    assert census.fatal_generation_count == 1
    assert census.last_drain_reason == "fatal"
    assert census.open_terminal_reservation_count == 0

    successor_start = replace(
        start,
        connection_id="connection-depth-2",
        connection_generation=2,
    )
    with pytest.raises(DepthBridgeEvidenceErrorV8, match="fatal generation drain"):
        depth_bridge_evidence_census_v8(
            (start, fatal_drain, successor_start)
        )
    with pytest.raises(
        CaptureIntegrityLedgerIntegrityError,
        match="phase order",
    ):
        ledger.append_depth_bridge_v8(
            successor_start,
            PROMOTING_PLANS_V8,
            DEPTH_PLAN_V8,
        )


def test_depth_bridge_only_reconnect_drain_can_precede_successor_generation() -> None:
    start = _depth_bridge_lifecycle()[0]
    normal_drain = replace(
        _depth_bridge_lifecycle()[-1],
        material=replace(
            _depth_bridge_lifecycle()[-1].material,
            registered_cycle_count=0,
            accepted_cycle_count=0,
        ),
    )
    successor_start = replace(
        start,
        connection_id="connection-depth-2",
        connection_generation=2,
    )
    with pytest.raises(DepthBridgeEvidenceErrorV8, match="only a reconnect drain"):
        depth_bridge_evidence_census_v8(
            (start, normal_drain, successor_start)
        )

    reconnect_drain = replace(
        normal_drain,
        material=replace(normal_drain.material, reason="reconnect"),
    )
    census = depth_bridge_evidence_census_v8(
        (start, reconnect_drain, successor_start)
    )
    assert census.generation_started_count == 2
    assert census.generation_drained_count == 1
    assert census.fatal_generation_count == 0
    assert census.last_drain_reason == "reconnect"
    assert census.open_generation_count == 1


def test_depth_bridge_wait_is_exactly_paired_and_blocks_early_retry() -> None:
    start, trigger, attempt_start, *_ = _depth_bridge_lifecycle()
    cycle = _depth_bridge_cycle()
    ws_source = _depth_bridge_ws_source()
    range_summary = build_depth_bridge_range_summary_v8((ws_source,))
    wait_started = 3_000
    wait_deadline = wait_started + DEPTH_PLAN_V8.bridge_wait_timeout_ms * 1_000_000
    waiting = _depth_bridge_payload(
        DepthBridgePhaseV8.ATTEMPT_TERMINAL,
        DepthBridgeAttemptTerminalV8(
            cycle=cycle,
            bridge_attempt=1,
            classification="waiting",
            rest_source=DepthBridgeRestSourceLocatorV8(
                symbol="BTCUSDT",
                trigger_seq=1,
                first_buffered_u=100,
                bridge_attempt=1,
                ingest_seq=3,
                raw_record_sha256="3" * 64,
                attempt_payload_sha256="4" * 64,
                receipt_wall_ms=1_001,
                receipt_monotonic_ns=2_001,
            ),
            semantic_admission_sha256="2" * 64,
            last_update_id=100,
            target_update_id=100,
            discarded_range_count=0,
            range_summary=range_summary,
            failure_code=None,
            wait_started_monotonic_ns=wait_started,
            wait_deadline_monotonic_ns=wait_deadline,
        ),
    )
    retry = _depth_bridge_payload(
        DepthBridgePhaseV8.ATTEMPT_STARTED,
        DepthBridgeAttemptStartedV8(cycle=cycle, bridge_attempt=2),
    )
    with pytest.raises(DepthBridgeEvidenceErrorV8, match="attempt or paired wait"):
        depth_bridge_evidence_census_v8(
            (start, trigger, attempt_start, waiting, retry)
        )

    accepted_wait = _depth_bridge_payload(
        DepthBridgePhaseV8.WAIT_TERMINAL,
        DepthBridgeWaitTerminalV8(
            cycle=cycle,
            bridge_attempt=1,
            outcome="accepted",
            wait_started_monotonic_ns=wait_started,
            wait_deadline_monotonic_ns=wait_deadline,
            wait_ended_monotonic_ns=4_000,
            target_update_id=100,
            discarded_range_count=0,
            range_summary=range_summary,
        ),
    )
    cycle_terminal = _depth_bridge_payload(
        DepthBridgePhaseV8.CYCLE_TERMINAL,
        DepthBridgeCycleTerminalV8(
            cycle=cycle,
            outcome="accepted",
            reason="snapshot_range_bridge",
            terminal_bridge_attempt=1,
            semantic_admission_sha256="2" * 64,
            target_update_id=100,
            bridging_range_summary=range_summary,
        ),
    )
    drain = _depth_bridge_lifecycle()[-1]
    census = depth_bridge_evidence_census_v8(
        (
            start,
            trigger,
            attempt_start,
            waiting,
            accepted_wait,
            cycle_terminal,
            drain,
        )
    )
    assert census.open_terminal_reservation_count == 0

    mismatched_wait = replace(
        accepted_wait,
        material=replace(
            accepted_wait.material,
            wait_started_monotonic_ns=wait_started + 1,
        ),
    )
    with pytest.raises(DepthBridgeEvidenceErrorV8, match="paired waiting attempt"):
        depth_bridge_evidence_census_v8(
            (start, trigger, attempt_start, waiting, mismatched_wait)
        )


def test_depth_bridge_trigger_sequence_and_first_u_are_strict() -> None:
    start, trigger, *_ = _depth_bridge_lifecycle()
    old_cycle = _depth_bridge_cycle()

    def successor(*, trigger_seq: int, first_u: int) -> DepthBridgeEvidencePayloadV8:
        cycle = build_depth_bridge_cycle_ref_v8(
            session_id="session-integrity",
            protocol_hash=HASH,
            plan_bundle_sha256=PROMOTING_PLAN_SHA256_V8,
            depth_plan_sha256=public_depth_rest_plan_sha256_v8(DEPTH_PLAN_V8),
            connection_id="connection-depth-1",
            connection_generation=1,
            symbol="BTCUSDT",
            symbol_ordinal=0,
            trigger_seq=trigger_seq,
            first_buffered_u=first_u,
        )
        source = replace(
            _depth_bridge_ws_source(),
            frame_seq=2,
            ingest_seq=3,
            first_update_id=first_u,
            final_update_id=first_u,
        )
        return _depth_bridge_payload(
            DepthBridgePhaseV8.TRIGGER_REGISTERED,
            DepthBridgeTriggerRegisteredV8(
                trigger="sequence_gap",
                trigger_seq=trigger_seq,
                cycles=(
                    DepthBridgeRegisteredCycleV8(
                        cycle=cycle,
                        initial_range_source=source,
                        supersedes_cycle_id=old_cycle.cycle_id,
                    ),
                ),
            ),
        )

    with pytest.raises(DepthBridgeEvidenceErrorV8, match="trigger sequence"):
        depth_bridge_evidence_census_v8(
            (start, trigger, successor(trigger_seq=3, first_u=101))
        )
    with pytest.raises(DepthBridgeEvidenceErrorV8, match="first buffered U"):
        depth_bridge_evidence_census_v8(
            (start, trigger, successor(trigger_seq=2, first_u=100))
        )


def test_depth_bridge_failed_attempt_retains_source_and_closes_census() -> None:
    start, trigger, attempt_start, *_ = _depth_bridge_lifecycle()
    cycle = _depth_bridge_cycle()
    range_summary = build_depth_bridge_range_summary_v8(
        (_depth_bridge_ws_source(),)
    )
    rest_source = DepthBridgeRestSourceLocatorV8(
        symbol="BTCUSDT",
        trigger_seq=1,
        first_buffered_u=100,
        bridge_attempt=1,
        ingest_seq=3,
        raw_record_sha256="3" * 64,
        attempt_payload_sha256="4" * 64,
        receipt_wall_ms=1_001,
        receipt_monotonic_ns=2_001,
    )
    failed_material = DepthBridgeAttemptTerminalV8(
        cycle=cycle,
        bridge_attempt=1,
        classification="failed",
        rest_source=rest_source,
        semantic_admission_sha256=None,
        last_update_id=None,
        target_update_id=None,
        discarded_range_count=0,
        range_summary=range_summary,
        failure_code="http_terminal",
        wait_started_monotonic_ns=None,
        wait_deadline_monotonic_ns=None,
    )
    with pytest.raises(DepthBridgeEvidenceErrorV8, match="REST source locator"):
        replace(failed_material, rest_source=None)
    failed_attempt = _depth_bridge_payload(
        DepthBridgePhaseV8.ATTEMPT_TERMINAL,
        failed_material,
    )
    failed_cycle = _depth_bridge_payload(
        DepthBridgePhaseV8.CYCLE_TERMINAL,
        DepthBridgeCycleTerminalV8(
            cycle=cycle,
            outcome="failed",
            reason="http_terminal",
            terminal_bridge_attempt=1,
            semantic_admission_sha256=None,
            target_update_id=None,
            bridging_range_summary=None,
        ),
    )
    failed_drain = _depth_bridge_payload(
        DepthBridgePhaseV8.GENERATION_DRAINED,
        replace(
            _depth_bridge_lifecycle()[-1].material,
            accepted_cycle_count=0,
            failed_cycle_count=1,
        ),
    )
    census = depth_bridge_evidence_census_v8(
        (start, trigger, attempt_start, failed_attempt, failed_cycle, failed_drain)
    )
    assert census.failed_cycle_count == 1
    assert census.open_terminal_reservation_count == 0


def test_depth_bridge_range_summary_rejects_above_frozen_capacity() -> None:
    with pytest.raises(DepthBridgeEvidenceErrorV8, match="frozen per-symbol"):
        DepthBridgeRangeSummaryV8(
            symbol="BTCUSDT",
            range_count=(
                DEPTH_BRIDGE_MAXIMUM_BUFFERED_RANGES_PER_SYMBOL_V8 + 1
            ),
            range_root_sha256="6" * 64,
            first_ingest_seq=1,
            last_ingest_seq=2,
        )


def test_v2_clean_closure_rejects_v8_authority_before_terminalizing_blocks(
    tmp_path: Path,
) -> None:
    lease = _acquire_closure_lease(tmp_path)
    try:
        authority = _authority(PROMOTING_PLAN_SHA256_V8)
        _, wal_writer, block_writer, ledger, finality = _closure_stack(
            tmp_path,
            lease,
            authority=authority,
        )
        _append_depth_bridge_lifecycle(ledger)
        with pytest.raises(
            CaptureIntegrityLedgerIntegrityError,
            match="plan authority differs",
        ):
            _seal_closure(ledger, wal_writer, block_writer, finality)
        assert not (
            block_writer.directory / "block-clean-tail-terminal.json"
        ).exists()

        with pytest.raises(TypeError, match="non-exact plan members"):
            ledger.seal_clean_closure_v2(
                promoting_plans=PROMOTING_PLANS_V8,  # type: ignore[arg-type]
                finality_receipt=finality,
                wal_writer=wal_writer,
                block_writer=block_writer,
                session_id="session-integrity",
                process_boot_id="boot-integrity",
                seal_wall_ms=100_000,
                seal_monotonic_ns=10_000_000,
            )
        assert not (
            block_writer.directory / "block-clean-tail-terminal.json"
        ).exists()
    finally:
        lease.release()
