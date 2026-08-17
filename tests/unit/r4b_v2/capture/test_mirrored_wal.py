from __future__ import annotations

import hashlib
import json
import os
from dataclasses import replace
from pathlib import Path

import pytest

from signalbot.r4b_v2.capture.batching import (
    BatchPolicyV2,
    QueuedRawRecordV2,
)
from signalbot.r4b_v2.capture.block_container import (
    BlockSigningAuthorityV2,
    Ed25519BlockSignerV2,
)
from signalbot.r4b_v2.capture.blocks import (
    BlockPolicyV2,
    GroupedBlockBuilderV2,
    GroupedBlockWriterV2,
    verify_grouped_blocks,
)
from signalbot.r4b_v2.capture.mirrored_wal import (
    MirroredWalFailedError,
    MirroredWalIntegrityError,
    MirroredWalPrefixProofV2,
    MirroredWalWriterV2,
)
from signalbot.r4b_v2.capture.models import RawRecordV2, TransportV2, VenueV2
from signalbot.r4b_v2.capture.pipeline import (
    DurableCaptureBatchWriterV2,
    verify_capture_finality_fence_receipt_v2,
)
from signalbot.r4b_v2.capture.wal import (
    FaultHook,
    WalAuthorityV2,
    WalDurabilityBindingV2,
    WalIntegrityError,
    WalSyncPolicyV2,
    WalWriterV2,
    encode_wal_frame,
    verify_wal_segments,
)
from signalbot.r4b_v2.capture.wal_qualification import (
    WAL_QUALIFICATION_DURATION_MS_V2,
    WAL_RECORD_CAP_CANDIDATES_V2,
    WAL_SYNC_CANDIDATES_MS_V2,
    WalCandidateMetricsV2,
    WalCandidateQualificationV2,
    WalQualificationError,
    WalQualificationRunV2,
    WalSelectionReceiptV2,
    select_wal_candidate_v2,
    wal_candidate_id_v2,
)

HASH = "a" * 64
QUALIFICATION = "wal-final-panel-24h-grid-q1"
WINDOW_START_MS = 2_000_000_000_000
WINDOW_END_MS = WINDOW_START_MS + WAL_QUALIFICATION_DURATION_MS_V2
H_START_MS = WINDOW_END_MS + 60_000
MAXIMUM_BYTES = 64 * 1024 * 1024
RESERVE_BYTES = 1024


class _Clock:
    def __init__(self) -> None:
        self.value = 1_000_000_000

    def __call__(self) -> int:
        return self.value


def _authority() -> WalAuthorityV2:
    return WalAuthorityV2(
        attempt_id="attempt-dual-wal",
        protocol_sha256=HASH,
        plan_sha256="b" * 64,
        source_manifest_sha256="c" * 64,
        schema_sha256="d" * 64,
        runtime_manifest_sha256="e" * 64,
    )


def _policy(
    sync_ms: int = 10,
    record_cap: int = 256,
    **overrides: int | str,
) -> WalSyncPolicyV2:
    values: dict[str, int | str] = {
        "qualification_id": QUALIFICATION,
        "fsync_candidate_id": wal_candidate_id_v2(
            sync_ms=sync_ms,
            record_cap=record_cap,
        ),
        "interval_ms": sync_ms,
        "max_unsynced_records": record_cap,
        "max_unsynced_bytes": 8_000_000,
        "max_record_bytes": 20_000,
        "max_segment_bytes": 16_000_000,
    }
    values.update(overrides)
    return WalSyncPolicyV2(**values)  # type: ignore[arg-type]


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


def _selection_receipt(
    *,
    sync_ms: int = 10,
    record_cap: int = 256,
    selection_wall_ms: int = WINDOW_END_MS,
) -> WalSelectionReceiptV2:
    selected = (sync_ms, record_cap)
    candidates = tuple(
        WalCandidateQualificationV2(
            policy=_policy(candidate_sync, candidate_cap),
            metrics=_candidate_metrics(passed=(candidate_sync, candidate_cap) == selected),
            measurement_root_sha256=hashlib.sha256(
                f"{candidate_sync}:{candidate_cap}".encode()
            ).hexdigest(),
        )
        for candidate_sync in WAL_SYNC_CANDIDATES_MS_V2
        for candidate_cap in WAL_RECORD_CAP_CANDIDATES_V2
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
        selection_wall_ms=selection_wall_ms,
        h_start_wall_ms=H_START_MS,
    )


def _queued(
    ingest_seq: int,
    *,
    raw_payload: str = '{"p":"100","q":"1"}',
) -> QueuedRawRecordV2:
    monotonic_ns = 1_000_000 + ingest_seq
    record = RawRecordV2.from_payload(
        session_id="session-dual-wal",
        plan_id="plan-dual-wal",
        protocol_hash=HASH,
        transport=TransportV2.WEBSOCKET,
        venue=VenueV2.USDM_FUTURES,
        route_id="futures-market",
        symbol="BTCUSDT",
        connection_id="connection-dual-wal",
        generation=1,
        frame_seq=ingest_seq,
        ingest_seq=ingest_seq,
        receipt_wall_ms=1_000 + ingest_seq,
        receipt_monotonic_ns=monotonic_ns,
        raw_payload=raw_payload,
        source_logical_key=f"trade-{ingest_seq}",
    )
    return QueuedRawRecordV2.encode(
        record,
        enqueued_monotonic_ns=monotonic_ns + 1,
    )


def _mirrored(
    tmp_path: Path,
    *,
    selection_receipt: WalSelectionReceiptV2 | None = None,
    policy: WalSyncPolicyV2 | None = None,
    primary_fault_hook: FaultHook | None = None,
    mirror_fault_hook: FaultHook | None = None,
) -> MirroredWalWriterV2:
    receipt = selection_receipt or _selection_receipt()
    selected_policy = receipt.selected_policy
    runtime_policy = policy if policy is not None else selected_policy
    if runtime_policy is None:
        raise AssertionError("test helper requires an explicit policy for a blocked receipt")
    return MirroredWalWriterV2(
        tmp_path / "primary",
        tmp_path / "mirror",
        authority=_authority(),
        policy=runtime_policy,
        selection_receipt=receipt,
        primary_maximum_total_bytes=MAXIMUM_BYTES,
        mirror_maximum_total_bytes=MAXIMUM_BYTES,
        primary_emergency_reserve_bytes=RESERVE_BYTES,
        mirror_emergency_reserve_bytes=RESERVE_BYTES,
        primary_failure_domain_id="declared-device-primary",
        mirror_failure_domain_id="declared-device-mirror",
        clock_ns=_Clock(),
        primary_fault_hook=primary_fault_hook,
        mirror_fault_hook=mirror_fault_hook,
    )


def _mirrored_verification_only(
    tmp_path: Path,
    *,
    selection_receipt: WalSelectionReceiptV2 | None = None,
) -> MirroredWalWriterV2:
    receipt = selection_receipt or _selection_receipt()
    policy = receipt.selected_policy
    if policy is None:
        raise AssertionError("test helper requires a selected policy")
    return MirroredWalWriterV2.open_verification_only_v2(
        tmp_path / "primary",
        tmp_path / "mirror",
        authority=_authority(),
        policy=policy,
        selection_receipt=receipt,
        primary_maximum_total_bytes=MAXIMUM_BYTES,
        mirror_maximum_total_bytes=MAXIMUM_BYTES,
        primary_emergency_reserve_bytes=RESERVE_BYTES,
        mirror_emergency_reserve_bytes=RESERVE_BYTES,
        primary_failure_domain_id="declared-device-primary",
        mirror_failure_domain_id="declared-device-mirror",
        clock_ns=_Clock(),
    )


def _seed_copy(
    root: Path,
    *,
    role: str,
    failure_domain_id: str,
    records: list[QueuedRawRecordV2],
    policy: WalSyncPolicyV2,
    selection_receipt_sha256: str,
) -> None:
    writer = WalWriterV2(
        root,
        authority=_authority(),
        policy=policy,
        maximum_total_bytes=MAXIMUM_BYTES,
        emergency_reserve_bytes=RESERVE_BYTES,
        clock_ns=_Clock(),
        root_role=role,
        failure_domain_id=failure_domain_id,
        qualification_selection_receipt_sha256=selection_receipt_sha256,
    )
    writer.append_batch(records)
    writer.sync()
    writer.close()


def test_joint_ack_advances_only_after_both_copies_sync_and_reopens(
    tmp_path: Path,
) -> None:
    receipt = _selection_receipt()
    policy = receipt.selected_policy
    assert policy is not None
    writer = _mirrored(tmp_path, selection_receipt=receipt)
    records = [_queued(1), _queued(2)]
    result = writer.append_batch(records)
    assert result.durable_ack_seq == 0
    assert not result.fsynced
    assert writer.durable_ack_seq == 0

    assert writer.sync() == 2
    assert writer.durable_ack_seq == 2
    observed: list[tuple[int, bytes]] = []
    assert writer.consume_durable_records(lambda seq, line: observed.append((seq, line))) == 2
    assert [seq for seq, _ in observed] == [1, 2]
    assert writer.primary_root_binding.root_role == "PRIMARY"
    assert writer.mirror_root_binding.root_role == "INDEPENDENT_MIRROR"
    assert not writer.physical_failure_domain_independence_verified
    writer.abort()

    reopened = _mirrored(tmp_path, selection_receipt=receipt)
    assert reopened.durable_ack_seq == 2
    assert reopened.next_ingest_seq == 3
    reopened.close()
    reopened.close()
    primary = verify_wal_segments(tmp_path / "primary", authority=_authority(), policy=policy)
    mirror = verify_wal_segments(tmp_path / "mirror", authority=_authority(), policy=policy)
    assert [(item.last_ingest_seq, item.sha256) for item in primary] == [
        (item.last_ingest_seq, item.sha256) for item in mirror
    ]


def test_mirrored_wal_exposes_exact_qualified_dual_owner_binding(
    tmp_path: Path,
) -> None:
    receipt = _selection_receipt()
    writer = _mirrored(tmp_path, selection_receipt=receipt)

    binding = writer.durability_binding

    assert binding.mode == "QUALIFIED_DUAL_OWNER"
    assert binding.root_bindings == (
        writer.primary_root_binding,
        writer.mirror_root_binding,
    )
    assert writer.root_directories == (
        (tmp_path / "primary").resolve(),
        (tmp_path / "mirror").resolve(),
    )
    assert binding.qualification_selection_receipt_sha256 == receipt.sha256
    assert (
        binding.physical_failure_domain_independence_verified
        is writer.physical_failure_domain_independence_verified
        is False
    )
    assert binding.sha256 == writer.durability_binding.sha256
    writer.abort()


def test_qualified_dual_binding_validates_selection_roles_and_authority(
    tmp_path: Path,
) -> None:
    receipt = _selection_receipt()
    writer = _mirrored(tmp_path, selection_receipt=receipt)
    roots = (writer.primary_root_binding, writer.mirror_root_binding)

    with pytest.raises(ValueError, match="requires its selection receipt"):
        WalDurabilityBindingV2(
            mode="QUALIFIED_DUAL_OWNER",
            root_bindings=roots,
            qualification_selection_receipt_sha256=None,
            physical_failure_domain_independence_verified=False,
        )
    with pytest.raises(ValueError, match="ordered PRIMARY"):
        WalDurabilityBindingV2(
            mode="QUALIFIED_DUAL_OWNER",
            root_bindings=(roots[1], roots[0]),
            qualification_selection_receipt_sha256=receipt.sha256,
            physical_failure_domain_independence_verified=False,
        )
    with pytest.raises(ValueError, match="root authorities differ"):
        WalDurabilityBindingV2(
            mode="QUALIFIED_DUAL_OWNER",
            root_bindings=(
                roots[0],
                replace(roots[1], authority_sha256="f" * 64),
            ),
            qualification_selection_receipt_sha256=receipt.sha256,
            physical_failure_domain_independence_verified=False,
        )
    with pytest.raises(ValueError, match="lowercase SHA-256"):
        WalDurabilityBindingV2(
            mode="QUALIFIED_DUAL_OWNER",
            root_bindings=roots,
            qualification_selection_receipt_sha256="F" * 64,
            physical_failure_domain_independence_verified=False,
        )
    writer.abort()


def test_mirrored_wal_revalidates_both_current_root_binding_files(
    tmp_path: Path,
) -> None:
    writer = _mirrored(tmp_path)
    writer.assert_root_binding_current()
    (tmp_path / "mirror" / "storage-root-binding.json").unlink()

    with pytest.raises(WalIntegrityError, match="root binding is missing"):
        writer.assert_root_binding_current()
    with pytest.raises(MirroredWalFailedError, match="fault-latched"):
        writer.assert_root_binding_current()
    writer.abort()


@pytest.mark.parametrize("root_name", ["primary", "mirror"])
def test_recreated_dual_wal_root_with_copied_binding_cannot_mutate_or_ack(
    tmp_path: Path,
    root_name: str,
) -> None:
    writer = _mirrored(tmp_path)
    root = tmp_path / root_name
    moved = tmp_path / f"{root_name}-moved"
    binding = root.joinpath("storage-root-binding.json").read_bytes()
    root.rename(moved)
    root.mkdir()
    root.joinpath("storage-root-binding.json").write_bytes(binding)

    with pytest.raises(WalIntegrityError, match=r"identity differs|fault-latched"):
        writer.append_batch([_queued(1)])

    assert writer.durable_ack_seq == 0
    assert writer.next_ingest_seq == 1
    assert not list(root.glob("wal-*.partial"))
    writer.abort()


def test_second_copy_fsync_failure_cannot_advance_joint_ack(tmp_path: Path) -> None:
    def fail_mirror_fsync(point: str) -> None:
        if point == "before_wal_fsync":
            raise OSError("injected mirror fsync failure")

    writer = _mirrored(tmp_path, mirror_fault_hook=fail_mirror_fsync)
    assert writer.append_batch([_queued(1)]).durable_ack_seq == 0
    with pytest.raises(OSError, match="injected mirror fsync failure"):
        writer.sync()
    assert writer.durable_ack_seq == 0
    primary_state = json.loads(
        (tmp_path / "primary" / "wal-state.json").read_text(encoding="utf-8")
    )
    assert primary_state["durable_ack_seq"] == 1
    with pytest.raises(MirroredWalFailedError, match="fault-latched"):
        writer.append_batch([_queued(2)])
    writer.abort()


@pytest.mark.parametrize("record_cap", WAL_RECORD_CAP_CANDIDATES_V2)
def test_exact_selected_record_cap_syncs_both_copies(
    tmp_path: Path,
    record_cap: int,
) -> None:
    receipt = _selection_receipt(sync_ms=100, record_cap=record_cap)
    writer = _mirrored(tmp_path, selection_receipt=receipt)
    first = writer.append_batch([_queued(seq) for seq in range(1, record_cap)])
    assert first.durable_ack_seq == 0
    assert first.pending_records == record_cap - 1
    assert not first.fsynced

    boundary = writer.append_batch([_queued(record_cap)])
    assert boundary.durable_ack_seq == record_cap
    assert boundary.pending_records == 0
    assert boundary.fsynced
    writer.close()


@pytest.mark.parametrize("sync_ms", WAL_SYNC_CANDIDATES_MS_V2)
def test_exact_selected_time_boundary_syncs_both_copies(
    tmp_path: Path,
    sync_ms: int,
) -> None:
    receipt = _selection_receipt(sync_ms=sync_ms, record_cap=4096)
    writer = _mirrored(tmp_path, selection_receipt=receipt)
    start_ns = 1_000_000_000
    assert not writer.append_batch([_queued(1)], now_ns=start_ns).fsynced
    before = writer.append_batch(
        [_queued(2)],
        now_ns=start_ns + sync_ms * 1_000_000 - 1,
    )
    assert before.durable_ack_seq == 0
    assert not before.fsynced

    exact = writer.append_batch(
        [_queued(3)],
        now_ns=start_ns + sync_ms * 1_000_000,
    )
    assert exact.durable_ack_seq == 3
    assert exact.fsynced
    writer.close()


def test_asymmetric_append_failure_is_fault_latched_without_ack(tmp_path: Path) -> None:
    def fail_mirror_write(point: str) -> None:
        if point == "before_batch_write":
            raise OSError("injected mirror write failure")

    writer = _mirrored(tmp_path, mirror_fault_hook=fail_mirror_write)
    with pytest.raises(OSError, match="injected mirror write failure"):
        writer.append_batch([_queued(1)])
    assert writer.durable_ack_seq == 0
    with pytest.raises(MirroredWalFailedError, match="fault-latched"):
        writer.sync()
    writer.abort()


@pytest.mark.parametrize(
    ("primary_records", "mirror_records"),
    [
        ([_queued(1)], [_queued(1), _queued(2)]),
        ([_queued(1, raw_payload='{"p":"100"}')], [_queued(1, raw_payload='{"p":"101"}')]),
    ],
    ids=("different-sequence", "different-bytes"),
)
def test_construction_rejects_roots_with_different_durable_prefixes(
    tmp_path: Path,
    primary_records: list[QueuedRawRecordV2],
    mirror_records: list[QueuedRawRecordV2],
) -> None:
    receipt = _selection_receipt()
    policy = receipt.selected_policy
    assert policy is not None
    _seed_copy(
        tmp_path / "primary",
        role="PRIMARY",
        failure_domain_id="declared-device-primary",
        records=primary_records,
        policy=policy,
        selection_receipt_sha256=receipt.sha256,
    )
    _seed_copy(
        tmp_path / "mirror",
        role="INDEPENDENT_MIRROR",
        failure_domain_id="declared-device-mirror",
        records=mirror_records,
        policy=policy,
        selection_receipt_sha256=receipt.sha256,
    )
    with pytest.raises(MirroredWalIntegrityError, match="bytes or sequences differ"):
        _mirrored(tmp_path, selection_receipt=receipt)


def test_consume_verifies_mirror_before_exposing_primary_records(tmp_path: Path) -> None:
    writer = _mirrored(tmp_path)
    writer.append_batch([_queued(1)])
    writer.sync()
    mirror_partial = tmp_path / "mirror" / "wal-00000001.partial"
    with mirror_partial.open("ab", buffering=0) as handle:
        handle.write(encode_wal_frame(_queued(2).encoded_line))
        os.fsync(handle.fileno())

    exposed: list[int] = []
    with pytest.raises(
        WalIntegrityError,
        match="active WAL bytes exceed or miss the durable ACK",
    ):
        writer.consume_durable_records(lambda seq, _line: exposed.append(seq))
    assert exposed == []
    assert writer.durable_ack_seq == 1
    writer.abort()


def test_public_prefix_proof_supports_empty_and_nonempty_dual_wal(
    tmp_path: Path,
) -> None:
    empty = _mirrored(tmp_path / "empty")
    empty_proof = empty.prove_durable_prefix_v2()
    assert empty_proof.durable_ack_seq == empty_proof.record_count == 0
    assert len(empty_proof.prefix_sha256) == 64
    assert len(empty_proof.proof_sha256) == 64
    empty.close()

    reopened_empty = _mirrored_verification_only(tmp_path / "empty")
    assert reopened_empty.prove_durable_prefix_v2() == empty_proof

    nonempty = _mirrored(tmp_path / "nonempty")
    nonempty.append_batch([_queued(1)])
    nonempty_proof = nonempty.prove_durable_prefix_v2()
    assert nonempty_proof.durable_ack_seq == nonempty_proof.record_count == 1
    assert nonempty_proof.prefix_sha256 != empty_proof.prefix_sha256
    nonempty.close()


def test_prefix_proof_rejects_unknown_schema() -> None:
    with pytest.raises(ValueError, match="unsupported mirrored WAL prefix-proof schema"):
        MirroredWalPrefixProofV2(
            durable_ack_seq=0,
            record_count=0,
            prefix_sha256="1" * 64,
            durability_binding_sha256="2" * 64,
            selection_receipt_sha256="3" * 64,
            schema_version="bogus",
        )


def test_paths_and_declared_failure_domains_must_be_distinct(tmp_path: Path) -> None:
    receipt = _selection_receipt()
    policy = receipt.selected_policy
    assert policy is not None
    common = {
        "authority": _authority(),
        "policy": policy,
        "selection_receipt": receipt,
        "primary_maximum_total_bytes": MAXIMUM_BYTES,
        "mirror_maximum_total_bytes": MAXIMUM_BYTES,
        "primary_emergency_reserve_bytes": RESERVE_BYTES,
        "mirror_emergency_reserve_bytes": RESERVE_BYTES,
        "clock_ns": _Clock(),
    }
    with pytest.raises(ValueError, match="roots must not overlap"):
        MirroredWalWriterV2(
            tmp_path / "same",
            tmp_path / "same" / "nested",
            primary_failure_domain_id="primary",
            mirror_failure_domain_id="mirror",
            **common,
        )
    with pytest.raises(ValueError, match="after normalization"):
        MirroredWalWriterV2(
            tmp_path / "primary",
            tmp_path / "mirror",
            primary_failure_domain_id="DEVICE-A",
            mirror_failure_domain_id="device-a",
            **common,
        )


def test_runtime_policy_must_match_the_bound_selection_receipt(tmp_path: Path) -> None:
    receipt = _selection_receipt(sync_ms=50, record_cap=1024)
    with pytest.raises(WalQualificationError, match="runtime WAL policy differs"):
        _mirrored(
            tmp_path,
            selection_receipt=receipt,
            policy=_policy(50, 4096),
        )


def test_dual_root_cannot_omit_or_launder_selection_receipt_hash(
    tmp_path: Path,
) -> None:
    receipt = _selection_receipt()
    policy = receipt.selected_policy
    assert policy is not None
    with pytest.raises(ValueError, match="require a canonical qualification"):
        WalWriterV2(
            tmp_path / "omitted",
            authority=_authority(),
            policy=policy,
            maximum_total_bytes=MAXIMUM_BYTES,
            emergency_reserve_bytes=RESERVE_BYTES,
            root_role="PRIMARY",
            failure_domain_id="declared-device-primary",
        )

    original = _mirrored(tmp_path / "bound", selection_receipt=receipt)
    original.abort()
    different_receipt = _selection_receipt(selection_wall_ms=WINDOW_END_MS + 1)
    assert different_receipt.selected_policy == policy
    assert different_receipt.sha256 != receipt.sha256
    with pytest.raises(WalIntegrityError, match="storage root binding differs"):
        _mirrored(tmp_path / "bound", selection_receipt=different_receipt)


def test_durable_batch_adapter_accepts_dual_wal_owner(tmp_path: Path) -> None:
    receipt = _selection_receipt(sync_ms=50, record_cap=1024)
    policy = receipt.selected_policy
    assert policy is not None
    wal = _mirrored(tmp_path / "wal", selection_receipt=receipt)
    signer = Ed25519BlockSignerV2.from_private_key_bytes(
        key_id="test-block-key",
        private_key_bytes=b"\x17" * 32,
    )
    signing_authority = BlockSigningAuthorityV2.from_public_key_bytes(
        key_id=signer.key_id,
        public_key_bytes=signer.public_key_bytes,
    )
    block_policy = BlockPolicyV2(
        qualification_id=QUALIFICATION,
        codec_candidate_id="zstd-l9-single-checksum-content-size",
        compression_level=9,
        max_uncompressed_bytes=4_194_304,
        max_linger_ms=1_000,
    )
    block_writer = GroupedBlockWriterV2(
        tmp_path / "blocks",
        authority=_authority(),
        policy=block_policy,
        signer=signer,
        signing_authority=signing_authority,
        stream_group_id="futures-depth-trade",
        segment_id="segment-1",
        maximum_total_bytes=MAXIMUM_BYTES,
        emergency_reserve_bytes=RESERVE_BYTES,
    )
    writer = DurableCaptureBatchWriterV2(
        batch_policy=BatchPolicyV2(
            max_records=policy.max_unsynced_records,
            max_encoded_bytes=policy.max_unsynced_bytes,
            max_linger_us=policy.interval_ms * 1_000,
            queue_max_events=8_192,
            queue_max_encoded_bytes=32_000_000,
            low_water_events=512,
            low_water_encoded_bytes=2_000_000,
            qualification_id=QUALIFICATION,
        ),
        wal_writer=wal,
        block_builder=GroupedBlockBuilderV2(block_policy),
        block_writer=block_writer,
        clock_ns=_Clock(),
    )
    assert writer.append_many([_queued(1), _queued(2)]) == 2
    finality = writer.finalize_through(
        requested_ingest_seq=2,
        fence_ingest_seq=2,
        fence_monotonic_ns=1_000_000_000,
    )
    assert finality.wal_durability_binding.mode == "QUALIFIED_DUAL_OWNER"
    assert tuple(
        binding.root_role for binding in finality.wal_durability_binding.root_bindings
    ) == ("PRIMARY", "INDEPENDENT_MIRROR")
    assert finality.wal_durability_binding.qualification_selection_receipt_sha256 == receipt.sha256
    assert not (finality.wal_durability_binding.physical_failure_domain_independence_verified)
    assert (
        verify_capture_finality_fence_receipt_v2(
            finality,
            wal_writer=wal,
            block_writer=block_writer,
        )
        == finality.prefix_proof_sha256
    )
    writer.close()
    wal.assert_cleanly_closed_and_current_v2()
    observed_after_close: list[int] = []
    assert wal.consume_durable_records(
        lambda ingest_seq, _line: observed_after_close.append(ingest_seq)
    ) == 2
    assert observed_after_close == [1, 2]
    assert (
        verify_capture_finality_fence_receipt_v2(
            finality,
            wal_writer=wal,
            block_writer=block_writer,
        )
        == finality.prefix_proof_sha256
    )
    [block] = verify_grouped_blocks(
        tmp_path / "blocks",
        authority=_authority(),
        policy=block_policy,
        signing_authority=signing_authority,
        stream_group_id="futures-depth-trade",
        segment_id="segment-1",
    )
    assert block.last_ingest_seq == 2


def test_aborted_dual_wal_cannot_masquerade_as_cleanly_closed_prefix(
    tmp_path: Path,
) -> None:
    writer = _mirrored(tmp_path)
    writer.append_batch([_queued(1)])
    writer.sync()
    writer.abort()

    with pytest.raises(MirroredWalFailedError, match="not cleanly closed"):
        writer.assert_cleanly_closed_and_current_v2()
    with pytest.raises(MirroredWalFailedError, match="aborted"):
        writer.assert_root_binding_current()
    with pytest.raises(MirroredWalFailedError, match="aborted"):
        writer.consume_durable_records(lambda _seq, _line: None)


def test_verification_only_reopen_is_read_only_and_does_not_claim_prior_close(
    tmp_path: Path,
) -> None:
    receipt = _selection_receipt()
    writer = _mirrored(tmp_path, selection_receipt=receipt)
    writer.append_batch([_queued(1), _queued(2)])
    writer.sync()
    writer.close()

    reopened = _mirrored_verification_only(tmp_path, selection_receipt=receipt)
    assert reopened.verification_only is True
    reopened.assert_verification_only_prefix_current_v2(
        expected_durable_ack_seq=2,
        expected_durability_binding=writer.durability_binding,
    )
    observed: list[int] = []
    assert reopened.consume_durable_records(
        lambda ingest_seq, _line: observed.append(ingest_seq)
    ) == 2
    assert observed == [1, 2]
    with pytest.raises(MirroredWalFailedError, match="not cleanly closed"):
        reopened.assert_cleanly_closed_and_current_v2()
    with pytest.raises(MirroredWalFailedError, match="closed"):
        reopened.append_batch([_queued(3)])
    with pytest.raises(MirroredWalFailedError, match="closed"):
        reopened.sync()


def test_verification_only_reopen_rejects_missing_authority_and_later_partial(
    tmp_path: Path,
) -> None:
    missing_primary = tmp_path / "missing" / "primary"
    missing_mirror = tmp_path / "missing" / "mirror"
    with pytest.raises(MirroredWalIntegrityError, match="must already exist"):
        MirroredWalWriterV2.open_verification_only_v2(
            missing_primary,
            missing_mirror,
            authority=_authority(),
            policy=_policy(),
            selection_receipt=_selection_receipt(),
            primary_maximum_total_bytes=MAXIMUM_BYTES,
            mirror_maximum_total_bytes=MAXIMUM_BYTES,
            primary_emergency_reserve_bytes=RESERVE_BYTES,
            mirror_emergency_reserve_bytes=RESERVE_BYTES,
            primary_failure_domain_id="declared-device-primary",
            mirror_failure_domain_id="declared-device-mirror",
        )
    assert not missing_primary.exists()
    assert not missing_mirror.exists()

    writer = _mirrored(tmp_path)
    writer.append_batch([_queued(1)])
    writer.sync()
    writer.close()
    reopened = _mirrored_verification_only(tmp_path)
    (tmp_path / "primary" / "wal-00000002.partial").write_bytes(b"unfinished")
    with pytest.raises(MirroredWalIntegrityError, match="unfinished partial"):
        reopened.assert_verification_only_prefix_current_v2(
            expected_durable_ack_seq=1,
            expected_durability_binding=writer.durability_binding,
        )
