from __future__ import annotations

import asyncio
import threading
from collections.abc import Callable
from dataclasses import replace
from pathlib import Path

import pytest

from signalbot.capture.writer_lease import WriterLease, WriterLeaseNotHeldError
from signalbot.r4b_v2.capture.authority import StorageRootBindingV2
from signalbot.r4b_v2.capture.batching import (
    BatchPolicyV2,
    BoundedBatchHandoffV2,
    CaptureBatchAckErrorV2,
    CaptureBatchClockErrorV2,
    CaptureFinalityFenceErrorV2,
    QueuedRawRecordV2,
)
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
from signalbot.r4b_v2.capture.models import RawRecordV2, TransportV2, VenueV2
from signalbot.r4b_v2.capture.pipeline import (
    CaptureBatchPipelineV2,
    CaptureFinalityFenceReceiptV2,
    DurableCaptureBatchWriterV2,
    replay_durable_wal_backlog_v2,
    verify_capture_finality_fence_receipt_v2,
    verify_clean_stopped_current_tail_v2,
)
from signalbot.r4b_v2.capture.wal import (
    WalAuthorityV2,
    WalDurabilityBindingV2,
    WalSyncPolicyV2,
    WalWriterV2,
    verify_wal_segments,
)

HASH = "a" * 64
QUALIFICATION = "joint-q-10ms-zstd-1.5.7-l9"
STREAM_GROUP_ID = "futures-depth-group"
SEGMENT_ID = "segment-000001"


def _signer() -> Ed25519BlockSignerV2:
    return Ed25519BlockSignerV2.from_private_key_bytes(
        key_id="writer-key-1",
        private_key_bytes=b"\x01" * 32,
    )


def _signing_authority() -> BlockSigningAuthorityV2:
    signer = _signer()
    return BlockSigningAuthorityV2.from_public_key_bytes(
        key_id=signer.key_id,
        public_key_bytes=signer.public_key_bytes,
    )


class _Clock:
    def __init__(self) -> None:
        self.value = 1_000_000_000

    def __call__(self) -> int:
        return self.value


def _authority() -> WalAuthorityV2:
    return WalAuthorityV2(
        attempt_id="attempt-1",
        protocol_sha256=HASH,
        plan_sha256="b" * 64,
        source_manifest_sha256="c" * 64,
        schema_sha256="d" * 64,
        runtime_manifest_sha256="e" * 64,
    )


def _batch_policy(**overrides: int | str) -> BatchPolicyV2:
    values: dict[str, int | str] = {
        "max_records": 10,
        "max_encoded_bytes": 100_000,
        "max_linger_us": 10_000,
        "queue_max_events": 100,
        "queue_max_encoded_bytes": 1_000_000,
        "low_water_events": 10,
        "low_water_encoded_bytes": 100_000,
        "qualification_id": QUALIFICATION,
    }
    values.update(overrides)
    return BatchPolicyV2(**values)  # type: ignore[arg-type]


def _wal_policy(**overrides: int | str) -> WalSyncPolicyV2:
    values: dict[str, int | str] = {
        "qualification_id": QUALIFICATION,
        "fsync_candidate_id": "fsync-10ms-r10",
        "interval_ms": 10,
        "max_unsynced_records": 10,
        "max_unsynced_bytes": 100_000,
        "max_record_bytes": 20_000,
        "max_segment_bytes": 1_000_000,
    }
    values.update(overrides)
    return WalSyncPolicyV2(**values)  # type: ignore[arg-type]


def _block_policy(**overrides: int | str) -> BlockPolicyV2:
    values: dict[str, int | str] = {
        "qualification_id": QUALIFICATION,
        "codec_candidate_id": "zstd-1.5.7-l9-w0-checksum-content-size",
        "compression_level": 9,
        "max_uncompressed_bytes": 4_194_304,
        "max_linger_ms": 1_000,
    }
    values.update(overrides)
    return BlockPolicyV2(**values)  # type: ignore[arg-type]


def _queued(
    ingest_seq: int,
    *,
    receipt_ns: int | None = None,
    receipt_wall_ms: int | None = None,
    raw_payload: str = '{"p":"100","q":"1"}',
    protocol_hash: str = HASH,
) -> QueuedRawRecordV2:
    monotonic_ns = 1_000_000 + ingest_seq if receipt_ns is None else receipt_ns
    record = RawRecordV2.from_payload(
        session_id="session-1",
        plan_id="plan-1",
        protocol_hash=protocol_hash,
        transport=TransportV2.WEBSOCKET,
        venue=VenueV2.USDM_FUTURES,
        route_id="futures-market",
        symbol="BTCUSDT",
        connection_id="connection-1",
        generation=1,
        frame_seq=ingest_seq,
        ingest_seq=ingest_seq,
        receipt_wall_ms=(1_000 + ingest_seq if receipt_wall_ms is None else receipt_wall_ms),
        receipt_monotonic_ns=monotonic_ns,
        raw_payload=raw_payload,
        source_logical_key=f"trade-{ingest_seq}",
    )
    return QueuedRawRecordV2.encode(
        record,
        enqueued_monotonic_ns=monotonic_ns + 1,
    )


def _durable_writer(
    tmp_path: Path,
    *,
    batch_policy: BatchPolicyV2 | None = None,
    wal_policy: WalSyncPolicyV2 | None = None,
    block_policy: BlockPolicyV2 | None = None,
    clock: _Clock | None = None,
    writer_lease: WriterLease | None = None,
    wal_fault_hook: Callable[[str], None] | None = None,
    block_fault_hook: Callable[[str], None] | None = None,
) -> DurableCaptureBatchWriterV2:
    wal_root = tmp_path / "wal"
    block_root = tmp_path / "blocks"
    selected_block_policy = block_policy or _block_policy()
    selected_clock = clock or _Clock()
    wal = WalWriterV2(
        wal_root,
        authority=_authority(),
        policy=wal_policy or _wal_policy(),
        maximum_total_bytes=8 * 1024 * 1024,
        emergency_reserve_bytes=1024,
        clock_ns=selected_clock,
        fault_hook=wal_fault_hook,
    )
    blocks = GroupedBlockWriterV2(
        block_root,
        authority=_authority(),
        policy=selected_block_policy,
        signer=_signer(),
        signing_authority=_signing_authority(),
        stream_group_id=STREAM_GROUP_ID,
        segment_id=SEGMENT_ID,
        maximum_total_bytes=8 * 1024 * 1024,
        emergency_reserve_bytes=1024,
        fault_hook=block_fault_hook,
    )
    return DurableCaptureBatchWriterV2(
        batch_policy=batch_policy or _batch_policy(),
        wal_writer=wal,
        block_builder=GroupedBlockBuilderV2(selected_block_policy),
        block_writer=blocks,
        clock_ns=selected_clock,
        writer_lease=writer_lease,
    )


def _verify_blocks(root: Path):  # type: ignore[no-untyped-def]
    return verify_grouped_blocks(
        root,
        authority=_authority(),
        policy=_block_policy(),
        signing_authority=_signing_authority(),
        stream_group_id=STREAM_GROUP_ID,
        segment_id=SEGMENT_ID,
    )


def test_exact_wal_ack_precedes_queue_ack_and_close_commits_tail(tmp_path: Path) -> None:
    writer = _durable_writer(tmp_path)
    records = [_queued(1), _queued(2)]
    assert writer.append_many(records) == 2
    assert writer.wal_writer.durable_ack_seq == 2
    assert not list((tmp_path / "blocks").glob("*.manifest.json"))

    writer.close()
    [wal_manifest] = verify_wal_segments(
        tmp_path / "wal",
        authority=_authority(),
        policy=_wal_policy(),
    )
    [block_manifest] = _verify_blocks(tmp_path / "blocks")
    assert wal_manifest.last_ingest_seq == block_manifest.last_ingest_seq == 2


def test_released_bound_writer_lease_blocks_next_append(tmp_path: Path) -> None:
    lease = WriterLease.acquire(tmp_path)
    writer = _durable_writer(tmp_path, writer_lease=lease)
    lease.release()

    with pytest.raises(WriterLeaseNotHeldError, match="released"):
        writer.append_many([_queued(1)])

    assert writer.wal_writer.next_ingest_seq == 1
    writer.abort()


def test_released_bound_writer_lease_blocks_next_finality_fence(
    tmp_path: Path,
) -> None:
    lease = WriterLease.acquire(tmp_path)
    writer = _durable_writer(tmp_path, writer_lease=lease)
    writer.append_many([_queued(1)])
    lease.release()

    with pytest.raises(WriterLeaseNotHeldError, match="released"):
        writer.finalize_through(
            requested_ingest_seq=1,
            fence_ingest_seq=1,
            fence_monotonic_ns=1_000_001,
        )

    assert writer.block_writer.next_ingest_seq == 1
    writer.abort()


def test_concurrent_release_waits_for_active_wal_append_operation(
    tmp_path: Path,
) -> None:
    lease = WriterLease.acquire(tmp_path)
    mutation_entered = threading.Event()
    allow_mutation = threading.Event()

    def fault_hook(point: str) -> None:
        if point == "before_batch_write":
            mutation_entered.set()
            if not allow_mutation.wait(timeout=2):
                raise TimeoutError("test did not release blocked WAL mutation")

    writer = _durable_writer(
        tmp_path,
        writer_lease=lease,
        wal_fault_hook=fault_hook,
    )
    append_errors: list[BaseException] = []
    release_errors: list[BaseException] = []
    release_started = threading.Event()
    release_done = threading.Event()

    def append() -> None:
        try:
            writer.append_many([_queued(1)])
        except BaseException as exc:
            append_errors.append(exc)

    def release() -> None:
        release_started.set()
        try:
            lease.release()
        except BaseException as exc:
            release_errors.append(exc)
        finally:
            release_done.set()

    append_thread = threading.Thread(target=append)
    append_thread.start()
    assert mutation_entered.wait(timeout=1)
    release_thread = threading.Thread(target=release)
    release_thread.start()
    assert release_started.wait(timeout=1)
    assert not release_done.wait(timeout=0.05)

    allow_mutation.set()
    append_thread.join(timeout=2)
    release_thread.join(timeout=2)

    assert append_errors == []
    assert release_errors == []
    assert release_done.is_set()
    assert writer.wal_writer.next_ingest_seq == 2
    writer.abort()


def test_concurrent_release_waits_for_active_finality_block_commit(
    tmp_path: Path,
) -> None:
    lease = WriterLease.acquire(tmp_path)
    mutation_entered = threading.Event()
    allow_mutation = threading.Event()

    def fault_hook(point: str) -> None:
        if point == "before_block_write":
            mutation_entered.set()
            if not allow_mutation.wait(timeout=2):
                raise TimeoutError("test did not release blocked block mutation")

    clock = _Clock()
    writer = _durable_writer(
        tmp_path,
        writer_lease=lease,
        block_fault_hook=fault_hook,
        clock=clock,
    )
    writer.append_many([_queued(1)])
    finality_errors: list[BaseException] = []
    release_errors: list[BaseException] = []
    release_started = threading.Event()
    release_done = threading.Event()

    def finalize() -> None:
        try:
            writer.finalize_through(
                requested_ingest_seq=1,
                fence_ingest_seq=1,
                fence_monotonic_ns=clock.value,
            )
        except BaseException as exc:
            finality_errors.append(exc)

    def release() -> None:
        release_started.set()
        try:
            lease.release()
        except BaseException as exc:
            release_errors.append(exc)
        finally:
            release_done.set()

    finality_thread = threading.Thread(target=finalize)
    finality_thread.start()
    assert mutation_entered.wait(timeout=1)
    release_thread = threading.Thread(target=release)
    release_thread.start()
    assert release_started.wait(timeout=1)
    assert not release_done.wait(timeout=0.05)

    allow_mutation.set()
    finality_thread.join(timeout=2)
    release_thread.join(timeout=2)

    assert finality_errors == []
    assert release_errors == []
    assert release_done.is_set()
    assert writer.block_writer.next_ingest_seq == 2
    writer.abort()


def test_finality_fence_commits_exact_prefix_and_writer_remains_live(
    tmp_path: Path,
) -> None:
    clock = _Clock()
    writer = _durable_writer(tmp_path, clock=clock)
    writer.append_many([_queued(1), _queued(2)])

    receipt = writer.finalize_through(
        requested_ingest_seq=2,
        fence_ingest_seq=2,
        fence_monotonic_ns=clock.value,
    )

    [first_manifest] = _verify_blocks(tmp_path / "blocks")
    assert receipt.requested_ingest_seq == 2
    assert receipt.fence_ingest_seq == 2
    assert receipt.wal_durable_ack_seq == 2
    assert receipt.finalized_block_tail_ingest_seq == 2
    assert receipt.final_block_sequence == first_manifest.block_sequence
    assert receipt.final_block_hash == first_manifest.block_hash
    assert len(receipt.sha256) == 64
    assert len(receipt.prefix_proof_sha256) == 64

    clock.value += 1
    assert writer.append_many([_queued(3, receipt_ns=clock.value)]) == 3
    second = writer.finalize_through(
        requested_ingest_seq=3,
        fence_ingest_seq=3,
        fence_monotonic_ns=clock.value,
    )
    assert second.final_block_sequence == 2
    assert second.finalized_block_tail_ingest_seq == 3
    assert second.final_block_hash != receipt.final_block_hash
    assert receipt.wal_durability_binding.mode == "SINGLE_ROOT"
    assert len(receipt.wal_durability_binding.root_bindings) == 1
    assert receipt.grouped_block_root_binding.storage_kind == "GROUPED_BLOCK"
    assert (
        verify_capture_finality_fence_receipt_v2(
            receipt,
            wal_writer=writer.wal_writer,
            block_writer=writer.block_writer,
        )
        == receipt.prefix_proof_sha256
    )
    writer.close()


@pytest.mark.asyncio
async def test_pipeline_orders_successor_after_the_exact_finality_fence(
    tmp_path: Path,
) -> None:
    clock = _Clock()
    writer = _durable_writer(tmp_path, clock=clock)
    handoff = BoundedBatchHandoffV2(_batch_policy(), monotonic_ns=clock)
    pipeline = CaptureBatchPipelineV2(handoff, writer)
    pipeline.start()
    pipeline.offer(_queued(1).record)

    first_waiter = asyncio.create_task(pipeline.finalize_through(1, timeout_seconds=2))
    await asyncio.sleep(0)
    assert handoff.finality_fence_in_flight

    clock.value += 1
    pipeline.offer(_queued(2, receipt_ns=clock.value).record)
    first = await first_waiter
    second = await pipeline.finalize_through(2, timeout_seconds=2)
    await pipeline.stop()

    assert first.fence_ingest_seq == 1
    assert second.fence_ingest_seq == 2
    manifests = _verify_blocks(tmp_path / "blocks")
    assert [manifest.last_ingest_seq for manifest in manifests] == [1, 2]


@pytest.mark.asyncio
async def test_clean_stop_verifier_binds_exact_receipt_and_unextended_tail(
    tmp_path: Path,
) -> None:
    clock = _Clock()
    writer = _durable_writer(tmp_path, clock=clock)
    handoff = BoundedBatchHandoffV2(_batch_policy(), monotonic_ns=clock)
    pipeline = CaptureBatchPipelineV2(handoff, writer)
    pipeline.start()
    pipeline.offer(_queued(1).record)
    historical = await pipeline.finalize_through(1, timeout_seconds=2)
    clock.value += 1
    pipeline.offer(_queued(2, receipt_ns=clock.value).record)

    receipt = await pipeline.finalize_current_tail_and_stop(timeout_seconds=2)

    assert receipt.fence_ingest_seq == 2
    assert writer.closed
    assert (
        verify_clean_stopped_current_tail_v2(receipt, pipeline=pipeline)
        == receipt.prefix_proof_sha256
    )
    with pytest.raises(CaptureFinalityFenceErrorV2, match="internally completed"):
        verify_clean_stopped_current_tail_v2(historical, pipeline=pipeline)
    with pytest.raises(CaptureFinalityFenceErrorV2, match="internally completed"):
        verify_clean_stopped_current_tail_v2(
            replace(receipt, exact_prefix_sha256="0" * 64),
            pipeline=pipeline,
        )


def test_repeated_finality_fence_has_a_stable_prefix_proof_identity(
    tmp_path: Path,
) -> None:
    clock = _Clock()
    writer = _durable_writer(tmp_path, clock=clock)
    writer.append_many([_queued(1)])
    first = writer.finalize_through(
        requested_ingest_seq=1,
        fence_ingest_seq=1,
        fence_monotonic_ns=clock.value,
    )
    clock.value += 1
    second = writer.finalize_through(
        requested_ingest_seq=1,
        fence_ingest_seq=1,
        fence_monotonic_ns=clock.value,
    )

    assert second.final_block_hash == first.final_block_hash
    assert second.prefix_proof_sha256 == first.prefix_proof_sha256
    assert second.sha256 != first.sha256
    assert len(_verify_blocks(tmp_path / "blocks")) == 1
    writer.close()


def test_finality_fence_rejects_noncausal_or_nonexact_boundaries(
    tmp_path: Path,
) -> None:
    clock = _Clock()
    writer = _durable_writer(tmp_path, clock=clock)
    writer.append_many([_queued(1, receipt_ns=1_500_000_000)])
    clock.value = 2_000_000_000

    with pytest.raises(ValueError, match="equal the requested"):
        writer.finalize_through(
            requested_ingest_seq=2,
            fence_ingest_seq=1,
            fence_monotonic_ns=2_000_000_000,
        )
    with pytest.raises(CaptureBatchAckErrorV2, match="durable WAL tail"):
        writer.finalize_through(
            requested_ingest_seq=2,
            fence_ingest_seq=2,
            fence_monotonic_ns=2_000_000_000,
        )
    with pytest.raises(CaptureBatchClockErrorV2, match="precedes"):
        writer.finalize_through(
            requested_ingest_seq=1,
            fence_ingest_seq=1,
            fence_monotonic_ns=1_499_999_999,
        )

    assert not list((tmp_path / "blocks").glob("*.manifest.json"))
    writer.abort()


def test_empty_finality_rejection_leaves_writer_usable(tmp_path: Path) -> None:
    clock = _Clock()
    writer = _durable_writer(tmp_path, clock=clock)

    with pytest.raises(CaptureBatchAckErrorV2, match="durable WAL tail"):
        writer.finalize_through(
            requested_ingest_seq=1,
            fence_ingest_seq=1,
            fence_monotonic_ns=clock.value,
        )
    assert writer.wal_writer.durable_ack_seq == 0
    assert not list((tmp_path / "blocks").glob("*.manifest.json"))

    writer.append_many([_queued(1)])
    receipt = writer.finalize_through(
        requested_ingest_seq=1,
        fence_ingest_seq=1,
        fence_monotonic_ns=clock.value,
    )
    assert receipt.fence_ingest_seq == 1
    writer.close()


def test_caller_cannot_future_date_the_ordered_fence_clock(tmp_path: Path) -> None:
    clock = _Clock()
    writer = _durable_writer(tmp_path, clock=clock)
    writer.append_many([_queued(1)])

    with pytest.raises(CaptureBatchClockErrorV2, match="writer clock precedes"):
        writer.finalize_through(
            requested_ingest_seq=1,
            fence_ingest_seq=1,
            fence_monotonic_ns=clock.value + 1,
        )
    assert not list((tmp_path / "blocks").glob("*.manifest.json"))

    receipt = writer.finalize_through(
        requested_ingest_seq=1,
        fence_ingest_seq=1,
        fence_monotonic_ns=clock.value,
    )
    assert receipt.writer_observed_monotonic_ns == clock.value
    writer.close()


def test_artifact_verifier_rejects_a_shape_valid_forged_receipt(
    tmp_path: Path,
) -> None:
    writer = _durable_writer(tmp_path)
    writer.append_many([_queued(1)])
    receipt = writer.finalize_through(
        requested_ingest_seq=1,
        fence_ingest_seq=1,
        fence_monotonic_ns=1_000_000_000,
    )
    forged = replace(receipt, exact_prefix_sha256="0" * 64)

    with pytest.raises(CaptureBatchAckErrorV2, match="exact-prefix digest"):
        verify_capture_finality_fence_receipt_v2(
            forged,
            wal_writer=writer.wal_writer,
            block_writer=writer.block_writer,
        )
    writer.close()


@pytest.mark.parametrize("root_name", ["wal", "blocks"])
def test_current_root_binding_loss_blocks_finality_before_block_commit(
    tmp_path: Path,
    root_name: str,
) -> None:
    writer = _durable_writer(tmp_path)
    writer.append_many([_queued(1)])
    (tmp_path / root_name / "storage-root-binding.json").unlink()

    with pytest.raises(RuntimeError, match="binding is missing"):
        writer.finalize_through(
            requested_ingest_seq=1,
            fence_ingest_seq=1,
            fence_monotonic_ns=1_000_000_000,
        )

    assert not list((tmp_path / "blocks").glob("*.manifest.json"))
    writer.abort()


@pytest.mark.parametrize("root_name", ["wal", "blocks"])
def test_recreated_storage_root_with_copied_binding_cannot_return_ack(
    tmp_path: Path,
    root_name: str,
) -> None:
    writer = _durable_writer(tmp_path)
    root = tmp_path / root_name
    moved = tmp_path / f"{root_name}-moved"
    binding = root.joinpath("storage-root-binding.json").read_bytes()
    root.rename(moved)
    root.mkdir()
    root.joinpath("storage-root-binding.json").write_bytes(binding)

    with pytest.raises(RuntimeError, match="identity differs"):
        writer.append_many([_queued(1)])

    assert writer.wal_writer.durable_ack_seq == 0
    assert writer.wal_writer.next_ingest_seq == 1
    assert writer.block_writer.next_ingest_seq == 1
    writer.abort()


def test_wal_record_policy_cannot_admit_a_grouped_block_poison_tail(
    tmp_path: Path,
) -> None:
    with pytest.raises(ValueError, match="max_record_bytes"):
        _durable_writer(
            tmp_path / "too-large",
            wal_policy=_wal_policy(
                max_record_bytes=4_194_305,
                max_segment_bytes=8_388_608,
            ),
        )

    boundary = _durable_writer(
        tmp_path / "boundary",
        wal_policy=_wal_policy(
            max_record_bytes=4_194_304,
            max_segment_bytes=8_388_608,
        ),
    )
    boundary.abort()


@pytest.mark.parametrize(
    ("records", "message"),
    [
        (
            (
                _queued(1, receipt_ns=100, receipt_wall_ms=100),
                _queued(2, receipt_ns=99, receipt_wall_ms=101),
            ),
            "monotonic time moved backwards",
        ),
        (
            (
                _queued(1, receipt_ns=100, receipt_wall_ms=101),
                _queued(2, receipt_ns=101, receipt_wall_ms=100),
            ),
            "wall time moved backwards",
        ),
    ],
)
def test_block_incompatible_batch_is_rejected_before_any_wal_append(
    tmp_path: Path,
    records: tuple[QueuedRawRecordV2, ...],
    message: str,
) -> None:
    writer = _durable_writer(tmp_path)

    with pytest.raises(BlockIntegrityError, match=message):
        writer.append_many(records)

    assert writer.wal_writer.durable_ack_seq == 0
    assert writer.wal_writer.next_ingest_seq == 1
    assert not list((tmp_path / "wal").glob("*.partial"))
    writer.abort()


@pytest.mark.asyncio
async def test_protocol_authority_mismatch_is_the_first_fatal_before_wal(
    tmp_path: Path,
) -> None:
    clock = _Clock()
    writer = _durable_writer(tmp_path, clock=clock)
    handoff = BoundedBatchHandoffV2(_batch_policy(), monotonic_ns=clock)
    pipeline = CaptureBatchPipelineV2(handoff, writer)
    pipeline.start()
    pipeline.offer(_queued(1, protocol_hash="f" * 64).record)

    await asyncio.wait_for(pipeline.wait_failed(), timeout=2)
    failure = handoff.fatal_state.failure
    assert failure is not None
    original = failure.cause
    assert isinstance(original, BlockIntegrityError)
    assert "protocol hash differs" in str(original)
    assert failure.failing_ingest_seq == 1

    handoff.fail_consumer(
        RuntimeError("later synthetic failure"),
        failing_ingest_seq=None,
    )
    assert handoff.fatal_state.failure is failure
    with pytest.raises(BlockIntegrityError) as captured:
        await pipeline.stop()

    assert captured.value is original
    assert writer.wal_writer.durable_ack_seq == 0
    assert writer.wal_writer.next_ingest_seq == 1
    assert not list((tmp_path / "wal").glob("*.partial"))
    assert not list((tmp_path / "blocks").glob("*.manifest.json"))


def test_post_fence_receipt_uses_data_order_not_the_later_operation_clock(
    tmp_path: Path,
) -> None:
    clock = _Clock()
    clock.value = 300
    writer = _durable_writer(tmp_path, clock=clock)
    writer.append_many([_queued(1, receipt_ns=100, receipt_wall_ms=100)])
    first = writer.finalize_through(
        requested_ingest_seq=1,
        fence_ingest_seq=1,
        fence_monotonic_ns=300,
    )

    assert writer.append_many([_queued(2, receipt_ns=200, receipt_wall_ms=101)]) == 2
    clock.value = 400
    second = writer.finalize_through(
        requested_ingest_seq=2,
        fence_ingest_seq=2,
        fence_monotonic_ns=400,
    )

    manifests = _verify_blocks(tmp_path / "blocks")
    assert [manifest.last_receipt_monotonic_ns for manifest in manifests] == [100, 200]
    assert first.target_last_receipt_monotonic_ns == 100
    assert second.target_last_receipt_monotonic_ns == 200
    assert (
        verify_capture_finality_fence_receipt_v2(
            first,
            wal_writer=writer.wal_writer,
            block_writer=writer.block_writer,
        )
        == first.prefix_proof_sha256
    )
    assert (
        verify_capture_finality_fence_receipt_v2(
            second,
            wal_writer=writer.wal_writer,
            block_writer=writer.block_writer,
        )
        == second.prefix_proof_sha256
    )
    writer.close()


def test_post_fence_record_receipt_regression_is_rejected_before_wal(
    tmp_path: Path,
) -> None:
    clock = _Clock()
    clock.value = 300
    writer = _durable_writer(tmp_path, clock=clock)
    writer.append_many([_queued(1, receipt_ns=100, receipt_wall_ms=100)])
    writer.finalize_through(
        requested_ingest_seq=1,
        fence_ingest_seq=1,
        fence_monotonic_ns=300,
    )

    with pytest.raises(BlockIntegrityError, match="monotonic time moved backwards"):
        writer.append_many([_queued(2, receipt_ns=99, receipt_wall_ms=101)])

    assert writer.wal_writer.durable_ack_seq == 1
    assert writer.wal_writer.next_ingest_seq == 2
    assert [manifest.last_ingest_seq for manifest in _verify_blocks(tmp_path / "blocks")] == [1]
    writer.abort()


def test_finality_receipt_rejects_a_false_joint_tail() -> None:
    values = {
        "authority_sha256": "a" * 64,
        "attempt_id": "attempt-1",
        "qualification_id": QUALIFICATION,
        "requested_ingest_seq": 1,
        "fence_ingest_seq": 2,
        "fence_monotonic_ns": 10,
        "writer_observed_monotonic_ns": 10,
        "wal_durable_ack_seq": 1,
        "finalized_block_tail_ingest_seq": 2,
        "durable_record_count": 2,
        "final_block_sequence": 1,
        "final_block_hash": "b" * 64,
        "final_block_manifest_sha256": "c" * 64,
        "final_block_container_sha256": "d" * 64,
        "exact_prefix_sha256": "e" * 64,
        "wal_durability_binding": WalDurabilityBindingV2(
            mode="SINGLE_ROOT",
            root_bindings=(
                StorageRootBindingV2(
                    storage_kind="WAL",
                    root_role="PROVISIONAL_SINGLE",
                    failure_domain_id="local-provisional",
                    authority_sha256="a" * 64,
                    contract_sha256="f" * 64,
                ),
            ),
            qualification_selection_receipt_sha256=None,
            physical_failure_domain_independence_verified=False,
        ),
        "grouped_block_root_binding": StorageRootBindingV2(
            storage_kind="GROUPED_BLOCK",
            root_role="PROVISIONAL_SINGLE",
            failure_domain_id="local-provisional",
            authority_sha256="a" * 64,
            contract_sha256="1" * 64,
        ),
        "block_signing_authority_sha256": "2" * 64,
        "target_last_receipt_wall_ms": 1,
        "target_last_receipt_monotonic_ns": 1,
        "stream_group_id": STREAM_GROUP_ID,
        "segment_id": SEGMENT_ID,
    }
    with pytest.raises(ValueError, match="WAL ACK"):
        CaptureFinalityFenceReceiptV2(**values)  # type: ignore[arg-type]


def test_receipt_clock_makes_block_boundaries_independent_of_writer_clock(
    tmp_path: Path,
) -> None:
    clock = _Clock()
    clock.value = 99_000_000_000
    writer = _durable_writer(tmp_path, clock=clock)
    writer.append_many([_queued(1, receipt_ns=1), _queued(2, receipt_ns=1_000_000_001)])
    writer.close()
    manifests = _verify_blocks(tmp_path / "blocks")
    assert [manifest.record_count for manifest in manifests] == [1, 1]


@pytest.mark.parametrize(
    ("batch_overrides", "wal_overrides", "message"),
    [
        ({"qualification_id": "other"}, {}, "qualification IDs"),
        ({"max_records": 9}, {}, "record candidates"),
        ({"max_encoded_bytes": 99_999}, {}, "byte candidates"),
        ({"max_linger_us": 50_000}, {}, "fsync interval"),
    ],
)
def test_unmatched_joint_candidate_is_rejected(
    tmp_path: Path,
    batch_overrides: dict[str, int | str],
    wal_overrides: dict[str, int | str],
    message: str,
) -> None:
    with pytest.raises(ValueError, match=message):
        _durable_writer(
            tmp_path,
            batch_policy=_batch_policy(**batch_overrides),
            wal_policy=_wal_policy(**wal_overrides),
        )


def test_existing_durable_wal_backlog_requires_explicit_replay(tmp_path: Path) -> None:
    clock = _Clock()
    wal = WalWriterV2(
        tmp_path / "wal",
        authority=_authority(),
        policy=_wal_policy(),
        maximum_total_bytes=8 * 1024 * 1024,
        emergency_reserve_bytes=1024,
        clock_ns=clock,
    )
    wal.append_batch([_queued(1)])
    wal.sync()
    wal.abort()

    with pytest.raises(CaptureBatchAckErrorV2, match="explicit grouped-block replay"):
        _durable_writer(tmp_path, clock=clock)


def test_abort_preserves_wal_partial_without_false_block_commit(tmp_path: Path) -> None:
    writer = _durable_writer(tmp_path)
    writer.append_many([_queued(1)])
    writer.abort()
    assert (tmp_path / "wal" / "wal-00000001.partial").exists()
    assert not list((tmp_path / "blocks").glob("*.manifest.json"))


def test_explicit_replay_rebuilds_only_missing_durable_wal_suffix(
    tmp_path: Path,
) -> None:
    original = _durable_writer(tmp_path)
    original.append_many([_queued(1), _queued(2)])
    original.abort()

    clock = _Clock()
    wal = WalWriterV2(
        tmp_path / "wal",
        authority=_authority(),
        policy=_wal_policy(),
        maximum_total_bytes=8 * 1024 * 1024,
        emergency_reserve_bytes=1024,
        clock_ns=clock,
    )
    blocks = GroupedBlockWriterV2(
        tmp_path / "blocks",
        authority=_authority(),
        policy=_block_policy(),
        signer=_signer(),
        signing_authority=_signing_authority(),
        stream_group_id=STREAM_GROUP_ID,
        segment_id=SEGMENT_ID,
        maximum_total_bytes=8 * 1024 * 1024,
        emergency_reserve_bytes=1024,
    )
    receipt = replay_durable_wal_backlog_v2(
        wal_writer=wal,
        block_writer=blocks,
    )
    assert receipt.first_replayed_ingest_seq == 1
    assert receipt.last_replayed_ingest_seq == 2
    assert receipt.replayed_record_count == 2
    assert receipt.final_block_hash == blocks.last_block_hash

    resumed = DurableCaptureBatchWriterV2(
        batch_policy=_batch_policy(),
        wal_writer=wal,
        block_builder=GroupedBlockBuilderV2(_block_policy()),
        block_writer=blocks,
        clock_ns=clock,
    )
    assert resumed.append_many([_queued(3)]) == 3
    resumed.close()
    block_manifests = _verify_blocks(tmp_path / "blocks")
    assert block_manifests[-1].last_ingest_seq == 3


def test_replay_is_noop_when_block_prefix_already_matches_wal(tmp_path: Path) -> None:
    writer = _durable_writer(tmp_path)
    writer.append_many([_queued(1)])
    writer.close()

    wal = WalWriterV2(
        tmp_path / "wal",
        authority=_authority(),
        policy=_wal_policy(),
        maximum_total_bytes=8 * 1024 * 1024,
        emergency_reserve_bytes=1024,
        clock_ns=_Clock(),
    )
    blocks = GroupedBlockWriterV2(
        tmp_path / "blocks",
        authority=_authority(),
        policy=_block_policy(),
        signer=_signer(),
        signing_authority=_signing_authority(),
        stream_group_id=STREAM_GROUP_ID,
        segment_id=SEGMENT_ID,
        maximum_total_bytes=8 * 1024 * 1024,
        emergency_reserve_bytes=1024,
    )
    receipt = replay_durable_wal_backlog_v2(
        wal_writer=wal,
        block_writer=blocks,
    )
    assert receipt.replayed_record_count == 0
    assert receipt.first_new_block_sequence is None
    wal.close()


def test_replay_rejects_same_sequence_with_different_wal_and_block_bytes(
    tmp_path: Path,
) -> None:
    wal = WalWriterV2(
        tmp_path / "wal",
        authority=_authority(),
        policy=_wal_policy(),
        maximum_total_bytes=8 * 1024 * 1024,
        emergency_reserve_bytes=1024,
        clock_ns=_Clock(),
    )
    wal.append_batch([_queued(1, raw_payload='{"p":"100"}')])
    wal.sync()
    wal.close()

    policy = _block_policy()
    blocks = GroupedBlockWriterV2(
        tmp_path / "blocks",
        authority=_authority(),
        policy=policy,
        signer=_signer(),
        signing_authority=_signing_authority(),
        stream_group_id=STREAM_GROUP_ID,
        segment_id=SEGMENT_ID,
        maximum_total_bytes=8 * 1024 * 1024,
        emergency_reserve_bytes=1024,
    )
    builder = GroupedBlockBuilderV2(policy)
    assert not builder.offer(
        _queued(1, raw_payload='{"p":"101"}'),
        now_ns=1_000_001,
    )
    tail = builder.flush_tail(now_ns=1_000_002)
    assert tail is not None
    blocks.commit(tail)

    with pytest.raises(CaptureBatchAckErrorV2, match="bytes differ"):
        replay_durable_wal_backlog_v2(wal_writer=wal, block_writer=blocks)
