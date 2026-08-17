from __future__ import annotations

import asyncio
import hashlib
import logging
import math
import re
import struct
import time
from collections.abc import Callable, Sequence
from concurrent.futures import ThreadPoolExecutor
from contextlib import AbstractContextManager, nullcontext
from dataclasses import asdict, dataclass
from functools import partial
from typing import Protocol

from signalbot.capture.writer_lease import WriterLease
from signalbot.r4b_v2.canonical import canonical_json_line
from signalbot.r4b_v2.capture.authority import (
    StorageRootBindingV2,
    assert_storage_root_binding_v2,
)
from signalbot.r4b_v2.capture.batching import (
    BatchDrainerV2,
    BatchPolicyV2,
    BatchTerminalV2,
    BoundedBatchHandoffV2,
    CaptureBatchAckErrorV2,
    CaptureBatchClockErrorV2,
    CaptureBatchClosedV2,
    CaptureBatchV2,
    CaptureFinalityFenceErrorV2,
    CaptureFinalityFenceRequestV2,
    CaptureQueueAdmissionReceiptV2,
    QueuedRawRecordV2,
)
from signalbot.r4b_v2.capture.blocks import (
    BlockCapacityError,
    BlockIntegrityError,
    GroupedBlockBuilderV2,
    GroupedBlockWriterV2,
    parse_raw_record_line_v2,
    verify_grouped_blocks,
)
from signalbot.r4b_v2.capture.mirrored_wal import DurableWalWriterProtocolV2
from signalbot.r4b_v2.capture.models import RawRecordV2
from signalbot.r4b_v2.capture.telemetry import CaptureHealthSnapshotV2
from signalbot.r4b_v2.capture.wal import WalDurabilityBindingV2

LOGGER = logging.getLogger(__name__)
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_FINALITY_RECEIPT_DOMAIN = b"R4B_V2_CAPTURE_FINALITY_FENCE_RECEIPT\0"
_FINALITY_PREFIX_PROOF_DOMAIN = b"R4B_V2_CAPTURE_FINALITY_PREFIX_PROOF\0"


class CaptureFinalityFenceTimeoutV2(CaptureFinalityFenceErrorV2):
    """Raised when an ordered fence misses the caller's bounded wait."""


@dataclass(frozen=True, slots=True)
class CaptureFinalityFenceReceiptV2:
    """Root-bound proof that one ordered ingress prefix is block-finalized."""

    authority_sha256: str
    attempt_id: str
    qualification_id: str
    requested_ingest_seq: int
    fence_ingest_seq: int
    fence_monotonic_ns: int
    writer_observed_monotonic_ns: int
    wal_durable_ack_seq: int
    finalized_block_tail_ingest_seq: int
    durable_record_count: int
    exact_prefix_sha256: str
    wal_durability_binding: WalDurabilityBindingV2
    grouped_block_root_binding: StorageRootBindingV2
    block_signing_authority_sha256: str
    final_block_sequence: int
    final_block_hash: str
    final_block_manifest_sha256: str
    final_block_container_sha256: str
    target_last_receipt_wall_ms: int
    target_last_receipt_monotonic_ns: int
    stream_group_id: str
    segment_id: str
    schema_version: str = "r4b_v2_capture_finality_fence_receipt_v1"

    def __post_init__(self) -> None:
        for field_name in (
            "authority_sha256",
            "final_block_hash",
            "final_block_manifest_sha256",
            "final_block_container_sha256",
            "exact_prefix_sha256",
            "block_signing_authority_sha256",
        ):
            value = getattr(self, field_name)
            if not isinstance(value, str) or _SHA256_RE.fullmatch(value) is None:
                raise ValueError(f"{field_name} must be a lowercase SHA-256 digest")
        for field_name in (
            "requested_ingest_seq",
            "fence_ingest_seq",
            "wal_durable_ack_seq",
            "finalized_block_tail_ingest_seq",
            "durable_record_count",
            "final_block_sequence",
        ):
            value = getattr(self, field_name)
            if type(value) is not int or value < 1:
                raise ValueError(f"{field_name} must be a positive integer")
        if (
            self.requested_ingest_seq != self.fence_ingest_seq
            or self.wal_durable_ack_seq != self.fence_ingest_seq
            or self.finalized_block_tail_ingest_seq != self.fence_ingest_seq
        ):
            raise ValueError("finality fence requires requested == fence == WAL ACK == block tail")
        if self.final_block_sequence > self.finalized_block_tail_ingest_seq:
            raise ValueError("final block sequence cannot exceed its ingest tail")
        if self.durable_record_count != self.fence_ingest_seq:
            raise ValueError("durable record count must equal the one-based fence tail")
        if type(self.fence_monotonic_ns) is not int or self.fence_monotonic_ns < 0:
            raise ValueError("fence_monotonic_ns must be a nonnegative integer")
        if (
            type(self.writer_observed_monotonic_ns) is not int
            or self.writer_observed_monotonic_ns < self.fence_monotonic_ns
        ):
            raise ValueError("writer_observed_monotonic_ns cannot precede the ordered fence")
        for field_name in (
            "target_last_receipt_wall_ms",
            "target_last_receipt_monotonic_ns",
        ):
            value = getattr(self, field_name)
            if type(value) is not int or value < 0:
                raise ValueError(f"{field_name} must be a nonnegative integer")
        if self.fence_monotonic_ns < self.target_last_receipt_monotonic_ns:
            raise ValueError("finality fence cannot precede its target receipt")
        for value, field_name in (
            (self.attempt_id, "attempt_id"),
            (self.qualification_id, "qualification_id"),
            (self.stream_group_id, "stream_group_id"),
            (self.segment_id, "segment_id"),
        ):
            if (
                not isinstance(value, str)
                or not value
                or value.strip() != value
                or len(value) > 256
                or any(character in value for character in "\r\n\x00")
            ):
                raise ValueError(f"{field_name} must be a bounded normalized identity")
        if not isinstance(self.wal_durability_binding, WalDurabilityBindingV2):
            raise ValueError("wal_durability_binding must be a canonical WAL binding")
        if any(
            binding.authority_sha256 != self.authority_sha256
            for binding in self.wal_durability_binding.root_bindings
        ):
            raise ValueError("WAL root authority differs from the finality receipt")
        if not isinstance(self.grouped_block_root_binding, StorageRootBindingV2):
            raise ValueError("grouped_block_root_binding must be a StorageRootBindingV2")
        if (
            self.grouped_block_root_binding.storage_kind != "GROUPED_BLOCK"
            or self.grouped_block_root_binding.authority_sha256 != self.authority_sha256
        ):
            raise ValueError("grouped-block root binding differs from the finality authority")
        if self.schema_version != "r4b_v2_capture_finality_fence_receipt_v1":
            raise ValueError("unsupported capture finality fence receipt schema")

    @property
    def sha256(self) -> str:
        return hashlib.sha256(_FINALITY_RECEIPT_DOMAIN + canonical_json_line(self)).hexdigest()

    @property
    def grouped_block_root_binding_sha256(self) -> str:
        return hashlib.sha256(
            canonical_json_line(asdict(self.grouped_block_root_binding))
        ).hexdigest()

    @property
    def prefix_proof_sha256(self) -> str:
        """Return a repeat-stable identity for the finalized data prefix."""

        document = asdict(self)
        del document["fence_monotonic_ns"]
        del document["writer_observed_monotonic_ns"]
        return hashlib.sha256(
            _FINALITY_PREFIX_PROOF_DOMAIN + canonical_json_line(document)
        ).hexdigest()


class CaptureBatchWriterV2(Protocol):
    """Durable batch boundary required by the disconnected V2 pipeline."""

    def append_many(self, records: Sequence[QueuedRawRecordV2]) -> int:
        """Persist a contiguous batch and return its exact durable tail sequence."""
        ...

    def finalize_through(
        self,
        *,
        requested_ingest_seq: int,
        fence_ingest_seq: int,
        fence_monotonic_ns: int,
    ) -> CaptureFinalityFenceReceiptV2:
        """Finalize the exact ordered prefix without closing the writer."""
        ...

    def close(self) -> None: ...

    def abort(self) -> None: ...


class _DigestV2(Protocol):
    def update(self, value: bytes, /) -> None: ...


@dataclass(frozen=True, slots=True)
class BlockBacklogReplayV2:
    wal_authority_sha256: str
    first_replayed_ingest_seq: int | None
    last_replayed_ingest_seq: int | None
    replayed_record_count: int
    first_new_block_sequence: int | None
    last_new_block_sequence: int | None
    final_block_hash: str | None
    reason: str = "durable_wal_to_grouped_block_recovery"
    schema_version: str = "r4b_v2_block_backlog_replay_v1"


def replay_durable_wal_backlog_v2(
    *,
    wal_writer: DurableWalWriterProtocolV2,
    block_writer: GroupedBlockWriterV2,
) -> BlockBacklogReplayV2:
    """Rebuild only the missing grouped-block suffix from verified durable WAL."""

    if wal_writer.authority.sha256 != block_writer.authority.sha256:
        raise CaptureBatchAckErrorV2("WAL/block replay authorities differ")
    wal_writer.sync()
    first_replayed = block_writer.next_ingest_seq
    durable_tail = wal_writer.durable_ack_seq
    if first_replayed > durable_tail + 1:
        raise CaptureBatchAckErrorV2("grouped blocks extend beyond the durable WAL")
    _verify_committed_block_prefix_v2(
        wal_writer=wal_writer,
        block_writer=block_writer,
        block_tail=first_replayed - 1,
    )
    if first_replayed == durable_tail + 1:
        return BlockBacklogReplayV2(
            wal_authority_sha256=wal_writer.authority.sha256,
            first_replayed_ingest_seq=None,
            last_replayed_ingest_seq=None,
            replayed_record_count=0,
            first_new_block_sequence=None,
            last_new_block_sequence=None,
            final_block_hash=block_writer.last_block_hash,
        )

    builder = GroupedBlockBuilderV2(block_writer.policy)
    first_new_block_sequence = block_writer.next_block_sequence
    replayed = 0
    last_receipt_ns: int | None = None

    def consume(ingest_seq: int, encoded_line: bytes) -> None:
        nonlocal replayed, last_receipt_ns
        if ingest_seq < first_replayed:
            return
        if ingest_seq > durable_tail:
            raise CaptureBatchAckErrorV2("WAL replay crossed its durable ACK")
        record = parse_raw_record_line_v2(encoded_line)
        if record.ingest_seq != ingest_seq:
            raise CaptureBatchAckErrorV2("decoded WAL replay sequence differs")
        queued = QueuedRawRecordV2(
            record=record,
            encoded_line=encoded_line,
            encoded_len=len(encoded_line),
            encoded_sha256=hashlib.sha256(encoded_line).hexdigest(),
            raw_len=record.raw_len,
            ingest_seq=record.ingest_seq,
            enqueued_monotonic_ns=record.receipt_monotonic_ns,
        )
        for block in builder.offer(
            queued,
            now_ns=record.receipt_monotonic_ns,
        ):
            block_writer.commit(block)
        replayed += 1
        last_receipt_ns = record.receipt_monotonic_ns

    wal_writer.consume_durable_records(consume)
    expected_count = durable_tail - first_replayed + 1
    if replayed != expected_count or last_receipt_ns is None:
        raise CaptureBatchAckErrorV2("WAL replay count differs from its durable suffix")
    tail = builder.flush_recovery_tail(now_ns=last_receipt_ns)
    if tail is not None:
        block_writer.commit(tail)
    if block_writer.next_ingest_seq != durable_tail + 1:
        raise CaptureBatchAckErrorV2("grouped-block replay did not cover the durable WAL")
    return BlockBacklogReplayV2(
        wal_authority_sha256=wal_writer.authority.sha256,
        first_replayed_ingest_seq=first_replayed,
        last_replayed_ingest_seq=durable_tail,
        replayed_record_count=replayed,
        first_new_block_sequence=first_new_block_sequence,
        last_new_block_sequence=block_writer.next_block_sequence - 1,
        final_block_hash=block_writer.last_block_hash,
    )


def _verify_committed_block_prefix_v2(
    *,
    wal_writer: DurableWalWriterProtocolV2,
    block_writer: GroupedBlockWriterV2,
    block_tail: int,
) -> str:
    domain = b"R4B_V2_WAL_BLOCK_PREFIX\0"
    if block_tail == 0:
        return hashlib.sha256(domain).hexdigest()
    wal_digest = hashlib.sha256(domain)
    block_digest = hashlib.sha256(domain)
    wal_count = 0
    block_count = 0

    def update(digest: _DigestV2, ingest_seq: int, encoded_line: bytes) -> None:
        digest.update(struct.pack(">Q", ingest_seq))
        digest.update(struct.pack(">Q", len(encoded_line)))
        digest.update(encoded_line)

    def consume_wal(ingest_seq: int, encoded_line: bytes) -> None:
        nonlocal wal_count
        if ingest_seq > block_tail:
            return
        update(wal_digest, ingest_seq, encoded_line)
        wal_count += 1

    def consume_block(ingest_seq: int, encoded_line: bytes) -> None:
        nonlocal block_count
        if ingest_seq > block_tail:
            return
        update(block_digest, ingest_seq, encoded_line)
        block_count += 1

    wal_writer.consume_durable_records(consume_wal)
    block_writer.consume_committed_records(consume_block)
    if (
        wal_count != block_tail
        or block_count != block_tail
        or wal_digest.digest() != block_digest.digest()
    ):
        raise CaptureBatchAckErrorV2(
            "committed grouped-block bytes differ from the durable WAL prefix"
        )
    return wal_digest.hexdigest()


def verify_capture_finality_fence_receipt_v2(
    receipt: CaptureFinalityFenceReceiptV2,
    *,
    wal_writer: DurableWalWriterProtocolV2,
    block_writer: GroupedBlockWriterV2,
) -> str:
    """Recompute one historical prefix proof from current WAL/block artifacts."""

    if not isinstance(receipt, CaptureFinalityFenceReceiptV2):
        raise TypeError("receipt must be a CaptureFinalityFenceReceiptV2")
    if wal_writer.authority.sha256 != block_writer.authority.sha256:
        raise CaptureBatchAckErrorV2("WAL/block finality authorities differ")
    if receipt.authority_sha256 != wal_writer.authority.sha256:
        raise CaptureBatchAckErrorV2("receipt authority differs from current storage")
    if receipt.attempt_id != wal_writer.authority.attempt_id:
        raise CaptureBatchAckErrorV2("receipt attempt differs from current storage")
    if receipt.qualification_id != wal_writer.policy.qualification_id or (
        receipt.qualification_id != block_writer.policy.qualification_id
    ):
        raise CaptureBatchAckErrorV2("receipt qualification differs from current storage")
    if receipt.wal_durability_binding != wal_writer.durability_binding:
        raise CaptureBatchAckErrorV2(
            "receipt WAL durability binding differs from the current owner"
        )
    if receipt.grouped_block_root_binding != block_writer.root_binding:
        raise CaptureBatchAckErrorV2("receipt grouped-block binding differs from the current root")
    wal_writer.assert_root_binding_current()
    assert_storage_root_binding_v2(block_writer.directory, block_writer.root_binding)
    if wal_writer.durable_ack_seq < receipt.fence_ingest_seq:
        raise CaptureBatchAckErrorV2("current WAL does not contain the receipt prefix")
    if block_writer.next_ingest_seq <= receipt.fence_ingest_seq:
        raise CaptureBatchAckErrorV2("current blocks do not contain the receipt prefix")

    exact_prefix_sha256 = _verify_committed_block_prefix_v2(
        wal_writer=wal_writer,
        block_writer=block_writer,
        block_tail=receipt.fence_ingest_seq,
    )
    if receipt.exact_prefix_sha256 != exact_prefix_sha256:
        raise CaptureBatchAckErrorV2("receipt exact-prefix digest differs from current storage")
    manifests = verify_grouped_blocks(
        block_writer.directory,
        authority=block_writer.authority,
        policy=block_writer.policy,
        signing_authority=block_writer.signing_authority,
        stream_group_id=block_writer.stream_group_id,
        segment_id=block_writer.segment_id,
    )
    matching = tuple(
        manifest for manifest in manifests if manifest.last_ingest_seq == receipt.fence_ingest_seq
    )
    if len(matching) != 1:
        raise CaptureBatchAckErrorV2("receipt fence is not an exact signed grouped-block boundary")
    manifest = matching[0]
    expected_manifest_sha256 = hashlib.sha256(canonical_json_line(asdict(manifest))).hexdigest()
    derived = {
        "wal_durable_ack_seq": receipt.fence_ingest_seq,
        "finalized_block_tail_ingest_seq": manifest.last_ingest_seq,
        "durable_record_count": receipt.fence_ingest_seq,
        "final_block_sequence": manifest.block_sequence,
        "final_block_hash": manifest.block_hash,
        "final_block_manifest_sha256": expected_manifest_sha256,
        "final_block_container_sha256": manifest.container_sha256,
        "target_last_receipt_wall_ms": manifest.last_receipt_wall_ms,
        "target_last_receipt_monotonic_ns": manifest.last_receipt_monotonic_ns,
        "block_signing_authority_sha256": block_writer.signing_authority.sha256,
        "stream_group_id": block_writer.stream_group_id,
        "segment_id": block_writer.segment_id,
    }
    for field_name, expected in derived.items():
        if getattr(receipt, field_name) != expected:
            raise CaptureBatchAckErrorV2(
                f"receipt {field_name} differs from current signed artifacts"
            )
    return receipt.prefix_proof_sha256


class DurableCaptureBatchWriterV2:
    """Jointly sealed WAL ACK and deterministic grouped-block materializer.

    It is the explicit adapter between the async batch pipeline and storage. A
    queue batch is acknowledged only after the selected WAL owner confirms its
    exact durable prefix; the selected dual owner requires both copies. Grouped
    blocks consume only that durable prefix. Raw receipt clocks retain source
    ordering; the nondecreasing block-operation clock additionally absorbs
    finality fences without rewriting those raw receipt values.
    """

    def __init__(
        self,
        *,
        batch_policy: BatchPolicyV2,
        wal_writer: DurableWalWriterProtocolV2,
        block_builder: GroupedBlockBuilderV2,
        block_writer: GroupedBlockWriterV2,
        clock_ns: Callable[[], int] = time.monotonic_ns,
        writer_lease: WriterLease | None = None,
    ) -> None:
        if writer_lease is not None and type(writer_lease) is not WriterLease:
            raise TypeError("writer_lease must be a WriterLease or None")
        if writer_lease is not None:
            writer_lease.assert_held()
        qualification_ids = {
            batch_policy.qualification_id,
            wal_writer.policy.qualification_id,
            block_builder.policy.qualification_id,
            block_writer.policy.qualification_id,
        }
        if len(qualification_ids) != 1:
            raise ValueError("batch/WAL/block qualification IDs must match exactly")
        if block_builder.policy != block_writer.policy:
            raise ValueError("block builder and writer policies must be identical")
        if wal_writer.authority.sha256 != block_writer.authority.sha256:
            raise ValueError("WAL and grouped-block authorities must match")
        if wal_writer.policy.max_record_bytes > block_builder.policy.max_uncompressed_bytes:
            raise ValueError("WAL max_record_bytes cannot exceed the grouped-block record bound")
        if batch_policy.max_records != wal_writer.policy.max_unsynced_records:
            raise ValueError("batch and WAL record candidates must match")
        if batch_policy.max_encoded_bytes != wal_writer.policy.max_unsynced_bytes:
            raise ValueError("batch and WAL byte candidates must match")
        if batch_policy.max_linger_us != wal_writer.policy.interval_ms * 1_000:
            raise ValueError("batch linger and WAL fsync interval candidates must match")
        if wal_writer.next_ingest_seq != block_writer.next_ingest_seq:
            raise CaptureBatchAckErrorV2(
                "durable WAL prefix requires explicit grouped-block replay"
            )
        self.batch_policy = batch_policy
        self.wal_writer = wal_writer
        self.block_builder = block_builder
        self.block_writer = block_writer
        self.writer_lease = writer_lease
        self._clock_ns = clock_ns
        self._last_receipt_wall_ms: int | None = None
        self._last_receipt_monotonic_ns: int | None = None
        self._last_block_clock_ns: int | None = None
        self._closed = False
        manifests = verify_grouped_blocks(
            self.block_writer.directory,
            authority=self.block_writer.authority,
            policy=self.block_writer.policy,
            signing_authority=self.block_writer.signing_authority,
            stream_group_id=self.block_writer.stream_group_id,
            segment_id=self.block_writer.segment_id,
        )
        if manifests:
            final_manifest = manifests[-1]
            if final_manifest.last_ingest_seq != self.block_writer.next_ingest_seq - 1:
                raise CaptureBatchAckErrorV2(
                    "verified grouped-block tail differs from writer state"
                )
            _verify_committed_block_prefix_v2(
                wal_writer=self.wal_writer,
                block_writer=self.block_writer,
                block_tail=final_manifest.last_ingest_seq,
            )
            self._last_receipt_wall_ms = final_manifest.last_receipt_wall_ms
            self._last_receipt_monotonic_ns = final_manifest.last_receipt_monotonic_ns
            self._last_block_clock_ns = final_manifest.last_receipt_monotonic_ns
        elif self.block_writer.next_ingest_seq != 1:
            raise CaptureBatchAckErrorV2("non-empty grouped-block cursor has no verified manifest")
        self._assert_writer_lease_held()

    @property
    def closed(self) -> bool:
        """Return whether the joint durable writer completed close or abort."""

        return self._closed

    def append_many(self, records: Sequence[QueuedRawRecordV2]) -> int:
        with self._writer_lease_operation():
            return self._append_many_guarded(records)

    def _append_many_guarded(self, records: Sequence[QueuedRawRecordV2]) -> int:
        if self._closed:
            raise CaptureBatchClosedV2("durable V2 batch writer is closed")
        self._assert_writer_lease_held()
        self._assert_storage_mutation_health()
        if not records:
            raise ValueError("append_many requires a non-empty batch")
        self._preflight_records(records)
        result = self.wal_writer.append_batch(records)
        durable_ack_seq = self.wal_writer.sync()
        if durable_ack_seq != records[-1].ingest_seq:
            raise CaptureBatchAckErrorV2(
                "WAL sync did not acknowledge the exact submitted batch tail"
            )
        for record in records:
            receipt_ns = record.record.receipt_monotonic_ns
            block_now_ns = (
                receipt_ns
                if self._last_block_clock_ns is None
                else max(receipt_ns, self._last_block_clock_ns)
            )
            completed = self.block_builder.offer(record, now_ns=block_now_ns)
            for block in completed:
                self.block_writer.commit(block)
            self._last_receipt_wall_ms = record.record.receipt_wall_ms
            self._last_receipt_monotonic_ns = receipt_ns
            self._last_block_clock_ns = block_now_ns
        if result.last_ingest_seq != durable_ack_seq:
            raise CaptureBatchAckErrorV2("WAL append result differs from durable ACK")
        self._assert_storage_mutation_health()
        self._assert_writer_lease_held()
        return durable_ack_seq

    def finalize_through(
        self,
        *,
        requested_ingest_seq: int,
        fence_ingest_seq: int,
        fence_monotonic_ns: int,
    ) -> CaptureFinalityFenceReceiptV2:
        """Synchronously finalize one queue-ordered prefix and remain writable."""

        with self._writer_lease_operation():
            return self._finalize_through_guarded(
                requested_ingest_seq=requested_ingest_seq,
                fence_ingest_seq=fence_ingest_seq,
                fence_monotonic_ns=fence_monotonic_ns,
            )

    def _finalize_through_guarded(
        self,
        *,
        requested_ingest_seq: int,
        fence_ingest_seq: int,
        fence_monotonic_ns: int,
    ) -> CaptureFinalityFenceReceiptV2:

        if self._closed:
            raise CaptureBatchClosedV2("durable V2 batch writer is closed")
        self._assert_writer_lease_held()
        self._assert_storage_mutation_health()
        if type(requested_ingest_seq) is not int or requested_ingest_seq < 1:
            raise ValueError("requested_ingest_seq must be a positive integer")
        if type(fence_ingest_seq) is not int or fence_ingest_seq != requested_ingest_seq:
            raise ValueError("fence_ingest_seq must equal the requested sequence")
        if type(fence_monotonic_ns) is not int or fence_monotonic_ns < 0:
            raise ValueError("fence_monotonic_ns must be a nonnegative integer")
        if self._last_block_clock_ns is not None and (
            fence_monotonic_ns < self._last_block_clock_ns
        ):
            raise CaptureBatchClockErrorV2("finality fence precedes the retained block clock")
        writer_observed_monotonic_ns = self._clock_ns()
        if (
            type(writer_observed_monotonic_ns) is not int
            or writer_observed_monotonic_ns < fence_monotonic_ns
        ):
            raise CaptureBatchClockErrorV2("writer clock precedes the ordered finality fence")

        self._assert_current_storage_bindings()
        durable_ack_seq = self.wal_writer.sync(now_ns=fence_monotonic_ns)
        if (
            durable_ack_seq != fence_ingest_seq
            or self.wal_writer.next_ingest_seq != fence_ingest_seq + 1
        ):
            raise CaptureBatchAckErrorV2("finality fence differs from the exact durable WAL tail")
        committed_tail = self.block_writer.next_ingest_seq - 1
        if committed_tail > fence_ingest_seq:
            raise CaptureBatchAckErrorV2(
                "grouped blocks already extend beyond the exact finality fence"
            )
        _verify_committed_block_prefix_v2(
            wal_writer=self.wal_writer,
            block_writer=self.block_writer,
            block_tail=committed_tail,
        )

        tail = self.block_builder.flush_finality_fence(now_ns=fence_monotonic_ns)
        self._last_block_clock_ns = fence_monotonic_ns
        if tail is not None:
            if tail.last_ingest_seq != fence_ingest_seq:
                raise CaptureBatchAckErrorV2(
                    "finality fence block differs from the ordered queue tail"
                )
            self.block_writer.commit(tail)
        if self.block_writer.next_ingest_seq != fence_ingest_seq + 1:
            raise CaptureBatchAckErrorV2("grouped blocks do not finalize the exact fence prefix")
        exact_prefix_sha256 = _verify_committed_block_prefix_v2(
            wal_writer=self.wal_writer,
            block_writer=self.block_writer,
            block_tail=fence_ingest_seq,
        )
        manifests = verify_grouped_blocks(
            self.block_writer.directory,
            authority=self.block_writer.authority,
            policy=self.block_writer.policy,
            signing_authority=self.block_writer.signing_authority,
            stream_group_id=self.block_writer.stream_group_id,
            segment_id=self.block_writer.segment_id,
        )
        if not manifests or manifests[-1].last_ingest_seq != fence_ingest_seq:
            raise CaptureBatchAckErrorV2(
                "verified grouped-block manifests differ from the fence tail"
            )
        final_manifest = manifests[-1]
        if fence_monotonic_ns < final_manifest.last_receipt_monotonic_ns:
            raise CaptureBatchClockErrorV2(
                "finality fence precedes the finalized block receipt clock"
            )
        self._assert_current_storage_bindings()
        receipt = CaptureFinalityFenceReceiptV2(
            authority_sha256=self.wal_writer.authority.sha256,
            attempt_id=self.wal_writer.authority.attempt_id,
            qualification_id=self.wal_writer.policy.qualification_id,
            requested_ingest_seq=requested_ingest_seq,
            fence_ingest_seq=fence_ingest_seq,
            fence_monotonic_ns=fence_monotonic_ns,
            writer_observed_monotonic_ns=writer_observed_monotonic_ns,
            wal_durable_ack_seq=durable_ack_seq,
            finalized_block_tail_ingest_seq=final_manifest.last_ingest_seq,
            durable_record_count=fence_ingest_seq,
            exact_prefix_sha256=exact_prefix_sha256,
            wal_durability_binding=self.wal_writer.durability_binding,
            grouped_block_root_binding=self.block_writer.root_binding,
            block_signing_authority_sha256=(self.block_writer.signing_authority.sha256),
            final_block_sequence=final_manifest.block_sequence,
            final_block_hash=final_manifest.block_hash,
            final_block_manifest_sha256=hashlib.sha256(
                canonical_json_line(asdict(final_manifest))
            ).hexdigest(),
            final_block_container_sha256=final_manifest.container_sha256,
            target_last_receipt_wall_ms=final_manifest.last_receipt_wall_ms,
            target_last_receipt_monotonic_ns=(final_manifest.last_receipt_monotonic_ns),
            stream_group_id=self.block_writer.stream_group_id,
            segment_id=self.block_writer.segment_id,
        )
        verify_capture_finality_fence_receipt_v2(
            receipt,
            wal_writer=self.wal_writer,
            block_writer=self.block_writer,
        )
        self._assert_storage_mutation_health()
        self._assert_writer_lease_held()
        return receipt

    def _assert_current_storage_bindings(self) -> None:
        self.wal_writer.assert_root_binding_current()
        assert_storage_root_binding_v2(
            self.block_writer.directory,
            self.block_writer.root_binding,
        )

    def _assert_storage_mutation_health(self) -> None:
        self.wal_writer.assert_running_healthy_and_writer_open_v2()
        self.block_writer.assert_running_healthy_and_writer_open_v2()

    def assert_running_healthy_and_writer_open_v2(self) -> None:
        """Fail closed unless the exact durable storage stack remains usable."""

        if self._closed:
            raise CaptureBatchClosedV2("durable V2 batch writer is closed")
        self._assert_writer_lease_held()
        self.wal_writer.assert_running_healthy_and_writer_open_v2()
        self.block_writer.assert_running_healthy_and_writer_open_v2()
        self._assert_current_storage_bindings()
        if self.wal_writer.next_ingest_seq != self.block_writer.next_ingest_seq:
            raise CaptureBatchAckErrorV2(
                "durable WAL and grouped-block cursors differ at admission"
            )
        self._assert_writer_lease_held()

    def assert_live_storage_authority_v2(self) -> None:
        """Validate live storage authority without requiring a quiescent cursor.

        During one admitted append the WAL cursor legitimately advances before
        the grouped block cursor.  Runtime admission guards still need to
        reprove lease, roots, and writer health at that instant, but cursor
        equality is a quiescent-boundary assertion and would be a false fatal.
        """

        if self._closed:
            raise CaptureBatchClosedV2("durable V2 batch writer is closed")
        self._assert_writer_lease_held()
        self.wal_writer.assert_running_healthy_and_writer_open_v2()
        self.block_writer.assert_running_healthy_and_writer_open_v2()
        self._assert_current_storage_bindings()
        self._assert_writer_lease_held()

    def _assert_writer_lease_held(self) -> None:
        """Fail closed around storage operations when production binds a lease.

        The pre/post checks make loss observable at the same writer-thread
        boundary. They cannot make an arbitrary cross-thread OS unlock atomic
        with every underlying filesystem syscall; a post-check failure still
        fatalizes the pipeline and prevents any subsequent acknowledged work.
        """

        if self.writer_lease is not None:
            self.writer_lease.assert_held()

    def _writer_lease_operation(self) -> AbstractContextManager[None]:
        if self.writer_lease is None:
            return nullcontext()
        return self.writer_lease.operation_guard()

    def _preflight_records(self, records: Sequence[QueuedRawRecordV2]) -> None:
        """Reject every block-incompatible batch before any WAL byte is written."""

        expected_ingest_seq = self.wal_writer.next_ingest_seq
        previous_wall_ms = self._last_receipt_wall_ms
        previous_monotonic_ns = self._last_receipt_monotonic_ns
        for record in records:
            record.verify_integrity()
            if record.ingest_seq != expected_ingest_seq:
                raise BlockIntegrityError(
                    "preflight grouped-block ingest sequence is not contiguous"
                )
            if record.encoded_len > self.block_builder.policy.max_uncompressed_bytes:
                raise BlockCapacityError(
                    "single record exceeds grouped-block byte bound before WAL append"
                )
            parsed = parse_raw_record_line_v2(record.encoded_line)
            if parsed != record.record:
                raise BlockIntegrityError(
                    "encoded raw record differs from its queued model before WAL append"
                )
            if canonical_json_line(parsed) != record.encoded_line:
                raise BlockIntegrityError(
                    "queued raw record is not canonical JSONL before WAL append"
                )
            if parsed.protocol_hash != self.wal_writer.authority.protocol_sha256:
                raise BlockIntegrityError(
                    "raw record protocol hash differs from WAL authority before WAL append"
                )
            if previous_wall_ms is not None and parsed.receipt_wall_ms < previous_wall_ms:
                raise BlockIntegrityError(
                    "grouped-block receipt wall time moved backwards before WAL append"
                )
            if (
                previous_monotonic_ns is not None
                and parsed.receipt_monotonic_ns < previous_monotonic_ns
            ):
                raise BlockIntegrityError(
                    "grouped-block receipt monotonic time moved backwards before WAL append"
                )
            previous_wall_ms = parsed.receipt_wall_ms
            previous_monotonic_ns = parsed.receipt_monotonic_ns
            expected_ingest_seq += 1

    def close(self) -> None:
        if self._closed:
            return
        with self._writer_lease_operation():
            self._close_guarded()

    def _close_guarded(self) -> None:
        self._assert_writer_lease_held()
        self._assert_storage_mutation_health()
        observed_ns = self._clock_ns()
        if self._last_block_clock_ns is not None:
            observed_ns = max(observed_ns, self._last_block_clock_ns)
        tail = self.block_builder.flush_tail(now_ns=observed_ns)
        if tail is not None:
            self.block_writer.commit(tail)
        if self.block_writer.next_ingest_seq != self.wal_writer.next_ingest_seq:
            raise CaptureBatchAckErrorV2("grouped blocks do not cover the exact durable WAL prefix")
        self.wal_writer.close()
        self._closed = True
        self._assert_writer_lease_held()

    def abort(self) -> None:
        if self._closed:
            return
        self.wal_writer.abort()
        self._closed = True


class CaptureBatchPipelineV2:
    """One dedicated writer thread crossing per bounded record batch.

    The pipeline is intentionally not wired to V1 live capture, scanners,
    alerts, positions, orders, or outcome computation.
    """

    def __init__(
        self,
        handoff: BoundedBatchHandoffV2,
        writer: CaptureBatchWriterV2,
    ) -> None:
        self.handoff = handoff
        self.writer = writer
        self._drainer = BatchDrainerV2(handoff)
        self._worker: asyncio.Task[None] | None = None
        self._stop_task: asyncio.Task[None] | None = None
        self._executor: ThreadPoolExecutor | None = None
        self._started_once = False
        self._active_batch: CaptureBatchV2 | None = None
        self._active_records_acknowledged = False
        self._clean_tail_shutdown_receipt: CaptureFinalityFenceReceiptV2 | None = None

    def start(self) -> None:
        if (
            self._worker is not None
            or self._executor is not None
            or self._stop_task is not None
            or self._started_once
        ):
            raise RuntimeError("V2 capture batch pipeline was already started")
        self._started_once = True
        self._executor = ThreadPoolExecutor(
            max_workers=1,
            thread_name_prefix="r4b-v2-capture-writer",
        )
        self._worker = asyncio.create_task(
            self._guarded_run(),
            name="r4b-v2-capture-batch-writer",
        )

    def offer(self, record: RawRecordV2) -> QueuedRawRecordV2:
        self._require_running_worker()
        return self.handoff.offer(record)

    def offer_with_admission_receipt(
        self,
        record: RawRecordV2,
    ) -> CaptureQueueAdmissionReceiptV2:
        """Forward the exact handoff-owned synchronous admission proof."""

        self._require_running_worker()
        return self.handoff.offer_with_admission_receipt(record)

    def validate_queue_admission_receipt_v2(
        self,
        receipt: CaptureQueueAdmissionReceiptV2,
    ) -> QueuedRawRecordV2:
        """Revalidate that a proof belongs to this pipeline's handoff."""

        return self.handoff.validate_queue_admission_receipt_v2(receipt)

    async def finalize_through(
        self,
        requested_ingest_seq: int,
        *,
        timeout_seconds: float,
    ) -> CaptureFinalityFenceReceiptV2:
        """Await one exact queue-ordered, WAL-and-block finality boundary."""

        if type(requested_ingest_seq) is not int or requested_ingest_seq < 1:
            raise ValueError("requested_ingest_seq must be a positive integer")
        if (
            not isinstance(timeout_seconds, (int, float))
            or isinstance(timeout_seconds, bool)
            or timeout_seconds <= 0
            or not math.isfinite(float(timeout_seconds))
        ):
            raise ValueError("timeout_seconds must be positive")
        self._require_running_worker()
        future = self.handoff.offer_finality_fence(requested_ingest_seq)
        try:
            result = await asyncio.wait_for(
                asyncio.shield(future),
                timeout=float(timeout_seconds),
            )
        except TimeoutError as exc:
            future.add_done_callback(self._consume_late_fence_result)
            raise CaptureFinalityFenceTimeoutV2(
                "V2 finality fence did not complete within the bounded wait"
            ) from exc
        except asyncio.CancelledError:
            future.add_done_callback(self._consume_late_fence_result)
            raise
        if not isinstance(result, CaptureFinalityFenceReceiptV2):
            raise CaptureBatchAckErrorV2("finality fence returned an invalid receipt type")
        if (
            result.requested_ingest_seq != requested_ingest_seq
            or result.fence_ingest_seq != requested_ingest_seq
        ):
            raise CaptureBatchAckErrorV2("finality receipt differs from the exact requested prefix")
        return result

    async def finalize_current_tail_and_stop(
        self,
        *,
        timeout_seconds: float,
    ) -> CaptureFinalityFenceReceiptV2:
        """Atomically fence the non-empty accepted tail and close the writer.

        A bounded timeout or caller cancellation never cancels the already
        queue-ordered fence. The worker continues toward its queued STOP, and a
        later ``stop()`` call retains ownership of cleanup and any fatal error.
        """

        if (
            not isinstance(timeout_seconds, (int, float))
            or isinstance(timeout_seconds, bool)
            or timeout_seconds <= 0
            or not math.isfinite(float(timeout_seconds))
        ):
            raise ValueError("timeout_seconds must be positive")
        self._require_running_worker()
        future = self.handoff.begin_clean_tail_shutdown()
        request = self.handoff.clean_tail_shutdown_request
        if type(request) is not CaptureFinalityFenceRequestV2:
            raise CaptureFinalityFenceErrorV2(
                "clean tail shutdown did not retain its exact fence request"
            )
        try:
            result = await asyncio.wait_for(
                asyncio.shield(future),
                timeout=float(timeout_seconds),
            )
        except TimeoutError as exc:
            future.add_done_callback(self._consume_late_fence_result)
            raise CaptureFinalityFenceTimeoutV2(
                "V2 clean tail finality fence did not complete within the bounded wait"
            ) from exc
        except asyncio.CancelledError:
            future.add_done_callback(self._consume_late_fence_result)
            raise
        if type(result) is not CaptureFinalityFenceReceiptV2:
            raise CaptureBatchAckErrorV2(
                "clean tail finality fence returned an invalid exact receipt type"
            )
        self._validate_fence_receipt(request, result)
        if result != self._clean_tail_shutdown_receipt:
            raise CaptureBatchAckErrorV2(
                "clean tail receipt differs from the internally completed fence"
            )
        await self.stop()
        return result

    async def stop(self) -> None:
        """Own one cancellation-safe drain/close task and surface its failure."""

        stop_task = self._stop_task
        if stop_task is None:
            if self._worker is None:
                return
            stop_task = asyncio.create_task(
                self._stop_owned(),
                name="r4b-v2-capture-stop-owner",
            )
            self._stop_task = stop_task
        try:
            await asyncio.shield(stop_task)
        finally:
            if stop_task.done() and self._stop_task is stop_task:
                self._stop_task = None

    async def _stop_owned(self) -> None:
        """Finish the exact worker before releasing its executor ownership."""

        worker = self._worker
        if worker is None:
            return
        try:
            if not worker.done():
                self.handoff.stop_producer()
                await self.handoff.join()
            await worker
            self.handoff.fatal_state.raise_if_failed()
        finally:
            if worker.done():
                self._worker = None
                self._shutdown_executor()

    async def wait_failed(self) -> None:
        await self.handoff.fatal_state.failed_event.wait()

    def health_snapshot(self) -> CaptureHealthSnapshotV2:
        return self.handoff.snapshot()

    def assert_running_healthy_and_writer_open_v2(self) -> None:
        """Prove the live worker, accepting handoff, and durable writer."""

        worker = self._worker
        if worker is None or self._executor is None:
            raise RuntimeError("V2 capture batch pipeline is not started")
        if worker.done():
            raise RuntimeError("V2 capture batch worker is not running")
        if not self.handoff.accepting:
            raise CaptureBatchClosedV2("V2 capture handoff is not accepting")
        self.handoff.fatal_state.raise_if_failed()
        if type(self.writer) is not DurableCaptureBatchWriterV2:
            raise TypeError("production pipeline requires DurableCaptureBatchWriterV2")
        self.writer.assert_running_healthy_and_writer_open_v2()

    def assert_live_runtime_authority_v2(self) -> None:
        """Prove a running pipeline while permitting its in-flight append seam."""

        worker = self._worker
        if worker is None or self._executor is None:
            raise RuntimeError("V2 capture batch pipeline is not started")
        if worker.done():
            raise RuntimeError("V2 capture batch worker is not running")
        if not self.handoff.accepting:
            raise CaptureBatchClosedV2("V2 capture handoff is not accepting")
        self.handoff.fatal_state.raise_if_failed()
        if type(self.writer) is not DurableCaptureBatchWriterV2:
            raise TypeError("production pipeline requires DurableCaptureBatchWriterV2")
        self.writer.assert_live_storage_authority_v2()

    async def _guarded_run(self) -> None:
        try:
            await self._run()
        except asyncio.CancelledError:
            error = RuntimeError("V2 capture batch worker was cancelled")
            self.handoff.fail_consumer(
                error,
                failing_ingest_seq=self._active_failing_seq(),
            )
            await self._abort_writer()
            self._discard_active_and_pending()
        except BaseException as exc:
            self.handoff.fail_consumer(
                exc,
                failing_ingest_seq=self._active_failing_seq(),
            )
            await self._abort_writer()
            self._discard_active_and_pending()

    async def _run(self) -> None:
        while True:
            batch = await self._drainer.next_batch()
            self._active_batch = batch
            self._active_records_acknowledged = False
            if batch.records:
                await self._append_and_ack(batch)
                if self.handoff.fatal_state.failed:
                    await self._abort_writer()
                    self._discard_active_and_pending()
                    return
            if batch.finality_fence is not None:
                await self._finalize_and_complete(batch)
                if self.handoff.fatal_state.failed:
                    await self._abort_writer()
                    self._discard_active_and_pending()
                    return
                continue
            if batch.terminal is BatchTerminalV2.FATAL:
                await self._abort_writer()
                self._discard_active_and_pending()
                return
            if batch.terminal is BatchTerminalV2.STOP:
                await self._close_writer()
                self.handoff.complete_terminal(batch)
                self._active_batch = None
                return
            self._active_batch = None

    async def _finalize_and_complete(self, batch: CaptureBatchV2) -> None:
        request = batch.finality_fence
        if request is None:
            raise CaptureBatchAckErrorV2("batch has no finality fence")
        executor = self._require_executor()
        loop = asyncio.get_running_loop()
        receipt = await loop.run_in_executor(
            executor,
            partial(
                self.writer.finalize_through,
                requested_ingest_seq=request.requested_ingest_seq,
                fence_ingest_seq=request.fence_ingest_seq,
                fence_monotonic_ns=request.fence_monotonic_ns,
            ),
        )
        self._validate_fence_receipt(request, receipt)
        if request == self.handoff.clean_tail_shutdown_request:
            if type(receipt) is not CaptureFinalityFenceReceiptV2:
                raise CaptureBatchAckErrorV2("clean tail writer returned a non-exact receipt type")
            if self._clean_tail_shutdown_receipt is not None:
                raise CaptureBatchAckErrorV2("clean tail finality receipt completed more than once")
            self._clean_tail_shutdown_receipt = receipt
        self.handoff.complete_finality_fence(batch, result=receipt)
        self._active_batch = None
        self._active_records_acknowledged = False

    @staticmethod
    def _validate_fence_receipt(
        request: CaptureFinalityFenceRequestV2,
        receipt: CaptureFinalityFenceReceiptV2,
    ) -> None:
        if not isinstance(receipt, CaptureFinalityFenceReceiptV2):
            raise CaptureBatchAckErrorV2("writer returned an invalid finality-fence receipt type")
        if (
            receipt.requested_ingest_seq != request.requested_ingest_seq
            or receipt.fence_ingest_seq != request.fence_ingest_seq
            or receipt.fence_monotonic_ns != request.fence_monotonic_ns
            or receipt.wal_durable_ack_seq != request.fence_ingest_seq
            or receipt.finalized_block_tail_ingest_seq != request.fence_ingest_seq
        ):
            raise CaptureBatchAckErrorV2(
                "writer finality receipt differs from the exact ordered fence"
            )

    async def _append_and_ack(self, batch: CaptureBatchV2) -> None:
        executor = self._require_executor()
        loop = asyncio.get_running_loop()
        started_ns = self.handoff._now()
        self.handoff.telemetry.note_worker_crossing()
        try:
            durable_ack_seq = await loop.run_in_executor(
                executor,
                self.writer.append_many,
                batch.records,
            )
        except BaseException:
            try:
                completed_ns = self.handoff._now()
            except CaptureBatchClockErrorV2:
                completed_ns = self.handoff._failure_snapshot_ns()
            self.handoff.telemetry.note_batch_failure(
                writer_latency_ns=max(0, completed_ns - started_ns)
            )
            raise
        completed_ns = self.handoff._now()
        latency_ns = max(0, completed_ns - started_ns)
        if type(durable_ack_seq) is not int or durable_ack_seq != batch.last_ingest_seq:
            self.handoff.telemetry.note_batch_failure(writer_latency_ns=latency_ns)
            raise CaptureBatchAckErrorV2(
                "append_many returned a non-exact durable batch acknowledgement"
            )
        self.handoff.acknowledge_records(
            batch,
            durable_ack_seq=durable_ack_seq,
            completed_monotonic_ns=completed_ns,
            writer_latency_ns=latency_ns,
        )
        self._active_records_acknowledged = True
        if batch.terminal is None:
            self._active_batch = None
            self._active_records_acknowledged = False

    async def _close_writer(self) -> None:
        executor = self._require_executor()
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(executor, self.writer.close)

    async def _abort_writer(self) -> None:
        executor = self._executor
        if executor is None:
            return
        try:
            await asyncio.get_running_loop().run_in_executor(executor, self.writer.abort)
        except BaseException:
            LOGGER.exception("V2 capture writer abort failed after fatal state")

    def _discard_active_and_pending(self) -> None:
        self.handoff.discard_all(
            active_batch=self._active_batch,
            active_records_acknowledged=self._active_records_acknowledged,
        )
        self._active_batch = None
        self._active_records_acknowledged = False

    def _active_failing_seq(self) -> int | None:
        if self._active_batch is None:
            return None
        if self._active_batch.finality_fence is not None:
            return self._active_batch.finality_fence.fence_ingest_seq
        if not self._active_batch.records:
            return None
        return self._active_batch.records[0].ingest_seq

    def _require_running_worker(self) -> asyncio.Task[None]:
        worker = self._worker
        if worker is None:
            raise RuntimeError("V2 capture batch pipeline is not started")
        if worker.done():
            if not self.handoff.fatal_state.failed:
                self.handoff.fail_consumer(
                    RuntimeError("V2 capture batch worker stopped unexpectedly"),
                    failing_ingest_seq=None,
                )
            self.handoff.fatal_state.raise_if_failed()
            raise CaptureBatchClosedV2("V2 capture batch worker is stopped")
        return worker

    @staticmethod
    def _consume_late_fence_result(future: asyncio.Future[object]) -> None:
        if future.cancelled():
            return
        try:
            future.result()
        except BaseException:
            # The original failure remains owned by CaptureFatalStateV2 and is
            # surfaced by stop(); retrieving it here prevents an orphan warning.
            return

    def _require_executor(self) -> ThreadPoolExecutor:
        if self._executor is None:
            raise RuntimeError("V2 capture writer executor is unavailable")
        return self._executor

    def _shutdown_executor(self) -> None:
        if self._executor is None:
            return
        self._executor.shutdown(wait=True, cancel_futures=True)
        self._executor = None


def verify_clean_stopped_current_tail_v2(
    receipt: CaptureFinalityFenceReceiptV2,
    *,
    pipeline: CaptureBatchPipelineV2,
) -> str:
    """Verify the exact clean-shutdown receipt against the unextended tail."""

    if type(receipt) is not CaptureFinalityFenceReceiptV2:
        raise TypeError("receipt must be an exact CaptureFinalityFenceReceiptV2")
    if type(pipeline) is not CaptureBatchPipelineV2:
        raise TypeError("pipeline must be an exact CaptureBatchPipelineV2")
    if type(pipeline.handoff) is not BoundedBatchHandoffV2:
        raise TypeError("pipeline handoff must be an exact BoundedBatchHandoffV2")
    if type(pipeline.writer) is not DurableCaptureBatchWriterV2:
        raise TypeError("pipeline writer must be an exact DurableCaptureBatchWriterV2")
    if pipeline._worker is not None or pipeline._executor is not None:
        raise CaptureFinalityFenceErrorV2("clean tail pipeline is not fully stopped")
    if receipt != pipeline._clean_tail_shutdown_receipt:
        raise CaptureFinalityFenceErrorV2(
            "receipt differs from the internally completed clean tail receipt"
        )

    request = pipeline.handoff.clean_tail_shutdown_request
    if type(request) is not CaptureFinalityFenceRequestV2:
        raise CaptureFinalityFenceErrorV2(
            "clean tail pipeline has no exact captured shutdown request"
        )
    if (
        receipt.requested_ingest_seq != request.requested_ingest_seq
        or receipt.fence_ingest_seq != request.fence_ingest_seq
        or receipt.fence_monotonic_ns != request.fence_monotonic_ns
    ):
        raise CaptureFinalityFenceErrorV2(
            "receipt differs from the internally captured shutdown request"
        )
    pipeline.handoff.assert_clean_stopped_current_tail_v2(request)

    writer = pipeline.writer
    if not writer.closed:
        raise CaptureFinalityFenceErrorV2("clean tail durable writer is not closed")
    tail = request.fence_ingest_seq
    if (
        writer.wal_writer.durable_ack_seq != tail
        or writer.wal_writer.next_ingest_seq != tail + 1
        or writer.block_writer.next_ingest_seq != tail + 1
    ):
        raise CaptureBatchAckErrorV2(
            "WAL or grouped-block tail extended beyond the clean shutdown receipt"
        )
    return verify_capture_finality_fence_receipt_v2(
        receipt,
        wal_writer=writer.wal_writer,
        block_writer=writer.block_writer,
    )
