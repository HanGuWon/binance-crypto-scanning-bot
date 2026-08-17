from __future__ import annotations

import hashlib
import hmac
import json
import os
import re
import stat
import threading
from collections.abc import Callable, Sequence
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, BinaryIO, Protocol, cast

import zstandard as zstd

from signalbot.r4b_v2.canonical import canonical_json_line
from signalbot.r4b_v2.capture.authority import (
    StorageRootBindingError,
    StorageRootBindingV2,
    StorageRootOpenedIdentityV2,
    bind_storage_root_v2,
    inspect_storage_root_opened_identity_v2,
)
from signalbot.r4b_v2.capture.block_container import (
    BLOCK_COMMIT_MARKER_V2,
    BLOCK_FORMAT_VERSION_V2,
    BLOCK_MAGIC_V2,
    BlockCodecParametersV2,
    BlockContainerHeaderV2,
    BlockSignerV2,
    BlockSigningAuthorityV2,
    SignedBlockContainerError,
    SignedBlockContainerV2,
    encode_signed_block_container_v2,
    parse_and_verify_signed_block_container_v2,
)
from signalbot.r4b_v2.capture.models import (
    RawEncodingV2,
    RawRecordV2,
    TransportV2,
    VenueV2,
)
from signalbot.r4b_v2.capture.wal import WalAuthorityV2

_FINAL_RE = re.compile(r"block-(?P<sequence>[0-9]{8})\.r4bblk")
_PARTIAL_RE = re.compile(r"block-(?P<sequence>[0-9]{8})\.r4bblk\.partial")
_MANIFEST_RE = re.compile(r"block-(?P<sequence>[0-9]{8})\.manifest\.json")
_RECOVERY_RE = re.compile(r"block-(?P<sequence>[0-9]{8})\.recovery\.json")
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_CLEAN_TAIL_TERMINAL_FILE = "block-clean-tail-terminal.json"
_CLEAN_TAIL_TERMINAL_PARTIAL_FILE = "block-clean-tail-terminal.json.partial"
_LEAF_DOMAIN = b"R4B2-RECORD-LEAF-V1\0"
_NODE_DOMAIN = b"R4B2-MERKLE-NODE-V1\0"
_EMPTY_MERKLE = hashlib.sha256(b"R4B2-EMPTY-MERKLE-V1").hexdigest()
_SEALED_ZSTD_VERSION = (1, 5, 7)
_SEALED_COMPRESSION_LEVEL = 9
_SEALED_UNCOMPRESSED_BYTES = 4_194_304
_SEALED_LINGER_MS = 1_000
_SEALED_CLOSE_REASONS = (
    "causal_finality_fence",
    "clean_shutdown",
    "max_bytes",
    "max_linger",
    "next_record_bound",
    "wal_recovery_tail",
)


class BlockError(RuntimeError):
    """Base error for the disconnected grouped-block substrate."""


class BlockIntegrityError(BlockError):
    """Raised when a grouped block cannot be proven exact and contiguous."""


class BlockCapacityError(BlockError):
    """Raised before a block or storage bound would be crossed."""


class BlockShortWriteError(BlockError):
    """Raised when a grouped block write is shorter than requested."""


class BlockRawRecordV2(Protocol):
    @property
    def ingest_seq(self) -> int: ...

    @property
    def receipt_wall_ms(self) -> int: ...

    @property
    def receipt_monotonic_ns(self) -> int: ...


class BlockQueuedRecordV2(Protocol):
    @property
    def record(self) -> RawRecordV2: ...

    @property
    def ingest_seq(self) -> int: ...

    @property
    def encoded_line(self) -> bytes: ...

    @property
    def encoded_len(self) -> int: ...

    @property
    def encoded_sha256(self) -> str: ...

    def verify_integrity(self) -> None: ...


class BlockCleanTailFinalityV2(Protocol):
    """Lower-level view of the finality authority needed by the block owner."""

    @property
    def sha256(self) -> str: ...

    @property
    def authority_sha256(self) -> str: ...

    @property
    def attempt_id(self) -> str: ...

    @property
    def qualification_id(self) -> str: ...

    @property
    def grouped_block_root_binding(self) -> StorageRootBindingV2: ...

    @property
    def grouped_block_root_binding_sha256(self) -> str: ...

    @property
    def block_signing_authority_sha256(self) -> str: ...

    @property
    def stream_group_id(self) -> str: ...

    @property
    def segment_id(self) -> str: ...

    @property
    def fence_ingest_seq(self) -> int: ...

    @property
    def exact_prefix_sha256(self) -> str: ...

    @property
    def prefix_proof_sha256(self) -> str: ...

    @property
    def final_block_sequence(self) -> int: ...

    @property
    def final_block_hash(self) -> str: ...

    @property
    def final_block_manifest_sha256(self) -> str: ...

    @property
    def final_block_container_sha256(self) -> str: ...

    @property
    def target_last_receipt_wall_ms(self) -> int: ...

    @property
    def target_last_receipt_monotonic_ns(self) -> int: ...


@dataclass(frozen=True, slots=True)
class BlockPolicyV2:
    qualification_id: str
    codec_candidate_id: str
    compression_level: int
    max_uncompressed_bytes: int
    max_linger_ms: int

    def __post_init__(self) -> None:
        if not self.qualification_id or not self.codec_candidate_id:
            raise ValueError("qualification and codec candidate IDs must be non-empty")
        if self.compression_level != _SEALED_COMPRESSION_LEVEL:
            raise ValueError("compression_level must be the sealed zstd level 9")
        if self.max_uncompressed_bytes != _SEALED_UNCOMPRESSED_BYTES:
            raise ValueError("max_uncompressed_bytes must be the sealed 4194304 bytes")
        if self.max_linger_ms != _SEALED_LINGER_MS:
            raise ValueError("max_linger_ms must be the sealed 1000 ms")
        if zstd.ZSTD_VERSION != _SEALED_ZSTD_VERSION:
            raise ValueError("linked libzstd must be the sealed version 1.5.7")


def grouped_block_root_contract_v2(
    policy: BlockPolicyV2,
    signing_authority: BlockSigningAuthorityV2,
    stream_group_id: str,
    segment_id: str,
) -> dict[str, object]:
    """Return the one immutable public-read contract for a grouped-block root."""

    if not isinstance(policy, BlockPolicyV2):
        raise TypeError("policy must be a BlockPolicyV2")
    if not isinstance(signing_authority, BlockSigningAuthorityV2):
        raise TypeError("signing_authority must be a BlockSigningAuthorityV2")
    for value, label in (
        (stream_group_id, "stream_group_id"),
        (segment_id, "segment_id"),
    ):
        if not isinstance(value, str) or not value or value.strip() != value:
            raise ValueError(f"{label} must be a normalized identity")
    return {
        "policy": asdict(policy),
        "signing_authority": asdict(signing_authority),
        "stream_group_id": stream_group_id,
        "segment_id": segment_id,
        "container_format": "R4BBLK21/R4BCOMMIT21",
        "close_reasons": _SEALED_CLOSE_REASONS,
    }


@dataclass(frozen=True, slots=True)
class CaptureBlockV2:
    records: tuple[BlockQueuedRecordV2, ...]
    opened_monotonic_ns: int
    closed_monotonic_ns: int
    close_reason: str

    def __post_init__(self) -> None:
        if not self.records:
            raise ValueError("a capture block must contain at least one record")
        if self.opened_monotonic_ns < 0 or self.closed_monotonic_ns < self.opened_monotonic_ns:
            raise ValueError("capture block monotonic bounds are invalid")
        if self.close_reason not in _SEALED_CLOSE_REASONS:
            raise ValueError("capture block close reason is not in the sealed set")
        expected = self.records[0].ingest_seq
        for record in self.records:
            record.verify_integrity()
            if record.ingest_seq != expected:
                raise BlockIntegrityError("capture block ingest sequence is not contiguous")
            expected += 1

    @property
    def record_count(self) -> int:
        return len(self.records)

    @property
    def uncompressed_bytes(self) -> int:
        return sum(record.encoded_len for record in self.records)

    @property
    def first_ingest_seq(self) -> int:
        return self.records[0].ingest_seq

    @property
    def last_ingest_seq(self) -> int:
        return self.records[-1].ingest_seq


@dataclass(frozen=True, slots=True)
class BlockManifestV2:
    data_file: str
    magic: str
    format_version: int
    codec_id: str
    codec_version: str
    codec_candidate_id: str
    compression_level: int
    thread_count: int
    checksum_enabled: bool
    content_size_enabled: bool
    dictionary_sha256: str | None
    authority_sha256: str
    attempt_id: str
    protocol_sha256: str
    plan_sha256: str
    source_manifest_sha256: str
    schema_sha256: str
    runtime_manifest_sha256: str
    qualification_id: str
    stream_group_id: str
    segment_id: str
    block_sequence: int
    previous_block_hash: str | None
    record_count: int
    first_ingest_seq: int
    last_ingest_seq: int
    first_receipt_wall_ms: int
    last_receipt_wall_ms: int
    first_receipt_monotonic_ns: int
    last_receipt_monotonic_ns: int
    uncompressed_bytes: int
    compressed_bytes: int
    uncompressed_sha256: str
    compressed_sha256: str
    compressed_crc32c: int
    record_merkle_root: str
    block_hash: str
    container_bytes: int
    container_sha256: str
    writer_key_id: str
    writer_ed25519_signature: str
    signing_authority_sha256: str
    commit_marker: str
    schema_version: str = "r4b_v2_signed_grouped_block_manifest_v2"


@dataclass(frozen=True, slots=True)
class BlockRecoveryReceiptV2:
    original_file: str
    final_file: str
    block_sequence: int
    block_hash: str
    recovery_kind: str
    manifest_file: str
    schema_version: str = "r4b_v2_grouped_block_recovery_receipt_v1"


@dataclass(frozen=True, slots=True)
class GroupedBlockCleanTailTerminalV2:
    """Durable commit barrier for one exact block tail.

    This is deliberately a storage-level terminal marker, not a claim that the
    overall capture session closed cleanly.  Only the integrity-ledger CLEAN
    seal can make that broader claim.
    """

    finality_receipt_sha256: str
    grouped_block_root_binding_sha256: str
    finality_tail_ingest_seq: int
    final_block_hash: str
    schema_version: str = "r4b_v2_grouped_block_clean_tail_terminal_v1"

    def __post_init__(self) -> None:
        for value, label in (
            (self.finality_receipt_sha256, "finality_receipt_sha256"),
            (
                self.grouped_block_root_binding_sha256,
                "grouped_block_root_binding_sha256",
            ),
            (self.final_block_hash, "final_block_hash"),
        ):
            if not isinstance(value, str) or _SHA256_RE.fullmatch(value) is None:
                raise ValueError(f"{label} must be a lowercase SHA-256 digest")
        if type(self.finality_tail_ingest_seq) is not int or (
            self.finality_tail_ingest_seq < 1
        ):
            raise ValueError("finality_tail_ingest_seq must be a positive integer")
        if self.schema_version != "r4b_v2_grouped_block_clean_tail_terminal_v1":
            raise ValueError("unsupported grouped-block clean-tail terminal schema")

    @property
    def encoded_line(self) -> bytes:
        return canonical_json_line(asdict(self))

    @property
    def sha256(self) -> str:
        return hashlib.sha256(self.encoded_line).hexdigest()


@dataclass(frozen=True, slots=True)
class _BlockMaterial:
    uncompressed: bytes
    compressed: bytes
    records: tuple[RawRecordV2, ...]
    uncompressed_sha256: str
    compressed_sha256: str
    record_merkle_root: str


FaultHook = Callable[[str], None]
BlockRecordConsumer = Callable[[int, bytes], None]


class GroupedBlockBuilderV2:
    """Bounded builder closed by bytes, 1000 ms linger, count, or clean tail."""

    def __init__(self, policy: BlockPolicyV2) -> None:
        self.policy = policy
        self._records: list[BlockQueuedRecordV2] = []
        self._bytes = 0
        self._opened_ns: int | None = None
        self._last_now_ns: int | None = None
        self._last_ingest_seq: int | None = None

    def offer(
        self,
        record: BlockQueuedRecordV2,
        *,
        now_ns: int,
    ) -> tuple[CaptureBlockV2, ...]:
        record.verify_integrity()
        self._validate_now(now_ns)
        if record.encoded_len > self.policy.max_uncompressed_bytes:
            raise BlockCapacityError("single record exceeds grouped-block byte bound")
        if self._last_ingest_seq is not None and record.ingest_seq != self._last_ingest_seq + 1:
            raise BlockIntegrityError("grouped-block builder ingest sequence is not contiguous")
        completed: list[CaptureBlockV2] = []
        if self._opened_ns is not None and (
            now_ns - self._opened_ns >= self.policy.max_linger_ms * 1_000_000
        ):
            completed.append(self._seal(now_ns=now_ns, reason="max_linger"))
        if self._records and (
            self._bytes + record.encoded_len > self.policy.max_uncompressed_bytes
        ):
            completed.append(self._seal(now_ns=now_ns, reason="next_record_bound"))
        if not self._records:
            self._opened_ns = now_ns
        self._records.append(record)
        self._bytes += record.encoded_len
        self._last_ingest_seq = record.ingest_seq
        if self._bytes == self.policy.max_uncompressed_bytes:
            completed.append(self._seal(now_ns=now_ns, reason="max_bytes"))
        return tuple(completed)

    def flush_due(self, *, now_ns: int) -> CaptureBlockV2 | None:
        self._validate_now(now_ns)
        if self._opened_ns is None:
            return None
        if now_ns - self._opened_ns < self.policy.max_linger_ms * 1_000_000:
            return None
        return self._seal(now_ns=now_ns, reason="max_linger")

    def flush_finality_fence(self, *, now_ns: int) -> CaptureBlockV2 | None:
        """Seal the current prefix at an ordered causal finality fence."""

        self._validate_now(now_ns)
        if not self._records:
            return None
        return self._seal(now_ns=now_ns, reason="causal_finality_fence")

    def flush_tail(self, *, now_ns: int) -> CaptureBlockV2 | None:
        self._validate_now(now_ns)
        if not self._records:
            return None
        return self._seal(now_ns=now_ns, reason="clean_shutdown")

    def flush_recovery_tail(self, *, now_ns: int) -> CaptureBlockV2 | None:
        self._validate_now(now_ns)
        if not self._records:
            return None
        return self._seal(now_ns=now_ns, reason="wal_recovery_tail")

    def _seal(self, *, now_ns: int, reason: str) -> CaptureBlockV2:
        assert self._opened_ns is not None
        block = CaptureBlockV2(
            records=tuple(self._records),
            opened_monotonic_ns=self._opened_ns,
            closed_monotonic_ns=now_ns,
            close_reason=reason,
        )
        self._records.clear()
        self._bytes = 0
        self._opened_ns = None
        return block

    def _validate_now(self, now_ns: int) -> None:
        if type(now_ns) is not int or now_ns < 0:
            raise ValueError("now_ns must be a nonnegative integer")
        if self._last_now_ns is not None and now_ns < self._last_now_ns:
            raise BlockIntegrityError("grouped-block monotonic clock moved backwards")
        self._last_now_ns = now_ns


class GroupedBlockWriterV2:
    """Commit signed R4BBLK21 containers with hash-chained manifests."""

    def __init__(
        self,
        directory: str | Path,
        *,
        authority: WalAuthorityV2,
        policy: BlockPolicyV2,
        signer: BlockSignerV2,
        signing_authority: BlockSigningAuthorityV2,
        stream_group_id: str,
        segment_id: str,
        maximum_total_bytes: int,
        emergency_reserve_bytes: int,
        fault_hook: FaultHook | None = None,
        root_role: str = "PROVISIONAL_SINGLE",
        failure_domain_id: str = "local-provisional",
        verification_only: bool = False,
    ) -> None:
        if emergency_reserve_bytes < 1024:
            raise ValueError("emergency_reserve_bytes must be at least 1024")
        if maximum_total_bytes <= emergency_reserve_bytes:
            raise ValueError("maximum_total_bytes must exceed emergency reserve")
        self.directory = Path(directory)
        self.authority = authority
        self.policy = policy
        if signer.key_id != signing_authority.key_id or not hmac.compare_digest(
            signer.public_key_bytes,
            signing_authority.public_key_bytes,
        ):
            raise ValueError("block signer differs from the trusted signing authority")
        self.signer = signer
        self.signing_authority = signing_authority
        self.stream_group_id = stream_group_id
        self.segment_id = segment_id
        self.maximum_total_bytes = maximum_total_bytes
        self.emergency_reserve_bytes = emergency_reserve_bytes
        self._fault_hook = fault_hook
        self._failed: BaseException | None = None
        if type(verification_only) is not bool:
            raise TypeError("verification_only must be boolean")
        self._verification_only = verification_only
        self._lock = threading.RLock()
        self._clean_tail_terminal: GroupedBlockCleanTailTerminalV2 | None = None
        self._clean_tail_terminal_identity: tuple[int, int, int, int] | None = None
        self._terminalization_claimed = False
        if verification_only:
            if not self.directory.is_dir():
                raise BlockIntegrityError(
                    "verification-only grouped-block root must already exist"
                )
            binding_path = self.directory / "storage-root-binding.json"
            if not binding_path.is_file() or binding_path.is_symlink():
                raise BlockIntegrityError(
                    "verification-only grouped-block root binding must already exist"
                )
        else:
            self.directory.mkdir(parents=True, exist_ok=True)
        try:
            self.root_binding: StorageRootBindingV2 = bind_storage_root_v2(
                self.directory,
                storage_kind="GROUPED_BLOCK",
                root_role=root_role,
                failure_domain_id=failure_domain_id,
                authority_sha256=authority.sha256,
                contract=grouped_block_root_contract_v2(
                    policy,
                    signing_authority,
                    stream_group_id,
                    segment_id,
                ),
            )
        except StorageRootBindingError as exc:
            raise BlockIntegrityError(str(exc)) from exc
        self._opened_root_identity = inspect_storage_root_opened_identity_v2(
            self.directory,
            self.root_binding,
        )
        self._known_disk_bytes = _directory_size(self.directory)
        if self._known_disk_bytes > maximum_total_bytes:
            raise BlockCapacityError("grouped-block directory already exceeds its quota")
        self._manifests = verify_grouped_blocks(
            self.directory,
            authority=authority,
            policy=policy,
            signing_authority=signing_authority,
            stream_group_id=stream_group_id,
            segment_id=segment_id,
            allow_finalized_orphan=not verification_only,
            allow_partial=not verification_only,
        )
        self._previous_block_hash = (
            self._manifests[-1].block_hash if self._manifests else None
        )
        self._next_block_sequence = (
            self._manifests[-1].block_sequence + 1 if self._manifests else 1
        )
        self._next_ingest_seq = (
            self._manifests[-1].last_ingest_seq + 1 if self._manifests else 1
        )
        terminal_artifact_exists = any(
            path.exists() or path.is_symlink()
            for path in (
                self.directory / _CLEAN_TAIL_TERMINAL_FILE,
                self.directory / _CLEAN_TAIL_TERMINAL_PARTIAL_FILE,
            )
        )
        if not verification_only and not terminal_artifact_exists:
            self._recover_single_orphan()
        self._refresh_clean_tail_terminal_unlocked(
            recover_partial=not verification_only,
        )

    @property
    def opened_directory(self) -> Path:
        return Path(self._opened_root_identity.canonical_path)

    @property
    def opened_root_identity(self) -> StorageRootOpenedIdentityV2:
        return self._opened_root_identity

    def assert_running_healthy_and_writer_open_v2(self) -> None:
        """Fail closed unless this exact construction-time block root is usable."""

        with self._lock:
            self._assert_storage_current_unlocked()
            if self._verification_only:
                raise BlockIntegrityError(
                    "verification-only grouped-block owner cannot accept commits"
                )
            if self._clean_tail_terminal is not None:
                raise BlockIntegrityError(
                    "grouped-block writer is irreversibly clean-tail terminal"
                )

    def assert_storage_current_v2(self) -> None:
        """Reprove this exact root while allowing either OPEN or terminal state."""

        with self._lock:
            self._assert_storage_current_unlocked()

    def _assert_storage_current_unlocked(self) -> None:
        self._raise_if_failed()
        if os.path.normcase(os.path.abspath(self.directory)) != (
            self._opened_root_identity.canonical_path
        ):
            raise BlockIntegrityError(
                "grouped-block directory differs from its construction-time root"
            )
        try:
            observed = inspect_storage_root_opened_identity_v2(
                self.opened_directory,
                self.root_binding,
            )
        except StorageRootBindingError as exc:
            raise BlockIntegrityError(str(exc)) from exc
        if observed != self._opened_root_identity:
            raise BlockIntegrityError(
                "grouped-block identity differs from its construction-time root"
            )
        self._refresh_clean_tail_terminal_unlocked(
            recover_partial=not self._verification_only,
        )

    @property
    def next_block_sequence(self) -> int:
        return self._next_block_sequence

    @property
    def next_ingest_seq(self) -> int:
        return self._next_ingest_seq

    @property
    def last_block_hash(self) -> str | None:
        return self._previous_block_hash

    def consume_committed_records(self, consume: BlockRecordConsumer) -> int:
        """Verify and stream the exact committed JSONL bytes in ingest order."""

        self._raise_if_failed()
        delivered = consume_verified_grouped_records_v2(
            self.directory,
            authority=self.authority,
            policy=self.policy,
            signing_authority=self.signing_authority,
            stream_group_id=self.stream_group_id,
            segment_id=self.segment_id,
            consume=consume,
        )
        if delivered != self._next_ingest_seq - 1:
            raise BlockIntegrityError("committed block prefix differs from writer state")
        return delivered

    def commit(self, block: CaptureBlockV2) -> BlockManifestV2:
        with self._lock:
            return self._commit_unlocked(block)

    def _commit_unlocked(self, block: CaptureBlockV2) -> BlockManifestV2:
        self.assert_running_healthy_and_writer_open_v2()
        if block.first_ingest_seq != self._next_ingest_seq:
            raise BlockIntegrityError("grouped block does not continue the committed prefix")
        if block.uncompressed_bytes > self.policy.max_uncompressed_bytes:
            raise BlockCapacityError("grouped block exceeds max_uncompressed_bytes")
        material = _material_from_queued(block.records, self.policy)
        if self._manifests:
            previous_manifest = self._manifests[-1]
            _validate_receipt_order(
                material.records[0],
                previous_manifest.last_receipt_wall_ms,
                previous_manifest.last_receipt_monotonic_ns,
            )
        try:
            header = _build_container_header(
                sequence=self._next_block_sequence,
                previous_block_hash=self._previous_block_hash,
                authority=self.authority,
                policy=self.policy,
                stream_group_id=self.stream_group_id,
                segment_id=self.segment_id,
                material=material,
            )
            container = encode_signed_block_container_v2(
                header=header,
                compressed=material.compressed,
                uncompressed_sha256=material.uncompressed_sha256,
                record_merkle_root_sha256=material.record_merkle_root,
                signer=self.signer,
                signing_authority=self.signing_authority,
            )
            final_path = self.directory / f"block-{self._next_block_sequence:08d}.r4bblk"
            partial_path = final_path.with_name(final_path.name + ".partial")
            if partial_path.exists() or final_path.exists():
                raise BlockIntegrityError("grouped-block output path already exists")
            self._ensure_capacity(len(container.encoded))
            self._call_fault("before_block_write")
            _write_new_unflushed(partial_path, container.encoded)
            self._known_disk_bytes += len(container.encoded)
            self._call_fault("after_block_write")
            self._call_fault("before_block_fsync")
            _fsync_path(partial_path)
            self._call_fault("after_block_fsync")
            self._call_fault("before_block_rename")
            os.replace(partial_path, final_path)
            _fsync_parent(final_path)
            _fsync_path(final_path)
            self._call_fault("after_block_rename")
            manifest = _build_manifest(
                data_file=final_path.name,
                sequence=self._next_block_sequence,
                previous_block_hash=self._previous_block_hash,
                authority=self.authority,
                policy=self.policy,
                material=material,
                container=container,
                signing_authority=self.signing_authority,
                stream_group_id=self.stream_group_id,
                segment_id=self.segment_id,
            )
            self._call_fault("before_block_manifest")
            self._write_manifest(manifest)
            self._call_fault("after_block_manifest")
            self._accept_manifest(manifest)
            self.assert_running_healthy_and_writer_open_v2()
            return manifest
        except BaseException as exc:
            self._failed = exc
            raise

    def terminalize_clean_tail_v2(
        self,
        finality: BlockCleanTailFinalityV2,
    ) -> GroupedBlockCleanTailTerminalV2:
        """Durably and irreversibly stop commits at one exact current tail.

        Repeating the call for the same already-durable finality is an
        idempotent verification.  Any partial failure poisons this live owner;
        a new writable owner may only recover a fully fsynced partial marker.
        """

        with self._lock:
            self._assert_storage_current_unlocked()
            if self._clean_tail_terminal is not None:
                self._validate_clean_tail_terminal_against_current_unlocked(
                    self._clean_tail_terminal,
                    finality,
                )
                return self._clean_tail_terminal
            if self._verification_only:
                raise BlockIntegrityError(
                    "verification-only grouped-block owner cannot terminalize"
                )
            if self._terminalization_claimed:
                raise BlockIntegrityError(
                    "grouped-block clean-tail terminalization was already consumed"
                )
            self._terminalization_claimed = True
            try:
                terminal = self._expected_clean_tail_terminal_unlocked(finality)
                self._validate_clean_tail_terminal_against_current_unlocked(
                    terminal,
                    finality,
                )
                self._persist_clean_tail_terminal_unlocked(terminal)
                self._refresh_clean_tail_terminal_unlocked(recover_partial=False)
                if self._clean_tail_terminal != terminal:
                    raise BlockIntegrityError(
                        "persisted grouped-block clean-tail terminal differs"
                    )
                self._validate_clean_tail_terminal_against_current_unlocked(
                    terminal,
                    finality,
                )
                return terminal
            except BaseException as exc:
                self._failed = exc
                raise

    def assert_clean_tail_terminal_and_current_v2(
        self,
        finality: BlockCleanTailFinalityV2,
    ) -> str:
        """Return the marker hash only for the exact retained terminal tail."""

        with self._lock:
            self._assert_storage_current_unlocked()
            terminal = self._clean_tail_terminal
            if terminal is None:
                raise BlockIntegrityError(
                    "grouped-block clean-tail terminal marker is absent"
                )
            self._validate_clean_tail_terminal_against_current_unlocked(
                terminal,
                finality,
            )
            return terminal.sha256

    def _expected_clean_tail_terminal_unlocked(
        self,
        finality: BlockCleanTailFinalityV2,
    ) -> GroupedBlockCleanTailTerminalV2:
        root_binding_sha256 = hashlib.sha256(
            canonical_json_line(asdict(self.root_binding))
        ).hexdigest()
        return GroupedBlockCleanTailTerminalV2(
            finality_receipt_sha256=finality.sha256,
            grouped_block_root_binding_sha256=root_binding_sha256,
            finality_tail_ingest_seq=finality.fence_ingest_seq,
            final_block_hash=finality.final_block_hash,
        )

    def _validate_clean_tail_terminal_against_current_unlocked(
        self,
        terminal: GroupedBlockCleanTailTerminalV2,
        finality: BlockCleanTailFinalityV2,
    ) -> None:
        if type(terminal) is not GroupedBlockCleanTailTerminalV2:
            raise TypeError(
                "terminal must be an exact GroupedBlockCleanTailTerminalV2"
            )
        try:
            terminal.__post_init__()
            expected_terminal = self._expected_clean_tail_terminal_unlocked(finality)
        except (AttributeError, TypeError, ValueError) as exc:
            raise BlockIntegrityError(
                "grouped-block clean-tail finality contract is invalid"
            ) from exc
        if terminal != expected_terminal:
            raise BlockIntegrityError(
                "grouped-block clean-tail terminal differs from finality"
            )
        root_binding_sha256 = hashlib.sha256(
            canonical_json_line(asdict(self.root_binding))
        ).hexdigest()
        if (
            finality.authority_sha256 != self.authority.sha256
            or finality.attempt_id != self.authority.attempt_id
            or finality.qualification_id != self.policy.qualification_id
            or finality.grouped_block_root_binding != self.root_binding
            or finality.grouped_block_root_binding_sha256 != root_binding_sha256
            or finality.block_signing_authority_sha256
            != self.signing_authority.sha256
            or finality.stream_group_id != self.stream_group_id
            or finality.segment_id != self.segment_id
        ):
            raise BlockIntegrityError(
                "grouped-block clean-tail finality differs from storage authority"
            )
        for value, label in (
            (finality.sha256, "finality receipt"),
            (finality.exact_prefix_sha256, "exact prefix"),
            (finality.prefix_proof_sha256, "prefix proof"),
            (finality.final_block_hash, "final block"),
            (finality.final_block_manifest_sha256, "final block manifest"),
            (finality.final_block_container_sha256, "final block container"),
        ):
            if not isinstance(value, str) or _SHA256_RE.fullmatch(value) is None:
                raise BlockIntegrityError(
                    f"grouped-block clean-tail {label} hash is invalid"
                )
        manifests = verify_grouped_blocks(
            self.directory,
            authority=self.authority,
            policy=self.policy,
            signing_authority=self.signing_authority,
            stream_group_id=self.stream_group_id,
            segment_id=self.segment_id,
        )
        if not manifests:
            raise BlockIntegrityError(
                "grouped-block clean-tail terminal requires a nonempty prefix"
            )
        manifest = manifests[-1]
        manifest_sha256 = hashlib.sha256(
            canonical_json_line(asdict(manifest))
        ).hexdigest()
        observed = (
            self._next_ingest_seq,
            self._next_block_sequence,
            self._previous_block_hash,
            manifest.last_ingest_seq,
            manifest.block_sequence,
            manifest.block_hash,
            manifest_sha256,
            manifest.container_sha256,
            manifest.last_receipt_wall_ms,
            manifest.last_receipt_monotonic_ns,
        )
        expected = (
            finality.fence_ingest_seq + 1,
            finality.final_block_sequence + 1,
            finality.final_block_hash,
            finality.fence_ingest_seq,
            finality.final_block_sequence,
            finality.final_block_hash,
            finality.final_block_manifest_sha256,
            finality.final_block_container_sha256,
            finality.target_last_receipt_wall_ms,
            finality.target_last_receipt_monotonic_ns,
        )
        if observed != expected:
            raise BlockIntegrityError(
                "grouped-block clean-tail finality is not the exact current tail"
            )

    def _persist_clean_tail_terminal_unlocked(
        self,
        terminal: GroupedBlockCleanTailTerminalV2,
    ) -> None:
        final_path = self.directory / _CLEAN_TAIL_TERMINAL_FILE
        partial_path = self.directory / _CLEAN_TAIL_TERMINAL_PARTIAL_FILE
        if final_path.exists() or final_path.is_symlink() or (
            partial_path.exists() or partial_path.is_symlink()
        ):
            raise BlockIntegrityError(
                "grouped-block clean-tail terminal path already exists"
            )
        encoded = terminal.encoded_line
        self._known_disk_bytes = _directory_size(self.directory)
        if len(encoded) > self.emergency_reserve_bytes or (
            self._known_disk_bytes + len(encoded) > self.maximum_total_bytes
        ):
            raise BlockCapacityError(
                "grouped-block clean-tail terminal exceeds its reserved capacity"
            )
        self._call_fault("before_clean_tail_terminal_write")
        with partial_path.open("xb", buffering=0) as handle:
            _write_all(handle, encoded)
            self._call_fault("after_clean_tail_terminal_write")
            os.fsync(handle.fileno())
        self._known_disk_bytes += len(encoded)
        self._call_fault("after_clean_tail_terminal_fsync")
        os.replace(partial_path, final_path)
        self._call_fault("after_clean_tail_terminal_rename")
        _fsync_parent(final_path)
        self._call_fault("after_clean_tail_terminal_parent_fsync")
        _fsync_path(final_path)

    def _refresh_clean_tail_terminal_unlocked(
        self,
        *,
        recover_partial: bool,
    ) -> None:
        final_path = self.directory / _CLEAN_TAIL_TERMINAL_FILE
        partial_path = self.directory / _CLEAN_TAIL_TERMINAL_PARTIAL_FILE
        final_exists = final_path.exists() or final_path.is_symlink()
        partial_exists = partial_path.exists() or partial_path.is_symlink()
        if final_exists and partial_exists:
            raise BlockIntegrityError(
                "grouped-block clean-tail has both final and partial markers"
            )
        if not final_exists and not partial_exists:
            if self._clean_tail_terminal is not None:
                raise BlockIntegrityError(
                    "grouped-block clean-tail terminal marker was removed"
                )
            return
        if partial_exists and not recover_partial:
            raise BlockIntegrityError(
                "verification-only grouped-block owner rejects partial terminal marker"
            )
        source = partial_path if partial_exists else final_path
        terminal, identity = _read_clean_tail_terminal(source)
        self._validate_clean_tail_marker_against_current_unlocked(terminal)
        if partial_exists:
            _fsync_path(partial_path)
            os.replace(partial_path, final_path)
            _fsync_parent(final_path)
            _fsync_path(final_path)
            terminal, identity = _read_clean_tail_terminal(final_path)
            self._validate_clean_tail_marker_against_current_unlocked(terminal)
        if self._clean_tail_terminal is None:
            self._clean_tail_terminal = terminal
            self._clean_tail_terminal_identity = identity
            self._terminalization_claimed = True
            return
        if (
            terminal != self._clean_tail_terminal
            or identity != self._clean_tail_terminal_identity
        ):
            raise BlockIntegrityError(
                "grouped-block clean-tail terminal identity changed"
            )

    def _validate_clean_tail_marker_against_current_unlocked(
        self,
        terminal: GroupedBlockCleanTailTerminalV2,
    ) -> None:
        root_binding_sha256 = hashlib.sha256(
            canonical_json_line(asdict(self.root_binding))
        ).hexdigest()
        if terminal.grouped_block_root_binding_sha256 != root_binding_sha256:
            raise BlockIntegrityError(
                "grouped-block clean-tail terminal differs from the bound root"
            )
        manifests = verify_grouped_blocks(
            self.directory,
            authority=self.authority,
            policy=self.policy,
            signing_authority=self.signing_authority,
            stream_group_id=self.stream_group_id,
            segment_id=self.segment_id,
        )
        if not manifests:
            raise BlockIntegrityError(
                "grouped-block clean-tail terminal requires a retained prefix"
            )
        manifest = manifests[-1]
        if (
            terminal.finality_tail_ingest_seq != manifest.last_ingest_seq
            or terminal.final_block_hash != manifest.block_hash
            or self._next_ingest_seq != terminal.finality_tail_ingest_seq + 1
            or self._next_block_sequence != manifest.block_sequence + 1
            or self._previous_block_hash != terminal.final_block_hash
        ):
            raise BlockIntegrityError(
                "grouped-block clean-tail terminal is not the exact current tail"
            )

    def _recover_single_orphan(self) -> None:
        manifested = {manifest.data_file for manifest in self._manifests}
        finals = sorted(
            path
            for path in self.directory.glob("block-*.r4bblk")
            if path.name not in manifested
        )
        partials = sorted(self.directory.glob("block-*.r4bblk.partial"))
        if len(finals) + len(partials) > 1:
            raise BlockIntegrityError("only one grouped-block crash tail is recoverable")
        if not finals and not partials:
            return
        source = (finals or partials)[0]
        sequence = _sequence_from_name(
            source,
            _FINAL_RE if finals else _PARTIAL_RE,
        )
        if sequence != self._next_block_sequence:
            raise BlockIntegrityError("grouped-block orphan is not the contiguous tail")
        receipt_path = self.directory / f"block-{sequence:08d}.recovery.json"
        if receipt_path.exists():
            raise BlockIntegrityError("grouped-block recovery receipt already exists")
        encoded = source.read_bytes()
        container = _parse_container(encoded, self.signing_authority)
        material = _material_from_compressed(container.compressed, self.policy)
        _validate_container_authority(
            container,
            authority=self.authority,
            policy=self.policy,
            signing_authority=self.signing_authority,
            stream_group_id=self.stream_group_id,
            segment_id=self.segment_id,
            sequence=sequence,
            previous_block_hash=self._previous_block_hash,
            material=material,
        )
        if material.records[0].ingest_seq != self._next_ingest_seq:
            raise BlockIntegrityError("grouped-block orphan ingest prefix differs")
        final_path = self.directory / f"block-{sequence:08d}.r4bblk"
        recovery_kind = "finalized_without_manifest"
        if partials:
            _fsync_path(source)
            os.replace(source, final_path)
            _fsync_parent(final_path)
            _fsync_path(final_path)
            recovery_kind = "complete_partial_before_rename"
        manifest = _build_manifest(
            data_file=final_path.name,
            sequence=sequence,
            previous_block_hash=self._previous_block_hash,
            authority=self.authority,
            policy=self.policy,
            material=material,
            container=container,
            signing_authority=self.signing_authority,
            stream_group_id=self.stream_group_id,
            segment_id=self.segment_id,
        )
        self._write_manifest(manifest)
        receipt = BlockRecoveryReceiptV2(
            original_file=source.name,
            final_file=final_path.name,
            block_sequence=sequence,
            block_hash=manifest.block_hash,
            recovery_kind=recovery_kind,
            manifest_file=f"block-{sequence:08d}.manifest.json",
        )
        self._replace_json(receipt_path, asdict(receipt))
        self._accept_manifest(manifest)

    def _write_manifest(self, manifest: BlockManifestV2) -> None:
        path = self.directory / f"block-{manifest.block_sequence:08d}.manifest.json"
        if path.exists():
            raise BlockIntegrityError("grouped-block manifest already exists")
        self._replace_json(path, asdict(manifest))

    def _replace_json(self, path: Path, payload: dict[str, Any]) -> None:
        encoded = canonical_json_line(payload)
        self._ensure_capacity(len(encoded))
        _atomic_replace_bytes(path, encoded)
        self._known_disk_bytes += len(encoded)

    def _accept_manifest(self, manifest: BlockManifestV2) -> None:
        self._manifests.append(manifest)
        self._previous_block_hash = manifest.block_hash
        self._next_block_sequence = manifest.block_sequence + 1
        self._next_ingest_seq = manifest.last_ingest_seq + 1

    def _ensure_capacity(self, additional_bytes: int) -> None:
        if (
            self._known_disk_bytes
            + additional_bytes
            + self.emergency_reserve_bytes
            > self.maximum_total_bytes
        ):
            raise BlockCapacityError(
                "grouped-block write would consume the reserved disk budget"
            )

    def _call_fault(self, point: str) -> None:
        if self._fault_hook is not None:
            self._fault_hook(point)

    def _raise_if_failed(self) -> None:
        if self._failed is not None:
            raise BlockError(
                "grouped-block writer is failed and cannot be reused"
            ) from self._failed


def consume_verified_grouped_records_v2(
    directory: str | Path,
    *,
    authority: WalAuthorityV2,
    policy: BlockPolicyV2,
    signing_authority: BlockSigningAuthorityV2,
    stream_group_id: str,
    segment_id: str,
    consume: BlockRecordConsumer,
) -> int:
    """Verify a finalized signed chain and stream its exact canonical records.

    This public read path needs only the explicitly trusted public authority and
    policy.  It never needs a writer private key and never repairs capture data.
    """

    manifests = verify_grouped_blocks(
        directory,
        authority=authority,
        policy=policy,
        signing_authority=signing_authority,
        stream_group_id=stream_group_id,
        segment_id=segment_id,
    )
    root = Path(directory)
    delivered = 0
    expected_ingest = 1
    for manifest in manifests:
        encoded = (root / manifest.data_file).read_bytes()
        if hashlib.sha256(encoded).hexdigest() != manifest.container_sha256:
            raise BlockIntegrityError(
                "grouped-block container changed after chain verification"
            )
        container = _parse_container(encoded, signing_authority)
        material = _material_from_compressed(container.compressed, policy)
        _validate_container_authority(
            container,
            authority=authority,
            policy=policy,
            signing_authority=signing_authority,
            stream_group_id=stream_group_id,
            segment_id=segment_id,
            sequence=manifest.block_sequence,
            previous_block_hash=manifest.previous_block_hash,
            material=material,
        )
        _validate_material_against_manifest(material, manifest)
        _validate_container_against_manifest(container, manifest)
        lines = material.uncompressed.splitlines(keepends=True)
        if len(lines) != len(material.records):
            raise BlockIntegrityError("grouped-block JSONL line count differs")
        for record, encoded_line in zip(material.records, lines, strict=True):
            if canonical_json_line(json.loads(encoded_line)) != encoded_line:
                raise BlockIntegrityError(
                    "grouped-block record is not canonical JCS JSONL"
                )
            if record.ingest_seq != expected_ingest:
                raise BlockIntegrityError(
                    "grouped-block committed stream is not contiguous"
                )
            consume(record.ingest_seq, encoded_line)
            expected_ingest += 1
            delivered += 1
    expected_delivered = manifests[-1].last_ingest_seq if manifests else 0
    if delivered != expected_delivered:
        raise BlockIntegrityError(
            "committed grouped-block prefix differs from verified manifests"
        )
    return delivered


def verify_grouped_blocks(
    directory: str | Path,
    *,
    authority: WalAuthorityV2,
    policy: BlockPolicyV2,
    signing_authority: BlockSigningAuthorityV2,
    stream_group_id: str,
    segment_id: str,
    allow_finalized_orphan: bool = False,
    allow_partial: bool = False,
) -> list[BlockManifestV2]:
    root = Path(directory)
    _reject_unknown_block_artifacts(root)
    manifests: list[BlockManifestV2] = []
    for path in sorted(root.glob("block-*.manifest.json")):
        sequence = _sequence_from_name(path, _MANIFEST_RE)
        try:
            manifest_bytes = path.read_bytes()
            raw = json.loads(manifest_bytes)
            if canonical_json_line(raw) != manifest_bytes:
                raise ValueError("manifest is not canonical JCS JSONL")
            manifest = BlockManifestV2(**raw)
        except (
            OSError,
            UnicodeDecodeError,
            json.JSONDecodeError,
            TypeError,
            ValueError,
        ) as exc:
            raise BlockIntegrityError(f"invalid grouped-block manifest: {path.name}") from exc
        if sequence != manifest.block_sequence:
            raise BlockIntegrityError("grouped-block manifest filename differs")
        manifests.append(manifest)
    previous: str | None = None
    expected_ingest = 1
    previous_receipt_wall_ms: int | None = None
    previous_receipt_monotonic_ns: int | None = None
    seen_data: set[str] = set()
    for expected_sequence, manifest in enumerate(manifests, start=1):
        if manifest.block_sequence != expected_sequence:
            raise BlockIntegrityError("grouped-block sequence is not contiguous")
        if manifest.previous_block_hash != previous:
            raise BlockIntegrityError("grouped-block previous-hash chain is broken")
        _validate_manifest_authority(
            manifest,
            authority,
            policy,
            signing_authority,
            stream_group_id,
            segment_id,
        )
        if Path(manifest.data_file).name != manifest.data_file:
            raise BlockIntegrityError("grouped-block data filename is not local")
        if manifest.data_file in seen_data:
            raise BlockIntegrityError("grouped-block manifests reuse a data file")
        seen_data.add(manifest.data_file)
        data_path = root / manifest.data_file
        if _sequence_from_name(data_path, _FINAL_RE) != expected_sequence:
            raise BlockIntegrityError("grouped-block data filename differs")
        if not data_path.is_file() or data_path.stat().st_size != manifest.container_bytes:
            raise BlockIntegrityError("grouped-block data is missing or has the wrong size")
        encoded = data_path.read_bytes()
        if hashlib.sha256(encoded).hexdigest() != manifest.container_sha256:
            raise BlockIntegrityError("grouped-block container SHA-256 differs")
        container = _parse_container(encoded, signing_authority)
        material = _material_from_compressed(container.compressed, policy)
        _validate_container_authority(
            container,
            authority=authority,
            policy=policy,
            signing_authority=signing_authority,
            stream_group_id=stream_group_id,
            segment_id=segment_id,
            sequence=expected_sequence,
            previous_block_hash=previous,
            material=material,
        )
        _validate_material_against_manifest(material, manifest)
        _validate_container_against_manifest(container, manifest)
        expected_manifest = _build_manifest(
            data_file=data_path.name,
            sequence=expected_sequence,
            previous_block_hash=previous,
            authority=authority,
            policy=policy,
            material=material,
            container=container,
            signing_authority=signing_authority,
            stream_group_id=stream_group_id,
            segment_id=segment_id,
        )
        if canonical_json_line(asdict(manifest)) != canonical_json_line(
            asdict(expected_manifest)
        ):
            raise BlockIntegrityError("grouped-block manifest is not an exact signed projection")
        if manifest.first_ingest_seq != expected_ingest:
            raise BlockIntegrityError("grouped-block ingest prefix has a gap")
        if (
            previous_receipt_wall_ms is not None
            and manifest.first_receipt_wall_ms < previous_receipt_wall_ms
        ):
            raise BlockIntegrityError(
                "grouped-block receipt wall time moved backwards across blocks"
            )
        if (
            previous_receipt_monotonic_ns is not None
            and manifest.first_receipt_monotonic_ns
            < previous_receipt_monotonic_ns
        ):
            raise BlockIntegrityError(
                "grouped-block receipt monotonic time moved backwards across blocks"
            )
        previous = manifest.block_hash
        expected_ingest = manifest.last_ingest_seq + 1
        previous_receipt_wall_ms = manifest.last_receipt_wall_ms
        previous_receipt_monotonic_ns = manifest.last_receipt_monotonic_ns
    final_files = {path.name for path in root.glob("block-*.r4bblk")}
    if seen_data - final_files:
        raise BlockIntegrityError("grouped-block data and manifest sets differ")
    orphan_finals = final_files - seen_data
    expected_orphan = f"block-{len(manifests) + 1:08d}.r4bblk"
    if orphan_finals and (
        not allow_finalized_orphan or orphan_finals != {expected_orphan}
    ):
        raise BlockIntegrityError("grouped-block data and manifest sets differ")
    partials = list(root.glob("block-*.r4bblk.partial"))
    if partials and not allow_partial:
        raise BlockIntegrityError("grouped-block directory has an unfinished partial")
    _verify_recovery_receipts(root, manifests)
    return manifests


def _reject_unknown_block_artifacts(root: Path) -> None:
    patterns = (_FINAL_RE, _PARTIAL_RE, _MANIFEST_RE, _RECOVERY_RE)
    for path in root.glob("block-*"):
        if path.name in {
            _CLEAN_TAIL_TERMINAL_FILE,
            _CLEAN_TAIL_TERMINAL_PARTIAL_FILE,
        }:
            try:
                status = path.lstat()
            except OSError as exc:
                raise BlockIntegrityError(
                    "grouped-block clean-tail terminal is unreadable"
                ) from exc
            if not stat.S_ISREG(status.st_mode) or path.is_symlink():
                raise BlockIntegrityError(
                    "grouped-block clean-tail terminal must be a regular file"
                )
            continue
        if not path.is_file() or not any(
            pattern.fullmatch(path.name) for pattern in patterns
        ):
            raise BlockIntegrityError(
                f"unknown grouped-block artifact requires audit: {path.name}"
            )


def _verify_recovery_receipts(
    root: Path,
    manifests: Sequence[BlockManifestV2],
) -> None:
    for path in sorted(root.glob("block-*.recovery.json")):
        sequence = _sequence_from_name(path, _RECOVERY_RE)
        if sequence < 1 or sequence > len(manifests):
            raise BlockIntegrityError(
                "grouped-block recovery receipt already exists without committed block"
            )
        try:
            encoded = path.read_bytes()
            document = json.loads(encoded)
            if canonical_json_line(document) != encoded:
                raise ValueError("recovery receipt is not canonical JCS JSONL")
            receipt = BlockRecoveryReceiptV2(**document)
        except (
            OSError,
            UnicodeDecodeError,
            json.JSONDecodeError,
            TypeError,
            ValueError,
        ) as exc:
            raise BlockIntegrityError(
                f"invalid grouped-block recovery receipt: {path.name}"
            ) from exc
        manifest = manifests[sequence - 1]
        if receipt.schema_version != "r4b_v2_grouped_block_recovery_receipt_v1":
            raise BlockIntegrityError("unsupported grouped-block recovery receipt schema")
        if receipt.recovery_kind not in {
            "finalized_without_manifest",
            "complete_partial_before_rename",
        }:
            raise BlockIntegrityError("grouped-block recovery kind is invalid")
        expected_original = manifest.data_file
        if receipt.recovery_kind == "complete_partial_before_rename":
            expected_original += ".partial"
        expected_receipt = BlockRecoveryReceiptV2(
            original_file=expected_original,
            final_file=manifest.data_file,
            block_sequence=sequence,
            block_hash=manifest.block_hash,
            recovery_kind=receipt.recovery_kind,
            manifest_file=f"block-{sequence:08d}.manifest.json",
        )
        if canonical_json_line(asdict(receipt)) != canonical_json_line(
            asdict(expected_receipt)
        ):
            raise BlockIntegrityError("grouped-block recovery receipt differs from block")


def record_merkle_root(encoded_lines: Sequence[bytes]) -> str:
    if not encoded_lines:
        return _EMPTY_MERKLE
    level = [hashlib.sha256(_LEAF_DOMAIN + line).digest() for line in encoded_lines]
    while len(level) > 1:
        if len(level) % 2:
            level.append(level[-1])
        level = [
            hashlib.sha256(_NODE_DOMAIN + level[index] + level[index + 1]).digest()
            for index in range(0, len(level), 2)
        ]
    return level[0].hex()


def parse_raw_record_line_v2(line: bytes) -> RawRecordV2:
    """Strictly recover the typed raw envelope from retained canonical bytes."""

    return _parse_raw_record(line)


def _material_from_queued(
    records: Sequence[BlockQueuedRecordV2],
    policy: BlockPolicyV2,
) -> _BlockMaterial:
    if not records:
        raise BlockIntegrityError("cannot encode an empty grouped block")
    encoded_lines: list[bytes] = []
    expected = records[0].ingest_seq
    previous_wall: int | None = None
    previous_monotonic: int | None = None
    for queued in records:
        queued.verify_integrity()
        if queued.ingest_seq != expected:
            raise BlockIntegrityError("queued grouped-block sequence is not contiguous")
        parsed = _parse_raw_record(queued.encoded_line)
        if parsed != queued.record:
            raise BlockIntegrityError("encoded raw record differs from its queued model")
        if not hmac.compare_digest(
            hashlib.sha256(queued.encoded_line).hexdigest(),
            queued.encoded_sha256,
        ):
            raise BlockIntegrityError("queued grouped-block digest differs")
        _validate_receipt_order(parsed, previous_wall, previous_monotonic)
        previous_wall = parsed.receipt_wall_ms
        previous_monotonic = parsed.receipt_monotonic_ns
        encoded_lines.append(queued.encoded_line)
        expected += 1
    uncompressed = b"".join(encoded_lines)
    return _compress_material(uncompressed, tuple(record.record for record in records), policy)


def _material_from_compressed(compressed: bytes, policy: BlockPolicyV2) -> _BlockMaterial:
    if not compressed:
        raise BlockIntegrityError("grouped-block compressed payload is empty")
    try:
        parameters = zstd.get_frame_parameters(compressed[:18])
    except zstd.ZstdError as exc:
        raise BlockIntegrityError("grouped-block zstd header is invalid") from exc
    if not parameters.has_checksum:
        raise BlockIntegrityError("grouped-block zstd checksum is disabled")
    if parameters.content_size < 1 or parameters.content_size > policy.max_uncompressed_bytes:
        raise BlockIntegrityError("grouped-block zstd content size is outside its bound")
    try:
        uncompressed = zstd.ZstdDecompressor().decompress(
            compressed,
            max_output_size=policy.max_uncompressed_bytes,
        )
    except zstd.ZstdError as exc:
        raise BlockIntegrityError("grouped-block zstd payload or checksum is invalid") from exc
    if len(uncompressed) != parameters.content_size:
        raise BlockIntegrityError("grouped-block zstd content size differs")
    expected_compressed = _compress(uncompressed, policy)
    if not hmac.compare_digest(expected_compressed, compressed):
        raise BlockIntegrityError("grouped-block bytes differ from the sealed codec candidate")
    lines = uncompressed.splitlines(keepends=True)
    if not lines or b"".join(lines) != uncompressed:
        raise BlockIntegrityError("grouped block is not complete JSONL")
    records: list[RawRecordV2] = []
    previous_wall: int | None = None
    previous_monotonic: int | None = None
    expected_ingest: int | None = None
    for line in lines:
        record = _parse_raw_record(line)
        if expected_ingest is not None and record.ingest_seq != expected_ingest:
            raise BlockIntegrityError("grouped-block ingest sequence is not contiguous")
        _validate_receipt_order(record, previous_wall, previous_monotonic)
        previous_wall = record.receipt_wall_ms
        previous_monotonic = record.receipt_monotonic_ns
        expected_ingest = record.ingest_seq + 1
        records.append(record)
    return _BlockMaterial(
        uncompressed=uncompressed,
        compressed=compressed,
        records=tuple(records),
        uncompressed_sha256=hashlib.sha256(uncompressed).hexdigest(),
        compressed_sha256=hashlib.sha256(compressed).hexdigest(),
        record_merkle_root=record_merkle_root(lines),
    )


def _compress_material(
    uncompressed: bytes,
    records: tuple[RawRecordV2, ...],
    policy: BlockPolicyV2,
) -> _BlockMaterial:
    if len(uncompressed) > policy.max_uncompressed_bytes:
        raise BlockCapacityError("grouped block exceeds its uncompressed-byte bound")
    compressed = _compress(uncompressed, policy)
    return _BlockMaterial(
        uncompressed=uncompressed,
        compressed=compressed,
        records=records,
        uncompressed_sha256=hashlib.sha256(uncompressed).hexdigest(),
        compressed_sha256=hashlib.sha256(compressed).hexdigest(),
        record_merkle_root=record_merkle_root(
            uncompressed.splitlines(keepends=True)
        ),
    )


def _compress(uncompressed: bytes, policy: BlockPolicyV2) -> bytes:
    return zstd.ZstdCompressor(
        level=policy.compression_level,
        threads=0,
        write_checksum=True,
        write_content_size=True,
    ).compress(uncompressed)


def _parse_raw_record(line: bytes) -> RawRecordV2:
    if not line.endswith(b"\n") or line.count(b"\n") != 1:
        raise BlockIntegrityError("grouped-block record is not exactly one JSONL line")
    try:
        document = json.loads(line)
        if not isinstance(document, dict):
            raise TypeError("raw record must be a JSON object")
        converted = dict(document)
        converted["transport"] = TransportV2(converted["transport"])
        converted["venue"] = VenueV2(converted["venue"])
        converted["raw_encoding"] = RawEncodingV2(converted["raw_encoding"])
        return RawRecordV2(**cast(Any, converted))
    except (
        KeyError,
        TypeError,
        ValueError,
        UnicodeDecodeError,
        json.JSONDecodeError,
    ) as exc:
        raise BlockIntegrityError("grouped-block raw record is invalid") from exc


def _validate_receipt_order(
    record: BlockRawRecordV2,
    previous_wall: int | None,
    previous_monotonic: int | None,
) -> None:
    if previous_wall is not None and record.receipt_wall_ms < previous_wall:
        raise BlockIntegrityError("grouped-block receipt wall time moved backwards")
    if (
        previous_monotonic is not None
        and record.receipt_monotonic_ns < previous_monotonic
    ):
        raise BlockIntegrityError("grouped-block receipt monotonic time moved backwards")


def _codec_parameters(policy: BlockPolicyV2) -> BlockCodecParametersV2:
    return BlockCodecParametersV2(
        codec="zstd",
        library_version=".".join(str(part) for part in zstd.ZSTD_VERSION),
        level=policy.compression_level,
        workers=0,
        checksum=True,
        content_size=True,
        dictionary_sha256=None,
        codec_candidate_id=policy.codec_candidate_id,
        qualification_id=policy.qualification_id,
    )


def _build_container_header(
    *,
    sequence: int,
    previous_block_hash: str | None,
    authority: WalAuthorityV2,
    policy: BlockPolicyV2,
    stream_group_id: str,
    segment_id: str,
    material: _BlockMaterial,
) -> BlockContainerHeaderV2:
    first = material.records[0]
    last = material.records[-1]
    return BlockContainerHeaderV2(
        magic=BLOCK_MAGIC_V2.decode("ascii"),
        format_version=BLOCK_FORMAT_VERSION_V2,
        codec_and_parameters=_codec_parameters(policy),
        schema_hash=authority.schema_sha256,
        protocol_hash=authority.protocol_sha256,
        authority_hash=authority.sha256,
        plan_hash=authority.plan_sha256,
        source_manifest_hash=authority.source_manifest_sha256,
        runtime_manifest_hash=authority.runtime_manifest_sha256,
        attempt_id=authority.attempt_id,
        stream_group_id=stream_group_id,
        segment_id=segment_id,
        block_index=sequence,
        previous_block_hash=previous_block_hash,
        record_count=len(material.records),
        first_ingest_seq=first.ingest_seq,
        last_ingest_seq=last.ingest_seq,
        first_receipt_monotonic_ns=first.receipt_monotonic_ns,
        last_receipt_monotonic_ns=last.receipt_monotonic_ns,
        uncompressed_length=len(material.uncompressed),
    )


def _build_manifest(
    *,
    data_file: str,
    sequence: int,
    previous_block_hash: str | None,
    authority: WalAuthorityV2,
    policy: BlockPolicyV2,
    material: _BlockMaterial,
    container: SignedBlockContainerV2,
    signing_authority: BlockSigningAuthorityV2,
    stream_group_id: str,
    segment_id: str,
) -> BlockManifestV2:
    _validate_container_authority(
        container,
        authority=authority,
        policy=policy,
        signing_authority=signing_authority,
        stream_group_id=stream_group_id,
        segment_id=segment_id,
        sequence=sequence,
        previous_block_hash=previous_block_hash,
        material=material,
    )
    first = material.records[0]
    last = material.records[-1]
    trailer = container.trailer
    return BlockManifestV2(
        data_file=data_file,
        magic=container.header.magic,
        format_version=container.header.format_version,
        codec_id="zstd",
        codec_version=".".join(str(part) for part in zstd.ZSTD_VERSION),
        codec_candidate_id=policy.codec_candidate_id,
        compression_level=policy.compression_level,
        thread_count=0,
        checksum_enabled=True,
        content_size_enabled=True,
        dictionary_sha256=None,
        authority_sha256=authority.sha256,
        attempt_id=authority.attempt_id,
        protocol_sha256=authority.protocol_sha256,
        plan_sha256=authority.plan_sha256,
        source_manifest_sha256=authority.source_manifest_sha256,
        schema_sha256=authority.schema_sha256,
        runtime_manifest_sha256=authority.runtime_manifest_sha256,
        qualification_id=policy.qualification_id,
        stream_group_id=stream_group_id,
        segment_id=segment_id,
        block_sequence=sequence,
        previous_block_hash=previous_block_hash,
        record_count=len(material.records),
        first_ingest_seq=first.ingest_seq,
        last_ingest_seq=last.ingest_seq,
        first_receipt_wall_ms=first.receipt_wall_ms,
        last_receipt_wall_ms=last.receipt_wall_ms,
        first_receipt_monotonic_ns=first.receipt_monotonic_ns,
        last_receipt_monotonic_ns=last.receipt_monotonic_ns,
        uncompressed_bytes=len(material.uncompressed),
        compressed_bytes=len(material.compressed),
        uncompressed_sha256=material.uncompressed_sha256,
        compressed_sha256=material.compressed_sha256,
        compressed_crc32c=trailer.crc32c,
        record_merkle_root=material.record_merkle_root,
        block_hash=trailer.block_hash_sha256,
        container_bytes=len(container.encoded),
        container_sha256=container.container_sha256,
        writer_key_id=trailer.writer_key_id,
        writer_ed25519_signature=trailer.writer_ed25519_signature,
        signing_authority_sha256=signing_authority.sha256,
        commit_marker=trailer.commit_marker,
    )


def _validate_manifest_authority(
    manifest: BlockManifestV2,
    authority: WalAuthorityV2,
    policy: BlockPolicyV2,
    signing_authority: BlockSigningAuthorityV2,
    stream_group_id: str,
    segment_id: str,
) -> None:
    if manifest.schema_version != "r4b_v2_signed_grouped_block_manifest_v2":
        raise BlockIntegrityError("unsupported grouped-block manifest schema")
    if (
        manifest.magic != BLOCK_MAGIC_V2.decode("ascii")
        or manifest.format_version != BLOCK_FORMAT_VERSION_V2
        or manifest.codec_id != "zstd"
    ):
        raise BlockIntegrityError("unsupported grouped-block codec format")
    if manifest.codec_version != "1.5.7":
        raise BlockIntegrityError("grouped-block zstd runtime version differs")
    if (
        manifest.codec_candidate_id != policy.codec_candidate_id
        or manifest.compression_level != policy.compression_level
        or manifest.qualification_id != policy.qualification_id
    ):
        raise BlockIntegrityError("grouped-block codec qualification differs")
    if (
        manifest.thread_count != 0
        or not manifest.checksum_enabled
        or not manifest.content_size_enabled
        or manifest.dictionary_sha256 is not None
    ):
        raise BlockIntegrityError("grouped-block codec flags differ")
    expected_authority = (
        authority.sha256,
        authority.attempt_id,
        authority.protocol_sha256,
        authority.plan_sha256,
        authority.source_manifest_sha256,
        authority.schema_sha256,
        authority.runtime_manifest_sha256,
    )
    actual_authority = (
        manifest.authority_sha256,
        manifest.attempt_id,
        manifest.protocol_sha256,
        manifest.plan_sha256,
        manifest.source_manifest_sha256,
        manifest.schema_sha256,
        manifest.runtime_manifest_sha256,
    )
    if actual_authority != expected_authority:
        raise BlockIntegrityError("grouped-block authority differs")
    if (
        manifest.stream_group_id != stream_group_id
        or manifest.segment_id != segment_id
    ):
        raise BlockIntegrityError("grouped-block stream or segment authority differs")
    if (
        manifest.writer_key_id != signing_authority.key_id
        or manifest.signing_authority_sha256 != signing_authority.sha256
    ):
        raise BlockIntegrityError("grouped-block signing authority differs")
    if manifest.commit_marker != BLOCK_COMMIT_MARKER_V2.decode("ascii"):
        raise BlockIntegrityError("grouped-block commit marker is absent")


def _parse_container(
    encoded: bytes,
    signing_authority: BlockSigningAuthorityV2,
) -> SignedBlockContainerV2:
    try:
        return parse_and_verify_signed_block_container_v2(
            encoded,
            signing_authority=signing_authority,
        )
    except (SignedBlockContainerError, ValueError) as exc:
        raise BlockIntegrityError(str(exc)) from exc


def _validate_container_authority(
    container: SignedBlockContainerV2,
    *,
    authority: WalAuthorityV2,
    policy: BlockPolicyV2,
    signing_authority: BlockSigningAuthorityV2,
    stream_group_id: str,
    segment_id: str,
    sequence: int,
    previous_block_hash: str | None,
    material: _BlockMaterial,
) -> None:
    expected_header = _build_container_header(
        sequence=sequence,
        previous_block_hash=previous_block_hash,
        authority=authority,
        policy=policy,
        stream_group_id=stream_group_id,
        segment_id=segment_id,
        material=material,
    )
    if container.header != expected_header:
        raise BlockIntegrityError("signed block embedded header authority differs")
    trailer = container.trailer
    actual_trailer_material = (
        trailer.compressed_length,
        trailer.uncompressed_sha256,
        trailer.compressed_sha256,
        trailer.record_merkle_root_sha256,
        trailer.writer_key_id,
        trailer.commit_marker,
    )
    expected_trailer_material = (
        len(material.compressed),
        material.uncompressed_sha256,
        material.compressed_sha256,
        material.record_merkle_root,
        signing_authority.key_id,
        BLOCK_COMMIT_MARKER_V2.decode("ascii"),
    )
    if actual_trailer_material != expected_trailer_material:
        raise BlockIntegrityError("signed block embedded trailer metadata differs")


def _validate_container_against_manifest(
    container: SignedBlockContainerV2,
    manifest: BlockManifestV2,
) -> None:
    header = container.header
    trailer = container.trailer
    actual = (
        header.magic,
        header.format_version,
        header.authority_hash,
        header.attempt_id,
        header.protocol_hash,
        header.plan_hash,
        header.source_manifest_hash,
        header.schema_hash,
        header.runtime_manifest_hash,
        header.codec_and_parameters.qualification_id,
        header.stream_group_id,
        header.segment_id,
        header.block_index,
        header.previous_block_hash,
        header.record_count,
        header.first_ingest_seq,
        header.last_ingest_seq,
        header.first_receipt_monotonic_ns,
        header.last_receipt_monotonic_ns,
        header.uncompressed_length,
        trailer.compressed_length,
        trailer.crc32c,
        trailer.uncompressed_sha256,
        trailer.compressed_sha256,
        trailer.record_merkle_root_sha256,
        trailer.block_hash_sha256,
        trailer.writer_key_id,
        trailer.writer_ed25519_signature,
        trailer.commit_marker,
        len(container.encoded),
        container.container_sha256,
    )
    expected = (
        manifest.magic,
        manifest.format_version,
        manifest.authority_sha256,
        manifest.attempt_id,
        manifest.protocol_sha256,
        manifest.plan_sha256,
        manifest.source_manifest_sha256,
        manifest.schema_sha256,
        manifest.runtime_manifest_sha256,
        manifest.qualification_id,
        manifest.stream_group_id,
        manifest.segment_id,
        manifest.block_sequence,
        manifest.previous_block_hash,
        manifest.record_count,
        manifest.first_ingest_seq,
        manifest.last_ingest_seq,
        manifest.first_receipt_monotonic_ns,
        manifest.last_receipt_monotonic_ns,
        manifest.uncompressed_bytes,
        manifest.compressed_bytes,
        manifest.compressed_crc32c,
        manifest.uncompressed_sha256,
        manifest.compressed_sha256,
        manifest.record_merkle_root,
        manifest.block_hash,
        manifest.writer_key_id,
        manifest.writer_ed25519_signature,
        manifest.commit_marker,
        manifest.container_bytes,
        manifest.container_sha256,
    )
    if actual != expected:
        raise BlockIntegrityError("signed block embedded metadata differs from manifest")


def _validate_material_against_manifest(
    material: _BlockMaterial,
    manifest: BlockManifestV2,
) -> None:
    first = material.records[0]
    last = material.records[-1]
    actual = (
        len(material.records),
        first.ingest_seq,
        last.ingest_seq,
        first.receipt_wall_ms,
        last.receipt_wall_ms,
        first.receipt_monotonic_ns,
        last.receipt_monotonic_ns,
        len(material.uncompressed),
        len(material.compressed),
        material.uncompressed_sha256,
        material.compressed_sha256,
        material.record_merkle_root,
    )
    expected = (
        manifest.record_count,
        manifest.first_ingest_seq,
        manifest.last_ingest_seq,
        manifest.first_receipt_wall_ms,
        manifest.last_receipt_wall_ms,
        manifest.first_receipt_monotonic_ns,
        manifest.last_receipt_monotonic_ns,
        manifest.uncompressed_bytes,
        manifest.compressed_bytes,
        manifest.uncompressed_sha256,
        manifest.compressed_sha256,
        manifest.record_merkle_root,
    )
    if actual != expected:
        raise BlockIntegrityError("grouped-block metadata differs from its contents")


def _sequence_from_name(path: Path, pattern: re.Pattern[str]) -> int:
    match = pattern.fullmatch(path.name)
    if match is None:
        raise BlockIntegrityError(f"invalid grouped-block filename: {path.name}")
    return int(match.group("sequence"))


def _read_clean_tail_terminal(
    path: Path,
) -> tuple[GroupedBlockCleanTailTerminalV2, tuple[int, int, int, int]]:
    try:
        before = path.lstat()
        if path.is_symlink() or not stat.S_ISREG(before.st_mode):
            raise BlockIntegrityError(
                "grouped-block clean-tail terminal must be a regular file"
            )
        if int(before.st_nlink) != 1:
            raise BlockIntegrityError(
                "grouped-block clean-tail terminal must have exactly one link"
            )
        encoded = path.read_bytes()
        document = json.loads(encoded)
        if canonical_json_line(document) != encoded:
            raise ValueError("clean-tail terminal is not canonical JCS JSONL")
        terminal = GroupedBlockCleanTailTerminalV2(**document)
        after = path.lstat()
    except BlockIntegrityError:
        raise
    except (
        OSError,
        UnicodeDecodeError,
        json.JSONDecodeError,
        TypeError,
        ValueError,
    ) as exc:
        raise BlockIntegrityError(
            "invalid grouped-block clean-tail terminal marker"
        ) from exc
    before_identity = (
        int(before.st_dev),
        int(before.st_ino),
        int(before.st_nlink),
        int(before.st_size),
    )
    after_identity = (
        int(after.st_dev),
        int(after.st_ino),
        int(after.st_nlink),
        int(after.st_size),
    )
    if before_identity != after_identity or len(encoded) != before_identity[3]:
        raise BlockIntegrityError(
            "grouped-block clean-tail terminal changed during validation"
        )
    return terminal, before_identity


def _write_all(handle: BinaryIO, payload: bytes) -> None:
    written = handle.write(payload)
    if written != len(payload):
        raise BlockShortWriteError(
            f"grouped-block short write: expected {len(payload)} bytes, wrote {written}"
        )


def _write_new_unflushed(path: Path, payload: bytes) -> None:
    with path.open("xb", buffering=0) as handle:
        _write_all(handle, payload)


def _atomic_replace_bytes(path: Path, payload: bytes) -> None:
    temporary = path.with_name(path.name + ".partial")
    if temporary.exists():
        raise BlockIntegrityError(
            f"grouped-block atomic residue requires audit: {temporary.name}"
        )
    with temporary.open("xb", buffering=0) as handle:
        _write_all(handle, payload)
        os.fsync(handle.fileno())
    os.replace(temporary, path)
    _fsync_parent(path)
    _fsync_path(path)


def _directory_size(directory: Path) -> int:
    return sum(path.stat().st_size for path in directory.rglob("*") if path.is_file())


def _fsync_path(path: Path) -> None:
    with path.open("rb+") as handle:
        os.fsync(handle.fileno())


def _fsync_parent(path: Path) -> None:
    if os.name == "nt":
        return
    descriptor = os.open(path.parent, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
