from __future__ import annotations

import hashlib
import json
import os
import re
import struct
import time
from collections.abc import Callable, Sequence
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, BinaryIO, Protocol

from signalbot.r4b_v2.canonical import canonical_json_line
from signalbot.r4b_v2.capture.authority import (
    StorageRootBindingError,
    StorageRootBindingV2,
    StorageRootOpenedIdentityV2,
    bind_storage_root_v2,
    inspect_storage_root_opened_identity_v2,
)

_FRAME_CRC32C = struct.Struct(">I")
_MAX_UVARINT_BYTES = 10
_HEX_SHA256 = re.compile(r"[0-9a-f]{64}")
_PARTIAL_RE = re.compile(r"wal-(?P<sequence>[0-9]{8})\.partial")
_FINAL_RE = re.compile(r"wal-(?P<sequence>[0-9]{8})\.wal")
_MANIFEST_RE = re.compile(r"wal-(?P<sequence>[0-9]{8})\.manifest\.json")
_SYNC_INTERVAL_CANDIDATES_MS = frozenset({10, 50, 100})
_DUAL_ROOT_ROLES = frozenset({"PRIMARY", "INDEPENDENT_MIRROR"})
_WAL_ROOT_ROLES = _DUAL_ROOT_ROLES | {"PROVISIONAL_SINGLE"}
_WAL_DURABILITY_MODES = frozenset({"SINGLE_ROOT", "QUALIFIED_DUAL_OWNER"})


class WalError(RuntimeError):
    """Base class for the disconnected V2 write-ahead log substrate."""


class WalIntegrityError(WalError):
    """Raised when WAL authority, ordering, or bytes are not trustworthy."""


class WalCapacityError(WalError):
    """Raised before a bounded WAL capacity would be exceeded."""


class WalShortWriteError(WalError):
    """Raised when an OS write does not accept the complete batch."""


class WalQueuedRecordV2(Protocol):
    @property
    def ingest_seq(self) -> int: ...

    @property
    def encoded_line(self) -> bytes: ...

    @property
    def encoded_len(self) -> int: ...

    @property
    def encoded_sha256(self) -> str: ...

    def verify_integrity(self) -> None: ...


@dataclass(frozen=True, slots=True)
class WalAuthorityV2:
    attempt_id: str
    protocol_sha256: str
    plan_sha256: str
    source_manifest_sha256: str
    schema_sha256: str
    runtime_manifest_sha256: str

    def __post_init__(self) -> None:
        if not self.attempt_id:
            raise ValueError("attempt_id must be non-empty")
        for name in (
            "protocol_sha256",
            "plan_sha256",
            "source_manifest_sha256",
            "schema_sha256",
            "runtime_manifest_sha256",
        ):
            if _HEX_SHA256.fullmatch(getattr(self, name)) is None:
                raise ValueError(f"{name} must be a lowercase SHA-256 digest")

    @property
    def sha256(self) -> str:
        return hashlib.sha256(_canonical_json_bytes(asdict(self))).hexdigest()


@dataclass(frozen=True, slots=True)
class WalDurabilityBindingV2:
    """Canonical declaration of the exact roots behind one WAL durability claim."""

    mode: str
    root_bindings: tuple[StorageRootBindingV2, ...]
    qualification_selection_receipt_sha256: str | None
    physical_failure_domain_independence_verified: bool
    schema_version: str = "r4b_v2_wal_durability_binding_v1"

    def __post_init__(self) -> None:
        if self.schema_version != "r4b_v2_wal_durability_binding_v1":
            raise ValueError("unsupported WAL durability binding schema")
        if not isinstance(self.mode, str) or self.mode not in _WAL_DURABILITY_MODES:
            raise ValueError("unsupported WAL durability binding mode")
        if type(self.root_bindings) is not tuple or any(
            not isinstance(binding, StorageRootBindingV2)
            for binding in self.root_bindings
        ):
            raise ValueError(
                "WAL durability root_bindings must be an exact StorageRootBindingV2 tuple"
            )
        if type(self.physical_failure_domain_independence_verified) is not bool:
            raise ValueError(
                "physical_failure_domain_independence_verified must be a boolean"
            )
        selection_sha256 = self.qualification_selection_receipt_sha256
        if (
            selection_sha256 is not None
            and (
                not isinstance(selection_sha256, str)
                or _HEX_SHA256.fullmatch(selection_sha256) is None
            )
        ):
            raise ValueError(
                "qualification_selection_receipt_sha256 must be a lowercase SHA-256 digest"
            )

        for binding in self.root_bindings:
            if binding.schema_version != "r4b_v2_storage_root_binding_v1":
                raise ValueError("unsupported WAL storage root binding schema")
            if binding.storage_kind != "WAL":
                raise ValueError("WAL durability binding contains a non-WAL root")
            if (
                not isinstance(binding.root_role, str)
                or binding.root_role not in _WAL_ROOT_ROLES
            ):
                raise ValueError("WAL durability binding contains an unsupported root role")
            if (
                not isinstance(binding.authority_sha256, str)
                or _HEX_SHA256.fullmatch(binding.authority_sha256) is None
            ):
                raise ValueError("WAL root authority must be a lowercase SHA-256 digest")
            if (
                not isinstance(binding.contract_sha256, str)
                or _HEX_SHA256.fullmatch(binding.contract_sha256) is None
            ):
                raise ValueError("WAL root contract must be a lowercase SHA-256 digest")
            if (
                not isinstance(binding.failure_domain_id, str)
                or not binding.failure_domain_id
                or binding.failure_domain_id.strip() != binding.failure_domain_id
            ):
                raise ValueError("WAL root failure domain must be a normalized identity")

        if self.mode == "SINGLE_ROOT":
            self._validate_single_root(selection_sha256)
            return
        self._validate_qualified_dual_owner(selection_sha256)

    @property
    def sha256(self) -> str:
        return hashlib.sha256(_canonical_json_bytes(asdict(self))).hexdigest()

    def _validate_single_root(self, selection_sha256: str | None) -> None:
        if len(self.root_bindings) != 1:
            raise ValueError("SINGLE_ROOT durability requires exactly one WAL root")
        role = self.root_bindings[0].root_role
        if role in _DUAL_ROOT_ROLES and selection_sha256 is None:
            raise ValueError(
                "a qualified dual-root component requires its selection receipt SHA-256"
            )
        if self.physical_failure_domain_independence_verified:
            raise ValueError(
                "SINGLE_ROOT cannot claim physical failure-domain independence"
            )

    def _validate_qualified_dual_owner(self, selection_sha256: str | None) -> None:
        if len(self.root_bindings) != 2:
            raise ValueError(
                "QUALIFIED_DUAL_OWNER durability requires exactly two WAL roots"
            )
        roles = tuple(binding.root_role for binding in self.root_bindings)
        if roles != ("PRIMARY", "INDEPENDENT_MIRROR"):
            raise ValueError(
                "QUALIFIED_DUAL_OWNER roots must be ordered PRIMARY then INDEPENDENT_MIRROR"
            )
        if selection_sha256 is None:
            raise ValueError(
                "QUALIFIED_DUAL_OWNER requires its selection receipt SHA-256"
            )
        if len({binding.authority_sha256 for binding in self.root_bindings}) != 1:
            raise ValueError("qualified dual WAL root authorities differ")
        if len({binding.contract_sha256 for binding in self.root_bindings}) != 1:
            raise ValueError("qualified dual WAL root contracts differ")
        if len({binding.failure_domain_id for binding in self.root_bindings}) != 2:
            raise ValueError("qualified dual WAL failure-domain IDs must be distinct")


@dataclass(frozen=True, slots=True)
class WalSyncPolicyV2:
    qualification_id: str
    fsync_candidate_id: str
    interval_ms: int
    max_unsynced_records: int
    max_unsynced_bytes: int
    max_record_bytes: int
    max_segment_bytes: int

    def __post_init__(self) -> None:
        if not self.qualification_id or not self.fsync_candidate_id:
            raise ValueError("qualification and fsync candidate IDs must be non-empty")
        if self.interval_ms not in _SYNC_INTERVAL_CANDIDATES_MS:
            raise ValueError("interval_ms must be an explicitly qualified 10/50/100 ms candidate")
        for name in (
            "max_unsynced_records",
            "max_unsynced_bytes",
            "max_record_bytes",
            "max_segment_bytes",
        ):
            if getattr(self, name) < 1:
                raise ValueError(f"{name} must be positive")
        if self.max_record_bytes > self.max_segment_bytes:
            raise ValueError("max_record_bytes cannot exceed max_segment_bytes")


@dataclass(frozen=True, slots=True)
class WalSegmentManifestV2:
    data_file: str
    segment_sequence: int
    previous_segment_sha256: str | None
    authority_sha256: str
    qualification_id: str
    fsync_candidate_id: str
    first_ingest_seq: int
    last_ingest_seq: int
    durable_ack_seq: int
    record_count: int
    encoded_bytes: int
    wal_bytes: int
    sha256: str
    schema_version: str = "r4b_v2_wal_segment_manifest_v1"


@dataclass(frozen=True, slots=True)
class WalDurableStateV2:
    authority_sha256: str
    active_file: str | None
    segment_sequence: int
    previous_segment_sha256: str | None
    durable_offset: int
    durable_ack_seq: int
    last_committed_block_sequence: int
    qualification_id: str
    fsync_candidate_id: str
    acknowledged_loss_upper_bound_records: int
    schema_version: str = "r4b_v2_wal_durable_state_v1"


@dataclass(frozen=True, slots=True)
class WalRecoveryReceiptV2:
    partial_file: str
    segment_sequence: int
    original_bytes: int
    retained_bytes: int
    discarded_bytes: int
    discarded_sha256: str
    discarded_file: str
    last_complete_ingest_seq: int
    schema_version: str = "r4b_v2_wal_recovery_receipt_v1"


@dataclass(frozen=True, slots=True)
class WalFinalizedOrphanReceiptV2:
    data_file: str
    segment_sequence: int
    data_sha256: str
    manifest_file: str
    first_ingest_seq: int
    last_ingest_seq: int
    record_count: int
    schema_version: str = "r4b_v2_wal_finalized_orphan_receipt_v1"


@dataclass(frozen=True, slots=True)
class WalScanV2:
    first_ingest_seq: int | None
    last_ingest_seq: int | None
    record_count: int
    encoded_bytes: int
    complete_bytes: int
    torn_tail_offset: int | None
    durable_offset_is_boundary: bool
    durable_boundary_ingest_seq: int | None


@dataclass(frozen=True, slots=True)
class WalAppendResultV2:
    first_ingest_seq: int
    last_ingest_seq: int
    record_count: int
    encoded_bytes: int
    durable_ack_seq: int
    pending_records: int
    pending_bytes: int
    fsynced: bool


FaultHook = Callable[[str], None]
ClockNs = Callable[[], int]
WalRecordConsumer = Callable[[int, bytes], None]


class WalWriterV2:
    """Bounded, batch-written WAL with explicit candidate policy and durable ACKs.

    This class is deliberately disconnected from networking, strategy, alerts, and
    PnL. A batch crosses into this writer once; ACK advances only after the WAL and
    its authority state have both been fsynced.
    """

    def __init__(
        self,
        directory: str | Path,
        *,
        authority: WalAuthorityV2,
        policy: WalSyncPolicyV2,
        maximum_total_bytes: int,
        emergency_reserve_bytes: int,
        clock_ns: ClockNs = time.monotonic_ns,
        fault_hook: FaultHook | None = None,
        recover_torn_tail: bool = True,
        root_role: str = "PROVISIONAL_SINGLE",
        failure_domain_id: str = "local-provisional",
        qualification_selection_receipt_sha256: str | None = None,
    ) -> None:
        if emergency_reserve_bytes < 1024:
            raise ValueError("emergency_reserve_bytes must be at least 1024")
        if maximum_total_bytes <= emergency_reserve_bytes:
            raise ValueError("maximum_total_bytes must exceed emergency reserve")
        self.directory = Path(directory)
        self.authority = authority
        self.policy = policy
        self.maximum_total_bytes = maximum_total_bytes
        self.emergency_reserve_bytes = emergency_reserve_bytes
        self._clock_ns = clock_ns
        self._fault_hook = fault_hook
        if root_role not in _WAL_ROOT_ROLES:
            raise ValueError("root_role must be a sealed WAL root role")
        if root_role in _DUAL_ROOT_ROLES and qualification_selection_receipt_sha256 is None:
            raise ValueError(
                "dual WAL roots require a canonical qualification selection receipt SHA-256"
            )
        if (
            qualification_selection_receipt_sha256 is not None
            and _HEX_SHA256.fullmatch(qualification_selection_receipt_sha256) is None
        ):
            raise ValueError(
                "qualification_selection_receipt_sha256 must be a lowercase SHA-256 digest"
            )
        self.qualification_selection_receipt_sha256 = (
            qualification_selection_receipt_sha256
        )
        self.directory.mkdir(parents=True, exist_ok=True)
        root_contract: dict[str, object] = {"policy": asdict(policy)}
        if qualification_selection_receipt_sha256 is not None:
            root_contract["qualification_selection_receipt_sha256"] = (
                qualification_selection_receipt_sha256
            )
        try:
            self.root_binding: StorageRootBindingV2 = bind_storage_root_v2(
                self.directory,
                storage_kind="WAL",
                root_role=root_role,
                failure_domain_id=failure_domain_id,
                authority_sha256=authority.sha256,
                contract=root_contract,
            )
        except StorageRootBindingError as exc:
            raise WalIntegrityError(str(exc)) from exc
        self._opened_root_identity = inspect_storage_root_opened_identity_v2(
            self.directory,
            self.root_binding,
        )
        self._durability_binding = WalDurabilityBindingV2(
            mode="SINGLE_ROOT",
            root_bindings=(self.root_binding,),
            qualification_selection_receipt_sha256=(
                qualification_selection_receipt_sha256
            ),
            physical_failure_domain_independence_verified=False,
        )
        self._known_disk_bytes = _directory_size(self.directory)
        if self._known_disk_bytes > maximum_total_bytes:
            raise WalCapacityError("WAL directory already exceeds its configured quota")
        self._manifests = verify_wal_segments(
            self.directory,
            authority=authority,
            policy=policy,
            allow_finalized_orphan=True,
        )
        self._previous_segment_sha256 = (
            self._manifests[-1].sha256 if self._manifests else None
        )
        self._segment_sequence = (
            self._manifests[-1].segment_sequence + 1 if self._manifests else 1
        )
        self._last_ingest_seq = (
            self._manifests[-1].last_ingest_seq if self._manifests else 0
        )
        self._durable_ack_seq = self._last_ingest_seq
        orphan = _finalized_orphan(self.directory, self._manifests)
        recovered_orphan = orphan is not None
        if orphan is not None:
            if not recover_torn_tail:
                raise WalIntegrityError(
                    "finalized WAL orphan requires explicit recovery authority"
                )
            self._seal_finalized_orphan(orphan)
            self._manifests = verify_wal_segments(
                self.directory,
                authority=authority,
                policy=policy,
            )
            self._previous_segment_sha256 = self._manifests[-1].sha256
            self._segment_sequence = self._manifests[-1].segment_sequence + 1
            self._last_ingest_seq = self._manifests[-1].last_ingest_seq
            self._durable_ack_seq = self._last_ingest_seq

        self._handle: BinaryIO | None = None
        self._partial_path: Path | None = None
        self._first_ingest_seq: int | None = None
        self._record_count = 0
        self._encoded_bytes = 0
        self._wal_bytes = 0
        self._pending_records = 0
        self._pending_bytes = 0
        self._last_sync_ns = self._clock_ns()
        self._closed = False
        self._failed: BaseException | None = None

        partials = sorted(self.directory.glob("wal-*.partial"))
        if len(partials) > 1:
            raise WalIntegrityError("only one contiguous WAL partial tail is allowed")
        if partials:
            partial = partials[0]
            if _sequence_from_name(partial, _PARTIAL_RE) != self._segment_sequence:
                raise WalIntegrityError("WAL partial sequence is not the contiguous tail")
            if not recover_torn_tail:
                raise WalIntegrityError("WAL partial tail requires explicit recovery")
            self._adopt_partial(partial)
        elif recovered_orphan:
            self._write_state(active_file=None, durable_offset=0)

    @property
    def durable_ack_seq(self) -> int:
        return self._durable_ack_seq

    @property
    def durability_binding(self) -> WalDurabilityBindingV2:
        return self._durability_binding

    @property
    def opened_directory(self) -> Path:
        return Path(self._opened_root_identity.canonical_path)

    @property
    def opened_root_identity(self) -> StorageRootOpenedIdentityV2:
        return self._opened_root_identity

    def assert_running_healthy_and_writer_open_v2(self) -> None:
        self._raise_if_failed()
        if self._closed:
            raise WalIntegrityError("WAL writer is closed")
        self.assert_root_binding_current()

    def assert_root_binding_current(self) -> None:
        """Fail unless the immutable root binding still has its exact bytes."""

        self._raise_if_failed()
        try:
            if os.path.normcase(os.path.abspath(self.directory)) != (
                self._opened_root_identity.canonical_path
            ):
                raise StorageRootBindingError(
                    "WAL directory differs from its construction-time root"
                )
            observed = inspect_storage_root_opened_identity_v2(
                self.opened_directory,
                self.root_binding,
            )
            if observed != self._opened_root_identity:
                raise StorageRootBindingError(
                    "WAL root identity differs from its construction-time root"
                )
        except StorageRootBindingError as exc:
            self._failed = exc
            raise WalIntegrityError(str(exc)) from exc

    @property
    def next_ingest_seq(self) -> int:
        return self._last_ingest_seq + 1

    @property
    def pending_records(self) -> int:
        return self._pending_records

    @property
    def pending_bytes(self) -> int:
        return self._pending_bytes

    def consume_durable_records(self, consume: WalRecordConsumer) -> int:
        """Verify, then stream the exact durable prefix without buffering a segment."""

        self._raise_if_failed()
        if self._handle is not None and self._pending_records:
            self.sync()
        manifests = verify_wal_segments(
            self.directory,
            authority=self.authority,
            policy=self.policy,
            allow_finalized_orphan=False,
        )
        expected_ingest = 1
        delivered = 0
        for manifest in manifests:
            scan_wal_file(
                self.directory / manifest.data_file,
                expected_first_ingest_seq=expected_ingest,
                max_record_bytes=self.policy.max_record_bytes,
                consume=consume,
            )
            delivered += manifest.record_count
            expected_ingest = manifest.last_ingest_seq + 1
        if self._partial_path is not None:
            # First pass validates the complete active prefix before a callback can
            # materialize any of its records.
            scan = scan_wal_file(
                self._partial_path,
                expected_first_ingest_seq=expected_ingest,
                max_record_bytes=self.policy.max_record_bytes,
            )
            if scan.torn_tail_offset is not None:
                raise WalIntegrityError("active durable WAL has a torn tail")
            if scan.last_ingest_seq != self._durable_ack_seq:
                raise WalIntegrityError("active WAL bytes exceed or miss the durable ACK")
            scan_wal_file(
                self._partial_path,
                expected_first_ingest_seq=expected_ingest,
                max_record_bytes=self.policy.max_record_bytes,
                consume=consume,
            )
            delivered += scan.record_count
        if delivered != self._durable_ack_seq:
            raise WalIntegrityError("delivered WAL prefix differs from durable ACK")
        return delivered

    def append_batch(
        self,
        records: Sequence[WalQueuedRecordV2],
        *,
        now_ns: int | None = None,
    ) -> WalAppendResultV2:
        self.assert_running_healthy_and_writer_open_v2()
        if not records:
            raise ValueError("records must be a non-empty contiguous batch")
        expected = self._last_ingest_seq + 1
        frames = bytearray()
        encoded_bytes = 0
        for record in records:
            record.verify_integrity()
            if record.ingest_seq != expected:
                raise WalIntegrityError("WAL batch ingest sequence is not contiguous")
            if record.encoded_len != len(record.encoded_line):
                raise WalIntegrityError("queued encoded length differs from WAL bytes")
            if record.encoded_len > self.policy.max_record_bytes:
                raise WalCapacityError("single WAL record exceeds max_record_bytes")
            if hashlib.sha256(record.encoded_line).hexdigest() != record.encoded_sha256:
                raise WalIntegrityError("queued encoded SHA-256 differs from WAL bytes")
            frames.extend(encode_wal_frame(record.encoded_line))
            encoded_bytes += record.encoded_len
            expected += 1
        if len(records) > self.policy.max_unsynced_records:
            raise WalCapacityError("single WAL batch exceeds max_unsynced_records")
        if encoded_bytes > self.policy.max_unsynced_bytes:
            raise WalCapacityError("single WAL batch exceeds max_unsynced_bytes")
        if len(frames) > self.policy.max_segment_bytes:
            raise WalCapacityError("single WAL batch exceeds max_segment_bytes")

        try:
            result = self._append_prevalidated(
                records,
                frames=frames,
                encoded_bytes=encoded_bytes,
                now_ns=now_ns,
            )
            self.assert_running_healthy_and_writer_open_v2()
            return result
        except BaseException as exc:
            self._failed = exc
            raise

    def _append_prevalidated(
        self,
        records: Sequence[WalQueuedRecordV2],
        *,
        frames: bytearray,
        encoded_bytes: int,
        now_ns: int | None,
    ) -> WalAppendResultV2:

        if self._pending_records and (
            self._pending_records + len(records) > self.policy.max_unsynced_records
            or self._pending_bytes + encoded_bytes > self.policy.max_unsynced_bytes
        ):
            self.sync(now_ns=now_ns)

        if (
            self._handle is not None
            and self._wal_bytes + len(frames) > self.policy.max_segment_bytes
        ):
            self._finalize_current()
        if self._handle is None:
            self._open_segment()
        self._ensure_capacity(len(frames))
        assert self._handle is not None
        self._call_fault("before_batch_write")
        _write_all(self._handle, frames)
        self._call_fault("after_batch_write")
        self._known_disk_bytes += len(frames)

        first_ingest_seq = records[0].ingest_seq
        last_ingest_seq = records[-1].ingest_seq
        if self._first_ingest_seq is None:
            self._first_ingest_seq = first_ingest_seq
        self._last_ingest_seq = last_ingest_seq
        self._record_count += len(records)
        self._encoded_bytes += encoded_bytes
        self._wal_bytes += len(frames)
        self._pending_records += len(records)
        self._pending_bytes += encoded_bytes

        observed_now_ns = self._clock_ns() if now_ns is None else now_ns
        if observed_now_ns < self._last_sync_ns:
            raise WalIntegrityError("monotonic WAL clock moved backwards")
        due = (
            observed_now_ns - self._last_sync_ns >= self.policy.interval_ms * 1_000_000
            or self._pending_records >= self.policy.max_unsynced_records
            or self._pending_bytes >= self.policy.max_unsynced_bytes
        )
        if due:
            self.sync(now_ns=observed_now_ns)
        return WalAppendResultV2(
            first_ingest_seq=first_ingest_seq,
            last_ingest_seq=last_ingest_seq,
            record_count=len(records),
            encoded_bytes=encoded_bytes,
            durable_ack_seq=self._durable_ack_seq,
            pending_records=self._pending_records,
            pending_bytes=self._pending_bytes,
            fsynced=due,
        )

    def sync(self, *, now_ns: int | None = None) -> int:
        self._raise_if_failed()
        if self._closed:
            self.assert_root_binding_current()
            return self._durable_ack_seq
        self.assert_running_healthy_and_writer_open_v2()
        if self._handle is None or self._pending_records == 0:
            return self._durable_ack_seq
        try:
            observed_now_ns = self._clock_ns() if now_ns is None else now_ns
            if observed_now_ns < self._last_sync_ns:
                raise WalIntegrityError("monotonic WAL clock moved backwards")
            self._call_fault("before_wal_flush")
            self._handle.flush()
            self._call_fault("before_wal_fsync")
            os.fsync(self._handle.fileno())
            self._call_fault("after_wal_fsync")
            assert self._partial_path is not None
            self._durable_ack_seq = self._last_ingest_seq
            self._pending_records = 0
            self._pending_bytes = 0
            self._last_sync_ns = observed_now_ns
            self._write_state(
                active_file=self._partial_path.name,
                durable_offset=self._handle.tell(),
            )
            self._call_fault("after_durable_state")
            self.assert_running_healthy_and_writer_open_v2()
            return self._durable_ack_seq
        except BaseException as exc:
            self._failed = exc
            raise

    def close(self) -> None:
        if self._closed:
            return
        self.assert_running_healthy_and_writer_open_v2()
        try:
            if self._handle is not None:
                self._finalize_current()
            self.assert_running_healthy_and_writer_open_v2()
            self._closed = True
        except BaseException as exc:
            self._failed = exc
            raise

    def abort(self) -> None:
        """Close an unfinished partial without claiming durability or finality."""

        if self._handle is not None:
            self._handle.close()
            self._handle = None
        self._closed = True

    def _open_segment(self) -> None:
        path = self.directory / f"wal-{self._segment_sequence:08d}.partial"
        self._handle = path.open("xb", buffering=0)
        self._partial_path = path
        self._first_ingest_seq = None
        self._record_count = 0
        self._encoded_bytes = 0
        self._wal_bytes = 0
        self._pending_records = 0
        self._pending_bytes = 0

    def _finalize_current(self) -> None:
        if self._handle is None or self._partial_path is None:
            return
        if self._record_count < 1 or self._first_ingest_seq is None:
            raise WalIntegrityError("cannot finalize an empty WAL segment")
        self.sync()
        self._handle.close()
        self._handle = None
        final_path = self._partial_path.with_suffix(".wal")
        self._call_fault("before_wal_rename")
        os.replace(self._partial_path, final_path)
        _fsync_parent(final_path)
        _fsync_path(final_path)
        self._call_fault("after_wal_rename")
        digest = _sha256_file(final_path)
        manifest = WalSegmentManifestV2(
            data_file=final_path.name,
            segment_sequence=self._segment_sequence,
            previous_segment_sha256=self._previous_segment_sha256,
            authority_sha256=self.authority.sha256,
            qualification_id=self.policy.qualification_id,
            fsync_candidate_id=self.policy.fsync_candidate_id,
            first_ingest_seq=self._first_ingest_seq,
            last_ingest_seq=self._last_ingest_seq,
            durable_ack_seq=self._durable_ack_seq,
            record_count=self._record_count,
            encoded_bytes=self._encoded_bytes,
            wal_bytes=self._wal_bytes,
            sha256=digest,
        )
        manifest_path = self.directory / f"wal-{self._segment_sequence:08d}.manifest.json"
        self._replace_json(manifest_path, asdict(manifest))
        self._previous_segment_sha256 = digest
        self._segment_sequence += 1
        self._partial_path = None
        self._first_ingest_seq = None
        self._record_count = 0
        self._encoded_bytes = 0
        self._wal_bytes = 0
        self._write_state(active_file=None, durable_offset=0)

    def _adopt_partial(self, path: Path) -> None:
        state = _read_state(self.directory / "wal-state.json")
        if state is not None and state.active_file not in (None, path.name):
            raise WalIntegrityError("durable WAL state names a different partial tail")
        durable_offset = (
            state.durable_offset
            if state is not None and state.active_file == path.name
            else 0
        )
        scan = scan_wal_file(
            path,
            expected_first_ingest_seq=self._last_ingest_seq + 1,
            max_record_bytes=self.policy.max_record_bytes,
            durable_offset=durable_offset,
        )
        if durable_offset and not scan.durable_offset_is_boundary:
            raise WalIntegrityError("durable WAL offset is not a complete frame boundary")
        if scan.torn_tail_offset is not None:
            self._preserve_and_truncate_tail(path, scan)
        if scan.record_count < 1 or scan.first_ingest_seq is None or scan.last_ingest_seq is None:
            raise WalIntegrityError("WAL partial has no complete record")
        if state is not None and state.active_file == path.name:
            self._validate_state(state, scan)
            self._durable_ack_seq = state.durable_ack_seq
        self._partial_path = path
        self._handle = path.open("ab", buffering=0)
        self._first_ingest_seq = scan.first_ingest_seq
        self._last_ingest_seq = scan.last_ingest_seq
        self._record_count = scan.record_count
        self._encoded_bytes = scan.encoded_bytes
        self._wal_bytes = scan.complete_bytes
        self._pending_records = self._last_ingest_seq - self._durable_ack_seq
        self._pending_bytes = self._encoded_bytes if self._pending_records else 0
        if self._pending_records:
            # Conservatively seal every CRC-valid recovered frame before accepting
            # the next sequence. The durable state then becomes authoritative.
            self.sync()

    def _seal_finalized_orphan(self, path: Path) -> None:
        sequence = _sequence_from_name(path, _FINAL_RE)
        if sequence != self._segment_sequence:
            raise WalIntegrityError("only the contiguous finalized WAL tail is recoverable")
        scan = scan_wal_file(
            path,
            expected_first_ingest_seq=self._last_ingest_seq + 1,
            max_record_bytes=self.policy.max_record_bytes,
        )
        if (
            scan.torn_tail_offset is not None
            or scan.first_ingest_seq is None
            or scan.last_ingest_seq is None
            or scan.record_count < 1
        ):
            raise WalIntegrityError("finalized WAL orphan is incomplete")
        digest = _sha256_file(path)
        manifest = WalSegmentManifestV2(
            data_file=path.name,
            segment_sequence=sequence,
            previous_segment_sha256=self._previous_segment_sha256,
            authority_sha256=self.authority.sha256,
            qualification_id=self.policy.qualification_id,
            fsync_candidate_id=self.policy.fsync_candidate_id,
            first_ingest_seq=scan.first_ingest_seq,
            last_ingest_seq=scan.last_ingest_seq,
            durable_ack_seq=scan.last_ingest_seq,
            record_count=scan.record_count,
            encoded_bytes=scan.encoded_bytes,
            wal_bytes=scan.complete_bytes,
            sha256=digest,
        )
        manifest_path = self.directory / f"wal-{sequence:08d}.manifest.json"
        receipt_path = self.directory / f"wal-{sequence:08d}.orphan-recovery.json"
        if manifest_path.exists() or receipt_path.exists():
            raise WalIntegrityError("finalized WAL orphan recovery residue already exists")
        self._replace_json(manifest_path, asdict(manifest))
        receipt = WalFinalizedOrphanReceiptV2(
            data_file=path.name,
            segment_sequence=sequence,
            data_sha256=digest,
            manifest_file=manifest_path.name,
            first_ingest_seq=scan.first_ingest_seq,
            last_ingest_seq=scan.last_ingest_seq,
            record_count=scan.record_count,
        )
        self._replace_json(receipt_path, asdict(receipt))

    def _validate_state(self, state: WalDurableStateV2, scan: WalScanV2) -> None:
        if state.authority_sha256 != self.authority.sha256:
            raise WalIntegrityError("durable WAL state authority hash differs")
        if state.segment_sequence != self._segment_sequence:
            raise WalIntegrityError("durable WAL state segment sequence differs")
        if state.previous_segment_sha256 != self._previous_segment_sha256:
            raise WalIntegrityError("durable WAL state previous hash differs")
        if state.qualification_id != self.policy.qualification_id:
            raise WalIntegrityError("durable WAL state qualification differs")
        if state.fsync_candidate_id != self.policy.fsync_candidate_id:
            raise WalIntegrityError("durable WAL state fsync candidate differs")
        expected_ack = (
            self._last_ingest_seq
            if state.durable_offset == 0
            else scan.durable_boundary_ingest_seq
        )
        if expected_ack is None or state.durable_ack_seq != expected_ack:
            raise WalIntegrityError(
                "durable WAL ACK does not match the exact durable frame boundary"
            )

    def _preserve_and_truncate_tail(self, path: Path, scan: WalScanV2) -> None:
        assert scan.torn_tail_offset is not None
        original_size = path.stat().st_size
        with path.open("rb") as handle:
            handle.seek(scan.torn_tail_offset)
            discarded = handle.read()
        if not discarded:
            raise WalIntegrityError("torn WAL tail marker has no discarded bytes")
        discarded_path = path.with_name(path.name + ".torn-tail")
        receipt_path = path.with_name(path.name + ".recovery.json")
        if discarded_path.exists() or receipt_path.exists():
            raise WalIntegrityError("WAL recovery evidence already exists")
        _write_new_fsynced(discarded_path, discarded)
        with path.open("rb+") as handle:
            handle.truncate(scan.complete_bytes)
            handle.flush()
            os.fsync(handle.fileno())
        receipt = WalRecoveryReceiptV2(
            partial_file=path.name,
            segment_sequence=self._segment_sequence,
            original_bytes=original_size,
            retained_bytes=scan.complete_bytes,
            discarded_bytes=len(discarded),
            discarded_sha256=hashlib.sha256(discarded).hexdigest(),
            discarded_file=discarded_path.name,
            last_complete_ingest_seq=scan.last_ingest_seq or self._last_ingest_seq,
        )
        self._replace_json(receipt_path, asdict(receipt))

    def _write_state(self, *, active_file: str | None, durable_offset: int) -> None:
        state = WalDurableStateV2(
            authority_sha256=self.authority.sha256,
            active_file=active_file,
            segment_sequence=self._segment_sequence,
            previous_segment_sha256=self._previous_segment_sha256,
            durable_offset=durable_offset,
            durable_ack_seq=self._durable_ack_seq,
            last_committed_block_sequence=0,
            qualification_id=self.policy.qualification_id,
            fsync_candidate_id=self.policy.fsync_candidate_id,
            acknowledged_loss_upper_bound_records=self.policy.max_unsynced_records,
        )
        self._replace_json(self.directory / "wal-state.json", asdict(state))

    def _replace_json(self, path: Path, payload: dict[str, Any]) -> None:
        encoded = _canonical_json_bytes(payload) + b"\n"
        old_size = path.stat().st_size if path.exists() else 0
        self._ensure_capacity(max(0, len(encoded) - old_size))
        _atomic_replace_bytes(path, encoded)
        self._known_disk_bytes += len(encoded) - old_size

    def _ensure_capacity(self, additional_bytes: int) -> None:
        if (
            self._known_disk_bytes
            + additional_bytes
            + self.emergency_reserve_bytes
            > self.maximum_total_bytes
        ):
            raise WalCapacityError("WAL write would consume the reserved disk budget")

    def _call_fault(self, point: str) -> None:
        if self._fault_hook is not None:
            self._fault_hook(point)

    def _raise_if_failed(self) -> None:
        if self._failed is not None:
            raise WalError("WAL writer is failed and cannot be reused") from self._failed


def encode_wal_frame(encoded_line: bytes) -> bytes:
    if not encoded_line.endswith(b"\n") or encoded_line.count(b"\n") != 1:
        raise WalIntegrityError("WAL payload must be exactly one JSONL record")
    return (
        encode_uvarint(len(encoded_line))
        + encoded_line
        + _FRAME_CRC32C.pack(crc32c(encoded_line))
    )


def encode_uvarint(value: int) -> bytes:
    """Encode an unsigned 64-bit integer as canonical LEB128/uvarint bytes."""

    if type(value) is not int or value < 0 or value > 0xFFFFFFFFFFFFFFFF:
        raise ValueError("uvarint value must be an unsigned 64-bit integer")
    encoded = bytearray()
    while value >= 0x80:
        encoded.append((value & 0x7F) | 0x80)
        value >>= 7
    encoded.append(value)
    return bytes(encoded)


def crc32c(payload: bytes) -> int:
    """Return CRC-32C (Castagnoli), independent of platform zlib variants."""

    crc = 0xFFFFFFFF
    for byte in payload:
        crc = _CRC32C_TABLE[(crc ^ byte) & 0xFF] ^ (crc >> 8)
    return crc ^ 0xFFFFFFFF


def scan_wal_file(
    path: str | Path,
    *,
    expected_first_ingest_seq: int,
    max_record_bytes: int,
    durable_offset: int = 0,
    consume: WalRecordConsumer | None = None,
) -> WalScanV2:
    source = Path(path)
    if expected_first_ingest_seq < 1 or max_record_bytes < 1 or durable_offset < 0:
        raise ValueError("WAL scan bounds must be positive")
    file_size = source.stat().st_size
    first: int | None = None
    last: int | None = None
    count = 0
    encoded_bytes = 0
    expected = expected_first_ingest_seq
    complete_bytes = 0
    durable_is_boundary = durable_offset == 0
    durable_boundary_ingest_seq = expected_first_ingest_seq - 1 if durable_offset == 0 else None
    with source.open("rb") as handle:
        while handle.tell() < file_size:
            frame_start = handle.tell()
            decoded_length = _read_uvarint(handle, file_size=file_size)
            if decoded_length is None:
                return WalScanV2(
                    first,
                    last,
                    count,
                    encoded_bytes,
                    complete_bytes,
                    frame_start,
                    durable_is_boundary,
                    durable_boundary_ingest_seq,
                )
            length, _ = decoded_length
            if length < 1 or length > max_record_bytes:
                raise WalIntegrityError("WAL frame length is outside its sealed bound")
            required = length + _FRAME_CRC32C.size
            if file_size - handle.tell() < required:
                return WalScanV2(
                    first,
                    last,
                    count,
                    encoded_bytes,
                    complete_bytes,
                    frame_start,
                    durable_is_boundary,
                    durable_boundary_ingest_seq,
                )
            line = handle.read(length)
            stored_crc = _FRAME_CRC32C.unpack(handle.read(_FRAME_CRC32C.size))[0]
            if crc32c(line) != stored_crc:
                raise WalIntegrityError("complete WAL frame CRC-32C mismatch")
            if not line.endswith(b"\n") or line.count(b"\n") != 1:
                raise WalIntegrityError("complete WAL frame is not exactly one JSONL record")
            try:
                raw = json.loads(line)
            except (UnicodeDecodeError, json.JSONDecodeError) as exc:
                raise WalIntegrityError("complete WAL frame contains invalid JSON") from exc
            ingest_seq = raw.get("ingest_seq") if isinstance(raw, dict) else None
            if type(ingest_seq) is not int or ingest_seq != expected:
                raise WalIntegrityError("WAL frame ingest sequence is not contiguous")
            if first is None:
                first = ingest_seq
            last = ingest_seq
            if consume is not None:
                consume(ingest_seq, line)
            expected += 1
            count += 1
            encoded_bytes += len(line)
            complete_bytes = handle.tell()
            if durable_offset == complete_bytes:
                durable_is_boundary = True
                durable_boundary_ingest_seq = ingest_seq
    return WalScanV2(
        first,
        last,
        count,
        encoded_bytes,
        complete_bytes,
        None,
        durable_is_boundary,
        durable_boundary_ingest_seq,
    )


def _read_uvarint(
    handle: BinaryIO,
    *,
    file_size: int,
) -> tuple[int, bytes] | None:
    """Read one canonical uvarint, returning ``None`` only for a torn EOF prefix."""

    prefix = bytearray()
    value = 0
    for index in range(_MAX_UVARINT_BYTES):
        if handle.tell() >= file_size:
            return None
        raw = handle.read(1)
        if len(raw) != 1:
            return None
        byte = raw[0]
        prefix.append(byte)
        if index == _MAX_UVARINT_BYTES - 1 and byte > 1:
            raise WalIntegrityError("WAL frame uvarint exceeds unsigned 64-bit range")
        value |= (byte & 0x7F) << (7 * index)
        if byte & 0x80 == 0:
            encoded = bytes(prefix)
            if encoded != encode_uvarint(value):
                raise WalIntegrityError("WAL frame uvarint is not minimally encoded")
            return value, encoded
    raise WalIntegrityError("WAL frame uvarint exceeds unsigned 64-bit range")


def verify_wal_segments(
    directory: str | Path,
    *,
    authority: WalAuthorityV2,
    policy: WalSyncPolicyV2,
    allow_finalized_orphan: bool = False,
) -> list[WalSegmentManifestV2]:
    root = Path(directory)
    manifests: list[WalSegmentManifestV2] = []
    for path in sorted(root.glob("wal-*.manifest.json")):
        sequence = _sequence_from_name(path, _MANIFEST_RE)
        try:
            raw = json.loads(path.read_text(encoding="utf-8"))
            manifest = WalSegmentManifestV2(**raw)
        except (OSError, UnicodeDecodeError, json.JSONDecodeError, TypeError) as exc:
            raise WalIntegrityError(f"invalid WAL manifest: {path.name}") from exc
        if manifest.segment_sequence != sequence:
            raise WalIntegrityError("WAL manifest sequence differs from its filename")
        manifests.append(manifest)
    previous: str | None = None
    expected_ingest = 1
    seen_data: set[str] = set()
    for expected_sequence, manifest in enumerate(manifests, start=1):
        if manifest.schema_version != "r4b_v2_wal_segment_manifest_v1":
            raise WalIntegrityError("unsupported WAL manifest schema")
        if manifest.segment_sequence != expected_sequence:
            raise WalIntegrityError("WAL manifest sequence is not contiguous")
        if manifest.previous_segment_sha256 != previous:
            raise WalIntegrityError("WAL previous-segment hash chain is broken")
        if manifest.authority_sha256 != authority.sha256:
            raise WalIntegrityError("WAL manifest authority hash differs")
        if (
            manifest.qualification_id != policy.qualification_id
            or manifest.fsync_candidate_id != policy.fsync_candidate_id
        ):
            raise WalIntegrityError("WAL manifest qualification candidate differs")
        if Path(manifest.data_file).name != manifest.data_file:
            raise WalIntegrityError("WAL manifest data filename is not local")
        if manifest.data_file in seen_data:
            raise WalIntegrityError("WAL manifests reuse a data file")
        seen_data.add(manifest.data_file)
        data_path = root / manifest.data_file
        if _sequence_from_name(data_path, _FINAL_RE) != expected_sequence:
            raise WalIntegrityError("WAL data sequence differs from its manifest")
        if not data_path.is_file() or data_path.stat().st_size != manifest.wal_bytes:
            raise WalIntegrityError("WAL data file is missing or has the wrong size")
        if _sha256_file(data_path) != manifest.sha256:
            raise WalIntegrityError("WAL data SHA-256 differs from its manifest")
        scan = scan_wal_file(
            data_path,
            expected_first_ingest_seq=expected_ingest,
            max_record_bytes=policy.max_record_bytes,
        )
        if scan.torn_tail_offset is not None:
            raise WalIntegrityError("finalized WAL segment has a torn tail")
        actual = (
            scan.first_ingest_seq,
            scan.last_ingest_seq,
            scan.record_count,
            scan.encoded_bytes,
            scan.complete_bytes,
        )
        expected = (
            manifest.first_ingest_seq,
            manifest.last_ingest_seq,
            manifest.record_count,
            manifest.encoded_bytes,
            manifest.wal_bytes,
        )
        if actual != expected or manifest.durable_ack_seq != manifest.last_ingest_seq:
            raise WalIntegrityError("WAL record metadata differs from its manifest")
        expected_ingest = manifest.last_ingest_seq + 1
        previous = manifest.sha256
    final_files = {path.name for path in root.glob("wal-*.wal")}
    orphan_files = final_files - seen_data
    if seen_data - final_files:
        raise WalIntegrityError("WAL finalized data and manifest sets differ")
    if orphan_files:
        expected_orphan = f"wal-{len(manifests) + 1:08d}.wal"
        if not allow_finalized_orphan or orphan_files != {expected_orphan}:
            raise WalIntegrityError("WAL finalized data and manifest sets differ")
    return manifests


def _build_crc32c_table() -> tuple[int, ...]:
    values: list[int] = []
    for index in range(256):
        crc = index
        for _ in range(8):
            crc = (crc >> 1) ^ (0x82F63B78 if crc & 1 else 0)
        values.append(crc)
    return tuple(values)


_CRC32C_TABLE = _build_crc32c_table()


def _canonical_json_bytes(value: object) -> bytes:
    return canonical_json_line(value).removesuffix(b"\n")


def _read_state(path: Path) -> WalDurableStateV2 | None:
    if not path.exists():
        return None
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
        state = WalDurableStateV2(**raw)
    except (OSError, UnicodeDecodeError, json.JSONDecodeError, TypeError) as exc:
        raise WalIntegrityError("invalid durable WAL state") from exc
    if state.schema_version != "r4b_v2_wal_durable_state_v1":
        raise WalIntegrityError("unsupported durable WAL state schema")
    return state


def _finalized_orphan(
    directory: Path,
    manifests: Sequence[WalSegmentManifestV2],
) -> Path | None:
    manifested = {manifest.data_file for manifest in manifests}
    orphans = sorted(path for path in directory.glob("wal-*.wal") if path.name not in manifested)
    if not orphans:
        return None
    if len(orphans) != 1:
        raise WalIntegrityError("multiple finalized WAL orphans are not recoverable")
    return orphans[0]


def _sequence_from_name(path: Path, pattern: re.Pattern[str]) -> int:
    match = pattern.fullmatch(path.name)
    if match is None:
        raise WalIntegrityError(f"invalid WAL filename: {path.name}")
    return int(match.group("sequence"))


def _write_all(handle: BinaryIO, payload: bytes | bytearray) -> None:
    written = handle.write(payload)
    if written != len(payload):
        raise WalShortWriteError(
            f"WAL short write: expected {len(payload)} bytes, wrote {written}"
        )


def _write_new_fsynced(path: Path, payload: bytes) -> None:
    with path.open("xb", buffering=0) as handle:
        _write_all(handle, payload)
        os.fsync(handle.fileno())
    _fsync_parent(path)


def _atomic_replace_bytes(path: Path, payload: bytes) -> None:
    temporary = path.with_name(path.name + ".partial")
    if temporary.exists():
        raise WalIntegrityError(f"atomic WAL residue requires audit: {temporary.name}")
    with temporary.open("xb", buffering=0) as handle:
        _write_all(handle, payload)
        os.fsync(handle.fileno())
    os.replace(temporary, path)
    _fsync_parent(path)
    _fsync_path(path)


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


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
