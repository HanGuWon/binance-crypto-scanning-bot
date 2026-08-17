"""Durable, append-only owner for one irreversible D1 historical attempt.

The owner is deliberately independent from the historical runner and artifact
publisher.  ``ARMED`` is created without invoking a callback.  Every later
transition appends to the same inode under an operating-system file lock and
syncs the WAL before returning.

Each binary frame contains exactly one RFC 8785 canonical JSONL record.  A
fixed magic/length header makes an incomplete final append distinguishable
from corruption in an earlier complete frame.  Recovery is observation only:
the loader reports a typed torn tail and never truncates, repairs, retries, or
deletes any bytes.

On POSIX, each successful record sync is followed by fsync of the attempt
directory and its parent.  On supported Windows hosts, creation is restricted
to local fixed NTFS and directory entries are flushed through writeable
``CreateFileW`` directory handles and ``FlushFileBuffers``.  The recorded
threat model covers process and power loss on a trusted local filesystem.  It
explicitly excludes both privileged full-volume snapshot rollback and an
active actor restoring or replacing bytes that had already crossed a
successful durable-file boundary.  Process code is trusted: the grant is
sealed only at the public-API correctness boundary.  In-process reflection,
adversarial object mutation, and direct runner invocation are excluded.
"""

from __future__ import annotations

import ctypes
import hashlib
import json
import os
import re
import stat
import struct
import sys
import threading
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from dataclasses import asdict, dataclass, replace
from pathlib import Path
from typing import Final, Literal, Never, TypeVar, cast, final

from signalbot.r4b_v2.canonical import canonical_json_line

D1_ATTEMPT_WAL_FILE_V0: Final = "attempt.wal"
D1_ATTEMPT_START_SEAL_FILE_V0: Final = "start.seal"
D1_ATTEMPT_WAL_MAX_RECORD_BYTES_V0: Final = 16 * 1024
D1_ATTEMPT_WAL_MAX_BYTES_V0: Final = 64 * 1024
D1_ATTEMPT_WAL_MAX_RECORDS_V0: Final = 4
D1_ATTEMPT_START_SEAL_MAX_BYTES_V0: Final = 8 * 1024
D1_ATTEMPT_DIRECTORY_MAX_ENTRIES_V0: Final = 2
D1_ATTEMPT_MAX_SAFE_TIMESTAMP_MS_V0: Final = (1 << 53) - 1

D1_ATTEMPT_WAL_POSIX_DURABILITY_CONTRACT_V0: Final = (
    "POSIX_WAL_FILE_ATTEMPT_DIRECTORY_AND_PARENT_FSYNC_V0"
)
D1_ATTEMPT_WAL_WINDOWS_DURABILITY_CONTRACT_V0: Final = (
    "WINDOWS_LOCAL_FIXED_NTFS_WAL_START_SEAL_AND_DIRECTORY_FLUSH_V0"
)
D1_ATTEMPT_WAL_THREAT_MODEL_V0: Final = (
    "TRUSTED_LOCAL_FILESYSTEM_AND_PROCESS_CODE_PROCESS_AND_POWER_LOSS_PUBLIC_API_"
    "SEALED_ACTIVE_DURABLE_FILE_RESTORE_PRIVILEGED_FULL_SNAPSHOT_ROLLBACK_IN_PROCESS_"
    "REFLECTION_ADVERSARIAL_MUTATION_AND_DIRECT_RUNNER_INVOCATION_EXCLUDED_V0"
)

D1AttemptWalStateV0 = Literal[
    "ARMED",
    "STARTED_BEFORE_OUTCOME_ACCESS",
    "COMPLETED",
    "FAILED",
    "AMBIGUOUS_OUTPUT",
]
D1AttemptWalTerminalStateV0 = Literal["COMPLETED", "FAILED", "AMBIGUOUS_OUTPUT"]
D1AttemptWalTornTailKindV0 = Literal["TORN_FRAME_HEADER", "TORN_FRAME_PAYLOAD"]
D1AttemptWalDurabilityContractV0 = Literal[
    "POSIX_WAL_FILE_ATTEMPT_DIRECTORY_AND_PARENT_FSYNC_V0",
    "WINDOWS_LOCAL_FIXED_NTFS_WAL_START_SEAL_AND_DIRECTORY_FLUSH_V0",
]

_SCHEMA_VERSION: Final = "d1_historical_attempt_wal_record_v0"
_FRAME_VERSION: Final = "fixed_u32be_length_rfc8785_jsonl_v0"
_FRAME_MAGIC: Final = b"D1W0"
_FRAME_HEADER: Final = struct.Struct(">4sI")
_RECORD_HASH_DOMAIN: Final = b"D1_HISTORICAL_ATTEMPT_WAL_RECORD_V0\0"
_BINDINGS_HASH_DOMAIN: Final = b"D1_HISTORICAL_ATTEMPT_WAL_BINDINGS_V0\0"
_PREFIX_HASH_DOMAIN: Final = b"D1_HISTORICAL_ATTEMPT_WAL_PREFIX_V0\0"
_GENERATION_HASH_DOMAIN: Final = b"D1_HISTORICAL_ATTEMPT_WAL_DIRECTORY_GENERATION_V0\0"
_START_SEAL_HASH_DOMAIN: Final = b"D1_HISTORICAL_ATTEMPT_WAL_START_SEAL_V0\0"
_SHA256_RE: Final = re.compile(r"^[0-9a-f]{64}$")
_RUN_ID_RE: Final = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")
_DETAIL_CODE_RE: Final = re.compile(r"^[A-Z0-9][A-Z0-9_]{0,127}$")
_RECORD_KEYS: Final = frozenset(
    {
        "artifact_manifest_sha256",
        "attempt_directory_sha256",
        "bindings",
        "bindings_sha256",
        "detail_code",
        "directory_durability_contract",
        "frame_version",
        "observed_at_ms",
        "previous_record_sha256",
        "production_order_placement",
        "record_sha256",
        "result_sha256",
        "schema_version",
        "sequence",
        "state",
    }
)
_BINDING_KEYS: Final = frozenset(
    {
        "code_freeze_manifest_sha256",
        "funding_authority_file_sha256",
        "input_authority_file_sha256",
        "input_authority_sha256",
        "output_path_sha256",
        "preregistration_sha256",
        "run_id",
    }
)
_START_SEAL_KEYS: Final = frozenset(
    {
        "attempt_directory_sha256",
        "bindings_sha256",
        "directory_durability_contract",
        "prefix_sha256",
        "production_order_placement",
        "schema_version",
        "seal_sha256",
        "start_record_sha256",
        "started_at_ms",
        "threat_model",
    }
)
_START_SEAL_SCHEMA_VERSION: Final = "d1_historical_attempt_start_seal_v0"
_OutcomeT = TypeVar("_OutcomeT")


class D1HistoricalAttemptWalErrorV0(RuntimeError):
    """Base error for the immutable D1 attempt WAL contract."""


class D1HistoricalAttemptWalIntegrityErrorV0(D1HistoricalAttemptWalErrorV0):
    """Raised when durable bytes, identities, or canonical records are invalid."""


class D1HistoricalAttemptWalStateErrorV0(D1HistoricalAttemptWalErrorV0):
    """Raised when a requested state transition is forbidden."""


class D1HistoricalAttemptWalConcurrentWriteErrorV0(D1HistoricalAttemptWalErrorV0):
    """Raised for a busy writer lock or a stale expected-prefix capability."""


class D1HistoricalAttemptWalDurabilityErrorV0(D1HistoricalAttemptWalErrorV0):
    """Raised when an append or durability boundary cannot be proved."""


@dataclass(frozen=True, slots=True)
class D1AttemptWalBindingsV0:
    """Exact immutable identities bound into every attempt record."""

    run_id: str
    code_freeze_manifest_sha256: str
    input_authority_sha256: str
    input_authority_file_sha256: str
    funding_authority_file_sha256: str
    preregistration_sha256: str
    output_path_sha256: str

    def __post_init__(self) -> None:
        if not isinstance(self.run_id, str) or _RUN_ID_RE.fullmatch(self.run_id) is None:
            raise D1HistoricalAttemptWalIntegrityErrorV0("run_id is not fixed safe text")
        for value, label in (
            (self.code_freeze_manifest_sha256, "code_freeze_manifest_sha256"),
            (self.input_authority_sha256, "input_authority_sha256"),
            (self.input_authority_file_sha256, "input_authority_file_sha256"),
            (self.funding_authority_file_sha256, "funding_authority_file_sha256"),
            (self.preregistration_sha256, "preregistration_sha256"),
            (self.output_path_sha256, "output_path_sha256"),
        ):
            _require_sha256(value, label)

    @property
    def bindings_sha256(self) -> str:
        """Return the canonical aggregate identity for these exact bindings."""

        return hashlib.sha256(
            _BINDINGS_HASH_DOMAIN + canonical_json_line(asdict(self))
        ).hexdigest()


@dataclass(frozen=True, slots=True)
class D1AttemptWalRecordV0:
    """One fully framed, canonical, hash-chained attempt state record."""

    state: D1AttemptWalStateV0
    sequence: int
    previous_record_sha256: str | None
    record_sha256: str
    bindings: D1AttemptWalBindingsV0
    bindings_sha256: str
    attempt_directory_sha256: str
    observed_at_ms: int
    detail_code: str | None
    result_sha256: str | None
    artifact_manifest_sha256: str | None
    directory_durability_contract: D1AttemptWalDurabilityContractV0
    production_order_placement: Literal[False] = False
    schema_version: Literal["d1_historical_attempt_wal_record_v0"] = _SCHEMA_VERSION
    frame_version: Literal["fixed_u32be_length_rfc8785_jsonl_v0"] = _FRAME_VERSION

    def __post_init__(self) -> None:
        _validate_record_fields(self)


@dataclass(frozen=True, slots=True)
class D1AttemptWalPrefixV0:
    """Optimistic append capability for one exact complete WAL prefix."""

    record_count: int
    complete_bytes: int
    last_record_sha256: str
    prefix_sha256: str

    def __post_init__(self) -> None:
        if (
            type(self.record_count) is not int
            or self.record_count < 1
            or self.record_count > D1_ATTEMPT_WAL_MAX_RECORDS_V0
        ):
            raise D1HistoricalAttemptWalIntegrityErrorV0("prefix record_count is invalid")
        if (
            type(self.complete_bytes) is not int
            or self.complete_bytes < 1
            or self.complete_bytes > D1_ATTEMPT_WAL_MAX_BYTES_V0
        ):
            raise D1HistoricalAttemptWalIntegrityErrorV0("prefix complete_bytes is invalid")
        _require_sha256(self.last_record_sha256, "last_record_sha256")
        _require_sha256(self.prefix_sha256, "prefix_sha256")


@dataclass(frozen=True, slots=True)
class D1AttemptWalTornTailV0:
    """Read-only evidence for bytes after the longest valid complete prefix."""

    kind: D1AttemptWalTornTailKindV0
    offset_bytes: int
    length_bytes: int
    tail_sha256: str

    def __post_init__(self) -> None:
        if self.kind not in {"TORN_FRAME_HEADER", "TORN_FRAME_PAYLOAD"}:
            raise D1HistoricalAttemptWalIntegrityErrorV0("torn-tail kind is unsupported")
        if type(self.offset_bytes) is not int or self.offset_bytes < 1:
            raise D1HistoricalAttemptWalIntegrityErrorV0("torn-tail offset is invalid")
        if type(self.length_bytes) is not int or self.length_bytes < 1:
            raise D1HistoricalAttemptWalIntegrityErrorV0("torn-tail length is invalid")
        _require_sha256(self.tail_sha256, "tail_sha256")


@dataclass(frozen=True, slots=True)
class D1AttemptStartSealV0:
    """Canonical durable marker authorizing outcome access after one exact START."""

    start_record_sha256: str
    prefix_sha256: str
    bindings_sha256: str
    attempt_directory_sha256: str
    started_at_ms: int
    directory_durability_contract: D1AttemptWalDurabilityContractV0
    seal_sha256: str
    production_order_placement: Literal[False] = False
    threat_model: Literal[
        "TRUSTED_LOCAL_FILESYSTEM_AND_PROCESS_CODE_PROCESS_AND_POWER_LOSS_PUBLIC_API_SEALED_ACTIVE_DURABLE_FILE_RESTORE_PRIVILEGED_FULL_SNAPSHOT_ROLLBACK_IN_PROCESS_REFLECTION_ADVERSARIAL_MUTATION_AND_DIRECT_RUNNER_INVOCATION_EXCLUDED_V0"
    ] = D1_ATTEMPT_WAL_THREAT_MODEL_V0
    schema_version: Literal["d1_historical_attempt_start_seal_v0"] = (
        _START_SEAL_SCHEMA_VERSION
    )

    def __post_init__(self) -> None:
        for value, label in (
            (self.start_record_sha256, "start_record_sha256"),
            (self.prefix_sha256, "prefix_sha256"),
            (self.bindings_sha256, "bindings_sha256"),
            (self.attempt_directory_sha256, "attempt_directory_sha256"),
            (self.seal_sha256, "seal_sha256"),
        ):
            _require_sha256(value, label)
        _require_timestamp_ms(self.started_at_ms, "started_at_ms")
        if self.directory_durability_contract not in {
            D1_ATTEMPT_WAL_POSIX_DURABILITY_CONTRACT_V0,
            D1_ATTEMPT_WAL_WINDOWS_DURABILITY_CONTRACT_V0,
        }:
            raise D1HistoricalAttemptWalIntegrityErrorV0(
                "START seal durability contract is unsupported"
            )
        if self.production_order_placement is not False:
            raise D1HistoricalAttemptWalIntegrityErrorV0(
                "START seal cannot authorize production order placement"
            )
        if self.threat_model != D1_ATTEMPT_WAL_THREAT_MODEL_V0:
            raise D1HistoricalAttemptWalIntegrityErrorV0(
                "START seal threat model differs"
            )
        if self.schema_version != _START_SEAL_SCHEMA_VERSION:
            raise D1HistoricalAttemptWalIntegrityErrorV0(
                "START seal schema is unsupported"
            )


@dataclass(frozen=True, slots=True)
class D1AttemptWalSnapshotV0:
    """Verified complete prefix plus optional non-mutating torn-tail evidence."""

    attempt_dir: Path
    wal_path: Path
    records: tuple[D1AttemptWalRecordV0, ...]
    prefix: D1AttemptWalPrefixV0
    total_file_bytes: int
    torn_tail: D1AttemptWalTornTailV0 | None
    start_seal: D1AttemptStartSealV0 | None = None
    start_seal_torn: bool = False

    def __post_init__(self) -> None:
        if not self.attempt_dir.is_absolute() or not self.wal_path.is_absolute():
            raise D1HistoricalAttemptWalIntegrityErrorV0("snapshot paths must be absolute")
        if self.wal_path != self.attempt_dir / D1_ATTEMPT_WAL_FILE_V0:
            raise D1HistoricalAttemptWalIntegrityErrorV0("snapshot WAL path is not fixed")
        if not self.records or len(self.records) != self.prefix.record_count:
            raise D1HistoricalAttemptWalIntegrityErrorV0("snapshot record count differs")
        if self.records[-1].record_sha256 != self.prefix.last_record_sha256:
            raise D1HistoricalAttemptWalIntegrityErrorV0("snapshot tail hash differs")
        if (
            type(self.total_file_bytes) is not int
            or self.total_file_bytes < self.prefix.complete_bytes
            or self.total_file_bytes > D1_ATTEMPT_WAL_MAX_BYTES_V0
        ):
            raise D1HistoricalAttemptWalIntegrityErrorV0("snapshot byte total is invalid")
        expected_tail_bytes = self.total_file_bytes - self.prefix.complete_bytes
        if self.torn_tail is None:
            if expected_tail_bytes != 0:
                raise D1HistoricalAttemptWalIntegrityErrorV0("snapshot omits a torn tail")
        elif (
            self.torn_tail.offset_bytes != self.prefix.complete_bytes
            or self.torn_tail.length_bytes != expected_tail_bytes
        ):
            raise D1HistoricalAttemptWalIntegrityErrorV0("snapshot torn-tail bounds differ")
        if type(self.start_seal_torn) is not bool:
            raise D1HistoricalAttemptWalIntegrityErrorV0(
                "snapshot START-seal torn flag must be boolean"
            )
        if self.start_seal is not None and self.start_seal_torn:
            raise D1HistoricalAttemptWalIntegrityErrorV0(
                "snapshot cannot contain both valid and torn START seal"
            )

    @property
    def start_seal_valid(self) -> bool:
        """Report audit evidence only; this property never grants outcome access."""

        return self.start_seal is not None and not self.start_seal_torn

    @property
    def last_state(self) -> D1AttemptWalStateV0:
        """Return the effective state of the longest complete prefix."""

        return self.records[-1].state

    @property
    def bindings(self) -> D1AttemptWalBindingsV0:
        """Return the bindings repeated and verified across the complete prefix."""

        return self.records[0].bindings


@final
class D1OutcomeAccessGrantV0:
    """Public-API-sealed, one-use authority for trusted-process callback entry.

    A durable START and its seal are audit evidence, not executable authority.
    Only the successful return path of :func:`append_started_v0` can mint this
    non-serializable capability.  Consumption is committed before the callback
    is entered, so a callback exception cannot make the capability reusable.
    This is an operator-correctness boundary, not protection against hostile
    in-process reflection, mutation, or direct runner invocation.
    """

    __slots__ = (
        "_attempt_directory_sha256",
        "_bindings",
        "_consume_lock",
        "_consumed",
        "_mint_process_id",
        "_start_prefix",
        "_start_record_sha256",
        "_start_seal_sha256",
    )

    _attempt_directory_sha256: str
    _bindings: D1AttemptWalBindingsV0
    _consume_lock: threading.Lock
    _consumed: bool
    _mint_process_id: int
    _start_prefix: D1AttemptWalPrefixV0
    _start_record_sha256: str
    _start_seal_sha256: str

    def __new__(cls, *_args: object, **_kwargs: object) -> Never:
        raise TypeError(
            "D1OutcomeAccessGrantV0 is public-API factory-sealed and cannot be constructed directly"
        )

    def __init_subclass__(cls, **_kwargs: object) -> Never:
        raise TypeError("D1OutcomeAccessGrantV0 cannot be subclassed")

    def __setattr__(self, _name: str, _value: object) -> Never:
        raise TypeError("D1OutcomeAccessGrantV0 fields are immutable")

    @property
    def start_record_sha256(self) -> str:
        """Return the exact START record identity bound to this capability."""

        return self._start_record_sha256

    @property
    def start_prefix(self) -> D1AttemptWalPrefixV0:
        """Return the exact two-record WAL prefix bound to this capability."""

        return self._start_prefix

    @property
    def bindings(self) -> D1AttemptWalBindingsV0:
        """Return the full immutable attempt bindings bound to this capability."""

        return self._bindings

    @property
    def attempt_directory_sha256(self) -> str:
        """Return the durable attempt-directory generation identity."""

        return self._attempt_directory_sha256

    @property
    def consumed(self) -> bool:
        """Return whether callback entry has already been irreversibly claimed."""

        with self._consume_lock:
            return self._consumed

    @property
    def mint_process_id(self) -> int:
        """Return the only process allowed to consume this ephemeral grant."""

        return self._mint_process_id

    def consume_once_v0(self, callback: Callable[[], _OutcomeT]) -> _OutcomeT:
        """Enter ``callback`` at most once, marking consumption before entry."""

        if not callable(callback):
            raise TypeError("outcome callback must be callable")
        current_process_id = os.getpid()
        if (
            type(current_process_id) is not int
            or current_process_id <= 0
            or current_process_id != self._mint_process_id
        ):
            # This check deliberately precedes the lock.  A child forked while
            # another thread held the lock inherits an unusably locked object.
            object.__setattr__(self, "_consumed", True)
            raise D1HistoricalAttemptWalStateErrorV0(
                "outcome access grant belongs to another process"
            )
        with self._consume_lock:
            if self._consumed:
                raise D1HistoricalAttemptWalStateErrorV0(
                    "outcome access grant was already consumed"
                )
            object.__setattr__(self, "_consumed", True)
        return callback()

    def __copy__(self) -> Never:
        raise TypeError("D1OutcomeAccessGrantV0 cannot be copied")

    def __deepcopy__(self, _memo: dict[int, object]) -> Never:
        raise TypeError("D1OutcomeAccessGrantV0 cannot be deep-copied")

    def __reduce__(self) -> Never:
        raise TypeError("D1OutcomeAccessGrantV0 cannot be serialized")

    def __reduce_ex__(self, _protocol: object) -> Never:
        raise TypeError("D1OutcomeAccessGrantV0 cannot be serialized")

    def __getstate__(self) -> Never:
        raise TypeError("D1OutcomeAccessGrantV0 cannot be serialized")


@dataclass(frozen=True, slots=True)
class D1AppendStartedResultV0:
    """Durable START snapshot plus its sole ephemeral outcome capability."""

    snapshot: D1AttemptWalSnapshotV0
    outcome_access_grant: D1OutcomeAccessGrantV0

    def __post_init__(self) -> None:
        if type(self.snapshot) is not D1AttemptWalSnapshotV0:
            raise TypeError("snapshot must be an exact D1AttemptWalSnapshotV0")
        if type(self.outcome_access_grant) is not D1OutcomeAccessGrantV0:
            raise TypeError(
                "outcome_access_grant must be an exact D1OutcomeAccessGrantV0"
            )
        if not _grant_matches_start_snapshot(self.outcome_access_grant, self.snapshot):
            raise D1HistoricalAttemptWalIntegrityErrorV0(
                "outcome access grant differs from its exact durable START"
            )


@dataclass(frozen=True, slots=True)
class _FileMetadataV0:
    device: int
    inode: int
    mode: int
    links: int
    size: int
    modified_ns: int
    changed_ns: int


@dataclass(frozen=True, slots=True)
class _DirectoryIdentityV0:
    device: int
    inode: int
    os_family: str
    volume_identity: str


@dataclass(frozen=True, slots=True)
class _LockedWalV0:
    descriptor: int
    attempt_dir: Path
    wal_path: Path
    directory_identity: _DirectoryIdentityV0
    file_identity: _FileMetadataV0


def create_armed_wal_v0(
    *,
    attempt_dir: str | Path,
    bindings: D1AttemptWalBindingsV0,
    armed_at_ms: int,
) -> D1AttemptWalSnapshotV0:
    """Create a fresh fixed directory containing one durable ``ARMED`` record.

    This function accepts values only.  It has no runner, publisher, callback,
    or context-manager parameter and cannot initiate outcome access.
    """

    if type(bindings) is not D1AttemptWalBindingsV0:
        raise TypeError("bindings must be an exact D1AttemptWalBindingsV0")
    _require_timestamp_ms(armed_at_ms, "armed_at_ms")
    target = _creation_target(attempt_dir)

    try:
        os.mkdir(target, 0o700)
    except FileExistsError as error:
        raise D1HistoricalAttemptWalStateErrorV0(
            "attempt directory already exists; no retry or replacement is allowed"
        ) from error
    except OSError as error:
        raise D1HistoricalAttemptWalDurabilityErrorV0(
            "cannot create the fresh attempt directory"
        ) from error

    wal_path = target / D1_ATTEMPT_WAL_FILE_V0
    descriptor: int | None = None
    try:
        directory_identity = _require_attempt_directory(target, require_wal=False)
        attempt_directory_sha256 = _attempt_directory_sha256(
            target,
            directory_identity,
        )
        contract = _durability_contract_for_directory(target)
        record = _mint_record(
            state="ARMED",
            sequence=0,
            previous_record_sha256=None,
            bindings=bindings,
            attempt_directory_sha256=attempt_directory_sha256,
            observed_at_ms=armed_at_ms,
            detail_code=None,
            result_sha256=None,
            artifact_manifest_sha256=None,
            directory_durability_contract=contract,
        )
        frame = _encode_frame(record)
        if len(frame) > D1_ATTEMPT_WAL_MAX_BYTES_V0:
            raise D1HistoricalAttemptWalIntegrityErrorV0(
                "ARMED frame exceeds the WAL cap"
            )
        descriptor = os.open(wal_path, _new_wal_open_flags(), 0o600)
        opened = _metadata_from_fd(descriptor)
        _require_regular_single_link(opened, "new attempt WAL")
        _write_all(descriptor, frame)
        _sync_wal_fd(descriptor)
        final = _metadata_from_fd(descriptor)
        _require_same_file_identity(opened, final, "new attempt WAL")
        if final.size != len(frame):
            raise D1HistoricalAttemptWalDurabilityErrorV0(
                "new attempt WAL size differs after ARMED append"
            )
        _require_path_matches_descriptor(wal_path, final)
        _require_same_directory_identity(
            directory_identity,
            _require_attempt_directory(target, require_wal=True),
        )
    except FileExistsError as error:
        raise D1HistoricalAttemptWalStateErrorV0(
            "attempt WAL already exists; no retry or replacement is allowed"
        ) from error
    except OSError as error:
        raise D1HistoricalAttemptWalDurabilityErrorV0(
            "ARMED append or WAL fsync failed; existing bytes require audit"
        ) from error
    finally:
        if descriptor is not None:
            try:
                os.close(descriptor)
            except OSError as error:
                raise D1HistoricalAttemptWalDurabilityErrorV0(
                    "ARMED WAL close failed; existing bytes require audit"
                ) from error

    _sync_attempt_directory_and_parent(target)
    return load_attempt_wal_v0(target, expected_bindings=bindings)


def load_attempt_wal_v0(
    attempt_dir: str | Path,
    *,
    expected_bindings: D1AttemptWalBindingsV0 | None = None,
) -> D1AttemptWalSnapshotV0:
    """Load the longest valid complete prefix without modifying a torn tail."""

    if expected_bindings is not None and type(expected_bindings) is not D1AttemptWalBindingsV0:
        raise TypeError("expected_bindings must be an exact D1AttemptWalBindingsV0")
    target = _existing_attempt_target(attempt_dir)
    with _open_locked_wal(target) as locked:
        raw = _read_locked_wal(locked)
        snapshot = _parse_wal_bytes(target, raw)
        _require_snapshot_generation(snapshot, locked.directory_identity)
        snapshot = _attach_and_validate_start_seal(locked, snapshot)
        if expected_bindings is not None and snapshot.bindings != expected_bindings:
            raise D1HistoricalAttemptWalIntegrityErrorV0(
                "attempt WAL bindings differ from the expected authority"
            )
        return snapshot


def append_started_v0(
    *,
    attempt_dir: str | Path,
    expected_prefix: D1AttemptWalPrefixV0,
    started_at_ms: int,
) -> D1AppendStartedResultV0:
    """Durably append START and return its sole ephemeral outcome capability."""

    _require_timestamp_ms(started_at_ms, "started_at_ms")
    snapshot = _append_state(
        attempt_dir=attempt_dir,
        expected_prefix=expected_prefix,
        state="STARTED_BEFORE_OUTCOME_ACCESS",
        observed_at_ms=started_at_ms,
        detail_code=None,
        result_sha256=None,
        artifact_manifest_sha256=None,
    )
    return D1AppendStartedResultV0(
        snapshot=snapshot,
        outcome_access_grant=_mint_outcome_access_grant(snapshot),
    )


def append_terminal_v0(
    *,
    attempt_dir: str | Path,
    expected_prefix: D1AttemptWalPrefixV0,
    state: D1AttemptWalTerminalStateV0,
    terminal_at_ms: int,
    detail_code: str | None = None,
    result_sha256: str | None = None,
    artifact_manifest_sha256: str | None = None,
) -> D1AttemptWalSnapshotV0:
    """Append one terminal state, or the sole COMPLETED-to-AMBIGUOUS override.

    ``FAILED`` is a caller assertion that the canonical output is absent; this
    low-level owner records but cannot independently establish that fact.
    ``AMBIGUOUS_OUTPUT`` may follow ``STARTED`` directly or override one
    ``COMPLETED`` record after a post-write verification exception.
    """

    if state not in {"COMPLETED", "FAILED", "AMBIGUOUS_OUTPUT"}:
        raise D1HistoricalAttemptWalStateErrorV0("terminal state is unsupported")
    _require_timestamp_ms(terminal_at_ms, "terminal_at_ms")
    return _append_state(
        attempt_dir=attempt_dir,
        expected_prefix=expected_prefix,
        state=state,
        observed_at_ms=terminal_at_ms,
        detail_code=detail_code,
        result_sha256=result_sha256,
        artifact_manifest_sha256=artifact_manifest_sha256,
    )


def current_d1_attempt_wal_os_durability_label_v0() -> D1AttemptWalDurabilityContractV0:
    """Return only the OS-family label; path support is validated separately."""

    if os.name == "nt":
        return D1_ATTEMPT_WAL_WINDOWS_DURABILITY_CONTRACT_V0
    return D1_ATTEMPT_WAL_POSIX_DURABILITY_CONTRACT_V0


def _append_state(
    *,
    attempt_dir: str | Path,
    expected_prefix: D1AttemptWalPrefixV0,
    state: D1AttemptWalStateV0,
    observed_at_ms: int,
    detail_code: str | None,
    result_sha256: str | None,
    artifact_manifest_sha256: str | None,
) -> D1AttemptWalSnapshotV0:
    if type(expected_prefix) is not D1AttemptWalPrefixV0:
        raise TypeError("expected_prefix must be an exact D1AttemptWalPrefixV0")
    target = _existing_attempt_target(attempt_dir)
    with _open_locked_wal(target) as locked:
        raw = _read_locked_wal(locked)
        snapshot = _parse_wal_bytes(target, raw)
        _require_snapshot_generation(snapshot, locked.directory_identity)
        snapshot = _attach_and_validate_start_seal(locked, snapshot)
        if snapshot.torn_tail is not None:
            raise D1HistoricalAttemptWalStateErrorV0(
                "torn WAL tail is immutable and forbids every later append"
            )
        if snapshot.prefix != expected_prefix:
            raise D1HistoricalAttemptWalConcurrentWriteErrorV0(
                "expected WAL prefix is stale or foreign"
            )
        previous = snapshot.records[-1]
        if (
            previous.directory_durability_contract
            != _durability_contract_for_directory(target)
        ):
            raise D1HistoricalAttemptWalStateErrorV0(
                "attempt WAL is read-only because its durability contract belongs to another host"
            )
        if len(snapshot.records) >= D1_ATTEMPT_WAL_MAX_RECORDS_V0:
            raise D1HistoricalAttemptWalStateErrorV0(
                "attempt WAL already reached its fixed terminal record cap"
            )
        if previous.state != "ARMED" and not snapshot.start_seal_valid:
            raise D1HistoricalAttemptWalStateErrorV0(
                "START has no exact durable seal; every later append is forbidden"
            )
        if not _state_transition_allowed(previous.state, state):
            raise D1HistoricalAttemptWalStateErrorV0(
                f"forbidden attempt WAL transition {previous.state}->{state}"
            )
        candidate = _mint_record(
            state=state,
            sequence=len(snapshot.records),
            previous_record_sha256=previous.record_sha256,
            bindings=previous.bindings,
            attempt_directory_sha256=previous.attempt_directory_sha256,
            observed_at_ms=observed_at_ms,
            detail_code=detail_code,
            result_sha256=result_sha256,
            artifact_manifest_sha256=artifact_manifest_sha256,
            directory_durability_contract=previous.directory_durability_contract,
        )
        _validate_transition(snapshot.records, candidate)
        frame = _encode_frame(candidate)
        combined_size = len(raw) + len(frame)
        if combined_size > D1_ATTEMPT_WAL_MAX_BYTES_V0:
            raise D1HistoricalAttemptWalStateErrorV0("append would exceed the fixed WAL cap")

        try:
            _write_all(locked.descriptor, frame)
            _sync_wal_fd(locked.descriptor)
        except OSError as error:
            raise D1HistoricalAttemptWalDurabilityErrorV0(
                "attempt WAL append/fsync failed; no retry or truncation is allowed"
            ) from error
        final = _metadata_from_fd(locked.descriptor)
        _require_same_file_identity(locked.file_identity, final, "attempt WAL append")
        if final.size != combined_size:
            raise D1HistoricalAttemptWalDurabilityErrorV0(
                "attempt WAL size differs after append"
            )
        _require_path_matches_descriptor(locked.wal_path, final)
        _require_same_directory_identity(
            locked.directory_identity,
            _require_attempt_directory(target, require_wal=True),
        )
        expected_raw = raw + frame
        durable_raw = _read_locked_wal(locked)
        if durable_raw != expected_raw:
            raise D1HistoricalAttemptWalDurabilityErrorV0(
                "post-fsync WAL readback differs from the exact appended bytes"
            )
        appended = _parse_wal_bytes(target, durable_raw)
        _require_snapshot_generation(appended, locked.directory_identity)
        if state == "STARTED_BEFORE_OUTCOME_ACCESS":
            appended = _create_and_sync_start_seal(locked, appended)
        else:
            _sync_attempt_directory_and_parent(target)
            _require_path_matches_descriptor(locked.wal_path, final)
            _require_same_directory_identity(
                locked.directory_identity,
                _require_attempt_directory(target, require_wal=True),
            )
            appended = _attach_and_validate_start_seal(locked, appended)
        return appended


def _mint_record(
    *,
    state: D1AttemptWalStateV0,
    sequence: int,
    previous_record_sha256: str | None,
    bindings: D1AttemptWalBindingsV0,
    attempt_directory_sha256: str,
    observed_at_ms: int,
    detail_code: str | None,
    result_sha256: str | None,
    artifact_manifest_sha256: str | None,
    directory_durability_contract: D1AttemptWalDurabilityContractV0,
) -> D1AttemptWalRecordV0:
    body = _record_body_document(
        state=state,
        sequence=sequence,
        previous_record_sha256=previous_record_sha256,
        bindings=bindings,
        bindings_sha256=bindings.bindings_sha256,
        attempt_directory_sha256=attempt_directory_sha256,
        observed_at_ms=observed_at_ms,
        detail_code=detail_code,
        result_sha256=result_sha256,
        artifact_manifest_sha256=artifact_manifest_sha256,
        directory_durability_contract=directory_durability_contract,
    )
    digest = hashlib.sha256(_RECORD_HASH_DOMAIN + canonical_json_line(body)).hexdigest()
    return D1AttemptWalRecordV0(
        state=state,
        sequence=sequence,
        previous_record_sha256=previous_record_sha256,
        record_sha256=digest,
        bindings=bindings,
        bindings_sha256=bindings.bindings_sha256,
        attempt_directory_sha256=attempt_directory_sha256,
        observed_at_ms=observed_at_ms,
        detail_code=detail_code,
        result_sha256=result_sha256,
        artifact_manifest_sha256=artifact_manifest_sha256,
        directory_durability_contract=directory_durability_contract,
    )


def _encode_frame(record: D1AttemptWalRecordV0) -> bytes:
    payload = canonical_json_line(_record_document(record))
    if payload.count(b"\n") != 1 or not payload.endswith(b"\n"):
        raise D1HistoricalAttemptWalIntegrityErrorV0(
            "attempt record must be exactly one canonical JSONL line"
        )
    if len(payload) > D1_ATTEMPT_WAL_MAX_RECORD_BYTES_V0:
        raise D1HistoricalAttemptWalIntegrityErrorV0("attempt record exceeds its fixed cap")
    return _FRAME_HEADER.pack(_FRAME_MAGIC, len(payload)) + payload


def _parse_wal_bytes(attempt_dir: Path, raw: bytes) -> D1AttemptWalSnapshotV0:
    if not raw or len(raw) > D1_ATTEMPT_WAL_MAX_BYTES_V0:
        raise D1HistoricalAttemptWalIntegrityErrorV0("attempt WAL size is outside its cap")
    records: list[D1AttemptWalRecordV0] = []
    offset = 0
    torn: D1AttemptWalTornTailV0 | None = None
    while offset < len(raw):
        frame_start = offset
        remaining = len(raw) - offset
        if remaining < _FRAME_HEADER.size:
            torn = _torn_tail("TORN_FRAME_HEADER", frame_start, raw[frame_start:])
            break
        magic, payload_length = _FRAME_HEADER.unpack_from(raw, offset)
        if magic != _FRAME_MAGIC:
            raise D1HistoricalAttemptWalIntegrityErrorV0(
                "complete attempt WAL frame has invalid magic"
            )
        if payload_length < 1 or payload_length > D1_ATTEMPT_WAL_MAX_RECORD_BYTES_V0:
            raise D1HistoricalAttemptWalIntegrityErrorV0(
                "attempt WAL frame length is outside its fixed cap"
            )
        payload_start = offset + _FRAME_HEADER.size
        payload_end = payload_start + payload_length
        if payload_end > len(raw):
            torn = _torn_tail("TORN_FRAME_PAYLOAD", frame_start, raw[frame_start:])
            break
        payload = raw[payload_start:payload_end]
        record = _parse_record(payload)
        _validate_transition(tuple(records), record)
        records.append(record)
        if len(records) > D1_ATTEMPT_WAL_MAX_RECORDS_V0:
            raise D1HistoricalAttemptWalIntegrityErrorV0(
                "attempt WAL exceeds its fixed record count"
            )
        offset = payload_end

    if not records:
        raise D1HistoricalAttemptWalIntegrityErrorV0(
            "attempt WAL has no complete valid ARMED prefix"
        )
    complete_raw = raw[:offset]
    prefix = D1AttemptWalPrefixV0(
        record_count=len(records),
        complete_bytes=len(complete_raw),
        last_record_sha256=records[-1].record_sha256,
        prefix_sha256=hashlib.sha256(_PREFIX_HASH_DOMAIN + complete_raw).hexdigest(),
    )
    return D1AttemptWalSnapshotV0(
        attempt_dir=attempt_dir,
        wal_path=attempt_dir / D1_ATTEMPT_WAL_FILE_V0,
        records=tuple(records),
        prefix=prefix,
        total_file_bytes=len(raw),
        torn_tail=torn,
    )


def _parse_record(payload: bytes) -> D1AttemptWalRecordV0:
    if payload.count(b"\n") != 1 or not payload.endswith(b"\n"):
        raise D1HistoricalAttemptWalIntegrityErrorV0(
            "complete WAL frame is not exactly one newline-terminated JSONL record"
        )
    try:
        value = json.loads(
            payload.decode("utf-8"),
            object_pairs_hook=_unique_object,
            parse_float=_reject_json_number,
            parse_constant=_reject_json_number,
        )
    except (UnicodeDecodeError, json.JSONDecodeError, ValueError, TypeError) as error:
        raise D1HistoricalAttemptWalIntegrityErrorV0(
            "complete WAL frame contains invalid canonical JSON"
        ) from error
    if type(value) is not dict:
        raise D1HistoricalAttemptWalIntegrityErrorV0("attempt record must be a JSON object")
    document = cast(dict[str, object], value)
    if frozenset(document) != _RECORD_KEYS:
        raise D1HistoricalAttemptWalIntegrityErrorV0("attempt record keys are not exact")
    if canonical_json_line(document) != payload:
        raise D1HistoricalAttemptWalIntegrityErrorV0("attempt record is not canonical JSONL")

    bindings = _parse_bindings(document.get("bindings"))
    state_value = _text(document, "state")
    if state_value not in {
        "ARMED",
        "STARTED_BEFORE_OUTCOME_ACCESS",
        "COMPLETED",
        "FAILED",
        "AMBIGUOUS_OUTPUT",
    }:
        raise D1HistoricalAttemptWalIntegrityErrorV0("attempt record state is unsupported")
    contract_value = _text(document, "directory_durability_contract")
    if contract_value not in {
        D1_ATTEMPT_WAL_POSIX_DURABILITY_CONTRACT_V0,
        D1_ATTEMPT_WAL_WINDOWS_DURABILITY_CONTRACT_V0,
    }:
        raise D1HistoricalAttemptWalIntegrityErrorV0(
            "attempt record durability contract is unsupported"
        )
    previous = _optional_text(document, "previous_record_sha256")
    detail = _optional_text(document, "detail_code")
    result = _optional_text(document, "result_sha256")
    manifest = _optional_text(document, "artifact_manifest_sha256")
    record = D1AttemptWalRecordV0(
        state=cast(D1AttemptWalStateV0, state_value),
        sequence=_integer(document, "sequence"),
        previous_record_sha256=previous,
        record_sha256=_text(document, "record_sha256"),
        bindings=bindings,
        bindings_sha256=_text(document, "bindings_sha256"),
        attempt_directory_sha256=_text(document, "attempt_directory_sha256"),
        observed_at_ms=_integer(document, "observed_at_ms"),
        detail_code=detail,
        result_sha256=result,
        artifact_manifest_sha256=manifest,
        directory_durability_contract=cast(D1AttemptWalDurabilityContractV0, contract_value),
        production_order_placement=cast(Literal[False], document.get("production_order_placement")),
        schema_version=cast(
            Literal["d1_historical_attempt_wal_record_v0"],
            _text(document, "schema_version"),
        ),
        frame_version=cast(
            Literal["fixed_u32be_length_rfc8785_jsonl_v0"],
            _text(document, "frame_version"),
        ),
    )
    body = dict(document)
    del body["record_sha256"]
    expected = hashlib.sha256(_RECORD_HASH_DOMAIN + canonical_json_line(body)).hexdigest()
    if record.record_sha256 != expected:
        raise D1HistoricalAttemptWalIntegrityErrorV0("attempt record self-hash differs")
    return record


def _parse_bindings(value: object) -> D1AttemptWalBindingsV0:
    if type(value) is not dict:
        raise D1HistoricalAttemptWalIntegrityErrorV0("attempt bindings must be an object")
    document = cast(dict[str, object], value)
    if frozenset(document) != _BINDING_KEYS:
        raise D1HistoricalAttemptWalIntegrityErrorV0("attempt binding keys are not exact")
    return D1AttemptWalBindingsV0(
        run_id=_text(document, "run_id"),
        code_freeze_manifest_sha256=_text(document, "code_freeze_manifest_sha256"),
        input_authority_sha256=_text(document, "input_authority_sha256"),
        input_authority_file_sha256=_text(document, "input_authority_file_sha256"),
        funding_authority_file_sha256=_text(document, "funding_authority_file_sha256"),
        preregistration_sha256=_text(document, "preregistration_sha256"),
        output_path_sha256=_text(document, "output_path_sha256"),
    )


def _validate_transition(
    prefix: tuple[D1AttemptWalRecordV0, ...],
    candidate: D1AttemptWalRecordV0,
) -> None:
    if not prefix:
        if candidate.sequence != 0 or candidate.state != "ARMED":
            raise D1HistoricalAttemptWalIntegrityErrorV0(
                "attempt WAL must begin with sequence-zero ARMED"
            )
        if candidate.previous_record_sha256 is not None:
            raise D1HistoricalAttemptWalIntegrityErrorV0(
                "ARMED cannot name a previous record"
            )
        return

    previous = prefix[-1]
    if candidate.sequence != len(prefix):
        raise D1HistoricalAttemptWalIntegrityErrorV0("attempt sequence is not contiguous")
    if candidate.previous_record_sha256 != previous.record_sha256:
        raise D1HistoricalAttemptWalIntegrityErrorV0("attempt previous-record hash differs")
    if candidate.bindings != previous.bindings:
        raise D1HistoricalAttemptWalIntegrityErrorV0("attempt bindings changed mid-WAL")
    if candidate.bindings_sha256 != previous.bindings_sha256:
        raise D1HistoricalAttemptWalIntegrityErrorV0("attempt binding hash changed mid-WAL")
    if candidate.attempt_directory_sha256 != previous.attempt_directory_sha256:
        raise D1HistoricalAttemptWalIntegrityErrorV0("attempt directory changed mid-WAL")
    if candidate.directory_durability_contract != previous.directory_durability_contract:
        raise D1HistoricalAttemptWalIntegrityErrorV0(
            "attempt durability contract changed mid-WAL"
        )
    if candidate.observed_at_ms < previous.observed_at_ms:
        raise D1HistoricalAttemptWalIntegrityErrorV0("attempt timestamps moved backwards")

    if not _state_transition_allowed(previous.state, candidate.state):
        raise D1HistoricalAttemptWalStateErrorV0(
            f"forbidden attempt WAL transition {previous.state}->{candidate.state}"
        )
    if previous.state == "COMPLETED":
        if (
            candidate.result_sha256 != previous.result_sha256
            or candidate.artifact_manifest_sha256 != previous.artifact_manifest_sha256
        ):
            raise D1HistoricalAttemptWalStateErrorV0(
                "post-COMPLETED ambiguity must preserve the published artifact hashes"
            )


def _state_transition_allowed(
    previous: D1AttemptWalStateV0,
    candidate: D1AttemptWalStateV0,
) -> bool:
    return (
        previous == "ARMED" and candidate == "STARTED_BEFORE_OUTCOME_ACCESS"
    ) or (
        previous == "STARTED_BEFORE_OUTCOME_ACCESS"
        and candidate in {"COMPLETED", "FAILED", "AMBIGUOUS_OUTPUT"}
    ) or (previous == "COMPLETED" and candidate == "AMBIGUOUS_OUTPUT")


def _validate_record_fields(record: D1AttemptWalRecordV0) -> None:
    if record.schema_version != _SCHEMA_VERSION or record.frame_version != _FRAME_VERSION:
        raise D1HistoricalAttemptWalIntegrityErrorV0("attempt record schema is unsupported")
    if type(record.sequence) is not int or not 0 <= record.sequence < D1_ATTEMPT_WAL_MAX_RECORDS_V0:
        raise D1HistoricalAttemptWalIntegrityErrorV0("attempt record sequence is invalid")
    if record.previous_record_sha256 is not None:
        _require_sha256(record.previous_record_sha256, "previous_record_sha256")
    _require_sha256(record.record_sha256, "record_sha256")
    if type(record.bindings) is not D1AttemptWalBindingsV0:
        raise D1HistoricalAttemptWalIntegrityErrorV0("attempt record bindings are not exact")
    _require_sha256(record.bindings_sha256, "bindings_sha256")
    if record.bindings_sha256 != record.bindings.bindings_sha256:
        raise D1HistoricalAttemptWalIntegrityErrorV0("attempt binding self-hash differs")
    _require_sha256(record.attempt_directory_sha256, "attempt_directory_sha256")
    _require_timestamp_ms(record.observed_at_ms, "observed_at_ms")
    if record.production_order_placement is not False:
        raise D1HistoricalAttemptWalIntegrityErrorV0(
            "historical attempt WAL cannot claim production order placement"
        )
    if record.directory_durability_contract not in {
        D1_ATTEMPT_WAL_POSIX_DURABILITY_CONTRACT_V0,
        D1_ATTEMPT_WAL_WINDOWS_DURABILITY_CONTRACT_V0,
    }:
        raise D1HistoricalAttemptWalIntegrityErrorV0(
            "attempt directory durability contract is unsupported"
        )

    if record.state in {"ARMED", "STARTED_BEFORE_OUTCOME_ACCESS"}:
        if (
            record.detail_code is not None
            or record.result_sha256 is not None
            or record.artifact_manifest_sha256 is not None
        ):
            raise D1HistoricalAttemptWalIntegrityErrorV0(
                "nonterminal attempt record contains terminal fields"
            )
    elif record.state == "COMPLETED":
        if record.detail_code is not None:
            raise D1HistoricalAttemptWalIntegrityErrorV0(
                "COMPLETED cannot contain a detail code"
            )
        _require_sha256(record.result_sha256, "result_sha256")
        _require_sha256(record.artifact_manifest_sha256, "artifact_manifest_sha256")
    elif record.state in {"FAILED", "AMBIGUOUS_OUTPUT"}:
        _require_detail_code(record.detail_code)
        if (record.result_sha256 is None) != (record.artifact_manifest_sha256 is None):
            raise D1HistoricalAttemptWalIntegrityErrorV0(
                "ambiguous artifact hashes must be both present or both absent"
            )
        if record.state == "FAILED" and record.result_sha256 is not None:
            raise D1HistoricalAttemptWalIntegrityErrorV0(
                "FAILED cannot claim published artifact hashes"
            )
        if record.result_sha256 is not None:
            _require_sha256(record.result_sha256, "result_sha256")
            _require_sha256(record.artifact_manifest_sha256, "artifact_manifest_sha256")
    else:
        raise D1HistoricalAttemptWalIntegrityErrorV0("attempt record state is unsupported")


def _record_document(record: D1AttemptWalRecordV0) -> dict[str, object]:
    return {
        **_record_body_document(
            state=record.state,
            sequence=record.sequence,
            previous_record_sha256=record.previous_record_sha256,
            bindings=record.bindings,
            bindings_sha256=record.bindings_sha256,
            attempt_directory_sha256=record.attempt_directory_sha256,
            observed_at_ms=record.observed_at_ms,
            detail_code=record.detail_code,
            result_sha256=record.result_sha256,
            artifact_manifest_sha256=record.artifact_manifest_sha256,
            directory_durability_contract=record.directory_durability_contract,
        ),
        "record_sha256": record.record_sha256,
    }


def _record_body_document(
    *,
    state: D1AttemptWalStateV0,
    sequence: int,
    previous_record_sha256: str | None,
    bindings: D1AttemptWalBindingsV0,
    bindings_sha256: str,
    attempt_directory_sha256: str,
    observed_at_ms: int,
    detail_code: str | None,
    result_sha256: str | None,
    artifact_manifest_sha256: str | None,
    directory_durability_contract: D1AttemptWalDurabilityContractV0,
) -> dict[str, object]:
    return {
        "artifact_manifest_sha256": artifact_manifest_sha256,
        "attempt_directory_sha256": attempt_directory_sha256,
        "bindings": asdict(bindings),
        "bindings_sha256": bindings_sha256,
        "detail_code": detail_code,
        "directory_durability_contract": directory_durability_contract,
        "frame_version": _FRAME_VERSION,
        "observed_at_ms": observed_at_ms,
        "previous_record_sha256": previous_record_sha256,
        "production_order_placement": False,
        "result_sha256": result_sha256,
        "schema_version": _SCHEMA_VERSION,
        "sequence": sequence,
        "state": state,
    }


@contextmanager
def _open_locked_wal(attempt_dir: Path) -> Iterator[_LockedWalV0]:
    directory_identity = _require_attempt_directory(attempt_dir, require_wal=True)
    wal_path = attempt_dir / D1_ATTEMPT_WAL_FILE_V0
    descriptor: int | None = None
    locked = False
    try:
        descriptor = os.open(wal_path, _existing_wal_open_flags())
        opened = _metadata_from_fd(descriptor)
        _require_regular_single_link(opened, "attempt WAL")
        _require_path_matches_descriptor(wal_path, opened)
        _acquire_exclusive_lock(descriptor)
        locked = True
        _require_path_matches_descriptor(wal_path, opened)
        _require_same_directory_identity(
            directory_identity,
            _require_attempt_directory(attempt_dir, require_wal=True),
        )
        yield _LockedWalV0(
            descriptor=descriptor,
            attempt_dir=attempt_dir,
            wal_path=wal_path,
            directory_identity=directory_identity,
            file_identity=opened,
        )
    except FileNotFoundError as error:
        raise D1HistoricalAttemptWalIntegrityErrorV0("attempt WAL is unavailable") from error
    except OSError as error:
        raise D1HistoricalAttemptWalIntegrityErrorV0("cannot open the attempt WAL") from error
    finally:
        if descriptor is not None and locked:
            try:
                _release_exclusive_lock(descriptor)
            except OSError as error:
                raise D1HistoricalAttemptWalDurabilityErrorV0(
                    "attempt WAL lock release failed; no retry is allowed"
                ) from error
        if descriptor is not None:
            try:
                os.close(descriptor)
            except OSError as error:
                raise D1HistoricalAttemptWalDurabilityErrorV0(
                    "attempt WAL close failed; no retry is allowed"
                ) from error


def _read_locked_wal(locked: _LockedWalV0) -> bytes:
    before = _metadata_from_fd(locked.descriptor)
    _require_same_file_identity(locked.file_identity, before, "attempt WAL read")
    _require_regular_single_link(before, "attempt WAL")
    if before.size < 1 or before.size > D1_ATTEMPT_WAL_MAX_BYTES_V0:
        raise D1HistoricalAttemptWalIntegrityErrorV0("attempt WAL size is outside its cap")
    os.lseek(locked.descriptor, 0, os.SEEK_SET)
    chunks: list[bytes] = []
    remaining = before.size
    while remaining:
        chunk = os.read(locked.descriptor, min(remaining, 64 * 1024))
        if not chunk:
            raise D1HistoricalAttemptWalIntegrityErrorV0("attempt WAL ended during stable read")
        chunks.append(chunk)
        remaining -= len(chunk)
    raw = b"".join(chunks)
    after = _metadata_from_fd(locked.descriptor)
    if after != before:
        raise D1HistoricalAttemptWalIntegrityErrorV0("attempt WAL changed during stable read")
    _require_path_matches_descriptor(locked.wal_path, after)
    _require_same_directory_identity(
        locked.directory_identity,
        _require_attempt_directory(locked.attempt_dir, require_wal=True),
    )
    return raw


def _creation_target(value: str | Path) -> Path:
    target = _absolute_path(value, "attempt_dir")
    _require_real_directory_tree(target.parent)
    if _lstat_or_none(target) is not None:
        raise D1HistoricalAttemptWalStateErrorV0(
            "attempt directory already exists; no retry or replacement is allowed"
        )
    return target


def _existing_attempt_target(value: str | Path) -> Path:
    target = _absolute_path(value, "attempt_dir")
    _require_real_directory_tree(target)
    _require_attempt_directory(target, require_wal=True)
    return target


def _absolute_path(value: str | Path, label: str) -> Path:
    candidate = Path(value)
    if not candidate.is_absolute() or any(part in {".", ".."} for part in candidate.parts):
        raise D1HistoricalAttemptWalIntegrityErrorV0(f"{label} must be normalized absolute")
    return Path(os.path.abspath(candidate))


def _require_real_directory_tree(path: Path) -> None:
    if not path.is_absolute():
        raise D1HistoricalAttemptWalIntegrityErrorV0("directory path must be absolute")
    current = Path(path.anchor)
    for part in path.parts[1:]:
        current /= part
        metadata = _lstat_required(current, "directory component")
        if _is_link_or_reparse(metadata) or not stat.S_ISDIR(metadata.st_mode):
            raise D1HistoricalAttemptWalIntegrityErrorV0(
                "attempt path contains a symlink, reparse point, or non-directory"
            )


def _require_attempt_directory(path: Path, *, require_wal: bool) -> _DirectoryIdentityV0:
    metadata = _lstat_required(path, "attempt directory")
    if _is_link_or_reparse(metadata) or not stat.S_ISDIR(metadata.st_mode):
        raise D1HistoricalAttemptWalIntegrityErrorV0(
            "attempt directory must be a real non-symlink directory"
        )
    names: set[str] = set()
    try:
        with os.scandir(path) as entries:
            for entry in entries:
                names.add(entry.name)
                if len(names) > D1_ATTEMPT_DIRECTORY_MAX_ENTRIES_V0:
                    raise D1HistoricalAttemptWalIntegrityErrorV0(
                        "attempt directory exceeds its bounded membership cap"
                    )
    except OSError as error:
        raise D1HistoricalAttemptWalIntegrityErrorV0(
            "cannot inspect attempt directory membership"
        ) from error
    allowed = (
        {
            frozenset({D1_ATTEMPT_WAL_FILE_V0}),
            frozenset({D1_ATTEMPT_WAL_FILE_V0, D1_ATTEMPT_START_SEAL_FILE_V0}),
        }
        if require_wal
        else {frozenset()}
    )
    if frozenset(names) not in allowed:
        raise D1HistoricalAttemptWalIntegrityErrorV0(
            "attempt directory membership differs from the fixed WAL contract"
        )
    return _DirectoryIdentityV0(
        device=metadata.st_dev,
        inode=metadata.st_ino,
        os_family=_host_os_family(),
        volume_identity=_volume_identity(path, metadata),
    )


def _require_same_directory_identity(
    expected: _DirectoryIdentityV0,
    observed: _DirectoryIdentityV0,
) -> None:
    if observed != expected:
        raise D1HistoricalAttemptWalIntegrityErrorV0("attempt directory identity changed")


def _metadata_from_fd(descriptor: int) -> _FileMetadataV0:
    try:
        return _file_metadata(os.fstat(descriptor))
    except OSError as error:
        raise D1HistoricalAttemptWalIntegrityErrorV0("cannot inspect open WAL identity") from error


def _metadata_from_path(path: Path) -> _FileMetadataV0:
    return _file_metadata(_lstat_required(path, "attempt WAL path"))


def _file_metadata(value: os.stat_result) -> _FileMetadataV0:
    return _FileMetadataV0(
        device=value.st_dev,
        inode=value.st_ino,
        mode=value.st_mode,
        links=value.st_nlink,
        size=value.st_size,
        modified_ns=value.st_mtime_ns,
        changed_ns=value.st_ctime_ns,
    )


def _require_regular_single_link(metadata: _FileMetadataV0, label: str) -> None:
    if not stat.S_ISREG(metadata.mode) or metadata.links != 1:
        raise D1HistoricalAttemptWalIntegrityErrorV0(
            f"{label} must be one regular file with exactly one link"
        )


def _require_path_matches_descriptor(path: Path, descriptor: _FileMetadataV0) -> None:
    observed_stat = _lstat_required(path, "attempt WAL path")
    if _is_link_or_reparse(observed_stat):
        raise D1HistoricalAttemptWalIntegrityErrorV0(
            "attempt WAL path became a symlink or reparse point"
        )
    observed = _file_metadata(observed_stat)
    _require_regular_single_link(observed, "attempt WAL path")
    _require_same_file_identity(descriptor, observed, "attempt WAL path")
    if observed.size != descriptor.size:
        raise D1HistoricalAttemptWalIntegrityErrorV0(
            "attempt WAL path size differs from its open descriptor"
        )


def _require_same_file_identity(
    expected: _FileMetadataV0,
    observed: _FileMetadataV0,
    label: str,
) -> None:
    if (observed.device, observed.inode) != (expected.device, expected.inode):
        raise D1HistoricalAttemptWalIntegrityErrorV0(f"{label} inode changed")


def _lstat_required(path: Path, label: str) -> os.stat_result:
    try:
        return os.lstat(path)
    except OSError as error:
        raise D1HistoricalAttemptWalIntegrityErrorV0(f"{label} is unavailable") from error


def _lstat_or_none(path: Path) -> os.stat_result | None:
    try:
        return os.lstat(path)
    except FileNotFoundError:
        return None
    except OSError as error:
        raise D1HistoricalAttemptWalIntegrityErrorV0("cannot inspect attempt target") from error


def _is_link_or_reparse(metadata: os.stat_result) -> bool:
    if stat.S_ISLNK(metadata.st_mode):
        return True
    attributes = getattr(metadata, "st_file_attributes", 0)
    reparse_flag = getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0)
    return bool(attributes & reparse_flag)


def _new_wal_open_flags() -> int:
    return (
        os.O_WRONLY
        | os.O_CREAT
        | os.O_EXCL
        | os.O_APPEND
        | getattr(os, "O_BINARY", 0)
        | getattr(os, "O_CLOEXEC", 0)
        | getattr(os, "O_NOFOLLOW", 0)
    )


def _existing_wal_open_flags() -> int:
    return (
        os.O_RDWR
        | os.O_APPEND
        | getattr(os, "O_BINARY", 0)
        | getattr(os, "O_CLOEXEC", 0)
        | getattr(os, "O_NOFOLLOW", 0)
    )


def _write_all(descriptor: int, payload: bytes) -> None:
    offset = 0
    while offset < len(payload):
        written = _write_once(descriptor, payload[offset:])
        if type(written) is not int or written <= 0 or written > len(payload) - offset:
            raise D1HistoricalAttemptWalDurabilityErrorV0(
                "attempt WAL write made invalid progress; existing bytes require audit"
            )
        offset += written


def _write_once(descriptor: int, payload: bytes) -> int:
    return os.write(descriptor, payload)


def _sync_wal_fd(descriptor: int) -> None:
    os.fsync(descriptor)


def _sync_start_seal_fd(descriptor: int) -> None:
    os.fsync(descriptor)


def _sync_attempt_directory_and_parent(attempt_dir: Path) -> None:
    _sync_directory_entry(attempt_dir)
    _sync_directory_entry(attempt_dir.parent)


def _sync_directory_entry(path: Path) -> None:
    if os.name == "nt":
        metadata = _lstat_required(path, "Windows directory flush target")
        if _is_link_or_reparse(metadata) or not stat.S_ISDIR(metadata.st_mode):
            raise D1HistoricalAttemptWalIntegrityErrorV0(
                "Windows directory flush target must be a real directory"
            )
        _windows_local_volume_identity(path)
        handle: int | None = None
        try:
            handle = _windows_open_directory_handle(path)
            _windows_flush_directory_handle(handle)
        finally:
            if handle is not None:
                _windows_close_handle(handle)
        return
    flags = (
        os.O_RDONLY
        | getattr(os, "O_DIRECTORY", 0)
        | getattr(os, "O_CLOEXEC", 0)
        | getattr(os, "O_NOFOLLOW", 0)
    )
    descriptor: int | None = None
    try:
        descriptor = os.open(path, flags)
        metadata = os.fstat(descriptor)
        if not stat.S_ISDIR(metadata.st_mode):
            raise D1HistoricalAttemptWalIntegrityErrorV0(
                "directory fsync target is not a directory"
            )
        os.fsync(descriptor)
    except OSError as error:
        raise D1HistoricalAttemptWalDurabilityErrorV0(
            "directory fsync is required by the POSIX durability contract"
        ) from error
    finally:
        if descriptor is not None:
            os.close(descriptor)


def _windows_kernel32():
    loader = getattr(ctypes, "WinDLL", None)
    if loader is None:
        raise D1HistoricalAttemptWalDurabilityErrorV0(
            "Win32 directory durability APIs are unavailable"
        )
    return loader("kernel32", use_last_error=True)


def _windows_last_error() -> int:
    getter = getattr(ctypes, "get_last_error", None)
    return 0 if getter is None else int(getter())


def _windows_open_directory_handle(path: Path) -> int:
    from ctypes import wintypes

    kernel32 = _windows_kernel32()
    create_file = kernel32.CreateFileW
    create_file.argtypes = (
        wintypes.LPCWSTR,
        wintypes.DWORD,
        wintypes.DWORD,
        wintypes.LPVOID,
        wintypes.DWORD,
        wintypes.DWORD,
        wintypes.HANDLE,
    )
    create_file.restype = wintypes.HANDLE
    generic_write = 0x40000000
    share_read_write_delete = 0x00000001 | 0x00000002 | 0x00000004
    open_existing = 3
    backup_semantics = 0x02000000
    open_reparse_point = 0x00200000
    write_through = 0x80000000
    handle = create_file(
        os.fspath(path),
        generic_write,
        share_read_write_delete,
        None,
        open_existing,
        backup_semantics | open_reparse_point | write_through,
        None,
    )
    value = handle if isinstance(handle, int) else getattr(handle, "value", None)
    invalid = ctypes.c_void_p(-1).value
    if value is None or value == invalid:
        error_number = _windows_last_error()
        raise D1HistoricalAttemptWalDurabilityErrorV0(
            f"CreateFileW directory durability handle failed with error {error_number}"
        )
    return int(value)


def _windows_flush_directory_handle(handle: int) -> None:
    from ctypes import wintypes

    kernel32 = _windows_kernel32()
    flush = kernel32.FlushFileBuffers
    flush.argtypes = (wintypes.HANDLE,)
    flush.restype = wintypes.BOOL
    if not flush(wintypes.HANDLE(handle)):
        error_number = _windows_last_error()
        raise D1HistoricalAttemptWalDurabilityErrorV0(
            f"FlushFileBuffers directory durability failed with error {error_number}"
        )


def _windows_close_handle(handle: int) -> None:
    from ctypes import wintypes

    kernel32 = _windows_kernel32()
    close = kernel32.CloseHandle
    close.argtypes = (wintypes.HANDLE,)
    close.restype = wintypes.BOOL
    if not close(wintypes.HANDLE(handle)):
        error_number = _windows_last_error()
        raise D1HistoricalAttemptWalDurabilityErrorV0(
            f"CloseHandle directory durability failed with error {error_number}"
        )


def _windows_local_volume_identity(path: Path) -> str:
    from ctypes import wintypes

    kernel32 = _windows_kernel32()
    volume_path = ctypes.create_unicode_buffer(261)
    get_volume_path = kernel32.GetVolumePathNameW
    get_volume_path.argtypes = (wintypes.LPCWSTR, wintypes.LPWSTR, wintypes.DWORD)
    get_volume_path.restype = wintypes.BOOL
    if not get_volume_path(os.fspath(path), volume_path, len(volume_path)):
        error_number = _windows_last_error()
        raise D1HistoricalAttemptWalDurabilityErrorV0(
            f"GetVolumePathNameW failed with error {error_number}"
        )
    root = volume_path.value
    get_drive_type = kernel32.GetDriveTypeW
    get_drive_type.argtypes = (wintypes.LPCWSTR,)
    get_drive_type.restype = wintypes.UINT
    if int(get_drive_type(root)) != 3:
        raise D1HistoricalAttemptWalDurabilityErrorV0(
            "attempt WAL requires a local fixed Windows volume"
        )

    serial = wintypes.DWORD()
    filesystem = ctypes.create_unicode_buffer(64)
    get_volume_information = kernel32.GetVolumeInformationW
    get_volume_information.argtypes = (
        wintypes.LPCWSTR,
        wintypes.LPWSTR,
        wintypes.DWORD,
        ctypes.POINTER(wintypes.DWORD),
        ctypes.POINTER(wintypes.DWORD),
        ctypes.POINTER(wintypes.DWORD),
        wintypes.LPWSTR,
        wintypes.DWORD,
    )
    get_volume_information.restype = wintypes.BOOL
    if not get_volume_information(
        root,
        None,
        0,
        ctypes.byref(serial),
        None,
        None,
        filesystem,
        len(filesystem),
    ):
        error_number = _windows_last_error()
        raise D1HistoricalAttemptWalDurabilityErrorV0(
            f"GetVolumeInformationW failed with error {error_number}"
        )
    filesystem_name = filesystem.value.upper()
    if filesystem_name != "NTFS":
        raise D1HistoricalAttemptWalDurabilityErrorV0(
            "attempt WAL requires a local fixed NTFS volume"
        )
    return f"{os.path.normcase(root)}|{filesystem_name}|{serial.value:08x}"


def _acquire_exclusive_lock(descriptor: int) -> None:
    if os.name == "nt":
        import msvcrt

        os.lseek(descriptor, 0, os.SEEK_SET)
        try:
            msvcrt.locking(descriptor, msvcrt.LK_NBLCK, 1)
        except OSError as error:
            raise D1HistoricalAttemptWalConcurrentWriteErrorV0(
                "attempt WAL append lock is already held"
            ) from error
        return
    import fcntl

    try:
        fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError as error:
        raise D1HistoricalAttemptWalConcurrentWriteErrorV0(
            "attempt WAL append lock is already held"
        ) from error


def _release_exclusive_lock(descriptor: int) -> None:
    if os.name == "nt":
        import msvcrt

        os.lseek(descriptor, 0, os.SEEK_SET)
        msvcrt.locking(descriptor, msvcrt.LK_UNLCK, 1)
        return
    import fcntl

    fcntl.flock(descriptor, fcntl.LOCK_UN)


def _host_os_family() -> str:
    if os.name == "nt":
        return "WINDOWS_NT"
    return f"POSIX_{sys.platform.upper()}"


def _volume_identity(path: Path, metadata: os.stat_result) -> str:
    if os.name == "nt":
        return _windows_local_volume_identity(path)
    try:
        anchor_device = os.stat(path.anchor).st_dev
    except OSError as error:
        raise D1HistoricalAttemptWalIntegrityErrorV0(
            "cannot identify the attempt filesystem volume"
        ) from error
    return f"{sys.platform}|device={metadata.st_dev}|anchor_device={anchor_device}"


def _durability_contract_for_directory(path: Path) -> D1AttemptWalDurabilityContractV0:
    contract = current_d1_attempt_wal_os_durability_label_v0()
    if os.name == "nt":
        _windows_local_volume_identity(path)
    return contract


def _attempt_directory_sha256(
    path: Path,
    identity: _DirectoryIdentityV0,
) -> str:
    document = {
        "absolute_normalized_path": os.path.normcase(os.path.abspath(os.fspath(path))),
        "device_decimal": str(identity.device),
        "inode_decimal": str(identity.inode),
        "os_family": identity.os_family,
        "threat_model": D1_ATTEMPT_WAL_THREAT_MODEL_V0,
        "volume_identity": identity.volume_identity,
    }
    return hashlib.sha256(
        _GENERATION_HASH_DOMAIN + canonical_json_line(document)
    ).hexdigest()


def _require_snapshot_generation(
    snapshot: D1AttemptWalSnapshotV0,
    identity: _DirectoryIdentityV0,
) -> None:
    expected = _attempt_directory_sha256(snapshot.attempt_dir, identity)
    if any(record.attempt_directory_sha256 != expected for record in snapshot.records):
        raise D1HistoricalAttemptWalIntegrityErrorV0(
            "attempt WAL belongs to another directory generation"
        )


def _start_prefix(snapshot: D1AttemptWalSnapshotV0) -> D1AttemptWalPrefixV0:
    if len(snapshot.records) < 2:
        raise D1HistoricalAttemptWalIntegrityErrorV0(
            "START seal requires a complete START record"
        )
    raw = b"".join(_encode_frame(record) for record in snapshot.records[:2])
    start_record = snapshot.records[1]
    return D1AttemptWalPrefixV0(
        record_count=2,
        complete_bytes=len(raw),
        last_record_sha256=start_record.record_sha256,
        prefix_sha256=hashlib.sha256(_PREFIX_HASH_DOMAIN + raw).hexdigest(),
    )


def _grant_matches_start_snapshot(
    grant: D1OutcomeAccessGrantV0,
    snapshot: D1AttemptWalSnapshotV0,
) -> bool:
    if (
        snapshot.torn_tail is not None
        or len(snapshot.records) != 2
        or snapshot.last_state != "STARTED_BEFORE_OUTCOME_ACCESS"
        or not snapshot.start_seal_valid
        or snapshot.start_seal is None
    ):
        return False
    start_record = snapshot.records[1]
    return (
        grant._start_record_sha256 == start_record.record_sha256
        and grant._start_prefix == snapshot.prefix
        and grant._bindings == start_record.bindings
        and grant._attempt_directory_sha256
        == start_record.attempt_directory_sha256
        and grant._start_seal_sha256 == snapshot.start_seal.seal_sha256
        and type(grant._mint_process_id) is int
        and grant._mint_process_id > 0
    )


def _mint_outcome_access_grant(
    snapshot: D1AttemptWalSnapshotV0,
) -> D1OutcomeAccessGrantV0:
    if (
        snapshot.torn_tail is not None
        or len(snapshot.records) != 2
        or snapshot.last_state != "STARTED_BEFORE_OUTCOME_ACCESS"
        or not snapshot.start_seal_valid
        or snapshot.start_seal is None
    ):
        raise D1HistoricalAttemptWalDurabilityErrorV0(
            "ephemeral outcome access requires the exact durable START and seal"
        )
    start_record = snapshot.records[1]
    grant = object.__new__(D1OutcomeAccessGrantV0)
    object.__setattr__(grant, "_start_record_sha256", start_record.record_sha256)
    object.__setattr__(grant, "_start_prefix", snapshot.prefix)
    object.__setattr__(grant, "_bindings", start_record.bindings)
    object.__setattr__(
        grant,
        "_attempt_directory_sha256",
        start_record.attempt_directory_sha256,
    )
    object.__setattr__(grant, "_start_seal_sha256", snapshot.start_seal.seal_sha256)
    object.__setattr__(grant, "_consume_lock", threading.Lock())
    object.__setattr__(grant, "_consumed", False)
    object.__setattr__(grant, "_mint_process_id", os.getpid())
    if not _grant_matches_start_snapshot(grant, snapshot):
        raise D1HistoricalAttemptWalIntegrityErrorV0(
            "minted outcome access grant differs from its exact durable START"
        )
    return grant


def _mint_start_seal(snapshot: D1AttemptWalSnapshotV0) -> D1AttemptStartSealV0:
    start_record = snapshot.records[1]
    if start_record.state != "STARTED_BEFORE_OUTCOME_ACCESS":
        raise D1HistoricalAttemptWalIntegrityErrorV0(
            "sequence-one record is not the fixed START"
        )
    prefix = _start_prefix(snapshot)
    body = _start_seal_body(
        start_record_sha256=start_record.record_sha256,
        prefix_sha256=prefix.prefix_sha256,
        bindings_sha256=start_record.bindings_sha256,
        attempt_directory_sha256=start_record.attempt_directory_sha256,
        started_at_ms=start_record.observed_at_ms,
        directory_durability_contract=start_record.directory_durability_contract,
    )
    digest = hashlib.sha256(
        _START_SEAL_HASH_DOMAIN + canonical_json_line(body)
    ).hexdigest()
    return D1AttemptStartSealV0(
        start_record_sha256=start_record.record_sha256,
        prefix_sha256=prefix.prefix_sha256,
        bindings_sha256=start_record.bindings_sha256,
        attempt_directory_sha256=start_record.attempt_directory_sha256,
        started_at_ms=start_record.observed_at_ms,
        directory_durability_contract=start_record.directory_durability_contract,
        seal_sha256=digest,
    )


def _start_seal_body(
    *,
    start_record_sha256: str,
    prefix_sha256: str,
    bindings_sha256: str,
    attempt_directory_sha256: str,
    started_at_ms: int,
    directory_durability_contract: D1AttemptWalDurabilityContractV0,
) -> dict[str, object]:
    return {
        "attempt_directory_sha256": attempt_directory_sha256,
        "bindings_sha256": bindings_sha256,
        "directory_durability_contract": directory_durability_contract,
        "prefix_sha256": prefix_sha256,
        "production_order_placement": False,
        "schema_version": _START_SEAL_SCHEMA_VERSION,
        "start_record_sha256": start_record_sha256,
        "started_at_ms": started_at_ms,
        "threat_model": D1_ATTEMPT_WAL_THREAT_MODEL_V0,
    }


def _canonical_start_seal(seal: D1AttemptStartSealV0) -> bytes:
    return canonical_json_line(
        {
            **_start_seal_body(
                start_record_sha256=seal.start_record_sha256,
                prefix_sha256=seal.prefix_sha256,
                bindings_sha256=seal.bindings_sha256,
                attempt_directory_sha256=seal.attempt_directory_sha256,
                started_at_ms=seal.started_at_ms,
                directory_durability_contract=seal.directory_durability_contract,
            ),
            "seal_sha256": seal.seal_sha256,
        }
    )


def _parse_start_seal(raw: bytes) -> D1AttemptStartSealV0:
    try:
        value = json.loads(
            raw.decode("utf-8"),
            object_pairs_hook=_unique_object,
            parse_float=_reject_json_number,
            parse_constant=_reject_json_number,
        )
    except (UnicodeDecodeError, json.JSONDecodeError, ValueError, TypeError) as error:
        raise D1HistoricalAttemptWalIntegrityErrorV0(
            "START seal contains invalid canonical JSON"
        ) from error
    if type(value) is not dict:
        raise D1HistoricalAttemptWalIntegrityErrorV0("START seal must be a JSON object")
    document = cast(dict[str, object], value)
    if frozenset(document) != _START_SEAL_KEYS:
        raise D1HistoricalAttemptWalIntegrityErrorV0("START seal keys are not exact")
    if canonical_json_line(document) != raw:
        raise D1HistoricalAttemptWalIntegrityErrorV0("START seal is not canonical JSONL")
    contract = _text(document, "directory_durability_contract")
    if contract not in {
        D1_ATTEMPT_WAL_POSIX_DURABILITY_CONTRACT_V0,
        D1_ATTEMPT_WAL_WINDOWS_DURABILITY_CONTRACT_V0,
    }:
        raise D1HistoricalAttemptWalIntegrityErrorV0(
            "START seal durability contract is unsupported"
        )
    seal = D1AttemptStartSealV0(
        start_record_sha256=_text(document, "start_record_sha256"),
        prefix_sha256=_text(document, "prefix_sha256"),
        bindings_sha256=_text(document, "bindings_sha256"),
        attempt_directory_sha256=_text(document, "attempt_directory_sha256"),
        started_at_ms=_integer(document, "started_at_ms"),
        directory_durability_contract=cast(D1AttemptWalDurabilityContractV0, contract),
        seal_sha256=_text(document, "seal_sha256"),
        production_order_placement=cast(
            Literal[False], document.get("production_order_placement")
        ),
        threat_model=cast(
            Literal[
                "TRUSTED_LOCAL_FILESYSTEM_AND_PROCESS_CODE_PROCESS_AND_POWER_LOSS_PUBLIC_API_SEALED_ACTIVE_DURABLE_FILE_RESTORE_PRIVILEGED_FULL_SNAPSHOT_ROLLBACK_IN_PROCESS_REFLECTION_ADVERSARIAL_MUTATION_AND_DIRECT_RUNNER_INVOCATION_EXCLUDED_V0"
            ],
            _text(document, "threat_model"),
        ),
        schema_version=cast(
            Literal["d1_historical_attempt_start_seal_v0"],
            _text(document, "schema_version"),
        ),
    )
    body = dict(document)
    del body["seal_sha256"]
    expected = hashlib.sha256(
        _START_SEAL_HASH_DOMAIN + canonical_json_line(body)
    ).hexdigest()
    if seal.seal_sha256 != expected:
        raise D1HistoricalAttemptWalIntegrityErrorV0("START seal self-hash differs")
    return seal


def _validate_start_seal_binding(
    snapshot: D1AttemptWalSnapshotV0,
    seal: D1AttemptStartSealV0,
) -> None:
    expected = _mint_start_seal(snapshot)
    if seal != expected:
        raise D1HistoricalAttemptWalIntegrityErrorV0(
            "START seal differs from the exact START WAL prefix"
        )


def _attach_and_validate_start_seal(
    locked: _LockedWalV0,
    snapshot: D1AttemptWalSnapshotV0,
) -> D1AttemptWalSnapshotV0:
    seal_path = locked.attempt_dir / D1_ATTEMPT_START_SEAL_FILE_V0
    metadata = _lstat_or_none(seal_path)
    if snapshot.last_state == "ARMED":
        if metadata is not None:
            raise D1HistoricalAttemptWalIntegrityErrorV0(
                "ARMED WAL with START seal proves a forbidden rollback"
            )
        return replace(snapshot, start_seal=None, start_seal_torn=False)
    if metadata is None:
        return replace(snapshot, start_seal=None, start_seal_torn=False)
    if _is_link_or_reparse(metadata) or not stat.S_ISREG(metadata.st_mode):
        raise D1HistoricalAttemptWalIntegrityErrorV0(
            "START seal must be a regular non-symlink file"
        )
    if metadata.st_size == 0:
        return replace(snapshot, start_seal=None, start_seal_torn=True)
    raw = _read_exact_start_seal(seal_path)
    if not raw.endswith(b"\n"):
        return replace(snapshot, start_seal=None, start_seal_torn=True)
    seal = _parse_start_seal(raw)
    _validate_start_seal_binding(snapshot, seal)
    _require_same_directory_identity(
        locked.directory_identity,
        _require_attempt_directory(locked.attempt_dir, require_wal=True),
    )
    return replace(snapshot, start_seal=seal, start_seal_torn=False)


def _create_and_sync_start_seal(
    locked: _LockedWalV0,
    snapshot: D1AttemptWalSnapshotV0,
) -> D1AttemptWalSnapshotV0:
    if snapshot.last_state != "STARTED_BEFORE_OUTCOME_ACCESS":
        raise D1HistoricalAttemptWalStateErrorV0(
            "START seal can be created only for the exact START prefix"
        )
    seal = _mint_start_seal(snapshot)
    raw = _canonical_start_seal(seal)
    if len(raw) > D1_ATTEMPT_START_SEAL_MAX_BYTES_V0:
        raise D1HistoricalAttemptWalIntegrityErrorV0("START seal exceeds its byte cap")
    path = locked.attempt_dir / D1_ATTEMPT_START_SEAL_FILE_V0
    descriptor: int | None = None
    opened: _FileMetadataV0 | None = None
    try:
        descriptor = os.open(path, _new_start_seal_open_flags(), 0o600)
        opened = _metadata_from_fd(descriptor)
        _require_regular_single_link(opened, "new START seal")
        _write_start_seal_all(descriptor, raw)
        _sync_start_seal_fd(descriptor)
        os.lseek(descriptor, 0, os.SEEK_SET)
        observed = _read_exact_descriptor(descriptor, len(raw))
        if observed != raw:
            raise D1HistoricalAttemptWalDurabilityErrorV0(
                "post-fsync START seal readback differs"
            )
        final = _metadata_from_fd(descriptor)
        _require_same_file_identity(opened, final, "START seal creation")
        if final.size != len(raw):
            raise D1HistoricalAttemptWalDurabilityErrorV0(
                "START seal size differs after fsync"
            )
        _require_regular_path_matches_descriptor(path, final, "START seal")
    except FileExistsError as error:
        raise D1HistoricalAttemptWalStateErrorV0(
            "START seal already exists; a second START is forbidden"
        ) from error
    except OSError as error:
        raise D1HistoricalAttemptWalDurabilityErrorV0(
            "START seal write/fsync failed; no outcome access or retry is allowed"
        ) from error
    finally:
        if descriptor is not None:
            try:
                os.close(descriptor)
            except OSError as error:
                raise D1HistoricalAttemptWalDurabilityErrorV0(
                    "START seal close failed; no outcome access or retry is allowed"
                ) from error
    _sync_attempt_directory_and_parent(locked.attempt_dir)
    _require_same_directory_identity(
        locked.directory_identity,
        _require_attempt_directory(locked.attempt_dir, require_wal=True),
    )
    return _attach_and_validate_start_seal(locked, snapshot)


def _new_start_seal_open_flags() -> int:
    return (
        os.O_RDWR
        | os.O_CREAT
        | os.O_EXCL
        | getattr(os, "O_BINARY", 0)
        | getattr(os, "O_CLOEXEC", 0)
        | getattr(os, "O_NOFOLLOW", 0)
    )


def _write_start_seal_all(descriptor: int, payload: bytes) -> None:
    offset = 0
    while offset < len(payload):
        written = _write_start_seal_once(descriptor, payload[offset:])
        if type(written) is not int or written <= 0 or written > len(payload) - offset:
            raise D1HistoricalAttemptWalDurabilityErrorV0(
                "START seal write made invalid progress; no retry is allowed"
            )
        offset += written


def _write_start_seal_once(descriptor: int, payload: bytes) -> int:
    return os.write(descriptor, payload)


def _read_exact_descriptor(descriptor: int, expected_size: int) -> bytes:
    chunks: list[bytes] = []
    remaining = expected_size
    while remaining:
        chunk = os.read(descriptor, min(remaining, 64 * 1024))
        if not chunk:
            raise D1HistoricalAttemptWalDurabilityErrorV0(
                "durable file ended during same-descriptor readback"
            )
        chunks.append(chunk)
        remaining -= len(chunk)
    if os.read(descriptor, 1):
        raise D1HistoricalAttemptWalDurabilityErrorV0(
            "durable file grew during same-descriptor readback"
        )
    return b"".join(chunks)


def _read_exact_start_seal(path: Path) -> bytes:
    descriptor: int | None = None
    try:
        before_stat = _lstat_required(path, "START seal")
        if _is_link_or_reparse(before_stat) or not stat.S_ISREG(before_stat.st_mode):
            raise D1HistoricalAttemptWalIntegrityErrorV0(
                "START seal must be a regular non-symlink file"
            )
        descriptor = os.open(
            path,
            os.O_RDONLY
            | getattr(os, "O_BINARY", 0)
            | getattr(os, "O_CLOEXEC", 0)
            | getattr(os, "O_NOFOLLOW", 0),
        )
        opened = _metadata_from_fd(descriptor)
        before = _file_metadata(before_stat)
        _require_regular_single_link(opened, "START seal")
        _require_same_file_identity(before, opened, "START seal open")
        if opened.size < 1 or opened.size > D1_ATTEMPT_START_SEAL_MAX_BYTES_V0:
            raise D1HistoricalAttemptWalIntegrityErrorV0(
                "START seal size is outside its cap"
            )
        raw = _read_exact_descriptor(descriptor, opened.size)
        after = _metadata_from_fd(descriptor)
        if after != opened:
            raise D1HistoricalAttemptWalIntegrityErrorV0(
                "START seal changed during stable read"
            )
        _require_regular_path_matches_descriptor(path, after, "START seal")
        return raw
    except OSError as error:
        raise D1HistoricalAttemptWalIntegrityErrorV0(
            "START seal cannot be read stably"
        ) from error
    finally:
        if descriptor is not None:
            os.close(descriptor)


def _require_regular_path_matches_descriptor(
    path: Path,
    descriptor: _FileMetadataV0,
    label: str,
) -> None:
    observed_stat = _lstat_required(path, label)
    if _is_link_or_reparse(observed_stat):
        raise D1HistoricalAttemptWalIntegrityErrorV0(
            f"{label} path became a symlink or reparse point"
        )
    observed = _file_metadata(observed_stat)
    _require_regular_single_link(observed, f"{label} path")
    _require_same_file_identity(descriptor, observed, f"{label} path")
    if observed.size != descriptor.size:
        raise D1HistoricalAttemptWalIntegrityErrorV0(
            f"{label} path size differs from its open descriptor"
        )


def _torn_tail(
    kind: D1AttemptWalTornTailKindV0,
    offset: int,
    raw: bytes,
) -> D1AttemptWalTornTailV0:
    return D1AttemptWalTornTailV0(
        kind=kind,
        offset_bytes=offset,
        length_bytes=len(raw),
        tail_sha256=hashlib.sha256(raw).hexdigest(),
    )


def _unique_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate JSON key")
        result[key] = value
    return result


def _reject_json_number(value: str) -> object:
    raise ValueError(f"non-integer JSON number is forbidden: {value}")


def _text(document: dict[str, object], key: str) -> str:
    value = document.get(key)
    if not isinstance(value, str):
        raise D1HistoricalAttemptWalIntegrityErrorV0(f"attempt field {key} must be text")
    return value


def _optional_text(document: dict[str, object], key: str) -> str | None:
    value = document.get(key)
    if value is None:
        return None
    if not isinstance(value, str):
        raise D1HistoricalAttemptWalIntegrityErrorV0(
            f"attempt field {key} must be text or null"
        )
    return value


def _integer(document: dict[str, object], key: str) -> int:
    value = document.get(key)
    if type(value) is not int:
        raise D1HistoricalAttemptWalIntegrityErrorV0(f"attempt field {key} must be an int")
    return value


def _require_sha256(value: object, label: str) -> str:
    if not isinstance(value, str) or _SHA256_RE.fullmatch(value) is None:
        raise D1HistoricalAttemptWalIntegrityErrorV0(f"{label} must be lowercase SHA-256")
    return value


def _require_timestamp_ms(value: object, label: str) -> int:
    if (
        type(value) is not int
        or value < 0
        or value > D1_ATTEMPT_MAX_SAFE_TIMESTAMP_MS_V0
    ):
        raise D1HistoricalAttemptWalIntegrityErrorV0(
            f"{label} must be nonnegative JSON-safe integer ms"
        )
    return value


def _require_detail_code(value: object) -> str:
    if not isinstance(value, str) or _DETAIL_CODE_RE.fullmatch(value) is None:
        raise D1HistoricalAttemptWalIntegrityErrorV0(
            "terminal detail_code must be fixed uppercase token text"
        )
    return value
