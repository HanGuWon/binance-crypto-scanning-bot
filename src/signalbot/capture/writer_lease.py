from __future__ import annotations

import ctypes
import json
import os
import stat
import threading
import time
import uuid
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from types import TracebackType
from typing import Literal

from signalbot.capture.errors import CaptureError
from signalbot.capture.path_safety import (
    FILE_ATTRIBUTE_REPARSE_POINT,
    inspect_link_free_path,
)

_INVALID_HANDLE_VALUE = ctypes.c_void_p(-1).value
_GENERIC_READ = 0x80000000
_GENERIC_WRITE = 0x40000000
_FILE_READ_ATTRIBUTES = 0x0080
_FILE_SHARE_READ = 0x00000001
_FILE_SHARE_WRITE = 0x00000002
_OPEN_EXISTING = 3
_OPEN_ALWAYS = 4
_FILE_ATTRIBUTE_NORMAL = 0x00000080
_FILE_FLAG_BACKUP_SEMANTICS = 0x02000000
_FILE_FLAG_OPEN_REPARSE_POINT = 0x00200000
_HANDLE_FLAG_INHERIT = 0x00000001
_LOCKFILE_FAIL_IMMEDIATELY = 0x00000001
_LOCKFILE_EXCLUSIVE_LOCK = 0x00000002
_ERROR_LOCK_VIOLATION = 33
_ERROR_SHARING_VIOLATION = 32
_FILE_BEGIN = 0


if os.name == "nt":
    from ctypes import wintypes
else:
    import fcntl


WRITER_LEASE_FILE_NAME = ".signalbot-writer.lock"
MAX_WRITER_LEASE_METADATA_BYTES = 1_024
_MAX_POLL_INTERVAL_MS = 1_000
WriterLeaseBackend = Literal["WINDOWS_LOCKFILEEX", "POSIX_FLOCK"]
WriterLeaseState = Literal["HELD", "RELEASING", "RELEASED"]


class WriterLeaseError(CaptureError):
    """Base error for the process-wide, OS-backed writer lease."""


class WriterLeaseContendedError(WriterLeaseError):
    """Raised when another owner still holds the requested writer scope."""


class WriterLeaseCancelledError(WriterLeaseError):
    """Raised when a bounded acquisition is cancelled by its caller."""


class WriterLeaseNotHeldError(WriterLeaseError):
    """Raised when an operation requires a live lease owned by this process."""


class WriterLeaseSessionStartClaimError(WriterLeaseError):
    """Raised when one lease acquisition cannot issue one unique session start."""


class WriterLeaseSessionClosureClaimError(WriterLeaseError):
    """Raised when one lease acquisition cannot issue one unique session closure."""


class WriterLeaseProspectiveAttemptClaimError(WriterLeaseError):
    """Raised when one lease acquisition forks prospective attempt ownership."""


@dataclass(frozen=True, slots=True)
class _SessionStartClaim:
    canonical_path: str
    manifest_sha256: str | None = None
    byte_count: int | None = None
    file_device: int | None = None
    file_inode: int | None = None
    file_nlink: int | None = None

    @property
    def sealed(self) -> bool:
        return self.manifest_sha256 is not None


@dataclass(frozen=True, slots=True)
class _SessionClosureClaim:
    canonical_path: str
    manifest_sha256: str | None = None
    byte_count: int | None = None
    file_device: int | None = None
    file_inode: int | None = None
    file_nlink: int | None = None

    @property
    def sealed(self) -> bool:
        return self.manifest_sha256 is not None


class _FileTime(ctypes.Structure):
    _fields_ = [("low", ctypes.c_uint32), ("high", ctypes.c_uint32)]


class _Overlapped(ctypes.Structure):
    _fields_ = [
        ("internal", ctypes.c_void_p),
        ("internal_high", ctypes.c_void_p),
        ("offset", ctypes.c_uint32),
        ("offset_high", ctypes.c_uint32),
        ("event", ctypes.c_void_p),
    ]


class _ByHandleFileInformation(ctypes.Structure):
    _fields_ = [
        ("file_attributes", ctypes.c_uint32),
        ("creation_time", _FileTime),
        ("last_access_time", _FileTime),
        ("last_write_time", _FileTime),
        ("volume_serial_number", ctypes.c_uint32),
        ("file_size_high", ctypes.c_uint32),
        ("file_size_low", ctypes.c_uint32),
        ("number_of_links", ctypes.c_uint32),
        ("file_index_high", ctypes.c_uint32),
        ("file_index_low", ctypes.c_uint32),
    ]


_FILE_ID = tuple[int, int]


class _OpenedLease:
    __slots__ = (
        "lock_handle",
        "lock_identity",
        "scope_handle",
        "scope_identity",
    )

    def __init__(
        self,
        *,
        scope_handle: int,
        lock_handle: int,
        scope_identity: _FILE_ID,
        lock_identity: _FILE_ID,
    ) -> None:
        self.scope_handle = scope_handle
        self.lock_handle = lock_handle
        self.scope_identity = scope_identity
        self.lock_identity = lock_identity


class _Reservation:
    __slots__ = ()


class WriterLease:
    """One exclusive writer lease rooted at a stable, link-free directory.

    The OS lock and the identity of the open lock file are the ownership proof. The
    bounded JSON document left in the lock file contains diagnostic owner metadata only;
    a PID, timestamp, or stale document never implies that the lease is held or abandoned.
    The lock file is intentionally persistent and is never deleted during release or crash
    recovery.
    """

    _backend: WriterLeaseBackend

    __slots__ = (
        "_acquired_monotonic_ns",
        "_acquired_wall_ms",
        "_active_operation_count",
        "_backend",
        "_invalid_after_fork",
        "_lock_handle",
        "_lock_identity",
        "_lock_path",
        "_owner_id",
        "_owner_pid",
        "_prospective_attempt_plan_sha256",
        "_registry_key",
        "_scope_handle",
        "_scope_identity",
        "_scope_root",
        "_session_closure_claim",
        "_session_start_claim",
        "_state",
        "_state_lock",
    )

    def __init__(
        self,
        *,
        scope_root: Path,
        lock_path: Path,
        registry_key: str,
        owner_pid: int,
        owner_id: str,
        backend: WriterLeaseBackend,
        acquired_wall_ms: int,
        acquired_monotonic_ns: int,
        opened: _OpenedLease,
    ) -> None:
        self._scope_root = scope_root
        self._lock_path = lock_path
        self._registry_key = registry_key
        self._owner_pid = owner_pid
        self._owner_id = owner_id
        self._backend = backend
        self._acquired_wall_ms = acquired_wall_ms
        self._acquired_monotonic_ns = acquired_monotonic_ns
        self._scope_handle = opened.scope_handle
        self._lock_handle = opened.lock_handle
        self._scope_identity = opened.scope_identity
        self._lock_identity = opened.lock_identity
        self._state: WriterLeaseState = "HELD"
        self._state_lock = threading.RLock()
        self._active_operation_count = 0
        self._prospective_attempt_plan_sha256: str | None = None
        self._session_closure_claim: _SessionClosureClaim | None = None
        self._session_start_claim: _SessionStartClaim | None = None
        self._invalid_after_fork = False

    @classmethod
    def acquire(
        cls,
        scope_root: str | Path,
        *,
        wait_timeout_ms: int = 0,
        poll_interval_ms: int = 25,
        cancelled: Callable[[], bool] | None = None,
    ) -> WriterLease:
        """Acquire a lease without blocking, or poll for at most ``wait_timeout_ms``.

        Waiting is always finite. ``cancelled`` is checked before every lock attempt and
        during the bounded polling loop so shutdown paths do not need to wait for the
        deadline.
        """

        _validate_wait_parameters(wait_timeout_ms, poll_interval_ms)
        inspected_scope = inspect_link_free_path(scope_root, "writer lease scope_root")
        scope_status = inspected_scope.final_status
        if scope_status is None or not stat.S_ISDIR(scope_status.st_mode):
            raise ValueError("writer lease scope_root must be an existing directory")
        absolute_scope = inspected_scope.absolute_path
        lock_path = absolute_scope / WRITER_LEASE_FILE_NAME
        _inspect_lock_path(lock_path)
        registry_key = os.path.normcase(os.fspath(absolute_scope))
        opened = _open_lease_resources(absolute_scope, lock_path)
        owner_pid = os.getpid()
        owner_id = uuid.uuid4().hex
        deadline_ns = time.monotonic_ns() + wait_timeout_ms * 1_000_000
        reservation = _Reservation()

        try:
            while True:
                if cancelled is not None and cancelled():
                    raise WriterLeaseCancelledError(
                        f"writer lease acquisition was cancelled for {absolute_scope}"
                    )
                reserved = _reserve_process_scope(registry_key, reservation)
                if reserved:
                    try:
                        locked = _try_lock(opened.lock_handle)
                    except BaseException:
                        _remove_reservation(registry_key, reservation)
                        raise
                    if locked:
                        return cls._finish_acquire(
                            scope_root=absolute_scope,
                            lock_path=lock_path,
                            registry_key=registry_key,
                            owner_pid=owner_pid,
                            owner_id=owner_id,
                            opened=opened,
                            reservation=reservation,
                        )
                    _remove_reservation(registry_key, reservation)

                now_ns = time.monotonic_ns()
                if now_ns >= deadline_ns:
                    raise WriterLeaseContendedError(
                        f"writer lease is already held for {absolute_scope}"
                    )
                remaining_ns = deadline_ns - now_ns
                sleep_seconds = min(poll_interval_ms / 1_000, remaining_ns / 1_000_000_000)
                time.sleep(sleep_seconds)
        except BaseException:
            _close_opened(opened)
            raise

    @classmethod
    def _finish_acquire(
        cls,
        *,
        scope_root: Path,
        lock_path: Path,
        registry_key: str,
        owner_pid: int,
        owner_id: str,
        opened: _OpenedLease,
        reservation: _Reservation,
    ) -> WriterLease:
        try:
            _assert_resource_identities(
                scope_root=scope_root,
                lock_path=lock_path,
                opened=opened,
            )
            backend = _writer_lease_backend()
            acquired_wall_ms = time.time_ns() // 1_000_000
            acquired_monotonic_ns = time.monotonic_ns()
            if acquired_wall_ms <= 0 or acquired_monotonic_ns <= 0:
                raise WriterLeaseError("writer lease acquisition clocks must be positive")
            metadata = _owner_metadata(
                owner_pid=owner_pid,
                owner_id=owner_id,
                backend=backend,
                acquired_wall_ms=acquired_wall_ms,
                acquired_monotonic_ns=acquired_monotonic_ns,
            )
            _write_owner_metadata(opened, metadata)
            lease = cls(
                scope_root=scope_root,
                lock_path=lock_path,
                registry_key=registry_key,
                owner_pid=owner_pid,
                owner_id=owner_id,
                backend=backend,
                acquired_wall_ms=acquired_wall_ms,
                acquired_monotonic_ns=acquired_monotonic_ns,
                opened=opened,
            )
            _promote_reservation(registry_key, reservation, lease)
            return lease
        except BaseException:
            try:
                _unlock(opened.lock_handle)
            finally:
                _remove_reservation(registry_key, reservation)
            raise

    @property
    def scope_root(self) -> Path:
        return self._scope_root

    @property
    def lock_path(self) -> Path:
        return self._lock_path

    @property
    def owner_pid(self) -> int:
        return self._owner_pid

    @property
    def owner_id(self) -> str:
        return self._owner_id

    @property
    def backend(self) -> WriterLeaseBackend:
        return self._backend

    @property
    def acquired_wall_ms(self) -> int:
        return self._acquired_wall_ms

    @property
    def acquired_monotonic_ns(self) -> int:
        return self._acquired_monotonic_ns

    def assert_held(self) -> None:
        """Fail closed unless this exact process still owns this exact path identity."""

        with self._state_lock:
            if self._state == "RELEASED":
                raise WriterLeaseNotHeldError("writer lease has already been released")
            if self._invalid_after_fork:
                raise WriterLeaseNotHeldError(
                    "writer lease was invalidated in a forked child"
                )
            if self._state == "RELEASING":
                raise WriterLeaseNotHeldError("writer lease release is already in progress")
            if os.getpid() != self._owner_pid:
                raise WriterLeaseNotHeldError("writer lease belongs to a different process")
            with _REGISTRY_LOCK:
                if _REGISTRY.get(self._registry_key) is not self:
                    raise WriterLeaseNotHeldError(
                        "writer lease is absent from the current process registry"
                    )
            _assert_resource_identities(
                scope_root=self._scope_root,
                lock_path=self._lock_path,
                opened=_OpenedLease(
                    scope_handle=self._scope_handle,
                    lock_handle=self._lock_handle,
                    scope_identity=self._scope_identity,
                    lock_identity=self._lock_identity,
                ),
            )

    def release(self) -> None:
        """Release the OS lock while deliberately preserving the stable lock file."""

        with self._state_lock:
            if self._active_operation_count > 0:
                raise WriterLeaseError(
                    "writer lease cannot be released during an active storage operation"
                )
            self.assert_held()
            self._state = "RELEASING"
            failure: BaseException | None = None
            try:
                _unlock(self._lock_handle)
            except BaseException as exc:
                failure = exc
            try:
                _close_handle(self._lock_handle)
            except BaseException as exc:
                if failure is None:
                    failure = exc
            try:
                _close_handle(self._scope_handle)
            except BaseException as exc:
                if failure is None:
                    failure = exc
            self._state = "RELEASED"
            with _REGISTRY_LOCK:
                if _REGISTRY.get(self._registry_key) is self:
                    del _REGISTRY[self._registry_key]
            if failure is not None:
                raise WriterLeaseError("writer lease release failed") from failure

    @contextmanager
    def operation_guard(self) -> Iterator[None]:
        """Hold the lease state lock across one exact storage mutation boundary.

        A release from another thread blocks until the operation exits. A
        same-thread reentrant release is rejected by the active-operation
        counter, including another asyncio task scheduled during an awaited
        connector admission.
        """

        if type(self) is not WriterLease:
            raise TypeError("writer lease operation guard requires exact WriterLease")
        with self._state_lock:
            self.assert_held()
            self._active_operation_count += 1
            try:
                yield
                self.assert_held()
            finally:
                self._active_operation_count -= 1

    def claim_session_start_authority(self, *, canonical_path: str) -> None:
        """Consume this acquisition's sole session-start issuance attempt.

        The claim is deliberately irreversible. If validation or persistence later
        fails, callers must release this lease and acquire a new one rather than
        manufacturing a second start under the same acquisition identity.
        """

        if type(self) is not WriterLease:
            raise TypeError("session-start claim requires exact WriterLease")
        with self._state_lock:
            self._assert_claim_operation_guarded()
            if not isinstance(canonical_path, str) or not canonical_path:
                raise ValueError("session-start canonical_path must be non-empty")
            if not os.path.isabs(canonical_path):
                raise ValueError("session-start canonical_path must be absolute")
            if self._session_start_claim is not None:
                raise WriterLeaseSessionStartClaimError(
                    "writer lease acquisition already consumed its session-start claim"
                )
            self._session_start_claim = _SessionStartClaim(
                canonical_path=canonical_path,
            )

    def seal_session_start_authority(
        self,
        *,
        canonical_path: str,
        manifest_sha256: str,
        byte_count: int,
        file_device: int,
        file_inode: int,
        file_nlink: int,
    ) -> None:
        """Irreversibly bind the claimed start to its exact persisted identity."""

        if type(self) is not WriterLease:
            raise TypeError("session-start seal requires exact WriterLease")
        with self._state_lock:
            self._assert_claim_operation_guarded()
            claim = self._session_start_claim
            if claim is None:
                raise WriterLeaseSessionStartClaimError(
                    "writer lease acquisition has no session-start claim"
                )
            if claim.sealed:
                raise WriterLeaseSessionStartClaimError(
                    "writer lease acquisition session-start claim is already sealed"
                )
            if canonical_path != claim.canonical_path:
                raise WriterLeaseSessionStartClaimError(
                    "session-start seal path differs from its initial lease claim"
                )
            _validate_session_start_identity(
                manifest_sha256=manifest_sha256,
                byte_count=byte_count,
                file_device=file_device,
                file_inode=file_inode,
                file_nlink=file_nlink,
            )
            self._session_start_claim = _SessionStartClaim(
                canonical_path=canonical_path,
                manifest_sha256=manifest_sha256,
                byte_count=byte_count,
                file_device=file_device,
                file_inode=file_inode,
                file_nlink=file_nlink,
            )

    def assert_session_start_authority_claim(
        self,
        *,
        canonical_path: str,
        manifest_sha256: str,
        byte_count: int,
        file_device: int,
        file_inode: int,
        file_nlink: int,
    ) -> None:
        """Verify a receipt against this acquisition's immutable sealed claim."""

        if type(self) is not WriterLease:
            raise TypeError("session-start claim assertion requires exact WriterLease")
        with self._state_lock:
            self._assert_claim_operation_guarded()
            expected = _SessionStartClaim(
                canonical_path=canonical_path,
                manifest_sha256=manifest_sha256,
                byte_count=byte_count,
                file_device=file_device,
                file_inode=file_inode,
                file_nlink=file_nlink,
            )
            if self._session_start_claim != expected:
                raise WriterLeaseSessionStartClaimError(
                    "persisted session-start receipt differs from the lease claim"
                )

    def claim_session_closure_authority(self, *, canonical_path: str) -> None:
        """Consume this acquisition's sole session-closure issuance attempt.

        The claim is deliberately irreversible. If validation or persistence later
        fails, callers must release this lease and acquire a new one rather than
        manufacturing a second closure under the same acquisition identity.
        """

        if type(self) is not WriterLease:
            raise TypeError("session-closure claim requires exact WriterLease")
        with self._state_lock:
            self._assert_session_closure_claim_operation_guarded()
            if not isinstance(canonical_path, str) or not canonical_path:
                raise ValueError("session-closure canonical_path must be non-empty")
            if not os.path.isabs(canonical_path):
                raise ValueError("session-closure canonical_path must be absolute")
            if self._session_closure_claim is not None:
                raise WriterLeaseSessionClosureClaimError(
                    "writer lease acquisition already consumed its session-closure claim"
                )
            self._session_closure_claim = _SessionClosureClaim(
                canonical_path=canonical_path,
            )

    def seal_session_closure_authority(
        self,
        *,
        canonical_path: str,
        manifest_sha256: str,
        byte_count: int,
        file_device: int,
        file_inode: int,
        file_nlink: int,
    ) -> None:
        """Irreversibly bind the claimed closure to its persisted identity."""

        if type(self) is not WriterLease:
            raise TypeError("session-closure seal requires exact WriterLease")
        with self._state_lock:
            self._assert_session_closure_claim_operation_guarded()
            claim = self._session_closure_claim
            if claim is None:
                raise WriterLeaseSessionClosureClaimError(
                    "writer lease acquisition has no session-closure claim"
                )
            if claim.sealed:
                raise WriterLeaseSessionClosureClaimError(
                    "writer lease acquisition session-closure claim is already sealed"
                )
            if canonical_path != claim.canonical_path:
                raise WriterLeaseSessionClosureClaimError(
                    "session-closure seal path differs from its initial lease claim"
                )
            _validate_session_closure_identity(
                manifest_sha256=manifest_sha256,
                byte_count=byte_count,
                file_device=file_device,
                file_inode=file_inode,
                file_nlink=file_nlink,
            )
            self._session_closure_claim = _SessionClosureClaim(
                canonical_path=canonical_path,
                manifest_sha256=manifest_sha256,
                byte_count=byte_count,
                file_device=file_device,
                file_inode=file_inode,
                file_nlink=file_nlink,
            )

    def assert_session_closure_authority_claim(
        self,
        *,
        canonical_path: str,
        manifest_sha256: str,
        byte_count: int,
        file_device: int,
        file_inode: int,
        file_nlink: int,
    ) -> None:
        """Verify a receipt against this acquisition's immutable sealed claim."""

        if type(self) is not WriterLease:
            raise TypeError("session-closure claim assertion requires exact WriterLease")
        with self._state_lock:
            self._assert_session_closure_claim_operation_guarded()
            claim = self._session_closure_claim
            if claim is None or not claim.sealed:
                raise WriterLeaseSessionClosureClaimError(
                    "writer lease acquisition session-closure claim is not sealed"
                )
            expected = _SessionClosureClaim(
                canonical_path=canonical_path,
                manifest_sha256=manifest_sha256,
                byte_count=byte_count,
                file_device=file_device,
                file_inode=file_inode,
                file_nlink=file_nlink,
            )
            if claim != expected:
                raise WriterLeaseSessionClosureClaimError(
                    "persisted session-closure receipt differs from the lease claim"
                )

    def claim_prospective_attempt_authority(
        self,
        *,
        attempt_plan_sha256: str,
    ) -> None:
        """Irreversibly assign this acquisition to one prospective owner.

        A later lease acquisition may resume the same durable attempt.  Within
        one acquisition, however, a second owner is forbidden even when it asks
        for the same plan, preventing two in-process writers from sharing the
        same OS-lock proof.
        """

        if type(self) is not WriterLease:
            raise TypeError("prospective-attempt claim requires exact WriterLease")
        with self._state_lock:
            self._assert_prospective_attempt_claim_operation_guarded()
            _validate_sha256_text(
                attempt_plan_sha256,
                "attempt_plan_sha256",
            )
            if self._prospective_attempt_plan_sha256 is not None:
                raise WriterLeaseProspectiveAttemptClaimError(
                    "writer lease acquisition already consumed its prospective-attempt claim"
                )
            self._prospective_attempt_plan_sha256 = attempt_plan_sha256

    def assert_prospective_attempt_authority_claim(
        self,
        *,
        attempt_plan_sha256: str,
    ) -> None:
        """Verify that the guarded operation belongs to the claimed attempt."""

        if type(self) is not WriterLease:
            raise TypeError(
                "prospective-attempt claim assertion requires exact WriterLease"
            )
        with self._state_lock:
            self._assert_prospective_attempt_claim_operation_guarded()
            _validate_sha256_text(
                attempt_plan_sha256,
                "attempt_plan_sha256",
            )
            if self._prospective_attempt_plan_sha256 != attempt_plan_sha256:
                raise WriterLeaseProspectiveAttemptClaimError(
                    "prospective-attempt plan differs from the lease claim"
                )

    def _assert_prospective_attempt_claim_operation_guarded(self) -> None:
        self.assert_held()
        if self._active_operation_count < 1:
            raise WriterLeaseProspectiveAttemptClaimError(
                "prospective-attempt claim mutation requires an active lease operation guard"
            )

    def _assert_session_closure_claim_operation_guarded(self) -> None:
        self.assert_held()
        if self._active_operation_count < 1:
            raise WriterLeaseSessionClosureClaimError(
                "session-closure claim mutation requires an active lease operation guard"
            )

    def _assert_claim_operation_guarded(self) -> None:
        self.assert_held()
        if self._active_operation_count < 1:
            raise WriterLeaseSessionStartClaimError(
                "session-start claim mutation requires an active lease operation guard"
            )

    def __enter__(self) -> WriterLease:
        self.assert_held()
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        del exc_type, exc_value, traceback
        self.release()

    def _invalidate_in_forked_child(self) -> None:
        # A lock held by another parent thread remains locked after fork. Replace it
        # before touching inherited handles so the child cannot deadlock here.
        self._state_lock = threading.RLock()
        with self._state_lock:
            self._active_operation_count = 0
            if self._state == "RELEASED":
                return
            self._invalid_after_fork = True
            # Never call LOCK_UN in the child: POSIX flock state is shared by the
            # inherited open-file description, so an explicit unlock could release the
            # parent's lease.
            for handle in (self._lock_handle, self._scope_handle):
                try:
                    _close_handle(handle)
                except OSError:
                    pass


type _RegistryEntry = WriterLease | _Reservation
_REGISTRY: dict[str, _RegistryEntry] = {}
_REGISTRY_LOCK = threading.RLock()


def _validate_sha256_text(value: object, field_name: str) -> None:
    if (
        not isinstance(value, str)
        or len(value) != 64
        or any(character not in "0123456789abcdef" for character in value)
    ):
        raise ValueError(f"{field_name} must be a lowercase SHA-256")


def _validate_session_start_identity(
    *,
    manifest_sha256: str,
    byte_count: int,
    file_device: int,
    file_inode: int,
    file_nlink: int,
) -> None:
    if (
        not isinstance(manifest_sha256, str)
        or len(manifest_sha256) != 64
        or any(character not in "0123456789abcdef" for character in manifest_sha256)
    ):
        raise ValueError("session-start manifest_sha256 must be a lowercase SHA-256")
    if type(byte_count) is not int or byte_count < 1:
        raise ValueError("session-start byte_count must be positive")
    for value, field in (
        (file_device, "file_device"),
        (file_inode, "file_inode"),
    ):
        if type(value) is not int or value < 0:
            raise ValueError(f"session-start {field} must be nonnegative")
    if type(file_nlink) is not int or file_nlink != 1:
        raise ValueError("session-start file_nlink must equal one")


def _validate_session_closure_identity(
    *,
    manifest_sha256: str,
    byte_count: int,
    file_device: int,
    file_inode: int,
    file_nlink: int,
) -> None:
    if (
        not isinstance(manifest_sha256, str)
        or len(manifest_sha256) != 64
        or any(character not in "0123456789abcdef" for character in manifest_sha256)
    ):
        raise ValueError("session-closure manifest_sha256 must be a lowercase SHA-256")
    if type(byte_count) is not int or byte_count < 1:
        raise ValueError("session-closure byte_count must be positive")
    for value, field in (
        (file_device, "file_device"),
        (file_inode, "file_inode"),
    ):
        if type(value) is not int or value < 0:
            raise ValueError(f"session-closure {field} must be nonnegative")
    if type(file_nlink) is not int or file_nlink != 1:
        raise ValueError("session-closure file_nlink must equal one")


def _validate_wait_parameters(wait_timeout_ms: int, poll_interval_ms: int) -> None:
    if type(wait_timeout_ms) is not int or wait_timeout_ms < 0:
        raise ValueError("wait_timeout_ms must be a nonnegative integer")
    if (
        type(poll_interval_ms) is not int
        or not 1 <= poll_interval_ms <= _MAX_POLL_INTERVAL_MS
    ):
        raise ValueError(
            f"poll_interval_ms must be between 1 and {_MAX_POLL_INTERVAL_MS}"
        )


def _inspect_lock_path(lock_path: Path) -> None:
    inspection = inspect_link_free_path(
        lock_path,
        "writer lease lock_path",
        allow_missing_tail=True,
    )
    status = inspection.final_status
    if status is not None and not stat.S_ISREG(status.st_mode):
        raise ValueError("writer lease lock_path must be a regular file when present")


def _reserve_process_scope(registry_key: str, reservation: _Reservation) -> bool:
    with _REGISTRY_LOCK:
        if registry_key in _REGISTRY:
            return False
        _REGISTRY[registry_key] = reservation
        return True


def _remove_reservation(registry_key: str, reservation: _Reservation) -> None:
    with _REGISTRY_LOCK:
        if _REGISTRY.get(registry_key) is reservation:
            del _REGISTRY[registry_key]


def _promote_reservation(
    registry_key: str,
    reservation: _Reservation,
    lease: WriterLease,
) -> None:
    with _REGISTRY_LOCK:
        if _REGISTRY.get(registry_key) is not reservation:
            raise WriterLeaseError("writer lease process reservation was lost")
        _REGISTRY[registry_key] = lease


def _owner_metadata(
    *,
    owner_pid: int,
    owner_id: str,
    backend: WriterLeaseBackend,
    acquired_wall_ms: int,
    acquired_monotonic_ns: int,
) -> bytes:
    document = {
        "acquired_monotonic_ns": acquired_monotonic_ns,
        "acquired_wall_ms": acquired_wall_ms,
        "backend": backend,
        "notice": "diagnostic_only_not_ownership_proof",
        "owner_id": owner_id,
        "owner_pid": owner_pid,
        "schema_version": "signalbot_writer_lease_metadata_v1",
    }
    payload = (
        json.dumps(document, sort_keys=True, separators=(",", ":"), ensure_ascii=True) + "\n"
    ).encode("ascii")
    if len(payload) > MAX_WRITER_LEASE_METADATA_BYTES:
        raise WriterLeaseError("writer lease metadata exceeded its fixed byte bound")
    return payload


def _writer_lease_backend() -> WriterLeaseBackend:
    if os.name == "nt":
        return "WINDOWS_LOCKFILEEX"
    return "POSIX_FLOCK"


def _open_lease_resources(scope_root: Path, lock_path: Path) -> _OpenedLease:
    if os.name == "nt":
        return _windows_open_resources(scope_root, lock_path)
    return _posix_open_resources(scope_root, lock_path)


def _try_lock(lock_handle: int) -> bool:
    if os.name == "nt":
        return _windows_try_lock(lock_handle)
    try:
        fcntl.flock(lock_handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        return False
    return True


def _unlock(lock_handle: int) -> None:
    if os.name == "nt":
        _windows_unlock(lock_handle)
        return
    fcntl.flock(lock_handle, fcntl.LOCK_UN)


def _close_handle(handle: int) -> None:
    if os.name == "nt":
        _windows_close_handle(handle)
        return
    os.close(handle)


def _close_opened(opened: _OpenedLease) -> None:
    failure: OSError | None = None
    for handle in (opened.lock_handle, opened.scope_handle):
        try:
            _close_handle(handle)
        except OSError as exc:
            if failure is None:
                failure = exc
    if failure is not None:
        raise failure


def _write_owner_metadata(opened: _OpenedLease, payload: bytes) -> None:
    if os.name == "nt":
        _windows_write_metadata(opened.lock_handle, payload)
        return
    os.lseek(opened.lock_handle, 0, os.SEEK_SET)
    total = 0
    while total < len(payload):
        written = os.write(opened.lock_handle, payload[total:])
        if written <= 0:
            raise WriterLeaseError("writer lease metadata write made no progress")
        total += written
    os.ftruncate(opened.lock_handle, len(payload))
    os.fsync(opened.lock_handle)
    os.fsync(opened.scope_handle)


def _assert_resource_identities(
    *,
    scope_root: Path,
    lock_path: Path,
    opened: _OpenedLease,
) -> None:
    try:
        inspected_scope = inspect_link_free_path(scope_root, "writer lease scope_root")
        inspected_lock = inspect_link_free_path(lock_path, "writer lease lock_path")
    except ValueError as exc:
        raise WriterLeaseNotHeldError("writer lease pathname is no longer link-free") from exc
    scope_status = inspected_scope.final_status
    if scope_status is None or not stat.S_ISDIR(scope_status.st_mode):
        raise WriterLeaseNotHeldError("writer lease scope_root is no longer a directory")
    lock_status = inspected_lock.final_status
    if lock_status is None or not stat.S_ISREG(lock_status.st_mode):
        raise WriterLeaseNotHeldError("writer lease lock_path is no longer a regular file")

    if os.name == "nt":
        current_scope = _windows_file_identity_for_handle(opened.scope_handle)
        current_lock_info = _windows_file_information(opened.lock_handle)
        if int(current_lock_info.number_of_links) != 1:
            raise WriterLeaseNotHeldError(
                "writer lease lock handle no longer has exactly one hard link"
            )
        current_lock = _windows_identity(lock_info=current_lock_info)
        path_scope = _windows_file_identity_for_path(scope_root, directory=True)
        path_lock = _windows_file_identity_for_path(lock_path, directory=False)
    else:
        current_scope_status = os.fstat(opened.scope_handle)
        current_lock_status = os.fstat(opened.lock_handle)
        if int(current_lock_status.st_nlink) != 1:
            raise WriterLeaseNotHeldError(
                "writer lease lock handle no longer has exactly one hard link"
            )
        current_scope = _posix_identity(current_scope_status)
        current_lock = _posix_identity(current_lock_status)
        path_scope = _posix_identity(scope_status)
        path_lock = _posix_identity(lock_status)

    if current_scope != opened.scope_identity or path_scope != opened.scope_identity:
        raise WriterLeaseNotHeldError("writer lease scope_root changed pathname identity")
    if current_lock != opened.lock_identity or path_lock != opened.lock_identity:
        raise WriterLeaseNotHeldError("writer lease lock_path changed pathname identity")


def _posix_open_resources(scope_root: Path, lock_path: Path) -> _OpenedLease:
    nofollow = int(getattr(os, "O_NOFOLLOW", 0))
    directory = int(getattr(os, "O_DIRECTORY", 0))
    if nofollow == 0 or directory == 0:
        raise WriterLeaseError("this POSIX platform lacks required no-follow directory opens")
    scope_flags = os.O_RDONLY | nofollow | directory | int(getattr(os, "O_CLOEXEC", 0))
    try:
        scope_handle = os.open(scope_root, scope_flags)
    except OSError as exc:
        raise WriterLeaseError(
            f"writer lease scope_root could not be opened safely: {exc}"
        ) from exc
    try:
        os.set_inheritable(scope_handle, False)
        scope_status = os.fstat(scope_handle)
        if not stat.S_ISDIR(scope_status.st_mode):
            raise WriterLeaseError("writer lease scope_root handle is not a directory")
        lock_flags = (
            os.O_RDWR
            | os.O_CREAT
            | nofollow
            | int(getattr(os, "O_CLOEXEC", 0))
        )
        lock_handle = os.open(
            lock_path.name,
            lock_flags,
            0o600,
            dir_fd=scope_handle,
        )
        try:
            os.set_inheritable(lock_handle, False)
            lock_status = os.fstat(lock_handle)
            if not stat.S_ISREG(lock_status.st_mode):
                raise WriterLeaseError("writer lease lock handle is not a regular file")
            if int(lock_status.st_nlink) != 1:
                raise WriterLeaseError(
                    "writer lease lock handle must have exactly one hard link"
                )
            opened = _OpenedLease(
                scope_handle=scope_handle,
                lock_handle=lock_handle,
                scope_identity=_posix_identity(scope_status),
                lock_identity=_posix_identity(lock_status),
            )
            _assert_resource_identities(
                scope_root=scope_root,
                lock_path=lock_path,
                opened=opened,
            )
            return opened
        except BaseException:
            os.close(lock_handle)
            raise
    except BaseException:
        os.close(scope_handle)
        raise


def _posix_identity(status: os.stat_result) -> _FILE_ID:
    return (int(status.st_dev), int(status.st_ino))


if os.name == "nt":
    _KERNEL32 = ctypes.WinDLL("kernel32", use_last_error=True)
    _CREATE_FILE_W = _KERNEL32.CreateFileW
    _CREATE_FILE_W.argtypes = [
        wintypes.LPCWSTR,
        wintypes.DWORD,
        wintypes.DWORD,
        ctypes.c_void_p,
        wintypes.DWORD,
        wintypes.DWORD,
        wintypes.HANDLE,
    ]
    _CREATE_FILE_W.restype = wintypes.HANDLE
    _CLOSE_HANDLE = _KERNEL32.CloseHandle
    _CLOSE_HANDLE.argtypes = [wintypes.HANDLE]
    _CLOSE_HANDLE.restype = wintypes.BOOL
    _SET_HANDLE_INFORMATION = _KERNEL32.SetHandleInformation
    _SET_HANDLE_INFORMATION.argtypes = [wintypes.HANDLE, wintypes.DWORD, wintypes.DWORD]
    _SET_HANDLE_INFORMATION.restype = wintypes.BOOL
    _LOCK_FILE_EX = _KERNEL32.LockFileEx
    _LOCK_FILE_EX.argtypes = [
        wintypes.HANDLE,
        wintypes.DWORD,
        wintypes.DWORD,
        wintypes.DWORD,
        wintypes.DWORD,
        ctypes.POINTER(_Overlapped),
    ]
    _LOCK_FILE_EX.restype = wintypes.BOOL
    _UNLOCK_FILE_EX = _KERNEL32.UnlockFileEx
    _UNLOCK_FILE_EX.argtypes = [
        wintypes.HANDLE,
        wintypes.DWORD,
        wintypes.DWORD,
        wintypes.DWORD,
        ctypes.POINTER(_Overlapped),
    ]
    _UNLOCK_FILE_EX.restype = wintypes.BOOL
    _GET_FILE_INFORMATION = _KERNEL32.GetFileInformationByHandle
    _GET_FILE_INFORMATION.argtypes = [
        wintypes.HANDLE,
        ctypes.POINTER(_ByHandleFileInformation),
    ]
    _GET_FILE_INFORMATION.restype = wintypes.BOOL
    _SET_FILE_POINTER_EX = _KERNEL32.SetFilePointerEx
    _SET_FILE_POINTER_EX.argtypes = [
        wintypes.HANDLE,
        ctypes.c_int64,
        ctypes.POINTER(ctypes.c_int64),
        wintypes.DWORD,
    ]
    _SET_FILE_POINTER_EX.restype = wintypes.BOOL
    _WRITE_FILE = _KERNEL32.WriteFile
    _WRITE_FILE.argtypes = [
        wintypes.HANDLE,
        ctypes.c_void_p,
        wintypes.DWORD,
        ctypes.POINTER(wintypes.DWORD),
        ctypes.c_void_p,
    ]
    _WRITE_FILE.restype = wintypes.BOOL
    _SET_END_OF_FILE = _KERNEL32.SetEndOfFile
    _SET_END_OF_FILE.argtypes = [wintypes.HANDLE]
    _SET_END_OF_FILE.restype = wintypes.BOOL
    _FLUSH_FILE_BUFFERS = _KERNEL32.FlushFileBuffers
    _FLUSH_FILE_BUFFERS.argtypes = [wintypes.HANDLE]
    _FLUSH_FILE_BUFFERS.restype = wintypes.BOOL
    _get_last_error = ctypes.get_last_error
else:

    def _windows_api_unavailable(*_args: object) -> int:
        raise WriterLeaseError("Windows writer lease backend is unavailable")

    _CREATE_FILE_W = _windows_api_unavailable
    _CLOSE_HANDLE = _windows_api_unavailable
    _SET_HANDLE_INFORMATION = _windows_api_unavailable
    _LOCK_FILE_EX = _windows_api_unavailable
    _UNLOCK_FILE_EX = _windows_api_unavailable
    _GET_FILE_INFORMATION = _windows_api_unavailable
    _SET_FILE_POINTER_EX = _windows_api_unavailable
    _WRITE_FILE = _windows_api_unavailable
    _SET_END_OF_FILE = _windows_api_unavailable
    _FLUSH_FILE_BUFFERS = _windows_api_unavailable
    _get_last_error = _windows_api_unavailable


def _windows_open_resources(scope_root: Path, lock_path: Path) -> _OpenedLease:
    if os.name != "nt":
        raise WriterLeaseError("Windows writer lease backend is unavailable")
    scope_handle = _windows_create_file(
        scope_root,
        access=_FILE_READ_ATTRIBUTES,
        creation=_OPEN_EXISTING,
        flags=_FILE_FLAG_BACKUP_SEMANTICS | _FILE_FLAG_OPEN_REPARSE_POINT,
    )
    try:
        scope_info = _windows_file_information(scope_handle)
        if scope_info.file_attributes & FILE_ATTRIBUTE_REPARSE_POINT:
            raise WriterLeaseError("writer lease scope_root is a reparse point")
        lock_handle = _windows_create_file(
            lock_path,
            access=_GENERIC_READ | _GENERIC_WRITE,
            creation=_OPEN_ALWAYS,
            flags=_FILE_ATTRIBUTE_NORMAL | _FILE_FLAG_OPEN_REPARSE_POINT,
        )
        try:
            lock_info = _windows_file_information(lock_handle)
            if lock_info.file_attributes & FILE_ATTRIBUTE_REPARSE_POINT:
                raise WriterLeaseError("writer lease lock_path is a reparse point")
            if int(lock_info.number_of_links) != 1:
                raise WriterLeaseError(
                    "writer lease lock handle must have exactly one hard link"
                )
            opened = _OpenedLease(
                scope_handle=scope_handle,
                lock_handle=lock_handle,
                scope_identity=_windows_identity(lock_info=scope_info),
                lock_identity=_windows_identity(lock_info=lock_info),
            )
            _assert_resource_identities(
                scope_root=scope_root,
                lock_path=lock_path,
                opened=opened,
            )
            return opened
        except BaseException:
            _windows_close_handle(lock_handle)
            raise
    except BaseException:
        _windows_close_handle(scope_handle)
        raise


def _windows_create_file(
    path: Path,
    *,
    access: int,
    creation: int,
    flags: int,
) -> int:
    handle = _CREATE_FILE_W(
        os.fspath(path),
        access,
        _FILE_SHARE_READ | _FILE_SHARE_WRITE,
        None,
        creation,
        flags,
        None,
    )
    value = ctypes.cast(handle, ctypes.c_void_p).value
    if value is None or value == _INVALID_HANDLE_VALUE:
        error = _get_last_error()
        raise OSError(error, os.strerror(error), os.fspath(path))
    if not _SET_HANDLE_INFORMATION(value, _HANDLE_FLAG_INHERIT, 0):
        error = _get_last_error()
        _CLOSE_HANDLE(value)
        raise OSError(error, os.strerror(error), os.fspath(path))
    return value


def _windows_try_lock(lock_handle: int) -> bool:
    overlapped = _Overlapped()
    if _LOCK_FILE_EX(
        lock_handle,
        _LOCKFILE_EXCLUSIVE_LOCK | _LOCKFILE_FAIL_IMMEDIATELY,
        0,
        0xFFFFFFFF,
        0xFFFFFFFF,
        ctypes.byref(overlapped),
    ):
        return True
    error = _get_last_error()
    if error in {_ERROR_LOCK_VIOLATION, _ERROR_SHARING_VIOLATION}:
        return False
    raise OSError(error, os.strerror(error))


def _windows_unlock(lock_handle: int) -> None:
    overlapped = _Overlapped()
    if not _UNLOCK_FILE_EX(
        lock_handle,
        0,
        0xFFFFFFFF,
        0xFFFFFFFF,
        ctypes.byref(overlapped),
    ):
        error = _get_last_error()
        raise OSError(error, os.strerror(error))


def _windows_close_handle(handle: int) -> None:
    if not _CLOSE_HANDLE(handle):
        error = _get_last_error()
        raise OSError(error, os.strerror(error))


def _windows_file_information(handle: int) -> _ByHandleFileInformation:
    information = _ByHandleFileInformation()
    if not _GET_FILE_INFORMATION(handle, ctypes.byref(information)):
        error = _get_last_error()
        raise OSError(error, os.strerror(error))
    return information


def _windows_identity(*, lock_info: _ByHandleFileInformation) -> _FILE_ID:
    file_index = (int(lock_info.file_index_high) << 32) | int(lock_info.file_index_low)
    return (int(lock_info.volume_serial_number), file_index)


def _windows_file_identity_for_handle(handle: int) -> _FILE_ID:
    return _windows_identity(lock_info=_windows_file_information(handle))


def _windows_file_identity_for_path(path: Path, *, directory: bool) -> _FILE_ID:
    flags = _FILE_FLAG_OPEN_REPARSE_POINT
    if directory:
        flags |= _FILE_FLAG_BACKUP_SEMANTICS
    handle = _windows_create_file(
        path,
        access=_FILE_READ_ATTRIBUTES,
        creation=_OPEN_EXISTING,
        flags=flags,
    )
    try:
        information = _windows_file_information(handle)
        if information.file_attributes & FILE_ATTRIBUTE_REPARSE_POINT:
            raise WriterLeaseNotHeldError(f"writer lease path became a reparse point: {path}")
        return _windows_identity(lock_info=information)
    finally:
        _windows_close_handle(handle)


def _windows_write_metadata(lock_handle: int, payload: bytes) -> None:
    new_position = ctypes.c_int64()
    if not _SET_FILE_POINTER_EX(lock_handle, 0, ctypes.byref(new_position), _FILE_BEGIN):
        error = _get_last_error()
        raise OSError(error, os.strerror(error))
    buffer = ctypes.create_string_buffer(payload)
    written = ctypes.c_uint32()
    if not _WRITE_FILE(
        lock_handle,
        buffer,
        len(payload),
        ctypes.byref(written),
        None,
    ):
        error = _get_last_error()
        raise OSError(error, os.strerror(error))
    if int(written.value) != len(payload):
        raise WriterLeaseError("writer lease metadata write was short")
    if not _SET_END_OF_FILE(lock_handle):
        error = _get_last_error()
        raise OSError(error, os.strerror(error))
    if not _FLUSH_FILE_BUFFERS(lock_handle):
        error = _get_last_error()
        raise OSError(error, os.strerror(error))


def _after_fork_child() -> None:
    global _REGISTRY, _REGISTRY_LOCK
    inherited_entries = tuple(_REGISTRY.values())
    _REGISTRY = {}
    _REGISTRY_LOCK = threading.RLock()
    for entry in inherited_entries:
        if isinstance(entry, WriterLease):
            entry._invalidate_in_forked_child()


_REGISTER_AT_FORK = getattr(os, "register_at_fork", None)
if _REGISTER_AT_FORK is not None:
    _REGISTER_AT_FORK(after_in_child=_after_fork_child)
