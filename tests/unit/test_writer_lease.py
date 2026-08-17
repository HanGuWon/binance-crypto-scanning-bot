from __future__ import annotations

import json
import os
import subprocess
import sys
import threading
import time
from collections.abc import Iterator
from pathlib import Path
from typing import cast

import pytest

import signalbot.capture.writer_lease as writer_lease_module
from signalbot.capture.writer_lease import (
    MAX_WRITER_LEASE_METADATA_BYTES,
    WRITER_LEASE_FILE_NAME,
    WriterLease,
    WriterLeaseCancelledError,
    WriterLeaseContendedError,
    WriterLeaseError,
    WriterLeaseNotHeldError,
    WriterLeaseSessionClosureClaimError,
    WriterLeaseSessionStartClaimError,
)

_PROJECT_ROOT = Path(__file__).parents[2]
_HOLDER_SCRIPT = """
import sys
from pathlib import Path
from signalbot.capture.writer_lease import WriterLease

lease = WriterLease.acquire(Path(sys.argv[1]))
print(f"READY:{lease.owner_pid}:{lease.owner_id}", flush=True)
sys.stdin.buffer.read(1)
lease.release()
"""


def _scope(tmp_path: Path, name: str = "capture") -> Path:
    scope = tmp_path / name
    scope.mkdir()
    return scope


def _spawn_holder(scope: Path) -> subprocess.Popen[str]:
    environment = os.environ.copy()
    source_path = os.fspath(_PROJECT_ROOT / "src")
    existing_python_path = environment.get("PYTHONPATH")
    environment["PYTHONPATH"] = (
        source_path
        if not existing_python_path
        else os.pathsep.join((source_path, existing_python_path))
    )
    process = subprocess.Popen(
        [sys.executable, "-c", _HOLDER_SCRIPT, os.fspath(scope)],
        cwd=_PROJECT_ROOT,
        env=environment,
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    assert process.stdout is not None
    ready = process.stdout.readline().strip()
    if not ready.startswith("READY:"):
        _, stderr = process.communicate(timeout=5)
        raise AssertionError(f"holder failed to start: {ready!r} {stderr!r}")
    return process


def _stop_holder(process: subprocess.Popen[str]) -> None:
    assert process.stdin is not None
    process.stdin.write("x")
    process.stdin.flush()
    stdout, stderr = process.communicate(timeout=10)
    assert process.returncode == 0, (stdout, stderr)


@pytest.fixture
def held_lease(tmp_path: Path) -> Iterator[WriterLease]:
    lease = WriterLease.acquire(_scope(tmp_path))
    try:
        yield lease
    finally:
        try:
            lease.assert_held()
        except WriterLeaseNotHeldError:
            return
        lease.release()


def test_same_process_duplicate_is_rejected_and_release_preserves_file(tmp_path: Path) -> None:
    scope = _scope(tmp_path)
    first = WriterLease.acquire(scope)
    try:
        with pytest.raises(WriterLeaseContendedError, match="already held"):
            WriterLease.acquire(scope)
        first.assert_held()
    finally:
        first.release()

    lock_path = scope / WRITER_LEASE_FILE_NAME
    assert lock_path.is_file()
    second = WriterLease.acquire(scope)
    try:
        assert second.owner_pid == os.getpid()
        assert second.owner_id != first.owner_id
    finally:
        second.release()
    assert lock_path.is_file()


def test_owner_metadata_is_bounded_durable_and_not_a_stale_pid_decision(tmp_path: Path) -> None:
    scope = _scope(tmp_path)
    lock_path = scope / WRITER_LEASE_FILE_NAME
    stale = {
        "owner_pid": os.getpid(),
        "owner_id": "stale-owner-metadata",
        "acquired_wall_ms": 1,
    }
    lock_path.write_text(json.dumps(stale), encoding="utf-8")

    lease = WriterLease.acquire(scope)
    owner_id = lease.owner_id
    lease.release()

    payload = lock_path.read_bytes()
    document = json.loads(payload)
    assert len(payload) <= MAX_WRITER_LEASE_METADATA_BYTES
    assert document == {
        "acquired_monotonic_ns": lease.acquired_monotonic_ns,
        "acquired_wall_ms": lease.acquired_wall_ms,
        "backend": lease.backend,
        "notice": "diagnostic_only_not_ownership_proof",
        "owner_id": owner_id,
        "owner_pid": os.getpid(),
        "schema_version": "signalbot_writer_lease_metadata_v1",
    }
    assert lease.acquired_wall_ms > 0
    assert lease.acquired_monotonic_ns > 0
    expected_backend = "WINDOWS_LOCKFILEEX" if os.name == "nt" else "POSIX_FLOCK"
    assert lease.backend == expected_backend
    with pytest.raises(AttributeError):
        lease.backend = expected_backend  # type: ignore[misc]


def test_existing_hard_link_rejects_acquisition_without_overwriting_bytes(
    tmp_path: Path,
) -> None:
    scope = _scope(tmp_path)
    lock_path = scope / WRITER_LEASE_FILE_NAME
    sentinel = b"sentinel-owner-metadata-must-survive\n"
    lock_path.write_bytes(sentinel)
    hard_link = tmp_path / "writer-lock-hard-link"
    try:
        os.link(lock_path, hard_link)
    except OSError:
        pytest.skip("creating hard links is not permitted on this host")

    with pytest.raises(WriterLeaseError, match="exactly one hard link"):
        WriterLease.acquire(scope)

    assert lock_path.read_bytes() == sentinel
    assert hard_link.read_bytes() == sentinel


def test_distinct_scope_roots_can_be_held_together(tmp_path: Path) -> None:
    first = WriterLease.acquire(_scope(tmp_path, "first"))
    second = WriterLease.acquire(_scope(tmp_path, "second"))
    try:
        first.assert_held()
        second.assert_held()
    finally:
        second.release()
        first.release()


def test_bounded_wait_times_out_and_successfully_polls_after_release(tmp_path: Path) -> None:
    scope = _scope(tmp_path)
    first = WriterLease.acquire(scope)
    started = time.monotonic()
    with pytest.raises(WriterLeaseContendedError):
        WriterLease.acquire(scope, wait_timeout_ms=80, poll_interval_ms=10)
    elapsed = time.monotonic() - started
    assert elapsed >= 0.06
    assert elapsed < 2.0

    release_thread = threading.Thread(target=lambda: (time.sleep(0.05), first.release()))
    release_thread.start()
    second = WriterLease.acquire(scope, wait_timeout_ms=1_000, poll_interval_ms=10)
    release_thread.join(timeout=2)
    assert not release_thread.is_alive()
    second.release()


def test_bounded_wait_honors_cancellation(tmp_path: Path) -> None:
    scope = _scope(tmp_path)
    first = WriterLease.acquire(scope)
    checks = 0

    def cancelled() -> bool:
        nonlocal checks
        checks += 1
        return checks >= 3

    try:
        started = time.monotonic()
        with pytest.raises(WriterLeaseCancelledError, match="cancelled"):
            WriterLease.acquire(
                scope,
                wait_timeout_ms=1_000,
                poll_interval_ms=10,
                cancelled=cancelled,
            )
        assert time.monotonic() - started < 0.5
    finally:
        first.release()


@pytest.mark.parametrize(
    ("wait_timeout_ms", "poll_interval_ms"),
    [
        (-1, 10),
        (True, 10),
        (1.5, 10),
        ("10", 10),
        (0, 0),
        (0, 1_001),
        (0, True),
        (0, 1.5),
        (0, "10"),
    ],
)
def test_invalid_wait_bounds_are_rejected(
    tmp_path: Path,
    wait_timeout_ms: object,
    poll_interval_ms: object,
) -> None:
    with pytest.raises(ValueError):
        WriterLease.acquire(
            _scope(tmp_path),
            wait_timeout_ms=cast(int, wait_timeout_ms),
            poll_interval_ms=cast(int, poll_interval_ms),
        )


def test_subprocess_contention_then_clean_release(tmp_path: Path) -> None:
    scope = _scope(tmp_path)
    holder = _spawn_holder(scope)
    try:
        with pytest.raises(WriterLeaseContendedError):
            WriterLease.acquire(scope)
        _stop_holder(holder)
    finally:
        if holder.poll() is None:
            holder.kill()
            holder.wait(timeout=10)

    lease = WriterLease.acquire(scope)
    lease.release()


def test_subprocess_crash_releases_os_lock_without_deleting_file(tmp_path: Path) -> None:
    scope = _scope(tmp_path)
    holder = _spawn_holder(scope)
    lock_path = scope / WRITER_LEASE_FILE_NAME
    assert lock_path.exists()
    holder.kill()
    holder.wait(timeout=10)
    assert lock_path.is_file()

    lease = WriterLease.acquire(scope, wait_timeout_ms=1_000)
    lease.release()
    assert lock_path.is_file()


def test_assert_held_rejects_wrong_pid_and_double_release(
    held_lease: WriterLease,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    actual_pid = os.getpid()
    monkeypatch.setattr(writer_lease_module.os, "getpid", lambda: actual_pid + 1)
    with pytest.raises(WriterLeaseNotHeldError, match="different process"):
        held_lease.assert_held()
    monkeypatch.setattr(writer_lease_module.os, "getpid", lambda: actual_pid)
    held_lease.release()
    with pytest.raises(WriterLeaseNotHeldError, match="released"):
        held_lease.release()


def test_concurrent_release_unlocks_and_closes_each_handle_once(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    lease = WriterLease.acquire(_scope(tmp_path))
    expected_handles = {lease._lock_handle, lease._scope_handle}
    original_unlock = writer_lease_module._unlock
    original_close_handle = writer_lease_module._close_handle
    unlock_entered = threading.Event()
    allow_unlock = threading.Event()
    calls_lock = threading.Lock()
    unlock_calls: list[int] = []
    close_calls: list[int] = []

    def counted_unlock(handle: int) -> None:
        with calls_lock:
            unlock_calls.append(handle)
        unlock_entered.set()
        assert allow_unlock.wait(timeout=2)
        original_unlock(handle)

    def counted_close(handle: int) -> None:
        with calls_lock:
            close_calls.append(handle)
        original_close_handle(handle)

    monkeypatch.setattr(writer_lease_module, "_unlock", counted_unlock)
    monkeypatch.setattr(writer_lease_module, "_close_handle", counted_close)
    outcomes: list[BaseException | None] = []

    def release() -> None:
        try:
            lease.release()
        except BaseException as exc:
            outcomes.append(exc)
        else:
            outcomes.append(None)

    first = threading.Thread(target=release)
    second = threading.Thread(target=release)
    first.start()
    assert unlock_entered.wait(timeout=2)
    second.start()
    allow_unlock.set()
    first.join(timeout=2)
    second.join(timeout=2)

    assert not first.is_alive()
    assert not second.is_alive()
    assert outcomes.count(None) == 1
    errors = [outcome for outcome in outcomes if outcome is not None]
    assert len(errors) == 1
    assert isinstance(errors[0], WriterLeaseNotHeldError)
    assert unlock_calls == [lease._lock_handle]
    assert len(close_calls) == 2
    assert set(close_calls) == expected_handles


def test_context_manager_asserts_and_releases(tmp_path: Path) -> None:
    scope = _scope(tmp_path)
    with WriterLease.acquire(scope) as lease:
        lease.assert_held()
    with pytest.raises(WriterLeaseNotHeldError, match="released"):
        lease.assert_held()


def test_session_closure_claim_seals_and_asserts_exact_identity(
    held_lease: WriterLease,
    tmp_path: Path,
) -> None:
    canonical_path = os.fspath((tmp_path / "session-closure.json").resolve())
    manifest_sha256 = "a" * 64

    with held_lease.operation_guard():
        held_lease.claim_session_closure_authority(
            canonical_path=canonical_path,
        )
        held_lease.seal_session_closure_authority(
            canonical_path=canonical_path,
            manifest_sha256=manifest_sha256,
            byte_count=101,
            file_device=7,
            file_inode=11,
            file_nlink=1,
        )
        held_lease.assert_session_closure_authority_claim(
            canonical_path=canonical_path,
            manifest_sha256=manifest_sha256,
            byte_count=101,
            file_device=7,
            file_inode=11,
            file_nlink=1,
        )


def test_session_start_and_closure_claims_are_independent_per_acquisition(
    held_lease: WriterLease,
    tmp_path: Path,
) -> None:
    start_path = os.fspath((tmp_path / "session-start.json").resolve())
    closure_path = os.fspath((tmp_path / "session-closure.json").resolve())

    with held_lease.operation_guard():
        held_lease.claim_session_closure_authority(canonical_path=closure_path)
        held_lease.claim_session_start_authority(canonical_path=start_path)
        held_lease.seal_session_closure_authority(
            canonical_path=closure_path,
            manifest_sha256="c" * 64,
            byte_count=103,
            file_device=13,
            file_inode=17,
            file_nlink=1,
        )
        held_lease.seal_session_start_authority(
            canonical_path=start_path,
            manifest_sha256="b" * 64,
            byte_count=107,
            file_device=19,
            file_inode=23,
            file_nlink=1,
        )
        held_lease.assert_session_closure_authority_claim(
            canonical_path=closure_path,
            manifest_sha256="c" * 64,
            byte_count=103,
            file_device=13,
            file_inode=17,
            file_nlink=1,
        )
        held_lease.assert_session_start_authority_claim(
            canonical_path=start_path,
            manifest_sha256="b" * 64,
            byte_count=107,
            file_device=19,
            file_inode=23,
            file_nlink=1,
        )
        with pytest.raises(WriterLeaseSessionClosureClaimError, match="consumed"):
            held_lease.claim_session_closure_authority(canonical_path=closure_path)
        with pytest.raises(WriterLeaseSessionStartClaimError, match="consumed"):
            held_lease.claim_session_start_authority(canonical_path=start_path)


def test_session_closure_lifecycle_requires_exact_lease_and_operation_guard(
    held_lease: WriterLease,
    tmp_path: Path,
) -> None:
    canonical_path = os.fspath((tmp_path / "session-closure.json").resolve())

    class DerivedWriterLease(WriterLease):
        pass

    derived = object.__new__(DerivedWriterLease)
    with pytest.raises(TypeError, match="exact WriterLease"):
        derived.claim_session_closure_authority(canonical_path=canonical_path)
    with pytest.raises(TypeError, match="exact WriterLease"):
        derived.seal_session_closure_authority(
            canonical_path=canonical_path,
            manifest_sha256="a" * 64,
            byte_count=1,
            file_device=1,
            file_inode=1,
            file_nlink=1,
        )
    with pytest.raises(TypeError, match="exact WriterLease"):
        derived.assert_session_closure_authority_claim(
            canonical_path=canonical_path,
            manifest_sha256="a" * 64,
            byte_count=1,
            file_device=1,
            file_inode=1,
            file_nlink=1,
        )

    with pytest.raises(WriterLeaseSessionClosureClaimError, match="active"):
        held_lease.claim_session_closure_authority(canonical_path=canonical_path)
    with held_lease.operation_guard():
        held_lease.claim_session_closure_authority(canonical_path=canonical_path)
    with pytest.raises(WriterLeaseSessionClosureClaimError, match="active"):
        held_lease.seal_session_closure_authority(
            canonical_path=canonical_path,
            manifest_sha256="a" * 64,
            byte_count=1,
            file_device=1,
            file_inode=1,
            file_nlink=1,
        )
    with pytest.raises(WriterLeaseSessionClosureClaimError, match="active"):
        held_lease.assert_session_closure_authority_claim(
            canonical_path=canonical_path,
            manifest_sha256="a" * 64,
            byte_count=1,
            file_device=1,
            file_inode=1,
            file_nlink=1,
        )


def test_session_closure_claim_rejects_path_drift_unsealed_assertion_and_duplicate_seal(
    held_lease: WriterLease,
    tmp_path: Path,
) -> None:
    canonical_path = os.fspath((tmp_path / "session-closure.json").resolve())
    alternate_path = os.fspath((tmp_path / "alternate-closure.json").resolve())

    with held_lease.operation_guard():
        held_lease.claim_session_closure_authority(canonical_path=canonical_path)
        with pytest.raises(
            WriterLeaseSessionClosureClaimError,
            match="not sealed",
        ):
            held_lease.assert_session_closure_authority_claim(
                canonical_path=canonical_path,
                manifest_sha256="a" * 64,
                byte_count=109,
                file_device=29,
                file_inode=31,
                file_nlink=1,
            )
        with pytest.raises(
            WriterLeaseSessionClosureClaimError,
            match="path differs",
        ):
            held_lease.seal_session_closure_authority(
                canonical_path=alternate_path,
                manifest_sha256="a" * 64,
                byte_count=109,
                file_device=29,
                file_inode=31,
                file_nlink=1,
            )
        held_lease.seal_session_closure_authority(
            canonical_path=canonical_path,
            manifest_sha256="a" * 64,
            byte_count=109,
            file_device=29,
            file_inode=31,
            file_nlink=1,
        )
        with pytest.raises(
            WriterLeaseSessionClosureClaimError,
            match="already sealed",
        ):
            held_lease.seal_session_closure_authority(
                canonical_path=canonical_path,
                manifest_sha256="a" * 64,
                byte_count=109,
                file_device=29,
                file_inode=31,
                file_nlink=1,
            )


@pytest.mark.parametrize(
    ("manifest_sha256", "byte_count", "file_device", "file_inode", "file_nlink"),
    [
        ("A" * 64, 1, 1, 1, 1),
        ("a" * 64, 0, 1, 1, 1),
        ("a" * 64, 1, -1, 1, 1),
        ("a" * 64, 1, 1, -1, 1),
        ("a" * 64, 1, 1, 1, 2),
    ],
)
def test_failed_session_closure_seal_keeps_sole_claim_consumed(
    held_lease: WriterLease,
    tmp_path: Path,
    manifest_sha256: str,
    byte_count: int,
    file_device: int,
    file_inode: int,
    file_nlink: int,
) -> None:
    canonical_path = os.fspath((tmp_path / "session-closure.json").resolve())

    with held_lease.operation_guard():
        held_lease.claim_session_closure_authority(canonical_path=canonical_path)
        with pytest.raises(ValueError, match="session-closure"):
            held_lease.seal_session_closure_authority(
                canonical_path=canonical_path,
                manifest_sha256=manifest_sha256,
                byte_count=byte_count,
                file_device=file_device,
                file_inode=file_inode,
                file_nlink=file_nlink,
            )
        with pytest.raises(WriterLeaseSessionClosureClaimError, match="consumed"):
            held_lease.claim_session_closure_authority(canonical_path=canonical_path)


@pytest.mark.parametrize(
    ("manifest_sha256", "byte_count", "file_device", "file_inode", "file_nlink"),
    [
        ("b" * 64, 101, 7, 11, 1),
        ("a" * 64, 102, 7, 11, 1),
        ("a" * 64, 101, 8, 11, 1),
        ("a" * 64, 101, 7, 12, 1),
        ("a" * 64, 101, 7, 11, 2),
    ],
)
def test_session_closure_assertion_binds_every_persisted_identity_field(
    held_lease: WriterLease,
    tmp_path: Path,
    manifest_sha256: str,
    byte_count: int,
    file_device: int,
    file_inode: int,
    file_nlink: int,
) -> None:
    canonical_path = os.fspath((tmp_path / "session-closure.json").resolve())

    with held_lease.operation_guard():
        held_lease.claim_session_closure_authority(canonical_path=canonical_path)
        held_lease.seal_session_closure_authority(
            canonical_path=canonical_path,
            manifest_sha256="a" * 64,
            byte_count=101,
            file_device=7,
            file_inode=11,
            file_nlink=1,
        )
        with pytest.raises(WriterLeaseSessionClosureClaimError, match="differs"):
            held_lease.assert_session_closure_authority_claim(
                canonical_path=canonical_path,
                manifest_sha256=manifest_sha256,
                byte_count=byte_count,
                file_device=file_device,
                file_inode=file_inode,
                file_nlink=file_nlink,
            )


def test_released_lease_rejects_session_closure_lifecycle(
    held_lease: WriterLease,
    tmp_path: Path,
) -> None:
    canonical_path = os.fspath((tmp_path / "session-closure.json").resolve())
    held_lease.release()

    with pytest.raises(WriterLeaseNotHeldError, match="released"):
        held_lease.claim_session_closure_authority(canonical_path=canonical_path)
    with pytest.raises(WriterLeaseNotHeldError, match="released"):
        held_lease.seal_session_closure_authority(
            canonical_path=canonical_path,
            manifest_sha256="a" * 64,
            byte_count=1,
            file_device=1,
            file_inode=1,
            file_nlink=1,
        )
    with pytest.raises(WriterLeaseNotHeldError, match="released"):
        held_lease.assert_session_closure_authority_claim(
            canonical_path=canonical_path,
            manifest_sha256="a" * 64,
            byte_count=1,
            file_device=1,
            file_inode=1,
            file_nlink=1,
        )


def test_scope_must_exist_and_be_a_directory(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="does not exist"):
        WriterLease.acquire(tmp_path / "missing")
    ordinary_file = tmp_path / "ordinary"
    ordinary_file.write_text("not a scope", encoding="utf-8")
    with pytest.raises(ValueError, match="existing directory"):
        WriterLease.acquire(ordinary_file)


def test_existing_lock_path_must_be_a_regular_file(tmp_path: Path) -> None:
    scope = _scope(tmp_path)
    (scope / WRITER_LEASE_FILE_NAME).mkdir()
    with pytest.raises(ValueError, match="regular file"):
        WriterLease.acquire(scope)


def test_symlinked_scope_and_lock_path_are_rejected_when_supported(tmp_path: Path) -> None:
    real_scope = _scope(tmp_path, "real")
    linked_scope = tmp_path / "linked"
    try:
        linked_scope.symlink_to(real_scope, target_is_directory=True)
    except OSError:
        pytest.skip("creating directory symlinks is not permitted on this host")
    with pytest.raises(ValueError, match=r"symbolic-link|reparse-point"):
        WriterLease.acquire(linked_scope)

    outside_file = tmp_path / "outside-lock"
    outside_file.write_text("outside", encoding="utf-8")
    lock_link = real_scope / WRITER_LEASE_FILE_NAME
    try:
        lock_link.symlink_to(outside_file)
    except OSError:
        pytest.skip("creating file symlinks is not permitted on this host")
    with pytest.raises(ValueError, match=r"symbolic-link|reparse-point"):
        WriterLease.acquire(real_scope)


@pytest.mark.skipif(os.name == "nt", reason="Windows denies pathname replacement while held")
def test_assert_held_detects_lock_path_replacement(tmp_path: Path) -> None:
    scope = _scope(tmp_path)
    lease = WriterLease.acquire(scope)
    lock_path = lease.lock_path
    moved_path = scope / "moved-lock"
    lock_path.rename(moved_path)
    lock_path.write_text("replacement", encoding="utf-8")
    try:
        with pytest.raises(WriterLeaseNotHeldError, match="changed pathname identity"):
            lease.assert_held()
    finally:
        lock_path.unlink()
        moved_path.rename(lock_path)
    lease.assert_held()
    lease.release()


@pytest.mark.skipif(os.name == "nt", reason="Windows denies scope replacement while held")
def test_assert_held_detects_scope_path_replacement(tmp_path: Path) -> None:
    scope = _scope(tmp_path)
    lease = WriterLease.acquire(scope)
    moved_scope = tmp_path / "moved-scope"
    scope.rename(moved_scope)
    scope.mkdir()
    try:
        with pytest.raises(
            WriterLeaseNotHeldError,
            match=r"scope_root changed|pathname is no longer link-free",
        ):
            lease.assert_held()
    finally:
        scope.rmdir()
        moved_scope.rename(scope)
    lease.assert_held()
    lease.release()


def test_assert_held_detects_hard_link_added_after_acquisition(tmp_path: Path) -> None:
    scope = _scope(tmp_path)
    lease = WriterLease.acquire(scope)
    hard_link = tmp_path / "held-writer-lock-hard-link"
    try:
        os.link(lease.lock_path, hard_link)
    except OSError:
        lease.release()
        pytest.skip("creating hard links is not permitted on this host")

    try:
        with pytest.raises(WriterLeaseNotHeldError, match="exactly one hard link"):
            lease.assert_held()
    finally:
        hard_link.unlink()
    lease.assert_held()
    lease.release()


@pytest.mark.skipif(os.name != "nt", reason="Windows sharing semantics test")
def test_windows_handles_deny_pathname_replacement_while_held(tmp_path: Path) -> None:
    scope = _scope(tmp_path)
    lease = WriterLease.acquire(scope)
    try:
        with pytest.raises(OSError):
            lease.lock_path.rename(scope / "moved-lock")
        with pytest.raises(OSError):
            scope.rename(tmp_path / "moved-scope")
        lease.assert_held()
    finally:
        lease.release()


@pytest.mark.skipif(os.name == "nt", reason="os.fork is POSIX-only")
def test_at_fork_child_invalidates_inherited_handle(tmp_path: Path) -> None:
    lease = WriterLease.acquire(_scope(tmp_path))
    read_fd, write_fd = os.pipe()
    fork = getattr(os, "fork", None)
    assert fork is not None
    child_pid = fork()
    if child_pid == 0:
        os.close(read_fd)
        try:
            lease.assert_held()
        except WriterLeaseNotHeldError:
            result = b"invalidated"
        else:
            result = b"unexpectedly-held"
        os.write(write_fd, result)
        os.close(write_fd)
        os._exit(0)

    os.close(write_fd)
    try:
        result = os.read(read_fd, 64)
        _, status = os.waitpid(child_pid, 0)
        assert os.waitstatus_to_exitcode(status) == 0
        assert result == b"invalidated"
        lease.assert_held()
    finally:
        os.close(read_fd)
        lease.release()
