from __future__ import annotations

import copy
import inspect
import json
import os
import pickle
import struct
import threading
from pathlib import Path
from typing import cast

import pytest

from signalbot.backtest import d1_scefb_historical_attempt_wal as subject
from signalbot.r4b_v2.canonical import canonical_json_line


def _sha(character: str) -> str:
    return character * 64


def _bindings() -> subject.D1AttemptWalBindingsV0:
    return subject.D1AttemptWalBindingsV0(
        run_id="d1-unit-run-001",
        code_freeze_manifest_sha256=_sha("1"),
        input_authority_sha256=_sha("2"),
        input_authority_file_sha256=_sha("3"),
        funding_authority_file_sha256=_sha("4"),
        preregistration_sha256=_sha("5"),
        output_path_sha256=_sha("6"),
    )


def _arm(tmp_path: Path) -> subject.D1AttemptWalSnapshotV0:
    return subject.create_armed_wal_v0(
        attempt_dir=(tmp_path / "attempt").resolve(),
        bindings=_bindings(),
        armed_at_ms=1_000,
    )


def _started(tmp_path: Path) -> subject.D1AttemptWalSnapshotV0:
    armed = _arm(tmp_path)
    return subject.append_started_v0(
        attempt_dir=armed.attempt_dir,
        expected_prefix=armed.prefix,
        started_at_ms=2_000,
    ).snapshot


def _append_raw(path: Path, raw: bytes) -> None:
    with path.open("ab", buffering=0) as handle:
        handle.write(raw)
        handle.flush()
        os.fsync(handle.fileno())


def test_arm_is_value_only_and_creates_one_fixed_canonical_frame(tmp_path: Path) -> None:
    assert set(inspect.signature(subject.create_armed_wal_v0).parameters) == {
        "attempt_dir",
        "bindings",
        "armed_at_ms",
    }
    snapshot = _arm(tmp_path)

    assert snapshot.last_state == "ARMED"
    assert snapshot.torn_tail is None
    assert snapshot.prefix.record_count == 1
    assert snapshot.bindings == _bindings()
    assert {path.name for path in snapshot.attempt_dir.iterdir()} == {
        subject.D1_ATTEMPT_WAL_FILE_V0
    }
    record = snapshot.records[0]
    assert record.sequence == 0
    assert record.previous_record_sha256 is None
    assert record.production_order_placement is False

    raw = snapshot.wal_path.read_bytes()
    magic, length = struct.unpack(">4sI", raw[:8])
    payload = raw[8:]
    assert magic == b"D1W0"
    assert length == len(payload)
    assert payload.endswith(b"\n") and payload.count(b"\n") == 1
    assert canonical_json_line(json.loads(payload)) == payload


def test_threat_model_explicitly_excludes_active_durable_file_restore() -> None:
    assert "ACTIVE_DURABLE_FILE_RESTORE" in subject.D1_ATTEMPT_WAL_THREAT_MODEL_V0
    assert "PRIVILEGED_FULL_SNAPSHOT" in (
        subject.D1_ATTEMPT_WAL_THREAT_MODEL_V0
    )
    assert "TRUSTED_LOCAL_FILESYSTEM_AND_PROCESS_CODE" in (
        subject.D1_ATTEMPT_WAL_THREAT_MODEL_V0
    )
    assert "PUBLIC_API_SEALED" in subject.D1_ATTEMPT_WAL_THREAT_MODEL_V0
    assert "IN_PROCESS_REFLECTION_ADVERSARIAL_MUTATION" in (
        subject.D1_ATTEMPT_WAL_THREAT_MODEL_V0
    )
    assert "DIRECT_RUNNER_INVOCATION_EXCLUDED" in (
        subject.D1_ATTEMPT_WAL_THREAT_MODEL_V0
    )


def test_arm_rejects_every_existing_target_without_deleting_it(tmp_path: Path) -> None:
    target = (tmp_path / "attempt").resolve()
    target.mkdir()
    marker = target / "owner.txt"
    marker.write_text("preserve", encoding="utf-8")

    with pytest.raises(subject.D1HistoricalAttemptWalStateErrorV0, match="already exists"):
        subject.create_armed_wal_v0(
            attempt_dir=target,
            bindings=_bindings(),
            armed_at_ms=1,
        )
    assert marker.read_text(encoding="utf-8") == "preserve"


def test_start_returns_only_with_exact_canonical_seal_and_fixed_membership(
    tmp_path: Path,
) -> None:
    armed = _arm(tmp_path)
    start_result = subject.append_started_v0(
        attempt_dir=armed.attempt_dir,
        expected_prefix=armed.prefix,
        started_at_ms=2_000,
    )
    started = start_result.snapshot

    assert started.last_state == "STARTED_BEFORE_OUTCOME_ACCESS"
    assert started.start_seal_valid
    assert not start_result.outcome_access_grant.consumed
    assert start_result.outcome_access_grant.start_record_sha256 == (
        started.records[1].record_sha256
    )
    assert start_result.outcome_access_grant.start_prefix == started.prefix
    assert start_result.outcome_access_grant.bindings == started.bindings
    assert start_result.outcome_access_grant.attempt_directory_sha256 == (
        started.records[1].attempt_directory_sha256
    )
    assert started.start_seal is not None
    assert not started.start_seal_torn
    assert started.start_seal.start_record_sha256 == started.records[1].record_sha256
    assert started.start_seal.bindings_sha256 == started.bindings.bindings_sha256
    assert started.start_seal.attempt_directory_sha256 == (
        started.records[0].attempt_directory_sha256
    )
    assert {path.name for path in started.attempt_dir.iterdir()} == {
        subject.D1_ATTEMPT_WAL_FILE_V0,
        subject.D1_ATTEMPT_START_SEAL_FILE_V0,
    }
    seal_raw = (started.attempt_dir / subject.D1_ATTEMPT_START_SEAL_FILE_V0).read_bytes()
    assert canonical_json_line(json.loads(seal_raw)) == seal_raw
    loaded = subject.load_attempt_wal_v0(started.attempt_dir)
    assert loaded.start_seal == started.start_seal
    assert loaded.start_seal_valid
    assert not hasattr(loaded, "outcome_access_grant")


def test_outcome_access_grant_is_factory_sealed_nonserializable_and_one_use(
    tmp_path: Path,
) -> None:
    armed = _arm(tmp_path)
    start_result = subject.append_started_v0(
        attempt_dir=armed.attempt_dir,
        expected_prefix=armed.prefix,
        started_at_ms=2_000,
    )
    grant = start_result.outcome_access_grant

    with pytest.raises(TypeError, match="factory-sealed"):
        subject.D1OutcomeAccessGrantV0()
    with pytest.raises(TypeError, match="subclassed"):
        type("_ForgedGrant", (subject.D1OutcomeAccessGrantV0,), {})

    consumed_attribute = "_consumed"
    with pytest.raises(TypeError, match="immutable"):
        setattr(grant, consumed_attribute, False)
    with pytest.raises(TypeError, match="copied"):
        copy.copy(grant)
    with pytest.raises(TypeError, match="deep-copied"):
        copy.deepcopy(grant)
    with pytest.raises(TypeError, match="serialized"):
        pickle.dumps(grant)

    callback_calls = 0

    def callback() -> str:
        nonlocal callback_calls
        callback_calls += 1
        return "entered"

    assert grant.consume_once_v0(callback) == "entered"
    assert grant.consumed
    with pytest.raises(subject.D1HistoricalAttemptWalStateErrorV0, match="already consumed"):
        grant.consume_once_v0(callback)
    assert callback_calls == 1


def test_outcome_callback_error_consumes_grant_before_entry_and_never_reenters(
    tmp_path: Path,
) -> None:
    armed = _arm(tmp_path)
    grant = subject.append_started_v0(
        attempt_dir=armed.attempt_dir,
        expected_prefix=armed.prefix,
        started_at_ms=2_000,
    ).outcome_access_grant
    callback_calls = 0

    def crashing_callback() -> None:
        nonlocal callback_calls
        callback_calls += 1
        raise RuntimeError("injected outcome crash")

    with pytest.raises(RuntimeError, match="outcome crash"):
        grant.consume_once_v0(crashing_callback)
    assert grant.consumed
    with pytest.raises(subject.D1HistoricalAttemptWalStateErrorV0, match="already consumed"):
        grant.consume_once_v0(crashing_callback)
    assert callback_calls == 1


def test_foreign_process_id_is_rejected_before_lock_and_consumes_local_copy(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    armed = _arm(tmp_path)
    grant = subject.append_started_v0(
        attempt_dir=armed.attempt_dir,
        expected_prefix=armed.prefix,
        started_at_ms=2_000,
    ).outcome_access_grant
    callback_calls = 0

    def forbidden_callback() -> None:
        nonlocal callback_calls
        callback_calls += 1

    monkeypatch.setattr(subject.os, "getpid", lambda: grant.mint_process_id + 1)
    with pytest.raises(subject.D1HistoricalAttemptWalStateErrorV0, match="another process"):
        grant.consume_once_v0(forbidden_callback)
    assert grant._consumed
    assert callback_calls == 0


def test_concurrent_consumers_enter_one_outcome_callback_at_most_once(
    tmp_path: Path,
) -> None:
    armed = _arm(tmp_path)
    grant = subject.append_started_v0(
        attempt_dir=armed.attempt_dir,
        expected_prefix=armed.prefix,
        started_at_ms=2_000,
    ).outcome_access_grant
    callback_entered = threading.Event()
    callback_release = threading.Event()
    callback_calls = 0
    outcomes: list[str] = []
    failures: list[Exception] = []

    def callback() -> str:
        nonlocal callback_calls
        callback_calls += 1
        callback_entered.set()
        if not callback_release.wait(timeout=5):
            raise RuntimeError("test callback release timed out")
        return "entered"

    def consume() -> None:
        try:
            outcomes.append(grant.consume_once_v0(callback))
        except Exception as error:
            failures.append(error)

    first = threading.Thread(target=consume, daemon=True)
    second = threading.Thread(target=consume, daemon=True)
    first.start()
    assert callback_entered.wait(timeout=5)
    second.start()
    second.join(timeout=5)
    callback_release.set()
    first.join(timeout=5)

    assert not first.is_alive() and not second.is_alive()
    assert outcomes == ["entered"]
    assert callback_calls == 1
    assert len(failures) == 1
    assert isinstance(failures[0], subject.D1HistoricalAttemptWalStateErrorV0)


def test_start_and_all_direct_terminal_transitions(tmp_path: Path) -> None:
    for index, state in enumerate(("COMPLETED", "FAILED", "AMBIGUOUS_OUTPUT")):
        case = tmp_path / str(index)
        case.mkdir()
        started = _started(case)
        kwargs: dict[str, str | None] = {
            "detail_code": None,
            "result_sha256": None,
            "artifact_manifest_sha256": None,
        }
        if state == "COMPLETED":
            kwargs["result_sha256"] = _sha("a")
            kwargs["artifact_manifest_sha256"] = _sha("b")
        else:
            kwargs["detail_code"] = (
                "RUN_FAILED_OUTPUT_ABSENT"
                if state == "FAILED"
                else "OUTPUT_COMMIT_UNCERTAIN"
            )
        terminal = subject.append_terminal_v0(
            attempt_dir=started.attempt_dir,
            expected_prefix=started.prefix,
            state=cast(subject.D1AttemptWalTerminalStateV0, state),
            terminal_at_ms=3_000,
            **kwargs,
        )
        assert not hasattr(terminal, "outcome_access_grant")
        assert terminal.last_state == state
        assert [record.sequence for record in terminal.records] == [0, 1, 2]
        assert terminal.records[1].previous_record_sha256 == terminal.records[0].record_sha256
        assert terminal.records[2].previous_record_sha256 == terminal.records[1].record_sha256


def test_completed_can_be_overridden_once_by_hash_preserving_ambiguity(tmp_path: Path) -> None:
    started = _started(tmp_path)
    completed = subject.append_terminal_v0(
        attempt_dir=started.attempt_dir,
        expected_prefix=started.prefix,
        state="COMPLETED",
        terminal_at_ms=3_000,
        result_sha256=_sha("a"),
        artifact_manifest_sha256=_sha("b"),
    )
    ambiguous = subject.append_terminal_v0(
        attempt_dir=completed.attempt_dir,
        expected_prefix=completed.prefix,
        state="AMBIGUOUS_OUTPUT",
        terminal_at_ms=4_000,
        detail_code="POST_WRITE_VERIFICATION_FAILED",
        result_sha256=_sha("a"),
        artifact_manifest_sha256=_sha("b"),
    )

    assert ambiguous.last_state == "AMBIGUOUS_OUTPUT"
    assert ambiguous.prefix.record_count == subject.D1_ATTEMPT_WAL_MAX_RECORDS_V0
    with pytest.raises(subject.D1HistoricalAttemptWalStateErrorV0, match="record cap"):
        subject.append_terminal_v0(
            attempt_dir=ambiguous.attempt_dir,
            expected_prefix=ambiguous.prefix,
            state="AMBIGUOUS_OUTPUT",
            terminal_at_ms=5_000,
            detail_code="SECOND_OVERRIDE_FORBIDDEN",
            result_sha256=_sha("a"),
            artifact_manifest_sha256=_sha("b"),
        )


def test_completed_override_must_preserve_artifact_hashes(tmp_path: Path) -> None:
    started = _started(tmp_path)
    completed = subject.append_terminal_v0(
        attempt_dir=started.attempt_dir,
        expected_prefix=started.prefix,
        state="COMPLETED",
        terminal_at_ms=3_000,
        result_sha256=_sha("a"),
        artifact_manifest_sha256=_sha("b"),
    )

    with pytest.raises(subject.D1HistoricalAttemptWalStateErrorV0, match="preserve"):
        subject.append_terminal_v0(
            attempt_dir=completed.attempt_dir,
            expected_prefix=completed.prefix,
            state="AMBIGUOUS_OUTPUT",
            terminal_at_ms=4_000,
            detail_code="POST_WRITE_VERIFICATION_FAILED",
            result_sha256=_sha("c"),
            artifact_manifest_sha256=_sha("b"),
        )
    assert subject.load_attempt_wal_v0(completed.attempt_dir).last_state == "COMPLETED"


def test_second_start_and_terminal_before_start_are_rejected(tmp_path: Path) -> None:
    armed = _arm(tmp_path)
    with pytest.raises(subject.D1HistoricalAttemptWalStateErrorV0, match="ARMED->FAILED"):
        subject.append_terminal_v0(
            attempt_dir=armed.attempt_dir,
            expected_prefix=armed.prefix,
            state="FAILED",
            terminal_at_ms=2_000,
            detail_code="RUN_FAILED_OUTPUT_ABSENT",
        )
    started = subject.append_started_v0(
        attempt_dir=armed.attempt_dir,
        expected_prefix=armed.prefix,
        started_at_ms=2_000,
    ).snapshot
    with pytest.raises(subject.D1HistoricalAttemptWalStateErrorV0, match=r"STARTED.*STARTED"):
        subject.append_started_v0(
            attempt_dir=started.attempt_dir,
            expected_prefix=started.prefix,
            started_at_ms=3_000,
        )


def test_stale_expected_prefix_loses_without_writing(tmp_path: Path) -> None:
    armed = _arm(tmp_path)
    started = subject.append_started_v0(
        attempt_dir=armed.attempt_dir,
        expected_prefix=armed.prefix,
        started_at_ms=2_000,
    ).snapshot
    before = started.wal_path.read_bytes()

    with pytest.raises(subject.D1HistoricalAttemptWalConcurrentWriteErrorV0, match="stale"):
        subject.append_terminal_v0(
            attempt_dir=started.attempt_dir,
            expected_prefix=armed.prefix,
            state="FAILED",
            terminal_at_ms=3_000,
            detail_code="RUN_FAILED_OUTPUT_ABSENT",
        )
    assert started.wal_path.read_bytes() == before


def test_concurrent_append_lock_then_stale_prefix_both_fail_closed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    armed = _arm(tmp_path)
    original_write = subject._write_once
    writer_entered = threading.Event()
    writer_release = threading.Event()
    outcomes: list[subject.D1AttemptWalSnapshotV0] = []
    failures: list[Exception] = []

    def blocking_write(descriptor: int, payload: bytes) -> int:
        writer_entered.set()
        if not writer_release.wait(timeout=5):
            raise OSError("test writer release timed out")
        return original_write(descriptor, payload)

    def first_writer() -> None:
        try:
            outcomes.append(
                subject.append_started_v0(
                    attempt_dir=armed.attempt_dir,
                    expected_prefix=armed.prefix,
                    started_at_ms=2_000,
                ).snapshot
            )
        except Exception as error:
            failures.append(error)

    monkeypatch.setattr(subject, "_write_once", blocking_write)
    thread = threading.Thread(target=first_writer, daemon=True)
    thread.start()
    assert writer_entered.wait(timeout=5)
    try:
        with pytest.raises(
            subject.D1HistoricalAttemptWalConcurrentWriteErrorV0,
            match="lock",
        ):
            subject.append_started_v0(
                attempt_dir=armed.attempt_dir,
                expected_prefix=armed.prefix,
                started_at_ms=2_000,
            )
    finally:
        writer_release.set()
        thread.join(timeout=5)
    assert not thread.is_alive()
    assert failures == []
    assert len(outcomes) == 1

    monkeypatch.setattr(subject, "_write_once", original_write)
    with pytest.raises(subject.D1HistoricalAttemptWalConcurrentWriteErrorV0, match="stale"):
        subject.append_started_v0(
            attempt_dir=armed.attempt_dir,
            expected_prefix=armed.prefix,
            started_at_ms=2_000,
        )


def test_short_os_writes_are_completed_before_fsync(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    armed = _arm(tmp_path)
    original = subject._write_once
    write_sizes: list[int] = []

    def short_write(descriptor: int, payload: bytes) -> int:
        piece = payload[: min(7, len(payload))]
        write_sizes.append(len(piece))
        return original(descriptor, piece)

    monkeypatch.setattr(subject, "_write_once", short_write)
    started = subject.append_started_v0(
        attempt_dir=armed.attempt_dir,
        expected_prefix=armed.prefix,
        started_at_ms=2_000,
    ).snapshot

    assert started.last_state == "STARTED_BEFORE_OUTCOME_ACCESS"
    assert len(write_sizes) > 1
    assert started.torn_tail is None


def test_partial_append_is_reported_as_torn_and_never_repaired(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    started = _started(tmp_path)
    original = subject._write_once
    calls = 0

    def fail_after_prefix(descriptor: int, payload: bytes) -> int:
        nonlocal calls
        calls += 1
        if calls == 1:
            return original(descriptor, payload[:11])
        raise OSError("injected partial append")

    monkeypatch.setattr(subject, "_write_once", fail_after_prefix)
    with pytest.raises(subject.D1HistoricalAttemptWalDurabilityErrorV0, match="append/fsync"):
        subject.append_terminal_v0(
            attempt_dir=started.attempt_dir,
            expected_prefix=started.prefix,
            state="FAILED",
            terminal_at_ms=3_000,
            detail_code="RUN_FAILED_OUTPUT_ABSENT",
        )

    observed = subject.load_attempt_wal_v0(started.attempt_dir)
    assert observed.last_state == "STARTED_BEFORE_OUTCOME_ACCESS"
    assert observed.torn_tail is not None
    assert observed.torn_tail.kind == "TORN_FRAME_PAYLOAD"
    before = observed.wal_path.read_bytes()
    with pytest.raises(subject.D1HistoricalAttemptWalStateErrorV0, match="torn"):
        subject.append_terminal_v0(
            attempt_dir=observed.attempt_dir,
            expected_prefix=observed.prefix,
            state="AMBIGUOUS_OUTPUT",
            terminal_at_ms=4_000,
            detail_code="TORN_TERMINAL_APPEND",
        )
    assert observed.wal_path.read_bytes() == before


def test_partial_start_wal_append_returns_no_grant_and_burns_attempt(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    armed = _arm(tmp_path)
    original = subject._write_once
    calls = 0

    def partial_start_then_fail(descriptor: int, payload: bytes) -> int:
        nonlocal calls
        calls += 1
        if calls == 1:
            return original(descriptor, payload[:13])
        raise OSError("injected partial START append")

    monkeypatch.setattr(subject, "_write_once", partial_start_then_fail)
    with pytest.raises(subject.D1HistoricalAttemptWalDurabilityErrorV0, match="append/fsync"):
        subject.append_started_v0(
            attempt_dir=armed.attempt_dir,
            expected_prefix=armed.prefix,
            started_at_ms=2_000,
        )
    monkeypatch.undo()

    observed = subject.load_attempt_wal_v0(armed.attempt_dir)
    assert observed.last_state == "ARMED"
    assert observed.torn_tail is not None
    assert not observed.start_seal_valid
    assert not hasattr(observed, "outcome_access_grant")
    with pytest.raises(subject.D1HistoricalAttemptWalStateErrorV0, match="torn WAL tail"):
        subject.append_started_v0(
            attempt_dir=armed.attempt_dir,
            expected_prefix=observed.prefix,
            started_at_ms=3_000,
        )


@pytest.mark.parametrize(
    ("tail", "kind"),
    [
        (b"D1", "TORN_FRAME_HEADER"),
        (struct.pack(">4sI", b"D1W0", 100) + b"{", "TORN_FRAME_PAYLOAD"),
    ],
)
def test_loader_accepts_valid_prefix_with_typed_torn_tail(
    tmp_path: Path,
    tail: bytes,
    kind: str,
) -> None:
    started = _started(tmp_path)
    _append_raw(started.wal_path, tail)

    observed = subject.load_attempt_wal_v0(started.attempt_dir)
    assert observed.prefix == started.prefix
    assert observed.last_state == "STARTED_BEFORE_OUTCOME_ACCESS"
    assert observed.torn_tail is not None
    assert observed.torn_tail.kind == kind
    assert observed.torn_tail.length_bytes == len(tail)


def test_corruption_in_a_complete_frame_before_a_torn_tail_is_rejected(tmp_path: Path) -> None:
    started = _started(tmp_path)
    invalid_complete = struct.pack(">4sI", b"D1W0", 3) + b"{}\n"
    _append_raw(started.wal_path, invalid_complete + b"D1")

    with pytest.raises(subject.D1HistoricalAttemptWalIntegrityErrorV0, match="keys"):
        subject.load_attempt_wal_v0(started.attempt_dir)


def test_payload_tamper_is_rejected_without_mutation(tmp_path: Path) -> None:
    armed = _arm(tmp_path)
    raw = bytearray(armed.wal_path.read_bytes())
    marker = b'"state":"ARMED"'
    index = raw.index(marker) + len(b'"state":"')
    raw[index] = ord("X")
    armed.wal_path.write_bytes(raw)
    before = armed.wal_path.read_bytes()

    with pytest.raises(subject.D1HistoricalAttemptWalIntegrityErrorV0):
        subject.load_attempt_wal_v0(armed.attempt_dir)
    assert armed.wal_path.read_bytes() == before


def test_copying_valid_wal_bytes_to_another_directory_is_rejected(tmp_path: Path) -> None:
    armed = _arm(tmp_path)
    copied_dir = (tmp_path / "copied-attempt").resolve()
    copied_dir.mkdir()
    copied_wal = copied_dir / subject.D1_ATTEMPT_WAL_FILE_V0
    copied_wal.write_bytes(armed.wal_path.read_bytes())

    with pytest.raises(
        subject.D1HistoricalAttemptWalIntegrityErrorV0,
        match="another directory generation",
    ):
        subject.load_attempt_wal_v0(copied_dir)


def test_recreating_the_same_path_is_rejected_as_another_directory_generation(
    tmp_path: Path,
) -> None:
    armed = _arm(tmp_path)
    saved = armed.wal_path.read_bytes()
    original_inode = armed.attempt_dir.stat().st_ino
    armed.wal_path.unlink()
    armed.attempt_dir.rmdir()
    armed.attempt_dir.mkdir()
    (armed.attempt_dir / subject.D1_ATTEMPT_WAL_FILE_V0).write_bytes(saved)
    if armed.attempt_dir.stat().st_ino == original_inode:
        pytest.skip("host immediately reused the directory inode")

    with pytest.raises(
        subject.D1HistoricalAttemptWalIntegrityErrorV0,
        match="another directory generation",
    ):
        subject.load_attempt_wal_v0(armed.attempt_dir)


def test_saved_armed_same_inode_rollback_is_detected_by_surviving_start_seal(
    tmp_path: Path,
) -> None:
    armed = _arm(tmp_path)
    saved_armed = armed.wal_path.read_bytes()
    inode = armed.wal_path.stat().st_ino
    started = subject.append_started_v0(
        attempt_dir=armed.attempt_dir,
        expected_prefix=armed.prefix,
        started_at_ms=2_000,
    ).snapshot
    assert started.start_seal_valid
    with armed.wal_path.open("r+b", buffering=0) as handle:
        handle.seek(0)
        handle.write(saved_armed)
        handle.truncate()
        handle.flush()
        os.fsync(handle.fileno())
    assert armed.wal_path.stat().st_ino == inode

    with pytest.raises(
        subject.D1HistoricalAttemptWalIntegrityErrorV0,
        match="forbidden rollback",
    ):
        subject.load_attempt_wal_v0(armed.attempt_dir)


def test_extra_member_and_hardlink_are_rejected(tmp_path: Path) -> None:
    armed = _arm(tmp_path)
    extra = armed.attempt_dir / "unexpected"
    extra.write_bytes(b"x")
    with pytest.raises(subject.D1HistoricalAttemptWalIntegrityErrorV0, match="membership"):
        subject.load_attempt_wal_v0(armed.attempt_dir)
    extra.unlink()

    alternate = tmp_path / "alternate.wal"
    os.link(armed.wal_path, alternate)
    with pytest.raises(subject.D1HistoricalAttemptWalIntegrityErrorV0, match="one link"):
        subject.load_attempt_wal_v0(armed.attempt_dir)


def test_symlinked_wal_is_rejected(tmp_path: Path) -> None:
    armed = _arm(tmp_path)
    real = tmp_path / "real.wal"
    armed.wal_path.replace(real)
    try:
        armed.wal_path.symlink_to(real)
    except OSError:
        pytest.skip("symlink creation is unavailable on this Windows host")

    with pytest.raises(subject.D1HistoricalAttemptWalIntegrityErrorV0):
        subject.load_attempt_wal_v0(armed.attempt_dir)


def test_wal_total_byte_cap_fails_closed(tmp_path: Path) -> None:
    armed = _arm(tmp_path)
    extra = b"x" * (subject.D1_ATTEMPT_WAL_MAX_BYTES_V0 - armed.total_file_bytes + 1)
    _append_raw(armed.wal_path, extra)

    with pytest.raises(subject.D1HistoricalAttemptWalIntegrityErrorV0, match="cap"):
        subject.load_attempt_wal_v0(armed.attempt_dir)


def test_append_keeps_inode_and_orders_write_before_all_syncs(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    armed = _arm(tmp_path)
    inode = armed.wal_path.stat().st_ino
    original_write = subject._write_once
    original_wal_sync = subject._sync_wal_fd
    original_seal_write = subject._write_start_seal_once
    original_seal_sync = subject._sync_start_seal_fd
    original_directory_sync = subject._sync_directory_entry
    events: list[str] = []

    def record_write(descriptor: int, payload: bytes) -> int:
        events.append("write")
        return original_write(descriptor, payload)

    def record_wal_sync(descriptor: int) -> None:
        events.append("wal_fsync")
        original_wal_sync(descriptor)

    def record_seal_write(descriptor: int, payload: bytes) -> int:
        events.append("seal_write")
        return original_seal_write(descriptor, payload)

    def record_seal_sync(descriptor: int) -> None:
        events.append("seal_fsync")
        original_seal_sync(descriptor)

    def record_directory_sync(path: Path) -> None:
        events.append(f"directory_fsync:{path}")
        original_directory_sync(path)

    monkeypatch.setattr(subject, "_write_once", record_write)
    monkeypatch.setattr(subject, "_sync_wal_fd", record_wal_sync)
    monkeypatch.setattr(subject, "_write_start_seal_once", record_seal_write)
    monkeypatch.setattr(subject, "_sync_start_seal_fd", record_seal_sync)
    monkeypatch.setattr(subject, "_sync_directory_entry", record_directory_sync)
    started = subject.append_started_v0(
        attempt_dir=armed.attempt_dir,
        expected_prefix=armed.prefix,
        started_at_ms=2_000,
    ).snapshot

    assert started.wal_path.stat().st_ino == inode
    write_index = events.index("write")
    wal_index = events.index("wal_fsync")
    seal_write_index = events.index("seal_write")
    seal_sync_index = events.index("seal_fsync")
    attempt_index = events.index(f"directory_fsync:{armed.attempt_dir}")
    parent_index = events.index(f"directory_fsync:{armed.attempt_dir.parent}")
    assert (
        write_index
        < wal_index
        < seal_write_index
        < seal_sync_index
        < attempt_index
        < parent_index
    )


def test_append_refuses_a_wal_with_another_host_durability_contract(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    armed = _arm(tmp_path)
    other = (
        subject.D1_ATTEMPT_WAL_POSIX_DURABILITY_CONTRACT_V0
        if armed.records[0].directory_durability_contract
        == subject.D1_ATTEMPT_WAL_WINDOWS_DURABILITY_CONTRACT_V0
        else subject.D1_ATTEMPT_WAL_WINDOWS_DURABILITY_CONTRACT_V0
    )
    monkeypatch.setattr(
        subject,
        "current_d1_attempt_wal_os_durability_label_v0",
        lambda: other,
    )

    assert subject.load_attempt_wal_v0(armed.attempt_dir).last_state == "ARMED"
    with pytest.raises(subject.D1HistoricalAttemptWalStateErrorV0, match="another host"):
        subject.append_started_v0(
            attempt_dir=armed.attempt_dir,
            expected_prefix=armed.prefix,
            started_at_ms=2_000,
        )


def test_fsync_failure_returns_no_success_and_stale_prefix_cannot_retry(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    armed = _arm(tmp_path)

    def fail_sync(_descriptor: int) -> None:
        raise OSError("injected fsync failure")

    monkeypatch.setattr(subject, "_sync_wal_fd", fail_sync)
    with pytest.raises(subject.D1HistoricalAttemptWalDurabilityErrorV0, match="append/fsync"):
        subject.append_started_v0(
            attempt_dir=armed.attempt_dir,
            expected_prefix=armed.prefix,
            started_at_ms=2_000,
        )
    monkeypatch.undo()

    observed = subject.load_attempt_wal_v0(armed.attempt_dir)
    assert observed.last_state == "STARTED_BEFORE_OUTCOME_ACCESS"
    with pytest.raises(subject.D1HistoricalAttemptWalConcurrentWriteErrorV0, match="stale"):
        subject.append_terminal_v0(
            attempt_dir=observed.attempt_dir,
            expected_prefix=armed.prefix,
            state="FAILED",
            terminal_at_ms=3_000,
            detail_code="RUN_FAILED_OUTPUT_ABSENT",
        )


def test_start_without_seal_is_pre_outcome_incomplete_and_permanently_blocks_retry(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    armed = _arm(tmp_path)

    def fail_before_seal(*_args, **_kwargs):
        raise subject.D1HistoricalAttemptWalDurabilityErrorV0(
            "injected crash before START seal creation"
        )

    monkeypatch.setattr(subject, "_create_and_sync_start_seal", fail_before_seal)
    with pytest.raises(
        subject.D1HistoricalAttemptWalDurabilityErrorV0,
        match="before START seal",
    ):
        subject.append_started_v0(
            attempt_dir=armed.attempt_dir,
            expected_prefix=armed.prefix,
            started_at_ms=2_000,
        )
    monkeypatch.undo()

    observed = subject.load_attempt_wal_v0(armed.attempt_dir)
    assert observed.last_state == "STARTED_BEFORE_OUTCOME_ACCESS"
    assert observed.start_seal is None
    assert not observed.start_seal_torn
    assert not observed.start_seal_valid
    with pytest.raises(subject.D1HistoricalAttemptWalStateErrorV0, match="no exact durable seal"):
        subject.append_terminal_v0(
            attempt_dir=observed.attempt_dir,
            expected_prefix=observed.prefix,
            state="FAILED",
            terminal_at_ms=3_000,
            detail_code="RUN_FAILED_OUTPUT_ABSENT",
        )
    with pytest.raises(subject.D1HistoricalAttemptWalStateErrorV0):
        subject.append_started_v0(
            attempt_dir=observed.attempt_dir,
            expected_prefix=observed.prefix,
            started_at_ms=3_000,
        )


def test_partial_start_seal_is_typed_torn_and_permanently_blocks_retry(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    armed = _arm(tmp_path)
    real_write = subject._write_start_seal_once
    calls = 0

    def partial_then_fail(descriptor: int, payload: bytes) -> int:
        nonlocal calls
        calls += 1
        if calls == 1:
            return real_write(descriptor, payload[:9])
        raise OSError("injected seal write crash")

    monkeypatch.setattr(subject, "_write_start_seal_once", partial_then_fail)
    with pytest.raises(subject.D1HistoricalAttemptWalDurabilityErrorV0, match="seal write"):
        subject.append_started_v0(
            attempt_dir=armed.attempt_dir,
            expected_prefix=armed.prefix,
            started_at_ms=2_000,
        )
    monkeypatch.undo()

    observed = subject.load_attempt_wal_v0(armed.attempt_dir)
    assert observed.last_state == "STARTED_BEFORE_OUTCOME_ACCESS"
    assert observed.start_seal is None
    assert observed.start_seal_torn
    assert not observed.start_seal_valid
    with pytest.raises(subject.D1HistoricalAttemptWalStateErrorV0, match="no exact durable seal"):
        subject.append_terminal_v0(
            attempt_dir=observed.attempt_dir,
            expected_prefix=observed.prefix,
            state="FAILED",
            terminal_at_ms=3_000,
            detail_code="RUN_FAILED_OUTPUT_ABSENT",
        )


@pytest.mark.parametrize(
    "failure_point",
    ("seal_fsync", "directory_flush", "seal_close", "wal_close", "lock_release"),
)
def test_post_start_durability_failures_never_allow_a_second_start(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    failure_point: str,
) -> None:
    armed = _arm(tmp_path)
    if failure_point == "seal_fsync":
        monkeypatch.setattr(
            subject,
            "_sync_start_seal_fd",
            lambda _descriptor: (_ for _ in ()).throw(OSError("seal fsync failure")),
        )
    elif failure_point == "directory_flush":
        monkeypatch.setattr(
            subject,
            "_sync_directory_entry",
            lambda _path: (_ for _ in ()).throw(
                subject.D1HistoricalAttemptWalDurabilityErrorV0(
                    "directory flush failure"
                )
            ),
        )
    elif failure_point in {"seal_close", "wal_close"}:
        real_open = subject.os.open
        real_close = subject.os.close
        descriptor_kind: dict[int, str] = {}
        failed = False

        def tracking_open(path, flags, mode=0o777):
            descriptor = real_open(path, flags, mode)
            name = Path(path).name
            if name == subject.D1_ATTEMPT_START_SEAL_FILE_V0:
                descriptor_kind[descriptor] = "seal_close"
            elif name == subject.D1_ATTEMPT_WAL_FILE_V0:
                descriptor_kind[descriptor] = "wal_close"
            return descriptor

        def close_then_fail(descriptor: int) -> None:
            nonlocal failed
            kind = descriptor_kind.get(descriptor)
            real_close(descriptor)
            if not failed and kind == failure_point:
                failed = True
                raise OSError(f"{failure_point} failure")

        monkeypatch.setattr(subject.os, "open", tracking_open)
        monkeypatch.setattr(subject.os, "close", close_then_fail)
    else:
        real_release = subject._release_exclusive_lock

        def release_then_fail(descriptor: int) -> None:
            real_release(descriptor)
            raise OSError("lock release failure")

        monkeypatch.setattr(subject, "_release_exclusive_lock", release_then_fail)

    with pytest.raises(subject.D1HistoricalAttemptWalDurabilityErrorV0):
        subject.append_started_v0(
            attempt_dir=armed.attempt_dir,
            expected_prefix=armed.prefix,
            started_at_ms=2_000,
        )
    monkeypatch.undo()

    observed = subject.load_attempt_wal_v0(armed.attempt_dir)
    assert observed.last_state == "STARTED_BEFORE_OUTCOME_ACCESS"
    assert observed.start_seal is not None
    assert observed.start_seal_valid
    with pytest.raises(subject.D1HistoricalAttemptWalStateErrorV0):
        subject.append_started_v0(
            attempt_dir=observed.attempt_dir,
            expected_prefix=observed.prefix,
            started_at_ms=3_000,
        )


def test_post_fsync_readback_rejects_bytes_different_from_requested_append(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    armed = _arm(tmp_path)
    original_write = subject._write_once

    def corrupting_write(descriptor: int, payload: bytes) -> int:
        corrupted = bytearray(payload)
        corrupted[-1] = ord(" ")
        return original_write(descriptor, bytes(corrupted))

    monkeypatch.setattr(subject, "_write_once", corrupting_write)
    with pytest.raises(subject.D1HistoricalAttemptWalDurabilityErrorV0, match="readback differs"):
        subject.append_started_v0(
            attempt_dir=armed.attempt_dir,
            expected_prefix=armed.prefix,
            started_at_ms=2_000,
        )
    with pytest.raises(subject.D1HistoricalAttemptWalIntegrityErrorV0):
        subject.load_attempt_wal_v0(armed.attempt_dir)


@pytest.mark.skipif(os.name == "nt", reason="Windows does not permit this open-inode swap")
def test_path_swap_after_append_is_not_mistaken_for_the_open_inode(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    armed = _arm(tmp_path)
    replacement = tmp_path / "replacement.wal"
    replacement.write_bytes(armed.wal_path.read_bytes())
    original_sync = subject._sync_wal_fd

    def swap_after_sync(descriptor: int) -> None:
        original_sync(descriptor)
        os.replace(replacement, armed.wal_path)

    monkeypatch.setattr(subject, "_sync_wal_fd", swap_after_sync)
    with pytest.raises(subject.D1HistoricalAttemptWalIntegrityErrorV0, match="inode"):
        subject.append_started_v0(
            attempt_dir=armed.attempt_dir,
            expected_prefix=armed.prefix,
            started_at_ms=2_000,
        )


def test_timestamp_regression_and_oversized_detail_fail_before_write(tmp_path: Path) -> None:
    started = _started(tmp_path)
    before = started.wal_path.read_bytes()
    with pytest.raises(subject.D1HistoricalAttemptWalIntegrityErrorV0, match="backwards"):
        subject.append_terminal_v0(
            attempt_dir=started.attempt_dir,
            expected_prefix=started.prefix,
            state="FAILED",
            terminal_at_ms=1_999,
            detail_code="RUN_FAILED_OUTPUT_ABSENT",
        )
    with pytest.raises(subject.D1HistoricalAttemptWalIntegrityErrorV0, match="detail_code"):
        subject.append_terminal_v0(
            attempt_dir=started.attempt_dir,
            expected_prefix=started.prefix,
            state="FAILED",
            terminal_at_ms=3_000,
            detail_code="X" * 129,
        )
    assert started.wal_path.read_bytes() == before


def test_timestamp_domain_is_bounded_to_the_json_safe_integer_limit(
    tmp_path: Path,
) -> None:
    maximum = subject.D1_ATTEMPT_MAX_SAFE_TIMESTAMP_MS_V0
    armed = subject.create_armed_wal_v0(
        attempt_dir=(tmp_path / "maximum").resolve(),
        bindings=_bindings(),
        armed_at_ms=maximum,
    )
    assert armed.records[0].observed_at_ms == maximum
    rejected = (tmp_path / "too-large").resolve()
    with pytest.raises(
        subject.D1HistoricalAttemptWalIntegrityErrorV0,
        match="JSON-safe",
    ):
        subject.create_armed_wal_v0(
            attempt_dir=rejected,
            bindings=_bindings(),
            armed_at_ms=maximum + 1,
        )
    assert not rejected.exists()


def test_attempt_directory_scandir_is_bounded(tmp_path: Path) -> None:
    armed = _arm(tmp_path)
    (armed.attempt_dir / "extra-one").write_bytes(b"1")
    (armed.attempt_dir / "extra-two").write_bytes(b"2")

    with pytest.raises(subject.D1HistoricalAttemptWalIntegrityErrorV0, match="bounded"):
        subject.load_attempt_wal_v0(armed.attempt_dir)


def test_start_seal_tamper_and_hardlink_are_rejected(tmp_path: Path) -> None:
    started = _started(tmp_path)
    seal_path = started.attempt_dir / subject.D1_ATTEMPT_START_SEAL_FILE_V0
    original = seal_path.read_bytes()
    tampered = bytearray(original)
    marker = b'"production_order_placement":false'
    index = tampered.index(marker) + len(b'"production_order_placement":')
    tampered[index] = ord("t")
    seal_path.write_bytes(tampered)
    with pytest.raises(subject.D1HistoricalAttemptWalIntegrityErrorV0):
        subject.load_attempt_wal_v0(started.attempt_dir)

    seal_path.write_bytes(original)
    alternate = tmp_path / "seal-hardlink"
    os.link(seal_path, alternate)
    with pytest.raises(subject.D1HistoricalAttemptWalIntegrityErrorV0, match="one link"):
        subject.load_attempt_wal_v0(started.attempt_dir)


@pytest.mark.skipif(os.name != "nt", reason="Win32 directory flush adapter only")
def test_windows_start_success_flushes_attempt_and_parent_with_write_handles(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    armed = _arm(tmp_path)
    real_open = subject._windows_open_directory_handle
    real_flush = subject._windows_flush_directory_handle
    real_close = subject._windows_close_handle
    handle_path: dict[int, Path] = {}
    events: list[tuple[str, Path]] = []

    def tracking_open(path: Path) -> int:
        handle = real_open(path)
        handle_path[handle] = path
        events.append(("open", path))
        return handle

    def tracking_flush(handle: int) -> None:
        events.append(("flush", handle_path[handle]))
        real_flush(handle)

    def tracking_close(handle: int) -> None:
        events.append(("close", handle_path[handle]))
        real_close(handle)

    monkeypatch.setattr(subject, "_windows_open_directory_handle", tracking_open)
    monkeypatch.setattr(subject, "_windows_flush_directory_handle", tracking_flush)
    monkeypatch.setattr(subject, "_windows_close_handle", tracking_close)
    started = subject.append_started_v0(
        attempt_dir=armed.attempt_dir,
        expected_prefix=armed.prefix,
        started_at_ms=2_000,
    ).snapshot

    assert started.start_seal_valid
    assert events == [
        ("open", armed.attempt_dir),
        ("flush", armed.attempt_dir),
        ("close", armed.attempt_dir),
        ("open", armed.attempt_dir.parent),
        ("flush", armed.attempt_dir.parent),
        ("close", armed.attempt_dir.parent),
    ]


@pytest.mark.skipif(os.name != "nt", reason="Win32 directory flush adapter only")
def test_windows_directory_flush_failure_returns_no_start_success_and_blocks_retry(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    armed = _arm(tmp_path)

    def fail_flush(_handle: int) -> None:
        raise subject.D1HistoricalAttemptWalDurabilityErrorV0(
            "injected FlushFileBuffers failure"
        )

    monkeypatch.setattr(subject, "_windows_flush_directory_handle", fail_flush)
    with pytest.raises(
        subject.D1HistoricalAttemptWalDurabilityErrorV0,
        match="FlushFileBuffers",
    ):
        subject.append_started_v0(
            attempt_dir=armed.attempt_dir,
            expected_prefix=armed.prefix,
            started_at_ms=2_000,
        )
    monkeypatch.undo()

    observed = subject.load_attempt_wal_v0(armed.attempt_dir)
    assert observed.last_state == "STARTED_BEFORE_OUTCOME_ACCESS"
    assert observed.start_seal_valid
    with pytest.raises(subject.D1HistoricalAttemptWalStateErrorV0):
        subject.append_started_v0(
            attempt_dir=observed.attempt_dir,
            expected_prefix=observed.prefix,
            started_at_ms=3_000,
        )


@pytest.mark.skipif(os.name != "nt", reason="Windows local-volume policy only")
def test_windows_nonlocal_or_unsupported_volume_fails_before_armed_success(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    target = (tmp_path / "unsupported").resolve()
    monkeypatch.setattr(
        subject,
        "_windows_local_volume_identity",
        lambda _path: (_ for _ in ()).throw(
            subject.D1HistoricalAttemptWalDurabilityErrorV0(
                "attempt WAL requires a local fixed NTFS volume"
            )
        ),
    )

    with pytest.raises(
        subject.D1HistoricalAttemptWalDurabilityErrorV0,
        match="fixed NTFS",
    ):
        subject.create_armed_wal_v0(
            attempt_dir=target,
            bindings=_bindings(),
            armed_at_ms=1_000,
        )
    assert target.is_dir()
    assert not tuple(target.iterdir())


@pytest.mark.skipif(os.name != "nt", reason="Windows NTFS-only policy")
def test_windows_refs_volume_is_rejected_by_the_actual_volume_adapter(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class _FakeFunction:
        argtypes: object = None
        restype: object = None

        def __init__(self, callback) -> None:
            self._callback = callback

        def __call__(self, *args: object):
            return self._callback(*args)

    def volume_path(_path, output, _size) -> int:
        output.value = "R:\\"
        return 1

    def refs_volume(
        _root,
        _label,
        _label_size,
        _serial,
        _maximum_component,
        _flags,
        filesystem,
        _filesystem_size,
    ) -> int:
        filesystem.value = "ReFS"
        return 1

    class _FakeKernel32:
        GetVolumePathNameW = _FakeFunction(volume_path)
        GetDriveTypeW = _FakeFunction(lambda _root: 3)
        GetVolumeInformationW = _FakeFunction(refs_volume)

    monkeypatch.setattr(subject, "_windows_kernel32", lambda: _FakeKernel32())
    with pytest.raises(
        subject.D1HistoricalAttemptWalDurabilityErrorV0,
        match="fixed NTFS",
    ):
        subject._windows_local_volume_identity(tmp_path)


def test_host_durability_contract_claims_only_the_verified_host_boundary(
    tmp_path: Path,
) -> None:
    armed = _arm(tmp_path)
    contract = armed.records[0].directory_durability_contract
    if os.name == "nt":
        assert contract == subject.D1_ATTEMPT_WAL_WINDOWS_DURABILITY_CONTRACT_V0
        assert "LOCAL_FIXED_NTFS" in contract
        assert "REFS" not in contract
        assert "DIRECTORY_FLUSH" in contract
    else:
        assert contract == subject.D1_ATTEMPT_WAL_POSIX_DURABILITY_CONTRACT_V0
