from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, replace
from pathlib import Path

import pytest

from signalbot.r4b_v2.capture.wal import (
    WalAuthorityV2,
    WalCapacityError,
    WalDurabilityBindingV2,
    WalError,
    WalIntegrityError,
    WalSyncPolicyV2,
    WalWriterV2,
    crc32c,
    encode_uvarint,
    encode_wal_frame,
    scan_wal_file,
    verify_wal_segments,
)

HASH = "a" * 64


@dataclass(frozen=True, slots=True)
class _Queued:
    ingest_seq: int
    encoded_line: bytes
    encoded_len: int
    encoded_sha256: str

    def verify_integrity(self) -> None:
        if len(self.encoded_line) != self.encoded_len:
            raise ValueError("bad encoded length")
        if hashlib.sha256(self.encoded_line).hexdigest() != self.encoded_sha256:
            raise ValueError("bad encoded digest")


class _Clock:
    def __init__(self) -> None:
        self.value = 1_000_000_000

    def __call__(self) -> int:
        return self.value


def _record(ingest_seq: int, payload_bytes: int = 0) -> _Queued:
    encoded = (
        json.dumps(
            {"ingest_seq": ingest_seq, "payload": "x" * payload_bytes},
            separators=(",", ":"),
            sort_keys=True,
        ).encode()
        + b"\n"
    )
    return _Queued(
        ingest_seq=ingest_seq,
        encoded_line=encoded,
        encoded_len=len(encoded),
        encoded_sha256=hashlib.sha256(encoded).hexdigest(),
    )


def _authority() -> WalAuthorityV2:
    return WalAuthorityV2(
        attempt_id="attempt-1",
        protocol_sha256=HASH,
        plan_sha256="b" * 64,
        source_manifest_sha256="c" * 64,
        schema_sha256="d" * 64,
        runtime_manifest_sha256="e" * 64,
    )


def _other_authority() -> WalAuthorityV2:
    return WalAuthorityV2(
        attempt_id="attempt-2",
        protocol_sha256="f" * 64,
        plan_sha256="1" * 64,
        source_manifest_sha256="2" * 64,
        schema_sha256="3" * 64,
        runtime_manifest_sha256="4" * 64,
    )


def _policy(**overrides: int | str) -> WalSyncPolicyV2:
    values: dict[str, int | str] = {
        "qualification_id": "q-10ms-r10",
        "fsync_candidate_id": "fsync-10ms-r10",
        "interval_ms": 10,
        "max_unsynced_records": 10,
        "max_unsynced_bytes": 100_000,
        "max_record_bytes": 10_000,
        "max_segment_bytes": 100_000,
    }
    values.update(overrides)
    return WalSyncPolicyV2(**values)  # type: ignore[arg-type]


def _writer(
    tmp_path: Path,
    *,
    policy: WalSyncPolicyV2 | None = None,
    clock: _Clock | None = None,
) -> WalWriterV2:
    return WalWriterV2(
        tmp_path,
        authority=_authority(),
        policy=policy or _policy(),
        maximum_total_bytes=8 * 1024 * 1024,
        emergency_reserve_bytes=1024,
        clock_ns=clock or _Clock(),
    )


def test_crc32c_uses_castagnoli_known_vector() -> None:
    assert crc32c(b"123456789") == 0xE3069283


@pytest.mark.parametrize(
    ("value", "encoded"),
    [
        (0, b"\x00"),
        (1, b"\x01"),
        (127, b"\x7f"),
        (128, b"\x80\x01"),
        (16_383, b"\xff\x7f"),
        (16_384, b"\x80\x80\x01"),
        (0xFFFFFFFFFFFFFFFF, b"\xff" * 9 + b"\x01"),
    ],
)
def test_uvarint_known_boundaries(value: int, encoded: bytes) -> None:
    assert encode_uvarint(value) == encoded


@pytest.mark.parametrize("value", [-1, 0x1_0000_0000_0000_0000, True])
def test_uvarint_rejects_values_outside_unsigned_64_bit_domain(value: int) -> None:
    with pytest.raises(ValueError, match="unsigned 64-bit"):
        encode_uvarint(value)


def test_wal_frame_uses_uvarint_length_prefix() -> None:
    line = b'{"payload":"' + (b"x" * 120) + b'"}\n'
    assert len(line) == 135
    assert encode_wal_frame(line).startswith(b"\x87\x01" + line)


@pytest.mark.parametrize("interval_ms", [10, 50, 100])
def test_only_sealed_fsync_grid_is_accepted(interval_ms: int) -> None:
    assert _policy(interval_ms=interval_ms).interval_ms == interval_ms


@pytest.mark.parametrize("interval_ms", [0, 9, 11, 1000])
def test_unsealed_fsync_interval_is_rejected(interval_ms: int) -> None:
    with pytest.raises(ValueError, match="10/50/100"):
        _policy(interval_ms=interval_ms)


def test_single_wal_exposes_its_exact_canonical_durability_binding(
    tmp_path: Path,
) -> None:
    writer = _writer(tmp_path)

    binding = writer.durability_binding

    assert binding.mode == "SINGLE_ROOT"
    assert binding.root_bindings == (writer.root_binding,)
    assert binding.qualification_selection_receipt_sha256 is None
    assert not binding.physical_failure_domain_independence_verified
    assert binding.schema_version == "r4b_v2_wal_durability_binding_v1"
    assert len(binding.sha256) == 64
    assert binding.sha256 == writer.durability_binding.sha256
    writer.abort()


def test_single_wal_durability_binding_validates_count_role_authority_and_truth(
    tmp_path: Path,
) -> None:
    writer = _writer(tmp_path)
    root = writer.root_binding

    with pytest.raises(ValueError, match="requires exactly one"):
        WalDurabilityBindingV2(
            mode="SINGLE_ROOT",
            root_bindings=(),
            qualification_selection_receipt_sha256=None,
            physical_failure_domain_independence_verified=False,
        )
    with pytest.raises(ValueError, match="unsupported root role"):
        WalDurabilityBindingV2(
            mode="SINGLE_ROOT",
            root_bindings=(replace(root, root_role="UNSEALED"),),
            qualification_selection_receipt_sha256=None,
            physical_failure_domain_independence_verified=False,
        )
    with pytest.raises(ValueError, match="root authority"):
        WalDurabilityBindingV2(
            mode="SINGLE_ROOT",
            root_bindings=(replace(root, authority_sha256="not-a-digest"),),
            qualification_selection_receipt_sha256=None,
            physical_failure_domain_independence_verified=False,
        )
    with pytest.raises(ValueError, match="cannot claim physical"):
        WalDurabilityBindingV2(
            mode="SINGLE_ROOT",
            root_bindings=(root,),
            qualification_selection_receipt_sha256=None,
            physical_failure_domain_independence_verified=True,
        )
    writer.abort()


def test_single_wal_revalidates_current_root_binding_bytes(tmp_path: Path) -> None:
    writer = _writer(tmp_path)
    writer.assert_root_binding_current()
    (tmp_path / "storage-root-binding.json").write_bytes(b"{}\n")

    with pytest.raises(WalIntegrityError, match="expected current bytes"):
        writer.assert_root_binding_current()
    writer.abort()


def test_batch_ack_advances_only_after_sync(tmp_path: Path) -> None:
    clock = _Clock()
    writer = _writer(tmp_path, clock=clock)
    result = writer.append_batch([_record(1), _record(2)], now_ns=clock.value)
    assert result.durable_ack_seq == 0
    assert result.pending_records == 2

    clock.value += 10_000_000
    assert writer.sync(now_ns=clock.value) == 2
    assert writer.pending_records == 0
    state = json.loads((tmp_path / "wal-state.json").read_text(encoding="utf-8"))
    assert state["durable_ack_seq"] == 2
    writer.close()


def test_record_count_cap_forces_fsync(tmp_path: Path) -> None:
    writer = _writer(tmp_path, policy=_policy(max_unsynced_records=2))
    result = writer.append_batch([_record(1), _record(2)])
    assert result.fsynced is True
    assert result.durable_ack_seq == 2
    writer.close()


def test_single_batch_cannot_overshoot_unsynced_record_cap(tmp_path: Path) -> None:
    writer = _writer(tmp_path, policy=_policy(max_unsynced_records=3))

    with pytest.raises(WalCapacityError, match="max_unsynced_records"):
        writer.append_batch([_record(index) for index in range(1, 6)])
    assert writer.next_ingest_seq == 1
    assert not list(tmp_path.glob("wal-*.partial"))


def test_pending_prefix_syncs_before_next_batch_would_cross_cap(tmp_path: Path) -> None:
    writer = _writer(tmp_path, policy=_policy(max_unsynced_records=3))
    writer.append_batch([_record(1), _record(2)])
    result = writer.append_batch([_record(3), _record(4)])

    assert result.durable_ack_seq == 2
    assert result.pending_records == 2
    assert writer.next_ingest_seq == 5
    writer.close()


def test_batch_must_be_contiguous_before_any_write(tmp_path: Path) -> None:
    writer = _writer(tmp_path)
    with pytest.raises(WalIntegrityError, match="not contiguous"):
        writer.append_batch([_record(1), _record(3)])
    assert not list(tmp_path.glob("wal-*.partial"))


def test_encoded_integrity_is_checked_without_reserializing(tmp_path: Path) -> None:
    good = _record(1)
    bad = _Queued(
        ingest_seq=1,
        encoded_line=good.encoded_line,
        encoded_len=good.encoded_len,
        encoded_sha256="0" * 64,
    )
    writer = _writer(tmp_path)
    with pytest.raises(ValueError, match="bad encoded digest"):
        writer.append_batch([bad])


@pytest.mark.parametrize("fault_point", ["after_batch_write", "after_wal_fsync"])
def test_mutating_io_fault_permanently_latches_writer_before_reuse(
    tmp_path: Path,
    fault_point: str,
) -> None:
    def fail(point: str) -> None:
        if point == fault_point:
            raise OSError(f"synthetic {fault_point}")

    writer = WalWriterV2(
        tmp_path,
        authority=_authority(),
        policy=_policy(max_unsynced_records=1),
        maximum_total_bytes=8 * 1024 * 1024,
        emergency_reserve_bytes=1024,
        clock_ns=_Clock(),
        fault_hook=fail,
    )
    with pytest.raises(OSError, match=fault_point):
        writer.append_batch([_record(1)])
    assert writer.durable_ack_seq == 0

    with pytest.raises(WalError, match="failed and cannot be reused"):
        writer.append_batch([_record(1)])
    writer.abort()
    assert not list(tmp_path.glob("wal-*.manifest.json"))


def test_record_and_batch_bounds_fail_closed(tmp_path: Path) -> None:
    oversized = _record(1, payload_bytes=500)
    writer = _writer(
        tmp_path,
        policy=_policy(max_record_bytes=100, max_segment_bytes=200),
    )
    with pytest.raises(WalCapacityError, match="max_record_bytes"):
        writer.append_batch([oversized])


def test_segment_rotates_only_when_next_batch_would_cross_bound(tmp_path: Path) -> None:
    first = _record(1)
    second = _record(2)
    exact = len(encode_wal_frame(first.encoded_line))
    writer = _writer(
        tmp_path,
        policy=_policy(max_segment_bytes=exact, max_record_bytes=exact),
    )
    writer.append_batch([first])
    writer.append_batch([second])
    writer.close()
    manifests = verify_wal_segments(
        tmp_path,
        authority=_authority(),
        policy=_policy(max_segment_bytes=exact, max_record_bytes=exact),
    )
    assert [item.record_count for item in manifests] == [1, 1]
    assert manifests[1].previous_segment_sha256 == manifests[0].sha256


def test_clean_close_binds_hash_chain_and_record_metadata(tmp_path: Path) -> None:
    writer = _writer(tmp_path)
    writer.append_batch([_record(1), _record(2)])
    writer.close()
    [manifest] = verify_wal_segments(
        tmp_path,
        authority=_authority(),
        policy=_policy(),
    )
    assert manifest.first_ingest_seq == 1
    assert manifest.last_ingest_seq == 2
    assert manifest.durable_ack_seq == 2
    assert manifest.record_count == 2


def test_torn_final_frame_is_preserved_then_prefix_is_recovered(tmp_path: Path) -> None:
    writer = _writer(tmp_path)
    writer.append_batch([_record(1)])
    writer.abort()
    [partial] = tmp_path.glob("wal-*.partial")
    torn = encode_wal_frame(_record(2).encoded_line)[:-3]
    with partial.open("ab") as handle:
        handle.write(torn)

    recovered = _writer(tmp_path)
    assert recovered.next_ingest_seq == 2
    assert recovered.durable_ack_seq == 1
    [receipt_path] = tmp_path.glob("*.recovery.json")
    receipt = json.loads(receipt_path.read_text(encoding="utf-8"))
    discarded = tmp_path / receipt["discarded_file"]
    assert discarded.read_bytes() == torn
    assert hashlib.sha256(torn).hexdigest() == receipt["discarded_sha256"]
    recovered.close()


def test_complete_crc_mismatch_is_never_repaired(tmp_path: Path) -> None:
    writer = _writer(tmp_path)
    writer.append_batch([_record(1)])
    writer.abort()
    [partial] = tmp_path.glob("wal-*.partial")
    payload = bytearray(partial.read_bytes())
    payload[-1] ^= 1
    partial.write_bytes(payload)

    with pytest.raises(WalIntegrityError, match="CRC-32C mismatch"):
        _writer(tmp_path)
    assert partial.read_bytes() == payload
    assert not list(tmp_path.glob("*.recovery.json"))


def test_crash_after_rename_seals_only_the_verified_finalized_orphan(
    tmp_path: Path,
) -> None:
    def crash_after_rename(point: str) -> None:
        if point == "after_wal_rename":
            raise RuntimeError("synthetic crash after rename")

    writer = WalWriterV2(
        tmp_path,
        authority=_authority(),
        policy=_policy(),
        maximum_total_bytes=8 * 1024 * 1024,
        emergency_reserve_bytes=1024,
        clock_ns=_Clock(),
        fault_hook=crash_after_rename,
    )
    writer.append_batch([_record(1)])
    with pytest.raises(RuntimeError, match="synthetic crash"):
        writer.close()
    assert (tmp_path / "wal-00000001.wal").is_file()
    assert not (tmp_path / "wal-00000001.manifest.json").exists()

    recovered = _writer(tmp_path)
    assert recovered.durable_ack_seq == 1
    [manifest] = verify_wal_segments(
        tmp_path,
        authority=_authority(),
        policy=_policy(),
    )
    assert manifest.last_ingest_seq == 1
    receipt = json.loads(
        (tmp_path / "wal-00000001.orphan-recovery.json").read_text(encoding="utf-8")
    )
    assert receipt["data_sha256"] == manifest.sha256
    recovered.close()


def test_recovery_disabled_rejects_finalized_orphan_without_mutating_it(
    tmp_path: Path,
) -> None:
    def crash_after_rename(point: str) -> None:
        if point == "after_wal_rename":
            raise RuntimeError("synthetic crash after rename")

    writer = WalWriterV2(
        tmp_path,
        authority=_authority(),
        policy=_policy(),
        maximum_total_bytes=8 * 1024 * 1024,
        emergency_reserve_bytes=1024,
        clock_ns=_Clock(),
        fault_hook=crash_after_rename,
    )
    writer.append_batch([_record(1)])
    with pytest.raises(RuntimeError, match="synthetic crash"):
        writer.close()
    orphan = tmp_path / "wal-00000001.wal"
    before = orphan.read_bytes()

    with pytest.raises(WalIntegrityError, match="requires explicit recovery"):
        WalWriterV2(
            tmp_path,
            authority=_authority(),
            policy=_policy(),
            maximum_total_bytes=8 * 1024 * 1024,
            emergency_reserve_bytes=1024,
            clock_ns=_Clock(),
            recover_torn_tail=False,
        )
    assert orphan.read_bytes() == before
    assert not (tmp_path / "wal-00000001.manifest.json").exists()
    assert not tuple(tmp_path.glob("*.recovery.json"))


def test_finalized_orphan_cannot_be_laundered_into_a_new_authority(tmp_path: Path) -> None:
    def crash_after_rename(point: str) -> None:
        if point == "after_wal_rename":
            raise RuntimeError("synthetic crash after rename")

    writer = WalWriterV2(
        tmp_path,
        authority=_authority(),
        policy=_policy(),
        maximum_total_bytes=8 * 1024 * 1024,
        emergency_reserve_bytes=1024,
        clock_ns=_Clock(),
        fault_hook=crash_after_rename,
    )
    writer.append_batch([_record(1)])
    with pytest.raises(RuntimeError, match="synthetic crash"):
        writer.close()

    with pytest.raises(WalIntegrityError, match="root binding differs"):
        WalWriterV2(
            tmp_path,
            authority=_other_authority(),
            policy=_policy(),
            maximum_total_bytes=8 * 1024 * 1024,
            emergency_reserve_bytes=1024,
            clock_ns=_Clock(),
        )
    assert not (tmp_path / "wal-00000001.manifest.json").exists()


def test_missing_root_binding_never_rebinds_a_nonempty_wal_root(tmp_path: Path) -> None:
    writer = _writer(tmp_path)
    writer.append_batch([_record(1)])
    writer.abort()
    (tmp_path / "storage-root-binding.json").unlink()

    with pytest.raises(WalIntegrityError, match="non-empty storage root"):
        _writer(tmp_path)


def test_durable_offset_must_be_a_frame_boundary(tmp_path: Path) -> None:
    writer = _writer(tmp_path)
    writer.append_batch([_record(1)])
    writer.sync()
    writer.abort()
    state_path = tmp_path / "wal-state.json"
    state = json.loads(state_path.read_text(encoding="utf-8"))
    state["durable_offset"] -= 1
    state_path.write_text(json.dumps(state), encoding="utf-8")

    with pytest.raises(WalIntegrityError, match="complete frame boundary"):
        _writer(tmp_path)


def test_durable_ack_must_equal_the_sequence_at_its_exact_offset(tmp_path: Path) -> None:
    first = _record(1)
    writer = _writer(tmp_path)
    writer.append_batch([first, _record(2)])
    writer.sync()
    writer.abort()
    state_path = tmp_path / "wal-state.json"
    state = json.loads(state_path.read_text(encoding="utf-8"))
    state["durable_offset"] = len(encode_wal_frame(first.encoded_line))
    assert state["durable_ack_seq"] == 2
    state_path.write_text(json.dumps(state), encoding="utf-8")

    with pytest.raises(WalIntegrityError, match="exact durable frame boundary"):
        _writer(tmp_path)


def test_scan_rejects_ingest_gap_even_with_valid_crc(tmp_path: Path) -> None:
    path = tmp_path / "wal-00000001.partial"
    path.write_bytes(encode_wal_frame(_record(2).encoded_line))
    with pytest.raises(WalIntegrityError, match="not contiguous"):
        scan_wal_file(
            path,
            expected_first_ingest_seq=1,
            max_record_bytes=10_000,
        )


def test_scan_rejects_nonminimal_uvarint_even_with_complete_payload(tmp_path: Path) -> None:
    line = _record(1).encoded_line
    canonical = encode_wal_frame(line)
    path = tmp_path / "wal-00000001.partial"
    path.write_bytes(b"\x80\x00" + canonical[1:])

    with pytest.raises(WalIntegrityError, match="not minimally encoded"):
        scan_wal_file(path, expected_first_ingest_seq=1, max_record_bytes=10_000)


def test_scan_classifies_torn_uvarint_prefix_as_recoverable_tail(tmp_path: Path) -> None:
    path = tmp_path / "wal-00000001.partial"
    path.write_bytes(encode_wal_frame(_record(1).encoded_line) + b"\x80")

    scan = scan_wal_file(path, expected_first_ingest_seq=1, max_record_bytes=10_000)
    assert scan.last_ingest_seq == 1
    assert scan.torn_tail_offset == len(encode_wal_frame(_record(1).encoded_line))


def test_scan_rejects_overflowing_uvarint_prefix(tmp_path: Path) -> None:
    path = tmp_path / "wal-00000001.partial"
    path.write_bytes(b"\xff" * 10 + b"\x00")

    with pytest.raises(WalIntegrityError, match="unsigned 64-bit range"):
        scan_wal_file(path, expected_first_ingest_seq=1, max_record_bytes=10_000)


def test_consume_durable_records_streams_exact_verified_prefix(tmp_path: Path) -> None:
    frame_bytes = len(encode_wal_frame(_record(1).encoded_line))
    writer = _writer(
        tmp_path,
        policy=_policy(max_record_bytes=frame_bytes, max_segment_bytes=frame_bytes),
    )
    writer.append_batch([_record(1)])
    writer.append_batch([_record(2)])
    writer.sync()
    delivered: list[tuple[int, bytes]] = []
    assert writer.consume_durable_records(
        lambda ingest_seq, line: delivered.append((ingest_seq, line))
    ) == 2
    assert [item[0] for item in delivered] == [1, 2]
    assert delivered[0][1] == _record(1).encoded_line
    writer.close()
