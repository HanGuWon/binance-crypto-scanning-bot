from __future__ import annotations

import gc
import hashlib
import json
import random
import struct
import tracemalloc
from pathlib import Path

import pytest
import zstandard as zstd

from signalbot.capture.errors import CaptureIntegrityError
from signalbot.capture.models import (
    CaptureEnvelopeV1,
    CoverageReason,
    invalidation_for_record,
    record_to_json_line,
)
from signalbot.capture.storage import (
    SegmentedCaptureWriter,
    read_segment_lines,
    verify_capture_segments,
)
from signalbot.domain.enums import Market

PLAN_SHA256 = hashlib.sha256(b"prospective-capture-test-plan").hexdigest()
_OUTER_FRAME_MAGIC = b"SBCAPFRM"
_OUTER_FRAME_FORMAT_VERSION = 1
_OUTER_FRAME_HEADER_CORE = struct.Struct(">8sBQQ32s")
_OUTER_FRAME_HEADER_SIZE = _OUTER_FRAME_HEADER_CORE.size + hashlib.sha256().digest_size


def _encode_test_outer_frame(
    decoded: bytes,
    *,
    write_checksum: bool = True,
) -> bytes:
    compressed = zstd.ZstdCompressor(write_checksum=write_checksum).compress(decoded)
    core = _OUTER_FRAME_HEADER_CORE.pack(
        _OUTER_FRAME_MAGIC,
        _OUTER_FRAME_FORMAT_VERSION,
        len(compressed),
        len(decoded),
        hashlib.sha256(compressed).digest(),
    )
    return core + hashlib.sha256(core).digest() + compressed


def _outer_frame_ranges(encoded: bytes | bytearray) -> list[tuple[int, int, int]]:
    ranges: list[tuple[int, int, int]] = []
    offset = 0
    while offset < len(encoded):
        assert len(encoded) - offset >= _OUTER_FRAME_HEADER_SIZE
        core_end = offset + _OUTER_FRAME_HEADER_CORE.size
        header_end = offset + _OUTER_FRAME_HEADER_SIZE
        _, _, compressed_length, _, _ = _OUTER_FRAME_HEADER_CORE.unpack(
            encoded[offset:core_end]
        )
        payload_end = header_end + compressed_length
        assert payload_end <= len(encoded)
        ranges.append((offset, header_end, payload_end))
        offset = payload_end
    return ranges


def _rewrite_manifest_file_identity(tmp_path: Path, data_path: Path) -> None:
    [manifest_path] = tmp_path.glob("*.manifest.json")
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    encoded = data_path.read_bytes()
    manifest["compressed_bytes"] = len(encoded)
    manifest["sha256"] = hashlib.sha256(encoded).hexdigest()
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")


def _frame(
    ingest_seq: int,
    *,
    received_at_ms: int | None = None,
    process_boot_id: str = "boot-1",
    raw_payload: str = "{}",
) -> CaptureEnvelopeV1:
    receipt = 300_000 + ingest_seq if received_at_ms is None else received_at_ms
    return CaptureEnvelopeV1(
        received_at_ms=receipt,
        received_monotonic_ns=ingest_seq * 1_000,
        plan_sha256=PLAN_SHA256,
        process_boot_id=process_boot_id,
        connection_id="connection-1",
        frame_seq=ingest_seq,
        ingest_seq=ingest_seq,
        market=Market.SPOT,
        route="spot",
        stream="btcusdt@aggTrade",
        subscription_streams=("btcusdt@aggTrade",),
        raw_payload=raw_payload,
    )


def _writer(tmp_path: Path, **overrides: object) -> SegmentedCaptureWriter:
    arguments: dict[str, object] = {
        "plan_sha256": PLAN_SHA256,
        "process_boot_id": "boot-1",
        "maximum_total_bytes": 4 * 1024 * 1024,
        "emergency_reserve_bytes": 1024,
    }
    arguments.update(overrides)
    return SegmentedCaptureWriter(tmp_path, **arguments)  # pyright: ignore[reportArgumentType]


@pytest.mark.parametrize("rotation", ["time", "bytes", "frames"])
def test_writer_rotates_at_each_configured_boundary(tmp_path: Path, rotation: str) -> None:
    first = _frame(1, received_at_ms=599_999)
    second = _frame(2, received_at_ms=600_000 if rotation == "time" else 599_999)
    kwargs: dict[str, object] = {}
    if rotation == "bytes":
        kwargs["maximum_uncompressed_bytes"] = len(record_to_json_line(first))
    if rotation == "frames":
        kwargs["maximum_frames"] = 1
    writer = _writer(tmp_path, **kwargs)

    writer.append(first, record_to_json_line(first))
    writer.append(second, record_to_json_line(second))
    writer.close()

    manifests = verify_capture_segments(tmp_path)
    assert [item.sequence for item in manifests] == [1, 2]
    assert [item.record_count for item in manifests] == [1, 1]
    assert manifests[1].previous_segment_sha256 == manifests[0].sha256


def test_exact_byte_boundary_stays_in_current_segment(tmp_path: Path) -> None:
    frame = _frame(1)
    writer = _writer(
        tmp_path,
        maximum_uncompressed_bytes=len(record_to_json_line(frame)),
    )

    writer.append(frame, record_to_json_line(frame))
    writer.close()

    [manifest] = verify_capture_segments(tmp_path)
    assert manifest.uncompressed_bytes == len(record_to_json_line(frame))


def test_same_boot_ingest_gap_is_rejected_inside_segment(tmp_path: Path) -> None:
    writer = _writer(tmp_path)
    first = _frame(1)
    third = _frame(3)
    writer.append(first, record_to_json_line(first))

    with pytest.raises(CaptureIntegrityError, match="ingest sequence is not contiguous"):
        writer.append(third, record_to_json_line(third))
    writer.abort()


def test_verification_rejects_missing_initial_ingest_prefix(tmp_path: Path) -> None:
    writer = _writer(tmp_path)
    second = _frame(2)
    writer.append(second, record_to_json_line(second))
    writer.close()

    with pytest.raises(CaptureIntegrityError, match="initial ingest-sequence prefix"):
        verify_capture_segments(tmp_path)


def test_segment_hash_tamper_is_fatal(tmp_path: Path) -> None:
    frame = _frame(1)
    writer = _writer(tmp_path)
    writer.append(frame, record_to_json_line(frame))
    writer.close()
    [data_path] = tmp_path.glob("*.jsonl.zst")
    tampered = bytearray(data_path.read_bytes())
    tampered[-1] ^= 1
    data_path.write_bytes(tampered)

    with pytest.raises(CaptureIntegrityError, match=r"SHA-256|compressed size"):
        verify_capture_segments(tmp_path)


@pytest.mark.parametrize("tamper", ["torn", "corrupt"])
def test_finalized_segment_rejects_frame_damage_even_if_manifest_hash_is_rewritten(
    tmp_path: Path,
    tamper: str,
) -> None:
    writer = _writer(tmp_path)
    for frame in (_frame(1), _frame(2)):
        writer.append(frame, record_to_json_line(frame))
    writer.close()
    [data_path] = tmp_path.glob("*.jsonl.zst")
    encoded = bytearray(data_path.read_bytes())
    if tamper == "torn":
        encoded.pop()
        expected_error = "finalized capture segment has a torn frame"
    else:
        encoded[-1] ^= 1
        expected_error = "compressed SHA-256 mismatch"
    data_path.write_bytes(encoded)
    _rewrite_manifest_file_identity(tmp_path, data_path)

    with pytest.raises(CaptureIntegrityError, match=expected_error):
        verify_capture_segments(tmp_path)


@pytest.mark.parametrize("available_payload_bytes", [0, 1, -1])
def test_partial_recovery_seals_complete_prefix_and_drops_only_true_torn_payload(
    tmp_path: Path,
    available_payload_bytes: int,
) -> None:
    first = _frame(1)
    second = _frame(2)
    third = _frame(3)
    writer = _writer(tmp_path)
    writer.append(first, record_to_json_line(first))
    writer.append(second, record_to_json_line(second))
    writer.abort()
    [partial] = tmp_path.glob("*.jsonl.zst.partial")
    third_frame = _encode_test_outer_frame(record_to_json_line(third))
    compressed_length = len(third_frame) - _OUTER_FRAME_HEADER_SIZE
    available = (
        compressed_length - 1
        if available_payload_bytes == -1
        else available_payload_bytes
    )
    assert 0 <= available < compressed_length
    with partial.open("ab") as handle:
        handle.write(third_frame[: _OUTER_FRAME_HEADER_SIZE + available])

    recovered = SegmentedCaptureWriter(
        tmp_path,
        plan_sha256=PLAN_SHA256,
        process_boot_id="boot-2",
        maximum_total_bytes=4 * 1024 * 1024,
        emergency_reserve_bytes=1024,
    )
    recovered.close()

    [manifest] = verify_capture_segments(tmp_path)
    assert manifest.recovered_from_partial is True
    assert manifest.process_boot_id == "boot-1"
    assert manifest.record_count == 2
    [data_path] = tmp_path.glob("*.jsonl.zst")
    assert read_segment_lines(data_path) == [
        record_to_json_line(first),
        record_to_json_line(second),
    ]


def test_authenticated_absurd_payload_length_recovers_without_length_allocation(
    tmp_path: Path,
) -> None:
    first = _frame(1)
    writer = _writer(tmp_path)
    writer.append(first, record_to_json_line(first))
    writer.abort()
    [partial] = tmp_path.glob("*.jsonl.zst.partial")
    absurd_length = (1 << 64) - 1
    core = _OUTER_FRAME_HEADER_CORE.pack(
        _OUTER_FRAME_MAGIC,
        _OUTER_FRAME_FORMAT_VERSION,
        absurd_length,
        absurd_length,
        b"\x00" * hashlib.sha256().digest_size,
    )
    with partial.open("ab") as handle:
        handle.write(core + hashlib.sha256(core).digest())

    gc.collect()
    tracemalloc.start()
    try:
        recovered = SegmentedCaptureWriter(
            tmp_path,
            plan_sha256=PLAN_SHA256,
            process_boot_id="boot-2",
            maximum_total_bytes=4 * 1024 * 1024,
            emergency_reserve_bytes=1024,
        )
        recovered.close()
        _, peak_bytes = tracemalloc.get_traced_memory()
    finally:
        tracemalloc.stop()

    [manifest] = verify_capture_segments(tmp_path)
    assert manifest.record_count == 1
    assert peak_bytes < 4 * 1024 * 1024


def test_authenticated_absurd_uncompressed_length_fails_before_decoded_allocation(
    tmp_path: Path,
) -> None:
    writer = _writer(tmp_path)
    writer.append(_frame(1), record_to_json_line(_frame(1)))
    writer.abort()
    [partial] = tmp_path.glob("*.jsonl.zst.partial")
    encoded = bytearray(partial.read_bytes())
    frame_start, payload_start, payload_end = _outer_frame_ranges(encoded)[0]
    magic, version, compressed_length, _, compressed_sha256 = (
        _OUTER_FRAME_HEADER_CORE.unpack(
            encoded[frame_start : frame_start + _OUTER_FRAME_HEADER_CORE.size]
        )
    )
    core = _OUTER_FRAME_HEADER_CORE.pack(
        magic,
        version,
        compressed_length,
        (1 << 64) - 1,
        compressed_sha256,
    )
    core_end = frame_start + _OUTER_FRAME_HEADER_CORE.size
    encoded[frame_start:core_end] = core
    encoded[core_end:payload_start] = hashlib.sha256(core).digest()
    assert payload_end == len(encoded)
    partial.write_bytes(encoded)

    with pytest.raises(CaptureIntegrityError, match="content size differs"):
        SegmentedCaptureWriter(
            tmp_path,
            plan_sha256=PLAN_SHA256,
            process_boot_id="boot-2",
            maximum_total_bytes=4 * 1024 * 1024,
            emergency_reserve_bytes=1024,
        )


def test_middle_frame_corruption_is_not_repaired(tmp_path: Path) -> None:
    frames = [_frame(index) for index in (1, 2, 3)]
    writer = _writer(tmp_path)
    for frame in frames:
        writer.append(frame, record_to_json_line(frame))
    writer.abort()
    [partial] = tmp_path.glob("*.jsonl.zst.partial")
    payload = bytearray(partial.read_bytes())
    _, second_payload_start, second_payload_end = _outer_frame_ranges(payload)[1]
    payload[second_payload_start + (second_payload_end - second_payload_start) // 2] ^= 1
    partial.write_bytes(payload)

    with pytest.raises(CaptureIntegrityError, match="compressed SHA-256 mismatch"):
        SegmentedCaptureWriter(
            tmp_path,
            plan_sha256=PLAN_SHA256,
            process_boot_id="boot-2",
            maximum_total_bytes=4 * 1024 * 1024,
            emergency_reserve_bytes=1024,
        )


def test_final_frame_checksum_bitflip_is_not_torn_recovery(tmp_path: Path) -> None:
    first = _frame(1)
    second = _frame(2)
    writer = _writer(tmp_path)
    writer.append(first, record_to_json_line(first))
    writer.append(second, record_to_json_line(second))
    writer.abort()
    [partial] = tmp_path.glob("*.jsonl.zst.partial")
    payload = bytearray(partial.read_bytes())
    payload[-1] ^= 1
    partial.write_bytes(payload)

    with pytest.raises(CaptureIntegrityError, match="compressed SHA-256 mismatch"):
        SegmentedCaptureWriter(
            tmp_path,
            plan_sha256=PLAN_SHA256,
            process_boot_id="boot-2",
            maximum_total_bytes=4 * 1024 * 1024,
            emergency_reserve_bytes=1024,
        )


@pytest.mark.parametrize(
    "header_relative_offset",
    [
        pytest.param(0, id="magic"),
        pytest.param(8, id="version"),
        pytest.param(16, id="compressed-length"),
        pytest.param(24, id="uncompressed-length"),
        pytest.param(25, id="compressed-digest"),
        pytest.param(_OUTER_FRAME_HEADER_CORE.size, id="header-digest"),
    ],
)
def test_final_outer_header_bitflip_is_never_torn_recovery(
    tmp_path: Path,
    header_relative_offset: int,
) -> None:
    writer = _writer(tmp_path)
    for frame in (_frame(1), _frame(2)):
        writer.append(frame, record_to_json_line(frame))
    writer.abort()
    [partial] = tmp_path.glob("*.jsonl.zst.partial")
    encoded = bytearray(partial.read_bytes())
    final_start, _, _ = _outer_frame_ranges(encoded)[-1]
    encoded[final_start + header_relative_offset] ^= 1
    partial.write_bytes(encoded)

    with pytest.raises(CaptureIntegrityError, match="header digest mismatch"):
        SegmentedCaptureWriter(
            tmp_path,
            plan_sha256=PLAN_SHA256,
            process_boot_id="boot-2",
            maximum_total_bytes=4 * 1024 * 1024,
            emergency_reserve_bytes=1024,
        )


@pytest.mark.parametrize("header_bytes", [1, _OUTER_FRAME_HEADER_SIZE - 1])
def test_incomplete_final_outer_header_fails_closed(
    tmp_path: Path,
    header_bytes: int,
) -> None:
    first = _frame(1)
    writer = _writer(tmp_path)
    writer.append(first, record_to_json_line(first))
    writer.abort()
    [partial] = tmp_path.glob("*.jsonl.zst.partial")
    second_frame = _encode_test_outer_frame(record_to_json_line(_frame(2)))
    with partial.open("ab") as handle:
        handle.write(second_frame[:header_bytes])

    with pytest.raises(CaptureIntegrityError, match="header is incomplete or corrupt"):
        SegmentedCaptureWriter(
            tmp_path,
            plan_sha256=PLAN_SHA256,
            process_boot_id="boot-2",
            maximum_total_bytes=4 * 1024 * 1024,
            emergency_reserve_bytes=1024,
        )


@pytest.mark.parametrize(
    ("core_relative_offset", "expected_error"),
    [
        pytest.param(0, "magic is invalid", id="authenticated-invalid-magic"),
        pytest.param(8, "format version is unsupported", id="authenticated-version"),
    ],
)
def test_authenticated_but_unsupported_outer_header_is_rejected(
    tmp_path: Path,
    core_relative_offset: int,
    expected_error: str,
) -> None:
    writer = _writer(tmp_path)
    writer.append(_frame(1), record_to_json_line(_frame(1)))
    writer.append(_frame(2), record_to_json_line(_frame(2)))
    writer.abort()
    [partial] = tmp_path.glob("*.jsonl.zst.partial")
    encoded = bytearray(partial.read_bytes())
    frame_start, _, _ = _outer_frame_ranges(encoded)[-1]
    core_end = frame_start + _OUTER_FRAME_HEADER_CORE.size
    core = bytearray(encoded[frame_start:core_end])
    core[core_relative_offset] ^= 1
    encoded[frame_start:core_end] = core
    encoded[core_end : core_end + hashlib.sha256().digest_size] = hashlib.sha256(
        core
    ).digest()
    partial.write_bytes(encoded)

    with pytest.raises(CaptureIntegrityError, match=expected_error):
        SegmentedCaptureWriter(
            tmp_path,
            plan_sha256=PLAN_SHA256,
            process_boot_id="boot-2",
            maximum_total_bytes=4 * 1024 * 1024,
            emergency_reserve_bytes=1024,
        )


def test_inner_zstd_checksum_bitflip_is_rejected_after_outer_digests_are_updated(
    tmp_path: Path,
) -> None:
    writer = _writer(tmp_path)
    writer.append(_frame(1), record_to_json_line(_frame(1)))
    writer.append(_frame(2), record_to_json_line(_frame(2)))
    writer.abort()
    [partial] = tmp_path.glob("*.jsonl.zst.partial")
    encoded = bytearray(partial.read_bytes())
    frame_start, payload_start, payload_end = _outer_frame_ranges(encoded)[-1]
    encoded[payload_end - 1] ^= 1
    magic, version, compressed_length, uncompressed_length, _ = (
        _OUTER_FRAME_HEADER_CORE.unpack(
            encoded[frame_start : frame_start + _OUTER_FRAME_HEADER_CORE.size]
        )
    )
    compressed = encoded[payload_start:payload_end]
    core = _OUTER_FRAME_HEADER_CORE.pack(
        magic,
        version,
        compressed_length,
        uncompressed_length,
        hashlib.sha256(compressed).digest(),
    )
    core_end = frame_start + _OUTER_FRAME_HEADER_CORE.size
    encoded[frame_start:core_end] = core
    encoded[core_end:payload_start] = hashlib.sha256(core).digest()
    partial.write_bytes(encoded)

    with pytest.raises(CaptureIntegrityError, match="inner zstd checksum"):
        SegmentedCaptureWriter(
            tmp_path,
            plan_sha256=PLAN_SHA256,
            process_boot_id="boot-2",
            maximum_total_bytes=4 * 1024 * 1024,
            emergency_reserve_bytes=1024,
        )


def test_inner_zstd_block_header_bitflip_is_never_torn_recovery(
    tmp_path: Path,
) -> None:
    writer = _writer(tmp_path)
    writer.append(_frame(1), record_to_json_line(_frame(1)))
    writer.append(_frame(2), record_to_json_line(_frame(2)))
    writer.abort()
    [partial] = tmp_path.glob("*.jsonl.zst.partial")
    encoded = bytearray(partial.read_bytes())
    frame_start, payload_start, payload_end = _outer_frame_ranges(encoded)[-1]
    encoded[payload_start + 7] ^= 0x80
    magic, version, compressed_length, uncompressed_length, _ = (
        _OUTER_FRAME_HEADER_CORE.unpack(
            encoded[frame_start : frame_start + _OUTER_FRAME_HEADER_CORE.size]
        )
    )
    compressed = encoded[payload_start:payload_end]
    core = _OUTER_FRAME_HEADER_CORE.pack(
        magic,
        version,
        compressed_length,
        uncompressed_length,
        hashlib.sha256(compressed).digest(),
    )
    core_end = frame_start + _OUTER_FRAME_HEADER_CORE.size
    encoded[frame_start:core_end] = core
    encoded[core_end:payload_start] = hashlib.sha256(core).digest()
    partial.write_bytes(encoded)

    with pytest.raises(CaptureIntegrityError, match="torn inner zstd frame"):
        SegmentedCaptureWriter(
            tmp_path,
            plan_sha256=PLAN_SHA256,
            process_boot_id="boot-2",
            maximum_total_bytes=4 * 1024 * 1024,
            emergency_reserve_bytes=1024,
        )


def test_inner_zstd_frame_without_checksum_is_rejected(tmp_path: Path) -> None:
    first = _frame(1)
    writer = _writer(tmp_path)
    writer.append(first, record_to_json_line(first))
    writer.abort()
    [partial] = tmp_path.glob("*.jsonl.zst.partial")
    unchecked = _encode_test_outer_frame(
        record_to_json_line(_frame(2)),
        write_checksum=False,
    )
    with partial.open("ab") as handle:
        handle.write(unchecked)

    with pytest.raises(CaptureIntegrityError, match="inner zstd frame has no checksum"):
        SegmentedCaptureWriter(
            tmp_path,
            plan_sha256=PLAN_SHA256,
            process_boot_id="boot-2",
            maximum_total_bytes=4 * 1024 * 1024,
            emergency_reserve_bytes=1024,
        )


@pytest.mark.parametrize(
    ("decoded", "expected_error"),
    [
        pytest.param(b"{}", "exactly one JSONL record", id="missing-newline"),
        pytest.param(b"{}\n{}\n", "exactly one JSONL record", id="two-lines"),
        pytest.param(b"{]\n", "invalid JSON", id="invalid-json"),
    ],
)
def test_inner_jsonl_invariants_remain_fail_closed(
    tmp_path: Path,
    decoded: bytes,
    expected_error: str,
) -> None:
    first = _frame(1)
    writer = _writer(tmp_path)
    writer.append(first, record_to_json_line(first))
    writer.abort()
    [partial] = tmp_path.glob("*.jsonl.zst.partial")
    with partial.open("ab") as handle:
        handle.write(_encode_test_outer_frame(decoded))

    with pytest.raises(CaptureIntegrityError, match=expected_error):
        SegmentedCaptureWriter(
            tmp_path,
            plan_sha256=PLAN_SHA256,
            process_boot_id="boot-2",
            maximum_total_bytes=4 * 1024 * 1024,
            emergency_reserve_bytes=1024,
        )


def test_auxiliary_recovery_residue_is_rejected_for_manual_audit(tmp_path: Path) -> None:
    (tmp_path / "x.manifest.json.partial").write_text("{}", encoding="utf-8")
    with pytest.raises(CaptureIntegrityError, match="crash residue"):
        _writer(tmp_path)


def test_verifier_rejects_manifest_metadata_tamper_even_with_updated_file_hash(
    tmp_path: Path,
) -> None:
    frame = _frame(1)
    writer = _writer(tmp_path)
    writer.append(frame, record_to_json_line(frame))
    writer.close()
    [manifest_path] = tmp_path.glob("*.manifest.json")
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["record_count"] = 2
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")

    with pytest.raises(CaptureIntegrityError, match="record metadata"):
        verify_capture_segments(tmp_path)


@pytest.mark.parametrize("bad_version", [True, 1.0, 2, "1"])
def test_verifier_rejects_manifest_outer_frame_format_version_tamper(
    tmp_path: Path,
    bad_version: object,
) -> None:
    writer = _writer(tmp_path)
    frame = _frame(1)
    writer.append(frame, record_to_json_line(frame))
    writer.close()
    [manifest_path] = tmp_path.glob("*.manifest.json")
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    assert manifest["frame_format_version"] == 1
    manifest["frame_format_version"] = bad_version
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")

    with pytest.raises(CaptureIntegrityError, match="outer-frame format version"):
        verify_capture_segments(tmp_path)


def test_verifier_rejects_manifest_without_explicit_outer_frame_format_version(
    tmp_path: Path,
) -> None:
    writer = _writer(tmp_path)
    frame = _frame(1)
    writer.append(frame, record_to_json_line(frame))
    writer.close()
    [manifest_path] = tmp_path.glob("*.manifest.json")
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    del manifest["frame_format_version"]
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")

    with pytest.raises(CaptureIntegrityError, match="invalid segment manifest"):
        verify_capture_segments(tmp_path)


def test_verifier_memory_is_bounded_by_one_frame_not_segment_size(
    tmp_path: Path,
) -> None:
    frame_total = 1024
    writer = _writer(
        tmp_path,
        maximum_total_bytes=64 * 1024 * 1024,
    )
    for ingest_seq in range(1, frame_total + 1):
        raw_payload = random.Random(ingest_seq).randbytes(8192).hex()
        frame = _frame(ingest_seq, raw_payload=raw_payload)
        writer.append(frame, record_to_json_line(frame))
    writer.close()
    [data_path] = tmp_path.glob("*.jsonl.zst")
    segment_size = data_path.stat().st_size
    assert segment_size > 8 * 1024 * 1024

    gc.collect()
    tracemalloc.start()
    try:
        [manifest] = verify_capture_segments(tmp_path)
        _, peak_bytes = tracemalloc.get_traced_memory()
    finally:
        tracemalloc.stop()

    assert manifest.record_count == frame_total
    assert peak_bytes < 4 * 1024 * 1024
    assert peak_bytes * 2 < segment_size


def test_verifier_rejects_unfinished_tail_outside_recovery_owner(tmp_path: Path) -> None:
    frame = _frame(1)
    writer = _writer(tmp_path)
    writer.append(frame, record_to_json_line(frame))
    writer.abort()

    with pytest.raises(CaptureIntegrityError, match="unfinished segment tail"):
        verify_capture_segments(tmp_path)


def test_fatal_coverage_journal_prevents_false_successful_verification(tmp_path: Path) -> None:
    writer = _writer(tmp_path)
    writer.write_emergency_transition(
        invalidation_for_record(
            _frame(1),
            CoverageReason.STORAGE_CAPACITY,
            "synthetic fatal storage failure",
        )
    )

    with pytest.raises(CaptureIntegrityError, match="fatal coverage journal"):
        verify_capture_segments(tmp_path)


def test_recovered_orphan_is_reverified_against_previous_segment_boundary(
    tmp_path: Path,
) -> None:
    first = _frame(1, received_at_ms=300_001)
    writer = _writer(tmp_path)
    writer.append(first, record_to_json_line(first))
    writer.close()
    backwards = _frame(1, received_at_ms=300_000, process_boot_id="boot-2")
    partial = tmp_path / "0000000300000-00000002.jsonl.zst.partial"
    partial.write_bytes(_encode_test_outer_frame(record_to_json_line(backwards)))

    with pytest.raises(CaptureIntegrityError, match="receipt order crosses"):
        SegmentedCaptureWriter(
            tmp_path,
            plan_sha256=PLAN_SHA256,
            process_boot_id="boot-3",
            maximum_total_bytes=4 * 1024 * 1024,
            emergency_reserve_bytes=1024,
        )


def test_recovery_disabled_rejects_finalized_orphan_tail(tmp_path: Path) -> None:
    orphan = tmp_path / "0000000300000-00000001.jsonl.zst"
    orphan.write_bytes(_encode_test_outer_frame(record_to_json_line(_frame(1))))

    with pytest.raises(CaptureIntegrityError, match="unfinished capture tail"):
        SegmentedCaptureWriter(
            tmp_path,
            plan_sha256=PLAN_SHA256,
            process_boot_id="boot-2",
            maximum_total_bytes=4 * 1024 * 1024,
            emergency_reserve_bytes=1024,
            recover_partials=False,
        )


def test_finalized_orphan_with_true_torn_payload_is_not_recovered(tmp_path: Path) -> None:
    orphan = tmp_path / "0000000300000-00000001.jsonl.zst"
    encoded = _encode_test_outer_frame(record_to_json_line(_frame(1)))
    orphan.write_bytes(encoded[:-1])

    with pytest.raises(CaptureIntegrityError, match="finalized orphan contains a torn"):
        SegmentedCaptureWriter(
            tmp_path,
            plan_sha256=PLAN_SHA256,
            process_boot_id="boot-2",
            maximum_total_bytes=4 * 1024 * 1024,
            emergency_reserve_bytes=1024,
        )
