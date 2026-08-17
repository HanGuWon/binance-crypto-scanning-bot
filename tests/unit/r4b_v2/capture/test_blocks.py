from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass, replace
from pathlib import Path

import pytest
import zstandard as zstd

from signalbot.r4b_v2.canonical import canonical_json_line
from signalbot.r4b_v2.capture.authority import StorageRootBindingV2
from signalbot.r4b_v2.capture.batching import QueuedRawRecordV2
from signalbot.r4b_v2.capture.block_container import (
    BLOCK_COMMIT_MARKER_V2,
    BLOCK_MAGIC_V2,
    BlockSigningAuthorityV2,
    Ed25519BlockSignerV2,
    SignedBlockContainerError,
    parse_and_verify_signed_block_container_v2,
)
from signalbot.r4b_v2.capture.blocks import (
    BlockCapacityError,
    BlockError,
    BlockIntegrityError,
    BlockManifestV2,
    BlockPolicyV2,
    CaptureBlockV2,
    GroupedBlockBuilderV2,
    GroupedBlockWriterV2,
    grouped_block_root_contract_v2,
    record_merkle_root,
    verify_grouped_blocks,
)
from signalbot.r4b_v2.capture.models import RawRecordV2, TransportV2, VenueV2
from signalbot.r4b_v2.capture.wal import WalAuthorityV2

HASH = "a" * 64
STREAM_GROUP_ID = "futures-depth-group"
SEGMENT_ID = "segment-000001"


def _signer(*, seed: int = 1, key_id: str = "writer-key-1") -> Ed25519BlockSignerV2:
    return Ed25519BlockSignerV2.from_private_key_bytes(
        key_id=key_id,
        private_key_bytes=bytes([seed]) * 32,
    )


def _signing_authority(
    *,
    seed: int = 1,
    key_id: str = "writer-key-1",
) -> BlockSigningAuthorityV2:
    signer = _signer(seed=seed, key_id=key_id)
    return BlockSigningAuthorityV2.from_public_key_bytes(
        key_id=key_id,
        public_key_bytes=signer.public_key_bytes,
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


def _policy(**overrides: int | str) -> BlockPolicyV2:
    values: dict[str, int | str] = {
        "qualification_id": "sealed-zstd-1.5.7-l9",
        "codec_candidate_id": "zstd-1.5.7-l9-w0-checksum-content-size",
        "compression_level": 9,
        "max_uncompressed_bytes": 4_194_304,
        "max_linger_ms": 1_000,
    }
    values.update(overrides)
    return BlockPolicyV2(**values)  # type: ignore[arg-type]


@dataclass(frozen=True, slots=True)
class _FakeQueued:
    record: RawRecordV2
    ingest_seq: int
    encoded_line: bytes
    encoded_len: int
    encoded_sha256: str

    def verify_integrity(self) -> None:
        assert len(self.encoded_line) == self.encoded_len
        assert hashlib.sha256(self.encoded_line).hexdigest() == self.encoded_sha256


def _fake_queued(ingest_seq: int, encoded_len: int) -> _FakeQueued:
    if encoded_len < 1:
        raise ValueError("encoded_len must be positive")
    encoded = (b"x" * (encoded_len - 1)) + b"\n"
    return _FakeQueued(
        record=_queued(ingest_seq).record,
        ingest_seq=ingest_seq,
        encoded_line=encoded,
        encoded_len=encoded_len,
        encoded_sha256=hashlib.sha256(encoded).hexdigest(),
    )


def _queued(
    ingest_seq: int,
    *,
    payload_size: int = 0,
    receipt_wall_ms: int | None = None,
    receipt_monotonic_ns: int | None = None,
) -> QueuedRawRecordV2:
    wall = 1_000 + ingest_seq if receipt_wall_ms is None else receipt_wall_ms
    monotonic = (
        1_000_000 + ingest_seq
        if receipt_monotonic_ns is None
        else receipt_monotonic_ns
    )
    record = RawRecordV2.from_payload(
        session_id="session-1",
        plan_id="plan-1",
        protocol_hash=HASH,
        transport=TransportV2.WEBSOCKET,
        venue=VenueV2.USDM_FUTURES,
        route_id="futures-market",
        symbol="BTCUSDT",
        connection_id="connection-1",
        generation=1,
        frame_seq=ingest_seq,
        ingest_seq=ingest_seq,
        receipt_wall_ms=wall,
        receipt_monotonic_ns=monotonic,
        raw_payload='{"payload":"' + "x" * payload_size + '"}',
        source_logical_key=f"trade-{ingest_seq}",
    )
    return QueuedRawRecordV2.encode(
        record,
        enqueued_monotonic_ns=monotonic + 1,
    )


def _writer(
    tmp_path: Path,
    *,
    policy: BlockPolicyV2 | None = None,
    fault_hook: object | None = None,
    signer: Ed25519BlockSignerV2 | None = None,
    signing_authority: BlockSigningAuthorityV2 | None = None,
    stream_group_id: str = STREAM_GROUP_ID,
    segment_id: str = SEGMENT_ID,
    verification_only: bool = False,
) -> GroupedBlockWriterV2:
    return GroupedBlockWriterV2(
        tmp_path,
        authority=_authority(),
        policy=policy or _policy(),
        signer=signer or _signer(),
        signing_authority=signing_authority or _signing_authority(),
        stream_group_id=stream_group_id,
        segment_id=segment_id,
        maximum_total_bytes=8 * 1024 * 1024,
        emergency_reserve_bytes=1024,
        fault_hook=fault_hook,  # type: ignore[arg-type]
        verification_only=verification_only,
    )


def _verify(
    tmp_path: Path,
    *,
    policy: BlockPolicyV2,
    authority: WalAuthorityV2 | None = None,
    signing_authority: BlockSigningAuthorityV2 | None = None,
):  # type: ignore[no-untyped-def]
    return verify_grouped_blocks(
        tmp_path,
        authority=authority or _authority(),
        policy=policy,
        signing_authority=signing_authority or _signing_authority(),
        stream_group_id=STREAM_GROUP_ID,
        segment_id=SEGMENT_ID,
    )


def _tail_block(
    records: list[QueuedRawRecordV2],
    policy: BlockPolicyV2,
):  # type: ignore[no-untyped-def]
    builder = GroupedBlockBuilderV2(policy)
    now = 10_000_000
    completed = []
    for record in records:
        completed.extend(builder.offer(record, now_ns=now))
        now += 1
    tail = builder.flush_tail(now_ns=now)
    if tail is not None:
        completed.append(tail)
    assert len(completed) == 1
    return completed[0]


@dataclass(frozen=True, slots=True)
class _FakeBlockFinality:
    sha256: str
    authority_sha256: str
    attempt_id: str
    qualification_id: str
    grouped_block_root_binding: StorageRootBindingV2
    grouped_block_root_binding_sha256: str
    block_signing_authority_sha256: str
    stream_group_id: str
    segment_id: str
    fence_ingest_seq: int
    exact_prefix_sha256: str
    prefix_proof_sha256: str
    final_block_sequence: int
    final_block_hash: str
    final_block_manifest_sha256: str
    final_block_container_sha256: str
    target_last_receipt_wall_ms: int
    target_last_receipt_monotonic_ns: int


def _fake_block_finality(
    writer: GroupedBlockWriterV2,
    manifest: BlockManifestV2,
) -> _FakeBlockFinality:
    return _FakeBlockFinality(
        sha256="1" * 64,
        authority_sha256=writer.authority.sha256,
        attempt_id=writer.authority.attempt_id,
        qualification_id=writer.policy.qualification_id,
        grouped_block_root_binding=writer.root_binding,
        grouped_block_root_binding_sha256=hashlib.sha256(
            canonical_json_line(asdict(writer.root_binding))
        ).hexdigest(),
        block_signing_authority_sha256=writer.signing_authority.sha256,
        stream_group_id=writer.stream_group_id,
        segment_id=writer.segment_id,
        fence_ingest_seq=manifest.last_ingest_seq,
        exact_prefix_sha256="2" * 64,
        prefix_proof_sha256="3" * 64,
        final_block_sequence=manifest.block_sequence,
        final_block_hash=manifest.block_hash,
        final_block_manifest_sha256=hashlib.sha256(
            canonical_json_line(asdict(manifest))
        ).hexdigest(),
        final_block_container_sha256=manifest.container_sha256,
        target_last_receipt_wall_ms=manifest.last_receipt_wall_ms,
        target_last_receipt_monotonic_ns=manifest.last_receipt_monotonic_ns,
    )


def test_only_selected_zstd_codec_and_boundaries_are_accepted() -> None:
    policy = _policy()
    assert policy.compression_level == 9
    assert policy.max_uncompressed_bytes == 4_194_304
    assert policy.max_linger_ms == 1_000
    assert zstd.ZSTD_VERSION == (1, 5, 7)


@pytest.mark.parametrize("level", [0, 3, 6, 7, 22])
def test_unsealed_codec_level_is_rejected(level: int) -> None:
    with pytest.raises(ValueError, match="level 9"):
        _policy(compression_level=level)


@pytest.mark.parametrize(
    ("overrides", "message"),
    [
        ({"max_uncompressed_bytes": 4_194_303}, "4194304"),
        ({"max_uncompressed_bytes": 4_194_305}, "4194304"),
        ({"max_linger_ms": 999}, "1000"),
        ({"max_linger_ms": 1001}, "1000"),
    ],
)
def test_unsealed_block_close_boundary_is_rejected(
    overrides: dict[str, int | str],
    message: str,
) -> None:
    with pytest.raises(ValueError, match=message):
        _policy(**overrides)


def test_builder_closes_at_exact_byte_boundary() -> None:
    first = _fake_queued(1, 1_000_000)
    second = _fake_queued(2, 3_194_304)
    builder = GroupedBlockBuilderV2(_policy())
    assert builder.offer(first, now_ns=0) == ()
    [block] = builder.offer(second, now_ns=1)
    assert block.uncompressed_bytes == 4_194_304
    assert block.close_reason == "max_bytes"


def test_next_record_above_byte_boundary_closes_existing_prefix() -> None:
    first = _fake_queued(1, 2_000_000)
    second = _fake_queued(2, 2_194_305)
    builder = GroupedBlockBuilderV2(_policy())
    assert builder.offer(first, now_ns=0) == ()
    [block] = builder.offer(second, now_ns=1)
    assert block.records == (first,)
    tail = builder.flush_tail(now_ns=2)
    assert tail is not None and tail.records == (second,)


def test_single_record_above_bound_fails_before_buffering() -> None:
    record = _fake_queued(1, 4_194_305)
    builder = GroupedBlockBuilderV2(_policy())
    with pytest.raises(BlockCapacityError, match="single record"):
        builder.offer(record, now_ns=0)
    assert builder.flush_tail(now_ns=1) is None


def test_linger_boundary_and_clean_small_tail() -> None:
    builder = GroupedBlockBuilderV2(_policy())
    builder.offer(_queued(1), now_ns=10)
    assert builder.flush_due(now_ns=1_000_000_009) is None
    block = builder.flush_due(now_ns=1_000_000_010)
    assert block is not None and block.close_reason == "max_linger"
    assert builder.flush_tail(now_ns=1_000_000_011) is None


def test_record_count_does_not_add_an_unsealed_close_trigger() -> None:
    builder = GroupedBlockBuilderV2(_policy())
    assert builder.offer(_queued(1), now_ns=0) == ()
    assert builder.offer(_queued(2), now_ns=1) == ()
    tail = builder.flush_tail(now_ns=2)
    assert tail is not None and tail.record_count == 2
    assert tail.close_reason == "clean_shutdown"


def test_empty_finality_fence_is_a_noop_at_zero_clock_boundary() -> None:
    builder = GroupedBlockBuilderV2(_policy())
    assert builder.flush_finality_fence(now_ns=0) is None
    assert builder.flush_finality_fence(now_ns=0) is None


def test_finality_fence_forces_the_exact_current_prefix_before_linger() -> None:
    records = (_queued(1), _queued(2))
    builder = GroupedBlockBuilderV2(_policy())
    assert builder.offer(records[0], now_ns=10) == ()
    assert builder.offer(records[1], now_ns=11) == ()

    block = builder.flush_finality_fence(now_ns=11)

    assert block is not None
    assert block.records == records
    assert (block.first_ingest_seq, block.last_ingest_seq) == (1, 2)
    assert (block.opened_monotonic_ns, block.closed_monotonic_ns) == (10, 11)
    assert block.close_reason == "causal_finality_fence"
    assert builder.flush_tail(now_ns=11) is None


def test_builder_continues_contiguous_append_after_finality_fence() -> None:
    builder = GroupedBlockBuilderV2(_policy())
    assert builder.offer(_queued(1), now_ns=10) == ()
    first = builder.flush_finality_fence(now_ns=10)
    assert first is not None and first.last_ingest_seq == 1

    assert builder.offer(_queued(2), now_ns=10) == ()
    second = builder.flush_finality_fence(now_ns=11)

    assert second is not None
    assert (second.first_ingest_seq, second.last_ingest_seq) == (2, 2)
    assert (second.opened_monotonic_ns, second.closed_monotonic_ns) == (10, 11)
    assert second.close_reason == "causal_finality_fence"


def test_explicit_finality_fence_keeps_its_reason_at_exact_linger_boundary() -> None:
    builder = GroupedBlockBuilderV2(_policy())
    builder.offer(_queued(1), now_ns=10)

    block = builder.flush_finality_fence(now_ns=1_000_000_010)

    assert block is not None
    assert block.closed_monotonic_ns == 1_000_000_010
    assert block.close_reason == "causal_finality_fence"


def test_finality_fence_reason_is_in_the_sealed_public_root_contract() -> None:
    contract = grouped_block_root_contract_v2(
        _policy(),
        _signing_authority(),
        STREAM_GROUP_ID,
        SEGMENT_ID,
    )
    assert contract["close_reasons"] == (
        "causal_finality_fence",
        "clean_shutdown",
        "max_bytes",
        "max_linger",
        "next_record_bound",
        "wal_recovery_tail",
    )


def test_capture_block_rejects_unknown_close_reason() -> None:
    with pytest.raises(ValueError, match="close reason is not in the sealed set"):
        CaptureBlockV2(
            records=(_queued(1),),
            opened_monotonic_ns=10,
            closed_monotonic_ns=10,
            close_reason="manual_flush",
        )


def test_builder_rejects_ingest_and_clock_regression() -> None:
    builder = GroupedBlockBuilderV2(_policy())
    builder.offer(_queued(1), now_ns=10)
    with pytest.raises(BlockIntegrityError, match="ingest sequence"):
        builder.offer(_queued(3), now_ns=11)
    with pytest.raises(BlockIntegrityError, match="clock moved backwards"):
        builder.flush_tail(now_ns=9)


def test_committed_data_is_signed_container_with_exact_jsonl_payload(tmp_path: Path) -> None:
    policy = _policy()
    records = [_queued(1), _queued(2, payload_size=50)]
    block = _tail_block(records, policy)
    writer = _writer(tmp_path, policy=policy)
    manifest = writer.commit(block)
    data_path = tmp_path / manifest.data_file
    encoded = data_path.read_bytes()
    assert encoded.startswith(BLOCK_MAGIC_V2)
    assert encoded.endswith(BLOCK_COMMIT_MARKER_V2)
    container = parse_and_verify_signed_block_container_v2(
        encoded,
        signing_authority=_signing_authority(),
    )
    decoded = zstd.ZstdDecompressor().decompress(container.compressed)
    assert decoded == b"".join(record.encoded_line for record in records)
    assert manifest.uncompressed_sha256 == hashlib.sha256(decoded).hexdigest()
    assert manifest.record_merkle_root == record_merkle_root(
        [record.encoded_line for record in records]
    )
    [verified] = _verify(tmp_path, policy=policy)
    assert verified.block_hash == manifest.block_hash


def test_blocks_chain_by_block_hash_and_ingest_prefix(tmp_path: Path) -> None:
    policy = _policy()
    writer = _writer(tmp_path, policy=policy)
    first = writer.commit(_tail_block([_queued(1)], policy))
    second = writer.commit(_tail_block([_queued(2)], policy))
    assert second.previous_block_hash == first.block_hash
    manifests = _verify(tmp_path, policy=policy)
    assert [item.block_sequence for item in manifests] == [1, 2]


def test_clean_tail_terminal_is_exact_idempotent_and_blocks_commits(
    tmp_path: Path,
) -> None:
    policy = _policy()
    writer = _writer(tmp_path, policy=policy)
    manifest = writer.commit(_tail_block([_queued(1)], policy))
    finality = _fake_block_finality(writer, manifest)

    terminal = writer.terminalize_clean_tail_v2(finality)

    assert writer.terminalize_clean_tail_v2(finality) == terminal
    assert (
        writer.assert_clean_tail_terminal_and_current_v2(finality)
        == terminal.sha256
    )
    with pytest.raises(BlockIntegrityError, match="irreversibly clean-tail terminal"):
        writer.commit(_tail_block([_queued(2)], policy))
    with pytest.raises(BlockIntegrityError, match="differs from finality"):
        writer.assert_clean_tail_terminal_and_current_v2(
            replace(finality, sha256="f" * 64)
        )


def test_clean_tail_terminal_survives_writable_and_verification_only_reopen(
    tmp_path: Path,
) -> None:
    policy = _policy()
    writer = _writer(tmp_path, policy=policy)
    manifest = writer.commit(_tail_block([_queued(1)], policy))
    finality = _fake_block_finality(writer, manifest)
    expected_sha256 = writer.terminalize_clean_tail_v2(finality).sha256

    reopened = _writer(tmp_path, policy=policy)
    verifier = _writer(tmp_path, policy=policy, verification_only=True)

    assert (
        reopened.assert_clean_tail_terminal_and_current_v2(finality)
        == expected_sha256
    )
    assert (
        verifier.assert_clean_tail_terminal_and_current_v2(finality)
        == expected_sha256
    )
    with pytest.raises(BlockIntegrityError, match="irreversibly clean-tail terminal"):
        reopened.commit(_tail_block([_queued(2)], policy))
    with pytest.raises(BlockIntegrityError, match="verification-only"):
        verifier.commit(_tail_block([_queued(2)], policy))


def test_verification_only_open_never_creates_missing_root_binding(
    tmp_path: Path,
) -> None:
    empty = tmp_path / "empty"
    empty.mkdir()

    with pytest.raises(BlockIntegrityError, match="binding must already exist"):
        _writer(empty, verification_only=True)

    assert tuple(empty.iterdir()) == ()


def test_verification_only_rejects_partial_terminal_without_recovery(
    tmp_path: Path,
) -> None:
    policy = _policy()

    def crash_after_terminal_fsync(point: str) -> None:
        if point == "after_clean_tail_terminal_fsync":
            raise OSError("synthetic terminal crash")

    writer = _writer(tmp_path, policy=policy, fault_hook=crash_after_terminal_fsync)
    manifest = writer.commit(_tail_block([_queued(1)], policy))
    finality = _fake_block_finality(writer, manifest)
    partial = tmp_path / "block-clean-tail-terminal.json.partial"

    with pytest.raises(OSError, match="synthetic terminal crash"):
        writer.terminalize_clean_tail_v2(finality)
    assert partial.is_file()
    with pytest.raises(BlockIntegrityError, match="rejects partial terminal"):
        _writer(tmp_path, policy=policy, verification_only=True)
    assert partial.is_file()

    recovered = _writer(tmp_path, policy=policy)
    assert not partial.exists()
    assert recovered.assert_clean_tail_terminal_and_current_v2(finality)


def test_terminal_reopen_never_recovers_a_later_block_orphan(tmp_path: Path) -> None:
    policy = _policy()
    writer = _writer(tmp_path, policy=policy)
    manifest = writer.commit(_tail_block([_queued(1)], policy))
    finality = _fake_block_finality(writer, manifest)
    writer.terminalize_clean_tail_v2(finality)
    orphan = tmp_path / "block-00000002.r4bblk.partial"
    orphan.write_bytes(b"must-not-be-recovered")

    with pytest.raises(BlockIntegrityError, match="unfinished partial"):
        _writer(tmp_path, policy=policy)

    assert orphan.read_bytes() == b"must-not-be-recovered"
    assert not (tmp_path / "block-00000002.r4bblk").exists()
    assert not (tmp_path / "block-00000002.manifest.json").exists()


@pytest.mark.parametrize(
    ("field", "value", "message"),
    [
        ("fence_ingest_seq", 0, "positive integer"),
        ("final_block_sequence", 2, "exact current tail"),
        ("final_block_hash", "g" * 64, "SHA-256"),
    ],
)
def test_clean_tail_terminal_rejects_boundary_and_tail_drift(
    tmp_path: Path,
    field: str,
    value: object,
    message: str,
) -> None:
    policy = _policy()
    writer = _writer(tmp_path, policy=policy)
    manifest = writer.commit(_tail_block([_queued(1)], policy))
    finality = replace(_fake_block_finality(writer, manifest), **{field: value})

    with pytest.raises((BlockIntegrityError, ValueError), match=message):
        writer.terminalize_clean_tail_v2(finality)
    assert not (tmp_path / "block-clean-tail-terminal.json").exists()


def test_adjacent_blocks_allow_equal_receipt_boundaries(tmp_path: Path) -> None:
    policy = _policy()
    writer = _writer(tmp_path, policy=policy)
    writer.commit(
        _tail_block(
            [_queued(1, receipt_wall_ms=100, receipt_monotonic_ns=1_000)],
            policy,
        )
    )
    writer.commit(
        _tail_block(
            [_queued(2, receipt_wall_ms=100, receipt_monotonic_ns=1_000)],
            policy,
        )
    )

    manifests = _verify(tmp_path, policy=policy)

    assert len(manifests) == 2
    assert manifests[0].last_receipt_wall_ms == manifests[1].first_receipt_wall_ms
    assert (
        manifests[0].last_receipt_monotonic_ns
        == manifests[1].first_receipt_monotonic_ns
    )


def test_writer_rejects_adjacent_wall_regression_before_second_block_write(
    tmp_path: Path,
) -> None:
    policy = _policy()
    writer = _writer(tmp_path, policy=policy)
    writer.commit(
        _tail_block(
            [_queued(1, receipt_wall_ms=100, receipt_monotonic_ns=1_000)],
            policy,
        )
    )
    second = _tail_block(
        [_queued(2, receipt_wall_ms=99, receipt_monotonic_ns=1_001)],
        policy,
    )

    with pytest.raises(BlockIntegrityError, match="wall time moved backwards"):
        writer.commit(second)
    assert writer.next_block_sequence == 2
    assert not list(tmp_path.glob("block-00000002*"))


def test_writer_rejects_adjacent_monotonic_regression_before_second_block_write(
    tmp_path: Path,
) -> None:
    policy = _policy()
    writer = _writer(tmp_path, policy=policy)
    writer.commit(
        _tail_block(
            [_queued(1, receipt_wall_ms=100, receipt_monotonic_ns=1_000)],
            policy,
        )
    )
    second = _tail_block(
        [_queued(2, receipt_wall_ms=101, receipt_monotonic_ns=999)],
        policy,
    )

    with pytest.raises(
        BlockIntegrityError,
        match="monotonic time moved backwards",
    ):
        writer.commit(second)
    assert writer.next_block_sequence == 2
    assert not list(tmp_path.glob("block-00000002*"))


def test_verifier_rejects_adjacent_receipt_wall_regression(tmp_path: Path) -> None:
    policy = _policy()
    writer = _writer(tmp_path, policy=policy)
    first = writer.commit(
        _tail_block(
            [_queued(1, receipt_wall_ms=100, receipt_monotonic_ns=1_000)],
            policy,
        )
    )
    # Model a chain created before the commit-time guard while preserving its
    # valid signed bytes on disk; the public verifier must remain fail-closed.
    writer._manifests[-1] = replace(first, last_receipt_wall_ms=98)
    writer.commit(
        _tail_block(
            [_queued(2, receipt_wall_ms=99, receipt_monotonic_ns=1_001)],
            policy,
        )
    )

    with pytest.raises(BlockIntegrityError, match="wall time moved backwards across blocks"):
        _verify(tmp_path, policy=policy)


def test_verifier_rejects_adjacent_receipt_monotonic_regression(
    tmp_path: Path,
) -> None:
    policy = _policy()
    writer = _writer(tmp_path, policy=policy)
    first = writer.commit(
        _tail_block(
            [_queued(1, receipt_wall_ms=100, receipt_monotonic_ns=1_000)],
            policy,
        )
    )
    writer._manifests[-1] = replace(first, last_receipt_monotonic_ns=998)
    writer.commit(
        _tail_block(
            [_queued(2, receipt_wall_ms=101, receipt_monotonic_ns=999)],
            policy,
        )
    )

    with pytest.raises(
        BlockIntegrityError,
        match="monotonic time moved backwards across blocks",
    ):
        _verify(tmp_path, policy=policy)


def test_complete_compressed_corruption_is_never_repaired(tmp_path: Path) -> None:
    policy = _policy()
    writer = _writer(tmp_path, policy=policy)
    manifest = writer.commit(_tail_block([_queued(1)], policy))
    path = tmp_path / manifest.data_file
    original = path.read_bytes()
    container = parse_and_verify_signed_block_container_v2(
        original,
        signing_authority=_signing_authority(),
    )
    compressed_offset = original.index(container.compressed)
    corrupted = bytearray(original)
    corrupted[compressed_offset + len(container.compressed) // 2] ^= 1
    path.write_bytes(corrupted)
    with pytest.raises(BlockIntegrityError):
        _verify(tmp_path, policy=policy)
    assert path.read_bytes() == corrupted


def test_manifest_metadata_tamper_is_rejected(tmp_path: Path) -> None:
    policy = _policy()
    writer = _writer(tmp_path, policy=policy)
    writer.commit(_tail_block([_queued(1)], policy))
    manifest_path = tmp_path / "block-00000001.manifest.json"
    raw = json.loads(manifest_path.read_text(encoding="utf-8"))
    raw["record_count"] = 2
    manifest_path.write_bytes(canonical_json_line(raw))
    with pytest.raises(BlockIntegrityError, match="metadata differs"):
        _verify(tmp_path, policy=policy)


def test_crash_after_rename_recovers_verified_final_orphan(tmp_path: Path) -> None:
    policy = _policy()

    def crash(point: str) -> None:
        if point == "after_block_rename":
            raise RuntimeError("synthetic rename crash")

    writer = _writer(tmp_path, policy=policy, fault_hook=crash)
    with pytest.raises(RuntimeError, match="rename crash"):
        writer.commit(_tail_block([_queued(1)], policy))
    assert (tmp_path / "block-00000001.r4bblk").exists()
    assert not (tmp_path / "block-00000001.manifest.json").exists()

    recovered = _writer(tmp_path, policy=policy)
    assert recovered.next_ingest_seq == 2
    assert (tmp_path / "block-00000001.recovery.json").exists()
    _verify(tmp_path, policy=policy)


@pytest.mark.parametrize("fault_point", ["after_block_write", "after_block_rename"])
def test_mutating_block_fault_permanently_latches_writer(
    tmp_path: Path,
    fault_point: str,
) -> None:
    policy = _policy()

    def crash(point: str) -> None:
        if point == fault_point:
            raise OSError(f"synthetic {fault_point}")

    writer = _writer(tmp_path, policy=policy, fault_hook=crash)
    block = _tail_block([_queued(1)], policy)
    with pytest.raises(OSError, match=fault_point):
        writer.commit(block)
    with pytest.raises(BlockError, match="failed and cannot be reused"):
        writer.commit(block)


def test_stale_recovery_receipt_cannot_mutate_or_launder_partial(tmp_path: Path) -> None:
    policy = _policy()

    def crash(point: str) -> None:
        if point == "after_block_write":
            raise RuntimeError("synthetic write crash")

    writer = _writer(tmp_path, policy=policy, fault_hook=crash)
    with pytest.raises(RuntimeError, match="write crash"):
        writer.commit(_tail_block([_queued(1)], policy))
    receipt = tmp_path / "block-00000001.recovery.json"
    receipt.write_text("{}\n", encoding="utf-8")

    for _ in range(2):
        with pytest.raises(BlockIntegrityError, match="recovery receipt already exists"):
            _writer(tmp_path, policy=policy)
        assert (tmp_path / "block-00000001.r4bblk.partial").exists()
        assert not (tmp_path / "block-00000001.r4bblk").exists()
        assert not (tmp_path / "block-00000001.manifest.json").exists()


def test_block_orphan_cannot_be_laundered_into_a_new_authority(tmp_path: Path) -> None:
    policy = _policy()

    def crash(point: str) -> None:
        if point == "after_block_rename":
            raise RuntimeError("synthetic rename crash")

    writer = _writer(tmp_path, policy=policy, fault_hook=crash)
    with pytest.raises(RuntimeError, match="rename crash"):
        writer.commit(_tail_block([_queued(1)], policy))

    with pytest.raises(BlockIntegrityError, match="root binding differs"):
        GroupedBlockWriterV2(
            tmp_path,
            authority=_other_authority(),
            policy=policy,
            signer=_signer(),
            signing_authority=_signing_authority(),
            stream_group_id=STREAM_GROUP_ID,
            segment_id=SEGMENT_ID,
            maximum_total_bytes=8 * 1024 * 1024,
            emergency_reserve_bytes=1024,
        )
    assert not (tmp_path / "block-00000001.manifest.json").exists()


def test_missing_root_binding_never_rebinds_nonempty_block_root(tmp_path: Path) -> None:
    policy = _policy()
    writer = _writer(tmp_path, policy=policy)
    writer.commit(_tail_block([_queued(1)], policy))
    (tmp_path / "storage-root-binding.json").unlink()

    with pytest.raises(BlockIntegrityError, match="non-empty storage root"):
        _writer(tmp_path, policy=policy)


def test_crash_after_complete_partial_recovers_before_rename(tmp_path: Path) -> None:
    policy = _policy()

    def crash(point: str) -> None:
        if point == "after_block_fsync":
            raise RuntimeError("synthetic fsync crash")

    writer = _writer(tmp_path, policy=policy, fault_hook=crash)
    with pytest.raises(RuntimeError, match="fsync crash"):
        writer.commit(_tail_block([_queued(1)], policy))
    [partial] = tmp_path.glob("*.r4bblk.partial")
    original = partial.read_bytes()

    recovered = _writer(tmp_path, policy=policy)
    final = tmp_path / "block-00000001.r4bblk"
    assert final.read_bytes() == original
    assert recovered.next_ingest_seq == 2
    _verify(tmp_path, policy=policy)


def test_incomplete_partial_is_not_silently_repaired(tmp_path: Path) -> None:
    _writer(tmp_path)
    partial = tmp_path / "block-00000001.r4bblk.partial"
    partial.write_bytes(b"not-a-complete-zstd-frame")
    with pytest.raises(BlockIntegrityError, match="truncated"):
        _writer(tmp_path)
    assert partial.read_bytes() == b"not-a-complete-zstd-frame"
    assert not (tmp_path / "block-00000001.r4bblk").exists()


def test_authority_change_rejects_existing_blocks(tmp_path: Path) -> None:
    policy = _policy()
    writer = _writer(tmp_path, policy=policy)
    writer.commit(_tail_block([_queued(1)], policy))
    wrong = WalAuthorityV2(
        attempt_id="attempt-2",
        protocol_sha256=HASH,
        plan_sha256="b" * 64,
        source_manifest_sha256="c" * 64,
        schema_sha256="d" * 64,
        runtime_manifest_sha256="e" * 64,
    )
    with pytest.raises(BlockIntegrityError, match="authority differs"):
        _verify(tmp_path, authority=wrong, policy=policy)


def test_embedded_header_trailer_and_out_of_band_key_contract(tmp_path: Path) -> None:
    policy = _policy()
    records = [_queued(1), _queued(2, payload_size=20)]
    manifest = _writer(tmp_path, policy=policy).commit(_tail_block(records, policy))
    encoded = (tmp_path / manifest.data_file).read_bytes()
    authority = _signing_authority()
    container = parse_and_verify_signed_block_container_v2(
        encoded,
        signing_authority=authority,
    )

    header = container.header
    trailer = container.trailer
    assert header.magic == "R4BBLK21"
    assert header.format_version == 2
    assert header.codec_and_parameters.level == 9
    assert header.codec_and_parameters.workers == 0
    assert header.schema_hash == _authority().schema_sha256
    assert header.protocol_hash == _authority().protocol_sha256
    assert header.attempt_id == _authority().attempt_id
    assert header.stream_group_id == STREAM_GROUP_ID
    assert header.segment_id == SEGMENT_ID
    assert header.block_index == 1
    assert header.previous_block_hash is None
    assert (header.record_count, header.first_ingest_seq, header.last_ingest_seq) == (2, 1, 2)
    assert header.first_receipt_monotonic_ns == records[0].record.receipt_monotonic_ns
    assert header.last_receipt_monotonic_ns == records[-1].record.receipt_monotonic_ns
    assert trailer.compressed_length == len(container.compressed)
    assert trailer.block_hash_sha256 == manifest.block_hash
    assert trailer.writer_key_id == authority.key_id
    assert trailer.commit_marker == "R4BCOMMIT21"
    assert authority.public_key_base64.encode("ascii") not in encoded


def test_signed_container_is_byte_deterministic_across_storage_roots(
    tmp_path: Path,
) -> None:
    policy = _policy()
    block = _tail_block([_queued(1), _queued(2)], policy)
    first = _writer(tmp_path / "first", policy=policy).commit(block)
    second = _writer(tmp_path / "second", policy=policy).commit(block)
    first_bytes = (tmp_path / "first" / first.data_file).read_bytes()
    second_bytes = (tmp_path / "second" / second.data_file).read_bytes()
    assert first_bytes == second_bytes
    assert first.block_hash == second.block_hash
    assert first.writer_ed25519_signature == second.writer_ed25519_signature


def test_explicit_untrusted_public_key_rejects_valid_container(tmp_path: Path) -> None:
    policy = _policy()
    manifest = _writer(tmp_path, policy=policy).commit(
        _tail_block([_queued(1)], policy)
    )
    encoded = (tmp_path / manifest.data_file).read_bytes()
    with pytest.raises(SignedBlockContainerError, match="signature"):
        parse_and_verify_signed_block_container_v2(
            encoded,
            signing_authority=_signing_authority(seed=2),
        )


def test_ed25519_signature_tamper_is_rejected(tmp_path: Path) -> None:
    policy = _policy()
    manifest = _writer(tmp_path, policy=policy).commit(
        _tail_block([_queued(1)], policy)
    )
    encoded = (tmp_path / manifest.data_file).read_bytes()
    container = parse_and_verify_signed_block_container_v2(
        encoded,
        signing_authority=_signing_authority(),
    )
    signature = container.trailer.writer_ed25519_signature
    replacement = ("A" if signature[0] != "A" else "B") + signature[1:]
    tampered = encoded.replace(signature.encode("ascii"), replacement.encode("ascii"), 1)
    with pytest.raises(SignedBlockContainerError, match="signature"):
        parse_and_verify_signed_block_container_v2(
            tampered,
            signing_authority=_signing_authority(),
        )


def test_embedded_block_hash_tamper_is_rejected_before_signature(tmp_path: Path) -> None:
    policy = _policy()
    manifest = _writer(tmp_path, policy=policy).commit(
        _tail_block([_queued(1)], policy)
    )
    encoded = (tmp_path / manifest.data_file).read_bytes()
    replacement = ("0" if manifest.block_hash[0] != "0" else "1") + manifest.block_hash[1:]
    tampered = encoded.replace(
        manifest.block_hash.encode("ascii"),
        replacement.encode("ascii"),
        1,
    )
    with pytest.raises(SignedBlockContainerError, match="domain hash"):
        parse_and_verify_signed_block_container_v2(
            tampered,
            signing_authority=_signing_authority(),
        )


def test_physical_commit_marker_tamper_is_rejected(tmp_path: Path) -> None:
    policy = _policy()
    manifest = _writer(tmp_path, policy=policy).commit(
        _tail_block([_queued(1)], policy)
    )
    encoded = bytearray((tmp_path / manifest.data_file).read_bytes())
    encoded[-1] ^= 1
    with pytest.raises(SignedBlockContainerError, match="physical commit marker"):
        parse_and_verify_signed_block_container_v2(
            bytes(encoded),
            signing_authority=_signing_authority(),
        )


def test_compressed_payload_crc32c_tamper_is_rejected(tmp_path: Path) -> None:
    policy = _policy()
    manifest = _writer(tmp_path, policy=policy).commit(
        _tail_block([_queued(1, payload_size=200)], policy)
    )
    encoded = (tmp_path / manifest.data_file).read_bytes()
    container = parse_and_verify_signed_block_container_v2(
        encoded,
        signing_authority=_signing_authority(),
    )
    offset = encoded.index(container.compressed)
    tampered = bytearray(encoded)
    tampered[offset + len(container.compressed) // 2] ^= 1
    with pytest.raises(SignedBlockContainerError, match="CRC32C"):
        parse_and_verify_signed_block_container_v2(
            bytes(tampered),
            signing_authority=_signing_authority(),
        )


def test_external_signature_metadata_cannot_override_embedded_signature(
    tmp_path: Path,
) -> None:
    policy = _policy()
    _writer(tmp_path, policy=policy).commit(_tail_block([_queued(1)], policy))
    manifest_path = tmp_path / "block-00000001.manifest.json"
    document = json.loads(manifest_path.read_bytes())
    signature = document["writer_ed25519_signature"]
    assert isinstance(signature, str)
    document["writer_ed25519_signature"] = (
        ("A" if signature[0] != "A" else "B") + signature[1:]
    )
    manifest_path.write_bytes(canonical_json_line(document))
    with pytest.raises(BlockIntegrityError, match="embedded metadata differs"):
        _verify(tmp_path, policy=policy)


def test_recovery_rejects_valid_signature_for_wrong_stream_before_mutation(
    tmp_path: Path,
) -> None:
    policy = _policy()

    def crash(point: str) -> None:
        if point == "after_block_fsync":
            raise RuntimeError("synthetic fsync crash")

    source = tmp_path / "source"
    source_writer = _writer(
        source,
        policy=policy,
        fault_hook=crash,
        stream_group_id="other-depth-group",
    )
    with pytest.raises(RuntimeError, match="fsync crash"):
        source_writer.commit(_tail_block([_queued(1)], policy))
    [source_partial] = source.glob("*.r4bblk.partial")

    target = tmp_path / "target"
    _writer(target, policy=policy)
    target_partial = target / "block-00000001.r4bblk.partial"
    original = source_partial.read_bytes()
    target_partial.write_bytes(original)
    with pytest.raises(BlockIntegrityError, match="embedded header authority differs"):
        _writer(target, policy=policy)
    assert target_partial.read_bytes() == original
    assert not (target / "block-00000001.r4bblk").exists()
    assert not (target / "block-00000001.manifest.json").exists()


def test_manifest_boolean_cannot_alias_signed_integer_metadata(tmp_path: Path) -> None:
    policy = _policy()
    _writer(tmp_path, policy=policy).commit(_tail_block([_queued(1)], policy))
    manifest_path = tmp_path / "block-00000001.manifest.json"
    document = json.loads(manifest_path.read_bytes())
    document["block_sequence"] = True
    manifest_path.write_bytes(canonical_json_line(document))
    with pytest.raises(BlockIntegrityError, match="exact signed projection"):
        _verify(tmp_path, policy=policy)


def test_recovery_receipt_tamper_is_rejected(tmp_path: Path) -> None:
    policy = _policy()

    def crash(point: str) -> None:
        if point == "after_block_rename":
            raise RuntimeError("synthetic rename crash")

    writer = _writer(tmp_path, policy=policy, fault_hook=crash)
    with pytest.raises(RuntimeError, match="rename crash"):
        writer.commit(_tail_block([_queued(1)], policy))
    _writer(tmp_path, policy=policy)
    receipt_path = tmp_path / "block-00000001.recovery.json"
    document = json.loads(receipt_path.read_bytes())
    document["block_hash"] = "0" * 64
    receipt_path.write_bytes(canonical_json_line(document))
    with pytest.raises(BlockIntegrityError, match="receipt differs"):
        _verify(tmp_path, policy=policy)


def test_legacy_or_unknown_block_artifact_is_fail_closed(tmp_path: Path) -> None:
    policy = _policy()
    _writer(tmp_path, policy=policy)
    residue = tmp_path / "block-00000001.jsonl.zst"
    residue.write_bytes(b"legacy-bare-zstd")
    with pytest.raises(BlockIntegrityError, match="unknown grouped-block artifact"):
        _writer(tmp_path, policy=policy)
    assert residue.read_bytes() == b"legacy-bare-zstd"


def test_writer_rejects_signer_that_differs_from_trusted_authority(
    tmp_path: Path,
) -> None:
    with pytest.raises(ValueError, match="signer differs"):
        _writer(
            tmp_path,
            signer=_signer(seed=1),
            signing_authority=_signing_authority(seed=2),
        )


def test_empty_merkle_root_is_deterministic() -> None:
    assert record_merkle_root([]) == record_merkle_root([])
    assert record_merkle_root([]) != record_merkle_root([b"{}\n"])
