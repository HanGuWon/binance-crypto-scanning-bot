from __future__ import annotations

import base64
import hashlib
import json
import shutil
from dataclasses import asdict, replace
from pathlib import Path

import pytest

import signalbot.r4b_v2.capture.integrity_ledger as integrity_ledger_module
import signalbot.r4b_v2.capture.membership as membership_module
from signalbot.r4b_v2.canonical import canonical_json_line
from signalbot.r4b_v2.capture.batching import QueuedRawRecordV2
from signalbot.r4b_v2.capture.block_container import (
    BlockSigningAuthorityV2,
    Ed25519BlockSignerV2,
)
from signalbot.r4b_v2.capture.blocks import (
    BlockManifestV2,
    BlockPolicyV2,
    GroupedBlockBuilderV2,
    GroupedBlockWriterV2,
)
from signalbot.r4b_v2.capture.integrity_ledger import (
    CaptureIntegrityLedgerV2,
    DataGapCauseV2,
    SourceGapCauseV2,
    SourceGapLeftBoundaryV2,
    attest_finalized_block_v2,
)
from signalbot.r4b_v2.capture.membership import (
    RAW_MEMBERSHIP_ONLY_REASON_V2,
    RawRecordMembershipErrorV2,
    VerifiedRawMembershipLeafV2,
    append_source_gap_bounded_from_membership_v2,
    attest_raw_record_membership_v2,
    canonical_verified_raw_membership_leaf_v2,
    parse_raw_record_membership_certificate_v2,
    reverify_source_gap_bounded_membership_v2,
    reverify_verified_raw_membership_leaf_v2,
    verify_raw_record_membership_leaf_v2,
    verify_raw_record_membership_v2,
)
from signalbot.r4b_v2.capture.models import RawRecordV2, TransportV2, VenueV2
from signalbot.r4b_v2.capture.plans import (
    ProvisionalPromotingCapturePlanV2,
    build_provisional_promoting_capture_plans_v2,
    provisional_promoting_plan_sha256_v2,
)
from signalbot.r4b_v2.capture.wal import WalAuthorityV2

HASH = "a" * 64
MAXIMUM_BYTES = 8 * 1024 * 1024
RESERVE_BYTES = 1024
CERTIFICATE_ID_DOMAIN = b"R4B_V2_RAW_RECORD_MEMBERSHIP_CERTIFICATE_ID\0"
SOURCE_PLANS = build_provisional_promoting_capture_plans_v2(("BTCUSDT",))
SOURCE_MARKET_PLAN = next(
    plan
    for plan in SOURCE_PLANS
    if isinstance(plan, ProvisionalPromotingCapturePlanV2)
    and plan.route_id == "usdm_market"
)
SOURCE_PLAN_SHA256 = provisional_promoting_plan_sha256_v2(SOURCE_PLANS)


class _SourceClock:
    def __init__(self, *, wall_ms: int, monotonic_ns: int) -> None:
        self.wall_value = wall_ms
        self.monotonic_value = monotonic_ns

    def wall_ms(self) -> int:
        return self.wall_value

    def monotonic_ns(self) -> int:
        return self.monotonic_value


def _authority(*, plan_sha256: str = "b" * 64) -> WalAuthorityV2:
    return WalAuthorityV2(
        attempt_id="attempt-membership",
        protocol_sha256=HASH,
        plan_sha256=plan_sha256,
        source_manifest_sha256="c" * 64,
        schema_sha256="d" * 64,
        runtime_manifest_sha256="e" * 64,
    )


def _signer() -> Ed25519BlockSignerV2:
    return Ed25519BlockSignerV2.from_private_key_bytes(
        key_id="writer-key-membership",
        private_key_bytes=b"\x11" * 32,
    )


def _signing_authority() -> BlockSigningAuthorityV2:
    signer = _signer()
    return BlockSigningAuthorityV2.from_public_key_bytes(
        key_id=signer.key_id,
        public_key_bytes=signer.public_key_bytes,
    )


def _policy() -> BlockPolicyV2:
    return BlockPolicyV2(
        qualification_id="sealed-zstd-1.5.7-l9",
        codec_candidate_id="zstd-1.5.7-l9-w0-checksum-content-size",
        compression_level=9,
        max_uncompressed_bytes=4_194_304,
        max_linger_ms=1_000,
    )


def _queued(
    ingest_seq: int,
    *,
    price: str | None = None,
    protocol_hash: str = HASH,
) -> QueuedRawRecordV2:
    monotonic_ns = 1_000_000 + ingest_seq
    record = RawRecordV2.from_payload(
        session_id="session-membership",
        plan_id="plan-membership",
        protocol_hash=protocol_hash,
        transport=TransportV2.WEBSOCKET,
        venue=VenueV2.USDM_FUTURES,
        route_id="futures-market",
        symbol="BTCUSDT",
        connection_id="connection-membership",
        generation=1,
        frame_seq=ingest_seq,
        ingest_seq=ingest_seq,
        receipt_wall_ms=1_000 + ingest_seq,
        receipt_monotonic_ns=monotonic_ns,
        raw_payload=canonical_json_line(
            {"p": price or str(100 + ingest_seq), "q": "1"}
        ).rstrip(b"\n"),
        source_logical_key=f"trade-{ingest_seq}",
    )
    return QueuedRawRecordV2.encode(
        record,
        enqueued_monotonic_ns=monotonic_ns + 1,
    )


def _source_queued(
    ingest_seq: int,
    *,
    connection_id: str,
    generation: int,
    frame_seq: int,
    receipt_wall_ms: int,
    receipt_monotonic_ns: int,
    route_id: str = "usdm_market",
    symbol: str | None = None,
    raw_payload: bytes = b'{"stream":"btcusdt@aggTrade","data":{}}',
) -> QueuedRawRecordV2:
    record = RawRecordV2.from_payload(
        session_id="session-source-gap",
        plan_id=SOURCE_MARKET_PLAN.name,
        protocol_hash=HASH,
        transport=TransportV2.WEBSOCKET,
        venue=VenueV2.USDM_FUTURES,
        route_id=route_id,
        symbol=symbol,
        connection_id=connection_id,
        generation=generation,
        frame_seq=frame_seq,
        ingest_seq=ingest_seq,
        receipt_wall_ms=receipt_wall_ms,
        receipt_monotonic_ns=receipt_monotonic_ns,
        raw_payload=raw_payload,
        source_logical_key=None,
    )
    return QueuedRawRecordV2.encode(
        record,
        enqueued_monotonic_ns=receipt_monotonic_ns + 1,
    )


def _writer(directory: Path) -> GroupedBlockWriterV2:
    return GroupedBlockWriterV2(
        directory,
        authority=_authority(),
        policy=_policy(),
        signer=_signer(),
        signing_authority=_signing_authority(),
        stream_group_id="futures-depth-group",
        segment_id="segment-000001",
        maximum_total_bytes=MAXIMUM_BYTES,
        emergency_reserve_bytes=RESERVE_BYTES,
    )


def _source_writer(directory: Path) -> GroupedBlockWriterV2:
    return GroupedBlockWriterV2(
        directory,
        authority=_authority(plan_sha256=SOURCE_PLAN_SHA256),
        policy=_policy(),
        signer=_signer(),
        signing_authority=_signing_authority(),
        stream_group_id="usdm-market-promoting-group",
        segment_id="segment-source-gap-000001",
        maximum_total_bytes=MAXIMUM_BYTES,
        emergency_reserve_bytes=RESERVE_BYTES,
    )


def _commit(
    writer: GroupedBlockWriterV2,
    records: tuple[QueuedRawRecordV2, ...],
) -> BlockManifestV2:
    builder = GroupedBlockBuilderV2(writer.policy)
    for record in records:
        assert not builder.offer(
            record,
            now_ns=record.record.receipt_monotonic_ns + 1,
        )
    block = builder.flush_tail(
        now_ns=records[-1].record.receipt_monotonic_ns + 2
    )
    assert block is not None
    return writer.commit(block)


def _ledger(
    root: Path,
    writer: GroupedBlockWriterV2,
    *,
    clock: _SourceClock | None = None,
) -> CaptureIntegrityLedgerV2:
    if clock is None:
        return CaptureIntegrityLedgerV2(
            root,
            authority=writer.authority,
            block_directory=writer.directory,
            block_root_binding=writer.root_binding,
            block_signing_authority=writer.signing_authority,
            block_policy=writer.policy,
            block_stream_group_id=writer.stream_group_id,
            block_segment_id=writer.segment_id,
            maximum_total_bytes=MAXIMUM_BYTES,
            emergency_reserve_bytes=RESERVE_BYTES,
            max_events=32,
            failure_domain_id="membership-ledger-device",
        )
    return CaptureIntegrityLedgerV2(
        root,
        authority=writer.authority,
        block_directory=writer.directory,
        block_root_binding=writer.root_binding,
        block_signing_authority=writer.signing_authority,
        block_policy=writer.policy,
        block_stream_group_id=writer.stream_group_id,
        block_segment_id=writer.segment_id,
        maximum_total_bytes=MAXIMUM_BYTES,
        emergency_reserve_bytes=RESERVE_BYTES,
        max_events=32,
        failure_domain_id="membership-ledger-device",
        wall_clock_ms=clock.wall_ms,
        monotonic_clock_ns=clock.monotonic_ns,
    )


def _chain(tmp_path: Path):  # type: ignore[no-untyped-def]
    writer = _writer(tmp_path / "blocks")
    first_records = (_queued(1), _queued(2))
    second_records = (_queued(3),)
    first = _commit(writer, first_records)
    second = _commit(writer, second_records)
    ledger = _ledger(tmp_path / "ledger", writer)
    return writer, ledger, first, second, first_records, second_records


def _verify(certificate, writer, ledger, **overrides):  # type: ignore[no-untyped-def]
    arguments = {
        "block_directory": writer.directory,
        "block_root_binding": writer.root_binding,
        "authority": writer.authority,
        "policy": writer.policy,
        "signing_authority": writer.signing_authority,
        "stream_group_id": writer.stream_group_id,
        "segment_id": writer.segment_id,
        "integrity_ledger": ledger,
    }
    arguments.update(overrides)
    return verify_raw_record_membership_v2(certificate, **arguments)


def _leaf(certificate, writer, ledger, **overrides):  # type: ignore[no-untyped-def]
    arguments = {
        "block_directory": writer.directory,
        "block_root_binding": writer.root_binding,
        "authority": writer.authority,
        "policy": writer.policy,
        "signing_authority": writer.signing_authority,
        "stream_group_id": writer.stream_group_id,
        "segment_id": writer.segment_id,
        "integrity_ledger": ledger,
        "expected_transport": TransportV2.WEBSOCKET,
        "expected_venue": VenueV2.USDM_FUTURES,
        "expected_route_id": "futures-market",
        "expected_symbol": "BTCUSDT",
    }
    arguments.update(overrides)
    return verify_raw_record_membership_leaf_v2(certificate, **arguments)


def _reverify_leaf(leaf, writer, ledger, **overrides):  # type: ignore[no-untyped-def]
    arguments = {
        "block_directory": writer.directory,
        "block_root_binding": writer.root_binding,
        "authority": writer.authority,
        "policy": writer.policy,
        "signing_authority": writer.signing_authority,
        "stream_group_id": writer.stream_group_id,
        "segment_id": writer.segment_id,
        "integrity_ledger": ledger,
        "expected_transport": TransportV2.WEBSOCKET,
        "expected_venue": VenueV2.USDM_FUTURES,
        "expected_route_id": "futures-market",
        "expected_symbol": "BTCUSDT",
    }
    arguments.update(overrides)
    return reverify_verified_raw_membership_leaf_v2(leaf, **arguments)


def _bound_source_gap_from_membership(
    writer: GroupedBlockWriterV2,
    ledger: CaptureIntegrityLedgerV2,
    open_event: object,
    *,
    left_certificate: object | None,
    right_certificate: object,
    evidence_sha256: str = "7" * 64,
):  # type: ignore[no-untyped-def]
    return append_source_gap_bounded_from_membership_v2(
        ledger,
        open_event,  # type: ignore[arg-type]
        left_certificate=left_certificate,  # type: ignore[arg-type]
        right_certificate=right_certificate,  # type: ignore[arg-type]
        block_directory=writer.directory,
        block_root_binding=writer.root_binding,
        authority=writer.authority,
        policy=writer.policy,
        signing_authority=writer.signing_authority,
        stream_group_id=writer.stream_group_id,
        segment_id=writer.segment_id,
        evidence_sha256=evidence_sha256,
    )


def _reverify_source_gap_membership(
    writer: GroupedBlockWriterV2,
    ledger: CaptureIntegrityLedgerV2,
    bounded_event: object,
) -> None:
    reverify_source_gap_bounded_membership_v2(
        ledger,
        bounded_event,  # type: ignore[arg-type]
        block_directory=writer.directory,
        block_root_binding=writer.root_binding,
        authority=writer.authority,
        policy=writer.policy,
        signing_authority=writer.signing_authority,
        stream_group_id=writer.stream_group_id,
        segment_id=writer.segment_id,
    )


def _reidentify(document: dict[str, object]) -> bytes:
    identity = dict(document)
    identity.pop("certificate_id")
    document["certificate_id"] = hashlib.sha256(
        CERTIFICATE_ID_DOMAIN + canonical_json_line(identity)
    ).hexdigest()
    return canonical_json_line(document)


def test_exact_membership_is_deterministic_canonical_and_publicly_verifiable(
    tmp_path: Path,
) -> None:
    writer, ledger, first, _, first_records, _ = _chain(tmp_path)
    line = first_records[-1].encoded_line
    certificate = attest_raw_record_membership_v2(
        writer,
        first,
        expected_record_jsonl=line,
        integrity_ledger=ledger,
    )
    replay = attest_raw_record_membership_v2(
        writer,
        first,
        expected_record_jsonl=line,
        integrity_ledger=ledger,
    )

    assert certificate == replay
    assert certificate.canonical_bytes == replay.canonical_bytes
    assert parse_raw_record_membership_certificate_v2(
        certificate.canonical_bytes
    ) == certificate
    assert certificate.leaf_index == 1
    assert certificate.leaf_count == 2
    assert certificate.ingest_seq == 2
    assert certificate.transport == "websocket"
    assert certificate.venue == "usdm_futures"
    assert certificate.route_id == "futures-market"
    assert certificate.symbol == "BTCUSDT"
    assert _verify(certificate, writer, ledger) == first_records[-1].record


def test_first_and_last_leaf_boundaries_and_wrong_block_are_rejected(
    tmp_path: Path,
) -> None:
    writer, ledger, first, second, first_records, second_records = _chain(tmp_path)
    first_certificate = attest_raw_record_membership_v2(
        writer,
        first,
        expected_record_jsonl=first_records[0].encoded_line,
        integrity_ledger=ledger,
    )
    tail_certificate = attest_raw_record_membership_v2(
        writer,
        second,
        expected_record_jsonl=second_records[0].encoded_line,
        integrity_ledger=ledger,
    )
    assert (first_certificate.leaf_index, first_certificate.leaf_count) == (0, 2)
    assert (tail_certificate.leaf_index, tail_certificate.leaf_count) == (0, 1)
    assert _verify(first_certificate, writer, ledger).ingest_seq == 1
    assert _verify(tail_certificate, writer, ledger).ingest_seq == 3

    with pytest.raises(RawRecordMembershipErrorV2, match="requested finalized block"):
        attest_raw_record_membership_v2(
            writer,
            second,
            expected_record_jsonl=first_records[-1].encoded_line,
            integrity_ledger=ledger,
        )
    future = replace(second, block_sequence=writer.next_block_sequence)
    with pytest.raises(RawRecordMembershipErrorV2, match="failed closed"):
        attest_raw_record_membership_v2(
            writer,
            future,
            expected_record_jsonl=second_records[0].encoded_line,
            integrity_ledger=ledger,
        )


def test_noncanonical_expected_line_and_leaf_index_tamper_fail_closed(
    tmp_path: Path,
) -> None:
    writer, ledger, first, _, first_records, _ = _chain(tmp_path)
    pretty = json.dumps(json.loads(first_records[0].encoded_line), indent=2).encode() + b"\n"
    with pytest.raises(RawRecordMembershipErrorV2, match="failed closed"):
        attest_raw_record_membership_v2(
            writer,
            first,
            expected_record_jsonl=pretty,
            integrity_ledger=ledger,
        )

    certificate = attest_raw_record_membership_v2(
        writer,
        first,
        expected_record_jsonl=first_records[-1].encoded_line,
        integrity_ledger=ledger,
    )
    document = json.loads(certificate.canonical_bytes)
    document["leaf_index"] = 0
    with pytest.raises(RawRecordMembershipErrorV2, match="invalid"):
        parse_raw_record_membership_certificate_v2(_reidentify(document))


def test_self_consistent_forged_payload_is_not_a_signed_member(tmp_path: Path) -> None:
    writer, ledger, first, _, first_records, _ = _chain(tmp_path)
    certificate = attest_raw_record_membership_v2(
        writer,
        first,
        expected_record_jsonl=first_records[-1].encoded_line,
        integrity_ledger=ledger,
    )
    forged = _queued(2, price="999999")
    document = json.loads(certificate.canonical_bytes)
    document["record_jsonl_base64"] = base64.b64encode(forged.encoded_line).decode()
    document["record_jsonl_sha256"] = hashlib.sha256(forged.encoded_line).hexdigest()
    document["raw_payload_hash_v2"] = forged.record.derive_raw_payload_hash(
        writer.stream_group_id
    )
    forged_certificate = parse_raw_record_membership_certificate_v2(
        _reidentify(document)
    )

    with pytest.raises(RawRecordMembershipErrorV2, match="signed block bytes"):
        _verify(forged_certificate, writer, ledger)


@pytest.mark.parametrize("wrong_scope", ["authority", "segment", "path", "ledger"])
def test_wrong_trusted_scope_or_storage_root_is_rejected(
    tmp_path: Path,
    wrong_scope: str,
) -> None:
    writer, ledger, first, _, first_records, _ = _chain(tmp_path / "primary")
    certificate = attest_raw_record_membership_v2(
        writer,
        first,
        expected_record_jsonl=first_records[0].encoded_line,
        integrity_ledger=ledger,
    )
    overrides: dict[str, object]
    if wrong_scope == "authority":
        overrides = {"authority": _authority(plan_sha256="f" * 64)}
    elif wrong_scope == "segment":
        overrides = {"segment_id": "segment-000002"}
    elif wrong_scope == "path":
        other_writer = _writer(tmp_path / "other-blocks")
        _commit(other_writer, (_queued(1, price="555"),))
        overrides = {
            "block_directory": other_writer.directory,
            "block_root_binding": other_writer.root_binding,
        }
    else:
        overrides = {"integrity_ledger": _ledger(tmp_path / "other-ledger", writer)}

    with pytest.raises(RawRecordMembershipErrorV2):
        _verify(certificate, writer, ledger, **overrides)
    with pytest.raises(RawRecordMembershipErrorV2):
        _leaf(certificate, writer, ledger, **overrides)


def test_current_block_corruption_or_missing_manifest_is_rejected(tmp_path: Path) -> None:
    writer, ledger, _, second, _, second_records = _chain(tmp_path)
    certificate = attest_raw_record_membership_v2(
        writer,
        second,
        expected_record_jsonl=second_records[0].encoded_line,
        integrity_ledger=ledger,
    )
    data_path = writer.directory / second.data_file
    original = data_path.read_bytes()
    data_path.write_bytes(original + b"tamper")
    with pytest.raises(RawRecordMembershipErrorV2, match="failed closed"):
        _verify(certificate, writer, ledger)
    data_path.write_bytes(original)

    manifest_path = writer.directory / certificate.finalized_block.manifest_file
    manifest_bytes = manifest_path.read_bytes()
    manifest_path.unlink()
    with pytest.raises(RawRecordMembershipErrorV2, match="failed closed"):
        _verify(certificate, writer, ledger)
    manifest_path.write_bytes(manifest_bytes)
    assert _verify(certificate, writer, ledger).ingest_seq == 3


def test_valid_prefix_substitution_breaks_later_block_chain(tmp_path: Path) -> None:
    writer, ledger, _, second, _, second_records = _chain(tmp_path / "primary")
    certificate = attest_raw_record_membership_v2(
        writer,
        second,
        expected_record_jsonl=second_records[0].encoded_line,
        integrity_ledger=ledger,
    )
    substitute = _writer(tmp_path / "substitute")
    substitute_first = _commit(substitute, (_queued(1, price="777"), _queued(2)))
    for name in (substitute_first.data_file, "block-00000001.manifest.json"):
        shutil.copyfile(substitute.directory / name, writer.directory / name)

    with pytest.raises(RawRecordMembershipErrorV2, match="failed closed"):
        _verify(certificate, writer, ledger)


def test_void_prefix_remains_poisoned_even_after_physical_bytes_are_restored(
    tmp_path: Path,
) -> None:
    writer, ledger, first, second, _, second_records = _chain(tmp_path)
    certificate = attest_raw_record_membership_v2(
        writer,
        second,
        expected_record_jsonl=second_records[0].encoded_line,
        integrity_ledger=ledger,
    )
    reference = attest_finalized_block_v2(writer, first)
    first_data = writer.directory / "block-00000001.r4bblk"
    original = first_data.read_bytes()
    first_data.write_bytes(original + b"corrupt")
    ledger.append_void_for_finalized_block(
        reference,
        detector_component="membership-auditor",
        detection_evidence_sha256="9" * 64,
    )
    first_data.write_bytes(original)

    with pytest.raises(RawRecordMembershipErrorV2, match="VOID"):
        _verify(certificate, writer, ledger)
    with pytest.raises(RawRecordMembershipErrorV2, match="VOID"):
        _leaf(certificate, writer, ledger)


def test_durable_leaf_is_only_an_issuance_snapshot_and_must_live_reverify(
    tmp_path: Path,
) -> None:
    writer, ledger, first, second, _, second_records = _chain(tmp_path)
    certificate = attest_raw_record_membership_v2(
        writer,
        second,
        expected_record_jsonl=second_records[0].encoded_line,
        integrity_ledger=ledger,
    )
    leaf = _leaf(certificate, writer, ledger)
    reference = attest_finalized_block_v2(writer, first)
    first_data = writer.directory / "block-00000001.r4bblk"
    original = first_data.read_bytes()
    first_data.write_bytes(original + b"corrupt-after-leaf-mint")
    ledger.append_void_for_finalized_block(
        reference,
        detector_component="post-mint-membership-auditor",
        detection_evidence_sha256="8" * 64,
    )
    first_data.write_bytes(original)

    snapshot = json.loads(canonical_verified_raw_membership_leaf_v2(leaf))
    assert snapshot["verified_raw_membership_m0_at_issuance"] is True
    assert snapshot["live_reverification_required"] is True
    assert snapshot["current_authority_claimed"] is False
    with pytest.raises(RawRecordMembershipErrorV2, match="VOID"):
        _reverify_leaf(leaf, writer, ledger)


def test_verified_leaf_is_factory_only_canonical_and_non_promoting(
    tmp_path: Path,
) -> None:
    writer, ledger, first, _, first_records, _ = _chain(tmp_path)
    certificate = attest_raw_record_membership_v2(
        writer,
        first,
        expected_record_jsonl=first_records[0].encoded_line,
        integrity_ledger=ledger,
    )
    leaf = _leaf(certificate, writer, ledger)
    replay = _leaf(certificate, writer, ledger)

    assert leaf == replay
    assert leaf.authority_sha256 == writer.authority.sha256
    assert leaf.certificate_canonical_sha256 == hashlib.sha256(
        certificate.canonical_bytes
    ).hexdigest()
    assert leaf.raw_payload_hash_v2 == first_records[0].record.derive_raw_payload_hash(
        writer.stream_group_id
    )
    assert not leaf.parser_bound
    assert not leaf.cursor_complete
    assert not leaf.causal_inputs_complete
    assert leaf.verified_raw_membership_m0_at_issuance
    assert leaf.live_reverification_required
    assert not leaf.current_authority_claimed
    assert leaf.authority_reason == RAW_MEMBERSHIP_ONLY_REASON_V2
    assert _reverify_leaf(leaf, writer, ledger) is None

    encoded = canonical_verified_raw_membership_leaf_v2(leaf)
    assert encoded == canonical_verified_raw_membership_leaf_v2(replay)
    document = json.loads(encoded)
    assert document["leaf_sha256"] == leaf.leaf_sha256
    assert document["certificate_id"] == certificate.certificate_id
    assert document["authority_sha256"] == writer.authority.sha256
    assert document["causal_inputs_complete"] is False
    assert document["current_authority_claimed"] is False
    assert document["cursor_complete"] is False
    assert document["live_reverification_required"] is True
    assert document["parser_bound"] is False
    assert document["verified_raw_membership_m0_at_issuance"] is True
    assert document["authority_reason"] == RAW_MEMBERSHIP_ONLY_REASON_V2
    assert document["stream_group_id"] == writer.stream_group_id
    assert document["segment_id"] == writer.segment_id
    assert document["record"] == json.loads(first_records[0].encoded_line)
    assert document["finalized_block_scope"]["block_hash"] == first.block_hash
    assert document["integrity_scope"] == {
        "ledger_root_binding_sha256": ledger.ledger_root_binding_sha256,
        "ledger_root_path_sha256": ledger.ledger_root_path_sha256,
    }

    with pytest.raises(RawRecordMembershipErrorV2, match="live membership"):
        VerifiedRawMembershipLeafV2(
            certificate=certificate,
            authority=writer.authority,
            stream_group_id=writer.stream_group_id,
            segment_id=writer.segment_id,
            record=first_records[0].record,
        )
    with pytest.raises(RawRecordMembershipErrorV2, match="live membership"):
        replace(leaf)


@pytest.mark.parametrize(
    ("override", "value"),
    [
        ("expected_transport", TransportV2.HTTPS),
        ("expected_venue", VenueV2.SPOT),
        ("expected_route_id", "other-route"),
        ("expected_symbol", "ETHUSDT"),
    ],
)
def test_verified_leaf_rejects_trusted_source_scope_mismatch(
    tmp_path: Path,
    override: str,
    value: object,
) -> None:
    writer, ledger, first, _, first_records, _ = _chain(tmp_path)
    certificate = attest_raw_record_membership_v2(
        writer,
        first,
        expected_record_jsonl=first_records[0].encoded_line,
        integrity_ledger=ledger,
    )

    with pytest.raises(RawRecordMembershipErrorV2, match="trusted scope"):
        _leaf(certificate, writer, ledger, **{override: value})


def test_verified_leaf_rejects_protocol_mismatch_in_signed_record(
    tmp_path: Path,
) -> None:
    writer = _writer(tmp_path / "blocks")
    record = _queued(1, protocol_hash="f" * 64)
    manifest = _commit(writer, (record,))
    ledger = _ledger(tmp_path / "ledger", writer)
    certificate = attest_raw_record_membership_v2(
        writer,
        manifest,
        expected_record_jsonl=record.encoded_line,
        integrity_ledger=ledger,
    )

    with pytest.raises(RawRecordMembershipErrorV2, match="protocol differs"):
        _leaf(certificate, writer, ledger)


def test_verified_leaf_canonicalization_rejects_tampering(
    tmp_path: Path,
) -> None:
    writer, ledger, first, _, first_records, _ = _chain(tmp_path)
    certificate = attest_raw_record_membership_v2(
        writer,
        first,
        expected_record_jsonl=first_records[0].encoded_line,
        integrity_ledger=ledger,
    )
    digest_tampered = _leaf(certificate, writer, ledger)
    object.__setattr__(digest_tampered, "authority_sha256", "f" * 64)
    with pytest.raises(RawRecordMembershipErrorV2, match="invalid"):
        canonical_verified_raw_membership_leaf_v2(digest_tampered)

    record_tampered = _leaf(certificate, writer, ledger)
    object.__setattr__(record_tampered, "record", _queued(1, price="999").record)
    with pytest.raises(RawRecordMembershipErrorV2, match="invalid"):
        canonical_verified_raw_membership_leaf_v2(record_tampered)


def test_source_gap_bounded_embeds_live_verified_endpoint_membership(
    tmp_path: Path,
) -> None:
    writer = _source_writer(tmp_path / "blocks")
    left = _source_queued(
        1,
        connection_id="source-connection-1",
        generation=1,
        frame_seq=1,
        receipt_wall_ms=1_000,
        receipt_monotonic_ns=1_000,
    )
    right = _source_queued(
        2,
        connection_id="source-connection-2",
        generation=2,
        frame_seq=1,
        receipt_wall_ms=2_000,
        receipt_monotonic_ns=2_000,
    )
    manifest = _commit(writer, (left, right))
    clock = _SourceClock(wall_ms=950, monotonic_ns=950)
    ledger = _ledger(tmp_path / "ledger", writer, clock=clock)
    left_certificate = attest_raw_record_membership_v2(
        writer,
        manifest,
        expected_record_jsonl=left.encoded_line,
        integrity_ledger=ledger,
    )
    right_certificate = attest_raw_record_membership_v2(
        writer,
        manifest,
        expected_record_jsonl=right.encoded_line,
        integrity_ledger=ledger,
    )

    session_open = ledger.append_source_gap_open(
        SOURCE_PLANS,
        SOURCE_MARKET_PLAN,
        session_id="session-source-gap",
        process_boot_id="boot-source-gap",
        cause=SourceGapCauseV2.SESSION_START_PENDING,
        left_boundary_kind=SourceGapLeftBoundaryV2.SESSION_START,
        left_connection_id=None,
        left_generation=None,
        left_frame_seq=None,
        left_ingest_seq=None,
        left_wall_ms=900,
        left_monotonic_ns=900,
        detected_wall_ms=900,
        detected_monotonic_ns=900,
        source_component="v2-usdm-websocket-owner",
        evidence_sha256="5" * 64,
    )
    clock.wall_value = 1_100
    clock.monotonic_value = 1_100
    session_bounded = _bound_source_gap_from_membership(
        writer,
        ledger,
        session_open,
        left_certificate=None,
        right_certificate=left_certificate,
    )
    assert session_bounded.payload["left_record_locator"] is None

    clock.wall_value = 1_300
    clock.monotonic_value = 1_300
    reconnect_open = ledger.append_source_gap_open(
        SOURCE_PLANS,
        SOURCE_MARKET_PLAN,
        session_id="session-source-gap",
        process_boot_id="boot-source-gap",
        cause=SourceGapCauseV2.WEBSOCKET_DISCONNECT,
        left_boundary_kind=SourceGapLeftBoundaryV2.RETAINED_FRAME,
        left_connection_id=left.record.connection_id,
        left_generation=left.record.generation,
        left_frame_seq=left.record.frame_seq,
        left_ingest_seq=left.record.ingest_seq,
        left_wall_ms=left.record.receipt_wall_ms,
        left_monotonic_ns=left.record.receipt_monotonic_ns,
        detected_wall_ms=1_200,
        detected_monotonic_ns=1_200,
        source_component="v2-usdm-websocket-owner",
        evidence_sha256="6" * 64,
    )
    clock.wall_value = 1_999
    clock.monotonic_value = 1_999
    with pytest.raises(RawRecordMembershipErrorV2, match="commit failed closed"):
        _bound_source_gap_from_membership(
            writer,
            ledger,
            reconnect_open,
            left_certificate=left_certificate,
            right_certificate=right_certificate,
        )
    assert ledger.events[-1] == reconnect_open

    clock.wall_value = 2_100
    clock.monotonic_value = 2_100
    reconnect_bounded = _bound_source_gap_from_membership(
        writer,
        ledger,
        reconnect_open,
        left_certificate=left_certificate,
        right_certificate=right_certificate,
    )
    left_locator = reconnect_bounded.payload["left_record_locator"]
    right_locator = reconnect_bounded.payload["right_record_locator"]
    assert isinstance(left_locator, dict)
    assert isinstance(right_locator, dict)
    assert left_locator["ingest_seq"] == left.record.ingest_seq
    assert right_locator["ingest_seq"] == right.record.ingest_seq
    assert left_locator["record_jsonl_sha256"] == left_certificate.record_jsonl_sha256
    assert right_locator["record_jsonl_sha256"] == (
        right_certificate.record_jsonl_sha256
    )
    assert reconnect_bounded.payload["right_connection_id"] == (
        right.record.connection_id
    )
    assert reconnect_bounded.payload["right_ingest_seq"] == 2
    assert _reverify_source_gap_membership(
        writer,
        ledger,
        session_bounded,
    ) is None
    assert _reverify_source_gap_membership(
        writer,
        ledger,
        reconnect_bounded,
    ) is None
    assert len(_ledger(tmp_path / "ledger", writer).events) == 4


def test_source_gap_compact_locator_closes_large_valid_signed_record(
    tmp_path: Path,
) -> None:
    writer = _source_writer(tmp_path / "blocks")
    raw_payload = b'{"payload":"' + (b"x" * 100_000) + b'"}'
    right = _source_queued(
        1,
        connection_id="source-connection-large",
        generation=1,
        frame_seq=1,
        receipt_wall_ms=1_000,
        receipt_monotonic_ns=1_000,
        raw_payload=raw_payload,
    )
    manifest = _commit(writer, (right,))
    clock = _SourceClock(wall_ms=900, monotonic_ns=900)
    ledger = _ledger(tmp_path / "ledger", writer, clock=clock)
    certificate = attest_raw_record_membership_v2(
        writer,
        manifest,
        expected_record_jsonl=right.encoded_line,
        integrity_ledger=ledger,
    )
    assert len(certificate.canonical_bytes) > 128 * 1024
    opened = ledger.append_source_gap_open(
        SOURCE_PLANS,
        SOURCE_MARKET_PLAN,
        session_id="session-source-gap",
        process_boot_id="boot-source-gap",
        cause=SourceGapCauseV2.SESSION_START_PENDING,
        left_boundary_kind=SourceGapLeftBoundaryV2.SESSION_START,
        left_connection_id=None,
        left_generation=None,
        left_frame_seq=None,
        left_ingest_seq=None,
        left_wall_ms=800,
        left_monotonic_ns=800,
        detected_wall_ms=800,
        detected_monotonic_ns=800,
        source_component="v2-usdm-websocket-owner",
        evidence_sha256="5" * 64,
    )
    clock.wall_value = 1_100
    clock.monotonic_value = 1_100
    bounded = _bound_source_gap_from_membership(
        writer,
        ledger,
        opened,
        left_certificate=None,
        right_certificate=certificate,
    )

    assert len(canonical_json_line(asdict(bounded))) < 64 * 1024
    locator = bounded.payload["right_record_locator"]
    assert isinstance(locator, dict)
    assert locator["record_jsonl_sha256"] == certificate.record_jsonl_sha256
    assert _reverify_source_gap_membership(
        writer,
        ledger,
        bounded,
    ) is None


def test_source_gap_current_replay_rejects_later_void_even_after_bytes_restored(
    tmp_path: Path,
) -> None:
    writer = _source_writer(tmp_path / "blocks")
    right = _source_queued(
        1,
        connection_id="source-connection-1",
        generation=1,
        frame_seq=1,
        receipt_wall_ms=1_000,
        receipt_monotonic_ns=1_000,
    )
    manifest = _commit(writer, (right,))
    clock = _SourceClock(wall_ms=900, monotonic_ns=900)
    ledger = _ledger(tmp_path / "ledger", writer, clock=clock)
    certificate = attest_raw_record_membership_v2(
        writer,
        manifest,
        expected_record_jsonl=right.encoded_line,
        integrity_ledger=ledger,
    )
    opened = ledger.append_source_gap_open(
        SOURCE_PLANS,
        SOURCE_MARKET_PLAN,
        session_id="session-source-gap",
        process_boot_id="boot-source-gap",
        cause=SourceGapCauseV2.SESSION_START_PENDING,
        left_boundary_kind=SourceGapLeftBoundaryV2.SESSION_START,
        left_connection_id=None,
        left_generation=None,
        left_frame_seq=None,
        left_ingest_seq=None,
        left_wall_ms=800,
        left_monotonic_ns=800,
        detected_wall_ms=800,
        detected_monotonic_ns=800,
        source_component="v2-usdm-websocket-owner",
        evidence_sha256="5" * 64,
    )
    clock.wall_value = 1_100
    clock.monotonic_value = 1_100
    bounded = _bound_source_gap_from_membership(
        writer,
        ledger,
        opened,
        left_certificate=None,
        right_certificate=certificate,
    )
    data_path = writer.directory / certificate.finalized_block.data_file
    original = data_path.read_bytes()
    data_path.write_bytes(original + b"corruption")
    ledger.append_void_for_finalized_block(
        certificate.finalized_block,
        detector_component="source-gap-void-test",
        detection_evidence_sha256="7" * 64,
    )
    data_path.write_bytes(original)

    with pytest.raises(RawRecordMembershipErrorV2, match="failed closed"):
        _reverify_source_gap_membership(writer, ledger, bounded)


def test_source_gap_current_replay_rejects_void_appended_during_signed_scan(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    writer = _source_writer(tmp_path / "blocks")
    right = _source_queued(
        1,
        connection_id="source-connection-1",
        generation=1,
        frame_seq=1,
        receipt_wall_ms=1_000,
        receipt_monotonic_ns=1_000,
    )
    manifest = _commit(writer, (right,))
    clock = _SourceClock(wall_ms=900, monotonic_ns=900)
    ledger = _ledger(tmp_path / "ledger", writer, clock=clock)
    certificate = attest_raw_record_membership_v2(
        writer,
        manifest,
        expected_record_jsonl=right.encoded_line,
        integrity_ledger=ledger,
    )
    opened = ledger.append_source_gap_open(
        SOURCE_PLANS,
        SOURCE_MARKET_PLAN,
        session_id="session-source-gap",
        process_boot_id="boot-source-gap",
        cause=SourceGapCauseV2.SESSION_START_PENDING,
        left_boundary_kind=SourceGapLeftBoundaryV2.SESSION_START,
        left_connection_id=None,
        left_generation=None,
        left_frame_seq=None,
        left_ingest_seq=None,
        left_wall_ms=800,
        left_monotonic_ns=800,
        detected_wall_ms=800,
        detected_monotonic_ns=800,
        source_component="v2-usdm-websocket-owner",
        evidence_sha256="5" * 64,
    )
    clock.wall_value = 1_100
    clock.monotonic_value = 1_100
    bounded = _bound_source_gap_from_membership(
        writer,
        ledger,
        opened,
        left_certificate=None,
        right_certificate=certificate,
    )

    other_clock = _SourceClock(wall_ms=2_000, monotonic_ns=2_000)
    other_ledger = _ledger(
        tmp_path / "ledger",
        writer,
        clock=other_clock,
    )
    data_path = writer.directory / certificate.finalized_block.data_file
    original_data = data_path.read_bytes()
    original_consume = integrity_ledger_module.consume_verified_grouped_records_v2
    void_appended = False

    def consume_then_append_void(*args: object, **kwargs: object) -> int:
        nonlocal void_appended
        delivered = original_consume(*args, **kwargs)  # type: ignore[arg-type]
        if not void_appended:
            void_appended = True
            data_path.write_bytes(original_data + b"corruption")
            try:
                other_ledger.append_void_for_finalized_block(
                    certificate.finalized_block,
                    detector_component="source-gap-scan-race-test",
                    detection_evidence_sha256="8" * 64,
                )
            finally:
                data_path.write_bytes(original_data)
        return delivered

    monkeypatch.setattr(
        integrity_ledger_module,
        "consume_verified_grouped_records_v2",
        consume_then_append_void,
    )
    with pytest.raises(RawRecordMembershipErrorV2, match="failed closed"):
        _reverify_source_gap_membership(writer, ledger, bounded)

    assert void_appended
    assert ledger.events[-1].event_type == "VOID"


def test_source_gap_commit_revalidates_after_unrelated_ledger_change(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    writer = _source_writer(tmp_path / "blocks")
    right = _source_queued(
        1,
        connection_id="source-connection-1",
        generation=1,
        frame_seq=1,
        receipt_wall_ms=1_000,
        receipt_monotonic_ns=1_000,
    )
    manifest = _commit(writer, (right,))
    clock = _SourceClock(wall_ms=900, monotonic_ns=900)
    ledger = _ledger(tmp_path / "ledger", writer, clock=clock)
    right_certificate = attest_raw_record_membership_v2(
        writer,
        manifest,
        expected_record_jsonl=right.encoded_line,
        integrity_ledger=ledger,
    )
    session_open = ledger.append_source_gap_open(
        SOURCE_PLANS,
        SOURCE_MARKET_PLAN,
        session_id="session-source-gap",
        process_boot_id="boot-source-gap",
        cause=SourceGapCauseV2.SESSION_START_PENDING,
        left_boundary_kind=SourceGapLeftBoundaryV2.SESSION_START,
        left_connection_id=None,
        left_generation=None,
        left_frame_seq=None,
        left_ingest_seq=None,
        left_wall_ms=800,
        left_monotonic_ns=800,
        detected_wall_ms=800,
        detected_monotonic_ns=800,
        source_component="v2-usdm-websocket-owner",
        evidence_sha256="5" * 64,
    )
    original_verify = membership_module.verify_raw_record_membership_v2
    changed_tip = False

    def verify_then_change_tip(*args: object, **kwargs: object) -> RawRecordV2:
        nonlocal changed_tip
        record = original_verify(*args, **kwargs)  # type: ignore[arg-type]
        if not changed_tip:
            changed_tip = True
            ledger.append_data_gap(
                first_missing_ingest_seq=2,
                last_missing_ingest_seq=2,
                receipt_wall_lower_bound_ms=1_001,
                receipt_wall_upper_bound_ms=1_001,
                receipt_monotonic_lower_bound_ns=1_001,
                receipt_monotonic_upper_bound_ns=1_001,
                cause=DataGapCauseV2.INGEST_SEQUENCE_DISCONTINUITY,
                source_component="membership-race-test",
                evidence_sha256="6" * 64,
            )
        return record

    monkeypatch.setattr(
        membership_module,
        "verify_raw_record_membership_v2",
        verify_then_change_tip,
    )
    clock.wall_value = 1_100
    clock.monotonic_value = 1_100
    bounded = _bound_source_gap_from_membership(
        writer,
        ledger,
        session_open,
        left_certificate=None,
        right_certificate=right_certificate,
    )

    assert changed_tip
    assert bounded.event_sequence == 3
    assert [event.event_type for event in ledger.events] == [
        "SOURCE_GAP",
        "DATA_GAP",
        "SOURCE_GAP",
    ]


def test_source_gap_membership_binding_rejects_missing_left_and_scope_drift(
    tmp_path: Path,
) -> None:
    writer = _source_writer(tmp_path / "blocks")
    left = _source_queued(
        1,
        connection_id="source-connection-1",
        generation=1,
        frame_seq=1,
        receipt_wall_ms=1_000,
        receipt_monotonic_ns=1_000,
    )
    wrong_scope = _source_queued(
        2,
        connection_id="source-connection-2",
        generation=2,
        frame_seq=1,
        receipt_wall_ms=2_000,
        receipt_monotonic_ns=2_000,
        symbol="BTCUSDT",
    )
    manifest = _commit(writer, (left, wrong_scope))
    clock = _SourceClock(wall_ms=900, monotonic_ns=900)
    ledger = _ledger(tmp_path / "ledger", writer, clock=clock)
    left_certificate = attest_raw_record_membership_v2(
        writer,
        manifest,
        expected_record_jsonl=left.encoded_line,
        integrity_ledger=ledger,
    )
    wrong_certificate = attest_raw_record_membership_v2(
        writer,
        manifest,
        expected_record_jsonl=wrong_scope.encoded_line,
        integrity_ledger=ledger,
    )
    session_open = ledger.append_source_gap_open(
        SOURCE_PLANS,
        SOURCE_MARKET_PLAN,
        session_id="session-source-gap",
        process_boot_id="boot-source-gap",
        cause=SourceGapCauseV2.SESSION_START_PENDING,
        left_boundary_kind=SourceGapLeftBoundaryV2.SESSION_START,
        left_connection_id=None,
        left_generation=None,
        left_frame_seq=None,
        left_ingest_seq=None,
        left_wall_ms=800,
        left_monotonic_ns=800,
        detected_wall_ms=800,
        detected_monotonic_ns=800,
        source_component="v2-usdm-websocket-owner",
        evidence_sha256="5" * 64,
    )
    clock.wall_value = 1_100
    clock.monotonic_value = 1_100
    _bound_source_gap_from_membership(
        writer,
        ledger,
        session_open,
        left_certificate=None,
        right_certificate=left_certificate,
    )
    clock.wall_value = 1_300
    clock.monotonic_value = 1_300
    reconnect_open = ledger.append_source_gap_open(
        SOURCE_PLANS,
        SOURCE_MARKET_PLAN,
        session_id="session-source-gap",
        process_boot_id="boot-source-gap",
        cause=SourceGapCauseV2.WEBSOCKET_DISCONNECT,
        left_boundary_kind=SourceGapLeftBoundaryV2.RETAINED_FRAME,
        left_connection_id=left.record.connection_id,
        left_generation=left.record.generation,
        left_frame_seq=left.record.frame_seq,
        left_ingest_seq=left.record.ingest_seq,
        left_wall_ms=left.record.receipt_wall_ms,
        left_monotonic_ns=left.record.receipt_monotonic_ns,
        detected_wall_ms=1_200,
        detected_monotonic_ns=1_200,
        source_component="v2-usdm-websocket-owner",
        evidence_sha256="6" * 64,
    )
    clock.wall_value = 2_100
    clock.monotonic_value = 2_100
    with pytest.raises(RawRecordMembershipErrorV2, match="requires left"):
        _bound_source_gap_from_membership(
            writer,
            ledger,
            reconnect_open,
            left_certificate=None,
            right_certificate=left_certificate,
        )
    with pytest.raises(RawRecordMembershipErrorV2, match="combined-stream scope"):
        _bound_source_gap_from_membership(
            writer,
            ledger,
            reconnect_open,
            left_certificate=left_certificate,
            right_certificate=wrong_certificate,
        )
    assert ledger.events[-1] == reconnect_open
