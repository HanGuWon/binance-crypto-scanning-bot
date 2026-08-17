from __future__ import annotations

import hashlib
import struct
from collections.abc import Iterator
from dataclasses import replace
from pathlib import Path
from typing import cast

import pytest

from signalbot.capture.receipts import ReceiptTimestamp
from signalbot.r4b_v2.canonical import canonical_json_line
from signalbot.r4b_v2.capture.authority import StorageRootBindingV2
from signalbot.r4b_v2.capture.batching import QueuedRawRecordV2
from signalbot.r4b_v2.capture.block_container import (
    BlockSigningAuthorityV2,
    Ed25519BlockSignerV2,
)
from signalbot.r4b_v2.capture.blocks import (
    BlockPolicyV2,
    GroupedBlockBuilderV2,
    GroupedBlockWriterV2,
    parse_raw_record_line_v2,
)
from signalbot.r4b_v2.capture.models import RawRecordV2, TransportV2, VenueV2
from signalbot.r4b_v2.capture.pipeline import CaptureFinalityFenceReceiptV2
from signalbot.r4b_v2.capture.plans import (
    ProvisionalPromotingRestCapturePlanV2,
    build_provisional_promoting_capture_plans_v2,
    provisional_promoting_plan_sha256_v2,
)
from signalbot.r4b_v2.capture.rest import PublicOiRestTerminalObservationV2
from signalbot.r4b_v2.capture.rest_census import (
    PublicOiRestCellOutcomeV2,
    PublicOiRestCoverageCloseV2,
    PublicOiRestForwardGapRangeV2,
    PublicOiRestSlotCensusEntryV2,
    PublicOiRestSlotCensusV2,
    public_oi_rest_attempt_record_sha256_v2,
)
from signalbot.r4b_v2.capture.rest_census_verifier import (
    BODY_SEMANTICS_UNVERIFIED_V2,
    PublicOiRestCensusPrefixVerifierV2,
    PublicOiRestCensusVerificationCertificateV2,
    PublicOiRestCensusVerificationErrorV2,
    create_public_oi_rest_census_prefix_verifier_v2,
    validate_public_oi_rest_census_verification_certificate_v2,
    verify_public_oi_rest_census_prefix_v2,
)
from signalbot.r4b_v2.capture.wal import WalAuthorityV2, WalDurabilityBindingV2

_SESSION_ID = "session-rest-census-verifier"
_PROTOCOL_HASH = hashlib.sha256(b"rest-census-verifier-protocol").hexdigest()
_START_HASH = hashlib.sha256(b"rest-census-verifier-start").hexdigest()
_SLOT = 1_700_000_000_000
_PREFIX_DOMAIN = b"R4B_V2_WAL_BLOCK_PREFIX\0"
_AUTHORITY_HASH = "a" * 64


def _plan_and_bundle() -> tuple[ProvisionalPromotingRestCapturePlanV2, str]:
    plans = build_provisional_promoting_capture_plans_v2(("BTCUSDT", "ETHUSDT"))
    plan = cast(
        ProvisionalPromotingRestCapturePlanV2,
        next(value for value in plans if type(value) is ProvisionalPromotingRestCapturePlanV2),
    )
    return plan, provisional_promoting_plan_sha256_v2(plans)


def _attempt_record(
    plan: ProvisionalPromotingRestCapturePlanV2,
    *,
    ingest_seq: int,
    symbol: str,
    scheduled_slot_wall_ms: int,
    receipt_wall_ms: int,
    receipt_monotonic_ns: int,
    poll_cycle_seq: int = 1,
) -> RawRecordV2:
    ordinal = plan.symbols.index(symbol)
    observation = PublicOiRestTerminalObservationV2.for_plan(
        plan,
        symbol=symbol,
        poll_cycle_seq=poll_cycle_seq,
        symbol_ordinal=ordinal,
        scheduled_slot_wall_ms=scheduled_slot_wall_ms,
        attempt=1,
        request_started_wall_ms=scheduled_slot_wall_ms + 10,
        request_started_monotonic_ns=receipt_monotonic_ns - 30,
        response_first_header_wall_ms=scheduled_slot_wall_ms + 20,
        response_first_header_monotonic_ns=receipt_monotonic_ns - 20,
        attempt_ended_wall_ms=scheduled_slot_wall_ms + 30,
        attempt_ended_monotonic_ns=receipt_monotonic_ns - 10,
        response_status=200,
        response_headers=(),
        payload_complete=True,
        body=(
            b'{"openInterest":"1.0","symbol":"'
            + symbol.encode("ascii")
            + b'","time":1700000000000}'
        ),
    )
    payload = observation(
        ReceiptTimestamp(
            received_at_ms=receipt_wall_ms,
            received_monotonic_ns=receipt_monotonic_ns,
        )
    )
    return RawRecordV2.from_payload(
        session_id=_SESSION_ID,
        plan_id=plan.name,
        protocol_hash=_PROTOCOL_HASH,
        transport=TransportV2.HTTPS,
        venue=VenueV2.USDM_FUTURES,
        route_id=plan.route_id,
        symbol=symbol,
        connection_id="rest-verifier-connection",
        generation=1,
        frame_seq=None,
        ingest_seq=ingest_seq,
        receipt_wall_ms=receipt_wall_ms,
        receipt_monotonic_ns=receipt_monotonic_ns,
        raw_payload=payload,
        source_logical_key=f"openInterest:{symbol}",
    )


def _census_record(
    plan: ProvisionalPromotingRestCapturePlanV2,
    *,
    payload: PublicOiRestSlotCensusV2 | PublicOiRestForwardGapRangeV2 | PublicOiRestCoverageCloseV2,
    ingest_seq: int,
    receipt_wall_ms: int,
    receipt_monotonic_ns: int,
) -> RawRecordV2:
    return RawRecordV2.from_payload(
        session_id=_SESSION_ID,
        plan_id=plan.name,
        protocol_hash=_PROTOCOL_HASH,
        transport=TransportV2.HTTPS,
        venue=VenueV2.USDM_FUTURES,
        route_id=plan.route_id,
        symbol=None,
        connection_id="oi-rest-census",
        generation=1,
        frame_seq=None,
        ingest_seq=ingest_seq,
        receipt_wall_ms=receipt_wall_ms,
        receipt_monotonic_ns=receipt_monotonic_ns,
        raw_payload=payload.canonical_bytes(),
        source_logical_key="openInterest:census",
    )


def _websocket_record(
    *, ingest_seq: int, receipt_wall_ms: int, receipt_monotonic_ns: int
) -> RawRecordV2:
    return RawRecordV2.from_payload(
        session_id=_SESSION_ID,
        plan_id="v2-usdm-market-promoting-abc",
        protocol_hash=_PROTOCOL_HASH,
        transport=TransportV2.WEBSOCKET,
        venue=VenueV2.USDM_FUTURES,
        route_id="usdm_market",
        symbol="BTCUSDT",
        connection_id="ws-verifier-connection",
        generation=1,
        frame_seq=ingest_seq,
        ingest_seq=ingest_seq,
        receipt_wall_ms=receipt_wall_ms,
        receipt_monotonic_ns=receipt_monotonic_ns,
        raw_payload=b"{}",
        source_logical_key="btcusdt@aggTrade",
    )


def _encoded_line(value: RawRecordV2 | QueuedRawRecordV2) -> bytes:
    if type(value) is QueuedRawRecordV2:
        return value.encoded_line
    return canonical_json_line(value)


def _prefix_sha256(records: tuple[RawRecordV2 | QueuedRawRecordV2, ...]) -> str:
    digest = hashlib.sha256(_PREFIX_DOMAIN)
    for ingest_seq, value in enumerate(records, start=1):
        line = _encoded_line(value)
        digest.update(struct.pack(">Q", ingest_seq))
        digest.update(struct.pack(">Q", len(line)))
        digest.update(line)
    return digest.hexdigest()


def _finality_receipt(
    records: tuple[RawRecordV2 | QueuedRawRecordV2, ...],
) -> CaptureFinalityFenceReceiptV2:
    last_value = records[-1]
    last = last_value.record if isinstance(last_value, QueuedRawRecordV2) else last_value
    tail = len(records)
    wal_root = StorageRootBindingV2(
        storage_kind="WAL",
        root_role="PROVISIONAL_SINGLE",
        failure_domain_id="rest-census-verifier-wal",
        authority_sha256=_AUTHORITY_HASH,
        contract_sha256="b" * 64,
    )
    return CaptureFinalityFenceReceiptV2(
        authority_sha256=_AUTHORITY_HASH,
        attempt_id="rest-census-verifier-attempt",
        qualification_id="rest-census-verifier-policy",
        requested_ingest_seq=tail,
        fence_ingest_seq=tail,
        fence_monotonic_ns=last.receipt_monotonic_ns + 100,
        writer_observed_monotonic_ns=last.receipt_monotonic_ns + 100,
        wal_durable_ack_seq=tail,
        finalized_block_tail_ingest_seq=tail,
        durable_record_count=tail,
        exact_prefix_sha256=_prefix_sha256(records),
        wal_durability_binding=WalDurabilityBindingV2(
            mode="SINGLE_ROOT",
            root_bindings=(wal_root,),
            qualification_selection_receipt_sha256=None,
            physical_failure_domain_independence_verified=False,
        ),
        grouped_block_root_binding=StorageRootBindingV2(
            storage_kind="GROUPED_BLOCK",
            root_role="PROVISIONAL_SINGLE",
            failure_domain_id="rest-census-verifier-block",
            authority_sha256=_AUTHORITY_HASH,
            contract_sha256="c" * 64,
        ),
        block_signing_authority_sha256="d" * 64,
        final_block_sequence=1,
        final_block_hash="e" * 64,
        final_block_manifest_sha256="f" * 64,
        final_block_container_sha256="1" * 64,
        target_last_receipt_wall_ms=last.receipt_wall_ms,
        target_last_receipt_monotonic_ns=last.receipt_monotonic_ns,
        stream_group_id="rest-census-verifier-stream",
        segment_id="rest-census-verifier-segment",
    )


def _verify(
    records: tuple[RawRecordV2 | QueuedRawRecordV2, ...],
    *,
    finality_receipt: CaptureFinalityFenceReceiptV2 | None = None,
) -> PublicOiRestCensusVerificationCertificateV2:
    plan, bundle_sha256 = _plan_and_bundle()
    return verify_public_oi_rest_census_prefix_v2(
        iter(records),
        plan=plan,
        session_id=_SESSION_ID,
        protocol_hash=_PROTOCOL_HASH,
        session_start_manifest_sha256=_START_HASH,
        plan_bundle_sha256=bundle_sha256,
        finality_receipt=(
            _finality_receipt(records) if finality_receipt is None else finality_receipt
        ),
    )


def _push_verifier(
    records: tuple[RawRecordV2 | QueuedRawRecordV2, ...],
) -> PublicOiRestCensusPrefixVerifierV2:
    plan, bundle_sha256 = _plan_and_bundle()
    return create_public_oi_rest_census_prefix_verifier_v2(
        plan=plan,
        session_id=_SESSION_ID,
        protocol_hash=_PROTOCOL_HASH,
        session_start_manifest_sha256=_START_HASH,
        plan_bundle_sha256=bundle_sha256,
        finality_receipt=_finality_receipt(records),
    )


def _one_slot_records() -> tuple[RawRecordV2, ...]:
    plan, bundle_sha256 = _plan_and_bundle()
    btc = _attempt_record(
        plan,
        ingest_seq=1,
        symbol="BTCUSDT",
        scheduled_slot_wall_ms=_SLOT,
        receipt_wall_ms=_SLOT + 50,
        receipt_monotonic_ns=100,
    )
    ws_before_second_attempt = _websocket_record(
        ingest_seq=2,
        receipt_wall_ms=_SLOT + 60,
        receipt_monotonic_ns=110,
    )
    eth = _attempt_record(
        plan,
        ingest_seq=3,
        symbol="ETHUSDT",
        scheduled_slot_wall_ms=_SLOT,
        receipt_wall_ms=_SLOT + 70,
        receipt_monotonic_ns=120,
    )
    entries = tuple(
        PublicOiRestSlotCensusEntryV2.for_plan(
            plan,
            session_start_manifest_sha256=_START_HASH,
            plan_bundle_sha256=bundle_sha256,
            symbol_ordinal=ordinal,
            scheduled_slot_wall_ms=_SLOT,
            outcome=PublicOiRestCellOutcomeV2.ATTEMPT_RETAINED,
            attempt_ingest_seq=record.ingest_seq,
            attempt_record_sha256=public_oi_rest_attempt_record_sha256_v2(record),
        )
        for ordinal, record in enumerate((btc, eth))
    )
    slot = PublicOiRestSlotCensusV2.for_plan(
        plan,
        session_id=_SESSION_ID,
        session_start_manifest_sha256=_START_HASH,
        plan_bundle_sha256=bundle_sha256,
        scheduled_slot_wall_ms=_SLOT,
        entries=entries,
        closed_wall_ms=_SLOT + 80,
        closed_monotonic_ns=130,
    )
    slot_record = _census_record(
        plan,
        payload=slot,
        ingest_seq=4,
        receipt_wall_ms=_SLOT + 90,
        receipt_monotonic_ns=140,
    )
    ws_after_slot = _websocket_record(
        ingest_seq=5,
        receipt_wall_ms=_SLOT + 100,
        receipt_monotonic_ns=150,
    )
    close = PublicOiRestCoverageCloseV2.for_plan(
        plan,
        session_id=_SESSION_ID,
        session_start_manifest_sha256=_START_HASH,
        plan_bundle_sha256=bundle_sha256,
        coverage_start_slot_wall_ms=_SLOT,
        stop_requested_wall_ms=_SLOT + 5_000,
        stop_requested_monotonic_ns=160,
        last_census_ingest_seq=4,
    )
    close_record = _census_record(
        plan,
        payload=close,
        ingest_seq=6,
        receipt_wall_ms=_SLOT + 5_001,
        receipt_monotonic_ns=170,
    )
    return (
        btc,
        ws_before_second_attempt,
        eth,
        slot_record,
        ws_after_slot,
        close_record,
    )


def test_verifier_accepts_interleaved_websocket_prefix_and_never_claims_m2() -> None:
    certificate = _verify(_one_slot_records())

    assert certificate.coverage_closed is True
    assert certificate.covered_slot_count == 1
    assert certificate.scheduled_cell_count == 2
    assert certificate.rest_attempt_record_count == 2
    assert certificate.attempt_retained_cell_count == 2
    assert certificate.unstarted_cell_count == 0
    assert certificate.ignored_websocket_record_count == 2
    assert certificate.data_complete is False
    assert certificate.data_completeness_reason == BODY_SEMANTICS_UNVERIFIED_V2
    assert certificate.m2_certified is False
    assert certificate.session_close_authorized is False
    assert certificate.current_storage_reproved is False
    assert (
        validate_public_oi_rest_census_verification_certificate_v2(certificate)
        == certificate.certificate_sha256
    )


def test_verifier_is_restart_deterministic_for_queued_and_reparsed_raw_prefix() -> None:
    raw_records = _one_slot_records()
    queued = tuple(
        QueuedRawRecordV2.encode(
            record,
            enqueued_monotonic_ns=record.receipt_monotonic_ns,
        )
        for record in raw_records
    )
    receipt = _finality_receipt(queued)
    from_queued = _verify(queued, finality_receipt=receipt)
    reparsed = tuple(parse_raw_record_line_v2(value.encoded_line) for value in queued)
    from_restart = _verify(reparsed, finality_receipt=receipt)

    assert from_queued == from_restart
    assert from_queued.certificate_sha256 == from_restart.certificate_sha256


def test_push_verifier_matches_iterable_api_across_arbitrary_split_boundaries() -> None:
    records = _one_slot_records()
    verifier = _push_verifier(records)

    for chunk in (records[:2], records[2:5], records[5:]):
        for record in chunk:
            verifier.consume(record.ingest_seq, canonical_json_line(record))

    from_push = verifier.finalize()
    from_iterable = _verify(records)

    assert from_push == from_iterable
    assert from_push.certificate_sha256 == from_iterable.certificate_sha256
    assert (
        from_push.certificate_sha256
        == "a9a8e492f3a7e43b77badd8d1cc638682abdc5ed179355253be37a269c22dae9"
    )


def test_push_verifier_rejects_callback_and_encoded_ingest_mismatch() -> None:
    records = _one_slot_records()
    verifier = _push_verifier(records)

    with pytest.raises(
        PublicOiRestCensusVerificationErrorV2,
        match="callback ingest sequence differs",
    ):
        verifier.consume(2, canonical_json_line(records[0]))

    with pytest.raises(PublicOiRestCensusVerificationErrorV2, match="failed"):
        verifier.consume(1, canonical_json_line(records[0]))


def test_push_verifier_rejects_matching_but_noncontiguous_callback_sequence() -> None:
    records = _one_slot_records()
    verifier = _push_verifier(records)

    with pytest.raises(
        PublicOiRestCensusVerificationErrorV2,
        match=r"exact ingest sequences 1\.\.tail",
    ):
        verifier.consume(2, canonical_json_line(records[1]))


def test_push_verifier_early_finalize_is_terminal() -> None:
    records = _one_slot_records()
    verifier = _push_verifier(records)
    for record in records[:3]:
        verifier.consume(record.ingest_seq, canonical_json_line(record))

    with pytest.raises(
        PublicOiRestCensusVerificationErrorV2,
        match="does not reach the exact finality-fence tail",
    ):
        verifier.finalize()

    with pytest.raises(PublicOiRestCensusVerificationErrorV2, match="failed"):
        verifier.consume(records[3].ingest_seq, canonical_json_line(records[3]))
    with pytest.raises(PublicOiRestCensusVerificationErrorV2, match="failed"):
        verifier.finalize()


def test_push_verifier_rejects_consume_and_double_finalize_after_success() -> None:
    records = _one_slot_records()
    verifier = _push_verifier(records)
    for record in records:
        verifier.consume(record.ingest_seq, canonical_json_line(record))

    verifier.finalize()

    with pytest.raises(PublicOiRestCensusVerificationErrorV2, match="finalized"):
        verifier.consume(records[-1].ingest_seq, canonical_json_line(records[-1]))
    with pytest.raises(PublicOiRestCensusVerificationErrorV2, match="finalized"):
        verifier.finalize()


def test_grouped_block_writer_streams_directly_into_push_verifier(
    tmp_path: Path,
) -> None:
    records = _one_slot_records()
    queued = tuple(
        QueuedRawRecordV2.encode(
            record,
            enqueued_monotonic_ns=record.receipt_monotonic_ns,
        )
        for record in records
    )
    authority = WalAuthorityV2(
        attempt_id="rest-census-push-blocks",
        protocol_sha256=_PROTOCOL_HASH,
        plan_sha256="2" * 64,
        source_manifest_sha256="3" * 64,
        schema_sha256="4" * 64,
        runtime_manifest_sha256="5" * 64,
    )
    signer = Ed25519BlockSignerV2.from_private_key_bytes(
        key_id="rest-census-push-writer",
        private_key_bytes=bytes(range(32)),
    )
    signing_authority = BlockSigningAuthorityV2.from_public_key_bytes(
        key_id=signer.key_id,
        public_key_bytes=signer.public_key_bytes,
    )
    policy = BlockPolicyV2(
        qualification_id="rest-census-push-zstd",
        codec_candidate_id="rest-census-push-zstd-candidate",
        compression_level=9,
        max_uncompressed_bytes=4 * 1024 * 1024,
        max_linger_ms=1_000,
    )
    writer = GroupedBlockWriterV2(
        tmp_path / "blocks",
        authority=authority,
        policy=policy,
        signer=signer,
        signing_authority=signing_authority,
        stream_group_id="rest-census-push-stream",
        segment_id="rest-census-push-segment",
        maximum_total_bytes=8 * 1024 * 1024,
        emergency_reserve_bytes=1024,
    )
    builder = GroupedBlockBuilderV2(policy)
    for index, queued_record in enumerate(queued, start=1):
        assert builder.offer(queued_record, now_ns=index) == ()
        if index % 2 == 0:
            block = builder.flush_finality_fence(now_ns=index)
            assert block is not None
            writer.commit(block)

    verifier = _push_verifier(records)
    delivered = writer.consume_committed_records(verifier.consume)
    from_blocks = verifier.finalize()

    assert delivered == len(records)
    assert from_blocks == _verify(records)


def test_iterable_api_validates_authority_before_advancing_records() -> None:
    records = _one_slot_records()
    plan, bundle_sha256 = _plan_and_bundle()
    advanced = False

    def lazy_records() -> Iterator[RawRecordV2]:
        nonlocal advanced
        advanced = True
        yield from records

    with pytest.raises(
        PublicOiRestCensusVerificationErrorV2,
        match="protocol_hash must be a lowercase SHA-256 digest",
    ):
        verify_public_oi_rest_census_prefix_v2(
            lazy_records(),
            plan=plan,
            session_id=_SESSION_ID,
            protocol_hash="invalid",
            session_start_manifest_sha256=_START_HASH,
            plan_bundle_sha256=bundle_sha256,
            finality_receipt=_finality_receipt(records),
        )

    assert advanced is False


def test_verifier_accepts_exact_empty_half_open_stop_boundary() -> None:
    plan, bundle_sha256 = _plan_and_bundle()
    close = PublicOiRestCoverageCloseV2.for_plan(
        plan,
        session_id=_SESSION_ID,
        session_start_manifest_sha256=_START_HASH,
        plan_bundle_sha256=bundle_sha256,
        coverage_start_slot_wall_ms=_SLOT,
        stop_requested_wall_ms=_SLOT,
        stop_requested_monotonic_ns=100,
        last_census_ingest_seq=None,
    )
    records = (
        _census_record(
            plan,
            payload=close,
            ingest_seq=1,
            receipt_wall_ms=_SLOT,
            receipt_monotonic_ns=110,
        ),
    )

    certificate = _verify(records)

    assert certificate.coverage_start_slot_wall_ms == _SLOT
    assert certificate.coverage_end_slot_exclusive_wall_ms == _SLOT
    assert certificate.covered_slot_count == 0
    assert certificate.last_census_ingest_seq is None


def test_verifier_accepts_compact_contiguous_forward_gap() -> None:
    plan, bundle_sha256 = _plan_and_bundle()
    gap = PublicOiRestForwardGapRangeV2.for_plan(
        plan,
        session_id=_SESSION_ID,
        session_start_manifest_sha256=_START_HASH,
        plan_bundle_sha256=bundle_sha256,
        first_slot_wall_ms=_SLOT,
        end_slot_exclusive_wall_ms=_SLOT + 10_000,
        observed_wall_ms=_SLOT + 10_000,
        observed_monotonic_ns=100,
    )
    gap_record = _census_record(
        plan,
        payload=gap,
        ingest_seq=1,
        receipt_wall_ms=_SLOT + 10_000,
        receipt_monotonic_ns=110,
    )
    close = PublicOiRestCoverageCloseV2.for_plan(
        plan,
        session_id=_SESSION_ID,
        session_start_manifest_sha256=_START_HASH,
        plan_bundle_sha256=bundle_sha256,
        coverage_start_slot_wall_ms=_SLOT,
        stop_requested_wall_ms=_SLOT + 10_000,
        stop_requested_monotonic_ns=120,
        last_census_ingest_seq=1,
    )
    records = (
        gap_record,
        _census_record(
            plan,
            payload=close,
            ingest_seq=2,
            receipt_wall_ms=_SLOT + 10_000,
            receipt_monotonic_ns=130,
        ),
    )

    certificate = _verify(records)

    assert certificate.forward_gap_record_count == 1
    assert certificate.covered_slot_count == 2
    assert certificate.scheduled_cell_count == 4
    assert certificate.unstarted_cell_count == 4


def test_verifier_rejects_orphan_attempt_before_forward_gap() -> None:
    plan, bundle_sha256 = _plan_and_bundle()
    attempt = _attempt_record(
        plan,
        ingest_seq=1,
        symbol="BTCUSDT",
        scheduled_slot_wall_ms=_SLOT,
        receipt_wall_ms=_SLOT + 50,
        receipt_monotonic_ns=100,
    )
    gap = PublicOiRestForwardGapRangeV2.for_plan(
        plan,
        session_id=_SESSION_ID,
        session_start_manifest_sha256=_START_HASH,
        plan_bundle_sha256=bundle_sha256,
        first_slot_wall_ms=_SLOT,
        end_slot_exclusive_wall_ms=_SLOT + 5_000,
        observed_wall_ms=_SLOT + 5_000,
        observed_monotonic_ns=110,
    )
    gap_record = _census_record(
        plan,
        payload=gap,
        ingest_seq=2,
        receipt_wall_ms=_SLOT + 5_000,
        receipt_monotonic_ns=120,
    )
    close = PublicOiRestCoverageCloseV2.for_plan(
        plan,
        session_id=_SESSION_ID,
        session_start_manifest_sha256=_START_HASH,
        plan_bundle_sha256=bundle_sha256,
        coverage_start_slot_wall_ms=_SLOT,
        stop_requested_wall_ms=_SLOT + 5_000,
        stop_requested_monotonic_ns=130,
        last_census_ingest_seq=2,
    )
    records = (
        attempt,
        gap_record,
        _census_record(
            plan,
            payload=close,
            ingest_seq=3,
            receipt_wall_ms=_SLOT + 5_000,
            receipt_monotonic_ns=140,
        ),
    )

    with pytest.raises(PublicOiRestCensusVerificationErrorV2, match="orphan"):
        _verify(records)


def test_verifier_rejects_tampered_attempt_reference_hash() -> None:
    records = list(_one_slot_records())
    plan, bundle_sha256 = _plan_and_bundle()
    original_slot = PublicOiRestSlotCensusV2.from_canonical_bytes(
        records[3].payload_bytes(), plan=plan
    )
    entries = list(original_slot.entries)
    entries[0] = replace(entries[0], attempt_record_sha256="0" * 64)
    tampered_slot = PublicOiRestSlotCensusV2.for_plan(
        plan,
        session_id=_SESSION_ID,
        session_start_manifest_sha256=_START_HASH,
        plan_bundle_sha256=bundle_sha256,
        scheduled_slot_wall_ms=_SLOT,
        entries=tuple(entries),
        closed_wall_ms=original_slot.closed_wall_ms,
        closed_monotonic_ns=original_slot.closed_monotonic_ns,
    )
    records[3] = _census_record(
        plan,
        payload=tampered_slot,
        ingest_seq=4,
        receipt_wall_ms=_SLOT + 90,
        receipt_monotonic_ns=140,
    )

    with pytest.raises(PublicOiRestCensusVerificationErrorV2, match="reference"):
        _verify(tuple(records))


def test_verifier_rejects_slot_terminal_clock_before_referenced_attempt() -> None:
    records = list(_one_slot_records())
    plan, _ = _plan_and_bundle()
    original_slot = PublicOiRestSlotCensusV2.from_canonical_bytes(
        records[3].payload_bytes(), plan=plan
    )
    backdated_slot = replace(
        original_slot,
        closed_wall_ms=_SLOT + 60,
        closed_monotonic_ns=115,
    )
    records[3] = _census_record(
        plan,
        payload=backdated_slot,
        ingest_seq=4,
        receipt_wall_ms=_SLOT + 90,
        receipt_monotonic_ns=140,
    )

    with pytest.raises(PublicOiRestCensusVerificationErrorV2, match="terminal clocks"):
        _verify(tuple(records))


def test_verifier_reparses_and_rejects_noncanonical_queued_line() -> None:
    records = _one_slot_records()
    first = records[0]
    canonical = canonical_json_line(first)
    noncanonical = b"{ " + canonical[1:]
    forged = QueuedRawRecordV2(
        record=first,
        encoded_line=noncanonical,
        encoded_len=len(noncanonical),
        encoded_sha256=hashlib.sha256(noncanonical).hexdigest(),
        raw_len=first.raw_len,
        ingest_seq=first.ingest_seq,
        enqueued_monotonic_ns=first.receipt_monotonic_ns,
    )
    mixed: tuple[RawRecordV2 | QueuedRawRecordV2, ...] = (forged, *records[1:])

    with pytest.raises(PublicOiRestCensusVerificationErrorV2, match="canonical"):
        _verify(mixed)


def test_verifier_rejects_old_slot_attempt_after_census_carrier() -> None:
    plan, bundle_sha256 = _plan_and_bundle()
    entries = tuple(
        PublicOiRestSlotCensusEntryV2.for_plan(
            plan,
            session_start_manifest_sha256=_START_HASH,
            plan_bundle_sha256=bundle_sha256,
            symbol_ordinal=ordinal,
            scheduled_slot_wall_ms=_SLOT,
            outcome=PublicOiRestCellOutcomeV2.UNSTARTED_SLOT_EXPIRED,
        )
        for ordinal in range(len(plan.symbols))
    )
    slot = PublicOiRestSlotCensusV2.for_plan(
        plan,
        session_id=_SESSION_ID,
        session_start_manifest_sha256=_START_HASH,
        plan_bundle_sha256=bundle_sha256,
        scheduled_slot_wall_ms=_SLOT,
        entries=entries,
        closed_wall_ms=_SLOT + 5_000,
        closed_monotonic_ns=100,
    )
    slot_record = _census_record(
        plan,
        payload=slot,
        ingest_seq=1,
        receipt_wall_ms=_SLOT + 5_000,
        receipt_monotonic_ns=110,
    )
    late = _attempt_record(
        plan,
        ingest_seq=2,
        symbol="BTCUSDT",
        scheduled_slot_wall_ms=_SLOT,
        receipt_wall_ms=_SLOT + 5_010,
        receipt_monotonic_ns=120,
    )
    close = PublicOiRestCoverageCloseV2.for_plan(
        plan,
        session_id=_SESSION_ID,
        session_start_manifest_sha256=_START_HASH,
        plan_bundle_sha256=bundle_sha256,
        coverage_start_slot_wall_ms=_SLOT,
        stop_requested_wall_ms=_SLOT + 5_000,
        stop_requested_monotonic_ns=130,
        last_census_ingest_seq=1,
    )
    records = (
        slot_record,
        late,
        _census_record(
            plan,
            payload=close,
            ingest_seq=3,
            receipt_wall_ms=_SLOT + 5_020,
            receipt_monotonic_ns=140,
        ),
    )

    with pytest.raises(PublicOiRestCensusVerificationErrorV2, match="earlier or later"):
        _verify(records)


def test_verifier_accepts_non_rest_websocket_after_coverage_close() -> None:
    plan, bundle_sha256 = _plan_and_bundle()
    close = PublicOiRestCoverageCloseV2.for_plan(
        plan,
        session_id=_SESSION_ID,
        session_start_manifest_sha256=_START_HASH,
        plan_bundle_sha256=bundle_sha256,
        coverage_start_slot_wall_ms=_SLOT,
        stop_requested_wall_ms=_SLOT,
        stop_requested_monotonic_ns=100,
        last_census_ingest_seq=None,
    )
    records = (
        _census_record(
            plan,
            payload=close,
            ingest_seq=1,
            receipt_wall_ms=_SLOT,
            receipt_monotonic_ns=110,
        ),
        _websocket_record(
            ingest_seq=2,
            receipt_wall_ms=_SLOT + 1,
            receipt_monotonic_ns=120,
        ),
    )

    certificate = _verify(records)

    assert certificate.coverage_close_ingest_seq == 1
    assert certificate.verified_prefix_tail_ingest_seq == 2
    assert certificate.ignored_websocket_record_count == 1


def test_verifier_rejects_public_oi_attempt_after_coverage_close() -> None:
    plan, bundle_sha256 = _plan_and_bundle()
    close = PublicOiRestCoverageCloseV2.for_plan(
        plan,
        session_id=_SESSION_ID,
        session_start_manifest_sha256=_START_HASH,
        plan_bundle_sha256=bundle_sha256,
        coverage_start_slot_wall_ms=_SLOT,
        stop_requested_wall_ms=_SLOT,
        stop_requested_monotonic_ns=100,
        last_census_ingest_seq=None,
    )
    records = (
        _census_record(
            plan,
            payload=close,
            ingest_seq=1,
            receipt_wall_ms=_SLOT,
            receipt_monotonic_ns=110,
        ),
        _attempt_record(
            plan,
            ingest_seq=2,
            symbol="BTCUSDT",
            scheduled_slot_wall_ms=_SLOT,
            receipt_wall_ms=_SLOT + 50,
            receipt_monotonic_ns=120,
        ),
    )

    with pytest.raises(PublicOiRestCensusVerificationErrorV2, match="non-REST WebSocket"):
        _verify(records)


def test_verifier_rejects_census_carrier_after_coverage_close() -> None:
    plan, bundle_sha256 = _plan_and_bundle()
    close = PublicOiRestCoverageCloseV2.for_plan(
        plan,
        session_id=_SESSION_ID,
        session_start_manifest_sha256=_START_HASH,
        plan_bundle_sha256=bundle_sha256,
        coverage_start_slot_wall_ms=_SLOT,
        stop_requested_wall_ms=_SLOT,
        stop_requested_monotonic_ns=100,
        last_census_ingest_seq=None,
    )
    records = (
        _census_record(
            plan,
            payload=close,
            ingest_seq=1,
            receipt_wall_ms=_SLOT,
            receipt_monotonic_ns=110,
        ),
        _census_record(
            plan,
            payload=close,
            ingest_seq=2,
            receipt_wall_ms=_SLOT + 1,
            receipt_monotonic_ns=120,
        ),
    )

    with pytest.raises(PublicOiRestCensusVerificationErrorV2, match="non-REST WebSocket"):
        _verify(records)


def test_verifier_rejects_finality_prefix_digest_tamper() -> None:
    records = _one_slot_records()
    receipt = replace(_finality_receipt(records), exact_prefix_sha256="0" * 64)

    with pytest.raises(PublicOiRestCensusVerificationErrorV2, match="prefix digest"):
        _verify(records, finality_receipt=receipt)


def test_verifier_rejects_duplicate_attempt_for_one_schedule_cell() -> None:
    plan, _ = _plan_and_bundle()
    first = _attempt_record(
        plan,
        ingest_seq=1,
        symbol="BTCUSDT",
        scheduled_slot_wall_ms=_SLOT,
        receipt_wall_ms=_SLOT + 50,
        receipt_monotonic_ns=100,
    )
    duplicate = _attempt_record(
        plan,
        ingest_seq=2,
        symbol="BTCUSDT",
        scheduled_slot_wall_ms=_SLOT,
        receipt_wall_ms=_SLOT + 60,
        receipt_monotonic_ns=110,
    )

    with pytest.raises(PublicOiRestCensusVerificationErrorV2, match="duplicate"):
        _verify((first, duplicate))


def test_verifier_bounds_census_payload_before_schema_selection() -> None:
    plan, _ = _plan_and_bundle()
    oversized = RawRecordV2.from_payload(
        session_id=_SESSION_ID,
        plan_id=plan.name,
        protocol_hash=_PROTOCOL_HASH,
        transport=TransportV2.HTTPS,
        venue=VenueV2.USDM_FUTURES,
        route_id=plan.route_id,
        symbol=None,
        connection_id="oi-rest-census",
        generation=1,
        frame_seq=None,
        ingest_seq=1,
        receipt_wall_ms=_SLOT,
        receipt_monotonic_ns=100,
        raw_payload=b"{" + (b" " * 12_000),
        source_logical_key="openInterest:census",
    )

    with pytest.raises(PublicOiRestCensusVerificationErrorV2, match="bounded"):
        _verify((oversized,))


def test_verifier_rejects_foreign_https_attempt_even_when_interleaving_is_allowed() -> None:
    records = list(_one_slot_records())
    records[0] = replace(records[0], plan_id="foreign-rest-plan")

    with pytest.raises(PublicOiRestCensusVerificationErrorV2, match="foreign or malformed"):
        _verify(tuple(records))


def test_verifier_rejects_close_with_nonexact_last_census_reference() -> None:
    records = list(_one_slot_records())
    plan, _ = _plan_and_bundle()
    original_close = PublicOiRestCoverageCloseV2.from_canonical_bytes(
        records[-1].payload_bytes(), plan=plan
    )
    wrong_close = replace(original_close, last_census_ingest_seq=3)
    records[-1] = _census_record(
        plan,
        payload=wrong_close,
        ingest_seq=6,
        receipt_wall_ms=_SLOT + 5_001,
        receipt_monotonic_ns=170,
    )

    with pytest.raises(PublicOiRestCensusVerificationErrorV2, match="last-census"):
        _verify(tuple(records))


def test_verifier_does_not_ignore_attempts_before_declared_coverage_start() -> None:
    records = list(_one_slot_records())
    plan, bundle_sha256 = _plan_and_bundle()
    shifted_close = PublicOiRestCoverageCloseV2.for_plan(
        plan,
        session_id=_SESSION_ID,
        session_start_manifest_sha256=_START_HASH,
        plan_bundle_sha256=bundle_sha256,
        coverage_start_slot_wall_ms=_SLOT + 5_000,
        stop_requested_wall_ms=_SLOT + 5_000,
        stop_requested_monotonic_ns=160,
        last_census_ingest_seq=None,
    )
    records[-1] = _census_record(
        plan,
        payload=shifted_close,
        ingest_seq=6,
        receipt_wall_ms=_SLOT + 5_001,
        receipt_monotonic_ns=170,
    )

    with pytest.raises(PublicOiRestCensusVerificationErrorV2, match="contiguous coverage"):
        _verify(tuple(records))


def test_certificate_binds_finality_receipt_clocks_against_replay() -> None:
    records = _one_slot_records()
    first_receipt = _finality_receipt(records)
    second_receipt = replace(
        first_receipt,
        fence_monotonic_ns=first_receipt.fence_monotonic_ns + 1,
        writer_observed_monotonic_ns=(first_receipt.writer_observed_monotonic_ns + 1),
    )

    first = _verify(records, finality_receipt=first_receipt)
    second = _verify(records, finality_receipt=second_receipt)

    assert first.finality_exact_prefix_sha256 == second.finality_exact_prefix_sha256
    assert first.finality_prefix_proof_sha256 == second.finality_prefix_proof_sha256
    assert first.finality_receipt_sha256 != second.finality_receipt_sha256
    assert first.certificate_sha256 != second.certificate_sha256


def test_certificate_constructor_is_not_publicly_forgeable() -> None:
    certificate = _verify(_one_slot_records())
    values = {
        model_field.name: getattr(certificate, model_field.name)
        for model_field in certificate.__dataclass_fields__.values()
        if model_field.init and model_field.name != "_factory_token"
    }

    with pytest.raises(TypeError, match="factory-sealed"):
        PublicOiRestCensusVerificationCertificateV2(**values)  # type: ignore[arg-type]


def test_certificate_validator_rejects_post_factory_mutation() -> None:
    certificate = _verify(_one_slot_records())
    object.__setattr__(certificate, "data_complete", True)

    with pytest.raises(PublicOiRestCensusVerificationErrorV2, match="may not claim"):
        validate_public_oi_rest_census_verification_certificate_v2(certificate)
