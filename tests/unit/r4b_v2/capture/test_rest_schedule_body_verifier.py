from __future__ import annotations

import base64
import hashlib
from dataclasses import replace
from pathlib import Path

import pytest

from signalbot.r4b_v2.canonical import canonical_json_line
from signalbot.r4b_v2.capture.batching import QueuedRawRecordV2
from signalbot.r4b_v2.capture.block_container import (
    BlockSigningAuthorityV2,
    Ed25519BlockSignerV2,
)
from signalbot.r4b_v2.capture.blocks import (
    BlockPolicyV2,
    GroupedBlockBuilderV2,
    GroupedBlockWriterV2,
)
from signalbot.r4b_v2.capture.models import RawRecordV2
from signalbot.r4b_v2.capture.pipeline import CaptureFinalityFenceReceiptV2
from signalbot.r4b_v2.capture.rest import PublicOiRestAttemptPayloadV2
from signalbot.r4b_v2.capture.rest_census import (
    PublicOiRestCellOutcomeV2,
    PublicOiRestCoverageCloseV2,
    PublicOiRestSlotCensusEntryV2,
    PublicOiRestSlotCensusV2,
    public_oi_rest_attempt_record_sha256_v2,
)
from signalbot.r4b_v2.capture.rest_census_verifier import (
    PublicOiRestCensusVerificationCertificateV2,
)
from signalbot.r4b_v2.capture.rest_schedule_body_verifier import (
    BINANCE_TRANSACTION_TIME_CAUSAL_BOUND_REASON_V2,
    PUBLIC_OI_SCHEDULE_BODY_VERIFICATION_SCOPE_V2,
    PublicOiScheduleBodyPrefixVerifierV2,
    PublicOiScheduleBodyVerificationCertificateV2,
    PublicOiScheduleBodyVerificationErrorV2,
    create_public_oi_schedule_body_prefix_verifier_v2,
    validate_public_oi_schedule_body_verification_certificate_v2,
    verify_public_oi_schedule_bodies_v2,
)
from signalbot.r4b_v2.capture.wal import WalAuthorityV2

from .test_rest_census_verifier import (
    _PROTOCOL_HASH,
    _SESSION_ID,
    _SLOT,
    _START_HASH,
    _attempt_record,
    _census_record,
    _finality_receipt,
    _one_slot_records,
    _plan_and_bundle,
    _verify,
    _websocket_record,
)


def _successor_certificate(
    records: tuple[RawRecordV2 | QueuedRawRecordV2, ...],
    *,
    schedule_certificate: PublicOiRestCensusVerificationCertificateV2 | None = None,
    finality_receipt_override: CaptureFinalityFenceReceiptV2 | None = None,
) -> PublicOiScheduleBodyVerificationCertificateV2:
    plan, bundle_sha256 = _plan_and_bundle()
    finality_receipt = (
        _finality_receipt(records)
        if finality_receipt_override is None
        else finality_receipt_override
    )
    schedule = (
        _verify(records, finality_receipt=finality_receipt)
        if schedule_certificate is None
        else schedule_certificate
    )
    return verify_public_oi_schedule_bodies_v2(
        iter(records),
        plan=plan,
        session_id=_SESSION_ID,
        protocol_hash=_PROTOCOL_HASH,
        session_start_manifest_sha256=_START_HASH,
        plan_bundle_sha256=bundle_sha256,
        finality_receipt=finality_receipt,
        observed_schedule_certificate=schedule,
    )


def _push_verifier(
    records: tuple[RawRecordV2 | QueuedRawRecordV2, ...],
) -> PublicOiScheduleBodyPrefixVerifierV2:
    plan, bundle_sha256 = _plan_and_bundle()
    finality_receipt = _finality_receipt(records)
    return create_public_oi_schedule_body_prefix_verifier_v2(
        plan=plan,
        session_id=_SESSION_ID,
        protocol_hash=_PROTOCOL_HASH,
        session_start_manifest_sha256=_START_HASH,
        plan_bundle_sha256=bundle_sha256,
        finality_receipt=finality_receipt,
        observed_schedule_certificate=_verify(
            records,
            finality_receipt=finality_receipt,
        ),
    )


def _records_with_replaced_first_body(body: bytes) -> tuple[RawRecordV2, ...]:
    records = list(_one_slot_records())
    plan, _ = _plan_and_bundle()
    original_record = records[0]
    original_payload = PublicOiRestAttemptPayloadV2.from_canonical_bytes(
        original_record.payload_bytes(), plan=plan
    )
    changed_payload = replace(
        original_payload,
        body_len=len(body),
        body_sha256=hashlib.sha256(body).hexdigest(),
        body_base64=base64.b64encode(body).decode("ascii"),
    )
    changed_record = RawRecordV2.from_payload(
        session_id=original_record.session_id,
        plan_id=original_record.plan_id,
        protocol_hash=original_record.protocol_hash,
        transport=original_record.transport,
        venue=original_record.venue,
        route_id=original_record.route_id,
        symbol=original_record.symbol,
        connection_id=original_record.connection_id,
        generation=original_record.generation,
        frame_seq=original_record.frame_seq,
        ingest_seq=original_record.ingest_seq,
        receipt_wall_ms=original_record.receipt_wall_ms,
        receipt_monotonic_ns=original_record.receipt_monotonic_ns,
        raw_payload=changed_payload.canonical_bytes(),
        source_logical_key=original_record.source_logical_key,
    )
    records[0] = changed_record

    original_slot = PublicOiRestSlotCensusV2.from_canonical_bytes(
        records[3].payload_bytes(), plan=plan
    )
    entries = list(original_slot.entries)
    entries[0] = replace(
        entries[0],
        attempt_record_sha256=public_oi_rest_attempt_record_sha256_v2(changed_record),
    )
    records[3] = _census_record(
        plan,
        payload=replace(original_slot, entries=tuple(entries)),
        ingest_seq=4,
        receipt_wall_ms=_SLOT + 90,
        receipt_monotonic_ns=140,
    )
    return tuple(records)


def _one_slot_with_unstarted_cell() -> tuple[RawRecordV2, ...]:
    plan, bundle_sha256 = _plan_and_bundle()
    btc = _attempt_record(
        plan,
        ingest_seq=1,
        symbol="BTCUSDT",
        scheduled_slot_wall_ms=_SLOT,
        receipt_wall_ms=_SLOT + 50,
        receipt_monotonic_ns=100,
    )
    entries = (
        PublicOiRestSlotCensusEntryV2.for_plan(
            plan,
            session_start_manifest_sha256=_START_HASH,
            plan_bundle_sha256=bundle_sha256,
            symbol_ordinal=0,
            scheduled_slot_wall_ms=_SLOT,
            outcome=PublicOiRestCellOutcomeV2.ATTEMPT_RETAINED,
            attempt_ingest_seq=1,
            attempt_record_sha256=public_oi_rest_attempt_record_sha256_v2(btc),
        ),
        PublicOiRestSlotCensusEntryV2.for_plan(
            plan,
            session_start_manifest_sha256=_START_HASH,
            plan_bundle_sha256=bundle_sha256,
            symbol_ordinal=1,
            scheduled_slot_wall_ms=_SLOT,
            outcome=PublicOiRestCellOutcomeV2.UNSTARTED_NORMAL_STOP,
        ),
    )
    slot = PublicOiRestSlotCensusV2.for_plan(
        plan,
        session_id=_SESSION_ID,
        session_start_manifest_sha256=_START_HASH,
        plan_bundle_sha256=bundle_sha256,
        scheduled_slot_wall_ms=_SLOT,
        entries=entries,
        closed_wall_ms=_SLOT + 80,
        closed_monotonic_ns=110,
    )
    slot_record = _census_record(
        plan,
        payload=slot,
        ingest_seq=2,
        receipt_wall_ms=_SLOT + 90,
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
    return (
        btc,
        slot_record,
        _census_record(
            plan,
            payload=close,
            ingest_seq=3,
            receipt_wall_ms=_SLOT + 5_000,
            receipt_monotonic_ns=140,
        ),
    )


def _empty_schedule_records() -> tuple[RawRecordV2, ...]:
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
    return (
        _census_record(
            plan,
            payload=close,
            ingest_seq=1,
            receipt_wall_ms=_SLOT,
            receipt_monotonic_ns=110,
        ),
    )


def test_verifier_certifies_only_exact_schedule_body_scope() -> None:
    certificate = _successor_certificate(_one_slot_records())

    assert certificate.verification_scope == PUBLIC_OI_SCHEDULE_BODY_VERIFICATION_SCOPE_V2
    assert certificate.schedule_body_complete is True
    assert certificate.body_semantics_verified is True
    assert certificate.covered_slot_count == 1
    assert certificate.scheduled_cell_count == 2
    assert certificate.verified_body_count == 2
    assert certificate.ignored_websocket_record_count == 2
    assert certificate.freshness_verified is False
    assert certificate.transaction_time_causally_bounded is False
    assert (
        certificate.transaction_time_causal_bound_reason
        == BINANCE_TRANSACTION_TIME_CAUSAL_BOUND_REASON_V2
    )
    assert certificate.websocket_completeness_verified is False
    assert certificate.m2_certified is False
    assert certificate.session_close_authorized is False
    assert certificate.profitability_verified is False
    assert certificate.current_storage_reproved is False
    assert (
        validate_public_oi_schedule_body_verification_certificate_v2(certificate)
        == certificate.certificate_sha256
    )


def test_verifier_is_deterministic_for_raw_and_queued_exact_prefix() -> None:
    raw = _one_slot_records()
    receipt = _finality_receipt(raw)
    schedule = _verify(raw, finality_receipt=receipt)
    raw_certificate = _successor_certificate(
        raw,
        schedule_certificate=schedule,
        finality_receipt_override=receipt,
    )
    queued = tuple(
        QueuedRawRecordV2.encode(
            record,
            enqueued_monotonic_ns=record.receipt_monotonic_ns,
        )
        for record in raw
    )
    queued_certificate = _successor_certificate(
        queued,
        schedule_certificate=schedule,
        finality_receipt_override=receipt,
    )

    assert raw_certificate == queued_certificate
    assert (
        raw_certificate.verified_body_bindings_sha256
        == queued_certificate.verified_body_bindings_sha256
    )


def test_push_verifier_matches_iterable_api_across_split_boundaries() -> None:
    records = _one_slot_records()
    verifier = _push_verifier(records)

    for chunk in (records[:1], records[1:4], records[4:]):
        for record in chunk:
            verifier.consume(record.ingest_seq, canonical_json_line(record))

    from_push = verifier.finalize()
    from_iterable = _successor_certificate(records)

    assert from_push == from_iterable
    assert (
        from_push.certificate_sha256
        == "ad7b0df98fe37799dcd353aa0e7cab48a3cc5ee08cb2bf280fa3ad30590d493f"
    )
    assert (
        from_push.verified_body_bindings_sha256
        == "8a08171e408668438774b203e9c724131c861c25b0ca4256c85adfe0a0d2bbf2"
    )


def test_push_verifier_constructor_is_factory_sealed() -> None:
    records = _one_slot_records()
    plan, bundle_sha256 = _plan_and_bundle()
    finality_receipt = _finality_receipt(records)

    with pytest.raises(TypeError, match="created by their factory"):
        PublicOiScheduleBodyPrefixVerifierV2(
            plan=plan,
            session_id=_SESSION_ID,
            protocol_hash=_PROTOCOL_HASH,
            session_start_manifest_sha256=_START_HASH,
            plan_bundle_sha256=bundle_sha256,
            finality_receipt=finality_receipt,
            observed_schedule_certificate=_verify(
                records,
                finality_receipt=finality_receipt,
            ),
        )


def test_push_verifier_rejects_callback_and_contiguous_sequence_mismatches() -> None:
    records = _one_slot_records()
    callback_mismatch = _push_verifier(records)

    with pytest.raises(
        PublicOiScheduleBodyVerificationErrorV2,
        match="callback ingest sequence differs",
    ):
        callback_mismatch.consume(2, canonical_json_line(records[0]))
    with pytest.raises(PublicOiScheduleBodyVerificationErrorV2, match="failed"):
        callback_mismatch.finalize()

    noncontiguous = _push_verifier(records)
    with pytest.raises(
        PublicOiScheduleBodyVerificationErrorV2,
        match=r"exact ingest sequences 1\.\.tail",
    ):
        noncontiguous.consume(2, canonical_json_line(records[1]))


def test_push_verifier_early_finalize_is_terminal() -> None:
    records = _one_slot_records()
    verifier = _push_verifier(records)
    for record in records[:2]:
        verifier.consume(record.ingest_seq, canonical_json_line(record))

    with pytest.raises(
        PublicOiScheduleBodyVerificationErrorV2,
        match="does not reach the exact finality-fence tail",
    ):
        verifier.finalize()
    with pytest.raises(PublicOiScheduleBodyVerificationErrorV2, match="failed"):
        verifier.consume(records[2].ingest_seq, canonical_json_line(records[2]))
    with pytest.raises(PublicOiScheduleBodyVerificationErrorV2, match="failed"):
        verifier.finalize()


def test_push_verifier_rejects_consume_and_double_finalize_after_success() -> None:
    records = _one_slot_records()
    verifier = _push_verifier(records)
    for record in records:
        verifier.consume(record.ingest_seq, canonical_json_line(record))

    verifier.finalize()

    with pytest.raises(PublicOiScheduleBodyVerificationErrorV2, match="finalized"):
        verifier.consume(records[-1].ingest_seq, canonical_json_line(records[-1]))
    with pytest.raises(PublicOiScheduleBodyVerificationErrorV2, match="finalized"):
        verifier.finalize()


def test_push_verifier_semantic_failure_is_terminal() -> None:
    records = _records_with_replaced_first_body(
        b'{"extra":1,"openInterest":"1.0","symbol":"BTCUSDT",'
        b'"time":1700000000000}'
    )
    verifier = _push_verifier(records)

    with pytest.raises(PublicOiScheduleBodyVerificationErrorV2, match="semantic"):
        verifier.consume(1, canonical_json_line(records[0]))
    with pytest.raises(PublicOiScheduleBodyVerificationErrorV2, match="failed"):
        verifier.consume(1, canonical_json_line(records[0]))
    with pytest.raises(PublicOiScheduleBodyVerificationErrorV2, match="failed"):
        verifier.finalize()


def test_grouped_block_writer_streams_across_blocks_into_push_verifier(
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
        attempt_id="rest-schedule-body-push-blocks",
        protocol_sha256=_PROTOCOL_HASH,
        plan_sha256="2" * 64,
        source_manifest_sha256="3" * 64,
        schema_sha256="4" * 64,
        runtime_manifest_sha256="5" * 64,
    )
    signer = Ed25519BlockSignerV2.from_private_key_bytes(
        key_id="rest-schedule-body-push-writer",
        private_key_bytes=bytes(range(32)),
    )
    signing_authority = BlockSigningAuthorityV2.from_public_key_bytes(
        key_id=signer.key_id,
        public_key_bytes=signer.public_key_bytes,
    )
    policy = BlockPolicyV2(
        qualification_id="rest-schedule-body-push-zstd",
        codec_candidate_id="rest-schedule-body-push-zstd-candidate",
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
        stream_group_id="rest-schedule-body-push-stream",
        segment_id="rest-schedule-body-push-segment",
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
    assert writer.next_block_sequence == 4
    assert from_blocks == _successor_certificate(records)


def test_verifier_accepts_non_rest_websocket_tail_after_coverage_close() -> None:
    records = (
        *_one_slot_records(),
        _websocket_record(
            ingest_seq=7,
            receipt_wall_ms=_SLOT + 5_050,
            receipt_monotonic_ns=180,
        ),
    )

    certificate = _successor_certificate(records)

    assert certificate.coverage_close_ingest_seq == 6
    assert certificate.verified_prefix_tail_ingest_seq == 7
    assert certificate.ignored_websocket_record_count == 3
    assert certificate.schedule_body_complete is True


def test_verifier_rejects_trailing_oi_attempt_outside_bound_schedule_prefix() -> None:
    plan, _ = _plan_and_bundle()
    authorized_records = (
        *_one_slot_records(),
        _websocket_record(
            ingest_seq=7,
            receipt_wall_ms=_SLOT + 5_050,
            receipt_monotonic_ns=180,
        ),
    )
    receipt = _finality_receipt(authorized_records)
    schedule = _verify(authorized_records, finality_receipt=receipt)
    forged_records = (
        *_one_slot_records(),
        _attempt_record(
            plan,
            ingest_seq=7,
            symbol="BTCUSDT",
            scheduled_slot_wall_ms=_SLOT + 5_000,
            receipt_wall_ms=_SLOT + 5_050,
            receipt_monotonic_ns=180,
            poll_cycle_seq=2,
        ),
    )

    with pytest.raises(
        PublicOiScheduleBodyVerificationErrorV2,
        match="supplied prefix differs",
    ):
        _successor_certificate(
            forged_records,
            schedule_certificate=schedule,
            finality_receipt_override=receipt,
        )


def test_verifier_rejects_malformed_body_even_when_schedule_certificate_is_valid() -> None:
    records = _records_with_replaced_first_body(
        b'{"extra":1,"openInterest":"1.0","symbol":"BTCUSDT","time":1700000000000}'
    )
    receipt = _finality_receipt(records)
    schedule = _verify(records, finality_receipt=receipt)

    with pytest.raises(PublicOiScheduleBodyVerificationErrorV2, match="semantic"):
        _successor_certificate(
            records,
            schedule_certificate=schedule,
            finality_receipt_override=receipt,
        )


def test_verifier_accepts_int64_transaction_time_but_never_claims_causal_bound() -> None:
    records = _records_with_replaced_first_body(
        b'{"openInterest":"1.0","symbol":"BTCUSDT","time":9223372036854775807}'
    )

    certificate = _successor_certificate(records)

    assert certificate.body_semantics_verified is True
    assert certificate.freshness_verified is False
    assert certificate.transaction_time_causally_bounded is False


def test_verifier_rejects_schedule_with_any_unstarted_cell() -> None:
    records = _one_slot_with_unstarted_cell()
    receipt = _finality_receipt(records)
    schedule = _verify(records, finality_receipt=receipt)

    with pytest.raises(
        PublicOiScheduleBodyVerificationErrorV2,
        match="gaps, omissions, or non-retained",
    ):
        _successor_certificate(
            records,
            schedule_certificate=schedule,
            finality_receipt_override=receipt,
        )


def test_verifier_rejects_vacuous_empty_schedule() -> None:
    records = _empty_schedule_records()
    receipt = _finality_receipt(records)
    schedule = _verify(records, finality_receipt=receipt)

    with pytest.raises(PublicOiScheduleBodyVerificationErrorV2, match="vacuous"):
        _successor_certificate(
            records,
            schedule_certificate=schedule,
            finality_receipt_override=receipt,
        )


def test_verifier_rejects_different_finality_receipt_for_same_prefix() -> None:
    records = _one_slot_records()
    original_receipt = _finality_receipt(records)
    schedule = _verify(records, finality_receipt=original_receipt)
    different_receipt = replace(original_receipt, attempt_id="different-attempt")

    with pytest.raises(
        PublicOiScheduleBodyVerificationErrorV2,
        match="finality_receipt_sha256",
    ):
        _successor_certificate(
            records,
            schedule_certificate=schedule,
            finality_receipt_override=different_receipt,
        )


def test_certificate_cannot_be_copied_without_factory_provenance() -> None:
    certificate = _successor_certificate(_one_slot_records())

    with pytest.raises(TypeError, match="factory-sealed"):
        replace(certificate)


def test_validator_rejects_post_factory_material_tamper() -> None:
    certificate = _successor_certificate(_one_slot_records())
    object.__setattr__(certificate, "verified_body_count", 3)

    with pytest.raises(PublicOiScheduleBodyVerificationErrorV2, match="counts"):
        validate_public_oi_schedule_body_verification_certificate_v2(certificate)
