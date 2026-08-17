from __future__ import annotations

import json
from dataclasses import dataclass, replace
from pathlib import Path

import pytest

from signalbot.capture.receipts import ReceiptTimestamp
from signalbot.r4b_v2.alerts.actionability import CausalTargetCursorV2
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
from signalbot.r4b_v2.capture.causal_target_cursor import (
    CAUSAL_TARGET_CURSOR_SNAPSHOT_ONLY_REASON_V2,
    CausalTargetCursorDerivationErrorV2,
    canonical_causal_target_cursor_snapshot_v2,
    derive_causal_target_cursor_snapshot_v2,
    require_factory_causal_target_cursor_snapshot_v2,
)
from signalbot.r4b_v2.capture.integrity_ledger import (
    CaptureIntegrityLedgerV2,
    DataGapCauseV2,
    attest_finalized_block_v2,
)
from signalbot.r4b_v2.capture.models import RawRecordV2, TransportV2, VenueV2
from signalbot.r4b_v2.capture.plans import (
    ProvisionalPromotingPlanV9,
    ProvisionalUsdmVenueClockRestCapturePlanV9,
    build_provisional_promoting_capture_plans_v9,
    provisional_promoting_plan_sha256_v9,
)
from signalbot.r4b_v2.capture.rest_clock import (
    PUBLIC_USDM_VENUE_CLOCK_SOURCE_LOGICAL_KEY_V9,
    PublicUsdmVenueClockRestTerminalObservationV9,
)
from signalbot.r4b_v2.capture.wal import WalAuthorityV2

_PROTOCOL_SHA256 = "a" * 64
_BASE_WALL_MS = 1_710_000_000_000
_MAXIMUM_BYTES = 8 * 1024 * 1024
_RESERVE_BYTES = 1_024


@dataclass(frozen=True, slots=True)
class _Fixture:
    writer: GroupedBlockWriterV2
    ledger: CaptureIntegrityLedgerV2
    plans: tuple[ProvisionalPromotingPlanV9, ...]
    manifest: BlockManifestV2


def _plans() -> tuple[ProvisionalPromotingPlanV9, ...]:
    return build_provisional_promoting_capture_plans_v9(("BTCUSDT",))


def _clock_plan(
    plans: tuple[ProvisionalPromotingPlanV9, ...],
) -> ProvisionalUsdmVenueClockRestCapturePlanV9:
    [plan] = [item for item in plans if type(item) is ProvisionalUsdmVenueClockRestCapturePlanV9]
    return plan


def _signer() -> Ed25519BlockSignerV2:
    return Ed25519BlockSignerV2.from_private_key_bytes(
        key_id="causal-target-cursor-test-key",
        private_key_bytes=b"\x47" * 32,
    )


def _signing_authority() -> BlockSigningAuthorityV2:
    signer = _signer()
    return BlockSigningAuthorityV2.from_public_key_bytes(
        key_id=signer.key_id,
        public_key_bytes=signer.public_key_bytes,
    )


def _policy() -> BlockPolicyV2:
    return BlockPolicyV2(
        qualification_id="causal-target-cursor-zstd",
        codec_candidate_id="causal-target-cursor-zstd-candidate",
        compression_level=9,
        max_uncompressed_bytes=4_194_304,
        max_linger_ms=1_000,
    )


def _authority(
    plans: tuple[ProvisionalPromotingPlanV9, ...],
) -> WalAuthorityV2:
    return WalAuthorityV2(
        attempt_id="attempt-causal-target-cursor",
        protocol_sha256=_PROTOCOL_SHA256,
        plan_sha256=provisional_promoting_plan_sha256_v9(plans),
        source_manifest_sha256="b" * 64,
        schema_sha256="c" * 64,
        runtime_manifest_sha256="d" * 64,
    )


def _clock_record(
    plans: tuple[ProvisionalPromotingPlanV9, ...],
    *,
    ingest_seq: int,
    elapsed_ms: int,
    server_time_ms: int,
    poll_cycle_seq: int = 1,
    body: bytes | None = None,
) -> QueuedRawRecordV2:
    plan = _clock_plan(plans)
    wall_ms = _BASE_WALL_MS + elapsed_ms
    monotonic_ns = elapsed_ms * 1_000_000
    observation = PublicUsdmVenueClockRestTerminalObservationV9.for_plan(
        plan,
        session_id="session-causal-target-cursor",
        protocol_hash=_PROTOCOL_SHA256,
        connection_id="connection-causal-target-cursor",
        connection_generation=1,
        poll_cycle_seq=poll_cycle_seq,
        scheduled_slot_wall_ms=_BASE_WALL_MS + elapsed_ms,
        request_started_wall_ms=wall_ms,
        request_started_monotonic_ns=monotonic_ns,
        response_first_header_wall_ms=wall_ms,
        response_first_header_monotonic_ns=monotonic_ns,
        attempt_ended_wall_ms=wall_ms,
        attempt_ended_monotonic_ns=monotonic_ns,
        response_status=200,
        response_headers=(("content-type", "application/json"),),
        payload_complete=True,
        body=(f'{{"serverTime":{server_time_ms}}}'.encode() if body is None else body),
    )
    payload = observation(ReceiptTimestamp(wall_ms, monotonic_ns))
    record = RawRecordV2.from_payload(
        session_id="session-causal-target-cursor",
        plan_id=plan.name,
        protocol_hash=_PROTOCOL_SHA256,
        transport=TransportV2.HTTPS,
        venue=VenueV2.USDM_FUTURES,
        route_id=plan.route_id,
        symbol=None,
        connection_id="connection-causal-target-cursor",
        generation=1,
        frame_seq=None,
        ingest_seq=ingest_seq,
        receipt_wall_ms=wall_ms,
        receipt_monotonic_ns=monotonic_ns,
        raw_payload=payload,
        source_logical_key=PUBLIC_USDM_VENUE_CLOCK_SOURCE_LOGICAL_KEY_V9,
    )
    return QueuedRawRecordV2.encode(
        record,
        enqueued_monotonic_ns=monotonic_ns + 1,
    )


def _plain_record(
    *,
    ingest_seq: int,
    elapsed_ms: int,
    elapsed_extra_ns: int = 0,
) -> QueuedRawRecordV2:
    wall_ms = _BASE_WALL_MS + elapsed_ms
    monotonic_ns = elapsed_ms * 1_000_000 + elapsed_extra_ns
    record = RawRecordV2.from_payload(
        session_id="session-causal-target-cursor",
        plan_id="usdm-market-test-plan",
        protocol_hash=_PROTOCOL_SHA256,
        transport=TransportV2.WEBSOCKET,
        venue=VenueV2.USDM_FUTURES,
        route_id="usdm_market",
        symbol=None,
        connection_id="market-connection-causal-target-cursor",
        generation=1,
        frame_seq=ingest_seq,
        ingest_seq=ingest_seq,
        receipt_wall_ms=wall_ms,
        receipt_monotonic_ns=monotonic_ns,
        raw_payload=canonical_json_line(
            {"data": {"event": ingest_seq}, "stream": "btcusdt@aggTrade"}
        ).rstrip(b"\n"),
    )
    return QueuedRawRecordV2.encode(
        record,
        enqueued_monotonic_ns=monotonic_ns + 1,
    )


def _fixture(
    root: Path,
    records: tuple[QueuedRawRecordV2, ...],
) -> _Fixture:
    plans = _plans()
    writer = GroupedBlockWriterV2(
        root / "blocks",
        authority=_authority(plans),
        policy=_policy(),
        signer=_signer(),
        signing_authority=_signing_authority(),
        stream_group_id="causal-target-cursor-group",
        segment_id="segment-000001",
        maximum_total_bytes=_MAXIMUM_BYTES,
        emergency_reserve_bytes=_RESERVE_BYTES,
    )
    builder = GroupedBlockBuilderV2(writer.policy)
    manifests: list[BlockManifestV2] = []
    for queued in records:
        closed_blocks = builder.offer(
            queued,
            now_ns=queued.record.receipt_monotonic_ns + 2,
        )
        manifests.extend(writer.commit(block) for block in closed_blocks)
    block = builder.flush_tail(now_ns=records[-1].record.receipt_monotonic_ns + 3)
    if block is not None:
        manifests.append(writer.commit(block))
    assert manifests
    manifest = manifests[-1]
    ledger = CaptureIntegrityLedgerV2(
        root / "ledger",
        authority=writer.authority,
        block_directory=writer.directory,
        block_root_binding=writer.root_binding,
        block_signing_authority=writer.signing_authority,
        block_policy=writer.policy,
        block_stream_group_id=writer.stream_group_id,
        block_segment_id=writer.segment_id,
        maximum_total_bytes=_MAXIMUM_BYTES,
        emergency_reserve_bytes=_RESERVE_BYTES,
        max_events=32,
        failure_domain_id="causal-target-cursor-ledger-device",
    )
    return _Fixture(writer=writer, ledger=ledger, plans=plans, manifest=manifest)


def _normal_records(
    plans: tuple[ProvisionalPromotingPlanV9, ...],
) -> tuple[QueuedRawRecordV2, ...]:
    return (
        _clock_record(
            plans,
            ingest_seq=1,
            elapsed_ms=0,
            server_time_ms=_BASE_WALL_MS,
        ),
        _plain_record(ingest_seq=2, elapsed_ms=10_011),
        _plain_record(ingest_seq=3, elapsed_ms=10_012),
    )


def _normal_fixture(root: Path) -> _Fixture:
    plans = _plans()
    return _fixture(root, _normal_records(plans))


def _derive(fixture: _Fixture, *, decision_cutoff_ms: int):  # type: ignore[no-untyped-def]
    return derive_causal_target_cursor_snapshot_v2(
        fixture.writer,
        integrity_ledger=fixture.ledger,
        promoting_plans=fixture.plans,
        decision_cutoff_ms=decision_cutoff_ms,
    )


def test_factory_derives_exact_first_crossing_and_explicit_nonclaims(
    tmp_path: Path,
) -> None:
    fixture = _normal_fixture(tmp_path)
    snapshot = _derive(fixture, decision_cutoff_ms=_BASE_WALL_MS)
    replay = _derive(fixture, decision_cutoff_ms=_BASE_WALL_MS)

    assert snapshot == replay
    assert snapshot.prior_ingest_seq == 2
    assert snapshot.target_ingest_seq == 3
    assert snapshot.prior_venue_lower_bound_ms == snapshot.target_venue_ms - 1
    assert snapshot.target_venue_lower_bound_ms == snapshot.target_venue_ms
    assert snapshot.prior_local_cursor_ms == _BASE_WALL_MS + 10_011
    assert snapshot.target_local_cursor_ms == _BASE_WALL_MS + 10_012
    assert snapshot.cursor_math_complete_at_issuance
    assert snapshot.signed_prefix_verified_at_issuance
    assert not snapshot.caller_cursor_scalars_accepted
    assert not snapshot.current_authority_claimed
    assert not snapshot.paper_input_authorized
    assert not snapshot.production_order_placement
    assert snapshot.authority_reason == CAUSAL_TARGET_CURSOR_SNAPSHOT_ONLY_REASON_V2
    assert require_factory_causal_target_cursor_snapshot_v2(snapshot) is snapshot

    encoded = canonical_causal_target_cursor_snapshot_v2(snapshot)
    document = json.loads(encoded)
    assert document["target_ingest_seq"] == 3
    assert document["target_receipt_monotonic_ns_text"] == "10012000000"
    assert document["current_authority_claimed"] is False
    assert document["paper_input_authorized"] is False


def test_clock_age_exact_sixty_seconds_crosses_and_one_nanosecond_over_rejects(
    tmp_path: Path,
) -> None:
    plans = _plans()
    exact = _fixture(
        tmp_path / "exact",
        (
            _clock_record(
                plans,
                ingest_seq=1,
                elapsed_ms=0,
                server_time_ms=_BASE_WALL_MS,
            ),
            _plain_record(ingest_seq=2, elapsed_ms=59_999),
            _plain_record(ingest_seq=3, elapsed_ms=60_000),
        ),
    )
    snapshot = _derive(
        exact,
        decision_cutoff_ms=_BASE_WALL_MS + 49_939,
    )
    assert snapshot.target_venue_lower_bound_ms == _BASE_WALL_MS + 59_939

    stale = _fixture(
        tmp_path / "stale",
        (
            _clock_record(
                plans,
                ingest_seq=1,
                elapsed_ms=0,
                server_time_ms=_BASE_WALL_MS,
            ),
            _plain_record(ingest_seq=2, elapsed_ms=59_999),
            _plain_record(
                ingest_seq=3,
                elapsed_ms=60_000,
                elapsed_extra_ns=1,
            ),
        ),
    )
    with pytest.raises(CausalTargetCursorDerivationErrorV2, match="stale"):
        _derive(stale, decision_cutoff_ms=_BASE_WALL_MS + 49_939)


def test_first_crossing_without_left_witness_and_absent_crossing_fail_closed(
    tmp_path: Path,
) -> None:
    plans = _plans()
    first_crosses = _fixture(
        tmp_path / "first",
        (
            _clock_record(
                plans,
                ingest_seq=1,
                elapsed_ms=0,
                server_time_ms=_BASE_WALL_MS,
            ),
        ),
    )
    with pytest.raises(CausalTargetCursorDerivationErrorV2, match="left-bound"):
        _derive(first_crosses, decision_cutoff_ms=_BASE_WALL_MS - 10_001)

    no_crossing = _fixture(
        tmp_path / "absent",
        (
            _clock_record(
                plans,
                ingest_seq=1,
                elapsed_ms=0,
                server_time_ms=_BASE_WALL_MS,
            ),
            _plain_record(ingest_seq=2, elapsed_ms=1),
        ),
    )
    with pytest.raises(CausalTargetCursorDerivationErrorV2, match="does not contain"):
        _derive(no_crossing, decision_cutoff_ms=_BASE_WALL_MS)


def test_invalid_clock_member_and_rate_discontinuity_fail_closed(
    tmp_path: Path,
) -> None:
    plans = _plans()
    invalid = _fixture(
        tmp_path / "invalid",
        (
            _clock_record(
                plans,
                ingest_seq=1,
                elapsed_ms=0,
                server_time_ms=_BASE_WALL_MS,
                body=b"{}",
            ),
            _plain_record(ingest_seq=2, elapsed_ms=10_011),
        ),
    )
    with pytest.raises(CausalTargetCursorDerivationErrorV2, match="failed closed"):
        _derive(invalid, decision_cutoff_ms=_BASE_WALL_MS)

    discontinuous = _fixture(
        tmp_path / "rate",
        (
            _clock_record(
                plans,
                ingest_seq=1,
                elapsed_ms=0,
                server_time_ms=_BASE_WALL_MS,
            ),
            _clock_record(
                plans,
                ingest_seq=2,
                elapsed_ms=30_000,
                server_time_ms=_BASE_WALL_MS + 40_000,
                poll_cycle_seq=2,
            ),
        ),
    )
    with pytest.raises(CausalTargetCursorDerivationErrorV2, match="rate-continuity"):
        _derive(discontinuous, decision_cutoff_ms=_BASE_WALL_MS)

    later_invalid = _fixture(
        tmp_path / "later-invalid",
        (
            *_normal_records(plans),
            _clock_record(
                plans,
                ingest_seq=4,
                elapsed_ms=30_000,
                server_time_ms=_BASE_WALL_MS + 30_000,
                poll_cycle_seq=2,
                body=b"{}",
            ),
        ),
    )
    snapshot = _derive(later_invalid, decision_cutoff_ms=_BASE_WALL_MS)
    assert snapshot.target_ingest_seq == 3


def test_data_gap_and_restored_void_prefix_are_rejected(tmp_path: Path) -> None:
    gap_fixture = _normal_fixture(tmp_path / "gap")
    gap_fixture.ledger.append_data_gap(
        first_missing_ingest_seq=4,
        last_missing_ingest_seq=4,
        receipt_wall_lower_bound_ms=_BASE_WALL_MS + 10_013,
        receipt_wall_upper_bound_ms=_BASE_WALL_MS + 10_013,
        receipt_monotonic_lower_bound_ns=10_013_000_000,
        receipt_monotonic_upper_bound_ns=10_013_000_000,
        cause=DataGapCauseV2.BOUNDED_QUEUE_OVERFLOW,
        source_component="causal-target-test",
        evidence_sha256="e" * 64,
    )
    with pytest.raises(CausalTargetCursorDerivationErrorV2, match="DATA_GAP"):
        _derive(gap_fixture, decision_cutoff_ms=_BASE_WALL_MS)

    void_fixture = _normal_fixture(tmp_path / "void")
    reference = attest_finalized_block_v2(
        void_fixture.writer,
        void_fixture.manifest,
    )
    data_path = void_fixture.writer.directory / void_fixture.manifest.data_file
    original = data_path.read_bytes()
    data_path.write_bytes(original + b"corrupt")
    void_fixture.ledger.append_void_for_finalized_block(
        reference,
        detector_component="causal-target-test",
        detection_evidence_sha256="f" * 64,
    )
    data_path.write_bytes(original)
    with pytest.raises(RuntimeError, match="VOID"):
        _derive(void_fixture, decision_cutoff_ms=_BASE_WALL_MS)


def test_direct_cursor_forgery_snapshot_reconstruction_and_tamper_are_rejected(
    tmp_path: Path,
) -> None:
    fixture = _normal_fixture(tmp_path)
    snapshot = _derive(fixture, decision_cutoff_ms=_BASE_WALL_MS)
    direct = CausalTargetCursorV2(
        decision_cutoff_ms=snapshot.decision_cutoff_ms,
        target_venue_ms=snapshot.target_venue_ms,
        prior_local_cursor_ms=snapshot.prior_local_cursor_ms,
        prior_venue_lower_bound_ms=snapshot.prior_venue_lower_bound_ms,
        target_local_cursor_ms=snapshot.target_local_cursor_ms,
        target_venue_lower_bound_ms=snapshot.target_venue_lower_bound_ms,
        clock_segment_root_sha256=snapshot.clock_segment_root_sha256,
        contiguous_cursor_evidence=True,
    )
    with pytest.raises(TypeError, match="direct CausalTargetCursorV2"):
        require_factory_causal_target_cursor_snapshot_v2(direct)
    with pytest.raises(CausalTargetCursorDerivationErrorV2, match="factory-sealed"):
        replace(snapshot)

    object.__setattr__(snapshot, "target_ingest_seq", 4)
    with pytest.raises(CausalTargetCursorDerivationErrorV2):
        canonical_causal_target_cursor_snapshot_v2(snapshot)


def test_wal_only_or_empty_writer_cannot_mint_a_snapshot(tmp_path: Path) -> None:
    plans = _plans()
    writer = GroupedBlockWriterV2(
        tmp_path / "blocks",
        authority=_authority(plans),
        policy=_policy(),
        signer=_signer(),
        signing_authority=_signing_authority(),
        stream_group_id="causal-target-cursor-group",
        segment_id="segment-000001",
        maximum_total_bytes=_MAXIMUM_BYTES,
        emergency_reserve_bytes=_RESERVE_BYTES,
    )
    ledger = CaptureIntegrityLedgerV2(
        tmp_path / "ledger",
        authority=writer.authority,
        block_directory=writer.directory,
        block_root_binding=writer.root_binding,
        block_signing_authority=writer.signing_authority,
        block_policy=writer.policy,
        block_stream_group_id=writer.stream_group_id,
        block_segment_id=writer.segment_id,
        maximum_total_bytes=_MAXIMUM_BYTES,
        emergency_reserve_bytes=_RESERVE_BYTES,
        max_events=32,
        failure_domain_id="causal-target-cursor-ledger-device",
    )
    with pytest.raises(CausalTargetCursorDerivationErrorV2, match="finalized"):
        derive_causal_target_cursor_snapshot_v2(
            writer,
            integrity_ledger=ledger,
            promoting_plans=plans,
            decision_cutoff_ms=_BASE_WALL_MS,
        )
