from __future__ import annotations

import json
from dataclasses import replace

import pytest

from signalbot.capture.receipts import ReceiptTimestamp
from signalbot.r4b_v2.canonical import canonical_json_line
from signalbot.r4b_v2.capture.batching import QueuedRawRecordV2
from signalbot.r4b_v2.capture.models import RawRecordV2, TransportV2, VenueV2
from signalbot.r4b_v2.capture.plans import ProvisionalPromotingRestCapturePlanV2
from signalbot.r4b_v2.capture.rest import PublicOiRestTerminalObservationV2
from signalbot.r4b_v2.capture.rest_census import (
    LOCAL_SCHEDULE_EVIDENCE_V2,
    PUBLIC_OI_REST_CENSUS_MAX_CANONICAL_BYTES_V2,
    PUBLIC_OI_REST_WAL_MAX_RECORD_BYTES_V2,
    PublicOiRestCellOutcomeV2,
    PublicOiRestCoverageCloseV2,
    PublicOiRestForwardGapRangeV2,
    PublicOiRestSlotCensusEntryV2,
    PublicOiRestSlotCensusV2,
    public_oi_rest_attempt_record_sha256_v2,
    public_oi_rest_cell_event_id_v2,
    public_oi_rest_plan_sha256_v2,
    public_oi_rest_symbol_census_sha256_v2,
)

_SLOT = 1_700_000_000_000
_SESSION_START = "1" * 64
_PLAN_BUNDLE = "2" * 64
_ATTEMPT_HASH = "a" * 64


def _plan(
    symbols: tuple[str, ...] = ("BTCUSDT", "ETHUSDT"),
    *,
    name: str = "v2-usdm-public-rest-oi-promoting-abc",
) -> ProvisionalPromotingRestCapturePlanV2:
    return ProvisionalPromotingRestCapturePlanV2(
        name=name,
        venue=VenueV2.USDM_FUTURES,
        route_id="usdm_public_rest",
        method="GET",
        endpoint="/fapi/v1/openInterest",
        symbols=symbols,
    )


def _entry(
    plan: ProvisionalPromotingRestCapturePlanV2,
    ordinal: int,
    *,
    outcome: PublicOiRestCellOutcomeV2 = PublicOiRestCellOutcomeV2.ATTEMPT_RETAINED,
) -> PublicOiRestSlotCensusEntryV2:
    attempted = outcome is PublicOiRestCellOutcomeV2.ATTEMPT_RETAINED
    return PublicOiRestSlotCensusEntryV2.for_plan(
        plan,
        session_start_manifest_sha256=_SESSION_START,
        plan_bundle_sha256=_PLAN_BUNDLE,
        symbol_ordinal=ordinal,
        scheduled_slot_wall_ms=_SLOT,
        outcome=outcome,
        attempt_ingest_seq=ordinal + 1 if attempted else None,
        attempt_record_sha256=_ATTEMPT_HASH if attempted else None,
    )


def _slot(
    plan: ProvisionalPromotingRestCapturePlanV2 | None = None,
    *,
    outcomes: tuple[PublicOiRestCellOutcomeV2, ...] | None = None,
    closed_wall_ms: int = _SLOT + 4_000,
) -> PublicOiRestSlotCensusV2:
    selected = _plan() if plan is None else plan
    selected_outcomes = outcomes or tuple(
        PublicOiRestCellOutcomeV2.ATTEMPT_RETAINED for _ in selected.symbols
    )
    entries = tuple(
        _entry(selected, ordinal, outcome=outcome)
        for ordinal, outcome in enumerate(selected_outcomes)
    )
    return PublicOiRestSlotCensusV2.for_plan(
        selected,
        session_id="1700000000000-boot-a",
        session_start_manifest_sha256=_SESSION_START,
        plan_bundle_sha256=_PLAN_BUNDLE,
        scheduled_slot_wall_ms=_SLOT,
        entries=entries,
        closed_wall_ms=closed_wall_ms,
        closed_monotonic_ns=123_456,
    )


def test_slot_census_round_trip_is_local_non_completeness_evidence() -> None:
    plan = _plan()
    payload = _slot(plan)

    restored = PublicOiRestSlotCensusV2.from_canonical_bytes(
        payload.canonical_bytes(),
        plan=plan,
    )

    assert restored == payload
    assert restored.provenance == LOCAL_SCHEDULE_EVIDENCE_V2
    assert restored.data_completeness_claimed is False
    assert tuple(plan.symbols[entry.symbol_ordinal] for entry in restored.entries) == (plan.symbols)
    assert restored.sha256 == payload.sha256
    assert len(payload.canonical_bytes()) <= PUBLIC_OI_REST_CENSUS_MAX_CANONICAL_BYTES_V2


def test_cell_ids_are_domain_bound_to_session_plan_slot_ordinal_and_symbol() -> None:
    plan = _plan()
    rest_hash = public_oi_rest_plan_sha256_v2(plan)
    baseline = public_oi_rest_cell_event_id_v2(
        session_start_manifest_sha256=_SESSION_START,
        plan_bundle_sha256=_PLAN_BUNDLE,
        rest_plan_sha256=rest_hash,
        scheduled_slot_wall_ms=_SLOT,
        symbol_ordinal=0,
        symbol="BTCUSDT",
    )

    assert baseline == _entry(plan, 0).cell_event_id
    assert baseline != public_oi_rest_cell_event_id_v2(
        session_start_manifest_sha256="3" * 64,
        plan_bundle_sha256=_PLAN_BUNDLE,
        rest_plan_sha256=rest_hash,
        scheduled_slot_wall_ms=_SLOT,
        symbol_ordinal=0,
        symbol="BTCUSDT",
    )
    assert baseline != public_oi_rest_cell_event_id_v2(
        session_start_manifest_sha256=_SESSION_START,
        plan_bundle_sha256=_PLAN_BUNDLE,
        rest_plan_sha256=rest_hash,
        scheduled_slot_wall_ms=_SLOT + 5_000,
        symbol_ordinal=0,
        symbol="BTCUSDT",
    )
    assert baseline != _entry(plan, 1).cell_event_id


def test_attempt_and_unstarted_reference_contracts_are_disjoint() -> None:
    plan = _plan()
    attempted = _entry(plan, 0)
    expired = _entry(
        plan,
        0,
        outcome=PublicOiRestCellOutcomeV2.UNSTARTED_SLOT_EXPIRED,
    )

    with pytest.raises(ValueError, match="positive integer"):
        replace(attempted, attempt_ingest_seq=None)
    with pytest.raises(ValueError, match="forbid attempt references"):
        replace(expired, attempt_ingest_seq=1, attempt_record_sha256=_ATTEMPT_HASH)
    with pytest.raises(TypeError, match="exact PublicOiRestCellOutcomeV2"):
        replace(attempted, outcome="attempt_retained")  # type: ignore[arg-type]


def test_slot_requires_exact_plan_order_unique_cells_and_exact_carrier_id() -> None:
    payload = _slot()

    with pytest.raises(ValueError, match="plan order"):
        replace(payload, entries=tuple(reversed(payload.entries)))
    with pytest.raises(ValueError, match="cell event identity"):
        replace(
            payload,
            entries=(replace(payload.entries[0], cell_event_id="b" * 64), payload.entries[1]),
        )
    with pytest.raises(ValueError, match="carrier event identity"):
        replace(payload, event_id="b" * 64)
    with pytest.raises(ValueError, match="one entry per planned symbol"):
        replace(payload, entries=payload.entries[:1])


def test_slot_expiry_and_normal_stop_have_strict_wall_boundaries() -> None:
    expired_outcomes = (
        PublicOiRestCellOutcomeV2.ATTEMPT_RETAINED,
        PublicOiRestCellOutcomeV2.UNSTARTED_SLOT_EXPIRED,
    )
    stopped_outcomes = (
        PublicOiRestCellOutcomeV2.ATTEMPT_RETAINED,
        PublicOiRestCellOutcomeV2.UNSTARTED_NORMAL_STOP,
    )

    _slot(outcomes=expired_outcomes, closed_wall_ms=_SLOT + 5_000)
    _slot(outcomes=stopped_outcomes, closed_wall_ms=_SLOT + 5_000)
    with pytest.raises(ValueError, match="at or after slot end"):
        _slot(outcomes=expired_outcomes, closed_wall_ms=_SLOT + 4_999)
    with pytest.raises(ValueError, match="after slot end"):
        _slot(outcomes=stopped_outcomes, closed_wall_ms=_SLOT + 5_001)
    with pytest.raises(ValueError, match="compact range"):
        _slot(
            outcomes=(
                PublicOiRestCellOutcomeV2.ATTEMPT_RETAINED,
                PublicOiRestCellOutcomeV2.UNSTARTED_FORWARD_CLOCK_GAP,
            ),
            closed_wall_ms=_SLOT + 5_000,
        )


def test_compact_forward_gap_round_trip_covers_arbitrarily_many_aligned_slots() -> None:
    plan = _plan()
    end = _SLOT + 10_000_000_000
    gap = PublicOiRestForwardGapRangeV2.for_plan(
        plan,
        session_id="1700000000000-boot-a",
        session_start_manifest_sha256=_SESSION_START,
        plan_bundle_sha256=_PLAN_BUNDLE,
        first_slot_wall_ms=_SLOT,
        end_slot_exclusive_wall_ms=end,
        observed_wall_ms=end,
        observed_monotonic_ns=999,
    )

    restored = PublicOiRestForwardGapRangeV2.from_canonical_bytes(
        gap.canonical_bytes(),
        plan=plan,
    )

    assert restored == gap
    assert restored.covered_slot_count == 2_000_000
    assert restored.outcome is PublicOiRestCellOutcomeV2.UNSTARTED_FORWARD_CLOCK_GAP
    assert len(restored.canonical_bytes()) < 2_000
    assert restored.data_completeness_claimed is False


@pytest.mark.parametrize(
    ("field", "value", "match"),
    [
        ("first_slot_wall_ms", _SLOT + 1, "UTC epoch multiple"),
        ("end_slot_exclusive_wall_ms", _SLOT, "non-empty"),
        ("covered_slot_count", 2, "differs from its range"),
        ("observed_wall_ms", _SLOT + 4_999, "precedes"),
        ("event_id", "b" * 64, "identity differs"),
    ],
)
def test_forward_gap_rejects_alignment_count_time_and_identity_drift(
    field: str,
    value: object,
    match: str,
) -> None:
    gap = PublicOiRestForwardGapRangeV2.for_plan(
        _plan(),
        session_id="1700000000000-boot-a",
        session_start_manifest_sha256=_SESSION_START,
        plan_bundle_sha256=_PLAN_BUNDLE,
        first_slot_wall_ms=_SLOT,
        end_slot_exclusive_wall_ms=_SLOT + 5_000,
        observed_wall_ms=_SLOT + 5_000,
        observed_monotonic_ns=999,
    )
    with pytest.raises(ValueError, match=match):
        replace(gap, **{field: value})


def test_forward_gap_rejects_a_non_gap_outcome() -> None:
    gap = PublicOiRestForwardGapRangeV2.for_plan(
        _plan(),
        session_id="1700000000000-boot-a",
        session_start_manifest_sha256=_SESSION_START,
        plan_bundle_sha256=_PLAN_BUNDLE,
        first_slot_wall_ms=_SLOT,
        end_slot_exclusive_wall_ms=_SLOT + 5_000,
        observed_wall_ms=_SLOT + 5_000,
        observed_monotonic_ns=999,
    )
    with pytest.raises(ValueError, match="exact unstarted outcome"):
        replace(gap, outcome=PublicOiRestCellOutcomeV2.ATTEMPT_RETAINED)


@pytest.mark.parametrize(
    ("stop_wall_ms", "expected_end", "last_seq"),
    [
        (_SLOT, _SLOT, None),
        (_SLOT + 1, _SLOT + 5_000, 7),
        (_SLOT + 4_999, _SLOT + 5_000, 7),
        (_SLOT + 5_000, _SLOT + 5_000, 7),
        (_SLOT + 5_001, _SLOT + 10_000, 8),
    ],
)
def test_coverage_close_uses_exact_half_open_stop_boundary(
    stop_wall_ms: int,
    expected_end: int,
    last_seq: int | None,
) -> None:
    plan = _plan()
    close = PublicOiRestCoverageCloseV2.for_plan(
        plan,
        session_id="1700000000000-boot-a",
        session_start_manifest_sha256=_SESSION_START,
        plan_bundle_sha256=_PLAN_BUNDLE,
        coverage_start_slot_wall_ms=_SLOT,
        stop_requested_wall_ms=stop_wall_ms,
        stop_requested_monotonic_ns=111,
        last_census_ingest_seq=last_seq,
    )

    assert close.coverage_end_slot_exclusive_wall_ms == expected_end
    assert close.write_once is True
    assert close.close_reason == "NORMAL_STOP"
    assert close.data_completeness_claimed is False
    assert (
        PublicOiRestCoverageCloseV2.from_canonical_bytes(close.canonical_bytes(), plan=plan)
        == close
    )


def test_close_write_once_identity_is_stable_while_conflicting_payload_hash_changes() -> None:
    plan = _plan()
    first = PublicOiRestCoverageCloseV2.for_plan(
        plan,
        session_id="1700000000000-boot-a",
        session_start_manifest_sha256=_SESSION_START,
        plan_bundle_sha256=_PLAN_BUNDLE,
        coverage_start_slot_wall_ms=_SLOT,
        stop_requested_wall_ms=_SLOT + 1,
        stop_requested_monotonic_ns=111,
        last_census_ingest_seq=7,
    )
    conflict = PublicOiRestCoverageCloseV2.for_plan(
        plan,
        session_id="1700000000000-boot-a",
        session_start_manifest_sha256=_SESSION_START,
        plan_bundle_sha256=_PLAN_BUNDLE,
        coverage_start_slot_wall_ms=_SLOT,
        stop_requested_wall_ms=_SLOT + 2,
        stop_requested_monotonic_ns=112,
        last_census_ingest_seq=7,
    )

    assert first.event_id == conflict.event_id
    assert first.sha256 != conflict.sha256
    with pytest.raises(ValueError, match="write-once"):
        replace(first, write_once=False)
    with pytest.raises(ValueError, match="NORMAL_STOP"):
        replace(first, close_reason="FATAL")  # type: ignore[arg-type]


def test_empty_and_nonempty_close_references_are_disjoint() -> None:
    empty = PublicOiRestCoverageCloseV2.for_plan(
        _plan(),
        session_id="1700000000000-boot-a",
        session_start_manifest_sha256=_SESSION_START,
        plan_bundle_sha256=_PLAN_BUNDLE,
        coverage_start_slot_wall_ms=_SLOT,
        stop_requested_wall_ms=_SLOT,
        stop_requested_monotonic_ns=111,
        last_census_ingest_seq=None,
    )
    with pytest.raises(ValueError, match="empty coverage"):
        replace(empty, last_census_ingest_seq=1)
    with pytest.raises(ValueError, match="positive integer"):
        PublicOiRestCoverageCloseV2.for_plan(
            _plan(),
            session_id="1700000000000-boot-a",
            session_start_manifest_sha256=_SESSION_START,
            plan_bundle_sha256=_PLAN_BUNDLE,
            coverage_start_slot_wall_ms=_SLOT,
            stop_requested_wall_ms=_SLOT + 1,
            stop_requested_monotonic_ns=111,
            last_census_ingest_seq=None,
        )


def test_every_payload_rejects_any_data_completeness_claim() -> None:
    slot = _slot()
    gap = PublicOiRestForwardGapRangeV2.for_plan(
        _plan(),
        session_id="1700000000000-boot-a",
        session_start_manifest_sha256=_SESSION_START,
        plan_bundle_sha256=_PLAN_BUNDLE,
        first_slot_wall_ms=_SLOT,
        end_slot_exclusive_wall_ms=_SLOT + 5_000,
        observed_wall_ms=_SLOT + 5_000,
        observed_monotonic_ns=999,
    )
    close = PublicOiRestCoverageCloseV2.for_plan(
        _plan(),
        session_id="1700000000000-boot-a",
        session_start_manifest_sha256=_SESSION_START,
        plan_bundle_sha256=_PLAN_BUNDLE,
        coverage_start_slot_wall_ms=_SLOT,
        stop_requested_wall_ms=_SLOT + 1,
        stop_requested_monotonic_ns=111,
        last_census_ingest_seq=7,
    )
    for payload in (slot, gap, close):
        with pytest.raises(ValueError, match="may not claim data completeness"):
            replace(payload, data_completeness_claimed=True)


def test_strict_parser_rejects_tamper_duplicate_float_noncanonical_and_fields() -> None:
    payload = _slot()
    encoded = payload.canonical_bytes()
    duplicate = encoded.replace(
        b'"session_id":',
        b'"session_id":"duplicate","session_id":',
        1,
    )
    floated = encoded.replace(
        str(payload.closed_wall_ms).encode(),
        b"1.5",
        1,
    )
    document = json.loads(encoded)
    document["extra"] = "forbidden"
    extra = canonical_json_line(document)
    del document["extra"]
    del document["event_id"]
    missing = canonical_json_line(document)

    for candidate in (duplicate, floated, b" " + encoded, extra, missing, encoded + b"\n"):
        with pytest.raises((TypeError, ValueError)):
            PublicOiRestSlotCensusV2.from_canonical_bytes(candidate)


def test_parser_rejects_boolean_integer_and_nested_duplicate() -> None:
    encoded = _slot().canonical_bytes()
    bool_integer = encoded.replace(b'"attempt_ingest_seq":1', b'"attempt_ingest_seq":true', 1)
    nested_duplicate = encoded.replace(
        b'"cell_event_id":',
        b'"cell_event_id":"duplicate","cell_event_id":',
        1,
    )

    with pytest.raises(TypeError, match="exact integer"):
        PublicOiRestSlotCensusV2.from_canonical_bytes(bool_integer)
    with pytest.raises(ValueError, match="duplicate key"):
        PublicOiRestSlotCensusV2.from_canonical_bytes(nested_duplicate)


def test_parser_and_live_validation_reject_a_different_exact_plan() -> None:
    payload = _slot()
    different = _plan(("BTCUSDT", "ETHUSDT", "SOLUSDT"))

    with pytest.raises(ValueError, match="exact REST plan"):
        PublicOiRestSlotCensusV2.from_canonical_bytes(
            payload.canonical_bytes(),
            plan=different,
        )
    with pytest.raises(ValueError, match="exact REST plan"):
        payload.validate_against_plan(different)


def test_plan_and_symbol_census_hashes_are_deterministic_and_domain_separated() -> None:
    plan = _plan()
    assert public_oi_rest_plan_sha256_v2(plan) == public_oi_rest_plan_sha256_v2(plan)
    assert public_oi_rest_symbol_census_sha256_v2(plan) == public_oi_rest_symbol_census_sha256_v2(
        plan
    )
    assert public_oi_rest_plan_sha256_v2(plan) != public_oi_rest_symbol_census_sha256_v2(plan)
    assert public_oi_rest_plan_sha256_v2(plan) != public_oi_rest_plan_sha256_v2(
        _plan(name="different-rest-plan")
    )


def test_maximum_32_symbol_slot_payload_and_real_wal_envelope_stay_under_20kb() -> None:
    symbols = tuple("A" * 24 + f"{ordinal:02d}USDT" for ordinal in range(32))
    plan = _plan(symbols, name="p" * 256)
    entries = tuple(
        PublicOiRestSlotCensusEntryV2.for_plan(
            plan,
            session_start_manifest_sha256=_SESSION_START,
            plan_bundle_sha256=_PLAN_BUNDLE,
            symbol_ordinal=ordinal,
            scheduled_slot_wall_ms=_SLOT,
            outcome=PublicOiRestCellOutcomeV2.ATTEMPT_RETAINED,
            attempt_ingest_seq=1_000_000_000_000 + ordinal,
            attempt_record_sha256=_ATTEMPT_HASH,
        )
        for ordinal in range(32)
    )
    payload = PublicOiRestSlotCensusV2.for_plan(
        plan,
        session_id="s" * 256,
        session_start_manifest_sha256=_SESSION_START,
        plan_bundle_sha256=_PLAN_BUNDLE,
        scheduled_slot_wall_ms=_SLOT,
        entries=entries,
        closed_wall_ms=_SLOT + 4_999,
        closed_monotonic_ns=1_000_000_000_000,
    )

    encoded_payload = payload.canonical_bytes()
    record = RawRecordV2.from_payload(
        session_id=payload.session_id,
        plan_id=plan.name,
        protocol_hash="f" * 64,
        transport=TransportV2.HTTPS,
        venue=VenueV2.USDM_FUTURES,
        route_id="usdm_public_rest",
        symbol=None,
        connection_id="c" * 256,
        generation=9_007_199_254_740_991,
        frame_seq=None,
        ingest_seq=9_007_199_254_740_991,
        receipt_wall_ms=9_007_199_254_740_991,
        receipt_monotonic_ns=9_007_199_254_740_991,
        raw_payload=encoded_payload,
        source_logical_key="openInterest:census",
    )
    queued = QueuedRawRecordV2.encode(
        record,
        enqueued_monotonic_ns=9_007_199_254_740_991,
    )

    assert len(payload.entries) == 32
    assert len(encoded_payload) <= PUBLIC_OI_REST_CENSUS_MAX_CANONICAL_BYTES_V2
    assert queued.encoded_len <= PUBLIC_OI_REST_WAL_MAX_RECORD_BYTES_V2
    assert PublicOiRestSlotCensusV2.from_canonical_bytes(encoded_payload, plan=plan) == payload


def test_attempt_record_hash_means_exact_canonical_outer_raw_record_line() -> None:
    plan = _plan()
    observation = PublicOiRestTerminalObservationV2.for_plan(
        plan,
        symbol="BTCUSDT",
        poll_cycle_seq=1,
        symbol_ordinal=0,
        scheduled_slot_wall_ms=_SLOT,
        attempt=1,
        request_started_wall_ms=_SLOT + 1,
        request_started_monotonic_ns=90,
        response_first_header_wall_ms=_SLOT + 2,
        response_first_header_monotonic_ns=91,
        attempt_ended_wall_ms=_SLOT + 3,
        attempt_ended_monotonic_ns=92,
        response_status=200,
        response_headers=(("content-type", "application/json"),),
        payload_complete=True,
        body=b'{"openInterest":"1","symbol":"BTCUSDT","time":1700000000001}',
    )
    attempt_payload = observation(ReceiptTimestamp(_SLOT + 4, 93))
    record = RawRecordV2.from_payload(
        session_id="session-a",
        plan_id=plan.name,
        protocol_hash="f" * 64,
        transport=TransportV2.HTTPS,
        venue=VenueV2.USDM_FUTURES,
        route_id="usdm_public_rest",
        symbol="BTCUSDT",
        connection_id="rest-a",
        generation=1,
        frame_seq=None,
        ingest_seq=7,
        receipt_wall_ms=_SLOT + 4,
        receipt_monotonic_ns=93,
        raw_payload=attempt_payload,
        source_logical_key="openInterest:BTCUSDT",
    )
    queued = QueuedRawRecordV2.encode(record, enqueued_monotonic_ns=94)

    assert public_oi_rest_attempt_record_sha256_v2(record) == queued.encoded_sha256
    with pytest.raises(ValueError, match="exact public OI REST record"):
        public_oi_rest_attempt_record_sha256_v2(replace(record, frame_seq=1))
    with pytest.raises(ValueError, match="payload identity differs"):
        public_oi_rest_attempt_record_sha256_v2(
            replace(record, receipt_wall_ms=record.receipt_wall_ms + 1)
        )


def test_12kb_payload_cap_survives_maximum_escaped_ascii_outer_identities() -> None:
    record = RawRecordV2.from_payload(
        session_id="\\" * 256,
        plan_id="\\" * 256,
        protocol_hash="f" * 64,
        transport=TransportV2.HTTPS,
        venue=VenueV2.USDM_FUTURES,
        route_id="usdm_public_rest",
        symbol=None,
        connection_id="\\" * 256,
        generation=9_007_199_254_740_991,
        frame_seq=None,
        ingest_seq=9_007_199_254_740_991,
        receipt_wall_ms=9_007_199_254_740_991,
        receipt_monotonic_ns=9_007_199_254_740_991,
        raw_payload=b"x" * PUBLIC_OI_REST_CENSUS_MAX_CANONICAL_BYTES_V2,
        source_logical_key="openInterest:census",
    )
    queued = QueuedRawRecordV2.encode(
        record,
        enqueued_monotonic_ns=9_007_199_254_740_991,
    )

    assert queued.encoded_len <= PUBLIC_OI_REST_WAL_MAX_RECORD_BYTES_V2


def test_plan_rejects_over_32_and_models_reject_noncanonical_symbol_census() -> None:
    symbols = tuple(f"C{ordinal:02d}USDT" for ordinal in range(33))
    with pytest.raises(ValueError, match="maximum of 32"):
        _plan(symbols)

    payload = _slot()
    with pytest.raises(ValueError, match="unique lexicographic order"):
        replace(payload, symbols=tuple(reversed(payload.symbols)))


def test_carrier_payload_hash_domains_do_not_alias() -> None:
    plan = _plan()
    slot = _slot(plan)
    gap = PublicOiRestForwardGapRangeV2.for_plan(
        plan,
        session_id=slot.session_id,
        session_start_manifest_sha256=_SESSION_START,
        plan_bundle_sha256=_PLAN_BUNDLE,
        first_slot_wall_ms=_SLOT,
        end_slot_exclusive_wall_ms=_SLOT + 5_000,
        observed_wall_ms=_SLOT + 5_000,
        observed_monotonic_ns=999,
    )
    close = PublicOiRestCoverageCloseV2.for_plan(
        plan,
        session_id=slot.session_id,
        session_start_manifest_sha256=_SESSION_START,
        plan_bundle_sha256=_PLAN_BUNDLE,
        coverage_start_slot_wall_ms=_SLOT,
        stop_requested_wall_ms=_SLOT + 1,
        stop_requested_monotonic_ns=111,
        last_census_ingest_seq=7,
    )

    assert len({slot.event_id, gap.event_id, close.event_id}) == 3
    assert len({slot.sha256, gap.sha256, close.sha256}) == 3
