from __future__ import annotations

import hashlib
import json
from collections.abc import Callable
from dataclasses import dataclass
from typing import cast

import pytest
from pydantic import ValidationError

import signalbot.capture.clock_health_report as report_module
from signalbot.capture.clock_health_report import (
    ClockHealthReportV1,
    PerVenueClockHealthV1,
    VenueClockSampleV1,
    assess_causal_clock_cutoff,
    build_clock_health_report,
    clock_samples_rate_continuous,
)
from signalbot.capture.closed_evidence import ClosedCaptureAuthority
from signalbot.capture.models import (
    CaptureEnvelopeV1,
    CaptureRecord,
    RestEnvelopeV1,
    RestEnvelopeV2,
)
from signalbot.capture.storage import SegmentManifestV1
from signalbot.domain.enums import Market

_PLAN_SHA = "a" * 64
_START_WALL_MS = 1_700_000_000_000
_START_NS = 10_000_000_000
_FULL_DURATION_NS = 86_400 * 1_000_000_000


@dataclass(frozen=True, slots=True)
class _StartStub:
    session_id: str
    protocol_sha256: str
    source_manifest_sha256: str
    capture_plan_sha256: str
    started_at_ms: int
    started_monotonic_ns: int


@dataclass(frozen=True, slots=True)
class _ClosureStub:
    closed_at_ms: int
    closed_monotonic_ns: int
    fatal: bool
    stop_reason: str


@dataclass(frozen=True, slots=True)
class _AuthorityStub:
    start: _StartStub
    closure: _ClosureStub
    manifests: tuple[SegmentManifestV1, ...]
    start_sha256: str
    closure_sha256: str
    external_start_sha256: str
    external_closure_sha256: str


def test_full_day_separate_venue_clocks_pass_and_hash_deterministically(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    records = _full_day_records(initial_gap_ns=100_000_000)
    authority = _authority(records, elapsed_ns=_FULL_DURATION_NS)
    _patch_closed_evidence(monkeypatch, authority, records)

    first = build_clock_health_report(
        start_path="ignored-start",
        closure_path="ignored-closure",
        capture_directory="ignored-capture",
    )
    second = build_clock_health_report(
        start_path="ignored-start",
        closure_path="ignored-closure",
        capture_directory="ignored-capture",
    )

    assert first.report.verdict == "CLOCK_HEALTH_PASS"
    assert first.report.verdict_reasons == (
        "clock_health_requirements_satisfied",
    )
    assert tuple(item.market for item in first.report.venues) == ("spot", "futures")
    spot, futures = first.report.venues
    assert spot.request_role == "spot_venue_time"
    assert futures.request_role == "futures_venue_time"
    assert spot.valid_sample_count > 2_800
    assert futures.valid_sample_count == spot.valid_sample_count
    assert spot.meets_clock_coverage_requirement is True
    assert futures.meets_clock_coverage_requirement is True
    assert spot.minimum_offset_lower_ms is not None
    assert futures.minimum_offset_lower_ms is not None
    assert spot.minimum_offset_lower_ms > futures.minimum_offset_lower_ms
    assert first.report.scope_boundaries.future_clock_interpolation_performed is False
    assert (
        first.report.scope_boundaries.report_runtime_attested_to_capture_source_manifest
        is False
    )
    assert first.report.scope_boundaries.alert_runtime_integrated is False
    assert first.report.scope_boundaries.paper_runtime_integrated is False
    assert first.report.scope_boundaries.promotion_authorized is False
    assert first.canonical_bytes == second.canonical_bytes
    assert first.sha256 == second.sha256
    assert first.sha256 == hashlib.sha256(first.canonical_bytes).hexdigest()
    assert b'"pnl"' not in first.canonical_bytes
    assert b'"outcome"' not in first.canonical_bytes
    tampered_pass = first.report.model_dump(mode="python")
    tampered_pass["verdict"] = "INCOMPLETE"
    tampered_pass["verdict_reasons"] = ("configured_duration_not_observed",)
    with pytest.raises(ValidationError, match="verdict or reasons contradict"):
        ClockHealthReportV1.model_validate(tampered_pass)

    tampered_counts = spot.model_dump(mode="python")
    tampered_counts["time_record_count"] += 1
    tampered_counts["unusable_sample_count"] += 1
    with pytest.raises(ValidationError, match="unusable reasons do not partition"):
        PerVenueClockHealthV1.model_validate(tampered_counts)

    tampered_coverage = spot.model_dump(mode="python")
    tampered_coverage["time_record_count"] = 1
    tampered_coverage["valid_sample_count"] = 1
    tampered_coverage["last_valid_sample"] = tampered_coverage[
        "first_valid_sample"
    ]
    with pytest.raises(ValidationError, match="sample-count bound"):
        PerVenueClockHealthV1.model_validate(tampered_coverage)

    tampered_continuity = spot.model_dump(mode="python")
    tampered_continuity["continuity_failure_count"] = spot.valid_sample_count
    with pytest.raises(ValidationError, match="adjacent sample pairs"):
        PerVenueClockHealthV1.model_validate(tampered_continuity)

    tampered_endpoint = spot.model_dump(mode="python")
    assert spot.first_valid_sample is not None
    tampered_endpoint["minimum_offset_lower_ms"] = (
        spot.first_valid_sample.offset_lower_ms + 1
    )
    with pytest.raises(ValidationError, match="exclude an endpoint sample"):
        PerVenueClockHealthV1.model_validate(tampered_endpoint)


def test_short_operator_smoke_is_incomplete(monkeypatch: pytest.MonkeyPatch) -> None:
    records = _clock_pair_records(completed_ns=_START_NS + 100_000_000)
    authority = _authority(
        records,
        elapsed_ns=10 * 1_000_000_000,
        stop_reason="operator_requested",
    )
    _patch_closed_evidence(monkeypatch, authority, records)

    report = build_clock_health_report(
        start_path="ignored-start",
        closure_path="ignored-closure",
        capture_directory="ignored-capture",
    ).report

    assert report.verdict == "INCOMPLETE"
    assert report.verdict_reasons == (
        "configured_duration_not_observed",
        "closure_not_completed_duration",
    )
    assert all(item.valid_sample_count == 1 for item in report.venues)
    tampered_incomplete = report.model_dump(mode="python")
    tampered_incomplete["verdict"] = "FAIL"
    tampered_incomplete["verdict_reasons"] = ("fatal_session_closure",)
    with pytest.raises(ValidationError, match="verdict or reasons contradict"):
        ClockHealthReportV1.model_validate(tampered_incomplete)


def test_causal_mapper_uses_only_completed_fresh_sample_and_three_way_cutoff(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    records = _clock_pair_records(completed_ns=_START_NS + 100_000_000)
    authority = _authority(
        records,
        elapsed_ns=61 * 1_000_000_000,
        stop_reason="operator_requested",
    )
    _patch_closed_evidence(monkeypatch, authority, records)
    report = build_clock_health_report(
        start_path="ignored-start",
        closure_path="ignored-closure",
        capture_directory="ignored-capture",
    ).report
    sample = report.venues[0].first_valid_sample
    assert sample is not None
    receipt_ns = sample.available_at_monotonic_ns

    probe = assess_causal_clock_cutoff(
        sample,
        receipt_monotonic_ns=receipt_ns,
        cutoff_ms=0,
    )
    assert probe.venue_time_lower_ms is not None
    assert probe.venue_time_upper_ms is not None
    admissible = assess_causal_clock_cutoff(
        sample,
        receipt_monotonic_ns=receipt_ns,
        cutoff_ms=probe.venue_time_upper_ms,
    )
    late = assess_causal_clock_cutoff(
        sample,
        receipt_monotonic_ns=receipt_ns,
        cutoff_ms=probe.venue_time_lower_ms - 1,
    )
    straddles = assess_causal_clock_cutoff(
        sample,
        receipt_monotonic_ns=receipt_ns,
        cutoff_ms=probe.venue_time_lower_ms,
    )
    assert admissible.verdict == "ADMISSIBLE"
    assert late.verdict == "LATE"
    assert straddles.verdict == "CLOCK_INCONCLUSIVE"
    assert straddles.reason == "receipt_interval_straddles_cutoff"

    future = assess_causal_clock_cutoff(
        sample,
        receipt_monotonic_ns=receipt_ns - 1,
        cutoff_ms=probe.venue_time_upper_ms,
    )
    exact_age = assess_causal_clock_cutoff(
        sample,
        receipt_monotonic_ns=receipt_ns + 60_000_000_000,
        cutoff_ms=probe.venue_time_upper_ms + 100_000,
    )
    stale = assess_causal_clock_cutoff(
        sample,
        receipt_monotonic_ns=receipt_ns + 60_000_000_001,
        cutoff_ms=probe.venue_time_upper_ms + 100_000,
    )
    missing = assess_causal_clock_cutoff(
        None,
        receipt_monotonic_ns=receipt_ns,
        cutoff_ms=probe.venue_time_upper_ms,
    )
    assert future.reason == "clock_sample_not_yet_available"
    assert exact_age.verdict == "ADMISSIBLE"
    assert stale.reason == "clock_sample_stale"
    assert missing.reason == "clock_sample_missing"
    assert future.source_ingest_seq == sample.source_ingest_seq
    assert stale.source_ingest_seq == sample.source_ingest_seq
    assert missing.source_ingest_seq is None
    assert stale.venue_time_lower_ms is None


def test_header_rtt_exact_two_seconds_is_valid_and_one_ns_more_is_unusable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    exact = _time_record(
        market=Market.SPOT,
        ingest_seq=1,
        completed_ns=_START_NS + 3_000_000_000,
        header_rtt_ns=2_000_000_000,
    )
    over = _time_record(
        market=Market.FUTURES,
        ingest_seq=2,
        completed_ns=_START_NS + 3_000_000_000,
        header_rtt_ns=2_000_000_001,
    )
    records: list[CaptureRecord] = [exact, over]
    authority = _authority(
        records,
        elapsed_ns=10 * 1_000_000_000,
        stop_reason="operator_requested",
    )
    _patch_closed_evidence(monkeypatch, authority, records)

    report = build_clock_health_report(
        start_path="ignored-start",
        closure_path="ignored-closure",
        capture_directory="ignored-capture",
    ).report

    spot, futures = report.venues
    assert spot.valid_sample_count == 1
    assert spot.header_rtt_exceeded_count == 0
    assert futures.valid_sample_count == 0
    assert futures.header_rtt_exceeded_count == 1
    assert report.verdict == "INCOMPLETE"


@pytest.mark.parametrize(
    ("residual_ns", "expected_valid", "expected_inconclusive", "expected_fail"),
    [
        pytest.param(2_000_000, 1, 0, False, id="exact-2ms-healthy"),
        pytest.param(2_000_001, 0, 1, False, id="above-2ms-inconclusive"),
        pytest.param(99_999_999, 0, 1, False, id="below-100ms-inconclusive"),
        pytest.param(100_000_000, 0, 0, True, id="exact-100ms-fail"),
    ],
)
def test_sample_wall_monotonic_residual_boundaries(
    monkeypatch: pytest.MonkeyPatch,
    residual_ns: int,
    expected_valid: int,
    expected_inconclusive: int,
    expected_fail: bool,
) -> None:
    record = _time_record(
        market=Market.SPOT,
        ingest_seq=1,
        completed_ns=_START_NS + 2_000_000_000,
        header_rtt_ns=500_000_000,
        wall_elapsed_ms=500 + residual_ns // 1_000_000,
        first_byte_extra_ns=residual_ns % 1_000_000,
        align_completion_wall=True,
    )
    records: list[CaptureRecord] = [record]
    authority = _authority(
        records,
        elapsed_ns=10 * 1_000_000_000,
        stop_reason="operator_requested",
    )
    _patch_closed_evidence(monkeypatch, authority, records)

    report = build_clock_health_report(
        start_path="ignored-start",
        closure_path="ignored-closure",
        capture_directory="ignored-capture",
    ).report

    spot = report.venues[0]
    assert spot.valid_sample_count == expected_valid
    assert spot.sample_wall_monotonic_inconclusive_count == expected_inconclusive
    assert (report.verdict == "FAIL") is expected_fail
    if expected_fail:
        assert report.verdict_reasons == ("wall_clock_discontinuity_observed",)
    else:
        assert report.verdict == "INCOMPLETE"


@pytest.mark.parametrize(
    ("residual_ns", "expected_healthy", "expected_inconclusive", "expected_fail"),
    [
        pytest.param(2_000_000, 2, 0, False, id="exact-2ms-healthy"),
        pytest.param(2_000_001, 0, 2, False, id="above-2ms-inconclusive"),
        pytest.param(99_999_999, 0, 2, False, id="below-100ms-inconclusive"),
        pytest.param(100_000_000, 0, 0, True, id="exact-100ms-fail"),
    ],
)
def test_global_wall_monotonic_residual_boundaries(
    monkeypatch: pytest.MonkeyPatch,
    residual_ns: int,
    expected_healthy: int,
    expected_inconclusive: int,
    expected_fail: bool,
) -> None:
    mono_delta_ns = 1_000_000_000 - residual_ns % 1_000_000
    wall_delta_ms = 1_000 + residual_ns // 1_000_000
    record = _generic_record(
        ingest_seq=1,
        receipt_ns=_START_NS + mono_delta_ns,
        received_at_ms=_START_WALL_MS + wall_delta_ms,
    )
    records: list[CaptureRecord] = [record]
    authority = _authority(
        records,
        elapsed_ns=10 * 1_000_000_000,
        stop_reason="operator_requested",
    )
    _patch_closed_evidence(monkeypatch, authority, records)

    report = build_clock_health_report(
        start_path="ignored-start",
        closure_path="ignored-closure",
        capture_directory="ignored-capture",
    ).report

    assert report.global_clock.healthy_interval_count == expected_healthy
    assert report.global_clock.inconclusive_interval_count == expected_inconclusive
    assert (report.verdict == "FAIL") is expected_fail


def test_header_to_completion_clock_step_is_not_hidden(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    record = _time_record(
        market=Market.SPOT,
        ingest_seq=1,
        completed_ns=_START_NS + 2_000_000_000,
        completion_wall_adjust_ms=100,
    )
    records: list[CaptureRecord] = [record]
    authority = _authority(
        records,
        elapsed_ns=10 * 1_000_000_000,
        stop_reason="operator_requested",
    )
    _patch_closed_evidence(monkeypatch, authority, records)

    report = build_clock_health_report(
        start_path="ignored-start",
        closure_path="ignored-closure",
        capture_directory="ignored-capture",
    ).report

    assert report.verdict == "FAIL"
    assert report.venues[0].sample_wall_clock_discontinuity_count == 1
    assert report.verdict_reasons == ("wall_clock_discontinuity_observed",)


def test_legacy_time_record_is_counted_but_never_used_as_header_proxy(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    receipt_ns = _START_NS + 100_000_000
    legacy = RestEnvelopeV1(
        request_started_at_ms=_wall_for_mono(receipt_ns - 10_000_000),
        request_started_monotonic_ns=receipt_ns - 10_000_000,
        response_received_at_ms=_wall_for_mono(receipt_ns),
        response_received_monotonic_ns=receipt_ns,
        plan_sha256=_PLAN_SHA,
        process_boot_id="boot-1",
        request_id="legacy-clock",
        attempt=1,
        ingest_seq=1,
        market=Market.SPOT,
        endpoint_path="/api/v3/time",
        canonical_query=(),
        response_status=200,
        raw_payload='{"serverTime":1700000001600}',
    )
    records: list[CaptureRecord] = [legacy]
    authority = _authority(
        records,
        elapsed_ns=10 * 1_000_000_000,
        stop_reason="operator_requested",
    )
    _patch_closed_evidence(monkeypatch, authority, records)

    report = build_clock_health_report(
        start_path="ignored-start",
        closure_path="ignored-closure",
        capture_directory="ignored-capture",
    ).report

    spot = report.venues[0]
    assert spot.time_record_count == 1
    assert spot.valid_sample_count == 0
    assert spot.unusable_sample_count == 1
    assert spot.missing_first_byte_count == 1
    assert report.verdict == "INCOMPLETE"


def test_clock_coverage_exact_999000_ppm_passes_and_one_ns_less_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    exact_records = _full_day_records(initial_gap_ns=86_400_000_000)
    exact_authority = _authority(exact_records, elapsed_ns=_FULL_DURATION_NS)
    _patch_closed_evidence(monkeypatch, exact_authority, exact_records)
    exact = build_clock_health_report(
        start_path="ignored-start",
        closure_path="ignored-closure",
        capture_directory="ignored-capture",
    ).report
    assert {item.valid_coverage_ppm for item in exact.venues} == {999_000}
    assert exact.verdict == "CLOCK_HEALTH_PASS"

    below_records = _full_day_records(initial_gap_ns=86_400_000_001)
    below_authority = _authority(below_records, elapsed_ns=_FULL_DURATION_NS)
    _patch_closed_evidence(monkeypatch, below_authority, below_records)
    below = build_clock_health_report(
        start_path="ignored-start",
        closure_path="ignored-closure",
        capture_directory="ignored-capture",
    ).report
    assert {item.valid_coverage_ppm for item in below.venues} == {998_999}
    assert below.verdict == "FAIL"
    assert below.verdict_reasons == (
        "spot_clock_coverage_below_requirement",
        "futures_clock_coverage_below_requirement",
    )


def test_rate_continuity_accepts_exact_upper_envelope_and_rejects_one_ms_more() -> None:
    previous = _sample(
        market=Market.SPOT,
        ingest_seq=1,
        request_ns=_START_NS,
        server_time_ms=1_700_000_001_500,
    )
    exact_upper = _sample(
        market=Market.SPOT,
        ingest_seq=2,
        request_ns=_START_NS + 30_000_000_000,
        server_time_ms=previous.server_time_ms + 30_032,
    )
    above = _sample(
        market=Market.SPOT,
        ingest_seq=3,
        request_ns=_START_NS + 30_000_000_000,
        server_time_ms=previous.server_time_ms + 30_033,
    )

    assert clock_samples_rate_continuous(previous, exact_upper) is True
    assert clock_samples_rate_continuous(previous, above) is False


@pytest.mark.parametrize(
    "payload",
    [
        pytest.param('{"serverTime":true}', id="boolean"),
        pytest.param('{"serverTime":1.5}', id="float"),
        pytest.param('{"serverTime":1,"extra":2}', id="extra-key"),
        pytest.param('{"serverTime":1,"serverTime":2}', id="duplicate-key"),
        pytest.param("not-json", id="invalid-json"),
    ],
)
def test_malformed_time_payload_is_unusable_without_proxy_substitution(
    monkeypatch: pytest.MonkeyPatch,
    payload: str,
) -> None:
    record = _time_record(
        market=Market.SPOT,
        ingest_seq=1,
        completed_ns=_START_NS + 100_000_000,
        raw_payload=payload,
    )
    records: list[CaptureRecord] = [record]
    authority = _authority(
        records,
        elapsed_ns=10 * 1_000_000_000,
        stop_reason="operator_requested",
    )
    _patch_closed_evidence(monkeypatch, authority, records)

    report = build_clock_health_report(
        start_path="ignored-start",
        closure_path="ignored-closure",
        capture_directory="ignored-capture",
    ).report

    assert report.venues[0].valid_sample_count == 0
    assert report.venues[0].malformed_payload_count == 1
    assert report.verdict == "INCOMPLETE"


def test_global_monotonic_regression_is_hard_fail(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    later = _generic_record(ingest_seq=1, receipt_ns=_START_NS + 2_000_000_000)
    earlier = _generic_record(ingest_seq=2, receipt_ns=_START_NS + 1_000_000_000)
    records: list[CaptureRecord] = [later, earlier]
    authority = _authority(
        records,
        elapsed_ns=10 * 1_000_000_000,
        stop_reason="operator_requested",
    )
    _patch_closed_evidence(monkeypatch, authority, records)

    report = build_clock_health_report(
        start_path="ignored-start",
        closure_path="ignored-closure",
        capture_directory="ignored-capture",
    ).report

    assert report.verdict == "FAIL"
    assert report.verdict_reasons == ("monotonic_regression_observed",)
    assert report.global_clock.monotonic_regression_count == 1
    tampered_fail = report.model_dump(mode="python")
    tampered_fail["verdict"] = "INCOMPLETE"
    tampered_fail["verdict_reasons"] = ("configured_duration_not_observed",)
    with pytest.raises(ValidationError, match="verdict or reasons contradict"):
        ClockHealthReportV1.model_validate(tampered_fail)


def _full_day_records(*, initial_gap_ns: int) -> list[CaptureRecord]:
    records: list[CaptureRecord] = []
    completed_ns = _START_NS + initial_gap_ns
    end_ns = _START_NS + _FULL_DURATION_NS
    while completed_ns < end_ns:
        records.extend(
            _clock_pair_records(
                completed_ns=completed_ns,
                initial_ingest_seq=len(records) + 1,
            )
        )
        completed_ns += 30_000_000_000
    return records


def _clock_pair_records(
    *,
    completed_ns: int,
    initial_ingest_seq: int = 1,
) -> list[CaptureRecord]:
    return [
        _time_record(
            market=Market.SPOT,
            ingest_seq=initial_ingest_seq,
            completed_ns=completed_ns,
            venue_offset_ms=1_500,
        ),
        _time_record(
            market=Market.FUTURES,
            ingest_seq=initial_ingest_seq + 1,
            completed_ns=completed_ns,
            venue_offset_ms=1_400,
        ),
    ]


def _time_record(
    *,
    market: Market,
    ingest_seq: int,
    completed_ns: int,
    venue_offset_ms: int = 1_500,
    header_rtt_ns: int = 80_000_000,
    wall_elapsed_ms: int | None = None,
    first_byte_extra_ns: int = 0,
    completion_wall_adjust_ms: int = 0,
    align_completion_wall: bool = False,
    raw_payload: str | None = None,
) -> RestEnvelopeV2:
    first_ns = completed_ns - 1_000_000
    start_ns = first_ns - header_rtt_ns
    start_wall = _wall_for_mono(start_ns)
    first_wall = (
        _wall_for_mono(first_ns)
        if wall_elapsed_ms is None
        else start_wall + wall_elapsed_ms
    )
    if first_byte_extra_ns:
        first_ns -= first_byte_extra_ns
    stamp_ns = start_ns + (first_ns - start_ns) // 2
    server_time_ms = _wall_for_mono(stamp_ns) + venue_offset_ms
    is_spot = market is Market.SPOT
    completion_wall = (
        first_wall + (completed_ns - first_ns) // 1_000_000
        if align_completion_wall
        else _wall_for_mono(completed_ns) + completion_wall_adjust_ms
    )
    return RestEnvelopeV2(
        request_started_at_ms=start_wall,
        request_started_monotonic_ns=start_ns,
        response_first_byte_at_ms=first_wall,
        response_first_byte_monotonic_ns=first_ns,
        response_completed_at_ms=completion_wall,
        response_completed_monotonic_ns=completed_ns,
        plan_sha256=_PLAN_SHA,
        process_boot_id="boot-1",
        request_role="spot_venue_time" if is_spot else "futures_venue_time",
        correlation_id=f"clock-{market.value}-{ingest_seq}",
        attempt=1,
        ingest_seq=ingest_seq,
        market=market,
        endpoint_path="/api/v3/time" if is_spot else "/fapi/v1/time",
        canonical_query=(),
        response_status=200,
        response_headers=(("content-type", "application/json"),),
        payload_complete=True,
        raw_payload=(
            raw_payload
            if raw_payload is not None
            else json.dumps({"serverTime": server_time_ms}, separators=(",", ":"))
        ),
    )


def _sample(
    *,
    market: Market,
    ingest_seq: int,
    request_ns: int,
    server_time_ms: int,
) -> VenueClockSampleV1:
    wall_ms = _wall_for_mono(request_ns)
    role = "spot_venue_time" if market is Market.SPOT else "futures_venue_time"
    return VenueClockSampleV1(
        schema_version="venue_clock_sample_v1",
        market=market.value,
        request_role=role,
        source_ingest_seq=ingest_seq,
        request_started_at_ms=wall_ms,
        request_started_monotonic_ns=request_ns,
        response_first_byte_at_ms=wall_ms,
        response_first_byte_monotonic_ns=request_ns,
        response_completed_at_ms=wall_ms,
        response_completed_monotonic_ns=request_ns,
        server_time_ms=server_time_ms,
        header_rtt_ns=0,
        wall_header_elapsed_ms=0,
        header_wall_monotonic_residual_ns=0,
        completion_wall_elapsed_ms=0,
        completion_wall_monotonic_residual_ns=0,
        offset_lower_ms=server_time_ms - wall_ms - 1,
        offset_upper_ms=server_time_ms - wall_ms + 1,
        available_at_monotonic_ns=request_ns,
    )


def _generic_record(
    *,
    ingest_seq: int,
    receipt_ns: int,
    received_at_ms: int | None = None,
) -> CaptureEnvelopeV1:
    return CaptureEnvelopeV1(
        received_at_ms=(
            _wall_for_mono(receipt_ns)
            if received_at_ms is None
            else received_at_ms
        ),
        received_monotonic_ns=receipt_ns,
        plan_sha256=_PLAN_SHA,
        process_boot_id="boot-1",
        connection_id="spot-g1",
        frame_seq=ingest_seq,
        ingest_seq=ingest_seq,
        market=Market.SPOT,
        route="spot",
        stream="btcusdt@aggTrade",
        subscription_streams=("btcusdt@aggTrade",),
        raw_payload="{}",
    )


def _wall_for_mono(monotonic_ns: int) -> int:
    return _START_WALL_MS + (monotonic_ns - _START_NS) // 1_000_000


def _authority(
    records: list[CaptureRecord],
    *,
    elapsed_ns: int,
    stop_reason: str = "completed_duration",
    fatal: bool = False,
) -> ClosedCaptureAuthority:
    manifests: tuple[SegmentManifestV1, ...]
    if records:
        manifests = (
            SegmentManifestV1(
                data_file="0000000000000-00000001.jsonl.zst",
                sequence=1,
                bucket_start_ms=0,
                rotation_interval_ms=300_000,
                plan_sha256=_PLAN_SHA,
                process_boot_id="boot-1",
                first_received_at_ms=0,
                last_received_at_ms=0,
                first_ingest_seq=1,
                last_ingest_seq=len(records),
                record_count=len(records),
                frame_count=sum(isinstance(item, CaptureEnvelopeV1) for item in records),
                uncompressed_bytes=1,
                compressed_bytes=1,
                sha256="b" * 64,
                previous_segment_sha256=None,
                recovered_from_partial=False,
                frame_format_version=1,
            ),
        )
    else:
        manifests = ()
    stub = _AuthorityStub(
        start=_StartStub(
            session_id="session-1",
            protocol_sha256="c" * 64,
            source_manifest_sha256="d" * 64,
            capture_plan_sha256=_PLAN_SHA,
            started_at_ms=_START_WALL_MS,
            started_monotonic_ns=_START_NS,
        ),
        closure=_ClosureStub(
            closed_at_ms=_START_WALL_MS + elapsed_ns // 1_000_000,
            closed_monotonic_ns=_START_NS + elapsed_ns,
            fatal=fatal,
            stop_reason=stop_reason,
        ),
        manifests=manifests,
        start_sha256="e" * 64,
        closure_sha256="f" * 64,
        external_start_sha256="1" * 64,
        external_closure_sha256="2" * 64,
    )
    return cast(ClosedCaptureAuthority, stub)


def _patch_closed_evidence(
    monkeypatch: pytest.MonkeyPatch,
    authority: ClosedCaptureAuthority,
    records: list[CaptureRecord],
) -> None:
    monkeypatch.setattr(
        report_module,
        "verify_closed_capture_authority",
        lambda **_kwargs: authority,
    )

    def consume(
        _authority: ClosedCaptureAuthority,
        callback: Callable[[CaptureRecord], None],
    ) -> None:
        for record in records:
            callback(record)

    monkeypatch.setattr(report_module, "consume_closed_capture_records", consume)
