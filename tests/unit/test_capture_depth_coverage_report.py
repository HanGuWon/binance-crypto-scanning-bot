from __future__ import annotations

import hashlib
import json
from collections.abc import Callable
from dataclasses import dataclass
from typing import cast

import pytest

import signalbot.capture.depth_coverage_report as report_module
from signalbot.capture.closed_evidence import ClosedCaptureAuthority
from signalbot.capture.depth_coverage_report import (
    build_depth_reconstruction_coverage_report,
)
from signalbot.capture.errors import CaptureIntegrityError
from signalbot.capture.local_book import (
    LocalBookCoverageState,
    LocalBookMaterializer,
    LocalBookReason,
    LocalBookStatus,
)
from signalbot.capture.models import (
    CaptureEnvelopeV1,
    CaptureRecord,
    ConnectionState,
    ConnectionTransitionV1,
    CoverageReason,
    CoverageState,
    CoverageTransitionV1,
    RestEnvelopeV2,
)
from signalbot.capture.storage import SegmentManifestV1
from signalbot.domain.enums import Market

_PLAN_SHA = "a" * 64
_START_NS = 10_000_000_000
_FULL_DURATION_NS = 86_400 * 1_000_000_000
_SYMBOLS = ("BTCUSDT", "ETHUSDT", "SOLUSDT")


@dataclass(frozen=True, slots=True)
class _StartStub:
    session_id: str
    protocol_sha256: str
    source_manifest_sha256: str
    capture_plan_sha256: str
    started_monotonic_ns: int


@dataclass(frozen=True, slots=True)
class _ClosureStub:
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


def test_one_bootstrap_then_full_day_silence_fails_freshness_without_sort(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    records = _valid_six_book_records()
    authority = _authority(records, elapsed_ns=_FULL_DURATION_NS)
    _patch_closed_evidence(monkeypatch, authority, records)

    def reject_sorted_view(
        _self: LocalBookMaterializer, _market: Market, _symbol: str
    ) -> object:
        raise AssertionError("coverage report must not call the sorted full-book view")

    monkeypatch.setattr(LocalBookMaterializer, "view", reject_sorted_view)
    first = build_depth_reconstruction_coverage_report(
        start_path="ignored-start",
        closure_path="ignored-closure",
        capture_directory="ignored-capture",
    )
    second = build_depth_reconstruction_coverage_report(
        start_path="ignored-start",
        closure_path="ignored-closure",
        capture_directory="ignored-capture",
    )

    assert first.report.verdict == "FAIL"
    assert first.report.verdict_reasons == (
        "fresh_two_sided_uncrossed_coverage_below_canary_requirement",
    )
    assert len(first.report.books) == 6
    assert tuple((book.market, book.symbol) for book in first.report.books) == (
        ("spot", "BTCUSDT"),
        ("spot", "ETHUSDT"),
        ("spot", "SOLUSDT"),
        ("futures", "BTCUSDT"),
        ("futures", "ETHUSDT"),
        ("futures", "SOLUSDT"),
    )
    assert all(book.diagnostics.books_became_valid == 1 for book in first.report.books)
    assert all(book.sequence_valid_coverage_ppm >= 995_000 for book in first.report.books)
    assert all(book.stale_sequence_valid_duration_ns > 0 for book in first.report.books)
    assert all(
        book.fresh_depth_evidence_coverage_ppm < 995_000
        for book in first.report.books
    )
    assert first.report.scope_boundaries.efficacy_acceptance_performed is False
    assert first.report.scope_boundaries.family_b_authorized is False
    assert first.report.scope_boundaries.promotion_authorized is False
    assert first.canonical_bytes == second.canonical_bytes
    assert first.sha256 == hashlib.sha256(first.canonical_bytes).hexdigest()
    assert b'"pnl"' not in first.canonical_bytes
    assert b'"outcome"' not in first.canonical_bytes
    assert b'"price"' not in first.canonical_bytes


def test_short_operator_smoke_is_incomplete(monkeypatch: pytest.MonkeyPatch) -> None:
    records = _valid_six_book_records()
    authority = _authority(
        records,
        elapsed_ns=60 * 1_000_000_000,
        stop_reason="operator_requested",
    )
    _patch_closed_evidence(monkeypatch, authority, records)

    report = build_depth_reconstruction_coverage_report(
        start_path="ignored-start",
        closure_path="ignored-closure",
        capture_directory="ignored-capture",
    ).report

    assert report.verdict == "INCOMPLETE"
    assert report.verdict_reasons == (
        "configured_duration_not_observed",
        "closure_not_completed_duration",
    )


def test_full_duration_without_depth_is_fail_not_incomplete(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    records: list[CaptureRecord] = []
    authority = _authority(records, elapsed_ns=_FULL_DURATION_NS)
    _patch_closed_evidence(monkeypatch, authority, records)

    report = build_depth_reconstruction_coverage_report(
        start_path="ignored-start",
        closure_path="ignored-closure",
        capture_directory="ignored-capture",
    ).report

    assert report.verdict == "FAIL"
    assert report.verdict_reasons == (
        "book_never_became_sequence_valid",
        "sequence_valid_coverage_below_canary_requirement",
        "fresh_two_sided_uncrossed_coverage_below_canary_requirement",
    )


def test_global_unattributed_malformed_depth_is_hard_fail(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    records = _valid_six_book_records()
    last = records[-1]
    assert isinstance(last, RestEnvelopeV2)
    receipt_ns = last.response_completed_monotonic_ns + 1_000_000
    records.append(
        CaptureEnvelopeV1(
            received_at_ms=receipt_ns // 1_000_000,
            received_monotonic_ns=receipt_ns,
            plan_sha256=_PLAN_SHA,
            process_boot_id="boot-1",
            connection_id="spot-g1",
            frame_seq=4,
            ingest_seq=len(records) + 1,
            market=Market.SPOT,
            route="spot",
            stream="combined:capture-spot-1",
            subscription_streams=("bnbusdt@depth@100ms",),
            raw_payload='{"stream":"bnbusdt@depth@100ms","data":{}}',
        )
    )
    authority = _authority(records, elapsed_ns=_FULL_DURATION_NS)
    _patch_closed_evidence(monkeypatch, authority, records)

    report = build_depth_reconstruction_coverage_report(
        start_path="ignored-start",
        closure_path="ignored-closure",
        capture_directory="ignored-capture",
    ).report

    assert report.verdict == "FAIL"
    assert report.replay.malformed_records == 1
    assert report.verdict_reasons == ("malformed_depth_record_observed",)


def test_crossed_duration_is_separate_and_terminal_disconnect_is_not_gap(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # This legacy coverage fixture crosses a 100/101 toy book with a 102 bid.
    # Keep its diagnostic intent separate from the production 10/20 bp guard.
    monkeypatch.setattr(
        report_module,
        "LocalBookMaterializer",
        lambda: LocalBookMaterializer(guard_band_bps=2_000),
    )
    records = _valid_six_book_records()
    receipt_ns = _last_receipt_ns(records) + 1_000_000
    records.append(
        _depth_record(
            ingest_seq=len(records) + 1,
            frame_seq=4,
            market=Market.SPOT,
            symbol="BTCUSDT",
            receipt_ns=receipt_ns,
            first_u=103,
            final_u=103,
            bids=(("102", "1"),),
        )
    )
    restored_ns = receipt_ns + 10_000_000_000
    records.append(
        _depth_record(
            ingest_seq=len(records) + 1,
            frame_seq=5,
            market=Market.SPOT,
            symbol="BTCUSDT",
            receipt_ns=restored_ns,
            first_u=104,
            final_u=104,
            bids=(("102", "0"),),
        )
    )
    terminal_ns = _START_NS + _FULL_DURATION_NS - 1
    for market in (Market.SPOT, Market.FUTURES):
        records.append(
            _connection_transition(
                ingest_seq=len(records) + 1,
                market=market,
                receipt_ns=terminal_ns,
                state=ConnectionState.DISCONNECTED,
            )
        )
    authority = _authority(records, elapsed_ns=_FULL_DURATION_NS)
    _patch_closed_evidence(monkeypatch, authority, records)

    report = build_depth_reconstruction_coverage_report(
        start_path="ignored-start",
        closure_path="ignored-closure",
        capture_directory="ignored-capture",
    ).report
    btc = report.books[0]

    assert btc.crossed_or_locked_duration_ns == 2_000_000_000
    assert btc.stale_sequence_valid_duration_ns > 0
    assert btc.unresolved_sequence_gap is False
    assert all(book.unresolved_reconstruction is False for book in report.books)
    assert report.verdict == "FAIL"


def test_unresolved_gap_is_reported_and_fails_full_run(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    records = _valid_six_book_records()
    records.append(
        _depth_record(
            ingest_seq=len(records) + 1,
            frame_seq=4,
            market=Market.SPOT,
            symbol="BTCUSDT",
            receipt_ns=_last_receipt_ns(records) + 1_000_000,
            first_u=200,
            final_u=201,
        )
    )
    authority = _authority(records, elapsed_ns=_FULL_DURATION_NS)
    _patch_closed_evidence(monkeypatch, authority, records)

    report = build_depth_reconstruction_coverage_report(
        start_path="ignored-start",
        closure_path="ignored-closure",
        capture_directory="ignored-capture",
    ).report

    assert report.books[0].unresolved_sequence_gap is True
    assert report.books[0].diagnostics.sequence_gaps == 1
    assert report.verdict == "FAIL"
    assert "unresolved_sequence_gap_observed" in report.verdict_reasons


def test_late_unresolved_reconnect_fails_even_when_coverage_passes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(report_module, "_QUOTE_AGE_NS_MAX", _FULL_DURATION_NS)
    records = _valid_six_book_records()
    reconnect_ns = _START_NS + _FULL_DURATION_NS - 10_000_000_000
    records.append(
        _connection_transition(
            ingest_seq=len(records) + 1,
            market=Market.SPOT,
            receipt_ns=reconnect_ns,
            state=ConnectionState.DISCONNECTED,
        )
    )
    records.append(
        _connection_transition(
            ingest_seq=len(records) + 1,
            market=Market.SPOT,
            receipt_ns=reconnect_ns + 1,
            connection_id="spot-g2",
        )
    )
    records.append(
        _connection_transition(
            ingest_seq=len(records) + 1,
            market=Market.SPOT,
            receipt_ns=reconnect_ns + 2,
            connection_id="spot-g2",
            state=ConnectionState.DISCONNECTED,
        )
    )
    authority = _authority(records, elapsed_ns=_FULL_DURATION_NS)
    _patch_closed_evidence(monkeypatch, authority, records)

    report = build_depth_reconstruction_coverage_report(
        start_path="ignored-start",
        closure_path="ignored-closure",
        capture_directory="ignored-capture",
    ).report

    assert all(
        book.meets_canary_sequence_valid_requirement for book in report.books
    )
    assert all(
        book.unresolved_reconstruction
        for book in report.books
        if book.market == "spot"
    )
    assert not any(book.unresolved_sequence_gap for book in report.books)
    assert report.verdict == "FAIL"
    assert report.verdict_reasons == ("unresolved_reconstruction_observed",)


def test_non_orderly_terminal_disconnect_requires_resynchronization(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(report_module, "_QUOTE_AGE_NS_MAX", _FULL_DURATION_NS)
    records = _valid_six_book_records()
    records.append(
        _connection_transition(
            ingest_seq=len(records) + 1,
            market=Market.SPOT,
            receipt_ns=_START_NS + _FULL_DURATION_NS - 1,
            state=ConnectionState.DISCONNECTED,
            reason="connection_failure",
        )
    )
    authority = _authority(records, elapsed_ns=_FULL_DURATION_NS)
    _patch_closed_evidence(monkeypatch, authority, records)

    report = build_depth_reconstruction_coverage_report(
        start_path="ignored-start",
        closure_path="ignored-closure",
        capture_directory="ignored-capture",
    ).report

    assert all(
        book.unresolved_reconstruction
        for book in report.books
        if book.market == "spot"
    )
    assert report.verdict_reasons == ("unresolved_reconstruction_observed",)


def test_integer_ppm_boundary_is_conservative() -> None:
    assert report_module._ppm(995_000, 1_000_000) == 995_000
    assert report_module._ppm(994_999, 1_000_000) == 994_999
    assert report_module._ppm(999_999, 1_000_000) == 999_999


def test_local_receipt_freshness_exact_deadline_silence_and_resume() -> None:
    diagnostics = LocalBookMaterializer().book_metrics(Market.SPOT, "BTCUSDT")
    initial = _coverage_state(availability_ns=0)

    exact = report_module._BookCoverageAccumulator(start_ns=0, last_ns=0)
    exact.observe(0, initial)
    exact_report = exact.finish(
        end_ns=2_000_000_000,
        state=initial,
        diagnostics=diagnostics,
    )
    assert exact_report.two_sided_uncrossed_duration_ns == 2_000_000_000
    assert exact_report.stale_sequence_valid_duration_ns == 0

    stale = report_module._BookCoverageAccumulator(start_ns=0, last_ns=0)
    stale.observe(0, initial)
    # An observation whose applied-evidence receipt did not advance cannot refresh age.
    stale.observe(1_000_000_000, initial)
    stale_report = stale.finish(
        end_ns=2_000_000_001,
        state=initial,
        diagnostics=diagnostics,
    )
    assert stale_report.two_sided_uncrossed_duration_ns == 2_000_000_000
    assert stale_report.stale_sequence_valid_duration_ns == 1
    assert stale_report.longest_depth_evidence_silence_ns == 2_000_000_001

    resumed = report_module._BookCoverageAccumulator(start_ns=0, last_ns=0)
    resumed.observe(0, initial)
    refreshed = _coverage_state(availability_ns=5_000_000_000)
    resumed.observe(5_000_000_000, refreshed)
    resumed_report = resumed.finish(
        end_ns=7_000_000_000,
        state=refreshed,
        diagnostics=diagnostics,
    )
    assert resumed_report.two_sided_uncrossed_duration_ns == 4_000_000_000
    assert resumed_report.stale_sequence_valid_duration_ns == 3_000_000_000
    assert resumed_report.longest_depth_evidence_silence_ns == 5_000_000_000


def test_local_receipt_freshness_refreshes_at_each_exact_deadline() -> None:
    diagnostics = LocalBookMaterializer().book_metrics(Market.SPOT, "BTCUSDT")
    accumulator = report_module._BookCoverageAccumulator(start_ns=0, last_ns=0)
    state = _coverage_state(availability_ns=0)
    accumulator.observe(0, state)

    for receipt_ns in (2_000_000_000, 4_000_000_000, 6_000_000_000):
        state = _coverage_state(availability_ns=receipt_ns)
        accumulator.observe(receipt_ns, state)

    coverage = accumulator.finish(
        end_ns=8_000_000_000,
        state=state,
        diagnostics=diagnostics,
    )

    assert coverage.two_sided_uncrossed_duration_ns == 8_000_000_000
    assert coverage.fresh_depth_evidence_duration_ns == 8_000_000_000
    assert coverage.stale_sequence_valid_duration_ns == 0
    assert coverage.longest_depth_evidence_silence_ns == 2_000_000_000


def test_fatal_closure_is_hard_fail(monkeypatch: pytest.MonkeyPatch) -> None:
    records = _valid_six_book_records()
    authority = _authority(records, elapsed_ns=60 * 1_000_000_000, fatal=True)
    _patch_closed_evidence(monkeypatch, authority, records)

    report = build_depth_reconstruction_coverage_report(
        start_path="ignored-start",
        closure_path="ignored-closure",
        capture_directory="ignored-capture",
    ).report

    assert report.verdict == "FAIL"
    assert report.verdict_reasons == ("fatal_session_closure",)


def test_persisted_coverage_invalid_is_hard_fail(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    records = _valid_six_book_records()
    receipt_ns = _last_receipt_ns(records) + 1_000_000
    records.append(
        CoverageTransitionV1(
            received_at_ms=receipt_ns // 1_000_000,
            received_monotonic_ns=receipt_ns,
            plan_sha256=_PLAN_SHA,
            process_boot_id="boot-1",
            connection_id="capture-rest",
            frame_seq=0,
            ingest_seq=len(records) + 1,
            market=Market.SPOT,
            route="rest",
            stream="capture",
            state=CoverageState.INVALID,
            reason=CoverageReason.HASH_INTEGRITY,
            detail="test coverage invalidation",
        )
    )
    authority = _authority(records, elapsed_ns=_FULL_DURATION_NS)
    _patch_closed_evidence(monkeypatch, authority, records)

    report = build_depth_reconstruction_coverage_report(
        start_path="ignored-start",
        closure_path="ignored-closure",
        capture_directory="ignored-capture",
    ).report

    assert report.verdict == "FAIL"
    assert report.replay.coverage_invalid_record_count == 1
    assert report.verdict_reasons == ("coverage_invalid_record_observed",)


def test_buffer_and_level_overflow_are_hard_fail(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    buffer_records = _valid_six_book_records()
    receipt_ns = _last_receipt_ns(buffer_records) + 1_000_000
    for first_u in (200, 300):
        buffer_records.append(
            _depth_record(
                ingest_seq=len(buffer_records) + 1,
                frame_seq=3 + (first_u // 100),
                market=Market.SPOT,
                symbol="BTCUSDT",
                receipt_ns=receipt_ns,
                first_u=first_u,
                final_u=first_u + 1,
            )
        )
        receipt_ns += 1_000_000
    buffer_authority = _authority(buffer_records, elapsed_ns=60 * 1_000_000_000)
    _patch_closed_evidence(monkeypatch, buffer_authority, buffer_records)
    monkeypatch.setattr(
        report_module,
        "LocalBookMaterializer",
        lambda: LocalBookMaterializer(max_buffered_events_per_book=1),
    )

    buffer_report = build_depth_reconstruction_coverage_report(
        start_path="ignored-start",
        closure_path="ignored-closure",
        capture_directory="ignored-capture",
    ).report
    assert buffer_report.verdict == "FAIL"
    assert buffer_report.replay.buffer_overflows == 1
    assert buffer_report.verdict_reasons == ("bounded_replay_overflow_observed",)

    level_records = _valid_six_book_records()
    level_records.append(
        _depth_record(
            ingest_seq=len(level_records) + 1,
            frame_seq=4,
            market=Market.SPOT,
            symbol="BTCUSDT",
            receipt_ns=_last_receipt_ns(level_records) + 1_000_000,
            first_u=103,
            final_u=103,
            bids=(("100.01", "1"),),
        )
    )
    level_authority = _authority(level_records, elapsed_ns=60 * 1_000_000_000)
    _patch_closed_evidence(monkeypatch, level_authority, level_records)
    monkeypatch.setattr(
        report_module,
        "LocalBookMaterializer",
        lambda: LocalBookMaterializer(max_levels_per_side=1),
    )

    level_report = build_depth_reconstruction_coverage_report(
        start_path="ignored-start",
        closure_path="ignored-closure",
        capture_directory="ignored-capture",
    ).report
    assert level_report.verdict == "FAIL"
    assert level_report.replay.level_overflows == 1
    assert level_report.verdict_reasons == ("bounded_replay_overflow_observed",)


@pytest.mark.parametrize(
    ("startup_unavailable_ns", "expected_verdict"),
    [
        (432_000_000_000, "DEPTH_RECONSTRUCTION_COVERAGE_PASS"),
        (432_000_000_001, "FAIL"),
    ],
)
def test_canary_995000_ppm_exact_boundary(
    monkeypatch: pytest.MonkeyPatch,
    startup_unavailable_ns: int,
    expected_verdict: str,
) -> None:
    # Isolate integer verdict arithmetic; receipt freshness has dedicated exact tests.
    monkeypatch.setattr(report_module, "_QUOTE_AGE_NS_MAX", _FULL_DURATION_NS)
    records = _valid_six_book_records_at(_START_NS + startup_unavailable_ns)
    authority = _authority(records, elapsed_ns=_FULL_DURATION_NS)
    _patch_closed_evidence(monkeypatch, authority, records)

    report = build_depth_reconstruction_coverage_report(
        start_path="ignored-start",
        closure_path="ignored-closure",
        capture_directory="ignored-capture",
    ).report

    assert report.verdict == expected_verdict
    expected_ppm = 995_000 if startup_unavailable_ns == 432_000_000_000 else 994_999
    assert {book.sequence_valid_coverage_ppm for book in report.books} == {expected_ppm}
    assert {
        book.two_sided_uncrossed_coverage_ppm for book in report.books
    } == {expected_ppm}


@pytest.mark.parametrize(
    ("startup_unavailable_ns", "expected_flag", "expected_ppm"),
    [
        (86_400_000_000, True, 999_000),
        (86_400_000_001, False, 998_999),
    ],
)
def test_prospective_999000_ppm_diagnostic_boundary(
    monkeypatch: pytest.MonkeyPatch,
    startup_unavailable_ns: int,
    expected_flag: bool,
    expected_ppm: int,
) -> None:
    monkeypatch.setattr(report_module, "_QUOTE_AGE_NS_MAX", _FULL_DURATION_NS)
    records = _valid_six_book_records_at(_START_NS + startup_unavailable_ns)
    authority = _authority(records, elapsed_ns=_FULL_DURATION_NS)
    _patch_closed_evidence(monkeypatch, authority, records)

    report = build_depth_reconstruction_coverage_report(
        start_path="ignored-start",
        closure_path="ignored-closure",
        capture_directory="ignored-capture",
    ).report

    assert report.verdict == "DEPTH_RECONSTRUCTION_COVERAGE_PASS"
    assert {book.sequence_valid_coverage_ppm for book in report.books} == {
        expected_ppm
    }
    assert {
        book.meets_prospective_sequence_diagnostic for book in report.books
    } == {expected_flag}
    assert {
        book.meets_prospective_two_sided_uncrossed_diagnostic
        for book in report.books
    } == {expected_flag}


def test_receipt_monotonic_reversal_fails_closed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    records = _valid_six_book_records()
    later_ns = _last_receipt_ns(records) + 2_000_000
    for index, receipt_ns in enumerate((later_ns, later_ns - 1_000_000), start=1):
        records.append(
            CaptureEnvelopeV1(
                received_at_ms=receipt_ns // 1_000_000,
                received_monotonic_ns=receipt_ns,
                plan_sha256=_PLAN_SHA,
                process_boot_id="boot-1",
                connection_id="spot-g1",
                frame_seq=3 + index,
                ingest_seq=len(records) + 1,
                market=Market.SPOT,
                route="spot",
                stream="btcusdt@aggTrade",
                subscription_streams=("btcusdt@aggTrade",),
                raw_payload="{}",
            )
        )
    authority = _authority(records, elapsed_ns=_FULL_DURATION_NS)
    _patch_closed_evidence(monkeypatch, authority, records)

    with pytest.raises(CaptureIntegrityError, match="monotonic time moved backwards"):
        build_depth_reconstruction_coverage_report(
            start_path="ignored-start",
            closure_path="ignored-closure",
            capture_directory="ignored-capture",
        )


def _valid_six_book_records() -> list[CaptureRecord]:
    records: list[CaptureRecord] = []
    receipt_ns = _START_NS + 1_000_000_000
    for market in (Market.SPOT, Market.FUTURES):
        records.append(
            _connection_transition(
                ingest_seq=len(records) + 1,
                market=market,
                receipt_ns=receipt_ns,
            )
        )
        receipt_ns += 1_000_000
    frame_sequence = {Market.SPOT: 0, Market.FUTURES: 0}
    for market in (Market.SPOT, Market.FUTURES):
        for symbol in _SYMBOLS:
            frame_sequence[market] += 1
            records.append(
                _depth_record(
                    ingest_seq=len(records) + 1,
                    frame_seq=frame_sequence[market],
                    market=market,
                    symbol=symbol,
                    receipt_ns=receipt_ns,
                    first_u=100,
                    final_u=102,
                    previous_u=99,
                )
            )
            receipt_ns += 1_000_000
            records.append(
                _snapshot_record(
                    ingest_seq=len(records) + 1,
                    market=market,
                    symbol=symbol,
                    receipt_ns=receipt_ns,
                    update_id=101,
                )
            )
            receipt_ns += 1_000_000
    return records


def _valid_six_book_records_at(receipt_ns: int) -> list[CaptureRecord]:
    records: list[CaptureRecord] = []
    for market in (Market.SPOT, Market.FUTURES):
        records.append(
            _connection_transition(
                ingest_seq=len(records) + 1,
                market=market,
                receipt_ns=receipt_ns,
            )
        )
    frame_sequence = {Market.SPOT: 0, Market.FUTURES: 0}
    for market in (Market.SPOT, Market.FUTURES):
        for symbol in _SYMBOLS:
            frame_sequence[market] += 1
            records.append(
                _depth_record(
                    ingest_seq=len(records) + 1,
                    frame_seq=frame_sequence[market],
                    market=market,
                    symbol=symbol,
                    receipt_ns=receipt_ns,
                    first_u=100,
                    final_u=102,
                    previous_u=99,
                )
            )
            records.append(
                _snapshot_record(
                    ingest_seq=len(records) + 1,
                    market=market,
                    symbol=symbol,
                    receipt_ns=receipt_ns,
                    update_id=101,
                )
            )
    return records


def _connection_transition(
    *,
    ingest_seq: int,
    market: Market,
    receipt_ns: int,
    state: ConnectionState = ConnectionState.CONNECTED,
    connection_id: str | None = None,
    reason: str | None = None,
) -> ConnectionTransitionV1:
    streams = tuple(f"{symbol.lower()}@depth@100ms" for symbol in _SYMBOLS)
    return ConnectionTransitionV1(
        received_at_ms=receipt_ns // 1_000_000,
        received_monotonic_ns=receipt_ns,
        plan_sha256=_PLAN_SHA,
        process_boot_id="boot-1",
        connection_id=(
            connection_id
            or ("spot-g1" if market is Market.SPOT else "futures-g1")
        ),
        ingest_seq=ingest_seq,
        last_frame_seq=3 if state is ConnectionState.DISCONNECTED else 0,
        market=market,
        route="spot" if market is Market.SPOT else "public",
        streams=streams,
        state=state,
        reason=reason
        or (
            "owner_stop"
            if state is ConnectionState.DISCONNECTED
            else f"test_{state.value}"
        ),
    )


def _depth_record(
    *,
    ingest_seq: int,
    frame_seq: int,
    market: Market,
    symbol: str,
    receipt_ns: int,
    first_u: int,
    final_u: int,
    previous_u: int = 0,
    bids: tuple[tuple[str, str], ...] = (),
) -> CaptureEnvelopeV1:
    stream = f"{symbol.lower()}@depth@100ms"
    streams = tuple(f"{item.lower()}@depth@100ms" for item in _SYMBOLS)
    data: dict[str, object] = {
        "e": "depthUpdate",
        "s": symbol,
        "U": first_u,
        "u": final_u,
        "b": list(bids),
        "a": [],
    }
    if market is Market.FUTURES:
        data.update({"pu": previous_u, "ps": symbol, "st": 1})
    return CaptureEnvelopeV1(
        received_at_ms=receipt_ns // 1_000_000,
        received_monotonic_ns=receipt_ns,
        plan_sha256=_PLAN_SHA,
        process_boot_id="boot-1",
        connection_id="spot-g1" if market is Market.SPOT else "futures-g1",
        frame_seq=frame_seq,
        ingest_seq=ingest_seq,
        market=market,
        route="spot" if market is Market.SPOT else "public",
        stream=(
            "combined:capture-spot-1"
            if market is Market.SPOT
            else "combined:capture-futures-public-1"
        ),
        subscription_streams=streams,
        raw_payload=json.dumps({"stream": stream, "data": data}, separators=(",", ":")),
    )


def _snapshot_record(
    *,
    ingest_seq: int,
    market: Market,
    symbol: str,
    receipt_ns: int,
    update_id: int,
) -> RestEnvelopeV2:
    is_spot = market is Market.SPOT
    return RestEnvelopeV2(
        request_started_at_ms=receipt_ns // 1_000_000,
        request_started_monotonic_ns=receipt_ns,
        response_first_byte_at_ms=receipt_ns // 1_000_000,
        response_first_byte_monotonic_ns=receipt_ns,
        response_completed_at_ms=receipt_ns // 1_000_000,
        response_completed_monotonic_ns=receipt_ns,
        plan_sha256=_PLAN_SHA,
        process_boot_id="boot-1",
        request_role="spot_depth_snapshot" if is_spot else "futures_depth_snapshot",
        correlation_id=f"snapshot-{market.value}-{symbol}",
        attempt=1,
        ingest_seq=ingest_seq,
        market=market,
        endpoint_path="/api/v3/depth" if is_spot else "/fapi/v1/depth",
        canonical_query=(
            ("limit", "5000" if is_spot else "1000"),
            ("symbol", symbol),
        ),
        response_status=200,
        response_headers=(("content-type", "application/json"),),
        payload_complete=True,
        raw_payload=json.dumps(
            {
                "lastUpdateId": update_id,
                "bids": [["100", "1"]],
                "asks": [["101", "1"]],
            },
            separators=(",", ":"),
        ),
    )


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
            started_monotonic_ns=_START_NS,
        ),
        closure=_ClosureStub(
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


def _last_receipt_ns(records: list[CaptureRecord]) -> int:
    return report_module._record_receipt_monotonic_ns(records[-1])


def _coverage_state(*, availability_ns: int) -> LocalBookCoverageState:
    return LocalBookCoverageState(
        market=Market.SPOT,
        symbol="BTCUSDT",
        status=LocalBookStatus.VALID,
        reason=LocalBookReason.VALID,
        generation=1,
        availability_receipt_monotonic_ns=availability_ns,
        bid_level_count=1,
        ask_level_count=1,
        has_bid=True,
        has_ask=True,
        crossed_or_locked=False,
        unresolved_sequence_gap=False,
        unresolved_reconstruction=False,
    )
