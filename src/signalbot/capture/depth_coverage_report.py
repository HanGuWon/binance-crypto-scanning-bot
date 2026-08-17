from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Literal, TypeAlias

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from signalbot.capture.closed_evidence import (
    ClosedCaptureAuthority,
    consume_closed_capture_records,
    verify_closed_capture_authority,
)
from signalbot.capture.errors import CaptureIntegrityError
from signalbot.capture.local_book import (
    LocalBookCoverageState,
    LocalBookMaterializer,
    LocalBookMetrics,
    LocalBookPerBookMetrics,
    LocalBookStatus,
)
from signalbot.capture.models import (
    CaptureRecord,
    CoverageState,
    CoverageTransitionV1,
    RestEnvelopeV1,
    RestEnvelopeV2,
)
from signalbot.capture.provenance import canonical_json_bytes
from signalbot.domain.enums import Market

_EXPECTED_DURATION_SECONDS = 86_400
_CANARY_DEPTH_COVERAGE_MIN_PPM = 995_000
_PROSPECTIVE_DIAGNOSTIC_MIN_PPM = 999_000
_QUOTE_AGE_NS_MAX = 2_000_000_000
_EXPECTED_BOOK_KEYS = tuple(
    (market, symbol)
    for market in (Market.SPOT, Market.FUTURES)
    for symbol in ("BTCUSDT", "ETHUSDT", "SOLUSDT")
)
_FORBIDDEN_REPORT_KEYS = frozenset(
    {"pnl", "outcome", "return", "label", "signal", "order", "price"}
)

DepthCoverageVerdict: TypeAlias = Literal[  # noqa: UP040 - host may be Python 3.11
    "DEPTH_RECONSTRUCTION_COVERAGE_PASS",
    "INCOMPLETE",
    "FAIL",
]
DepthCoverageVerdictReason: TypeAlias = Literal[  # noqa: UP040
    "depth_reconstruction_coverage_requirements_satisfied",
    "fatal_session_closure",
    "coverage_invalid_record_observed",
    "malformed_depth_record_observed",
    "bounded_replay_overflow_observed",
    "configured_duration_not_observed",
    "closure_not_completed_duration",
    "book_never_became_sequence_valid",
    "sequence_valid_coverage_below_canary_requirement",
    "fresh_two_sided_uncrossed_coverage_below_canary_requirement",
    "unresolved_sequence_gap_observed",
    "unresolved_reconstruction_observed",
]
_CoverageCategory: TypeAlias = Literal[  # noqa: UP040
    "sequence_unavailable",
    "two_sided_uncrossed",
    "crossed_or_locked",
    "one_sided_or_empty",
    "stale_sequence_valid",
]


class _StrictReportModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)


class DepthAuthorityMetricsV1(_StrictReportModel):
    session_id: str
    protocol_sha256: str
    source_manifest_sha256: str
    capture_plan_sha256: str
    session_start_sha256: str
    session_closure_sha256: str
    external_start_sha256: str
    external_closure_sha256: str
    canonical_session_documents_verified: Literal[True]
    external_audit_subject_chain_verified: Literal[True]
    full_segment_chain_verified: Literal[True]
    clean_closure: bool

    @field_validator(
        "protocol_sha256",
        "source_manifest_sha256",
        "capture_plan_sha256",
        "session_start_sha256",
        "session_closure_sha256",
        "external_start_sha256",
        "external_closure_sha256",
    )
    @classmethod
    def require_sha256(cls, value: str) -> str:
        if re.fullmatch(r"[0-9a-f]{64}", value) is None:
            raise ValueError("report authority hashes must be lowercase SHA-256 digests")
        return value


class DepthObservationWindowV1(_StrictReportModel):
    started_monotonic_ns: int = Field(ge=0)
    closed_monotonic_ns: int = Field(ge=0)
    observation_duration_ns: int = Field(ge=0)
    configured_duration_seconds: Literal[86_400]
    full_configured_duration_observed: bool

    @model_validator(mode="after")
    def require_coherent_window(self) -> DepthObservationWindowV1:
        if self.closed_monotonic_ns < self.started_monotonic_ns:
            raise ValueError("depth observation window is reversed")
        if (
            self.observation_duration_ns
            != self.closed_monotonic_ns - self.started_monotonic_ns
        ):
            raise ValueError("depth observation duration differs from its endpoints")
        expected_full = self.observation_duration_ns >= 86_400 * 1_000_000_000
        if self.full_configured_duration_observed != expected_full:
            raise ValueError("full-duration flag differs from the observation window")
        return self


class DepthStorageBindingV1(_StrictReportModel):
    segment_count: int = Field(ge=0)
    record_count: int = Field(ge=0)
    compressed_bytes: int = Field(ge=0)
    uncompressed_bytes: int = Field(ge=0)
    typed_canonical_records_streamed: Literal[True]
    ingest_sequence_verified: Literal[True]
    receipt_monotonic_order_verified: Literal[True]


class PerBookReplayDiagnosticsV1(_StrictReportModel):
    depth_transitions: int = Field(ge=0)
    connection_resets: int = Field(ge=0)
    disconnects: int = Field(ge=0)
    depth_events_received: int = Field(ge=0)
    depth_events_buffered: int = Field(ge=0)
    depth_events_applied: int = Field(ge=0)
    old_events_ignored: int = Field(ge=0)
    snapshots_received: int = Field(ge=0)
    snapshot_candidates_held: int = Field(ge=0)
    snapshots_applied: int = Field(ge=0)
    snapshot_request_failures: int = Field(ge=0)
    stale_snapshots_rejected: int = Field(ge=0)
    redundant_snapshots_ignored: int = Field(ge=0)
    bridge_failures: int = Field(ge=0)
    sequence_gaps: int = Field(ge=0)
    malformed_records: int = Field(ge=0)
    buffer_overflows: int = Field(ge=0)
    level_overflows: int = Field(ge=0)
    stale_connection_records: int = Field(ge=0)
    books_became_valid: int = Field(ge=0)


class PerBookDepthCoverageV1(_StrictReportModel):
    market: Literal["spot", "futures"]
    symbol: str
    book_key: str
    observation_duration_ns: int = Field(ge=0)
    sequence_unavailable_duration_ns: int = Field(ge=0)
    two_sided_uncrossed_duration_ns: int = Field(ge=0)
    crossed_or_locked_duration_ns: int = Field(ge=0)
    one_sided_or_empty_duration_ns: int = Field(ge=0)
    stale_sequence_valid_duration_ns: int = Field(ge=0)
    fresh_depth_evidence_duration_ns: int = Field(ge=0)
    sequence_valid_duration_ns: int = Field(ge=0)
    fresh_depth_evidence_coverage_ppm: int = Field(ge=0, le=1_000_000)
    sequence_valid_coverage_ppm: int = Field(ge=0, le=1_000_000)
    two_sided_uncrossed_coverage_ppm: int = Field(ge=0, le=1_000_000)
    meets_canary_sequence_valid_requirement: bool
    meets_canary_two_sided_uncrossed_requirement: bool
    meets_prospective_sequence_diagnostic: bool
    meets_prospective_two_sided_uncrossed_diagnostic: bool
    longest_sequence_unavailable_interval_ns: int = Field(ge=0)
    longest_non_usable_interval_ns: int = Field(ge=0)
    longest_depth_evidence_silence_ns: int = Field(ge=0)
    sequence_valid_interval_count: int = Field(ge=0)
    two_sided_uncrossed_interval_count: int = Field(ge=0)
    first_sequence_valid_offset_ns: int | None = Field(default=None, ge=0)
    first_two_sided_uncrossed_offset_ns: int | None = Field(default=None, ge=0)
    terminal_status: Literal[
        "disconnected", "awaiting_snapshot", "valid", "replay_failed"
    ]
    terminal_reason: Literal[
        "not_connected",
        "connecting",
        "connected_awaiting_snapshot",
        "snapshot_pending_first_event",
        "snapshot_request_failed",
        "stale_snapshot",
        "connection_protocol_error",
        "malformed_event",
        "malformed_snapshot",
        "bridge_failure",
        "sequence_gap",
        "buffer_overflow",
        "level_overflow",
        "disconnected",
        "recycled",
        "valid",
        "ingest_gap",
    ]
    unresolved_sequence_gap: bool
    unresolved_reconstruction: bool
    diagnostics: PerBookReplayDiagnosticsV1

    @model_validator(mode="after")
    def require_coherent_book_metrics(self) -> PerBookDepthCoverageV1:
        if self.book_key != f"{self.market}|{self.symbol}":
            raise ValueError("depth book key differs from market and symbol")
        category_total = (
            self.sequence_unavailable_duration_ns
            + self.two_sided_uncrossed_duration_ns
            + self.crossed_or_locked_duration_ns
            + self.one_sided_or_empty_duration_ns
            + self.stale_sequence_valid_duration_ns
        )
        if category_total != self.observation_duration_ns:
            raise ValueError("depth coverage categories do not partition the window")
        expected_valid = (
            self.two_sided_uncrossed_duration_ns
            + self.crossed_or_locked_duration_ns
            + self.one_sided_or_empty_duration_ns
            + self.stale_sequence_valid_duration_ns
        )
        if self.sequence_valid_duration_ns != expected_valid:
            raise ValueError("sequence-valid duration differs from valid categories")
        expected_sequence_ppm = _ppm(expected_valid, self.observation_duration_ns)
        expected_fresh = (
            self.two_sided_uncrossed_duration_ns
            + self.crossed_or_locked_duration_ns
            + self.one_sided_or_empty_duration_ns
        )
        if self.fresh_depth_evidence_duration_ns != expected_fresh:
            raise ValueError("fresh depth duration differs from fresh categories")
        expected_fresh_ppm = _ppm(expected_fresh, self.observation_duration_ns)
        if self.fresh_depth_evidence_coverage_ppm != expected_fresh_ppm:
            raise ValueError("fresh depth evidence coverage ppm is inconsistent")
        expected_usable_ppm = _ppm(
            self.two_sided_uncrossed_duration_ns,
            self.observation_duration_ns,
        )
        if self.sequence_valid_coverage_ppm != expected_sequence_ppm:
            raise ValueError("sequence-valid coverage ppm is inconsistent")
        if self.two_sided_uncrossed_coverage_ppm != expected_usable_ppm:
            raise ValueError("two-sided coverage ppm is inconsistent")
        flags = (
            self.meets_canary_sequence_valid_requirement
            == (expected_sequence_ppm >= _CANARY_DEPTH_COVERAGE_MIN_PPM),
            self.meets_canary_two_sided_uncrossed_requirement
            == (expected_usable_ppm >= _CANARY_DEPTH_COVERAGE_MIN_PPM),
            self.meets_prospective_sequence_diagnostic
            == (expected_sequence_ppm >= _PROSPECTIVE_DIAGNOSTIC_MIN_PPM),
            self.meets_prospective_two_sided_uncrossed_diagnostic
            == (expected_usable_ppm >= _PROSPECTIVE_DIAGNOSTIC_MIN_PPM),
        )
        if not all(flags):
            raise ValueError("depth coverage requirement flags are inconsistent")
        for offset in (
            self.first_sequence_valid_offset_ns,
            self.first_two_sided_uncrossed_offset_ns,
        ):
            if offset is not None and offset > self.observation_duration_ns:
                raise ValueError("first-valid offset escapes the observation window")
        return self


class ReplayDiagnosticsV1(_StrictReportModel):
    records_processed: int = Field(ge=0)
    depth_transitions: int = Field(ge=0)
    connection_resets: int = Field(ge=0)
    disconnects: int = Field(ge=0)
    depth_events_received: int = Field(ge=0)
    depth_events_buffered: int = Field(ge=0)
    depth_events_applied: int = Field(ge=0)
    old_events_ignored: int = Field(ge=0)
    snapshots_received: int = Field(ge=0)
    snapshot_candidates_held: int = Field(ge=0)
    snapshots_applied: int = Field(ge=0)
    snapshot_request_failures: int = Field(ge=0)
    stale_snapshots_rejected: int = Field(ge=0)
    redundant_snapshots_ignored: int = Field(ge=0)
    bridge_failures: int = Field(ge=0)
    sequence_gaps: int = Field(ge=0)
    malformed_records: int = Field(ge=0)
    buffer_overflows: int = Field(ge=0)
    level_overflows: int = Field(ge=0)
    stale_connection_records: int = Field(ge=0)
    books_became_valid: int = Field(ge=0)
    ingest_gaps: int = Field(ge=0)
    coverage_invalid_record_count: int = Field(ge=0)


class DepthCoverageScopeBoundariesV1(_StrictReportModel):
    infrastructure_depth_only: Literal[True]
    market_payload_interpreted: Literal[True]
    future_information_interpreted: Literal[False]
    depth_sequence_acceptance_performed: Literal[True]
    depth_coverage_acceptance_performed: Literal[True]
    local_receipt_depth_freshness_acceptance_performed: Literal[True]
    efficacy_acceptance_performed: Literal[False]
    thirty_day_holdout_gate_evaluated: Literal[False]
    family_b_authorized: Literal[False]
    promotion_authorized: Literal[False]


class DepthReconstructionCoverageReportV1(_StrictReportModel):
    schema_version: Literal["depth_reconstruction_coverage_report_v1"]
    purpose: Literal["infrastructure_depth_reconstruction_coverage_only"]
    authority: DepthAuthorityMetricsV1
    observation_window: DepthObservationWindowV1
    storage: DepthStorageBindingV1
    local_receipt_depth_evidence_age_ms_max: Literal[2_000]
    canary_depth_coverage_min_ppm: Literal[995_000]
    prospective_diagnostic_min_ppm: Literal[999_000]
    books: tuple[PerBookDepthCoverageV1, ...]
    replay: ReplayDiagnosticsV1
    scope_boundaries: DepthCoverageScopeBoundariesV1
    verdict: DepthCoverageVerdict
    verdict_reasons: tuple[DepthCoverageVerdictReason, ...]

    @model_validator(mode="after")
    def require_coherent_report(self) -> DepthReconstructionCoverageReportV1:
        identities = tuple((book.market, book.symbol) for book in self.books)
        expected = tuple((market.value, symbol) for market, symbol in _EXPECTED_BOOK_KEYS)
        if identities != expected:
            raise ValueError("depth report must contain the exact six ordered books")
        if any(
            book.observation_duration_ns
            != self.observation_window.observation_duration_ns
            for book in self.books
        ):
            raise ValueError("book observation windows differ from session authority")
        if self.storage.record_count != self.replay.records_processed:
            raise ValueError("replay record count differs from storage authority")
        if not self.verdict_reasons:
            raise ValueError("depth coverage verdict requires at least one reason")
        if self.verdict == "DEPTH_RECONSTRUCTION_COVERAGE_PASS":
            if self.verdict_reasons != (
                "depth_reconstruction_coverage_requirements_satisfied",
            ):
                raise ValueError("PASS requires its single satisfied-requirements reason")
            if (
                not self.authority.clean_closure
                or not self.observation_window.full_configured_duration_observed
                or self.replay.coverage_invalid_record_count
                or self.replay.malformed_records
                or self.replay.buffer_overflows
                or self.replay.level_overflows
                or any(
                    not book.meets_canary_sequence_valid_requirement
                    or not book.meets_canary_two_sided_uncrossed_requirement
                    or book.unresolved_sequence_gap
                    or book.unresolved_reconstruction
                    or book.diagnostics.books_became_valid < 1
                    for book in self.books
                )
            ):
                raise ValueError("PASS contradicts depth reconstruction acceptance fields")
        return self


@dataclass(frozen=True, slots=True)
class CanonicalDepthCoverageReport:
    report: DepthReconstructionCoverageReportV1
    canonical_bytes: bytes
    sha256: str


@dataclass(slots=True)
class _BookCoverageAccumulator:
    start_ns: int
    last_ns: int
    category: _CoverageCategory = "sequence_unavailable"
    sequence_unavailable_ns: int = 0
    two_sided_uncrossed_ns: int = 0
    crossed_or_locked_ns: int = 0
    one_sided_or_empty_ns: int = 0
    stale_sequence_valid_ns: int = 0
    sequence_unavailable_run_ns: int = 0
    non_usable_run_ns: int = 0
    longest_sequence_unavailable_ns: int = 0
    longest_non_usable_ns: int = 0
    longest_evidence_silence_ns: int = 0
    last_evidence_ns: int | None = None
    availability_ns: int | None = None
    sequence_valid_interval_count: int = 0
    two_sided_uncrossed_interval_count: int = 0
    first_sequence_valid_offset_ns: int | None = None
    first_two_sided_uncrossed_offset_ns: int | None = None

    def observe(self, receipt_ns: int, state: LocalBookCoverageState) -> None:
        if receipt_ns < self.last_ns:
            raise CaptureIntegrityError("book coverage receipt monotonic time moved backwards")
        self._advance(receipt_ns)
        evidence_ns = state.availability_receipt_monotonic_ns
        if evidence_ns is not None and (
            self.last_evidence_ns is None or evidence_ns > self.last_evidence_ns
        ):
            previous = self.start_ns if self.last_evidence_ns is None else self.last_evidence_ns
            self.longest_evidence_silence_ns = max(
                self.longest_evidence_silence_ns,
                evidence_ns - previous,
            )
            self.last_evidence_ns = evidence_ns
        self.availability_ns = evidence_ns
        self._transition(_coverage_category(state, receipt_ns), receipt_ns)

    def _transition(
        self,
        next_category: _CoverageCategory,
        transition_ns: int,
    ) -> None:
        was_valid = self.category != "sequence_unavailable"
        is_valid = next_category != "sequence_unavailable"
        was_usable = self.category == "two_sided_uncrossed"
        is_usable = next_category == "two_sided_uncrossed"
        if not was_valid and is_valid:
            self.longest_sequence_unavailable_ns = max(
                self.longest_sequence_unavailable_ns,
                self.sequence_unavailable_run_ns,
            )
            self.sequence_unavailable_run_ns = 0
            self.sequence_valid_interval_count += 1
            if self.first_sequence_valid_offset_ns is None:
                self.first_sequence_valid_offset_ns = transition_ns - self.start_ns
        elif was_valid and not is_valid:
            self.sequence_unavailable_run_ns = 0
        if not was_usable and is_usable:
            self.longest_non_usable_ns = max(
                self.longest_non_usable_ns,
                self.non_usable_run_ns,
            )
            self.non_usable_run_ns = 0
            self.two_sided_uncrossed_interval_count += 1
            if self.first_two_sided_uncrossed_offset_ns is None:
                self.first_two_sided_uncrossed_offset_ns = transition_ns - self.start_ns
        elif was_usable and not is_usable:
            self.non_usable_run_ns = 0
        self.category = next_category

    def finish(
        self,
        *,
        end_ns: int,
        state: LocalBookCoverageState,
        diagnostics: LocalBookPerBookMetrics,
    ) -> PerBookDepthCoverageV1:
        if end_ns < self.last_ns:
            raise CaptureIntegrityError("closure monotonic time precedes book coverage")
        self._advance(end_ns)
        if self.category == "sequence_unavailable":
            self.longest_sequence_unavailable_ns = max(
                self.longest_sequence_unavailable_ns,
                self.sequence_unavailable_run_ns,
            )
        if self.category != "two_sided_uncrossed":
            self.longest_non_usable_ns = max(
                self.longest_non_usable_ns,
                self.non_usable_run_ns,
            )
        evidence_origin = self.start_ns if self.last_evidence_ns is None else self.last_evidence_ns
        self.longest_evidence_silence_ns = max(
            self.longest_evidence_silence_ns,
            end_ns - evidence_origin,
        )
        observation_ns = end_ns - self.start_ns
        fresh_depth_ns = (
            self.two_sided_uncrossed_ns
            + self.crossed_or_locked_ns
            + self.one_sided_or_empty_ns
        )
        sequence_valid_ns = (
            fresh_depth_ns + self.stale_sequence_valid_ns
        )
        sequence_ppm = _ppm(sequence_valid_ns, observation_ns)
        fresh_ppm = _ppm(fresh_depth_ns, observation_ns)
        usable_ppm = _ppm(self.two_sided_uncrossed_ns, observation_ns)
        return PerBookDepthCoverageV1(
            market=state.market.value,
            symbol=state.symbol,
            book_key=f"{state.market.value}|{state.symbol}",
            observation_duration_ns=observation_ns,
            sequence_unavailable_duration_ns=self.sequence_unavailable_ns,
            two_sided_uncrossed_duration_ns=self.two_sided_uncrossed_ns,
            crossed_or_locked_duration_ns=self.crossed_or_locked_ns,
            one_sided_or_empty_duration_ns=self.one_sided_or_empty_ns,
            stale_sequence_valid_duration_ns=self.stale_sequence_valid_ns,
            fresh_depth_evidence_duration_ns=fresh_depth_ns,
            sequence_valid_duration_ns=sequence_valid_ns,
            fresh_depth_evidence_coverage_ppm=fresh_ppm,
            sequence_valid_coverage_ppm=sequence_ppm,
            two_sided_uncrossed_coverage_ppm=usable_ppm,
            meets_canary_sequence_valid_requirement=(
                sequence_ppm >= _CANARY_DEPTH_COVERAGE_MIN_PPM
            ),
            meets_canary_two_sided_uncrossed_requirement=(
                usable_ppm >= _CANARY_DEPTH_COVERAGE_MIN_PPM
            ),
            meets_prospective_sequence_diagnostic=(
                sequence_ppm >= _PROSPECTIVE_DIAGNOSTIC_MIN_PPM
            ),
            meets_prospective_two_sided_uncrossed_diagnostic=(
                usable_ppm >= _PROSPECTIVE_DIAGNOSTIC_MIN_PPM
            ),
            longest_sequence_unavailable_interval_ns=(
                self.longest_sequence_unavailable_ns
            ),
            longest_non_usable_interval_ns=self.longest_non_usable_ns,
            longest_depth_evidence_silence_ns=self.longest_evidence_silence_ns,
            sequence_valid_interval_count=self.sequence_valid_interval_count,
            two_sided_uncrossed_interval_count=(
                self.two_sided_uncrossed_interval_count
            ),
            first_sequence_valid_offset_ns=self.first_sequence_valid_offset_ns,
            first_two_sided_uncrossed_offset_ns=(
                self.first_two_sided_uncrossed_offset_ns
            ),
            terminal_status=state.status.value,
            terminal_reason=state.reason.value,
            unresolved_sequence_gap=state.unresolved_sequence_gap,
            unresolved_reconstruction=state.unresolved_reconstruction,
            diagnostics=_per_book_diagnostics(diagnostics),
        )

    def _advance(self, receipt_ns: int) -> None:
        if receipt_ns < self.last_ns:
            raise CaptureIntegrityError("depth coverage duration became negative")
        if (
            self.category
            in {"two_sided_uncrossed", "crossed_or_locked", "one_sided_or_empty"}
            and self.availability_ns is not None
        ):
            deadline = self.availability_ns + _QUOTE_AGE_NS_MAX
            if self.last_ns <= deadline < receipt_ns:
                self._accrue(deadline - self.last_ns)
                self.last_ns = deadline
                self._transition("stale_sequence_valid", deadline)
        self._accrue(receipt_ns - self.last_ns)
        self.last_ns = receipt_ns

    def _accrue(self, elapsed_ns: int) -> None:
        if elapsed_ns < 0:
            raise CaptureIntegrityError("depth coverage duration became negative")
        if self.category == "sequence_unavailable":
            self.sequence_unavailable_ns += elapsed_ns
            self.sequence_unavailable_run_ns += elapsed_ns
        elif self.category == "two_sided_uncrossed":
            self.two_sided_uncrossed_ns += elapsed_ns
        elif self.category == "crossed_or_locked":
            self.crossed_or_locked_ns += elapsed_ns
        elif self.category == "one_sided_or_empty":
            self.one_sided_or_empty_ns += elapsed_ns
        else:
            self.stale_sequence_valid_ns += elapsed_ns
        if self.category != "two_sided_uncrossed":
            self.non_usable_run_ns += elapsed_ns


class _DepthCoverageAccumulator:
    def __init__(self, *, start_ns: int, end_ns: int) -> None:
        if end_ns < start_ns:
            raise CaptureIntegrityError("session closure monotonic time precedes its start")
        self.start_ns = start_ns
        self.end_ns = end_ns
        self.last_record_ns = start_ns
        self.materializer = LocalBookMaterializer()
        self.books = {
            key: _BookCoverageAccumulator(start_ns=start_ns, last_ns=start_ns)
            for key in _EXPECTED_BOOK_KEYS
        }
        self.coverage_invalid_record_count = 0

    def consume(self, record: CaptureRecord) -> None:
        receipt_ns = _record_receipt_monotonic_ns(record)
        if receipt_ns < self.start_ns or receipt_ns > self.end_ns:
            raise CaptureIntegrityError("capture receipt monotonic time escapes the session")
        if receipt_ns < self.last_record_ns:
            raise CaptureIntegrityError("capture receipt monotonic time moved backwards")
        self.last_record_ns = receipt_ns
        if isinstance(record, CoverageTransitionV1) and record.state is CoverageState.INVALID:
            self.coverage_invalid_record_count += 1
        result = self.materializer.process(record)
        for state in result.affected_books:
            self.books[(state.market, state.symbol)].observe(receipt_ns, state)

    def finish(
        self, authority: ClosedCaptureAuthority
    ) -> DepthReconstructionCoverageReportV1:
        book_reports = tuple(
            self.books[key].finish(
                end_ns=self.end_ns,
                state=self.materializer.coverage_state(*key),
                diagnostics=self.materializer.book_metrics(*key),
            )
            for key in _EXPECTED_BOOK_KEYS
        )
        metrics = self.materializer.metrics
        elapsed_ns = self.end_ns - self.start_ns
        full_duration = elapsed_ns >= _EXPECTED_DURATION_SECONDS * 1_000_000_000
        verdict, reasons = _verdict(
            authority=authority,
            full_duration=full_duration,
            books=book_reports,
            metrics=metrics,
            coverage_invalid_record_count=self.coverage_invalid_record_count,
        )
        manifests = authority.manifests
        return DepthReconstructionCoverageReportV1(
            schema_version="depth_reconstruction_coverage_report_v1",
            purpose="infrastructure_depth_reconstruction_coverage_only",
            authority=DepthAuthorityMetricsV1(
                session_id=authority.start.session_id,
                protocol_sha256=authority.start.protocol_sha256,
                source_manifest_sha256=authority.start.source_manifest_sha256,
                capture_plan_sha256=authority.start.capture_plan_sha256,
                session_start_sha256=authority.start_sha256,
                session_closure_sha256=authority.closure_sha256,
                external_start_sha256=authority.external_start_sha256,
                external_closure_sha256=authority.external_closure_sha256,
                canonical_session_documents_verified=True,
                external_audit_subject_chain_verified=True,
                full_segment_chain_verified=True,
                clean_closure=(
                    not authority.closure.fatal
                    and authority.closure.stop_reason == "completed_duration"
                ),
            ),
            observation_window=DepthObservationWindowV1(
                started_monotonic_ns=self.start_ns,
                closed_monotonic_ns=self.end_ns,
                observation_duration_ns=elapsed_ns,
                configured_duration_seconds=86_400,
                full_configured_duration_observed=full_duration,
            ),
            storage=DepthStorageBindingV1(
                segment_count=len(manifests),
                record_count=sum(item.record_count for item in manifests),
                compressed_bytes=sum(item.compressed_bytes for item in manifests),
                uncompressed_bytes=sum(item.uncompressed_bytes for item in manifests),
                typed_canonical_records_streamed=True,
                ingest_sequence_verified=True,
                receipt_monotonic_order_verified=True,
            ),
            local_receipt_depth_evidence_age_ms_max=2_000,
            canary_depth_coverage_min_ppm=995_000,
            prospective_diagnostic_min_ppm=999_000,
            books=book_reports,
            replay=_replay_diagnostics(metrics, self.coverage_invalid_record_count),
            scope_boundaries=DepthCoverageScopeBoundariesV1(
                infrastructure_depth_only=True,
                market_payload_interpreted=True,
                future_information_interpreted=False,
                depth_sequence_acceptance_performed=True,
                depth_coverage_acceptance_performed=True,
                local_receipt_depth_freshness_acceptance_performed=True,
                efficacy_acceptance_performed=False,
                thirty_day_holdout_gate_evaluated=False,
                family_b_authorized=False,
                promotion_authorized=False,
            ),
            verdict=verdict,
            verdict_reasons=reasons,
        )


def build_depth_reconstruction_coverage_report(
    *,
    start_path: str | Path,
    closure_path: str | Path,
    capture_directory: str | Path,
) -> CanonicalDepthCoverageReport:
    """Build one deterministic, non-efficacy depth report from closed evidence."""

    authority = verify_closed_capture_authority(
        start_path=start_path,
        closure_path=closure_path,
        capture_directory=capture_directory,
    )
    accumulator = _DepthCoverageAccumulator(
        start_ns=authority.start.started_monotonic_ns,
        end_ns=authority.closure.closed_monotonic_ns,
    )
    consume_closed_capture_records(authority, accumulator.consume)
    report = accumulator.finish(authority)
    document = report.model_dump(mode="json")
    _require_no_forbidden_report_keys(document)
    encoded = canonical_json_bytes(document)
    return CanonicalDepthCoverageReport(
        report=report,
        canonical_bytes=encoded,
        sha256=hashlib.sha256(encoded).hexdigest(),
    )


def _coverage_category(
    state: LocalBookCoverageState,
    receipt_ns: int,
) -> _CoverageCategory:
    if state.status is not LocalBookStatus.VALID:
        return "sequence_unavailable"
    availability_ns = state.availability_receipt_monotonic_ns
    if availability_ns is None or receipt_ns > availability_ns + _QUOTE_AGE_NS_MAX:
        return "stale_sequence_valid"
    if not state.has_bid or not state.has_ask:
        return "one_sided_or_empty"
    if state.crossed_or_locked:
        return "crossed_or_locked"
    return "two_sided_uncrossed"


def _record_receipt_monotonic_ns(record: CaptureRecord) -> int:
    if isinstance(record, RestEnvelopeV1):
        return record.response_received_monotonic_ns
    if isinstance(record, RestEnvelopeV2):
        return record.response_completed_monotonic_ns
    return record.received_monotonic_ns


def _ppm(numerator: int, denominator: int) -> int:
    if denominator < 0 or numerator < 0 or numerator > denominator:
        raise ValueError("coverage ppm inputs are invalid")
    if denominator == 0:
        return 0
    return numerator * 1_000_000 // denominator


def _per_book_diagnostics(
    metrics: LocalBookPerBookMetrics,
) -> PerBookReplayDiagnosticsV1:
    return PerBookReplayDiagnosticsV1(
        depth_transitions=metrics.depth_transitions,
        connection_resets=metrics.connection_resets,
        disconnects=metrics.disconnects,
        depth_events_received=metrics.depth_events_received,
        depth_events_buffered=metrics.depth_events_buffered,
        depth_events_applied=metrics.depth_events_applied,
        old_events_ignored=metrics.old_events_ignored,
        snapshots_received=metrics.snapshots_received,
        snapshot_candidates_held=metrics.snapshot_candidates_held,
        snapshots_applied=metrics.snapshots_applied,
        snapshot_request_failures=metrics.snapshot_request_failures,
        stale_snapshots_rejected=metrics.stale_snapshots_rejected,
        redundant_snapshots_ignored=metrics.redundant_snapshots_ignored,
        bridge_failures=metrics.bridge_failures,
        sequence_gaps=metrics.sequence_gaps,
        malformed_records=metrics.malformed_records,
        buffer_overflows=metrics.buffer_overflows,
        level_overflows=metrics.level_overflows,
        stale_connection_records=metrics.stale_connection_records,
        books_became_valid=metrics.books_became_valid,
    )


def _replay_diagnostics(
    metrics: LocalBookMetrics,
    coverage_invalid_record_count: int,
) -> ReplayDiagnosticsV1:
    return ReplayDiagnosticsV1(
        records_processed=metrics.records_processed,
        depth_transitions=metrics.depth_transitions,
        connection_resets=metrics.connection_resets,
        disconnects=metrics.disconnects,
        depth_events_received=metrics.depth_events_received,
        depth_events_buffered=metrics.depth_events_buffered,
        depth_events_applied=metrics.depth_events_applied,
        old_events_ignored=metrics.old_events_ignored,
        snapshots_received=metrics.snapshots_received,
        snapshot_candidates_held=metrics.snapshot_candidates_held,
        snapshots_applied=metrics.snapshots_applied,
        snapshot_request_failures=metrics.snapshot_request_failures,
        stale_snapshots_rejected=metrics.stale_snapshots_rejected,
        redundant_snapshots_ignored=metrics.redundant_snapshots_ignored,
        bridge_failures=metrics.bridge_failures,
        sequence_gaps=metrics.sequence_gaps,
        malformed_records=metrics.malformed_records,
        buffer_overflows=metrics.buffer_overflows,
        level_overflows=metrics.level_overflows,
        stale_connection_records=metrics.stale_connection_records,
        books_became_valid=metrics.books_became_valid,
        ingest_gaps=metrics.ingest_gaps,
        coverage_invalid_record_count=coverage_invalid_record_count,
    )


def _verdict(
    *,
    authority: ClosedCaptureAuthority,
    full_duration: bool,
    books: tuple[PerBookDepthCoverageV1, ...],
    metrics: LocalBookMetrics,
    coverage_invalid_record_count: int,
) -> tuple[DepthCoverageVerdict, tuple[DepthCoverageVerdictReason, ...]]:
    hard_failures: list[DepthCoverageVerdictReason] = []
    if authority.closure.fatal:
        hard_failures.append("fatal_session_closure")
    if coverage_invalid_record_count:
        hard_failures.append("coverage_invalid_record_observed")
    if metrics.malformed_records:
        hard_failures.append("malformed_depth_record_observed")
    if metrics.buffer_overflows or metrics.level_overflows:
        hard_failures.append("bounded_replay_overflow_observed")
    if hard_failures:
        return "FAIL", tuple(hard_failures)

    incomplete: list[DepthCoverageVerdictReason] = []
    if not full_duration:
        incomplete.append("configured_duration_not_observed")
    if authority.closure.stop_reason != "completed_duration":
        incomplete.append("closure_not_completed_duration")
    never_valid = any(book.diagnostics.books_became_valid < 1 for book in books)
    if incomplete and never_valid:
        incomplete.append("book_never_became_sequence_valid")
    if incomplete:
        return "INCOMPLETE", tuple(incomplete)

    failures: list[DepthCoverageVerdictReason] = []
    if never_valid:
        failures.append("book_never_became_sequence_valid")
    if any(not book.meets_canary_sequence_valid_requirement for book in books):
        failures.append("sequence_valid_coverage_below_canary_requirement")
    if any(not book.meets_canary_two_sided_uncrossed_requirement for book in books):
        failures.append("fresh_two_sided_uncrossed_coverage_below_canary_requirement")
    if any(book.unresolved_sequence_gap for book in books):
        failures.append("unresolved_sequence_gap_observed")
    if any(book.unresolved_reconstruction for book in books):
        failures.append("unresolved_reconstruction_observed")
    if failures:
        return "FAIL", tuple(failures)
    return (
        "DEPTH_RECONSTRUCTION_COVERAGE_PASS",
        ("depth_reconstruction_coverage_requirements_satisfied",),
    )


def _require_no_forbidden_report_keys(value: object) -> None:
    if isinstance(value, dict):
        forbidden = {
            str(key).casefold()
            for key in value
            if str(key).casefold() in _FORBIDDEN_REPORT_KEYS
        }
        if forbidden:
            raise ValueError(f"depth report contains forbidden keys: {sorted(forbidden)}")
        for item in value.values():
            _require_no_forbidden_report_keys(item)
    elif isinstance(value, list | tuple):
        for item in value:
            _require_no_forbidden_report_keys(item)
