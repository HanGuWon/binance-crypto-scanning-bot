from __future__ import annotations

import hashlib
import json
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
from signalbot.capture.models import (
    CaptureRecord,
    RawPayloadEncoding,
    RestEnvelopeV1,
    RestEnvelopeV2,
    payload_bytes,
)
from signalbot.capture.provenance import canonical_json_bytes
from signalbot.domain.enums import Market

_EXPECTED_DURATION_SECONDS = 86_400
_EXPECTED_DURATION_NS = _EXPECTED_DURATION_SECONDS * 1_000_000_000
CLOCK_SAMPLE_MAX_AGE_MS_V1 = 60_000
CLOCK_HEADER_RTT_MAX_MS_V1 = 2_000
CLOCK_RATE_ERROR_PPM_V1 = 1_000
WALL_MONOTONIC_HEALTHY_MAX_RESIDUAL_MS_V1 = 2

_CLOCK_SAMPLE_MAX_AGE_NS = CLOCK_SAMPLE_MAX_AGE_MS_V1 * 1_000_000
_CLOCK_HEADER_RTT_MAX_NS = CLOCK_HEADER_RTT_MAX_MS_V1 * 1_000_000
_CLOCK_RATE_ERROR_PPM = CLOCK_RATE_ERROR_PPM_V1
_CLOCK_COVERAGE_MIN_PPM = 999_000
_TIMESTAMP_QUANTIZATION_MARGIN_MS = 1
_CONTINUITY_QUANTIZATION_MARGIN_MS = 2
_WALL_MONOTONIC_HEALTHY_MAX_RESIDUAL_NS = (
    WALL_MONOTONIC_HEALTHY_MAX_RESIDUAL_MS_V1 * 1_000_000
)
_WALL_MONOTONIC_FAIL_MIN_RESIDUAL_NS = 100_000_000
_RATE_DENOMINATOR = 1_000_000 * 1_000_000
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_FORBIDDEN_REPORT_KEYS = frozenset(
    {"pnl", "outcome", "return", "label", "signal", "order", "price"}
)

ClockVenue: TypeAlias = Literal["spot", "futures"]  # noqa: UP040
ClockRole: TypeAlias = Literal[  # noqa: UP040
    "spot_venue_time",
    "futures_venue_time",
]
ClockEndpoint: TypeAlias = Literal[  # noqa: UP040
    "/api/v3/time",
    "/fapi/v1/time",
]
ClockHealthVerdict: TypeAlias = Literal[  # noqa: UP040
    "CLOCK_HEALTH_PASS",
    "INCOMPLETE",
    "FAIL",
]
ClockHealthVerdictReason: TypeAlias = Literal[  # noqa: UP040
    "clock_health_requirements_satisfied",
    "fatal_session_closure",
    "monotonic_regression_observed",
    "wall_clock_regression_observed",
    "wall_clock_discontinuity_observed",
    "time_sample_contract_mismatch_observed",
    "venue_clock_continuity_failure_observed",
    "configured_duration_not_observed",
    "closure_not_completed_duration",
    "wall_clock_mapping_inconclusive_observed",
    "spot_clock_never_valid",
    "futures_clock_never_valid",
    "spot_clock_coverage_below_requirement",
    "futures_clock_coverage_below_requirement",
]
ClockCutoffVerdict: TypeAlias = Literal[  # noqa: UP040
    "ADMISSIBLE",
    "LATE",
    "CLOCK_INCONCLUSIVE",
]
ClockCutoffReason: TypeAlias = Literal[  # noqa: UP040
    "receipt_interval_at_or_before_cutoff",
    "receipt_interval_after_cutoff",
    "receipt_interval_straddles_cutoff",
    "clock_sample_missing",
    "clock_sample_not_yet_available",
    "clock_sample_stale",
]
ClockStopReason: TypeAlias = Literal[  # noqa: UP040
    "completed_duration",
    "operator_requested",
    "capture_failure",
    "capacity_exhausted",
    "clock_discontinuity",
    "network_retry_exhausted",
]


class _StrictClockModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)


class ClockAuthorityMetricsV1(_StrictClockModel):
    session_id: str
    protocol_sha256: str
    source_manifest_sha256: str
    capture_plan_sha256: str
    session_start_sha256: str
    session_closure_sha256: str
    external_start_sha256: str
    external_closure_sha256: str
    closure_stop_reason: ClockStopReason
    closure_fatal: bool
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
        if _SHA256_RE.fullmatch(value) is None:
            raise ValueError("clock report authority hashes must be lowercase SHA-256")
        return value

    @model_validator(mode="after")
    def require_clean_closure_consistency(self) -> ClockAuthorityMetricsV1:
        expected = not self.closure_fatal and self.closure_stop_reason == "completed_duration"
        if self.clean_closure != expected:
            raise ValueError("clock authority clean-closure flag is inconsistent")
        return self


class ClockObservationWindowV1(_StrictClockModel):
    started_at_ms: int = Field(ge=0)
    closed_at_ms: int = Field(ge=0)
    started_monotonic_ns: int = Field(ge=0)
    closed_monotonic_ns: int = Field(ge=0)
    observation_duration_ns: int = Field(ge=0)
    configured_duration_seconds: Literal[86_400]
    full_configured_duration_observed: bool

    @model_validator(mode="after")
    def require_coherent_window(self) -> ClockObservationWindowV1:
        if self.closed_at_ms < self.started_at_ms:
            raise ValueError("clock observation wall-time window is reversed")
        if self.closed_monotonic_ns < self.started_monotonic_ns:
            raise ValueError("clock observation monotonic window is reversed")
        expected = self.closed_monotonic_ns - self.started_monotonic_ns
        if self.observation_duration_ns != expected:
            raise ValueError("clock observation duration differs from its endpoints")
        if self.full_configured_duration_observed != (expected >= _EXPECTED_DURATION_NS):
            raise ValueError("clock full-duration flag differs from its window")
        return self


class ClockStorageBindingV1(_StrictClockModel):
    segment_count: int = Field(ge=0)
    record_count: int = Field(ge=0)
    compressed_bytes: int = Field(ge=0)
    uncompressed_bytes: int = Field(ge=0)
    typed_canonical_records_streamed: Literal[True]
    ingest_sequence_verified: Literal[True]


class VenueClockSampleV1(_StrictClockModel):
    schema_version: Literal["venue_clock_sample_v1"]
    market: ClockVenue
    request_role: ClockRole
    source_ingest_seq: int = Field(ge=1)
    request_started_at_ms: int = Field(ge=0)
    request_started_monotonic_ns: int = Field(ge=0)
    response_first_byte_at_ms: int = Field(ge=0)
    response_first_byte_monotonic_ns: int = Field(ge=0)
    response_completed_at_ms: int = Field(ge=0)
    response_completed_monotonic_ns: int = Field(ge=0)
    server_time_ms: int = Field(ge=0)
    header_rtt_ns: int = Field(ge=0, le=_CLOCK_HEADER_RTT_MAX_NS)
    wall_header_elapsed_ms: int = Field(ge=0)
    header_wall_monotonic_residual_ns: int = Field(
        ge=0,
        le=_WALL_MONOTONIC_HEALTHY_MAX_RESIDUAL_NS,
    )
    completion_wall_elapsed_ms: int = Field(ge=0)
    completion_wall_monotonic_residual_ns: int = Field(
        ge=0,
        le=_WALL_MONOTONIC_HEALTHY_MAX_RESIDUAL_NS,
    )
    offset_lower_ms: int
    offset_upper_ms: int
    available_at_monotonic_ns: int = Field(ge=0)

    @model_validator(mode="after")
    def require_exact_sample_contract(self) -> VenueClockSampleV1:
        expected_role = (
            "spot_venue_time" if self.market == "spot" else "futures_venue_time"
        )
        if self.request_role != expected_role:
            raise ValueError("venue clock role differs from its market")
        if not (
            self.request_started_monotonic_ns
            <= self.response_first_byte_monotonic_ns
            <= self.response_completed_monotonic_ns
        ):
            raise ValueError("venue clock monotonic timestamps are reversed")
        expected_rtt = (
            self.response_first_byte_monotonic_ns
            - self.request_started_monotonic_ns
        )
        if self.header_rtt_ns != expected_rtt:
            raise ValueError("venue clock RTT differs from its monotonic timestamps")
        expected_wall_elapsed = (
            self.response_first_byte_at_ms - self.request_started_at_ms
        )
        if self.wall_header_elapsed_ms != expected_wall_elapsed:
            raise ValueError("venue clock wall RTT differs from its wall timestamps")
        expected_header_residual = abs(
            expected_wall_elapsed * 1_000_000 - expected_rtt
        )
        if self.header_wall_monotonic_residual_ns != expected_header_residual:
            raise ValueError("venue clock header wall/monotonic residual is inconsistent")
        expected_completion_wall = (
            self.response_completed_at_ms - self.response_first_byte_at_ms
        )
        if self.completion_wall_elapsed_ms != expected_completion_wall:
            raise ValueError("venue clock completion wall duration is inconsistent")
        completion_monotonic_ns = (
            self.response_completed_monotonic_ns
            - self.response_first_byte_monotonic_ns
        )
        expected_completion_residual = abs(
            expected_completion_wall * 1_000_000 - completion_monotonic_ns
        )
        if (
            self.completion_wall_monotonic_residual_ns
            != expected_completion_residual
        ):
            raise ValueError(
                "venue clock completion wall/monotonic residual is inconsistent"
            )
        expected_lower = (
            self.server_time_ms
            - self.response_first_byte_at_ms
            - _TIMESTAMP_QUANTIZATION_MARGIN_MS
        )
        expected_upper = (
            self.server_time_ms
            - self.request_started_at_ms
            + _TIMESTAMP_QUANTIZATION_MARGIN_MS
        )
        if (self.offset_lower_ms, self.offset_upper_ms) != (
            expected_lower,
            expected_upper,
        ):
            raise ValueError("venue clock offset interval is inconsistent")
        if self.offset_lower_ms > self.offset_upper_ms:
            raise ValueError("venue clock offset interval is reversed")
        if self.available_at_monotonic_ns != self.response_completed_monotonic_ns:
            raise ValueError("venue clock availability must be response completion")
        return self


class GlobalClockDiagnosticsV1(_StrictClockModel):
    observation_count: int = Field(ge=2)
    interval_count: int = Field(ge=1)
    healthy_interval_count: int = Field(ge=0)
    inconclusive_interval_count: int = Field(ge=0)
    discontinuity_interval_count: int = Field(ge=0)
    monotonic_regression_count: int = Field(ge=0)
    wall_clock_regression_count: int = Field(ge=0)
    maximum_absolute_wall_monotonic_residual_ns: int = Field(ge=0)

    @model_validator(mode="after")
    def require_interval_partition(self) -> GlobalClockDiagnosticsV1:
        if self.interval_count != self.observation_count - 1:
            raise ValueError("global clock interval count is inconsistent")
        categorized = (
            self.healthy_interval_count
            + self.inconclusive_interval_count
            + self.discontinuity_interval_count
            + self.monotonic_regression_count
            + self.wall_clock_regression_count
        )
        if categorized != self.interval_count:
            raise ValueError("global clock interval categories do not partition observations")
        return self


class PerVenueClockHealthV1(_StrictClockModel):
    market: ClockVenue
    request_role: ClockRole
    endpoint_path: ClockEndpoint
    time_record_count: int = Field(ge=0)
    valid_sample_count: int = Field(ge=0)
    unusable_sample_count: int = Field(ge=0)
    contract_mismatch_count: int = Field(ge=0)
    unsuccessful_response_count: int = Field(ge=0)
    malformed_payload_count: int = Field(ge=0)
    missing_first_byte_count: int = Field(ge=0)
    header_rtt_exceeded_count: int = Field(ge=0)
    sample_wall_clock_regression_count: int = Field(ge=0)
    sample_wall_monotonic_inconclusive_count: int = Field(ge=0)
    sample_wall_clock_discontinuity_count: int = Field(ge=0)
    continuity_failure_count: int = Field(ge=0)
    maximum_header_rtt_ns: int = Field(ge=0)
    maximum_sample_wall_monotonic_residual_ns: int = Field(ge=0)
    minimum_offset_lower_ms: int | None = None
    maximum_offset_upper_ms: int | None = None
    first_valid_sample: VenueClockSampleV1 | None = None
    last_valid_sample: VenueClockSampleV1 | None = None
    observation_duration_ns: int = Field(ge=0)
    valid_coverage_duration_ns: int = Field(ge=0)
    valid_coverage_ppm: int = Field(ge=0, le=1_000_000)
    meets_clock_coverage_requirement: bool
    longest_uncovered_interval_ns: int = Field(ge=0)

    @model_validator(mode="after")
    def require_coherent_venue(self) -> PerVenueClockHealthV1:
        expected = {
            "spot": ("spot_venue_time", "/api/v3/time"),
            "futures": ("futures_venue_time", "/fapi/v1/time"),
        }[self.market]
        if (self.request_role, self.endpoint_path) != expected:
            raise ValueError("venue clock identity differs from its market")
        if self.valid_sample_count + self.unusable_sample_count != self.time_record_count:
            raise ValueError("venue clock sample counts do not partition time records")
        unusable_reason_count = sum(
            (
                self.contract_mismatch_count,
                self.unsuccessful_response_count,
                self.malformed_payload_count,
                self.missing_first_byte_count,
                self.header_rtt_exceeded_count,
                self.sample_wall_clock_regression_count,
                self.sample_wall_monotonic_inconclusive_count,
                self.sample_wall_clock_discontinuity_count,
            )
        )
        if unusable_reason_count != self.unusable_sample_count:
            raise ValueError(
                "venue clock unusable reasons do not partition unusable samples"
            )
        optional_values = (
            self.minimum_offset_lower_ms,
            self.maximum_offset_upper_ms,
            self.first_valid_sample,
            self.last_valid_sample,
        )
        if self.valid_sample_count == 0:
            if any(item is not None for item in optional_values):
                raise ValueError("an empty venue clock cannot expose valid-sample bounds")
        elif any(item is None for item in optional_values):
            raise ValueError("a non-empty venue clock must expose valid-sample bounds")
        if self.first_valid_sample is not None and self.last_valid_sample is not None:
            for sample in (self.first_valid_sample, self.last_valid_sample):
                if sample.market != self.market or sample.request_role != self.request_role:
                    raise ValueError("venue report contains a sample from another clock")
            if (
                self.first_valid_sample.source_ingest_seq
                > self.last_valid_sample.source_ingest_seq
                or self.first_valid_sample.available_at_monotonic_ns
                > self.last_valid_sample.available_at_monotonic_ns
            ):
                raise ValueError("venue clock first/last samples are reversed")
            if self.valid_sample_count == 1 and (
                self.first_valid_sample != self.last_valid_sample
            ):
                raise ValueError("a single-sample venue clock must repeat one endpoint")
            if self.valid_sample_count > 1 and (
                self.first_valid_sample.source_ingest_seq
                == self.last_valid_sample.source_ingest_seq
            ):
                raise ValueError("a multi-sample venue clock must expose distinct endpoints")
            if self.maximum_header_rtt_ns < max(
                self.first_valid_sample.header_rtt_ns,
                self.last_valid_sample.header_rtt_ns,
            ):
                raise ValueError("venue clock maximum RTT excludes an endpoint sample")
            if self.maximum_sample_wall_monotonic_residual_ns < max(
                self.first_valid_sample.header_wall_monotonic_residual_ns,
                self.first_valid_sample.completion_wall_monotonic_residual_ns,
                self.last_valid_sample.header_wall_monotonic_residual_ns,
                self.last_valid_sample.completion_wall_monotonic_residual_ns,
            ):
                raise ValueError("venue clock maximum residual excludes an endpoint sample")
        if (
            self.minimum_offset_lower_ms is not None
            and self.maximum_offset_upper_ms is not None
        ):
            endpoint_samples = (self.first_valid_sample, self.last_valid_sample)
            if self.minimum_offset_lower_ms > self.maximum_offset_upper_ms:
                raise ValueError("venue clock aggregate offset interval is reversed")
            if any(
                sample is not None
                and (
                    self.minimum_offset_lower_ms > sample.offset_lower_ms
                    or self.maximum_offset_upper_ms < sample.offset_upper_ms
                )
                for sample in endpoint_samples
            ):
                raise ValueError("venue clock aggregate offsets exclude an endpoint sample")
        if self.continuity_failure_count > max(0, self.valid_sample_count - 1):
            raise ValueError("venue clock continuity failures exceed adjacent sample pairs")
        expected_ppm = _ppm(
            self.valid_coverage_duration_ns,
            self.observation_duration_ns,
        )
        if self.valid_coverage_ppm != expected_ppm:
            raise ValueError("venue clock coverage ppm is inconsistent")
        maximum_coverage_ns = min(
            self.observation_duration_ns,
            self.valid_sample_count * _CLOCK_SAMPLE_MAX_AGE_NS,
        )
        if self.valid_coverage_duration_ns > maximum_coverage_ns:
            raise ValueError("venue clock coverage exceeds its sample-count bound")
        uncovered_ns = (
            self.observation_duration_ns - self.valid_coverage_duration_ns
        )
        if self.longest_uncovered_interval_ns > uncovered_ns:
            raise ValueError("venue clock longest gap exceeds total uncovered time")
        if (self.longest_uncovered_interval_ns == 0) != (uncovered_ns == 0):
            raise ValueError("venue clock longest gap contradicts uncovered time")
        if (
            uncovered_ns > 0
            and self.longest_uncovered_interval_ns * (self.valid_sample_count + 1)
            < uncovered_ns
        ):
            raise ValueError("venue clock longest gap is too small for its sample count")
        if self.valid_sample_count == 0 and (
            self.valid_coverage_duration_ns != 0
            or self.longest_uncovered_interval_ns != self.observation_duration_ns
        ):
            raise ValueError("an empty venue clock must leave the full window uncovered")
        expected_flag = expected_ppm >= _CLOCK_COVERAGE_MIN_PPM
        if self.meets_clock_coverage_requirement != expected_flag:
            raise ValueError("venue clock coverage flag is inconsistent")
        return self


class ClockScopeBoundariesV1(_StrictClockModel):
    infrastructure_clock_only: Literal[True]
    venue_time_payload_interpreted: Literal[True]
    future_market_information_interpreted: Literal[False]
    future_clock_interpolation_performed: Literal[False]
    causal_cutoff_mapper_exposed: Literal[True]
    report_runtime_attested_to_capture_source_manifest: Literal[False]
    alert_runtime_integrated: Literal[False]
    paper_runtime_integrated: Literal[False]
    efficacy_acceptance_performed: Literal[False]
    thirty_day_holdout_gate_evaluated: Literal[False]
    promotion_authorized: Literal[False]


class ClockHealthReportV1(_StrictClockModel):
    schema_version: Literal["clock_health_report_v1"]
    purpose: Literal["infrastructure_clock_health_only"]
    authority: ClockAuthorityMetricsV1
    observation_window: ClockObservationWindowV1
    storage: ClockStorageBindingV1
    venue_time_poll_interval_seconds: Literal[30]
    clock_sample_max_age_ms: Literal[60_000]
    maximum_header_rtt_ms: Literal[2_000]
    maximum_rate_error_ppm: Literal[1_000]
    timestamp_quantization_margin_ms: Literal[1]
    wall_monotonic_healthy_max_residual_ms: Literal[2]
    wall_monotonic_fail_min_residual_ms: Literal[100]
    clock_coverage_min_ppm: Literal[999_000]
    global_clock: GlobalClockDiagnosticsV1
    venues: tuple[PerVenueClockHealthV1, PerVenueClockHealthV1]
    scope_boundaries: ClockScopeBoundariesV1
    verdict: ClockHealthVerdict
    verdict_reasons: tuple[ClockHealthVerdictReason, ...]

    @model_validator(mode="after")
    def require_coherent_report(self) -> ClockHealthReportV1:
        if tuple(item.market for item in self.venues) != ("spot", "futures"):
            raise ValueError("clock report must contain Spot then Futures")
        if self.storage.record_count != self.global_clock.observation_count - 2:
            raise ValueError("clock report record count differs from streamed observations")
        if sum(venue.time_record_count for venue in self.venues) > self.storage.record_count:
            raise ValueError("venue clock records exceed total streamed records")
        if any(
            sample is not None and sample.source_ingest_seq > self.storage.record_count
            for venue in self.venues
            for sample in (venue.first_valid_sample, venue.last_valid_sample)
        ):
            raise ValueError("venue clock endpoint ingest sequence escapes stored records")
        if any(
            venue.observation_duration_ns
            != self.observation_window.observation_duration_ns
            for venue in self.venues
        ):
            raise ValueError("venue clock windows differ from session authority")
        if not self.verdict_reasons:
            raise ValueError("clock report verdict requires at least one reason")
        expected_verdict, expected_reasons = _verdict_from_fields(
            closure_fatal=self.authority.closure_fatal,
            closure_stop_reason=self.authority.closure_stop_reason,
            full_duration=self.observation_window.full_configured_duration_observed,
            global_clock=self.global_clock,
            venues=self.venues,
        )
        if self.verdict != expected_verdict or self.verdict_reasons != expected_reasons:
            raise ValueError("clock verdict or reasons contradict report evidence")
        return self


class ClockCutoffAssessmentV1(_StrictClockModel):
    verdict: ClockCutoffVerdict
    reason: ClockCutoffReason
    cutoff_ms: int = Field(ge=0)
    receipt_monotonic_ns: int = Field(ge=0)
    source_ingest_seq: int | None = Field(default=None, ge=1)
    venue_time_lower_ms: int | None = None
    venue_time_upper_ms: int | None = None

    @model_validator(mode="after")
    def require_coherent_assessment(self) -> ClockCutoffAssessmentV1:
        paired = (self.venue_time_lower_ms is None) == (
            self.venue_time_upper_ms is None
        )
        if not paired:
            raise ValueError("clock cutoff interval endpoints must be paired")
        if self.venue_time_lower_ms is None:
            if self.verdict != "CLOCK_INCONCLUSIVE":
                raise ValueError("an interval-free assessment must be inconclusive")
            if self.reason == "clock_sample_missing":
                if self.source_ingest_seq is not None:
                    raise ValueError("a missing-sample assessment cannot bind a sample")
            elif self.source_ingest_seq is None:
                raise ValueError("a rejected clock sample must remain source-bound")
            return self
        assert self.venue_time_upper_ms is not None
        if self.source_ingest_seq is None:
            raise ValueError("a clock cutoff interval must bind its source sample")
        if self.venue_time_lower_ms > self.venue_time_upper_ms:
            raise ValueError("clock cutoff interval is reversed")
        if self.verdict == "ADMISSIBLE":
            if (
                self.reason != "receipt_interval_at_or_before_cutoff"
                or self.venue_time_upper_ms > self.cutoff_ms
            ):
                raise ValueError("ADMISSIBLE contradicts the clock interval")
        elif self.verdict == "LATE":
            if (
                self.reason != "receipt_interval_after_cutoff"
                or self.venue_time_lower_ms <= self.cutoff_ms
            ):
                raise ValueError("LATE contradicts the clock interval")
        elif (
            self.reason != "receipt_interval_straddles_cutoff"
            or not self.venue_time_lower_ms
            <= self.cutoff_ms
            < self.venue_time_upper_ms
        ):
            raise ValueError("CLOCK_INCONCLUSIVE contradicts the clock interval")
        return self


@dataclass(frozen=True, slots=True)
class CanonicalClockHealthReport:
    report: ClockHealthReportV1
    canonical_bytes: bytes
    sha256: str


@dataclass(slots=True)
class _CoverageUnion:
    start_ns: int
    end_ns: int
    interval_start_ns: int | None = None
    interval_end_ns: int | None = None
    covered_ns: int = 0
    longest_uncovered_ns: int = 0

    def add(self, availability_ns: int) -> None:
        start = min(max(availability_ns, self.start_ns), self.end_ns)
        end = min(max(availability_ns + _CLOCK_SAMPLE_MAX_AGE_NS, self.start_ns), self.end_ns)
        if end <= start:
            return
        if self.interval_start_ns is None or self.interval_end_ns is None:
            self.longest_uncovered_ns = max(
                self.longest_uncovered_ns,
                start - self.start_ns,
            )
            self.interval_start_ns = start
            self.interval_end_ns = end
            return
        if start > self.interval_end_ns:
            self.covered_ns += self.interval_end_ns - self.interval_start_ns
            self.longest_uncovered_ns = max(
                self.longest_uncovered_ns,
                start - self.interval_end_ns,
            )
            self.interval_start_ns = start
            self.interval_end_ns = end
            return
        self.interval_end_ns = max(self.interval_end_ns, end)

    def finish(self) -> tuple[int, int]:
        if self.interval_start_ns is None or self.interval_end_ns is None:
            return 0, self.end_ns - self.start_ns
        covered = self.covered_ns + self.interval_end_ns - self.interval_start_ns
        longest = max(
            self.longest_uncovered_ns,
            self.end_ns - self.interval_end_ns,
        )
        return covered, longest


class _GlobalClockAccumulator:
    def __init__(self, *, wall_ms: int, monotonic_ns: int) -> None:
        self.previous_wall_ms = wall_ms
        self.previous_monotonic_ns = monotonic_ns
        self.observation_count = 1
        self.healthy_count = 0
        self.inconclusive_count = 0
        self.discontinuity_count = 0
        self.monotonic_regression_count = 0
        self.wall_regression_count = 0
        self.maximum_residual_ns = 0

    def observe(self, wall_ms: int, monotonic_ns: int) -> None:
        self.observation_count += 1
        wall_delta_ms = wall_ms - self.previous_wall_ms
        monotonic_delta_ns = monotonic_ns - self.previous_monotonic_ns
        residual_ns = abs(wall_delta_ms * 1_000_000 - monotonic_delta_ns)
        self.maximum_residual_ns = max(self.maximum_residual_ns, residual_ns)
        if monotonic_delta_ns < 0:
            self.monotonic_regression_count += 1
        elif wall_delta_ms < 0:
            self.wall_regression_count += 1
        elif residual_ns >= _WALL_MONOTONIC_FAIL_MIN_RESIDUAL_NS:
            self.discontinuity_count += 1
        elif residual_ns > _WALL_MONOTONIC_HEALTHY_MAX_RESIDUAL_NS:
            self.inconclusive_count += 1
        else:
            self.healthy_count += 1
        self.previous_wall_ms = wall_ms
        self.previous_monotonic_ns = monotonic_ns

    def metrics(self) -> GlobalClockDiagnosticsV1:
        return GlobalClockDiagnosticsV1(
            observation_count=self.observation_count,
            interval_count=self.observation_count - 1,
            healthy_interval_count=self.healthy_count,
            inconclusive_interval_count=self.inconclusive_count,
            discontinuity_interval_count=self.discontinuity_count,
            monotonic_regression_count=self.monotonic_regression_count,
            wall_clock_regression_count=self.wall_regression_count,
            maximum_absolute_wall_monotonic_residual_ns=self.maximum_residual_ns,
        )


class _VenueClockAccumulator:
    def __init__(self, *, market: Market, start_ns: int, end_ns: int) -> None:
        self.market = market
        self.start_ns = start_ns
        self.end_ns = end_ns
        self.request_role: ClockRole
        self.endpoint_path: ClockEndpoint
        self.request_role, self.endpoint_path = _clock_identity(market)
        self.coverage = _CoverageUnion(start_ns=start_ns, end_ns=end_ns)
        self.time_record_count = 0
        self.valid_sample_count = 0
        self.unusable_sample_count = 0
        self.contract_mismatch_count = 0
        self.unsuccessful_response_count = 0
        self.malformed_payload_count = 0
        self.missing_first_byte_count = 0
        self.header_rtt_exceeded_count = 0
        self.sample_wall_clock_regression_count = 0
        self.sample_wall_monotonic_inconclusive_count = 0
        self.sample_wall_clock_discontinuity_count = 0
        self.continuity_failure_count = 0
        self.maximum_header_rtt_ns = 0
        self.maximum_sample_residual_ns = 0
        self.minimum_offset_lower_ms: int | None = None
        self.maximum_offset_upper_ms: int | None = None
        self.first_valid_sample: VenueClockSampleV1 | None = None
        self.last_valid_sample: VenueClockSampleV1 | None = None

    def consume_legacy(self, record: RestEnvelopeV1) -> None:
        """Count legacy `/time` evidence without treating completion as headers."""

        self.time_record_count += 1
        self.unusable_sample_count += 1
        self.missing_first_byte_count += 1
        if record.market is not self.market or record.endpoint_path != self.endpoint_path:
            self.contract_mismatch_count += 1

    def consume(self, record: RestEnvelopeV2) -> None:
        self.time_record_count += 1
        if (
            record.market is not self.market
            or record.request_role != self.request_role
            or record.endpoint_path != self.endpoint_path
            or record.canonical_query
            or record.request_started_monotonic_ns < self.start_ns
            or record.response_completed_monotonic_ns > self.end_ns
        ):
            self.contract_mismatch_count += 1
            self.unusable_sample_count += 1
            return
        if (
            record.response_status != 200
            or not record.payload_complete
            or record.error_category is not None
        ):
            self.unsuccessful_response_count += 1
            self.unusable_sample_count += 1
            return
        first_wall = record.response_first_byte_at_ms
        first_monotonic = record.response_first_byte_monotonic_ns
        if first_wall is None or first_monotonic is None:
            self.missing_first_byte_count += 1
            self.unusable_sample_count += 1
            return
        wall_elapsed_ms = first_wall - record.request_started_at_ms
        header_rtt_ns = first_monotonic - record.request_started_monotonic_ns
        completion_wall_elapsed_ms = record.response_completed_at_ms - first_wall
        completion_monotonic_ns = (
            record.response_completed_monotonic_ns - first_monotonic
        )
        if wall_elapsed_ms < 0 or completion_wall_elapsed_ms < 0:
            self.sample_wall_clock_regression_count += 1
            self.unusable_sample_count += 1
            return
        header_residual_ns = abs(
            wall_elapsed_ms * 1_000_000 - header_rtt_ns
        )
        completion_residual_ns = abs(
            completion_wall_elapsed_ms * 1_000_000 - completion_monotonic_ns
        )
        residual_ns = max(header_residual_ns, completion_residual_ns)
        self.maximum_header_rtt_ns = max(self.maximum_header_rtt_ns, header_rtt_ns)
        self.maximum_sample_residual_ns = max(
            self.maximum_sample_residual_ns,
            residual_ns,
        )
        if residual_ns >= _WALL_MONOTONIC_FAIL_MIN_RESIDUAL_NS:
            self.sample_wall_clock_discontinuity_count += 1
            self.unusable_sample_count += 1
            return
        if residual_ns > _WALL_MONOTONIC_HEALTHY_MAX_RESIDUAL_NS:
            self.sample_wall_monotonic_inconclusive_count += 1
            self.unusable_sample_count += 1
            return
        if header_rtt_ns > _CLOCK_HEADER_RTT_MAX_NS:
            self.header_rtt_exceeded_count += 1
            self.unusable_sample_count += 1
            return
        server_time_ms = _parse_server_time(record.raw_payload, record.raw_payload_encoding)
        if server_time_ms is None:
            self.malformed_payload_count += 1
            self.unusable_sample_count += 1
            return
        sample = VenueClockSampleV1(
            schema_version="venue_clock_sample_v1",
            market=self.market.value,
            request_role=self.request_role,
            source_ingest_seq=record.ingest_seq,
            request_started_at_ms=record.request_started_at_ms,
            request_started_monotonic_ns=record.request_started_monotonic_ns,
            response_first_byte_at_ms=first_wall,
            response_first_byte_monotonic_ns=first_monotonic,
            response_completed_at_ms=record.response_completed_at_ms,
            response_completed_monotonic_ns=record.response_completed_monotonic_ns,
            server_time_ms=server_time_ms,
            header_rtt_ns=header_rtt_ns,
            wall_header_elapsed_ms=wall_elapsed_ms,
            header_wall_monotonic_residual_ns=header_residual_ns,
            completion_wall_elapsed_ms=completion_wall_elapsed_ms,
            completion_wall_monotonic_residual_ns=completion_residual_ns,
            offset_lower_ms=(
                server_time_ms - first_wall - _TIMESTAMP_QUANTIZATION_MARGIN_MS
            ),
            offset_upper_ms=(
                server_time_ms
                - record.request_started_at_ms
                + _TIMESTAMP_QUANTIZATION_MARGIN_MS
            ),
            available_at_monotonic_ns=record.response_completed_monotonic_ns,
        )
        if self.last_valid_sample is not None and not clock_samples_rate_continuous(
            self.last_valid_sample,
            sample,
        ):
            self.continuity_failure_count += 1
        self.valid_sample_count += 1
        self.coverage.add(sample.available_at_monotonic_ns)
        if self.first_valid_sample is None:
            self.first_valid_sample = sample
        self.last_valid_sample = sample
        self.minimum_offset_lower_ms = (
            sample.offset_lower_ms
            if self.minimum_offset_lower_ms is None
            else min(self.minimum_offset_lower_ms, sample.offset_lower_ms)
        )
        self.maximum_offset_upper_ms = (
            sample.offset_upper_ms
            if self.maximum_offset_upper_ms is None
            else max(self.maximum_offset_upper_ms, sample.offset_upper_ms)
        )

    def finish(self, observation_duration_ns: int) -> PerVenueClockHealthV1:
        covered_ns, longest_uncovered_ns = self.coverage.finish()
        coverage_ppm = _ppm(covered_ns, observation_duration_ns)
        return PerVenueClockHealthV1(
            market=self.market.value,
            request_role=self.request_role,
            endpoint_path=self.endpoint_path,
            time_record_count=self.time_record_count,
            valid_sample_count=self.valid_sample_count,
            unusable_sample_count=self.unusable_sample_count,
            contract_mismatch_count=self.contract_mismatch_count,
            unsuccessful_response_count=self.unsuccessful_response_count,
            malformed_payload_count=self.malformed_payload_count,
            missing_first_byte_count=self.missing_first_byte_count,
            header_rtt_exceeded_count=self.header_rtt_exceeded_count,
            sample_wall_clock_regression_count=(
                self.sample_wall_clock_regression_count
            ),
            sample_wall_monotonic_inconclusive_count=(
                self.sample_wall_monotonic_inconclusive_count
            ),
            sample_wall_clock_discontinuity_count=(
                self.sample_wall_clock_discontinuity_count
            ),
            continuity_failure_count=self.continuity_failure_count,
            maximum_header_rtt_ns=self.maximum_header_rtt_ns,
            maximum_sample_wall_monotonic_residual_ns=(
                self.maximum_sample_residual_ns
            ),
            minimum_offset_lower_ms=self.minimum_offset_lower_ms,
            maximum_offset_upper_ms=self.maximum_offset_upper_ms,
            first_valid_sample=self.first_valid_sample,
            last_valid_sample=self.last_valid_sample,
            observation_duration_ns=observation_duration_ns,
            valid_coverage_duration_ns=covered_ns,
            valid_coverage_ppm=coverage_ppm,
            meets_clock_coverage_requirement=(
                coverage_ppm >= _CLOCK_COVERAGE_MIN_PPM
            ),
            longest_uncovered_interval_ns=longest_uncovered_ns,
        )


class _ClockHealthAccumulator:
    def __init__(self, authority: ClosedCaptureAuthority) -> None:
        self.authority = authority
        self.start_ns = authority.start.started_monotonic_ns
        self.end_ns = authority.closure.closed_monotonic_ns
        if self.end_ns < self.start_ns:
            raise ValueError("clock session closure precedes its start")
        self.global_clock = _GlobalClockAccumulator(
            wall_ms=authority.start.started_at_ms,
            monotonic_ns=self.start_ns,
        )
        self.venues = {
            market: _VenueClockAccumulator(
                market=market,
                start_ns=self.start_ns,
                end_ns=self.end_ns,
            )
            for market in (Market.SPOT, Market.FUTURES)
        }

    def consume(self, record: CaptureRecord) -> None:
        wall_ms, monotonic_ns = _record_receipt(record)
        if monotonic_ns < self.start_ns or monotonic_ns > self.end_ns:
            raise CaptureIntegrityError(
                "clock report receipt monotonic time escapes the session"
            )
        self.global_clock.observe(wall_ms, monotonic_ns)
        market = _market_for_clock_record(record)
        if market is not None and isinstance(record, RestEnvelopeV2):
            self.venues[market].consume(record)
        elif market is not None and isinstance(record, RestEnvelopeV1):
            self.venues[market].consume_legacy(record)

    def finish(self) -> ClockHealthReportV1:
        self.global_clock.observe(
            self.authority.closure.closed_at_ms,
            self.authority.closure.closed_monotonic_ns,
        )
        duration_ns = self.end_ns - self.start_ns
        full_duration = duration_ns >= _EXPECTED_DURATION_NS
        venue_reports = (
            self.venues[Market.SPOT].finish(duration_ns),
            self.venues[Market.FUTURES].finish(duration_ns),
        )
        global_metrics = self.global_clock.metrics()
        verdict, reasons = _verdict_from_fields(
            closure_fatal=self.authority.closure.fatal,
            closure_stop_reason=self.authority.closure.stop_reason,
            full_duration=full_duration,
            global_clock=global_metrics,
            venues=venue_reports,
        )
        manifests = self.authority.manifests
        return ClockHealthReportV1(
            schema_version="clock_health_report_v1",
            purpose="infrastructure_clock_health_only",
            authority=ClockAuthorityMetricsV1(
                session_id=self.authority.start.session_id,
                protocol_sha256=self.authority.start.protocol_sha256,
                source_manifest_sha256=self.authority.start.source_manifest_sha256,
                capture_plan_sha256=self.authority.start.capture_plan_sha256,
                session_start_sha256=self.authority.start_sha256,
                session_closure_sha256=self.authority.closure_sha256,
                external_start_sha256=self.authority.external_start_sha256,
                external_closure_sha256=self.authority.external_closure_sha256,
                closure_stop_reason=self.authority.closure.stop_reason,
                closure_fatal=self.authority.closure.fatal,
                canonical_session_documents_verified=True,
                external_audit_subject_chain_verified=True,
                full_segment_chain_verified=True,
                clean_closure=(
                    not self.authority.closure.fatal
                    and self.authority.closure.stop_reason == "completed_duration"
                ),
            ),
            observation_window=ClockObservationWindowV1(
                started_at_ms=self.authority.start.started_at_ms,
                closed_at_ms=self.authority.closure.closed_at_ms,
                started_monotonic_ns=self.start_ns,
                closed_monotonic_ns=self.end_ns,
                observation_duration_ns=duration_ns,
                configured_duration_seconds=86_400,
                full_configured_duration_observed=full_duration,
            ),
            storage=ClockStorageBindingV1(
                segment_count=len(manifests),
                record_count=sum(item.record_count for item in manifests),
                compressed_bytes=sum(item.compressed_bytes for item in manifests),
                uncompressed_bytes=sum(item.uncompressed_bytes for item in manifests),
                typed_canonical_records_streamed=True,
                ingest_sequence_verified=True,
            ),
            venue_time_poll_interval_seconds=30,
            clock_sample_max_age_ms=60_000,
            maximum_header_rtt_ms=2_000,
            maximum_rate_error_ppm=1_000,
            timestamp_quantization_margin_ms=1,
            wall_monotonic_healthy_max_residual_ms=2,
            wall_monotonic_fail_min_residual_ms=100,
            clock_coverage_min_ppm=999_000,
            global_clock=global_metrics,
            venues=(venue_reports[0], venue_reports[1]),
            scope_boundaries=ClockScopeBoundariesV1(
                infrastructure_clock_only=True,
                venue_time_payload_interpreted=True,
                future_market_information_interpreted=False,
                future_clock_interpolation_performed=False,
                causal_cutoff_mapper_exposed=True,
                report_runtime_attested_to_capture_source_manifest=False,
                alert_runtime_integrated=False,
                paper_runtime_integrated=False,
                efficacy_acceptance_performed=False,
                thirty_day_holdout_gate_evaluated=False,
                promotion_authorized=False,
            ),
            verdict=verdict,
            verdict_reasons=reasons,
        )


def build_clock_health_report(
    *,
    start_path: str | Path,
    closure_path: str | Path,
    capture_directory: str | Path,
) -> CanonicalClockHealthReport:
    """Build one deterministic infrastructure-only clock report from closed evidence."""

    authority = verify_closed_capture_authority(
        start_path=start_path,
        closure_path=closure_path,
        capture_directory=capture_directory,
    )
    accumulator = _ClockHealthAccumulator(authority)
    consume_closed_capture_records(authority, accumulator.consume)
    report = accumulator.finish()
    document = report.model_dump(mode="json")
    _require_no_forbidden_report_keys(document)
    encoded = canonical_json_bytes(document)
    return CanonicalClockHealthReport(
        report=report,
        canonical_bytes=encoded,
        sha256=hashlib.sha256(encoded).hexdigest(),
    )


def assess_causal_clock_cutoff(
    sample: VenueClockSampleV1 | None,
    *,
    receipt_monotonic_ns: int,
    cutoff_ms: int,
) -> ClockCutoffAssessmentV1:
    """Classify receipt availability without future clock interpolation.

    The caller supplies only the latest sample whose complete payload was already
    available. This helper independently rejects future or stale samples.
    """

    if (
        type(receipt_monotonic_ns) is not int
        or type(cutoff_ms) is not int
        or receipt_monotonic_ns < 0
        or cutoff_ms < 0
    ):
        raise ValueError("clock cutoff inputs must be nonnegative")
    if sample is None:
        return ClockCutoffAssessmentV1(
            verdict="CLOCK_INCONCLUSIVE",
            reason="clock_sample_missing",
            cutoff_ms=cutoff_ms,
            receipt_monotonic_ns=receipt_monotonic_ns,
            source_ingest_seq=None,
            venue_time_lower_ms=None,
            venue_time_upper_ms=None,
        )
    if receipt_monotonic_ns < sample.available_at_monotonic_ns:
        return ClockCutoffAssessmentV1(
            verdict="CLOCK_INCONCLUSIVE",
            reason="clock_sample_not_yet_available",
            cutoff_ms=cutoff_ms,
            receipt_monotonic_ns=receipt_monotonic_ns,
            source_ingest_seq=sample.source_ingest_seq,
            venue_time_lower_ms=None,
            venue_time_upper_ms=None,
        )
    age_ns = receipt_monotonic_ns - sample.available_at_monotonic_ns
    if age_ns > _CLOCK_SAMPLE_MAX_AGE_NS:
        return ClockCutoffAssessmentV1(
            verdict="CLOCK_INCONCLUSIVE",
            reason="clock_sample_stale",
            cutoff_ms=cutoff_ms,
            receipt_monotonic_ns=receipt_monotonic_ns,
            source_ingest_seq=sample.source_ingest_seq,
            venue_time_lower_ms=None,
            venue_time_upper_ms=None,
        )
    lower_delta_ns = receipt_monotonic_ns - sample.response_first_byte_monotonic_ns
    upper_delta_ns = receipt_monotonic_ns - sample.request_started_monotonic_ns
    lower_elapsed_ms = _scaled_floor_ms(
        lower_delta_ns,
        1_000_000 - _CLOCK_RATE_ERROR_PPM,
    )
    upper_elapsed_ms = _scaled_ceil_ms(
        upper_delta_ns,
        1_000_000 + _CLOCK_RATE_ERROR_PPM,
    )
    lower_ms = (
        sample.server_time_ms
        + lower_elapsed_ms
        - _TIMESTAMP_QUANTIZATION_MARGIN_MS
    )
    upper_ms = (
        sample.server_time_ms
        + upper_elapsed_ms
        + _TIMESTAMP_QUANTIZATION_MARGIN_MS
    )
    if upper_ms <= cutoff_ms:
        verdict: ClockCutoffVerdict = "ADMISSIBLE"
        reason: ClockCutoffReason = "receipt_interval_at_or_before_cutoff"
    elif lower_ms > cutoff_ms:
        verdict = "LATE"
        reason = "receipt_interval_after_cutoff"
    else:
        verdict = "CLOCK_INCONCLUSIVE"
        reason = "receipt_interval_straddles_cutoff"
    return ClockCutoffAssessmentV1(
        verdict=verdict,
        reason=reason,
        cutoff_ms=cutoff_ms,
        receipt_monotonic_ns=receipt_monotonic_ns,
        source_ingest_seq=sample.source_ingest_seq,
        venue_time_lower_ms=lower_ms,
        venue_time_upper_ms=upper_ms,
    )


def clock_samples_rate_continuous(
    previous: VenueClockSampleV1,
    current: VenueClockSampleV1,
) -> bool:
    """Return whether two same-venue samples fit the frozen ±1,000 ppm envelope."""

    if previous.market != current.market or previous.request_role != current.request_role:
        return False
    minimum_elapsed_ns = max(
        0,
        current.request_started_monotonic_ns
        - previous.response_first_byte_monotonic_ns,
    )
    maximum_elapsed_ns = (
        current.response_first_byte_monotonic_ns
        - previous.request_started_monotonic_ns
    )
    if maximum_elapsed_ns < 0:
        return False
    lower_ms = (
        _scaled_floor_ms(
            minimum_elapsed_ns,
            1_000_000 - _CLOCK_RATE_ERROR_PPM,
        )
        - _CONTINUITY_QUANTIZATION_MARGIN_MS
    )
    upper_ms = (
        _scaled_ceil_ms(
            maximum_elapsed_ns,
            1_000_000 + _CLOCK_RATE_ERROR_PPM,
        )
        + _CONTINUITY_QUANTIZATION_MARGIN_MS
    )
    server_elapsed_ms = current.server_time_ms - previous.server_time_ms
    return lower_ms <= server_elapsed_ms <= upper_ms


def _record_receipt(record: CaptureRecord) -> tuple[int, int]:
    if isinstance(record, RestEnvelopeV1):
        return record.response_received_at_ms, record.response_received_monotonic_ns
    if isinstance(record, RestEnvelopeV2):
        return record.response_completed_at_ms, record.response_completed_monotonic_ns
    return record.received_at_ms, record.received_monotonic_ns


def _market_for_clock_record(record: CaptureRecord) -> Market | None:
    if isinstance(record, RestEnvelopeV2):
        if record.request_role == "spot_venue_time":
            return Market.SPOT
        if record.request_role == "futures_venue_time":
            return Market.FUTURES
    if isinstance(record, RestEnvelopeV1 | RestEnvelopeV2):
        if record.endpoint_path == "/api/v3/time":
            return Market.SPOT
        if record.endpoint_path == "/fapi/v1/time":
            return Market.FUTURES
    return None


def _clock_identity(market: Market) -> tuple[ClockRole, ClockEndpoint]:
    if market is Market.SPOT:
        return "spot_venue_time", "/api/v3/time"
    return "futures_venue_time", "/fapi/v1/time"


def _parse_server_time(
    raw_payload: str,
    encoding: RawPayloadEncoding,
) -> int | None:
    try:
        raw = payload_bytes(raw_payload, encoding).decode("utf-8")
        parsed = json.loads(raw, object_pairs_hook=_reject_duplicate_keys)
    except (UnicodeDecodeError, ValueError, json.JSONDecodeError):
        return None
    if not isinstance(parsed, dict) or tuple(parsed) != ("serverTime",):
        return None
    value = parsed["serverTime"]
    if type(value) is not int or value < 0:
        return None
    return value


def _reject_duplicate_keys(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate JSON object key is forbidden")
        result[key] = value
    return result


def _scaled_floor_ms(delta_ns: int, rate_ppm: int) -> int:
    if delta_ns < 0:
        raise ValueError("clock mapping delta must be nonnegative")
    return delta_ns * rate_ppm // _RATE_DENOMINATOR


def _scaled_ceil_ms(delta_ns: int, rate_ppm: int) -> int:
    if delta_ns < 0:
        raise ValueError("clock mapping delta must be nonnegative")
    numerator = delta_ns * rate_ppm
    return (numerator + _RATE_DENOMINATOR - 1) // _RATE_DENOMINATOR


def _ppm(numerator: int, denominator: int) -> int:
    if denominator < 0 or numerator < 0 or numerator > denominator:
        raise ValueError("clock coverage ppm inputs are invalid")
    if denominator == 0:
        return 0
    return numerator * 1_000_000 // denominator


def _verdict_from_fields(
    *,
    closure_fatal: bool,
    closure_stop_reason: ClockStopReason,
    full_duration: bool,
    global_clock: GlobalClockDiagnosticsV1,
    venues: tuple[PerVenueClockHealthV1, PerVenueClockHealthV1],
) -> tuple[ClockHealthVerdict, tuple[ClockHealthVerdictReason, ...]]:
    hard_failures: list[ClockHealthVerdictReason] = []
    if closure_fatal:
        hard_failures.append("fatal_session_closure")
    if global_clock.monotonic_regression_count:
        hard_failures.append("monotonic_regression_observed")
    if global_clock.wall_clock_regression_count or any(
        venue.sample_wall_clock_regression_count for venue in venues
    ):
        hard_failures.append("wall_clock_regression_observed")
    if global_clock.discontinuity_interval_count or any(
        venue.sample_wall_clock_discontinuity_count for venue in venues
    ):
        hard_failures.append("wall_clock_discontinuity_observed")
    if any(venue.contract_mismatch_count for venue in venues):
        hard_failures.append("time_sample_contract_mismatch_observed")
    if any(venue.continuity_failure_count for venue in venues):
        hard_failures.append("venue_clock_continuity_failure_observed")
    if hard_failures:
        return "FAIL", tuple(hard_failures)

    incomplete: list[ClockHealthVerdictReason] = []
    if not full_duration:
        incomplete.append("configured_duration_not_observed")
    if closure_stop_reason != "completed_duration":
        incomplete.append("closure_not_completed_duration")
    if incomplete:
        return "INCOMPLETE", tuple(incomplete)

    failures: list[ClockHealthVerdictReason] = []
    if global_clock.inconclusive_interval_count or any(
        venue.sample_wall_monotonic_inconclusive_count for venue in venues
    ):
        failures.append("wall_clock_mapping_inconclusive_observed")
    spot, futures = venues
    if spot.valid_sample_count == 0:
        failures.append("spot_clock_never_valid")
    if futures.valid_sample_count == 0:
        failures.append("futures_clock_never_valid")
    if not spot.meets_clock_coverage_requirement:
        failures.append("spot_clock_coverage_below_requirement")
    if not futures.meets_clock_coverage_requirement:
        failures.append("futures_clock_coverage_below_requirement")
    if failures:
        return "FAIL", tuple(failures)
    return "CLOCK_HEALTH_PASS", ("clock_health_requirements_satisfied",)


def _require_no_forbidden_report_keys(value: object) -> None:
    if isinstance(value, dict):
        forbidden = {
            str(key).casefold()
            for key in value
            if str(key).casefold() in _FORBIDDEN_REPORT_KEYS
        }
        if forbidden:
            raise ValueError(f"clock report contains forbidden keys: {sorted(forbidden)}")
        for item in value.values():
            _require_no_forbidden_report_keys(item)
    elif isinstance(value, list | tuple):
        for item in value:
            _require_no_forbidden_report_keys(item)
