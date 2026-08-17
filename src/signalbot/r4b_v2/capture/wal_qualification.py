from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass
from typing import Literal

from signalbot.r4b_v2.canonical import canonical_json_line
from signalbot.r4b_v2.capture.wal import WalSyncPolicyV2

WAL_SYNC_CANDIDATES_MS_V2 = (10, 50, 100)
WAL_RECORD_CAP_CANDIDATES_V2 = (256, 1024, 4096)
WAL_QUALIFICATION_DURATION_MS_V2 = 24 * 60 * 60 * 1_000

_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_SELECTION_RULE = "PASS_ONLY_THEN_SYNC_MS_ASC_THEN_RECORD_CAP_ASC"
_SELECTED = "SELECTED"
_BLOCKED = "T0_BLOCKED_NO_PASSING_CANDIDATE"


class WalQualificationError(ValueError):
    """Raised when engineering qualification evidence is incomplete or inconsistent."""


class WalT0BlockedError(RuntimeError):
    """Raised when no WAL candidate passed the frozen pre-H_start grid."""


@dataclass(frozen=True, slots=True)
class WalCandidateMetricsV2:
    """Integer-unit metrics for one actual-final-panel 24-hour candidate run."""

    unresolved_overflow_or_drop_count: int
    p99_queue_fraction_ppm: int
    maximum_queue_fraction_ppm: int
    p99_enqueue_latency_ns: int
    maximum_enqueue_latency_ns: int
    p99_cpu_fraction_ppm: int
    maximum_cpu_fraction_ppm: int
    p99_fsync_latency_ns: int
    maximum_fsync_latency_ns: int
    service_rate_over_p99_ingress_ppm: int
    service_rate_over_peak_1s_ingress_ppm: int
    crash_replay_root_equality: bool
    schema_version: str = "r4b_v2_wal_candidate_metrics_v1"

    def __post_init__(self) -> None:
        for field_name in (
            "unresolved_overflow_or_drop_count",
            "p99_queue_fraction_ppm",
            "maximum_queue_fraction_ppm",
            "p99_enqueue_latency_ns",
            "maximum_enqueue_latency_ns",
            "p99_cpu_fraction_ppm",
            "maximum_cpu_fraction_ppm",
            "p99_fsync_latency_ns",
            "maximum_fsync_latency_ns",
            "service_rate_over_p99_ingress_ppm",
            "service_rate_over_peak_1s_ingress_ppm",
        ):
            _require_nonnegative_int(getattr(self, field_name), field_name)
        if type(self.crash_replay_root_equality) is not bool:
            raise WalQualificationError("crash_replay_root_equality must be boolean")
        if self.schema_version != "r4b_v2_wal_candidate_metrics_v1":
            raise WalQualificationError("unsupported WAL candidate metrics schema")

    @property
    def failed_gates(self) -> tuple[str, ...]:
        failures: list[str] = []
        checks = (
            (
                self.unresolved_overflow_or_drop_count == 0,
                "unresolved_overflow_or_drop_count",
            ),
            (self.p99_queue_fraction_ppm <= 500_000, "p99_queue_fraction"),
            (self.maximum_queue_fraction_ppm <= 750_000, "maximum_queue_fraction"),
            (self.p99_enqueue_latency_ns <= 10_000_000, "p99_enqueue_latency"),
            (
                self.maximum_enqueue_latency_ns <= 100_000_000,
                "maximum_enqueue_latency",
            ),
            (self.p99_cpu_fraction_ppm <= 700_000, "p99_CPU"),
            (self.maximum_cpu_fraction_ppm <= 850_000, "maximum_CPU"),
            (self.p99_fsync_latency_ns <= 100_000_000, "p99_fsync_latency"),
            (
                self.maximum_fsync_latency_ns <= 500_000_000,
                "maximum_fsync_latency",
            ),
            (
                self.service_rate_over_p99_ingress_ppm >= 2_000_000,
                "service_rate_over_p99_ingress",
            ),
            (
                self.service_rate_over_peak_1s_ingress_ppm >= 1_250_000,
                "service_rate_over_peak_1s_ingress",
            ),
            (self.crash_replay_root_equality, "crash_replay_root_equality"),
        )
        for passed, name in checks:
            if not passed:
                failures.append(name)
        return tuple(failures)

    @property
    def passed(self) -> bool:
        return not self.failed_gates


@dataclass(frozen=True, slots=True)
class WalCandidateQualificationV2:
    policy: WalSyncPolicyV2
    metrics: WalCandidateMetricsV2
    measurement_root_sha256: str
    schema_version: str = "r4b_v2_wal_candidate_qualification_v1"

    def __post_init__(self) -> None:
        _require_sha256(self.measurement_root_sha256, "measurement_root_sha256")
        if self.schema_version != "r4b_v2_wal_candidate_qualification_v1":
            raise WalQualificationError("unsupported WAL candidate qualification schema")

    @property
    def candidate_id(self) -> str:
        return wal_candidate_id_v2(
            sync_ms=self.policy.interval_ms,
            record_cap=self.policy.max_unsynced_records,
        )

    @property
    def passed(self) -> bool:
        return self.metrics.passed


@dataclass(frozen=True, slots=True)
class WalQualificationRunV2:
    """Self-contained outcome-free evidence for the complete nine-candidate grid."""

    qualification_id: str
    window_start_wall_ms: int
    window_end_wall_ms: int
    actual_final_panel_sha256: str
    final_codec_sha256: str
    source_manifest_sha256: str
    runtime_manifest_sha256: str
    independent_failure_domain_evidence_sha256: str
    actual_final_panel_passed: bool
    final_codec_passed: bool
    independent_failure_domains_passed: bool
    engineering_only: bool
    strategy_or_outcome_data_accessed: bool
    candidates: tuple[WalCandidateQualificationV2, ...]
    schema_version: str = "r4b_v2_wal_qualification_run_v1"

    def __post_init__(self) -> None:
        _require_identity(self.qualification_id, "qualification_id")
        _require_nonnegative_int(self.window_start_wall_ms, "window_start_wall_ms")
        _require_nonnegative_int(self.window_end_wall_ms, "window_end_wall_ms")
        if self.window_end_wall_ms - self.window_start_wall_ms != (
            WAL_QUALIFICATION_DURATION_MS_V2
        ):
            raise WalQualificationError(
                "WAL qualification interval must be exactly 24 complete hours"
            )
        for field_name in (
            "actual_final_panel_sha256",
            "final_codec_sha256",
            "source_manifest_sha256",
            "runtime_manifest_sha256",
            "independent_failure_domain_evidence_sha256",
        ):
            _require_sha256(getattr(self, field_name), field_name)
        required_true = (
            "actual_final_panel_passed",
            "final_codec_passed",
            "independent_failure_domains_passed",
            "engineering_only",
        )
        for field_name in required_true:
            value = getattr(self, field_name)
            if type(value) is not bool or not value:
                raise WalQualificationError(f"{field_name} must be true")
        if (
            type(self.strategy_or_outcome_data_accessed) is not bool
            or self.strategy_or_outcome_data_accessed
        ):
            raise WalQualificationError(
                "WAL selection may not access strategy signals, returns, or outcomes"
            )
        if self.schema_version != "r4b_v2_wal_qualification_run_v1":
            raise WalQualificationError("unsupported WAL qualification run schema")

        ordered = tuple(
            sorted(
                self.candidates,
                key=lambda item: (
                    item.policy.interval_ms,
                    item.policy.max_unsynced_records,
                ),
            )
        )
        object.__setattr__(self, "candidates", ordered)
        expected_grid = {
            (sync_ms, record_cap)
            for sync_ms in WAL_SYNC_CANDIDATES_MS_V2
            for record_cap in WAL_RECORD_CAP_CANDIDATES_V2
        }
        observed_grid = {
            (item.policy.interval_ms, item.policy.max_unsynced_records)
            for item in ordered
        }
        if len(ordered) != len(expected_grid) or observed_grid != expected_grid:
            raise WalQualificationError(
                "WAL qualification must contain each frozen grid candidate exactly once"
            )
        common_non_grid_policy: set[tuple[int, int, int]] = set()
        for item in ordered:
            policy = item.policy
            if policy.qualification_id != self.qualification_id:
                raise WalQualificationError(
                    "candidate policy qualification_id differs from its run"
                )
            if policy.fsync_candidate_id != item.candidate_id:
                raise WalQualificationError(
                    "candidate policy ID differs from the deterministic grid ID"
                )
            common_non_grid_policy.add(
                (
                    policy.max_unsynced_bytes,
                    policy.max_record_bytes,
                    policy.max_segment_bytes,
                )
            )
        if len(common_non_grid_policy) != 1:
            raise WalQualificationError(
                "WAL grid candidates may differ only by sync_ms and record_cap"
            )

    @property
    def sha256(self) -> str:
        return hashlib.sha256(canonical_json_line(self)).hexdigest()

    @property
    def passing_candidates(self) -> tuple[WalCandidateQualificationV2, ...]:
        return tuple(item for item in self.candidates if item.passed)


WalSelectionStatusV2 = Literal[
    "SELECTED",
    "T0_BLOCKED_NO_PASSING_CANDIDATE",
]


@dataclass(frozen=True, slots=True)
class WalSelectionReceiptV2:
    """Canonical pre-H_start selection receipt, including its full evidence run."""

    qualification: WalQualificationRunV2
    selection_wall_ms: int
    h_start_wall_ms: int | None
    status: WalSelectionStatusV2
    selected_policy: WalSyncPolicyV2 | None
    passing_candidate_ids: tuple[str, ...]
    selection_rule: str = _SELECTION_RULE
    schema_version: str = "r4b_v2_wal_selection_receipt_v1"

    def __post_init__(self) -> None:
        _require_nonnegative_int(self.selection_wall_ms, "selection_wall_ms")
        if self.selection_wall_ms < self.qualification.window_end_wall_ms:
            raise WalQualificationError(
                "WAL selection cannot precede completion of its 24-hour qualification"
            )
        if self.selection_rule != _SELECTION_RULE:
            raise WalQualificationError("unsupported WAL candidate selection rule")
        if self.schema_version != "r4b_v2_wal_selection_receipt_v1":
            raise WalQualificationError("unsupported WAL selection receipt schema")

        passing = self.qualification.passing_candidates
        expected_ids = tuple(item.candidate_id for item in passing)
        if self.passing_candidate_ids != expected_ids:
            raise WalQualificationError(
                "selection receipt passing IDs differ from the engineering gates"
            )
        if not passing:
            if (
                self.status != _BLOCKED
                or self.selected_policy is not None
                or self.h_start_wall_ms is not None
            ):
                raise WalQualificationError(
                    "no passing WAL candidate must block T0 and leave H_start unset"
                )
            return

        if self.status != _SELECTED or self.selected_policy != passing[0].policy:
            raise WalQualificationError(
                "selected WAL policy is not the lexicographically first passer"
            )
        if self.h_start_wall_ms is None:
            raise WalQualificationError("a passing WAL selection requires H_start")
        _require_nonnegative_int(self.h_start_wall_ms, "h_start_wall_ms")
        if self.selection_wall_ms >= self.h_start_wall_ms:
            raise WalQualificationError("WAL candidate must be selected before H_start")

    @property
    def sha256(self) -> str:
        return hashlib.sha256(canonical_json_line(self)).hexdigest()

    def require_selected_policy(self, policy: WalSyncPolicyV2) -> None:
        if self.status == _BLOCKED:
            raise WalT0BlockedError("no WAL qualification candidate passed; T0 is blocked")
        if self.selected_policy != policy:
            raise WalQualificationError(
                "runtime WAL policy differs from the canonical selection receipt"
            )


def select_wal_candidate_v2(
    qualification: WalQualificationRunV2,
    *,
    selection_wall_ms: int,
    h_start_wall_ms: int | None,
) -> WalSelectionReceiptV2:
    """Select deterministically using engineering gates and no strategy outcomes."""

    passing = qualification.passing_candidates
    if not passing:
        if h_start_wall_ms is not None:
            raise WalQualificationError(
                "H_start must remain unset when no WAL candidate passes"
            )
        return WalSelectionReceiptV2(
            qualification=qualification,
            selection_wall_ms=selection_wall_ms,
            h_start_wall_ms=None,
            status=_BLOCKED,
            selected_policy=None,
            passing_candidate_ids=(),
        )
    return WalSelectionReceiptV2(
        qualification=qualification,
        selection_wall_ms=selection_wall_ms,
        h_start_wall_ms=h_start_wall_ms,
        status=_SELECTED,
        selected_policy=passing[0].policy,
        passing_candidate_ids=tuple(item.candidate_id for item in passing),
    )


def wal_candidate_id_v2(*, sync_ms: int, record_cap: int) -> str:
    if sync_ms not in WAL_SYNC_CANDIDATES_MS_V2:
        raise WalQualificationError("sync_ms is outside the frozen WAL candidate grid")
    if record_cap not in WAL_RECORD_CAP_CANDIDATES_V2:
        raise WalQualificationError("record_cap is outside the frozen WAL candidate grid")
    return f"wal-fdatasync-{sync_ms}ms-r{record_cap}"


def _require_identity(value: str, field_name: str) -> None:
    if (
        not isinstance(value, str)
        or not value
        or value.strip() != value
        or len(value) > 256
    ):
        raise WalQualificationError(f"{field_name} must be a bounded non-empty identity")


def _require_sha256(value: str, field_name: str) -> None:
    if not isinstance(value, str) or _SHA256_RE.fullmatch(value) is None:
        raise WalQualificationError(f"{field_name} must be a lowercase SHA-256 digest")


def _require_nonnegative_int(value: int, field_name: str) -> None:
    if type(value) is not int or value < 0:
        raise WalQualificationError(f"{field_name} must be a nonnegative integer")
