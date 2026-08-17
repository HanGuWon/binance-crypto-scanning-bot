from __future__ import annotations

import hashlib
from dataclasses import replace

import pytest

from signalbot.r4b_v2.capture.wal import WalSyncPolicyV2
from signalbot.r4b_v2.capture.wal_qualification import (
    WAL_QUALIFICATION_DURATION_MS_V2,
    WAL_RECORD_CAP_CANDIDATES_V2,
    WAL_SYNC_CANDIDATES_MS_V2,
    WalCandidateMetricsV2,
    WalCandidateQualificationV2,
    WalQualificationError,
    WalQualificationRunV2,
    WalT0BlockedError,
    select_wal_candidate_v2,
    wal_candidate_id_v2,
)

QUALIFICATION_ID = "wal-final-panel-24h-grid-q1"
WINDOW_START_MS = 2_000_000_000_000
WINDOW_END_MS = WINDOW_START_MS + WAL_QUALIFICATION_DURATION_MS_V2
SELECTION_MS = WINDOW_END_MS
H_START_MS = SELECTION_MS + 60_000


def _policy(sync_ms: int, record_cap: int) -> WalSyncPolicyV2:
    return WalSyncPolicyV2(
        qualification_id=QUALIFICATION_ID,
        fsync_candidate_id=wal_candidate_id_v2(
            sync_ms=sync_ms,
            record_cap=record_cap,
        ),
        interval_ms=sync_ms,
        max_unsynced_records=record_cap,
        max_unsynced_bytes=8_000_000,
        max_record_bytes=20_000,
        max_segment_bytes=16_000_000,
    )


def _metrics(**overrides: int | bool) -> WalCandidateMetricsV2:
    values: dict[str, int | bool] = {
        "unresolved_overflow_or_drop_count": 0,
        "p99_queue_fraction_ppm": 500_000,
        "maximum_queue_fraction_ppm": 750_000,
        "p99_enqueue_latency_ns": 10_000_000,
        "maximum_enqueue_latency_ns": 100_000_000,
        "p99_cpu_fraction_ppm": 700_000,
        "maximum_cpu_fraction_ppm": 850_000,
        "p99_fsync_latency_ns": 100_000_000,
        "maximum_fsync_latency_ns": 500_000_000,
        "service_rate_over_p99_ingress_ppm": 2_000_000,
        "service_rate_over_peak_1s_ingress_ppm": 1_250_000,
        "crash_replay_root_equality": True,
    }
    values.update(overrides)
    return WalCandidateMetricsV2(**values)  # type: ignore[arg-type]


def _run(
    passing: set[tuple[int, int]],
    *,
    reverse_candidates: bool = False,
    **overrides: object,
) -> WalQualificationRunV2:
    candidates = [
        WalCandidateQualificationV2(
            policy=_policy(sync_ms, record_cap),
            metrics=(
                _metrics()
                if (sync_ms, record_cap) in passing
                else _metrics(unresolved_overflow_or_drop_count=1)
            ),
            measurement_root_sha256=hashlib.sha256(
                f"{sync_ms}:{record_cap}".encode()
            ).hexdigest(),
        )
        for sync_ms in WAL_SYNC_CANDIDATES_MS_V2
        for record_cap in WAL_RECORD_CAP_CANDIDATES_V2
    ]
    if reverse_candidates:
        candidates.reverse()
    values: dict[str, object] = {
        "qualification_id": QUALIFICATION_ID,
        "window_start_wall_ms": WINDOW_START_MS,
        "window_end_wall_ms": WINDOW_END_MS,
        "actual_final_panel_sha256": "a" * 64,
        "final_codec_sha256": "b" * 64,
        "source_manifest_sha256": "c" * 64,
        "runtime_manifest_sha256": "d" * 64,
        "independent_failure_domain_evidence_sha256": "e" * 64,
        "actual_final_panel_passed": True,
        "final_codec_passed": True,
        "independent_failure_domains_passed": True,
        "engineering_only": True,
        "strategy_or_outcome_data_accessed": False,
        "candidates": tuple(candidates),
    }
    values.update(overrides)
    return WalQualificationRunV2(**values)  # type: ignore[arg-type]


@pytest.mark.parametrize(
    ("field_name", "failing_value"),
    [
        ("unresolved_overflow_or_drop_count", 1),
        ("p99_queue_fraction_ppm", 500_001),
        ("maximum_queue_fraction_ppm", 750_001),
        ("p99_enqueue_latency_ns", 10_000_001),
        ("maximum_enqueue_latency_ns", 100_000_001),
        ("p99_cpu_fraction_ppm", 700_001),
        ("maximum_cpu_fraction_ppm", 850_001),
        ("p99_fsync_latency_ns", 100_000_001),
        ("maximum_fsync_latency_ns", 500_000_001),
        ("service_rate_over_p99_ingress_ppm", 1_999_999),
        ("service_rate_over_peak_1s_ingress_ppm", 1_249_999),
        ("crash_replay_root_equality", False),
    ],
)
def test_every_exact_engineering_gate_passes_and_one_quantum_beyond_fails(
    field_name: str,
    failing_value: int | bool,
) -> None:
    boundary = _metrics()
    assert boundary.passed
    failed = replace(boundary, **{field_name: failing_value})
    assert not failed.passed
    assert failed.failed_gates


def test_complete_grid_is_canonical_and_selection_is_lexicographic() -> None:
    passing = {(10, 1024), (10, 4096), (50, 256), (100, 256)}
    forward = _run(passing)
    reversed_run = _run(passing, reverse_candidates=True)
    assert forward.sha256 == reversed_run.sha256

    receipt = select_wal_candidate_v2(
        forward,
        selection_wall_ms=SELECTION_MS,
        h_start_wall_ms=H_START_MS,
    )
    assert receipt.status == "SELECTED"
    assert receipt.selected_policy == _policy(10, 1024)
    assert receipt.passing_candidate_ids == (
        wal_candidate_id_v2(sync_ms=10, record_cap=1024),
        wal_candidate_id_v2(sync_ms=10, record_cap=4096),
        wal_candidate_id_v2(sync_ms=50, record_cap=256),
        wal_candidate_id_v2(sync_ms=100, record_cap=256),
    )


def test_no_passing_candidate_blocks_t0_and_leaves_h_start_unset() -> None:
    receipt = select_wal_candidate_v2(
        _run(set()),
        selection_wall_ms=SELECTION_MS,
        h_start_wall_ms=None,
    )
    assert receipt.status == "T0_BLOCKED_NO_PASSING_CANDIDATE"
    assert receipt.selected_policy is None
    with pytest.raises(WalT0BlockedError, match="T0 is blocked"):
        receipt.require_selected_policy(_policy(10, 256))
    with pytest.raises(WalQualificationError, match="H_start must remain unset"):
        select_wal_candidate_v2(
            _run(set()),
            selection_wall_ms=SELECTION_MS,
            h_start_wall_ms=H_START_MS,
        )


def test_selection_must_follow_qualification_and_strictly_precede_h_start() -> None:
    run = _run({(10, 256)})
    with pytest.raises(WalQualificationError, match="cannot precede"):
        select_wal_candidate_v2(
            run,
            selection_wall_ms=WINDOW_END_MS - 1,
            h_start_wall_ms=H_START_MS,
        )
    receipt = select_wal_candidate_v2(
        run,
        selection_wall_ms=H_START_MS - 1,
        h_start_wall_ms=H_START_MS,
    )
    assert receipt.selection_wall_ms == H_START_MS - 1
    with pytest.raises(WalQualificationError, match="before H_start"):
        select_wal_candidate_v2(
            run,
            selection_wall_ms=H_START_MS,
            h_start_wall_ms=H_START_MS,
        )


@pytest.mark.parametrize("duration_delta_ms", [-1, 1])
def test_qualification_window_is_exactly_24_complete_hours(
    duration_delta_ms: int,
) -> None:
    with pytest.raises(WalQualificationError, match="exactly 24 complete hours"):
        _run(
            {(10, 256)},
            window_end_wall_ms=WINDOW_END_MS + duration_delta_ms,
        )


@pytest.mark.parametrize(
    ("field_name", "value", "message"),
    [
        ("engineering_only", False, "engineering_only"),
        ("strategy_or_outcome_data_accessed", True, "strategy signals"),
        ("actual_final_panel_passed", False, "actual_final_panel_passed"),
        ("final_codec_passed", False, "final_codec_passed"),
        (
            "independent_failure_domains_passed",
            False,
            "independent_failure_domains_passed",
        ),
    ],
)
def test_nonengineering_or_incomplete_evidence_is_rejected(
    field_name: str,
    value: bool,
    message: str,
) -> None:
    with pytest.raises(WalQualificationError, match=message):
        _run({(10, 256)}, **{field_name: value})


def test_grid_must_be_complete_unique_and_change_only_two_dimensions() -> None:
    run = _run({(10, 256)})
    with pytest.raises(WalQualificationError, match="each frozen grid candidate"):
        replace(run, candidates=run.candidates[:-1])

    changed = replace(
        run.candidates[-1],
        policy=replace(run.candidates[-1].policy, max_unsynced_bytes=7_999_999),
    )
    with pytest.raises(WalQualificationError, match="differ only"):
        replace(run, candidates=(*run.candidates[:-1], changed))


def test_runtime_policy_must_exactly_match_the_selected_receipt() -> None:
    receipt = select_wal_candidate_v2(
        _run({(50, 1024)}),
        selection_wall_ms=SELECTION_MS,
        h_start_wall_ms=H_START_MS,
    )
    receipt.require_selected_policy(_policy(50, 1024))
    with pytest.raises(WalQualificationError, match="runtime WAL policy differs"):
        receipt.require_selected_policy(_policy(50, 4096))
