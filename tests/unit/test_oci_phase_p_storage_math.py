from __future__ import annotations

import sys
from pathlib import Path

ROOT = str(Path(__file__).resolve().parents[2])
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from tools.oci_phase_p_storage_math import (  # noqa: E402
    BUCKET_MS,
    GrowthSample,
    SegmentSample,
    classify_publish_sequence,
    compute_gates,
    compute_tape_rates,
    conservative_growth_rate,
    deterministic_finalize_reserve,
)


def test_manifest_bytes_are_part_of_retained_rates() -> None:
    activation = 1_000_000
    captured = activation + 3_600_000
    segment = SegmentSample(
        bucket_start_ms=activation,
        last_received_at_ms=captured,
        compressed_bytes=1,
        manifest_bytes=10_000,
        uncompressed_bytes=20,
    )

    result = compute_tape_rates(
        activation_ms=activation,
        captured_at_ms=captured,
        segments=[segment],
        active_partials={},
    )

    assert result["finalized_bytes"] == 10_001
    assert result["manifest_bytes"] == 10_000
    assert result["whole_clock_bytes_per_h"] == 10_001
    assert result["recent_bytes_per_h"] == 10_001


def test_p95_uses_retained_bytes_not_transient_plaintext() -> None:
    activation = 0
    segments = [
        SegmentSample(
            bucket_start_ms=index * BUCKET_MS,
            last_received_at_ms=(index + 1) * BUCKET_MS,
            compressed_bytes=100,
            manifest_bytes=10,
            uncompressed_bytes=1_000,
        )
            for index in range(36)
    ]

    result = compute_tape_rates(
        activation_ms=activation,
        captured_at_ms=10_800_000,
        segments=segments,
        active_partials={},
    )

    assert result["p95_bytes_per_h"] == 1_320
    assert result["observed_max_terminal_plaintext_bytes"] == 1_000


def test_p95_requires_complete_one_hour_windows() -> None:
    result = compute_tape_rates(
        activation_ms=0,
        captured_at_ms=3_600_000,
        segments=[
            SegmentSample(
                bucket_start_ms=index * BUCKET_MS,
                last_received_at_ms=(index + 1) * BUCKET_MS,
                compressed_bytes=100,
                manifest_bytes=10,
                uncompressed_bytes=1_000,
            )
            for index in range(11)
        ],
        active_partials={},
    )

    assert result["p95_bytes_per_h"] is None
    assert result["p95_window_count"] == 0


def test_same_bucket_partials_keep_market_dimension() -> None:
    result = compute_tape_rates(
        activation_ms=0,
        captured_at_ms=3_600_000,
        segments=[],
        active_partials={("futures", 0): 100, ("spot", 0): 200},
    )

    assert result["active_partial_bytes"] == 300
    assert result["recent_bytes_per_h"] == 300


def test_finalize_reserve_uses_deterministic_bound_not_history() -> None:
    assert deterministic_finalize_reserve(
        [8], manifest_schema_bound_bytes=100
    ) == 8 + 1 + 128 + 100


def test_quota_and_root_gate_equality_boundaries() -> None:
    quota_requirement = 10 * 48 + 20 + 5
    root_requirement = 12 * 48 + 30
    result = compute_gates(
        quota_bytes=1_000 + quota_requirement,
        current_physical_quota_bytes=1_000,
        conservative_tape_bytes_per_h=10,
        finalize_reserve_bytes=20,
        quota_safety_margin_bytes=5,
        root_free_bytes=root_requirement,
        conservative_root_growth_bytes_per_h=12,
        root_emergency_floor_bytes=30,
    )
    assert result["quota_gate_met"] is True
    assert result["root_gate_met"] is True
    assert result["state"] == "GREEN"

    quota_fail = compute_gates(
        quota_bytes=1_000 + quota_requirement - 1,
        current_physical_quota_bytes=1_000,
        conservative_tape_bytes_per_h=10,
        finalize_reserve_bytes=20,
        quota_safety_margin_bytes=5,
        root_free_bytes=root_requirement,
        conservative_root_growth_bytes_per_h=12,
        root_emergency_floor_bytes=30,
    )
    assert quota_fail["quota_gate_met"] is False
    assert quota_fail["root_gate_met"] is True
    assert quota_fail["state"] == "YELLOW"

    root_fail = compute_gates(
        quota_bytes=1_000 + quota_requirement,
        current_physical_quota_bytes=1_000,
        conservative_tape_bytes_per_h=10,
        finalize_reserve_bytes=20,
        quota_safety_margin_bytes=5,
        root_free_bytes=root_requirement - 1,
        conservative_root_growth_bytes_per_h=12,
        root_emergency_floor_bytes=30,
    )
    assert root_fail["quota_gate_met"] is True
    assert root_fail["root_gate_met"] is False
    assert root_fail["state"] == "YELLOW"


def test_missing_root_history_is_yellow_not_zero_growth() -> None:
    result = compute_gates(
        quota_bytes=1_000,
        current_physical_quota_bytes=100,
        conservative_tape_bytes_per_h=1,
        finalize_reserve_bytes=1,
        quota_safety_margin_bytes=1,
        root_free_bytes=10_000,
        conservative_root_growth_bytes_per_h=None,
        root_emergency_floor_bytes=1,
    )
    assert result["quota_gate_met"] is True
    assert result["root_gate_met"] is None
    assert result["root_history_available"] is False
    assert result["state"] == "YELLOW"


def test_non_tape_growth_requires_trustworthy_interval_and_surface() -> None:
    short = conservative_growth_rate(
        [GrowthSample(0, 10, "root-a"), GrowthSample(1_000, 100, "root-a")]
    )
    changed_surface = conservative_growth_rate(
        [
            GrowthSample(0, 10, "root-a"),
            GrowthSample(3_600_000, 100, "root-b"),
        ]
    )
    measured = conservative_growth_rate(
        [
            GrowthSample(0, 10, "root-a"),
            GrowthSample(3_600_000, 110, "root-a"),
        ]
    )
    assert short is None
    assert changed_surface is None
    assert measured == 100


def test_all_normal_finalize_publish_states_converge() -> None:
    base = "100-00000001"
    states = [
        {f"{base}.jsonl.zst.partial", f"{base}.jsonl.zst.tmp"},
        {f"{base}.jsonl.zst.partial", f"{base}.jsonl.zst"},
        {
            f"{base}.jsonl.zst.partial",
            f"{base}.jsonl.zst",
            f"{base}.manifest.json.tmp",
        },
        {
            f"{base}.jsonl.zst.partial",
            f"{base}.jsonl.zst",
            f"{base}.manifest.json",
        },
        {f"{base}.jsonl.zst", f"{base}.manifest.json"},
    ]
    assert (
        classify_publish_sequence(states, writer_running=True)
        == "TRANSIENT_CONVERGED"
    )


def test_stable_or_stopped_publish_anomaly_fails_closed() -> None:
    files = {"100-00000001.jsonl.zst"}
    assert (
        classify_publish_sequence([files, files], writer_running=True)
        == "INTEGRITY_FAILURE"
    )
    transient = {
        "100-00000001.jsonl.zst.partial",
        "100-00000001.jsonl.zst.tmp",
    }
    assert (
        classify_publish_sequence([transient], writer_running=False)
        == "INTEGRITY_FAILURE"
    )
