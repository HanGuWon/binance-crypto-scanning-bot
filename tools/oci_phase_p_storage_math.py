"""Pure storage math and segmented publish-state helpers for Phase P."""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from math import ceil
from typing import Any, Literal

HOUR_MS = 3_600_000
BUCKET_MS = 300_000
MIN_GROWTH_INTERVAL_MS = 45 * 60 * 1000
DEFAULT_MANIFEST_SCHEMA_BOUND_BYTES = 64 * 1024

GateState = Literal["GREEN", "YELLOW"]
PublishState = Literal[
    "COHERENT",
    "TRANSIENT_RETRY",
    "TRANSIENT_CONVERGED",
    "INTEGRITY_FAILURE",
]


@dataclass(frozen=True)
class SegmentSample:
    """Physical fields needed to compute one retained segment contribution."""

    bucket_start_ms: int
    last_received_at_ms: int
    compressed_bytes: int
    manifest_bytes: int
    uncompressed_bytes: int
    healthy: bool = True

    @property
    def retained_bytes(self) -> int:
        return self.compressed_bytes + self.manifest_bytes


@dataclass(frozen=True)
class GrowthSample:
    """Timestamped byte count on one unchanged measurement surface."""

    captured_at_ms: int
    bytes_used: int
    surface_id: str


def compress_bound(size: int) -> int:
    """Return the production writer's deterministic zstd upper bound."""

    if size < 0:
        raise ValueError("size must be non-negative")
    return size + size // 8 + 128


def deterministic_finalize_reserve(
    active_partial_bytes: Iterable[int],
    *,
    manifest_schema_bound_bytes: int = DEFAULT_MANIFEST_SCHEMA_BOUND_BYTES,
) -> int:
    """Bound additional bytes while every active partial is finalized."""

    if manifest_schema_bound_bytes <= 0:
        raise ValueError("manifest schema bound must be positive")
    partials = tuple(active_partial_bytes)
    if any(value < 0 for value in partials):
        raise ValueError("partial bytes must be non-negative")
    return sum(
        compress_bound(value) + manifest_schema_bound_bytes
        for value in partials
        if value > 0
    )


def _rate(bytes_used: int, elapsed_ms: int) -> float | None:
    if bytes_used < 0:
        raise ValueError("bytes must be non-negative")
    if elapsed_ms <= 0:
        return None
    return bytes_used * HOUR_MS / elapsed_ms


def _nearest_rank_p95(values: Sequence[float]) -> float | None:
    if len(values) < 3:
        return None
    ordered = sorted(values)
    return ordered[max(0, ceil(0.95 * len(ordered)) - 1)]


def compute_tape_rates(
    *,
    activation_ms: int,
    captured_at_ms: int,
    segments: Sequence[SegmentSample],
    active_partials: Mapping[tuple[str, int], int],
) -> dict[str, float | int | None]:
    """Compute manifest-inclusive whole/recent/healthy/p95 tape rates."""

    elapsed_ms = captured_at_ms - activation_ms
    if elapsed_ms <= 0:
        raise ValueError("capture must follow activation")
    if any(value < 0 for value in active_partials.values()):
        raise ValueError("partial bytes must be non-negative")

    finalized_bytes = sum(item.retained_bytes for item in segments)
    partial_bytes = sum(active_partials.values())
    whole_rate = _rate(finalized_bytes + partial_bytes, elapsed_ms)
    finalized_rate = _rate(finalized_bytes, elapsed_ms)

    window_ms = min(HOUR_MS, elapsed_ms)
    window_start = captured_at_ms - window_ms
    recent_bytes = sum(
        item.retained_bytes
        for item in segments
        if item.last_received_at_ms >= window_start
    ) + sum(
        size
        for (_, bucket), size in active_partials.items()
        if bucket + BUCKET_MS >= window_start
    )
    recent_rate = _rate(recent_bytes, window_ms)
    healthy_bytes = sum(
        item.retained_bytes
        for item in segments
        if item.healthy and item.last_received_at_ms >= window_start
    ) + sum(
        size
        for (_, bucket), size in active_partials.items()
        if bucket + BUCKET_MS >= window_start
    )
    healthy_rate = _rate(healthy_bytes, window_ms)

    retained_by_bucket: dict[int, int] = defaultdict(int)
    for item in segments:
        retained_by_bucket[item.bucket_start_ms] += item.retained_bytes

    p95_windows: list[float] = []
    for terminal in sorted(retained_by_bucket):
        completed_start = terminal - (HOUR_MS - BUCKET_MS)
        expected_buckets = range(completed_start, terminal + BUCKET_MS, BUCKET_MS)
        if any(bucket not in retained_by_bucket for bucket in expected_buckets):
            continue
        retained = sum(
            value
            for bucket, value in retained_by_bucket.items()
            if completed_start <= bucket <= terminal
        )
        rate = _rate(retained, HOUR_MS)
        if rate is not None:
            p95_windows.append(rate)
    p95_rate = _nearest_rank_p95(p95_windows)

    candidates = [
        value
        for value in (whole_rate, recent_rate, healthy_rate, p95_rate)
        if value is not None and value > 0
    ]
    return {
        "finalized_bytes": finalized_bytes,
        "manifest_bytes": sum(item.manifest_bytes for item in segments),
        "active_partial_bytes": partial_bytes,
        "whole_clock_bytes_per_h": whole_rate,
        "finalized_bytes_per_h": finalized_rate,
        "recent_bytes_per_h": recent_rate,
        "healthy_bytes_per_h": healthy_rate,
        "p95_bytes_per_h": p95_rate,
        "observed_max_terminal_plaintext_bytes": max(
            (item.uncompressed_bytes for item in segments), default=0
        ),
        "conservative_tape_bytes_per_h": max(candidates) if candidates else None,
        "p95_window_count": len(p95_windows),
    }


def conservative_growth_rate(samples: Sequence[GrowthSample]) -> float | None:
    """Return maximum non-negative growth rate across trustworthy intervals."""

    rates: list[float] = []
    ordered = sorted(samples, key=lambda item: item.captured_at_ms)
    for index, first in enumerate(ordered):
        for second in ordered[index + 1 :]:
            elapsed = second.captured_at_ms - first.captured_at_ms
            if first.surface_id != second.surface_id:
                continue
            if elapsed < MIN_GROWTH_INTERVAL_MS:
                continue
            growth = max(second.bytes_used - first.bytes_used, 0)
            rate = _rate(growth, elapsed)
            if rate is not None:
                rates.append(rate)
    return max(rates) if rates else None


def compute_gates(
    *,
    quota_bytes: int,
    current_physical_quota_bytes: int,
    conservative_tape_bytes_per_h: float,
    finalize_reserve_bytes: int,
    quota_safety_margin_bytes: int,
    root_free_bytes: int,
    conservative_root_growth_bytes_per_h: float | None,
    root_emergency_floor_bytes: int,
    gate_hours: int = 48,
) -> dict[str, Any]:
    """Evaluate independent quota/root gates with unavailable root history."""

    numeric = (
        quota_bytes,
        current_physical_quota_bytes,
        finalize_reserve_bytes,
        quota_safety_margin_bytes,
        root_free_bytes,
        root_emergency_floor_bytes,
        gate_hours,
    )
    if any(value < 0 for value in numeric) or conservative_tape_bytes_per_h < 0:
        raise ValueError("gate inputs must be non-negative")
    quota_headroom = max(quota_bytes - current_physical_quota_bytes, 0)
    quota_required = (
        conservative_tape_bytes_per_h * gate_hours
        + finalize_reserve_bytes
        + quota_safety_margin_bytes
    )
    quota_met = quota_headroom >= quota_required
    root_required = None
    root_met = None
    if conservative_root_growth_bytes_per_h is not None:
        if conservative_root_growth_bytes_per_h < 0:
            raise ValueError("root growth rate must be non-negative")
        root_required = (
            conservative_root_growth_bytes_per_h * gate_hours
            + root_emergency_floor_bytes
        )
        root_met = root_free_bytes >= root_required
    state: GateState = "GREEN" if quota_met and root_met is True else "YELLOW"
    return {
        "gate_hours": gate_hours,
        "quota_headroom_bytes": quota_headroom,
        "quota_required_bytes": quota_required,
        "quota_gate_met": quota_met,
        "root_required_bytes": root_required,
        "root_gate_met": root_met,
        "root_history_available": root_met is not None,
        "state": state,
    }


_SUFFIXES = (
    ".jsonl.zst.partial",
    ".jsonl.zst.tmp",
    ".jsonl.zst",
    ".manifest.json.tmp",
    ".manifest.json",
)


def _split_segment_name(name: str) -> tuple[str, str] | None:
    for suffix in _SUFFIXES:
        if name.endswith(suffix):
            return name[: -len(suffix)], suffix
    return None


def publish_snapshot_state(names: Iterable[str]) -> PublishState:
    """Classify one segmented-writer filesystem snapshot."""

    grouped: dict[str, set[str]] = defaultdict(set)
    for name in names:
        split = _split_segment_name(name)
        if split is not None:
            grouped[split[0]].add(split[1])
    transient = False
    for suffixes in grouped.values():
        if suffixes in (
            {".jsonl.zst.partial"},
            {".jsonl.zst", ".manifest.json"},
        ):
            continue
        if ".jsonl.zst.partial" in suffixes and suffixes <= {
            ".jsonl.zst.partial",
            ".jsonl.zst.tmp",
            ".jsonl.zst",
            ".manifest.json.tmp",
            ".manifest.json",
        }:
            transient = True
            continue
        return "INTEGRITY_FAILURE"
    return "TRANSIENT_RETRY" if transient else "COHERENT"


def classify_publish_sequence(
    snapshots: Sequence[Iterable[str]], *, writer_running: bool
) -> PublishState:
    """Classify bounded retry snapshots without false finalize corruption."""

    if not snapshots:
        raise ValueError("at least one snapshot is required")
    states = [publish_snapshot_state(item) for item in snapshots]
    if not writer_running and any(state != "COHERENT" for state in states):
        return "INTEGRITY_FAILURE"
    if states[-1] == "COHERENT":
        return "TRANSIENT_CONVERGED" if "TRANSIENT_RETRY" in states else "COHERENT"
    if "INTEGRITY_FAILURE" in states:
        return "INTEGRITY_FAILURE"
    normalized = [frozenset(item) for item in snapshots]
    return (
        "TRANSIENT_RETRY"
        if len(set(normalized)) > 1
        else "INTEGRITY_FAILURE"
    )
