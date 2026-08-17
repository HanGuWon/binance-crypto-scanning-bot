from __future__ import annotations

import hashlib
from dataclasses import replace

import pytest
from hypothesis import given
from hypothesis import strategies as st

from signalbot.r4b_v2.capture.spot_snapshot_qualification import (
    SPOT_CERTIFIED_NOTIONAL_MIN_MICRO_USDT_V2,
    SPOT_RECOVERY_MAXIMUM_MS_V2,
    SPOT_RECOVERY_P99_MAX_MS_V2,
    SPOT_SNAPSHOT_LIMIT_GRID_V2,
    SPOT_SNAPSHOT_QUALIFICATION_DURATION_MS_V2,
    SPOT_SNAPSHOT_WEIGHT_GRID_V2,
    SpotFinalPanelRecoveryProofV2,
    SpotRestWeightBudgetEvidenceV2,
    SpotSnapshotCandidateQualificationV2,
    SpotSnapshotQualificationError,
    SpotSnapshotQualificationRunV2,
    SpotSnapshotQualificationSampleV2,
    SpotSnapshotT0BlockedError,
    qualify_spot_snapshot_candidate_v2,
    select_spot_snapshot_limits_v2,
)

WINDOW_START_MS = 2_000_000_000_000
WINDOW_END_MS = WINDOW_START_MS + SPOT_SNAPSHOT_QUALIFICATION_DURATION_MS_V2
SELECTION_MS = WINDOW_END_MS
H_START_MS = SELECTION_MS + 60_000
NOTIONAL_MIN = SPOT_CERTIFIED_NOTIONAL_MIN_MICRO_USDT_V2


def _hash(label: str) -> str:
    return hashlib.sha256(label.encode()).hexdigest()


def _candidate(
    symbol: str,
    limit: int,
    *,
    sample_count: int = 10_000,
    complete_count: int = 9_999,
    bid_p1: int = NOTIONAL_MIN,
    ask_p1: int = NOTIONAL_MIN,
) -> SpotSnapshotCandidateQualificationV2:
    weights: dict[int, int] = dict(
        zip(SPOT_SNAPSHOT_LIMIT_GRID_V2, SPOT_SNAPSHOT_WEIGHT_GRID_V2, strict=True)
    )
    weight = weights[limit]
    return SpotSnapshotCandidateQualificationV2(
        symbol=symbol,
        limit=limit,
        request_weight=weight,
        sample_count=sample_count,
        two_sided_10bp_complete_count=complete_count,
        bid_first_percentile_haircutted_notional_micro_usdt=bid_p1,
        ask_first_percentile_haircutted_notional_micro_usdt=ask_p1,
        measurement_root_sha256=_hash(f"{symbol}:{limit}:{sample_count}:{complete_count}:{bid_p1}:{ask_p1}"),
    )


def _grid(
    symbol: str,
    first_passing_limit: int | None,
) -> tuple[SpotSnapshotCandidateQualificationV2, ...]:
    return tuple(
        _candidate(
            symbol,
            limit,
            bid_p1=(
                NOTIONAL_MIN
                if first_passing_limit is not None and limit >= first_passing_limit
                else NOTIONAL_MIN - 1
            ),
        )
        for limit in SPOT_SNAPSHOT_LIMIT_GRID_V2
    )


def _budget(*, total: int = 250, limit: int = 1_000) -> SpotRestWeightBudgetEvidenceV2:
    return SpotRestWeightBudgetEvidenceV2(
        smallest_active_minute_weight_limit=limit,
        snapshots_weight=total,
        retries_weight=0,
        rule_polling_weight=0,
        reference_bootstrap_weight=0,
        block_trade_backfill_weight=0,
        measurement_root_sha256=_hash(f"budget:{limit}:{total}"),
    )


def _recovery(
    *,
    p99_ms: int = SPOT_RECOVERY_P99_MAX_MS_V2,
    maximum_ms: int = SPOT_RECOVERY_MAXIMUM_MS_V2,
    unresolved_gap_count: int = 0,
    reverse: bool = False,
) -> SpotFinalPanelRecoveryProofV2:
    durations = [p99_ms] * 100 + [maximum_ms]
    if reverse:
        durations.reverse()
    return SpotFinalPanelRecoveryProofV2(
        all_symbol_recovery_durations_ms=tuple(durations),
        unresolved_gap_count=unresolved_gap_count,
        measurement_root_sha256=_hash("recovery-proof"),
    )


def _run(
    *,
    candidates: tuple[SpotSnapshotCandidateQualificationV2, ...] | None = None,
    budget: SpotRestWeightBudgetEvidenceV2 | None = None,
    recovery: SpotFinalPanelRecoveryProofV2 | None = None,
    reverse_candidates: bool = False,
    **overrides: object,
) -> SpotSnapshotQualificationRunV2:
    cells = candidates or (*_grid("BTCUSDT", 500), *_grid("ETHUSDT", 1000))
    if reverse_candidates:
        cells = tuple(reversed(cells))
    values: dict[str, object] = {
        "qualification_id": "synthetic-spot-final-panel-q1",
        "window_start_wall_ms": WINDOW_START_MS,
        "window_end_wall_ms": WINDOW_END_MS,
        "actual_final_panel_sha256": "a" * 64,
        "source_manifest_sha256": "b" * 64,
        "runtime_manifest_sha256": "c" * 64,
        "engineering_only": True,
        "strategy_signal_or_return_data_accessed": False,
        "candidates": cells,
        "rest_weight_budget": budget or _budget(),
        "recovery_proof": recovery or _recovery(),
    }
    values.update(overrides)
    return SpotSnapshotQualificationRunV2(**values)  # type: ignore[arg-type]


def _sample(
    sample_id: str,
    *,
    bid: int = NOTIONAL_MIN,
    ask: int = NOTIONAL_MIN,
) -> SpotSnapshotQualificationSampleV2:
    return SpotSnapshotQualificationSampleV2(
        sample_id=sample_id,
        bid_10bp_complete=True,
        ask_10bp_complete=True,
        bid_haircutted_certified_notional_micro_usdt=bid,
        ask_haircutted_certified_notional_micro_usdt=ask,
    )


def test_documented_limit_and_weight_grid_is_frozen() -> None:
    assert SPOT_SNAPSHOT_LIMIT_GRID_V2 == (100, 500, 1000, 5000)
    assert SPOT_SNAPSHOT_WEIGHT_GRID_V2 == (5, 25, 50, 250)
    for limit, weight in zip(
        SPOT_SNAPSHOT_LIMIT_GRID_V2,
        SPOT_SNAPSHOT_WEIGHT_GRID_V2,
        strict=True,
    ):
        assert _candidate("BTCUSDT", limit).request_weight == weight


def test_exact_candidate_boundaries_pass_and_one_quantum_fails() -> None:
    boundary = _candidate("BTCUSDT", 100)
    assert boundary.passed

    completeness_fail = replace(boundary, two_sided_10bp_complete_count=9_998)
    bid_fail = replace(
        boundary,
        bid_first_percentile_haircutted_notional_micro_usdt=NOTIONAL_MIN - 1,
    )
    ask_fail = replace(
        boundary,
        ask_first_percentile_haircutted_notional_micro_usdt=NOTIONAL_MIN - 1,
    )
    assert completeness_fail.failed_gates == ("two_sided_10bp_completeness",)
    assert bid_fail.failed_gates == (
        "bid_first_percentile_haircutted_certified_notional",
    )
    assert ask_fail.failed_gates == (
        "ask_first_percentile_haircutted_certified_notional",
    )


def test_nearest_rank_first_percentile_uses_ceil_rank_and_each_side() -> None:
    one_low = tuple(
        [_sample("000", bid=NOTIONAL_MIN - 1), *(_sample(f"{index:03}") for index in range(1, 101))]
    )
    two_low = (
        _sample("000", bid=NOTIONAL_MIN - 1),
        _sample("001", bid=NOTIONAL_MIN - 1),
        *(_sample(f"{index:03}") for index in range(2, 101)),
    )

    passing = qualify_spot_snapshot_candidate_v2(
        symbol="BTCUSDT",
        limit=100,
        samples=one_low,
    )
    failing = qualify_spot_snapshot_candidate_v2(
        symbol="BTCUSDT",
        limit=100,
        samples=two_low,
    )
    assert passing.bid_first_percentile_haircutted_notional_micro_usdt == NOTIONAL_MIN
    assert passing.passed
    assert failing.bid_first_percentile_haircutted_notional_micro_usdt == NOTIONAL_MIN - 1
    assert not failing.passed


def test_two_sided_completeness_requires_both_sides_for_each_sample() -> None:
    samples = (
        _sample("complete"),
        replace(_sample("bid-only"), ask_10bp_complete=False),
        replace(_sample("ask-only"), bid_10bp_complete=False),
    )
    candidate = qualify_spot_snapshot_candidate_v2(
        symbol="BTCUSDT",
        limit=500,
        samples=samples,
    )
    assert candidate.two_sided_10bp_complete_count == 1
    assert not candidate.passed


def test_duplicate_sample_ids_are_rejected_before_metric_derivation() -> None:
    with pytest.raises(SpotSnapshotQualificationError, match="sample IDs must be unique"):
        qualify_spot_snapshot_candidate_v2(
            symbol="BTCUSDT",
            limit=500,
            samples=(_sample("duplicate"), _sample("duplicate")),
        )


def test_smallest_passing_limit_is_selected_per_symbol() -> None:
    run = _run()
    receipt = select_spot_snapshot_limits_v2(
        run,
        selection_wall_ms=SELECTION_MS,
        h_start_wall_ms=H_START_MS,
    )

    assert receipt.status == "SELECTED"
    assert tuple(
        (item.symbol, item.selected_limit, item.request_weight)
        for item in receipt.selections
    ) == (("BTCUSDT", 500, 25), ("ETHUSDT", 1000, 50))
    receipt.require_selected_limit("BTCUSDT", 500)
    with pytest.raises(SpotSnapshotQualificationError, match="differs"):
        receipt.require_selected_limit("BTCUSDT", 1000)


def test_no_passing_limit_blocks_t0_without_partial_runtime_selection() -> None:
    run = _run(candidates=(*_grid("BTCUSDT", 500), *_grid("ETHUSDT", None)))
    receipt = select_spot_snapshot_limits_v2(
        run,
        selection_wall_ms=SELECTION_MS,
        h_start_wall_ms=None,
    )

    assert receipt.status == "T0_BLOCKED_SPOT_SNAPSHOT_QUALIFICATION"
    assert receipt.selections == ()
    assert receipt.ineligible_symbols == ("ETHUSDT",)
    assert receipt.failed_gates == ("symbol:ETHUSDT:no_passing_limit",)
    with pytest.raises(SpotSnapshotT0BlockedError, match="T0 is blocked"):
        receipt.require_selected_limit("BTCUSDT", 500)
    with pytest.raises(SpotSnapshotQualificationError, match="H_start must remain unset"):
        select_spot_snapshot_limits_v2(
            run,
            selection_wall_ms=SELECTION_MS,
            h_start_wall_ms=H_START_MS,
        )


def test_rest_budget_equality_passes_and_one_weight_quantum_fails() -> None:
    assert _budget(total=250, limit=1000).passed
    assert not _budget(total=251, limit=1000).passed

    receipt = select_spot_snapshot_limits_v2(
        _run(budget=_budget(total=251, limit=1000)),
        selection_wall_ms=SELECTION_MS,
        h_start_wall_ms=None,
    )
    assert receipt.failed_gates == ("REST_weight_budget",)


def test_rest_budget_total_includes_every_required_public_rest_role() -> None:
    boundary = SpotRestWeightBudgetEvidenceV2(
        smallest_active_minute_weight_limit=1_000,
        snapshots_weight=50,
        retries_weight=50,
        rule_polling_weight=50,
        reference_bootstrap_weight=50,
        block_trade_backfill_weight=50,
        measurement_root_sha256=_hash("all-rest-roles"),
    )
    assert boundary.total_weight == 250
    assert boundary.passed
    assert not replace(boundary, retries_weight=51).passed


@pytest.mark.parametrize(
    ("recovery", "failed_gate"),
    [
        (
            _recovery(p99_ms=SPOT_RECOVERY_P99_MAX_MS_V2 + 1),
            "p99_all_symbol_recovery",
        ),
        (
            _recovery(maximum_ms=SPOT_RECOVERY_MAXIMUM_MS_V2 + 1),
            "maximum_all_symbol_recovery",
        ),
        (_recovery(unresolved_gap_count=1), "unresolved_gap_count"),
    ],
)
def test_recovery_exact_boundaries_pass_and_one_quantum_fails(
    recovery: SpotFinalPanelRecoveryProofV2,
    failed_gate: str,
) -> None:
    boundary = _recovery()
    assert boundary.p99_recovery_ms == SPOT_RECOVERY_P99_MAX_MS_V2
    assert boundary.maximum_recovery_ms == SPOT_RECOVERY_MAXIMUM_MS_V2
    assert boundary.passed
    assert failed_gate in recovery.failed_gates


def test_nonengineering_inputs_and_incomplete_grid_are_rejected() -> None:
    with pytest.raises(SpotSnapshotQualificationError, match="engineering_only"):
        _run(engineering_only=False)
    with pytest.raises(SpotSnapshotQualificationError, match="strategy signals"):
        _run(strategy_signal_or_return_data_accessed=True)
    with pytest.raises(SpotSnapshotQualificationError, match="every frozen snapshot limit"):
        _run(candidates=_grid("BTCUSDT", 100)[:-1])
    with pytest.raises(SpotSnapshotQualificationError, match="documented Spot depth weight"):
        replace(_candidate("BTCUSDT", 100), request_weight=25)


@pytest.mark.parametrize("duration_delta_ms", [-1, 1])
def test_qualification_window_is_exactly_24_complete_hours(
    duration_delta_ms: int,
) -> None:
    with pytest.raises(SpotSnapshotQualificationError, match="exactly 24 complete hours"):
        _run(window_end_wall_ms=WINDOW_END_MS + duration_delta_ms)


def test_selection_timing_must_follow_qualification_and_precede_h_start() -> None:
    run = _run()
    with pytest.raises(SpotSnapshotQualificationError, match="cannot precede"):
        select_spot_snapshot_limits_v2(
            run,
            selection_wall_ms=WINDOW_END_MS - 1,
            h_start_wall_ms=H_START_MS,
        )
    with pytest.raises(SpotSnapshotQualificationError, match="strictly precede"):
        select_spot_snapshot_limits_v2(
            run,
            selection_wall_ms=H_START_MS,
            h_start_wall_ms=H_START_MS,
        )


def test_candidate_run_and_receipt_hashes_are_order_independent() -> None:
    samples = tuple(_sample(f"sample-{index}") for index in range(20))
    forward_candidate = qualify_spot_snapshot_candidate_v2(
        symbol="BTCUSDT",
        limit=100,
        samples=samples,
    )
    reversed_candidate = qualify_spot_snapshot_candidate_v2(
        symbol="BTCUSDT",
        limit=100,
        samples=reversed(samples),
    )
    assert forward_candidate == reversed_candidate

    forward_run = _run(reverse_candidates=False, recovery=_recovery(reverse=False))
    reversed_run = _run(reverse_candidates=True, recovery=_recovery(reverse=True))
    assert forward_run.sha256 == reversed_run.sha256

    forward_receipt = select_spot_snapshot_limits_v2(
        forward_run,
        selection_wall_ms=SELECTION_MS,
        h_start_wall_ms=H_START_MS,
    )
    reversed_receipt = select_spot_snapshot_limits_v2(
        reversed_run,
        selection_wall_ms=SELECTION_MS,
        h_start_wall_ms=H_START_MS,
    )
    assert forward_receipt.sha256 == reversed_receipt.sha256


@given(st.permutations(tuple(_sample(f"property-{index}") for index in range(8))))
def test_property_sample_shuffle_cannot_change_metrics_or_measurement_root(
    shuffled: list[SpotSnapshotQualificationSampleV2],
) -> None:
    baseline = qualify_spot_snapshot_candidate_v2(
        symbol="BTCUSDT",
        limit=500,
        samples=tuple(sorted(shuffled, key=lambda item: item.sample_id)),
    )
    observed = qualify_spot_snapshot_candidate_v2(
        symbol="BTCUSDT",
        limit=500,
        samples=shuffled,
    )
    assert observed == baseline


@given(
    sample_count=st.integers(min_value=1, max_value=100_000),
    complete_count=st.integers(min_value=0, max_value=100_000),
)
def test_property_completeness_gate_is_exact_integer_cross_multiplication(
    sample_count: int,
    complete_count: int,
) -> None:
    if complete_count > sample_count:
        return
    candidate = _candidate(
        "BTCUSDT",
        100,
        sample_count=sample_count,
        complete_count=complete_count,
    )
    expected = complete_count * 10_000 >= sample_count * 9_999
    assert ("two_sided_10bp_completeness" not in candidate.failed_gates) is expected
