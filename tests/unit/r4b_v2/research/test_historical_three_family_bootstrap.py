from __future__ import annotations

import hashlib
from dataclasses import dataclass, replace

import pytest

from signalbot.backtest.historical_three_family_census import (
    HISTORICAL_THREE_FAMILY_CENSUS_PROTOCOL_V2,
)
from signalbot.backtest.historical_three_family_outcomes import (
    HISTORICAL_THREE_FAMILY_FIXED_HORIZON_RUNNER_PROTOCOL_V2,
)
from signalbot.backtest.historical_three_family_report import (
    HISTORICAL_THREE_FAMILY_REPORT_VERSION_V2,
    HistoricalThreeFamilyReportErrorV2,
    historical_three_family_report_sha256_v2,
    render_historical_three_family_report_ko_v2,
)
from signalbot.backtest.historical_three_family_te0 import (
    HISTORICAL_THREE_FAMILY_TE0_PROTOCOL_V2,
)
from signalbot.r4b_v2.capture.models import VenueV2
from signalbot.r4b_v2.research.historical_three_family_bootstrap import (
    HISTORICAL_THREE_FAMILY_BOOTSTRAP_BLOCK_DAYS_V2,
    HISTORICAL_THREE_FAMILY_BOOTSTRAP_MIN_EVALUABLE_V2,
    HISTORICAL_THREE_FAMILY_BOOTSTRAP_SAMPLES_V2,
    HISTORICAL_THREE_FAMILY_BOOTSTRAP_SEED_V2,
    HISTORICAL_THREE_FAMILY_BOOTSTRAP_VERSION_V2,
    HISTORICAL_THREE_FAMILY_CONFLICTED_OUTCOME_PROTOCOL_V2,
    HistoricalExactRationalV2,
    HistoricalThreeFamilyBootstrapBucketV2,
    HistoricalThreeFamilyBootstrapComparisonV2,
    HistoricalThreeFamilyBootstrapErrorV2,
    HistoricalThreeFamilyBootstrapFeasibilityV2,
    HistoricalThreeFamilyBootstrapMetricV2,
    HistoricalThreeFamilyConflictedOutcomeV2,
    HistoricalThreeFamilyCostAttributionV2,
    HistoricalThreeFamilyCostSourceV2,
    bootstrap_historical_three_family_outcomes_v2,
    build_historical_three_family_bootstrap_schedule_v2,
    canonical_historical_three_family_bootstrap_v2,
    cost_attribution_from_fixed_horizon_row_v2,
)
from signalbot.r4b_v2.research.historical_three_family_outcome_audit import (
    HISTORICAL_THREE_FAMILY_OUTCOME_HORIZONS_BARS_V2,
    HISTORICAL_THREE_FAMILY_OUTCOME_PROTOCOL_V2,
    HistoricalThreeFamilyOutcomeV2,
    HistoricalThreeFamilyProfitFactorStateV2,
    HistoricalThreeFamilySideV2,
)
from signalbot.r4b_v2.research.historical_three_family_topology import (
    HISTORICAL_THREE_FAMILY_TOPOLOGY_RULE_VERSION_V2,
)
from signalbot.r4b_v2.strategy.directional_evidence import DirectionalStateClassV2
from signalbot.r4b_v2.strategy.historical_three_family_consensus import (
    HISTORICAL_THREE_FAMILY_CONSENSUS_RULE_VERSION_V2,
)

DAY_MS = 86_400_000
CALENDAR_START_MS = 1_719_792_000_000
EXECUTION_SHA = "7" * 64


def _event_id(label: str) -> str:
    return hashlib.sha256(label.encode()).hexdigest()


def _primary_event(
    label: str,
    *,
    state: DirectionalStateClassV2,
    day: int,
    values: tuple[int | None, ...],
    offset_ms: int = 1_000,
) -> tuple[HistoricalThreeFamilyOutcomeV2, ...]:
    assert len(values) == len(HISTORICAL_THREE_FAMILY_OUTCOME_HORIZONS_BARS_V2)
    bullish = state in {
        DirectionalStateClassV2.BULLISH_STATE_TILT,
        DirectionalStateClassV2.BROAD_BULLISH_STATE,
    }
    return tuple(
        HistoricalThreeFamilyOutcomeV2(
            event_id=_event_id(label),
            outcome_protocol_version=HISTORICAL_THREE_FAMILY_OUTCOME_PROTOCOL_V2,
            rule_version=HISTORICAL_THREE_FAMILY_CONSENSUS_RULE_VERSION_V2,
            execution_contract_sha256=EXECUTION_SHA,
            venue=VenueV2.USDM_FUTURES,
            symbol="BTCUSDT",
            decision_time_ms=CALENDAR_START_MS + day * DAY_MS + offset_ms,
            state_class=state,
            directional_agreement_micros=700_000 if bullish else -700_000,
            horizon_bars=horizon,
            evaluable=value is not None,
            exclusion_reason="" if value is not None else "DATA_GAP_IN_HORIZON",
            net_return_micros=value,
        )
        for horizon, value in zip(
            HISTORICAL_THREE_FAMILY_OUTCOME_HORIZONS_BARS_V2,
            values,
            strict=True,
        )
    )


def _conflicted_event(
    label: str,
    *,
    side: HistoricalThreeFamilySideV2,
    day: int,
    values: tuple[int | None, ...],
    agreement: int | None = None,
) -> tuple[HistoricalThreeFamilyConflictedOutcomeV2, ...]:
    return tuple(
        HistoricalThreeFamilyConflictedOutcomeV2(
            event_id=_event_id(label),
            comparator_protocol_version=(
                HISTORICAL_THREE_FAMILY_CONFLICTED_OUTCOME_PROTOCOL_V2
            ),
            topology_rule_version=HISTORICAL_THREE_FAMILY_TOPOLOGY_RULE_VERSION_V2,
            execution_contract_sha256=EXECUTION_SHA,
            symbol="ETHUSDT",
            decision_time_ms=CALENDAR_START_MS + day * DAY_MS + 2_000,
            side=side,
            directional_agreement_micros=(
                agreement
                if agreement is not None
                else (400_000 if side is HistoricalThreeFamilySideV2.BULLISH else -400_000)
            ),
            horizon_bars=horizon,
            evaluable=value is not None,
            exclusion_reason="" if value is not None else "DATA_GAP_IN_HORIZON",
            net_return_micros=value,
        )
        for horizon, value in zip(
            HISTORICAL_THREE_FAMILY_OUTCOME_HORIZONS_BARS_V2,
            values,
            strict=True,
        )
    )


def _primary_rows() -> tuple[HistoricalThreeFamilyOutcomeV2, ...]:
    return (
        *_primary_event(
            "clean",
            state=DirectionalStateClassV2.BULLISH_STATE_TILT,
            day=0,
            values=(100, 110, 120, 130, 140),
        ),
        *_primary_event(
            "broad",
            state=DirectionalStateClassV2.BROAD_BULLISH_STATE,
            day=0,
            values=(300, 320, 340, 360, 380),
        ),
    )


def _run(
    rows: tuple[HistoricalThreeFamilyOutcomeV2, ...] | None = None,
    **kwargs: object,
):
    return bootstrap_historical_three_family_outcomes_v2(
        _primary_rows() if rows is None else rows,
        calendar_start_ms=CALENDAR_START_MS,
        calendar_end_ms=CALENDAR_START_MS + 7 * DAY_MS,
        **kwargs,  # type: ignore[arg-type]
    )


def _cell(result, bucket: HistoricalThreeFamilyBootstrapBucketV2):
    return next(
        cell
        for cell in result.cells
        if cell.horizon_bars == 1
        and cell.side is HistoricalThreeFamilySideV2.BULLISH
        and cell.bucket is bucket
    )


def _endpoint(values, metric: HistoricalThreeFamilyBootstrapMetricV2):
    return next(endpoint for endpoint in values if endpoint.metric is metric)


def test_exact_shared_schedule_is_order_invariant_and_hash_bound() -> None:
    rows = _primary_rows()
    schedule = build_historical_three_family_bootstrap_schedule_v2(
        calendar_start_ms=CALENDAR_START_MS,
        calendar_end_ms=CALENDAR_START_MS + 7 * DAY_MS,
    )
    first = _run(rows)
    reversed_result = _run(tuple(reversed(rows)))

    assert first == reversed_result
    assert first.bootstrap_version == HISTORICAL_THREE_FAMILY_BOOTSTRAP_VERSION_V2
    assert first.block_days == HISTORICAL_THREE_FAMILY_BOOTSTRAP_BLOCK_DAYS_V2 == 7
    assert first.samples == HISTORICAL_THREE_FAMILY_BOOTSTRAP_SAMPLES_V2 == 10_000
    assert first.seed == HISTORICAL_THREE_FAMILY_BOOTSTRAP_SEED_V2 == 20_260_720
    assert (
        first.minimum_evaluable_per_cell
        == HISTORICAL_THREE_FAMILY_BOOTSTRAP_MIN_EVALUABLE_V2
        == 30
    )
    assert first.primary_event_count == 2
    assert first.primary_outcome_count == 10
    assert first.conflicted_event_count == 0
    assert first.cost_attribution_rows_sha256 is None
    assert not first.cost_attribution_complete
    assert first.shared_draw_schedule_sha256 == (
        "e13fc4b87a25616183ca3d3d230c67cf6d691f7bfb53974664bb630f0767a891"
    )
    assert schedule.schedule_sha256 == first.shared_draw_schedule_sha256
    assert schedule.outcome_data_used is False
    assert schedule.artifact()["shared_across_all_horizons_sides_buckets_metrics"] is True
    assert first.primary_rows_sha256 == (
        "a1d23e2db1f9390361d85720dd96958a69124fd6c42025599a9d9cfe55d4293a"
    )
    assert first.artifact_sha256 == (
        "a61bc2ecf40a0fadcc5df74e826f15ce2ad0d2b6ea515c403dc26d89da751134"
    )
    assert len(first.cells) == 30
    assert len(first.contrasts) == 10
    assert canonical_historical_three_family_bootstrap_v2(first).endswith(b"\n")
    assert hashlib.sha256(canonical_historical_three_family_bootstrap_v2(first)).hexdigest()
    hashes = {
        endpoint.shared_draw_schedule_sha256
        for cell in first.cells
        for endpoint in cell.endpoints
    }
    hashes.update(cell.shared_draw_schedule_sha256 for cell in first.cells)
    hashes.update(contrast.shared_draw_schedule_sha256 for contrast in first.contrasts)
    assert hashes == {first.shared_draw_schedule_sha256}
    assert first.historical_only
    assert first.exposed_retrospective_only
    assert first.zero_alert_days_retained
    assert first.shared_draws_across_all_cells
    assert not first.conflicted_pooled_with_clean
    assert not first.inference_complete
    assert not first.multiplicity_adjusted
    assert not first.efficacy_validated
    assert not first.probability
    assert not first.probability_calibrated
    assert not first.promoting
    assert not first.order_placement


def test_point_mean_hit_profit_factor_and_positive_contrast_are_exact() -> None:
    result = _run()
    clean = _cell(result, HistoricalThreeFamilyBootstrapBucketV2.CLEAN_2_PLUS_NEUTRAL)
    broad = _cell(result, HistoricalThreeFamilyBootstrapBucketV2.BROAD_3_OF_3)
    empty = _cell(result, HistoricalThreeFamilyBootstrapBucketV2.CONFLICTED_2_VS_1)

    assert clean.events == clean.evaluable == 1
    assert clean.zero_alert_days == 6
    assert clean.feasibility is HistoricalThreeFamilyBootstrapFeasibilityV2.INCONCLUSIVE_SPARSE
    mean = _endpoint(clean.endpoints, HistoricalThreeFamilyBootstrapMetricV2.MEAN_NET_RETURN_MICROS)
    hit = _endpoint(
        clean.endpoints,
        HistoricalThreeFamilyBootstrapMetricV2.STRICT_AFTER_COST_HIT_RATE_MICROS,
    )
    profit_factor = _endpoint(
        clean.endpoints,
        HistoricalThreeFamilyBootstrapMetricV2.PROFIT_FACTOR_MICROS,
    )
    assert mean.point_estimate == HistoricalExactRationalV2("100", "1")
    assert mean.two_sided_percentile_95_interval == (
        HistoricalExactRationalV2("100", "1"),
        HistoricalExactRationalV2("100", "1"),
    )
    assert mean.valid_replicates == 10_000
    assert hit.point_estimate == HistoricalExactRationalV2("1000000", "1")
    assert clean.profit_factor_state is (
        HistoricalThreeFamilyProfitFactorStateV2.POSITIVE_WITH_NO_LOSSES
    )
    assert profit_factor.point_estimate is None
    assert profit_factor.valid_replicates == 0
    assert profit_factor.invalid_replicates == 10_000
    assert broad.events == 1
    assert empty.events == empty.evaluable == 0
    assert empty.zero_alert_days == 7
    assert empty.feasibility is HistoricalThreeFamilyBootstrapFeasibilityV2.INCONCLUSIVE_EMPTY

    contrast = next(
        item
        for item in result.contrasts
        if item.horizon_bars == 1
        and item.side is HistoricalThreeFamilySideV2.BULLISH
    )
    contrast_mean = _endpoint(
        contrast.endpoints,
        HistoricalThreeFamilyBootstrapMetricV2.MEAN_NET_RETURN_MICROS,
    )
    assert contrast.comparison is (
        HistoricalThreeFamilyBootstrapComparisonV2.BROAD_MINUS_CLEAN_2_PLUS_NEUTRAL
    )
    assert contrast.feasibility is (
        HistoricalThreeFamilyBootstrapFeasibilityV2.INCONCLUSIVE_SPARSE
    )
    assert not contrast.comparison_interpretable
    assert contrast_mean.point_estimate == HistoricalExactRationalV2("200", "1")
    assert contrast_mean.one_sided_basic_95_lower == HistoricalExactRationalV2(
        "200", "1"
    )
    assert contrast_mean.null_centered_one_sided_p_value == HistoricalExactRationalV2(
        "1", "10001"
    )


def test_unevaluable_alert_is_missing_not_zero_but_not_a_zero_alert_day() -> None:
    rows = _primary_event(
        "missing",
        state=DirectionalStateClassV2.BROAD_BULLISH_STATE,
        day=0,
        values=(None, None, None, None, None),
    )
    result = _run(rows)
    broad = _cell(result, HistoricalThreeFamilyBootstrapBucketV2.BROAD_3_OF_3)
    mean = _endpoint(broad.endpoints, HistoricalThreeFamilyBootstrapMetricV2.MEAN_NET_RETURN_MICROS)

    assert broad.events == 1
    assert broad.evaluable == 0
    assert broad.zero_alert_days == 6
    assert mean.point_estimate is None
    assert mean.valid_replicates == 0
    assert mean.invalid_replicates == 10_000


def test_finite_profit_factor_hit_rate_and_nonpositive_contrast_p_value() -> None:
    rows = (
        *_primary_event(
            "finite-clean-gain",
            state=DirectionalStateClassV2.BULLISH_STATE_TILT,
            day=0,
            values=(100, 100, 100, 100, 100),
            offset_ms=1_000,
        ),
        *_primary_event(
            "finite-clean-loss",
            state=DirectionalStateClassV2.BULLISH_STATE_TILT,
            day=0,
            values=(-50, -50, -50, -50, -50),
            offset_ms=2_000,
        ),
        *_primary_event(
            "finite-broad",
            state=DirectionalStateClassV2.BROAD_BULLISH_STATE,
            day=0,
            values=(-100, -100, -100, -100, -100),
            offset_ms=3_000,
        ),
    )
    result = _run(rows)
    clean = _cell(result, HistoricalThreeFamilyBootstrapBucketV2.CLEAN_2_PLUS_NEUTRAL)
    assert clean.profit_factor_state is HistoricalThreeFamilyProfitFactorStateV2.FINITE
    assert _endpoint(
        clean.endpoints,
        HistoricalThreeFamilyBootstrapMetricV2.MEAN_NET_RETURN_MICROS,
    ).point_estimate == HistoricalExactRationalV2("25", "1")
    assert _endpoint(
        clean.endpoints,
        HistoricalThreeFamilyBootstrapMetricV2.STRICT_AFTER_COST_HIT_RATE_MICROS,
    ).point_estimate == HistoricalExactRationalV2("500000", "1")
    assert _endpoint(
        clean.endpoints,
        HistoricalThreeFamilyBootstrapMetricV2.PROFIT_FACTOR_MICROS,
    ).point_estimate == HistoricalExactRationalV2("2000000", "1")
    contrast = next(
        item
        for item in result.contrasts
        if item.horizon_bars == 1
        and item.side is HistoricalThreeFamilySideV2.BULLISH
    )
    mean = _endpoint(
        contrast.endpoints,
        HistoricalThreeFamilyBootstrapMetricV2.MEAN_NET_RETURN_MICROS,
    )
    assert mean.point_estimate == HistoricalExactRationalV2("-125", "1")
    assert mean.null_centered_one_sided_p_value == HistoricalExactRationalV2("1", "1")


def test_minimum_evaluable_boundary_is_descriptive_but_never_inferential() -> None:
    rows = tuple(
        outcome
        for index in range(HISTORICAL_THREE_FAMILY_BOOTSTRAP_MIN_EVALUABLE_V2)
        for outcome in _primary_event(
            f"boundary-{index}",
            state=DirectionalStateClassV2.BROAD_BULLISH_STATE,
            day=index % 7,
            values=(1, 1, 1, 1, 1),
            offset_ms=1_000 + index,
        )
    )
    result = _run(rows)
    broad = _cell(result, HistoricalThreeFamilyBootstrapBucketV2.BROAD_3_OF_3)
    assert broad.evaluable == HISTORICAL_THREE_FAMILY_BOOTSTRAP_MIN_EVALUABLE_V2
    assert broad.feasibility is (
        HistoricalThreeFamilyBootstrapFeasibilityV2.DESCRIPTIVE_EXPOSED_ONLY
    )
    assert not broad.inference_complete
    assert not broad.efficacy_validated
    assert not broad.probability
    assert not broad.promoting


def test_conflicted_comparator_is_separately_versioned_and_never_pooled() -> None:
    conflicted = _conflicted_event(
        "conflicted",
        side=HistoricalThreeFamilySideV2.BULLISH,
        day=1,
        values=(50, 60, 70, 80, 90),
    )
    without = _run()
    with_conflict = _run(conflicted_rows=conflicted)

    clean_without = _cell(
        without,
        HistoricalThreeFamilyBootstrapBucketV2.CLEAN_2_PLUS_NEUTRAL,
    )
    clean_with = _cell(
        with_conflict,
        HistoricalThreeFamilyBootstrapBucketV2.CLEAN_2_PLUS_NEUTRAL,
    )
    conflict_cell = _cell(
        with_conflict,
        HistoricalThreeFamilyBootstrapBucketV2.CONFLICTED_2_VS_1,
    )
    assert clean_with.events == clean_without.events == 1
    assert conflict_cell.events == 1
    assert with_conflict.conflicted_event_count == 1
    assert with_conflict.conflicted_outcome_count == 5
    assert with_conflict.conflicted_outcome_protocol_version == (
        HISTORICAL_THREE_FAMILY_CONFLICTED_OUTCOME_PROTOCOL_V2
    )
    assert with_conflict.conflicted_rows_sha256 is not None
    assert len(with_conflict.contrasts) == 20
    conflicted_contrast = next(
        item
        for item in with_conflict.contrasts
        if item.comparison
        is HistoricalThreeFamilyBootstrapComparisonV2.BROAD_MINUS_CONFLICTED_2_VS_1
        and item.horizon_bars == 1
        and item.side is HistoricalThreeFamilySideV2.BULLISH
    )
    assert conflicted_contrast.comparator_bucket is (
        HistoricalThreeFamilyBootstrapBucketV2.CONFLICTED_2_VS_1
    )
    assert conflicted_contrast.comparator_protocol_version == (
        HISTORICAL_THREE_FAMILY_CONFLICTED_OUTCOME_PROTOCOL_V2
    )
    assert _endpoint(
        conflicted_contrast.endpoints,
        HistoricalThreeFamilyBootstrapMetricV2.MEAN_NET_RETURN_MICROS,
    ).point_estimate == HistoricalExactRationalV2("250", "1")


@pytest.mark.parametrize(
    ("side", "agreement"),
    (
        (HistoricalThreeFamilySideV2.BULLISH, 500_000),
        (HistoricalThreeFamilySideV2.BULLISH, -500_000),
        (HistoricalThreeFamilySideV2.BULLISH, 0),
        (HistoricalThreeFamilySideV2.BEARISH, -500_000),
        (HistoricalThreeFamilySideV2.BEARISH, 500_000),
        (HistoricalThreeFamilySideV2.BEARISH, 0),
    ),
)
def test_conflicted_weighted_agreement_does_not_redefine_sign_count_side(
    side: HistoricalThreeFamilySideV2,
    agreement: int,
) -> None:
    rows = _conflicted_event(
        f"descriptive-{side.value}-{agreement}",
        side=side,
        day=1,
        values=(50, 60, 70, 80, 90),
        agreement=agreement,
    )
    result = _run(conflicted_rows=rows)
    assert result.conflicted_event_count == 1
    assert all(row.side is side for row in rows)
    assert all(row.directional_agreement_micros == agreement for row in rows)


@dataclass(frozen=True)
class _CompatibleCostRow:
    event_id: str
    execution_contract_sha256: str
    agreement_bucket: str
    primary_direction: str
    horizon_bars: int
    evaluable: bool
    exclusion_reason: str
    gross_directional_return_micros: int | None
    slippage_return_micros: int | None
    fee_return_micros: int | None
    funding_return_micros: int | None
    rounding_residual_micros: int | None
    total_cost_micros: int | None
    net_return_micros: int | None


def _costs_for(rows: tuple[HistoricalThreeFamilyOutcomeV2, ...]):
    output: list[HistoricalThreeFamilyCostAttributionV2] = []
    for row in rows:
        broad = row.state_class is DirectionalStateClassV2.BROAD_BULLISH_STATE
        gross = 500 if broad else 100
        net = 300 if broad else -100
        assert row.net_return_micros == net
        compatible = _CompatibleCostRow(
            event_id=row.event_id,
            execution_contract_sha256=row.execution_contract_sha256,
            agreement_bucket=row.bucket.value,
            primary_direction="long",
            horizon_bars=row.horizon_bars,
            evaluable=True,
            exclusion_reason="",
            gross_directional_return_micros=gross,
            slippage_return_micros=100,
            fee_return_micros=100,
            funding_return_micros=0,
            rounding_residual_micros=0,
            total_cost_micros=200,
            net_return_micros=net,
        )
        output.append(
            cost_attribution_from_fixed_horizon_row_v2(
                compatible,
                source=HistoricalThreeFamilyCostSourceV2.PRIMARY_CLEAN,
            )
        )
    return tuple(output)


def test_fixed_horizon_cost_adapter_reconciles_gross_to_net_attribution() -> None:
    rows = (
        *_primary_event(
            "cost-clean",
            state=DirectionalStateClassV2.BULLISH_STATE_TILT,
            day=0,
            values=(-100, -100, -100, -100, -100),
        ),
        *_primary_event(
            "cost-broad",
            state=DirectionalStateClassV2.BROAD_BULLISH_STATE,
            day=0,
            values=(300, 300, 300, 300, 300),
        ),
    )
    result = _run(rows, cost_attributions=_costs_for(rows))
    clean = _cell(result, HistoricalThreeFamilyBootstrapBucketV2.CLEAN_2_PLUS_NEUTRAL)
    broad = _cell(result, HistoricalThreeFamilyBootstrapBucketV2.BROAD_3_OF_3)

    assert result.cost_attribution_complete
    assert result.cost_attribution_rows_sha256 is not None
    assert clean.cost_attribution is not None
    assert clean.cost_attribution.events == clean.cost_attribution.evaluable == 1
    assert clean.cost_attribution.coverage_micros == 1_000_000
    assert clean.cost_attribution.gross_directional_strict_hits == 1
    assert clean.cost_attribution.gross_directional_strict_hit_rate_micros == 1_000_000
    assert clean.cost_attribution.net_strict_hits == 0
    assert clean.cost_attribution.net_strict_hit_rate_micros == 0
    assert clean.cost_attribution.gross_to_net_hit_loss_count == 1
    assert clean.cost_attribution.gross_to_net_hit_loss_rate_micros == 1_000_000
    assert clean.cost_attribution.net_positive_without_gross_positive_count == 0
    assert clean.cost_attribution.mean_gross_directional_return_micros == 100
    assert clean.cost_attribution.mean_total_cost_micros == 200
    assert clean.cost_attribution.mean_net_return_micros == -100
    assert clean.cost_attribution.gross_to_net_mean_change_micros == -200
    assert broad.cost_attribution is not None
    assert broad.cost_attribution.mean_gross_directional_return_micros == 500


def test_cost_input_is_exact_complete_and_cannot_cross_source_or_side() -> None:
    rows = (
        *_primary_event(
            "cost-clean-boundary",
            state=DirectionalStateClassV2.BULLISH_STATE_TILT,
            day=0,
            values=(-100, -100, -100, -100, -100),
        ),
        *_primary_event(
            "cost-broad-boundary",
            state=DirectionalStateClassV2.BROAD_BULLISH_STATE,
            day=0,
            values=(300, 300, 300, 300, 300),
        ),
    )
    costs = _costs_for(rows)
    with pytest.raises(HistoricalThreeFamilyBootstrapErrorV2, match="cover every"):
        _run(rows, cost_attributions=costs[:-1])
    with pytest.raises(HistoricalThreeFamilyBootstrapErrorV2, match="differs"):
        wrong_side = replace(costs[0], side=HistoricalThreeFamilySideV2.BEARISH)
        _run(rows, cost_attributions=(wrong_side, *costs[1:]))
    with pytest.raises(HistoricalThreeFamilyBootstrapErrorV2, match="reconcile"):
        replace(costs[0], total_cost_micros=201)
    with pytest.raises(HistoricalThreeFamilyBootstrapErrorV2, match="source boundary"):
        replace(
            costs[0],
            source=HistoricalThreeFamilyCostSourceV2.CONFLICTED_COMPARATOR,
        )


def test_conflicted_partial_overlap_and_wrong_contract_fail_closed() -> None:
    conflicted = _conflicted_event(
        "conflicted-boundary",
        side=HistoricalThreeFamilySideV2.BULLISH,
        day=1,
        values=(1, 2, 3, 4, 5),
    )
    with pytest.raises(HistoricalThreeFamilyBootstrapErrorV2, match="every frozen horizon"):
        _run(conflicted_rows=conflicted[:-1])
    with pytest.raises(HistoricalThreeFamilyBootstrapErrorV2, match="same execution"):
        wrong_contract = replace(conflicted[0], execution_contract_sha256="8" * 64)
        _run(conflicted_rows=(wrong_contract, *conflicted[1:]))
    primary = _primary_rows()
    overlap = tuple(
        replace(
            row,
            event_id=primary[0].event_id,
            symbol=primary[0].symbol,
            decision_time_ms=primary[0].decision_time_ms,
        )
        for row in conflicted
    )
    with pytest.raises(HistoricalThreeFamilyBootstrapErrorV2, match="share an event ID"):
        _run(conflicted_rows=overlap)
    with pytest.raises(HistoricalThreeFamilyBootstrapErrorV2, match="horizon"):
        replace(conflicted[0], horizon_bars=True)


@pytest.mark.parametrize(
    ("start", "end", "message"),
    [
        (CALENDAR_START_MS + 1, CALENDAR_START_MS + 7 * DAY_MS, "UTC-midnight"),
        (CALENDAR_START_MS, CALENDAR_START_MS + 6 * DAY_MS, "between 7"),
    ],
)
def test_calendar_boundaries_fail_closed(start: int, end: int, message: str) -> None:
    with pytest.raises(HistoricalThreeFamilyBootstrapErrorV2, match=message):
        bootstrap_historical_three_family_outcomes_v2(
            _primary_rows(),
            calendar_start_ms=start,
            calendar_end_ms=end,
        )


def test_outcome_outside_calendar_and_fixed_claim_tamper_fail_closed() -> None:
    outside = _primary_event(
        "outside",
        state=DirectionalStateClassV2.BROAD_BULLISH_STATE,
        day=7,
        values=(1, 2, 3, 4, 5),
    )
    with pytest.raises(HistoricalThreeFamilyBootstrapErrorV2, match="outside"):
        _run(outside)

    result = _run()
    tampered = replace(result, samples=9_999)
    with pytest.raises(HistoricalThreeFamilyBootstrapErrorV2, match="fixed claims"):
        canonical_historical_three_family_bootstrap_v2(tampered)


def test_exact_rational_rejects_noncanonical_or_unreduced_values() -> None:
    with pytest.raises(HistoricalThreeFamilyBootstrapErrorV2, match="canonical"):
        HistoricalExactRationalV2("01", "1")
    with pytest.raises(HistoricalThreeFamilyBootstrapErrorV2, match="reduced"):
        HistoricalExactRationalV2("2", "2")


def _report_documents() -> tuple[dict[str, object], dict[str, object], dict[str, object]]:
    common_sha = "a" * 64
    census: dict[str, object] = {
        "protocol": HISTORICAL_THREE_FAMILY_CENSUS_PROTOCOL_V2,
        "historical_only": True,
        "probability": False,
        "probability_calibrated": False,
        "promoting": False,
        "outcome_data_read": False,
        "census_complete": True,
        "authenticated_anchors": 2,
        "consensus_rows": 2,
        "execution_contract_sha256": EXECUTION_SHA,
        "topology_analysis": {
            "admission_reconciliation": {
                "source_admitted_rows": 2,
                "clean_primary_audit_eligible_rows": 2,
                "conflicted_comparator_eligible_rows": 0,
            }
        },
    }
    outcomes: dict[str, object] = {
        "protocol": HISTORICAL_THREE_FAMILY_FIXED_HORIZON_RUNNER_PROTOCOL_V2,
        "historical_only": True,
        "probability": False,
        "probability_calibrated": False,
        "promoting": False,
        "bootstrap_included": False,
        "multiplicity_claim": False,
        "order_placement": False,
        "execution_contract_sha256": EXECUTION_SHA,
        "census_manifest_sha256": common_sha,
        "consensus_sha256": "b" * 64,
        "census_rows": 2,
        "admitted_events": 2,
        "outcome_rows": 10,
        "evaluable_outcomes": 10,
    }
    te0: dict[str, object] = {
        "protocol": HISTORICAL_THREE_FAMILY_TE0_PROTOCOL_V2,
        "historical_only": True,
        "probability": False,
        "probability_calibrated": False,
        "promoting": False,
        "opposite_signal_evaluated": False,
        "order_placement": False,
        "portfolio_equity_claim": False,
        "drawdown_claim": False,
        "execution_contract_sha256": EXECUTION_SHA,
        "census_manifest_sha256": common_sha,
        "consensus_sha256": "b" * 64,
        "census_rows": 2,
        "admitted_events": 2,
        "result_rows": 2,
        "evaluable_rows": 2,
        "exit_reason_counts": {"time": 2},
        "exclusion_counts": {},
    }
    return census, outcomes, te0


def test_pure_korean_report_reconciles_documents_and_states_limitations() -> None:
    census, outcomes, te0 = _report_documents()
    report = render_historical_three_family_report_ko_v2(
        census_results=census,
        fixed_horizon_results=outcomes,
        bootstrap=_run(),
        te0_results=te0,
    )

    assert report.startswith("# 3개 증거군 역사적 백테스트 감사 보고서\n")
    assert report.endswith("\n")
    assert "\r" not in report
    assert HISTORICAL_THREE_FAMILY_REPORT_VERSION_V2 in report
    assert "평균 순수익 -35.62bp" in report
    assert "uplift -0.87bp" in report
    assert "profit factor 0.452" in report
    assert "엄격 적중률 34.66%" in report
    assert "비용 drag가 35.85bp" in report
    assert "이미 노출된 과거 구간" in report
    assert "깨끗한 2+중립과 합산 금지" in report
    assert "exact aggTrade M1과 동등하지 않습니다" in report
    assert "전향 PAPER/BBO" in report
    assert "주문 배치 기능" in report
    assert len(historical_three_family_report_sha256_v2(report)) == 64
    assert historical_three_family_report_sha256_v2(report) == (
        historical_three_family_report_sha256_v2(report)
    )


def test_report_fails_closed_on_cross_document_contract_or_probability_drift() -> None:
    census, outcomes, te0 = _report_documents()
    with pytest.raises(HistoricalThreeFamilyReportErrorV2, match="execution/cost"):
        render_historical_three_family_report_ko_v2(
            census_results=census,
            fixed_horizon_results={**outcomes, "execution_contract_sha256": "c" * 64},
            bootstrap=_run(),
            te0_results=te0,
        )
    with pytest.raises(HistoricalThreeFamilyReportErrorV2, match="claims differ"):
        render_historical_three_family_report_ko_v2(
            census_results=census,
            fixed_horizon_results={**outcomes, "probability": True},
            bootstrap=_run(),
            te0_results=te0,
        )
    with pytest.raises(HistoricalThreeFamilyReportErrorV2, match="LF-only"):
        historical_three_family_report_sha256_v2("not-final")
