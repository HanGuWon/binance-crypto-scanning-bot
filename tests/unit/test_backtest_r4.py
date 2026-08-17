from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from signalbot.backtest.r4 import (
    R4ForecastSpec,
    R4Observation,
    _selection,
    _validate_fold_population,
    analyze_r4_selective_forecast,
)


def _spec(**updates: object) -> R4ForecastSpec:
    raw: dict[str, object] = {
        "protocol_version": "r4-test",
        "source_protocol_version": "r3-test",
        "source_opportunities_sha256": "0" * 64,
        "experiment_plan_path": "plan.md",
        "interval": "5m",
        "horizon_bars": 12,
        "evaluation_start": "2024-07-01T00:00:00Z",
        "first_test_start": "2025-03-01T00:00:00Z",
        "evaluation_end": "2025-05-01T00:00:00Z",
        "minimum_training_months": 6,
        "calibration_months": 2,
        "laplace_alpha": 1.0,
        "variance_floor": 0.0001,
        "temperature_grid": [0.5, 1.0, 2.0],
        "intercept_grid": [-0.5, 0.0, 0.5],
        "edge_margin_bps": 5.0,
        "minimum_edge_probability": 0.60,
        "minimum_expected_net_bps": 5.0,
        "minimum_group_rows": 1,
        "reliability_bins": 5,
        "bootstrap_samples": 100,
        "bootstrap_block_days": 7,
        "matched_random_samples": 100,
        "seed": 7,
        "acceptance": {
            "minimum_selected": 1,
            "minimum_days": 1,
            "minimum_coverage": 0.01,
            "maximum_coverage": 1.0,
            "minimum_profit_factor": 1.0,
            "minimum_positive_assets": 1,
            "maximum_positive_contribution_share": 1.0,
            "maximum_ece": 1.0,
        },
    }
    raw.update(updates)
    return R4ForecastSpec.model_validate(raw)


def _observation(
    timestamp: datetime,
    market: str,
    edge: bool,
    sequence: int,
) -> R4Observation:
    sign = 1.0 if edge else -1.0
    net = 0.003 if edge else -0.002
    return R4Observation(
        opportunity_id=f"{market}-{timestamp:%Y%m%d}-{sequence}-{int(edge)}",
        protocol_version="r3-test",
        market=market,
        asset="BTC" if edge else "ETH",
        cohort="anchor",
        regime="risk_on" if edge else "risk_off",
        btc_trend="bullish" if edge else "bearish",
        htf_filter_accepted=edge,
        decision_time_ms=int(timestamp.timestamp() * 1000),
        breadth_ratio=0.75 if edge else 0.25,
        taker_delta_3=0.5 * sign,
        taker_delta_12=0.4 * sign,
        normalized_vpci=1.0 * sign,
        normalized_vpci_signal=0.6 * sign,
        normalized_vpci_slope_3=0.3 * sign,
        gross_return=net + 0.003,
        fee_return=0.002,
        slippage_return=0.001,
        funding_return=0.0,
        net_return=net,
    )


def _panel() -> tuple[R4Observation, ...]:
    start = datetime(2024, 7, 1, 12, tzinfo=UTC)
    end = datetime(2025, 5, 1, tzinfo=UTC)
    rows: list[R4Observation] = []
    current = start
    sequence = 0
    while current < end:
        for market in ("spot", "futures"):
            rows.append(_observation(current, market, False, sequence))
            rows.append(_observation(current + timedelta(minutes=5), market, True, sequence))
        sequence += 1
        current += timedelta(days=1)
    return tuple(rows)


def test_r4_walk_forward_is_deterministic_and_strictly_causal() -> None:
    result, predictions, model = analyze_r4_selective_forecast(_panel(), _spec())
    repeated = analyze_r4_selective_forecast(_panel(), _spec())

    assert predictions
    assert all(item.training_cutoff_ms < item.decision_time_ms for item in predictions)
    assert {item.aligned_direction for item in predictions if item.market == "spot"} == {
        "long"
    }
    assert {
        item.aligned_direction for item in predictions if item.market == "futures"
    } == {"short"}
    assert any(item.selected for item in predictions)
    assert result == repeated[0]
    assert predictions == repeated[1]
    assert model == repeated[2]


def test_r4_selection_has_positive_negative_and_exact_boundary_behavior() -> None:
    spec = _spec()

    assert _selection(spec, 0.60, 0.00051) == (True, "")
    assert _selection(spec, 0.59, 0.001) == (
        False,
        "edge_probability_below_threshold",
    )
    assert _selection(spec, 0.80, 0.0005) == (
        False,
        "expected_net_not_above_hurdle",
    )


def test_r4_fold_validation_uses_the_configured_edge_margin() -> None:
    test_start = datetime(2025, 3, 1, tzinfo=UTC)
    rows = (
        _observation(test_start - timedelta(days=2), "spot", False, 1),
        _observation(test_start - timedelta(days=1), "spot", True, 2),
    )

    _validate_fold_population(rows, rows, rows, "spot", test_start, 0.002)
    with pytest.raises(ValueError, match="train fold lacks both classes"):
        _validate_fold_population(rows, rows, rows, "spot", test_start, 0.004)
