from __future__ import annotations

import csv
import json
from pathlib import Path

import pytest

from signalbot.backtest.indicator_analysis import (
    FEATURE_COLUMNS,
    IndicatorObservation,
    block_bootstrap_direction_accuracy,
    build_historical_success_gate,
    deduplicate_spot_priority,
    evaluate_indicator_discrimination,
    fit_percentile_composite,
    read_indicator_input_sets,
    read_indicator_inputs,
    run_indicator_analysis,
    score_observations,
)

DAY_MS = 86_400_000


def _observation(
    name: str,
    value: float,
    *,
    day: int = 0,
    split: str = "development",
    market: str = "spot",
    direction: str = "long",
    raw_return: float | None = 0.01,
    features: tuple[float | None, ...] | None = None,
    asset: str = "TEST",
    cohort: str = "volatile",
) -> IndicatorObservation:
    return IndicatorObservation(
        event_id=name,
        asset=asset,
        cohort=cohort,
        market=market,
        direction=direction,
        decision_time_ms=day * DAY_MS + 1,
        split=split,
        features=(value,) * len(FEATURE_COLUMNS) if features is None else features,
        recommendation_price=100.0,
        outcome_available=raw_return is not None,
        evaluable=raw_return is not None,
        future_close=None if raw_return is None else 100.0 * (1 + raw_return),
    )


def _development_rows() -> list[IndicatorObservation]:
    return [_observation(f"dev-{index}", float(index), day=index) for index in range(4)]


def test_development_ecdf_and_top_quartile_cutoff_are_frozen() -> None:
    development = _development_rows()
    model = fit_percentile_composite(development)
    scored = score_observations(development, model)

    assert [row.composite_score for row in scored] == pytest.approx(
        [12.5, 37.5, 62.5, 87.5]
    )
    assert model.top_quartile_cutoff == pytest.approx(68.75)
    assert model.development_rows == 4
    assert model.scored_development_rows == 4


def test_composite_equal_weights_axes_and_allows_within_axis_missing_values() -> None:
    model = fit_percentile_composite(_development_rows())
    partial = (3.0, None, None, 0.0, 0.0, None, 0.0, None)
    missing_axis = (3.0, None, None, None, 0.0, None, 0.0, None)
    scored = score_observations(
        [
            _observation("partial", 0.0, features=partial),
            _observation("missing-axis", 0.0, features=missing_axis),
        ],
        model,
    )

    assert scored[0].axis_scores == pytest.approx((0.875, 0.125, 0.125, 0.125))
    assert scored[0].composite_score == pytest.approx(31.25)
    assert scored[1].axis_scores[1] is None
    assert scored[1].composite_score is None


def test_fit_rejects_feature_that_is_unavailable_throughout_development() -> None:
    rows = [
        _observation(
            f"dev-{index}",
            float(index),
            features=(None, *(float(index),) * (len(FEATURE_COLUMNS) - 1)),
        )
        for index in range(4)
    ]

    with pytest.raises(ValueError, match="efficiency_ratio_20"):
        fit_percentile_composite(rows)


def test_spot_priority_deduplication_is_deterministic() -> None:
    rows = [
        _observation("futures", 1.0, market="futures"),
        _observation("spot", 2.0, market="spot"),
        _observation("later", 3.0, market="futures", day=1),
    ]

    result = deduplicate_spot_priority(tuple(reversed(rows)))

    assert [row.event_id for row in result.observations] == ["spot", "later"]
    assert result.audit.duplicate_rows_dropped == 1
    assert result.audit.groups_where_spot_was_preferred == 1


def test_direction_evaluation_reports_fixed_buckets_top_quartile_and_short_sign() -> None:
    model = fit_percentile_composite(_development_rows())
    validation = [
        _observation("v0", 0.0, split="validation", raw_return=0.01),
        _observation(
            "v1",
            1.0,
            day=1,
            split="validation",
            direction="short",
            raw_return=0.01,
        ),
        _observation("v2", 2.0, day=2, split="validation", raw_return=0.0),
        _observation("v3", 3.0, day=3, split="validation", raw_return=0.02),
    ]
    scored = score_observations(validation, model)

    evaluations = evaluate_indicator_discrimination(
        scored,
        split_names=("validation",),
        top_quartile_cutoff=model.top_quartile_cutoff,
        bootstrap_samples=100,
        bootstrap_block_days=1,
        seed=7,
    )
    overall = {
        row["selection"]: row
        for row in evaluations
        if row["dimension"] == "overall"
    }

    assert set(overall) == {
        "baseline_all",
        "score_0_to_lt25",
        "score_25_to_lt50",
        "score_50_to_lt75",
        "score_75_to_100",
        "development_top_quartile",
    }
    assert overall["baseline_all"]["correct"] == 2
    assert overall["baseline_all"]["wrong"] == 1
    assert overall["baseline_all"]["tie"] == 1
    assert overall["baseline_all"]["strict_accuracy"] == pytest.approx(0.5)
    top = overall["development_top_quartile"]
    assert top["selected_events"] == 1
    assert top["selection_coverage"] == pytest.approx(0.25)
    assert top["strict_accuracy"] == 1.0
    assert top["strict_accuracy_uplift_vs_baseline"] == pytest.approx(0.5)


def test_block_bootstrap_is_order_invariant_and_uses_shared_draws() -> None:
    model = fit_percentile_composite(_development_rows())
    observations = [
        _observation("a", 0.0, day=0, raw_return=0.01),
        _observation("b", 1.0, day=1, direction="short", raw_return=-0.01),
        _observation("c", 2.0, day=2, raw_return=0.0),
        _observation("d", 3.0, day=3, raw_return=-0.01),
    ]
    rows = score_observations(observations, model)

    first = block_bootstrap_direction_accuracy(
        rows[2:],
        rows,
        calendar_start_day=0,
        calendar_end_day=3,
        samples=100,
        block_days=1,
        seed=11,
    )
    second = block_bootstrap_direction_accuracy(
        tuple(reversed(rows[2:])),
        tuple(reversed(rows)),
        calendar_start_day=0,
        calendar_end_day=3,
        samples=100,
        block_days=1,
        seed=11,
    )

    assert first == second
    assert first["valid_replicates"]["selected_strict"] > 0
    assert first["shared_draw_schedule_sha256"]


def test_historical_success_gate_applies_every_preregistered_condition() -> None:
    overall = {
        "split": "validation",
        "selection": "development_top_quartile",
        "dimension": "overall",
        "value": "all",
        "strict_accuracy": 0.54,
        "strict_accuracy_uplift_vs_baseline": 0.04,
        "median_directional_return": 0.001,
        "selection_coverage": 0.22,
        "selected_evaluable": 300,
        "bootstrap": {
            "selected_strict_accuracy_95_interval": [0.51, 0.57],
            "strict_accuracy_uplift_vs_baseline_95_interval": [0.01, 0.07],
        },
    }
    directions = [
        {
            **overall,
            "dimension": "direction",
            "value": direction,
            "strict_accuracy": 0.51,
        }
        for direction in ("long", "short")
    ]
    assets = [
        {
            **overall,
            "dimension": "asset",
            "value": f"A{index}",
            "strict_accuracy_uplift_vs_baseline": 0.01,
        }
        for index in range(8)
    ]

    gate = build_historical_success_gate(
        [overall, *directions, *assets], validation_split="validation"
    )

    assert gate["overall_pass"] is True
    assert all(gate["criteria"].values())


def _write_csv_inputs(
    root: Path,
    *,
    omit_feature: str | None = None,
    splits: tuple[str, ...] = ("development", "validation", "retrospective_test"),
) -> tuple[Path, Path]:
    recommendations = root / "recommendations.csv"
    outcomes = root / "outcomes.csv"
    recommendation_fields = [
        "event_id",
        "asset",
        "cohort",
        "market",
        "direction",
        "decision_time_ms",
        "split",
        "price",
        *(name for name in FEATURE_COLUMNS if name != omit_feature),
    ]
    with recommendations.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=recommendation_fields)
        writer.writeheader()
        index = 0
        for split in splits:
            for value in range(4):
                event_id = f"{split}-{value}"
                row = {
                    "event_id": event_id,
                    "asset": "TEST",
                    "cohort": "volatile",
                    "market": "spot",
                    "direction": "long",
                    "decision_time_ms": str(index * DAY_MS + 1),
                    "split": split,
                    "price": "100",
                    **{name: str(value) for name in FEATURE_COLUMNS if name != omit_feature},
                }
                writer.writerow(row)
                index += 1
    with outcomes.open("w", encoding="utf-8", newline="") as handle:
        fields = ["event_id", "horizon_bars", "evaluable", "exit_price"]
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        for split in splits:
            for value in range(4):
                writer.writerow(
                    {
                        "event_id": f"{split}-{value}",
                        "horizon_bars": "12",
                        "evaluable": "True",
                        "exit_price": "101" if value == 3 else "99",
                    }
                )
    return recommendations, outcomes


def test_csv_contract_is_strict_and_end_to_end_runner_writes_json_and_korean_report(
    tmp_path: Path,
) -> None:
    recommendations, outcomes = _write_csv_inputs(tmp_path)
    loaded = read_indicator_inputs(recommendations, outcomes)
    output = tmp_path / "output"

    assert loaded.audit.matched_outcome_rows == 12
    results = run_indicator_analysis(
        recommendations_path=recommendations,
        outcomes_path=outcomes,
        output_dir=output,
        development_split="development",
        validation_split="validation",
        stress_split="retrospective_test",
        assets=("TEST",),
        bootstrap_samples=100,
        bootstrap_block_days=1,
        seed=17,
    )

    result_path = output / "indicator_analysis_results.json"
    report_path = output / "indicator_analysis_report_ko.md"
    assert (output / "fitted_score.json").is_file()
    assert (output / "results.json").is_file()
    assert (output / "report_ko.md").is_file()
    assert json.loads(result_path.read_text(encoding="utf-8"))["schema_version"] == 1
    assert "기술지표 방향 판별력 실험" in report_path.read_text(encoding="utf-8")
    assert results["primary_endpoint"]["horizon_bars"] == 12

    broken_root = tmp_path / "broken"
    broken_root.mkdir()
    broken_recommendations, broken_outcomes = _write_csv_inputs(
        broken_root, omit_feature="efficiency_ratio_20"
    )
    with pytest.raises(ValueError, match="efficiency_ratio_20"):
        read_indicator_inputs(broken_recommendations, broken_outcomes)


def test_independently_replayed_split_files_can_be_merged(tmp_path: Path) -> None:
    pairs = []
    for split in ("development", "validation", "retrospective_test"):
        root = tmp_path / split
        root.mkdir()
        pairs.append(_write_csv_inputs(root, splits=(split,)))

    loaded = read_indicator_input_sets(pairs)

    assert loaded.audit.recommendation_rows == 12
    assert {row.split for row in loaded.observations} == {
        "development",
        "validation",
        "retrospective_test",
    }
