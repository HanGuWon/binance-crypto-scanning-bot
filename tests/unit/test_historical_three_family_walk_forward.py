from __future__ import annotations

import csv
import hashlib
import inspect
import io
import json
from pathlib import Path
from typing import Literal

import pytest

import signalbot.backtest.historical_three_family_walk_forward as walk_forward
from signalbot.backtest.historical_three_family_walk_forward import (
    DAY_MS_V1,
    EMBARGO_BARS_V1,
    EMBARGO_MS_V1,
    FROZEN_WALK_FORWARD_CONTRACT_V1,
    INITIAL_TRAINING_DAYS_V1,
    LOGISTIC_GATE_PROBABILITY_V1,
    LOGISTIC_LAMBDA_V1,
    LOGISTIC_MAX_ITERATIONS_V1,
    LOGISTIC_TOLERANCE_V1,
    RIDGE_GATE_BPS_V1,
    RIDGE_LAMBDA_V1,
    TARGET_HORIZON_BARS_V1,
    TEST_WINDOW_DAYS_V1,
    TEST_WINDOW_MS_V1,
    WalkForwardObservationV1,
    build_expanding_walk_forward_slices_v1,
    evaluate_frozen_walk_forward_v1,
    fit_train_only_feature_transform_v1,
    frozen_walk_forward_contract_document_v1,
    run_frozen_walk_forward_diagnostic_v1,
)

_T0 = 1_700_000_000_000


def _event_id(label: str) -> str:
    return hashlib.sha256(label.encode()).hexdigest()


def _row(
    label: str,
    time_ms: int,
    *,
    asset: str = "ALPHA",
    side: Literal["long", "short"] = "long",
    feature: float = 0.1,
    net_return_micros: int = 1_000,
) -> WalkForwardObservationV1:
    return WalkForwardObservationV1(
        event_id=_event_id(label),
        split="synthetic",
        asset=asset,
        symbol=f"{asset}USDT",
        side=side,
        decision_time_ms=time_ms,
        price_signed_strength=feature,
        participation_signed_strength=feature * -0.5,
        cross_section_signed_strength=feature * 0.25,
        absolute_directional_agreement=abs(feature),
        round_trip_cost_to_atr=0.5 + abs(feature) / 10,
        net_return_micros=net_return_micros,
    )


def test_frozen_schedule_has_exact_initial_train_embargo_and_no_overlap() -> None:
    training_end = _T0 + INITIAL_TRAINING_DAYS_V1 * DAY_MS_V1
    first_test_start = training_end + EMBARGO_MS_V1
    rows = tuple(
        sorted(
            (
                _row("train-0", _T0),
                _row("train-1", _T0 + 100 * DAY_MS_V1),
                _row("train-last", training_end - 1),
                _row("embargo-first", training_end),
                _row("embargo-last", first_test_start - 1),
                _row("test-first", first_test_start),
                _row("test-later", first_test_start + DAY_MS_V1),
            ),
            key=lambda row: (row.decision_time_ms, row.event_id),
        )
    )

    slices = build_expanding_walk_forward_slices_v1(rows)

    assert len(slices) == 1
    fold = slices[0]
    assert fold.training_cutoff_ms_exclusive == training_end
    assert fold.test_start_ms == first_test_start
    assert fold.test_start_ms - fold.training_cutoff_ms_exclusive == EMBARGO_MS_V1
    assert fold.test_end_ms_exclusive == rows[-1].decision_time_ms + 1
    assert fold.last_partial
    assert tuple(rows[index].event_id for index in fold.train_indices) == tuple(
        row.event_id for row in rows[:3]
    )
    assert tuple(rows[index].event_id for index in fold.embargo_indices) == tuple(
        row.event_id for row in rows[3:5]
    )
    assert tuple(rows[index].event_id for index in fold.test_indices) == tuple(
        row.event_id for row in rows[5:]
    )
    assert not (set(fold.train_indices) & set(fold.embargo_indices))
    assert not (set(fold.train_indices) & set(fold.test_indices))
    assert not (set(fold.embargo_indices) & set(fold.test_indices))


def test_consecutive_folds_are_fixed_30_day_windows_with_expanding_prior_fit() -> None:
    first_test_start = (
        _T0 + INITIAL_TRAINING_DAYS_V1 * DAY_MS_V1 + EMBARGO_MS_V1
    )
    rows = tuple(
        _row(
            f"day-{day}",
            _T0 + day * DAY_MS_V1,
            net_return_micros=1_000 if day % 2 else -1_000,
        )
        for day in range(246)
    )

    slices = build_expanding_walk_forward_slices_v1(rows)

    assert len(slices) == 3
    assert slices[0].test_start_ms == first_test_start
    assert slices[1].test_start_ms == slices[0].test_start_ms + TEST_WINDOW_MS_V1
    assert slices[2].test_start_ms == slices[1].test_start_ms + TEST_WINDOW_MS_V1
    assert not slices[0].last_partial
    assert not slices[1].last_partial
    assert slices[2].last_partial
    assert set(slices[0].train_indices) < set(slices[1].train_indices)
    assert set(slices[1].train_indices) < set(slices[2].train_indices)
    assert max(
        rows[index].decision_time_ms for index in slices[1].train_indices
    ) < slices[1].test_start_ms - EMBARGO_MS_V1


def test_standardizer_and_asset_vocabulary_use_training_rows_only() -> None:
    training = (
        _row("a", _T0, asset="ALPHA", side="long", feature=1.0),
        _row("b", _T0 + 1, asset="BETA", side="short", feature=3.0),
    )
    future = _row(
        "future",
        _T0 + 2,
        asset="UNSEEN",
        side="long",
        feature=1_000_000.0,
        net_return_micros=-999_999_999,
    )

    fitted = fit_train_only_feature_transform_v1(training)
    fitted_again = fit_train_only_feature_transform_v1(training)

    assert fitted == fitted_again
    assert fitted.assets == ("ALPHA", "BETA")
    assert fitted.means[1] == 2.0
    assert fitted.scales[1] == 1.0
    transformed_future = fitted.transform(future)
    assert transformed_future[-2:] == (0.0, 0.0)
    assert fitted.means[1] != future.price_signed_strength


def test_appending_extreme_future_row_cannot_change_first_fold_fit_or_predictions() -> None:
    base = tuple(
        _row(
            f"base-{day}",
            _T0 + day * DAY_MS_V1,
            asset="ALPHA" if day % 3 else "BETA",
            side="long" if day % 2 else "short",
            feature=(day % 11 - 5) / 10,
            net_return_micros=2_000 if day % 4 else -5_000,
        )
        for day in range(230)
    )
    future = _row(
        "extreme-future",
        _T0 + 250 * DAY_MS_V1,
        asset="FUTURE_ONLY",
        feature=1_000_000_000.0,
        net_return_micros=2_000_000_000,
    )

    base_evaluations = evaluate_frozen_walk_forward_v1(base)
    extended_evaluations = evaluate_frozen_walk_forward_v1((*base, future))

    base_first = base_evaluations[0]
    extended_first = extended_evaluations[0]
    assert base_first.transform == extended_first.transform
    assert base_first.ridge_coefficients == extended_first.ridge_coefficients
    assert base_first.logistic_coefficients == extended_first.logistic_coefficients
    assert base_first.predictions == extended_first.predictions
    assert future.decision_time_ms >= extended_first.slice.test_end_ms_exclusive


def test_model_and_gate_parameters_are_non_overridable_and_threshold_search_is_false() -> None:
    contract = FROZEN_WALK_FORWARD_CONTRACT_V1
    document = frozen_walk_forward_contract_document_v1()

    assert contract.initial_training_days == INITIAL_TRAINING_DAYS_V1 == 180
    assert contract.embargo_bars == EMBARGO_BARS_V1 == 72
    assert contract.test_window_days == TEST_WINDOW_DAYS_V1 == 30
    assert contract.target_horizon_bars == TARGET_HORIZON_BARS_V1 == 12
    assert contract.ridge_lambda == RIDGE_LAMBDA_V1 == 10.0
    assert contract.logistic_lambda == LOGISTIC_LAMBDA_V1 == 10.0
    assert contract.logistic_max_iterations == LOGISTIC_MAX_ITERATIONS_V1 == 100
    assert contract.logistic_tolerance == LOGISTIC_TOLERANCE_V1 == 1e-10
    assert contract.ridge_gate_bps == RIDGE_GATE_BPS_V1 == 0.0
    assert (
        contract.logistic_gate_probability
        == LOGISTIC_GATE_PROBABILITY_V1
        == 0.5
    )
    assert inspect.signature(type(contract)).parameters == {}
    assert set(inspect.signature(run_frozen_walk_forward_diagnostic_v1).parameters) == {
        "consensus_path",
        "outcomes_path",
        "output_dir",
    }
    assert document["promoting"] is False
    assert document["historical_only"] is True
    assert document["paper_executable"] is False
    fixed_gate = document["fixed_gate"]
    assert isinstance(fixed_gate, dict)
    assert fixed_gate["threshold_candidates"] == 1
    assert fixed_gate["threshold_tuning"] is False
    assert fixed_gate["outcome_conditioned_selection"] is False


def test_schedule_rejects_unsorted_or_duplicate_rows() -> None:
    first = _row("first", _T0)
    later = _row("later", _T0 + 200 * DAY_MS_V1)

    with pytest.raises(ValueError, match="ordered"):
        build_expanding_walk_forward_slices_v1((later, first))
    with pytest.raises(ValueError, match="duplicate event IDs"):
        build_expanding_walk_forward_slices_v1((first, first, later))


def test_synthetic_end_to_end_publication_is_exact_and_non_promoting(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    consensus_raw, outcomes_raw = _synthetic_inputs(days=230)
    consensus_path = tmp_path / "consensus.csv"
    outcomes_path = tmp_path / "fixed_horizon_outcomes.csv"
    consensus_path.write_bytes(consensus_raw)
    outcomes_path.write_bytes(outcomes_raw)
    consensus_sha = hashlib.sha256(consensus_raw).hexdigest()
    outcomes_sha = hashlib.sha256(outcomes_raw).hexdigest()
    monkeypatch.setattr(walk_forward, "FROZEN_CONSENSUS_SHA256_V1", consensus_sha)
    monkeypatch.setattr(
        walk_forward,
        "FROZEN_FIXED_HORIZON_OUTCOMES_SHA256_V1",
        outcomes_sha,
    )
    output_dir = tmp_path / "published"

    published = run_frozen_walk_forward_diagnostic_v1(
        consensus_path=consensus_path,
        outcomes_path=outcomes_path,
        output_dir=output_dir,
    )

    assert published.prediction_count > 0
    assert published.fold_count == 2
    assert {path.name for path in output_dir.iterdir()} == {
        "fold_models.json",
        "folds.csv",
        "manifest.json",
        "predictions.csv",
        "report.ko.md",
        "results.json",
    }
    manifest = json.loads((output_dir / "manifest.json").read_text(encoding="utf-8"))
    results = json.loads((output_dir / "results.json").read_text(encoding="utf-8"))
    assert manifest["inputs"] == {
        "consensus.csv": consensus_sha,
        "fixed_horizon_outcomes.csv": outcomes_sha,
    }
    assert manifest["status"] == "EXPOSED_HISTORICAL_ONLY"
    assert manifest["historical_only"] is True
    assert manifest["exposed"] is True
    assert manifest["promoting"] is False
    assert manifest["qualification"] is False
    assert manifest["paper_executable"] is False
    assert manifest["order_placement"] is False
    assert manifest["threshold_tuning"] is False
    assert manifest["publication"][
        "source_inputs_reverified_immediately_before_publication"
    ] is True
    assert results["fixed_gate"]["rule"] == (
        "ridge_expected_net_bps > 0 AND logistic_probability > 0.5"
    )
    assert results["prior_probe_disposition"]["status"] == (
        "PRIOR_PROBE_UNREPRODUCED_NOT_AUTHORITATIVE"
    )
    for name, expected_sha in manifest["outputs"].items():
        assert hashlib.sha256((output_dir / name).read_bytes()).hexdigest() == expected_sha


def _synthetic_inputs(*, days: int) -> tuple[bytes, bytes]:
    consensus_fields = (
        "admitted",
        "asset",
        "atr_fraction_micros",
        "cross_section_direction",
        "cross_section_strength_micros",
        "decision_time_ms",
        "directional_agreement_micros",
        "event_id",
        "participation_direction",
        "participation_status",
        "participation_strength_micros",
        "price_direction",
        "price_strength_micros",
        "primary_direction",
        "split",
        "symbol",
        "zero_move_round_trip_cost_micros",
    )
    outcome_fields = (
        "asset",
        "decision_time_ms",
        "directional_agreement_micros",
        "evaluable",
        "event_id",
        "exclusion_reason",
        "fee_return_micros",
        "funding_return_micros",
        "gross_directional_return_micros",
        "historical_only",
        "horizon_bars",
        "horizon_minutes",
        "net_return_micros",
        "order_placement",
        "primary_direction",
        "probability",
        "probability_calibrated",
        "promoting",
        "rounding_residual_micros",
        "slippage_return_micros",
        "split",
        "symbol",
        "total_cost_micros",
    )
    consensus_output = io.StringIO(newline="")
    outcome_output = io.StringIO(newline="")
    consensus_writer = csv.DictWriter(
        consensus_output, fieldnames=consensus_fields, lineterminator="\n"
    )
    outcome_writer = csv.DictWriter(
        outcome_output, fieldnames=outcome_fields, lineterminator="\n"
    )
    consensus_writer.writeheader()
    outcome_writer.writeheader()
    for day in range(days):
        event_id = _event_id(f"synthetic-input-{day}")
        side = "long" if day % 2 else "short"
        direction = 1 if side == "long" else -1
        asset = "ALPHA" if day % 3 else "BETA"
        decision_time_ms = _T0 + day * DAY_MS_V1
        agreement = direction * 500_000
        net = 2_000 if day % 4 else -5_000
        consensus_writer.writerow(
            {
                "admitted": "true",
                "asset": asset,
                "atr_fraction_micros": 5_000,
                "cross_section_direction": direction,
                "cross_section_strength_micros": 300_000 + day,
                "decision_time_ms": decision_time_ms,
                "directional_agreement_micros": agreement,
                "event_id": event_id,
                "participation_direction": -direction if day % 5 == 0 else direction,
                "participation_status": "READY",
                "participation_strength_micros": 200_000 + day,
                "price_direction": direction,
                "price_strength_micros": 400_000 + day,
                "primary_direction": side,
                "split": "synthetic",
                "symbol": f"{asset}USDT",
                "zero_move_round_trip_cost_micros": 2_600,
            }
        )
        for horizon in (1, 3, 6, 12, 72):
            outcome_writer.writerow(
                {
                    "asset": asset,
                    "decision_time_ms": decision_time_ms,
                    "directional_agreement_micros": agreement,
                    "evaluable": "true",
                    "event_id": event_id,
                    "exclusion_reason": "",
                    "fee_return_micros": 100,
                    "funding_return_micros": 0,
                    "gross_directional_return_micros": net + 200,
                    "historical_only": "true",
                    "horizon_bars": horizon,
                    "horizon_minutes": horizon * 5,
                    "net_return_micros": net,
                    "order_placement": "false",
                    "primary_direction": side,
                    "probability": "false",
                    "probability_calibrated": "false",
                    "promoting": "false",
                    "rounding_residual_micros": 0,
                    "slippage_return_micros": 100,
                    "split": "synthetic",
                    "symbol": f"{asset}USDT",
                    "total_cost_micros": 200,
                }
            )
    return (
        consensus_output.getvalue().encode("utf-8"),
        outcome_output.getvalue().encode("utf-8"),
    )
