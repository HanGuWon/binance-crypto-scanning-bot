from __future__ import annotations

import csv
import hashlib
import json
import math
import random
from collections import defaultdict
from collections.abc import Sequence
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from statistics import median
from typing import Any, Literal

import yaml
from pydantic import Field, field_validator, model_validator

from signalbot.backtest.calibration import (
    BinaryFeatureRow,
    GaussianCategoricalNaiveBayes,
    binary_log_loss,
    brier_score,
    equal_count_ece,
    finite_mean,
    fit_sigmoid_calibrator,
)
from signalbot.backtest.runner import source_code_digest
from signalbot.config import StrictModel

DAY_MS = 86_400_000
_PREDICTION_FIELDS = (
    "opportunity_id",
    "fold",
    "market",
    "asset",
    "cohort",
    "regime",
    "btc_trend",
    "decision_time_ms",
    "aligned_direction",
    "edge_probability",
    "expected_net_bps",
    "selected",
    "abstention_reason",
    "actual_edge",
    "gross_return",
    "fee_return",
    "slippage_return",
    "funding_return",
    "net_return",
    "net_return_2x_slippage",
    "climatology_probability",
    "temperature",
    "intercept",
    "training_cutoff_ms",
)


class R4AcceptanceSettings(StrictModel):
    minimum_selected: int = Field(ge=1)
    minimum_days: int = Field(ge=1)
    minimum_coverage: float = Field(gt=0, le=1)
    maximum_coverage: float = Field(gt=0, le=1)
    minimum_profit_factor: float = Field(gt=0)
    minimum_positive_assets: int = Field(ge=1)
    maximum_positive_contribution_share: float = Field(gt=0, le=1)
    maximum_ece: float = Field(ge=0, le=1)

    @model_validator(mode="after")
    def ordered_coverage(self) -> R4AcceptanceSettings:
        if self.maximum_coverage < self.minimum_coverage:
            raise ValueError("maximum_coverage must be at least minimum_coverage")
        return self


class R4ForecastSpec(StrictModel):
    protocol_version: str
    source_protocol_version: str
    source_opportunities_sha256: str
    experiment_plan_path: str
    interval: Literal["5m"]
    horizon_bars: Literal[12]
    evaluation_start: datetime
    first_test_start: datetime
    evaluation_end: datetime
    minimum_training_months: int = Field(ge=1, le=120)
    calibration_months: int = Field(ge=1, le=24)
    laplace_alpha: float = Field(gt=0, allow_inf_nan=False)
    variance_floor: float = Field(gt=0, allow_inf_nan=False)
    temperature_grid: list[float]
    intercept_grid: list[float]
    edge_margin_bps: float = Field(gt=0, le=1000, allow_inf_nan=False)
    minimum_edge_probability: float = Field(gt=0, lt=1)
    minimum_expected_net_bps: float = Field(ge=0, le=1000, allow_inf_nan=False)
    minimum_group_rows: int = Field(ge=1)
    reliability_bins: int = Field(ge=2, le=100)
    bootstrap_samples: int = Field(ge=100, le=100_000)
    bootstrap_block_days: int = Field(ge=1, le=365)
    matched_random_samples: int = Field(ge=100, le=100_000)
    seed: int
    acceptance: R4AcceptanceSettings

    @field_validator("evaluation_start", "first_test_start", "evaluation_end")
    @classmethod
    def normalize_utc(cls, value: datetime) -> datetime:
        if value.tzinfo is None:
            raise ValueError("R4 timestamps must include a UTC offset")
        return value.astimezone(UTC)

    @field_validator("source_opportunities_sha256")
    @classmethod
    def validate_sha256(cls, value: str) -> str:
        normalized = value.lower()
        if len(normalized) != 64 or any(ch not in "0123456789abcdef" for ch in normalized):
            raise ValueError("source_opportunities_sha256 must be a SHA-256 hex digest")
        return normalized

    @model_validator(mode="after")
    def validate_contract(self) -> R4ForecastSpec:
        if not self.evaluation_start < self.first_test_start < self.evaluation_end:
            raise ValueError("expected evaluation_start < first_test_start < evaluation_end")
        if not self.temperature_grid or not self.intercept_grid:
            raise ValueError("calibration grids must be non-empty")
        if any(not math.isfinite(value) or value <= 0 for value in self.temperature_grid):
            raise ValueError("temperature_grid values must be finite and positive")
        if any(not math.isfinite(value) for value in self.intercept_grid):
            raise ValueError("intercept_grid values must be finite")
        required_months = self.minimum_training_months + self.calibration_months
        available_months = _months_between(self.evaluation_start, self.first_test_start)
        if available_months < required_months:
            raise ValueError("first test does not leave enough train and calibration months")
        return self


@dataclass(frozen=True, slots=True)
class R4Observation:
    opportunity_id: str
    protocol_version: str
    market: str
    asset: str
    cohort: str
    regime: str
    btc_trend: str
    htf_filter_accepted: bool
    decision_time_ms: int
    breadth_ratio: float
    taker_delta_3: float
    taker_delta_12: float
    normalized_vpci: float
    normalized_vpci_signal: float
    normalized_vpci_slope_3: float
    gross_return: float
    fee_return: float
    slippage_return: float
    funding_return: float
    net_return: float

    def features(self) -> BinaryFeatureRow:
        return BinaryFeatureRow(
            numeric=(
                self.breadth_ratio,
                self.taker_delta_3,
                self.taker_delta_12,
                math.tanh(self.normalized_vpci),
                math.tanh(self.normalized_vpci_signal),
                math.tanh(self.normalized_vpci_slope_3),
            ),
            categorical=(
                self.cohort,
                self.regime,
                self.btc_trend,
                "htf_accept" if self.htf_filter_accepted else "htf_reject",
            ),
            target=False,
        )


@dataclass(frozen=True, slots=True)
class R4Prediction:
    opportunity_id: str
    fold: str
    market: str
    asset: str
    cohort: str
    regime: str
    btc_trend: str
    decision_time_ms: int
    aligned_direction: str
    edge_probability: float
    expected_net_bps: float
    selected: bool
    abstention_reason: str
    actual_edge: bool
    gross_return: float
    fee_return: float
    slippage_return: float
    funding_return: float
    net_return: float
    net_return_2x_slippage: float
    climatology_probability: float
    temperature: float
    intercept: float
    training_cutoff_ms: int

    @property
    def utc_day(self) -> int:
        return self.decision_time_ms // DAY_MS


@dataclass(frozen=True, slots=True)
class _ExpectationModel:
    group_means: dict[tuple[str, bool], float]
    market_means: dict[bool, float]
    minimum_group_rows: int
    group_counts: dict[tuple[str, bool], int]

    def expected(self, cohort: str, probability: float) -> float:
        values = []
        for target in (False, True):
            key = (cohort, target)
            value = (
                self.group_means[key]
                if self.group_counts.get(key, 0) >= self.minimum_group_rows
                else self.market_means[target]
            )
            values.append(value)
        return (1 - probability) * values[0] + probability * values[1]


def load_r4_forecast_spec(path: str | Path) -> R4ForecastSpec:
    with Path(path).open("r", encoding="utf-8") as handle:
        raw = yaml.safe_load(handle) or {}
    if not isinstance(raw, dict):
        raise ValueError("R4 configuration root must be a mapping")
    return R4ForecastSpec.model_validate(raw)


def read_r4_observations(
    path: str | Path,
    spec: R4ForecastSpec,
) -> tuple[R4Observation, ...]:
    source = Path(path)
    if _sha256_file(source) != spec.source_opportunities_sha256:
        raise ValueError("R4 source opportunities digest does not match the frozen spec")
    observations: list[R4Observation] = []
    identities: set[str] = set()
    with source.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        required = {
            "opportunity_id",
            "protocol_version",
            "market",
            "asset",
            "cohort",
            "regime",
            "btc_trend",
            "htf_filter_accepted",
            "decision_time_ms",
            "breadth_ratio",
            "analysis_eligible_12",
            "taker_delta_3",
            "taker_delta_12",
            "normalized_vpci",
            "normalized_vpci_signal",
            "normalized_vpci_slope_3",
            "signal_gross_return_12",
            "signal_fee_return_12",
            "signal_slippage_return_12",
            "signal_funding_return_12",
            "signal_net_return_12",
        }
        if reader.fieldnames is None or not required.issubset(reader.fieldnames):
            raise ValueError("R4 source opportunities are missing required columns")
        for row_number, row in enumerate(reader, start=2):
            if row["analysis_eligible_12"] != "True":
                continue
            identity = _required_text(row, "opportunity_id", row_number)
            if identity in identities:
                raise ValueError(f"duplicate opportunity_id at row {row_number}")
            identities.add(identity)
            protocol_version = _required_text(row, "protocol_version", row_number)
            if protocol_version != spec.source_protocol_version:
                raise ValueError(f"unexpected source protocol at row {row_number}")
            market = _required_text(row, "market", row_number)
            if market not in {"spot", "futures"}:
                raise ValueError(f"unsupported market at row {row_number}")
            values = {
                field: _finite_float(row, field, row_number)
                for field in (
                    "breadth_ratio",
                    "taker_delta_3",
                    "taker_delta_12",
                    "normalized_vpci",
                    "normalized_vpci_signal",
                    "normalized_vpci_slope_3",
                    "signal_gross_return_12",
                    "signal_fee_return_12",
                    "signal_slippage_return_12",
                    "signal_funding_return_12",
                    "signal_net_return_12",
                )
            }
            observations.append(
                R4Observation(
                    opportunity_id=identity,
                    protocol_version=protocol_version,
                    market=market,
                    asset=_required_text(row, "asset", row_number),
                    cohort=_required_text(row, "cohort", row_number),
                    regime=_required_text(row, "regime", row_number),
                    btc_trend=_required_text(row, "btc_trend", row_number),
                    htf_filter_accepted=_strict_bool(
                        row, "htf_filter_accepted", row_number
                    ),
                    decision_time_ms=int(
                        _required_text(row, "decision_time_ms", row_number)
                    ),
                    breadth_ratio=values["breadth_ratio"],
                    taker_delta_3=values["taker_delta_3"],
                    taker_delta_12=values["taker_delta_12"],
                    normalized_vpci=values["normalized_vpci"],
                    normalized_vpci_signal=values["normalized_vpci_signal"],
                    normalized_vpci_slope_3=values["normalized_vpci_slope_3"],
                    gross_return=values["signal_gross_return_12"],
                    fee_return=values["signal_fee_return_12"],
                    slippage_return=values["signal_slippage_return_12"],
                    funding_return=values["signal_funding_return_12"],
                    net_return=values["signal_net_return_12"],
                )
            )
    if not observations:
        raise ValueError("R4 source contains no eligible 12-bar observations")
    return tuple(
        sorted(
            observations,
            key=lambda item: (item.decision_time_ms, item.opportunity_id),
        )
    )


def analyze_r4_selective_forecast(
    observations: Sequence[R4Observation],
    spec: R4ForecastSpec,
) -> tuple[dict[str, Any], tuple[R4Prediction, ...], dict[str, Any]]:
    horizon_ms = spec.horizon_bars * 300_000
    edge_margin = spec.edge_margin_bps / 10_000
    predictions: list[R4Prediction] = []
    model_artifacts: list[dict[str, Any]] = []
    test_start = spec.first_test_start
    while test_start < spec.evaluation_end:
        test_end = min(_add_months(test_start, 1), spec.evaluation_end)
        calibration_start = _add_months(test_start, -spec.calibration_months)
        calibration_start_ms = int(calibration_start.timestamp() * 1000)
        test_start_ms = int(test_start.timestamp() * 1000)
        test_end_ms = int(test_end.timestamp() * 1000)
        for market in ("spot", "futures"):
            train = [
                item
                for item in observations
                if item.market == market
                and item.decision_time_ms >= int(spec.evaluation_start.timestamp() * 1000)
                and item.decision_time_ms + horizon_ms < calibration_start_ms
            ]
            calibration = [
                item
                for item in observations
                if item.market == market
                and item.decision_time_ms >= calibration_start_ms + horizon_ms
                and item.decision_time_ms + horizon_ms < test_start_ms
            ]
            test = [
                item
                for item in observations
                if item.market == market
                and item.decision_time_ms >= test_start_ms + horizon_ms
                and item.decision_time_ms + horizon_ms < test_end_ms
            ]
            _validate_fold_population(
                train,
                calibration,
                test,
                market,
                test_start,
                edge_margin,
            )
            model = GaussianCategoricalNaiveBayes(
                alpha=spec.laplace_alpha,
                variance_floor=spec.variance_floor,
            ).fit([_training_row(item, edge_margin) for item in train])
            calibration_logits = [
                model.log_odds(item.features().numeric, item.features().categorical)
                for item in calibration
            ]
            calibration_targets = [item.net_return > edge_margin for item in calibration]
            calibrator = fit_sigmoid_calibrator(
                calibration_logits,
                calibration_targets,
                temperatures=spec.temperature_grid,
                intercepts=spec.intercept_grid,
            )
            expectation = _fit_expectation_model(
                train,
                edge_margin,
                spec.minimum_group_rows,
            )
            prevalence = sum(item.net_return > edge_margin for item in train) / len(train)
            training_cutoff_ms = max(item.decision_time_ms + horizon_ms for item in train)
            fold = test_start.strftime("%Y-%m")
            model_artifacts.append(
                {
                    "fold": fold,
                    "market": market,
                    "train_rows": len(train),
                    "calibration_rows": len(calibration),
                    "test_rows": len(test),
                    "training_cutoff_ms": training_cutoff_ms,
                    "calibration_start_ms": calibration_start_ms,
                    "test_start_ms": test_start_ms,
                    "test_end_ms": test_end_ms,
                    "edge_prevalence": prevalence,
                    "calibrator": asdict(calibrator),
                    "model": model.artifact(),
                    "expectation": {
                        "group_counts": {
                            f"{cohort}|{int(target)}": count
                            for (cohort, target), count in sorted(
                                expectation.group_counts.items()
                            )
                        },
                        "group_means": {
                            f"{cohort}|{int(target)}": value
                            for (cohort, target), value in sorted(
                                expectation.group_means.items()
                            )
                        },
                        "market_means": {
                            str(int(target)): value
                            for target, value in expectation.market_means.items()
                        },
                    },
                }
            )
            for item in test:
                if training_cutoff_ms >= item.decision_time_ms:
                    raise ValueError("training cutoff is not strictly prior to prediction")
                feature = item.features()
                probability = calibrator.probability(
                    model.log_odds(feature.numeric, feature.categorical)
                )
                expected_net = expectation.expected(item.cohort, probability)
                selected, reason = _selection(spec, probability, expected_net)
                predictions.append(
                    R4Prediction(
                        opportunity_id=item.opportunity_id,
                        fold=fold,
                        market=market,
                        asset=item.asset,
                        cohort=item.cohort,
                        regime=item.regime,
                        btc_trend=item.btc_trend,
                        decision_time_ms=item.decision_time_ms,
                        aligned_direction="long" if market == "spot" else "short",
                        edge_probability=probability,
                        expected_net_bps=expected_net * 10_000,
                        selected=selected,
                        abstention_reason=reason,
                        actual_edge=item.net_return > edge_margin,
                        gross_return=item.gross_return,
                        fee_return=item.fee_return,
                        slippage_return=item.slippage_return,
                        funding_return=item.funding_return,
                        net_return=item.net_return,
                        net_return_2x_slippage=item.net_return - item.slippage_return,
                        climatology_probability=prevalence,
                        temperature=calibrator.temperature,
                        intercept=calibrator.intercept,
                        training_cutoff_ms=training_cutoff_ms,
                    )
                )
        test_start = test_end

    ordered = tuple(
        sorted(predictions, key=lambda item: (item.decision_time_ms, item.opportunity_id))
    )
    market_results = {
        market: _market_summary(
            [item for item in ordered if item.market == market], spec, market
        )
        for market in ("spot", "futures")
    }
    _apply_holm(market_results)
    for result in market_results.values():
        result["acceptance"] = _acceptance_checks(result, spec)
        result["status"] = (
            "EXPLORATORY_PASS"
            if all(result["acceptance"].values())
            else "EXPLORATORY_FAIL"
        )
    result = {
        "protocol_version": spec.protocol_version,
        "scope": "existing_c0_event_universe_only",
        "primary_horizon_bars": spec.horizon_bars,
        "edge_margin_bps": spec.edge_margin_bps,
        "prediction_rows": len(ordered),
        "folds": sorted({item.fold for item in ordered}),
        "markets": market_results,
        "status": (
            "EXPLORATORY_PASS"
            if any(item["status"] == "EXPLORATORY_PASS" for item in market_results.values())
            else "EXPLORATORY_FAIL"
        ),
        "generalization": "INCONCLUSIVE_NO_UNTOUCHED_OOS",
        "execution_validity": "INCONCLUSIVE_NO_HISTORICAL_BBO",
        "deployment": "NOT_APPROVED",
        "limitations": [
            "R4a selects only archived C0 Spot-long and Futures-short events.",
            "It cannot measure missed all-bar opportunities or bidirectional Futures performance.",
            "The complete evaluation period was exposed before R4a was designed.",
            "Fixed-horizon opportunities overlap and are not a realizable portfolio ledger.",
        ],
    }
    return result, ordered, {"fold_models": model_artifacts}


def write_r4_analysis(
    result: dict[str, Any],
    predictions: Sequence[R4Prediction],
    model_artifact: dict[str, Any],
    *,
    output_dir: str | Path,
    source_path: str | Path,
    spec_path: str | Path,
    workspace_root: str | Path,
) -> dict[str, str]:
    output = Path(output_dir)
    output.mkdir(parents=True, exist_ok=True)
    predictions_path = output / "predictions.csv"
    with predictions_path.open("w", encoding="utf-8", newline="") as handle:
        prediction_fields: list[str] = list(_PREDICTION_FIELDS)
        writer: csv.DictWriter[str] = csv.DictWriter(
            handle, fieldnames=prediction_fields
        )
        writer.writeheader()
        for item in predictions:
            writer.writerow(asdict(item))
    model_path = output / "calibration_model.json"
    analysis_path = output / "r4_analysis.json"
    report_path = output / "r4_report_ko.md"
    model_path.write_text(_json(model_artifact), encoding="utf-8", newline="\n")
    analysis_path.write_text(_json(result), encoding="utf-8", newline="\n")
    report_path.write_text(_render_report(result), encoding="utf-8", newline="\n")
    workspace = Path(workspace_root).resolve()
    spec = load_r4_forecast_spec(spec_path)
    plan_path = workspace / spec.experiment_plan_path
    manifest = {
        "protocol_version": spec.protocol_version,
        "source_code_sha256": source_code_digest(workspace),
        "inputs": {
            "opportunities": _sha256_file(Path(source_path)),
            "spec": _sha256_file(Path(spec_path)),
            "experiment_plan": _sha256_file(plan_path),
        },
        "outputs": {
            path.name: _sha256_file(path)
            for path in (predictions_path, model_path, analysis_path, report_path)
        },
        "feature_contract": {
            "numeric": [
                "breadth_ratio",
                "taker_delta_3",
                "taker_delta_12",
                "tanh(normalized_vpci)",
                "tanh(normalized_vpci_signal)",
                "tanh(normalized_vpci_slope_3)",
            ],
            "categorical": ["cohort", "regime", "btc_trend", "htf_acceptance"],
            "prohibited": ["asset", "setup_strength", "ADX threshold", "future returns"],
        },
    }
    manifest_path = output / "r4_manifest.json"
    manifest_path.write_text(_json(manifest), encoding="utf-8", newline="\n")
    return {
        "analysis": str(analysis_path),
        "predictions": str(predictions_path),
        "model": str(model_path),
        "report": str(report_path),
        "manifest": str(manifest_path),
    }


def analyze_r4_run(
    opportunities_path: str | Path,
    spec_path: str | Path,
    output_dir: str | Path,
    *,
    workspace_root: str | Path,
) -> tuple[dict[str, Any], dict[str, str]]:
    spec = load_r4_forecast_spec(spec_path)
    observations = read_r4_observations(opportunities_path, spec)
    result, predictions, model_artifact = analyze_r4_selective_forecast(
        observations, spec
    )
    paths = write_r4_analysis(
        result,
        predictions,
        model_artifact,
        output_dir=output_dir,
        source_path=opportunities_path,
        spec_path=spec_path,
        workspace_root=workspace_root,
    )
    return result, paths


def _training_row(item: R4Observation, edge_margin: float) -> BinaryFeatureRow:
    feature = item.features()
    return BinaryFeatureRow(
        numeric=feature.numeric,
        categorical=feature.categorical,
        target=item.net_return > edge_margin,
    )


def _fit_expectation_model(
    rows: Sequence[R4Observation],
    edge_margin: float,
    minimum_group_rows: int,
) -> _ExpectationModel:
    grouped: dict[tuple[str, bool], list[float]] = defaultdict(list)
    market: dict[bool, list[float]] = defaultdict(list)
    for item in rows:
        target = item.net_return > edge_margin
        grouped[(item.cohort, target)].append(item.net_return)
        market[target].append(item.net_return)
    if not market[False] or not market[True]:
        raise ValueError("expectation model requires both edge classes")
    return _ExpectationModel(
        group_means={key: finite_mean(values) for key, values in grouped.items()},
        market_means={target: finite_mean(values) for target, values in market.items()},
        minimum_group_rows=minimum_group_rows,
        group_counts={key: len(values) for key, values in grouped.items()},
    )


def _selection(
    spec: R4ForecastSpec,
    probability: float,
    expected_net: float,
) -> tuple[bool, str]:
    if not math.isfinite(probability) or not math.isfinite(expected_net):
        return False, "non_finite_forecast"
    if probability < spec.minimum_edge_probability:
        return False, "edge_probability_below_threshold"
    if expected_net * 10_000 <= spec.minimum_expected_net_bps:
        return False, "expected_net_not_above_hurdle"
    return True, ""


def _market_summary(
    rows: Sequence[R4Prediction],
    spec: R4ForecastSpec,
    market: str,
) -> dict[str, Any]:
    if not rows:
        raise ValueError(f"no R4 predictions for {market}")
    selected = [item for item in rows if item.selected]
    probabilities = [item.edge_probability for item in rows]
    targets = [item.actual_edge for item in rows]
    baseline_probabilities = [item.climatology_probability for item in rows]
    brier = brier_score(probabilities, targets)
    baseline_brier = brier_score(baseline_probabilities, targets)
    log_loss = binary_log_loss(probabilities, targets)
    baseline_log_loss = binary_log_loss(baseline_probabilities, targets)
    ece, reliability = equal_count_ece(
        probabilities, targets, spec.reliability_bins
    )
    bootstrap = _calendar_block_bootstrap(rows, spec, market)
    matched_random = _matched_random_uplift(rows, spec, market)
    asset_rows = {
        asset: _simple_selected_summary(
            [item for item in selected if item.asset == asset]
        )
        for asset in sorted({item.asset for item in rows})
    }
    positive_contributions = [
        max(0.0, float(summary["net_sum"])) for summary in asset_rows.values()
    ]
    total_positive = math.fsum(positive_contributions)
    concentration = (
        max(positive_contributions) / total_positive if total_positive > 0 else 1.0
    )
    summary = _simple_selected_summary(selected)
    summary.update(
        {
            "raw_candidates": len(rows),
            "coverage": len(selected) / len(rows),
            "selected_days": len({item.utc_day for item in selected}),
            "alerts_per_day": (
                len(selected) / len({item.utc_day for item in rows}) if rows else 0.0
            ),
            "unconditional_contribution_bps": (
                math.fsum(item.net_return for item in selected) / len(rows) * 10_000
            ),
            "brier_score": brier,
            "climatology_brier_score": baseline_brier,
            "brier_skill_score": (
                1 - brier / baseline_brier if baseline_brier > 0 else 0.0
            ),
            "log_loss": log_loss,
            "climatology_log_loss": baseline_log_loss,
            "ece": ece,
            "reliability": reliability,
            "bootstrap": bootstrap,
            "matched_random": matched_random,
            "assets": asset_rows,
            "positive_assets": sum(
                float(item["mean_net_bps"]) > 0 for item in asset_rows.values()
            ),
            "maximum_positive_contribution_share": concentration,
            "market": market,
        }
    )
    return summary


def _simple_selected_summary(rows: Sequence[R4Prediction]) -> dict[str, Any]:
    if not rows:
        return {
            "selected": 0,
            "mean_gross_bps": 0.0,
            "mean_net_bps": 0.0,
            "median_net_bps": 0.0,
            "mean_net_2x_slippage_bps": 0.0,
            "net_win_rate": 0.0,
            "edge_hit_rate": 0.0,
            "profit_factor": None,
            "net_sum": 0.0,
        }
    net = [item.net_return for item in rows]
    gains = math.fsum(value for value in net if value > 0)
    losses = -math.fsum(value for value in net if value < 0)
    return {
        "selected": len(rows),
        "mean_gross_bps": finite_mean(item.gross_return for item in rows) * 10_000,
        "mean_net_bps": finite_mean(net) * 10_000,
        "median_net_bps": median(net) * 10_000,
        "mean_net_2x_slippage_bps": (
            finite_mean(item.net_return_2x_slippage for item in rows) * 10_000
        ),
        "net_win_rate": sum(value > 0 for value in net) / len(net),
        "edge_hit_rate": sum(item.actual_edge for item in rows) / len(rows),
        "profit_factor": gains / losses if losses > 0 else None,
        "net_sum": math.fsum(net),
    }


def _calendar_block_bootstrap(
    rows: Sequence[R4Prediction],
    spec: R4ForecastSpec,
    market: str,
) -> dict[str, float | int | None]:
    by_day: dict[int, tuple[int, float, int]] = defaultdict(lambda: (0, 0.0, 0))
    for item in rows:
        selected_count, selected_sum, raw_count = by_day[item.utc_day]
        if item.selected:
            selected_count += 1
            selected_sum += item.net_return
        by_day[item.utc_day] = (selected_count, selected_sum, raw_count + 1)
    minimum_day = min(by_day)
    maximum_day = max(by_day)
    calendar = [
        by_day.get(day, (0, 0.0, 0))
        for day in range(minimum_day, maximum_day + 1)
    ]
    rng = random.Random(spec.seed + (0 if market == "spot" else 1))
    means: list[float] = []
    contributions: list[float] = []
    block = min(spec.bootstrap_block_days, len(calendar))
    for _ in range(spec.bootstrap_samples):
        selected_count = 0
        selected_sum = 0.0
        raw_count = 0
        drawn = 0
        while drawn < len(calendar):
            start = rng.randrange(len(calendar))
            for offset in range(min(block, len(calendar) - drawn)):
                count, value, raw = calendar[(start + offset) % len(calendar)]
                selected_count += count
                selected_sum += value
                raw_count += raw
                drawn += 1
        if selected_count:
            means.append(selected_sum / selected_count)
        if raw_count:
            contributions.append(selected_sum / raw_count)
    if not means or not contributions:
        return {
            "samples": spec.bootstrap_samples,
            "mean_net_lower_95_bps": None,
            "unconditional_lower_95_bps": None,
            "one_sided_p_value": 1.0,
        }
    return {
        "samples": spec.bootstrap_samples,
        "mean_net_lower_95_bps": _quantile(means, 0.05) * 10_000,
        "unconditional_lower_95_bps": _quantile(contributions, 0.05) * 10_000,
        "one_sided_p_value": (1 + sum(value <= 0 for value in means)) / (
            len(means) + 1
        ),
    }


def _matched_random_uplift(
    rows: Sequence[R4Prediction],
    spec: R4ForecastSpec,
    market: str,
) -> dict[str, float | int | None]:
    selected = [item for item in rows if item.selected]
    if not selected:
        return {
            "samples": spec.matched_random_samples,
            "mean_uplift_bps": None,
            "lower_95_uplift_bps": None,
        }
    strata: dict[tuple[str, str], list[R4Prediction]] = defaultdict(list)
    selected_counts: dict[tuple[str, str], int] = defaultdict(int)
    for item in rows:
        strata[(item.fold, item.cohort)].append(item)
        if item.selected:
            selected_counts[(item.fold, item.cohort)] += 1
    selected_mean = finite_mean(item.net_return for item in selected)
    rng = random.Random(spec.seed + (11 if market == "spot" else 17))
    uplifts: list[float] = []
    for _ in range(spec.matched_random_samples):
        sampled: list[float] = []
        for key, count in selected_counts.items():
            pool = strata[key]
            sampled.extend(item.net_return for item in rng.sample(pool, count))
        uplifts.append(selected_mean - finite_mean(sampled))
    return {
        "samples": spec.matched_random_samples,
        "mean_uplift_bps": finite_mean(uplifts) * 10_000,
        "lower_95_uplift_bps": _quantile(uplifts, 0.05) * 10_000,
    }


def _apply_holm(results: dict[str, dict[str, Any]]) -> None:
    ordered = sorted(
        (
            (float(result["bootstrap"]["one_sided_p_value"]), market)
            for market, result in results.items()
        )
    )
    keep_rejecting = True
    total = len(ordered)
    for rank, (p_value, market) in enumerate(ordered, start=1):
        threshold = 0.05 / (total - rank + 1)
        rejected = keep_rejecting and p_value <= threshold
        results[market]["holm"] = {
            "rank": rank,
            "p_value": p_value,
            "threshold": threshold,
            "reject_nonpositive_mean": rejected,
        }
        if not rejected:
            keep_rejecting = False


def _acceptance_checks(
    result: dict[str, Any], spec: R4ForecastSpec
) -> dict[str, bool]:
    acceptance = spec.acceptance
    bootstrap = result["bootstrap"]
    random_result = result["matched_random"]
    pf = result["profit_factor"]
    return {
        "minimum_selected": result["selected"] >= acceptance.minimum_selected,
        "minimum_days": result["selected_days"] >= acceptance.minimum_days,
        "coverage_range": (
            acceptance.minimum_coverage
            <= result["coverage"]
            <= acceptance.maximum_coverage
        ),
        "mean_net_above_5bp": result["mean_net_bps"] > spec.edge_margin_bps,
        "bootstrap_lower_above_zero": (
            bootstrap["mean_net_lower_95_bps"] is not None
            and bootstrap["mean_net_lower_95_bps"] > 0
        ),
        "holm_rejects_nonpositive_mean": result["holm"][
            "reject_nonpositive_mean"
        ],
        "profit_factor": pf is not None and pf > acceptance.minimum_profit_factor,
        "two_x_slippage_nonnegative": result["mean_net_2x_slippage_bps"] >= 0,
        "positive_assets": (
            result["positive_assets"] >= acceptance.minimum_positive_assets
        ),
        "concentration": (
            result["maximum_positive_contribution_share"]
            <= acceptance.maximum_positive_contribution_share
        ),
        "unconditional_contribution": (
            result["unconditional_contribution_bps"] > 0
            and bootstrap["unconditional_lower_95_bps"] is not None
            and bootstrap["unconditional_lower_95_bps"] > 0
        ),
        "matched_random_uplift": (
            random_result["lower_95_uplift_bps"] is not None
            and random_result["lower_95_uplift_bps"] > 0
        ),
        "brier_skill": result["brier_skill_score"] > 0,
        "log_loss": result["log_loss"] < result["climatology_log_loss"],
        "ece": result["ece"] <= acceptance.maximum_ece,
    }


def _render_report(result: dict[str, Any]) -> str:
    lines = [
        "# R4a 선택형 예측 게이트 백테스트",
        "",
        f"- 상태: **{result['status']}**",
        f"- 일반화: **{result['generalization']}**",
        f"- 실행 유효성: **{result['execution_validity']}**",
        f"- 배포: **{result['deployment']}**",
        "",
        "## 핵심 결과",
        "",
        "| 시장 | 선택/N | Coverage | Mean net | 2x slip | PF | Brier skill | ECE | 판정 |",
        "|---|---:|---:|---:|---:|---:|---:|---:|---|",
    ]
    for market in ("spot", "futures"):
        item = result["markets"][market]
        pf = "N/A" if item["profit_factor"] is None else f"{item['profit_factor']:.3f}"
        lines.append(
            "| "
            f"{market} | {item['selected']:,}/{item['raw_candidates']:,} | "
            f"{item['coverage']:.2%} | {item['mean_net_bps']:.2f} bp | "
            f"{item['mean_net_2x_slippage_bps']:.2f} bp | {pf} | "
            f"{item['brier_skill_score']:.4f} | {item['ece']:.4f} | "
            f"{item['status']} |"
        )
    lines.extend(
        [
            "",
            "## 해석",
            "",
            "R4a는 기존 C0 이벤트를 비용 후 +5bp edge 확률과 기대 순수익으로 "
            "선별한 빠른 진단입니다. 임계값과 특징은 실행 전에 동결됐으며, "
            "실패한 시장은 라이브 알림에 통합하지 않습니다.",
            "",
            "이 결과는 전체 5분봉 상승·하락 확률이 아닙니다. Spot breakout-long과 "
            "Futures breakdown-short 원장만 포함하며, 양방향 R4b와 기술적 종료 성과는 "
            "R4a 통과 후 별도로 검증해야 합니다.",
            "",
            "## 합격 조건 상세",
            "",
        ]
    )
    for market in ("spot", "futures"):
        lines.append(f"### {market}")
        lines.append("")
        for name, passed in result["markets"][market]["acceptance"].items():
            lines.append(f"- {'PASS' if passed else 'FAIL'}: `{name}`")
        lines.append("")
    lines.extend(["## 제한", ""])
    lines.extend(f"- {value}" for value in result["limitations"])
    lines.append("")
    return "\n".join(lines)


def _validate_fold_population(
    train: Sequence[R4Observation],
    calibration: Sequence[R4Observation],
    test: Sequence[R4Observation],
    market: str,
    test_start: datetime,
    edge_margin: float,
) -> None:
    if not train or not calibration or not test:
        raise ValueError(f"empty R4 fold for {market}:{test_start:%Y-%m}")
    for name, rows in (("train", train), ("calibration", calibration)):
        targets = {item.net_return > edge_margin for item in rows}
        if len(targets) != 2:
            raise ValueError(f"{name} fold lacks both classes for {market}:{test_start:%Y-%m}")


def _required_text(row: dict[str, str], field: str, row_number: int) -> str:
    value = row.get(field, "").strip()
    if not value:
        raise ValueError(f"missing {field} at row {row_number}")
    return value


def _finite_float(row: dict[str, str], field: str, row_number: int) -> float:
    value = float(_required_text(row, field, row_number))
    if not math.isfinite(value):
        raise ValueError(f"non-finite {field} at row {row_number}")
    return value


def _strict_bool(row: dict[str, str], field: str, row_number: int) -> bool:
    value = _required_text(row, field, row_number)
    if value not in {"True", "False"}:
        raise ValueError(f"invalid Boolean {field} at row {row_number}")
    return value == "True"


def _add_months(value: datetime, months: int) -> datetime:
    zero_based = value.year * 12 + value.month - 1 + months
    year, month_index = divmod(zero_based, 12)
    return value.replace(year=year, month=month_index + 1, day=1)


def _months_between(start: datetime, end: datetime) -> int:
    return (end.year - start.year) * 12 + end.month - start.month


def _quantile(values: Sequence[float], probability: float) -> float:
    if not values or not 0 <= probability <= 1:
        raise ValueError("quantile requires values and probability in [0, 1]")
    ordered = sorted(values)
    index = probability * (len(ordered) - 1)
    lower = math.floor(index)
    upper = math.ceil(index)
    if lower == upper:
        return ordered[lower]
    fraction = index - lower
    return ordered[lower] * (1 - fraction) + ordered[upper] * fraction


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def _json(value: object) -> str:
    return json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True) + "\n"
