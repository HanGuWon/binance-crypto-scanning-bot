"""Frozen, exposed walk-forward diagnostic for historical three-family signals.

This module intentionally owns a separate publication boundary from the
authenticated bootstrap report.  The bootstrap is an inference audit, whereas
this diagnostic fits two fixed models and therefore needs its own explicit
feature, fold, solver, and non-promotion contract.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import io
import json
import math
import os
import shutil
import tempfile
from collections import Counter
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Final, Literal, cast

HISTORICAL_THREE_FAMILY_WALK_FORWARD_PROTOCOL_V1: Final = (
    "historical_three_family_frozen_walk_forward_diagnostic_v1_2026-07-21"
)
HISTORICAL_THREE_FAMILY_WALK_FORWARD_SCHEMA_VERSION_V1: Final = 1
HISTORICAL_THREE_FAMILY_WALK_FORWARD_STATUS_V1: Final = "EXPOSED_HISTORICAL_ONLY"

FROZEN_CONSENSUS_SHA256_V1: Final = (
    "a8fd55e4ce439629d4ec9df92511420415c53b2ed526e882c70c62e81b19e168"
)
FROZEN_FIXED_HORIZON_OUTCOMES_SHA256_V1: Final = (
    "040ec3915aa8db0e8f954b17646d2e3e409b50c712e993de632a3b8fc2153778"
)

FIVE_MINUTE_MS_V1: Final = 5 * 60 * 1_000
DAY_MS_V1: Final = 24 * 60 * 60 * 1_000
INITIAL_TRAINING_DAYS_V1: Final = 180
EMBARGO_BARS_V1: Final = 72
EMBARGO_MS_V1: Final = EMBARGO_BARS_V1 * FIVE_MINUTE_MS_V1
TEST_WINDOW_DAYS_V1: Final = 30
TEST_WINDOW_MS_V1: Final = TEST_WINDOW_DAYS_V1 * DAY_MS_V1
TARGET_HORIZON_BARS_V1: Final = 12
TARGET_HORIZON_MINUTES_V1: Final = 60
RIDGE_LAMBDA_V1: Final = 10.0
LOGISTIC_LAMBDA_V1: Final = 10.0
LOGISTIC_MAX_ITERATIONS_V1: Final = 100
LOGISTIC_TOLERANCE_V1: Final = 1e-10
RIDGE_GATE_BPS_V1: Final = 0.0
LOGISTIC_GATE_PROBABILITY_V1: Final = 0.5

_MICROS_PER_BASIS_POINT: Final = 100
_MICROS_PER_UNIT: Final = 1_000_000
_SHA256_HEX_LENGTH: Final = 64
_OUTPUT_NAMES: Final = (
    "fold_models.json",
    "folds.csv",
    "predictions.csv",
    "report.ko.md",
    "results.json",
)
_PUBLISHED_NAMES: Final = frozenset((*_OUTPUT_NAMES, "manifest.json"))
_CONTINUOUS_FEATURE_NAMES: Final = (
    "primary_side",
    "price_signed_strength",
    "participation_signed_strength",
    "cross_section_signed_strength",
    "absolute_directional_agreement",
    "round_trip_cost_to_atr",
)
_CONSENSUS_REQUIRED_COLUMNS: Final = frozenset(
    {
        "admitted",
        "asset",
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
        "atr_fraction_micros",
    }
)
_OUTCOME_REQUIRED_COLUMNS: Final = frozenset(
    {
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
    }
)


class HistoricalThreeFamilyWalkForwardErrorV1(ValueError):
    """Raised when the frozen diagnostic contract cannot be proved."""


@dataclass(frozen=True, slots=True)
class FrozenWalkForwardContractV1:
    """The complete outcome-affecting contract; callers cannot override it."""

    initial_training_days: Literal[180] = field(
        default=INITIAL_TRAINING_DAYS_V1, init=False
    )
    embargo_bars: Literal[72] = field(default=EMBARGO_BARS_V1, init=False)
    test_window_days: Literal[30] = field(default=TEST_WINDOW_DAYS_V1, init=False)
    target_horizon_bars: Literal[12] = field(
        default=TARGET_HORIZON_BARS_V1, init=False
    )
    ridge_lambda: float = field(default=RIDGE_LAMBDA_V1, init=False)
    logistic_lambda: float = field(default=LOGISTIC_LAMBDA_V1, init=False)
    logistic_max_iterations: Literal[100] = field(
        default=LOGISTIC_MAX_ITERATIONS_V1, init=False
    )
    logistic_tolerance: float = field(default=LOGISTIC_TOLERANCE_V1, init=False)
    ridge_gate_bps: float = field(default=RIDGE_GATE_BPS_V1, init=False)
    logistic_gate_probability: float = field(
        default=LOGISTIC_GATE_PROBABILITY_V1, init=False
    )


FROZEN_WALK_FORWARD_CONTRACT_V1: Final = FrozenWalkForwardContractV1()


@dataclass(frozen=True, slots=True)
class WalkForwardObservationV1:
    """One exactly joined, evaluable 60-minute historical observation."""

    event_id: str
    split: str
    asset: str
    symbol: str
    side: Literal["long", "short"]
    decision_time_ms: int
    price_signed_strength: float
    participation_signed_strength: float
    cross_section_signed_strength: float
    absolute_directional_agreement: float
    round_trip_cost_to_atr: float
    net_return_micros: int

    def __post_init__(self) -> None:
        _require_sha256(self.event_id, "event_id")
        if not self.split or not self.asset or not self.symbol:
            raise HistoricalThreeFamilyWalkForwardErrorV1(
                "split, asset, and symbol must be non-empty"
            )
        if self.side not in {"long", "short"}:
            raise HistoricalThreeFamilyWalkForwardErrorV1("side must be long or short")
        if type(self.decision_time_ms) is not int or self.decision_time_ms < 0:
            raise HistoricalThreeFamilyWalkForwardErrorV1(
                "decision_time_ms must be a nonnegative integer"
            )
        feature_values = (
            self.price_signed_strength,
            self.participation_signed_strength,
            self.cross_section_signed_strength,
            self.absolute_directional_agreement,
            self.round_trip_cost_to_atr,
        )
        if any(not math.isfinite(value) for value in feature_values):
            raise HistoricalThreeFamilyWalkForwardErrorV1("features must be finite")
        if self.absolute_directional_agreement < 0 or self.round_trip_cost_to_atr < 0:
            raise HistoricalThreeFamilyWalkForwardErrorV1(
                "agreement and cost/ATR features must be nonnegative"
            )
        if type(self.net_return_micros) is not int:
            raise HistoricalThreeFamilyWalkForwardErrorV1(
                "net_return_micros must be an exact integer"
            )

    @property
    def target_positive(self) -> bool:
        return self.net_return_micros > 0

    @property
    def side_value(self) -> float:
        return 1.0 if self.side == "long" else -1.0

    @property
    def continuous_features(self) -> tuple[float, ...]:
        return (
            self.side_value,
            self.price_signed_strength,
            self.participation_signed_strength,
            self.cross_section_signed_strength,
            self.absolute_directional_agreement,
            self.round_trip_cost_to_atr,
        )


@dataclass(frozen=True, slots=True)
class WalkForwardSliceV1:
    fold: int
    training_start_ms: int
    training_cutoff_ms_exclusive: int
    embargo_start_ms: int
    test_start_ms: int
    test_end_ms_exclusive: int
    train_indices: tuple[int, ...]
    embargo_indices: tuple[int, ...]
    test_indices: tuple[int, ...]
    last_partial: bool

    def __post_init__(self) -> None:
        if self.training_cutoff_ms_exclusive != self.embargo_start_ms:
            raise HistoricalThreeFamilyWalkForwardErrorV1(
                "training cutoff and embargo start must be identical"
            )
        if self.test_start_ms - self.embargo_start_ms != EMBARGO_MS_V1:
            raise HistoricalThreeFamilyWalkForwardErrorV1(
                "fold does not contain the frozen 72-bar embargo"
            )
        if self.test_end_ms_exclusive <= self.test_start_ms:
            raise HistoricalThreeFamilyWalkForwardErrorV1("test interval must be positive")
        if set(self.train_indices) & set(self.embargo_indices):
            raise HistoricalThreeFamilyWalkForwardErrorV1(
                "training and embargo populations overlap"
            )
        if set(self.train_indices) & set(self.test_indices):
            raise HistoricalThreeFamilyWalkForwardErrorV1(
                "training and test populations overlap"
            )
        if set(self.embargo_indices) & set(self.test_indices):
            raise HistoricalThreeFamilyWalkForwardErrorV1(
                "embargo and test populations overlap"
            )


@dataclass(frozen=True, slots=True)
class TrainOnlyFeatureTransformV1:
    """Continuous scaling and train-universe one-hot encoding fitted on one fold."""

    means: tuple[float, ...]
    scales: tuple[float, ...]
    assets: tuple[str, ...]

    def __post_init__(self) -> None:
        if len(self.means) != len(_CONTINUOUS_FEATURE_NAMES):
            raise HistoricalThreeFamilyWalkForwardErrorV1(
                "continuous transform dimension is not frozen"
            )
        if len(self.means) != len(self.scales):
            raise HistoricalThreeFamilyWalkForwardErrorV1(
                "transform means and scales differ in dimension"
            )
        if any(not math.isfinite(value) for value in self.means):
            raise HistoricalThreeFamilyWalkForwardErrorV1("transform means must be finite")
        if any(not math.isfinite(value) or value <= 0 for value in self.scales):
            raise HistoricalThreeFamilyWalkForwardErrorV1(
                "transform scales must be finite and positive"
            )
        if self.assets != tuple(sorted(set(self.assets))) or not self.assets:
            raise HistoricalThreeFamilyWalkForwardErrorV1(
                "training assets must be non-empty, unique, and canonical"
            )

    @property
    def feature_names(self) -> tuple[str, ...]:
        return (*_CONTINUOUS_FEATURE_NAMES, *(f"asset::{asset}" for asset in self.assets))

    def transform(self, row: WalkForwardObservationV1) -> tuple[float, ...]:
        standardized = tuple(
            (value - mean) / scale
            for value, mean, scale in zip(
                row.continuous_features,
                self.means,
                self.scales,
                strict=True,
            )
        )
        # One-hots are deliberately not centered.  An unseen test asset is the
        # explicit all-zero vector and is counted in each fold's audit.
        one_hot = tuple(1.0 if row.asset == asset else 0.0 for asset in self.assets)
        return (*standardized, *one_hot)


@dataclass(frozen=True, slots=True)
class WalkForwardPredictionV1:
    fold: int
    event_id: str
    split: str
    asset: str
    side: Literal["long", "short"]
    decision_time_ms: int
    net_return_micros: int
    ridge_expected_net_bps: float
    logistic_positive_probability: float
    fixed_gate_selected: bool
    unseen_test_asset: bool


@dataclass(frozen=True, slots=True)
class WalkForwardFoldEvaluationV1:
    slice: WalkForwardSliceV1
    transform: TrainOnlyFeatureTransformV1
    ridge_coefficients: tuple[float, ...]
    logistic_coefficients: tuple[float, ...]
    logistic_iterations: int
    predictions: tuple[WalkForwardPredictionV1, ...]


@dataclass(frozen=True, slots=True)
class LoadedWalkForwardInputsV1:
    observations: tuple[WalkForwardObservationV1, ...]
    consensus_sha256: str
    outcomes_sha256: str
    join_audit: Mapping[str, object]
    availability_audit: Mapping[str, object]


@dataclass(frozen=True, slots=True)
class PublishedWalkForwardArtifactsV1:
    output_dir: Path
    manifest_sha256: str
    results_sha256: str
    prediction_count: int
    fold_count: int


@dataclass(frozen=True, slots=True)
class _AdmittedConsensusRowV1:
    event_id: str
    split: str
    asset: str
    symbol: str
    side: Literal["long", "short"]
    decision_time_ms: int
    directional_agreement_micros: int
    price_direction: int
    price_strength_micros: int
    participation_direction: int
    participation_strength_micros: int
    cross_section_direction: int
    cross_section_strength_micros: int
    zero_move_round_trip_cost_micros: int
    atr_fraction_micros: int


@dataclass(frozen=True, slots=True)
class _OutcomeRowV1:
    event_id: str
    split: str
    asset: str
    symbol: str
    side: Literal["long", "short"]
    decision_time_ms: int
    directional_agreement_micros: int
    evaluable: bool
    exclusion_reason: str
    net_return_micros: int | None


@dataclass(frozen=True, slots=True)
class _LinearModelV1:
    coefficients: tuple[float, ...]

    def predict(self, features: Sequence[float]) -> float:
        if len(self.coefficients) != len(features) + 1:
            raise HistoricalThreeFamilyWalkForwardErrorV1(
                "prediction feature dimension differs from fitted model"
            )
        value = self.coefficients[0] + math.fsum(
            coefficient * feature
            for coefficient, feature in zip(
                self.coefficients[1:], features, strict=True
            )
        )
        if not math.isfinite(value):
            raise HistoricalThreeFamilyWalkForwardErrorV1(
                "linear model produced a non-finite value"
            )
        return value


def frozen_walk_forward_contract_document_v1() -> dict[str, object]:
    """Return the complete immutable, non-promoting model contract."""

    return {
        "status": HISTORICAL_THREE_FAMILY_WALK_FORWARD_STATUS_V1,
        "historical_only": True,
        "exposed": True,
        "promoting": False,
        "qualification": False,
        "paper_executable": False,
        "order_placement": False,
        "interval": "5m",
        "target": {
            "horizon_bars": TARGET_HORIZON_BARS_V1,
            "horizon_minutes": TARGET_HORIZON_MINUTES_V1,
            "regression": "net_return_micros / 100 basis points",
            "classification": "net_return_micros > 0",
        },
        "folds": {
            "anchor": "earliest_evaluable_time_plus_180_days_plus_6_hour_embargo",
            "initial_training_days": INITIAL_TRAINING_DAYS_V1,
            "embargo_bars": EMBARGO_BARS_V1,
            "embargo_minutes": EMBARGO_MS_V1 // 60_000,
            "test_window_days": TEST_WINDOW_DAYS_V1,
            "last_partial_fold": True,
            "refit": "expanding_prior_rows_only",
            "timestamp_rule": (
                "train decision_time < test_start-6h; "
                "test_start <= test decision_time < test_end"
            ),
        },
        "features": {
            "continuous_order": list(_CONTINUOUS_FEATURE_NAMES),
            "family_signed_strength": "family_direction * family_strength_micros / 1e6",
            "primary_side": "long=+1;short=-1",
            "absolute_directional_agreement": (
                "abs(directional_agreement_micros) / 1e6"
            ),
            "round_trip_cost_to_atr": (
                "zero_move_round_trip_cost_micros / atr_fraction_micros"
            ),
            "zero_atr_policy": "FAIL_CLOSED",
            "continuous_standardization": "training_population_mean_and_population_sd",
            "zero_variance_scale": "1",
            "asset_encoding": "canonical_sorted_full_one_hot_from_training_universe",
            "asset_one_hot_standardized": False,
            "unseen_test_asset": "all_zero_one_hot_and_explicit_count",
        },
        "models": {
            "ridge": {
                "lambda": "10",
                "solver": "closed_form_normal_equations_cholesky",
                "intercept_penalized": False,
                "coefficients_penalized": True,
            },
            "logistic": {
                "lambda": "10",
                "objective": "sum_log_loss_plus_lambda_over_2_times_l2_squared",
                "solver": "deterministic_newton_irls_with_backtracking",
                "intercept_penalized": False,
                "coefficients_penalized": True,
                "maximum_iterations": LOGISTIC_MAX_ITERATIONS_V1,
                "coefficient_delta_tolerance": "1e-10",
            },
        },
        "fixed_gate": {
            "rule": "ridge_expected_net_bps > 0 AND logistic_probability > 0.5",
            "ridge_strict_threshold_bps": "0",
            "logistic_strict_threshold": "0.5",
            "threshold_candidates": 1,
            "threshold_tuning": False,
            "outcome_conditioned_selection": False,
        },
        "descriptive_only": {
            "ridge_positive_subset_is_not_an_alternate_gate": True,
            "equal_count_deciles": 10,
            "decile_threshold_search": False,
        },
    }


def frozen_walk_forward_contract_sha256_v1() -> str:
    return _sha256_bytes(_json_bytes(frozen_walk_forward_contract_document_v1()))


def fit_train_only_feature_transform_v1(
    rows: Sequence[WalkForwardObservationV1],
) -> TrainOnlyFeatureTransformV1:
    """Fit continuous scaling and the asset vocabulary from training rows only."""

    if not rows:
        raise HistoricalThreeFamilyWalkForwardErrorV1(
            "feature transform requires at least one training row"
        )
    columns = tuple(zip(*(row.continuous_features for row in rows), strict=True))
    means = tuple(math.fsum(column) / len(column) for column in columns)
    scales: list[float] = []
    for column, mean in zip(columns, means, strict=True):
        variance = math.fsum((value - mean) ** 2 for value in column) / len(column)
        scale = math.sqrt(variance)
        scales.append(scale if scale > 0 else 1.0)
    return TrainOnlyFeatureTransformV1(
        means=means,
        scales=tuple(scales),
        assets=tuple(sorted({row.asset for row in rows})),
    )


def build_expanding_walk_forward_slices_v1(
    rows: Sequence[WalkForwardObservationV1],
) -> tuple[WalkForwardSliceV1, ...]:
    """Build the fixed 180d/6h/30d schedule from sorted evaluable rows."""

    _require_chronological_unique_observations(rows)
    earliest = rows[0].decision_time_ms
    data_end_exclusive = rows[-1].decision_time_ms + 1
    initial_training_end = earliest + INITIAL_TRAINING_DAYS_V1 * DAY_MS_V1
    first_test_start = initial_training_end + EMBARGO_MS_V1
    if first_test_start >= data_end_exclusive:
        raise HistoricalThreeFamilyWalkForwardErrorV1(
            "observations do not extend beyond the frozen initial train and embargo"
        )

    slices: list[WalkForwardSliceV1] = []
    test_start = first_test_start
    fold = 1
    while test_start < data_end_exclusive:
        nominal_end = test_start + TEST_WINDOW_MS_V1
        test_end = min(nominal_end, data_end_exclusive)
        training_cutoff = test_start - EMBARGO_MS_V1
        train_indices = tuple(
            index
            for index, row in enumerate(rows)
            if row.decision_time_ms < training_cutoff
        )
        embargo_indices = tuple(
            index
            for index, row in enumerate(rows)
            if training_cutoff <= row.decision_time_ms < test_start
        )
        test_indices = tuple(
            index
            for index, row in enumerate(rows)
            if test_start <= row.decision_time_ms < test_end
        )
        if not train_indices:
            raise HistoricalThreeFamilyWalkForwardErrorV1(
                f"fold {fold} has no prior training population"
            )
        if not test_indices:
            raise HistoricalThreeFamilyWalkForwardErrorV1(
                f"fold {fold} has no test observations; missing windows are not skipped"
            )
        result = WalkForwardSliceV1(
            fold=fold,
            training_start_ms=earliest,
            training_cutoff_ms_exclusive=training_cutoff,
            embargo_start_ms=training_cutoff,
            test_start_ms=test_start,
            test_end_ms_exclusive=test_end,
            train_indices=train_indices,
            embargo_indices=embargo_indices,
            test_indices=test_indices,
            last_partial=test_end < nominal_end,
        )
        _validate_slice_times(rows, result)
        slices.append(result)
        test_start = nominal_end
        fold += 1
    if sum(item.last_partial for item in slices) > 1:
        raise HistoricalThreeFamilyWalkForwardErrorV1(
            "walk-forward schedule contains multiple partial folds"
        )
    return tuple(slices)


def evaluate_frozen_walk_forward_v1(
    rows: Sequence[WalkForwardObservationV1],
) -> tuple[WalkForwardFoldEvaluationV1, ...]:
    """Fit and score both frozen models without any threshold search."""

    slices = build_expanding_walk_forward_slices_v1(rows)
    evaluations: list[WalkForwardFoldEvaluationV1] = []
    for fold_slice in slices:
        training = tuple(rows[index] for index in fold_slice.train_indices)
        test = tuple(rows[index] for index in fold_slice.test_indices)
        transform = fit_train_only_feature_transform_v1(training)
        train_matrix = tuple(transform.transform(row) for row in training)
        ridge = _fit_ridge_v1(
            train_matrix,
            tuple(row.net_return_micros / _MICROS_PER_BASIS_POINT for row in training),
        )
        logistic, iterations = _fit_logistic_v1(
            train_matrix,
            tuple(row.target_positive for row in training),
        )
        predictions: list[WalkForwardPredictionV1] = []
        for row in test:
            features = transform.transform(row)
            expected = ridge.predict(features)
            probability = _sigmoid(logistic.predict(features))
            selected = (
                expected > RIDGE_GATE_BPS_V1
                and probability > LOGISTIC_GATE_PROBABILITY_V1
            )
            predictions.append(
                WalkForwardPredictionV1(
                    fold=fold_slice.fold,
                    event_id=row.event_id,
                    split=row.split,
                    asset=row.asset,
                    side=row.side,
                    decision_time_ms=row.decision_time_ms,
                    net_return_micros=row.net_return_micros,
                    ridge_expected_net_bps=expected,
                    logistic_positive_probability=probability,
                    fixed_gate_selected=selected,
                    unseen_test_asset=row.asset not in transform.assets,
                )
            )
        evaluations.append(
            WalkForwardFoldEvaluationV1(
                slice=fold_slice,
                transform=transform,
                ridge_coefficients=ridge.coefficients,
                logistic_coefficients=logistic.coefficients,
                logistic_iterations=iterations,
                predictions=tuple(predictions),
            )
        )
    return tuple(evaluations)


def load_frozen_walk_forward_inputs_v1(
    consensus_path: str | Path,
    outcomes_path: str | Path,
) -> LoadedWalkForwardInputsV1:
    """Authenticate, exactly join, and audit the frozen consensus/outcome files."""

    consensus_raw = _read_frozen_input(
        consensus_path,
        FROZEN_CONSENSUS_SHA256_V1,
        "consensus.csv",
    )
    admitted, census_audit, availability = _parse_consensus_input(consensus_raw)
    outcomes_raw = _read_frozen_input(
        outcomes_path,
        FROZEN_FIXED_HORIZON_OUTCOMES_SHA256_V1,
        "fixed_horizon_outcomes.csv",
    )
    outcomes, outcome_audit = _parse_outcome_input(outcomes_raw)

    observations: list[WalkForwardObservationV1] = []
    mismatches = 0
    excluded: Counter[str] = Counter()
    ledger_mismatches = cast(int, outcome_audit["economic_ledger_mismatches"])
    for event_id, outcome in sorted(
        outcomes.items(), key=lambda item: (item[1].decision_time_ms, item[0])
    ):
        consensus = admitted.get(event_id)
        if consensus is None:
            continue
        if (
            consensus.split != outcome.split
            or consensus.asset != outcome.asset
            or consensus.symbol != outcome.symbol
            or consensus.side != outcome.side
            or consensus.decision_time_ms != outcome.decision_time_ms
            or consensus.directional_agreement_micros
            != outcome.directional_agreement_micros
        ):
            mismatches += 1
            continue
        if not outcome.evaluable:
            excluded[outcome.exclusion_reason] += 1
            continue
        if outcome.net_return_micros is None:
            raise HistoricalThreeFamilyWalkForwardErrorV1(
                "evaluable joined outcome is missing net return"
            )
        if consensus.atr_fraction_micros == 0:
            raise HistoricalThreeFamilyWalkForwardErrorV1(
                f"zero ATR denominator for joined event {event_id}"
            )
        observations.append(
            WalkForwardObservationV1(
                event_id=event_id,
                split=consensus.split,
                asset=consensus.asset,
                symbol=consensus.symbol,
                side=consensus.side,
                decision_time_ms=consensus.decision_time_ms,
                price_signed_strength=(
                    consensus.price_direction
                    * consensus.price_strength_micros
                    / _MICROS_PER_UNIT
                ),
                participation_signed_strength=(
                    consensus.participation_direction
                    * consensus.participation_strength_micros
                    / _MICROS_PER_UNIT
                ),
                cross_section_signed_strength=(
                    consensus.cross_section_direction
                    * consensus.cross_section_strength_micros
                    / _MICROS_PER_UNIT
                ),
                absolute_directional_agreement=(
                    abs(consensus.directional_agreement_micros) / _MICROS_PER_UNIT
                ),
                round_trip_cost_to_atr=(
                    consensus.zero_move_round_trip_cost_micros
                    / consensus.atr_fraction_micros
                ),
                net_return_micros=outcome.net_return_micros,
            )
        )

    consensus_ids = set(admitted)
    outcome_ids = set(outcomes)
    missing_outcome_ids = consensus_ids - outcome_ids
    orphan_outcome_ids = outcome_ids - consensus_ids
    if missing_outcome_ids or orphan_outcome_ids or mismatches or ledger_mismatches:
        raise HistoricalThreeFamilyWalkForwardErrorV1(
            "the 60-minute outcome and admitted consensus populations do not join exactly"
        )
    ordered = tuple(sorted(observations, key=lambda row: (row.decision_time_ms, row.event_id)))
    _require_chronological_unique_observations(ordered)
    join_audit: dict[str, object] = {
        **census_audit,
        **outcome_audit,
        "exact_one_to_one_60m_join": True,
        "joined_60m_rows": len(outcomes),
        "field_mismatches": mismatches,
        "missing_outcome_ids": len(missing_outcome_ids),
        "orphan_outcome_ids": len(orphan_outcome_ids),
        "evaluable_rows": len(ordered),
        "excluded_rows": sum(excluded.values()),
        "exclusions": dict(sorted(excluded.items())),
    }
    return LoadedWalkForwardInputsV1(
        observations=ordered,
        consensus_sha256=FROZEN_CONSENSUS_SHA256_V1,
        outcomes_sha256=FROZEN_FIXED_HORIZON_OUTCOMES_SHA256_V1,
        join_audit=join_audit,
        availability_audit=availability,
    )


def run_frozen_walk_forward_diagnostic_v1(
    *,
    consensus_path: str | Path,
    outcomes_path: str | Path,
    output_dir: str | Path,
) -> PublishedWalkForwardArtifactsV1:
    """Run and atomically publish the one frozen, non-promoting diagnostic."""

    loaded = load_frozen_walk_forward_inputs_v1(consensus_path, outcomes_path)
    evaluations = evaluate_frozen_walk_forward_v1(loaded.observations)
    predictions = tuple(
        prediction for evaluation in evaluations for prediction in evaluation.predictions
    )
    expected_oof_start = (
        loaded.observations[0].decision_time_ms
        + INITIAL_TRAINING_DAYS_V1 * DAY_MS_V1
        + EMBARGO_MS_V1
    )
    if predictions[0].decision_time_ms < expected_oof_start:
        raise HistoricalThreeFamilyWalkForwardErrorV1(
            "OOF predictions begin before the frozen training and embargo boundary"
        )

    results = _results_document(loaded, evaluations, predictions)
    payloads: dict[str, bytes] = {
        "fold_models.json": _json_bytes(_fold_models_document(evaluations)),
        "folds.csv": _folds_csv_bytes(evaluations),
        "predictions.csv": _predictions_csv_bytes(predictions),
        "report.ko.md": _render_report_ko(results).encode("utf-8"),
        "results.json": _json_bytes(results),
    }
    output_hashes = {name: _sha256_bytes(payload) for name, payload in payloads.items()}
    source_path = Path(__file__).resolve()
    manifest = {
        "protocol": HISTORICAL_THREE_FAMILY_WALK_FORWARD_PROTOCOL_V1,
        "schema_version": HISTORICAL_THREE_FAMILY_WALK_FORWARD_SCHEMA_VERSION_V1,
        "status": HISTORICAL_THREE_FAMILY_WALK_FORWARD_STATUS_V1,
        "historical_only": True,
        "exposed": True,
        "promoting": False,
        "qualification": False,
        "paper_executable": False,
        "order_placement": False,
        "threshold_tuning": False,
        "contract_sha256": frozen_walk_forward_contract_sha256_v1(),
        "code": {
            "relative_owner": "src/signalbot/backtest/historical_three_family_walk_forward.py",
            "sha256": _sha256_bytes(source_path.read_bytes()),
        },
        "inputs": {
            "consensus.csv": loaded.consensus_sha256,
            "fixed_horizon_outcomes.csv": loaded.outcomes_sha256,
        },
        "outputs": output_hashes,
    }
    # Close the read/compute/publish race: the exact inputs are read and hashed
    # again after all fitting and before any artifact becomes visible.
    _verify_frozen_input_unchanged(
        consensus_path,
        loaded.consensus_sha256,
        "consensus.csv",
    )
    _verify_frozen_input_unchanged(
        outcomes_path,
        loaded.outcomes_sha256,
        "fixed_horizon_outcomes.csv",
    )
    manifest["publication"] = {
        "source_inputs_reverified_immediately_before_publication": True,
        "staged_payloads_fsynced_before_atomic_directory_replace": True,
        "parent_directory_fsynced_after_replace_when_supported": True,
    }
    manifest_raw = _json_bytes(manifest)
    payloads["manifest.json"] = manifest_raw
    target = Path(output_dir).resolve()
    _publish_artifacts(target, payloads)
    return PublishedWalkForwardArtifactsV1(
        output_dir=target,
        manifest_sha256=_sha256_bytes(manifest_raw),
        results_sha256=output_hashes["results.json"],
        prediction_count=len(predictions),
        fold_count=len(evaluations),
    )


def _parse_consensus_input(
    raw: bytes,
) -> tuple[
    dict[str, _AdmittedConsensusRowV1],
    dict[str, object],
    dict[str, object],
]:
    reader = _csv_reader(raw, _CONSENSUS_REQUIRED_COLUMNS, "consensus")
    admitted: dict[str, _AdmittedConsensusRowV1] = {}
    seen: set[str] = set()
    monthly: dict[str, Counter[str]] = {}
    row_count = 0
    duplicate_count = 0
    for row_number, record in enumerate(reader, start=2):
        row_count += 1
        event_id = _required_text(record, "event_id", row_number)
        _require_sha256(event_id, f"consensus row {row_number} event_id")
        if event_id in seen:
            duplicate_count += 1
            continue
        seen.add(event_id)
        decision_time_ms = _required_int(record, "decision_time_ms", row_number)
        month = datetime.fromtimestamp(decision_time_ms / 1_000, UTC).strftime("%Y-%m")
        status = _required_text(record, "participation_status", row_number)
        monthly.setdefault(month, Counter())[status] += 1
        if not _required_bool(record, "admitted", row_number):
            continue
        side = _required_side(record, "primary_direction", row_number)
        admitted[event_id] = _AdmittedConsensusRowV1(
            event_id=event_id,
            split=_required_text(record, "split", row_number),
            asset=_required_text(record, "asset", row_number),
            symbol=_required_text(record, "symbol", row_number),
            side=side,
            decision_time_ms=decision_time_ms,
            directional_agreement_micros=_required_int(
                record, "directional_agreement_micros", row_number
            ),
            price_direction=_required_direction(record, "price_direction", row_number),
            price_strength_micros=_required_nonnegative_int(
                record, "price_strength_micros", row_number
            ),
            participation_direction=_required_direction(
                record, "participation_direction", row_number
            ),
            participation_strength_micros=_required_nonnegative_int(
                record, "participation_strength_micros", row_number
            ),
            cross_section_direction=_required_direction(
                record, "cross_section_direction", row_number
            ),
            cross_section_strength_micros=_required_nonnegative_int(
                record, "cross_section_strength_micros", row_number
            ),
            zero_move_round_trip_cost_micros=_required_nonnegative_int(
                record, "zero_move_round_trip_cost_micros", row_number
            ),
            atr_fraction_micros=_required_nonnegative_int(
                record, "atr_fraction_micros", row_number
            ),
        )
    if duplicate_count:
        raise HistoricalThreeFamilyWalkForwardErrorV1(
            "consensus contains duplicate event IDs"
        )
    monthly_rows = []
    hole_months = []
    for month, counts in sorted(monthly.items()):
        ready = counts.get("READY", 0)
        inconclusive = counts.get("INCONCLUSIVE_DATA", 0)
        is_hole = inconclusive > ready
        if is_hole:
            hole_months.append(month)
        monthly_rows.append(
            {
                "utc_month": month,
                "ready": ready,
                "inconclusive_data": inconclusive,
                "other": sum(counts.values()) - ready - inconclusive,
                "source_rows": sum(counts.values()),
                "participation_hole": is_hole,
            }
        )
    return (
        admitted,
        {
            "consensus_rows": row_count,
            "consensus_unique_event_ids": len(seen),
            "consensus_duplicate_event_ids": duplicate_count,
            "admitted_consensus_rows": len(admitted),
        },
        {
            "hole_rule": (
                "UTC month where participation_status INCONCLUSIVE_DATA count "
                "exceeds READY count; source-only and outcome-blind"
            ),
            "participation_hole_months": hole_months,
            "monthly_participation_status": monthly_rows,
        },
    )


def _parse_outcome_input(
    raw: bytes,
) -> tuple[dict[str, _OutcomeRowV1], dict[str, object]]:
    reader = _csv_reader(raw, _OUTCOME_REQUIRED_COLUMNS, "fixed-horizon outcomes")
    outcomes: dict[str, _OutcomeRowV1] = {}
    seen_keys: set[tuple[str, int]] = set()
    total_rows = 0
    duplicate_keys = 0
    ledger_mismatches = 0
    horizon_counts: Counter[int] = Counter()
    for row_number, record in enumerate(reader, start=2):
        total_rows += 1
        event_id = _required_text(record, "event_id", row_number)
        _require_sha256(event_id, f"outcome row {row_number} event_id")
        horizon = _required_int(record, "horizon_bars", row_number)
        horizon_counts[horizon] += 1
        key = (event_id, horizon)
        if key in seen_keys:
            duplicate_keys += 1
            continue
        seen_keys.add(key)
        if _required_int(record, "horizon_minutes", row_number) != horizon * 5:
            raise HistoricalThreeFamilyWalkForwardErrorV1(
                f"outcome row {row_number} horizon minutes do not reconcile"
            )
        for claim_field, expected in (
            ("historical_only", True),
            ("probability", False),
            ("probability_calibrated", False),
            ("promoting", False),
            ("order_placement", False),
        ):
            if _required_bool(record, claim_field, row_number) is not expected:
                raise HistoricalThreeFamilyWalkForwardErrorV1(
                    f"outcome row {row_number} violates non-promoting claim {claim_field}"
                )
        if horizon != TARGET_HORIZON_BARS_V1:
            continue
        evaluable = _required_bool(record, "evaluable", row_number)
        exclusion = record.get("exclusion_reason", "")
        net: int | None = None
        if evaluable:
            gross = _required_int(record, "gross_directional_return_micros", row_number)
            slippage = _required_nonnegative_int(
                record, "slippage_return_micros", row_number
            )
            fee = _required_nonnegative_int(record, "fee_return_micros", row_number)
            funding = _required_int(record, "funding_return_micros", row_number)
            residual = _required_int(record, "rounding_residual_micros", row_number)
            total_cost = _required_nonnegative_int(record, "total_cost_micros", row_number)
            net = _required_int(record, "net_return_micros", row_number)
            if total_cost != slippage + fee - residual or gross - total_cost + funding != net:
                ledger_mismatches += 1
        elif not exclusion:
            raise HistoricalThreeFamilyWalkForwardErrorV1(
                f"excluded outcome row {row_number} has no exclusion reason"
            )
        outcomes[event_id] = _OutcomeRowV1(
            event_id=event_id,
            split=_required_text(record, "split", row_number),
            asset=_required_text(record, "asset", row_number),
            symbol=_required_text(record, "symbol", row_number),
            side=_required_side(record, "primary_direction", row_number),
            decision_time_ms=_required_int(record, "decision_time_ms", row_number),
            directional_agreement_micros=_required_int(
                record, "directional_agreement_micros", row_number
            ),
            evaluable=evaluable,
            exclusion_reason=exclusion,
            net_return_micros=net,
        )
    if duplicate_keys:
        raise HistoricalThreeFamilyWalkForwardErrorV1(
            "fixed-horizon outcomes contain duplicate event/horizon keys"
        )
    if ledger_mismatches:
        raise HistoricalThreeFamilyWalkForwardErrorV1(
            "fixed-horizon outcomes contain economic ledger mismatches"
        )
    if set(horizon_counts) != {1, 3, 6, 12, 72} or len(set(horizon_counts.values())) != 1:
        raise HistoricalThreeFamilyWalkForwardErrorV1(
            "fixed-horizon outcomes do not contain an exact five-horizon census"
        )
    return outcomes, {
        "fixed_horizon_total_rows": total_rows,
        "fixed_horizon_unique_event_horizon_keys": len(seen_keys),
        "fixed_horizon_duplicate_event_horizon_keys": duplicate_keys,
        "horizon_row_counts": {
            str(horizon): count for horizon, count in sorted(horizon_counts.items())
        },
        "target_60m_rows": len(outcomes),
        "target_60m_unique_event_ids": len(outcomes),
        "economic_ledger_mismatches": ledger_mismatches,
    }


def _fit_ridge_v1(
    matrix: Sequence[Sequence[float]],
    targets: Sequence[float],
) -> _LinearModelV1:
    design = _validate_model_inputs(matrix, targets)
    dimension = len(design[0])
    normal = [[0.0] * dimension for _ in range(dimension)]
    rhs = [0.0] * dimension
    for features, target in zip(design, targets, strict=True):
        for row_index, left in enumerate(features):
            rhs[row_index] += left * target
            for column_index in range(row_index + 1):
                normal[row_index][column_index] += left * features[column_index]
    for row_index in range(dimension):
        for column_index in range(row_index):
            normal[column_index][row_index] = normal[row_index][column_index]
        if row_index > 0:
            normal[row_index][row_index] += RIDGE_LAMBDA_V1
    return _LinearModelV1(tuple(_solve_positive_definite(normal, rhs)))


def _fit_logistic_v1(
    matrix: Sequence[Sequence[float]],
    targets: Sequence[bool],
) -> tuple[_LinearModelV1, int]:
    numeric_targets = tuple(float(target) for target in targets)
    design = _validate_model_inputs(matrix, numeric_targets)
    positives = sum(targets)
    if positives == 0 or positives == len(targets):
        raise HistoricalThreeFamilyWalkForwardErrorV1(
            "logistic training population must contain both target classes"
        )
    dimension = len(design[0])
    prior = positives / len(targets)
    coefficients = [math.log(prior / (1.0 - prior)), *([0.0] * (dimension - 1))]
    current_objective = _logistic_objective(design, numeric_targets, coefficients)
    for iteration in range(1, LOGISTIC_MAX_ITERATIONS_V1 + 1):
        hessian = [[0.0] * dimension for _ in range(dimension)]
        negative_gradient = [0.0] * dimension
        for features, target in zip(design, numeric_targets, strict=True):
            probability = _sigmoid(_dot(coefficients, features))
            weight = probability * (1.0 - probability)
            residual = target - probability
            for row_index, left in enumerate(features):
                negative_gradient[row_index] += left * residual
                for column_index in range(row_index + 1):
                    hessian[row_index][column_index] += (
                        weight * left * features[column_index]
                    )
        for row_index in range(dimension):
            for column_index in range(row_index):
                hessian[column_index][row_index] = hessian[row_index][column_index]
            if row_index > 0:
                hessian[row_index][row_index] += LOGISTIC_LAMBDA_V1
                negative_gradient[row_index] -= (
                    LOGISTIC_LAMBDA_V1 * coefficients[row_index]
                )
        delta = _solve_positive_definite(hessian, negative_gradient)
        step = 1.0
        accepted: list[float] | None = None
        accepted_objective = current_objective
        while step >= 2.0**-30:
            candidate = [
                coefficient + step * change
                for coefficient, change in zip(coefficients, delta, strict=True)
            ]
            objective = _logistic_objective(design, numeric_targets, candidate)
            if objective <= current_objective:
                accepted = candidate
                accepted_objective = objective
                break
            step *= 0.5
        if accepted is None:
            raise HistoricalThreeFamilyWalkForwardErrorV1(
                "logistic Newton backtracking could not decrease the objective"
            )
        coefficients = accepted
        current_objective = accepted_objective
        if max(abs(step * change) for change in delta) <= LOGISTIC_TOLERANCE_V1:
            return _LinearModelV1(tuple(coefficients)), iteration
    raise HistoricalThreeFamilyWalkForwardErrorV1(
        "logistic solver did not converge within the frozen iteration cap"
    )


def _results_document(
    loaded: LoadedWalkForwardInputsV1,
    evaluations: Sequence[WalkForwardFoldEvaluationV1],
    predictions: Sequence[WalkForwardPredictionV1],
) -> dict[str, object]:
    selected = tuple(row for row in predictions if row.fixed_gate_selected)
    ridge_positive = tuple(row for row in predictions if row.ridge_expected_net_bps > 0)
    folds = [_fold_result_document(evaluation) for evaluation in evaluations]
    fold_negative_count = sum(
        cast(int, cast(Mapping[str, object], fold["all_oof_metrics"])["net_sum_micros"])
        < 0
        for fold in folds
    )
    actual_net = tuple(row.net_return_micros / _MICROS_PER_BASIS_POINT for row in predictions)
    probabilities = tuple(row.logistic_positive_probability for row in predictions)
    expected = tuple(row.ridge_expected_net_bps for row in predictions)
    targets = tuple(float(row.net_return_micros > 0) for row in predictions)
    return {
        "protocol": HISTORICAL_THREE_FAMILY_WALK_FORWARD_PROTOCOL_V1,
        "schema_version": HISTORICAL_THREE_FAMILY_WALK_FORWARD_SCHEMA_VERSION_V1,
        "status": HISTORICAL_THREE_FAMILY_WALK_FORWARD_STATUS_V1,
        "historical_only": True,
        "exposed": True,
        "promoting": False,
        "qualification": False,
        "paper_executable": False,
        "order_placement": False,
        "threshold_tuning": False,
        "contract": frozen_walk_forward_contract_document_v1(),
        "contract_sha256": frozen_walk_forward_contract_sha256_v1(),
        "input_sha256": {
            "consensus.csv": loaded.consensus_sha256,
            "fixed_horizon_outcomes.csv": loaded.outcomes_sha256,
        },
        "join_audit": dict(loaded.join_audit),
        "availability_audit": dict(loaded.availability_audit),
        "walk_forward_audit": {
            "folds": len(evaluations),
            "oof_rows": len(predictions),
            "first_oof_time_ms": predictions[0].decision_time_ms,
            "last_oof_time_ms": predictions[-1].decision_time_ms,
            "all_fold_actual_net_sums_negative": fold_negative_count == len(evaluations),
            "negative_fold_count": fold_negative_count,
            "unseen_test_asset_rows": sum(row.unseen_test_asset for row in predictions),
        },
        "all_oof_metrics": _net_metrics(predictions),
        "side_metrics": {
            side: _net_metrics(tuple(row for row in predictions if row.side == side))
            for side in ("long", "short")
        },
        "fixed_gate_metrics": _net_metrics(selected),
        "fixed_gate": {
            "selected_rows": len(selected),
            "rule": "ridge_expected_net_bps > 0 AND logistic_probability > 0.5",
            "maximum_logistic_probability": _float_text(max(probabilities)),
            "maximum_ridge_expected_net_bps": _float_text(max(expected)),
        },
        "ridge_positive_descriptive_only": {
            "not_an_alternate_gate": True,
            "metrics": _net_metrics(ridge_positive),
        },
        "prediction_diagnostics_descriptive_only": {
            "ridge_actual_pearson": _optional_float_text(_pearson(expected, actual_net)),
            "logistic_target_pearson": _optional_float_text(
                _pearson(probabilities, targets)
            ),
            "ridge_equal_count_deciles": _equal_count_deciles(
                predictions,
                key=lambda row: row.ridge_expected_net_bps,
            ),
            "logistic_equal_count_deciles": _equal_count_deciles(
                predictions,
                key=lambda row: row.logistic_positive_probability,
            ),
            "threshold_search_performed": False,
        },
        "fold_metrics": folds,
        "prior_probe_disposition": {
            "status": "PRIOR_PROBE_UNREPRODUCED_NOT_AUTHORITATIVE",
            "reason": (
                "the earlier ad-hoc probe preserved metrics but not its exact feature "
                "transform, intercept penalty, or solver contract; this artifact is the "
                "authoritative reproducible diagnostic"
            ),
            "reported_oof_rows": 2869,
            "reported_mean_net_bps": "-25.531",
            "reported_profit_factor": "0.529",
            "reported_hit_rate": "0.3667",
            "reported_fixed_gate_rows": 0,
        },
    }


def _fold_result_document(
    evaluation: WalkForwardFoldEvaluationV1,
) -> dict[str, object]:
    fold_slice = evaluation.slice
    return {
        "fold": fold_slice.fold,
        "training_start_ms": fold_slice.training_start_ms,
        "training_cutoff_ms_exclusive": fold_slice.training_cutoff_ms_exclusive,
        "embargo_start_ms": fold_slice.embargo_start_ms,
        "test_start_ms": fold_slice.test_start_ms,
        "test_end_ms_exclusive": fold_slice.test_end_ms_exclusive,
        "last_partial": fold_slice.last_partial,
        "training_rows": len(fold_slice.train_indices),
        "embargo_rows": len(fold_slice.embargo_indices),
        "test_rows": len(fold_slice.test_indices),
        "unseen_test_asset_rows": sum(
            prediction.unseen_test_asset for prediction in evaluation.predictions
        ),
        "fixed_gate_rows": sum(
            prediction.fixed_gate_selected for prediction in evaluation.predictions
        ),
        "logistic_iterations": evaluation.logistic_iterations,
        "all_oof_metrics": _net_metrics(evaluation.predictions),
    }


def _fold_models_document(
    evaluations: Sequence[WalkForwardFoldEvaluationV1],
) -> dict[str, object]:
    return {
        "protocol": HISTORICAL_THREE_FAMILY_WALK_FORWARD_PROTOCOL_V1,
        "contract_sha256": frozen_walk_forward_contract_sha256_v1(),
        "historical_only": True,
        "exposed": True,
        "promoting": False,
        "models": [
            {
                "fold": evaluation.slice.fold,
                "feature_names": list(evaluation.transform.feature_names),
                "continuous_means": [
                    _float_text(value) for value in evaluation.transform.means
                ],
                "continuous_scales": [
                    _float_text(value) for value in evaluation.transform.scales
                ],
                "training_asset_universe": list(evaluation.transform.assets),
                "ridge_coefficients_intercept_first": [
                    _float_text(value) for value in evaluation.ridge_coefficients
                ],
                "logistic_coefficients_intercept_first": [
                    _float_text(value) for value in evaluation.logistic_coefficients
                ],
                "logistic_iterations": evaluation.logistic_iterations,
            }
            for evaluation in evaluations
        ],
    }


def _net_metrics(rows: Sequence[WalkForwardPredictionV1]) -> dict[str, object]:
    if not rows:
        return {
            "rows": 0,
            "positive_rows": 0,
            "negative_rows": 0,
            "zero_rows": 0,
            "net_sum_micros": 0,
            "mean_net_bps": None,
            "profit_factor": None,
            "hit_rate": None,
        }
    values = tuple(row.net_return_micros for row in rows)
    gains = sum(value for value in values if value > 0)
    losses = -sum(value for value in values if value < 0)
    positives = sum(value > 0 for value in values)
    return {
        "rows": len(values),
        "positive_rows": positives,
        "negative_rows": sum(value < 0 for value in values),
        "zero_rows": sum(value == 0 for value in values),
        "net_sum_micros": sum(values),
        "mean_net_bps": _float_text(
            sum(values) / len(values) / _MICROS_PER_BASIS_POINT
        ),
        "profit_factor": None if losses == 0 else _float_text(gains / losses),
        "hit_rate": _float_text(positives / len(values)),
    }


def _equal_count_deciles(
    rows: Sequence[WalkForwardPredictionV1],
    *,
    key: Callable[[WalkForwardPredictionV1], float],
) -> list[dict[str, object]]:
    ordered = sorted(rows, key=lambda row: (key(row), row.event_id))
    results: list[dict[str, object]] = []
    for index in range(10):
        start = index * len(ordered) // 10
        end = (index + 1) * len(ordered) // 10
        group = ordered[start:end]
        values = [key(row) for row in group]
        results.append(
            {
                "decile": index + 1,
                "minimum_score": _float_text(min(values)),
                "maximum_score": _float_text(max(values)),
                "mean_score": _float_text(math.fsum(values) / len(values)),
                "metrics": _net_metrics(group),
            }
        )
    return results


def _folds_csv_bytes(evaluations: Sequence[WalkForwardFoldEvaluationV1]) -> bytes:
    output = io.StringIO(newline="")
    fields: Sequence[str] = (
        "fold",
        "training_start_ms",
        "training_cutoff_ms_exclusive",
        "embargo_start_ms",
        "test_start_ms",
        "test_end_ms_exclusive",
        "last_partial",
        "training_rows",
        "embargo_rows",
        "test_rows",
        "unseen_test_asset_rows",
        "fixed_gate_rows",
        "logistic_iterations",
        "rows",
        "positive_rows",
        "negative_rows",
        "zero_rows",
        "net_sum_micros",
        "mean_net_bps",
        "profit_factor",
        "hit_rate",
    )
    writer: csv.DictWriter[str] = csv.DictWriter(
        output, fieldnames=fields, lineterminator="\n"
    )
    writer.writeheader()
    for evaluation in evaluations:
        row = _fold_result_document(evaluation)
        metrics = cast(Mapping[str, object], row.pop("all_oof_metrics"))
        writer.writerow({**row, **metrics})
    return output.getvalue().encode("utf-8")


def _predictions_csv_bytes(predictions: Sequence[WalkForwardPredictionV1]) -> bytes:
    output = io.StringIO(newline="")
    fields = (
        "fold",
        "event_id",
        "split",
        "asset",
        "side",
        "decision_time_ms",
        "net_return_micros",
        "net_return_bps",
        "ridge_expected_net_bps",
        "logistic_positive_probability",
        "fixed_gate_selected",
        "unseen_test_asset",
        "historical_only",
        "exposed",
        "promoting",
    )
    writer: csv.DictWriter[str] = csv.DictWriter(
        output, fieldnames=fields, lineterminator="\n"
    )
    writer.writeheader()
    for row in predictions:
        writer.writerow(
            {
                "fold": row.fold,
                "event_id": row.event_id,
                "split": row.split,
                "asset": row.asset,
                "side": row.side,
                "decision_time_ms": row.decision_time_ms,
                "net_return_micros": row.net_return_micros,
                "net_return_bps": _float_text(
                    row.net_return_micros / _MICROS_PER_BASIS_POINT
                ),
                "ridge_expected_net_bps": _float_text(row.ridge_expected_net_bps),
                "logistic_positive_probability": _float_text(
                    row.logistic_positive_probability
                ),
                "fixed_gate_selected": str(row.fixed_gate_selected).lower(),
                "unseen_test_asset": str(row.unseen_test_asset).lower(),
                "historical_only": "true",
                "exposed": "true",
                "promoting": "false",
            }
        )
    return output.getvalue().encode("utf-8")


def _render_report_ko(results: Mapping[str, object]) -> str:
    overall = cast(Mapping[str, object], results["all_oof_metrics"])
    gate = cast(Mapping[str, object], results["fixed_gate_metrics"])
    audit = cast(Mapping[str, object], results["walk_forward_audit"])
    availability = cast(Mapping[str, object], results["availability_audit"])
    return "\n".join(
        (
            "# 동결 5분봉 워크포워드 진단 V1",
            "",
            "이 산출물은 **EXPOSED / HISTORICAL_ONLY / NON-PROMOTING**이다. "
            "PAPER/BBO 실행 가능성이나 미래 수익성을 입증하지 않으며 실주문을 허용하지 않는다.",
            "",
            "## 동결 계약",
            "",
            "- 최초 180일 학습 후 72개 5분봉(6시간) embargo",
            "- 이후 연속 30일 평가창, expanding refit, 마지막 부분 창 포함",
            "- Ridge λ=10 및 Logistic λ=10, intercept 비패널티",
            "- 연속형은 학습 행만으로 표준화; 자산 one-hot은 학습 자산만 사용",
            "- 유일한 고정 gate: `ridge > 0bp AND logistic > 0.5`",
            "- threshold tuning, symbol/period selection, test-set rescue 금지",
            "",
            "## 비용 후 OOF 결과",
            "",
            f"- folds: {audit['folds']}",
            f"- OOF rows: {overall['rows']}",
            f"- 평균 비용 후 수익: {overall['mean_net_bps']} bp",
            f"- Profit Factor: {overall['profit_factor']}",
            f"- 적중률: {overall['hit_rate']}",
            f"- 고정 gate 선택: {gate['rows']}건",
            f"- 모든 fold의 실제 순손익 합 음수: {audit['all_fold_actual_net_sums_negative']}",
            "",
            "## 데이터 가용성",
            "",
            "- 참여도 source-only hole 판정 월: "
            + ", ".join(cast(Sequence[str], availability["participation_hole_months"])),
            "- hole 판정은 결과값을 읽지 않고 월별 `INCONCLUSIVE_DATA > READY`로 고정했다.",
            "",
            "## 이전 임시 probe 처리",
            "",
            "이전 19-fold 수치는 OOF 모집단 요약은 재확인되었지만 정확한 scaler/solver 계약이 "
            "보존되지 않았다. 따라서 `PRIOR_PROBE_UNREPRODUCED_NOT_AUTHORITATIVE`로 남기며, "
            "이 산출물을 최초의 재현 가능한 authoritative 진단으로 사용한다.",
            "",
        )
    )


def _validate_model_inputs(
    matrix: Sequence[Sequence[float]],
    targets: Sequence[float],
) -> tuple[tuple[float, ...], ...]:
    if not matrix or len(matrix) != len(targets):
        raise HistoricalThreeFamilyWalkForwardErrorV1(
            "model matrix and targets must be equally non-empty"
        )
    width = len(matrix[0])
    if width == 0 or any(len(row) != width for row in matrix):
        raise HistoricalThreeFamilyWalkForwardErrorV1(
            "model matrix must have a fixed positive width"
        )
    if any(not math.isfinite(value) for row in matrix for value in row):
        raise HistoricalThreeFamilyWalkForwardErrorV1(
            "model matrix contains non-finite values"
        )
    if any(not math.isfinite(target) for target in targets):
        raise HistoricalThreeFamilyWalkForwardErrorV1(
            "model targets contain non-finite values"
        )
    return tuple((1.0, *row) for row in matrix)


def _solve_positive_definite(
    matrix: Sequence[Sequence[float]],
    rhs: Sequence[float],
) -> list[float]:
    dimension = len(matrix)
    if dimension == 0 or len(rhs) != dimension or any(
        len(row) != dimension for row in matrix
    ):
        raise HistoricalThreeFamilyWalkForwardErrorV1(
            "linear system dimensions are inconsistent"
        )
    lower = [[0.0] * dimension for _ in range(dimension)]
    for row in range(dimension):
        for column in range(row + 1):
            value = matrix[row][column] - math.fsum(
                lower[row][index] * lower[column][index] for index in range(column)
            )
            if row == column:
                if value <= 0 or not math.isfinite(value):
                    raise HistoricalThreeFamilyWalkForwardErrorV1(
                        "penalized normal matrix is not positive definite"
                    )
                lower[row][column] = math.sqrt(value)
            else:
                lower[row][column] = value / lower[column][column]
    intermediate = [0.0] * dimension
    for row in range(dimension):
        intermediate[row] = (
            rhs[row]
            - math.fsum(lower[row][column] * intermediate[column] for column in range(row))
        ) / lower[row][row]
    solution = [0.0] * dimension
    for row in range(dimension - 1, -1, -1):
        solution[row] = (
            intermediate[row]
            - math.fsum(
                lower[column][row] * solution[column]
                for column in range(row + 1, dimension)
            )
        ) / lower[row][row]
    if any(not math.isfinite(value) for value in solution):
        raise HistoricalThreeFamilyWalkForwardErrorV1(
            "linear solver produced non-finite coefficients"
        )
    return solution


def _logistic_objective(
    design: Sequence[Sequence[float]],
    targets: Sequence[float],
    coefficients: Sequence[float],
) -> float:
    loss = math.fsum(
        _softplus(_dot(coefficients, features))
        - target * _dot(coefficients, features)
        for features, target in zip(design, targets, strict=True)
    )
    penalty = 0.5 * LOGISTIC_LAMBDA_V1 * math.fsum(
        coefficient * coefficient for coefficient in coefficients[1:]
    )
    return loss + penalty


def _softplus(value: float) -> float:
    if value > 0:
        return value + math.log1p(math.exp(-value))
    return math.log1p(math.exp(value))


def _sigmoid(value: float) -> float:
    if value >= 0:
        exponent = math.exp(-min(value, 745.0))
        return 1.0 / (1.0 + exponent)
    exponent = math.exp(max(value, -745.0))
    return exponent / (1.0 + exponent)


def _dot(left: Sequence[float], right: Sequence[float]) -> float:
    return math.fsum(a * b for a, b in zip(left, right, strict=True))


def _pearson(left: Sequence[float], right: Sequence[float]) -> float | None:
    if not left or len(left) != len(right):
        raise HistoricalThreeFamilyWalkForwardErrorV1(
            "correlation inputs must be equally non-empty"
        )
    left_mean = math.fsum(left) / len(left)
    right_mean = math.fsum(right) / len(right)
    numerator = math.fsum(
        (a - left_mean) * (b - right_mean) for a, b in zip(left, right, strict=True)
    )
    left_ss = math.fsum((value - left_mean) ** 2 for value in left)
    right_ss = math.fsum((value - right_mean) ** 2 for value in right)
    if left_ss == 0 or right_ss == 0:
        return None
    return numerator / math.sqrt(left_ss * right_ss)


def _require_chronological_unique_observations(
    rows: Sequence[WalkForwardObservationV1],
) -> None:
    if not rows:
        raise HistoricalThreeFamilyWalkForwardErrorV1(
            "walk-forward requires at least one observation"
        )
    keys = tuple((row.decision_time_ms, row.event_id) for row in rows)
    if keys != tuple(sorted(keys)):
        raise HistoricalThreeFamilyWalkForwardErrorV1(
            "observations must be ordered by decision time and event ID"
        )
    if len({row.event_id for row in rows}) != len(rows):
        raise HistoricalThreeFamilyWalkForwardErrorV1(
            "walk-forward observations contain duplicate event IDs"
        )


def _validate_slice_times(
    rows: Sequence[WalkForwardObservationV1],
    fold_slice: WalkForwardSliceV1,
) -> None:
    if any(
        rows[index].decision_time_ms >= fold_slice.training_cutoff_ms_exclusive
        for index in fold_slice.train_indices
    ):
        raise HistoricalThreeFamilyWalkForwardErrorV1(
            "training row reaches the embargo boundary"
        )
    if any(
        not (
            fold_slice.embargo_start_ms
            <= rows[index].decision_time_ms
            < fold_slice.test_start_ms
        )
        for index in fold_slice.embargo_indices
    ):
        raise HistoricalThreeFamilyWalkForwardErrorV1(
            "embargo row lies outside the frozen gap"
        )
    if any(
        not (
            fold_slice.test_start_ms
            <= rows[index].decision_time_ms
            < fold_slice.test_end_ms_exclusive
        )
        for index in fold_slice.test_indices
    ):
        raise HistoricalThreeFamilyWalkForwardErrorV1(
            "test row lies outside its fold interval"
        )


def _csv_reader(
    raw: bytes,
    required_columns: frozenset[str],
    label: str,
) -> csv.DictReader[str]:
    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise HistoricalThreeFamilyWalkForwardErrorV1(
            f"{label} is not UTF-8"
        ) from exc
    reader = csv.DictReader(io.StringIO(text, newline=""))
    fields = frozenset(reader.fieldnames or ())
    missing = required_columns - fields
    if missing:
        raise HistoricalThreeFamilyWalkForwardErrorV1(
            f"{label} is missing required columns: {sorted(missing)}"
        )
    return reader


def _required_text(record: Mapping[str, str | None], field: str, row: int) -> str:
    value = record.get(field)
    if value is None or value == "":
        raise HistoricalThreeFamilyWalkForwardErrorV1(
            f"row {row} field {field} must be non-empty"
        )
    return value


def _required_int(record: Mapping[str, str | None], field: str, row: int) -> int:
    value = _required_text(record, field, row)
    try:
        parsed = int(value)
    except ValueError as exc:
        raise HistoricalThreeFamilyWalkForwardErrorV1(
            f"row {row} field {field} must be an integer"
        ) from exc
    return parsed


def _required_nonnegative_int(
    record: Mapping[str, str | None], field: str, row: int
) -> int:
    value = _required_int(record, field, row)
    if value < 0:
        raise HistoricalThreeFamilyWalkForwardErrorV1(
            f"row {row} field {field} must be nonnegative"
        )
    return value


def _required_direction(
    record: Mapping[str, str | None], field: str, row: int
) -> int:
    value = _required_int(record, field, row)
    if value not in {-1, 0, 1}:
        raise HistoricalThreeFamilyWalkForwardErrorV1(
            f"row {row} field {field} must be -1, 0, or 1"
        )
    return value


def _required_bool(record: Mapping[str, str | None], field: str, row: int) -> bool:
    value = _required_text(record, field, row)
    if value == "true":
        return True
    if value == "false":
        return False
    raise HistoricalThreeFamilyWalkForwardErrorV1(
        f"row {row} field {field} must be lowercase true or false"
    )


def _required_side(
    record: Mapping[str, str | None], field: str, row: int
) -> Literal["long", "short"]:
    value = _required_text(record, field, row)
    if value not in {"long", "short"}:
        raise HistoricalThreeFamilyWalkForwardErrorV1(
            f"row {row} field {field} must be long or short"
        )
    return cast(Literal["long", "short"], value)


def _read_frozen_input(path: str | Path, expected_sha256: str, label: str) -> bytes:
    source = Path(path).resolve()
    try:
        raw = source.read_bytes()
    except OSError as exc:
        raise HistoricalThreeFamilyWalkForwardErrorV1(
            f"cannot read frozen {label}"
        ) from exc
    if _sha256_bytes(raw) != expected_sha256:
        raise HistoricalThreeFamilyWalkForwardErrorV1(
            f"{label} differs from the frozen SHA-256"
        )
    return raw


def _verify_frozen_input_unchanged(
    path: str | Path, expected_sha256: str, label: str
) -> None:
    source = Path(path).resolve()
    try:
        actual = _sha256_bytes(source.read_bytes())
    except OSError as exc:
        raise HistoricalThreeFamilyWalkForwardErrorV1(
            f"cannot reverify frozen {label} before publication"
        ) from exc
    if actual != expected_sha256:
        raise HistoricalThreeFamilyWalkForwardErrorV1(
            f"{label} changed during walk-forward analysis"
        )


def _publish_artifacts(target: Path, payloads: Mapping[str, bytes]) -> None:
    if set(payloads) != _PUBLISHED_NAMES:
        raise HistoricalThreeFamilyWalkForwardErrorV1(
            "walk-forward publication file set is not exact"
        )
    target.parent.mkdir(parents=True, exist_ok=True)
    if target.exists():
        raise HistoricalThreeFamilyWalkForwardErrorV1(
            "walk-forward output directory already exists"
        )
    temporary = Path(tempfile.mkdtemp(prefix=f".{target.name}.", dir=target.parent))
    try:
        for name, payload in payloads.items():
            path = temporary / name
            with path.open("wb") as handle:
                written = handle.write(payload)
                if written != len(payload):
                    raise HistoricalThreeFamilyWalkForwardErrorV1(
                        f"short artifact write for {name}"
                    )
                handle.flush()
                os.fsync(handle.fileno())
        os.replace(temporary, target)
        _fsync_parent_directory(target)
    except OSError as exc:
        raise HistoricalThreeFamilyWalkForwardErrorV1(
            "cannot atomically publish walk-forward artifacts"
        ) from exc
    finally:
        if temporary.exists():
            shutil.rmtree(temporary)


def _fsync_parent_directory(path: Path) -> None:
    # Windows has no portable directory fsync. Files are still fsynced before
    # the atomic rename; POSIX additionally makes the parent rename durable.
    if os.name == "nt":
        return
    descriptor = os.open(path.parent, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _require_sha256(value: str, label: str) -> None:
    if len(value) != _SHA256_HEX_LENGTH or any(
        character not in "0123456789abcdef" for character in value
    ):
        raise HistoricalThreeFamilyWalkForwardErrorV1(
            f"{label} must be a lowercase SHA-256 digest"
        )


def _float_text(value: float) -> str:
    if not math.isfinite(value):
        raise HistoricalThreeFamilyWalkForwardErrorV1(
            "artifact cannot contain non-finite numeric values"
        )
    return format(value, ".17g")


def _optional_float_text(value: float | None) -> str | None:
    return None if value is None else _float_text(value)


def _json_bytes(value: object) -> bytes:
    return (
        json.dumps(
            value,
            ensure_ascii=False,
            allow_nan=False,
            sort_keys=True,
            separators=(",", ":"),
        )
        + "\n"
    ).encode("utf-8")


def _sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Run the frozen EXPOSED/HISTORICAL_ONLY 5m three-family walk-forward diagnostic"
        )
    )
    parser.add_argument("--consensus", required=True)
    parser.add_argument("--outcomes", required=True)
    parser.add_argument("--output-dir", required=True)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    run_frozen_walk_forward_diagnostic_v1(
        consensus_path=args.consensus,
        outcomes_path=args.outcomes,
        output_dir=args.output_dir,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
