from __future__ import annotations

import math
from collections import Counter
from collections.abc import Iterable, Sequence
from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class BinaryFeatureRow:
    """One causal feature row for deterministic binary calibration."""

    numeric: tuple[float, ...]
    categorical: tuple[str, ...]
    target: bool


@dataclass(frozen=True, slots=True)
class SigmoidCalibrator:
    """Frozen affine sigmoid calibration applied to a model log-odds value."""

    temperature: float
    intercept: float

    def probability(self, log_odds: float) -> float:
        if not math.isfinite(log_odds):
            raise ValueError("log_odds must be finite")
        return _sigmoid(log_odds / self.temperature + self.intercept)


class GaussianCategoricalNaiveBayes:
    """Small deterministic mixed-feature classifier with no runtime dependency."""

    def __init__(self, *, alpha: float = 1.0, variance_floor: float = 1e-4) -> None:
        if not math.isfinite(alpha) or alpha <= 0:
            raise ValueError("alpha must be finite and positive")
        if not math.isfinite(variance_floor) or variance_floor <= 0:
            raise ValueError("variance_floor must be finite and positive")
        self.alpha = alpha
        self.variance_floor = variance_floor
        self._fitted = False
        self._class_counts: tuple[int, int] = (0, 0)
        self._means: tuple[tuple[float, ...], tuple[float, ...]] = ((), ())
        self._variances: tuple[tuple[float, ...], tuple[float, ...]] = ((), ())
        self._category_counts: tuple[
            tuple[Counter[str], ...], tuple[Counter[str], ...]
        ] = ((), ())
        self._vocabularies: tuple[frozenset[str], ...] = ()

    def fit(self, rows: Sequence[BinaryFeatureRow]) -> GaussianCategoricalNaiveBayes:
        if not rows:
            raise ValueError("at least one training row is required")
        numeric_size = len(rows[0].numeric)
        categorical_size = len(rows[0].categorical)
        counts = [0, 0]
        sums = [[0.0] * numeric_size for _ in range(2)]
        sums_sq = [[0.0] * numeric_size for _ in range(2)]
        category_counts = [
            [Counter[str]() for _ in range(categorical_size)] for _ in range(2)
        ]
        vocabularies = [set[str]() for _ in range(categorical_size)]

        for row in rows:
            self._validate_dimensions(row, numeric_size, categorical_size)
            class_index = int(row.target)
            counts[class_index] += 1
            for index, value in enumerate(row.numeric):
                if not math.isfinite(value):
                    raise ValueError("numeric training features must be finite")
                sums[class_index][index] += value
                sums_sq[class_index][index] += value * value
            for index, value in enumerate(row.categorical):
                if not value:
                    raise ValueError("categorical training features must be non-empty")
                category_counts[class_index][index][value] += 1
                vocabularies[index].add(value)

        if min(counts) == 0:
            raise ValueError("training rows must contain both target classes")

        means: list[tuple[float, ...]] = []
        variances: list[tuple[float, ...]] = []
        for class_index, count in enumerate(counts):
            class_means = tuple(value / count for value in sums[class_index])
            class_variances = tuple(
                max(
                    (
                        sums_sq[class_index][index]
                        - sums[class_index][index] ** 2 / count
                    )
                    / max(count - 1, 1),
                    self.variance_floor,
                )
                for index in range(numeric_size)
            )
            means.append(class_means)
            variances.append(class_variances)

        self._class_counts = (counts[0], counts[1])
        self._means = (means[0], means[1])
        self._variances = (variances[0], variances[1])
        self._category_counts = (
            tuple(category_counts[0]),
            tuple(category_counts[1]),
        )
        self._vocabularies = tuple(frozenset(values) for values in vocabularies)
        self._fitted = True
        return self

    def log_odds(
        self,
        numeric: Sequence[float],
        categorical: Sequence[str],
    ) -> float:
        self._require_fitted()
        row = BinaryFeatureRow(tuple(numeric), tuple(categorical), False)
        self._validate_dimensions(
            row,
            len(self._means[0]),
            len(self._vocabularies),
        )
        if any(not math.isfinite(value) for value in row.numeric):
            raise ValueError("numeric prediction features must be finite")
        if any(not value for value in row.categorical):
            raise ValueError("categorical prediction features must be non-empty")
        scores = [self._class_log_score(class_index, row) for class_index in (0, 1)]
        value = scores[1] - scores[0]
        if not math.isfinite(value):
            raise ValueError("model produced non-finite log odds")
        return value

    def probability(
        self,
        numeric: Sequence[float],
        categorical: Sequence[str],
    ) -> float:
        return _sigmoid(self.log_odds(numeric, categorical))

    def artifact(self) -> dict[str, object]:
        self._require_fitted()
        return {
            "model": "gaussian_categorical_naive_bayes",
            "alpha": self.alpha,
            "variance_floor": self.variance_floor,
            "class_counts": {"non_edge": self._class_counts[0], "edge": self._class_counts[1]},
            "means": [list(values) for values in self._means],
            "variances": [list(values) for values in self._variances],
            "category_counts": [
                [dict(sorted(counter.items())) for counter in class_counters]
                for class_counters in self._category_counts
            ],
            "vocabularies": [sorted(values) for values in self._vocabularies],
        }

    @staticmethod
    def _validate_dimensions(
        row: BinaryFeatureRow,
        numeric_size: int,
        categorical_size: int,
    ) -> None:
        if len(row.numeric) != numeric_size:
            raise ValueError("numeric feature dimension changed")
        if len(row.categorical) != categorical_size:
            raise ValueError("categorical feature dimension changed")

    def _require_fitted(self) -> None:
        if not self._fitted:
            raise RuntimeError("model must be fitted before use")

    def _class_log_score(self, class_index: int, row: BinaryFeatureRow) -> float:
        total_count = sum(self._class_counts)
        class_count = self._class_counts[class_index]
        prior = (class_count + self.alpha) / (total_count + 2 * self.alpha)
        score = math.log(prior)
        for index, value in enumerate(row.numeric):
            mean = self._means[class_index][index]
            variance = self._variances[class_index][index]
            score += -0.5 * (
                math.log(2 * math.pi * variance) + (value - mean) ** 2 / variance
            )
        for index, value in enumerate(row.categorical):
            vocabulary_size = len(self._vocabularies[index]) + 1
            count = self._category_counts[class_index][index].get(value, 0)
            probability = (count + self.alpha) / (
                class_count + self.alpha * vocabulary_size
            )
            score += math.log(probability)
        return score


def fit_sigmoid_calibrator(
    log_odds: Sequence[float],
    targets: Sequence[bool],
    *,
    temperatures: Sequence[float],
    intercepts: Sequence[float],
) -> SigmoidCalibrator:
    if not log_odds or len(log_odds) != len(targets):
        raise ValueError("calibration values and targets must be equally non-empty")
    if min(sum(targets), len(targets) - sum(targets)) == 0:
        raise ValueError("calibration targets must contain both classes")
    candidates: list[tuple[float, float, float, float, float]] = []
    for temperature in temperatures:
        if not math.isfinite(temperature) or temperature <= 0:
            raise ValueError("calibration temperatures must be finite and positive")
        for intercept in intercepts:
            if not math.isfinite(intercept):
                raise ValueError("calibration intercepts must be finite")
            calibrator = SigmoidCalibrator(temperature, intercept)
            probabilities = [calibrator.probability(value) for value in log_odds]
            loss = binary_log_loss(probabilities, targets)
            candidates.append(
                (
                    loss,
                    abs(temperature - 1.0),
                    abs(intercept),
                    temperature,
                    intercept,
                )
            )
    _, _, _, temperature, intercept = min(candidates)
    return SigmoidCalibrator(temperature, intercept)


def binary_log_loss(probabilities: Sequence[float], targets: Sequence[bool]) -> float:
    _validate_metric_inputs(probabilities, targets)
    epsilon = 1e-15
    losses = []
    for probability, target in zip(probabilities, targets, strict=True):
        clipped = min(1 - epsilon, max(epsilon, probability))
        losses.append(-math.log(clipped if target else 1 - clipped))
    return math.fsum(losses) / len(losses)


def brier_score(probabilities: Sequence[float], targets: Sequence[bool]) -> float:
    _validate_metric_inputs(probabilities, targets)
    return math.fsum(
        (probability - float(target)) ** 2
        for probability, target in zip(probabilities, targets, strict=True)
    ) / len(probabilities)


def equal_count_ece(
    probabilities: Sequence[float],
    targets: Sequence[bool],
    bins: int,
) -> tuple[float, list[dict[str, float | int]]]:
    _validate_metric_inputs(probabilities, targets)
    if bins < 2:
        raise ValueError("reliability bins must be at least two")
    ordered = sorted(
        zip(probabilities, targets, strict=True),
        key=lambda item: (item[0], item[1]),
    )
    bin_count = min(bins, len(ordered))
    rows: list[dict[str, float | int]] = []
    weighted_error = 0.0
    for index in range(bin_count):
        start = index * len(ordered) // bin_count
        end = (index + 1) * len(ordered) // bin_count
        values = ordered[start:end]
        mean_probability = math.fsum(item[0] for item in values) / len(values)
        observed_rate = math.fsum(float(item[1]) for item in values) / len(values)
        weighted_error += len(values) * abs(mean_probability - observed_rate)
        rows.append(
            {
                "bin": index + 1,
                "rows": len(values),
                "minimum_probability": values[0][0],
                "maximum_probability": values[-1][0],
                "mean_probability": mean_probability,
                "observed_rate": observed_rate,
            }
        )
    return weighted_error / len(ordered), rows


def _validate_metric_inputs(
    probabilities: Sequence[float], targets: Sequence[bool]
) -> None:
    if not probabilities or len(probabilities) != len(targets):
        raise ValueError("probabilities and targets must be equally non-empty")
    if any(
        not math.isfinite(probability) or probability < 0 or probability > 1
        for probability in probabilities
    ):
        raise ValueError("probabilities must be finite values in [0, 1]")


def _sigmoid(value: float) -> float:
    if value >= 0:
        exponent = math.exp(-min(value, 745.0))
        return 1.0 / (1.0 + exponent)
    exponent = math.exp(max(value, -745.0))
    return exponent / (1.0 + exponent)


def finite_mean(values: Iterable[float]) -> float:
    materialized = tuple(values)
    if not materialized or any(not math.isfinite(value) for value in materialized):
        raise ValueError("finite_mean requires non-empty finite values")
    return math.fsum(materialized) / len(materialized)
