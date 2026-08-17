from __future__ import annotations

import math

import pytest

from signalbot.backtest.calibration import (
    BinaryFeatureRow,
    GaussianCategoricalNaiveBayes,
    binary_log_loss,
    brier_score,
    equal_count_ece,
    fit_sigmoid_calibrator,
)


def _rows() -> list[BinaryFeatureRow]:
    return [
        BinaryFeatureRow((-1.0, -0.5), ("neutral",), False),
        BinaryFeatureRow((-0.8, -0.4), ("neutral",), False),
        BinaryFeatureRow((0.8, 0.4), ("risk_on",), True),
        BinaryFeatureRow((1.0, 0.5), ("risk_on",), True),
    ]


def test_mixed_naive_bayes_is_deterministic_and_handles_unseen_category() -> None:
    first = GaussianCategoricalNaiveBayes().fit(_rows())
    second = GaussianCategoricalNaiveBayes().fit(_rows())

    positive = first.probability((0.9, 0.45), ("risk_on",))
    negative = first.probability((-0.9, -0.45), ("neutral",))
    unseen = first.probability((0.0, 0.0), ("unseen",))

    assert positive > 0.5
    assert negative < 0.5
    assert math.isfinite(unseen) and 0 <= unseen <= 1
    assert first.artifact() == second.artifact()


def test_mixed_naive_bayes_requires_both_classes() -> None:
    with pytest.raises(ValueError, match="both target classes"):
        GaussianCategoricalNaiveBayes().fit(
            [BinaryFeatureRow((0.0,), ("neutral",), False)]
        )


def test_sigmoid_calibration_and_metrics_obey_probability_contract() -> None:
    logits = [-2.0, -1.0, 1.0, 2.0]
    targets = [False, False, True, True]
    calibrator = fit_sigmoid_calibrator(
        logits,
        targets,
        temperatures=[0.5, 1.0, 2.0],
        intercepts=[-0.5, 0.0, 0.5],
    )
    probabilities = [calibrator.probability(value) for value in logits]
    ece, bins = equal_count_ece(probabilities, targets, 2)

    assert all(0 <= value <= 1 for value in probabilities)
    assert brier_score(probabilities, targets) < 0.25
    assert binary_log_loss(probabilities, targets) < math.log(2)
    assert 0 <= ece <= 1
    assert sum(int(item["rows"]) for item in bins) == len(targets)

