from __future__ import annotations

import argparse
import bisect
import csv
import hashlib
import json
import math
import random
import statistics
from collections import defaultdict
from collections.abc import Callable, Sequence
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

_DAY_MS = 86_400_000
_PRIMARY_HORIZON_BARS = 12
_SCHEMA_VERSION = 1

FEATURE_COLUMNS = (
    "efficiency_ratio_20",
    "directional_di_balance",
    "adx_delta",
    "directional_macd_delta_atr",
    "directional_taker_delta",
    "volume_zscore",
    "pullback_range_contraction",
    "pullback_volume_contraction",
)

AXIS_DEFINITIONS = (
    (
        "trend_quality",
        ("efficiency_ratio_20", "directional_di_balance", "adx_delta"),
    ),
    ("momentum_acceleration", ("directional_macd_delta_atr",)),
    ("directional_participation", ("directional_taker_delta", "volume_zscore")),
    (
        "pullback_contraction",
        ("pullback_range_contraction", "pullback_volume_contraction"),
    ),
)

_FEATURE_INDEX = {name: index for index, name in enumerate(FEATURE_COLUMNS)}
_FIXED_SCORE_BUCKETS = (
    ("score_0_to_lt25", 0.0, 25.0, False),
    ("score_25_to_lt50", 25.0, 50.0, False),
    ("score_50_to_lt75", 50.0, 75.0, False),
    ("score_75_to_100", 75.0, 100.0, True),
)


@dataclass(frozen=True, slots=True)
class IndicatorObservation:
    """One recommendation joined to its primary direction-only outcome."""

    event_id: str
    asset: str
    cohort: str
    market: str
    direction: str
    decision_time_ms: int
    split: str
    features: tuple[float | None, ...]
    recommendation_price: float
    outcome_available: bool
    evaluable: bool
    future_close: float | None

    def feature(self, name: str) -> float | None:
        try:
            return self.features[_FEATURE_INDEX[name]]
        except KeyError as error:
            raise ValueError(f"unknown indicator feature: {name}") from error


@dataclass(frozen=True, slots=True)
class InputAudit:
    recommendation_rows: int
    primary_horizon_outcome_rows: int
    matched_outcome_rows: int
    missing_outcome_rows: int
    orphan_outcome_rows: int


@dataclass(frozen=True, slots=True)
class LoadedIndicatorData:
    observations: tuple[IndicatorObservation, ...]
    audit: InputAudit


@dataclass(frozen=True, slots=True)
class DeduplicationAudit:
    input_rows: int
    output_rows: int
    duplicate_rows_dropped: int
    groups_with_duplicates: int
    groups_where_spot_was_preferred: int


@dataclass(frozen=True, slots=True)
class DeduplicatedIndicatorData:
    observations: tuple[IndicatorObservation, ...]
    audit: DeduplicationAudit


@dataclass(frozen=True, slots=True)
class PercentileCompositeModel:
    """Development-only empirical CDFs and the one frozen selection cutoff."""

    distributions: tuple[tuple[float, ...], ...]
    development_rows: int
    scored_development_rows: int
    top_quartile_cutoff: float

    def artifact(self) -> dict[str, Any]:
        feature_distributions = {
            name: {
                "count": len(values),
                "minimum": values[0],
                "maximum": values[-1],
                "sorted_development_values": list(values),
            }
            for name, values in zip(FEATURE_COLUMNS, self.distributions, strict=True)
        }
        return {
            "fit_uses_outcomes": False,
            "feature_percentile_method": (
                "development empirical CDF midrank; outside-range values map to 0 or 1"
            ),
            "feature_orientation": "higher_is_better_for_recommendation_direction",
            "axes": [
                {"name": name, "features": list(features), "weight": 0.25}
                for name, features in AXIS_DEFINITIONS
            ],
            "missing_value_policy": (
                "average available features within an axis; do not score when any axis "
                "has no available feature"
            ),
            "score_formula": "100 * arithmetic mean of the four axis percentiles",
            "score_is_probability": False,
            "development_rows": self.development_rows,
            "scored_development_rows": self.scored_development_rows,
            "development_score_75th_percentile": self.top_quartile_cutoff,
            "feature_distributions": feature_distributions,
        }


@dataclass(frozen=True, slots=True)
class ScoredIndicatorObservation:
    observation: IndicatorObservation
    axis_scores: tuple[float | None, ...]
    composite_score: float | None


def _finite_optional(value: float | None, *, label: str) -> float | None:
    if value is None:
        return None
    if not math.isfinite(value):
        raise ValueError(f"{label} must be finite when present")
    return value


def _quantile(values: Sequence[float], probability: float) -> float:
    if not values:
        raise ValueError("quantile requires at least one value")
    if not 0 <= probability <= 1:
        raise ValueError("quantile probability must be in [0, 1]")
    ordered = sorted(values)
    position = (len(ordered) - 1) * probability
    lower = math.floor(position)
    upper = math.ceil(position)
    if lower == upper:
        return ordered[lower]
    weight = position - lower
    return ordered[lower] * (1 - weight) + ordered[upper] * weight


def _empirical_percentile(value: float, distribution: Sequence[float]) -> float:
    if not distribution:
        raise ValueError("empirical percentile requires a fitted distribution")
    left = bisect.bisect_left(distribution, value)
    right = bisect.bisect_right(distribution, value)
    return (left + right) / (2 * len(distribution))


def _score_features(
    features: Sequence[float | None],
    distributions: Sequence[Sequence[float]],
) -> tuple[tuple[float | None, ...], float | None]:
    if len(features) != len(FEATURE_COLUMNS):
        raise ValueError("indicator feature dimension changed")
    if len(distributions) != len(FEATURE_COLUMNS):
        raise ValueError("fitted distribution dimension changed")
    percentiles = tuple(
        None if value is None else _empirical_percentile(value, distributions[index])
        for index, value in enumerate(features)
    )
    axes: list[float | None] = []
    for _, feature_names in AXIS_DEFINITIONS:
        available: list[float] = []
        for name in feature_names:
            percentile = percentiles[_FEATURE_INDEX[name]]
            if percentile is not None:
                available.append(percentile)
        axes.append(statistics.fmean(available) if available else None)
    composite = (
        None
        if any(value is None for value in axes)
        else 100.0 * statistics.fmean(value for value in axes if value is not None)
    )
    return tuple(axes), composite


def fit_percentile_composite(
    development_rows: Sequence[IndicatorObservation],
) -> PercentileCompositeModel:
    """Fit label-free feature CDFs and one top-quartile cutoff on development only."""

    if not development_rows:
        raise ValueError("at least one development recommendation is required")
    distributions: list[tuple[float, ...]] = []
    for index, name in enumerate(FEATURE_COLUMNS):
        values = tuple(
            sorted(
                value
                for row in development_rows
                if (value := _finite_optional(row.features[index], label=name)) is not None
            )
        )
        if not values:
            raise ValueError(f"development feature has no finite observations: {name}")
        distributions.append(values)
    frozen_distributions = tuple(distributions)
    development_scores = [
        score
        for row in development_rows
        if (
            score := _score_features(row.features, frozen_distributions)[1]
        )
        is not None
    ]
    if not development_scores:
        raise ValueError("no development row has all four indicator axes available")
    return PercentileCompositeModel(
        distributions=frozen_distributions,
        development_rows=len(development_rows),
        scored_development_rows=len(development_scores),
        top_quartile_cutoff=_quantile(development_scores, 0.75),
    )


def score_observations(
    rows: Sequence[IndicatorObservation],
    model: PercentileCompositeModel,
) -> tuple[ScoredIndicatorObservation, ...]:
    output: list[ScoredIndicatorObservation] = []
    for row in rows:
        axis_scores, composite = _score_features(row.features, model.distributions)
        output.append(ScoredIndicatorObservation(row, axis_scores, composite))
    return tuple(output)


def deduplicate_spot_priority(
    rows: Sequence[IndicatorObservation],
) -> DeduplicatedIndicatorData:
    """Keep one asset/direction/time recommendation, preferring Spot deterministically."""

    grouped: dict[tuple[str, str, int], list[IndicatorObservation]] = defaultdict(list)
    for row in rows:
        grouped[(row.asset, row.direction, row.decision_time_ms)].append(row)
    selected: list[IndicatorObservation] = []
    duplicate_groups = 0
    spot_preferred = 0
    for values in grouped.values():
        ordered = sorted(values, key=lambda row: (row.market != "spot", row.event_id))
        selected.append(ordered[0])
        if len(ordered) > 1:
            duplicate_groups += 1
            if ordered[0].market == "spot" and any(row.market != "spot" for row in ordered[1:]):
                spot_preferred += 1
    selected.sort(
        key=lambda row: (
            row.decision_time_ms,
            row.asset,
            row.direction,
            row.event_id,
        )
    )
    return DeduplicatedIndicatorData(
        observations=tuple(selected),
        audit=DeduplicationAudit(
            input_rows=len(rows),
            output_rows=len(selected),
            duplicate_rows_dropped=len(rows) - len(selected),
            groups_with_duplicates=duplicate_groups,
            groups_where_spot_was_preferred=spot_preferred,
        ),
    )


def _direction_status(row: IndicatorObservation) -> str | None:
    if not row.evaluable:
        return None
    directional_return = _directional_return(row)
    if directional_return is None:
        raise ValueError("evaluable outcome requires a future close")
    if directional_return > 0:
        return "correct"
    if directional_return < 0:
        return "wrong"
    return "tie"


def _directional_return(row: IndicatorObservation) -> float | None:
    if not row.evaluable:
        return None
    if row.recommendation_price <= 0:
        raise ValueError("recommendation price must be positive")
    if row.future_close is None or row.future_close <= 0:
        raise ValueError("evaluable outcome requires a positive future close")
    market_return = row.future_close / row.recommendation_price - 1
    return market_return if row.direction == "long" else -market_return


def _daily_direction_counts(
    rows: Sequence[ScoredIndicatorObservation],
    *,
    start_day: int,
    day_count: int,
) -> tuple[list[int], list[int], list[int]]:
    evaluable = [0] * day_count
    correct = [0] * day_count
    resolved = [0] * day_count
    for scored in rows:
        status = _direction_status(scored.observation)
        if status is None:
            continue
        offset = scored.observation.decision_time_ms // _DAY_MS - start_day
        if not 0 <= offset < day_count:
            raise ValueError("recommendation lies outside the bootstrap calendar")
        evaluable[offset] += 1
        correct[offset] += int(status == "correct")
        resolved[offset] += int(status in {"correct", "wrong"})
    return evaluable, correct, resolved


def _circular_block_sums(values: Sequence[int], length: int) -> tuple[int, ...]:
    size = len(values)
    return tuple(
        sum(values[(start + offset) % size] for offset in range(length))
        for start in range(size)
    )


def _bootstrap_totals(
    sums: tuple[dict[int, tuple[int, ...]], ...],
    starts: Sequence[int],
    block_lengths: Sequence[int],
) -> tuple[int, int, int]:
    totals = tuple(
        sum(
            sums[index][length][start]
            for start, length in zip(starts, block_lengths, strict=True)
        )
        for index in range(3)
    )
    return totals[0], totals[1], totals[2]


def block_bootstrap_direction_accuracy(
    selected_rows: Sequence[ScoredIndicatorObservation],
    baseline_rows: Sequence[ScoredIndicatorObservation],
    *,
    calendar_start_day: int,
    calendar_end_day: int,
    samples: int,
    block_days: int,
    seed: int,
) -> dict[str, Any]:
    """Bootstrap accuracy and paired uplift with shared circular UTC-day blocks."""

    if samples < 100:
        raise ValueError("bootstrap samples must be at least 100")
    if block_days <= 0:
        raise ValueError("bootstrap block days must be positive")
    if calendar_end_day < calendar_start_day:
        raise ValueError("bootstrap calendar must be non-empty")
    day_count = calendar_end_day - calendar_start_day + 1
    effective_block_days = min(block_days, day_count)
    full_blocks, remainder = divmod(day_count, effective_block_days)
    block_lengths = [effective_block_days] * full_blocks
    if remainder:
        block_lengths.append(remainder)

    selected = _daily_direction_counts(
        selected_rows,
        start_day=calendar_start_day,
        day_count=day_count,
    )
    baseline = _daily_direction_counts(
        baseline_rows,
        start_day=calendar_start_day,
        day_count=day_count,
    )
    lengths = set(block_lengths)
    selected_sums = tuple(
        {length: _circular_block_sums(values, length) for length in lengths}
        for values in selected
    )
    baseline_sums = tuple(
        {length: _circular_block_sums(values, length) for length in lengths}
        for values in baseline
    )

    selected_strict: list[float] = []
    selected_resolved: list[float] = []
    strict_uplift: list[float] = []
    resolved_uplift: list[float] = []
    invalid = defaultdict(int)
    schedule_digest = hashlib.sha256()
    rng = random.Random(seed)
    for _ in range(samples):
        starts = [rng.randrange(day_count) for _ in block_lengths]
        schedule_digest.update(
            ",".join(str(value) for value in starts).encode("ascii") + b"\n"
        )
        selected_evaluable, selected_correct, selected_resolved_count = _bootstrap_totals(
            selected_sums, starts, block_lengths
        )
        baseline_evaluable, baseline_correct, baseline_resolved_count = _bootstrap_totals(
            baseline_sums, starts, block_lengths
        )
        if selected_evaluable > 0:
            selected_strict_value = selected_correct / selected_evaluable
            selected_strict.append(selected_strict_value)
            if baseline_evaluable > 0:
                strict_uplift.append(
                    selected_strict_value - baseline_correct / baseline_evaluable
                )
            else:
                invalid["strict_uplift"] += 1
        else:
            invalid["selected_strict"] += 1
            invalid["strict_uplift"] += 1
        if selected_resolved_count > 0:
            selected_resolved_value = selected_correct / selected_resolved_count
            selected_resolved.append(selected_resolved_value)
            if baseline_resolved_count > 0:
                resolved_uplift.append(
                    selected_resolved_value - baseline_correct / baseline_resolved_count
                )
            else:
                invalid["resolved_uplift"] += 1
        else:
            invalid["selected_resolved"] += 1
            invalid["resolved_uplift"] += 1

    def interval(values: Sequence[float]) -> list[float | None]:
        return (
            [None, None]
            if not values
            else [_quantile(values, 0.025), _quantile(values, 0.975)]
        )

    return {
        "method": "shared circular UTC-calendar moving-block percentile bootstrap",
        "samples": samples,
        "seed": seed,
        "requested_block_days": block_days,
        "effective_block_days": effective_block_days,
        "calendar_days": day_count,
        "shared_draw_schedule_sha256": schedule_digest.hexdigest(),
        "selected_strict_accuracy_95_interval": interval(selected_strict),
        "selected_resolved_accuracy_95_interval": interval(selected_resolved),
        "strict_accuracy_uplift_vs_baseline_95_interval": interval(strict_uplift),
        "resolved_accuracy_uplift_vs_baseline_95_interval": interval(resolved_uplift),
        "valid_replicates": {
            "selected_strict": len(selected_strict),
            "selected_resolved": len(selected_resolved),
            "strict_uplift": len(strict_uplift),
            "resolved_uplift": len(resolved_uplift),
        },
        "invalid_replicates": dict(sorted(invalid.items())),
    }


def _in_score_bucket(score: float | None, lower: float, upper: float, closed: bool) -> bool:
    if score is None:
        return False
    return lower <= score <= upper if closed else lower <= score < upper


def _selection_predicates(
    top_quartile_cutoff: float,
) -> tuple[tuple[str, Callable[[ScoredIndicatorObservation], bool]], ...]:
    buckets = tuple(
        (
            name,
            lambda row, low=lower, high=upper, include_upper=closed: _in_score_bucket(
                row.composite_score, low, high, include_upper
            ),
        )
        for name, lower, upper, closed in _FIXED_SCORE_BUCKETS
    )
    return (
        ("baseline_all", lambda row: True),
        *buckets,
        (
            "development_top_quartile",
            lambda row: row.composite_score is not None
            and row.composite_score >= top_quartile_cutoff,
        ),
    )


def _direction_summary(
    selected: Sequence[ScoredIndicatorObservation],
    baseline: Sequence[ScoredIndicatorObservation],
) -> dict[str, Any]:
    statuses = [_direction_status(row.observation) for row in selected]
    evaluable_statuses = [status for status in statuses if status is not None]
    correct = evaluable_statuses.count("correct")
    wrong = evaluable_statuses.count("wrong")
    ties = evaluable_statuses.count("tie")
    resolved = correct + wrong
    directional_returns = [
        value
        for row in selected
        if (value := _directional_return(row.observation)) is not None
    ]
    baseline_evaluable = sum(
        _direction_status(row.observation) is not None for row in baseline
    )
    scored_baseline = sum(row.composite_score is not None for row in baseline)
    selected_scored = sum(row.composite_score is not None for row in selected)
    return {
        "baseline_events": len(baseline),
        "baseline_evaluable": baseline_evaluable,
        "baseline_scored": scored_baseline,
        "feature_coverage": scored_baseline / len(baseline) if baseline else 0.0,
        "selected_events": len(selected),
        "selected_scored": selected_scored,
        "selection_coverage": len(selected) / len(baseline) if baseline else 0.0,
        "selected_evaluable": len(evaluable_statuses),
        "selected_evaluable_coverage": (
            len(evaluable_statuses) / baseline_evaluable if baseline_evaluable else 0.0
        ),
        "correct": correct,
        "wrong": wrong,
        "tie": ties,
        "strict_accuracy": correct / len(evaluable_statuses) if evaluable_statuses else None,
        "resolved_accuracy": correct / resolved if resolved else None,
        "resolved_coverage": resolved / len(evaluable_statuses) if evaluable_statuses else 0.0,
        "median_directional_return": (
            statistics.median(directional_returns) if directional_returns else None
        ),
    }


def evaluate_indicator_discrimination(
    scored_rows: Sequence[ScoredIndicatorObservation],
    *,
    split_names: Sequence[str],
    top_quartile_cutoff: float,
    bootstrap_samples: int,
    bootstrap_block_days: int,
    seed: int,
) -> list[dict[str, Any]]:
    """Evaluate fixed buckets and the frozen top-quartile rule without refitting."""

    if not split_names or len(set(split_names)) != len(split_names):
        raise ValueError("evaluation split names must be non-empty and unique")
    output: list[dict[str, Any]] = []
    dimensions: tuple[
        tuple[str, Callable[[ScoredIndicatorObservation], str]], ...
    ] = (
        ("overall", lambda row: "all"),
        ("direction", lambda row: row.observation.direction),
        ("market", lambda row: row.observation.market),
        ("cohort", lambda row: row.observation.cohort),
        ("asset", lambda row: row.observation.asset),
    )
    selections = _selection_predicates(top_quartile_cutoff)
    for split in split_names:
        split_rows = [row for row in scored_rows if row.observation.split == split]
        if not split_rows:
            raise ValueError(f"evaluation split has no recommendations: {split}")
        start_day = min(row.observation.decision_time_ms for row in split_rows) // _DAY_MS
        end_day = max(row.observation.decision_time_ms for row in split_rows) // _DAY_MS
        for dimension, key_function in dimensions:
            grouped: dict[str, list[ScoredIndicatorObservation]] = defaultdict(list)
            for row in split_rows:
                grouped[key_function(row)].append(row)
            for value, baseline in sorted(grouped.items()):
                dimension_selections = (
                    selections
                    if dimension == "overall"
                    else (selections[0], selections[-1])
                )
                for selection_name, predicate in dimension_selections:
                    selected = [row for row in baseline if predicate(row)]
                    summary = _direction_summary(selected, baseline)
                    bootstrap = block_bootstrap_direction_accuracy(
                        selected,
                        baseline,
                        calendar_start_day=start_day,
                        calendar_end_day=end_day,
                        samples=bootstrap_samples,
                        block_days=bootstrap_block_days,
                        seed=seed,
                    )
                    baseline_accuracy = _direction_summary(baseline, baseline)[
                        "strict_accuracy"
                    ]
                    baseline_resolved_accuracy = _direction_summary(baseline, baseline)[
                        "resolved_accuracy"
                    ]
                    summary["strict_accuracy_uplift_vs_baseline"] = (
                        None
                        if summary["strict_accuracy"] is None or baseline_accuracy is None
                        else summary["strict_accuracy"] - baseline_accuracy
                    )
                    summary["resolved_accuracy_uplift_vs_baseline"] = (
                        None
                        if summary["resolved_accuracy"] is None
                        or baseline_resolved_accuracy is None
                        else summary["resolved_accuracy"] - baseline_resolved_accuracy
                    )
                    output.append(
                        {
                            "split": split,
                            "dimension": dimension,
                            "value": value,
                            "selection": selection_name,
                            **summary,
                            "bootstrap": bootstrap,
                        }
                    )
    return output


def build_historical_success_gate(
    evaluations: Sequence[dict[str, Any]],
    *,
    validation_split: str,
) -> dict[str, Any]:
    """Apply the preregistered descriptive validation gate without tuning it."""

    top_rows = [
        row
        for row in evaluations
        if row["split"] == validation_split
        and row["selection"] == "development_top_quartile"
    ]

    def one(dimension: str, value: str) -> dict[str, Any] | None:
        matches = [
            row
            for row in top_rows
            if row["dimension"] == dimension and row["value"] == value
        ]
        if len(matches) > 1:
            raise ValueError(f"duplicate validation gate row: {dimension}={value}")
        return matches[0] if matches else None

    overall = one("overall", "all")
    if overall is None:
        raise ValueError("validation gate requires the overall top-quartile row")
    long_row = one("direction", "long")
    short_row = one("direction", "short")
    asset_rows = [row for row in top_rows if row["dimension"] == "asset"]

    accuracy = overall["strict_accuracy"]
    accuracy_lcb = overall["bootstrap"]["selected_strict_accuracy_95_interval"][0]
    uplift = overall["strict_accuracy_uplift_vs_baseline"]
    uplift_lcb = overall["bootstrap"][
        "strict_accuracy_uplift_vs_baseline_95_interval"
    ][0]
    median_return = overall["median_directional_return"]
    long_accuracy = None if long_row is None else long_row["strict_accuracy"]
    short_accuracy = None if short_row is None else short_row["strict_accuracy"]
    positive_asset_lifts = sum(
        row["strict_accuracy_uplift_vs_baseline"] is not None
        and row["strict_accuracy_uplift_vs_baseline"] > 0
        for row in asset_rows
    )
    criteria = {
        "strict_accuracy_at_least_52_5pct": accuracy is not None and accuracy >= 0.525,
        "strict_accuracy_lcb_above_50pct": accuracy_lcb is not None and accuracy_lcb > 0.50,
        "strict_accuracy_uplift_at_least_3pp": uplift is not None and uplift >= 0.03,
        "strict_accuracy_uplift_lcb_above_zero": (
            uplift_lcb is not None and uplift_lcb > 0
        ),
        "median_directional_return_above_zero": (
            median_return is not None and median_return > 0
        ),
        "long_and_short_accuracy_above_50pct": (
            long_accuracy is not None
            and short_accuracy is not None
            and long_accuracy > 0.50
            and short_accuracy > 0.50
        ),
        "retention_at_least_20pct": overall["selection_coverage"] >= 0.20,
        "evaluable_n_at_least_300": overall["selected_evaluable"] >= 300,
        "positive_asset_lift_at_least_6_of_8": (
            len(asset_rows) >= 8 and positive_asset_lifts >= 6
        ),
    }
    return {
        "role": "historical_validation_diagnostic_not_prospective_proof",
        "selection": "development_top_quartile",
        "metrics": {
            "strict_accuracy": accuracy,
            "strict_accuracy_lcb_95": accuracy_lcb,
            "strict_accuracy_uplift_vs_baseline": uplift,
            "strict_accuracy_uplift_lcb_95": uplift_lcb,
            "median_directional_return": median_return,
            "long_strict_accuracy": long_accuracy,
            "short_strict_accuracy": short_accuracy,
            "retention": overall["selection_coverage"],
            "selected_evaluable": overall["selected_evaluable"],
            "asset_groups_evaluated": len(asset_rows),
            "assets_with_positive_accuracy_lift": positive_asset_lifts,
        },
        "criteria": criteria,
        "overall_pass": all(criteria.values()),
    }


def _required_columns(reader: csv.DictReader[str], required: set[str], *, label: str) -> None:
    available = set(reader.fieldnames or ())
    missing = sorted(required - available)
    if missing:
        raise ValueError(f"{label} is missing required columns: {missing}")


def _parse_int(value: str | None, *, label: str) -> int:
    if value is None or not value.strip():
        raise ValueError(f"{label} must be present")
    try:
        return int(value)
    except ValueError as error:
        raise ValueError(f"{label} must be an integer") from error


def _parse_float(value: str | None, *, label: str, optional: bool = False) -> float | None:
    if value is None or not value.strip():
        if optional:
            return None
        raise ValueError(f"{label} must be present")
    try:
        parsed = float(value)
    except ValueError as error:
        raise ValueError(f"{label} must be numeric") from error
    return _finite_optional(parsed, label=label)


def _parse_bool(value: str | None, *, label: str) -> bool:
    normalized = "" if value is None else value.strip().lower()
    if normalized in {"true", "1"}:
        return True
    if normalized in {"false", "0"}:
        return False
    raise ValueError(f"{label} must be a boolean")


def read_indicator_inputs(
    recommendations_path: str | Path,
    outcomes_path: str | Path,
    *,
    horizon_bars: int = _PRIMARY_HORIZON_BARS,
) -> LoadedIndicatorData:
    """Strictly read alert-replay CSVs without mutating either source artifact."""

    if horizon_bars <= 0:
        raise ValueError("horizon_bars must be positive")
    outcome_required = {"event_id", "horizon_bars", "evaluable", "exit_price"}
    outcomes: dict[str, tuple[bool, float | None]] = {}
    primary_outcome_rows = 0
    with Path(outcomes_path).open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        _required_columns(reader, outcome_required, label="outcomes.csv")
        for line_number, row in enumerate(reader, start=2):
            outcome_horizon = _parse_int(
                row.get("horizon_bars"),
                label=f"outcomes line {line_number} horizon",
            )
            if outcome_horizon != horizon_bars:
                continue
            primary_outcome_rows += 1
            event_id = (row.get("event_id") or "").strip()
            if not event_id:
                raise ValueError(f"outcomes line {line_number} event_id must be present")
            if event_id in outcomes:
                raise ValueError(f"duplicate primary outcome event_id: {event_id}")
            evaluable = _parse_bool(
                row.get("evaluable"), label=f"outcomes line {line_number} evaluable"
            )
            future_close = _parse_float(
                row.get("exit_price"),
                label=f"outcomes line {line_number} exit_price",
                optional=not evaluable,
            )
            if future_close is not None and future_close <= 0:
                raise ValueError(f"outcomes line {line_number} exit_price must be positive")
            outcomes[event_id] = (evaluable, future_close)

    recommendation_required = {
        "event_id",
        "asset",
        "cohort",
        "market",
        "direction",
        "decision_time_ms",
        "split",
        "price",
        *FEATURE_COLUMNS,
    }
    observations: list[IndicatorObservation] = []
    seen_event_ids: set[str] = set()
    matched = 0
    with Path(recommendations_path).open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        _required_columns(reader, recommendation_required, label="recommendations.csv")
        for line_number, row in enumerate(reader, start=2):
            event_id = (row.get("event_id") or "").strip()
            if not event_id:
                raise ValueError(f"recommendations line {line_number} event_id must be present")
            if event_id in seen_event_ids:
                raise ValueError(f"duplicate recommendation event_id: {event_id}")
            seen_event_ids.add(event_id)
            direction = (row.get("direction") or "").strip().lower()
            if direction not in {"long", "short"}:
                raise ValueError(f"recommendations line {line_number} has invalid direction")
            market = (row.get("market") or "").strip().lower()
            if market not in {"spot", "futures"}:
                raise ValueError(f"recommendations line {line_number} has invalid market")
            asset = (row.get("asset") or "").strip().upper()
            cohort = (row.get("cohort") or "").strip()
            split = (row.get("split") or "").strip()
            if not asset or not cohort or not split:
                raise ValueError(
                    f"recommendations line {line_number} asset/cohort/split must be present"
                )
            outcome = outcomes.get(event_id)
            if outcome is None:
                outcome_available, evaluable, future_close = False, False, None
            else:
                outcome_available, evaluable, future_close = True, outcome[0], outcome[1]
                matched += 1
            recommendation_price = _parse_float(
                row.get("price"),
                label=f"recommendations line {line_number} price",
            )
            if recommendation_price is None or recommendation_price <= 0:
                raise ValueError(
                    f"recommendations line {line_number} price must be positive"
                )
            features = tuple(
                _parse_float(
                    row.get(name),
                    label=f"recommendations line {line_number} {name}",
                    optional=True,
                )
                for name in FEATURE_COLUMNS
            )
            observations.append(
                IndicatorObservation(
                    event_id=event_id,
                    asset=asset,
                    cohort=cohort,
                    market=market,
                    direction=direction,
                    decision_time_ms=_parse_int(
                        row.get("decision_time_ms"),
                        label=f"recommendations line {line_number} decision_time_ms",
                    ),
                    split=split,
                    features=features,
                    recommendation_price=recommendation_price,
                    outcome_available=outcome_available,
                    evaluable=evaluable,
                    future_close=future_close,
                )
            )
    return LoadedIndicatorData(
        observations=tuple(observations),
        audit=InputAudit(
            recommendation_rows=len(observations),
            primary_horizon_outcome_rows=primary_outcome_rows,
            matched_outcome_rows=matched,
            missing_outcome_rows=len(observations) - matched,
            orphan_outcome_rows=len(set(outcomes) - seen_event_ids),
        ),
    )


def read_indicator_input_sets(
    input_pairs: Sequence[tuple[str | Path, str | Path]],
) -> LoadedIndicatorData:
    """Merge independently replayed split artifacts under one strict event-ID contract."""

    if not input_pairs:
        raise ValueError("at least one recommendations/outcomes input pair is required")
    observations: list[IndicatorObservation] = []
    seen: set[str] = set()
    audits: list[InputAudit] = []
    for recommendations_path, outcomes_path in input_pairs:
        loaded = read_indicator_inputs(recommendations_path, outcomes_path)
        duplicates = sorted(
            row.event_id for row in loaded.observations if row.event_id in seen
        )
        if duplicates:
            raise ValueError(
                "duplicate recommendation event_id across input sets: "
                f"{duplicates[0]}"
            )
        observations.extend(loaded.observations)
        seen.update(row.event_id for row in loaded.observations)
        audits.append(loaded.audit)
    return LoadedIndicatorData(
        observations=tuple(observations),
        audit=InputAudit(
            recommendation_rows=sum(audit.recommendation_rows for audit in audits),
            primary_horizon_outcome_rows=sum(
                audit.primary_horizon_outcome_rows for audit in audits
            ),
            matched_outcome_rows=sum(audit.matched_outcome_rows for audit in audits),
            missing_outcome_rows=sum(audit.missing_outcome_rows for audit in audits),
            orphan_outcome_rows=sum(audit.orphan_outcome_rows for audit in audits),
        ),
    )


def _sha256(path: str | Path) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        while chunk := handle.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def _json_text(payload: Any) -> str:
    return (
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True, allow_nan=False)
        + "\n"
    )


def _fmt_percent(value: float | None) -> str:
    return "n/a" if value is None else f"{value * 100:.2f}%"


def _fmt_interval(values: Sequence[float | None]) -> str:
    if len(values) != 2 or values[0] is None or values[1] is None:
        return "n/a"
    return f"{values[0] * 100:.2f}%~{values[1] * 100:.2f}%"


def render_korean_report(results: dict[str, Any]) -> str:
    model = results["model"]
    audit = results["deduplication"]
    lines = [
        "# 기술지표 방향 판별력 실험",
        "",
        "## 결론을 읽는 법",
        "",
        "이 보고서는 추천 뒤 12개 5분봉(60분)의 **가격 방향만** 평가합니다. "
        "점수는 확률이 아니며, 개발구간에서 적합한 경험적 백분위 변환과 "
        "75분위 선택선을 검증·스트레스 구간에 그대로 적용합니다.",
        "",
        f"- 개발 추천: {model['development_rows']:,}개",
        f"- 개발 점수 계산 가능: {model['scored_development_rows']:,}개",
        f"- 동결된 개발 점수 75분위: {model['development_score_75th_percentile']:.4f}/100",
        f"- 현물 우선 중복 제거: {audit['input_rows']:,}개 → {audit['output_rows']:,}개 "
        f"(제거 {audit['duplicate_rows_dropped']:,}개)",
        "",
        "## 전체 표본: 기준선·고정 점수대·개발 상위 25%",
        "",
        "`strict` 정확도는 보합을 오답으로 포함합니다. `resolved` 정확도는 보합을 "
        "제외합니다. CI와 uplift CI는 같은 UTC 날짜 블록 추출표를 공유합니다.",
        "",
        "| 구간 | 선택 | 선택/기준 | 선택 coverage | 정답/오답/보합 | strict 정확도 (95% CI) | "
        "strict uplift (95% CI) | resolved 정확도 |",
        "|---|---|---:|---:|---:|---:|---:|---:|",
    ]
    overall_rows = [
        row for row in results["evaluations"] if row["dimension"] == "overall"
    ]
    for row in overall_rows:
        bootstrap = row["bootstrap"]
        lines.append(
            f"| {row['split']} | {row['selection']} | "
            f"{row['selected_events']:,}/{row['baseline_events']:,} | "
            f"{_fmt_percent(row['selection_coverage'])} | "
            f"{row['correct']:,}/{row['wrong']:,}/{row['tie']:,} | "
            f"{_fmt_percent(row['strict_accuracy'])} "
            f"({_fmt_interval(bootstrap['selected_strict_accuracy_95_interval'])}) | "
            f"{_fmt_percent(row['strict_accuracy_uplift_vs_baseline'])} "
            f"({_fmt_interval(bootstrap['strict_accuracy_uplift_vs_baseline_95_interval'])}) | "
            f"{_fmt_percent(row['resolved_accuracy'])} |"
        )

    lines.extend(
        [
            "",
            "## 방향·시장·코호트별 기준선과 개발 상위 25%",
            "",
            "| 구간 | 분류 | 값 | 선택 | 선택/기준 | coverage | strict 정확도 | 95% CI | uplift |",
            "|---|---|---|---|---:|---:|---:|---:|---:|",
        ]
    )
    grouped_rows = [
        row
        for row in results["evaluations"]
        if row["dimension"] != "overall"
        and row["selection"] in {"baseline_all", "development_top_quartile"}
    ]
    for row in grouped_rows:
        bootstrap = row["bootstrap"]
        lines.append(
            f"| {row['split']} | {row['dimension']} | {row['value']} | "
            f"{row['selection']} | {row['selected_events']:,}/{row['baseline_events']:,} | "
            f"{_fmt_percent(row['selection_coverage'])} | "
            f"{_fmt_percent(row['strict_accuracy'])} | "
            f"{_fmt_interval(bootstrap['selected_strict_accuracy_95_interval'])} | "
            f"{_fmt_percent(row['strict_accuracy_uplift_vs_baseline'])} |"
        )

    gate = results["historical_success_gate"]
    gate_metrics = gate["metrics"]
    lines.extend(
        [
            "",
            "## Validation historical success gate",
            "",
            f"전체 판정: **{'PASS' if gate['overall_pass'] else 'FAIL'}**  ",
            f"정확도 {_fmt_percent(gate_metrics['strict_accuracy'])}, "
            f"하한 {_fmt_percent(gate_metrics['strict_accuracy_lcb_95'])}, "
            f"uplift {_fmt_percent(gate_metrics['strict_accuracy_uplift_vs_baseline'])}, "
            f"중앙 방향수익률 {_fmt_percent(gate_metrics['median_directional_return'])}, "
            f"유지율 {_fmt_percent(gate_metrics['retention'])}, "
            f"평가 가능 N={gate_metrics['selected_evaluable']:,}.",
            "",
            "| 사전등록 조건 | 통과 |",
            "|---|---:|",
        ]
    )
    for name, passed in gate["criteria"].items():
        lines.append(f"| `{name}` | {passed} |")

    lines.extend(
        [
            "",
            "## 해석 경계",
            "",
            "- 입력은 alert replay의 12봉 결과이며, 원시 가격 방향만 사용합니다. "
            "수수료·슬리피지·최소 수익 문턱은 사용하지 않습니다.",
            "- 결과 가격 기준은 추천 판단봉 종가(P0)에서 12봉 뒤 종가(P60)까지입니다.",
            "- `(asset, direction, decision_time_ms)`가 같으면 현물을 우선합니다. 현물 SHORT는 "
            "실현 가능한 공매도 수익이 아니라 하락 방향 진단입니다.",
            "- 네 축은 동일가중입니다. 축 안에서 결측 지표는 남은 지표로 평균하지만, 어느 한 "
            "축 전체가 비면 점수를 만들지 않고 feature coverage에 반영합니다.",
            "- `validation`은 새 점수에 대한 확인 구간이고 `retrospective_test`는 이미 관찰된 "
            "기간일 수 있으므로 스트레스 진단이지 독립적인 최종 검증이 아닙니다.",
            "- 개발구간 성과표는 cutoff 유지율 점검용입니다. 같은 구간에서 CDF와 cutoff를 "
            "적합했으므로 성과 증거로 사용하지 않습니다.",
            "- 상위 25% 선택은 개발분포에서 한 번 고정한 문턱입니다. 이 점수나 백분위는 "
            "상승 확률로 해석할 수 없습니다.",
            "",
            "## Material Passport",
            "",
            "- Generator: `academic-research-suite / experiment-agent`",
            "- Stage: development-fit, validation, retrospective stress",
            "- Inputs: alert replay `recommendations.csv` + `outcomes.csv`",
            "- Primary endpoint: 12-bar raw direction accuracy, tie counted incorrect",
            "- Dependence control: shared circular UTC-calendar moving-block bootstrap",
            "- Production impact: none; frozen protocol and live alert rules are unchanged",
            "",
        ]
    )
    return "\n".join(lines)


def run_indicator_analysis_files(
    *,
    input_pairs: Sequence[tuple[str | Path, str | Path]],
    output_dir: str | Path,
    development_split: str,
    validation_split: str,
    stress_split: str,
    assets: Sequence[str] | None,
    bootstrap_samples: int,
    bootstrap_block_days: int,
    seed: int,
) -> dict[str, Any]:
    split_names = (development_split, validation_split, stress_split)
    if len(set(split_names)) != len(split_names) or any(not name for name in split_names):
        raise ValueError("development, validation, and stress splits must be distinct")
    loaded = read_indicator_input_sets(input_pairs)
    asset_filter = None if assets is None else {asset.strip().upper() for asset in assets}
    if asset_filter is not None and (not asset_filter or "" in asset_filter):
        raise ValueError("asset filter must contain non-empty asset names")
    relevant = [
        row
        for row in loaded.observations
        if row.split in split_names and (asset_filter is None or row.asset in asset_filter)
    ]
    if not relevant:
        raise ValueError("no recommendations match the requested splits and assets")
    deduplicated = deduplicate_spot_priority(relevant)
    development = [
        row for row in deduplicated.observations if row.split == development_split
    ]
    model = fit_percentile_composite(development)
    scored = score_observations(deduplicated.observations, model)
    evaluations = evaluate_indicator_discrimination(
        scored,
        split_names=(development_split, validation_split, stress_split),
        top_quartile_cutoff=model.top_quartile_cutoff,
        bootstrap_samples=bootstrap_samples,
        bootstrap_block_days=bootstrap_block_days,
        seed=seed,
    )
    historical_success_gate = build_historical_success_gate(
        evaluations,
        validation_split=validation_split,
    )
    split_inventory = []
    for split in split_names:
        values = [row for row in scored if row.observation.split == split]
        split_inventory.append(
            {
                "split": split,
                "recommendations": len(values),
                "outcome_available": sum(row.observation.outcome_available for row in values),
                "evaluable": sum(row.observation.evaluable for row in values),
                "scored": sum(row.composite_score is not None for row in values),
                "feature_coverage": (
                    sum(row.composite_score is not None for row in values) / len(values)
                    if values
                    else 0.0
                ),
            }
        )
    results: dict[str, Any] = {
        "schema_version": _SCHEMA_VERSION,
        "protocol": "indicator_direction_discrimination_exploratory_v1",
        "primary_endpoint": {
            "horizon_bars": _PRIMARY_HORIZON_BARS,
            "reference": "recommendation candle close P0 to 12-bar horizon close P60",
            "long_correct": "P60 > P0",
            "short_correct": "P60 < P0",
            "tie_is_correct": False,
            "costs_or_no_call_margin_used": False,
        },
        "splits": {
            "development": development_split,
            "validation": validation_split,
            "retrospective_stress": stress_split,
        },
        "assets": sorted(asset_filter) if asset_filter is not None else "all input assets",
        "input_audit": asdict(loaded.audit),
        "deduplication": {
            "key": ["asset", "direction", "decision_time_ms"],
            "priority": ["spot", "futures"],
            **asdict(deduplicated.audit),
        },
        "model": model.artifact(),
        "split_inventory": split_inventory,
        "evaluations": evaluations,
        "historical_success_gate": historical_success_gate,
        "bootstrap_contract": {
            "samples": bootstrap_samples,
            "block_days": bootstrap_block_days,
            "seed": seed,
            "calendar": "observed recommendation span within each evaluation split",
            "shared_draws": "same seed and calendar within a split/group comparison",
        },
        "source_inputs": [
            {
                "recommendations_path": str(recommendations_path),
                "recommendations_sha256": _sha256(recommendations_path),
                "outcomes_path": str(outcomes_path),
                "outcomes_sha256": _sha256(outcomes_path),
            }
            for recommendations_path, outcomes_path in input_pairs
        ],
        "material_passport": {
            "generator": "academic-research-suite / experiment-agent",
            "stage": "development-fit, validation, retrospective stress",
            "production_impact": "none",
        },
    }
    root = Path(output_dir)
    root.mkdir(parents=True, exist_ok=True)
    model_text = _json_text(model.artifact())
    results_text = _json_text(results)
    report_text = render_korean_report(results)
    (root / "fitted_score.json").write_text(model_text, encoding="utf-8")
    (root / "results.json").write_text(results_text, encoding="utf-8")
    (root / "report_ko.md").write_text(report_text, encoding="utf-8")
    (root / "indicator_analysis_results.json").write_text(results_text, encoding="utf-8")
    (root / "indicator_analysis_report_ko.md").write_text(report_text, encoding="utf-8")
    return results


def run_indicator_analysis(
    *,
    recommendations_path: str | Path,
    outcomes_path: str | Path,
    output_dir: str | Path,
    development_split: str,
    validation_split: str,
    stress_split: str,
    assets: Sequence[str] | None,
    bootstrap_samples: int,
    bootstrap_block_days: int,
    seed: int,
) -> dict[str, Any]:
    """Analyze one alert-replay directory; use ``run_indicator_analysis_files`` for many."""

    return run_indicator_analysis_files(
        input_pairs=((recommendations_path, outcomes_path),),
        output_dir=output_dir,
        development_split=development_split,
        validation_split=validation_split,
        stress_split=stress_split,
        assets=assets,
        bootstrap_samples=bootstrap_samples,
        bootstrap_block_days=bootstrap_block_days,
        seed=seed,
    )


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Fit a development ECDF indicator score and evaluate direction accuracy."
    )
    parser.add_argument("--recommendations", type=Path)
    parser.add_argument("--outcomes", type=Path)
    parser.add_argument(
        "--replay-dir",
        type=Path,
        action="append",
        help="Repeat for independently replayed split directories.",
    )
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--development-split", default="development")
    parser.add_argument("--validation-split", default="validation")
    parser.add_argument("--stress-split", default="retrospective_test")
    parser.add_argument("--assets", nargs="+")
    parser.add_argument("--bootstrap-samples", type=int, default=10_000)
    parser.add_argument("--bootstrap-block-days", type=int, default=7)
    parser.add_argument("--seed", type=int, default=20_260_720)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = _parser()
    args = parser.parse_args(argv)
    if args.replay_dir:
        if args.recommendations is not None or args.outcomes is not None:
            parser.error("--replay-dir cannot be mixed with --recommendations/--outcomes")
        input_pairs = tuple(
            (root / "recommendations.csv", root / "outcomes.csv")
            for root in args.replay_dir
        )
    else:
        if args.recommendations is None or args.outcomes is None:
            parser.error(
                "provide both --recommendations and --outcomes, or repeat --replay-dir"
            )
        input_pairs = ((args.recommendations, args.outcomes),)
    try:
        run_indicator_analysis_files(
            input_pairs=input_pairs,
            output_dir=args.output_dir,
            development_split=args.development_split,
            validation_split=args.validation_split,
            stress_split=args.stress_split,
            assets=args.assets,
            bootstrap_samples=args.bootstrap_samples,
            bootstrap_block_days=args.bootstrap_block_days,
            seed=args.seed,
        )
    except (OSError, ValueError) as error:
        parser.error(str(error))
    print(Path(args.output_dir) / "fitted_score.json")
    print(Path(args.output_dir) / "results.json")
    print(Path(args.output_dir) / "report_ko.md")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
