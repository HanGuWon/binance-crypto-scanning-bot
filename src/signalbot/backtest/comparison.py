from __future__ import annotations

import csv
import math
import random
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any


@dataclass(frozen=True, slots=True)
class TradeObservation:
    market: str
    direction: str
    exit_time_ms: int
    net_return: float


@dataclass(frozen=True, slots=True)
class OpportunityObservation:
    """One variant's frozen view of a price-trigger opportunity."""

    opportunity_id: str
    market: str
    direction: str
    decision_time_ms: int
    eligible: bool
    analysis_eligible: bool
    volume_feature_available: bool
    forward_return_12: float | None
    analysis_eligible_3: bool = False
    analysis_eligible_72: bool = False
    forward_return_3: float | None = None
    forward_return_72: float | None = None


@dataclass(frozen=True, slots=True)
class AlignedOpportunity:
    """C0, G2, and G4 observations joined on one immutable opportunity ID."""

    opportunity_id: str
    c0: OpportunityObservation
    g2: OpportunityObservation
    g4: OpportunityObservation

    def observation(self, variant: str) -> OpportunityObservation:
        if variant == "C0":
            return self.c0
        if variant == "G2":
            return self.g2
        if variant == "G4":
            return self.g4
        raise ValueError(f"unsupported opportunity variant: {variant}")


_OPPORTUNITY_VARIANTS = ("C0", "G2", "G4")
_OPPORTUNITY_SIDES = (("spot", "long"), ("futures", "short"))
_OPPORTUNITY_CONTRASTS = (
    ("G2-C0", "G2", "C0"),
    ("G4-C0", "G4", "C0"),
)
_DAY_MS = 86_400_000


def read_trade_observations(path: str | Path) -> list[TradeObservation]:
    with Path(path).open("r", encoding="utf-8", newline="") as handle:
        rows = csv.DictReader(handle)
        return [
            TradeObservation(
                market=str(row["market"]),
                direction=str(row["direction"]),
                exit_time_ms=int(row["exit_time_ms"]),
                net_return=float(row["net_return"]),
            )
            for row in rows
        ]


def _strict_bool(value: str | None, field: str) -> bool:
    normalized = "" if value is None else value.strip().lower()
    if normalized in {"true", "1"}:
        return True
    if normalized in {"false", "0"}:
        return False
    raise ValueError(f"{field} must be true or false")


def _optional_finite_float(value: str | None, field: str) -> float | None:
    if value is None or not value.strip():
        return None
    parsed = float(value)
    if not math.isfinite(parsed):
        raise ValueError(f"{field} must be finite when present")
    return parsed


def read_opportunity_observations(path: str | Path) -> list[OpportunityObservation]:
    """Read the typed subset of a backtest ``opportunities.csv`` artifact."""

    observations: list[OpportunityObservation] = []
    with Path(path).open("r", encoding="utf-8", newline="") as handle:
        rows = csv.DictReader(handle)
        for line_number, row in enumerate(rows, start=2):
            try:
                opportunity_id = str(row["opportunity_id"]).strip()
                if not opportunity_id:
                    raise ValueError("opportunity_id must not be empty")
                decision_time_ms = int(row["decision_time_ms"])
                if decision_time_ms < 0:
                    raise ValueError("decision_time_ms must be non-negative")
                analysis_eligible = _strict_bool(
                    row.get("analysis_eligible"), "analysis_eligible"
                )
                analysis_eligible_12 = _strict_bool(
                    row.get("analysis_eligible_12"), "analysis_eligible_12"
                )
                if analysis_eligible != analysis_eligible_12:
                    raise ValueError("analysis_eligible must alias analysis_eligible_12")
                observations.append(
                    OpportunityObservation(
                        opportunity_id=opportunity_id,
                        market=str(row["market"]).strip().lower(),
                        direction=str(row["direction"]).strip().lower(),
                        decision_time_ms=decision_time_ms,
                        eligible=_strict_bool(row.get("eligible"), "eligible"),
                        analysis_eligible=analysis_eligible,
                        volume_feature_available=_strict_bool(
                            row.get("volume_feature_available"),
                            "volume_feature_available",
                        ),
                        forward_return_12=_optional_finite_float(
                            row.get("forward_return_12"), "forward_return_12"
                        ),
                        analysis_eligible_3=_strict_bool(
                            row.get("analysis_eligible_3"), "analysis_eligible_3"
                        ),
                        analysis_eligible_72=_strict_bool(
                            row.get("analysis_eligible_72"), "analysis_eligible_72"
                        ),
                        forward_return_3=_optional_finite_float(
                            row.get("forward_return_3"), "forward_return_3"
                        ),
                        forward_return_72=_optional_finite_float(
                            row.get("forward_return_72"), "forward_return_72"
                        ),
                    )
                )
            except (KeyError, TypeError, ValueError) as exc:
                raise ValueError(
                    f"invalid opportunity observation on CSV line {line_number}"
                ) from exc
    return observations


def _validated_opportunity_index(
    variant: str, observations: Sequence[OpportunityObservation]
) -> dict[str, OpportunityObservation]:
    indexed: dict[str, OpportunityObservation] = {}
    for item in observations:
        if not item.opportunity_id:
            raise ValueError(f"{variant} contains an empty opportunity_id")
        if item.decision_time_ms < 0:
            raise ValueError(f"{variant}:{item.opportunity_id} has a negative decision time")
        for horizon, eligible, outcome in (
            (3, item.analysis_eligible_3, item.forward_return_3),
            (12, item.analysis_eligible, item.forward_return_12),
            (72, item.analysis_eligible_72, item.forward_return_72),
        ):
            if outcome is not None and not math.isfinite(outcome):
                raise ValueError(
                    f"{variant}:{item.opportunity_id} has a non-finite h{horizon} outcome"
                )
            if eligible and outcome is None:
                raise ValueError(
                    f"{variant}:{item.opportunity_id} is h{horizon}-eligible without an outcome"
                )
        if item.opportunity_id in indexed:
            raise ValueError(f"{variant} contains duplicate opportunity_id {item.opportunity_id}")
        indexed[item.opportunity_id] = item
    return indexed


def align_common_opportunities(
    panels: Mapping[str, Sequence[OpportunityObservation]],
) -> tuple[AlignedOpportunity, ...]:
    """Join the frozen C0/G2/G4 panels on their common opportunity IDs.

    Outcome and identity fields must be invariant across variants. A mismatch is
    treated as a point-in-time integrity failure rather than averaged away.
    """

    if set(panels) != set(_OPPORTUNITY_VARIANTS) or len(panels) != len(
        _OPPORTUNITY_VARIANTS
    ):
        raise ValueError(f"opportunity panels must contain exactly {_OPPORTUNITY_VARIANTS}")
    indexed = {
        variant: _validated_opportunity_index(variant, panels[variant])
        for variant in _OPPORTUNITY_VARIANTS
    }
    id_sets = {variant: set(indexed[variant]) for variant in _OPPORTUNITY_VARIANTS}
    common_ids = id_sets["C0"]
    if not common_ids:
        raise ValueError("opportunity panels contain no opportunity_id")
    if any(id_sets[variant] != common_ids for variant in ("G2", "G4")):
        counts = {variant: len(values) for variant, values in id_sets.items()}
        raise ValueError(
            "opportunity_id sets differ across variants; "
            f"every C0 trigger must be recorded exactly once: {counts}"
        )

    aligned: list[AlignedOpportunity] = []
    for opportunity_id in common_ids:
        values = tuple(indexed[variant][opportunity_id] for variant in _OPPORTUNITY_VARIANTS)
        first = values[0]
        identity = (first.market, first.direction, first.decision_time_ms)
        if any(
            (item.market, item.direction, item.decision_time_ms) != identity
            for item in values[1:]
        ):
            raise ValueError(f"opportunity identity mismatch for {opportunity_id}")
        first_labels = (
            first.analysis_eligible_3,
            first.analysis_eligible,
            first.analysis_eligible_72,
            first.forward_return_3,
            first.forward_return_12,
            first.forward_return_72,
        )
        if any(
            (
                item.analysis_eligible_3,
                item.analysis_eligible,
                item.analysis_eligible_72,
                item.forward_return_3,
                item.forward_return_12,
                item.forward_return_72,
            )
            != first_labels
            for item in values[1:]
        ):
            raise ValueError(f"future label mismatch for {opportunity_id}")
        aligned.append(
            AlignedOpportunity(
                opportunity_id=opportunity_id,
                c0=values[0],
                g2=values[1],
                g4=values[2],
            )
        )
    return tuple(
        sorted(
            aligned,
            key=lambda item: (item.c0.decision_time_ms, item.opportunity_id),
        )
    )


def _quantile(sorted_values: list[float], probability: float) -> float:
    if not sorted_values:
        raise ValueError("cannot take a quantile of an empty sample")
    bounded = max(0.0, min(1.0, probability))
    index = bounded * (len(sorted_values) - 1)
    lower = math.floor(index)
    upper = math.ceil(index)
    if lower == upper:
        return sorted_values[lower]
    weight = index - lower
    return sorted_values[lower] * (1 - weight) + sorted_values[upper] * weight


def _ratio(values: list[tuple[float, int]], draw: list[int] | None = None) -> float:
    selected = values if draw is None else [values[index] for index in draw]
    total_return = sum(item[0] for item in selected)
    trades = sum(item[1] for item in selected)
    return total_return / trades if trades else 0.0


def compare_strategy_runs(
    runs: dict[str, list[TradeObservation]],
    *,
    evaluation_start_ms: int,
    evaluation_end_ms: int,
    samples: int,
    block_days: int,
    seed: int,
) -> dict[str, Any]:
    """Paired fixed-block comparison with shared calendar draws across contrasts."""
    required = ("b0", "b3", "b2", "headline")
    if tuple(runs) != required:
        raise ValueError(f"runs must be insertion-ordered as {required}")
    if evaluation_end_ms <= evaluation_start_ms:
        raise ValueError("evaluation end must be after start")
    if samples < 100:
        raise ValueError("samples must be at least 100")
    if block_days <= 0:
        raise ValueError("block_days must be positive")

    block_ms = block_days * 86_400_000
    block_count = math.ceil((evaluation_end_ms - evaluation_start_ms) / block_ms)
    sides = (("spot", "long"), ("futures", "short"))
    grouped: dict[tuple[str, str, str], list[tuple[float, int]]] = {}
    for run_name, observations in runs.items():
        for market, direction in sides:
            blocks = [[0.0, 0] for _ in range(block_count)]
            for item in observations:
                if item.market != market or item.direction != direction:
                    continue
                if not evaluation_start_ms <= item.exit_time_ms < evaluation_end_ms:
                    continue
                index = (item.exit_time_ms - evaluation_start_ms) // block_ms
                blocks[index][0] += item.net_return
                blocks[index][1] += 1
            grouped[(run_name, market, direction)] = [
                (float(total), int(count)) for total, count in blocks
            ]

    contrasts = (
        ("headline_vs_zero", "headline", None),
        ("b3_minus_b0", "b3", "b0"),
        ("b2_minus_b3", "b2", "b3"),
        ("headline_minus_b2", "headline", "b2"),
    )
    distributions: dict[tuple[str, str, str], list[float]] = {
        (name, market, direction): []
        for name, _, _ in contrasts
        for market, direction in sides
    }
    rng = random.Random(seed)
    for _ in range(samples):
        draw = [rng.randrange(block_count) for _ in range(block_count)]
        for name, upper, lower in contrasts:
            for market, direction in sides:
                upper_value = _ratio(grouped[(upper, market, direction)], draw)
                lower_value = (
                    0.0
                    if lower is None
                    else _ratio(grouped[(lower, market, direction)], draw)
                )
                distributions[(name, market, direction)].append(
                    upper_value - lower_value
                )

    family_tests = len(contrasts) * len(sides)
    family_alpha = 0.05 / family_tests
    rows: list[dict[str, Any]] = []
    for name, upper, lower in contrasts:
        for market, direction in sides:
            upper_value = _ratio(grouped[(upper, market, direction)])
            lower_value = (
                0.0
                if lower is None
                else _ratio(grouped[(lower, market, direction)])
            )
            distribution = sorted(distributions[(name, market, direction)])
            rows.append(
                {
                    "contrast": name,
                    "market": market,
                    "direction": direction,
                    "effect": upper_value - lower_value,
                    "ci_95_low": _quantile(distribution, 0.025),
                    "ci_95_high": _quantile(distribution, 0.975),
                    "simultaneous_low": _quantile(
                        distribution, family_alpha / 2
                    ),
                    "simultaneous_high": _quantile(
                        distribution, 1 - family_alpha / 2
                    ),
                    "probability_positive": sum(value > 0 for value in distribution)
                    / len(distribution),
                }
            )
    return {
        "method": "paired ratio-of-sums fixed UTC block bootstrap",
        "block_days": block_days,
        "calendar_blocks": block_count,
        "samples": samples,
        "seed": seed,
        "family_tests": family_tests,
        "simultaneous_method": "Bonferroni percentile intervals",
        "rows": rows,
    }


def compare_common_opportunity_panels(
    panels: Mapping[str, Sequence[OpportunityObservation]],
    *,
    evaluation_start_ms: int | None = None,
    evaluation_end_ms: int | None = None,
    samples: int = 50_000,
    block_days: int = 7,
    seed: int = 20_260_715,
) -> dict[str, Any]:
    """Compare C0/G2/G4 on one aligned opportunity-and-outcome panel.

    Each variant contributes ``eligible * forward_return_12`` to the common
    denominator. Moving UTC-day blocks are sampled with replacement, and every
    side and contrast receives the same block-start draw.
    ``probability_positive`` is only the fraction of bootstrap effects above
    zero; it is not a p-value or a probability of future profitability.
    """

    if samples < 100:
        raise ValueError("samples must be at least 100")
    if block_days <= 0:
        raise ValueError("block_days must be positive")
    aligned = align_common_opportunities(panels)
    minimum_time = min(item.c0.decision_time_ms for item in aligned)
    maximum_time = max(item.c0.decision_time_ms for item in aligned)
    start_ms = (
        minimum_time // _DAY_MS * _DAY_MS
        if evaluation_start_ms is None
        else evaluation_start_ms
    )
    end_ms = (
        (maximum_time // _DAY_MS + 1) * _DAY_MS
        if evaluation_end_ms is None
        else evaluation_end_ms
    )
    if start_ms < 0 or end_ms <= start_ms:
        raise ValueError("evaluation window must be non-negative and ordered")
    if start_ms % _DAY_MS or end_ms % _DAY_MS:
        raise ValueError("evaluation window must use UTC-midnight boundaries")
    day_count = (end_ms - start_ms) // _DAY_MS
    if block_days > day_count:
        raise ValueError("block_days must not exceed evaluation calendar days")
    side_rows: dict[tuple[str, str], list[AlignedOpportunity]] = {}
    summary_rows: list[dict[str, Any]] = []
    point_contributions: dict[tuple[str, str, str], float] = {}

    for market, direction in _OPPORTUNITY_SIDES:
        all_side = [
            item
            for item in aligned
            if item.c0.market == market
            and item.c0.direction == direction
            and start_ms <= item.c0.decision_time_ms < end_ms
        ]
        common = [item for item in all_side if item.c0.analysis_eligible]
        if not common:
            raise ValueError(f"no common analyzable opportunities for {market}-{direction}")
        side_rows[(market, direction)] = common
        for variant in _OPPORTUNITY_VARIANTS:
            observations = [item.observation(variant) for item in common]
            all_observations = [item.observation(variant) for item in all_side]
            eligible_outcomes = [
                item.forward_return_12
                for item in observations
                if item.eligible and item.forward_return_12 is not None
            ]
            contribution = sum(
                item.forward_return_12
                if item.eligible and item.forward_return_12 is not None
                else 0.0
                for item in observations
            ) / len(observations)
            point_contributions[(variant, market, direction)] = contribution
            summary_rows.append(
                {
                    "variant": variant,
                    "market": market,
                    "direction": direction,
                    "common_opportunities": len(observations),
                    "available_opportunities": sum(
                        item.volume_feature_available for item in observations
                    ),
                    "availability_rate": sum(
                        item.volume_feature_available for item in observations
                    )
                    / len(observations),
                    "eligible_opportunities": len(eligible_outcomes),
                    "conditional_h12_mean": (
                        sum(eligible_outcomes) / len(eligible_outcomes)
                        if eligible_outcomes
                        else None
                    ),
                    "unconditional_contribution": contribution,
                    "conditional_h3_mean": _conditional_mean(
                        all_observations, horizon=3
                    ),
                    "conditional_h72_mean": _conditional_mean(
                        all_observations, horizon=72
                    ),
                }
            )

    counts: dict[tuple[str, str], list[int]] = {
        side: [0] * day_count for side in _OPPORTUNITY_SIDES
    }
    contribution_sums: dict[tuple[str, str, str], list[float]] = {
        (variant, market, direction): [0.0] * day_count
        for variant in _OPPORTUNITY_VARIANTS
        for market, direction in _OPPORTUNITY_SIDES
    }
    for (market, direction), values in side_rows.items():
        for item in values:
            index = (item.c0.decision_time_ms - start_ms) // _DAY_MS
            counts[(market, direction)][index] += 1
            for variant in _OPPORTUNITY_VARIANTS:
                observation = item.observation(variant)
                if observation.eligible and observation.forward_return_12 is not None:
                    contribution_sums[(variant, market, direction)][index] += (
                        observation.forward_return_12
                    )

    distributions: dict[tuple[str, str, str], list[float]] = {
        (name, market, direction): []
        for name, _, _ in _OPPORTUNITY_CONTRASTS
        for market, direction in _OPPORTUNITY_SIDES
    }
    rng = random.Random(seed)
    full_blocks, remainder_days = divmod(day_count, block_days)
    block_lengths = [block_days] * full_blocks
    if remainder_days:
        block_lengths.append(remainder_days)
    unique_lengths = set(block_lengths)
    count_windows = {
        (side, length): _rolling_sums(values, length)
        for side, values in counts.items()
        for length in unique_lengths
    }
    contribution_windows = {
        (variant, market, direction, length): _rolling_sums(values, length)
        for (variant, market, direction), values in contribution_sums.items()
        for length in unique_lengths
    }
    for _ in range(samples):
        starts = [rng.randrange(day_count - length + 1) for length in block_lengths]
        draw_values: dict[tuple[str, str, str], float] = {}
        for market, direction in _OPPORTUNITY_SIDES:
            denominator = sum(
                count_windows[((market, direction), length)][start]
                for length, start in zip(block_lengths, starts, strict=True)
            )
            for variant in _OPPORTUNITY_VARIANTS:
                numerator = sum(
                    contribution_windows[
                        (variant, market, direction, length)
                    ][start]
                    for length, start in zip(block_lengths, starts, strict=True)
                )
                draw_values[(variant, market, direction)] = (
                    numerator / denominator if denominator else 0.0
                )
        for name, upper, lower in _OPPORTUNITY_CONTRASTS:
            for market, direction in _OPPORTUNITY_SIDES:
                upper_value = draw_values[(upper, market, direction)]
                lower_value = (
                    0.0 if lower is None else draw_values[(lower, market, direction)]
                )
                distributions[(name, market, direction)].append(
                    upper_value - lower_value
                )

    family_tests = len(_OPPORTUNITY_CONTRASTS) * len(_OPPORTUNITY_SIDES)
    family_alpha = 0.05 / family_tests
    contrast_rows: list[dict[str, Any]] = []
    for name, upper, lower in _OPPORTUNITY_CONTRASTS:
        for market, direction in _OPPORTUNITY_SIDES:
            upper_value = point_contributions[(upper, market, direction)]
            lower_value = (
                0.0
                if lower is None
                else point_contributions[(lower, market, direction)]
            )
            distribution = sorted(distributions[(name, market, direction)])
            contrast_rows.append(
                {
                    "contrast": name,
                    "market": market,
                    "direction": direction,
                    "effect": upper_value - lower_value,
                    "ci_95_low": _quantile(distribution, 0.025),
                    "ci_95_high": _quantile(distribution, 0.975),
                    "simultaneous_one_sided_low": _quantile(
                        distribution, family_alpha
                    ),
                    "probability_positive": sum(value > 0 for value in distribution)
                    / len(distribution),
                }
            )

    input_counts = {variant: len(panels[variant]) for variant in _OPPORTUNITY_VARIANTS}
    return {
        "method": "common-opportunity unconditional contribution moving UTC-day block bootstrap",
        "estimand": "mean(eligible * forward_return_12) on common opportunity IDs",
        "block_days": block_days,
        "evaluation_start_ms": start_ms,
        "evaluation_end_ms": end_ms,
        "calendar_days": day_count,
        "blocks_per_draw": len(block_lengths),
        "samples": samples,
        "seed": seed,
        "family_tests": family_tests,
        "simultaneous_method": "Bonferroni one-sided percentile lower bounds",
        "input_opportunities": input_counts,
        "common_opportunity_ids": len(aligned),
        "excluded_noncommon_ids": {variant: 0 for variant in _OPPORTUNITY_VARIANTS},
        "summary_rows": summary_rows,
        "rows": contrast_rows,
    }


def _rolling_sums(values: Sequence[float] | Sequence[int], length: int) -> list[float]:
    """Return every non-wrapping contiguous calendar-window sum."""

    if length <= 0 or length > len(values):
        raise ValueError("rolling-sum length must fit the input")
    running = float(sum(values[:length]))
    output = [running]
    for index in range(length, len(values)):
        running += float(values[index]) - float(values[index - length])
        output.append(running)
    return output


def _conditional_mean(
    observations: Sequence[OpportunityObservation], *, horizon: int
) -> float | None:
    if horizon == 3:
        values = [
            item.forward_return_3
            for item in observations
            if item.eligible
            and item.analysis_eligible_3
            and item.forward_return_3 is not None
        ]
    elif horizon == 72:
        values = [
            item.forward_return_72
            for item in observations
            if item.eligible
            and item.analysis_eligible_72
            and item.forward_return_72 is not None
        ]
    else:
        raise ValueError("secondary conditional mean supports only h3 or h72")
    return sum(values) / len(values) if values else None
