from __future__ import annotations

import csv
import math
from collections.abc import Mapping, Sequence
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

_VARIANTS = ("C0", "G2", "G4")
_FEATURE_VARIANTS = ("G2", "G4")
_SIDES = (("spot", "long"), ("futures", "short"))

_AVAILABILITY_THRESHOLD = 0.95
_MEAN_NET_RETURN_THRESHOLD = 0.0005
_PROFIT_FACTOR_THRESHOLD = 1.05
_CONCENTRATION_THRESHOLD = 0.35
_POSITIVE_ASSET_THRESHOLD = 6


@dataclass(frozen=True, slots=True)
class VerdictOpportunity:
    opportunity_id: str
    asset: str
    market: str
    direction: str
    decision_time_ms: int
    eligible: bool
    analysis_eligible_12: bool
    volume_feature_available: bool
    forward_return_12: float | None


@dataclass(frozen=True, slots=True)
class VerdictTrade:
    trade_id: str
    asset: str
    market: str
    direction: str
    net_return: float
    net_pnl_usdt: float


@dataclass(frozen=True, slots=True)
class CriterionResult:
    rule: int
    criterion: str
    value: float | int | bool | None
    operator: str
    threshold: float | int | bool
    passed: bool
    detail: str


@dataclass(frozen=True, slots=True)
class AssetContribution:
    asset: str
    additive_h12_contribution: float


@dataclass(frozen=True, slots=True)
class FeatureDirectionVerdict:
    variant: str
    market: str
    direction: str
    criteria: tuple[CriterionResult, ...]
    asset_contributions: tuple[AssetContribution, ...]
    overall_pass: bool
    fail_reasons: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class ExperimentVerdict:
    hypotheses: tuple[FeatureDirectionVerdict, ...]
    determinism_parity_passed: bool
    overall_pass: bool
    fail_reasons: tuple[str, ...]

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-safe representation; undefined profit factor remains ``None``."""

        return asdict(self)


def _strict_bool(value: str | None, field: str) -> bool:
    normalized = "" if value is None else value.strip().lower()
    if normalized in {"true", "1"}:
        return True
    if normalized in {"false", "0"}:
        return False
    raise ValueError(f"{field} must be true or false")


def _finite_float(value: object, field: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (str, int, float)):
        raise ValueError(f"{field} must be a finite number")
    try:
        parsed = float(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{field} must be a finite number") from exc
    if not math.isfinite(parsed):
        raise ValueError(f"{field} must be a finite number")
    return parsed


def _optional_finite_float(value: str | None, field: str) -> float | None:
    if value is None or not value.strip():
        return None
    return _finite_float(value, field)


def _analysis_eligible_12(row: Mapping[str, str | None]) -> bool:
    current = row.get("analysis_eligible_12")
    alias = row.get("analysis_eligible")
    if current is None or not current.strip():
        if alias is None:
            raise ValueError("analysis_eligible_12 is required")
        return _strict_bool(alias, "analysis_eligible")
    parsed = _strict_bool(current, "analysis_eligible_12")
    if alias is not None and alias.strip():
        if _strict_bool(alias, "analysis_eligible") != parsed:
            raise ValueError("analysis_eligible must alias analysis_eligible_12")
    return parsed


def read_verdict_opportunities(path: str | Path) -> tuple[VerdictOpportunity, ...]:
    """Read and validate the R1 verdict subset of an ``opportunities.csv`` file."""

    output: list[VerdictOpportunity] = []
    with Path(path).open("r", encoding="utf-8", newline="") as handle:
        for line_number, row in enumerate(csv.DictReader(handle), start=2):
            try:
                opportunity_id = str(row["opportunity_id"]).strip()
                asset = str(row["asset"]).strip().upper()
                if not opportunity_id or not asset:
                    raise ValueError("opportunity_id and asset must not be empty")
                decision_time_ms = int(row["decision_time_ms"])
                if decision_time_ms < 0:
                    raise ValueError("decision_time_ms must be non-negative")
                output.append(
                    VerdictOpportunity(
                        opportunity_id=opportunity_id,
                        asset=asset,
                        market=str(row["market"]).strip().lower(),
                        direction=str(row["direction"]).strip().lower(),
                        decision_time_ms=decision_time_ms,
                        eligible=_strict_bool(row.get("eligible"), "eligible"),
                        analysis_eligible_12=_analysis_eligible_12(row),
                        volume_feature_available=_strict_bool(
                            row.get("volume_feature_available"),
                            "volume_feature_available",
                        ),
                        forward_return_12=_optional_finite_float(
                            row.get("forward_return_12"), "forward_return_12"
                        ),
                    )
                )
            except (KeyError, TypeError, ValueError) as exc:
                raise ValueError(
                    f"invalid verdict opportunity on CSV line {line_number}"
                ) from exc
    return tuple(output)


def read_verdict_trades(path: str | Path) -> tuple[VerdictTrade, ...]:
    """Read and validate the R1 verdict subset of a ``trades.csv`` file."""

    output: list[VerdictTrade] = []
    with Path(path).open("r", encoding="utf-8", newline="") as handle:
        for line_number, row in enumerate(csv.DictReader(handle), start=2):
            try:
                trade_id = str(row["trade_id"]).strip()
                asset = str(row["asset"]).strip().upper()
                if not trade_id or not asset:
                    raise ValueError("trade_id and asset must not be empty")
                output.append(
                    VerdictTrade(
                        trade_id=trade_id,
                        asset=asset,
                        market=str(row["market"]).strip().lower(),
                        direction=str(row["direction"]).strip().lower(),
                        net_return=_finite_float(row.get("net_return"), "net_return"),
                        net_pnl_usdt=_finite_float(
                            row.get("net_pnl_usdt"), "net_pnl_usdt"
                        ),
                    )
                )
            except (KeyError, TypeError, ValueError) as exc:
                raise ValueError(f"invalid verdict trade on CSV line {line_number}") from exc
    return tuple(output)


def _validate_side(market: str, direction: str, identity: str) -> None:
    if (market, direction) not in _SIDES:
        raise ValueError(f"{identity} has unsupported R1 side {market}-{direction}")


def _opportunity_indexes(
    panels: Mapping[str, Sequence[VerdictOpportunity]],
) -> dict[str, dict[str, VerdictOpportunity]]:
    if set(panels) != set(_VARIANTS) or len(panels) != len(_VARIANTS):
        raise ValueError(f"opportunity panels must contain exactly {_VARIANTS}")
    indexes: dict[str, dict[str, VerdictOpportunity]] = {}
    for variant in _VARIANTS:
        indexed: dict[str, VerdictOpportunity] = {}
        for item in panels[variant]:
            identity = f"{variant}:{item.opportunity_id}"
            if not item.opportunity_id or not item.asset:
                raise ValueError(f"{identity} has an empty ID or asset")
            if item.decision_time_ms < 0:
                raise ValueError(f"{identity} has a negative decision time")
            if not isinstance(item.eligible, bool) or not isinstance(
                item.analysis_eligible_12, bool
            ):
                raise ValueError(f"{identity} has a non-boolean eligibility field")
            if not isinstance(item.volume_feature_available, bool):
                raise ValueError(f"{identity} has non-boolean feature availability")
            _validate_side(item.market, item.direction, identity)
            if item.forward_return_12 is not None and not math.isfinite(
                item.forward_return_12
            ):
                raise ValueError(f"{identity} has a non-finite h12 outcome")
            if item.analysis_eligible_12 != (item.forward_return_12 is not None):
                raise ValueError(f"{identity} has inconsistent h12 availability")
            if item.opportunity_id in indexed:
                raise ValueError(f"{variant} has duplicate opportunity_id {item.opportunity_id}")
            indexed[item.opportunity_id] = item
        indexes[variant] = indexed

    base_ids = set(indexes["C0"])
    for variant in _FEATURE_VARIANTS:
        if set(indexes[variant]) != base_ids:
            raise ValueError(f"{variant} opportunity_id set differs from C0")
    for opportunity_id in sorted(base_ids):
        values = tuple(indexes[variant][opportunity_id] for variant in _VARIANTS)
        first = values[0]
        identity = (
            first.asset.upper(),
            first.market,
            first.direction,
            first.decision_time_ms,
        )
        if any(
            (item.asset.upper(), item.market, item.direction, item.decision_time_ms)
            != identity
            for item in values[1:]
        ):
            raise ValueError(f"opportunity identity mismatch for {opportunity_id}")
        if any(
            item.analysis_eligible_12 != first.analysis_eligible_12
            for item in values[1:]
        ):
            raise ValueError(f"h12 eligibility mismatch for {opportunity_id}")
        if any(item.forward_return_12 != first.forward_return_12 for item in values[1:]):
            raise ValueError(f"future outcome mismatch for {opportunity_id}")
    return indexes


def _validated_trades(
    runs: Mapping[str, Sequence[VerdictTrade]],
) -> dict[str, tuple[VerdictTrade, ...]]:
    if set(runs) != set(_VARIANTS) or len(runs) != len(_VARIANTS):
        raise ValueError(f"trade runs must contain exactly {_VARIANTS}")
    output: dict[str, tuple[VerdictTrade, ...]] = {}
    for variant in _VARIANTS:
        seen: set[str] = set()
        values: list[VerdictTrade] = []
        for item in runs[variant]:
            identity = f"{variant}:{item.trade_id}"
            if not item.trade_id or not item.asset:
                raise ValueError(f"{identity} has an empty ID or asset")
            _validate_side(item.market, item.direction, identity)
            if not math.isfinite(item.net_return) or not math.isfinite(item.net_pnl_usdt):
                raise ValueError(f"{identity} has a non-finite result")
            if item.trade_id in seen:
                raise ValueError(f"{variant} has duplicate trade_id {item.trade_id}")
            seen.add(item.trade_id)
            values.append(item)
        output[variant] = tuple(values)
    return output


def _comparison_lower_bound(
    comparison: Mapping[str, Any], variant: str, market: str, direction: str
) -> float:
    rows = comparison.get("rows")
    if not isinstance(rows, list):
        raise ValueError("comparison rows must be a list")
    contrast = f"{variant}-C0"
    matches = [
        row
        for row in rows
        if isinstance(row, Mapping)
        and row.get("contrast") == contrast
        and row.get("market") == market
        and row.get("direction") == direction
    ]
    if len(matches) != 1:
        raise ValueError(
            f"comparison must contain exactly one {contrast} {market}-{direction} row"
        )
    return _finite_float(
        matches[0].get("simultaneous_one_sided_low"),
        "simultaneous_one_sided_low",
    )


def _criterion(
    rule: int,
    name: str,
    value: float | int | bool | None,
    operator: str,
    threshold: float | int | bool,
    passed: bool,
    detail: str,
) -> CriterionResult:
    return CriterionResult(rule, name, value, operator, threshold, passed, detail)


def _profit_factor(values: Sequence[VerdictTrade]) -> tuple[float | None, bool, str]:
    wins = sum(item.net_return for item in values if item.net_return > 0)
    losses = -sum(item.net_return for item in values if item.net_return < 0)
    if losses > 0:
        value = wins / losses
        return (
            value,
            value > _PROFIT_FACTOR_THRESHOLD,
            f"profit factor {value:.12g} must be > {_PROFIT_FACTOR_THRESHOLD}",
        )
    if wins > 0:
        return (
            None,
            True,
            "profit factor is mathematically +infinity because wins are positive "
            "and losses are zero; JSON value is None",
        )
    return (
        None,
        False,
        "profit factor is undefined because there are no positive wins and no gross losses",
    )


def evaluate_r1_verdict(
    opportunities: Mapping[str, Sequence[VerdictOpportunity]],
    trades: Mapping[str, Sequence[VerdictTrade]],
    comparison: Mapping[str, Any],
    *,
    determinism_parity_passed: bool,
) -> ExperimentVerdict:
    """Apply frozen R1 stop/reject rules 1-6 without tuning or fallback."""

    if not isinstance(determinism_parity_passed, bool):
        raise ValueError("determinism_parity_passed must be boolean")
    opportunity_indexes = _opportunity_indexes(opportunities)
    trade_runs = _validated_trades(trades)
    hypotheses: list[FeatureDirectionVerdict] = []

    for variant in _FEATURE_VARIANTS:
        for market, direction in _SIDES:
            base = [
                item
                for item in opportunity_indexes["C0"].values()
                if item.market == market
                and item.direction == direction
                and item.analysis_eligible_12
            ]
            variant_by_id = opportunity_indexes[variant]
            available_count = sum(
                variant_by_id[item.opportunity_id].volume_feature_available
                for item in base
            )
            availability = available_count / len(base) if base else None

            contributions: dict[str, float] = {}
            for base_item in base:
                variant_item = variant_by_id[base_item.opportunity_id]
                outcome = base_item.forward_return_12
                if outcome is None:  # guarded by _opportunity_indexes
                    raise AssertionError("analysis-eligible h12 outcome unexpectedly absent")
                delta = (int(variant_item.eligible) - int(base_item.eligible)) * outcome
                asset = base_item.asset.upper()
                contributions[asset] = contributions.get(asset, 0.0) + delta / len(base)
            asset_rows = tuple(
                AssetContribution(asset, contributions[asset])
                for asset in sorted(contributions)
            )
            positives = [value for value in contributions.values() if value > 0]
            concentration = max(positives) / sum(positives) if positives else None
            positive_asset_count = len(positives)

            side_trades = [
                item
                for item in trade_runs[variant]
                if item.market == market and item.direction == direction
            ]
            total_net_pnl = sum(item.net_pnl_usdt for item in side_trades)
            mean_net_return = (
                sum(item.net_return for item in side_trades) / len(side_trades)
                if side_trades
                else None
            )
            profit_factor, profit_factor_passed, profit_factor_detail = _profit_factor(
                side_trades
            )
            lower_bound = _comparison_lower_bound(
                comparison, variant, market, direction
            )

            criteria = (
                _criterion(
                    1,
                    "feature_availability",
                    availability,
                    ">=",
                    _AVAILABILITY_THRESHOLD,
                    availability is not None
                    and availability >= _AVAILABILITY_THRESHOLD,
                    (
                        f"availability {availability:.12g} "
                        f"({available_count}/{len(base)}) must be >= "
                        f"{_AVAILABILITY_THRESHOLD}"
                        if availability is not None
                        else "availability is undefined because there are no valid "
                        "C0 h12 opportunities"
                    ),
                ),
                _criterion(
                    2,
                    "simultaneous_one_sided_low",
                    lower_bound,
                    ">",
                    0.0,
                    lower_bound > 0,
                    f"multiplicity-adjusted one-sided lower bound {lower_bound:.12g} must be > 0",
                ),
                _criterion(
                    3,
                    "total_net_pnl_usdt",
                    total_net_pnl,
                    ">",
                    0.0,
                    total_net_pnl > 0,
                    f"total net P&L {total_net_pnl:.12g} over "
                    f"{len(side_trades)} trades must be > 0",
                ),
                _criterion(
                    3,
                    "mean_net_return",
                    mean_net_return,
                    ">=",
                    _MEAN_NET_RETURN_THRESHOLD,
                    mean_net_return is not None
                    and mean_net_return >= _MEAN_NET_RETURN_THRESHOLD,
                    (
                        f"mean net return {mean_net_return:.12g} must be >= "
                        f"{_MEAN_NET_RETURN_THRESHOLD}"
                        if mean_net_return is not None
                        else "mean net return is undefined because there are zero executed trades"
                    ),
                ),
                _criterion(
                    4,
                    "profit_factor",
                    profit_factor,
                    ">",
                    _PROFIT_FACTOR_THRESHOLD,
                    profit_factor_passed,
                    profit_factor_detail,
                ),
                _criterion(
                    5,
                    "positive_contribution_concentration",
                    concentration,
                    "<=",
                    _CONCENTRATION_THRESHOLD,
                    concentration is not None
                    and concentration <= _CONCENTRATION_THRESHOLD,
                    (
                        f"largest positive-asset concentration {concentration:.12g} "
                        f"must be <= {_CONCENTRATION_THRESHOLD}"
                        if concentration is not None
                        else "positive-contribution concentration is undefined "
                        "because no asset contribution is positive"
                    ),
                ),
                _criterion(
                    5,
                    "positive_asset_count",
                    positive_asset_count,
                    ">=",
                    _POSITIVE_ASSET_THRESHOLD,
                    positive_asset_count >= _POSITIVE_ASSET_THRESHOLD,
                    f"positive asset count {positive_asset_count} must be >= "
                    f"{_POSITIVE_ASSET_THRESHOLD}",
                ),
                _criterion(
                    6,
                    "determinism_and_parity",
                    determinism_parity_passed,
                    "is",
                    True,
                    determinism_parity_passed,
                    (
                        "deterministic replay and parity checks passed"
                        if determinism_parity_passed
                        else "deterministic replay, leakage, or parity checks failed"
                    ),
                ),
            )
            fail_reasons = tuple(
                f"rule {item.rule} {item.criterion}: {item.detail}"
                for item in criteria
                if not item.passed
            )
            hypotheses.append(
                FeatureDirectionVerdict(
                    variant=variant,
                    market=market,
                    direction=direction,
                    criteria=criteria,
                    asset_contributions=asset_rows,
                    overall_pass=not fail_reasons,
                    fail_reasons=fail_reasons,
                )
            )

    experiment_failures = tuple(
        f"{item.variant} {item.market}-{item.direction}: {reason}"
        for item in hypotheses
        for reason in item.fail_reasons
    )
    return ExperimentVerdict(
        hypotheses=tuple(hypotheses),
        determinism_parity_passed=determinism_parity_passed,
        overall_pass=not experiment_failures,
        fail_reasons=experiment_failures,
    )
