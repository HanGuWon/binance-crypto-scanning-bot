from __future__ import annotations

import hashlib
import math
import statistics
from bisect import bisect_left, bisect_right
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import StrEnum
from itertools import pairwise
from pathlib import Path
from typing import Literal

import yaml
from pydantic import field_validator, model_validator

from signalbot.backtest.config import (
    BacktestAsset,
    BacktestSplit,
    CostSettings,
)
from signalbot.backtest.dataset import KlineDataset
from signalbot.backtest.engine import (
    FundingRate,
    calculate_execution_returns,
    calculate_funding_return,
)
from signalbot.backtest.funding import FundingDataset
from signalbot.config import StrictModel
from signalbot.data.candles import interval_to_milliseconds
from signalbot.domain.enums import Direction, Market
from signalbot.domain.models import Candle

_DAY_MS = 86_400_000
_HOUR_MS = 3_600_000
_FROZEN_INTERVAL = "5m"
_FROZEN_PROTOCOL_VERSION = "c1_exposed_funding_basis_carry_v1"
_FROZEN_RULE_VERSION = "v3.3.0-c1-funding-basis-carry"
_FROZEN_PLAN_PATH = "artifacts/backtest/2026-07-17-c1/experiment_plan.md"
_FROZEN_DATES = (
    datetime(2024, 3, 1, tzinfo=UTC),
    datetime(2024, 7, 1, tzinfo=UTC),
    datetime(2026, 7, 1, tzinfo=UTC),
)
_FROZEN_ASSETS = (
    ("BTC", "anchor", "BTCUSDT", "BTCUSDT"),
    ("ETH", "anchor", "ETHUSDT", "ETHUSDT"),
    ("BNB", "major", "BNBUSDT", "BNBUSDT"),
    ("SOL", "major", "SOLUSDT", "SOLUSDT"),
    ("XRP", "major", "XRPUSDT", "XRPUSDT"),
    ("DOGE", "major", "DOGEUSDT", "DOGEUSDT"),
    ("SUI", "volatile", "SUIUSDT", "SUIUSDT"),
    ("WIF", "volatile", "WIFUSDT", "WIFUSDT"),
)
_FROZEN_SPLITS = (
    (
        "development",
        datetime(2024, 7, 1, tzinfo=UTC),
        datetime(2025, 3, 1, tzinfo=UTC),
    ),
    (
        "validation",
        datetime(2025, 3, 1, tzinfo=UTC),
        datetime(2025, 11, 1, tzinfo=UTC),
    ),
    (
        "retrospective_test",
        datetime(2025, 11, 1, tzinfo=UTC),
        datetime(2026, 7, 1, tzinfo=UTC),
    ),
)
_FROZEN_SPOT_SLIPPAGE_BPS = {"anchor": 5.0, "major": 5.0, "volatile": 10.0}
_FROZEN_FUTURES_SLIPPAGE_BPS = {"anchor": 3.0, "major": 3.0, "volatile": 8.0}


class CarryPolicySettings(StrictModel):
    """Frozen C1 funding/basis carry policy.

    The validator intentionally rejects parameter drift. A changed value is a new
    experiment, not another run of C1.
    """

    basis_lookback_bars: int = 8_640
    funding_lookback_days: int = 30
    funding_minimum_events: int = 60
    entry_basis_quantile: float = 0.90
    funding_floor_quantile: float = 0.25
    minimum_positive_funding_fraction: float = 0.75
    expected_convergence_fraction: float = 0.50
    edge_margin_bps: float = 10.0
    cooldown_hours: int = 24
    maximum_holding_days: int = 7
    split_edge_purge_days: int = 7
    stop_floor_bps: float = 50.0
    stop_mad_multiple: float = 3.0
    mad_consistency_scale: float = 1.4826

    @model_validator(mode="after")
    def require_frozen_c1_values(self) -> CarryPolicySettings:
        expected: dict[str, int | float] = {
            "basis_lookback_bars": 8_640,
            "funding_lookback_days": 30,
            "funding_minimum_events": 60,
            "entry_basis_quantile": 0.90,
            "funding_floor_quantile": 0.25,
            "minimum_positive_funding_fraction": 0.75,
            "expected_convergence_fraction": 0.50,
            "edge_margin_bps": 10.0,
            "cooldown_hours": 24,
            "maximum_holding_days": 7,
            "split_edge_purge_days": 7,
            "stop_floor_bps": 50.0,
            "stop_mad_multiple": 3.0,
            "mad_consistency_scale": 1.4826,
        }
        for field_name, frozen_value in expected.items():
            if getattr(self, field_name) != frozen_value:
                raise ValueError(f"{field_name} is frozen at {frozen_value} for C1")
        return self


class CarryAcceptanceSettings(StrictModel):
    minimum_completed_episodes: int = 100
    minimum_2x_slippage_mean_pair_return_bps: float = 10.0
    bootstrap_block_days: int = 7
    bootstrap_samples: int = 50_000
    bootstrap_seed: int = 20_260_717
    one_sided_confidence: float = 0.95
    maximum_invalid_bootstrap_fraction: float = 0.001
    bootstrap_pnl_attribution: Literal["entry_decision_utc_day"] = (
        "entry_decision_utc_day"
    )
    include_zero_event_days: bool = True
    primary_calendar_start: datetime = datetime(2025, 3, 1, tzinfo=UTC)
    primary_calendar_end: datetime = datetime(2026, 7, 1, tzinfo=UTC)
    minimum_2x_slippage_profit_factor: float = 1.25
    required_positive_splits: tuple[str, ...] = ("validation", "retrospective_test")
    maximum_asset_positive_pnl_share: float = 0.50
    maximum_outcome_unobservable: int = 0
    prospective_minimum_months: int = 6

    @field_validator("primary_calendar_start", "primary_calendar_end")
    @classmethod
    def normalize_calendar_utc(cls, value: datetime) -> datetime:
        if value.tzinfo is None:
            raise ValueError("primary calendar timestamps must include a UTC offset")
        return value.astimezone(UTC)

    @model_validator(mode="after")
    def require_frozen_c1_values(self) -> CarryAcceptanceSettings:
        expected: dict[
            str, int | float | bool | str | tuple[str, ...] | datetime
        ] = {
            "minimum_completed_episodes": 100,
            "minimum_2x_slippage_mean_pair_return_bps": 10.0,
            "bootstrap_block_days": 7,
            "bootstrap_samples": 50_000,
            "bootstrap_seed": 20_260_717,
            "one_sided_confidence": 0.95,
            "maximum_invalid_bootstrap_fraction": 0.001,
            "bootstrap_pnl_attribution": "entry_decision_utc_day",
            "include_zero_event_days": True,
            "primary_calendar_start": datetime(2025, 3, 1, tzinfo=UTC),
            "primary_calendar_end": datetime(2026, 7, 1, tzinfo=UTC),
            "minimum_2x_slippage_profit_factor": 1.25,
            "required_positive_splits": ("validation", "retrospective_test"),
            "maximum_asset_positive_pnl_share": 0.50,
            "maximum_outcome_unobservable": 0,
            "prospective_minimum_months": 6,
        }
        for field_name, frozen_value in expected.items():
            if getattr(self, field_name) != frozen_value:
                raise ValueError(f"{field_name} is frozen at {frozen_value} for C1")
        return self


class CarryExperimentSpec(StrictModel):
    protocol_version: str
    rule_version: str
    experiment_plan_path: str
    interval: str = _FROZEN_INTERVAL
    data_start: datetime
    evaluation_start: datetime
    evaluation_end: datetime
    assets: list[BacktestAsset]
    splits: list[BacktestSplit]
    carry: CarryPolicySettings = CarryPolicySettings()
    costs: CostSettings = CostSettings()
    acceptance: CarryAcceptanceSettings = CarryAcceptanceSettings()

    @field_validator("data_start", "evaluation_start", "evaluation_end")
    @classmethod
    def normalize_utc(cls, value: datetime) -> datetime:
        if value.tzinfo is None:
            raise ValueError("study timestamps must include a UTC offset")
        return value.astimezone(UTC)

    @field_validator("interval")
    @classmethod
    def require_five_minute_interval(cls, value: str) -> str:
        if value != _FROZEN_INTERVAL:
            raise ValueError("C1 is frozen to closed 5m candles")
        return value

    @model_validator(mode="after")
    def validate_contract(self) -> CarryExperimentSpec:
        identity = (
            self.protocol_version,
            self.rule_version,
            self.experiment_plan_path,
        )
        if identity != (
            _FROZEN_PROTOCOL_VERSION,
            _FROZEN_RULE_VERSION,
            _FROZEN_PLAN_PATH,
        ):
            raise ValueError("C1 protocol, rule, and experiment-plan identity are frozen")
        if not self.data_start < self.evaluation_start < self.evaluation_end:
            raise ValueError("expected data_start < evaluation_start < evaluation_end")
        if not self.assets:
            raise ValueError("at least one asset is required")
        if not self.splits:
            raise ValueError("at least one split is required")
        if not self.costs.include_funding:
            raise ValueError("C1 requires settled funding P&L")
        if (self.data_start, self.evaluation_start, self.evaluation_end) != _FROZEN_DATES:
            raise ValueError("C1 data and evaluation dates are frozen")
        assets = tuple(
            (item.asset, item.cohort, item.spot_symbol, item.futures_symbol)
            for item in self.assets
        )
        if assets != _FROZEN_ASSETS:
            raise ValueError("C1 requires the exact ordered eight-asset Spot/perpetual panel")
        splits = tuple((item.name, item.start, item.end) for item in self.splits)
        if splits != _FROZEN_SPLITS:
            raise ValueError("C1 requires the exact frozen chronological splits")
        _validate_frozen_costs(self.costs)
        ordered = sorted(self.splits, key=lambda item: item.start)
        if ordered != self.splits:
            raise ValueError("splits must be ordered by start time")
        previous_end: datetime | None = None
        for split in self.splits:
            if split.start < self.evaluation_start or split.end > self.evaluation_end:
                raise ValueError("every split must be inside the evaluation window")
            if previous_end is not None and split.start < previous_end:
                raise ValueError("splits must not overlap")
            previous_end = split.end
        return self


def load_carry_spec(path: str | Path) -> CarryExperimentSpec:
    """Load the strict, research-only C1 YAML contract."""

    with Path(path).open("r", encoding="utf-8") as handle:
        raw = yaml.safe_load(handle) or {}
    if not isinstance(raw, dict):
        raise ValueError("carry configuration root must be a mapping")
    return CarryExperimentSpec.model_validate(raw)


class CarryExitReason(StrEnum):
    STOP = "STOP"
    FUNDING_FLIP = "FUNDING_FLIP"
    CONVERGENCE = "CONVERGENCE"
    TIME = "TIME"
    DATA_GAP = "DATA_GAP"


@dataclass(frozen=True, slots=True)
class CarryEntryDecision:
    decision_id: str
    protocol_version: str
    rule_version: str
    asset: str
    cohort: str
    split: str
    decision_time_ms: int
    entry_time_ms: int | None
    triggering_funding_time_ms: int
    basis: float
    basis_median: float | None
    basis_q90: float | None
    basis_mad: float | None
    funding_last: float | None
    funding_q25: float | None
    positive_funding_fraction: float | None
    funding_cadence_ms: float | None
    expected_funding_events: int | None
    expected_pair_edge: float | None
    stress_cost_hurdle: float | None
    target_basis: float | None
    stop_basis: float | None
    accepted: bool
    rejection_reasons: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class CarryTrade:
    trade_id: str
    decision_id: str
    protocol_version: str
    rule_version: str
    asset: str
    cohort: str
    split: str
    triggering_funding_time_ms: int
    entry_decision_time_ms: int
    exit_decision_time_ms: int
    entry_time_ms: int
    exit_time_ms: int
    exit_reason: str
    target_basis: float
    stop_basis: float
    entry_signal_basis: float
    entry_fill_basis: float
    exit_signal_basis: float
    base_quantity: float
    pair_capital_usdt: float
    spot_entry_price: float
    spot_exit_price: float
    futures_entry_price: float
    futures_exit_price: float
    gross_pnl_usdt: float
    slippage_usdt: float
    fees_usdt: float
    funding_pnl_usdt: float
    net_pnl_usdt: float
    gross_return: float
    net_return: float
    funding_event_count: int
    analysis_eligible: bool
    exclusion_reason: str


@dataclass(frozen=True, slots=True)
class CarryRun:
    decisions: tuple[CarryEntryDecision, ...]
    trades: tuple[CarryTrade, ...]
    common_bar_count: int
    gap_count: int
    open_position_at_end: bool


@dataclass(frozen=True, slots=True)
class _CommonBar:
    spot: Candle
    futures: Candle
    basis: float


@dataclass(slots=True)
class _Position:
    decision: CarryEntryDecision
    entry_time_ms: int
    spot_entry_price: float
    futures_entry_price: float
    entry_fill_basis: float
    last_observed_funding_time_ms: int


def _quantile(values: list[float], probability: float) -> float:
    """Return the deterministic type-7 sample quantile used by C1."""

    if not values:
        raise ValueError("quantile requires at least one value")
    if not 0 <= probability <= 1:
        raise ValueError("quantile probability must be between zero and one")
    ordered = sorted(values)
    position = (len(ordered) - 1) * probability
    lower = math.floor(position)
    upper = math.ceil(position)
    if lower == upper:
        return ordered[lower]
    weight = position - lower
    return ordered[lower] * (1 - weight) + ordered[upper] * weight


def _basis(spot_price: float, futures_price: float) -> float:
    if not all(math.isfinite(value) and value > 0 for value in (spot_price, futures_price)):
        raise ValueError("basis prices must be finite and positive")
    return (futures_price - spot_price) / spot_price


def _align_common_bars(spot: KlineDataset, futures: KlineDataset) -> list[_CommonBar]:
    futures_by_open = {item.open_time_ms: item for item in futures.candles}
    common: list[_CommonBar] = []
    for spot_candle in spot.candles:
        futures_candle = futures_by_open.get(spot_candle.open_time_ms)
        if futures_candle is None or futures_candle.close_time_ms != spot_candle.close_time_ms:
            continue
        common.append(
            _CommonBar(
                spot=spot_candle,
                futures=futures_candle,
                basis=_basis(float(spot_candle.close), float(futures_candle.close)),
            )
        )
    return common


def _is_contiguous(previous: _CommonBar, current: _CommonBar, step_ms: int) -> bool:
    return (
        current.spot.open_time_ms == previous.spot.open_time_ms + step_ms
        and current.futures.open_time_ms == previous.futures.open_time_ms + step_ms
    )


def _validate_inputs(
    asset: BacktestAsset,
    spot: KlineDataset,
    futures: KlineDataset,
    funding: FundingDataset,
) -> None:
    expected = (
        spot.request.market is Market.SPOT,
        futures.request.market is Market.FUTURES,
        spot.request.symbol == asset.spot_symbol,
        futures.request.symbol == asset.futures_symbol,
        funding.symbol == asset.futures_symbol,
        spot.request.interval == _FROZEN_INTERVAL,
        futures.request.interval == _FROZEN_INTERVAL,
    )
    if not all(expected):
        raise ValueError("carry inputs do not match the frozen Spot/Futures 5m asset identity")


def _validate_frozen_costs(costs: CostSettings) -> None:
    actual = (
        costs.notional_usdt,
        costs.spot_fee_bps,
        costs.futures_fee_bps,
        costs.spot_slippage_bps,
        costs.futures_slippage_bps,
        costs.include_funding,
    )
    expected = (
        100.0,
        10.0,
        5.0,
        _FROZEN_SPOT_SLIPPAGE_BPS,
        _FROZEN_FUTURES_SLIPPAGE_BPS,
        True,
    )
    if actual != expected:
        raise ValueError("C1 requires the exact frozen full-pair cost contract")


def _funding_window(
    funding: list[FundingRate],
    funding_times: list[int],
    decision_time_ms: int,
    policy: CarryPolicySettings,
) -> list[FundingRate]:
    cutoff = decision_time_ms - policy.funding_lookback_days * _DAY_MS
    start = bisect_left(funding_times, cutoff)
    end = bisect_left(funding_times, decision_time_ms)
    return funding[start:end]


def _decision_id(
    protocol_version: str,
    asset: str,
    split: str,
    decision_time_ms: int,
    funding_time_ms: int,
) -> str:
    raw = "|".join(
        (protocol_version, asset, split, str(decision_time_ms), str(funding_time_ms))
    )
    return hashlib.sha256(raw.encode()).hexdigest()[:24]


def _entry_decision(
    *,
    protocol_version: str,
    rule_version: str,
    asset: BacktestAsset,
    split: BacktestSplit,
    current: _CommonBar,
    next_bar: _CommonBar,
    contiguous_basis_history: list[float],
    prior_funding: list[FundingRate],
    triggering_funding: FundingRate,
    costs: CostSettings,
    policy: CarryPolicySettings,
    position_open: bool,
    last_exit_time_ms: int | None,
    next_bar_is_contiguous: bool,
) -> CarryEntryDecision:
    split_start_ms = int(split.start.timestamp() * 1000)
    split_end_ms = int(split.end.timestamp() * 1000)
    prospective_entry_time = next_bar.spot.open_time_ms
    maximum_holding_ms = policy.maximum_holding_days * _DAY_MS
    reasons: list[str] = []

    basis_median: float | None = None
    basis_q90: float | None = None
    basis_mad: float | None = None
    if len(contiguous_basis_history) != policy.basis_lookback_bars:
        reasons.append("insufficient_contiguous_basis_history")
    else:
        basis_median = statistics.median(contiguous_basis_history)
        basis_q90 = _quantile(contiguous_basis_history, policy.entry_basis_quantile)
        basis_mad = statistics.median(
            abs(value - basis_median) for value in contiguous_basis_history
        )

    funding_last: float | None = None
    funding_q25: float | None = None
    positive_fraction: float | None = None
    funding_cadence_ms: float | None = None
    expected_events: int | None = None
    if len(prior_funding) < policy.funding_minimum_events:
        reasons.append("insufficient_strict_prior_funding_history")
    else:
        rates = [item.rate for item in prior_funding]
        funding_last = rates[-1]
        funding_q25 = _quantile(rates, policy.funding_floor_quantile)
        positive_fraction = sum(value > 0 for value in rates) / len(rates)
        intervals = [
            later.funding_time_ms - earlier.funding_time_ms
            for earlier, later in pairwise(prior_funding)
        ]
        funding_cadence_ms = statistics.median(intervals)
        if funding_cadence_ms <= 0:
            reasons.append("invalid_funding_cadence")
        else:
            expected_events = max(
                0,
                math.floor(maximum_holding_ms / funding_cadence_ms) - 1,
            )

    if position_open:
        reasons.append("position_open")
    if (
        last_exit_time_ms is not None
        and prospective_entry_time < last_exit_time_ms + policy.cooldown_hours * _HOUR_MS
    ):
        reasons.append("post_exit_cooldown")
    if prospective_entry_time < split_start_ms + policy.split_edge_purge_days * _DAY_MS:
        reasons.append("split_start_purge")
    if prospective_entry_time + maximum_holding_ms >= split_end_ms:
        reasons.append("split_end_purge")
    if not next_bar_is_contiguous:
        reasons.append("next_common_open_gap")
    if current.spot.close_time_ms >= prospective_entry_time:
        reasons.append("noncausal_next_open")

    expected_edge: float | None = None
    stress_hurdle: float | None = None
    target_basis: float | None = basis_median
    stop_basis: float | None = None
    if basis_median is not None and basis_q90 is not None and basis_mad is not None:
        if current.basis <= 0:
            reasons.append("basis_not_positive")
        if current.basis < basis_q90:
            reasons.append("basis_below_prior_q90")
        stop_basis = current.basis + max(
            policy.stop_floor_bps / 10_000,
            policy.stop_mad_multiple * policy.mad_consistency_scale * basis_mad,
        )

    if (
        funding_last is not None
        and funding_q25 is not None
        and positive_fraction is not None
        and expected_events is not None
        and basis_median is not None
    ):
        if funding_last <= 0:
            reasons.append("latest_funding_not_positive")
        if funding_q25 <= 0:
            reasons.append("funding_q25_not_positive")
        if positive_fraction < policy.minimum_positive_funding_fraction:
            reasons.append("positive_funding_fraction_below_75pct")

        pair_denominator = 2 + current.basis
        expected_edge = (
            policy.expected_convergence_fraction
            * max(current.basis - basis_median, 0)
            + expected_events * funding_q25 * (1 + current.basis)
        ) / pair_denominator
        spot_fee = costs.spot_fee_bps / 10_000
        spot_slippage = costs.spot_slippage_bps[asset.cohort] / 10_000
        futures_fee = costs.futures_fee_bps / 10_000
        futures_slippage = costs.futures_slippage_bps[asset.cohort] / 10_000
        stress_hurdle = 2 * (
            (spot_fee + 2 * spot_slippage)
            + (1 + current.basis) * (futures_fee + 2 * futures_slippage)
        ) / pair_denominator
        if expected_edge <= stress_hurdle + policy.edge_margin_bps / 10_000:
            reasons.append("expected_edge_not_above_stress_plus_10bp")

    accepted = not reasons
    decision_time_ms = current.spot.close_time_ms
    return CarryEntryDecision(
        decision_id=_decision_id(
            protocol_version,
            asset.asset,
            split.name,
            decision_time_ms,
            triggering_funding.funding_time_ms,
        ),
        protocol_version=protocol_version,
        rule_version=rule_version,
        asset=asset.asset,
        cohort=asset.cohort,
        split=split.name,
        decision_time_ms=decision_time_ms,
        entry_time_ms=prospective_entry_time if accepted else None,
        triggering_funding_time_ms=triggering_funding.funding_time_ms,
        basis=current.basis,
        basis_median=basis_median,
        basis_q90=basis_q90,
        basis_mad=basis_mad,
        funding_last=funding_last,
        funding_q25=funding_q25,
        positive_funding_fraction=positive_fraction,
        funding_cadence_ms=funding_cadence_ms,
        expected_funding_events=expected_events,
        expected_pair_edge=expected_edge,
        stress_cost_hurdle=stress_hurdle,
        target_basis=target_basis,
        stop_basis=stop_basis,
        accepted=accepted,
        rejection_reasons=tuple(reasons),
    )


def _newly_observed_funding(
    funding: list[FundingRate],
    funding_times: list[int],
    after_time_ms: int,
    decision_time_ms: int,
) -> list[FundingRate]:
    start = bisect_right(funding_times, after_time_ms)
    end = bisect_left(funding_times, decision_time_ms)
    return funding[start:end]


def _close_trade(
    *,
    position: _Position,
    exit_decision_time_ms: int,
    exit_time_ms: int,
    exit_spot_price: float,
    exit_futures_price: float,
    exit_signal_basis: float,
    exit_reason: CarryExitReason,
    funding: list[FundingRate],
    funding_times: list[int],
    costs: CostSettings,
    asset: BacktestAsset,
    analysis_eligible: bool,
    exclusion_reason: str,
) -> CarryTrade:
    decision = position.decision
    if decision.target_basis is None or decision.stop_basis is None:
        raise ValueError("accepted carry decision is missing frozen exit levels")
    pair_capital = costs.notional_usdt
    base_quantity = pair_capital / (
        position.spot_entry_price + position.futures_entry_price
    )
    spot_execution = calculate_execution_returns(
        Direction.LONG,
        position.spot_entry_price,
        exit_spot_price,
        costs.spot_fee_bps,
        costs.spot_slippage_bps[asset.cohort],
    )
    futures_execution = calculate_execution_returns(
        Direction.SHORT,
        position.futures_entry_price,
        exit_futures_price,
        costs.futures_fee_bps,
        costs.futures_slippage_bps[asset.cohort],
    )
    spot_scale = base_quantity * position.spot_entry_price
    futures_scale = base_quantity * position.futures_entry_price
    gross_pnl = (
        spot_execution.gross_return * spot_scale
        + futures_execution.gross_return * futures_scale
    )
    slippage = (
        spot_execution.slippage_return * spot_scale
        + futures_execution.slippage_return * futures_scale
    )
    fees = spot_execution.fee_return * spot_scale + futures_execution.fee_return * futures_scale

    first_eligible_funding_time = position.entry_time_ms + interval_to_milliseconds(
        _FROZEN_INTERVAL
    )
    start = bisect_left(funding_times, first_eligible_funding_time)
    end = bisect_left(funding_times, exit_decision_time_ms)
    included_funding = funding[start:end]
    funding_return = calculate_funding_return(
        Direction.SHORT,
        position.entry_time_ms,
        exit_decision_time_ms,
        position.futures_entry_price,
        included_funding,
    )
    funding_pnl = funding_return * futures_scale
    net_pnl = gross_pnl - slippage - fees + funding_pnl
    trade_identity = "|".join(
        (decision.decision_id, str(exit_time_ms), exit_reason.value)
    )
    return CarryTrade(
        trade_id=hashlib.sha256(trade_identity.encode()).hexdigest()[:24],
        decision_id=decision.decision_id,
        protocol_version=decision.protocol_version,
        rule_version=decision.rule_version,
        asset=decision.asset,
        cohort=decision.cohort,
        split=decision.split,
        triggering_funding_time_ms=decision.triggering_funding_time_ms,
        entry_decision_time_ms=decision.decision_time_ms,
        exit_decision_time_ms=exit_decision_time_ms,
        entry_time_ms=position.entry_time_ms,
        exit_time_ms=exit_time_ms,
        exit_reason=exit_reason.value,
        target_basis=decision.target_basis,
        stop_basis=decision.stop_basis,
        entry_signal_basis=decision.basis,
        entry_fill_basis=position.entry_fill_basis,
        exit_signal_basis=exit_signal_basis,
        base_quantity=base_quantity,
        pair_capital_usdt=pair_capital,
        spot_entry_price=position.spot_entry_price,
        spot_exit_price=exit_spot_price,
        futures_entry_price=position.futures_entry_price,
        futures_exit_price=exit_futures_price,
        gross_pnl_usdt=gross_pnl,
        slippage_usdt=slippage,
        fees_usdt=fees,
        funding_pnl_usdt=funding_pnl,
        net_pnl_usdt=net_pnl,
        gross_return=gross_pnl / pair_capital,
        net_return=net_pnl / pair_capital,
        funding_event_count=len(included_funding),
        analysis_eligible=analysis_eligible,
        exclusion_reason=exclusion_reason,
    )


def run_carry_pair(
    *,
    protocol_version: str,
    rule_version: str,
    asset: BacktestAsset,
    split: BacktestSplit,
    spot: KlineDataset,
    futures: KlineDataset,
    funding: FundingDataset,
    costs: CostSettings,
    policy: CarryPolicySettings,
) -> CarryRun:
    """Replay one research-only long-Spot/short-perpetual C1 pair.

    Inputs are already validated public datasets. The function has no network,
    account, order, or other side effects.
    """

    _validate_inputs(asset, spot, futures, funding)
    _validate_frozen_costs(costs)
    common = _align_common_bars(spot, futures)
    step_ms = interval_to_milliseconds(_FROZEN_INTERVAL)
    rates = list(funding.rates)
    funding_times = [item.funding_time_ms for item in rates]
    split_start_ms = int(split.start.timestamp() * 1000)
    split_end_ms = int(split.end.timestamp() * 1000)
    decisions: list[CarryEntryDecision] = []
    trades: list[CarryTrade] = []
    position: _Position | None = None
    last_exit_time_ms: int | None = None
    segment_start = 0
    gap_count = 0

    for index in range(max(0, len(common) - 1)):
        current = common[index]
        next_bar = common[index + 1]
        previous = common[index - 1] if index > 0 else None
        contiguous_from_previous = (
            previous is not None and _is_contiguous(previous, current, step_ms)
        )
        if previous is not None and not contiguous_from_previous:
            gap_count += 1
            segment_start = index
            if position is not None:
                gap_exit = _close_trade(
                    position=position,
                    exit_decision_time_ms=current.spot.open_time_ms,
                    exit_time_ms=current.spot.open_time_ms,
                    exit_spot_price=float(current.spot.open),
                    exit_futures_price=float(current.futures.open),
                    exit_signal_basis=_basis(
                        float(current.spot.open), float(current.futures.open)
                    ),
                    exit_reason=CarryExitReason.DATA_GAP,
                    funding=rates,
                    funding_times=funding_times,
                    costs=costs,
                    asset=asset,
                    analysis_eligible=False,
                    exclusion_reason="common_5m_gap_while_open",
                )
                trades.append(gap_exit)
                last_exit_time_ms = gap_exit.exit_time_ms
                position = None

        decision_time_ms = current.spot.close_time_ms
        if decision_time_ms < split_start_ms or current.spot.open_time_ms >= split_end_ms:
            continue

        next_is_contiguous = _is_contiguous(current, next_bar, step_ms)
        if position is not None:
            newly_observed = _newly_observed_funding(
                rates,
                funding_times,
                position.last_observed_funding_time_ms,
                decision_time_ms,
            )
            if newly_observed:
                position.last_observed_funding_time_ms = newly_observed[-1].funding_time_ms
            eligible_new_funding = [
                item
                for item in newly_observed
                if item.funding_time_ms >= position.entry_time_ms + step_ms
            ]
            funding_flip = any(item.rate <= 0 for item in eligible_new_funding)
            next_open_time_ms = next_bar.spot.open_time_ms
            exit_reason: CarryExitReason | None = None
            decision = position.decision
            if decision.stop_basis is None or decision.target_basis is None:
                raise ValueError("open carry position is missing frozen exit levels")
            if current.basis >= decision.stop_basis:
                exit_reason = CarryExitReason.STOP
            elif funding_flip:
                exit_reason = CarryExitReason.FUNDING_FLIP
            elif current.basis <= decision.target_basis:
                exit_reason = CarryExitReason.CONVERGENCE
            elif (
                next_open_time_ms
                >= position.entry_time_ms + policy.maximum_holding_days * _DAY_MS
            ):
                exit_reason = CarryExitReason.TIME

            if exit_reason is not None:
                if next_is_contiguous and next_open_time_ms < split_end_ms:
                    trade = _close_trade(
                        position=position,
                        exit_decision_time_ms=decision_time_ms,
                        exit_time_ms=next_open_time_ms,
                        exit_spot_price=float(next_bar.spot.open),
                        exit_futures_price=float(next_bar.futures.open),
                        exit_signal_basis=current.basis,
                        exit_reason=exit_reason,
                        funding=rates,
                        funding_times=funding_times,
                        costs=costs,
                        asset=asset,
                        analysis_eligible=True,
                        exclusion_reason="",
                    )
                else:
                    trade = _close_trade(
                        position=position,
                        exit_decision_time_ms=next_bar.spot.open_time_ms,
                        exit_time_ms=next_bar.spot.open_time_ms,
                        exit_spot_price=float(next_bar.spot.open),
                        exit_futures_price=float(next_bar.futures.open),
                        exit_signal_basis=_basis(
                            float(next_bar.spot.open), float(next_bar.futures.open)
                        ),
                        exit_reason=CarryExitReason.DATA_GAP,
                        funding=rates,
                        funding_times=funding_times,
                        costs=costs,
                        asset=asset,
                        analysis_eligible=False,
                        exclusion_reason="exit_next_common_open_not_contiguous_or_outside_split",
                    )
                trades.append(trade)
                last_exit_time_ms = trade.exit_time_ms
                position = None

        if previous is None:
            continue
        newly_settled_start = bisect_left(funding_times, previous.spot.close_time_ms)
        newly_settled_end = bisect_left(funding_times, decision_time_ms)
        if newly_settled_start >= newly_settled_end:
            continue
        triggering_funding = rates[newly_settled_end - 1]
        history_start = index - policy.basis_lookback_bars
        basis_history = (
            [item.basis for item in common[history_start:index]]
            if history_start >= segment_start
            else []
        )
        prior_funding = _funding_window(rates, funding_times, decision_time_ms, policy)
        entry_decision = _entry_decision(
            protocol_version=protocol_version,
            rule_version=rule_version,
            asset=asset,
            split=split,
            current=current,
            next_bar=next_bar,
            contiguous_basis_history=basis_history,
            prior_funding=prior_funding,
            triggering_funding=triggering_funding,
            costs=costs,
            policy=policy,
            position_open=position is not None,
            last_exit_time_ms=last_exit_time_ms,
            next_bar_is_contiguous=next_is_contiguous,
        )
        decisions.append(entry_decision)
        if not entry_decision.accepted:
            continue
        position = _Position(
            decision=entry_decision,
            entry_time_ms=next_bar.spot.open_time_ms,
            spot_entry_price=float(next_bar.spot.open),
            futures_entry_price=float(next_bar.futures.open),
            entry_fill_basis=_basis(
                float(next_bar.spot.open), float(next_bar.futures.open)
            ),
            last_observed_funding_time_ms=triggering_funding.funding_time_ms,
        )

    if len(common) >= 2 and not _is_contiguous(common[-2], common[-1], step_ms):
        gap_count += 1
        if position is not None:
            terminal = common[-1]
            gap_exit = _close_trade(
                position=position,
                exit_decision_time_ms=terminal.spot.open_time_ms,
                exit_time_ms=terminal.spot.open_time_ms,
                exit_spot_price=float(terminal.spot.open),
                exit_futures_price=float(terminal.futures.open),
                exit_signal_basis=_basis(
                    float(terminal.spot.open), float(terminal.futures.open)
                ),
                exit_reason=CarryExitReason.DATA_GAP,
                funding=rates,
                funding_times=funding_times,
                costs=costs,
                asset=asset,
                analysis_eligible=False,
                exclusion_reason="terminal_common_5m_gap_while_open",
            )
            trades.append(gap_exit)
            position = None

    return CarryRun(
        decisions=tuple(decisions),
        trades=tuple(trades),
        common_bar_count=len(common),
        gap_count=gap_count,
        open_position_at_end=position is not None,
    )
