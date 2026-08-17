from __future__ import annotations

import csv
import hashlib
import json
import math
import platform
import random
import statistics
import struct
import sys
import time
from collections import Counter, defaultdict
from collections.abc import Mapping, Sequence
from dataclasses import asdict, dataclass, fields
from datetime import UTC, datetime
from itertools import pairwise
from pathlib import Path
from typing import Any

from signalbot.backtest.carry import (
    CarryEntryDecision,
    CarryExitReason,
    CarryExperimentSpec,
    CarryTrade,
    load_carry_spec,
    run_carry_pair,
)
from signalbot.backtest.dataset import (
    DatasetManifest,
    DatasetValidationError,
    KlineDataset,
    KlineDatasetRequest,
    read_dataset_manifest,
    read_kline_csv,
    sha256_file,
    verify_dataset_manifest,
)
from signalbot.backtest.engine import calculate_execution_returns
from signalbot.backtest.funding import (
    FundingDataset,
    funding_sha256,
    verify_funding_dataset,
)
from signalbot.backtest.r2 import (
    circular_moving_block_indices,
    one_sided_basic_lower_bound,
    pro_one_sided_p_value,
)
from signalbot.backtest.runner import dataset_path, funding_path, source_code_digest
from signalbot.data.candles import interval_to_milliseconds
from signalbot.domain.enums import Direction, Market

_DAY_MS = 86_400_000
_HOUR_MS = 3_600_000
_FUNDING_TIMESTAMP_TOLERANCE_MS = 60_000
# First public WIFUSDT Spot 5m open in the frozen Binance archive (listing boundary).
_WIF_SPOT_LISTING_OPEN_MS = 1_709_647_200_000
# Verified on 2026-07-17 against Binance public USDⓈ-M `/fapi/v1/fundingRate` history:
# WIFUSDT has no 2026-06-24 04:00 UTC settlement between these official rows.
# This exact timestamp pair is the only permitted departure from the 4h schedule.
_WIF_EIGHT_HOUR_FUNDING_EXCEPTION = (
    1_782_259_200_000,
    1_782_288_000_000,
)
_FROZEN_FUNDING_INTERVAL_HOURS = {
    "BTC": 8,
    "ETH": 8,
    "BNB": 8,
    "SOL": 8,
    "XRP": 8,
    "DOGE": 8,
    "SUI": 8,
    "WIF": 4,
}
_OUTPUT_NAMES = (
    "c1_decisions.csv",
    "c1_trades.csv",
    "c1_daily_pair_pnl.csv",
    "c1_analysis.json",
    "c1_report_ko.md",
)
_ARTIFACT_ROLE = "EXPOSED_RETROSPECTIVE_RESEARCH_ONLY"
_PRIMARY_FAMILY = "C1_FUNDING_BASIS_CARRY_SINGLE_PRIMARY"


@dataclass(frozen=True, slots=True)
class DailyPairPnl:
    utc_day_start_ms: int
    utc_date: str
    split: str
    asset: str
    eligible_completed_episodes: int
    pair_capital_usdt: float
    gross_pnl_usdt: float
    fees_usdt: float
    base_slippage_usdt: float
    funding_pnl_usdt: float
    base_net_pnl_usdt: float
    two_x_slippage_net_pnl_usdt: float


def _canonical_json(payload: object) -> str:
    return (
        json.dumps(
            payload,
            ensure_ascii=False,
            indent=2,
            sort_keys=True,
            allow_nan=False,
        )
        + "\n"
    )


def _close(left: float, right: float) -> bool:
    return math.isclose(left, right, rel_tol=1e-9, abs_tol=1e-9)


def _primary_splits(spec: CarryExperimentSpec) -> frozenset[str]:
    return frozenset(spec.acceptance.required_positive_splits)


def _validate_finite(values: Sequence[float], context: str) -> None:
    if any(not math.isfinite(value) for value in values):
        raise ValueError(f"{context} contains a non-finite value")


def _required_kline_first_open_ms(
    spec: CarryExperimentSpec,
    market: Market,
    asset: str,
) -> int:
    if market is Market.SPOT and asset == "WIF":
        return _WIF_SPOT_LISTING_OPEN_MS
    return int(spec.data_start.timestamp() * 1000)


def _validate_kline_manifest_coverage(
    manifest: DatasetManifest,
    spec: CarryExperimentSpec,
    market: Market,
    asset: str,
) -> None:
    """Require the exact frozen C1 candle grid, not only request metadata."""

    step_ms = interval_to_milliseconds(spec.interval)
    first_open_ms = _required_kline_first_open_ms(spec, market, asset)
    end_exclusive_ms = int(spec.evaluation_end.timestamp() * 1000)
    duration_ms = end_exclusive_ms - first_open_ms
    expected_rows, remainder = divmod(duration_ms, step_ms)
    if expected_rows <= 0 or remainder:
        raise DatasetValidationError("frozen C1 kline coverage is not a whole 5m grid")
    expected_last_close_ms = end_exclusive_ms - 1
    if manifest.first_open_time_ms != first_open_ms:
        raise DatasetValidationError(
            f"C1 {market.value} {asset} first candle does not match the frozen coverage boundary"
        )
    if manifest.last_close_time_ms != expected_last_close_ms:
        raise DatasetValidationError(
            f"C1 {market.value} {asset} final candle does not reach evaluation_end"
        )
    if manifest.gap_count != 0 or manifest.missing_intervals != 0:
        raise DatasetValidationError(
            f"C1 {market.value} {asset} contains an internal 5m coverage gap"
        )
    if manifest.row_count != expected_rows:
        raise DatasetValidationError(
            f"C1 {market.value} {asset} row count does not cover the exact 5m grid"
        )


def _on_hour_grid(timestamp_ms: int) -> bool:
    nearest_hour_ms = round(timestamp_ms / _HOUR_MS) * _HOUR_MS
    return abs(timestamp_ms - nearest_hour_ms) <= _FUNDING_TIMESTAMP_TOLERANCE_MS


def _matches_funding_interval(
    earlier_ms: int,
    later_ms: int,
    expected_hours: int,
) -> bool:
    expected_ms = expected_hours * _HOUR_MS
    return abs((later_ms - earlier_ms) - expected_ms) <= (
        2 * _FUNDING_TIMESTAMP_TOLERANCE_MS
    )


def _is_wif_funding_exception(earlier_ms: int, later_ms: int) -> bool:
    expected_earlier, expected_later = _WIF_EIGHT_HOUR_FUNDING_EXCEPTION
    return (
        abs(earlier_ms - expected_earlier) <= _FUNDING_TIMESTAMP_TOLERANCE_MS
        and abs(later_ms - expected_later) <= _FUNDING_TIMESTAMP_TOLERANCE_MS
        and _matches_funding_interval(earlier_ms, later_ms, 8)
    )


def _validate_c1_funding_coverage(
    dataset: FundingDataset,
    spec: CarryExperimentSpec,
    asset: str,
) -> None:
    """Fail closed on missing boundaries or unsupported Binance funding gaps."""

    interval_hours = _FROZEN_FUNDING_INTERVAL_HOURS.get(asset)
    if interval_hours is None:
        raise ValueError(f"C1 has no frozen funding schedule for {asset}")
    if not dataset.rates:
        raise ValueError(f"C1 funding dataset is empty for {asset}")
    start_ms = int(spec.data_start.timestamp() * 1000)
    end_exclusive_ms = int(spec.evaluation_end.timestamp() * 1000)
    first_ms = dataset.rates[0].funding_time_ms
    last_ms = dataset.rates[-1].funding_time_ms
    expected_last_ms = end_exclusive_ms - interval_hours * _HOUR_MS
    if abs(first_ms - start_ms) > _FUNDING_TIMESTAMP_TOLERANCE_MS:
        raise ValueError(f"C1 funding coverage does not start at data_start for {asset}")
    if abs(last_ms - expected_last_ms) > _FUNDING_TIMESTAMP_TOLERANCE_MS:
        raise ValueError(f"C1 funding coverage does not reach evaluation_end for {asset}")
    if any(not _on_hour_grid(item.funding_time_ms) for item in dataset.rates):
        raise ValueError(f"C1 funding timestamp is off the Binance hourly grid for {asset}")

    for earlier, later in pairwise(dataset.rates):
        if _matches_funding_interval(
            earlier.funding_time_ms,
            later.funding_time_ms,
            interval_hours,
        ):
            continue
        if asset == "WIF" and _is_wif_funding_exception(
            earlier.funding_time_ms, later.funding_time_ms
        ):
            continue
        raise ValueError(
            f"C1 funding schedule gap is unsupported for {asset}: "
            f"{earlier.funding_time_ms}->{later.funding_time_ms}"
        )


def _expected_decision_id(decision: CarryEntryDecision) -> str:
    identity = "|".join(
        (
            decision.protocol_version,
            decision.asset,
            decision.split,
            str(decision.decision_time_ms),
            str(decision.triggering_funding_time_ms),
        )
    )
    return hashlib.sha256(identity.encode()).hexdigest()[:24]


def _expected_trade_id(trade: CarryTrade) -> str:
    identity = "|".join(
        (trade.decision_id, str(trade.exit_time_ms), trade.exit_reason)
    )
    return hashlib.sha256(identity.encode()).hexdigest()[:24]


def _validate_carry_ledgers(
    decisions: Sequence[CarryEntryDecision],
    trades: Sequence[CarryTrade],
    spec: CarryExperimentSpec,
) -> dict[str, Any]:
    decision_by_id: dict[str, CarryEntryDecision] = {}
    asset_by_name = {item.asset: item for item in spec.assets}
    split_by_name = {item.name: item for item in spec.splits}
    step_ms = interval_to_milliseconds(spec.interval)
    maximum_holding_ms = spec.carry.maximum_holding_days * _DAY_MS
    purge_ms = spec.carry.split_edge_purge_days * _DAY_MS
    for decision in decisions:
        if decision.decision_id in decision_by_id:
            raise ValueError(f"duplicate carry decision_id: {decision.decision_id}")
        if decision.decision_id != _expected_decision_id(decision):
            raise ValueError("carry decision deterministic ID mismatch")
        if decision.protocol_version != spec.protocol_version:
            raise ValueError("carry decision protocol_version mismatch")
        if decision.rule_version != spec.rule_version:
            raise ValueError("carry decision rule_version mismatch")
        asset = asset_by_name.get(decision.asset)
        if asset is None or decision.cohort != asset.cohort:
            raise ValueError("carry decision asset/cohort mismatch")
        split = split_by_name.get(decision.split)
        if split is None:
            raise ValueError("carry decision split is not declared")
        split_start_ms = int(split.start.timestamp() * 1000)
        split_end_ms = int(split.end.timestamp() * 1000)
        if not split_start_ms <= decision.decision_time_ms < split_end_ms:
            raise ValueError("carry decision timestamp lies outside its split")
        if decision.decision_time_ms % step_ms != step_ms - 1:
            raise ValueError("carry decision is not on a fully closed 5m grid")
        if decision.triggering_funding_time_ms >= decision.decision_time_ms:
            raise ValueError("carry decision uses funding that was not strictly observable")
        optional_values = (
            decision.basis_median,
            decision.basis_q90,
            decision.basis_mad,
            decision.funding_last,
            decision.funding_q25,
            decision.positive_funding_fraction,
            decision.funding_cadence_ms,
            decision.expected_pair_edge,
            decision.stress_cost_hurdle,
            decision.target_basis,
            decision.stop_basis,
        )
        _validate_finite(
            [decision.basis, *(value for value in optional_values if value is not None)],
            f"decision {decision.decision_id}",
        )
        if decision.accepted:
            if decision.entry_time_ms is None or decision.rejection_reasons:
                raise ValueError("accepted carry decision has no entry or has rejections")
            if (
                decision.entry_time_ms != decision.decision_time_ms + 1
                or decision.entry_time_ms % step_ms != 0
            ):
                raise ValueError("accepted carry decision does not fill at the next 5m open")
            if decision.entry_time_ms < split_start_ms + purge_ms:
                raise ValueError("accepted carry decision violates the split-start purge")
            if decision.entry_time_ms + maximum_holding_ms >= split_end_ms:
                raise ValueError("accepted carry decision violates the split-end purge")
            if decision.target_basis is None or decision.stop_basis is None:
                raise ValueError("accepted carry decision is missing frozen exit levels")
        elif decision.entry_time_ms is not None or not decision.rejection_reasons:
            raise ValueError("rejected carry decision has an entry or no rejection reason")
        decision_by_id[decision.decision_id] = decision

    trade_ids: set[str] = set()
    trades_by_decision: dict[str, list[CarryTrade]] = defaultdict(list)
    valid_exit_reasons = {item.value for item in CarryExitReason}
    for trade in trades:
        if trade.trade_id in trade_ids:
            raise ValueError(f"duplicate carry trade_id: {trade.trade_id}")
        trade_ids.add(trade.trade_id)
        decision = decision_by_id.get(trade.decision_id)
        if decision is None:
            raise ValueError("carry trade references an unknown decision")
        if not decision.accepted:
            raise ValueError("rejected carry decision produced a trade")
        if trade.trade_id != _expected_trade_id(trade):
            raise ValueError("carry trade deterministic ID mismatch")
        if (
            trade.protocol_version != decision.protocol_version
            or trade.rule_version != decision.rule_version
            or trade.asset != decision.asset
            or trade.cohort != decision.cohort
            or trade.split != decision.split
            or trade.triggering_funding_time_ms != decision.triggering_funding_time_ms
            or trade.entry_decision_time_ms != decision.decision_time_ms
            or trade.entry_time_ms != decision.entry_time_ms
        ):
            raise ValueError("carry trade identity does not match its decision")
        if trade.exit_reason not in valid_exit_reasons:
            raise ValueError("carry trade has an unsupported exit reason")
        entry_ordered = trade.entry_decision_time_ms < trade.entry_time_ms
        if trade.exit_reason == CarryExitReason.DATA_GAP.value:
            exit_ordered = (
                trade.entry_time_ms < trade.exit_time_ms
                and trade.exit_decision_time_ms == trade.exit_time_ms
            )
        else:
            exit_ordered = (
                trade.entry_time_ms <= trade.exit_decision_time_ms
                and trade.exit_decision_time_ms < trade.exit_time_ms
            )
        if not entry_ordered or not exit_ordered:
            raise ValueError("carry trade timestamps are not causally ordered")
        if trade.entry_time_ms % step_ms != 0 or trade.exit_time_ms % step_ms != 0:
            raise ValueError("carry trade fills are not on the 5m open grid")
        split = split_by_name[trade.split]
        split_end_ms = int(split.end.timestamp() * 1000)
        if trade.exit_reason != CarryExitReason.DATA_GAP.value:
            if trade.exit_time_ms != trade.exit_decision_time_ms + 1:
                raise ValueError("ordinary carry exit does not fill at the next 5m open")
            if trade.exit_time_ms >= split_end_ms:
                raise ValueError("ordinary carry exit lies outside its split")
            if trade.exit_time_ms > trade.entry_time_ms + maximum_holding_ms:
                raise ValueError("ordinary carry exit exceeds the maximum holding period")
        _validate_finite(
            [
                trade.target_basis,
                trade.stop_basis,
                trade.entry_signal_basis,
                trade.entry_fill_basis,
                trade.exit_signal_basis,
                trade.base_quantity,
                trade.pair_capital_usdt,
                trade.spot_entry_price,
                trade.spot_exit_price,
                trade.futures_entry_price,
                trade.futures_exit_price,
                trade.gross_pnl_usdt,
                trade.slippage_usdt,
                trade.fees_usdt,
                trade.funding_pnl_usdt,
                trade.net_pnl_usdt,
                trade.gross_return,
                trade.net_return,
            ],
            f"trade {trade.trade_id}",
        )
        if (
            trade.base_quantity <= 0
            or trade.pair_capital_usdt <= 0
            or trade.slippage_usdt < 0
            or trade.fees_usdt < 0
            or trade.funding_event_count < 0
        ):
            raise ValueError("carry trade size/cost/count contract is invalid")
        if not _close(trade.pair_capital_usdt, spec.costs.notional_usdt):
            raise ValueError("carry trade pair capital differs from the frozen notional")
        if (
            not _close(trade.target_basis, decision.target_basis or 0.0)
            or not _close(trade.stop_basis, decision.stop_basis or 0.0)
            or not _close(trade.entry_signal_basis, decision.basis)
        ):
            raise ValueError("carry trade frozen entry/exit levels differ from its decision")
        expected_quantity = spec.costs.notional_usdt / (
            trade.spot_entry_price + trade.futures_entry_price
        )
        if not _close(trade.base_quantity, expected_quantity):
            raise ValueError("carry trade equal-base-quantity identity mismatch")
        if not _close(
            trade.entry_fill_basis,
            (trade.futures_entry_price - trade.spot_entry_price)
            / trade.spot_entry_price,
        ):
            raise ValueError("carry trade entry fill basis identity mismatch")
        spot_execution = calculate_execution_returns(
            Direction.LONG,
            trade.spot_entry_price,
            trade.spot_exit_price,
            spec.costs.spot_fee_bps,
            spec.costs.spot_slippage_bps[trade.cohort],
        )
        futures_execution = calculate_execution_returns(
            Direction.SHORT,
            trade.futures_entry_price,
            trade.futures_exit_price,
            spec.costs.futures_fee_bps,
            spec.costs.futures_slippage_bps[trade.cohort],
        )
        spot_scale = expected_quantity * trade.spot_entry_price
        futures_scale = expected_quantity * trade.futures_entry_price
        expected_gross = (
            spot_execution.gross_return * spot_scale
            + futures_execution.gross_return * futures_scale
        )
        expected_slippage = (
            spot_execution.slippage_return * spot_scale
            + futures_execution.slippage_return * futures_scale
        )
        expected_fees = (
            spot_execution.fee_return * spot_scale
            + futures_execution.fee_return * futures_scale
        )
        if not _close(trade.gross_pnl_usdt, expected_gross):
            raise ValueError("carry trade gross P&L does not match recorded prices")
        if not _close(trade.slippage_usdt, expected_slippage):
            raise ValueError("carry trade slippage does not match frozen costs")
        if not _close(trade.fees_usdt, expected_fees):
            raise ValueError("carry trade fees do not match frozen costs")
        if trade.funding_event_count == 0 and not _close(trade.funding_pnl_usdt, 0.0):
            raise ValueError("carry trade has funding P&L without a funding event")
        expected_net = (
            trade.gross_pnl_usdt
            - trade.slippage_usdt
            - trade.fees_usdt
            + trade.funding_pnl_usdt
        )
        if not _close(trade.net_pnl_usdt, expected_net):
            raise ValueError("carry trade base-cost P&L identity mismatch")
        if not _close(trade.gross_return, trade.gross_pnl_usdt / trade.pair_capital_usdt):
            raise ValueError("carry trade gross return identity mismatch")
        if not _close(trade.net_return, trade.net_pnl_usdt / trade.pair_capital_usdt):
            raise ValueError("carry trade net return identity mismatch")
        is_data_gap = trade.exit_reason == CarryExitReason.DATA_GAP.value
        if is_data_gap:
            if trade.analysis_eligible or not trade.exclusion_reason:
                raise ValueError("DATA_GAP carry trade must be analysis-ineligible")
        elif not trade.analysis_eligible or trade.exclusion_reason:
            raise ValueError("ordinary carry exit must be analysis-eligible")
        trades_by_decision[trade.decision_id].append(trade)

    for decision_id, mapped in trades_by_decision.items():
        if len(mapped) > 1:
            raise ValueError(f"accepted carry decision has multiple trades: {decision_id}")

    by_pair: dict[tuple[str, str], list[CarryTrade]] = defaultdict(list)
    for trade in trades:
        by_pair[(trade.asset, trade.split)].append(trade)
    for pair_trades in by_pair.values():
        ordered = sorted(pair_trades, key=lambda item: (item.entry_time_ms, item.trade_id))
        for previous, current in pairwise(ordered):
            if current.entry_time_ms <= previous.exit_time_ms:
                raise ValueError("carry trades overlap or reverse at the same open")
            if current.entry_time_ms < (
                previous.exit_time_ms + spec.carry.cooldown_hours * _HOUR_MS
            ):
                raise ValueError("carry trades violate the frozen post-exit cooldown")

    unmatched = [
        item for item in decisions if item.accepted and item.decision_id not in trades_by_decision
    ]
    primary = _primary_splits(spec)
    return {
        "valid": True,
        "decisions": len(decisions),
        "accepted_decisions": sum(item.accepted for item in decisions),
        "trades": len(trades),
        "analysis_eligible_trades": sum(item.analysis_eligible for item in trades),
        "analysis_ineligible_trades": sum(not item.analysis_eligible for item in trades),
        "outcome_unobservable": len(unmatched),
        "primary_outcome_unobservable": sum(item.split in primary for item in unmatched),
        "unobservable_decision_ids": sorted(item.decision_id for item in unmatched),
    }


def _stress_pnl(trade: CarryTrade) -> float:
    return trade.net_pnl_usdt - trade.slippage_usdt


def _profit_factor(values: Sequence[float]) -> tuple[float | None, str]:
    positive = sum(value for value in values if value > 0)
    negative = -sum(value for value in values if value < 0)
    if negative > 0:
        return positive / negative, "FINITE"
    if positive > 0:
        return None, "POSITIVE_WITH_NO_LOSSES"
    return None, "NO_POSITIVE_RETURNS"


def _metric_summary(trades: Sequence[CarryTrade], *, stress: bool) -> dict[str, Any]:
    pnl_values = [_stress_pnl(item) if stress else item.net_pnl_usdt for item in trades]
    pair_returns = [
        value / item.pair_capital_usdt
        for value, item in zip(pnl_values, trades, strict=True)
    ]
    total_pair_capital = sum(item.pair_capital_usdt for item in trades)
    net_pnl = sum(pnl_values)
    mean_return = statistics.fmean(pair_returns) if pair_returns else None
    profit_factor, profit_factor_state = _profit_factor(pnl_values)
    return {
        "completed_episodes": len(trades),
        "pair_capital_usdt": total_pair_capital,
        "gross_pnl_usdt": sum(item.gross_pnl_usdt for item in trades),
        "fees_usdt": sum(item.fees_usdt for item in trades),
        "slippage_usdt": sum(item.slippage_usdt for item in trades) * (2 if stress else 1),
        "funding_pnl_usdt": sum(item.funding_pnl_usdt for item in trades),
        "net_pnl_usdt": net_pnl,
        "aggregate_pair_return": (
            net_pnl / total_pair_capital if total_pair_capital > 0 else None
        ),
        "mean_pair_return": mean_return,
        "mean_pair_return_bps": None if mean_return is None else mean_return * 10_000,
        "win_rate": (
            sum(value > 0 for value in pnl_values) / len(pnl_values)
            if pnl_values
            else None
        ),
        "profit_factor": profit_factor,
        "profit_factor_state": profit_factor_state,
        "cost_scenario": "two_x_adverse_slippage" if stress else "frozen_base_costs",
    }


def _split_for_day(spec: CarryExperimentSpec, day_start_ms: int) -> str:
    day = datetime.fromtimestamp(day_start_ms / 1000, UTC)
    for split in spec.splits:
        if split.start <= day < split.end:
            return split.name
    raise ValueError("primary calendar day is not contained by a declared split")


def build_daily_pair_pnl(
    trades: Sequence[CarryTrade], spec: CarryExperimentSpec
) -> tuple[DailyPairPnl, ...]:
    """Build the frozen day-by-asset primary inference panel, including zero days."""

    start_ms = int(spec.acceptance.primary_calendar_start.timestamp() * 1000)
    end_ms = int(spec.acceptance.primary_calendar_end.timestamp() * 1000)
    day_count, remainder = divmod(end_ms - start_ms, _DAY_MS)
    if day_count <= 0 or remainder:
        raise ValueError("C1 primary calendar must contain whole UTC days")
    primary = _primary_splits(spec)
    eligible = [
        item for item in trades if item.analysis_eligible and item.split in primary
    ]
    by_cell: dict[tuple[int, str], list[CarryTrade]] = defaultdict(list)
    for trade in eligible:
        day_start = trade.entry_decision_time_ms // _DAY_MS * _DAY_MS
        if not start_ms <= day_start < end_ms:
            raise ValueError("primary carry trade lies outside the frozen primary calendar")
        by_cell[(day_start, trade.asset)].append(trade)

    rows: list[DailyPairPnl] = []
    for day_offset in range(day_count):
        day_start = start_ms + day_offset * _DAY_MS
        split = _split_for_day(spec, day_start)
        for asset in spec.assets:
            cell = by_cell.get((day_start, asset.asset), [])
            rows.append(
                DailyPairPnl(
                    utc_day_start_ms=day_start,
                    utc_date=datetime.fromtimestamp(day_start / 1000, UTC).date().isoformat(),
                    split=split,
                    asset=asset.asset,
                    eligible_completed_episodes=len(cell),
                    pair_capital_usdt=sum(item.pair_capital_usdt for item in cell),
                    gross_pnl_usdt=sum(item.gross_pnl_usdt for item in cell),
                    fees_usdt=sum(item.fees_usdt for item in cell),
                    base_slippage_usdt=sum(item.slippage_usdt for item in cell),
                    funding_pnl_usdt=sum(item.funding_pnl_usdt for item in cell),
                    base_net_pnl_usdt=sum(item.net_pnl_usdt for item in cell),
                    two_x_slippage_net_pnl_usdt=sum(_stress_pnl(item) for item in cell),
                )
            )
    return tuple(rows)


def _shared_calendar_bootstrap(
    rows: Sequence[DailyPairPnl],
    spec: CarryExperimentSpec,
    *,
    samples: int | None = None,
    seed: int | None = None,
) -> dict[str, Any]:
    acceptance = spec.acceptance
    sample_count = acceptance.bootstrap_samples if samples is None else samples
    bootstrap_seed = acceptance.bootstrap_seed if seed is None else seed
    if sample_count <= 0:
        raise ValueError("bootstrap samples must be positive")
    start_day = int(acceptance.primary_calendar_start.timestamp() * 1000) // _DAY_MS
    end_day = int(acceptance.primary_calendar_end.timestamp() * 1000) // _DAY_MS
    day_count = end_day - start_day
    numerators = [0.0] * day_count
    denominators = [0.0] * day_count
    expected_assets = {item.asset for item in spec.assets}
    seen_cells: set[tuple[int, str]] = set()
    for row in rows:
        offset = row.utc_day_start_ms // _DAY_MS - start_day
        if not 0 <= offset < day_count:
            raise ValueError("daily carry row lies outside the primary calendar")
        if row.asset not in expected_assets:
            raise ValueError("daily carry row contains an undeclared asset")
        cell = (row.utc_day_start_ms, row.asset)
        if cell in seen_cells:
            raise ValueError("daily carry panel contains a duplicate day/asset cell")
        seen_cells.add(cell)
        numerators[offset] += row.two_x_slippage_net_pnl_usdt
        denominators[offset] += row.pair_capital_usdt
    if len(seen_cells) != day_count * len(expected_assets):
        raise ValueError("daily carry panel does not cover every primary day/asset cell")

    total_denominator = sum(denominators)
    point = sum(numerators) / total_denominator if total_denominator > 0 else None
    full_blocks, remainder = divmod(day_count, acceptance.bootstrap_block_days)
    block_count = full_blocks + bool(remainder)
    rng = random.Random(bootstrap_seed)
    digest = hashlib.sha256()
    packer = struct.Struct(f"<{block_count}I")
    estimates: list[float] = []
    invalid = 0
    for _ in range(sample_count):
        starts = tuple(rng.randrange(day_count) for _ in range(block_count))
        digest.update(packer.pack(*starts))
        indices = circular_moving_block_indices(
            day_count,
            acceptance.bootstrap_block_days,
            starts,
        )
        denominator = sum(denominators[index] for index in indices)
        if denominator <= 0:
            invalid += 1
            continue
        estimates.append(sum(numerators[index] for index in indices) / denominator)

    lower: float | None = None
    p_value: float | None = None
    if point is not None and estimates:
        alpha = 1 - acceptance.one_sided_confidence
        lower = one_sided_basic_lower_bound(point, estimates, alpha=alpha)
        p_value = pro_one_sided_p_value(point, estimates)
    return {
        "method": "shared_asset_circular_moving_utc_day_block_bootstrap",
        "pnl_attribution": acceptance.bootstrap_pnl_attribution,
        "include_zero_event_days": acceptance.include_zero_event_days,
        "calendar_start": acceptance.primary_calendar_start.isoformat(),
        "calendar_end": acceptance.primary_calendar_end.isoformat(),
        "calendar_days": day_count,
        "block_days": acceptance.bootstrap_block_days,
        "samples": sample_count,
        "seed": bootstrap_seed,
        "point_estimate": point,
        "valid_replicates": len(estimates),
        "invalid_replicates": invalid,
        "invalid_fraction": invalid / sample_count,
        "one_sided_basic_95_lower": lower,
        "null_centered_one_sided_p_value": p_value,
        "shared_draw_schedule_sha256": digest.hexdigest(),
    }


def _profit_factor_gate(summary: Mapping[str, Any], threshold: float) -> bool:
    value = summary["profit_factor"]
    return bool(
        (isinstance(value, (int, float)) and value >= threshold)
        or summary["profit_factor_state"] == "POSITIVE_WITH_NO_LOSSES"
    )


def evaluate_c1_acceptance(
    *,
    primary: Mapping[str, Any],
    bootstrap: Mapping[str, Any],
    split_summaries: Mapping[str, Mapping[str, Any]],
    maximum_asset_positive_pnl_share: float | None,
    primary_outcome_unobservable: int,
    spec: CarryExperimentSpec,
) -> tuple[str, dict[str, Any]]:
    acceptance = spec.acceptance
    mean = primary["mean_pair_return"]
    lower = bootstrap["one_sided_basic_95_lower"]
    split_criteria = {
        name: {
            "aggregate_pnl_positive": split_summaries[name]["net_pnl_usdt"] > 0,
            "mean_pair_return_positive": (
                split_summaries[name]["mean_pair_return"] is not None
                and split_summaries[name]["mean_pair_return"] > 0
            ),
        }
        for name in acceptance.required_positive_splits
    }
    criteria: dict[str, Any] = {
        "completed_episodes_at_least_100": (
            primary["completed_episodes"] >= acceptance.minimum_completed_episodes
        ),
        "outcome_unobservable_equals_zero": (
            primary_outcome_unobservable <= acceptance.maximum_outcome_unobservable
        ),
        "two_x_mean_pair_return_at_least_10_bps": (
            mean is not None
            and mean
            >= acceptance.minimum_2x_slippage_mean_pair_return_bps / 10_000
        ),
        "seven_day_one_sided_basic_lower_above_zero": (
            lower is not None and lower > 0
        ),
        "two_x_profit_factor_at_least_1_25": _profit_factor_gate(
            primary, acceptance.minimum_2x_slippage_profit_factor
        ),
        "required_positive_splits": split_criteria,
        "maximum_asset_positive_pnl_share_at_most_50_percent": (
            maximum_asset_positive_pnl_share is not None
            and maximum_asset_positive_pnl_share
            <= acceptance.maximum_asset_positive_pnl_share
        ),
    }
    split_pass = all(all(values.values()) for values in split_criteria.values())
    all_economic = all(
        bool(value)
        for name, value in criteria.items()
        if name != "required_positive_splits"
    ) and split_pass
    criteria["all_gates_pass"] = all_economic

    if primary_outcome_unobservable > acceptance.maximum_outcome_unobservable:
        status = "INCONCLUSIVE_OUTCOME_UNOBSERVABLE"
    elif (
        primary["completed_episodes"] < acceptance.minimum_completed_episodes
        or bootstrap["valid_replicates"] == 0
        or bootstrap["invalid_fraction"]
        > acceptance.maximum_invalid_bootstrap_fraction
    ):
        status = "INCONCLUSIVE_LOW_INFORMATION"
    elif all_economic:
        status = "EXPLORATORY_SCREEN_PASS"
    else:
        status = "EXPLORATORY_FAIL"
    return status, criteria


def analyze_carry_ledgers(
    decisions: Sequence[CarryEntryDecision],
    trades: Sequence[CarryTrade],
    spec: CarryExperimentSpec,
) -> tuple[dict[str, Any], tuple[DailyPairPnl, ...]]:
    integrity = _validate_carry_ledgers(decisions, trades, spec)
    primary_names = _primary_splits(spec)
    eligible = [item for item in trades if item.analysis_eligible]
    primary_trades = [item for item in eligible if item.split in primary_names]
    daily = build_daily_pair_pnl(trades, spec)
    bootstrap = _shared_calendar_bootstrap(daily, spec)
    base_primary = _metric_summary(primary_trades, stress=False)
    stressed_primary = _metric_summary(primary_trades, stress=True)
    by_split = {
        split.name: {
            "base_costs": _metric_summary(
                [item for item in eligible if item.split == split.name], stress=False
            ),
            "two_x_slippage": _metric_summary(
                [item for item in eligible if item.split == split.name], stress=True
            ),
            "analysis_role": (
                "PRIMARY_COMPONENT" if split.name in primary_names else "DIAGNOSTIC_ONLY"
            ),
        }
        for split in spec.splits
    }
    by_asset = {
        asset.asset: {
            "base_costs": _metric_summary(
                [item for item in primary_trades if item.asset == asset.asset],
                stress=False,
            ),
            "two_x_slippage": _metric_summary(
                [item for item in primary_trades if item.asset == asset.asset],
                stress=True,
            ),
        }
        for asset in spec.assets
    }
    positive_asset_pnl = {
        name: max(0.0, summary["two_x_slippage"]["net_pnl_usdt"])
        for name, summary in by_asset.items()
    }
    total_positive = sum(positive_asset_pnl.values())
    concentration = (
        max(positive_asset_pnl.values()) / total_positive
        if total_positive > 0 and positive_asset_pnl
        else None
    )
    stressed_splits = {
        name: values["two_x_slippage"] for name, values in by_split.items()
    }
    status, gates = evaluate_c1_acceptance(
        primary=stressed_primary,
        bootstrap=bootstrap,
        split_summaries=stressed_splits,
        maximum_asset_positive_pnl_share=concentration,
        primary_outcome_unobservable=integrity["primary_outcome_unobservable"],
        spec=spec,
    )

    rejection_counts: Counter[str] = Counter()
    for decision in decisions:
        rejection_counts.update(decision.rejection_reasons)
    exclusion_counts = Counter(
        item.exclusion_reason for item in trades if not item.analysis_eligible
    )
    exit_counts = Counter(item.exit_reason for item in trades)
    return (
        {
            "schema_version": "c1_analysis_v1",
            "protocol_version": spec.protocol_version,
            "rule_version": spec.rule_version,
            "artifact_role": _ARTIFACT_ROLE,
            "status_axes": {
                "data_integrity": "PASS",
                "efficacy": status,
                "execution_validity": "INCONCLUSIVE_KLINE_NEXT_OPEN_NO_HISTORICAL_BBO",
                "exposure": "EXPOSED_RETROSPECTIVE_ONLY",
                "deployment": "NOT_DEPLOYABLE",
            },
            "integrity": integrity,
            "population": {
                "primary_splits": sorted(primary_names),
                "diagnostic_splits": sorted(
                    split.name for split in spec.splits if split.name not in primary_names
                ),
                "assets": [item.asset for item in spec.assets],
                "analysis_eligible_trades": len(eligible),
                "primary_analysis_eligible_trades": len(primary_trades),
            },
            "cost_contract": {
                "pair_capital_usdt": spec.costs.notional_usdt,
                "base_costs": spec.costs.model_dump(mode="json"),
                "two_x_slippage_formula": "base_net_pnl_usdt - base_slippage_usdt",
            },
            "base_costs_primary": base_primary,
            "primary_two_x_slippage": stressed_primary,
            "bootstrap": bootstrap,
            "by_split": by_split,
            "by_asset": by_asset,
            "asset_positive_pnl_concentration": {
                "positive_pnl_usdt": positive_asset_pnl,
                "maximum_positive_pnl_share": concentration,
            },
            "decision_funnel": {
                "decisions": len(decisions),
                "accepted": sum(item.accepted for item in decisions),
                "rejected": sum(not item.accepted for item in decisions),
                "rejection_reasons": dict(sorted(rejection_counts.items())),
            },
            "trade_diagnostics": {
                "exit_reasons": dict(sorted(exit_counts.items())),
                "analysis_exclusions": dict(sorted(exclusion_counts.items())),
            },
            "acceptance": {
                "thresholds": spec.acceptance.model_dump(mode="json"),
                "gates": gates,
            },
            "multiplicity": {
                "family": _PRIMARY_FAMILY,
                "primary_hypotheses": 1,
                "holm_applied": False,
                "method": "NONE_SINGLE_PRESPECIFIED_PRIMARY",
                "relation_to_r4b": "SEPARATE_EXPERIMENT_FAMILY",
            },
            "limitations": [
                "The full retrospective period was exposed by earlier research.",
                "Historical fills are synchronized 5m next-open proxies without BBO/depth/latency.",
                "A pass is exploratory and cannot approve live trading or order placement.",
                "At least six frozen prospective months are required before reconsideration.",
            ],
        },
        daily,
    )


def _write_decisions(decisions: Sequence[CarryEntryDecision], path: Path) -> None:
    names = [field.name for field in fields(CarryEntryDecision)]
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=names, lineterminator="\n")
        writer.writeheader()
        for item in sorted(
            decisions, key=lambda value: (value.decision_time_ms, value.asset, value.decision_id)
        ):
            row = asdict(item)
            row["rejection_reasons"] = json.dumps(
                list(item.rejection_reasons), ensure_ascii=False, separators=(",", ":")
            )
            writer.writerow(row)


def _write_trades(trades: Sequence[CarryTrade], path: Path) -> None:
    names = [
        *(field.name for field in fields(CarryTrade)),
        "two_x_slippage_pnl_usdt",
        "two_x_slippage_return",
    ]
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=names, lineterminator="\n")
        writer.writeheader()
        for item in sorted(
            trades, key=lambda value: (value.entry_time_ms, value.asset, value.trade_id)
        ):
            stress_pnl = _stress_pnl(item)
            writer.writerow(
                {
                    **asdict(item),
                    "two_x_slippage_pnl_usdt": stress_pnl,
                    "two_x_slippage_return": stress_pnl / item.pair_capital_usdt,
                }
            )


def _write_daily(rows: Sequence[DailyPairPnl], path: Path) -> None:
    names = [field.name for field in fields(DailyPairPnl)]
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=names, lineterminator="\n")
        writer.writeheader()
        for item in rows:
            writer.writerow(asdict(item))


def _format_metric(value: object, *, digits: int = 4) -> str:
    if not isinstance(value, (int, float)) or not math.isfinite(float(value)):
        return "NA"
    return f"{float(value):.{digits}f}"


def render_c1_report_ko(result: Mapping[str, Any]) -> str:
    status = result["status_axes"]
    base = result["base_costs_primary"]
    stress = result["primary_two_x_slippage"]
    bootstrap = result["bootstrap"]
    lines = [
        "# C1 funding/basis carry 노출표본 연구 보고서",
        "",
        f"- 효능 상태: `{status['efficacy']}`",
        f"- 데이터 무결성: `{status['data_integrity']}`",
        f"- 배포 상태: `{status['deployment']}`",
        "- 본 결과는 공개 데이터 기반 PAPER 연구이며 주문 기능을 포함하지 않는다.",
        "",
        "## Primary 결과: base costs",
        "",
        "| 에피소드 | Gross | Fees | Slippage | Funding | Net (USDT) | 평균 (bp) | PF |",
        "|---:|---:|---:|---:|---:|---:|---:|---:|",
        "| {episodes} | {gross} | {fees} | {slip} | {funding} | {pnl} | {mean} | {pf} |".format(
            episodes=base["completed_episodes"],
            gross=_format_metric(base["gross_pnl_usdt"]),
            fees=_format_metric(base["fees_usdt"]),
            slip=_format_metric(base["slippage_usdt"]),
            funding=_format_metric(base["funding_pnl_usdt"]),
            pnl=_format_metric(base["net_pnl_usdt"]),
            mean=_format_metric(base["mean_pair_return_bps"]),
            pf=_format_metric(base["profit_factor"]),
        ),
        "",
        "## Primary 결과: 2x adverse slippage",
        "",
        "| 에피소드 | 순 P&L (USDT) | 평균 pair return (bp) | PF |",
        "|---:|---:|---:|---:|",
        "| {episodes} | {pnl} | {mean} | {pf} |".format(
            episodes=stress["completed_episodes"],
            pnl=_format_metric(stress["net_pnl_usdt"]),
            mean=_format_metric(stress["mean_pair_return_bps"]),
            pf=_format_metric(stress["profit_factor"]),
        ),
        "",
        "## 불확실성",
        "",
        f"- 7일 shared UTC-calendar bootstrap: {bootstrap['samples']:,}회",
        "- 단측 95% basic lower: "
        + _format_metric(
            None
            if bootstrap["one_sided_basic_95_lower"] is None
            else bootstrap["one_sided_basic_95_lower"] * 10_000
        )
        + " bp",
        f"- invalid replicate 비율: {_format_metric(bootstrap['invalid_fraction'] * 100)}%",
        "",
        "## Split별 2x slippage 결과",
        "",
        "| Split | 역할 | 에피소드 | Net (USDT) | 평균 (bp) |",
        "|---|---|---:|---:|---:|",
    ]
    for split_name, values in result["by_split"].items():
        split_summary = values["two_x_slippage"]
        lines.append(
            "| {name} | {role} | {episodes} | {net} | {mean} |".format(
                name=split_name,
                role=values["analysis_role"],
                episodes=split_summary["completed_episodes"],
                net=_format_metric(split_summary["net_pnl_usdt"]),
                mean=_format_metric(split_summary["mean_pair_return_bps"]),
            )
        )
    lines.extend(
        [
            "",
            "## 자산별 primary 2x slippage 결과",
            "",
            "| Asset | 에피소드 | Net (USDT) | 평균 (bp) |",
            "|---|---:|---:|---:|",
        ]
    )
    for asset, values in result["by_asset"].items():
        asset_summary = values["two_x_slippage"]
        lines.append(
            "| {asset} | {episodes} | {net} | {mean} |".format(
                asset=asset,
                episodes=asset_summary["completed_episodes"],
                net=_format_metric(asset_summary["net_pnl_usdt"]),
                mean=_format_metric(asset_summary["mean_pair_return_bps"]),
            )
        )
    lines.extend(
        [
            "",
        "## 동결 수용 게이트",
        "",
        ]
    )
    gates = result["acceptance"]["gates"]
    for name, value in gates.items():
        if name == "required_positive_splits":
            for split_name, split_values in value.items():
                for split_gate, split_value in split_values.items():
                    lines.append(
                        f"- {'PASS' if split_value else 'FAIL'}: {split_name}.{split_gate}"
                    )
        elif name != "all_gates_pass":
            lines.append(f"- {'PASS' if value else 'FAIL'}: {name}")
    lines.extend(
        [
            "",
            "## 해석 한계",
            "",
            "- 2024-07-01~2026-07-01은 이미 노출된 표본이므로 confirmatory 결과가 아니다.",
            "- 동시 5분 next-open 체결은 BBO, 호가 깊이, 충격, 지연을 관찰한 체결이 아니다.",
            "- 통과하더라도 최소 6개월의 별도 동결 prospective BBO shadow가 필요하다.",
            "- 이 scanner에는 실거래 주문 배치 기능이 없다.",
            "",
        ]
    )
    return "\n".join(lines)


def _path_label(path: str | Path, workspace: Path) -> str:
    resolved = Path(path).resolve()
    try:
        return resolved.relative_to(workspace.resolve()).as_posix()
    except ValueError:
        return resolved.name


def write_carry_artifacts(
    *,
    decisions: Sequence[CarryEntryDecision],
    trades: Sequence[CarryTrade],
    daily: Sequence[DailyPairPnl],
    analysis: Mapping[str, Any],
    spec: CarryExperimentSpec,
    spec_path: str | Path,
    output_dir: str | Path,
    workspace_root: str | Path,
    input_hashes: Mapping[str, str],
    started_at_utc: datetime | None = None,
    duration_seconds: float = 0.0,
) -> dict[str, str]:
    for name, digest in input_hashes.items():
        if (
            not name
            or len(digest) != 64
            or any(character not in "0123456789abcdef" for character in digest)
        ):
            raise ValueError("C1 manifest input names and SHA-256 digests must be valid")
    root = Path(output_dir)
    root.mkdir(parents=True, exist_ok=True)
    paths = {name: root / name for name in _OUTPUT_NAMES}
    _write_decisions(decisions, paths["c1_decisions.csv"])
    _write_trades(trades, paths["c1_trades.csv"])
    _write_daily(daily, paths["c1_daily_pair_pnl.csv"])
    paths["c1_analysis.json"].write_text(
        _canonical_json(analysis), encoding="utf-8", newline="\n"
    )
    paths["c1_report_ko.md"].write_text(
        render_c1_report_ko(analysis), encoding="utf-8", newline="\n"
    )

    workspace = Path(workspace_root)
    plan_path = (workspace / spec.experiment_plan_path).resolve()
    if not plan_path.is_file():
        raise FileNotFoundError(f"C1 experiment plan not found: {plan_path}")
    spec_source = Path(spec_path)
    uv_lock = workspace / "uv.lock"
    manifest = {
        "schema_version": "c1_run_manifest_v1",
        "artifact_role": _ARTIFACT_ROLE,
        "protocol_version": spec.protocol_version,
        "rule_version": spec.rule_version,
        "created_at_utc": (started_at_utc or datetime.now(UTC)).isoformat(),
        "spec": {
            "path": _path_label(spec_source, workspace),
            "sha256": sha256_file(spec_source),
        },
        "experiment_plan": {
            "path": _path_label(plan_path, workspace),
            "sha256": sha256_file(plan_path),
        },
        "source_code_sha256": source_code_digest(workspace),
        "environment": {
            "python_version": platform.python_version(),
            "platform": platform.platform(),
            "uv_lock_sha256": sha256_file(uv_lock) if uv_lock.is_file() else None,
        },
        "execution": {
            "argv": sys.argv,
            "working_directory": _path_label(Path.cwd(), workspace),
            "duration_seconds": duration_seconds,
            "exit_code": 0,
        },
        "frozen_contract": {
            "interval": spec.interval,
            "policy": spec.carry.model_dump(mode="json"),
            "costs": spec.costs.model_dump(mode="json"),
            "acceptance": spec.acceptance.model_dump(mode="json"),
            "primary_splits": sorted(_primary_splits(spec)),
            "artifact_has_order_placement": False,
        },
        "multiplicity": analysis["multiplicity"],
        "inputs": dict(sorted(input_hashes.items())),
        "outputs": {name: sha256_file(path) for name, path in sorted(paths.items())},
        "result_status_axes": analysis["status_axes"],
    }
    manifest_path = root / "c1_run_manifest.json"
    manifest_path.write_text(_canonical_json(manifest), encoding="utf-8", newline="\n")
    return {**{name: str(path) for name, path in paths.items()}, "manifest": str(manifest_path)}


def _kline_request(
    spec: CarryExperimentSpec,
    market: Market,
    asset: str,
    symbol: str,
) -> KlineDatasetRequest:
    return KlineDatasetRequest(
        market=market,
        symbol=symbol,
        alias=asset,
        interval=spec.interval,
        start_time_ms=int(spec.data_start.timestamp() * 1000),
        end_time_ms=int(spec.evaluation_end.timestamp() * 1000) - 1,
    )


def _load_kline(
    data_root: Path,
    spec: CarryExperimentSpec,
    market: Market,
    asset: str,
    symbol: str,
    input_hashes: dict[str, str],
) -> KlineDataset:
    path = dataset_path(data_root, market, asset, symbol, spec.interval)
    manifest_path = path.with_suffix(path.suffix + ".manifest.json")
    manifest = read_dataset_manifest(manifest_path)
    verify_dataset_manifest(
        path,
        manifest,
        expected_request=_kline_request(spec, market, asset, symbol),
    )
    _validate_kline_manifest_coverage(manifest, spec, market, asset)
    input_hashes[path.relative_to(data_root).as_posix()] = sha256_file(path)
    input_hashes[manifest_path.relative_to(data_root).as_posix()] = sha256_file(
        manifest_path
    )
    return read_kline_csv(path)


def run_carry_experiment(
    spec_path: str | Path,
    data_dir: str | Path,
    output_dir: str | Path,
    *,
    workspace_root: str | Path,
) -> tuple[dict[str, Any], dict[str, str]]:
    """Run the frozen C1 public-data PAPER experiment without any network or orders."""

    started_at_utc = datetime.now(UTC)
    started_counter = time.perf_counter()
    spec = load_carry_spec(spec_path)
    data_root = Path(data_dir)
    input_hashes: dict[str, str] = {}
    decisions: list[CarryEntryDecision] = []
    trades: list[CarryTrade] = []
    replay_diagnostics: list[dict[str, Any]] = []
    start_ms = int(spec.data_start.timestamp() * 1000)
    end_ms = int(spec.evaluation_end.timestamp() * 1000) - 1
    for asset in spec.assets:
        spot = _load_kline(
            data_root,
            spec,
            Market.SPOT,
            asset.asset,
            asset.spot_symbol,
            input_hashes,
        )
        futures = _load_kline(
            data_root,
            spec,
            Market.FUTURES,
            asset.asset,
            asset.futures_symbol,
            input_hashes,
        )
        funding_file = funding_path(
            data_root, asset.asset, asset.futures_symbol, spec.interval
        )
        funding = verify_funding_dataset(
            funding_file,
            expected_symbol=asset.futures_symbol,
            expected_start_time_ms=start_ms,
            expected_end_time_ms=end_ms,
        )
        _validate_c1_funding_coverage(funding, spec, asset.asset)
        input_hashes[funding_file.relative_to(data_root).as_posix()] = funding_sha256(
            funding_file
        )
        for split in spec.splits:
            run = run_carry_pair(
                protocol_version=spec.protocol_version,
                rule_version=spec.rule_version,
                asset=asset,
                split=split,
                spot=spot,
                futures=futures,
                funding=funding,
                costs=spec.costs,
                policy=spec.carry,
            )
            decisions.extend(run.decisions)
            trades.extend(run.trades)
            replay_diagnostics.append(
                {
                    "asset": asset.asset,
                    "split": split.name,
                    "common_bar_count": run.common_bar_count,
                    "gap_count": run.gap_count,
                    "open_position_at_end": run.open_position_at_end,
                    "decisions": len(run.decisions),
                    "trades": len(run.trades),
                }
            )

    analysis, daily = analyze_carry_ledgers(decisions, trades, spec)
    analysis["replay_diagnostics"] = sorted(
        replay_diagnostics, key=lambda item: (item["asset"], item["split"])
    )
    paths = write_carry_artifacts(
        decisions=decisions,
        trades=trades,
        daily=daily,
        analysis=analysis,
        spec=spec,
        spec_path=spec_path,
        output_dir=output_dir,
        workspace_root=workspace_root,
        input_hashes=input_hashes,
        started_at_utc=started_at_utc,
        duration_seconds=time.perf_counter() - started_counter,
    )
    return analysis, paths
