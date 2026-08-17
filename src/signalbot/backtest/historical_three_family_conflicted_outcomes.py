"""Fixed-horizon outcomes for the separately authorized conflicted comparator.

This sibling never recasts conflicted rows as clean consensus.  It consumes the
exact adapter artifact set, authenticates the same recorded kline and funding
authorities, and reuses the public execution/funding arithmetic owners.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import io
import json
import os
import shutil
import tempfile
from collections.abc import Mapping, Sequence
from dataclasses import asdict, dataclass, fields
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Final, Literal, cast

from signalbot.backtest.dataset import KlineDataset
from signalbot.backtest.downstream_code_freeze import (
    DownstreamCodeFreezeAuthorityV1,
    DownstreamCodeFreezeErrorV1,
    load_downstream_code_freeze_v1,
)
from signalbot.backtest.engine import calculate_execution_returns, calculate_funding_return
from signalbot.backtest.funding import FundingDataset
from signalbot.backtest.historical_three_family_conflicted_adapter import (
    HISTORICAL_THREE_FAMILY_CONFLICTED_ADAPTER_RULE_VERSION_V1,
    HISTORICAL_THREE_FAMILY_CONFLICTED_TOPOLOGY_VERSION_V1,
    HistoricalConflictedComparatorEventV1,
    HistoricalThreeFamilyConflictedAdapterErrorV1,
    LoadedHistoricalConflictedAdapterArtifactsV1,
    load_authenticated_historical_conflicted_adapter_artifacts_v1,
)
from signalbot.backtest.historical_three_family_outcomes import (
    HISTORICAL_THREE_FAMILY_CENSUS_CODE_FREEZE_SHA256_V2,
    HISTORICAL_THREE_FAMILY_FUNDING_AUTHORITY_PROTOCOL_V2,
    HistoricalFixedHorizonExclusionV2,
    HistoricalOutcomeKlineAuthorityV2,
    LoadedHistoricalFundingAuthorityV2,
    historical_return_to_micros_v2,
    historical_three_family_split_bounds_v2,
    load_authenticated_historical_funding_authority_v2,
    load_authenticated_historical_kline_panel_v2,
)
from signalbot.domain.enums import Direction, Market
from signalbot.domain.models import Candle
from signalbot.r4b_v2.canonical import canonical_json_line
from signalbot.r4b_v2.protocol.decision_clock import FIVE_MINUTE_MS_V2
from signalbot.r4b_v2.research.historical_three_family_bootstrap import (
    HISTORICAL_THREE_FAMILY_CONFLICTED_OUTCOME_PROTOCOL_V2,
    HISTORICAL_THREE_FAMILY_FULL_CALENDAR_SCHEDULE_SHA256_V2,
    HistoricalThreeFamilyConflictedOutcomeV2,
)
from signalbot.r4b_v2.research.historical_three_family_outcome_audit import (
    HISTORICAL_THREE_FAMILY_OUTCOME_HORIZONS_BARS_V2,
    HistoricalThreeFamilySideV2,
)
from signalbot.r4b_v2.research.historical_three_family_topology import (
    HISTORICAL_THREE_FAMILY_TOPOLOGY_RULE_VERSION_V2,
)
from signalbot.r4b_v2.strategy.historical_three_family_consensus import (
    HistoricalExecutionContractV2,
    build_historical_execution_contract_v2,
)

HISTORICAL_CONFLICTED_FIXED_HORIZON_RUNNER_PROTOCOL_V1: Final = (
    "historical_three_family_conflicted_fixed_horizon_outcomes_v1_2026-07-20"
)
HISTORICAL_CONFLICTED_FIXED_HORIZON_SCHEMA_VERSION_V1: Final = 1
HISTORICAL_CONFLICTED_FIXED_HORIZON_COST_SUMMARY_V1: Final = (
    "R4B_CAUSAL_V2_HISTORICAL_CONFLICTED_COST_ATTRIBUTION_V1_FROZEN"
)
_OUTCOME_NAMES: Final = ("fixed_horizon_outcomes.csv", "results.json")
_PUBLISHED_NAMES: Final = frozenset((*_OUTCOME_NAMES, "manifest.json"))
_SHA256_LENGTH: Final = 64
_MAX_ROWS: Final = 1_000_000
_FROZEN_ASSETS: Final = frozenset({"ARB", "BONK", "ENA", "FLOKI", "OP", "SEI", "WIF"})


class HistoricalConflictedFixedHorizonErrorV1(ValueError):
    """Raised when the separate conflicted outcome contract is violated."""


@dataclass(frozen=True, slots=True)
class HistoricalConflictedFixedHorizonOutcomeRowV1:
    """One after-cost horizon result without any clean-topology representation."""

    topology_version: str
    adapter_rule_version: str
    split: str
    asset: str
    symbol: str
    event_id: str
    anchor_sha256: str
    source_census_row_sha256: str
    adapter_contract_sha256: str
    adapter_manifest_sha256: str
    downstream_freeze_manifest_sha256: str
    topology_rule_version: str
    execution_contract_sha256: str
    agreement_bucket: str
    primary_direction: str
    directional_agreement_micros: int
    decision_time_ms: int
    horizon_bars: int
    horizon_minutes: int
    expected_entry_time_ms: int
    expected_exit_close_time_ms: int
    entry_price: Decimal | None
    exit_price: Decimal | None
    gross_directional_return_micros: int | None
    slippage_return_micros: int | None
    fee_return_micros: int | None
    funding_return_micros: int | None
    rounding_residual_micros: int | None
    total_cost_micros: int | None
    funding_event_count: int | None
    evaluable: bool
    exclusion_reason: str
    net_return_micros: int | None
    historical_only: Literal[True] = True
    conflicted_comparator: Literal[True] = True
    clean_population_pooled: Literal[False] = False
    probability: Literal[False] = False
    probability_calibrated: Literal[False] = False
    promoting: Literal[False] = False
    order_placement: Literal[False] = False

    def __post_init__(self) -> None:
        _validate_outcome_row(self)

    def to_bootstrap_outcome(self) -> HistoricalThreeFamilyConflictedOutcomeV2:
        """Project the exact subset owned by the existing bootstrap contract."""

        return HistoricalThreeFamilyConflictedOutcomeV2(
            event_id=self.event_id,
            comparator_protocol_version=HISTORICAL_THREE_FAMILY_CONFLICTED_OUTCOME_PROTOCOL_V2,
            topology_rule_version=self.topology_rule_version,
            execution_contract_sha256=self.execution_contract_sha256,
            symbol=self.symbol,
            decision_time_ms=self.decision_time_ms,
            side=(
                HistoricalThreeFamilySideV2.BULLISH
                if self.primary_direction == Direction.LONG.value
                else HistoricalThreeFamilySideV2.BEARISH
            ),
            directional_agreement_micros=self.directional_agreement_micros,
            horizon_bars=self.horizon_bars,
            evaluable=self.evaluable,
            exclusion_reason=self.exclusion_reason,
            net_return_micros=self.net_return_micros,
        )


@dataclass(frozen=True, slots=True)
class HistoricalConflictedFixedHorizonArtifactsV1:
    output_dir: Path
    outcomes_sha256: str
    results_sha256: str
    manifest_sha256: str
    event_count: int
    outcome_rows: int


@dataclass(frozen=True, slots=True)
class LoadedHistoricalConflictedFixedHorizonArtifactsV1:
    """Authenticated public input for downstream analysis and bootstrap."""

    artifact_dir: Path
    manifest_sha256: str
    outcomes_sha256: str
    results_sha256: str
    rows: tuple[HistoricalConflictedFixedHorizonOutcomeRowV1, ...]
    adapter_manifest_sha256: str
    execution_contract_sha256: str
    downstream_code_freeze_manifest_sha256: str
    funding_authority_manifest_sha256: str
    census_manifest_sha256: str
    historical_only: Literal[True] = True
    conflicted_comparator: Literal[True] = True
    clean_population_pooled: Literal[False] = False
    probability: Literal[False] = False
    probability_calibrated: Literal[False] = False
    promoting: Literal[False] = False
    order_placement: Literal[False] = False


@dataclass(frozen=True, slots=True)
class HistoricalConflictedCostSummaryV1:
    horizon_bars: int
    horizon_minutes: int
    side: str
    events: int
    evaluable: int
    coverage_micros: int
    gross_strict_hits: int
    net_strict_hits: int
    mean_gross_return_micros: int | None
    mean_slippage_return_micros: int | None
    mean_fee_return_micros: int | None
    mean_funding_return_micros: int | None
    mean_total_cost_micros: int | None
    mean_net_return_micros: int | None
    historical_only: Literal[True] = True
    conflicted_comparator: Literal[True] = True
    probability: Literal[False] = False
    promoting: Literal[False] = False


def evaluate_historical_conflicted_fixed_horizons_v1(
    event: HistoricalConflictedComparatorEventV1,
    dataset: KlineDataset,
    funding: FundingDataset | None,
    *,
    adapter_manifest_sha256: str,
    downstream_freeze_manifest_sha256: str,
    execution_contract: HistoricalExecutionContractV2 | None = None,
) -> tuple[HistoricalConflictedFixedHorizonOutcomeRowV1, ...]:
    """Evaluate the five frozen horizons using public cost/funding owners."""

    if type(event) is not HistoricalConflictedComparatorEventV1:
        raise HistoricalConflictedFixedHorizonErrorV1(
            "event must be an exact authorized conflicted adapter event"
        )
    if type(dataset) is not KlineDataset:
        raise HistoricalConflictedFixedHorizonErrorV1("dataset must be an exact KlineDataset")
    _require_sha256(adapter_manifest_sha256, "adapter manifest")
    _require_sha256(downstream_freeze_manifest_sha256, "downstream freeze")
    contract = execution_contract or build_historical_execution_contract_v2()
    if type(contract) is not HistoricalExecutionContractV2:
        raise HistoricalConflictedFixedHorizonErrorV1(
            "execution contract must be the exact public owner type"
        )
    if contract.execution_contract_sha256 != event.execution_contract_sha256:
        raise HistoricalConflictedFixedHorizonErrorV1(
            "event and execution owner contracts differ"
        )
    request = dataset.request
    if (
        request.market is not Market.FUTURES
        or request.symbol != event.symbol
        or request.dataset_alias != event.asset
        or request.interval != "5m"
    ):
        raise HistoricalConflictedFixedHorizonErrorV1(
            "kline dataset identity differs from the adapter event"
        )
    if funding is not None and funding.symbol != event.symbol:
        raise HistoricalConflictedFixedHorizonErrorV1(
            "funding dataset identity differs from the adapter event"
        )
    by_open = {candle.open_time_ms: candle for candle in dataset.candles}
    if len(by_open) != len(dataset.candles):
        raise HistoricalConflictedFixedHorizonErrorV1(
            "outcome dataset contains duplicate open times"
        )
    decision_open_ms = event.decision_time_ms - FIVE_MINUTE_MS_V2 + 1
    decision_candle = by_open.get(decision_open_ms)
    if decision_candle is not None and decision_candle.close != Decimal(event.decision_price):
        raise HistoricalConflictedFixedHorizonErrorV1(
            "adapter decision price differs from the authenticated kline"
        )
    return tuple(
        _evaluate_one_horizon(
            event=event,
            dataset=dataset,
            by_open=by_open,
            decision_candle_present=decision_candle is not None,
            funding=funding,
            execution_contract=contract,
            horizon_bars=horizon,
            adapter_manifest_sha256=adapter_manifest_sha256,
            downstream_freeze_manifest_sha256=downstream_freeze_manifest_sha256,
        )
        for horizon in HISTORICAL_THREE_FAMILY_OUTCOME_HORIZONS_BARS_V2
    )


def run_historical_conflicted_fixed_horizons_v1(
    *,
    adapter_artifact_dir: str | Path,
    expected_adapter_manifest_sha256: str,
    consensus_path: str | Path,
    census_manifest_path: str | Path,
    expected_census_manifest_sha256: str,
    expected_experiment_contract_sha256: str,
    expected_topology_amendment_sha256: str,
    workspace_root: str | Path,
    adapter_contract_path: str | Path,
    expected_adapter_contract_sha256: str,
    adapter_code_freeze_manifest_path: str | Path,
    expected_adapter_code_freeze_manifest_sha256: str,
    downstream_code_freeze_manifest_path: str | Path,
    expected_downstream_code_freeze_manifest_sha256: str,
    funding_authority_manifest_path: str | Path,
    expected_funding_authority_manifest_sha256: str,
    data_root: str | Path,
    output_dir: str | Path,
) -> HistoricalConflictedFixedHorizonArtifactsV1:
    """Authenticate all pre-outcome authorities, evaluate, and publish atomically."""

    root = Path(workspace_root).resolve()
    downstream_freeze = _load_downstream_code_freeze(
        workspace_root=root,
        manifest_path=downstream_code_freeze_manifest_path,
        expected_manifest_sha256=expected_downstream_code_freeze_manifest_sha256,
        census_manifest_sha256=expected_census_manifest_sha256,
        experiment_contract_sha256=expected_experiment_contract_sha256,
        topology_amendment_sha256=expected_topology_amendment_sha256,
        adapter_code_freeze_manifest_sha256=(
            expected_adapter_code_freeze_manifest_sha256
        ),
        adapter_manifest_sha256=expected_adapter_manifest_sha256,
        adapter_contract_sha256=expected_adapter_contract_sha256,
        funding_authority_manifest_sha256=expected_funding_authority_manifest_sha256,
    )
    try:
        adapter = load_authenticated_historical_conflicted_adapter_artifacts_v1(
            adapter_artifact_dir=adapter_artifact_dir,
            expected_adapter_manifest_sha256=expected_adapter_manifest_sha256,
            consensus_path=consensus_path,
            census_manifest_path=census_manifest_path,
            expected_census_manifest_sha256=expected_census_manifest_sha256,
            expected_experiment_contract_sha256=expected_experiment_contract_sha256,
            expected_topology_amendment_sha256=expected_topology_amendment_sha256,
            workspace_root=root,
            adapter_contract_path=adapter_contract_path,
            expected_adapter_contract_sha256=expected_adapter_contract_sha256,
            code_freeze_manifest_path=adapter_code_freeze_manifest_path,
            expected_code_freeze_manifest_sha256=(
                expected_adapter_code_freeze_manifest_sha256
            ),
        )
    except HistoricalThreeFamilyConflictedAdapterErrorV1 as exc:
        raise HistoricalConflictedFixedHorizonErrorV1(
            "adapter artifact authentication failed"
        ) from exc
    source = adapter.authorization.source
    try:
        kline_panel = load_authenticated_historical_kline_panel_v2(source, data_root)
        funding_authority = load_authenticated_historical_funding_authority_v2(
            funding_authority_manifest_path,
            expected_manifest_sha256=expected_funding_authority_manifest_sha256,
            data_root=data_root,
        )
    except ValueError as exc:
        raise HistoricalConflictedFixedHorizonErrorV1(
            "recorded kline or funding authority authentication failed"
        ) from exc
    datasets = kline_panel.by_asset()
    funding = funding_authority.by_symbol()
    contract = build_historical_execution_contract_v2()
    rows = tuple(
        row
        for event in adapter.authorization.events
        for row in evaluate_historical_conflicted_fixed_horizons_v1(
            event,
            datasets[event.asset],
            funding.get(event.symbol),
            adapter_manifest_sha256=adapter.adapter_manifest_sha256,
            downstream_freeze_manifest_sha256=(
                downstream_freeze.manifest_sha256
            ),
            execution_contract=contract,
        )
    )
    _validate_complete_rows(adapter.authorization.events, rows)
    outcomes_raw = _outcomes_csv_bytes(rows)
    results_raw = canonical_json_line(
        _results_document(
            adapter=adapter,
            funding=funding_authority,
            rows=rows,
            outcomes_sha256=_sha256_bytes(outcomes_raw),
            downstream_freeze_manifest_sha256=(
                downstream_freeze.manifest_sha256
            ),
        )
    )
    manifest_raw = canonical_json_line(
        _manifest_document(
            adapter=adapter,
            funding=funding_authority,
            kline_authority=kline_panel.authorities,
            outcomes_sha256=_sha256_bytes(outcomes_raw),
            results_sha256=_sha256_bytes(results_raw),
            downstream_freeze_manifest_sha256=(
                downstream_freeze.manifest_sha256
            ),
        )
    )
    target = Path(output_dir).resolve()
    _publish(
        target,
        {
            "fixed_horizon_outcomes.csv": outcomes_raw,
            "results.json": results_raw,
            "manifest.json": manifest_raw,
        },
    )
    return HistoricalConflictedFixedHorizonArtifactsV1(
        output_dir=target,
        outcomes_sha256=_sha256_bytes(outcomes_raw),
        results_sha256=_sha256_bytes(results_raw),
        manifest_sha256=_sha256_bytes(manifest_raw),
        event_count=len(adapter.authorization.events),
        outcome_rows=len(rows),
    )


def load_authenticated_historical_conflicted_fixed_horizon_artifacts_v1(
    artifact_dir: str | Path,
    *,
    expected_manifest_sha256: str,
    expected_adapter_manifest_sha256: str,
    expected_execution_contract_sha256: str,
    expected_downstream_code_freeze_manifest_sha256: str,
    expected_funding_authority_manifest_sha256: str,
    expected_census_manifest_sha256: str,
) -> LoadedHistoricalConflictedFixedHorizonArtifactsV1:
    """Load exact conflicted results for separate downstream analysis."""

    for value, label in (
        (expected_manifest_sha256, "conflicted outcome manifest"),
        (expected_adapter_manifest_sha256, "adapter manifest"),
        (expected_execution_contract_sha256, "execution contract"),
        (expected_downstream_code_freeze_manifest_sha256, "downstream code freeze"),
        (expected_funding_authority_manifest_sha256, "funding authority"),
        (expected_census_manifest_sha256, "census manifest"),
    ):
        _require_sha256(value, label)
    source = Path(artifact_dir).resolve()
    try:
        names = {path.name for path in source.iterdir()}
    except OSError as exc:
        raise HistoricalConflictedFixedHorizonErrorV1(
            "cannot enumerate conflicted outcome artifact directory"
        ) from exc
    if names != _PUBLISHED_NAMES:
        raise HistoricalConflictedFixedHorizonErrorV1(
            "conflicted outcome directory must contain exactly three files"
        )
    manifest_raw = _read_bytes(source / "manifest.json", "conflicted outcome manifest")
    if _sha256_bytes(manifest_raw) != expected_manifest_sha256:
        raise HistoricalConflictedFixedHorizonErrorV1(
            "conflicted outcome manifest differs from the external hash"
        )
    manifest = _decode_canonical_object(manifest_raw, "conflicted outcome manifest")
    _validate_loaded_manifest(
        manifest,
        expected_adapter_manifest_sha256,
        expected_execution_contract_sha256,
        expected_downstream_code_freeze_manifest_sha256,
        expected_funding_authority_manifest_sha256,
        expected_census_manifest_sha256,
    )
    outputs = _require_object(manifest.get("outputs"), "conflicted outputs")
    outcomes_raw = _read_bytes(source / "fixed_horizon_outcomes.csv", "conflicted outcomes")
    results_raw = _read_bytes(source / "results.json", "conflicted results")
    outcomes_sha = _require_sha256(outputs.get("fixed_horizon_outcomes.csv"), "outcomes")
    results_sha = _require_sha256(outputs.get("results.json"), "results")
    if _sha256_bytes(outcomes_raw) != outcomes_sha or _sha256_bytes(results_raw) != results_sha:
        raise HistoricalConflictedFixedHorizonErrorV1(
            "conflicted output bytes differ from manifest hashes"
        )
    rows = _parse_outcomes_csv(outcomes_raw)
    if any(
        row.adapter_manifest_sha256 != expected_adapter_manifest_sha256
        or row.execution_contract_sha256 != expected_execution_contract_sha256
        or row.downstream_freeze_manifest_sha256
        != expected_downstream_code_freeze_manifest_sha256
        for row in rows
    ):
        raise HistoricalConflictedFixedHorizonErrorV1(
            "conflicted rows differ from manifest-level adapter, execution, or freeze authority"
        )
    _validate_loaded_results(
        _decode_canonical_object(results_raw, "conflicted results"),
        rows=rows,
        outcomes_sha256=outcomes_sha,
        adapter_manifest_sha256=expected_adapter_manifest_sha256,
        execution_contract_sha256=expected_execution_contract_sha256,
        funding_authority_manifest_sha256=(
            expected_funding_authority_manifest_sha256
        ),
        downstream_freeze_manifest_sha256=(
            expected_downstream_code_freeze_manifest_sha256
        ),
        census_manifest_sha256=expected_census_manifest_sha256,
    )
    _validate_complete_loaded_rows(rows)
    return LoadedHistoricalConflictedFixedHorizonArtifactsV1(
        artifact_dir=source,
        manifest_sha256=expected_manifest_sha256,
        outcomes_sha256=outcomes_sha,
        results_sha256=results_sha,
        rows=rows,
        adapter_manifest_sha256=expected_adapter_manifest_sha256,
        execution_contract_sha256=expected_execution_contract_sha256,
        downstream_code_freeze_manifest_sha256=(
            expected_downstream_code_freeze_manifest_sha256
        ),
        funding_authority_manifest_sha256=expected_funding_authority_manifest_sha256,
        census_manifest_sha256=expected_census_manifest_sha256,
    )


def _evaluate_one_horizon(
    *,
    event: HistoricalConflictedComparatorEventV1,
    dataset: KlineDataset,
    by_open: Mapping[int, Candle],
    decision_candle_present: bool,
    funding: FundingDataset | None,
    execution_contract: HistoricalExecutionContractV2,
    horizon_bars: int,
    adapter_manifest_sha256: str,
    downstream_freeze_manifest_sha256: str,
) -> HistoricalConflictedFixedHorizonOutcomeRowV1:
    expected_entry_ms = event.decision_time_ms + 1
    expected_exit_open_ms = expected_entry_ms + (horizon_bars - 1) * FIVE_MINUTE_MS_V2
    expected_exit_close_ms = expected_exit_open_ms + FIVE_MINUTE_MS_V2 - 1
    split_start_ms, split_end_ms = historical_three_family_split_bounds_v2(event.split)
    exclusion: HistoricalFixedHorizonExclusionV2 | None = None
    if not split_start_ms <= expected_entry_ms < split_end_ms:
        exclusion = HistoricalFixedHorizonExclusionV2.SPLIT_BOUNDARY_ENTRY
    elif expected_exit_close_ms >= split_end_ms:
        exclusion = HistoricalFixedHorizonExclusionV2.HORIZON_CROSSES_SPLIT
    elif not decision_candle_present:
        exclusion = HistoricalFixedHorizonExclusionV2.MISSING_DECISION_BAR
    entry = by_open.get(expected_entry_ms)
    exit_candle = by_open.get(expected_exit_open_ms)
    if exclusion is None and entry is None:
        exclusion = HistoricalFixedHorizonExclusionV2.MISSING_NEXT_OPEN
    if exclusion is None:
        expected_opens = range(
            expected_entry_ms,
            expected_exit_open_ms + FIVE_MINUTE_MS_V2,
            FIVE_MINUTE_MS_V2,
        )
        missing = tuple(value for value in expected_opens if value not in by_open)
        if missing:
            exclusion = (
                HistoricalFixedHorizonExclusionV2.MISSING_HORIZON_CLOSE
                if expected_exit_open_ms > dataset.candles[-1].open_time_ms
                else HistoricalFixedHorizonExclusionV2.DATA_GAP_IN_HORIZON
            )
    if exclusion is None and exit_candle is None:
        exclusion = HistoricalFixedHorizonExclusionV2.MISSING_HORIZON_CLOSE
    if exclusion is None and funding is None:
        exclusion = HistoricalFixedHorizonExclusionV2.FUNDING_DATASET_UNAVAILABLE
    if (
        exclusion is None
        and funding is not None
        and not (
            funding.start_time_ms <= expected_entry_ms
            and funding.end_time_ms >= expected_exit_close_ms
        )
    ):
        exclusion = HistoricalFixedHorizonExclusionV2.FUNDING_COVERAGE_UNAVAILABLE
    common = {
        "topology_version": event.topology_version,
        "adapter_rule_version": event.adapter_rule_version,
        "split": event.split,
        "asset": event.asset,
        "symbol": event.symbol,
        "event_id": event.event_id,
        "anchor_sha256": event.anchor_sha256,
        "source_census_row_sha256": event.source_census_row_sha256,
        "adapter_contract_sha256": event.adapter_contract_sha256,
        "adapter_manifest_sha256": adapter_manifest_sha256,
        "downstream_freeze_manifest_sha256": downstream_freeze_manifest_sha256,
        "topology_rule_version": HISTORICAL_THREE_FAMILY_TOPOLOGY_RULE_VERSION_V2,
        "execution_contract_sha256": execution_contract.execution_contract_sha256,
        "agreement_bucket": "CONFLICTED_2_VS_1",
        "primary_direction": event.primary_direction,
        "directional_agreement_micros": event.directional_agreement_micros,
        "decision_time_ms": event.decision_time_ms,
        "horizon_bars": horizon_bars,
        "horizon_minutes": horizon_bars * 5,
        "expected_entry_time_ms": expected_entry_ms,
        "expected_exit_close_time_ms": expected_exit_close_ms,
    }
    if exclusion is not None:
        return HistoricalConflictedFixedHorizonOutcomeRowV1(
            **common,
            entry_price=None if entry is None else entry.open,
            exit_price=None if exit_candle is None else exit_candle.close,
            gross_directional_return_micros=None,
            slippage_return_micros=None,
            fee_return_micros=None,
            funding_return_micros=None,
            rounding_residual_micros=None,
            total_cost_micros=None,
            funding_event_count=None,
            evaluable=False,
            exclusion_reason=exclusion.value,
            net_return_micros=None,
        )
    if entry is None or exit_candle is None or funding is None:  # pragma: no cover
        raise HistoricalConflictedFixedHorizonErrorV1(
            "internal conflicted outcome eligibility is inconsistent"
        )
    direction = Direction(event.primary_direction)
    execution = calculate_execution_returns(
        direction,
        float(entry.open),
        float(exit_candle.close),
        float(execution_contract.fee_bps_per_side),
        float(execution_contract.slippage_bps_per_side),
    )
    rates = list(funding.rates)
    funding_return = calculate_funding_return(
        direction,
        expected_entry_ms,
        expected_exit_close_ms,
        float(entry.open),
        rates,
    )
    gross = historical_return_to_micros_v2(execution.gross_return)
    slippage = historical_return_to_micros_v2(execution.slippage_return)
    fee = historical_return_to_micros_v2(execution.fee_return)
    funding_micros = historical_return_to_micros_v2(funding_return)
    net = historical_return_to_micros_v2(execution.net_before_funding + funding_return)
    residual = net - (gross - slippage - fee + funding_micros)
    total_cost = slippage + fee - residual
    funding_events = sum(
        expected_entry_ms < item.funding_time_ms < expected_exit_close_ms for item in rates
    )
    return HistoricalConflictedFixedHorizonOutcomeRowV1(
        **common,
        entry_price=entry.open,
        exit_price=exit_candle.close,
        gross_directional_return_micros=gross,
        slippage_return_micros=slippage,
        fee_return_micros=fee,
        funding_return_micros=funding_micros,
        rounding_residual_micros=residual,
        total_cost_micros=total_cost,
        funding_event_count=funding_events,
        evaluable=True,
        exclusion_reason="",
        net_return_micros=net,
    )


def _validate_outcome_row(row: HistoricalConflictedFixedHorizonOutcomeRowV1) -> None:
    if (
        row.topology_version != HISTORICAL_THREE_FAMILY_CONFLICTED_TOPOLOGY_VERSION_V1
        or row.adapter_rule_version
        != HISTORICAL_THREE_FAMILY_CONFLICTED_ADAPTER_RULE_VERSION_V1
        or row.topology_rule_version != HISTORICAL_THREE_FAMILY_TOPOLOGY_RULE_VERSION_V2
        or row.agreement_bucket != "CONFLICTED_2_VS_1"
        or row.historical_only is not True
        or row.conflicted_comparator is not True
        or row.clean_population_pooled is not False
        or row.probability is not False
        or row.probability_calibrated is not False
        or row.promoting is not False
        or row.order_placement is not False
    ):
        raise HistoricalConflictedFixedHorizonErrorV1(
            "conflicted outcome topology or fixed claims differ"
        )
    for value, label in (
        (row.event_id, "event_id"),
        (row.anchor_sha256, "anchor"),
        (row.source_census_row_sha256, "source census row"),
        (row.adapter_contract_sha256, "adapter contract"),
        (row.adapter_manifest_sha256, "adapter manifest"),
        (row.downstream_freeze_manifest_sha256, "downstream freeze"),
        (row.execution_contract_sha256, "execution contract"),
    ):
        _require_sha256(value, label)
    if row.execution_contract_sha256 != (
        build_historical_execution_contract_v2().execution_contract_sha256
    ):
        raise HistoricalConflictedFixedHorizonErrorV1(
            "conflicted row execution contract differs from the public owner"
        )
    if row.horizon_bars not in HISTORICAL_THREE_FAMILY_OUTCOME_HORIZONS_BARS_V2:
        raise HistoricalConflictedFixedHorizonErrorV1("unsupported conflicted horizon")
    if row.horizon_minutes != row.horizon_bars * 5:
        raise HistoricalConflictedFixedHorizonErrorV1("horizon minutes are inconsistent")
    if row.expected_entry_time_ms != row.decision_time_ms + 1:
        raise HistoricalConflictedFixedHorizonErrorV1("entry is not next contiguous open")
    if row.expected_exit_close_time_ms != (
        row.decision_time_ms + row.horizon_bars * FIVE_MINUTE_MS_V2
    ):
        raise HistoricalConflictedFixedHorizonErrorV1("exit close time is inconsistent")
    if row.primary_direction not in {Direction.LONG.value, Direction.SHORT.value}:
        raise HistoricalConflictedFixedHorizonErrorV1("unsupported conflicted side")
    if not -1_000_000 <= row.directional_agreement_micros <= 1_000_000:
        raise HistoricalConflictedFixedHorizonErrorV1(
            "conflicted weighted agreement is outside its descriptive range"
        )
    economic = (
        row.gross_directional_return_micros,
        row.slippage_return_micros,
        row.fee_return_micros,
        row.funding_return_micros,
        row.rounding_residual_micros,
        row.total_cost_micros,
        row.funding_event_count,
        row.net_return_micros,
    )
    if row.evaluable:
        if row.exclusion_reason or any(type(value) is not int for value in economic):
            raise HistoricalConflictedFixedHorizonErrorV1(
                "evaluable conflicted row requires exact economics"
            )
        if row.entry_price is None or row.exit_price is None:
            raise HistoricalConflictedFixedHorizonErrorV1(
                "evaluable conflicted row requires prices"
            )
        gross, slippage, fee, funding, residual, total_cost, funding_count, net = cast(
            tuple[int, ...], economic
        )
        if total_cost != slippage + fee - residual or net != gross - total_cost + funding:
            raise HistoricalConflictedFixedHorizonErrorV1(
                "conflicted cost components do not reconcile"
            )
        if slippage < 0 or fee < 0 or total_cost < 0 or funding_count < 0:
            raise HistoricalConflictedFixedHorizonErrorV1(
                "conflicted costs or funding count are negative"
            )
    else:
        allowed = {value.value for value in HistoricalFixedHorizonExclusionV2}
        if row.exclusion_reason not in allowed or any(value is not None for value in economic):
            raise HistoricalConflictedFixedHorizonErrorV1(
                "excluded conflicted row requires one reason and no economics"
            )


def _summaries(
    rows: Sequence[HistoricalConflictedFixedHorizonOutcomeRowV1],
) -> tuple[HistoricalConflictedCostSummaryV1, ...]:
    result: list[HistoricalConflictedCostSummaryV1] = []
    for horizon in HISTORICAL_THREE_FAMILY_OUTCOME_HORIZONS_BARS_V2:
        for side in (Direction.LONG.value, Direction.SHORT.value):
            cell = [
                row
                for row in rows
                if row.horizon_bars == horizon and row.primary_direction == side
            ]
            evaluable = [row for row in cell if row.evaluable]
            result.append(
                HistoricalConflictedCostSummaryV1(
                    horizon_bars=horizon,
                    horizon_minutes=horizon * 5,
                    side="BULLISH" if side == Direction.LONG.value else "BEARISH",
                    events=len(cell),
                    evaluable=len(evaluable),
                    coverage_micros=_rate_micros(len(evaluable), len(cell)),
                    gross_strict_hits=sum(
                        cast(int, row.gross_directional_return_micros) > 0
                        for row in evaluable
                    ),
                    net_strict_hits=sum(
                        cast(int, row.net_return_micros) > 0 for row in evaluable
                    ),
                    mean_gross_return_micros=_mean(
                        [cast(int, row.gross_directional_return_micros) for row in evaluable]
                    ),
                    mean_slippage_return_micros=_mean(
                        [cast(int, row.slippage_return_micros) for row in evaluable]
                    ),
                    mean_fee_return_micros=_mean(
                        [cast(int, row.fee_return_micros) for row in evaluable]
                    ),
                    mean_funding_return_micros=_mean(
                        [cast(int, row.funding_return_micros) for row in evaluable]
                    ),
                    mean_total_cost_micros=_mean(
                        [cast(int, row.total_cost_micros) for row in evaluable]
                    ),
                    mean_net_return_micros=_mean(
                        [cast(int, row.net_return_micros) for row in evaluable]
                    ),
                )
            )
    return tuple(result)


def _results_document(
    *,
    adapter: LoadedHistoricalConflictedAdapterArtifactsV1,
    funding: LoadedHistoricalFundingAuthorityV2,
    rows: tuple[HistoricalConflictedFixedHorizonOutcomeRowV1, ...],
    outcomes_sha256: str,
    downstream_freeze_manifest_sha256: str,
) -> dict[str, object]:
    return {
        "adapter_manifest_sha256": adapter.adapter_manifest_sha256,
        "clean_population_pooled": False,
        "conflicted_comparator": True,
        "census_manifest_sha256": adapter.authorization.source.census_manifest_sha256,
        "cost_attribution": [asdict(value) for value in _summaries(rows)],
        "cost_summary_version": HISTORICAL_CONFLICTED_FIXED_HORIZON_COST_SUMMARY_V1,
        "downstream_freeze_manifest_sha256": downstream_freeze_manifest_sha256,
        "event_count": len(adapter.authorization.events),
        "execution_contract_sha256": (
            build_historical_execution_contract_v2().execution_contract_sha256
        ),
        "funding_authority_manifest_sha256": funding.manifest_sha256,
        "historical_only": True,
        "horizons_bars": list(HISTORICAL_THREE_FAMILY_OUTCOME_HORIZONS_BARS_V2),
        "order_placement": False,
        "outcome_rows": len(rows),
        "outcomes_sha256": outcomes_sha256,
        "probability": False,
        "probability_calibrated": False,
        "promoting": False,
        "protocol": HISTORICAL_CONFLICTED_FIXED_HORIZON_RUNNER_PROTOCOL_V1,
        "schema_version": HISTORICAL_CONFLICTED_FIXED_HORIZON_SCHEMA_VERSION_V1,
    }


def _manifest_document(
    *,
    adapter: LoadedHistoricalConflictedAdapterArtifactsV1,
    funding: LoadedHistoricalFundingAuthorityV2,
    kline_authority: Sequence[HistoricalOutcomeKlineAuthorityV2],
    outcomes_sha256: str,
    results_sha256: str,
    downstream_freeze_manifest_sha256: str,
) -> dict[str, object]:
    return {
        "adapter_manifest_sha256": adapter.adapter_manifest_sha256,
        "clean_population_pooled": False,
        "conflicted_comparator": True,
        "census_manifest_sha256": adapter.authorization.source.census_manifest_sha256,
        "downstream_freeze_manifest_sha256": downstream_freeze_manifest_sha256,
        "execution_contract_sha256": (
            build_historical_execution_contract_v2().execution_contract_sha256
        ),
        "funding_authority_manifest_sha256": funding.manifest_sha256,
        "funding_authority_protocol": HISTORICAL_THREE_FAMILY_FUNDING_AUTHORITY_PROTOCOL_V2,
        "historical_only": True,
        "kline_authority": [asdict(value) for value in kline_authority],
        "order_placement": False,
        "outputs": {
            "fixed_horizon_outcomes.csv": outcomes_sha256,
            "results.json": results_sha256,
        },
        "probability": False,
        "probability_calibrated": False,
        "promoting": False,
        "protocol": HISTORICAL_CONFLICTED_FIXED_HORIZON_RUNNER_PROTOCOL_V1,
        "schema_version": HISTORICAL_CONFLICTED_FIXED_HORIZON_SCHEMA_VERSION_V1,
        "topology_rule_version": HISTORICAL_THREE_FAMILY_TOPOLOGY_RULE_VERSION_V2,
        "topology_version": HISTORICAL_THREE_FAMILY_CONFLICTED_TOPOLOGY_VERSION_V1,
    }


_ROW_COLUMNS: Final = tuple(
    field.name for field in fields(HistoricalConflictedFixedHorizonOutcomeRowV1)
)


def _outcomes_csv_bytes(
    rows: Sequence[HistoricalConflictedFixedHorizonOutcomeRowV1],
) -> bytes:
    ordered = sorted(
        rows,
        key=lambda row: (
            row.split,
            row.asset,
            row.decision_time_ms,
            row.event_id,
            row.horizon_bars,
        ),
    )
    buffer = io.StringIO(newline="")
    writer = csv.DictWriter(buffer, fieldnames=_ROW_COLUMNS, lineterminator="\n")
    writer.writeheader()
    for row in ordered:
        document: dict[str, str] = {}
        for field in fields(row):
            value = getattr(row, field.name)
            if value is None:
                document[field.name] = ""
            elif type(value) is bool:
                document[field.name] = "true" if value else "false"
            else:
                document[field.name] = str(value)
        writer.writerow(document)
    return buffer.getvalue().encode("utf-8")


def _parse_outcomes_csv(raw: bytes) -> tuple[HistoricalConflictedFixedHorizonOutcomeRowV1, ...]:
    if not raw or b"\r" in raw or not raw.endswith(b"\n"):
        raise HistoricalConflictedFixedHorizonErrorV1(
            "conflicted outcomes must be nonempty canonical LF-only CSV"
        )
    try:
        reader = csv.DictReader(io.StringIO(raw.decode("utf-8"), newline=""), strict=True)
        if tuple(reader.fieldnames or ()) != _ROW_COLUMNS:
            raise HistoricalConflictedFixedHorizonErrorV1(
                "conflicted outcome columns differ from the frozen schema"
            )
        records = list(reader)
    except (UnicodeError, csv.Error) as exc:
        raise HistoricalConflictedFixedHorizonErrorV1(
            "conflicted outcomes are not valid UTF-8 CSV"
        ) from exc
    if len(records) > _MAX_ROWS:
        raise HistoricalConflictedFixedHorizonErrorV1("conflicted outcome row cap exceeded")
    if not records:
        raise HistoricalConflictedFixedHorizonErrorV1(
            "conflicted outcome artifact cannot be empty"
        )
    rows: list[HistoricalConflictedFixedHorizonOutcomeRowV1] = []
    for record in records:
        if None in record or any(value is None for value in record.values()):
            raise HistoricalConflictedFixedHorizonErrorV1(
                "conflicted outcome row has a missing or surplus column"
            )
        row = cast(dict[str, str], record)
        rows.append(
            HistoricalConflictedFixedHorizonOutcomeRowV1(
                topology_version=row["topology_version"],
                adapter_rule_version=row["adapter_rule_version"],
                split=row["split"],
                asset=row["asset"],
                symbol=row["symbol"],
                event_id=row["event_id"],
                anchor_sha256=row["anchor_sha256"],
                source_census_row_sha256=row["source_census_row_sha256"],
                adapter_contract_sha256=row["adapter_contract_sha256"],
                adapter_manifest_sha256=row["adapter_manifest_sha256"],
                downstream_freeze_manifest_sha256=row["downstream_freeze_manifest_sha256"],
                topology_rule_version=row["topology_rule_version"],
                execution_contract_sha256=row["execution_contract_sha256"],
                agreement_bucket=row["agreement_bucket"],
                primary_direction=row["primary_direction"],
                directional_agreement_micros=_parse_int(row["directional_agreement_micros"]),
                decision_time_ms=_parse_int(row["decision_time_ms"]),
                horizon_bars=_parse_int(row["horizon_bars"]),
                horizon_minutes=_parse_int(row["horizon_minutes"]),
                expected_entry_time_ms=_parse_int(row["expected_entry_time_ms"]),
                expected_exit_close_time_ms=_parse_int(row["expected_exit_close_time_ms"]),
                entry_price=_optional_decimal(row["entry_price"]),
                exit_price=_optional_decimal(row["exit_price"]),
                gross_directional_return_micros=_optional_int(
                    row["gross_directional_return_micros"]
                ),
                slippage_return_micros=_optional_int(row["slippage_return_micros"]),
                fee_return_micros=_optional_int(row["fee_return_micros"]),
                funding_return_micros=_optional_int(row["funding_return_micros"]),
                rounding_residual_micros=_optional_int(row["rounding_residual_micros"]),
                total_cost_micros=_optional_int(row["total_cost_micros"]),
                funding_event_count=_optional_int(row["funding_event_count"]),
                evaluable=_parse_bool(row["evaluable"]),
                exclusion_reason=row["exclusion_reason"],
                net_return_micros=_optional_int(row["net_return_micros"]),
                historical_only=cast(Literal[True], _parse_bool(row["historical_only"])),
                conflicted_comparator=cast(
                    Literal[True], _parse_bool(row["conflicted_comparator"])
                ),
                clean_population_pooled=cast(
                    Literal[False], _parse_bool(row["clean_population_pooled"])
                ),
                probability=cast(Literal[False], _parse_bool(row["probability"])),
                probability_calibrated=cast(
                    Literal[False], _parse_bool(row["probability_calibrated"])
                ),
                promoting=cast(Literal[False], _parse_bool(row["promoting"])),
                order_placement=cast(Literal[False], _parse_bool(row["order_placement"])),
            )
        )
    return tuple(rows)


def _validate_complete_rows(
    events: Sequence[HistoricalConflictedComparatorEventV1],
    rows: Sequence[HistoricalConflictedFixedHorizonOutcomeRowV1],
) -> None:
    expected = set(HISTORICAL_THREE_FAMILY_OUTCOME_HORIZONS_BARS_V2)
    grouped = {event.event_id: set() for event in events}
    for row in rows:
        if row.event_id not in grouped or row.horizon_bars in grouped[row.event_id]:
            raise HistoricalConflictedFixedHorizonErrorV1(
                "conflicted rows contain unknown or duplicate event/horizon"
            )
        grouped[row.event_id].add(row.horizon_bars)
    if any(horizons != expected for horizons in grouped.values()):
        raise HistoricalConflictedFixedHorizonErrorV1(
            "each conflicted event requires all five horizons"
        )


def _validate_complete_loaded_rows(
    rows: Sequence[HistoricalConflictedFixedHorizonOutcomeRowV1],
) -> None:
    by_event: dict[str, set[int]] = {}
    identities: dict[str, tuple[object, ...]] = {}
    for row in rows:
        horizons = by_event.setdefault(row.event_id, set())
        if row.horizon_bars in horizons:
            raise HistoricalConflictedFixedHorizonErrorV1(
                "loaded conflicted rows duplicate event/horizon"
            )
        horizons.add(row.horizon_bars)
        identity = (
            row.symbol,
            row.decision_time_ms,
            row.primary_direction,
            row.directional_agreement_micros,
            row.adapter_manifest_sha256,
            row.execution_contract_sha256,
        )
        if identities.setdefault(row.event_id, identity) != identity:
            raise HistoricalConflictedFixedHorizonErrorV1(
                "loaded conflicted event identity changes across horizons"
            )
    expected = set(HISTORICAL_THREE_FAMILY_OUTCOME_HORIZONS_BARS_V2)
    if any(horizons != expected for horizons in by_event.values()):
        raise HistoricalConflictedFixedHorizonErrorV1(
            "loaded conflicted event lacks a frozen horizon"
        )


def _validate_loaded_manifest(
    document: Mapping[str, object],
    adapter_manifest_sha256: str,
    execution_contract_sha256: str,
    downstream_freeze_manifest_sha256: str,
    funding_authority_manifest_sha256: str,
    census_manifest_sha256: str,
) -> None:
    required = {
        "adapter_manifest_sha256": adapter_manifest_sha256,
        "clean_population_pooled": False,
        "conflicted_comparator": True,
        "census_manifest_sha256": census_manifest_sha256,
        "downstream_freeze_manifest_sha256": downstream_freeze_manifest_sha256,
        "execution_contract_sha256": execution_contract_sha256,
        "funding_authority_manifest_sha256": funding_authority_manifest_sha256,
        "historical_only": True,
        "funding_authority_protocol": HISTORICAL_THREE_FAMILY_FUNDING_AUTHORITY_PROTOCOL_V2,
        "order_placement": False,
        "probability": False,
        "probability_calibrated": False,
        "promoting": False,
        "protocol": HISTORICAL_CONFLICTED_FIXED_HORIZON_RUNNER_PROTOCOL_V1,
        "schema_version": HISTORICAL_CONFLICTED_FIXED_HORIZON_SCHEMA_VERSION_V1,
        "topology_rule_version": HISTORICAL_THREE_FAMILY_TOPOLOGY_RULE_VERSION_V2,
        "topology_version": HISTORICAL_THREE_FAMILY_CONFLICTED_TOPOLOGY_VERSION_V1,
    }
    for name, expected in required.items():
        if document.get(name) != expected:
            raise HistoricalConflictedFixedHorizonErrorV1(
                f"conflicted outcome manifest field {name} differs"
            )
    outputs = _require_object(document.get("outputs"), "conflicted outputs")
    if set(outputs) != set(_OUTCOME_NAMES):
        raise HistoricalConflictedFixedHorizonErrorV1(
            "conflicted outcome hash set differs"
        )
    _validate_kline_authority(document.get("kline_authority"))


def _validate_loaded_results(
    document: Mapping[str, object],
    *,
    rows: tuple[HistoricalConflictedFixedHorizonOutcomeRowV1, ...],
    outcomes_sha256: str,
    adapter_manifest_sha256: str,
    execution_contract_sha256: str,
    funding_authority_manifest_sha256: str,
    downstream_freeze_manifest_sha256: str,
    census_manifest_sha256: str,
) -> None:
    required = {
        "adapter_manifest_sha256": adapter_manifest_sha256,
        "clean_population_pooled": False,
        "conflicted_comparator": True,
        "census_manifest_sha256": census_manifest_sha256,
        "execution_contract_sha256": execution_contract_sha256,
        "funding_authority_manifest_sha256": funding_authority_manifest_sha256,
        "historical_only": True,
        "horizons_bars": list(HISTORICAL_THREE_FAMILY_OUTCOME_HORIZONS_BARS_V2),
        "order_placement": False,
        "outcome_rows": len(rows),
        "outcomes_sha256": outcomes_sha256,
        "probability": False,
        "probability_calibrated": False,
        "promoting": False,
        "protocol": HISTORICAL_CONFLICTED_FIXED_HORIZON_RUNNER_PROTOCOL_V1,
        "schema_version": HISTORICAL_CONFLICTED_FIXED_HORIZON_SCHEMA_VERSION_V1,
        "downstream_freeze_manifest_sha256": downstream_freeze_manifest_sha256,
        "cost_summary_version": HISTORICAL_CONFLICTED_FIXED_HORIZON_COST_SUMMARY_V1,
    }
    for name, expected in required.items():
        if document.get(name) != expected:
            raise HistoricalConflictedFixedHorizonErrorV1(
                f"conflicted results field {name} differs"
            )
    if document.get("event_count") != len({row.event_id for row in rows}):
        raise HistoricalConflictedFixedHorizonErrorV1(
            "conflicted results event count differs from rows"
        )
    if document.get("cost_attribution") != [asdict(value) for value in _summaries(rows)]:
        raise HistoricalConflictedFixedHorizonErrorV1(
            "conflicted results cost attribution differs from exact rows"
        )


def _validate_kline_authority(value: object) -> None:
    if not isinstance(value, list) or len(value) != len(_FROZEN_ASSETS):
        raise HistoricalConflictedFixedHorizonErrorV1(
            "conflicted manifest must bind the exact seven-asset kline panel"
        )
    assets: set[str] = set()
    expected_fields = {field.name for field in fields(HistoricalOutcomeKlineAuthorityV2)}
    for raw in value:
        if not isinstance(raw, dict) or set(raw) != expected_fields:
            raise HistoricalConflictedFixedHorizonErrorV1(
                "conflicted kline authority row schema differs"
            )
        row = cast(dict[str, object], raw)
        asset = row.get("asset")
        if not isinstance(asset, str):
            raise HistoricalConflictedFixedHorizonErrorV1(
                "conflicted kline authority asset is invalid"
            )
        assets.add(asset)
        _require_sha256(row.get("data_sha256"), "kline data")
        _require_sha256(row.get("manifest_sha256"), "kline manifest")
        if type(row.get("row_count")) is not int or cast(int, row["row_count"]) <= 0:
            raise HistoricalConflictedFixedHorizonErrorV1(
                "conflicted kline authority row count is invalid"
            )
    if assets != _FROZEN_ASSETS:
        raise HistoricalConflictedFixedHorizonErrorV1(
            "conflicted kline authority asset set differs"
        )


def _load_downstream_code_freeze(
    *,
    workspace_root: Path,
    manifest_path: str | Path,
    expected_manifest_sha256: str,
    census_manifest_sha256: str,
    experiment_contract_sha256: str,
    topology_amendment_sha256: str,
    adapter_code_freeze_manifest_sha256: str,
    adapter_manifest_sha256: str,
    adapter_contract_sha256: str,
    funding_authority_manifest_sha256: str,
) -> DownstreamCodeFreezeAuthorityV1:
    try:
        return load_downstream_code_freeze_v1(
            manifest_path,
            workspace_root=workspace_root,
            expected_manifest_sha256=expected_manifest_sha256,
            required_upstream_sha256={
                "adapter_code_freeze": adapter_code_freeze_manifest_sha256,
                "bootstrap_schedule": (
                    HISTORICAL_THREE_FAMILY_FULL_CALENDAR_SCHEDULE_SHA256_V2
                ),
                "census_artifact_manifest": census_manifest_sha256,
                "census_code_freeze": HISTORICAL_THREE_FAMILY_CENSUS_CODE_FREEZE_SHA256_V2,
                "conflicted_adapter_contract": adapter_contract_sha256,
                "conflicted_adapter_manifest": adapter_manifest_sha256,
                "experiment_contract": experiment_contract_sha256,
                "funding_authority": funding_authority_manifest_sha256,
                "topology_amendment": topology_amendment_sha256,
            },
            forbidden_manifest_sha256=(
                HISTORICAL_THREE_FAMILY_CENSUS_CODE_FREEZE_SHA256_V2,
            ),
        )
    except DownstreamCodeFreezeErrorV1 as exc:
        raise HistoricalConflictedFixedHorizonErrorV1(
            "generic downstream code freeze authentication failed"
        ) from exc


def _publish(target: Path, payloads: Mapping[str, bytes]) -> None:
    if set(payloads) != _PUBLISHED_NAMES:
        raise HistoricalConflictedFixedHorizonErrorV1(
            "conflicted outcome publication requires exactly three artifacts"
        )
    if target.exists():
        raise HistoricalConflictedFixedHorizonErrorV1(
            "conflicted outcome output requires a fresh directory"
        )
    target.parent.mkdir(parents=True, exist_ok=True)
    staging = Path(tempfile.mkdtemp(prefix=f".{target.name}.", dir=target.parent))
    try:
        for name, payload in payloads.items():
            (staging / name).write_bytes(payload)
        os.replace(staging, target)
    except OSError as exc:
        shutil.rmtree(staging, ignore_errors=True)
        raise HistoricalConflictedFixedHorizonErrorV1(
            "cannot atomically publish conflicted outcomes"
        ) from exc


def _rate_micros(numerator: int, denominator: int) -> int:
    return 0 if denominator == 0 else (numerator * 1_000_000 + denominator // 2) // denominator


def _mean(values: Sequence[int]) -> int | None:
    if not values:
        return None
    total = sum(values)
    return (total + len(values) // 2) // len(values) if total >= 0 else -(
        (-total + len(values) // 2) // len(values)
    )


def _parse_int(value: str) -> int:
    if not value or value.startswith("+") or (value.startswith("0") and value != "0"):
        raise HistoricalConflictedFixedHorizonErrorV1("integer text is noncanonical")
    try:
        return int(value)
    except ValueError as exc:
        raise HistoricalConflictedFixedHorizonErrorV1("integer text is invalid") from exc


def _optional_int(value: str) -> int | None:
    return None if value == "" else _parse_int(value)


def _optional_decimal(value: str) -> Decimal | None:
    if value == "":
        return None
    try:
        result = Decimal(value)
    except InvalidOperation as exc:
        raise HistoricalConflictedFixedHorizonErrorV1("decimal text is invalid") from exc
    if not result.is_finite() or result <= 0:
        raise HistoricalConflictedFixedHorizonErrorV1("price must be positive finite Decimal")
    return result


def _parse_bool(value: str) -> bool:
    if value == "true":
        return True
    if value == "false":
        return False
    raise HistoricalConflictedFixedHorizonErrorV1("boolean text is noncanonical")


def _decode_canonical_object(raw: bytes, label: str) -> dict[str, object]:
    try:
        value = json.loads(raw)
    except (json.JSONDecodeError, UnicodeError) as exc:
        raise HistoricalConflictedFixedHorizonErrorV1(
            f"{label} is not valid UTF-8 JSON"
        ) from exc
    if not isinstance(value, dict):
        raise HistoricalConflictedFixedHorizonErrorV1(f"{label} must be an object")
    document = cast(dict[str, object], value)
    if canonical_json_line(document) != raw:
        raise HistoricalConflictedFixedHorizonErrorV1(
            f"{label} must be canonical RFC 8785 JSONL"
        )
    return document


def _require_object(value: object, label: str) -> dict[str, object]:
    if not isinstance(value, dict):
        raise HistoricalConflictedFixedHorizonErrorV1(f"{label} must be an object")
    return cast(dict[str, object], value)


def _require_sha256(value: object, label: str) -> str:
    if (
        not isinstance(value, str)
        or len(value) != _SHA256_LENGTH
        or any(character not in "0123456789abcdef" for character in value)
    ):
        raise HistoricalConflictedFixedHorizonErrorV1(
            f"{label} must be a lowercase SHA-256 digest"
        )
    return value


def _read_bytes(path: Path, label: str) -> bytes:
    try:
        return path.read_bytes()
    except OSError as exc:
        raise HistoricalConflictedFixedHorizonErrorV1(f"cannot read {label}: {path}") from exc


def _sha256_bytes(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--adapter-artifact-dir", required=True)
    parser.add_argument("--adapter-manifest-sha256", required=True)
    parser.add_argument("--consensus", required=True)
    parser.add_argument("--census-manifest", required=True)
    parser.add_argument("--census-manifest-sha256", required=True)
    parser.add_argument("--experiment-contract-sha256", required=True)
    parser.add_argument("--topology-amendment-sha256", required=True)
    parser.add_argument("--workspace-root", required=True)
    parser.add_argument("--adapter-contract", required=True)
    parser.add_argument("--adapter-contract-sha256", required=True)
    parser.add_argument("--adapter-code-freeze-manifest", required=True)
    parser.add_argument("--adapter-code-freeze-manifest-sha256", required=True)
    parser.add_argument("--downstream-code-freeze-manifest", required=True)
    parser.add_argument("--expected-downstream-code-freeze-manifest-sha256", required=True)
    parser.add_argument("--funding-authority-manifest", required=True)
    parser.add_argument("--funding-authority-manifest-sha256", required=True)
    parser.add_argument("--data-root", required=True)
    parser.add_argument("--output-dir", required=True)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    run_historical_conflicted_fixed_horizons_v1(
        adapter_artifact_dir=args.adapter_artifact_dir,
        expected_adapter_manifest_sha256=args.adapter_manifest_sha256,
        consensus_path=args.consensus,
        census_manifest_path=args.census_manifest,
        expected_census_manifest_sha256=args.census_manifest_sha256,
        expected_experiment_contract_sha256=args.experiment_contract_sha256,
        expected_topology_amendment_sha256=args.topology_amendment_sha256,
        workspace_root=args.workspace_root,
        adapter_contract_path=args.adapter_contract,
        expected_adapter_contract_sha256=args.adapter_contract_sha256,
        adapter_code_freeze_manifest_path=args.adapter_code_freeze_manifest,
        expected_adapter_code_freeze_manifest_sha256=(
            args.adapter_code_freeze_manifest_sha256
        ),
        downstream_code_freeze_manifest_path=args.downstream_code_freeze_manifest,
        expected_downstream_code_freeze_manifest_sha256=(
            args.expected_downstream_code_freeze_manifest_sha256
        ),
        funding_authority_manifest_path=args.funding_authority_manifest,
        expected_funding_authority_manifest_sha256=(
            args.funding_authority_manifest_sha256
        ),
        data_root=args.data_root,
        output_dir=args.output_dir,
    )
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
