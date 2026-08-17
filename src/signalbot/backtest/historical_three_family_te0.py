"""Authenticated TE0 technical exits for the historical three-family census.

This sibling adapter is intentionally downstream of the outcome-blind census.
It authenticates the frozen consensus before reading forward candles, admits
only the current clean/broad supporting population, and delegates all exit and
execution arithmetic to :class:`CounterfactualTechnicalExitEvaluator`.

TE0 is historical, overlapping, and counterfactual.  It never evaluates an
opposite signal, places an order, constructs portfolio equity, or makes a
probability or promotion claim.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import io
import json
import os
import re
import shutil
import tempfile
from collections import Counter
from collections.abc import Collection, Mapping, Sequence
from dataclasses import asdict, dataclass, field, fields
from decimal import Decimal, InvalidOperation
from enum import StrEnum
from pathlib import Path
from typing import Final, Literal, TypedDict, cast

from signalbot.backtest.config import ExitPolicySettings
from signalbot.backtest.dataset import KlineDataset
from signalbot.backtest.downstream_code_freeze import load_downstream_code_freeze_v1
from signalbot.backtest.funding import FundingDataset
from signalbot.backtest.historical_three_family_outcomes import (
    HISTORICAL_THREE_FAMILY_CENSUS_CODE_FREEZE_SHA256_V2,
    HISTORICAL_THREE_FAMILY_PRIMARY_TOPOLOGY_V2,
    HistoricalConsensusOutcomeEventV2,
    LoadedHistoricalConsensusV2,
    LoadedHistoricalFundingAuthorityV2,
    LoadedHistoricalKlinePanelV2,
    historical_return_to_micros_v2,
    historical_three_family_split_bounds_v2,
    load_authenticated_historical_consensus_v2,
    load_authenticated_historical_funding_authority_v2,
    load_authenticated_historical_kline_panel_v2,
)
from signalbot.backtest.outcomes import (
    CounterfactualTechnicalExitEvaluator,
    CounterfactualTechnicalExitExclusion,
    CounterfactualTechnicalExitOutcome,
    TechnicalExitOutcomeExclusionReason,
)
from signalbot.config import SignalSettings
from signalbot.domain.enums import Direction, Market, SignalStage
from signalbot.domain.models import FeatureSnapshot, SignalDecision
from signalbot.indicators.core import FeatureEngine
from signalbot.r4b_v2.canonical import canonical_json_line
from signalbot.r4b_v2.protocol.decision_clock import FIVE_MINUTE_MS_V2
from signalbot.r4b_v2.research.historical_three_family_bootstrap import (
    HISTORICAL_THREE_FAMILY_FULL_CALENDAR_SCHEDULE_SHA256_V2,
)
from signalbot.r4b_v2.strategy.historical_three_family_consensus import (
    HISTORICAL_THREE_FAMILY_CONSENSUS_RULE_VERSION_V2,
    HistoricalExecutionContractV2,
    build_historical_execution_contract_v2,
)

HISTORICAL_THREE_FAMILY_TE0_PROTOCOL_V2: Final = (
    "historical_three_family_te0_no_opposite_signal_v2_2026-07-20"
)
HISTORICAL_THREE_FAMILY_TE0_SCHEMA_VERSION_V2: Final = 1
HISTORICAL_THREE_FAMILY_TE0_RULE_V2: Final = "TE0_NO_OPPOSITE_SIGNAL"
HISTORICAL_THREE_FAMILY_TE0_POLICY_VERSION_V2: Final = (
    "R4B_CAUSAL_V2.4.1_HISTORICAL_TE0_NO_OPPOSITE_SIGNAL_V1_FROZEN"
)
HISTORICAL_THREE_FAMILY_TE0_FEATURE_PROTOCOL_V2: Final = (
    "historical_three_family_te0_closed_5m_feature_path_v2_2026-07-20"
)

_TE0_TREND_FAILURE_BARS: Final = 3
_TE0_TRAILING_ACTIVATION_R: Final = 1
_TE0_TRAILING_ATR_MULTIPLE: Final = 2
_TE0_MAX_HOLDING_BARS: Final = 72
_TE0_FEATURE_MINIMUM_HISTORY: Final = 210
_TE0_BREAKOUT_LOOKBACK: Final = 20
_TE0_POLICY_DOMAIN: Final = b"signalbot:r4b-v2:historical-three-family:te0-policy:v1\0"
_TE0_FEATURE_DOMAIN: Final = b"signalbot:r4b-v2:historical-three-family:te0-feature:v1\0"
_OUTPUT_NAMES: Final = ("technical_exit_te0.csv", "results.json")
_PUBLISHED_NAMES: Final = frozenset((*_OUTPUT_NAMES, "manifest.json"))
_SHA256_RE: Final = re.compile(r"^[0-9a-f]{64}$")
_TE0_ROW_CAP: Final = 100_000
_ALLOWED_STATE_TO_BUCKET_AND_SIDE: Final = {
    "BROAD_BULLISH_STATE": ("BROAD_3_OF_3", Direction.LONG.value),
    "BULLISH_STATE_TILT": ("TILT_2_OF_3", Direction.LONG.value),
    "BROAD_BEARISH_STATE": ("BROAD_3_OF_3", Direction.SHORT.value),
    "BEARISH_STATE_TILT": ("TILT_2_OF_3", Direction.SHORT.value),
}


class HistoricalThreeFamilyTe0ErrorV2(ValueError):
    """Raised when TE0 inputs or artifacts violate the frozen contract."""


class HistoricalThreeFamilyTe0ExclusionV2(StrEnum):
    """Exhaustive one-row exclusions emitted by the TE0 adapter."""

    MISSING_DECISION_BAR = "MISSING_DECISION_BAR"
    SOURCE_INVALIDATION_MISSING = "SOURCE_INVALIDATION_MISSING"
    SOURCE_INVALIDATION_WRONG_SIDE = "SOURCE_INVALIDATION_WRONG_SIDE"
    SOURCE_INVALIDATION_INVALID = "SOURCE_INVALIDATION_INVALID"
    UNSUPPORTED_TIMEFRAME = "UNSUPPORTED_TIMEFRAME"
    UNSUPPORTED_DIRECTION = "UNSUPPORTED_DIRECTION"
    SPOT_SHORT_NOT_EXECUTABLE = "SPOT_SHORT_NOT_EXECUTABLE"
    SPLIT_LEAKAGE = "SPLIT_LEAKAGE"
    NEXT_BAR_UNAVAILABLE = "NEXT_BAR_UNAVAILABLE"
    NEXT_BAR_NOT_CONTIGUOUS = "NEXT_BAR_NOT_CONTIGUOUS"
    INVALID_CANDLE_SERIES = "INVALID_CANDLE_SERIES"
    DATA_GAP = "DATA_GAP"
    FEATURE_UNAVAILABLE = "FEATURE_UNAVAILABLE"
    FEATURE_MISMATCH = "FEATURE_MISMATCH"
    END_OF_DATA_BEFORE_EXIT = "END_OF_DATA_BEFORE_EXIT"
    FUNDING_DATASET_UNAVAILABLE = "FUNDING_DATASET_UNAVAILABLE"
    FUNDING_COVERAGE_UNAVAILABLE = "FUNDING_COVERAGE_UNAVAILABLE"


@dataclass(frozen=True, slots=True)
class HistoricalThreeFamilyTe0PolicyV2:
    """Frozen technical-exit policy and its deterministic contract digest."""

    rule: Literal["TE0_NO_OPPOSITE_SIGNAL"] = HISTORICAL_THREE_FAMILY_TE0_RULE_V2
    trend_failure_bars: Literal[3] = _TE0_TREND_FAILURE_BARS
    trailing_activation_r: Literal[1] = _TE0_TRAILING_ACTIVATION_R
    trailing_atr_multiple: Literal[2] = _TE0_TRAILING_ATR_MULTIPLE
    max_holding_bars: Literal[72] = _TE0_MAX_HOLDING_BARS
    evaluator_policy_version: str = CounterfactualTechnicalExitEvaluator.POLICY_VERSION
    policy_sha256: str = field(init=False)
    policy_version: str = field(
        init=False,
        default=HISTORICAL_THREE_FAMILY_TE0_POLICY_VERSION_V2,
    )
    next_contiguous_open_entry: Literal[True] = field(init=False, default=True)
    source_invalidation_initial_stop: Literal[True] = field(init=False, default=True)
    closed_candle_exit_observations_only: Literal[True] = field(init=False, default=True)
    opposite_signal_evaluated: Literal[False] = field(init=False, default=False)
    order_placement: Literal[False] = field(init=False, default=False)
    portfolio_equity_claim: Literal[False] = field(init=False, default=False)
    drawdown_claim: Literal[False] = field(init=False, default=False)

    def __post_init__(self) -> None:
        if (
            self.rule != HISTORICAL_THREE_FAMILY_TE0_RULE_V2
            or self.trend_failure_bars != _TE0_TREND_FAILURE_BARS
            or self.trailing_activation_r != _TE0_TRAILING_ACTIVATION_R
            or self.trailing_atr_multiple != _TE0_TRAILING_ATR_MULTIPLE
            or self.max_holding_bars != _TE0_MAX_HOLDING_BARS
            or self.evaluator_policy_version != CounterfactualTechnicalExitEvaluator.POLICY_VERSION
        ):
            raise HistoricalThreeFamilyTe0ErrorV2("TE0 policy differs from the frozen contract")
        object.__setattr__(
            self,
            "policy_sha256",
            hashlib.sha256(
                _TE0_POLICY_DOMAIN + canonical_historical_three_family_te0_policy_v2(self)
            ).hexdigest(),
        )

    def evaluator_settings(self) -> ExitPolicySettings:
        """Return the existing evaluator's typed settings without alternate arithmetic."""

        return ExitPolicySettings(
            trend_failure_bars=self.trend_failure_bars,
            trailing_activation_r=self.trailing_activation_r,
            trailing_atr_multiple=self.trailing_atr_multiple,
            max_holding_bars=self.max_holding_bars,
        )


@dataclass(frozen=True, slots=True)
class HistoricalThreeFamilyTe0RowV2:
    """Exactly one evaluable result or explicit exclusion for one admitted event."""

    topology_version: str
    split: str
    asset: str
    symbol: str
    event_id: str
    anchor_sha256: str
    consensus_rule_version: str
    te0_protocol_version: str
    te0_policy_version: str
    te0_policy_sha256: str
    evaluator_policy_version: str
    feature_contract_sha256: str
    execution_contract_sha256: str
    state_class: str
    agreement_bucket: str
    primary_family: str
    primary_direction: str
    directional_agreement_micros: int
    decision_time_ms: int
    source_invalidation: Decimal | None
    expected_entry_time_ms: int
    actual_open_time_ms: int | None
    observed_bars: int
    observed_until_ms: int
    evaluable: bool
    exclusion_reason: str
    exclusion_expected_open_time_ms: int | None
    entry_action_label: str | None
    exit_action_label: str | None
    entry_time_ms: int | None
    exit_time_ms: int | None
    exit_signal_observed_at_ms: int | None
    entry_price: Decimal | None
    exit_price: Decimal | None
    initial_stop: Decimal | None
    active_stop: Decimal | None
    exit_reason: str | None
    execution_model: str | None
    bars_held: int | None
    gross_directional_return_micros: int | None
    slippage_return_micros: int | None
    fee_return_micros: int | None
    funding_return_micros: int | None
    rounding_residual_micros: int | None
    total_cost_micros: int | None
    funding_event_count: int | None
    net_return_micros: int | None
    mfe_micros: int | None
    mae_micros: int | None
    source_information_only: Literal[True] = True
    opposite_signal_evaluated: Literal[False] = False
    historical_only: Literal[True] = True
    probability: Literal[False] = False
    probability_calibrated: Literal[False] = False
    promoting: Literal[False] = False
    order_placement: Literal[False] = False
    portfolio_equity_claim: Literal[False] = False
    drawdown_claim: Literal[False] = False

    def __post_init__(self) -> None:
        _validate_te0_row_identity_v2(self)
        _validate_te0_row_claims_v2(self)
        if self.evaluable:
            _validate_evaluable_te0_row_v2(self)
        else:
            _validate_excluded_te0_row_v2(self)


@dataclass(frozen=True, slots=True)
class HistoricalThreeFamilyTe0ArtifactsV2:
    output_dir: Path
    technical_exit_sha256: str
    results_sha256: str
    manifest_sha256: str
    admitted_events: int
    result_rows: int
    evaluable_rows: int


@dataclass(frozen=True, slots=True)
class LoadedHistoricalThreeFamilyTe0ArtifactsV2:
    """Hash-authenticated TE0 artifacts for downstream analysis."""

    artifact_dir: Path
    manifest_sha256: str
    technical_exit_sha256: str
    results_sha256: str
    census_manifest_sha256: str
    consensus_sha256: str
    experiment_contract_sha256: str
    topology_amendment_sha256: str
    execution_contract_sha256: str
    funding_authority_manifest_sha256: str
    downstream_code_freeze_manifest_sha256: str
    census_rows: int
    admitted_events: int
    rows: tuple[HistoricalThreeFamilyTe0RowV2, ...]
    results: Mapping[str, object]
    historical_only: Literal[True] = True
    probability: Literal[False] = False
    promoting: Literal[False] = False


class _Te0CommonRowV2(TypedDict):
    topology_version: str
    split: str
    asset: str
    symbol: str
    event_id: str
    anchor_sha256: str
    consensus_rule_version: str
    te0_protocol_version: str
    te0_policy_version: str
    te0_policy_sha256: str
    evaluator_policy_version: str
    feature_contract_sha256: str
    execution_contract_sha256: str
    state_class: str
    agreement_bucket: str
    primary_family: str
    primary_direction: str
    directional_agreement_micros: int
    decision_time_ms: int
    source_invalidation: Decimal | None
    expected_entry_time_ms: int


def canonical_historical_three_family_te0_policy_v2(
    policy: HistoricalThreeFamilyTe0PolicyV2,
) -> bytes:
    """Serialize the frozen TE0 policy without its self-referential digest."""

    if type(policy) is not HistoricalThreeFamilyTe0PolicyV2:
        raise HistoricalThreeFamilyTe0ErrorV2("TE0 policy requires its exact contract type")
    return canonical_json_line(
        {
            "closed_candle_exit_observations_only": (policy.closed_candle_exit_observations_only),
            "drawdown_claim": policy.drawdown_claim,
            "evaluator_policy_version": policy.evaluator_policy_version,
            "max_holding_bars": policy.max_holding_bars,
            "next_contiguous_open_entry": policy.next_contiguous_open_entry,
            "opposite_signal_evaluated": policy.opposite_signal_evaluated,
            "order_placement": policy.order_placement,
            "policy_version": policy.policy_version,
            "portfolio_equity_claim": policy.portfolio_equity_claim,
            "rule": policy.rule,
            "source_invalidation_initial_stop": policy.source_invalidation_initial_stop,
            "trailing_activation_r": policy.trailing_activation_r,
            "trailing_atr_multiple": policy.trailing_atr_multiple,
            "trend_failure_bars": policy.trend_failure_bars,
        }
    )


def build_historical_three_family_te0_policy_v2() -> HistoricalThreeFamilyTe0PolicyV2:
    """Construct the one allowed TE0 policy."""

    return HistoricalThreeFamilyTe0PolicyV2()


def canonical_historical_three_family_te0_feature_contract_v2() -> bytes:
    """Describe the causal closed-kline feature path consumed by the shared evaluator."""

    return canonical_json_line(
        {
            "breakout_lookback": _TE0_BREAKOUT_LOOKBACK,
            "causal_segment_reset_after_gap": True,
            "closed_klines_only": True,
            "feature_engine_owner": "signalbot.indicators.core.FeatureEngine",
            "features_used": ["atr", "ema20", "macd_histogram"],
            "future_rows_used_for_prior_features": False,
            "interval": "5m",
            "minimum_history": _TE0_FEATURE_MINIMUM_HISTORY,
            "protocol": HISTORICAL_THREE_FAMILY_TE0_FEATURE_PROTOCOL_V2,
            "runner_materialization": "ADMITTED_EVENT_PATH_ROWS_ONLY",
        }
    )


def historical_three_family_te0_feature_contract_sha256_v2() -> str:
    """Return the deterministic feature-path contract digest."""

    return hashlib.sha256(
        _TE0_FEATURE_DOMAIN + canonical_historical_three_family_te0_feature_contract_v2()
    ).hexdigest()


def build_historical_three_family_te0_features_v2(
    dataset: KlineDataset,
    *,
    required_open_times: Collection[int] | None = None,
) -> tuple[FeatureSnapshot | None, ...]:
    """Build causal features at requested rows, resetting every discontinuity."""

    if type(dataset) is not KlineDataset:
        raise HistoricalThreeFamilyTe0ErrorV2("TE0 features require a KlineDataset")
    request = dataset.request
    if request.market is not Market.FUTURES or request.interval != "5m":
        raise HistoricalThreeFamilyTe0ErrorV2(
            "TE0 features require an authenticated USD-M Futures 5m dataset"
        )
    candles = dataset.candles
    engine = FeatureEngine(SignalSettings(breakout_lookback=_TE0_BREAKOUT_LOOKBACK))
    if engine.minimum_history != _TE0_FEATURE_MINIMUM_HISTORY:
        raise HistoricalThreeFamilyTe0ErrorV2(
            "FeatureEngine minimum history differs from the frozen TE0 feature contract"
        )
    output: list[FeatureSnapshot | None] = [None] * len(candles)
    requested = (
        {candle.open_time_ms for candle in candles}
        if required_open_times is None
        else set(required_open_times)
    )
    if any(type(value) is not int or value < 0 for value in requested):
        raise HistoricalThreeFamilyTe0ErrorV2(
            "TE0 required feature open times must be nonnegative integers"
        )
    segment_start = 0
    for index in range(1, len(candles) + 1):
        segment_end = index == len(candles)
        gap_before_index = (
            not segment_end
            and candles[index].open_time_ms - candles[index - 1].open_time_ms != FIVE_MINUTE_MS_V2
        )
        if not segment_end and not gap_before_index:
            continue
        segment = candles[segment_start:index]
        selected = tuple(
            local_index
            for local_index, candle in enumerate(segment)
            if candle.open_time_ms in requested
        )
        if selected:
            computed = engine.compute_at_indices(
                segment,
                selected,
                spread_bps=0.0,
                spread_is_proxy=True,
            )
            for local_index, feature in zip(selected, computed, strict=True):
                output[segment_start + local_index] = feature
        segment_start = index
    return tuple(output)


def evaluate_historical_three_family_te0_v2(
    event: HistoricalConsensusOutcomeEventV2,
    dataset: KlineDataset,
    features: Sequence[FeatureSnapshot | None],
    funding: FundingDataset | None,
    *,
    execution_contract: HistoricalExecutionContractV2 | None = None,
    policy: HistoricalThreeFamilyTe0PolicyV2 | None = None,
) -> HistoricalThreeFamilyTe0RowV2:
    """Evaluate one admitted event by delegating to the shared causal exit owner."""

    if type(event) is not HistoricalConsensusOutcomeEventV2:
        raise HistoricalThreeFamilyTe0ErrorV2(
            "TE0 event must be an exact authenticated consensus outcome event"
        )
    if type(dataset) is not KlineDataset:
        raise HistoricalThreeFamilyTe0ErrorV2("TE0 dataset must be a KlineDataset")
    contract = execution_contract or build_historical_execution_contract_v2()
    if type(contract) is not HistoricalExecutionContractV2:
        raise HistoricalThreeFamilyTe0ErrorV2(
            "TE0 execution contract must be the exact frozen contract type"
        )
    if contract.execution_contract_sha256 != event.execution_contract_sha256:
        raise HistoricalThreeFamilyTe0ErrorV2("TE0 event and execution contract digests differ")
    te0_policy = policy or build_historical_three_family_te0_policy_v2()
    if type(te0_policy) is not HistoricalThreeFamilyTe0PolicyV2:
        raise HistoricalThreeFamilyTe0ErrorV2("TE0 policy must be the exact frozen type")
    _validate_te0_dataset_identity_v2(event, dataset)
    if funding is not None and funding.symbol != event.symbol:
        raise HistoricalThreeFamilyTe0ErrorV2("TE0 funding symbol differs from the consensus event")

    by_open = {candle.open_time_ms: candle for candle in dataset.candles}
    decision_open_ms = event.decision_time_ms - FIVE_MINUTE_MS_V2 + 1
    decision_candle = by_open.get(decision_open_ms)
    if decision_candle is None:
        return _te0_exclusion_row_v2(
            event,
            te0_policy,
            HistoricalThreeFamilyTe0ExclusionV2.MISSING_DECISION_BAR,
            expected_open_time_ms=event.decision_time_ms + 1,
            observed_until_ms=event.decision_time_ms,
        )
    if decision_candle.close != event.decision_price:
        raise HistoricalThreeFamilyTe0ErrorV2(
            "authenticated consensus decision price differs from the bound TE0 kline"
        )
    if event.invalidation is None:
        return _te0_exclusion_row_v2(
            event,
            te0_policy,
            HistoricalThreeFamilyTe0ExclusionV2.SOURCE_INVALIDATION_MISSING,
            expected_open_time_ms=event.decision_time_ms + 1,
            observed_until_ms=event.decision_time_ms,
        )

    expected_entry_ms = event.decision_time_ms + 1
    future = tuple(candle for candle in dataset.candles if candle.open_time_ms >= expected_entry_ms)
    if future and future[0].open_time_ms == expected_entry_ms:
        entry = future[0].open
        wrong_side = (
            event.primary_direction is Direction.LONG and event.invalidation >= entry
        ) or (event.primary_direction is Direction.SHORT and event.invalidation <= entry)
        if wrong_side:
            return _te0_exclusion_row_v2(
                event,
                te0_policy,
                HistoricalThreeFamilyTe0ExclusionV2.SOURCE_INVALIDATION_WRONG_SIDE,
                expected_open_time_ms=expected_entry_ms,
                actual_open_time_ms=expected_entry_ms,
                observed_until_ms=event.decision_time_ms,
            )

    decision = _te0_signal_decision_v2(event)
    split_start_ms, split_end_ms = historical_three_family_split_bounds_v2(event.split)
    evaluator = CounterfactualTechnicalExitEvaluator(te0_policy.evaluator_settings())
    result = evaluator.evaluate(
        decision,
        dataset.candles,
        tuple(features),
        split_start_ms=split_start_ms,
        split_end_ms=split_end_ms,
        fee_bps=float(contract.fee_bps_per_side),
        slippage_bps=float(contract.slippage_bps_per_side),
        funding=() if funding is None else funding.rates,
    )
    if isinstance(result, CounterfactualTechnicalExitExclusion):
        return _te0_exclusion_from_evaluator_v2(event, te0_policy, result)
    if not isinstance(result, CounterfactualTechnicalExitOutcome):  # pragma: no cover
        raise RuntimeError("shared technical-exit evaluator returned an unknown result")
    if funding is None:
        return _te0_exclusion_from_outcome_v2(
            event,
            te0_policy,
            result,
            HistoricalThreeFamilyTe0ExclusionV2.FUNDING_DATASET_UNAVAILABLE,
        )
    if not (
        funding.start_time_ms <= result.entry_time_ms and funding.end_time_ms >= result.exit_time_ms
    ):
        return _te0_exclusion_from_outcome_v2(
            event,
            te0_policy,
            result,
            HistoricalThreeFamilyTe0ExclusionV2.FUNDING_COVERAGE_UNAVAILABLE,
        )
    return _te0_outcome_row_v2(event, te0_policy, result, funding)


def run_historical_three_family_te0_v2(
    *,
    consensus_path: str | Path,
    census_manifest_path: str | Path,
    expected_census_manifest_sha256: str,
    expected_experiment_contract_sha256: str,
    expected_topology_amendment_sha256: str,
    data_root: str | Path,
    output_dir: str | Path,
    workspace_root: str | Path,
    downstream_code_freeze_manifest_path: str | Path,
    expected_downstream_code_freeze_manifest_sha256: str,
    funding_authority_manifest_path: str | Path,
    expected_funding_authority_manifest_sha256: str,
) -> HistoricalThreeFamilyTe0ArtifactsV2:
    """Authenticate frozen inputs, evaluate TE0, and atomically publish artifacts."""

    _validate_optional_funding_authority_v2(
        funding_authority_manifest_path,
        expected_funding_authority_manifest_sha256,
    )
    downstream_freeze = load_downstream_code_freeze_v1(
        downstream_code_freeze_manifest_path,
        workspace_root=workspace_root,
        expected_manifest_sha256=expected_downstream_code_freeze_manifest_sha256,
        required_upstream_sha256={
            "bootstrap_schedule": HISTORICAL_THREE_FAMILY_FULL_CALENDAR_SCHEDULE_SHA256_V2,
            "census_artifact_manifest": expected_census_manifest_sha256,
            "census_code_freeze": HISTORICAL_THREE_FAMILY_CENSUS_CODE_FREEZE_SHA256_V2,
            "experiment_contract": expected_experiment_contract_sha256,
            "funding_authority": expected_funding_authority_manifest_sha256,
            "topology_amendment": expected_topology_amendment_sha256,
        },
        forbidden_manifest_sha256=(HISTORICAL_THREE_FAMILY_CENSUS_CODE_FREEZE_SHA256_V2,),
    )
    loaded = load_authenticated_historical_consensus_v2(
        consensus_path,
        census_manifest_path,
        expected_census_manifest_sha256=expected_census_manifest_sha256,
        expected_experiment_contract_sha256=expected_experiment_contract_sha256,
        expected_topology_amendment_sha256=expected_topology_amendment_sha256,
    )
    panel = load_authenticated_historical_kline_panel_v2(loaded, data_root)
    funding_authority = load_authenticated_historical_funding_authority_v2(
        funding_authority_manifest_path,
        expected_manifest_sha256=expected_funding_authority_manifest_sha256,
        data_root=data_root,
    )
    rows = _evaluate_authenticated_te0_population_v2(
        loaded,
        panel,
        funding_authority,
    )
    _validate_complete_te0_census_v2(loaded.events, rows)
    csv_raw = _technical_exit_te0_csv_bytes_v2(rows)
    policy = build_historical_three_family_te0_policy_v2()
    results_raw = canonical_json_line(
        _te0_results_document_v2(
            loaded=loaded,
            rows=rows,
            funding_authority=funding_authority,
            downstream_code_freeze_manifest_sha256=downstream_freeze.manifest_sha256,
            technical_exit_sha256=_sha256_bytes_v2(csv_raw),
            policy=policy,
        )
    )
    manifest_raw = canonical_json_line(
        _te0_manifest_document_v2(
            loaded=loaded,
            panel=panel,
            funding_authority=funding_authority,
            downstream_code_freeze_manifest_sha256=downstream_freeze.manifest_sha256,
            policy=policy,
            output_hashes={
                "technical_exit_te0.csv": _sha256_bytes_v2(csv_raw),
                "results.json": _sha256_bytes_v2(results_raw),
            },
        )
    )
    target = Path(output_dir).resolve()
    _publish_te0_artifacts_v2(
        target,
        {
            "technical_exit_te0.csv": csv_raw,
            "results.json": results_raw,
            "manifest.json": manifest_raw,
        },
    )
    return HistoricalThreeFamilyTe0ArtifactsV2(
        output_dir=target,
        technical_exit_sha256=_sha256_bytes_v2(csv_raw),
        results_sha256=_sha256_bytes_v2(results_raw),
        manifest_sha256=_sha256_bytes_v2(manifest_raw),
        admitted_events=len(loaded.events),
        result_rows=len(rows),
        evaluable_rows=sum(row.evaluable for row in rows),
    )


def load_authenticated_historical_three_family_te0_artifacts_v2(
    artifact_dir: str | Path,
    *,
    expected_manifest_sha256: str,
    expected_census_manifest_sha256: str,
    expected_experiment_contract_sha256: str,
    expected_topology_amendment_sha256: str,
    expected_funding_authority_manifest_sha256: str,
    expected_downstream_code_freeze_manifest_sha256: str,
) -> LoadedHistoricalThreeFamilyTe0ArtifactsV2:
    """Load exactly one published TE0 artifact set and fail closed."""

    for value, label in (
        (expected_manifest_sha256, "expected TE0 manifest SHA-256"),
        (expected_census_manifest_sha256, "expected census manifest SHA-256"),
        (expected_experiment_contract_sha256, "expected experiment contract SHA-256"),
        (expected_topology_amendment_sha256, "expected topology amendment SHA-256"),
        (
            expected_downstream_code_freeze_manifest_sha256,
            "expected downstream code-freeze manifest SHA-256",
        ),
    ):
        _require_sha256_te0_v2(value, label)
    _require_sha256_te0_v2(
        expected_funding_authority_manifest_sha256,
        "expected funding authority SHA-256",
    )
    root = Path(artifact_dir).resolve()
    _require_exact_te0_artifact_files_v2(root)
    manifest_raw = _read_te0_bytes_v2(root / "manifest.json", "TE0 manifest")
    if _sha256_bytes_v2(manifest_raw) != expected_manifest_sha256:
        raise HistoricalThreeFamilyTe0ErrorV2(
            "TE0 manifest differs from the externally frozen SHA-256"
        )
    manifest = _decode_canonical_te0_json_v2(manifest_raw, "TE0 manifest")
    _validate_loaded_te0_manifest_v2(
        manifest,
        expected_census_manifest_sha256=expected_census_manifest_sha256,
        expected_experiment_contract_sha256=expected_experiment_contract_sha256,
        expected_topology_amendment_sha256=expected_topology_amendment_sha256,
        expected_funding_authority_manifest_sha256=(expected_funding_authority_manifest_sha256),
        expected_downstream_code_freeze_manifest_sha256=(
            expected_downstream_code_freeze_manifest_sha256
        ),
    )
    csv_raw = _read_te0_bytes_v2(root / "technical_exit_te0.csv", "TE0 CSV")
    results_raw = _read_te0_bytes_v2(root / "results.json", "TE0 results")
    technical_exit_sha256 = _sha256_bytes_v2(csv_raw)
    results_sha256 = _sha256_bytes_v2(results_raw)
    outputs = _require_te0_mapping_v2(manifest.get("outputs"), "TE0 outputs")
    if outputs != {
        "technical_exit_te0.csv": technical_exit_sha256,
        "results.json": results_sha256,
    }:
        raise HistoricalThreeFamilyTe0ErrorV2(
            "TE0 payload hashes differ from the authenticated manifest"
        )
    rows = _parse_technical_exit_te0_csv_v2(csv_raw)
    results = _decode_canonical_te0_json_v2(results_raw, "TE0 results")
    _validate_loaded_te0_results_v2(
        results,
        manifest=manifest,
        rows=rows,
        technical_exit_sha256=technical_exit_sha256,
        expected_funding_authority_manifest_sha256=(expected_funding_authority_manifest_sha256),
        expected_downstream_code_freeze_manifest_sha256=(
            expected_downstream_code_freeze_manifest_sha256
        ),
    )
    return LoadedHistoricalThreeFamilyTe0ArtifactsV2(
        artifact_dir=root,
        manifest_sha256=expected_manifest_sha256,
        technical_exit_sha256=technical_exit_sha256,
        results_sha256=results_sha256,
        census_manifest_sha256=expected_census_manifest_sha256,
        consensus_sha256=_require_sha256_te0_v2(
            manifest.get("consensus_sha256"), "TE0 consensus SHA-256"
        ),
        experiment_contract_sha256=expected_experiment_contract_sha256,
        topology_amendment_sha256=expected_topology_amendment_sha256,
        execution_contract_sha256=_require_sha256_te0_v2(
            manifest.get("execution_contract_sha256"),
            "TE0 execution contract SHA-256",
        ),
        funding_authority_manifest_sha256=expected_funding_authority_manifest_sha256,
        downstream_code_freeze_manifest_sha256=(expected_downstream_code_freeze_manifest_sha256),
        census_rows=_require_nonnegative_te0_int_v2(results.get("census_rows"), "census_rows"),
        admitted_events=_require_nonnegative_te0_int_v2(
            results.get("admitted_events"), "admitted_events"
        ),
        rows=rows,
        results=results,
    )


def _evaluate_authenticated_te0_population_v2(
    loaded: LoadedHistoricalConsensusV2,
    panel: LoadedHistoricalKlinePanelV2,
    funding_authority: LoadedHistoricalFundingAuthorityV2 | None,
) -> tuple[HistoricalThreeFamilyTe0RowV2, ...]:
    datasets = panel.by_asset()
    funding_by_symbol = {} if funding_authority is None else funding_authority.by_symbol()
    needed_assets = {event.asset for event in loaded.events}
    feature_by_asset = {
        asset: build_historical_three_family_te0_features_v2(
            datasets[asset],
            required_open_times={
                event.decision_time_ms + 1 + offset * FIVE_MINUTE_MS_V2
                for event in loaded.events
                if event.asset == asset
                for offset in range(_TE0_MAX_HOLDING_BARS)
            },
        )
        for asset in sorted(needed_assets)
    }
    execution_contract = build_historical_execution_contract_v2()
    policy = build_historical_three_family_te0_policy_v2()
    return tuple(
        evaluate_historical_three_family_te0_v2(
            event,
            datasets[event.asset],
            feature_by_asset[event.asset],
            funding_by_symbol.get(event.symbol),
            execution_contract=execution_contract,
            policy=policy,
        )
        for event in loaded.events
    )


def _te0_signal_decision_v2(event: HistoricalConsensusOutcomeEventV2) -> SignalDecision:
    return SignalDecision(
        event_id=event.event_id,
        market=Market.FUTURES,
        symbol=event.symbol,
        family=event.primary_family,
        stage=SignalStage.SETUP,
        direction=event.primary_direction,
        timeframe="5m",
        event_time_ms=event.decision_time_ms,
        score=100,
        price=event.decision_price,
        reasons=(
            "HISTORICAL_THREE_FAMILY_TE0_COUNTERFACTUAL",
            "NO_OPPOSITE_SIGNAL_EXIT",
        ),
        invalidation=event.invalidation,
        rule_version=HISTORICAL_THREE_FAMILY_CONSENSUS_RULE_VERSION_V2,
        metadata={
            "historical_only": True,
            "informational_only": True,
            "order_placement": False,
            "te0_policy": HISTORICAL_THREE_FAMILY_TE0_RULE_V2,
        },
    )


def _te0_outcome_row_v2(
    event: HistoricalConsensusOutcomeEventV2,
    policy: HistoricalThreeFamilyTe0PolicyV2,
    outcome: CounterfactualTechnicalExitOutcome,
    funding: FundingDataset,
) -> HistoricalThreeFamilyTe0RowV2:
    if (
        outcome.policy_version != policy.evaluator_policy_version
        or outcome.source_information_only is not True
        or outcome.opposite_signal_evaluated
        or outcome.order_placed
        or outcome.exit_reason.value == "opposite_signal"
    ):
        raise HistoricalThreeFamilyTe0ErrorV2(
            "shared evaluator output violates the TE0 no-opposite/no-order contract"
        )
    if event.invalidation is None:  # pragma: no cover - prevalidated before evaluation
        raise RuntimeError("evaluable TE0 outcome lacks source invalidation")
    if outcome.initial_stop != float(event.invalidation):
        raise HistoricalThreeFamilyTe0ErrorV2(
            "shared evaluator initial stop differs from source invalidation"
        )
    gross = historical_return_to_micros_v2(outcome.gross_return)
    slippage = historical_return_to_micros_v2(outcome.slippage_return)
    fee = historical_return_to_micros_v2(outcome.fee_return)
    funding_return = historical_return_to_micros_v2(outcome.funding_return)
    net = historical_return_to_micros_v2(outcome.net_return)
    component_net = gross - slippage - fee + funding_return
    residual = net - component_net
    total_cost = slippage + fee - residual
    funding_count = sum(
        outcome.entry_time_ms < item.funding_time_ms < outcome.exit_time_ms
        for item in funding.rates
    )
    return HistoricalThreeFamilyTe0RowV2(
        **_te0_common_row_v2(event, policy),
        actual_open_time_ms=outcome.entry_time_ms,
        observed_bars=outcome.bars_held,
        observed_until_ms=outcome.exit_signal_observed_at_ms,
        evaluable=True,
        exclusion_reason="",
        exclusion_expected_open_time_ms=None,
        entry_action_label=outcome.entry_action_label,
        exit_action_label=outcome.exit_action_label,
        entry_time_ms=outcome.entry_time_ms,
        exit_time_ms=outcome.exit_time_ms,
        exit_signal_observed_at_ms=outcome.exit_signal_observed_at_ms,
        entry_price=Decimal(str(outcome.entry_price)),
        exit_price=Decimal(str(outcome.exit_price)),
        initial_stop=event.invalidation,
        active_stop=Decimal(str(outcome.active_stop)),
        exit_reason=outcome.exit_reason.value,
        execution_model=outcome.execution_model,
        bars_held=outcome.bars_held,
        gross_directional_return_micros=gross,
        slippage_return_micros=slippage,
        fee_return_micros=fee,
        funding_return_micros=funding_return,
        rounding_residual_micros=residual,
        total_cost_micros=total_cost,
        funding_event_count=funding_count,
        net_return_micros=net,
        mfe_micros=historical_return_to_micros_v2(outcome.mfe),
        mae_micros=historical_return_to_micros_v2(outcome.mae),
    )


def _te0_exclusion_from_evaluator_v2(
    event: HistoricalConsensusOutcomeEventV2,
    policy: HistoricalThreeFamilyTe0PolicyV2,
    exclusion: CounterfactualTechnicalExitExclusion,
) -> HistoricalThreeFamilyTe0RowV2:
    reason = _EVALUATOR_EXCLUSION_MAP[exclusion.reason]
    return _te0_exclusion_row_v2(
        event,
        policy,
        reason,
        expected_open_time_ms=exclusion.expected_open_time_ms,
        actual_open_time_ms=exclusion.actual_open_time_ms,
        observed_bars=exclusion.observed_bars,
        observed_until_ms=exclusion.observed_until_ms,
    )


def _te0_exclusion_from_outcome_v2(
    event: HistoricalConsensusOutcomeEventV2,
    policy: HistoricalThreeFamilyTe0PolicyV2,
    outcome: CounterfactualTechnicalExitOutcome,
    reason: HistoricalThreeFamilyTe0ExclusionV2,
) -> HistoricalThreeFamilyTe0RowV2:
    return _te0_exclusion_row_v2(
        event,
        policy,
        reason,
        expected_open_time_ms=event.decision_time_ms + 1,
        actual_open_time_ms=outcome.entry_time_ms,
        observed_bars=outcome.bars_held,
        observed_until_ms=outcome.exit_signal_observed_at_ms,
    )


def _te0_exclusion_row_v2(
    event: HistoricalConsensusOutcomeEventV2,
    policy: HistoricalThreeFamilyTe0PolicyV2,
    reason: HistoricalThreeFamilyTe0ExclusionV2,
    *,
    expected_open_time_ms: int,
    actual_open_time_ms: int | None = None,
    observed_bars: int = 0,
    observed_until_ms: int,
) -> HistoricalThreeFamilyTe0RowV2:
    return HistoricalThreeFamilyTe0RowV2(
        **_te0_common_row_v2(event, policy),
        actual_open_time_ms=actual_open_time_ms,
        observed_bars=observed_bars,
        observed_until_ms=observed_until_ms,
        evaluable=False,
        exclusion_reason=reason.value,
        exclusion_expected_open_time_ms=expected_open_time_ms,
        entry_action_label=None,
        exit_action_label=None,
        entry_time_ms=None,
        exit_time_ms=None,
        exit_signal_observed_at_ms=None,
        entry_price=None,
        exit_price=None,
        initial_stop=None,
        active_stop=None,
        exit_reason=None,
        execution_model=None,
        bars_held=None,
        gross_directional_return_micros=None,
        slippage_return_micros=None,
        fee_return_micros=None,
        funding_return_micros=None,
        rounding_residual_micros=None,
        total_cost_micros=None,
        funding_event_count=None,
        net_return_micros=None,
        mfe_micros=None,
        mae_micros=None,
    )


def _te0_common_row_v2(
    event: HistoricalConsensusOutcomeEventV2,
    policy: HistoricalThreeFamilyTe0PolicyV2,
) -> _Te0CommonRowV2:
    return {
        "topology_version": event.topology_version,
        "split": event.split,
        "asset": event.asset,
        "symbol": event.symbol,
        "event_id": event.event_id,
        "anchor_sha256": event.anchor_sha256,
        "consensus_rule_version": HISTORICAL_THREE_FAMILY_CONSENSUS_RULE_VERSION_V2,
        "te0_protocol_version": HISTORICAL_THREE_FAMILY_TE0_PROTOCOL_V2,
        "te0_policy_version": policy.policy_version,
        "te0_policy_sha256": policy.policy_sha256,
        "evaluator_policy_version": policy.evaluator_policy_version,
        "feature_contract_sha256": (historical_three_family_te0_feature_contract_sha256_v2()),
        "execution_contract_sha256": event.execution_contract_sha256,
        "state_class": event.state_class.value,
        "agreement_bucket": event.bucket,
        "primary_family": event.primary_family.value,
        "primary_direction": event.primary_direction.value,
        "directional_agreement_micros": event.directional_agreement_micros,
        "decision_time_ms": event.decision_time_ms,
        "source_invalidation": event.invalidation,
        "expected_entry_time_ms": event.decision_time_ms + 1,
    }


_EVALUATOR_EXCLUSION_MAP: Final = {
    TechnicalExitOutcomeExclusionReason.UNSUPPORTED_TIMEFRAME: (
        HistoricalThreeFamilyTe0ExclusionV2.UNSUPPORTED_TIMEFRAME
    ),
    TechnicalExitOutcomeExclusionReason.UNSUPPORTED_DIRECTION: (
        HistoricalThreeFamilyTe0ExclusionV2.UNSUPPORTED_DIRECTION
    ),
    TechnicalExitOutcomeExclusionReason.SPOT_SHORT_NOT_EXECUTABLE: (
        HistoricalThreeFamilyTe0ExclusionV2.SPOT_SHORT_NOT_EXECUTABLE
    ),
    TechnicalExitOutcomeExclusionReason.SPLIT_LEAKAGE: (
        HistoricalThreeFamilyTe0ExclusionV2.SPLIT_LEAKAGE
    ),
    TechnicalExitOutcomeExclusionReason.NEXT_BAR_UNAVAILABLE: (
        HistoricalThreeFamilyTe0ExclusionV2.NEXT_BAR_UNAVAILABLE
    ),
    TechnicalExitOutcomeExclusionReason.NEXT_BAR_NOT_CONTIGUOUS: (
        HistoricalThreeFamilyTe0ExclusionV2.NEXT_BAR_NOT_CONTIGUOUS
    ),
    TechnicalExitOutcomeExclusionReason.INVALID_CANDLE_SERIES: (
        HistoricalThreeFamilyTe0ExclusionV2.INVALID_CANDLE_SERIES
    ),
    TechnicalExitOutcomeExclusionReason.INVALID_INVALIDATION: (
        HistoricalThreeFamilyTe0ExclusionV2.SOURCE_INVALIDATION_INVALID
    ),
    TechnicalExitOutcomeExclusionReason.DATA_GAP: HistoricalThreeFamilyTe0ExclusionV2.DATA_GAP,
    TechnicalExitOutcomeExclusionReason.FEATURE_UNAVAILABLE: (
        HistoricalThreeFamilyTe0ExclusionV2.FEATURE_UNAVAILABLE
    ),
    TechnicalExitOutcomeExclusionReason.FEATURE_MISMATCH: (
        HistoricalThreeFamilyTe0ExclusionV2.FEATURE_MISMATCH
    ),
    TechnicalExitOutcomeExclusionReason.END_OF_DATA_BEFORE_EXIT: (
        HistoricalThreeFamilyTe0ExclusionV2.END_OF_DATA_BEFORE_EXIT
    ),
}


def _validate_te0_dataset_identity_v2(
    event: HistoricalConsensusOutcomeEventV2,
    dataset: KlineDataset,
) -> None:
    request = dataset.request
    if (
        request.market is not Market.FUTURES
        or request.symbol != event.symbol
        or request.dataset_alias != event.asset
        or request.interval != "5m"
    ):
        raise HistoricalThreeFamilyTe0ErrorV2(
            "TE0 dataset identity differs from the authenticated consensus event"
        )


def _validate_te0_row_identity_v2(row: HistoricalThreeFamilyTe0RowV2) -> None:
    policy = build_historical_three_family_te0_policy_v2()
    if (
        row.topology_version != HISTORICAL_THREE_FAMILY_PRIMARY_TOPOLOGY_V2
        or row.consensus_rule_version != HISTORICAL_THREE_FAMILY_CONSENSUS_RULE_VERSION_V2
        or row.te0_protocol_version != HISTORICAL_THREE_FAMILY_TE0_PROTOCOL_V2
        or row.te0_policy_version != policy.policy_version
        or row.te0_policy_sha256 != policy.policy_sha256
        or row.evaluator_policy_version != policy.evaluator_policy_version
        or row.feature_contract_sha256 != historical_three_family_te0_feature_contract_sha256_v2()
    ):
        raise HistoricalThreeFamilyTe0ErrorV2("TE0 row identity differs from frozen contracts")
    if row.expected_entry_time_ms != row.decision_time_ms + 1:
        raise HistoricalThreeFamilyTe0ErrorV2("TE0 expected entry is not the next bar open")
    if _ALLOWED_STATE_TO_BUCKET_AND_SIDE.get(row.state_class) != (
        row.agreement_bucket,
        row.primary_direction,
    ):
        raise HistoricalThreeFamilyTe0ErrorV2(
            "TE0 cannot pool conflicted-majority or unsupported topology rows"
        )
    expected_family = (
        "pullback_long" if row.primary_direction == Direction.LONG.value else "pullback_short"
    )
    if row.primary_family != expected_family:
        raise HistoricalThreeFamilyTe0ErrorV2("TE0 source family and side are inconsistent")
    if (
        _SHA256_RE.fullmatch(row.event_id) is None
        or _SHA256_RE.fullmatch(row.anchor_sha256) is None
        or _SHA256_RE.fullmatch(row.execution_contract_sha256) is None
        or row.execution_contract_sha256
        != build_historical_execution_contract_v2().execution_contract_sha256
    ):
        raise HistoricalThreeFamilyTe0ErrorV2("TE0 row digest authority is invalid")
    if not -1_000_000 <= row.directional_agreement_micros <= 1_000_000:
        raise HistoricalThreeFamilyTe0ErrorV2("TE0 directional agreement is out of range")
    if (row.directional_agreement_micros > 0) != (row.primary_direction == Direction.LONG.value):
        raise HistoricalThreeFamilyTe0ErrorV2("TE0 directional agreement has the wrong side")
    split_start_ms, split_end_ms = historical_three_family_split_bounds_v2(row.split)
    if not split_start_ms <= row.decision_time_ms < split_end_ms:
        raise HistoricalThreeFamilyTe0ErrorV2("TE0 decision lies outside its source split")
    if row.source_invalidation is not None and (
        type(row.source_invalidation) is not Decimal
        or not row.source_invalidation.is_finite()
        or row.source_invalidation <= 0
    ):
        raise HistoricalThreeFamilyTe0ErrorV2("TE0 source invalidation is invalid")
    if row.observed_bars < 0 or row.observed_until_ms < row.decision_time_ms:
        raise HistoricalThreeFamilyTe0ErrorV2("TE0 observation fields are invalid")
    if row.actual_open_time_ms is not None and row.actual_open_time_ms < 0:
        raise HistoricalThreeFamilyTe0ErrorV2("TE0 actual open time must be nonnegative")


def _validate_te0_row_claims_v2(row: HistoricalThreeFamilyTe0RowV2) -> None:
    if (
        row.source_information_only is not True
        or row.opposite_signal_evaluated is not False
        or row.historical_only is not True
        or row.probability is not False
        or row.probability_calibrated is not False
        or row.promoting is not False
        or row.order_placement is not False
        or row.portfolio_equity_claim is not False
        or row.drawdown_claim is not False
    ):
        raise HistoricalThreeFamilyTe0ErrorV2("TE0 row contains a prohibited claim")


def _validate_evaluable_te0_row_v2(row: HistoricalThreeFamilyTe0RowV2) -> None:
    result_values = (
        row.entry_action_label,
        row.exit_action_label,
        row.entry_time_ms,
        row.exit_time_ms,
        row.exit_signal_observed_at_ms,
        row.entry_price,
        row.exit_price,
        row.initial_stop,
        row.active_stop,
        row.exit_reason,
        row.execution_model,
        row.bars_held,
        row.gross_directional_return_micros,
        row.slippage_return_micros,
        row.fee_return_micros,
        row.funding_return_micros,
        row.rounding_residual_micros,
        row.total_cost_micros,
        row.funding_event_count,
        row.net_return_micros,
        row.mfe_micros,
        row.mae_micros,
    )
    if (
        row.exclusion_reason
        or row.exclusion_expected_open_time_ms is not None
        or any(value is None for value in result_values)
    ):
        raise HistoricalThreeFamilyTe0ErrorV2(
            "evaluable TE0 row requires complete result fields and no exclusion"
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
        row.mfe_micros,
        row.mae_micros,
    )
    if any(type(value) is not int for value in economic):
        raise HistoricalThreeFamilyTe0ErrorV2("TE0 economic fields must be exact integers")
    gross = cast(int, row.gross_directional_return_micros)
    slippage = cast(int, row.slippage_return_micros)
    fee = cast(int, row.fee_return_micros)
    funding = cast(int, row.funding_return_micros)
    residual = cast(int, row.rounding_residual_micros)
    total_cost = cast(int, row.total_cost_micros)
    net = cast(int, row.net_return_micros)
    if total_cost != slippage + fee - residual or gross - total_cost + funding != net:
        raise HistoricalThreeFamilyTe0ErrorV2("TE0 cost components do not reconcile")
    if (
        slippage < 0
        or fee < 0
        or total_cost < 0
        or cast(int, row.funding_event_count) < 0
        or cast(int, row.mfe_micros) < 0
        or cast(int, row.mae_micros) > 0
    ):
        raise HistoricalThreeFamilyTe0ErrorV2("TE0 cost or excursion signs are invalid")
    if row.exit_reason == "opposite_signal":
        raise HistoricalThreeFamilyTe0ErrorV2("TE0 cannot contain an opposite-signal exit")
    if row.bars_held is None or not 1 <= row.bars_held <= _TE0_MAX_HOLDING_BARS:
        raise HistoricalThreeFamilyTe0ErrorV2("TE0 held bars exceed the frozen limit")
    if row.initial_stop != row.source_invalidation:
        raise HistoricalThreeFamilyTe0ErrorV2(
            "TE0 initial stop differs from the source structural invalidation"
        )
    expected_entry_action = (
        "FUTURES_LONG" if row.primary_direction == Direction.LONG.value else "FUTURES_SHORT"
    )
    expected_exit_action = f"{expected_entry_action}_EXIT"
    if (
        row.entry_action_label != expected_entry_action
        or row.exit_action_label != expected_exit_action
    ):
        raise HistoricalThreeFamilyTe0ErrorV2("TE0 action labels violate Futures semantics")


def _validate_excluded_te0_row_v2(row: HistoricalThreeFamilyTe0RowV2) -> None:
    try:
        HistoricalThreeFamilyTe0ExclusionV2(row.exclusion_reason)
    except ValueError as exc:
        raise HistoricalThreeFamilyTe0ErrorV2(
            "excluded TE0 row requires a frozen exclusion reason"
        ) from exc
    if row.exclusion_expected_open_time_ms is None or row.exclusion_expected_open_time_ms < 0:
        raise HistoricalThreeFamilyTe0ErrorV2(
            "excluded TE0 row requires the evaluator's expected open time"
        )
    result_values = (
        row.entry_action_label,
        row.exit_action_label,
        row.entry_time_ms,
        row.exit_time_ms,
        row.exit_signal_observed_at_ms,
        row.entry_price,
        row.exit_price,
        row.initial_stop,
        row.active_stop,
        row.exit_reason,
        row.execution_model,
        row.bars_held,
        row.gross_directional_return_micros,
        row.slippage_return_micros,
        row.fee_return_micros,
        row.funding_return_micros,
        row.rounding_residual_micros,
        row.total_cost_micros,
        row.funding_event_count,
        row.net_return_micros,
        row.mfe_micros,
        row.mae_micros,
    )
    if any(value is not None for value in result_values):
        raise HistoricalThreeFamilyTe0ErrorV2("excluded TE0 row must not retain result economics")


_TE0_COLUMNS: Final = tuple(field.name for field in fields(HistoricalThreeFamilyTe0RowV2))


def _technical_exit_te0_csv_bytes_v2(
    rows: Sequence[HistoricalThreeFamilyTe0RowV2],
) -> bytes:
    ordered = tuple(
        sorted(
            rows,
            key=lambda item: (
                item.split,
                item.asset,
                item.decision_time_ms,
                item.event_id,
            ),
        )
    )
    buffer = io.StringIO(newline="")
    writer: csv.DictWriter[str] = csv.DictWriter(
        buffer,
        fieldnames=list(_TE0_COLUMNS),
        extrasaction="raise",
        lineterminator="\n",
    )
    writer.writeheader()
    for row in ordered:
        document: dict[str, str] = {}
        for item in fields(row):
            value = getattr(row, item.name)
            if value is None:
                document[item.name] = ""
            elif type(value) is bool:
                document[item.name] = "true" if value else "false"
            else:
                document[item.name] = str(value)
        writer.writerow(document)
    payload = buffer.getvalue().encode("utf-8")
    if b"\r" in payload or not payload.endswith(b"\n"):
        raise HistoricalThreeFamilyTe0ErrorV2("TE0 CSV serialization must be LF-only UTF-8")
    return payload


def _parse_technical_exit_te0_csv_v2(
    raw: bytes,
) -> tuple[HistoricalThreeFamilyTe0RowV2, ...]:
    if not raw or b"\r" in raw or not raw.endswith(b"\n"):
        raise HistoricalThreeFamilyTe0ErrorV2("TE0 CSV must be non-empty canonical LF-only bytes")
    try:
        reader = csv.DictReader(io.StringIO(raw.decode("utf-8"), newline=""), strict=True)
        if tuple(reader.fieldnames or ()) != _TE0_COLUMNS:
            raise HistoricalThreeFamilyTe0ErrorV2("TE0 CSV header differs from the frozen schema")
        records: list[dict[str, str]] = []
        for record in reader:
            if len(records) >= _TE0_ROW_CAP:
                raise HistoricalThreeFamilyTe0ErrorV2("TE0 CSV exceeds the bounded row cap")
            if None in record or any(value is None for value in record.values()):
                raise HistoricalThreeFamilyTe0ErrorV2("TE0 CSV has a surplus or missing column")
            records.append(cast(dict[str, str], record))
    except (UnicodeError, csv.Error) as exc:
        raise HistoricalThreeFamilyTe0ErrorV2("TE0 CSV is not valid UTF-8 CSV") from exc
    rows: list[HistoricalThreeFamilyTe0RowV2] = []
    for record in records:
        rows.append(
            HistoricalThreeFamilyTe0RowV2(
                topology_version=record["topology_version"],
                split=record["split"],
                asset=record["asset"],
                symbol=record["symbol"],
                event_id=record["event_id"],
                anchor_sha256=record["anchor_sha256"],
                consensus_rule_version=record["consensus_rule_version"],
                te0_protocol_version=record["te0_protocol_version"],
                te0_policy_version=record["te0_policy_version"],
                te0_policy_sha256=record["te0_policy_sha256"],
                evaluator_policy_version=record["evaluator_policy_version"],
                feature_contract_sha256=record["feature_contract_sha256"],
                execution_contract_sha256=record["execution_contract_sha256"],
                state_class=record["state_class"],
                agreement_bucket=record["agreement_bucket"],
                primary_family=record["primary_family"],
                primary_direction=record["primary_direction"],
                directional_agreement_micros=_parse_te0_int_v2(
                    record["directional_agreement_micros"],
                    "directional_agreement_micros",
                ),
                decision_time_ms=_parse_te0_int_v2(record["decision_time_ms"], "decision_time_ms"),
                source_invalidation=_parse_te0_optional_decimal_v2(
                    record["source_invalidation"], "source_invalidation"
                ),
                expected_entry_time_ms=_parse_te0_int_v2(
                    record["expected_entry_time_ms"], "expected_entry_time_ms"
                ),
                actual_open_time_ms=_parse_te0_optional_int_v2(
                    record["actual_open_time_ms"], "actual_open_time_ms"
                ),
                observed_bars=_parse_te0_int_v2(record["observed_bars"], "observed_bars"),
                observed_until_ms=_parse_te0_int_v2(
                    record["observed_until_ms"], "observed_until_ms"
                ),
                evaluable=_parse_te0_bool_v2(record["evaluable"], "evaluable"),
                exclusion_reason=record["exclusion_reason"],
                exclusion_expected_open_time_ms=_parse_te0_optional_int_v2(
                    record["exclusion_expected_open_time_ms"],
                    "exclusion_expected_open_time_ms",
                ),
                entry_action_label=record["entry_action_label"] or None,
                exit_action_label=record["exit_action_label"] or None,
                entry_time_ms=_parse_te0_optional_int_v2(record["entry_time_ms"], "entry_time_ms"),
                exit_time_ms=_parse_te0_optional_int_v2(record["exit_time_ms"], "exit_time_ms"),
                exit_signal_observed_at_ms=_parse_te0_optional_int_v2(
                    record["exit_signal_observed_at_ms"],
                    "exit_signal_observed_at_ms",
                ),
                entry_price=_parse_te0_optional_decimal_v2(record["entry_price"], "entry_price"),
                exit_price=_parse_te0_optional_decimal_v2(record["exit_price"], "exit_price"),
                initial_stop=_parse_te0_optional_decimal_v2(record["initial_stop"], "initial_stop"),
                active_stop=_parse_te0_optional_decimal_v2(record["active_stop"], "active_stop"),
                exit_reason=record["exit_reason"] or None,
                execution_model=record["execution_model"] or None,
                bars_held=_parse_te0_optional_int_v2(record["bars_held"], "bars_held"),
                gross_directional_return_micros=_parse_te0_optional_int_v2(
                    record["gross_directional_return_micros"],
                    "gross_directional_return_micros",
                ),
                slippage_return_micros=_parse_te0_optional_int_v2(
                    record["slippage_return_micros"], "slippage_return_micros"
                ),
                fee_return_micros=_parse_te0_optional_int_v2(
                    record["fee_return_micros"], "fee_return_micros"
                ),
                funding_return_micros=_parse_te0_optional_int_v2(
                    record["funding_return_micros"], "funding_return_micros"
                ),
                rounding_residual_micros=_parse_te0_optional_int_v2(
                    record["rounding_residual_micros"], "rounding_residual_micros"
                ),
                total_cost_micros=_parse_te0_optional_int_v2(
                    record["total_cost_micros"], "total_cost_micros"
                ),
                funding_event_count=_parse_te0_optional_int_v2(
                    record["funding_event_count"], "funding_event_count"
                ),
                net_return_micros=_parse_te0_optional_int_v2(
                    record["net_return_micros"], "net_return_micros"
                ),
                mfe_micros=_parse_te0_optional_int_v2(record["mfe_micros"], "mfe_micros"),
                mae_micros=_parse_te0_optional_int_v2(record["mae_micros"], "mae_micros"),
                source_information_only=cast(
                    Literal[True],
                    _parse_te0_bool_v2(
                        record["source_information_only"], "source_information_only"
                    ),
                ),
                opposite_signal_evaluated=cast(
                    Literal[False],
                    _parse_te0_bool_v2(
                        record["opposite_signal_evaluated"], "opposite_signal_evaluated"
                    ),
                ),
                historical_only=cast(
                    Literal[True], _parse_te0_bool_v2(record["historical_only"], "historical_only")
                ),
                probability=cast(
                    Literal[False], _parse_te0_bool_v2(record["probability"], "probability")
                ),
                probability_calibrated=cast(
                    Literal[False],
                    _parse_te0_bool_v2(record["probability_calibrated"], "probability_calibrated"),
                ),
                promoting=cast(
                    Literal[False], _parse_te0_bool_v2(record["promoting"], "promoting")
                ),
                order_placement=cast(
                    Literal[False], _parse_te0_bool_v2(record["order_placement"], "order_placement")
                ),
                portfolio_equity_claim=cast(
                    Literal[False],
                    _parse_te0_bool_v2(record["portfolio_equity_claim"], "portfolio_equity_claim"),
                ),
                drawdown_claim=cast(
                    Literal[False], _parse_te0_bool_v2(record["drawdown_claim"], "drawdown_claim")
                ),
            )
        )
    if raw != _technical_exit_te0_csv_bytes_v2(rows):
        raise HistoricalThreeFamilyTe0ErrorV2(
            "TE0 CSV is not in canonical row order or representation"
        )
    return tuple(rows)


def _validate_complete_te0_census_v2(
    events: Sequence[HistoricalConsensusOutcomeEventV2],
    rows: Sequence[HistoricalThreeFamilyTe0RowV2],
) -> None:
    expected = {event.event_id for event in events}
    actual = [row.event_id for row in rows]
    if len(actual) != len(set(actual)) or set(actual) != expected:
        raise HistoricalThreeFamilyTe0ErrorV2(
            "TE0 requires exactly one result or exclusion for every admitted event"
        )


def _te0_results_document_v2(
    *,
    loaded: LoadedHistoricalConsensusV2,
    rows: Sequence[HistoricalThreeFamilyTe0RowV2],
    funding_authority: LoadedHistoricalFundingAuthorityV2 | None,
    downstream_code_freeze_manifest_sha256: str,
    technical_exit_sha256: str,
    policy: HistoricalThreeFamilyTe0PolicyV2,
) -> dict[str, object]:
    exclusions = Counter(row.exclusion_reason for row in rows if not row.evaluable)
    exits = Counter(cast(str, row.exit_reason) for row in rows if row.evaluable)
    return {
        "admitted_events": len(loaded.events),
        "census_manifest_sha256": loaded.census_manifest_sha256,
        "census_rows": loaded.census_rows,
        "conflicted_majority_included": False,
        "consensus_rule_version": HISTORICAL_THREE_FAMILY_CONSENSUS_RULE_VERSION_V2,
        "downstream_code_freeze_manifest_sha256": downstream_code_freeze_manifest_sha256,
        "consensus_sha256": loaded.consensus_sha256,
        "drawdown_claim": False,
        "evaluable_rows": sum(row.evaluable for row in rows),
        "evaluator_policy_version": policy.evaluator_policy_version,
        "execution_contract_sha256": loaded.execution_contract_sha256,
        "exit_reason_counts": dict(sorted(exits.items())),
        "exclusion_counts": dict(sorted(exclusions.items())),
        "experiment_contract_sha256": loaded.experiment_contract_sha256,
        "feature_contract_sha256": historical_three_family_te0_feature_contract_sha256_v2(),
        "funding_authority_manifest_sha256": (
            None if funding_authority is None else funding_authority.manifest_sha256
        ),
        "funding_missing_is_zero": False,
        "historical_only": True,
        "inference_complete": False,
        "one_result_or_exclusion_per_event": True,
        "opposite_signal_evaluated": False,
        "order_placement": False,
        "overlaps_fixed_horizon_outcomes": True,
        "policy": _te0_policy_document_v2(policy),
        "portfolio_equity_claim": False,
        "probability": False,
        "probability_calibrated": False,
        "promoting": False,
        "protocol": HISTORICAL_THREE_FAMILY_TE0_PROTOCOL_V2,
        "result_rows": len(rows),
        "schema_version": HISTORICAL_THREE_FAMILY_TE0_SCHEMA_VERSION_V2,
        "technical_exit_te0_sha256": technical_exit_sha256,
        "topology_amendment_sha256": loaded.topology_amendment_sha256,
        "topology_version": HISTORICAL_THREE_FAMILY_PRIMARY_TOPOLOGY_V2,
    }


def _te0_manifest_document_v2(
    *,
    loaded: LoadedHistoricalConsensusV2,
    panel: LoadedHistoricalKlinePanelV2,
    funding_authority: LoadedHistoricalFundingAuthorityV2 | None,
    downstream_code_freeze_manifest_sha256: str,
    policy: HistoricalThreeFamilyTe0PolicyV2,
    output_hashes: Mapping[str, str],
) -> dict[str, object]:
    if set(output_hashes) != set(_OUTPUT_NAMES):
        raise HistoricalThreeFamilyTe0ErrorV2("TE0 manifest output hash set is incomplete")
    return {
        "census_manifest_sha256": loaded.census_manifest_sha256,
        "conflicted_majority_included": False,
        "consensus_rule_version": HISTORICAL_THREE_FAMILY_CONSENSUS_RULE_VERSION_V2,
        "downstream_code_freeze_manifest_sha256": downstream_code_freeze_manifest_sha256,
        "consensus_sha256": loaded.consensus_sha256,
        "drawdown_claim": False,
        "evaluator_policy_version": policy.evaluator_policy_version,
        "execution_contract_sha256": loaded.execution_contract_sha256,
        "experiment_contract_sha256": loaded.experiment_contract_sha256,
        "feature_contract": {
            "canonical_sha256": historical_three_family_te0_feature_contract_sha256_v2(),
            "protocol": HISTORICAL_THREE_FAMILY_TE0_FEATURE_PROTOCOL_V2,
        },
        "forward_data_read": True,
        "funding_authority": {
            "files": (
                []
                if funding_authority is None
                else [asdict(item) for item in funding_authority.bindings]
            ),
            "manifest_sha256": (
                None if funding_authority is None else funding_authority.manifest_sha256
            ),
            "missing_dataset_policy": "EXPLICIT_EXCLUSION_NOT_ZERO",
        },
        "historical_only": True,
        "inputs": {
            "futures_klines": [asdict(item) for item in panel.authorities],
        },
        "opposite_signal_evaluated": False,
        "order_placement": False,
        "outputs": dict(sorted(output_hashes.items())),
        "policy": _te0_policy_document_v2(policy),
        "portfolio_equity_claim": False,
        "probability": False,
        "promoting": False,
        "protocol": HISTORICAL_THREE_FAMILY_TE0_PROTOCOL_V2,
        "schema_version": HISTORICAL_THREE_FAMILY_TE0_SCHEMA_VERSION_V2,
        "topology_amendment_sha256": loaded.topology_amendment_sha256,
        "topology_version": HISTORICAL_THREE_FAMILY_PRIMARY_TOPOLOGY_V2,
    }


def _te0_policy_document_v2(
    policy: HistoricalThreeFamilyTe0PolicyV2,
) -> dict[str, object]:
    return {
        "evaluator_policy_version": policy.evaluator_policy_version,
        "max_holding_bars": policy.max_holding_bars,
        "opposite_signal_evaluated": policy.opposite_signal_evaluated,
        "policy_sha256": policy.policy_sha256,
        "policy_version": policy.policy_version,
        "rule": policy.rule,
        "source_invalidation_initial_stop": policy.source_invalidation_initial_stop,
        "trailing_activation_r": policy.trailing_activation_r,
        "trailing_atr_multiple": policy.trailing_atr_multiple,
        "trend_failure_bars": policy.trend_failure_bars,
    }


def _validate_optional_funding_authority_v2(
    manifest_path: str | Path | None,
    expected_sha256: str | None,
) -> None:
    if (manifest_path is None) != (expected_sha256 is None):
        raise HistoricalThreeFamilyTe0ErrorV2(
            "funding authority path and frozen SHA-256 must be supplied together"
        )


def _validate_loaded_te0_manifest_v2(
    document: Mapping[str, object],
    *,
    expected_census_manifest_sha256: str,
    expected_experiment_contract_sha256: str,
    expected_topology_amendment_sha256: str,
    expected_funding_authority_manifest_sha256: str,
    expected_downstream_code_freeze_manifest_sha256: str,
) -> None:
    policy = build_historical_three_family_te0_policy_v2()
    required: Mapping[str, object] = {
        "census_manifest_sha256": expected_census_manifest_sha256,
        "conflicted_majority_included": False,
        "consensus_rule_version": HISTORICAL_THREE_FAMILY_CONSENSUS_RULE_VERSION_V2,
        "downstream_code_freeze_manifest_sha256": (expected_downstream_code_freeze_manifest_sha256),
        "drawdown_claim": False,
        "evaluator_policy_version": policy.evaluator_policy_version,
        "execution_contract_sha256": (
            build_historical_execution_contract_v2().execution_contract_sha256
        ),
        "experiment_contract_sha256": expected_experiment_contract_sha256,
        "forward_data_read": True,
        "historical_only": True,
        "opposite_signal_evaluated": False,
        "order_placement": False,
        "portfolio_equity_claim": False,
        "probability": False,
        "promoting": False,
        "protocol": HISTORICAL_THREE_FAMILY_TE0_PROTOCOL_V2,
        "schema_version": HISTORICAL_THREE_FAMILY_TE0_SCHEMA_VERSION_V2,
        "topology_amendment_sha256": expected_topology_amendment_sha256,
        "topology_version": HISTORICAL_THREE_FAMILY_PRIMARY_TOPOLOGY_V2,
    }
    for key, expected in required.items():
        if document.get(key) != expected:
            raise HistoricalThreeFamilyTe0ErrorV2(
                f"TE0 manifest field {key} differs from frozen authority"
            )
    _require_sha256_te0_v2(document.get("consensus_sha256"), "TE0 consensus SHA-256")
    funding = _require_te0_mapping_v2(document.get("funding_authority"), "TE0 funding")
    if (
        funding.get("manifest_sha256") != expected_funding_authority_manifest_sha256
        or funding.get("missing_dataset_policy") != "EXPLICIT_EXCLUSION_NOT_ZERO"
    ):
        raise HistoricalThreeFamilyTe0ErrorV2("TE0 funding authority differs")
    outputs = _require_te0_mapping_v2(document.get("outputs"), "TE0 outputs")
    if set(outputs) != set(_OUTPUT_NAMES):
        raise HistoricalThreeFamilyTe0ErrorV2("TE0 output hash set is not exact")
    for name, digest in outputs.items():
        _require_sha256_te0_v2(digest, f"TE0 output {name}")


def _validate_loaded_te0_results_v2(
    document: Mapping[str, object],
    *,
    manifest: Mapping[str, object],
    rows: tuple[HistoricalThreeFamilyTe0RowV2, ...],
    technical_exit_sha256: str,
    expected_funding_authority_manifest_sha256: str,
    expected_downstream_code_freeze_manifest_sha256: str,
) -> None:
    required: Mapping[str, object] = {
        "census_manifest_sha256": manifest.get("census_manifest_sha256"),
        "conflicted_majority_included": False,
        "consensus_rule_version": HISTORICAL_THREE_FAMILY_CONSENSUS_RULE_VERSION_V2,
        "downstream_code_freeze_manifest_sha256": (expected_downstream_code_freeze_manifest_sha256),
        "consensus_sha256": manifest.get("consensus_sha256"),
        "drawdown_claim": False,
        "execution_contract_sha256": manifest.get("execution_contract_sha256"),
        "experiment_contract_sha256": manifest.get("experiment_contract_sha256"),
        "funding_authority_manifest_sha256": expected_funding_authority_manifest_sha256,
        "funding_missing_is_zero": False,
        "historical_only": True,
        "inference_complete": False,
        "one_result_or_exclusion_per_event": True,
        "opposite_signal_evaluated": False,
        "order_placement": False,
        "overlaps_fixed_horizon_outcomes": True,
        "portfolio_equity_claim": False,
        "probability": False,
        "probability_calibrated": False,
        "promoting": False,
        "protocol": HISTORICAL_THREE_FAMILY_TE0_PROTOCOL_V2,
        "schema_version": HISTORICAL_THREE_FAMILY_TE0_SCHEMA_VERSION_V2,
        "technical_exit_te0_sha256": technical_exit_sha256,
        "topology_amendment_sha256": manifest.get("topology_amendment_sha256"),
        "topology_version": HISTORICAL_THREE_FAMILY_PRIMARY_TOPOLOGY_V2,
    }
    for key, expected in required.items():
        if document.get(key) != expected:
            raise HistoricalThreeFamilyTe0ErrorV2(
                f"TE0 results field {key} differs from frozen authority"
            )
    admitted = _require_nonnegative_te0_int_v2(document.get("admitted_events"), "admitted_events")
    result_rows = _require_nonnegative_te0_int_v2(document.get("result_rows"), "result_rows")
    evaluable = _require_nonnegative_te0_int_v2(document.get("evaluable_rows"), "evaluable_rows")
    if (
        result_rows != len(rows)
        or result_rows != admitted
        or evaluable != sum(row.evaluable for row in rows)
        or len({row.event_id for row in rows}) != admitted
    ):
        raise HistoricalThreeFamilyTe0ErrorV2("TE0 result counts do not reconcile to exact rows")


def _require_exact_te0_artifact_files_v2(root: Path) -> None:
    try:
        entries = tuple(root.iterdir())
    except OSError as exc:
        raise HistoricalThreeFamilyTe0ErrorV2(
            f"cannot inspect TE0 artifact directory: {root}"
        ) from exc
    if any(entry.is_symlink() or not entry.is_file() for entry in entries) or {
        entry.name for entry in entries
    } != set(_PUBLISHED_NAMES):
        raise HistoricalThreeFamilyTe0ErrorV2(
            "TE0 artifact directory does not contain the exact published file set"
        )


def _read_te0_bytes_v2(path: Path, label: str) -> bytes:
    try:
        return path.read_bytes()
    except OSError as exc:
        raise HistoricalThreeFamilyTe0ErrorV2(f"cannot read {label}: {path}") from exc


def _decode_canonical_te0_json_v2(raw: bytes, label: str) -> dict[str, object]:
    try:
        value = json.loads(raw)
    except (json.JSONDecodeError, UnicodeError) as exc:
        raise HistoricalThreeFamilyTe0ErrorV2(f"{label} is not valid UTF-8 JSON") from exc
    document = _require_te0_mapping_v2(value, label)
    try:
        canonical = canonical_json_line(document)
    except (TypeError, ValueError) as exc:
        raise HistoricalThreeFamilyTe0ErrorV2(
            f"{label} contains unsupported protocol JSON"
        ) from exc
    if raw != canonical:
        raise HistoricalThreeFamilyTe0ErrorV2(f"{label} must be canonical RFC 8785 JSONL")
    return dict(document)


def _require_te0_mapping_v2(value: object, label: str) -> Mapping[str, object]:
    if not isinstance(value, dict):
        raise HistoricalThreeFamilyTe0ErrorV2(f"{label} must be an object")
    return cast(dict[str, object], value)


def _require_sha256_te0_v2(value: object, label: str) -> str:
    if not isinstance(value, str) or _SHA256_RE.fullmatch(value) is None:
        raise HistoricalThreeFamilyTe0ErrorV2(f"{label} must be a lowercase SHA-256 digest")
    return value


def _parse_te0_bool_v2(value: str, label: str) -> bool:
    if value == "true":
        return True
    if value == "false":
        return False
    raise HistoricalThreeFamilyTe0ErrorV2(f"{label} must be true or false")


def _parse_te0_int_v2(value: str, label: str) -> int:
    if not value or value.startswith("+") or (value.startswith("0") and value != "0"):
        raise HistoricalThreeFamilyTe0ErrorV2(f"{label} must be canonical integer text")
    try:
        parsed = int(value)
    except ValueError as exc:
        raise HistoricalThreeFamilyTe0ErrorV2(f"{label} must be canonical integer text") from exc
    if str(parsed) != value:
        raise HistoricalThreeFamilyTe0ErrorV2(f"{label} must be canonical integer text")
    return parsed


def _parse_te0_optional_int_v2(value: str, label: str) -> int | None:
    return None if value == "" else _parse_te0_int_v2(value, label)


def _parse_te0_optional_decimal_v2(value: str, label: str) -> Decimal | None:
    if value == "":
        return None
    if value.startswith("+") or value.strip() != value:
        raise HistoricalThreeFamilyTe0ErrorV2(f"{label} must be canonical finite decimal text")
    try:
        parsed = Decimal(value)
    except InvalidOperation as exc:
        raise HistoricalThreeFamilyTe0ErrorV2(
            f"{label} must be canonical finite decimal text"
        ) from exc
    if not parsed.is_finite():
        raise HistoricalThreeFamilyTe0ErrorV2(f"{label} must be finite")
    return parsed


def _require_nonnegative_te0_int_v2(value: object, label: str) -> int:
    if type(value) is not int or value < 0:
        raise HistoricalThreeFamilyTe0ErrorV2(f"{label} must be a nonnegative integer")
    return value


def _publish_te0_artifacts_v2(target: Path, payloads: Mapping[str, bytes]) -> None:
    if set(payloads) != _PUBLISHED_NAMES:
        raise HistoricalThreeFamilyTe0ErrorV2(
            "TE0 publication requires exactly CSV, results, and manifest artifacts"
        )
    if target.exists():
        raise HistoricalThreeFamilyTe0ErrorV2("TE0 output requires a fresh target directory")
    target.parent.mkdir(parents=True, exist_ok=True)
    staging = Path(tempfile.mkdtemp(prefix=f".{target.name}.", dir=target.parent))
    try:
        for name, payload in payloads.items():
            (staging / name).write_bytes(payload)
        os.replace(staging, target)
    except OSError as exc:
        shutil.rmtree(staging, ignore_errors=True)
        raise HistoricalThreeFamilyTe0ErrorV2("cannot atomically publish TE0 artifacts") from exc


def _sha256_bytes_v2(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _parser_v2() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run frozen historical three-family TE0 technical exits"
    )
    parser.add_argument("--consensus-csv", required=True)
    parser.add_argument("--census-manifest", required=True)
    parser.add_argument("--expected-census-manifest-sha256", required=True)
    parser.add_argument("--expected-experiment-contract-sha256", required=True)
    parser.add_argument("--expected-topology-amendment-sha256", required=True)
    parser.add_argument("--data-dir", required=True)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--workspace-root", required=True)
    parser.add_argument("--downstream-code-freeze-manifest", required=True)
    parser.add_argument("--expected-downstream-code-freeze-manifest-sha256", required=True)
    parser.add_argument("--funding-authority-manifest", required=True)
    parser.add_argument("--expected-funding-authority-manifest-sha256", required=True)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser_v2().parse_args(argv)
    artifacts = run_historical_three_family_te0_v2(
        consensus_path=args.consensus_csv,
        census_manifest_path=args.census_manifest,
        expected_census_manifest_sha256=args.expected_census_manifest_sha256,
        expected_experiment_contract_sha256=args.expected_experiment_contract_sha256,
        expected_topology_amendment_sha256=args.expected_topology_amendment_sha256,
        data_root=args.data_dir,
        output_dir=args.output_dir,
        workspace_root=args.workspace_root,
        downstream_code_freeze_manifest_path=args.downstream_code_freeze_manifest,
        expected_downstream_code_freeze_manifest_sha256=(
            args.expected_downstream_code_freeze_manifest_sha256
        ),
        funding_authority_manifest_path=args.funding_authority_manifest,
        expected_funding_authority_manifest_sha256=(
            args.expected_funding_authority_manifest_sha256
        ),
    )
    print(artifacts.output_dir)
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
