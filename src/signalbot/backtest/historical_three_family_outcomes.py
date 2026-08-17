"""Authenticated fixed-horizon outcomes for the historical three-family census.

This module is deliberately a sibling of the outcome-blind census.  It cannot
produce or alter consensus decisions, fit a threshold, place an order, or make
a probability claim.  A caller must supply the externally frozen SHA-256 of
the complete census manifest before any forward candle is read.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import io
import json
import math
import os
import re
import shutil
import tempfile
from collections import Counter
from collections.abc import Mapping, Sequence
from dataclasses import asdict, dataclass, fields
from decimal import ROUND_HALF_UP, Decimal, InvalidOperation
from enum import StrEnum
from pathlib import Path
from typing import Final, Literal, cast

from signalbot.backtest.dataset import (
    DatasetValidationError,
    KlineDataset,
    read_dataset_manifest,
    read_kline_csv,
    sha256_file,
    verify_dataset_manifest,
)
from signalbot.backtest.downstream_code_freeze import load_downstream_code_freeze_v1
from signalbot.backtest.engine import calculate_execution_returns, calculate_funding_return
from signalbot.backtest.funding import (
    FundingDataset,
    FundingValidationError,
    funding_sha256,
    read_funding_csv,
)
from signalbot.backtest.historical_three_family_census import (
    HISTORICAL_THREE_FAMILY_CENSUS_PROTOCOL_V2,
    HISTORICAL_THREE_FAMILY_CENSUS_SCHEMA_VERSION_V2,
    HistoricalConsensusCensusRowV2,
)
from signalbot.domain.enums import Direction, Market, SignalFamily
from signalbot.domain.models import Candle
from signalbot.r4b_v2.canonical import canonical_json_line
from signalbot.r4b_v2.capture.models import VenueV2
from signalbot.r4b_v2.protocol.decision_clock import FIVE_MINUTE_MS_V2
from signalbot.r4b_v2.research.historical_three_family_bootstrap import (
    HISTORICAL_THREE_FAMILY_FULL_CALENDAR_SCHEDULE_SHA256_V2,
)
from signalbot.r4b_v2.research.historical_three_family_outcome_audit import (
    HISTORICAL_THREE_FAMILY_OUTCOME_AUDIT_VERSION_V2,
    HISTORICAL_THREE_FAMILY_OUTCOME_HORIZONS_BARS_V2,
    HISTORICAL_THREE_FAMILY_OUTCOME_PROTOCOL_V2,
    HistoricalThreeFamilyOutcomeAuditV2,
    HistoricalThreeFamilyOutcomeV2,
    audit_historical_three_family_outcomes_v2,
)
from signalbot.r4b_v2.research.historical_three_family_topology import (
    HISTORICAL_THREE_FAMILY_TOPOLOGY_RULE_VERSION_V2,
    HistoricalThreeFamilyComparisonBucketV2,
    HistoricalThreeFamilyDisplayGradeV2,
    HistoricalThreeFamilyMajorityDirectionV2,
    HistoricalThreeFamilyTopologyClassV2,
)
from signalbot.r4b_v2.strategy.directional_evidence import DirectionalStateClassV2
from signalbot.r4b_v2.strategy.historical_three_family_consensus import (
    HISTORICAL_THREE_FAMILY_CONSENSUS_RULE_VERSION_V2,
    HistoricalExecutionContractV2,
    build_historical_execution_contract_v2,
)

HISTORICAL_THREE_FAMILY_FIXED_HORIZON_RUNNER_PROTOCOL_V2: Final = (
    "historical_three_family_fixed_horizon_outcomes_v2_2026-07-20"
)
HISTORICAL_THREE_FAMILY_FIXED_HORIZON_SCHEMA_VERSION_V2: Final = 1
HISTORICAL_THREE_FAMILY_PRIMARY_TOPOLOGY_V2: Final = (
    "PRIMARY_SUPPORTING_CLEAN_2_OF_3_OR_BROAD_3_OF_3_V1_FROZEN"
)
HISTORICAL_THREE_FAMILY_FUNDING_AUTHORITY_PROTOCOL_V2: Final = (
    "historical_three_family_funding_authority_v2_2026-07-20"
)
HISTORICAL_THREE_FAMILY_FUNDING_AUTHORITY_SCHEMA_VERSION_V2: Final = 1
HISTORICAL_THREE_FAMILY_COST_ATTRIBUTION_SUMMARY_VERSION_V2: Final = (
    "R4B_CAUSAL_V2_HISTORICAL_THREE_FAMILY_COST_ATTRIBUTION_SUMMARY_V1_FROZEN"
)
HISTORICAL_THREE_FAMILY_CENSUS_CODE_FREEZE_SHA256_V2: Final = (
    "b7868404318b3179274bde738e28c9574718e380a5f37c3a2b4b195ca5fafb60"
)

_SHA256_RE: Final = re.compile(r"^[0-9a-f]{64}$")
_SYMBOL_RE: Final = re.compile(r"^[A-Z0-9]{2,30}$")
_ASSET_RE: Final = re.compile(r"^[A-Z0-9]{2,20}$")
_MICROS: Final = Decimal(1_000_000)
_JCS_SAFE_INTEGER_MAX: Final = 2**53 - 1
_CONSENSUS_ROW_CAP: Final = 100_000
_OUTCOME_NAMES: Final = ("fixed_horizon_outcomes.csv", "results.json")
_PUBLISHED_NAMES: Final = frozenset((*_OUTCOME_NAMES, "manifest.json"))
_FUTURES_FILE_BY_ASSET: Final = {
    "BONK": "BONK__1000BONKUSDT__5m.csv.gz",
    "ENA": "ENA__ENAUSDT__5m.csv.gz",
    "WIF": "WIF__WIFUSDT__5m.csv.gz",
    "FLOKI": "FLOKI__1000FLOKIUSDT__5m.csv.gz",
    "ARB": "ARB__ARBUSDT__5m.csv.gz",
    "OP": "OP__OPUSDT__5m.csv.gz",
    "SEI": "SEI__SEIUSDT__5m.csv.gz",
}
_FUTURES_SYMBOL_BY_ASSET: Final = {
    "BONK": "1000BONKUSDT",
    "ENA": "ENAUSDT",
    "WIF": "WIFUSDT",
    "FLOKI": "1000FLOKIUSDT",
    "ARB": "ARBUSDT",
    "OP": "OPUSDT",
    "SEI": "SEIUSDT",
}
_SPLIT_RANGES_MS: Final = {
    "development": (1_719_792_000_000, 1_740_787_200_000),
    "validation": (1_740_787_200_000, 1_761_955_200_000),
    "retrospective_test": (1_761_955_200_000, 1_782_864_000_000),
}
_CLEAN_STATE_CLASSES: Final = frozenset(
    {
        DirectionalStateClassV2.BULLISH_STATE_TILT,
        DirectionalStateClassV2.BROAD_BULLISH_STATE,
        DirectionalStateClassV2.BEARISH_STATE_TILT,
        DirectionalStateClassV2.BROAD_BEARISH_STATE,
    }
)


class HistoricalThreeFamilyFixedHorizonErrorV2(ValueError):
    """Raised when an input or output violates the frozen outcome contract."""


class HistoricalFixedHorizonExclusionV2(StrEnum):
    SPLIT_BOUNDARY_ENTRY = "SPLIT_BOUNDARY_ENTRY"
    HORIZON_CROSSES_SPLIT = "HORIZON_CROSSES_SPLIT"
    MISSING_DECISION_BAR = "MISSING_DECISION_BAR"
    MISSING_NEXT_OPEN = "MISSING_NEXT_OPEN"
    DATA_GAP_IN_HORIZON = "DATA_GAP_IN_HORIZON"
    MISSING_HORIZON_CLOSE = "MISSING_HORIZON_CLOSE"
    FUNDING_DATASET_UNAVAILABLE = "FUNDING_DATASET_UNAVAILABLE"
    FUNDING_COVERAGE_UNAVAILABLE = "FUNDING_COVERAGE_UNAVAILABLE"


@dataclass(frozen=True, slots=True)
class HistoricalFundingFileBindingV2:
    """Hash-bound relative path for one recorded public funding dataset."""

    symbol: str
    relative_path: str
    sha256: str

    def __post_init__(self) -> None:
        _require_symbol(self.symbol, "funding symbol")
        _require_relative_posix_path(self.relative_path, "funding relative_path")
        _require_sha256(self.sha256, "funding sha256")
        _validate_funding_binding_panel(self)


@dataclass(frozen=True, slots=True)
class LoadedHistoricalFundingAuthorityV2:
    manifest_path: Path
    manifest_sha256: str
    bindings: tuple[HistoricalFundingFileBindingV2, ...]
    datasets: tuple[FundingDataset, ...]
    historical_only: Literal[True] = True

    def by_symbol(self) -> dict[str, FundingDataset]:
        return {
            binding.symbol: dataset
            for binding, dataset in zip(self.bindings, self.datasets, strict=True)
        }


@dataclass(frozen=True, slots=True)
class HistoricalConsensusOutcomeEventV2:
    """The only current-V1 consensus topology allowed into outcome matching."""

    split: str
    asset: str
    symbol: str
    event_id: str
    anchor_sha256: str
    primary_family: SignalFamily
    primary_direction: Direction
    decision_time_ms: int
    decision_price: Decimal
    invalidation: Decimal | None
    atr: Decimal
    state_class: DirectionalStateClassV2
    directional_agreement_micros: int
    execution_contract_sha256: str
    topology_version: str = HISTORICAL_THREE_FAMILY_PRIMARY_TOPOLOGY_V2

    def __post_init__(self) -> None:
        if self.split not in _SPLIT_RANGES_MS:
            raise HistoricalThreeFamilyFixedHorizonErrorV2("unsupported source split")
        _require_asset(self.asset)
        _require_symbol(self.symbol, "consensus symbol")
        if _FUTURES_SYMBOL_BY_ASSET.get(self.asset) != self.symbol:
            raise HistoricalThreeFamilyFixedHorizonErrorV2(
                "consensus asset/symbol pair is outside the frozen seven-asset panel"
            )
        _require_sha256(self.event_id, "event_id")
        _require_sha256(self.anchor_sha256, "anchor_sha256")
        _require_sha256(self.execution_contract_sha256, "execution contract")
        if self.execution_contract_sha256 != (
            build_historical_execution_contract_v2().execution_contract_sha256
        ):
            raise HistoricalThreeFamilyFixedHorizonErrorV2(
                "event does not bind the frozen historical execution contract"
            )
        if self.primary_direction not in (Direction.LONG, Direction.SHORT):
            raise HistoricalThreeFamilyFixedHorizonErrorV2(
                "outcome event direction must be long or short"
            )
        expected_family = (
            SignalFamily.PULLBACK_LONG
            if self.primary_direction is Direction.LONG
            else SignalFamily.PULLBACK_SHORT
        )
        if self.primary_family is not expected_family:
            raise HistoricalThreeFamilyFixedHorizonErrorV2(
                "outcome event must retain the source pullback family and side"
            )
        if self.state_class not in _CLEAN_STATE_CLASSES:
            raise HistoricalThreeFamilyFixedHorizonErrorV2(
                "only the frozen clean supporting topology is admissible"
            )
        bullish = self.state_class in {
            DirectionalStateClassV2.BULLISH_STATE_TILT,
            DirectionalStateClassV2.BROAD_BULLISH_STATE,
        }
        if bullish != (self.primary_direction is Direction.LONG):
            raise HistoricalThreeFamilyFixedHorizonErrorV2(
                "state side does not support the primary direction"
            )
        if type(self.decision_time_ms) is not int or not (
            0 <= self.decision_time_ms <= _JCS_SAFE_INTEGER_MAX
        ):
            raise HistoricalThreeFamilyFixedHorizonErrorV2(
                "decision_time_ms must be a nonnegative JCS-safe integer"
            )
        split_start, split_end = historical_three_family_split_bounds_v2(self.split)
        if not split_start <= self.decision_time_ms < split_end:
            raise HistoricalThreeFamilyFixedHorizonErrorV2(
                "decision_time_ms lies outside the claimed split"
            )
        if (
            type(self.decision_price) is not Decimal
            or not self.decision_price.is_finite()
            or self.decision_price <= 0
        ):
            raise HistoricalThreeFamilyFixedHorizonErrorV2(
                "decision_price must be a positive finite Decimal"
            )
        if self.invalidation is not None and (
            type(self.invalidation) is not Decimal
            or not self.invalidation.is_finite()
            or self.invalidation <= 0
        ):
            raise HistoricalThreeFamilyFixedHorizonErrorV2(
                "invalidation must be a positive finite Decimal when present"
            )
        if type(self.atr) is not Decimal or not self.atr.is_finite() or self.atr < 0:
            raise HistoricalThreeFamilyFixedHorizonErrorV2(
                "ATR must be a nonnegative finite Decimal"
            )
        if type(self.directional_agreement_micros) is not int or not (
            -1_000_000 <= self.directional_agreement_micros <= 1_000_000
        ):
            raise HistoricalThreeFamilyFixedHorizonErrorV2(
                "directional agreement must be an integer in [-1e6, 1e6]"
            )
        if bullish != (self.directional_agreement_micros > 0):
            raise HistoricalThreeFamilyFixedHorizonErrorV2(
                "directional agreement sign does not support the primary direction"
            )
        if self.topology_version != HISTORICAL_THREE_FAMILY_PRIMARY_TOPOLOGY_V2:
            raise HistoricalThreeFamilyFixedHorizonErrorV2(
                "outcome event topology is not the separately frozen current V1 topology"
            )

    @property
    def bucket(self) -> str:
        if self.state_class in {
            DirectionalStateClassV2.BROAD_BULLISH_STATE,
            DirectionalStateClassV2.BROAD_BEARISH_STATE,
        }:
            return "BROAD_3_OF_3"
        return "TILT_2_OF_3"


@dataclass(frozen=True, slots=True)
class LoadedHistoricalConsensusV2:
    consensus_path: Path
    census_manifest_path: Path
    consensus_sha256: str
    census_manifest_sha256: str
    experiment_contract_sha256: str
    topology_amendment_sha256: str
    execution_contract_sha256: str
    census_rows: int
    events: tuple[HistoricalConsensusOutcomeEventV2, ...]
    futures_data_sha256: tuple[tuple[str, str], ...]
    futures_manifest_sha256: tuple[tuple[str, str], ...]
    historical_only: Literal[True] = True
    probability: Literal[False] = False
    promoting: Literal[False] = False


@dataclass(frozen=True, slots=True)
class HistoricalOutcomeKlineAuthorityV2:
    asset: str
    symbol: str
    relative_data_path: str
    data_sha256: str
    manifest_sha256: str
    row_count: int


@dataclass(frozen=True, slots=True)
class LoadedHistoricalKlinePanelV2:
    datasets: tuple[tuple[str, KlineDataset], ...]
    authorities: tuple[HistoricalOutcomeKlineAuthorityV2, ...]
    historical_only: Literal[True] = True

    def by_asset(self) -> dict[str, KlineDataset]:
        return dict(self.datasets)


@dataclass(frozen=True, slots=True)
class HistoricalFixedHorizonOutcomeRowV2:
    topology_version: str
    split: str
    asset: str
    symbol: str
    event_id: str
    anchor_sha256: str
    rule_version: str
    outcome_protocol_version: str
    execution_contract_sha256: str
    state_class: str
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
    probability: Literal[False] = False
    probability_calibrated: Literal[False] = False
    promoting: Literal[False] = False
    order_placement: Literal[False] = False

    def __post_init__(self) -> None:
        if self.topology_version != HISTORICAL_THREE_FAMILY_PRIMARY_TOPOLOGY_V2:
            raise HistoricalThreeFamilyFixedHorizonErrorV2("unsupported outcome topology")
        if self.split not in _SPLIT_RANGES_MS:
            raise HistoricalThreeFamilyFixedHorizonErrorV2("unsupported outcome split")
        _require_asset(self.asset)
        _require_symbol(self.symbol, "outcome symbol")
        if _FUTURES_SYMBOL_BY_ASSET.get(self.asset) != self.symbol:
            raise HistoricalThreeFamilyFixedHorizonErrorV2(
                "outcome asset/symbol pair is outside the frozen panel"
            )
        _require_sha256(self.event_id, "outcome event_id")
        _require_sha256(self.anchor_sha256, "outcome anchor_sha256")
        _require_sha256(
            self.execution_contract_sha256,
            "outcome execution_contract_sha256",
        )
        if self.execution_contract_sha256 != (
            build_historical_execution_contract_v2().execution_contract_sha256
        ):
            raise HistoricalThreeFamilyFixedHorizonErrorV2(
                "outcome execution contract differs from the frozen contract"
            )
        if self.rule_version != HISTORICAL_THREE_FAMILY_CONSENSUS_RULE_VERSION_V2:
            raise HistoricalThreeFamilyFixedHorizonErrorV2("wrong consensus rule version")
        if self.outcome_protocol_version != HISTORICAL_THREE_FAMILY_OUTCOME_PROTOCOL_V2:
            raise HistoricalThreeFamilyFixedHorizonErrorV2("wrong outcome protocol version")
        if self.horizon_bars not in HISTORICAL_THREE_FAMILY_OUTCOME_HORIZONS_BARS_V2:
            raise HistoricalThreeFamilyFixedHorizonErrorV2("unsupported outcome horizon")
        if self.horizon_minutes != self.horizon_bars * 5:
            raise HistoricalThreeFamilyFixedHorizonErrorV2("horizon minutes are inconsistent")
        if type(self.decision_time_ms) is not int or self.decision_time_ms < 0:
            raise HistoricalThreeFamilyFixedHorizonErrorV2(
                "outcome decision time must be nonnegative integer milliseconds"
            )
        split_start, split_end = historical_three_family_split_bounds_v2(self.split)
        if not split_start <= self.decision_time_ms < split_end:
            raise HistoricalThreeFamilyFixedHorizonErrorV2(
                "outcome decision time lies outside its source split"
            )
        if self.expected_entry_time_ms != self.decision_time_ms + 1:
            raise HistoricalThreeFamilyFixedHorizonErrorV2(
                "outcome entry time is not the next contiguous bar open"
            )
        if self.expected_exit_close_time_ms != (
            self.decision_time_ms + self.horizon_bars * FIVE_MINUTE_MS_V2
        ):
            raise HistoricalThreeFamilyFixedHorizonErrorV2(
                "outcome exit time differs from the frozen closed-bar horizon"
            )
        try:
            direction = Direction(self.primary_direction)
            state = DirectionalStateClassV2(self.state_class)
        except ValueError as exc:
            raise HistoricalThreeFamilyFixedHorizonErrorV2(
                "outcome side or state is unsupported"
            ) from exc
        bullish = state in {
            DirectionalStateClassV2.BULLISH_STATE_TILT,
            DirectionalStateClassV2.BROAD_BULLISH_STATE,
        }
        if type(self.directional_agreement_micros) is not int or not (
            -1_000_000 <= self.directional_agreement_micros <= 1_000_000
        ):
            raise HistoricalThreeFamilyFixedHorizonErrorV2(
                "outcome agreement must be an integer in [-1e6, 1e6]"
            )
        expected_bucket = (
            "BROAD_3_OF_3"
            if state
            in {
                DirectionalStateClassV2.BROAD_BULLISH_STATE,
                DirectionalStateClassV2.BROAD_BEARISH_STATE,
            }
            else "TILT_2_OF_3"
        )
        if (
            state not in _CLEAN_STATE_CLASSES
            or direction not in (Direction.LONG, Direction.SHORT)
            or bullish != (direction is Direction.LONG)
            or self.agreement_bucket != expected_bucket
            or bullish != (self.directional_agreement_micros > 0)
        ):
            raise HistoricalThreeFamilyFixedHorizonErrorV2(
                "outcome state, side, bucket, or agreement is inconsistent"
            )
        economic = (
            self.gross_directional_return_micros,
            self.slippage_return_micros,
            self.fee_return_micros,
            self.funding_return_micros,
            self.rounding_residual_micros,
            self.total_cost_micros,
            self.funding_event_count,
            self.net_return_micros,
        )
        if self.evaluable:
            if self.exclusion_reason or any(value is None for value in economic):
                raise HistoricalThreeFamilyFixedHorizonErrorV2(
                    "evaluable row requires complete economics and no exclusion"
                )
            if self.entry_price is None or self.exit_price is None:
                raise HistoricalThreeFamilyFixedHorizonErrorV2(
                    "evaluable row requires entry and exit prices"
                )
            if any(type(value) is not int for value in economic):
                raise HistoricalThreeFamilyFixedHorizonErrorV2(
                    "evaluable economic components must be exact integers"
                )
            gross = cast(int, self.gross_directional_return_micros)
            slippage = cast(int, self.slippage_return_micros)
            fee = cast(int, self.fee_return_micros)
            funding = cast(int, self.funding_return_micros)
            residual = cast(int, self.rounding_residual_micros)
            total_cost = cast(int, self.total_cost_micros)
            net = cast(int, self.net_return_micros)
            if total_cost != slippage + fee - residual:
                raise HistoricalThreeFamilyFixedHorizonErrorV2(
                    "total cost must reconcile fee, slippage, and rounding residual"
                )
            if gross - total_cost + funding != net:
                raise HistoricalThreeFamilyFixedHorizonErrorV2(
                    "gross, total cost, funding, and net must reconcile exactly"
                )
            if slippage < 0 or fee < 0 or total_cost < 0 or cast(int, self.funding_event_count) < 0:
                raise HistoricalThreeFamilyFixedHorizonErrorV2(
                    "costs and funding event count must be nonnegative"
                )
        else:
            valid_exclusions = {value.value for value in HistoricalFixedHorizonExclusionV2}
            if self.exclusion_reason not in valid_exclusions or any(
                value is not None for value in economic
            ):
                raise HistoricalThreeFamilyFixedHorizonErrorV2(
                    "excluded row requires a reason and no economic values"
                )
        for value, label in (
            (self.entry_price, "entry_price"),
            (self.exit_price, "exit_price"),
        ):
            if value is not None and (
                type(value) is not Decimal or not value.is_finite() or value <= 0
            ):
                raise HistoricalThreeFamilyFixedHorizonErrorV2(
                    f"{label} must be a positive finite Decimal when present"
                )

    def to_audit_outcome(self) -> HistoricalThreeFamilyOutcomeV2:
        return HistoricalThreeFamilyOutcomeV2(
            event_id=self.event_id,
            outcome_protocol_version=self.outcome_protocol_version,
            rule_version=self.rule_version,
            execution_contract_sha256=self.execution_contract_sha256,
            venue=VenueV2.USDM_FUTURES,
            symbol=self.symbol,
            decision_time_ms=self.decision_time_ms,
            state_class=DirectionalStateClassV2(self.state_class),
            directional_agreement_micros=self.directional_agreement_micros,
            horizon_bars=self.horizon_bars,
            evaluable=self.evaluable,
            exclusion_reason=self.exclusion_reason,
            net_return_micros=self.net_return_micros,
        )


@dataclass(frozen=True, slots=True)
class HistoricalFixedHorizonArtifactsV2:
    output_dir: Path
    outcomes_sha256: str
    results_sha256: str
    manifest_sha256: str
    admitted_events: int
    outcome_rows: int


@dataclass(frozen=True, slots=True)
class LoadedHistoricalFixedHorizonArtifactsV2:
    """Hash-authenticated fixed-horizon artifacts for downstream analysis."""

    artifact_dir: Path
    manifest_sha256: str
    outcomes_sha256: str
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
    rows: tuple[HistoricalFixedHorizonOutcomeRowV2, ...]
    results: Mapping[str, object]
    historical_only: Literal[True] = True
    probability: Literal[False] = False
    promoting: Literal[False] = False


@dataclass(frozen=True, slots=True)
class HistoricalFixedHorizonCostSummaryV2:
    horizon_bars: int
    horizon_minutes: int
    side: str
    agreement_bucket: str
    events: int
    evaluable: int
    coverage_micros: int
    gross_directional_strict_hits: int
    gross_directional_strict_hit_rate_micros: int | None
    net_strict_hits: int
    net_strict_hit_rate_micros: int | None
    gross_to_net_hit_loss_count: int
    gross_to_net_hit_loss_rate_micros: int | None
    net_positive_without_gross_positive_count: int
    mean_gross_directional_return_micros: int | None
    mean_slippage_return_micros: int | None
    mean_fee_return_micros: int | None
    mean_funding_return_micros: int | None
    mean_total_cost_micros: int | None
    mean_net_return_micros: int | None
    gross_to_net_mean_change_micros: int | None
    historical_only: Literal[True] = True
    probability: Literal[False] = False
    promoting: Literal[False] = False


def historical_three_family_split_bounds_v2(split: str) -> tuple[int, int]:
    """Return the immutable half-open millisecond bounds for one source split."""

    try:
        return _SPLIT_RANGES_MS[split]
    except (KeyError, TypeError) as exc:
        raise HistoricalThreeFamilyFixedHorizonErrorV2(
            "unsupported historical three-family split"
        ) from exc


def summarize_historical_fixed_horizon_cost_components_v2(
    rows: Sequence[HistoricalFixedHorizonOutcomeRowV2],
) -> tuple[HistoricalFixedHorizonCostSummaryV2, ...]:
    """Summarize gross direction and each cost component without fitting."""

    snapshot = tuple(rows)
    if any(type(row) is not HistoricalFixedHorizonOutcomeRowV2 for row in snapshot):
        raise HistoricalThreeFamilyFixedHorizonErrorV2(
            "cost attribution summary accepts exact fixed-horizon rows only"
        )
    grouped: dict[tuple[int, str, str], list[HistoricalFixedHorizonOutcomeRowV2]] = {}
    for row in snapshot:
        side = "BULLISH" if row.primary_direction == Direction.LONG.value else "BEARISH"
        grouped.setdefault((row.horizon_bars, side, row.agreement_bucket), []).append(row)
    documents: list[HistoricalFixedHorizonCostSummaryV2] = []
    for horizon in HISTORICAL_THREE_FAMILY_OUTCOME_HORIZONS_BARS_V2:
        for side in ("BULLISH", "BEARISH"):
            for bucket in ("TILT_2_OF_3", "BROAD_3_OF_3"):
                cell = grouped.get((horizon, side, bucket), [])
                evaluable = [row for row in cell if row.evaluable]
                gross = [cast(int, row.gross_directional_return_micros) for row in evaluable]
                slippage = [cast(int, row.slippage_return_micros) for row in evaluable]
                fees = [cast(int, row.fee_return_micros) for row in evaluable]
                funding = [cast(int, row.funding_return_micros) for row in evaluable]
                total_cost = [cast(int, row.total_cost_micros) for row in evaluable]
                net = [cast(int, row.net_return_micros) for row in evaluable]
                gross_hits = sum(value > 0 for value in gross)
                net_hits = sum(value > 0 for value in net)
                hit_loss = sum(
                    gross_value > 0 and net_value <= 0
                    for gross_value, net_value in zip(gross, net, strict=True)
                )
                net_only = sum(
                    net_value > 0 and gross_value <= 0
                    for gross_value, net_value in zip(gross, net, strict=True)
                )
                mean_gross = _mean_integer(gross)
                mean_net = _mean_integer(net)
                denominator = len(evaluable)
                documents.append(
                    HistoricalFixedHorizonCostSummaryV2(
                        horizon_bars=horizon,
                        horizon_minutes=horizon * 5,
                        side=side,
                        agreement_bucket=bucket,
                        events=len(cell),
                        evaluable=denominator,
                        coverage_micros=(
                            _round_ratio(denominator * 1_000_000, len(cell)) if cell else 0
                        ),
                        gross_directional_strict_hits=gross_hits,
                        gross_directional_strict_hit_rate_micros=(
                            _round_ratio(gross_hits * 1_000_000, denominator)
                            if denominator
                            else None
                        ),
                        net_strict_hits=net_hits,
                        net_strict_hit_rate_micros=(
                            _round_ratio(net_hits * 1_000_000, denominator) if denominator else None
                        ),
                        gross_to_net_hit_loss_count=hit_loss,
                        gross_to_net_hit_loss_rate_micros=(
                            _round_ratio(hit_loss * 1_000_000, denominator) if denominator else None
                        ),
                        net_positive_without_gross_positive_count=net_only,
                        mean_gross_directional_return_micros=mean_gross,
                        mean_slippage_return_micros=_mean_integer(slippage),
                        mean_fee_return_micros=_mean_integer(fees),
                        mean_funding_return_micros=_mean_integer(funding),
                        mean_total_cost_micros=_mean_integer(total_cost),
                        mean_net_return_micros=mean_net,
                        gross_to_net_mean_change_micros=(
                            None
                            if mean_gross is None or mean_net is None
                            else mean_net - mean_gross
                        ),
                    )
                )
    return tuple(documents)


def canonical_historical_funding_authority_manifest_v2(
    bindings: Sequence[HistoricalFundingFileBindingV2],
) -> bytes:
    """Serialize funding bindings; the returned hash must be frozen externally."""

    snapshot = tuple(bindings)
    if any(type(item) is not HistoricalFundingFileBindingV2 for item in snapshot):
        raise HistoricalThreeFamilyFixedHorizonErrorV2(
            "funding authority accepts exact binding values only"
        )
    ordered = tuple(sorted(snapshot, key=lambda item: (item.symbol, item.relative_path)))
    if len({item.symbol for item in ordered}) != len(ordered):
        raise HistoricalThreeFamilyFixedHorizonErrorV2(
            "funding authority contains duplicate symbols"
        )
    if len({item.relative_path for item in ordered}) != len(ordered):
        raise HistoricalThreeFamilyFixedHorizonErrorV2("funding authority contains duplicate paths")
    return canonical_json_line(
        {
            "files": [asdict(item) for item in ordered],
            "historical_only": True,
            "protocol": HISTORICAL_THREE_FAMILY_FUNDING_AUTHORITY_PROTOCOL_V2,
            "schema_version": HISTORICAL_THREE_FAMILY_FUNDING_AUTHORITY_SCHEMA_VERSION_V2,
        }
    )


def load_authenticated_historical_funding_authority_v2(
    manifest_path: str | Path,
    *,
    expected_manifest_sha256: str,
    data_root: str | Path,
) -> LoadedHistoricalFundingAuthorityV2:
    """Load only funding bytes covered by an externally frozen manifest hash."""

    _require_sha256(expected_manifest_sha256, "expected funding manifest SHA-256")
    path = Path(manifest_path).resolve()
    raw = _read_required_bytes(path, "funding authority manifest")
    if _sha256_bytes(raw) != expected_manifest_sha256:
        raise HistoricalThreeFamilyFixedHorizonErrorV2(
            "funding authority manifest SHA-256 differs from the frozen value"
        )
    document = _decode_canonical_json_object(raw, "funding authority manifest")
    if (
        document.get("protocol") != HISTORICAL_THREE_FAMILY_FUNDING_AUTHORITY_PROTOCOL_V2
        or document.get("schema_version")
        != HISTORICAL_THREE_FAMILY_FUNDING_AUTHORITY_SCHEMA_VERSION_V2
        or document.get("historical_only") is not True
    ):
        raise HistoricalThreeFamilyFixedHorizonErrorV2(
            "funding authority protocol/schema/role is unsupported"
        )
    raw_files = document.get("files")
    if not isinstance(raw_files, list):
        raise HistoricalThreeFamilyFixedHorizonErrorV2("funding authority files must be a list")
    bindings: list[HistoricalFundingFileBindingV2] = []
    for item in raw_files:
        if not isinstance(item, dict) or set(item) != {"relative_path", "sha256", "symbol"}:
            raise HistoricalThreeFamilyFixedHorizonErrorV2(
                "funding authority file row has an unsupported schema"
            )
        try:
            bindings.append(
                HistoricalFundingFileBindingV2(
                    symbol=cast(str, item["symbol"]),
                    relative_path=cast(str, item["relative_path"]),
                    sha256=cast(str, item["sha256"]),
                )
            )
        except (KeyError, TypeError) as exc:
            raise HistoricalThreeFamilyFixedHorizonErrorV2(
                "funding authority file row is invalid"
            ) from exc
    expected_raw = canonical_historical_funding_authority_manifest_v2(bindings)
    if raw != expected_raw:
        raise HistoricalThreeFamilyFixedHorizonErrorV2(
            "funding authority manifest is not canonical or is contradictory"
        )

    root = Path(data_root).resolve()
    datasets: list[FundingDataset] = []
    for binding in sorted(bindings, key=lambda item: (item.symbol, item.relative_path)):
        source = _resolve_relative_input(root, binding.relative_path)
        try:
            actual_funding_sha256 = funding_sha256(source)
        except OSError as exc:
            raise HistoricalThreeFamilyFixedHorizonErrorV2(
                f"cannot read funding dataset for {binding.symbol}"
            ) from exc
        if actual_funding_sha256 != binding.sha256:
            raise HistoricalThreeFamilyFixedHorizonErrorV2(
                f"funding dataset SHA-256 mismatch for {binding.symbol}"
            )
        try:
            dataset = read_funding_csv(source)
        except FundingValidationError as exc:
            raise HistoricalThreeFamilyFixedHorizonErrorV2(
                f"invalid funding dataset for {binding.symbol}"
            ) from exc
        if dataset.symbol != binding.symbol:
            raise HistoricalThreeFamilyFixedHorizonErrorV2(
                "funding dataset symbol differs from its authority binding"
            )
        datasets.append(dataset)
    ordered_bindings = tuple(sorted(bindings, key=lambda item: (item.symbol, item.relative_path)))
    return LoadedHistoricalFundingAuthorityV2(
        manifest_path=path,
        manifest_sha256=expected_manifest_sha256,
        bindings=ordered_bindings,
        datasets=tuple(datasets),
    )


def load_authenticated_historical_consensus_v2(
    consensus_path: str | Path,
    census_manifest_path: str | Path,
    *,
    expected_census_manifest_sha256: str,
    expected_experiment_contract_sha256: str,
    expected_topology_amendment_sha256: str,
) -> LoadedHistoricalConsensusV2:
    """Authenticate a complete outcome-blind census before reading outcomes."""

    _require_sha256(expected_census_manifest_sha256, "expected census manifest SHA-256")
    _require_sha256(expected_experiment_contract_sha256, "experiment contract SHA-256")
    _require_sha256(
        expected_topology_amendment_sha256,
        "topology amendment SHA-256",
    )
    manifest_path = Path(census_manifest_path).resolve()
    manifest_raw = _read_required_bytes(manifest_path, "census manifest")
    if _sha256_bytes(manifest_raw) != expected_census_manifest_sha256:
        raise HistoricalThreeFamilyFixedHorizonErrorV2(
            "census manifest SHA-256 differs from the externally frozen value"
        )
    manifest = _decode_canonical_json_object(manifest_raw, "census manifest")
    _validate_census_manifest(
        manifest,
        expected_experiment_contract_sha256,
        expected_topology_amendment_sha256,
    )

    source = Path(consensus_path).resolve()
    raw = _read_required_bytes(source, "consensus.csv")
    outputs = _require_dict(manifest.get("outputs"), "census outputs")
    expected_consensus_sha = _require_sha256(
        outputs.get("consensus.csv"), "census consensus.csv output hash"
    )
    actual_consensus_sha = _sha256_bytes(raw)
    if actual_consensus_sha != expected_consensus_sha:
        raise HistoricalThreeFamilyFixedHorizonErrorV2(
            "consensus.csv SHA-256 differs from the authenticated census manifest"
        )
    rows = _parse_consensus_csv(raw)
    execution_contract_sha = cast(str, manifest["execution_contract_sha256"])
    events = _select_primary_supporting_events(
        rows,
        execution_contract_sha,
        expected_topology_amendment_sha256,
    )
    inputs = _require_dict(manifest.get("inputs"), "census inputs")
    futures_data = _string_hash_map(inputs.get("futures_data_sha256"), "census futures data")
    futures_manifests = _string_hash_map(
        inputs.get("futures_manifest_sha256"), "census futures manifests"
    )
    _validate_futures_authority_keys(futures_data, futures_manifests)
    return LoadedHistoricalConsensusV2(
        consensus_path=source,
        census_manifest_path=manifest_path,
        consensus_sha256=actual_consensus_sha,
        census_manifest_sha256=expected_census_manifest_sha256,
        experiment_contract_sha256=expected_experiment_contract_sha256,
        topology_amendment_sha256=expected_topology_amendment_sha256,
        execution_contract_sha256=execution_contract_sha,
        census_rows=len(rows),
        events=events,
        futures_data_sha256=tuple(sorted(futures_data.items())),
        futures_manifest_sha256=tuple(sorted(futures_manifests.items())),
    )


def evaluate_historical_fixed_horizons_v2(
    event: HistoricalConsensusOutcomeEventV2,
    dataset: KlineDataset,
    funding: FundingDataset | None,
    *,
    execution_contract: HistoricalExecutionContractV2 | None = None,
) -> tuple[HistoricalFixedHorizonOutcomeRowV2, ...]:
    """Evaluate all five horizons from one next-contiguous-open entry."""

    if type(event) is not HistoricalConsensusOutcomeEventV2:
        raise HistoricalThreeFamilyFixedHorizonErrorV2(
            "event must be an exact HistoricalConsensusOutcomeEventV2"
        )
    if type(dataset) is not KlineDataset:
        raise HistoricalThreeFamilyFixedHorizonErrorV2("dataset must be a KlineDataset")
    contract = execution_contract or build_historical_execution_contract_v2()
    if type(contract) is not HistoricalExecutionContractV2:
        raise HistoricalThreeFamilyFixedHorizonErrorV2(
            "execution contract must be the exact frozen contract type"
        )
    if contract.execution_contract_sha256 != event.execution_contract_sha256:
        raise HistoricalThreeFamilyFixedHorizonErrorV2(
            "event and evaluator execution contracts differ"
        )
    request = dataset.request
    if (
        request.market is not Market.FUTURES
        or request.symbol != event.symbol
        or request.dataset_alias != event.asset
        or request.interval != "5m"
    ):
        raise HistoricalThreeFamilyFixedHorizonErrorV2(
            "outcome dataset identity differs from the consensus event"
        )
    if funding is not None and funding.symbol != event.symbol:
        raise HistoricalThreeFamilyFixedHorizonErrorV2(
            "funding dataset symbol differs from the consensus event"
        )

    by_open = {candle.open_time_ms: candle for candle in dataset.candles}
    if len(by_open) != len(dataset.candles):
        raise HistoricalThreeFamilyFixedHorizonErrorV2(
            "outcome dataset contains duplicate open times"
        )
    decision_open_ms = event.decision_time_ms - FIVE_MINUTE_MS_V2 + 1
    decision_candle = by_open.get(decision_open_ms)
    if decision_candle is not None and decision_candle.close != event.decision_price:
        raise HistoricalThreeFamilyFixedHorizonErrorV2(
            "authenticated consensus decision price differs from the bound kline"
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
        )
        for horizon in HISTORICAL_THREE_FAMILY_OUTCOME_HORIZONS_BARS_V2
    )


def run_historical_three_family_fixed_horizons_v2(
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
) -> HistoricalFixedHorizonArtifactsV2:
    """Run and publish deterministic fixed-horizon outcomes from frozen inputs."""

    _validate_optional_funding_authority_pair(
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
    kline_panel = load_authenticated_historical_kline_panel_v2(loaded, data_root)
    datasets = kline_panel.by_asset()
    funding_authority = load_authenticated_historical_funding_authority_v2(
        funding_authority_manifest_path,
        expected_manifest_sha256=expected_funding_authority_manifest_sha256,
        data_root=data_root,
    )
    funding_by_symbol = funding_authority.by_symbol()
    execution_contract = build_historical_execution_contract_v2()
    outcomes = tuple(
        outcome
        for event in loaded.events
        for outcome in evaluate_historical_fixed_horizons_v2(
            event,
            datasets[event.asset],
            funding_by_symbol.get(event.symbol),
            execution_contract=execution_contract,
        )
    )
    _validate_complete_outcome_census(loaded.events, outcomes)
    outcomes_raw = _fixed_horizon_csv_bytes(outcomes)
    audit = (
        audit_historical_three_family_outcomes_v2(
            tuple(item.to_audit_outcome() for item in outcomes)
        )
        if outcomes
        else None
    )
    results_document = _results_document(
        loaded=loaded,
        outcomes=outcomes,
        audit=audit,
        funding_authority=funding_authority,
        downstream_code_freeze_manifest_sha256=downstream_freeze.manifest_sha256,
        outcomes_sha256=_sha256_bytes(outcomes_raw),
    )
    results_raw = canonical_json_line(results_document)
    manifest_document = _outcome_manifest_document(
        loaded=loaded,
        kline_authority=kline_panel.authorities,
        funding_authority=funding_authority,
        downstream_code_freeze_manifest_sha256=downstream_freeze.manifest_sha256,
        output_hashes={
            "fixed_horizon_outcomes.csv": _sha256_bytes(outcomes_raw),
            "results.json": _sha256_bytes(results_raw),
        },
    )
    manifest_raw = canonical_json_line(manifest_document)
    target = Path(output_dir).resolve()
    _publish_artifacts(
        target,
        {
            "fixed_horizon_outcomes.csv": outcomes_raw,
            "results.json": results_raw,
            "manifest.json": manifest_raw,
        },
    )
    return HistoricalFixedHorizonArtifactsV2(
        output_dir=target,
        outcomes_sha256=_sha256_bytes(outcomes_raw),
        results_sha256=_sha256_bytes(results_raw),
        manifest_sha256=_sha256_bytes(manifest_raw),
        admitted_events=len(loaded.events),
        outcome_rows=len(outcomes),
    )


def load_authenticated_historical_fixed_horizon_artifacts_v2(
    artifact_dir: str | Path,
    *,
    expected_manifest_sha256: str,
    expected_census_manifest_sha256: str,
    expected_experiment_contract_sha256: str,
    expected_topology_amendment_sha256: str,
    expected_funding_authority_manifest_sha256: str,
    expected_downstream_code_freeze_manifest_sha256: str,
) -> LoadedHistoricalFixedHorizonArtifactsV2:
    """Load exactly one published fixed-horizon artifact set and fail closed."""

    for value, label in (
        (expected_manifest_sha256, "expected fixed-horizon manifest SHA-256"),
        (expected_census_manifest_sha256, "expected census manifest SHA-256"),
        (expected_experiment_contract_sha256, "expected experiment contract SHA-256"),
        (expected_topology_amendment_sha256, "expected topology amendment SHA-256"),
        (
            expected_downstream_code_freeze_manifest_sha256,
            "expected downstream code-freeze manifest SHA-256",
        ),
    ):
        _require_sha256(value, label)
    _require_sha256(
        expected_funding_authority_manifest_sha256,
        "expected funding authority SHA-256",
    )
    root = Path(artifact_dir).resolve()
    _require_exact_artifact_files_v2(root, _PUBLISHED_NAMES, "fixed-horizon")
    manifest_raw = _read_required_bytes(root / "manifest.json", "fixed-horizon manifest")
    if _sha256_bytes(manifest_raw) != expected_manifest_sha256:
        raise HistoricalThreeFamilyFixedHorizonErrorV2(
            "fixed-horizon manifest differs from the externally frozen SHA-256"
        )
    manifest = _decode_canonical_json_object(manifest_raw, "fixed-horizon manifest")
    _validate_loaded_fixed_horizon_manifest_v2(
        manifest,
        expected_census_manifest_sha256=expected_census_manifest_sha256,
        expected_experiment_contract_sha256=expected_experiment_contract_sha256,
        expected_topology_amendment_sha256=expected_topology_amendment_sha256,
        expected_funding_authority_manifest_sha256=(expected_funding_authority_manifest_sha256),
        expected_downstream_code_freeze_manifest_sha256=(
            expected_downstream_code_freeze_manifest_sha256
        ),
    )
    outputs = _require_dict(manifest.get("outputs"), "fixed-horizon outputs")
    outcomes_raw = _read_required_bytes(
        root / "fixed_horizon_outcomes.csv",
        "fixed-horizon outcomes",
    )
    results_raw = _read_required_bytes(root / "results.json", "fixed-horizon results")
    outcomes_sha256 = _sha256_bytes(outcomes_raw)
    results_sha256 = _sha256_bytes(results_raw)
    if outputs != {
        "fixed_horizon_outcomes.csv": outcomes_sha256,
        "results.json": results_sha256,
    }:
        raise HistoricalThreeFamilyFixedHorizonErrorV2(
            "fixed-horizon payload hashes differ from the authenticated manifest"
        )
    rows = _parse_fixed_horizon_csv_v2(outcomes_raw)
    results = _decode_canonical_json_object(results_raw, "fixed-horizon results")
    _validate_loaded_fixed_horizon_results_v2(
        results,
        manifest=manifest,
        rows=rows,
        outcomes_sha256=outcomes_sha256,
        expected_funding_authority_manifest_sha256=(expected_funding_authority_manifest_sha256),
        expected_downstream_code_freeze_manifest_sha256=(
            expected_downstream_code_freeze_manifest_sha256
        ),
    )
    return LoadedHistoricalFixedHorizonArtifactsV2(
        artifact_dir=root,
        manifest_sha256=expected_manifest_sha256,
        outcomes_sha256=outcomes_sha256,
        results_sha256=results_sha256,
        census_manifest_sha256=expected_census_manifest_sha256,
        consensus_sha256=_require_sha256(
            manifest.get("consensus_sha256"), "fixed-horizon consensus SHA-256"
        ),
        experiment_contract_sha256=expected_experiment_contract_sha256,
        topology_amendment_sha256=expected_topology_amendment_sha256,
        execution_contract_sha256=_require_sha256(
            manifest.get("execution_contract_sha256"),
            "fixed-horizon execution contract SHA-256",
        ),
        funding_authority_manifest_sha256=expected_funding_authority_manifest_sha256,
        downstream_code_freeze_manifest_sha256=(expected_downstream_code_freeze_manifest_sha256),
        census_rows=_require_nonnegative_int_v2(results.get("census_rows"), "census_rows"),
        admitted_events=_require_nonnegative_int_v2(
            results.get("admitted_events"), "admitted_events"
        ),
        rows=rows,
        results=results,
    )


def _evaluate_one_horizon(
    *,
    event: HistoricalConsensusOutcomeEventV2,
    dataset: KlineDataset,
    by_open: Mapping[int, Candle],
    decision_candle_present: bool,
    funding: FundingDataset | None,
    execution_contract: HistoricalExecutionContractV2,
    horizon_bars: int,
) -> HistoricalFixedHorizonOutcomeRowV2:
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
            last_open = dataset.candles[-1].open_time_ms
            exclusion = (
                HistoricalFixedHorizonExclusionV2.MISSING_HORIZON_CLOSE
                if expected_exit_open_ms > last_open
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
        "split": event.split,
        "asset": event.asset,
        "symbol": event.symbol,
        "event_id": event.event_id,
        "anchor_sha256": event.anchor_sha256,
        "rule_version": HISTORICAL_THREE_FAMILY_CONSENSUS_RULE_VERSION_V2,
        "outcome_protocol_version": HISTORICAL_THREE_FAMILY_OUTCOME_PROTOCOL_V2,
        "execution_contract_sha256": execution_contract.execution_contract_sha256,
        "state_class": event.state_class.value,
        "agreement_bucket": event.bucket,
        "primary_direction": event.primary_direction.value,
        "directional_agreement_micros": event.directional_agreement_micros,
        "decision_time_ms": event.decision_time_ms,
        "horizon_bars": horizon_bars,
        "horizon_minutes": horizon_bars * 5,
        "expected_entry_time_ms": expected_entry_ms,
        "expected_exit_close_time_ms": expected_exit_close_ms,
    }
    if exclusion is not None:
        return HistoricalFixedHorizonOutcomeRowV2(
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
        raise HistoricalThreeFamilyFixedHorizonErrorV2(
            "internal outcome eligibility state is inconsistent"
        )
    execution = calculate_execution_returns(
        event.primary_direction,
        float(entry.open),
        float(exit_candle.close),
        float(execution_contract.fee_bps_per_side),
        float(execution_contract.slippage_bps_per_side),
    )
    rates = list(funding.rates)
    funding_return = calculate_funding_return(
        event.primary_direction,
        expected_entry_ms,
        expected_exit_close_ms,
        float(entry.open),
        rates,
    )
    gross_micros = historical_return_to_micros_v2(execution.gross_return)
    slippage_micros = historical_return_to_micros_v2(execution.slippage_return)
    fee_micros = historical_return_to_micros_v2(execution.fee_return)
    funding_micros = historical_return_to_micros_v2(funding_return)
    net_micros = historical_return_to_micros_v2(execution.net_before_funding + funding_return)
    component_net = gross_micros - slippage_micros - fee_micros + funding_micros
    rounding_residual_micros = net_micros - component_net
    total_cost_micros = slippage_micros + fee_micros - rounding_residual_micros
    event_count = sum(
        expected_entry_ms < item.funding_time_ms < expected_exit_close_ms for item in rates
    )
    return HistoricalFixedHorizonOutcomeRowV2(
        **common,
        entry_price=entry.open,
        exit_price=exit_candle.close,
        gross_directional_return_micros=gross_micros,
        slippage_return_micros=slippage_micros,
        fee_return_micros=fee_micros,
        funding_return_micros=funding_micros,
        rounding_residual_micros=rounding_residual_micros,
        total_cost_micros=total_cost_micros,
        funding_event_count=event_count,
        evaluable=True,
        exclusion_reason="",
        net_return_micros=net_micros,
    )


def _validate_census_manifest(
    document: dict[str, object],
    expected_experiment: str,
    expected_topology_amendment: str,
) -> None:
    expected_contract = build_historical_execution_contract_v2().execution_contract_sha256
    required = {
        "census_complete": True,
        "conflicted_comparator_outcome_authorized": False,
        "consensus_rule_version": HISTORICAL_THREE_FAMILY_CONSENSUS_RULE_VERSION_V2,
        "diagnostic_mode": False,
        "execution_contract_sha256": expected_contract,
        "experiment_contract_sha256": expected_experiment,
        "historical_only": True,
        "maximum_anchors": None,
        "outcome_data_read": False,
        "probability": False,
        "promoting": False,
        "protocol": HISTORICAL_THREE_FAMILY_CENSUS_PROTOCOL_V2,
        "schema_version": HISTORICAL_THREE_FAMILY_CENSUS_SCHEMA_VERSION_V2,
        "topology_rule_version": HISTORICAL_THREE_FAMILY_TOPOLOGY_RULE_VERSION_V2,
        "topology_contract_sha256": expected_topology_amendment,
        "v1a_fitted_selection_used": False,
    }
    for key, expected in required.items():
        if document.get(key) != expected:
            raise HistoricalThreeFamilyFixedHorizonErrorV2(
                f"census manifest field {key} is not the frozen complete-census value"
            )
    _require_dict(document.get("inputs"), "census inputs")
    outputs = _require_dict(document.get("outputs"), "census outputs")
    if set(outputs) != {"consensus.csv", "results.json"}:
        raise HistoricalThreeFamilyFixedHorizonErrorV2("census output hash set is unsupported")


def _parse_consensus_csv(raw: bytes) -> tuple[HistoricalConsensusCensusRowV2, ...]:
    if not raw or b"\r" in raw or not raw.endswith(b"\n"):
        raise HistoricalThreeFamilyFixedHorizonErrorV2(
            "consensus.csv must be non-empty canonical LF-only bytes"
        )
    try:
        text = raw.decode("utf-8")
        reader = csv.DictReader(io.StringIO(text, newline=""), strict=True)
        expected_columns = tuple(field.name for field in fields(HistoricalConsensusCensusRowV2))
        if tuple(reader.fieldnames or ()) != expected_columns:
            raise HistoricalThreeFamilyFixedHorizonErrorV2(
                "consensus.csv header differs from the frozen census schema"
            )
        raw_rows: list[dict[str, str]] = []
        for row in reader:
            if len(raw_rows) >= _CONSENSUS_ROW_CAP:
                raise HistoricalThreeFamilyFixedHorizonErrorV2(
                    "consensus.csv exceeds the bounded row cap"
                )
            if None in row or any(value is None for value in row.values()):
                raise HistoricalThreeFamilyFixedHorizonErrorV2(
                    "consensus.csv has a surplus or missing column"
                )
            raw_rows.append(cast(dict[str, str], row))
    except (UnicodeError, csv.Error) as exc:
        raise HistoricalThreeFamilyFixedHorizonErrorV2(
            "consensus.csv is not valid UTF-8 CSV"
        ) from exc
    rows: list[HistoricalConsensusCensusRowV2] = []
    for line, row in enumerate(raw_rows, start=2):
        try:
            rows.append(
                HistoricalConsensusCensusRowV2(
                    split=row["split"],
                    asset=row["asset"],
                    symbol=row["symbol"],
                    source_event_id=row["source_event_id"],
                    source_row_sha256=row["source_row_sha256"],
                    source_replay_manifest_sha256=row["source_replay_manifest_sha256"],
                    anchor_sha256=row["anchor_sha256"],
                    primary_family=row["primary_family"],
                    primary_direction=row["primary_direction"],
                    decision_time_ms=_parse_int(row["decision_time_ms"], "decision_time_ms"),
                    price=_parse_decimal(row["price"], "price"),
                    invalidation=_parse_optional_decimal(row["invalidation"], "invalidation"),
                    atr=_parse_decimal(row["atr"], "atr"),
                    event_id=row["event_id"],
                    payload_sha256=row["payload_sha256"],
                    canonical_consensus_sha256=row["canonical_consensus_sha256"],
                    topology_sha256=row["topology_sha256"],
                    canonical_topology_sha256=row["canonical_topology_sha256"],
                    topology_contract_sha256=row["topology_contract_sha256"],
                    topology_rule_version=row["topology_rule_version"],
                    topology_class=row["topology_class"],
                    topology_comparison_bucket=row["topology_comparison_bucket"],
                    topology_display_grade=row["topology_display_grade"],
                    topology_majority_direction=(row["topology_majority_direction"] or None),
                    topology_majority_family_count=_parse_optional_int(
                        row["topology_majority_family_count"],
                        "topology_majority_family_count",
                    ),
                    topology_opposing_family_count=_parse_optional_int(
                        row["topology_opposing_family_count"],
                        "topology_opposing_family_count",
                    ),
                    topology_has_opposition=_parse_optional_bool(
                        row["topology_has_opposition"],
                        "topology_has_opposition",
                    ),
                    topology_primary_support_count=_parse_optional_int(
                        row["topology_primary_support_count"],
                        "topology_primary_support_count",
                    ),
                    topology_primary_oppose_count=_parse_optional_int(
                        row["topology_primary_oppose_count"],
                        "topology_primary_oppose_count",
                    ),
                    topology_primary_neutral_count=_parse_optional_int(
                        row["topology_primary_neutral_count"],
                        "topology_primary_neutral_count",
                    ),
                    clean_primary_audit_eligible=_parse_bool(
                        row["clean_primary_audit_eligible"],
                        "clean_primary_audit_eligible",
                    ),
                    conflicted_comparator_eligible=_parse_bool(
                        row["conflicted_comparator_eligible"],
                        "conflicted_comparator_eligible",
                    ),
                    conflicted_comparator_outcome_authorized=_parse_bool(
                        row["conflicted_comparator_outcome_authorized"],
                        "conflicted_comparator_outcome_authorized",
                    ),
                    rule_version=row["rule_version"],
                    status=row["status"],
                    state_class=row["state_class"],
                    directional_numerator_micros=_parse_optional_int(
                        row["directional_numerator_micros"],
                        "directional_numerator_micros",
                    ),
                    directional_denominator=_parse_optional_int(
                        row["directional_denominator"], "directional_denominator"
                    ),
                    directional_agreement_micros=_parse_optional_int(
                        row["directional_agreement_micros"],
                        "directional_agreement_micros",
                    ),
                    bullish_family_count=_parse_optional_int(
                        row["bullish_family_count"], "bullish_family_count"
                    ),
                    bearish_family_count=_parse_optional_int(
                        row["bearish_family_count"], "bearish_family_count"
                    ),
                    neutral_family_count=_parse_optional_int(
                        row["neutral_family_count"], "neutral_family_count"
                    ),
                    primary_relationship=row["primary_relationship"],
                    admitted=_parse_bool(row["admitted"], "admitted"),
                    price_status=row["price_status"],
                    price_direction=_parse_optional_int(row["price_direction"], "price_direction"),
                    price_strength_micros=_parse_optional_int(
                        row["price_strength_micros"], "price_strength_micros"
                    ),
                    price_calculation_sha256=row["price_calculation_sha256"],
                    price_source_slice_sha256=row["price_source_slice_sha256"],
                    participation_status=row["participation_status"],
                    participation_direction=_parse_optional_int(
                        row["participation_direction"], "participation_direction"
                    ),
                    participation_strength_micros=_parse_optional_int(
                        row["participation_strength_micros"],
                        "participation_strength_micros",
                    ),
                    participation_calculation_sha256=row["participation_calculation_sha256"],
                    participation_source_slice_sha256=row["participation_source_slice_sha256"],
                    cross_section_status=row["cross_section_status"],
                    cross_section_direction=_parse_optional_int(
                        row["cross_section_direction"], "cross_section_direction"
                    ),
                    cross_section_strength_micros=_parse_optional_int(
                        row["cross_section_strength_micros"],
                        "cross_section_strength_micros",
                    ),
                    cross_section_calculation_sha256=row["cross_section_calculation_sha256"],
                    cross_section_source_slice_sha256=row["cross_section_source_slice_sha256"],
                    execution_contract_sha256=row["execution_contract_sha256"],
                    zero_move_round_trip_cost_micros=_parse_optional_int(
                        row["zero_move_round_trip_cost_micros"],
                        "zero_move_round_trip_cost_micros",
                    ),
                    atr_fraction_micros=_parse_optional_int(
                        row["atr_fraction_micros"], "atr_fraction_micros"
                    ),
                    one_atr_cost_headroom_micros=_parse_optional_int(
                        row["one_atr_cost_headroom_micros"],
                        "one_atr_cost_headroom_micros",
                    ),
                    cross_peer_set_root_sha256=row["cross_peer_set_root_sha256"],
                    cross_peer_input_sha256=row["cross_peer_input_sha256"],
                    reasons=tuple(row["reasons"].split("|")) if row["reasons"] else (),
                )
            )
        except (KeyError, TypeError) as exc:
            raise HistoricalThreeFamilyFixedHorizonErrorV2(
                f"consensus.csv line {line} is invalid"
            ) from exc
    if len({row.anchor_sha256 for row in rows}) != len(rows):
        raise HistoricalThreeFamilyFixedHorizonErrorV2(
            "consensus.csv contains duplicate anchor rows"
        )
    if len({row.event_id for row in rows}) != len(rows):
        raise HistoricalThreeFamilyFixedHorizonErrorV2("consensus.csv contains duplicate event IDs")
    return tuple(rows)


def _select_primary_supporting_events(
    rows: Sequence[HistoricalConsensusCensusRowV2],
    execution_contract_sha256: str,
    topology_contract_sha256: str,
) -> tuple[HistoricalConsensusOutcomeEventV2, ...]:
    events: list[HistoricalConsensusOutcomeEventV2] = []
    for row in rows:
        _validate_common_consensus_row(
            row,
            execution_contract_sha256,
            topology_contract_sha256,
        )
        if not row.admitted:
            continue
        try:
            state = DirectionalStateClassV2(row.state_class)
            direction = Direction(row.primary_direction)
            primary_family = SignalFamily(row.primary_family)
        except ValueError as exc:
            raise HistoricalThreeFamilyFixedHorizonErrorV2(
                "admitted consensus row has an unsupported state or direction"
            ) from exc
        _validate_admitted_topology(row, state, direction)
        agreement = row.directional_agreement_micros
        if agreement is None:
            raise HistoricalThreeFamilyFixedHorizonErrorV2(
                "admitted consensus row lacks directional agreement"
            )
        events.append(
            HistoricalConsensusOutcomeEventV2(
                split=row.split,
                asset=row.asset,
                symbol=row.symbol,
                event_id=row.event_id,
                anchor_sha256=row.anchor_sha256,
                primary_family=primary_family,
                primary_direction=direction,
                decision_time_ms=row.decision_time_ms,
                decision_price=row.price,
                invalidation=row.invalidation,
                atr=row.atr,
                state_class=state,
                directional_agreement_micros=agreement,
                execution_contract_sha256=row.execution_contract_sha256,
            )
        )
    return tuple(
        sorted(
            events, key=lambda item: (item.split, item.asset, item.decision_time_ms, item.event_id)
        )
    )


def _validate_common_consensus_row(
    row: HistoricalConsensusCensusRowV2,
    execution_contract_sha256: str,
    topology_contract_sha256: str,
) -> None:
    _require_asset(row.asset)
    _require_symbol(row.symbol, "consensus symbol")
    for value, label in (
        (row.anchor_sha256, "anchor_sha256"),
        (row.event_id, "event_id"),
        (row.payload_sha256, "payload_sha256"),
        (row.canonical_consensus_sha256, "canonical_consensus_sha256"),
        (row.topology_sha256, "topology_sha256"),
        (row.canonical_topology_sha256, "canonical_topology_sha256"),
        (row.topology_contract_sha256, "topology_contract_sha256"),
        (row.execution_contract_sha256, "execution_contract_sha256"),
    ):
        _require_sha256(value, label)
    if row.rule_version != HISTORICAL_THREE_FAMILY_CONSENSUS_RULE_VERSION_V2:
        raise HistoricalThreeFamilyFixedHorizonErrorV2(
            "consensus row rule version differs from the frozen V1 rule"
        )
    if row.execution_contract_sha256 != execution_contract_sha256:
        raise HistoricalThreeFamilyFixedHorizonErrorV2(
            "consensus row execution contract differs from the census manifest"
        )
    if row.topology_rule_version != HISTORICAL_THREE_FAMILY_TOPOLOGY_RULE_VERSION_V2:
        raise HistoricalThreeFamilyFixedHorizonErrorV2(
            "consensus row topology version differs from the pre-outcome amendment"
        )
    if row.topology_contract_sha256 != topology_contract_sha256:
        raise HistoricalThreeFamilyFixedHorizonErrorV2(
            "consensus row topology contract differs from the census manifest"
        )
    if row.clean_primary_audit_eligible is not row.admitted:
        raise HistoricalThreeFamilyFixedHorizonErrorV2(
            "clean topology eligibility differs from the frozen source admission"
        )
    if row.conflicted_comparator_outcome_authorized:
        raise HistoricalThreeFamilyFixedHorizonErrorV2(
            "conflicted-majority outcome use requires a separate frozen adapter"
        )


def _validate_admitted_topology(
    row: HistoricalConsensusCensusRowV2,
    state: DirectionalStateClassV2,
    direction: Direction,
) -> None:
    if row.status != "READY" or row.primary_relationship != "SUPPORTS_PRIMARY":
        raise HistoricalThreeFamilyFixedHorizonErrorV2(
            "admitted consensus must be READY and SUPPORTS_PRIMARY"
        )
    if state not in _CLEAN_STATE_CLASSES:
        raise HistoricalThreeFamilyFixedHorizonErrorV2(
            "admitted mixed/withheld topology cannot be pooled into current V1"
        )
    if direction not in (Direction.LONG, Direction.SHORT):
        raise HistoricalThreeFamilyFixedHorizonErrorV2(
            "admitted primary direction must be long or short"
        )
    counts = (
        row.bullish_family_count,
        row.bearish_family_count,
        row.neutral_family_count,
    )
    expected_counts = {
        DirectionalStateClassV2.BROAD_BULLISH_STATE: (3, 0, 0),
        DirectionalStateClassV2.BULLISH_STATE_TILT: (2, 0, 1),
        DirectionalStateClassV2.BROAD_BEARISH_STATE: (0, 3, 0),
        DirectionalStateClassV2.BEARISH_STATE_TILT: (0, 2, 1),
    }[state]
    if counts != expected_counts:
        raise HistoricalThreeFamilyFixedHorizonErrorV2(
            "admitted state counts do not match the frozen clean topology"
        )
    topology_expected = {
        DirectionalStateClassV2.BROAD_BULLISH_STATE: (
            HistoricalThreeFamilyTopologyClassV2.UNANIMOUS_BULLISH_3_0_0,
            HistoricalThreeFamilyComparisonBucketV2.BROAD_3_OF_3,
            HistoricalThreeFamilyDisplayGradeV2.UNANIMOUS_BREADTH_UNCALIBRATED,
            HistoricalThreeFamilyMajorityDirectionV2.BULLISH,
        ),
        DirectionalStateClassV2.BULLISH_STATE_TILT: (
            HistoricalThreeFamilyTopologyClassV2.CLEAN_BULLISH_2_0_1,
            HistoricalThreeFamilyComparisonBucketV2.CLEAN_2_PLUS_NEUTRAL,
            HistoricalThreeFamilyDisplayGradeV2.CLEAN_TWO_FAMILY_BREADTH_UNCALIBRATED,
            HistoricalThreeFamilyMajorityDirectionV2.BULLISH,
        ),
        DirectionalStateClassV2.BROAD_BEARISH_STATE: (
            HistoricalThreeFamilyTopologyClassV2.UNANIMOUS_BEARISH_0_3_0,
            HistoricalThreeFamilyComparisonBucketV2.BROAD_3_OF_3,
            HistoricalThreeFamilyDisplayGradeV2.UNANIMOUS_BREADTH_UNCALIBRATED,
            HistoricalThreeFamilyMajorityDirectionV2.BEARISH,
        ),
        DirectionalStateClassV2.BEARISH_STATE_TILT: (
            HistoricalThreeFamilyTopologyClassV2.CLEAN_BEARISH_0_2_1,
            HistoricalThreeFamilyComparisonBucketV2.CLEAN_2_PLUS_NEUTRAL,
            HistoricalThreeFamilyDisplayGradeV2.CLEAN_TWO_FAMILY_BREADTH_UNCALIBRATED,
            HistoricalThreeFamilyMajorityDirectionV2.BEARISH,
        ),
    }[state]
    topology_actual = (
        row.topology_class,
        row.topology_comparison_bucket,
        row.topology_display_grade,
        row.topology_majority_direction,
    )
    if topology_actual != tuple(value.value for value in topology_expected):
        raise HistoricalThreeFamilyFixedHorizonErrorV2(
            "admitted topology labels differ from the frozen clean comparison"
        )
    support_count = 3 if "BROAD" in state.value else 2
    neutral_count = 0 if support_count == 3 else 1
    if (
        row.topology_majority_family_count != support_count
        or row.topology_opposing_family_count != 0
        or row.topology_has_opposition is not False
        or row.topology_primary_support_count != support_count
        or row.topology_primary_oppose_count != 0
        or row.topology_primary_neutral_count != neutral_count
        or row.clean_primary_audit_eligible is not True
        or row.conflicted_comparator_eligible is not False
        or row.conflicted_comparator_outcome_authorized is not False
    ):
        raise HistoricalThreeFamilyFixedHorizonErrorV2(
            "admitted topology counts or authorization flags are inconsistent"
        )
    if row.directional_denominator != 3 or row.directional_numerator_micros is None:
        raise HistoricalThreeFamilyFixedHorizonErrorV2(
            "admitted consensus arithmetic is incomplete"
        )


def load_authenticated_historical_kline_panel_v2(
    loaded: LoadedHistoricalConsensusV2,
    data_root: str | Path,
) -> LoadedHistoricalKlinePanelV2:
    """Reauthenticate the exact seven kline files bound by the census manifest."""

    if type(loaded) is not LoadedHistoricalConsensusV2:
        raise HistoricalThreeFamilyFixedHorizonErrorV2(
            "kline panel loader requires authenticated consensus input"
        )
    root = Path(data_root).resolve()
    data_hashes = dict(loaded.futures_data_sha256)
    manifest_hashes = dict(loaded.futures_manifest_sha256)
    datasets: dict[str, KlineDataset] = {}
    authority: list[HistoricalOutcomeKlineAuthorityV2] = []
    for asset, filename in _FUTURES_FILE_BY_ASSET.items():
        relative_data = f"futures/{filename}"
        relative_manifest = f"{relative_data}.manifest.json"
        data_path = _resolve_relative_input(root, relative_data)
        manifest_path = _resolve_relative_input(root, relative_manifest)
        try:
            actual_data_sha256 = sha256_file(data_path)
            actual_manifest_sha256 = sha256_file(manifest_path)
        except OSError as exc:
            raise HistoricalThreeFamilyFixedHorizonErrorV2(
                f"cannot read authenticated kline input for {asset}"
            ) from exc
        if actual_data_sha256 != data_hashes[relative_data]:
            raise HistoricalThreeFamilyFixedHorizonErrorV2(
                f"kline SHA-256 differs from census authority for {asset}"
            )
        if actual_manifest_sha256 != manifest_hashes[relative_manifest]:
            raise HistoricalThreeFamilyFixedHorizonErrorV2(
                f"kline manifest SHA-256 differs from census authority for {asset}"
            )
        try:
            manifest = read_dataset_manifest(manifest_path)
            verify_dataset_manifest(data_path, manifest_path)
            dataset = read_kline_csv(data_path)
        except (DatasetValidationError, OSError) as exc:
            raise HistoricalThreeFamilyFixedHorizonErrorV2(
                f"invalid authenticated kline dataset for {asset}"
            ) from exc
        if (
            manifest.sha256 != data_hashes[relative_data]
            or manifest.market != Market.FUTURES.value
            or manifest.symbol != _FUTURES_SYMBOL_BY_ASSET[asset]
            or manifest.alias != asset
            or manifest.interval != "5m"
        ):
            raise HistoricalThreeFamilyFixedHorizonErrorV2(
                f"kline manifest identity differs from the frozen panel for {asset}"
            )
        datasets[asset] = dataset
        authority.append(
            HistoricalOutcomeKlineAuthorityV2(
                asset=asset,
                data_sha256=data_hashes[relative_data],
                manifest_sha256=manifest_hashes[relative_manifest],
                relative_data_path=relative_data,
                row_count=len(dataset.candles),
                symbol=dataset.request.symbol,
            )
        )
    return LoadedHistoricalKlinePanelV2(
        datasets=tuple(datasets.items()),
        authorities=tuple(authority),
    )


def _validate_futures_authority_keys(
    data_hashes: Mapping[str, str], manifest_hashes: Mapping[str, str]
) -> None:
    expected_data = {f"futures/{filename}" for filename in _FUTURES_FILE_BY_ASSET.values()}
    expected_manifests = {f"{value}.manifest.json" for value in expected_data}
    if set(data_hashes) != expected_data or set(manifest_hashes) != expected_manifests:
        raise HistoricalThreeFamilyFixedHorizonErrorV2(
            "census manifest does not bind exactly the frozen seven-asset Futures panel"
        )


def _validate_complete_outcome_census(
    events: Sequence[HistoricalConsensusOutcomeEventV2],
    rows: Sequence[HistoricalFixedHorizonOutcomeRowV2],
) -> None:
    expected = set(HISTORICAL_THREE_FAMILY_OUTCOME_HORIZONS_BARS_V2)
    grouped: dict[str, set[int]] = {event.event_id: set() for event in events}
    for row in rows:
        if row.event_id not in grouped or row.horizon_bars in grouped[row.event_id]:
            raise HistoricalThreeFamilyFixedHorizonErrorV2(
                "fixed-horizon rows contain an unknown or duplicate event/horizon"
            )
        grouped[row.event_id].add(row.horizon_bars)
    if any(horizons != expected for horizons in grouped.values()):
        raise HistoricalThreeFamilyFixedHorizonErrorV2(
            "every admitted event must contain exactly five frozen horizons"
        )


_OUTCOME_COLUMNS: Final = tuple(field.name for field in fields(HistoricalFixedHorizonOutcomeRowV2))


def _fixed_horizon_csv_bytes(
    rows: Sequence[HistoricalFixedHorizonOutcomeRowV2],
) -> bytes:
    ordered = tuple(
        sorted(
            rows,
            key=lambda item: (
                item.split,
                item.asset,
                item.decision_time_ms,
                item.event_id,
                item.horizon_bars,
            ),
        )
    )
    buffer = io.StringIO(newline="")
    writer: csv.DictWriter[str] = csv.DictWriter(
        buffer,
        fieldnames=list(_OUTCOME_COLUMNS),
        extrasaction="raise",
        lineterminator="\n",
    )
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
    payload = buffer.getvalue().encode("utf-8")
    if b"\r" in payload or not payload.endswith(b"\n"):
        raise HistoricalThreeFamilyFixedHorizonErrorV2(
            "fixed-horizon CSV serialization is not canonical LF-only UTF-8"
        )
    return payload


def _parse_fixed_horizon_csv_v2(
    raw: bytes,
) -> tuple[HistoricalFixedHorizonOutcomeRowV2, ...]:
    records = _parse_exact_csv_records_v2(raw, _OUTCOME_COLUMNS, "fixed-horizon outcomes")
    rows: list[HistoricalFixedHorizonOutcomeRowV2] = []
    for record in records:
        rows.append(
            HistoricalFixedHorizonOutcomeRowV2(
                topology_version=record["topology_version"],
                split=record["split"],
                asset=record["asset"],
                symbol=record["symbol"],
                event_id=record["event_id"],
                anchor_sha256=record["anchor_sha256"],
                rule_version=record["rule_version"],
                outcome_protocol_version=record["outcome_protocol_version"],
                execution_contract_sha256=record["execution_contract_sha256"],
                state_class=record["state_class"],
                agreement_bucket=record["agreement_bucket"],
                primary_direction=record["primary_direction"],
                directional_agreement_micros=_parse_int(
                    record["directional_agreement_micros"],
                    "directional_agreement_micros",
                ),
                decision_time_ms=_parse_int(record["decision_time_ms"], "decision_time_ms"),
                horizon_bars=_parse_int(record["horizon_bars"], "horizon_bars"),
                horizon_minutes=_parse_int(record["horizon_minutes"], "horizon_minutes"),
                expected_entry_time_ms=_parse_int(
                    record["expected_entry_time_ms"], "expected_entry_time_ms"
                ),
                expected_exit_close_time_ms=_parse_int(
                    record["expected_exit_close_time_ms"],
                    "expected_exit_close_time_ms",
                ),
                entry_price=_parse_optional_decimal(record["entry_price"], "entry_price"),
                exit_price=_parse_optional_decimal(record["exit_price"], "exit_price"),
                gross_directional_return_micros=_parse_optional_int(
                    record["gross_directional_return_micros"],
                    "gross_directional_return_micros",
                ),
                slippage_return_micros=_parse_optional_int(
                    record["slippage_return_micros"], "slippage_return_micros"
                ),
                fee_return_micros=_parse_optional_int(
                    record["fee_return_micros"], "fee_return_micros"
                ),
                funding_return_micros=_parse_optional_int(
                    record["funding_return_micros"], "funding_return_micros"
                ),
                rounding_residual_micros=_parse_optional_int(
                    record["rounding_residual_micros"], "rounding_residual_micros"
                ),
                total_cost_micros=_parse_optional_int(
                    record["total_cost_micros"], "total_cost_micros"
                ),
                funding_event_count=_parse_optional_int(
                    record["funding_event_count"], "funding_event_count"
                ),
                evaluable=_parse_bool(record["evaluable"], "evaluable"),
                exclusion_reason=record["exclusion_reason"],
                net_return_micros=_parse_optional_int(
                    record["net_return_micros"], "net_return_micros"
                ),
                historical_only=cast(
                    Literal[True], _parse_bool(record["historical_only"], "historical_only")
                ),
                probability=cast(Literal[False], _parse_bool(record["probability"], "probability")),
                probability_calibrated=cast(
                    Literal[False],
                    _parse_bool(record["probability_calibrated"], "probability_calibrated"),
                ),
                promoting=cast(Literal[False], _parse_bool(record["promoting"], "promoting")),
                order_placement=cast(
                    Literal[False], _parse_bool(record["order_placement"], "order_placement")
                ),
            )
        )
    canonical = _fixed_horizon_csv_bytes(rows)
    if raw != canonical:
        raise HistoricalThreeFamilyFixedHorizonErrorV2(
            "fixed-horizon outcomes are not in canonical row order or representation"
        )
    return tuple(rows)


def _parse_exact_csv_records_v2(
    raw: bytes,
    columns: tuple[str, ...],
    label: str,
) -> tuple[dict[str, str], ...]:
    if not raw or b"\r" in raw or not raw.endswith(b"\n"):
        raise HistoricalThreeFamilyFixedHorizonErrorV2(
            f"{label} must be non-empty canonical LF-only bytes"
        )
    try:
        reader = csv.DictReader(io.StringIO(raw.decode("utf-8"), newline=""), strict=True)
        if tuple(reader.fieldnames or ()) != columns:
            raise HistoricalThreeFamilyFixedHorizonErrorV2(
                f"{label} header differs from the frozen schema"
            )
        records: list[dict[str, str]] = []
        for record in reader:
            if len(records) >= _CONSENSUS_ROW_CAP * len(
                HISTORICAL_THREE_FAMILY_OUTCOME_HORIZONS_BARS_V2
            ):
                raise HistoricalThreeFamilyFixedHorizonErrorV2(
                    f"{label} exceeds the bounded row cap"
                )
            if None in record or any(value is None for value in record.values()):
                raise HistoricalThreeFamilyFixedHorizonErrorV2(
                    f"{label} has a surplus or missing column"
                )
            records.append(cast(dict[str, str], record))
    except (UnicodeError, csv.Error) as exc:
        raise HistoricalThreeFamilyFixedHorizonErrorV2(f"{label} is not valid UTF-8 CSV") from exc
    return tuple(records)


def _validate_loaded_fixed_horizon_manifest_v2(
    document: Mapping[str, object],
    *,
    expected_census_manifest_sha256: str,
    expected_experiment_contract_sha256: str,
    expected_topology_amendment_sha256: str,
    expected_funding_authority_manifest_sha256: str,
    expected_downstream_code_freeze_manifest_sha256: str,
) -> None:
    required: Mapping[str, object] = {
        "census_manifest_sha256": expected_census_manifest_sha256,
        "consensus_rule_version": HISTORICAL_THREE_FAMILY_CONSENSUS_RULE_VERSION_V2,
        "downstream_code_freeze_manifest_sha256": (expected_downstream_code_freeze_manifest_sha256),
        "execution_contract_sha256": (
            build_historical_execution_contract_v2().execution_contract_sha256
        ),
        "experiment_contract_sha256": expected_experiment_contract_sha256,
        "historical_only": True,
        "horizons_bars": list(HISTORICAL_THREE_FAMILY_OUTCOME_HORIZONS_BARS_V2),
        "order_placement": False,
        "outcome_protocol_version": HISTORICAL_THREE_FAMILY_OUTCOME_PROTOCOL_V2,
        "probability": False,
        "promoting": False,
        "protocol": HISTORICAL_THREE_FAMILY_FIXED_HORIZON_RUNNER_PROTOCOL_V2,
        "schema_version": HISTORICAL_THREE_FAMILY_FIXED_HORIZON_SCHEMA_VERSION_V2,
        "topology_amendment_sha256": expected_topology_amendment_sha256,
        "topology_version": HISTORICAL_THREE_FAMILY_PRIMARY_TOPOLOGY_V2,
    }
    for key, expected in required.items():
        if document.get(key) != expected:
            raise HistoricalThreeFamilyFixedHorizonErrorV2(
                f"fixed-horizon manifest field {key} differs from frozen authority"
            )
    _require_sha256(document.get("consensus_sha256"), "fixed-horizon consensus SHA-256")
    funding = _require_dict(document.get("funding_authority"), "funding authority")
    if funding.get("manifest_sha256") != expected_funding_authority_manifest_sha256:
        raise HistoricalThreeFamilyFixedHorizonErrorV2(
            "fixed-horizon funding authority differs from external authority"
        )
    if funding.get("missing_dataset_policy") != "EXPLICIT_EXCLUSION_NOT_ZERO":
        raise HistoricalThreeFamilyFixedHorizonErrorV2(
            "fixed-horizon funding missing-data policy differs"
        )
    outputs = _require_dict(document.get("outputs"), "fixed-horizon outputs")
    if set(outputs) != set(_OUTCOME_NAMES):
        raise HistoricalThreeFamilyFixedHorizonErrorV2("fixed-horizon output hash set is not exact")
    for name, digest in outputs.items():
        _require_sha256(digest, f"fixed-horizon output {name}")


def _validate_loaded_fixed_horizon_results_v2(
    document: Mapping[str, object],
    *,
    manifest: Mapping[str, object],
    rows: tuple[HistoricalFixedHorizonOutcomeRowV2, ...],
    outcomes_sha256: str,
    expected_funding_authority_manifest_sha256: str,
    expected_downstream_code_freeze_manifest_sha256: str,
) -> None:
    required: Mapping[str, object] = {
        "bootstrap_included": False,
        "census_manifest_sha256": manifest.get("census_manifest_sha256"),
        "consensus_rule_version": HISTORICAL_THREE_FAMILY_CONSENSUS_RULE_VERSION_V2,
        "downstream_code_freeze_manifest_sha256": (expected_downstream_code_freeze_manifest_sha256),
        "consensus_sha256": manifest.get("consensus_sha256"),
        "execution_contract_sha256": manifest.get("execution_contract_sha256"),
        "experiment_contract_sha256": manifest.get("experiment_contract_sha256"),
        "fixed_horizon_outcomes_sha256": outcomes_sha256,
        "frozen_formula_efficacy_validated": False,
        "funding_authority_manifest_sha256": expected_funding_authority_manifest_sha256,
        "funding_missing_is_zero": False,
        "historical_only": True,
        "horizons_bars": list(HISTORICAL_THREE_FAMILY_OUTCOME_HORIZONS_BARS_V2),
        "inference_complete": False,
        "multiplicity_claim": False,
        "order_placement": False,
        "outcome_protocol_version": HISTORICAL_THREE_FAMILY_OUTCOME_PROTOCOL_V2,
        "probability": False,
        "probability_calibrated": False,
        "promoting": False,
        "protocol": HISTORICAL_THREE_FAMILY_FIXED_HORIZON_RUNNER_PROTOCOL_V2,
        "schema_version": HISTORICAL_THREE_FAMILY_FIXED_HORIZON_SCHEMA_VERSION_V2,
        "topology_amendment_sha256": manifest.get("topology_amendment_sha256"),
        "topology_version": HISTORICAL_THREE_FAMILY_PRIMARY_TOPOLOGY_V2,
    }
    for key, expected in required.items():
        if document.get(key) != expected:
            raise HistoricalThreeFamilyFixedHorizonErrorV2(
                f"fixed-horizon results field {key} differs from frozen authority"
            )
    admitted = _require_nonnegative_int_v2(document.get("admitted_events"), "admitted_events")
    row_count = _require_nonnegative_int_v2(document.get("outcome_rows"), "outcome_rows")
    evaluable = _require_nonnegative_int_v2(
        document.get("evaluable_outcomes"), "evaluable_outcomes"
    )
    unevaluable = _require_nonnegative_int_v2(
        document.get("unevaluable_outcomes"), "unevaluable_outcomes"
    )
    if (
        row_count != len(rows)
        or row_count != admitted * len(HISTORICAL_THREE_FAMILY_OUTCOME_HORIZONS_BARS_V2)
        or evaluable != sum(row.evaluable for row in rows)
        or unevaluable != row_count - evaluable
    ):
        raise HistoricalThreeFamilyFixedHorizonErrorV2(
            "fixed-horizon results counts do not reconcile to exact outcome rows"
        )
    keys = [(row.event_id, row.horizon_bars) for row in rows]
    if len(keys) != len(set(keys)):
        raise HistoricalThreeFamilyFixedHorizonErrorV2(
            "fixed-horizon artifacts contain duplicate event/horizon rows"
        )
    by_event = Counter(row.event_id for row in rows)
    if len(by_event) != admitted or any(
        count != len(HISTORICAL_THREE_FAMILY_OUTCOME_HORIZONS_BARS_V2)
        for count in by_event.values()
    ):
        raise HistoricalThreeFamilyFixedHorizonErrorV2(
            "fixed-horizon event population is incomplete"
        )


def _results_document(
    *,
    loaded: LoadedHistoricalConsensusV2,
    outcomes: tuple[HistoricalFixedHorizonOutcomeRowV2, ...],
    audit: HistoricalThreeFamilyOutcomeAuditV2 | None,
    funding_authority: LoadedHistoricalFundingAuthorityV2 | None,
    downstream_code_freeze_manifest_sha256: str,
    outcomes_sha256: str,
) -> dict[str, object]:
    exclusion_counts = Counter(row.exclusion_reason for row in outcomes if not row.evaluable)
    evaluable = sum(row.evaluable for row in outcomes)
    component_summaries = summarize_historical_fixed_horizon_cost_components_v2(outcomes)
    execution_schedule = _execution_schedule_document(loaded.execution_contract_sha256)
    return {
        "admitted_events": len(loaded.events),
        "audit": None if audit is None else _audit_document(audit),
        "audit_version": HISTORICAL_THREE_FAMILY_OUTCOME_AUDIT_VERSION_V2,
        "bootstrap_included": False,
        "census_manifest_sha256": loaded.census_manifest_sha256,
        "census_rows": loaded.census_rows,
        "consensus_rule_version": HISTORICAL_THREE_FAMILY_CONSENSUS_RULE_VERSION_V2,
        "downstream_code_freeze_manifest_sha256": downstream_code_freeze_manifest_sha256,
        "consensus_sha256": loaded.consensus_sha256,
        "cost_attribution_summaries": [asdict(value) for value in component_summaries],
        "cost_attribution_summary_version": (
            HISTORICAL_THREE_FAMILY_COST_ATTRIBUTION_SUMMARY_VERSION_V2
        ),
        "evaluable_outcomes": evaluable,
        "execution_contract": execution_schedule,
        "execution_contract_sha256": loaded.execution_contract_sha256,
        "exclusion_counts": dict(sorted(exclusion_counts.items())),
        "experiment_contract_sha256": loaded.experiment_contract_sha256,
        "topology_amendment_sha256": loaded.topology_amendment_sha256,
        "fixed_horizon_outcomes_sha256": outcomes_sha256,
        "frozen_formula_efficacy_validated": False,
        "funding_authority_manifest_sha256": (
            None if funding_authority is None else funding_authority.manifest_sha256
        ),
        "funding_missing_is_zero": False,
        "historical_only": True,
        "horizons_bars": list(HISTORICAL_THREE_FAMILY_OUTCOME_HORIZONS_BARS_V2),
        "inference_complete": False,
        "multiplicity_claim": False,
        "order_placement": False,
        "outcome_protocol_version": HISTORICAL_THREE_FAMILY_OUTCOME_PROTOCOL_V2,
        "outcome_rows": len(outcomes),
        "probability": False,
        "probability_calibrated": False,
        "promoting": False,
        "protocol": HISTORICAL_THREE_FAMILY_FIXED_HORIZON_RUNNER_PROTOCOL_V2,
        "schema_version": HISTORICAL_THREE_FAMILY_FIXED_HORIZON_SCHEMA_VERSION_V2,
        "topology_version": HISTORICAL_THREE_FAMILY_PRIMARY_TOPOLOGY_V2,
        "unevaluable_outcomes": len(outcomes) - evaluable,
    }


def _audit_document(audit: HistoricalThreeFamilyOutcomeAuditV2) -> dict[str, object]:
    return {
        "audit_version": audit.audit_version,
        "consensus_rule_version": audit.consensus_rule_version,
        "contrasts": [asdict(value) for value in audit.contrasts],
        "event_count": audit.event_count,
        "execution_contract_sha256s": list(audit.execution_contract_sha256s),
        "frozen_formula_efficacy_validated": audit.frozen_formula_efficacy_validated,
        "historical_only": audit.historical_only,
        "horizons_bars": list(audit.horizons_bars),
        "inference_complete": audit.inference_complete,
        "outcome_count": audit.outcome_count,
        "outcome_protocol_version": audit.outcome_protocol_version,
        "overlapping_event_drawdown_valid": audit.overlapping_event_drawdown_valid,
        "probability": audit.probability,
        "probability_calibrated": audit.probability_calibrated,
        "promoting": audit.promoting,
        "summaries": [asdict(value) for value in audit.summaries],
    }


def _outcome_manifest_document(
    *,
    loaded: LoadedHistoricalConsensusV2,
    kline_authority: tuple[HistoricalOutcomeKlineAuthorityV2, ...],
    funding_authority: LoadedHistoricalFundingAuthorityV2 | None,
    downstream_code_freeze_manifest_sha256: str,
    output_hashes: Mapping[str, str],
) -> dict[str, object]:
    if set(output_hashes) != set(_OUTCOME_NAMES):
        raise HistoricalThreeFamilyFixedHorizonErrorV2(
            "outcome manifest requires exactly the two payload hashes"
        )
    return {
        "census_manifest_sha256": loaded.census_manifest_sha256,
        "consensus_rule_version": HISTORICAL_THREE_FAMILY_CONSENSUS_RULE_VERSION_V2,
        "downstream_code_freeze_manifest_sha256": downstream_code_freeze_manifest_sha256,
        "consensus_sha256": loaded.consensus_sha256,
        "execution_contract": _execution_schedule_document(loaded.execution_contract_sha256),
        "execution_contract_sha256": loaded.execution_contract_sha256,
        "experiment_contract_sha256": loaded.experiment_contract_sha256,
        "topology_amendment_sha256": loaded.topology_amendment_sha256,
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
        "horizons_bars": list(HISTORICAL_THREE_FAMILY_OUTCOME_HORIZONS_BARS_V2),
        "inputs": {
            "futures_klines": [asdict(value) for value in kline_authority],
        },
        "order_placement": False,
        "outcome_protocol_version": HISTORICAL_THREE_FAMILY_OUTCOME_PROTOCOL_V2,
        "outputs": {
            name: _require_sha256(output_hashes[name], f"output hash {name}")
            for name in _OUTCOME_NAMES
        },
        "probability": False,
        "promoting": False,
        "protocol": HISTORICAL_THREE_FAMILY_FIXED_HORIZON_RUNNER_PROTOCOL_V2,
        "schema_version": HISTORICAL_THREE_FAMILY_FIXED_HORIZON_SCHEMA_VERSION_V2,
        "topology_version": HISTORICAL_THREE_FAMILY_PRIMARY_TOPOLOGY_V2,
    }


def _execution_schedule_document(expected_sha256: str) -> dict[str, object]:
    contract = build_historical_execution_contract_v2()
    if contract.execution_contract_sha256 != expected_sha256:
        raise HistoricalThreeFamilyFixedHorizonErrorV2(
            "loaded execution contract differs from the frozen schedule"
        )
    return {
        "fee_bps_per_side": contract.fee_bps_per_side,
        "slippage_bps_per_side": contract.slippage_bps_per_side,
        "zero_move_round_trip_cost_micros": (contract.zero_move_round_trip_cost_micros),
    }


def _publish_artifacts(target: Path, payloads: Mapping[str, bytes]) -> None:
    if set(payloads) != _PUBLISHED_NAMES:
        raise HistoricalThreeFamilyFixedHorizonErrorV2(
            "fixed-horizon publication requires exactly three artifacts"
        )
    if target.exists():
        raise HistoricalThreeFamilyFixedHorizonErrorV2(
            "fixed-horizon output requires a fresh target directory"
        )
    target.parent.mkdir(parents=True, exist_ok=True)
    staging = Path(tempfile.mkdtemp(prefix=f".{target.name}.", dir=target.parent))
    try:
        for name, payload in payloads.items():
            (staging / name).write_bytes(payload)
        os.replace(staging, target)
    except OSError as exc:
        shutil.rmtree(staging, ignore_errors=True)
        raise HistoricalThreeFamilyFixedHorizonErrorV2(
            "cannot atomically publish fixed-horizon artifacts"
        ) from exc


def _validate_optional_funding_authority_pair(
    manifest_path: str | Path | None, expected_sha256: str | None
) -> None:
    if (manifest_path is None) != (expected_sha256 is None):
        raise HistoricalThreeFamilyFixedHorizonErrorV2(
            "funding authority path and frozen SHA-256 must be supplied together"
        )
    if expected_sha256 is not None:
        _require_sha256(expected_sha256, "expected funding authority SHA-256")


def _validate_funding_binding_panel(binding: HistoricalFundingFileBindingV2) -> None:
    if binding.symbol not in set(_FUTURES_SYMBOL_BY_ASSET.values()):
        raise HistoricalThreeFamilyFixedHorizonErrorV2(
            "funding binding symbol is outside the frozen seven-asset panel"
        )


def _string_hash_map(value: object, label: str) -> dict[str, str]:
    raw = _require_dict(value, label)
    result: dict[str, str] = {}
    for key, digest in raw.items():
        if not isinstance(key, str):
            raise HistoricalThreeFamilyFixedHorizonErrorV2(f"{label} key must be text")
        _require_relative_posix_path(key, f"{label} path")
        result[key] = _require_sha256(digest, f"{label} hash")
    return result


def _require_dict(value: object, label: str) -> dict[str, object]:
    if not isinstance(value, dict):
        raise HistoricalThreeFamilyFixedHorizonErrorV2(f"{label} must be an object")
    return cast(dict[str, object], value)


def _decode_canonical_json_object(raw: bytes, label: str) -> dict[str, object]:
    try:
        value = json.loads(raw)
    except (json.JSONDecodeError, UnicodeError) as exc:
        raise HistoricalThreeFamilyFixedHorizonErrorV2(f"{label} is not valid UTF-8 JSON") from exc
    document = _require_dict(value, label)
    try:
        canonical = canonical_json_line(document)
    except (TypeError, ValueError) as exc:
        raise HistoricalThreeFamilyFixedHorizonErrorV2(
            f"{label} contains unsupported protocol JSON"
        ) from exc
    if raw != canonical:
        raise HistoricalThreeFamilyFixedHorizonErrorV2(f"{label} must be canonical RFC 8785 JSONL")
    return document


def _read_required_bytes(path: Path, label: str) -> bytes:
    try:
        return path.read_bytes()
    except OSError as exc:
        raise HistoricalThreeFamilyFixedHorizonErrorV2(f"cannot read {label}: {path}") from exc


def _require_exact_artifact_files_v2(
    root: Path,
    expected_names: frozenset[str],
    label: str,
) -> None:
    try:
        entries = tuple(root.iterdir())
    except OSError as exc:
        raise HistoricalThreeFamilyFixedHorizonErrorV2(
            f"cannot inspect {label} artifact directory: {root}"
        ) from exc
    if any(entry.is_symlink() or not entry.is_file() for entry in entries) or {
        entry.name for entry in entries
    } != set(expected_names):
        raise HistoricalThreeFamilyFixedHorizonErrorV2(
            f"{label} artifact directory does not contain the exact published file set"
        )


def _resolve_relative_input(root: Path, relative_path: str) -> Path:
    _require_relative_posix_path(relative_path, "input relative path")
    candidate = (root / Path(*relative_path.split("/"))).resolve()
    if candidate == root or root not in candidate.parents:
        raise HistoricalThreeFamilyFixedHorizonErrorV2("input path escapes the declared data root")
    return candidate


def _require_relative_posix_path(value: object, label: str) -> str:
    if (
        not isinstance(value, str)
        or not value
        or value.startswith("/")
        or "\\" in value
        or "//" in value
        or any(part in {"", ".", ".."} for part in value.split("/"))
    ):
        raise HistoricalThreeFamilyFixedHorizonErrorV2(
            f"{label} must be a normalized relative POSIX path"
        )
    return value


def _require_sha256(value: object, label: str) -> str:
    if not isinstance(value, str) or _SHA256_RE.fullmatch(value) is None:
        raise HistoricalThreeFamilyFixedHorizonErrorV2(
            f"{label} must be a lowercase SHA-256 digest"
        )
    return value


def _require_symbol(value: str, label: str) -> None:
    if not isinstance(value, str) or _SYMBOL_RE.fullmatch(value) is None:
        raise HistoricalThreeFamilyFixedHorizonErrorV2(
            f"{label} must be an uppercase normalized symbol"
        )


def _require_asset(value: str) -> None:
    if not isinstance(value, str) or _ASSET_RE.fullmatch(value) is None:
        raise HistoricalThreeFamilyFixedHorizonErrorV2(
            "asset must be an uppercase normalized identifier"
        )


def _parse_bool(value: str, label: str) -> bool:
    if value == "true":
        return True
    if value == "false":
        return False
    raise HistoricalThreeFamilyFixedHorizonErrorV2(f"{label} must be true or false")


def _parse_optional_bool(value: str, label: str) -> bool | None:
    return None if value == "" else _parse_bool(value, label)


def _parse_int(value: str, label: str) -> int:
    if not value or value.startswith("+") or (value.startswith("0") and value != "0"):
        raise HistoricalThreeFamilyFixedHorizonErrorV2(f"{label} must be canonical integer text")
    try:
        parsed = int(value)
    except ValueError as exc:
        raise HistoricalThreeFamilyFixedHorizonErrorV2(
            f"{label} must be canonical integer text"
        ) from exc
    if str(parsed) != value or not -_JCS_SAFE_INTEGER_MAX <= parsed <= _JCS_SAFE_INTEGER_MAX:
        raise HistoricalThreeFamilyFixedHorizonErrorV2(
            f"{label} must be a JCS-safe canonical integer"
        )
    return parsed


def _parse_optional_int(value: str, label: str) -> int | None:
    return None if value == "" else _parse_int(value, label)


def _require_nonnegative_int_v2(value: object, label: str) -> int:
    if type(value) is not int or value < 0 or value > _JCS_SAFE_INTEGER_MAX:
        raise HistoricalThreeFamilyFixedHorizonErrorV2(
            f"{label} must be a nonnegative JCS-safe integer"
        )
    return value


def _parse_decimal(value: str, label: str) -> Decimal:
    if not value or value.startswith("+") or value.strip() != value:
        raise HistoricalThreeFamilyFixedHorizonErrorV2(
            f"{label} must be canonical finite decimal text"
        )
    try:
        parsed = Decimal(value)
    except InvalidOperation as exc:
        raise HistoricalThreeFamilyFixedHorizonErrorV2(
            f"{label} must be canonical finite decimal text"
        ) from exc
    if not parsed.is_finite():
        raise HistoricalThreeFamilyFixedHorizonErrorV2(f"{label} must be finite")
    return parsed


def _parse_optional_decimal(value: str, label: str) -> Decimal | None:
    return None if value == "" else _parse_decimal(value, label)


def historical_return_to_micros_v2(value: float) -> int:
    """Round one finite binary return to micros, nearest with ties away."""

    if not math.isfinite(value):
        raise HistoricalThreeFamilyFixedHorizonErrorV2("execution return must be finite")
    return int((Decimal(str(value)) * _MICROS).to_integral_value(rounding=ROUND_HALF_UP))


def _mean_integer(values: Sequence[int]) -> int | None:
    return None if not values else _round_ratio(sum(values), len(values))


def _round_ratio(numerator: int, denominator: int) -> int:
    if denominator <= 0:
        raise HistoricalThreeFamilyFixedHorizonErrorV2("ratio denominator must be positive")
    sign = -1 if numerator < 0 else 1
    quotient, remainder = divmod(abs(numerator), denominator)
    if 2 * remainder >= denominator:
        quotient += 1
    return sign * quotient


def _sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run frozen historical three-family fixed-horizon outcomes"
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
    args = _parser().parse_args(argv)
    artifacts = run_historical_three_family_fixed_horizons_v2(
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
