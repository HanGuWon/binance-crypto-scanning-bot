"""Outcome-blind historical census for the frozen three-family consensus.

The runner authenticates the latest V1A Amendment-1 recommendation ledgers,
selects the complete Futures pullback population without consulting fitted V1A
scores or any forward-return ledger, and evaluates the three retrospective
kline proxies at each closed decision bar.  Outputs remain historical,
non-promoting, and explicitly uncalibrated.
"""

from __future__ import annotations

import argparse
import csv
import errno
import hashlib
import io
import json
import os
import shutil
import tempfile
from collections import Counter, defaultdict
from collections.abc import Callable, Mapping, Sequence
from dataclasses import asdict, dataclass, fields
from decimal import Decimal, DecimalException, InvalidOperation, localcontext
from enum import StrEnum
from itertools import product
from pathlib import Path
from typing import Final, Literal, cast

from signalbot.backtest.alert_replay import RecommendationEvent
from signalbot.backtest.dataset import (
    DatasetManifest,
    DatasetValidationError,
    KlineDataset,
    find_kline_gaps,
    read_dataset_manifest,
    read_kline_csv,
    sha256_file,
)
from signalbot.domain.enums import Direction, Market, SignalFamily
from signalbot.domain.models import Candle
from signalbot.r4b_v2.canonical import canonical_json_line
from signalbot.r4b_v2.capture.models import VenueV2
from signalbot.r4b_v2.protocol.decimal_context import protocol_decimal_context_v2
from signalbot.r4b_v2.protocol.decision_clock import FIVE_MINUTE_MS_V2
from signalbot.r4b_v2.research.historical_three_family_topology import (
    HISTORICAL_THREE_FAMILY_TOPOLOGY_RULE_VERSION_V2,
    HistoricalThreeFamilyTopologyClassV2,
    HistoricalThreeFamilyTopologyV2,
    canonical_historical_three_family_topology_v2,
    derive_historical_three_family_topology_v2,
)
from signalbot.r4b_v2.strategy.cross_sectional_historical_7asset_proxy import (
    HISTORICAL_CROSS_SECTIONAL_7ASSET_PEER_COUNT_V2,
    HistoricalCrossSectional7AssetCalculationV2,
    calculate_historical_cross_sectional_7asset_returns_v2,
)
from signalbot.r4b_v2.strategy.family_c import (
    FAMILY_C_PANEL_BAR_COUNT_V2,
    FamilyCClosedCandleV2,
)
from signalbot.r4b_v2.strategy.historical_numeric_precompute import (
    HISTORICAL_NUMERIC_PRECOMPUTE_RULE_VERSION_V2,
    HistoricalNumericPrecomputeContractErrorV2,
    HistoricalR3SeriesCacheV2,
    HistoricalTargetNumericCacheV2,
    build_historical_r3_series_cache_v2,
    build_historical_target_excluded_median_r3_cache_v2,
    build_historical_target_numeric_cache_v2,
    calculate_historical_cross_anchor_v2,
    calculate_historical_target_anchor_v2,
)
from signalbot.r4b_v2.strategy.historical_three_family_consensus import (
    HISTORICAL_THREE_FAMILY_CONSENSUS_RULE_VERSION_V2,
    HISTORICAL_THREE_FAMILY_PANEL_SYMBOLS_V2,
    HISTORICAL_THREE_FAMILY_SOURCE_PROTOCOL_VERSION_V2,
    HISTORICAL_THREE_FAMILY_SOURCE_RULE_VERSION_V2,
    HistoricalDirectionalLeafV2,
    HistoricalExecutionContractV2,
    HistoricalFamilyV2,
    HistoricalRecommendationAnchorV2,
    HistoricalThreeFamilyConsensusV2,
    build_historical_directional_leaf_from_calculation_v2,
    build_historical_execution_contract_v2,
    build_historical_three_family_consensus_from_leaves_v2,
    canonical_historical_three_family_consensus_v2,
)
from signalbot.r4b_v2.strategy.participation_evidence import (
    ROBUST_Z_PRIOR_WINDOW_V2,
    ParticipationFlowCalculationV2,
    build_participation_flow_bar_value_v2,
    calculate_participation_flow_v2,
)
from signalbot.r4b_v2.strategy.price_evidence import (
    PRICE_STRUCTURE_MOMENTUM_ROW_COUNT_V2,
    PriceClosePathCalculationV2,
    calculate_price_close_path_v2,
)

HISTORICAL_THREE_FAMILY_CENSUS_PROTOCOL_V2: Final = (
    "historical_three_family_census_v2_2026-07-20"
)
HISTORICAL_THREE_FAMILY_CENSUS_SCHEMA_VERSION_V2: Final = 1
HISTORICAL_THREE_FAMILY_CENSUS_SPLITS_V2: Final = (
    "development",
    "validation",
    "retrospective_test",
)
HISTORICAL_THREE_FAMILY_EXPECTED_ANCHORS_BY_SPLIT_V2: Final = {
    "development": 2_263,
    "validation": 2_087,
    "retrospective_test": 1_991,
}
HISTORICAL_THREE_FAMILY_EXPECTED_ANCHORS_V2: Final = sum(
    HISTORICAL_THREE_FAMILY_EXPECTED_ANCHORS_BY_SPLIT_V2.values()
)

_SPLIT_RANGES_MS: Final = {
    "development": (1_719_792_000_000, 1_740_787_200_000),
    "validation": (1_740_787_200_000, 1_761_955_200_000),
    "retrospective_test": (1_761_955_200_000, 1_782_864_000_000),
}
_RUN_MANIFEST_SHA256_BY_SPLIT: Final = {
    "development": "68699ac1b924061669b767a65f201c1338525835470bf2db4acdff170b571a67",
    "validation": "957cabd27ded47075f8119b8afafb5b8b9b6eb633435156af0f38fd04b9db4d6",
    "retrospective_test": "4208cfd32a4475e13e8663eee133f840caae2de6019f1659e4fa58b7e3e5f762",
}
_RECOMMENDATIONS_SHA256_BY_SPLIT: Final = {
    "development": "568c5750caddb78a9135ce345ef03b5c9c3e038c8885ec632cde8d3e44379ea4",
    "validation": "558319cdad25f459dcb8f73f1ef2a737832fb1ce295be1b039bd8e36c5877a4a",
    "retrospective_test": "e4670a19437bc57df9e27706b5e093174b92d28fc42776260dfd09a9b40ea04e",
}
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
_SPOT_SYMBOL_BY_ASSET: Final = {
    "BONK": "BONKUSDT",
    "ENA": "ENAUSDT",
    "WIF": "WIFUSDT",
    "FLOKI": "FLOKIUSDT",
    "ARB": "ARBUSDT",
    "OP": "OPUSDT",
    "SEI": "SEIUSDT",
}
_ASSETS: Final = tuple(_FUTURES_FILE_BY_ASSET)
_RECOMMENDATION_COLUMNS: Final = tuple(field.name for field in fields(RecommendationEvent))
_RECOMMENDATION_ROW_CAP: Final = 2_000_000
_JCS_SAFE_INTEGER_MAX: Final = 2**53 - 1
_PRICE_ROWS: Final = PRICE_STRUCTURE_MOMENTUM_ROW_COUNT_V2
_PARTICIPATION_ROWS: Final = ROBUST_Z_PRIOR_WINDOW_V2 + 1
_CROSS_ROWS: Final = FAMILY_C_PANEL_BAR_COUNT_V2
_OUTPUT_NAMES: Final = frozenset({"consensus.csv", "results.json", "manifest.json"})
_PAYLOAD_NAMES: Final = ("consensus.csv", "results.json")
_SOURCE_ROW_DOMAIN: Final = b"R4B_HISTORICAL_CENSUS_SOURCE_ROW_V2\0"
_ANCHOR_SET_DOMAIN: Final = b"R4B_HISTORICAL_CENSUS_ANCHOR_SET_V2\0"
_DISPOSITION_DOMAIN: Final = b"R4B_HISTORICAL_CENSUS_DISPOSITIONS_V2\0"
_FAMILY_C_SOURCE_DOMAIN: Final = b"R4B_HISTORICAL_CENSUS_FAMILY_C_SOURCE_V2\0"
_CONSENSUS_ROWS_DOMAIN: Final = b"R4B_HISTORICAL_CENSUS_ROWS_V2\0"
_TOPOLOGY_ANALYSIS_DOMAIN: Final = b"R4B_HISTORICAL_CENSUS_TOPOLOGY_ANALYSIS_V2\0"
_TOPOLOGY_ROWS_DOMAIN: Final = b"R4B_HISTORICAL_CENSUS_TOPOLOGY_ROWS_V2\0"
_PRICE_NUMERIC_SLICE_DOMAIN: Final = b"R4B_HISTORICAL_CENSUS_PRICE_NUMERIC_SLICE_V2\0"
_PARTICIPATION_NUMERIC_SLICE_DOMAIN: Final = (
    b"R4B_HISTORICAL_CENSUS_PARTICIPATION_NUMERIC_SLICE_V2\0"
)
_CROSS_NUMERIC_PATH_DOMAIN: Final = b"R4B_HISTORICAL_CENSUS_CROSS_NUMERIC_PATH_V2\0"
_CROSS_NUMERIC_INPUT_DOMAIN: Final = b"R4B_HISTORICAL_CENSUS_CROSS_NUMERIC_INPUT_V2\0"
_EXACT_CROSS_SECONDS_PER_ANCHOR_LOWER_BOUND: Final = Decimal("2.92")
_EXPERIMENT_CONTRACT_RELATIVE_PATH: Final = (
    "docs/r4b-v2-historical-three-family-consensus-experiment.md"
)
_TOPOLOGY_CONTRACT_RELATIVE_PATH: Final = (
    "docs/r4b-v2-historical-three-family-topology-preoutcome-amendment.md"
)


class HistoricalThreeFamilyCensusErrorV2(ValueError):
    """Raised when a census input or artifact violates the frozen contract."""


class HistoricalAnchorDispositionV2(StrEnum):
    """Exhaustive outcome-blind disposition for every authenticated anchor."""

    CONSENSUS_EMITTED = "CONSENSUS_EMITTED"
    DIAGNOSTIC_LIMIT_NOT_EVALUATED = "DIAGNOSTIC_LIMIT_NOT_EVALUATED"


RecommendationReaderV2 = Callable[[Path], bytes]


@dataclass(frozen=True, slots=True)
class HistoricalSourceReplayAuditV2:
    """Authenticated recommendation-only authority for one V1A split."""

    split: str
    replay_dir: Path
    run_manifest_sha256: str
    recommendations_sha256: str
    recommendation_rows: int
    anchor_rows: int
    anchor_set_sha256: str
    futures_input_sha256s: tuple[tuple[str, str], ...]


@dataclass(frozen=True, slots=True)
class LoadedHistoricalRecommendationAnchorsV2:
    """Complete immutable recommendation population before proxy evaluation."""

    anchors: tuple[HistoricalRecommendationAnchorV2, ...]
    replay_audits: tuple[HistoricalSourceReplayAuditV2, ...]
    anchor_set_sha256: str
    historical_only: Literal[True] = True
    fitted_v1a_selection_used: Literal[False] = False
    outcome_data_read: Literal[False] = False


@dataclass(frozen=True, slots=True)
class HistoricalFuturesKlineAuthorityV2:
    """Verified file and manifest authority without retaining dataset rows."""

    asset: str
    symbol: str
    relative_data_path: str
    data_sha256: str
    manifest_sha256: str
    row_count: int
    first_open_time_ms: int
    last_close_time_ms: int


@dataclass(frozen=True, slots=True)
class HistoricalContractAuthorityV2:
    """Caller-bound contract digests, optionally verified against workspace files."""

    experiment_contract_sha256: str
    experiment_contract_relative_path: str
    code_freeze_manifest_sha256: str
    topology_contract_sha256: str
    topology_contract_relative_path: str
    workspace_files_verified: bool


@dataclass(frozen=True, slots=True)
class HistoricalNumericRepresentationProvenanceV2:
    """Cache roots are reproducibility provenance, never calculation authority."""

    rule_version: str
    r3_cache_sha256s: tuple[tuple[str, str], ...]
    target_cache_sha256s: tuple[tuple[str, str], ...]
    cross_cache_sha256s: tuple[tuple[str, str], ...]
    numeric_representation_only: Literal[True] = True
    calculation_authority: Literal[False] = False
    outcome_used: Literal[False] = False


@dataclass(frozen=True, slots=True)
class HistoricalConsensusCensusRowV2:
    """Compact consensus projection; it never retains proxy source windows."""

    split: str
    asset: str
    symbol: str
    source_event_id: str
    source_row_sha256: str
    source_replay_manifest_sha256: str
    anchor_sha256: str
    primary_family: str
    primary_direction: str
    decision_time_ms: int
    price: Decimal
    invalidation: Decimal | None
    atr: Decimal
    event_id: str
    payload_sha256: str
    canonical_consensus_sha256: str
    topology_sha256: str
    canonical_topology_sha256: str
    topology_contract_sha256: str
    topology_rule_version: str
    topology_class: str
    topology_comparison_bucket: str
    topology_display_grade: str
    topology_majority_direction: str | None
    topology_majority_family_count: int | None
    topology_opposing_family_count: int | None
    topology_has_opposition: bool | None
    topology_primary_support_count: int | None
    topology_primary_oppose_count: int | None
    topology_primary_neutral_count: int | None
    clean_primary_audit_eligible: bool
    conflicted_comparator_eligible: bool
    conflicted_comparator_outcome_authorized: bool
    rule_version: str
    status: str
    state_class: str
    directional_numerator_micros: int | None
    directional_denominator: int | None
    directional_agreement_micros: int | None
    bullish_family_count: int | None
    bearish_family_count: int | None
    neutral_family_count: int | None
    primary_relationship: str
    admitted: bool
    price_status: str
    price_direction: int | None
    price_strength_micros: int | None
    price_calculation_sha256: str
    price_source_slice_sha256: str
    participation_status: str
    participation_direction: int | None
    participation_strength_micros: int | None
    participation_calculation_sha256: str
    participation_source_slice_sha256: str
    cross_section_status: str
    cross_section_direction: int | None
    cross_section_strength_micros: int | None
    cross_section_calculation_sha256: str
    cross_section_source_slice_sha256: str
    execution_contract_sha256: str
    zero_move_round_trip_cost_micros: int | None
    atr_fraction_micros: int | None
    one_atr_cost_headroom_micros: int | None
    cross_peer_set_root_sha256: str
    cross_peer_input_sha256: str
    reasons: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class HistoricalAnchorDispositionRowV2:
    """One anchor's exhaustive census disposition."""

    split: str
    asset: str
    primary_direction: str
    decision_time_ms: int
    anchor_sha256: str
    disposition: HistoricalAnchorDispositionV2
    consensus_event_id: str | None
    consensus_payload_sha256: str | None


@dataclass(frozen=True, slots=True)
class HistoricalThreeFamilyCensusArtifactsV2:
    """Published artifact identity returned by the runner boundary."""

    output_dir: Path
    consensus_csv_sha256: str
    results_sha256: str
    manifest_sha256: str
    authenticated_anchors: int
    processed_anchors: int
    census_complete: bool
    diagnostic_mode: bool


@dataclass(frozen=True, slots=True)
class _ParsedRecommendationsV2:
    anchors: tuple[HistoricalRecommendationAnchorV2, ...]
    recommendation_rows: int


@dataclass(frozen=True, slots=True)
class _ReplayManifestV2:
    sha256: str
    recommendations_sha256: str
    futures_input_sha256s: tuple[tuple[str, str], ...]


@dataclass(frozen=True, slots=True)
class _HistoricalCloseIndexV2:
    """Gap-free Decimal close path with an O(1) arithmetic timestamp index."""

    asset: str
    symbol: str
    dataset_sha256: str
    manifest_sha256: str
    first_open_time_ms: int
    closes: tuple[Decimal, ...]

    @property
    def last_open_time_ms(self) -> int:
        return self.first_open_time_ms + (len(self.closes) - 1) * FIVE_MINUTE_MS_V2

    def index_for_open_time(self, open_time_ms: int) -> int:
        difference = open_time_ms - self.first_open_time_ms
        if difference < 0 or difference % FIVE_MINUTE_MS_V2 != 0:
            raise HistoricalThreeFamilyCensusErrorV2(
                f"{self.symbol} has no exact indexed candle at {open_time_ms}"
            )
        index = difference // FIVE_MINUTE_MS_V2
        if not 0 <= index < len(self.closes):
            raise HistoricalThreeFamilyCensusErrorV2(
                f"{self.symbol} indexed candle lies outside the verified dataset"
            )
        return index

    def family_c_slice(
        self,
        *,
        final_open_time_ms: int,
        row_count: int,
    ) -> tuple[FamilyCClosedCandleV2, ...]:
        final_index = self.index_for_open_time(final_open_time_ms)
        first_index = final_index - row_count + 1
        if first_index < 0:
            raise HistoricalThreeFamilyCensusErrorV2(
                f"{self.symbol} lacks {row_count} causal cross-sectional rows"
            )
        rows: list[FamilyCClosedCandleV2] = []
        for index in range(first_index, final_index + 1):
            bar_open_ms = self.first_open_time_ms + index * FIVE_MINUTE_MS_V2
            bar_close_ms = bar_open_ms + FIVE_MINUTE_MS_V2 - 1
            close = self.closes[index]
            source_hash = _family_c_source_evidence_sha256(
                dataset_sha256=self.dataset_sha256,
                manifest_sha256=self.manifest_sha256,
                symbol=self.symbol,
                bar_open_ms=bar_open_ms,
                bar_close_ms=bar_close_ms,
                close=close,
            )
            rows.append(
                FamilyCClosedCandleV2(
                    symbol=self.symbol,
                    bar_open_ms=bar_open_ms,
                    bar_close_ms=bar_close_ms,
                    event_time_ms=bar_close_ms,
                    receipt_time_ms=bar_close_ms,
                    close=close,
                    source_evidence_sha256=source_hash,
                )
            )
        return tuple(rows)

    def compact_window(
        self,
        *,
        final_open_time_ms: int,
        row_count: int,
    ) -> _HistoricalPeerWindowV2:
        final_index = self.index_for_open_time(final_open_time_ms)
        first_index = final_index - row_count + 1
        if first_index < 0:
            raise HistoricalThreeFamilyCensusErrorV2(
                f"{self.symbol} lacks {row_count} causal compact peer rows"
            )
        first_open_ms = self.first_open_time_ms + first_index * FIVE_MINUTE_MS_V2
        closes = self.closes[first_index : final_index + 1]
        if len(closes) != row_count:
            raise HistoricalThreeFamilyCensusErrorV2(
                f"{self.symbol} compact peer window is incomplete"
            )
        path_sha256 = _compact_cross_path_sha256(
            dataset_sha256=self.dataset_sha256,
            manifest_sha256=self.manifest_sha256,
            symbol=self.symbol,
            first_open_ms=first_open_ms,
            final_open_ms=final_open_time_ms,
            row_count=row_count,
        )
        return _HistoricalPeerWindowV2(
            symbol=self.symbol,
            dataset_sha256=self.dataset_sha256,
            manifest_sha256=self.manifest_sha256,
            first_open_ms=first_open_ms,
            final_open_ms=final_open_time_ms,
            closes=closes,
            path_sha256=path_sha256,
        )


@dataclass(frozen=True, slots=True)
class _HistoricalPeerWindowV2:
    """Compact dataset-root-bound peer window for numeric calculation."""

    symbol: str
    dataset_sha256: str
    manifest_sha256: str
    first_open_ms: int
    final_open_ms: int
    closes: tuple[Decimal, ...]
    path_sha256: str

    @property
    def final_close_ms(self) -> int:
        return self.final_open_ms + FIVE_MINUTE_MS_V2 - 1

    @property
    def event_time_ms(self) -> int:
        return self.final_close_ms

    @property
    def receipt_time_ms(self) -> int:
        return self.final_close_ms


@dataclass(frozen=True, slots=True)
class _CompactPriceCalculationV2:
    calculation: PriceClosePathCalculationV2
    source_slice_sha256: str


@dataclass(frozen=True, slots=True)
class _CompactParticipationCalculationV2:
    calculation: ParticipationFlowCalculationV2
    source_slice_sha256: str


@dataclass(frozen=True, slots=True)
class _CompactCrossCalculationV2:
    calculation: HistoricalCrossSectional7AssetCalculationV2
    peer_path_sha256s: tuple[tuple[str, str], ...]
    peer_input_sha256: str


@dataclass(frozen=True, slots=True)
class _TargetCandleIndexV2:
    asset: str
    symbol: str
    dataset_sha256: str
    first_open_time_ms: int
    candles: tuple[Candle, ...]

    def slice_ending(
        self,
        *,
        final_open_time_ms: int,
        row_count: int,
    ) -> tuple[Candle, ...]:
        difference = final_open_time_ms - self.first_open_time_ms
        if difference < 0 or difference % FIVE_MINUTE_MS_V2 != 0:
            raise HistoricalThreeFamilyCensusErrorV2(
                f"{self.symbol} has no exact target candle at {final_open_time_ms}"
            )
        final_index = difference // FIVE_MINUTE_MS_V2
        first_index = final_index - row_count + 1
        if first_index < 0 or final_index >= len(self.candles):
            raise HistoricalThreeFamilyCensusErrorV2(
                f"{self.symbol} lacks {row_count} causal target rows"
            )
        selected = self.candles[first_index : final_index + 1]
        if len(selected) != row_count or selected[-1].open_time_ms != final_open_time_ms:
            raise HistoricalThreeFamilyCensusErrorV2(
                f"{self.symbol} target slice is not exact"
            )
        return selected


@dataclass(frozen=True, slots=True)
class _WindowContractV2:
    price_rows: int
    participation_rows: int
    cross_rows: int


@dataclass(frozen=True, slots=True)
class _CensusBuildersV2[PriceT, ParticipationT, CrossT]:
    price: Callable[[str, str, int, tuple[Candle, ...]], PriceT]
    participation: Callable[[str, str, int, tuple[Candle, ...]], ParticipationT]
    cross_section: Callable[
        [str, int, tuple[_HistoricalPeerWindowV2, ...]],
        CrossT,
    ]
    compact_consensus: Callable[
        [
            HistoricalRecommendationAnchorV2,
            PriceT,
            ParticipationT,
            CrossT,
            HistoricalExecutionContractV2,
            str,
            str,
        ],
        HistoricalConsensusCensusRowV2,
    ]


_EXACT_WINDOWS: Final = _WindowContractV2(
    price_rows=_PRICE_ROWS,
    participation_rows=_PARTICIPATION_ROWS,
    cross_rows=_CROSS_ROWS,
)


def load_historical_recommendation_anchors_v2(
    *,
    replay_dirs: Mapping[str, str | Path],
    recommendation_reader: RecommendationReaderV2 | None = None,
) -> LoadedHistoricalRecommendationAnchorsV2:
    """Authenticate exactly three recommendation ledgers without outcome access."""

    if type(replay_dirs) is not dict or set(replay_dirs) != set(
        HISTORICAL_THREE_FAMILY_CENSUS_SPLITS_V2
    ):
        raise HistoricalThreeFamilyCensusErrorV2(
            "replay_dirs must be an exact dict containing the three frozen splits"
        )
    reader = recommendation_reader or _read_recommendations_bytes_v2
    if not callable(reader):
        raise HistoricalThreeFamilyCensusErrorV2(
            "recommendation_reader must be callable"
        )
    resolved = {
        split: Path(replay_dirs[split]).resolve()
        for split in HISTORICAL_THREE_FAMILY_CENSUS_SPLITS_V2
    }
    if len(set(resolved.values())) != len(resolved):
        raise HistoricalThreeFamilyCensusErrorV2(
            "the three replay directories must be distinct"
        )

    anchors: list[HistoricalRecommendationAnchorV2] = []
    audits: list[HistoricalSourceReplayAuditV2] = []
    for split in HISTORICAL_THREE_FAMILY_CENSUS_SPLITS_V2:
        root = resolved[split]
        replay_manifest = _load_replay_manifest_v2(root, expected_split=split)
        recommendation_path = root / "recommendations.csv"
        try:
            raw = reader(recommendation_path)
        except OSError as exc:
            raise HistoricalThreeFamilyCensusErrorV2(
                f"cannot read authenticated recommendations for {split}"
            ) from exc
        if type(raw) is not bytes:
            raise HistoricalThreeFamilyCensusErrorV2(
                "recommendation_reader must return exact bytes"
            )
        actual_recommendations_sha256 = _sha256_bytes(raw)
        if (
            actual_recommendations_sha256 != replay_manifest.recommendations_sha256
            or actual_recommendations_sha256
            != _RECOMMENDATIONS_SHA256_BY_SPLIT[split]
        ):
            raise HistoricalThreeFamilyCensusErrorV2(
                f"authenticated recommendations hash mismatch for {split}"
            )
        parsed = _parse_recommendations_v2(
            raw,
            expected_split=split,
            source_replay_manifest_sha256=replay_manifest.sha256,
        )
        expected_count = HISTORICAL_THREE_FAMILY_EXPECTED_ANCHORS_BY_SPLIT_V2[split]
        if len(parsed.anchors) != expected_count:
            raise HistoricalThreeFamilyCensusErrorV2(
                f"{split} requires exactly {expected_count} Futures pullback anchors"
            )
        split_anchor_hash = _anchor_set_sha256(parsed.anchors)
        anchors.extend(parsed.anchors)
        audits.append(
            HistoricalSourceReplayAuditV2(
                split=split,
                replay_dir=root,
                run_manifest_sha256=replay_manifest.sha256,
                recommendations_sha256=actual_recommendations_sha256,
                recommendation_rows=parsed.recommendation_rows,
                anchor_rows=len(parsed.anchors),
                anchor_set_sha256=split_anchor_hash,
                futures_input_sha256s=replay_manifest.futures_input_sha256s,
            )
        )

    ordered = tuple(sorted(anchors, key=_anchor_sort_key))
    if len(ordered) != HISTORICAL_THREE_FAMILY_EXPECTED_ANCHORS_V2:
        raise HistoricalThreeFamilyCensusErrorV2(
            "authenticated anchor census does not have exactly 6,341 rows"
        )
    _require_unique_anchor_identities(ordered)
    return LoadedHistoricalRecommendationAnchorsV2(
        anchors=ordered,
        replay_audits=tuple(audits),
        anchor_set_sha256=_anchor_set_sha256(ordered),
    )


def run_historical_three_family_census_v2(
    *,
    replay_dirs: Mapping[str, str | Path],
    data_dir: str | Path,
    output_dir: str | Path,
    experiment_contract_sha256: str,
    topology_contract_sha256: str,
    code_freeze_manifest_sha256: str,
    workspace_root: str | Path | None = None,
    maximum_anchors: int | None = None,
    recommendation_reader: RecommendationReaderV2 | None = None,
) -> HistoricalThreeFamilyCensusArtifactsV2:
    """Run and atomically publish the exact outcome-blind historical census."""

    _validate_sha256(experiment_contract_sha256, "experiment_contract_sha256")
    _validate_sha256(topology_contract_sha256, "topology_contract_sha256")
    _validate_sha256(code_freeze_manifest_sha256, "code_freeze_manifest_sha256")
    _validate_maximum_anchors(maximum_anchors)
    contract_authority = _contract_authority_v2(
        experiment_contract_sha256=experiment_contract_sha256,
        topology_contract_sha256=topology_contract_sha256,
        code_freeze_manifest_sha256=code_freeze_manifest_sha256,
        workspace_root=workspace_root,
    )
    loaded = load_historical_recommendation_anchors_v2(
        replay_dirs=replay_dirs,
        recommendation_reader=recommendation_reader,
    )
    data_root = Path(data_dir).resolve()
    authorities, close_indexes, r3_caches = _load_futures_kline_sources_v2(
        data_root=data_root,
        replay_audits=loaded.replay_audits,
    )
    authority_by_asset = {value.asset: value for value in authorities}

    r3_by_asset = {value.asset: cache for value, cache in zip(authorities, r3_caches, strict=True)}

    def target_loader(asset: str) -> HistoricalTargetNumericCacheV2:
        authority = authority_by_asset[asset]
        dataset, reread_authority = _load_one_verified_dataset_v2(
            data_root=data_root,
            asset=asset,
            replay_expected_sha256=authority.data_sha256,
        )
        if reread_authority != authority:
            raise HistoricalThreeFamilyCensusErrorV2(
                f"{asset} dataset authority changed between verification and evaluation"
            )
        try:
            return build_historical_target_numeric_cache_v2(
                source_r3_cache=r3_by_asset[asset],
                rows=dataset.candles,
            )
        except HistoricalNumericPrecomputeContractErrorV2 as exc:
            raise HistoricalThreeFamilyCensusErrorV2(
                f"{asset} target numeric precompute failed"
            ) from exc

    execution_contract = build_historical_execution_contract_v2()
    rows, dispositions, numeric_provenance = _evaluate_precomputed_anchors_v2(
        anchors=loaded.anchors,
        close_indexes={value.asset: value for value in close_indexes},
        r3_caches=r3_by_asset,
        target_loader=target_loader,
        execution_contract=execution_contract,
        experiment_contract_sha256=experiment_contract_sha256,
        topology_contract_sha256=topology_contract_sha256,
        maximum_anchors=maximum_anchors,
    )
    consensus_csv = _consensus_csv_bytes_v2(rows)
    results = _census_results_document_v2(
        loaded=loaded,
        authorities=authorities,
        rows=rows,
        dispositions=dispositions,
        execution_contract=execution_contract,
        contract_authority=contract_authority,
        numeric_provenance=numeric_provenance,
        maximum_anchors=maximum_anchors,
        consensus_csv_sha256=_sha256_bytes(consensus_csv),
    )
    results_raw = canonical_json_line(results)
    target = _fresh_output_target_v2(output_dir)
    manifest = _artifact_manifest_document_v2(
        loaded=loaded,
        authorities=authorities,
        contract_authority=contract_authority,
        numeric_provenance=numeric_provenance,
        execution_contract=execution_contract,
        maximum_anchors=maximum_anchors,
        payload_sha256={
            "consensus.csv": _sha256_bytes(consensus_csv),
            "results.json": _sha256_bytes(results_raw),
        },
    )
    manifest_raw = canonical_json_line(manifest)
    _publish_artifacts_v2(
        target=target,
        payloads={
            "consensus.csv": consensus_csv,
            "results.json": results_raw,
            "manifest.json": manifest_raw,
        },
    )
    return HistoricalThreeFamilyCensusArtifactsV2(
        output_dir=target,
        consensus_csv_sha256=_sha256_bytes(consensus_csv),
        results_sha256=_sha256_bytes(results_raw),
        manifest_sha256=_sha256_bytes(manifest_raw),
        authenticated_anchors=len(loaded.anchors),
        processed_anchors=len(rows),
        census_complete=maximum_anchors is None,
        diagnostic_mode=maximum_anchors is not None,
    )


def _parse_recommendations_v2(
    raw: bytes,
    *,
    expected_split: str,
    source_replay_manifest_sha256: str,
) -> _ParsedRecommendationsV2:
    if expected_split not in HISTORICAL_THREE_FAMILY_CENSUS_SPLITS_V2:
        raise HistoricalThreeFamilyCensusErrorV2("unknown frozen split")
    _validate_sha256(source_replay_manifest_sha256, "source replay manifest hash")
    if not raw or b"\r" in raw or not raw.endswith(b"\n") or b"\x00" in raw:
        raise HistoricalThreeFamilyCensusErrorV2(
            "recommendations must be nonempty LF-only UTF-8 CSV"
        )
    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise HistoricalThreeFamilyCensusErrorV2(
            "recommendations are not valid UTF-8"
        ) from exc
    reader = csv.DictReader(io.StringIO(text, newline=""), strict=True)
    if tuple(reader.fieldnames or ()) != _RECOMMENDATION_COLUMNS:
        raise HistoricalThreeFamilyCensusErrorV2(
            "recommendation header must exactly match the producer dataclass order"
        )
    if len(_RECOMMENDATION_COLUMNS) != len(set(_RECOMMENDATION_COLUMNS)):
        raise HistoricalThreeFamilyCensusErrorV2(
            "recommendation producer schema contains duplicate columns"
        )

    event_ids: set[str] = set()
    anchor_identities: set[tuple[str, str, int]] = set()
    anchors: list[HistoricalRecommendationAnchorV2] = []
    start_ms, end_ms = _SPLIT_RANGES_MS[expected_split]
    try:
        for line_number, row in enumerate(reader, start=2):
            if None in row or any(value is None for value in row.values()):
                raise HistoricalThreeFamilyCensusErrorV2(
                    f"recommendations line {line_number} has missing or surplus cells"
                )
            if len(event_ids) >= _RECOMMENDATION_ROW_CAP:
                raise HistoricalThreeFamilyCensusErrorV2(
                    "recommendation row cap exceeded"
                )
            exact_row = cast(dict[str, str], row)
            event_id = _require_source_event_id(
                exact_row["event_id"], f"recommendations line {line_number} event_id"
            )
            if event_id in event_ids:
                raise HistoricalThreeFamilyCensusErrorV2(
                    f"duplicate recommendation event_id: {event_id}"
                )
            event_ids.add(event_id)
            if (
                exact_row["protocol_version"]
                != HISTORICAL_THREE_FAMILY_SOURCE_PROTOCOL_VERSION_V2
            ):
                raise HistoricalThreeFamilyCensusErrorV2(
                    f"recommendations line {line_number} protocol drift"
                )
            if (
                exact_row["rule_version"]
                != HISTORICAL_THREE_FAMILY_SOURCE_RULE_VERSION_V2
            ):
                raise HistoricalThreeFamilyCensusErrorV2(
                    f"recommendations line {line_number} rule drift"
                )
            if exact_row["split"] != expected_split:
                raise HistoricalThreeFamilyCensusErrorV2(
                    f"recommendations line {line_number} split drift"
                )
            asset = exact_row["asset"]
            if asset not in _ASSETS or exact_row["cohort"] != "volatile":
                raise HistoricalThreeFamilyCensusErrorV2(
                    f"recommendations line {line_number} asset/cohort drift"
                )
            market_text = exact_row["market"]
            if market_text not in {Market.SPOT.value, Market.FUTURES.value}:
                raise HistoricalThreeFamilyCensusErrorV2(
                    f"recommendations line {line_number} market drift"
                )
            expected_symbol = (
                _FUTURES_SYMBOL_BY_ASSET[asset]
                if market_text == Market.FUTURES.value
                else _SPOT_SYMBOL_BY_ASSET[asset]
            )
            if exact_row["symbol"] != expected_symbol:
                raise HistoricalThreeFamilyCensusErrorV2(
                    f"recommendations line {line_number} symbol drift"
                )
            decision_time_ms = _parse_unsigned_int(
                exact_row["decision_time_ms"],
                f"recommendations line {line_number} decision_time_ms",
            )
            if (
                not start_ms <= decision_time_ms < end_ms
                or decision_time_ms % FIVE_MINUTE_MS_V2
                != FIVE_MINUTE_MS_V2 - 1
            ):
                raise HistoricalThreeFamilyCensusErrorV2(
                    f"recommendations line {line_number} decision time is outside the split/grid"
                )
            information_only = _parse_exact_bool(
                exact_row["information_only"],
                f"recommendations line {line_number} information_only",
            )
            _parse_exact_bool(
                exact_row["recovery_confirmed"],
                f"recommendations line {line_number} recovery_confirmed",
            )
            _parse_exact_bool(
                exact_row["structure_intact"],
                f"recommendations line {line_number} structure_intact",
            )
            price = _parse_decimal(
                exact_row["price"], f"recommendations line {line_number} price"
            )
            atr = _parse_decimal(
                exact_row["atr"], f"recommendations line {line_number} atr"
            )
            score = _parse_decimal(
                exact_row["score"], f"recommendations line {line_number} score"
            )
            if price <= 0 or atr < 0:
                raise HistoricalThreeFamilyCensusErrorV2(
                    f"recommendations line {line_number} price/ATR is invalid"
                )
            invalidation = _parse_optional_decimal(
                exact_row["invalidation"],
                f"recommendations line {line_number} invalidation",
            )
            family_text = exact_row["family"]
            direction_text = exact_row["direction"]
            if direction_text not in {Direction.LONG.value, Direction.SHORT.value}:
                raise HistoricalThreeFamilyCensusErrorV2(
                    f"recommendations line {line_number} direction drift"
                )
            is_anchor = (
                market_text == Market.FUTURES.value
                and information_only
                and exact_row["stage"] == "setup"
                and family_text
                in {SignalFamily.PULLBACK_LONG.value, SignalFamily.PULLBACK_SHORT.value}
                and score == Decimal(100)
            )
            if not is_anchor:
                continue
            primary_family = SignalFamily(family_text)
            primary_direction = Direction(direction_text)
            expected_direction = (
                Direction.LONG
                if primary_family is SignalFamily.PULLBACK_LONG
                else Direction.SHORT
            )
            if primary_direction is not expected_direction:
                raise HistoricalThreeFamilyCensusErrorV2(
                    f"recommendations line {line_number} family/direction conflict"
                )
            if invalidation is None or invalidation <= 0:
                raise HistoricalThreeFamilyCensusErrorV2(
                    f"recommendations line {line_number} lacks a positive invalidation"
                )
            identity = (asset, primary_direction.value, decision_time_ms)
            if identity in anchor_identities:
                raise HistoricalThreeFamilyCensusErrorV2(
                    "duplicate (asset, direction, decision_time_ms) anchor"
                )
            anchor_identities.add(identity)
            anchors.append(
                HistoricalRecommendationAnchorV2(
                    source_event_id=event_id,
                    source_row_sha256=_source_row_sha256(exact_row),
                    source_replay_manifest_sha256=source_replay_manifest_sha256,
                    split=expected_split,
                    asset=asset,
                    cohort="volatile",
                    symbol=expected_symbol,
                    primary_family=primary_family,
                    primary_direction=primary_direction,
                    decision_time_ms=decision_time_ms,
                    price=price,
                    invalidation=invalidation,
                    atr=atr,
                    source_rule_version=exact_row["rule_version"],
                    source_protocol_version=exact_row["protocol_version"],
                )
            )
    except csv.Error as exc:
        raise HistoricalThreeFamilyCensusErrorV2(
            "recommendations contain malformed CSV"
        ) from exc
    return _ParsedRecommendationsV2(
        anchors=tuple(sorted(anchors, key=_anchor_sort_key)),
        recommendation_rows=len(event_ids),
    )


def _load_replay_manifest_v2(root: Path, *, expected_split: str) -> _ReplayManifestV2:
    path = root / "run_manifest.json"
    raw = _read_required_bytes_v2(path, label=f"{expected_split} run manifest")
    actual_hash = _sha256_bytes(raw)
    if actual_hash != _RUN_MANIFEST_SHA256_BY_SPLIT[expected_split]:
        raise HistoricalThreeFamilyCensusErrorV2(
            f"{expected_split} is not the latest authenticated Amendment-1 replay manifest"
        )
    document = _decode_json_object_v2(raw, label=f"{expected_split} run manifest")
    if document.get("protocol_version") != HISTORICAL_THREE_FAMILY_SOURCE_PROTOCOL_VERSION_V2:
        raise HistoricalThreeFamilyCensusErrorV2(
            f"{expected_split} replay protocol drift"
        )
    if document.get("rule_version") != HISTORICAL_THREE_FAMILY_SOURCE_RULE_VERSION_V2:
        raise HistoricalThreeFamilyCensusErrorV2(f"{expected_split} replay rule drift")
    outputs = _require_json_dict(document.get("outputs"), "replay outputs")
    recommendation_hash = _require_sha256(
        outputs.get("recommendations.csv"), "replay recommendations output hash"
    )
    if recommendation_hash != _RECOMMENDATIONS_SHA256_BY_SPLIT[expected_split]:
        raise HistoricalThreeFamilyCensusErrorV2(
            f"{expected_split} replay recommendation authority drift"
        )
    inputs = _require_json_dict(document.get("inputs"), "replay inputs")
    futures_inputs: list[tuple[str, str]] = []
    for asset in _ASSETS:
        relative = f"futures/{_FUTURES_FILE_BY_ASSET[asset]}"
        futures_inputs.append(
            (relative, _require_sha256(inputs.get(relative), f"replay input {relative}"))
        )
    return _ReplayManifestV2(
        sha256=actual_hash,
        recommendations_sha256=recommendation_hash,
        futures_input_sha256s=tuple(futures_inputs),
    )


def _load_futures_kline_sources_v2(
    *,
    data_root: Path,
    replay_audits: tuple[HistoricalSourceReplayAuditV2, ...],
) -> tuple[
    tuple[HistoricalFuturesKlineAuthorityV2, ...],
    tuple[_HistoricalCloseIndexV2, ...],
    tuple[HistoricalR3SeriesCacheV2, ...],
]:
    if not data_root.is_dir():
        raise HistoricalThreeFamilyCensusErrorV2(
            "historical data_dir must be an existing directory"
        )
    replay_maps = tuple(dict(audit.futures_input_sha256s) for audit in replay_audits)
    if not replay_maps or any(value != replay_maps[0] for value in replay_maps[1:]):
        raise HistoricalThreeFamilyCensusErrorV2(
            "the three replay manifests disagree on Futures kline authority"
        )
    authorities: list[HistoricalFuturesKlineAuthorityV2] = []
    indexes: list[_HistoricalCloseIndexV2] = []
    r3_caches: list[HistoricalR3SeriesCacheV2] = []
    for asset in _ASSETS:
        relative = f"futures/{_FUTURES_FILE_BY_ASSET[asset]}"
        dataset, authority = _load_one_verified_dataset_v2(
            data_root=data_root,
            asset=asset,
            replay_expected_sha256=replay_maps[0][relative],
        )
        authorities.append(authority)
        indexes.append(
            _HistoricalCloseIndexV2(
                asset=asset,
                symbol=authority.symbol,
                dataset_sha256=authority.data_sha256,
                manifest_sha256=authority.manifest_sha256,
                first_open_time_ms=dataset.candles[0].open_time_ms,
                closes=tuple(candle.close for candle in dataset.candles),
            )
        )
        try:
            r3_caches.append(
                build_historical_r3_series_cache_v2(
                    dataset_sha256=authority.data_sha256,
                    manifest_sha256=authority.manifest_sha256,
                    rows=dataset.candles,
                )
            )
        except HistoricalNumericPrecomputeContractErrorV2 as exc:
            raise HistoricalThreeFamilyCensusErrorV2(
                f"{asset} R3 numeric precompute failed"
            ) from exc
        del dataset
    if tuple(value.symbol for value in indexes) != HISTORICAL_THREE_FAMILY_PANEL_SYMBOLS_V2:
        raise HistoricalThreeFamilyCensusErrorV2(
            "verified Futures datasets differ from the frozen seven-symbol panel"
        )
    if tuple(value.symbol for value in r3_caches) != (
        HISTORICAL_THREE_FAMILY_PANEL_SYMBOLS_V2
    ):
        raise HistoricalThreeFamilyCensusErrorV2(
            "R3 caches differ from the frozen seven-symbol panel"
        )
    return tuple(authorities), tuple(indexes), tuple(r3_caches)


def _load_one_verified_dataset_v2(
    *,
    data_root: Path,
    asset: str,
    replay_expected_sha256: str,
) -> tuple[KlineDataset, HistoricalFuturesKlineAuthorityV2]:
    if asset not in _ASSETS:
        raise HistoricalThreeFamilyCensusErrorV2("unknown frozen Futures asset")
    filename = _FUTURES_FILE_BY_ASSET[asset]
    data_path = data_root / "futures" / filename
    manifest_path = data_path.with_name(f"{data_path.name}.manifest.json")
    try:
        manifest_raw = _read_required_bytes_v2(
            manifest_path, label=f"{asset} dataset manifest"
        )
        manifest = read_dataset_manifest(manifest_path)
        canonical_manifest = (
            json.dumps(
                asdict(manifest),
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
                allow_nan=False,
            ).encode("utf-8")
            + b"\n"
        )
        if manifest_raw != canonical_manifest:
            raise HistoricalThreeFamilyCensusErrorV2(
                f"{asset} dataset manifest is not canonical JSON"
            )
        actual_data_sha256 = sha256_file(data_path)
        if (
            actual_data_sha256 != replay_expected_sha256
            or actual_data_sha256 != manifest.sha256
        ):
            raise HistoricalThreeFamilyCensusErrorV2(
                f"{asset} dataset hash differs from manifest/replay authority"
            )
        dataset = read_kline_csv(data_path)
    except DatasetValidationError as exc:
        raise HistoricalThreeFamilyCensusErrorV2(
            f"{asset} dataset failed strict validation"
        ) from exc
    _validate_dataset_manifest_match_v2(
        dataset=dataset,
        manifest=manifest,
        asset=asset,
        filename=filename,
    )
    return dataset, HistoricalFuturesKlineAuthorityV2(
        asset=asset,
        symbol=_FUTURES_SYMBOL_BY_ASSET[asset],
        relative_data_path=f"futures/{filename}",
        data_sha256=actual_data_sha256,
        manifest_sha256=_sha256_bytes(manifest_raw),
        row_count=len(dataset.candles),
        first_open_time_ms=dataset.candles[0].open_time_ms,
        last_close_time_ms=dataset.candles[-1].close_time_ms,
    )


def _validate_dataset_manifest_match_v2(
    *,
    dataset: KlineDataset,
    manifest: DatasetManifest,
    asset: str,
    filename: str,
) -> None:
    request = dataset.request
    gaps = find_kline_gaps(dataset)
    exact = (
        manifest.schema_version == 2
        and manifest.data_file == filename
        and manifest.market == Market.FUTURES.value
        and manifest.symbol == _FUTURES_SYMBOL_BY_ASSET[asset]
        and manifest.alias == asset
        and manifest.interval == "5m"
        and manifest.request_start_time_ms == request.start_time_ms
        and manifest.request_end_time_ms == request.end_time_ms
        and manifest.row_count == len(dataset.candles)
        and manifest.first_open_time_ms == dataset.candles[0].open_time_ms
        and manifest.last_close_time_ms == dataset.candles[-1].close_time_ms
        and manifest.gap_count == 0
        and manifest.missing_intervals == 0
        and not gaps
        and request.market is Market.FUTURES
        and request.symbol == _FUTURES_SYMBOL_BY_ASSET[asset]
        and request.dataset_alias == asset
        and request.interval == "5m"
    )
    if not exact:
        raise HistoricalThreeFamilyCensusErrorV2(
            f"{asset} dataset and manifest identity/continuity differ"
        )


def _target_index_from_dataset_v2(
    dataset: KlineDataset,
    *,
    asset: str,
    dataset_sha256: str,
) -> _TargetCandleIndexV2:
    _validate_sha256(dataset_sha256, "target dataset_sha256")
    expected_symbol = _FUTURES_SYMBOL_BY_ASSET[asset]
    if (
        not dataset.candles
        or dataset.request.market is not Market.FUTURES
        or dataset.request.symbol != expected_symbol
        or dataset.request.interval != "5m"
    ):
        raise HistoricalThreeFamilyCensusErrorV2(
            "target dataset identity differs from the frozen Futures panel"
        )
    return _TargetCandleIndexV2(
        asset=asset,
        symbol=expected_symbol,
        dataset_sha256=dataset_sha256,
        first_open_time_ms=dataset.candles[0].open_time_ms,
        candles=dataset.candles,
    )


def _evaluate_precomputed_anchors_v2(
    *,
    anchors: tuple[HistoricalRecommendationAnchorV2, ...],
    close_indexes: Mapping[str, _HistoricalCloseIndexV2],
    r3_caches: Mapping[str, HistoricalR3SeriesCacheV2],
    target_loader: Callable[[str], HistoricalTargetNumericCacheV2],
    execution_contract: HistoricalExecutionContractV2,
    experiment_contract_sha256: str,
    topology_contract_sha256: str,
    maximum_anchors: int | None,
) -> tuple[
    tuple[HistoricalConsensusCensusRowV2, ...],
    tuple[HistoricalAnchorDispositionRowV2, ...],
    HistoricalNumericRepresentationProvenanceV2,
]:
    """Evaluate anchors from bounded dataset-level numeric caches."""

    _validate_maximum_anchors(maximum_anchors)
    _validate_sha256(experiment_contract_sha256, "experiment_contract_sha256")
    _validate_sha256(topology_contract_sha256, "topology_contract_sha256")
    if set(close_indexes) != set(_ASSETS) or set(r3_caches) != set(_ASSETS):
        raise HistoricalThreeFamilyCensusErrorV2(
            "precomputed evaluation requires the exact seven-asset authorities"
        )
    for asset in _ASSETS:
        close_index = close_indexes[asset]
        cache = r3_caches[asset]
        if (
            close_index.asset != asset
            or close_index.symbol != cache.symbol
            or close_index.dataset_sha256 != cache.dataset_sha256
            or close_index.manifest_sha256 != cache.manifest_sha256
        ):
            raise HistoricalThreeFamilyCensusErrorV2(
                f"{asset} R3 cache differs from its verified close authority"
            )

    ordered_anchors = tuple(sorted(anchors, key=_anchor_sort_key))
    _require_unique_anchor_identities(ordered_anchors)
    process_count = (
        len(ordered_anchors)
        if maximum_anchors is None
        else min(maximum_anchors, len(ordered_anchors))
    )
    selected = ordered_anchors[:process_count]
    selected_hashes = {anchor.anchor_sha256 for anchor in selected}
    if len(selected_hashes) != len(selected):
        raise HistoricalThreeFamilyCensusErrorV2(
            "selected diagnostic anchor hashes are not unique"
        )
    by_asset: dict[str, list[HistoricalRecommendationAnchorV2]] = defaultdict(list)
    for anchor in selected:
        by_asset[anchor.asset].append(anchor)

    rows_by_anchor: dict[str, HistoricalConsensusCensusRowV2] = {}
    target_cache_roots: list[tuple[str, str]] = []
    cross_cache_roots: list[tuple[str, str]] = []
    for asset in _ASSETS:
        asset_anchors = by_asset.get(asset, [])
        if not asset_anchors:
            continue
        target_cache = target_loader(asset)
        source_r3 = r3_caches[asset]
        if (
            target_cache.symbol != _FUTURES_SYMBOL_BY_ASSET[asset]
            or target_cache.source_r3_cache is not source_r3
            or target_cache.dataset_sha256 != close_indexes[asset].dataset_sha256
        ):
            raise HistoricalThreeFamilyCensusErrorV2(
                f"{asset} target numeric cache returned incompatible authority"
            )
        peer_caches = tuple(r3_caches[value] for value in _ASSETS if value != asset)
        try:
            cross_cache = build_historical_target_excluded_median_r3_cache_v2(
                target_symbol=target_cache.symbol,
                peer_caches=peer_caches,
            )
        except HistoricalNumericPrecomputeContractErrorV2 as exc:
            raise HistoricalThreeFamilyCensusErrorV2(
                f"{asset} target-excluded median R3 precompute failed"
            ) from exc
        target_cache_roots.append((asset, target_cache.cache_sha256))
        cross_cache_roots.append((asset, cross_cache.cache_sha256))

        for anchor in sorted(asset_anchors, key=_anchor_sort_key):
            close_index = close_indexes[asset]
            close = close_index.closes[
                close_index.index_for_open_time(anchor.bar_open_ms)
            ]
            if close != anchor.price:
                raise HistoricalThreeFamilyCensusErrorV2(
                    f"anchor {anchor.anchor_sha256} price differs from its closed decision candle"
                )
            try:
                target_inputs = target_cache.anchor_inputs_at(anchor.bar_open_ms)
                target_calculations = calculate_historical_target_anchor_v2(
                    target_inputs
                )
                cross_inputs = cross_cache.anchor_inputs_at(anchor.bar_open_ms)
                cross_calculation = calculate_historical_cross_anchor_v2(cross_inputs)
                price = _CompactPriceCalculationV2(
                    calculation=target_calculations.price,
                    source_slice_sha256=target_inputs.price_source_slice_sha256,
                )
                participation = _CompactParticipationCalculationV2(
                    calculation=target_calculations.participation,
                    source_slice_sha256=(
                        target_inputs.participation_source_slice_sha256
                    ),
                )
                cross_section = _CompactCrossCalculationV2(
                    calculation=cross_calculation.calculation,
                    peer_path_sha256s=cross_inputs.peer_path_sha256s,
                    peer_input_sha256=cross_inputs.peer_input_sha256,
                )
                row = _build_compact_consensus_v2(
                    anchor,
                    price,
                    participation,
                    cross_section,
                    execution_contract,
                    experiment_contract_sha256,
                    topology_contract_sha256,
                )
            except HistoricalThreeFamilyCensusErrorV2:
                raise
            except Exception as exc:
                raise HistoricalThreeFamilyCensusErrorV2(
                    f"cached calculation/consensus failed for anchor {anchor.anchor_sha256}"
                ) from exc
            if row.anchor_sha256 != anchor.anchor_sha256:
                raise HistoricalThreeFamilyCensusErrorV2(
                    "compact consensus row changed its source anchor identity"
                )
            if anchor.anchor_sha256 in rows_by_anchor:
                raise HistoricalThreeFamilyCensusErrorV2(
                    "one anchor produced more than one compact consensus row"
                )
            rows_by_anchor[anchor.anchor_sha256] = row
            del target_inputs, target_calculations, cross_inputs, cross_calculation
            del price, participation, cross_section, row
        del target_cache, cross_cache

    rows, dispositions = _finalize_evaluation_v2(
        ordered_anchors=ordered_anchors,
        selected_hashes=selected_hashes,
        rows_by_anchor=rows_by_anchor,
        process_count=process_count,
    )
    provenance = HistoricalNumericRepresentationProvenanceV2(
        rule_version=HISTORICAL_NUMERIC_PRECOMPUTE_RULE_VERSION_V2,
        r3_cache_sha256s=tuple(
            (asset, r3_caches[asset].cache_sha256) for asset in _ASSETS
        ),
        target_cache_sha256s=tuple(target_cache_roots),
        cross_cache_sha256s=tuple(cross_cache_roots),
    )
    _validate_numeric_provenance_v2(provenance, processed_assets=set(by_asset))
    return rows, dispositions, provenance


def _evaluate_anchors_v2[PriceT, ParticipationT, CrossT](
    *,
    anchors: tuple[HistoricalRecommendationAnchorV2, ...],
    close_indexes: Mapping[str, _HistoricalCloseIndexV2],
    target_loader: Callable[[str], _TargetCandleIndexV2],
    execution_contract: HistoricalExecutionContractV2,
    experiment_contract_sha256: str,
    topology_contract_sha256: str,
    maximum_anchors: int | None,
    windows: _WindowContractV2,
    builders: _CensusBuildersV2[PriceT, ParticipationT, CrossT],
) -> tuple[
    tuple[HistoricalConsensusCensusRowV2, ...],
    tuple[HistoricalAnchorDispositionRowV2, ...],
]:
    _validate_maximum_anchors(maximum_anchors)
    if set(close_indexes) != set(_ASSETS):
        raise HistoricalThreeFamilyCensusErrorV2(
            "cross-sectional close indexes must contain the exact seven assets"
        )
    ordered_anchors = tuple(sorted(anchors, key=_anchor_sort_key))
    _require_unique_anchor_identities(ordered_anchors)
    process_count = (
        len(ordered_anchors)
        if maximum_anchors is None
        else min(maximum_anchors, len(ordered_anchors))
    )
    selected = ordered_anchors[:process_count]
    selected_hashes = {anchor.anchor_sha256 for anchor in selected}
    if len(selected_hashes) != len(selected):
        raise HistoricalThreeFamilyCensusErrorV2(
            "selected diagnostic anchor hashes are not unique"
        )
    by_asset: dict[str, list[HistoricalRecommendationAnchorV2]] = defaultdict(list)
    for anchor in selected:
        by_asset[anchor.asset].append(anchor)

    rows_by_anchor: dict[str, HistoricalConsensusCensusRowV2] = {}
    for asset in _ASSETS:
        asset_anchors = by_asset.get(asset, [])
        if not asset_anchors:
            continue
        target = target_loader(asset)
        if (
            target.asset != asset
            or target.symbol != _FUTURES_SYMBOL_BY_ASSET[asset]
            or target.dataset_sha256 != close_indexes[asset].dataset_sha256
        ):
            raise HistoricalThreeFamilyCensusErrorV2(
                f"{asset} target loader returned incompatible authority"
            )
        for anchor in sorted(asset_anchors, key=_anchor_sort_key):
            price_rows = target.slice_ending(
                final_open_time_ms=anchor.bar_open_ms,
                row_count=windows.price_rows,
            )
            participation_rows = target.slice_ending(
                final_open_time_ms=anchor.bar_open_ms,
                row_count=windows.participation_rows,
            )
            if price_rows[-1].close != anchor.price:
                raise HistoricalThreeFamilyCensusErrorV2(
                    f"anchor {anchor.anchor_sha256} price differs from its closed decision candle"
                )
            peer_windows = tuple(
                close_indexes[peer_asset].compact_window(
                    final_open_time_ms=anchor.bar_open_ms,
                    row_count=windows.cross_rows,
                )
                for peer_asset in _ASSETS
                if peer_asset != asset
            )
            if len(peer_windows) != HISTORICAL_CROSS_SECTIONAL_7ASSET_PEER_COUNT_V2:
                raise HistoricalThreeFamilyCensusErrorV2(
                    "cross-sectional target exclusion did not yield exactly six peers"
                )
            attempt_id = f"historical-census:{anchor.anchor_sha256}"
            try:
                price = builders.price(
                    attempt_id,
                    target.dataset_sha256,
                    anchor.bar_open_ms,
                    price_rows,
                )
                participation = builders.participation(
                    attempt_id,
                    target.dataset_sha256,
                    anchor.bar_open_ms,
                    participation_rows,
                )
                cross_section = builders.cross_section(
                    anchor.symbol,
                    anchor.bar_open_ms,
                    peer_windows,
                )
                row = builders.compact_consensus(
                    anchor,
                    price,
                    participation,
                    cross_section,
                    execution_contract,
                    experiment_contract_sha256,
                    topology_contract_sha256,
                )
            except HistoricalThreeFamilyCensusErrorV2:
                raise
            except Exception as exc:
                raise HistoricalThreeFamilyCensusErrorV2(
                    f"proxy/consensus construction failed for anchor {anchor.anchor_sha256}"
                ) from exc
            if row.anchor_sha256 != anchor.anchor_sha256:
                raise HistoricalThreeFamilyCensusErrorV2(
                    "compact consensus row changed its source anchor identity"
                )
            if anchor.anchor_sha256 in rows_by_anchor:
                raise HistoricalThreeFamilyCensusErrorV2(
                    "one anchor produced more than one compact consensus row"
                )
            rows_by_anchor[anchor.anchor_sha256] = row
            del price_rows, participation_rows, peer_windows, price, participation
            del cross_section, row
        del target

    return _finalize_evaluation_v2(
        ordered_anchors=ordered_anchors,
        selected_hashes=selected_hashes,
        rows_by_anchor=rows_by_anchor,
        process_count=process_count,
    )


def _finalize_evaluation_v2(
    *,
    ordered_anchors: tuple[HistoricalRecommendationAnchorV2, ...],
    selected_hashes: set[str],
    rows_by_anchor: Mapping[str, HistoricalConsensusCensusRowV2],
    process_count: int,
) -> tuple[
    tuple[HistoricalConsensusCensusRowV2, ...],
    tuple[HistoricalAnchorDispositionRowV2, ...],
]:
    rows = tuple(sorted(rows_by_anchor.values(), key=_consensus_row_sort_key))
    if len(rows) != process_count or set(rows_by_anchor) != selected_hashes:
        raise HistoricalThreeFamilyCensusErrorV2(
            "processed anchor identities differ from compact consensus rows"
        )
    dispositions = tuple(
        HistoricalAnchorDispositionRowV2(
            split=anchor.split,
            asset=anchor.asset,
            primary_direction=anchor.primary_direction.value,
            decision_time_ms=anchor.decision_time_ms,
            anchor_sha256=anchor.anchor_sha256,
            disposition=(
                HistoricalAnchorDispositionV2.CONSENSUS_EMITTED
                if anchor.anchor_sha256 in selected_hashes
                else HistoricalAnchorDispositionV2.DIAGNOSTIC_LIMIT_NOT_EVALUATED
            ),
            consensus_event_id=(
                rows_by_anchor[anchor.anchor_sha256].event_id
                if anchor.anchor_sha256 in selected_hashes
                else None
            ),
            consensus_payload_sha256=(
                rows_by_anchor[anchor.anchor_sha256].payload_sha256
                if anchor.anchor_sha256 in selected_hashes
                else None
            ),
        )
        for anchor in ordered_anchors
    )
    if len(dispositions) != len(ordered_anchors):
        raise HistoricalThreeFamilyCensusErrorV2(
            "all-anchor disposition ledger is incomplete"
        )
    return rows, dispositions


def _production_builders_v2() -> _CensusBuildersV2[
    _CompactPriceCalculationV2,
    _CompactParticipationCalculationV2,
    _CompactCrossCalculationV2,
]:
    return _CensusBuildersV2(
        price=_build_compact_price_calculation_v2,
        participation=_build_compact_participation_calculation_v2,
        cross_section=_build_compact_cross_calculation_v2,
        compact_consensus=_build_compact_consensus_v2,
    )


def _build_compact_price_calculation_v2(
    _attempt_id: str,
    dataset_sha256: str,
    bar_open_ms: int,
    rows: tuple[Candle, ...],
) -> _CompactPriceCalculationV2:
    if len(rows) != _PRICE_ROWS or rows[-1].open_time_ms != bar_open_ms:
        raise HistoricalThreeFamilyCensusErrorV2(
            "compact price calculation requires the exact causal close window"
        )
    calculation = calculate_price_close_path_v2(tuple(row.close for row in rows))
    source_slice_sha256 = _compact_target_slice_sha256(
        domain=_PRICE_NUMERIC_SLICE_DOMAIN,
        representation="PRICE_CLOSE_PATH_DATASET_ROOT_WINDOW",
        dataset_sha256=dataset_sha256,
        symbol=rows[-1].symbol,
        first_open_ms=rows[0].open_time_ms,
        final_open_ms=bar_open_ms,
        row_count=len(rows),
    )
    return _CompactPriceCalculationV2(
        calculation=calculation,
        source_slice_sha256=source_slice_sha256,
    )


def _build_compact_participation_calculation_v2(
    _attempt_id: str,
    dataset_sha256: str,
    bar_open_ms: int,
    rows: tuple[Candle, ...],
) -> _CompactParticipationCalculationV2:
    if len(rows) != _PARTICIPATION_ROWS or rows[-1].open_time_ms != bar_open_ms:
        raise HistoricalThreeFamilyCensusErrorV2(
            "compact participation calculation requires the exact causal kline window"
        )
    values = []
    try:
        with localcontext(protocol_decimal_context_v2()):
            for row in rows:
                total = +row.quote_volume
                signed = Decimal(2) * row.taker_buy_quote_volume - row.quote_volume
                values.append(
                    build_participation_flow_bar_value_v2(
                        bar_open_ms=row.open_time_ms,
                        bar_close_ms=row.close_time_ms,
                        signed_normal_notional=signed,
                        normal_notional=total,
                        total_trade_notional=total,
                        signed_share=signed / total if total > 0 else None,
                    )
                )
    except DecimalException as exc:
        raise HistoricalThreeFamilyCensusErrorV2(
            "compact participation kline arithmetic violated Decimal34"
        ) from exc
    calculation = calculate_participation_flow_v2(
        current_bar=values[-1],
        prior_bars=tuple(values[:-1]),
    )
    source_slice_sha256 = _compact_target_slice_sha256(
        domain=_PARTICIPATION_NUMERIC_SLICE_DOMAIN,
        representation="ALL_TRADES_ASSUMED_NORMAL_KLINE_PROXY_DATASET_ROOT_WINDOW",
        dataset_sha256=dataset_sha256,
        symbol=rows[-1].symbol,
        first_open_ms=rows[0].open_time_ms,
        final_open_ms=bar_open_ms,
        row_count=len(rows),
    )
    return _CompactParticipationCalculationV2(
        calculation=calculation,
        source_slice_sha256=source_slice_sha256,
    )


def _build_compact_cross_calculation_v2(
    target_symbol: str,
    final_decision_bar_open_ms: int,
    peer_windows: tuple[_HistoricalPeerWindowV2, ...],
) -> _CompactCrossCalculationV2:
    if (
        type(peer_windows) is not tuple
        or len(peer_windows) != HISTORICAL_CROSS_SECTIONAL_7ASSET_PEER_COUNT_V2
        or target_symbol in {value.symbol for value in peer_windows}
        or len({value.symbol for value in peer_windows}) != len(peer_windows)
    ):
        raise HistoricalThreeFamilyCensusErrorV2(
            "compact cross calculation requires exactly six target-excluded peers"
        )
    if any(
        value.final_open_ms != final_decision_bar_open_ms
        or len(value.closes) != _CROSS_ROWS
        for value in peer_windows
    ):
        raise HistoricalThreeFamilyCensusErrorV2(
            "compact cross peer windows have the wrong decision scope"
        )
    peer_returns: list[tuple[Decimal, ...]] = []
    try:
        with localcontext(protocol_decimal_context_v2()):
            for window in peer_windows:
                peer_returns.append(
                    tuple(
                        (window.closes[index] / window.closes[index - 3]).ln()
                        for index in range(3, len(window.closes))
                    )
                )
            prior_market_median = tuple(
                _median_decimal_v2(
                    tuple(values[index] for values in peer_returns)
                )
                for index in range(_CROSS_ROWS - 4)
            )
            current_peer_returns = tuple(values[-1] for values in peer_returns)
    except DecimalException as exc:
        raise HistoricalThreeFamilyCensusErrorV2(
            "compact cross return arithmetic violated Decimal34"
        ) from exc
    calculation = calculate_historical_cross_sectional_7asset_returns_v2(
        prior_market_median_returns_3=prior_market_median,
        current_peer_returns_3=current_peer_returns,
    )
    peer_paths = tuple(
        sorted(
            ((value.symbol, value.path_sha256) for value in peer_windows),
            key=lambda value: value[0].encode("utf-8"),
        )
    )
    peer_input_sha256 = _compact_cross_input_sha256(
        target_symbol=target_symbol,
        final_open_ms=final_decision_bar_open_ms,
        peer_path_sha256s=peer_paths,
    )
    return _CompactCrossCalculationV2(
        calculation=calculation,
        peer_path_sha256s=peer_paths,
        peer_input_sha256=peer_input_sha256,
    )


def _build_compact_consensus_v2(
    anchor: HistoricalRecommendationAnchorV2,
    price: _CompactPriceCalculationV2,
    participation: _CompactParticipationCalculationV2,
    cross_section: _CompactCrossCalculationV2,
    execution_contract: HistoricalExecutionContractV2,
    experiment_contract_sha256: str,
    topology_contract_sha256: str,
) -> HistoricalConsensusCensusRowV2:
    _validate_sha256(topology_contract_sha256, "topology_contract_sha256")
    leaves: tuple[HistoricalDirectionalLeafV2, ...] = (
        build_historical_directional_leaf_from_calculation_v2(
            calculation=price.calculation,
            source_slice_sha256=price.source_slice_sha256,
            symbol=anchor.symbol,
            venue=VenueV2.USDM_FUTURES,
            interval="5m",
            bar_open_ms=anchor.bar_open_ms,
            bar_close_ms=anchor.bar_close_ms,
            historical_slice_through_ms=anchor.bar_close_ms,
        ),
        build_historical_directional_leaf_from_calculation_v2(
            calculation=participation.calculation,
            source_slice_sha256=participation.source_slice_sha256,
            symbol=anchor.symbol,
            venue=VenueV2.USDM_FUTURES,
            interval="5m",
            bar_open_ms=anchor.bar_open_ms,
            bar_close_ms=anchor.bar_close_ms,
            historical_slice_through_ms=anchor.bar_close_ms,
        ),
        build_historical_directional_leaf_from_calculation_v2(
            calculation=cross_section.calculation,
            source_slice_sha256=cross_section.peer_input_sha256,
            symbol=anchor.symbol,
            venue=VenueV2.USDM_FUTURES,
            interval="5m",
            bar_open_ms=anchor.bar_open_ms,
            bar_close_ms=anchor.bar_close_ms,
            historical_slice_through_ms=anchor.bar_close_ms,
        ),
    )
    consensus = build_historical_three_family_consensus_from_leaves_v2(
        anchor=anchor,
        leaves=leaves,
        execution_contract=execution_contract,
        experiment_contract_sha256=experiment_contract_sha256,
        cross_peer_path_sha256s=cross_section.peer_path_sha256s,
        cross_peer_input_sha256=cross_section.peer_input_sha256,
    )
    canonical = canonical_historical_three_family_consensus_v2(consensus)
    canonical_consensus_sha256 = _sha256_bytes(canonical)
    topology = derive_historical_three_family_topology_v2(consensus)
    canonical_topology = canonical_historical_three_family_topology_v2(topology)
    if topology.source_consensus_canonical_sha256 != canonical_consensus_sha256:
        raise HistoricalThreeFamilyCensusErrorV2(
            "topology source hash differs from the canonical consensus"
        )
    return _compact_consensus_row_v2(
        consensus,
        topology=topology,
        canonical_consensus_sha256=canonical_consensus_sha256,
        canonical_topology_sha256=_sha256_bytes(canonical_topology),
        topology_contract_sha256=topology_contract_sha256,
    )


def _compact_consensus_row_v2(
    value: HistoricalThreeFamilyConsensusV2,
    *,
    topology: HistoricalThreeFamilyTopologyV2,
    canonical_consensus_sha256: str,
    canonical_topology_sha256: str,
    topology_contract_sha256: str,
) -> HistoricalConsensusCensusRowV2:
    _validate_sha256(canonical_consensus_sha256, "canonical consensus hash")
    _validate_sha256(canonical_topology_sha256, "canonical topology hash")
    _validate_sha256(topology_contract_sha256, "topology_contract_sha256")
    if (
        topology.source_consensus is not value
        or topology.source_consensus_canonical_sha256 != canonical_consensus_sha256
        or topology.source_event_id != value.event_id
        or topology.source_payload_sha256 != value.payload_sha256
        or topology.source_anchor_sha256 != value.anchor.anchor_sha256
        or topology.conflicted_comparator_outcome_authorized is not False
    ):
        raise HistoricalThreeFamilyCensusErrorV2(
            "topology projection is not bound to the exact compact consensus"
        )
    leaves = {leaf.family: leaf for leaf in value.leaves}
    if set(leaves) != set(HistoricalFamilyV2):
        raise HistoricalThreeFamilyCensusErrorV2(
            "consensus does not contain the exact three compact leaves"
        )
    price = leaves[HistoricalFamilyV2.PRICE_STRUCTURE_MOMENTUM]
    participation = leaves[HistoricalFamilyV2.PARTICIPATION_FLOW]
    cross = leaves[HistoricalFamilyV2.CROSS_SECTIONAL_CONTEXT_EX_TARGET]
    anchor = value.anchor
    cost = value.cost_context
    return HistoricalConsensusCensusRowV2(
        split=anchor.split,
        asset=anchor.asset,
        symbol=anchor.symbol,
        source_event_id=anchor.source_event_id,
        source_row_sha256=anchor.source_row_sha256,
        source_replay_manifest_sha256=anchor.source_replay_manifest_sha256,
        anchor_sha256=anchor.anchor_sha256,
        primary_family=anchor.primary_family.value,
        primary_direction=anchor.primary_direction.value,
        decision_time_ms=anchor.decision_time_ms,
        price=anchor.price,
        invalidation=anchor.invalidation,
        atr=anchor.atr,
        event_id=value.event_id,
        payload_sha256=value.payload_sha256,
        canonical_consensus_sha256=canonical_consensus_sha256,
        topology_sha256=topology.topology_sha256,
        canonical_topology_sha256=canonical_topology_sha256,
        topology_contract_sha256=topology_contract_sha256,
        topology_rule_version=topology.rule_version,
        topology_class=topology.topology.value,
        topology_comparison_bucket=topology.comparison_bucket.value,
        topology_display_grade=topology.display_grade.value,
        topology_majority_direction=(
            None
            if topology.majority_direction is None
            else topology.majority_direction.value
        ),
        topology_majority_family_count=topology.majority_family_count,
        topology_opposing_family_count=topology.opposing_family_count,
        topology_has_opposition=topology.has_opposition,
        topology_primary_support_count=topology.primary_support_count,
        topology_primary_oppose_count=topology.primary_oppose_count,
        topology_primary_neutral_count=topology.primary_neutral_count,
        clean_primary_audit_eligible=topology.clean_primary_audit_eligible,
        conflicted_comparator_eligible=topology.conflicted_comparator_eligible,
        conflicted_comparator_outcome_authorized=(
            topology.conflicted_comparator_outcome_authorized
        ),
        rule_version=value.rule_version,
        status=value.status.value,
        state_class=value.state_class.value,
        directional_numerator_micros=value.directional_numerator_micros,
        directional_denominator=value.directional_denominator,
        directional_agreement_micros=value.directional_agreement_micros,
        bullish_family_count=value.bullish_family_count,
        bearish_family_count=value.bearish_family_count,
        neutral_family_count=value.neutral_family_count,
        primary_relationship=value.primary_relationship.value,
        admitted=value.admitted,
        price_status=price.status.value,
        price_direction=price.direction,
        price_strength_micros=price.strength_micros,
        price_calculation_sha256=price.source_payload_sha256,
        price_source_slice_sha256=price.source_slice_sha256,
        participation_status=participation.status.value,
        participation_direction=participation.direction,
        participation_strength_micros=participation.strength_micros,
        participation_calculation_sha256=participation.source_payload_sha256,
        participation_source_slice_sha256=participation.source_slice_sha256,
        cross_section_status=cross.status.value,
        cross_section_direction=cross.direction,
        cross_section_strength_micros=cross.strength_micros,
        cross_section_calculation_sha256=cross.source_payload_sha256,
        cross_section_source_slice_sha256=cross.source_slice_sha256,
        execution_contract_sha256=value.execution_contract.execution_contract_sha256,
        zero_move_round_trip_cost_micros=cost.zero_move_round_trip_cost_micros,
        atr_fraction_micros=cost.atr_fraction_micros,
        one_atr_cost_headroom_micros=cost.one_atr_cost_headroom_micros,
        cross_peer_set_root_sha256=value.cross_peer_set_root_sha256,
        cross_peer_input_sha256=value.cross_peer_input_sha256,
        reasons=value.reasons,
    )


def _census_results_document_v2(
    *,
    loaded: LoadedHistoricalRecommendationAnchorsV2,
    authorities: tuple[HistoricalFuturesKlineAuthorityV2, ...],
    rows: tuple[HistoricalConsensusCensusRowV2, ...],
    dispositions: tuple[HistoricalAnchorDispositionRowV2, ...],
    execution_contract: HistoricalExecutionContractV2,
    contract_authority: HistoricalContractAuthorityV2,
    numeric_provenance: HistoricalNumericRepresentationProvenanceV2,
    maximum_anchors: int | None,
    consensus_csv_sha256: str,
) -> dict[str, object]:
    if any(
        row.topology_contract_sha256 != contract_authority.topology_contract_sha256
        for row in rows
    ):
        raise HistoricalThreeFamilyCensusErrorV2(
            "compact topology rows differ from the caller-bound amendment hash"
        )
    _validate_numeric_provenance_v2(
        numeric_provenance,
        processed_assets={row.asset for row in rows},
    )
    disposition_counts = Counter(item.disposition.value for item in dispositions)
    topology_analysis = _topology_analysis_document_v2(rows)
    split_documents: list[dict[str, object]] = []
    for split in HISTORICAL_THREE_FAMILY_CENSUS_SPLITS_V2:
        split_dispositions = tuple(item for item in dispositions if item.split == split)
        split_rows = tuple(item for item in rows if item.split == split)
        split_documents.append(
            {
                "all_anchor_dispositions_sha256": _disposition_sha256(split_dispositions),
                "authenticated_anchors": len(split_dispositions),
                "consensus_rows": len(split_rows),
                "disposition_counts": dict(
                    sorted(
                        Counter(item.disposition.value for item in split_dispositions).items()
                    )
                ),
                "split": split,
            }
        )
    lower_bound_seconds = (
        _EXACT_CROSS_SECONDS_PER_ANCHOR_LOWER_BOUND
        * HISTORICAL_THREE_FAMILY_EXPECTED_ANCHORS_V2
    )
    return {
        "all_anchor_dispositions_sha256": _disposition_sha256(dispositions),
        "anchor_set_sha256": loaded.anchor_set_sha256,
        "authenticated_anchors": len(loaded.anchors),
        "census_complete": maximum_anchors is None,
        "code_freeze_manifest_sha256": contract_authority.code_freeze_manifest_sha256,
        "consensus_csv_sha256": consensus_csv_sha256,
        "consensus_rows": len(rows),
        "consensus_rows_sha256": _consensus_rows_sha256(rows),
        "consensus_rule_version": HISTORICAL_THREE_FAMILY_CONSENSUS_RULE_VERSION_V2,
        "conflicted_comparator_outcome_authorized": False,
        "contract_authority": _contract_authority_document_v2(contract_authority),
        "data_authority": [_kline_authority_document(value) for value in authorities],
        "diagnostic_limit_reached": (
            maximum_anchors is not None and maximum_anchors < len(loaded.anchors)
        ),
        "diagnostic_mode": maximum_anchors is not None,
        "disposition_counts": dict(sorted(disposition_counts.items())),
        "execution_contract_sha256": execution_contract.execution_contract_sha256,
        "experiment_contract_sha256": contract_authority.experiment_contract_sha256,
        "historical_only": True,
        "historical_receipt_policy": "RECEIPT_EQUALS_CLOSED_KLINE_CLOSE_TIME",
        "maximum_anchors": maximum_anchors,
        "numeric_representation_provenance": _numeric_provenance_document_v2(
            numeric_provenance
        ),
        "outcome_data_read": False,
        "performance_contract": {
            "batch_precompute_used": True,
            "compact_numeric_calculation_path_used": True,
            "full_source_proxy_hot_loop_used": False,
            "numeric_provenance": (
                "AUTHENTICATED_DATASET_ROOT_PLUS_EXACT_WINDOW_BOUNDARIES"
            ),
            "exact_cross_seconds_per_anchor_observed_lower_bound": str(
                _EXACT_CROSS_SECONDS_PER_ANCHOR_LOWER_BOUND
            ),
            "full_exact_cross_lower_bound_seconds": str(lower_bound_seconds),
            "lower_bound_excludes_consensus_proxy_canonical_revalidation": True,
            "warning": "FULL_SOURCE_PROXY_FALLBACK_EXPECTED_TO_EXCEED_FIVE_HOURS",
        },
        "probability": False,
        "probability_calibrated": False,
        "promoting": False,
        "protocol": HISTORICAL_THREE_FAMILY_CENSUS_PROTOCOL_V2,
        "schema_version": HISTORICAL_THREE_FAMILY_CENSUS_SCHEMA_VERSION_V2,
        "source_replays": [_replay_audit_document(value) for value in loaded.replay_audits],
        "splits": split_documents,
        "target_return_used": False,
        "topology_analysis": topology_analysis,
        "topology_analysis_sha256": topology_analysis["topology_analysis_sha256"],
        "topology_contract_sha256": contract_authority.topology_contract_sha256,
        "topology_rule_version": HISTORICAL_THREE_FAMILY_TOPOLOGY_RULE_VERSION_V2,
        "v1a_fitted_selection_used": False,
    }


def _topology_analysis_document_v2(
    rows: tuple[HistoricalConsensusCensusRowV2, ...],
) -> dict[str, object]:
    """Build exhaustive outcome-blind topology counts for compact consensus rows."""

    ordered_rows = tuple(sorted(rows, key=_consensus_row_sort_key))
    if len({row.anchor_sha256 for row in ordered_rows}) != len(ordered_rows):
        raise HistoricalThreeFamilyCensusErrorV2(
            "topology analysis cannot contain duplicate anchor rows"
        )
    for row in ordered_rows:
        _validate_compact_topology_row_v2(row)

    topology_classes = tuple(value.value for value in HistoricalThreeFamilyTopologyClassV2)
    grouped = Counter(
        (row.split, row.asset, row.primary_direction, row.topology_class)
        for row in ordered_rows
    )
    grouped_counts = [
        {
            "asset": asset,
            "consensus_rows": grouped[(split, asset, primary_direction, topology)],
            "primary_direction": primary_direction,
            "split": split,
            "topology": topology,
        }
        for split in HISTORICAL_THREE_FAMILY_CENSUS_SPLITS_V2
        for asset in _ASSETS
        for primary_direction in (Direction.LONG.value, Direction.SHORT.value)
        for topology in topology_classes
    ]

    ready_rows: dict[tuple[int, int, int], list[HistoricalConsensusCensusRowV2]] = (
        defaultdict(list)
    )
    withheld_rows = 0
    for row in ordered_rows:
        sign_tuple = _compact_ready_sign_tuple_v2(row)
        if sign_tuple is None:
            withheld_rows += 1
        else:
            ready_rows[sign_tuple].append(row)
    ordered_sign_table = []
    for ordinal, signs in enumerate(product((-1, 0, 1), repeat=3)):
        sign_tuple = cast(tuple[int, int, int], signs)
        matching = ready_rows.get(sign_tuple, [])
        ordered_sign_table.append(
            {
                "admitted_rows": sum(row.admitted for row in matching),
                "clean_primary_audit_eligible_rows": sum(
                    row.clean_primary_audit_eligible for row in matching
                ),
                "conflicted_comparator_eligible_rows": sum(
                    row.conflicted_comparator_eligible for row in matching
                ),
                "consensus_rows": len(matching),
                "cross_sectional_context_ex_target_direction": sign_tuple[2],
                "ordinal": ordinal,
                "participation_flow_direction": sign_tuple[1],
                "price_structure_momentum_direction": sign_tuple[0],
                "topology": _topology_class_for_sign_tuple_v2(sign_tuple),
            }
        )

    admitted_rows = sum(row.admitted for row in ordered_rows)
    clean_rows = sum(row.clean_primary_audit_eligible for row in ordered_rows)
    conflicted_rows = sum(row.conflicted_comparator_eligible for row in ordered_rows)
    admitted_without_clean = sum(
        row.admitted and not row.clean_primary_audit_eligible for row in ordered_rows
    )
    clean_without_admitted = sum(
        row.clean_primary_audit_eligible and not row.admitted for row in ordered_rows
    )
    conflicted_and_admitted = sum(
        row.conflicted_comparator_eligible and row.admitted for row in ordered_rows
    )
    if admitted_without_clean or clean_without_admitted or conflicted_and_admitted:
        raise HistoricalThreeFamilyCensusErrorV2(
            "topology eligibility does not reconcile to frozen consensus admission"
        )

    document: dict[str, object] = {
        "admission_reconciliation": {
            "admission_parity": True,
            "admitted_not_clean_primary_rows": admitted_without_clean,
            "clean_primary_audit_eligible_rows": clean_rows,
            "clean_primary_not_admitted_rows": clean_without_admitted,
            "conflicted_and_admitted_rows": conflicted_and_admitted,
            "conflicted_comparator_eligible_rows": conflicted_rows,
            "conflicted_comparator_outcome_authorized": False,
            "consensus_rows": len(ordered_rows),
            "source_admitted_rows": admitted_rows,
        },
        "conflicted_comparator_outcome_authorized": False,
        "consensus_rows": len(ordered_rows),
        "family_order": [value.value for value in HistoricalFamilyV2],
        "leaf_sign_rates": _leaf_sign_rate_documents_v2(ordered_rows),
        "ordered_27_sign_table": ordered_sign_table,
        "outcome_data_used": False,
        "pairwise_family_relationship_rates": (
            _pairwise_family_relationship_rate_documents_v2(ordered_rows)
        ),
        "probability": False,
        "probability_calibrated": False,
        "ready_sign_rows": sum(len(values) for values in ready_rows.values()),
        "schema_version": "r4b_historical_three_family_topology_analysis_v2",
        "sign_order": [-1, 0, 1],
        "split_asset_primary_direction_topology_counts": grouped_counts,
        "topology_order": list(topology_classes),
        "topology_rows_sha256": _topology_rows_sha256_v2(ordered_rows),
        "topology_rule_version": HISTORICAL_THREE_FAMILY_TOPOLOGY_RULE_VERSION_V2,
        "withheld_rows": withheld_rows,
    }
    document["topology_analysis_sha256"] = hashlib.sha256(
        _TOPOLOGY_ANALYSIS_DOMAIN + canonical_json_line(document)
    ).hexdigest()
    return document


def _topology_rows_sha256_v2(
    rows: tuple[HistoricalConsensusCensusRowV2, ...],
) -> str:
    return hashlib.sha256(
        _TOPOLOGY_ROWS_DOMAIN
        + canonical_json_line(
            {
                "rows": [
                    {
                        "anchor_sha256": row.anchor_sha256,
                        "canonical_consensus_sha256": row.canonical_consensus_sha256,
                        "canonical_topology_sha256": row.canonical_topology_sha256,
                        "topology_sha256": row.topology_sha256,
                        "topology_contract_sha256": row.topology_contract_sha256,
                    }
                    for row in rows
                ],
                "schema_version": "r4b_historical_census_topology_rows_v2",
            }
        )
    ).hexdigest()


def _validate_compact_topology_row_v2(row: HistoricalConsensusCensusRowV2) -> None:
    for digest, label in (
        (row.topology_sha256, "topology_sha256"),
        (row.canonical_topology_sha256, "canonical_topology_sha256"),
        (row.topology_contract_sha256, "topology_contract_sha256"),
    ):
        _validate_sha256(digest, label)
    if (
        row.split not in HISTORICAL_THREE_FAMILY_CENSUS_SPLITS_V2
        or row.asset not in _ASSETS
        or row.primary_direction not in {Direction.LONG.value, Direction.SHORT.value}
        or row.topology_rule_version
        != HISTORICAL_THREE_FAMILY_TOPOLOGY_RULE_VERSION_V2
        or row.topology_class
        not in {value.value for value in HistoricalThreeFamilyTopologyClassV2}
        or row.conflicted_comparator_outcome_authorized is not False
        or row.clean_primary_audit_eligible is not row.admitted
    ):
        raise HistoricalThreeFamilyCensusErrorV2(
            "compact topology row violates its frozen identity or admission contract"
        )
    sign_tuple = _compact_ready_sign_tuple_v2(row)
    if sign_tuple is None:
        if (
            row.topology_class != HistoricalThreeFamilyTopologyClassV2.WITHHELD.value
            or row.topology_comparison_bucket != "WITHHELD"
            or row.topology_display_grade != "WITHHELD_DATA"
            or row.topology_majority_direction is not None
            or row.topology_majority_family_count is not None
            or row.topology_opposing_family_count is not None
            or row.topology_has_opposition is not None
            or row.topology_primary_support_count is not None
            or row.topology_primary_oppose_count is not None
            or row.topology_primary_neutral_count is not None
            or row.clean_primary_audit_eligible
            or row.conflicted_comparator_eligible
        ):
            raise HistoricalThreeFamilyCensusErrorV2(
                "a compact row with an unavailable leaf must have exact WITHHELD topology"
            )
        return
    expected_class = _topology_class_for_sign_tuple_v2(sign_tuple)
    expected_bucket, expected_grade = _topology_labels_for_class_v2(expected_class)
    bullish = sign_tuple.count(1)
    bearish = sign_tuple.count(-1)
    neutral = sign_tuple.count(0)
    if bullish >= 2:
        majority_direction = "BULLISH"
        majority_count = bullish
        opposing_count = bearish
    elif bearish >= 2:
        majority_direction = "BEARISH"
        majority_count = bearish
        opposing_count = bullish
    else:
        majority_direction = None
        majority_count = None
        opposing_count = None
    if row.primary_direction == Direction.LONG.value:
        support, oppose = bullish, bearish
    else:
        support, oppose = bearish, bullish
    expected_clean = (
        expected_bucket in {"BROAD_3_OF_3", "CLEAN_2_PLUS_NEUTRAL"}
        and support >= 2
        and oppose == 0
    )
    expected_conflicted = (
        expected_bucket == "CONFLICTED_2_VS_1"
        and support == 2
        and oppose == 1
    )
    if (
        row.topology_class != expected_class
        or row.topology_comparison_bucket != expected_bucket
        or row.topology_display_grade != expected_grade
        or row.bullish_family_count != bullish
        or row.bearish_family_count != bearish
        or row.neutral_family_count != neutral
        or row.topology_majority_direction != majority_direction
        or row.topology_majority_family_count != majority_count
        or row.topology_opposing_family_count != opposing_count
        or row.topology_has_opposition is not (bullish > 0 and bearish > 0)
        or row.topology_primary_support_count != support
        or row.topology_primary_oppose_count != oppose
        or row.topology_primary_neutral_count != neutral
        or row.clean_primary_audit_eligible is not expected_clean
        or row.conflicted_comparator_eligible is not expected_conflicted
    ):
        raise HistoricalThreeFamilyCensusErrorV2(
            "compact topology fields differ from the exact three-leaf signs"
        )


def _compact_ready_sign_tuple_v2(
    row: HistoricalConsensusCensusRowV2,
) -> tuple[int, int, int] | None:
    statuses = (
        row.price_status,
        row.participation_status,
        row.cross_section_status,
    )
    directions = (
        row.price_direction,
        row.participation_direction,
        row.cross_section_direction,
    )
    ready = tuple(status == "READY" for status in statuses)
    if any(
        not is_ready and direction is not None
        for is_ready, direction in zip(ready, directions, strict=True)
    ):
        raise HistoricalThreeFamilyCensusErrorV2(
            "an unavailable compact leaf cannot expose a direction"
        )
    if all(ready):
        if any(type(value) is not int or value not in (-1, 0, 1) for value in directions):
            raise HistoricalThreeFamilyCensusErrorV2(
                "READY compact leaves require exact -1, 0, or 1 directions"
            )
        return cast(tuple[int, int, int], directions)
    return None


def _topology_class_for_sign_tuple_v2(signs: tuple[int, int, int]) -> str:
    counts = (signs.count(1), signs.count(-1), signs.count(0))
    mapping = {
        (3, 0, 0): HistoricalThreeFamilyTopologyClassV2.UNANIMOUS_BULLISH_3_0_0,
        (0, 3, 0): HistoricalThreeFamilyTopologyClassV2.UNANIMOUS_BEARISH_0_3_0,
        (2, 0, 1): HistoricalThreeFamilyTopologyClassV2.CLEAN_BULLISH_2_0_1,
        (0, 2, 1): HistoricalThreeFamilyTopologyClassV2.CLEAN_BEARISH_0_2_1,
        (2, 1, 0): HistoricalThreeFamilyTopologyClassV2.CONFLICTED_BULLISH_2_1_0,
        (1, 2, 0): HistoricalThreeFamilyTopologyClassV2.CONFLICTED_BEARISH_1_2_0,
        (1, 0, 2): HistoricalThreeFamilyTopologyClassV2.LONE_BULLISH_1_0_2,
        (0, 1, 2): HistoricalThreeFamilyTopologyClassV2.LONE_BEARISH_0_1_2,
        (1, 1, 1): HistoricalThreeFamilyTopologyClassV2.BALANCED_1_1_1,
        (0, 0, 3): HistoricalThreeFamilyTopologyClassV2.ALL_NEUTRAL_0_0_3,
    }
    try:
        return mapping[counts].value
    except KeyError as exc:
        raise HistoricalThreeFamilyCensusErrorV2(
            "three-leaf sign tuple is outside the exhaustive topology table"
        ) from exc


def _topology_labels_for_class_v2(topology: str) -> tuple[str, str]:
    if topology in {
        HistoricalThreeFamilyTopologyClassV2.UNANIMOUS_BULLISH_3_0_0.value,
        HistoricalThreeFamilyTopologyClassV2.UNANIMOUS_BEARISH_0_3_0.value,
    }:
        return "BROAD_3_OF_3", "UNANIMOUS_BREADTH_UNCALIBRATED"
    if topology in {
        HistoricalThreeFamilyTopologyClassV2.CLEAN_BULLISH_2_0_1.value,
        HistoricalThreeFamilyTopologyClassV2.CLEAN_BEARISH_0_2_1.value,
    }:
        return "CLEAN_2_PLUS_NEUTRAL", "CLEAN_TWO_FAMILY_BREADTH_UNCALIBRATED"
    if topology in {
        HistoricalThreeFamilyTopologyClassV2.CONFLICTED_BULLISH_2_1_0.value,
        HistoricalThreeFamilyTopologyClassV2.CONFLICTED_BEARISH_1_2_0.value,
    }:
        return "CONFLICTED_2_VS_1", "CONFLICTED_MAJORITY_UNCALIBRATED"
    if topology in {
        HistoricalThreeFamilyTopologyClassV2.LONE_BULLISH_1_0_2.value,
        HistoricalThreeFamilyTopologyClassV2.LONE_BEARISH_0_1_2.value,
    }:
        return "NOT_COMPARABLE", "INSUFFICIENT_DIRECTIONAL_BREADTH"
    return "NOT_COMPARABLE", "NO_DIRECTIONAL_CONSENSUS"


def _leaf_sign_rate_documents_v2(
    rows: tuple[HistoricalConsensusCensusRowV2, ...],
) -> list[dict[str, object]]:
    family_fields = (
        (
            HistoricalFamilyV2.PRICE_STRUCTURE_MOMENTUM,
            "price_status",
            "price_direction",
        ),
        (
            HistoricalFamilyV2.PARTICIPATION_FLOW,
            "participation_status",
            "participation_direction",
        ),
        (
            HistoricalFamilyV2.CROSS_SECTIONAL_CONTEXT_EX_TARGET,
            "cross_section_status",
            "cross_section_direction",
        ),
    )
    denominator = len(rows)
    documents: list[dict[str, object]] = []
    for family, status_field, direction_field in family_fields:
        counts: Counter[int | None] = Counter()
        for row in rows:
            status = cast(str, getattr(row, status_field))
            direction = cast(int | None, getattr(row, direction_field))
            if status == "READY":
                if type(direction) is not int or direction not in (-1, 0, 1):
                    raise HistoricalThreeFamilyCensusErrorV2(
                        "READY leaf sign-rate row has an invalid direction"
                    )
                counts[direction] += 1
            else:
                counts[None] += 1
        sign_counts = [
            {
                "consensus_rows": counts[direction],
                "direction": direction,
                "rate_micros": _rate_micros_v2(counts[direction], denominator),
            }
            for direction in (-1, 0, 1, None)
        ]
        documents.append(
            {
                "denominator_consensus_rows": denominator,
                "family": family.value,
                "rate_rounding": "HALF_UP_NEAREST_MICRO",
                "rate_scale": 1_000_000,
                "sign_counts": sign_counts,
            }
        )
    return documents


def _pairwise_family_relationship_rate_documents_v2(
    rows: tuple[HistoricalConsensusCensusRowV2, ...],
) -> list[dict[str, object]]:
    family_fields = (
        (
            HistoricalFamilyV2.PRICE_STRUCTURE_MOMENTUM,
            "price_status",
            "price_direction",
        ),
        (
            HistoricalFamilyV2.PARTICIPATION_FLOW,
            "participation_status",
            "participation_direction",
        ),
        (
            HistoricalFamilyV2.CROSS_SECTIONAL_CONTEXT_EX_TARGET,
            "cross_section_status",
            "cross_section_direction",
        ),
    )
    documents: list[dict[str, object]] = []
    for left_index, right_index in ((0, 1), (0, 2), (1, 2)):
        left_family, left_status_field, left_direction_field = family_fields[left_index]
        right_family, right_status_field, right_direction_field = family_fields[
            right_index
        ]
        relationship_counts = Counter[str]()
        for row in rows:
            left_status = cast(str, getattr(row, left_status_field))
            right_status = cast(str, getattr(row, right_status_field))
            left_direction = cast(int | None, getattr(row, left_direction_field))
            right_direction = cast(int | None, getattr(row, right_direction_field))
            if left_status != "READY" or right_status != "READY":
                relationship_counts["UNAVAILABLE"] += 1
            elif left_direction == 0 or right_direction == 0:
                relationship_counts["NEUTRAL_INVOLVED"] += 1
            elif left_direction == right_direction:
                relationship_counts["AGREEMENT"] += 1
            else:
                relationship_counts["DISAGREEMENT"] += 1
        ready_denominator = len(rows) - relationship_counts["UNAVAILABLE"]
        documents.append(
            {
                "agreement_rate_micros": _rate_micros_v2(
                    relationship_counts["AGREEMENT"], ready_denominator
                ),
                "agreement_rows": relationship_counts["AGREEMENT"],
                "both_ready_denominator_rows": ready_denominator,
                "disagreement_rate_micros": _rate_micros_v2(
                    relationship_counts["DISAGREEMENT"], ready_denominator
                ),
                "disagreement_rows": relationship_counts["DISAGREEMENT"],
                "family_a": left_family.value,
                "family_b": right_family.value,
                "neutral_involved_rate_micros": _rate_micros_v2(
                    relationship_counts["NEUTRAL_INVOLVED"], ready_denominator
                ),
                "neutral_involved_rows": relationship_counts["NEUTRAL_INVOLVED"],
                "rate_rounding": "HALF_UP_NEAREST_MICRO",
                "rate_scale": 1_000_000,
                "unavailable_rows": relationship_counts["UNAVAILABLE"],
            }
        )
    return documents


def _rate_micros_v2(numerator: int, denominator: int) -> int | None:
    if denominator == 0:
        return None
    return (numerator * 1_000_000 + denominator // 2) // denominator


def _artifact_manifest_document_v2(
    *,
    loaded: LoadedHistoricalRecommendationAnchorsV2,
    authorities: tuple[HistoricalFuturesKlineAuthorityV2, ...],
    contract_authority: HistoricalContractAuthorityV2,
    numeric_provenance: HistoricalNumericRepresentationProvenanceV2,
    execution_contract: HistoricalExecutionContractV2,
    maximum_anchors: int | None,
    payload_sha256: Mapping[str, str],
) -> dict[str, object]:
    if set(payload_sha256) != set(_PAYLOAD_NAMES):
        raise HistoricalThreeFamilyCensusErrorV2(
            "artifact manifest requires exactly consensus.csv and results.json hashes"
        )
    _validate_numeric_provenance_v2(
        numeric_provenance,
        processed_assets={asset for asset, _digest in numeric_provenance.target_cache_sha256s},
    )
    return {
        "anchor_set_sha256": loaded.anchor_set_sha256,
        "census_complete": maximum_anchors is None,
        "code_freeze_manifest_sha256": contract_authority.code_freeze_manifest_sha256,
        "conflicted_comparator_outcome_authorized": False,
        "contract_authority": _contract_authority_document_v2(contract_authority),
        "consensus_rule_version": HISTORICAL_THREE_FAMILY_CONSENSUS_RULE_VERSION_V2,
        "code_authority": {
            "consensus_rule_version": HISTORICAL_THREE_FAMILY_CONSENSUS_RULE_VERSION_V2,
            "numeric_precompute_rule_version": numeric_provenance.rule_version,
            "topology_rule_version": HISTORICAL_THREE_FAMILY_TOPOLOGY_RULE_VERSION_V2,
        },
        "diagnostic_mode": maximum_anchors is not None,
        "execution_contract_sha256": execution_contract.execution_contract_sha256,
        "experiment_contract_sha256": contract_authority.experiment_contract_sha256,
        "historical_only": True,
        "historical_receipt_policy": "RECEIPT_EQUALS_CLOSED_KLINE_CLOSE_TIME",
        "inputs": {
            "futures_data_sha256": {
                value.relative_data_path: value.data_sha256 for value in authorities
            },
            "futures_manifest_sha256": {
                f"{value.relative_data_path}.manifest.json": value.manifest_sha256
                for value in authorities
            },
            "numeric_representation_provenance": _numeric_provenance_document_v2(
                numeric_provenance
            ),
            "recommendations_sha256": {
                value.split: value.recommendations_sha256
                for value in loaded.replay_audits
            },
            "run_manifest_sha256": {
                value.split: value.run_manifest_sha256 for value in loaded.replay_audits
            },
        },
        "maximum_anchors": maximum_anchors,
        "outcome_data_read": False,
        "outputs": {
            name: _require_sha256(payload_sha256[name], f"output hash {name}")
            for name in _PAYLOAD_NAMES
        },
        "probability": False,
        "promoting": False,
        "protocol": HISTORICAL_THREE_FAMILY_CENSUS_PROTOCOL_V2,
        "source_representation": "CANONICAL_NUMERIC_CALCULATION",
        "schema_version": HISTORICAL_THREE_FAMILY_CENSUS_SCHEMA_VERSION_V2,
        "topology_rule_version": HISTORICAL_THREE_FAMILY_TOPOLOGY_RULE_VERSION_V2,
        "topology_contract_sha256": contract_authority.topology_contract_sha256,
        "v1a_fitted_selection_used": False,
    }


_CONSENSUS_CSV_COLUMNS: Final = tuple(
    field.name for field in fields(HistoricalConsensusCensusRowV2)
)


def _consensus_csv_bytes_v2(
    rows: tuple[HistoricalConsensusCensusRowV2, ...],
) -> bytes:
    ordered = tuple(sorted(rows, key=_consensus_row_sort_key))
    if len({row.anchor_sha256 for row in ordered}) != len(ordered):
        raise HistoricalThreeFamilyCensusErrorV2(
            "consensus CSV cannot contain duplicate anchor rows"
        )
    buffer = io.StringIO(newline="")
    writer: csv.DictWriter[str] = csv.DictWriter(
        buffer,
        fieldnames=list(_CONSENSUS_CSV_COLUMNS),
        extrasaction="raise",
        lineterminator="\n",
    )
    writer.writeheader()
    for row in ordered:
        writer.writerow(_consensus_csv_row_v2(row))
    payload = buffer.getvalue().encode("utf-8")
    if b"\r" in payload or not payload.endswith(b"\n"):
        raise HistoricalThreeFamilyCensusErrorV2(
            "consensus CSV serialization is not canonical LF-only UTF-8"
        )
    return payload


def _consensus_csv_row_v2(value: HistoricalConsensusCensusRowV2) -> dict[str, str]:
    document: dict[str, str] = {}
    for field in fields(value):
        item = getattr(value, field.name)
        if item is None:
            document[field.name] = ""
        elif type(item) is bool:
            document[field.name] = "true" if item else "false"
        elif type(item) is tuple:
            document[field.name] = "|".join(cast(tuple[str, ...], item))
        else:
            document[field.name] = str(item)
    return document


def _consensus_rows_sha256(
    rows: tuple[HistoricalConsensusCensusRowV2, ...],
) -> str:
    documents = [
        {
            key: value
            for key, value in _consensus_csv_row_v2(row).items()
        }
        for row in sorted(rows, key=_consensus_row_sort_key)
    ]
    return hashlib.sha256(
        _CONSENSUS_ROWS_DOMAIN
        + canonical_json_line(
            {
                "rows": documents,
                "schema_version": "r4b_historical_consensus_compact_rows_v2",
            }
        )
    ).hexdigest()


def _disposition_sha256(
    rows: tuple[HistoricalAnchorDispositionRowV2, ...],
) -> str:
    documents = [
        {
            "anchor_sha256": row.anchor_sha256,
            "asset": row.asset,
            "consensus_event_id": row.consensus_event_id,
            "consensus_payload_sha256": row.consensus_payload_sha256,
            "decision_time_ms": row.decision_time_ms,
            "disposition": row.disposition.value,
            "primary_direction": row.primary_direction,
            "split": row.split,
        }
        for row in rows
    ]
    return hashlib.sha256(
        _DISPOSITION_DOMAIN
        + canonical_json_line(
            {
                "rows": documents,
                "schema_version": "r4b_historical_anchor_dispositions_v2",
            }
        )
    ).hexdigest()


def _anchor_set_sha256(
    anchors: tuple[HistoricalRecommendationAnchorV2, ...],
) -> str:
    return hashlib.sha256(
        _ANCHOR_SET_DOMAIN
        + canonical_json_line(
            {
                "anchor_sha256s": [
                    value.anchor_sha256 for value in sorted(anchors, key=_anchor_sort_key)
                ],
                "schema_version": "r4b_historical_census_anchor_set_v2",
            }
        )
    ).hexdigest()


def _source_row_sha256(row: Mapping[str, str]) -> str:
    if tuple(row) != _RECOMMENDATION_COLUMNS:
        raise HistoricalThreeFamilyCensusErrorV2(
            "source recommendation row order differs from the exact producer schema"
        )
    return hashlib.sha256(
        _SOURCE_ROW_DOMAIN
        + canonical_json_line(
            {
                "columns": list(_RECOMMENDATION_COLUMNS),
                "schema_version": "v1a_amendment_1_recommendation_source_row_v2",
                "values": [row[name] for name in _RECOMMENDATION_COLUMNS],
            }
        )
    ).hexdigest()


def _compact_target_slice_sha256(
    *,
    domain: bytes,
    representation: str,
    dataset_sha256: str,
    symbol: str,
    first_open_ms: int,
    final_open_ms: int,
    row_count: int,
) -> str:
    """Bind an exact arithmetic slice through its authenticated dataset root."""

    _validate_sha256(dataset_sha256, "compact target dataset_sha256")
    return hashlib.sha256(
        domain
        + canonical_json_line(
            {
                "dataset_sha256": dataset_sha256,
                "final_close_ms": final_open_ms + FIVE_MINUTE_MS_V2 - 1,
                "final_open_ms": final_open_ms,
                "first_open_ms": first_open_ms,
                "historical_receipt_policy": "RECEIPT_EQUALS_CLOSED_KLINE_CLOSE_TIME",
                "interval": "5m",
                "market": "futures",
                "representation": representation,
                "row_count": row_count,
                "schema_version": "r4b_historical_numeric_dataset_root_slice_v2",
                "symbol": symbol,
            }
        )
    ).hexdigest()


def _compact_cross_path_sha256(
    *,
    dataset_sha256: str,
    manifest_sha256: str,
    symbol: str,
    first_open_ms: int,
    final_open_ms: int,
    row_count: int,
) -> str:
    _validate_sha256(dataset_sha256, "compact cross dataset_sha256")
    _validate_sha256(manifest_sha256, "compact cross manifest_sha256")
    return hashlib.sha256(
        _CROSS_NUMERIC_PATH_DOMAIN
        + canonical_json_line(
            {
                "dataset_sha256": dataset_sha256,
                "final_close_ms": final_open_ms + FIVE_MINUTE_MS_V2 - 1,
                "final_open_ms": final_open_ms,
                "first_open_ms": first_open_ms,
                "historical_receipt_policy": "RECEIPT_EQUALS_CLOSED_KLINE_CLOSE_TIME",
                "interval": "5m",
                "manifest_sha256": manifest_sha256,
                "market": "futures",
                "representation": "CROSS_CLOSE_PATH_DATASET_ROOT_WINDOW",
                "row_count": row_count,
                "schema_version": "r4b_historical_cross_numeric_peer_path_v2",
                "symbol": symbol,
            }
        )
    ).hexdigest()


def _compact_cross_input_sha256(
    *,
    target_symbol: str,
    final_open_ms: int,
    peer_path_sha256s: tuple[tuple[str, str], ...],
) -> str:
    ordered = tuple(
        sorted(peer_path_sha256s, key=lambda value: value[0].encode("utf-8"))
    )
    if (
        len(ordered) != HISTORICAL_CROSS_SECTIONAL_7ASSET_PEER_COUNT_V2
        or target_symbol in {symbol for symbol, _ in ordered}
        or len({symbol for symbol, _ in ordered}) != len(ordered)
    ):
        raise HistoricalThreeFamilyCensusErrorV2(
            "compact cross input requires six unique target-excluded peer roots"
        )
    for _symbol, digest in ordered:
        _validate_sha256(digest, "compact cross peer path_sha256")
    return hashlib.sha256(
        _CROSS_NUMERIC_INPUT_DOMAIN
        + canonical_json_line(
            {
                "final_close_ms": final_open_ms + FIVE_MINUTE_MS_V2 - 1,
                "final_open_ms": final_open_ms,
                "historical_receipt_policy": "RECEIPT_EQUALS_CLOSED_KLINE_CLOSE_TIME",
                "interval": "5m",
                "market": "futures",
                "peer_paths": [
                    {"path_sha256": digest, "symbol": symbol}
                    for symbol, digest in ordered
                ],
                "representation": "TARGET_EXCLUDED_DATASET_ROOT_PEER_WINDOWS",
                "schema_version": "r4b_historical_cross_numeric_input_v2",
                "target_symbol": target_symbol,
            }
        )
    ).hexdigest()


def _median_decimal_v2(values: tuple[Decimal, ...]) -> Decimal:
    if not values:
        raise HistoricalThreeFamilyCensusErrorV2(
            "compact cross median requires at least one value"
        )
    ordered = tuple(sorted(values))
    midpoint = len(ordered) // 2
    if len(ordered) % 2:
        return ordered[midpoint]
    with localcontext(protocol_decimal_context_v2()):
        return (ordered[midpoint - 1] + ordered[midpoint]) / Decimal(2)


def _family_c_source_evidence_sha256(
    *,
    dataset_sha256: str,
    manifest_sha256: str,
    symbol: str,
    bar_open_ms: int,
    bar_close_ms: int,
    close: Decimal,
) -> str:
    return hashlib.sha256(
        _FAMILY_C_SOURCE_DOMAIN
        + canonical_json_line(
            {
                "bar_close_ms": bar_close_ms,
                "bar_open_ms": bar_open_ms,
                "close": str(close),
                "dataset_sha256": dataset_sha256,
                "event_time_ms": bar_close_ms,
                "historical_receipt_policy": "RECEIPT_EQUALS_CLOSED_KLINE_CLOSE_TIME",
                "interval": "5m",
                "manifest_sha256": manifest_sha256,
                "market": "futures",
                "receipt_time_ms": bar_close_ms,
                "schema_version": "r4b_historical_family_c_dataset_member_v2",
                "symbol": symbol,
            }
        )
    ).hexdigest()


def _contract_authority_v2(
    *,
    experiment_contract_sha256: str,
    topology_contract_sha256: str,
    code_freeze_manifest_sha256: str,
    workspace_root: str | Path | None,
) -> HistoricalContractAuthorityV2:
    _validate_sha256(experiment_contract_sha256, "experiment_contract_sha256")
    _validate_sha256(topology_contract_sha256, "topology_contract_sha256")
    _validate_sha256(code_freeze_manifest_sha256, "code_freeze_manifest_sha256")
    verified = workspace_root is not None
    if workspace_root is not None:
        root = Path(workspace_root).resolve()
        if not root.is_dir():
            raise HistoricalThreeFamilyCensusErrorV2(
                "workspace_root must be an existing directory"
            )
        for relative_path, expected_sha256, label in (
            (
                _EXPERIMENT_CONTRACT_RELATIVE_PATH,
                experiment_contract_sha256,
                "experiment contract",
            ),
            (
                _TOPOLOGY_CONTRACT_RELATIVE_PATH,
                topology_contract_sha256,
                "topology pre-outcome amendment",
            ),
        ):
            path = root / Path(relative_path)
            raw = _read_required_bytes_v2(path, label=label)
            if _sha256_bytes(raw) != expected_sha256:
                raise HistoricalThreeFamilyCensusErrorV2(
                    f"{label} hash differs from the caller-bound contract"
                )
    return HistoricalContractAuthorityV2(
        experiment_contract_sha256=experiment_contract_sha256,
        experiment_contract_relative_path=_EXPERIMENT_CONTRACT_RELATIVE_PATH,
        code_freeze_manifest_sha256=code_freeze_manifest_sha256,
        topology_contract_sha256=topology_contract_sha256,
        topology_contract_relative_path=_TOPOLOGY_CONTRACT_RELATIVE_PATH,
        workspace_files_verified=verified,
    )


def _contract_authority_document_v2(
    value: HistoricalContractAuthorityV2,
) -> dict[str, object]:
    _validate_sha256(
        value.experiment_contract_sha256,
        "contract authority experiment_contract_sha256",
    )
    _validate_sha256(
        value.topology_contract_sha256,
        "contract authority topology_contract_sha256",
    )
    _validate_sha256(
        value.code_freeze_manifest_sha256,
        "contract authority code_freeze_manifest_sha256",
    )
    if (
        value.experiment_contract_relative_path
        != _EXPERIMENT_CONTRACT_RELATIVE_PATH
        or value.topology_contract_relative_path != _TOPOLOGY_CONTRACT_RELATIVE_PATH
        or type(value.workspace_files_verified) is not bool
    ):
        raise HistoricalThreeFamilyCensusErrorV2(
            "contract authority path or verification state differs"
        )
    return {
        "code_freeze": {
            "self_discovered": False,
            "sha256": value.code_freeze_manifest_sha256,
        },
        "experiment": {
            "relative_path": value.experiment_contract_relative_path,
            "sha256": value.experiment_contract_sha256,
            "workspace_file_verified": value.workspace_files_verified,
        },
        "topology_preoutcome_amendment": {
            "relative_path": value.topology_contract_relative_path,
            "sha256": value.topology_contract_sha256,
            "workspace_file_verified": value.workspace_files_verified,
        },
    }


def _validate_numeric_provenance_v2(
    value: HistoricalNumericRepresentationProvenanceV2,
    *,
    processed_assets: set[str],
) -> None:
    expected_processed = tuple(asset for asset in _ASSETS if asset in processed_assets)
    if (
        type(value) is not HistoricalNumericRepresentationProvenanceV2
        or value.rule_version != HISTORICAL_NUMERIC_PRECOMPUTE_RULE_VERSION_V2
        or value.numeric_representation_only is not True
        or value.calculation_authority is not False
        or value.outcome_used is not False
        or tuple(asset for asset, _digest in value.r3_cache_sha256s) != _ASSETS
        or tuple(asset for asset, _digest in value.target_cache_sha256s)
        != expected_processed
        or tuple(asset for asset, _digest in value.cross_cache_sha256s)
        != expected_processed
    ):
        raise HistoricalThreeFamilyCensusErrorV2(
            "numeric precompute provenance differs from its bounded representation role"
        )
    for _asset, digest in (
        *value.r3_cache_sha256s,
        *value.target_cache_sha256s,
        *value.cross_cache_sha256s,
    ):
        _validate_sha256(digest, "numeric precompute cache root")


def _numeric_provenance_document_v2(
    value: HistoricalNumericRepresentationProvenanceV2,
) -> dict[str, object]:
    return {
        "calculation_authority": value.calculation_authority,
        "cross_cache_sha256s": dict(value.cross_cache_sha256s),
        "numeric_representation_only": value.numeric_representation_only,
        "outcome_used": value.outcome_used,
        "r3_cache_sha256s": dict(value.r3_cache_sha256s),
        "rule_version": value.rule_version,
        "sequential_target_cache_lifecycle": True,
        "target_cache_sha256s": dict(value.target_cache_sha256s),
    }


def _replay_audit_document(value: HistoricalSourceReplayAuditV2) -> dict[str, object]:
    return {
        "anchor_rows": value.anchor_rows,
        "anchor_set_sha256": value.anchor_set_sha256,
        "futures_input_sha256s": dict(value.futures_input_sha256s),
        "recommendation_rows": value.recommendation_rows,
        "recommendations_sha256": value.recommendations_sha256,
        "run_manifest_sha256": value.run_manifest_sha256,
        "split": value.split,
    }


def _kline_authority_document(
    value: HistoricalFuturesKlineAuthorityV2,
) -> dict[str, object]:
    return {
        "asset": value.asset,
        "data_sha256": value.data_sha256,
        "first_open_time_ms": value.first_open_time_ms,
        "last_close_time_ms": value.last_close_time_ms,
        "manifest_sha256": value.manifest_sha256,
        "relative_data_path": value.relative_data_path,
        "row_count": value.row_count,
        "symbol": value.symbol,
    }


def _read_recommendations_bytes_v2(path: Path) -> bytes:
    if path.name != "recommendations.csv":
        raise HistoricalThreeFamilyCensusErrorV2(
            "recommendation reader accepts recommendations.csv only"
        )
    return _read_required_bytes_v2(path, label="recommendations.csv")


def _read_required_bytes_v2(path: Path, *, label: str) -> bytes:
    if not path.is_file() or path.is_symlink():
        raise HistoricalThreeFamilyCensusErrorV2(f"{label} must be a regular file")
    try:
        return path.read_bytes()
    except OSError as exc:
        raise HistoricalThreeFamilyCensusErrorV2(f"cannot read {label}") from exc


def _decode_json_object_v2(raw: bytes, *, label: str) -> dict[str, object]:
    def reject_duplicates(pairs: list[tuple[str, object]]) -> dict[str, object]:
        result: dict[str, object] = {}
        for key, value in pairs:
            if key in result:
                raise HistoricalThreeFamilyCensusErrorV2(
                    f"{label} contains duplicate JSON key {key!r}"
                )
            result[key] = value
        return result

    try:
        value = json.loads(raw, object_pairs_hook=reject_duplicates)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise HistoricalThreeFamilyCensusErrorV2(
            f"{label} is not valid UTF-8 JSON"
        ) from exc
    return _require_json_dict(value, label)


def _require_json_dict(value: object, label: str) -> dict[str, object]:
    if type(value) is not dict or any(type(key) is not str for key in value):
        raise HistoricalThreeFamilyCensusErrorV2(
            f"{label} must be an exact JSON object"
        )
    return cast(dict[str, object], value)


def _parse_exact_bool(value: str, label: str) -> bool:
    if value == "True":
        return True
    if value == "False":
        return False
    raise HistoricalThreeFamilyCensusErrorV2(f"{label} must be exact True or False")


def _parse_unsigned_int(value: str, label: str) -> int:
    if not value or not value.isascii() or not value.isdigit():
        raise HistoricalThreeFamilyCensusErrorV2(
            f"{label} must be an unsigned decimal integer"
        )
    parsed = int(value)
    if parsed > _JCS_SAFE_INTEGER_MAX:
        raise HistoricalThreeFamilyCensusErrorV2(
            f"{label} exceeds the JCS-safe integer range"
        )
    return parsed


def _parse_decimal(value: str, label: str) -> Decimal:
    if not value or value != value.strip():
        raise HistoricalThreeFamilyCensusErrorV2(f"{label} must be a finite Decimal")
    try:
        parsed = Decimal(value)
    except InvalidOperation as exc:
        raise HistoricalThreeFamilyCensusErrorV2(
            f"{label} must be a finite Decimal"
        ) from exc
    if not parsed.is_finite():
        raise HistoricalThreeFamilyCensusErrorV2(f"{label} must be a finite Decimal")
    return parsed


def _parse_optional_decimal(value: str, label: str) -> Decimal | None:
    return None if value == "" else _parse_decimal(value, label)


def _require_source_event_id(value: str, label: str) -> str:
    if len(value) != 24 or any(character not in "0123456789abcdef" for character in value):
        raise HistoricalThreeFamilyCensusErrorV2(
            f"{label} must be 24 lowercase hexadecimal characters"
        )
    return value


def _validate_sha256(value: str, label: str) -> None:
    if (
        type(value) is not str
        or len(value) != 64
        or any(character not in "0123456789abcdef" for character in value)
    ):
        raise HistoricalThreeFamilyCensusErrorV2(
            f"{label} must be a lowercase SHA-256 digest"
        )


def _require_sha256(value: object, label: str) -> str:
    if type(value) is not str:
        raise HistoricalThreeFamilyCensusErrorV2(
            f"{label} must be a lowercase SHA-256 digest"
        )
    _validate_sha256(value, label)
    return value


def _validate_maximum_anchors(value: int | None) -> None:
    if value is not None and (type(value) is not int or value < 1):
        raise HistoricalThreeFamilyCensusErrorV2(
            "maximum_anchors must be a positive integer when supplied"
        )


def _require_unique_anchor_identities(
    anchors: tuple[HistoricalRecommendationAnchorV2, ...],
) -> None:
    identities = tuple(
        (anchor.asset, anchor.primary_direction.value, anchor.decision_time_ms)
        for anchor in anchors
    )
    if len(set(identities)) != len(identities):
        raise HistoricalThreeFamilyCensusErrorV2(
            "anchor census contains duplicate (asset, direction, decision_time_ms)"
        )
    if len({anchor.anchor_sha256 for anchor in anchors}) != len(anchors):
        raise HistoricalThreeFamilyCensusErrorV2(
            "anchor census contains duplicate canonical anchor hashes"
        )


def _anchor_sort_key(value: HistoricalRecommendationAnchorV2) -> tuple[int, int, bytes, str, str]:
    return (
        HISTORICAL_THREE_FAMILY_CENSUS_SPLITS_V2.index(value.split),
        value.decision_time_ms,
        value.asset.encode("utf-8"),
        value.primary_direction.value,
        value.source_event_id,
    )


def _consensus_row_sort_key(
    value: HistoricalConsensusCensusRowV2,
) -> tuple[int, int, bytes, str, str]:
    return (
        HISTORICAL_THREE_FAMILY_CENSUS_SPLITS_V2.index(value.split),
        value.decision_time_ms,
        value.asset.encode("utf-8"),
        value.primary_direction,
        value.source_event_id,
    )


def _sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _fresh_output_target_v2(output_dir: str | Path) -> Path:
    target = Path(output_dir).resolve()
    if os.path.lexists(target):
        raise HistoricalThreeFamilyCensusErrorV2(
            "census output directory must not already exist"
        )
    try:
        target.parent.mkdir(parents=True, exist_ok=True)
    except OSError as exc:
        raise HistoricalThreeFamilyCensusErrorV2(
            "cannot create census output parent"
        ) from exc
    if os.path.lexists(target):
        raise HistoricalThreeFamilyCensusErrorV2(
            "census output directory appeared during validation"
        )
    return target


def _write_fsynced_bytes_v2(path: Path, payload: bytes) -> None:
    if type(payload) is not bytes or b"\r" in payload:
        raise HistoricalThreeFamilyCensusErrorV2(
            "census artifact payload must be exact LF-only bytes"
        )
    try:
        payload.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise HistoricalThreeFamilyCensusErrorV2(
            "census artifact payload must be UTF-8"
        ) from exc
    try:
        with path.open("xb") as handle:
            if handle.write(payload) != len(payload):
                raise OSError("short artifact write")
            handle.flush()
            try:
                os.fsync(handle.fileno())
            except OSError as exc:
                unsupported = {
                    errno.EINVAL,
                    getattr(errno, "ENOTSUP", errno.EINVAL),
                    getattr(errno, "EOPNOTSUPP", errno.EINVAL),
                }
                if exc.errno not in unsupported:
                    raise
    except OSError as exc:
        raise HistoricalThreeFamilyCensusErrorV2(
            f"failed to write census artifact {path.name}"
        ) from exc


def _publish_artifacts_v2(*, target: Path, payloads: Mapping[str, bytes]) -> None:
    if set(payloads) != _OUTPUT_NAMES or target != target.resolve() or os.path.lexists(target):
        raise HistoricalThreeFamilyCensusErrorV2(
            "census publication requires a fresh target and exact three-file payload"
        )
    temporary = Path(
        tempfile.mkdtemp(prefix=f".{target.name}.tmp-", dir=target.parent)
    ).resolve()
    if temporary.parent != target.parent:
        shutil.rmtree(temporary, ignore_errors=True)
        raise HistoricalThreeFamilyCensusErrorV2(
            "census temporary directory escaped its intended parent"
        )
    published = False
    try:
        for name in sorted(_OUTPUT_NAMES):
            _write_fsynced_bytes_v2(temporary / name, payloads[name])
        _verify_artifact_directory_v2(temporary, payloads=payloads)
        if os.path.lexists(target):
            raise HistoricalThreeFamilyCensusErrorV2(
                "census output target appeared before publication"
            )
        os.rename(temporary, target)
        published = True
        _verify_artifact_directory_v2(target, payloads=payloads)
    except Exception as exc:
        incomplete = target if published else temporary
        if incomplete.parent == target.parent and os.path.lexists(incomplete):
            if incomplete.is_symlink():
                incomplete.unlink()
            else:
                shutil.rmtree(incomplete, ignore_errors=True)
        if isinstance(exc, HistoricalThreeFamilyCensusErrorV2):
            raise
        raise HistoricalThreeFamilyCensusErrorV2(
            "failed to publish census artifacts atomically"
        ) from exc


def _verify_artifact_directory_v2(
    root: Path,
    *,
    payloads: Mapping[str, bytes],
) -> None:
    entries = tuple(root.iterdir())
    if (
        {entry.name for entry in entries} != _OUTPUT_NAMES
        or any(not entry.is_file() or entry.is_symlink() for entry in entries)
    ):
        raise HistoricalThreeFamilyCensusErrorV2(
            "published census directory has an unexpected file set"
        )
    for name in _OUTPUT_NAMES:
        raw = (root / name).read_bytes()
        if raw != payloads[name] or b"\r" in raw:
            raise HistoricalThreeFamilyCensusErrorV2(
                f"published census artifact differs: {name}"
            )


def _default_replay_dirs_v2(workspace_root: Path) -> dict[str, Path]:
    base = (
        workspace_root
        / "artifacts"
        / "backtest"
        / "2026-07-20-indicator-discriminator-v1a-7asset"
    )
    return {
        "development": base / "replay-development-amendment-1",
        "validation": base / "replay-validation-amendment-1",
        "retrospective_test": base / "replay-retrospective-amendment-1",
    }


def _parser_v2() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run the outcome-blind historical three-family consensus census."
    )
    parser.add_argument("--workspace-root", type=Path, default=Path.cwd())
    parser.add_argument("--data-dir", type=Path)
    parser.add_argument("--development-replay-dir", type=Path)
    parser.add_argument("--validation-replay-dir", type=Path)
    parser.add_argument("--retrospective-replay-dir", type=Path)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--experiment-contract-sha256", required=True)
    parser.add_argument("--topology-contract-sha256", required=True)
    parser.add_argument("--code-freeze-manifest-sha256", required=True)
    parser.add_argument("--maximum-anchors", type=int)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    """Standalone deterministic CLI; central alert/order wiring stays untouched."""

    parser = _parser_v2()
    args = parser.parse_args(argv)
    workspace = args.workspace_root.resolve()
    defaults = _default_replay_dirs_v2(workspace)
    replay_dirs = {
        "development": args.development_replay_dir or defaults["development"],
        "validation": args.validation_replay_dir or defaults["validation"],
        "retrospective_test": (
            args.retrospective_replay_dir or defaults["retrospective_test"]
        ),
    }
    try:
        run_historical_three_family_census_v2(
            replay_dirs=replay_dirs,
            data_dir=args.data_dir or workspace / "data" / "backtest",
            output_dir=args.output_dir,
            experiment_contract_sha256=args.experiment_contract_sha256,
            topology_contract_sha256=args.topology_contract_sha256,
            code_freeze_manifest_sha256=args.code_freeze_manifest_sha256,
            workspace_root=workspace,
            maximum_anchors=args.maximum_anchors,
        )
    except HistoricalThreeFamilyCensusErrorV2 as exc:
        parser.error(str(exc))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
