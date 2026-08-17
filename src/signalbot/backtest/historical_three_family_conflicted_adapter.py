"""Pre-outcome authorization adapter for conflicted three-family majorities.

The adapter authenticates an already completed, outcome-blind census and
selects the separately predeclared two-versus-one population.  It never reads
market outcomes and never changes the clean source admission.
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

from signalbot.backtest.historical_three_family_census import (
    HistoricalConsensusCensusRowV2,
)
from signalbot.backtest.historical_three_family_outcomes import (
    HistoricalThreeFamilyFixedHorizonErrorV2,
    LoadedHistoricalConsensusV2,
    load_authenticated_historical_consensus_v2,
)
from signalbot.domain.enums import Direction, SignalFamily
from signalbot.r4b_v2.canonical import canonical_json_line
from signalbot.r4b_v2.research.historical_three_family_outcome_audit import (
    HISTORICAL_THREE_FAMILY_OUTCOME_HORIZONS_BARS_V2,
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
    build_historical_execution_contract_v2,
)

HISTORICAL_THREE_FAMILY_CONFLICTED_ADAPTER_PROTOCOL_V1: Final = (
    "historical_three_family_conflicted_comparator_adapter_v1_2026-07-20"
)
HISTORICAL_THREE_FAMILY_CONFLICTED_ADAPTER_RULE_VERSION_V1: Final = (
    "R4B_CAUSAL_V2.4.1_HISTORICAL_CONFLICTED_2_VS_1_ADAPTER_V1_FROZEN"
)
HISTORICAL_THREE_FAMILY_CONFLICTED_ADAPTER_SCHEMA_VERSION_V1: Final = 1
HISTORICAL_THREE_FAMILY_CONFLICTED_ADAPTER_FREEZE_SCHEMA_V1: Final = (
    "r4b_historical_three_family_conflicted_adapter_all_source_freeze_v1"
)
HISTORICAL_THREE_FAMILY_CONFLICTED_ADAPTER_FREEZE_SCOPE_V1: Final = (
    "ALL_SRC_SIGNALBOT_PYTHON_PLUS_CONFLICTED_ADAPTER_CONTRACT_AND_ENVIRONMENT_PINS"
)
HISTORICAL_THREE_FAMILY_CONFLICTED_TOPOLOGY_VERSION_V1: Final = (
    "PRIMARY_SUPPORTING_CONFLICTED_2_VS_1_V1_FROZEN"
)
HISTORICAL_THREE_FAMILY_CONFLICTED_ADAPTER_CONTRACT_PATH_V1: Final = (
    "docs/r4b-v2-historical-three-family-conflicted-comparator-adapter.md"
)
_EXPERIMENT_CONTRACT_PATH: Final = (
    "docs/r4b-v2-historical-three-family-consensus-experiment.md"
)
_TOPOLOGY_CONTRACT_PATH: Final = (
    "docs/r4b-v2-historical-three-family-topology-preoutcome-amendment.md"
)
_ENVIRONMENT_PIN_PATHS: Final = (".python-version", "pyproject.toml")
_SHA256_LENGTH: Final = 64
_JCS_SAFE_INTEGER_MAX: Final = 2**53 - 1
_MAX_CENSUS_ROWS: Final = 100_000
_PUBLISHED_NAMES: Final = frozenset(
    {"conflicted_events.csv", "authorization.json", "manifest.json"}
)


class HistoricalThreeFamilyConflictedAdapterErrorV1(ValueError):
    """Raised when the separate pre-outcome adapter contract is violated."""


@dataclass(frozen=True, slots=True)
class HistoricalConflictedComparatorEventV1:
    """One source-side conflicted majority authorized by a separate envelope."""

    split: str
    asset: str
    symbol: str
    event_id: str
    anchor_sha256: str
    source_event_id: str
    source_row_sha256: str
    source_replay_manifest_sha256: str
    source_census_row_sha256: str
    payload_sha256: str
    canonical_consensus_sha256: str
    topology_sha256: str
    canonical_topology_sha256: str
    topology_contract_sha256: str
    topology_class: str
    topology_majority_direction: str
    primary_family: str
    primary_direction: str
    decision_time_ms: int
    decision_price: str
    invalidation: str | None
    atr: str
    directional_agreement_micros: int
    price_calculation_sha256: str
    price_source_slice_sha256: str
    participation_calculation_sha256: str
    participation_source_slice_sha256: str
    cross_section_calculation_sha256: str
    cross_section_source_slice_sha256: str
    cross_peer_set_root_sha256: str
    cross_peer_input_sha256: str
    execution_contract_sha256: str
    census_manifest_sha256: str
    experiment_contract_sha256: str
    adapter_contract_sha256: str
    code_freeze_manifest_sha256: str
    horizons_bars: tuple[int, ...] = HISTORICAL_THREE_FAMILY_OUTCOME_HORIZONS_BARS_V2
    topology_version: str = HISTORICAL_THREE_FAMILY_CONFLICTED_TOPOLOGY_VERSION_V1
    adapter_rule_version: str = HISTORICAL_THREE_FAMILY_CONFLICTED_ADAPTER_RULE_VERSION_V1
    display_grade: str = (
        HistoricalThreeFamilyDisplayGradeV2.CONFLICTED_MAJORITY_UNCALIBRATED.value
    )
    source_admitted: Literal[False] = False
    source_outcome_authorized: Literal[False] = False
    adapter_outcome_authorized: Literal[True] = True
    historical_only: Literal[True] = True
    probability: Literal[False] = False
    probability_calibrated: Literal[False] = False
    promoting: Literal[False] = False
    order_placement: Literal[False] = False
    changes_source_admission: Literal[False] = False
    changes_source_event_id: Literal[False] = False
    clean_population_pooled: Literal[False] = False

    def __post_init__(self) -> None:
        _validate_event(self)

    @property
    def horizon_minutes(self) -> tuple[int, ...]:
        return tuple(value * 5 for value in self.horizons_bars)


@dataclass(frozen=True, slots=True)
class LoadedHistoricalConflictedComparatorV1:
    """Authenticated pre-outcome adapter population and its frozen authorities."""

    source: LoadedHistoricalConsensusV2
    adapter_contract_sha256: str
    code_freeze_manifest_sha256: str
    events: tuple[HistoricalConflictedComparatorEventV1, ...]
    authorization_sha256: str
    historical_only: Literal[True] = True
    outcome_data_read: Literal[False] = False
    probability: Literal[False] = False
    probability_calibrated: Literal[False] = False
    promoting: Literal[False] = False
    order_placement: Literal[False] = False
    changes_source_admission: Literal[False] = False
    clean_population_pooled: Literal[False] = False


@dataclass(frozen=True, slots=True)
class HistoricalConflictedAdapterArtifactsV1:
    output_dir: Path
    events_sha256: str
    authorization_sha256: str
    manifest_sha256: str
    event_count: int


@dataclass(frozen=True, slots=True)
class LoadedHistoricalConflictedAdapterArtifactsV1:
    """Exact reauthentication of the three published adapter artifacts."""

    authorization: LoadedHistoricalConflictedComparatorV1
    artifact_dir: Path
    events_sha256: str
    adapter_manifest_sha256: str
    historical_only: Literal[True] = True
    outcome_data_read: Literal[False] = False


def build_historical_conflicted_adapter_code_freeze_manifest_v1(
    *,
    workspace_root: str | Path,
    census_manifest_sha256: str,
    experiment_contract_sha256: str,
    topology_contract_sha256: str,
    adapter_contract_sha256: str,
) -> bytes:
    """Build deterministic all-source freeze bytes without reading outcomes."""

    for value, label in (
        (census_manifest_sha256, "census manifest"),
        (experiment_contract_sha256, "experiment contract"),
        (topology_contract_sha256, "topology contract"),
        (adapter_contract_sha256, "adapter contract"),
    ):
        _require_sha256(value, label)
    root = Path(workspace_root).resolve()
    paths = _freeze_relative_paths(root)
    hashes = {path: _sha256_file(_resolve_relative(root, path), path) for path in paths}
    if hashes[HISTORICAL_THREE_FAMILY_CONFLICTED_ADAPTER_CONTRACT_PATH_V1] != (
        adapter_contract_sha256
    ):
        raise HistoricalThreeFamilyConflictedAdapterErrorV1(
            "adapter contract file differs from the supplied frozen hash"
        )
    if hashes[_EXPERIMENT_CONTRACT_PATH] != experiment_contract_sha256:
        raise HistoricalThreeFamilyConflictedAdapterErrorV1(
            "experiment contract file differs from the supplied frozen hash"
        )
    if hashes[_TOPOLOGY_CONTRACT_PATH] != topology_contract_sha256:
        raise HistoricalThreeFamilyConflictedAdapterErrorV1(
            "topology amendment file differs from the supplied frozen hash"
        )
    return canonical_json_line(
        {
            "adapter_contract_sha256": adapter_contract_sha256,
            "census_manifest_sha256": census_manifest_sha256,
            "experiment_contract_sha256": experiment_contract_sha256,
            "file_sha256": hashes,
            "freeze_scope": HISTORICAL_THREE_FAMILY_CONFLICTED_ADAPTER_FREEZE_SCOPE_V1,
            "historical_only": True,
            "outcome_data_read": False,
            "purpose": "PRE_OUTCOME_HISTORICAL_CONFLICTED_2_VS_1_ADAPTER_FREEZE",
            "schema_version": HISTORICAL_THREE_FAMILY_CONFLICTED_ADAPTER_FREEZE_SCHEMA_V1,
            "signalbot_python_source_count": sum(
                path.startswith("src/signalbot/") for path in paths
            ),
            "topology_contract_sha256": topology_contract_sha256,
        }
    )


def load_authorized_historical_conflicted_comparator_v1(
    *,
    consensus_path: str | Path,
    census_manifest_path: str | Path,
    expected_census_manifest_sha256: str,
    expected_experiment_contract_sha256: str,
    expected_topology_amendment_sha256: str,
    workspace_root: str | Path,
    adapter_contract_path: str | Path,
    expected_adapter_contract_sha256: str,
    code_freeze_manifest_path: str | Path,
    expected_code_freeze_manifest_sha256: str,
) -> LoadedHistoricalConflictedComparatorV1:
    """Authorize only source-side 2-vs-1 rows after every pre-outcome freeze."""

    for value, label in (
        (expected_adapter_contract_sha256, "adapter contract"),
        (expected_code_freeze_manifest_sha256, "code freeze manifest"),
    ):
        _require_sha256(value, label)
    root = Path(workspace_root).resolve()
    _authenticate_adapter_contract(
        root,
        adapter_contract_path,
        expected_adapter_contract_sha256,
    )
    _authenticate_code_freeze(
        root=root,
        manifest_path=code_freeze_manifest_path,
        expected_manifest_sha256=expected_code_freeze_manifest_sha256,
        census_manifest_sha256=expected_census_manifest_sha256,
        experiment_contract_sha256=expected_experiment_contract_sha256,
        topology_contract_sha256=expected_topology_amendment_sha256,
        adapter_contract_sha256=expected_adapter_contract_sha256,
    )
    try:
        source = load_authenticated_historical_consensus_v2(
            consensus_path,
            census_manifest_path,
            expected_census_manifest_sha256=expected_census_manifest_sha256,
            expected_experiment_contract_sha256=expected_experiment_contract_sha256,
            expected_topology_amendment_sha256=expected_topology_amendment_sha256,
        )
    except HistoricalThreeFamilyFixedHorizonErrorV2 as exc:
        raise HistoricalThreeFamilyConflictedAdapterErrorV1(
            "source census authentication failed"
        ) from exc
    raw = _read_bytes(Path(consensus_path).resolve(), "consensus.csv")
    if _sha256_bytes(raw) != source.consensus_sha256:
        raise HistoricalThreeFamilyConflictedAdapterErrorV1(
            "consensus changed after source authentication"
        )
    rows = _read_conflicted_source_rows(raw)
    events = tuple(
        sorted(
            (
                _adapt_conflicted_row(
                    row,
                    census_manifest_sha256=expected_census_manifest_sha256,
                    experiment_contract_sha256=expected_experiment_contract_sha256,
                    topology_contract_sha256=expected_topology_amendment_sha256,
                    adapter_contract_sha256=expected_adapter_contract_sha256,
                    code_freeze_manifest_sha256=expected_code_freeze_manifest_sha256,
                )
                for row in rows
                if row["conflicted_comparator_eligible"] == "true"
            ),
            key=lambda value: (value.split, value.asset, value.decision_time_ms, value.event_id),
        )
    )
    if len({event.event_id for event in events}) != len(events):
        raise HistoricalThreeFamilyConflictedAdapterErrorV1(
            "authorized conflicted population contains duplicate source event IDs"
        )
    authorization = _authorization_document(
        source=source,
        adapter_contract_sha256=expected_adapter_contract_sha256,
        code_freeze_manifest_sha256=expected_code_freeze_manifest_sha256,
        events=events,
    )
    authorization_sha = _sha256_bytes(canonical_json_line(authorization))
    return LoadedHistoricalConflictedComparatorV1(
        source=source,
        adapter_contract_sha256=expected_adapter_contract_sha256,
        code_freeze_manifest_sha256=expected_code_freeze_manifest_sha256,
        events=events,
        authorization_sha256=authorization_sha,
    )


def canonical_historical_conflicted_event_v1(
    value: HistoricalConflictedComparatorEventV1,
) -> bytes:
    """Serialize one revalidated adapter event without changing source identity."""

    if type(value) is not HistoricalConflictedComparatorEventV1:
        raise HistoricalThreeFamilyConflictedAdapterErrorV1(
            "event must be an exact HistoricalConflictedComparatorEventV1"
        )
    _validate_event(value)
    return canonical_json_line(_event_document(value))


def publish_historical_conflicted_comparator_v1(
    loaded: LoadedHistoricalConflictedComparatorV1,
    output_dir: str | Path,
) -> HistoricalConflictedAdapterArtifactsV1:
    """Publish only pre-outcome authorization artifacts to a fresh directory."""

    if type(loaded) is not LoadedHistoricalConflictedComparatorV1:
        raise HistoricalThreeFamilyConflictedAdapterErrorV1(
            "publisher requires exact authenticated adapter input"
        )
    authorization_raw = canonical_historical_conflicted_authorization_v1(loaded)
    if _sha256_bytes(authorization_raw) != loaded.authorization_sha256:
        raise HistoricalThreeFamilyConflictedAdapterErrorV1(
            "loaded authorization hash differs from canonical content"
        )
    events_raw = _events_csv_bytes(loaded.events)
    manifest_raw = canonical_historical_conflicted_adapter_manifest_v1(
        loaded,
        events_sha256=_sha256_bytes(events_raw),
    )
    target = Path(output_dir).resolve()
    _publish(
        target,
        {
            "conflicted_events.csv": events_raw,
            "authorization.json": authorization_raw,
            "manifest.json": manifest_raw,
        },
    )
    return HistoricalConflictedAdapterArtifactsV1(
        output_dir=target,
        events_sha256=_sha256_bytes(events_raw),
        authorization_sha256=loaded.authorization_sha256,
        manifest_sha256=_sha256_bytes(manifest_raw),
        event_count=len(loaded.events),
    )


def canonical_historical_conflicted_authorization_v1(
    loaded: LoadedHistoricalConflictedComparatorV1,
) -> bytes:
    """Serialize the complete adapter authorization envelope."""

    if type(loaded) is not LoadedHistoricalConflictedComparatorV1:
        raise HistoricalThreeFamilyConflictedAdapterErrorV1(
            "authorization serializer requires exact authenticated input"
        )
    return canonical_json_line(
        _authorization_document(
            source=loaded.source,
            adapter_contract_sha256=loaded.adapter_contract_sha256,
            code_freeze_manifest_sha256=loaded.code_freeze_manifest_sha256,
            events=loaded.events,
        )
    )


def canonical_historical_conflicted_adapter_manifest_v1(
    loaded: LoadedHistoricalConflictedComparatorV1,
    *,
    events_sha256: str,
) -> bytes:
    """Serialize the exact three-artifact adapter manifest."""

    if type(loaded) is not LoadedHistoricalConflictedComparatorV1:
        raise HistoricalThreeFamilyConflictedAdapterErrorV1(
            "manifest serializer requires exact authenticated input"
        )
    _require_sha256(events_sha256, "adapter events")
    return canonical_json_line(
        {
            "adapter_contract_sha256": loaded.adapter_contract_sha256,
            "authorization_sha256": loaded.authorization_sha256,
            "census_manifest_sha256": loaded.source.census_manifest_sha256,
            "clean_population_pooled": False,
            "code_freeze_manifest_sha256": loaded.code_freeze_manifest_sha256,
            "event_count": len(loaded.events),
            "historical_only": True,
            "order_placement": False,
            "outcome_data_read": False,
            "outputs": {
                "authorization.json": loaded.authorization_sha256,
                "conflicted_events.csv": events_sha256,
            },
            "probability": False,
            "probability_calibrated": False,
            "promoting": False,
            "protocol": HISTORICAL_THREE_FAMILY_CONFLICTED_ADAPTER_PROTOCOL_V1,
            "schema_version": HISTORICAL_THREE_FAMILY_CONFLICTED_ADAPTER_SCHEMA_VERSION_V1,
            "topology_contract_sha256": loaded.source.topology_amendment_sha256,
        }
    )


def load_authenticated_historical_conflicted_adapter_artifacts_v1(
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
    code_freeze_manifest_path: str | Path,
    expected_code_freeze_manifest_sha256: str,
) -> LoadedHistoricalConflictedAdapterArtifactsV1:
    """Rebuild authority and require byte-exact equality for all three artifacts."""

    _require_sha256(expected_adapter_manifest_sha256, "adapter manifest")
    loaded = load_authorized_historical_conflicted_comparator_v1(
        consensus_path=consensus_path,
        census_manifest_path=census_manifest_path,
        expected_census_manifest_sha256=expected_census_manifest_sha256,
        expected_experiment_contract_sha256=expected_experiment_contract_sha256,
        expected_topology_amendment_sha256=expected_topology_amendment_sha256,
        workspace_root=workspace_root,
        adapter_contract_path=adapter_contract_path,
        expected_adapter_contract_sha256=expected_adapter_contract_sha256,
        code_freeze_manifest_path=code_freeze_manifest_path,
        expected_code_freeze_manifest_sha256=expected_code_freeze_manifest_sha256,
    )
    source = Path(adapter_artifact_dir).resolve()
    try:
        names = {path.name for path in source.iterdir()}
    except OSError as exc:
        raise HistoricalThreeFamilyConflictedAdapterErrorV1(
            "cannot enumerate adapter artifact directory"
        ) from exc
    if names != _PUBLISHED_NAMES:
        raise HistoricalThreeFamilyConflictedAdapterErrorV1(
            "adapter artifact directory must contain exactly the frozen three files"
        )
    actual_events = _read_bytes(source / "conflicted_events.csv", "adapter events")
    actual_authorization = _read_bytes(source / "authorization.json", "adapter authorization")
    actual_manifest = _read_bytes(source / "manifest.json", "adapter manifest")
    expected_events = _events_csv_bytes(loaded.events)
    expected_authorization = canonical_historical_conflicted_authorization_v1(loaded)
    expected_manifest = canonical_historical_conflicted_adapter_manifest_v1(
        loaded,
        events_sha256=_sha256_bytes(expected_events),
    )
    if actual_events != expected_events or actual_authorization != expected_authorization:
        raise HistoricalThreeFamilyConflictedAdapterErrorV1(
            "adapter events or authorization differ from reauthenticated source census"
        )
    if (
        _sha256_bytes(actual_manifest) != expected_adapter_manifest_sha256
        or actual_manifest != expected_manifest
    ):
        raise HistoricalThreeFamilyConflictedAdapterErrorV1(
            "adapter manifest differs from its external hash or canonical authority"
        )
    return LoadedHistoricalConflictedAdapterArtifactsV1(
        authorization=loaded,
        artifact_dir=source,
        events_sha256=_sha256_bytes(actual_events),
        adapter_manifest_sha256=expected_adapter_manifest_sha256,
    )


def _adapt_conflicted_row(
    row: Mapping[str, str],
    *,
    census_manifest_sha256: str,
    experiment_contract_sha256: str,
    topology_contract_sha256: str,
    adapter_contract_sha256: str,
    code_freeze_manifest_sha256: str,
) -> HistoricalConflictedComparatorEventV1:
    _validate_conflicted_source_row(row, topology_contract_sha256)
    return HistoricalConflictedComparatorEventV1(
        split=row["split"],
        asset=row["asset"],
        symbol=row["symbol"],
        event_id=row["event_id"],
        anchor_sha256=row["anchor_sha256"],
        source_event_id=row["source_event_id"],
        source_row_sha256=row["source_row_sha256"],
        source_replay_manifest_sha256=row["source_replay_manifest_sha256"],
        source_census_row_sha256=_sha256_bytes(canonical_json_line(dict(row))),
        payload_sha256=row["payload_sha256"],
        canonical_consensus_sha256=row["canonical_consensus_sha256"],
        topology_sha256=row["topology_sha256"],
        canonical_topology_sha256=row["canonical_topology_sha256"],
        topology_contract_sha256=row["topology_contract_sha256"],
        topology_class=row["topology_class"],
        topology_majority_direction=row["topology_majority_direction"],
        primary_family=row["primary_family"],
        primary_direction=row["primary_direction"],
        decision_time_ms=_parse_int(row["decision_time_ms"], "decision_time_ms"),
        decision_price=_decimal_text(row["price"], "price", positive=True),
        invalidation=(
            None
            if not row["invalidation"]
            else _decimal_text(row["invalidation"], "invalidation", positive=True)
        ),
        atr=_decimal_text(row["atr"], "atr", positive=False),
        directional_agreement_micros=_parse_int(
            row["directional_agreement_micros"], "directional_agreement_micros"
        ),
        price_calculation_sha256=row["price_calculation_sha256"],
        price_source_slice_sha256=row["price_source_slice_sha256"],
        participation_calculation_sha256=row["participation_calculation_sha256"],
        participation_source_slice_sha256=row["participation_source_slice_sha256"],
        cross_section_calculation_sha256=row["cross_section_calculation_sha256"],
        cross_section_source_slice_sha256=row["cross_section_source_slice_sha256"],
        cross_peer_set_root_sha256=row["cross_peer_set_root_sha256"],
        cross_peer_input_sha256=row["cross_peer_input_sha256"],
        execution_contract_sha256=row["execution_contract_sha256"],
        census_manifest_sha256=census_manifest_sha256,
        experiment_contract_sha256=experiment_contract_sha256,
        adapter_contract_sha256=adapter_contract_sha256,
        code_freeze_manifest_sha256=code_freeze_manifest_sha256,
    )


def _validate_conflicted_source_row(
    row: Mapping[str, str], topology_contract_sha256: str
) -> None:
    if (
        row["conflicted_comparator_eligible"] != "true"
        or row["conflicted_comparator_outcome_authorized"] != "false"
        or row["clean_primary_audit_eligible"] != "false"
        or row["admitted"] != "false"
        or row["status"] != "READY"
        or row["state_class"] != DirectionalStateClassV2.MIXED_OR_NEUTRAL_STATE.value
        or row["primary_relationship"] != "MIXED_OR_NEUTRAL"
        or row["topology_comparison_bucket"]
        != HistoricalThreeFamilyComparisonBucketV2.CONFLICTED_2_VS_1.value
        or row["topology_display_grade"]
        != HistoricalThreeFamilyDisplayGradeV2.CONFLICTED_MAJORITY_UNCALIBRATED.value
        or row["topology_rule_version"]
        != HISTORICAL_THREE_FAMILY_TOPOLOGY_RULE_VERSION_V2
        or row["topology_contract_sha256"] != topology_contract_sha256
        or row["rule_version"] != HISTORICAL_THREE_FAMILY_CONSENSUS_RULE_VERSION_V2
        or row["topology_majority_family_count"] != "2"
        or row["topology_opposing_family_count"] != "1"
        or row["topology_has_opposition"] != "true"
        or row["topology_primary_support_count"] != "2"
        or row["topology_primary_oppose_count"] != "1"
        or row["topology_primary_neutral_count"] != "0"
        or row["neutral_family_count"] != "0"
        or row["directional_denominator"] != "3"
    ):
        raise HistoricalThreeFamilyConflictedAdapterErrorV1(
            "source row is not the frozen source-side conflicted-majority topology"
        )
    directions = tuple(
        _parse_int(row[name], name)
        for name in ("price_direction", "participation_direction", "cross_section_direction")
    )
    if any(value not in (-1, 1) for value in directions):
        raise HistoricalThreeFamilyConflictedAdapterErrorV1(
            "conflicted source row requires three nonneutral READY leaf directions"
        )
    direction = row["primary_direction"]
    long_expected = {
        "topology": HistoricalThreeFamilyTopologyClassV2.CONFLICTED_BULLISH_2_1_0.value,
        "majority": HistoricalThreeFamilyMajorityDirectionV2.BULLISH.value,
        "family": SignalFamily.PULLBACK_LONG.value,
        "bullish": "2",
        "bearish": "1",
    }
    short_expected = {
        "topology": HistoricalThreeFamilyTopologyClassV2.CONFLICTED_BEARISH_1_2_0.value,
        "majority": HistoricalThreeFamilyMajorityDirectionV2.BEARISH.value,
        "family": SignalFamily.PULLBACK_SHORT.value,
        "bullish": "1",
        "bearish": "2",
    }
    expected = long_expected if direction == Direction.LONG.value else short_expected
    if direction not in {Direction.LONG.value, Direction.SHORT.value}:
        raise HistoricalThreeFamilyConflictedAdapterErrorV1("unsupported source side")
    actual = (
        row["topology_class"],
        row["topology_majority_direction"],
        row["primary_family"],
        row["bullish_family_count"],
        row["bearish_family_count"],
    )
    expected_tuple = (
        expected["topology"],
        expected["majority"],
        expected["family"],
        expected["bullish"],
        expected["bearish"],
    )
    if actual != expected_tuple or directions.count(1) != int(row["bullish_family_count"]):
        raise HistoricalThreeFamilyConflictedAdapterErrorV1(
            "source side, topology, or family counts are inconsistent"
        )


def _validate_event(value: HistoricalConflictedComparatorEventV1) -> None:
    hashes = (
        value.event_id,
        value.anchor_sha256,
        value.source_row_sha256,
        value.source_replay_manifest_sha256,
        value.source_census_row_sha256,
        value.payload_sha256,
        value.canonical_consensus_sha256,
        value.topology_sha256,
        value.canonical_topology_sha256,
        value.topology_contract_sha256,
        value.price_calculation_sha256,
        value.price_source_slice_sha256,
        value.participation_calculation_sha256,
        value.participation_source_slice_sha256,
        value.cross_section_calculation_sha256,
        value.cross_section_source_slice_sha256,
        value.cross_peer_set_root_sha256,
        value.cross_peer_input_sha256,
        value.execution_contract_sha256,
        value.census_manifest_sha256,
        value.experiment_contract_sha256,
        value.adapter_contract_sha256,
        value.code_freeze_manifest_sha256,
    )
    for digest in hashes:
        _require_sha256(digest, "event provenance")
    if not value.source_event_id:
        raise HistoricalThreeFamilyConflictedAdapterErrorV1(
            "source recommendation event ID must be retained"
        )
    if value.horizons_bars != HISTORICAL_THREE_FAMILY_OUTCOME_HORIZONS_BARS_V2:
        raise HistoricalThreeFamilyConflictedAdapterErrorV1(
            "adapter horizons differ from the public historical outcome owner"
        )
    if value.execution_contract_sha256 != (
        build_historical_execution_contract_v2().execution_contract_sha256
    ):
        raise HistoricalThreeFamilyConflictedAdapterErrorV1(
            "adapter event execution contract differs from the public owner"
        )
    if (
        value.topology_version != HISTORICAL_THREE_FAMILY_CONFLICTED_TOPOLOGY_VERSION_V1
        or value.adapter_rule_version
        != HISTORICAL_THREE_FAMILY_CONFLICTED_ADAPTER_RULE_VERSION_V1
        or value.display_grade
        != HistoricalThreeFamilyDisplayGradeV2.CONFLICTED_MAJORITY_UNCALIBRATED.value
        or value.source_admitted is not False
        or value.source_outcome_authorized is not False
        or value.adapter_outcome_authorized is not True
        or value.historical_only is not True
        or value.probability is not False
        or value.probability_calibrated is not False
        or value.promoting is not False
        or value.order_placement is not False
        or value.changes_source_admission is not False
        or value.changes_source_event_id is not False
        or value.clean_population_pooled is not False
    ):
        raise HistoricalThreeFamilyConflictedAdapterErrorV1(
            "adapter version, role, authorization, or fixed claims differ"
        )
    if type(value.decision_time_ms) is not int or not (
        0 <= value.decision_time_ms <= _JCS_SAFE_INTEGER_MAX
    ):
        raise HistoricalThreeFamilyConflictedAdapterErrorV1(
            "decision time must be a nonnegative JCS-safe integer"
        )
    _decimal_text(value.decision_price, "decision_price", positive=True)
    if value.invalidation is not None:
        _decimal_text(value.invalidation, "invalidation", positive=True)
    _decimal_text(value.atr, "atr", positive=False)
    if not -1_000_000 <= value.directional_agreement_micros <= 1_000_000:
        raise HistoricalThreeFamilyConflictedAdapterErrorV1(
            "directional agreement must remain in [-1e6, 1e6]"
        )
    bullish = value.primary_direction == Direction.LONG.value
    if (
        value.primary_direction not in {Direction.LONG.value, Direction.SHORT.value}
        or bullish
        != (value.topology_majority_direction == HistoricalThreeFamilyMajorityDirectionV2.BULLISH)
    ):
        raise HistoricalThreeFamilyConflictedAdapterErrorV1(
            "event source side and sign-count conflicted majority are inconsistent"
        )


def _read_conflicted_source_rows(raw: bytes) -> tuple[dict[str, str], ...]:
    if not raw or b"\r" in raw or not raw.endswith(b"\n"):
        raise HistoricalThreeFamilyConflictedAdapterErrorV1(
            "consensus.csv must remain nonempty canonical LF-only bytes"
        )
    try:
        reader = csv.DictReader(io.StringIO(raw.decode("utf-8"), newline=""), strict=True)
        expected = tuple(field.name for field in fields(HistoricalConsensusCensusRowV2))
        if tuple(reader.fieldnames or ()) != expected:
            raise HistoricalThreeFamilyConflictedAdapterErrorV1(
                "consensus.csv header differs from the frozen census schema"
            )
        rows: list[dict[str, str]] = []
        for row in reader:
            if len(rows) >= _MAX_CENSUS_ROWS:
                raise HistoricalThreeFamilyConflictedAdapterErrorV1(
                    "consensus.csv exceeds the bounded adapter row cap"
                )
            if None in row or any(value is None for value in row.values()):
                raise HistoricalThreeFamilyConflictedAdapterErrorV1(
                    "consensus.csv has a surplus or missing column"
                )
            rows.append(cast(dict[str, str], row))
    except (UnicodeError, csv.Error) as exc:
        raise HistoricalThreeFamilyConflictedAdapterErrorV1(
            "consensus.csv is not valid UTF-8 CSV"
        ) from exc
    return tuple(rows)


def _authorization_document(
    *,
    source: LoadedHistoricalConsensusV2,
    adapter_contract_sha256: str,
    code_freeze_manifest_sha256: str,
    events: Sequence[HistoricalConflictedComparatorEventV1],
) -> dict[str, object]:
    event_hashes = [
        _sha256_bytes(canonical_historical_conflicted_event_v1(event)) for event in events
    ]
    return {
        "adapter_contract_sha256": adapter_contract_sha256,
        "adapter_rule_version": HISTORICAL_THREE_FAMILY_CONFLICTED_ADAPTER_RULE_VERSION_V1,
        "census_manifest_sha256": source.census_manifest_sha256,
        "census_rows": source.census_rows,
        "clean_population_pooled": False,
        "code_freeze_manifest_sha256": code_freeze_manifest_sha256,
        "event_count": len(events),
        "event_projection_root_sha256": _sha256_bytes(canonical_json_line({"rows": event_hashes})),
        "experiment_contract_sha256": source.experiment_contract_sha256,
        "historical_only": True,
        "horizons_bars": list(HISTORICAL_THREE_FAMILY_OUTCOME_HORIZONS_BARS_V2),
        "horizons_minutes": [
            value * 5 for value in HISTORICAL_THREE_FAMILY_OUTCOME_HORIZONS_BARS_V2
        ],
        "order_placement": False,
        "outcome_data_read": False,
        "probability": False,
        "probability_calibrated": False,
        "promoting": False,
        "protocol": HISTORICAL_THREE_FAMILY_CONFLICTED_ADAPTER_PROTOCOL_V1,
        "schema_version": HISTORICAL_THREE_FAMILY_CONFLICTED_ADAPTER_SCHEMA_VERSION_V1,
        "source_consensus_sha256": source.consensus_sha256,
        "source_event_identity_preserved": True,
        "source_outcome_authorized": False,
        "topology_contract_sha256": source.topology_amendment_sha256,
        "topology_version": HISTORICAL_THREE_FAMILY_CONFLICTED_TOPOLOGY_VERSION_V1,
    }


def _event_document(value: HistoricalConflictedComparatorEventV1) -> dict[str, object]:
    document = asdict(value)
    document["horizons_bars"] = list(value.horizons_bars)
    return document


def _events_csv_bytes(events: Sequence[HistoricalConflictedComparatorEventV1]) -> bytes:
    buffer = io.StringIO(newline="")
    writer = csv.DictWriter(
        buffer,
        fieldnames=tuple(field.name for field in fields(HistoricalConflictedComparatorEventV1)),
        lineterminator="\n",
    )
    writer.writeheader()
    for event in events:
        row = asdict(event)
        row["horizons_bars"] = "|".join(str(value) for value in event.horizons_bars)
        for name in (
            "source_admitted",
            "source_outcome_authorized",
            "adapter_outcome_authorized",
            "historical_only",
            "probability",
            "probability_calibrated",
            "promoting",
            "order_placement",
            "changes_source_admission",
            "changes_source_event_id",
            "clean_population_pooled",
        ):
            row[name] = "true" if row[name] else "false"
        row["invalidation"] = "" if event.invalidation is None else event.invalidation
        writer.writerow(row)
    return buffer.getvalue().encode("utf-8")


def _authenticate_adapter_contract(root: Path, path: str | Path, expected_sha256: str) -> None:
    expected_path = _resolve_relative(
        root, HISTORICAL_THREE_FAMILY_CONFLICTED_ADAPTER_CONTRACT_PATH_V1
    )
    actual_path = Path(path).resolve()
    actual_hash = _sha256_file(actual_path, "adapter contract")
    if actual_path != expected_path or actual_hash != expected_sha256:
        raise HistoricalThreeFamilyConflictedAdapterErrorV1(
            "adapter contract path or hash differs from the frozen authority"
        )


def _authenticate_code_freeze(
    *,
    root: Path,
    manifest_path: str | Path,
    expected_manifest_sha256: str,
    census_manifest_sha256: str,
    experiment_contract_sha256: str,
    topology_contract_sha256: str,
    adapter_contract_sha256: str,
) -> None:
    raw = _read_bytes(Path(manifest_path).resolve(), "adapter code freeze manifest")
    if _sha256_bytes(raw) != expected_manifest_sha256:
        raise HistoricalThreeFamilyConflictedAdapterErrorV1(
            "adapter code freeze manifest differs from the externally frozen hash"
        )
    document = _decode_canonical_object(raw, "adapter code freeze manifest")
    expected_fields = {
        "adapter_contract_sha256": adapter_contract_sha256,
        "census_manifest_sha256": census_manifest_sha256,
        "experiment_contract_sha256": experiment_contract_sha256,
        "freeze_scope": HISTORICAL_THREE_FAMILY_CONFLICTED_ADAPTER_FREEZE_SCOPE_V1,
        "historical_only": True,
        "outcome_data_read": False,
        "purpose": "PRE_OUTCOME_HISTORICAL_CONFLICTED_2_VS_1_ADAPTER_FREEZE",
        "schema_version": HISTORICAL_THREE_FAMILY_CONFLICTED_ADAPTER_FREEZE_SCHEMA_V1,
        "topology_contract_sha256": topology_contract_sha256,
    }
    for name, expected in expected_fields.items():
        if document.get(name) != expected:
            raise HistoricalThreeFamilyConflictedAdapterErrorV1(
                f"adapter code freeze field {name} differs"
            )
    hashes = document.get("file_sha256")
    if not isinstance(hashes, dict):
        raise HistoricalThreeFamilyConflictedAdapterErrorV1(
            "adapter code freeze file_sha256 must be an object"
        )
    expected_paths = _freeze_relative_paths(root)
    if set(hashes) != set(expected_paths):
        raise HistoricalThreeFamilyConflictedAdapterErrorV1(
            "adapter code freeze does not bind the exact all-source scope"
        )
    if document.get("signalbot_python_source_count") != sum(
        path.startswith("src/signalbot/") for path in expected_paths
    ):
        raise HistoricalThreeFamilyConflictedAdapterErrorV1(
            "adapter code freeze source count is inconsistent"
        )
    for relative in expected_paths:
        expected_hash = _require_sha256(hashes.get(relative), f"freeze hash {relative}")
        if _sha256_file(_resolve_relative(root, relative), relative) != expected_hash:
            raise HistoricalThreeFamilyConflictedAdapterErrorV1(
                f"workspace file drifted after adapter freeze: {relative}"
            )


def _freeze_relative_paths(root: Path) -> tuple[str, ...]:
    source_root = _resolve_relative(root, "src/signalbot")
    try:
        source_paths = tuple(
            sorted(path.relative_to(root).as_posix() for path in source_root.rglob("*.py"))
        )
    except OSError as exc:
        raise HistoricalThreeFamilyConflictedAdapterErrorV1(
            "cannot enumerate signalbot source for adapter freeze"
        ) from exc
    if not source_paths:
        raise HistoricalThreeFamilyConflictedAdapterErrorV1(
            "adapter freeze requires nonempty signalbot Python source"
        )
    required = (
        *_ENVIRONMENT_PIN_PATHS,
        _EXPERIMENT_CONTRACT_PATH,
        _TOPOLOGY_CONTRACT_PATH,
        HISTORICAL_THREE_FAMILY_CONFLICTED_ADAPTER_CONTRACT_PATH_V1,
        *source_paths,
    )
    for relative in required:
        if not _resolve_relative(root, relative).is_file():
            raise HistoricalThreeFamilyConflictedAdapterErrorV1(
                f"required adapter freeze file is missing: {relative}"
            )
    return tuple(sorted(required))


def _publish(target: Path, payloads: Mapping[str, bytes]) -> None:
    if set(payloads) != _PUBLISHED_NAMES:
        raise HistoricalThreeFamilyConflictedAdapterErrorV1(
            "adapter publication requires exactly three artifacts"
        )
    if target.exists():
        raise HistoricalThreeFamilyConflictedAdapterErrorV1(
            "adapter output requires a fresh directory"
        )
    target.parent.mkdir(parents=True, exist_ok=True)
    staging = Path(tempfile.mkdtemp(prefix=f".{target.name}.", dir=target.parent))
    try:
        for name, payload in payloads.items():
            (staging / name).write_bytes(payload)
        os.replace(staging, target)
    except OSError as exc:
        shutil.rmtree(staging, ignore_errors=True)
        raise HistoricalThreeFamilyConflictedAdapterErrorV1(
            "cannot atomically publish adapter artifacts"
        ) from exc


def _resolve_relative(root: Path, relative: str) -> Path:
    if (
        not relative
        or relative.startswith("/")
        or "\\" in relative
        or any(part in {"", ".", ".."} for part in relative.split("/"))
    ):
        raise HistoricalThreeFamilyConflictedAdapterErrorV1(
            "adapter path must be normalized relative POSIX text"
        )
    candidate = (root / Path(*relative.split("/"))).resolve()
    if candidate == root or root not in candidate.parents:
        raise HistoricalThreeFamilyConflictedAdapterErrorV1(
            "adapter path escapes the workspace root"
        )
    return candidate


def _decode_canonical_object(raw: bytes, label: str) -> dict[str, object]:
    try:
        value = json.loads(raw)
    except (json.JSONDecodeError, UnicodeError) as exc:
        raise HistoricalThreeFamilyConflictedAdapterErrorV1(
            f"{label} is not valid UTF-8 JSON"
        ) from exc
    if not isinstance(value, dict):
        raise HistoricalThreeFamilyConflictedAdapterErrorV1(f"{label} must be an object")
    document = cast(dict[str, object], value)
    if canonical_json_line(document) != raw:
        raise HistoricalThreeFamilyConflictedAdapterErrorV1(
            f"{label} must be canonical RFC 8785 JSONL"
        )
    return document


def _decimal_text(value: str, label: str, *, positive: bool) -> str:
    try:
        parsed = Decimal(value)
    except (InvalidOperation, TypeError) as exc:
        raise HistoricalThreeFamilyConflictedAdapterErrorV1(
            f"{label} must be finite Decimal text"
        ) from exc
    if not parsed.is_finite() or (parsed <= 0 if positive else parsed < 0):
        raise HistoricalThreeFamilyConflictedAdapterErrorV1(
            f"{label} is outside the frozen nonnegative/positive domain"
        )
    return value


def _parse_int(value: str, label: str) -> int:
    if not value or value.startswith("+") or (value.startswith("0") and value != "0"):
        raise HistoricalThreeFamilyConflictedAdapterErrorV1(
            f"{label} must be canonical integer text"
        )
    try:
        return int(value)
    except ValueError as exc:
        raise HistoricalThreeFamilyConflictedAdapterErrorV1(
            f"{label} must be canonical integer text"
        ) from exc


def _require_sha256(value: object, label: str) -> str:
    if (
        not isinstance(value, str)
        or len(value) != _SHA256_LENGTH
        or any(character not in "0123456789abcdef" for character in value)
    ):
        raise HistoricalThreeFamilyConflictedAdapterErrorV1(
            f"{label} must be a lowercase SHA-256 digest"
        )
    return value


def _read_bytes(path: Path, label: str) -> bytes:
    try:
        return path.read_bytes()
    except OSError as exc:
        raise HistoricalThreeFamilyConflictedAdapterErrorV1(
            f"cannot read {label}: {path}"
        ) from exc


def _sha256_file(path: Path, label: str) -> str:
    return _sha256_bytes(_read_bytes(path, label))


def _sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--consensus", required=True)
    parser.add_argument("--census-manifest", required=True)
    parser.add_argument("--census-manifest-sha256", required=True)
    parser.add_argument("--experiment-contract-sha256", required=True)
    parser.add_argument("--topology-amendment-sha256", required=True)
    parser.add_argument("--workspace-root", required=True)
    parser.add_argument("--adapter-contract", required=True)
    parser.add_argument("--adapter-contract-sha256", required=True)
    parser.add_argument("--code-freeze-manifest", required=True)
    parser.add_argument("--code-freeze-manifest-sha256", required=True)
    parser.add_argument("--output-dir", required=True)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    loaded = load_authorized_historical_conflicted_comparator_v1(
        consensus_path=args.consensus,
        census_manifest_path=args.census_manifest,
        expected_census_manifest_sha256=args.census_manifest_sha256,
        expected_experiment_contract_sha256=args.experiment_contract_sha256,
        expected_topology_amendment_sha256=args.topology_amendment_sha256,
        workspace_root=args.workspace_root,
        adapter_contract_path=args.adapter_contract,
        expected_adapter_contract_sha256=args.adapter_contract_sha256,
        code_freeze_manifest_path=args.code_freeze_manifest,
        expected_code_freeze_manifest_sha256=args.code_freeze_manifest_sha256,
    )
    publish_historical_conflicted_comparator_v1(loaded, args.output_dir)
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
