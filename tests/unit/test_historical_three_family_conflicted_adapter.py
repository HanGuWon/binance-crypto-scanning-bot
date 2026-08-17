from __future__ import annotations

import hashlib
import json
from dataclasses import replace
from decimal import Decimal
from pathlib import Path

import pytest

from signalbot.backtest.historical_three_family_census import (
    HISTORICAL_THREE_FAMILY_CENSUS_PROTOCOL_V2,
    HISTORICAL_THREE_FAMILY_CENSUS_SCHEMA_VERSION_V2,
    HistoricalConsensusCensusRowV2,
    _consensus_csv_bytes_v2,
)
from signalbot.backtest.historical_three_family_conflicted_adapter import (
    HISTORICAL_THREE_FAMILY_CONFLICTED_ADAPTER_CONTRACT_PATH_V1,
    HistoricalThreeFamilyConflictedAdapterErrorV1,
    build_historical_conflicted_adapter_code_freeze_manifest_v1,
    canonical_historical_conflicted_event_v1,
    load_authenticated_historical_conflicted_adapter_artifacts_v1,
    load_authorized_historical_conflicted_comparator_v1,
    publish_historical_conflicted_comparator_v1,
)
from signalbot.r4b_v2.canonical import canonical_json_line
from signalbot.r4b_v2.research.historical_three_family_topology import (
    HISTORICAL_THREE_FAMILY_TOPOLOGY_RULE_VERSION_V2,
)
from signalbot.r4b_v2.strategy.historical_three_family_consensus import (
    HISTORICAL_THREE_FAMILY_CONSENSUS_RULE_VERSION_V2,
    build_historical_execution_contract_v2,
)

_ROOT = Path(__file__).resolve().parents[2]
_EXPERIMENT = _ROOT / "docs/r4b-v2-historical-three-family-consensus-experiment.md"
_TOPOLOGY = _ROOT / "docs/r4b-v2-historical-three-family-topology-preoutcome-amendment.md"
_ADAPTER = _ROOT / HISTORICAL_THREE_FAMILY_CONFLICTED_ADAPTER_CONTRACT_PATH_V1
_EXPERIMENT_SHA = hashlib.sha256(_EXPERIMENT.read_bytes()).hexdigest()
_TOPOLOGY_SHA = hashlib.sha256(_TOPOLOGY.read_bytes()).hexdigest()
_ADAPTER_SHA = hashlib.sha256(_ADAPTER.read_bytes()).hexdigest()
_EXECUTION_SHA = build_historical_execution_contract_v2().execution_contract_sha256
_DECISION_MS = 1_730_000_299_999

_FILES = {
    "BONK": "BONK__1000BONKUSDT__5m.csv.gz",
    "ENA": "ENA__ENAUSDT__5m.csv.gz",
    "WIF": "WIF__WIFUSDT__5m.csv.gz",
    "FLOKI": "FLOKI__1000FLOKIUSDT__5m.csv.gz",
    "ARB": "ARB__ARBUSDT__5m.csv.gz",
    "OP": "OP__OPUSDT__5m.csv.gz",
    "SEI": "SEI__SEIUSDT__5m.csv.gz",
}


def _sha(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def _row(
    *,
    signs: tuple[int, int, int] = (1, 1, -1),
    primary_direction: str = "long",
    eligible: bool = True,
    seed: str = "a",
    agreement: int | None = None,
) -> HistoricalConsensusCensusRowV2:
    bullish = signs.count(1)
    bearish = signs.count(-1)
    majority_bullish = bullish == 2
    primary_support = bullish if primary_direction == "long" else bearish
    primary_oppose = bearish if primary_direction == "long" else bullish
    topology = (
        "CONFLICTED_BULLISH_2_1_0"
        if majority_bullish
        else "CONFLICTED_BEARISH_1_2_0"
    )
    return HistoricalConsensusCensusRowV2(
        split="development",
        asset="BONK",
        symbol="1000BONKUSDT",
        source_event_id=f"source-{seed}",
        source_row_sha256="2" * 64,
        source_replay_manifest_sha256="3" * 64,
        anchor_sha256=("b" if seed == "a" else "c") * 64,
        primary_family=("pullback_long" if primary_direction == "long" else "pullback_short"),
        primary_direction=primary_direction,
        decision_time_ms=_DECISION_MS + (0 if seed == "a" else 300_000),
        price=Decimal("100"),
        invalidation=Decimal("90") if primary_direction == "long" else Decimal("110"),
        atr=Decimal("5"),
        event_id=seed * 64,
        payload_sha256="4" * 64,
        canonical_consensus_sha256="5" * 64,
        topology_sha256="6" * 64,
        canonical_topology_sha256="7" * 64,
        topology_contract_sha256=_TOPOLOGY_SHA,
        topology_rule_version=HISTORICAL_THREE_FAMILY_TOPOLOGY_RULE_VERSION_V2,
        topology_class=topology,
        topology_comparison_bucket="CONFLICTED_2_VS_1",
        topology_display_grade="CONFLICTED_MAJORITY_UNCALIBRATED",
        topology_majority_direction="BULLISH" if majority_bullish else "BEARISH",
        topology_majority_family_count=2,
        topology_opposing_family_count=1,
        topology_has_opposition=True,
        topology_primary_support_count=primary_support,
        topology_primary_oppose_count=primary_oppose,
        topology_primary_neutral_count=0,
        clean_primary_audit_eligible=False,
        conflicted_comparator_eligible=eligible,
        conflicted_comparator_outcome_authorized=False,
        rule_version=HISTORICAL_THREE_FAMILY_CONSENSUS_RULE_VERSION_V2,
        status="READY",
        state_class="MIXED_OR_NEUTRAL_STATE",
        directional_numerator_micros=(
            agreement
            if agreement is not None
            else (166_667 if majority_bullish else -166_667)
        ),
        directional_denominator=3,
        directional_agreement_micros=(
            agreement
            if agreement is not None
            else (166_667 if majority_bullish else -166_667)
        ),
        bullish_family_count=bullish,
        bearish_family_count=bearish,
        neutral_family_count=0,
        primary_relationship="MIXED_OR_NEUTRAL",
        admitted=False,
        price_status="READY",
        price_direction=signs[0],
        price_strength_micros=500_000,
        price_calculation_sha256="8" * 64,
        price_source_slice_sha256="9" * 64,
        participation_status="READY",
        participation_direction=signs[1],
        participation_strength_micros=500_000,
        participation_calculation_sha256="c" * 64,
        participation_source_slice_sha256="d" * 64,
        cross_section_status="READY",
        cross_section_direction=signs[2],
        cross_section_strength_micros=500_000,
        cross_section_calculation_sha256="e" * 64,
        cross_section_source_slice_sha256="f" * 64,
        execution_contract_sha256=_EXECUTION_SHA,
        zero_move_round_trip_cost_micros=2_600,
        atr_fraction_micros=50_000,
        one_atr_cost_headroom_micros=47_400,
        cross_peer_set_root_sha256="0" * 64,
        cross_peer_input_sha256="1" * 64,
        reasons=("HISTORICAL_ONLY", "NOT_A_PROBABILITY"),
    )


def _source_authority(
    root: Path, rows: tuple[HistoricalConsensusCensusRowV2, ...]
) -> tuple[Path, Path, str]:
    raw = _consensus_csv_bytes_v2(rows)
    data_hashes = {
        f"futures/{filename}": format(index + 1, "x") * 64
        for index, filename in enumerate(_FILES.values())
    }
    manifest_hashes = {
        f"{path}.manifest.json": format(index + 8, "x") * 64
        for index, path in enumerate(data_hashes)
    }
    manifest = canonical_json_line(
        {
            "anchor_set_sha256": "f" * 64,
            "census_complete": True,
            "conflicted_comparator_outcome_authorized": False,
            "consensus_rule_version": HISTORICAL_THREE_FAMILY_CONSENSUS_RULE_VERSION_V2,
            "diagnostic_mode": False,
            "execution_contract_sha256": _EXECUTION_SHA,
            "experiment_contract_sha256": _EXPERIMENT_SHA,
            "historical_only": True,
            "historical_receipt_policy": "RECEIPT_EQUALS_CLOSED_KLINE_CLOSE_TIME",
            "inputs": {
                "futures_data_sha256": data_hashes,
                "futures_manifest_sha256": manifest_hashes,
                "recommendations_sha256": {},
                "run_manifest_sha256": {},
            },
            "maximum_anchors": None,
            "outcome_data_read": False,
            "outputs": {"consensus.csv": _sha(raw), "results.json": "d" * 64},
            "probability": False,
            "promoting": False,
            "protocol": HISTORICAL_THREE_FAMILY_CENSUS_PROTOCOL_V2,
            "schema_version": HISTORICAL_THREE_FAMILY_CENSUS_SCHEMA_VERSION_V2,
            "source_representation": "CANONICAL_NUMERIC_CALCULATION",
            "topology_contract_sha256": _TOPOLOGY_SHA,
            "topology_rule_version": HISTORICAL_THREE_FAMILY_TOPOLOGY_RULE_VERSION_V2,
            "v1a_fitted_selection_used": False,
        }
    )
    root.mkdir()
    consensus = root / "consensus.csv"
    census_manifest = root / "manifest.json"
    consensus.write_bytes(raw)
    census_manifest.write_bytes(manifest)
    return consensus, census_manifest, _sha(manifest)


def _freeze(root: Path, census_sha: str) -> tuple[Path, str]:
    raw = build_historical_conflicted_adapter_code_freeze_manifest_v1(
        workspace_root=_ROOT,
        census_manifest_sha256=census_sha,
        experiment_contract_sha256=_EXPERIMENT_SHA,
        topology_contract_sha256=_TOPOLOGY_SHA,
        adapter_contract_sha256=_ADAPTER_SHA,
    )
    path = root / "freeze.json"
    path.write_bytes(raw)
    return path, _sha(raw)


def _load(tmp_path: Path, rows: tuple[HistoricalConsensusCensusRowV2, ...]):
    consensus, manifest, census_sha = _source_authority(tmp_path / "source", rows)
    freeze, freeze_sha = _freeze(tmp_path, census_sha)
    return load_authorized_historical_conflicted_comparator_v1(
        consensus_path=consensus,
        census_manifest_path=manifest,
        expected_census_manifest_sha256=census_sha,
        expected_experiment_contract_sha256=_EXPERIMENT_SHA,
        expected_topology_amendment_sha256=_TOPOLOGY_SHA,
        workspace_root=_ROOT,
        adapter_contract_path=_ADAPTER,
        expected_adapter_contract_sha256=_ADAPTER_SHA,
        code_freeze_manifest_path=freeze,
        expected_code_freeze_manifest_sha256=freeze_sha,
    )


def test_freeze_is_canonical_deterministic_and_outcome_blind() -> None:
    kwargs = {
        "workspace_root": _ROOT,
        "census_manifest_sha256": "a" * 64,
        "experiment_contract_sha256": _EXPERIMENT_SHA,
        "topology_contract_sha256": _TOPOLOGY_SHA,
        "adapter_contract_sha256": _ADAPTER_SHA,
    }
    first = build_historical_conflicted_adapter_code_freeze_manifest_v1(**kwargs)
    second = build_historical_conflicted_adapter_code_freeze_manifest_v1(**kwargs)
    document = json.loads(first)
    assert first == second == canonical_json_line(document)
    assert document["outcome_data_read"] is False
    assert document["file_sha256"][HISTORICAL_THREE_FAMILY_CONFLICTED_ADAPTER_CONTRACT_PATH_V1]
    assert document["signalbot_python_source_count"] > 100


def test_adapter_selects_only_source_side_eligible_conflict_and_preserves_identity(
    tmp_path: Path,
) -> None:
    supporting = _row()
    wrong_side = _row(
        signs=(-1, -1, 1),
        primary_direction="long",
        eligible=False,
        seed="b",
    )
    loaded = _load(tmp_path, (supporting, wrong_side))
    assert len(loaded.events) == 1
    event = loaded.events[0]
    assert event.event_id == supporting.event_id
    assert event.anchor_sha256 == supporting.anchor_sha256
    assert event.source_admitted is False
    assert event.source_outcome_authorized is False
    assert event.adapter_outcome_authorized is True
    assert event.clean_population_pooled is False
    assert event.horizons_bars == (1, 3, 6, 12, 72)
    assert event.horizon_minutes == (5, 15, 30, 60, 360)
    assert canonical_historical_conflicted_event_v1(event).endswith(b"\n")


def test_adapter_accepts_short_majority_without_recasting_side(tmp_path: Path) -> None:
    loaded = _load(
        tmp_path,
        (_row(signs=(-1, -1, 1), primary_direction="short"),),
    )
    event = loaded.events[0]
    assert event.primary_direction == "short"
    assert event.primary_family == "pullback_short"
    assert event.topology_majority_direction == "BEARISH"
    assert event.directional_agreement_micros < 0


@pytest.mark.parametrize(
    ("primary_direction", "signs", "agreement"),
    (
        ("long", (1, 1, -1), 500_000),
        ("long", (1, 1, -1), -500_000),
        ("long", (1, 1, -1), 0),
        ("short", (-1, -1, 1), -500_000),
        ("short", (-1, -1, 1), 500_000),
        ("short", (-1, -1, 1), 0),
    ),
)
def test_weighted_agreement_is_descriptive_not_the_sign_count_side(
    tmp_path: Path,
    primary_direction: str,
    signs: tuple[int, int, int],
    agreement: int,
) -> None:
    loaded = _load(
        tmp_path,
        (
            _row(
                signs=signs,
                primary_direction=primary_direction,
                agreement=agreement,
            ),
        ),
    )
    event = loaded.events[0]
    assert event.primary_direction == primary_direction
    assert event.directional_agreement_micros == agreement
    assert event.topology_majority_direction == (
        "BULLISH" if primary_direction == "long" else "BEARISH"
    )


def test_source_authorization_mutation_is_rejected(tmp_path: Path) -> None:
    mutated = replace(_row(), conflicted_comparator_outcome_authorized=True)
    with pytest.raises(
        HistoricalThreeFamilyConflictedAdapterErrorV1,
        match="source census authentication failed",
    ):
        _load(tmp_path, (mutated,))


def test_code_freeze_hash_is_mandatory(tmp_path: Path) -> None:
    consensus, manifest, census_sha = _source_authority(tmp_path / "source", (_row(),))
    freeze, _ = _freeze(tmp_path, census_sha)
    with pytest.raises(
        HistoricalThreeFamilyConflictedAdapterErrorV1,
        match="externally frozen hash",
    ):
        load_authorized_historical_conflicted_comparator_v1(
            consensus_path=consensus,
            census_manifest_path=manifest,
            expected_census_manifest_sha256=census_sha,
            expected_experiment_contract_sha256=_EXPERIMENT_SHA,
            expected_topology_amendment_sha256=_TOPOLOGY_SHA,
            workspace_root=_ROOT,
            adapter_contract_path=_ADAPTER,
            expected_adapter_contract_sha256=_ADAPTER_SHA,
            code_freeze_manifest_path=freeze,
            expected_code_freeze_manifest_sha256="0" * 64,
        )


def test_publisher_is_outcome_blind_atomic_and_refuses_overwrite(tmp_path: Path) -> None:
    loaded = _load(tmp_path, (_row(),))
    output = tmp_path / "published"
    artifacts = publish_historical_conflicted_comparator_v1(loaded, output)
    assert artifacts.event_count == 1
    assert {path.name for path in output.iterdir()} == {
        "authorization.json",
        "conflicted_events.csv",
        "manifest.json",
    }
    manifest = json.loads((output / "manifest.json").read_bytes())
    assert manifest["outcome_data_read"] is False
    assert manifest["clean_population_pooled"] is False
    assert manifest["probability"] is False
    reloaded = load_authenticated_historical_conflicted_adapter_artifacts_v1(
        adapter_artifact_dir=output,
        expected_adapter_manifest_sha256=artifacts.manifest_sha256,
        consensus_path=loaded.source.consensus_path,
        census_manifest_path=loaded.source.census_manifest_path,
        expected_census_manifest_sha256=loaded.source.census_manifest_sha256,
        expected_experiment_contract_sha256=_EXPERIMENT_SHA,
        expected_topology_amendment_sha256=_TOPOLOGY_SHA,
        workspace_root=_ROOT,
        adapter_contract_path=_ADAPTER,
        expected_adapter_contract_sha256=_ADAPTER_SHA,
        code_freeze_manifest_path=tmp_path / "freeze.json",
        expected_code_freeze_manifest_sha256=loaded.code_freeze_manifest_sha256,
    )
    assert reloaded.adapter_manifest_sha256 == artifacts.manifest_sha256
    assert reloaded.events_sha256 == artifacts.events_sha256
    with pytest.raises(
        HistoricalThreeFamilyConflictedAdapterErrorV1,
        match="fresh directory",
    ):
        publish_historical_conflicted_comparator_v1(loaded, output)
