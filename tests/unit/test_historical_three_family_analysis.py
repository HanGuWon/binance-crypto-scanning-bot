from __future__ import annotations

import hashlib
from pathlib import Path
from types import SimpleNamespace

import pytest

from signalbot.backtest.historical_three_family_analysis import (
    HISTORICAL_THREE_FAMILY_FULL_CALENDAR_SCHEDULE_SHA256_V2,
    HistoricalThreeFamilyAnalysisErrorV2,
    load_authenticated_historical_three_family_census_artifacts_v2,
    run_historical_three_family_analysis_v2,
)
from signalbot.backtest.historical_three_family_census import (
    HISTORICAL_THREE_FAMILY_CENSUS_PROTOCOL_V2,
    HISTORICAL_THREE_FAMILY_CENSUS_SCHEMA_VERSION_V2,
)
from signalbot.r4b_v2.canonical import canonical_json_line
from signalbot.r4b_v2.strategy.historical_three_family_consensus import (
    HISTORICAL_THREE_FAMILY_CONSENSUS_RULE_VERSION_V2,
    build_historical_execution_contract_v2,
)

_EXPERIMENT_SHA = "1" * 64
_TOPOLOGY_SHA = "2" * 64
_EXECUTION_SHA = build_historical_execution_contract_v2().execution_contract_sha256


def _sha(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def _write_census(root: Path) -> str:
    root.mkdir(parents=True)
    consensus_raw = b"authenticated-outcome-blind-consensus\n"
    results = {
        "authenticated_anchors": 2,
        "census_complete": True,
        "conflicted_comparator_outcome_authorized": False,
        "consensus_csv_sha256": _sha(consensus_raw),
        "consensus_rows": 2,
        "consensus_rule_version": HISTORICAL_THREE_FAMILY_CONSENSUS_RULE_VERSION_V2,
        "diagnostic_mode": False,
        "execution_contract_sha256": _EXECUTION_SHA,
        "experiment_contract_sha256": _EXPERIMENT_SHA,
        "historical_only": True,
        "outcome_data_read": False,
        "probability": False,
        "probability_calibrated": False,
        "promoting": False,
        "protocol": HISTORICAL_THREE_FAMILY_CENSUS_PROTOCOL_V2,
        "schema_version": HISTORICAL_THREE_FAMILY_CENSUS_SCHEMA_VERSION_V2,
        "topology_analysis": {
            "admission_reconciliation": {
                "admission_parity": True,
                "clean_primary_audit_eligible_rows": 2,
                "conflicted_comparator_outcome_authorized": False,
                "consensus_rows": 2,
                "source_admitted_rows": 2,
            }
        },
        "topology_contract_sha256": _TOPOLOGY_SHA,
        "v1a_fitted_selection_used": False,
    }
    results_raw = canonical_json_line(results)
    manifest = {
        "census_complete": True,
        "conflicted_comparator_outcome_authorized": False,
        "consensus_rule_version": HISTORICAL_THREE_FAMILY_CONSENSUS_RULE_VERSION_V2,
        "diagnostic_mode": False,
        "execution_contract_sha256": _EXECUTION_SHA,
        "experiment_contract_sha256": _EXPERIMENT_SHA,
        "historical_only": True,
        "maximum_anchors": None,
        "outcome_data_read": False,
        "outputs": {
            "consensus.csv": _sha(consensus_raw),
            "results.json": _sha(results_raw),
        },
        "probability": False,
        "promoting": False,
        "protocol": HISTORICAL_THREE_FAMILY_CENSUS_PROTOCOL_V2,
        "schema_version": HISTORICAL_THREE_FAMILY_CENSUS_SCHEMA_VERSION_V2,
        "topology_contract_sha256": _TOPOLOGY_SHA,
        "v1a_fitted_selection_used": False,
    }
    manifest_raw = canonical_json_line(manifest)
    (root / "consensus.csv").write_bytes(consensus_raw)
    (root / "results.json").write_bytes(results_raw)
    (root / "manifest.json").write_bytes(manifest_raw)
    return _sha(manifest_raw)


def test_census_loader_authenticates_exact_file_hashes_claims_and_counts(
    tmp_path: Path,
) -> None:
    root = tmp_path / "census"
    manifest_sha = _write_census(root)

    loaded = load_authenticated_historical_three_family_census_artifacts_v2(
        root,
        expected_manifest_sha256=manifest_sha,
        expected_experiment_contract_sha256=_EXPERIMENT_SHA,
        expected_topology_amendment_sha256=_TOPOLOGY_SHA,
    )

    assert loaded.census_rows == loaded.admitted_events == 2
    assert not loaded.outcome_data_read
    assert loaded.consensus_sha256 == _sha((root / "consensus.csv").read_bytes())

    (root / "surplus.txt").write_text("surplus", encoding="utf-8")
    with pytest.raises(HistoricalThreeFamilyAnalysisErrorV2, match="exact file set"):
        load_authenticated_historical_three_family_census_artifacts_v2(
            root,
            expected_manifest_sha256=manifest_sha,
            expected_experiment_contract_sha256=_EXPERIMENT_SHA,
            expected_topology_amendment_sha256=_TOPOLOGY_SHA,
        )


def test_schedule_is_verified_before_any_outcome_artifact_is_opened(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    census = tmp_path / "census"
    manifest_sha = _write_census(census)
    outcome_opened = False

    def forbidden_outcome_loader(*args: object, **kwargs: object) -> object:
        nonlocal outcome_opened
        outcome_opened = True
        raise AssertionError("outcome loader must not run before schedule authentication")

    monkeypatch.setattr(
        "signalbot.backtest.historical_three_family_analysis."
        "build_historical_three_family_bootstrap_schedule_v2",
        lambda **kwargs: SimpleNamespace(schedule_sha256="0" * 64),
    )
    monkeypatch.setattr(
        "signalbot.backtest.historical_three_family_analysis."
        "load_authenticated_historical_fixed_horizon_artifacts_v2",
        forbidden_outcome_loader,
    )
    with pytest.raises(HistoricalThreeFamilyAnalysisErrorV2, match="schedule differs"):
        run_historical_three_family_analysis_v2(
            census_artifact_dir=census,
            expected_census_manifest_sha256=manifest_sha,
            fixed_horizon_artifact_dir=tmp_path / "not-opened-fixed",
            expected_fixed_horizon_manifest_sha256="3" * 64,
            te0_artifact_dir=tmp_path / "not-opened-te0",
            expected_te0_manifest_sha256="4" * 64,
            expected_experiment_contract_sha256=_EXPERIMENT_SHA,
            expected_topology_amendment_sha256=_TOPOLOGY_SHA,
            expected_funding_authority_manifest_sha256="5" * 64,
            expected_downstream_code_freeze_manifest_sha256="6" * 64,
            output_dir=tmp_path / "out",
        )

    assert not outcome_opened
    assert HISTORICAL_THREE_FAMILY_FULL_CALENDAR_SCHEDULE_SHA256_V2 != "0" * 64


def test_conflicted_comparator_arguments_are_all_or_none_and_never_implicitly_pooled(
    tmp_path: Path,
) -> None:
    with pytest.raises(HistoricalThreeFamilyAnalysisErrorV2, match="both external"):
        run_historical_three_family_analysis_v2(
            census_artifact_dir=tmp_path / "not-opened-census",
            expected_census_manifest_sha256="1" * 64,
            fixed_horizon_artifact_dir=tmp_path / "not-opened-fixed",
            expected_fixed_horizon_manifest_sha256="2" * 64,
            te0_artifact_dir=tmp_path / "not-opened-te0",
            expected_te0_manifest_sha256="3" * 64,
            expected_experiment_contract_sha256="4" * 64,
            expected_topology_amendment_sha256="5" * 64,
            expected_funding_authority_manifest_sha256="6" * 64,
            expected_downstream_code_freeze_manifest_sha256="7" * 64,
            output_dir=tmp_path / "out",
            conflicted_fixed_horizon_artifact_dir=tmp_path / "conflicted",
        )
