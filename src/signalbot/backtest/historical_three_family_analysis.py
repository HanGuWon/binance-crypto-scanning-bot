"""Authenticated bootstrap/report runner for frozen three-family artifacts.

The full-calendar bootstrap schedule is reconstructed and authenticated before
either downstream outcome artifact directory is opened.  This module only
analyzes the current clean/broad primary population; conflicted-majority rows
require their own separately frozen adapter and are never pooled here.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import shutil
import tempfile
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Final, Literal, cast

from signalbot.backtest.historical_three_family_census import (
    HISTORICAL_THREE_FAMILY_CENSUS_PROTOCOL_V2,
    HISTORICAL_THREE_FAMILY_CENSUS_SCHEMA_VERSION_V2,
)
from signalbot.backtest.historical_three_family_conflicted_outcomes import (
    LoadedHistoricalConflictedFixedHorizonArtifactsV1,
    load_authenticated_historical_conflicted_fixed_horizon_artifacts_v1,
)
from signalbot.backtest.historical_three_family_outcomes import (
    HISTORICAL_THREE_FAMILY_FIXED_HORIZON_RUNNER_PROTOCOL_V2,
    HISTORICAL_THREE_FAMILY_PRIMARY_TOPOLOGY_V2,
    LoadedHistoricalFixedHorizonArtifactsV2,
    load_authenticated_historical_fixed_horizon_artifacts_v2,
)
from signalbot.backtest.historical_three_family_report import (
    HISTORICAL_THREE_FAMILY_REPORT_VERSION_V2,
    historical_three_family_report_sha256_v2,
    render_historical_three_family_report_ko_v2,
)
from signalbot.backtest.historical_three_family_te0 import (
    HISTORICAL_THREE_FAMILY_TE0_PROTOCOL_V2,
    LoadedHistoricalThreeFamilyTe0ArtifactsV2,
    load_authenticated_historical_three_family_te0_artifacts_v2,
)
from signalbot.r4b_v2.canonical import canonical_json_line
from signalbot.r4b_v2.research.historical_three_family_bootstrap import (
    HISTORICAL_THREE_FAMILY_BOOTSTRAP_VERSION_V2,
    HISTORICAL_THREE_FAMILY_FULL_CALENDAR_END_MS_V2,
    HISTORICAL_THREE_FAMILY_FULL_CALENDAR_SCHEDULE_SHA256_V2,
    HISTORICAL_THREE_FAMILY_FULL_CALENDAR_START_MS_V2,
    HistoricalThreeFamilyCostSourceV2,
    bootstrap_historical_three_family_outcomes_v2,
    build_historical_three_family_bootstrap_schedule_v2,
    canonical_historical_three_family_bootstrap_v2,
    cost_attribution_from_fixed_horizon_row_v2,
)
from signalbot.r4b_v2.strategy.historical_three_family_consensus import (
    HISTORICAL_THREE_FAMILY_CONSENSUS_RULE_VERSION_V2,
    build_historical_execution_contract_v2,
)

HISTORICAL_THREE_FAMILY_ANALYSIS_PROTOCOL_V2: Final = (
    "historical_three_family_authenticated_analysis_v2_2026-07-20"
)
HISTORICAL_THREE_FAMILY_ANALYSIS_SCHEMA_VERSION_V2: Final = 1
_SHA256_RE: Final = re.compile(r"^[0-9a-f]{64}$")
_CENSUS_NAMES: Final = frozenset({"consensus.csv", "results.json", "manifest.json"})
_OUTPUT_NAMES: Final = ("bootstrap.json", "report.ko.md")
_PUBLISHED_NAMES: Final = frozenset((*_OUTPUT_NAMES, "manifest.json"))


class HistoricalThreeFamilyAnalysisErrorV2(ValueError):
    """Raised when analysis inputs, schedule, or publication fail closed."""


@dataclass(frozen=True, slots=True)
class LoadedHistoricalThreeFamilyCensusArtifactsV2:
    artifact_dir: Path
    manifest_sha256: str
    consensus_sha256: str
    results_sha256: str
    experiment_contract_sha256: str
    topology_amendment_sha256: str
    execution_contract_sha256: str
    census_rows: int
    admitted_events: int
    results: Mapping[str, object]
    historical_only: Literal[True] = True
    outcome_data_read: Literal[False] = False


@dataclass(frozen=True, slots=True)
class HistoricalThreeFamilyAnalysisArtifactsV2:
    output_dir: Path
    bootstrap_sha256: str
    report_sha256: str
    manifest_sha256: str
    primary_event_count: int
    primary_outcome_count: int


def load_authenticated_historical_three_family_census_artifacts_v2(
    artifact_dir: str | Path,
    *,
    expected_manifest_sha256: str,
    expected_experiment_contract_sha256: str,
    expected_topology_amendment_sha256: str,
) -> LoadedHistoricalThreeFamilyCensusArtifactsV2:
    """Authenticate the complete outcome-blind census and its exact file set."""

    for value, label in (
        (expected_manifest_sha256, "expected census manifest SHA-256"),
        (expected_experiment_contract_sha256, "expected experiment contract SHA-256"),
        (expected_topology_amendment_sha256, "expected topology amendment SHA-256"),
    ):
        _require_sha256(value, label)
    root = Path(artifact_dir).resolve()
    _require_exact_files(root, _CENSUS_NAMES, "census")
    manifest_raw = _read_bytes(root / "manifest.json", "census manifest")
    if _sha256(manifest_raw) != expected_manifest_sha256:
        raise HistoricalThreeFamilyAnalysisErrorV2(
            "census manifest differs from the externally frozen SHA-256"
        )
    manifest = _decode_canonical_object(manifest_raw, "census manifest")
    required: Mapping[str, object] = {
        "census_complete": True,
        "conflicted_comparator_outcome_authorized": False,
        "consensus_rule_version": HISTORICAL_THREE_FAMILY_CONSENSUS_RULE_VERSION_V2,
        "diagnostic_mode": False,
        "execution_contract_sha256": (
            build_historical_execution_contract_v2().execution_contract_sha256
        ),
        "experiment_contract_sha256": expected_experiment_contract_sha256,
        "historical_only": True,
        "maximum_anchors": None,
        "outcome_data_read": False,
        "probability": False,
        "promoting": False,
        "protocol": HISTORICAL_THREE_FAMILY_CENSUS_PROTOCOL_V2,
        "schema_version": HISTORICAL_THREE_FAMILY_CENSUS_SCHEMA_VERSION_V2,
        "topology_contract_sha256": expected_topology_amendment_sha256,
        "v1a_fitted_selection_used": False,
    }
    _require_fields(manifest, required, "census manifest")
    outputs = _require_mapping(manifest.get("outputs"), "census outputs")
    if set(outputs) != {"consensus.csv", "results.json"}:
        raise HistoricalThreeFamilyAnalysisErrorV2("census output hash set is not exact")
    consensus_raw = _read_bytes(root / "consensus.csv", "census consensus")
    results_raw = _read_bytes(root / "results.json", "census results")
    consensus_sha256 = _sha256(consensus_raw)
    results_sha256 = _sha256(results_raw)
    if outputs != {"consensus.csv": consensus_sha256, "results.json": results_sha256}:
        raise HistoricalThreeFamilyAnalysisErrorV2(
            "census payload hashes differ from the authenticated manifest"
        )
    results = _decode_canonical_object(results_raw, "census results")
    _require_fields(
        results,
        {
            "census_complete": True,
            "conflicted_comparator_outcome_authorized": False,
            "consensus_csv_sha256": consensus_sha256,
            "consensus_rule_version": HISTORICAL_THREE_FAMILY_CONSENSUS_RULE_VERSION_V2,
            "diagnostic_mode": False,
            "execution_contract_sha256": manifest.get("execution_contract_sha256"),
            "experiment_contract_sha256": expected_experiment_contract_sha256,
            "historical_only": True,
            "outcome_data_read": False,
            "probability": False,
            "probability_calibrated": False,
            "promoting": False,
            "protocol": HISTORICAL_THREE_FAMILY_CENSUS_PROTOCOL_V2,
            "schema_version": HISTORICAL_THREE_FAMILY_CENSUS_SCHEMA_VERSION_V2,
            "topology_contract_sha256": expected_topology_amendment_sha256,
            "v1a_fitted_selection_used": False,
        },
        "census results",
    )
    census_rows = _require_nonnegative_int(results.get("consensus_rows"), "consensus_rows")
    authenticated = _require_nonnegative_int(
        results.get("authenticated_anchors"), "authenticated_anchors"
    )
    topology = _require_mapping(results.get("topology_analysis"), "topology_analysis")
    reconciliation = _require_mapping(
        topology.get("admission_reconciliation"), "admission_reconciliation"
    )
    admitted = _require_nonnegative_int(
        reconciliation.get("source_admitted_rows"), "source_admitted_rows"
    )
    if (
        census_rows != authenticated
        or reconciliation.get("consensus_rows") != census_rows
        or reconciliation.get("clean_primary_audit_eligible_rows") != admitted
        or reconciliation.get("admission_parity") is not True
        or reconciliation.get("conflicted_comparator_outcome_authorized") is not False
    ):
        raise HistoricalThreeFamilyAnalysisErrorV2(
            "census anchor/admission counts do not reconcile"
        )
    return LoadedHistoricalThreeFamilyCensusArtifactsV2(
        artifact_dir=root,
        manifest_sha256=expected_manifest_sha256,
        consensus_sha256=consensus_sha256,
        results_sha256=results_sha256,
        experiment_contract_sha256=expected_experiment_contract_sha256,
        topology_amendment_sha256=expected_topology_amendment_sha256,
        execution_contract_sha256=cast(str, manifest["execution_contract_sha256"]),
        census_rows=census_rows,
        admitted_events=admitted,
        results=results,
    )


def run_historical_three_family_analysis_v2(
    *,
    census_artifact_dir: str | Path,
    expected_census_manifest_sha256: str,
    fixed_horizon_artifact_dir: str | Path,
    expected_fixed_horizon_manifest_sha256: str,
    te0_artifact_dir: str | Path,
    expected_te0_manifest_sha256: str,
    expected_experiment_contract_sha256: str,
    expected_topology_amendment_sha256: str,
    expected_funding_authority_manifest_sha256: str,
    expected_downstream_code_freeze_manifest_sha256: str,
    output_dir: str | Path,
    conflicted_fixed_horizon_artifact_dir: str | Path | None = None,
    expected_conflicted_fixed_horizon_manifest_sha256: str | None = None,
    expected_conflicted_adapter_manifest_sha256: str | None = None,
) -> HistoricalThreeFamilyAnalysisArtifactsV2:
    """Publish primary analysis plus an optional, separately authenticated comparator."""

    _validate_optional_conflicted_arguments(
        conflicted_fixed_horizon_artifact_dir,
        expected_conflicted_fixed_horizon_manifest_sha256,
        expected_conflicted_adapter_manifest_sha256,
    )
    census = load_authenticated_historical_three_family_census_artifacts_v2(
        census_artifact_dir,
        expected_manifest_sha256=expected_census_manifest_sha256,
        expected_experiment_contract_sha256=expected_experiment_contract_sha256,
        expected_topology_amendment_sha256=expected_topology_amendment_sha256,
    )

    # This gate intentionally precedes opening either forward-outcome artifact set.
    schedule = build_historical_three_family_bootstrap_schedule_v2(
        calendar_start_ms=HISTORICAL_THREE_FAMILY_FULL_CALENDAR_START_MS_V2,
        calendar_end_ms=HISTORICAL_THREE_FAMILY_FULL_CALENDAR_END_MS_V2,
    )
    if schedule.schedule_sha256 != HISTORICAL_THREE_FAMILY_FULL_CALENDAR_SCHEDULE_SHA256_V2:
        raise HistoricalThreeFamilyAnalysisErrorV2(
            "reconstructed full-calendar bootstrap schedule differs from frozen SHA-256"
        )

    fixed = load_authenticated_historical_fixed_horizon_artifacts_v2(
        fixed_horizon_artifact_dir,
        expected_manifest_sha256=expected_fixed_horizon_manifest_sha256,
        expected_census_manifest_sha256=expected_census_manifest_sha256,
        expected_experiment_contract_sha256=expected_experiment_contract_sha256,
        expected_topology_amendment_sha256=expected_topology_amendment_sha256,
        expected_funding_authority_manifest_sha256=(expected_funding_authority_manifest_sha256),
        expected_downstream_code_freeze_manifest_sha256=(
            expected_downstream_code_freeze_manifest_sha256
        ),
    )
    te0 = load_authenticated_historical_three_family_te0_artifacts_v2(
        te0_artifact_dir,
        expected_manifest_sha256=expected_te0_manifest_sha256,
        expected_census_manifest_sha256=expected_census_manifest_sha256,
        expected_experiment_contract_sha256=expected_experiment_contract_sha256,
        expected_topology_amendment_sha256=expected_topology_amendment_sha256,
        expected_funding_authority_manifest_sha256=(expected_funding_authority_manifest_sha256),
        expected_downstream_code_freeze_manifest_sha256=(
            expected_downstream_code_freeze_manifest_sha256
        ),
    )
    _reconcile_sources(census, fixed, te0)
    conflicted = _load_optional_conflicted(
        artifact_dir=conflicted_fixed_horizon_artifact_dir,
        expected_manifest_sha256=expected_conflicted_fixed_horizon_manifest_sha256,
        expected_adapter_manifest_sha256=expected_conflicted_adapter_manifest_sha256,
        expected_execution_contract_sha256=census.execution_contract_sha256,
        expected_downstream_code_freeze_manifest_sha256=(
            expected_downstream_code_freeze_manifest_sha256
        ),
        expected_funding_authority_manifest_sha256=(expected_funding_authority_manifest_sha256),
        expected_census_manifest_sha256=expected_census_manifest_sha256,
    )
    primary_rows = tuple(row.to_audit_outcome() for row in fixed.rows)
    primary_costs = tuple(
        cost_attribution_from_fixed_horizon_row_v2(
            row,
            source=HistoricalThreeFamilyCostSourceV2.PRIMARY_CLEAN,
        )
        for row in fixed.rows
    )
    conflicted_rows = (
        () if conflicted is None else tuple(row.to_bootstrap_outcome() for row in conflicted.rows)
    )
    conflicted_costs = (
        ()
        if conflicted is None
        else tuple(
            cost_attribution_from_fixed_horizon_row_v2(
                row,
                source=HistoricalThreeFamilyCostSourceV2.CONFLICTED_COMPARATOR,
            )
            for row in conflicted.rows
        )
    )
    bootstrap = bootstrap_historical_three_family_outcomes_v2(
        primary_rows,
        calendar_start_ms=HISTORICAL_THREE_FAMILY_FULL_CALENDAR_START_MS_V2,
        calendar_end_ms=HISTORICAL_THREE_FAMILY_FULL_CALENDAR_END_MS_V2,
        cost_attributions=(*primary_costs, *conflicted_costs),
        conflicted_rows=conflicted_rows,
    )
    if bootstrap.shared_draw_schedule_sha256 != schedule.schedule_sha256:
        raise HistoricalThreeFamilyAnalysisErrorV2(
            "bootstrap result does not bind the pre-authenticated schedule"
        )
    bootstrap_raw = canonical_historical_three_family_bootstrap_v2(bootstrap)
    report = render_historical_three_family_report_ko_v2(
        census_results=census.results,
        fixed_horizon_results=fixed.results,
        bootstrap=bootstrap,
        te0_results=te0.results,
    )
    report_sha256 = historical_three_family_report_sha256_v2(report)
    report_raw = report.encode("utf-8")
    output_hashes = {
        "bootstrap.json": _sha256(bootstrap_raw),
        "report.ko.md": report_sha256,
    }
    manifest_raw = canonical_json_line(
        _analysis_manifest_document(
            census=census,
            fixed=fixed,
            te0=te0,
            conflicted=conflicted,
            output_hashes=output_hashes,
        )
    )
    target = Path(output_dir).resolve()
    _publish_analysis(
        target,
        {
            "bootstrap.json": bootstrap_raw,
            "report.ko.md": report_raw,
            "manifest.json": manifest_raw,
        },
    )
    return HistoricalThreeFamilyAnalysisArtifactsV2(
        output_dir=target,
        bootstrap_sha256=output_hashes["bootstrap.json"],
        report_sha256=report_sha256,
        manifest_sha256=_sha256(manifest_raw),
        primary_event_count=bootstrap.primary_event_count,
        primary_outcome_count=bootstrap.primary_outcome_count,
    )


def _reconcile_sources(
    census: LoadedHistoricalThreeFamilyCensusArtifactsV2,
    fixed: LoadedHistoricalFixedHorizonArtifactsV2,
    te0: LoadedHistoricalThreeFamilyTe0ArtifactsV2,
) -> None:
    if (
        fixed.consensus_sha256 != census.consensus_sha256
        or te0.consensus_sha256 != census.consensus_sha256
        or fixed.census_rows != census.census_rows
        or te0.census_rows != census.census_rows
        or fixed.admitted_events != census.admitted_events
        or te0.admitted_events != census.admitted_events
        or fixed.execution_contract_sha256 != census.execution_contract_sha256
        or te0.execution_contract_sha256 != census.execution_contract_sha256
        or fixed.experiment_contract_sha256 != census.experiment_contract_sha256
        or te0.experiment_contract_sha256 != census.experiment_contract_sha256
        or fixed.topology_amendment_sha256 != census.topology_amendment_sha256
        or te0.topology_amendment_sha256 != census.topology_amendment_sha256
        or fixed.funding_authority_manifest_sha256 != te0.funding_authority_manifest_sha256
        or fixed.downstream_code_freeze_manifest_sha256
        != te0.downstream_code_freeze_manifest_sha256
    ):
        raise HistoricalThreeFamilyAnalysisErrorV2(
            "census, fixed-horizon, and TE0 authorities or counts do not reconcile"
        )
    fixed_ids = {row.event_id for row in fixed.rows}
    te0_ids = {row.event_id for row in te0.rows}
    if fixed_ids != te0_ids or len(fixed_ids) != census.admitted_events:
        raise HistoricalThreeFamilyAnalysisErrorV2(
            "fixed-horizon and TE0 event populations do not reconcile"
        )


def _load_optional_conflicted(
    *,
    artifact_dir: str | Path | None,
    expected_manifest_sha256: str | None,
    expected_adapter_manifest_sha256: str | None,
    expected_execution_contract_sha256: str,
    expected_downstream_code_freeze_manifest_sha256: str,
    expected_funding_authority_manifest_sha256: str,
    expected_census_manifest_sha256: str,
) -> LoadedHistoricalConflictedFixedHorizonArtifactsV1 | None:
    _validate_optional_conflicted_arguments(
        artifact_dir,
        expected_manifest_sha256,
        expected_adapter_manifest_sha256,
    )
    if artifact_dir is None:
        return None
    return load_authenticated_historical_conflicted_fixed_horizon_artifacts_v1(
        cast(str | Path, artifact_dir),
        expected_manifest_sha256=cast(str, expected_manifest_sha256),
        expected_adapter_manifest_sha256=cast(str, expected_adapter_manifest_sha256),
        expected_execution_contract_sha256=expected_execution_contract_sha256,
        expected_downstream_code_freeze_manifest_sha256=(
            expected_downstream_code_freeze_manifest_sha256
        ),
        expected_funding_authority_manifest_sha256=(expected_funding_authority_manifest_sha256),
        expected_census_manifest_sha256=expected_census_manifest_sha256,
    )


def _validate_optional_conflicted_arguments(
    artifact_dir: str | Path | None,
    expected_manifest_sha256: str | None,
    expected_adapter_manifest_sha256: str | None,
) -> None:
    supplied = (
        artifact_dir is not None,
        expected_manifest_sha256 is not None,
        expected_adapter_manifest_sha256 is not None,
    )
    if any(supplied) and not all(supplied):
        raise HistoricalThreeFamilyAnalysisErrorV2(
            "conflicted artifact directory and both external manifest hashes are atomic"
        )


def _analysis_manifest_document(
    *,
    census: LoadedHistoricalThreeFamilyCensusArtifactsV2,
    fixed: LoadedHistoricalFixedHorizonArtifactsV2,
    te0: LoadedHistoricalThreeFamilyTe0ArtifactsV2,
    conflicted: LoadedHistoricalConflictedFixedHorizonArtifactsV1 | None,
    output_hashes: Mapping[str, str],
) -> dict[str, object]:
    if set(output_hashes) != set(_OUTPUT_NAMES):
        raise HistoricalThreeFamilyAnalysisErrorV2("analysis output hash set is incomplete")
    return {
        "admitted_events": census.admitted_events,
        "bootstrap_version": HISTORICAL_THREE_FAMILY_BOOTSTRAP_VERSION_V2,
        "calendar_end_ms_exclusive": HISTORICAL_THREE_FAMILY_FULL_CALENDAR_END_MS_V2,
        "calendar_start_ms_inclusive": HISTORICAL_THREE_FAMILY_FULL_CALENDAR_START_MS_V2,
        "census_manifest_sha256": census.manifest_sha256,
        "census_rows": census.census_rows,
        "conflicted_majority_included": conflicted is not None,
        "conflicted_pooled_with_clean": False,
        "consensus_sha256": census.consensus_sha256,
        "efficacy_validated": False,
        "downstream_code_freeze_manifest_sha256": (fixed.downstream_code_freeze_manifest_sha256),
        "execution_contract_sha256": census.execution_contract_sha256,
        "experiment_contract_sha256": census.experiment_contract_sha256,
        "funding_authority_manifest_sha256": fixed.funding_authority_manifest_sha256,
        "historical_only": True,
        "inference_complete": False,
        "inputs": {
            "census": {
                "consensus_sha256": census.consensus_sha256,
                "manifest_sha256": census.manifest_sha256,
                "results_sha256": census.results_sha256,
            },
            "fixed_horizon": {
                "manifest_sha256": fixed.manifest_sha256,
                "outcomes_sha256": fixed.outcomes_sha256,
                "results_sha256": fixed.results_sha256,
            },
            "te0": {
                "manifest_sha256": te0.manifest_sha256,
                "results_sha256": te0.results_sha256,
                "technical_exit_sha256": te0.technical_exit_sha256,
            },
            "conflicted_fixed_horizon": (
                None
                if conflicted is None
                else {
                    "adapter_manifest_sha256": conflicted.adapter_manifest_sha256,
                    "manifest_sha256": conflicted.manifest_sha256,
                    "outcomes_sha256": conflicted.outcomes_sha256,
                    "results_sha256": conflicted.results_sha256,
                }
            ),
        },
        "multiplicity_adjusted": False,
        "order_placement": False,
        "outputs": dict(sorted(output_hashes.items())),
        "probability": False,
        "probability_calibrated": False,
        "promoting": False,
        "protocol": HISTORICAL_THREE_FAMILY_ANALYSIS_PROTOCOL_V2,
        "report_version": HISTORICAL_THREE_FAMILY_REPORT_VERSION_V2,
        "schedule_sha256": HISTORICAL_THREE_FAMILY_FULL_CALENDAR_SCHEDULE_SHA256_V2,
        "schema_version": HISTORICAL_THREE_FAMILY_ANALYSIS_SCHEMA_VERSION_V2,
        "source_protocols": {
            "census": HISTORICAL_THREE_FAMILY_CENSUS_PROTOCOL_V2,
            "fixed_horizon": HISTORICAL_THREE_FAMILY_FIXED_HORIZON_RUNNER_PROTOCOL_V2,
            "te0": HISTORICAL_THREE_FAMILY_TE0_PROTOCOL_V2,
        },
        "topology_amendment_sha256": census.topology_amendment_sha256,
        "topology_version": HISTORICAL_THREE_FAMILY_PRIMARY_TOPOLOGY_V2,
    }


def _publish_analysis(target: Path, payloads: Mapping[str, bytes]) -> None:
    if set(payloads) != _PUBLISHED_NAMES:
        raise HistoricalThreeFamilyAnalysisErrorV2(
            "analysis publication requires bootstrap, report, and manifest"
        )
    if target.exists():
        raise HistoricalThreeFamilyAnalysisErrorV2(
            "analysis output requires a fresh target directory"
        )
    target.parent.mkdir(parents=True, exist_ok=True)
    staging = Path(tempfile.mkdtemp(prefix=f".{target.name}.", dir=target.parent))
    try:
        for name, payload in payloads.items():
            (staging / name).write_bytes(payload)
        os.replace(staging, target)
    except OSError as exc:
        shutil.rmtree(staging, ignore_errors=True)
        raise HistoricalThreeFamilyAnalysisErrorV2(
            "cannot atomically publish analysis artifacts"
        ) from exc


def _require_exact_files(root: Path, expected: frozenset[str], label: str) -> None:
    try:
        entries = tuple(root.iterdir())
    except OSError as exc:
        raise HistoricalThreeFamilyAnalysisErrorV2(
            f"cannot inspect {label} artifact directory: {root}"
        ) from exc
    if any(entry.is_symlink() or not entry.is_file() for entry in entries) or {
        entry.name for entry in entries
    } != set(expected):
        raise HistoricalThreeFamilyAnalysisErrorV2(
            f"{label} artifact directory does not contain the exact file set"
        )


def _read_bytes(path: Path, label: str) -> bytes:
    try:
        return path.read_bytes()
    except OSError as exc:
        raise HistoricalThreeFamilyAnalysisErrorV2(f"cannot read {label}: {path}") from exc


def _decode_canonical_object(raw: bytes, label: str) -> dict[str, object]:
    try:
        value = json.loads(raw)
    except (json.JSONDecodeError, UnicodeError) as exc:
        raise HistoricalThreeFamilyAnalysisErrorV2(f"{label} is not valid UTF-8 JSON") from exc
    document = _require_mapping(value, label)
    try:
        canonical = canonical_json_line(document)
    except (TypeError, ValueError) as exc:
        raise HistoricalThreeFamilyAnalysisErrorV2(
            f"{label} contains unsupported protocol JSON"
        ) from exc
    if raw != canonical:
        raise HistoricalThreeFamilyAnalysisErrorV2(f"{label} must be canonical RFC 8785 JSONL")
    return dict(document)


def _require_mapping(value: object, label: str) -> Mapping[str, object]:
    if not isinstance(value, dict):
        raise HistoricalThreeFamilyAnalysisErrorV2(f"{label} must be an object")
    return cast(dict[str, object], value)


def _require_fields(
    document: Mapping[str, object], required: Mapping[str, object], label: str
) -> None:
    for key, expected in required.items():
        if document.get(key) != expected:
            raise HistoricalThreeFamilyAnalysisErrorV2(
                f"{label} field {key} differs from frozen authority"
            )


def _require_nonnegative_int(value: object, label: str) -> int:
    if type(value) is not int or value < 0:
        raise HistoricalThreeFamilyAnalysisErrorV2(f"{label} must be a nonnegative integer")
    return value


def _require_sha256(value: object, label: str) -> str:
    if not isinstance(value, str) or _SHA256_RE.fullmatch(value) is None:
        raise HistoricalThreeFamilyAnalysisErrorV2(f"{label} must be a lowercase SHA-256 digest")
    return value


def _sha256(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run authenticated three-family bootstrap and Korean report"
    )
    parser.add_argument("--census-artifact-dir", required=True)
    parser.add_argument("--expected-census-manifest-sha256", required=True)
    parser.add_argument("--fixed-horizon-artifact-dir", required=True)
    parser.add_argument("--expected-fixed-horizon-manifest-sha256", required=True)
    parser.add_argument("--te0-artifact-dir", required=True)
    parser.add_argument("--expected-te0-manifest-sha256", required=True)
    parser.add_argument("--expected-experiment-contract-sha256", required=True)
    parser.add_argument("--expected-topology-amendment-sha256", required=True)
    parser.add_argument("--expected-funding-authority-manifest-sha256", required=True)
    parser.add_argument("--expected-downstream-code-freeze-manifest-sha256", required=True)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--conflicted-fixed-horizon-artifact-dir")
    parser.add_argument("--expected-conflicted-fixed-horizon-manifest-sha256")
    parser.add_argument("--expected-conflicted-adapter-manifest-sha256")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    artifacts = run_historical_three_family_analysis_v2(
        census_artifact_dir=args.census_artifact_dir,
        expected_census_manifest_sha256=args.expected_census_manifest_sha256,
        fixed_horizon_artifact_dir=args.fixed_horizon_artifact_dir,
        expected_fixed_horizon_manifest_sha256=(args.expected_fixed_horizon_manifest_sha256),
        te0_artifact_dir=args.te0_artifact_dir,
        expected_te0_manifest_sha256=args.expected_te0_manifest_sha256,
        expected_experiment_contract_sha256=args.expected_experiment_contract_sha256,
        expected_topology_amendment_sha256=args.expected_topology_amendment_sha256,
        expected_funding_authority_manifest_sha256=(
            args.expected_funding_authority_manifest_sha256
        ),
        expected_downstream_code_freeze_manifest_sha256=(
            args.expected_downstream_code_freeze_manifest_sha256
        ),
        conflicted_fixed_horizon_artifact_dir=(args.conflicted_fixed_horizon_artifact_dir),
        expected_conflicted_fixed_horizon_manifest_sha256=(
            args.expected_conflicted_fixed_horizon_manifest_sha256
        ),
        expected_conflicted_adapter_manifest_sha256=(
            args.expected_conflicted_adapter_manifest_sha256
        ),
        output_dir=args.output_dir,
    )
    print(artifacts.output_dir)
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
