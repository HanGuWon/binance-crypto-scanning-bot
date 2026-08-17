from __future__ import annotations

import csv
import io
import json
from dataclasses import replace
from pathlib import Path
from typing import Any, cast

import pytest

import signalbot.backtest.indicator_analysis_v1a as v1a_module
from signalbot.backtest.indicator_analysis import FEATURE_COLUMNS
from signalbot.backtest.indicator_analysis_v1a import (
    V1A_ASSETS,
    V1A_BOOTSTRAP_BLOCK_DAYS,
    V1A_BOOTSTRAP_SAMPLES,
    V1A_BOOTSTRAP_SEED,
    V1A_EXPECTED_DATA_INPUT_KEYS,
    V1A_EXPOSURE_STATUS,
    V1A_FEATURE_POLICY,
    V1A_FREEZE_STATUS,
    V1A_HORIZONS_BARS,
    V1A_INTERVAL,
    V1A_MARKETS,
    V1A_POPULATION_FAMILIES,
    V1A_PRIMARY_HORIZON_BARS,
    V1A_REPLAY_PROTOCOL_VERSION,
    V1A_REQUIRED_FROZEN_FILE_PATHS,
    V1A_RULE_VERSION,
    V1A_SPLIT_RANGES_MS,
    V1A_SPLIT_RANGES_UTC,
    V1A_SPLITS,
    AnalyzableEventV1A,
    FeatureNotReadyV1A,
    FrozenAuthorityV1A,
    IndicatorV1AContractError,
    IntendedPopulationRowV1A,
    LoadedIndicatorV1A,
    NetOutcomeV1A,
    ValidationGateEvidenceV1A,
    bootstrap_evaluation_cell_v1a,
    build_descriptive_score_gradient_v1a,
    build_point_evaluations_v1a,
    build_shared_bootstrap_schedule_v1a,
    deduplicate_spot_priority_v1a,
    evaluate_validation_gate_v1a,
    fit_and_score_indicator_v1a,
    load_indicator_v1a_inputs,
    net_return_micros_v1a,
    run_indicator_v1a_analysis,
    score_quartile_v1a,
    sha256_bytes,
)
from signalbot.backtest.runner import source_code_digest


def _json_bytes(value: object) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode()


def _write(path: Path, value: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(value)


def _csv_bytes(fields: list[str], rows: list[dict[str, str]]) -> bytes:
    buffer = io.StringIO(newline="")
    writer = csv.DictWriter(buffer, fieldnames=fields)
    writer.writeheader()
    writer.writerows(rows)
    return buffer.getvalue().encode()


def _frozen_workspace(root: Path) -> dict[str, str]:
    assert root == v1a_module._runtime_workspace_root()
    return {
        relative: v1a_module._hash_file(root / relative)
        for relative in sorted(V1A_REQUIRED_FROZEN_FILE_PATHS)
    }


def _freeze_document(root: Path, file_hashes: dict[str, str]) -> dict[str, Any]:
    spec_path = "config/backtest.5m.indicator-discriminator-v1a-7asset.yaml"
    config_path = "config/settings.example.yaml"
    return {
        "schema_version": 1,
        "status": V1A_FREEZE_STATUS,
        "created_at_utc": "2026-07-20T04:00:00+00:00",
        "external_anchor": False,
        "historical_only": True,
        "experiment_contract": {
            "ordered_assets": list(V1A_ASSETS),
            "splits": {
                split: {
                    "start_ms": V1A_SPLIT_RANGES_MS[split][0],
                    "end_ms": V1A_SPLIT_RANGES_MS[split][1],
                }
                for split in V1A_SPLITS
            },
            "markets": list(V1A_MARKETS),
            "interval": V1A_INTERVAL,
            "horizons_bars": list(V1A_HORIZONS_BARS),
            "primary_horizon_bars": V1A_PRIMARY_HORIZON_BARS,
            "replay_protocol_version": V1A_REPLAY_PROTOCOL_VERSION,
            "rule_version": V1A_RULE_VERSION,
            "population": {
                "information_only": True,
                "stage": "setup",
                "families": list(V1A_POPULATION_FAMILIES),
                "score": "100",
            },
            "feature_policy": V1A_FEATURE_POLICY,
            "deduplication": {
                "key": ["asset", "direction", "decision_time_ms"],
                "priority": ["spot", "futures"],
            },
            "bootstrap": {
                "block_days": V1A_BOOTSTRAP_BLOCK_DAYS,
                "samples": V1A_BOOTSTRAP_SAMPLES,
                "seed": V1A_BOOTSTRAP_SEED,
            },
        },
        "spec_sha256": file_hashes[spec_path],
        "spec_semantics_sha256": v1a_module._semantic_sha256(
            v1a_module._load_and_validate_v1a_spec(root / spec_path).model_dump(
                mode="json"
            )
        ),
        "config_sha256": file_hashes[config_path],
        "settings_semantics_sha256": v1a_module._semantic_sha256(
            v1a_module._load_settings_without_environment(
                root / config_path
            ).model_dump(mode="json")
        ),
        "source_code_sha256": source_code_digest(root),
        "expected_input_count": len(V1A_EXPECTED_DATA_INPUT_KEYS),
        "data_input_sha256": {
            key: v1a_module._hash_file(root / "data" / "backtest" / key)
            for key in sorted(V1A_EXPECTED_DATA_INPUT_KEYS)
        },
        "file_sha256": file_hashes,
        "exposure_status": V1A_EXPOSURE_STATUS,
        "independent_validation_claim_allowed": False,
        "deployment_approved": False,
        "probability_calibrated": False,
    }


def _recommendation_fields() -> list[str]:
    return list(v1a_module._RECOMMENDATION_EXACT_COLUMNS)


def _replay_rows(
    split: str,
    *,
    missing_feature: bool = False,
    spot_duplicate: bool = False,
    orphan_outcome: bool = False,
    bad_net_return: str | None = None,
) -> tuple[list[dict[str, str]], list[dict[str, str]]]:
    recommendations: list[dict[str, str]] = []
    start_ms = V1A_SPLIT_RANGES_MS[split][0]
    for index, asset in enumerate(V1A_ASSETS):
        market = "spot" if index % 2 == 0 else "futures"
        direction = "long" if index % 2 == 0 else "short"
        family = "pullback_long" if direction == "long" else "pullback_short"
        row = dict.fromkeys(_recommendation_fields(), "")
        row.update({
            "event_id": f"{split}-{asset}-{market}",
            "protocol_version": V1A_REPLAY_PROTOCOL_VERSION,
            "rule_version": V1A_RULE_VERSION,
            "asset": asset,
            "cohort": "volatile",
            "market": market,
            "symbol": v1a_module._V1A_SYMBOL_BY_PAIR[(asset, market)],
            "family": family,
            "direction": direction,
            "stage": "setup",
            "information_only": "True",
            "score": "100",
            "decision_time_ms": str(start_ms + (80 + index) * 300_000 - 1),
            "split": split,
            "price": "1.25",
            **{name: str(index + 1) for name in FEATURE_COLUMNS},
        })
        recommendations.append(row)
    if missing_feature:
        recommendations[0][FEATURE_COLUMNS[0]] = ""
    if spot_duplicate:
        base = recommendations[0]
        base["market"] = "spot"
        duplicate = dict(base)
        duplicate["event_id"] = f"{split}-{V1A_ASSETS[0]}-futures-duplicate"
        duplicate["market"] = "futures"
        duplicate["symbol"] = v1a_module._V1A_SYMBOL_BY_PAIR[
            (V1A_ASSETS[0], "futures")
        ]
        duplicate[FEATURE_COLUMNS[0]] = "999"
        recommendations.append(duplicate)
    outcomes: list[dict[str, str]] = []
    for recommendation in recommendations:
        for horizon in V1A_HORIZONS_BARS:
            direction = recommendation["direction"]
            market = recommendation["market"]
            entry_time_ms = int(recommendation["decision_time_ms"]) + 1
            exit_time_ms = entry_time_ms + horizon * 300_000 - 1
            entry_price = 100.0
            exit_price = 100.4 if direction == "long" else 99.6
            direction_enum = (
                v1a_module.Direction.LONG
                if direction == "long"
                else v1a_module.Direction.SHORT
            )
            fee_bps, slippage_bps = v1a_module._V1A_EXECUTION_COST_BPS[
                (market, "volatile")
            ]
            execution = v1a_module.calculate_execution_returns(
                direction_enum,
                entry_price,
                exit_price,
                fee_bps,
                slippage_bps,
            )
            funding_return = 0.0 if market == "spot" else 0.0001
            expected_net = execution.net_before_funding + funding_return
            net_return = repr(expected_net)
            if bad_net_return is not None and horizon == V1A_PRIMARY_HORIZON_BARS:
                net_return = bad_net_return
            maximum_rise = 0.01
            maximum_drop = -0.01
            outcome = dict.fromkeys(v1a_module._OUTCOME_EXACT_COLUMNS, "")
            outcome.update(
                {
                    "event_id": recommendation["event_id"],
                    "horizon_bars": str(horizon),
                    "horizon_minutes": str(horizon * 5),
                    "evaluable": "True",
                    "exclusion_reason": "",
                    "entry_time_ms": str(entry_time_ms),
                    "exit_time_ms": str(exit_time_ms),
                    "entry_price": repr(entry_price),
                    "exit_price": repr(exit_price),
                    "raw_close_return": repr(exit_price / entry_price - 1),
                    "maximum_rise": repr(maximum_rise),
                    "maximum_drop": repr(maximum_drop),
                    "gross_return": repr(execution.gross_return),
                    "slippage_return": repr(execution.slippage_return),
                    "fee_return": repr(execution.fee_return),
                    "funding_return": repr(funding_return),
                    "net_return": net_return,
                    "mfe": repr(
                        maximum_rise if direction == "long" else -maximum_drop
                    ),
                    "mae": repr(
                        maximum_drop if direction == "long" else -maximum_rise
                    ),
                    "hit_status_5bps": v1a_module._hit_status_v1a(
                        expected_net, 5.0
                    ),
                    "hit_status_0bps": v1a_module._hit_status_v1a(
                        expected_net, 0.0
                    ),
                    "hit_status_10bps": v1a_module._hit_status_v1a(
                        expected_net, 10.0
                    ),
                    "hit_status_25bps": v1a_module._hit_status_v1a(
                        expected_net, 25.0
                    ),
                    "one_r_path_status": "invalid_invalidation",
                    "one_r_target_price": "",
                    "one_r_risk_fraction": "",
                    "observed_until_ms": str(exit_time_ms),
                }
            )
            outcomes.append(outcome)
    if orphan_outcome:
        outcomes[0]["event_id"] = "orphan"
    return recommendations, outcomes


def _parse_rows_directly(
    split: str,
    recommendations: list[dict[str, str]],
    outcomes: list[dict[str, str]],
) -> v1a_module._OutcomesV1A:
    parsed_recommendations = v1a_module._parse_recommendations(
        _csv_bytes(_recommendation_fields(), recommendations),
        expected_split=split,
    )
    return v1a_module._parse_outcomes(
        _csv_bytes(list(v1a_module._OUTCOME_EXACT_COLUMNS), outcomes),
        recommendations=parsed_recommendations,
    )


def _exclude_outcome_row(row: dict[str, str], reason: str) -> None:
    row["evaluable"] = "False"
    row["exclusion_reason"] = reason
    for field in (
        "entry_time_ms",
        "exit_time_ms",
        "entry_price",
        "exit_price",
        "raw_close_return",
        "maximum_rise",
        "maximum_drop",
        "gross_return",
        "slippage_return",
        "fee_return",
        "funding_return",
        "net_return",
        "mfe",
        "mae",
        "one_r_target_price",
        "one_r_risk_fraction",
        "observed_until_ms",
    ):
        row[field] = ""
    for field in (
        "hit_status_5bps",
        "hit_status_0bps",
        "hit_status_10bps",
        "hit_status_25bps",
        "one_r_path_status",
    ):
        row[field] = "unevaluable"


def _refresh_event_times(
    recommendation: dict[str, str], outcomes: list[dict[str, str]]
) -> None:
    entry_time_ms = int(recommendation["decision_time_ms"]) + 1
    for row in outcomes:
        if row["event_id"] != recommendation["event_id"]:
            continue
        horizon = int(row["horizon_bars"])
        exit_time_ms = entry_time_ms + horizon * 300_000 - 1
        row["entry_time_ms"] = str(entry_time_ms)
        row["exit_time_ms"] = str(exit_time_ms)
        row["observed_until_ms"] = str(exit_time_ms)


def _write_replay(
    root: Path,
    split: str,
    freeze: dict[str, Any],
    *,
    missing_feature: bool = False,
    spot_duplicate: bool = False,
    orphan_outcome: bool = False,
    bad_net_return: str | None = None,
    costs_fee: float = 5.0,
) -> Path:
    replay = root / f"replay-{split}"
    replay.mkdir(parents=True)
    recommendations, outcomes = _replay_rows(
        split,
        missing_feature=missing_feature,
        spot_duplicate=spot_duplicate,
        orphan_outcome=orphan_outcome,
        bad_net_return=bad_net_return,
    )
    recommendation_raw = _csv_bytes(_recommendation_fields(), recommendations)
    outcome_raw = _csv_bytes(
        list(v1a_module._OUTCOME_EXACT_COLUMNS),
        outcomes,
    )
    start_utc, end_utc = V1A_SPLIT_RANGES_UTC[split]
    results = {
        "protocol_version": V1A_REPLAY_PROTOCOL_VERSION,
        "rule_version": V1A_RULE_VERSION,
        "events": len(recommendations),
        "outcome_rows": len(outcomes),
        "per_symbol": [
            {
                "market": market,
                "asset": asset,
                "candles": 1_000,
                "events": sum(
                    row["market"] == market and row["asset"] == asset
                    for row in recommendations
                ),
                "outcome_rows": sum(
                    row["market"] == market and row["asset"] == asset
                    for row in recommendations
                )
                * len(V1A_HORIZONS_BARS),
                "duration_seconds": 0.1,
            }
            for market in V1A_MARKETS
            for asset in V1A_ASSETS
        ],
        "selection": {
            "start_utc": start_utc,
            "end_utc": end_utc,
            "splits": [split],
            "interval": V1A_INTERVAL,
            "assets": list(V1A_ASSETS),
            "markets": list(V1A_MARKETS),
            "universe_mode": "fixed_backtest_spec_assets_not_live_dynamic_top_n",
        },
        "evaluation_contract": {
            "horizons_bars": list(V1A_HORIZONS_BARS),
            "primary_horizon_bars": V1A_PRIMARY_HORIZON_BARS,
            "costs": {"futures_fee_bps": costs_fee, "include_funding": True},
        },
        "status": {"independently_validated": False, "deployment_approved": False},
    }
    output_raw = {
        "recommendations.csv": recommendation_raw,
        "outcomes.csv": outcome_raw,
        "results.json": _json_bytes(results),
        "report_ko.md": "역사 진단만 허용\n".encode(),
    }
    for name, raw in output_raw.items():
        _write(replay / name, raw)
    manifest = {
        "protocol_version": V1A_REPLAY_PROTOCOL_VERSION,
        "rule_version": V1A_RULE_VERSION,
        "spec_sha256": freeze["spec_sha256"],
        "config_sha256": freeze["config_sha256"],
        "code_sha256": freeze["source_code_sha256"],
        "spec_path": str(
            v1a_module._runtime_workspace_root()
            / "config/backtest.5m.indicator-discriminator-v1a-7asset.yaml"
        ),
        "config_path": str(
            v1a_module._runtime_workspace_root() / "config/settings.example.yaml"
        ),
        "started_at_utc": "2026-07-20T05:00:00+00:00",
        "completed_at_utc": "2026-07-20T05:00:01+00:00",
        "duration_seconds": 1.0,
        "inputs": freeze["data_input_sha256"],
        "outputs": {name: sha256_bytes(raw) for name, raw in output_raw.items()},
    }
    _write(replay / "run_manifest.json", _json_bytes(manifest))
    return replay


@pytest.fixture
def v1a_fixture(tmp_path: Path) -> tuple[Path, Path, dict[str, Any]]:
    workspace = v1a_module._runtime_workspace_root()
    missing_external_inputs = [
        workspace / "data" / "backtest" / relative
        for relative in sorted(V1A_EXPECTED_DATA_INPUT_KEYS)
        if not (workspace / "data" / "backtest" / relative).is_file()
    ]
    if missing_external_inputs:
        pytest.skip(
            "requires the ignored local V1A historical corpus; "
            "unit and contract tests remain runnable without publishing it"
        )
    file_hashes = _frozen_workspace(workspace)
    freeze = _freeze_document(workspace, file_hashes)
    freeze_path = tmp_path / "freeze_manifest.json"
    _write(freeze_path, _json_bytes(freeze))
    return workspace, freeze_path, freeze


def test_v1a_loads_three_order_invariant_complete_replays(
    v1a_fixture: tuple[Path, Path, dict[str, Any]],
) -> None:
    workspace, freeze_path, freeze = v1a_fixture
    replay_dirs = [
        _write_replay(freeze_path.parent, split, freeze)
        for split in V1A_SPLITS
    ]

    loaded = load_indicator_v1a_inputs(
        freeze_manifest_path=freeze_path,
        replay_dirs=tuple(reversed(replay_dirs)),
        workspace_root=workspace,
    )

    assert len(loaded.events) == len(V1A_ASSETS) * len(V1A_SPLITS)
    assert not loaded.feature_not_ready
    assert [audit.split for audit in loaded.audits] == list(V1A_SPLITS)
    assert all(len(event.outcomes) == len(V1A_HORIZONS_BARS) for event in loaded.events)
    expected_spot = v1a_module.calculate_execution_returns(
        v1a_module.Direction.LONG,
        100.0,
        100.4,
        10.0,
        10.0,
    ).net_before_funding
    assert loaded.events[0].outcomes[0].net_return_micros == net_return_micros_v1a(
        repr(expected_spot)
    )
    assert loaded.independently_validated is False
    assert loaded.deployment_approved is False
    assert loaded.probability_calibrated is False


def test_v1a_authority_binds_runtime_root_semantics_dependencies_and_data(
    v1a_fixture: tuple[Path, Path, dict[str, Any]],
    tmp_path: Path,
) -> None:
    workspace, freeze_path, freeze = v1a_fixture
    replay_dirs = [
        _write_replay(freeze_path.parent, split, freeze) for split in V1A_SPLITS
    ]
    assert {".python-version", "pyproject.toml", "uv.lock"} <= set(
        freeze["file_sha256"]
    )

    with pytest.raises(IndicatorV1AContractError, match="executing V1A module"):
        load_indicator_v1a_inputs(
            freeze_manifest_path=freeze_path,
            replay_dirs=replay_dirs,
            workspace_root=tmp_path,
        )

    semantic_drift = dict(freeze)
    semantic_drift["spec_semantics_sha256"] = "0" * 64
    semantic_path = freeze_path.parent / "semantic-drift.json"
    _write(semantic_path, _json_bytes(semantic_drift))
    with pytest.raises(IndicatorV1AContractError, match="semantic hash"):
        load_indicator_v1a_inputs(
            freeze_manifest_path=semantic_path,
            replay_dirs=replay_dirs,
            workspace_root=workspace,
        )

    data_drift = dict(freeze)
    data_drift["data_input_sha256"] = dict(freeze["data_input_sha256"])
    first_key = min(V1A_EXPECTED_DATA_INPUT_KEYS)
    data_drift["data_input_sha256"][first_key] = "0" * 64
    data_path = freeze_path.parent / "data-drift.json"
    _write(data_path, _json_bytes(data_drift))
    with pytest.raises(IndicatorV1AContractError, match="data input hash mismatch"):
        load_indicator_v1a_inputs(
            freeze_manifest_path=data_path,
            replay_dirs=replay_dirs,
            workspace_root=workspace,
        )


def test_v1a_uses_env_disabled_settings_and_strict_canonical_time_order(
    v1a_fixture: tuple[Path, Path, dict[str, Any]],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    workspace, freeze_path, freeze = v1a_fixture
    replay_dirs = [
        _write_replay(freeze_path.parent, split, freeze) for split in V1A_SPLITS
    ]
    monkeypatch.setenv("SIGNALBOT_LOG_LEVEL", "CRITICAL")
    loaded = load_indicator_v1a_inputs(
        freeze_manifest_path=freeze_path,
        replay_dirs=replay_dirs,
        workspace_root=workspace,
    )
    assert loaded.authority.settings_semantics_sha256 == freeze[
        "settings_semantics_sha256"
    ]

    noncanonical = dict(freeze)
    noncanonical["created_at_utc"] = "2026-07-20T04:00:00Z"
    noncanonical_path = freeze_path.parent / "noncanonical-freeze.json"
    _write(noncanonical_path, _json_bytes(noncanonical))
    with pytest.raises(IndicatorV1AContractError, match=r"explicit \+00:00"):
        load_indicator_v1a_inputs(
            freeze_manifest_path=noncanonical_path,
            replay_dirs=replay_dirs,
            workspace_root=workspace,
        )

    run_manifest_path = replay_dirs[0] / "run_manifest.json"
    run_manifest = json.loads(run_manifest_path.read_text(encoding="utf-8"))
    run_manifest["started_at_utc"] = "2026-07-20T03:59:59+00:00"
    run_manifest["completed_at_utc"] = "2026-07-20T04:00:00+00:00"
    _write(run_manifest_path, _json_bytes(run_manifest))
    with pytest.raises(IndicatorV1AContractError, match="freeze < start"):
        load_indicator_v1a_inputs(
            freeze_manifest_path=freeze_path,
            replay_dirs=replay_dirs,
            workspace_root=workspace,
        )


def test_spot_priority_precedes_feature_not_ready_no_call(
    v1a_fixture: tuple[Path, Path, dict[str, Any]],
) -> None:
    workspace, freeze_path, freeze = v1a_fixture
    replay_dirs = [
        _write_replay(
            freeze_path.parent,
            split,
            freeze,
            missing_feature=split == "development",
            spot_duplicate=split == "development",
        )
        for split in V1A_SPLITS
    ]

    loaded = load_indicator_v1a_inputs(
        freeze_manifest_path=freeze_path,
        replay_dirs=replay_dirs,
        workspace_root=workspace,
    )

    development = loaded.audits[0]
    assert development.intended_population_rows == len(V1A_ASSETS) + 1
    assert development.deduplicated_population_rows == len(V1A_ASSETS)
    assert development.duplicate_rows_dropped == 1
    assert development.complete_case_rows == len(V1A_ASSETS) - 1
    assert development.feature_not_ready_rows == 1
    assert loaded.feature_not_ready[0].missing_features == (FEATURE_COLUMNS[0],)
    assert len(development.feature_not_ready_sha256) == 64


def test_freeze_and_output_mutation_fail_closed(
    v1a_fixture: tuple[Path, Path, dict[str, Any]],
) -> None:
    workspace, freeze_path, freeze = v1a_fixture
    replay_dirs = [
        _write_replay(freeze_path.parent, split, freeze) for split in V1A_SPLITS
    ]
    document = json.loads(freeze_path.read_text(encoding="utf-8"))
    document["status"] = "DRAFT"
    freeze_path.write_bytes(_json_bytes(document))
    with pytest.raises(IndicatorV1AContractError, match="not sealed"):
        load_indicator_v1a_inputs(
            freeze_manifest_path=freeze_path,
            replay_dirs=replay_dirs,
            workspace_root=workspace,
        )

    freeze_path.write_bytes(_json_bytes(freeze))
    with (replay_dirs[0] / "recommendations.csv").open("ab") as handle:
        handle.write(b"tamper")
    with pytest.raises(IndicatorV1AContractError, match="output hash mismatch"):
        load_indicator_v1a_inputs(
            freeze_manifest_path=freeze_path,
            replay_dirs=replay_dirs,
            workspace_root=workspace,
        )


def test_orphan_and_nonfinite_outcomes_fail_closed(
    v1a_fixture: tuple[Path, Path, dict[str, Any]],
) -> None:
    workspace, freeze_path, freeze = v1a_fixture
    orphan_dirs = [
        _write_replay(
            freeze_path.parent,
            split,
            freeze,
            orphan_outcome=split == "validation",
        )
        for split in V1A_SPLITS
    ]
    with pytest.raises(IndicatorV1AContractError, match="orphan event_id"):
        load_indicator_v1a_inputs(
            freeze_manifest_path=freeze_path,
            replay_dirs=orphan_dirs,
            workspace_root=workspace,
        )

    second = freeze_path.parent / "second"
    second_freeze = freeze
    second_freeze_path = second / "freeze_manifest.json"
    _write(second_freeze_path, _json_bytes(second_freeze))
    nonfinite_dirs = [
        _write_replay(
            second,
            split,
            second_freeze,
            bad_net_return="NaN" if split == "validation" else None,
        )
        for split in V1A_SPLITS
    ]
    with pytest.raises(IndicatorV1AContractError, match="finite decimal"):
        load_indicator_v1a_inputs(
            freeze_manifest_path=second_freeze_path,
            replay_dirs=nonfinite_dirs,
            workspace_root=workspace,
        )


def test_cross_split_cost_identity_and_directory_cardinality_are_exact(
    v1a_fixture: tuple[Path, Path, dict[str, Any]],
) -> None:
    workspace, freeze_path, freeze = v1a_fixture
    replay_dirs = [
        _write_replay(
            freeze_path.parent,
            split,
            freeze,
            costs_fee=6.0 if split == "retrospective_test" else 5.0,
        )
        for split in V1A_SPLITS
    ]
    with pytest.raises(IndicatorV1AContractError, match="cost contract differs"):
        load_indicator_v1a_inputs(
            freeze_manifest_path=freeze_path,
            replay_dirs=replay_dirs,
            workspace_root=workspace,
        )
    with pytest.raises(IndicatorV1AContractError, match="exactly three"):
        load_indicator_v1a_inputs(
            freeze_manifest_path=freeze_path,
            replay_dirs=replay_dirs[:2],
            workspace_root=workspace,
        )


def test_decimal_micros_half_even_boundaries() -> None:
    assert net_return_micros_v1a("0.0000005") == 0
    assert net_return_micros_v1a("0.0000015") == 2
    assert net_return_micros_v1a("-0.0000015") == -2
    with pytest.raises(IndicatorV1AContractError, match="finite decimal"):
        net_return_micros_v1a("Infinity")


def test_exact_outcome_schema_reconciles_metadata_costs_paths_and_funding() -> None:
    recommendations, outcomes = _replay_rows("validation")

    parsed = _parse_rows_directly("validation", recommendations, outcomes)

    assert parsed.row_count == len(recommendations) * len(V1A_HORIZONS_BARS)
    spot_id = recommendations[0]["event_id"]
    futures_id = recommendations[1]["event_id"]
    assert all(row.evaluable for row in parsed.by_event_id[spot_id])
    assert all(row.evaluable for row in parsed.by_event_id[futures_id])


@pytest.mark.parametrize(
    ("field", "value", "message"),
    [
        ("exclusion_reason", "data_gap_in_horizon", "exclusion reason"),
        ("entry_time_ms", "1740816300001", "entry/exit times"),
        ("exit_time_ms", "1740819899998", "entry/exit times"),
        ("entry_price", "0", "prices must be positive"),
        ("raw_close_return", "0.4", "raw_close_return"),
        ("maximum_rise", "0.001", "maximum rise/drop"),
        ("gross_return", "0.4", "gross_return"),
        ("slippage_return", "0.4", "slippage_return"),
        ("fee_return", "0.4", "fee_return"),
        ("funding_return", "0.0001", "Spot funding_return"),
        ("net_return", "0.4", "net_return"),
        ("mfe", "0.4", "mfe"),
        ("mae", "-0.4", "mae"),
        ("hit_status_5bps", "hit", "hit_status_5bps"),
        ("one_r_path_status", "timeout", "invalid-invalidation fields"),
    ],
)
def test_evaluable_outcome_integrity_mutations_fail_closed(
    field: str,
    value: str,
    message: str,
) -> None:
    recommendations, outcomes = _replay_rows("validation")
    target = next(
        row
        for row in outcomes
        if row["event_id"] == recommendations[0]["event_id"]
        and row["horizon_bars"] == "12"
    )
    target[field] = value

    with pytest.raises(IndicatorV1AContractError, match=message):
        _parse_rows_directly("validation", recommendations, outcomes)


def test_outcome_float_tolerance_has_a_tight_deterministic_boundary() -> None:
    recommendations, outcomes = _replay_rows("validation")
    target = next(
        row
        for row in outcomes
        if row["event_id"] == recommendations[0]["event_id"]
        and row["horizon_bars"] == "12"
    )
    original = float(target["gross_return"])
    target["gross_return"] = repr(original + 5e-13)
    _parse_rows_directly("validation", recommendations, outcomes)

    target["gross_return"] = repr(original + 5e-10)
    with pytest.raises(IndicatorV1AContractError, match="gross_return"):
        _parse_rows_directly("validation", recommendations, outcomes)


def test_unevaluable_outcome_requires_empty_payload_and_exact_reason() -> None:
    recommendations, outcomes = _replay_rows("validation")
    target = next(
        row
        for row in outcomes
        if row["event_id"] == recommendations[0]["event_id"]
        and row["horizon_bars"] == "12"
    )
    _exclude_outcome_row(target, "data_gap_in_horizon")
    _parse_rows_directly("validation", recommendations, outcomes)

    target["net_return"] = "0"
    with pytest.raises(IndicatorV1AContractError, match="numeric or path data"):
        _parse_rows_directly("validation", recommendations, outcomes)
    target["net_return"] = ""
    target["hit_status_5bps"] = "ambiguous"
    with pytest.raises(IndicatorV1AContractError, match="non-unevaluable status"):
        _parse_rows_directly("validation", recommendations, outcomes)


def test_split_start_embargo_boundary_and_reason_are_exact() -> None:
    split = "validation"
    start_ms = V1A_SPLIT_RANGES_MS[split][0]
    recommendations, outcomes = _replay_rows(split)
    event = recommendations[0]
    event["decision_time_ms"] = str(start_ms + 72 * 300_000 - 1)
    _refresh_event_times(event, outcomes)
    _parse_rows_directly(split, recommendations, outcomes)

    event["decision_time_ms"] = str(start_ms + 71 * 300_000 - 1)
    for row in outcomes:
        if row["event_id"] == event["event_id"]:
            _exclude_outcome_row(row, "split_start_embargo")
    _parse_rows_directly(split, recommendations, outcomes)

    first = next(row for row in outcomes if row["event_id"] == event["event_id"])
    first["exclusion_reason"] = "data_gap_in_horizon"
    with pytest.raises(IndicatorV1AContractError, match="split_start_embargo"):
        _parse_rows_directly(split, recommendations, outcomes)


def test_end_of_split_outcomes_must_be_exact_exclusions() -> None:
    split = "validation"
    end_ms = V1A_SPLIT_RANGES_MS[split][1]
    recommendations, outcomes = _replay_rows(split)
    event = recommendations[0]
    event["decision_time_ms"] = str(end_ms - 1)
    for row in outcomes:
        if row["event_id"] == event["event_id"]:
            _exclude_outcome_row(row, "horizon_crosses_split")
    _parse_rows_directly(split, recommendations, outcomes)

    first = next(row for row in outcomes if row["event_id"] == event["event_id"])
    first["exclusion_reason"] = "insufficient_1_bar_horizon"
    _parse_rows_directly(split, recommendations, outcomes)
    first["exclusion_reason"] = "split_start_embargo"
    _parse_rows_directly(split, recommendations, outcomes)
    first["exclusion_reason"] = "next_bar_unavailable"
    with pytest.raises(IndicatorV1AContractError, match="split-end exclusion"):
        _parse_rows_directly(split, recommendations, outcomes)


def test_hit_status_threshold_equalities_are_ambiguous() -> None:
    recommendations, outcomes = _replay_rows("validation")
    futures = recommendations[1]
    target_net_by_horizon = {
        1: 0.0,
        3: 0.0005,
        6: -0.001,
        12: 0.0025,
        72: -0.0025,
    }
    for row in outcomes:
        if row["event_id"] != futures["event_id"]:
            continue
        target_net = target_net_by_horizon[int(row["horizon_bars"])]
        execution_before_funding = (
            float(row["gross_return"])
            - float(row["slippage_return"])
            - float(row["fee_return"])
        )
        row["funding_return"] = repr(target_net - execution_before_funding)
        row["net_return"] = repr(target_net)
        for field, margin in (
            ("hit_status_0bps", 0.0),
            ("hit_status_5bps", 5.0),
            ("hit_status_10bps", 10.0),
            ("hit_status_25bps", 25.0),
        ):
            row[field] = v1a_module._hit_status_v1a(target_net, margin)

    _parse_rows_directly("validation", recommendations, outcomes)
    rows_by_horizon = {
        int(row["horizon_bars"]): row
        for row in outcomes
        if row["event_id"] == futures["event_id"]
    }
    assert rows_by_horizon[1]["hit_status_0bps"] == "ambiguous"
    assert rows_by_horizon[3]["hit_status_5bps"] == "ambiguous"
    assert rows_by_horizon[6]["hit_status_10bps"] == "ambiguous"
    assert rows_by_horizon[12]["hit_status_25bps"] == "ambiguous"
    assert rows_by_horizon[72]["hit_status_25bps"] == "ambiguous"


def test_one_r_collision_fields_have_exact_geometry_and_path_support() -> None:
    recommendations, outcomes = _replay_rows("validation")
    event = recommendations[0]
    event["invalidation"] = "99.0"
    for row in outcomes:
        if row["event_id"] != event["event_id"]:
            continue
        row["one_r_path_status"] = "collision"
        row["one_r_target_price"] = "101.0"
        row["one_r_risk_fraction"] = "0.01"
    _parse_rows_directly("validation", recommendations, outcomes)

    first = next(row for row in outcomes if row["event_id"] == event["event_id"])
    first["maximum_drop"] = "-0.009"
    first["mae"] = "-0.009"
    with pytest.raises(IndicatorV1AContractError, match="collision"):
        _parse_rows_directly("validation", recommendations, outcomes)


@pytest.mark.parametrize(
    ("direction", "maximum_rise", "maximum_drop", "risk", "expected"),
    [
        (
            "long",
            0.004563786881766063,
            -0.0000530672893228612,
            0.0045637868817661655,
            (0, -1),
        ),
        (
            "short",
            0.0011207621182405259,
            -0.0033622863547211335,
            0.0033622863547211512,
            (0, -1),
        ),
        ("long", 0.01 - 0.5e-12, -(0.01 + 0.5e-12), 0.01, (0, 0)),
        ("long", 0.01 - 2e-12, -(0.01 + 2e-12), 0.01, (-1, 1)),
    ],
)
def test_one_r_excursion_relation_preserves_float_boundary_ambiguity(
    direction: str,
    maximum_rise: float,
    maximum_drop: float,
    risk: float,
    expected: tuple[int, int],
) -> None:
    assert v1a_module._one_r_touch_relations_v1a(
        cast(Any, direction),
        maximum_rise,
        maximum_drop,
        risk,
    ) == expected


def test_one_r_timeout_accepts_boundary_but_rejects_definite_touch() -> None:
    recommendations, outcomes = _replay_rows("validation")
    event = recommendations[0]
    event["invalidation"] = "99.0"
    for row in outcomes:
        if row["event_id"] != event["event_id"]:
            continue
        row["one_r_path_status"] = "timeout"
        row["one_r_target_price"] = "101.0"
        row["one_r_risk_fraction"] = "0.01"
        row["maximum_rise"] = repr(0.01 - 0.5e-12)
        row["maximum_drop"] = "-0.005"
        row["mfe"] = row["maximum_rise"]
        row["mae"] = row["maximum_drop"]
    _parse_rows_directly("validation", recommendations, outcomes)

    first = next(row for row in outcomes if row["event_id"] == event["event_id"])
    first["maximum_rise"] = repr(0.01 + 2e-12)
    first["mfe"] = first["maximum_rise"]
    with pytest.raises(IndicatorV1AContractError, match="timeout"):
        _parse_rows_directly("validation", recommendations, outcomes)


def test_one_r_hit_accepts_boundary_but_rejects_definite_shortfall() -> None:
    recommendations, outcomes = _replay_rows("validation")
    event = recommendations[0]
    event["invalidation"] = "99.0"
    for row in outcomes:
        if row["event_id"] != event["event_id"]:
            continue
        row["one_r_path_status"] = "target_first"
        row["one_r_target_price"] = "101.0"
        row["one_r_risk_fraction"] = "0.01"
        row["maximum_rise"] = repr(0.01 - 0.5e-12)
        row["maximum_drop"] = "-0.005"
        row["mfe"] = row["maximum_rise"]
        row["mae"] = row["maximum_drop"]
    _parse_rows_directly("validation", recommendations, outcomes)

    first = next(row for row in outcomes if row["event_id"] == event["event_id"])
    first["maximum_rise"] = repr(0.01 - 2e-12)
    first["mfe"] = first["maximum_rise"]
    with pytest.raises(IndicatorV1AContractError, match="target status"):
        _parse_rows_directly("validation", recommendations, outcomes)


def test_ambiguous_same_market_duplicate_is_rejected() -> None:
    row = IntendedPopulationRowV1A(
        event_id="one",
        asset="BONK",
        market="spot",
        direction="long",
        decision_time_ms=V1A_SPLIT_RANGES_MS["development"][0] + 1,
        split="development",
        features=(1.0,) * len(FEATURE_COLUMNS),
    )
    duplicate = IntendedPopulationRowV1A(
        event_id="two",
        asset=row.asset,
        market=row.market,
        direction=row.direction,
        decision_time_ms=row.decision_time_ms,
        split=row.split,
        features=row.features,
    )
    with pytest.raises(IndicatorV1AContractError, match="ambiguous duplicate"):
        deduplicate_spot_priority_v1a((row, duplicate))


def _outcomes(*values: int | None) -> tuple[NetOutcomeV1A, ...]:
    assert len(values) == len(V1A_HORIZONS_BARS)
    return tuple(
        NetOutcomeV1A(
            horizon_bars=horizon,
            evaluable=value is not None,
            net_return_micros=value,
        )
        for horizon, value in zip(V1A_HORIZONS_BARS, values, strict=True)
    )


def _loaded_for_analysis() -> LoadedIndicatorV1A:
    events: list[AnalyzableEventV1A] = []
    not_ready: list[FeatureNotReadyV1A] = []
    for split_index, split in enumerate(V1A_SPLITS):
        start = V1A_SPLIT_RANGES_MS[split][0]
        for index, asset in enumerate(V1A_ASSETS):
            direction = "long" if index % 2 == 0 else "short"
            events.append(
                AnalyzableEventV1A(
                    event_id=f"{split}-{asset}-complete",
                    asset=asset,
                    market="spot",
                    direction=direction,
                    decision_time_ms=start + (index + 1) * 300_000,
                    split=split,
                    features=tuple(
                        float(index + feature_index + split_index)
                        for feature_index in range(len(FEATURE_COLUMNS))
                    ),
                    outcomes=_outcomes(10, -5, 30, 40 + index, None),
                )
            )
        not_ready.append(
            FeatureNotReadyV1A(
                event_id=f"{split}-not-ready",
                asset=V1A_ASSETS[0],
                market="futures",
                direction="long",
                decision_time_ms=start + 20 * 300_000,
                split=split,
                missing_features=(FEATURE_COLUMNS[0],),
                outcomes=_outcomes(-10, -10, -10, -100, -10),
            )
        )
    authority = FrozenAuthorityV1A(
        manifest_path=Path("freeze.json"),
        manifest_sha256="0" * 64,
        created_at_utc="2026-07-20T04:00:00+00:00",
        created_at_ms=1_753_070_400_000,
        created_at_datetime=v1a_module.datetime(
            2026, 7, 20, 4, tzinfo=v1a_module.UTC
        ),
        spec_sha256="1" * 64,
        spec_semantics_sha256="4" * 64,
        config_sha256="2" * 64,
        settings_semantics_sha256="5" * 64,
        source_code_sha256="3" * 64,
        data_input_sha256=(),
        file_sha256=(),
    )
    return LoadedIndicatorV1A(
        authority=authority,
        events=tuple(events),
        feature_not_ready=tuple(not_ready),
        audits=(),
    )


def test_v1a_reuses_outcome_blind_complete_case_score_and_retains_no_calls() -> None:
    loaded = _loaded_for_analysis()
    model, scored = fit_and_score_indicator_v1a(loaded)
    altered = replace(
        loaded,
        events=tuple(
            replace(
                event,
                outcomes=_outcomes(-999, -999, -999, -999, -999),
            )
            for event in loaded.events
        ),
    )
    altered_model, altered_scored = fit_and_score_indicator_v1a(altered)

    assert model.development_rows == len(V1A_ASSETS)
    assert model.scored_development_rows == len(V1A_ASSETS)
    assert len(scored) == len(loaded.events)
    assert all(len(row.axis_scores) == 4 for row in scored)
    assert model == altered_model
    assert [
        (row.event.event_id, row.axis_scores, row.composite_score, row.selected)
        for row in scored
    ] == [
        (row.event.event_id, row.axis_scores, row.composite_score, row.selected)
        for row in altered_scored
    ]

    rows = build_point_evaluations_v1a(loaded, scored)
    overall_h12 = next(
        row
        for row in rows
        if row["split"] == "development"
        and row["horizon_bars"] == 12
        and row["dimension"] == "overall"
    )
    assert len(rows) == len(V1A_SPLITS) * len(V1A_HORIZONS_BARS) * 10
    assert overall_h12["baseline_includes_feature_not_ready"] is True
    assert overall_h12["complete_case_events"] == len(V1A_ASSETS)
    assert overall_h12["feature_not_ready_events"] == 1
    assert overall_h12["complete_case_coverage"] == pytest.approx(7 / 8)
    assert overall_h12["retention"] == pytest.approx(
        cast(int, overall_h12["selected_events"]) / 8
    )
    baseline = overall_h12["baseline"]
    assert isinstance(baseline, dict)
    assert baseline["population_events"] == 8
    assert baseline["evaluable_events"] == 8
    assert baseline["sum_net_return_micros"] == sum(40 + i for i in range(7)) - 100


def _passing_gate_evidence() -> ValidationGateEvidenceV1A:
    return ValidationGateEvidenceV1A(
        selected_sum_micros=301,
        selected_evaluable=300,
        selected_mean_lower_micros=0.1,
        baseline_sum_micros=0,
        baseline_evaluable=1000,
        uplift_mean_lower_micros=0.1,
        selected_gross_profit_micros=401,
        selected_gross_loss_abs_micros=100,
        selected_median_micros=0.5,
        long_selected_sum_micros=150,
        long_selected_evaluable=150,
        short_selected_sum_micros=151,
        short_selected_evaluable=150,
        selected_events=300,
        intended_population_events=1500,
        complete_case_events=1485,
        positive_asset_uplifts=6,
        asset_count=7,
        selected_mean_valid_replicates=10_000,
        uplift_mean_valid_replicates=10_000,
        bootstrap_samples=10_000,
    )


@pytest.mark.parametrize(
    ("changes", "failed_criterion"),
    [
        (
            {
                "selected_sum_micros": 0,
                "selected_gross_profit_micros": 100,
                "long_selected_sum_micros": 0,
                "short_selected_sum_micros": 0,
            },
            "selected_mean_net_return_strictly_positive",
        ),
        (
            {"selected_mean_lower_micros": 0.0},
            "selected_mean_one_sided_basic_95_lower_strictly_positive",
        ),
        (
            {"baseline_sum_micros": 301, "baseline_evaluable": 300},
            "selected_minus_baseline_mean_strictly_positive",
        ),
        (
            {"uplift_mean_lower_micros": 0.0},
            "selected_minus_baseline_one_sided_basic_95_lower_strictly_positive",
        ),
        (
            {
                "selected_sum_micros": 0,
                "selected_gross_profit_micros": 100,
                "long_selected_sum_micros": 0,
                "short_selected_sum_micros": 0,
            },
            "selected_profit_factor_strictly_greater_than_one",
        ),
        (
            {"selected_median_micros": 0.0},
            "selected_median_net_return_strictly_positive",
        ),
        (
            {"long_selected_sum_micros": 0, "short_selected_sum_micros": 301},
            "long_selected_mean_strictly_positive",
        ),
        (
            {"long_selected_sum_micros": 301, "short_selected_sum_micros": 0},
            "short_selected_mean_strictly_positive",
        ),
        (
            {"intended_population_events": 1501, "complete_case_events": 1486},
            "selected_retention_at_least_20_percent",
        ),
        (
            {"selected_evaluable": 299, "long_selected_evaluable": 149},
            "selected_evaluable_at_least_300",
        ),
        (
            {"positive_asset_uplifts": 5},
            "positive_asset_uplift_at_least_6_of_7",
        ),
        (
            {"complete_case_events": 1484},
            "complete_case_coverage_at_least_99_percent",
        ),
        (
            {"selected_mean_valid_replicates": 9_999},
            "confirmatory_mean_bootstrap_all_10000_replicates_valid",
        ),
    ],
)
def test_v1a_validation_gate_strict_and_inclusive_boundaries(
    changes: dict[str, object],
    failed_criterion: str,
) -> None:
    evidence = _passing_gate_evidence()
    changed = replace(evidence, **changes)

    passing = evaluate_validation_gate_v1a(evidence)
    failed = evaluate_validation_gate_v1a(changed)

    assert passing["overall_pass"] is True
    assert failed["overall_pass"] is False
    assert isinstance(failed["criteria"], dict)
    assert failed["criteria"][failed_criterion] is False


@pytest.mark.parametrize(
    "changes",
    [
        {"selected_events": 299},
        {"baseline_evaluable": 1501},
        {"baseline_evaluable": 299},
        {"long_selected_evaluable": 149},
        {"long_selected_sum_micros": 149},
        {"selected_gross_profit_micros": 400},
        {"selected_mean_valid_replicates": 10_001},
    ],
)
def test_v1a_validation_gate_rejects_impossible_count_and_sum_states(
    changes: dict[str, object],
) -> None:
    with pytest.raises(IndicatorV1AContractError):
        evaluate_validation_gate_v1a(replace(_passing_gate_evidence(), **changes))


def test_score_quartile_boundaries_and_empty_bins_are_descriptive_only() -> None:
    assert score_quartile_v1a(24.999, q25=25, q50=50, q75=75) == "Q1"
    assert score_quartile_v1a(25, q25=25, q50=50, q75=75) == "Q2"
    assert score_quartile_v1a(50, q25=25, q50=50, q75=75) == "Q3"
    assert score_quartile_v1a(75, q25=25, q50=50, q75=75) == "Q4"

    loaded = _loaded_for_analysis()
    model, scored = fit_and_score_indicator_v1a(loaded)
    equal_scored = tuple(
        replace(row, composite_score=50.0, selected=True) for row in scored
    )
    gradient = build_descriptive_score_gradient_v1a(
        replace(model, top_quartile_cutoff=50.0),
        equal_scored,
    )

    assert gradient["development_type7_cutoffs"] == {
        "q25": 50.0,
        "q50": 50.0,
        "q75": 50.0,
    }
    assert gradient["score_is_probability"] is False
    assert gradient["included_in_confirmatory_gate"] is False
    assert gradient["inferential_bootstrap_applied"] is False
    rows = gradient["rows"]
    assert isinstance(rows, list)
    assert len(rows) == len(V1A_SPLITS) * len(V1A_HORIZONS_BARS) * 4
    assert all(
        row["empty_bin"] is True
        for row in rows
        if row["quartile"] in {"Q1", "Q2", "Q3"}
    )
    assert all(row["label"] == "DESCRIPTIVE_ONLY" for row in rows)


def test_shared_full_calendar_bootstrap_is_constant_and_order_invariant() -> None:
    split = "validation"
    schedule = build_shared_bootstrap_schedule_v1a(split)
    rebuilt = build_shared_bootstrap_schedule_v1a(split)
    start_ms, end_ms = V1A_SPLIT_RANGES_MS[split]
    rows = tuple(
        AnalyzableEventV1A(
            event_id=f"day-{day}",
            asset=V1A_ASSETS[day % len(V1A_ASSETS)],
            market="spot",
            direction="long",
            decision_time_ms=start_ms + day * 86_400_000 + 300_000,
            split=split,
            features=(1.0,) * len(FEATURE_COLUMNS),
            outcomes=_outcomes(10, 10, 10, 10, 10),
        )
        for day in range((end_ms - start_ms) // 86_400_000)
    )

    first = bootstrap_evaluation_cell_v1a(
        rows,
        rows,
        horizon_bars=12,
        schedule=schedule,
    )
    reversed_result = bootstrap_evaluation_cell_v1a(
        tuple(reversed(rows)),
        tuple(reversed(rows)),
        horizon_bars=12,
        schedule=schedule,
    )
    sparse = bootstrap_evaluation_cell_v1a(
        (rows[100],),
        (rows[100],),
        horizon_bars=12,
        schedule=schedule,
    )

    assert schedule.schedule_sha256 == rebuilt.schedule_sha256
    assert schedule.calendar_start_ms == start_ms
    assert schedule.calendar_end_ms == end_ms
    assert schedule.calendar_days == len(rows)
    assert schedule.samples == 10_000
    assert first == reversed_result
    assert first["schedule_sha256"] == schedule.schedule_sha256
    endpoints = first["endpoints"]
    assert isinstance(endpoints, dict)
    selected_mean = endpoints["selected_mean_net_return_micros"]
    uplift = endpoints["selected_minus_baseline_mean_net_return_micros"]
    assert selected_mean["two_sided_percentile_95_interval"] == [10.0, 10.0]
    assert selected_mean["one_sided_basic_95_lower"] == 10.0
    assert selected_mean["invalid_replicates"] == 0
    assert uplift["two_sided_percentile_95_interval"] == [0.0, 0.0]
    assert uplift["one_sided_basic_95_lower"] == 0.0
    sparse_endpoints = sparse["endpoints"]
    assert isinstance(sparse_endpoints, dict)
    assert (
        sparse_endpoints["selected_mean_net_return_micros"]["invalid_replicates"]
        > 0
    )
    with pytest.raises(IndicatorV1AContractError, match="exactly 10000"):
        build_shared_bootstrap_schedule_v1a(split, samples=9_999)


def test_concentrated_confirmatory_population_cannot_condition_away_invalid_draws() -> None:
    split = "validation"
    start_ms = V1A_SPLIT_RANGES_MS[split][0]
    rows = tuple(
        AnalyzableEventV1A(
            event_id=f"cluster-{index}",
            asset=V1A_ASSETS[index % len(V1A_ASSETS)],
            market="spot",
            direction="long" if index % 2 == 0 else "short",
            decision_time_ms=start_ms + 300_000,
            split=split,
            features=(1.0,) * len(FEATURE_COLUMNS),
            outcomes=_outcomes(10, 10, 10, 10, 10),
        )
        for index in range(300)
    )
    result = bootstrap_evaluation_cell_v1a(
        rows,
        rows,
        horizon_bars=12,
        schedule=build_shared_bootstrap_schedule_v1a(split),
    )
    endpoints = result["endpoints"]
    assert isinstance(endpoints, dict)
    selected_valid = endpoints["selected_mean_net_return_micros"]["valid_replicates"]
    uplift_valid = endpoints[
        "selected_minus_baseline_mean_net_return_micros"
    ]["valid_replicates"]
    assert 0 < selected_valid < V1A_BOOTSTRAP_SAMPLES
    decision = evaluate_validation_gate_v1a(
        replace(
            _passing_gate_evidence(),
            selected_mean_valid_replicates=selected_valid,
            uplift_mean_valid_replicates=uplift_valid,
        )
    )
    assert decision["overall_pass"] is False
    criteria = decision["criteria"]
    assert isinstance(criteria, dict)
    assert (
        criteria["confirmatory_mean_bootstrap_all_10000_replicates_valid"]
        is False
    )


def test_v1a_artifacts_are_deterministic_and_never_promote(
    v1a_fixture: tuple[Path, Path, dict[str, Any]],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    workspace, freeze_path, freeze = v1a_fixture
    replay_dirs = [
        _write_replay(freeze_path.parent, split, freeze) for split in V1A_SPLITS
    ]

    def fake_bootstrap(
        loaded: LoadedIndicatorV1A,
        scored: tuple[v1a_module.ScoredEventV1A, ...],
    ) -> tuple[list[dict[str, object]], tuple[v1a_module.SharedBootstrapScheduleV1A, ...]]:
        rows = build_point_evaluations_v1a(loaded, scored)
        schedules = tuple(
            build_shared_bootstrap_schedule_v1a(split) for split in V1A_SPLITS
        )
        by_split = {value.split: value for value in schedules}
        for row in rows:
            row["bootstrap"] = {
                "samples": 10_000,
                "schedule_sha256": by_split[cast(str, row["split"])].schedule_sha256,
                "endpoints": {
                    "selected_mean_net_return_micros": {
                        "one_sided_basic_95_lower": None,
                        "valid_replicates": 10_000,
                    },
                    "selected_minus_baseline_mean_net_return_micros": {
                        "one_sided_basic_95_lower": None,
                        "valid_replicates": 10_000,
                    },
                },
            }
        return rows, schedules

    monkeypatch.setattr(v1a_module, "build_bootstrapped_evaluations_v1a", fake_bootstrap)
    first_dir = freeze_path.parent / "analysis-one"
    second_dir = freeze_path.parent / "analysis-two"
    first = run_indicator_v1a_analysis(
        freeze_manifest_path=freeze_path,
        replay_dirs=replay_dirs,
        output_dir=first_dir,
        workspace_root=workspace,
    )
    second = run_indicator_v1a_analysis(
        freeze_manifest_path=freeze_path,
        replay_dirs=tuple(reversed(replay_dirs)),
        output_dir=second_dir,
        workspace_root=workspace,
    )

    assert first == second
    assert (first_dir / "fitted_score.json").read_bytes() == (
        second_dir / "fitted_score.json"
    ).read_bytes()
    assert (first_dir / "results.json").read_bytes() == (
        second_dir / "results.json"
    ).read_bytes()
    assert (first_dir / "report_ko.md").read_bytes() == (
        second_dir / "report_ko.md"
    ).read_bytes()
    for output_dir in (first_dir, second_dir):
        assert {path.name for path in output_dir.iterdir()} == {
            "fitted_score.json",
            "results.json",
            "report_ko.md",
            "analysis_manifest.json",
        }
        manifest = json.loads(
            (output_dir / "analysis_manifest.json").read_text(encoding="utf-8")
        )
        assert manifest["schema_version"] == 1
        assert manifest["protocol"] == v1a_module._ANALYSIS_PROTOCOL
        assert manifest["freeze_manifest_sha256"] == sha256_bytes(
            freeze_path.read_bytes()
        )
        assert manifest["input_authority_sha256"] == v1a_module._semantic_sha256(
            first["input_authority"]
        )
        assert manifest["historical_only"] is True
        assert manifest["external_anchor"] is False
        assert manifest["deployment_approved"] is False
        assert manifest["probability_calibrated"] is False
        assert set(manifest["outputs"]) == {
            "fitted_score.json",
            "results.json",
            "report_ko.md",
        }
        assert "analysis_manifest.json" not in manifest["outputs"]
        for name, expected_sha256 in manifest["outputs"].items():
            assert sha256_bytes((output_dir / name).read_bytes()) == expected_sha256
        for path in output_dir.iterdir():
            raw = path.read_bytes()
            raw.decode("utf-8")
            assert b"\r" not in raw
    report = (first_dir / "report_ko.md").read_text(encoding="utf-8")
    assert "역사적 비용 후 분석" in report
    assert "동결 게이트" in report
    assert "해석 제한" in report
    assert "\ufffd" not in report
    assert not any(marker in report for marker in ("?꾩", "吏", "媛", "瑜"))
    fitted = json.loads((first_dir / "fitted_score.json").read_text(encoding="utf-8"))
    assert fitted["missing_value_policy"] == (
        "strict eight-feature complete-case; any missing feature is "
        "FEATURE_NOT_READY no-call"
    )
    gradient = first["descriptive_score_gradient"]
    assert isinstance(gradient, dict)
    assert gradient["score_is_probability"] is False
    assert gradient["included_in_confirmatory_gate"] is False
    outcome_integrity = first["outcome_integrity"]
    assert isinstance(outcome_integrity, dict)
    assert outcome_integrity["raw_candles_reparsed"] is False
    assert outcome_integrity["raw_funding_reparsed"] is False
    assert outcome_integrity["independent_outcome_recomputation_claimed"] is False
    assert outcome_integrity["all_recommendations_joined_before_population_filtering"]
    status = first["status"]
    assert isinstance(status, dict)
    assert status["independently_validated"] is False
    assert status["probability_calibrated"] is False
    assert status["deployment_approved"] is False
    assert status["production_order_execution"] is False


def test_v1a_analysis_rejects_even_empty_existing_output_target(
    tmp_path: Path,
) -> None:
    target = tmp_path / "analysis"
    target.mkdir()

    with pytest.raises(IndicatorV1AContractError, match="must not already exist"):
        run_indicator_v1a_analysis(
            freeze_manifest_path=tmp_path / "missing-freeze.json",
            replay_dirs=(),
            output_dir=target,
            workspace_root=v1a_module._runtime_workspace_root(),
        )

    assert target.is_dir()
    assert not tuple(target.iterdir())


def test_atomic_analysis_partial_write_failure_removes_owned_temporary(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    target = v1a_module._resolved_fresh_analysis_target_v1a(
        tmp_path / "analysis"
    )
    original = v1a_module._write_fsynced_bytes_v1a
    calls = 0

    def fail_second_write(path: Path, payload: bytes) -> None:
        nonlocal calls
        calls += 1
        if calls == 2:
            raise OSError("injected partial-write failure")
        original(path, payload)

    monkeypatch.setattr(
        v1a_module,
        "_write_fsynced_bytes_v1a",
        fail_second_write,
    )
    with pytest.raises(IndicatorV1AContractError, match="publication failed"):
        v1a_module._publish_analysis_artifacts_v1a(
            target=target,
            payloads={
                "fitted_score.json": b"{}\n",
                "results.json": b"{}\n",
                "report_ko.md": b"# report\n",
            },
            freeze_manifest_sha256="0" * 64,
            input_authority={},
        )

    assert not target.exists()
    assert not tuple(tmp_path.glob(".analysis.tmp-*"))


def test_atomic_analysis_rename_failure_removes_temp_without_target(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    target = v1a_module._resolved_fresh_analysis_target_v1a(
        tmp_path / "analysis"
    )

    def fail_rename(source: Path, destination: Path) -> None:
        raise OSError(f"injected rename failure: {source} -> {destination}")

    monkeypatch.setattr(v1a_module.os, "rename", fail_rename)
    with pytest.raises(IndicatorV1AContractError, match="publication failed"):
        v1a_module._publish_analysis_artifacts_v1a(
            target=target,
            payloads={
                "fitted_score.json": b"{}\n",
                "results.json": b"{}\n",
                "report_ko.md": b"# report\n",
            },
            freeze_manifest_sha256="0" * 64,
            input_authority={},
        )

    assert not target.exists()
    assert not tuple(tmp_path.glob(".analysis.tmp-*"))


def test_v1a_cli_requires_exactly_three_replay_directories(tmp_path: Path) -> None:
    with pytest.raises(SystemExit) as error:
        v1a_module.main(
            [
                "--freeze-manifest",
                str(tmp_path / "freeze.json"),
                "--replay-dir",
                str(tmp_path / "one"),
                "--replay-dir",
                str(tmp_path / "two"),
                "--output-dir",
                str(tmp_path / "analysis"),
            ]
        )
    assert error.value.code == 2
