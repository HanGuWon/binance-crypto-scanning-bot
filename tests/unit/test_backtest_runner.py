import hashlib
from pathlib import Path

from conftest import ROOT
from signalbot.backtest.config import load_backtest_spec
from signalbot.backtest.runner import _build_run_manifest
from signalbot.config import Settings


def _write_manifest_inputs(root: Path) -> tuple[Path, Path, Path]:
    (root / "src" / "signalbot").mkdir(parents=True)
    (root / "src" / "signalbot" / "module.py").write_text(
        "VALUE = 1\n", encoding="utf-8"
    )
    (root / "config").mkdir()
    spec_path = root / "config" / "research.yaml"
    config_path = root / "config" / "settings.yaml"
    spec_path.write_text("protocol_version: test\n", encoding="utf-8")
    config_path.write_text("rule_version: effective-v1\n", encoding="utf-8")
    output_root = root / "artifacts"
    output_root.mkdir()
    for name in ("trades.csv", "results.json", "report.md"):
        (output_root / name).write_text(f"{name}\n", encoding="utf-8")
    return spec_path, config_path, output_root


def test_run_manifest_hashes_effective_settings_and_config_input(
    tmp_path: Path,
) -> None:
    workspace = tmp_path / "workspace"
    spec_path, config_path, output_root = _write_manifest_inputs(workspace)
    spec = load_backtest_spec(ROOT / "config" / "backtest.research.yaml")
    settings = Settings.model_validate({"rule_version": "effective-v1"})

    manifest = _build_run_manifest(
        settings=settings,
        spec=spec,
        workspace=workspace,
        spec_path=spec_path,
        config_path=config_path,
        input_hashes={"b.csv.gz": "b", "a.csv.gz": "a"},
        output_root=output_root,
    )
    changed = _build_run_manifest(
        settings=settings.model_copy(update={"rule_version": "effective-v2"}),
        spec=spec,
        workspace=workspace,
        spec_path=spec_path,
        config_path=config_path,
        input_hashes={},
        output_root=output_root,
    )

    assert manifest["config_input_sha256"] == hashlib.sha256(
        config_path.read_bytes()
    ).hexdigest()
    assert manifest["config_input_path"] == "config/settings.yaml"
    assert manifest["spec_input_path"] == "config/research.yaml"
    assert manifest["backtest_contract"] == {
        "candidate_policy": None,
        "confirmation_mode": None,
        "interval": "1h",
        "max_holding_bars": 24,
        "opportunity_panel_horizon_bars": 72,
        "outcome_edge_margin_bps": 0.0,
        "prediction_horizons_bars": [3, 6, 12],
        "prediction_entry": "next_contiguous_1h_open",
        "prediction_exit": "decision_index_plus_h_close",
        "outcome_labels": [
            "KLINE_PROXY_LONG",
            "KLINE_PROXY_FLAT",
            "KLINE_PROXY_SHORT",
        ],
    }
    assert manifest["inputs"] == {"a.csv.gz": "a", "b.csv.gz": "b"}
    assert manifest["effective_settings_sha256"] != changed[
        "effective_settings_sha256"
    ]


def test_five_minute_r2_and_r3_manifest_entry_contract_remains_exact(
    tmp_path: Path,
) -> None:
    workspace = tmp_path / "workspace"
    spec_path, config_path, output_root = _write_manifest_inputs(workspace)
    base = load_backtest_spec(ROOT / "config" / "backtest.research.yaml")
    shared_updates = {
        "interval": "5m",
        "confirmation_mode": "explicit_trigger",
        "exits": base.exits.model_copy(update={"max_holding_bars": 72}),
    }
    r2_spec = base.model_copy(
        update={
            **shared_updates,
            "protocol_version": "r2_retrospective_screen_v1_c0_corrected",
            "candidate_policy": "c0_frozen",
            "opportunity_panel_horizon_bars": 72,
        }
    )
    r3_spec = base.model_copy(
        update={
            **shared_updates,
            "protocol_version": "r3_exposed_kline_proxy_diagnostic_v1",
            "candidate_policy": "c0_frozen",
            "opportunity_panel_horizon_bars": 12,
        }
    )

    for spec in (r2_spec, r3_spec):
        manifest = _build_run_manifest(
            settings=Settings(),
            spec=spec,
            workspace=workspace,
            spec_path=spec_path,
            config_path=config_path,
            input_hashes={},
            output_root=output_root,
        )
        contract = manifest["backtest_contract"]
        assert contract["prediction_entry"] == "next_contiguous_5m_open"
        assert contract["prediction_horizons_bars"] == [3, 6, 12]
        assert contract["prediction_exit"] == "decision_index_plus_h_close"


def test_run_manifest_marks_missing_config_hook_explicitly(tmp_path: Path) -> None:
    workspace = tmp_path / "workspace"
    spec_path, _, output_root = _write_manifest_inputs(workspace)
    spec = load_backtest_spec(ROOT / "config" / "backtest.research.yaml")

    manifest = _build_run_manifest(
        settings=Settings(),
        spec=spec,
        workspace=workspace,
        spec_path=spec_path,
        config_path=None,
        input_hashes={},
        output_root=output_root,
    )

    assert manifest["config_input_sha256"] is None
    assert manifest["config_input_path"] is None
