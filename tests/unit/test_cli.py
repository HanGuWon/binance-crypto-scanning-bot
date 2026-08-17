import json
import sys
from pathlib import Path

import pytest

from signalbot.cli import _evaluate_outcomes, _parser, main

FIXTURE = Path(__file__).resolve().parents[1] / "fixtures/outcomes/sample.json"


def test_evaluate_outcomes_cli_helper_outputs_machine_readable_metrics(capsys) -> None:
    _evaluate_outcomes(FIXTURE, [900])
    output = json.loads(capsys.readouterr().out)
    assert output["event_id"] == "event-1"
    assert output["outcomes"][0]["horizon_seconds"] == 900
    assert output["outcomes"][0]["mfe"] == 0.1
    assert output["outcomes"][0]["mae"] == -0.1


def test_r3_analysis_cli_requires_run_spec_and_output_without_live_config() -> None:
    args = _parser().parse_args(
        [
            "backtest-r3-analyze",
            "--run-dir",
            "run",
            "--spec",
            "r3.yaml",
            "--output-dir",
            "analysis",
        ]
    )

    assert args.command == "backtest-r3-analyze"
    assert args.run_dir == "run"
    assert args.spec == "r3.yaml"
    assert args.output_dir == "analysis"
    assert not hasattr(args, "config")


def test_r3_analysis_cli_prints_frozen_status_axes(monkeypatch, capsys) -> None:
    observed: dict[str, object] = {}

    def fake_analyze(
        run_dir: str,
        spec_path: str,
        *,
        workspace_root: Path,
        output_dir: str,
    ) -> dict[str, object]:
        observed.update(
            run_dir=run_dir,
            spec_path=spec_path,
            workspace_root=workspace_root,
            output_dir=output_dir,
        )
        return {
            "status_axes": {
                "data_integrity": "PASS",
                "kline_proxy_efficacy": "EXPLORATORY_FAIL",
                "execution_validity": "INCONCLUSIVE_NO_HISTORICAL_BBO",
            }
        }

    monkeypatch.setattr("signalbot.cli.analyze_r3_run", fake_analyze)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "signalbot",
            "backtest-r3-analyze",
            "--run-dir",
            "run",
            "--spec",
            "r3.yaml",
            "--output-dir",
            "analysis",
        ],
    )

    main()

    output = json.loads(capsys.readouterr().out)
    assert output == {
        "data_integrity": "PASS",
        "status": "EXPLORATORY_FAIL",
        "execution_validity": "INCONCLUSIVE_NO_HISTORICAL_BBO",
        "output_dir": "analysis",
    }
    assert observed["run_dir"] == "run"
    assert observed["spec_path"] == "r3.yaml"
    assert observed["output_dir"] == "analysis"
    assert isinstance(observed["workspace_root"], Path)


def test_r3_analysis_cli_exits_nonzero_when_integrity_fails(
    monkeypatch, capsys
) -> None:
    monkeypatch.setattr(
        "signalbot.cli.analyze_r3_run",
        lambda *args, **kwargs: {
            "status_axes": {
                "data_integrity": "FAIL",
                "kline_proxy_efficacy": "INCONCLUSIVE_LOW_INFORMATION",
                "execution_validity": "INCONCLUSIVE_NO_HISTORICAL_BBO",
            }
        },
    )
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "signalbot",
            "backtest-r3-analyze",
            "--run-dir",
            "run",
            "--spec",
            "r3.yaml",
            "--output-dir",
            "analysis",
        ],
    )

    with pytest.raises(SystemExit) as exc_info:
        main()

    assert exc_info.value.code == 2
    output = json.loads(capsys.readouterr().out)
    assert output["data_integrity"] == "FAIL"
