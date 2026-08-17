from conftest import ROOT
from signalbot.backtest.analysis import build_results, render_report
from signalbot.backtest.config import load_backtest_spec


def test_results_and_report_labels_derive_from_current_spec() -> None:
    base = load_backtest_spec(ROOT / "config" / "backtest.research.yaml")
    spec = base.model_copy(
        update={
            "protocol_version": "custom-5m-protocol",
            "rule_version": "custom-rules",
            "interval": "5m",
            "bootstrap": base.bootstrap.model_copy(update={"block_days": 3}),
            "costs": base.costs.model_copy(update={"notional_usdt": 250.0}),
        }
    )

    results = build_results([], spec)
    report = render_report(results, spec, spec_label="config/custom-5m.yaml")

    assert results["interval"] == "5m"
    assert results["data_start"] == spec.data_start.isoformat()
    assert results["evaluation_start"] == spec.evaluation_start.isoformat()
    assert results["evaluation_end"] == spec.evaluation_end.isoformat()
    assert "protocol_frozen_date" not in results
    assert "Interval: `5m`" in report
    assert "Binance 5m klines" in report
    assert "3-day fixed calendar-block clusters" in report
    assert "250-USDT trade" in report
    assert "config/custom-5m.yaml" in report
    assert "Binance 1h klines" not in report
    assert "config/backtest.research.yaml" not in report
    assert "2026-07-14" not in report
    assert "artifact_role" not in results
    assert "R3 RAW REPLAY INPUT" not in report
    assert "## Headline: upward buys versus downward sells" in report
    assert "verification.json" not in report
    assert "run_manifest.json" in report
    assert "opportunities.csv" in report


def test_r3_raw_replay_is_not_presented_as_final_r3_analysis() -> None:
    base = load_backtest_spec(ROOT / "config" / "backtest.research.yaml")
    spec = base.model_copy(
        update={
            "protocol_version": "r3_exposed_kline_proxy_diagnostic_v1",
            "rule_version": "r3-test-rules",
            "interval": "5m",
        }
    )

    results = build_results([], spec)
    report = render_report(results, spec, spec_label="config/r3-test.yaml")

    assert results["artifact_role"] == "RAW_REPLAY_INPUT_NOT_FINAL_R3_ANALYSIS"
    contract = results["r3_raw_replay_contract"]
    assert contract["sequential_t72_ledger"] == {
        "independent_episodes": False,
        "analysis_role": "SECONDARY_NON_PRIMARY",
    }
    assert contract["legacy_bootstrap"] == {
        "is_frozen_r3_shared_utc_day_mbb": False,
        "analysis_role": "DESCRIPTIVE_NOT_FINAL_R3_INFERENCE",
    }
    assert contract["r2_c0_t72_status"] == "PROTOCOL_MISMATCH"
    assert contract["final_r3_analysis"] == {
        "source": "SEPARATE_OPPORTUNITY_BASED_R3_ANALYZER",
        "provides_primary_efficacy": True,
        "provides_status_axes": True,
    }

    assert "R3 RAW REPLAY INPUT — NOT FINAL R3 ANALYSIS" in report
    assert "Secondary sequential T72 ledger (non-independent; non-primary)" in report
    assert "legacy CI/bootstrap" in report
    assert "not the frozen R3 shared" in report
    assert "R2 C0 T72 result remains `PROTOCOL_MISMATCH`" in report
    assert "separate opportunity-based R3 analyzer" in report
    assert "## Headline: upward buys versus downward sells" not in report
    assert "verification.json" not in report
