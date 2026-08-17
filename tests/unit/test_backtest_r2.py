import csv
import hashlib
import json
from dataclasses import replace
from pathlib import Path

import pytest

from signalbot.backtest.r2 import (
    R2Opportunity,
    R2TechnicalTrade,
    analyze_r2_retrospective,
    circular_moving_block_indices,
    holm_step_down,
    one_sided_basic_lower_bound,
    pro_one_sided_p_value,
    read_r2_opportunities,
    read_r2_technical_trades,
    validate_r2_analysis_parameters,
    validate_r2_run_provenance,
)

DAY_MS = 86_400_000
ASSETS = ("BTC", "ETH", "BNB", "SOL", "XRP", "DOGE", "SUI", "WIF")
SIDES = (("spot", "long"), ("futures", "short"))
PLAN_SHA256 = "42fc382837747150f02cd14dacdb6b3573c77c1e017c9eb997529659c616d2de"
SPEC_SHA256 = {
    "c0": "2dce99a243c4f94c446cf48a0edcb103093db9cecf0e6da261c35b45fa235c7f",
    "h1": "e1585d449098115074e3cb735763a84b3b9982f8c5943ecf3dfe8db5b354732b",
}
FROZEN_INPUTS = {
    f"{market}/{asset}__{asset}USDT__5m.csv.gz": "1" * 64
    for market in ("spot", "futures", "funding")
    for asset in ASSETS
}


def _panels(
    *, days: int = 2
) -> tuple[
    tuple[R2Opportunity, ...],
    tuple[R2Opportunity, ...],
    tuple[R2TechnicalTrade, ...],
    tuple[R2TechnicalTrade, ...],
]:
    c0: list[R2Opportunity] = []
    h1: list[R2Opportunity] = []
    c0_trades: list[R2TechnicalTrade] = []
    h1_trades: list[R2TechnicalTrade] = []
    for day in range(days):
        for market, direction in SIDES:
            for asset_index, asset in enumerate(ASSETS):
                for accepted, net_return, suffix in (
                    (True, 0.010, "accepted"),
                    (False, -0.005, "rejected"),
                ):
                    opportunity_id = (
                        f"{day}-{market}-{direction}-{asset}-{suffix}"
                    )
                    decision_time_ms = (
                        (100 + day) * DAY_MS
                        + asset_index * 60_000
                        + (0 if accepted else 30_000)
                    )
                    gross_return = 0.012 if accepted else -0.003
                    base = R2Opportunity(
                        opportunity_id=opportunity_id,
                        asset=asset,
                        market=market,
                        direction=direction,
                        decision_time_ms=decision_time_ms,
                        h1_accepted=False,
                        episode_net_return_60m=net_return,
                        gross_return_60m=gross_return,
                        fee_return_60m=0.001,
                        slippage_return_60m=0.001,
                        funding_return_60m=0.0,
                        next_open_time_ms=decision_time_ms + 1,
                    )
                    c0.append(base)
                    h1.append(replace(base, h1_accepted=accepted))
                    technical = R2TechnicalTrade(
                        opportunity_id=opportunity_id,
                        asset=asset,
                        market=market,
                        direction=direction,
                        decision_time_ms=decision_time_ms,
                        entry_time_ms=decision_time_ms + 1,
                        exit_time_ms=decision_time_ms + 2,
                        technical_net_return=0.015,
                        gross_return=0.017,
                        fee_return=0.001,
                        slippage_return=0.001,
                        funding_return=0.0,
                        bars_held=1,
                    )
                    c0_trades.append(technical)
                    if accepted:
                        h1_trades.append(technical)
    return tuple(c0), tuple(h1), tuple(c0_trades), tuple(h1_trades)


def _analyze_pass_fixture(*, seed: int = 20_260_716):
    c0, h1, c0_trades, h1_trades = _panels()
    return analyze_r2_retrospective(
        c0,
        h1,
        c0_trades=c0_trades,
        h1_trades=h1_trades,
        bootstrap_samples=200,
        seed=seed,
        min_accepted=1,
        min_valid_days=1,
        min_positive_assets=6,
    )


def _side(result, market: str):
    return next(item for item in result["sides"] if item["market"] == market)


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _write_run_manifest(
    directory: Path,
    *,
    protocol: str,
    spec_hash: str,
    marker: str,
    contract_updates: dict[str, object] | None = None,
    contract_remove: str | None = None,
) -> None:
    directory.mkdir()
    for name in ("opportunities.csv", "trades.csv", "results.json", "report.md"):
        (directory / name).write_text(f"{marker}:{name}\n", encoding="utf-8")
    protocol_version, candidate_policy = (
        (
            "r2_retrospective_screen_v1_c0_corrected",
            "c0_frozen",
        )
        if protocol == "c0"
        else (
            "r2_retrospective_screen_v1_h1_strict_pit_htf",
            "strict_pit_htf_diagnostic",
        )
    )
    contract: dict[str, object] = {
        "candidate_policy": candidate_policy,
        "confirmation_mode": "explicit_trigger",
        "interval": "5m",
        "max_holding_bars": 72,
    }
    contract.update(contract_updates or {})
    if contract_remove is not None:
        contract.pop(contract_remove, None)
    manifest = {
        "protocol_version": protocol_version,
        "rule_version": protocol,
        "code_sha256": "c" * 64,
        "spec_sha256": spec_hash,
        "backtest_contract": contract,
        "config_input_sha256": "d" * 64,
        "effective_settings_sha256": "e" * 64,
        "experiment_plan_sha256": PLAN_SHA256,
        "inputs": FROZEN_INPUTS,
        "environment": {"uv_lock_sha256": "2" * 64},
        "outputs": {
            name: _sha256(directory / name)
            for name in ("opportunities.csv", "trades.csv", "results.json", "report.md")
        },
    }
    (directory / "run_manifest.json").write_text(
        json.dumps(manifest), encoding="utf-8"
    )


_RECOGNIZED_RUNNER_CONTRACT_METADATA: dict[str, object] = {
    "opportunity_panel_horizon_bars": 72,
    "outcome_edge_margin_bps": 0.0,
    "prediction_horizons_bars": [3, 6, 12],
    "prediction_entry": "next_contiguous_5m_open",
    "prediction_exit": "decision_index_plus_h_close",
    "outcome_labels": [
        "KLINE_PROXY_LONG",
        "KLINE_PROXY_FLAT",
        "KLINE_PROXY_SHORT",
    ],
}


def _write_r2_manifest_set(
    root: Path,
    *,
    contract_updates: dict[str, object] | None = None,
    contract_remove: str | None = None,
) -> tuple[Path, Path, Path, Path]:
    directories = (
        root / "c0-a",
        root / "c0-b",
        root / "h1-a",
        root / "h1-b",
    )
    for directory, protocol in zip(
        directories,
        ("c0", "c0", "h1", "h1"),
        strict=True,
    ):
        _write_run_manifest(
            directory,
            protocol=protocol,
            spec_hash=SPEC_SHA256[protocol],
            marker=protocol,
            contract_updates=contract_updates,
            contract_remove=contract_remove,
        )
    return directories


def test_r2_provenance_accepts_legacy_contract_manifest(tmp_path: Path) -> None:
    directories = _write_r2_manifest_set(tmp_path)

    result = validate_r2_run_provenance(*directories)

    assert result["valid"] is True


def test_r2_provenance_accepts_recognized_runner_contract_metadata(
    tmp_path: Path,
) -> None:
    directories = _write_r2_manifest_set(
        tmp_path,
        contract_updates=_RECOGNIZED_RUNNER_CONTRACT_METADATA,
    )

    result = validate_r2_run_provenance(*directories)

    assert result["valid"] is True


def test_r2_provenance_rejects_wrong_runner_contract_value(tmp_path: Path) -> None:
    metadata = {
        **_RECOGNIZED_RUNNER_CONTRACT_METADATA,
        "opportunity_panel_horizon_bars": 12,
    }
    directories = _write_r2_manifest_set(tmp_path, contract_updates=metadata)

    with pytest.raises(ValueError, match="metadata mismatch"):
        validate_r2_run_provenance(*directories)


@pytest.mark.parametrize("missing_key", ["interval", "outcome_labels"])
def test_r2_provenance_rejects_missing_contract_key(
    tmp_path: Path,
    missing_key: str,
) -> None:
    directories = _write_r2_manifest_set(
        tmp_path,
        contract_updates=_RECOGNIZED_RUNNER_CONTRACT_METADATA,
        contract_remove=missing_key,
    )

    with pytest.raises(ValueError, match=r"missing|incomplete"):
        validate_r2_run_provenance(*directories)


def test_r2_provenance_rejects_unknown_contract_key(tmp_path: Path) -> None:
    directories = _write_r2_manifest_set(
        tmp_path,
        contract_updates={
            **_RECOGNIZED_RUNNER_CONTRACT_METADATA,
            "unreviewed_runner_metadata": True,
        },
    )

    with pytest.raises(ValueError, match="unknown keys"):
        validate_r2_run_provenance(*directories)


def test_r2_provenance_requires_ab_identity_and_actual_output_hashes(tmp_path) -> None:
    c0_a = tmp_path / "c0-a"
    c0_b = tmp_path / "c0-b"
    h1_a = tmp_path / "h1-a"
    h1_b = tmp_path / "h1-b"
    _write_run_manifest(c0_a, protocol="c0", spec_hash=SPEC_SHA256["c0"], marker="c0")
    _write_run_manifest(c0_b, protocol="c0", spec_hash=SPEC_SHA256["c0"], marker="c0")
    _write_run_manifest(h1_a, protocol="h1", spec_hash=SPEC_SHA256["h1"], marker="h1")
    _write_run_manifest(h1_b, protocol="h1", spec_hash=SPEC_SHA256["h1"], marker="h1")

    result = validate_r2_run_provenance(c0_a, c0_b, h1_a, h1_b)
    assert result["valid"] is True
    assert result["ab_output_identity"] == {"c0": True, "h1": True}

    (h1_b / "trades.csv").write_text("tampered\n", encoding="utf-8")
    with pytest.raises(ValueError, match="hash mismatch"):
        validate_r2_run_provenance(c0_a, c0_b, h1_a, h1_b)


def test_r2_provenance_rejects_reused_or_swapped_role_directories(tmp_path) -> None:
    c0_a = tmp_path / "c0-a"
    c0_b = tmp_path / "c0-b"
    h1_a = tmp_path / "h1-a"
    h1_b = tmp_path / "h1-b"
    _write_run_manifest(c0_a, protocol="c0", spec_hash=SPEC_SHA256["c0"], marker="c0")
    _write_run_manifest(c0_b, protocol="c0", spec_hash=SPEC_SHA256["c0"], marker="c0")
    _write_run_manifest(h1_a, protocol="h1", spec_hash=SPEC_SHA256["h1"], marker="h1")
    _write_run_manifest(h1_b, protocol="h1", spec_hash=SPEC_SHA256["h1"], marker="h1")

    with pytest.raises(ValueError, match="four distinct paths"):
        validate_r2_run_provenance(c0_a, c0_a, h1_a, h1_b)
    with pytest.raises(ValueError, match="role/protocol mismatch"):
        validate_r2_run_provenance(h1_a, h1_b, c0_a, c0_b)


def test_frozen_r2_analysis_parameters_cannot_drift() -> None:
    validate_r2_analysis_parameters(50_000, 20_260_716)
    with pytest.raises(ValueError, match="50000 bootstrap samples"):
        validate_r2_analysis_parameters(49_999, 20_260_716)
    with pytest.raises(ValueError, match="seed 20260716"):
        validate_r2_analysis_parameters(50_000, 1)


def test_rejected_h1_opportunities_contribute_zero_not_their_return() -> None:
    result = _analyze_pass_fixture()
    spot = _side(result, "spot")

    assert spot["common_opportunities"] == 32
    assert spot["accepted_opportunities"] == 16
    assert spot["coverage"] == pytest.approx(0.5)
    assert spot["c0_policy_mean"] == pytest.approx(0.0025)
    assert spot["h1_policy_contribution"] == pytest.approx(0.005)
    assert spot["h1_conditional_mean"] == pytest.approx(0.010)
    assert spot["h1_policy_uplift_vs_c0"] == pytest.approx(0.0025)
    assert spot["h1_fixed_notional_pnl_usdt"] == pytest.approx(16.0)
    assert spot["c0_technical_exit"]["technical_mean"] == pytest.approx(0.015)
    assert spot["c0_technical_exit"]["technical_uplift_vs_f60"] == pytest.approx(
        0.0125
    )


def test_circular_blocks_wrap_and_truncate_to_calendar_length() -> None:
    assert circular_moving_block_indices(5, 3, (4, 3)) == (4, 0, 1, 3, 4)
    assert circular_moving_block_indices(3, 7, (2,)) == (2, 0, 1)
    with pytest.raises(ValueError, match="exactly"):
        circular_moving_block_indices(5, 3, (4,))


def test_seeded_analysis_is_deterministic_and_strict_json_ready() -> None:
    first = _analyze_pass_fixture(seed=17)
    second = _analyze_pass_fixture(seed=17)

    assert first == second
    encoded = json.dumps(first, sort_keys=True, allow_nan=False)
    assert "NaN" not in encoded and "Infinity" not in encoded
    assert len(first["entry_hypotheses"]) == 2
    assert len(first["exit_hypotheses"]) == 2


def test_small_panel_is_inconclusive_not_failed() -> None:
    c0, h1, c0_trades, h1_trades = _panels()
    result = analyze_r2_retrospective(
        c0,
        h1,
        c0_trades=c0_trades,
        h1_trades=h1_trades,
        bootstrap_samples=100,
    )

    assert result["status"] == "INCONCLUSIVE"
    assert all(
        item["status"] == "INCONCLUSIVE"
        for item in result["entry_hypotheses"] + result["exit_hypotheses"]
    )
    assert any("accepted opportunities" in reason for reason in result["status_reasons"])
    assert any("valid UTC days" in reason for reason in result["status_reasons"])


def test_holm_step_down_stops_after_first_non_rejection() -> None:
    result = holm_step_down({"a": 0.01, "b": 0.04, "c": 0.20})

    assert result["a"]["rank"] == 1
    assert result["a"]["local_alpha"] == pytest.approx(0.05 / 3)
    assert result["a"]["adjusted_p_value"] == pytest.approx(0.03)
    assert result["a"]["rejected"] is True
    assert result["b"]["rejected"] is False
    assert result["b"]["adjusted_p_value"] == pytest.approx(0.08)
    assert result["c"]["rejected"] is False


def test_pre_registered_p_value_and_basic_bound_formulas() -> None:
    bootstrap = (0.0, 0.1, 0.2, 0.3)

    assert pro_one_sided_p_value(0.1, bootstrap) == pytest.approx(0.6)
    assert pro_one_sided_p_value(0.0, bootstrap) == 1.0
    assert one_sided_basic_lower_bound(
        0.1, bootstrap, alpha=0.25
    ) == pytest.approx(-0.025)


def test_retrospective_pass_never_claims_full_r2_without_historical_bbo() -> None:
    result = _analyze_pass_fixture()

    assert result["status"] == "RETROSPECTIVE_SCREEN_PASS"
    assert result["full_r2_status"] == "INCONCLUSIVE_NO_HISTORICAL_BBO"
    assert result["historical_execution_boundary"] == {
        "historical_bbo_available": False,
        "status": "NOT_TESTABLE",
        "reason": (
            "historical opportunity data contain no "
            "decision-time BBO/depth/receipt-time"
        ),
    }
    assert all(
        item["holm"]["rejected"]
        for item in result["entry_hypotheses"] + result["exit_hypotheses"]
    )


def test_tampered_common_ids_are_invalid_without_partial_statistics() -> None:
    c0, h1, c0_trades, h1_trades = _panels()
    tampered = (replace(h1[0], opportunity_id="tampered-id"), *h1[1:])

    result = analyze_r2_retrospective(
        c0,
        tampered,
        c0_trades=c0_trades,
        h1_trades=h1_trades,
        bootstrap_samples=100,
    )

    assert result["status"] == "INVALID"
    assert result["integrity"]["valid"] is False
    assert result["sides"] == []
    assert "opportunity_id set differs" in result["status_reasons"][0]


def test_unknown_or_rejected_trade_mapping_is_invalid() -> None:
    c0, h1, c0_trades, h1_trades = _panels()
    unknown = replace(c0_trades[0], opportunity_id="not-in-panel")
    result = analyze_r2_retrospective(
        c0,
        h1,
        c0_trades=(unknown, *c0_trades[1:]),
        h1_trades=h1_trades,
        bootstrap_samples=100,
    )
    assert result["status"] == "INVALID"
    assert "unknown opportunity_id" in result["status_reasons"][0]

    rejected_id = next(item.opportunity_id for item in h1 if not item.h1_accepted)
    rejected_trade = next(
        item for item in c0_trades if item.opportunity_id == rejected_id
    )
    rejected_result = analyze_r2_retrospective(
        c0,
        h1,
        c0_trades=c0_trades,
        h1_trades=(*h1_trades, rejected_trade),
        bootstrap_samples=100,
    )
    assert rejected_result["status"] == "INVALID"
    assert "rejected opportunity" in rejected_result["status_reasons"][0]


def test_absent_technical_trade_panel_is_inconclusive_not_zero_filled() -> None:
    c0, h1, _, _ = _panels()
    result = analyze_r2_retrospective(
        c0,
        h1,
        bootstrap_samples=100,
        min_accepted=1,
        min_valid_days=1,
        min_positive_assets=1,
    )

    assert result["status"] == "INCONCLUSIVE"
    assert all(item["status"] == "INCONCLUSIVE" for item in result["exit_hypotheses"])
    assert all(
        _side(result, market)["c0_technical_exit"]["common_stop_openable_trades"]
        == 0
        for market, _ in SIDES
    )


def test_split_crossing_technical_trade_invalidates_analysis() -> None:
    c0, h1, c0_trades, h1_trades = _panels()
    crossing = replace(c0_trades[0], split_contained=False)

    result = analyze_r2_retrospective(
        c0,
        h1,
        c0_trades=(crossing, *c0_trades[1:]),
        h1_trades=h1_trades,
        bootstrap_samples=100,
    )

    assert result["status"] == "INVALID"
    assert "split-crossing" in result["status_reasons"][0]


def test_technical_trade_must_use_next_open_and_end_within_72_bars() -> None:
    c0, h1, c0_trades, h1_trades = _panels()
    late_entry = replace(
        c0_trades[0],
        entry_time_ms=c0_trades[0].entry_time_ms + 1,
        exit_time_ms=c0_trades[0].exit_time_ms + 1,
    )
    too_long = replace(c0_trades[0], bars_held=73)

    late_result = analyze_r2_retrospective(
        c0,
        h1,
        c0_trades=(late_entry, *c0_trades[1:]),
        h1_trades=h1_trades,
        bootstrap_samples=100,
    )
    long_result = analyze_r2_retrospective(
        c0,
        h1,
        c0_trades=(too_long, *c0_trades[1:]),
        h1_trades=h1_trades,
        bootstrap_samples=100,
    )

    assert late_result["status"] == "INVALID"
    assert "frozen next open" in late_result["status_reasons"][0]
    assert long_result["status"] == "INVALID"
    assert "between 0 and 72 bars" in long_result["status_reasons"][0]


def test_csv_readers_accept_engine_field_names_and_legacy_trade_tuple(tmp_path) -> None:
    opportunity_path = tmp_path / "opportunities.csv"
    with opportunity_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=(
                "opportunity_id",
                "asset",
                "market",
                "direction",
                "decision_time_ms",
                "htf_filter_accepted",
                "htf_filter_failures",
                "analysis_eligible_72",
                "execution_observed",
                "f60_gross_return",
                "f60_fee_return",
                "f60_slippage_return",
                "f60_funding_return",
                "f60_net_return",
            ),
        )
        writer.writeheader()
        writer.writerow(
            {
                "opportunity_id": "o1",
                "asset": "btc",
                "market": "SPOT",
                "direction": "LONG",
                "decision_time_ms": "100",
                "htf_filter_accepted": "true",
                "htf_filter_failures": "",
                "analysis_eligible_72": "true",
                "execution_observed": "false",
                "f60_gross_return": "0.012",
                "f60_fee_return": "0.001",
                "f60_slippage_return": "0.001",
                "f60_funding_return": "0",
                "f60_net_return": "0.010",
            }
        )
    trade_path = tmp_path / "trades.csv"
    with trade_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=(
                "asset",
                "market",
                "direction",
                "entry_signal_time_ms",
                "entry_time_ms",
                "exit_time_ms",
                "gross_return",
                "fee_return",
                "slippage_return",
                "funding_return",
                "net_return",
            ),
        )
        writer.writeheader()
        writer.writerow(
            {
                "asset": "btc",
                "market": "spot",
                "direction": "long",
                "entry_signal_time_ms": "100",
                "entry_time_ms": "101",
                "exit_time_ms": "102",
                "gross_return": "0.017",
                "fee_return": "0.001",
                "slippage_return": "0.001",
                "funding_return": "0",
                "net_return": "0.015",
            }
        )

    opportunities = read_r2_opportunities(opportunity_path)
    trades = read_r2_technical_trades(trade_path)

    assert opportunities == (
        R2Opportunity(
            "o1",
            "BTC",
            "spot",
            "long",
            100,
            True,
            0.010,
            True,
            True,
            0.012,
            0.001,
            0.001,
            0.0,
            False,
        ),
    )
    assert trades == (
        R2TechnicalTrade(
            "",
            "BTC",
            "spot",
            "long",
            100,
            101,
            102,
            0.015,
            True,
            0.017,
            0.001,
            0.001,
            0.0,
        ),
    )
