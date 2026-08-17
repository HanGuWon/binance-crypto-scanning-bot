import csv
from dataclasses import replace

import pytest

from signalbot.backtest.verdict import (
    VerdictOpportunity,
    VerdictTrade,
    evaluate_r1_verdict,
    read_verdict_opportunities,
    read_verdict_trades,
)

ASSETS = ("BTC", "ETH", "BNB", "SOL", "XRP", "DOGE", "SUI", "WIF")
SIDES = (("spot", "long"), ("futures", "short"))


def _opportunity_panels(
    *,
    count_per_side: int = 8,
    weights: dict[str, float] | None = None,
    unavailable: set[tuple[str, str, int]] | None = None,
) -> dict[str, list[VerdictOpportunity]]:
    weights = weights or {asset: 0.01 for asset in ASSETS}
    unavailable = unavailable or set()
    asset_counts = {
        asset: sum(ASSETS[index % len(ASSETS)] == asset for index in range(count_per_side))
        for asset in ASSETS
    }
    panels: dict[str, list[VerdictOpportunity]] = {
        "C0": [],
        "G2": [],
        "G4": [],
    }
    for market, direction in SIDES:
        for index in range(count_per_side):
            asset = ASSETS[index % len(ASSETS)]
            outcome = weights[asset] / asset_counts[asset]
            opportunity_id = f"{market}-{direction}-{index}"
            base = VerdictOpportunity(
                opportunity_id=opportunity_id,
                asset=asset,
                market=market,
                direction=direction,
                decision_time_ms=index + 1,
                eligible=False,
                analysis_eligible_12=True,
                volume_feature_available=True,
                forward_return_12=outcome,
            )
            panels["C0"].append(base)
            for variant in ("G2", "G4"):
                panels[variant].append(
                    replace(
                        base,
                        eligible=True,
                        volume_feature_available=(variant, market, index)
                        not in unavailable,
                    )
                )
    return panels


def _trade_runs() -> dict[str, list[VerdictTrade]]:
    runs: dict[str, list[VerdictTrade]] = {"C0": [], "G2": [], "G4": []}
    for variant in ("G2", "G4"):
        for market, direction in SIDES:
            runs[variant].extend(
                (
                    VerdictTrade(
                        f"{variant}-{market}-win",
                        "BTC",
                        market,
                        direction,
                        0.002,
                        0.20,
                    ),
                    VerdictTrade(
                        f"{variant}-{market}-loss",
                        "ETH",
                        market,
                        direction,
                        -0.0005,
                        -0.05,
                    ),
                )
            )
    return runs


def _comparison(lower: float = 0.001) -> dict[str, object]:
    return {
        "rows": [
            {
                "contrast": f"{variant}-C0",
                "market": market,
                "direction": direction,
                "simultaneous_one_sided_low": lower,
            }
            for variant in ("G2", "G4")
            for market, direction in SIDES
        ]
    }


def _hypothesis(result, variant: str, market: str):
    return next(
        item
        for item in result.hypotheses
        if item.variant == variant and item.market == market
    )


def _criterion(result, variant: str, market: str, name: str):
    hypothesis = _hypothesis(result, variant, market)
    return next(item for item in hypothesis.criteria if item.criterion == name)


def test_positive_verdict_passes_all_frozen_rules_deterministically() -> None:
    first = evaluate_r1_verdict(
        _opportunity_panels(),
        _trade_runs(),
        _comparison(),
        determinism_parity_passed=True,
    )
    second = evaluate_r1_verdict(
        _opportunity_panels(),
        _trade_runs(),
        _comparison(),
        determinism_parity_passed=True,
    )

    assert first == second
    assert first.overall_pass is True
    assert len(first.hypotheses) == 4
    assert all(item.overall_pass for item in first.hypotheses)
    assert _criterion(first, "G2", "spot", "profit_factor").value == pytest.approx(4.0)
    assert _criterion(first, "G2", "spot", "positive_asset_count").value == 8
    assert _criterion(
        first, "G2", "spot", "positive_contribution_concentration"
    ).value == pytest.approx(0.125)
    assert first.to_dict()["overall_pass"] is True


def test_availability_and_mean_return_boundaries_pass() -> None:
    panels = _opportunity_panels(
        count_per_side=20,
        unavailable={("G2", "spot", 0)},
    )
    trades = _trade_runs()
    trades["G2"] = [
        item
        for item in trades["G2"]
        if not (item.market == "spot" and item.direction == "long")
    ]
    trades["G2"].append(
        VerdictTrade("G2-spot-boundary", "BTC", "spot", "long", 0.0005, 0.05)
    )

    result = evaluate_r1_verdict(
        panels, trades, _comparison(), determinism_parity_passed=True
    )

    availability = _criterion(result, "G2", "spot", "feature_availability")
    mean_return = _criterion(result, "G2", "spot", "mean_net_return")
    profit_factor = _criterion(result, "G2", "spot", "profit_factor")
    assert availability.value == pytest.approx(0.95)
    assert availability.passed is True
    assert mean_return.value == pytest.approx(0.0005)
    assert mean_return.passed is True
    assert profit_factor.value is None
    assert profit_factor.passed is True
    assert "+infinity" in profit_factor.detail


def test_below_availability_and_zero_adjusted_lower_bound_fail() -> None:
    panels = _opportunity_panels(
        count_per_side=20,
        unavailable={("G2", "spot", 0), ("G2", "spot", 1)},
    )
    comparison = _comparison()
    rows = comparison["rows"]
    assert isinstance(rows, list)
    for row in rows:
        if (
            isinstance(row, dict)
            and row["contrast"] == "G2-C0"
            and row["market"] == "spot"
        ):
            row["simultaneous_one_sided_low"] = 0.0

    result = evaluate_r1_verdict(
        panels, _trade_runs(), comparison, determinism_parity_passed=True
    )

    availability = _criterion(result, "G2", "spot", "feature_availability")
    lower = _criterion(result, "G2", "spot", "simultaneous_one_sided_low")
    assert availability.value == pytest.approx(0.90)
    assert availability.passed is False
    assert lower.value == 0.0
    assert lower.passed is False
    assert _hypothesis(result, "G2", "spot").overall_pass is False


def test_zero_trades_fail_pnl_mean_and_profit_factor_without_json_infinity() -> None:
    result = evaluate_r1_verdict(
        _opportunity_panels(),
        {"C0": [], "G2": [], "G4": []},
        _comparison(),
        determinism_parity_passed=True,
    )

    pnl = _criterion(result, "G2", "spot", "total_net_pnl_usdt")
    mean_return = _criterion(result, "G2", "spot", "mean_net_return")
    profit_factor = _criterion(result, "G2", "spot", "profit_factor")
    assert pnl.value == 0.0 and pnl.passed is False
    assert mean_return.value is None and mean_return.passed is False
    assert profit_factor.value is None and profit_factor.passed is False
    assert result.overall_pass is False


def test_profit_factor_must_be_strictly_above_boundary() -> None:
    trades = _trade_runs()
    trades["G2"] = [
        item
        for item in trades["G2"]
        if not (item.market == "spot" and item.direction == "long")
    ]
    trades["G2"].extend(
        (
            VerdictTrade("pf-win", "BTC", "spot", "long", 0.00105, 0.105),
            VerdictTrade("pf-loss", "ETH", "spot", "long", -0.001, -0.10),
        )
    )

    result = evaluate_r1_verdict(
        _opportunity_panels(), trades, _comparison(), determinism_parity_passed=True
    )

    profit_factor = _criterion(result, "G2", "spot", "profit_factor")
    assert profit_factor.value == pytest.approx(1.05)
    assert profit_factor.passed is False


def test_concentration_boundary_passes_and_above_boundary_fails() -> None:
    boundary_weights = {asset: 0.65 / 7 for asset in ASSETS}
    boundary_weights["BTC"] = 0.35
    boundary = evaluate_r1_verdict(
        _opportunity_panels(weights=boundary_weights),
        _trade_runs(),
        _comparison(),
        determinism_parity_passed=True,
    )
    concentration = _criterion(
        boundary, "G2", "spot", "positive_contribution_concentration"
    )
    assert concentration.value == pytest.approx(0.35)
    assert concentration.passed is True

    concentrated_weights = {asset: 0.64 / 7 for asset in ASSETS}
    concentrated_weights["BTC"] = 0.36
    concentrated = evaluate_r1_verdict(
        _opportunity_panels(weights=concentrated_weights),
        _trade_runs(),
        _comparison(),
        determinism_parity_passed=True,
    )
    concentrated_result = _criterion(
        concentrated, "G2", "spot", "positive_contribution_concentration"
    )
    assert concentrated_result.value == pytest.approx(0.36)
    assert concentrated_result.passed is False


def test_fewer_than_six_positive_assets_and_failed_parity_reject() -> None:
    weights = {asset: (-0.01 if index >= 5 else 0.01) for index, asset in enumerate(ASSETS)}
    result = evaluate_r1_verdict(
        _opportunity_panels(weights=weights),
        _trade_runs(),
        _comparison(),
        determinism_parity_passed=False,
    )

    positive_count = _criterion(result, "G2", "spot", "positive_asset_count")
    parity = _criterion(result, "G2", "spot", "determinism_and_parity")
    assert positive_count.value == 5
    assert positive_count.passed is False
    assert parity.passed is False
    assert result.overall_pass is False
    assert any("rule 6" in reason for reason in result.fail_reasons)


def test_future_outcome_mismatch_is_rejected() -> None:
    panels = _opportunity_panels()
    panels["G4"][0] = replace(panels["G4"][0], forward_return_12=0.123)

    with pytest.raises(ValueError, match="future outcome mismatch"):
        evaluate_r1_verdict(
            panels, _trade_runs(), _comparison(), determinism_parity_passed=True
        )


def test_verdict_csv_readers_are_typed_and_strict(tmp_path) -> None:
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
                "eligible",
                "analysis_eligible",
                "analysis_eligible_12",
                "volume_feature_available",
                "forward_return_12",
            ),
        )
        writer.writeheader()
        writer.writerow(
            {
                "opportunity_id": "o1",
                "asset": "btc",
                "market": "SPOT",
                "direction": "LONG",
                "decision_time_ms": "10",
                "eligible": "true",
                "analysis_eligible": "true",
                "analysis_eligible_12": "true",
                "volume_feature_available": "1",
                "forward_return_12": "0.01",
            }
        )
    trade_path = tmp_path / "trades.csv"
    with trade_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=(
                "trade_id",
                "asset",
                "market",
                "direction",
                "net_return",
                "net_pnl_usdt",
            ),
        )
        writer.writeheader()
        writer.writerow(
            {
                "trade_id": "t1",
                "asset": "btc",
                "market": "SPOT",
                "direction": "LONG",
                "net_return": "0.001",
                "net_pnl_usdt": "0.1",
            }
        )

    opportunities = read_verdict_opportunities(opportunity_path)
    trades = read_verdict_trades(trade_path)

    assert opportunities == (
        VerdictOpportunity("o1", "BTC", "spot", "long", 10, True, True, True, 0.01),
    )
    assert trades == (VerdictTrade("t1", "BTC", "spot", "long", 0.001, 0.1),)
