from __future__ import annotations

import csv
import hashlib
import json
import sys
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import pytest
import yaml

from conftest import ROOT
from signalbot.backtest.carry import (
    CarryEntryDecision,
    CarryExperimentSpec,
    CarryTrade,
    load_carry_spec,
)
from signalbot.backtest.carry_runner import (
    _shared_calendar_bootstrap,
    _validate_c1_funding_coverage,
    _validate_carry_ledgers,
    _validate_kline_manifest_coverage,
    analyze_carry_ledgers,
    build_daily_pair_pnl,
    evaluate_c1_acceptance,
    run_carry_experiment,
    write_carry_artifacts,
)
from signalbot.backtest.dataset import (
    DatasetManifest,
    DatasetValidationError,
    KlineDataset,
    KlineDatasetRequest,
    build_dataset_manifest,
    write_dataset_manifest,
    write_kline_csv,
)
from signalbot.backtest.engine import FundingRate, calculate_execution_returns
from signalbot.backtest.funding import FundingDataset, write_funding_csv
from signalbot.backtest.runner import dataset_path, funding_path
from signalbot.cli import _parser, main
from signalbot.domain.enums import Direction, Market
from signalbot.domain.models import Candle

_DAY_MS = 86_400_000
_STEP_MS = 300_000


def _spec() -> CarryExperimentSpec:
    return load_carry_spec(ROOT / "config/backtest.5m.c1-funding-basis-carry.yaml")


def _decision(
    spec: CarryExperimentSpec,
    *,
    identity: int,
    when: datetime,
    asset: str = "BTC",
    split: str = "validation",
    accepted: bool = True,
) -> CarryEntryDecision:
    decision_time = int(when.timestamp() * 1000) + _STEP_MS - 1
    entry_time = decision_time + 1
    cohort = next(item.cohort for item in spec.assets if item.asset == asset)
    triggering_funding_time = decision_time - 1 - identity
    decision_identity = "|".join(
        (
            spec.protocol_version,
            asset,
            split,
            str(decision_time),
            str(triggering_funding_time),
        )
    )
    return CarryEntryDecision(
        decision_id=hashlib.sha256(decision_identity.encode()).hexdigest()[:24],
        protocol_version=spec.protocol_version,
        rule_version=spec.rule_version,
        asset=asset,
        cohort=cohort,
        split=split,
        decision_time_ms=decision_time,
        entry_time_ms=entry_time if accepted else None,
        triggering_funding_time_ms=triggering_funding_time,
        basis=0.02,
        basis_median=0.01,
        basis_q90=0.015,
        basis_mad=0.001,
        funding_last=0.0001,
        funding_q25=0.00005,
        positive_funding_fraction=0.8,
        funding_cadence_ms=28_800_000.0,
        expected_funding_events=20,
        expected_pair_edge=0.01,
        stress_cost_hurdle=0.004,
        target_basis=0.01,
        stop_basis=0.03,
        accepted=accepted,
        rejection_reasons=() if accepted else ("frozen_rejection",),
    )


def _trade(
    decision: CarryEntryDecision,
    *,
    base_net_pnl: float = 0.30,
    analysis_eligible: bool = True,
) -> CarryTrade:
    if decision.entry_time_ms is None:
        raise ValueError("trade fixture requires an accepted decision")
    spot_entry = 100.0
    spot_exit = 101.0
    futures_entry = 102.0
    futures_exit = 101.0
    base_quantity = 100.0 / (spot_entry + futures_entry)
    spot_slippage_bps = {"anchor": 5.0, "major": 5.0, "volatile": 10.0}[
        decision.cohort
    ]
    futures_slippage_bps = {"anchor": 3.0, "major": 3.0, "volatile": 8.0}[
        decision.cohort
    ]
    spot_execution = calculate_execution_returns(
        Direction.LONG, spot_entry, spot_exit, 10.0, spot_slippage_bps
    )
    futures_execution = calculate_execution_returns(
        Direction.SHORT, futures_entry, futures_exit, 5.0, futures_slippage_bps
    )
    spot_scale = base_quantity * spot_entry
    futures_scale = base_quantity * futures_entry
    gross = (
        spot_execution.gross_return * spot_scale
        + futures_execution.gross_return * futures_scale
    )
    slippage = (
        spot_execution.slippage_return * spot_scale
        + futures_execution.slippage_return * futures_scale
    )
    fees = (
        spot_execution.fee_return * spot_scale
        + futures_execution.fee_return * futures_scale
    )
    funding = base_net_pnl - gross + slippage + fees
    exit_decision = decision.entry_time_ms + _STEP_MS - 1
    exit_time = exit_decision + 1
    exit_reason = "CONVERGENCE"
    trade_identity = "|".join(
        (decision.decision_id, str(exit_time), exit_reason)
    )
    return CarryTrade(
        trade_id=hashlib.sha256(trade_identity.encode()).hexdigest()[:24],
        decision_id=decision.decision_id,
        protocol_version=decision.protocol_version,
        rule_version=decision.rule_version,
        asset=decision.asset,
        cohort=decision.cohort,
        split=decision.split,
        triggering_funding_time_ms=decision.triggering_funding_time_ms,
        entry_decision_time_ms=decision.decision_time_ms,
        exit_decision_time_ms=exit_decision,
        entry_time_ms=decision.entry_time_ms,
        exit_time_ms=exit_time,
        exit_reason=exit_reason,
        target_basis=0.01,
        stop_basis=0.03,
        entry_signal_basis=0.02,
        entry_fill_basis=(futures_entry - spot_entry) / spot_entry,
        exit_signal_basis=0.01,
        base_quantity=base_quantity,
        pair_capital_usdt=100.0,
        spot_entry_price=spot_entry,
        spot_exit_price=spot_exit,
        futures_entry_price=futures_entry,
        futures_exit_price=futures_exit,
        gross_pnl_usdt=gross,
        slippage_usdt=slippage,
        fees_usdt=fees,
        funding_pnl_usdt=funding,
        net_pnl_usdt=base_net_pnl,
        gross_return=gross / 100.0,
        net_return=base_net_pnl / 100.0,
        funding_event_count=1,
        analysis_eligible=analysis_eligible,
        exclusion_reason="" if analysis_eligible else "common_5m_gap_while_open",
    )


def _coverage_manifest(
    spec: CarryExperimentSpec,
    *,
    market: Market = Market.SPOT,
    asset: str = "BTC",
) -> DatasetManifest:
    start_ms = int(spec.data_start.timestamp() * 1000)
    first_open_ms = 1_709_647_200_000 if market is Market.SPOT and asset == "WIF" else start_ms
    end_exclusive_ms = int(spec.evaluation_end.timestamp() * 1000)
    symbol = f"{asset}USDT"
    return DatasetManifest(
        schema_version=2,
        data_file=f"{asset}__{symbol}__5m.csv.gz",
        sha256="a" * 64,
        market=market.value,
        symbol=symbol,
        alias=asset,
        interval="5m",
        request_start_time_ms=start_ms,
        request_end_time_ms=end_exclusive_ms - 1,
        row_count=(end_exclusive_ms - first_open_ms) // _STEP_MS,
        first_open_time_ms=first_open_ms,
        last_close_time_ms=end_exclusive_ms - 1,
        gap_count=0,
        missing_intervals=0,
    )


def _complete_funding(
    spec: CarryExperimentSpec,
    asset: str,
) -> FundingDataset:
    start_ms = int(spec.data_start.timestamp() * 1000)
    end_exclusive_ms = int(spec.evaluation_end.timestamp() * 1000)
    interval_ms = (4 if asset == "WIF" else 8) * 3_600_000
    times = list(range(start_ms, end_exclusive_ms, interval_ms))
    if asset == "WIF":
        # Binance public USDⓈ-M fundingRate history has no settlement at this
        # timestamp; the adjacent official rows are 00:00 and 08:00 UTC.
        times.remove(1_782_273_600_000)
    rates = tuple(FundingRate(timestamp, 0.0001, 100.0) for timestamp in times)
    return FundingDataset(f"{asset}USDT", start_ms, end_exclusive_ms - 1, rates)


def test_kline_coverage_requires_exact_boundaries_grid_and_wif_listing() -> None:
    spec = _spec()
    btc = _coverage_manifest(spec)
    wif = _coverage_manifest(spec, asset="WIF")

    _validate_kline_manifest_coverage(btc, spec, Market.SPOT, "BTC")
    _validate_kline_manifest_coverage(wif, spec, Market.SPOT, "WIF")

    with pytest.raises(DatasetValidationError, match="first candle"):
        _validate_kline_manifest_coverage(
            replace(btc, first_open_time_ms=btc.first_open_time_ms + _STEP_MS),
            spec,
            Market.SPOT,
            "BTC",
        )
    with pytest.raises(DatasetValidationError, match="final candle"):
        _validate_kline_manifest_coverage(
            replace(btc, last_close_time_ms=btc.last_close_time_ms - _STEP_MS),
            spec,
            Market.SPOT,
            "BTC",
        )
    with pytest.raises(DatasetValidationError, match="internal 5m coverage gap"):
        _validate_kline_manifest_coverage(
            replace(btc, gap_count=1, missing_intervals=1, row_count=btc.row_count - 1),
            spec,
            Market.SPOT,
            "BTC",
        )
    with pytest.raises(DatasetValidationError, match="row count"):
        _validate_kline_manifest_coverage(
            replace(btc, row_count=btc.row_count - 1),
            spec,
            Market.SPOT,
            "BTC",
        )
    with pytest.raises(DatasetValidationError, match="first candle"):
        _validate_kline_manifest_coverage(
            replace(wif, first_open_time_ms=wif.first_open_time_ms - _STEP_MS),
            spec,
            Market.SPOT,
            "WIF",
        )


def test_funding_coverage_requires_boundaries_hour_grid_and_frozen_schedule() -> None:
    spec = _spec()
    btc = _complete_funding(spec, "BTC")
    wif = _complete_funding(spec, "WIF")

    _validate_c1_funding_coverage(btc, spec, "BTC")
    _validate_c1_funding_coverage(wif, spec, "WIF")

    with pytest.raises(ValueError, match="does not start"):
        _validate_c1_funding_coverage(
            replace(btc, rates=btc.rates[1:]), spec, "BTC"
        )
    with pytest.raises(ValueError, match="does not reach"):
        _validate_c1_funding_coverage(
            replace(btc, rates=btc.rates[:-1]), spec, "BTC"
        )
    with pytest.raises(ValueError, match="schedule gap"):
        _validate_c1_funding_coverage(
            replace(btc, rates=(*btc.rates[:100], *btc.rates[101:])), spec, "BTC"
        )

    off_grid = replace(
        btc.rates[100], funding_time_ms=btc.rates[100].funding_time_ms + 120_000
    )
    with pytest.raises(ValueError, match="hourly grid"):
        _validate_c1_funding_coverage(
            replace(btc, rates=(*btc.rates[:100], off_grid, *btc.rates[101:])),
            spec,
            "BTC",
        )

    extra_wif_gap_index = 100
    with pytest.raises(ValueError, match="schedule gap"):
        _validate_c1_funding_coverage(
            replace(
                wif,
                rates=(
                    *wif.rates[:extra_wif_gap_index],
                    *wif.rates[extra_wif_gap_index + 1 :],
                ),
            ),
            spec,
            "WIF",
        )


def test_daily_panel_uses_entry_decision_day_and_explicit_zero_days() -> None:
    spec = _spec()
    decision = _decision(
        spec,
        identity=1,
        when=datetime(2025, 3, 10, 23, 55, tzinfo=UTC),
    )
    trade = _trade(decision)

    rows = build_daily_pair_pnl([trade], spec)

    expected_days = (
        spec.acceptance.primary_calendar_end - spec.acceptance.primary_calendar_start
    ).days
    assert len(rows) == expected_days * len(spec.assets)
    attributed = [row for row in rows if row.eligible_completed_episodes]
    assert len(attributed) == 1
    assert attributed[0].utc_date == "2025-03-10"
    assert attributed[0].asset == "BTC"
    assert attributed[0].base_net_pnl_usdt == pytest.approx(0.30)
    assert attributed[0].two_x_slippage_net_pnl_usdt == pytest.approx(
        trade.net_pnl_usdt - trade.slippage_usdt
    )
    assert sum(row.eligible_completed_episodes == 0 for row in rows) == len(rows) - 1


def test_integrity_cardinality_and_cost_identity_fail_closed() -> None:
    spec = _spec()
    decision = _decision(
        spec, identity=1, when=datetime(2025, 3, 10, tzinfo=UTC)
    )
    trade = _trade(decision)

    integrity = _validate_carry_ledgers([decision], [], spec)
    assert integrity["outcome_unobservable"] == 1
    assert integrity["primary_outcome_unobservable"] == 1

    with pytest.raises(ValueError, match="deterministic ID"):
        _validate_carry_ledgers([replace(decision, decision_id="tampered")], [], spec)

    second_exit = trade.exit_time_ms + _STEP_MS
    second_identity = "|".join(
        (trade.decision_id, str(second_exit), trade.exit_reason)
    )
    second_trade = replace(
        trade,
        trade_id=hashlib.sha256(second_identity.encode()).hexdigest()[:24],
        exit_decision_time_ms=second_exit - 1,
        exit_time_ms=second_exit,
    )
    with pytest.raises(ValueError, match="multiple trades"):
        _validate_carry_ledgers([decision], [trade, second_trade], spec)

    rejected = _decision(
        spec,
        identity=2,
        when=datetime(2025, 3, 11, tzinfo=UTC),
        accepted=False,
    )
    with pytest.raises(ValueError, match="rejected carry decision produced"):
        _validate_carry_ledgers(
            [rejected],
            [replace(trade, decision_id=rejected.decision_id, trade_id="rejected-trade")],
            spec,
        )

    with pytest.raises(ValueError, match="P&L identity"):
        _validate_carry_ledgers(
            [decision], [replace(trade, net_pnl_usdt=trade.net_pnl_usdt + 1)], spec
        )


def test_ledger_requires_next_open_purges_frozen_levels_and_recomputed_costs() -> None:
    spec = _spec()
    decision = _decision(
        spec, identity=1, when=datetime(2025, 3, 10, tzinfo=UTC)
    )
    trade = _trade(decision)
    assert decision.entry_time_ms is not None

    with pytest.raises(ValueError, match="next 5m open"):
        _validate_carry_ledgers(
            [replace(decision, entry_time_ms=decision.entry_time_ms + _STEP_MS)],
            [],
            spec,
        )

    validation_start = next(item.start for item in spec.splits if item.name == "validation")
    lower_boundary_decision = _decision(
        spec,
        identity=2,
        when=validation_start + timedelta(days=7) - timedelta(minutes=5),
    )
    _validate_carry_ledgers(
        [lower_boundary_decision], [_trade(lower_boundary_decision)], spec
    )
    before_lower_boundary = _decision(
        spec,
        identity=3,
        when=validation_start + timedelta(days=7) - timedelta(minutes=10),
    )
    with pytest.raises(ValueError, match="split-start purge"):
        _validate_carry_ledgers([before_lower_boundary], [], spec)

    with pytest.raises(ValueError, match="frozen entry/exit levels"):
        _validate_carry_ledgers(
            [decision], [replace(trade, target_basis=trade.target_basis + 0.001)], spec
        )

    changed_slippage = trade.slippage_usdt + 0.01
    changed_net = (
        trade.gross_pnl_usdt
        - changed_slippage
        - trade.fees_usdt
        + trade.funding_pnl_usdt
    )
    with pytest.raises(ValueError, match="slippage does not match frozen costs"):
        _validate_carry_ledgers(
            [decision],
            [
                replace(
                    trade,
                    slippage_usdt=changed_slippage,
                    net_pnl_usdt=changed_net,
                    net_return=changed_net / trade.pair_capital_usdt,
                )
            ],
            spec,
        )

    with pytest.raises(ValueError, match="ordinary carry exit does not fill"):
        _validate_carry_ledgers(
            [decision],
            [replace(trade, exit_decision_time_ms=trade.exit_decision_time_ms - 1)],
            spec,
        )


def test_data_gap_and_ordinary_exit_eligibility_are_exact_equivalents() -> None:
    spec = _spec()
    decision = _decision(
        spec, identity=1, when=datetime(2025, 3, 10, tzinfo=UTC)
    )
    ordinary = _trade(decision)
    with pytest.raises(ValueError, match="ordinary carry exit must be analysis-eligible"):
        _validate_carry_ledgers(
            [decision],
            [
                replace(
                    ordinary,
                    analysis_eligible=False,
                    exclusion_reason="unexpected_exclusion",
                )
            ],
            spec,
        )

    gap_identity = "|".join(
        (ordinary.decision_id, str(ordinary.exit_time_ms), "DATA_GAP")
    )
    eligible_gap = replace(
        ordinary,
        trade_id=hashlib.sha256(gap_identity.encode()).hexdigest()[:24],
        exit_reason="DATA_GAP",
        exit_decision_time_ms=ordinary.exit_time_ms,
    )
    with pytest.raises(ValueError, match="DATA_GAP carry trade must be analysis-ineligible"):
        _validate_carry_ledgers([decision], [eligible_gap], spec)


def test_data_gap_trade_is_observed_but_analysis_ineligible() -> None:
    spec = _spec()
    decision = _decision(
        spec, identity=1, when=datetime(2025, 3, 10, tzinfo=UTC)
    )
    trade = _trade(decision, analysis_eligible=False)
    gap_identity = "|".join(
        (trade.decision_id, str(trade.exit_time_ms), "DATA_GAP")
    )
    trade = replace(
        trade,
        trade_id=hashlib.sha256(gap_identity.encode()).hexdigest()[:24],
        exit_reason="DATA_GAP",
        exit_decision_time_ms=trade.exit_time_ms,
    )

    integrity = _validate_carry_ledgers([decision], [trade], spec)
    daily = build_daily_pair_pnl([trade], spec)

    assert integrity["outcome_unobservable"] == 0
    assert integrity["analysis_ineligible_trades"] == 1
    assert sum(row.eligible_completed_episodes for row in daily) == 0


def test_shared_calendar_bootstrap_is_deterministic_and_order_invariant() -> None:
    spec = _spec()
    decision = _decision(
        spec, identity=1, when=datetime(2025, 3, 10, tzinfo=UTC)
    )
    rows = build_daily_pair_pnl([_trade(decision)], spec)

    first = _shared_calendar_bootstrap(rows, spec, samples=100, seed=17)
    second = _shared_calendar_bootstrap(tuple(reversed(rows)), spec, samples=100, seed=17)

    assert first == second
    assert first["calendar_days"] == 487
    assert first["samples"] == 100
    assert first["shared_draw_schedule_sha256"]
    assert first["invalid_replicates"] > 0


def _passing_acceptance_inputs() -> tuple[
    dict[str, object], dict[str, object], dict[str, dict[str, object]]
]:
    primary: dict[str, object] = {
        "completed_episodes": 100,
        "mean_pair_return": 0.001,
        "profit_factor": 1.25,
        "profit_factor_state": "FINITE",
    }
    bootstrap: dict[str, object] = {
        "one_sided_basic_95_lower": 0.00001,
        "valid_replicates": 49_950,
        "invalid_fraction": 0.001,
    }
    split: dict[str, dict[str, object]] = {
        name: {"net_pnl_usdt": 1.0, "mean_pair_return": 0.0001}
        for name in ("validation", "retrospective_test")
    }
    return primary, bootstrap, split


def test_acceptance_exact_boundaries_and_status_precedence() -> None:
    spec = _spec()
    primary, bootstrap, split = _passing_acceptance_inputs()

    status, gates = evaluate_c1_acceptance(
        primary=primary,
        bootstrap=bootstrap,
        split_summaries=split,
        maximum_asset_positive_pnl_share=0.50,
        primary_outcome_unobservable=0,
        spec=spec,
    )
    assert status == "EXPLORATORY_SCREEN_PASS"
    assert gates["all_gates_pass"] is True

    below_count = {**primary, "completed_episodes": 99}
    status, _ = evaluate_c1_acceptance(
        primary=below_count,
        bootstrap=bootstrap,
        split_summaries=split,
        maximum_asset_positive_pnl_share=0.50,
        primary_outcome_unobservable=0,
        spec=spec,
    )
    assert status == "INCONCLUSIVE_LOW_INFORMATION"

    zero_lower = {**bootstrap, "one_sided_basic_95_lower": 0.0}
    status, gates = evaluate_c1_acceptance(
        primary=primary,
        bootstrap=zero_lower,
        split_summaries=split,
        maximum_asset_positive_pnl_share=0.50,
        primary_outcome_unobservable=0,
        spec=spec,
    )
    assert status == "EXPLORATORY_FAIL"
    assert gates["seven_day_one_sided_basic_lower_above_zero"] is False

    status, _ = evaluate_c1_acceptance(
        primary=primary,
        bootstrap=bootstrap,
        split_summaries=split,
        maximum_asset_positive_pnl_share=0.50,
        primary_outcome_unobservable=1,
        spec=spec,
    )
    assert status == "INCONCLUSIVE_OUTCOME_UNOBSERVABLE"

    excessive_invalid = {**bootstrap, "invalid_fraction": 0.0010001}
    status, _ = evaluate_c1_acceptance(
        primary=primary,
        bootstrap=excessive_invalid,
        split_summaries=split,
        maximum_asset_positive_pnl_share=0.50,
        primary_outcome_unobservable=0,
        spec=spec,
    )
    assert status == "INCONCLUSIVE_LOW_INFORMATION"


def test_acceptance_rejects_zero_split_and_over_concentration() -> None:
    spec = _spec()
    primary, bootstrap, split = _passing_acceptance_inputs()
    split["validation"] = {"net_pnl_usdt": 0.0, "mean_pair_return": 0.0}

    status, gates = evaluate_c1_acceptance(
        primary=primary,
        bootstrap=bootstrap,
        split_summaries=split,
        maximum_asset_positive_pnl_share=0.500001,
        primary_outcome_unobservable=0,
        spec=spec,
    )

    assert status == "EXPLORATORY_FAIL"
    assert gates["required_positive_splits"]["validation"] == {
        "aggregate_pnl_positive": False,
        "mean_pair_return_positive": False,
    }
    assert gates["maximum_asset_positive_pnl_share_at_most_50_percent"] is False


@pytest.fixture(scope="module")
def profitable_ledgers() -> tuple[
    CarryExperimentSpec,
    tuple[CarryEntryDecision, ...],
    tuple[CarryTrade, ...],
    dict[str, object],
    tuple[object, ...],
]:
    spec = _spec()
    decisions: list[CarryEntryDecision] = []
    trades: list[CarryTrade] = []
    identity = 0
    for split, start in (
        ("validation", datetime(2025, 3, 10, tzinfo=UTC)),
        ("retrospective_test", datetime(2025, 11, 10, tzinfo=UTC)),
    ):
        for index in range(50):
            asset = "BTC" if index % 2 == 0 else "ETH"
            decision = _decision(
                spec,
                identity=identity,
                when=start + timedelta(days=index * 4),
                asset=asset,
                split=split,
            )
            decisions.append(decision)
            trades.append(_trade(decision))
            identity += 1
    analysis, daily = analyze_carry_ledgers(decisions, trades, spec)
    return spec, tuple(decisions), tuple(trades), analysis, tuple(daily)


def test_full_analysis_pass_is_still_exploratory_and_not_deployable(
    profitable_ledgers,
) -> None:
    _, _, trades, analysis, _ = profitable_ledgers

    assert analysis["status_axes"] == {
        "data_integrity": "PASS",
        "efficacy": "EXPLORATORY_SCREEN_PASS",
        "execution_validity": "INCONCLUSIVE_KLINE_NEXT_OPEN_NO_HISTORICAL_BBO",
        "exposure": "EXPOSED_RETROSPECTIVE_ONLY",
        "deployment": "NOT_DEPLOYABLE",
    }
    assert analysis["population"]["primary_analysis_eligible_trades"] == 100
    assert analysis["base_costs_primary"]["mean_pair_return_bps"] == pytest.approx(30)
    expected_stress_mean_bps = (
        sum(item.net_pnl_usdt - item.slippage_usdt for item in trades)
        / len(trades)
        / 100.0
        * 10_000
    )
    assert analysis["primary_two_x_slippage"]["mean_pair_return_bps"] == pytest.approx(
        expected_stress_mean_bps
    )
    assert analysis["asset_positive_pnl_concentration"][
        "maximum_positive_pnl_share"
    ] == pytest.approx(0.5)
    assert analysis["multiplicity"]["holm_applied"] is False


def test_writers_emit_exact_schemas_hash_manifest_and_stable_bytes(
    profitable_ledgers,
    tmp_path: Path,
) -> None:
    spec, decisions, trades, analysis, daily = profitable_ledgers
    workspace = tmp_path / "workspace"
    (workspace / "src" / "signalbot").mkdir(parents=True)
    (workspace / "src" / "signalbot" / "sentinel.py").write_text(
        "VALUE = 1\n", encoding="utf-8"
    )
    plan_path = workspace / spec.experiment_plan_path
    plan_path.parent.mkdir(parents=True)
    plan_path.write_text("# frozen C1 plan\n", encoding="utf-8")
    spec_path = workspace / "c1.yaml"
    spec_path.write_text(yaml.safe_dump(spec.model_dump(mode="json")), encoding="utf-8")

    first = write_carry_artifacts(
        decisions=decisions,
        trades=trades,
        daily=daily,
        analysis=analysis,
        spec=spec,
        spec_path=spec_path,
        output_dir=workspace / "first",
        workspace_root=workspace,
        input_hashes={"spot/input.csv.gz": "a" * 64},
        started_at_utc=datetime(2026, 7, 17, tzinfo=UTC),
        duration_seconds=1.0,
    )
    second = write_carry_artifacts(
        decisions=decisions,
        trades=trades,
        daily=daily,
        analysis=analysis,
        spec=spec,
        spec_path=spec_path,
        output_dir=workspace / "second",
        workspace_root=workspace,
        input_hashes={"spot/input.csv.gz": "a" * 64},
        started_at_utc=datetime(2026, 7, 17, tzinfo=UTC),
        duration_seconds=1.0,
    )

    with Path(first["c1_decisions.csv"]).open(encoding="utf-8", newline="") as handle:
        decision_reader = csv.DictReader(handle)
        assert decision_reader.fieldnames is not None
        first_decision = next(decision_reader)
    assert json.loads(first_decision["rejection_reasons"]) == []
    with Path(first["c1_trades.csv"]).open(encoding="utf-8", newline="") as handle:
        trade_reader = csv.DictReader(handle)
        assert trade_reader.fieldnames is not None
        assert trade_reader.fieldnames[-2:] == [
            "two_x_slippage_pnl_usdt",
            "two_x_slippage_return",
        ]
    manifest = json.loads(Path(first["manifest"]).read_text(encoding="utf-8"))
    assert manifest["frozen_contract"]["artifact_has_order_placement"] is False
    assert set(manifest["outputs"]) == {
        "c1_decisions.csv",
        "c1_trades.csv",
        "c1_daily_pair_pnl.csv",
        "c1_analysis.json",
        "c1_report_ko.md",
    }
    for name in manifest["outputs"]:
        assert Path(first[name]).read_bytes() == Path(second[name]).read_bytes()
    assert Path(first["manifest"]).read_bytes() == Path(second["manifest"]).read_bytes()


def _one_candle_dataset(request: KlineDatasetRequest) -> KlineDataset:
    open_time = request.start_time_ms
    candle = Candle(
        market=request.market,
        symbol=request.symbol,
        interval=request.interval,
        open_time_ms=open_time,
        close_time_ms=open_time + _STEP_MS - 1,
        open=Decimal("100"),
        high=Decimal("100"),
        low=Decimal("100"),
        close=Decimal("100"),
        volume=Decimal("1"),
        quote_volume=Decimal("100"),
        trade_count=1,
        taker_buy_base_volume=Decimal("0.5"),
        taker_buy_quote_volume=Decimal("50"),
        is_closed=True,
    )
    return KlineDataset(request, (candle,))


def test_full_runner_rejects_request_metadata_without_actual_kline_coverage(
    tmp_path: Path,
) -> None:
    spec = _spec()
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    plan_path = workspace / spec.experiment_plan_path
    plan_path.parent.mkdir(parents=True)
    plan_path.write_text("# frozen\n", encoding="utf-8")
    spec_path = workspace / "c1.yaml"
    spec_path.write_text(yaml.safe_dump(spec.model_dump(mode="json")), encoding="utf-8")
    data_root = tmp_path / "data"
    start_ms = int(spec.data_start.timestamp() * 1000)
    end_ms = int(spec.evaluation_end.timestamp() * 1000) - 1
    for asset in spec.assets:
        for market, symbol in (
            (Market.SPOT, asset.spot_symbol),
            (Market.FUTURES, asset.futures_symbol),
        ):
            request = KlineDatasetRequest(
                market=market,
                symbol=symbol,
                alias=asset.asset,
                interval=spec.interval,
                start_time_ms=start_ms,
                end_time_ms=end_ms,
            )
            path = dataset_path(data_root, market, asset.asset, symbol, spec.interval)
            write_kline_csv(_one_candle_dataset(request), path)
            write_dataset_manifest(
                build_dataset_manifest(path),
                path.with_suffix(path.suffix + ".manifest.json"),
            )
        funding_file = funding_path(
            data_root, asset.asset, asset.futures_symbol, spec.interval
        )
        write_funding_csv(
            FundingDataset(
                asset.futures_symbol,
                start_ms,
                end_ms,
                (FundingRate(start_ms, 0.0001, 100.0),),
            ),
            funding_file,
        )

    with pytest.raises(DatasetValidationError, match="final candle"):
        run_carry_experiment(
            spec_path,
            data_root,
            tmp_path / "output",
            workspace_root=workspace,
        )


def test_c1_cli_is_configless_and_dispatches_research_runner(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    args = _parser().parse_args(
        [
            "backtest-c1-run",
            "--spec",
            "c1.yaml",
            "--data-dir",
            "data",
            "--output-dir",
            "output",
        ]
    )
    assert args.command == "backtest-c1-run"
    assert not hasattr(args, "config")

    observed: dict[str, object] = {}

    def fake_runner(
        spec_path: str,
        data_dir: str,
        output_dir: str,
        *,
        workspace_root: Path,
    ) -> tuple[dict[str, object], dict[str, str]]:
        observed.update(
            spec_path=spec_path,
            data_dir=data_dir,
            output_dir=output_dir,
            workspace_root=workspace_root,
        )
        return (
            {
                "status_axes": {
                    "data_integrity": "PASS",
                    "efficacy": "EXPLORATORY_FAIL",
                    "deployment": "NOT_DEPLOYABLE",
                }
            },
            {"manifest": "manifest.json"},
        )

    monkeypatch.setattr("signalbot.cli.run_carry_experiment", fake_runner)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "signalbot",
            "backtest-c1-run",
            "--spec",
            "c1.yaml",
            "--data-dir",
            "data",
            "--output-dir",
            "output",
        ],
    )

    main()

    output = json.loads(capsys.readouterr().out)
    assert output["status"] == "EXPLORATORY_FAIL"
    assert output["deployment"] == "NOT_DEPLOYABLE"
    assert observed["spec_path"] == "c1.yaml"
