from __future__ import annotations

import csv
import hashlib
import json
from dataclasses import fields, replace
from pathlib import Path
from typing import Any

import pytest

from signalbot.backtest.config import BacktestSpec, load_backtest_spec
from signalbot.backtest.engine import calculate_execution_returns
from signalbot.backtest.labels import classify_kline_proxy_outcome
from signalbot.backtest.r3 import (
    _OPPORTUNITY_SCHEMA_FIELDS,
    _TRADE_SCHEMA_FIELDS,
    R3Opportunity,
    R3TechnicalTrade,
    _analyze_r3_diagnostic,
    _holm_two,
    _validate_raw_results_contract,
    _validate_source_code_digest,
    _verify_input_panel_ledger,
    analyze_r3_diagnostic,
    evaluate_frozen_r3_screen,
    read_r3_opportunities,
    shared_calendar_moving_block_bootstrap,
    validate_r3_integrity,
    write_r3_analysis,
)
from signalbot.domain.enums import Direction

DAY_MS = 86_400_000
INTERVAL_MS = 300_000
SPEC_PATH = Path("config/backtest.5m.r3-c0-causal-labels.yaml")


def _required(value: float | None) -> float:
    assert value is not None
    return value


@pytest.fixture(scope="module")
def spec() -> BacktestSpec:
    return load_backtest_spec(SPEC_PATH)


def _opportunity(
    spec: BacktestSpec,
    asset_index: int,
    market: str,
    *,
    day: int = 20,
    net: float = 0.002,
    analysis_eligible: bool = True,
    analysis_eligible_72: bool | None = None,
    decision_time_ms: int | None = None,
) -> R3Opportunity:
    asset = spec.assets[asset_index]
    symbol = asset.spot_symbol if market == "spot" else asset.futures_symbol
    family = "breakout_long" if market == "spot" else "breakdown_short"
    direction = "long" if market == "spot" else "short"
    if decision_time_ms is None:
        start_ms = int(spec.evaluation_start.timestamp() * 1000)
        decision_time_ms = start_ms + day * DAY_MS + INTERVAL_MS - 1
    next_open = decision_time_ms + 1
    split = spec.split_name(next_open)
    assert split is not None
    if market == "spot":
        fee_bps = spec.costs.spot_fee_bps
        slippage_bps = spec.costs.spot_slippage_bps[asset.cohort]
    else:
        fee_bps = spec.costs.futures_fee_bps
        slippage_bps = spec.costs.futures_slippage_bps[asset.cohort]
    fee_rate = fee_bps / 10_000
    slippage_rate = slippage_bps / 10_000
    funding = 0.0
    if market == "spot":
        signal_gross = (net + 2 * slippage_rate + 2 * fee_rate) / (
            (1 - slippage_rate) * (1 - fee_rate)
        )
        underlying_return = signal_gross
    else:
        signal_gross = (net + 2 * slippage_rate + 2 * fee_rate) / (
            (1 + slippage_rate) * (1 + fee_rate)
        )
        underlying_return = -signal_gross
    long_execution = calculate_execution_returns(
        Direction.LONG,
        100.0,
        100.0 * (1 + underlying_return),
        fee_bps,
        slippage_bps,
    )
    short_execution = calculate_execution_returns(
        Direction.SHORT,
        100.0,
        100.0 * (1 + underlying_return),
        fee_bps,
        slippage_bps,
    )
    signal_execution = long_execution if market == "spot" else short_execution
    gross = signal_execution.gross_return
    fee = signal_execution.fee_return
    slippage = signal_execution.slippage_return
    long_net = long_execution.net_before_funding
    short_net = short_execution.net_before_funding
    net = signal_execution.net_before_funding
    label = classify_kline_proxy_outcome(long_net, short_net, 0).value
    opportunity_id = hashlib.sha256(
        f"{market}|{symbol}|{family}|{decision_time_ms}".encode()
    ).hexdigest()[:24]
    eligibility = (analysis_eligible,) * 3
    exclusions = ("",) * 3 if analysis_eligible else ("horizon_crosses_split",) * 3
    optional_value = (gross,) * 3 if analysis_eligible else (None,) * 3
    optional_fee = (fee,) * 3 if analysis_eligible else (None,) * 3
    optional_slippage = (slippage,) * 3 if analysis_eligible else (None,) * 3
    optional_funding = (funding,) * 3 if analysis_eligible else (None,) * 3
    optional_net = (net,) * 3 if analysis_eligible else (None,) * 3
    optional_long = (long_net,) * 3 if analysis_eligible else (None,) * 3
    optional_short = (short_net,) * 3 if analysis_eligible else (None,) * 3
    optional_label = (label,) * 3 if analysis_eligible else (None,) * 3
    eligible_72 = analysis_eligible if analysis_eligible_72 is None else analysis_eligible_72
    f60 = (
        (gross, fee, slippage, funding, net)
        if analysis_eligible
        else (None, None, None, None, None)
    )
    return R3Opportunity(
        opportunity_id=opportunity_id,
        protocol_version=spec.protocol_version,
        rule_version=spec.rule_version,
        asset=asset.asset,
        cohort=asset.cohort,
        market=market,
        symbol=symbol,
        direction=direction,
        family=family,
        decision_time_ms=decision_time_ms,
        next_open_time_ms=next_open,
        reasons="frozen C0 trigger",
        invalidation=1.0,
        eligible=True,
        gate_passed=True,
        execution_observed=False,
        full_r2_eligible=None,
        split=split,
        regime="neutral",
        btc_trend="neutral",
        breadth_ratio=0.5,
        analysis_eligible=analysis_eligible,
        analysis_exclusion=exclusions[2],
        analysis_eligible_by_horizon=eligibility,
        analysis_exclusion_by_horizon=exclusions,
        analysis_eligible_72=eligible_72,
        analysis_exclusion_72="" if eligible_72 else "horizon_crosses_split",
        forward_returns=optional_value,
        long_net_returns=optional_long,
        short_net_returns=optional_short,
        outcome_labels=optional_label,
        signal_gross_returns=optional_value,
        signal_fee_returns=optional_fee,
        signal_slippage_returns=optional_slippage,
        signal_funding_returns=optional_funding,
        signal_net_returns=optional_net,
        f60_execution_model="next_5m_open_to_12th_close_kline_proxy",
        f60_components=f60,
    )


def _panel(spec: BacktestSpec, *, net: float = 0.002) -> tuple[R3Opportunity, ...]:
    return tuple(
        _opportunity(spec, asset_index, market, net=net)
        for market in ("spot", "futures")
        for asset_index in range(len(spec.assets))
    )


def _trade(
    opportunity: R3Opportunity,
    *,
    exit_time_ms: int,
    exit_reason: str,
    split_contained: bool,
) -> R3TechnicalTrade:
    entry_signal_id = f"signal-{opportunity.opportunity_id}"
    entry_price = 100.0
    exit_price = 101.0 if opportunity.market == "spot" else 99.0
    spec = load_backtest_spec(SPEC_PATH)
    if opportunity.market == "spot":
        fee_bps = spec.costs.spot_fee_bps
        slippage_bps = spec.costs.spot_slippage_bps[opportunity.cohort]
        direction = Direction.LONG
    else:
        fee_bps = spec.costs.futures_fee_bps
        slippage_bps = spec.costs.futures_slippage_bps[opportunity.cohort]
        direction = Direction.SHORT
    execution = calculate_execution_returns(
        direction,
        entry_price,
        exit_price,
        fee_bps,
        slippage_bps,
    )
    trade_id = hashlib.sha256(
        "|".join(
            (
                opportunity.protocol_version,
                entry_signal_id,
                str(opportunity.next_open_time_ms),
                str(exit_time_ms),
                exit_reason,
            )
        ).encode()
    ).hexdigest()[:24]
    return R3TechnicalTrade(
        trade_id=trade_id,
        opportunity_id=opportunity.opportunity_id,
        protocol_version=opportunity.protocol_version,
        rule_version=opportunity.rule_version,
        asset=opportunity.asset,
        cohort=opportunity.cohort,
        market=opportunity.market,
        symbol=opportunity.symbol,
        direction=opportunity.direction,
        family=opportunity.family,
        split=opportunity.split,
        split_contained=split_contained,
        entry_signal_id=entry_signal_id,
        decision_time_ms=opportunity.decision_time_ms,
        entry_time_ms=opportunity.next_open_time_ms,
        exit_time_ms=exit_time_ms,
        entry_price=entry_price,
        exit_price=exit_price,
        entry_execution_price=execution.entry_execution_price,
        exit_execution_price=execution.exit_execution_price,
        exit_reason=exit_reason,
        bars_held=1,
        gross_return=execution.gross_return,
        slippage_return=execution.slippage_return,
        fee_return=execution.fee_return,
        funding_return=0.0,
        net_return=execution.net_before_funding,
    )


def test_integrity_fails_closed_on_duplicate_and_bad_label(spec: BacktestSpec) -> None:
    rows = _panel(spec)
    duplicate = _analyze_r3_diagnostic(
        (*rows, rows[0]), (), spec, bootstrap_samples=20, bootstrap_seed=17
    )
    bad_label = replace(
        rows[0],
        outcome_labels=("KLINE_PROXY_SHORT",) * 3,
    )
    wrong = _analyze_r3_diagnostic(
        (bad_label, *rows[1:]), (), spec, bootstrap_samples=20, bootstrap_seed=17
    )

    assert duplicate["status_axes"]["data_integrity"] == "FAIL"
    assert "duplicate opportunity_id" in duplicate["integrity"]["reason"]
    assert wrong["status_axes"]["data_integrity"] == "FAIL"
    assert "incorrect H3 outcome label" in wrong["integrity"]["reason"]
    assert wrong["market_horizon_summaries"] == []

    observed_execution = replace(rows[0], execution_observed=True)
    observed = _analyze_r3_diagnostic(
        (observed_execution, *rows[1:]),
        (),
        spec,
        bootstrap_samples=20,
        bootstrap_seed=17,
    )
    assert observed["status_axes"]["data_integrity"] == "FAIL"
    assert "no-historical-BBO" in observed["integrity"]["reason"]


def test_bootstrap_includes_zero_days_and_reuses_deterministic_shared_draws(
    spec: BacktestSpec,
) -> None:
    rows = _panel(spec)
    start_day = int(spec.evaluation_start.timestamp() * 1000) // DAY_MS
    day_count = (spec.evaluation_end - spec.evaluation_start).days
    by_market = {
        market: [item for item in rows if item.market == market]
        for market in ("spot", "futures")
    }
    first = shared_calendar_moving_block_bootstrap(
        by_market,
        calendar_start_day=start_day,
        calendar_day_count=day_count,
        samples=100,
        seed=17,
        block_days=7,
    )
    second = shared_calendar_moving_block_bootstrap(
        by_market,
        calendar_start_day=start_day,
        calendar_day_count=day_count,
        samples=100,
        seed=17,
        block_days=7,
    )

    assert first == second
    assert first["zero_days_by_market"] == {"spot": day_count - 1, "futures": day_count - 1}
    assert first["shared_draw_schedule_sha256"] == second["shared_draw_schedule_sha256"]
    assert first["markets"]["spot"]["invalid_replicates"] > 0
    assert first["markets"]["spot"]["invalid_replicates"] == first["markets"][
        "futures"
    ]["invalid_replicates"]


def test_asymmetric_markets_use_one_paired_day_draw_schedule(spec: BacktestSpec) -> None:
    spot_rows = [
        _opportunity(
            spec,
            day % len(spec.assets),
            "spot",
            day=20 + day,
            net=0.008 if day % 2 == 0 else -0.002,
        )
        for day in range(30)
    ]
    futures_rows = [
        _opportunity(
            spec,
            day % len(spec.assets),
            "futures",
            day=20 + day,
            net=0.015 if day % 3 == 0 else -0.006,
        )
        for day in range(30)
    ]
    calendar_start = (
        int(spec.evaluation_start.timestamp() * 1000) // DAY_MS + 20
    )
    first = shared_calendar_moving_block_bootstrap(
        {"spot": spot_rows, "futures": futures_rows},
        calendar_start_day=calendar_start,
        calendar_day_count=30,
        samples=500,
        seed=29,
        block_days=7,
    )
    swapped = shared_calendar_moving_block_bootstrap(
        {"spot": futures_rows, "futures": spot_rows},
        calendar_start_day=calendar_start,
        calendar_day_count=30,
        samples=500,
        seed=29,
        block_days=7,
    )

    assert first["shared_draw_schedule_sha256"] == swapped[
        "shared_draw_schedule_sha256"
    ]
    assert first["markets"]["spot"] == swapped["markets"]["futures"]
    assert first["markets"]["futures"] == swapped["markets"]["spot"]
    assert first["markets"]["spot"]["two_sided_95_interval"] != first["markets"][
        "futures"
    ]["two_sided_95_interval"]


def _screen_fixtures() -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    summary = {
        "eligible_opportunities": 600,
        "represented_utc_days": 150,
        "mean_net_return": 0.001,
        "profit_factor": 1.2,
        "profit_factor_state": "FINITE",
        "slippage_sensitivity": {"two_x_mean_net_return": 0.0005},
    }
    uncertainty = {
        "one_sided_basic_95_lower": 0.0001,
        "invalid_rate": 0.0,
    }
    concentration = {
        "positive_assets": 7,
        "maximum_positive_concentration": 0.2,
    }
    return (
        {market: dict(summary) for market in ("spot", "futures")},
        {market: dict(uncertainty) for market in ("spot", "futures")},
        {market: dict(concentration) for market in ("spot", "futures")},
    )


def test_frozen_screen_pass_adverse_and_inconclusive_precedence() -> None:
    primary, uncertainty, concentration = _screen_fixtures()
    status, detail = evaluate_frozen_r3_screen(primary, uncertainty, concentration)
    assert status == "EXPLORATORY_SCREEN_PASS"
    assert detail["both_markets_all_criteria"] is True

    primary["spot"] = {**primary["spot"], "mean_net_return": -0.0001}
    status, _ = evaluate_frozen_r3_screen(primary, uncertainty, concentration)
    assert status == "EXPLORATORY_FAIL"

    primary, uncertainty, concentration = _screen_fixtures()
    primary["spot"] = {**primary["spot"], "eligible_opportunities": 499}
    status, _ = evaluate_frozen_r3_screen(primary, uncertainty, concentration)
    assert status == "INCONCLUSIVE_LOW_INFORMATION"


def test_frozen_screen_exact_decision_boundaries_and_holm_chain() -> None:
    primary, uncertainty, concentration = _screen_fixtures()
    primary["spot"] = {**primary["spot"], "mean_net_return": 0.0}
    status, _ = evaluate_frozen_r3_screen(primary, uncertainty, concentration)
    assert status == "EXPLORATORY_FAIL"

    primary, uncertainty, concentration = _screen_fixtures()
    primary["spot"] = {**primary["spot"], "mean_net_return": 0.0005}
    _, detail = evaluate_frozen_r3_screen(primary, uncertainty, concentration)
    assert detail["markets"]["spot"]["mean_net_return_greater_than_5_bps"] is False

    primary, uncertainty, concentration = _screen_fixtures()
    uncertainty["spot"] = {**uncertainty["spot"], "one_sided_basic_95_lower": 0.0}
    primary["spot"] = {**primary["spot"], "profit_factor": 1.05}
    primary["futures"] = {
        **primary["futures"],
        "slippage_sensitivity": {"two_x_mean_net_return": 0.0},
    }
    concentration["futures"] = {
        **concentration["futures"],
        "maximum_positive_concentration": 0.35,
    }
    _, detail = evaluate_frozen_r3_screen(primary, uncertainty, concentration)
    assert detail["markets"]["spot"]["seven_day_basic_lower_greater_than_zero"] is False
    assert detail["markets"]["spot"]["profit_factor_greater_than_1_05"] is False
    assert detail["markets"]["futures"]["two_x_slippage_mean_nonnegative"] is True
    concentration_key = (
        "maximum_positive_asset_concentration_at_most_35_percent"
    )
    assert detail["markets"]["futures"][concentration_key] is True

    equal = _holm_two({"spot": 0.025, "futures": 0.05})
    assert equal["spot"]["rejected"] is True
    assert equal["futures"]["rejected"] is True
    stopped = _holm_two({"spot": 0.03, "futures": 0.04})
    assert stopped["spot"]["rejected"] is False
    assert stopped["futures"]["rejected"] is False

    primary, uncertainty, concentration = _screen_fixtures()
    uncertainty["spot"] = {**uncertainty["spot"], "invalid_rate": 0.00101}
    status, _ = evaluate_frozen_r3_screen(primary, uncertainty, concentration)
    assert status == "INCONCLUSIVE_LOW_INFORMATION"


def test_market_horizon_summary_has_cost_labels_and_long_short_counterfactuals(
    spec: BacktestSpec,
) -> None:
    panel = _panel(spec)
    result = _analyze_r3_diagnostic(
        panel, (), spec, bootstrap_samples=20, bootstrap_seed=17
    )

    spot = result["primary_60m"]["spot"]
    spot_rows = [item for item in panel if item.market == "spot"]
    mean_slippage = sum(_required(item.signal_slippage_returns[2]) for item in spot_rows) / len(
        spot_rows
    )
    mean_fee = sum(_required(item.signal_fee_returns[2]) for item in spot_rows) / len(
        spot_rows
    )
    mean_short = sum(_required(item.short_net_returns[2]) for item in spot_rows) / len(
        spot_rows
    )
    assert spot["mean_net_bps"] == pytest.approx(20)
    assert spot["cost_decomposition"]["mean_fee_bps"] == pytest.approx(
        mean_fee * 10_000
    )
    assert spot["slippage_sensitivity"]["zero_x_mean_net_bps"] == pytest.approx(
        20 + mean_slippage * 10_000
    )
    assert spot["slippage_sensitivity"]["two_x_mean_net_bps"] == pytest.approx(
        20 - mean_slippage * 10_000
    )
    assert spot["label_counts"] == {
        "KLINE_PROXY_LONG": 8,
        "KLINE_PROXY_FLAT": 0,
        "KLINE_PROXY_SHORT": 0,
    }
    assert spot["directional_label_rates"] == {
        "allowed_direction_label": "KLINE_PROXY_LONG",
        "directional_hit_rate": 1.0,
        "abstention_rate": 0.0,
        "opposite_direction_rate": 0.0,
    }
    assert spot["research_counterfactuals"]["mean_long_net_bps"] == pytest.approx(20)
    assert spot["research_counterfactuals"]["mean_short_net_bps"] == pytest.approx(
        mean_short * 10_000
    )
    assert "never a new Spot short order" in result["semantic_boundaries"][
        "spot_short_label"
    ]
    assert "per-event funding IDs" in result["analysis_contract"][
        "funding_provenance_scope"
    ]


def test_t72_split_time_bounds_and_noncontained_trades_fail_closed(
    spec: BacktestSpec,
) -> None:
    rows = list(_panel(spec))
    split_start_ms = int(spec.splits[0].start.timestamp() * 1000)
    split_end_ms = int(spec.splits[0].end.timestamp() * 1000)
    start_boundary = _opportunity(
        spec,
        0,
        "spot",
        decision_time_ms=split_start_ms + 72 * INTERVAL_MS - 1,
    )
    end_boundary = _opportunity(
        spec,
        0,
        "spot",
        decision_time_ms=split_end_ms - 73 * INTERVAL_MS - 1,
    )
    rows.extend((start_boundary, end_boundary))
    contained = _trade(
        rows[1],
        exit_time_ms=rows[1].next_open_time_ms + INTERVAL_MS - 1,
        exit_reason="initial_stop",
        split_contained=True,
    )

    valid = validate_r3_integrity(rows, (contained,), spec)
    result = _analyze_r3_diagnostic(
        rows, (contained,), spec, bootstrap_samples=20, bootstrap_seed=17
    )
    assert valid["valid"] is True
    assert result["status_axes"]["data_integrity"] == "PASS"

    noncontained = _trade(
        rows[1],
        exit_time_ms=rows[1].next_open_time_ms + INTERVAL_MS,
        exit_reason="trend_failure",
        split_contained=False,
    )
    with pytest.raises(ValueError, match="non-contained R3 technical exit"):
        validate_r3_integrity(rows, (noncontained,), spec)

    for impossible_reason in ("split_boundary", "data_gap", "end_of_data"):
        impossible = _trade(
            rows[1],
            exit_time_ms=rows[1].next_open_time_ms + INTERVAL_MS,
            exit_reason=impossible_reason,
            split_contained=False,
        )
        with pytest.raises(ValueError, match="unknown technical exit reason"):
            validate_r3_integrity(rows, (impossible,), spec)

    too_early = _opportunity(
        spec,
        0,
        "spot",
        decision_time_ms=split_start_ms + 71 * INTERVAL_MS - 1,
    )
    with pytest.raises(ValueError, match="gap-free time bounds"):
        validate_r3_integrity((*_panel(spec), too_early), (), spec)

    too_late = _opportunity(
        spec,
        0,
        "spot",
        decision_time_ms=split_end_ms - 72 * INTERVAL_MS - 1,
    )
    with pytest.raises(ValueError, match="gap-free time bounds"):
        validate_r3_integrity((*_panel(spec), too_late), (), spec)

    forged_false = replace(
        start_boundary,
        analysis_eligible_72=False,
        analysis_exclusion_72="forged_exclusion",
    )
    with pytest.raises(ValueError, match="gap-free time bounds"):
        validate_r3_integrity((*_panel(spec), forged_false), (), spec)


def test_technical_trade_clock_reason_and_frozen_cost_parity(spec: BacktestSpec) -> None:
    rows = list(_panel(spec))
    opportunity = rows[0]
    time_exit = replace(
        _trade(
            opportunity,
            exit_time_ms=opportunity.next_open_time_ms + 72 * INTERVAL_MS,
            exit_reason="time_exit",
            split_contained=True,
        ),
        bars_held=72,
    )
    assert validate_r3_integrity(rows, (time_exit,), spec)["valid"] is True

    wrong_bars = replace(time_exit, bars_held=71)
    with pytest.raises(ValueError, match="bars_held disagrees"):
        validate_r3_integrity(rows, (wrong_bars,), spec)

    early_time_exit = replace(
        _trade(
            opportunity,
            exit_time_ms=time_exit.entry_time_ms + INTERVAL_MS,
            exit_reason="time_exit",
            split_contained=True,
        ),
        bars_held=72,
    )
    with pytest.raises(ValueError, match="bars_held disagrees"):
        validate_r3_integrity(rows, (early_time_exit,), spec)

    changed_fee = time_exit.fee_return + 0.0001
    self_consistent_fee_tamper = replace(
        time_exit,
        fee_return=changed_fee,
        net_return=(
            time_exit.gross_return
            - time_exit.slippage_return
            - changed_fee
            + time_exit.funding_return
        ),
    )
    with pytest.raises(ValueError, match="technical fee parity"):
        validate_r3_integrity(rows, (self_consistent_fee_tamper,), spec)

    negative_slippage = replace(
        time_exit,
        slippage_return=-time_exit.slippage_return,
        net_return=(
            time_exit.gross_return
            + time_exit.slippage_return
            - time_exit.fee_return
            + time_exit.funding_return
        ),
    )
    with pytest.raises(ValueError, match="negative execution cost"):
        validate_r3_integrity(rows, (negative_slippage,), spec)

    spot_funding = replace(
        time_exit,
        funding_return=0.0001,
        net_return=time_exit.net_return + 0.0001,
    )
    with pytest.raises(ValueError, match="Spot technical trade has nonzero funding"):
        validate_r3_integrity(rows, (spot_funding,), spec)


def test_opportunity_frozen_cost_and_opposite_funding_parity(spec: BacktestSpec) -> None:
    rows = list(_panel(spec))
    spot = rows[0]
    changed_fee = tuple(_required(value) + 0.0001 for value in spot.signal_fee_returns)
    changed_net = tuple(
        _required(gross) - fee - _required(slippage) + _required(funding)
        for gross, fee, slippage, funding in zip(
            spot.signal_gross_returns,
            changed_fee,
            spot.signal_slippage_returns,
            spot.signal_funding_returns,
            strict=True,
        )
    )
    fee_tamper = replace(
        spot,
        signal_fee_returns=changed_fee,
        signal_net_returns=changed_net,
        long_net_returns=changed_net,
        f60_components=(
            spot.signal_gross_returns[2],
            changed_fee[2],
            spot.signal_slippage_returns[2],
            spot.signal_funding_returns[2],
            changed_net[2],
        ),
    )
    with pytest.raises(ValueError, match="frozen fee cost parity"):
        validate_r3_integrity((fee_tamper, *rows[1:]), (), spec)

    futures_index = next(index for index, item in enumerate(rows) if item.market == "futures")
    futures = rows[futures_index]
    funding = (0.0002,) * 3
    short_net = tuple(_required(value) + 0.0002 for value in futures.short_net_returns)
    long_net = tuple(_required(value) - 0.0002 for value in futures.long_net_returns)
    labels = tuple(
        classify_kline_proxy_outcome(long_value, short_value, 0).value
        for long_value, short_value in zip(long_net, short_net, strict=True)
    )
    futures_with_funding = replace(
        futures,
        signal_funding_returns=funding,
        signal_net_returns=short_net,
        long_net_returns=long_net,
        short_net_returns=short_net,
        outcome_labels=labels,
        f60_components=(
            futures.signal_gross_returns[2],
            futures.signal_fee_returns[2],
            futures.signal_slippage_returns[2],
            funding[2],
            short_net[2],
        ),
    )
    rows[futures_index] = futures_with_funding
    assert validate_r3_integrity(rows, (), spec)["valid"] is True

    wrong_opposite_funding = replace(
        futures_with_funding,
        long_net_returns=tuple(
            _required(value) + 0.0004
            for value in futures_with_funding.long_net_returns
        ),
    )
    rows[futures_index] = wrong_opposite_funding
    with pytest.raises(ValueError, match="long counterfactual net cost parity"):
        validate_r3_integrity(rows, (), spec)


def test_csv_reader_requires_decision_reasons_and_invalidation(tmp_path: Path) -> None:
    path = tmp_path / "opportunities.csv"
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=("opportunity_id",))
        writer.writeheader()
        writer.writerow({"opportunity_id": "x"})

    with pytest.raises(ValueError, match="missing required fields"):
        read_r3_opportunities(path)


def test_writer_emits_strict_json_korean_report_and_manifest(
    spec: BacktestSpec, tmp_path: Path
) -> None:
    result = _analyze_r3_diagnostic(
        _panel(spec), (), spec, bootstrap_samples=20, bootstrap_seed=17
    )
    paths = write_r3_analysis(result, tmp_path)

    assert Path(paths["json"]).is_file()
    report = Path(paths["report"]).read_text(encoding="utf-8")
    assert report.startswith("# R3 5분봉 노출표본 진단 보고서")
    assert "MC 해상도" in report
    assert "## 15·30·60분 비용·민감도·방향 적중률" in report
    assert "## 60분 자산별 결과" in report
    assert "## 60분 split·regime·BTC trend 분해" in report
    assert "### T72 종료 사유" in report
    assert Path(paths["manifest"]).is_file()


def test_inconclusive_zero_primary_rows_render_none_safely(
    spec: BacktestSpec, tmp_path: Path
) -> None:
    rows = tuple(
        _opportunity(
            spec,
            asset_index,
            market,
            day=0,
            analysis_eligible=False,
            analysis_eligible_72=False,
        )
        for market in ("spot", "futures")
        for asset_index in range(len(spec.assets))
    )
    result = _analyze_r3_diagnostic(
        rows, (), spec, bootstrap_samples=20, bootstrap_seed=17
    )
    paths = write_r3_analysis(result, tmp_path)
    report = Path(paths["report"]).read_text(encoding="utf-8")

    assert result["status_axes"]["data_integrity"] == "PASS"
    assert result["status_axes"]["kline_proxy_efficacy"] == (
        "INCONCLUSIVE_LOW_INFORMATION"
    )
    assert "NA" in report


def test_public_analyzer_rejects_frozen_spec_drift_without_resampling(
    spec: BacktestSpec,
) -> None:
    drifted = spec.model_copy(
        update={"bootstrap": spec.bootstrap.model_copy(update={"seed": 1})}
    )

    result = analyze_r3_diagnostic(_panel(spec), (), drifted)

    assert result["status_axes"]["data_integrity"] == "FAIL"
    assert "drifts from the frozen R3 protocol" in result["integrity"]["reason"]


def test_raw_results_interpretation_contract_fails_on_tampering(tmp_path: Path) -> None:
    path = tmp_path / "results.json"
    contract = {
        "artifact_role": "RAW_REPLAY_INPUT_NOT_FINAL_R3_ANALYSIS",
        "r3_raw_replay_contract": {
            "sequential_t72_ledger": {
                "independent_episodes": False,
                "analysis_role": "SECONDARY_NON_PRIMARY",
            },
            "legacy_bootstrap": {
                "is_frozen_r3_shared_utc_day_mbb": False,
                "analysis_role": "DESCRIPTIVE_NOT_FINAL_R3_INFERENCE",
            },
            "r2_c0_t72_status": "PROTOCOL_MISMATCH",
            "final_r3_analysis": {
                "source": "SEPARATE_OPPORTUNITY_BASED_R3_ANALYZER",
                "provides_primary_efficacy": True,
                "provides_status_axes": True,
            },
        },
    }
    path.write_text(json.dumps(contract), encoding="utf-8")
    assert _validate_raw_results_contract(path)["artifact_role"].startswith("RAW_REPLAY")

    contract["r3_raw_replay_contract"]["r2_c0_t72_status"] = "PASS"
    path.write_text(json.dumps(contract), encoding="utf-8")
    with pytest.raises(ValueError, match="tampered"):
        _validate_raw_results_contract(path)


def test_replay_source_digest_must_match_analyzer_source() -> None:
    with pytest.raises(ValueError, match="source code differs"):
        _validate_source_code_digest("0" * 64, Path.cwd())


def test_frozen_csv_schema_matches_engine_dataclasses() -> None:
    from signalbot.backtest.engine import Opportunity, Trade

    assert {field.name for field in fields(Opportunity)} == _OPPORTUNITY_SCHEMA_FIELDS
    assert {field.name for field in fields(Trade)} == _TRADE_SCHEMA_FIELDS


def _ledger_fixture(tmp_path: Path) -> tuple[Path, Path, dict[str, str], str]:
    data_root = tmp_path / "data" / "backtest"
    source = data_root / "spot" / "BTC.csv.gz"
    source.parent.mkdir(parents=True)
    source.write_bytes(b"frozen-input")
    files = {
        "spot/BTC.csv.gz": hashlib.sha256(b"frozen-input").hexdigest(),
    }
    ledger = {
        "data_root": "data/backtest",
        "files": files,
        "protocol_version": "test-protocol",
    }
    ledger_path = tmp_path / "ledger.json"
    ledger_path.write_text(json.dumps(ledger, sort_keys=True), encoding="utf-8")
    ledger_sha = hashlib.sha256(ledger_path.read_bytes()).hexdigest()
    return ledger_path, data_root, files, ledger_sha


def _verify_test_ledger(
    ledger_path: Path,
    data_root: Path,
    manifest: dict[str, str],
    ledger_sha: str,
) -> dict[str, Any]:
    return _verify_input_panel_ledger(
        ledger_path,
        data_root=data_root,
        manifest_inputs=manifest,
        expected_paths=frozenset({"spot/BTC.csv.gz"}),
        expected_protocol="test-protocol",
        expected_data_root_label="data/backtest",
        expected_ledger_sha256=ledger_sha,
    )


def test_input_ledger_verifies_actual_bytes_and_rejects_tampering(tmp_path: Path) -> None:
    ledger_path, data_root, files, ledger_sha = _ledger_fixture(tmp_path)
    verified = _verify_test_ledger(ledger_path, data_root, files, ledger_sha)
    assert verified["verified_actual_input_files"] == 1

    (data_root / "spot" / "BTC.csv.gz").write_bytes(b"tampered-input")
    with pytest.raises(ValueError, match="file hash mismatch"):
        _verify_test_ledger(ledger_path, data_root, files, ledger_sha)


def test_input_ledger_rejects_missing_actual_file(tmp_path: Path) -> None:
    ledger_path, _data_root, files, ledger_sha = _ledger_fixture(tmp_path)
    missing_root = tmp_path / "missing" / "data" / "backtest"

    with pytest.raises(ValueError, match="file is missing"):
        _verify_test_ledger(ledger_path, missing_root, files, ledger_sha)


def test_manifest_and_ledger_self_consistency_cannot_hide_ledger_tamper(
    tmp_path: Path,
) -> None:
    ledger_path, data_root, files, frozen_ledger_sha = _ledger_fixture(tmp_path)
    tampered_sha = hashlib.sha256(b"coordinated-tamper").hexdigest()
    raw = json.loads(ledger_path.read_text(encoding="utf-8"))
    raw["files"]["spot/BTC.csv.gz"] = tampered_sha
    ledger_path.write_text(json.dumps(raw, sort_keys=True), encoding="utf-8")
    (data_root / "spot" / "BTC.csv.gz").write_bytes(b"coordinated-tamper")
    manifest = {**files, "spot/BTC.csv.gz": tampered_sha}

    with pytest.raises(ValueError, match="ledger hash mismatch"):
        _verify_test_ledger(
            ledger_path,
            data_root,
            manifest,
            frozen_ledger_sha,
        )
