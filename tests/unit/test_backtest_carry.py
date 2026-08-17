from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path

import pytest
from pydantic import ValidationError

from signalbot.backtest.carry import (
    CarryExitReason,
    CarryExperimentSpec,
    CarryPolicySettings,
    load_carry_spec,
    run_carry_pair,
)
from signalbot.backtest.config import BacktestAsset, BacktestSplit, CostSettings
from signalbot.backtest.dataset import KlineDataset, KlineDatasetRequest
from signalbot.backtest.engine import FundingRate
from signalbot.backtest.funding import FundingDataset
from signalbot.domain.enums import Market
from signalbot.domain.models import Candle

_STEP_MS = 300_000
_DAY_MS = 86_400_000
_DECISION_INDEX = 8_640
_CANDLE_COUNT = _DECISION_INDEX + 8


@dataclass(frozen=True)
class _SyntheticPanel:
    spot: KlineDataset
    futures: KlineDataset
    funding: FundingDataset
    asset: BacktestAsset
    costs: CostSettings


def _candle(market: Market, symbol: str, index: int, price: Decimal) -> Candle:
    open_time_ms = index * _STEP_MS
    return Candle(
        market=market,
        symbol=symbol,
        interval="5m",
        open_time_ms=open_time_ms,
        close_time_ms=open_time_ms + _STEP_MS - 1,
        open=price,
        high=price,
        low=price,
        close=price,
        volume=Decimal("100"),
        quote_volume=Decimal("10000"),
        trade_count=10,
        taker_buy_base_volume=Decimal("50"),
        taker_buy_quote_volume=Decimal("5000"),
        is_closed=True,
    )


def _dataset(market: Market, symbol: str, candles: tuple[Candle, ...]) -> KlineDataset:
    if not candles:
        raise ValueError("synthetic dataset requires candles")
    return KlineDataset(
        KlineDatasetRequest(
            market=market,
            symbol=symbol,
            interval="5m",
            start_time_ms=0,
            end_time_ms=candles[-1].close_time_ms,
            alias="BTC",
        ),
        candles,
    )


@pytest.fixture(scope="module")
def panel() -> _SyntheticPanel:
    spot_candles = tuple(
        _candle(Market.SPOT, "BTCUSDT", index, Decimal("100"))
        for index in range(_CANDLE_COUNT)
    )
    futures_candles = tuple(
        _candle(Market.FUTURES, "BTCUSDT", index, Decimal("101"))
        for index in range(_CANDLE_COUNT)
    )
    end_time_ms = (_CANDLE_COUNT * _STEP_MS) - 1
    funding = tuple(
        FundingRate(timestamp, 0.001, 101.0)
        for timestamp in range(8 * 3_600_000, 30 * _DAY_MS + 1, 8 * 3_600_000)
    )
    return _SyntheticPanel(
        spot=_dataset(Market.SPOT, "BTCUSDT", spot_candles),
        futures=_dataset(Market.FUTURES, "BTCUSDT", futures_candles),
        funding=FundingDataset("BTCUSDT", 0, end_time_ms, funding),
        asset=BacktestAsset(
            asset="BTC",
            cohort="anchor",
            spot_symbol="BTCUSDT",
            futures_symbol="BTCUSDT",
        ),
        costs=CostSettings(),
    )


def _replace_futures_prices(
    dataset: KlineDataset, prices: dict[int, Decimal]
) -> KlineDataset:
    candles = []
    for index, candle in enumerate(dataset.candles):
        price = prices.get(index)
        candles.append(
            candle
            if price is None
            else candle.model_copy(
                update={"open": price, "high": price, "low": price, "close": price}
            )
        )
    return KlineDataset(dataset.request, tuple(candles))


def _split_for_entry(entry_time_ms: int, *, upper_days: int = 14) -> BacktestSplit:
    return BacktestSplit(
        name="validation",
        start=datetime.fromtimestamp((entry_time_ms - 7 * _DAY_MS) / 1000, UTC),
        end=datetime.fromtimestamp((entry_time_ms + upper_days * _DAY_MS) / 1000, UTC),
    )


def _twelve_hour_funding(
    panel: _SyntheticPanel, rates: tuple[float, ...]
) -> FundingDataset:
    timestamps = tuple(
        range(12 * 3_600_000, 30 * _DAY_MS + 1, 12 * 3_600_000)
    )
    if len(rates) != len(timestamps):
        raise ValueError("synthetic twelve-hour funding requires exactly 60 rates")
    return FundingDataset(
        panel.funding.symbol,
        panel.funding.start_time_ms,
        panel.funding.end_time_ms,
        tuple(
            FundingRate(timestamp, rate, 101.0)
            for timestamp, rate in zip(timestamps, rates, strict=True)
        ),
    )


def _run(
    panel: _SyntheticPanel,
    *,
    futures: KlineDataset,
    funding: FundingDataset | None = None,
    spot: KlineDataset | None = None,
    split: BacktestSplit | None = None,
):
    entry_time_ms = (_DECISION_INDEX + 1) * _STEP_MS
    return run_carry_pair(
        protocol_version="c1-test",
        rule_version="c1-test-rule",
        asset=panel.asset,
        split=split or _split_for_entry(entry_time_ms),
        spot=spot or panel.spot,
        futures=futures,
        funding=funding or panel.funding,
        costs=panel.costs,
        policy=CarryPolicySettings(),
    )


def test_positive_pair_uses_equal_quantity_next_opens_and_frozen_convergence(
    panel: _SyntheticPanel,
) -> None:
    futures = _replace_futures_prices(
        panel.futures,
        {
            _DECISION_INDEX: Decimal("103"),
            _DECISION_INDEX + 1: Decimal("102.5"),
            _DECISION_INDEX + 2: Decimal("100.9"),
            _DECISION_INDEX + 3: Decimal("100.9"),
        },
    )
    entry_time_ms = (_DECISION_INDEX + 1) * _STEP_MS
    # This post-fill negative event is inside the excluded first five minutes.
    # It must neither flip the position nor enter funding P&L.
    augmented_rates = tuple(
        sorted(
            (*panel.funding.rates, FundingRate(entry_time_ms + 120_000, -0.01, 102.0)),
            key=lambda item: item.funding_time_ms,
        )
    )
    funding = FundingDataset(
        panel.funding.symbol,
        panel.funding.start_time_ms,
        panel.funding.end_time_ms,
        augmented_rates,
    )

    result = _run(panel, futures=futures, funding=funding)
    accepted = [item for item in result.decisions if item.accepted]

    assert len(accepted) == 1
    decision = accepted[0]
    assert decision.entry_time_ms == entry_time_ms
    assert decision.triggering_funding_time_ms == 30 * _DAY_MS
    assert decision.basis_median == pytest.approx(0.01)
    assert decision.basis_q90 == pytest.approx(0.01)
    assert decision.basis_mad == pytest.approx(0.0)
    assert decision.target_basis == pytest.approx(0.01)
    assert decision.stop_basis == pytest.approx(0.035)
    assert decision.expected_funding_events == 20
    assert decision.expected_pair_edge is not None
    assert decision.stress_cost_hurdle is not None
    assert decision.expected_pair_edge > decision.stress_cost_hurdle + 0.001

    assert len(result.trades) == 1
    trade = result.trades[0]
    assert trade.exit_reason == CarryExitReason.CONVERGENCE
    assert trade.entry_time_ms == entry_time_ms
    assert trade.exit_time_ms == (_DECISION_INDEX + 3) * _STEP_MS
    assert trade.base_quantity == pytest.approx(100 / (100 + 102.5))
    assert trade.pair_capital_usdt == pytest.approx(100)
    assert trade.gross_pnl_usdt == pytest.approx(trade.base_quantity * (102.5 - 100.9))
    assert trade.funding_event_count == 0
    assert trade.funding_pnl_usdt == pytest.approx(0)
    assert trade.net_pnl_usdt == pytest.approx(
        trade.gross_pnl_usdt - trade.slippage_usdt - trade.fees_usdt
    )
    assert trade.analysis_eligible is True


def test_nonpersistent_funding_rejects_entry(panel: _SyntheticPanel) -> None:
    futures = _replace_futures_prices(
        panel.futures, {_DECISION_INDEX: Decimal("103")}
    )
    rates = tuple(
        FundingRate(
            item.funding_time_ms,
            0.001 if index % 2 or item.funding_time_ms == 30 * _DAY_MS else -0.001,
            item.mark_price,
        )
        for index, item in enumerate(panel.funding.rates)
    )
    funding = FundingDataset(
        panel.funding.symbol,
        panel.funding.start_time_ms,
        panel.funding.end_time_ms,
        rates,
    )

    result = _run(panel, futures=futures, funding=funding)
    decision = next(
        item
        for item in result.decisions
        if item.triggering_funding_time_ms == 30 * _DAY_MS
    )

    assert decision.accepted is False
    assert "funding_q25_not_positive" in decision.rejection_reasons
    assert "positive_funding_fraction_below_75pct" in decision.rejection_reasons
    assert result.trades == ()


def test_basis_equal_to_prior_q90_passes_the_inclusive_gate(
    panel: _SyntheticPanel,
) -> None:
    result = _run(panel, futures=panel.futures)
    decision = next(
        item
        for item in result.decisions
        if item.triggering_funding_time_ms == 30 * _DAY_MS
    )

    assert decision.basis == decision.basis_q90
    assert decision.accepted is True
    assert "basis_below_prior_q90" not in decision.rejection_reasons


def test_positive_funding_fraction_equal_to_75_percent_passes(
    panel: _SyntheticPanel,
) -> None:
    funding = _twelve_hour_funding(panel, (0.0,) * 15 + (0.001,) * 45)

    result = _run(panel, futures=panel.futures, funding=funding)
    decision = next(
        item
        for item in result.decisions
        if item.triggering_funding_time_ms == 30 * _DAY_MS
    )

    assert decision.positive_funding_fraction == 0.75
    assert decision.funding_q25 == pytest.approx(0.00075)
    assert decision.accepted is True
    assert "positive_funding_fraction_below_75pct" not in decision.rejection_reasons


def test_funding_q25_equal_to_zero_rejects(panel: _SyntheticPanel) -> None:
    funding = _twelve_hour_funding(panel, (-0.003,) * 15 + (0.001,) * 45)
    futures = _replace_futures_prices(
        panel.futures, {_DECISION_INDEX: Decimal("103")}
    )

    result = _run(panel, futures=futures, funding=funding)
    decision = next(
        item
        for item in result.decisions
        if item.triggering_funding_time_ms == 30 * _DAY_MS
    )

    assert decision.positive_funding_fraction == 0.75
    assert decision.funding_q25 == 0.0
    assert decision.accepted is False
    assert "funding_q25_not_positive" in decision.rejection_reasons
    assert "positive_funding_fraction_below_75pct" not in decision.rejection_reasons


def test_expected_edge_equal_to_stress_plus_10bp_rejects(
    panel: _SyntheticPanel,
) -> None:
    basis = (101.0 - 100.0) / 100.0
    pair_denominator = 2 + basis
    stress_hurdle = 2 * (
        (0.001 + 2 * 0.0005) + (1 + basis) * (0.0005 + 2 * 0.0003)
    ) / pair_denominator
    boundary_rate = (
        (stress_hurdle + 0.001) * pair_denominator / (20 * (1 + basis))
    )
    funding = FundingDataset(
        panel.funding.symbol,
        panel.funding.start_time_ms,
        panel.funding.end_time_ms,
        tuple(
            FundingRate(item.funding_time_ms, boundary_rate, item.mark_price)
            for item in panel.funding.rates
        ),
    )

    result = _run(panel, futures=panel.futures, funding=funding)
    decision = next(
        item
        for item in result.decisions
        if item.triggering_funding_time_ms == 30 * _DAY_MS
    )

    assert decision.expected_pair_edge is not None
    assert decision.stress_cost_hurdle is not None
    assert decision.expected_pair_edge == decision.stress_cost_hurdle + 0.001
    assert decision.accepted is False
    assert "expected_edge_not_above_stress_plus_10bp" in decision.rejection_reasons


def test_funding_timestamp_equal_to_close_waits_for_next_closed_bar(
    panel: _SyntheticPanel,
) -> None:
    equal_timestamp = (_DECISION_INDEX + 1) * _STEP_MS - 1
    rates = (
        *(
            item
            for item in panel.funding.rates
            if item.funding_time_ms != 30 * _DAY_MS
        ),
        FundingRate(equal_timestamp, 0.001, 101.0),
    )
    funding = FundingDataset(
        panel.funding.symbol,
        panel.funding.start_time_ms,
        panel.funding.end_time_ms,
        tuple(sorted(rates, key=lambda item: item.funding_time_ms)),
    )
    futures = _replace_futures_prices(
        panel.futures,
        {
            _DECISION_INDEX + 1: Decimal("103"),
            _DECISION_INDEX + 2: Decimal("102.5"),
            _DECISION_INDEX + 3: Decimal("100.9"),
            _DECISION_INDEX + 4: Decimal("100.9"),
        },
    )
    shifted_entry_time_ms = (_DECISION_INDEX + 2) * _STEP_MS

    result = _run(
        panel,
        futures=futures,
        funding=funding,
        split=_split_for_entry(shifted_entry_time_ms),
    )
    decision = next(
        item for item in result.decisions if item.triggering_funding_time_ms == equal_timestamp
    )

    assert decision.accepted is True
    assert decision.decision_time_ms == (_DECISION_INDEX + 2) * _STEP_MS - 1
    assert decision.entry_time_ms == shifted_entry_time_ms


def test_gap_cancels_next_common_open_entry(panel: _SyntheticPanel) -> None:
    futures = _replace_futures_prices(
        panel.futures, {_DECISION_INDEX: Decimal("103")}
    )
    spot_without_next_bar = KlineDataset(
        panel.spot.request,
        tuple(
            candle
            for index, candle in enumerate(panel.spot.candles)
            if index != _DECISION_INDEX + 1
        ),
    )

    result = _run(panel, futures=futures, spot=spot_without_next_bar)
    decision = next(
        item
        for item in result.decisions
        if item.triggering_funding_time_ms == 30 * _DAY_MS
    )

    assert decision.accepted is False
    assert "next_common_open_gap" in decision.rejection_reasons
    assert result.gap_count == 1
    assert result.trades == ()


def test_first_post_gap_funding_trigger_is_recorded_as_an_abstention(
    panel: _SyntheticPanel,
) -> None:
    spot_with_pretrigger_gap = KlineDataset(
        panel.spot.request,
        tuple(
            candle
            for index, candle in enumerate(panel.spot.candles)
            if index != _DECISION_INDEX - 1
        ),
    )

    result = _run(
        panel,
        futures=panel.futures,
        spot=spot_with_pretrigger_gap,
    )
    decision = next(
        item
        for item in result.decisions
        if item.triggering_funding_time_ms == 30 * _DAY_MS
    )

    assert decision.accepted is False
    assert "insufficient_contiguous_basis_history" in decision.rejection_reasons
    assert "next_common_open_gap" not in decision.rejection_reasons
    assert result.trades == ()


def test_split_boundaries_are_lower_inclusive_and_upper_exclusive(
    panel: _SyntheticPanel,
) -> None:
    futures = _replace_futures_prices(
        panel.futures,
        {
            _DECISION_INDEX: Decimal("103"),
            _DECISION_INDEX + 1: Decimal("102.5"),
            _DECISION_INDEX + 2: Decimal("100.9"),
            _DECISION_INDEX + 3: Decimal("100.9"),
        },
    )
    entry_time_ms = (_DECISION_INDEX + 1) * _STEP_MS
    lower_exact = _run(
        panel,
        futures=futures,
        split=_split_for_entry(entry_time_ms),
    )
    assert any(item.accepted for item in lower_exact.decisions)

    upper_equal_split = _split_for_entry(entry_time_ms, upper_days=7)
    upper_equal = _run(panel, futures=futures, split=upper_equal_split)
    decision = next(
        item
        for item in upper_equal.decisions
        if item.triggering_funding_time_ms == 30 * _DAY_MS
    )
    assert decision.accepted is False
    assert "split_end_purge" in decision.rejection_reasons


def test_stop_has_priority_over_new_nonpositive_funding(panel: _SyntheticPanel) -> None:
    futures = _replace_futures_prices(
        panel.futures,
        {
            _DECISION_INDEX: Decimal("103"),
            _DECISION_INDEX + 1: Decimal("102.5"),
            _DECISION_INDEX + 2: Decimal("103.5"),
            _DECISION_INDEX + 3: Decimal("103.5"),
        },
    )
    entry_time_ms = (_DECISION_INDEX + 1) * _STEP_MS
    rates = tuple(
        sorted(
            (*panel.funding.rates, FundingRate(entry_time_ms + _STEP_MS, -0.001, 103.5)),
            key=lambda item: item.funding_time_ms,
        )
    )
    funding = FundingDataset(
        panel.funding.symbol,
        panel.funding.start_time_ms,
        panel.funding.end_time_ms,
        rates,
    )

    result = _run(panel, futures=futures, funding=funding)

    assert len(result.trades) == 1
    assert result.trades[0].exit_reason == CarryExitReason.STOP
    assert result.trades[0].exit_signal_basis == pytest.approx(0.035)
    assert result.trades[0].funding_event_count == 1
    assert result.trades[0].funding_pnl_usdt < 0


def test_nonpositive_funding_exits_when_stop_and_target_do_not_fire(
    panel: _SyntheticPanel,
) -> None:
    futures = _replace_futures_prices(
        panel.futures,
        {
            _DECISION_INDEX: Decimal("103"),
            _DECISION_INDEX + 1: Decimal("102.5"),
            _DECISION_INDEX + 2: Decimal("102.5"),
            _DECISION_INDEX + 3: Decimal("102.5"),
        },
    )
    entry_time_ms = (_DECISION_INDEX + 1) * _STEP_MS
    rates = tuple(
        sorted(
            (*panel.funding.rates, FundingRate(entry_time_ms + _STEP_MS, 0.0, 102.5)),
            key=lambda item: item.funding_time_ms,
        )
    )
    funding = FundingDataset(
        panel.funding.symbol,
        panel.funding.start_time_ms,
        panel.funding.end_time_ms,
        rates,
    )

    result = _run(panel, futures=futures, funding=funding)

    assert len(result.trades) == 1
    assert result.trades[0].exit_reason == CarryExitReason.FUNDING_FLIP
    assert result.trades[0].funding_pnl_usdt == pytest.approx(0)


def test_funding_pnl_excludes_event_equal_to_exit_decision_time(
    panel: _SyntheticPanel,
) -> None:
    futures = _replace_futures_prices(
        panel.futures,
        {
            _DECISION_INDEX: Decimal("103"),
            _DECISION_INDEX + 1: Decimal("102.5"),
            _DECISION_INDEX + 2: Decimal("101"),
            _DECISION_INDEX + 3: Decimal("101"),
        },
    )
    exit_decision_time_ms = (_DECISION_INDEX + 3) * _STEP_MS - 1
    rates = tuple(
        sorted(
            (
                *panel.funding.rates,
                FundingRate(exit_decision_time_ms - 1, 0.002, 120.0),
                FundingRate(exit_decision_time_ms, 0.003, 120.0),
            ),
            key=lambda item: item.funding_time_ms,
        )
    )
    funding = FundingDataset(
        panel.funding.symbol,
        panel.funding.start_time_ms,
        panel.funding.end_time_ms,
        rates,
    )

    result = _run(panel, futures=futures, funding=funding)
    trade = result.trades[0]

    assert trade.exit_reason == CarryExitReason.CONVERGENCE
    assert trade.exit_decision_time_ms == exit_decision_time_ms
    assert trade.funding_event_count == 1
    assert trade.funding_pnl_usdt == pytest.approx(
        trade.base_quantity * 120.0 * 0.002
    )


def test_terminal_common_gap_forces_ineligible_close(panel: _SyntheticPanel) -> None:
    prices = {_DECISION_INDEX: Decimal("103")}
    prices.update(
        {
            index: Decimal("102.5")
            for index in range(_DECISION_INDEX + 1, _CANDLE_COUNT)
        }
    )
    futures = _replace_futures_prices(panel.futures, prices)
    missing_index = _CANDLE_COUNT - 2
    spot = KlineDataset(
        panel.spot.request,
        tuple(
            candle
            for index, candle in enumerate(panel.spot.candles)
            if index != missing_index
        ),
    )

    result = _run(panel, futures=futures, spot=spot)

    assert result.gap_count == 1
    assert result.open_position_at_end is False
    assert len(result.trades) == 1
    assert result.trades[0].exit_reason == CarryExitReason.DATA_GAP
    assert result.trades[0].analysis_eligible is False
    assert result.trades[0].exclusion_reason == "terminal_common_5m_gap_while_open"
    assert result.trades[0].exit_time_ms == (_CANDLE_COUNT - 1) * _STEP_MS


def test_open_at_end_is_explicit_outcome_unobservable_state(
    panel: _SyntheticPanel,
) -> None:
    prices = {_DECISION_INDEX: Decimal("103")}
    prices.update(
        {
            index: Decimal("102.5")
            for index in range(_DECISION_INDEX + 1, _CANDLE_COUNT)
        }
    )
    futures = _replace_futures_prices(panel.futures, prices)

    result = _run(panel, futures=futures)

    assert len([item for item in result.decisions if item.accepted]) == 1
    assert result.trades == ()
    assert result.open_position_at_end is True


def test_time_exit_and_24_hour_cooldown_boundaries() -> None:
    final_index = 38 * 288 + 1
    spot_candles = tuple(
        _candle(Market.SPOT, "BTCUSDT", index, Decimal("100"))
        for index in range(final_index + 1)
    )
    futures_candles = []
    cooldown_boundary_index = 38 * 288
    for index in range(final_index + 1):
        if index < _DECISION_INDEX:
            price = Decimal("101")
        elif index in {_DECISION_INDEX, cooldown_boundary_index}:
            price = Decimal("103")
        else:
            price = Decimal("102.5")
        futures_candles.append(_candle(Market.FUTURES, "BTCUSDT", index, price))
    end_time_ms = spot_candles[-1].close_time_ms
    funding_rates = tuple(
        FundingRate(timestamp, 0.001, 102.5)
        for timestamp in range(8 * 3_600_000, 38 * _DAY_MS + 1, 8 * 3_600_000)
    )
    asset = BacktestAsset(
        asset="BTC",
        cohort="anchor",
        spot_symbol="BTCUSDT",
        futures_symbol="BTCUSDT",
    )
    first_entry_time_ms = (_DECISION_INDEX + 1) * _STEP_MS

    result = run_carry_pair(
        protocol_version="c1-test",
        rule_version="c1-test-rule",
        asset=asset,
        split=_split_for_entry(first_entry_time_ms, upper_days=20),
        spot=_dataset(Market.SPOT, "BTCUSDT", spot_candles),
        futures=_dataset(Market.FUTURES, "BTCUSDT", tuple(futures_candles)),
        funding=FundingDataset("BTCUSDT", 0, end_time_ms, funding_rates),
        costs=CostSettings(),
        policy=CarryPolicySettings(),
    )

    assert len(result.trades) == 1
    first_trade = result.trades[0]
    assert first_trade.exit_reason == CarryExitReason.TIME
    assert first_trade.exit_time_ms == first_entry_time_ms + 7 * _DAY_MS

    within_cooldown = next(
        item
        for item in result.decisions
        if item.triggering_funding_time_ms == 37 * _DAY_MS + 16 * 3_600_000
    )
    assert within_cooldown.accepted is False
    assert "post_exit_cooldown" in within_cooldown.rejection_reasons

    at_cooldown_boundary = next(
        item
        for item in result.decisions
        if item.triggering_funding_time_ms == 38 * _DAY_MS
    )
    assert at_cooldown_boundary.entry_time_ms == first_trade.exit_time_ms + _DAY_MS
    assert at_cooldown_boundary.accepted is True
    assert result.open_position_at_end is True


def test_config_loads_and_frozen_policy_rejects_parameter_drift() -> None:
    root = Path(__file__).resolve().parents[2]
    spec = load_carry_spec(root / "config/backtest.5m.c1-funding-basis-carry.yaml")

    assert spec.protocol_version == "c1_exposed_funding_basis_carry_v1"
    assert spec.carry.basis_lookback_bars == 8_640
    assert spec.acceptance.bootstrap_samples == 50_000
    assert spec.acceptance.required_positive_splits == (
        "validation",
        "retrospective_test",
    )
    with pytest.raises(ValidationError, match="basis_lookback_bars is frozen"):
        CarryPolicySettings(basis_lookback_bars=8_639)

    changed_costs = spec.model_dump()
    changed_costs["costs"]["spot_fee_bps"] = 9.0
    with pytest.raises(ValidationError, match="exact frozen full-pair cost"):
        CarryExperimentSpec.model_validate(changed_costs)

    duplicate_asset = spec.model_dump()
    duplicate_asset["assets"][1] = duplicate_asset["assets"][0]
    with pytest.raises(ValidationError, match="exact ordered eight-asset"):
        CarryExperimentSpec.model_validate(duplicate_asset)

    changed_date = spec.model_dump()
    changed_date["evaluation_start"] = datetime(2024, 7, 2, tzinfo=UTC)
    with pytest.raises(ValidationError, match="data and evaluation dates are frozen"):
        CarryExperimentSpec.model_validate(changed_date)


def test_direct_run_rejects_cost_contract_drift(panel: _SyntheticPanel) -> None:
    with pytest.raises(ValueError, match="exact frozen full-pair cost"):
        run_carry_pair(
            protocol_version="c1-test",
            rule_version="c1-test-rule",
            asset=panel.asset,
            split=_split_for_entry((_DECISION_INDEX + 1) * _STEP_MS),
            spot=panel.spot,
            futures=panel.futures,
            funding=panel.funding,
            costs=panel.costs.model_copy(update={"include_funding": False}),
            policy=CarryPolicySettings(),
        )
