from __future__ import annotations

from dataclasses import replace
from decimal import Decimal

import pytest

from signalbot.backtest.d1_scefb_historical_math import (
    D1HistoricalFeeCellV0,
    D1HistoricalFundingBoundaryAmbiguityV0,
    D1HistoricalMathErrorV0,
    build_d1_historical_funding_point_v0,
    calculate_d1_historical_execution_v0,
    d1_historical_entry_execution_price_v0,
    d1_historical_exit_execution_price_v0,
    project_d1_historical_pnl_v0,
)
from signalbot.r4b_v2.strategy.d1_scefb import D1SideV0


@pytest.mark.parametrize("side", [D1SideV0.LONG, D1SideV0.SHORT])
@pytest.mark.parametrize(
    ("fee_cell", "expected_net"),
    [
        (D1HistoricalFeeCellV0.PRIMARY_1_0, Decimal("-0.0026000")),
        (D1HistoricalFeeCellV0.STRESS_1_5, Decimal("-0.00310000")),
    ],
)
def test_zero_move_cost_is_exact_and_symmetric(
    side: D1SideV0,
    fee_cell: D1HistoricalFeeCellV0,
    expected_net: Decimal,
) -> None:
    result = calculate_d1_historical_execution_v0(
        side=side,
        fee_cell=fee_cell,
        entry_time_ms=1_000,
        exit_time_ms=2_000,
        entry_reference_price=Decimal("100"),
        exit_reference_price=Decimal("100"),
        funding_points=(),
    )

    assert result.gross_return == 0
    assert result.slippage_return == Decimal("0.0016000")
    assert result.net_return == expected_net
    assert not result.historical_bbo_available
    assert not result.paper_fill_claim
    assert not result.execution_conclusive
    assert not result.promoting
    assert not result.production_order_placement


def test_long_and_short_directional_returns_are_exact_mirrors() -> None:
    long = calculate_d1_historical_execution_v0(
        side=D1SideV0.LONG,
        fee_cell=D1HistoricalFeeCellV0.PRIMARY_1_0,
        entry_time_ms=1_000,
        exit_time_ms=2_000,
        entry_reference_price=Decimal("100"),
        exit_reference_price=Decimal("103"),
        funding_points=(),
    )
    short = calculate_d1_historical_execution_v0(
        side=D1SideV0.SHORT,
        fee_cell=D1HistoricalFeeCellV0.PRIMARY_1_0,
        entry_time_ms=1_000,
        exit_time_ms=2_000,
        entry_reference_price=Decimal("100"),
        exit_reference_price=Decimal("97"),
        funding_points=(),
    )

    assert long.gross_return == short.gross_return == Decimal("0.03")
    assert long.slippage_return == Decimal("0.0016240")
    assert short.slippage_return == Decimal("0.0015760")
    assert long.net_return == Decimal("0.027361012")
    assert short.net_return == Decimal("0.027439012")


@pytest.mark.parametrize(
    ("side", "entry", "exit"),
    [
        (D1SideV0.LONG, Decimal("100.08"), Decimal("99.92")),
        (D1SideV0.SHORT, Decimal("99.92"), Decimal("100.08")),
    ],
)
def test_public_adverse_price_helpers_match_frozen_direction(
    side: D1SideV0,
    entry: Decimal,
    exit: Decimal,
) -> None:
    assert d1_historical_entry_execution_price_v0(
        side=side,
        reference_price=Decimal("100"),
    ) == entry
    assert d1_historical_exit_execution_price_v0(
        side=side,
        reference_price=Decimal("100"),
    ) == exit


@pytest.mark.parametrize(
    ("side", "expected"),
    [
        (D1SideV0.LONG, Decimal("-0.00101")),
        (D1SideV0.SHORT, Decimal("0.00101")),
    ],
)
def test_strict_interior_funding_uses_public_mark_and_direction(
    side: D1SideV0,
    expected: Decimal,
) -> None:
    point = build_d1_historical_funding_point_v0(
        funding_time_ms=1_500,
        rate=Decimal("0.001"),
        mark_price=Decimal("101"),
    )
    result = calculate_d1_historical_execution_v0(
        side=side,
        fee_cell=D1HistoricalFeeCellV0.PRIMARY_1_0,
        entry_time_ms=1_000,
        exit_time_ms=2_000,
        entry_reference_price=Decimal("100"),
        exit_reference_price=Decimal("100"),
        funding_points=(point,),
    )

    assert result.funding_return == expected
    assert result.funding_event_count == 1


@pytest.mark.parametrize("timestamp", [1_000, 2_000])
def test_endpoint_equal_funding_is_inconclusive(timestamp: int) -> None:
    point = build_d1_historical_funding_point_v0(
        funding_time_ms=timestamp,
        rate=Decimal("0.001"),
        mark_price=Decimal("100"),
    )

    with pytest.raises(
        D1HistoricalFundingBoundaryAmbiguityV0,
        match="equals entry or exit",
    ):
        calculate_d1_historical_execution_v0(
            side=D1SideV0.LONG,
            fee_cell=D1HistoricalFeeCellV0.PRIMARY_1_0,
            entry_time_ms=1_000,
            exit_time_ms=2_000,
            entry_reference_price=Decimal("100"),
            exit_reference_price=Decimal("100"),
            funding_points=(point,),
        )


def test_missing_strict_interior_mark_never_falls_back_to_entry() -> None:
    point = build_d1_historical_funding_point_v0(
        funding_time_ms=1_500,
        rate=Decimal("0.001"),
        mark_price=None,
    )

    with pytest.raises(D1HistoricalMathErrorV0, match="mark price is missing"):
        calculate_d1_historical_execution_v0(
            side=D1SideV0.LONG,
            fee_cell=D1HistoricalFeeCellV0.PRIMARY_1_0,
            entry_time_ms=1_000,
            exit_time_ms=2_000,
            entry_reference_price=Decimal("100"),
            exit_reference_price=Decimal("100"),
            funding_points=(point,),
        )


def test_outside_interval_missing_mark_is_irrelevant_but_order_must_be_strict() -> None:
    before = build_d1_historical_funding_point_v0(
        funding_time_ms=500,
        rate=Decimal("0.001"),
        mark_price=None,
    )
    after = build_d1_historical_funding_point_v0(
        funding_time_ms=2_500,
        rate=Decimal("0.001"),
        mark_price=None,
    )
    result = calculate_d1_historical_execution_v0(
        side=D1SideV0.LONG,
        fee_cell=D1HistoricalFeeCellV0.PRIMARY_1_0,
        entry_time_ms=1_000,
        exit_time_ms=2_000,
        entry_reference_price=Decimal("100"),
        exit_reference_price=Decimal("100"),
        funding_points=(before, after),
    )
    assert result.funding_return == 0
    assert result.funding_event_count == 0

    with pytest.raises(D1HistoricalMathErrorV0, match="strictly ordered"):
        calculate_d1_historical_execution_v0(
            side=D1SideV0.LONG,
            fee_cell=D1HistoricalFeeCellV0.PRIMARY_1_0,
            entry_time_ms=1_000,
            exit_time_ms=2_000,
            entry_reference_price=Decimal("100"),
            exit_reference_price=Decimal("100"),
            funding_points=(after, before),
        )


def test_projection_does_not_change_statistical_return() -> None:
    result = calculate_d1_historical_execution_v0(
        side=D1SideV0.LONG,
        fee_cell=D1HistoricalFeeCellV0.PRIMARY_1_0,
        entry_time_ms=1_000,
        exit_time_ms=2_000,
        entry_reference_price=Decimal("100"),
        exit_reference_price=Decimal("100"),
        funding_points=(),
    )

    assert project_d1_historical_pnl_v0(
        result,
        notional_usdt=Decimal("100"),
    ) == Decimal("-0.2600000")
    assert project_d1_historical_pnl_v0(
        result,
        notional_usdt=Decimal("1000"),
    ) == Decimal("-2.6000000")


def test_factory_seals_and_input_validation_fail_closed() -> None:
    with pytest.raises(D1HistoricalMathErrorV0, match="factory-created"):
        build = build_d1_historical_funding_point_v0(
            funding_time_ms=1_500,
            rate=Decimal("0.001"),
            mark_price=Decimal("100"),
        )
        replace(build, rate=Decimal("0.002"))

    duplicate = build_d1_historical_funding_point_v0(
        funding_time_ms=1_500,
        rate=Decimal("0.001"),
        mark_price=Decimal("100"),
    )
    with pytest.raises(D1HistoricalMathErrorV0, match="strictly ordered"):
        calculate_d1_historical_execution_v0(
            side=D1SideV0.LONG,
            fee_cell=D1HistoricalFeeCellV0.PRIMARY_1_0,
            entry_time_ms=1_000,
            exit_time_ms=2_000,
            entry_reference_price=Decimal("100"),
            exit_reference_price=Decimal("100"),
            funding_points=(duplicate, duplicate),
        )
