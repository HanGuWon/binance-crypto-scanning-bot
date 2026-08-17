"""Exact outcome-blind execution math for the D1 historical proxy.

The historical runner owns candle and funding-file authentication.  This
module owns only the frozen Decimal arithmetic from D1 amendment A0.  It does
not read files, inspect outcomes, place orders, or promote a historical proxy
to PAPER/BBO evidence.
"""

from __future__ import annotations

from dataclasses import InitVar, dataclass, field
from decimal import Decimal, localcontext
from enum import StrEnum
from itertools import pairwise
from typing import Final

from signalbot.r4b_v2.protocol.decimal_context import protocol_decimal_context_v2
from signalbot.r4b_v2.strategy.d1_scefb import D1SideV0

D1_HISTORICAL_EXECUTION_MATH_VERSION_V0: Final = (
    "D1_SCEFB_5M_HISTORICAL_EXECUTION_MATH_A0"
)
D1_HISTORICAL_SLIPPAGE_RATE_PER_SIDE_V0: Final = Decimal("0.0008")

_PRIMARY_FEE_RATE: Final = Decimal("0.0005")
_STRESS_FEE_RATE: Final = Decimal("0.00075")
_FUNDING_POINT_FACTORY_TOKEN: Final = object()
_EXECUTION_FACTORY_TOKEN: Final = object()


class D1HistoricalMathErrorV0(ValueError):
    """Raised when a historical execution input violates amendment A0."""


class D1HistoricalFundingBoundaryAmbiguityV0(D1HistoricalMathErrorV0):
    """Raised when funding and entry/exit share an unresolved timestamp."""


class D1HistoricalFeeCellV0(StrEnum):
    PRIMARY_1_0 = "FEE_1_0"
    STRESS_1_5 = "FEE_1_5"

    @property
    def rate_per_side(self) -> Decimal:
        if self is D1HistoricalFeeCellV0.PRIMARY_1_0:
            return _PRIMARY_FEE_RATE
        return _STRESS_FEE_RATE


@dataclass(frozen=True, slots=True)
class D1HistoricalFundingPointV0:
    """One exact public funding row; ``None`` mark is retained, never filled."""

    funding_time_ms: int
    rate: Decimal
    mark_price: Decimal | None
    _factory_token: InitVar[object | None] = None

    def __post_init__(self, _factory_token: object | None) -> None:
        if _factory_token is not _FUNDING_POINT_FACTORY_TOKEN:
            raise D1HistoricalMathErrorV0("funding points must be factory-created")
        _nonnegative_int(self.funding_time_ms, "funding_time_ms")
        _finite_decimal(self.rate, "rate")
        if self.mark_price is not None:
            _positive_decimal(self.mark_price, "mark_price")


@dataclass(frozen=True, slots=True)
class D1HistoricalExecutionV0:
    """One exact return decomposition for one fee cell and statistical unit."""

    side: D1SideV0
    fee_cell: D1HistoricalFeeCellV0
    entry_time_ms: int
    exit_time_ms: int
    entry_reference_price: Decimal
    exit_reference_price: Decimal
    entry_execution_price: Decimal
    exit_execution_price: Decimal
    gross_return: Decimal
    execution_return_before_fee_and_funding: Decimal
    slippage_return: Decimal
    fee_return: Decimal
    funding_return: Decimal
    net_return: Decimal
    funding_event_count: int
    _factory_token: InitVar[object | None] = None
    rule_version: str = field(
        init=False,
        default=D1_HISTORICAL_EXECUTION_MATH_VERSION_V0,
    )
    historical_bbo_available: bool = field(init=False, default=False)
    paper_fill_claim: bool = field(init=False, default=False)
    execution_conclusive: bool = field(init=False, default=False)
    promoting: bool = field(init=False, default=False)
    production_order_placement: bool = field(init=False, default=False)

    def __post_init__(self, _factory_token: object | None) -> None:
        if _factory_token is not _EXECUTION_FACTORY_TOKEN:
            raise D1HistoricalMathErrorV0("execution rows must be calculator-created")
        if not isinstance(self.side, D1SideV0):
            raise D1HistoricalMathErrorV0("side must be LONG or SHORT")
        if not isinstance(self.fee_cell, D1HistoricalFeeCellV0):
            raise D1HistoricalMathErrorV0("fee_cell is unsupported")
        _nonnegative_int(self.entry_time_ms, "entry_time_ms")
        _nonnegative_int(self.exit_time_ms, "exit_time_ms")
        if self.exit_time_ms <= self.entry_time_ms:
            raise D1HistoricalMathErrorV0("exit_time_ms must follow entry_time_ms")
        for value, label in (
            (self.entry_reference_price, "entry_reference_price"),
            (self.exit_reference_price, "exit_reference_price"),
            (self.entry_execution_price, "entry_execution_price"),
            (self.exit_execution_price, "exit_execution_price"),
        ):
            _positive_decimal(value, label)
        for value, label in (
            (self.gross_return, "gross_return"),
            (
                self.execution_return_before_fee_and_funding,
                "execution_return_before_fee_and_funding",
            ),
            (self.slippage_return, "slippage_return"),
            (self.fee_return, "fee_return"),
            (self.funding_return, "funding_return"),
            (self.net_return, "net_return"),
        ):
            _finite_decimal(value, label)
        if self.slippage_return < 0 or self.fee_return < 0:
            raise D1HistoricalMathErrorV0("cost components must be nonnegative")
        _nonnegative_int(self.funding_event_count, "funding_event_count")
        sign = Decimal(1) if self.side is D1SideV0.LONG else Decimal(-1)
        with localcontext(protocol_decimal_context_v2()):
            expected_entry_execution = self.entry_reference_price * (
                Decimal(1) + sign * D1_HISTORICAL_SLIPPAGE_RATE_PER_SIDE_V0
            )
            expected_exit_execution = self.exit_reference_price * (
                Decimal(1) - sign * D1_HISTORICAL_SLIPPAGE_RATE_PER_SIDE_V0
            )
            expected_gross = sign * (
                self.exit_reference_price - self.entry_reference_price
            ) / self.entry_reference_price
            expected_execution = sign * (
                self.exit_execution_price - self.entry_execution_price
            ) / self.entry_reference_price
            expected_slippage = expected_gross - expected_execution
            expected_fee = self.fee_cell.rate_per_side * (
                self.entry_execution_price + self.exit_execution_price
            ) / self.entry_reference_price
            expected_net = expected_execution - expected_fee + self.funding_return
        if (
            self.entry_execution_price != expected_entry_execution
            or self.exit_execution_price != expected_exit_execution
            or self.gross_return != expected_gross
            or self.execution_return_before_fee_and_funding != expected_execution
            or self.slippage_return != expected_slippage
            or self.fee_return != expected_fee
            or self.net_return != expected_net
        ):
            raise D1HistoricalMathErrorV0(
                "execution decomposition differs from amendment A0"
            )


def build_d1_historical_funding_point_v0(
    *,
    funding_time_ms: int,
    rate: Decimal,
    mark_price: Decimal | None,
) -> D1HistoricalFundingPointV0:
    """Build one exact funding input without supplying a missing mark fallback."""

    return D1HistoricalFundingPointV0(
        funding_time_ms=funding_time_ms,
        rate=rate,
        mark_price=mark_price,
        _factory_token=_FUNDING_POINT_FACTORY_TOKEN,
    )


def d1_historical_entry_execution_price_v0(
    *,
    side: D1SideV0,
    reference_price: Decimal,
) -> Decimal:
    """Return the frozen adverse entry price used as D1 threshold anchor ``E``."""

    sign = _side_sign(side)
    _positive_decimal(reference_price, "reference_price")
    with localcontext(protocol_decimal_context_v2()):
        return reference_price * (
            Decimal(1) + sign * D1_HISTORICAL_SLIPPAGE_RATE_PER_SIDE_V0
        )


def d1_historical_exit_execution_price_v0(
    *,
    side: D1SideV0,
    reference_price: Decimal,
) -> Decimal:
    """Return the frozen adverse exit execution price for a reference open."""

    sign = _side_sign(side)
    _positive_decimal(reference_price, "reference_price")
    with localcontext(protocol_decimal_context_v2()):
        return reference_price * (
            Decimal(1) - sign * D1_HISTORICAL_SLIPPAGE_RATE_PER_SIDE_V0
        )


def calculate_d1_historical_execution_v0(
    *,
    side: D1SideV0,
    fee_cell: D1HistoricalFeeCellV0,
    entry_time_ms: int,
    exit_time_ms: int,
    entry_reference_price: Decimal,
    exit_reference_price: Decimal,
    funding_points: tuple[D1HistoricalFundingPointV0, ...],
) -> D1HistoricalExecutionV0:
    """Apply adverse prices, fees, and strict-interior settled funding exactly."""

    if not isinstance(side, D1SideV0):
        raise D1HistoricalMathErrorV0("side must be LONG or SHORT")
    if not isinstance(fee_cell, D1HistoricalFeeCellV0):
        raise D1HistoricalMathErrorV0("fee_cell is unsupported")
    _nonnegative_int(entry_time_ms, "entry_time_ms")
    _nonnegative_int(exit_time_ms, "exit_time_ms")
    if exit_time_ms <= entry_time_ms:
        raise D1HistoricalMathErrorV0("exit_time_ms must follow entry_time_ms")
    _positive_decimal(entry_reference_price, "entry_reference_price")
    _positive_decimal(exit_reference_price, "exit_reference_price")
    if type(funding_points) is not tuple or any(
        type(item) is not D1HistoricalFundingPointV0 for item in funding_points
    ):
        raise D1HistoricalMathErrorV0("funding_points must be an exact immutable tuple")
    times = tuple(item.funding_time_ms for item in funding_points)
    if any(current <= previous for previous, current in pairwise(times)):
        raise D1HistoricalMathErrorV0("funding points must be strictly ordered and unique")

    sign = _side_sign(side)
    with localcontext(protocol_decimal_context_v2()):
        entry_execution = d1_historical_entry_execution_price_v0(
            side=side,
            reference_price=entry_reference_price,
        )
        exit_execution = d1_historical_exit_execution_price_v0(
            side=side,
            reference_price=exit_reference_price,
        )
        gross = sign * (exit_reference_price - entry_reference_price) / entry_reference_price
        execution = sign * (exit_execution - entry_execution) / entry_reference_price
        slippage = gross - execution
        fee = fee_cell.rate_per_side * (entry_execution + exit_execution) / entry_reference_price
        funding = Decimal(0)
        funding_event_count = 0
        for point in funding_points:
            if point.funding_time_ms in {entry_time_ms, exit_time_ms}:
                raise D1HistoricalFundingBoundaryAmbiguityV0(
                    "funding timestamp equals entry or exit time"
                )
            if not entry_time_ms < point.funding_time_ms < exit_time_ms:
                continue
            if point.mark_price is None:
                raise D1HistoricalMathErrorV0(
                    "strict-interior funding mark price is missing"
                )
            funding += -sign * point.rate * point.mark_price / entry_reference_price
            funding_event_count += 1
        net = execution - fee + funding

    return D1HistoricalExecutionV0(
        side=side,
        fee_cell=fee_cell,
        entry_time_ms=entry_time_ms,
        exit_time_ms=exit_time_ms,
        entry_reference_price=entry_reference_price,
        exit_reference_price=exit_reference_price,
        entry_execution_price=entry_execution,
        exit_execution_price=exit_execution,
        gross_return=gross,
        execution_return_before_fee_and_funding=execution,
        slippage_return=slippage,
        fee_return=fee,
        funding_return=funding,
        net_return=net,
        funding_event_count=funding_event_count,
        _factory_token=_EXECUTION_FACTORY_TOKEN,
    )


def project_d1_historical_pnl_v0(
    execution: D1HistoricalExecutionV0,
    *,
    notional_usdt: Decimal,
) -> Decimal:
    """Project one return onto a nominal cell without creating a new sample."""

    if type(execution) is not D1HistoricalExecutionV0:
        raise D1HistoricalMathErrorV0("execution must be exact D1HistoricalExecutionV0")
    _positive_decimal(notional_usdt, "notional_usdt")
    with localcontext(protocol_decimal_context_v2()):
        return execution.net_return * notional_usdt


def _finite_decimal(value: object, label: str) -> None:
    if type(value) is not Decimal or not value.is_finite():
        raise D1HistoricalMathErrorV0(f"{label} must be a finite Decimal")


def _positive_decimal(value: object, label: str) -> None:
    _finite_decimal(value, label)
    assert isinstance(value, Decimal)
    if value <= 0:
        raise D1HistoricalMathErrorV0(f"{label} must be positive")


def _nonnegative_int(value: object, label: str) -> None:
    if type(value) is not int or value < 0:
        raise D1HistoricalMathErrorV0(f"{label} must be a nonnegative integer")


def _side_sign(side: D1SideV0) -> Decimal:
    if not isinstance(side, D1SideV0):
        raise D1HistoricalMathErrorV0("side must be LONG or SHORT")
    return Decimal(1) if side is D1SideV0.LONG else Decimal(-1)
