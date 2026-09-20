from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Literal, Protocol

FailureCategory = Literal["temporary", "terminal"]
PositionSide = Literal["BOTH", "LONG", "SHORT"]


class GuardianReadError(RuntimeError):
    """A classified failure while reading the private Futures account."""

    def __init__(
        self,
        message: str,
        *,
        category: FailureCategory,
        status_code: int | None = None,
        binance_code: int | None = None,
    ) -> None:
        super().__init__(message)
        self.category = category
        self.status_code = status_code
        self.binance_code = binance_code


class AuthenticationError(GuardianReadError):
    def __init__(self, *, status_code: int | None = None, binance_code: int | None = None) -> None:
        super().__init__(
            "Binance private-read authentication failed",
            category="terminal",
            status_code=status_code,
            binance_code=binance_code,
        )


class ClockSkewError(GuardianReadError):
    def __init__(self, *, status_code: int | None = None, binance_code: int | None = None) -> None:
        super().__init__(
            "Binance rejected the signed request because of clock skew",
            category="temporary",
            status_code=status_code,
            binance_code=binance_code,
        )


class RateLimitError(GuardianReadError):
    def __init__(self, *, status_code: int) -> None:
        super().__init__(
            "Binance private-read rate limit requires bounded retry",
            category="temporary",
            status_code=status_code,
        )


class MalformedPayloadError(GuardianReadError):
    def __init__(self, message: str) -> None:
        super().__init__(message, category="terminal")


class UnknownPositionModeError(GuardianReadError):
    def __init__(self) -> None:
        super().__init__("Binance returned an unknown position mode", category="terminal")


@dataclass(frozen=True)
class ServerTime:
    server_time_ms: int


@dataclass(frozen=True)
class PositionSnapshot:
    symbol: str
    position_side: PositionSide
    position_amount: Decimal
    entry_price: Decimal
    mark_price: Decimal
    unrealized_profit: Decimal
    update_time_ms: int


@dataclass(frozen=True)
class PositionModeSnapshot:
    mode: Literal["one_way", "hedge"]


@dataclass(frozen=True)
class OpenOrderSnapshot:
    order_id: int
    symbol: str
    position_side: PositionSide
    side: Literal["BUY", "SELL"]
    order_type: str
    status: str
    quantity: Decimal
    stop_price: Decimal | None
    close_position: bool
    reduce_only: bool


@dataclass(frozen=True)
class AlgoOrderSnapshot:
    algo_id: int
    symbol: str
    position_side: PositionSide
    side: Literal["BUY", "SELL"]
    order_type: str
    status: str
    quantity: Decimal
    trigger_price: Decimal | None
    close_position: bool
    reduce_only: bool


@dataclass(frozen=True)
class SymbolFilterSnapshot:
    symbol: str
    status: str
    price_tick_size: Decimal
    quantity_step_size: Decimal
    minimum_quantity: Decimal


class PrivateReadClient(Protocol):
    async def server_time(self) -> ServerTime: ...

    async def positions(self, symbol: str | None = None) -> tuple[PositionSnapshot, ...]: ...

    async def position_mode(self) -> PositionModeSnapshot: ...

    async def open_orders(self, symbol: str | None = None) -> tuple[OpenOrderSnapshot, ...]: ...

    async def open_algo_orders(
        self, symbol: str | None = None
    ) -> tuple[AlgoOrderSnapshot, ...]: ...

    async def symbol_filters(
        self, symbol: str | None = None
    ) -> tuple[SymbolFilterSnapshot, ...]: ...
