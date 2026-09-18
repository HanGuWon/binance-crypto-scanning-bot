from __future__ import annotations

from decimal import Decimal
from types import SimpleNamespace
from typing import Any, cast

import pytest

from signalbot.domain.enums import Market
from signalbot.domain.models import Instrument
from signalbot.exchange.binance.universe import Universe
from signalbot.scanner import MarketScanner


def _instrument(symbol: str) -> Instrument:
    return Instrument(
        market=Market.SPOT,
        symbol=symbol,
        base_asset=symbol.removesuffix("USDT"),
        quote_asset="USDT",
        status="TRADING",
        quote_volume=Decimal("1000000"),
    )


def _universe(tradable: str) -> Universe:
    symbols = {tradable, "BTCUSDT"}
    instruments = {symbol: _instrument(symbol) for symbol in symbols}
    return Universe(
        market=Market.SPOT,
        tradable=(instruments[tradable],),
        surveillance=tuple(instruments[symbol] for symbol in sorted(symbols)),
        context=(instruments["BTCUSDT"],),
    )


@pytest.mark.asyncio
async def test_universe_rotation_waits_for_outgoing_paper_lifecycle() -> None:
    current = _universe("ETHUSDT")
    candidate = _universe("SOLUSDT")

    class Selector:
        async def select(self, _rest: Any) -> Universe:
            return candidate

    scanner = object.__new__(MarketScanner)
    scanner.market = Market.SPOT
    scanner.settings = cast(
        Any,
        SimpleNamespace(
            binance=SimpleNamespace(universe_change_confirmations=1)
        ),
    )
    scanner.runtime = cast(
        Any,
        SimpleNamespace(
            paper_positions=SimpleNamespace(
                continuation_symbols=frozenset({"ETHUSDT"})
            )
        ),
    )
    scanner.selector = cast(Any, Selector())
    scanner.rest = cast(Any, object())
    scanner.universe = current
    scanner._pending_universe_signature = None
    scanner._pending_universe_confirmations = 0

    assert await scanner._poll_universe_candidate() is None
    assert scanner.universe is current
    assert scanner._pending_universe_signature is None
    assert scanner._pending_universe_confirmations == 0

    scanner.runtime.paper_positions.continuation_symbols = frozenset()

    assert await scanner._poll_universe_candidate() is candidate
