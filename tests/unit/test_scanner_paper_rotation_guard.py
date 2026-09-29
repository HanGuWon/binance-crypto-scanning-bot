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
    priority_updates: list[tuple[Instrument, ...]] = []

    class Selector:
        async def select(self, _rest: Any) -> Universe:
            return candidate

    scanner = object.__new__(MarketScanner)
    scanner.market = Market.SPOT
    scanner.settings = cast(
        Any,
        SimpleNamespace(binance=SimpleNamespace(universe_change_confirmations=1)),
    )
    scanner.runtime = cast(
        Any,
        SimpleNamespace(
            paper_positions=SimpleNamespace(continuation_symbols=frozenset({"ETHUSDT"})),
            set_priority_gainers=priority_updates.append,
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
    assert priority_updates == []

    scanner.runtime.paper_positions.continuation_symbols = frozenset()

    assert await scanner._poll_universe_candidate() is candidate


@pytest.mark.asyncio
async def test_new_gainer_keeps_active_paper_symbol_and_updates_after_activation() -> None:
    instruments = {
        symbol: _instrument(symbol) for symbol in ("AAAUSDT", "BBBUSDT", "CCCUSDT", "DDDUSDT")
    }
    current = Universe(
        market=Market.SPOT,
        tradable=tuple(instruments[s] for s in ("AAAUSDT", "BBBUSDT", "CCCUSDT")),
        surveillance=tuple(instruments.values()),
    )
    candidate = Universe(
        market=Market.SPOT,
        tradable=tuple(instruments[s] for s in ("DDDUSDT", "BBBUSDT", "CCCUSDT")),
        surveillance=tuple(instruments[s] for s in ("DDDUSDT", "BBBUSDT", "CCCUSDT")),
        priority_gainers=(instruments["DDDUSDT"],),
    )

    class Selector:
        async def select(self, _rest: Any) -> Universe:
            return candidate

    priority_updates: list[tuple[Instrument, ...]] = []
    active_updates: list[frozenset[str]] = []
    scanner = object.__new__(MarketScanner)
    scanner.market = Market.SPOT
    scanner.settings = cast(
        Any,
        SimpleNamespace(binance=SimpleNamespace(universe_change_confirmations=2)),
    )
    scanner.runtime = cast(
        Any,
        SimpleNamespace(
            paper_positions=SimpleNamespace(continuation_symbols=frozenset({"AAAUSDT"})),
            set_priority_gainers=priority_updates.append,
            set_active_symbols=lambda tradable, *_args: active_updates.append(tradable),
        ),
    )
    scanner.selector = cast(Any, Selector())
    scanner.rest = cast(Any, object())
    scanner.universe = current
    scanner._pending_universe_signature = None
    scanner._pending_universe_confirmations = 0

    rotation = await scanner._poll_universe_candidate()

    assert rotation is not None
    assert set(rotation.tradable_symbols) == {"AAAUSDT", "BBBUSDT", "DDDUSDT"}
    assert set(rotation.tradable_symbols).issubset(rotation.surveillance_symbols)
    assert [item.symbol for item in rotation.priority_gainers] == ["DDDUSDT"]
    assert priority_updates == []

    async def bootstrap(_symbols: list[str]) -> None:
        return None

    scanner._bootstrap = cast(Any, bootstrap)
    await scanner._activate_universe(rotation)
    assert active_updates == [frozenset({"AAAUSDT", "BBBUSDT", "DDDUSDT"})]
    assert priority_updates == [rotation.priority_gainers]
