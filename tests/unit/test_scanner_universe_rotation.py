from __future__ import annotations

from decimal import Decimal
from types import SimpleNamespace
from typing import Any, cast

import pytest

from signalbot.domain.enums import Market
from signalbot.domain.models import Instrument
from signalbot.exchange.binance.universe import Universe
from signalbot.scanner import MarketScanner


def _instrument(symbol: str, *, volume: str = "1000000") -> Instrument:
    return Instrument(
        market=Market.SPOT,
        symbol=symbol,
        base_asset=symbol.removesuffix("USDT"),
        quote_asset="USDT",
        status="TRADING",
        quote_volume=Decimal(volume),
    )


def _universe(
    tradable: tuple[str, ...],
    *,
    surveillance: tuple[str, ...] | None = None,
    context: tuple[str, ...] = ("BTCUSDT",),
) -> Universe:
    surveillance_symbols = surveillance or tradable
    instruments = {
        symbol: _instrument(symbol)
        for symbol in {*tradable, *surveillance_symbols, *context}
    }
    return Universe(
        market=Market.SPOT,
        tradable=tuple(instruments[symbol] for symbol in tradable),
        surveillance=tuple(instruments[symbol] for symbol in surveillance_symbols),
        context=tuple(instruments[symbol] for symbol in context),
    )


@pytest.mark.asyncio
async def test_universe_change_requires_consecutive_confirmations() -> None:
    current = _universe(("ETHUSDT",), surveillance=("ETHUSDT", "BTCUSDT"))
    candidate = _universe(("SOLUSDT",), surveillance=("SOLUSDT", "BTCUSDT"))

    class Selector:
        async def select(self, _rest: Any) -> Universe:
            return candidate

    scanner = object.__new__(MarketScanner)
    scanner.market = Market.SPOT
    scanner.settings = cast(
        Any,
        SimpleNamespace(
            binance=SimpleNamespace(universe_change_confirmations=2)
        ),
    )
    scanner.runtime = cast(Any, SimpleNamespace(set_active_symbols=lambda *args: None))
    scanner.selector = cast(Any, Selector())
    scanner.rest = cast(Any, object())
    scanner.universe = current
    scanner._pending_universe_signature = None
    scanner._pending_universe_confirmations = 0

    assert await scanner._poll_universe_candidate() is None
    assert scanner._pending_universe_confirmations == 1
    assert await scanner._poll_universe_candidate() is candidate
    assert scanner._pending_universe_confirmations == 2


@pytest.mark.asyncio
async def test_surveillance_only_change_applies_without_subscription_rotation() -> None:
    current = _universe(("ETHUSDT",), surveillance=("ETHUSDT", "BTCUSDT"))
    candidate = _universe(
        ("ETHUSDT",),
        surveillance=("ETHUSDT", "BTCUSDT", "SOLUSDT"),
    )
    active_calls: list[tuple[Any, ...]] = []

    class Selector:
        async def select(self, _rest: Any) -> Universe:
            return candidate

    scanner = object.__new__(MarketScanner)
    scanner.market = Market.SPOT
    scanner.settings = cast(
        Any,
        SimpleNamespace(
            binance=SimpleNamespace(universe_change_confirmations=2)
        ),
    )
    scanner.runtime = cast(
        Any,
        SimpleNamespace(
            set_active_symbols=lambda *args: active_calls.append(args)
        ),
    )
    scanner.selector = cast(Any, Selector())
    scanner.rest = cast(Any, object())
    scanner.universe = current
    scanner._pending_universe_signature = None
    scanner._pending_universe_confirmations = 0

    assert await scanner._poll_universe_candidate() is None
    assert scanner.universe is candidate
    assert len(active_calls) == 1
    assert scanner._pending_universe_confirmations == 0


@pytest.mark.asyncio
async def test_activate_universe_bootstraps_only_new_detailed_symbols(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    current = _universe(("ETHUSDT",), surveillance=("ETHUSDT", "BTCUSDT"))
    candidate = _universe(("SOLUSDT",), surveillance=("SOLUSDT", "BTCUSDT"))
    active_calls: list[tuple[Any, ...]] = []
    bootstrap_calls: list[list[str]] = []

    scanner = object.__new__(MarketScanner)
    scanner.market = Market.SPOT
    scanner.runtime = cast(
        Any,
        SimpleNamespace(
            set_active_symbols=lambda *args: active_calls.append(args)
        ),
    )
    scanner.universe = current
    scanner._pending_universe_signature = MarketScanner._universe_signature(candidate)
    scanner._pending_universe_confirmations = 2

    async def bootstrap(symbols: list[str]) -> None:
        bootstrap_calls.append(symbols)

    monkeypatch.setattr(scanner, "_bootstrap", bootstrap)

    await scanner._activate_universe(candidate)

    assert len(active_calls) == 1
    assert bootstrap_calls == [["SOLUSDT"]]
    assert scanner.universe is candidate
    assert scanner._pending_universe_signature is None
    assert scanner._pending_universe_confirmations == 0
