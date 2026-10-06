from __future__ import annotations

import asyncio
from decimal import Decimal
from types import SimpleNamespace
from typing import Any, cast

import pytest

from signalbot.clock import ReplayClock
from signalbot.config import (
    DIRECTIONAL_FROZEN_SYMBOLS,
    DIRECTIONAL_PREREGISTRATION_SHA256,
    Settings,
)
from signalbot.domain.enums import Market
from signalbot.domain.models import Instrument
from signalbot.exchange.binance.universe import (
    RequiredUniverseUnavailableError,
    Universe,
)
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
    market: Market = Market.SPOT,
) -> Universe:
    surveillance_symbols = surveillance or tradable
    instruments = {
        symbol: _instrument(symbol)
        for symbol in {*tradable, *surveillance_symbols, *context}
    }
    return Universe(
        market=market,
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


def _directional_settings() -> Settings:
    return Settings.model_validate(
        {
            "shadow": {
                "directional_observation_enabled": True,
                "directional_campaign_id": "futures-bidirectional-test-1",
                "directional_source_identity": "worktree-source-v1:" + "a" * 64,
                "directional_campaign_created_at_ms": 100,
                "directional_activation_ms": 200,
                "directional_symbols": list(DIRECTIONAL_FROZEN_SYMBOLS),
                "directional_preregistration_sha256": (
                    DIRECTIONAL_PREREGISTRATION_SHA256
                ),
            }
        }
    )


def test_scanner_passes_frozen_symbols_only_to_directional_futures() -> None:
    runtime = SimpleNamespace(gap_recoverer=None)
    futures = MarketScanner(
        Market.FUTURES,
        _directional_settings(),
        ReplayClock(1_000),
        cast(Any, runtime),
        asyncio.Event(),
        rest_client=cast(Any, SimpleNamespace()),
    )
    spot = MarketScanner(
        Market.SPOT,
        _directional_settings(),
        ReplayClock(1_000),
        cast(Any, SimpleNamespace(gap_recoverer=None)),
        asyncio.Event(),
        rest_client=cast(Any, SimpleNamespace()),
    )
    disabled = MarketScanner(
        Market.FUTURES,
        Settings(),
        ReplayClock(1_000),
        cast(Any, SimpleNamespace(gap_recoverer=None)),
        asyncio.Event(),
        rest_client=cast(Any, SimpleNamespace()),
    )

    assert futures.selector.required_symbols == frozenset(DIRECTIONAL_FROZEN_SYMBOLS)
    assert spot.selector.required_symbols == frozenset()
    assert disabled.selector.required_symbols == frozenset()


@pytest.mark.asyncio
async def test_directional_prepare_reaches_funding_bootstrap_and_websocket(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    expected = tuple(DIRECTIONAL_FROZEN_SYMBOLS)
    universe = _universe(expected, context=(), market=Market.FUTURES)
    funding_calls: list[tuple[list[str], bool]] = []
    bootstrap_calls: list[list[str]] = []
    websocket_symbols: list[list[str]] = []

    class Selector:
        async def select(self, _rest: Any) -> Universe:
            return universe

    scanner = object.__new__(MarketScanner)
    scanner.market = Market.FUTURES
    scanner.settings = _directional_settings()
    scanner.selector = cast(Any, Selector())
    scanner.rest = cast(Any, object())
    scanner.runtime = cast(
        Any,
        SimpleNamespace(set_active_symbols=lambda *_args: None),
    )

    async def refresh_funding(symbols: list[str], *, bootstrap: bool) -> None:
        funding_calls.append((symbols, bootstrap))

    async def bootstrap(symbols: list[str]) -> None:
        bootstrap_calls.append(symbols)

    monkeypatch.setattr(scanner, "_refresh_funding", refresh_funding)
    monkeypatch.setattr(scanner, "_bootstrap", bootstrap)
    monkeypatch.setattr(
        "signalbot.scanner.build_websocket_plans",
        lambda _market, symbols, *_args, **_kwargs: websocket_symbols.append(symbols) or [],
    )

    await scanner.prepare()
    assert funding_calls == [(list(expected), True)]
    assert bootstrap_calls == [sorted(expected)]
    assert scanner._start_websocket_tasks(universe) == []
    assert websocket_symbols == [sorted(expected)]


@pytest.mark.asyncio
async def test_required_symbol_loss_escapes_refresh_fail_closed() -> None:
    class Selector:
        async def select(self, _rest: Any) -> Universe:
            raise RequiredUniverseUnavailableError("required futures symbols unavailable")

    scanner = object.__new__(MarketScanner)
    scanner.market = Market.FUTURES
    scanner.selector = cast(Any, Selector())
    scanner.rest = cast(Any, object())
    scanner.universe = _universe(("BTCUSDT",), market=Market.FUTURES)

    with pytest.raises(RequiredUniverseUnavailableError):
        await scanner._poll_universe_candidate()


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


@pytest.mark.asyncio
async def test_confirmed_rotation_censors_only_outgoing_retest_symbols(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    current = _universe(("ETHUSDT", "BTCUSDT"), surveillance=("ETHUSDT", "BTCUSDT"))
    candidate = _universe(("SOLUSDT", "BTCUSDT"), surveillance=("SOLUSDT", "BTCUSDT"))
    censor_calls: list[tuple[set[str], int]] = []
    scanner = object.__new__(MarketScanner)
    scanner.market = Market.SPOT
    scanner.clock = cast(Any, SimpleNamespace(now_ms=lambda: 1_710_000_123_000))
    scanner.runtime = cast(
        Any,
        SimpleNamespace(
            shadow_observer=SimpleNamespace(
                censor_universe_exit=lambda symbols, *, decision_time_ms: censor_calls.append(
                    (set(symbols), decision_time_ms)
                )
            ),
            set_active_symbols=lambda *args: None,
        ),
    )
    scanner.universe = current
    scanner._pending_universe_signature = MarketScanner._universe_signature(candidate)
    scanner._pending_universe_confirmations = 2

    async def bootstrap(_symbols: list[str]) -> None:
        return None

    monkeypatch.setattr(scanner, "_bootstrap", bootstrap)
    await scanner._activate_universe(candidate)

    assert censor_calls == [({"ETHUSDT"}, 1_710_000_123_000)]


@pytest.mark.asyncio
async def test_retest_censor_failure_does_not_block_confirmed_rotation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    current = _universe(("ETHUSDT",), surveillance=("ETHUSDT", "BTCUSDT"))
    candidate = _universe(("SOLUSDT",), surveillance=("SOLUSDT", "BTCUSDT"))
    scanner = object.__new__(MarketScanner)
    scanner.market = Market.SPOT
    scanner.clock = cast(Any, SimpleNamespace(now_ms=lambda: 1_710_000_123_000))
    scanner.runtime = cast(
        Any,
        SimpleNamespace(
            shadow_observer=SimpleNamespace(
                censor_universe_exit=lambda *_args, **_kwargs: (_ for _ in ()).throw(
                    RuntimeError("research store unavailable")
                )
            ),
            set_active_symbols=lambda *args: None,
        ),
    )
    scanner.universe = current
    scanner._pending_universe_signature = MarketScanner._universe_signature(candidate)
    scanner._pending_universe_confirmations = 2

    async def bootstrap(_symbols: list[str]) -> None:
        return None

    monkeypatch.setattr(scanner, "_bootstrap", bootstrap)
    await scanner._activate_universe(candidate)

    assert scanner.universe is candidate
