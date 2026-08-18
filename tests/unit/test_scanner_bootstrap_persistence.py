from decimal import Decimal
from types import SimpleNamespace
from typing import cast

import pytest

from conftest import make_candle
from signalbot.domain.enums import Market
from signalbot.domain.models import Candle, Instrument
from signalbot.exchange.binance.universe import Universe
from signalbot.scanner import MarketScanner


@pytest.mark.asyncio
async def test_scanner_bootstrap_persists_each_rest_batch_once() -> None:
    first_batch = [make_candle(index) for index in range(2)]
    second_batch = [
        candle.model_copy(update={"interval": "15m"}) for candle in first_batch
    ]

    class FakeRest:
        async def klines(
            self,
            _symbol: str,
            interval: str,
            _limit: int,
            *,
            now_ms: int,
        ) -> list[Candle]:
            assert now_ms == 1234
            return first_batch if interval == "5m" else second_batch

    class FakeRepository:
        def __init__(self) -> None:
            self.batches: list[list[Candle]] = []

        def save_candles(self, candles: list[Candle]) -> int:
            self.batches.append(list(candles))
            return len(candles)

    class FakeRuntime:
        def __init__(self) -> None:
            self.repository = FakeRepository()
            self.bootstraps: list[list[Candle]] = []
            self.rebuilds = 0

        def bootstrap(self, candles: list[Candle], *, rebuild: bool) -> None:
            assert rebuild is False
            self.bootstraps.append(list(candles))

        def rebuild_derived_state(self) -> None:
            self.rebuilds += 1

    runtime = FakeRuntime()
    scanner = cast(
        MarketScanner,
        SimpleNamespace(
            settings=SimpleNamespace(
                binance=SimpleNamespace(
                    rest_concurrency=1,
                    bootstrap_candles=2,
                    intervals=["5m", "15m"],
                ),
                runtime=SimpleNamespace(persist_candles=True),
            ),
            rest=FakeRest(),
            runtime=runtime,
            clock=SimpleNamespace(now_ms=lambda: 1234),
        ),
    )

    await MarketScanner._bootstrap(scanner, ["BTCUSDT"])

    assert runtime.bootstraps == [first_batch, second_batch]
    assert runtime.repository.batches == [first_batch, second_batch]
    assert runtime.rebuilds == 1


@pytest.mark.asyncio
async def test_scanner_prepares_tradable_and_independent_context_market_data() -> None:
    class FakeSelector:
        async def select(self, _rest: object) -> Universe:
            eth = Instrument(
                market=Market.SPOT,
                symbol="ETHUSDT",
                base_asset="ETH",
                quote_asset="USDT",
                status="TRADING",
                quote_volume=Decimal("1"),
            )
            btc = eth.model_copy(
                update={"symbol": "BTCUSDT", "base_asset": "BTC"}
            )
            return Universe(Market.SPOT, (eth,), (eth,), (btc,))

    class FakeRuntime:
        def __init__(self) -> None:
            self.active: tuple[frozenset[str], frozenset[str], frozenset[str]] | None = None

        def set_active_symbols(
            self,
            tradable: frozenset[str],
            surveillance: frozenset[str],
            context: frozenset[str],
        ) -> None:
            self.active = (tradable, surveillance, context)

    bootstrapped: list[list[str]] = []

    async def bootstrap(symbols: list[str]) -> None:
        bootstrapped.append(symbols)

    runtime = FakeRuntime()
    scanner = cast(
        MarketScanner,
        SimpleNamespace(
            selector=FakeSelector(),
            rest=object(),
            runtime=runtime,
            market=Market.SPOT,
            universe=None,
            _bootstrap=bootstrap,
        ),
    )

    universe = await MarketScanner.prepare(scanner)

    assert universe.tradable_symbols == ["ETHUSDT"]
    assert runtime.active == (
        frozenset({"ETHUSDT"}),
        frozenset({"ETHUSDT"}),
        frozenset({"BTCUSDT"}),
    )
    assert bootstrapped == [["BTCUSDT", "ETHUSDT"]]
