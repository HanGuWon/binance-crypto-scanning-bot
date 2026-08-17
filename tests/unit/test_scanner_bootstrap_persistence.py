from types import SimpleNamespace
from typing import cast

import pytest

from conftest import make_candle
from signalbot.domain.models import Candle
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
