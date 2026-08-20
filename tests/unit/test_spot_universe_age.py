from __future__ import annotations

from typing import Any, cast

import pytest

from signalbot.clock import ReplayClock
from signalbot.config import BinanceSettings
from signalbot.domain.enums import Market
from signalbot.exchange.binance.rest import BinanceRestClient
from signalbot.exchange.binance.universe import UniverseSelector

NOW_MS = 1_710_000_000_000
DAY_MS = 86_400_000


class FakeSpotRest:
    market = Market.SPOT

    def __init__(self) -> None:
        self.age_calls: list[str] = []

    async def exchange_info(self) -> dict[str, Any]:
        def row(symbol: str) -> dict[str, Any]:
            return {
                "symbol": symbol,
                "baseAsset": symbol.removesuffix("USDT"),
                "quoteAsset": "USDT",
                "status": "TRADING",
                "isSpotTradingAllowed": True,
            }

        return {
            "symbols": [row("NEWUSDT"), row("OLDUSDT"), row("BTCUSDT")]
        }

    async def tickers_24h(self) -> list[dict[str, Any]]:
        return [
            {"symbol": "NEWUSDT", "quoteVolume": "2000000"},
            {"symbol": "OLDUSDT", "quoteVolume": "1000000"},
            {"symbol": "BTCUSDT", "quoteVolume": "500000"},
        ]

    async def earliest_kline_open_time_ms(
        self, symbol: str, *, interval: str = "1d"
    ) -> int | None:
        assert interval == "1d"
        self.age_calls.append(symbol)
        anchors = {
            "NEWUSDT": NOW_MS - 5 * DAY_MS,
            "OLDUSDT": NOW_MS - 60 * DAY_MS,
            "BTCUSDT": NOW_MS - 1_000 * DAY_MS,
        }
        return anchors[symbol]


def _settings(*, minimum_age_days: int) -> BinanceSettings:
    return BinanceSettings(
        top_n=1,
        surveillance_n=2,
        min_quote_volume=100,
        minimum_age_days=minimum_age_days,
    )


@pytest.mark.asyncio
async def test_spot_minimum_age_uses_earliest_public_kline_and_backfills_panel() -> None:
    rest = FakeSpotRest()
    selector = UniverseSelector(_settings(minimum_age_days=30), ReplayClock(NOW_MS))

    universe = await selector.select(cast(BinanceRestClient, rest))

    assert universe.tradable_symbols == ["OLDUSDT"]
    assert universe.surveillance_symbols == frozenset({"OLDUSDT", "BTCUSDT"})
    assert universe.context_symbols == frozenset({"BTCUSDT"})
    assert universe.tradable[0].onboard_time_ms == NOW_MS - 60 * DAY_MS
    assert rest.age_calls == ["NEWUSDT", "OLDUSDT", "BTCUSDT"]


@pytest.mark.asyncio
async def test_spot_age_anchor_is_cached_across_universe_refreshes() -> None:
    rest = FakeSpotRest()
    selector = UniverseSelector(_settings(minimum_age_days=30), ReplayClock(NOW_MS))

    await selector.select(cast(BinanceRestClient, rest))
    await selector.select(cast(BinanceRestClient, rest))

    assert rest.age_calls == ["NEWUSDT", "OLDUSDT", "BTCUSDT"]


@pytest.mark.asyncio
async def test_spot_age_lookup_is_skipped_when_age_gate_is_disabled() -> None:
    class NoAgeCallSpotRest(FakeSpotRest):
        async def earliest_kline_open_time_ms(
            self, symbol: str, *, interval: str = "1d"
        ) -> int | None:
            raise AssertionError(f"unexpected age lookup for {symbol}")

    rest = NoAgeCallSpotRest()
    selector = UniverseSelector(_settings(minimum_age_days=0), ReplayClock(NOW_MS))

    universe = await selector.select(cast(BinanceRestClient, rest))

    assert universe.tradable_symbols == ["NEWUSDT"]
    assert universe.context_symbols == frozenset({"BTCUSDT"})
