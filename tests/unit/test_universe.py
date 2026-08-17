from typing import Any, cast

import pytest

from signalbot.clock import ReplayClock
from signalbot.config import BinanceSettings
from signalbot.domain.enums import Market
from signalbot.exchange.binance.rest import BinanceRestClient
from signalbot.exchange.binance.universe import UniverseSelector


class FakeRest:
    market = Market.FUTURES

    async def exchange_info(self) -> dict[str, Any]:
        old = 1_600_000_000_000
        return {
            "symbols": [
                {
                    "symbol": "BTCUSDT",
                    "baseAsset": "BTC",
                    "quoteAsset": "USDT",
                    "status": "TRADING",
                    "contractType": "PERPETUAL",
                    "onboardDate": old,
                },
                {
                    "symbol": "ETHUSDT",
                    "baseAsset": "ETH",
                    "quoteAsset": "USDT",
                    "status": "TRADING",
                    "contractType": "PERPETUAL",
                    "onboardDate": old,
                },
                {
                    "symbol": "BTCUPUSDT",
                    "baseAsset": "BTCUP",
                    "quoteAsset": "USDT",
                    "status": "TRADING",
                    "contractType": "PERPETUAL",
                    "onboardDate": old,
                },
                {
                    "symbol": "QUARTERUSDT",
                    "baseAsset": "QUARTER",
                    "quoteAsset": "USDT",
                    "status": "TRADING",
                    "contractType": "CURRENT_QUARTER",
                    "onboardDate": old,
                },
            ]
        }

    async def tickers_24h(self) -> list[dict[str, Any]]:
        return [
            {"symbol": "BTCUSDT", "quoteVolume": "1000000"},
            {"symbol": "ETHUSDT", "quoteVolume": "800000"},
            {"symbol": "BTCUPUSDT", "quoteVolume": "9999999"},
        ]


@pytest.mark.asyncio
async def test_universe_filters_contracts_and_leveraged_tokens_then_ranks_volume() -> None:
    settings = BinanceSettings(
        top_n=1,
        surveillance_n=2,
        min_quote_volume=100,
        minimum_age_days=30,
    )
    selector = UniverseSelector(settings, ReplayClock(1_710_000_000_000))
    universe = await selector.select(cast(BinanceRestClient, FakeRest()))
    assert universe.tradable_symbols == ["BTCUSDT"]
    assert universe.surveillance_symbols == frozenset({"BTCUSDT", "ETHUSDT"})


@pytest.mark.asyncio
async def test_surveillance_panel_is_volume_ranked_and_bounded() -> None:
    settings = BinanceSettings(
        top_n=1,
        surveillance_n=1,
        min_quote_volume=100,
        minimum_age_days=30,
    )
    selector = UniverseSelector(settings, ReplayClock(1_710_000_000_000))
    universe = await selector.select(cast(BinanceRestClient, FakeRest()))
    assert universe.tradable_symbols == ["BTCUSDT"]
    assert universe.surveillance_symbols == frozenset({"BTCUSDT"})
