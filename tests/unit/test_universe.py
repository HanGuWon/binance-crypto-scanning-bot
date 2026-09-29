from decimal import Decimal
from typing import Any, cast

import pytest

from signalbot.clock import ReplayClock
from signalbot.config import BinanceSettings, MarketWatchSettings
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


@pytest.mark.asyncio
async def test_btc_benchmark_is_retained_when_volume_rank_falls_outside_panel() -> None:
    class LowVolumeBtcRest(FakeRest):
        async def tickers_24h(self) -> list[dict[str, Any]]:
            return [
                {"symbol": "BTCUSDT", "quoteVolume": "100"},
                {"symbol": "ETHUSDT", "quoteVolume": "1000000"},
            ]

    settings = BinanceSettings(
        top_n=1,
        surveillance_n=1,
        min_quote_volume=500,
        minimum_age_days=30,
    )
    selector = UniverseSelector(settings, ReplayClock(1_710_000_000_000))
    universe = await selector.select(cast(BinanceRestClient, LowVolumeBtcRest()))

    assert universe.tradable_symbols == ["ETHUSDT"]
    assert universe.surveillance_symbols == frozenset({"ETHUSDT"})
    assert universe.context_symbols == frozenset({"BTCUSDT"})


@pytest.mark.asyncio
async def test_btc_benchmark_uses_only_a_non_tradable_surveillance_slot() -> None:
    class ThreeAssetRest(FakeRest):
        async def exchange_info(self) -> dict[str, Any]:
            payload = await super().exchange_info()
            symbols = cast(list[dict[str, Any]], payload["symbols"])
            symbols.append(
                {
                    "symbol": "SOLUSDT",
                    "baseAsset": "SOL",
                    "quoteAsset": "USDT",
                    "status": "TRADING",
                    "contractType": "PERPETUAL",
                    "onboardDate": 1_600_000_000_000,
                }
            )
            return payload

        async def tickers_24h(self) -> list[dict[str, Any]]:
            return [
                {"symbol": "BTCUSDT", "quoteVolume": "100"},
                {"symbol": "ETHUSDT", "quoteVolume": "1000000"},
                {"symbol": "SOLUSDT", "quoteVolume": "900000"},
            ]

    settings = BinanceSettings(
        top_n=1,
        surveillance_n=2,
        min_quote_volume=500,
        minimum_age_days=30,
    )
    selector = UniverseSelector(settings, ReplayClock(1_710_000_000_000))
    universe = await selector.select(cast(BinanceRestClient, ThreeAssetRest()))

    assert universe.tradable_symbols == ["ETHUSDT"]
    assert universe.surveillance_symbols == frozenset({"ETHUSDT", "BTCUSDT"})
    assert universe.context_symbols == frozenset({"BTCUSDT"})


@pytest.mark.asyncio
async def test_priority_gainers_are_top_five_by_24h_change_above_100m_volume() -> None:
    symbols = ["AAA", "BBB", "CCC", "DDD", "EEE", "FFF", "GGG"]

    class GainerRest:
        market = Market.FUTURES

        async def exchange_info(self) -> dict[str, Any]:
            return {
                "symbols": [
                    {
                        "symbol": f"{base}USDT",
                        "baseAsset": base,
                        "quoteAsset": "USDT",
                        "status": "TRADING",
                        "contractType": "PERPETUAL",
                        "onboardDate": 1_600_000_000_000,
                    }
                    for base in symbols
                ]
            }

        async def tickers_24h(self) -> list[dict[str, Any]]:
            return [
                {"symbol": "AAAUSDT", "quoteVolume": "100000001", "priceChangePercent": "5"},
                {"symbol": "BBBUSDT", "quoteVolume": "150000000", "priceChangePercent": "15"},
                {"symbol": "CCCUSDT", "quoteVolume": "160000000", "priceChangePercent": "12"},
                {"symbol": "DDDUSDT", "quoteVolume": "170000000", "priceChangePercent": "10"},
                {"symbol": "EEEUSDT", "quoteVolume": "180000000", "priceChangePercent": "8"},
                {"symbol": "FFFUSDT", "quoteVolume": "900000000", "priceChangePercent": "0"},
                {"symbol": "GGGUSDT", "quoteVolume": "100000000", "priceChangePercent": "99"},
            ]

    settings = BinanceSettings(
        top_n=5,
        surveillance_n=5,
        min_quote_volume=50_000_000,
        minimum_age_days=30,
    )
    watch = MarketWatchSettings(
        enabled=True,
        market=Market.FUTURES,
        top_n=5,
        min_quote_volume_usdt=100_000_000,
    )
    selector = UniverseSelector(
        settings,
        ReplayClock(1_710_000_000_000),
        watch,
    )
    universe = await selector.select(cast(BinanceRestClient, GainerRest()))

    assert [item.symbol for item in universe.priority_gainers] == [
        "BBBUSDT",
        "CCCUSDT",
        "DDDUSDT",
        "EEEUSDT",
        "AAAUSDT",
    ]
    assert set(universe.tradable_symbols) == {
        "AAAUSDT",
        "BBBUSDT",
        "CCCUSDT",
        "DDDUSDT",
        "EEEUSDT",
    }
    assert set(universe.tradable_symbols).issubset(universe.surveillance_symbols)
    assert "GGGUSDT" not in {item.symbol for item in universe.priority_gainers}


def test_nonfinite_ticker_values_cannot_enter_gainer_ranking() -> None:
    rows = [
        {"symbol": "AAAUSDT", "quoteVolume": "NaN", "priceChangePercent": "10"},
        {"symbol": "BBBUSDT", "quoteVolume": "Infinity", "priceChangePercent": "10"},
        {"symbol": "CCCUSDT", "quoteVolume": "100000001", "priceChangePercent": "Infinity"},
    ]

    assert UniverseSelector._ticker_volumes(rows) == {
        "AAAUSDT": Decimal("0"),
        "BBBUSDT": Decimal("0"),
        "CCCUSDT": Decimal("100000001"),
    }
    assert UniverseSelector._ticker_changes(rows) == {
        "AAAUSDT": 10.0,
        "BBBUSDT": 10.0,
        "CCCUSDT": 0.0,
    }
