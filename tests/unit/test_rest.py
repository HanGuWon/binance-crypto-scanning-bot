import json

import httpx
import pytest

from signalbot.domain.enums import Market
from signalbot.exchange.binance.rest import BinanceRestClient, BinanceRestError


@pytest.mark.asyncio
async def test_rest_client_parses_only_closed_historical_klines() -> None:
    async def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/api/v3/klines"
        rows = [
            [0, "1", "2", "0.5", "1.5", "10", 59_999, "15", 3, "6", "9", "0"],
            [60_000, "1", "2", "0.5", "1.5", "10", 119_999, "15", 3, "6", "9", "0"],
        ]
        return httpx.Response(200, content=json.dumps(rows))

    http = httpx.AsyncClient(
        base_url="https://api.binance.com", transport=httpx.MockTransport(handler)
    )
    client = BinanceRestClient(Market.SPOT, client=http)
    try:
        candles = await client.klines("btcusdt", "1m", now_ms=100_000)
        assert len(candles) == 1
        assert candles[0].close_time_ms == 59_999
    finally:
        await http.aclose()


@pytest.mark.asyncio
async def test_rest_client_raises_after_rate_limit_attempts() -> None:
    async def handler(_: httpx.Request) -> httpx.Response:
        return httpx.Response(429)

    http = httpx.AsyncClient(
        base_url="https://api.binance.com", transport=httpx.MockTransport(handler)
    )
    client = BinanceRestClient(Market.SPOT, max_attempts=1, client=http)
    try:
        with pytest.raises(BinanceRestError):
            await client.tickers_24h()
    finally:
        await http.aclose()


@pytest.mark.asyncio
async def test_spot_and_futures_kline_limits_are_clamped_to_exchange_contracts() -> None:
    observed: list[tuple[str, str]] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        observed.append((request.url.path, request.url.params["limit"]))
        return httpx.Response(200, json=[])

    spot_http = httpx.AsyncClient(
        base_url="https://api.binance.com", transport=httpx.MockTransport(handler)
    )
    futures_http = httpx.AsyncClient(
        base_url="https://fapi.binance.com", transport=httpx.MockTransport(handler)
    )
    try:
        await BinanceRestClient(Market.SPOT, client=spot_http).klines("BTCUSDT", "1m", 5000)
        await BinanceRestClient(Market.FUTURES, client=futures_http).klines("BTCUSDT", "1m", 5000)
    finally:
        await spot_http.aclose()
        await futures_http.aclose()
    assert observed == [("/api/v3/klines", "1000"), ("/fapi/v1/klines", "1500")]


@pytest.mark.asyncio
async def test_futures_funding_history_has_public_adapter() -> None:
    async def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/fapi/v1/fundingRate"
        assert request.url.params["symbol"] == "BTCUSDT"
        return httpx.Response(
            200,
            json=[{"symbol": "BTCUSDT", "fundingTime": 1, "fundingRate": "0.0001"}],
        )

    http = httpx.AsyncClient(
        base_url="https://fapi.binance.com", transport=httpx.MockTransport(handler)
    )
    try:
        values = await BinanceRestClient(Market.FUTURES, client=http).funding_rates(
            "btcusdt", 0, 10, 5000
        )
    finally:
        await http.aclose()
    assert values[0]["fundingRate"] == "0.0001"
