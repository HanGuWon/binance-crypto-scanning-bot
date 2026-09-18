import json

import httpx
import pytest

from signalbot.domain.enums import Market
from signalbot.exchange.binance.rest import (
    BinanceRateLimitError,
    BinanceRestClient,
    BinanceRestError,
)


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
async def test_rest_client_resolves_earliest_public_kline_open_time() -> None:
    async def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/api/v3/klines"
        assert request.url.params["symbol"] == "ETHUSDT"
        assert request.url.params["interval"] == "1d"
        assert request.url.params["startTime"] == "0"
        assert request.url.params["limit"] == "1"
        return httpx.Response(
            200,
            json=[
                [
                    1_500_000_000_000,
                    "1", "2", "0.5", "1.5", "10",
                    1_500_086_399_999, "15", 3, "6", "9", "0",
                ]
            ],
        )

    http = httpx.AsyncClient(
        base_url="https://api.binance.com", transport=httpx.MockTransport(handler)
    )
    try:
        client = BinanceRestClient(Market.SPOT, client=http)
        assert await client.earliest_kline_open_time_ms("ethusdt") == 1_500_000_000_000
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
async def test_rest_client_honors_retry_after_before_429_retry(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls = 0
    sleeps: list[float] = []

    async def fake_sleep(seconds: float) -> None:
        sleeps.append(seconds)
        client._embargo_until = 0.0

    async def handler(_: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        if calls == 1:
            return httpx.Response(429, headers={"Retry-After": "2"})
        return httpx.Response(200, json=[])

    monkeypatch.setattr("signalbot.exchange.binance.rest.asyncio.sleep", fake_sleep)
    http = httpx.AsyncClient(
        base_url="https://api.binance.com", transport=httpx.MockTransport(handler)
    )
    client = BinanceRestClient(Market.SPOT, max_attempts=2, client=http)
    try:
        assert await client.tickers_24h() == []
    finally:
        await http.aclose()

    assert calls == 2
    assert len(sleeps) == 1
    assert 1.9 <= sleeps[0] <= 2.0


@pytest.mark.asyncio
async def test_rest_client_418_fails_fast_and_preserves_ban_embargo() -> None:
    calls = 0

    async def handler(_: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return httpx.Response(418, headers={"Retry-After": "30"})

    http = httpx.AsyncClient(
        base_url="https://api.binance.com", transport=httpx.MockTransport(handler)
    )
    client = BinanceRestClient(Market.SPOT, max_attempts=4, client=http)
    try:
        with pytest.raises(BinanceRateLimitError) as exc_info:
            await client.tickers_24h()
    finally:
        await http.aclose()

    assert calls == 1
    assert exc_info.value.status_code == 418
    assert exc_info.value.retry_after_seconds == 30.0
    assert client.rate_limit_embargo_remaining_seconds > 29.0


@pytest.mark.asyncio
async def test_rest_client_captures_used_weight_headers() -> None:
    async def handler(_: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json=[],
            headers={
                "X-MBX-USED-WEIGHT-1M": "321",
                "X-MBX-USED-WEIGHT": "12",
            },
        )

    http = httpx.AsyncClient(
        base_url="https://api.binance.com", transport=httpx.MockTransport(handler)
    )
    client = BinanceRestClient(Market.SPOT, client=http)
    try:
        await client.tickers_24h()
    finally:
        await http.aclose()

    assert client.used_weight_headers == {
        "X-MBX-USED-WEIGHT-1M": 321,
        "X-MBX-USED-WEIGHT": 12,
    }


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
