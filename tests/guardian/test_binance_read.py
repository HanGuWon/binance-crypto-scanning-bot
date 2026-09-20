from __future__ import annotations

import asyncio
import json
from collections.abc import Awaitable, Callable
from decimal import Decimal
from pathlib import Path

import httpx
import pytest

from position_guardian.exchange.binance_read import BinancePrivateReadClient
from position_guardian.exchange.protocol import (
    AuthenticationError,
    ClockSkewError,
    GuardianReadError,
    MalformedPayloadError,
    UnknownPositionModeError,
)
from position_guardian.exchange.signing import canonical_query, sign_query

FIXTURE_ROOT = Path(__file__).parents[1] / "fixtures" / "binance_private"
type SleepFn = Callable[[float], Awaitable[None]]


def _fixture(name: str) -> object:
    return json.loads((FIXTURE_ROOT / name).read_text(encoding="utf-8"))


def _client(
    handler: Callable[[httpx.Request], httpx.Response],
    *,
    retry_delays_seconds: tuple[float, ...] = (),
    sleep: SleepFn | None = None,
) -> tuple[BinancePrivateReadClient, httpx.AsyncClient]:
    transport = httpx.MockTransport(handler)
    http_client = httpx.AsyncClient(transport=transport)
    return (
        BinancePrivateReadClient(
            api_key="fixture-api-key",
            api_secret="fixture-api-secret",
            client=http_client,
            retry_delays_seconds=retry_delays_seconds,
            sleep=sleep or asyncio.sleep,
            clock=lambda: 1700000000000,
        ),
        http_client,
    )


def test_signing_golden_vector_preserves_parameter_order() -> None:
    params = (("symbol", "BTCUSDT"), ("timestamp", 1700000000000), ("recvWindow", 5000))

    assert canonical_query(params) == "symbol=BTCUSDT&timestamp=1700000000000&recvWindow=5000"
    assert sign_query(params, "secret-key") == (
        "symbol=BTCUSDT&timestamp=1700000000000&recvWindow=5000"
        "&signature=8060b5a3659c282a31f2af0a1f52a97899704f79df4ef332ad3ba05390884195"
    )


@pytest.mark.asyncio
async def test_recorded_private_reads_parse_all_required_resources() -> None:
    payloads = {
        "/fapi/v1/time": _fixture("server_time.json"),
        "/fapi/v1/positionSide/dual": _fixture("position_mode_hedge.json"),
        "/fapi/v3/positionRisk": _fixture("position_risk.json"),
        "/fapi/v1/openOrders": _fixture("open_orders.json"),
        "/fapi/v1/openAlgoOrders": _fixture("open_algo_orders.json"),
        "/fapi/v1/exchangeInfo": _fixture("exchange_info.json"),
    }
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(200, json=payloads[request.url.path])

    client, http_client = _client(handler)
    async with http_client:
        assert (await client.server_time()).server_time_ms == 1700000000123
        assert (await client.position_mode()).mode == "hedge"
        positions = await client.positions("btcusdt")
        orders = await client.open_orders("BTCUSDT")
        algo_orders = await client.open_algo_orders("ETHUSDT")
        filters = await client.symbol_filters("BTCUSDT")

    assert positions[0].position_amount == Decimal("0.010")
    assert positions[0].mark_price == Decimal("60500")
    assert orders[0].stop_price == Decimal("59000")
    assert orders[0].reduce_only is True
    assert orders[1].quantity == Decimal("0")
    assert orders[1].close_position is True
    assert algo_orders[0].trigger_price == Decimal("3100")
    assert algo_orders[0].position_side == "SHORT"
    assert filters[0].price_tick_size == Decimal("0.10")
    assert filters[0].quantity_step_size == Decimal("0.001")
    assert all(request.headers["X-MBX-APIKEY"] == "fixture-api-key" for request in requests)
    assert all("signature=" in request.url.query.decode() for request in requests[1:5])
    assert "signature=" not in requests[-1].url.query.decode()


@pytest.mark.asyncio
async def test_rate_limit_retries_are_bounded_and_use_recorded_payload() -> None:
    responses = [
        httpx.Response(429, json=_fixture("error_rate_limit.json")),
        httpx.Response(200, json=_fixture("server_time.json")),
    ]
    sleeps: list[float] = []

    async def sleep(delay: float) -> None:
        sleeps.append(delay)

    def handler(_request: httpx.Request) -> httpx.Response:
        return responses.pop(0)

    client, http_client = _client(handler, retry_delays_seconds=(0.25,), sleep=sleep)
    async with http_client:
        assert (await client.server_time()).server_time_ms == 1700000000123
    assert sleeps == [0.25]


@pytest.mark.asyncio
async def test_server_error_retries_even_when_payload_is_not_a_success_schema() -> None:
    responses = [
        httpx.Response(503, json=_fixture("error_server.json")),
        httpx.Response(503, json=_fixture("error_server.json")),
    ]

    def handler(_request: httpx.Request) -> httpx.Response:
        return responses.pop(0)

    client, http_client = _client(handler, retry_delays_seconds=(0.0,))
    async with http_client:
        with pytest.raises(GuardianReadError) as error:
            await client.server_time()
    assert error.value.category == "temporary"
    assert error.value.status_code == 503


@pytest.mark.asyncio
async def test_auth_clock_skew_and_malformed_payload_are_classified() -> None:
    def auth_handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(401, json=_fixture("error_auth.json"))

    auth_client, auth_http = _client(auth_handler)
    async with auth_http:
        with pytest.raises(AuthenticationError) as auth_error:
            await auth_client.positions()
    assert auth_error.value.category == "terminal"
    assert "fixture-api-secret" not in str(auth_error.value)

    def clock_handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(400, json=_fixture("error_clock_skew.json"))

    clock_client, clock_http = _client(clock_handler)
    async with clock_http:
        with pytest.raises(ClockSkewError) as clock_error:
            await clock_client.position_mode()
    assert clock_error.value.category == "temporary"
    assert clock_error.value.binance_code == -1021

    def malformed_handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json=_fixture("malformed_position.json"))

    malformed_client, malformed_http = _client(malformed_handler)
    async with malformed_http:
        with pytest.raises(MalformedPayloadError) as malformed_error:
            await malformed_client.positions()
    assert malformed_error.value.category == "terminal"


@pytest.mark.asyncio
async def test_unknown_position_mode_is_terminal() -> None:
    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json=_fixture("unknown_position_mode.json"))

    client, http_client = _client(handler)
    async with http_client:
        with pytest.raises(UnknownPositionModeError) as error:
            await client.position_mode()
    assert error.value.category == "terminal"


@pytest.mark.asyncio
async def test_retry_sleep_preserves_cancellation_path() -> None:
    async def sleep(_delay: float) -> None:
        raise asyncio.CancelledError

    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(429, json=_fixture("error_rate_limit.json"))

    client, http_client = _client(handler, retry_delays_seconds=(0.25,), sleep=sleep)
    async with http_client:
        with pytest.raises(asyncio.CancelledError):
            await client.server_time()
