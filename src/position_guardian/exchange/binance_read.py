from __future__ import annotations

import asyncio
import json
import time
from collections.abc import Awaitable, Callable, Sequence
from decimal import Decimal, InvalidOperation
from typing import Any, cast

import httpx

from position_guardian.exchange.protocol import (
    AlgoOrderSnapshot,
    AuthenticationError,
    ClockSkewError,
    GuardianReadError,
    MalformedPayloadError,
    OpenOrderSnapshot,
    PositionModeSnapshot,
    PositionSnapshot,
    RateLimitError,
    ServerTime,
    SymbolFilterSnapshot,
    UnknownPositionModeError,
)
from position_guardian.exchange.signing import QueryPair, canonical_query, sign_query

Sleep = Callable[[float], Awaitable[None]]
Clock = Callable[[], int]


def _utc_now_ms() -> int:
    return time.time_ns() // 1_000_000


class BinancePrivateReadClient:
    """Bounded, GET-only USD-M Futures private reader.

    This adapter deliberately has no method capable of placing, amending, or
    cancelling an exchange order. It is safe to use in Guardian observe/shadow
    mode while later phases build a separate write boundary.
    """

    def __init__(
        self,
        *,
        api_key: str,
        api_secret: str,
        exchange_environment: str = "production",
        client: httpx.AsyncClient | None = None,
        retry_delays_seconds: Sequence[float] = (0.25, 0.5),
        sleep: Sleep = asyncio.sleep,
        clock: Clock = _utc_now_ms,
        recv_window_ms: int = 5_000,
    ) -> None:
        if not api_key.strip() or not api_secret.strip():
            raise ValueError("api_key and api_secret must be non-empty")
        if exchange_environment not in {"production", "testnet"}:
            raise ValueError("exchange_environment must be production or testnet")
        if recv_window_ms <= 0 or recv_window_ms > 60_000:
            raise ValueError("recv_window_ms must be between 1 and 60000")
        delays = tuple(float(delay) for delay in retry_delays_seconds)
        if any(delay < 0 for delay in delays):
            raise ValueError("retry delays must be non-negative")
        self._api_key = api_key
        self._api_secret = api_secret
        self._base_url = (
            "https://fapi.binance.com"
            if exchange_environment == "production"
            else "https://testnet.binancefuture.com"
        )
        self._client = client or httpx.AsyncClient(timeout=httpx.Timeout(10.0))
        self._owns_client = client is None
        self._retry_delays = delays
        self._sleep = sleep
        self._clock = clock
        self._recv_window_ms = recv_window_ms

    async def __aenter__(self) -> BinancePrivateReadClient:
        return self

    async def __aexit__(self, *_: object) -> None:
        if self._owns_client:
            await self._client.aclose()

    async def server_time(self) -> ServerTime:
        payload = await self._get_json("/fapi/v1/time")
        data = _mapping(payload, "serverTime")
        return ServerTime(server_time_ms=_row_integer(data, "serverTime", "serverTime"))

    async def positions(self, symbol: str | None = None) -> tuple[PositionSnapshot, ...]:
        params = _optional_symbol(symbol)
        payload = await self._get_json("/fapi/v3/positionRisk", signed=True, params=params)
        rows = _list_payload(payload, "positions")
        return tuple(_parse_position(row) for row in rows)

    async def position_mode(self) -> PositionModeSnapshot:
        payload = await self._get_json("/fapi/v1/positionSide/dual", signed=True)
        if not isinstance(payload, dict) or not isinstance(payload.get("dualSidePosition"), bool):
            raise UnknownPositionModeError()
        return PositionModeSnapshot(mode="hedge" if payload["dualSidePosition"] else "one_way")

    async def open_orders(self, symbol: str | None = None) -> tuple[OpenOrderSnapshot, ...]:
        payload = await self._get_json(
            "/fapi/v1/openOrders", signed=True, params=_optional_symbol(symbol)
        )
        rows = _list_payload(payload, "openOrders")
        return tuple(_parse_open_order(row) for row in rows)

    async def open_algo_orders(self, symbol: str | None = None) -> tuple[AlgoOrderSnapshot, ...]:
        payload = await self._get_json(
            "/fapi/v1/openAlgoOrders", signed=True, params=_optional_symbol(symbol)
        )
        rows = _list_payload(payload, "openAlgoOrders")
        return tuple(_parse_algo_order(row) for row in rows)

    async def symbol_filters(self, symbol: str | None = None) -> tuple[SymbolFilterSnapshot, ...]:
        payload = await self._get_json("/fapi/v1/exchangeInfo")
        if not isinstance(payload, dict):
            raise MalformedPayloadError("exchangeInfo payload must be an object")
        raw_symbols = payload.get("symbols")
        if not isinstance(raw_symbols, list):
            raise MalformedPayloadError("exchangeInfo symbols must be an array")
        wanted = symbol.upper() if symbol is not None else None
        result = tuple(_parse_symbol_filter(row) for row in raw_symbols)
        return tuple(row for row in result if wanted is None or row.symbol == wanted)

    async def _get_json(
        self,
        path: str,
        *,
        signed: bool = False,
        params: Sequence[QueryPair] = (),
    ) -> object:
        attempts = len(self._retry_delays) + 1
        for attempt in range(attempts):
            query_params = list(params)
            if signed:
                query_params.extend(
                    (("timestamp", self._clock()), ("recvWindow", self._recv_window_ms))
                )
                query = sign_query(query_params, self._api_secret)
            else:
                query = canonical_query(query_params)
            url = f"{self._base_url}{path}"
            if query:
                url = f"{url}?{query}"
            try:
                response = await self._client.get(url, headers={"X-MBX-APIKEY": self._api_key})
            except asyncio.CancelledError:
                raise
            except httpx.RequestError as exc:
                if attempt < len(self._retry_delays):
                    await self._sleep(self._retry_delays[attempt])
                    continue
                raise GuardianReadError(
                    "Binance private-read transport failed",
                    category="temporary",
                ) from exc

            if response.status_code in {418, 429}:
                if attempt < len(self._retry_delays):
                    await self._sleep(self._retry_delays[attempt])
                    continue
                raise RateLimitError(status_code=response.status_code)
            if response.status_code in {401, 403}:
                raise AuthenticationError(status_code=response.status_code)
            if response.status_code >= 500:
                if attempt < len(self._retry_delays):
                    await self._sleep(self._retry_delays[attempt])
                    continue
                raise GuardianReadError(
                    "Binance private-read server error",
                    category="temporary",
                    status_code=response.status_code,
                )
            try:
                payload = response.json()
            except (json.JSONDecodeError, ValueError) as exc:
                raise MalformedPayloadError("Binance returned non-JSON data") from exc

            if response.status_code >= 400:
                _raise_binance_error(response.status_code, payload)
            return payload
        raise AssertionError("bounded retry loop exhausted without a result")


def _optional_symbol(symbol: str | None) -> tuple[QueryPair, ...]:
    if symbol is None:
        return ()
    normalized = symbol.strip().upper()
    if not normalized:
        raise ValueError("symbol must not be blank")
    return (("symbol", normalized),)


def _raise_binance_error(status_code: int, payload: object) -> None:
    code = payload.get("code") if isinstance(payload, dict) else None
    numeric_code = code if isinstance(code, int) else None
    if numeric_code == -1021:
        raise ClockSkewError(status_code=status_code, binance_code=numeric_code)
    if numeric_code in {-2014, -2015} or status_code in {401, 403}:
        raise AuthenticationError(status_code=status_code, binance_code=numeric_code)
    raise GuardianReadError(
        "Binance rejected the private-read request",
        category="terminal",
        status_code=status_code,
        binance_code=numeric_code,
    )


def _list_payload(payload: object, label: str) -> list[object]:
    if not isinstance(payload, list):
        raise MalformedPayloadError(f"{label} payload must be an array")
    return payload


def _mapping(row: object, label: str) -> dict[str, Any]:
    if not isinstance(row, dict):
        raise MalformedPayloadError(f"{label} row must be an object")
    return cast(dict[str, Any], row)


def _text(row: dict[str, Any], key: str, label: str) -> str:
    value = row.get(key)
    if not isinstance(value, str) or not value:
        raise MalformedPayloadError(f"{label}.{key} must be a non-empty string")
    return value


def _integer(value: object, label: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise MalformedPayloadError(f"{label} must be an integer")
    return value


def _row_integer(row: dict[str, Any], key: str, label: str) -> int:
    return _integer(row.get(key), f"{label}.{key}")


def _decimal(row: dict[str, Any], key: str, label: str, *, allow_zero: bool = True) -> Decimal:
    value = row.get(key)
    if not isinstance(value, str):
        raise MalformedPayloadError(f"{label}.{key} must be a decimal string")
    try:
        parsed = Decimal(value)
    except InvalidOperation as exc:
        raise MalformedPayloadError(f"{label}.{key} is not a valid decimal") from exc
    if not parsed.is_finite() or (not allow_zero and parsed <= 0):
        raise MalformedPayloadError(f"{label}.{key} is outside the permitted decimal range")
    return parsed


def _optional_decimal(row: dict[str, Any], key: str, label: str) -> Decimal | None:
    value = row.get(key)
    if value in (None, "", "0", "0.0", "0.00000"):
        return None
    return _decimal(row, key, label)


def _position_side(value: object, label: str) -> str:
    if value not in {"BOTH", "LONG", "SHORT"}:
        raise MalformedPayloadError(f"{label}.positionSide is unknown")
    return cast(str, value)


def _parse_position(row: object) -> PositionSnapshot:
    data = _mapping(row, "position")
    return PositionSnapshot(
        symbol=_text(data, "symbol", "position"),
        position_side=cast(Any, _position_side(data.get("positionSide"), "position")),
        position_amount=_decimal(data, "positionAmt", "position"),
        entry_price=_decimal(data, "entryPrice", "position"),
        mark_price=_decimal(data, "markPrice", "position", allow_zero=False),
        unrealized_profit=_decimal(data, "unRealizedProfit", "position"),
        update_time_ms=_row_integer(data, "updateTime", "position"),
    )


def _parse_open_order(row: object) -> OpenOrderSnapshot:
    data = _mapping(row, "openOrder")
    side = data.get("side")
    if side not in {"BUY", "SELL"}:
        raise MalformedPayloadError("openOrder.side is unknown")
    close_position = _strict_bool(data, "closePosition", "openOrder")
    return OpenOrderSnapshot(
        order_id=_row_integer(data, "orderId", "openOrder"),
        symbol=_text(data, "symbol", "openOrder"),
        position_side=cast(Any, _position_side(data.get("positionSide"), "openOrder")),
        side=cast(Any, side),
        order_type=_text(data, "type", "openOrder"),
        status=_text(data, "status", "openOrder"),
        quantity=_decimal(data, "origQty", "openOrder", allow_zero=close_position),
        stop_price=_optional_decimal(data, "stopPrice", "openOrder"),
        close_position=close_position,
        reduce_only=_strict_bool(data, "reduceOnly", "openOrder"),
    )


def _parse_algo_order(row: object) -> AlgoOrderSnapshot:
    data = _mapping(row, "openAlgoOrder")
    side = data.get("side")
    if side not in {"BUY", "SELL"}:
        raise MalformedPayloadError("openAlgoOrder.side is unknown")
    close_position = _strict_bool(data, "closePosition", "openAlgoOrder")
    return AlgoOrderSnapshot(
        algo_id=_row_integer(data, "algoId", "openAlgoOrder"),
        symbol=_text(data, "symbol", "openAlgoOrder"),
        position_side=cast(Any, _position_side(data.get("positionSide"), "openAlgoOrder")),
        side=cast(Any, side),
        order_type=_text(data, "orderType", "openAlgoOrder"),
        status=_text(data, "algoStatus", "openAlgoOrder"),
        quantity=_decimal(data, "quantity", "openAlgoOrder", allow_zero=close_position),
        trigger_price=_optional_decimal(data, "triggerPrice", "openAlgoOrder"),
        close_position=close_position,
        reduce_only=_strict_bool(data, "reduceOnly", "openAlgoOrder"),
    )


def _strict_bool(row: dict[str, Any], key: str, label: str) -> bool:
    value = row.get(key)
    if not isinstance(value, bool):
        raise MalformedPayloadError(f"{label}.{key} must be boolean")
    return value


def _parse_symbol_filter(row: object) -> SymbolFilterSnapshot:
    data = _mapping(row, "symbol")
    filters = data.get("filters")
    if not isinstance(filters, list):
        raise MalformedPayloadError("symbol.filters must be an array")
    by_type: dict[str, dict[str, Any]] = {}
    for filter_row in filters:
        parsed = _mapping(filter_row, "filter")
        filter_type = _text(parsed, "filterType", "filter")
        by_type[filter_type] = parsed
    price_filter = by_type.get("PRICE_FILTER")
    lot_filter = by_type.get("LOT_SIZE")
    if price_filter is None or lot_filter is None:
        raise MalformedPayloadError("symbol lacks PRICE_FILTER or LOT_SIZE")
    return SymbolFilterSnapshot(
        symbol=_text(data, "symbol", "symbol"),
        status=_text(data, "status", "symbol"),
        price_tick_size=_decimal(price_filter, "tickSize", "PRICE_FILTER", allow_zero=False),
        quantity_step_size=_decimal(lot_filter, "stepSize", "LOT_SIZE", allow_zero=False),
        minimum_quantity=_decimal(lot_filter, "minQty", "LOT_SIZE", allow_zero=False),
    )
