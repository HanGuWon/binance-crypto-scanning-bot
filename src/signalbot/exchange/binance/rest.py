from __future__ import annotations

import asyncio
import logging
from typing import Any

import httpx

from signalbot.domain.enums import Market
from signalbot.domain.models import Candle
from signalbot.exchange.binance.endpoints import (
    exchange_info_path,
    funding_rate_history_path,
    klines_path,
    rest_base,
    ticker_24h_path,
)
from signalbot.exchange.binance.schemas import parse_rest_kline

LOGGER = logging.getLogger(__name__)


class BinanceRestError(RuntimeError):
    pass


class BinanceRestClient:
    def __init__(
        self,
        market: Market,
        timeout_seconds: float = 15,
        max_attempts: int = 4,
        client: httpx.AsyncClient | None = None,
    ) -> None:
        self.market = market
        self.max_attempts = max_attempts
        self._owns_client = client is None
        self._client = client or httpx.AsyncClient(
            base_url=rest_base(market),
            timeout=httpx.Timeout(timeout_seconds),
            headers={"User-Agent": "binance-signal-bot/0.1"},
        )

    async def __aenter__(self) -> BinanceRestClient:
        return self

    async def __aexit__(self, *_: object) -> None:
        await self.close()

    async def close(self) -> None:
        if self._owns_client:
            await self._client.aclose()

    async def _get_json(self, path: str, params: dict[str, Any] | None = None) -> Any:
        delay = 0.5
        last_error: Exception | None = None
        for attempt in range(1, self.max_attempts + 1):
            try:
                response = await self._client.get(path, params=params)
                if response.status_code in {418, 429}:
                    raise BinanceRestError(f"Binance rate limit status={response.status_code}")
                response.raise_for_status()
                return response.json()
            except (httpx.HTTPError, ValueError, BinanceRestError) as exc:
                last_error = exc
                if attempt == self.max_attempts:
                    break
                LOGGER.warning(
                    "Binance REST request failed; retrying",
                    extra={"market": self.market.value, "attempt": attempt},
                    exc_info=exc,
                )
                await asyncio.sleep(delay)
                delay = min(delay * 2, 8)
        raise BinanceRestError(f"Binance REST request failed: {path}") from last_error

    async def exchange_info(self) -> dict[str, Any]:
        payload = await self._get_json(exchange_info_path(self.market))
        if not isinstance(payload, dict):
            raise BinanceRestError("exchangeInfo response must be an object")
        return payload

    async def tickers_24h(self) -> list[dict[str, Any]]:
        payload = await self._get_json(ticker_24h_path(self.market))
        if not isinstance(payload, list):
            raise BinanceRestError("24h ticker response must be an array")
        return [item for item in payload if isinstance(item, dict)]

    async def klines(
        self,
        symbol: str,
        interval: str,
        limit: int = 500,
        start_time_ms: int | None = None,
        end_time_ms: int | None = None,
        now_ms: int | None = None,
    ) -> list[Candle]:
        maximum_limit = 1000 if self.market is Market.SPOT else 1500
        params: dict[str, Any] = {
            "symbol": symbol.upper(),
            "interval": interval,
            "limit": min(max(limit, 1), maximum_limit),
        }
        if start_time_ms is not None:
            params["startTime"] = start_time_ms
        if end_time_ms is not None:
            params["endTime"] = end_time_ms
        payload = await self._get_json(klines_path(self.market), params=params)
        if not isinstance(payload, list):
            raise BinanceRestError("klines response must be an array")
        candles = [
            parse_rest_kline(self.market, symbol.upper(), interval, row)
            for row in payload
            if isinstance(row, list)
        ]
        return [c for c in candles if now_ms is None or c.close_time_ms < now_ms]

    async def funding_rates(
        self,
        symbol: str,
        start_time_ms: int | None = None,
        end_time_ms: int | None = None,
        limit: int = 1000,
    ) -> list[dict[str, Any]]:
        if self.market is not Market.FUTURES:
            raise BinanceRestError("funding-rate history is available only for futures")
        params: dict[str, Any] = {
            "symbol": symbol.upper(),
            "limit": min(max(limit, 1), 1000),
        }
        if start_time_ms is not None:
            params["startTime"] = start_time_ms
        if end_time_ms is not None:
            params["endTime"] = end_time_ms
        payload = await self._get_json(
            funding_rate_history_path(),
            params=params,
        )
        if not isinstance(payload, list):
            raise BinanceRestError("fundingRate response must be an array")
        return [item for item in payload if isinstance(item, dict)]
