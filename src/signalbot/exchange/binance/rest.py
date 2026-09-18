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


class BinanceRateLimitError(BinanceRestError):
    def __init__(
        self,
        status_code: int,
        retry_after_seconds: float | None,
    ) -> None:
        self.status_code = status_code
        self.retry_after_seconds = retry_after_seconds
        retry_text = (
            "unknown"
            if retry_after_seconds is None
            else f"{retry_after_seconds:g}s"
        )
        super().__init__(
            f"Binance rate limit status={status_code} retry_after={retry_text}"
        )


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
        self._rate_limit_lock = asyncio.Lock()
        self._embargo_until = 0.0
        self._used_weight_headers: dict[str, int] = {}

    async def __aenter__(self) -> BinanceRestClient:
        return self

    async def __aexit__(self, *_: object) -> None:
        await self.close()

    async def close(self) -> None:
        if self._owns_client:
            await self._client.aclose()

    @property
    def used_weight_headers(self) -> dict[str, int]:
        """Latest Binance IP request-weight counters observed on this client."""

        return dict(self._used_weight_headers)

    @property
    def rate_limit_embargo_remaining_seconds(self) -> float:
        try:
            now = asyncio.get_running_loop().time()
        except RuntimeError:
            return 0.0
        return max(0.0, self._embargo_until - now)

    async def _wait_for_rate_limit_embargo(self) -> None:
        while True:
            async with self._rate_limit_lock:
                delay = max(
                    0.0,
                    self._embargo_until - asyncio.get_running_loop().time(),
                )
            if delay <= 0:
                return
            await asyncio.sleep(delay)

    async def _extend_rate_limit_embargo(self, seconds: float) -> None:
        if seconds <= 0:
            return
        deadline = asyncio.get_running_loop().time() + seconds
        async with self._rate_limit_lock:
            self._embargo_until = max(self._embargo_until, deadline)

    def _capture_used_weight(self, response: httpx.Response) -> None:
        for name, raw_value in response.headers.items():
            normalized = name.upper()
            if not normalized.startswith("X-MBX-USED-WEIGHT"):
                continue
            try:
                self._used_weight_headers[normalized] = int(raw_value)
            except ValueError:
                LOGGER.debug(
                    "ignoring malformed Binance used-weight header",
                    extra={"market": self.market.value, "header": normalized},
                )

    @staticmethod
    def _retry_after_seconds(response: httpx.Response) -> float | None:
        raw_value = response.headers.get("Retry-After")
        if raw_value is None:
            return None
        try:
            return max(0.0, float(raw_value))
        except ValueError:
            return None

    async def _get_json(self, path: str, params: dict[str, Any] | None = None) -> Any:
        delay = 0.5
        last_error: Exception | None = None
        for attempt in range(1, self.max_attempts + 1):
            await self._wait_for_rate_limit_embargo()
            try:
                response = await self._client.get(path, params=params)
                self._capture_used_weight(response)
                if response.status_code in {418, 429}:
                    retry_after = self._retry_after_seconds(response)
                    fallback = 120.0 if response.status_code == 418 else delay
                    await self._extend_rate_limit_embargo(
                        retry_after if retry_after is not None else fallback
                    )
                    rate_error = BinanceRateLimitError(
                        response.status_code,
                        retry_after,
                    )
                    last_error = rate_error
                    LOGGER.warning(
                        "Binance REST rate limited; applying client-wide embargo",
                        extra={
                            "market": self.market.value,
                            "attempt": attempt,
                            "status_code": response.status_code,
                            "retry_after_seconds": retry_after,
                        },
                    )
                    if response.status_code == 418 or attempt == self.max_attempts:
                        break
                    delay = min(delay * 2, 8)
                    continue
                response.raise_for_status()
                return response.json()
            except (httpx.HTTPError, ValueError) as exc:
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
        if isinstance(last_error, BinanceRateLimitError):
            raise last_error
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

    async def earliest_kline_open_time_ms(
        self,
        symbol: str,
        *,
        interval: str = "1d",
    ) -> int | None:
        """Return the earliest public kline open time available for a symbol.

        This is a conservative public-data age anchor for Spot, whose
        exchangeInfo payload does not expose a listing/onboard timestamp.
        """

        payload = await self._get_json(
            klines_path(self.market),
            params={
                "symbol": symbol.upper(),
                "interval": interval,
                "startTime": 0,
                "limit": 1,
            },
        )
        if not isinstance(payload, list):
            raise BinanceRestError("klines response must be an array")
        if not payload:
            return None
        first = payload[0]
        if not isinstance(first, list) or not first:
            raise BinanceRestError("earliest kline response row is malformed")
        try:
            return int(first[0])
        except (TypeError, ValueError) as exc:
            raise BinanceRestError("earliest kline open time is malformed") from exc

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
