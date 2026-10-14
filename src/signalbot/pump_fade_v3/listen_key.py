"""Narrow listenKey lifecycle client; it has no signing secret or other routes."""

from __future__ import annotations

from typing import Literal, cast

import httpx
from pydantic import SecretStr

from position_guardian.exchange.binance_read import PRODUCTION_REST_BASE_URL

LISTEN_KEY_PATH = "/fapi/v1/listenKey"
_ALLOWED_METHODS = {"POST", "PUT", "DELETE"}


class ListenKeyError(RuntimeError):
    def __init__(self, status_code: int) -> None:
        super().__init__(f"listenKey request failed with HTTP {status_code}")
        self.status_code = status_code


class ListenKeyClient:
    def __init__(self, api_key: SecretStr, client: httpx.AsyncClient) -> None:
        if not api_key.get_secret_value().strip():
            raise ValueError("api_key must be non-empty")
        self._api_key = api_key
        self._client = client

    async def create(self) -> SecretStr:
        response = await self._request("POST")
        payload = response.json()
        if not isinstance(payload, dict) or not isinstance(payload.get("listenKey"), str):
            raise ListenKeyError(response.status_code)
        key = payload["listenKey"].strip()
        if not key:
            raise ListenKeyError(response.status_code)
        return SecretStr(key)

    async def keep_alive(self) -> None:
        await self._request("PUT")

    async def close(self) -> None:
        await self._request("DELETE")

    async def _request(self, method: Literal["POST", "PUT", "DELETE"]) -> httpx.Response:
        if method not in _ALLOWED_METHODS:
            raise ValueError("unsupported listenKey method")
        response = await self._client.request(
            method,
            f"{PRODUCTION_REST_BASE_URL}{LISTEN_KEY_PATH}",
            headers={"X-MBX-APIKEY": self._api_key.get_secret_value()},
        )
        if response.status_code >= 400:
            raise ListenKeyError(response.status_code)
        return cast(httpx.Response, response)
