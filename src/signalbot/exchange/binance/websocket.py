from __future__ import annotations

import asyncio
import json
import logging
import random
from collections.abc import Awaitable, Callable
from typing import Any

from websockets.asyncio.client import connect
from websockets.exceptions import ConnectionClosed

from signalbot.exchange.binance.endpoints import WebSocketPlan

LOGGER = logging.getLogger(__name__)
PayloadHandler = Callable[[Any], Awaitable[None]]


class WebSocketConsumer:
    def __init__(
        self,
        max_connection_age_seconds: int,
        initial_backoff_seconds: float = 1,
        maximum_backoff_seconds: float = 30,
        max_queue: int = 2048,
    ) -> None:
        self.max_connection_age_seconds = max_connection_age_seconds
        self.initial_backoff_seconds = initial_backoff_seconds
        self.maximum_backoff_seconds = maximum_backoff_seconds
        self.max_queue = max_queue

    async def consume_forever(
        self, plan: WebSocketPlan, handler: PayloadHandler, stop_event: asyncio.Event
    ) -> None:
        delay = self.initial_backoff_seconds
        while not stop_event.is_set():
            try:
                LOGGER.info(
                    "connecting Binance WebSocket",
                    extra={"market": plan.market.value, "stream": plan.name},
                )
                async with connect(
                    plan.url,
                    open_timeout=20,
                    close_timeout=10,
                    ping_interval=120,
                    ping_timeout=60,
                    max_queue=self.max_queue,
                    max_size=4 * 1024 * 1024,
                ) as websocket:
                    delay = self.initial_backoff_seconds
                    async with asyncio.timeout(self.max_connection_age_seconds):
                        async for raw in websocket:
                            if stop_event.is_set():
                                return
                            try:
                                payload = json.loads(raw)
                            except (TypeError, json.JSONDecodeError):
                                LOGGER.warning(
                                    "discarding invalid WebSocket JSON",
                                    extra={"market": plan.market.value, "stream": plan.name},
                                )
                                continue
                            await handler(payload)
            except asyncio.CancelledError:
                raise
            except TimeoutError:
                LOGGER.info(
                    "recycling Binance WebSocket",
                    extra={"market": plan.market.value, "stream": plan.name},
                )
            except (ConnectionClosed, OSError, RuntimeError) as exc:
                LOGGER.warning(
                    "Binance WebSocket disconnected",
                    extra={"market": plan.market.value, "stream": plan.name},
                    exc_info=exc,
                )
            if stop_event.is_set():
                return
            try:
                await asyncio.wait_for(stop_event.wait(), timeout=delay * random.uniform(0.8, 1.2))
            except TimeoutError:
                pass
            delay = min(delay * 2, self.maximum_backoff_seconds)
