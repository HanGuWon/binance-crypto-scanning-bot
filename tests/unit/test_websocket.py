import asyncio
from typing import Any

import pytest

from signalbot.domain.enums import Market
from signalbot.exchange.binance.endpoints import WebSocketPlan
from signalbot.exchange.binance.websocket import WebSocketConsumer


class FakeConnection:
    def __init__(self, stop_event: asyncio.Event) -> None:
        self.stop_event = stop_event
        self.messages = iter(['{"ok": true}', "not-json"])

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_args: object) -> None:
        return None

    def __aiter__(self):
        return self

    async def __anext__(self) -> str:
        try:
            value = next(self.messages)
        except StopIteration as exc:
            self.stop_event.set()
            raise StopAsyncIteration from exc
        return value


@pytest.mark.asyncio
async def test_consumer_dispatches_valid_json_and_discards_invalid_json(monkeypatch) -> None:
    stop_event = asyncio.Event()
    connection = FakeConnection(stop_event)

    def fake_connect(*_args: Any, **_kwargs: Any) -> FakeConnection:
        return connection

    monkeypatch.setattr("signalbot.exchange.binance.websocket.connect", fake_connect)
    received: list[dict[str, bool]] = []

    async def handler(payload: Any) -> None:
        received.append(payload)

    plan = WebSocketPlan("test", Market.SPOT, "spot", ("x",), "wss://example.test")
    consumer = WebSocketConsumer(max_connection_age_seconds=60, initial_backoff_seconds=0.01)
    await consumer.consume_forever(plan, handler, stop_event)
    assert received == [{"ok": True}]
