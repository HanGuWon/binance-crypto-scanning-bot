from __future__ import annotations

import asyncio
from typing import Any

import pytest
from sqlalchemy.exc import DataError, OperationalError
from websockets.exceptions import ConnectionClosedError

from conftest import make_candle
from signalbot.data.candles import CandleConflictError
from signalbot.domain.enums import Market
from signalbot.errors import FATAL_PIPELINE_ERRORS, is_fatal_pipeline_error
from signalbot.exchange.binance.endpoints import WebSocketPlan
from signalbot.exchange.binance.websocket import WebSocketConsumer
from signalbot.persistence.repository import EventIdConflictError, OutboxCapacityError
from signalbot.signals.positions import PaperLifecycleBoundError

PLAN = WebSocketPlan("test", Market.SPOT, "spot", ("x",), "wss://example.test")


class _Connection:
    def __init__(self, messages: list[Any]) -> None:
        self._messages = iter(messages)

    async def __aenter__(self) -> _Connection:
        return self

    async def __aexit__(self, *_args: object) -> None:
        return None

    def __aiter__(self) -> _Connection:
        return self

    async def __anext__(self) -> str:
        try:
            value = next(self._messages)
        except StopIteration as exc:
            raise StopAsyncIteration from exc
        if isinstance(value, BaseException):
            raise value
        return value


def _consumer() -> WebSocketConsumer:
    return WebSocketConsumer(
        max_connection_age_seconds=60,
        initial_backoff_seconds=0.001,
        maximum_backoff_seconds=0.002,
    )


FATAL_INSTANCES: list[BaseException] = [
    OutboxCapacityError("full"),
    EventIdConflictError("conflict"),
    CandleConflictError(make_candle(0), make_candle(0)),
    PaperLifecycleBoundError("bound"),
    OperationalError("INSERT", {}, Exception("locked")),
    DataError("INSERT", {}, Exception("too long")),
    # Builtin classes that the old transport handler used to swallow:
    TimeoutError("handler timeout"),
    OSError("handler os error"),
]


@pytest.mark.asyncio
@pytest.mark.parametrize("error", FATAL_INSTANCES, ids=lambda e: type(e).__name__)
async def test_handler_errors_propagate_without_reconnect(
    monkeypatch: pytest.MonkeyPatch, error: BaseException
) -> None:
    connects = 0

    def fake_connect(*_a: Any, **_k: Any) -> _Connection:
        nonlocal connects
        connects += 1
        return _Connection(['{"k": 1}', '{"k": 2}'])

    monkeypatch.setattr("signalbot.exchange.binance.websocket.connect", fake_connect)
    handled = 0

    async def handler(_payload: Any) -> None:
        nonlocal handled
        handled += 1
        raise error

    with pytest.raises(type(error)) as caught:
        await asyncio.wait_for(
            _consumer().consume_forever(PLAN, handler, asyncio.Event()), timeout=2
        )
    assert caught.value is error
    assert connects == 1
    assert handled == 1


@pytest.mark.asyncio
async def test_transport_errors_still_reconnect_with_backoff(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    stop_event = asyncio.Event()
    connects = 0

    def fake_connect(*_a: Any, **_k: Any) -> _Connection:
        nonlocal connects
        connects += 1
        if connects == 1:
            return _Connection([ConnectionClosedError(None, None)])
        if connects == 2:
            return _Connection([OSError("reset")])
        stop_event.set()
        return _Connection(['{"ok": true}'])

    monkeypatch.setattr("signalbot.exchange.binance.websocket.connect", fake_connect)
    received: list[Any] = []

    async def handler(payload: Any) -> None:
        received.append(payload)

    await asyncio.wait_for(_consumer().consume_forever(PLAN, handler, stop_event), timeout=2)
    assert connects == 3
    assert received == [] or received == [{"ok": True}]


@pytest.mark.asyncio
async def test_connect_failure_reconnects(monkeypatch: pytest.MonkeyPatch) -> None:
    stop_event = asyncio.Event()
    connects = 0

    def fake_connect(*_a: Any, **_k: Any) -> _Connection:
        nonlocal connects
        connects += 1
        if connects < 3:
            raise OSError("connect refused")
        stop_event.set()
        return _Connection([])

    monkeypatch.setattr("signalbot.exchange.binance.websocket.connect", fake_connect)

    async def handler(_payload: Any) -> None:
        raise AssertionError("no payloads expected")

    await asyncio.wait_for(_consumer().consume_forever(PLAN, handler, stop_event), timeout=2)
    assert connects == 3


@pytest.mark.asyncio
async def test_stop_event_ends_consumer_promptly(monkeypatch: pytest.MonkeyPatch) -> None:
    stop_event = asyncio.Event()

    def fake_connect(*_a: Any, **_k: Any) -> _Connection:
        raise OSError("down")

    monkeypatch.setattr("signalbot.exchange.binance.websocket.connect", fake_connect)

    async def handler(_payload: Any) -> None:
        return None

    consumer = WebSocketConsumer(
        max_connection_age_seconds=60,
        initial_backoff_seconds=30,
        maximum_backoff_seconds=30,
    )
    task = asyncio.create_task(consumer.consume_forever(PLAN, handler, stop_event))
    await asyncio.sleep(0.05)
    stop_event.set()
    await asyncio.wait_for(task, timeout=1)


def test_taxonomy_classification() -> None:
    for error in FATAL_INSTANCES[:6]:
        assert is_fatal_pipeline_error(error), type(error).__name__
    assert not is_fatal_pipeline_error(OSError("transport"))
    assert not is_fatal_pipeline_error(ValueError("other"))
    assert OutboxCapacityError in FATAL_PIPELINE_ERRORS
