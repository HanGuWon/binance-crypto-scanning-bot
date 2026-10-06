from __future__ import annotations

import asyncio
import logging
from typing import Any

import httpx
import pytest
from pydantic import SecretStr
from sqlalchemy.exc import OperationalError

from conftest import make_decision
from signalbot.alerts.discord import DeliveryResult, DiscordNotifier
from signalbot.alerts.embeds import build_discord_payload
from signalbot.clock import ReplayClock
from signalbot.config import AlertSettings, Settings
from signalbot.persistence.repository import SqlRepository

WEBHOOK = "https://discord.test/api/webhooks/1/secret"


def _repo() -> SqlRepository:
    repo = SqlRepository("sqlite:///:memory:")
    repo.initialize()
    return repo


def _enqueue(repo: SqlRepository, event_id: str) -> None:
    decision = make_decision(event_id=event_id)
    repo.save_signal_and_enqueue(
        decision,
        build_discord_payload(decision, "Test Bot"),
        10,
        delivery_enabled=True,
        maximum_active_items=100,
    )


def _notifier(repo: SqlRepository, client: httpx.AsyncClient) -> DiscordNotifier:
    settings = AlertSettings(
        discord_enabled=True, discord_webhook_url=SecretStr(WEBHOOK)
    )
    return DiscordNotifier(settings, repo, ReplayClock(10), client)


def _db_error() -> OperationalError:
    return OperationalError("SELECT 1", {}, Exception("database is locked"))


@pytest.mark.asyncio
async def test_transient_error_is_logged_and_loop_recovers(
    caplog: pytest.LogCaptureFixture,
) -> None:
    stop_event = asyncio.Event()

    async def handler(_: httpx.Request) -> httpx.Response:
        stop_event.set()
        return httpx.Response(200, json={"id": "m1"})

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    repo = _repo()
    _enqueue(repo, "event-1")
    notifier = _notifier(repo, client)
    real_pending = repo.pending_outbox
    calls = 0

    def flaky(limit: int = 100) -> Any:
        nonlocal calls
        calls += 1
        if calls == 1:
            raise _db_error()
        return real_pending(limit)

    repo.pending_outbox = flaky  # type: ignore[method-assign]
    try:
        with caplog.at_level(logging.INFO):
            await asyncio.wait_for(
                notifier.run_dispatch_loop(
                    stop_event,
                    idle_seconds=0.01,
                    error_backoff_initial_seconds=0.01,
                    error_backoff_max_seconds=0.02,
                ),
                timeout=5,
            )
        assert repo.get_outbox("event-1").status == "delivered"  # type: ignore[union-attr]
        errors = [r for r in caplog.records if r.levelno == logging.ERROR]
        assert len(errors) == 1
        assert errors[0].exc_info is not None
    finally:
        await client.aclose()
        repo.close()


@pytest.mark.asyncio
async def test_persistent_error_uses_bounded_backoff_not_a_hot_loop(
    caplog: pytest.LogCaptureFixture,
) -> None:
    stop_event = asyncio.Event()
    client = httpx.AsyncClient(transport=httpx.MockTransport(lambda r: httpx.Response(200)))
    repo = _repo()
    notifier = _notifier(repo, client)
    calls = 0

    async def always_fail(limit: int = 100) -> list[DeliveryResult]:
        nonlocal calls
        calls += 1
        raise _db_error()

    notifier.dispatch_pending = always_fail  # type: ignore[method-assign]
    try:
        with caplog.at_level(logging.ERROR):
            task = asyncio.create_task(
                notifier.run_dispatch_loop(
                    stop_event,
                    error_backoff_initial_seconds=0.05,
                    error_backoff_max_seconds=0.1,
                )
            )
            await asyncio.sleep(0.5)
            stop_event.set()
            await asyncio.wait_for(task, timeout=2)
        # delays 0.05, 0.1, 0.1, ... => about 6 calls in 0.5s; a hot loop would be
        # thousands. Lower bound proves the loop kept retrying.
        assert 3 <= calls <= 12
        assert len([r for r in caplog.records if r.levelno == logging.ERROR]) == calls
    finally:
        await client.aclose()
        repo.close()


@pytest.mark.asyncio
async def test_stop_event_interrupts_error_backoff() -> None:
    stop_event = asyncio.Event()
    client = httpx.AsyncClient(transport=httpx.MockTransport(lambda r: httpx.Response(200)))
    repo = _repo()
    notifier = _notifier(repo, client)

    async def always_fail(limit: int = 100) -> list[DeliveryResult]:
        raise _db_error()

    notifier.dispatch_pending = always_fail  # type: ignore[method-assign]
    try:
        task = asyncio.create_task(notifier.run_dispatch_loop(stop_event))
        await asyncio.sleep(0.05)
        stop_event.set()
        await asyncio.wait_for(task, timeout=1)  # default backoff is 1s+, stop is prompt
    finally:
        await client.aclose()
        repo.close()


@pytest.mark.asyncio
async def test_cancellation_is_not_swallowed() -> None:
    stop_event = asyncio.Event()
    client = httpx.AsyncClient(transport=httpx.MockTransport(lambda r: httpx.Response(200)))
    repo = _repo()
    notifier = _notifier(repo, client)
    entered = asyncio.Event()

    async def hang(limit: int = 100) -> list[DeliveryResult]:
        entered.set()
        await asyncio.Event().wait()
        return []

    notifier.dispatch_pending = hang  # type: ignore[method-assign]
    try:
        task = asyncio.create_task(notifier.run_dispatch_loop(stop_event))
        await asyncio.wait_for(entered.wait(), timeout=1)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
    finally:
        await client.aclose()
        repo.close()


@pytest.mark.asyncio
async def test_normal_stop_logs_no_errors(caplog: pytest.LogCaptureFixture) -> None:
    stop_event = asyncio.Event()
    client = httpx.AsyncClient(transport=httpx.MockTransport(lambda r: httpx.Response(200)))
    repo = _repo()
    notifier = _notifier(repo, client)
    try:
        with caplog.at_level(logging.INFO):
            task = asyncio.create_task(
                notifier.run_dispatch_loop(stop_event, idle_seconds=0.01)
            )
            await asyncio.sleep(0.05)
            stop_event.set()
            await asyncio.wait_for(task, timeout=1)
        assert not [r for r in caplog.records if r.levelno >= logging.WARNING]
    finally:
        await client.aclose()
        repo.close()


@pytest.mark.parametrize(
    "kwargs",
    [
        {"error_backoff_initial_seconds": 0.0},
        {"error_backoff_initial_seconds": 2.0, "error_backoff_max_seconds": 1.0},
    ],
)
@pytest.mark.asyncio
async def test_invalid_backoff_limits_are_rejected(kwargs: dict[str, Any]) -> None:
    client = httpx.AsyncClient(transport=httpx.MockTransport(lambda r: httpx.Response(200)))
    repo = _repo()
    notifier = _notifier(repo, client)
    try:
        with pytest.raises(ValueError):
            await notifier.run_dispatch_loop(asyncio.Event(), **kwargs)
    finally:
        await client.aclose()
        repo.close()


# ---------------------------------------------------------------------------
# Application supervision
# ---------------------------------------------------------------------------


class _StubScanner:
    def __init__(self, market: Any, stop_event: asyncio.Event) -> None:
        self.market = market
        self._stop_event = stop_event

    async def run(self) -> None:
        await self._stop_event.wait()

    async def close(self) -> None:
        return None


class _StubNotifier:
    mode = "return"
    startup_dispatch_calls = 0

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        pass

    def recover_inflight(self) -> int:
        return 0

    async def dispatch_pending(self, limit: int = 100) -> list[DeliveryResult]:
        _StubNotifier.startup_dispatch_calls += 1
        await asyncio.Event().wait()  # a stalled Discord must never block startup
        return []

    async def close(self) -> None:
        return None

    async def run_dispatch_loop(self, stop_event: asyncio.Event, **_: Any) -> None:
        if self.mode == "raise":
            raise RuntimeError("drain exploded")
        if self.mode == "return":
            return
        await stop_event.wait()


def _make_app(tmp_path: Any, monkeypatch: pytest.MonkeyPatch, mode: str) -> Any:
    import signalbot.app as app_module
    from signalbot.app import SignalApplication

    settings = Settings.model_validate(
        {
            "binance": {"markets": ["spot", "futures"]},
            "alerts": {
                "discord_enabled": True,
                "discord_webhook_url": WEBHOOK,
            },
            "storage": {"url": f"sqlite:///{tmp_path / 'app.db'}"},
        }
    )
    _StubNotifier.mode = mode
    monkeypatch.setattr(app_module, "DiscordNotifier", _StubNotifier)
    monkeypatch.setattr(
        app_module,
        "MarketScanner",
        lambda market, settings, clock, runtime, stop_event, **kw: _StubScanner(
            market, stop_event
        ),
    )
    return SignalApplication(settings)


@pytest.mark.asyncio
async def test_unexpected_drain_return_fails_closed(
    tmp_path: Any, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    app = _make_app(tmp_path, monkeypatch, "return")
    with caplog.at_level(logging.INFO):
        with pytest.raises(RuntimeError, match="without a stop request"):
            await asyncio.wait_for(app.run(), timeout=15)
    assert app.stop_event.is_set()
    assert any(
        r.levelno == logging.CRITICAL and "outbox drain" in r.getMessage()
        for r in caplog.records
    )


@pytest.mark.asyncio
async def test_drain_exception_fails_closed_and_is_logged_once(
    tmp_path: Any, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    app = _make_app(tmp_path, monkeypatch, "raise")
    with caplog.at_level(logging.INFO):
        with pytest.raises(RuntimeError, match="drain exploded"):
            await asyncio.wait_for(app.run(), timeout=15)
    assert app.stop_event.is_set()
    critical = [r for r in caplog.records if r.levelno == logging.CRITICAL]
    assert any(r.exc_info and "drain exploded" in str(r.exc_info[1]) for r in critical)
    teardown_dupes = [
        r for r in caplog.records if "during teardown" in r.getMessage()
    ]
    assert not teardown_dupes


@pytest.mark.asyncio
async def test_normal_stop_with_healthy_drain_logs_no_errors(
    tmp_path: Any, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    app = _make_app(tmp_path, monkeypatch, "wait")
    app.stop_after_minutes = 0
    with caplog.at_level(logging.INFO):
        await asyncio.wait_for(app.run(), timeout=15)
    assert not [r for r in caplog.records if r.levelno >= logging.ERROR]


@pytest.mark.asyncio
async def test_startup_does_not_await_dispatch_pending_before_scanners(
    tmp_path: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    app = _make_app(tmp_path, monkeypatch, "wait")
    _StubNotifier.startup_dispatch_calls = 0
    app.stop_after_minutes = 0
    await asyncio.wait_for(app.run(), timeout=15)
    assert len(app.scanners) == 2
    assert _StubNotifier.startup_dispatch_calls == 0
