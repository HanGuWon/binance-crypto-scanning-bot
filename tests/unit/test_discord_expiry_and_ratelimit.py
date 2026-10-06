from __future__ import annotations

import asyncio
from typing import Any

import httpx
import pytest
from pydantic import SecretStr
from sqlalchemy import select
from sqlalchemy.orm import Session

from conftest import make_decision
from signalbot.alerts.discord import DiscordNotifier
from signalbot.alerts.embeds import build_discord_payload
from signalbot.clock import ReplayClock
from signalbot.config import AlertSettings
from signalbot.domain.enums import Direction, SignalFamily, SignalStage
from signalbot.persistence.models import AlertRow
from signalbot.persistence.repository import OutboxCapacityError, SqlRepository

EVENT_TIME_MS = 600_000
WEBHOOK = "https://discord.test/api/webhooks/1/secret"
RISK_FAMILIES = (SignalFamily.PUMP_RISK, SignalFamily.CRASH_RISK)


def _repo() -> SqlRepository:
    repo = SqlRepository("sqlite:///:memory:")
    repo.initialize()
    return repo


def _enqueue(
    repo: SqlRepository,
    event_id: str = "event-1",
    family: SignalFamily = SignalFamily.BREAKOUT_LONG,
) -> None:
    stage = SignalStage.WATCH if family in RISK_FAMILIES else SignalStage.CONFIRMED
    direction = {
        SignalFamily.PUMP_RISK: Direction.RISK_UP,
        SignalFamily.CRASH_RISK: Direction.RISK_DOWN,
    }.get(family, Direction.LONG)
    decision = make_decision(
        event_id=event_id, family=family, stage=stage, direction=direction
    )
    repo.save_signal_and_enqueue(
        decision,
        build_discord_payload(decision, "Test Bot"),
        10,
        delivery_enabled=True,
        maximum_active_items=100,
    )


class _Harness:
    def __init__(self, handler: Any = None, **alert_overrides: Any) -> None:
        self.requests = 0
        self.repo = _repo()
        self.clock = ReplayClock(EVENT_TIME_MS)

        def default(_: httpx.Request) -> httpx.Response:
            return httpx.Response(200, json={"id": "m-1"})

        inner = handler or default

        def counting(request: httpx.Request) -> httpx.Response:
            self.requests += 1
            return inner(request)

        self.client = httpx.AsyncClient(transport=httpx.MockTransport(counting))
        settings = AlertSettings(
            discord_enabled=True,
            discord_webhook_url=SecretStr(WEBHOOK),
            **alert_overrides,
        )
        self.notifier = DiscordNotifier(settings, self.repo, self.clock, self.client)

    def status(self, event_id: str = "event-1") -> str:
        item = self.repo.get_outbox(event_id)
        assert item is not None
        return item.status

    async def close(self) -> None:
        await self.client.aclose()
        self.repo.close()


CLASSES = [
    (SignalFamily.BREAKOUT_LONG, 900),
    (SignalFamily.PUMP_RISK, 180),
    (SignalFamily.CRASH_RISK, 180),
]


@pytest.mark.asyncio
@pytest.mark.parametrize(("family", "limit_s"), CLASSES)
@pytest.mark.parametrize(("offset_ms", "expired"), [(-1, False), (0, False), (1, True)])
async def test_expiry_boundary_per_class(
    family: SignalFamily, limit_s: int, offset_ms: int, expired: bool
) -> None:
    h = _Harness()
    try:
        _enqueue(h.repo, family=family)
        h.clock.current_ms = EVENT_TIME_MS + limit_s * 1000 + offset_ms
        results = await h.notifier.dispatch_pending()
        if expired:
            assert [r.status for r in results] == ["expired"]
            assert h.status() == "expired"
            assert h.requests == 0
        else:
            assert h.status() == "delivered"
            assert h.requests == 1
    finally:
        await h.close()


@pytest.mark.asyncio
async def test_expired_is_audited_without_overwriting_and_never_sent() -> None:
    h = _Harness()
    try:
        _enqueue(h.repo)
        h.repo.record_alert("event-1", 1, "rate_limited", 5, 429, "earlier")
        h.clock.current_ms = EVENT_TIME_MS + 900_001
        await h.notifier.dispatch_pending()
        with Session(h.repo.engine) as session:
            rows = session.scalars(select(AlertRow).where(AlertRow.event_id == "event-1")).all()
        assert sorted((r.attempt, r.status) for r in rows) == [
            (1, "rate_limited"),
            (2, "expired"),
        ]
        assert h.requests == 0
        assert (await h.notifier.deliver_event("event-1")).status == "expired"
        assert h.requests == 0
    finally:
        await h.close()


@pytest.mark.asyncio
async def test_expired_excluded_from_active_count_and_pending() -> None:
    h = _Harness()
    try:
        for index in range(100):
            _enqueue(h.repo, f"event-{index}")
        overflow = make_decision(event_id="overflow")
        payload = build_discord_payload(overflow, "Test Bot")
        with pytest.raises(OutboxCapacityError):
            h.repo.save_signal_and_enqueue(
                overflow, payload, 10, delivery_enabled=True, maximum_active_items=100
            )
        h.clock.current_ms = EVENT_TIME_MS + 900_001
        await h.notifier.dispatch_pending(limit=100)
        assert h.repo.pending_outbox(100) == []
        assert h.requests == 0
        assert h.repo.save_signal_and_enqueue(
            overflow, payload, 10, delivery_enabled=True, maximum_active_items=100
        )
    finally:
        await h.close()


@pytest.mark.asyncio
async def test_old_pending_item_at_startup_is_expired_not_sent() -> None:
    h = _Harness()
    try:
        _enqueue(h.repo)
        h.clock.current_ms = EVENT_TIME_MS + 3_600_000
        h.notifier.recover_inflight()
        stop = asyncio.Event()
        task = asyncio.create_task(h.notifier.run_dispatch_loop(stop, idle_seconds=0.01))
        await asyncio.sleep(0.1)
        stop.set()
        await asyncio.wait_for(task, timeout=2)
        assert h.status() == "expired"
        assert h.requests == 0
    finally:
        await h.close()


def _rate_limited(_: httpx.Request) -> httpx.Response:
    return httpx.Response(429, json={"retry_after": 0.05, "global": False})


@pytest.mark.asyncio
async def test_repeated_429_never_dead_and_stops_at_expiry() -> None:
    h = _Harness(_rate_limited, max_attempts=2)
    try:
        _enqueue(h.repo)
        for _ in range(3):
            await h.notifier.dispatch_pending()
            assert h.status() == "pending"
            h.clock.current_ms += 301_000  # beyond the 300 s embargo cap
        assert h.clock.current_ms - EVENT_TIME_MS > 900_000
        await h.notifier.dispatch_pending()
        assert h.status() == "expired"
        with Session(h.repo.engine) as session:
            statuses = {r.status for r in session.scalars(select(AlertRow)).all()}
        assert "dead" not in statuses
    finally:
        await h.close()


@pytest.mark.asyncio
async def test_429_within_attempts_retries_then_embargoes_without_dead() -> None:
    h = _Harness(_rate_limited, max_attempts=3)
    try:
        _enqueue(h.repo)
        results = await h.notifier.dispatch_pending()
        assert [r.status for r in results] == ["rate_limited"]
        assert h.requests == 3
        assert h.status() == "pending"
    finally:
        await h.close()


@pytest.mark.asyncio
async def test_embargo_pauses_all_items_and_prevents_hot_loop() -> None:
    h = _Harness(_rate_limited, max_attempts=1)
    try:
        _enqueue(h.repo, "event-1")
        _enqueue(h.repo, "event-2")
        stop = asyncio.Event()
        task = asyncio.create_task(h.notifier.run_dispatch_loop(stop, idle_seconds=0.01))
        await asyncio.sleep(0.3)
        stop.set()
        await asyncio.wait_for(task, timeout=2)
        # one request total: the first 429 starts the embargo for every item
        assert h.requests == 1
        assert h.status("event-1") == "pending"
        assert h.status("event-2") == "pending"
        h.clock.current_ms += 301_000
        await h.notifier.dispatch_pending()
        assert h.requests >= 2
    finally:
        await h.close()


@pytest.mark.asyncio
async def test_non_429_client_error_is_still_dead() -> None:
    h = _Harness(lambda r: httpx.Response(400, text="bad"))
    try:
        _enqueue(h.repo)
        results = await h.notifier.dispatch_pending()
        assert [r.status for r in results] == ["dead"]
    finally:
        await h.close()


def test_delivery_delay_settings_bounds_and_defaults() -> None:
    settings = AlertSettings()
    assert settings.max_delivery_delay_seconds == 900
    assert settings.risk_max_delivery_delay_seconds == 180
    with pytest.raises(ValueError):
        AlertSettings(max_delivery_delay_seconds=59)
    assert AlertSettings(max_delivery_delay_seconds=60).max_delivery_delay_seconds == 60
    assert "max_delivery_delay_seconds" not in AlertSettings().model_dump()


def test_risk_limit_never_exceeds_general_limit() -> None:
    settings = AlertSettings(max_delivery_delay_seconds=60, risk_max_delivery_delay_seconds=180)
    repo = _repo()
    notifier = DiscordNotifier(settings, repo, ReplayClock(0))
    assert notifier._delivery_limit_ms("pump_risk") == 60_000
    assert notifier._delivery_limit_ms("breakout_long") == 60_000
    repo.close()


@pytest.mark.asyncio
async def test_unknown_signal_row_is_not_expired() -> None:
    h = _Harness()
    try:
        assert h.notifier._expire_if_stale("does-not-exist") is None
    finally:
        await h.close()
