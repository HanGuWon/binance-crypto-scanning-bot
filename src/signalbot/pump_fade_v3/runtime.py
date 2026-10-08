"""Disabled-by-default, capture-only Pump Fade v3 runtime composition.

The existing capture pipeline owns durable receipt sequencing and writes. REST
attempts are delegated to its receipt-stamping adapter, and WebSocket plans to
its owner. No scanner, shared DB, execution or notification clients are used.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Protocol

from signalbot.capture.models import RestEnvelopeV2
from signalbot.capture.rest_scheduler import RestAttemptCapture
from signalbot.domain.enums import Market
from signalbot.exchange.binance.endpoints import WebSocketPlan
from signalbot.pump_fade_v3.collector import EventSymbolSet, build_websocket_plans
from signalbot.pump_fade_v3.config import CollectorConfig
from signalbot.pump_fade_v3.rest_budget import RestWeightBudget

_FAPI = "https://fapi.binance.com"


@dataclass(frozen=True, slots=True)
class ScheduledPoll:
    role: str
    path: str
    query: tuple[tuple[str, str], ...]
    interval_ms: int
    weight: int


def rest_poll_plan(symbols: tuple[str, ...]) -> tuple[ScheduledPoll, ...]:
    if len(symbols) > 25 or len(symbols) != len(set(symbols)):
        raise ValueError("at most 25 distinct event symbols")
    result = [
        ScheduledPoll("premium_all", "/fapi/v1/premiumIndex", (), 60_000, 10),
        ScheduledPoll("funding_all", "/fapi/v1/fundingInfo", (), 300_000, 0),
        ScheduledPoll("exchange_all", "/fapi/v1/exchangeInfo", (), 1_800_000, 1),
    ]
    result.extend(ScheduledPoll(f"oi_{symbol}", "/fapi/v1/openInterest",
                                (("symbol", symbol),), 60_000, 1) for symbol in symbols)
    return tuple(result)


class WebSocketPlanOwner(Protocol):
    """Existing capture WebSocket owner integration boundary."""

    def install(self, plans: tuple[WebSocketPlan, ...], *, generation: int,
                plan_sha256: str) -> None: ...


@dataclass(frozen=True, slots=True)
class RuntimePlan:
    generation: int
    sha256: str
    websocket: tuple[WebSocketPlan, ...]
    rest: tuple[ScheduledPoll, ...]


class PumpFadeV3Runtime:
    """Deterministic, in-memory orchestration; outer host owns IO and lifecycle.

    Call observe_ticker for each USD-M `!miniTicker@arr` constituent (st=1).
    A miniTicker frame represents changes only; snapshot() emits the current
    admitted view at most once per minute. `!forceOrder@arr` is censored: an
    absent event never proves the absence of liquidation activity.
    """

    def __init__(self, config: CollectorConfig, *, ws_owner: WebSocketPlanOwner,
                 rest_adapter: RestAttemptCapture, budget: RestWeightBudget,
                 monotonic: Callable[[], float] = time.monotonic,
                 sleep: Callable[[float], Awaitable[None]] = asyncio.sleep) -> None:
        self.config = config
        self.ws_owner = ws_owner
        self.rest_adapter = rest_adapter
        self.budget = budget
        self._monotonic = monotonic
        self._sleep = sleep
        self._rest_lock = asyncio.Lock()
        self.events = EventSymbolSet(config)
        self._plan: RuntimePlan | None = None
        self._last_snapshot_bucket: int | None = None
        self._last_due: dict[str, int] = {}

    @property
    def plan(self) -> RuntimePlan | None:
        return self._plan

    def observe_ticker(self, row: object, *, now_ms: int) -> RuntimePlan | None:
        if not self.config.enabled:
            return None
        update = self.events.observe(row, now_ms=now_ms)
        return self._refresh(update.active_symbols)

    def refresh(self, *, now_ms: int) -> RuntimePlan | None:
        if not self.config.enabled:
            return None
        return self._refresh(self.events.active_symbols(now_ms=now_ms))

    def snapshot(self, *, now_ms: int) -> tuple[str, ...] | None:
        """One bounded all-market miniTicker observation snapshot per UTC minute."""
        if not self.config.enabled:
            return None
        bucket = now_ms // 60_000
        if self._last_snapshot_bucket is not None and bucket <= self._last_snapshot_bucket:
            return None
        self._last_snapshot_bucket = bucket
        return self.events.active_symbols(now_ms=now_ms)

    @staticmethod
    def liquidation_is_censored() -> bool:
        return True

    def _refresh(self, symbols: tuple[str, ...]) -> RuntimePlan:
        websocket = build_websocket_plans(
            symbols, include_event_depth=self.config.event_depth_enabled
        )
        rest = rest_poll_plan(symbols)
        payload = json.dumps(
            {"websocket": [(p.route, p.streams) for p in websocket],
             "rest": [(p.role, p.path, p.query, p.interval_ms) for p in rest]},
            separators=(",", ":"), sort_keys=True,
        )
        digest = hashlib.sha256(payload.encode()).hexdigest()
        if self._plan is not None and self._plan.sha256 == digest:
            return self._plan
        generation = 1 if self._plan is None else self._plan.generation + 1
        next_plan = RuntimePlan(generation, digest, websocket, rest)
        self.ws_owner.install(websocket, generation=generation, plan_sha256=digest)
        self._plan = next_plan
        return next_plan

    def due_polls(self, *, now_ms: int) -> tuple[ScheduledPoll, ...]:
        if not self.config.enabled:
            return ()
        plan = self.refresh(now_ms=now_ms)
        if plan is None:
            return ()
        return tuple(
            p for p in plan.rest
            if now_ms >= self._last_due.get(p.role, -p.interval_ms) + p.interval_ms
        )

    async def attempt_poll(self, poll: ScheduledPoll, *, now_ms: int,
                           attempt: int = 1) -> RestEnvelopeV2 | None:
        if not self.config.enabled:
            return None
        if self._plan is None or poll not in self._plan.rest:
            raise ValueError("poll absent from active plan")
        if attempt != 1:
            raise ValueError("runtime owns retry attempts starting at one")
        async with self._rest_lock:
            return await self._attempt_poll_locked(poll, now_ms)

    async def _attempt_poll_locked(
        self, poll: ScheduledPoll, now_ms: int
    ) -> RestEnvelopeV2 | None:
        assert self._plan is not None
        correlation = f"v3:{self._plan.sha256}:{poll.role}:{now_ms}"
        for index in range(1, 4):
            if poll.weight and not self.budget.admit(
                weight=poll.weight, now=self._monotonic()
            ):
                return None
            envelope = await self.rest_adapter.capture_attempt(
                method="GET", market=Market.FUTURES, url=_FAPI + poll.path,
                request_role=poll.role, correlation_id=correlation,
                attempt=index, query=poll.query,
            )
            self.budget.observe(
                status=envelope.response_status,
                headers=dict(envelope.response_headers), now=self._monotonic(),
            )
            decision = self.budget.retry(
                status=envelope.response_status, attempt=index,
                headers=dict(envelope.response_headers),
            )
            if not decision.retry:
                self._last_due[poll.role] = now_ms
                return envelope
            await self._sleep(decision.delay_seconds)
        raise RuntimeError("unreachable bounded REST retry state")
