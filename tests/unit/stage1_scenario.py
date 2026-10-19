"""Deterministic MarketRuntime scenario shared by the stage-1 golden and behaviour tests.

The scenario feeds synthetic closed 5m candles through the real ``MarketRuntime``
(real indicators, state machine, PAPER lifecycle, repository and payload builder).
Only the rule engine is scripted so the trigger bars are exact. No network.
"""

from __future__ import annotations

import json
import math
from collections.abc import Iterable
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from signalbot.clock import ReplayClock
from signalbot.config import Settings
from signalbot.data.candles import CandleGap
from signalbot.domain.enums import Direction, Market, SignalFamily
from signalbot.domain.models import Candle, FeatureSnapshot, RuleEvaluation, SignalDecision
from signalbot.persistence.models import AlertOutboxRow, SignalRow
from signalbot.persistence.repository import SqlRepository
from signalbot.runtime import MarketRuntime

STEP_MS = 300_000
T0_MS = 5_866_667 * STEP_MS
SYMBOL = "BTCUSDT"
BAR_COUNT = 380
DEFAULT_TRIGGERS = (250, 258, 266, 300)


def price_path(index: int) -> float:
    """Slow decline to bar 300, then a rebound; small deterministic oscillation."""

    trend = 100.0 - 0.06 * min(index, 300) + 0.09 * max(0, index - 300)
    return round(trend + 0.35 * math.sin(index / 3.0), 4)


def make_bar(index: int, *, symbol: str = SYMBOL, market: Market = Market.FUTURES) -> Candle:
    close = price_path(index)
    open_ = price_path(index - 1) if index > 0 else close
    high = max(open_, close) + 0.15
    low = min(open_, close) - 0.15
    volume = 100.0 + (index % 7)
    return Candle(
        market=market,
        symbol=symbol,
        interval="5m",
        open_time_ms=T0_MS + index * STEP_MS,
        close_time_ms=T0_MS + (index + 1) * STEP_MS - 1,
        open=Decimal(str(open_)),
        high=Decimal(str(round(high, 4))),
        low=Decimal(str(round(low, 4))),
        close=Decimal(str(close)),
        volume=Decimal(str(volume)),
        quote_volume=Decimal(str(round(volume * close, 4))),
        trade_count=100 + index % 11,
        taker_buy_base_volume=Decimal(str(round(volume * 0.45, 4))),
        taker_buy_quote_volume=Decimal(str(round(volume * 0.45 * close, 4))),
        is_closed=True,
    )


def make_settings(overrides: dict[str, Any] | None = None) -> Settings:
    data: dict[str, Any] = {
        "rule_version": "stage1-golden-v1",
        "binance": {
            "markets": ["futures"],
            "intervals": ["5m", "15m", "1h"],
            "primary_interval": "5m",
        },
        "storage": {"url": "sqlite:///:memory:"},
        "signals": {"technical_exit": {"enabled": True}},
        "alerts": {
            "discord_enabled": True,
            "discord_webhook_url": "https://example.invalid/hook",
        },
        "runtime": {"persist_candles": False},
    }
    for section, values in (overrides or {}).items():
        merged = dict(data.get(section, {}))
        merged.update(values)
        data[section] = merged
    return Settings.model_validate(data)


@dataclass(slots=True)
class ScenarioResult:
    signals: list[dict[str, Any]]
    outbox: list[dict[str, Any]]
    emitted: list[SignalDecision]
    runtime: MarketRuntime
    repository: SqlRepository
    recoverer_calls: list[CandleGap] = field(default_factory=list)

    def canonical(self) -> str:
        return json.dumps(
            {"signals": self.signals, "outbox": self.outbox},
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
        )

    def close(self) -> None:
        self.runtime.close_public_structure_flow_shadow()
        self.repository.close()


def scripted_trigger(feature: FeatureSnapshot, family: SignalFamily) -> RuleEvaluation:
    short = family is SignalFamily.BREAKDOWN_SHORT
    atr = feature.atr
    invalidation = feature.price + atr if short else feature.price - atr
    return RuleEvaluation(
        market=feature.market,
        symbol=feature.symbol,
        family=family,
        direction=Direction.SHORT if short else Direction.LONG,
        timeframe=feature.interval,
        event_time_ms=feature.event_time_ms,
        score=90,
        triggered=True,
        eligible=True,
        price=Decimal(str(round(feature.price, 8))),
        reasons=("closed below 20-bar low" if short else "closed above 20-bar high",),
        invalidation=Decimal(str(round(invalidation, 8))),
        regime=feature.regime,
        metadata={},
    )


def idle_evaluation(feature: FeatureSnapshot, family: SignalFamily) -> RuleEvaluation:
    """Score-0 evaluation, as the real rule engine emits on non-trigger bars (re-arms IDLE)."""

    short = family is SignalFamily.BREAKDOWN_SHORT
    return RuleEvaluation(
        market=feature.market,
        symbol=feature.symbol,
        family=family,
        direction=Direction.SHORT if short else Direction.LONG,
        timeframe=feature.interval,
        event_time_ms=feature.event_time_ms,
        score=0,
        triggered=False,
        eligible=False,
        price=Decimal(str(round(feature.price, 8))),
        reasons=("no trigger",),
        invalidation=None,
        regime=feature.regime,
        metadata={},
    )


class Scenario:
    """Runtime plus a deterministic bar feed that can be continued step by step."""

    def __init__(
        self,
        overrides: dict[str, Any] | None = None,
        *,
        triggers: Iterable[int] = DEFAULT_TRIGGERS,
        missing: Iterable[int] = (),
        recover: str = "ok",
        family: SignalFamily = SignalFamily.BREAKDOWN_SHORT,
    ) -> None:
        self.settings = make_settings(overrides)
        self.repository = SqlRepository(self.settings.storage.url)
        self.repository.initialize()
        self.trigger_close_times = {make_bar(index).close_time_ms for index in triggers}
        self.missing = set(missing)
        self.recover = recover
        self.family = family
        self.emitted: list[SignalDecision] = []
        self.calls: list[CandleGap] = []
        self.clock = ReplayClock(make_bar(0).close_time_ms)
        self.runtime = MarketRuntime(
            Market.FUTURES,
            self.settings,
            self.repository,
            self.clock,
            self._handler,
            None if recover == "none" else self._recoverer,
        )
        self.runtime.set_active_symbols(frozenset({SYMBOL}), frozenset({SYMBOL}))
        self.runtime.rule_engine.evaluate = self._scripted  # type: ignore[method-assign]

    async def _handler(self, decision: SignalDecision) -> None:
        self.emitted.append(decision)

    async def _recoverer(self, gap: CandleGap) -> list[Candle]:
        self.calls.append(gap)
        if self.recover == "empty":
            return []
        return [
            make_bar((open_ms - T0_MS) // STEP_MS)
            for open_ms in range(gap.start_time_ms, gap.end_time_ms + 1, STEP_MS)
        ]

    def _scripted(
        self, feature: FeatureSnapshot, contexts: Any = None
    ) -> list[RuleEvaluation]:
        if feature.event_time_ms in self.trigger_close_times:
            return [scripted_trigger(feature, self.family)]
        return [idle_evaluation(feature, self.family)]

    async def feed(self, start: int, stop: int) -> None:
        for index in range(start, stop):
            if index in self.missing:
                continue
            bar = make_bar(index)
            self.clock.advance_to(bar.close_time_ms + 1_000)
            await self.runtime.handle_event(bar)

    def result(self) -> ScenarioResult:
        with Session(self.repository.engine) as session:
            signals = [
                {
                    "event_id": row.event_id,
                    "family": row.family,
                    "stage": row.stage,
                    "event_time_ms": row.event_time_ms,
                    "payload_json": row.payload_json,
                }
                for row in session.scalars(select(SignalRow).order_by(SignalRow.event_id))
            ]
            outbox = [
                {
                    "event_id": row.event_id,
                    "status": row.status,
                    "payload_json": row.payload_json,
                    "payload_sha256": row.payload_sha256,
                    "created_at_ms": row.created_at_ms,
                }
                for row in session.scalars(
                    select(AlertOutboxRow).order_by(AlertOutboxRow.event_id)
                )
            ]
        return ScenarioResult(
            signals, outbox, self.emitted, self.runtime, self.repository, self.calls
        )


async def run_scenario(
    overrides: dict[str, Any] | None = None,
    *,
    triggers: Iterable[int] = DEFAULT_TRIGGERS,
    missing: Iterable[int] = (),
    recover: str = "ok",
    bar_count: int = BAR_COUNT,
    family: SignalFamily = SignalFamily.BREAKDOWN_SHORT,
    close_runtime: bool = False,
) -> ScenarioResult:
    """Run the synthetic series. ``recover``: ok | empty | none (no recoverer)."""

    scenario = Scenario(
        overrides, triggers=triggers, missing=missing, recover=recover, family=family
    )
    await scenario.feed(0, bar_count)
    result = scenario.result()
    if close_runtime:
        result.close()
    return result