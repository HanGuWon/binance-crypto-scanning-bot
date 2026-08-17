from __future__ import annotations

import logging
from collections import deque
from collections.abc import Awaitable, Callable
from typing import Any

from signalbot.alerts.embeds import build_discord_payload
from signalbot.clock import Clock
from signalbot.config import Settings
from signalbot.data.anomaly import AnomalyDetector
from signalbot.data.candles import CandleGap, CandleStore, interval_to_milliseconds
from signalbot.data.funding import FundingRateTracker
from signalbot.data.microstructure import BookState, OrderFlowTracker
from signalbot.domain.enums import Market
from signalbot.domain.models import (
    AggTrade,
    BookTicker,
    Candle,
    FeatureSnapshot,
    MiniTicker,
    RuleEvaluation,
    SignalDecision,
)
from signalbot.exchange.binance.schemas import PayloadError, parse_payload
from signalbot.indicators.core import FeatureEngine
from signalbot.persistence.repository import SqlRepository
from signalbot.regime.market import MarketRegimeEngine
from signalbot.signals.positions import (
    PaperLifecycleCheckpoint,
    PaperPositionLifecycle,
)
from signalbot.signals.rules import SignalRuleEngine
from signalbot.signals.state_machine import SignalStateMachine

LOGGER = logging.getLogger(__name__)
DecisionHandler = Callable[[SignalDecision], Awaitable[object]]
GapRecoverer = Callable[[CandleGap], Awaitable[list[Candle]]]
FEATURE_HISTORY_LIMIT = 4


class MarketRuntime:
    def __init__(
        self,
        market: Market,
        settings: Settings,
        repository: SqlRepository,
        clock: Clock,
        decision_handler: DecisionHandler,
        gap_recoverer: GapRecoverer | None = None,
    ) -> None:
        self.market = market
        self.settings = settings
        self.repository = repository
        self.clock = clock
        self.decision_handler = decision_handler
        self.gap_recoverer = gap_recoverer
        self.candles = CandleStore(settings.binance.history_limit)
        self.order_flow = OrderFlowTracker()
        self.books = BookState()
        self.funding = FundingRateTracker(
            maximum_points=settings.binance.funding_history_points,
            minimum_history=settings.signals.funding_zscore_minimum_history,
            maximum_symbols=settings.binance.top_n,
            lookback_ms=settings.signals.funding_zscore_lookback_ms,
        )
        self.regime = MarketRegimeEngine(settings.binance.history_limit)
        self.feature_engine = FeatureEngine(settings.signals)
        self.rule_engine = SignalRuleEngine(settings.signals)
        self.state_machine = SignalStateMachine(settings.signals, settings.rule_version)
        self.paper_positions = PaperPositionLifecycle(
            settings.signals.technical_exit,
            rule_version=settings.rule_version,
            market=market,
            primary_interval=settings.binance.primary_interval,
            maximum_symbols=settings.binance.top_n,
        )
        self.anomaly = AnomalyDetector(settings.signals)
        self.tradable_symbols: frozenset[str] = frozenset()
        self.surveillance_symbols: frozenset[str] = frozenset()
        self._features: dict[tuple[str, str], FeatureSnapshot] = {}
        self._feature_history: dict[
            tuple[str, str], deque[FeatureSnapshot]
        ] = {}
        self.decision_count = 0
        self.parse_error_count = 0

    def set_surveillance_symbols(self, symbols: frozenset[str]) -> None:
        """Backward-compatible replay helper that treats one set as both universes."""

        self.set_active_symbols(symbols, symbols)

    def set_active_symbols(
        self,
        tradable_symbols: frozenset[str],
        surveillance_symbols: frozenset[str],
    ) -> None:
        """Apply one bounded universe rotation and prune all runtime-owned state."""

        tradable = frozenset(symbol.upper() for symbol in tradable_symbols)
        surveillance = frozenset(symbol.upper() for symbol in surveillance_symbols)
        if len(tradable) > self.settings.binance.top_n:
            raise ValueError("tradable universe exceeds configured top_n")
        if len(surveillance) > self.settings.binance.surveillance_n:
            raise ValueError("surveillance universe exceeds configured surveillance_n")
        if not tradable.issubset(surveillance):
            raise ValueError("tradable universe must be a subset of surveillance universe")

        self.tradable_symbols = tradable
        self.surveillance_symbols = surveillance
        self.candles.retain_symbols(self.market, tradable)
        self.order_flow.retain_symbols(self.market, tradable)
        self.books.retain_symbols(self.market, tradable)
        self.funding.retain_symbols(tradable)
        self.anomaly.retain_symbols(surveillance)
        self.regime.retain_symbols(self.market, tradable)
        self.state_machine.prune_symbols(surveillance)
        self.paper_positions.prune_symbols(tradable)
        self._features = {
            key: feature
            for key, feature in self._features.items()
            if key[0] in tradable
        }
        self._feature_history = {
            key: history
            for key, history in self._feature_history.items()
            if key[0] in tradable
        }

    def bootstrap(self, candles: list[Candle], *, rebuild: bool = True) -> int:
        accepted = [
            candle
            for candle in candles
            if candle.is_closed
            and candle.market is self.market
            and candle.symbol in self.tradable_symbols
            and candle.interval in self.settings.binance.intervals
        ]
        inserted = self.candles.add_many(accepted)
        if rebuild and inserted:
            self.rebuild_derived_state()
        return inserted

    def rebuild_derived_state(self) -> int:
        """Rebuild bounded features and regime inputs without replaying stale alerts.

        Regime histories are populated first, then queried point-in-time for every
        feature. This makes bootstrap output independent of REST response order and
        excludes same-close breadth and BTC trend inputs.
        """

        series = self.candles.series(self.market)
        self.regime = MarketRegimeEngine(self.settings.binance.history_limit)
        self._features.clear()
        self._feature_history.clear()

        for candles in series:
            for candle in candles:
                self.regime.update_candle(candle)

        for candles in series:
            if not candles or candles[0].symbol != "BTCUSDT" or candles[0].interval != "1h":
                continue
            for feature in self.feature_engine.compute_series(candles, spread_bps=None):
                if feature is not None:
                    self.regime.update_feature(feature)

        feature_count = 0
        for candles in series:
            regimes = [
                self.regime.snapshot(self.market, candle.close_time_ms)
                for candle in candles
            ]
            for feature in self.feature_engine.compute_series(
                candles,
                spread_bps=None,
                regimes=regimes,
            ):
                if feature is None:
                    continue
                self._store_feature(self._with_funding(feature))
                feature_count += 1
        return feature_count

    async def handle_payload(self, payload: Any) -> None:
        try:
            events = parse_payload(self.market, payload)
        except PayloadError as exc:
            self.parse_error_count += 1
            LOGGER.warning(
                "discarding malformed Binance payload",
                extra={"market": self.market.value},
                exc_info=exc,
            )
            return
        for event in events:
            await self.handle_event(event)

    async def handle_event(self, event: Candle | BookTicker | AggTrade | MiniTicker) -> None:
        if event.market is not self.market:
            return
        if isinstance(event, MiniTicker):
            if event.symbol not in self.surveillance_symbols:
                return
            if event.event_time_ms == 0:
                event = event.model_copy(update={"event_time_ms": self.clock.now_ms()})
            evaluations = self.anomaly.update(
                event,
                self.surveillance_symbols,
                self.regime.snapshot(self.market, event.event_time_ms),
            )
            for evaluation in evaluations:
                await self._process(evaluation)
            return
        if event.symbol not in self.tradable_symbols:
            return
        if isinstance(event, BookTicker):
            received_at_ms = self.clock.now_ms()
            event = event.model_copy(
                update={
                    "event_time_ms": event.event_time_ms or received_at_ms,
                    "receipt_time_ms": received_at_ms,
                }
            )
            self.books.update(event)
            return
        if isinstance(event, AggTrade):
            self.order_flow.update(event)
            return
        if event.interval not in self.settings.binance.intervals:
            return
        await self._handle_candle(event)

    async def _handle_candle(self, candle: Candle) -> None:
        if not candle.is_closed:
            return
        latest = self.candles.latest(candle.market, candle.symbol, candle.interval)
        if latest is not None and candle.open_time_ms < latest.open_time_ms:
            return
        gap = self.candles.detect_latest_gap(candle)
        recovered = True
        if gap is not None:
            if candle.interval == self.settings.binance.primary_interval:
                checkpoint = self.paper_positions.checkpoint_symbol(candle.symbol)
                try:
                    gap_exits = self.paper_positions.reset_for_gap(candle)
                except Exception:
                    self.paper_positions.restore_checkpoint(checkpoint)
                    raise
                await self._publish_paper_transition(gap_exits, checkpoint)
            recovered = await self._recover_gap(gap)
        if not recovered:
            LOGGER.warning(
                "holding signal evaluation after unrecovered candle gap",
                extra={"market": candle.market.value, "symbol": candle.symbol},
            )
            return
        inserted = self.candles.add(candle)
        if not inserted:
            return
        if self.settings.runtime.persist_candles:
            self.repository.save_candle(candle)
        feature = self._update_derived_for_candle(candle)
        if feature is None:
            return
        if candle.interval != self.settings.binance.primary_interval:
            return
        contexts = self._context_features(candle.symbol, feature.event_time_ms)
        new_decisions: list[SignalDecision] = []
        for evaluation in self.rule_engine.evaluate(feature, contexts):
            decision = await self._process(evaluation)
            if decision is not None:
                new_decisions.append(decision)
        checkpoint = self.paper_positions.checkpoint_symbol(candle.symbol)
        try:
            exits = self.paper_positions.on_closed_candle(
                candle,
                feature,
                new_decisions,
            )
        except Exception:
            self.paper_positions.restore_checkpoint(checkpoint)
            raise
        await self._publish_paper_transition(exits, checkpoint)

    async def _recover_gap(self, gap: CandleGap) -> bool:
        if self.gap_recoverer is None:
            LOGGER.warning(
                "candle gap detected without recovery provider",
                extra={"market": gap.market.value, "symbol": gap.symbol},
            )
            return False
        try:
            recovered = await self.gap_recoverer(gap)
        except Exception as exc:
            LOGGER.error(
                "candle gap recovery failed",
                extra={"market": gap.market.value, "symbol": gap.symbol},
                exc_info=exc,
            )
            return False
        step_ms = interval_to_milliseconds(gap.interval)
        expected_opens = list(
            range(gap.start_time_ms, gap.end_time_ms + 1, step_ms)
        )
        ordered = sorted(recovered, key=lambda item: item.open_time_ms)
        valid = [item.open_time_ms for item in ordered] == expected_opens and all(
            item.market is gap.market
            and item.symbol == gap.symbol
            and item.interval == gap.interval
            and item.is_closed
            and item.close_time_ms == item.open_time_ms + step_ms - 1
            for item in ordered
        )
        if not valid:
            LOGGER.error(
                "candle gap recovery returned an incomplete or invalid range",
                extra={
                    "market": gap.market.value,
                    "symbol": gap.symbol,
                    "expected": len(expected_opens),
                    "received": len(ordered),
                },
            )
            return False
        inserted = 0
        for candle in ordered:
            if not self.candles.add(candle):
                return False
            inserted += 1
            if self.settings.runtime.persist_candles:
                self.repository.save_candle(candle)
            self._update_derived_for_candle(candle)
        LOGGER.info(
            "recovered missing candles", extra={"market": gap.market.value, "symbol": gap.symbol}
        )
        return inserted == len(expected_opens)

    def _update_derived_for_candle(self, candle: Candle) -> FeatureSnapshot | None:
        self.regime.update_candle(candle)
        feature = self._refresh_feature(candle.symbol, candle.interval)
        if feature is not None and candle.symbol == "BTCUSDT" and candle.interval == "1h":
            self.regime.update_feature(feature)
        return feature

    def _refresh_feature(self, symbol: str, interval: str) -> FeatureSnapshot | None:
        series = self.candles.get(self.market, symbol, interval)
        if not series:
            return None
        latest = series[-1]
        book = self.books.snapshot(
            self.market,
            symbol,
            as_of_ms=self.clock.now_ms(),
            maximum_age_ms=self.settings.signals.book_maximum_age_ms,
        )
        feature = self.feature_engine.compute(
            series,
            self.order_flow.snapshot(self.market, symbol, latest.close_time_ms, 60),
            None if book is None else book.spread_bps,
            self.regime.snapshot(self.market, latest.close_time_ms),
        )
        if feature is not None:
            if book is not None:
                feature = feature.model_copy(
                    update={
                        "book_age_ms": book.age_ms,
                        "bid_quote_capacity": book.bid_quote_capacity,
                        "ask_quote_capacity": book.ask_quote_capacity,
                    }
                )
            feature = self._with_funding(feature)
            self._store_feature(feature)
        return feature

    def _store_feature(self, feature: FeatureSnapshot) -> None:
        key = (feature.symbol, feature.interval)
        history = self._feature_history.setdefault(
            key, deque(maxlen=FEATURE_HISTORY_LIMIT)
        )
        by_time = {item.event_time_ms: item for item in history}
        by_time[feature.event_time_ms] = feature
        ordered = sorted(by_time.values(), key=lambda item: item.event_time_ms)
        history.clear()
        history.extend(ordered[-FEATURE_HISTORY_LIMIT:])
        if not history or history[-1].event_time_ms != feature.event_time_ms:
            return
        self._features[key] = feature

    def _with_funding(self, feature: FeatureSnapshot) -> FeatureSnapshot:
        if self.market is not Market.FUTURES:
            return feature
        snapshot = self.funding.snapshot(
            feature.symbol,
            as_of_ms=feature.event_time_ms,
            maximum_age_ms=self.settings.signals.funding_maximum_age_ms,
        )
        if snapshot is None:
            return feature
        return feature.model_copy(
            update={"funding_rate": snapshot.rate, "funding_zscore": snapshot.zscore}
        )

    def _context_features(self, symbol: str, event_time_ms: int) -> dict[str, FeatureSnapshot]:
        normalized = symbol.upper()
        contexts: dict[str, FeatureSnapshot] = {}
        for (feature_symbol, interval), history in self._feature_history.items():
            if feature_symbol != normalized or interval == self.settings.binance.primary_interval:
                continue
            available = [item for item in history if item.event_time_ms < event_time_ms]
            if not available:
                continue
            snapshot = available[-1]
            maximum_age_ms = interval_to_milliseconds(interval) * 2
            if event_time_ms - snapshot.event_time_ms <= maximum_age_ms:
                contexts[interval] = snapshot
        return contexts

    async def _process(self, evaluation: RuleEvaluation) -> SignalDecision | None:
        decision = self.state_machine.process(evaluation)
        if decision is None:
            return None
        return await self._publish_decision(decision)

    async def _publish_decision(
        self, decision: SignalDecision
    ) -> SignalDecision | None:
        if not self._persist_decision(decision):
            return None
        self.decision_count += 1
        await self.decision_handler(decision)
        return decision

    def _persist_decision(self, decision: SignalDecision) -> bool:
        """Durably store one immutable signal/outbox intent without dispatching it."""

        delivery_enabled = (
            self.settings.alerts.discord_enabled
            and self.settings.alerts.discord_webhook_url is not None
        )
        payload = build_discord_payload(
            decision,
            self.settings.alerts.discord_username,
        )
        return self.repository.save_signal_and_enqueue(
            decision,
            payload,
            self.clock.now_ms(),
            delivery_enabled=delivery_enabled,
            maximum_active_items=self.settings.alerts.outbox_max_active_items,
        )

    async def _publish_paper_transition(
        self,
        exits: list[SignalDecision],
        checkpoint: PaperLifecycleCheckpoint,
    ) -> None:
        """Commit PAPER state only after every generated exit is durable.

        A duplicate/no-op means the immutable exit already exists and therefore
        commits the transition. Handler failures happen after durability and must
        not resurrect a PAPER position whose exit is already persisted.
        """

        newly_persisted: list[SignalDecision] = []
        durable_count = 0
        try:
            for decision in exits:
                created = self._persist_decision(decision)
                durable_count += 1
                if created:
                    newly_persisted.append(decision)
        except Exception:
            if durable_count == 0:
                self.paper_positions.restore_checkpoint(checkpoint)
            raise

        for decision in newly_persisted:
            self.decision_count += 1
            await self.decision_handler(decision)
