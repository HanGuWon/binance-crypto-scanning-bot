from __future__ import annotations

import logging
import time
from collections import deque
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any

from signalbot.alerts.embeds import build_discord_payload
from signalbot.alerts.policy import should_deliver_discord
from signalbot.clock import Clock
from signalbot.config import Settings
from signalbot.data.anomaly import AnomalyDetector
from signalbot.data.candles import CandleGap, CandleStore, interval_to_milliseconds
from signalbot.data.funding import FundingRateTracker
from signalbot.data.microstructure import (
    BookState,
    BookTickerConflictError,
    OrderFlowTracker,
    minute_pressure_snapshot,
)
from signalbot.domain.enums import Market, SignalFamily
from signalbot.domain.models import (
    AggTrade,
    BookTicker,
    Candle,
    FeatureSnapshot,
    GainerWatchState,
    Instrument,
    MiniTicker,
    RuleEvaluation,
    SignalDecision,
)
from signalbot.exchange.binance.schemas import PayloadError, parse_payload
from signalbot.indicators.core import FeatureEngine
from signalbot.persistence.repository import SqlRepository
from signalbot.prospective.pilot_guard import ShadowPilotGuard
from signalbot.prospective.public_structure_flow_v2 import PublicStructureFlowShadowObserver
from signalbot.regime.market import MarketRegimeEngine
from signalbot.signals.episodes import (
    FOUR_HOUR_BARS,
    TrendZoneTracker,
    build_position_line,
)
from signalbot.signals.market_watch import MarketWatchEngine
from signalbot.signals.positions import (
    PaperLifecycleCheckpoint,
    PaperPositionLifecycle,
    PaperRecoveryError,
)
from signalbot.signals.rules import SignalRuleEngine
from signalbot.signals.state_machine import SignalStateMachine

LOGGER = logging.getLogger(__name__)
DecisionHandler = Callable[[SignalDecision], Awaitable[object]]
GapRecoverer = Callable[[CandleGap], Awaitable[list[Candle]]]
FEATURE_HISTORY_LIMIT = 4
GAP_RECOVERY_POLICY = "paper_gap_recovery_v1"


@dataclass(frozen=True, slots=True)
class _AlertInputs:
    """Closed-bar facts the alert presentation needs; built only when it is enabled."""

    feature: FeatureSnapshot
    candles: list[Candle]


@dataclass(slots=True)
class _ContiguityHold:
    held_bars: int = 0
    released_through_open_ms: int = -1


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
        self.rule_engine = SignalRuleEngine(settings.signals, settings.shadow)
        self.state_machine = SignalStateMachine(settings.signals, settings.rule_version)
        self.paper_positions = PaperPositionLifecycle(
            settings.signals.technical_exit,
            rule_version=settings.rule_version,
            market=market,
            primary_interval=settings.binance.primary_interval,
            maximum_symbols=settings.binance.top_n,
            tag_tracking_interruption=settings.runtime.paper_gap_stats_split,
        )
        alerts = settings.alerts
        self.zone_tracker: TrendZoneTracker | None = (
            TrendZoneTracker(
                suppress=alerts.repeat_suppression,
                max_hours=alerts.repeat_suppression_max_hours,
                maximum_symbols=settings.binance.top_n,
            )
            if alerts.alert_position_line or alerts.repeat_suppression
            else None
        )
        self._contiguity: dict[tuple[str, str], _ContiguityHold] = {}
        self.anomaly = AnomalyDetector(settings.signals)
        self.market_watch = MarketWatchEngine(settings.alerts.market_watch)
        self.public_structure_flow_shadow: PublicStructureFlowShadowObserver | None = None
        self._public_shadow_repository: SqlRepository | None = None
        shadow_url = (
            settings.alerts.market_watch.public_structure_flow_database_url or repository.url
        )
        self._public_shadow_guard = ShadowPilotGuard(settings.alerts.market_watch, shadow_url)
        self._public_shadow_stop_logged = False
        if settings.alerts.market_watch.public_structure_flow_shadow_enabled:
            try:
                shadow_repository = repository
                if settings.alerts.market_watch.public_structure_flow_database_url:
                    self._public_shadow_repository = SqlRepository(shadow_url)
                    self._public_shadow_repository.initialize()
                    shadow_repository = self._public_shadow_repository
                self.public_structure_flow_shadow = PublicStructureFlowShadowObserver(
                    settings,
                    shadow_repository,
                    registered_at_ms=clock.now_ms(),
                )
            except Exception as exc:
                self.close_public_structure_flow_shadow()
                LOGGER.error(
                    "public structure-flow shadow startup failed; incumbent continues",
                    extra={"market": market.value},
                    exc_info=exc,
                )
        self.tradable_symbols: frozenset[str] = frozenset()
        self.surveillance_symbols: frozenset[str] = frozenset()
        self.context_symbols: frozenset[str] = frozenset()
        self.watch_symbols: frozenset[str] = frozenset()
        self._features: dict[tuple[str, str], FeatureSnapshot] = {}
        self._feature_history: dict[tuple[str, str], deque[FeatureSnapshot]] = {}
        self.decision_count = 0
        self.parse_error_count = 0

    def close_public_structure_flow_shadow(self) -> None:
        """Release only the owned research engine; the application owns production storage."""
        if self._public_shadow_repository is not None:
            self._public_shadow_repository.close()
            self._public_shadow_repository = None
        self.public_structure_flow_shadow = None

    def set_priority_gainers(
        self, instruments: tuple[Instrument, ...], retained: tuple[Instrument, ...] = (),
        states: tuple[GainerWatchState, ...] = (), observed_ms: int = 0,
    ) -> None:
        """Refresh the bounded 24h gainer panel without emitting on the refresh clock."""

        removed, _added = self.market_watch.set_priority_gainers(
            instruments, retained, states, observed_ms,
        )
        self.repository.prune_market_watch_episodes(
            self.market, self.market_watch.monitored_symbols(self.clock.now_ms()),
            self.clock.now_ms(),
        )
        for symbol in removed:
            for family in (
                SignalFamily.TOP_GAINER,
                SignalFamily.BOLLINGER_UPPER_TOUCH,
                SignalFamily.BOLLINGER_LOWER_TOUCH,
            ):
                self.state_machine.deactivate_event(
                    self.market,
                    symbol,
                    family,
                    self.settings.binance.primary_interval,
                )

    def set_surveillance_symbols(self, symbols: frozenset[str]) -> None:
        """Backward-compatible replay helper that treats one set as both universes."""

        self.set_active_symbols(symbols, symbols)

    def set_active_symbols(
        self,
        tradable_symbols: frozenset[str],
        surveillance_symbols: frozenset[str],
        context_symbols: frozenset[str] = frozenset(),
        watch_symbols: frozenset[str] = frozenset(),
    ) -> None:
        """Apply one bounded universe rotation and prune all runtime-owned state."""

        previous_market_data = self.tradable_symbols | self.context_symbols | self.watch_symbols
        tradable = frozenset(symbol.upper() for symbol in tradable_symbols)
        surveillance = frozenset(symbol.upper() for symbol in surveillance_symbols)
        context = frozenset(symbol.upper() for symbol in context_symbols)
        watch = frozenset(symbol.upper() for symbol in watch_symbols)
        watch_settings = self.settings.alerts.market_watch
        if len(watch) > watch_settings.max_retained_symbols + watch_settings.top_n:
            raise ValueError("retained watch exceeds configured maximum")
        if len(tradable) > self.settings.binance.top_n:
            raise ValueError("tradable universe exceeds configured top_n")
        if len(surveillance) > self.settings.binance.surveillance_n:
            raise ValueError("surveillance universe exceeds configured surveillance_n")
        if not tradable.issubset(surveillance):
            raise ValueError("tradable universe must be a subset of surveillance universe")
        self.tradable_symbols = tradable
        self.surveillance_symbols = surveillance
        self.context_symbols = context
        self.watch_symbols = watch
        retained_market_data = tradable | context | watch
        observer = self.public_structure_flow_shadow
        if observer is not None:
            removed_shadow_symbols = previous_market_data - retained_market_data
            if removed_shadow_symbols:
                try:
                    observer.finalize_removed_symbols(
                        removed_shadow_symbols,
                        now_ms=self.clock.now_ms(),
                    )
                except Exception as exc:
                    LOGGER.error(
                        "public structure-flow shadow rotation finalization failed; "
                        "incumbent continues",
                        extra={"market": self.market.value},
                        exc_info=exc,
                    )
        self.candles.retain_symbols(self.market, retained_market_data)
        self.order_flow.retain_symbols(self.market, tradable)
        self.books.retain_symbols(self.market, tradable)
        self.funding.retain_symbols(tradable)
        self.anomaly.retain_symbols(surveillance)
        self.regime.retain_symbols(self.market, retained_market_data)
        self.state_machine.prune_symbols(surveillance | watch)
        self.state_machine.prune_directional_states(tradable)
        self.paper_positions.prune_symbols(tradable)
        if self.zone_tracker is not None:
            self.zone_tracker.prune_symbols(tradable)
        self._contiguity = {
            key: hold for key, hold in self._contiguity.items() if key[0] in retained_market_data
        }
        self._features = {
            key: feature
            for key, feature in self._features.items()
            if key[0] in retained_market_data
        }
        self._feature_history = {
            key: history
            for key, history in self._feature_history.items()
            if key[0] in retained_market_data
        }

    def bootstrap(self, candles: list[Candle], *, rebuild: bool = True) -> int:
        accepted = [
            candle
            for candle in candles
            if candle.is_closed
            and candle.market is self.market
            and candle.symbol in (self.tradable_symbols | self.context_symbols | self.watch_symbols)
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
                self.regime.snapshot(self.market, candle.close_time_ms) for candle in candles
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
        if isinstance(event, Candle):
            if event.symbol not in (
                self.tradable_symbols | self.context_symbols | self.watch_symbols
            ):
                return
        elif event.symbol not in self.tradable_symbols:
            return
        if isinstance(event, BookTicker):
            received_at_ms = self.clock.now_ms()
            event = event.model_copy(
                update={
                    "event_time_ms": event.event_time_ms or received_at_ms,
                    "receipt_time_ms": received_at_ms,
                }
            )
            try:
                self.books.update(event)
            except BookTickerConflictError as exc:
                LOGGER.warning(
                    "discarding conflicting book-ticker cursor",
                    extra={"market": self.market.value, "symbol": event.symbol},
                    exc_info=exc,
                )
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
        observer = self.public_structure_flow_shadow
        if observer is not None:
            try:
                if candle.interval == "1m":
                    observer.accept_minute(candle, received_at_ms=self.clock.now_ms())
                observer.expire_due(self.clock.now_ms())
            except Exception as exc:
                LOGGER.error(
                    "public structure-flow shadow late-evidence handling failed; "
                    "incumbent continues",
                    extra={"market": self.market.value, "symbol": candle.symbol},
                    exc_info=exc,
                )
        latest = self.candles.latest(candle.market, candle.symbol, candle.interval)
        if latest is not None and candle.open_time_ms < latest.open_time_ms:
            return
        gap = self.candles.detect_latest_gap(candle)
        recovered = True
        if gap is not None:
            paper_symbol = (
                candle.interval == self.settings.binance.primary_interval
                and candle.symbol in self.tradable_symbols
            )
            if paper_symbol and self.settings.runtime.paper_gap_recovery:
                recovered = await self._recover_gap_before_paper(candle, gap)
            else:
                if paper_symbol:
                    checkpoint = self.paper_positions.checkpoint_symbol(candle.symbol)
                    try:
                        gap_exits = self.paper_positions.reset_for_gap(candle)
                    except Exception:
                        self.paper_positions.restore_checkpoint(checkpoint)
                        raise
                    await self._publish_paper_transition(gap_exits, checkpoint)
                recovered = await self._recover_gap(gap, candle=candle)
        if not recovered:
            if candle.interval == self.settings.binance.primary_interval:
                self._record_public_structure_flow_missing(
                    candle,
                    reason="unrecovered_primary_candle_gap",
                )
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
        if candle.interval != self.settings.binance.primary_interval:
            return
        if feature is None:
            self._record_public_structure_flow_missing(
                candle,
                reason="primary_feature_unavailable",
            )
            return
        self._register_public_structure_flow_opportunity(candle, feature)
        if self.settings.runtime.require_contiguous_bars and self._hold_for_missing_bars(candle):
            if candle.symbol in self.tradable_symbols:
                await self._advance_paper_without_signals(candle, feature)
            return
        new_decisions: list[SignalDecision] = []
        prior_candles = self.candles.get(candle.market, candle.symbol, candle.interval)
        pressure = None
        if self.settings.alerts.market_watch.minute_pressure_enabled:
            pressure = minute_pressure_snapshot(
                self.candles.get(candle.market, candle.symbol, "1m"),
                candle.market, candle.symbol, candle.close_time_ms,
            )
        observations = self.market_watch.evaluate(
            candle, feature, prior_candles, now_ms=self.clock.now_ms(), minute_pressure=pressure,
        )
        preference = {
            SignalFamily.RANGE_LONG: 0, SignalFamily.RANGE_SHORT: 0,
            SignalFamily.BOLLINGER_UPPER_TOUCH: 1, SignalFamily.BOLLINGER_LOWER_TOUCH: 1,
            SignalFamily.WATCH_TREND_UP: 2, SignalFamily.WATCH_TREND_DOWN: 2,
            SignalFamily.TOP_GAINER: 3,
        }
        emitted_observation = False
        for evaluation in sorted(observations, key=lambda item: preference[item.family]):
            decision = await self._process(evaluation, delivery_allowed=not emitted_observation)
            if decision is not None:
                emitted_observation = True
                new_decisions.append(decision)
        if candle.symbol not in self.tradable_symbols:
            return
        contexts = self._context_features(candle.symbol, feature.event_time_ms)
        alert_inputs: _AlertInputs | None = None
        if self.zone_tracker is not None:
            self.zone_tracker.update(feature, contexts)
            alert_inputs = _AlertInputs(feature, prior_candles[-FOUR_HOUR_BARS:])
        for evaluation in self.rule_engine.evaluate(feature, contexts):
            decision = await self._process(evaluation, alert_inputs=alert_inputs)
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

    async def _recover_gap(self, gap: CandleGap, *, candle: Candle | None = None) -> bool:
        """Bounded REST fill of one gap; ``True`` only when the whole range was applied."""

        started = time.monotonic()
        ok, rows = await self._recover_gap_rows(gap)
        if candle is not None:
            self._log_gap_event(
                gap, candle, ok=ok, recovered_bars=len(rows), started=started,
                paper_policy="legacy",
            )
        return ok

    def _log_gap_event(
        self,
        gap: CandleGap,
        candle: Candle,
        *,
        ok: bool,
        recovered_bars: int,
        started: float,
        paper_policy: str,
        paper_result: str = "n/a",
        paper_exit_reasons: tuple[str, ...] = (),
    ) -> None:
        """One structured line per gap: range, REST outcome, duration, PAPER outcome."""

        if not self.settings.runtime.paper_gap_log:
            return
        step_ms = interval_to_milliseconds(gap.interval)
        LOGGER.info(
            "candle gap handled",
            extra={
                "market": gap.market.value,
                "symbol": gap.symbol,
                "fields": {
                    "event": "candle_gap",
                    "interval": gap.interval,
                    "gap_start_open_ms": gap.start_time_ms,
                    "gap_end_ms": gap.end_time_ms,
                    "missing_bars": (gap.end_time_ms - gap.start_time_ms + 1) // step_ms,
                    "first_return_open_ms": candle.open_time_ms,
                    "rest_ok": ok,
                    "recovered_bars": recovered_bars,
                    "elapsed_ms": int((time.monotonic() - started) * 1000),
                    "paper_policy": paper_policy,
                    "paper_result": paper_result,
                    "paper_exit_reasons": list(paper_exit_reasons),
                },
            },
        )

    async def _recover_gap_rows(
        self, gap: CandleGap
    ) -> tuple[bool, list[tuple[Candle, FeatureSnapshot | None]]]:
        if self.gap_recoverer is None:
            LOGGER.warning(
                "candle gap detected without recovery provider",
                extra={"market": gap.market.value, "symbol": gap.symbol},
            )
            return False, []
        try:
            recovered = await self.gap_recoverer(gap)
        except Exception as exc:
            LOGGER.error(
                "candle gap recovery failed",
                extra={"market": gap.market.value, "symbol": gap.symbol},
                exc_info=exc,
            )
            return False, []
        step_ms = interval_to_milliseconds(gap.interval)
        expected_opens = list(range(gap.start_time_ms, gap.end_time_ms + 1, step_ms))
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
            return False, []
        inserted = 0
        rows: list[tuple[Candle, FeatureSnapshot | None]] = []
        for candle in ordered:
            if not self.candles.add(candle):
                return False, rows
            inserted += 1
            if self.settings.runtime.persist_candles:
                self.repository.save_candle(candle)
            rows.append((candle, self._update_derived_for_candle(candle)))
            observer = self.public_structure_flow_shadow
            if observer is not None and candle.interval == "1m":
                try:
                    observer.accept_minute(candle, received_at_ms=self.clock.now_ms())
                except Exception as exc:
                    LOGGER.error(
                        "public structure-flow shadow recovered-minute handling failed; "
                        "incumbent continues",
                        extra={"market": self.market.value, "symbol": candle.symbol},
                        exc_info=exc,
                    )
        LOGGER.info(
            "recovered missing candles", extra={"market": gap.market.value, "symbol": gap.symbol}
        )
        return inserted == len(expected_opens), rows

    async def _recover_gap_before_paper(self, candle: Candle, gap: CandleGap) -> bool:
        """``paper_gap_recovery``: finish the bounded REST fill first, then decide PAPER.

        When the whole range is recovered and every bar has an indicator row, the bars
        are replayed in order through the existing PAPER position only (exits of an
        already open position; no new entry, no old signal alert). Otherwise PAPER ends
        with the same ``data_gap`` exit as before.
        """

        started = time.monotonic()
        ok, rows = await self._recover_gap_rows(gap)
        checkpoint = self.paper_positions.checkpoint_symbol(candle.symbol)
        had_position = self.paper_positions.has_position(candle.symbol)
        exits: list[SignalDecision] = []
        continued = False
        if ok:
            try:
                for recovered_candle, feature in rows:
                    if feature is None:
                        raise PaperRecoveryError("no indicator row for a recovered bar")
                    exits.extend(
                        self.paper_positions.advance_recovered_candle(recovered_candle, feature)
                    )
                continued = True
            except PaperRecoveryError as exc:
                self.paper_positions.restore_checkpoint(checkpoint)
                exits = []
                LOGGER.warning(
                    "recovered bars could not continue the PAPER position; ending it as data_gap",
                    extra={"market": candle.market.value, "symbol": candle.symbol},
                    exc_info=exc,
                )
            except Exception:
                self.paper_positions.restore_checkpoint(checkpoint)
                raise
        if continued:
            observed_ms = self.clock.now_ms()
            exits = [
                item.model_copy(
                    update={
                        "metadata": {
                            **item.metadata,
                            "late_observation": True,
                            "observed_wall_time_ms": observed_ms,
                            "gap_recovery_policy": GAP_RECOVERY_POLICY,
                        }
                    }
                )
                for item in exits
            ]
            paper_result = (
                "no_position" if not had_position else "exited" if exits else "continued"
            )
        else:
            try:
                exits = self.paper_positions.reset_for_gap(candle)
            except Exception:
                self.paper_positions.restore_checkpoint(checkpoint)
                raise
            paper_result = "data_gap" if exits else "no_position"
        self._log_gap_event(
            gap, candle, ok=ok, recovered_bars=len(rows), started=started,
            paper_policy=GAP_RECOVERY_POLICY, paper_result=paper_result,
            paper_exit_reasons=tuple(str(item.metadata.get("exit_reason")) for item in exits),
        )
        await self._publish_paper_transition(exits, checkpoint)
        return ok

    def maintain_public_structure_flow_shadow(self) -> None:
        """Run bounded shadow expiry/retry work on an existing scanner maintenance clock."""

        observer = self.public_structure_flow_shadow
        if observer is None:
            return
        self._public_shadow_admission_allowed()
        try:
            observer.expire_due(self.clock.now_ms())
        except Exception as exc:
            LOGGER.error(
                "public structure-flow shadow maintenance failed; incumbent continues",
                extra={"market": self.market.value},
                exc_info=exc,
            )

    def _public_shadow_admission_allowed(self) -> bool:
        try:
            allowed = self._public_shadow_guard.admission_allowed(self.clock.now_ms())
        except OSError:
            self._public_shadow_guard.reason = "storage_guard_io_failure"
            allowed = False
        if not allowed and not self._public_shadow_stop_logged:
            self._public_shadow_stop_logged = True
            LOGGER.warning(
                "shadow admission stopped; pending drains and incumbent continues",
                extra={"reason": self._public_shadow_guard.reason},
            )
        return allowed

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
        history = self._feature_history.setdefault(key, deque(maxlen=FEATURE_HISTORY_LIMIT))
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

    def _register_public_structure_flow_opportunity(
        self, candle: Candle, feature: FeatureSnapshot
    ) -> None:
        """Freeze one event-time shadow denominator without changing production decisions."""

        observer = self.public_structure_flow_shadow
        watch_settings = self.settings.alerts.market_watch
        if observer is None or self.market is not watch_settings.market:
            return
        if not self._public_shadow_admission_allowed():
            return
        event_time_ms = candle.close_time_ms
        watch_snapshot = self.market_watch.snapshot_with_time_for(candle.symbol, event_time_ms)
        if watch_snapshot is None:
            return
        watch_observed_ms, watch_context = watch_snapshot
        primary_interval = self.settings.binance.primary_interval
        primary_rows = self.candles.get(self.market, candle.symbol, primary_interval)
        minute_rows = self.candles.get(self.market, candle.symbol, "1m")
        try:
            observer.register_opportunity(
                candle=candle,
                feature=feature,
                five_minute_candles=primary_rows,
                minute_candles=minute_rows,
                contexts=self._context_features(candle.symbol, event_time_ms),
                watch_context=watch_context,
                watch_observed_ms=watch_observed_ms,
                received_at_ms=self.clock.now_ms(),
            )
        except Exception as exc:
            LOGGER.error(
                "public structure-flow shadow opportunity failed; incumbent continues",
                extra={"market": self.market.value, "symbol": candle.symbol},
                exc_info=exc,
            )

    def _record_public_structure_flow_missing(self, candle: Candle, *, reason: str) -> None:
        """Record an unrecoverable shadow denominator without altering production control flow."""

        observer = self.public_structure_flow_shadow
        watch_settings = self.settings.alerts.market_watch
        if observer is None or self.market is not watch_settings.market:
            return
        if not self._public_shadow_admission_allowed():
            return
        watch_snapshot = self.market_watch.snapshot_with_time_for(
            candle.symbol,
            candle.close_time_ms,
        )
        if watch_snapshot is None:
            return
        watch_observed_ms, watch_context = watch_snapshot
        try:
            observer.register_missing_opportunity(
                candle=candle,
                watch_context=watch_context,
                watch_observed_ms=watch_observed_ms,
                received_at_ms=self.clock.now_ms(),
                reason=reason,
            )
        except Exception as exc:
            LOGGER.error(
                "public structure-flow shadow missing-denominator write failed; "
                "incumbent continues",
                extra={"market": self.market.value, "symbol": candle.symbol},
                exc_info=exc,
            )

    def _hold_for_missing_bars(self, candle: Candle) -> bool:
        """``require_contiguous_bars``: hold signal evaluation while the indicator input
        window still contains an unrecovered hole.

        The hold is bounded: after ``contiguity_hold_limit_bars`` consecutive held bars
        the holes seen so far are acknowledged (logged once) and no longer block, so a
        delisting, trading halt or permanently missing exchange bar cannot freeze a
        symbol. The state clears itself once the holes leave the window.
        """

        runtime = self.settings.runtime
        key = (candle.symbol, candle.interval)
        missing = self.candles.missing_opens(
            candle.market, candle.symbol, candle.interval, runtime.contiguity_window_bars
        )
        if not missing:
            self._contiguity.pop(key, None)
            return False
        newest_hole = max(missing)
        hold = self._contiguity.setdefault(key, _ContiguityHold())
        if newest_hole <= hold.released_through_open_ms:
            return False
        hold.held_bars += 1
        if hold.held_bars > runtime.contiguity_hold_limit_bars:
            hold.released_through_open_ms = newest_hole
            hold.held_bars = 0
            LOGGER.warning(
                "indicator window hole acknowledged after hold limit; evaluation resumes",
                extra={
                    "market": candle.market.value,
                    "symbol": candle.symbol,
                    "fields": {
                        "event": "contiguity_released",
                        "interval": candle.interval,
                        "missing_bars": len(missing),
                        "newest_missing_open_ms": newest_hole,
                        "hold_limit_bars": runtime.contiguity_hold_limit_bars,
                    },
                },
            )
            return False
        if hold.held_bars == 1:
            LOGGER.warning(
                "holding signal evaluation: indicator window has missing bars",
                extra={
                    "market": candle.market.value,
                    "symbol": candle.symbol,
                    "fields": {
                        "event": "contiguity_hold",
                        "interval": candle.interval,
                        "missing_bars": len(missing),
                        "newest_missing_open_ms": newest_hole,
                    },
                },
            )
        return True

    async def _advance_paper_without_signals(
        self, candle: Candle, feature: FeatureSnapshot
    ) -> None:
        """Keep an open PAPER position tracking while signal evaluation is held."""

        checkpoint = self.paper_positions.checkpoint_symbol(candle.symbol)
        try:
            exits = self.paper_positions.on_closed_candle(candle, feature, [])
        except Exception:
            self.paper_positions.restore_checkpoint(checkpoint)
            raise
        await self._publish_paper_transition(exits, checkpoint)

    async def _process(
        self,
        evaluation: RuleEvaluation,
        *,
        delivery_allowed: bool = True,
        alert_inputs: _AlertInputs | None = None,
    ) -> SignalDecision | None:
        if evaluation.metadata.get("market_watch") is True:
            if not evaluation.triggered:
                self.repository.rearm_market_watch(evaluation)
                return None
            decision = self.state_machine.decision_for_observation(evaluation)
            if not self._persist_decision(
                decision, observation=evaluation, delivery_allowed=delivery_allowed,
            ):
                return None
            self.decision_count += 1
            await self.decision_handler(decision)
            return decision
        decision = self.state_machine.process(evaluation)
        if decision is None:
            return None
        return await self._publish_decision(decision, alert_inputs=alert_inputs)

    async def _publish_decision(
        self, decision: SignalDecision, *, alert_inputs: _AlertInputs | None = None
    ) -> SignalDecision | None:
        if not self._persist_decision(decision, alert_inputs=alert_inputs):
            return None
        self.decision_count += 1
        await self.decision_handler(decision)
        return decision

    def _apply_zone_policy(
        self, decision: SignalDecision, delivery_enabled: bool, alert_inputs: _AlertInputs
    ) -> tuple[SignalDecision, bool, str | None]:
        """Zone ordinal, optional position line and optional repeat suppression.

        A suppressed repeat is still stored (signal row plus a disabled outbox row,
        tagged ``suppressed_repeat``) and still reaches PAPER exactly as before; only
        its Discord delivery is withheld.
        """

        tracker = self.zone_tracker
        if tracker is None:
            return decision, delivery_enabled, None
        alerts = self.settings.alerts
        assessment = tracker.register(decision)
        metadata = {
            **decision.metadata,
            "alert_episode_ordinal": assessment.ordinal,
            "alert_episode_start_ms": assessment.start_ms,
        }
        if assessment.suppressed and alerts.repeat_suppression:
            metadata["suppressed_repeat"] = True
            delivery_enabled = False
        line: str | None = None
        if alerts.alert_position_line:
            line = build_position_line(
                direction=decision.direction,
                ordinal=assessment.ordinal,
                feature=alert_inputs.feature,
                candles=alert_inputs.candles,
            ).text
        return decision.model_copy(update={"metadata": metadata}), delivery_enabled, line

    def _persist_decision(
        self, decision: SignalDecision, *, observation: RuleEvaluation | None = None,
        delivery_allowed: bool = True, alert_inputs: _AlertInputs | None = None,
    ) -> bool:
        """Durably store one immutable signal/outbox intent without dispatching it."""

        delivery_enabled = (
            delivery_allowed and self.settings.alerts.discord_enabled
            and self.settings.alerts.discord_webhook_url is not None
            and should_deliver_discord(decision, self.settings.alerts)
        )
        if delivery_enabled and decision.metadata.get("market_watch") is True:
            previous_ms = self.repository.latest_delivered_signal_time(
                decision.market, decision.symbol, decision.family
            )
            cooldown_ms = self.settings.alerts.market_watch.event_cooldown_seconds * 1000
            if previous_ms is not None and decision.event_time_ms - previous_ms < cooldown_ms:
                delivery_enabled = False
        position_line: str | None = None
        if (
            alert_inputs is not None
            and self.zone_tracker is not None
            and TrendZoneTracker.applies(decision)
        ):
            decision, delivery_enabled, position_line = self._apply_zone_policy(
                decision, delivery_enabled, alert_inputs
            )
        payload = build_discord_payload(
            decision,
            self.settings.alerts.discord_username,
            message_style=self.settings.alerts.message_style,
            wording_v2=self.settings.alerts.alert_wording_v2,
            position_line=position_line,
        )
        if observation is not None:
            return self.repository.save_signal_and_enqueue(
                decision, payload, self.clock.now_ms(),
                delivery_enabled=delivery_enabled,
                maximum_active_items=self.settings.alerts.outbox_max_active_items,
                observation=observation,
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
