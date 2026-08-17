from __future__ import annotations

import hashlib
from collections.abc import Collection, Sequence
from copy import deepcopy
from dataclasses import dataclass
from decimal import Decimal
from enum import StrEnum
from typing import Protocol

from signalbot.data.candles import interval_to_milliseconds
from signalbot.domain.enums import Direction, Market, SignalFamily, SignalStage
from signalbot.domain.models import Candle, FeatureSnapshot, SignalDecision


class ExitPolicy(Protocol):
    trend_failure_bars: int
    trailing_activation_r: float
    trailing_atr_multiple: float
    max_holding_bars: int


class RuntimeExitPolicy(ExitPolicy, Protocol):
    enabled: bool


class ExitReason(StrEnum):
    INITIAL_STOP = "initial_stop"
    TRAILING_STOP = "trailing_stop"
    OPPOSITE_SIGNAL = "opposite_signal"
    TREND_FAILURE = "trend_failure"
    TIME_EXIT = "time_exit"
    SPLIT_BOUNDARY = "split_boundary"
    DATA_GAP = "data_gap"
    END_OF_DATA = "end_of_data"


@dataclass(slots=True)
class PaperPosition:
    decision: SignalDecision
    entry_index: int
    entry_time_ms: int
    entry_price: float
    initial_stop: float
    active_stop: float
    active_stop_reason: ExitReason
    highest_price: float
    lowest_price: float
    trend_failure_count: int = 0

    @property
    def direction(self) -> Direction:
        return self.decision.direction

    @property
    def initial_risk(self) -> float:
        return abs(self.entry_price - self.initial_stop)


@dataclass(frozen=True, slots=True)
class StopFill:
    price: float
    reason: ExitReason


class TechnicalExitEngine:
    """Closed-candle exit policy shared by paper trading and backtests.

    Stops stored on ``PaperPosition`` are known before the next bar. Trailing
    updates made after a close therefore cannot act on that same bar.
    """

    def __init__(self, settings: ExitPolicy) -> None:
        self.settings = settings

    @staticmethod
    def open_position(
        decision: SignalDecision,
        candle: Candle,
        entry_index: int,
    ) -> PaperPosition | None:
        decision = SignalDecision.model_validate(
            decision.model_dump(mode="python", warnings="none")
        )
        if decision.invalidation is None:
            return None
        entry = float(candle.open)
        stop = float(decision.invalidation)
        if entry <= 0 or stop <= 0:
            return None
        if decision.direction is Direction.LONG and entry <= stop:
            return None
        if decision.direction is Direction.SHORT and entry >= stop:
            return None
        if decision.direction not in {Direction.LONG, Direction.SHORT}:
            return None
        return PaperPosition(
            decision=decision,
            entry_index=entry_index,
            entry_time_ms=candle.open_time_ms,
            entry_price=entry,
            initial_stop=stop,
            active_stop=stop,
            active_stop_reason=ExitReason.INITIAL_STOP,
            highest_price=entry,
            lowest_price=entry,
        )

    @staticmethod
    def stop_at_open(position: PaperPosition, open_price: float) -> StopFill | None:
        if position.direction is Direction.LONG and open_price <= position.active_stop:
            return StopFill(open_price, position.active_stop_reason)
        if position.direction is Direction.SHORT and open_price >= position.active_stop:
            return StopFill(open_price, position.active_stop_reason)
        return None

    @staticmethod
    def stop_in_bar(position: PaperPosition, candle: Candle) -> StopFill | None:
        if position.direction is Direction.LONG and float(candle.low) <= position.active_stop:
            return StopFill(position.active_stop, position.active_stop_reason)
        if position.direction is Direction.SHORT and float(candle.high) >= position.active_stop:
            return StopFill(position.active_stop, position.active_stop_reason)
        return None

    def after_close(
        self,
        position: PaperPosition,
        candle: Candle,
        feature: FeatureSnapshot,
        current_index: int,
        opposite_confirmed: bool,
    ) -> ExitReason | None:
        position.highest_price = max(position.highest_price, float(candle.high))
        position.lowest_price = min(position.lowest_price, float(candle.low))

        failed = (
            feature.price < feature.ema20 and feature.macd_histogram < 0
            if position.direction is Direction.LONG
            else feature.price > feature.ema20 and feature.macd_histogram > 0
        )
        position.trend_failure_count = position.trend_failure_count + 1 if failed else 0

        self._update_trailing_stop(position, feature.atr)

        if opposite_confirmed:
            return ExitReason.OPPOSITE_SIGNAL
        if position.trend_failure_count >= self.settings.trend_failure_bars:
            return ExitReason.TREND_FAILURE
        held_bars = current_index - position.entry_index + 1
        if held_bars >= self.settings.max_holding_bars:
            return ExitReason.TIME_EXIT
        return None

    def _update_trailing_stop(self, position: PaperPosition, atr: float) -> None:
        if position.initial_risk <= 0 or atr <= 0:
            return
        activation = position.initial_risk * self.settings.trailing_activation_r
        if position.direction is Direction.LONG:
            if position.highest_price - position.entry_price < activation:
                return
            candidate = position.highest_price - self.settings.trailing_atr_multiple * atr
            if candidate > position.active_stop:
                position.active_stop = candidate
                position.active_stop_reason = ExitReason.TRAILING_STOP
        else:
            if position.entry_price - position.lowest_price < activation:
                return
            candidate = position.lowest_price + self.settings.trailing_atr_multiple * atr
            if candidate < position.active_stop:
                position.active_stop = candidate
                position.active_stop_reason = ExitReason.TRAILING_STOP


@dataclass(slots=True)
class _SymbolLifecycle:
    last_open_time_ms: int | None = None
    bar_index: int = -1
    pending_entry: SignalDecision | None = None
    position: PaperPosition | None = None
    pending_exit: ExitReason | None = None
    last_closed_feature: FeatureSnapshot | None = None


@dataclass(frozen=True, slots=True)
class PaperLifecycleCheckpoint:
    """Opaque, bounded snapshot used to roll back one failed durable transition."""

    symbol: str
    _state: _SymbolLifecycle | None


class PaperPositionLifecycle:
    """Bounded, alert-only PAPER lifecycle driven by fully closed primary bars.

    State is intentionally in memory and is not reconstructed from persisted
    signal rows after a process restart. A restart therefore forgets pending
    entries and open PAPER positions; it never implies an exchange-side action.
    """

    POLICY_VERSION = "paper-technical-exit-v1"

    def __init__(
        self,
        settings: RuntimeExitPolicy,
        *,
        rule_version: str,
        market: Market,
        primary_interval: str,
        maximum_symbols: int,
    ) -> None:
        if maximum_symbols < 1:
            raise ValueError("maximum_symbols must be positive")
        self.settings = settings
        self.rule_version = rule_version
        self.market = market
        self.primary_interval = primary_interval
        self.maximum_symbols = maximum_symbols
        self.interval_ms = interval_to_milliseconds(primary_interval)
        self.engine = TechnicalExitEngine(settings)
        self._states: dict[str, _SymbolLifecycle] = {}

    @property
    def tracked_symbol_count(self) -> int:
        return len(self._states)

    @property
    def active_position_count(self) -> int:
        return sum(state.position is not None for state in self._states.values())

    @property
    def pending_entry_count(self) -> int:
        return sum(state.pending_entry is not None for state in self._states.values())

    def checkpoint_symbol(self, symbol: str) -> PaperLifecycleCheckpoint:
        """Snapshot only one bounded symbol state before attempting persistence."""

        normalized = symbol.upper()
        return PaperLifecycleCheckpoint(
            symbol=normalized,
            _state=deepcopy(self._states.get(normalized)),
        )

    def restore_checkpoint(self, checkpoint: PaperLifecycleCheckpoint) -> None:
        """Restore a symbol after its transition could not be persisted."""

        if checkpoint._state is None:
            self._states.pop(checkpoint.symbol, None)
            return
        if (
            checkpoint.symbol not in self._states
            and len(self._states) >= self.maximum_symbols
        ):
            raise RuntimeError("paper lifecycle cannot restore beyond its symbol bound")
        self._states[checkpoint.symbol] = deepcopy(checkpoint._state)

    def prune_symbols(self, active_symbols: Collection[str]) -> int:
        active = {symbol.upper() for symbol in active_symbols}
        stale = [symbol for symbol in self._states if symbol not in active]
        for symbol in stale:
            del self._states[symbol]
        return len(stale)

    def reset_for_gap(self, candle: Candle) -> list[SignalDecision]:
        """Fail closed at the first post-gap open and forget all symbol state."""

        if not self.settings.enabled or not self._is_primary_closed_candle(candle):
            return []
        symbol = candle.symbol.upper()
        state = self._states.pop(symbol, None)
        if state is None or state.position is None:
            return []
        regime_feature = state.last_closed_feature
        return [
            self._exit_decision(
                state.position,
                reason=ExitReason.DATA_GAP,
                price=float(candle.open),
                fill_time_ms=candle.open_time_ms,
                observed_at_ms=candle.close_time_ms,
                held_bars=max(0, state.bar_index - state.position.entry_index + 1),
                execution_model="paper_first_post_gap_open",
                regime_feature=regime_feature,
                regime_context_source=(
                    "last_pre_gap_closed_primary"
                    if regime_feature is not None
                    else "entry_signal_fallback"
                ),
                regime_observed_at_ms=(
                    regime_feature.event_time_ms
                    if regime_feature is not None
                    else state.position.decision.event_time_ms
                ),
            )
        ]

    def on_closed_candle(
        self,
        candle: Candle,
        feature: FeatureSnapshot,
        new_decisions: Sequence[SignalDecision],
    ) -> list[SignalDecision]:
        """Advance one symbol after its primary candle has fully closed.

        ``new_decisions`` must contain only decisions that were newly persisted
        atomically with their Discord outbox intent during this same close.
        """

        if not self.settings.enabled:
            return []
        if not self._is_primary_closed_candle(candle):
            raise ValueError("paper lifecycle requires a closed primary candle")
        self._validate_feature(candle, feature)
        new_decisions = tuple(
            SignalDecision.model_validate(
                decision.model_dump(mode="python", warnings="none")
            )
            for decision in new_decisions
        )
        symbol = candle.symbol.upper()
        state = self._state(symbol)
        if (
            state.last_open_time_ms is not None
            and candle.open_time_ms <= state.last_open_time_ms
        ):
            return []

        exits: list[SignalDecision] = []
        if (
            state.last_open_time_ms is not None
            and candle.open_time_ms - state.last_open_time_ms != self.interval_ms
        ):
            exits.extend(self.reset_for_gap(candle))
            state = self._state(symbol)

        state.last_open_time_ms = candle.open_time_ms
        state.bar_index += 1
        self._advance_existing_position(state, candle, feature, exits)
        self._open_pending_entry(state, candle, feature, exits)

        if state.position is not None:
            opposite = self._has_opposite_confirmed(
                state.position, candle, new_decisions
            )
            state.pending_exit = self.engine.after_close(
                state.position,
                candle,
                feature,
                state.bar_index,
                opposite,
            )

        if state.position is None and state.pending_entry is None:
            candidates = [
                decision
                for decision in new_decisions
                if self._valid_entry_candidate(decision, candle)
            ]
            if candidates:
                state.pending_entry = sorted(
                    candidates,
                    key=lambda item: (-item.score, item.family.value, item.event_id),
                )[0]
        state.last_closed_feature = feature
        return exits

    def _advance_existing_position(
        self,
        state: _SymbolLifecycle,
        candle: Candle,
        feature: FeatureSnapshot,
        exits: list[SignalDecision],
    ) -> None:
        position = state.position
        if position is None:
            return
        prior_feature = state.last_closed_feature
        regime_source = (
            "strict_prior_closed_primary"
            if prior_feature is not None
            else "entry_signal_fallback"
        )
        regime_observed_at_ms = (
            prior_feature.event_time_ms
            if prior_feature is not None
            else position.decision.event_time_ms
        )
        stop = self.engine.stop_at_open(position, float(candle.open))
        if stop is not None:
            exits.append(
                self._exit_decision(
                    position,
                    reason=stop.reason,
                    price=stop.price,
                    fill_time_ms=candle.open_time_ms,
                    observed_at_ms=candle.close_time_ms,
                    held_bars=state.bar_index - position.entry_index,
                    execution_model="paper_stop_gap_at_open",
                    regime_feature=prior_feature,
                    regime_context_source=regime_source,
                    regime_observed_at_ms=regime_observed_at_ms,
                )
            )
            self._clear_position(state)
            return
        if state.pending_exit is not None:
            exits.append(
                self._exit_decision(
                    position,
                    reason=state.pending_exit,
                    price=float(candle.open),
                    fill_time_ms=candle.open_time_ms,
                    observed_at_ms=candle.close_time_ms,
                    held_bars=state.bar_index - position.entry_index,
                    execution_model="paper_next_bar_open",
                    regime_feature=prior_feature,
                    regime_context_source=regime_source,
                    regime_observed_at_ms=regime_observed_at_ms,
                )
            )
            self._clear_position(state)
            return
        stop = self.engine.stop_in_bar(position, candle)
        if stop is None:
            return
        exits.append(
            self._exit_decision(
                position,
                reason=stop.reason,
                price=stop.price,
                fill_time_ms=candle.close_time_ms,
                observed_at_ms=candle.close_time_ms,
                held_bars=state.bar_index - position.entry_index + 1,
                execution_model="paper_stop_touch_in_closed_bar",
                regime_feature=feature,
                regime_context_source="observation_closed_primary",
                regime_observed_at_ms=feature.event_time_ms,
            )
        )
        self._clear_position(state)

    def _open_pending_entry(
        self,
        state: _SymbolLifecycle,
        candle: Candle,
        feature: FeatureSnapshot,
        exits: list[SignalDecision],
    ) -> None:
        if state.position is not None or state.pending_entry is None:
            return
        position = self.engine.open_position(
            state.pending_entry,
            candle,
            state.bar_index,
        )
        state.pending_entry = None
        if position is None:
            return
        state.position = position
        stop = self.engine.stop_in_bar(position, candle)
        if stop is None:
            return
        exits.append(
            self._exit_decision(
                position,
                reason=stop.reason,
                price=stop.price,
                fill_time_ms=candle.close_time_ms,
                observed_at_ms=candle.close_time_ms,
                held_bars=1,
                execution_model="paper_stop_touch_in_entry_bar",
                regime_feature=feature,
                regime_context_source="observation_closed_primary",
                regime_observed_at_ms=feature.event_time_ms,
            )
        )
        self._clear_position(state)

    @staticmethod
    def _clear_position(state: _SymbolLifecycle) -> None:
        state.position = None
        state.pending_exit = None

    def _state(self, symbol: str) -> _SymbolLifecycle:
        existing = self._states.get(symbol)
        if existing is not None:
            return existing
        if len(self._states) >= self.maximum_symbols:
            raise RuntimeError("paper lifecycle reached its configured symbol bound")
        created = _SymbolLifecycle()
        self._states[symbol] = created
        return created

    def _is_primary_closed_candle(self, candle: Candle) -> bool:
        return (
            candle.market is self.market
            and candle.interval == self.primary_interval
            and candle.is_closed
        )

    @staticmethod
    def _validate_feature(candle: Candle, feature: FeatureSnapshot) -> None:
        if (
            feature.market is not candle.market
            or feature.symbol != candle.symbol
            or feature.interval != candle.interval
            or feature.event_time_ms != candle.close_time_ms
        ):
            raise ValueError("paper lifecycle feature does not match its closed candle")

    @staticmethod
    def _technical_family(decision: SignalDecision) -> bool:
        return decision.family not in {
            SignalFamily.PUMP_RISK,
            SignalFamily.CRASH_RISK,
            SignalFamily.TECHNICAL_EXIT,
        }

    def _valid_entry_candidate(
        self, decision: SignalDecision, candle: Candle
    ) -> bool:
        if (
            decision.market is not self.market
            or decision.symbol != candle.symbol
            or decision.timeframe != self.primary_interval
            or decision.stage is not SignalStage.CONFIRMED
            or decision.event_time_ms != candle.close_time_ms
            or decision.invalidation is None
            or decision.invalidation <= 0
            or not self._technical_family(decision)
        ):
            return False
        if self.market is Market.SPOT and decision.direction is not Direction.LONG:
            return False
        if decision.direction is Direction.LONG:
            return decision.invalidation < decision.price
        if decision.direction is Direction.SHORT:
            return decision.invalidation > decision.price
        return False

    def _has_opposite_confirmed(
        self,
        position: PaperPosition,
        candle: Candle,
        decisions: Sequence[SignalDecision],
    ) -> bool:
        return any(
            decision.market is self.market
            and decision.symbol == candle.symbol
            and decision.timeframe == self.primary_interval
            and decision.event_time_ms == candle.close_time_ms
            and decision.stage is SignalStage.CONFIRMED
            and decision.direction is not position.direction
            and self._technical_family(decision)
            for decision in decisions
        )

    def _exit_decision(
        self,
        position: PaperPosition,
        *,
        reason: ExitReason,
        price: float,
        fill_time_ms: int,
        observed_at_ms: int,
        held_bars: int,
        execution_model: str,
        regime_feature: FeatureSnapshot | None,
        regime_context_source: str,
        regime_observed_at_ms: int,
    ) -> SignalDecision:
        identity = "|".join(
            (
                self.market.value,
                position.decision.symbol,
                SignalFamily.TECHNICAL_EXIT.value,
                position.decision.event_id,
                reason.value,
                str(fill_time_ms),
                self.rule_version,
                self.POLICY_VERSION,
            )
        )
        event_id = hashlib.sha256(identity.encode()).hexdigest()[:24]
        invalidation = Decimal(str(position.active_stop))
        return SignalDecision(
            event_id=event_id,
            market=self.market,
            symbol=position.decision.symbol,
            family=SignalFamily.TECHNICAL_EXIT,
            stage=SignalStage.CONFIRMED,
            direction=position.direction,
            timeframe=self.primary_interval,
            event_time_ms=fill_time_ms,
            score=100,
            price=Decimal(str(price)),
            reasons=(
                f"PAPER technical exit: {reason.value}",
                f"active invalidation/stop {invalidation}",
                "alert-only lifecycle; no exchange order was placed",
            ),
            invalidation=invalidation,
            regime=(
                regime_feature.regime
                if regime_feature is not None
                else position.decision.regime
            ),
            rule_version=self.rule_version,
            metadata={
                "paper_only": True,
                "order_placed": False,
                "policy_version": self.POLICY_VERSION,
                "state_persistence": "in_memory_only_not_restored_after_restart",
                "entry_event_id": position.decision.event_id,
                "entry_family": position.decision.family.value,
                "entry_time_ms": position.entry_time_ms,
                "entry_price": position.entry_price,
                "initial_stop": position.initial_stop,
                "active_stop": position.active_stop,
                "exit_reason": reason.value,
                "fill_time_ms": fill_time_ms,
                "observed_at_closed_candle_ms": observed_at_ms,
                "execution_model": execution_model,
                "held_bars": held_bars,
                "regime_context_source": regime_context_source,
                "regime_observed_at_ms": regime_observed_at_ms,
            },
        )
