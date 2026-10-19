from __future__ import annotations

from collections.abc import Collection, Mapping, Sequence
from dataclasses import dataclass

from signalbot.domain.enums import Direction, SignalFamily, SignalStage
from signalbot.domain.models import Candle, FeatureSnapshot, SignalDecision

# Closed 5m bars that make up the "4 hour" reference window.
FOUR_HOUR_BARS = 48
EXTENDED_ATR = 2.5
EXTENDED_FROM_EXTREME_ATR = 6.0
OVERSOLD_RSI = 30.0
OVERBOUGHT_RSI = 70.0

TREND_FAMILIES = frozenset({SignalFamily.BREAKDOWN_SHORT, SignalFamily.BREAKOUT_LONG})


@dataclass(slots=True)
class _Episode:
    start_ms: int
    last_alert_ms: int
    last_trigger_ms: int
    count: int
    last_suppressed: bool


@dataclass(frozen=True, slots=True)
class EpisodeAssessment:
    """Where one CONFIRMED trend alert sits inside its directional zone."""

    ordinal: int
    start_ms: int
    suppressed: bool


class TrendZoneTracker:
    """In-memory, bounded memory of the current directional zone per symbol.

    A zone starts with the first CONFIRMED ``breakdown_short`` (down) or
    ``breakout_long`` (up) of a symbol. It ends when the 5m close recovers through
    EMA20, the strictly-prior 1h alignment is lost, or ``max_hours`` have passed since
    the last alert that was actually sent. Triggers inside a live zone are counted
    and, when ``suppress`` is on, reported as suppressed (the caller still stores them).
    The state is not restored after a restart; the first alert afterwards is zone #1.
    """

    def __init__(self, *, suppress: bool, max_hours: int, maximum_symbols: int) -> None:
        if max_hours < 1 or maximum_symbols < 1:
            raise ValueError("max_hours and maximum_symbols must be positive")
        self.suppress = suppress
        self.max_ms = max_hours * 3_600_000
        self.maximum_symbols = maximum_symbols
        self._episodes: dict[tuple[str, Direction], _Episode] = {}

    @property
    def active_zone_count(self) -> int:
        return len(self._episodes)

    @staticmethod
    def applies(decision: SignalDecision) -> bool:
        return decision.family in TREND_FAMILIES and decision.stage is SignalStage.CONFIRMED

    def prune_symbols(self, active_symbols: Collection[str]) -> int:
        active = {symbol.upper() for symbol in active_symbols}
        stale = [key for key in self._episodes if key[0] not in active]
        for key in stale:
            del self._episodes[key]
        return len(stale)

    def update(self, feature: FeatureSnapshot, contexts: Mapping[str, FeatureSnapshot]) -> None:
        """End zones of this symbol whose closing conditions hold at this closed 5m bar."""

        symbol = feature.symbol.upper()
        for direction in (Direction.SHORT, Direction.LONG):
            key = (symbol, direction)
            episode = self._episodes.get(key)
            if episode is None:
                continue
            if self._ended(episode, direction, feature, contexts):
                del self._episodes[key]

    def register(self, decision: SignalDecision) -> EpisodeAssessment:
        """Count one CONFIRMED trend alert; idempotent for the same event time."""

        if not self.applies(decision):
            raise ValueError("only CONFIRMED breakdown_short/breakout_long start or join a zone")
        key = (decision.symbol.upper(), decision.direction)
        now_ms = decision.event_time_ms
        episode = self._episodes.get(key)
        if episode is not None and now_ms == episode.last_trigger_ms:
            return EpisodeAssessment(episode.count, episode.start_ms, episode.last_suppressed)
        if episode is not None and now_ms - episode.last_alert_ms >= self.max_ms:
            episode = None
        if episode is None:
            if key not in self._episodes and len(self._episodes) >= 2 * self.maximum_symbols:
                oldest = min(self._episodes, key=lambda item: self._episodes[item].last_trigger_ms)
                del self._episodes[oldest]
            episode = _Episode(now_ms, now_ms, now_ms, 1, False)
            self._episodes[key] = episode
            return EpisodeAssessment(1, now_ms, False)
        episode.count += 1
        episode.last_trigger_ms = now_ms
        episode.last_suppressed = self.suppress
        if not self.suppress:
            episode.last_alert_ms = now_ms
        return EpisodeAssessment(episode.count, episode.start_ms, self.suppress)

    def _ended(
        self,
        episode: _Episode,
        direction: Direction,
        feature: FeatureSnapshot,
        contexts: Mapping[str, FeatureSnapshot],
    ) -> bool:
        if direction is Direction.SHORT and feature.price > feature.ema20:
            return True
        if direction is Direction.LONG and feature.price < feature.ema20:
            return True
        hourly = contexts.get("1h")
        if hourly is not None and hourly.event_time_ms < feature.event_time_ms:
            aligned = (
                hourly.price < hourly.ema20 < hourly.ema50
                if direction is Direction.SHORT
                else hourly.price > hourly.ema20 > hourly.ema50
            )
            if not aligned:
                return True
        return feature.event_time_ms - episode.last_alert_ms >= self.max_ms


@dataclass(frozen=True, slots=True)
class PositionLine:
    text: str
    chase_risk: bool


def _extreme(candles: Sequence[Candle], direction: Direction) -> float | None:
    window = list(candles)[-FOUR_HOUR_BARS:]
    if len(window) < FOUR_HOUR_BARS:
        return None
    if direction is Direction.SHORT:
        return max(float(candle.high) for candle in window)
    return min(float(candle.low) for candle in window)


def build_position_line(
    *,
    direction: Direction,
    ordinal: int,
    feature: FeatureSnapshot,
    candles: Sequence[Candle],
) -> PositionLine:
    """Human-readable "where are we in the move" line and the chase-risk flag.

    Display only: the chase-risk thresholds are customary markers (not fitted to any
    sample) and never block an alert.
    """

    short = direction is Direction.SHORT
    price = feature.price
    extreme = _extreme(candles, direction)
    atr = feature.atr if feature.atr > 0 else None
    zone = "하락" if short else "상승"
    reference = "고점" if short else "저점"
    extreme_text = "n/a"
    from_extreme_atr: float | None = None
    if extreme is not None and extreme > 0:
        change = (price / extreme - 1.0) * 100.0
        extreme_text = f"{change:+.1f}%"
        if atr is not None:
            from_extreme_atr = abs(extreme - price) / atr
    ema_atr = None if atr is None else (price - feature.ema20) / atr
    ema_text = "n/a" if ema_atr is None else f"{ema_atr:+.1f}ATR"
    text = (
        f"이번 {zone} 구간 {ordinal}번째 알림 · 4시간 {reference} 대비 {extreme_text}"
        f" · EMA20 대비 {ema_text} · RSI {feature.rsi:.0f}"
    )
    if short:
        chase = (
            feature.rsi < OVERSOLD_RSI
            or (ema_atr is not None and ema_atr <= -EXTENDED_ATR)
            or (from_extreme_atr is not None and from_extreme_atr >= EXTENDED_FROM_EXTREME_ATR)
            or ordinal >= 2
        )
        tag = "⚠️ 추격 위험: 이미 많이 내림"
    else:
        chase = (
            feature.rsi > OVERBOUGHT_RSI
            or (ema_atr is not None and ema_atr >= EXTENDED_ATR)
            or (from_extreme_atr is not None and from_extreme_atr >= EXTENDED_FROM_EXTREME_ATR)
            or ordinal >= 2
        )
        tag = "⚠️ 추격 위험: 이미 많이 오름"
    return PositionLine(f"{text}\n{tag}" if chase else text, chase)
