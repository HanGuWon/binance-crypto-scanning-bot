from __future__ import annotations

from bisect import bisect_left
from dataclasses import dataclass
from typing import TypeVar

from signalbot.domain.enums import Market
from signalbot.domain.models import Candle, FeatureSnapshot, MarketRegime


@dataclass(frozen=True, slots=True)
class _ClosePoint:
    event_time_ms: int
    price: float


@dataclass(frozen=True, slots=True)
class _TrendPoint:
    event_time_ms: int
    trend: str


_PointT = TypeVar("_PointT", _ClosePoint, _TrendPoint)


class MarketRegimeEngine:
    """Bounded, point-in-time market regime inputs.

    Histories are indexed by exchange close time rather than arrival order. A
    snapshot with ``as_of_ms`` uses only values strictly before that timestamp,
    so same-close symbols and the same-close BTC hourly feature cannot leak into
    one another.
    """

    def __init__(self, maximum_points_per_symbol: int = 600) -> None:
        if maximum_points_per_symbol < 2:
            raise ValueError("maximum_points_per_symbol must be at least 2")
        self.maximum_points_per_symbol = maximum_points_per_symbol
        self._closes: dict[tuple[Market, str], list[_ClosePoint]] = {}
        self._btc_trends: dict[Market, list[_TrendPoint]] = {}

    def update_candle(self, candle: Candle, previous: Candle | None = None) -> None:
        """Insert a closed 5m price point; ``previous`` is retained for API compatibility."""

        del previous
        if not candle.is_closed or candle.interval != "5m":
            return
        key = (candle.market, candle.symbol)
        history = self._closes.setdefault(key, [])
        self._insert(
            history,
            _ClosePoint(candle.close_time_ms, float(candle.close)),
        )

    def update_feature(self, feature: FeatureSnapshot) -> None:
        if feature.symbol != "BTCUSDT" or feature.interval != "1h":
            return
        if feature.price > feature.ema20 > feature.ema50:
            trend = "bullish"
        elif feature.price < feature.ema20 < feature.ema50:
            trend = "bearish"
        else:
            trend = "neutral"
        history = self._btc_trends.setdefault(feature.market, [])
        self._insert(history, _TrendPoint(feature.event_time_ms, trend))

    def snapshot(self, market: Market, as_of_ms: int | None = None) -> MarketRegime:
        if as_of_ms is not None and as_of_ms < 0:
            raise ValueError("as_of_ms must be non-negative")
        directions: list[bool] = []
        for (history_market, _symbol), history in self._closes.items():
            if history_market is not market:
                continue
            end = self._available_count(history, as_of_ms)
            if end >= 2:
                directions.append(history[end - 1].price > history[end - 2].price)
        breadth = sum(directions) / len(directions) if directions else 0.5

        trend_history = self._btc_trends.get(market, [])
        trend_end = self._available_count(trend_history, as_of_ms)
        btc = trend_history[trend_end - 1].trend if trend_end else "neutral"
        label = (
            "risk_on"
            if btc == "bullish" and breadth >= 0.55
            else "risk_off"
            if btc == "bearish" and breadth <= 0.45
            else "neutral"
        )
        return MarketRegime(label=label, btc_trend=btc, breadth_ratio=breadth)

    def retain_symbols(self, market: Market, symbols: frozenset[str]) -> int:
        """Prune symbol histories, including BTC trend if BTC leaves the universe."""

        allowed = frozenset(symbol.upper() for symbol in symbols)
        stale = [
            key
            for key in self._closes
            if key[0] is market and key[1] not in allowed
        ]
        for key in stale:
            del self._closes[key]
        if "BTCUSDT" not in allowed:
            self._btc_trends.pop(market, None)
        return len(stale)

    def _insert(
        self,
        history: list[_PointT],
        point: _PointT,
    ) -> None:
        times = [item.event_time_ms for item in history]
        index = bisect_left(times, point.event_time_ms)
        if index < len(history) and history[index].event_time_ms == point.event_time_ms:
            history[index] = point
        else:
            history.insert(index, point)
        overflow = len(history) - self.maximum_points_per_symbol
        if overflow > 0:
            del history[:overflow]

    @staticmethod
    def _available_count(
        history: list[_PointT],
        as_of_ms: int | None,
    ) -> int:
        if as_of_ms is None:
            return len(history)
        times = [item.event_time_ms for item in history]
        return bisect_left(times, as_of_ms)
