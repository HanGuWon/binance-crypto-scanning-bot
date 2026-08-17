from __future__ import annotations

from bisect import bisect_left
from dataclasses import dataclass

from signalbot.domain.enums import Market
from signalbot.domain.models import Candle

_INTERVAL_UNITS = {"m": 60_000, "h": 3_600_000, "d": 86_400_000, "w": 604_800_000}


def interval_to_milliseconds(interval: str) -> int:
    if len(interval) < 2 or interval[-1] not in _INTERVAL_UNITS:
        raise ValueError(f"unsupported interval: {interval}")
    try:
        amount = int(interval[:-1])
    except ValueError as exc:
        raise ValueError(f"unsupported interval: {interval}") from exc
    if amount <= 0:
        raise ValueError(f"unsupported interval: {interval}")
    return amount * _INTERVAL_UNITS[interval[-1]]


@dataclass(frozen=True, slots=True)
class CandleGap:
    market: Market
    symbol: str
    interval: str
    start_time_ms: int
    end_time_ms: int


class CandleStore:
    def __init__(self, history_limit: int) -> None:
        self.history_limit = history_limit
        self._series: dict[tuple[Market, str, str], list[Candle]] = {}

    @staticmethod
    def key(candle: Candle) -> tuple[Market, str, str]:
        return candle.market, candle.symbol, candle.interval

    def add(self, candle: Candle) -> bool:
        if not candle.is_closed:
            return False
        series = self._series.setdefault(self.key(candle), [])
        positions = [item.open_time_ms for item in series]
        index = bisect_left(positions, candle.open_time_ms)
        if index < len(series) and series[index].open_time_ms == candle.open_time_ms:
            if series[index] == candle:
                return False
            series[index] = candle
        else:
            series.insert(index, candle)
        if len(series) > self.history_limit:
            del series[: len(series) - self.history_limit]
        return True

    def add_many(self, candles: list[Candle]) -> int:
        return sum(int(self.add(c)) for c in sorted(candles, key=lambda x: x.open_time_ms))

    def detect_latest_gap(self, candle: Candle) -> CandleGap | None:
        series = self.get(candle.market, candle.symbol, candle.interval)
        if not series or candle.open_time_ms <= series[-1].open_time_ms:
            return None
        step = interval_to_milliseconds(candle.interval)
        expected = series[-1].open_time_ms + step
        if candle.open_time_ms <= expected:
            return None
        return CandleGap(
            candle.market, candle.symbol, candle.interval, expected, candle.open_time_ms - 1
        )

    def get(self, market: Market, symbol: str, interval: str) -> list[Candle]:
        return list(self._series.get((market, symbol.upper(), interval), []))

    def latest(self, market: Market, symbol: str, interval: str) -> Candle | None:
        series = self._series.get((market, symbol.upper(), interval))
        return series[-1] if series else None

    def size(self, market: Market, symbol: str, interval: str) -> int:
        return len(self._series.get((market, symbol.upper(), interval), []))

    def series(self, market: Market | None = None) -> list[list[Candle]]:
        """Return bounded series copies in deterministic key order."""

        keys = sorted(
            (key for key in self._series if market is None or key[0] is market),
            key=lambda key: (key[0].value, key[1], key[2]),
        )
        return [list(self._series[key]) for key in keys]

    def retain_symbols(self, market: Market, symbols: frozenset[str]) -> int:
        """Drop all series for symbols that left one market's active universe."""

        allowed = frozenset(symbol.upper() for symbol in symbols)
        stale = [
            key
            for key in self._series
            if key[0] is market and key[1] not in allowed
        ]
        for key in stale:
            del self._series[key]
        return len(stale)
