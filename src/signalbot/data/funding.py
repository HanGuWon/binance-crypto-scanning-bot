from __future__ import annotations

import statistics
from bisect import bisect_left
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation


class FundingRatePayloadError(ValueError):
    """Raised when a public Binance funding-rate row is malformed."""


class FundingRateCapacityError(RuntimeError):
    """Raised instead of allowing the tracked-symbol cache to grow without bound."""


@dataclass(frozen=True, slots=True)
class FundingRatePoint:
    symbol: str
    funding_time_ms: int
    rate: Decimal

    def __post_init__(self) -> None:
        normalized = self.symbol.upper()
        if not normalized:
            raise ValueError("funding-rate symbol must not be empty")
        if self.funding_time_ms < 0:
            raise ValueError("funding_time_ms must be non-negative")
        if not self.rate.is_finite():
            raise ValueError("funding rate must be finite")
        object.__setattr__(self, "symbol", normalized)


@dataclass(frozen=True, slots=True)
class FundingRateSnapshot:
    """Latest settled funding input that was available at an exact decision time."""

    symbol: str
    funding_time_ms: int
    rate: float
    zscore: float
    prior_sample_size: int


def parse_funding_rate_point(
    expected_symbol: str, payload: Mapping[str, object]
) -> FundingRatePoint:
    """Parse one REST row without replacing missing or invalid data with zero."""
    symbol = str(payload.get("symbol", "")).upper()
    expected = expected_symbol.upper()
    if not symbol or symbol != expected:
        raise FundingRatePayloadError(
            f"funding-rate symbol mismatch: expected={expected!r} received={symbol!r}"
        )

    raw_time = payload.get("fundingTime")
    if isinstance(raw_time, bool) or not isinstance(raw_time, (int, str)):
        raise FundingRatePayloadError("fundingTime must be an integer timestamp")
    try:
        funding_time_ms = int(raw_time)
    except (TypeError, ValueError) as exc:
        raise FundingRatePayloadError("fundingTime must be an integer timestamp") from exc

    raw_rate = payload.get("fundingRate")
    if raw_rate is None:
        raise FundingRatePayloadError("fundingRate is required")
    try:
        rate = Decimal(str(raw_rate))
    except (InvalidOperation, ValueError) as exc:
        raise FundingRatePayloadError("fundingRate must be a decimal") from exc
    try:
        return FundingRatePoint(symbol, funding_time_ms, rate)
    except ValueError as exc:
        raise FundingRatePayloadError(str(exc)) from exc


class FundingRateTracker:
    """Bounded point-in-time funding history for a bounded futures universe."""

    def __init__(
        self,
        maximum_points: int,
        minimum_history: int,
        maximum_symbols: int,
        lookback_ms: int = 30 * 86_400_000,
    ) -> None:
        if minimum_history < 2:
            raise ValueError("minimum_history must be at least 2")
        if maximum_points < minimum_history + 1:
            raise ValueError("maximum_points must retain the current point and minimum_history")
        if maximum_symbols < 1:
            raise ValueError("maximum_symbols must be positive")
        if lookback_ms <= 0:
            raise ValueError("lookback_ms must be positive")
        self.maximum_points = maximum_points
        self.minimum_history = minimum_history
        self.maximum_symbols = maximum_symbols
        self.lookback_ms = lookback_ms
        self._histories: dict[str, list[FundingRatePoint]] = {}

    def ingest_payloads(self, symbol: str, payloads: Iterable[Mapping[str, object]]) -> int:
        """Validate a REST page atomically, then insert new or corrected points."""
        points = [parse_funding_rate_point(symbol, payload) for payload in payloads]
        return sum(self.update(point) for point in points)

    def update(self, point: FundingRatePoint) -> bool:
        """Insert a point by exchange timestamp while remaining idempotent and bounded."""
        symbol = point.symbol.upper()
        history = self._histories.get(symbol)
        if history is None:
            if len(self._histories) >= self.maximum_symbols:
                raise FundingRateCapacityError(
                    f"funding-rate symbol capacity exceeded: {self.maximum_symbols}"
                )
            history = []
            self._histories[symbol] = history

        times = [item.funding_time_ms for item in history]
        index = bisect_left(times, point.funding_time_ms)
        if index < len(history) and history[index].funding_time_ms == point.funding_time_ms:
            if history[index] == point:
                return False
            history[index] = point
            return True

        history.insert(index, point)
        overflow = len(history) - self.maximum_points
        if overflow > 0:
            del history[:overflow]
        return point in history

    def latest_time_ms(self, symbol: str) -> int | None:
        history = self._histories.get(symbol.upper())
        return history[-1].funding_time_ms if history else None

    def retain_symbols(self, symbols: frozenset[str]) -> int:
        """Prune settled histories for contracts outside the active futures universe."""

        allowed = frozenset(symbol.upper() for symbol in symbols)
        stale = [symbol for symbol in self._histories if symbol not in allowed]
        for symbol in stale:
            del self._histories[symbol]
        return len(stale)

    def snapshot(
        self,
        symbol: str,
        as_of_ms: int,
        maximum_age_ms: int,
    ) -> FundingRateSnapshot | None:
        """Return a fresh PIT z-score, using only observations strictly before the latest."""
        if as_of_ms < 0:
            raise ValueError("as_of_ms must be non-negative")
        if maximum_age_ms < 0:
            raise ValueError("maximum_age_ms must be non-negative")
        normalized = symbol.upper()
        history = self._histories.get(normalized, [])
        eligible = [point for point in history if point.funding_time_ms < as_of_ms]
        if len(eligible) < self.minimum_history + 1:
            return None

        latest = eligible[-1]
        if as_of_ms - latest.funding_time_ms > maximum_age_ms:
            return None
        cutoff_ms = latest.funding_time_ms - self.lookback_ms
        prior = [
            float(point.rate)
            for point in eligible[:-1]
            if point.funding_time_ms >= cutoff_ms
        ]
        if len(prior) < self.minimum_history:
            return None
        deviation = statistics.pstdev(prior)
        zscore = 0.0
        if deviation > 0:
            zscore = (float(latest.rate) - statistics.fmean(prior)) / deviation
        return FundingRateSnapshot(
            symbol=normalized,
            funding_time_ms=latest.funding_time_ms,
            rate=float(latest.rate),
            zscore=zscore,
            prior_sample_size=len(prior),
        )
