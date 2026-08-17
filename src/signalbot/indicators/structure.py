from __future__ import annotations

import math
import statistics
from bisect import bisect_left, bisect_right
from collections.abc import Sequence
from dataclasses import dataclass
from operator import attrgetter
from typing import Literal

from signalbot.domain.models import Candle, ChartStructureSnapshot

PIVOT_LEFT_BARS = 2
PIVOT_RIGHT_BARS = 2
PIVOT_TTL_BARS = 100
MINIMUM_PROMINENCE_ATR = 0.75
STRUCTURE_EQUALITY_ATR = 0.10
MINIMUM_IMPULSE_ATR = 2.0
MINIMUM_PULLBACK_DEPTH = 0.20
MAXIMUM_PULLBACK_DEPTH = 0.60
INVALID_PULLBACK_DEPTH = 0.75
MAXIMUM_PULLBACK_BARS = 12
CONFLUENCE_DISTANCE_ATR = 0.25
STRUCTURE_BREAK_BUFFER_ATR = 0.25


@dataclass(frozen=True, slots=True)
class ConfirmedPivot:
    kind: Literal["high", "low"]
    index: int
    price: float
    availability_index: int
    prominence: float


def confirmed_pivots(
    candles: Sequence[Candle],
    *,
    left: int = PIVOT_LEFT_BARS,
    right: int = PIVOT_RIGHT_BARS,
) -> tuple[ConfirmedPivot, ...]:
    """Return fractal pivots with an explicit delayed availability index.

    Ties on the right select the earliest plateau point. Ties on the left reject
    a later duplicate, so a flat extreme cannot create several anchors.
    """

    if left < 1 or right < 1:
        raise ValueError("pivot left and right windows must both be positive")
    if any(not candle.is_closed for candle in candles):
        raise ValueError("pivot confirmation requires a fully closed candle sequence")
    pivots: list[ConfirmedPivot] = []
    for index in range(left, len(candles) - right):
        high = float(candles[index].high)
        low = float(candles[index].low)
        left_rows = candles[index - left : index]
        right_rows = candles[index + 1 : index + right + 1]

        is_high = high > max(float(row.high) for row in left_rows) and high >= max(
            float(row.high) for row in right_rows
        )
        if is_high:
            left_trough = min(float(row.low) for row in left_rows)
            right_trough = min(float(row.low) for row in right_rows)
            pivots.append(
                ConfirmedPivot(
                    kind="high",
                    index=index,
                    price=high,
                    availability_index=index + right,
                    prominence=max(0.0, high - max(left_trough, right_trough)),
                )
            )

        is_low = low < min(float(row.low) for row in left_rows) and low <= min(
            float(row.low) for row in right_rows
        )
        if is_low:
            left_peak = max(float(row.high) for row in left_rows)
            right_peak = max(float(row.high) for row in right_rows)
            pivots.append(
                ConfirmedPivot(
                    kind="low",
                    index=index,
                    price=low,
                    availability_index=index + right,
                    prominence=max(0.0, min(left_peak, right_peak) - low),
                )
            )
    return tuple(pivots)


def _qualified_pivots(
    pivots: Sequence[ConfirmedPivot],
    atr: Sequence[float | None],
    *,
    reference_index: int,
) -> list[ConfirmedPivot]:
    first_index = max(0, reference_index - PIVOT_TTL_BARS + 1)
    first = bisect_left(pivots, first_index, key=attrgetter("index"))
    last = bisect_right(
        pivots,
        reference_index,
        key=attrgetter("availability_index"),
    )
    values: list[ConfirmedPivot] = []
    for pivot in pivots[first:last]:
        pivot_atr = atr[pivot.availability_index]
        if pivot_atr is None or not math.isfinite(pivot_atr) or pivot_atr <= 0:
            continue
        if pivot.prominence / pivot_atr >= MINIMUM_PROMINENCE_ATR:
            values.append(pivot)
    return values


def _line(first: ConfirmedPivot, second: ConfirmedPivot, index: int) -> tuple[float, float]:
    span = second.index - first.index
    if span <= 0:  # pragma: no cover - pivots are emitted in index order
        raise ValueError("trendline anchors must be strictly ordered")
    slope = (second.price - first.price) / span
    return second.price + slope * (index - second.index), slope


def _true_range(candles: Sequence[Candle], index: int) -> float:
    current = candles[index]
    if index == 0:
        return float(current.high - current.low)
    previous_close = float(candles[index - 1].close)
    return max(
        float(current.high - current.low),
        abs(float(current.high) - previous_close),
        abs(float(current.low) - previous_close),
    )


def _median_ratio(numerator: Sequence[float], denominator: Sequence[float]) -> float | None:
    if not numerator or not denominator:
        return None
    baseline = statistics.median(denominator)
    if baseline <= 0:
        return None
    return statistics.median(numerator) / baseline


def _pullback_snapshot(
    candles: Sequence[Candle],
    *,
    index: int,
    state: str,
    highs: Sequence[ConfirmedPivot],
    lows: Sequence[ConfirmedPivot],
    reference_atr: float,
    atr: Sequence[float | None],
    ema20: Sequence[float | None],
) -> ChartStructureSnapshot:
    unavailable = ChartStructureSnapshot(pullback_status="none")
    if state == "bullish":
        end = highs[-1]
        starts = [pivot for pivot in lows if pivot.index < end.index]
        direction: Literal["long", "short"] = "long"
        if not starts:
            return unavailable
        start = starts[-1]
        impulse = end.price - start.price
        pullback_rows = candles[end.index + 1 : index + 1]
        if not pullback_rows or impulse <= 0:
            return unavailable
        extremum_index = min(
            range(end.index + 1, index + 1),
            key=lambda row_index: float(candles[row_index].low),
        )
        extremum = float(candles[extremum_index].low)
        depth = (end.price - extremum) / impulse
        recovery = float(candles[index].close) > float(candles[index - 1].high)
        intact = float(candles[index].close) >= (
            start.price - STRUCTURE_BREAK_BUFFER_ATR * reference_atr
        )
    elif state == "bearish":
        end = lows[-1]
        starts = [pivot for pivot in highs if pivot.index < end.index]
        direction = "short"
        if not starts:
            return unavailable
        start = starts[-1]
        impulse = start.price - end.price
        pullback_rows = candles[end.index + 1 : index + 1]
        if not pullback_rows or impulse <= 0:
            return unavailable
        extremum_index = max(
            range(end.index + 1, index + 1),
            key=lambda row_index: float(candles[row_index].high),
        )
        extremum = float(candles[extremum_index].high)
        depth = (extremum - end.price) / impulse
        recovery = float(candles[index].close) < float(candles[index - 1].low)
        intact = float(candles[index].close) <= (
            start.price + STRUCTURE_BREAK_BUFFER_ATR * reference_atr
        )
    else:
        return unavailable

    duration = index - end.index
    impulse_ranges = [_true_range(candles, row) for row in range(start.index, end.index + 1)]
    pullback_ranges = [
        _true_range(candles, row) for row in range(end.index + 1, index + 1)
    ]
    impulse_quote = [
        float(candles[row].quote_volume) for row in range(start.index, end.index + 1)
    ]
    pullback_quote = [
        float(candles[row].quote_volume) for row in range(end.index + 1, index + 1)
    ]
    impulse_reference_atr = atr[end.availability_index]
    impulse_atr = (
        None
        if impulse_reference_atr is None
        or not math.isfinite(impulse_reference_atr)
        or impulse_reference_atr <= 0
        else impulse / impulse_reference_atr
    )
    confluence_reference_index = max(0, extremum_index - 1)
    confluence_reference_atr = atr[confluence_reference_index]
    confluence_reference_ema20 = ema20[confluence_reference_index]
    confluence = (
        None
        if confluence_reference_atr is None
        or confluence_reference_ema20 is None
        or not math.isfinite(confluence_reference_atr)
        or not math.isfinite(confluence_reference_ema20)
        or confluence_reference_atr <= 0
        else abs(extremum - confluence_reference_ema20) / confluence_reference_atr
    )
    ready = (
        impulse_atr is not None
        and impulse_atr >= MINIMUM_IMPULSE_ATR
        and MINIMUM_PULLBACK_DEPTH <= depth <= MAXIMUM_PULLBACK_DEPTH
        and duration <= MAXIMUM_PULLBACK_BARS
        and confluence is not None
        and confluence <= CONFLUENCE_DISTANCE_ATR
        and recovery
        and intact
    )
    invalid = not intact or depth > INVALID_PULLBACK_DEPTH
    developing = (
        impulse_atr is not None
        and impulse_atr >= MINIMUM_IMPULSE_ATR
        and 0 <= depth <= INVALID_PULLBACK_DEPTH
        and duration <= MAXIMUM_PULLBACK_BARS
    )
    status: Literal["invalid", "ready", "developing", "none"] = (
        "invalid"
        if invalid
        else "ready"
        if ready
        else "developing"
        if developing
        else "none"
    )
    return ChartStructureSnapshot(
        pullback_direction=direction,
        pullback_status=status,
        impulse_start=start.price,
        impulse_end=end.price,
        impulse_size_atr=impulse_atr,
        pullback_depth=depth,
        pullback_duration_bars=duration,
        confluence_distance_atr=confluence,
        pullback_range_ratio=_median_ratio(pullback_ranges, impulse_ranges),
        pullback_quote_volume_ratio=_median_ratio(pullback_quote, impulse_quote),
        recovery_confirmed=recovery,
        structure_intact=intact,
    )


def chart_structure_snapshot(
    candles: Sequence[Candle],
    *,
    index: int,
    atr: Sequence[float | None],
    ema20: Sequence[float | None],
    pivots: Sequence[ConfirmedPivot] | None = None,
    _closed_prefix_validated: bool = False,
) -> ChartStructureSnapshot:
    """Build a causal structure snapshot using levels frozen at ``t-1``."""

    if index <= 0 or index >= len(candles):
        raise IndexError("chart structure index must have a prior in-range candle")
    if len(atr) != len(candles) or len(ema20) != len(candles):
        raise ValueError("ATR and EMA20 series must align with candles")
    if not _closed_prefix_validated and any(
        not candle.is_closed for candle in candles[: index + 1]
    ):
        raise ValueError("chart structure requires closed candles through index")
    reference_index = index - 1
    reference_atr = atr[reference_index]
    reference_ema20 = ema20[reference_index]
    if reference_atr is None or reference_atr <= 0 or reference_ema20 is None:
        return ChartStructureSnapshot()
    if not math.isfinite(reference_atr) or not math.isfinite(reference_ema20):
        raise ValueError("chart structure ATR and EMA20 references must be finite")

    all_pivots = (
        confirmed_pivots(candles[: index + 1]) if pivots is None else tuple(pivots)
    )
    known = _qualified_pivots(all_pivots, atr, reference_index=reference_index)
    highs = [pivot for pivot in known if pivot.kind == "high"]
    lows = [pivot for pivot in known if pivot.kind == "low"]
    if len(highs) < 2 or len(lows) < 2:
        return ChartStructureSnapshot(
            qualified_high_count=len(highs),
            qualified_low_count=len(lows),
            pullback_status="unavailable",
        )

    previous_high, latest_high = highs[-2:]
    previous_low, latest_low = lows[-2:]
    equality = STRUCTURE_EQUALITY_ATR * reference_atr
    high_change = latest_high.price - previous_high.price
    low_change = latest_low.price - previous_low.price
    if high_change > equality and low_change > equality:
        state = "bullish"
    elif high_change < -equality and low_change < -equality:
        state = "bearish"
    else:
        state = "mixed"

    support, support_slope = _line(previous_low, latest_low, index)
    resistance, resistance_slope = _line(previous_high, latest_high, index)
    support = support if support > 0 else None
    resistance = resistance if resistance > 0 else None
    price = float(candles[index].close)
    pullback = _pullback_snapshot(
        candles,
        index=index,
        state=state,
        highs=highs,
        lows=lows,
        reference_atr=reference_atr,
        atr=atr,
        ema20=ema20,
    )
    return ChartStructureSnapshot(
        state=state,
        qualified_high_count=len(highs),
        qualified_low_count=len(lows),
        previous_swing_high=previous_high.price,
        latest_swing_high=latest_high.price,
        previous_swing_low=previous_low.price,
        latest_swing_low=latest_low.price,
        latest_high_bars_ago=index - latest_high.index,
        latest_low_bars_ago=index - latest_low.index,
        swing_high_change_atr=high_change / reference_atr,
        swing_low_change_atr=low_change / reference_atr,
        projected_support=support,
        projected_resistance=resistance,
        support_slope_atr_per_bar=support_slope / reference_atr,
        resistance_slope_atr_per_bar=resistance_slope / reference_atr,
        price_minus_support_atr=(None if support is None else (price - support) / reference_atr),
        resistance_minus_price_atr=(
            None if resistance is None else (resistance - price) / reference_atr
        ),
        support_broken=support is not None and price < support,
        resistance_broken=resistance is not None and price > resistance,
        pullback_direction=pullback.pullback_direction,
        pullback_status=pullback.pullback_status,
        impulse_start=pullback.impulse_start,
        impulse_end=pullback.impulse_end,
        impulse_size_atr=pullback.impulse_size_atr,
        pullback_depth=pullback.pullback_depth,
        pullback_duration_bars=pullback.pullback_duration_bars,
        confluence_distance_atr=pullback.confluence_distance_atr,
        pullback_range_ratio=pullback.pullback_range_ratio,
        pullback_quote_volume_ratio=pullback.pullback_quote_volume_ratio,
        recovery_confirmed=pullback.recovery_confirmed,
        structure_intact=pullback.structure_intact,
    )
