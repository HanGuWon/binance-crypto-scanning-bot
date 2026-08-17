from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from itertools import pairwise
from typing import Literal

from signalbot.domain.enums import Direction, Market
from signalbot.domain.models import Candle, FeatureSnapshot

EFFICIENCY_RATIO_PERIOD = 20
DMI_PERIOD = 14

TakerDeltaSource = Literal[
    "taker_delta_3",
    "current_taker_imbalance",
    "unavailable",
]


@dataclass(frozen=True, slots=True)
class WilderDmiSeries:
    """Point-in-time Wilder DMI values aligned one-to-one with closed candles."""

    plus_di: tuple[float | None, ...]
    minus_di: tuple[float | None, ...]
    adx: tuple[float | None, ...]
    adx_delta: tuple[float | None, ...]


@dataclass(frozen=True, slots=True)
class AlertFilterSeries:
    """Backtest-only experimental indicators; every row is closed-candle causal."""

    market: Market | None
    symbol: str | None
    interval: str | None
    close_time_ms: tuple[int, ...]
    efficiency_ratio_20: tuple[float | None, ...]
    plus_di_14: tuple[float | None, ...]
    minus_di_14: tuple[float | None, ...]
    adx_14: tuple[float | None, ...]
    adx_delta: tuple[float | None, ...]

    def __len__(self) -> int:
        return len(self.close_time_ms)


@dataclass(frozen=True, slots=True)
class AlertFilterSnapshot:
    """Direction-normalized candidate fields for one historical alert.

    These fields are experimental discriminators, not probabilities. A positive
    directional value supports the requested direction; a negative value opposes it.
    """

    direction: Direction
    event_time_ms: int
    efficiency_ratio_20: float | None
    plus_di_14: float | None
    minus_di_14: float | None
    adx_14: float | None
    adx_delta: float | None
    directional_di_balance: float | None
    directional_di_spread: float | None
    directional_macd_delta_atr: float | None
    directional_taker_delta: float | None
    directional_taker_delta_source: TakerDeltaSource
    volume_zscore: float
    pullback_range_contraction: float | None
    pullback_volume_contraction: float | None


def _validate_period(period: int) -> None:
    if period <= 0:
        raise ValueError("indicator period must be positive")


def _validate_closed_candles(candles: Sequence[Candle]) -> None:
    if any(not candle.is_closed for candle in candles):
        raise ValueError("experimental alert indicators require fully closed candles")
    if not candles:
        return
    first = candles[0]
    for previous, current in pairwise(candles):
        if (
            current.market is not first.market
            or current.symbol != first.symbol
            or current.interval != first.interval
        ):
            raise ValueError("candles must belong to one market, symbol, and interval")
        if current.open_time_ms <= previous.open_time_ms:
            raise ValueError("candles must be strictly ordered without duplicates")


def kaufman_efficiency_ratio_series(
    candles: Sequence[Candle],
    period: int = EFFICIENCY_RATIO_PERIOD,
) -> tuple[float | None, ...]:
    """Return Kaufman's path-efficiency ratio using current and past closes only.

    The value is ``absolute net change / sum of absolute one-bar changes``. It is
    zero for a flat zero-volatility window and unavailable until ``period + 1``
    closed candles exist.
    """

    _validate_period(period)
    _validate_closed_candles(candles)
    result: list[float | None] = [None] * len(candles)
    if len(candles) <= period:
        return tuple(result)

    closes = [float(candle.close) for candle in candles]
    one_bar_changes = [
        abs(current - previous) for previous, current in pairwise(closes)
    ]
    path_length = sum(one_bar_changes[:period])
    for index in range(period, len(closes)):
        if index > period:
            path_length += one_bar_changes[index - 1]
            path_length -= one_bar_changes[index - period - 1]
        net_change = abs(closes[index] - closes[index - period])
        result[index] = 0.0 if path_length <= 0 else net_change / path_length
    return tuple(result)


def _wilder_average_series(
    values: Sequence[float], period: int
) -> list[float | None]:
    result: list[float | None] = [None] * len(values)
    if len(values) < period:
        return result
    previous = sum(values[:period]) / period
    result[period - 1] = previous
    for index in range(period, len(values)):
        previous = (previous * (period - 1) + values[index]) / period
        result[index] = previous
    return result


def wilder_dmi_series(
    candles: Sequence[Candle],
    period: int = DMI_PERIOD,
) -> WilderDmiSeries:
    """Return Wilder +DI, -DI, ADX, and one-bar ADX change causally."""

    _validate_period(period)
    _validate_closed_candles(candles)
    size = len(candles)
    plus_di: list[float | None] = [None] * size
    minus_di: list[float | None] = [None] * size
    adx: list[float | None] = [None] * size
    adx_delta: list[float | None] = [None] * size
    if not candles:
        return WilderDmiSeries(tuple(plus_di), tuple(minus_di), tuple(adx), tuple(adx_delta))

    true_ranges = [float(candles[0].high - candles[0].low)]
    plus_dm = [0.0]
    minus_dm = [0.0]
    for previous, current in pairwise(candles):
        high = float(current.high)
        low = float(current.low)
        previous_close = float(previous.close)
        true_ranges.append(
            max(high - low, abs(high - previous_close), abs(low - previous_close))
        )
        upward_move = float(current.high - previous.high)
        downward_move = float(previous.low - current.low)
        plus_dm.append(upward_move if upward_move > downward_move and upward_move > 0 else 0.0)
        minus_dm.append(
            downward_move if downward_move > upward_move and downward_move > 0 else 0.0
        )

    smoothed_tr = _wilder_average_series(true_ranges, period)
    smoothed_plus = _wilder_average_series(plus_dm, period)
    smoothed_minus = _wilder_average_series(minus_dm, period)
    dx: list[float | None] = [None] * size
    for index in range(period - 1, size):
        tr_value = smoothed_tr[index]
        plus_value = smoothed_plus[index]
        minus_value = smoothed_minus[index]
        if (
            tr_value is None
            or plus_value is None
            or minus_value is None
            or tr_value <= 0
        ):
            continue
        plus_di_value = 100 * plus_value / tr_value
        minus_di_value = 100 * minus_value / tr_value
        plus_di[index] = plus_di_value
        minus_di[index] = minus_di_value
        denominator = plus_di_value + minus_di_value
        dx[index] = (
            0.0
            if denominator == 0
            else 100 * abs(plus_di_value - minus_di_value) / denominator
        )

    valid_dx = [(index, value) for index, value in enumerate(dx) if value is not None]
    if len(valid_dx) >= period:
        first_adx_index = valid_dx[period - 1][0]
        initial_dx = [value for _, value in valid_dx[:period]]
        previous_adx = sum(initial_dx) / period
        adx[first_adx_index] = previous_adx
        for index in range(first_adx_index + 1, size):
            dx_value = dx[index]
            if dx_value is None:
                continue
            previous_adx = (previous_adx * (period - 1) + dx_value) / period
            adx[index] = previous_adx

    for index in range(1, size):
        current_adx = adx[index]
        previous_adx = adx[index - 1]
        if current_adx is not None and previous_adx is not None:
            adx_delta[index] = current_adx - previous_adx

    return WilderDmiSeries(
        plus_di=tuple(plus_di),
        minus_di=tuple(minus_di),
        adx=tuple(adx),
        adx_delta=tuple(adx_delta),
    )


def compute_alert_filter_series(candles: Sequence[Candle]) -> AlertFilterSeries:
    """Precompute the fixed ER20/DMI14 experimental series for one candle stream."""

    _validate_closed_candles(candles)
    efficiency_ratio = kaufman_efficiency_ratio_series(candles)
    dmi = wilder_dmi_series(candles)
    first = candles[0] if candles else None
    return AlertFilterSeries(
        market=None if first is None else first.market,
        symbol=None if first is None else first.symbol,
        interval=None if first is None else first.interval,
        close_time_ms=tuple(candle.close_time_ms for candle in candles),
        efficiency_ratio_20=efficiency_ratio,
        plus_di_14=dmi.plus_di,
        minus_di_14=dmi.minus_di,
        adx_14=dmi.adx,
        adx_delta=dmi.adx_delta,
    )


def _direction_multiplier(direction: Direction) -> float:
    if direction is Direction.LONG:
        return 1.0
    if direction is Direction.SHORT:
        return -1.0
    raise ValueError("experimental alert filter supports only long or short directions")


def _contraction(ratio: float | None) -> float | None:
    return None if ratio is None else 1.0 - ratio


def alert_filter_snapshot_at(
    series: AlertFilterSeries,
    feature: FeatureSnapshot,
    direction: Direction,
    index: int,
) -> AlertFilterSnapshot:
    """Combine one causal ER/DMI row with its existing feature snapshot."""

    if index < 0 or index >= len(series):
        raise IndexError("alert filter snapshot index is out of range")
    if series.close_time_ms[index] != feature.event_time_ms:
        raise ValueError("feature event time does not align with the indicator row")
    if (
        feature.market is not series.market
        or feature.symbol != series.symbol
        or feature.interval != series.interval
    ):
        raise ValueError("feature market, symbol, or interval does not align with indicators")

    multiplier = _direction_multiplier(direction)
    plus_di = series.plus_di_14[index]
    minus_di = series.minus_di_14[index]
    directional_di_spread: float | None = None
    directional_di_balance: float | None = None
    if plus_di is not None and minus_di is not None:
        directional_di_spread = multiplier * (plus_di - minus_di)
        denominator = plus_di + minus_di
        directional_di_balance = (
            0.0 if denominator == 0 else directional_di_spread / denominator
        )

    directional_macd_delta_atr = (
        multiplier
        * (feature.macd_histogram - feature.macd_histogram_previous)
        / feature.atr
        if feature.atr > 0
        else None
    )
    if feature.taker_delta_3 is not None:
        directional_taker_delta = multiplier * feature.taker_delta_3
        taker_delta_source: TakerDeltaSource = "taker_delta_3"
    elif feature.closed_kline_flow_available:
        directional_taker_delta = multiplier * feature.taker_imbalance
        taker_delta_source = "current_taker_imbalance"
    else:
        directional_taker_delta = None
        taker_delta_source = "unavailable"

    structure = feature.chart_structure
    return AlertFilterSnapshot(
        direction=direction,
        event_time_ms=feature.event_time_ms,
        efficiency_ratio_20=series.efficiency_ratio_20[index],
        plus_di_14=plus_di,
        minus_di_14=minus_di,
        adx_14=series.adx_14[index],
        adx_delta=series.adx_delta[index],
        directional_di_balance=directional_di_balance,
        directional_di_spread=directional_di_spread,
        directional_macd_delta_atr=directional_macd_delta_atr,
        directional_taker_delta=directional_taker_delta,
        directional_taker_delta_source=taker_delta_source,
        volume_zscore=feature.volume_zscore,
        pullback_range_contraction=_contraction(structure.pullback_range_ratio),
        pullback_volume_contraction=_contraction(
            structure.pullback_quote_volume_ratio
        ),
    )
