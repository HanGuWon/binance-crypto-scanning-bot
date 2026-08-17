from __future__ import annotations

import math
import statistics
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from decimal import Decimal
from itertools import pairwise

from signalbot.data.candles import interval_to_milliseconds
from signalbot.domain.models import Candle

_TAKER_SHORT_WINDOW = 3
_TAKER_LONG_WINDOW = 12
_VPCI_SHORT_WINDOW = 5
_VPCI_LONG_WINDOW = 20
_VPCI_ATR_WINDOW = 20
_VPCI_SIGNAL_WINDOW = 5
_VPCI_SLOPE_LAG = 3


@dataclass(frozen=True, slots=True)
class TakerDeltaSnapshot:
    """Closed-kline normalized taker delta over the frozen 3/12-bar horizons."""

    d3: float
    d12: float


@dataclass(frozen=True, slots=True)
class NormalizedVpciSnapshot:
    """ATR-normalized quote-volume VPCI state for one fully closed candle."""

    value: float
    signal: float
    slope_3: float


def normalized_taker_delta_series(
    candles: Sequence[Candle],
) -> list[TakerDeltaSnapshot | None]:
    """Return causal D3/D12 snapshots, resetting after invalid candles or gaps."""

    output: list[TakerDeltaSnapshot | None] = [None] * len(candles)
    for start, end in _valid_segments(candles, _valid_flow_candle):
        first = start + _TAKER_LONG_WINDOW - 1
        for index in range(first, end):
            long_candles = candles[index - _TAKER_LONG_WINDOW + 1 : index + 1]
            short_candles = candles[index - _TAKER_SHORT_WINDOW + 1 : index + 1]
            d12 = _normalized_taker_delta(long_candles)
            d3 = _normalized_taker_delta(short_candles)
            if d3 is not None and d12 is not None:
                output[index] = TakerDeltaSnapshot(d3=d3, d12=d12)
    return output


def compute_normalized_taker_delta(
    candles: Sequence[Candle],
    *,
    index: int | None = None,
) -> TakerDeltaSnapshot | None:
    """Return the snapshot at ``index`` without consulting any later candle."""

    resolved = _resolve_index(candles, index)
    if resolved is None:
        return None
    return normalized_taker_delta_series(candles[: resolved + 1])[-1]


def normalized_vpci_series(
    candles: Sequence[Candle],
) -> list[NormalizedVpciSnapshot | None]:
    """Return causal S5/L20 VPCI/ATR20 states with a quote-VWMA5 signal."""

    output: list[NormalizedVpciSnapshot | None] = [None] * len(candles)
    for start, end in _valid_segments(candles, _valid_price_volume_candle):
        segment = candles[start:end]
        segment_values = _normalized_vpci_segment(
            segment,
            short_window=_VPCI_SHORT_WINDOW,
            long_window=_VPCI_LONG_WINDOW,
            atr_window=_VPCI_ATR_WINDOW,
            signal_window=_VPCI_SIGNAL_WINDOW,
            slope_lag=_VPCI_SLOPE_LAG,
        )
        output[start:end] = segment_values
    return output


def compute_normalized_vpci(
    candles: Sequence[Candle],
    *,
    index: int | None = None,
) -> NormalizedVpciSnapshot | None:
    """Return normalized VPCI at ``index`` without consulting future rows."""

    resolved = _resolve_index(candles, index)
    if resolved is None:
        return None
    return normalized_vpci_series(candles[: resolved + 1])[-1]


def taker_delta_unavailable_reason(
    candles: Sequence[Candle], *, index: int | None = None
) -> str | None:
    """Explain why the frozen D3/D12 state is unavailable at ``index``."""

    resolved = _resolve_index(candles, index)
    if resolved is None or resolved + 1 < _TAKER_LONG_WINDOW:
        return "immature_window"
    window = candles[resolved - _TAKER_LONG_WINDOW + 1 : resolved + 1]
    common_reason = _window_integrity_reason(window, _valid_flow_candle)
    if common_reason is not None:
        return common_reason
    long_total = sum((item.quote_volume for item in window), Decimal())
    short_total = sum(
        (item.quote_volume for item in window[-_TAKER_SHORT_WINDOW:]), Decimal()
    )
    if long_total <= 0 or short_total <= 0:
        return "zero_quote_volume_denominator"
    return None


def normalized_vpci_unavailable_reason(
    candles: Sequence[Candle], *, index: int | None = None
) -> str | None:
    """Explain why the frozen normalized-VPCI state is unavailable at ``index``."""

    resolved = _resolve_index(candles, index)
    required = _VPCI_LONG_WINDOW + max(
        _VPCI_SIGNAL_WINDOW - 1, _VPCI_SLOPE_LAG
    )
    if resolved is None or resolved + 1 < required:
        return "immature_window"
    window = candles[resolved - required + 1 : resolved + 1]
    common_reason = _window_integrity_reason(window, _valid_price_volume_candle)
    if common_reason is not None:
        return common_reason
    for offset in range(_VPCI_SIGNAL_WINDOW):
        target = len(window) - 1 - offset
        long_values = window[target - _VPCI_LONG_WINDOW + 1 : target + 1]
        short_values = window[target - _VPCI_SHORT_WINDOW + 1 : target + 1]
        if (
            sum((item.quote_volume for item in long_values), Decimal()) <= 0
            or sum((item.quote_volume for item in short_values), Decimal()) <= 0
        ):
            return "zero_quote_volume_denominator"
    atr = _rolling_atr_series(candles[: resolved + 1], _VPCI_ATR_WINDOW)
    for offset in range(_VPCI_SIGNAL_WINDOW):
        value = atr[resolved - offset]
        if value is None or value <= 0:
            return "zero_atr"
    return None


def _resolve_index(candles: Sequence[Candle], index: int | None) -> int | None:
    if not candles:
        return None
    resolved = len(candles) - 1 if index is None else index
    return resolved if 0 <= resolved < len(candles) else None


def _window_integrity_reason(
    candles: Sequence[Candle], validator: Callable[[Candle], bool]
) -> str | None:
    if any(not candle.is_closed for candle in candles):
        return "unclosed_candle"
    if any(not _valid_common_candle(candle) for candle in candles):
        return "invalid_candle_boundary"
    if any(not validator(candle) for candle in candles):
        return "invalid_feature_input"
    if any(not _follows(previous, current) for previous, current in pairwise(candles)):
        return "noncontiguous_window"
    return None


def _valid_segments(
    candles: Sequence[Candle], validator: Callable[[Candle], bool]
) -> list[tuple[int, int]]:
    segments: list[tuple[int, int]] = []
    start: int | None = None
    previous: Candle | None = None
    for index, candle in enumerate(candles):
        valid = validator(candle)
        follows = valid and previous is not None and _follows(previous, candle)
        if not valid:
            if start is not None:
                segments.append((start, index))
            start = None
            previous = None
            continue
        if start is None:
            start = index
        elif not follows:
            segments.append((start, index))
            start = index
        previous = candle
    if start is not None:
        segments.append((start, len(candles)))
    return segments


def _follows(previous: Candle, current: Candle) -> bool:
    if (
        previous.market is not current.market
        or previous.symbol != current.symbol
        or previous.interval != current.interval
    ):
        return False
    try:
        step_ms = interval_to_milliseconds(current.interval)
    except ValueError:
        return False
    return current.open_time_ms - previous.open_time_ms == step_ms


def _valid_common_candle(candle: Candle) -> bool:
    if not candle.is_closed or candle.open_time_ms < 0:
        return False
    try:
        step_ms = interval_to_milliseconds(candle.interval)
    except ValueError:
        return False
    return candle.close_time_ms == candle.open_time_ms + step_ms - 1


def _finite_nonnegative(value: Decimal) -> bool:
    return value.is_finite() and value >= 0


def _valid_flow_candle(candle: Candle) -> bool:
    if not _valid_common_candle(candle):
        return False
    quote = candle.quote_volume
    taker_buy = candle.taker_buy_quote_volume
    return (
        _finite_nonnegative(quote)
        and _finite_nonnegative(taker_buy)
        and taker_buy <= quote
    )


def _valid_price_volume_candle(candle: Candle) -> bool:
    if not _valid_common_candle(candle) or not _finite_nonnegative(candle.quote_volume):
        return False
    prices = (candle.open, candle.high, candle.low, candle.close)
    if any(not value.is_finite() or value <= 0 for value in prices):
        return False
    return (
        candle.low <= min(candle.open, candle.close)
        and candle.high >= max(candle.open, candle.close)
        and candle.low <= candle.high
    )


def _normalized_taker_delta(candles: Sequence[Candle]) -> float | None:
    denominator = sum((candle.quote_volume for candle in candles), Decimal())
    if denominator <= 0:
        return None
    numerator = sum(
        (
            Decimal(2) * candle.taker_buy_quote_volume - candle.quote_volume
            for candle in candles
        ),
        Decimal(),
    )
    value = float(numerator / denominator)
    return value if math.isfinite(value) else None


def _normalized_vpci_segment(
    candles: Sequence[Candle],
    *,
    short_window: int,
    long_window: int,
    atr_window: int,
    signal_window: int,
    slope_lag: int,
) -> list[NormalizedVpciSnapshot | None]:
    output: list[NormalizedVpciSnapshot | None] = [None] * len(candles)
    if len(candles) < long_window:
        return output

    closes = [float(candle.close) for candle in candles]
    quote_volumes = [float(candle.quote_volume) for candle in candles]
    atr = _rolling_atr_series(candles, atr_window)
    normalized: list[float | None] = [None] * len(candles)
    for index in range(long_window - 1, len(candles)):
        normalized[index] = _normalized_vpci_at(
            closes,
            quote_volumes,
            atr,
            index,
            short_window=short_window,
            long_window=long_window,
        )

    minimum_index = max(
        long_window + signal_window - 2,
        long_window + slope_lag - 1,
    )
    for index in range(minimum_index, len(candles)):
        signal_start = index - signal_window + 1
        signal_values = normalized[signal_start : index + 1]
        valid_signal_values = [value for value in signal_values if value is not None]
        if len(valid_signal_values) != signal_window:
            continue
        signal_volume = quote_volumes[signal_start : index + 1]
        signal_denominator = sum(signal_volume)
        if signal_denominator <= 0:
            continue
        signal = sum(
            value * volume
            for value, volume in zip(valid_signal_values, signal_volume, strict=True)
        ) / signal_denominator
        value = normalized[index]
        previous = normalized[index - slope_lag]
        if value is None or previous is None:
            continue
        slope = value - previous
        if all(math.isfinite(item) for item in (value, signal, slope)):
            output[index] = NormalizedVpciSnapshot(
                value=value,
                signal=signal,
                slope_3=slope,
            )
    return output


def _normalized_vpci_at(
    closes: Sequence[float],
    quote_volumes: Sequence[float],
    atr: Sequence[float | None],
    index: int,
    *,
    short_window: int,
    long_window: int,
) -> float | None:
    long_start = index - long_window + 1
    short_start = index - short_window + 1
    long_closes = closes[long_start : index + 1]
    short_closes = closes[short_start : index + 1]
    long_volume = quote_volumes[long_start : index + 1]
    short_volume = quote_volumes[short_start : index + 1]

    long_volume_total = sum(long_volume)
    short_volume_total = sum(short_volume)
    if long_volume_total <= 0 or short_volume_total <= 0:
        return None

    long_sma = statistics.fmean(long_closes)
    short_sma = statistics.fmean(short_closes)
    long_volume_mean = long_volume_total / long_window
    if long_sma <= 0 or short_sma <= 0 or long_volume_mean <= 0:
        return None

    long_vwma = sum(
        close * volume for close, volume in zip(long_closes, long_volume, strict=True)
    ) / long_volume_total
    short_vwma = sum(
        close * volume for close, volume in zip(short_closes, short_volume, strict=True)
    ) / short_volume_total
    vpc = long_vwma - long_sma
    vpr = short_vwma / short_sma
    volume_multiplier = (short_volume_total / short_window) / long_volume_mean
    atr_value = atr[index]
    if atr_value is None or atr_value <= 0:
        return None
    value = vpc * vpr * volume_multiplier / atr_value
    return value if math.isfinite(value) else None


def _rolling_atr_series(
    candles: Sequence[Candle], period: int
) -> list[float | None]:
    output: list[float | None] = [None] * len(candles)
    if period <= 0 or len(candles) < period:
        return output
    true_ranges = [float(candles[0].high - candles[0].low)]
    for previous, current in pairwise(candles):
        high = float(current.high)
        low = float(current.low)
        previous_close = float(previous.close)
        true_ranges.append(
            max(high - low, abs(high - previous_close), abs(low - previous_close))
        )
    running = sum(true_ranges[:period])
    output[period - 1] = running / period
    for index in range(period, len(candles)):
        running += true_ranges[index] - true_ranges[index - period]
        output[index] = running / period
    return output
