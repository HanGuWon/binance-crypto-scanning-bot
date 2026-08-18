from __future__ import annotations

import statistics
from collections.abc import Sequence
from dataclasses import dataclass
from itertools import pairwise

from signalbot.config import SignalSettings
from signalbot.data.microstructure import OrderFlowSnapshot, closed_kline_flow
from signalbot.domain.models import Candle, FeatureSnapshot, MarketRegime
from signalbot.indicators.structure import (
    ConfirmedPivot,
    chart_structure_snapshot,
    confirmed_pivots,
)
from signalbot.indicators.volume import (
    NormalizedVpciSnapshot,
    TakerDeltaSnapshot,
    normalized_taker_delta_series,
    normalized_vpci_series,
    normalized_vpci_unavailable_reason,
    taker_delta_unavailable_reason,
)


def ema_series(values: Sequence[float], period: int) -> list[float | None]:
    result: list[float | None]
    result = [None] * len(values)
    if period <= 0 or len(values) < period:
        return result
    seed = sum(values[:period]) / period
    result[period - 1] = seed
    multiplier = 2 / (period + 1)
    previous = seed
    for index in range(period, len(values)):
        previous = (values[index] - previous) * multiplier + previous
        result[index] = previous
    return result


def _rsi(avg_gain: float, avg_loss: float) -> float:
    if avg_loss == 0:
        return 100.0 if avg_gain > 0 else 50.0
    rs = avg_gain / avg_loss
    return 100 - 100 / (1 + rs)


def rsi_series(values: Sequence[float], period: int = 14) -> list[float | None]:
    result: list[float | None]
    result = [None] * len(values)
    if period <= 0 or len(values) <= period:
        return result
    gains = []
    losses = []
    for i in range(1, period + 1):
        delta = values[i] - values[i - 1]
        gains.append(max(delta, 0))
        losses.append(max(-delta, 0))
    avg_gain = sum(gains) / period
    avg_loss = sum(losses) / period
    result[period] = _rsi(avg_gain, avg_loss)
    for i in range(period + 1, len(values)):
        delta = values[i] - values[i - 1]
        avg_gain = (avg_gain * (period - 1) + max(delta, 0)) / period
        avg_loss = (avg_loss * (period - 1) + max(-delta, 0)) / period
        result[i] = _rsi(avg_gain, avg_loss)
    return result


def true_ranges(candles: Sequence[Candle]) -> list[float]:
    if not candles:
        return []
    out = [float(candles[0].high - candles[0].low)]
    for previous, current in pairwise(candles):
        high = float(current.high)
        low = float(current.low)
        pc = float(previous.close)
        out.append(max(high - low, abs(high - pc), abs(low - pc)))
    return out


def wilder_series(values: Sequence[float], period: int) -> list[float | None]:
    result: list[float | None]
    result = [None] * len(values)
    if period <= 0 or len(values) < period:
        return result
    previous = sum(values[:period]) / period
    result[period - 1] = previous
    for i in range(period, len(values)):
        previous = (previous * (period - 1) + values[i]) / period
        result[i] = previous
    return result


def atr_series(candles: Sequence[Candle], period: int = 14) -> list[float | None]:
    return wilder_series(true_ranges(candles), period)


def adx_series(candles: Sequence[Candle], period: int = 14) -> list[float | None]:
    result: list[float | None]
    result = [None] * len(candles)
    if period <= 0 or len(candles) < period * 2 - 1:
        return result
    tr = true_ranges(candles)
    plus = [0.0]
    minus = [0.0]
    for previous, current in pairwise(candles):
        up = float(current.high - previous.high)
        down = float(previous.low - current.low)
        plus.append(up if up > down and up > 0 else 0)
        minus.append(down if down > up and down > 0 else 0)
    trs = wilder_series(tr, period)
    ps = wilder_series(plus, period)
    ms = wilder_series(minus, period)
    dx: list[float | None] = [None] * len(candles)
    for i in range(period - 1, len(candles)):
        tr_value = trs[i]
        plus_value = ps[i]
        minus_value = ms[i]
        if tr_value is None or plus_value is None or minus_value is None or tr_value <= 0:
            continue
        pdi = 100 * plus_value / tr_value
        mdi = 100 * minus_value / tr_value
        denominator = pdi + mdi
        dx[i] = 0 if denominator == 0 else 100 * abs(pdi - mdi) / denominator
    valid_indices = [i for i, value in enumerate(dx) if value is not None]
    valid_values = [value for value in dx if value is not None]
    if len(valid_indices) < period:
        return result
    first = valid_indices[period - 1]
    adx = sum(valid_values[:period]) / period
    result[first] = adx
    for i in range(first + 1, len(candles)):
        value = dx[i]
        if value is not None:
            adx = (adx * (period - 1) + value) / period
            result[i] = adx
    return result


def bollinger_width_series(
    values: Sequence[float], period: int = 20, deviations: float = 2
) -> list[float | None]:
    result: list[float | None]
    result = [None] * len(values)
    for i in range(period - 1, len(values)):
        window = values[i - period + 1 : i + 1]
        mean = statistics.fmean(window)
        if mean != 0:
            result[i] = (2 * deviations * statistics.pstdev(window)) / abs(mean)
    return result


def macd_histogram_series(
    values: Sequence[float], fast: int = 12, slow: int = 26, signal_period: int = 9
) -> list[float | None]:
    fasts = ema_series(values, fast)
    slows = ema_series(values, slow)
    macd = [
        a - b if a is not None and b is not None else None
        for a, b in zip(fasts, slows, strict=True)
    ]
    result: list[float | None] = [None] * len(values)
    start = next((i for i, v in enumerate(macd) if v is not None), None)
    if start is None:
        return result
    compact = [float(v) for v in macd[start:] if v is not None]
    signals = ema_series(compact, signal_period)
    for offset, signal_value in enumerate(signals):
        macd_value = macd[start + offset]
        if signal_value is not None and macd_value is not None:
            result[start + offset] = macd_value - signal_value
    return result


def _latest(series: Sequence[float | None], offset: int = 0, default: float = 0) -> float:
    index = len(series) - 1 - offset
    if index < 0:
        return default
    value = series[index]
    return default if value is None else value


def _required_at(series: Sequence[float | None], index: int) -> float:
    value = series[index]
    if value is None:  # pragma: no cover - guarded by FeatureEngine._compute_at
        raise ValueError(f"required indicator is unavailable at index {index}")
    return float(value)


def _optional_at(series: Sequence[float | None], index: int) -> float | None:
    value = series[index]
    return None if value is None else float(value)


def _point_in_time_zscore(value: float, history: Sequence[float]) -> float:
    """Score a closed-bar value against strictly earlier observations."""
    if len(history) < 2:
        return 0.0
    deviation = statistics.pstdev(history)
    if deviation <= 0:
        return 0.0
    return (value - statistics.fmean(history)) / deviation


def _divergences(
    candles: Sequence[Candle], rsi: Sequence[float | None], lookback: int = 30
) -> tuple[bool, bool]:
    if len(candles) < lookback:
        return False, False
    start = len(candles) - lookback
    midpoint = start + lookback // 2
    first = range(start, midpoint)
    second = range(midpoint, len(candles))
    h1 = max(first, key=lambda i: float(candles[i].high))
    h2 = max(second, key=lambda i: float(candles[i].high))
    l1 = min(first, key=lambda i: float(candles[i].low))
    l2 = min(second, key=lambda i: float(candles[i].low))
    rsi_h1 = rsi[h1]
    rsi_h2 = rsi[h2]
    rsi_l1 = rsi[l1]
    rsi_l2 = rsi[l2]
    bearish = (
        rsi_h1 is not None
        and rsi_h2 is not None
        and float(candles[h2].high) > float(candles[h1].high) * 1.001
        and rsi_h2 < rsi_h1 - 2
    )
    bullish = (
        rsi_l1 is not None
        and rsi_l2 is not None
        and float(candles[l2].low) < float(candles[l1].low) * 0.999
        and rsi_l2 > rsi_l1 + 2
    )
    return bool(bearish), bool(bullish)


@dataclass(frozen=True, slots=True)
class _FeatureArrays:
    closes: list[float]
    volumes: list[float]
    quote_volumes: list[float]
    trade_counts: list[float]
    signed_taker_quote: list[float]
    ema9: list[float | None]
    ema20: list[float | None]
    ema50: list[float | None]
    ema200: list[float | None]
    rsi: list[float | None]
    macd: list[float | None]
    atr: list[float | None]
    adx: list[float | None]
    widths: list[float | None]
    taker_deltas: list[TakerDeltaSnapshot | None]
    normalized_vpci: list[NormalizedVpciSnapshot | None]
    pivots: tuple[ConfirmedPivot, ...]


class FeatureEngine:
    def __init__(self, settings: SignalSettings) -> None:
        self.settings = settings

    @property
    def minimum_history(self) -> int:
        return max(210, self.settings.breakout_lookback + 5)

    def compute(
        self,
        candles: Sequence[Candle],
        order_flow: OrderFlowSnapshot,
        spread_bps: float | None,
        regime: MarketRegime,
        spread_is_proxy: bool = False,
    ) -> FeatureSnapshot | None:
        if len(candles) < self.minimum_history:
            return None
        if any(not candle.is_closed for candle in candles):
            raise ValueError("feature computation requires a fully closed candle prefix")
        arrays = self._arrays(candles)
        return self._compute_at(
            candles,
            len(candles) - 1,
            arrays,
            order_flow,
            spread_bps,
            regime,
            spread_is_proxy,
        )

    def compute_series(
        self,
        candles: Sequence[Candle],
        order_flows: Sequence[OrderFlowSnapshot] | None = None,
        spread_bps: float | None = None,
        regimes: Sequence[MarketRegime] | None = None,
        spread_is_proxy: bool = False,
    ) -> list[FeatureSnapshot | None]:
        """Compute a historical feature series in one pass over each indicator.

        The scalar ``compute`` method remains the live-runtime owner. This method
        uses the same snapshot assembler but avoids recomputing every historical
        indicator for every backtest bar.
        """
        return self.compute_at_indices(
            candles,
            range(len(candles)),
            order_flows,
            spread_bps,
            regimes,
            spread_is_proxy,
        )

    def compute_at_indices(
        self,
        candles: Sequence[Candle],
        indices: Sequence[int],
        order_flows: Sequence[OrderFlowSnapshot] | None = None,
        spread_bps: float | None = None,
        regimes: Sequence[MarketRegime] | None = None,
        spread_is_proxy: bool = False,
    ) -> list[FeatureSnapshot | None]:
        """Compute causal snapshots only at selected candle indices.

        ``indices`` must be in range and strictly increasing, so returned values
        align one-to-one with the requested indices. Indicator arrays are
        precomputed once; each snapshot assembler reads only through its index.
        """
        if order_flows is not None and len(order_flows) != len(candles):
            raise ValueError("order_flows must align one-to-one with candles")
        if regimes is not None and len(regimes) != len(candles):
            raise ValueError("regimes must align one-to-one with candles")
        if any(index < 0 or index >= len(candles) for index in indices):
            raise ValueError("indices must be in range for candles")
        if any(current <= previous for previous, current in pairwise(indices)):
            raise ValueError("indices must be strictly increasing and unique")
        if not indices:
            return []
        last_index = indices[-1]
        visible_candles = candles[: last_index + 1]
        if any(not candle.is_closed for candle in visible_candles):
            raise ValueError("feature computation requires a fully closed candle prefix")
        arrays = self._arrays(visible_candles)
        neutral_flow = OrderFlowSnapshot()
        neutral_regime = MarketRegime()
        return [
            self._compute_at(
                visible_candles,
                index,
                arrays,
                order_flows[index] if order_flows is not None else neutral_flow,
                spread_bps,
                regimes[index] if regimes is not None else neutral_regime,
                spread_is_proxy,
            )
            if index + 1 >= self.minimum_history
            else None
            for index in indices
        ]

    def _arrays(self, candles: Sequence[Candle]) -> _FeatureArrays:
        closes = [float(c.close) for c in candles]
        quote_volumes = [float(c.quote_volume) for c in candles]
        return _FeatureArrays(
            closes=closes,
            volumes=[float(c.volume) for c in candles],
            quote_volumes=quote_volumes,
            trade_counts=[float(c.trade_count) for c in candles],
            signed_taker_quote=[
                2 * float(c.taker_buy_quote_volume) - quote_volume
                for c, quote_volume in zip(candles, quote_volumes, strict=True)
            ],
            ema9=ema_series(closes, 9),
            ema20=ema_series(closes, 20),
            ema50=ema_series(closes, 50),
            ema200=ema_series(closes, 200),
            rsi=rsi_series(closes),
            macd=macd_histogram_series(closes),
            atr=atr_series(candles),
            adx=adx_series(candles),
            widths=bollinger_width_series(closes),
            taker_deltas=normalized_taker_delta_series(candles),
            normalized_vpci=normalized_vpci_series(candles),
            pivots=confirmed_pivots(candles),
        )

    def _compute_at(
        self,
        candles: Sequence[Candle],
        index: int,
        arrays: _FeatureArrays,
        order_flow: OrderFlowSnapshot,
        spread_bps: float | None,
        regime: MarketRegime,
        spread_is_proxy: bool,
    ) -> FeatureSnapshot | None:
        if index + 1 < self.minimum_history:
            return None
        e9 = arrays.ema9
        e20 = arrays.ema20
        e50 = arrays.ema50
        e200 = arrays.ema200
        rsis = arrays.rsi
        macd = arrays.macd
        atr = arrays.atr
        adx = arrays.adx
        widths = arrays.widths
        required = [
            e9[index],
            e20[index],
            e50[index],
            rsis[index],
            rsis[index - 1],
            macd[index],
            macd[index - 1],
            macd[index - 2],
            atr[index],
            adx[index],
            widths[index],
        ]
        if any(v is None for v in required):
            return None
        current = candles[index]
        price = arrays.closes[index]
        prior = candles[index - self.settings.breakout_lookback : index]
        recent_high = max(float(c.high) for c in prior)
        recent_low = min(float(c.low) for c in prior)
        baseline = statistics.fmean(arrays.volumes[index - 20 : index])
        relative = arrays.volumes[index] / baseline if baseline > 0 else 1
        width_value = widths[index]
        if width_value is None:
            return None
        width = float(width_value)
        valid = [float(v) for v in widths[max(0, index - 119) : index + 1] if v is not None]
        percentile = 100 * sum(v <= width for v in valid) / len(valid) if valid else 50
        candle_range = max(float(current.high - current.low), 1e-12)
        upper = float(current.high) - max(float(current.open), float(current.close))
        lower = min(float(current.open), float(current.close)) - float(current.low)
        divergence_start = max(0, index - 29)
        bearish, bullish = _divergences(
            candles[divergence_start : index + 1],
            rsis[divergence_start : index + 1],
        )
        current_atr_value = atr[index]
        if current_atr_value is None:
            return None
        current_atr = float(current_atr_value)
        slope_lookback = 3
        previous_ema20 = e20[index - slope_lookback]
        ema20_slope_atr = (
            (_required_at(e20, index) - float(previous_ema20)) / current_atr
            if previous_ema20 is not None and current_atr > 0
            else 0.0
        )
        activity_start = max(0, index - 30)
        volume_zscore = _point_in_time_zscore(
            arrays.volumes[index], arrays.volumes[activity_start:index]
        )
        trade_count_zscore = _point_in_time_zscore(
            arrays.trade_counts[index], arrays.trade_counts[activity_start:index]
        )
        flow_start = max(0, index - 19)
        quote_total = sum(arrays.quote_volumes[flow_start : index + 1])
        cvd_pressure = (
            sum(arrays.signed_taker_quote[flow_start : index + 1]) / quote_total
            if quote_total > 0
            else 0.0
        )
        canonical_flow = closed_kline_flow(current)
        taker_imbalance = 2 * canonical_flow.taker_buy_ratio - 1
        intrabar_taker_imbalance = (
            max(-1.0, min(1.0, 2 * order_flow.taker_buy_ratio - 1))
            if order_flow.available
            else None
        )
        taker_delta = arrays.taker_deltas[index]
        normalized_vpci = arrays.normalized_vpci[index]
        chart_structure = chart_structure_snapshot(
            candles,
            index=index,
            atr=atr,
            ema20=e20,
            pivots=arrays.pivots,
            _closed_prefix_validated=True,
        )
        efficiency_window = arrays.closes[index - 20 : index + 1]
        efficiency_distance = abs(efficiency_window[-1] - efficiency_window[0])
        efficiency_path = sum(
            abs(current - previous)
            for previous, current in pairwise(efficiency_window)
        )
        efficiency_ratio_20 = (
            efficiency_distance / efficiency_path
            if efficiency_path > 0
            else None
        )
        taker_delta_reason = (
            None
            if taker_delta is not None
            else taker_delta_unavailable_reason(candles, index=index)
        )
        normalized_vpci_reason = (
            None
            if normalized_vpci is not None
            else normalized_vpci_unavailable_reason(candles, index=index)
        )
        return FeatureSnapshot(
            market=current.market,
            symbol=current.symbol,
            interval=current.interval,
            event_time_ms=current.close_time_ms,
            price=price,
            previous_close=arrays.closes[index - 1],
            ema9=_required_at(e9, index),
            ema20=_required_at(e20, index),
            ema50=_required_at(e50, index),
            ema200=_optional_at(e200, index),
            rsi=_required_at(rsis, index),
            rsi_previous=_required_at(rsis, index - 1),
            macd_histogram=_required_at(macd, index),
            macd_histogram_previous=_required_at(macd, index - 1),
            macd_histogram_previous2=_required_at(macd, index - 2),
            atr=current_atr,
            atr_percent=current_atr / price * 100 if price else 0,
            adx=_required_at(adx, index),
            bollinger_width=width,
            bollinger_width_percentile=percentile,
            relative_volume=relative,
            recent_high=recent_high,
            recent_low=recent_low,
            upper_wick_ratio=max(0, upper / candle_range),
            lower_wick_ratio=max(0, lower / candle_range),
            bearish_divergence=bearish,
            bullish_divergence=bullish,
            taker_buy_ratio=canonical_flow.taker_buy_ratio,
            ema20_slope_atr=ema20_slope_atr,
            volume_zscore=volume_zscore,
            trade_count_zscore=trade_count_zscore,
            taker_imbalance=max(-1.0, min(1.0, taker_imbalance)),
            cvd_pressure=max(-1.0, min(1.0, cvd_pressure)),
            closed_kline_flow_available=canonical_flow.available,
            intrabar_taker_imbalance_60s=intrabar_taker_imbalance,
            taker_delta_3=None if taker_delta is None else taker_delta.d3,
            taker_delta_12=None if taker_delta is None else taker_delta.d12,
            taker_delta_unavailable_reason=taker_delta_reason,
            normalized_vpci=(
                None if normalized_vpci is None else normalized_vpci.value
            ),
            normalized_vpci_signal=(
                None if normalized_vpci is None else normalized_vpci.signal
            ),
            normalized_vpci_slope_3=(
                None if normalized_vpci is None else normalized_vpci.slope_3
            ),
            normalized_vpci_unavailable_reason=normalized_vpci_reason,
            spread_bps=None if spread_bps is None else max(0, spread_bps),
            spread_is_proxy=spread_is_proxy,
            previous_high=float(candles[index - 1].high),
            previous_low=float(candles[index - 1].low),
            previous_ema20=_required_at(e20, index - 1),
            ema20_distance_atr=(
                (price - _required_at(e20, index)) / current_atr
                if current_atr > 0
                else None
            ),
            efficiency_ratio_20=efficiency_ratio_20,
            chart_structure=chart_structure,
            data_completeness=(
                70
                + (20 if canonical_flow.available else 0)
                + (10 if spread_bps is not None else 0)
            )
            / 100,
            regime=regime,
        )
