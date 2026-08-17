"""Dry-run/backtest parity strategy for candle-based scanner signals.

This is intentionally not imported by the main package. Freqtrade loads it from
its strategy path. It does not model the scanner's second-level anomaly feed.
"""

from __future__ import annotations

from typing import ClassVar

from freqtrade.strategy import IStrategy
from pandas import DataFrame, Series


def _rsi(close: Series, period: int = 14) -> Series:
    delta = close.diff()
    gains = delta.clip(lower=0)
    losses = -delta.clip(upper=0)
    average_gain = gains.ewm(alpha=1 / period, adjust=False, min_periods=period).mean()
    average_loss = losses.ewm(alpha=1 / period, adjust=False, min_periods=period).mean()
    relative_strength = average_gain / average_loss.replace(0, float("nan"))
    return (100 - (100 / (1 + relative_strength))).fillna(100)


def _atr(dataframe: DataFrame, period: int = 14) -> Series:
    previous_close = dataframe["close"].shift(1)
    true_range = DataFrame(
        {
            "high_low": dataframe["high"] - dataframe["low"],
            "high_close": (dataframe["high"] - previous_close).abs(),
            "low_close": (dataframe["low"] - previous_close).abs(),
        }
    ).max(axis=1)
    return true_range.ewm(alpha=1 / period, adjust=False, min_periods=period).mean()


class SignalParityFuturesStrategy(IStrategy):
    INTERFACE_VERSION = 3
    can_short = True
    timeframe = "5m"
    process_only_new_candles = True
    startup_candle_count = 210

    minimal_roi: ClassVar[dict[str, float]] = {"0": 0.08, "120": 0.03, "360": 0}
    stoploss = -0.08
    trailing_stop = False
    use_exit_signal = True
    exit_profit_only = False
    ignore_roi_if_entry_signal = False

    def populate_indicators(self, dataframe: DataFrame, metadata: dict) -> DataFrame:
        del metadata
        dataframe["ema9"] = dataframe["close"].ewm(span=9, adjust=False).mean()
        dataframe["ema20"] = dataframe["close"].ewm(span=20, adjust=False).mean()
        dataframe["ema50"] = dataframe["close"].ewm(span=50, adjust=False).mean()
        dataframe["rsi"] = _rsi(dataframe["close"])

        ema12 = dataframe["close"].ewm(span=12, adjust=False).mean()
        ema26 = dataframe["close"].ewm(span=26, adjust=False).mean()
        dataframe["macd"] = ema12 - ema26
        signal = dataframe["macd"].ewm(span=9, adjust=False).mean()
        dataframe["macd_hist"] = dataframe["macd"] - signal

        dataframe["atr"] = _atr(dataframe)
        dataframe["range_high"] = dataframe["high"].rolling(20).max().shift(1)
        dataframe["range_low"] = dataframe["low"].rolling(20).min().shift(1)
        dataframe["volume_mean"] = dataframe["volume"].rolling(20).mean().shift(1)
        dataframe["rvol"] = dataframe["volume"] / dataframe["volume_mean"]

        candle_range = (dataframe["high"] - dataframe["low"]).clip(lower=1e-12)
        dataframe["upper_wick_ratio"] = (
            dataframe["high"] - dataframe[["open", "close"]].max(axis=1)
        ) / candle_range
        dataframe["lower_wick_ratio"] = (
            dataframe[["open", "close"]].min(axis=1) - dataframe["low"]
        ) / candle_range
        return dataframe

    def populate_entry_trend(self, dataframe: DataFrame, metadata: dict) -> DataFrame:
        del metadata
        long_breakout = (
            (dataframe["close"] > dataframe["range_high"])
            & (dataframe["close"].shift(1) <= dataframe["range_high"].shift(1))
            & (dataframe["rvol"] >= 1.8)
            & (dataframe["macd_hist"] > 0)
            & (dataframe["macd_hist"] > dataframe["macd_hist"].shift(1))
            & (dataframe["ema20"] > dataframe["ema50"])
            & (dataframe["volume"] > 0)
        )
        short_breakdown = (
            (dataframe["close"] < dataframe["range_low"])
            & (dataframe["close"].shift(1) >= dataframe["range_low"].shift(1))
            & (dataframe["rvol"] >= 1.8)
            & (dataframe["macd_hist"] < 0)
            & (dataframe["macd_hist"] < dataframe["macd_hist"].shift(1))
            & (dataframe["ema20"] < dataframe["ema50"])
            & (dataframe["volume"] > 0)
        )
        dataframe.loc[long_breakout, ["enter_long", "enter_tag"]] = (1, "breakout_long")
        dataframe.loc[short_breakdown, ["enter_short", "enter_tag"]] = (
            1,
            "breakdown_short",
        )
        return dataframe

    def populate_exit_trend(self, dataframe: DataFrame, metadata: dict) -> DataFrame:
        del metadata
        exhaustion = (
            (dataframe["rsi"] >= 75)
            & (dataframe["upper_wick_ratio"] >= 0.45)
            & (dataframe["macd_hist"] < dataframe["macd_hist"].shift(1))
            & (dataframe["macd_hist"].shift(1) < dataframe["macd_hist"].shift(2))
            & (dataframe["close"] < dataframe["ema9"])
        )
        capitulation = (
            (dataframe["rsi"] <= 25)
            & (dataframe["lower_wick_ratio"] >= 0.45)
            & (dataframe["macd_hist"] > dataframe["macd_hist"].shift(1))
            & (dataframe["macd_hist"].shift(1) > dataframe["macd_hist"].shift(2))
            & (dataframe["close"] > dataframe["ema9"])
        )
        dataframe.loc[exhaustion, ["exit_long", "exit_tag"]] = (1, "exhaustion_exit")
        dataframe.loc[capitulation, ["exit_short", "exit_tag"]] = (
            1,
            "capitulation_exit",
        )
        return dataframe
