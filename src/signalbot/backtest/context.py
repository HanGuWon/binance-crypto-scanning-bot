from __future__ import annotations

from collections.abc import Mapping, Sequence
from decimal import Decimal

from signalbot.data.candles import interval_to_milliseconds
from signalbot.domain.models import Candle, FeatureSnapshot

HIGHER_TIMEFRAME_INTERVALS = ("15m", "1h")


def aggregate_closed_candles(candles: Sequence[Candle], target_interval: str) -> list[Candle]:
    """Aggregate complete, aligned source buckets without bridging data gaps."""
    if not candles:
        return []
    source_interval = candles[0].interval
    source_ms = interval_to_milliseconds(source_interval)
    target_ms = interval_to_milliseconds(target_interval)
    if target_ms <= source_ms or target_ms % source_ms != 0:
        raise ValueError("target interval must be an exact multiple larger than source interval")

    market = candles[0].market
    symbol = candles[0].symbol
    previous_open_ms = -1
    buckets: dict[int, list[Candle]] = {}
    for candle in candles:
        if candle.market is not market or candle.symbol != symbol:
            raise ValueError("source candles must have one market and symbol")
        if candle.interval != source_interval:
            raise ValueError("source candles must have one interval")
        if not candle.is_closed:
            raise ValueError("higher-timeframe aggregation requires closed candles")
        if candle.open_time_ms <= previous_open_ms:
            raise ValueError("source candles must be strictly ordered without duplicates")
        if candle.close_time_ms != candle.open_time_ms + source_ms - 1:
            raise ValueError("source candle close time does not match its interval")
        previous_open_ms = candle.open_time_ms
        bucket_open_ms = candle.open_time_ms // target_ms * target_ms
        buckets.setdefault(bucket_open_ms, []).append(candle)

    aggregated: list[Candle] = []
    for bucket_open_ms in sorted(buckets):
        bucket = buckets[bucket_open_ms]
        expected_opens = list(range(bucket_open_ms, bucket_open_ms + target_ms, source_ms))
        if [candle.open_time_ms for candle in bucket] != expected_opens:
            continue
        aggregated.append(
            Candle(
                market=market,
                symbol=symbol,
                interval=target_interval,
                open_time_ms=bucket_open_ms,
                close_time_ms=bucket_open_ms + target_ms - 1,
                open=bucket[0].open,
                high=max(candle.high for candle in bucket),
                low=min(candle.low for candle in bucket),
                close=bucket[-1].close,
                volume=sum((candle.volume for candle in bucket), Decimal()),
                quote_volume=sum((candle.quote_volume for candle in bucket), Decimal()),
                trade_count=sum(candle.trade_count for candle in bucket),
                taker_buy_base_volume=sum(
                    (candle.taker_buy_base_volume for candle in bucket), Decimal()
                ),
                taker_buy_quote_volume=sum(
                    (candle.taker_buy_quote_volume for candle in bucket), Decimal()
                ),
                is_closed=True,
            )
        )
    return aggregated


def strictly_available_close_time(decision_close_time_ms: int, interval: str) -> int | None:
    """Return the latest interval close strictly before a decision close."""
    interval_ms = interval_to_milliseconds(interval)
    boundary_ms = decision_close_time_ms // interval_ms * interval_ms
    return boundary_ms - 1 if boundary_ms > 0 else None


class StrictContextIndex:
    """Immutable-by-contract lookup of point-in-time higher-timeframe features."""

    def __init__(
        self,
        features_by_interval: Mapping[str, Sequence[FeatureSnapshot | None]],
    ) -> None:
        indexed: dict[str, dict[int, FeatureSnapshot]] = {}
        for interval, series in features_by_interval.items():
            by_close: dict[int, FeatureSnapshot] = {}
            for feature in series:
                if feature is None:
                    continue
                if feature.interval != interval:
                    raise ValueError("context feature interval does not match its index")
                existing = by_close.get(feature.event_time_ms)
                if existing is not None and existing != feature:
                    raise ValueError("conflicting context features share a close time")
                by_close[feature.event_time_ms] = feature
            indexed[interval] = by_close
        self._indexed = indexed

    def at(self, decision_close_time_ms: int) -> dict[str, FeatureSnapshot]:
        contexts: dict[str, FeatureSnapshot] = {}
        for interval, by_close in self._indexed.items():
            available_close = strictly_available_close_time(decision_close_time_ms, interval)
            if available_close is None:
                continue
            feature = by_close.get(available_close)
            if feature is not None:
                contexts[interval] = feature
        return contexts
