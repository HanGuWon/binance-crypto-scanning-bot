from decimal import Decimal

import pytest

from conftest import make_candle, make_feature
from signalbot.backtest.context import StrictContextIndex, aggregate_closed_candles


def test_aggregation_emits_only_complete_aligned_buckets() -> None:
    source = [make_candle(index) for index in range(6) if index != 1]

    candles = aggregate_closed_candles(source, "15m")

    assert len(candles) == 1
    aggregated = candles[0]
    assert aggregated.open_time_ms == 900_000
    assert aggregated.close_time_ms == 1_799_999
    assert aggregated.open == source[2].open
    assert aggregated.close == source[-1].close
    assert aggregated.high == max(candle.high for candle in source[2:])
    assert aggregated.low == min(candle.low for candle in source[2:])
    assert aggregated.volume == Decimal("300.0")
    assert aggregated.trade_count == 300


def test_strict_context_excludes_equal_close_and_does_not_reuse_stale_data() -> None:
    first = make_feature(interval="15m", event_time_ms=899_999, price=101.0)
    second = make_feature(interval="15m", event_time_ms=1_799_999, price=102.0)
    forward = StrictContextIndex({"15m": [first, second]})
    reversed_arrival = StrictContextIndex({"15m": [second, first]})

    assert forward.at(899_999) == {}
    assert forward.at(1_199_999) == {"15m": first}
    assert forward.at(1_799_999) == {"15m": first}
    assert forward.at(2_099_999) == {"15m": second}
    assert reversed_arrival.at(2_099_999) == forward.at(2_099_999)

    missing_latest_bucket = StrictContextIndex({"15m": [first]})
    assert missing_latest_bucket.at(2_099_999) == {}


def test_context_index_rejects_conflicting_duplicate_closes() -> None:
    first = make_feature(interval="1h", event_time_ms=3_599_999, price=101.0)
    conflict = first.model_copy(update={"price": 102.0})

    with pytest.raises(ValueError, match="conflicting context"):
        StrictContextIndex({"1h": [first, conflict]})
