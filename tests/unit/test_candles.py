import pytest

from conftest import make_candle
from signalbot.data.candles import CandleConflictError, CandleStore, interval_to_milliseconds
from signalbot.domain.enums import Market


def test_interval_parser() -> None:
    assert interval_to_milliseconds("1m") == 60_000
    assert interval_to_milliseconds("4h") == 14_400_000
    with pytest.raises(ValueError):
        interval_to_milliseconds("0m")
    with pytest.raises(ValueError):
        interval_to_milliseconds("1x")


def test_store_rejects_open_candles_and_deduplicates() -> None:
    store = CandleStore(history_limit=3)
    assert store.add(make_candle(0, is_closed=False)) is False
    first = make_candle(0)
    assert store.add(first) is True
    assert store.add(first) is False
    changed = make_candle(0, close=101)
    with pytest.raises(CandleConflictError):
        store.add(changed)
    assert store.latest(Market.SPOT, "btcusdt", "5m") == first


def test_store_orders_and_bounds_history() -> None:
    store = CandleStore(history_limit=3)
    store.add_many([make_candle(3), make_candle(1), make_candle(2), make_candle(0)])
    assert [c.open_time_ms for c in store.get(Market.SPOT, "BTCUSDT", "5m")] == [
        300_000,
        600_000,
        900_000,
    ]


def test_validate_series_rejects_gaps_and_off_grid_rows() -> None:
    CandleStore.validate_series([make_candle(0), make_candle(0)])
    with pytest.raises(ValueError, match="not contiguous"):
        CandleStore.validate_series([make_candle(0), make_candle(2)])
    with pytest.raises(CandleConflictError):
        CandleStore.validate_series([make_candle(0), make_candle(0, close=101)])
    with pytest.raises(ValueError, match="off the exchange time grid"):
        CandleStore.validate_series(
            [make_candle(0), make_candle(1).model_copy(update={"open_time_ms": 1})]
        )


def test_gap_is_computed_between_latest_and_incoming_candle() -> None:
    store = CandleStore(history_limit=10)
    store.add(make_candle(0))
    gap = store.detect_latest_gap(make_candle(3))
    assert gap is not None
    assert gap.start_time_ms == 300_000
    assert gap.end_time_ms == 899_999
    assert store.detect_latest_gap(make_candle(1)) is None


def test_store_prunes_only_rotated_symbols_in_the_requested_market() -> None:
    store = CandleStore(history_limit=10)
    store.add(make_candle(0, symbol="BTCUSDT"))
    store.add(make_candle(0, symbol="ETHUSDT"))
    store.add(make_candle(0, market=Market.FUTURES, symbol="BTCUSDT"))

    assert store.retain_symbols(Market.SPOT, frozenset({"ETHUSDT"})) == 1
    assert store.size(Market.SPOT, "BTCUSDT", "5m") == 0
    assert store.size(Market.SPOT, "ETHUSDT", "5m") == 1
    assert store.size(Market.FUTURES, "BTCUSDT", "5m") == 1
