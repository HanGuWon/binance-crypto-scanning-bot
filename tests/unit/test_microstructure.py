from decimal import Decimal

import pytest

from conftest import make_candle
from signalbot.data.microstructure import (
    BookState,
    BookTickerConflictError,
    OrderFlowTracker,
    closed_kline_flow,
)
from signalbot.domain.enums import Market
from signalbot.domain.models import AggTrade, BookTicker


def trade(identifier: int, timestamp: int, buyer_maker: bool, quantity: str = "1") -> AggTrade:
    return AggTrade(
        market=Market.FUTURES,
        symbol="BTCUSDT",
        event_time_ms=timestamp,
        trade_time_ms=timestamp,
        price=Decimal("100"),
        quantity=Decimal(quantity),
        is_buyer_maker=buyer_maker,
        aggregate_trade_id=identifier,
    )


def test_order_flow_deduplicates_and_classifies_taker_side() -> None:
    tracker = OrderFlowTracker(maximum_age_seconds=300, maximum_points=10)
    tracker.update(trade(1, 1_000, False, "2"))
    tracker.update(trade(1, 1_001, True, "100"))
    tracker.update(trade(2, 2_000, True, "1"))
    snapshot = tracker.snapshot(Market.FUTURES, "BTCUSDT", 2_000, 60)
    assert snapshot.trade_count == 2
    assert snapshot.taker_buy_ratio == pytest.approx(2 / 3)


def test_order_flow_snapshot_excludes_trades_newer_than_as_of_time() -> None:
    tracker = OrderFlowTracker(maximum_age_seconds=300, maximum_points=10)
    tracker.update(trade(1, 1_000, False, "2"))
    tracker.update(trade(2, 2_001, True, "100"))

    snapshot = tracker.snapshot(Market.FUTURES, "BTCUSDT", 2_000, 60)

    assert snapshot.trade_count == 1
    assert snapshot.buy_quote_volume == pytest.approx(200.0)
    assert snapshot.sell_quote_volume == 0.0
    assert snapshot.taker_buy_ratio == 1.0


def test_closed_kline_flow_returns_exact_ratio_and_quote_delta() -> None:
    candle = make_candle(1).model_copy(
        update={
            "volume": Decimal("10"),
            "quote_volume": Decimal("100"),
            "taker_buy_base_volume": Decimal("4"),
            "taker_buy_quote_volume": Decimal("40"),
            "trade_count": 12,
        }
    )

    snapshot = closed_kline_flow(candle)

    assert snapshot.available is True
    assert snapshot.taker_buy_ratio == pytest.approx(0.4)
    assert snapshot.buy_quote_volume == 40.0
    assert snapshot.sell_quote_volume == 60.0
    assert snapshot.quote_delta == -20.0
    assert snapshot.trade_count == 12


@pytest.mark.parametrize(
    ("taker_buy", "expected_ratio", "expected_delta"),
    [(Decimal("0"), 0.0, -100.0), (Decimal("100"), 1.0, 100.0)],
)
def test_closed_kline_flow_accepts_taker_buy_boundaries(
    taker_buy: Decimal, expected_ratio: float, expected_delta: float
) -> None:
    candle = make_candle(1).model_copy(
        update={
            "volume": Decimal("10"),
            "quote_volume": Decimal("100"),
            "taker_buy_base_volume": taker_buy / Decimal("10"),
            "taker_buy_quote_volume": taker_buy,
        }
    )

    snapshot = closed_kline_flow(candle)

    assert snapshot.available is True
    assert snapshot.taker_buy_ratio == expected_ratio
    assert snapshot.quote_delta == expected_delta


def test_closed_kline_flow_marks_all_zero_totals_unavailable() -> None:
    candle = make_candle(1).model_copy(
        update={
            "volume": Decimal("0"),
            "quote_volume": Decimal("0"),
            "taker_buy_base_volume": Decimal("0"),
            "taker_buy_quote_volume": Decimal("0"),
            "trade_count": 0,
        }
    )

    snapshot = closed_kline_flow(candle)

    assert snapshot.available is False
    assert snapshot.taker_buy_ratio == 0.5
    assert snapshot.quote_delta == 0.0
    assert snapshot.trade_count == 0


def test_closed_kline_flow_does_not_fallback_to_base_volume() -> None:
    candle = make_candle(1).model_copy(
        update={
            "volume": Decimal("10"),
            "quote_volume": Decimal("0"),
            "taker_buy_base_volume": Decimal("4"),
            "taker_buy_quote_volume": Decimal("0"),
        }
    )

    snapshot = closed_kline_flow(candle)

    assert snapshot.available is False
    assert snapshot.taker_buy_ratio == 0.5
    assert snapshot.quote_delta == 0.0


@pytest.mark.parametrize(
    "updates",
    [
        {
            "quote_volume": Decimal("100"),
            "taker_buy_quote_volume": Decimal("100.00000001"),
        },
        {
            "volume": Decimal("10"),
            "taker_buy_base_volume": Decimal("10.00000001"),
        },
    ],
)
def test_closed_kline_flow_rejects_taker_buy_above_total(
    updates: dict[str, Decimal],
) -> None:
    candle = make_candle(1).model_copy(update=updates)

    with pytest.raises(ValueError, match="exceeds total"):
        closed_kline_flow(candle)


def test_book_state_ignores_stale_update_and_computes_spread() -> None:
    state = BookState()
    recent = BookTicker(
        market=Market.SPOT,
        symbol="BTCUSDT",
        event_time_ms=2,
        bid_price=Decimal("99.9"),
        bid_quantity=Decimal("1"),
        ask_price=Decimal("100.1"),
        ask_quantity=Decimal("1"),
    )
    stale = recent.model_copy(
        update={"event_time_ms": 1, "bid_price": Decimal("90"), "ask_price": Decimal("110")}
    )
    state.update(recent)
    state.update(stale)
    assert state.spread_bps(
        Market.SPOT,
        "btcusdt",
        as_of_ms=502,
        maximum_age_ms=500,
    ) == pytest.approx(20.0)


def test_book_state_distinguishes_unavailable_book_from_zero_spread() -> None:
    state = BookState()
    zero_spread = BookTicker(
        market=Market.SPOT,
        symbol="BTCUSDT",
        event_time_ms=2_000,
        bid_price=Decimal("100"),
        bid_quantity=Decimal("1"),
        ask_price=Decimal("100"),
        ask_quantity=Decimal("1"),
    )

    assert state.spread_bps(
        Market.SPOT,
        "BTCUSDT",
        as_of_ms=2_000,
        maximum_age_ms=500,
    ) is None

    state.update(zero_spread)

    assert state.spread_bps(
        Market.SPOT,
        "BTCUSDT",
        as_of_ms=2_000,
        maximum_age_ms=500,
    ) == 0.0


@pytest.mark.parametrize(
    ("as_of_ms", "maximum_age_ms"),
    [
        (1_999, 500),  # Future book relative to the requested snapshot.
        (2_501, 500),  # Stale book beyond the inclusive freshness boundary.
    ],
)
def test_book_state_rejects_future_and_stale_books(
    as_of_ms: int, maximum_age_ms: int
) -> None:
    state = BookState()
    state.update(
        BookTicker(
            market=Market.SPOT,
            symbol="BTCUSDT",
            event_time_ms=2_000,
            bid_price=Decimal("99.9"),
            bid_quantity=Decimal("1"),
            ask_price=Decimal("100.1"),
            ask_quantity=Decimal("1"),
        )
    )

    assert state.spread_bps(
        Market.SPOT,
        "BTCUSDT",
        as_of_ms=as_of_ms,
        maximum_age_ms=maximum_age_ms,
    ) is None


def test_book_state_accepts_book_at_freshness_boundary() -> None:
    state = BookState()
    state.update(
        BookTicker(
            market=Market.SPOT,
            symbol="BTCUSDT",
            event_time_ms=2_000,
            bid_price=Decimal("99.9"),
            bid_quantity=Decimal("1"),
            ask_price=Decimal("100.1"),
            ask_quantity=Decimal("1"),
        )
    )

    assert state.spread_bps(
        Market.SPOT,
        "BTCUSDT",
        as_of_ms=2_500,
        maximum_age_ms=500,
    ) == pytest.approx(20.0)

    snapshot = state.snapshot(
        Market.SPOT,
        "BTCUSDT",
        as_of_ms=2_500,
        maximum_age_ms=500,
    )
    assert snapshot is not None
    assert snapshot.age_ms == 500
    assert snapshot.bid_quote_capacity == pytest.approx(99.9)
    assert snapshot.ask_quote_capacity == pytest.approx(100.1)


def test_book_state_uses_receipt_age_and_rejects_conflicting_cursor() -> None:
    state = BookState()
    book = BookTicker(
        market=Market.SPOT,
        symbol="BTCUSDT",
        event_time_ms=0,
        receipt_time_ms=2_000,
        bid_price=Decimal("99.9"),
        bid_quantity=Decimal("2"),
        ask_price=Decimal("100.1"),
        ask_quantity=Decimal("2"),
        update_id=7,
    )
    state.update(book)
    assert state.snapshot(
        Market.SPOT, "BTCUSDT", as_of_ms=2_500, maximum_age_ms=500
    ) is not None
    state.update(book.model_copy(update={"receipt_time_ms": 2_100}))
    with pytest.raises(BookTickerConflictError):
        state.update(book.model_copy(update={"ask_price": Decimal("100.2")}))


def test_book_state_rejects_invalid_book_and_negative_maximum_age() -> None:
    state = BookState()
    state.update(
        BookTicker(
            market=Market.SPOT,
            symbol="BTCUSDT",
            event_time_ms=2_000,
            bid_price=Decimal("101"),
            bid_quantity=Decimal("1"),
            ask_price=Decimal("100"),
            ask_quantity=Decimal("1"),
        )
    )

    assert state.spread_bps(
        Market.SPOT,
        "BTCUSDT",
        as_of_ms=2_000,
        maximum_age_ms=500,
    ) is None
    with pytest.raises(ValueError, match="maximum_age_ms"):
        state.spread_bps(
            Market.SPOT,
            "BTCUSDT",
            as_of_ms=2_000,
            maximum_age_ms=-1,
        )


def test_microstructure_stores_prune_all_removed_symbol_state() -> None:
    tracker = OrderFlowTracker(maximum_age_seconds=300, maximum_points=10)
    tracker.update(trade(1, 1_000, False))
    tracker.update(trade(1, 1_000, False).model_copy(update={"symbol": "ETHUSDT"}))
    assert tracker.retain_symbols(Market.FUTURES, frozenset({"ETHUSDT"})) == 1
    assert not tracker.snapshot(Market.FUTURES, "BTCUSDT", 1_000).available
    assert tracker.snapshot(Market.FUTURES, "ETHUSDT", 1_000).available

    state = BookState()
    btc_book = BookTicker(
        market=Market.SPOT,
        symbol="BTCUSDT",
        event_time_ms=2_000,
        bid_price=Decimal("99.9"),
        bid_quantity=Decimal("1"),
        ask_price=Decimal("100.1"),
        ask_quantity=Decimal("1"),
    )
    state.update(btc_book)
    state.update(btc_book.model_copy(update={"symbol": "ETHUSDT"}))
    assert state.retain_symbols(Market.SPOT, frozenset({"ETHUSDT"})) == 1
    assert state.spread_bps(
        Market.SPOT, "BTCUSDT", as_of_ms=2_000, maximum_age_ms=0
    ) is None
    assert state.spread_bps(
        Market.SPOT, "ETHUSDT", as_of_ms=2_000, maximum_age_ms=0
    ) == pytest.approx(20.0)
