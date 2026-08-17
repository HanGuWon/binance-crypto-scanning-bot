import json
from pathlib import Path

import pytest

from signalbot.domain.enums import Market
from signalbot.domain.models import AggTrade, BookTicker, Candle, MiniTicker
from signalbot.exchange.binance.schemas import PayloadError, parse_payload, parse_rest_kline

FIXTURES = Path(__file__).resolve().parents[1] / "fixtures/binance"


def load(name: str):
    return json.loads((FIXTURES / name).read_text(encoding="utf-8"))


def test_closed_and_open_kline_contract() -> None:
    closed = parse_payload(Market.SPOT, load("spot_kline_closed.json"))[0]
    opened = parse_payload(Market.SPOT, load("spot_kline_open.json"))[0]
    assert isinstance(closed, Candle) and closed.is_closed is True
    assert isinstance(opened, Candle) and opened.is_closed is False
    assert closed.symbol == "BTCUSDT"
    assert closed.interval == "5m"


def test_trade_book_and_mini_ticker_contracts() -> None:
    trade = parse_payload(Market.SPOT, load("spot_agg_trade.json"))[0]
    book = parse_payload(Market.SPOT, load("spot_book_ticker.json"))[0]
    futures_book = parse_payload(Market.FUTURES, load("futures_book_ticker.json"))[0]
    tickers = parse_payload(Market.SPOT, load("spot_mini_tickers.json"))
    assert isinstance(trade, AggTrade) and trade.is_buyer_maker is False
    assert isinstance(book, BookTicker) and book.event_time_ms == 0
    assert book.exchange_event_time_ms is None
    assert isinstance(futures_book, BookTicker) and futures_book.event_time_ms > 0
    assert futures_book.exchange_event_time_ms == futures_book.event_time_ms
    assert len(tickers) == 2 and all(isinstance(value, MiniTicker) for value in tickers)


def test_unknown_event_is_ignored_but_malformed_payload_is_rejected() -> None:
    assert parse_payload(Market.SPOT, {"e": "newFutureEvent", "s": "BTCUSDT"}) == []
    with pytest.raises(PayloadError):
        parse_payload(Market.SPOT, "not-an-object")


def test_rest_kline_requires_exchange_shape() -> None:
    row = [0, "1", "2", "0.5", "1.5", "10", 59_999, "15", 3, "6", "9", "0"]
    candle = parse_rest_kline(Market.SPOT, "btcusdt", "1m", row)
    assert candle.symbol == "BTCUSDT"
    assert candle.is_closed is True
    with pytest.raises(PayloadError):
        parse_rest_kline(Market.SPOT, "BTCUSDT", "1m", row[:5])


def test_malformed_provider_fields_are_normalized_to_payload_error() -> None:
    malformed_ticker = {"e": "24hrMiniTicker", "s": "BTCUSDT", "c": "not-a-number"}
    malformed_trade = {"e": "aggTrade", "s": "BTCUSDT"}
    with pytest.raises(PayloadError):
        parse_payload(Market.SPOT, malformed_ticker)
    with pytest.raises(PayloadError):
        parse_payload(Market.SPOT, malformed_trade)


@pytest.mark.parametrize("field", ["q", "n", "V", "Q"])
def test_kline_requires_all_volume_and_flow_fields(field: str) -> None:
    payload = load("spot_kline_closed.json")
    del payload["data"]["k"][field]

    with pytest.raises(PayloadError):
        parse_payload(Market.SPOT, payload)


@pytest.mark.parametrize(
    ("field", "value"),
    [("Q", "999999999"), ("V", "999999999"), ("q", "NaN")],
)
def test_kline_rejects_invalid_volume_relationships(field: str, value: str) -> None:
    payload = load("spot_kline_closed.json")
    payload["data"]["k"][field] = value

    with pytest.raises(PayloadError):
        parse_payload(Market.SPOT, payload)
