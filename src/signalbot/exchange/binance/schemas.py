from __future__ import annotations

from decimal import Decimal, InvalidOperation
from typing import Any

from signalbot.domain.enums import Market
from signalbot.domain.models import AggTrade, BookTicker, Candle, MiniTicker

BinanceEvent = Candle | BookTicker | AggTrade | MiniTicker


class PayloadError(ValueError):
    pass


def _decimal(payload: dict[str, Any], key: str, default: str | None = None) -> Decimal:
    value = payload.get(key, default)
    if value is None:
        raise PayloadError(f"missing decimal field {key}")
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError) as exc:
        raise PayloadError(f"invalid decimal field {key}: {value!r}") from exc


def _integer(payload: dict[str, Any], key: str, default: int | None = None) -> int:
    value = payload.get(key, default)
    if value is None:
        raise PayloadError(f"missing integer field {key}")
    try:
        return int(value)
    except (TypeError, ValueError) as exc:
        raise PayloadError(f"invalid integer field {key}: {value!r}") from exc


def _unwrap(payload: Any) -> Any:
    if isinstance(payload, dict) and "data" in payload and "stream" in payload:
        return payload["data"]
    return payload


def parse_payload(market: Market, payload: Any) -> list[BinanceEvent]:
    try:
        data = _unwrap(payload)
        if isinstance(data, list):
            events: list[BinanceEvent] = []
            for item in data:
                if (
                    isinstance(item, dict)
                    and (event := _parse_mini_ticker(market, item)) is not None
                ):
                    events.append(event)
            return events
        if not isinstance(data, dict):
            raise PayloadError("payload must be an object or array")
        event_type = data.get("e")
        if event_type == "kline" or "k" in data:
            return [_parse_kline(market, data)]
        if event_type == "aggTrade":
            return [_parse_agg_trade(market, data)]
        if event_type in {"24hrMiniTicker", "24hrTicker"}:
            event = _parse_mini_ticker(market, data)
            return [event] if event is not None else []
        if event_type == "bookTicker" or {"s", "b", "a"}.issubset(data):
            return [_parse_book_ticker(market, data)]
        return []
    except PayloadError:
        raise
    except (KeyError, TypeError, ValueError, InvalidOperation) as exc:
        raise PayloadError("malformed Binance payload") from exc


def _parse_kline(market: Market, payload: dict[str, Any]) -> Candle:
    kline = payload.get("k")
    if not isinstance(kline, dict):
        raise PayloadError("kline payload missing k object")
    symbol = str(kline.get("s") or payload.get("s") or "")
    if not symbol:
        raise PayloadError("kline payload missing symbol")
    return Candle(
        market=market,
        symbol=symbol,
        interval=str(kline["i"]),
        open_time_ms=_integer(kline, "t"),
        close_time_ms=_integer(kline, "T"),
        open=_decimal(kline, "o"),
        high=_decimal(kline, "h"),
        low=_decimal(kline, "l"),
        close=_decimal(kline, "c"),
        volume=_decimal(kline, "v"),
        quote_volume=_decimal(kline, "q"),
        trade_count=_integer(kline, "n"),
        taker_buy_base_volume=_decimal(kline, "V"),
        taker_buy_quote_volume=_decimal(kline, "Q"),
        is_closed=bool(kline.get("x", False)),
    )


def _parse_agg_trade(market: Market, payload: dict[str, Any]) -> AggTrade:
    return AggTrade(
        market=market,
        symbol=str(payload["s"]),
        event_time_ms=_integer(payload, "E"),
        trade_time_ms=int(payload.get("T") or payload.get("E") or 0),
        price=_decimal(payload, "p"),
        quantity=_decimal(payload, "q"),
        is_buyer_maker=bool(payload["m"]),
        aggregate_trade_id=_integer(payload, "a") if payload.get("a") is not None else None,
    )


def _parse_book_ticker(market: Market, payload: dict[str, Any]) -> BookTicker:
    raw_event_time = payload.get("E")
    if raw_event_time is None:
        raw_event_time = payload.get("T")
    exchange_event_time_ms = (
        int(raw_event_time) if raw_event_time is not None else None
    )
    return BookTicker(
        market=market,
        symbol=str(payload["s"]),
        event_time_ms=exchange_event_time_ms or 0,
        exchange_event_time_ms=exchange_event_time_ms,
        bid_price=_decimal(payload, "b"),
        bid_quantity=_decimal(payload, "B", "0"),
        ask_price=_decimal(payload, "a"),
        ask_quantity=_decimal(payload, "A", "0"),
        update_id=_integer(payload, "u") if payload.get("u") is not None else None,
    )


def _optional_decimal(payload: dict[str, Any], key: str) -> Decimal | None:
    return _decimal(payload, key) if payload.get(key) is not None else None


def _parse_mini_ticker(market: Market, payload: dict[str, Any]) -> MiniTicker | None:
    if payload.get("s") is None or payload.get("c") is None:
        return None
    return MiniTicker(
        market=market,
        symbol=str(payload["s"]),
        event_time_ms=int(payload.get("E") or payload.get("T") or 0),
        close=_decimal(payload, "c"),
        open=_optional_decimal(payload, "o"),
        high=_optional_decimal(payload, "h"),
        low=_optional_decimal(payload, "l"),
        volume=_optional_decimal(payload, "v"),
        quote_volume=_optional_decimal(payload, "q"),
    )


def parse_rest_kline(market: Market, symbol: str, interval: str, row: list[Any]) -> Candle:
    if len(row) < 11:
        raise PayloadError("REST kline row must contain at least 11 fields")
    return Candle(
        market=market,
        symbol=symbol,
        interval=interval,
        open_time_ms=int(row[0]),
        open=Decimal(str(row[1])),
        high=Decimal(str(row[2])),
        low=Decimal(str(row[3])),
        close=Decimal(str(row[4])),
        volume=Decimal(str(row[5])),
        close_time_ms=int(row[6]),
        quote_volume=Decimal(str(row[7])),
        trade_count=int(row[8]),
        taker_buy_base_volume=Decimal(str(row[9])),
        taker_buy_quote_volume=Decimal(str(row[10])),
        is_closed=True,
    )
