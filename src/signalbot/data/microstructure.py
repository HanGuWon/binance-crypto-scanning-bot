from __future__ import annotations

from collections import defaultdict, deque
from dataclasses import dataclass, replace
from decimal import Decimal
from itertools import pairwise
from math import isfinite

from signalbot.domain.enums import Market
from signalbot.domain.models import AggTrade, BookTicker, Candle


@dataclass(frozen=True, slots=True)
class OrderFlowSnapshot:
    taker_buy_ratio: float = 0.5
    buy_quote_volume: float = 0.0
    sell_quote_volume: float = 0.0
    trade_count: int = 0
    available: bool = False

    @property
    def quote_delta(self) -> float:
        """Signed taker quote volume: buyer-initiated minus seller-initiated."""

        return self.buy_quote_volume - self.sell_quote_volume


@dataclass(frozen=True, slots=True)
class BookSnapshot:
    spread_bps: float
    age_ms: int
    bid_quote_capacity: float
    ask_quote_capacity: float


class BookTickerConflictError(RuntimeError):
    """Raised when one Binance source cursor maps to different BBO content."""


@dataclass(frozen=True, slots=True)
class MinutePressureSnapshot:
    """Explanation only: three closed minute bars, never an entry gate."""

    market: str
    symbol: str
    as_of_ms: int
    available: bool = False
    reason: str = "insufficient_bars"
    start_ms: int | None = None
    end_ms: int | None = None
    lag_ms: int | None = None
    buy_ratio: float | None = None
    last_buy_ratio: float | None = None
    price_change_percent: float | None = None
    summary: str = "1분 체결 자료 부족"
    version: str = "closed-minute-pressure-v1"


def minute_pressure_snapshot(
    candles: list[Candle],
    market: Market,
    symbol: str,
    as_of_ms: int,
) -> MinutePressureSnapshot:
    """Use the latest three contiguous minutes at most one minute behind as-of.

    Other series, future rows and intrabar rows are ignored. Selected rows must
    be unique, contiguous and valid; missing observations never imply balance.
    Price changes at exactly +/-0.05% are flat. These are observation seeds.
    """

    base = MinutePressureSnapshot(market.value, symbol, as_of_ms)
    rows = sorted(
        (
            c
            for c in candles
            if c.market is market
            and c.symbol == symbol
            and c.interval == "1m"
            and c.is_closed
            and c.close_time_ms <= as_of_ms
        ),
        key=lambda c: c.open_time_ms,
    )[-3:]
    if len(rows) != 3:
        return base
    if any(
        c.close_time_ms - c.open_time_ms != 59_999 or c.open_time_ms % 60_000 for c in rows
    ) or any(a.close_time_ms + 1 != b.open_time_ms for a, b in pairwise(rows)):
        return replace(base, reason="noncontiguous_bars")
    lag = as_of_ms - rows[-1].close_time_ms
    if lag > 60_000:
        return replace(base, reason="stale_bars")
    try:
        for candle in rows:
            Candle.model_validate(candle.model_dump(mode="python"))
        flows = [closed_kline_flow(c) for c in rows]
    except ValueError:
        return replace(base, reason="invalid_candle")
    if not all(flow.available for flow in flows):
        return replace(base, reason="missing_flow")
    total = sum((c.quote_volume for c in rows), Decimal(0))
    buys = sum((c.taker_buy_quote_volume for c in rows), Decimal(0))
    ratio = float(buys / total)
    change = rows[-1].close / rows[0].open - 1
    percent = float(change * 100)
    if not isfinite(percent):
        return replace(base, reason="invalid_price_change")
    up, down = change > Decimal("0.0005"), change < Decimal("-0.0005")
    if ratio >= 0.55:
        summary = "매수 체결 우세 · 가격 상승" if up else "매수 체결 우세지만 가격은 상승하지 않음"
    elif ratio <= 0.45:
        summary = (
            "매도 체결 우세 · 가격 하락" if down else "매도 체결 우세지만 가격은 하락하지 않음"
        )
    else:
        summary = "매수·매도 체결이 비슷함"
    return replace(
        base,
        available=True,
        reason="available",
        start_ms=rows[0].open_time_ms,
        end_ms=rows[-1].close_time_ms,
        lag_ms=lag,
        buy_ratio=ratio,
        last_buy_ratio=flows[-1].taker_buy_ratio,
        price_change_percent=percent,
        summary=summary,
    )


def closed_kline_flow(candle: Candle) -> OrderFlowSnapshot:
    """Build canonical order flow from one fully closed Binance kline.

    Quote volume is the sole denominator because it also gives an exact signed
    quote-volume delta and matches the research feature contract. A zero-quote
    candle has no defined quote-flow ratio even if base volume is non-zero, so
    its flow snapshot is explicitly unavailable.
    """

    totals = (candle.quote_volume, candle.volume)
    taker_buys = (candle.taker_buy_quote_volume, candle.taker_buy_base_volume)
    if any(not value.is_finite() or value < 0 for value in (*totals, *taker_buys)):
        raise ValueError("kline flow volumes must be finite and non-negative")
    if candle.trade_count < 0:
        raise ValueError("kline flow trade_count must be non-negative")
    if candle.taker_buy_quote_volume > candle.quote_volume:
        raise ValueError("taker-buy quote volume exceeds total quote volume")
    if candle.taker_buy_base_volume > candle.volume:
        raise ValueError("taker-buy base volume exceeds total base volume")

    if candle.quote_volume <= 0:
        return OrderFlowSnapshot(trade_count=candle.trade_count, available=False)
    ratio = candle.taker_buy_quote_volume / candle.quote_volume

    buy_quote = candle.taker_buy_quote_volume
    sell_quote = candle.quote_volume - buy_quote
    return OrderFlowSnapshot(
        taker_buy_ratio=float(ratio),
        buy_quote_volume=float(buy_quote),
        sell_quote_volume=float(sell_quote),
        trade_count=candle.trade_count,
        available=True,
    )


class OrderFlowTracker:
    """Bounded intrabar ``aggTrade`` flow; not the canonical closed-kline flow."""

    def __init__(self, maximum_age_seconds: int = 300, maximum_points: int = 10_000) -> None:
        self.maximum_age_ms = maximum_age_seconds * 1000
        self._trades: dict[tuple[Market, str], deque[AggTrade]] = defaultdict(
            lambda: deque(maxlen=maximum_points)
        )
        self._last_trade_ids: dict[tuple[Market, str], int] = {}

    def update(self, trade: AggTrade) -> None:
        key = (trade.market, trade.symbol)
        if trade.aggregate_trade_id is not None:
            previous = self._last_trade_ids.get(key)
            if previous is not None and trade.aggregate_trade_id <= previous:
                return
            self._last_trade_ids[key] = trade.aggregate_trade_id
        bucket = self._trades[key]
        bucket.append(trade)
        cutoff = trade.event_time_ms - self.maximum_age_ms
        while bucket and bucket[0].event_time_ms < cutoff:
            bucket.popleft()

    def snapshot(
        self, market: Market, symbol: str, now_ms: int, horizon_seconds: int = 60
    ) -> OrderFlowSnapshot:
        bucket = self._trades.get((market, symbol.upper()))
        if not bucket:
            return OrderFlowSnapshot()
        cutoff = now_ms - horizon_seconds * 1000
        buy = Decimal("0")
        sell = Decimal("0")
        count = 0
        for trade in reversed(bucket):
            if trade.event_time_ms > now_ms:
                continue
            if trade.event_time_ms < cutoff:
                break
            quote = trade.price * trade.quantity
            if trade.is_buyer_maker:
                sell += quote
            else:
                buy += quote
            count += 1
        total = buy + sell
        ratio = float(buy / total) if total > 0 else 0.5
        return OrderFlowSnapshot(ratio, float(buy), float(sell), count, count > 0)

    def retain_symbols(self, market: Market, symbols: frozenset[str]) -> int:
        """Prune both trade buckets and deduplication cursors after universe rotation."""

        allowed = frozenset(symbol.upper() for symbol in symbols)
        stale = {
            key
            for key in (*self._trades.keys(), *self._last_trade_ids.keys())
            if key[0] is market and key[1] not in allowed
        }
        for key in stale:
            self._trades.pop(key, None)
            self._last_trade_ids.pop(key, None)
        return len(stale)


class BookState:
    def __init__(self) -> None:
        self._books: dict[tuple[Market, str], BookTicker] = {}

    def update(self, book: BookTicker) -> None:
        key = (book.market, book.symbol)
        existing = self._books.get(key)
        if existing is None:
            self._books[key] = book
            return
        same_cursor = (
            book.update_id == existing.update_id
            if book.update_id is not None and existing.update_id is not None
            else book.event_time_ms == existing.event_time_ms
        )
        if same_cursor:
            source_fields = (
                "bid_price",
                "bid_quantity",
                "ask_price",
                "ask_quantity",
                "exchange_event_time_ms",
            )
            if any(getattr(book, field) != getattr(existing, field) for field in source_fields):
                raise BookTickerConflictError(
                    f"conflicting book ticker cursor for {book.market.value}:{book.symbol}"
                )
            return
        source_is_newer = (
            book.update_id > existing.update_id
            if book.update_id is not None and existing.update_id is not None
            else book.event_time_ms > existing.event_time_ms
        )
        if source_is_newer:
            self._books[key] = book

    def spread_bps(
        self,
        market: Market,
        symbol: str,
        *,
        as_of_ms: int,
        maximum_age_ms: int,
    ) -> float | None:
        snapshot = self.snapshot(
            market,
            symbol,
            as_of_ms=as_of_ms,
            maximum_age_ms=maximum_age_ms,
        )
        return None if snapshot is None else snapshot.spread_bps

    def snapshot(
        self,
        market: Market,
        symbol: str,
        *,
        as_of_ms: int,
        maximum_age_ms: int,
    ) -> BookSnapshot | None:
        """Return one fresh, internally valid top-of-book observation."""

        if maximum_age_ms < 0:
            raise ValueError("maximum_age_ms must be non-negative")
        book = self._books.get((market, symbol.upper()))
        if book is None:
            return None
        observed_at_ms = book.receipt_time_ms or book.event_time_ms
        age_ms = as_of_ms - observed_at_ms
        if age_ms < 0 or age_ms > maximum_age_ms:
            return None
        if (
            book.ask_price <= 0
            or book.bid_price <= 0
            or book.ask_price < book.bid_price
            or book.ask_quantity < 0
            or book.bid_quantity < 0
        ):
            return None
        midpoint = (book.ask_price + book.bid_price) / Decimal("2")
        spread = float(
            (book.ask_price - book.bid_price) / midpoint * Decimal("10000")
        )
        return BookSnapshot(
            spread_bps=spread,
            age_ms=age_ms,
            bid_quote_capacity=float(book.bid_price * book.bid_quantity),
            ask_quote_capacity=float(book.ask_price * book.ask_quantity),
        )

    def retain_symbols(self, market: Market, symbols: frozenset[str]) -> int:
        """Drop books for symbols that are no longer streamed."""

        allowed = frozenset(symbol.upper() for symbol in symbols)
        stale = [
            key
            for key in self._books
            if key[0] is market and key[1] not in allowed
        ]
        for key in stale:
            del self._books[key]
        return len(stale)
