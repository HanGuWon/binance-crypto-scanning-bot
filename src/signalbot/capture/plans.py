from __future__ import annotations

import re
from urllib.parse import quote

from signalbot.domain.enums import Market
from signalbot.exchange.binance.endpoints import (
    FUTURES_WS_MARKET,
    FUTURES_WS_PUBLIC,
    SPOT_WS_MARKET_DATA_ONLY,
    WebSocketPlan,
)

_SYMBOL_RE = re.compile(r"^[A-Z0-9]+USDT$")


def build_prospective_capture_plans(
    symbols: tuple[str, ...],
    *,
    batch_size: int = 25,
) -> tuple[WebSocketPlan, ...]:
    """Build the fixed 5m R4b evidence stream set without scanner-only streams.

    These plans are intentionally distinct from ``build_websocket_plans``: no
    all-market mini-ticker, non-5m kline, user stream, API key, or private route
    can enter this capture path.
    """

    if not symbols:
        raise ValueError("prospective capture requires at least one symbol")
    if len(set(symbols)) != len(symbols):
        raise ValueError("prospective capture symbols must be unique")
    if any(_SYMBOL_RE.fullmatch(symbol) is None for symbol in symbols):
        raise ValueError("prospective capture symbols must be normalized USDT symbols")
    if not 1 <= batch_size <= 50:
        raise ValueError("prospective capture batch_size must be between 1 and 50")
    lowered = tuple(symbol.lower() for symbol in symbols)
    spot = tuple(
        stream
        for symbol in lowered
        for stream in (
            f"{symbol}@kline_5m",
            f"{symbol}@aggTrade",
            f"{symbol}@bookTicker",
            f"{symbol}@depth@100ms",
        )
    )
    futures_market = tuple(
        stream
        for symbol in lowered
        for stream in (
            f"{symbol}@kline_5m",
            f"{symbol}@aggTrade",
            f"{symbol}@markPrice@1s",
        )
    )
    futures_public = tuple(
        stream
        for symbol in lowered
        for stream in (
            f"{symbol}@bookTicker",
            f"{symbol}@depth@100ms",
        )
    )
    plans: list[WebSocketPlan] = []
    plans.extend(
        _plans(
            "capture-spot",
            Market.SPOT,
            "spot",
            SPOT_WS_MARKET_DATA_ONLY,
            spot,
            batch_size,
        )
    )
    plans.extend(
        _plans(
            "capture-futures-market",
            Market.FUTURES,
            "market",
            FUTURES_WS_MARKET,
            futures_market,
            batch_size,
        )
    )
    plans.extend(
        _plans(
            "capture-futures-public",
            Market.FUTURES,
            "public",
            FUTURES_WS_PUBLIC,
            futures_public,
            batch_size,
        )
    )
    return tuple(plans)


def _plans(
    name: str,
    market: Market,
    route: str,
    base_url: str,
    streams: tuple[str, ...],
    batch_size: int,
) -> list[WebSocketPlan]:
    chunks = [streams[index : index + batch_size] for index in range(0, len(streams), batch_size)]
    return [
        WebSocketPlan(
            name=f"{name}-{index}",
            market=market,
            route=route,
            streams=chunk,
            url=base_url
            + "/".join(quote(stream, safe="@!_-") for stream in chunk),
        )
        for index, chunk in enumerate(chunks, start=1)
    ]
