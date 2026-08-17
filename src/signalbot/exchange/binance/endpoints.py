from __future__ import annotations

from dataclasses import dataclass
from urllib.parse import quote

from signalbot.domain.enums import Market

SPOT_REST_BASE = "https://api.binance.com"
SPOT_MARKET_DATA_REST_BASE = "https://data-api.binance.vision"
FUTURES_REST_BASE = "https://fapi.binance.com"
SPOT_WS_COMBINED = "wss://stream.binance.com:9443/stream?streams="
SPOT_WS_MARKET_DATA_ONLY = "wss://data-stream.binance.vision:443/stream?streams="
FUTURES_WS_PUBLIC = "wss://fstream.binance.com/public/stream?streams="
FUTURES_WS_MARKET = "wss://fstream.binance.com/market/stream?streams="


@dataclass(frozen=True, slots=True)
class WebSocketPlan:
    name: str
    market: Market
    route: str
    streams: tuple[str, ...]
    url: str


def rest_base(market: Market) -> str:
    return SPOT_REST_BASE if market is Market.SPOT else FUTURES_REST_BASE


def exchange_info_path(market: Market) -> str:
    return "/api/v3/exchangeInfo" if market is Market.SPOT else "/fapi/v1/exchangeInfo"


def ticker_24h_path(market: Market) -> str:
    return "/api/v3/ticker/24hr" if market is Market.SPOT else "/fapi/v1/ticker/24hr"


def klines_path(market: Market) -> str:
    return "/api/v3/klines" if market is Market.SPOT else "/fapi/v1/klines"


def funding_rate_history_path() -> str:
    return "/fapi/v1/fundingRate"


def _combined_url(base: str, streams: tuple[str, ...]) -> str:
    return base + "/".join(quote(stream, safe="@!_-") for stream in streams)


def _chunks(values: list[str], size: int) -> list[tuple[str, ...]]:
    return [tuple(values[index : index + size]) for index in range(0, len(values), size)]


def build_websocket_plans(
    market: Market, symbols: list[str], intervals: list[str], batch_size: int
) -> list[WebSocketPlan]:
    lowered = [symbol.lower() for symbol in symbols]
    plans: list[WebSocketPlan] = []
    if market is Market.SPOT:
        detailed: list[str] = []
        for symbol in lowered:
            detailed.extend(f"{symbol}@kline_{interval}" for interval in intervals)
            detailed.extend((f"{symbol}@aggTrade", f"{symbol}@bookTicker"))
        for index, streams in enumerate(_chunks(detailed, batch_size), start=1):
            plans.append(
                WebSocketPlan(
                    f"spot-detailed-{index}",
                    market,
                    "spot",
                    streams,
                    _combined_url(SPOT_WS_COMBINED, streams),
                )
            )
        all_market = ("!miniTicker@arr",)
        plans.append(
            WebSocketPlan(
                "spot-all-mini-ticker",
                market,
                "spot",
                all_market,
                _combined_url(SPOT_WS_COMBINED, all_market),
            )
        )
        return plans
    market_streams: list[str] = []
    public_streams: list[str] = []
    for symbol in lowered:
        market_streams.extend(f"{symbol}@kline_{interval}" for interval in intervals)
        market_streams.append(f"{symbol}@aggTrade")
        public_streams.append(f"{symbol}@bookTicker")
    for index, streams in enumerate(_chunks(market_streams, batch_size), start=1):
        plans.append(
            WebSocketPlan(
                f"futures-market-{index}",
                market,
                "market",
                streams,
                _combined_url(FUTURES_WS_MARKET, streams),
            )
        )
    for index, streams in enumerate(_chunks(public_streams, batch_size), start=1):
        plans.append(
            WebSocketPlan(
                f"futures-public-{index}",
                market,
                "public",
                streams,
                _combined_url(FUTURES_WS_PUBLIC, streams),
            )
        )
    all_market = ("!miniTicker@arr",)
    plans.append(
        WebSocketPlan(
            "futures-all-mini-ticker",
            market,
            "market",
            all_market,
            _combined_url(FUTURES_WS_MARKET, all_market),
        )
    )
    return plans
