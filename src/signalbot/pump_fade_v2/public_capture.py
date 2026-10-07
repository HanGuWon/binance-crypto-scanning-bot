"""Opt-in USD-M public capture plans and receipt-aware normalization.

Use these plans with ``capture.websocket.PublicWebSocketCaptureAdapter`` and its
existing bounded durable ``CapturePipeline``. This module cannot start capture,
subscribe, place orders or send Discord messages by itself.
"""

from __future__ import annotations

import hashlib
import json
import math
import re
from dataclasses import dataclass
from itertools import pairwise
from typing import Any, Literal
from urllib.parse import quote

from signalbot.capture.models import validate_public_rest_path
from signalbot.capture.websocket import validate_public_websocket_plan
from signalbot.domain.enums import Market
from signalbot.exchange.binance.endpoints import (
    FUTURES_WS_MARKET,
    FUTURES_WS_PUBLIC,
    WebSocketPlan,
)
from signalbot.pump_fade_v2.state import (
    ClosedBar,
    FundingCap,
    LiquidationSample,
    Observation,
)

_SYMBOL = re.compile(r"^[A-Z0-9]+USDT$")


@dataclass(frozen=True, slots=True)
class PumpRestPoll:
    """Keyless opt-in request for the existing receipt-stamped REST adapter."""

    role: str
    path: str
    query: tuple[tuple[str, str], ...]
    min_interval_seconds: int
    maximum_attempts: int = 2
    trigger: Literal["interval", "bootstrap", "depth_resync", "utc_5m"] = "interval"


def pump_rest_poll_plan(symbols: tuple[str, ...]) -> tuple[PumpRestPoll, ...]:
    """Declare bounded USD-M REST requests; caller supplies rate-limit scheduler.

    ``fundingInfo`` is a single all-market snapshot; missing a symbol row does
    not imply no cap change. This planner makes no HTTP requests.
    """

    if not 1 <= len(symbols) <= 24 or len(set(symbols)) != len(symbols):
        raise ValueError("1..24 distinct admitted symbols required for REST poll")
    if any(_SYMBOL.fullmatch(symbol) is None for symbol in symbols):
        raise ValueError("symbols must be normalized USD-M USDT")
    result = [
        PumpRestPoll("exchange_info_all", "/fapi/v1/exchangeInfo", (), 300),
        PumpRestPoll("funding_info_all", "/fapi/v1/fundingInfo", (), 300),
    ]
    for symbol in symbols:
        result.extend((
            PumpRestPoll(f"depth_{symbol}", "/fapi/v1/depth",
                         (("limit", "1000"), ("symbol", symbol)), 0,
                         trigger="depth_resync"),
            PumpRestPoll(f"quality_depth_{symbol}", "/fapi/v1/depth",
                         (("limit", "1000"), ("symbol", symbol)), 300,
                         trigger="utc_5m"),
            PumpRestPoll(f"bootstrap_5m_{symbol}", "/fapi/v1/klines",
                         (("interval", "5m"), ("limit", "289"), ("symbol", symbol)),
                         0, trigger="bootstrap"),
            PumpRestPoll(f"oi_{symbol}", "/fapi/v1/openInterest",
                         (("symbol", symbol),), 60),
            PumpRestPoll(f"premium_{symbol}", "/fapi/v1/premiumIndex",
                         (("symbol", symbol),), 30),
            PumpRestPoll(f"oi_history_{symbol}", "/futures/data/openInterestHist",
                         (("limit", "2"), ("period", "5m"), ("symbol", symbol)), 300),
        ))
    for entry in result:
        validate_public_rest_path(Market.FUTURES, entry.path)
        if tuple(sorted(entry.query)) != entry.query:
            raise ValueError("REST query must be canonicalized")
    return tuple(result)


def pump_public_plans(
    symbols: tuple[str, ...], *, batch_size: int = 24
) -> tuple[WebSocketPlan, ...]:
    """Plan bounded capture for admitted symbols; global forceOrder is censored.

    The caller is responsible for selecting symbols using point-in-time events.
    All-market liquidation observations need symbol and ``st==1`` filtering.
    """

    if not symbols or len(symbols) != len(set(symbols)):
        raise ValueError("at least one unique admitted symbol required")
    if any(_SYMBOL.fullmatch(s) is None for s in symbols):
        raise ValueError("symbols must be normalized USD-M USDT symbols")
    if not 1 <= batch_size <= 48:
        raise ValueError("batch_size must be between 1 and 48")
    market = tuple(
        stream for symbol in symbols for stream in (
            f"{symbol.lower()}@kline_5m", f"{symbol.lower()}@aggTrade",
            f"{symbol.lower()}@markPrice@1s",
        )
    )
    public = tuple(stream for symbol in symbols for stream in (
        f"{symbol.lower()}@bookTicker", f"{symbol.lower()}@depth@100ms",
    ))
    plans: list[WebSocketPlan] = []
    for route, base, streams in (
        ("market", FUTURES_WS_MARKET, market),
        ("public", FUTURES_WS_PUBLIC, public),
        ("market", FUTURES_WS_MARKET, ("!forceOrder@arr",)),
    ):
        for i in range(0, len(streams), batch_size):
            chunk = streams[i:i + batch_size]
            name = f"pump-v2-{route}-{'force' if chunk[0].startswith('!') else i // batch_size}"
            url = base + "/".join(quote(stream, safe="@!_-") for stream in chunk)
            plan = WebSocketPlan(name, Market.FUTURES, route, chunk, url)
            validate_public_websocket_plan(plan)
            plans.append(plan)
    return tuple(plans)


@dataclass(frozen=True, slots=True)
class RawResearchEvidence:
    """Deterministically indexed *observation*, never inferred event absence."""

    evidence_id: str
    stream: str
    symbol: str
    exchange_event_ms: int
    received_ms: int
    fields: dict[str, str | int | float | bool | None]
    censored_sample: bool


@dataclass(frozen=True, slots=True)
class ListingObservation:
    """Point-in-time public exchangeInfo evidence used only prospectively."""

    symbol: str
    onboard_ms: int
    observed_ms: int
    status: str

    def __post_init__(self) -> None:
        if not self.symbol or self.onboard_ms < 0 or self.observed_ms < self.onboard_ms:
            raise ValueError("invalid listing observation")
        if not self.status:
            raise ValueError("listing status must be non-empty")


def _finite_number(value: object) -> float:
    number = float(str(value))
    if not math.isfinite(number):
        raise ValueError("non-finite Binance number")
    return number


def _funding_rate(value: object) -> float:
    """Parse Binance decimal-rate units; reject percent-points and impossible rates."""

    rate = _finite_number(value)
    if not -1 <= rate <= 1:
        raise ValueError("funding rate must be a decimal fraction within [-1, 1]")
    return rate


def normalize_public_frame(
    raw: dict[str, Any], *, received_ms: int,
    admitted_symbols: frozenset[str],
) -> RawResearchEvidence | None:
    """Normalize only admitted USD-M observations without manufacturing empty records."""

    frame = raw.get("data", raw)
    if not isinstance(frame, dict):
        raise ValueError("frame must contain a mapping")
    event_type = frame.get("e")
    order = frame.get("o", {}) if event_type == "forceOrder" else {}
    if event_type == "forceOrder" and not isinstance(order, dict):
        raise ValueError("forced order must be a mapping")
    symbol = str(order.get("s", "") if event_type == "forceOrder" else frame.get("s", ""))
    if symbol not in admitted_symbols:
        return None
    if frame.get("st", 1) != 1:
        return None  # post-2026 migration combines USD-M and COIN-M
    if event_type == "forceOrder":
        if frame.get("e") != "forceOrder" or order.get("S") not in ("BUY", "SELL"):
            raise ValueError("invalid liquidation side")
        fields: dict[str, str | int | float | bool | None] = {
            "forced_order_side": str(order["S"]),
            "short_liquidation": order["S"] == "BUY",
            "price": _finite_number(order.get("ap", order.get("p"))),
            "last_filled_quantity": _finite_number(order.get("l", "0")),
            "order_trade_ms": int(order["T"]),
            "sample_scope": "latest_symbol_order_in_one_second_not_total",
        }
        stream = "!forceOrder@arr"
    elif event_type == "markPriceUpdate":
        fields = {
            "mark_price": _finite_number(frame["p"]),
            "predicted_funding_rate": _finite_number(frame["r"]),
            "next_funding_ms": int(frame["T"]),
        }
        stream = f"{symbol.lower()}@markPrice@1s"
    elif event_type == "bookTicker":
        fields = {"bid": _finite_number(frame["b"]),
                  "ask": _finite_number(frame["a"]),
                  "bid_quantity": _finite_number(frame["B"]),
                  "ask_quantity": _finite_number(frame["A"])}
        stream = f"{symbol.lower()}@bookTicker"
    else:
        return None  # original raw frame remains in capture pipeline
    exchange_ms = int(frame["E"])
    if exchange_ms > received_ms:
        raise ValueError("receipt precedes exchange event")
    body = json.dumps([stream, symbol, exchange_ms, fields], sort_keys=True,
                      separators=(",", ":"), ensure_ascii=True)
    return RawResearchEvidence(hashlib.sha256(body.encode()).hexdigest(), stream, symbol,
                               exchange_ms, received_ms, fields,
                               event_type == "forceOrder")


def normalize_open_interest(payload: dict[str, Any], *, received_ms: int) -> Observation:
    """Map public /fapi/v1/openInterest response with receipt-time authority."""

    return Observation(event_ms=int(payload["time"]), received_ms=received_ms,
                       value=_finite_number(payload["openInterest"]))


def normalize_exchange_info(
    payload: dict[str, Any], *, received_ms: int, admitted_symbols: frozenset[str],
) -> tuple[ListingObservation, ...]:
    """Extract current USD-M listing metadata with receipt-time authority.

    This is prospective evidence only: a snapshot received today is never used to
    infer the listing state that existed before ``received_ms``.
    """

    rows = payload.get("symbols")
    if not isinstance(rows, list):
        raise ValueError("exchangeInfo symbols must be an array")
    output: list[ListingObservation] = []
    seen: set[str] = set()
    for raw in rows:
        if not isinstance(raw, dict):
            raise ValueError("exchangeInfo symbol row must be a mapping")
        symbol = str(raw.get("symbol", ""))
        if symbol not in admitted_symbols:
            continue
        if symbol in seen:
            raise ValueError("duplicate exchangeInfo symbol row")
        seen.add(symbol)
        onboard = raw.get("onboardDate")
        if isinstance(onboard, bool) or not isinstance(onboard, int):
            raise ValueError("exchangeInfo onboardDate must be an integer")
        status = str(raw.get("status", raw.get("contractStatus", "")))
        output.append(ListingObservation(symbol, onboard, received_ms, status))
    output.sort(key=lambda item: item.symbol)
    return tuple(output)


def normalize_rest_klines(
    payload: list[list[Any]], *, received_ms: int,
) -> tuple[ClosedBar, ...]:
    """Normalize only fully closed 5m REST klines available by response receipt."""

    rows: list[ClosedBar] = []
    for raw in payload:
        if len(raw) < 8:
            raise ValueError("REST kline row is incomplete")
        close_ms = int(raw[6])
        if close_ms >= received_ms:
            continue
        rows.append(ClosedBar(
            close_ms=close_ms,
            received_ms=received_ms,
            open=_finite_number(raw[1]),
            high=_finite_number(raw[2]),
            low=_finite_number(raw[3]),
            close=_finite_number(raw[4]),
            base_volume=_finite_number(raw[5]),
            quote_volume=_finite_number(raw[7]),
        ))
    rows.sort(key=lambda item: item.close_ms)
    if any(a.close_ms == b.close_ms for a, b in pairwise(rows)):
        raise ValueError("duplicate REST kline close time")
    return tuple(rows)


def normalize_oi_history(
    payload: list[dict[str, Any]], *, received_ms: int,
) -> tuple[Observation, ...]:
    """Normalize closed public five-minute OI history rows in timestamp order.

    The caller must still apply the preregistered one-row availability lag.  A
    REST response receipt is the earliest time *all* returned rows may be used;
    the exchange timestamp alone is never treated as publication time.
    """

    rows: list[Observation] = []
    for row in payload:
        event_ms = int(row["timestamp"])
        if event_ms > received_ms:
            raise ValueError("OI history row timestamp exceeds receipt time")
        rows.append(Observation(event_ms, received_ms,
                                _finite_number(row["sumOpenInterest"])))
    rows.sort(key=lambda item: (item.event_ms, item.received_ms))
    if any(a.event_ms == b.event_ms for a, b in pairwise(rows)):
        raise ValueError("duplicate OI history timestamps")
    return tuple(rows)


def normalize_premium_index(payload: dict[str, Any], *, received_ms: int) -> Observation:
    """Normalize the public premium-index funding observation with receipt authority."""

    return Observation(event_ms=int(payload["time"]), received_ms=received_ms,
                       value=_funding_rate(payload["lastFundingRate"]))


def normalize_closed_kline_frame(
    raw: dict[str, Any], *, received_ms: int,
    admitted_symbols: frozenset[str],
) -> tuple[str, ClosedBar] | None:
    """Convert one fully closed USD-M 5m kline frame to the decision-core type.

    Partial klines are intentionally ignored; wrong stream type or a close that
    was not yet available at local receipt fails closed.
    """

    frame = raw.get("data", raw)
    if not isinstance(frame, dict) or frame.get("e") != "kline":
        return None
    symbol = str(frame.get("s", ""))
    if symbol not in admitted_symbols:
        return None
    if frame.get("st", 1) != 1:
        return None
    kline = frame.get("k")
    if not isinstance(kline, dict):
        raise ValueError("kline event lacks a kline mapping")
    if kline.get("i") != "5m":
        return None
    if kline.get("x") is not True:
        return None
    close_ms = int(kline["T"])
    if received_ms <= close_ms:
        raise ValueError("closed kline receipt must follow exchange close")
    bar = ClosedBar(
        close_ms=close_ms,
        received_ms=received_ms,
        open=_finite_number(kline["o"]),
        high=_finite_number(kline["h"]),
        low=_finite_number(kline["l"]),
        close=_finite_number(kline["c"]),
        base_volume=_finite_number(kline["v"]),
        quote_volume=_finite_number(kline["q"]),
    )
    return symbol, bar


def evidence_to_predicted_funding(sample: RawResearchEvidence) -> Observation:
    """Convert only a receipt-stamped mark-price funding observation."""

    if not sample.stream.endswith("@markPrice@1s"):
        raise ValueError("predicted funding requires a mark-price observation")
    value = sample.fields.get("predicted_funding_rate")
    if value is None:
        raise ValueError("mark-price observation lacks predicted funding")
    return Observation(sample.exchange_event_ms, sample.received_ms, _funding_rate(value))


def normalize_funding_metadata(
    payload: list[dict[str, Any]], *, symbol: str, received_ms: int,
) -> FundingCap | None:
    """Return observed adjusted cap; lack of a symbol row is NOT historical proof."""

    matches = [entry for entry in payload if entry.get("symbol") == symbol]
    if len(matches) > 1:
        raise ValueError("duplicate funding metadata rows")
    if not matches:
        return None
    row = matches[0]
    floor = row.get("adjustedFundingRateFloor")
    return FundingCap(received_ms, _funding_rate(row["adjustedFundingRateCap"]),
                      int(row["fundingIntervalHours"]),
                      None if floor is None else _funding_rate(floor))


def normalize_liquidation(sample: RawResearchEvidence) -> LiquidationSample:
    """A forced BUY closes a short; stream is a censored sample."""

    if not sample.censored_sample or sample.stream != "!forceOrder@arr":
        raise ValueError("expected censored liquidation observation")
    side = str(sample.fields["forced_order_side"])
    return LiquidationSample(sample.exchange_event_ms, sample.received_ms,
                             sample.symbol, side)
