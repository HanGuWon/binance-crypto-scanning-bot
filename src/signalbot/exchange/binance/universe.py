from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import Any

from signalbot.clock import Clock
from signalbot.config import BinanceSettings
from signalbot.domain.enums import Market
from signalbot.domain.models import Instrument
from signalbot.exchange.binance.rest import BinanceRestClient

LEVERAGED_SUFFIXES = ("UP", "DOWN", "BULL", "BEAR")


@dataclass(frozen=True, slots=True)
class Universe:
    market: Market
    tradable: tuple[Instrument, ...]
    surveillance: tuple[Instrument, ...]

    @property
    def tradable_symbols(self) -> list[str]:
        return [item.symbol for item in self.tradable]

    @property
    def surveillance_symbols(self) -> frozenset[str]:
        return frozenset(item.symbol for item in self.surveillance)


class UniverseSelector:
    def __init__(self, settings: BinanceSettings, clock: Clock) -> None:
        self.settings = settings
        self.clock = clock

    async def select(self, client: BinanceRestClient) -> Universe:
        info, rows = await client.exchange_info(), await client.tickers_24h()
        volumes = self._ticker_volumes(rows)
        candidates = self._instruments(client.market, info, volumes)
        ranked = sorted(
            candidates,
            key=lambda item: (-item.quote_volume, item.symbol),
        )
        surveillance = tuple(ranked[: self.settings.surveillance_n])
        liquid = [
            item
            for item in candidates
            if float(item.quote_volume) >= self.settings.min_quote_volume
        ]
        tradable = tuple(
            sorted(liquid, key=lambda item: (-item.quote_volume, item.symbol))[
                : self.settings.top_n
            ]
        )
        return Universe(client.market, tradable, surveillance)

    @staticmethod
    def _ticker_volumes(rows: list[dict[str, Any]]) -> dict[str, Decimal]:
        result: dict[str, Decimal] = {}
        for row in rows:
            symbol = str(row.get("symbol", "")).upper()
            if not symbol:
                continue
            try:
                result[symbol] = Decimal(str(row.get("quoteVolume", "0")))
            except (InvalidOperation, ValueError):
                result[symbol] = Decimal("0")
        return result

    def _instruments(
        self, market: Market, payload: dict[str, Any], volumes: dict[str, Decimal]
    ) -> list[Instrument]:
        rows = payload.get("symbols", [])
        if not isinstance(rows, list):
            return []
        now = self.clock.now_ms()
        min_age = self.settings.minimum_age_days * 86_400_000
        excluded = set(self.settings.excluded_base_assets)
        blacklist = set(self.settings.blacklist)
        out = []
        for row in rows:
            if not isinstance(row, dict):
                continue
            symbol = str(row.get("symbol", "")).upper()
            base = str(row.get("baseAsset", "")).upper()
            quote_asset = str(row.get("quoteAsset", "")).upper()
            status = str(row.get("status", "")).upper()
            onboard_raw = row.get("onboardDate") or row.get("onboardTime")
            onboard = int(onboard_raw) if onboard_raw is not None else None
            contract_raw = row.get("contractType")
            contract = str(contract_raw).upper() if contract_raw is not None else None
            if not symbol or quote_asset != self.settings.quote_asset or status != "TRADING":
                continue
            if (
                symbol in blacklist
                or base in excluded
                or any(base.endswith(s) for s in LEVERAGED_SUFFIXES)
            ):
                continue
            if market is Market.SPOT and row.get("isSpotTradingAllowed") is False:
                continue
            if market is Market.FUTURES and contract != "PERPETUAL":
                continue
            if onboard is not None and now - onboard < min_age:
                continue
            out.append(
                Instrument(
                    market=market,
                    symbol=symbol,
                    base_asset=base,
                    quote_asset=quote_asset,
                    status=status,
                    quote_volume=volumes.get(symbol, Decimal("0")),
                    onboard_time_ms=onboard,
                    contract_type=contract,
                )
            )
        return out
