from __future__ import annotations

import logging
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import Any

from signalbot.clock import Clock
from signalbot.config import BinanceSettings
from signalbot.domain.enums import Market
from signalbot.domain.models import Instrument
from signalbot.exchange.binance.rest import (
    BinanceRateLimitError,
    BinanceRestClient,
    BinanceRestError,
)

LEVERAGED_SUFFIXES = ("UP", "DOWN", "BULL", "BEAR")
BENCHMARK_SYMBOL = "BTCUSDT"
LOGGER = logging.getLogger(__name__)


class RequiredUniverseUnavailableError(RuntimeError):
    """A preregistered live population cannot be represented by the exchange."""


@dataclass(frozen=True, slots=True)
class Universe:
    market: Market
    tradable: tuple[Instrument, ...]
    surveillance: tuple[Instrument, ...]
    context: tuple[Instrument, ...] = ()

    @property
    def tradable_symbols(self) -> list[str]:
        return [item.symbol for item in self.tradable]

    @property
    def surveillance_symbols(self) -> frozenset[str]:
        return frozenset(item.symbol for item in self.surveillance)

    @property
    def context_symbols(self) -> frozenset[str]:
        return frozenset(item.symbol for item in self.context)


class UniverseSelector:
    def __init__(
        self,
        settings: BinanceSettings,
        clock: Clock,
        *,
        required_symbols: tuple[str, ...] = (),
    ) -> None:
        self.settings = settings
        self.clock = clock
        self.required_symbols = frozenset(symbol.upper() for symbol in required_symbols)
        if len(self.required_symbols) > settings.top_n:
            raise ValueError("required symbol count exceeds top_n")
        if len(self.required_symbols) > settings.surveillance_n:
            raise ValueError("required symbol count exceeds surveillance_n")
        self._spot_age_anchor_ms: dict[str, int] = {}

    def _surveillance_with_benchmark(
        self,
        ranked: list[Instrument],
        benchmark: Instrument | None,
        required_symbols: frozenset[str],
    ) -> tuple[Instrument, ...]:
        required_items = [item for item in ranked if item.symbol in required_symbols]
        chosen = {item.symbol for item in required_items}
        if (
            benchmark is not None
            and benchmark.symbol not in chosen
            and len(chosen) < self.settings.surveillance_n
        ):
            chosen.add(benchmark.symbol)
        for item in ranked:
            if len(chosen) >= self.settings.surveillance_n:
                break
            chosen.add(item.symbol)
        return tuple(item for item in ranked if item.symbol in chosen)

    async def _spot_age_qualified(
        self,
        client: BinanceRestClient,
        ranked: list[Instrument],
    ) -> list[Instrument]:
        """Apply minimum Spot age using earliest public daily-kline evidence.

        Spot exchangeInfo does not provide an onboard timestamp. Resolve only
        as many volume-ranked symbols as are needed to fill the bounded
        surveillance/tradable panels, plus the BTC benchmark if present, and
        cache successful anchors for subsequent universe refreshes.
        """

        min_age_ms = self.settings.minimum_age_days * 86_400_000
        if min_age_ms <= 0:
            return ranked

        now_ms = self.clock.now_ms()
        benchmark_present = any(item.symbol == BENCHMARK_SYMBOL for item in ranked)
        benchmark_seen = not benchmark_present
        qualified: list[Instrument] = []
        liquid_qualified = 0
        for item in ranked:
            if item.symbol == BENCHMARK_SYMBOL:
                benchmark_seen = True
            anchor_ms = self._spot_age_anchor_ms.get(item.symbol)
            if anchor_ms is None:
                try:
                    anchor_ms = await client.earliest_kline_open_time_ms(item.symbol)
                except BinanceRateLimitError:
                    raise
                except BinanceRestError as exc:
                    LOGGER.warning(
                        "unable to resolve Spot public-data age anchor; excluding symbol",
                        extra={"symbol": item.symbol},
                        exc_info=exc,
                    )
                    continue
                if anchor_ms is None:
                    continue
                self._spot_age_anchor_ms[item.symbol] = anchor_ms

            if now_ms - anchor_ms < min_age_ms:
                continue
            qualified.append(
                item.model_copy(update={"onboard_time_ms": anchor_ms})
            )
            if float(item.quote_volume) >= self.settings.min_quote_volume:
                liquid_qualified += 1
            if (
                len(qualified) >= self.settings.surveillance_n
                and liquid_qualified >= self.settings.top_n
                and benchmark_seen
            ):
                break
        return qualified

    async def select(self, client: BinanceRestClient) -> Universe:
        info, rows = await client.exchange_info(), await client.tickers_24h()
        volumes = self._ticker_volumes(rows)
        candidates = self._instruments(client.market, info, volumes)
        ranked = sorted(
            candidates,
            key=lambda item: (-item.quote_volume, item.symbol),
        )
        if client.market is Market.SPOT:
            ranked = await self._spot_age_qualified(client, ranked)
        available = {item.symbol for item in ranked}
        missing_required = sorted(self.required_symbols - available)
        if missing_required:
            raise RequiredUniverseUnavailableError(
                "required futures symbols unavailable: " + ", ".join(missing_required)
            )
        liquid = [
            item
            for item in ranked
            if float(item.quote_volume) >= self.settings.min_quote_volume
        ]
        dynamic_slots = self.settings.top_n - len(self.required_symbols)
        dynamic = [
            item for item in liquid if item.symbol not in self.required_symbols
        ][:dynamic_slots]
        tradable_symbols = self.required_symbols | {item.symbol for item in dynamic}
        tradable = tuple(item for item in ranked if item.symbol in tradable_symbols)
        benchmark = next(
            (item for item in ranked if item.symbol == BENCHMARK_SYMBOL), None
        )
        surveillance = self._surveillance_with_benchmark(
            ranked,
            benchmark,
            self.required_symbols | frozenset(item.symbol for item in tradable),
        )
        context = () if benchmark is None else (benchmark,)
        return Universe(client.market, tradable, surveillance, context)

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
