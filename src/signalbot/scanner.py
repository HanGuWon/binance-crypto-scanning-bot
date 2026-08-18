from __future__ import annotations

import asyncio
import logging
from typing import Any

from signalbot.clock import Clock
from signalbot.config import Settings
from signalbot.data.candles import CandleGap, interval_to_milliseconds
from signalbot.data.funding import FundingRateCapacityError, FundingRatePayloadError
from signalbot.data.raw_events import RawEventCapacityError, RawEventRecorder
from signalbot.domain.enums import Market
from signalbot.domain.models import Candle
from signalbot.exchange.binance.endpoints import build_websocket_plans
from signalbot.exchange.binance.rest import BinanceRestClient, BinanceRestError
from signalbot.exchange.binance.universe import Universe, UniverseSelector
from signalbot.exchange.binance.websocket import WebSocketConsumer
from signalbot.runtime import MarketRuntime

LOGGER = logging.getLogger(__name__)
MAX_GAP_RECOVERY_PAGES = 100


class MarketScanner:
    def __init__(
        self,
        market: Market,
        settings: Settings,
        clock: Clock,
        runtime: MarketRuntime,
        stop_event: asyncio.Event,
        rest_client: BinanceRestClient | None = None,
        raw_recorder: RawEventRecorder | None = None,
    ) -> None:
        self.market = market
        self.settings = settings
        self.clock = clock
        self.runtime = runtime
        self.stop_event = stop_event
        self.rest = rest_client or BinanceRestClient(
            market, settings.binance.request_timeout_seconds
        )
        self.selector = UniverseSelector(settings.binance, clock)
        self.consumer = WebSocketConsumer(settings.binance.max_connection_age_seconds)
        self.universe: Universe | None = None
        self.raw_recorder = None
        if settings.runtime.record_raw_events:
            self.raw_recorder = raw_recorder or RawEventRecorder(
                settings.runtime.raw_event_directory,
                settings.runtime.raw_event_max_bytes,
            )
        runtime.gap_recoverer = self._recover_gap

    async def close(self) -> None:
        await self.rest.close()

    async def prepare(self) -> Universe:
        universe = await self.selector.select(self.rest)
        if not universe.tradable:
            raise RuntimeError(f"no tradable symbols selected for {self.market.value}")
        self.universe = universe
        market_data_symbols = sorted(
            set(universe.tradable_symbols) | set(universe.context_symbols)
        )
        self.runtime.set_active_symbols(
            frozenset(universe.tradable_symbols),
            universe.surveillance_symbols,
            universe.context_symbols,
        )
        if self.market is Market.FUTURES:
            await self._refresh_funding(universe.tradable_symbols, bootstrap=True)
        await self._bootstrap(market_data_symbols)
        LOGGER.info("market scanner prepared", extra={"market": self.market.value})
        return universe

    async def run(self) -> None:
        universe = self.universe or await self.prepare()
        market_data_symbols = sorted(
            set(universe.tradable_symbols) | set(universe.context_symbols)
        )
        plans = build_websocket_plans(
            self.market,
            market_data_symbols,
            self.settings.binance.intervals,
            self.settings.binance.websocket_batch_size,
        )
        tasks = [
            asyncio.create_task(
                self.consumer.consume_forever(plan, self._handle_payload, self.stop_event),
                name=plan.name,
            )
            for plan in plans
        ]
        if self.market is Market.FUTURES:
            tasks.append(
                asyncio.create_task(
                    self._funding_refresh_loop(universe.tradable_symbols),
                    name="futures-funding-refresh",
                )
            )
        try:
            await asyncio.gather(*tasks)
        finally:
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)

    async def _handle_payload(self, payload: Any) -> None:
        if self.raw_recorder is not None:
            try:
                await self.raw_recorder.append(
                    self.market, payload, self.clock.now_ms()
                )
            except RawEventCapacityError as exc:
                LOGGER.critical(
                    "raw market-evidence quota exhausted; stopping scanner",
                    extra={"market": self.market.value},
                    exc_info=exc,
                )
                self.stop_event.set()
                return
        await self.runtime.handle_payload(payload)

    async def _funding_refresh_loop(self, symbols: list[str]) -> None:
        """Refresh settled public funding until cancellation or the shared stop event."""
        while not self.stop_event.is_set():
            try:
                await asyncio.wait_for(
                    self.stop_event.wait(),
                    timeout=self.settings.binance.funding_refresh_seconds,
                )
            except TimeoutError:
                await self._refresh_funding(symbols, bootstrap=False)

    async def _refresh_funding(self, symbols: list[str], *, bootstrap: bool) -> None:
        semaphore = asyncio.Semaphore(self.settings.binance.rest_concurrency)
        now_ms = self.clock.now_ms()

        async def load(symbol: str) -> None:
            latest_ms = self.runtime.funding.latest_time_ms(symbol)
            if not bootstrap and latest_ms is not None and latest_ms >= now_ms:
                return
            start_time_ms = None if bootstrap or latest_ms is None else latest_ms + 1
            end_time_ms = None if bootstrap else now_ms
            try:
                async with semaphore:
                    payloads = await self.rest.funding_rates(
                        symbol,
                        start_time_ms=start_time_ms,
                        end_time_ms=end_time_ms,
                        limit=self.settings.binance.funding_history_points,
                    )
                inserted = self.runtime.funding.ingest_payloads(symbol, payloads)
            except (
                BinanceRestError,
                FundingRateCapacityError,
                FundingRatePayloadError,
            ) as exc:
                LOGGER.warning(
                    "public funding-rate refresh failed; futures crowding remains fail-closed",
                    extra={"market": self.market.value, "symbol": symbol},
                    exc_info=exc,
                )
                return
            LOGGER.debug(
                "public funding-rate history refreshed",
                extra={
                    "market": self.market.value,
                    "symbol": symbol,
                    "inserted": inserted,
                },
            )

        await asyncio.gather(*(load(symbol) for symbol in symbols))

    async def _bootstrap(self, symbols: list[str]) -> None:
        semaphore = asyncio.Semaphore(self.settings.binance.rest_concurrency)

        async def load(symbol: str, interval: str) -> None:
            async with semaphore:
                candles = await self.rest.klines(
                    symbol,
                    interval,
                    self.settings.binance.bootstrap_candles,
                    now_ms=self.clock.now_ms(),
                )
            self.runtime.bootstrap(candles, rebuild=False)
            if self.settings.runtime.persist_candles:
                self.runtime.repository.save_candles(candles)

        await asyncio.gather(
            *(
                load(symbol, interval)
                for symbol in symbols
                for interval in self.settings.binance.intervals
            )
        )
        self.runtime.rebuild_derived_state()

    async def _recover_gap(self, gap: CandleGap) -> list[Candle]:
        step_ms = interval_to_milliseconds(gap.interval)
        expected_count = (gap.end_time_ms - gap.start_time_ms + 1) // step_ms
        page_limit = 1000 if self.market is Market.SPOT else 1500
        required_pages = (expected_count + page_limit - 1) // page_limit
        if required_pages > MAX_GAP_RECOVERY_PAGES:
            LOGGER.error(
                "candle gap exceeds bounded recovery capacity",
                extra={
                    "market": gap.market.value,
                    "symbol": gap.symbol,
                    "required_pages": required_pages,
                },
            )
            return []
        recovered: list[Candle] = []
        cursor_ms = gap.start_time_ms
        for _ in range(required_pages):
            if self.stop_event.is_set():
                return []
            page = await self.rest.klines(
                gap.symbol,
                gap.interval,
                page_limit,
                cursor_ms,
                gap.end_time_ms,
                self.clock.now_ms(),
            )
            if not page:
                break
            recovered.extend(page)
            next_cursor_ms = page[-1].open_time_ms + step_ms
            if next_cursor_ms <= cursor_ms:
                break
            cursor_ms = next_cursor_ms
            if cursor_ms > gap.end_time_ms:
                break
        return recovered
