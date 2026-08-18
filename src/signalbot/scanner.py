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
        self._pending_universe_signature: tuple[frozenset[str], frozenset[str]] | None = None
        self._pending_universe_confirmations = 0
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

    @staticmethod
    def _market_data_symbols(universe: Universe) -> list[str]:
        return sorted(set(universe.tradable_symbols) | set(universe.context_symbols))

    @staticmethod
    def _universe_signature(
        universe: Universe,
    ) -> tuple[frozenset[str], frozenset[str]]:
        return frozenset(universe.tradable_symbols), universe.context_symbols

    def _paper_continuation_symbols(self) -> frozenset[str]:
        paper_positions = getattr(self.runtime, "paper_positions", None)
        if paper_positions is None:
            return frozenset()
        return frozenset(paper_positions.continuation_symbols)

    def _start_websocket_tasks(self, universe: Universe) -> list[asyncio.Task[None]]:
        context_only = universe.context_symbols - frozenset(universe.tradable_symbols)
        plans = build_websocket_plans(
            self.market,
            self._market_data_symbols(universe),
            self.settings.binance.intervals,
            self.settings.binance.websocket_batch_size,
            candle_only_symbols=context_only,
        )
        return [
            asyncio.create_task(
                self.consumer.consume_forever(plan, self._handle_payload, self.stop_event),
                name=plan.name,
            )
            for plan in plans
        ]

    @staticmethod
    async def _cancel_tasks(tasks: list[asyncio.Task[None]]) -> None:
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)

    @staticmethod
    def _raise_unexpected_task_exit(task: asyncio.Task[Any], label: str) -> None:
        if task.cancelled():
            raise asyncio.CancelledError
        error = task.exception()
        if error is not None:
            raise error
        raise RuntimeError(f"{label} exited unexpectedly")

    async def _poll_universe_candidate(self) -> Universe | None:
        current = self.universe
        if current is None:
            return None
        try:
            candidate = await self.selector.select(self.rest)
        except BinanceRestError as exc:
            LOGGER.warning(
                "universe refresh failed; retaining current membership",
                extra={"market": self.market.value},
                exc_info=exc,
            )
            return None
        if not candidate.tradable:
            LOGGER.warning(
                "universe refresh returned no tradable symbols; retaining current membership",
                extra={"market": self.market.value},
            )
            return None

        current_signature = self._universe_signature(current)
        candidate_signature = self._universe_signature(candidate)
        if candidate_signature == current_signature:
            self._pending_universe_signature = None
            self._pending_universe_confirmations = 0
            if candidate.surveillance_symbols != current.surveillance_symbols:
                self.runtime.set_active_symbols(
                    frozenset(candidate.tradable_symbols),
                    candidate.surveillance_symbols,
                    candidate.context_symbols,
                )
                self.universe = candidate
            return None

        outgoing = frozenset(current.tradable_symbols) - frozenset(
            candidate.tradable_symbols
        )
        protected = self._paper_continuation_symbols() & outgoing
        if protected:
            self._pending_universe_signature = None
            self._pending_universe_confirmations = 0
            LOGGER.info(
                "deferring universe rotation while PAPER lifecycle is active",
                extra={
                    "market": self.market.value,
                    "protected_symbols": sorted(protected),
                },
            )
            return None

        if candidate_signature == self._pending_universe_signature:
            self._pending_universe_confirmations += 1
        else:
            self._pending_universe_signature = candidate_signature
            self._pending_universe_confirmations = 1
        required = self.settings.binance.universe_change_confirmations
        LOGGER.info(
            "observed candidate universe change",
            extra={
                "market": self.market.value,
                "confirmations": self._pending_universe_confirmations,
                "required_confirmations": required,
            },
        )
        return candidate if self._pending_universe_confirmations >= required else None

    async def _activate_universe(self, candidate: Universe) -> None:
        previous = self.universe
        previous_market_data = (
            set() if previous is None else set(self._market_data_symbols(previous))
        )
        previous_tradable = (
            set() if previous is None else set(previous.tradable_symbols)
        )
        candidate_market_data = set(self._market_data_symbols(candidate))
        added_market_data = sorted(candidate_market_data - previous_market_data)
        added_tradable = sorted(set(candidate.tradable_symbols) - previous_tradable)

        self.runtime.set_active_symbols(
            frozenset(candidate.tradable_symbols),
            candidate.surveillance_symbols,
            candidate.context_symbols,
        )
        if self.market is Market.FUTURES and added_tradable:
            await self._refresh_funding(added_tradable, bootstrap=True)
        if added_market_data:
            await self._bootstrap(added_market_data)
        self.universe = candidate
        self._pending_universe_signature = None
        self._pending_universe_confirmations = 0
        LOGGER.info(
            "activated confirmed universe rotation",
            extra={
                "market": self.market.value,
                "tradable_symbols": len(candidate.tradable_symbols),
                "surveillance_symbols": len(candidate.surveillance_symbols),
                "added_market_data_symbols": len(added_market_data),
            },
        )

    async def run(self) -> None:
        universe = self.universe or await self.prepare()
        websocket_tasks = self._start_websocket_tasks(universe)
        if self.market is Market.FUTURES:
            funding_task = asyncio.create_task(
                self._funding_refresh_loop(),
                name="futures-funding-refresh",
            )
        else:
            funding_task = None
        stop_waiter = asyncio.create_task(
            self.stop_event.wait(), name=f"{self.market.value}-scanner-stop"
        )
        refresh_task = asyncio.create_task(
            asyncio.sleep(self.settings.binance.universe_refresh_seconds),
            name=f"{self.market.value}-universe-refresh",
        )
        try:
            while not self.stop_event.is_set():
                monitored: set[asyncio.Task[Any]] = {
                    *websocket_tasks,
                    stop_waiter,
                    refresh_task,
                }
                if funding_task is not None:
                    monitored.add(funding_task)
                done, _ = await asyncio.wait(
                    monitored,
                    return_when=asyncio.FIRST_COMPLETED,
                )
                if stop_waiter in done:
                    break
                if funding_task is not None and funding_task in done:
                    self._raise_unexpected_task_exit(
                        funding_task, "funding refresh loop"
                    )
                completed_websockets = [
                    task for task in websocket_tasks if task in done
                ]
                for task in completed_websockets:
                    self._raise_unexpected_task_exit(
                        task, "Binance WebSocket consumer"
                    )
                if refresh_task in done:
                    replacement = await self._poll_universe_candidate()
                    if replacement is not None:
                        await self._cancel_tasks(websocket_tasks)
                        await self._activate_universe(replacement)
                        universe = self.universe or replacement
                        websocket_tasks = self._start_websocket_tasks(universe)
                    refresh_task = asyncio.create_task(
                        asyncio.sleep(self.settings.binance.universe_refresh_seconds),
                        name=f"{self.market.value}-universe-refresh",
                    )
        finally:
            stop_waiter.cancel()
            refresh_task.cancel()
            await self._cancel_tasks(websocket_tasks)
            if funding_task is not None:
                funding_task.cancel()
            await asyncio.gather(
                stop_waiter,
                refresh_task,
                *(() if funding_task is None else (funding_task,)),
                return_exceptions=True,
            )

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

    async def _funding_refresh_loop(self) -> None:
        """Refresh settled public funding until cancellation or the shared stop event."""
        while not self.stop_event.is_set():
            try:
                await asyncio.wait_for(
                    self.stop_event.wait(),
                    timeout=self.settings.binance.funding_refresh_seconds,
                )
            except TimeoutError:
                universe = self.universe
                if universe is not None:
                    await self._refresh_funding(universe.tradable_symbols, bootstrap=False)

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
