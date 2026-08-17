from __future__ import annotations

import asyncio
import logging
import signal

from signalbot.alerts.discord import DiscordNotifier
from signalbot.clock import SystemClock
from signalbot.config import Settings
from signalbot.data.raw_events import RawEventRecorder
from signalbot.domain.models import SignalDecision
from signalbot.persistence.repository import SqlRepository
from signalbot.runtime import MarketRuntime
from signalbot.scanner import MarketScanner

LOGGER = logging.getLogger(__name__)


class SignalApplication:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.clock = SystemClock()
        self.repository = SqlRepository(settings.storage.url, settings.storage.echo_sql)
        self.stop_event = asyncio.Event()
        self.notifier: DiscordNotifier | None = None
        self.scanners: list[MarketScanner] = []
        self.raw_recorder = (
            RawEventRecorder(
                settings.runtime.raw_event_directory,
                settings.runtime.raw_event_max_bytes,
            )
            if settings.runtime.record_raw_events
            else None
        )

    @staticmethod
    async def _after_decision_persisted(decision: SignalDecision) -> object:
        """Keep provider I/O out of the market-ingestion coroutine.

        ``MarketRuntime`` already commits the immutable signal and outbox intent
        before invoking this callback. Discord delivery is therefore owned
        exclusively by the independent outbox worker below; a slow webhook,
        rate limit, or ambiguous provider response cannot stall WebSocket event
        processing.
        """

        LOGGER.debug(
            "signal and alert intent persisted",
            extra={"event_id": decision.event_id, "symbol": decision.symbol},
        )
        return None

    async def run(self) -> None:
        self.repository.initialize()
        self.notifier = DiscordNotifier(self.settings.alerts, self.repository, self.clock)
        uncertain_count = self.notifier.recover_inflight()
        if uncertain_count:
            LOGGER.warning(
                "quarantined interrupted Discord deliveries",
                extra={"uncertain_delivery_count": uncertain_count},
            )
        await self.notifier.dispatch_pending()
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(sig, self.stop_event.set)
            except NotImplementedError:
                pass
        for market in self.settings.binance.markets:
            runtime = MarketRuntime(
                market,
                self.settings,
                self.repository,
                self.clock,
                self._after_decision_persisted,
            )
            self.scanners.append(
                MarketScanner(
                    market,
                    self.settings,
                    self.clock,
                    runtime,
                    self.stop_event,
                    raw_recorder=self.raw_recorder,
                )
            )
        tasks = [
            asyncio.create_task(scanner.run(), name=f"scanner-{scanner.market.value}")
            for scanner in self.scanners
        ]
        if self.settings.alerts.discord_enabled:
            tasks.append(
                asyncio.create_task(
                    self.notifier.run_dispatch_loop(self.stop_event),
                    name="discord-outbox-drain",
                )
            )
        try:
            await asyncio.gather(*tasks)
        finally:
            self.stop_event.set()
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            for scanner in self.scanners:
                await scanner.close()
            if self.notifier is not None:
                await self.notifier.close()
            self.repository.close()
            LOGGER.info("signal application stopped")
