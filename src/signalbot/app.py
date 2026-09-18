from __future__ import annotations

import asyncio
import logging
import signal
from typing import Any

from signalbot.alerts.discord import DiscordNotifier
from signalbot.clock import SystemClock
from signalbot.config import Settings
from signalbot.data.raw_events import RawEventRecorder
from signalbot.domain.models import SignalDecision
from signalbot.persistence.repository import SqlRepository
from signalbot.runtime import MarketRuntime
from signalbot.scanner import MarketScanner

LOGGER = logging.getLogger(__name__)

_MONITOR_STOP_TIMEOUT_SECONDS = 5.0
_GRACEFUL_DRAIN_TIMEOUT_SECONDS = 30.0
_RESOURCE_CLOSE_TIMEOUT_SECONDS = 15.0


class SignalApplication:
    def __init__(self, settings: Settings, *, stop_after_minutes: int | None = None) -> None:
        self.settings = settings
        self.clock = SystemClock()
        self.repository = SqlRepository(settings.storage.url, settings.storage.echo_sql)
        self.stop_event = asyncio.Event()
        self.stop_after_minutes = stop_after_minutes
        self.notifier: DiscordNotifier | None = None
        self.scanners: list[MarketScanner] = []
        if (
            settings.runtime.record_raw_events
            and settings.runtime.storage_mode == "segmented_zstd_v1"
        ):
            from pathlib import Path as _Path

            from signalbot.prospective.segmented_storage import (
                ProspectiveTapeRecorder,
            )

            if not settings.shadow.campaign_id or not settings.shadow.source_identity:
                raise ValueError(
                    "segmented raw recorder requires campaign_id and "
                    "source_identity"
                )
            self.raw_recorder: Any = ProspectiveTapeRecorder(
                _Path(settings.runtime.raw_event_directory),
                campaign_id=settings.shadow.campaign_id,
                source_identity=settings.shadow.source_identity,
                raw_event_max_bytes=settings.runtime.raw_event_max_bytes,
            )
        elif settings.runtime.record_raw_events:
            self.raw_recorder = RawEventRecorder(
                settings.runtime.raw_event_directory,
                settings.runtime.raw_event_max_bytes,
            )
        else:
            self.raw_recorder = None

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
            restore = getattr(runtime, "restore_persisted_state", None)
            if callable(restore):
                restore()
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
        critical_tasks = [
            asyncio.create_task(scanner.run(), name=f"scanner-{scanner.market.value}")
            for scanner in self.scanners
        ]
        if self.raw_recorder is not None:
            critical_tasks.append(
                asyncio.create_task(
                    self._monitor_recorder_failure(), name="raw-recorder-failure"
                )
            )
        auxiliary_tasks: list[asyncio.Task[None]] = []
        if self.stop_after_minutes is not None:
            auxiliary_tasks.append(
                asyncio.create_task(self._bounded_stop_timer(), name="bounded-stop-timer")
            )
        if self.settings.alerts.discord_enabled:
            auxiliary_tasks.append(
                asyncio.create_task(
                    self.notifier.run_dispatch_loop(self.stop_event),
                    name="discord-outbox-drain",
                )
            )
        try:
            # Explicit task ownership (Phase-K supervisor):
            # - critical_tasks must ALL complete; whichever exits first decides.
            # - monitor is stop-aware and returns on normal stop.
            # - a scanner crash surfaces here and triggers fail-closed stop.
            done, pending = await asyncio.wait(
                critical_tasks, return_when=asyncio.FIRST_COMPLETED
            )
            if any(t in done for t in self._scanner_tasks(critical_tasks)):
                # A scanner exited first: crash or unexpected exit. Capture its
                # exception so it is never silently swallowed, then fail closed.
                for scanner_task in self._scanner_tasks(done):
                    error = scanner_task.exception()
                    if error is not None and not isinstance(
                        error, asyncio.CancelledError
                    ):
                        LOGGER.critical(
                            "market scanner exited unexpectedly; failing closed",
                            exc_info=error,
                        )
                self.stop_event.set()
                scanner_error: BaseException | None = None
                for scanner_task in self._scanner_tasks(done):
                    error = scanner_task.exception()
                    if error is not None and not isinstance(
                        error, asyncio.CancelledError
                    ):
                        if scanner_error is None:
                            scanner_error = error
                await self._wait_remaining_gracefully(pending)
                if scanner_error is not None:
                    raise scanner_error
            else:
                # Monitor or another critical task finished first. If it was
                # the recorder-failure monitor with a fatal error, it already
                # set stop_event. Await remaining critical tasks gracefully.
                await self._wait_remaining_gracefully(pending)
        finally:
            self.stop_event.set()
            all_tasks = [*critical_tasks, *auxiliary_tasks]
            for task in all_tasks:
                task.cancel()
            try:
                await asyncio.wait_for(
                    asyncio.gather(*all_tasks, return_exceptions=True),
                    timeout=_GRACEFUL_DRAIN_TIMEOUT_SECONDS,
                )
            except TimeoutError:
                LOGGER.critical(
                    "task teardown gather exceeded deadline; continuing resource "
                    "closes so repository close is still reached"
                )
            for scanner in self.scanners:
                await self._bounded_close(
                    scanner.close(),
                    f"scanner-close-{scanner.market.value}",
                )
            if self.raw_recorder is not None:
                await self._bounded_close(
                    self.raw_recorder.close(), "raw-recorder-close"
                )
            if self.notifier is not None:
                await self._bounded_close(self.notifier.close(), "notifier-close")
            self.repository.close()
        LOGGER.info("signal application stopped")

    @staticmethod
    def _scanner_tasks(tasks) -> list[asyncio.Task]:
        return [t for t in tasks if t.get_name().startswith("scanner-")]

    async def _wait_remaining_gracefully(self, pending) -> None:
        """Give remaining critical tasks a graceful drain window."""
        if not pending:
            return
        try:
            done_pending, _still = await asyncio.wait(
                set(pending), timeout=_GRACEFUL_DRAIN_TIMEOUT_SECONDS
            )
            for finished in done_pending:
                error = finished.exception()
                if error is not None and not isinstance(error, asyncio.CancelledError):
                    LOGGER.error(
                        "critical task raised during graceful drain",
                        extra={"task": finished.get_name()},
                        exc_info=error,
                    )
            still_running = [task for task in pending if not task.done()]
            if still_running:
                raise TimeoutError
        except TimeoutError:
            # Graceful drain expired (e.g., transport stuck in a stalled TLS
            # path). Fail-closed ownership: force-cancel stragglers under a
            # bounded join so shutdown stays finite, with full audit trail.
            LOGGER.critical(
                "graceful drain deadline exceeded during application shutdown; "
                "cancelling remaining critical tasks",
                extra={
                    "task_names": [task.get_name() for task in pending],
                },
            )
            for task in pending:
                task.cancel()
            try:
                await asyncio.wait_for(
                    asyncio.gather(*pending, return_exceptions=True),
                    timeout=_RESOURCE_CLOSE_TIMEOUT_SECONDS,
                )
            except TimeoutError:
                LOGGER.critical(
                    "critical task cancellation exceeded deadline; abandoning",
                    extra={
                        "task_names": [
                            task.get_name()
                            for task in pending
                            if not task.done()
                        ],
                    },
                )

    async def _bounded_close(self, close_coro, component: str) -> None:
        """Await a resource close under a deadline; never block teardown."""
        try:
            await asyncio.wait_for(
                asyncio.shield(close_coro), timeout=_RESOURCE_CLOSE_TIMEOUT_SECONDS
            )
        except TimeoutError:
            LOGGER.critical(
                "resource close exceeded shutdown deadline",
                extra={"component": component},
            )

    async def _monitor_recorder_failure(self) -> None:
        """Fail the whole application closed when the raw recorder dies.

        A dead writer must never surface only as WebSocket reconnect churn:
        one CRITICAL + shared stop_event is the single fail-closed path.
        """
        assert self.raw_recorder is not None
        # Stop-aware race: return cleanly when normal stop wins (healthy
        # recorder), otherwise escalate the fatal to a fail-closed stop.
        failed_task = asyncio.create_task(
            self.raw_recorder.wait_failed(), name="recorder-wait-failed"
        )
        stop_task = asyncio.create_task(self.stop_event.wait(), name="monitor-stop")
        try:
            done, _pending = await asyncio.wait(
                {failed_task, stop_task}, return_when=asyncio.FIRST_COMPLETED
            )
            if failed_task in done:
                error = self.raw_recorder.fatal_error
                LOGGER.critical(
                    "raw-event recorder fatal; requesting graceful stop",
                    extra={"fatal_error": str(error)},
                )
                self.stop_event.set()
        finally:
            for pending_task in (failed_task, stop_task):
                if not pending_task.done():
                    pending_task.cancel()
            await asyncio.gather(failed_task, stop_task, return_exceptions=True)

    async def _bounded_stop_timer(self) -> None:
        """Operational-only bounded run: trigger the same graceful shutdown path."""

        stop_delay_seconds = (self.stop_after_minutes or 0) * 60
        await asyncio.sleep(stop_delay_seconds)
        LOGGER.info(
            "bounded run timer elapsed; requesting graceful stop",
            extra={"stop_after_minutes": self.stop_after_minutes},
        )
        self.stop_event.set()
