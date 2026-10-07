"""Disabled-by-default local public capture composition for Pump-fade v2.

This module owns no scanner, Discord, PAPER or order path.  It composes the
existing public Binance capture primitives only when an operator explicitly
constructs the runtime with ``enabled=True``.  Unit tests use fake REST capture
adapters; the module makes no network calls at import or construction time.
"""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import math
import sqlite3
import time
import uuid
from collections.abc import Awaitable, Callable, Sequence
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Protocol

from signalbot.capture.depth_sequence import DepthRangeObservation, DepthResyncRequest
from signalbot.capture.handoff import BoundedCaptureHandoff, CaptureFatalState
from signalbot.capture.models import RestEnvelopeV2
from signalbot.capture.pipeline import CapturePipeline
from signalbot.capture.receipts import IngestSequencer, SystemReceiptClock
from signalbot.capture.rest import PublicRestCaptureAdapter
from signalbot.capture.storage import SegmentedCaptureWriter
from signalbot.capture.ws_owner import PublicWebSocketCaptureOwner, WebSocketOwnerSettings
from signalbot.domain.enums import Market
from signalbot.exchange.binance.endpoints import FUTURES_REST_BASE
from signalbot.pump_fade_v2.public_capture import (
    PumpRestPoll,
    pump_public_plans,
    pump_rest_poll_plan,
)


class RestAttemptCapture(Protocol):
    async def capture_attempt(
        self,
        *,
        method: str,
        market: Market,
        url: str,
        request_role: str,
        correlation_id: str,
        attempt: int,
        query: tuple[tuple[str, str], ...],
    ) -> RestEnvelopeV2: ...


Sleep = Callable[[float], Awaitable[None]]


@dataclass(frozen=True, slots=True)
class PumpCaptureSettings:
    """Resource and retry limits for the isolated research collector."""

    enabled: bool = False
    max_symbols: int = 24
    handoff_events: int = 4096
    handoff_bytes: int = 32 * 1024 * 1024
    maximum_total_bytes: int = 8 * 1024 * 1024 * 1024
    emergency_reserve_bytes: int = 256 * 1024 * 1024
    segment_rotation_ms: int = 60_000
    rest_timeout_seconds: float = 15.0
    rest_max_retry_after_seconds: float = 30.0
    smoke_min_seconds: int = 10
    smoke_max_seconds: int = 300

    def __post_init__(self) -> None:
        if not 1 <= self.max_symbols <= 24:
            raise ValueError("max_symbols must be between 1 and 24")
        if self.handoff_events < 2 or self.handoff_bytes < 1024:
            raise ValueError("capture handoff limits are too small")
        if self.maximum_total_bytes <= self.emergency_reserve_bytes:
            raise ValueError("capture total bytes must exceed emergency reserve")
        if not 1_000 <= self.segment_rotation_ms <= 300_000:
            raise ValueError("segment rotation must be between 1s and 5m")
        if not 0 < self.rest_timeout_seconds <= 60:
            raise ValueError("REST timeout must be within 60 seconds")
        if not 0 <= self.rest_max_retry_after_seconds <= 60:
            raise ValueError("REST retry-after bound must be within 60 seconds")
        if not 1 <= self.smoke_min_seconds <= self.smoke_max_seconds <= 300:
            raise ValueError("smoke duration bounds must be within 1..300 seconds")


def pump_capture_plan_sha256(symbols: tuple[str, ...], policy_sha256: str) -> str:
    """Bind admitted symbols, public routes and REST schedule to one plan identity."""

    if len(policy_sha256) != 64 or any(c not in "0123456789abcdef" for c in policy_sha256):
        raise ValueError("policy_sha256 must be lowercase SHA-256")
    websocket = tuple(
        {"name": p.name, "market": p.market.value, "route": p.route,
         "streams": p.streams, "url": p.url}
        for p in pump_public_plans(symbols)
    )
    rest = tuple(asdict(entry) for entry in pump_rest_poll_plan(symbols))
    material = json.dumps(
        {"policy_sha256": policy_sha256, "symbols": symbols,
         "websocket": websocket, "rest": rest},
        sort_keys=True, separators=(",", ":"), ensure_ascii=True,
    ).encode()
    return hashlib.sha256(material).hexdigest()


def _retry_after_seconds(envelope: RestEnvelopeV2, maximum: float) -> float:
    values = [value for name, value in envelope.response_headers if name == "retry-after"]
    if not values:
        return 1.0
    try:
        value = float(values[-1])
    except ValueError:
        return 1.0
    if not math.isfinite(value) or value < 0:
        return 1.0
    return min(value, maximum)


def _retryable(envelope: RestEnvelopeV2) -> bool:
    status = envelope.response_status
    # 418 is an IP-ban response and must not be hammered with an automatic retry.
    return status == 429 or (status is not None and status >= 500) or status is None


def next_utc_5m_poll_ms(now_ms: int, *, delay_ms: int = 1_000) -> int:
    """Return the first post-close UTC 5m quality-snapshot clock after ``now_ms``."""

    if now_ms < 0 or not 0 <= delay_ms <= 10_000:
        raise ValueError("invalid UTC 5m poll clock")
    step = 5 * 60_000
    boundary = ((now_ms // step) + 1) * step
    return boundary + delay_ms


class PumpRestScheduler:
    """Serial, bounded polling for admitted symbols with captured retry attempts."""

    def __init__(
        self,
        plan: tuple[PumpRestPoll, ...],
        adapter: RestAttemptCapture,
        *,
        maximum_retry_after_seconds: float = 30.0,
        sleep: Sleep = asyncio.sleep,
    ) -> None:
        if not plan:
            raise ValueError("pump REST schedule cannot be empty")
        if not 0 <= maximum_retry_after_seconds <= 60:
            raise ValueError("maximum_retry_after_seconds must be within 60 seconds")
        self.plan = plan
        self.adapter = adapter
        self.maximum_retry_after_seconds = maximum_retry_after_seconds
        self._sleep = sleep
        self._by_role = {entry.role: entry for entry in plan}
        if len(self._by_role) != len(plan):
            raise ValueError("pump REST schedule roles must be unique")
        self._depth_events: asyncio.Queue[DepthResyncRequest] = asyncio.Queue(maxsize=64)
        self._latest_depth_range: dict[str, tuple[int, int, int]] = {}

    def notify_depth_resync(self, request: DepthResyncRequest) -> None:
        """Queue one bounded USD-M snapshot request after startup/reconnect/gap."""

        if request.market is not Market.FUTURES:
            raise ValueError("Pump-fade v2 depth capture accepts USD-M Futures only")
        for symbol, _ in request.watermarks:
            if f"depth_{symbol}" not in self._by_role:
                raise ValueError("depth resync symbol is outside admitted Pump-fade symbols")
        self._depth_events.put_nowait(request)

    def notify_depth_range(self, observation: DepthRangeObservation) -> None:
        """Retain one bounded latest range per admitted symbol for diagnostics."""

        if observation.market is not Market.FUTURES:
            raise ValueError("Pump-fade v2 depth capture accepts USD-M Futures only")
        if f"depth_{observation.symbol}" not in self._by_role:
            raise ValueError("depth range symbol is outside admitted Pump-fade symbols")
        self._latest_depth_range[observation.symbol] = (
            observation.generation, observation.U, observation.u
        )

    async def capture_entry(
        self, entry: PumpRestPoll, *, scheduled_at_ms: int,
        stop_event: asyncio.Event | None = None,
    ) -> RestEnvelopeV2:
        """Capture one scheduled request, preserving every bounded retry as evidence."""

        if entry not in self.plan:
            raise ValueError("REST entry is outside the frozen runtime schedule")
        correlation = hashlib.sha256(
            f"pump-v2|{entry.role}|{scheduled_at_ms}|{entry.query}".encode()
        ).hexdigest()
        last: RestEnvelopeV2 | None = None
        for attempt in range(1, entry.maximum_attempts + 1):
            if stop_event is not None and stop_event.is_set():
                raise asyncio.CancelledError
            last = await self.adapter.capture_attempt(
                method="GET", market=Market.FUTURES,
                url=FUTURES_REST_BASE + entry.path,
                request_role=entry.role, correlation_id=correlation,
                attempt=attempt, query=entry.query,
            )
            if (last.response_status is not None
                    and 200 <= last.response_status < 300
                    and last.payload_complete):
                return last
            if attempt >= entry.maximum_attempts or not _retryable(last):
                return last
            delay = _retry_after_seconds(last, self.maximum_retry_after_seconds)
            if stop_event is None:
                await self._sleep(delay)
            else:
                try:
                    await asyncio.wait_for(stop_event.wait(), timeout=delay)
                except TimeoutError:
                    pass
                else:
                    raise asyncio.CancelledError
        assert last is not None
        return last

    async def _poll_loop(self, stop_event: asyncio.Event) -> None:
        interval_plan = tuple(entry for entry in self.plan if entry.trigger == "interval")
        due = {entry.role: 0.0 for entry in interval_plan}
        while not stop_event.is_set():
            now = time.monotonic()
            ready = [entry for entry in interval_plan if due[entry.role] <= now]
            if not ready:
                if not due:
                    await stop_event.wait()
                    return
                wait = min(due.values()) - now
                try:
                    await asyncio.wait_for(stop_event.wait(), timeout=max(0.001, wait))
                except TimeoutError:
                    continue
                return
            for entry in ready:
                if stop_event.is_set():
                    return
                scheduled_at_ms = time.time_ns() // 1_000_000
                await self.capture_entry(entry, scheduled_at_ms=scheduled_at_ms,
                                         stop_event=stop_event)
                due[entry.role] = time.monotonic() + entry.min_interval_seconds

    async def _depth_loop(self, stop_event: asyncio.Event) -> None:
        while not stop_event.is_set():
            request_task = asyncio.create_task(self._depth_events.get())
            stop_task = asyncio.create_task(stop_event.wait())
            done, pending = await asyncio.wait(
                {request_task, stop_task}, return_when=asyncio.FIRST_COMPLETED
            )
            for task in pending:
                task.cancel()
            if request_task in pending:
                try:
                    await request_task
                except asyncio.CancelledError:
                    pass
            if stop_task in pending:
                try:
                    await stop_task
                except asyncio.CancelledError:
                    pass
            if stop_task in done:
                if request_task in done:
                    self._depth_events.task_done()
                return
            request = request_task.result()
            try:
                for symbol, _ in request.watermarks:
                    entry = self._by_role[f"depth_{symbol}"]
                    await self.capture_entry(
                        entry,
                        scheduled_at_ms=time.time_ns() // 1_000_000,
                        stop_event=stop_event,
                    )
            finally:
                self._depth_events.task_done()

    async def _bootstrap(self, stop_event: asyncio.Event) -> None:
        """Capture each one-shot 24h kline bootstrap exactly once per process generation."""

        for entry in (item for item in self.plan if item.trigger == "bootstrap"):
            if stop_event.is_set():
                return
            await self.capture_entry(
                entry,
                scheduled_at_ms=time.time_ns() // 1_000_000,
                stop_event=stop_event,
            )

    async def _utc_5m_loop(self, stop_event: asyncio.Event) -> None:
        """Take bounded depth-quality snapshots just after each closed 5m boundary."""

        quality = tuple(entry for entry in self.plan if entry.trigger == "utc_5m")
        while quality and not stop_event.is_set():
            now_ms = time.time_ns() // 1_000_000
            due_ms = next_utc_5m_poll_ms(now_ms)
            wait_seconds = max(0.001, (due_ms - now_ms) / 1_000)
            try:
                await asyncio.wait_for(stop_event.wait(), timeout=wait_seconds)
                return
            except TimeoutError:
                pass
            scheduled_at_ms = due_ms
            for entry in quality:
                if stop_event.is_set():
                    return
                await self.capture_entry(
                    entry,
                    scheduled_at_ms=scheduled_at_ms,
                    stop_event=stop_event,
                )

    async def run(self, stop_event: asyncio.Event) -> None:
        """Run bounded interval polling plus event-triggered raw depth snapshots."""

        await self._bootstrap(stop_event)
        if stop_event.is_set():
            return
        poll_task = asyncio.create_task(self._poll_loop(stop_event))
        depth_task = asyncio.create_task(self._depth_loop(stop_event))
        quality_task = asyncio.create_task(self._utc_5m_loop(stop_event))
        stop_task = asyncio.create_task(stop_event.wait())
        tasks = {poll_task, depth_task, quality_task, stop_task}
        try:
            done, _ = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
            for task in done:
                if task is not stop_task:
                    await task
        finally:
            for task in tasks:
                if not task.done():
                    task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)


@dataclass(frozen=True, slots=True)
class PumpCaptureReceipt:
    plan_sha256: str
    policy_sha256: str
    symbols: tuple[str, ...]
    process_boot_id: str
    started_at_ms: int
    stopped_at_ms: int
    stop_reason: str
    fatal: bool
    schema_version: str = "pump_fade_v2_capture_receipt_v1"


@dataclass(frozen=True, slots=True)
class PumpCaptureGeneration:
    generation: int
    observed_at_ms: int
    policy_sha256: str
    plan_sha256: str
    symbols: tuple[str, ...]


class PumpCapturePlanLedger:
    """Durable immutable generations for changes to the admitted symbol set.

    The ledger never edits a prior generation. A runtime owner may stop one
    foreground generation and start the next; no in-place subscription mutation
    or production service registration is performed here.
    """

    def __init__(self, path: str | Path, *, maximum_generations: int = 1_024) -> None:
        if not 1 <= maximum_generations <= 100_000:
            raise ValueError("generation bound is invalid")
        self.path = Path(path)
        self.maximum_generations = maximum_generations
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with sqlite3.connect(self.path) as conn:
            conn.execute("""CREATE TABLE IF NOT EXISTS pump_v2_capture_generation (
                generation INTEGER PRIMARY KEY,
                observed_at_ms INTEGER NOT NULL,
                policy_sha256 TEXT NOT NULL,
                plan_sha256 TEXT NOT NULL UNIQUE,
                symbols_json TEXT NOT NULL
            )""")

    def register(
        self,
        symbols: tuple[str, ...],
        *,
        policy_sha256: str,
        observed_at_ms: int,
    ) -> tuple[PumpCaptureGeneration, bool]:
        if observed_at_ms < 0:
            raise ValueError("generation observation time must be nonnegative")
        plan_sha = pump_capture_plan_sha256(symbols, policy_sha256)
        symbols_json = json.dumps(symbols, separators=(",", ":"))
        with sqlite3.connect(self.path) as conn:
            conn.execute("BEGIN IMMEDIATE")
            latest = conn.execute(
                "SELECT generation,observed_at_ms,policy_sha256,plan_sha256,symbols_json "
                "FROM pump_v2_capture_generation ORDER BY generation DESC LIMIT 1"
            ).fetchone()
            if latest is not None and latest[2] == policy_sha256 and latest[4] == symbols_json:
                return self._row(latest), False
            count = conn.execute(
                "SELECT COUNT(*) FROM pump_v2_capture_generation"
            ).fetchone()
            assert count is not None
            if int(count[0]) >= self.maximum_generations:
                raise OverflowError("capture generation ledger reached its bound")
            generation = 1 if latest is None else int(latest[0]) + 1
            conn.execute(
                "INSERT INTO pump_v2_capture_generation "
                "(generation,observed_at_ms,policy_sha256,plan_sha256,symbols_json) "
                "VALUES (?,?,?,?,?)",
                (generation, observed_at_ms, policy_sha256, plan_sha, symbols_json),
            )
        return PumpCaptureGeneration(
            generation, observed_at_ms, policy_sha256, plan_sha, symbols
        ), True

    def latest(self) -> PumpCaptureGeneration | None:
        with sqlite3.connect(self.path) as conn:
            row = conn.execute(
                "SELECT generation,observed_at_ms,policy_sha256,plan_sha256,symbols_json "
                "FROM pump_v2_capture_generation ORDER BY generation DESC LIMIT 1"
            ).fetchone()
        return None if row is None else self._row(row)

    @staticmethod
    def _row(row: tuple[object, ...]) -> PumpCaptureGeneration:
        symbols_raw = json.loads(str(row[4]))
        if not isinstance(symbols_raw, list) or not all(
            isinstance(item, str) for item in symbols_raw
        ):
            raise ValueError("capture generation ledger contains invalid symbols")
        return PumpCaptureGeneration(
            generation=int(str(row[0])),
            observed_at_ms=int(str(row[1])),
            policy_sha256=str(row[2]),
            plan_sha256=str(row[3]),
            symbols=tuple(symbols_raw),
        )


class PumpCaptureRuntime:
    """Explicit foreground composition of existing public-only capture owners."""

    def __init__(
        self,
        *,
        symbols: tuple[str, ...],
        policy_sha256: str,
        output_directory: str | Path,
        settings: PumpCaptureSettings = PumpCaptureSettings(),
    ) -> None:
        if not symbols or len(symbols) > settings.max_symbols:
            raise ValueError("runtime symbol set exceeds configured bound")
        self.symbols = symbols
        self.policy_sha256 = policy_sha256
        self.output_directory = Path(output_directory)
        self.settings = settings
        self.plan_sha256 = pump_capture_plan_sha256(symbols, policy_sha256)

    async def run_smoke(self, *, duration_seconds: int) -> PumpCaptureReceipt:
        """Run an explicitly enabled, bounded foreground public-data smoke capture."""

        if not self.settings.enabled:
            raise RuntimeError("Pump-fade v2 capture is disabled by configuration")
        if not (
            self.settings.smoke_min_seconds
            <= duration_seconds
            <= self.settings.smoke_max_seconds
        ):
            raise ValueError("smoke duration is outside configured bounds")
        self.output_directory.mkdir(parents=True, exist_ok=True)
        if any(self.output_directory.iterdir()):
            raise ValueError("smoke output directory must be empty")

        process_boot_id = uuid.uuid4().hex
        fatal = CaptureFatalState()
        handoff = BoundedCaptureHandoff(
            max_events=self.settings.handoff_events,
            max_bytes=self.settings.handoff_bytes,
            fatal_state=fatal,
        )
        writer = SegmentedCaptureWriter(
            self.output_directory,
            plan_sha256=self.plan_sha256,
            process_boot_id=process_boot_id,
            rotation_interval_ms=self.settings.segment_rotation_ms,
            maximum_total_bytes=self.settings.maximum_total_bytes,
            emergency_reserve_bytes=self.settings.emergency_reserve_bytes,
        )
        pipeline = CapturePipeline(handoff, writer)
        clock = SystemReceiptClock()
        sequencer = IngestSequencer()
        owner_settings = WebSocketOwnerSettings(
            maximum_connection_age_seconds=23 * 3600,
            connect_timeout_seconds=15,
            close_timeout_seconds=10,
            heartbeat_interval_seconds=120,
            pong_timeout_seconds=30,
            internal_queue_frames=128,
            maximum_frame_bytes=2 * 1024 * 1024,
            maximum_reconnect_attempts=8,
            reconnect_delays_seconds=(1, 2, 4, 8, 16, 30, 30, 30),
        )
        owners = tuple(
            PublicWebSocketCaptureOwner(
                plan,
                plan_sha256=self.plan_sha256,
                process_boot_id=process_boot_id,
                pipeline=pipeline,
                clock=clock,
                sequencer=sequencer,
                settings=owner_settings,
            )
            for plan in pump_public_plans(self.symbols)
        )
        started_at_ms = time.time_ns() // 1_000_000
        stop_reason = "duration_complete"
        pipeline.start()
        tasks: list[asyncio.Task[None]] = []
        try:
            async with PublicRestCaptureAdapter(
                plan_sha256=self.plan_sha256,
                process_boot_id=process_boot_id,
                pipeline=pipeline,
                clock=clock,
                sequencer=sequencer,
                timeout_seconds=self.settings.rest_timeout_seconds,
            ) as rest:
                scheduler = PumpRestScheduler(
                    pump_rest_poll_plan(self.symbols), rest,
                    maximum_retry_after_seconds=self.settings.rest_max_retry_after_seconds,
                )
                tasks = [asyncio.create_task(owner.run(fatal.stop_event)) for owner in owners]
                for owner in owners:
                    owner.depth_resync_callback = scheduler.notify_depth_resync
                    owner.depth_range_callback = scheduler.notify_depth_range
                tasks.append(asyncio.create_task(scheduler.run(fatal.stop_event)))
                try:
                    await asyncio.wait_for(fatal.stop_event.wait(), timeout=duration_seconds)
                    if fatal.failed:
                        stop_reason = "capture_failure"
                except TimeoutError:
                    fatal.stop_event.set()
                results = await asyncio.gather(*tasks, return_exceptions=True)
                for result in results:
                    if isinstance(result, BaseException) and not isinstance(
                        result, asyncio.CancelledError
                    ):
                        if not fatal.failed:
                            fatal.trip_unbound(result)
                        stop_reason = "capture_failure"
        finally:
            fatal.stop_event.set()
            for task in tasks:
                if not task.done():
                    task.cancel()
            if tasks:
                await asyncio.gather(*tasks, return_exceptions=True)
            try:
                await pipeline.stop()
            except Exception as exc:
                stop_reason = "capture_failure"
                fatal.trip_unbound(exc)
        receipt = PumpCaptureReceipt(
            plan_sha256=self.plan_sha256,
            policy_sha256=self.policy_sha256,
            symbols=self.symbols,
            process_boot_id=process_boot_id,
            started_at_ms=started_at_ms,
            stopped_at_ms=time.time_ns() // 1_000_000,
            stop_reason=stop_reason,
            fatal=fatal.failed,
        )
        receipt_path = self.output_directory / "pump-v2-capture-receipt.json"
        receipt_path.write_text(json.dumps(asdict(receipt), indent=2, sort_keys=True) + "\n",
                                encoding="utf-8")
        if fatal.failed:
            fatal.raise_if_failed()
        return receipt


def main(argv: Sequence[str] | None = None) -> None:
    """Run one explicitly enabled, bounded public-data smoke capture."""

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--symbols", required=True,
                        help="comma-separated normalized USD-M USDT symbols")
    parser.add_argument("--policy-file", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--duration-seconds", type=int, default=10)
    parser.add_argument("--enable", action="store_true",
                        help="explicitly authorize this foreground public-data smoke run")
    args = parser.parse_args(argv)
    policy_path = args.policy_file.resolve(strict=True)
    policy_sha256 = hashlib.sha256(policy_path.read_bytes()).hexdigest()
    symbols = tuple(part.strip().upper() for part in args.symbols.split(",") if part.strip())
    runtime = PumpCaptureRuntime(
        symbols=symbols,
        policy_sha256=policy_sha256,
        output_directory=args.output_dir,
        settings=PumpCaptureSettings(enabled=args.enable),
    )
    receipt = asyncio.run(runtime.run_smoke(duration_seconds=args.duration_seconds))
    print(json.dumps(asdict(receipt), indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
