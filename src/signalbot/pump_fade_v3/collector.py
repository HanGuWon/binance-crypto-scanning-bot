from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import logging
import os
import time
import uuid
from collections import OrderedDict
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.parse import quote

from signalbot.capture.websocket import validate_public_websocket_plan
from signalbot.domain.enums import Market
from signalbot.exchange.binance.endpoints import (
    FUTURES_WS_MARKET,
    FUTURES_WS_PUBLIC,
    WebSocketPlan,
)
from signalbot.pump_fade_v3.config import (
    CollectorConfig,
    parse_ticker_event,
    validate_usdt_symbol,
)

LOGGER = logging.getLogger(__name__)
_PLAN_BATCH_SIZE = 24


@dataclass(frozen=True, slots=True)
class EventSymbolUpdate:
    active_symbols: tuple[str, ...]
    evicted_symbol: str | None


class EventSymbolSet:
    """Bounded LRU of qualifying USDⓈ-M pump events with a 26h hard lifetime."""

    def __init__(self, config: CollectorConfig) -> None:
        self.maximum_symbols = config.maximum_event_symbols
        self.event_lifetime_ms = config.event_lifetime_ms
        self.return_threshold_percent = config.event_return_percent
        self.minimum_quote_volume_usdt = config.event_minimum_quote_volume_usdt
        self._last_qualifying_event_ms: OrderedDict[str, int] = OrderedDict()
        self._await_reset: OrderedDict[str, None] = OrderedDict()

    def observe(self, ticker: object, *, now_ms: int) -> EventSymbolUpdate:
        if type(now_ms) is not int or now_ms < 0:
            raise ValueError("now_ms must be non-negative Unix milliseconds")
        self._expire(now_ms)
        if not isinstance(ticker, dict) or (
            "st" in ticker and (type(ticker["st"]) is not int or ticker["st"] != 1)
        ):
            return EventSymbolUpdate(tuple(self._last_qualifying_event_ms), None)
        symbol, change_percent, quote_volume, event_ms = parse_ticker_event(ticker)
        qualifies = (
            change_percent >= self.return_threshold_percent
            and quote_volume >= self.minimum_quote_volume_usdt
        )
        if not qualifies:
            self._await_reset.pop(symbol, None)
        elif symbol not in self._await_reset:
            if symbol not in self._last_qualifying_event_ms:
                self._last_qualifying_event_ms[symbol] = min(event_ms, now_ms)
            else:
                self._last_qualifying_event_ms.move_to_end(symbol)
        evicted: str | None = None
        if len(self._last_qualifying_event_ms) > self.maximum_symbols:
            evicted, _ = self._last_qualifying_event_ms.popitem(last=False)
            self._remember_reset(evicted)
            LOGGER.warning("pump-fade v3 event symbol evicted by LRU: %s", evicted)
        return EventSymbolUpdate(tuple(self._last_qualifying_event_ms), evicted)

    def active_symbols(self, *, now_ms: int) -> tuple[str, ...]:
        if type(now_ms) is not int or now_ms < 0:
            raise ValueError("now_ms must be non-negative Unix milliseconds")
        self._expire(now_ms)
        return tuple(self._last_qualifying_event_ms)

    def _expire(self, now_ms: int) -> None:
        expired = tuple(
            symbol
            for symbol, observed_ms in self._last_qualifying_event_ms.items()
            if now_ms - observed_ms >= self.event_lifetime_ms
        )
        for symbol in expired:
            del self._last_qualifying_event_ms[symbol]
            self._remember_reset(symbol)

    def _remember_reset(self, symbol: str) -> None:
        self._await_reset[symbol] = None
        self._await_reset.move_to_end(symbol)
        while len(self._await_reset) > 1024:
            self._await_reset.popitem(last=False)


def build_websocket_plans(
    symbols: tuple[str, ...], *, include_event_depth: bool
) -> tuple[WebSocketPlan, ...]:
    """Build routed public-only v3 plans for globals and bounded event symbols."""

    if len(symbols) > 25 or len(symbols) != len(set(symbols)):
        raise ValueError("event plan requires at most 25 distinct symbols")
    if type(include_event_depth) is not bool:
        raise TypeError("include_event_depth must be a boolean")
    market_streams = ["!forceOrder@arr", "!miniTicker@arr"]
    public_streams: list[str] = []
    for symbol in symbols:
        validate_usdt_symbol(symbol)
        normalized = symbol.lower()
        market_streams.append(f"{normalized}@markPrice@1s")
        if include_event_depth:
            public_streams.append(f"{normalized}@depth5@500ms")

    plans = [
        WebSocketPlan(
            name="pump-fade-v3-market-1",
            market=Market.FUTURES,
            route="market",
            streams=("!forceOrder@arr", "!miniTicker@arr"),
            url=FUTURES_WS_MARKET + "/".join(
                quote(stream, safe="@!_-")
                for stream in ("!forceOrder@arr", "!miniTicker@arr")
            ),
        )
    ]
    event_market = market_streams[2:]
    for start in range(0, len(event_market), _PLAN_BATCH_SIZE):
        chunk = tuple(event_market[start : start + _PLAN_BATCH_SIZE])
        plans.append(
            WebSocketPlan(
                name=f"pump-fade-v3-event-market-{start // _PLAN_BATCH_SIZE + 1}",
                market=Market.FUTURES,
                route="market",
                streams=chunk,
                url=FUTURES_WS_MARKET
                + "/".join(quote(stream, safe="@!_-") for stream in chunk),
            )
        )
    for start in range(0, len(public_streams), _PLAN_BATCH_SIZE):
        chunk = tuple(public_streams[start : start + _PLAN_BATCH_SIZE])
        plans.append(
            WebSocketPlan(
                name=f"pump-fade-v3-event-public-{start // _PLAN_BATCH_SIZE + 1}",
                market=Market.FUTURES,
                route="public",
                streams=chunk,
                url=FUTURES_WS_PUBLIC
                + "/".join(quote(stream, safe="@!_-") for stream in chunk),
            )
        )
    for plan in plans:
        validate_public_websocket_plan(plan)
    return tuple(plans)


class _PlanSink:
    def install(self, plans: tuple[WebSocketPlan, ...], *, generation: int,
                plan_sha256: str) -> None:
        """The session supervisor installs immutable plans at generation boundaries."""


def _owners_abnormally_completed(
    *, prematurely_done: bool, results: list[Any]
) -> bool:
    """Only supervisor-requested cancellations may participate in clean closure."""
    return prematurely_done or any(
        isinstance(result, BaseException)
        and not isinstance(result, asyncio.CancelledError)
        for result in results
    )


class _CaptureThenInspect:
    """Preserve the adapter's receipt-before-decode ordering."""

    def __init__(self, adapter: Any, on_frame: Any) -> None:
        self._adapter = adapter
        self._on_frame = on_frame

    @property
    def frame_seq(self) -> int:
        return int(self._adapter.frame_seq)

    async def consume(self, frames: Any) -> None:
        async for raw in frames:
            async def one(frame: str | bytes = raw) -> Any:
                yield frame

            await self._adapter.consume(one())
            # Adapter.offer has returned before any JSON inspection.
            self._on_frame(raw)


class _MinuteGlobalCapture:
    """Capture first minute frame and every frame changing admitted symbols.

    The clock samples the raw-frame receipt before any decode. Nonselected
    miniTicker frames are intentionally censored from durable capture; their
    individual absence is never treated as negative evidence.
    """

    def __init__(self, adapter: Any, on_frame: Any) -> None:
        self._adapter = adapter
        self._on_frame = on_frame
        self._last_minute: int | None = None

    @property
    def frame_seq(self) -> int:
        return int(self._adapter.frame_seq)

    async def consume(self, frames: Any) -> None:
        from signalbot.capture.models import CaptureEnvelopeV1, payload_text

        async for raw in frames:
            receipt = self._adapter.clock.capture()
            # The raw bytes are owned by this bounded single-frame admission
            # scope before attempting to interpret their JSON shape.
            changed = bool(self._on_frame(raw, receipt.received_at_ms))
            minute = receipt.received_at_ms // 60_000
            if self._last_minute == minute and not changed:
                continue
            self._last_minute = minute
            payload, encoding = payload_text(raw)
            adapter = self._adapter
            adapter.frame_seq += 1
            record = CaptureEnvelopeV1(
                received_at_ms=receipt.received_at_ms,
                received_monotonic_ns=receipt.received_monotonic_ns,
                plan_sha256=adapter.plan_sha256,
                process_boot_id=adapter.process_boot_id,
                connection_id=adapter.connection_id,
                frame_seq=adapter.frame_seq,
                ingest_seq=adapter.sequencer.next(),
                market=adapter.plan.market,
                route=adapter.plan.route,
                stream="combined:" + adapter.plan.name,
                subscription_streams=adapter.plan.streams,
                raw_payload=payload,
                raw_payload_encoding=encoding,
            )
            adapter.pipeline.offer(record)


def _durable_receipt(
    path: Path,
    payload: dict[str, Any],
    *,
    capacity_guard: Any | None = None,
) -> None:
    """Write a small immutable receipt with flush/fsync and atomic promotion."""
    if path.exists():
        raise FileExistsError(f"receipt already sealed: {path.name}")
    encoded = json.dumps(payload, sort_keys=True).encode("utf-8")
    if capacity_guard is not None:
        capacity_guard(0, len(encoded))
    temporary = path.with_suffix(".pending")
    with temporary.open("xb", buffering=0) as handle:
        written = handle.write(encoded)
        if written != len(encoded):
            raise OSError("short write while sealing generation receipt")
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(temporary, path)
    if os.name == "posix":
        directory_fd = os.open(path.parent, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)


def _terminal_payload(directory: Path, start: bytes, identity: dict[str, Any], *,
                      clean: bool, compressed: dict[str, int] | None = None) -> dict[str, Any]:
    return {
        "schema": "pump_fade_v3_generation_terminal_v1",
        "status": "CLEAN_CLOSED" if clean else "RECOVERED_CRASH_INCOMPLETE",
        "recovered_crash": not clean,
        "archive_clean": clean,
        "generation_directory": directory.name,
        "start_sha256": hashlib.sha256(start).hexdigest(),
        "plan_sha256": identity["plan_sha256"],
        "process_boot_id": identity["process_boot_id"],
        "terminal_at_ms": time.time_ns() // 1_000_000,
        "compressed_bytes_by_stream": compressed or {},
        **({} if clean else {"restart_gap": "UNKNOWN_AFTER_UNCLEAN_STOP"}),
    }


def recover_previous_generations(capture_root: Path, *, budget: Any) -> int:
    """Seal interrupted generations as incomplete; never mark clean or pruneable."""
    from signalbot.capture.storage import SegmentedCaptureWriter

    repaired = 0
    if not capture_root.exists():
        return 0
    for directory in sorted(capture_root.iterdir()):
        if not directory.is_dir():
            continue
        start_path = directory / "generation-receipt.json"
        terminal_path = directory / "generation-terminal.json"
        if not start_path.exists():
            raise RuntimeError(f"unidentified capture generation: {directory.name}")
        start_bytes = start_path.read_bytes()
        identity = json.loads(start_bytes)
        if identity.get("schema") != "pump_fade_v3_generation_v1":
            raise RuntimeError(f"unknown capture generation schema: {directory.name}")
        if terminal_path.exists():
            terminal = json.loads(terminal_path.read_bytes())
            if (terminal.get("schema") != "pump_fade_v3_generation_terminal_v1"
                    or terminal.get("generation_directory") != directory.name
                    or terminal.get("start_sha256") != hashlib.sha256(start_bytes).hexdigest()
                    or terminal.get("plan_sha256") != identity.get("plan_sha256")
                    or terminal.get("process_boot_id") != identity.get("process_boot_id")
                    or terminal.get("status") not in {
                        "CLEAN_CLOSED", "RECOVERED_CRASH_INCOMPLETE"
                    }):
                raise RuntimeError(f"generation terminal identity mismatch: {directory.name}")
            continue
        if identity.get("process_boot_id") is None or identity.get("plan_sha256") is None:
            raise RuntimeError("incomplete generation start identity")
        if directory.name != f"capture-{identity['boot']}-g{identity['generation']:04d}":
            raise RuntimeError("generation directory identity mismatch")
        writer = SegmentedCaptureWriter(
            directory, plan_sha256=identity["plan_sha256"],
            process_boot_id=identity["process_boot_id"],
            maximum_total_bytes=budget.maximum_total_bytes,
            emergency_reserve_bytes=budget.emergency_reserve_bytes,
            recover_partials=True, capacity_guard=budget.ensure_capacity,
        )
        writer.close()
        # The transient recovery copy was conservatively reserved while the old
        # partial still existed.  Reconcile only after atomic replacement and
        # finalization so future generations are charged against actual bytes.
        budget.reconcile()
        terminal = _terminal_payload(directory, start_bytes, identity, clean=False)
        _durable_receipt(
            terminal_path,
            terminal,
            capacity_guard=budget.ensure_capacity,
        )
        repaired += 1
    return repaired


def _recent_restart_gap_count(capture_root: Path, *, now_ms: int) -> int:
    cutoff_ms = now_ms - 60 * 60 * 1000
    count = 0
    for directory in capture_root.iterdir():
        terminal_path = directory / "generation-terminal.json"
        if not directory.is_dir() or not terminal_path.is_file():
            continue
        terminal = json.loads(terminal_path.read_text(encoding="utf-8"))
        if (
            terminal.get("status") == "RECOVERED_CRASH_INCOMPLETE"
            and isinstance(terminal.get("terminal_at_ms"), int)
            and terminal["terminal_at_ms"] >= cutoff_ms
            and terminal["terminal_at_ms"] <= now_ms
        ):
            count += 1
    return count


async def run_standalone(
    config_path: str | Path, *, enable: bool,
    expected_mount: str | Path,
    connector: Any = None,
    rest_transport: Any = None,
    operator_stop: asyncio.Event | None = None,
) -> None:
    """Run separate, receipt-stamped generations of public capture.

    Caller must explicitly arm a disabled config. No host resources are altered
    other than new files below its already existing dedicated DATA directory.
    """
    from signalbot.capture.handoff import BoundedCaptureHandoff, CaptureFatalState
    from signalbot.capture.pipeline import CapturePipeline
    from signalbot.capture.receipts import IngestSequencer, SystemReceiptClock
    from signalbot.capture.rest import PublicRestCaptureAdapter
    from signalbot.capture.storage import SegmentedCaptureWriter
    from signalbot.capture.websocket import PublicWebSocketCaptureAdapter
    from signalbot.capture.ws_owner import (
        PublicWebSocketCaptureOwner,
        WebSocketOwnerSettings,
    )
    from signalbot.pump_fade_v3.config import load_collector_config
    from signalbot.pump_fade_v3.disk_budget import DiskBudget
    from signalbot.pump_fade_v3.rest_budget import RestWeightBudget
    from signalbot.pump_fade_v3.runtime import PumpFadeV3Runtime

    config = load_collector_config(config_path)
    if not enable or config.enabled:
        raise ValueError("explicit --enable and disabled-by-default config are required")
    # Explicit operator arm applies in memory only; file never changes.
    from dataclasses import replace
    active = replace(config, enabled=True)
    root = active.data_directory
    budget = DiskBudget(
        root, maximum_total_bytes=active.maximum_total_bytes,
        minimum_free_bytes=active.minimum_free_bytes,
        expected_mount=expected_mount,
        emergency_reserve_bytes=512 * 1024 * 1024,
        event_depth_shed=lambda: LOGGER.critical("depth shedding requested; capture stopping"),
    )
    parent_stop = operator_stop or asyncio.Event()
    capture_root = root / "capture"
    recover_previous_generations(capture_root, budget=budget)
    restart_gaps = _recent_restart_gap_count(
        capture_root, now_ms=time.time_ns() // 1_000_000
    ) if capture_root.exists() else 0
    if restart_gaps > 6:
        LOGGER.critical(
            "collector restart limit exceeded: %d incomplete generations in the last hour",
            restart_gaps,
        )
        raise RuntimeError("collector restart limit exceeded; capture remains stopped")
    capture_root.mkdir(mode=0o700, exist_ok=True)
    clock = SystemReceiptClock()
    plan_runtime: PumpFadeV3Runtime | None = None
    rest_budget = RestWeightBudget(local_limit=active.rest_weight_budget_per_minute)
    boot = uuid.uuid4().hex
    generation = 0
    settings = WebSocketOwnerSettings(
        maximum_connection_age_seconds=3600, connect_timeout_seconds=15,
        close_timeout_seconds=10, heartbeat_interval_seconds=20,
        pong_timeout_seconds=20, internal_queue_frames=16,
        maximum_frame_bytes=2 * 1024 * 1024, maximum_reconnect_attempts=3,
        reconnect_delays_seconds=(1, 2, 4),
    )
    while not parent_stop.is_set():
        generation += 1
        fatal = CaptureFatalState()
        handoff = BoundedCaptureHandoff(max_events=2048, max_bytes=32 * 1024 * 1024,
                                       fatal_state=fatal)
        directory = capture_root / f"capture-{boot}-g{generation:04d}"
        budget.ensure_capacity(0, 0)
        directory.mkdir(mode=0o700, exist_ok=False)
        seq = IngestSequencer()
        # Maintain the admitted EventSymbolSet across generation changes.
        if plan_runtime is None:
            rest_placeholder = object()
            plan_runtime = PumpFadeV3Runtime(
                active, ws_owner=_PlanSink(), rest_adapter=rest_placeholder,  # type: ignore[arg-type]
                budget=rest_budget,
            )
        plan = plan_runtime.refresh(now_ms=clock.capture().received_at_ms)
        assert plan is not None
        generation_plan = plan
        active_runtime = plan_runtime
        receipt = {"schema": "pump_fade_v3_generation_v1", "boot": boot,
                   "generation": generation, "plan_sha256": plan.sha256,
                   "process_boot_id": f"{boot}-g{generation}",
                   "streams": [list(p.streams) for p in plan.websocket],
                   "rest_roles": [p.role for p in plan.rest],
                   "forceOrder_censored": True, "miniTicker_snapshot_ms": 60_000}
        _durable_receipt(
            directory / "generation-receipt.json",
            receipt,
            capacity_guard=budget.ensure_capacity,
        )
        writer = SegmentedCaptureWriter(
            directory, plan_sha256=plan.sha256, process_boot_id=f"{boot}-g{generation}",
            maximum_total_bytes=active.maximum_total_bytes,
            emergency_reserve_bytes=512 * 1024 * 1024,
            capacity_guard=budget.ensure_capacity,
        )
        pipeline = CapturePipeline(handoff, writer)
        rest = PublicRestCaptureAdapter(
            plan_sha256=plan.sha256, process_boot_id=f"{boot}-g{generation}",
            pipeline=pipeline, clock=clock, sequencer=seq, transport=rest_transport,
        )
        plan_runtime.rest_adapter = rest
        generation_change = asyncio.Event()

        def inspect(raw: str | bytes, receipt_ms: int, *,
                    active: PumpFadeV3Runtime = active_runtime,
                    current_sha: str = generation_plan.sha256,
                    changed: asyncio.Event = generation_change) -> bool:
            try:
                message = json.loads(raw)
            except (ValueError, UnicodeDecodeError, TypeError):
                return False
            if not isinstance(message, dict):
                return False
            stream = message.get("stream", "")
            if "!forceOrder@arr" in stream:
                # Every emitted liquidation sample is retained. Exchange
                # sampling is censored, so silence carries no negative label.
                return True
            if "!miniTicker@arr" not in stream:
                return False
            data = message.get("data")
            if not isinstance(data, list):
                return False
            newly_admitted = False
            for row in data:
                try:
                    new_plan = active.observe_ticker(row, now_ms=receipt_ms)
                except ValueError:
                    continue
                if new_plan is not None and new_plan.sha256 != current_sha:
                    changed.set()
                    newly_admitted = True
            return newly_admitted

        owners = []
        for ws_plan in plan.websocket:
            def make_adapter(*, connection_id: str, generation: int,
                             current: WebSocketPlan = ws_plan,
                             current_sha: str = generation_plan.sha256,
                             writer_pipeline: CapturePipeline = pipeline,
                             generation_seq: IngestSequencer = seq,
                             generation_boot: str = f"{boot}-g{receipt['generation']}",
                             inspector: Any = inspect,
                             ) -> _CaptureThenInspect | _MinuteGlobalCapture:
                adapter = PublicWebSocketCaptureAdapter(
                    current, plan_sha256=current_sha,
                    process_boot_id=generation_boot,
                    connection_id=connection_id, pipeline=writer_pipeline, clock=clock,
                    sequencer=generation_seq,
                )
                if current.name == "pump-fade-v3-market-1":
                    return _MinuteGlobalCapture(adapter, inspector)
                return _CaptureThenInspect(adapter, lambda _: None)

            owners.append(PublicWebSocketCaptureOwner(
                ws_plan, plan_sha256=plan.sha256,
                process_boot_id=f"{boot}-g{generation}", pipeline=pipeline,
                clock=clock, sequencer=seq, settings=settings, connector=connector,
                frame_adapter_factory=make_adapter,
            ))
        pipeline.start()
        tasks = [asyncio.create_task(owner.run(fatal.stop_event)) for owner in owners]
        completed_normally = False
        try:
            while not parent_stop.is_set() and not generation_change.is_set() and not fatal.failed:
                now_ms = clock.capture().received_at_ms
                rest_budget.flush_minute(now_ms=now_ms)
                updated_plan = plan_runtime.refresh(now_ms=now_ms)
                if updated_plan is None or updated_plan.sha256 != generation_plan.sha256:
                    generation_change.set()
                    break
                for poll in plan_runtime.due_polls(now_ms=now_ms):
                    if generation_change.is_set() or parent_stop.is_set() or fatal.failed:
                        break
                    await plan_runtime.attempt_poll(poll, now_ms=now_ms)
                await asyncio.sleep(1)
            completed_normally = True
        finally:
            fatal.stop_event.set()
            # A task already done before our intentional shutdown is abnormal,
            # whether it returned, raised, or was independently cancelled.
            prematurely_done = any(task.done() for task in tasks)
            for task in tasks:
                task.cancel()
            owner_results = await asyncio.gather(*tasks, return_exceptions=True)
            owner_failed = _owners_abnormally_completed(
                prematurely_done=prematurely_done, results=list(owner_results)
            )
            await rest.aclose()
            await pipeline.stop()
            if completed_normally and not owner_failed and not fatal.failed and not budget.tripped:
                start_bytes = (directory / "generation-receipt.json").read_bytes()
                terminal = _terminal_payload(
                    directory, start_bytes, receipt, clean=True,
                    compressed=writer.compressed_bytes_by_stream,
                )
                _durable_receipt(
                    directory / "generation-terminal.json",
                    terminal,
                    capacity_guard=budget.ensure_capacity,
                )
        if budget.tripped:
            raise RuntimeError("CRITICAL DATA disk budget reached; capture stopped")
        fatal.raise_if_failed()


def main(argv: list[str] | None = None) -> int:
    from signalbot.pump_fade_v3.config import DEFAULT_DATA_MOUNT

    parser = argparse.ArgumentParser(description="Isolated public Pump Fade v3 collector")
    parser.add_argument("--config", required=True)
    parser.add_argument("--enable", action="store_true")
    parser.add_argument("--data-mount", default=str(DEFAULT_DATA_MOUNT))
    args = parser.parse_args(argv)
    if not args.enable:
        parser.error("standalone collector requires explicit --enable")
    asyncio.run(run_standalone(args.config, enable=True, expected_mount=args.data_mount))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
