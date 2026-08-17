from __future__ import annotations

import asyncio
import binascii
import hashlib
import json
import math
import time
from collections import deque
from collections.abc import Awaitable, Callable, Mapping, Sequence
from dataclasses import dataclass, field
from typing import Literal, NoReturn, Protocol, TypeVar

from signalbot.capture.config import (
    CANARY_FIXED_REQUEST_HEADERS,
    CANARY_SYMBOLS,
    SPOT_DEPTH_SNAPSHOT_MINIMUM_ADMISSION_INTERVAL_SECONDS,
    SPOT_REQUEST_WEIGHT_LIMIT_PER_MINUTE,
    SPOT_USED_WEIGHT_QUARANTINE_THRESHOLD,
    CanaryRestRequestPlanEntry,
    CaptureCanaryConfig,
    capture_rest_request_plan,
)
from signalbot.capture.depth_sequence import (
    DepthRangeObservation,
    DepthResyncRequest,
    DepthSequenceError,
    classify_depth_snapshot_bridge,
)
from signalbot.capture.handoff import CaptureFatalState
from signalbot.capture.models import (
    RawPayloadEncoding,
    RestEnvelopeV2,
    RestErrorCategory,
    payload_bytes,
)
from signalbot.domain.enums import Market


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
        query: Mapping[str, str] | Sequence[tuple[str, str]] = (),
        request_headers: Mapping[str, str] | None = None,
    ) -> RestEnvelopeV2: ...


class CaptureRestSchedulerFailure(RuntimeError):
    """A bounded public REST scheduler failed or received a venue ban response."""


class CaptureRestScheduleOverflow(CaptureRestSchedulerFailure):
    """The bounded operational depth-resync event queue overflowed."""


_MAXIMUM_BUFFERED_DEPTH_RANGES_PER_BOOK = 1_024
_FIXED_DEPTH_BOOK_COUNT = 6
_REQUIRED_SPOT_EXCHANGE_INFO_FILTER_TYPES = frozenset(
    {"PRICE_FILTER", "LOT_SIZE", "MARKET_LOT_SIZE", "NOTIONAL"}
)
_T = TypeVar("_T")
_DepthBridgeResult = Literal["accepted", "stale", "timeout", "superseded"]


@dataclass(slots=True)
class _DepthRangeState:
    market: Market
    symbol: str
    generation: int = 0
    first_u: int | None = None
    pending: bool = False
    ranges: deque[DepthRangeObservation] = field(default_factory=deque)
    changed: asyncio.Event = field(default_factory=asyncio.Event)


class CanaryRestScheduler:
    """Execute frozen capture operations; never emit BBO, features, or efficacy."""

    def __init__(
        self,
        *,
        config: CaptureCanaryConfig,
        adapter: RestAttemptCapture,
        fatal_state: CaptureFatalState,
        wall_time_ms: Callable[[], int] | None = None,
        monotonic_time: Callable[[], float] | None = None,
        wait_or_stop: Callable[[asyncio.Event, float], Awaitable[bool]] | None = None,
    ) -> None:
        if config.symbols != CANARY_SYMBOLS:
            raise ValueError("REST scheduler requires the exact frozen canary symbols")
        self.config = config
        self.adapter = adapter
        self.fatal_state = fatal_state
        self.wall_time_ms = wall_time_ms or (lambda: time.time_ns() // 1_000_000)
        self._monotonic_time = monotonic_time or _event_loop_time
        self._wait_or_stop = wait_or_stop or _wait_or_stop
        self.plan = capture_rest_request_plan()
        self._by_role = {entry.role: entry for entry in self.plan}
        if len(self._by_role) != len(self.plan):
            raise ValueError("REST request-plan roles must be unique")
        self._semaphore = asyncio.Semaphore(config.rest.maximum_concurrency)
        self._depth_events: asyncio.Queue[DepthResyncRequest] = asyncio.Queue(maxsize=32)
        self._depth_ranges = {
            (market, symbol): _DepthRangeState(market=market, symbol=symbol)
            for market in (Market.SPOT, Market.FUTURES)
            for symbol in config.symbols
        }
        if len(self._depth_ranges) != _FIXED_DEPTH_BOOK_COUNT:
            raise ValueError("REST scheduler requires exactly six fixed depth range states")
        self._spot_depth_admission_lock = asyncio.Lock()
        self._spot_depth_next_admission_monotonic = 0.0
        self._not_before_monotonic = 0.0
        self._response_hashes: dict[str, str] = {}
        self._funding_due_ms: dict[str, tuple[int, ...]] = {}
        self._funding_confirmed_ms: dict[str, int] = {}
        self._funding_attempted_ms: dict[str, int] = {}
        self._correlation_sequence = 0
        self._local_failure: BaseException | None = None
        self._inflight_attempts = 0
        self._inflight_drained = asyncio.Event()
        self._inflight_drained.set()
        # One adapter attempt has separately bounded send, body, and close
        # phases. A venue failure may preserve siblings only up to that total.
        self._inflight_drain_timeout_seconds = float(config.rest.timeout_seconds * 3 + 1)

    @property
    def depth_state_count(self) -> int:
        return len(self._depth_ranges)

    @property
    def buffered_depth_range_count(self) -> int:
        return sum(len(state.ranges) for state in self._depth_ranges.values())

    def notify_depth_range(self, observation: DepthRangeObservation) -> None:
        """Synchronously update one of the six fixed operational range buffers."""

        if type(observation) is not DepthRangeObservation:
            raise ValueError("depth range callback requires an exact typed observation")
        state = self._depth_ranges.get((observation.market, observation.symbol))
        if state is None:
            raise ValueError("depth range observation is outside the exact canary books")
        if observation.generation < state.generation:
            return
        if observation.generation > state.generation:
            if not observation.reset:
                self._raise_depth_range_failure(
                    "new depth observation generation arrived without reset"
                )
            self._reset_depth_range_state(state, observation)
            return
        if observation.reset:
            self._reset_depth_range_state(state, observation)
            return
        if state.generation == 0 or state.first_u is None:
            self._raise_depth_range_failure(
                "depth range observation arrived before a generation reset"
            )
        if not state.pending:
            return
        self._append_depth_range(state, observation)
        state.changed.set()

    def notify_depth_resync(self, request: DepthResyncRequest) -> None:
        """Queue a bounded startup/reconnect/sequence-gap snapshot trigger."""

        self._validate_depth_request(request)
        try:
            self._depth_events.put_nowait(request)
        except asyncio.QueueFull as exc:
            error = CaptureRestScheduleOverflow(
                "bounded REST depth-resync event queue overflowed"
            )
            self.fatal_state.trip_unbound(error)
            raise error from exc

    async def run(self, stop_event: asyncio.Event) -> None:
        """Run fixed polling, aligned, funding, and event-triggered loops."""

        if stop_event is not self.fatal_state.stop_event:
            raise ValueError("scheduler stop_event must be the shared fatal-state event")
        tasks: list[asyncio.Task[None]] = []
        for entry in self.plan:
            if entry.trigger in {"interval", "interval_or_exchange_info_hash_change"}:
                tasks.append(
                    asyncio.create_task(
                        self._guarded_loop(self._interval_loop(entry)),
                        name=f"capture-rest-{entry.role}",
                    )
                )
            elif entry.trigger == "utc_bar_close":
                tasks.append(
                    asyncio.create_task(
                        self._guarded_loop(self._utc_bar_loop(entry)),
                        name=f"capture-rest-{entry.role}",
                    )
                )
        tasks.extend(
            (
                asyncio.create_task(
                    self._guarded_loop(self._depth_event_loop()),
                    name="capture-rest-depth-events",
                ),
                asyncio.create_task(
                    self._guarded_loop(self._funding_confirmation_loop()),
                    name="capture-rest-funding-confirmation",
                ),
            )
        )
        try:
            await stop_event.wait()
        finally:
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
        self.fatal_state.raise_if_failed()

    async def capture_entry_once(
        self,
        entry: CanaryRestRequestPlanEntry,
        *,
        scheduled_at_ms: int,
        symbol: str | None = None,
    ) -> RestEnvelopeV2:
        """Capture one exact planned request and apply operational scheduling state."""

        if self._by_role.get(entry.role) != entry:
            raise ValueError("REST request entry differs from the frozen canary plan")
        canonical = planned_query(entry, symbol=symbol)
        identity = "global" if symbol is None else symbol
        self._correlation_sequence += 1
        correlation_id = (
            f"{entry.role}:{identity}:{scheduled_at_ms}:"
            f"{self._correlation_sequence:012d}"
        )
        envelope: RestEnvelopeV2 | None = None
        try:
            for attempt in range(1, entry.maximum_attempts + 1):
                if entry.role == "spot_depth_snapshot":
                    # Keep contenders serialized through the adapter attempt.
                    # The next cadence begins only after this adapter attempt
                    # terminates, so an adapter-internal connection/cleanup
                    # wait cannot consume spacing and later create a wire burst.
                    async with self._spot_depth_admission_lock:
                        envelope = await self._capture_adapter_attempt(
                            entry=entry,
                            canonical=canonical,
                            correlation_id=correlation_id,
                            attempt=attempt,
                            pace_spot_depth=True,
                        )
                else:
                    envelope = await self._capture_adapter_attempt(
                        entry=entry,
                        canonical=canonical,
                        correlation_id=correlation_id,
                        attempt=attempt,
                        pace_spot_depth=False,
                    )
                if _attempt_succeeded(envelope):
                    break
                if attempt < entry.maximum_attempts:
                    if await _wait_or_stop(
                        self.fatal_state.stop_event,
                        self.config.rest.retry_delays_seconds[attempt - 1],
                    ):
                        break
        except CaptureRestSchedulerFailure as exc:
            await self._trip_after_inflight_drain(exc)
            raise
        assert envelope is not None
        await self._observe_operational_payload(entry, symbol, envelope)
        return envelope

    async def _capture_adapter_attempt(
        self,
        *,
        entry: CanaryRestRequestPlanEntry,
        canonical: tuple[tuple[str, str], ...],
        correlation_id: str,
        attempt: int,
        pace_spot_depth: bool,
    ) -> RestEnvelopeV2:
        async with self._semaphore:
            self.fatal_state.raise_if_failed()
            self._raise_if_locally_failed()
            await self._wait_rate_limit_gate()
            self.fatal_state.raise_if_failed()
            self._raise_if_locally_failed()
            if pace_spot_depth:
                await self._wait_spot_depth_admission_locked()
            self._begin_inflight_attempt()
            try:
                envelope = await self.adapter.capture_attempt(
                    method=entry.method,
                    market=entry.market,
                    url=f"{entry.rest_base}{entry.path}",
                    request_role=entry.role,
                    correlation_id=correlation_id,
                    attempt=attempt,
                    query=canonical,
                )
            finally:
                self._end_inflight_attempt()
                if pace_spot_depth:
                    self._spot_depth_next_admission_monotonic = (
                        self._monotonic_time()
                        + SPOT_DEPTH_SNAPSHOT_MINIMUM_ADMISSION_INTERVAL_SECONDS
                    )
            self._reject_body_limit(envelope)
            self._observe_spot_used_weight(envelope)
            self._observe_rate_limit(envelope)
            if envelope.response_status == 418:
                error = CaptureRestSchedulerFailure(
                    "Binance HTTP 418 requires capture quarantine"
                )
                self._set_local_failure(error)
                raise error
            return envelope

    async def _wait_spot_depth_admission_locked(self) -> None:
        """Wait until one adapter admission is eligible at the frozen cadence."""

        if not self._spot_depth_admission_lock.locked():
            raise RuntimeError("Spot depth admission requires its serialization lock")
        self.fatal_state.raise_if_failed()
        self._raise_if_locally_failed()
        if self.fatal_state.stop_event.is_set():
            raise asyncio.CancelledError
        delay = max(
            0.0,
            self._spot_depth_next_admission_monotonic - self._monotonic_time(),
        )
        if delay > 0 and await self._wait_or_stop(
            self.fatal_state.stop_event,
            delay,
        ):
            raise asyncio.CancelledError
        self.fatal_state.raise_if_failed()
        self._raise_if_locally_failed()
        if self.fatal_state.stop_event.is_set():
            raise asyncio.CancelledError

    async def handle_depth_event(
        self,
        request: DepthResyncRequest,
    ) -> tuple[RestEnvelopeV2, ...]:
        """Capture snapshots and coordinate a bounded online sequence bridge.

        This operational coordinator emits no BBO, features, or efficacy claim.
        Persisted raw frames and request-start clocks remain the authoritative
        inputs to the offline local-book materializer.
        """

        self._validate_depth_request(request)
        role = (
            "spot_depth_snapshot"
            if request.market is Market.SPOT
            else "futures_depth_snapshot"
        )
        scheduled_at_ms = self.wall_time_ms()
        jobs = [
            self._capture_depth_snapshot_cycle(
                self._by_role[role],
                scheduled_at_ms=scheduled_at_ms,
                symbol=symbol,
                generation=request.generation,
                first_buffered_u=first_buffered_u,
            )
            for symbol, first_buffered_u in request.watermarks
        ]
        outcomes = await self._gather_attempts(jobs)
        return tuple(envelope for envelope in outcomes if envelope is not None)

    async def _capture_depth_snapshot_cycle(
        self,
        entry: CanaryRestRequestPlanEntry,
        *,
        scheduled_at_ms: int,
        symbol: str,
        generation: int,
        first_buffered_u: int,
    ) -> RestEnvelopeV2 | None:
        state = self._depth_ranges[(entry.market, symbol)]
        if not self._depth_request_is_current(
            state,
            generation=generation,
            first_buffered_u=first_buffered_u,
        ):
            return None
        maximum_attempts = self.config.polling.depth_snapshot_bridge_maximum_attempts
        last_envelope: RestEnvelopeV2 | None = None
        last_result: _DepthBridgeResult | None = None
        for _bridge_attempt in range(maximum_attempts):
            envelope = await self.capture_entry_once(
                entry,
                scheduled_at_ms=scheduled_at_ms,
                symbol=symbol,
            )
            last_envelope = envelope
            if not _attempt_succeeded(envelope) or envelope.response_status != 200:
                await self._raise_scheduler_failure(
                    "bounded depth snapshot non-200 HTTP/transport failure for "
                    f"{entry.market.value}:{symbol}"
                )
            try:
                last_update_id = _depth_snapshot_last_update_id(envelope)
            except (binascii.Error, UnicodeDecodeError, json.JSONDecodeError, ValueError) as exc:
                await self._raise_scheduler_failure(
                    "malformed 2xx depth snapshot for "
                    f"{entry.market.value}:{symbol}",
                    cause=exc,
                )
            if not self._depth_request_is_current(
                state,
                generation=generation,
                first_buffered_u=first_buffered_u,
            ):
                return envelope
            last_result = await self._apply_depth_snapshot_bridge(
                state,
                generation=generation,
                first_buffered_u=first_buffered_u,
                last_update_id=last_update_id,
            )
            if last_result == "accepted":
                return envelope
            if last_result == "superseded":
                return envelope
        assert last_envelope is not None and last_result is not None
        if not self._depth_request_is_current(
            state,
            generation=generation,
            first_buffered_u=first_buffered_u,
        ):
            return last_envelope
        await self._raise_scheduler_failure(
            f"depth snapshot bridge {last_result} exhausted the fixed attempts for "
            f"{entry.market.value}:{symbol}:generation-{generation}"
        )

    async def _raise_scheduler_failure(
        self,
        detail: str,
        *,
        cause: BaseException | None = None,
    ) -> NoReturn:
        error = CaptureRestSchedulerFailure(detail)
        if cause is not None:
            error.__cause__ = cause
        await self._trip_after_inflight_drain(error)
        raise error

    async def _apply_depth_snapshot_bridge(
        self,
        state: _DepthRangeState,
        *,
        generation: int,
        first_buffered_u: int,
        last_update_id: int,
    ) -> _DepthBridgeResult:
        deadline = (
            asyncio.get_running_loop().time()
            + self.config.polling.depth_snapshot_bridge_wait_seconds
        )
        while True:
            if not self._depth_request_is_current(
                state,
                generation=generation,
                first_buffered_u=first_buffered_u,
            ):
                return "superseded"
            try:
                decision = classify_depth_snapshot_bridge(
                    state.market,
                    last_update_id,
                    tuple((observation.U, observation.u) for observation in state.ranges),
                )
            except DepthSequenceError as exc:
                await self._raise_scheduler_failure(
                    "depth snapshot bridge retained invalid range evidence for "
                    f"{state.market.value}:{state.symbol}",
                    cause=exc,
                )
            for _discarded_range in range(decision.discarded_range_count):
                state.ranges.popleft()
            if decision.status == "accepted":
                if not self._depth_request_is_current(
                    state,
                    generation=generation,
                    first_buffered_u=first_buffered_u,
                ):
                    return "superseded"
                state.ranges.clear()
                state.pending = False
                state.changed.set()
                return "accepted"
            if decision.status == "stale":
                return "stale"

            remaining = deadline - asyncio.get_running_loop().time()
            if remaining <= 0:
                return "timeout"
            # No task switch occurs between the empty-buffer decision and clear;
            # the next synchronous observation sets the event after this point.
            state.changed.clear()
            if not self._depth_request_is_current(
                state,
                generation=generation,
                first_buffered_u=first_buffered_u,
            ):
                return "superseded"
            try:
                await asyncio.wait_for(state.changed.wait(), timeout=remaining)
            except TimeoutError:
                return "timeout"

    def _depth_request_is_current(
        self,
        state: _DepthRangeState,
        *,
        generation: int,
        first_buffered_u: int,
    ) -> bool:
        return (
            state.pending
            and state.generation == generation
            and state.first_u == first_buffered_u
        )

    def _reset_depth_range_state(
        self,
        state: _DepthRangeState,
        observation: DepthRangeObservation,
    ) -> None:
        state.ranges.clear()
        state.generation = observation.generation
        state.first_u = observation.U
        state.pending = True
        self._append_depth_range(state, observation)
        state.changed.set()

    def _append_depth_range(
        self,
        state: _DepthRangeState,
        observation: DepthRangeObservation,
    ) -> None:
        if len(state.ranges) >= _MAXIMUM_BUFFERED_DEPTH_RANGES_PER_BOOK:
            error = CaptureRestScheduleOverflow(
                "bounded operational depth range buffer overflowed for "
                f"{state.market.value}:{state.symbol}"
            )
            self._set_local_failure(error)
            self.fatal_state.trip_unbound(error)
            raise error
        state.ranges.append(observation)

    def _raise_depth_range_failure(self, detail: str) -> NoReturn:
        error = CaptureRestSchedulerFailure(detail)
        self._set_local_failure(error)
        self.fatal_state.trip_unbound(error)
        raise error

    def _validate_depth_request(self, request: DepthResyncRequest) -> None:
        if type(request) is not DepthResyncRequest:
            raise ValueError("depth resync callback requires an exact typed request")
        if request.event not in self.config.polling.depth_snapshot_triggers:
            raise ValueError("depth resync event is not in the frozen trigger set")
        symbols = tuple(symbol for symbol, _first_u in request.watermarks)
        expected_symbols = tuple(sorted(self.config.symbols))
        if request.event in {"startup", "reconnect"}:
            if symbols != expected_symbols:
                raise ValueError(
                    "startup/reconnect depth resync must cover the exact canary symbols"
                )
        elif len(symbols) != 1 or symbols[0] not in expected_symbols:
            raise ValueError("sequence-gap depth resync must cover one canary symbol")

    async def capture_due_funding_confirmations(
        self,
        now_ms: int | None = None,
    ) -> tuple[RestEnvelopeV2, ...]:
        current = self.wall_time_ms() if now_ms is None else now_ms
        due = sorted(
            (symbol, funding_time)
            for symbol, funding_times in self._funding_due_ms.items()
            for funding_time in funding_times
            if funding_time
            + self.config.polling.futures_funding_rate_delay_seconds * 1_000
            <= current
        )
        entry = self._by_role["futures_funding_rate_confirmation"]
        results: list[RestEnvelopeV2] = []
        for symbol, funding_time in due:
            result = await self.capture_entry_once(
                entry,
                scheduled_at_ms=current,
                symbol=symbol,
            )
            results.append(result)
            self._funding_attempted_ms[symbol] = funding_time
            if _attempt_succeeded(result):
                self._funding_confirmed_ms[symbol] = funding_time
            remaining = tuple(
                value
                for value in self._funding_due_ms.get(symbol, ())
                if value != funding_time
            )
            if remaining:
                self._funding_due_ms[symbol] = remaining
            else:
                self._funding_due_ms.pop(symbol, None)
        return tuple(results)

    async def _capture_entry_batch(
        self,
        entry: CanaryRestRequestPlanEntry,
        scheduled_at_ms: int,
    ) -> tuple[RestEnvelopeV2, ...]:
        symbols: tuple[str | None, ...] = (
            tuple(self.config.symbols)
            if "symbol" in entry.allowed_query_keys
            else (None,)
        )
        return await self._gather_attempts(
            [
                self.capture_entry_once(
                    entry,
                    scheduled_at_ms=scheduled_at_ms,
                    symbol=symbol,
                )
                for symbol in symbols
            ]
        )

    async def _gather_attempts(
        self,
        attempts: Sequence[Awaitable[_T]],
    ) -> tuple[_T, ...]:
        """Reap every sibling and stop queued sends after the first fatal result."""

        tasks = [asyncio.ensure_future(attempt) for attempt in attempts]
        try:
            outcomes = await asyncio.gather(*tasks, return_exceptions=True)
        except asyncio.CancelledError:
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            raise
        results: list[_T] = []
        for outcome in outcomes:
            if isinstance(outcome, BaseException):
                raise outcome
            results.append(outcome)
        return tuple(results)

    async def _interval_loop(self, entry: CanaryRestRequestPlanEntry) -> None:
        assert entry.interval_seconds is not None
        interval = float(entry.interval_seconds)
        next_due = asyncio.get_running_loop().time()
        while not self.fatal_state.stop_event.is_set():
            await _sleep_until(next_due)
            if self.fatal_state.stop_event.is_set():
                return
            await self._capture_entry_batch(entry, self.wall_time_ms())
            next_due += interval
            now = asyncio.get_running_loop().time()
            if next_due <= now:
                next_due = now + interval

    async def _utc_bar_loop(self, entry: CanaryRestRequestPlanEntry) -> None:
        assert entry.interval_seconds is not None and entry.delay_seconds is not None
        interval_ms = entry.interval_seconds * 1_000
        while not self.fatal_state.stop_event.is_set():
            now_ms = self.wall_time_ms()
            due_ms = ((now_ms // interval_ms) + 1) * interval_ms + entry.delay_seconds * 1_000
            if await _wait_or_stop(
                self.fatal_state.stop_event,
                max(0.0, (due_ms - now_ms) / 1_000),
            ):
                return
            await self._capture_entry_batch(entry, due_ms)

    async def _depth_event_loop(self) -> None:
        while not self.fatal_state.stop_event.is_set():
            request = await self._depth_events.get()
            try:
                await self.handle_depth_event(request)
            finally:
                self._depth_events.task_done()

    async def _funding_confirmation_loop(self) -> None:
        while not self.fatal_state.stop_event.is_set():
            await self.capture_due_funding_confirmations()
            if await _wait_or_stop(self.fatal_state.stop_event, 1.0):
                return

    async def _observe_operational_payload(
        self,
        entry: CanaryRestRequestPlanEntry,
        symbol: str | None,
        envelope: RestEnvelopeV2,
    ) -> None:
        if not _attempt_succeeded(envelope):
            return
        if entry.role == "spot_exchange_info":
            try:
                body = payload_bytes(envelope.raw_payload, envelope.raw_payload_encoding)
                _validate_spot_exchange_info_contract(body)
            except (
                binascii.Error,
                UnicodeDecodeError,
                json.JSONDecodeError,
                ValueError,
            ) as exc:
                await self._raise_scheduler_failure(
                    "Spot exchangeInfo contract differs from frozen "
                    f"REQUEST_WEIGHT/MINUTE/1={SPOT_REQUEST_WEIGHT_LIMIT_PER_MINUTE} "
                    "and exact canary-symbol schema",
                    cause=exc,
                )
        else:
            body = payload_bytes(envelope.raw_payload, envelope.raw_payload_encoding)
        if entry.hash_on_change:
            digest = _stable_operational_digest(entry, body)
            if digest is None:
                return
            previous = self._response_hashes.get(entry.role)
            self._response_hashes[entry.role] = digest
            if previous is not None and previous != digest and entry.market is Market.FUTURES:
                funding_info = self._by_role["futures_funding_info"]
                await self._capture_entry_batch(funding_info, self.wall_time_ms())
        if entry.role != "futures_premium_index" or symbol is None:
            return
        if envelope.raw_payload_encoding is not RawPayloadEncoding.TEXT:
            return
        try:
            parsed = json.loads(envelope.raw_payload)
        except json.JSONDecodeError:
            return
        if not isinstance(parsed, dict):
            return
        next_funding = parsed.get("nextFundingTime")
        if isinstance(next_funding, bool) or not isinstance(next_funding, int):
            return
        already_handled = max(
            self._funding_confirmed_ms.get(symbol, -1),
            self._funding_attempted_ms.get(symbol, -1),
        )
        if next_funding <= already_handled:
            return
        pending = self._funding_due_ms.get(symbol, ())
        if next_funding in pending:
            return
        if len(pending) >= 2:
            error = CaptureRestScheduleOverflow(
                "bounded pending funding-confirmation queue overflowed"
            )
            await self._trip_after_inflight_drain(error)
            raise error
        self._funding_due_ms[symbol] = tuple(sorted((*pending, next_funding)))

    def _reject_body_limit(self, envelope: RestEnvelopeV2) -> None:
        if envelope.error_category is not RestErrorCategory.BODY_LIMIT:
            return
        error = CaptureRestSchedulerFailure(
            "REST body-limit evidence requires capture quarantine"
        )
        self._set_local_failure(error)
        raise error

    def _observe_spot_used_weight(self, envelope: RestEnvelopeV2) -> None:
        if envelope.market is not Market.SPOT or not _attempt_succeeded(envelope):
            return
        values = [
            value
            for name, value in envelope.response_headers
            if name == "x-mbx-used-weight-1m"
        ]
        if len(values) != 1:
            error = CaptureRestSchedulerFailure(
                "successful Binance Spot response lacks one exact "
                "x-mbx-used-weight-1m header"
            )
            self._set_local_failure(error)
            raise error
        value = values[0]
        try:
            used_weight = int(value)
        except ValueError as exc:
            error = CaptureRestSchedulerFailure(
                "Binance Spot x-mbx-used-weight-1m is not a canonical "
                "nonnegative integer"
            )
            self._set_local_failure(error)
            raise error from exc
        if used_weight < 0 or value != str(used_weight):
            error = CaptureRestSchedulerFailure(
                "Binance Spot x-mbx-used-weight-1m is not a canonical "
                "nonnegative integer"
            )
            self._set_local_failure(error)
            raise error
        if used_weight >= SPOT_USED_WEIGHT_QUARANTINE_THRESHOLD:
            error = CaptureRestSchedulerFailure(
                "Binance Spot request-weight high-water requires capture quarantine"
            )
            self._set_local_failure(error)
            raise error

    def _observe_rate_limit(self, envelope: RestEnvelopeV2) -> None:
        if envelope.response_status != 429:
            return
        values = [
            value for name, value in envelope.response_headers if name == "retry-after"
        ]
        if len(values) != 1:
            error = CaptureRestSchedulerFailure(
                "Binance HTTP 429 lacks one unambiguous numeric Retry-After"
            )
            self._set_local_failure(error)
            raise error
        try:
            retry_after = float(values[0])
        except ValueError as exc:
            error = CaptureRestSchedulerFailure(
                "Binance HTTP 429 Retry-After is not numeric seconds"
            )
            self._set_local_failure(error)
            raise error from exc
        maximum = float(self.config.rest.maximum_retry_after_seconds)
        if not math.isfinite(retry_after) or retry_after < 0 or retry_after > maximum:
            error = CaptureRestSchedulerFailure(
                "Binance Retry-After exceeds the bounded capture policy"
            )
            self._set_local_failure(error)
            raise error
        self._not_before_monotonic = max(
            self._not_before_monotonic,
            asyncio.get_running_loop().time() + retry_after,
        )

    async def _wait_rate_limit_gate(self) -> None:
        await _sleep_until(self._not_before_monotonic)

    def _begin_inflight_attempt(self) -> None:
        if self._inflight_attempts == 0:
            self._inflight_drained.clear()
        self._inflight_attempts += 1

    def _end_inflight_attempt(self) -> None:
        if self._inflight_attempts < 1:
            raise RuntimeError("REST in-flight attempt accounting underflowed")
        self._inflight_attempts -= 1
        if self._inflight_attempts == 0:
            self._inflight_drained.set()

    def _set_local_failure(self, error: BaseException) -> None:
        if self._local_failure is None:
            self._local_failure = error

    def _raise_if_locally_failed(self) -> None:
        if self._local_failure is not None:
            raise self._local_failure

    async def _trip_after_inflight_drain(
        self,
        error: BaseException,
    ) -> None:
        self._set_local_failure(error)
        try:
            try:
                await asyncio.wait_for(
                    self._inflight_drained.wait(),
                    timeout=self._inflight_drain_timeout_seconds,
                )
            except TimeoutError:
                # The first venue/program failure remains authoritative. Evidence
                # preservation is bounded; an uncooperative sibling cannot prevent
                # the shared supervisor from entering quarantine indefinitely.
                pass
        finally:
            # Parent/operator cancellation must not turn a known venue failure
            # into a clean closure classification.
            assert self._local_failure is not None
            self.fatal_state.trip_unbound(self._local_failure)

    async def _guarded_loop(self, awaitable: Awaitable[None]) -> None:
        try:
            await awaitable
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            await self._trip_after_inflight_drain(exc)


def planned_query(
    entry: CanaryRestRequestPlanEntry,
    *,
    symbol: str | None,
) -> tuple[tuple[str, str], ...]:
    """Materialize and revalidate the exact query authorized by one plan entry."""

    query = dict(entry.fixed_query)
    if "symbol" in entry.allowed_query_keys:
        if symbol not in CANARY_SYMBOLS:
            raise ValueError("planned symbol must be one of the exact canary symbols")
        assert symbol is not None
        query["symbol"] = symbol
    elif symbol is not None:
        raise ValueError("this planned request does not accept a symbol")
    canonical = tuple(sorted(query.items()))
    if tuple(name for name, _value in canonical) != entry.allowed_query_keys:
        raise ValueError("planned query does not cover the exact allowed key set")
    allowed = {item.key: item.values for item in entry.allowed_query_values}
    if any(value not in allowed[name] for name, value in canonical):
        raise ValueError("planned query value is outside the frozen allowlist")
    if entry.fixed_request_headers != CANARY_FIXED_REQUEST_HEADERS:
        raise ValueError("planned request headers differ from the fixed identity encoding")
    return canonical


def _validate_spot_exchange_info_contract(body: bytes) -> None:
    parsed = json.loads(body)
    if not isinstance(parsed, dict):
        raise ValueError("Spot exchangeInfo root must be an object")
    rate_limits = parsed.get("rateLimits")
    if not isinstance(rate_limits, list):
        raise ValueError("Spot exchangeInfo rateLimits must be an array")
    matches = [
        item
        for item in rate_limits
        if isinstance(item, dict)
        and item.get("rateLimitType") == "REQUEST_WEIGHT"
        and item.get("interval") == "MINUTE"
        and item.get("intervalNum") == 1
    ]
    if len(matches) != 1:
        raise ValueError(
            "Spot exchangeInfo must contain one REQUEST_WEIGHT/MINUTE/1 limit"
        )
    interval_num = matches[0].get("intervalNum")
    limit = matches[0].get("limit")
    if isinstance(interval_num, bool) or not isinstance(interval_num, int):
        raise ValueError("Spot exchangeInfo intervalNum must be the integer one")
    if isinstance(limit, bool) or not isinstance(limit, int):
        raise ValueError("Spot exchangeInfo request-weight limit must be an integer")
    if limit != SPOT_REQUEST_WEIGHT_LIMIT_PER_MINUTE:
        raise ValueError("Spot exchangeInfo request-weight limit drifted")
    symbol_rows = parsed.get("symbols")
    if not isinstance(symbol_rows, list):
        raise ValueError("Spot exchangeInfo symbols must be an array")
    returned_symbols: set[str] = set()
    for row in symbol_rows:
        if not isinstance(row, dict):
            raise ValueError("Spot exchangeInfo symbol row must be an object")
        symbol = row.get("symbol")
        if not isinstance(symbol, str) or not symbol:
            raise ValueError("Spot exchangeInfo row symbol must be a non-empty string")
        if symbol in returned_symbols:
            raise ValueError("Spot exchangeInfo contains a duplicate symbol")
        returned_symbols.add(symbol)
        status = row.get("status")
        if not isinstance(status, str) or not status:
            raise ValueError("Spot exchangeInfo row status must be a non-empty string")
        filters = row.get("filters")
        if not isinstance(filters, list):
            raise ValueError("Spot exchangeInfo row filters must be an array")
        filter_types: set[str] = set()
        for item in filters:
            if not isinstance(item, dict):
                raise ValueError("Spot exchangeInfo filter must be an object")
            filter_type = item.get("filterType")
            if not isinstance(filter_type, str) or not filter_type:
                raise ValueError("Spot exchangeInfo filterType must be a non-empty string")
            if filter_type in filter_types:
                raise ValueError("Spot exchangeInfo contains a duplicate filterType")
            filter_types.add(filter_type)
        if not _REQUIRED_SPOT_EXCHANGE_INFO_FILTER_TYPES.issubset(filter_types):
            raise ValueError("Spot exchangeInfo row lacks a required filterType")
    if returned_symbols != set(CANARY_SYMBOLS) or len(symbol_rows) != len(CANARY_SYMBOLS):
        raise ValueError("Spot exchangeInfo returned symbol set differs from the canary")


def _attempt_succeeded(envelope: RestEnvelopeV2) -> bool:
    return (
        envelope.payload_complete
        and envelope.response_status is not None
        and 200 <= envelope.response_status < 300
        and envelope.error_category is None
    )


def _depth_snapshot_last_update_id(envelope: RestEnvelopeV2) -> int:
    body = payload_bytes(envelope.raw_payload, envelope.raw_payload_encoding)
    parsed = json.loads(body)
    if not isinstance(parsed, dict):
        raise ValueError("depth snapshot root must be an object")
    for side in ("bids", "asks"):
        if not isinstance(parsed.get(side), list):
            raise ValueError(f"depth snapshot {side} must be an array")
    last_update_id = parsed.get("lastUpdateId")
    if (
        isinstance(last_update_id, bool)
        or not isinstance(last_update_id, int)
        or last_update_id < 0
    ):
        raise ValueError("depth snapshot lastUpdateId must be a nonnegative integer")
    return last_update_id


async def _sleep_until(monotonic_deadline: float) -> None:
    delay = monotonic_deadline - asyncio.get_running_loop().time()
    if delay > 0:
        await asyncio.sleep(delay)


def _event_loop_time() -> float:
    return asyncio.get_running_loop().time()


async def _wait_or_stop(stop_event: asyncio.Event, delay_seconds: float) -> bool:
    if stop_event.is_set():
        return True
    try:
        await asyncio.wait_for(stop_event.wait(), timeout=delay_seconds)
    except TimeoutError:
        return False
    return True


def _stable_operational_digest(
    entry: CanaryRestRequestPlanEntry,
    body: bytes,
) -> str | None:
    """Hash stable exchange metadata, excluding Binance's volatile server clock."""

    if entry.path.endswith("/exchangeInfo"):
        try:
            parsed = json.loads(body)
        except (UnicodeDecodeError, json.JSONDecodeError):
            return None
        if not isinstance(parsed, dict):
            return None
        stable = dict(parsed)
        stable.pop("serverTime", None)
        canonical = json.dumps(
            stable,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
        return hashlib.sha256(canonical).hexdigest()
    return hashlib.sha256(body).hexdigest()
