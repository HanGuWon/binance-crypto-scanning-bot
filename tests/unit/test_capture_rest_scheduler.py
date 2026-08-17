from __future__ import annotations

import asyncio
import hashlib
import json
from collections.abc import Mapping, Sequence
from dataclasses import replace
from pathlib import Path
from typing import cast

import pytest

from signalbot.capture.config import (
    CANARY_SYMBOLS,
    SPOT_DEPTH_SNAPSHOT_MINIMUM_ADMISSION_INTERVAL_SECONDS,
    CanaryRestRequestPlanEntry,
    CaptureCanaryConfig,
    capture_rest_request_plan,
    load_capture_canary_config,
)
from signalbot.capture.depth_sequence import (
    DepthRangeObservation,
    DepthResyncEvent,
    DepthResyncRequest,
)
from signalbot.capture.handoff import CaptureFatalState
from signalbot.capture.models import RestEnvelopeV2, RestErrorCategory
from signalbot.capture.rest_scheduler import (
    CanaryRestScheduler,
    CaptureRestScheduleOverflow,
    CaptureRestSchedulerFailure,
    planned_query,
)
from signalbot.domain.enums import Market

ROOT = Path(__file__).resolve().parents[2]
CONFIG = ROOT / "config/capture.r4b-canary-v1.yaml"
PROTOCOL = ROOT / "artifacts/oracle/2026-07-17/R4b_frozen_experiment_spec_v1.yaml"
PLAN_SHA256 = hashlib.sha256(b"rest-scheduler-test").hexdigest()


def _spot_exchange_info_symbol_row(
    symbol: object,
    *,
    status: object = "TRADING",
) -> dict[str, object]:
    return {
        "symbol": symbol,
        "status": status,
        "filters": [
            {"filterType": filter_type}
            for filter_type in (
                "PRICE_FILTER",
                "LOT_SIZE",
                "MARKET_LOT_SIZE",
                "NOTIONAL",
            )
        ],
    }


def _spot_exchange_info_payload() -> dict[str, object]:
    return {
        "rateLimits": [
            {
                "rateLimitType": "REQUEST_WEIGHT",
                "interval": "MINUTE",
                "intervalNum": 1,
                "limit": 6_000,
            }
        ],
        "symbols": [_spot_exchange_info_symbol_row(symbol) for symbol in CANARY_SYMBOLS],
    }


def _spot_exchange_info_body() -> str:
    return json.dumps(_spot_exchange_info_payload(), separators=(",", ":"))


class FakePacerClock:
    def __init__(self) -> None:
        self.now = 0.0
        self.delays: list[float] = []

    def monotonic(self) -> float:
        return self.now

    async def wait_or_stop(self, stop_event: asyncio.Event, delay: float) -> bool:
        if stop_event.is_set():
            return True
        self.delays.append(delay)
        self.now += delay
        await asyncio.sleep(0)
        return False


class FakeRestAdapter:
    def __init__(self) -> None:
        self.calls: list[dict[str, object]] = []
        self.active = 0
        self.peak_active = 0
        self.responses: dict[str, list[tuple[int | None, str]]] = {}
        self.response_headers: dict[str, tuple[tuple[str, str], ...]] = {}
        self.spot_used_weight = 0
        self._sequence = 0

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
    ) -> RestEnvelopeV2:
        self.active += 1
        self.peak_active = max(self.peak_active, self.active)
        try:
            await asyncio.sleep(0)
            canonical = tuple(sorted(query.items() if isinstance(query, Mapping) else query))
            self.calls.append(
                {
                    "method": method,
                    "market": market,
                    "url": url,
                    "role": request_role,
                    "correlation_id": correlation_id,
                    "attempt": attempt,
                    "query": canonical,
                    "headers": request_headers,
                }
            )
            queued = self.responses.get(request_role, [])
            if request_role.endswith("_depth_snapshot"):
                default_body = '{"lastUpdateId":100,"bids":[],"asks":[]}'
            elif request_role == "spot_exchange_info":
                default_body = _spot_exchange_info_body()
            else:
                default_body = "{}"
            status, body = queued.pop(0) if queued else (200, default_body)
            self._sequence += 1
            start = 10_000 + self._sequence * 10
            if status is None:
                return RestEnvelopeV2(
                    request_started_at_ms=start,
                    request_started_monotonic_ns=start,
                    response_first_byte_at_ms=None,
                    response_first_byte_monotonic_ns=None,
                    response_completed_at_ms=start + 1,
                    response_completed_monotonic_ns=start + 1,
                    plan_sha256=PLAN_SHA256,
                    process_boot_id="boot-1",
                    request_role=request_role,
                    correlation_id=correlation_id,
                    attempt=attempt,
                    ingest_seq=self._sequence,
                    market=market,
                    endpoint_path="/" + url.split("/", 3)[-1],
                    canonical_query=canonical,
                    response_status=None,
                    response_headers=(),
                    payload_complete=False,
                    raw_payload="",
                    error_category=RestErrorCategory.NETWORK,
                    error_detail="network request failed before response headers",
                )
            error_category = None if 200 <= status < 300 else RestErrorCategory.HTTP_STATUS
            response_headers = self.response_headers.get(request_role)
            if response_headers is None and market is Market.SPOT and 200 <= status < 300:
                weights = {
                    "spot_venue_time": 1,
                    "spot_exchange_info": 20,
                    "spot_depth_snapshot": 250,
                }
                self.spot_used_weight += weights[request_role]
                response_headers = (
                    ("x-mbx-used-weight-1m", str(self.spot_used_weight)),
                )
            return RestEnvelopeV2(
                request_started_at_ms=start,
                request_started_monotonic_ns=start,
                response_first_byte_at_ms=start + 1,
                response_first_byte_monotonic_ns=start + 1,
                response_completed_at_ms=start + 2,
                response_completed_monotonic_ns=start + 2,
                plan_sha256=PLAN_SHA256,
                process_boot_id="boot-1",
                request_role=request_role,
                correlation_id=correlation_id,
                attempt=attempt,
                ingest_seq=self._sequence,
                market=market,
                endpoint_path="/" + url.split("/", 3)[-1],
                canonical_query=canonical,
                response_status=status,
                response_headers=response_headers or (),
                payload_complete=True,
                raw_payload=body,
                error_category=error_category,
                error_detail=None if error_category is None else f"HTTP status {status}",
            )
        finally:
            self.active -= 1


def _config() -> CaptureCanaryConfig:
    return load_capture_canary_config(CONFIG, protocol_file=PROTOCOL)


def _scheduler(
    adapter: FakeRestAdapter,
    *,
    fatal: CaptureFatalState | None = None,
    now_ms: int = 1_000_000,
    config: CaptureCanaryConfig | None = None,
    pacer_clock: FakePacerClock | None = None,
) -> CanaryRestScheduler:
    clock = pacer_clock or FakePacerClock()
    return CanaryRestScheduler(
        config=_config() if config is None else config,
        adapter=adapter,
        fatal_state=fatal or CaptureFatalState(),
        wall_time_ms=lambda: now_ms,
        monotonic_time=clock.monotonic,
        wait_or_stop=clock.wait_or_stop,
    )


def _entry(role: str) -> CanaryRestRequestPlanEntry:
    return next(item for item in capture_rest_request_plan() if item.role == role)


def _depth_request(
    *,
    event: str = "startup",
    market: Market = Market.SPOT,
    generation: int = 1,
    watermarks: tuple[tuple[str, int], ...] | None = None,
) -> DepthResyncRequest:
    return DepthResyncRequest(
        event=cast(DepthResyncEvent, event),
        market=market,
        generation=generation,
        watermarks=(
            tuple((symbol, 100) for symbol in sorted(CANARY_SYMBOLS))
            if watermarks is None
            else watermarks
        ),
    )


def _prime_depth_request(
    scheduler: CanaryRestScheduler,
    request: DepthResyncRequest,
    *,
    width: int = 10,
) -> None:
    for symbol, first_u in request.watermarks:
        scheduler.notify_depth_range(
            DepthRangeObservation(
                market=request.market,
                symbol=symbol,
                generation=request.generation,
                U=first_u,
                u=first_u + width,
                reset=True,
            )
        )


def _config_with_test_bridge_wait(seconds: float) -> CaptureCanaryConfig:
    config = _config()
    polling = config.polling.model_copy(
        update={"depth_snapshot_bridge_wait_seconds": seconds}
    )
    return config.model_copy(update={"polling": polling})


def test_every_planned_query_is_exact_and_rejects_symbol_drift() -> None:
    for entry in capture_rest_request_plan():
        if "symbol" in entry.allowed_query_keys:
            query = planned_query(entry, symbol="BTCUSDT")
            assert dict(query)["symbol"] == "BTCUSDT"
            with pytest.raises(ValueError, match="exact canary symbols"):
                planned_query(entry, symbol="BNBUSDT")
            with pytest.raises(ValueError, match="exact canary symbols"):
                planned_query(entry, symbol=None)
        else:
            assert planned_query(entry, symbol=None) == entry.fixed_query
            with pytest.raises(ValueError, match="does not accept a symbol"):
                planned_query(entry, symbol="BTCUSDT")


@pytest.mark.asyncio
async def test_capture_rejects_a_structurally_valid_but_nonfrozen_plan_entry() -> None:
    adapter = FakeRestAdapter()
    scheduler = _scheduler(adapter)
    drifted = _entry("spot_venue_time").model_copy(update={"path": "/api/v3/klines"})

    with pytest.raises(ValueError, match="differs from the frozen"):
        await scheduler.capture_entry_once(
            drifted,
            scheduled_at_ms=1_000_000,
        )
    assert adapter.calls == []


@pytest.mark.asyncio
async def test_spot_depth_admissions_use_exact_monotonic_pacing() -> None:
    clock = FakePacerClock()

    class RecordingAdapter(FakeRestAdapter):
        def __init__(self) -> None:
            super().__init__()
            self.sent_at: list[float] = []

        async def capture_attempt(self, **kwargs: object) -> RestEnvelopeV2:
            self.sent_at.append(clock.monotonic())
            return await super().capture_attempt(**kwargs)  # pyright: ignore[reportArgumentType]

    adapter = RecordingAdapter()
    scheduler = _scheduler(adapter, pacer_clock=clock)
    entry = _entry("spot_depth_snapshot")

    await asyncio.gather(
        *(
            scheduler.capture_entry_once(
                entry,
                scheduled_at_ms=1_000_000,
                symbol=symbol,
            )
            for symbol in CANARY_SYMBOLS
        )
    )

    interval = SPOT_DEPTH_SNAPSHOT_MINIMUM_ADMISSION_INTERVAL_SECONDS
    assert adapter.sent_at == pytest.approx([0.0, interval, interval * 2])
    assert clock.delays == pytest.approx([interval, interval])


@pytest.mark.asyncio
async def test_spot_depth_pacing_limits_every_sixty_second_window_to_nineteen() -> None:
    clock = FakePacerClock()

    class RecordingAdapter(FakeRestAdapter):
        def __init__(self) -> None:
            super().__init__()
            self.sent_at: list[float] = []

        async def capture_attempt(self, **kwargs: object) -> RestEnvelopeV2:
            self.sent_at.append(clock.monotonic())
            return await super().capture_attempt(**kwargs)  # pyright: ignore[reportArgumentType]

    adapter = RecordingAdapter()
    adapter.response_headers["spot_depth_snapshot"] = (
        ("x-mbx-used-weight-1m", "1"),
    )
    scheduler = _scheduler(adapter, pacer_clock=clock)
    entry = _entry("spot_depth_snapshot")

    await asyncio.gather(
        *(
            scheduler.capture_entry_once(
                entry,
                scheduled_at_ms=1_000_000,
                symbol=CANARY_SYMBOLS[index % len(CANARY_SYMBOLS)],
            )
            for index in range(20)
        )
    )

    assert len([sent_at for sent_at in adapter.sent_at if sent_at < 60.0]) == 19
    assert adapter.sent_at[18] == pytest.approx(57.6)
    assert adapter.sent_at[19] >= 60.8


@pytest.mark.asyncio
async def test_spot_depth_pacing_is_reserved_after_semaphore_admission() -> None:
    clock = FakePacerClock()

    class BlockingAdapter(FakeRestAdapter):
        def __init__(self) -> None:
            super().__init__()
            self.blocker_entered = asyncio.Event()
            self.release_blocker = asyncio.Event()
            self.depth_sent_at: list[float] = []

        async def capture_attempt(self, **kwargs: object) -> RestEnvelopeV2:
            role = kwargs["request_role"]
            if role == "spot_venue_time":
                self.blocker_entered.set()
                await self.release_blocker.wait()
            elif role == "spot_depth_snapshot":
                self.depth_sent_at.append(clock.monotonic())
            return await super().capture_attempt(
                **kwargs  # pyright: ignore[reportArgumentType]
            )

    adapter = BlockingAdapter()
    scheduler = _scheduler(adapter, pacer_clock=clock)
    scheduler._semaphore = asyncio.Semaphore(1)
    blocker = asyncio.create_task(
        scheduler.capture_entry_once(
            _entry("spot_venue_time"),
            scheduled_at_ms=1_000_000,
        )
    )
    await adapter.blocker_entered.wait()
    depth_entry = _entry("spot_depth_snapshot")
    depth_tasks = [
        asyncio.create_task(
            scheduler.capture_entry_once(
                depth_entry,
                scheduled_at_ms=1_000_000,
                symbol=symbol,
            )
        )
        for symbol in CANARY_SYMBOLS[:2]
    ]
    await asyncio.sleep(0)
    await asyncio.sleep(0)

    adapter.release_blocker.set()
    await asyncio.gather(blocker, *depth_tasks)

    interval = SPOT_DEPTH_SNAPSHOT_MINIMUM_ADMISSION_INTERVAL_SECONDS
    assert adapter.depth_sent_at == pytest.approx([0.0, interval])
    assert clock.delays == pytest.approx([interval])


@pytest.mark.asyncio
async def test_spot_depth_pacing_starts_after_adapter_internal_delay() -> None:
    clock = FakePacerClock()

    class DelayedStartAdapter(FakeRestAdapter):
        def __init__(self) -> None:
            super().__init__()
            self.depth_sent_at: list[float] = []

        async def capture_attempt(self, **kwargs: object) -> RestEnvelopeV2:
            if kwargs["request_role"] == "spot_depth_snapshot":
                if not self.depth_sent_at:
                    # Model adapter-internal cleanup/connection admission after
                    # the scheduler call but before the wire send begins.
                    clock.now += 10.0
                self.depth_sent_at.append(clock.monotonic())
            return await super().capture_attempt(
                **kwargs  # pyright: ignore[reportArgumentType]
            )

    adapter = DelayedStartAdapter()
    scheduler = _scheduler(adapter, pacer_clock=clock)
    entry = _entry("spot_depth_snapshot")

    await asyncio.gather(
        *(
            scheduler.capture_entry_once(
                entry,
                scheduled_at_ms=1_000_000,
                symbol=symbol,
            )
            for symbol in CANARY_SYMBOLS[:2]
        )
    )

    interval = SPOT_DEPTH_SNAPSHOT_MINIMUM_ADMISSION_INTERVAL_SECONDS
    assert adapter.depth_sent_at == pytest.approx([10.0, 10.0 + interval])
    assert clock.delays == pytest.approx([interval])


@pytest.mark.asyncio
async def test_spot_depth_pacer_stop_does_not_delay_other_rest_roles() -> None:
    class StopBlockingPacerClock(FakePacerClock):
        def __init__(self) -> None:
            super().__init__()
            self.waiting = asyncio.Event()

        async def wait_or_stop(self, stop_event: asyncio.Event, delay: float) -> bool:
            self.delays.append(delay)
            self.waiting.set()
            return await stop_event.wait()

    clock = StopBlockingPacerClock()
    adapter = FakeRestAdapter()
    fatal = CaptureFatalState()
    scheduler = _scheduler(adapter, fatal=fatal, pacer_clock=clock)
    spot_depth = _entry("spot_depth_snapshot")
    await scheduler.capture_entry_once(
        spot_depth,
        scheduled_at_ms=1_000_000,
        symbol="BTCUSDT",
    )
    waiting_depth = asyncio.create_task(
        scheduler.capture_entry_once(
            spot_depth,
            scheduled_at_ms=1_000_000,
            symbol="ETHUSDT",
        )
    )
    await asyncio.wait_for(clock.waiting.wait(), timeout=1)

    await asyncio.gather(
        scheduler.capture_entry_once(
            _entry("spot_venue_time"),
            scheduled_at_ms=1_000_000,
        ),
        scheduler.capture_entry_once(
            _entry("futures_depth_snapshot"),
            scheduled_at_ms=1_000_000,
            symbol="BTCUSDT",
        ),
    )

    assert waiting_depth.done() is False
    assert str(adapter.calls[0]["role"]) == "spot_depth_snapshot"
    assert {str(call["role"]) for call in adapter.calls[1:]} == {
        "spot_venue_time",
        "futures_depth_snapshot",
    }
    fatal.stop_event.set()
    with pytest.raises(asyncio.CancelledError):
        await waiting_depth
    assert len(adapter.calls) == 3
    assert fatal.failed is False


@pytest.mark.asyncio
async def test_startup_depth_requests_capture_each_exact_market_symbol_set() -> None:
    adapter = FakeRestAdapter()
    scheduler = _scheduler(adapter)
    spot_request = _depth_request()
    futures_request = _depth_request(market=Market.FUTURES)
    _prime_depth_request(scheduler, spot_request)
    _prime_depth_request(scheduler, futures_request)

    spot = await scheduler.handle_depth_event(spot_request)
    futures = await scheduler.handle_depth_event(futures_request)

    assert len(spot) == len(futures) == 3
    assert adapter.peak_active == 3
    assert {str(call["role"]) for call in adapter.calls} == {
        "spot_depth_snapshot",
        "futures_depth_snapshot",
    }
    queries = [dict(cast(Sequence[tuple[str, str]], call["query"])) for call in adapter.calls]
    assert {query["symbol"] for query in queries} == set(CANARY_SYMBOLS)
    limits_by_market = {
        cast(Market, call["market"]): dict(cast(Sequence[tuple[str, str]], call["query"]))["limit"]
        for call in adapter.calls
    }
    assert limits_by_market == {
        Market.SPOT: "5000",
        Market.FUTURES: "1000",
    }
    assert all(call["method"] == "GET" and call["headers"] is None for call in adapter.calls)


def test_depth_range_coordinator_has_six_fixed_states_and_overflow_fails_closed() -> None:
    adapter = FakeRestAdapter()
    fatal = CaptureFatalState()
    scheduler = _scheduler(adapter, fatal=fatal)
    assert scheduler.depth_state_count == 6

    scheduler.notify_depth_range(
        DepthRangeObservation(Market.SPOT, "BTCUSDT", 1, 1, 1, True)
    )
    for update_id in range(2, 1_025):
        scheduler.notify_depth_range(
            DepthRangeObservation(
                Market.SPOT,
                "BTCUSDT",
                1,
                update_id,
                update_id,
                False,
            )
        )
    assert scheduler.buffered_depth_range_count == 1_024

    with pytest.raises(CaptureRestScheduleOverflow, match="range buffer overflowed"):
        scheduler.notify_depth_range(
            DepthRangeObservation(Market.SPOT, "BTCUSDT", 1, 1_025, 1_025, False)
        )
    assert fatal.failed is True


def test_new_depth_generation_reset_replaces_older_buffer() -> None:
    scheduler = _scheduler(FakeRestAdapter())
    scheduler.notify_depth_range(
        DepthRangeObservation(Market.SPOT, "ETHUSDT", 1, 10, 12, True)
    )
    scheduler.notify_depth_range(
        DepthRangeObservation(Market.SPOT, "ETHUSDT", 1, 13, 15, False)
    )
    scheduler.notify_depth_range(
        DepthRangeObservation(Market.SPOT, "ETHUSDT", 2, 100, 102, True)
    )
    scheduler.notify_depth_range(
        DepthRangeObservation(Market.SPOT, "ETHUSDT", 1, 16, 18, True)
    )

    assert scheduler.buffered_depth_range_count == 1


@pytest.mark.asyncio
async def test_snapshot_ahead_waits_and_wakes_on_a_bridging_range() -> None:
    adapter = FakeRestAdapter()
    adapter.responses["spot_depth_snapshot"] = [
        (200, '{"lastUpdateId":100,"bids":[],"asks":[]}')
    ]
    scheduler = _scheduler(adapter)
    request = _depth_request(
        event="sequence_gap",
        watermarks=(("BTCUSDT", 90),),
    )
    _prime_depth_request(scheduler, request, width=5)
    task = asyncio.create_task(scheduler.handle_depth_event(request))
    for _index in range(100):
        if adapter.calls:
            break
        await asyncio.sleep(0.001)
    assert len(adapter.calls) == 1 and task.done() is False

    scheduler.notify_depth_range(
        DepthRangeObservation(Market.SPOT, "BTCUSDT", 1, 96, 105, False)
    )
    [accepted] = await asyncio.wait_for(task, timeout=1)

    assert accepted.raw_payload.startswith('{"lastUpdateId":100')
    assert len(adapter.calls) == 1
    assert scheduler.buffered_depth_range_count == 0


@pytest.mark.asyncio
async def test_spot_discards_u_equal_snapshot_then_accepts_successor_same_cycle() -> None:
    adapter = FakeRestAdapter()
    adapter.responses["spot_depth_snapshot"] = [
        (200, '{"lastUpdateId":100,"bids":[],"asks":[]}'),
        (200, '{"lastUpdateId":101,"bids":[],"asks":[]}'),
    ]
    scheduler = _scheduler(adapter)
    request = _depth_request(
        event="sequence_gap",
        watermarks=(("BTCUSDT", 90),),
    )
    _prime_depth_request(scheduler, request, width=10)
    task = asyncio.create_task(scheduler.handle_depth_event(request))
    for _index in range(100):
        if adapter.calls:
            break
        await asyncio.sleep(0.001)
    assert len(adapter.calls) == 1 and task.done() is False

    scheduler.notify_depth_range(
        DepthRangeObservation(Market.SPOT, "BTCUSDT", 1, 101, 105, False)
    )
    [accepted] = await asyncio.wait_for(task, timeout=1)

    assert '"lastUpdateId":100' in accepted.raw_payload
    assert len(adapter.calls) == 1


@pytest.mark.asyncio
async def test_online_bridge_accepts_failed_smoke_spot_successor() -> None:
    last_update_id = 78_896_562_817
    adapter = FakeRestAdapter()
    adapter.responses["spot_depth_snapshot"] = [
        (
            200,
            '{"lastUpdateId":78896562817,"bids":[],"asks":[]}',
        )
    ]
    scheduler = _scheduler(adapter)
    request = _depth_request(
        event="sequence_gap",
        watermarks=(("ETHUSDT", 78_896_562_791),),
    )
    scheduler.notify_depth_range(
        DepthRangeObservation(
            Market.SPOT,
            "ETHUSDT",
            1,
            78_896_562_791,
            last_update_id,
            True,
        )
    )
    scheduler.notify_depth_range(
        DepthRangeObservation(
            Market.SPOT,
            "ETHUSDT",
            1,
            last_update_id + 1,
            78_896_562_832,
            False,
        )
    )

    [accepted] = await scheduler.handle_depth_event(request)

    assert f'"lastUpdateId":{last_update_id}' in accepted.raw_payload
    assert len(adapter.calls) == 1
    assert scheduler.buffered_depth_range_count == 0


@pytest.mark.asyncio
async def test_futures_u_equal_snapshot_successor_is_stale_until_next_cycle() -> None:
    adapter = FakeRestAdapter()
    adapter.responses["futures_depth_snapshot"] = [
        (200, '{"lastUpdateId":100,"bids":[],"asks":[]}'),
        (200, '{"lastUpdateId":101,"bids":[],"asks":[]}'),
    ]
    scheduler = _scheduler(adapter)
    request = _depth_request(
        event="sequence_gap",
        market=Market.FUTURES,
        watermarks=(("BTCUSDT", 101),),
    )
    _prime_depth_request(scheduler, request, width=4)

    [accepted] = await scheduler.handle_depth_event(request)

    assert '"lastUpdateId":101' in accepted.raw_payload
    assert len(adapter.calls) == 2


@pytest.mark.asyncio
async def test_futures_gap_u_equal_snapshot_accepts_first_cycle() -> None:
    adapter = FakeRestAdapter()
    adapter.responses["futures_depth_snapshot"] = [
        (200, '{"lastUpdateId":100,"bids":[],"asks":[]}')
    ]
    scheduler = _scheduler(adapter)
    request = _depth_request(
        event="sequence_gap",
        market=Market.FUTURES,
        watermarks=(("BTCUSDT", 90),),
    )
    _prime_depth_request(scheduler, request, width=10)

    [accepted] = await scheduler.handle_depth_event(request)

    assert '"lastUpdateId":100' in accepted.raw_payload
    assert len(adapter.calls) == 1


@pytest.mark.asyncio
async def test_waiting_old_request_is_superseded_without_clearing_newer_resync() -> None:
    adapter = FakeRestAdapter()
    adapter.responses["spot_depth_snapshot"] = [
        (200, '{"lastUpdateId":100,"bids":[],"asks":[]}'),
        (200, '{"lastUpdateId":200,"bids":[],"asks":[]}'),
    ]
    fatal = CaptureFatalState()
    scheduler = _scheduler(adapter, fatal=fatal)
    old_request = _depth_request(
        event="sequence_gap",
        watermarks=(("BTCUSDT", 90),),
    )
    _prime_depth_request(scheduler, old_request, width=5)
    old_task = asyncio.create_task(scheduler.handle_depth_event(old_request))
    for _index in range(100):
        if adapter.calls:
            break
        await asyncio.sleep(0.001)

    scheduler.notify_depth_range(
        DepthRangeObservation(Market.SPOT, "BTCUSDT", 1, 200, 210, True)
    )
    [old_snapshot] = await asyncio.wait_for(old_task, timeout=1)
    assert '"lastUpdateId":100' in old_snapshot.raw_payload
    assert scheduler.buffered_depth_range_count == 1
    assert fatal.failed is False

    newer_request = _depth_request(
        event="sequence_gap",
        watermarks=(("BTCUSDT", 200),),
    )
    [new_snapshot] = await scheduler.handle_depth_event(newer_request)

    assert '"lastUpdateId":200' in new_snapshot.raw_payload
    assert scheduler.buffered_depth_range_count == 0
    assert fatal.failed is False


@pytest.mark.asyncio
async def test_ahead_snapshot_wait_timeout_exhausts_three_cycles() -> None:
    adapter = FakeRestAdapter()
    adapter.responses["spot_depth_snapshot"] = [
        (200, '{"lastUpdateId":100,"bids":[],"asks":[]}'),
        (200, '{"lastUpdateId":100,"bids":[],"asks":[]}'),
        (200, '{"lastUpdateId":100,"bids":[],"asks":[]}'),
    ]
    fatal = CaptureFatalState()
    scheduler = _scheduler(
        adapter,
        fatal=fatal,
        config=_config_with_test_bridge_wait(0.01),
    )
    request = _depth_request(
        event="sequence_gap",
        watermarks=(("SOLUSDT", 90),),
    )
    _prime_depth_request(scheduler, request, width=5)

    with pytest.raises(CaptureRestSchedulerFailure, match="bridge timeout"):
        await scheduler.handle_depth_event(request)

    assert len(adapter.calls) == 3
    assert fatal.failed is True


@pytest.mark.asyncio
async def test_synced_depth_book_does_not_grow_operational_buffer() -> None:
    scheduler = _scheduler(FakeRestAdapter())
    request = _depth_request(
        event="sequence_gap",
        market=Market.FUTURES,
        watermarks=(("SOLUSDT", 100),),
    )
    _prime_depth_request(scheduler, request)
    await scheduler.handle_depth_event(request)
    assert scheduler.buffered_depth_range_count == 0

    for update_id in range(111, 150):
        scheduler.notify_depth_range(
            DepthRangeObservation(
                Market.FUTURES,
                "SOLUSDT",
                1,
                update_id,
                update_id,
                False,
            )
        )
    assert scheduler.buffered_depth_range_count == 0


@pytest.mark.asyncio
async def test_sequence_gap_depth_request_captures_only_affected_symbol() -> None:
    adapter = FakeRestAdapter()
    scheduler = _scheduler(adapter)
    request = _depth_request(
        event="sequence_gap",
        market=Market.FUTURES,
        watermarks=(("ETHUSDT", 100),),
    )
    _prime_depth_request(scheduler, request)

    [envelope] = await scheduler.handle_depth_event(
        request
    )

    assert envelope.request_role == "futures_depth_snapshot"
    assert len(adapter.calls) == 1
    query = dict(cast(Sequence[tuple[str, str]], adapter.calls[0]["query"]))
    assert query["symbol"] == "ETHUSDT"


@pytest.mark.asyncio
async def test_depth_snapshot_accepts_last_update_id_equal_to_first_buffered_u() -> None:
    adapter = FakeRestAdapter()
    adapter.responses["spot_depth_snapshot"] = [
        (200, '{"lastUpdateId":100,"bids":[],"asks":[]}')
    ]
    scheduler = _scheduler(adapter)
    request = _depth_request(
        event="sequence_gap",
        watermarks=(("BTCUSDT", 100),),
    )
    _prime_depth_request(scheduler, request)

    [envelope] = await scheduler.handle_depth_event(request)

    assert envelope.request_started_monotonic_ns > 0
    assert len(adapter.calls) == 1


@pytest.mark.asyncio
async def test_stale_depth_snapshots_retry_then_accept_and_preserve_every_attempt() -> None:
    adapter = FakeRestAdapter()
    adapter.responses["spot_depth_snapshot"] = [
        (200, '{"lastUpdateId":97,"bids":[],"asks":[]}'),
        (200, '{"lastUpdateId":98,"bids":[],"asks":[]}'),
        (200, '{"lastUpdateId":99,"bids":[],"asks":[]}'),
    ]
    scheduler = _scheduler(adapter)
    request = _depth_request(
        event="sequence_gap",
        watermarks=(("SOLUSDT", 100),),
    )
    _prime_depth_request(scheduler, request)

    [accepted] = await scheduler.handle_depth_event(request)

    assert accepted.raw_payload == '{"lastUpdateId":99,"bids":[],"asks":[]}'
    assert len(adapter.calls) == 3
    assert len({str(call["correlation_id"]) for call in adapter.calls}) == 3


@pytest.mark.asyncio
async def test_three_stale_depth_snapshots_exhaust_and_quarantine() -> None:
    adapter = FakeRestAdapter()
    adapter.responses["spot_depth_snapshot"] = [
        (200, '{"lastUpdateId":98,"bids":[],"asks":[]}'),
        (200, '{"lastUpdateId":98,"bids":[],"asks":[]}'),
        (200, '{"lastUpdateId":98,"bids":[],"asks":[]}'),
    ]
    fatal = CaptureFatalState()
    scheduler = _scheduler(adapter, fatal=fatal)
    request = _depth_request(
        event="sequence_gap",
        watermarks=(("BTCUSDT", 100),),
    )
    _prime_depth_request(scheduler, request)

    with pytest.raises(CaptureRestSchedulerFailure, match="bridge stale"):
        await scheduler.handle_depth_event(request)

    assert len(adapter.calls) == 3
    assert fatal.failed is True


@pytest.mark.parametrize(
    "body",
    [
        pytest.param("{}", id="missing"),
        pytest.param("[]", id="root"),
        pytest.param('{"lastUpdateId":1,"asks":[]}', id="missing-bids"),
        pytest.param(
            '{"lastUpdateId":1,"bids":{},"asks":[]}',
            id="non-array-bids",
        ),
        pytest.param(
            '{"lastUpdateId":true,"bids":[],"asks":[]}',
            id="boolean",
        ),
        pytest.param(
            '{"lastUpdateId":-1,"bids":[],"asks":[]}',
            id="negative",
        ),
        pytest.param("not-json", id="json"),
    ],
)
@pytest.mark.asyncio
async def test_malformed_2xx_depth_snapshot_is_fatal(body: str) -> None:
    adapter = FakeRestAdapter()
    adapter.responses["spot_depth_snapshot"] = [(200, body)]
    fatal = CaptureFatalState()
    scheduler = _scheduler(adapter, fatal=fatal)
    request = _depth_request(
        event="sequence_gap",
        watermarks=(("BTCUSDT", 100),),
    )
    _prime_depth_request(scheduler, request)

    with pytest.raises(CaptureRestSchedulerFailure, match="malformed 2xx"):
        await scheduler.handle_depth_event(request)

    assert len(adapter.calls) == 1
    assert fatal.failed is True


@pytest.mark.asyncio
async def test_bounded_depth_snapshot_http_failure_is_fatal() -> None:
    adapter = FakeRestAdapter()
    adapter.responses["futures_depth_snapshot"] = [(None, "")]
    fatal = CaptureFatalState()
    scheduler = _scheduler(adapter, fatal=fatal)
    request = _depth_request(
        event="sequence_gap",
        market=Market.FUTURES,
        watermarks=(("ETHUSDT", 100),),
    )
    _prime_depth_request(scheduler, request)

    with pytest.raises(CaptureRestSchedulerFailure, match="HTTP/transport"):
        await scheduler.handle_depth_event(request)

    assert len(adapter.calls) == 1
    assert fatal.failed is True


@pytest.mark.asyncio
async def test_partial_content_depth_snapshot_is_fatal() -> None:
    adapter = FakeRestAdapter()
    adapter.responses["spot_depth_snapshot"] = [
        (206, '{"lastUpdateId":100,"bids":[],"asks":[]}')
    ]
    fatal = CaptureFatalState()
    scheduler = _scheduler(adapter, fatal=fatal)
    request = _depth_request(
        event="sequence_gap",
        watermarks=(("BTCUSDT", 100),),
    )
    _prime_depth_request(scheduler, request)

    with pytest.raises(CaptureRestSchedulerFailure, match="non-200 HTTP/transport"):
        await scheduler.handle_depth_event(request)

    assert fatal.failed is True


@pytest.mark.asyncio
async def test_scheduler_rejects_nonexact_generation_request_before_network() -> None:
    adapter = FakeRestAdapter()
    scheduler = _scheduler(adapter)
    incomplete = _depth_request(
        watermarks=(("BTCUSDT", 100), ("ETHUSDT", 100)),
    )

    with pytest.raises(ValueError, match="exact canary symbols"):
        await scheduler.handle_depth_event(incomplete)
    assert adapter.calls == []


@pytest.mark.asyncio
async def test_premium_index_schedules_one_bounded_funding_confirmation() -> None:
    adapter = FakeRestAdapter()
    adapter.responses["futures_premium_index"] = [(200, '{"nextFundingTime":1000000}')]
    adapter.responses["futures_funding_rate_confirmation"] = [
        (None, ""),
        (200, "[]"),
    ]
    scheduler = _scheduler(adapter, now_ms=1_015_000)

    await scheduler.capture_entry_once(
        _entry("futures_premium_index"),
        scheduled_at_ms=900_000,
        symbol="BTCUSDT",
    )
    [confirmation] = await scheduler.capture_due_funding_confirmations(1_015_000)
    assert confirmation.response_status == 200
    funding_calls = [
        call for call in adapter.calls if call["role"] == "futures_funding_rate_confirmation"
    ]
    assert [call["attempt"] for call in funding_calls] == [1, 2]
    assert await scheduler.capture_due_funding_confirmations(1_016_000) == ()

    await scheduler.capture_entry_once(
        _entry("futures_premium_index"),
        scheduled_at_ms=1_020_000,
        symbol="BTCUSDT",
    )
    assert await scheduler.capture_due_funding_confirmations(1_030_000) == ()


@pytest.mark.asyncio
async def test_funding_confirmation_uses_exact_15_second_millisecond_boundary() -> None:
    adapter = FakeRestAdapter()
    adapter.responses["futures_premium_index"] = [(200, '{"nextFundingTime":1000000}')]
    scheduler = _scheduler(adapter)

    await scheduler.capture_entry_once(
        _entry("futures_premium_index"),
        scheduled_at_ms=900_000,
        symbol="BTCUSDT",
    )
    assert await scheduler.capture_due_funding_confirmations(1_014_999) == ()
    [confirmation] = await scheduler.capture_due_funding_confirmations(1_015_000)
    assert confirmation.request_role == "futures_funding_rate_confirmation"


@pytest.mark.asyncio
async def test_funding_rollover_preserves_old_and_new_pending_events() -> None:
    first = 1_000_000
    second = first + 8 * 60 * 60 * 1_000
    adapter = FakeRestAdapter()
    adapter.responses["futures_premium_index"] = [
        (200, f'{{"nextFundingTime":{first}}}'),
        (200, f'{{"nextFundingTime":{second}}}'),
    ]
    scheduler = _scheduler(adapter)

    for scheduled in (900_000, 999_000):
        await scheduler.capture_entry_once(
            _entry("futures_premium_index"),
            scheduled_at_ms=scheduled,
            symbol="ETHUSDT",
        )
    [old_confirmation] = await scheduler.capture_due_funding_confirmations(first + 15_000)
    [new_confirmation] = await scheduler.capture_due_funding_confirmations(second + 15_000)

    assert old_confirmation.correlation_id != new_confirmation.correlation_id
    assert (
        len([call for call in adapter.calls if call["role"] == "futures_funding_rate_confirmation"])
        == 2
    )


@pytest.mark.asyncio
async def test_third_unconfirmed_funding_event_fails_bounded_queue_closed() -> None:
    adapter = FakeRestAdapter()
    adapter.responses["futures_premium_index"] = [
        (200, '{"nextFundingTime":1000000}'),
        (200, '{"nextFundingTime":2000000}'),
        (200, '{"nextFundingTime":3000000}'),
    ]
    fatal = CaptureFatalState()
    scheduler = _scheduler(adapter, fatal=fatal)

    for scheduled in (800_000, 900_000):
        await scheduler.capture_entry_once(
            _entry("futures_premium_index"),
            scheduled_at_ms=scheduled,
            symbol="SOLUSDT",
        )
    with pytest.raises(CaptureRestScheduleOverflow, match="funding-confirmation"):
        await scheduler.capture_entry_once(
            _entry("futures_premium_index"),
            scheduled_at_ms=950_000,
            symbol="SOLUSDT",
        )
    assert fatal.failed is True


@pytest.mark.asyncio
async def test_failed_funding_confirmation_is_not_retried_forever() -> None:
    adapter = FakeRestAdapter()
    adapter.responses["futures_premium_index"] = [
        (200, '{"nextFundingTime":1000000}'),
        (200, '{"nextFundingTime":1000000}'),
    ]
    adapter.responses["futures_funding_rate_confirmation"] = [
        (None, ""),
        (None, ""),
    ]
    scheduler = _scheduler(adapter, now_ms=1_015_000)

    await scheduler.capture_entry_once(
        _entry("futures_premium_index"),
        scheduled_at_ms=900_000,
        symbol="ETHUSDT",
    )
    [failed] = await scheduler.capture_due_funding_confirmations(1_015_000)
    assert failed.error_category is RestErrorCategory.NETWORK
    await scheduler.capture_entry_once(
        _entry("futures_premium_index"),
        scheduled_at_ms=1_020_000,
        symbol="ETHUSDT",
    )
    assert await scheduler.capture_due_funding_confirmations(1_030_000) == ()
    assert (
        len([call for call in adapter.calls if call["role"] == "futures_funding_rate_confirmation"])
        == 2
    )


@pytest.mark.asyncio
async def test_body_limit_is_preserved_once_then_immediately_quarantined() -> None:
    class BodyLimitAdapter(FakeRestAdapter):
        async def capture_attempt(self, **kwargs: object) -> RestEnvelopeV2:
            envelope = await super().capture_attempt(
                **kwargs  # pyright: ignore[reportArgumentType]
            )
            return replace(
                envelope,
                payload_complete=False,
                error_category=RestErrorCategory.BODY_LIMIT,
                error_detail="response body exceeded bounded capture limit",
            )

    adapter = BodyLimitAdapter()
    fatal = CaptureFatalState()
    scheduler = _scheduler(adapter, fatal=fatal)

    with pytest.raises(CaptureRestSchedulerFailure, match="body-limit"):
        await scheduler.capture_entry_once(
            _entry("futures_funding_rate_confirmation"),
            scheduled_at_ms=1_000_000,
            symbol="BTCUSDT",
        )

    assert [call["attempt"] for call in adapter.calls] == [1]
    assert fatal.failed is True


@pytest.mark.asyncio
async def test_spot_used_weight_4999_is_allowed_but_5000_quarantines() -> None:
    allowed_adapter = FakeRestAdapter()
    allowed_adapter.response_headers["spot_venue_time"] = (
        ("x-mbx-used-weight-1m", "4999"),
    )
    allowed_fatal = CaptureFatalState()
    allowed_scheduler = _scheduler(allowed_adapter, fatal=allowed_fatal)

    allowed = await allowed_scheduler.capture_entry_once(
        _entry("spot_venue_time"),
        scheduled_at_ms=1_000_000,
    )

    assert allowed.response_status == 200
    assert allowed_fatal.failed is False

    quarantined_adapter = FakeRestAdapter()
    quarantined_adapter.response_headers["spot_venue_time"] = (
        ("x-mbx-used-weight-1m", "5000"),
    )
    quarantined_fatal = CaptureFatalState()
    quarantined_scheduler = _scheduler(quarantined_adapter, fatal=quarantined_fatal)

    with pytest.raises(CaptureRestSchedulerFailure, match="high-water"):
        await quarantined_scheduler.capture_entry_once(
            _entry("spot_venue_time"),
            scheduled_at_ms=1_000_000,
        )
    assert len(quarantined_adapter.calls) == 1
    assert quarantined_fatal.failed is True


@pytest.mark.parametrize(
    "headers",
    [
        pytest.param((), id="missing"),
        pytest.param(
            (
                ("x-mbx-used-weight-1m", "1"),
                ("x-mbx-used-weight-1m", "2"),
            ),
            id="duplicate",
        ),
        pytest.param(
            (("x-mbx-used-weight-1m", "1.0"),),
            id="non-integer",
        ),
        pytest.param(
            (("x-mbx-used-weight-1m", "-1"),),
            id="negative",
        ),
        pytest.param(
            (("x-mbx-used-weight-1m", "01"),),
            id="non-canonical",
        ),
    ],
)
@pytest.mark.asyncio
async def test_successful_spot_response_requires_exact_used_weight_header(
    headers: tuple[tuple[str, str], ...],
) -> None:
    adapter = FakeRestAdapter()
    adapter.response_headers["spot_venue_time"] = headers
    fatal = CaptureFatalState()
    scheduler = _scheduler(adapter, fatal=fatal)

    with pytest.raises(CaptureRestSchedulerFailure, match="x-mbx-used-weight-1m"):
        await scheduler.capture_entry_once(
            _entry("spot_venue_time"),
            scheduled_at_ms=1_000_000,
        )

    assert len(adapter.calls) == 1
    assert fatal.failed is True


@pytest.mark.parametrize(
    "body",
    [
        pytest.param('{"rateLimits":[]}', id="missing"),
        pytest.param(
            '{"rateLimits":['
            '{"rateLimitType":"REQUEST_WEIGHT","interval":"MINUTE",'
            '"intervalNum":1,"limit":6000},'
            '{"rateLimitType":"REQUEST_WEIGHT","interval":"MINUTE",'
            '"intervalNum":1,"limit":6000}'
            "]}",
            id="duplicate",
        ),
        pytest.param(
            '{"rateLimits":[{"rateLimitType":"REQUEST_WEIGHT",'
            '"interval":"MINUTE","intervalNum":true,"limit":6000}]}',
            id="boolean-interval",
        ),
        pytest.param(
            '{"rateLimits":[{"rateLimitType":"REQUEST_WEIGHT",'
            '"interval":"MINUTE","intervalNum":1,"limit":5999}]}',
            id="limit-drift",
        ),
    ],
)
@pytest.mark.asyncio
async def test_spot_exchange_info_request_weight_contract_drift_is_fatal(
    body: str,
) -> None:
    adapter = FakeRestAdapter()
    adapter.responses["spot_exchange_info"] = [(200, body)]
    fatal = CaptureFatalState()
    scheduler = _scheduler(adapter, fatal=fatal)

    with pytest.raises(CaptureRestSchedulerFailure, match="REQUEST_WEIGHT/MINUTE/1"):
        await scheduler.capture_entry_once(
            _entry("spot_exchange_info"),
            scheduled_at_ms=1_000_000,
        )

    assert len(adapter.calls) == 1
    assert fatal.failed is True


@pytest.mark.asyncio
async def test_spot_exchange_info_allows_future_status_and_new_unique_filter_type() -> None:
    payload = _spot_exchange_info_payload()
    symbols = cast(list[dict[str, object]], payload["symbols"])
    symbols.reverse()
    symbols[0]["status"] = "FUTURE_STATUS"
    filters = cast(list[dict[str, object]], symbols[0]["filters"])
    filters.append({"filterType": "FUTURE_FILTER"})
    adapter = FakeRestAdapter()
    adapter.responses["spot_exchange_info"] = [
        (200, json.dumps(payload, separators=(",", ":")))
    ]
    fatal = CaptureFatalState()
    scheduler = _scheduler(adapter, fatal=fatal)

    result = await scheduler.capture_entry_once(
        _entry("spot_exchange_info"),
        scheduled_at_ms=1_000_000,
    )

    assert result.response_status == 200
    assert fatal.failed is False


@pytest.mark.parametrize(
    "case",
    [
        "missing-symbol",
        "extra-symbol",
        "duplicate-symbol",
        "non-string-symbol",
        "invalid-status",
        "non-array-filters",
        "duplicate-filter-type",
        "missing-required-filter",
    ],
)
@pytest.mark.asyncio
async def test_spot_exchange_info_exact_symbol_and_filter_schema_is_required(
    case: str,
) -> None:
    payload = _spot_exchange_info_payload()
    symbols = cast(list[dict[str, object]], payload["symbols"])
    if case == "missing-symbol":
        symbols.pop()
    elif case == "extra-symbol":
        symbols.append(_spot_exchange_info_symbol_row("BNBUSDT"))
    elif case == "duplicate-symbol":
        symbols[-1] = _spot_exchange_info_symbol_row("BTCUSDT")
    elif case == "non-string-symbol":
        symbols[0]["symbol"] = 1
    elif case == "invalid-status":
        symbols[0]["status"] = 1
    elif case == "non-array-filters":
        symbols[0]["filters"] = {}
    else:
        filters = cast(list[dict[str, object]], symbols[0]["filters"])
        if case == "duplicate-filter-type":
            filters.append({"filterType": "PRICE_FILTER"})
        else:
            symbols[0]["filters"] = [
                item for item in filters if item["filterType"] != "NOTIONAL"
            ]
    adapter = FakeRestAdapter()
    adapter.responses["spot_exchange_info"] = [
        (200, json.dumps(payload, separators=(",", ":")))
    ]
    fatal = CaptureFatalState()
    scheduler = _scheduler(adapter, fatal=fatal)

    with pytest.raises(CaptureRestSchedulerFailure, match="exact canary-symbol schema"):
        await scheduler.capture_entry_once(
            _entry("spot_exchange_info"),
            scheduled_at_ms=1_000_000,
        )

    assert len(adapter.calls) == 1
    assert fatal.failed is True


@pytest.mark.asyncio
async def test_http_418_is_captured_then_trips_shared_fatal_state() -> None:
    adapter = FakeRestAdapter()
    adapter.responses["spot_venue_time"] = [(418, "banned")]
    fatal = CaptureFatalState()
    scheduler = _scheduler(adapter, fatal=fatal)

    with pytest.raises(CaptureRestSchedulerFailure, match="418"):
        await scheduler.capture_entry_once(
            _entry("spot_venue_time"),
            scheduled_at_ms=1_000_000,
        )

    assert fatal.failed is True
    assert len(adapter.calls) == 1


@pytest.mark.asyncio
async def test_batch_fatal_reaps_inflight_and_prevents_queued_sibling_sends() -> None:
    class BlockingAdapter(FakeRestAdapter):
        def __init__(self) -> None:
            super().__init__()
            self.release = asyncio.Event()
            self.entered = 0

        async def capture_attempt(self, **kwargs: object) -> RestEnvelopeV2:
            self.entered += 1
            role = str(kwargs["request_role"])
            if self.entered == 1:
                self.responses[role] = [(418, "banned")]
            else:
                await self.release.wait()
            return await super().capture_attempt(**kwargs)  # pyright: ignore[reportArgumentType]

    adapter = BlockingAdapter()
    fatal = CaptureFatalState()
    scheduler = _scheduler(adapter, fatal=fatal)
    scheduler._inflight_drain_timeout_seconds = 0.05
    request = _depth_request(market=Market.FUTURES)
    _prime_depth_request(scheduler, request)
    task = asyncio.create_task(scheduler.handle_depth_event(request))

    await asyncio.wait_for(fatal.failed_event.wait(), timeout=1)
    assert adapter.entered == len(CANARY_SYMBOLS)
    assert task.done() is False
    adapter.release.set()
    with pytest.raises(CaptureRestSchedulerFailure, match="418"):
        await asyncio.wait_for(task, timeout=1)

    assert len(adapter.calls) <= len(CANARY_SYMBOLS)
    assert fatal.failed is True


@pytest.mark.asyncio
async def test_parent_cancel_cannot_erase_a_known_418_during_sibling_drain() -> None:
    class BlockingAdapter(FakeRestAdapter):
        def __init__(self) -> None:
            super().__init__()
            self.entered = 0
            self.release = asyncio.Event()

        async def capture_attempt(self, **kwargs: object) -> RestEnvelopeV2:
            self.entered += 1
            role = str(kwargs["request_role"])
            if self.entered == 1:
                self.responses[role] = [(418, "banned")]
            else:
                await self.release.wait()
            return await super().capture_attempt(**kwargs)  # pyright: ignore[reportArgumentType]

    adapter = BlockingAdapter()
    fatal = CaptureFatalState()
    scheduler = _scheduler(adapter, fatal=fatal)
    request = _depth_request(market=Market.FUTURES)
    _prime_depth_request(scheduler, request)
    task = asyncio.create_task(scheduler.handle_depth_event(request))
    for _index in range(100):
        if scheduler._local_failure is not None:
            break
        await asyncio.sleep(0.001)
    assert isinstance(scheduler._local_failure, CaptureRestSchedulerFailure)
    assert fatal.failed is False

    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    assert fatal.failed is True
    assert fatal.failure is not None
    assert fatal.failure.cause is scheduler._local_failure


@pytest.mark.asyncio
async def test_queued_request_rechecks_new_429_embargo_inside_semaphore() -> None:
    class GateAdapter(FakeRestAdapter):
        def __init__(self) -> None:
            super().__init__()
            self.first_entered = asyncio.Event()
            self.release_first = asyncio.Event()
            self.sent_at: list[float] = []

        async def capture_attempt(self, **kwargs: object) -> RestEnvelopeV2:
            self.sent_at.append(asyncio.get_running_loop().time())
            role = str(kwargs["request_role"])
            if len(self.sent_at) == 1:
                self.first_entered.set()
                await self.release_first.wait()
                self.responses[role] = [(429, "limited")]
                self.response_headers[role] = (("retry-after", "0.05"),)
            envelope = await super().capture_attempt(
                **kwargs  # pyright: ignore[reportArgumentType]
            )
            if len(self.sent_at) == 1:
                self.response_headers.pop(role, None)
            return envelope

    adapter = GateAdapter()
    scheduler = _scheduler(adapter)
    scheduler._semaphore = asyncio.Semaphore(1)
    entry = _entry("spot_venue_time")
    first = asyncio.create_task(scheduler.capture_entry_once(entry, scheduled_at_ms=1_000_000))
    await adapter.first_entered.wait()
    second = asyncio.create_task(scheduler.capture_entry_once(entry, scheduled_at_ms=1_000_000))
    await asyncio.sleep(0)
    released_at = asyncio.get_running_loop().time()
    adapter.release_first.set()
    await asyncio.gather(first, second)

    assert adapter.sent_at[1] - released_at >= 0.045
    assert len({str(call["correlation_id"]) for call in adapter.calls}) == 2


@pytest.mark.asyncio
async def test_retry_after_beyond_bound_fails_closed_after_capture() -> None:
    adapter = FakeRestAdapter()
    adapter.responses["spot_venue_time"] = [(429, "limited")]
    adapter.response_headers["spot_venue_time"] = (("retry-after", "31"),)
    fatal = CaptureFatalState()
    scheduler = _scheduler(adapter, fatal=fatal)

    with pytest.raises(CaptureRestSchedulerFailure, match="Retry-After"):
        await scheduler.capture_entry_once(_entry("spot_venue_time"), scheduled_at_ms=1_000_000)
    assert fatal.failed is True
    assert len(adapter.calls) == 1


@pytest.mark.parametrize(
    "headers",
    [
        (),
        (("retry-after", "not-a-number"),),
        (("retry-after", "1"), ("retry-after", "2")),
    ],
)
@pytest.mark.asyncio
async def test_ambiguous_or_unparseable_retry_after_fails_closed(
    headers: tuple[tuple[str, str], ...],
) -> None:
    adapter = FakeRestAdapter()
    adapter.responses["spot_venue_time"] = [(429, "limited")]
    adapter.response_headers["spot_venue_time"] = headers
    fatal = CaptureFatalState()
    scheduler = _scheduler(adapter, fatal=fatal)

    with pytest.raises(CaptureRestSchedulerFailure, match="Retry-After"):
        await scheduler.capture_entry_once(_entry("spot_venue_time"), scheduled_at_ms=1_000_000)
    assert fatal.failed is True


@pytest.mark.asyncio
async def test_exchange_info_hash_ignores_volatile_server_time() -> None:
    adapter = FakeRestAdapter()
    adapter.responses["futures_exchange_info"] = [
        (200, '{"serverTime":1,"symbols":[{"symbol":"BTCUSDT"}]}'),
        (200, '{"serverTime":2,"symbols":[{"symbol":"BTCUSDT"}]}'),
    ]
    scheduler = _scheduler(adapter)
    entry = _entry("futures_exchange_info")

    await scheduler.capture_entry_once(entry, scheduled_at_ms=1_000_000)
    await scheduler.capture_entry_once(entry, scheduled_at_ms=1_060_000)

    assert not any(call["role"] == "futures_funding_info" for call in adapter.calls)


def test_depth_resync_queue_is_bounded_and_overflow_is_fatal() -> None:
    adapter = FakeRestAdapter()
    fatal = CaptureFatalState()
    scheduler = _scheduler(adapter, fatal=fatal)
    request = _depth_request(event="reconnect")
    for _index in range(32):
        scheduler.notify_depth_resync(request)

    with pytest.raises(CaptureRestScheduleOverflow, match="overflowed"):
        scheduler.notify_depth_resync(request)
    assert fatal.failed is True


@pytest.mark.asyncio
async def test_run_starts_all_immediate_public_loops_and_stops_without_retry_leak() -> None:
    adapter = FakeRestAdapter()
    fatal = CaptureFatalState()
    scheduler = _scheduler(adapter, fatal=fatal)
    task = asyncio.create_task(scheduler.run(fatal.stop_event))

    for _index in range(100):
        if len(adapter.calls) >= 10:
            break
        await asyncio.sleep(0.01)
    fatal.stop_event.set()
    await asyncio.wait_for(task, timeout=2)

    assert fatal.failed is False
    assert adapter.calls
    assert all(str(call["url"]).startswith("https://") for call in adapter.calls)
    assert all("order" not in str(call["url"]).casefold() for call in adapter.calls)


@pytest.mark.asyncio
async def test_run_rejects_a_split_brain_stop_event() -> None:
    scheduler = _scheduler(FakeRestAdapter())

    with pytest.raises(ValueError, match="shared fatal-state"):
        await scheduler.run(asyncio.Event())
