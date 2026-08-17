from __future__ import annotations

import asyncio
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, cast
from unittest.mock import patch

import httpx
import pytest

from signalbot.capture.receipts import ReceiptTimestamp
from signalbot.r4b_v2.capture.batching import (
    BatchPolicyV2,
    BoundedBatchHandoffV2,
    CaptureQueueAdmissionReceiptV2,
    QueuedRawRecordV2,
)
from signalbot.r4b_v2.capture.full_runtime import (
    PublicUsdmVenueClockRuntimeBindingErrorV9,
    PublicUsdmVenueClockRuntimeStateErrorV9,
    PublicUsdmVenueClockRuntimeV9,
)
from signalbot.r4b_v2.capture.models import RawRecordV2
from signalbot.r4b_v2.capture.pipeline import (
    CaptureBatchPipelineV2,
)
from signalbot.r4b_v2.capture.plans import (
    ProvisionalPromotingPlanV2,
    ProvisionalPromotingPlanV9,
    ProvisionalUsdmVenueClockRestCapturePlanV9,
    build_provisional_promoting_capture_plans_v9,
    provisional_promoting_plan_sha256_v9,
)
from signalbot.r4b_v2.capture.rest_adapter import (
    PipelineRestCaptureFatalCoordinatorV2,
)
from signalbot.r4b_v2.capture.rest_clock import (
    PublicUsdmVenueClockRestAttemptPayloadV9,
    PublicUsdmVenueClockRestErrorCategoryV9,
)
from signalbot.r4b_v2.capture.rest_clock_adapter import (
    PublicUsdmVenueClockRestCaptureAdapterV9,
)
from signalbot.r4b_v2.capture.rest_clock_scheduler import (
    PublicUsdmVenueClockMissedSlotV9,
    PublicUsdmVenueClockRestSchedulerV9,
)
from signalbot.r4b_v2.capture.websocket import SharedWebSocketIngressV2

from . import test_full_runtime as runtime_testkit

_PROTOCOL_HASH = "d" * 64
_SESSION_ID = "venue-clock-runtime-test"
_CONNECTION_ID = "usdm-venue-clock-g000001"
_SLOT_MS = 1_800_000_000_000


def _handoff() -> BoundedBatchHandoffV2:
    return BoundedBatchHandoffV2(
        BatchPolicyV2(
            max_records=8,
            max_encoded_bytes=1_000_000,
            max_linger_us=1_000,
            queue_max_events=32,
            queue_max_encoded_bytes=4_000_000,
            low_water_events=0,
            low_water_encoded_bytes=0,
            qualification_id="venue-clock-runtime-test",
        )
    )


@dataclass(slots=True)
class _RecordingOfferer:
    records: list[RawRecordV2] = field(default_factory=list)
    handoff: BoundedBatchHandoffV2 = field(default_factory=_handoff)
    after_offer: Callable[[], None] | None = None

    def offer(self, record: RawRecordV2) -> QueuedRawRecordV2:
        queued = self.handoff.offer(record)
        self.records.append(record)
        if self.after_offer is not None:
            self.after_offer()
        return queued

    def offer_with_admission_receipt(
        self,
        record: RawRecordV2,
    ) -> CaptureQueueAdmissionReceiptV2:
        receipt = self.handoff.offer_with_admission_receipt(record)
        self.records.append(record)
        if self.after_offer is not None:
            self.after_offer()
        return receipt

    def validate_queue_admission_receipt_v2(
        self,
        receipt: CaptureQueueAdmissionReceiptV2,
    ) -> QueuedRawRecordV2:
        return self.handoff.validate_queue_admission_receipt_v2(receipt)


@dataclass(slots=True)
class _RecordingFatalCoordinator:
    causes: list[BaseException] = field(default_factory=list)

    def trip_fatal(self, cause: BaseException) -> None:
        if not self.causes:
            self.causes.append(cause)

    def raise_if_failed(self) -> None:
        if self.causes:
            raise self.causes[0]


class _IncrementingReceiptClock:
    def __init__(self, start_wall_ms: int = _SLOT_MS) -> None:
        self._wall_ms = start_wall_ms
        self._monotonic_ns = 10_000_000_000

    def capture(self) -> ReceiptTimestamp:
        receipt = ReceiptTimestamp(self._wall_ms, self._monotonic_ns)
        self._wall_ms += 1
        self._monotonic_ns += 1_000_000
        return receipt


class _SchedulerClock:
    def __init__(self, wall_ms: int) -> None:
        self.wall_ms = wall_ms
        self.monotonic = 1_000_000_000
        self.deadlines: list[int] = []

    def utc_wall_ms(self) -> int:
        return self.wall_ms

    def monotonic_ns(self) -> int:
        return self.monotonic

    async def wait_until(
        self,
        stop_event: asyncio.Event,
        deadline_monotonic_ns: int,
    ) -> bool:
        self.deadlines.append(deadline_monotonic_ns)
        if stop_event.is_set():
            return True
        delta_ns = deadline_monotonic_ns - self.monotonic
        if delta_ns > 0:
            self.monotonic = deadline_monotonic_ns
            self.wall_ms += delta_ns // 1_000_000
        await asyncio.sleep(0)
        return stop_event.is_set()


class _StopOnlySchedulerClock(_SchedulerClock):
    async def wait_until(
        self,
        stop_event: asyncio.Event,
        deadline_monotonic_ns: int,
    ) -> bool:
        self.deadlines.append(deadline_monotonic_ns)
        await stop_event.wait()
        return True


class _NonBooleanWaitSchedulerClock(_SchedulerClock):
    async def wait_until(
        self,
        stop_event: asyncio.Event,
        deadline_monotonic_ns: int,
    ) -> bool:
        del stop_event
        self.deadlines.append(deadline_monotonic_ns)
        return cast(bool, "not-a-boolean")


class _RegressingWallSchedulerClock(_SchedulerClock):
    def __init__(self) -> None:
        super().__init__(_SLOT_MS)
        self._wall_reads = 0

    def utc_wall_ms(self) -> int:
        self._wall_reads += 1
        return _SLOT_MS if self._wall_reads <= 2 else _SLOT_MS - 1


class _GatedTransport(httpx.AsyncBaseTransport):
    def __init__(self) -> None:
        self.started = asyncio.Event()
        self.requests: list[httpx.Request] = []

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        self.started.set()
        await asyncio.Event().wait()
        raise AssertionError("unreachable gated transport continuation")


def _plan() -> ProvisionalUsdmVenueClockRestCapturePlanV9:
    plans = build_provisional_promoting_capture_plans_v9(("BTCUSDT",))
    [plan] = [item for item in plans if type(item) is ProvisionalUsdmVenueClockRestCapturePlanV9]
    return plan


def _adapter(
    transport: httpx.AsyncBaseTransport,
    *,
    receipt_start_wall_ms: int = _SLOT_MS,
) -> tuple[
    PublicUsdmVenueClockRestCaptureAdapterV9,
    _RecordingOfferer,
    _RecordingFatalCoordinator,
]:
    offerer = _RecordingOfferer()
    fatal = _RecordingFatalCoordinator()
    adapter = PublicUsdmVenueClockRestCaptureAdapterV9(
        _plan(),
        session_id=_SESSION_ID,
        protocol_hash=_PROTOCOL_HASH,
        connection_id=_CONNECTION_ID,
        generation=1,
        clock=_IncrementingReceiptClock(receipt_start_wall_ms),
        ingress=SharedWebSocketIngressV2(
            offerer,
            recovered_wal_tail_ingest_seq=0,
        ),
        fatal_coordinator=fatal,
        transport=transport,
    )
    return adapter, offerer, fatal


def _payload(record: RawRecordV2) -> PublicUsdmVenueClockRestAttemptPayloadV9:
    return PublicUsdmVenueClockRestAttemptPayloadV9.from_canonical_bytes(
        record.payload_bytes(),
        plan=_plan(),
    )


def _runtime_parts(
    tmp_path: Path,
    transport: httpx.AsyncBaseTransport,
    *,
    scheduler_clock: _SchedulerClock,
    bind_v9_authority: bool = True,
) -> tuple[
    Any,
    tuple[ProvisionalPromotingPlanV9, ...],
    CaptureBatchPipelineV2,
    SharedWebSocketIngressV2,
    PublicUsdmVenueClockRestCaptureAdapterV9,
    PublicUsdmVenueClockRestSchedulerV9,
]:
    plans = build_provisional_promoting_capture_plans_v9(("BTCUSDT",))
    v2_plans = cast(tuple[ProvisionalPromotingPlanV2, ...], plans[:3])
    original_v2_hash = runtime_testkit.provisional_promoting_plan_sha256_v2

    def selected_plans(
        symbols: tuple[str, ...],
    ) -> tuple[ProvisionalPromotingPlanV2, ...]:
        assert symbols == ("BTCUSDT",)
        return v2_plans

    def selected_plan_hash(actual_plans: object) -> str:
        assert actual_plans is v2_plans
        return (
            provisional_promoting_plan_sha256_v9(plans)
            if bind_v9_authority
            else original_v2_hash(v2_plans)
        )

    with (
        patch.object(
            runtime_testkit,
            "build_provisional_promoting_capture_plans_v2",
            selected_plans,
        ),
        patch.object(
            runtime_testkit,
            "provisional_promoting_plan_sha256_v2",
            selected_plan_hash,
        ),
        patch.object(
            runtime_testkit._Fixture,
            "_composition",
            lambda *_args, **_kwargs: object(),
        ),
    ):
        fixture = runtime_testkit._Fixture(tmp_path)
    [plan] = [item for item in plans if type(item) is ProvisionalUsdmVenueClockRestCapturePlanV9]
    pipeline = cast(CaptureBatchPipelineV2, fixture.pipeline)
    ingress = cast(SharedWebSocketIngressV2, fixture.ingress)
    adapter = PublicUsdmVenueClockRestCaptureAdapterV9(
        plan,
        session_id=fixture.session_id,
        protocol_hash=fixture.authority.protocol_sha256,
        connection_id=_CONNECTION_ID,
        generation=1,
        clock=_IncrementingReceiptClock(),
        ingress=ingress,
        fatal_coordinator=PipelineRestCaptureFatalCoordinatorV2(pipeline),
        transport=transport,
    )
    scheduler = PublicUsdmVenueClockRestSchedulerV9(
        plan,
        adapter,
        clock=scheduler_clock,
    )
    return fixture, plans, pipeline, ingress, adapter, scheduler


@pytest.mark.asyncio
async def test_scheduler_polls_exact_public_path_once_and_stops_cleanly() -> None:
    requests: list[httpx.Request] = []

    async def respond(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(
            200,
            content=b'{"serverTime":1800000000001}',
            request=request,
        )

    adapter, offerer, fatal = _adapter(httpx.MockTransport(respond))
    scheduler = PublicUsdmVenueClockRestSchedulerV9(
        adapter.plan,
        adapter,
        clock=_SchedulerClock(_SLOT_MS),
    )
    offerer.after_offer = scheduler.request_stop

    result = await scheduler.run()
    await adapter.aclose()

    assert result.attempted_cycle_count == 1
    assert result.retries_performed == 0
    assert result.causal_cursor_complete is False
    assert scheduler.drained
    assert len(requests) == 1
    request = requests[0]
    assert request.method == "GET"
    assert request.url == httpx.URL("https://fapi.binance.com/fapi/v1/time")
    assert "api-key" not in request.headers
    assert len(offerer.records) == 1
    payload = _payload(offerer.records[0])
    assert payload.poll_cycle_seq == 1
    assert payload.scheduled_slot_wall_ms == _SLOT_MS
    assert payload.http_attempt == 1
    assert payload.error_category is None
    assert payload.body_bytes() == b'{"serverTime":1800000000001}'
    assert adapter.cleanly_closed
    assert not fatal.causes


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "request_start_wall_ms",
    (_SLOT_MS, _SLOT_MS + 29_999),
)
async def test_request_start_slot_boundaries_are_inclusive_before_end(
    request_start_wall_ms: int,
) -> None:
    adapter, offerer, fatal = _adapter(
        httpx.MockTransport(lambda request: httpx.Response(200, content=b"{}", request=request)),
        receipt_start_wall_ms=request_start_wall_ms,
    )
    scheduler = PublicUsdmVenueClockRestSchedulerV9(
        adapter.plan,
        adapter,
        clock=_SchedulerClock(_SLOT_MS),
    )
    offerer.after_offer = scheduler.request_stop

    result = await scheduler.run()
    await adapter.aclose()

    assert result.attempted_cycle_count == 1
    assert len(offerer.records) == 1
    assert not fatal.causes


@pytest.mark.asyncio
async def test_request_start_at_slot_end_skips_without_network_or_retry() -> None:
    requests: list[httpx.Request] = []

    async def respond(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(200, content=b"{}", request=request)

    adapter, offerer, fatal = _adapter(
        httpx.MockTransport(respond),
        receipt_start_wall_ms=_SLOT_MS + 30_000,
    )
    scheduler = PublicUsdmVenueClockRestSchedulerV9(
        adapter.plan,
        adapter,
        clock=_SchedulerClock(_SLOT_MS),
    )

    with pytest.raises(PublicUsdmVenueClockMissedSlotV9):
        await scheduler.run()
    await adapter.aclose()

    assert not requests
    assert not offerer.records
    assert scheduler.attempted_cycle_count == 0
    assert scheduler.drained
    assert adapter.cleanly_closed
    assert not fatal.causes


@pytest.mark.asyncio
async def test_network_failure_is_one_terminal_observation_with_zero_retry() -> None:
    requests: list[httpx.Request] = []

    async def fail(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        raise httpx.ConnectError("offline", request=request)

    adapter, offerer, fatal = _adapter(httpx.MockTransport(fail))
    scheduler = PublicUsdmVenueClockRestSchedulerV9(
        adapter.plan,
        adapter,
        clock=_SchedulerClock(_SLOT_MS),
    )
    offerer.after_offer = scheduler.request_stop

    result = await scheduler.run()
    await adapter.aclose()

    assert result.attempted_cycle_count == 1
    assert result.retries_performed == 0
    assert len(requests) == 1
    assert len(offerer.records) == 1
    payload = _payload(offerer.records[0])
    assert payload.error_category is PublicUsdmVenueClockRestErrorCategoryV9.NETWORK
    assert payload.payload_complete is False
    assert not fatal.causes


@pytest.mark.asyncio
async def test_caller_cancellation_retains_cancelled_terminal_and_drains() -> None:
    transport = _GatedTransport()
    adapter, offerer, fatal = _adapter(transport)
    scheduler = PublicUsdmVenueClockRestSchedulerV9(
        adapter.plan,
        adapter,
        clock=_SchedulerClock(_SLOT_MS),
    )
    task = asyncio.create_task(scheduler.run())
    await asyncio.wait_for(transport.started.wait(), timeout=1.0)

    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    await adapter.aclose()

    assert len(transport.requests) == 1
    assert len(offerer.records) == 1
    payload = _payload(offerer.records[0])
    assert payload.error_category is PublicUsdmVenueClockRestErrorCategoryV9.CANCELLED
    assert payload.admission_cancellation_requested is True
    assert scheduler.attempted_cycle_count == 0
    assert scheduler.drained
    assert adapter.cleanly_closed
    assert not fatal.causes


@pytest.mark.asyncio
async def test_idle_stop_wakes_deadline_without_attempt_or_transport() -> None:
    requests: list[httpx.Request] = []

    async def respond(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(200, content=b"{}", request=request)

    adapter, offerer, fatal = _adapter(httpx.MockTransport(respond))
    clock = _StopOnlySchedulerClock(_SLOT_MS + 1)
    scheduler = PublicUsdmVenueClockRestSchedulerV9(
        adapter.plan,
        adapter,
        clock=clock,
    )
    task = asyncio.create_task(scheduler.run())
    for _ in range(100):
        if clock.deadlines:
            break
        await asyncio.sleep(0)
    assert clock.deadlines

    scheduler.request_stop()
    result = await asyncio.wait_for(task, timeout=1.0)
    await adapter.aclose()

    assert result.attempted_cycle_count == 0
    assert not requests
    assert not offerer.records
    assert scheduler.drained
    assert adapter.cleanly_closed
    assert not fatal.causes


@pytest.mark.asyncio
async def test_scheduler_rejects_non_boolean_wait_result_before_io() -> None:
    requests: list[httpx.Request] = []

    async def respond(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(200, content=b"{}", request=request)

    adapter, offerer, fatal = _adapter(httpx.MockTransport(respond))
    scheduler = PublicUsdmVenueClockRestSchedulerV9(
        adapter.plan,
        adapter,
        clock=_NonBooleanWaitSchedulerClock(_SLOT_MS + 1),
    )

    with pytest.raises(TypeError, match="boolean"):
        await scheduler.run()
    await adapter.aclose()

    assert not requests
    assert not offerer.records
    assert scheduler.drained
    assert not fatal.causes


@pytest.mark.asyncio
async def test_scheduler_rejects_wall_regression_after_terminal_admission() -> None:
    adapter, offerer, fatal = _adapter(
        httpx.MockTransport(lambda request: httpx.Response(200, content=b"{}", request=request))
    )
    scheduler = PublicUsdmVenueClockRestSchedulerV9(
        adapter.plan,
        adapter,
        clock=_RegressingWallSchedulerClock(),
    )

    with pytest.raises(ValueError, match="moved backwards"):
        await scheduler.run()
    await adapter.aclose()

    assert len(offerer.records) == 1
    assert scheduler.drained
    assert not fatal.causes


@pytest.mark.asyncio
async def test_v9_runtime_binds_shared_pipeline_runs_and_closes_adapter(
    tmp_path: Path,
) -> None:
    runtime_holder: dict[str, PublicUsdmVenueClockRuntimeV9] = {}

    async def respond(request: httpx.Request) -> httpx.Response:
        await runtime_holder["runtime"].request_normal_stop()
        return httpx.Response(
            200,
            content=b'{"serverTime":1800000000001}',
            request=request,
        )

    fixture, plans, _pipeline, ingress, adapter, scheduler = _runtime_parts(
        tmp_path,
        httpx.MockTransport(respond),
        scheduler_clock=_SchedulerClock(_SLOT_MS),
    )
    runtime = PublicUsdmVenueClockRuntimeV9(
        plans,
        ingress,
        adapter,
        scheduler,
        adapter_shutdown_timeout_seconds=1.0,
    )
    runtime_holder["runtime"] = runtime

    result = await runtime.run()

    assert result.scheduler_result.attempted_cycle_count == 1
    assert result.adapter_cleanly_closed is True
    assert result.scheduler_drained is True
    assert result.durable_session_closure_issued is False
    assert result.causal_cursor_complete is False
    assert runtime.result is result
    assert adapter.cleanly_closed
    await fixture.pipeline.stop()
    assert fixture.wal_writer.next_ingest_seq == 2
    with pytest.raises(PublicUsdmVenueClockRuntimeStateErrorV9, match="only once"):
        await runtime.run()
    await fixture.close()


@pytest.mark.asyncio
async def test_v9_runtime_rejects_foreign_shared_ingress_before_io(
    tmp_path: Path,
) -> None:
    fixture, plans, pipeline, _ingress, adapter, scheduler = _runtime_parts(
        tmp_path,
        httpx.MockTransport(lambda request: httpx.Response(200, content=b"{}", request=request)),
        scheduler_clock=_SchedulerClock(_SLOT_MS),
    )
    foreign_ingress = SharedWebSocketIngressV2(
        pipeline,
        recovered_wal_tail_ingest_seq=0,
    )

    with pytest.raises(
        PublicUsdmVenueClockRuntimeBindingErrorV9,
        match="shared ingress",
    ):
        PublicUsdmVenueClockRuntimeV9(
            plans,
            foreign_ingress,
            adapter,
            scheduler,
            adapter_shutdown_timeout_seconds=1.0,
        )

    assert not scheduler.started_once
    await adapter.aclose()
    assert adapter.cleanly_closed
    await fixture.close()


@pytest.mark.asyncio
async def test_v9_runtime_rejects_pipeline_with_legacy_v2_plan_hash(
    tmp_path: Path,
) -> None:
    fixture, plans, _pipeline, ingress, adapter, scheduler = _runtime_parts(
        tmp_path,
        httpx.MockTransport(lambda request: httpx.Response(200, content=b"{}", request=request)),
        scheduler_clock=_SchedulerClock(_SLOT_MS),
        bind_v9_authority=False,
    )

    with pytest.raises(
        PublicUsdmVenueClockRuntimeBindingErrorV9,
        match="durable V9 plan",
    ):
        PublicUsdmVenueClockRuntimeV9(
            plans,
            ingress,
            adapter,
            scheduler,
            adapter_shutdown_timeout_seconds=1.0,
        )

    assert not scheduler.started_once
    await adapter.aclose()
    await fixture.close()


@pytest.mark.asyncio
async def test_v9_runtime_idle_normal_stop_is_bounded_and_empty(
    tmp_path: Path,
) -> None:
    requests: list[httpx.Request] = []

    async def respond(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(200, content=b"{}", request=request)

    clock = _StopOnlySchedulerClock(_SLOT_MS + 1)
    fixture, plans, _pipeline, ingress, adapter, scheduler = _runtime_parts(
        tmp_path,
        httpx.MockTransport(respond),
        scheduler_clock=clock,
    )
    runtime = PublicUsdmVenueClockRuntimeV9(
        plans,
        ingress,
        adapter,
        scheduler,
        adapter_shutdown_timeout_seconds=1.0,
    )
    task = asyncio.create_task(runtime.run())
    for _ in range(100):
        if clock.deadlines:
            break
        await asyncio.sleep(0)
    assert clock.deadlines

    await runtime.request_normal_stop()
    result = await asyncio.wait_for(task, timeout=1.0)

    assert result.scheduler_result.attempted_cycle_count == 0
    assert not requests
    assert fixture.wal_writer.next_ingest_seq == 1
    assert adapter.cleanly_closed
    await fixture.close()


def test_plan_is_frozen_to_one_concurrent_attempt_and_zero_retries() -> None:
    plan = _plan()
    assert plan.poll_interval_ms == 30_000
    assert plan.request_timeout_ms == 2_000
    assert plan.maximum_concurrency == 1
    assert plan.maximum_attempts == 1
    assert plan.retryable_status_codes == ()
    assert plan.retryable_error_categories == ()
    assert plan.retry_backoff_ms == ()
    assert plan.requires_api_key is False
    assert plan.order_execution_enabled is False
    assert cast(object, plan.fixed_query) == ()
