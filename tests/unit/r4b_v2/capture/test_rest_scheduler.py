from __future__ import annotations

import asyncio
from collections.abc import Callable
from dataclasses import dataclass, replace
from typing import cast

import pytest

from signalbot.capture.receipts import ReceiptTimestamp
from signalbot.r4b_v2.capture import websocket as websocket_module
from signalbot.r4b_v2.capture.batching import (
    BatchDrainerV2,
    BatchPolicyV2,
    BoundedBatchHandoffV2,
    QueuedRawRecordV2,
)
from signalbot.r4b_v2.capture.models import RawRecordV2, TransportV2
from signalbot.r4b_v2.capture.plans import (
    ProvisionalPromotingRestCapturePlanV2,
    build_provisional_promoting_capture_plans_v2,
)
from signalbot.r4b_v2.capture.rest import (
    PublicOiRestMissedSlotV2,
    PublicOiRestTerminalObservationV2,
    public_oi_rest_source_logical_key_v2,
)
from signalbot.r4b_v2.capture.rest_census import (
    PublicOiRestCellOutcomeV2,
    PublicOiRestCoverageCloseV2,
    PublicOiRestForwardGapRangeV2,
    PublicOiRestSlotCensusV2,
    public_oi_rest_attempt_record_sha256_v2,
)
from signalbot.r4b_v2.capture.rest_scheduler import (
    PublicOiRestAttemptSelfCancelledV2,
    PublicOiRestCensusContextV2,
    PublicOiRestNormalStopBoundaryRaceV2,
    PublicOiScheduleAuthorityV2,
    PublicOiScheduledAttemptTokenV2,
    PublicOpenInterestRestSchedulerV2,
    consume_public_oi_scheduled_attempt_token_v2,
    create_public_oi_rest_census_context_v2,
)
from signalbot.r4b_v2.capture.websocket import (
    PublicOiAdmissionReceiptV2,
    SharedWebSocketIngressV2,
)

_PROTOCOL_HASH = "a" * 64
_START_MANIFEST_HASH = "b" * 64
_PLAN_BUNDLE_HASH = "c" * 64


@dataclass(frozen=True, slots=True)
class AttemptCall:
    symbol: str
    poll_cycle_seq: int
    symbol_ordinal: int
    scheduled_slot_wall_ms: int
    attempt: int


class FakeSchedulerClock:
    def __init__(self, *, wall_ms: int, monotonic_ns: int) -> None:
        self.wall_ms = wall_ms
        self.current_monotonic_ns = monotonic_ns
        self.deadlines: asyncio.Queue[int] = asyncio.Queue()
        self._advanced = asyncio.Event()

    def utc_wall_ms(self) -> int:
        return self.wall_ms

    def monotonic_ns(self) -> int:
        return self.current_monotonic_ns

    async def wait_until(
        self,
        stop_event: asyncio.Event,
        deadline_monotonic_ns: int,
    ) -> bool:
        self.deadlines.put_nowait(deadline_monotonic_ns)
        await asyncio.sleep(0)
        while self.current_monotonic_ns < deadline_monotonic_ns:
            if stop_event.is_set():
                return True
            advanced = asyncio.create_task(self._advanced.wait())
            stopped = asyncio.create_task(stop_event.wait())
            try:
                done, _pending = await asyncio.wait(
                    {advanced, stopped},
                    return_when=asyncio.FIRST_COMPLETED,
                )
            finally:
                for task in (advanced, stopped):
                    if not task.done():
                        task.cancel()
                await asyncio.gather(advanced, stopped, return_exceptions=True)
            if stopped in done and stop_event.is_set():
                return True
            self._advanced.clear()
        return stop_event.is_set()

    def advance_to(self, monotonic_ns: int) -> None:
        assert monotonic_ns >= self.current_monotonic_ns
        self.current_monotonic_ns = monotonic_ns
        self._advanced.set()

    def advance_without_wake(self, monotonic_ns: int) -> None:
        assert monotonic_ns >= self.current_monotonic_ns
        self.current_monotonic_ns = monotonic_ns

    def set_wall(self, wall_ms: int) -> None:
        self.wall_ms = wall_ms


class CoupledReceiptClock:
    def __init__(self, scheduler_clock: FakeSchedulerClock) -> None:
        self.scheduler_clock = scheduler_clock

    def capture(self) -> ReceiptTimestamp:
        return ReceiptTimestamp(
            self.scheduler_clock.wall_ms,
            self.scheduler_clock.current_monotonic_ns,
        )


class RecordingAttemptAdapter:
    def __init__(
        self,
        plan: ProvisionalPromotingRestCapturePlanV2,
        *,
        expected_symbols_per_cycle: int,
        gate: asyncio.Event | None = None,
        failure_symbol: str | None = None,
        failure: BaseException | None = None,
        failure_poll_cycle_seq: int | None = None,
        started_signal_count: int | None = None,
        on_cycle_complete: Callable[[int], None] | None = None,
        result_factory: Callable[[AttemptCall, int], object] | None = None,
        gate_exempt_symbol: str | None = None,
        post_admission_gate: asyncio.Event | None = None,
    ) -> None:
        self.plan = plan
        self.expected_symbols_per_cycle = expected_symbols_per_cycle
        self.gate = gate
        self.failure_symbol = failure_symbol
        self.failure = failure
        self.failure_poll_cycle_seq = failure_poll_cycle_seq
        self.started_signal_count = started_signal_count
        self.on_cycle_complete = on_cycle_complete
        self.result_factory = result_factory
        self.gate_exempt_symbol = gate_exempt_symbol
        self.post_admission_gate = post_admission_gate
        self.calls: list[AttemptCall] = []
        self.tokens: list[PublicOiScheduledAttemptTokenV2] = []
        self.schedule_authority: PublicOiScheduleAuthorityV2 | None = None
        self.ingress: SharedWebSocketIngressV2 | None = None
        self.receipt_clock: CoupledReceiptClock | None = None
        self.handoff: BoundedBatchHandoffV2 | None = None
        self.started: asyncio.Queue[AttemptCall] = asyncio.Queue()
        self.admitted: asyncio.Queue[PublicOiAdmissionReceiptV2] = asyncio.Queue()
        self.all_started = asyncio.Event()
        self.cancelled_symbols: list[str] = []
        self.completed_symbols: list[str] = []
        self.active = 0
        self.maximum_active = 0
        self._completed_by_cycle: dict[int, int] = {}
        self._cycle_completed: dict[int, asyncio.Event] = {}

    def cycle_completed(self, poll_cycle_seq: int) -> asyncio.Event:
        return self._cycle_completed.setdefault(poll_cycle_seq, asyncio.Event())

    def bind_schedule_authority(
        self,
        schedule_authority: PublicOiScheduleAuthorityV2,
        /,
    ) -> None:
        if self.schedule_authority is not None:
            raise RuntimeError("test adapter schedule authority was bound twice")
        self.schedule_authority = schedule_authority

    def configure_capture_context(
        self,
        *,
        ingress: SharedWebSocketIngressV2,
        receipt_clock: CoupledReceiptClock,
        handoff: BoundedBatchHandoffV2,
    ) -> None:
        if self.ingress is not None or self.calls:
            raise RuntimeError("test adapter capture context was configured twice or late")
        self.ingress = ingress
        self.receipt_clock = receipt_clock
        self.handoff = handoff

    async def capture_attempt(
        self,
        token: PublicOiScheduledAttemptTokenV2,
        /,
    ) -> PublicOiAdmissionReceiptV2:
        schedule_authority = self.schedule_authority
        if schedule_authority is None:
            raise RuntimeError("test adapter lacks a schedule authority")
        consume_public_oi_scheduled_attempt_token_v2(
            token,
            plan=self.plan,
            schedule_authority=schedule_authority,
        )
        self.tokens.append(token)
        call = AttemptCall(
            symbol=token.symbol,
            poll_cycle_seq=token.poll_cycle_seq,
            symbol_ordinal=token.symbol_ordinal,
            scheduled_slot_wall_ms=token.scheduled_slot_wall_ms,
            attempt=token.attempt,
        )
        self.calls.append(call)
        self.started.put_nowait(call)
        receipt_clock = self.receipt_clock
        ingress = self.ingress
        if receipt_clock is None or ingress is None:
            raise RuntimeError("test adapter lacks its exact shared capture context")
        request_started = receipt_clock.capture()
        observation = PublicOiRestTerminalObservationV2.for_plan(
            self.plan,
            symbol=call.symbol,
            poll_cycle_seq=call.poll_cycle_seq,
            symbol_ordinal=call.symbol_ordinal,
            scheduled_slot_wall_ms=call.scheduled_slot_wall_ms,
            attempt=call.attempt,
            request_started_wall_ms=request_started.received_at_ms,
            request_started_monotonic_ns=request_started.received_monotonic_ns,
            response_first_header_wall_ms=request_started.received_at_ms,
            response_first_header_monotonic_ns=request_started.received_monotonic_ns,
            attempt_ended_wall_ms=request_started.received_at_ms,
            attempt_ended_monotonic_ns=request_started.received_monotonic_ns,
            response_status=200,
            response_headers=(("content-type", "application/json"),),
            payload_complete=True,
            body=b'{"openInterest":"1"}',
        )
        self.active += 1
        self.maximum_active = max(self.maximum_active, self.active)
        if self.started_signal_count is not None and len(self.calls) >= self.started_signal_count:
            self.all_started.set()
        succeeded = False
        try:
            if self.failure_symbol == token.symbol and (
                self.failure_poll_cycle_seq is None
                or self.failure_poll_cycle_seq == token.poll_cycle_seq
            ):
                await self.all_started.wait()
                assert self.failure is not None
                raise self.failure
            if self.gate is not None:
                if token.symbol == self.gate_exempt_symbol:
                    await self.all_started.wait()
                else:
                    await self.gate.wait()
            ingest_seq = len(self.calls)
            if self.result_factory is not None:
                result = cast(
                    PublicOiAdmissionReceiptV2,
                    self.result_factory(call, ingest_seq),
                )
                succeeded = True
                return result
            receipt = await ingress.offer_https_attempt(
                plan=self.plan,
                session_id="scheduler-test-session",
                protocol_hash=_PROTOCOL_HASH,
                connection_id="oi-rest-producer",
                generation=1,
                symbol=call.symbol,
                clock=receipt_clock,
                observation=observation,
                source_logical_key=public_oi_rest_source_logical_key_v2(call.symbol),
            )
            self.admitted.put_nowait(receipt)
            if self.post_admission_gate is not None:
                await self.post_admission_gate.wait()
            succeeded = True
            return receipt
        except asyncio.CancelledError:
            self.cancelled_symbols.append(token.symbol)
            raise
        finally:
            self.active -= 1
            if succeeded:
                self.completed_symbols.append(token.symbol)
                count = self._completed_by_cycle.get(token.poll_cycle_seq, 0) + 1
                self._completed_by_cycle[token.poll_cycle_seq] = count
                if count == self.expected_symbols_per_cycle:
                    if self.on_cycle_complete is not None:
                        self.on_cycle_complete(token.poll_cycle_seq)
                    self.cycle_completed(token.poll_cycle_seq).set()


def _rest_plan(symbols: tuple[str, ...]) -> ProvisionalPromotingRestCapturePlanV2:
    plans = build_provisional_promoting_capture_plans_v2(symbols)
    rest = tuple(plan for plan in plans if type(plan) is ProvisionalPromotingRestCapturePlanV2)
    assert len(rest) == 1
    return rest[0]


def _scheduler(
    plan: ProvisionalPromotingRestCapturePlanV2,
    adapter: RecordingAttemptAdapter,
    *,
    clock: FakeSchedulerClock,
    coverage_start_slot_wall_ms: int | None = None,
) -> PublicOpenInterestRestSchedulerV2:
    handoff = BoundedBatchHandoffV2(
        BatchPolicyV2(
            max_records=64,
            max_encoded_bytes=1_280_000,
            max_linger_us=1_000,
            queue_max_events=4_096,
            queue_max_encoded_bytes=100_000_000,
            low_water_events=0,
            low_water_encoded_bytes=0,
            qualification_id="scheduler-census-test-handoff",
        ),
        expected_first_ingest_seq=1,
    )
    ingress = SharedWebSocketIngressV2(
        handoff,
        recovered_wal_tail_ingest_seq=0,
    )
    receipt_clock = CoupledReceiptClock(clock)
    adapter.configure_capture_context(
        ingress=ingress,
        receipt_clock=receipt_clock,
        handoff=handoff,
    )
    start_slot = (
        clock.wall_ms - (clock.wall_ms % plan.poll_interval_ms)
        if coverage_start_slot_wall_ms is None
        else coverage_start_slot_wall_ms
    )
    context = create_public_oi_rest_census_context_v2(
        plan,
        session_id="scheduler-test-session",
        session_start_manifest_sha256=_START_MANIFEST_HASH,
        plan_bundle_sha256=_PLAN_BUNDLE_HASH,
        protocol_hash=_PROTOCOL_HASH,
        coverage_start_slot_wall_ms=start_slot,
        ingress=ingress,
        receipt_clock=receipt_clock,
    )
    return PublicOpenInterestRestSchedulerV2(
        plan,
        adapter,
        census_context=context,
        clock=clock,
    )


def _valid_record(
    plan: ProvisionalPromotingRestCapturePlanV2,
    call: AttemptCall,
    *,
    ingest_seq: int,
) -> RawRecordV2:
    start_monotonic_ns = 1_000_000 + call.poll_cycle_seq * 10_000 + call.symbol_ordinal * 10
    observation = PublicOiRestTerminalObservationV2.for_plan(
        plan,
        symbol=call.symbol,
        poll_cycle_seq=call.poll_cycle_seq,
        symbol_ordinal=call.symbol_ordinal,
        scheduled_slot_wall_ms=call.scheduled_slot_wall_ms,
        attempt=call.attempt,
        request_started_wall_ms=call.scheduled_slot_wall_ms,
        request_started_monotonic_ns=start_monotonic_ns,
        response_first_header_wall_ms=call.scheduled_slot_wall_ms + 1,
        response_first_header_monotonic_ns=start_monotonic_ns + 1,
        attempt_ended_wall_ms=call.scheduled_slot_wall_ms + 2,
        attempt_ended_monotonic_ns=start_monotonic_ns + 2,
        response_status=200,
        response_headers=(("content-type", "application/json"),),
        payload_complete=True,
        body=b'{"openInterest":"1"}',
    )
    completion = ReceiptTimestamp(
        call.scheduled_slot_wall_ms + 3,
        start_monotonic_ns + 3,
    )
    return RawRecordV2.from_payload(
        session_id="scheduler-test-session",
        plan_id=plan.name,
        protocol_hash=_PROTOCOL_HASH,
        transport=TransportV2.HTTPS,
        venue=plan.venue,
        route_id=plan.route_id,
        symbol=call.symbol,
        connection_id="oi-rest-producer",
        generation=1,
        frame_seq=None,
        ingest_seq=ingest_seq,
        receipt_wall_ms=completion.received_at_ms,
        receipt_monotonic_ns=completion.received_monotonic_ns,
        raw_payload=observation(completion),
        source_logical_key=public_oi_rest_source_logical_key_v2(call.symbol),
    )


def _receipt_for_record(record: RawRecordV2) -> PublicOiAdmissionReceiptV2:
    handoff = BoundedBatchHandoffV2(
        BatchPolicyV2(
            max_records=2,
            max_encoded_bytes=1_000_000,
            max_linger_us=1_000,
            queue_max_events=2,
            queue_max_encoded_bytes=2_000_000,
            low_water_events=0,
            low_water_encoded_bytes=0,
            qualification_id="scheduler-receipt-test-handoff",
        ),
        expected_first_ingest_seq=record.ingest_seq,
    )
    queue_admission_receipt = handoff.offer_with_admission_receipt(record)
    return PublicOiAdmissionReceiptV2(
        record=record,
        queue_admission_receipt=queue_admission_receipt,
        _factory_token=websocket_module._PUBLIC_OI_ADMISSION_RECEIPT_FACTORY_TOKEN,
    )


def _valid_receipt(
    plan: ProvisionalPromotingRestCapturePlanV2,
    call: AttemptCall,
    *,
    ingest_seq: int,
) -> PublicOiAdmissionReceiptV2:
    return _receipt_for_record(_valid_record(plan, call, ingest_seq=ingest_seq))


async def _drain_queued_records(
    adapter: RecordingAttemptAdapter,
) -> tuple[QueuedRawRecordV2, ...]:
    handoff = adapter.handoff
    if handoff is None:
        raise RuntimeError("test adapter lacks its bounded handoff")
    drainer = BatchDrainerV2(handoff)
    records: list[QueuedRawRecordV2] = []
    while handoff.current_events:
        batch = await drainer.next_batch()
        if not batch.records:
            raise AssertionError("scheduler test handoff produced a non-record batch")
        records.extend(batch.records)
        handoff.acknowledge_records(
            batch,
            durable_ack_seq=batch.records[-1].ingest_seq,
            completed_monotonic_ns=max(
                record.enqueued_monotonic_ns for record in batch.records
            ),
            writer_latency_ns=0,
        )
    return tuple(records)


def _slot_payloads(
    records: tuple[QueuedRawRecordV2, ...],
    *,
    plan: ProvisionalPromotingRestCapturePlanV2,
) -> tuple[tuple[QueuedRawRecordV2, PublicOiRestSlotCensusV2], ...]:
    parsed: list[tuple[QueuedRawRecordV2, PublicOiRestSlotCensusV2]] = []
    for queued in records:
        if queued.record.source_logical_key != "openInterest:census":
            continue
        try:
            payload = PublicOiRestSlotCensusV2.from_canonical_bytes(
                queued.record.payload_bytes(),
                plan=plan,
            )
        except ValueError:
            continue
        parsed.append((queued, payload))
    return tuple(parsed)


def _close_payloads(
    records: tuple[QueuedRawRecordV2, ...],
    *,
    plan: ProvisionalPromotingRestCapturePlanV2,
) -> tuple[tuple[QueuedRawRecordV2, PublicOiRestCoverageCloseV2], ...]:
    parsed: list[tuple[QueuedRawRecordV2, PublicOiRestCoverageCloseV2]] = []
    for queued in records:
        if queued.record.source_logical_key != "openInterest:census":
            continue
        try:
            payload = PublicOiRestCoverageCloseV2.from_canonical_bytes(
                queued.record.payload_bytes(),
                plan=plan,
            )
        except ValueError:
            continue
        parsed.append((queued, payload))
    return tuple(parsed)


def _gap_payloads(
    records: tuple[QueuedRawRecordV2, ...],
    *,
    plan: ProvisionalPromotingRestCapturePlanV2,
) -> tuple[tuple[QueuedRawRecordV2, PublicOiRestForwardGapRangeV2], ...]:
    parsed: list[tuple[QueuedRawRecordV2, PublicOiRestForwardGapRangeV2]] = []
    for queued in records:
        if queued.record.source_logical_key != "openInterest:census":
            continue
        try:
            payload = PublicOiRestForwardGapRangeV2.from_canonical_bytes(
                queued.record.payload_bytes(),
                plan=plan,
            )
        except ValueError:
            continue
        parsed.append((queued, payload))
    return tuple(parsed)


async def _stop_after_next_deadline(
    task: asyncio.Task[None],
    *,
    clock: FakeSchedulerClock,
    scheduler: PublicOpenInterestRestSchedulerV2,
) -> int:
    deadline = await clock.deadlines.get()
    if clock.wall_ms % scheduler.plan.poll_interval_ms == 0:
        clock.set_wall(clock.wall_ms + 1)
    await scheduler.request_normal_stop()
    await task
    return deadline


@pytest.mark.asyncio
async def test_immediate_cycle_uses_utc_floor_sorted_symbols_and_one_attempt() -> None:
    plan = _rest_plan(("SOLUSDT", "BTCUSDT", "ETHUSDT"))
    clock = FakeSchedulerClock(wall_ms=12_345, monotonic_ns=1_000)
    adapter = RecordingAttemptAdapter(plan, expected_symbols_per_cycle=3)
    scheduler = _scheduler(plan, adapter, clock=clock)

    task = asyncio.create_task(scheduler.run())
    await adapter.cycle_completed(1).wait()
    deadline = await _stop_after_next_deadline(
        task,
        clock=clock,
        scheduler=scheduler,
    )

    assert deadline == 2_655_001_000
    assert adapter.calls == [
        AttemptCall("BTCUSDT", 1, 0, 10_000, 1),
        AttemptCall("ETHUSDT", 1, 1, 10_000, 1),
        AttemptCall("SOLUSDT", 1, 2, 10_000, 1),
    ]
    assert len(adapter.tokens) == 3
    assert all(type(token) is PublicOiScheduledAttemptTokenV2 for token in adapter.tokens)
    assert all(token.plan is plan for token in adapter.tokens)
    assert all(
        token.schedule_authority is scheduler.schedule_authority
        for token in adapter.tokens
    )
    assert [token.symbol for token in adapter.tokens] == list(plan.symbols)
    assert scheduler.last_started_poll_cycle_seq == 1
    assert scheduler.last_completed_poll_cycle_seq == 1
    assert scheduler.drained
    assert not scheduler.running
    queued = await _drain_queued_records(adapter)
    slots = _slot_payloads(queued, plan=plan)
    closes = _close_payloads(queued, plan=plan)
    assert len(slots) == 1
    assert len(closes) == 1
    assert [record.ingest_seq for record in queued] == list(
        range(1, len(queued) + 1)
    )
    assert all(
        entry.outcome is PublicOiRestCellOutcomeV2.ATTEMPT_RETAINED
        for entry in slots[0][1].entries
    )
    assert closes[0][1].last_census_ingest_seq == slots[0][0].ingest_seq


@pytest.mark.asyncio
async def test_monotonic_cadence_advances_due_and_wall_slot_by_five_seconds() -> None:
    plan = _rest_plan(("BTCUSDT",))
    clock = FakeSchedulerClock(wall_ms=12_345, monotonic_ns=100)
    adapter = RecordingAttemptAdapter(plan, expected_symbols_per_cycle=1)
    scheduler = _scheduler(plan, adapter, clock=clock)
    task = asyncio.create_task(scheduler.run())

    await adapter.cycle_completed(1).wait()
    first_deadline = await clock.deadlines.get()
    assert first_deadline == 2_655_000_100
    clock.set_wall(15_000)
    clock.advance_to(first_deadline)
    await adapter.cycle_completed(2).wait()
    second_deadline = await _stop_after_next_deadline(
        task,
        clock=clock,
        scheduler=scheduler,
    )

    assert second_deadline == 7_655_000_100
    assert adapter.calls == [
        AttemptCall("BTCUSDT", 1, 0, 10_000, 1),
        AttemptCall("BTCUSDT", 2, 0, 15_000, 1),
    ]
    assert scheduler.last_completed_poll_cycle_seq == 2


@pytest.mark.asyncio
async def test_exact_utc_due_boundary_is_run_immediately_not_skipped() -> None:
    plan = _rest_plan(("BTCUSDT",))
    clock = FakeSchedulerClock(wall_ms=12_345, monotonic_ns=100)
    def move_to_exact_boundary(poll_cycle_seq: int) -> None:
        if poll_cycle_seq == 1:
            clock.set_wall(15_000)

    adapter = RecordingAttemptAdapter(
        plan,
        expected_symbols_per_cycle=1,
        on_cycle_complete=move_to_exact_boundary,
    )
    scheduler = _scheduler(plan, adapter, clock=clock)

    task = asyncio.create_task(scheduler.run())
    await adapter.cycle_completed(2).wait()
    clock.set_wall(15_001)
    await scheduler.request_normal_stop()
    await task

    assert clock.deadlines.get_nowait() == 100
    assert adapter.calls == [
        AttemptCall("BTCUSDT", 1, 0, 10_000, 1),
        AttemptCall("BTCUSDT", 2, 0, 15_000, 1),
    ]
    assert scheduler.last_completed_poll_cycle_seq == 2


@pytest.mark.asyncio
async def test_cycle_overrun_skips_missed_slots_without_catch_up_or_backfill() -> None:
    plan = _rest_plan(("BTCUSDT",))
    initial_monotonic_ns = 1_000
    clock = FakeSchedulerClock(
        wall_ms=12_345,
        monotonic_ns=initial_monotonic_ns,
    )

    def overrun_first_cycle(poll_cycle_seq: int) -> None:
        if poll_cycle_seq == 1:
            clock.set_wall(24_345)
            clock.advance_without_wake(initial_monotonic_ns + 12_000_000_000)

    adapter = RecordingAttemptAdapter(
        plan,
        expected_symbols_per_cycle=1,
        on_cycle_complete=overrun_first_cycle,
    )
    scheduler = _scheduler(plan, adapter, clock=clock)
    task = asyncio.create_task(scheduler.run())

    await adapter.cycle_completed(1).wait()
    skipped_deadline = await clock.deadlines.get()
    assert skipped_deadline == initial_monotonic_ns + 12_000_000_000
    assert len(adapter.calls) == 1
    clock.advance_to(skipped_deadline)
    await adapter.cycle_completed(2).wait()
    await _stop_after_next_deadline(task, clock=clock, scheduler=scheduler)

    assert adapter.calls == [
        AttemptCall("BTCUSDT", 1, 0, 10_000, 1),
        AttemptCall("BTCUSDT", 2, 0, 20_000, 1),
    ]
    queued = await _drain_queued_records(adapter)
    gaps = _gap_payloads(queued, plan=plan)
    slots = _slot_payloads(queued, plan=plan)
    assert len(gaps) == 1
    assert gaps[0][1].first_slot_wall_ms == 15_000
    assert gaps[0][1].end_slot_exclusive_wall_ms == 20_000
    assert gaps[0][1].covered_slot_count == 1
    assert [payload.scheduled_slot_wall_ms for _, payload in slots] == [10_000, 20_000]


@pytest.mark.asyncio
async def test_scheduler_never_launches_more_than_four_attempts_at_once() -> None:
    symbols = tuple(f"S{index:02d}USDT" for index in range(32))
    plan = _rest_plan(symbols)
    gate = asyncio.Event()
    clock = FakeSchedulerClock(wall_ms=20_001, monotonic_ns=500)
    adapter = RecordingAttemptAdapter(
        plan,
        expected_symbols_per_cycle=len(symbols),
        gate=gate,
    )
    scheduler = _scheduler(plan, adapter, clock=clock)
    task = asyncio.create_task(scheduler.run())

    first_chunk = [await adapter.started.get() for _ in range(4)]
    assert [call.symbol_ordinal for call in first_chunk] == [0, 1, 2, 3]
    assert adapter.started.empty()
    assert scheduler.in_flight_attempt_count == 4
    gate.set()
    await adapter.cycle_completed(1).wait()
    await _stop_after_next_deadline(task, clock=clock, scheduler=scheduler)

    assert len(adapter.calls) == 32
    assert [call.symbol_ordinal for call in adapter.calls] == list(range(32))
    assert adapter.maximum_active == 4
    assert scheduler.drained
    queued = await _drain_queued_records(adapter)
    slots = _slot_payloads(queued, plan=plan)
    assert len(slots) == 1
    assert len(slots[0][1].entries) == 32
    assert all(
        entry.outcome is PublicOiRestCellOutcomeV2.ATTEMPT_RETAINED
        for entry in slots[0][1].entries
    )


@pytest.mark.asyncio
async def test_normal_stop_drains_launched_chunk_and_skips_remaining_symbols() -> None:
    symbols = tuple(f"S{index:02d}USDT" for index in range(6))
    plan = _rest_plan(symbols)
    gate = asyncio.Event()
    clock = FakeSchedulerClock(wall_ms=5_001, monotonic_ns=600)
    adapter = RecordingAttemptAdapter(
        plan,
        expected_symbols_per_cycle=len(symbols),
        gate=gate,
    )
    scheduler = _scheduler(plan, adapter, clock=clock)
    task = asyncio.create_task(scheduler.run())

    launched = [await adapter.started.get() for _ in range(4)]
    await scheduler.request_normal_stop()
    gate.set()
    await task

    assert [call.symbol for call in launched] == list(plan.symbols[:4])
    assert len(adapter.calls) == 4
    assert sorted(adapter.completed_symbols) == sorted(plan.symbols[:4])
    assert adapter.cancelled_symbols == []
    assert scheduler.last_started_poll_cycle_seq == 1
    assert scheduler.last_completed_poll_cycle_seq == 0
    assert scheduler.drained
    queued = await _drain_queued_records(adapter)
    slots = _slot_payloads(queued, plan=plan)
    closes = _close_payloads(queued, plan=plan)
    assert len(slots) == 1
    assert len(closes) == 1
    outcomes = tuple(entry.outcome for entry in slots[0][1].entries)
    assert outcomes == (
        PublicOiRestCellOutcomeV2.ATTEMPT_RETAINED,
        PublicOiRestCellOutcomeV2.ATTEMPT_RETAINED,
        PublicOiRestCellOutcomeV2.ATTEMPT_RETAINED,
        PublicOiRestCellOutcomeV2.ATTEMPT_RETAINED,
        PublicOiRestCellOutcomeV2.UNSTARTED_NORMAL_STOP,
        PublicOiRestCellOutcomeV2.UNSTARTED_NORMAL_STOP,
    )
    attempts = tuple(record for record in queued if record.record.symbol is not None)
    for entry, admitted in zip(slots[0][1].entries[:4], attempts, strict=True):
        assert entry.attempt_ingest_seq == admitted.ingest_seq
        assert entry.attempt_record_sha256 == admitted.encoded_sha256
        assert entry.attempt_record_sha256 == public_oi_rest_attempt_record_sha256_v2(
            admitted.record
        )
        assert admitted.ingest_seq < slots[0][0].ingest_seq
    assert closes[0][1].last_census_ingest_seq == slots[0][0].ingest_seq


@pytest.mark.asyncio
async def test_adapter_failure_cancels_and_joins_chunk_siblings() -> None:
    symbols = ("BTCUSDT", "ETHUSDT", "SOLUSDT", "XRPUSDT")
    plan = _rest_plan(symbols)
    failure = RuntimeError("synthetic OI adapter failure")
    clock = FakeSchedulerClock(wall_ms=5_001, monotonic_ns=700)
    adapter = RecordingAttemptAdapter(
        plan,
        expected_symbols_per_cycle=len(symbols),
        gate=asyncio.Event(),
        failure_symbol="BTCUSDT",
        failure=failure,
        started_signal_count=4,
    )
    scheduler = _scheduler(plan, adapter, clock=clock)
    task = asyncio.create_task(scheduler.run())

    await adapter.all_started.wait()
    with pytest.raises(RuntimeError) as captured:
        await task

    assert captured.value is failure
    assert sorted(adapter.cancelled_symbols) == ["ETHUSDT", "SOLUSDT", "XRPUSDT"]
    assert adapter.active == 0
    assert scheduler.drained
    assert not scheduler.running
    assert scheduler.last_completed_poll_cycle_seq == 0
    assert not scheduler.coverage_closed
    assert scheduler.coverage_close_receipt is None


@pytest.mark.asyncio
async def test_caller_cancellation_cancels_and_joins_active_chunk() -> None:
    symbols = ("BTCUSDT", "ETHUSDT", "SOLUSDT", "XRPUSDT")
    plan = _rest_plan(symbols)
    clock = FakeSchedulerClock(wall_ms=5_001, monotonic_ns=800)
    adapter = RecordingAttemptAdapter(
        plan,
        expected_symbols_per_cycle=len(symbols),
        gate=asyncio.Event(),
        started_signal_count=4,
    )
    scheduler = _scheduler(plan, adapter, clock=clock)
    task = asyncio.create_task(scheduler.run())

    await adapter.all_started.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    assert sorted(adapter.cancelled_symbols) == sorted(symbols)
    assert adapter.active == 0
    assert scheduler.drained
    assert not scheduler.running
    assert not scheduler.coverage_closed
    assert scheduler.coverage_close_receipt is None


@pytest.mark.asyncio
async def test_scheduler_rejects_every_second_run_even_after_clean_return() -> None:
    plan = _rest_plan(("BTCUSDT",))
    adapter = RecordingAttemptAdapter(plan, expected_symbols_per_cycle=1)
    clock = FakeSchedulerClock(wall_ms=0, monotonic_ns=0)
    scheduler = _scheduler(plan, adapter, clock=clock)
    await scheduler.request_normal_stop()

    await scheduler.run()
    with pytest.raises(RuntimeError, match="only once"):
        await scheduler.run()

    assert adapter.calls == []
    assert scheduler.started_once
    assert scheduler.drained


@pytest.mark.asyncio
async def test_expired_slot_after_first_chunk_marks_cold_cycle_incomplete() -> None:
    symbols = tuple(f"S{index:02d}USDT" for index in range(6))
    plan = _rest_plan(symbols)
    gate = asyncio.Event()
    clock = FakeSchedulerClock(wall_ms=14_999, monotonic_ns=10)
    adapter = RecordingAttemptAdapter(
        plan,
        expected_symbols_per_cycle=len(symbols),
        gate=gate,
    )
    scheduler = _scheduler(plan, adapter, clock=clock)
    task = asyncio.create_task(scheduler.run())

    await asyncio.gather(*(adapter.started.get() for _ in range(4)))
    clock.set_wall(15_000)
    gate.set()
    await _stop_after_next_deadline(task, clock=clock, scheduler=scheduler)

    assert len(adapter.calls) == 4
    assert scheduler.last_started_poll_cycle_seq == 1
    assert scheduler.last_completed_poll_cycle_seq == 0


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("next_wall_ms", "error_type", "message"),
    [
        (12_344, ValueError, "moved backwards"),
        (12_345, RuntimeError, "did not advance"),
    ],
)
async def test_next_due_fails_closed_on_backward_or_stale_utc_wall(
    next_wall_ms: int,
    error_type: type[Exception],
    message: str,
) -> None:
    plan = _rest_plan(("BTCUSDT",))
    clock = FakeSchedulerClock(wall_ms=12_345, monotonic_ns=100)
    adapter = RecordingAttemptAdapter(plan, expected_symbols_per_cycle=1)
    scheduler = _scheduler(plan, adapter, clock=clock)
    task = asyncio.create_task(scheduler.run())

    await adapter.cycle_completed(1).wait()
    deadline = await clock.deadlines.get()
    clock.set_wall(next_wall_ms)
    clock.advance_to(deadline)

    with pytest.raises(error_type, match=message):
        await task
    assert scheduler.last_completed_poll_cycle_seq == 1
    assert not scheduler.coverage_closed
    assert scheduler.coverage_close_receipt is None


@pytest.mark.asyncio
async def test_missed_slot_is_incomplete_joins_chunk_and_continues_next_due() -> None:
    symbols = tuple(f"S{index:02d}USDT" for index in range(6))
    plan = _rest_plan(symbols)
    clock = FakeSchedulerClock(wall_ms=5_001, monotonic_ns=200)
    gate = asyncio.Event()
    missed = PublicOiRestMissedSlotV2(
        symbol=symbols[0],
        poll_cycle_seq=1,
        symbol_ordinal=0,
        scheduled_slot_wall_ms=5_000,
        observed_request_start_wall_ms=10_000,
    )
    adapter = RecordingAttemptAdapter(
        plan,
        expected_symbols_per_cycle=len(symbols),
        gate=gate,
        failure_symbol=symbols[0],
        failure=missed,
        failure_poll_cycle_seq=1,
        started_signal_count=4,
    )
    scheduler = _scheduler(plan, adapter, clock=clock)
    task = asyncio.create_task(scheduler.run())

    await adapter.all_started.wait()
    clock.set_wall(10_000)
    gate.set()
    deadline = await clock.deadlines.get()
    assert len(adapter.calls) == 4
    assert scheduler.last_completed_poll_cycle_seq == 0
    clock.advance_to(deadline)
    await adapter.cycle_completed(2).wait()
    await _stop_after_next_deadline(task, clock=clock, scheduler=scheduler)

    assert [call.poll_cycle_seq for call in adapter.calls] == [1] * 4 + [2] * 6
    assert adapter.cancelled_symbols == []
    assert sorted(adapter.completed_symbols[:3]) == sorted(symbols[1:4])
    assert scheduler.last_completed_poll_cycle_seq == 2
    queued = await _drain_queued_records(adapter)
    slots = _slot_payloads(queued, plan=plan)
    assert len(slots) == 2
    first = slots[0][1]
    assert tuple(entry.outcome for entry in first.entries) == (
        PublicOiRestCellOutcomeV2.UNSTARTED_SLOT_EXPIRED,
        PublicOiRestCellOutcomeV2.ATTEMPT_RETAINED,
        PublicOiRestCellOutcomeV2.ATTEMPT_RETAINED,
        PublicOiRestCellOutcomeV2.ATTEMPT_RETAINED,
        PublicOiRestCellOutcomeV2.UNSTARTED_SLOT_EXPIRED,
        PublicOiRestCellOutcomeV2.UNSTARTED_SLOT_EXPIRED,
    )
    assert all(
        entry.attempt_ingest_seq is not None
        for entry in first.entries[1:4]
    )


@pytest.mark.asyncio
async def test_noop_adapter_result_is_fatal_and_never_completes_cycle() -> None:
    plan = _rest_plan(("BTCUSDT",))
    adapter = RecordingAttemptAdapter(
        plan,
        expected_symbols_per_cycle=1,
        result_factory=lambda _call, _seq: object(),
    )
    clock = FakeSchedulerClock(wall_ms=5_001, monotonic_ns=1)
    scheduler = _scheduler(plan, adapter, clock=clock)

    with pytest.raises(TypeError, match="exact PublicOiAdmissionReceiptV2"):
        await scheduler.run()
    assert scheduler.last_completed_poll_cycle_seq == 0


@pytest.mark.asyncio
async def test_invalid_result_cancels_and_joins_pending_chunk_siblings() -> None:
    symbols = ("BTCUSDT", "ETHUSDT", "SOLUSDT", "XRPUSDT")
    plan = _rest_plan(symbols)
    adapter = RecordingAttemptAdapter(
        plan,
        expected_symbols_per_cycle=len(symbols),
        gate=asyncio.Event(),
        started_signal_count=len(symbols),
        result_factory=lambda _call, _seq: object(),
        gate_exempt_symbol="BTCUSDT",
    )
    clock = FakeSchedulerClock(wall_ms=5_001, monotonic_ns=1)
    scheduler = _scheduler(plan, adapter, clock=clock)

    with pytest.raises(TypeError, match="exact PublicOiAdmissionReceiptV2"):
        await scheduler.run()

    assert adapter.completed_symbols == ["BTCUSDT"]
    assert sorted(adapter.cancelled_symbols) == ["ETHUSDT", "SOLUSDT", "XRPUSDT"]
    assert adapter.active == 0
    assert scheduler.drained
    assert not scheduler.running
    assert scheduler.last_completed_poll_cycle_seq == 0


@pytest.mark.asyncio
async def test_early_missed_slot_is_fatal_and_joins_pending_siblings() -> None:
    symbols = ("BTCUSDT", "ETHUSDT", "SOLUSDT", "XRPUSDT")
    plan = _rest_plan(symbols)
    missed = PublicOiRestMissedSlotV2(
        symbol="BTCUSDT",
        poll_cycle_seq=1,
        symbol_ordinal=0,
        scheduled_slot_wall_ms=5_000,
        observed_request_start_wall_ms=4_999,
    )
    adapter = RecordingAttemptAdapter(
        plan,
        expected_symbols_per_cycle=len(symbols),
        gate=asyncio.Event(),
        failure_symbol="BTCUSDT",
        failure=missed,
        started_signal_count=len(symbols),
    )
    clock = FakeSchedulerClock(wall_ms=5_001, monotonic_ns=1)
    scheduler = _scheduler(plan, adapter, clock=clock)

    with pytest.raises(RuntimeError, match="missed-slot identity differs") as captured:
        await scheduler.run()

    assert captured.value.__cause__ is missed
    assert sorted(adapter.cancelled_symbols) == ["ETHUSDT", "SOLUSDT", "XRPUSDT"]
    assert adapter.active == 0
    assert scheduler.drained
    assert not scheduler.running
    assert scheduler.last_completed_poll_cycle_seq == 0


@pytest.mark.asyncio
@pytest.mark.parametrize("mismatch", ["outer", "inner", "receipt"])
async def test_wrong_record_or_inner_outer_mismatch_is_fatal(mismatch: str) -> None:
    plan = _rest_plan(("BTCUSDT",))

    def wrong_result(call: AttemptCall, ingest_seq: int) -> PublicOiAdmissionReceiptV2:
        record_call = call
        if mismatch == "inner":
            record_call = replace(call, poll_cycle_seq=call.poll_cycle_seq + 1)
        record = _valid_record(plan, record_call, ingest_seq=ingest_seq)
        if mismatch == "outer":
            record = replace(record, route_id="wrong-public-route")
        if mismatch == "receipt":
            record = replace(record, receipt_wall_ms=record.receipt_wall_ms + 1)
        return _receipt_for_record(record)

    adapter = RecordingAttemptAdapter(
        plan,
        expected_symbols_per_cycle=1,
        result_factory=wrong_result,
    )
    clock = FakeSchedulerClock(wall_ms=5_001, monotonic_ns=1)
    scheduler = _scheduler(plan, adapter, clock=clock)

    with pytest.raises(ValueError, match="differs"):
        await scheduler.run()
    assert scheduler.last_completed_poll_cycle_seq == 0


@pytest.mark.asyncio
async def test_child_self_cancellation_is_runtime_failure_not_outer_cancellation() -> None:
    symbols = ("BTCUSDT", "ETHUSDT", "SOLUSDT", "XRPUSDT")
    plan = _rest_plan(symbols)
    adapter = RecordingAttemptAdapter(
        plan,
        expected_symbols_per_cycle=len(symbols),
        gate=asyncio.Event(),
        failure_symbol="BTCUSDT",
        failure=asyncio.CancelledError(),
        started_signal_count=4,
    )
    clock = FakeSchedulerClock(wall_ms=5_001, monotonic_ns=1)
    scheduler = _scheduler(plan, adapter, clock=clock)

    with pytest.raises(PublicOiRestAttemptSelfCancelledV2):
        await scheduler.run()

    assert scheduler.last_completed_poll_cycle_seq == 0
    assert scheduler.drained


@pytest.mark.asyncio
async def test_stop_during_completed_final_chunk_still_marks_cycle_completed() -> None:
    symbols = ("BTCUSDT", "ETHUSDT", "SOLUSDT", "XRPUSDT")
    plan = _rest_plan(symbols)
    gate = asyncio.Event()
    adapter = RecordingAttemptAdapter(
        plan,
        expected_symbols_per_cycle=len(symbols),
        gate=gate,
        started_signal_count=4,
    )
    clock = FakeSchedulerClock(wall_ms=5_001, monotonic_ns=1)
    scheduler = _scheduler(plan, adapter, clock=clock)
    task = asyncio.create_task(scheduler.run())

    await adapter.all_started.wait()
    await scheduler.request_normal_stop()
    gate.set()
    await task

    assert scheduler.last_completed_poll_cycle_seq == 1
    assert sorted(adapter.completed_symbols) == sorted(symbols)


@pytest.mark.asyncio
async def test_run_without_exact_census_context_never_schedules() -> None:
    plan = _rest_plan(("BTCUSDT",))
    adapter = RecordingAttemptAdapter(plan, expected_symbols_per_cycle=1)
    scheduler = PublicOpenInterestRestSchedulerV2(
        plan,
        adapter,
        clock=FakeSchedulerClock(wall_ms=5_001, monotonic_ns=1),
    )

    with pytest.raises(RuntimeError, match="cannot run without its exact census context"):
        await scheduler.run()

    assert adapter.calls == []
    assert not scheduler.started_once
    assert not scheduler.running


def test_census_context_is_factory_sealed_and_bound_to_one_scheduler() -> None:
    plan = _rest_plan(("BTCUSDT",))
    clock = FakeSchedulerClock(wall_ms=5_001, monotonic_ns=1)
    owner_adapter = RecordingAttemptAdapter(plan, expected_symbols_per_cycle=1)
    owner = _scheduler(plan, owner_adapter, clock=clock)
    context = owner.census_context
    assert context is not None

    with pytest.raises(TypeError, match="exact factory"):
        PublicOiRestCensusContextV2(
            plan=plan,
            session_id=context.session_id,
            session_start_manifest_sha256=context.session_start_manifest_sha256,
            plan_bundle_sha256=context.plan_bundle_sha256,
            protocol_hash=context.protocol_hash,
            coverage_start_slot_wall_ms=context.coverage_start_slot_wall_ms,
            ingress=context.ingress,
            receipt_clock=context.receipt_clock,
        )

    foreign_adapter = RecordingAttemptAdapter(plan, expected_symbols_per_cycle=1)
    with pytest.raises(RuntimeError, match="already bound"):
        PublicOpenInterestRestSchedulerV2(
            plan,
            foreign_adapter,
            census_context=context,
            clock=clock,
        )
    assert foreign_adapter.schedule_authority is None


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("stop_wall_ms", "expected_slot_count", "expected_coverage_end"),
    [(5_000, 0, 5_000), (5_001, 1, 10_000)],
)
async def test_half_open_stop_boundary_excludes_t_and_t_plus_one_includes_t(
    stop_wall_ms: int,
    expected_slot_count: int,
    expected_coverage_end: int,
) -> None:
    plan = _rest_plan(("BTCUSDT", "ETHUSDT"))
    clock = FakeSchedulerClock(wall_ms=stop_wall_ms, monotonic_ns=9)
    adapter = RecordingAttemptAdapter(plan, expected_symbols_per_cycle=2)
    scheduler = _scheduler(
        plan,
        adapter,
        clock=clock,
        coverage_start_slot_wall_ms=5_000,
    )

    first = await scheduler.request_normal_stop()
    second = await scheduler.request_normal_stop()
    await scheduler.run()
    third = await scheduler.request_normal_stop()

    assert first is second is third
    assert adapter.calls == []
    queued = await _drain_queued_records(adapter)
    slots = _slot_payloads(queued, plan=plan)
    closes = _close_payloads(queued, plan=plan)
    assert len(slots) == expected_slot_count
    assert len(closes) == 1
    assert closes[0][1].coverage_end_slot_exclusive_wall_ms == expected_coverage_end
    assert sum(
        queued_record.record.source_logical_key == "openInterest:census"
        for queued_record in queued
    ) == expected_slot_count + 1
    if slots:
        assert all(
            entry.outcome is PublicOiRestCellOutcomeV2.UNSTARTED_NORMAL_STOP
            for entry in slots[0][1].entries
        )
        assert closes[0][1].last_census_ingest_seq == slots[0][0].ingest_seq
    else:
        assert closes[0][1].last_census_ingest_seq is None


@pytest.mark.asyncio
async def test_cancelled_stop_caller_cannot_lose_sampled_write_once_boundary() -> None:
    plan = _rest_plan(("BTCUSDT",))
    clock = FakeSchedulerClock(wall_ms=5_001, monotonic_ns=11)
    adapter = RecordingAttemptAdapter(plan, expected_symbols_per_cycle=1)
    scheduler = _scheduler(plan, adapter, clock=clock)
    await scheduler._control_lock.acquire()
    stop_task = asyncio.create_task(scheduler.request_normal_stop())
    await asyncio.sleep(0)
    stop_task.cancel()
    await asyncio.sleep(0)
    assert scheduler.normal_stop_receipt is None
    scheduler._control_lock.release()

    with pytest.raises(asyncio.CancelledError):
        await stop_task

    receipt = scheduler.normal_stop_receipt
    assert receipt == ReceiptTimestamp(5_001, 11)
    assert scheduler.in_flight_attempt_count == 0
    await scheduler.run()
    assert scheduler.coverage_closed
    assert len(_close_payloads(await _drain_queued_records(adapter), plan=plan)) == 1


@pytest.mark.asyncio
async def test_exact_active_slot_boundary_is_fatal_and_never_closes_coverage() -> None:
    plan = _rest_plan(("BTCUSDT",))
    gate = asyncio.Event()
    clock = FakeSchedulerClock(wall_ms=5_000, monotonic_ns=21)
    adapter = RecordingAttemptAdapter(
        plan,
        expected_symbols_per_cycle=1,
        gate=gate,
    )
    scheduler = _scheduler(plan, adapter, clock=clock)
    run_task = asyncio.create_task(scheduler.run())
    await adapter.started.get()

    with pytest.raises(PublicOiRestNormalStopBoundaryRaceV2) as requested:
        await scheduler.request_normal_stop()
    gate.set()
    with pytest.raises(PublicOiRestNormalStopBoundaryRaceV2) as run_failure:
        await run_task

    assert requested.value is scheduler.normal_stop_boundary_failure
    assert run_failure.value is requested.value
    assert not scheduler.coverage_closed
    queued = await _drain_queued_records(adapter)
    assert len(tuple(record for record in queued if record.record.symbol is not None)) == 1
    assert _slot_payloads(queued, plan=plan) == ()
    assert _close_payloads(queued, plan=plan) == ()


@pytest.mark.asyncio
async def test_exact_slot_end_marks_unstarted_cells_expired_not_normal_stop() -> None:
    symbols = tuple(f"S{index:02d}USDT" for index in range(6))
    plan = _rest_plan(symbols)
    gate = asyncio.Event()
    clock = FakeSchedulerClock(wall_ms=5_001, monotonic_ns=31)
    adapter = RecordingAttemptAdapter(plan, expected_symbols_per_cycle=6, gate=gate)
    scheduler = _scheduler(plan, adapter, clock=clock)
    run_task = asyncio.create_task(scheduler.run())
    await asyncio.gather(*(adapter.started.get() for _ in range(4)))
    clock.set_wall(10_000)
    await scheduler.request_normal_stop()
    gate.set()
    await run_task

    queued = await _drain_queued_records(adapter)
    slots = _slot_payloads(queued, plan=plan)
    assert len(slots) == 1
    assert tuple(entry.outcome for entry in slots[0][1].entries) == (
        PublicOiRestCellOutcomeV2.ATTEMPT_RETAINED,
        PublicOiRestCellOutcomeV2.ATTEMPT_RETAINED,
        PublicOiRestCellOutcomeV2.ATTEMPT_RETAINED,
        PublicOiRestCellOutcomeV2.ATTEMPT_RETAINED,
        PublicOiRestCellOutcomeV2.UNSTARTED_SLOT_EXPIRED,
        PublicOiRestCellOutcomeV2.UNSTARTED_SLOT_EXPIRED,
    )
    assert _close_payloads(queued, plan=plan)[0][1].coverage_end_slot_exclusive_wall_ms == 10_000


@pytest.mark.asyncio
async def test_stop_commit_does_not_wait_for_blocked_census_ingress() -> None:
    plan = _rest_plan(("BTCUSDT",))
    post_admission_gate = asyncio.Event()
    clock = FakeSchedulerClock(wall_ms=5_001, monotonic_ns=41)
    adapter = RecordingAttemptAdapter(
        plan,
        expected_symbols_per_cycle=1,
        post_admission_gate=post_admission_gate,
    )
    scheduler = _scheduler(plan, adapter, clock=clock)
    context = scheduler.census_context
    assert context is not None
    run_task = asyncio.create_task(scheduler.run())
    await adapter.admitted.get()
    await context.ingress._ingress_lock.acquire()
    try:
        post_admission_gate.set()
        await asyncio.sleep(0)
        await asyncio.sleep(0)
        assert not scheduler._control_lock.locked()
        receipt = await asyncio.wait_for(scheduler.request_normal_stop(), timeout=0.5)
        assert receipt == ReceiptTimestamp(5_001, 41)
    finally:
        context.ingress._ingress_lock.release()
    await run_task

    assert scheduler.coverage_closed
    queued = await _drain_queued_records(adapter)
    slots = _slot_payloads(queued, plan=plan)
    closes = _close_payloads(queued, plan=plan)
    assert len(slots) == 1
    assert len(closes) == 1
    assert slots[0][0].ingest_seq < closes[0][0].ingest_seq
    assert closes[0][1].last_census_ingest_seq == slots[0][0].ingest_seq
