"""Phase-M WP2: deterministic adversarial contracts for the async
ProspectiveTapeRecorder (devlog/_plan/260826_phase-m/020_wp2_async_core.md).

Cases A-G mirror tests/unit/test_raw_event_recorder_queue.py. Determinism:
no sleeps; writer blocking uses an asyncio gate invoked in the writer TASK
right before the to_thread batch sink.
"""

from __future__ import annotations

import asyncio
import json
from unittest.mock import MagicMock

import pytest

from signalbot.clock import ReplayClock
from signalbot.config import Settings
from signalbot.data.raw_events import (
    RawEventCapacityError,
    RawEventOverflowError,
    RawEventRecorderFatalError,
    RawEventWriterError,
)
from signalbot.domain.enums import Market
from signalbot.prospective.segmented_replay import iter_segmented_zstd
from signalbot.prospective.segmented_storage import ProspectiveTapeRecorder

BASE = 1_710_000_000_000


class BlockedBackendGate:
    """Async gate at the writer-task call site (never inside the thread)."""

    def __init__(self) -> None:
        self.block = asyncio.Event()
        self.calls = 0

    async def __call__(self, writer) -> None:
        self.calls += 1
        await self.block.wait()


class ExplodingBackendGate:
    def __init__(self) -> None:
        self.armed = False

    async def __call__(self, writer) -> None:
        if self.armed:
            raise OSError("segment sink exploded")


def make_recorder(tmp_path, *, quota=1 << 30, queue_size=100, hook=None):
    return ProspectiveTapeRecorder(
        tmp_path / "prospective",
        campaign_id="wp2-test",
        source_identity="worktree-source-v1:wp2test",
        max_queue_size=queue_size,
        raw_event_max_bytes=quota,
        write_batch_hook=hook,
    )


def read_records(tmp_path, market):
    return list(iter_segmented_zstd(tmp_path / "prospective" / market.value))


# --- A. Non-blocking acceptance ---------------------------------------------


@pytest.mark.asyncio
async def test_a_append_returns_after_queue_acceptance_before_backend(tmp_path):
    gate = BlockedBackendGate()
    recorder = make_recorder(tmp_path, queue_size=10, hook=gate)
    await recorder.append(Market.FUTURES, {"seq": 0}, BASE)
    status = recorder.status()
    assert status["accepted_records"] == 1
    assert status["durable_records"] == 0
    assert status["queued_records"] == 1
    assert not list((tmp_path / "prospective").rglob("*.jsonl.zst"))
    gate.block.set()
    assert await recorder.wait_drained()
    assert recorder.status()["durable_records"] == 1
    await recorder.close()


@pytest.mark.asyncio
async def test_a2_append_resolves_while_sink_blocked_then_durable_catches_up(
    tmp_path,
):
    """Red/green proof: on the OLD sync recorder this append would only
    resolve after the disk write; here it resolves during the block."""
    gate = BlockedBackendGate()
    recorder = make_recorder(tmp_path, queue_size=5, hook=gate)
    await recorder.append(Market.FUTURES, {"first": True}, BASE)
    for _ in range(4):
        await asyncio.sleep(0)
    assert gate.calls >= 1, "writer task must be blocked inside the gate"
    await asyncio.wait_for(
        recorder.append(Market.FUTURES, {"second": True}, BASE + 1), timeout=2
    )
    assert recorder.accepted_records == 2
    assert recorder.durable_records == 0
    gate.block.set()
    assert await recorder.wait_drained()
    await recorder.close()
    records = read_records(tmp_path, Market.FUTURES)
    assert [r["payload"] for r in records] == [{"first": True}, {"second": True}]


# --- B. Durable watermark catches accepted -----------------------------------


@pytest.mark.asyncio
async def test_b_durable_watermark_reaches_accepted_after_release(tmp_path):
    gate = BlockedBackendGate()
    recorder = make_recorder(tmp_path, queue_size=100, hook=gate)
    for i in range(25):
        await recorder.append(Market.SPOT, {"seq": i}, BASE + i)
    assert recorder.status()["durable_records"] == 0
    gate.block.set()
    assert await recorder.wait_drained()
    status = recorder.status()
    assert status["durable_records"] == status["accepted_records"] == 25
    await recorder.close()
    records = read_records(tmp_path, Market.SPOT)
    assert [r["payload"]["seq"] for r in records] == list(range(25))


# --- C. Burst without per-record disk waits; per-market exact order ----------


@pytest.mark.asyncio
async def test_c_burst_exact_order_no_loss(tmp_path):
    recorder = make_recorder(tmp_path, queue_size=1000)
    total = 600
    for i in range(total):
        # No durability awaits between appends: true burst admission.
        await recorder.append(Market.FUTURES, {"seq": i}, BASE + i)
    await recorder.close()
    records = read_records(tmp_path, Market.FUTURES)
    assert len(records) == total
    seqs = [r["payload"]["seq"] for r in records]
    assert seqs == list(range(total))


@pytest.mark.asyncio
async def test_c2_same_timestamp_admission_order_preserved(tmp_path):
    recorder = make_recorder(tmp_path)
    for i in range(40):
        await recorder.append(Market.SPOT, {"i": i}, BASE)
    await recorder.close()
    records = read_records(tmp_path, Market.SPOT)
    assert [r["payload"]["i"] for r in records] == list(range(40))


# --- D. Overflow is fatal and observable; no silent drop ---------------------


@pytest.mark.asyncio
async def test_d_queue_overflow_is_fatal_not_silent_drop(tmp_path):
    gate = BlockedBackendGate()
    recorder = make_recorder(tmp_path, queue_size=2, hook=gate)
    for i in range(2):
        await recorder.append(Market.SPOT, {"i": i}, BASE + i)
    with pytest.raises(RawEventOverflowError):
        await recorder.append(Market.SPOT, {"i": 2}, BASE + 2)
    assert recorder.is_failed()
    assert recorder.status()["overflow_count"] >= 1
    with pytest.raises(RawEventRecorderFatalError):
        await recorder.append(Market.SPOT, {"late": True}, BASE + 3)
    gate.block.set()
    await recorder.close()


@pytest.mark.asyncio
async def test_d2_quota_exhaustion_raises_capacity_error(tmp_path):
    line_bytes = len(
        json.dumps(
            {"market": "spot", "received_at_ms": 1, "payload": {"e": "t"}},
            separators=(",", ":"),
            ensure_ascii=False,
        ).encode("utf-8")
    )
    recorder = make_recorder(tmp_path, quota=line_bytes * 3)
    admitted = 0
    while True:
        try:
            await recorder.append(Market.SPOT, {"e": "t"}, BASE)
            admitted += 1
        except RawEventCapacityError:
            break
    assert admitted >= 1
    assert recorder.reserved_bytes <= line_bytes * 3
    await recorder.close()


@pytest.mark.asyncio
async def test_i_quota_race_spot_futures_never_exceeds(tmp_path):
    import asyncio as _asyncio

    line = json.dumps(
        {"market": "spot", "received_at_ms": 1, "payload": {"e": "t"}},
        separators=(",", ":"),
        ensure_ascii=False,
    ) + "\n"
    encoded = len(line.encode("utf-8"))
    quota = encoded * 20
    recorder = make_recorder(tmp_path, quota=quota)
    admitted = {"spot": 0, "futures": 0}
    overflow: list[Exception] = []

    async def drive(market):
        for _ in range(100):
            try:
                await recorder.append(market, {"e": "t"}, BASE)
                admitted[market.value] += 1
            except RawEventCapacityError:
                return
            except Exception as exc:  # pragma: no cover
                overflow.append(exc)
                return

    await _asyncio.gather(
        drive(Market.SPOT), drive(Market.FUTURES)
    )
    assert not overflow
    total_accepted = sum(admitted.values())
    assert total_accepted >= 2  # both markets made progress
    # Combined accepted bytes can NEVER exceed the hard quota.
    assert (
        recorder.accepted_records * encoded <= quota
    )
    await recorder.wait_drained(10)
    snap = recorder.quota.snapshot()
    projected_after_drain = (
        snap["baseline_bytes"]
        + snap["durable_data_bytes"]
        + snap["durable_manifest_bytes"]
        + snap["reserved_bytes"]
    )
    assert projected_after_drain <= quota + 4096  # manifest margin


# --- E. Writer exception -> failure event (real Event, no sleep) -------------


@pytest.mark.asyncio
async def test_e_writer_exception_sets_real_failure_event(tmp_path):
    from signalbot.scanner import MarketScanner

    hook = ExplodingBackendGate()
    hook.armed = True
    recorder = make_recorder(tmp_path, hook=hook)
    stop_event = asyncio.Event()

    class _CapturingRuntime(MagicMock):
        async def handle_payload(self, inner_payload, *, received_at_ms=None):
            pass

    scanner = MarketScanner(
        Market.SPOT,
        Settings.model_validate(
            {
                "runtime": {
                    "record_raw_events": True,
                    "raw_event_directory": str(tmp_path / "tape"),
                    "raw_event_max_bytes": 1_048_576,
                    "storage_mode": "segmented_zstd_v1",
                }
            }
        ),
        ReplayClock(BASE),
        _CapturingRuntime(),
        stop_event,
        rest_client=MagicMock(),
        raw_recorder=recorder,  # type: ignore[arg-type]
    )
    payload = {"stream": "bnbusdt@bookTicker", "data": {"u": 1}}
    await scanner._handle_payload(payload)
    # Queue accept succeeded; crash surfaces via the real Event:
    await asyncio.wait_for(recorder.wait_failed(), timeout=5)
    assert isinstance(recorder.fatal_error, RawEventWriterError)
    detail = str(recorder.fatal_error)
    assert "accepted=" in detail and "lost_including_batch=" in detail
    with pytest.raises(RawEventRecorderFatalError):
        await recorder.append(Market.SPOT, {"late": True}, BASE + 1)
    # Close after failure must terminate deterministically and surface the
    # evidence loss (accepted-but-lost batch) instead of returning silently.
    with pytest.raises(RawEventWriterError):
        await asyncio.wait_for(asyncio.shield(recorder.close()), timeout=5)


# --- F. Graceful close drains everything and finalizes -----------------------


@pytest.mark.asyncio
async def test_f_graceful_close_drains_every_accepted_record(tmp_path):
    recorder = make_recorder(tmp_path, queue_size=500)
    for i in range(200):
        await recorder.append(Market.SPOT, {"n": i}, BASE + i * 1_000)
    await recorder.close()
    status = recorder.status()
    assert status["accepted_records"] == status["durable_records"] == 200
    assert status["queued_records"] == 0
    records = read_records(tmp_path, Market.SPOT)
    assert [r["payload"]["n"] for r in records] == list(range(200))
    d = tmp_path / "prospective" / "spot"
    assert not list(d.glob("*.partial"))


# --- G. Close after failure terminates deterministically ---------------------


@pytest.mark.asyncio
async def test_g_close_after_writer_failure_terminates_deterministically(tmp_path):
    gate = BlockedBackendGate()
    recorder = make_recorder(tmp_path, queue_size=3, hook=gate)
    for i in range(3):
        await recorder.append(Market.FUTURES, {"i": i}, BASE + i)
    task = recorder._tasks[Market.FUTURES]
    assert not task.done()
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass
    with pytest.raises(RawEventWriterError):
        await asyncio.wait_for(recorder.close(), timeout=5)
    assert recorder._writers[Market.FUTURES].closed or recorder.is_failed()


@pytest.mark.asyncio
async def test_g2_close_is_idempotent(tmp_path):
    recorder = make_recorder(tmp_path)
    await recorder.append(Market.SPOT, {"x": 1}, BASE)
    await recorder.close()
    await recorder.close()  # must not raise


# --- H. Reviewer P3: initialization I/O must not live in append -------------


@pytest.mark.asyncio
async def test_h_all_writers_initialized_at_construction(tmp_path):
    """Regression guard for review finding: writer construction + quota
    bootstrap scan must happen at recorder construction, never in append()."""
    gate = BlockedBackendGate()
    recorder = make_recorder(tmp_path, queue_size=5, hook=gate)
    assert set(recorder._writers.keys()) == set(Market), (
        "all per-market writers must exist before any append"
    )
    await recorder.append(Market.FUTURES, {"x": 1}, BASE)
    await recorder.append(Market.SPOT, {"x": 2}, BASE)
    gate.block.set()
    assert await recorder.wait_drained()
    await recorder.close()
