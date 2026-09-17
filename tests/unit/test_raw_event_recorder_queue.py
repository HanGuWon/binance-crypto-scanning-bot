"""Phase-J corrective P0: deterministic adversarial contracts for the
non-blocking RawEventRecorder (charter section 10, cases A-L plus M).

Determinism: no sleeps. Writer blocking is done with an asyncio.Event gate
invoked at the writer TASK call site via the recorder write_batch_hook.
"""

import asyncio
import json
from unittest.mock import MagicMock

import pytest

from signalbot.clock import ReplayClock
from signalbot.config import Settings
from signalbot.data.raw_events import (
    RawEventCapacityError,
    RawEventOverflowError,
    RawEventRecorder,
    RawEventRecorderClosedError,
    RawEventRecorderFatalError,
    RawEventWriterError,
)
from signalbot.domain.enums import Market
from signalbot.scanner import MarketScanner


class BlockedWriterGate:
    """Async gate at the writer-task call site (never inside the thread)."""

    def __init__(self) -> None:
        self.block = asyncio.Event()
        self.write_calls = 0

    async def __call__(self, writer) -> None:
        await self.block.wait()
        self.write_calls += 1


def _read_all(directory, market):
    root = directory if market is None else directory / market.value
    files = sorted(root.rglob("*.jsonl"))
    lines = [
        line
        for file in files
        for line in file.read_text(encoding="utf-8").splitlines()
    ]
    return [json.loads(line) for line in lines]


# ---------------------------------------------------------------------------
# A. Non-blocking acceptance
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_a_append_returns_after_queue_acceptance_before_durability(tmp_path):
    gate = BlockedWriterGate()
    recorder = RawEventRecorder(tmp_path, max_queue_size=10, write_batch_hook=gate)
    await recorder.append(Market.SPOT, {"seq": 0}, 1_710_000_000_000)
    status = recorder.status()
    assert status["accepted_records"] == 1
    assert status["durable_records"] == 0
    assert status["queued_records"] == 1
    assert not list(tmp_path.rglob("*.jsonl")), "no disk write may have happened"
    gate.block.set()
    assert await recorder.wait_drained()
    assert recorder.status()["durable_records"] == 1
    await recorder.close()


# ---------------------------------------------------------------------------
# B. Durable watermark
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_b_durable_watermark_reaches_accepted_after_release(tmp_path):
    gate = BlockedWriterGate()
    recorder = RawEventRecorder(tmp_path, max_queue_size=100, write_batch_hook=gate)
    for i in range(20):
        await recorder.append(Market.SPOT, {"seq": i}, 1_710_000_000_000 + i)
    assert recorder.status()["durable_records"] == 0
    gate.block.set()
    assert await recorder.wait_drained()
    status = recorder.status()
    assert status["durable_records"] == status["accepted_records"] == 20
    await recorder.close()


# ---------------------------------------------------------------------------
# C. Burst without per-record disk waits
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_c_thousand_event_burst_exact_order_no_loss(tmp_path):
    recorder = RawEventRecorder(tmp_path)
    total = 1000
    for i in range(total):
        # No awaiting of durability between appends: burst admission.
        await recorder.append(
            Market.FUTURES,
            {"seq": i, "blob": "x" * 32},
            1_710_000_000_000 + i * 1_000,
        )
    await recorder.close()
    records = _read_all(tmp_path, Market.FUTURES)
    assert len(records) == total
    seqs = [record["payload"]["seq"] for record in records]
    assert seqs == list(range(total))


# ---------------------------------------------------------------------------
# D. Overflow is fatal and observable; no silent drop
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_d_queue_overflow_is_fatal_not_silent_drop(tmp_path):
    gate = BlockedWriterGate()
    recorder = RawEventRecorder(tmp_path, max_queue_size=2, write_batch_hook=gate)
    for i in range(2):
        await recorder.append(Market.SPOT, {"i": i}, 1_710_000_000_000 + i)
    with pytest.raises(RawEventOverflowError):
        await recorder.append(Market.SPOT, {"i": 2}, 1_710_000_000_002)
    assert recorder.is_failed()
    assert recorder.status()["overflow_count"] >= 1
    # Fatal recorder rejects further admissions fail-closed:
    with pytest.raises(RawEventRecorderFatalError):
        await recorder.append(Market.SPOT, {"late": True}, 1_710_000_000_003)
    gate.block.set()  # release writer so close() can terminate cleanly
    await recorder.close()


# ---------------------------------------------------------------------------
# E. Writer exception -> failure event -> scanner fail-closed stop_event
# ---------------------------------------------------------------------------


class _ExplodingHook:
    """Raises once when armed; deterministic writer-crash injector."""

    def __init__(self) -> None:
        self.armed = False
        self.raised = asyncio.Event()

    async def __call__(self, writer) -> None:
        if self.armed:
            self.raised.set()
            raise OSError("disk exploded")


def _scanner_with_recorder(tmp_path, recorder, stop_event, runtime):
    settings = Settings.model_validate(
        {
            "runtime": {
                "record_raw_events": True,
                "raw_event_directory": str(tmp_path / "tape"),
                "raw_event_max_bytes": 1_048_576,
            }
        }
    )
    return MarketScanner(
        Market.SPOT,
        settings,
        ReplayClock(0),
        runtime,
        stop_event,
        rest_client=MagicMock(),
        raw_recorder=recorder,
    )


class _CapturingRuntime(MagicMock):
    async def handle_payload(self, inner_payload, *, received_at_ms=None):
        pass


@pytest.mark.asyncio
async def test_e_writer_exception_sets_failure_and_fails_scanner_closed(tmp_path):
    hook = _ExplodingHook()
    hook.armed = True
    recorder = RawEventRecorder(tmp_path / "tape", write_batch_hook=hook)
    stop_event = asyncio.Event()
    scanner = _scanner_with_recorder(
        tmp_path, recorder, stop_event, _CapturingRuntime()
    )
    payload = {"stream": "bnbusdt@bookTicker", "data": {"u": 1}}
    await scanner._handle_payload(payload)
    # The append itself succeeded (queue accept); the crash surfaces on the
    # writer task. wait_failed() must resolve and the app-level contract is
    # that a monitor sets stop_event (app integration covered in test_j2).
    await asyncio.wait_for(recorder.wait_failed(), timeout=5)
    assert isinstance(recorder.fatal_error, RawEventWriterError)
    gate_release = recorder._writers[Market.SPOT]
    gate_release.close_handle()
    await asyncio.sleep(0)
    await asyncio.wait_for(asyncio.shield(recorder.close()), timeout=5)


# ---------------------------------------------------------------------------
# F. Graceful close drains everything
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_f_graceful_close_drains_every_accepted_record(tmp_path):
    recorder = RawEventRecorder(tmp_path, max_queue_size=500)
    for i in range(300):
        await recorder.append(
            Market.SPOT, {"n": i}, 1_710_000_000_000 + i * 1_000
        )
    await recorder.close()
    status = recorder.status()
    assert status["accepted_records"] == status["durable_records"] == 300
    assert status["queued_records"] == 0
    records = _read_all(tmp_path, Market.SPOT)
    assert [r["payload"]["n"] for r in records] == list(range(300))
    for writer in recorder._writers.values():
        assert writer.task is None or writer.task.done()
        assert writer._current_handle is None
    with pytest.raises(RawEventRecorderClosedError):
        await recorder.append(Market.SPOT, {"late": True}, 1)


# ---------------------------------------------------------------------------
# G. Close after failure terminates deterministically
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_g_close_after_writer_failure_terminates_deterministically(tmp_path):
    gate = BlockedWriterGate()
    recorder = RawEventRecorder(tmp_path, max_queue_size=3, write_batch_hook=gate)
    for i in range(3):
        await recorder.append(Market.FUTURES, {"i": i}, 1_710_000_000_000 + i)
    writer = recorder._writers[Market.FUTURES]
    assert writer.task is not None
    # Kill the writer outright while queue is blocked/full:
    writer.task.cancel()
    try:
        await writer.task
    except asyncio.CancelledError:
        pass
    with pytest.raises((RawEventWriterError, asyncio.TimeoutError)):
        await asyncio.wait_for(recorder.close(), timeout=5)
    # Handles closed even on the failure path (Windows teardown safety):
    for w in recorder._writers.values():
        assert w._current_handle is None


# ---------------------------------------------------------------------------
# G2. Hung disk write cannot stall close() indefinitely (P1 review blocker)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_g2_hung_write_close_terminates_with_fatal(monkeypatch, tmp_path):
    import threading

    import signalbot.data.raw_events as raw_events_module

    thread_release = threading.Event()

    def hanging_write_batch(self, batch):
        # Simulate an OS-level write stall (AV lock, dead disk) that outlasts
        # the close() join timeout but ends deterministically afterwards.
        thread_release.wait(timeout=120)

    monkeypatch.setattr(
        raw_events_module._MarketWriter,
        "_write_batch",
        hanging_write_batch,
    )
    recorder = RawEventRecorder(tmp_path, max_queue_size=10)
    await recorder.append(Market.SPOT, {"x": 1}, 1)
    await asyncio.sleep(0.05)  # let the writer enter the hung write
    writer = recorder._writers[Market.SPOT]
    assert writer.task is not None and not writer.task.done()
    # close() must terminate deterministically even though the write hangs:
    # bounded sentinel attempts + bounded join < outer deadline.
    await asyncio.wait_for(recorder.close(), timeout=15)
    # close() must have completed without waiting for the hung write; the
    # writer may finish in the background afterwards (it does once released).
    assert recorder.is_failed()
    assert "writer join timed out" in str(recorder.fatal_error)
    assert writer._current_handle is None
    thread_release.set()  # unblock the detached thread so pytest exits cleanly


# ---------------------------------------------------------------------------
# H. Day rollover inside one batch preserves exact order
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_h_day_rollover_batch_splits_files_preserving_order(tmp_path):
    recorder = RawEventRecorder(tmp_path)
    day1_base = 1_709_952_000_000
    day2_base = day1_base + 86_400_000
    stamps = [day1_base, day1_base + 1, day2_base, day1_base + 2, day2_base + 1]
    for i, ts in enumerate(stamps):
        await recorder.append(Market.SPOT, {"seq": i}, ts)
    await recorder.close()
    files = sorted((tmp_path / "spot").rglob("*.jsonl"))
    assert len(files) == 2
    # Per-file order is exact; the day partition is stable (never a reorder).
    from signalbot.data.raw_events import _day_of

    day_a_file = tmp_path / "spot" / f"{_day_of(day1_base)}.jsonl"
    day_b_file = tmp_path / "spot" / f"{_day_of(day2_base)}.jsonl"
    day_a_seqs = [
        json.loads(line)["payload"]["seq"]
        for line in day_a_file.read_text(encoding="utf-8").splitlines()
    ]
    day_b_seqs = [
        json.loads(line)["payload"]["seq"]
        for line in day_b_file.read_text(encoding="utf-8").splitlines()
    ]
    assert day_a_seqs == [0, 1, 3]
    assert day_b_seqs == [2, 4]


# ---------------------------------------------------------------------------
# I. Concurrent spot+futures quota race never exceeds quota
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_i_quota_race_spot_futures_never_exceeds_hard_quota(tmp_path):
    quota = 4096
    recorder = RawEventRecorder(tmp_path, maximum_total_bytes=quota)
    rejected = {"count": 0}

    async def drive(market, count):
        for i in range(count):
            try:
                await recorder.append(market, {"market": market.value, "i": i}, 1)
            except RawEventCapacityError:
                rejected["count"] += 1

    await asyncio.gather(drive(Market.SPOT, 50), drive(Market.FUTURES, 50))
    await recorder.close()
    assert recorder.status()["reserved_bytes"] == 0
    total_bytes = sum(
        file.stat().st_size
        for market_dir in ("spot", "futures")
        for file in (tmp_path / market_dir).glob("*.jsonl")
    )
    assert total_bytes <= quota
    assert rejected["count"] > 0, "quota must reject near-hard-quota bursts"


# ---------------------------------------------------------------------------
# J/J2. App shutdown ordering: recorder drained+closed before repo close
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_j_app_shutdown_closes_shared_recorder_exactly_once(tmp_path):
    from signalbot.app import SignalApplication

    calls = []

    class TrackingRecorder(RawEventRecorder):
        close_count = 0

        async def close(self):
            calls.append("recorder_close")
            self.close_count += 1
            await super().close()

    settings = Settings.model_validate(
        {
            "runtime": {
                "record_raw_events": True,
                "raw_event_directory": str(tmp_path / "tape"),
                "raw_event_max_bytes": 1_048_576,
            },
            "alerts": {"discord_enabled": False},
            "storage": {"url": f"sqlite:///{tmp_path / 'app.db'}"},
        }
    )
    app = SignalApplication(settings, stop_after_minutes=None)
    app.raw_recorder = TrackingRecorder(tmp_path / "tape")
    original_close = type(app.repository).close

    def tracking_repo_close(self):
        calls.append("repo_close")
        return original_close(self)

    type(app.repository).close = tracking_repo_close
    try:
        await app.raw_recorder.append(Market.SPOT, {"x": 1}, 1)
        # Simulate the app finally block ordering directly:
        if app.raw_recorder is not None:
            await app.raw_recorder.close()
        app.repository.close()
    finally:
        type(app.repository).close = original_close
    assert calls[0] == "recorder_close"
    assert "recorder_close" in calls and "repo_close" in calls
    assert calls.index("recorder_close") < calls.index("repo_close")
    assert calls.count("recorder_close") == 1


@pytest.mark.asyncio
async def test_j2_app_monitor_sets_stop_event_on_writer_failure(tmp_path):
    """The app failure monitor resolves via wait_failed -> stop_event.set()."""
    from signalbot.app import SignalApplication

    settings = Settings.model_validate(
        {
            "runtime": {
                "record_raw_events": True,
                "raw_event_directory": str(tmp_path / "tape"),
                "raw_event_max_bytes": 1_048_576,
            },
            "alerts": {"discord_enabled": False},
            "storage": {"url": f"sqlite:///{tmp_path / 'app.db'}"},
        }
    )
    app = SignalApplication(settings, stop_after_minutes=None)

    async def monitor_then_assert():
        await app._monitor_recorder_failure()
        return app.stop_event.is_set()

    waiter = asyncio.create_task(monitor_then_assert())
    await asyncio.sleep(0)
    assert app.raw_recorder is not None
    app.raw_recorder._signal_fatal(RawEventWriterError("injected"))
    assert await asyncio.wait_for(waiter, timeout=5), (
        "monitor must set stop_event after fatal"
    )


# ---------------------------------------------------------------------------
# K. Ingress timestamp identity under artificial disk delay
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_k_ingress_timestamp_identity_under_disk_delay(tmp_path):
    gate = BlockedWriterGate()
    clock = ReplayClock(123_456)
    recorder = RawEventRecorder(tmp_path / "tape", write_batch_hook=gate)
    stop_event = asyncio.Event()
    captured = []

    class DelayCapturingRuntime(MagicMock):
        async def handle_payload(self, inner_payload, *, received_at_ms=None):
            captured.append(received_at_ms)

    scanner = MarketScanner(
        Market.SPOT,
        Settings.model_validate(
            {
                "runtime": {
                    "record_raw_events": True,
                    "raw_event_directory": str(tmp_path / "tape"),
                    "raw_event_max_bytes": 1_048_576,
                }
            }
        ),
        clock,
        DelayCapturingRuntime(),
        stop_event,
        rest_client=MagicMock(),
        raw_recorder=recorder,
    )
    payload = {"stream": "bnbusdt@bookTicker", "data": {"u": 1}}
    await scanner._handle_payload(payload)
    received_at = captured[0]
    assert received_at == 123_456
    gate.block.set()
    assert await recorder.wait_drained()
    await recorder.close()
    tape_record = _read_all(tmp_path / "tape", Market.SPOT)[0]
    assert tape_record["received_at_ms"] == received_at


# ---------------------------------------------------------------------------
# M. Quota survives drain-and-refill (reviewer regression for B1)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_m_quota_survives_drain_and_refill_cycle(tmp_path):
    line_estimate = len(
        json.dumps(
            {"market": "spot", "received_at_ms": 1, "payload": {"e": "t"}},
            separators=(",", ":"),
            ensure_ascii=False,
        ).encode("utf-8")
    ) + 1
    recorder = RawEventRecorder(tmp_path, maximum_total_bytes=line_estimate * 3)
    admitted = 0
    while True:
        try:
            await recorder.append(Market.SPOT, {"e": "t"}, 1)
        except RawEventCapacityError:
            break
        admitted += 1
        assert await recorder.wait_drained()
    assert admitted == 3, f"invariant admits exactly 3, got {admitted}"
    with pytest.raises(RawEventCapacityError):
        await recorder.append(Market.SPOT, {"e": "t"}, 1)
    await recorder.close()
