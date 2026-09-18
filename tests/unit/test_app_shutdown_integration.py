"""Phase-K WP1: REAL SignalApplication.run() shutdown integration tests.

These tests execute the actual SignalApplication.run() coroutine with bounded
fakes (no simulated finally-block replay). They fail on the pre-fix
implementation where the recorder-failure monitor never resolves on healthy
stop.
"""

from __future__ import annotations

import asyncio
from unittest.mock import MagicMock

import pytest

from signalbot.config import Settings
from signalbot.domain.enums import Market


class FakeMarketScanner:
    """Bounded fake scanner: controllable lifecycle for integration tests."""

    def __init__(
        self,
        market_name: str,
        *,
        crash_on_start: bool = False,
        close_stall_event: asyncio.Event | None = None,
    ) -> None:
        self.market = MagicMock()
        self.market.value = market_name
        self.crash_on_start = crash_on_start
        self.close_stall_event = close_stall_event
        self.close_calls = 0
        self.started = asyncio.Event()
        self._monitor_done: asyncio.Event | None = None

    async def run(self) -> None:
        self.started.set()
        if self.crash_on_start:
            raise RuntimeError(f"scanner {self.market.value} crashed")
        await self.stop_event.wait()

    # injected by app via attribute assignment in tests:
    stop_event: asyncio.Event = None  # type: ignore[assignment]

    async def close(self) -> None:
        self.close_calls += 1
        if self.close_stall_event is not None:
            await self.close_stall_event.wait()


def _settings(tmp_path, *, discord_enabled: bool = False) -> Settings:
    return Settings.model_validate(
        {
            "binance": {"markets": ["spot", "futures"]},
            "runtime": {
                "record_raw_events": True,
                "raw_event_directory": str(tmp_path / "tape"),
                "raw_event_max_bytes": 1_048_576,
            },
            "alerts": {
                "discord_enabled": discord_enabled,
                "discord_webhook_url": None,
            },
            "storage": {"url": f"sqlite:///{tmp_path / 'app.db'}"},
        }
    )


def make_app_factory(tmp_path):
    """Build a real SignalApplication whose scanners are fakes."""

    import signalbot.app as app_module
    from signalbot.app import SignalApplication

    created: list[FakeMarketScanner] = []

    def _factory(**scanner_kwargs):
        pending_scanner_kwargs = dict(scanner_kwargs)

        settings = _settings(tmp_path)
        app = SignalApplication.__new__(SignalApplication)
        # bypass heavy __init__; wire manually like __init__ would:
        from signalbot.clock import SystemClock
        from signalbot.data.raw_events import RawEventRecorder
        from signalbot.persistence.repository import SqlRepository

        app.settings = settings
        app.clock = SystemClock()
        app.repository = SqlRepository(settings.storage.url, False)
        app.stop_event = asyncio.Event()
        app.stop_after_minutes = None
        app.notifier = None
        app.scanners = []
        app.raw_recorder = RawEventRecorder(tmp_path / "tape")

        # Patch the app module's MarketScanner symbol so run() instantiates
        # our fakes instead of real network scanners:
        real_market_scanner = app_module.MarketScanner

        def fake_market_scanner_ctor(market, settings, clock, runtime, stop_event, *args, **kwargs):
            kwargs.pop("raw_recorder", None)
            merged = {**kwargs, **pending_scanner_kwargs}
            merged.pop("raw_recorder", None)
            scanner = FakeMarketScanner(market.value, **merged)
            scanner.stop_event = stop_event
            created.append(scanner)
            return scanner

        app_module.MarketScanner = fake_market_scanner_ctor
        app._restore_market_scanner = lambda: setattr(  # type: ignore[attr-defined]
            app_module, "MarketScanner", real_market_scanner
        )
        return app

    return _factory, created


@pytest.mark.asyncio
async def test_a_healthy_bounded_stop_exits_within_deadline(tmp_path):
    """RED on pre-fix code: monitor never resolves; run() must now exit."""

    factory, _scanners = make_app_factory(tmp_path)
    app = factory()
    app.stop_after_minutes = 0  # immediate bounded stop
    # stop_after_minutes=0 sleeps 0s then sets stop -> immediate normal stop.

    started = asyncio.Event()

    async def _run():
        started.set()
        await app.run()

    task = asyncio.create_task(_run())
    try:
        await asyncio.wait_for(started.wait(), timeout=5)
        # Pre-fix implementation hangs here forever (gather never returns).
        await asyncio.wait_for(task, timeout=15)
    finally:
        if hasattr(app, "_restore_market_scanner"):
            getattr(app, "_restore_market_scanner", lambda: None)()


@pytest.mark.asyncio
async def test_f_monitor_not_pending_after_normal_stop(tmp_path):
    factory, _scanners = make_app_factory(tmp_path)

    app = factory()
    app.stop_after_minutes = 0

    async def _run():
        await app.run()

    task = asyncio.create_task(_run())
    try:
        await asyncio.wait_for(task, timeout=15)
    finally:
        if hasattr(app, "_restore_market_scanner"):
            getattr(app, "_restore_market_scanner", lambda: None)()
    # After run() returns, no app-created tasks may remain pending.
    remaining = [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]
    assert not [t for t in remaining if not t.done()], (
        f"leaked tasks: {[t.get_name() for t in remaining if not t.done()]}"
    )



# ---------------------------------------------------------------------------
# C. Recorder fatal -> monitor wins -> clean exit
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_c_recorder_fatal_stops_application(tmp_path):
    factory, _scanners = make_app_factory(tmp_path)
    app = factory()
    app.stop_after_minutes = None
    # Inject fatal before start:
    from signalbot.data.raw_events import RawEventWriterError

    assert app.raw_recorder is not None
    app.raw_recorder._signal_fatal(RawEventWriterError("injected fatal"))

    async def _run():
        await app.run()

    task = asyncio.create_task(_run())
    try:
        await asyncio.wait_for(task, timeout=15)
    finally:
        if hasattr(app, "_restore_market_scanner"):
            getattr(app, "_restore_market_scanner", lambda: None)()
    assert app.stop_event.is_set()


# ---------------------------------------------------------------------------
# D. Scanner crash -> sibling torn down + resources closed
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_d_scanner_crash_fails_closed(tmp_path):
    factory, _scanners = make_app_factory(tmp_path)
    app = factory(crash_on_start=True)

    close_order: list[str] = []
    original_close = FakeMarketScanner.close

    async def tracking_close(self):
        close_order.append(self.market.value)
        await original_close(self)

    FakeMarketScanner.close = tracking_close
    try:
        with pytest.raises(RuntimeError, match="crashed"):
            await asyncio.wait_for(app.run(), timeout=15)
    except BaseException:
        raise
    finally:
        FakeMarketScanner.close = original_close
        if hasattr(app, "_restore_market_scanner"):
            getattr(app, "_restore_market_scanner", lambda: None)()
    # Every created scanner (both crashed here) must have been closed during
    # fail-closed teardown - no scanner resource is skipped.
    assert len(close_order) == 2


# ---------------------------------------------------------------------------
# E. Shutdown ordering: scanner close < recorder close < repo close
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_e_shutdown_ordering(tmp_path):
    factory, _scanners = make_app_factory(tmp_path)
    app = factory()
    app.stop_after_minutes = 0

    calls: list[str] = []
    original_class_close = FakeMarketScanner.close

    def class_tracking_close(self):
        calls.append(f"scanner-close-{self.market.value}")
        return original_class_close(self)

    FakeMarketScanner.close = class_tracking_close

    recorder = app.raw_recorder
    assert recorder is not None
    original_recorder_close = recorder.close

    async def tracked_recorder_close():
        calls.append("recorder-close")
        await original_recorder_close()

    recorder.close = tracked_recorder_close  # type: ignore[method-assign]

    original_repo_close = type(app.repository).close

    def tracked_repo_close(self):
        calls.append("repo-close")
        return original_repo_close(self)

    type(app.repository).close = tracked_repo_close
    try:
        await asyncio.wait_for(app.run(), timeout=15)
    finally:
        type(app.repository).close = original_repo_close
        if hasattr(app, "_restore_market_scanner"):
            getattr(app, "_restore_market_scanner", lambda: None)()
        FakeMarketScanner.close = original_class_close

    assert "recorder-close" in calls
    assert "repo-close" in calls
    assert calls.index("recorder-close") < calls.index("repo-close")
    for name in ("scanner-close-spot", "scanner-close-futures"):
        assert name in calls
        assert calls.index(name) < calls.index("recorder-close")


# ---------------------------------------------------------------------------
# G. No leaked tasks after run() returns (extended assertions live in F)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_g_no_task_leak_after_run(tmp_path):
    factory, _scanners = make_app_factory(tmp_path)
    app = factory()
    app.stop_after_minutes = 0

    async def _run():
        await app.run()

    task = asyncio.create_task(_run())
    try:
        await asyncio.wait_for(task, timeout=15)
    finally:
        if hasattr(app, "_restore_market_scanner"):
            getattr(app, "_restore_market_scanner", lambda: None)()
    await asyncio.sleep(0.05)  # one scheduling tick for done-callbacks
    remaining = [
        t
        for t in asyncio.all_tasks()
        if t is not asyncio.current_task() and not t.done()
    ]
    assert not remaining, f"leaked: {[t.get_name() for t in remaining]}"



# ---------------------------------------------------------------------------
# WS transport cancellation matrix (charter s5 / s12 cases 3-6, 9)
# ---------------------------------------------------------------------------


class HangingFakeScanner:
    """Scanner fake whose websocket task hangs in a selectable phase."""

    def __init__(self, market_name, mode, release: asyncio.Event):
        self.market = MagicMock()
        self.market.value = market_name
        self.mode = mode
        self.release = release
        self.stop_event = asyncio.Event()
        self.close_calls = 0

    async def run(self):
        if self.mode == "CONNECT_HANG":
            await self.release.wait()
        elif self.mode == "RECEIVE_HANG":
            await self.release.wait()
        else:
            await self.stop_event.wait()

    async def close(self):
        self.close_calls += 1


def _app_with_hanging_scanners(tmp_path, mode):
    import signalbot.app as app_module
    from signalbot.app import SignalApplication
    from signalbot.clock import SystemClock
    from signalbot.data.raw_events import RawEventRecorder
    from signalbot.persistence.repository import SqlRepository

    settings = _settings(tmp_path)
    app = SignalApplication.__new__(SignalApplication)
    app.settings = settings
    app.clock = SystemClock()
    app.repository = SqlRepository(settings.storage.url, False)
    app.stop_event = asyncio.Event()
    app.stop_after_minutes = None
    app.notifier = None
    app.scanners = []
    app.raw_recorder = RawEventRecorder(tmp_path / "tape")

    release = asyncio.Event()
    real_market_scanner = app_module.MarketScanner

    def hanging_ctor(market, *args, **kwargs):
        scanner = MagicMock()
        scanner.market.value = market.value
        hang = HangingFakeScanner(market.value, mode, release)

        async def scanner_run():
            await hang.run()

        async def scanner_close():
            hang.close_calls += 1

        wrapper = MagicMock()
        wrapper.market = hang.market
        wrapper.run = scanner_run
        wrapper.close = scanner_close
        return wrapper

    app_module.MarketScanner = hanging_ctor

    def restore():
        app_module.MarketScanner = real_market_scanner

    return app, release, restore


@pytest.mark.asyncio
@pytest.mark.parametrize("mode", ["CONNECT_HANG", "RECEIVE_HANG"])
async def test_ws3_4_hang_cannot_block_shutdown_forever(tmp_path, mode):
    """Cases 3/4: connect/receive hangs must not stall app shutdown."""
    app, release, restore = _app_with_hanging_scanners(tmp_path, mode)
    # External stop shortly after start:
    async def stopper():
        await asyncio.sleep(0.05)
        app.stop_event.set()

    stopper_task = asyncio.create_task(stopper())
    try:
        # Graceful drain deadline + bounded cancel must keep this finite:
        # drain(30) is skipped because hanging tasks don't finish gracefully,
        # so expected path = drain-timeout -> cancel -> bounded join.
        await asyncio.wait_for(app.run(), timeout=60)
    finally:
        restore()
        stopper_task.cancel()
        release.set()


@pytest.mark.asyncio
async def test_ws9_bounded_stop_during_connect_attempt(tmp_path):
    """Case 9: bounded stop fires while scanners are mid-connect-hang."""
    app, release, restore = _app_with_hanging_scanners(tmp_path, "CONNECT_HANG")
    app.stop_after_minutes = 0  # immediate bounded stop

    try:
        await asyncio.wait_for(app.run(), timeout=60)
    finally:
        restore()
        release.set()


# ---------------------------------------------------------------------------
# Case 14: recorder fatal preserved after unhealthy stop
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_recorder_fatal_state_preserved_after_stop(tmp_path):
    from signalbot.data.raw_events import RawEventWriterError

    factory, _scanners = make_app_factory(tmp_path)
    app = factory()
    assert app.raw_recorder is not None
    app.raw_recorder._signal_fatal(RawEventWriterError("fatal before start"))

    async def _run():
        await app.run()

    task = asyncio.create_task(_run())
    try:
        await asyncio.wait_for(task, timeout=15)
    finally:
        if hasattr(app, "_restore_market_scanner"):
            getattr(app, "_restore_market_scanner", lambda: None)()
    assert app.raw_recorder.is_failed()
    assert "fatal before start" in str(app.raw_recorder.fatal_error)


# ---------------------------------------------------------------------------
# Case 13: accepted == durable after healthy stop
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_recorder_accepted_equals_durable_after_healthy_stop(tmp_path):
    factory, _scanners = make_app_factory(tmp_path)
    app = factory()
    app.stop_after_minutes = 0

    assert app.raw_recorder is not None
    for i in range(10):
        await app.raw_recorder.append(Market.SPOT, {"i": i}, 1_710_000_000_000 + i)

    async def _run():
        await app.run()

    task = asyncio.create_task(_run())
    try:
        await asyncio.wait_for(task, timeout=15)
    finally:
        if hasattr(app, "_restore_market_scanner"):
            getattr(app, "_restore_market_scanner", lambda: None)()
    status = app.raw_recorder.status()
    assert status["accepted_records"] == status["durable_records"] == 10
