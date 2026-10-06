from __future__ import annotations

import asyncio
import logging
import math
from collections.abc import Iterator
from typing import Any

import pytest

from conftest import make_candle
from signalbot.clock import ReplayClock
from signalbot.config import RuntimeSettings, Settings
from signalbot.domain.enums import Market
from signalbot.domain.models import Candle
from signalbot.exchange.binance.endpoints import WebSocketPlan
from signalbot.exchange.binance.websocket import WebSocketConsumer
from signalbot.heartbeat import HeartbeatRecorder
from signalbot.observability.handler_timing import MAX_TRACKED_STREAMS, HandlerDiagnostics
from signalbot.observability.loop_lag import LoopLagMonitor
from signalbot.persistence.repository import SqlRepository
from signalbot.runtime import MarketRuntime
from signalbot.signals.protection_context import ProtectionContext

# --------------------------------------------------------------------------
# LoopLagMonitor
# --------------------------------------------------------------------------


def _monitor(**kwargs: Any) -> LoopLagMonitor:
    warning_ms = kwargs.pop("warning_ms", 500)
    return LoopLagMonitor(warning_ms=warning_ms, stop_event=asyncio.Event(), **kwargs)


def _warnings(caplog: pytest.LogCaptureFixture) -> list[logging.LogRecord]:
    return [r for r in caplog.records if r.levelno == logging.WARNING]


@pytest.mark.parametrize(("lag_ms", "warns"), [(499, False), (500, False), (501, True)])
def test_warning_threshold_boundary(
    lag_ms: int, warns: bool, caplog: pytest.LogCaptureFixture
) -> None:
    monitor = _monitor()
    with caplog.at_level(logging.WARNING):
        monitor.observe(lag_ms, now_s=0.0)
    assert bool(_warnings(caplog)) is warns
    assert monitor.warning_count == int(warns)


def test_warnings_are_rate_limited_and_report_the_window_maximum(
    caplog: pytest.LogCaptureFixture,
) -> None:
    monitor = _monitor()
    with caplog.at_level(logging.WARNING):
        monitor.observe(600, now_s=0.0)
        monitor.observe(700, now_s=10.0)  # suppressed
        monitor.observe(900, now_s=20.0)  # suppressed, but remembered
        monitor.observe(800, now_s=59.999)  # still inside the 60 s limit
        assert monitor.warning_count == 1
        monitor.observe(550, now_s=60.0)  # exactly one interval later
    assert monitor.warning_count == 2
    second = _warnings(caplog)[1].getMessage()
    assert "scheduling lag 550 ms" in second
    assert "max since last warning 900 ms" in second


def test_lag_samples_below_threshold_never_warn_but_feed_the_heartbeat() -> None:
    repo = SqlRepository("sqlite:///:memory:")
    repo.initialize()
    recorder = HeartbeatRecorder(repo, "spot", ReplayClock(1_000))
    monitor = _monitor(heartbeats=[recorder])
    monitor.observe(120, now_s=0.0)
    assert monitor.warning_count == 0
    assert repo.get_heartbeats()["spot"].max_loop_lag_ms == 120
    repo.close()


def test_heartbeat_write_uses_the_existing_throttle() -> None:
    repo = SqlRepository("sqlite:///:memory:")
    repo.initialize()
    clock = ReplayClock(1_000)
    recorder = HeartbeatRecorder(repo, "spot", clock)
    monitor = _monitor(heartbeats=[recorder])
    for step in range(100):
        monitor.observe(10 + step, now_s=float(step))
    assert recorder.write_count == 1  # clock never advanced past the 15 s interval
    clock.current_ms += 15_000
    monitor.observe(5, now_s=101.0)
    assert recorder.write_count == 2
    repo.close()


def test_rolling_maximum_covers_two_windows_then_forgets() -> None:
    monitor = _monitor(window_seconds=60.0)
    monitor.observe(300, now_s=0.0)
    assert monitor.rolling_max_ms == 300
    monitor.observe(100, now_s=60.0)  # window rolls; previous window still counts
    assert monitor.rolling_max_ms == 300
    monitor.observe(100, now_s=120.0)  # previous window (max 100) replaces it
    assert monitor.rolling_max_ms == 100


def test_negative_lag_is_clamped_and_invalid_limits_rejected() -> None:
    monitor = _monitor()
    monitor.observe(-40, now_s=0.0)
    assert monitor.rolling_max_ms == 0
    for bad in ({"warning_ms": 0}, {"interval_seconds": 0}, {"window_seconds": 0}):
        with pytest.raises(ValueError):
            _monitor(**bad)


@pytest.mark.asyncio
async def test_run_measures_lag_with_a_fake_clock(caplog: pytest.LogCaptureFixture) -> None:
    interval = 0.01
    ticks: Iterator[float] = iter(())

    def script() -> Iterator[float]:
        now = 0.0
        while True:
            yield now  # loop start
            now += interval + 0.600  # woke 600 ms late
            yield now

    ticks = script()
    stop = asyncio.Event()
    monitor = LoopLagMonitor(
        warning_ms=500,
        stop_event=stop,
        interval_seconds=interval,
        monotonic=lambda: next(ticks),
    )
    with caplog.at_level(logging.WARNING):
        task = asyncio.create_task(monitor.run())
        await asyncio.sleep(0.15)
        stop.set()
        await asyncio.wait_for(task, timeout=2)
    assert monitor.warning_count == 1  # rate limited although every sample was late
    assert monitor.rolling_max_ms == 600


@pytest.mark.asyncio
async def test_run_stops_promptly_on_stop_event_and_propagates_cancellation() -> None:
    stop = asyncio.Event()
    monitor = LoopLagMonitor(warning_ms=500, stop_event=stop, interval_seconds=30.0)
    task = asyncio.create_task(monitor.run())
    await asyncio.sleep(0.05)
    stop.set()
    await asyncio.wait_for(task, timeout=1)  # not 30 s

    other = LoopLagMonitor(warning_ms=500, stop_event=asyncio.Event(), interval_seconds=30.0)
    running = asyncio.create_task(other.run())
    await asyncio.sleep(0.05)
    running.cancel()
    with pytest.raises(asyncio.CancelledError):
        await running


# --------------------------------------------------------------------------
# HandlerDiagnostics
# --------------------------------------------------------------------------


class _Clock:
    def __init__(self) -> None:
        self.now = 0.0

    def __call__(self) -> float:
        return self.now


def _diag(clock: _Clock, **kwargs: Any) -> HandlerDiagnostics:
    return HandlerDiagnostics(slow_warning_ms=1_000, market="spot", monotonic=clock, **kwargs)


@pytest.mark.parametrize(("handler_ms", "warns"), [(999, False), (1_000, False), (1_001, True)])
def test_slow_handler_boundary(
    handler_ms: float, warns: bool, caplog: pytest.LogCaptureFixture
) -> None:
    diag = _diag(_Clock())
    with caplog.at_level(logging.WARNING):
        diag.record("btcusdt@kline_5m", receive_to_handler_ms=0, handler_ms=handler_ms)
    assert bool(_warnings(caplog)) is warns
    if warns:
        assert "btcusdt@kline_5m" in _warnings(caplog)[0].getMessage()


def test_slow_handler_warning_is_rate_limited_per_stream(
    caplog: pytest.LogCaptureFixture,
) -> None:
    clock = _Clock()
    diag = _diag(clock)
    with caplog.at_level(logging.WARNING):
        diag.record("a", receive_to_handler_ms=0, handler_ms=2_000)
        diag.record("b", receive_to_handler_ms=0, handler_ms=2_000)  # other stream: own limit
        clock.now = 59.999
        diag.record("a", receive_to_handler_ms=0, handler_ms=2_000)  # suppressed
        assert diag.slow_warning_count == 2
        clock.now = 60.0
        diag.record("a", receive_to_handler_ms=0, handler_ms=2_000)
    assert diag.slow_warning_count == 3


def test_receive_to_handler_delay_is_tracked_and_warned_separately(
    caplog: pytest.LogCaptureFixture,
) -> None:
    diag = _diag(_Clock())
    with caplog.at_level(logging.WARNING):
        diag.record("a", receive_to_handler_ms=1_001, handler_ms=1)
        diag.record("a", receive_to_handler_ms=400, handler_ms=1)
    assert diag.max_receive_to_handler_ms == 1_001
    assert diag.delay_warning_count == 1 and diag.slow_warning_count == 0


def test_tracked_streams_are_bounded() -> None:
    clock = _Clock()
    diag = _diag(clock)
    for index in range(MAX_TRACKED_STREAMS + 50):
        diag.record(f"s{index}", receive_to_handler_ms=0, handler_ms=5_000)
    assert len(diag._last_slow_warning) <= MAX_TRACKED_STREAMS + 1


def test_invalid_diagnostics_limits_rejected() -> None:
    with pytest.raises(ValueError):
        HandlerDiagnostics(slow_warning_ms=0, market="spot")
    with pytest.raises(ValueError):
        HandlerDiagnostics(slow_warning_ms=1, market="spot", warning_interval_seconds=0)


class _Connection:
    def __init__(self, messages: list[str]) -> None:
        self._messages = iter(messages)

    async def __aenter__(self) -> _Connection:
        return self

    async def __aexit__(self, *_a: object) -> None:
        return None

    def __aiter__(self) -> _Connection:
        return self

    async def __anext__(self) -> str:
        try:
            return next(self._messages)
        except StopIteration as exc:
            raise StopAsyncIteration from exc


PLAN = WebSocketPlan("btcusdt@kline_5m", Market.SPOT, "spot", ("x",), "wss://example.test")


@pytest.mark.asyncio
async def test_consumer_records_handler_timing_and_receive_delay(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    script = iter([0.0, 0.2, 1.5])  # received, handler start, handler end
    last = [1.5]

    def fake_monotonic() -> float:
        value = next(script, None)
        if value is None:
            return last[0]
        last[0] = value
        return value

    diag = HandlerDiagnostics(slow_warning_ms=1_000, market="spot", monotonic=fake_monotonic)
    stop = asyncio.Event()
    monkeypatch.setattr(
        "signalbot.exchange.binance.websocket.connect",
        lambda *_a, **_k: _Connection(['{"ok": true}']),
    )
    seen: list[Any] = []

    async def handler(payload: Any) -> None:
        seen.append(payload)
        stop.set()

    consumer = WebSocketConsumer(60, initial_backoff_seconds=0.001, diagnostics=diag)
    with caplog.at_level(logging.WARNING):
        await asyncio.wait_for(consumer.consume_forever(PLAN, handler, stop), timeout=2)
    assert seen == [{"ok": True}]  # payload reaches the handler untouched
    assert diag.max_receive_to_handler_ms == pytest.approx(200)
    assert diag.max_handler_ms == pytest.approx(1_300)
    assert diag.slow_warning_count == 1


@pytest.mark.asyncio
async def test_timing_is_recorded_even_when_the_handler_raises(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    diag = HandlerDiagnostics(slow_warning_ms=1_000, market="spot")
    monkeypatch.setattr(
        "signalbot.exchange.binance.websocket.connect",
        lambda *_a, **_k: _Connection(['{"ok": true}']),
    )

    async def handler(_payload: Any) -> None:
        raise ValueError("pipeline failure")

    consumer = WebSocketConsumer(60, diagnostics=diag)
    with pytest.raises(ValueError, match="pipeline failure"):
        await asyncio.wait_for(consumer.consume_forever(PLAN, handler, asyncio.Event()), timeout=2)
    assert diag.max_handler_ms >= 0.0 and diag.max_receive_to_handler_ms >= 0.0


def test_runtime_settings_thresholds_defaults_bounds_and_dump_exclusion() -> None:
    settings = RuntimeSettings()
    assert (settings.loop_lag_warning_ms, settings.handler_slow_warning_ms) == (500, 1_000)
    dumped = settings.model_dump()
    assert "loop_lag_warning_ms" not in dumped and "handler_slow_warning_ms" not in dumped
    assert RuntimeSettings(loop_lag_warning_ms=50).loop_lag_warning_ms == 50
    assert RuntimeSettings(handler_slow_warning_ms=100).handler_slow_warning_ms == 100
    with pytest.raises(ValueError):
        RuntimeSettings(loop_lag_warning_ms=49)
    with pytest.raises(ValueError):
        RuntimeSettings(handler_slow_warning_ms=99)


# --------------------------------------------------------------------------
# App supervision of the monitor
# --------------------------------------------------------------------------


@pytest.mark.asyncio
@pytest.mark.parametrize("mode", ["raise", "return"])
async def test_monitor_failure_fails_closed(
    mode: str,
    tmp_path: Any,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    from test_discord_dispatch_supervision import _make_app

    import signalbot.app as app_module

    class BrokenMonitor:
        def __init__(self, **_kwargs: Any) -> None:
            pass

        async def run(self) -> None:
            if mode == "raise":
                raise RuntimeError("monitor exploded")

    app = _make_app(tmp_path, monkeypatch, "wait")
    monkeypatch.setattr(app_module, "LoopLagMonitor", BrokenMonitor)
    expected = "monitor exploded" if mode == "raise" else "without a stop request"
    with caplog.at_level(logging.INFO):
        with pytest.raises(RuntimeError, match=expected):
            await asyncio.wait_for(app.run(), timeout=15)
    assert app.stop_event.is_set()
    assert any(
        r.levelno == logging.CRITICAL and "Event-loop lag monitor" in r.getMessage()
        for r in caplog.records
    )


@pytest.mark.asyncio
async def test_normal_stop_logs_thresholds_once_and_no_errors(
    tmp_path: Any, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    from test_discord_dispatch_supervision import _make_app

    app = _make_app(tmp_path, monkeypatch, "wait")
    app.stop_after_minutes = 0
    with caplog.at_level(logging.INFO):
        await asyncio.wait_for(app.run(), timeout=15)
    assert not [r for r in caplog.records if r.levelno >= logging.ERROR]
    threshold_logs = [r for r in caplog.records if "diagnostic thresholds" in r.getMessage()]
    assert len(threshold_logs) == 1
    assert "500 ms" in threshold_logs[0].getMessage()
    assert "1000 ms" in threshold_logs[0].getMessage()


# --------------------------------------------------------------------------
# Sub-primary feature skipping: no consumer, byte-identical outputs
# --------------------------------------------------------------------------

HOUR_MS = 3_600_000
T0_MS = 400 * HOUR_MS
STEPS = {"1m": 60_000, "5m": 300_000, "15m": 900_000, "1h": HOUR_MS}
BOOTSTRAP_BARS = 230
LIVE_HORIZON_MS = 2 * HOUR_MS


def _price(close_time_ms: int) -> float:
    slow = 3 * math.sin(close_time_ms / (6 * HOUR_MS))
    fast = 0.5 * math.sin(close_time_ms / 1_000_000)
    return 100 + slow + fast


def _series(interval: str, first_index: int, count: int) -> list[Candle]:
    step = STEPS[interval]
    candles = []
    for index in range(first_index, first_index + count):
        close = _price((index + 1) * step - 1)
        candles.append(make_candle(index, interval=interval, step_ms=step, close=close))
    return candles


def _settings() -> Settings:
    return Settings.model_validate(
        {
            "binance": {
                "markets": ["spot"],
                "intervals": ["1m", "5m", "15m", "1h"],
                "primary_interval": "5m",
            },
            "storage": {"url": "sqlite:///:memory:"},
        }
    )


async def _replay_sequence(
    skip_sub_primary: bool, monkeypatch: pytest.MonkeyPatch
) -> dict[str, Any]:
    settings = _settings()
    repo = SqlRepository("sqlite:///:memory:")
    repo.initialize()
    clock = ReplayClock(T0_MS)
    decisions: list[str] = []
    contexts: list[str] = []
    evaluations: list[str] = []

    async def on_decision(decision: Any) -> None:
        decisions.append(decision.model_dump_json())

    async def on_context(context: ProtectionContext) -> None:
        contexts.append(context.model_dump_json())

    runtime = MarketRuntime(
        Market.SPOT, settings, repo, clock, on_decision, protection_context_handler=on_context
    )
    if not skip_sub_primary:
        monkeypatch.setattr(runtime, "_feature_has_no_consumer", lambda _candle: False)
    original_evaluate = runtime.rule_engine.evaluate

    def recording_evaluate(feature: Any, ctx: Any) -> Any:
        result = original_evaluate(feature, ctx)
        evaluations.append(
            repr((feature.event_time_ms, sorted(ctx), [e.model_dump_json() for e in result]))
        )
        return result

    monkeypatch.setattr(runtime.rule_engine, "evaluate", recording_evaluate)
    runtime.set_active_symbols(frozenset({"BTCUSDT"}), frozenset({"BTCUSDT"}))
    for interval, step in STEPS.items():
        end_index = T0_MS // step
        bootstrap_series = _series(interval, end_index - BOOTSTRAP_BARS, BOOTSTRAP_BARS)
        runtime.bootstrap(bootstrap_series, rebuild=False)
    runtime.rebuild_derived_state()
    live: list[Candle] = []
    for interval, step in STEPS.items():
        live.extend(_series(interval, T0_MS // step, LIVE_HORIZON_MS // step))
    live.sort(key=lambda c: (c.close_time_ms, STEPS[c.interval]))
    for candle in live:
        clock.current_ms = max(clock.current_ms, candle.close_time_ms)
        await runtime.handle_event(candle)
    one_minute = runtime.candles.get(Market.SPOT, "BTCUSDT", "1m")
    history_1m = runtime._feature_history.get(("BTCUSDT", "1m"))
    snapshot = {
        "decisions": decisions,
        "contexts": contexts,
        "evaluations": evaluations,
        "history": {
            interval: [f.model_dump_json() for f in runtime._feature_history[("BTCUSDT", interval)]]
            for interval in ("5m", "15m", "1h")
        },
        "stored_1m_candles": len(one_minute),
        "last_1m_close": one_minute[-1].close_time_ms,
        "last_1m_feature_time": None if not history_1m else history_1m[-1].event_time_ms,
        "regime": runtime.regime.snapshot(Market.SPOT, clock.current_ms).model_dump_json(),
    }
    repo.close()
    return snapshot


@pytest.mark.asyncio
async def test_skipping_sub_primary_features_is_byte_identical_for_all_consumers(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    baseline = await _replay_sequence(skip_sub_primary=False, monkeypatch=monkeypatch)
    monkeypatch.undo()
    optimized = await _replay_sequence(skip_sub_primary=True, monkeypatch=monkeypatch)

    assert len(optimized["contexts"]) == LIVE_HORIZON_MS // STEPS["5m"]  # primary path ran
    assert len(optimized["evaluations"]) == LIVE_HORIZON_MS // STEPS["5m"]
    for key in ("decisions", "contexts", "evaluations", "history", "regime"):
        assert optimized[key] == baseline[key], key
    # the 1m candles are still stored, but their feature is no longer recomputed
    assert optimized["stored_1m_candles"] == baseline["stored_1m_candles"]
    assert baseline["last_1m_feature_time"] == baseline["last_1m_close"]
    assert optimized["last_1m_feature_time"] is not None
    assert optimized["last_1m_feature_time"] < optimized["last_1m_close"]


def test_feature_consumer_rule_boundaries() -> None:
    repo = SqlRepository("sqlite:///:memory:")
    repo.initialize()
    runtime = MarketRuntime(
        Market.SPOT, _settings(), repo, ReplayClock(0), lambda decision: asyncio.sleep(0)
    )
    one_minute = make_candle(1, interval="1m", step_ms=60_000)
    primary = make_candle(1, interval="5m", step_ms=300_000)
    higher = make_candle(1, interval="15m", step_ms=900_000)
    btc_hour = make_candle(1, interval="1h", step_ms=HOUR_MS)
    assert runtime._feature_has_no_consumer(one_minute) is True
    assert runtime._feature_has_no_consumer(primary) is False  # exactly the primary
    assert runtime._feature_has_no_consumer(higher) is False
    assert runtime._feature_has_no_consumer(btc_hour) is False

    hourly_primary = Settings.model_validate(
        {
            "binance": {
                "markets": ["spot"],
                "intervals": ["5m", "1h", "4h"],
                "primary_interval": "4h",
            },
            "storage": {"url": "sqlite:///:memory:"},
        }
    )
    other = MarketRuntime(
        Market.SPOT, hourly_primary, repo, ReplayClock(0), lambda decision: asyncio.sleep(0)
    )
    # BTCUSDT 1h feeds the regime engine even when 1h is shorter than the primary
    assert other._feature_has_no_consumer(btc_hour) is False
    eth_hour = make_candle(1, symbol="ETHUSDT", interval="1h", step_ms=HOUR_MS)
    assert other._feature_has_no_consumer(eth_hour) is True
    repo.close()
