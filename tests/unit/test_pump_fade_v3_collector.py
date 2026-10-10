from __future__ import annotations

import asyncio
import json
import shutil
from contextlib import asynccontextmanager
from decimal import Decimal
from pathlib import Path

import pytest

from signalbot.capture.errors import CaptureStorageCapacityError
from signalbot.capture.websocket import validate_public_websocket_plan
from signalbot.pump_fade_v3.collector import (
    EventSymbolSet,
    _CaptureThenInspect,
    _MinuteGlobalCapture,
    build_websocket_plans,
    main,
    recover_previous_generations,
    run_standalone,
)
from signalbot.pump_fade_v3.config import (
    CollectorConfig,
    load_collector_config,
    parse_ticker_event,
)
from signalbot.pump_fade_v3.disk_budget import DiskBudget


def _config(**overrides: object) -> CollectorConfig:
    defaults = {
        "enabled": False,
        "data_directory": Path("/data/pump-fade-v3"),
        "maximum_total_bytes": 16 * 1024**3,
        "minimum_free_bytes": 12 * 1024**3,
        "maximum_event_symbols": 25,
        "event_lifetime_ms": 26 * 60 * 60 * 1000,
        "event_return_percent": Decimal("20.0"),
        "event_minimum_quote_volume_usdt": Decimal("2000000.0"),
        "rest_weight_budget_per_minute": 600,
        "event_depth_enabled": True,
    }
    defaults.update(overrides)
    return CollectorConfig(
        enabled=bool(defaults["enabled"]),
        data_directory=Path(defaults["data_directory"]),
        maximum_total_bytes=int(defaults["maximum_total_bytes"]),
        minimum_free_bytes=int(defaults["minimum_free_bytes"]),
        maximum_event_symbols=int(defaults["maximum_event_symbols"]),
        event_lifetime_ms=int(defaults["event_lifetime_ms"]),
        event_return_percent=Decimal(str(defaults["event_return_percent"])),
        event_minimum_quote_volume_usdt=Decimal(
            str(defaults["event_minimum_quote_volume_usdt"])
        ),
        rest_weight_budget_per_minute=int(defaults["rest_weight_budget_per_minute"]),
        event_depth_enabled=bool(defaults["event_depth_enabled"]),
    )


def _ticker(
    symbol: str, *, change: float = 20.0, volume: float = 2_000_000.0,
    event_ms: int = 1_000,
) -> dict[str, object]:
    return {"s": symbol, "P": str(change), "q": str(volume), "E": event_ms, "st": 1}


def test_event_admission_includes_exact_threshold_boundary() -> None:
    events = EventSymbolSet(_config())

    result = events.observe(_ticker("BTCUSDT"), now_ms=1_000)

    assert result.active_symbols == ("BTCUSDT",)
    assert result.evicted_symbol is None


def test_event_admission_parses_mini_ticker_and_ignores_coin_m_rows() -> None:
    events = EventSymbolSet(_config())
    mini_ticker = {
        "s": "BTCUSDT",
        "o": "100",
        "c": "120",
        "q": "2000000",
        "E": 1_000,
        "st": 1,
    }
    coin_m_row = {**mini_ticker, "s": "BTCUSD_PERP", "st": 2}

    assert events.observe(mini_ticker, now_ms=1_000).active_symbols == ("BTCUSDT",)
    assert events.observe(coin_m_row, now_ms=1_000).active_symbols == ("BTCUSDT",)


@pytest.mark.parametrize(
    "ticker",
    (
        _ticker("BTCUSDT", change=19.99),
        _ticker("BTCUSDT", volume=1_999_999.99),
        _ticker("BTCUSDT", change=-30.0),
    ),
)
def test_event_admission_rejects_below_thresholds(ticker: dict[str, object]) -> None:
    events = EventSymbolSet(_config())

    assert events.observe(ticker, now_ms=1_000).active_symbols == ()


def test_event_symbols_are_lru_bounded_at_25() -> None:
    events = EventSymbolSet(_config())
    for index in range(25):
        events.observe(_ticker(f"T{index}USDT", event_ms=1_000 + index), now_ms=2_000)
    events.observe(_ticker("NEWUSDT", event_ms=2_000), now_ms=2_000)

    assert events.active_symbols(now_ms=2_000) == tuple(
        [f"T{index}USDT" for index in range(1, 25)] + ["NEWUSDT"]
    )


def test_event_symbol_expires_at_26_hour_boundary() -> None:
    events = EventSymbolSet(_config())
    events.observe(_ticker("BTCUSDT"), now_ms=1_000)

    assert events.active_symbols(now_ms=1_000 + 26 * 60 * 60 * 1000 - 1) == (
        "BTCUSDT",
    )
    assert events.active_symbols(now_ms=1_000 + 26 * 60 * 60 * 1000) == ()


def test_continuous_qualifying_ticker_cannot_extend_lifetime() -> None:
    events = EventSymbolSet(_config())
    events.observe(_ticker("BTCUSDT"), now_ms=1_000)
    lifetime = 26 * 60 * 60 * 1000
    events.observe(_ticker("BTCUSDT", event_ms=lifetime), now_ms=lifetime)
    assert events.observe(
        _ticker("BTCUSDT", event_ms=lifetime + 1_000), now_ms=lifetime + 1_000
    ).active_symbols == ()
    assert events.observe(
        _ticker("BTCUSDT", change=19), now_ms=lifetime + 2_000
    ).active_symbols == ()
    assert events.observe(
        _ticker("BTCUSDT", event_ms=lifetime + 3_000), now_ms=lifetime + 3_000
    ).active_symbols == ("BTCUSDT",)


@pytest.mark.parametrize("st", [True, "1", 2])
def test_explicit_ambiguous_market_type_is_ignored(st: object) -> None:
    events = EventSymbolSet(_config())
    row = _ticker("BTCUSDT")
    row["st"] = st
    assert events.observe(row, now_ms=1_000).active_symbols == ()


def test_official_miniticker_without_st_admits_usdm_event() -> None:
    events = EventSymbolSet(_config())
    official_shape = {"s": "BTCUSDT", "o": "100", "c": "120", "q": "2000000", "E": 1000}
    assert events.observe(official_shape, now_ms=1000).active_symbols == ("BTCUSDT",)


def test_ticker_parser_rejects_non_usdt_and_nonfinite_values() -> None:
    with pytest.raises(ValueError, match="USDT"):
        parse_ticker_event(_ticker("BTCUSD"))
    with pytest.raises(ValueError, match="finite"):
        parse_ticker_event(_ticker("BTCUSDT", change=float("nan")))


def test_websocket_plans_are_routed_public_and_depth_is_optional() -> None:
    symbols = tuple(f"T{index}USDT" for index in range(25))
    plans = build_websocket_plans(symbols, include_event_depth=True)

    for plan in plans:
        validate_public_websocket_plan(plan)
        assert len(plan.streams) <= 24
    assert any("!forceOrder@arr" in plan.streams for plan in plans)
    assert any("!miniTicker@arr" in plan.streams for plan in plans)
    assert not any(stream.endswith("@bookTicker") for plan in plans for stream in plan.streams)
    assert sum(stream.endswith("@depth5@500ms") for plan in plans for stream in plan.streams) == 25

    no_depth = build_websocket_plans(symbols, include_event_depth=False)
    assert not any("@depth" in stream for plan in no_depth for stream in plan.streams)


def test_recent_restart_gap_count_uses_one_hour_window(tmp_path: Path) -> None:
    from signalbot.pump_fade_v3.collector import _recent_restart_gap_count

    capture_root = tmp_path / "capture"
    capture_root.mkdir()
    for index, terminal_at_ms in enumerate((10_000, 3_600_000, 3_660_001), start=1):
        generation = capture_root / f"capture-boot-g{index:04d}"
        generation.mkdir()
        (generation / "generation-terminal.json").write_text(json.dumps({
            "status": "RECOVERED_CRASH_INCOMPLETE",
            "terminal_at_ms": terminal_at_ms,
        }))

    assert _recent_restart_gap_count(capture_root, now_ms=3_660_000) == 1


def test_websocket_plan_rejects_more_than_25_symbols() -> None:
    with pytest.raises(ValueError, match="at most 25"):
        build_websocket_plans(
            tuple(f"T{index}USDT" for index in range(26)), include_event_depth=True
        )


def test_collector_config_rejects_unknown_fields(tmp_path: Path) -> None:
    source = Path("config/pump_fade_v3/collector.json").read_text(encoding="utf-8")
    payload = json.loads(source)
    payload["unexpected"] = True
    config_path = tmp_path / "collector.json"
    config_path.write_text(json.dumps(payload), encoding="utf-8")

    with pytest.raises(ValueError, match="exactly the declared fields"):
        load_collector_config(config_path)


def test_collector_config_is_disabled_and_uses_16gib_cap_12gib_floor() -> None:
    config = load_collector_config("config/pump_fade_v3/collector.json")

    assert config.enabled is False
    assert config.maximum_total_bytes == 16 * 1024**3
    assert config.minimum_free_bytes == 12 * 1024**3


def test_disk_budget_sheds_depth_then_stops_at_total_cap(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    data_directory = tmp_path / "data"
    data_directory.mkdir()
    shed_calls: list[str] = []
    monkeypatch.setattr("os.path.ismount", lambda path: Path(path) == tmp_path)
    budget = DiskBudget(
        data_directory,
        maximum_total_bytes=100,
        minimum_free_bytes=10,
        event_depth_shed=lambda: shed_calls.append("depth"),
        expected_mount=tmp_path,
    )

    with pytest.raises(CaptureStorageCapacityError, match="hard data cap"):
        budget.ensure_capacity(current_bytes=100, incoming_bytes=1)

    assert shed_calls == ["depth"]
    assert budget.tripped is True
    assert budget.tripped is True
    with pytest.raises(CaptureStorageCapacityError, match="already tripped"):
        budget.ensure_capacity(current_bytes=0, incoming_bytes=0)


def test_disk_budget_sheds_depth_then_stops_at_free_space_floor(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    data_directory = tmp_path / "data"
    data_directory.mkdir()
    shed_calls: list[str] = []
    monkeypatch.setattr("os.path.ismount", lambda path: Path(path) == tmp_path)
    budget = DiskBudget(
        data_directory,
        maximum_total_bytes=1_000,
        minimum_free_bytes=100,
        event_depth_shed=lambda: shed_calls.append("depth"),
        expected_mount=tmp_path,
    )
    usage_type = type(shutil.disk_usage(data_directory))
    monkeypatch.setattr(shutil, "disk_usage", lambda _path: usage_type(1_000, 901, 99))

    with pytest.raises(CaptureStorageCapacityError, match="free-space floor"):
        budget.ensure_capacity(current_bytes=0, incoming_bytes=1)

    assert shed_calls == ["depth"]
    assert budget.tripped is True


def test_disk_budget_rejects_nonmount_authority(tmp_path: Path) -> None:
    directory = tmp_path / "data"
    directory.mkdir()
    with pytest.raises(ValueError, match="mount point"):
        DiskBudget(
            directory, maximum_total_bytes=100, minimum_free_bytes=10,
            event_depth_shed=lambda: None, expected_mount=directory,
        )


def test_collector_total_cap_spans_generations_and_restart_seed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path / "data"
    previous = root / "generation-one"
    previous.mkdir(parents=True)
    (previous / "segment.zst").write_bytes(b"x" * 60)
    monkeypatch.setattr("os.path.ismount", lambda p: Path(p) == tmp_path)
    shed_calls: list[int] = []
    budget = DiskBudget(root, maximum_total_bytes=100, minimum_free_bytes=10,
                        expected_mount=tmp_path, event_depth_shed=lambda: shed_calls.append(1),
                        emergency_reserve_bytes=10)
    budget.ensure_capacity(current_bytes=0, incoming_bytes=20)
    budget.ensure_capacity(current_bytes=20, incoming_bytes=10)
    with pytest.raises(CaptureStorageCapacityError, match="hard data cap"):
        budget.ensure_capacity(current_bytes=30, incoming_bytes=1)
    assert budget.tripped and shed_calls == [1]

    # A fresh process accounts for all on-disk generations at startup.
    (root / "generation-two").mkdir()
    (root / "generation-two" / "segment.zst").write_bytes(b"y" * 31)
    restarted = DiskBudget(root, maximum_total_bytes=100, minimum_free_bytes=10,
                           expected_mount=tmp_path, event_depth_shed=lambda: None,
                           emergency_reserve_bytes=10)
    with pytest.raises(CaptureStorageCapacityError, match="hard data cap"):
        restarted.ensure_capacity(current_bytes=0, incoming_bytes=1)


def test_disk_budget_rejects_nested_symlinks_at_initialization_and_reconcile(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path / "data"
    nested = root / "nested"
    nested.mkdir(parents=True)
    outside = tmp_path / "outside"
    outside.write_bytes(b"private")
    monkeypatch.setattr("os.path.ismount", lambda p: Path(p) == tmp_path)
    link = nested / "escape"
    try:
        link.symlink_to(outside)
    except OSError:
        pytest.skip("host does not permit symlink fixture")

    def make_budget() -> DiskBudget:
        return DiskBudget(root, maximum_total_bytes=1000, minimum_free_bytes=10,
                          expected_mount=tmp_path, event_depth_shed=lambda: None)

    with pytest.raises(ValueError, match="unsafe link"):
        make_budget()
    link.unlink()
    budget = make_budget()
    link.symlink_to(outside)
    with pytest.raises(ValueError, match="unsafe link"):
        budget.reconcile()


def test_disk_budget_reconcile_releases_only_conservative_reservations(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path / "data"
    root.mkdir()
    (root / "retained.bin").write_bytes(b"x" * 60)
    monkeypatch.setattr("os.path.ismount", lambda p: Path(p) == tmp_path)
    budget = DiskBudget(
        root,
        maximum_total_bytes=100,
        minimum_free_bytes=10,
        expected_mount=tmp_path,
        event_depth_shed=lambda: None,
        emergency_reserve_bytes=10,
    )
    budget.ensure_capacity(current_bytes=0, incoming_bytes=20)
    assert budget.accounted_total_bytes == 80
    assert budget.reconcile() == 60
    assert budget.accounted_total_bytes == 60


def test_recovery_transient_write_trips_data_free_space_floor(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    from signalbot.capture.models import CaptureEnvelopeV1, record_to_json_line
    from signalbot.capture.storage import SegmentedCaptureWriter
    from signalbot.domain.enums import Market

    root = tmp_path / "data"
    generation = root / "capture" / "capture-boot-g0001"
    generation.mkdir(parents=True)
    plan_sha = "a" * 64
    frame = CaptureEnvelopeV1(
        received_at_ms=1,
        received_monotonic_ns=1,
        plan_sha256=plan_sha,
        process_boot_id="boot-g1",
        connection_id="c1",
        frame_seq=1,
        ingest_seq=1,
        market=Market.FUTURES,
        route="market",
        stream="!miniTicker@arr",
        subscription_streams=("!miniTicker@arr",),
        raw_payload='{"stream":"!miniTicker@arr","data":[]}',
    )
    writer = SegmentedCaptureWriter(
        generation,
        plan_sha256=plan_sha,
        process_boot_id="boot-g1",
        maximum_total_bytes=1_000_000,
        emergency_reserve_bytes=1024,
    )
    writer.append(frame, record_to_json_line(frame))
    writer.abort()
    [partial] = generation.glob("*.partial")
    original = partial.read_bytes()

    monkeypatch.setattr("os.path.ismount", lambda p: Path(p) == tmp_path)
    usage_type = type(shutil.disk_usage(root))
    monkeypatch.setattr(shutil, "disk_usage", lambda _path: usage_type(10_000, 9_900, 100))
    shed: list[str] = []
    budget = DiskBudget(
        root,
        maximum_total_bytes=1_000_000,
        minimum_free_bytes=100,
        expected_mount=tmp_path,
        event_depth_shed=lambda: shed.append("depth"),
        emergency_reserve_bytes=1024,
    )

    with pytest.raises(CaptureStorageCapacityError, match="free-space floor"):
        SegmentedCaptureWriter(
            generation,
            plan_sha256=plan_sha,
            process_boot_id="boot-g1",
            maximum_total_bytes=1_000_000,
            emergency_reserve_bytes=1024,
            capacity_guard=budget.ensure_capacity,
        )
    assert shed == ["depth"]
    assert partial.read_bytes() == original
    assert not list(generation.glob("*.recovery"))


@pytest.mark.asyncio
async def test_capture_frame_offered_before_miniticker_inspection() -> None:
    observed: list[str] = []

    class ReceiptAdapter:
        frame_seq = 0

        async def consume(self, frames: object) -> None:
            async for frame in frames:  # type: ignore[attr-defined]
                assert isinstance(frame, str)
                observed.append("raw_offered")
                self.frame_seq += 1

    async def frames() -> object:
        yield '{"stream":"!miniTicker@arr","data":[]}'

    adapter = _CaptureThenInspect(ReceiptAdapter(), lambda _: observed.append("parsed"))
    await adapter.consume(frames())
    assert observed == ["raw_offered", "parsed"]
    assert adapter.frame_seq == 1


def test_cli_requires_explicit_enable() -> None:
    with pytest.raises(SystemExit, match="2"):
        main(["--config", "config/pump_fade_v3/collector.json"])


def test_owner_completion_gates_clean_generation() -> None:
    from signalbot.pump_fade_v3.collector import _owners_abnormally_completed

    assert not _owners_abnormally_completed(
        prematurely_done=False, results=[asyncio.CancelledError()]
    )
    assert _owners_abnormally_completed(prematurely_done=True, results=[None])
    assert _owners_abnormally_completed(
        prematurely_done=True, results=[asyncio.CancelledError()]
    )
    assert _owners_abnormally_completed(
        prematurely_done=False, results=[RuntimeError("owner failed")]
    )


@pytest.mark.asyncio
async def test_minute_gate_keeps_trigger_and_forceorder_frames() -> None:
    from types import SimpleNamespace

    from signalbot.capture.models import CaptureEnvelopeV1
    from signalbot.capture.receipts import IngestSequencer, ReceiptTimestamp

    captured: list[CaptureEnvelopeV1] = []
    samples = iter((1000, 1010, 1020, 1030, 61_000))

    class Clock:
        def capture(self) -> ReceiptTimestamp:
            now = next(samples)
            return ReceiptTimestamp(now, now * 1_000_000)

    plan = build_websocket_plans((), include_event_depth=False)[0]
    adapter = SimpleNamespace(
        frame_seq=0, clock=Clock(), sequencer=IngestSequencer(),
        plan=plan, plan_sha256="a" * 64, process_boot_id="boot",
        connection_id="global-g1", pipeline=SimpleNamespace(offer=captured.append),
    )

    def on_frame(raw: str | bytes, _now_ms: int) -> bool:
        message = json.loads(raw)
        return message["stream"] == "!forceOrder@arr" or message.get("data") == "trigger"

    async def frames():
        for stream, data in (
            ("!miniTicker@arr", "quiet"), ("!miniTicker@arr", "quiet"),
            ("!miniTicker@arr", "trigger"), ("!forceOrder@arr", "liquidation"),
            ("!miniTicker@arr", "quiet"),
        ):
            yield json.dumps({"stream": stream, "data": data})

    consumer = _MinuteGlobalCapture(adapter, on_frame)
    await consumer.consume(frames())
    assert len(captured) == 4
    assert consumer.frame_seq == 4
    assert [record.ingest_seq for record in captured] == [1, 2, 3, 4]


@pytest.mark.asyncio
async def test_standalone_fake_websocket_and_rest_capture(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    import httpx

    root = tmp_path / "data"
    root.mkdir()
    source = await asyncio.to_thread(
        Path("config/pump_fade_v3/collector.json").read_text
    )
    config = json.loads(source)
    config.update(data_directory=str(root), maximum_total_bytes=1024**3,
                  minimum_free_bytes=1024**2)
    config_path = tmp_path / "config.json"
    config_path.write_text(json.dumps(config))
    monkeypatch.setattr("os.path.ismount", lambda p: Path(p) == tmp_path)
    stop = asyncio.Event()

    @asynccontextmanager
    async def connector(_url: str):
        async def frames():
            yield json.dumps({"stream": "!miniTicker@arr", "data": [
                {"s": "BTCUSDT", "o": "100", "c": "120", "q": "2000000", "E": 1000}
            ]})
            stop.set()
        yield frames()

    def respond(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"data": []})

    await asyncio.wait_for(run_standalone(
        config_path, enable=True, expected_mount=tmp_path,
        connector=connector, rest_transport=httpx.MockTransport(respond),
        operator_stop=stop,
    ), timeout=6)
    receipts = list(root.glob("capture/capture-*-g????/generation-receipt.json"))
    assert len(receipts) == 1
    assert json.loads(receipts[0].read_text())["forceOrder_censored"] is True
    terminal = receipts[0].parent / "generation-terminal.json"
    assert json.loads(terminal.read_text())["archive_clean"] is True
    data = json.loads(terminal.read_text())
    assert data["schema"] == "pump_fade_v3_generation_terminal_v1"
    assert data["status"] == "CLEAN_CLOSED"
    assert data["recovered_crash"] is False
    assert data["generation_directory"] == receipts[0].parent.name
    assert data["start_sha256"] == __import__("hashlib").sha256(
        receipts[0].read_bytes()
    ).hexdigest()


def test_interrupted_generation_recovery_is_incomplete_and_idempotent(tmp_path: Path) -> None:
    directory = tmp_path / "capture" / "capture-boot-g0001"
    directory.mkdir(parents=True)
    receipt = {"schema": "pump_fade_v3_generation_v1", "boot": "boot",
               "generation": 1, "plan_sha256": "a" * 64,
               "process_boot_id": "boot-g1"}
    (directory / "generation-receipt.json").write_text(json.dumps(receipt))
    class Budget:
        maximum_total_bytes = 1_000_000
        emergency_reserve_bytes = 1024

        def ensure_capacity(self, current_bytes: int, incoming_bytes: int) -> None:
            assert incoming_bytes >= 0

        def reconcile(self) -> int:
            return 0

    assert recover_previous_generations(tmp_path / "capture", budget=Budget()) == 1
    terminal = json.loads((directory / "generation-terminal.json").read_text())
    assert terminal["status"] == "RECOVERED_CRASH_INCOMPLETE"
    assert terminal["schema"] == "pump_fade_v3_generation_terminal_v1"
    assert terminal["recovered_crash"] is True
    assert terminal["archive_clean"] is False
    assert terminal["restart_gap"] == "UNKNOWN_AFTER_UNCLEAN_STOP"
    assert recover_previous_generations(tmp_path / "capture", budget=Budget()) == 0
