from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast

import httpx
import pytest

from conftest import make_candle
from signalbot.config import BinanceSettings, load_settings
from signalbot.data.candles import CandleConflictError, CandleStore
from signalbot.domain.enums import Market
from signalbot.domain.models import Candle
from signalbot.exchange.binance.rest import BinanceRestClient
from signalbot.scanner import MarketScanner

STEP_MS = 300_000
MARGIN_MS = 2_000


def _row(open_time_ms: int, close: str = "100") -> list[Any]:
    return [
        open_time_ms, "100", "101", "99", close, "10",
        open_time_ms + STEP_MS - 1, "1000", 5, "4", "400", "0",
    ]  # fmt: skip


def _rest(rows: list[list[Any]]) -> BinanceRestClient:
    transport = httpx.MockTransport(lambda request: httpx.Response(200, json=rows))
    http = httpx.AsyncClient(transport=transport, base_url="https://example.test")
    return BinanceRestClient(Market.SPOT, client=http)


class _Runtime:
    def __init__(self) -> None:
        self.store = CandleStore(600)
        self.repository = SimpleNamespace(save_candles=lambda candles: len(candles))

    def bootstrap(self, candles: list[Candle], *, rebuild: bool) -> None:
        for candle in candles:
            self.store.add(candle)

    def rebuild_derived_state(self) -> None:
        return None


def _scanner(rest: BinanceRestClient, runtime: _Runtime, now_ms: int, margin: int) -> MarketScanner:
    return cast(
        MarketScanner,
        SimpleNamespace(
            settings=SimpleNamespace(
                binance=SimpleNamespace(
                    rest_concurrency=1,
                    bootstrap_candles=60,
                    intervals=["5m"],
                    bootstrap_close_margin_ms=margin,
                ),
                runtime=SimpleNamespace(persist_candles=False),
            ),
            rest=rest,
            runtime=runtime,
            clock=SimpleNamespace(now_ms=lambda: now_ms),
        ),
    )


def _bootstrapped_open_times(runtime: _Runtime) -> list[int]:
    series = runtime.store._series.get((Market.SPOT, "BTCUSDT", "5m"), [])
    return [c.open_time_ms for c in series]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("close_offset_from_threshold", "kept"),
    [(-1, True), (0, False), (1, False)],
)
async def test_margin_boundary(close_offset_from_threshold: int, kept: bool) -> None:
    now_ms = 10 * STEP_MS
    threshold = now_ms - MARGIN_MS
    # candle whose close_time_ms == threshold + offset
    open_time = threshold + close_offset_from_threshold - (STEP_MS - 1)
    runtime = _Runtime()
    scanner = _scanner(_rest([_row(open_time)]), runtime, now_ms, MARGIN_MS)
    await MarketScanner._bootstrap(scanner, ["BTCUSDT"])
    assert (open_time in _bootstrapped_open_times(runtime)) is kept


@pytest.mark.asyncio
async def test_clock_skew_of_50ms_no_longer_admits_open_candle() -> None:
    open_time = 5 * STEP_MS
    close_time = open_time + STEP_MS - 1
    exchange_now = close_time - 20  # candle is still open on Binance
    local_now = exchange_now + 50  # local clock 50 ms ahead => now > close_time
    assert local_now > close_time
    runtime = _Runtime()
    rows = [_row(open_time - STEP_MS), _row(open_time, close="100")]
    scanner = _scanner(_rest(rows), runtime, local_now, MARGIN_MS)
    await MarketScanner._bootstrap(scanner, ["BTCUSDT"])
    assert _bootstrapped_open_times(runtime) == [open_time - STEP_MS]
    final = make_candle(0).model_copy(
        update={
            "market": Market.SPOT,
            "symbol": "BTCUSDT",
            "interval": "5m",
            "open_time_ms": open_time,
            "close_time_ms": close_time,
            "close": 100.5,
        }
    )
    assert runtime.store.add(final) is True  # no CandleConflictError


@pytest.mark.asyncio
async def test_zero_margin_preserves_legacy_behavior_and_exposes_the_conflict() -> None:
    open_time = 5 * STEP_MS
    close_time = open_time + STEP_MS - 1
    runtime = _Runtime()
    scanner = _scanner(_rest([_row(open_time)]), runtime, close_time + 30, 0)
    await MarketScanner._bootstrap(scanner, ["BTCUSDT"])
    assert open_time in _bootstrapped_open_times(runtime)
    final = runtime.store._series[(Market.SPOT, "BTCUSDT", "5m")][0].model_copy(
        update={"close": runtime.store._series[(Market.SPOT, "BTCUSDT", "5m")][0].close + 1}
    )
    with pytest.raises(CandleConflictError):
        runtime.store.add(final)


def test_margin_setting_bounds_and_default() -> None:
    assert BinanceSettings().bootstrap_close_margin_ms == 2_000
    assert BinanceSettings(bootstrap_close_margin_ms=0).bootstrap_close_margin_ms == 0
    assert BinanceSettings(bootstrap_close_margin_ms=60_000).bootstrap_close_margin_ms == 60_000
    with pytest.raises(ValueError):
        BinanceSettings(bootstrap_close_margin_ms=-1)
    with pytest.raises(ValueError):
        BinanceSettings(bootstrap_close_margin_ms=60_001)


def test_existing_config_files_still_validate(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The new optional setting must not break any shipped Settings YAML."""

    monkeypatch.delenv("SIGNALBOT_DISCORD_WEBHOOK_URL", raising=False)
    root = Path(__file__).resolve().parents[2]
    sources = [
        root / "config" / "settings.example.yaml",
        *sorted((root / "config").glob("prospective.*.yaml")),
        *sorted((root / "config").glob("research.*.yaml")),
    ]
    substitutions = {
        "__CAMPAIGN_ID__": "campaign-test",
        "__SOURCE_IDENTITY_HEX__": "a" * 64,
        "__CREATED_AT_MS__": "1700000000000",
        "__ACTIVATION_MS__": "1700000300000",
    }
    for template in sorted((root / "deployment" / "config").glob("*.template")):
        text = template.read_text(encoding="utf-8")
        for token, value in substitutions.items():
            text = text.replace(token, value)
        rendered = tmp_path / template.name.removesuffix(".template")
        rendered.write_text(text, encoding="utf-8")
        sources.append(rendered)
    assert len(sources) >= 5
    loaded = 0
    for path in sources:
        try:
            settings = load_settings(path)
        except ValueError as exc:
            # Some frozen historical configs are intentionally incomplete
            # placeholders; they must not fail because of the new setting.
            assert "bootstrap_close_margin_ms" not in str(exc), path.name
            continue
        assert settings.binance.bootstrap_close_margin_ms == 2_000, path.name
        loaded += 1
    assert loaded >= 3
