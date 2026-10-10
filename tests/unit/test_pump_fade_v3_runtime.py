from __future__ import annotations

from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from signalbot.pump_fade_v3.config import CollectorConfig
from signalbot.pump_fade_v3.rest_budget import RestCircuitOpen, RestWeightBudget
from signalbot.pump_fade_v3.runtime import PumpFadeV3Runtime, rest_poll_plan


class StubOwner:
    def __init__(self) -> None:
        self.calls: list[tuple[int, str]] = []

    def install(self, plans: Any, *, generation: int, plan_sha256: str) -> None:
        self.calls.append((generation, plan_sha256))


class StubRest:
    def __init__(self) -> None:
        self.calls: list[dict[str, Any]] = []

    async def capture_attempt(self, **kwargs: Any) -> Any:
        self.calls.append(kwargs)
        return SimpleNamespace(
            response_status=200, response_headers=(), response_completed_at_ms=0
        )


def _config(enabled: bool = True) -> CollectorConfig:
    return CollectorConfig(
        enabled=enabled, data_directory=Path("/data/v3"), maximum_total_bytes=16 * 1024**3,
        minimum_free_bytes=12 * 1024**3, maximum_event_symbols=25,
        event_lifetime_ms=26 * 3600 * 1000, event_return_percent=Decimal(20),
        event_minimum_quote_volume_usdt=Decimal(2_000_000),
        rest_weight_budget_per_minute=600, event_depth_enabled=True,
    )


def test_disabled_runtime_cannot_schedule_or_install() -> None:
    owner, adapter = StubOwner(), StubRest()
    runtime = PumpFadeV3Runtime(_config(False), ws_owner=owner,
                                rest_adapter=adapter, budget=RestWeightBudget())
    assert runtime.refresh(now_ms=0) is None
    assert runtime.due_polls(now_ms=0) == ()
    assert owner.calls == []


def test_dynamic_generation_idempotence_and_cadences() -> None:
    owner, adapter = StubOwner(), StubRest()
    runtime = PumpFadeV3Runtime(_config(), ws_owner=owner,
                                rest_adapter=adapter, budget=RestWeightBudget())
    initial = runtime.refresh(now_ms=0)
    assert initial is not None and initial.generation == 1
    assert runtime.refresh(now_ms=1) is initial
    row = {"s": "BTCUSDT", "st": 1, "P": "20", "q": "2000000", "E": 1000}
    updated = runtime.observe_ticker(row, now_ms=1000)
    assert updated is not None and updated.generation == 2
    assert len(owner.calls) == 2
    assert [p.interval_ms for p in rest_poll_plan(("BTCUSDT",))] == [
        60_000, 300_000, 1_800_000, 60_000,
    ]
    assert len(runtime.due_polls(now_ms=1000)) == 4
    assert runtime.snapshot(now_ms=1000) == ("BTCUSDT",)
    assert runtime.snapshot(now_ms=2000) is None
    assert runtime.liquidation_is_censored()


@pytest.mark.asyncio
async def test_poll_uses_receipt_adapter_without_network() -> None:
    owner, adapter = StubOwner(), StubRest()
    runtime = PumpFadeV3Runtime(_config(), ws_owner=owner,
                                rest_adapter=adapter, budget=RestWeightBudget())
    runtime.refresh(now_ms=0)
    poll = rest_poll_plan(())[0]
    await runtime.attempt_poll(poll, now_ms=0)
    assert adapter.calls[0]["url"] == "https://fapi.binance.com/fapi/v1/premiumIndex"
    assert adapter.calls[0]["method"] == "GET"


@pytest.mark.asyncio
async def test_http_418_opens_circuit_and_preserves_capture_attempt() -> None:
    owner, adapter = StubOwner(), StubRest()

    async def banned(**kwargs: Any) -> Any:
        adapter.calls.append(kwargs)
        return SimpleNamespace(
            response_status=418, response_headers=(), response_completed_at_ms=0
        )

    adapter.capture_attempt = banned  # type: ignore[method-assign]
    runtime = PumpFadeV3Runtime(_config(), ws_owner=owner,
                                rest_adapter=adapter, budget=RestWeightBudget())
    runtime.refresh(now_ms=0)
    with pytest.raises(RestCircuitOpen):
        await runtime.attempt_poll(rest_poll_plan(())[0], now_ms=0)
    assert len(adapter.calls) == 1
    with pytest.raises(RestCircuitOpen):
        await runtime.attempt_poll(rest_poll_plan(())[0], now_ms=60_000)
    assert len(adapter.calls) == 1


@pytest.mark.asyncio
async def test_http_429_retries_with_stable_correlation(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    owner, adapter = StubOwner(), StubRest()
    sleeps: list[float] = []

    monotonic = [0.0]

    async def no_sleep(seconds: float) -> None:
        sleeps.append(seconds)
        monotonic[0] += seconds

    async def responses(**kwargs: Any) -> Any:
        adapter.calls.append(kwargs)
        if len(adapter.calls) == 1:
            return SimpleNamespace(
                response_status=429,
                response_headers=(("Retry-After", "3"),),
                response_completed_at_ms=59_999,
            )
        return SimpleNamespace(
            response_status=200,
            response_headers=(("X-MBX-USED-WEIGHT-1M", "18"),),
            response_completed_at_ms=62_050,
        )

    adapter.capture_attempt = responses  # type: ignore[method-assign]
    runtime = PumpFadeV3Runtime(_config(), ws_owner=owner,
                                rest_adapter=adapter, budget=RestWeightBudget(),
                                monotonic=lambda: monotonic[0], sleep=no_sleep)
    runtime.refresh(now_ms=0)
    budget = runtime.budget
    with caplog.at_level("INFO", logger="signalbot.pump_fade_v3.rest_budget"):
        await runtime.attempt_poll(rest_poll_plan(())[0], now_ms=59_000)
        budget.flush_minute(now_ms=120_000)
    assert [call["attempt"] for call in adapter.calls] == [1, 2]
    assert adapter.calls[0]["correlation_id"] == adapter.calls[1]["correlation_id"]
    assert sleeps == [3]
    # The first request and retry belong to separate UTC minutes. The response
    # header is bucketed by completion time, not the initial poll timestamp.
    assert budget._summary_bucket == 2
    assert "requests=1 weight=10 last_x_mbx_used_weight_1m=None" in caplog.text
    assert "requests=1 weight=10 last_x_mbx_used_weight_1m=18" in caplog.text
