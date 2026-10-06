import httpx
import pytest

from conftest import make_candle, make_decision, make_feature
from signalbot.api.server import create_api
from signalbot.domain.enums import Market
from signalbot.persistence.repository import SqlRepository
from signalbot.signals.protection_context import ProtectionContext


def _context(index: int) -> ProtectionContext:
    candle = make_candle(index, market=Market.FUTURES, symbol="BTCUSDT")
    feature = make_feature(
        market=Market.FUTURES,
        symbol="BTCUSDT",
        interval="5m",
        event_time_ms=candle.close_time_ms,
    )
    return ProtectionContext.from_closed_candle(
        candle=candle,
        feature=feature,
        higher_timeframe_contexts={},
        source_decision_clock_id=f"futures:BTCUSDT:5m:{candle.close_time_ms}",
    )


@pytest.mark.asyncio
async def test_read_only_api_health_and_signal_projection() -> None:
    repository = SqlRepository("sqlite:///:memory:")
    repository.initialize()
    repository.save_signal(make_decision())
    transport = httpx.ASGITransport(app=create_api(repository))
    try:
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            assert (await client.get("/health/live")).json() == {"status": "alive"}
            assert (await client.get("/health/ready")).json() == {"status": "ready"}
            response = await client.get("/signals/recent", params={"limit": 1})
            assert response.status_code == 200
            assert response.json()[0]["event_id"] == "event-1"
    finally:
        repository.close()


@pytest.mark.asyncio
async def test_protection_context_api_supports_bounded_polling_and_stale_status() -> None:
    repository = SqlRepository("sqlite:///:memory:")
    repository.initialize()
    first = _context(1)
    second = _context(2)
    repository.save_protection_context(first, created_at_ms=1_000)
    repository.save_protection_context(second, created_at_ms=2_000)
    transport = httpx.ASGITransport(app=create_api(repository))
    try:
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            response = await client.get(
                "/protection-contexts",
                params={
                    "market": "futures",
                    "symbol": "btcusdt",
                    "primary_interval": "5m",
                    "limit": 1,
                    "after_context_id": first.context_id,
                },
            )
            assert response.status_code == 200
            assert [item["context_id"] for item in response.json()] == [second.context_id]

            latest = await client.get(
                "/protection-contexts/latest",
                params={
                    "market": "futures",
                    "symbol": "BTCUSDT",
                    "primary_interval": "5m",
                    "max_age_ms": 1,
                },
            )
            assert latest.status_code == 200
            assert latest.json()["status"] == "STALE_CONTEXT"
            assert latest.json()["context"]["context_id"] == second.context_id

            invalid = await client.get(
                "/protection-contexts",
                params={
                    "market": "futures",
                    "symbol": "BTCUSDT",
                    "primary_interval": "5m",
                    "after_context_id": "missing",
                },
            )
            assert invalid.status_code == 409
            assert invalid.json()["detail"] == "PROTECTION_CONTEXT_CURSOR_INVALID"
    finally:
        repository.close()
