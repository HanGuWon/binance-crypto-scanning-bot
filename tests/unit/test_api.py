import httpx
import pytest

from conftest import make_decision
from signalbot.api.server import create_api
from signalbot.persistence.repository import SqlRepository


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
