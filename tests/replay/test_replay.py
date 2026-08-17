from pathlib import Path

import pytest

from signalbot.backtest.replay import ReplayEngine
from signalbot.clock import ReplayClock
from signalbot.config import Settings
from signalbot.domain.enums import Market
from signalbot.persistence.repository import SqlRepository
from signalbot.runtime import MarketRuntime

FIXTURE = Path(__file__).resolve().parents[1] / "fixtures/replay/sample_events.jsonl"


async def run_once():
    settings = Settings.model_validate(
        {
            "binance": {
                "markets": ["spot"],
                "intervals": ["5m"],
                "primary_interval": "5m",
                "bootstrap_candles": 260,
                "history_limit": 300,
            },
            "storage": {"url": "sqlite:///:memory:"},
            "runtime": {"persist_candles": False},
        }
    )
    clock = ReplayClock()
    repo = SqlRepository(settings.storage.url)
    repo.initialize()

    async def discard(_decision):
        return None

    runtime = MarketRuntime(Market.SPOT, settings, repo, clock, discard)
    result = await ReplayEngine(runtime, clock).run_file(FIXTURE)
    repo.close()
    return result, clock.now_ms()


@pytest.mark.asyncio
async def test_replay_is_deterministic_and_advances_virtual_clock() -> None:
    first, first_clock = await run_once()
    second, second_clock = await run_once()
    assert first == second
    assert first.events_read == 3
    assert first.parse_errors == 0
    assert first.decisions == ()
    assert first_clock == second_clock == 1_710_000_300_000
