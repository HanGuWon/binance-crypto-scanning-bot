from collections import deque
from typing import Any

from conftest import make_feature
from signalbot.clock import ReplayClock
from signalbot.config import Settings
from signalbot.domain.enums import Market
from signalbot.persistence.repository import SqlRepository
from signalbot.runtime import MarketRuntime


def test_higher_timeframe_context_never_uses_a_future_closed_candle() -> None:
    settings = Settings.model_validate(
        {
            "binance": {
                "markets": ["spot"],
                "intervals": ["5m", "15m", "1h"],
                "primary_interval": "5m",
            },
            "storage": {"url": "sqlite:///:memory:"},
        }
    )
    repository = SqlRepository(settings.storage.url)
    repository.initialize()

    async def discard(_decision: Any) -> None:
        return None

    runtime = MarketRuntime(Market.SPOT, settings, repository, ReplayClock(), discard)
    runtime._feature_history[("BTCUSDT", "5m")] = deque(
        [make_feature(market=Market.SPOT, interval="5m", event_time_ms=299_999)],
        maxlen=4,
    )
    runtime._feature_history[("BTCUSDT", "15m")] = deque(
        [
            make_feature(market=Market.SPOT, interval="15m", event_time_ms=1),
            make_feature(market=Market.SPOT, interval="15m", event_time_ms=300_000),
            make_feature(market=Market.SPOT, interval="15m", event_time_ms=900_000),
        ],
        maxlen=4,
    )
    runtime._feature_history[("BTCUSDT", "1h")] = deque(
        [make_feature(market=Market.SPOT, interval="1h", event_time_ms=299_999)],
        maxlen=4,
    )
    contexts = runtime._context_features("btcusdt", 300_000)
    assert set(contexts) == {"15m", "1h"}
    assert contexts["15m"].event_time_ms == 1
    repository.close()
