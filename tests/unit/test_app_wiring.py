import pytest

import signalbot.app as app_module
from signalbot.app import SignalApplication
from signalbot.config import Settings
from signalbot.domain.enums import Market


@pytest.mark.asyncio
async def test_application_market_runtime_does_not_use_notifier_send(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    handlers: list[object] = []

    class FakeNotifier:
        def __init__(self, *_args: object, **_kwargs: object) -> None:
            pass

        def recover_inflight(self) -> int:
            return 0

        async def dispatch_pending(self) -> list[object]:
            return []

        async def send(self, _decision: object) -> object:
            raise AssertionError("market runtime must not call Discord send directly")

        async def close(self) -> None:
            return None

    class FakeRuntime:
        def __init__(
            self,
            _market: Market,
            _settings: Settings,
            _repository: object,
            _clock: object,
            decision_handler: object,
        ) -> None:
            handlers.append(decision_handler)

    class FakeScanner:
        def __init__(
            self,
            market: Market,
            _settings: Settings,
            _clock: object,
            _runtime: object,
            _stop_event: object,
            **_kwargs: object,
        ) -> None:
            self.market = market

        async def run(self) -> None:
            return None

        async def close(self) -> None:
            return None

    monkeypatch.setattr(app_module, "DiscordNotifier", FakeNotifier)
    monkeypatch.setattr(app_module, "MarketRuntime", FakeRuntime)
    monkeypatch.setattr(app_module, "MarketScanner", FakeScanner)

    settings = Settings.model_validate(
        {
            "binance": {"markets": ["spot"]},
            "storage": {"url": "sqlite:///:memory:"},
        }
    )
    application = SignalApplication(settings)
    await application.run()

    assert handlers == [application._after_decision_persisted]
