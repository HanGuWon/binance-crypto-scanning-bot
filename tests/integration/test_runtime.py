from decimal import Decimal

import pytest

from conftest import make_candle, make_decision, make_feature
from signalbot.clock import ReplayClock
from signalbot.config import Settings
from signalbot.domain.enums import Direction, Market, SignalFamily, SignalStage
from signalbot.domain.models import Candle, MiniTicker, RuleEvaluation, SignalDecision
from signalbot.persistence.repository import SqlRepository
from signalbot.runtime import MarketRuntime


@pytest.mark.asyncio
async def test_runtime_ignores_open_candle_and_runs_intrabar_anomaly_pipeline() -> None:
    settings = Settings.model_validate(
        {
            "binance": {
                "markets": ["spot"],
                "intervals": ["5m"],
                "primary_interval": "5m",
                "bootstrap_candles": 260,
                "history_limit": 300,
            },
            "signals": {
                "anomaly_horizon_seconds": 10,
                "anomaly_min_absolute_return": 0.01,
                "anomaly_robust_zscore": 2,
                "anomaly_min_points": 5,
                "anomaly_history_points": 50,
            },
            "storage": {"url": "sqlite:///:memory:"},
            "runtime": {"persist_candles": True},
        }
    )
    repo = SqlRepository(settings.storage.url)
    repo.initialize()
    clock = ReplayClock()
    decisions = []

    async def collect(decision):
        decisions.append(decision)

    runtime = MarketRuntime(Market.SPOT, settings, repo, clock, collect)
    runtime.set_surveillance_symbols(frozenset({"TESTUSDT"}))
    open_candle = Candle(
        market=Market.SPOT,
        symbol="TESTUSDT",
        interval="5m",
        open_time_ms=0,
        close_time_ms=299_999,
        open=Decimal("100"),
        high=Decimal("101"),
        low=Decimal("99"),
        close=Decimal("100"),
        volume=Decimal("10"),
        quote_volume=Decimal("1000"),
        trade_count=1,
        taker_buy_base_volume=Decimal("5"),
        taker_buy_quote_volume=Decimal("500"),
        is_closed=False,
    )
    await runtime.handle_event(open_candle)
    assert runtime.candles.size(Market.SPOT, "TESTUSDT", "5m") == 0

    prices = [100 + (0.001 if i % 2 else 0) for i in range(10)] + [103]
    for index, price in enumerate(prices):
        await runtime.handle_event(
            MiniTicker(
                market=Market.SPOT,
                symbol="TESTUSDT",
                event_time_ms=index * 1_000,
                close=Decimal(str(price)),
            )
        )

    assert len(decisions) == 1
    assert decisions[0].family is SignalFamily.PUMP_RISK
    assert decisions[0].stage is SignalStage.CONFIRMED
    assert repo.recent_signals() == decisions
    repo.close()


@pytest.mark.asyncio
async def test_runtime_rejects_partial_gap_recovery_without_inserting_new_data() -> None:
    settings = Settings.model_validate(
        {
            "binance": {"markets": ["spot"], "intervals": ["5m"]},
            "storage": {"url": "sqlite:///:memory:"},
        }
    )
    repo = SqlRepository(settings.storage.url)
    repo.initialize()

    async def collect(_decision):
        return None

    async def partial(_gap):
        return [make_candle(1)]

    runtime = MarketRuntime(
        Market.SPOT, settings, repo, ReplayClock(), collect, partial
    )
    runtime.set_surveillance_symbols(frozenset({"BTCUSDT"}))
    runtime.bootstrap([make_candle(0)])

    await runtime.handle_event(make_candle(3))

    assert runtime.candles.size(Market.SPOT, "BTCUSDT", "5m") == 1
    assert runtime.candles.latest(Market.SPOT, "BTCUSDT", "5m") == make_candle(0)
    repo.close()


@pytest.mark.asyncio
async def test_runtime_accepts_only_a_complete_contiguous_gap_recovery() -> None:
    settings = Settings.model_validate(
        {
            "binance": {"markets": ["spot"], "intervals": ["5m"]},
            "storage": {"url": "sqlite:///:memory:"},
        }
    )
    repo = SqlRepository(settings.storage.url)
    repo.initialize()

    async def collect(_decision):
        return None

    async def complete(_gap):
        return [make_candle(2), make_candle(1)]

    runtime = MarketRuntime(
        Market.SPOT, settings, repo, ReplayClock(), collect, complete
    )
    runtime.set_surveillance_symbols(frozenset({"BTCUSDT"}))
    runtime.bootstrap([make_candle(0)])

    await runtime.handle_event(make_candle(3))

    assert runtime.candles.size(Market.SPOT, "BTCUSDT", "5m") == 4
    assert runtime.candles.latest(Market.SPOT, "BTCUSDT", "5m") == make_candle(3)
    assert runtime.regime.snapshot(Market.SPOT, 1_500_000).breadth_ratio == 1.0
    repo.close()


@pytest.mark.asyncio
async def test_runtime_atomically_keeps_pending_alert_when_handler_crashes() -> None:
    settings = Settings.model_validate(
        {
            "binance": {"markets": ["spot"], "intervals": ["5m"]},
            "alerts": {
                "discord_enabled": True,
                "discord_webhook_url": "https://discord.test/webhook",
            },
            "storage": {"url": "sqlite:///:memory:"},
        }
    )
    repo = SqlRepository(settings.storage.url)
    repo.initialize()

    async def crash(_decision):
        raise RuntimeError("injected crash after atomic enqueue")

    runtime = MarketRuntime(Market.SPOT, settings, repo, ReplayClock(1_000), crash)
    evaluation = RuleEvaluation(
        market=Market.SPOT,
        symbol="BTCUSDT",
        family=SignalFamily.BREAKOUT_LONG,
        direction=Direction.LONG,
        timeframe="5m",
        event_time_ms=600_000,
        score=65,
        triggered=True,
        eligible=True,
        price=Decimal("100"),
        reasons=("frozen C0 trigger",),
        invalidation=Decimal("98"),
    )

    with pytest.raises(RuntimeError, match="injected crash"):
        await runtime._process(evaluation)

    signals = repo.recent_signals()
    assert len(signals) == 1
    outbox = repo.get_outbox(signals[0].event_id)
    assert outbox is not None
    assert outbox.status == "pending"
    assert outbox.attempts == 0
    repo.close()


@pytest.mark.asyncio
async def test_runtime_persists_one_paper_gap_exit_without_replay_duplicate() -> None:
    settings = Settings.model_validate(
        {
            "binance": {
                "markets": ["spot"],
                "intervals": ["5m"],
                "primary_interval": "5m",
            },
            "signals": {"technical_exit": {"enabled": True}},
            "storage": {"url": "sqlite:///:memory:"},
        }
    )
    repo = SqlRepository(settings.storage.url)
    repo.initialize()
    decisions = []

    async def collect(decision):
        decisions.append(decision)

    runtime = MarketRuntime(Market.SPOT, settings, repo, ReplayClock(), collect)
    runtime.set_surveillance_symbols(frozenset({"BTCUSDT"}))
    signal_bar = make_candle(0, close=100)
    entry_bar = make_candle(1, close=100)
    runtime.bootstrap([signal_bar, entry_bar])
    entry = make_decision(
        event_id="new-persisted-entry",
        market=Market.SPOT,
        symbol="BTCUSDT",
        family=SignalFamily.BREAKOUT_LONG,
        direction=Direction.LONG,
        timeframe="5m",
        event_time_ms=signal_bar.close_time_ms,
        price=signal_bar.close,
        invalidation=Decimal("90"),
    )
    persisted = await runtime._publish_decision(entry)
    assert persisted == entry
    assert await runtime._publish_decision(entry) is None
    signal_feature = make_feature(
        market=Market.SPOT,
        symbol="BTCUSDT",
        interval="5m",
        event_time_ms=signal_bar.close_time_ms,
        price=float(signal_bar.close),
    )
    entry_feature = signal_feature.model_copy(
        update={
            "event_time_ms": entry_bar.close_time_ms,
            "price": float(entry_bar.close),
        }
    )
    runtime.paper_positions.on_closed_candle(signal_bar, signal_feature, [entry])
    runtime.paper_positions.on_closed_candle(entry_bar, entry_feature, [])
    assert runtime.paper_positions.active_position_count == 1

    post_gap = make_candle(3, close=98).model_copy(
        update={"open": Decimal("98.5")}
    )
    await runtime.handle_event(post_gap)
    await runtime.handle_event(post_gap)

    exits = [
        decision
        for decision in repo.recent_signals()
        if decision.family is SignalFamily.TECHNICAL_EXIT
    ]
    assert len(exits) == 1
    assert exits[0].metadata["exit_reason"] == "data_gap"
    assert exits[0].metadata["paper_only"] is True
    assert exits[0].metadata["order_placed"] is False
    assert exits[0].action_label == "SPOT_EXIT"
    assert repo.get_outbox(exits[0].event_id).status == "disabled"  # type: ignore[union-attr]
    assert decisions.count(exits[0]) == 1
    repo.close()


@pytest.mark.asyncio
async def test_paper_gap_exit_restores_state_when_durable_persistence_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    settings = Settings.model_validate(
        {
            "binance": {
                "markets": ["spot"],
                "intervals": ["5m"],
                "primary_interval": "5m",
            },
            "signals": {"technical_exit": {"enabled": True}},
            "storage": {"url": "sqlite:///:memory:"},
        }
    )
    repo = SqlRepository(settings.storage.url)
    repo.initialize()

    async def collect(_decision: SignalDecision) -> None:
        return None

    runtime = MarketRuntime(Market.SPOT, settings, repo, ReplayClock(), collect)
    runtime.set_surveillance_symbols(frozenset({"BTCUSDT"}))
    signal_bar = make_candle(0, close=100)
    entry_bar = make_candle(1, close=100)
    runtime.bootstrap([signal_bar, entry_bar])
    entry = make_decision(
        event_id="persistence-failure-entry",
        market=Market.SPOT,
        symbol="BTCUSDT",
        family=SignalFamily.BREAKOUT_LONG,
        direction=Direction.LONG,
        timeframe="5m",
        event_time_ms=signal_bar.close_time_ms,
        price=signal_bar.close,
        invalidation=Decimal("90"),
    )
    feature = make_feature(
        market=Market.SPOT,
        symbol="BTCUSDT",
        interval="5m",
        event_time_ms=signal_bar.close_time_ms,
        price=float(signal_bar.close),
    )
    runtime.paper_positions.on_closed_candle(signal_bar, feature, [entry])
    runtime.paper_positions.on_closed_candle(
        entry_bar,
        feature.model_copy(
            update={
                "event_time_ms": entry_bar.close_time_ms,
                "price": float(entry_bar.close),
            }
        ),
        [],
    )
    assert runtime.paper_positions.active_position_count == 1

    original = repo.save_signal_and_enqueue

    def fail_technical_exit(
        decision: SignalDecision,
        payload: dict[str, object],
        created_at_ms: int,
        *,
        delivery_enabled: bool,
        maximum_active_items: int | None = None,
    ) -> bool:
        if decision.family is SignalFamily.TECHNICAL_EXIT:
            raise RuntimeError("injected paper-exit persistence failure")
        return original(
            decision,
            payload,
            created_at_ms,
            delivery_enabled=delivery_enabled,
            maximum_active_items=maximum_active_items,
        )

    monkeypatch.setattr(repo, "save_signal_and_enqueue", fail_technical_exit)
    post_gap = make_candle(3, close=98).model_copy(
        update={"open": Decimal("98.5")}
    )
    with pytest.raises(RuntimeError, match="persistence failure"):
        await runtime.handle_event(post_gap)

    assert runtime.paper_positions.active_position_count == 1
    assert all(
        decision.family is not SignalFamily.TECHNICAL_EXIT
        for decision in repo.recent_signals()
    )
    repo.close()


@pytest.mark.asyncio
async def test_paper_gap_exit_stays_committed_when_handler_fails_after_persistence() -> None:
    settings = Settings.model_validate(
        {
            "binance": {
                "markets": ["spot"],
                "intervals": ["5m"],
                "primary_interval": "5m",
            },
            "signals": {"technical_exit": {"enabled": True}},
            "storage": {"url": "sqlite:///:memory:"},
        }
    )
    repo = SqlRepository(settings.storage.url)
    repo.initialize()

    async def fail_after_persistence(decision: SignalDecision) -> None:
        if decision.family is SignalFamily.TECHNICAL_EXIT:
            raise RuntimeError("injected handler failure")

    runtime = MarketRuntime(
        Market.SPOT,
        settings,
        repo,
        ReplayClock(),
        fail_after_persistence,
    )
    runtime.set_surveillance_symbols(frozenset({"BTCUSDT"}))
    signal_bar = make_candle(0, close=100)
    entry_bar = make_candle(1, close=100)
    runtime.bootstrap([signal_bar, entry_bar])
    entry = make_decision(
        event_id="handler-failure-entry",
        market=Market.SPOT,
        symbol="BTCUSDT",
        family=SignalFamily.BREAKOUT_LONG,
        direction=Direction.LONG,
        timeframe="5m",
        event_time_ms=signal_bar.close_time_ms,
        price=signal_bar.close,
        invalidation=Decimal("90"),
    )
    feature = make_feature(
        market=Market.SPOT,
        symbol="BTCUSDT",
        interval="5m",
        event_time_ms=signal_bar.close_time_ms,
        price=float(signal_bar.close),
    )
    runtime.paper_positions.on_closed_candle(signal_bar, feature, [entry])
    runtime.paper_positions.on_closed_candle(
        entry_bar,
        feature.model_copy(
            update={
                "event_time_ms": entry_bar.close_time_ms,
                "price": float(entry_bar.close),
            }
        ),
        [],
    )

    post_gap = make_candle(3, close=98).model_copy(
        update={"open": Decimal("98.5")}
    )
    with pytest.raises(RuntimeError, match="handler failure"):
        await runtime.handle_event(post_gap)

    assert runtime.paper_positions.active_position_count == 0
    exits = [
        decision
        for decision in repo.recent_signals()
        if decision.family is SignalFamily.TECHNICAL_EXIT
    ]
    assert len(exits) == 1
    assert repo.get_outbox(exits[0].event_id) is not None
    repo.close()
