from __future__ import annotations

import json
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast

import pytest
from sqlalchemy.orm import Session

from conftest import make_candle, make_decision, make_feature
from signalbot.alerts.embeds import build_discord_payload
from signalbot.alerts.policy import should_deliver_discord
from signalbot.clock import ReplayClock
from signalbot.config import BinanceSettings, MarketWatchSettings, Settings, SignalSettings
from signalbot.domain.enums import Direction, Market, SignalFamily, SignalStage
from signalbot.domain.models import Candle, GainerWatchState, Instrument, SignalDecision
from signalbot.exchange.binance.endpoints import build_websocket_plans
from signalbot.exchange.binance.rest import BinanceRestClient, BinanceRestError
from signalbot.exchange.binance.universe import Universe, UniverseSelector
from signalbot.persistence.models import MarketWatchEpisodeRow
from signalbot.persistence.repository import OutboxCapacityError, SqlRepository
from signalbot.runtime import MarketRuntime
from signalbot.scanner import MarketScanner
from signalbot.signals.market_watch import MarketWatchEngine
from signalbot.signals.state_machine import SignalStateMachine

HOURS_48 = 48 * 3_600_000


def instrument(symbol: str = "TESTUSDT", volume: str = "1000000000") -> Instrument:
    return Instrument(
        market=Market.FUTURES, symbol=symbol, base_asset=symbol.removesuffix("USDT"),
        quote_asset="USDT", status="TRADING", contract_type="PERPETUAL",
        quote_volume=Decimal(volume), price_change_percent_24h=20,
    )


def update(
    repo: SqlRepository, gainers: tuple[Instrument, ...], now: int,
    valid: frozenset[str] = frozenset({"TESTUSDT"}), cap: int = 10,
) -> tuple[GainerWatchState, ...]:
    return repo.update_gainer_watch(
        Market.FUTURES, gainers, valid, now, qualifying_quote_volume=1_000_000_000,
        retention_ms=HOURS_48, maximum_retained=cap,
    )


def test_retention_1b_boundary_restart_last_top5_and_exact_expiry(tmp_path: Path) -> None:
    url = f"sqlite:///{tmp_path / 'watch.db'}"
    repo = SqlRepository(url)
    repo.initialize()
    assert update(repo, (instrument(volume="999999999"),), 1000) == ()
    rows = update(repo, (instrument(),), 2000)
    assert rows[0].peak_quote_volume_usdt == 1_000_000_000
    assert rows[0].expires_at_ms == 2000 + HOURS_48
    # Qualifying once is enough; top5 membership, rather than volume outside top5, extends TTL.
    rows = update(repo, (instrument(volume="150000000"),), 3000)
    assert rows[0].expires_at_ms == 3000 + HOURS_48
    repo.close()
    restarted = SqlRepository(url)
    restarted.initialize()
    assert update(restarted, (), 3000 + HOURS_48 - 1)[0].last_top5_observed_ms == 3000
    assert update(restarted, (), 3000 + HOURS_48) == ()
    restarted.close()


def test_retention_cap_eviction_is_deterministic_and_delisting_prunes() -> None:
    repo = SqlRepository("sqlite:///:memory:")
    repo.initialize()
    valid = frozenset({"AAAUSDT", "BBBUSDT", "CCCUSDT"})
    update(repo, (instrument("BBBUSDT"), instrument("AAAUSDT")), 1000, valid, cap=1)
    rows = update(repo, (instrument("CCCUSDT"),), 2000, valid, cap=1)
    assert {row.symbol for row in rows} == {"AAAUSDT", "CCCUSDT"}
    # Evicted qualification is ended, even if a sub-1B coin returns to top5.
    rows = update(repo, (instrument("BBBUSDT", "150000000"),), 3000, valid, cap=1)
    assert {row.symbol for row in rows} == {"CCCUSDT"}
    assert update(repo, (), 4000, frozenset()) == ()
    repo.close()


def context() -> tuple[MarketWatchEngine, Candle, list[Candle]]:
    engine = MarketWatchEngine(MarketWatchSettings(enabled=True, context_alerts_enabled=True))
    state = GainerWatchState(
        market=Market.FUTURES, symbol="TESTUSDT", last_top5_observed_ms=0,
        expires_at_ms=HOURS_48, peak_quote_volume_usdt=1_200_000_000,
    )
    engine.set_priority_gainers((), (instrument(volume="150000000"),), (state,), 0)
    prior = [
        make_candle(i, market=Market.FUTURES, symbol="TESTUSDT", close=100).model_copy(
            update={"low": Decimal("90"), "high": Decimal("110")}
        ) for i in range(20)
    ]
    candle = make_candle(20, market=Market.FUTURES, symbol="TESTUSDT", close=96).model_copy(
        update={"low": Decimal("91"), "open": Decimal("95")}
    )
    return engine, candle, prior


def range_feature(candle: Candle, **changes: object):
    return make_feature(
        event_time_ms=candle.close_time_ms, price=float(candle.close), adx=19,
        efficiency_ratio_20=0.30, ema20_slope_atr=0.25, taker_buy_ratio=0.55,
        **changes,
    )


def test_retained_range_long_boundary_is_informational_with_target_and_invalidation() -> None:
    engine, candle, prior = context()
    evaluations = engine.evaluate(candle, range_feature(candle), prior)
    long = next(e for e in evaluations if e.family is SignalFamily.RANGE_LONG)
    assert long.triggered and not long.eligible
    assert long.invalidation == Decimal("89.5")
    assert long.metadata["range_target"] == 100
    assert long.metadata["retained_gainer"] is True
    assert not next(e for e in evaluations if e.family is SignalFamily.TOP_GAINER).triggered
    machine = SignalStateMachine(SignalSettings(), "test-watch")
    decision = machine.decision_for_observation(long)
    assert decision.stage is SignalStage.SETUP
    round_trip = SignalDecision.model_validate_json(decision.model_dump_json())
    assert round_trip.model_dump(mode="json") == decision.model_dump(mode="json")
    assert decision.action_label == "MARKET_WATCH"
    assert should_deliver_discord(decision, Settings().alerts.model_copy(
        update={"delivery_mode": "focused"}
    ))
    payload = str(build_discord_payload(decision, "test"))
    assert "LONG 매수 후보" in payload and "유지 추적" in payload
    assert "주도 매수 55.0%" in payload and "근거 강도: 100" not in payload


def test_retained_range_short_boundary() -> None:
    engine, candle, prior = context()
    candle = candle.model_copy(update={
        "close": Decimal("104"), "open": Decimal("105"),
        "high": Decimal("109"), "low": Decimal("103"),
    })
    feature = range_feature(candle).model_copy(update={"taker_buy_ratio": 0.45})
    short = next(
        e for e in engine.evaluate(candle, feature, prior) if e.family is SignalFamily.RANGE_SHORT
    )
    assert short.triggered and short.direction is Direction.SHORT and not short.eligible
    assert short.invalidation == Decimal("110.5")


@pytest.mark.parametrize("changes", [
    {"closed_kline_flow_available": False}, {"taker_buy_ratio": 0.549999},
    {"efficiency_ratio_20": None}, {"efficiency_ratio_20": 0.300001},
    {"adx": 20}, {"ema20_slope_atr": 0.250001}, {"atr": 11},
])
def test_range_rejects_missing_flow_and_threshold_negatives(changes: dict[str, object]) -> None:
    engine, candle, prior = context()
    feature = range_feature(candle).model_copy(update=changes)
    assert not any(
        e.triggered for e in engine.evaluate(candle, feature, prior)
        if e.family in {SignalFamily.RANGE_LONG, SignalFamily.RANGE_SHORT}
    )


def test_range_rejects_gap_two_edge_candle_and_cost_without_future_contamination() -> None:
    engine, candle, prior = context()
    feature = range_feature(candle)
    assert not next(
        e for e in engine.evaluate(candle, feature, prior[1:])
        if e.family is SignalFamily.RANGE_LONG
    ).triggered
    ambiguous = candle.model_copy(update={"high": Decimal("109")})
    assert not next(
        e for e in engine.evaluate(ambiguous, feature, prior) if e.family is SignalFamily.RANGE_LONG
    ).triggered
    future = make_candle(21, market=Market.FUTURES, symbol="TESTUSDT", close=10000)
    assert engine.evaluate(candle, feature, [*prior, future]) == engine.evaluate(
        candle, feature, prior
    )
    engine.settings = engine.settings.model_copy(update={"range_round_trip_cost_bps": 1000})
    assert not next(
        e for e in engine.evaluate(candle, feature, prior) if e.family is SignalFamily.RANGE_LONG
    ).triggered


def test_retained_trend_is_not_restricted_to_range_and_rearms_other_direction() -> None:
    engine, candle, prior = context()
    feature = make_feature(
        event_time_ms=candle.close_time_ms, price=110, ema20=105, ema50=100,
        ema20_slope_atr=0.5, macd_histogram=1, rsi=60, adx=25,
        bollinger_upper=95, bollinger_lower=80,
    )
    evaluations = engine.evaluate(candle, feature, prior)
    assert next(e for e in evaluations if e.family is SignalFamily.WATCH_TREND_UP).triggered
    assert not next(e for e in evaluations if e.family is SignalFamily.WATCH_TREND_DOWN).triggered
    assert next(e for e in evaluations if e.family is SignalFamily.BOLLINGER_UPPER_TOUCH).triggered
    bearish = feature.model_copy(update={
        "price": 90, "ema20": 95, "ema50": 100,
        "ema20_slope_atr": -0.5, "macd_histogram": -1, "rsi": 40,
    })
    evaluations = engine.evaluate(candle, bearish, prior)
    assert not next(e for e in evaluations if e.family is SignalFamily.WATCH_TREND_UP).triggered
    assert next(e for e in evaluations if e.family is SignalFamily.WATCH_TREND_DOWN).triggered
    # REST failure cannot extend retained TTL; wall-clock expiry suppresses all triggers.
    assert not any(e.triggered for e in engine.evaluate(candle, feature, prior, now_ms=HOURS_48))
    assert engine.evaluate(candle.model_copy(update={"is_closed": False}), feature, prior) == ()


def test_ranking_snapshot_is_causal_and_bounded() -> None:
    engine = MarketWatchEngine(MarketWatchSettings(enabled=True))
    candle = make_candle(20, market=Market.FUTURES, symbol="TESTUSDT")
    feature = make_feature(event_time_ms=candle.close_time_ms)
    engine.set_priority_gainers((instrument("TESTUSDT", "200000000"),), observed_ms=0)
    engine.set_priority_gainers(
        (instrument("TESTUSDT", "300000000"),), observed_ms=candle.close_time_ms + 1,
    )
    assert engine.evaluate(candle, feature)[0].triggered
    assert engine.evaluate(candle, feature)[0].metadata["quote_volume_24h_usdt"] == 200_000_000
    later = make_candle(21, market=Market.FUTURES, symbol="TESTUSDT")
    later_feature = feature.model_copy(update={"event_time_ms": later.close_time_ms})
    assert engine.evaluate(later, later_feature)[0].metadata["quote_volume_24h_usdt"] == 300_000_000
    for i in range(5):
        engine.set_priority_gainers((instrument(),), observed_ms=later.close_time_ms + 1 + i)
    assert engine.evaluate(candle, feature) == ()  # delay exceeds retained snapshot history


def runtime(repo: SqlRepository) -> MarketRuntime:
    settings = Settings.model_validate({
        "alerts": {"discord_enabled": True, "discord_webhook_url": "https://discord.test/webhook",
                   "delivery_mode": "focused", "market_watch": {"enabled": True}},
    })

    async def collected(_decision: SignalDecision) -> None:
        return None

    return MarketRuntime(Market.FUTURES, settings, repo, ReplayClock(1000), collected)


@pytest.mark.asyncio
async def test_pending_episode_survives_restart_and_false_observation_cannot_rewind() -> None:
    repo = SqlRepository("sqlite:///:memory:")
    repo.initialize()
    engine, candle, prior = context()
    event = next(e for e in engine.evaluate(candle, range_feature(candle), prior)
                 if e.family is SignalFamily.RANGE_LONG)
    first = await runtime(repo)._process(event)
    assert first is not None
    assert repo.get_outbox(first.event_id).status == "pending"  # type: ignore[union-attr]
    later = event.model_copy(update={"event_time_ms": event.event_time_ms + 3_600_000})
    assert await runtime(repo)._process(later) is None  # still same episode, even >cooldown
    old_false = event.model_copy(update={"triggered": False, "score": 0})
    await runtime(repo)._process(old_false)
    assert await runtime(repo)._process(later.model_copy(
        update={"event_time_ms": later.event_time_ms + 1}
    )) is None
    false = later.model_copy(update={"event_time_ms": later.event_time_ms + 2,
                                     "triggered": False, "score": 0})
    await runtime(repo)._process(false)
    assert await runtime(repo)._process(false.model_copy(
        update={"event_time_ms": false.event_time_ms + 1, "triggered": True, "score": 100}
    )) is not None
    repo.close()


def test_episode_and_outbox_rollback_together_on_capacity_failure() -> None:
    repo = SqlRepository("sqlite:///:memory:")
    repo.initialize()
    repo.save_signal_and_enqueue(make_decision(), {"content": "occupied"}, 1, delivery_enabled=True)
    engine, candle, prior = context()
    event = next(e for e in engine.evaluate(candle, range_feature(candle), prior)
                 if e.family is SignalFamily.RANGE_LONG)
    decision = SignalStateMachine(SignalSettings(), "test").decision_for_observation(event)
    payload = build_discord_payload(decision, "test")
    with pytest.raises(OutboxCapacityError):
        repo.save_signal_and_enqueue(
            decision, payload, 2, delivery_enabled=True, maximum_active_items=1, observation=event,
        )
    with Session(repo.engine) as session:
        key = ("futures", "TESTUSDT", "range_long", "5m")
        assert session.get(MarketWatchEpisodeRow, key) is None
    assert repo.mark_outbox("event-1", "disabled", 3, expected_status="pending")
    assert repo.save_signal_and_enqueue(
        decision, payload, 4, delivery_enabled=True, maximum_active_items=1, observation=event,
    )
    repo.close()


@pytest.mark.asyncio
async def test_membership_departure_rearms_but_preserves_cooldown() -> None:
    repo = SqlRepository("sqlite:///:memory:")
    repo.initialize()
    engine, candle, prior = context()
    event = next(e for e in engine.evaluate(candle, range_feature(candle), prior)
                 if e.family is SignalFamily.RANGE_LONG)
    assert await runtime(repo)._process(event) is not None
    repo.prune_market_watch_episodes(Market.FUTURES, frozenset(), event.event_time_ms + 1)
    assert await runtime(repo)._process(event.model_copy(
        update={"event_time_ms": event.event_time_ms + 1_799_998}
    )) is None
    # A false condition rearms; the exact 30m boundary is accepted.
    await runtime(repo)._process(event.model_copy(update={
        "event_time_ms": event.event_time_ms + 1_800_000 - 1, "score": 0, "triggered": False,
    }))
    assert await runtime(repo)._process(event.model_copy(
        update={"event_time_ms": event.event_time_ms + 1_800_000}
    )) is not None
    repo.close()


@pytest.mark.asyncio
async def test_selector_keeps_alumni_separate_and_uses_candle_only_streams() -> None:
    fixture = json.loads((Path(__file__).parents[1] / "fixtures/retained_gainers.json").read_text())

    class RecordedRest:
        market = Market.FUTURES
        phase = "initial"

        async def exchange_info(self) -> dict[str, Any]:
            return fixture["exchange_info"]

        async def tickers_24h(self) -> list[dict[str, Any]]:
            return fixture[self.phase]

    rest = RecordedRest()
    repo = SqlRepository("sqlite:///:memory:")
    repo.initialize()
    clock = ReplayClock(1000)

    def updater(
        market: Market, gainers: tuple[Instrument, ...], valid: frozenset[str], observed: int,
    ) -> tuple[GainerWatchState, ...]:
        assert market is Market.FUTURES
        return update(repo, gainers, observed, valid)

    selector = UniverseSelector(
        BinanceSettings(top_n=1, surveillance_n=1, minimum_age_days=0), clock,
        MarketWatchSettings(enabled=True, top_n=1, retention_enabled=True), updater,
    )
    first = await selector.select(cast(BinanceRestClient, rest))
    assert first.tradable_symbols == ["TESTUSDT"]
    rest.phase = "rotated"
    second = await selector.select(cast(BinanceRestClient, rest))
    assert second.tradable_symbols == ["ETHUSDT"]
    assert second.watch_symbols == frozenset({"TESTUSDT"})
    assert second.context_symbols == frozenset({"BTCUSDT"})
    assert second.retained_watch[0].quote_volume == Decimal("200000000")
    assert second.watch_states[1].expires_at_ms == 1000 + HOURS_48
    streams = [stream for plan in build_websocket_plans(
        Market.FUTURES, MarketScanner._market_data_symbols(second), ["1m", "5m"], 100,
        candle_only_symbols=(second.watch_symbols | second.context_symbols)
        - frozenset(second.tradable_symbols),
    ) for stream in plan.streams]
    assert "testusdt@kline_5m" in streams and "ethusdt@aggTrade" in streams
    assert "testusdt@aggTrade" not in streams and "testusdt@bookTicker" not in streams
    repo.close()


@pytest.mark.asyncio
async def test_watch_only_runtime_bootstraps_and_alerts_without_r2_or_paper_entries() -> None:
    repo = SqlRepository("sqlite:///:memory:")
    repo.initialize()
    settings = Settings.model_validate({
        "alerts": {"market_watch": {"enabled": True, "context_alerts_enabled": True},
                   "discord_enabled": True, "discord_webhook_url": "https://discord.test/webhook"},
        "signals": {"technical_exit": {"enabled": True}},
    })
    decisions: list[SignalDecision] = []

    async def collected(decision: SignalDecision) -> None:
        decisions.append(decision)

    active = MarketRuntime(Market.FUTURES, settings, repo, ReplayClock(1000), collected)
    active.set_active_symbols(
        frozenset({"OTHERUSDT"}), frozenset({"OTHERUSDT"}),
        watch_symbols=frozenset({"TESTUSDT"}),
    )
    state = GainerWatchState(market=Market.FUTURES, symbol="TESTUSDT", last_top5_observed_ms=0,
                            expires_at_ms=HOURS_48, peak_quote_volume_usdt=1_000_000_000)
    active.set_priority_gainers((), (instrument(volume="200000000"),), (state,), 0)
    candles = [make_candle(i, market=Market.FUTURES, symbol="TESTUSDT", close=100 + i * .1)
               for i in range(260)]
    assert active.bootstrap(candles) == 260
    assert decisions == []
    await active.handle_event(make_candle(
        260, market=Market.FUTURES, symbol="TESTUSDT", close=126,
    ))
    assert decisions and all(d.metadata.get("market_watch") is True for d in decisions)
    assert any(d.family is SignalFamily.WATCH_TREND_UP for d in decisions)
    assert len(decisions) >= 2 and len(repo.pending_outbox()) == 1
    assert all(d.stage is not SignalStage.CONFIRMED for d in decisions)
    assert active.paper_positions.continuation_symbols == frozenset()
    assert active.tradable_symbols == frozenset({"OTHERUSDT"})
    active.set_active_symbols(frozenset({"OTHERUSDT"}), frozenset({"OTHERUSDT"}))
    assert active.candles.get(Market.FUTURES, "TESTUSDT", "5m") == []
    repo.close()


@pytest.mark.asyncio
async def test_paper_full_panel_cannot_block_new_top5_or_alumni_monitoring() -> None:
    current = Universe(Market.FUTURES, (instrument("OLDUSDT"),), (instrument("OLDUSDT"),))
    candidate = Universe(
        Market.FUTURES, (instrument("NEWUSDT"),), (instrument("NEWUSDT"),),
        priority_gainers=(instrument("NEWUSDT"),),
        retained_watch=(instrument("TESTUSDT"),),
    )

    class Selector:
        async def select(self, _rest: Any) -> Universe:
            return candidate

    scanner = object.__new__(MarketScanner)
    scanner.market = Market.FUTURES
    scanner.settings = cast(Any, SimpleNamespace(
        binance=SimpleNamespace(universe_change_confirmations=2)
    ))
    scanner.runtime = cast(Any, SimpleNamespace(
        paper_positions=SimpleNamespace(continuation_symbols=frozenset({"OLDUSDT"}))
    ))
    scanner.selector = cast(Any, Selector())
    scanner.rest = cast(Any, object())
    scanner.universe = current
    scanner._pending_universe_signature = None
    scanner._pending_universe_confirmations = 0
    replacement = await scanner._poll_universe_candidate()
    assert replacement is not None
    assert replacement.tradable_symbols == ["OLDUSDT"]
    assert replacement.watch_symbols == frozenset({"NEWUSDT", "TESTUSDT"})
    assert replacement.priority_gainers == candidate.priority_gainers


@pytest.mark.asyncio
@pytest.mark.parametrize("invalid_part", ["empty_info", "partial_info", "partial_tickers"])
async def test_incomplete_snapshot_never_deletes_saved_qualification(invalid_part: str) -> None:
    fixture = json.loads((Path(__file__).parents[1] / "fixtures/retained_gainers.json").read_text())

    class Rest:
        market = Market.FUTURES

        async def exchange_info(self) -> dict[str, Any]:
            return fixture["exchange_info"]

        async def tickers_24h(self) -> list[dict[str, Any]]:
            return fixture["initial"]

    repo = SqlRepository("sqlite:///:memory:")
    repo.initialize()
    selector = UniverseSelector(
        BinanceSettings(top_n=1, surveillance_n=3, minimum_age_days=0), ReplayClock(1000),
        MarketWatchSettings(enabled=True, retention_enabled=True, top_n=1),
        lambda market, gainers, valid, now: update(repo, gainers, now, valid),
    )
    await selector.select(cast(BinanceRestClient, Rest()))
    if invalid_part == "empty_info":
        fixture["exchange_info"] = {"symbols": []}
    elif invalid_part == "partial_info":
        fixture["exchange_info"]["symbols"] = fixture["exchange_info"]["symbols"][:1]
    else:
        fixture["initial"] = fixture["initial"][:1]
    with pytest.raises(BinanceRestError, match="incomplete ranking snapshot"):
        await selector.select(cast(BinanceRestClient, Rest()))
    assert update(repo, (), 1001)[0].last_top5_observed_ms == 1000
    repo.close()


@pytest.mark.asyncio
async def test_expired_episode_old_intent_does_not_block_fresh_reentry() -> None:
    repo = SqlRepository("sqlite:///:memory:")
    repo.initialize()
    engine, candle, prior = context()
    event = next(e for e in engine.evaluate(candle, range_feature(candle), prior)
                 if e.family is SignalFamily.RANGE_LONG)
    assert await runtime(repo)._process(event) is not None
    later = event.event_time_ms + 49 * 3_600_000
    repo.prune_market_watch_episodes(Market.FUTURES, frozenset({"TESTUSDT"}), later)
    fresh = await runtime(repo)._process(event.model_copy(update={"event_time_ms": later}))
    assert fresh is not None
    repo.close()


@pytest.mark.asyncio
async def test_membership_exit_barrier_rejects_delayed_old_period_candle() -> None:
    repo = SqlRepository("sqlite:///:memory:")
    repo.initialize()
    engine, candle, prior = context()
    event = next(e for e in engine.evaluate(candle, range_feature(candle), prior)
                 if e.family is SignalFamily.RANGE_LONG)
    assert await runtime(repo)._process(event) is not None
    departed = event.event_time_ms + 3_600_000
    repo.prune_market_watch_episodes(Market.FUTURES, frozenset(), departed)
    assert await runtime(repo)._process(event.model_copy(
        update={"event_time_ms": departed - 1}
    )) is None
    assert await runtime(repo)._process(event.model_copy(
        update={"event_time_ms": departed + 1}
    )) is not None
    repo.close()


def test_priority_payload_names_all_coincident_observations() -> None:
    engine, candle, prior = context()
    feature = range_feature(candle).model_copy(update={"bollinger_lower": 92})
    evaluations = engine.evaluate(candle, feature, prior)
    event = next(e for e in evaluations if e.family is SignalFamily.RANGE_LONG)
    assert "bollinger_lower_touch" in event.metadata["concurrent_watch_events"]
    decision = SignalStateMachine(SignalSettings(), "test").decision_for_observation(event)
    payload = str(build_discord_payload(decision, "test"))
    assert "볼린저 하단 접촉" in payload and "박스권 LONG 매수 후보" in payload


@pytest.mark.asyncio
async def test_old_pending_intent_cooldown_carries_over_after_false_observation() -> None:
    repo = SqlRepository("sqlite:///:memory:")
    repo.initialize()
    old = make_decision(
        event_id="old-pending", family=SignalFamily.TOP_GAINER, direction=Direction.RISK_UP,
        symbol="TESTUSDT", stage=SignalStage.SETUP, event_time_ms=600_000,
        metadata={"informational_only": True, "market_watch": True},
    )
    assert runtime(repo)._persist_decision(old)
    engine = MarketWatchEngine(MarketWatchSettings(enabled=True))
    engine.set_priority_gainers((instrument(),))
    candle = make_candle(2, market=Market.FUTURES, symbol="TESTUSDT")
    feature = make_feature(event_time_ms=candle.close_time_ms)
    event = engine.evaluate(candle, feature)[0].model_copy(update={"event_time_ms": 900_000})
    await runtime(repo)._process(event.model_copy(update={"triggered": False, "score": 0}))
    assert await runtime(repo)._process(event.model_copy(update={"event_time_ms": 900_001})) is None
    assert len(repo.pending_outbox()) == 1
    assert repo.pending_outbox()[0].event_id == "old-pending"
    repo.close()


def test_volume_floor_preserves_monitoring_and_future_feature_is_rejected() -> None:
    engine, candle, prior = context()
    state = GainerWatchState(market=Market.FUTURES, symbol="TESTUSDT", last_top5_observed_ms=0,
                            expires_at_ms=HOURS_48, peak_quote_volume_usdt=1_000_000_000)
    engine.set_priority_gainers((), (instrument(volume="100000000"),), (state,), 1)
    feature = range_feature(candle)
    assert engine.monitored_symbols(1) == frozenset({"TESTUSDT"})
    assert not any(e.triggered for e in engine.evaluate(candle, feature, prior))
    future_feature = feature.model_copy(update={"event_time_ms": candle.close_time_ms + 1})
    assert engine.evaluate(candle, future_feature, prior) == ()
    # Latest low liquidity stops new notifications even for a delayed, historically liquid candle.
    engine, candle, prior = context()
    engine.set_priority_gainers(
        (), (instrument(volume="100000000"),), (state,), candle.close_time_ms + 1,
    )
    delayed = engine.evaluate(candle, range_feature(candle), prior)
    assert delayed[0].metadata["quote_volume_24h_usdt"] == 150_000_000
    assert not any(e.triggered for e in delayed)


def test_new_watch_features_require_futures() -> None:
    with pytest.raises(ValueError, match="require USDT perpetual futures"):
        MarketWatchSettings(market=Market.SPOT, retention_enabled=True)
