import pytest
from sqlalchemy import event, update

from conftest import make_candle, make_decision, make_feature
from signalbot.domain.enums import Market
from signalbot.persistence.models import ProtectionContextRow
from signalbot.persistence.repository import (
    EventIdConflictError,
    OutboxCapacityError,
    ProtectionContextCursorError,
    SqlRepository,
)
from signalbot.signals.protection_context import ProtectionContext


def _protection_context(index: int) -> ProtectionContext:
    candle = make_candle(index, market=Market.FUTURES, symbol="BTCUSDT", close=100.0 + index)
    feature = make_feature(
        market=Market.FUTURES,
        symbol="BTCUSDT",
        interval="5m",
        event_time_ms=candle.close_time_ms,
        price=float(candle.close),
        ema20=float(candle.close) - 1.0,
        ema50=float(candle.close) - 2.0,
        atr=2.0,
    )
    return ProtectionContext.from_closed_candle(
        candle=candle,
        feature=feature,
        higher_timeframe_contexts={},
        source_decision_clock_id=(
            f"futures:BTCUSDT:5m:{candle.close_time_ms}"
        ),
    )


def test_repository_round_trip_and_idempotency() -> None:
    repository = SqlRepository("sqlite:///:memory:")
    repository.initialize()
    try:
        candle = make_candle(1)
        assert repository.save_candle(candle) is True
        assert repository.save_candle(candle) is False

        decision = make_decision()
        assert repository.save_signal(decision) is True
        assert repository.save_signal(decision) is False
        assert repository.recent_signals() == [decision]

        assert repository.has_successful_alert(decision.event_id) is False
        repository.record_alert(decision.event_id, 1, "sent", 1234, 204)
        assert repository.has_successful_alert(decision.event_id) is True

        repository.save_outcome(decision.event_id, 3600, 0.1, -0.05, 0.03, 2_000)
        repository.save_outcome(decision.event_id, 3600, 0.2, -0.04, 0.05, 3_000)
    finally:
        repository.close()


def test_recent_signals_can_be_filtered_by_market() -> None:
    repository = SqlRepository("sqlite:///:memory:")
    repository.initialize()
    try:
        futures = make_decision(event_id="futures")
        spot = make_decision(event_id="spot", market=Market.SPOT)
        assert repository.save_signal(futures) is True
        assert repository.save_signal(spot) is True
        assert repository.recent_signals(market=Market.FUTURES) == [futures]
        assert repository.recent_signals(market=Market.SPOT) == [spot]
    finally:
        repository.close()


def test_protection_context_persistence_is_idempotent_and_latest_is_monotonic() -> None:
    repository = SqlRepository("sqlite:///:memory:")
    repository.initialize()
    try:
        older = _protection_context(10)
        newer = _protection_context(12)
        middle = _protection_context(11)

        assert repository.save_protection_context(older, created_at_ms=1_000) is True
        assert repository.save_protection_context(newer, created_at_ms=1_200) is True
        assert repository.save_protection_context(middle, created_at_ms=1_100) is True
        assert repository.save_protection_context(newer, created_at_ms=1_300) is False

        latest = repository.latest_protection_context(
            market=Market.FUTURES,
            symbol="btcusdt",
            primary_interval="5m",
        )
        assert latest == newer
        assert repository.list_protection_contexts(
            market=Market.FUTURES,
            symbol="BTCUSDT",
            primary_interval="5m",
        ) == [older, middle, newer]
    finally:
        repository.close()


def test_protection_context_retention_expires_old_id_cursor_fail_closed() -> None:
    repository = SqlRepository("sqlite:///:memory:")
    repository.initialize()
    try:
        contexts = [_protection_context(index) for index in (20, 21, 22)]
        for created_at_ms, context in enumerate(contexts, start=2_000):
            assert repository.save_protection_context(
                context,
                created_at_ms=created_at_ms,
                retention_per_stream=2,
            )

        assert repository.list_protection_contexts(
            market=Market.FUTURES,
            symbol="BTCUSDT",
            primary_interval="5m",
        ) == contexts[1:]
        with pytest.raises(ProtectionContextCursorError):
            repository.list_protection_contexts(
                market=Market.FUTURES,
                symbol="BTCUSDT",
                primary_interval="5m",
                after_context_id=contexts[0].context_id,
            )
        assert repository.list_protection_contexts(
            market=Market.FUTURES,
            symbol="BTCUSDT",
            primary_interval="5m",
            after_close_time_ms=contexts[1].candle_close_time_ms,
        ) == [contexts[2]]
    finally:
        repository.close()


def test_protection_context_same_clock_conflict_and_payload_corruption_fail_closed() -> None:
    repository = SqlRepository("sqlite:///:memory:")
    repository.initialize()
    try:
        context = _protection_context(30)
        assert repository.save_protection_context(context, created_at_ms=3_000)
        conflicting_clock = ProtectionContext.from_closed_candle(
            candle=make_candle(
                30,
                market=Market.FUTURES,
                symbol="BTCUSDT",
                close=130.0,
            ),
            feature=make_feature(
                market=Market.FUTURES,
                symbol="BTCUSDT",
                interval="5m",
                event_time_ms=context.candle_close_time_ms,
                price=130.0,
                ema20=129.0,
                ema50=128.0,
                atr=3.0,
            ),
            higher_timeframe_contexts={},
            source_decision_clock_id=context.source_decision_clock_id,
            consecutive_trend_failure_count=1,
        )
        with pytest.raises(EventIdConflictError, match="decision clock"):
            repository.save_protection_context(conflicting_clock, created_at_ms=3_001)

        with repository.engine.begin() as connection:
            connection.execute(
                update(ProtectionContextRow)
                .where(ProtectionContextRow.context_id == context.context_id)
                .values(payload_json="{}")
            )
        with pytest.raises(EventIdConflictError, match="conflicting payloads"):
            repository.save_protection_context(context, created_at_ms=3_002)
    finally:
        repository.close()

def test_repository_candle_batch_matches_single_row_idempotency() -> None:
    repository = SqlRepository("sqlite:///:memory:")
    repository.initialize()
    try:
        commits: list[object] = []
        event.listen(repository.engine, "commit", lambda _connection: commits.append(object()))
        candles = [make_candle(index) for index in range(3)]
        assert repository.save_candles([]) == 0
        assert commits == []
        assert repository.save_candles(candles) == 3
        assert len(commits) == 1
        assert repository.save_candles(candles) == 0
        assert len(commits) == 2
        assert repository.save_candle(candles[0]) is False
        assert len(commits) == 3
    finally:
        repository.close()


def test_signal_and_outbox_are_atomic_and_conflicts_fail_closed() -> None:
    repository = SqlRepository("sqlite:///:memory:")
    repository.initialize()
    try:
        decision = make_decision()
        payload: dict[str, object] = {"content": "deterministic", "embeds": []}
        assert repository.save_signal_and_enqueue(
            decision, payload, 1234, delivery_enabled=True
        )
        assert not repository.save_signal_and_enqueue(
            decision, payload, 1234, delivery_enabled=True
        )

        item = repository.get_outbox(decision.event_id)
        assert item is not None
        assert item.status == "pending"
        claimed = repository.claim_outbox(decision.event_id, 1235)
        assert claimed is not None
        assert claimed.status == "sending"
        assert claimed.attempts == 1
        assert repository.mark_outbox(
            decision.event_id,
            "delivered",
            1236,
            response_code=200,
            message_id="message-1",
        )

        with pytest.raises(EventIdConflictError):
            repository.save_signal_and_enqueue(
                decision,
                {"content": "changed"},
                1237,
                delivery_enabled=True,
            )
        with pytest.raises(EventIdConflictError):
            repository.save_signal(make_decision(score=84))
    finally:
        repository.close()


def test_restart_quarantines_inflight_outbox() -> None:
    repository = SqlRepository("sqlite:///:memory:")
    repository.initialize()
    try:
        decision = make_decision()
        repository.save_signal_and_enqueue(
            decision, {"content": "test"}, 1234, delivery_enabled=True
        )
        assert repository.claim_outbox(decision.event_id, 1235) is not None
        assert repository.mark_inflight_uncertain(1236) == 1
        item = repository.get_outbox(decision.event_id)
        assert item is not None
        assert item.status == "uncertain"
    finally:
        repository.close()


def test_active_outbox_capacity_fails_before_signal_commit() -> None:
    repository = SqlRepository("sqlite:///:memory:")
    repository.initialize()
    try:
        first = make_decision()
        second = make_decision(event_id="event-2", event_time_ms=900_000)
        assert repository.save_signal_and_enqueue(
            first,
            {"content": "first"},
            1,
            delivery_enabled=True,
            maximum_active_items=1,
        )
        with pytest.raises(OutboxCapacityError, match="hard limit"):
            repository.save_signal_and_enqueue(
                second,
                {"content": "second"},
                2,
                delivery_enabled=True,
                maximum_active_items=1,
            )
        assert repository.recent_signals() == [first]
        assert repository.get_outbox(second.event_id) is None
    finally:
        repository.close()
