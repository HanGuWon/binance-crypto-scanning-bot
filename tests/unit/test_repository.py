import pytest
from sqlalchemy import event

from conftest import make_candle, make_decision
from signalbot.persistence.repository import (
    EventIdConflictError,
    OutboxCapacityError,
    SqlRepository,
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
