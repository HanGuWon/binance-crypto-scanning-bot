from __future__ import annotations

from collections.abc import Generator
from decimal import Decimal

import pytest

from position_guardian.domain import (
    AdoptionCandidate,
    ManagedPositionIdentity,
    ProtectiveOrderReference,
)
from position_guardian.exchange.protocol import PositionSnapshot
from position_guardian.persistence.repository import (
    GuardianEventConflictError,
    GuardianProjectionError,
    GuardianRepository,
)


def _identity() -> ManagedPositionIdentity:
    return ManagedPositionIdentity(
        account_alias="manual-main",
        symbol="BTCUSDT",
        position_side="LONG",
        adoption_generation=3,
    )


def _candidate() -> AdoptionCandidate:
    return AdoptionCandidate(
        identity=_identity(),
        quantity=Decimal("0.01"),
        entry_price=Decimal("60000"),
        mark_price=Decimal("61000"),
        original_risk_stop=Decimal("59000"),
        protection_floor=Decimal("60500"),
        protection_source="exchange_stop",
        protective_order=ProtectiveOrderReference(
            source="open_order",
            order_id=11,
            trigger_price=Decimal("59000"),
        ),
    )


def _position(*, mark: str, amount: str = "0.01") -> PositionSnapshot:
    return PositionSnapshot(
        symbol="BTCUSDT",
        position_side="BOTH",
        position_amount=Decimal(amount),
        entry_price=Decimal("60000"),
        mark_price=Decimal(mark),
        unrealized_profit=Decimal("10"),
        update_time_ms=1700000000000,
    )


@pytest.fixture
def repository() -> Generator[GuardianRepository, None, None]:
    with GuardianRepository("sqlite://") as repository:
        yield repository


def test_adoption_persists_projection_and_active_protection(repository: GuardianRepository) -> None:
    assert repository.record_adoption(
        event_id="adopt-1",
        event_time_ms=1000,
        created_at_ms=1001,
        candidate=_candidate(),
    )

    projection = repository.get_projection(_identity())
    assert projection is not None
    assert projection.state == "ADOPTED"
    assert projection.original_risk_stop == "59000"
    assert projection.protection_floor == "60500"
    assert projection.active_protection_order_id == 11
    assert projection.highest_price_since_adoption == "61000"
    assert projection.lowest_price_since_adoption == "61000"


def test_account_snapshots_update_extrema_and_partial_quantity(
    repository: GuardianRepository,
) -> None:
    repository.record_adoption(
        event_id="adopt-1",
        event_time_ms=1000,
        created_at_ms=1001,
        candidate=_candidate(),
    )
    assert repository.record_account_snapshot(
        event_id="snapshot-1",
        event_time_ms=2000,
        created_at_ms=2001,
        identity=_identity(),
        position=_position(mark="62000", amount="0.004"),
    )
    assert repository.record_account_snapshot(
        event_id="snapshot-2",
        event_time_ms=3000,
        created_at_ms=3001,
        identity=_identity(),
        position=_position(mark="58000", amount="0.002"),
    )

    projection = repository.get_projection(_identity())
    assert projection is not None
    assert projection.quantity == "0.002"
    assert projection.highest_price_since_adoption == "62000"
    assert projection.lowest_price_since_adoption == "58000"
    assert projection.last_event_id == "snapshot-2"


def test_same_event_is_noop_and_conflicting_event_id_is_hard_failure(
    repository: GuardianRepository,
) -> None:
    cursor = {
        "event_id": "cursor-1",
        "event_time_ms": 1000,
        "created_at_ms": 1001,
        "cursor_name": "user-stream",
        "cursor_value": "42",
        "uncertainty_state": "CERTAIN",
    }
    assert repository.record_reconciliation_cursor(**cursor)
    assert not repository.record_reconciliation_cursor(**cursor)

    with pytest.raises(GuardianEventConflictError):
        repository.record_reconciliation_cursor(**{**cursor, "cursor_value": "43"})
    assert len(repository.list_events()) == 1


def test_release_is_atomic_and_terminal(repository: GuardianRepository) -> None:
    repository.record_adoption(
        event_id="adopt-1",
        event_time_ms=1000,
        created_at_ms=1001,
        candidate=_candidate(),
    )
    assert repository.record_release(
        event_id="release-1",
        event_time_ms=2000,
        created_at_ms=2001,
        identity=_identity(),
        reason="manual_close",
    )
    projection = repository.get_projection(_identity())
    assert projection is not None
    assert projection.state == "RELEASED"
    assert not repository.record_release(
        event_id="release-1",
        event_time_ms=2000,
        created_at_ms=2001,
        identity=_identity(),
        reason="manual_close",
    )
    with pytest.raises(GuardianProjectionError):
        repository.record_release(
            event_id="release-2",
            event_time_ms=3000,
            created_at_ms=3001,
            identity=_identity(),
            reason="duplicate_release",
        )


def test_planned_intent_and_execution_receipt_are_append_only(
    repository: GuardianRepository,
) -> None:
    repository.record_adoption(
        event_id="adopt-1",
        event_time_ms=1000,
        created_at_ms=1001,
        candidate=_candidate(),
    )
    assert repository.record_planned_intent(
        event_id="intent-1",
        event_time_ms=2000,
        created_at_ms=2001,
        identity=_identity(),
        intent_type="STOP_ADJUSTMENT_SHADOW",
        payload={"target": "60500", "mode": "shadow"},
    )
    assert repository.record_execution_receipt(
        event_id="receipt-1",
        event_time_ms=2002,
        created_at_ms=2003,
        identity=_identity(),
        intent_id="intent-1",
        outcome="NOT_SENT",
        payload={"exchange_write_calls": 0},
    )

    events = repository.list_events(identity=_identity())
    assert [event.event_type for event in events] == [
        "ADOPTION",
        "PLANNED_INTENT",
        "EXECUTION_RECEIPT",
    ]


def test_cursor_projection_keeps_uncertainty_state(repository: GuardianRepository) -> None:
    assert repository.record_reconciliation_cursor(
        event_id="cursor-1",
        event_time_ms=1000,
        created_at_ms=1001,
        cursor_name="rest-poll",
        cursor_value="2026-09-20T00:00:00Z",
        uncertainty_state="CERTAIN",
    )
    assert repository.record_reconciliation_cursor(
        event_id="cursor-2",
        event_time_ms=2000,
        created_at_ms=2001,
        cursor_name="rest-poll",
        cursor_value="2026-09-20T00:01:00Z",
        uncertainty_state="DEGRADED",
        detail="stream gap",
    )

    cursor = repository.get_cursor("rest-poll")
    assert cursor is not None
    assert cursor.cursor_value == "2026-09-20T00:01:00Z"
    assert cursor.uncertainty_state == "DEGRADED"
    assert cursor.detail == "stream gap"


def test_observation_without_adoption_records_event_but_creates_no_management_state(
    repository: GuardianRepository,
) -> None:
    assert repository.record_account_snapshot(
        event_id="snapshot-before-read",
        event_time_ms=1000,
        created_at_ms=1001,
        identity=_identity(),
        position=_position(mark="61000"),
    )

    assert repository.get_projection(_identity()) is None
    assert [event.event_type for event in repository.list_events()] == ["ACCOUNT_SNAPSHOT"]


def test_missing_projection_rejects_intent_and_release(repository: GuardianRepository) -> None:
    with pytest.raises(GuardianProjectionError):
        repository.record_planned_intent(
            event_id="intent-1",
            event_time_ms=1000,
            created_at_ms=1001,
            identity=_identity(),
            intent_type="STOP_ADJUSTMENT_SHADOW",
            payload={},
        )
    with pytest.raises(GuardianProjectionError):
        repository.record_release(
            event_id="release-1",
            event_time_ms=1000,
            created_at_ms=1001,
            identity=_identity(),
            reason="unknown",
        )
    assert repository.list_events() == []
