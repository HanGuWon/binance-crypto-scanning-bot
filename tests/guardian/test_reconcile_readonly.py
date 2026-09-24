from __future__ import annotations

from collections.abc import Generator
from decimal import Decimal

import pytest
from pydantic import SecretStr

from position_guardian.config import GuardianCredentials, GuardianSettings
from position_guardian.domain import (
    AdoptionCandidate,
    ManagedPositionIdentity,
    ProtectiveOrderReference,
)
from position_guardian.exchange.protocol import PositionSnapshot
from position_guardian.persistence.repository import GuardianRepository
from position_guardian.reconcile import (
    ReconciliationError,
    ReconciliationRequest,
    reconcile_once,
)
from position_guardian.runtime import build_reconciliation_report


def _identity(generation: int = 3) -> ManagedPositionIdentity:
    return ManagedPositionIdentity("manual-main", "BTCUSDT", "LONG", generation)


def _candidate(identity: ManagedPositionIdentity | None = None) -> AdoptionCandidate:
    selected = identity or _identity()
    return AdoptionCandidate(
        identity=selected,
        quantity=Decimal("0.01"),
        entry_price=Decimal("60000"),
        mark_price=Decimal("61000"),
        source_update_time_ms=1700000000000,
        original_risk_stop=Decimal("59000"),
        protection_floor=Decimal("60500"),
        protection_source="exchange_stop",
        protective_order=ProtectiveOrderReference("open_order", 11, Decimal("59000")),
    )


def _position(
    *,
    amount: str = "0.01",
    mark: str = "61000",
    update_time_ms: int = 1700000000001,
) -> PositionSnapshot:
    return PositionSnapshot(
        symbol="BTCUSDT",
        position_side="BOTH",
        position_amount=Decimal(amount),
        entry_price=Decimal("60000"),
        mark_price=Decimal(mark),
        unrealized_profit=Decimal("10"),
        update_time_ms=update_time_ms,
    )


def _request(
    *,
    event_id: str,
    position: PositionSnapshot,
    protective: bool = True,
    candidate: AdoptionCandidate | None = None,
    uncertainty: str = "CERTAIN",
    release_event_id: str | None = None,
) -> ReconciliationRequest:
    return ReconciliationRequest(
        identity=_identity(),
        position=position,
        snapshot_event_id=event_id,
        event_time_ms=2000,
        created_at_ms=2001,
        protective_order_confirmed=protective,
        adoption_candidate=candidate,
        uncertainty_state=uncertainty,  # type: ignore[arg-type]
        release_event_id=release_event_id,
    )


@pytest.fixture
def repository() -> Generator[GuardianRepository, None, None]:
    with GuardianRepository("sqlite://") as repository:
        yield repository


def _adopt(repository: GuardianRepository) -> None:
    assert repository.record_adoption(
        event_id="adopt-1",
        event_time_ms=1000,
        created_at_ms=1001,
        candidate=_candidate(),
    )


def test_discovered_and_adoptable_never_auto_adopt(repository: GuardianRepository) -> None:
    discovered = reconcile_once(
        repository,
        _request(event_id="snapshot-1", position=_position()),
    )
    adoptable = reconcile_once(
        repository,
        _request(event_id="snapshot-2", position=_position(), candidate=_candidate()),
    )

    assert discovered.state == "DISCOVERED"
    assert adoptable.state == "ADOPTABLE"
    assert repository.get_projection(_identity()) is None


def test_observed_snapshot_enters_managed_shadow(repository: GuardianRepository) -> None:
    _adopt(repository)

    result = reconcile_once(
        repository,
        _request(event_id="snapshot-1", position=_position()),
    )

    assert result.state == "MANAGED_SHADOW"
    assert result.alerts == ()
    assert result.exchange_write_calls == 0


def test_partial_close_updates_quantity_and_requires_attention(
    repository: GuardianRepository,
) -> None:
    _adopt(repository)

    result = reconcile_once(
        repository,
        _request(event_id="snapshot-1", position=_position(amount="0.004")),
    )

    assert result.state == "MANAGED_SHADOW"
    assert result.alerts == ("MANUAL_PARTIAL_CLOSE",)
    assert result.operator_attention
    assert repository.get_projection(_identity()).quantity == "0.004"  # type: ignore[union-attr]


def test_manual_add_never_expands_protection_automatically(repository: GuardianRepository) -> None:
    _adopt(repository)

    result = reconcile_once(
        repository,
        _request(event_id="snapshot-1", position=_position(amount="0.02")),
    )

    assert result.alerts == ("MANUAL_POSITION_ADD",)
    assert result.operator_attention
    assert result.exchange_write_calls == 0


def test_missing_order_is_alert_only_in_shadow(repository: GuardianRepository) -> None:
    _adopt(repository)

    result = reconcile_once(
        repository,
        _request(event_id="snapshot-1", position=_position(), protective=False),
    )

    assert result.state == "MANAGED_SHADOW"
    assert result.alerts == ("PROTECTIVE_ORDER_MISSING",)
    assert result.exchange_write_calls == 0


def test_side_flip_releases_old_generation_and_needs_new_approval(
    repository: GuardianRepository,
) -> None:
    _adopt(repository)

    result = reconcile_once(
        repository,
        _request(
            event_id="snapshot-flip",
            position=_position(amount="-0.01", mark="59000"),
            release_event_id="release-flip",
        ),
    )

    assert result.state == "RELEASED"
    assert result.alerts == ("SIDE_FLIP_REQUIRES_REAPPROVAL",)
    assert repository.get_projection(_identity()).state == "RELEASED"  # type: ignore[union-attr]
    assert repository.get_projection(_identity(4)) is None


def test_zero_quantity_closes_position(repository: GuardianRepository) -> None:
    _adopt(repository)

    result = reconcile_once(
        repository,
        _request(
            event_id="snapshot-close",
            position=_position(amount="0"),
            release_event_id="release-close",
        ),
    )

    assert result.state == "CLOSED"
    assert result.quantity == Decimal("0")


def test_uncertainty_degrades_without_release_or_write(repository: GuardianRepository) -> None:
    _adopt(repository)

    result = reconcile_once(
        repository,
        _request(
            event_id="snapshot-gap",
            position=_position(),
            uncertainty="DEGRADED",
        ),
    )

    assert result.state == "DEGRADED"
    assert result.operator_attention
    assert repository.get_projection(_identity()).state == "ADOPTED"  # type: ignore[union-attr]
    assert result.exchange_write_calls == 0


@pytest.mark.parametrize("amount", ["0", "-0.01"])
def test_uncertain_terminal_looking_snapshot_never_releases(
    repository: GuardianRepository,
    amount: str,
) -> None:
    _adopt(repository)

    result = reconcile_once(
        repository,
        _request(
            event_id=f"snapshot-degraded-{amount}",
            position=_position(amount=amount),
            uncertainty="DEGRADED",
            release_event_id=f"release-degraded-{amount}",
        ),
    )

    assert result.state == "DEGRADED"
    assert result.alerts == ("RECONCILIATION_UNCERTAIN",)
    projection = repository.get_projection(_identity())
    assert projection is not None
    assert projection.state == "ADOPTED"
    assert all(
        event.event_type != "RELEASE"
        for event in repository.list_events(identity=_identity())
    )


def test_duplicate_snapshot_replay_is_deterministic(repository: GuardianRepository) -> None:
    _adopt(repository)
    request = _request(event_id="snapshot-duplicate", position=_position())

    first = reconcile_once(repository, request)
    second = reconcile_once(repository, request)

    assert first.state == second.state
    assert first.alerts == second.alerts
    assert first.snapshot_event_accepted is True
    assert second.snapshot_event_accepted is False
    assert len(repository.list_events()) == 2


def test_terminal_release_requires_explicit_event_id(repository: GuardianRepository) -> None:
    _adopt(repository)

    with pytest.raises(ReconciliationError):
        reconcile_once(
            repository,
            _request(event_id="snapshot-flip", position=_position(amount="-0.01")),
        )


def test_runtime_report_is_read_only() -> None:
    from position_guardian.reconcile import ReconciliationResult

    result = ReconciliationResult("MANAGED_SHADOW", Decimal("0.01"), (), False, True)
    report = build_reconciliation_report(
        GuardianSettings(
            account_alias="manual-main",
            credentials=GuardianCredentials(
                api_key=SecretStr("key"),
                api_secret=SecretStr("secret"),
            ),
        ),
        result,
    )

    assert report["state"] == "MANAGED_SHADOW"
    assert report["exchange_write_calls"] == 0
